package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		var task = RequestTask()

		switch task.TaskType {
		case MapTask:
			err := processMap(
				mapf,
				task.TaskNumber,
				task.Filename,
				task.NReduce,
			)
			if err != nil {
				log.Printf("map task %d failed: %v", task.TaskNumber, err)
				return
			}
			ReportTaskDone(task)
		case ReduceTask:
			err := processReduce(
				reducef,
				task.TaskNumber,
				task.NMap,
			)
			if err != nil {
				log.Printf(
					"reduce task %d failed: %v",
					task.TaskNumber,
					err,
				)
				return
			}
			ReportTaskDone(task)
		case Wait:
			time.Sleep(time.Second)
		case Exit:
			return
		}
	}
}

func processMap(f func(string, string) []KeyValue, taskNumber int, filename string, n int) error {
	var buckets = make([][]KeyValue, n)

	contents, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	var pairs = f(filename, string(contents))
	for _, kv := range pairs {
		reduceNumber := ihash(kv.Key) % n
		buckets[reduceNumber] = append(buckets[reduceNumber], kv)
	}

	for i, bucket := range buckets {
		var reduceFilename = fmt.Sprintf("mr-%d-%d", taskNumber, i)
		if err := writeIntermediateFile(reduceFilename, bucket); err != nil {
			return fmt.Errorf("write map task %d partition %d: %w", taskNumber, i, err)
		}
	}
	return nil
}

func processReduce(f func(string, []string) string, taskNumber int, m int) error {
	var total []KeyValue
	for i := 0; i < m; i++ {
		pairs, err := readIntermediateFile(fmt.Sprintf("mr-%d-%d", i, taskNumber))
		if err != nil {
			return fmt.Errorf(
				"read input for reduce task %d: %w",
				taskNumber,
				err,
			)
		}

		total = append(total, pairs...)
	}
	sort.Slice(total, func(i, j int) bool {
		return total[i].Key < total[j].Key
	})

	var results []KeyValue

	i := 0
	for i < len(total) {
		key := total[i].Key

		j := i + 1
		for j < len(total) && total[j].Key == key {
			j++
		}

		values := make([]string, 0)
		for _, kv := range total[i:j] {
			values = append(values, kv.Value)
		}

		output := f(key, values)

		results = append(results, KeyValue{
			Key:   key,
			Value: output,
		})

		i = j
	}

	if err := writeReduceOutput(taskNumber, results); err != nil {
		return fmt.Errorf(
			"write reduce task %d output: %w",
			taskNumber,
			err,
		)
	}

	return nil
}

func RequestTask() WorkTask {
	args := Empty{}
	reply := WorkTask{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return reply
	}
	return WorkTask{}
}

func ReportTaskDone(task WorkTask) {
	args := TaskDoneArgs{TaskType: task.TaskType, TaskNumber: task.TaskNumber}
	reply := Empty{}

	call("Coordinator.TaskDone", &args, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

func writeIntermediateFile(filename string, kva []KeyValue) error {
	tempFile, err := os.CreateTemp(".", "mr-tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary file for %s: %w", filename, err)
	}

	tempName := tempFile.Name()

	defer os.Remove(tempName)

	encoder := json.NewEncoder(tempFile)

	for _, kv := range kva {
		if err := encoder.Encode(&kv); err != nil {
			tempFile.Close()
			return fmt.Errorf("encode intermediate data for %s: %w", filename, err)
		}
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close temporary file for %s: %w", filename, err)
	}

	if err := os.Rename(tempName, filename); err != nil {
		return fmt.Errorf("rename %s to %s: %w", tempName, filename, err)
	}

	return nil
}

func readIntermediateFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open intermediate file %s: %w", filename, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var kva []KeyValue

	for {
		var kv KeyValue

		err := decoder.Decode(&kv)

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, fmt.Errorf(
				"decode intermediate file %s: %w",
				filename,
				err,
			)
		}

		kva = append(kva, kv)
	}

	return kva, nil
}

func writeReduceOutput(taskNumber int, results []KeyValue) error {
	finalName := fmt.Sprintf("mr-out-%d", taskNumber)

	tempFile, err := os.CreateTemp(".", "mr-out-tmp-*")
	if err != nil {
		return fmt.Errorf(
			"create temporary reduce output for task %d: %w",
			taskNumber,
			err,
		)
	}

	tempName := tempFile.Name()
	defer os.Remove(tempName)

	for _, result := range results {
		_, err := fmt.Fprintf(
			tempFile,
			"%v %v\n",
			result.Key,
			result.Value,
		)
		if err != nil {
			tempFile.Close()
			return fmt.Errorf(
				"write reduce output for task %d: %w",
				taskNumber,
				err,
			)
		}
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf(
			"close reduce output for task %d: %w",
			taskNumber,
			err,
		)
	}

	if err := os.Rename(tempName, finalName); err != nil {
		return fmt.Errorf(
			"rename %s to %s: %w",
			tempName,
			finalName,
			err,
		)
	}

	return nil
}
