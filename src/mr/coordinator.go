package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Done
)

type Task struct {
	Status    TaskStatus
	StartedAt time.Time
	Filename  string // нужно только map-задаче
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	nReduce int
	mMap    int

	mapTasks    []Task
	reduceTasks []Task
}

func allDone(tasks []Task) bool {
	for _, task := range tasks {
		if task.Status != Done {
			return false
		}
	}
	return true
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *Empty, reply *WorkTask) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !allDone(c.mapTasks) {
		reply.TaskType = Wait

		for i, t := range c.mapTasks {
			available := t.Status == Idle ||
				(t.Status == InProgress &&
					time.Since(t.StartedAt) >= 10*time.Second)
			if available {
				c.mapTasks[i].Status = InProgress
				c.mapTasks[i].StartedAt = time.Now()

				reply.TaskNumber = i
				reply.TaskType = MapTask
				reply.Filename = c.mapTasks[i].Filename
				reply.NReduce = c.nReduce
				return nil
			}
		}
		return nil
	}

	if !allDone(c.reduceTasks) {
		reply.TaskType = Wait

		for i, t := range c.reduceTasks {
			available := t.Status == Idle ||
				(t.Status == InProgress &&
					time.Since(t.StartedAt) >= 10*time.Second)
			if available {
				c.reduceTasks[i].Status = InProgress
				c.reduceTasks[i].StartedAt = time.Now()

				reply.TaskNumber = i
				reply.TaskType = ReduceTask
				reply.NMap = c.mMap
				return nil
			}
		}
		return nil
	}

	reply.TaskType = Exit
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *Empty) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		c.mapTasks[args.TaskNumber].Status = Done
	} else {
		c.reduceTasks[args.TaskNumber].Status = Done
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return allDone(c.mapTasks) && allDone(c.reduceTasks)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.mMap = len(files)

	for _, filename := range files {
		c.mapTasks = append(c.mapTasks, Task{Status: Idle, Filename: filename})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{Status: Idle})
	}

	c.server(sockname)
	return &c
}
