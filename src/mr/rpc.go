package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Empty struct{}

type TaskType string

const (
	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
	Wait       TaskType = "wait"
	Exit       TaskType = "exit"
)

type WorkTask struct {
	TaskType   TaskType
	TaskNumber int

	Filename string
	NMap        int
	NReduce        int
}

type TaskDoneArgs struct {
	TaskType   TaskType
	TaskNumber int
}
