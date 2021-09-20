package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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
type TaskType string

const (
	MapTask    TaskType = "MAP_TASK"
	ReduceTask TaskType = "REDUCE_TASK"
)

type RequestTaskArgs struct{}

type RequestTaskReply struct {
	TaskID   int
	TaskType TaskType
	// for map task
	File    string
	NReduce int
	// for reduce task
	NMap int
}

type ReportTaskResultArgs struct {
	TaskID   int
	TaskType TaskType
	Result   bool
}

type ReportTaskResultReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
