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

type Phrase string

const (
	MapPhrase    Phrase = "MAP_PHRASE"
	ReducePhrase Phrase = "REDUCE_PHRASE"
	MergePhrase  Phrase = "MERGE_PHRASE"
)

type TaskStatus string

const (
	NotStart   TaskStatus = "NOT_START"
	InProgress TaskStatus = "IN_PROGRESS"
	Done       TaskStatus = "DONE"
)

type Coordinator struct {
	// Your definitions here.
	sync.Mutex
	// list of filenames
	files []string
	// number of reducer
	nReduce int
	// current phrase
	phrase Phrase
	// current map task index
	mapTaskStatuses map[int]TaskStatus
	// current reduce task index
	reduceTaskStatuses map[int]TaskStatus
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.Lock()
	reply.TaskID = -1

	switch c.phrase {
	case MapPhrase:
		for idx, t := range c.mapTaskStatuses {
			if t == NotStart {
				log.Printf("assigned map taskid=%v", idx)
				reply.TaskID = idx
				reply.File = c.files[idx]
				reply.TaskType = MapTask
				reply.NReduce = c.nReduce

				c.mapTaskStatuses[idx] = InProgress

				go func() {
					wg := sync.WaitGroup{}
					wg.Add(1)

					timeout := time.Second * 10
					if c.waitTaskTimeout(MapTask, idx, &wg, timeout) {
						log.Printf("map task with id=%v is timeouted, turn it to NOT_START", idx)
						c.Lock()
						c.mapTaskStatuses[idx] = NotStart
						c.Unlock()
					} else {
						log.Printf("map task with id=%v completed", idx)
					}
				}()
				break
			}
		}

		if reply.TaskID == -1 {
			log.Printf("there no available map task")
		}
	case ReducePhrase:
		for idx, t := range c.reduceTaskStatuses {
			if t == NotStart {
				log.Printf("assigned reduce taskid=%v", idx)
				reply.TaskID = idx
				reply.TaskType = ReduceTask
				reply.NMap = len(c.files)

				c.reduceTaskStatuses[idx] = InProgress
				go func() {
					wg := sync.WaitGroup{}
					wg.Add(1)

					timeout := time.Second * 10
					if c.waitTaskTimeout(ReduceTask, idx, &wg, timeout) {
						log.Printf("map task with id=%v is timeouted, turn it to NOT_START", idx)
						c.Lock()
						c.reduceTaskStatuses[idx] = NotStart
						c.Unlock()
					} else {
						log.Printf("map task with id=%v completed", idx)
					}
				}()
				break
			}
		}

		if reply.TaskID == -1 {
			log.Printf("there no available reduce task")
		}
	case MergePhrase:
		// TODO: implement this
		log.Printf("server is doing merge job, please wait...")
	default:
		log.Fatalf("invalid phrase: %v", c.phrase)
	}
	c.Unlock()

	return nil
}

func (c *Coordinator) ReportTaskResult(args *ReportTaskResultArgs, reply *ReportTaskResultReply) error {
	log.Printf("tasktype=%v taskid=%v, result=%v", args.TaskType, args.TaskID, args.Result)
	c.Lock()
	switch args.TaskType {
	case MapTask:
		if _, ok := c.mapTaskStatuses[args.TaskID]; !ok {
			log.Printf("invalid map task id: %v", args.TaskID)
		}

		if args.Result {
			c.mapTaskStatuses[args.TaskID] = Done
		} else {
			c.mapTaskStatuses[args.TaskID] = NotStart
		}

		if c.isAllMapTaskDoneNonLock() {
			c.phrase = ReducePhrase
			log.Printf("transition to reduce phrase")
		}
	case ReduceTask:
		if _, ok := c.reduceTaskStatuses[args.TaskID]; !ok {
			log.Printf("invalid reduce task id: %v", args.TaskID)
		}

		if args.Result {
			c.reduceTaskStatuses[args.TaskID] = Done
		} else {
			c.reduceTaskStatuses[args.TaskID] = NotStart
		}

		if c.isAllReduceTaskDoneNonLock() {
			c.phrase = MergePhrase
			log.Printf("transition to merge phrase")
		}
	default:
		log.Printf("invalid task type: %v", args.TaskType)
	}
	c.Unlock()

	return nil
}

func (c *Coordinator) isAllMapTaskDoneNonLock() bool {
	for _, t := range c.mapTaskStatuses {
		if t != Done {
			return false
		}
	}

	return true
}

func (c *Coordinator) isAllReduceTaskDoneNonLock() bool {
	for _, t := range c.reduceTaskStatuses {
		if t != Done {
			return false
		}
	}

	return true
}

func (c *Coordinator) isTaskDone(taskType TaskType, taskID int) bool {
	c.Lock()
	var taskStatus TaskStatus
	switch taskType {
	case MapTask:
		taskStatus = c.mapTaskStatuses[taskID]
	case ReduceTask:
		taskStatus = c.reduceTaskStatuses[taskID]
	default:
		log.Printf("invalid taskType: %v", taskType)
	}
	c.Unlock()

	return taskStatus == Done
}

// waitTaskDoneWithTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func (c *Coordinator) waitTaskTimeout(
	taskType TaskType,
	taskID int,
	wg *sync.WaitGroup,
	timeout time.Duration,
) bool {
	ch := make(chan struct{})

	go func() {
		for !c.isTaskDone(taskType, taskID) {
			time.Sleep(100 * time.Millisecond)
		}

		defer close(ch)
	}()

	select {
	case <-ch:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// TODO: implement merge phrase and change this
	c.Lock()
	ret := c.phrase == MergePhrase
	c.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:              files,
		nReduce:            nReduce,
		phrase:             MapPhrase,
		mapTaskStatuses:    initMapTaskStatuses(files),
		reduceTaskStatuses: initReduceTaskStatuses(nReduce),
	}

	// Your code here.

	c.server()
	return &c
}

func initMapTaskStatuses(files []string) map[int]TaskStatus {
	m := make(map[int]TaskStatus)
	for idx := range files {
		m[idx] = NotStart
	}
	return m
}

func initReduceTaskStatuses(nReduce int) map[int]TaskStatus {
	m := make(map[int]TaskStatus)
	for i := 0; i < nReduce; i++ {
		m[i] = NotStart
	}
	return m
}
