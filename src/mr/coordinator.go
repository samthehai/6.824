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
	sync.RWMutex
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

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	reply.TaskID = -1

	c.Lock()
	debug("RequestTask - map locked 1")
	switch c.phrase {
	case MapPhrase:
		for idx, t := range c.mapTaskStatuses {
			if t == NotStart {
				debug("assigned map taskid=%v", idx)
				reply.TaskID = idx
				reply.File = c.files[idx]
				reply.TaskType = MapTask
				reply.NReduce = c.nReduce

				c.mapTaskStatuses[idx] = InProgress

				go func() {
					wg := sync.WaitGroup{}
					wg.Add(1)

					timeout := time.Second * 10
					if c.waitTaskCheckTimeout(MapTask, idx, &wg, timeout) {
						c.Lock()
						debug("RequestTask - map locked 2")
						c.mapTaskStatuses[idx] = NotStart
						c.Unlock()
						debug("RequestTask - map released lock 2")

						debug("map task with id=%v is timeout, reseted status", idx)
					}
				}()
				break
			}
		}

		if reply.TaskID == -1 {
			debug("there no available map task")
		}
	case ReducePhrase:
		for idx, t := range c.reduceTaskStatuses {
			if t == NotStart {
				debug("assigned reduce taskid=%v", idx)
				reply.TaskID = idx
				reply.TaskType = ReduceTask
				reply.NMap = len(c.files)

				c.reduceTaskStatuses[idx] = InProgress
				go func() {
					wg := sync.WaitGroup{}
					wg.Add(1)

					timeout := time.Second * 10
					if c.waitTaskCheckTimeout(ReduceTask, idx, &wg, timeout) {
						c.Lock()
						debug("RequestTask - reduce locked 2")
						c.reduceTaskStatuses[idx] = NotStart
						c.Unlock()
						debug("RequestTask - reduce released lock 2")

						debug("map task with id=%v is timeout, reseted status", idx)
					}
				}()
				break
			}
		}

		if reply.TaskID == -1 {
			debug("there no available reduce task")
		}
	case MergePhrase:
		// TODO: implement this
		debug("server is doing merge job, please wait...")
	default:
		log.Fatalf("invalid phrase: %v", c.phrase)
	}
	c.Unlock()
	debug("RequestTask - map released lock 1")

	return nil
}

func (c *Coordinator) ReportTaskResult(args *ReportTaskResultArgs, reply *ReportTaskResultReply) error {
	debug("tasktype=%v taskid=%v, result=%v", args.TaskType, args.TaskID, args.Result)

	switch args.TaskType {
	case MapTask:
		c.RLock()
		debug("ReportTaskResult - map locked 1")
		if _, ok := c.mapTaskStatuses[args.TaskID]; !ok {
			debug("invalid map task id: %v", args.TaskID)
		}
		c.RUnlock()
		debug("ReportTaskResult - map released lock 1")

		c.Lock()
		debug("ReportTaskResult - map locked 2")
		if args.Result {
			c.mapTaskStatuses[args.TaskID] = Done
		} else {
			c.mapTaskStatuses[args.TaskID] = NotStart
		}
		c.Unlock()
		debug("ReportTaskResult - map released lock 2")

		if c.isAllMapTaskDone() {
			c.Lock()
			debug("ReportTaskResult - map locked 3")
			c.phrase = ReducePhrase
			c.Unlock()
			debug("ReportTaskResult - map released lock 3")

			debug("transited to reduce phrase")
		}
	case ReduceTask:
		c.RLock()
		debug("ReportTaskResult - reduce locked 1")
		if _, ok := c.reduceTaskStatuses[args.TaskID]; !ok {
			debug("invalid reduce task id: %v", args.TaskID)
		}
		c.RUnlock()
		debug("ReportTaskResult - reduce released lock 1")

		c.Lock()
		debug("ReportTaskResult - reduce locked 2")
		if args.Result {
			c.reduceTaskStatuses[args.TaskID] = Done
		} else {
			c.reduceTaskStatuses[args.TaskID] = NotStart
		}
		c.Unlock()
		debug("ReportTaskResult - reduce released 2")

		if c.isAllReduceTaskDone() {
			c.Lock()
			debug("ReportTaskResult - reduce locked 3")
			c.phrase = MergePhrase
			c.Unlock()
			debug("ReportTaskResult - reduce released 3")

			debug("transited to merge phrase")
		}
	default:
		debug("invalid task type: %v", args.TaskType)
	}

	return nil
}

func (c *Coordinator) isAllMapTaskDone() bool {
	c.RLock()
	debug("isAllMapTaskDone - locked")
	defer func() {
		c.RUnlock()
		debug("isAllMapTaskDone - released lock")
	}()

	for _, t := range c.mapTaskStatuses {
		if t != Done {
			return false
		}
	}

	return true
}

func (c *Coordinator) isAllReduceTaskDone() bool {
	c.RLock()
	debug("isAllReduceTaskDone - locked")
	defer func() {
		c.RUnlock()
		debug("isAllReduceTaskDone - released lock")
	}()

	for _, t := range c.reduceTaskStatuses {
		if t != Done {
			return false
		}
	}

	return true
}

func (c *Coordinator) isTaskDone(taskType TaskType, taskID int) bool {
	c.RLock()
	debug("isTaskDone - locked")
	var taskStatus TaskStatus
	switch taskType {
	case MapTask:
		taskStatus = c.mapTaskStatuses[taskID]
	case ReduceTask:
		taskStatus = c.reduceTaskStatuses[taskID]
	default:
		debug("invalid taskType: %v", taskType)
	}
	c.RUnlock()
	debug("isTaskDone - released")

	return taskStatus == Done
}

// waitTaskDoneWithTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func (c *Coordinator) waitTaskCheckTimeout(
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
	c.RLock()
	debug("Done - locked")
	ret := c.phrase == MergePhrase
	c.RUnlock()
	debug("Done - released lock")

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
