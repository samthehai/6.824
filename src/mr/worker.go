package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		res := call("Coordinator.RequestTask", &args, &reply)
		if !res {
			log.Fatalf("failed to call rpc")
		}

		if reply.TaskID == -1 {
			debug("there is no available task, go to sleep...")
			time.Sleep(time.Second)
			continue
		}

		taskID := reply.TaskID
		taskType := reply.TaskType

		switch reply.TaskType {
		case MapTask:
			doMap(reply.TaskID, reply.NReduce, reply.File, mapf)
		case ReduceTask:
			doReduce(reply.TaskID, reduceOutputFileName(reply.TaskID), reply.NMap, reducef)
		default:
			log.Fatalf("invalid task type")
		}

		rArgs := ReportTaskResultArgs{
			TaskID:   taskID,
			TaskType: taskType,
			Result:   true,
		}
		rReply := ReportTaskResultReply{}
		res = call("Coordinator.ReportTaskResult", &rArgs, &rReply)
		if !res {
			log.Fatalf("failed to call rpc")
		}
	}
}

func doMap(
	mapTaskNumber int, // which map task this is
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	filename string,
	mapf func(string, string) []KeyValue,
) {
	debug("doMap: %d-%d", mapTaskNumber, nReduce)

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("failed to read file %v: %v", filename, err)
	}
	kvList := mapf(filename, string(content))

	encList := make([]*json.Encoder, 0)
	for i := 0; i < nReduce; i++ {
		tmpFileName := intermediateFileName(mapTaskNumber, i)
		tmpFile, err := os.Create(tmpFileName)
		if err != nil {
			log.Fatalf("failed to create tmp: %v", err)
		}

		enc := json.NewEncoder(tmpFile)
		encList = append(encList, enc)

		defer tmpFile.Close()
	}

	for _, kv := range kvList {
		key := kv.Key
		reduceTaskNumber := ihash(key) % nReduce

		err := encList[reduceTaskNumber].Encode(&kv)
		if err != nil {
			log.Fatalf("failed to encode kv: %v", err)
		}
	}
}

func doReduce(
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	debug("doReduce: %d", reduceTaskNumber)
	kvList := make([]KeyValue, 0)

	for i := 0; i < nMap; i++ {
		tmpFileName := intermediateFileName(i, reduceTaskNumber)
		tmpFile, err := os.Open(tmpFileName)
		if err != nil {
			log.Fatalf("failed to open tmp file: %v", err)
		}

		dec := json.NewDecoder(tmpFile)
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatalf("failed to decode tmp file: %v", err)
			}
			kvList = append(kvList, kv)
		}
	}

	// TODO: sort to enhance performance

	kvsMap := make(map[string][]string)
	for _, kv := range kvList {
		if _, ok := kvsMap[kv.Key]; !ok {
			kvsMap[kv.Key] = make([]string, 0, 1)
		}
		kvsMap[kv.Key] = append(kvsMap[kv.Key], kv.Value)
	}

	writeFile, err := os.Create(outFile)
	if err != nil {
		log.Fatalf("failed to create output file: %v", err)
	}
	defer writeFile.Close()

	for key, vlist := range kvsMap {
		output := reduceF(key, vlist)
		fmt.Fprintf(writeFile, "%v %v\n", key, output)
	}
}

func intermediateFileName(mapTask int, reduceTask int) string {
	return fmt.Sprintf("mr-%v-%v", mapTask, reduceTask)
}

func reduceOutputFileName(reduceTask int) string {
	return fmt.Sprintf("mr-out-%v", reduceTask)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
