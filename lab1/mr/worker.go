package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapFun func(string, string) []KeyValue, reduceFun func(string, []string) string) {

	for true {
		arg := &TaskArgs{}
		var reply TaskReply
		call("Master.TaskHandler", arg, &reply)
		if reply.Done {
			return
		}
		task := reply.Task
		if task.Type == Map {
			file, err := os.Open(task.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", task.FileName)
			}
			content, err := ioutil.ReadAll(file)
			kvs := mapFun(task.FileName, string(content))
			kvss := make([][]KeyValue, task.NReduce, task.NReduce)
			for _, kv := range kvs {
				idx := ihash(kv.Key) % task.NReduce
				kvss[idx] = append(kvss[idx], kv)
			}
			for idx, kvs := range kvss {
				fileName := reduceName(task.Seq, idx)
				f, _ := os.Create(fileName)

				enc := json.NewEncoder(f)
				for _, kv := range kvs {
					enc.Encode(&kv)
				}
				f.Close()
			}
			rarg := &ReportTaskArgs{
				Done: true,
				Seq:  task.Seq,
				Type: task.Type,
			}
			rreply := &ReportTaskReply{}
			call("Master.ReportTaskHandler", rarg, rreply)
		} else {

		}
	}

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
	//	sockname := masterSock()
	//	c, err := rpc.DialHTTP("unix", sockname)
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
