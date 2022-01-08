package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

type TaskStatus int64

const (
	Ready   TaskStatus = 0
	Finish  TaskStatus = 1
	Running TaskStatus = 2
)

type Master struct {
	// Your definitions here.
	mu               *sync.Mutex
	done             bool
	noTask           bool
	files            []string
	mapTaskStatus    []TaskStatus
	mapDone          bool
	reduceTaskStatus []TaskStatus
	nReduce          int
	taskCh           chan *Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) TaskHandler(args *TaskArgs, reply *TaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Println(123123123)
	done := true
	for _, v := range m.reduceTaskStatus {
		if v == Ready {
			done = false
		}
	}
	if done {
		reply.Done = true

	} else {
		task := <-m.taskCh
		if task.Type == Map {
			m.mapTaskStatus[task.Seq] = Running
		} else {
			m.reduceTaskStatus[task.Seq] = Running
		}
		reply.Done = false
		reply.Task = task

	}
	return nil
}

func (m *Master) ReportTaskHandler(args *ReportTaskArgs, reply *ReportTaskReply) error {

	if args.Type == Map {
		m.mapTaskStatus[args.Seq] = Finish
		for _, v := range m.mapTaskStatus {
			if v == Running || v == Ready {
				return nil
			}
		}
		m.mapDone = true

		for i := 0; i < m.nReduce; i++ {
			m.taskCh <- &Task{
				Seq:     i,
				NReduce: m.nReduce,
				NMap:    len(m.files),
				Type:    Reduce,
			}
		}
	} else {
		m.reduceTaskStatus[args.Seq] = Finish
		for _, v := range m.reduceTaskStatus {
			if v == Running || v == Ready {
				return nil
			}
		}
		m.done = true
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "127.0.0.1:1234")
	//sockname := masterSock()
	//os.Remove(sockname)
	//l, e := net.Listen("tcp", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:   files,
		nReduce: nReduce,
		mu:      &sync.Mutex{},
	}
	m.reduceTaskStatus = make([]TaskStatus, nReduce, nReduce)
	m.mapTaskStatus = make([]TaskStatus, len(files), len(files))

	m.taskCh = make(chan *Task, int(math.Max(float64(len(files)), float64(nReduce))))
	for k, file := range files {
		task := &Task{
			Seq:      k,
			FileName: file,
			NReduce:  nReduce,
			Type:     Map,
		}
		m.taskCh <- task
	}

	m.server()
	return &m
}
