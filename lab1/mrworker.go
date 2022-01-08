package main

import (
	"github.com/jiahuiyin/6.824/lab1/mr"
	"github.com/jiahuiyin/6.824/lab1/mrapp"
)

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one master.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

func main() {
	mapF := mrapp.Map
	reduceF := mrapp.Reduce
	mr.Worker(mapF, reduceF)
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
