package mr

import (
	"fmt"
)

type TaskPhase int64

const (
	MapPhase    TaskPhase = 1
	ReducePhase TaskPhase = 2
)

type Task struct {
	FileName string
	NReduce  int
	NMaps    int
	Seq      int
	Phase    TaskPhase
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
