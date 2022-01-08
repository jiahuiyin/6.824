package mr

import (
	"fmt"
)

type TaskType int64

const (
	Map    TaskType = 1
	Reduce TaskType = 2
)

type Task struct {
	Seq      int
	FileName string
	NReduce  int
	NMap     int
	Type     TaskType
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("./tmp/mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("./out/mr-out-%d", reduceIdx)
}
