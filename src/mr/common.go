package mr

import (
	"fmt"
	"log"
)

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

const Debug = true

func DPrintf(format string, v ...interface{}) {
	if Debug {
		log.Printf(format+"\n", v...)
	}
}

type Task struct {
	FileName string    // the name of input file
	NReduce  int       // the number of reduce tasks, as "R" in the paper, not the number of reduce workers
	NMaps    int       // the number of map tasks, "M"
	Seq      int       // the sequence of tasks
	Phase    TaskPhase // the task is in map/reduce phase
	Alive    bool      // worker should exit if Alive is false
}

func intermediateFileName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func outputFileName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
