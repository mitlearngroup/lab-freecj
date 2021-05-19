package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

const (
	TaskMap    = "map"
	TaskReduce = "reduce"
)

type Task struct {
	Type      string // map task or reduce task
	TaskIndex int
	WorkerId  string
	MapFile   string // file for map task
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type AssignTaskRequest struct {
	WorkerId     string
	LastTaskId   int
	LastTaskType string
}

type AssignTaskReponse struct {
	ToDoTask  Task
	MapCnt    int
	ReduceCnt int // number of reduce output
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func tmpMapOutFile(worker string, mapId int, reduceId int) string {
	return fmt.Sprintf("tmp-%s-%d-%d", worker, mapId, reduceId)
}

func finalMapOutFile(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func tmpReduceOutFile(worker string, reduceId int) string {
	return fmt.Sprintf("tmp-%s-out-%d", worker, reduceId)
}

func finalReduceOutFile(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}
