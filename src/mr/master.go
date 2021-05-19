package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	lk            sync.Mutex
	status        string
	taskState     map[string]Task
	taskAvailable chan Task
	nMap          int
	nReduce       int
	nTask         int
}

func genKey(t string, i int) string {
	return fmt.Sprintf("%s-%d", t, i)
}

func (m *Master) trasistion() {
	if m.status == TaskMap {
		m.status = TaskReduce
		for i := 0; i < m.nReduce; i++ {
			t := Task{
				Type:      TaskReduce,
				TaskIndex: i,
			}
			m.taskState[genKey(TaskReduce, i)] = t
			m.taskAvailable <- t
		}

	} else if m.status == TaskReduce {
		m.status = ""
		close(m.taskAvailable)
	}

}
func (m *Master) Handler(args *AssignTaskRequest, reply *AssignTaskReponse) error {

	if args.LastTaskType != "" {
		key := genKey(args.LastTaskType, args.LastTaskId)
		m.lk.Lock()
		defer m.lk.Unlock()
		if val, ok := m.taskState[key]; ok && val.WorkerId == args.WorkerId {
			delete(m.taskState, key)

			if len(m.taskState) == 0 {
				m.trasistion()
			}
		}
		return nil
	}

	t := <-m.taskAvailable
	key := genKey(t.Type, t.TaskIndex)
	t.WorkerId = args.WorkerId

	reply.MapCnt = m.nMap
	reply.ReduceCnt = m.nReduce
	reply.ToDoTask = t

	m.lk.Lock()
	defer m.lk.Unlock()
	m.taskState[key] = t

	go func() {

		time.Sleep(10 * time.Second)
		m.lk.Lock()
		defer m.lk.Unlock()
		if _, ok := m.taskState[key]; ok {
			t.WorkerId = ""
			m.taskState[key] = t
			m.taskAvailable <- t
		}

	}()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
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
	m.lk.Lock()
	defer m.lk.Unlock()
	return m.status == ""
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		status:        TaskMap,
		taskState:     make(map[string]Task),
		taskAvailable: make(chan Task, len(files)*nReduce),
		nMap:          (len(files)),
		nReduce:       (nReduce),
	}

	for i, f := range files {
		t := Task{
			Type:      TaskMap,
			TaskIndex: i,
			MapFile:   f, // file for map task
		}
		m.taskState[genKey(TaskMap, i)] = t
		m.taskAvailable <- t
	}

	// Your code here.
	m.server()

	return &m
}
