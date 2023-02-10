package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	lastTaskId := 0
	var lastTaskType string
	for {

		args := AssignTaskRequest{
			strconv.Itoa(os.Getpid()),
			lastTaskId,
			lastTaskType,
		}
		reply := AssignTaskReponse{}

		call("Master.Handler", &args, &reply)
		if reply.ToDoTask.Type == TaskMap {
			doMap(reply.ToDoTask, mapf, reply.ReduceCnt)
		} else if reply.ToDoTask.Type == TaskReduce {
			doReduce(reply.ToDoTask, reducef, reply.MapCnt, reply.ReduceCnt)
		}

		lastTaskId = reply.ToDoTask.TaskIndex
		lastTaskType = reply.ToDoTask.Type

	}
}

func doMap(t Task, mapf func(string, string) []KeyValue, reduceCnt int) {
	f, err := os.Open(t.MapFile)
	if err != nil {
		log.Fatalf("Failed to open map input file %s: %e", t.MapFile, err)
	}
	content, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("Failed to read map input file %s: %e", t.MapFile, err)
	}

	kva := mapf(t.MapFile, string(content))
	hashKva := make(map[int][]KeyValue)

	for _, k := range kva {
		h := ihash(k.Key) % reduceCnt
		hashKva[h] = append(hashKva[h], k)
	}

	for i := 0; i < reduceCnt; i++ {
		out, _ := os.Create(tmpMapOutFile(t.WorkerId, t.TaskIndex, i))
		defer out.Close()
		enc := json.NewEncoder(out)
		for _, kv := range hashKva[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Failed to encode reduct task: %e", err)
			}
		}
	}
	for i := 0; i < reduceCnt; i++ {

		err := os.Rename(
			tmpMapOutFile(t.WorkerId, t.TaskIndex, i),
			finalMapOutFile(t.TaskIndex, i))
		if err != nil {
			log.Fatalf(
				"Failed to renmae map output file `%s` as final: %e",
				tmpMapOutFile(t.WorkerId, t.TaskIndex, i), err)
		}
	}
}

func doReduce(t Task, reducef func(string, []string) string, mapCnt int, reduceCnt int) {

	intermediate := []KeyValue{}
	for i := 0; i < mapCnt; i++ {
		f, _ := os.Open(finalMapOutFile(i, t.TaskIndex))
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	out, _ := os.Create(tmpReduceOutFile(t.WorkerId, t.TaskIndex))
	defer out.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(out, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	err := os.Rename(
		tmpReduceOutFile(t.WorkerId, t.TaskIndex),
		finalReduceOutFile(t.TaskIndex))
	if err != nil {
		log.Fatalf(
			"Failed to renmae final output file `%s` as final: %e",
			tmpReduceOutFile(t.WorkerId, t.TaskIndex), err)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {
//
// 	// declare an argument structure.
// 	args := ExampleArgs{}
//
// 	// fill in the argument(s).
// 	args.X = 99
//
// 	// declare a reply structure.
// 	reply := ExampleReply{}
//
// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)
//
// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
