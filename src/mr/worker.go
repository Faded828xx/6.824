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
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()

}

func (w *worker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	// make an RPC call to the master
	call("Master.RegisterWorker", &args, &reply)
	w.id = reply.WorkerId
	fmt.Println(w.id)
}

func (w *worker) run() {
	for {
		task := w.getTask()
		if !task.Alive {
			DPrintf("worker get task not alive, exit")
			return
		}
		w.doTask(task)
	}

}

func (w *worker) getTask() Task {
	args := TaskArgs{}
	reply := TaskReply{}
	args.WorkerId = w.id
	if ok := call("Master.GetOneTask", &args, &reply); !ok {
		DPrintf("worker get task fail,exit")
		os.Exit(1)
	}
	DPrintf("worker get task:%+v", reply.Task)
	return *reply.Task
}

func (w *worker) reportTask(task Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := ReportArgs{}
	args.WorkerId = w.id
	args.Done = done
	args.Phase = task.Phase
	args.Seq = task.Seq
	reply := ReportReply{}
	call("Master.ReportTask", args, reply)

}

func (w *worker) doTask(task Task) {
	switch task.Phase {
	case MapPhase:
		w.doMapTask(task)
	case ReducePhase:
		w.doReduceTask(task)
	default:
		panic(fmt.Sprintf("task phase err: %v", task.Phase))
	}
}

func (w *worker) doMapTask(task Task) {
	content, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
		return
	}
	kva := w.mapf(task.FileName, string(content)) // 1-D KV array
	// convert kva to 2-D KV array with the len of nReduce
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		intermediate[idx] = append(intermediate[idx], kv)
	}
	// nReduce个KV数组写入到nReduce个中间文件
	for i, l := range intermediate {
		file := intermediateFileName(task.Seq, i)
		ofile, err := os.Create(file)
		if err != nil {
			w.reportTask(task, false, err)
			return
		}
		//for _, kv := range l {
		//fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		//}
		enc := json.NewEncoder(ofile)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(task, false, err)
			}
		}
		if err := ofile.Close(); err != nil {
			w.reportTask(task, false, err)
		}
		w.reportTask(task, true, nil)
	}
}

func (w *worker) doReduceTask(task Task) {
	// 从NMaps个文件里读取KV对 得到若干(k, list(v)) 调用reduce函数后将kv写到结果集
	maps := make(map[string][]string)
	for i := 0; i < task.NMaps; i++ {
		file := intermediateFileName(i, task.Seq)
		ofile, err := os.Open(file)
		if err != nil {
			w.reportTask(task, false, err)
			return
		}
		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	// 将maps的key排序
	keys := make([]string, 0, len(maps))
	for k, _ := range maps {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// 将maps中key和对应的value数组 reducef处理
	res := make([]string, 0, 100)
	for _, k := range keys {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, maps[k])))
	}
	if err := ioutil.WriteFile(outputFileName(task.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(task, false, err)
	}
	w.reportTask(task, true, nil)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
