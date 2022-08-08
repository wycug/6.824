package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
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
var (
	job   Jobs
	reply Reply
)

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	job = Jobs{}
	reply = Reply{}
	job.WorkerId = -1
	job.Status = Waiting
	for {
		call("Coordinator.Coordinate", &job, &reply)
		if job.WorkerId == -1 {
			register()
		}
		job.Status = reply.Status
		switch job.Status {
		case MapRunning:
			mapHandler(mapf)
			break
		case ReduceRuning:
			reduceHandler(reducef)
			break
		case Finished:
			fmt.Printf("worker %v Finished\n", job.WorkerId)
			return
		}

		// job1 := job
		// reply1 := reply
		// fmt.Printf("%v,%v, %v\n", reply1.Status, reply1.Task.InputFile, job1.Status)

		// time.Sleep(time.Second)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func mapHandler(mapf func(string, string) []KeyValue) {

	job.MapId = reply.Job.MapId
	filename := reply.Task.InputFile
	file, err := os.Open(fmt.Sprintf("%v", filename))

	if err != nil {
		nowPath, _ := os.Getwd()
		log.Printf("%v", nowPath)
		log.Fatalf("cannot open %v, err:%v", filename, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	writeMapResultToTemp(kva)
	job.Status = MapFinished
}
func writeMapResultToTemp(kva []KeyValue) {
	files := make([]*os.File, reply.Task.ReduceNum)
	buffers := make([]*bufio.Writer, reply.Task.ReduceNum)
	encoders := make([]*json.Encoder, reply.Task.ReduceNum)
	for i := 0; i < reply.Task.ReduceNum; i++ {
		file, err := ioutil.TempFile(job.TempDir, "tmp")
		if err != nil {
			log.Fatalf("Cannot create temp file err:%v\n", err)
		}
		buf := bufio.NewWriter(file)
		files[i] = file
		buffers[i] = buf
		encoders[i] = json.NewEncoder(buf)
	}
	// write map outputs to temp files
	for _, kv := range kva {
		idx := ihash(kv.Key) % reply.Task.ReduceNum
		encoders[idx].Encode(&kv)

	}

	// flush file buffer to disk
	for _, buf := range buffers {
		buf.Flush()
	}
	// atomically rename temp files to ensure no one observes partial files
	for i, file := range files {
		filePath := fmt.Sprintf("%v/temp-%v-%v", job.TempDir, job.MapId, i)
		os.Rename(file.Name(), filePath)
		file.Close()
	}
}
func reduceHandler(reducef func(string, []string) string) {
	job.ReduceId = reply.Job.ReduceId
	files, err := filepath.Glob(fmt.Sprintf("%v/temp-%v-%v", job.TempDir, "*", job.ReduceId))
	if err != nil {
		log.Fatalf("Cannot list reduce files, %v", err)
	}
	kvMap := make(map[string][]string)
	var kv KeyValue
	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("Cannot open file %v, err:%v\n", filePath, err)
		}

		dec := json.NewDecoder(file)
		for dec.More() {
			err = dec.Decode(&kv)
			if err != nil {
				log.Fatalf("Cannot decode from file %v\n, err:%v", filePath, err)
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	writeReduceResultToOutput(reducef, kvMap)
	job.Status = ReduceFinished
}
func writeReduceResultToOutput(reducef func(string, []string) string, kvMap map[string][]string) {

	// sort the kv map by key
	var keys []string
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create temp file

	file, err := ioutil.TempFile("./", "tmp-mr-")
	defer file.Close()
	if err != nil {
		log.Fatalf("Cannot create temp file , err:%v\n", err)
	}

	// Call reduce and write to temp file
	for _, k := range keys {
		v := reducef(k, kvMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, v)
		if err != nil {
			log.Fatalf("Cannot write mr output (%v, %v) to file, err:%v\n", k, v, err)
		}

	}

	// atomically rename temp files to ensure no one observes partial files
	oname := fmt.Sprintf("mr-out-%v", job.ReduceId)
	os.Rename(file.Name(), oname)
}

func register() {
	job.WorkerId = reply.Job.WorkerId
	job.TempDir = reply.Job.TempDir
}

func mapFinished() {

}

func reduceFinished() {

}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
