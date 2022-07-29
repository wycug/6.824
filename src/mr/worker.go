package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
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
	for {
		call("Coordinator.Coordinate", &job, &reply)
		fmt.Printf("%v,%v\n", reply.Status, reply.Task.InputFile)
		if job.WorkerId == -1 {
			job = reply.Job
		}
		switch reply.Status {
		case MapRunning:
			mapHandler(mapf)
			break
		case ReduceRuning:
			reduceHandler(reducef)
			break
		case Finished:
			fmt.Println("worker Finished")
			return
		}
		time.Sleep(2 * time.Second)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func mapHandler(mapf func(string, string) []KeyValue) {

	filename := reply.Task.InputFile
	file, err := os.Open(fmt.Sprintf("%v", filename))
	defer file.Close()
	if err != nil {
		nowPath, _ := os.Getwd()
		log.Printf("%v", nowPath)
		log.Fatalf("cannot open %v, err:%v", filename, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content))
	writeMapResultToTemp(kva)
}
func writeMapResultToTemp(kva []KeyValue) {
	files := make([]*os.File, reply.Task.ReduceNum)
	buffers := make([]*bufio.Writer, 0, reply.Task.ReduceNum)
	encoders := make([]*json.Encoder, 0, reply.Task.ReduceNum)
	for i := 0; i < reply.Task.ReduceNum; i++ {
		filePath := fmt.Sprintf("%v/temp-%v-%v", job.TempDir, job.WorkerId, i)
		file, err := os.Create(filePath)
		if err != nil {
			log.Fatalf("%v Cannot create file %v\n", err, filePath)
		}
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
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
	//for i, file := range files {
	//	file.Close()
	//	newPath := fmt.Sprintf("temp/%v-%v", reply.Task.WorkerId, i)
	//	os.Rename(file.Name(), newPath)
	//}
}
func reduceHandler(reducef func(string, []string) string) {

	files, err := filepath.Glob(fmt.Sprintf("%v/temp-%v-%v", job.TempDir, "*", reply.Task.Id))
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
}
func writeReduceResultToOutput(reducef func(string, []string) string, kvMap map[string][]string) {

	// sort the kv map by key
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create temp file
	oname := fmt.Sprintf("mr-out-%v", reply.Task.Id)
	file, err := os.Create(oname)
	defer file.Close()
	if err != nil {
		log.Fatalf("Cannot create file %v, err:%v\n", oname, err)
	}

	// Call reduce and write to temp file
	for _, k := range keys {
		v := reducef(k, kvMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, reducef(k, kvMap[k]))
		if err != nil {
			log.Fatalf("Cannot write mr output (%v, %v) to file, err:%v\n", k, v, err)
		}

	}

	// atomically rename temp files to ensure no one observes partial files

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
