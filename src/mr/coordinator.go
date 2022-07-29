package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	MapTask       []Task
	ReduceTask    []Task
	MapNum        int
	ReduceNum     int
	WorkerNum     int
	FreeWorkers   []int
	Workers       int
	RegisterMutex sync.Mutex
	DoMapMutex    sync.Mutex
	DoReduceMutex sync.Mutex
	WorkerStatus  []int
	Status        int
	TempDir       string
}

const (
	Waiting = iota
	MapRunning
	ReduceRuning
	Finished
)

type Task struct {
	Id        int
	Status    int
	InputFile string
	ReduceNum int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Coordinate(job *Jobs, reply *Reply) error {
	if job.WorkerId == -1 {
		c.RegisterWorker(job, reply)
	}
	switch c.Status {
	case Waiting:
		fmt.Errorf("waiting")
		reply.Status = Waiting
		break
	case MapRunning:
		c.doMap(job, reply)
		break
	case ReduceRuning:
		c.doReduce(job, reply)
		break
	case Finished:
		reply.Status = Finished
		break
	}
	fmt.Printf("coordinator : %v\n", c.Status)
	return nil
}
func (c *Coordinator) RegisterWorker(job *Jobs, reply *Reply) {
	c.RegisterMutex.Lock()
	defer c.RegisterMutex.Unlock()
	reply.Job.WorkerId = c.WorkerNum
	reply.Job.TempDir = c.TempDir
	c.WorkerNum++
}
func (c *Coordinator) doMap(job *Jobs, reply *Reply) {
	c.DoMapMutex.Lock()
	defer c.DoMapMutex.Unlock()
	for i := 0; i < c.MapNum; i++ {
		if c.MapTask[i].Status == Waiting {
			c.MapTask[i].Status = MapRunning
			reply.Task = c.MapTask[i]
			reply.Status = MapRunning
			return
		}
	}
	c.Status = ReduceRuning
}

func (c *Coordinator) doReduce(job *Jobs, reply *Reply) {
	c.DoReduceMutex.Lock()
	defer c.DoReduceMutex.Unlock()
	for i := 0; i < c.ReduceNum; i++ {
		if c.ReduceTask[i].Status == Waiting {
			c.ReduceTask[i].Status = ReduceRuning
			reply.Task = c.ReduceTask[i]
			reply.Status = ReduceRuning
			return
		}
	}
	c.Status = Finished
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.file
	if c.Status == Finished {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println(files)
	c := Coordinator{}

	// Your code here.
	c.Status = Waiting
	tempDir := fmt.Sprintf("temp-%v", time.Now())
	err := os.Mkdir(tempDir, os.ModePerm)
	if err != nil {
		log.Fatalf("cant mkdir %v", tempDir)
	}
	c.TempDir = tempDir
	c.MapNum = len(files)
	c.ReduceNum = nReduce
	c.MapTask = make([]Task, c.MapNum)
	c.ReduceTask = make([]Task, c.ReduceNum)
	for i := 0; i < c.MapNum; i++ {
		c.MapTask[i] = Task{
			Id:        i,
			Status:    Waiting,
			InputFile: files[i],
			ReduceNum: nReduce,
		}
	}
	for i := 0; i < c.ReduceNum; i++ {
		c.ReduceTask[i] = Task{
			Id:        i,
			Status:    Waiting,
			ReduceNum: nReduce,
		}
	}

	c.Status = MapRunning
	c.server()
	return &c
}
