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
	MapFinished
	ReduceRuning
	ReduceFinished
	Finished
)

var TimeOut int64

type Task struct {
	Id        int
	Status    int
	InputFile string
	ReduceNum int
	StartTime int64
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
	// PrintJobs(job)
	if job.WorkerId == -1 {
		c.RegisterWorker(job, reply)
	}
	switch c.Status {
	case Waiting:
		fmt.Errorf("waiting")
		break
	case MapRunning:
		c.doMap(job, reply)
		break
	case ReduceRuning:
		c.doReduce(job, reply)
		break
	case Finished:
		reply.Status = Finished
		c.WorkerStatus[job.WorkerId] = Finished
		break
	}
	return nil
}
func (c *Coordinator) RegisterWorker(job *Jobs, reply *Reply) {
	c.RegisterMutex.Lock()
	defer c.RegisterMutex.Unlock()
	reply.Job.WorkerId = c.WorkerNum
	job.WorkerId = c.WorkerNum
	reply.Job.TempDir = c.TempDir
	c.WorkerStatus = append(c.WorkerStatus, Waiting)
	c.WorkerNum++
}
func (c *Coordinator) doMap(job *Jobs, reply *Reply) {
	c.DoMapMutex.Lock()
	defer c.DoMapMutex.Unlock()
	if job.Status == MapFinished {
		c.MapTask[job.MapId].Status = MapFinished
		reply.Status = Waiting
		//fmt.Printf("change reply.status to %v\n", reply.Status)
	}
	for i := 0; i < c.MapNum; i++ {
		if c.MapTask[i].Status == Waiting {
			c.MapTask[i].Status = MapRunning
			c.MapTask[i].StartTime = time.Now().Unix()
			reply.Task = c.MapTask[i]
			reply.Status = MapRunning
			reply.Job.MapId = i
			return
		}
	}

	for i := 0; i < c.MapNum; i++ {
		if c.MapTask[i].Status != MapFinished {
			if c.MapTask[i].Status == MapRunning && c.MapTask[i].StartTime <= time.Now().Unix()-10 {
				c.MapTask[i].Status = Waiting
			}
			return
		}
	}

	c.Status = ReduceRuning
}

func (c *Coordinator) doReduce(job *Jobs, reply *Reply) {
	c.DoReduceMutex.Lock()
	defer c.DoReduceMutex.Unlock()
	if job.Status == ReduceFinished {
		c.ReduceTask[job.ReduceId].Status = ReduceFinished
		reply.Status = Waiting
	}
	for i := 0; i < c.ReduceNum; i++ {
		if c.ReduceTask[i].Status == Waiting {
			c.ReduceTask[i].Status = ReduceRuning
			c.ReduceTask[i].StartTime = time.Now().Unix()
			reply.Task = c.ReduceTask[i]
			reply.Status = ReduceRuning
			reply.Job.ReduceId = i
			return
		}
	}
	for i := 0; i < c.ReduceNum; i++ {
		if c.ReduceTask[i].Status != ReduceFinished {
			if c.ReduceTask[i].Status == ReduceRuning && c.ReduceTask[i].StartTime <= time.Now().Unix()-TimeOut {
				c.ReduceTask[i].Status = Waiting
			}
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
	// Your code here.file
	if c.Status != Finished {
		return false
	}
	for i := 0; i < c.ReduceNum; i++ {
		if c.ReduceTask[i].Status != ReduceFinished {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	TimeOut = 10
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
