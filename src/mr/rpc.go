package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Jobs struct {
	WorkerId int
	TempDir  string
	Status   int
	MapId    int
	ReduceId int
}

type Reply struct {
	Task   Task
	Status int
	Job    Jobs
}

func PrintJobs(job *Jobs) {
	fmt.Printf("\n======================\n")
	fmt.Printf("WorkerId:%v\nTempDIr:%v\nStatus:%v\nMapId%v\nReduceId:%v\n", job.WorkerId, job.TempDir, job.Status, job.MapId, job.ReduceId)
	fmt.Printf("======================\n")
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
