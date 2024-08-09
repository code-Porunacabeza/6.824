package mr

//
// RPC definitions. RPC用到的一些配置文件
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
type Task struct {
	TaskType   TaskType //任务类型map 还是 reduce
	TaskId     int      //任务Id
	ReducerNum int      //传入reducer的数量， 用于hash
	Filename   string
	FileSlice  []string //输入文件
}
type TaskArgs struct{}
type TaskType int
type Phase int
type State int

const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask
	ExitTask
)

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

const (
	Working State = iota
	Waiting
	Done
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)
	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("call err")
	}
	return reply
}
