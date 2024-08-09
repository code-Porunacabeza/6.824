package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var intermediate []KeyValue
	filename := response.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	err = file.Close()
	if err != nil {
		return
	}
	//map
	intermediate = mapf(filename, string(content))
	rn := response.ReducerNum

	HashedKV := make([][]KeyValue, rn)
	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
}

func callDone() Task {
	args := Task{}
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &args, &reply)
	if ok {
		fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func DoReduceTask(reducef func(string, []string) string, response *Task) {
	reduceFileNum := response.TaskId
	intermediate := shuffle(response.FileSlice)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("cannot create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)
}

func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	//sort.Sort(SortedKey(kva))
	return kva
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// mapf map函数，返回值是keyvalue的数组
	// reducef reduce函数，返回值是string
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	keepFlag := true
	for keepFlag {
		task := GetTask()      //向协调者请求任务
		switch task.TaskType { //根据任务类型做相关操作
		case MapTask:
			{
				DoMapTask(mapf, &task) //去做map task，这里keepFlag为true，如果可以获取到task就一直循环获取，做任务
				callDone()
			}
		case WaittingTask:
			{
				fmt.Println("All tasks are in progress, please wait...")
				time.Sleep(time.Second)
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				callDone()
			}
		case ExitTask:
			{
				fmt.Println("Task about:[", task.TaskId, "] is terminated...")
				keepFlag = false //结束任务
			}
		default:
			panic("unhandled default case")
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
