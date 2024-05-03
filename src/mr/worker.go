package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
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

// for sorting by key.
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
	// - One way to get started is to modify mr/worker.go's Worker() to send an RPC to the coordinator asking for a task.
	//- Then modify the coordinator to respond with the file name of an as-yet-unstarted map task.

	// uncomment to send the Example RPC to the master.
	//CallExample()
	reply := CallForTask()
	if reply.Identity == "map" {
		MapWorker(reply.MapTask, reply.NReduce, mapf)

	} else {
		ReduceWorker(reply.ReduceTask, reducef)
	}
}

func MapWorker(task MapTask, nReduce int, mapf func(string, string) []KeyValue) {
	/*
		A reasonable naming convention for intermediate files is mr-X-Y,
		where X is the Map task number, and Y is the reduce task number.
	*/
	filename := task.Filename

	// open file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	// read file
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// generate intermediate keys
	intermediate := []KeyValue{}
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	// sort
	sort.Sort(ByKey(intermediate))

	// Periodically, the buffered pairs are written to local disk,
	// partitioned into R regions by the partitioning function.
	// Open or create the file

	i := 0
	X := task.TaskId
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// this is the correct format for each line of Reduce output.
		fmt.Printf("%v %v\n", intermediate[i].Key, values)
		// The map part of your worker can use the ihash(key) function (in worker.go)
		// to pick the reduce task for a given key.
		Y := ihash(intermediate[i].Key) % nReduce
		//  reasonable naming convention for intermediate files is mr-X-Y,
		// where X is the Map task number, and Y is the reduce task number.
		outFilename := fmt.Sprint("mr-%d-%d", X, Y)

		file, err := os.Create(outFilename)
		if err != nil {
			panic(err)
		}
		defer file.Close()
		// Create a JSON encoder
		encoder := json.NewEncoder(file)
		// write json file
		err = encoder.Encode(&values)
		// TODO: update immediate location
		CallNotifyReduceLoc(Y, filename)
		i = j
	}
	task.State = "completed"
}

func ReduceWorker(task ReduceTask, reducef func(string, []string) string) {
	/*
		When a reduce worker is notified by the master about these locations,
		it uses remote procedure calls to read the buffered data from the local disks of the map workers.
		When a reduce worker has read all intermediate data,
		it sorts it by the intermediate keys so that all occurrences of the same key are grouped together.
		The sorting is needed because typically many different keys map to the same reduce task.
	*/
	/*
		Another possibility is for the relevant RPC handler in the coordinator to have a loop that waits, either with time.Sleep() or sync.Cond.
		Go runs the handler for each RPC in its own thread, so the fact that one handler is waiting won't prevent the coordinator from processing other RPCs.
	*/
	// wait for other
	CallIfReduceOk()
	taskId := task.TaskId
	// TODO: read all intermediate file for partition `taskId` mr-*-taskId
	// get location from master
	// read json file
	// write formatted data to outfile `mr-out-X`, one for each reduce task
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() ExampleReply {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
	return reply
}
func CallForTask() Reply {

	// declare an argument structure.
	args := Args{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	call("Master.TaskFinder", &args, &reply)

	fmt.Printf("reply.mapTask %v, reply.reduceTask %v \n", reply.MapTask, reply.ReduceTask)
	return reply
}
func CallIfReduceOk() bool {
	// declare an argument structure.
	args := ExampleArgs{}
	// declare a reply structure.
	reply := IsOKReply{}

	// send the RPC request, wait for the reply.
	call("Master.MapDone", &args, &reply)

	// reply.IsOk should be true.
	return reply.IsOK
}
func CallNotifyReduceLoc(taskId int, filename string) {
	// declare an argument structure.
	args := Partition{}
	// fill in the argument(s).
	args.Location = filename
	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	call("Master.UpdateDiskLocation", &args, &reply)

	fmt.Printf("reply.mapTask %v, reply.reduceTask %v \n", reply.MapTask, reply.ReduceTask)
	return reply
}

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
