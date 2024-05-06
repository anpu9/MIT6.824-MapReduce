package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.
	// - One way to get started is to modify mr/worker.go's Worker() to send an RPC to the coordinator asking for a task.
	//- Then modify the coordinator to respond with the file name of an as-yet-unstarted map task.
	time.Sleep(time.Second)
	reply := CallForTask()
	for reply.Identity != Exit {
		if reply.Identity == Map {
			MapWorker(*reply.MapTask, reply.NReduce, mapf)
		} else if reply.Identity == Reduce {
			ReduceWorker(*reply.ReduceTask, reducef)
		}
		time.Sleep(time.Second)
		reply = CallForTask()
	}

	// there is no other idle tasks
	os.Exit(0)
}

func MapWorker(task MapTask, nReduce int, mapf func(string, string) []KeyValue) {
	/*
		A reasonable naming convention for intermediate files is mr-X-Y,
		where X is the Map task number, and Y is the reduce task number.
	*/

	fmt.Printf("This is %d th Map task started! \n", task.TaskId)
	filename := task.Filename
	// open file
	file, err := os.Open(filename)
	checkError(err, "cannot open %v", filename)
	content, err := ioutil.ReadAll(file)
	checkError(err, "cannot read %v", filename)
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

	// assign reduce task for intermediate file produced by map worker
	for i < len(intermediate) {
		// The map part of your worker can use the ihash(key) function (in worker.go)
		// to pick the reduce task for a given key.
		Y := ihash(intermediate[i].Key) % nReduce
		//  reasonable naming convention for intermediate files is mr-X-Y,
		// where X is the Map task number, and Y is the reduce task number.
		outFilename := fmt.Sprintf("mr-%d-%d", X, Y)

		file, err := os.OpenFile(outFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			panic(err)
		}
		defer file.Close()
		// Create a JSON encoder
		encoder := json.NewEncoder(file)
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		// write json file
		for k := i; k < j; k++ {
			err = encoder.Encode(&intermediate[k])
		}
		//fmt.Printf("The outfile is %v", outFilename)
		// update location
		CallNotifyUpdateDiskLocation(Y, outFilename)
		i = j
	}
	CallNotifyTaskProgress(Map, Done, task.TaskId)
	fmt.Printf("This is %d th Map task completed! ", task.TaskId)
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
	fmt.Printf("This is %d th Reduce task started! \n", task.TaskId)
	// wait for other
	CallIfReduceOk()
	// update
	CallNotifyTaskProgress(Reduce, Assigned, task.TaskId)

	//fmt.Printf("This is %d th Reduce task's partiton! %v \n", task.TaskId, task.Partition)

	var kva []KeyValue
	fmt.Printf("The length of partition is %d \n", len(task.Partition))
	// read all intermediate file for this reduce task in task.Partition
	for _, filename := range task.Partition {
		// Open the file
		file, err := os.Open(filename)
		if err != nil {
			// Handle error if file cannot be opened
			log.Fatalf("cannot open %v", filename)
			panic(err)
		}
		defer file.Close()

		// Create a JSON decoder for the file
		dec := json.NewDecoder(file)

		// Decode each key-value pair from the file
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// Break out of loop when there are no more key-value pairs
				break
			}
			// Append the key-value pair to kva slice
			kva = append(kva, kv)
		}
	}
	// sort by intermediate key
	sort.Sort(ByKey(kva))
	fmt.Printf("This is %d th Reduce task's immediate key-pair length %d! \n", task.TaskId, len(kva))
	// write formatted data to outfile `mr-out-X`, one for each reduce task
	outFilename := fmt.Sprintf("mr-out-%d", task.TaskId)
	fmt.Printf("Reduce number %d is creating the  outputfile % v \n", task.TaskId, outFilename)
	ofile, err := os.Create(outFilename)
	if err != nil {
		panic(err)
	}
	i := 0
	// function reduce func
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()

	CallNotifyTaskProgress(Reduce, Done, task.TaskId)
	fmt.Printf("This is %d th Reduce task completed! \n", task.TaskId)
}

func CallForTask() TaskReply {

	// declare an argument structure.
	args := Args{}

	// fill in the argument(s).
	args.X = os.Getpid()

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	call("Master.TaskFinder", &args, &reply)
	fmt.Printf("The task type is  %v\n", reply.Identity)
	fmt.Printf("reply.mapTask %v, reply.reduceTask %v \n", reply.MapTask, reply.ReduceTask)
	return reply
}
func CallIfReduceOk() bool {
	// declare an argument structure.
	args := Args{}
	// declare a reply structure.
	reply := IsOKReply{}

	// send the RPC request, wait for the reply.
	call("Master.MapDone", &args, &reply)
	// reply.IsOk should be true.
	return reply.IsOK
}
func CallNotifyUpdateDiskLocation(taskId int, filename string) IsOKReply {
	// declare an argument structure.
	args := BufferArgs{}
	// fill in the argument(s).
	args.TaskId = taskId
	args.Location = filename
	// declare a reply structure.
	reply := IsOKReply{}

	// send the RPC request, wait for the reply.
	call("Master.UpdateDiskLocation", &args, &reply)
	return reply
}
func CallNotifyTaskProgress(identity TaskType, state TaskState, taskId int) {
	args := NotificationArg{}
	args.Identity = identity
	args.TaskId = taskId
	args.State = state
	reply := IsOKReply{}
	call("Master.NotifyTaskProgress", &args, &reply)
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
func checkError(err error, format string, v ...interface{}) {
	if err != nil {
		log.Fatalf(format, v)
	}
}
