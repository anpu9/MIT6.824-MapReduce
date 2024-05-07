package mr

import (
	"bufio"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var nReduce int

const TaskInterval = 200

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	n, succ := getReduceCount()
	if succ == false {
		fmt.Println("Failed to get reduce task count, worker exiting.")
		return
	}
	nReduce = n

	for {
		reply, succ := CallForTask()
		if succ == false {
			fmt.Println("Failed to contact master, worker exiting.")
			return
		}
		exit, succ := false, true
		if reply.Task.Type == Map {
			MapWorker(reply.Task, mapf)
			exit, succ = ReportTaskDone(Map, reply.Task.Index)
		} else if reply.Task.Type == Reduce {
			ReduceWorker(reply.Task, reducef)
			exit, succ = ReportTaskDone(Reduce, reply.Task.Index)
		} else if reply.Task.Type == NoTask {
			// (map/all) tasks have been assigned, but still working
		} else {
			// exit, all task has finished
			return
		}
		if exit || !succ {
			fmt.Println("Master exited or all tasks done, worker exiting.")
			return
		}
		time.Sleep(TaskInterval * time.Millisecond)
	}

}

func ReportTaskDone(taskType TaskType, index int) (bool, bool) {
	args := ReportTaskArgs{os.Getpid(), taskType, index}
	reply := ReportTaskReply{}
	succ := call("Master.ReportTaskDone", &args, &reply)
	return reply.CanExit, succ
}

func getReduceCount() (int, bool) {
	args := GetReduceCountArgs{}
	reply := GetReduceCountReply{}
	succ := call("Master.GetReduceCount", &args, &reply)
	return reply.ReduceCount, succ
}

func MapWorker(task Task, mapf func(string, string) []KeyValue) {
	fmt.Printf("This is %d th Map task started! \n", task.Index)
	filename := task.File
	// open file
	file, err := os.Open(filename)
	checkError(err, "cannot open %v", filename)
	content, err := ioutil.ReadAll(file)
	checkError(err, "cannot read %v", filename)
	err = file.Close()
	checkError(err, "cannot close %v", filename)
	// generate intermediate keys
	kva := mapf(filename, string(content))
	WriteMapOutput(kva, task.Index)
}

func WriteMapOutput(kva []KeyValue, index int) {
	// use io buffers to reduce disk I/O, which greatly improves
	// performance when running in containers with mounted volumes
	prefix := fmt.Sprintf("%v/mr-%d", TempDir, index)
	files := make([]*os.File, 0, nReduce)        // disks
	buffers := make([]*bufio.Writer, 0, nReduce) // buffers
	encodes := make([]*json.Encoder, 0, nReduce) // encoders
	// create temp files, use pid to identity this unique worker
	for i := 0; i < nReduce; i++ {
		filePath := fmt.Sprintf("%v-%d-%d", prefix, i, os.Getpid())
		file, err := os.Create(filePath)
		checkError(err, "cannot create %v", filePath)
		buffer := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buffer)
		encodes = append(encodes, json.NewEncoder(buffer))
	}
	// write to buffer
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		err := encodes[idx].Encode(&kv)
		checkError(err, "cannot encode %v", kv.Key)
	}
	// flush buffer file to disk
	for i, buf := range buffers {
		err := buf.Flush()
		checkError(err, "cannot flush %v", files[i])
	}
	// atomically rename temp files to ensure no one observes partial files
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%d", prefix, i)
		err := os.Rename(file.Name(), newPath)
		CallNotifyUpdateDiskLocation(i, newPath)
		checkError(err, "cannot rename %v", newPath)
	}
}

func ReduceWorker(task Task, reducef func(string, []string) string) {
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
	fmt.Printf("This is %d th Reduce task started! \n", task.Index)

	//fmt.Printf("This is %d th Reduce task's partiton! %v \n", task.Index, task.Partition)

	//fmt.Printf("The length of partition is %d \n", len(task.Partition))
	// read all intermediate file for this reduce task in task.Partition
	kvMap := make(map[string][]string)
	var kv KeyValue
	// read all intermediate for a reduce task
	for _, filename := range task.Partition {
		// Open the file
		file, err := os.Open(filename)
		checkError(err, "cannot open %v", filename)
		decoder := json.NewDecoder(file)
		for decoder.More() {
			err := decoder.Decode(&kv)
			checkError(err, "cannot decode %v", filename)
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
		file.Close()
	}
	WriteReduceOutput(reducef, kvMap, task.Index)
}

func WriteReduceOutput(reducef func(string, []string) string, kvMap map[string][]string, index int) {
	// sort the kv map by key
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// Create temp file
	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, index, os.Getpid())
	file, err := os.Create(filePath)
	checkError(err, "Cannot create file %v\n", filePath)
	for _, k := range keys {
		output := reducef(k, kvMap[k])
		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(file, "%v %v\n", k, output)
		checkError(err, "Cannot write to mr output file %v\n", filePath)
	}

	// atomically rename temp files to ensure no one observes partial files
	file.Close()
	newPath := fmt.Sprintf("mr-out-%d", index)
	err = os.Rename(file.Name(), newPath)
	checkError(err, "cannot rename %v", newPath)
}

func CallForTask() (*TaskReply, bool) {
	args := TaskArgs{os.Getpid()}
	// declare a reply structure.
	reply := TaskReply{}
	// send the RPC request, wait for the reply.
	succ := call("Master.AssignTask", &args, &reply)
	//fmt.Printf("The task type is  %v\n", reply.Identity)
	//fmt.Printf("reply.mapTask %v, reply.reduceTask %v \n", reply.MapTask, reply.ReduceTask)
	return &reply, succ
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
