package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"

type Master struct {
	// Your definitions here.
	/*
		For each map task and reduce task, it stores the state (idle, in-progress, or completed),
		and the identity of the worker machine (for non-idle tasks).

		for each completed map task,
		the master stores the locations and sizes of the R intermediate file regions produced by the map task
	*/
	MapTasks    []MapTask    // len = nMap
	ReduceTasks []ReduceTask // len = nReduce
	nMap        int
	nReduce     int
}

type MapTask struct {
	TaskId   int
	Filename string
	State    string
}
type ReduceTask struct {
	TaskId    int
	Partition Partition
	State     string
}
type Partition struct {
	Location string
	Size     int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (m *Master) TaskFinder(args *Args, reply *Reply) error {
	// 1. check if there is any map task
	var isAssign = false

	for _, task := range m.MapTasks {
		// 2. if so, return the filename and index
		if task.State == "idle" {
			task.State = "in-progress"
			reply.MapTask = task
			reply.NReduce = m.nReduce
			isAssign = true
		}
	}
	if !isAssign {
		// 3. if not, find any reduce tasks
		for _, task := range m.ReduceTasks {
			// 4. if so
			if task.State == "idle" {
				task.State = "in-progress"
				reply.ReduceTask = task
			}
		}
	}
	return nil
}
func (m *Master) UpdateDiskLocation(args *BufferArgs, reply *ExampleReply) error {

}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}
func (m *Master) MapDone() bool {
	for _, task := range m.MapTasks {
		for task.State != "completed" {
			time.Sleep(100 * time.Millisecond)
		}
	}
	return true
}

//
// create a Master.
// m := mr.MakeMaster(os.Args[1:], 10)
// the cli is `go run -race mrsequential.go wc.so pg*.txt`
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// get map and reduce function
	// assign M map worker
	// assign R reduce worker
	// Your code here.
	m.nMap = len(files)
	m.nReduce = nReduce

	// initialize map task
	for i, file := range files {
		mapTask := MapTask{
			TaskId:   i,
			State:    "idle",
			Filename: file}
		m.MapTasks = append(m.MapTasks, mapTask)
	}
	// initialize reduce task
	for i := 0; i < m.nReduce; i++ {
		reduceTask := ReduceTask{
			TaskId: i,
			State:  "idle",
		}
		m.ReduceTasks = append(m.ReduceTasks, reduceTask)
	}
	m.server()
	return &m
}
