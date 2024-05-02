package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

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
type Task struct {
	State    string
	Identity string
}
type MapTask struct {
	Task     Task
	Filename string
}
type ReduceTask struct {
	Task      Task
	Partition Partition
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
		if task.Task.State == "idle" {
			task.Task.State = "in-progress"
			task.Task.Identity = "map"
			reply.MapTask = task
			isAssign = true
		}
	}
	if !isAssign {
		// 3. if not, find any reduce tasks
		for _, task := range m.ReduceTasks {
			// 4. if so
			if task.Task.State == "idle" {
				task.Task.State = "in-progress"
				task.Task.Identity = "reduce"
				reply.ReduceTask = task
			}
		}
	}
	return nil
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

	// initialize
	for _, file := range files {
		mapTask := MapTask{Task: Task{
			State:    "idle",
			Identity: "map",
		}, Filename: file}
		m.MapTasks = append(m.MapTasks, mapTask)
	}

	m.server()
	return &m
}
