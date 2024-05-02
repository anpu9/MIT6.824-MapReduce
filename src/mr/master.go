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
	MapTasks    []string // len = nMap + nReduce
	ReduceTasks []string // len = nMap + nReduce
	State       []string
	Identity    []string
	nMap        int
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Index = args.X + 1
	return nil
}
func (m *Master) TaskFinder(args *ExampleArgs, reply *ExampleReply) error {
	// 1. check if there is any map task
	// 2. if so, return the filename and index
	// 3. if not, find any reduce tasks
	// 4. if so,
	reply.Index = args.X + 1
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
		m.MapTasks = append(m.MapTasks, file)
		m.State = append(m.State, "idle")
		m.Identity = append(m.Identity, "idle")
	}

	m.server()
	return &m
}
