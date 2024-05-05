package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

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
	mu          sync.Mutex
}

type MapTask struct {
	TaskId   int
	Filename string
	State    string
}
type ReduceTask struct {
	TaskId    int
	Partition []string
	State     string
}

func (m *Master) TaskFinder(args *Args, reply *TaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Println("Finding a Map Task.... \n")
	var isAssign = false
	for _, task := range m.MapTasks {
		// 2. if so, return the filename and index
		if task.State == "idle" {
			task.State = "assigned"
			reply.MapTask = task
			reply.NReduce = m.nReduce
			reply.Identity = "map"
			isAssign = true
			fmt.Printf("Map Task Find! The number of Map task: %d \n", task.TaskId)
			break
		}
	}
	// 3. if not, find any reduce tasks
	if !isAssign {
		fmt.Println("finding a Reduce Task.... \n")
		for _, task := range m.ReduceTasks {
			// 4. if so
			if task.State == "idle" {
				task.State = "assigned"
				reply.ReduceTask = task
				reply.Identity = "reduce"
				isAssign = true
				fmt.Printf("Reduce Task Find! The number of Reduce task: %d \n", task.TaskId)
				break
			}
		}
	}

	if !isAssign {
		fmt.Println("All Tasks have been assignment, wait for Done() \n")
		reply.Identity = "exit"
	}

	return nil
}
func (m *Master) UpdateDiskLocation(args *BufferArgs, reply *IsOKReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	reduceTaskId := args.TaskId
	location := args.Location

	// Check if the location already exists in the partition
	exists := false
	for _, loc := range m.ReduceTasks[reduceTaskId].Partition {
		if loc == location {
			exists = true
			break
		}
	}

	// If the location doesn't exist, append it
	if !exists {
		m.ReduceTasks[reduceTaskId].Partition = append(m.ReduceTasks[reduceTaskId].Partition, location)
		fmt.Printf("Now this partition is %v\n", m.ReduceTasks[reduceTaskId].Partition)
	}

	reply.IsOK = true
	return nil
}
func (m *Master) NotifyTaskProgress(args *NotificationArg, reply *IsOKReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskId := args.TaskId
	taskType := args.Identity
	taskState := args.State

	switch taskType {
	case "map":
		m.MapTasks[taskId].State = taskState
	case "reduce":
		m.ReduceTasks[taskId].State = taskState
	default:
		// Handle unknown task type
		return fmt.Errorf("unknown task type: %s", taskType)
	}

	reply.IsOK = true
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
	ret := true // Assume everything is completed by default
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, task := range m.MapTasks {
		if task.State != "completed" {
			ret = false // If any map task is not completed, set ret to false
			break
		}
	}

	if ret { // Only check reduce tasks if all map tasks are completed
		for _, task := range m.ReduceTasks {
			if task.State != "completed" {
				ret = false // If any reduce task is not completed, set ret to false
				break
			}
		}
	}

	return ret // Return the final result
}
func (m *Master) MapDone(args *Args, reply *IsOKReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Use a condition variable to wait for map tasks to be completed
	for _, task := range m.MapTasks {
		for task.State != "completed" {
			// Release the lock to allow other goroutines to acquire it
			m.mu.Unlock()

			// Sleep for a short duration before checking again
			time.Sleep(100 * time.Millisecond)

			// Reacquire the lock before checking the state again
			m.mu.Lock()
		}
	}
	reply.IsOK = true
	return nil
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
	m.mu.Lock()
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
	fmt.Printf("After Map tasks Initialization. The number of Map task: %d \n", len(m.MapTasks))
	// initialize reduce task
	for i := 0; i < m.nReduce; i++ {
		reduceTask := ReduceTask{
			TaskId:    i,
			State:     "idle",
			Partition: make([]string, 0),
		}
		m.ReduceTasks = append(m.ReduceTasks, reduceTask)
	}
	fmt.Printf("After Map tasks Initialization. The number of Reduce task: %d \n", len(m.ReduceTasks))
	m.mu.Unlock()
	m.server()
	return &m
}
