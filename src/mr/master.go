package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

/*
For each map task and reduce task, it stores the state (idle, in-progress, or completed),
and the identity of the worker machine (for non-idle tasks).

for each completed map task,
the master stores the locations and sizes of the R intermediate file regions produced by the map task
*/
type TaskStatus int
type TaskType int

const TempDir = "tmp"
const TaskTimeout = 10
const (
	Idle TaskStatus = iota
	Assigned
	Done
)
const (
	Map TaskType = iota
	Reduce
	Exit
	NoTask
)

type Task struct {
	Type      TaskType
	Status    TaskStatus
	Index     int
	File      string
	WorkerId  int
	Partition []string
}

type Master struct {
	// Your definitions here.
	Mu          sync.Mutex
	MapTasks    []Task
	ReduceTasks []Task
	nMap        int
	nReduce     int
}

func (m *Master) AssignTask(args *TaskArgs, reply *TaskReply) error {
	//fmt.Printf("The process %d is trying to get Lock \n", args.WorkerId)
	m.Mu.Lock()
	var task *Task
	if m.nMap > 0 {
		//fmt.Println("Finding a Map Task.... \n")
		task = m.selectTask(m.MapTasks, args.WorkerId)
	} else if m.nReduce > 0 {
		//fmt.Println("Finding a Reduce Task.... \n")
		task = m.selectTask(m.ReduceTasks, args.WorkerId)
	} else {
		fmt.Println("All Tasks have been assigned, wait for Done() \n")
		task = &Task{
			Type:      Exit,
			Status:    Done,
			Index:     0,
			File:      "",
			WorkerId:  0,
			Partition: nil,
		}
	}
	//fmt.Printf("Find the task is %v \n", *task)
	reply.Task = *task
	m.Mu.Unlock()
	go m.waitForTask(task)
	return nil
}
func (m *Master) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	taskType := args.TaskType
	var task *Task
	if taskType == Map {
		task = &m.MapTasks[args.TaskId]
	} else {
		task = &m.ReduceTasks[args.TaskId]
	}
	if task.WorkerId == args.WorkerId && task.Status == Assigned {
		task.Status = Done
		if taskType == Map && m.nMap > 0 {
			//fmt.Printf("Map Task %d finished! \n", args.TaskId)
			m.nMap--
		} else if taskType == Reduce && m.nReduce > 0 {
			//fmt.Printf("Reduce Task %d finished! \n", args.TaskId)
			m.nReduce--
		}
	}
	reply.CanExit = m.nMap == 0 && m.nReduce == 0

	return nil
}
func (m *Master) UpdateDiskLocation(args *BufferArgs, reply *IsOKReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
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
		//fmt.Printf("Now this partition is %v\n", m.ReduceTasks[reduceTaskId].Partition)
	}

	reply.IsOK = true
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	ret := m.nMap == 0 && m.nReduce == 0 // Assume everything is completed by default
	return ret                           // Return the final result
}

func (m *Master) selectTask(taskList []Task, id int) *Task {
	var task *Task
	for i := 0; i < len(taskList); i++ {
		if taskList[i].Status == Idle {
			task = &taskList[i]
			task.Status = Assigned
			task.WorkerId = id
			return task
		}
	}
	task = &Task{
		Type:      NoTask,
		Status:    Assigned,
		Index:     0,
		File:      "",
		WorkerId:  0,
		Partition: nil,
	}
	return task
}
func (m *Master) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	reply.ReduceCount = len(m.ReduceTasks)

	return nil
}

func (m *Master) waitForTask(task *Task) {
	if task.Type != Map && task.Type != Reduce {
		return
	}
	<-time.After(TaskTimeout * time.Second)
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if task.Status == Assigned {
		task.Status = Idle
		task.WorkerId = -1
		fmt.Println("Task timeout, reset task status: ", *task)
	}
}

// create a Master.
// m := mr.MakeMaster(os.Args[1:], 10)
// the cli is `go run -race mrsequential.go wc.so pg*.txt`
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// get map and reduce function
	// assign M map worker
	// assign R reduce worker
	// Your code here.
	m.nMap = len(files)
	m.nReduce = nReduce
	m.MapTasks = make([]Task, 0, m.nMap)
	m.ReduceTasks = make([]Task, 0, m.nReduce)

	// initialize map task
	for i := 0; i < m.nMap; i++ {
		mTask := Task{
			Type:      Map,
			Status:    Idle,
			Index:     i,
			File:      files[i],
			WorkerId:  -1,
			Partition: nil,
		}
		m.MapTasks = append(m.MapTasks, mTask)
	}
	fmt.Printf("After Map tasks Initialization. The number of Map task: %d \n", len(m.MapTasks))
	// initialize reduce task
	for i := 0; i < m.nReduce; i++ {
		reduceTask := Task{
			Type:      Reduce,
			Status:    Idle,
			Index:     i,
			File:      "",
			WorkerId:  -1,
			Partition: make([]string, 0, m.nMap),
		}
		m.ReduceTasks = append(m.ReduceTasks, reduceTask)
	}
	fmt.Printf("After Reduce tasks Initialization. The number of Reduce task: %d \n", len(m.ReduceTasks))

	// clean up and create temp directory
	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}

	m.server()
	return &m
}
