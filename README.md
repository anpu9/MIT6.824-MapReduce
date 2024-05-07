# MIT6.824-MapReduce

## Introduction

This is the first lab of [MIT 6.824](http://nil.csail.mit.edu/6.824/2021/index.html), distributed system.

This Lab is an implementation of [MapReduce](http://research.google.com/archive/mapreduce-osdi04.pdf), a framework  introduced by Google, which can make programs written in functional styles automatically parallelized and executed in a cluster of comodity machines.

## Key Concepts

Under the hood, this framework consists of one `master` and multiple `worker`, which can be either`Map worker` or `Reduce worker`.

The `Master` will assign as-yet-unstarted tasks and keep track of the progress of these tasks.

As for `workers`, There are two phases:

1. `Map`: the user-defined functions will receives an inputfile split, takes an input pair and produces a set of intermediate key-value pairs `Map (k1,v1) -> list(k2,v2)`. And these buffered pairs will be written into local disks, partitioned into `R` partitons.
2. `Reduce`: When last map task has finished, the worker assigned with reduce tasks will be notified by the Master about these location. It reads remotely the buffered data from local disks, sorts them by intermediate keys and applies them to `reducef`, finally append the output to `R` output files

## Implementations

We're required to implement three major components: `Master`, `Worker`, `RPC`

### Master

Master needs **data structures** that keeps tracks of the state and type for each tasks. And for each finished map tasks, it stores the locations of `R` intermediate files produced by map workers.

The **responsibilities** for `master` are:

1. Assign each unstarted task to a certain worker. Especially, if the worker does not report the task back after an duration (10s here), reassign the task to another worker.
```go
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
```
2. Monitor the progress. Assign Reduce tasks until all map tasks have finished. When all tasks are done, master needs to notify worker to exit
```go
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
```
3. Validate the output. Ensure that nobody observers partially written files in the crashes. Only confirm an output file when it's completely written
```go
newPath := fmt.Sprintf("mr-out-%d", index)
	err = os.Rename(file.Name(), newPath)
```
### RPC

It handles two **data flow directions** between worker and master:

1. `Master -> Worker` : Master assigns an idle task for workers
2. `Worker -> Master` : Workers report the task's progress to the master
```go
/*
`Worker -> Master` : Workers report the task's progress to the master
 */
type ReportTaskArgs struct { 
	WorkerId int
	TaskType TaskType
	TaskId   int
}
type ReportTaskReply struct {
	CanExit bool
}
/*
`Worker -> Master` : Workers report the Reduce task's partition to the master
*/
type BufferArgs struct {
	TaskId   int
	Location string
}
/*
 `Master -> Worker` : Master assigns an idle task for workers
*/
type TaskArgs struct {
	WorkerId int
}
type TaskReply struct {
	Task Task
}
```
### Worker

`worker` is kind of single thread. It keeps requesting new task, processing it either by `mapf` or ``reducef`, report it and exit when `master` sends signal to exit.

```go
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
```