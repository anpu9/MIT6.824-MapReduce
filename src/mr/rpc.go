package mr

import "os"
import "strconv"

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

type IsOKReply struct {
	IsOK bool
}
type GetReduceCountArgs struct {
}

type GetReduceCountReply struct {
	ReduceCount int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
