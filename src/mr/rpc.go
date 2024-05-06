package mr

import "os"
import "strconv"

type Args struct {
	X int
}
type NotificationArg struct {
	TaskId   int
	State    TaskState
	Identity TaskType
}
type BufferArgs struct {
	TaskId   int
	Location string
}
type TaskReply struct {
	Identity   TaskType
	MapTask    *MapTask
	ReduceTask *ReduceTask
	NReduce    int
}

type IsOKReply struct {
	IsOK bool
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
