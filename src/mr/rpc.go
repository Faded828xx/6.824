package mr

import "os"
import "strconv"

// register worker with no arguments
type RegisterArgs struct {
}

// get a workerId as a reply
type RegisterReply struct {
	WorkerId int
}

// request for a task
type TaskArgs struct {
	WorkerId int
}
type TaskReply struct {
	Task *Task
}

type ReportArgs struct {
	Done     bool
	Seq      int
	Phase    TaskPhase
	WorkerId int
}
type ReportReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
