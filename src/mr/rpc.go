package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type GetTaskArgs struct {
	WorkerId string
}
type GetTaskReply struct {
	MapOrReduce string
	InputKeys []string
	NReduce   int
	TaskId    string
	Expiration time.Time
}

type CompleteTaskArgs struct {
	TaskId                        string
	OutputFilenamesByPartitionKey map[string]string
}

type CompleteTaskReply struct {
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
