package model

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	WAIT
	EXIT
)

type Task struct {
	MapTask
	ReduceTask
	TaskType
}

type MapTask struct {
	FileName            string
	NumberOfReduceTasks int
}

type ReduceTask struct {
	TaskId                int
	IntermediateFileNames []string
}

type MapTaskOutput struct {
	TaskId            string
	IntermediateFiles []string
}

type ReduceTaskOutput struct {
	TaskId         int
	OutputFileName string
}

type Empty struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func MasterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
