package master

import (
	"log"
	"mapreduce/model"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	scheduledMapTasks    []model.MapTask
	runningMapTasks      map[string]bool
	completedMapTasks    map[string]bool
	scheduledReduceTasks []model.ReduceTask
	runningReduceTasks   map[int]model.ReduceTask
	completeReduceTasks  map[int]string
	nReduce              int
	nMap                 int

	mu sync.Mutex
}

func (m *Master) GetTask(input string, reply *model.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.scheduledMapTasks) != 0 {
		*reply = m.getScheduledMapTask()
	} else if len(m.completedMapTasks) == m.nMap && len(m.scheduledReduceTasks) != 0 {
		*reply = m.getScheduledReduceTask()
	} else {
		*reply = model.Task{TaskType: model.WAIT}
	}

	return nil
}

func (m *Master) getScheduledMapTask() model.Task {
	numberOfMapTasks := len(m.scheduledMapTasks)
	choosenTask := m.scheduledMapTasks[numberOfMapTasks-1]
	m.runningMapTasks[choosenTask.FileName] = false
	m.scheduledMapTasks = m.scheduledMapTasks[:numberOfMapTasks-1]
	go func() {
		time.Sleep(time.Second * 10)
		m.mu.Lock()
		defer m.mu.Unlock()
		if _, ok := m.runningMapTasks[choosenTask.FileName]; ok {
			m.scheduledMapTasks = append(m.scheduledMapTasks, choosenTask)
		}
	}()
	return model.Task{MapTask: model.MapTask{choosenTask.FileName, m.nReduce}, TaskType: model.MAP}
}

func (m *Master) getScheduledReduceTask() model.Task {
	numberOfReduceTasks := len(m.scheduledReduceTasks)
	reduceTask := m.scheduledReduceTasks[numberOfReduceTasks-1]
	m.runningReduceTasks[reduceTask.TaskId] = reduceTask
	m.scheduledReduceTasks = m.scheduledReduceTasks[:numberOfReduceTasks-1]
	go func() {
		time.Sleep(time.Second * 10)
		m.mu.Lock()
		defer m.mu.Unlock()
		if _, ok := m.runningReduceTasks[reduceTask.TaskId]; ok {
			m.scheduledReduceTasks = append(m.scheduledReduceTasks, reduceTask)
		}
	}()
	return model.Task{ReduceTask: model.ReduceTask{reduceTask.TaskId, reduceTask.IntermediateFileNames}, TaskType: model.REDUCE}
}

func (m *Master) CompleteMapTask(input model.MapTaskOutput, _ *model.Empty) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.runningMapTasks, input.TaskId)
	m.completedMapTasks[input.TaskId] = true
	for index, val := range input.IntermediateFiles {
		m.scheduledReduceTasks[index].IntermediateFileNames = append(m.scheduledReduceTasks[index].IntermediateFileNames, val)
	}
	return nil

}

func (m *Master) CompleteReduceTask(input model.ReduceTaskOutput, _ *model.Empty) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.runningReduceTasks, input.TaskId)
	m.completeReduceTasks[input.TaskId] = input.OutputFileName
	return nil

}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := model.MasterSock()
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.completeReduceTasks) == m.nReduce
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nReduce = nReduce
	m.nMap = len(files)
	m.completedMapTasks = make(map[string]bool)
	m.runningMapTasks = make(map[string]bool)
	m.scheduledMapTasks = []model.MapTask{}
	for i := 0; i < nReduce; i++ {
		m.scheduledReduceTasks = append(m.scheduledReduceTasks, model.ReduceTask{TaskId: i})
	}
	m.runningReduceTasks = make(map[int]model.ReduceTask)
	m.completeReduceTasks = make(map[int]string)
	for index, file := range files {
		m.scheduledMapTasks = append(m.scheduledMapTasks, model.MapTask{file, index})
	}
	m.server()
	return &m
}
