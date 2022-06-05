package worker

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"mapreduce/model"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		taskDetails, masterUp := GetTask()
		if !masterUp {
			return
		}
		switch taskDetails.TaskType {
		case model.MAP:
			executeMapTask(taskDetails, mapf)
		case model.REDUCE:
			executeReduceTask(taskDetails, reducef)
		}
		time.Sleep(time.Second)
	}

}

func executeReduceTask(taskDetails model.Task, reducef func(string, []string) string) {
	oName := "mr-out-" + strconv.Itoa(taskDetails.TaskId)
	ofile, _ := ioutil.TempFile(".", oName)
	results := make(map[string][]string)
	for _, intermediateFile := range taskDetails.IntermediateFileNames {
		intermediateFile, _ := os.Open(intermediateFile)
		br := bufio.NewReader(intermediateFile)
		for {
			ln, err := br.ReadBytes('\n')
			parts := bytes.SplitN(ln, []byte{' '}, 2)
			if len(parts) > 1 {
				key := strings.TrimSpace(string(parts[0]))
				value := strings.TrimSuffix(string(parts[1]), "\n")
				results[key] = append(results[key], value)
			}

			if err != nil {
				break
			}
		}
	}
	for key, value := range results {
		ofile.WriteString(key + " " + reducef(key, value) + "\n")
	}

	input := model.ReduceTaskOutput{taskDetails.TaskId, oName}
	os.Rename(ofile.Name(), oName)
	call("Master.CompleteReduceTask", input, &model.Empty{})

}

func executeMapTask(taskDetails model.Task, mapf func(string, string) []KeyValue) {
	file, err := os.Open(taskDetails.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", taskDetails.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", taskDetails.FileName)
	}
	file.Close()
	kva := mapf(taskDetails.FileName, string(content))
	fileNames := writeDataToIntermediateFiles(kva, taskDetails.FileName, taskDetails.NumberOfReduceTasks)
	completeMapTask(taskDetails.FileName, fileNames)
}

func writeDataToIntermediateFiles(kva []KeyValue, taskId string, nReduce int) []string {
	intermediateFileNames := generateIntermediateFileNames(taskId, nReduce)
	files := []*os.File{}
	for _, filename := range intermediateFileNames {
		file, err := ioutil.TempFile(".", filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		files = append(files, file)
		defer file.Close()
		defer os.Rename(file.Name(), filename)
	}
	for _, kv := range kva {
		file := files[ihash(kv.Key)%nReduce]
		file.WriteString(kv.Key + " " + kv.Value + "\n")
	}
	return intermediateFileNames

}

func generateIntermediateFileNames(taskId string, nReduce int) []string {
	var fileNames []string
	for i := 0; i < nReduce; i++ {
		fileNames = append(fileNames, "mr-intermediate-"+filepath.Base(taskId)+"-"+strconv.Itoa(i))
	}
	return fileNames
}

func completeMapTask(taskId string, intermediateFiles []string) {
	input := model.MapTaskOutput{taskId, intermediateFiles}
	call("Master.CompleteMapTask", input, &model.Empty{})
}

func GetTask() (model.Task, bool) {
	input := "ABC"
	reply := model.Task{}
	masterUp := call("Master.GetTask", input, &reply)
	return reply, masterUp
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := model.MasterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
