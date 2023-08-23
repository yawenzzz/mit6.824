package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func askForTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Master.AssignTask", &args, &reply)
	return reply
}

// mapping过程
func mapping(task *Task, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.InputFile)
	if err != nil {
		log.Fatalf("cannot open %v", task.InputFile)
	}
	//读取文件的内容
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.InputFile)
	}
	file.Close()
	// 中间态, 缓存结果
	intermediates := mapf(task.InputFile, string(content))
	// 将缓存后的结果写入本地，并且分成R份
	// buffer[NReducer][KeyValue]
	buffer := make([][]KeyValue, task.NReduce)
	for _, intermediate := range intermediates {
		// hash(key) mod R
		split := ihash(intermediate.Key) % task.NReduce
		buffer[split] = append(buffer[split], intermediate)
	}
	outputfiles := make([]string, 0)
	for i := 0; i < task.NReduce; i++ {
		outputfiles = append(outputfiles, writeToLocalFile(task.Taskid, i, &buffer[i]))
	}
	task.Intermediates = outputfiles
	reply := ExampleReply{}
	call("Master.TaskCompleted", task, &reply)

}

func reducing(task *Task, reducef func(string, []string) string) {
	intermediate := *readLocalFile(task.Intermediates)

	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Fail to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.Taskid+1)
	os.Rename(tempFile.Name(), oname)
	task.OutputFile = oname
	reply := ExampleReply{}
	call("Master.TaskCompleted", task, &reply)

}

// 返回文件路径 dir+outputname
func writeToLocalFile(taskid int, ireducer int, kva *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create tempfile ", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kva {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", taskid, ireducer)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}

func readLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Fail to open file "+filepath, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return &kva
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	//fmt.Println("Worker ready...")
	// Your worker implementation here.
	for {
		task := askForTask()
		switch task.State {
		case Map:
			fmt.Println("Mapping...")
			mapping(&task, mapf)
		case Reduce:
			fmt.Println("Reducing...")
			reducing(&task, reducef)
		case Wait:
			fmt.Println("Waiting...")
			time.Sleep(10 * time.Second)
		case Exited:
			fmt.Println("Finished!")
			return
		}
	}
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// rpcname是远程函数名
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
