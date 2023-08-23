package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 定义状态常量用于标识worker的状态
type State int

const (
	Map State = iota
	Reduce
	Wait
	Exited
)

// 定义任务的状态
// 状态只有两个，要么是完成，要么就是进行中
type TaskState int

const (
	Completed TaskState = iota
	InProgress
	Idle
)

type Task struct {
	Taskid        int
	InputFile     string
	OutputFile    string // 最终的输出文件的列表
	NReduce       int
	Intermediates []string // worker mapping的中间文件
	State         State
}

// 记录Task的信息
type TaskInfo struct {
	TaskState TaskState
	StartTime time.Time // 记录任务开始的时间，用来应对crashed
	ThisTask  *Task
}
type Master struct {
	// Your definitions here.
	TaskQueue     chan *Task        //待处理的任务队列 储存的是指向Task的指针
	TaskData      map[int]*TaskInfo // k: id v: TaskInfo
	MasterState   State
	NReducer      int
	InputFiles    []string //输入文件的序列
	Intermediates [][]string
}

var mux sync.Mutex

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) catchTimeOut() {
	for {
		fmt.Println("catching timeout...")
		time.Sleep(10 * time.Second)
		mux.Lock()
		fmt.Println(time.Now())
		fmt.Println("State of master: ", m.MasterState)
		fmt.Println("length of Task: ", len(m.TaskQueue))
		if m.MasterState == Exited {
			mux.Unlock()
			return
		}
		for _, taskInfo := range m.TaskData {
			if taskInfo.TaskState == InProgress && time.Now().Sub(taskInfo.StartTime) > 10*time.Second {
				m.TaskQueue <- taskInfo.ThisTask
				taskInfo.TaskState = Idle
			}
		}
		mux.Unlock()
	}
}

/*
	func (m *Master) catchTimeOut() {
		for {
			fmt.Println("catching timeout...")
			time.Sleep(1 * time.Second)
			mux.Lock()
			fmt.Println(time.Now())
			fmt.Println("State of master: ", m.MasterState)
			fmt.Println("length of Task: ", len(m.TaskQueue))
			if m.MasterState == Map {
				for _, taskinfo := range m.TaskData {
					// 如果超时则需要重新等待分配任务
					if taskinfo.TaskState == InProgress && time.Now().Sub(taskinfo.StartTime) > 3*time.Second {
						m.TaskQueue <- taskinfo.ThisTask
						taskinfo.TaskState = Idle
					}
				}
			} else if m.MasterState == Reduce {
				for _, taskinfo := range m.TaskData {
					// 如果超时则需要重新等待分配任务
					if taskinfo.TaskState == InProgress && time.Now().Sub(taskinfo.StartTime) > 3*time.Second {
						m.TaskQueue <- taskinfo.ThisTask
						taskinfo.TaskState = Idle
					}
				}
			} else if m.MasterState == Exited {
				mux.Unlock()
				return
			}
		}
	}
*/
func (m *Master) createMapTask() {
	for index, filename := range m.InputFiles {
		task := Task{
			InputFile: filename,
			Taskid:    index,
			NReduce:   m.NReducer,
			State:     Map,
		}
		m.TaskQueue <- &task
		m.TaskData[index] = &TaskInfo{
			TaskState: Idle,
			ThisTask:  &task,
		}
	}
}

func (m *Master) createReduceTask() {
	m.TaskData = make(map[int]*TaskInfo)
	for index, files := range m.Intermediates {
		task := Task{
			Taskid:        index,
			NReduce:       m.NReducer,
			State:         Reduce,
			Intermediates: files,
		}
		m.TaskQueue <- &task
		m.TaskData[index] = &TaskInfo{
			TaskState: Idle,
			ThisTask:  &task,
		}
	}
}

// worker通知master任务已结束
func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	mux.Lock()
	defer mux.Unlock()
	if task.State != m.MasterState || m.TaskData[task.Taskid].TaskState == Completed {
		return nil
	}
	m.TaskData[task.Taskid].TaskState = Completed
	go m.processTaskResult(task)
	return nil
}

// 给worker分配任务
func (m *Master) AssignTask(args *ExampleArgs, reply *Task) error {
	mux.Lock()
	defer mux.Unlock()
	// 检查消息队列是否有任务待分配
	if len(m.TaskQueue) > 0 {
		*reply = *<-m.TaskQueue
		// 更新状态
		m.TaskData[reply.Taskid].TaskState = InProgress
		m.TaskData[reply.Taskid].StartTime = time.Now()
	} else if m.MasterState == Exited {
		*reply = Task{State: Exited}
	} else {
		*reply = Task{State: Wait}
	}
	return nil
}

func (m *Master) processTaskResult(task *Task) {
	mux.Lock()
	defer mux.Unlock()
	switch task.State {
	case Map:
		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		if m.allTaskDone() {
			m.createReduceTask()
			m.MasterState = Reduce
		}
	case Reduce:
		if m.allTaskDone() {
			m.MasterState = Exited
		}
	}
}

// 判断所有的task是否完成，如果全部完成则返回true
func (m *Master) allTaskDone() bool {
	for _, task := range m.TaskData {
		if task.TaskState != Completed {
			return false
		}
	}
	return true
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
	// Your code here.
	mux.Lock()
	defer mux.Unlock()
	ret := m.MasterState == Exited

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	fmt.Println("Master ready...")
	m := Master{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		TaskData:      make(map[int]*TaskInfo),
		MasterState:   Map,
		NReducer:      nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
	}

	m.createMapTask()
	fmt.Println("created map tasks!")
	m.server()
	go m.catchTimeOut()
	return &m
}
