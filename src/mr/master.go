package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	stateIdle       = iota
	stateInProgress = iota
	stateComplete   = iota
)

const (
	timeoutSeconds = 3
)

type Task struct {
	taskType  int // We reuse the enums from rpc.go -- could use our own internal repr
	state     int
	taskId    string
	inputKeys []string
	workerId  string
	startTime time.Time
}

type Master struct {
	inputFiles                          []string
	tasksById                           map[string]*Task
	mu                       			sync.Mutex
	cond								sync.Cond
	intermediateFilesByPartitionKey     map[string][]string
	nReduce                             int
	isMapDone                           bool
	isReduceDone                        bool
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	log.Printf("Serving GetTask with args %#v\n", args)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isMapDone && m.isReduceDone {
		reply.TaskType = TaskTypeDone
	} else {
		reply.TaskType = TaskTypeWait
	}

	for _, task := range m.tasksById {
		if task.state != stateIdle {
			continue
		}
		t := time.Now().UTC()
		reply.TaskType = task.taskType
		reply.InputKeys = task.inputKeys
		reply.NReduce = m.nReduce
		reply.TaskId = task.taskId
		reply.Expiration = t.Add(3 * time.Second)
		task.workerId = args.WorkerId
		task.state = stateInProgress
		task.startTime = t
		break
	}

	log.Printf("Setting GetTask reply %#v\n", reply)
	m.cond.Broadcast()
	return nil
}

func (m *Master) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	log.Printf("Serving CompleteTask with args %#v\n", args)
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasksById[args.TaskId]
	task.state = stateComplete

	for partitionKey, outputFilename := range args.IntermediateFilesByPartitionKey {
		m.intermediateFilesByPartitionKey[partitionKey] = append(m.intermediateFilesByPartitionKey[partitionKey], outputFilename)
	}

	log.Printf("Serving CompleteTask reply %#v\n", reply)
	m.cond.Broadcast()
	return nil
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
	go m.main()
	go m.cleanUpStaleTasks()
}

func (m *Master) main() {
	// Map phase
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < len(m.inputFiles); i++ {
		// Create map tasks for each of the input files
		taskId := fmt.Sprintf("m%v", i)
		task := Task{taskType: TaskTypeMap, state: stateIdle, taskId: taskId, inputKeys: []string{m.inputFiles[i]}}
		m.tasksById[taskId] = &task
	}
	for !m.isTasksDone() {
		m.cond.Wait()
	}
	m.isMapDone = true
	// Reduce phase
	for key, outputFilenames := range m.intermediateFilesByPartitionKey {
		taskId := fmt.Sprintf("r%v", key)
		task := Task{taskType: TaskTypeReduce, state: stateIdle, taskId: taskId, inputKeys: outputFilenames}
		m.tasksById[taskId] = &task
	}

	for !m.isTasksDone() {
		m.cond.Wait()
	}
	m.isReduceDone = true
}

func (m *Master) cleanUpStaleTasks() {
	for {
		m.mu.Lock()
		for _, task := range m.tasksById {
			if task.workerId == "" || task.state != stateInProgress {
				continue
			}
			t := time.Now().UTC().Sub(task.startTime)
			if t >= timeoutSeconds*time.Second {
				task.startTime = time.Time{}
				task.state = stateIdle
				task.workerId = ""
			}
		}
		m.mu.Unlock()
		time.Sleep(time.Second)
	}
}


// Assumes the lock is held
func (m *Master) isTasksDone() bool {
	for _, task := range m.tasksById {
		if task.state != stateComplete {
			return false
		}
	}
	return true
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	return m.isMapDone && m.isReduceDone
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	log.SetOutput(io.Discard)
	m := Master{}
	m.inputFiles = files
	m.nReduce = nReduce
	m.isMapDone = false
	m.isReduceDone = false
	m.tasksById = make(map[string]*Task)
	m.mu = sync.Mutex{}
	m.cond = *sync.NewCond(&m.mu)
	m.intermediateFilesByPartitionKey = make(map[string][]string)
	m.server()
	return &m
}
