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

type Task struct {
	mapOrReduce string
	state       string
	taskId      string
	inputKeys   []string
	workerId    string
	startTime	time.Time
}

type Master struct {
	files                            []string
	tasksById                        map[string]*Task
	tasksByIdLock sync.Mutex
	mapOutputFilenamesByPartitionKey map[string][]string
	mapOutputFilenamesByPartitionKeyLock sync.Mutex
	nReduce                          int
	isMapDone                        bool
	isReduceDone                     bool
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	log.Printf("Serving GetTask with args %#v\n", args)
	if m.isMapDone && m.isReduceDone {
		reply.MapOrReduce = ""
	} else {
		reply.MapOrReduce = "wait"
	}
	m.tasksByIdLock.Lock()

	for _, task := range m.tasksById {
		if task.state != "idle" {
			continue
		}
		t := time.Now().UTC()
		reply.MapOrReduce = task.mapOrReduce
		reply.InputKeys = task.inputKeys
		reply.NReduce = m.nReduce
		reply.TaskId = task.taskId
		reply.Expiration = t.Add(3 * time.Second)
		task.workerId = args.WorkerId
		task.state = "inProgress"
		task.startTime = t
		break
	}

	m.tasksByIdLock.Unlock()
	log.Printf("Setting GetTask reply %#v\n", reply)
	return nil
}

func (m *Master) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	log.Printf("Serving CompleteTask with args %#v\n", args)
	m.tasksByIdLock.Lock()
	task := m.tasksById[args.TaskId]
	task.state = "complete"
	m.tasksByIdLock.Unlock()

	m.mapOutputFilenamesByPartitionKeyLock.Lock()
	for partitionKey, outputFilename := range args.OutputFilenamesByPartitionKey {
		outputFilenames := m.mapOutputFilenamesByPartitionKey[partitionKey]

		if outputFilenames == nil {
			outputFilenames = []string{}
		}

		m.mapOutputFilenamesByPartitionKey[partitionKey] = append(outputFilenames, outputFilename)
	}
	m.mapOutputFilenamesByPartitionKeyLock.Unlock()

	log.Printf("Serving CompleteTask reply %#v\n", reply)
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
	m.tasksByIdLock.Lock()
	for i := 0; i < len(m.files); i++ {
		// Create map tasks for each of the input files
		taskId := fmt.Sprintf("m%v", i)
		task := Task{mapOrReduce: "map", state: "idle", taskId: taskId, inputKeys: []string{m.files[i]}}
		m.tasksById[taskId] = &task
	}
	m.tasksByIdLock.Unlock()
	for {
		if m.isTasksDone() {
			m.isMapDone = true
			break
		}
		time.Sleep(time.Second)
	}
	// Reduce phase
	m.tasksByIdLock.Lock()
	m.mapOutputFilenamesByPartitionKeyLock.Lock()
	for key, outputFilenames := range m.mapOutputFilenamesByPartitionKey {
		taskId := fmt.Sprintf("r%v", key)
		task := Task{mapOrReduce: "reduce", state: "idle", taskId: taskId, inputKeys: outputFilenames}
		m.tasksById[taskId] = &task
	}
	m.mapOutputFilenamesByPartitionKeyLock.Unlock()
	m.tasksByIdLock.Unlock()

	for {
		if m.isTasksDone() {
			m.isReduceDone = true
			break
		}
		time.Sleep(time.Second)
	}
}

func (m *Master) cleanUpStaleTasks() {
	for {
		m.tasksByIdLock.Lock()
		for _, task := range m.tasksById {
			if task.workerId == "" || task.state != "inProgress" {
				continue
			}
			t := time.Now().UTC().Sub(task.startTime)
			if t >= 3 * time.Second {
				task.startTime = time.Time{}
				task.state = "idle"
				task.workerId = ""
			}
		}
		m.tasksByIdLock.Unlock()
		time.Sleep(time.Second)
	}
}

func (m *Master) isTasksDone() bool {
	m.tasksByIdLock.Lock()
	defer m.tasksByIdLock.Unlock()
	for _, task := range m.tasksById {
		if task.state != "complete" {
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
	m.files = files
	m.nReduce = nReduce
	m.isMapDone = false
	m.isReduceDone = false
	m.tasksById = make(map[string]*Task)
	m.tasksByIdLock = sync.Mutex{}
	m.mapOutputFilenamesByPartitionKey = make(map[string][]string)
	m.mapOutputFilenamesByPartitionKeyLock = sync.Mutex{}
	m.server()
	return &m
}
