package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type task struct {
	taskType   int
	inputKeys  []string
	nReduce    int
	taskId     string
	expiration time.Time
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	log.SetOutput(io.Discard)
	workerId := strconv.Itoa(rand.Intn(1e9))
	for {
		t := getTask(workerId)
		switch t.taskType {
		case TaskTypeMap:
			handleMapTask(mapf, t.inputKeys, t.nReduce, t.taskId, t.expiration)
		case TaskTypeReduce:
			handleReduceTask(reducef, t.inputKeys, t.taskId, t.expiration)
		case TaskTypeWait:
		case TaskTypeDone:
			return
		default:
			log.Println("Unknown task type")
		}
	}
}

func handleMapTask(mapf func(string, string) []KeyValue, inputKeys []string, nReduce int, taskId string, expiration time.Time) {
	if len(inputKeys) != 1 {
		log.Fatal("Only one input key supported for map operations")
	}
	inputKey := inputKeys[0]

	rawContent, err := os.ReadFile(inputKey)
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
	content := string(rawContent)
	keyValues := mapf(inputKey, content)

	if isExpired(expiration) {
		return
	}

	keyValuesByPartitionKey := make(map[string][]KeyValue)

	for _, kv := range keyValues {
		partitionKey := strconv.Itoa(ihash(kv.Key) % nReduce)

		values := keyValuesByPartitionKey[partitionKey]

		if values == nil {
			values = []KeyValue{}
		}

		keyValuesByPartitionKey[partitionKey] = append(values, kv)
	}

	outputFilesByPartitionKey := make(map[string]string)

	for partitionKey, keyValues := range keyValuesByPartitionKey {
		outputFilename := fmt.Sprintf("%s-%s.json", taskId, partitionKey)
		log.Printf("Creating output file %s", outputFilename)
		file, err := os.Create(outputFilename)
		if err != nil {
			log.Fatal("Error creating file")
		}
		encoder := json.NewEncoder(file)
		encodingErr := encoder.Encode(keyValues)
		if encodingErr != nil {
			log.Fatal("Error encoding intermediate key values")
		}
		file.Close()
		outputFilesByPartitionKey[partitionKey] = outputFilename
	}

	completeTask(taskId, outputFilesByPartitionKey)
}

func handleReduceTask(reducef func(string, []string) string, inputKeys []string, taskId string, expiration time.Time) {
	keyValues := make(map[string][]string)
	for _, inputKey := range inputKeys {
		rawContent, err := os.ReadFile(inputKey)
		if err != nil {
			log.Fatalf("Error reading file: %v", err)
		}

		var pairs []KeyValue
		err = json.Unmarshal(rawContent, &pairs)
		if err != nil {
			log.Fatal("Error unmarshaling JSON:", err)
		}

		for _, pair := range pairs {
			values := keyValues[pair.Key]

			if values == nil {
				values = []string{}
			}
			keyValues[pair.Key] = append(values, pair.Value)
		}
	}

	outputFilename := fmt.Sprintf("mr-out-%v", string(taskId[1]))
	file, err := os.Create(outputFilename)
	if err != nil {
		log.Fatal("Error creating file")
	}

	formattedOutputs := []string{}

	for key, values := range keyValues {
		output := reducef(key, values)
		formattedOutput := fmt.Sprintf("%v %v\n", key, output)
		formattedOutputs = append(formattedOutputs, formattedOutput)
	}

	if isExpired(expiration) {
		return
	}

	for _, formattedOutput := range formattedOutputs {
		_, writeErr := file.Write([]byte(formattedOutput))
		if writeErr != nil {
			log.Fatal("Error writing to file")
		}
	}
	completeTask(taskId, nil)
}

func getTask(workerId string) task {
	args := GetTaskArgs{WorkerId: workerId}
	reply := GetTaskReply{}
	log.Printf("Sending GetTaskArgs: %#v\n", args)
	call("Master.GetTask", &args, &reply)
	log.Printf("Received GetTaskReply: %#v\n", reply)

	// Map reply to task
	return task{taskType: reply.TaskType, inputKeys: reply.InputKeys, nReduce: reply.NReduce, taskId: reply.TaskId, expiration: reply.Expiration}
}

func completeTask(taskId string, outputFilenamesByPartitionKey map[string]string) {
	args := CompleteTaskArgs{TaskId: taskId, IntermediateFilesByPartitionKey: outputFilenamesByPartitionKey}
	reply := CompleteTaskReply{}
	log.Printf("Sending CompleteTaskArgs: %#v\n", args)
	call("Master.CompleteTask", &args, &reply)
	log.Printf("Received CompleteTaskReply: %#v\n", reply)
}

func isExpired(expiration time.Time) bool {
	return time.Now().UTC().After(expiration)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	log.Println(err)
	return false
}
