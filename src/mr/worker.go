package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type Task struct {
	Type TaskType

	Filename   string
	MapTaskNum int
	NReduce    int
	RealToTmp  map[string]string

	ReduceTaskNum int
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task, err := GetTask()
		if err != nil {
			break
		}
		switch task.Type {
		case Map:
			fmt.Printf("[Worker] Map %v taskNum:%v\n", task.Filename, task.MapTaskNum)
			filename := task.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			file.Close()
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			kva := mapf(filename, string(content))
			realToTmp := make(map[string]string)
			for _, kv := range kva {
				reduceTaskNum := ihash(kv.Key) % task.NReduce
				intermediate := fmt.Sprintf("mr-%v-%v", task.MapTaskNum, reduceTaskNum)
				var ofile *os.File
				var err error
				if tmp, ok := realToTmp[intermediate]; ok {
					ofile, err = os.OpenFile(tmp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				} else {
					ofile, err = os.CreateTemp("", intermediate+"-")
					realToTmp[intermediate] = ofile.Name()
				}

				if err != nil {
					log.Fatalln("cannot open or create file", intermediate, err)
				}

				enc := json.NewEncoder(ofile)

				// Encode writes to the file in append mode
				if err := enc.Encode(&kv); err != nil {
					ofile.Close()
					log.Fatalln("cannot write to file", intermediate)
				}
				ofile.Close()
			}
			task.RealToTmp = realToTmp
		case Reduce:
			fmt.Printf("[Worker] Reduce taskNum:%v\n", task.ReduceTaskNum)
			reduceTaskNum := task.ReduceTaskNum
			filenames, err := filepath.Glob("mr-*-*")
			if err != nil {
				log.Fatal(err)
			}
			kva := []KeyValue{}
			for _, filename := range filenames {
				if strings.Split(filename, "-")[2] != strconv.Itoa(reduceTaskNum) {
					continue
				}
				file, err := os.Open(filename)
				if err != nil {
					file.Close()
					log.Fatalf("cannot open %v", filename)
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
			sort.Sort(ByKey(kva))

			oname := fmt.Sprintf("mr-out-%v", reduceTaskNum)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			ofile.Close()
		}
		CompleteTask(task)
	}
}

func GetTask() (*Task, error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return &reply.Task, nil
	} else {
		return nil, errors.New("GetTask failed")
	}
}

func CompleteTask(task *Task) error {
	args := CompleteTaskArgs{Task: *task}
	reply := CompleteTaskReply{}

	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		return nil
	} else {
		return errors.New("Failed CompleteTask")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
