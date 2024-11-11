package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Pending TaskState = iota
	Running
	Completed
)

type MapTask struct {
	Filename string
	State    TaskState
}

type ReduceTask struct {
	State TaskState
}

type Coordinator struct {
	MapTasks                  []MapTask
	ReduceTasks               []ReduceTask
	CompletedMapTasksCount    int
	CompletedReduceTasksCount int
	NReduce                   int
	mu                        sync.Mutex
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	for {
		c.mu.Lock()
		if c.CompletedMapTasksCount == len(c.MapTasks) {
			for i, reduceTask := range c.ReduceTasks {
				if reduceTask.State == Pending {
					reply.Task.Type = Reduce
					reply.Task.ReduceTaskNum = i
					c.ReduceTasks[i].State = Running
					c.mu.Unlock()
					fmt.Printf("[Worker] Reduce taskNum:%v\n", i)
					go c.WaitForTaskCompletion(Reduce, i)
					return nil
				}
			}
		} else {
			for i, mapTask := range c.MapTasks {
				if mapTask.State == Pending {
					reply.Task.Type = Map
					reply.Task.Filename = mapTask.Filename
					reply.Task.MapTaskNum = i
					reply.Task.NReduce = c.NReduce
					c.MapTasks[i].State = Running
					fmt.Printf("[GetTask] Map %v taskNum:%v\n", mapTask.Filename, i)
					c.mu.Unlock()
					go c.WaitForTaskCompletion(Map, i)
					return nil
				}
			}
		}
		c.mu.Unlock()
		time.Sleep(time.Second)
	}
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	task := args.Task
	switch task.Type {
	case Map:
		c.mu.Lock()
		if c.MapTasks[task.MapTaskNum].State == Completed {
			c.mu.Unlock()
			return nil
		}
		c.CompletedMapTasksCount++
		c.MapTasks[task.MapTaskNum].State = Completed
		c.mu.Unlock()
		for real, tmp := range task.RealToTmp {
			os.Rename(tmp, real)
		}
		fmt.Printf("[CompleteTask] Map %v taskNum:%v\n", task.Filename, task.MapTaskNum)
	case Reduce:
		c.mu.Lock()
		c.CompletedReduceTasksCount++
		c.ReduceTasks[task.ReduceTaskNum].State = Completed
		c.mu.Unlock()
		fmt.Printf("[CompleteTask] Reduce taskNum:%v\n", task.ReduceTaskNum)
	}
	return nil
}

func (c *Coordinator) WaitForTaskCompletion(taskType TaskType, taskNum int) {
	time.Sleep(time.Second * 10)
	c.mu.Lock()
	switch taskType {
	case Map:
		if c.MapTasks[taskNum].State == Running {
			fmt.Println("[WaitForTaskCompletion] Map taskNum:", taskNum)
			c.MapTasks[taskNum].State = Pending
		}
	case Reduce:
		if c.ReduceTasks[taskNum].State == Running {
			fmt.Println("[WaitForTaskCompletion] Reduce taskNum:", taskNum)
			c.ReduceTasks[taskNum].State = Pending
		}
	}
	c.mu.Unlock()
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	if c.CompletedMapTasksCount == len(c.MapTasks) && c.CompletedReduceTasksCount == len(c.ReduceTasks) {
		c.mu.Unlock()
		return true
	}
	c.mu.Unlock()
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{NReduce: nReduce}

	c.MapTasks = []MapTask{}
	for _, filename := range files {
		mapTask := MapTask{Filename: filename, State: Pending}
		c.MapTasks = append(c.MapTasks, mapTask)
	}

	c.ReduceTasks = []ReduceTask{}
	for i := 0; i < nReduce; i++ {
		reduceTask := ReduceTask{State: Pending}
		c.ReduceTasks = append(c.ReduceTasks, reduceTask)
	}

	fmt.Printf("[MakeCoordinator] MapTasks:%v ReduceTasks:%v nReduce:%v\n", c.MapTasks, c.ReduceTasks, c.NReduce)

	c.server()
	return &c
}
