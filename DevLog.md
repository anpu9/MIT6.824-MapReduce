# MIT6.824-MapReduce

### Get started
1. Q1: How many tasks does a worker handle?

   (1)A worker means a threads or process?
```bash
# start multiple workers.
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &

# wait for one of the processes to exit.
# under bash, this waits for all processes,
# including the master.
wait

# the master or a worker has exited. since workers are required
# to exit when a job is completely finished, and not before,
# that means the job has finished.

sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi
```
this line `timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &`
the line starts a new process by executing the mrworker executable with the specified arguments, and it runs this process in the background.
   
   (2) since a worker is a process, which has capacity to run multithread concurrently
   is it possibile that a worker that the worker don't use go routines
   a process just handle a task one time, once it finishes, the master can change its states
   
   (3) Or use chanel?
2. How many tasks does the master has to assign?
- M map tasks
- R reduce tasks. The number of partitions (R) and the partitioning function are specified by the user.
- The master picks idle workers and assigns each one a map task or a reduce task.
   
2. Q2: What does a mrworker executable do?
   (1) Create a (worker) process to call `Worker` function in mr/master
```go
func main() {
if len(os.Args) != 2 {
fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
os.Exit(1)
}

mapf, reducef := loadPlugin(os.Args[1])

mr.Worker(mapf, reducef)
}
```
3. How can Master detect available worker?
- The coordinator, as an RPC server, will be concurrent; don't forget to lock shared data.
- Workers will sometimes need to wait, e.g. reduces can't start until the last map has finished. 
- One possibility is for workers to periodically ask the coordinator for work, sleeping with time.Sleep() between each request. 
- Another possibility is for the relevant RPC handler in the coordinator to have a loop that waits, either with time.Sleep() or sync.Cond. Go runs the handler for each RPC in its own thread, 
- so the fact that one handler is waiting won't prevent the coordinator from processing other RPCs.

4. What is RPC handler?
- In a client-server architecture where communication happens over a network, 
- RPC handlers are crucial for handling requests from remote clients and executing the corresponding procedures or methods on the server side.
- A typical RPC handler in Go might involve registering methods to be exposed remotely, handling incoming requests, invoking the appropriate methods or functions, and sending back responses to the clients.

5. is input files needed to split into M splits?
- there is no need, each file corresponds to one "split", and is the input to one Map task.
- 
6. What parameters are needed to declare in rpc.go when making a RPC call?
- One way to get started is to modify mr/worker.go's Worker() to send an RPC to the coordinator asking for a task. 
- Then modify the coordinator to respond with the file name of an as-yet-unstarted map task. 
- Then modify the worker to read that file and call the application Map function, as in mrsequential.go.
```go
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
```

7. How can master get the location of the disk's location when all reduce 
- mapworker to make a RPC call
8. How can master detect crashed worker?
9. How can exit when task finishes
- When the job is completely finished, the worker processes should exit. 
- A simple way to implement this is to use the return value from call(): if the worker fails to contact the coordinator, it can assume that the coordinator has exited because the job is done, and so the worker can terminate too. 
- Depending on your design, you might also find it helpful to have a "please exit" pseudo-task that the coordinator can give to workers.

# Log
- add wait loop to make sure reduce start after all map tasks finished
- master initialize reduce task
- map worker write intermediate file to disk
  TODO:
1. clean the disk space
2. find the reason why the same reduce task could be assigned to different hosts
3. make sure the reduce output format correctly
- initialize an array with make([]string, 0)
- One possibility is that the ReduceTask object is being copied instead of being referenced directly. 
- To ensure that you're updating the original ReduceTask object in the m.ReduceTasks slice, you should directly reference it using its index:
Key points:
1. the mutex is only used to protect access to the MapTasks and ReduceTasks slices within the TaskFinder function. Once a task is marked as "assigned", it is still possible for multiple workers to obtain the same task if they make concurrent RPC calls to the TaskFinder function before the first worker finishes processing the task.
2. It seems like you are encountering a race condition where multiple processes are attempting to acquire the mutex lock simultaneously. This can happen if the mutex is not properly synchronized across multiple processes.

In your scenario, each process appears to be running independently, and they are all trying to acquire the same mutex lock from the Master struct. However, since each process runs in its own memory space and has its own copy of the Master struct, they are not actually sharing the mutex lock.

To ensure that only one process has access to the shared data protected by the mutex, you need to use a synchronization mechanism that works across multiple processes. In Go, one common approach is to use a named mutex, which can be shared across processes.