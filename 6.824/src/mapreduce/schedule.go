package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
// Hao: all workers will be registered each time the schedule() get invoked.
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	var waitGroup sync.WaitGroup
	workerChan := initWorkerChan(registerChan, 10)

	for i:= 0; i < ntasks; i++ {
		fmt.Printf("Submitting task: %d\n", i)
		waitGroup.Add(1)

		go func(registerChan chan string, workerChan chan string, jobNum int, mapFile []string) {
			defer waitGroup.Done()

			populateNewWorkerFromRegisterChan(registerChan, workerChan)

			worker := <- workerChan
			fmt.Printf("Avaialbe worker: %s\n", worker)

			// Do map/reduce
			var inFile string
			if phase == mapPhase {
				inFile = mapFiles[jobNum]
				//fmt.Printf("file %s\n", inFile)
			}
			doTaskArgs := DoTaskArgs{
				JobName:jobName,
				File: inFile,
				Phase: phase,
				TaskNumber:jobNum,
				NumOtherPhase: n_other}
			doTask(worker, doTaskArgs)

			fmt.Printf("Task %d done\n", jobNum)

			//When task finished, add worker back then update waitgroup.
			workerChan <- worker
		}(registerChan, workerChan, i, mapFiles)
	}
	// wait all submitted tasks to finish
	waitGroup.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}

func initWorkerChan(registerChan chan string, size int) chan string {
	workerChan := make(chan string, size)
	worker := <- registerChan
	workerChan <- worker
	return workerChan
}

func populateNewWorkerFromRegisterChan(registerChan chan string, workerChan chan string) {
	select {
	case newWorker := <- registerChan:
		fmt.Printf("new worker found: %s\n", newWorker)
		workerChan <- newWorker
	default:
	}
}

func doTask(worker string, doTaskArgs DoTaskArgs) bool {
	rpcName := "Worker.DoTask"
	return call(worker, rpcName, doTaskArgs, nil)
}
