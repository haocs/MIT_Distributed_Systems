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

	waitGroup.Add(ntasks)
	workerChan := initWorkerChan(registerChan, 100)

	for i:= 0; i < ntasks; i++ {
		debug("Submitting task: %d\n", i)

		// each goroutine will return only when task completes successfully
		go func(registerChan chan string, workerChan chan string, jobNum int, mapFile []string) {

			for {
				worker := <- workerChan

				// Do map/reduce
				var inFile string
				if phase == mapPhase {
					inFile = mapFiles[jobNum]
				}
				doTaskArgs := DoTaskArgs{
					JobName:jobName,
					File: inFile,
					Phase: phase,
					TaskNumber:jobNum,
					NumOtherPhase: n_other}
				success := doTask(worker, doTaskArgs)

				if success {
					workerChan <- worker
					waitGroup.Done()
					debug("Task %d done\n", jobNum)
					break
				} else {
					debug("Task %d failed\n", jobNum)
				}
			}

		}(registerChan, workerChan, i, mapFiles)
	}
	// wait all submitted tasks to finish
	waitGroup.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}

func initWorkerChan(registerChan chan string, size int) chan string {
	workerChan := make(chan string, size)

	// keep populating new worker from registerChan to workerChan
	go func(registerChan chan string, workerChan chan string) {
		for {
			populateNewWorkerFromRegisterChan(registerChan, workerChan)
		}
	}(registerChan, workerChan)

	return workerChan
}

func populateNewWorkerFromRegisterChan(registerChan chan string, workerChan chan string) {
	select {
	case newWorker := <- registerChan:
		workerChan <- newWorker
	default:
	}
}

func doTask(worker string, doTaskArgs DoTaskArgs) bool {
	rpcName := "Worker.DoTask"
	return call(worker, rpcName, doTaskArgs, nil)
}
