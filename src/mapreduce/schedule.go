package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(
	jobName string,
	mapFiles []string,
	nReduce int,
	phase jobPhase,
	registerChan chan string) {
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

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	/*
		1. Load all tasks into a channel
		2. Until channel is empty
			get worker, assign work -> use go
			wait until all done.
	*/
	taskChan := make(chan DoTaskArgs, ntasks)
	for i := 0; i < ntasks; i++ {
		doTaskArg := DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		}
		taskChan <- doTaskArg
	}
	close(taskChan) // otherwise ranging over channel will not quit
	var wg sync.WaitGroup
	// Assign tasks
	for task := range taskChan {
		wg.Add(1)
		go executeTask(&wg, registerChan, task)
	}
	wg.Wait() // make sure all task are done before executing next schedule()
	fmt.Printf("Schedule: %v done\n", phase)
}
func executeTask(
	wg *sync.WaitGroup, // need to use pointer to share one wg instead of copies
	registerChan chan string,
	args DoTaskArgs) {
	worker := <-registerChan // waiting for an available worker
	succeed := call(worker, "Worker.DoTask", args, nil)
	if !succeed {
		go executeTask(wg, registerChan, args)
		return
	}
	wg.Done()
	registerChan <- worker
}
