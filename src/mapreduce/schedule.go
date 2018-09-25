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

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	// NOTE: wait until all tasks have completed, call wg.Done() when any task completes
	var wg sync.WaitGroup
	wg.Add(ntasks)

	// usually #tasks > #workers, schedule must give each worker a sequence of tasks, one at a time
	go func() {
		taskChannels := make(chan int, ntasks)
		for taskNumber := 0; taskNumber < ntasks; taskNumber++ {
			taskChannels <- taskNumber
		}
		for {
			taskNumber, ok := <- taskChannels

			if !ok {
				break
			}

			var mapFile string
			if phase == mapPhase {
				mapFile = mapFiles[taskNumber]
			}
			var doTaskArgs = DoTaskArgs{
				JobName:       jobName,
				File:          mapFile,
				Phase:         phase,
				TaskNumber:    taskNumber,
				NumOtherPhase: n_other,
			}

			go func() {
				w := <- registerChan
				var hasSucc = call(w, "Worker.DoTask", doTaskArgs, nil)
				if hasSucc {
					go func() {
						registerChan <- w
					}()
					wg.Done()
				} else {
					go func() {
						taskChannels <- taskNumber
						registerChan <- w
					}()
				}
			}()
		}
	}()

	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
