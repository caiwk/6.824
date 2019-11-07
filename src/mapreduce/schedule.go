package mapreduce

import (
	"fmt"
	log "log_manager"
	"sync"
	"time"
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

	log.Infof("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var workadrs []string
	var lock = sync.Mutex{}
	wg := sync.WaitGroup{}
	failwork := make(chan DoTaskArgs)
	//kill := make(chan bool)
	go func() {
	f:
		for {
			select {
			case taskStruct := <-failwork:
				workadr := workadrs[0]
				lock.Lock()
				workadrs = workadrs[1:]
				lock.Unlock()
				go func() {
					res := call(workadr, "Worker.DoTask", taskStruct, nil)
					lock.Lock()
					workadrs = append(workadrs, workadr)
					lock.Unlock()
					if !res {
						log.Error("worker failed",workadr)
						failwork <- taskStruct
						log.Error("start retry")
					}else {
						wg.Done()
					}

				}()
			case <-time.After(time.Second):
				log.Info("kill myself")
				break f
			}
		}

	}()
	for i := 0; i < ntasks; i++ {
		var workadr string
	reselect:
		select {
		case workadr = <-registerChan:
			break
		default:
			if len(workadrs) == 0 {
				time.Sleep(time.Millisecond * 100)
				goto reselect
			}
			workadr = workadrs[0]
			lock.Lock()
			workadrs = workadrs[1:]
			lock.Unlock()
		}
		taskStruct := DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		}
		go func() {
			wg.Add(1)
			res := call(workadr, "Worker.DoTask", taskStruct, nil)
			lock.Lock()
			workadrs = append(workadrs, workadr)
			lock.Unlock()
			if !res {
				log.Error("worker failed",workadr)
				failwork <- taskStruct
				log.Error("start retry")
			}else {
				wg.Done()
			}

		}()
	}
	log.Info("start wait")
	wg.Wait()
	log.Info("stop wait")
	//kill <- true
	fmt.Printf("Schedule: %v done\n", phase)
}
