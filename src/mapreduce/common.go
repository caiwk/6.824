package mapreduce

import (
	"fmt"
	"strconv"
)

// Debugging enabled?
const debugEnabled = false

// debug() will only print if debugEnabled is true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "mapPhase"
	reducePhase          = "reducePhase"
)
type kvk  []KeyValue
// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
}
func (a kvk) Len() int {
	return len(a)
}

func (a kvk) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a kvk) Less(i, j int) bool {
	//ii,err := strconv.Atoi(a[i].Key)
	//jj,err1 := strconv.Atoi(a[j].Key)
	//if err != nil || err1 != nil{
	//	log.Fatal(err ,err1)
	//}
	//return ii < jj
	return a[i].Key < a[j].Key
}
// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}
