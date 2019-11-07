package mapreduce

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	log "log_manager"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int,       // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	infos, err := ioutil.ReadDir(".")
	if err != nil{
		log.Error(err)
	}
	var reduceNames []string
	//log.Info(len(infos))
	prefix := fmt.Sprintf("mrtmp.%s",jobName)
	for _,v := range infos {
		suffix:= fmt.Sprintf("%d",reduceTask)
		if strings.HasSuffix(v.Name(),suffix) && strings.HasPrefix(v.Name(),prefix){
			reduceNames = append(reduceNames,filepath.Join(".",v.Name()))
			//log.Info(v.Name())
		}
	}
	var kvs []KeyValue

	for _,name := range reduceNames{
		fd, err := os.Open(name)
		if err != nil {
			log.Error(err)
		}
		dec := json.NewDecoder(fd)
		if err != nil {
			log.Error(err)
		}
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		fd.Close()
	}

	sort.Sort(kvk(kvs))
	log.Info(len(kvs))
	out, err := os.Create(outFile)
	if err != nil {
		log.Error(err)
	}
	defer out.Close()
	enc := json.NewEncoder(out)
	var lk string
	var val []string
	for _, v := range kvs {
		if len(val) != 0 {
			if v.Key == lk {
				val = append(val, v.Value)
				continue
			}
			if err = enc.Encode(KeyValue{lk, reduceF(v.Key, val)}); err != nil {
				log.Error(err)
			}
			lk = ""
			val = val[:0]
		}
		val = append(val, v.Value)
		lk = v.Key
	}
	if len(val) != 0 {
		if err = enc.Encode(KeyValue{lk, reduceF(lk, val)}); err != nil {
			log.Error(err)
		}
	}

}
