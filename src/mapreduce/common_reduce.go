package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
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

	// read one intermediate file from each map task
	var kvs []KeyValue
	for mapTask := 0; mapTask < nMap; mapTask++ {
		var tmpFilename = reduceName(jobName, mapTask, reduceTask)
		var fp, err = os.Open(tmpFilename)
		if err != nil {
			fmt.Printf("Reducer: open tmp file %v error\n", tmpFilename)
		}
		dec := json.NewDecoder(fp)
		for {
			var kv KeyValue
			var err = dec.Decode(&kv)
			if err != nil {
				// fmt.Printf("Reducer: decode json error in tmp file %v\n", tmpFilename)
				break
			}
			kvs = append(kvs, kv)
		}
	}
	// sort the intermediate key/value pairs by key
	sort.Sort(ByKey(kvs))

	// call the reduceF for each key
	var values []string
	var reduceKvs []KeyValue
	var prevKey string
	for _, kv := range kvs {
		if prevKey != "" && kv.Key != prevKey {
			reduceKvs = append(reduceKvs, KeyValue{Key: prevKey, Value: reduceF(prevKey, values)})
			values = []string{}
		}
		values = append(values, kv.Value)
		prevKey = kv.Key
	}
	if prevKey != "" {
		reduceKvs = append(reduceKvs, KeyValue{Key: prevKey, Value: reduceF(prevKey, values)})
	}
	// write reduceF's output to disk in json
	var resFilename = mergeName(jobName, reduceTask)
	var resFile, err = os.OpenFile(resFilename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Printf("Reducer: create result file %v error\n", resFilename)
	}
	var enc = json.NewEncoder(resFile)
	for _, kv := range reduceKvs {
		enc.Encode(kv)
	}
	resFile.Close()
}
