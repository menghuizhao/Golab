package mapreduce

import (
	. "menghuibasic"
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

	/*
		Output data for result file, one reduce task has one result file.
	*/
	resultMap := make(map[string][]string)
	/*
		Iterate map tasks, for each map task, pick one of its intermediate file
	*/
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		var intermediate MapReduceIntermediateFileJSON
		ReadFileToJSON(fileName, &intermediate)
		for k, v := range intermediate.IntermediateCollection {
			if resultMap[k] == nil {
				resultMap[k] = make([]string, 0)
			}
			resultMap[k] = append(resultMap[k], v...)
		}
	}

	/*
		Result file should be like this
		{"Key":"a1","Value":"b1"}
		{"Key":"a2","Value":"b2"}
		{"Key":"a3","Value":"b3"}
		{"Key":"a4","Value":"b4"}
		Therefore, keys need to be sorted
	*/
	reducedResultMap := make(map[string]string)
	keys := make([]string, 0)
	for k, v := range resultMap {
		reducedResultMap[k] = reduceF(k, v)
		keys = append(keys, k)
	}
	sort.Strings(keys)
	keyValueList := make([]interface{}, 0)
	for _, key := range keys {
		keyValueList = append(keyValueList, KeyValue{key, reducedResultMap[key]})
	}

	EncodeJSONToFile(outFile, keyValueList...)
}
