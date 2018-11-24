package menghuibasic

/*
	Lab 1: Map Reduce
*/
/*
	JSON format for map reduce intermediate file
	{
		"IntermediateFileName": "inFile",
		"IntermediateCollection":
		{
			"word1": ["value1", "value2"],
			"word2": ["value3", "value4"]
		}

	}
*/
type MapReduceIntermediateFileJSON struct {
	IntermediateFileName   string
	IntermediateCollection map[string][]string
}

/*
	Lab 2: Raft
*/
type NullInt struct {
	Value int
	Valid bool
}
