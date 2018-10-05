package menghuibasic

import (
	"encoding/json"
	"os"
)

/*
	Encode json data to file
*/
func EncodeJSONToFile(fileName string, data ...interface{}) {
	f, createErr := os.Create(fileName)
	CheckError(createErr)
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, v := range data {
		encodeErr := enc.Encode(v)
		CheckError(encodeErr)
	}
}

/*
	Open a json file and output structured data specified by interface v
*/
func ReadFileToJSON(fileName string, v interface{}) interface{} {
	data := ReadFileAsString(fileName)
	err := json.Unmarshal([]byte(data), &v)
	CheckError(err)
	return v
}
