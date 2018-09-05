package menghuibasic

import (
	"encoding/json"
	"os"
)

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

func ReadFileToJSON(fileName string, v interface{}) interface{} {
	data := ReadFileAsString(fileName)
	err := json.Unmarshal([]byte(data), &v)
	CheckError(err)
	return v
}
