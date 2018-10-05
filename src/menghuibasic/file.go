package menghuibasic

import "io/ioutil"

/*
	Read file as String
*/
func ReadFileAsString(fileName string) string {
	inFileData, err := ioutil.ReadFile(fileName)
	CheckError(err)
	return string(inFileData)
}
