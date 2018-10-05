package menghuibasic

/*
	Error handling
*/

/*
	Check error. If error exists, panic it.
*/
func CheckError(e error) {
	if e != nil {
		panic(e)
	}
}
