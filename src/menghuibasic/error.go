package menghuibasic

/*
	Error handling
*/

/*
	Check error
*/
func CheckError(e error) {
	if e != nil {
		panic(e)
	}
}
