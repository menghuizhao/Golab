package menghuibasic

import "log"

const DebugFlag = true

func DePrintf(format string, a ...interface{}) (n int, err error) {
	if DebugFlag {
		log.Printf(format, a...)
	}
	return
}
