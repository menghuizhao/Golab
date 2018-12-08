package menghuibasic

import (
	"log"
	"sync"
)

const DebugFlag = true

func DePrintf(format string, a ...interface{}) (n int, err error) {
	if DebugFlag {
		log.Printf(format, a...)
	}
	return
}

func LockPrint(mu *sync.Mutex, info interface{}) {
	if DebugFlag {
		log.Printf("Lock %d", info)
	}
	mu.Lock()
}

func UnlockPrint(mu *sync.Mutex, info interface{}) {
	if DebugFlag {
		log.Printf("Unlock %d", info)
	}
	mu.Unlock()
}
