package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) WriteLog(format string) {
	DPrintf("Raft Server %d: %s.", rf.me, format)
	return
}

/*
	If Log1 is more up-to-date than log2 ?
*/
func moreUpToDate(log1 []LogEntry, log2 []LogEntry) bool {
	if log1 == nil && log2 == nil {
		return true
	}
	if log1 == nil {
		return false
	}
	if log2 == nil {
		return true
	}
	if len(log1) == 0 && len(log2) == 0 {
		return true
	}
	if len(log1) == 0 {
		return false
	}
	if len(log2) == 0 {
		return true
	}
	lastEntry1 := log1[len(log1)-1]
	lastEntry2 := log2[len(log2)-1]
	return lastEntry1.Term >= lastEntry2.Term
}

/*
	if log is less up to date than this term
*/
func lessUpToDateThanTerm(term int, log2 []LogEntry) bool {
	if log2 == nil || len(log2) == 0 {
		return true
	}
	lastEntry2 := log2[len(log2)-1]
	return term >= lastEntry2.Term
}

func (rf *Raft) checkState(state State) bool {
	return rf.state == state
}
func (rf *Raft) checkTerm(term int) bool {
	return rf.currentTerm == term
}
