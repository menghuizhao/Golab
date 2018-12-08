package raft

import (
	"fmt"
	"labrpc"
	"math/rand"
	. "menghuibasic"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

/*
	APIs
*/

// <index if committed, current term, if im leader>
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}
	// Your initialization code here (2A, 2B, 2C).
	rf.Lock(1)
	rf.roleChan = make(chan State, 1)
	rf.electionTimeout = getTimeout()
	rf.electionTimer = time.NewTimer(rf.electionTimeout)
	rf.WriteLog("Election timeout is set to %d", rf.electionTimeout)
	rf.votedFor = NullInt{Value: 0, Valid: false}
	rf.state = Follower
	rf.Unlock(1)
	go rf.stateMachine()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

// return <currentTerm, whether this server believes it is the leader>.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.Lock(2)
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.Unlock(2)
	return term, isleader
}

/*
	Startup Processes
*/
func (rf *Raft) stateMachine() {
	for {
		select {
		case <-rf.electionTimer.C:
			rf.WriteLog("Election timeout.")
			rf.Lock(3)
			switch rf.state {
			case Follower:
				rf.roleChan <- Candidate
				break
			case Candidate:
				rf.WriteLog("Restart election.")
				go rf.leaderElection()
				break
			case Leader:
				break
			default:
				panic("Unknown target type.")
			}
			rf.Unlock(3)
		case changeTo := <-rf.roleChan:
			switch changeTo {
			case Follower:
				rf.Lock(6)
				rf.state = changeTo
				rf.votedFor = NullInt{Value: 0, Valid: false}
				rf.resetElectionTimer()
				rf.Unlock(6)
				break
			case Candidate:
				rf.Lock(7)
				rf.state = changeTo
				rf.WriteLog("Start election.")
				go rf.leaderElection()
				rf.Unlock(7)
				break
			case Leader:
				rf.Lock(8)
				rf.state = changeTo
				go rf.sendHeartbeat()
				rf.Unlock(8)
				break
			default:
				panic("Unknown target type.")
			}
		}
	}
}

//needs refactoring, collect returns of RequestVote and retry, without waiting for timeout
func (rf *Raft) leaderElection() {
	rf.Lock(9)
	rf.currentTerm += 1
	rf.WriteLog(fmt.Sprintf("Current term increased to %d", rf.currentTerm))
	rf.WriteLog(fmt.Sprintf("Start a leader election for term %d.", rf.currentTerm))
	rf.WriteLog("Votes for itself...")
	rf.votedFor = NullInt{Value: rf.me, Valid: true}
	rf.myVotes = 1
	rf.resetElectionTimer()
	requestVoteArgs := &RequestVoteArgs{
		CandidateId:   rf.me,
		CandidateTerm: rf.currentTerm,
	}
	rf.Unlock(9)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int, args *RequestVoteArgs) {
				requestVoteReply := &RequestVoteReply{}
				if rf.sendRequestVote(i, requestVoteArgs, requestVoteReply) {
					rf.Lock(10)
					defer rf.Unlock(10)
					if !rf.checkAllServerRule(requestVoteReply.Term, i) {
						return
					}
					if requestVoteReply.VoteGranted {
						rf.WriteLog("Server %d votes for me", i)
						rf.myVotes++
					} else {
						rf.WriteLog("Server %d refused to vote for me", i)
					}
					rf.processVotes(requestVoteArgs.CandidateTerm)
				} else {
					rf.WriteLog(fmt.Sprintf("RequestVote rpc call to server %d returned as a failure.", i))
				}
			}(i, requestVoteArgs)
		}
	}
	// Process Votes
	rf.Lock(11)
	rf.processVotes()
	rf.Unlock(11)
}

func (rf *Raft) processVotes(termWhenSentRequest ...int) {
	if (len(termWhenSentRequest) == 1 && rf.currentTerm > termWhenSentRequest[0]) || rf.state != Candidate {
		rf.WriteLog("This election is stale. Ignore all results.")
		return
	}
	if rf.myVotes*2 >= len(rf.peers) {
		rf.WriteLog("Vote number reaches majority. Becoming a leader.")
		rf.roleChan <- Leader
	}

}

func (rf *Raft) sendHeartbeat() {
	for {
		rf.Lock(12)
		if rf.state != Leader {
			rf.Unlock(12)
			return
		}
		appendEntriesReply := &AppendEntriesReply{}
		appendEntriesArgs := &AppendEntriesArgs{
			LeaderId: rf.me,
			Term:     rf.currentTerm,
		}
		rf.Unlock(12)

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(i int, rf *Raft) {
					rf.WriteLog(fmt.Sprintf("Sending append Entries to Raft Server %d", i))
					if rf.sendAppendEntries(i, appendEntriesArgs, appendEntriesReply) {
						rf.Lock(13)
						defer rf.Unlock(13)
						if !rf.checkAllServerRule(appendEntriesReply.Term, i) {
							return
						}
					} else {
						rf.WriteLog("AppendEntries rpc call to server %d returned as a failure.", i)
					}
				}(i, rf)
			}
		}
		time.Sleep(heartbeatInterval)
	}

}

/*
	Helpers
*/

/*
	Handlers
*/

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock(14)
	defer rf.Unlock(14)
	rf.WriteLog("Received AppendEntries from server %d", args.LeaderId)
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		rf.WriteLog("Server %d's term is smaller than mine. Refuse to vote.", args.LeaderId)
		return
	}
	if !rf.checkAllServerRule(args.Term, args.LeaderId) {
		rf.roleChan <- Follower
		return
	}
	if rf.state == Follower {
		rf.roleChan <- Follower
		return
	}
	if rf.state == Candidate {
		rf.roleChan <- Follower
		return
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock(15)
	defer rf.Unlock(15)
	rf.WriteLog("Received RequestVote request from server %d.", args.CandidateId)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.CandidateTerm < rf.currentTerm {
		rf.WriteLog("Server %d's term is smaller than mine. Refuse to vote.", args.CandidateId)
		return
	}

	if !rf.checkAllServerRule(args.CandidateTerm, args.CandidateId) {
		/*
			Since you have just updated your current term and
			reset your votedFor for the new current term,
			you should definitely vote
		*/
		reply.VoteGranted = true
		rf.votedFor = NullInt{Value: args.CandidateId, Valid: true}
		return
	}
	//vote?
	if !rf.votedFor.Valid || (rf.votedFor.Valid && rf.votedFor.Value == args.CandidateId) {
		if lessUpToDateThanTerm(args.LastLogTerm, rf.log) {
			rf.WriteLog(fmt.Sprintf("Server %d's is qualified to be a leader from my point of view. Vote for it.", args.CandidateId))
			reply.VoteGranted = true
			rf.votedFor = NullInt{Value: args.CandidateId, Valid: true}
		}
	}
}

/*
	SendRequestVote message to peer
*/
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// blocking
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// blocking
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func getTimeout() time.Duration {
	return time.Duration(3+rand.Intn(22)) * heartbeatInterval
}

func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
			break
		default:
		}
	}
	rf.electionTimer.Reset(rf.electionTimeout)
}

func (rf *Raft) checkAllServerRule(term int, server int) bool {
	if term > rf.currentTerm {
		rf.WriteLog("Found a higher term %d from server %d", term, server)
		rf.currentTerm = term
		rf.roleChan <- Follower
		return false
	}
	return true
}
