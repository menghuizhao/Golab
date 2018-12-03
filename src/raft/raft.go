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
	rf.stopFollowerChan = make(chan bool, 1)
	rf.resetFollowerChan = make(chan bool, 1)
	rf.resetCandidateChan = make(chan bool, 1)
	rf.restartElectionChan = make(chan bool, 1)
	rf.stopCandidateChan = make(chan bool, 1)
	rf.stopLeaderChan = make(chan bool, 1)

	rf.mu.Lock()
	rf.WriteLog("lock 1")
	rf.votedFor = NullInt{Value: 0, Valid: false}
	rf.state = Undefined
	rf.mu.Unlock()
	rf.WriteLog("Unlock 1")

	rf.listenElectionTimeout()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

// return <currentTerm, whether this server believes it is the leader>.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	rf.WriteLog("lock 2")

	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	rf.WriteLog("Unlock 2")
	return term, isleader
}

/*
	Startup Processes
*/
func (rf *Raft) follower() {
	rf.mu.Lock()
	rf.WriteLog("lock 3")
	rf.state = Follower
	rf.stopFollowerChan = make(chan bool, 1)
	rf.resetFollowerChan = make(chan bool, 1)
	rf.mu.Unlock()
	rf.WriteLog("Unlock 3")

	go func(rf *Raft) {
		rf.listenElectionTimeout()
	}(rf)
}
func (rf *Raft) listenElectionTimeout() {
	rf.mu.Lock()
	rf.WriteLog("lock 4")
	followerElectionTimeout := time.Duration(rand.Intn(3950)+50) * time.Millisecond
	rf.WriteLog(
		fmt.Sprintf("Initialize timer: followerElelection timeout to be %d ms", followerElectionTimeout),
	)
	rf.followerTimer = time.NewTimer(followerElectionTimeout)
	rf.mu.Unlock()
	rf.WriteLog("Unlock 4")

	// Start a service to monitor election time out
	for {
		select {
		case <-rf.followerTimer.C:

			rf.mu.Lock()
			rf.WriteLog("lock 5")
			rf.WriteLog("Election timeout reaches. Starting an election.")
			rf.mu.Unlock()
			rf.WriteLog("Unlock 5")

			go func(rf *Raft) {
				rf.candidate()
			}(rf)
			return
		case <-rf.stopFollowerChan:

			rf.mu.Lock()
			rf.WriteLog("lock 6")
			StopTimer(rf.followerTimer)
			rf.mu.Unlock()
			rf.WriteLog("Unlock 6")

			return
		case <-rf.resetFollowerChan:

			rf.mu.Lock()
			rf.WriteLog("lock 7")
			StopTimer(rf.followerTimer)
			rf.mu.Unlock()
			rf.WriteLog("Unlock 7")

			go func(rf *Raft) {
				rf.listenElectionTimeout()
			}(rf)
			return
		default:
		}
	}
}

func (rf *Raft) candidate() {
	rf.mu.Lock()
	rf.WriteLog("lock 8")
	rf.state = Candidate
	rf.resetCandidateChan = make(chan bool, 1)
	rf.restartElectionChan = make(chan bool, 1)
	rf.stopCandidateChan = make(chan bool, 1)
	rf.mu.Unlock()
	rf.WriteLog("Unlock 8")

	go func(rf *Raft) {
		rf.listenLeaderElection()
	}(rf)
}

func (rf *Raft) listenLeaderElection() {

	rf.mu.Lock()
	rf.WriteLog("lock 9")
	rf.candidateTimer = time.NewTimer(candidateElectionTimeLimit)
	rf.mu.Unlock()
	rf.WriteLog("Unlock 9")

	go func(rf *Raft) {
		rf.leaderElection()
	}(rf)

	for {
		select {
		case <-rf.stopCandidateChan:

			rf.mu.Lock()
			rf.WriteLog("lock 10")
			StopTimer(rf.candidateTimer)
			rf.mu.Unlock()
			rf.WriteLog("Unlock 10")

			return
		case <-rf.resetCandidateChan:

			rf.mu.Lock()
			rf.WriteLog("lock 11")
			StopTimer(rf.candidateTimer)
			rf.candidateTimer = time.NewTimer(candidateElectionTimeLimit)
			rf.mu.Unlock()
			rf.WriteLog("Unlock 11")
			break

		case <-rf.candidateTimer.C:
			//case <- rf.restartElectionChan:
			rf.mu.Lock()
			rf.WriteLog("lock 12")
			StopTimer(rf.candidateTimer)
			rf.mu.Unlock()
			rf.WriteLog("Unlock 12")

			go func(rf *Raft) {
				rf.listenLeaderElection()
			}(rf)
			return
		default:
		}
	}
}

//needs refactoring, collect returns of RequestVote and retry, without waiting for timeout
func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	rf.WriteLog("lock 13")
	rf.currentTerm += 1
	rf.votedFor = NullInt{Value: 0, Valid: false}
	rf.WriteLog(fmt.Sprintf("Current term increased to %d", rf.currentTerm))
	rf.WriteLog(fmt.Sprintf("Start a leader election for term %d.", rf.currentTerm))
	rf.WriteLog("Votes for itself...")

	rf.votedFor = NullInt{Value: rf.me, Valid: true}
	rf.myVotes = 1
	rf.replyVoteCounter = 1
	requestVoteArgs := &RequestVoteArgs{
		CandidateId:   rf.me,
		CandidateTerm: rf.currentTerm,
	}
	rf.mu.Unlock()
	rf.WriteLog("Unlock 13")

	rf.WriteLog("Sending RequestVote calls to peers.")
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int, args *RequestVoteArgs) {
				rf.WriteLog(fmt.Sprintf("Sending Request Vote to Raft Server %d", i))
				requestVoteReply := &RequestVoteReply{}
				if rf.sendRequestVote(i, requestVoteArgs, requestVoteReply) {

					rf.WriteLog(
						fmt.Sprintf("RequestVote rpc call to server %d returned successfully.", i),
					)
					rf.mu.Lock()
					rf.WriteLog("lock 14")
					defer rf.mu.Unlock()
					defer rf.WriteLog("Unlock 1")

					if requestVoteReply.Term > rf.currentTerm {
						rf.stopCandidateChan <- true
						go func(rf *Raft) {
							rf.follower()
						}(rf)
						return
					}

					if rf.state != Candidate {
						return
					}
					// after reply is back. the currentTerm already updated and bigger than the term
					// when rpc was sent
					if rf.currentTerm > args.CandidateTerm {
						rf.WriteLog(
							fmt.Sprintf("Current term is %d. This election is stale and for term %d. Exit.", rf.currentTerm, args.CandidateTerm),
						)
						return // another
					}

					rf.replyVoteCounter += 1

					if requestVoteReply.Term > rf.currentTerm {
						rf.WriteLog(
							fmt.Sprintf("Found a higher term %d from server %d", requestVoteReply.Term, i),
						)
						rf.WriteLog(
							fmt.Sprintf("Therefore, elevate the current term to that number %d.", requestVoteReply.Term),
						)
						rf.currentTerm = requestVoteReply.Term
						rf.votedFor = NullInt{Value: 0, Valid: false}
						rf.WriteLog("Start over as Follower...")
						rf.stopCandidateChan <- true
						go func(rf *Raft) {
							rf.follower()
						}(rf)
						return
					}

					if requestVoteReply.VoteGranted {
						rf.WriteLog(
							fmt.Sprintf("Server %d votes for me", i),
						)
						rf.myVotes += 1
						if rf.myVotes*2 >= len(rf.peers) {
							rf.WriteLog("Vote number reaches majority. Becoming a leader.")
							rf.stopCandidateChan <- true
							go func(rf *Raft) {
								rf.leader()
							}(rf)
							return
						}
					} else {
						if rf.replyVoteCounter == len(rf.peers) {
							rf.WriteLog("All votes returned. Didn't get majority nor return to follower. Starting another round.")
							//rf.restartElectionChan <- true
						}
						rf.WriteLog(fmt.Sprintf("Server %d refused to vote for me", i))
					}

				} else {
					rf.WriteLog(fmt.Sprintf("RequestVote rpc call to server %d returned as a failure.", i))
				}
			}(i, requestVoteArgs)
		}
	}
}
func (rf *Raft) leader() {
	rf.mu.Lock()
	rf.WriteLog("lock 15")
	rf.state = Leader
	rf.stopLeaderChan = make(chan bool, 1)
	rf.mu.Unlock()
	rf.WriteLog("Unlock 15")

	go func(rf *Raft) {
		rf.sendHeartbeat()
	}(rf)
}

func (rf *Raft) sendHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int, rf *Raft) {
				for {
					rf.mu.Lock()
					rf.WriteLog("lock 16")
					if rf.state != Leader {
						rf.mu.Unlock()
						rf.WriteLog("Unlock 16")
						return
					}
					appendEntriesArgs := &AppendEntriesArgs{
						LeaderId: rf.me,
						Term:     rf.currentTerm,
					}
					rf.mu.Unlock()
					rf.WriteLog("Unlock 16")
					appendEntriesReply := &AppendEntriesReply{}
					rf.WriteLog(fmt.Sprintf("Sending append Entries to Raft Server %d", i))
					if rf.sendAppendEntries(i, appendEntriesArgs, appendEntriesReply) {
						rf.WriteLog(fmt.Sprintf("AppendEntries rpc call to server %d returned successfully.", i))
						rf.mu.Lock()
						rf.WriteLog("lock 17")

						if appendEntriesReply.Term > rf.currentTerm {
							rf.stopLeaderChan <- true
							go func(rf *Raft) {
								rf.follower()
							}(rf)
						}
						rf.mu.Unlock()
						rf.WriteLog("Unlock 17")

					} else {
						rf.WriteLog(fmt.Sprintf("AppendEntries rpc call to server %d returned as a failure.", i))
					}
					time.Sleep(heartbeatInterval)
				}

			}(i, rf)
		}
	}
}

/*
	Helpers
*/

/*
	Handlers
*/

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.WriteLog("lock 18")
	defer rf.mu.Unlock()
	defer rf.WriteLog("Unlock 18")

	rf.WriteLog(fmt.Sprintf("Received AppendEntries from server %d", args.LeaderId))
	reply.Term = rf.currentTerm
	reply.Success = false
	convertToFollower := false
	if args.Term < rf.currentTerm {
		rf.WriteLog(fmt.Sprintf("Server %d's term is smaller than mine. Refuse to vote.", args.LeaderId))
		return
	}
	if rf.state == Follower {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = NullInt{Value: 0, Valid: false}
		}
		rf.WriteLog("I refresh my follower state.")
		rf.resetFollowerChan <- true
	} else if rf.state == Candidate {
		if args.Term >= rf.currentTerm {
			rf.WriteLog("AllServer Rule: back to follower")
			rf.currentTerm = args.Term
			rf.votedFor = NullInt{Value: 0, Valid: false}
			rf.stopCandidateChan <- true
			convertToFollower = true
		}
	} else if rf.state == Leader {
		if args.Term >= rf.currentTerm {
			rf.WriteLog("AllServer Rule: back to follower")
			rf.currentTerm = args.Term
			rf.votedFor = NullInt{Value: 0, Valid: false}
			rf.stopLeaderChan <- true
			convertToFollower = true
		}
	}

	if convertToFollower {
		go func(rf *Raft) {
			rf.follower()
		}(rf)
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.WriteLog("lock 19")
	defer rf.mu.Unlock()
	defer rf.WriteLog("Unlock 19")

	rf.WriteLog(fmt.Sprintf("Received RequestVote request from server %d.", args.CandidateId))
	reply.Term = rf.currentTerm
	convertToFollower := false

	if args.CandidateTerm < rf.currentTerm {
		rf.WriteLog(fmt.Sprintf("Server %d's term is smaller than mine. Refuse to vote.", args.CandidateId))
		reply.VoteGranted = false
		return
	}

	if rf.state == Follower {
		if args.CandidateTerm > rf.currentTerm {
			rf.currentTerm = args.CandidateTerm
			rf.votedFor = NullInt{Value: 0, Valid: false}
			//vote definitely!
			reply.VoteGranted = true
			rf.votedFor = NullInt{Value: args.CandidateId, Valid: true}
		}
		//vote?
		if !rf.votedFor.Valid || (rf.votedFor.Valid && rf.votedFor.Value == args.CandidateId) {
			if lessUpToDateThanTerm(args.LastLogTerm, rf.log) {
				rf.WriteLog(fmt.Sprintf("Server %d's is qualified to be a leader from my point of view. Vote for it.", args.CandidateId))
				reply.VoteGranted = true
			}
		}
		if reply.VoteGranted {
			rf.WriteLog("I refresh my follower state.")
			rf.resetFollowerChan <- true
		}

	} else if rf.state == Candidate {
		if args.CandidateTerm > rf.currentTerm {
			rf.WriteLog("AllServer Rule: back to follower")
			rf.currentTerm = args.CandidateTerm
			rf.votedFor = NullInt{Value: 0, Valid: false}
			rf.stopCandidateChan <- true
			convertToFollower = true
			//vote definitely!
			reply.VoteGranted = true
			rf.votedFor = NullInt{Value: args.CandidateId, Valid: true}
		} else {
			//vote?
			if !rf.votedFor.Valid || (rf.votedFor.Valid && rf.votedFor.Value == args.CandidateId) {
				if lessUpToDateThanTerm(args.LastLogTerm, rf.log) {
					rf.WriteLog(fmt.Sprintf("Server %d's is qualified to be a leader from my point of view. Vote for it.", args.CandidateId))
					reply.VoteGranted = true
				}
			}
		}
	} else if rf.state == Leader {
		if args.CandidateTerm >= rf.currentTerm {
			rf.WriteLog("AllServer Rule: back to follower")
			rf.currentTerm = args.CandidateTerm
			rf.votedFor = NullInt{Value: 0, Valid: false}
			rf.stopLeaderChan <- true
			convertToFollower = true
			//vote definitely!
			reply.VoteGranted = true
			rf.votedFor = NullInt{Value: args.CandidateId, Valid: true}
		}
	}
	if convertToFollower {
		go func(rf *Raft) {
			rf.follower()
		}(rf)
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
