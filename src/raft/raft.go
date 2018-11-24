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
	rf.votedFor = NullInt{Value: 0, Valid: false}
	rf.state = Undefined
	// Your initialization code here (2A, 2B, 2C).
	rf.stateSafeStartup(Follower, followerStartup)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

// return <currentTerm, whether this server believes it is the leader>.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

/*
	Startup Processes
*/
func followerStartup(rf *Raft) {
	rf.WriteLog("Starting as Follower...")
	rf.withLock(func(rf *Raft) {
		rf.myVotes = 0
	})
	rf.initiateTimers(Follower)
	rf.startServiceForState(
		Follower,
		func(rf *Raft) {
			select {
			case <-rf.followerTimer.C:
				rf.WriteLog("Election timeout reaches. Starting an election.")
				rf.stateSafeStartup(Candidate, candidateStartup)
				break
			}
		},
	)
}
func (rf *Raft) listenElectionTimeout() {
	followerElectionTimeout := time.Duration(rand.Intn(175)+225) * time.Millisecond
	rf.WriteLog(
		fmt.Sprintf("Initialize timer: followerElelection timeout to be %d ms", followerElectionTimeout),
	)
	rf.followerTimer = TimerGroup{}
	rf.followerTimer.T = time.NewTimer(followerElectionTimeout)
	rf.followerTimer.StopChan = make(chan bool, 1)
	rf.followerTimer.ResetChan = make(chan bool, 1)
	select {
	case <-rf.followerTimer.T.C:
		rf.WriteLog("Election timeout reaches. Starting an election.")
		rf.stateSafeStartup(Candidate, candidateStartup)
		break
	case <-rf.followerTimer.StopChan:
		DeleteTimer(rf.followerTimer.T)
		close(rf.followerTimer.StopChan)
		close(rf.followerTimer.ResetChan)
		break
	case <-rf.followerTimer.ResetChan:
		DeleteTimer(rf.followerTimer.T)
		close(rf.followerTimer.StopChan)
		close(rf.followerTimer.ResetChan)
		rf.listenElectionTimeout()
	default:
	}
}
func (rf *Raft) listenLeaderElection() {

}
func candidateStartup(rf *Raft) {
	rf.WriteLog("Starting as Candidate...")
	rf.initiateTimers(Candidate)
	rf.startServiceForState(
		Candidate,
		func(rf *Raft) {
			select {
			case <-rf.candidateTimer.C:
				rf.WriteLog("Election time limit reaches. Starting another new election.")
				rf.leaderElection()
				break
			}
		},
	)
	rf.leaderElection()
}

func leaderStartup(rf *Raft) {
	rf.WriteLog("Starting as Leader...")
	rf.initiateTimers(Leader)
	rf.startServiceForState(
		Leader,
		func(rf *Raft) {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(i int) {
						appendEntriesArgs := &AppendEntriesArgs{
							LeaderId: rf.me,
							Term:     rf.currentTerm,
						}
						appendEntriesReply := &AppendEntriesReply{}
						rf.WriteLog(fmt.Sprintf("Sending append Entries to Raft Server %d", i))
						rf.sendAppendEntries(i, appendEntriesArgs, appendEntriesReply)
					}(i)
				}
			}
			time.Sleep(heartbeatInterval)
		},
	)
}

//needs refactoring, collect returns of RequestVote and retry, without waiting for timeout
func (rf *Raft) leaderElection() {
	rf.withLock(func(rf *Raft) {
		rf.currentTerm += 1
		rf.votedFor = NullInt{Value: 0, Valid: false}
		rf.WriteLog(fmt.Sprintf("Current term increased to %d", rf.currentTerm))
		rf.WriteLog(fmt.Sprintf("Start a leader election for term %d.", rf.currentTerm))
		rf.WriteLog("Votes for itself...")

		rf.votedFor = NullInt{Value: rf.me, Valid: true}
		rf.myVotes = 1
		rf.replyVoteCounter = 1
	})

	rf.WriteLog("Sending RequestVote calls to peers.")
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				rf.WriteLog(fmt.Sprintf("Sending Request Vote to Raft Server %d", i))
				requestVoteArgs := &RequestVoteArgs{
					CandidateId:   rf.me,
					CandidateTerm: rf.currentTerm,
				}
				requestVoteReply := &RequestVoteReply{}
				if rf.sendRequestVote(i, requestVoteArgs, requestVoteReply) {
					rf.WriteLog(
						fmt.Sprintf("RequestVote rpc call to server %d returned successfully.", i),
					)

					rf.withLock(func(rf *Raft) {
						rf.replyVoteCounter += 1
					})

					if requestVoteReply.Term > rf.currentTerm {
						rf.WriteLog(
							fmt.Sprintf("Found a higher term %d from server %d", requestVoteReply.Term, i),
						)
						rf.WriteLog(
							fmt.Sprintf("Therefore, elevate the current term to that number %d.", requestVoteReply.Term),
						)
						rf.withLock(func(rf *Raft) {
							rf.currentTerm = requestVoteReply.Term
							rf.votedFor = NullInt{Value: 0, Valid: false}
						})

						rf.WriteLog("Start over as Follower...")
						rf.stateSafeStartup(Follower, followerStartup)
					}

					if requestVoteReply.VoteGranted {
						rf.WriteLog(
							fmt.Sprintf("Server %d votes for me", i),
						)
						rf.withLock(func(rf *Raft) {
							rf.myVotes += 1
						})
						if rf.myVotes*2 >= len(rf.peers) {
							rf.WriteLog("Vote number reaches majority. Becoming a leader.")
							rf.stateSafeStartup(Leader, leaderStartup)
						}
					} else {
						if rf.replyVoteCounter == len(rf.peers) {
							rf.WriteLog("All votes returned. Didn't get majority nor return to follower. Starting another round.")
							rf.leaderElection()
						}
						rf.WriteLog(fmt.Sprintf("Server %d refused to vote for me", i))
					}

				} else {
					rf.WriteLog(fmt.Sprintf("RequestVote rpc call to server %d returned as a failure.", i))
				}
			}(i)
		}
	}
}

/*
	Helpers
*/
func (rf *Raft) initiateTimers(state State) {
	DeleteTimer(rf.followerTimer, rf.candidateTimer, rf.leaderTimer)
	switch state {
	case Follower:
		//225~400 ms
		followerElectionTimeout := time.Duration(rand.Intn(175)+225) * time.Millisecond
		rf.WriteLog(
			fmt.Sprintf("Initialize timer: followerElelection timeout to be %d ms", followerElectionTimeout),
		)
		rf.followerTimer = time.NewTimer(followerElectionTimeout)
		break
	case Candidate:
		rf.candidateTimer = time.NewTimer(candidateElectionTimeLimit)
		break
	case Leader:
		rf.leaderTimer = time.NewTimer(heartbeatInterval)
		break
	default:
	}
}

func (rf *Raft) stateSafeStartup(state State, action func(rf *Raft)) {
	if state != Follower && rf.state == state {
		return // need to reset timer
	}
	rf.withLock(func(rf *Raft) {
		rf.state = state
	})
	action(rf)
}

func (rf *Raft) startServiceForState(state State, action func(rf *Raft)) {
	go func(rf *Raft) {
		for rf.state == state {
			action(rf)
		}
	}(rf)
}

func (rf *Raft) withLock(update func(rf *Raft)) {
	rf.mu.Lock()
	update(rf)
	rf.mu.Unlock()
}

/*
	Handlers
*/

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.WriteLog(fmt.Sprintf("Received AppendEntries from server %d", args.LeaderId))
	reply.Term = rf.currentTerm
	reply.Success = false

	if rf.state == Follower {
		rf.WriteLog("so I refresh my follower state.")
		rf.stateSafeStartup(Follower, followerStartup)

	}
	if rf.state == Candidate {
		if args.Term >= rf.currentTerm {
			rf.withLock(func(rf *Raft) {
				rf.currentTerm = args.Term
				rf.votedFor = NullInt{Value: 0, Valid: false}
			})
			rf.WriteLog("so I stop my candidate state and convert to follower.")
			rf.stateSafeStartup(Follower, followerStartup)

		}
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.WriteLog(fmt.Sprintf("Received RequestVote request from server %d.", args.CandidateId))
	reply.Term = rf.currentTerm
	if args.CandidateTerm < rf.currentTerm {
		rf.WriteLog(fmt.Sprintf("Server %d's term is smaller than mine. Refuse to vote.", args.CandidateId))
		reply.VoteGranted = false
	} else {
		rf.withLock(func(rf *Raft) {
			rf.currentTerm = args.CandidateTerm
			rf.votedFor = NullInt{Value: 0, Valid: false}
		})
		if !rf.votedFor.Valid || (rf.votedFor.Valid && rf.votedFor.Value == args.CandidateId) {
			if lessUpToDateThanTerm(args.LastLogTerm, rf.log) {
				rf.WriteLog(fmt.Sprintf("Server %d's is qualified to be a leader from my point of view. Vote for it.", args.CandidateId))
				reply.VoteGranted = true
			}
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
