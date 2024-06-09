package raft

import (
	"sync"
)

type Election struct {
	participantCount int
	mu               sync.Mutex
	successCount     int
	responseCount    int
}

func (e *Election) majority() int {
	return (e.participantCount + 1) / 2
}

func (e *Election) isPending() bool {
	majorityCount := e.majority()
	return (e.successCount < majorityCount) && ((e.responseCount - e.successCount) <= majorityCount)
}

func (e *Election) isSuccess() bool {
	return e.successCount >= e.majority()
}

func (rf *Raft) callElection(argsById map[int]RequestVoteArgs) {
	election := Election{participantCount: len(rf.peers), successCount: 1, responseCount: 1}
	cond := sync.NewCond(&election.mu)

	for i, args := range argsById {
		go func(peer int) {
			reply := RequestVoteReply{}
			success := rf.sendRequestVote(peer, &args, &reply)
			election.mu.Lock()
			election.responseCount += 1
			election.mu.Unlock()
			if success {
				rf.processRequestVoteReply(reply, &election, peer)
			}
			cond.Broadcast()
		}(i)
	}

	election.mu.Lock()
	for election.isPending() {
		cond.Wait()
	}
	election.mu.Unlock()
	rf.processElection(&election)
}

func (rf *Raft) prepareAllRequestVoteArgs() map[int]RequestVoteArgs {
	argsById := make(map[int]RequestVoteArgs)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		argsById[i] = rf.prepareRequestVoteArgs(i)
	}
	return argsById
}

func (rf *Raft) prepareRequestVoteArgs(peer int) RequestVoteArgs {
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.getLogAtIndex(lastLogIndex).Term
	}
	args := RequestVoteArgs{Term: rf.persistentState.CurrentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	rf.log("prepareRequestVoteArgs -- prepared %#v for peer %v", args, peer)
	return args
}

func (rf *Raft) processRequestVoteReply(reply RequestVoteReply, election *Election, peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.CurrentTerm > rf.persistentState.CurrentTerm {
		rf.log("processRequestVoteReply -- converting to follower since observed higher term %v from peer %v", reply.CurrentTerm, peer)
		rf.role = Follower
		rf.persistentState.CurrentTerm = reply.CurrentTerm
		rf.persistentState.VotedFor = -1
		rf.persist()
		return
	}
	if rf.role != Candidate {
		return
	}
	if reply.VoteGranted {
		rf.log("processRequestVoteReply -- received vote from peer %v", peer)
		election.mu.Lock()
		defer election.mu.Unlock()
		election.successCount += 1
	}
}

func (rf *Raft) processElection(election *Election) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Candidate {
		return
	}
	election.mu.Lock()
	defer election.mu.Unlock()
	if !election.isSuccess() {
		rf.log("processElection -- lost the election with %v votes", election.successCount)
		return
	}
	rf.log("processElection -- won the election with %v votes", election.successCount)
	rf.wonElectionCh <- true

}
