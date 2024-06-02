package raft

import (
	"sync"
	"time"
)

func (rf *Raft) monitorTimeout() {
	for {
		isExpired := rf.isTimeoutExpired()
		if isExpired && !rf.isLeader() {
			rf.log("monitorTimeout -- converting to candidate")
			rf.status = StatusCandidate
			rf.callElection()
		}
		rf.sleep()
	}
}

func (rf *Raft) callElection() {
	rf.mu.Lock()
	rf.persistentState.CurrentTerm += 1
	rf.lastLeaderMessageTime = time.Now().UTC()

	election := QuorumOperation{participantCount: len(rf.peers), successCount: 1, responseCount: 1}
	cond := sync.NewCond(&election.mu)
	rf.mu.Unlock()

	for i := 0; i < election.participantCount; i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			args := rf.prepareRequestVoteArgs(peer)
			reply := RequestVoteReply{}
			success := rf.sendRequestVote(peer, &args, &reply)
			election.mu.Lock()
			election.responseCount += 1
			election.mu.Unlock()
			// TODO(LL): retries?
			if success {
				rf.processRequestVoteReply(reply, &election, peer)
			}
			cond.Broadcast()
		}(i)
	}

	election.mu.Lock()
	for election.isPending() && rf.isCandidate() && !rf.isTimeoutExpired() {
		cond.Wait()
	}
	election.mu.Unlock()
	rf.processElection(&election)
}

func (rf *Raft) prepareRequestVoteArgs(peer int) RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.getLogAtIndex(lastLogIndex).Term
	}
	args := RequestVoteArgs{Term: rf.persistentState.CurrentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	rf.log("prepareRequestVoteArgs -- prepared %#v for peer %v", args, peer)
	return args
}

func (rf *Raft) processRequestVoteReply(reply RequestVoteReply, election *QuorumOperation, peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != StatusCandidate {
		return
	}
	if reply.CurrentTerm > rf.persistentState.CurrentTerm {
		rf.log("processRequestVoteReply -- converting to follower since observed higher term %v from peer %v", reply.CurrentTerm, peer)
		rf.persistentState.CurrentTerm = reply.CurrentTerm
		rf.persist()
		rf.status = StatusFollower
		return
	}
	if reply.VoteGranted {
		// This should probably be a method of the Election struct but putting it here makes logging easier :)
		rf.log("processRequestVoteReply -- received vote from peer %v", peer)
		election.mu.Lock()
		defer election.mu.Unlock()
		election.successCount += 1
	} 
}

func (rf *Raft) processElection(election *QuorumOperation) {
	rf.mu.Lock()
	if rf.status != StatusCandidate {
		rf.mu.Unlock()
		return
	}
	election.mu.Lock()
	defer election.mu.Unlock()
	if election.isSuccess() {
		rf.log("processElection -- won the election with %v votes", election.successCount)
		rf.status = StatusLeader
		rf.lastLeaderMessageTime = time.Now().UTC()
		rf.initialiseNextIndex()
		rf.initialiseMatchIndex()
		rf.mu.Unlock()
		rf.sendLogsToFollowers()
	} else {
		rf.log("processElection -- lost the election with %v votes", election.successCount)
		rf.mu.Unlock()
	}
}
