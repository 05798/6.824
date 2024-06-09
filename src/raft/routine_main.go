package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) main() {
	for !rf.killed() {
		rf.mu.Lock()
		switch rf.role {
		case Follower:
			rf.mu.Unlock()
			select {
			case <- rf.heartbeatCh:
				// we received a valid AppendEntry
			case <- rf.votedCh:
				// we received a valid RequestVote
			case <- time.After(generateRandomTimeout()):
				rf.mu.Lock()
				rf.log("Becoming a candidate")
				rf.role = Candidate
				rf.mu.Unlock()
			}
		case Candidate:
			rf.persistentState.CurrentTerm += 1
			requestVoteArgsById := rf.prepareAllRequestVoteArgs()
			rf.mu.Unlock()
			go rf.callElection(requestVoteArgsById)
			select {
			case <- rf.heartbeatCh:
				// we received a valid AppendEntry
			case <- rf.wonElectionCh:
				rf.mu.Lock()
				rf.log("Becoming a leader")
				rf.role = Leader
				rf.initialiseNextIndex()
				rf.initialiseMatchIndex()
				rf.mu.Unlock()
			case <- time.After(generateRandomTimeout()):
				// Call another election
			}
		case Leader:
			appendEntriesArgsById := rf.prepareAllAppendEntriesArgs()
			rf.mu.Unlock()
			go rf.sendLogsToFollowers(appendEntriesArgsById)
			time.Sleep(50 * time.Millisecond)
		}
	}

}

func generateRandomTimeout() time.Duration {
	return time.Duration(minTimeoutMillis+rand.Intn(timeoutRangeMillis)) * time.Millisecond
}
