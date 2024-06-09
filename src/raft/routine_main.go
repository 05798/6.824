package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) main() {
	for !rf.killed() {
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		switch role {
		case Follower:
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
			go rf.callElection()
			select {
			case <- rf.heartbeatCh:
				// we received a valid AppendEntry
			case <- rf.wonElectionCh:
				rf.mu.Lock()
				rf.role = Leader
				rf.initialiseNextIndex()
				rf.initialiseMatchIndex()
				rf.mu.Unlock()
			case <- time.After(generateRandomTimeout()):
				// Call another election
			}
		case Leader:
			go rf.sendLogsToFollowers()
			time.Sleep(50 * time.Millisecond)
		}
	}

}

func generateRandomTimeout() time.Duration {
	return time.Duration(minTimeoutMillis+rand.Intn(timeoutRangeMillis)) * time.Millisecond
}
