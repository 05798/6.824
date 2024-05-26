package raft

import (
	"log"
	"fmt"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func toStatusString(status int) string {
	switch (status) {
		case StatusFollower: 
			return "Follower"
		case StatusCandidate: 
			return "Candidate"
		case StatusLeader:
			return "Leader"
		default: 
			log.Fatalf("Unknown status")
			return ""
	}
}

func (rf *Raft) log(formatSpecifier string, args ...interface{}) {
	status := toStatusString(rf.status)
	raftState := fmt.Sprintf("ID: %v | Status: %v | Term: %v", rf.me, status, rf.persistentState.currentTerm)
	insertedLog := fmt.Sprintf(formatSpecifier, args...)
	log.Printf("|| %v || %v", raftState, insertedLog)
}

func (rf *Raft) initialiseNextIndex() {
	nextIndex := rf.getLastLogIndex() + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.volatileState.nextIndex[i] = nextIndex
	}
}

func (rf *Raft) initialiseMatchIndex() {
	for i := 0; i < len(rf.peers); i++ {
		rf.volatileState.matchIndex[i] = 0
	}
}

func (rf *Raft) getLogAtIndex(index int) Log {
	// Indexes are 1 based unfortunately
	return rf.persistentState.log[index-1]
}

func (rf *Raft) getLogsSuffixFromIndex(index int) []Log {
	return rf.persistentState.log[index-1:]
}

func (rf *Raft) spliceLogsAtIndex(firstIndex int, logs []Log) {
	rf.log("spliceLogsAtIndex with %#v from first index %v", logs, firstIndex)
	rf.persistentState.log = append(rf.persistentState.log[:firstIndex-1], logs...)
	rf.log("spliceLogsAtIndex %#v", rf.persistentState.log)
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.persistentState.log)
}