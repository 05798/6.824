package raft

import (
	"fmt"
	"log"
	"time"
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
	switch status {
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
	raftState := fmt.Sprintf("ID: %v | Status: %v | Term: %v", rf.me, status, rf.persistentState.CurrentTerm)
	insertedLog := fmt.Sprintf(formatSpecifier, args...)
	log.Printf("|| %v || %v", raftState, insertedLog)
}

func (rf *Raft) sleep() {
	time.Sleep(50 * time.Millisecond)
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status == StatusLeader
}

func (rf *Raft) isCandidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status == StatusCandidate
}

func (rf *Raft) isTimeoutExpired() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Now().UTC().After(rf.lastLeaderMessageTime.Add(rf.timeout))
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log("sendRequestVote -- received response %#v from %v", reply, server)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log("sendAppendEntries -- received response %#v from %v", reply, server)
	return ok
}
