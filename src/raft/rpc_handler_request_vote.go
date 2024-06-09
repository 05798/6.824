package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	CurrentTerm int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	rf.log("RequestVote -- received request %#v", args)
	defer rf.mu.Unlock()
	if args.Term > rf.persistentState.CurrentTerm {
		rf.log("RequestVote -- setting term to %v", args.Term)
		rf.persistentState.CurrentTerm = args.Term
		rf.persistentState.VotedFor = -1
		rf.persist()
		rf.role = Follower
	}
	reply.CurrentTerm = rf.persistentState.CurrentTerm
	reply.VoteGranted = false
	if args.Term < rf.persistentState.CurrentTerm {
		rf.log("RequestVote -- responding with response %#v (request term out of date)", reply)
		return
	}
	if rf.role != Follower {
		rf.log("RequestVote -- responding with response %#v (not a follower)", reply)
		return
	}
	if rf.persistentState.VotedFor >= 0 && rf.persistentState.VotedFor != args.CandidateId {
		rf.log("RequestVote -- responding with response %#v (already voted for %v)", reply, rf.persistentState.VotedFor)
		return
	}
	lastLogIndex := rf.getLastLogIndex()
	if lastLogIndex > 0 {
		lastLogTerm := rf.getLogAtIndex(lastLogIndex).Term
		if lastLogTerm > args.LastLogTerm {
			rf.log("RequestVote -- responding with response %#v (have lastLogTerm %v)", reply, lastLogTerm)
			return
		}
		if lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
			rf.log("RequestVote -- responding with response %#v (have lastLogIndex %v)", reply, lastLogIndex)
			return
		}
	}
	rf.votedCh <- true
	rf.persistentState.VotedFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
	rf.log("RequestVote -- responding with response %#v (vote granted)", reply)
}
