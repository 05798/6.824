package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term                   int
	Success                bool
	ConflictTerm           int
	FirstConflictTermIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log("AppendEntries-- received request %#v", args)
	reply.Term = rf.persistentState.CurrentTerm
	reply.Success = false
	if args.Term > rf.persistentState.CurrentTerm {
		rf.log("AppendEntries -- setting term to %v", args.Term)
		rf.persistentState.CurrentTerm = args.Term
		rf.persistentState.VotedFor = -1
		rf.persist()
		rf.role = Follower
	} else if args.Term < rf.persistentState.CurrentTerm {
		rf.log("AppendEntries -- responding with response %#v (request term out of date)", reply)
		return
	}
	if rf.role != Follower {
		rf.log("AppendEntries -- responding with response %#v (not a follower)", reply)
		return
	}
	rf.heartbeatCh <- true
	if args.PrevLogIndex > rf.getLastLogIndex() {
		rf.log("AppendEntries -- responding with response %#v (no log at PrevLogIndex %v)", reply, args.PrevLogIndex)
		return
	}
	if args.PrevLogIndex > 0 {
		prevLog := rf.getLogAtIndex(args.PrevLogIndex)
		rf.log("AA %#v", prevLog)
		if prevLog.Term != args.PrevLogTerm {
			rf.log("AppendEntries -- responding with response %#v (inconsistent log entry %#v at PrevLogIndex %v)", reply, prevLog, args.PrevLogIndex)
			reply.ConflictTerm = prevLog.Term
			reply.FirstConflictTermIndex = rf.getFirstIndexWithTerm(prevLog.Term)
			return
		}
	}
	rf.spliceLogsAtIndex(args.PrevLogIndex+1, args.Entries)
	if args.LeaderCommit > rf.volatileState.commitIndex {
		rf.volatileState.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}
	reply.Term = rf.persistentState.CurrentTerm
	reply.Success = true
	rf.log("AppendEntries -- responding with response %#v (successfully appended logs)", reply)
}
