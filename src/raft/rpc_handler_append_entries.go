package raft

import "time"

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
	rf.log("AppendEntries-- received request %#v", args)
	defer rf.mu.Unlock()
	reply.Term = rf.persistentState.CurrentTerm
	reply.Success = false
	if args.Term > rf.persistentState.CurrentTerm {
		rf.log("AppendEntries -- setting term to %v", args.Term)
		rf.persistentState.CurrentTerm = args.Term
		rf.persistentState.VotedFor = -1
		rf.persist()
		rf.status = StatusFollower
	} else if args.Term < rf.persistentState.CurrentTerm {
		rf.log("AppendEntries -- responding with response %#v (request term out of date)", reply)
		return
	}
	rf.lastLeaderMessageTime = time.Now().UTC()
	if args.PrevLogIndex > rf.getLastLogIndex() {
		rf.log("AppendEntries -- responding with response %#v (no log at PrevLogIndex %v)", reply, args.PrevLogIndex)
		return
	}
	if args.PrevLogIndex > 0 {
		prevLog := rf.getLogAtIndex(args.PrevLogIndex)
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
