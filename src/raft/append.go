package raft


func (rf *Raft) sendLogsToFollowers(argsById map[int]AppendEntriesArgs) {
	for i, args := range(argsById) {
		go func(peer int) {
			reply := AppendEntriesReply{}
			success := rf.sendAppendEntries(peer, &args, &reply)
			if success {
				rf.processAppendEntriesReply(args, reply, peer)
			}
		}(i)
	}
}

func (rf *Raft) prepareAllAppendEntriesArgs() map[int]AppendEntriesArgs {
	argsById := make(map[int]AppendEntriesArgs)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		argsById[i] = rf.prepareAppendEntriesArgs(i)
	}
	return argsById
}

func (rf *Raft) prepareAppendEntriesArgs(peer int) AppendEntriesArgs {
	prevLogIndex := rf.volatileState.nextIndex[peer] - 1
	var prevLogTerm int
	if prevLogIndex < 1 {
		prevLogTerm = 0
	} else {
		prevLogTerm = rf.getLogAtIndex(prevLogIndex).Term
	}
	entries := rf.getLogsSuffixFromIndex(prevLogIndex + 1)
	args := AppendEntriesArgs{
		Term:         rf.persistentState.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.volatileState.commitIndex,
	}
	rf.log("prepareAppendEntriesArgs -- sending request %#v to peer %v", args, peer)
	return args
}

func (rf *Raft) processAppendEntriesReply(args AppendEntriesArgs, reply AppendEntriesReply, peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.log("processAppendEntriesReply -- received response %#v from peer %v", reply, peer)
	if reply.Term > rf.persistentState.CurrentTerm {
		rf.role = Follower
		rf.persistentState.CurrentTerm = reply.Term
		rf.persist()
		return
	}
	if rf.role != Leader {
		return
	}
	if reply.Success {
		index := args.PrevLogIndex + len(args.Entries) + 1
		rf.log("processAppendEntriesReply -- updating nextIndex to %v and matchIndex to %v for peer %v", index, index-1, peer)
		rf.volatileState.nextIndex[peer] = index
		rf.volatileState.matchIndex[peer] = index - 1
		rf.updateLeaderCommitIndex()
	} else {
		rf.volatileState.nextIndex[peer] = max(1, reply.FirstConflictTermIndex)
	}
}

func (rf *Raft) updateLeaderCommitIndex() {
	totalCount := len(rf.peers)
	majorityCount := (totalCount + 1) / 2
	for n := rf.volatileState.commitIndex + 1; n <= rf.getLastLogIndex(); n++ {
		log := rf.getLogAtIndex(n)
		if log.Term != rf.persistentState.CurrentTerm {
			continue
		}
		count := 1
		for peer, matchIndex := range rf.volatileState.matchIndex {
			if peer == rf.me {
				continue
			}
			if matchIndex >= n {
				count++
			}
		}
		if count < majorityCount {
			break
		}
		rf.log("updateLeaderCommitIndex -- setting leader commit to %v with match indices %#v", n, rf.volatileState.matchIndex)
		rf.volatileState.commitIndex = n
	}
}
