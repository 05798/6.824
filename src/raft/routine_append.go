package raft

import "sync"

func (rf *Raft) streamEntriesToFollowers() {
	for {
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()
		if status == StatusLeader {
			rf.sendLogsToFollowers()
		}
		rf.sleep()
	}
}

func (rf *Raft) sendLogsToFollowers() {
	rf.mu.Lock()
	operation := QuorumOperation{participantCount: len(rf.peers), successCount: 1, responseCount: 1}
	cond := sync.NewCond(&operation.mu)
	rf.mu.Unlock()

	for i := 0; i < operation.participantCount; i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			for rf.isLeader() {
				args := rf.prepareAppendEntriesArgs(peer)
				reply := AppendEntriesReply{}
				success := rf.sendAppendEntries(peer, &args, &reply)
				if success {
					rf.processAppendEntriesReply(args, reply, &operation, peer)
					cond.Broadcast()
					break
				}
			}
		}(i)
	}

	operation.mu.Lock()
	for operation.isPending() && rf.isLeader() {
		cond.Wait()
	}
	operation.mu.Unlock()
	rf.updateLeaderCommitIndex()
}

func (rf *Raft) prepareAppendEntriesArgs(peer int) AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

func (rf *Raft) processAppendEntriesReply(args AppendEntriesArgs, reply AppendEntriesReply, operation *QuorumOperation, peer int) {
	operation.mu.Lock()
	defer operation.mu.Unlock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != StatusLeader {
		return
	}

	rf.log("processAppendEntriesReply -- received response %#v from peer %v", reply, peer)
	operation.responseCount += 1
	if reply.Term > rf.persistentState.CurrentTerm {
		rf.status = StatusFollower
		rf.persistentState.CurrentTerm = reply.Term
		rf.persist()
		return
	}
	if reply.Success {
		index := args.PrevLogIndex + len(args.Entries) + 1
		rf.log("processAppendEntriesReply -- updating nextIndex to %v and matchIndex to %v for peer %v", index, index-1, peer)
		rf.volatileState.nextIndex[peer] = index
		rf.volatileState.matchIndex[peer] = index - 1
		operation.successCount += 1
	} else {
		rf.volatileState.nextIndex[peer] = max(1, reply.FirstConflictTermIndex)
	}
}

func (rf *Raft) updateLeaderCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != StatusLeader {
		return
	}
	totalCount := len(rf.peers)
	majorityCount := (totalCount + 1) / 2
	for n := rf.volatileState.commitIndex + 1; ; n++ {
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
			rf.log("updateLeaderCommitIndex -- setting leader commit to %v", n-1)
			rf.volatileState.commitIndex = n - 1
			break
		}
	}
}
