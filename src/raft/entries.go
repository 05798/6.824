package raft

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
	return rf.persistentState.Log[index-1]
}

func (rf *Raft) getLogsSuffixFromIndex(index int) []Log {
	return rf.persistentState.Log[index-1:]
}

func (rf *Raft) spliceLogsAtIndex(firstIndex int, logs []Log) {
	rf.log("spliceLogsAtIndex with %#v from first index %v", logs, firstIndex)
	rf.persistentState.Log = append(rf.persistentState.Log[:firstIndex-1], logs...)
	rf.persist()
	rf.log("spliceLogsAtIndex %#v", rf.persistentState.Log)
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.persistentState.Log)
}

func (rf *Raft) getFirstIndexWithTerm(term int) int {
	for i, log := range rf.persistentState.Log {
		if log.Term == term {
			return i + 1
		}
	}
	return 0
}
