package raft

func (rf *Raft) streamToListener() {
	for !rf.killed() {
		messages := []ApplyMsg{}
		rf.mu.Lock()
		newLastApplied := rf.volatileState.commitIndex
		for i := rf.volatileState.lastApplied + 1; i <= rf.volatileState.commitIndex; i++ {
			rf.log("streamApplyMsg - sending index at %v with lastApplied %v and commitIndex %v", i, rf.volatileState.lastApplied, rf.volatileState.commitIndex)
			log := rf.getLogAtIndex(i)
			messages = append(messages, ApplyMsg{CommandValid: true, Command: log.Command, CommandIndex: i})
		}
		rf.mu.Unlock()

		for _, message := range messages {
			rf.applyCh <- message
		}

		rf.mu.Lock()
		rf.volatileState.lastApplied = newLastApplied
		rf.mu.Unlock()
	}
}
