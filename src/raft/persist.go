package raft

import (
	"bytes"
	"log"
	"6.824/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// Assumes the lock has been acquired
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.persistentState)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.log("Persisted %#v", rf.persistentState)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var persistentState PersistentState
	if d.Decode(&persistentState) != nil {
		log.Fatal("Error decoding persistent state")
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persistentState = &persistentState
}
