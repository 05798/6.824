package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labrpc"
	"io"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "6.824/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	StatusLeader    = iota
	StatusFollower  = iota
	StatusCandidate = iota
)

const (
	minTimeoutMillis   = 500
	timeoutRangeMillis = 500
)

type Log struct {
	Command interface{}
	Term    int
}

type PersistentState struct {
	currentTerm int
	votedFor    int // -1 if uninitialized
	log         []Log
}

type VolatileState struct {
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	persistentState *PersistentState
	volatileState   *VolatileState
	status          int

	lastLeaderMessageTime time.Time
	timeout               time.Duration
}

func (rf *Raft) sleep() {
	time.Sleep(50 * time.Millisecond)
}

func (rf *Raft) monitorTimeout() {
	for {
		isExpired := rf.isTimeoutExpired()
		if isExpired && !rf.isLeader() {
			rf.log("monitorTimeout -- converting to candidate")
			rf.status = StatusCandidate
			rf.callElection()
		}
		rf.sleep()
	}
}

func (rf *Raft) streamEntries() {
	for {
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()
		if status == StatusLeader {
			rf.doAppendEntries()
		}
		rf.sleep()
	}
}

func (rf *Raft) streamApplyMsg() {
	for {
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

		rf.sleep()
	}
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status == StatusLeader
}

func (rf *Raft) isTimeoutExpired() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Now().UTC().After(rf.lastLeaderMessageTime.Add(rf.timeout))
}

func (rf *Raft) callElection() {
	rf.mu.Lock()
	rf.persistentState.currentTerm += 1
	rf.lastLeaderMessageTime = time.Now().UTC()

	electionMu := sync.Mutex{}
	electionCond := sync.NewCond(&electionMu)
	totalCount := len(rf.peers)
	majorityCount := (totalCount + 1) / 2

	voteCount := 1
	replyCount := 1
	rf.mu.Unlock()

	for i := 0; i < totalCount; i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			lastLogIndex := rf.getLastLogIndex()
			lastLogTerm := 0
			if lastLogIndex > 0 {
				lastLogTerm = rf.getLogAtIndex(lastLogIndex).Term
			} 
			args := RequestVoteArgs{Term: rf.persistentState.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
			reply := RequestVoteReply{}
			rf.log("callElection -- sending request %#v to %v", args, peer)
			rf.mu.Unlock()
			success := rf.sendRequestVote(peer, &args, &reply)
			electionMu.Lock()
			defer electionMu.Unlock()
			rf.mu.Lock()
			defer rf.mu.Unlock()
			replyCount += 1
			if success && rf.status == StatusCandidate {
				rf.log("callElection -- received response %#v from %v", reply, peer)
				if reply.VoteGranted {
					voteCount += 1
				} else if reply.CurrentTerm > rf.persistentState.currentTerm {
					rf.persistentState.currentTerm = reply.CurrentTerm
					rf.status = StatusFollower
				}
			}
			electionCond.Broadcast()
		}(i)
	}

	electionMu.Lock()
	for voteCount < majorityCount && (replyCount-voteCount) <= majorityCount && !rf.isTimeoutExpired() {
		electionCond.Wait()
	}
	electionMu.Unlock()
	rf.mu.Lock()
	if rf.status != StatusCandidate {
		rf.mu.Unlock()
		return
	}
	if voteCount >= majorityCount {
		rf.log("callElection -- won the election with %v votes", voteCount)
		rf.status = StatusLeader
		rf.lastLeaderMessageTime = time.Now().UTC()
		rf.initialiseNextIndex()
		rf.initialiseMatchIndex()
		rf.mu.Unlock()
		rf.doAppendEntries()
	} else {
		rf.log("callElection -- lost the election with %v votes", voteCount)
		rf.mu.Unlock()
	}
}

func (rf *Raft) doAppendEntries() {
	rf.mu.Lock()
	totalCount := len(rf.peers)
	rf.mu.Unlock()

	appendMu := sync.Mutex{}
	appendCond := sync.NewCond(&appendMu)
	successCount := 1
	replyCount := 1
	majorityCount := (totalCount + 1) / 2
	for i := 0; i < totalCount; i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			for rf.isLeader() {
				rf.mu.Lock()
				prevLogIndex := rf.volatileState.nextIndex[peer] - 1
				var prevLogTerm int
				if prevLogIndex < 1 {
					prevLogTerm = 0
				} else {
					prevLogTerm = rf.getLogAtIndex(prevLogIndex).Term
				}
				entries := rf.getLogsSuffixFromIndex(prevLogIndex + 1)
				args := AppendEntriesArgs{
					Term:         rf.persistentState.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.volatileState.commitIndex,
				}
				reply := AppendEntriesReply{}
				rf.mu.Unlock()
				rf.log("doAppendEntries -- sending request %#v to peer %v", args, peer)
				success := rf.sendAppendEntries(peer, &args, &reply)
				if success && rf.isLeader() {
					rf.log("doAppendEntries -- received response %#v from peer %v", reply, peer)
					appendMu.Lock()
					defer appendMu.Unlock()
					defer appendCond.Broadcast()
					replyCount += 1
					rf.mu.Lock()
					if reply.Term > rf.persistentState.currentTerm {
						rf.status = StatusFollower
						rf.persistentState.currentTerm = reply.Term
						rf.mu.Unlock()
						break
					}
					if reply.Success {
						index := prevLogIndex + len(entries) + 1
						rf.log("doAppendEntries -- updating nextIndex to %v and matchIndex to %v for peer %v", index, index-1, peer)
						rf.volatileState.nextIndex[peer] = index
						rf.volatileState.matchIndex[peer] = index - 1
						successCount += 1
					} else {
						rf.volatileState.nextIndex[peer] = max(1, reply.FirstConflictTermIndex)
					}
					rf.mu.Unlock()
					break
				}
			}
		}(i)
	}

	appendMu.Lock()
	for successCount < majorityCount && (replyCount-successCount) <= majorityCount {
		appendCond.Wait()
	}
	appendMu.Unlock()
	rf.mu.Lock()
	if rf.status != StatusLeader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	rf.updateLeaderCommitIndex()
}

func (rf *Raft) updateLeaderCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persistentState.currentTerm, rf.status == StatusLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
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
	if args.Term > rf.persistentState.currentTerm {
		rf.log("RequestVote -- setting term to %v", args.Term)
		rf.persistentState.currentTerm = args.Term
		rf.persistentState.votedFor = -1
		rf.status = StatusFollower
	}
	reply.CurrentTerm = rf.persistentState.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.persistentState.currentTerm {
		rf.log("RequestVote -- responding with response %#v (request term out of date)", reply)
		return
	}
	if rf.status != StatusFollower {
		rf.log("RequestVote -- responding with response %#v (not a follower)", reply)
		return
	}
	if rf.persistentState.votedFor >= 0 && rf.persistentState.votedFor != args.CandidateId {
		rf.log("RequestVote -- responding with response %#v (already voted for %v)", reply, rf.persistentState.votedFor)
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
	rf.persistentState.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.log("RequestVote -- responding with response %#v (vote granted)", reply)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	FirstConflictTermIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.log("AppendEntries-- received request %#v", args)
	defer rf.mu.Unlock()
	if args.Term > rf.persistentState.currentTerm {
		rf.log("AppendEntries -- setting term to %v", args.Term)
		rf.persistentState.currentTerm = args.Term
		rf.persistentState.votedFor = -1
		rf.status = StatusFollower
	}
	reply.Term = rf.persistentState.currentTerm
	reply.Success = false
	if args.Term < rf.persistentState.currentTerm {
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
	reply.Term = rf.persistentState.currentTerm
	reply.Success = true
	rf.log("AppendEntries -- responding with response %#v (successfully appended logs)", reply)
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
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.getLastLogIndex() + 1
	term := rf.persistentState.currentTerm
	isLeader := rf.status == StatusLeader

	if isLeader {
		logs := []Log{{Term: term, Command: command}}
		rf.log("Start -- received command %#v", command)
		rf.spliceLogsAtIndex(index, logs)
		rf.log("Start -- responding with %v %v %v", index, term, isLeader)
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetOutput(io.Discard)
	peerCount := len(peers)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	persistentState := PersistentState{currentTerm: 0, votedFor: -1, log: make([]Log, 0)}
	rf.persistentState = &persistentState
	volatileState := VolatileState{commitIndex: 0, lastApplied: 0, nextIndex: make([]int, peerCount), matchIndex: make([]int, peerCount)}
	rf.volatileState = &volatileState
	rf.status = StatusFollower
	rf.applyCh = applyCh

	rf.lastLeaderMessageTime = time.Now().UTC()
	rf.timeout = time.Duration(minTimeoutMillis+rand.Intn(timeoutRangeMillis)) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.streamEntries()
	go rf.streamApplyMsg()
	go rf.monitorTimeout()

	return rf
}
