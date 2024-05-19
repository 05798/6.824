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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"io"
	"6.824/labrpc"
)

// import "bytes"
// import "../labgob"

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
	minTimeoutMillis   = 300
	timeoutRangeMillis = 300
)

type PersistentState struct {
	currentTerm int
	votedFor    int // -1 if uninitialized
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	persistentState *PersistentState
	status          int

	lastLeaderMessageTime time.Time
	timeout               time.Duration
}

func (rf *Raft) main() {
	for {
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()
		switch status {
		case StatusFollower:
			rf.handleFollower()
		case StatusCandidate:
			rf.handleCandidate()
		case StatusLeader:
			rf.handleLeader()
		default:
			log.Fatal("Unknown status")
		}
	}

}

func (rf *Raft) handleFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isTimeoutExpired() {
		log.Printf("%v %v %#v handleFollower -- converting to candidate", rf.me, rf.status, rf.persistentState)
		rf.status = StatusCandidate
	}
	time.Sleep(10 * time.Millisecond)
}

func (rf *Raft) isTimeoutExpired() bool {
	return time.Now().UTC().After(rf.lastLeaderMessageTime.Add(rf.timeout))
}

func (rf *Raft) isTimeoutExpiredWithLocks() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.isTimeoutExpired()
}

func (rf *Raft) handleCandidate() {
	log.Printf("%v %v %#v handleCandidate -- calling an election", rf.me, rf.status, rf.persistentState)
	rf.callElection()
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
			log.Printf("%v %v %#v callElection -- sending request to %v", rf.me, rf.status, rf.persistentState, peer)
			args := RequestVoteArgs{Term: rf.persistentState.currentTerm, CandidateId: rf.me}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			success := rf.sendRequestVote(peer, &args, &reply)
			electionMu.Lock()
			defer electionMu.Unlock()
			rf.mu.Lock()
			defer rf.mu.Unlock()
			log.Printf("%v %v %#v callElection -- received response from %v", rf.me, rf.status, rf.persistentState, peer)
			replyCount += 1
			if success && reply.VoteGranted {
				log.Printf("%v %v %#v callElection -- received vote from %v", rf.me, rf.status, rf.persistentState, peer)
				voteCount += 1
			}
			electionCond.Broadcast()
		}(i)
	}

	electionMu.Lock()
	for voteCount < majorityCount && (replyCount-voteCount) <= majorityCount && !rf.isTimeoutExpiredWithLocks() {
		electionCond.Wait()
	}
	electionMu.Unlock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if voteCount >= majorityCount {
		log.Printf("%v %v %#v callElection -- won the election with %v votes", rf.me, rf.status, rf.persistentState, voteCount)
		rf.status = StatusLeader
		rf.lastLeaderMessageTime = time.Now().UTC()
	}
}

func (rf *Raft) handleLeader() {
	totalCount := len(rf.peers)
	for i := 0; i < totalCount; i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			log.Printf("%v %v %#v handleLeader -- sending request to %v", rf.me, rf.status, rf.persistentState, peer)
			args := AppendEntriesArgs{Term: rf.persistentState.currentTerm, LeaderId: rf.me}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			rf.sendAppendEntries(peer, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			log.Printf("%v %v %#v handleLeader -- received response from %v", rf.me, rf.status, rf.persistentState, peer)
		}(i)
	}
	time.Sleep(10 * time.Millisecond)
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
	// TODO log fields
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
	log.Printf("%v %v %#v RequestVote -- received args %#v", rf.me, rf.status, rf.persistentState, args)
	defer log.Printf("%v %v %#v RequestVote -- responding with reply %#v", rf.me, rf.status, rf.persistentState, reply)
	defer rf.mu.Unlock()
	if args.Term > rf.persistentState.currentTerm {
		log.Printf("%v %v %#v RequestVote -- setting term to %v", rf.me, rf.status, rf.persistentState, args.Term)
		rf.persistentState.currentTerm = args.Term
		rf.persistentState.votedFor = -1
		rf.status = StatusFollower
	}
	reply.CurrentTerm = rf.persistentState.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.persistentState.currentTerm {
		log.Printf("%v %v %#v RequestVote -- discarding request from candidate %v due to lesser term %v but currently on %v", rf.me, rf.status, rf.persistentState, args.CandidateId, args.Term, rf.persistentState.currentTerm)
		return
	}
	if rf.status != StatusFollower {
		log.Printf("%v %v %#v RequestVote -- discarding request from candidate %v on term %v since no longer follower", rf.me, rf.status, rf.persistentState, args.CandidateId, rf.persistentState.currentTerm)
		return
	}
	if rf.persistentState.votedFor >= 0 && rf.persistentState.votedFor != args.CandidateId {
		log.Printf("%v %v %#v RequestVote -- not voting for candidate %v on term %v since already voted for %v", rf.me, rf.status, rf.persistentState, args.CandidateId, rf.persistentState.currentTerm, rf.persistentState.votedFor)
		return
	}
	rf.persistentState.votedFor = args.CandidateId
	reply.VoteGranted = true
	log.Printf("%v %v %#v RequestVote -- voting for %v on term %v", rf.me, rf.status, rf.persistentState, args.CandidateId, args.Term)
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	log.Printf("%v %v %#v AppendEntries -- received args %#v", rf.me, rf.status, rf.persistentState, args)
	defer log.Printf("%v %v %#v AppendEntries -- responding with reply %#v", rf.me, rf.status, rf.persistentState, reply)
	defer rf.mu.Unlock()
	if args.Term > rf.persistentState.currentTerm {
		log.Printf("%v %v %#v AppendEntries -- setting term to %v", rf.me, rf.status, rf.persistentState, args.Term)
		rf.persistentState.currentTerm = args.Term
		rf.persistentState.votedFor = -1
		rf.status = StatusFollower
	}
	reply.Term = rf.persistentState.currentTerm
	reply.Success = false
	if args.Term < rf.persistentState.currentTerm {
		log.Printf("%v %v %#v AppendEntries -- discarding request from leader %v due to lesser term %v but currently on %v", rf.me, rf.status, rf.persistentState, args.LeaderId, args.Term, rf.persistentState.currentTerm)
		return
	}
	rf.lastLeaderMessageTime = time.Now().UTC()
	reply.Term = rf.persistentState.currentTerm
	reply.Success = true
	log.Printf("%v %v %#v AppendEntires -- appending entries from leader %v on term %v", rf.me, rf.status, rf.persistentState, args.LeaderId, rf.persistentState.currentTerm)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	persistentState := PersistentState{currentTerm: 0, votedFor: -1}
	rf.persistentState = &persistentState
	rf.status = StatusFollower

	rf.lastLeaderMessageTime = time.Now().UTC()
	rf.timeout = time.Duration(minTimeoutMillis+rand.Intn(timeoutRangeMillis)) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.main()

	return rf
}
