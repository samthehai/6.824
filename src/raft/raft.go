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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const None int = -1
const heartBeatTimeout = time.Duration(100) * time.Millisecond

// Possible values for StateType.
const (
	Follower RaftState = iota
	Candidate
	Leader
	numStates
)

// Possible values for CampaignType
const (
	campaignElection CampaignType = "CampaignElection"
)

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// StateType represents the role of a peer
type RaftState int

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

var smap = [...]string{
	"Follower",
	"Candidate",
	"Leader",
}

func (s RaftState) String() string {
	return smap[uint64(s)]
}

type Entry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	// (Updated on stable storage before responding to RPCs)
	currentTerm int
	votedFor    int
	log         []Entry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	state RaftState

	resetCh chan struct{}
}

func (rf *Raft) goFunc(f func()) {
	go func() {
		f()
	}()
}

// func (rf *Raft) getRaftState() RaftState {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	return rf.state
// }

// func (rf *Raft) setRaftState(state RaftState) {
// 	rf.mu.Lock()
// 	rf.state = state
// 	rf.mu.Unlock()
// }

// func (rf *Raft) getCurrentTerm() int {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	return rf.currentTerm
// }

// func (rf *Raft) setCurrentTerm(term int) {
// 	rf.mu.Lock()
// 	rf.currentTerm = term
// 	rf.mu.Unlock()
// }

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// candidate’s term
	Term int
	// candidate requesting vote
	CandidateId int
	// index of candidate’s last log entry
	LastLogIndex int
	// term of candidate’s last log entry
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// currentTerm, for candidate to update itself
	Term int
	// true means candidate received vote
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.mu.Unlock()

		return
	}

	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = None
	}

	if rf.votedFor == None || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		DPrintf("RPCRequestVote: id=%v; state=%v; term=%v votedFor=%v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
		rf.mu.Unlock()

		rf.resetCh <- struct{}{}
		return
	}

	reply.VoteGranted = false

	rf.mu.Unlock()
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// leader’s term
	Term int
	// so follower can redirect clients
	LeaderId int
	// index of log entry immediately preceding new ones
	PrevLogIndex int
	// term of prevLogIndex entry
	PrevLogTerm int
	// log entries to store (empty for heartbeat;
	// may send more than one for efficiency)
	Entries []Entry
	// leader’s commitIndex
	LeaderCommit int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		reply.Success = false
		rf.mu.Unlock()

		rf.resetCh <- struct{}{}
		return
	}

	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.votedFor = None
		rf.currentTerm = args.Term
	}

	rf.state = Follower
	rf.votedFor = None
	DPrintf("RPCAppendEntries: id=%v; state=%v; term=%v votedFor=%v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
	reply.Success = true

	rf.mu.Unlock()

	rf.resetCh <- struct{}{}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) quorumSize() int {
	voters := len(rf.peers)
	return voters/2 + 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		electionTimeout := time.Duration(globalRand.Intn(150)+350) * time.Millisecond

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Candidate:
			select {
			case <-time.After(electionTimeout):
				rf.mu.Lock()
				rf.runCandidate()
				rf.mu.Unlock()
			case <-rf.resetCh:
			}
		case Follower:
			DPrintf("electionTimeout=%v", electionTimeout)
			select {
			case <-time.After(electionTimeout):
				rf.mu.Lock()
				rf.state = Candidate
				DPrintf("ticker Follower -> Candidate: id=%v; state=%v; term=%v votedFor=%v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
				rf.runCandidate()
				rf.mu.Unlock()
			case <-rf.resetCh:
			}
		case Leader:
			rf.mu.Lock()
			rf.runLeader()
			rf.mu.Unlock()

			time.Sleep(heartBeatTimeout)
		}
	}
}

// func (rf *Raft) runFollower() {}

func (rf *Raft) runCandidate() {
	rf.electSelf()
}

func (rf *Raft) runLeader() {
	rf.processHeartbeat()
}

func (rf *Raft) processHeartbeat() {
	req := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	askPeer := func(idx int) {
		resp := AppendEntriesReply{}
		ok := rf.sendAppendEntries(idx, &req, &resp)
		if ok {
			rf.mu.Lock()
			// Check if the term is greater than ours, bail
			if resp.Term > rf.currentTerm {
				rf.state = Follower
				rf.currentTerm = resp.Term
				DPrintf("processHeartbeat resp.Term > rf.currentTerm: id=%v; state=%v; term=%v votedFor=%v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
				rf.mu.Unlock()

				return
			}

			rf.mu.Unlock()
		}
	}

	// For each peer, request the vote
	for idx := range rf.peers {
		if idx != rf.me {
			go askPeer(idx)
		}
	}
}

func (rf *Raft) electSelf() {
	rf.currentTerm++
	rf.votedFor = rf.me

	var grantedVotes int32 = 1
	votesNeeded := int32(rf.quorumSize())

	req := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	askPeer := func(idx int) {
		resp := RequestVoteReply{}
		ok := rf.sendRequestVote(idx, &req, &resp)
		if ok {
			rf.mu.Lock()

			// Check if the term is greater than ours, bail
			if resp.Term > rf.currentTerm {
				rf.state = Follower
				rf.currentTerm = resp.Term
				rf.votedFor = None
				DPrintf("electSelf resp.Term > rf.currentTerm: id=%v; state=%v; term=%v votedFor=%v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
				rf.mu.Unlock()

				rf.resetCh <- struct{}{}
				return
			}

			if rf.state != Candidate || rf.currentTerm != req.Term {
				rf.mu.Unlock()

				return
			}

			if resp.VoteGranted {
				atomic.AddInt32(&grantedVotes, 1)
				// Check if become the leader
				if atomic.LoadInt32(&grantedVotes) >= votesNeeded {
					rf.state = Leader
					rf.votedFor = None
					DPrintf("electSelf VoteGranted: id=%v; state=%v; term=%v votedFor=%v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
					rf.mu.Unlock()

					rf.resetCh <- struct{}{}
					return
				}
			}

			rf.mu.Unlock()
		}
	}

	// For each peer, request the vote
	for idx := range rf.peers {
		if idx != rf.me {
			go askPeer(idx)
		}
	}

	DPrintf("electSelf: id=%v; state=%v; term=%v votedFor=%v", rf.me, rf.state, rf.currentTerm, rf.votedFor)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.resetCh = make(chan struct{}, 1)
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.state = Follower
	rf.log = make([]Entry, 1)
	DPrintf("Make: id=%v; state=%v; term=%v votedFor=%v", rf.me, rf.state, rf.currentTerm, rf.votedFor)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
