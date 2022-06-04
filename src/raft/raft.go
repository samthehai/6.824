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

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

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
	state       RaftState

	// Volatile state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int

	applyCh chan ApplyMsg
	resetCh chan struct{}

	// newCommitReadyChan is an internal notification channel used by goroutines
	// that commit new entries to the log to notify that these entries may be sent
	// on commitChan.
	newCommitReadyChan chan struct{}
}

func (rf *Raft) goFunc(f func()) {
	go func() {
		f()
	}()
}

// // lastIndex returns the last index in log.
// func (rf *Raft) lastIndex() int {
// 	return len(rf.log)
// }

// // lastTerm returns the last index in log.
// func (rf *Raft) lastTerm() int {
// 	DPrintf("lastTerm log=%v", rf.log)
// 	lastIndex := rf.lastIndex()
// 	if lastIndex == 0 {
// 		return 0
// 	}
// 	return rf.log[lastIndex-1].Term
// }

// lastLogIndexAndTerm returns the last log index and the last log entry's term
// (or 0 if there's no log) for this server.
// Expects cm.mu to be locked.
func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	if len(rf.log) > 0 {
		lastIndex := len(rf.log)
		return lastIndex, rf.log[lastIndex-1].Term
	} else {
		return 0, 0
	}
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
// RequestVoteArgs is RequestVote arguments structure.
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
// RequestVoteReply is RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here (2A).
	// currentTerm, for candidate to update itself
	Term int
	// true means candidate received vote
	VoteGranted bool
}

//
// RequestVote function is invoked by candidates to gather votes (§5.2).
// - Arguments: RequestVoteArgs
// - Results: RequestVoteReply
// - Receiver implementation:
//   1. Reply false if term < currentTerm (§5.1)
//   2. If votedFor is null or candidateId, and
//   candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
//
// 	 Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
//   If the logs have last entries with different terms, then the log with the later term is more up-to-date.
//   If the logs end with the same term, then whichever log is longer is more up-to-date.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.mu.Unlock()

		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.votedFor = -1
	}

	isCandidateLogMoreUpdated := func() bool {
		lastLogIndex, lastTerm := rf.lastLogIndexAndTerm()
		if args.LastLogTerm > lastTerm {
			return true
		}

		if args.LastLogTerm == lastTerm {
			return args.LastLogIndex >= lastLogIndex
		}

		return false
	}()

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isCandidateLogMoreUpdated {
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		DPrintf("RPCRequestVote: id=%v; state=%v; term=%v votedFor=%v",
			rf.me, rf.state, rf.currentTerm, rf.votedFor)
		rf.mu.Unlock()

		rf.resetCh <- struct{}{}
		return
	}

	reply.VoteGranted = false

	rf.mu.Unlock()
}

// sendRequestVote send a RequestVote RPC to a server.
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
// AppendEntriesArgs is AppendEntries RPC arguments structure.
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
// AppendEntriesReply is AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
}

//
// AppendEntries is invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
// - Arguments: AppendEntriesArgs
// - Results: AppendEntriesReply
// - Receiver implementation:
//   1. Reply false if term < currentTerm (§5.1)
//   2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
//   3. If an existing entry conflicts with a new one (same index but different terms),
//   delete the existing entry and all that follow it (§5.3)
//   4. Append any new entries not already in the log
//   5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("RPCAppendEntries: idx=%v, args=%v, rf.log=%v, rf.commitIndex=%v, rf.lastApplied=%v",
		rf.me, args, rf.log, rf.commitIndex, rf.lastApplied)
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.mu.Unlock()

		rf.resetCh <- struct{}{}
		return
	}

	if args.PrevLogIndex > len(rf.log) ||
		(args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		reply.Success = false
		rf.mu.Unlock()

		rf.resetCh <- struct{}{}
		return
	}

	logInsertIndex := args.PrevLogIndex + 1
	newEntriesIndex := 0

	for {
		if logInsertIndex > len(rf.log) || newEntriesIndex >= len(args.Entries) {
			break
		}

		if logInsertIndex > 0 && rf.log[logInsertIndex-1].Term != args.Entries[newEntriesIndex].Term {
			break
		}

		logInsertIndex++
		newEntriesIndex++
	}

	DPrintf("RPCAppendEntries: idx=%v, logInsertIndex=%v, newEntriesIndex=%v",
		rf.me, logInsertIndex, newEntriesIndex)

	if newEntriesIndex < len(args.Entries) {
		DPrintf("RPCAppendEntries: idx=%v inserting entries %v from index %d", rf.me, args.Entries[newEntriesIndex:], logInsertIndex)
		rf.log = append(rf.log[:logInsertIndex-1], args.Entries[newEntriesIndex:]...)
		DPrintf("RPCAppendEntries: idx=%v log is now: %v", rf.me, rf.log)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		DPrintf("RPCAppendEntries: idx=%v sets commitIndex := %d", rf.me, rf.commitIndex)
		rf.newCommitReadyChan <- struct{}{}
	}

	rf.state = Follower
	rf.votedFor = -1
	DPrintf("RPCAppendEntries: id=%v; state=%v; term=%v votedFor=%v",
		rf.me, rf.state, rf.currentTerm, rf.votedFor)
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
	rf.mu.Lock()
	index = len(rf.log) + 1
	isLeader = rf.state == Leader
	term = rf.currentTerm

	// If command received from client: append entry to local log,
	// respond after entry applied to state machine (§5.3)
	if isLeader {
		DPrintf("Start: command=%v by leader=%v", command, rf.me)
		rf.log = append(rf.log, Entry{rf.currentTerm, command})
	}

	rf.mu.Unlock()

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
			// If election timeout elapses: start new election
			case <-time.After(electionTimeout):
				DPrintf("ticker: candidate=%v election timed out electionTimeout=%v", rf.me, electionTimeout)
				rf.mu.Lock()
				rf.runCandidate()
				rf.mu.Unlock()
			case <-rf.resetCh:
			}
		case Follower:
			select {
			// If election timeout elapses without receiving AppendEntries RPC from current leader or
			// granting vote to candidate:
			// convert to candidate
			case <-time.After(electionTimeout):
				rf.mu.Lock()
				rf.state = Candidate
				DPrintf("ticker: Follower -> Candidate: id=%v; state=%v; term=%v votedFor=%v",
					rf.me, rf.state, rf.currentTerm, rf.votedFor)
				rf.runCandidate()
				rf.mu.Unlock()
			case <-rf.resetCh:
			}
		case Leader:
			DPrintf("ticker: leader=%v run heartBeatTimeout=%v", rf.me, heartBeatTimeout)
			rf.mu.Lock()
			rf.runLeader()
			rf.mu.Unlock()

			time.Sleep(heartBeatTimeout)
		}
	}
}

// runCandidate (§5.2):
// on conversion to candidate, start election
func (rf *Raft) runCandidate() {
	rf.electSelf()
}

func (rf *Raft) electSelf() {
	// increment currentTerm
	rf.currentTerm++
	// vote for self
	rf.votedFor = rf.me

	var grantedVotes int32 = 1
	votesNeeded := int32(rf.quorumSize())
	lastLogIndex, lastTerm := rf.lastLogIndexAndTerm()

	req := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastTerm,
	}

	askPeer := func(idx int) {
		resp := RequestVoteReply{}
		ok := rf.sendRequestVote(idx, &req, &resp)
		if ok {
			rf.mu.Lock()
			// Check if the term is greater than ours, bail
			if resp.Term > rf.currentTerm {
				rf.becomeFollower(resp.Term)
				rf.votedFor = -1

				DPrintf("electSelf: resp.Term > rf.currentTerm - id=%v; state=%v; term=%v votedFor=%v",
					rf.me, rf.state, rf.currentTerm, rf.votedFor)
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
				// If votes received from majority of servers: become leader
				if atomic.LoadInt32(&grantedVotes) >= votesNeeded {
					rf.state = Leader
					rf.votedFor = -1

					// init nextIndex, matchIndex
					for idx := range rf.peers {
						rf.nextIndex[idx] = len(rf.log)
						rf.matchIndex[idx] = 0
					}

					DPrintf("electSelf: voteGranted - id=%v; state=%v; term=%v votedFor=%v",
						rf.me, rf.state, rf.currentTerm, rf.votedFor)
					rf.mu.Unlock()

					rf.resetCh <- struct{}{}
					return
				}
			}

			rf.mu.Unlock()
		}
	}

	// Send RequestVote RPCs to all other servers
	for idx := range rf.peers {
		if idx != rf.me {
			go askPeer(idx)
		}
	}

	DPrintf("electSelf: id=%v; state=%v; term=%v votedFor=%v",
		rf.me, rf.state, rf.currentTerm, rf.votedFor)
}

// runLeader
func (rf *Raft) runLeader() {
	// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
	// repeat during idle periods to prevent election timeouts (§5.2)
	rf.processHeartbeat()
}

func (rf *Raft) processHeartbeat() {
	if rf.state != Leader {
		return
	}

	savedCurrentTerm := rf.currentTerm

	askPeer := func(idx int) {
		rf.mu.Lock()
		ni := rf.nextIndex[idx]
		prevLogIndex := rf.dedcreaseNextIndex(ni)
		prevLogTerm := 0
		if prevLogIndex > 0 {
			fmt.Printf("idx = %v rf.nextIndex = %v rf.log = %v\n", idx, rf.nextIndex, rf.log)
			prevLogTerm = rf.log[prevLogIndex-1].Term
		}

		// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with
		// log entries starting at nextIndex
		var entries []Entry
		if ni > 0 {
			entries = rf.log[ni-1:]
		} else {
			entries = rf.log
		}

		fmt.Printf("idx=%v ni=%v rf.log=%v\n", idx, ni, rf.log)

		req := AppendEntriesArgs{
			Term:         savedCurrentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		DPrintf("askPeer: idx=%v, req=%v", idx, req)
		rf.mu.Unlock()

		var resp AppendEntriesReply

		if ok := rf.sendAppendEntries(idx, &req, &resp); ok {
			rf.mu.Lock()

			// If RPC request or response contains term T > currentTerm: set currentTerm = T,
			// convert to follower (§5.1)
			if resp.Term > rf.currentTerm {
				rf.becomeFollower(resp.Term)

				DPrintf("processHeartbeat: resp.Term > rf.currentTerm: id=%v; state=%v; term=%v votedFor=%v",
					rf.me, rf.state, rf.currentTerm, rf.votedFor)
				rf.mu.Unlock()

				return
			}

			if rf.state != Leader {
				DPrintf("processHeartbeat: rf.state != Leader: id=%v; state=%v; term=%v votedFor=%v",
					rf.me, rf.state, rf.currentTerm, rf.votedFor)
				rf.mu.Unlock()

				return
			}

			if savedCurrentTerm != resp.Term {
				DPrintf("processHeartbeat: savedCurrentTerm != resp.Term: id=%v; state=%v; term=%v votedFor=%v",
					rf.me, rf.state, rf.currentTerm, rf.votedFor)
				rf.mu.Unlock()

				return
			}

			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
			if !resp.Success {
				rf.nextIndex[idx] = rf.dedcreaseNextIndex(ni)
				DPrintf("processHeartbeat: reply from %d !success: nextIndex := %d", idx, rf.nextIndex[idx])
				rf.mu.Unlock()

				return
			}

			// If successful: update nextIndex and matchIndex for follower (§5.3)
			if ni == 0 {
				rf.nextIndex[idx] = len(entries) + 1
			} else {
				rf.nextIndex[idx] = ni + len(entries)
			}
			rf.matchIndex[idx] = rf.dedcreaseNextIndex(rf.nextIndex[idx])
			DPrintf("processHeartbeat: reply from %d success: nextIndex := %v, matchIndex := %v",
				idx, rf.nextIndex, rf.matchIndex)

			// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
			// and log[N].term == currentTerm:
			// set commitIndex = N (§5.3, §5.4).
			savedCommitIndex := rf.commitIndex
			for i := rf.commitIndex + 1; i <= len(rf.log); i++ {
				if rf.log[i-1].Term == rf.currentTerm {
					matchCount := 1
					for peerIdx := range rf.peers {
						if rf.matchIndex[peerIdx] >= i {
							matchCount++
						}
						if matchCount >= rf.quorumSize() {
							rf.commitIndex = i
						}
					}
				}
			}

			DPrintf("rf.commitIndex=%v, savedCommitIndex=%v", rf.commitIndex, savedCommitIndex)

			if rf.commitIndex != savedCommitIndex {
				DPrintf("processHeartbeat: leader sets commitIndex := %d", rf.commitIndex)
				rf.newCommitReadyChan <- struct{}{}
			}

			rf.mu.Unlock()
		}
	}

	// For each peer, request the vote
	for peerIndex := range rf.peers {
		if peerIndex != rf.me {
			go askPeer(peerIndex)
		}
	}
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
	rf.log = make([]Entry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.applyCh = applyCh
	rf.newCommitReadyChan = make(chan struct{}, 1)
	DPrintf("Make: id=%v; state=%v; term=%v votedFor=%v commitIndex=%v lastApplied=%v log=%v",
		rf.me, rf.state, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied, rf.log)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.commitChanSender()

	return rf
}

// commitChanSender is responsible for sending committed entries on
// cm.commitChan. It watches newCommitReadyChan for notifications and calculates
// which new entries are ready to be sent. This method should run in a separate
// background goroutine; cm.commitChan may be buffered and will limit how fast
// the client consumes new committed entries. Returns when newCommitReadyChan is
// closed.
func (rf *Raft) commitChanSender() {
	for range rf.newCommitReadyChan {
		// Find which entries we have to apply.
		rf.mu.Lock()
		savedLastApplied := rf.lastApplied
		var entries []Entry
		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
		if rf.commitIndex > rf.lastApplied {
			entries = rf.log[rf.lastApplied:rf.commitIndex]
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		DPrintf("commitChanSender: log=%v, savedLastApplied=%d lastApplied=%d commitIndex=%d entries=%v",
			rf.log, savedLastApplied, rf.lastApplied, rf.commitIndex, entries)

		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				Command:      entry.Command,
				CommandIndex: savedLastApplied + i + 1,
				CommandValid: true,
			}
		}
	}
	DPrintf("commitChanSender: done")
}

// becomeFollower makes raft a follower and resets its state.
// Expects rf.mu to be locked.
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
}

// dedcreaseNextIndex
func (rf *Raft) dedcreaseNextIndex(nextIndex int) int {
	if nextIndex > 0 {
		return nextIndex - 1
	}

	return 0
}
