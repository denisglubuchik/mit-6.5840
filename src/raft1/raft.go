package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type LogEntry struct {
	Command interface{}
	Term    int
}

type RaftState string

const (
	Leader    RaftState = "leader"
	Candidate RaftState = "candidate"
	Follower  RaftState = "follower"
)

const (
	electionCheckInterval = 10 * time.Millisecond
	minElectionTimeout    = 400 * time.Millisecond
	electionTimeoutJitter = 300 * time.Millisecond
	heartbeatInterval     = 100 * time.Millisecond
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu               sync.Mutex          // Lock to protect shared access to this peer's state
	peers            []*labrpc.ClientEnd // RPC end points of all peers
	persister        *tester.Persister   // Object to hold this peer's persisted state
	me               int                 // this peer's index into peers[]
	state            RaftState
	electionDeadline time.Time
	applyCh          chan raftapi.ApplyMsg
	applyCond        *sync.Cond

	// persisted state
	currentTerm       int
	votedFor          int
	log               []LogEntry
	snapshot          []byte
	lastIncludedIndex int

	// volatile state
	commitIndex int
	lastApplied int

	// volatile on leaders
	nextIndex  []int
	matchIndex []int

	snapshotPending bool
}

func (rf *Raft) logOffset(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) termAt(index int) int {
	return rf.log[rf.logOffset(index)].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

func (rf *Raft) resetElectionTimerLocked() {
	jitter := time.Duration(rand.Int63n(int64(electionTimeoutJitter)))
	timeout := minElectionTimeout + jitter

	rf.electionDeadline = time.Now().Add(timeout)
}

func (rf *Raft) observeHigherTermLocked(term int) bool {
	if term <= rf.currentTerm {
		return false
	}

	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()
	return true
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.lastIncludedIndex) != nil {
		panic("cannot encode persisted Raft state")
	}

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
		panic("cannot decode persisted Raft state")
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.mu.Unlock()
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}

	offset := rf.logOffset(index)
	includedTerm := rf.termAt(index)

	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{Term: includedTerm}
	newLog = append(newLog, rf.log[offset+1:]...)

	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.snapshot = append([]byte(nil), snapshot...)

	rf.persist()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.observeHigherTermLocked(args.Term)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	rf.state = Follower
	rf.resetElectionTimerLocked()

	if args.LastIncludedIndex <= rf.lastApplied ||
       args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{Term: args.LastIncludedTerm}

	canKeepSuffix :=
		args.LastIncludedIndex <= rf.lastLogIndex() &&
			rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm

	if canKeepSuffix {
		offset := rf.logOffset(args.LastIncludedIndex)
		newLog = append(newLog, rf.log[offset+1:]...)
	}
	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)

	rf.snapshot = append([]byte(nil), args.Data...)
	rf.persist()
	rf.snapshotPending = true

	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.observeHigherTermLocked(args.Term)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	myLastIndex := rf.lastLogIndex()
	myLastTerm := rf.termAt(myLastIndex)

	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId

	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm &&
			args.LastLogIndex >= myLastIndex)

	if canVote && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimerLocked()
		rf.persist()
	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.observeHigherTermLocked(args.Term)
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.state = Follower
	rf.resetElectionTimerLocked()

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.XLen = rf.lastIncludedIndex + 1
		return
	}

	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.XLen = rf.lastLogIndex() + 1
		return
	}

	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		xTerm := rf.termAt(args.PrevLogIndex)
		xIndex := args.PrevLogIndex
		for xIndex > rf.lastIncludedIndex && rf.termAt(xIndex-1) == xTerm {
			xIndex--
		}
		reply.XTerm = xTerm
		reply.XIndex = xIndex
		return
	}

	for i, entry := range args.Entries {
		currLogIndex := args.PrevLogIndex + i + 1
		if currLogIndex <= rf.lastLogIndex() {
			if rf.termAt(currLogIndex) == entry.Term {
				continue
			}
			offset := rf.logOffset(currLogIndex)
			rf.log = append(rf.log[:offset], args.Entries[i:]...)
			break
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		break
	}
	rf.persist()

	lastIndex := args.PrevLogIndex + len(args.Entries)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastIndex)
		rf.applyCond.Signal()
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) updateCommitLocked() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.termAt(n) != rf.currentTerm {
			continue
		}
		count := 1
		for i, idx := range rf.matchIndex {
			if i == rf.me {
				continue
			}
			if idx >= n {
				count++
			}
		}
		if count >= len(rf.peers)/2+1 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			break
		}
	}
}

func (rf *Raft) replicateSnapshotToPeer(peer int, args InstallSnapshotArgs) bool {
	var reply InstallSnapshotReply
	if !rf.sendInstallSnapshot(peer, &args, &reply) {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.observeHigherTermLocked(reply.Term)
		return false
	}

	if rf.state != Leader || rf.currentTerm != args.Term {
		return false
	}

	if rf.matchIndex[peer] < args.LastIncludedIndex {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	if rf.nextIndex[peer] < args.LastIncludedIndex+1 {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}

	return true
}

func (rf *Raft) replicateToPeer(peer int) {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		lastLogIndex := rf.lastLogIndex()
		next := rf.nextIndex[peer]

		if next <= rf.lastIncludedIndex {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.termAt(rf.lastIncludedIndex),
				Data:              append([]byte(nil), rf.snapshot...),
			}

			rf.mu.Unlock()

			if !rf.replicateSnapshotToPeer(peer, args) {
				return
			}
			continue
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: next - 1,
			PrevLogTerm:  rf.termAt(next - 1),
			Entries:      rf.log[rf.logOffset(next):],
			LeaderCommit: rf.commitIndex,
		}

		rf.mu.Unlock()

		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(peer, &args, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.observeHigherTermLocked(reply.Term)
			rf.mu.Unlock()
			return
		}
		if rf.state != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			rf.matchIndex[peer] = lastLogIndex
			rf.nextIndex[peer] = lastLogIndex + 1
			rf.updateCommitLocked()

			rf.mu.Unlock()
			return
		} else {
			if reply.XLen > 0 {
				rf.nextIndex[peer] = reply.XLen
			} else {
				foundIndex := -1
				for i := lastLogIndex; i >= rf.lastIncludedIndex; i-- {
					if rf.termAt(i) == reply.XTerm {
						foundIndex = i
						break
					}
				}

				if foundIndex != -1 {
					rf.nextIndex[peer] = foundIndex + 1
				} else {
					rf.nextIndex[peer] = reply.XIndex
				}

			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for peer := range rf.peers {
		go func(peer int) {
			if peer == rf.me {
				return
			}

			rf.replicateToPeer(peer)
		}(peer)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for peer := range rf.peers {
		go func(peer int) {
			if peer == rf.me {
				return
			}
			rf.replicateToPeer(peer)
		}(peer)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	index := -1
	term := -1
	isLeader := rf.state == Leader

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	entry := LogEntry{Command: command, Term: rf.currentTerm}

	rf.log = append(rf.log, entry)
	index = rf.lastLogIndex()
	term = rf.currentTerm

	rf.persist()

	rf.mu.Unlock()

	rf.broadcastAppendEntries()

	return index, term, isLeader
}

func (rf *Raft) startElectionLocked() RequestVoteArgs {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimerLocked()
	rf.persist()

	return RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.termAt(rf.lastLogIndex()),
	}
}

func (rf *Raft) requestVoteFromPeer(
	peer int,
	args RequestVoteArgs,
	electionTerm int,
	votes *int,
) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(peer, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()

	if reply.Term > rf.currentTerm {
		rf.observeHigherTermLocked(reply.Term)
		rf.mu.Unlock()
		return
	}

	if rf.state != Candidate || rf.currentTerm != electionTerm {
		rf.mu.Unlock()
		return
	}

	becameLeader := false
	if reply.VoteGranted {
		(*votes)++

		if *votes >= len(rf.peers)/2+1 {
			rf.becomeLeaderLocked()
			becameLeader = true
		}
	}
	rf.mu.Unlock()

	if becameLeader {
		rf.broadcastHeartbeat()
	}
}

func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader

	lastPlusOne := rf.lastLogIndex() + 1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for peer := range rf.peers {
		rf.nextIndex[peer] = lastPlusOne
		rf.matchIndex[peer] = 0
	}
}

func (rf *Raft) ticker() {
	for true {
		time.Sleep(electionCheckInterval)

		rf.mu.Lock()

		expired := rf.state != Leader && time.Now().After(rf.electionDeadline)
		if !expired {
			rf.mu.Unlock()
			continue
		}

		args := rf.startElectionLocked()
		electionTerm := rf.currentTerm
		votes := 1

		rf.mu.Unlock()

		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			go rf.requestVoteFromPeer(
				peer,
				args,
				electionTerm,
				&votes,
			)
		}
	}
}

func (rf *Raft) heartbeatTicker() {
	for {
		time.Sleep(heartbeatInterval)
		rf.broadcastHeartbeat()
	}
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()

		for !rf.snapshotPending && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		var msg raftapi.ApplyMsg

		if rf.snapshotPending {
			msg = raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      append([]byte(nil), rf.snapshot...),
				SnapshotTerm:  rf.termAt(rf.lastIncludedIndex),
				SnapshotIndex: rf.lastIncludedIndex,
			}

			rf.snapshotPending = false
			rf.lastApplied = rf.lastIncludedIndex

		} else {
			rf.lastApplied++
			index := rf.lastApplied

			msg = raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.logOffset(index)].Command,
				CommandIndex: index,
			}
		}

		rf.mu.Unlock()
		rf.applyCh <- msg
	}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}}
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = append([]byte(nil), persister.ReadSnapshot()...)
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	rf.resetElectionTimerLocked()

	go rf.ticker()
	go rf.heartbeatTicker()
	go rf.applier()

	return rf
}
