package raft

import (
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

const (
	//electionTimeoutMin time.Duration = 250 * time.Millisecond
	//electionTimeoutMax time.Duration = 400 * time.Millisecond
	//replicateInterval time.Duration = 30 * time.Millisecond

	// For show
	electionTimeoutMin time.Duration = 3000 * time.Millisecond
	electionTimeoutMax time.Duration = 5000 * time.Millisecond
	replicateInterval  time.Duration = 1000 * time.Millisecond
)

const (
	InvalidTerm  int = 0
	InvalidIndex int = 0
)

type Role string

const (
	Follower     Role = "Follower"
	PreCandidate Role = "PreCandidate"
	Candidate    Role = "Candidate"
	Leader       Role = "Leader"
)

// For Raft peer ===============================================================

// Raft :A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex    // Lock to protect shared access to this peer's state
	peers     []*rpc.Client // RPC end points of all peers
	persister *Persister    // Object to hold this peer's persisted state
	me        int           // this peer's index into peers[]
	dead      int32         // set by Kill()

	role        Role
	currentTerm int
	votedFor    int // -1 means vote for none

	// log in the Peer's local
	log *RaftLog

	// only used in Leader
	// every peer's view
	nextIndex  []int
	matchIndex []int

	// fields for apply loop
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
	snapPending bool
	applyCond   *sync.Cond

	electionStart   time.Time
	electionTimeout time.Duration // random
}

func Make(peers []*rpc.Client, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	// a dummy entry to aovid lots of corner checks
	rf.log = NewLog(InvalidIndex, InvalidTerm, nil, nil)

	// initialize the leader's view slice
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize the fields used for apply
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.snapPending = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	MyLog(rf.me, rf.currentTerm, DInfo,
		"\033[32mRaft init .....\033[0m")
	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.applicationTicker()

	return rf
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		MyLog(rf.me, rf.currentTerm, DError, "\033[31mNot leader, can't make Start\033[0m")
		return 0, 0, false
	}
	rf.log.append(LogEntry{
		CommandValid: true,
		Command:      command,
		Term:         rf.currentTerm,
	})
	MyLog(rf.me, rf.currentTerm, DRLog,
		"\033[32mLeader accept log:[%d] Term:%d\033[0m", rf.log.size()-1, rf.currentTerm)
	rf.persistLocked()

	return rf.log.size() - 1, rf.currentTerm, true
}

// For Role change =============================================================

func (rf *Raft) becomeFollowerLocked(term int) {
	if term < rf.currentTerm {
		MyLog(rf.me, rf.currentTerm, DError, "Can't become Follower, lower term: T%v", term)
		return
	}

	MyLog(rf.me, rf.currentTerm, DROLE,
		"role: %s -> Follower, term: %v->%v", rf.role, rf.currentTerm, term)
	rf.role = Follower
	shouldPersit := rf.currentTerm != term
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
	if shouldPersit {
		rf.persistLocked()
	}
}

func (rf *Raft) becomePreCandidateLocked() {
	if rf.role == Leader || rf.role == Candidate {
		MyLog(rf.me, rf.currentTerm, DError, "Leader or Candidate can't become PreCandidate")
		return
	}

	MyLog(rf.me, rf.currentTerm, DROLE,
		"role: %s -> PreCandidate, term: %v -> %v", rf.role, rf.currentTerm, rf.currentTerm+1)

	rf.role = PreCandidate
	rf.persistLocked()
}

func (rf *Raft) becomeCandidateLocked() {
	if rf.role != PreCandidate {
		MyLog(rf.me, rf.currentTerm, DError, "%s can't become Candidate", rf.role)
		return
	}

	MyLog(rf.me, rf.currentTerm, DROLE,
		"role: %s -> Candidate, term: %v -> %v", rf.role, rf.currentTerm, rf.currentTerm+1)

	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.persistLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		MyLog(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
	}

	MyLog(rf.me, rf.currentTerm, DROLE,
		"\033[32mrole: %s -> Leader, in term: %v\033[0m", rf.role, rf.currentTerm)

	rf.role = Leader
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.log.size()
		rf.matchIndex[peer] = 0
	}
}

func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
}

//For Raft State ==============================================================

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) GetRaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.becomeFollowerLocked(rf.currentTerm)
}

func (rf *Raft) Restart() {
	atomic.StoreInt32(&rf.dead, 0)
	go rf.electionTicker()
}
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
