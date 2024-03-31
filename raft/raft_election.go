package raft

/*
raft_election： Raft 心跳检测
RequestVote ：客户端 RPC 响应投票结果
sendRequestVote： 发送 RPC 请求获取投票
startElection：选举函数，对每个节点发送 RPC请求
*/
import (
	"fmt"
	"math/rand"
	"time"
)

// 重置选举发起时钟
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

// 判断选举时钟是否超时
func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// 判断当前日志是否大于候选者的日志
func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
	lastIndex, lastTerm := rf.log.last()

	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}
	return lastIndex > candidateIndex
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d, T%d, Last: [%d]T%d", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, VoteGranted: %v", reply.Term, reply.VoteGranted)
}

// RequestVote ：客户端 RPC 响应投票结果
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, VoteAsked, Args=%v", args.CandidateId, args.String())

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// align the term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return nil
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// check for votedFor
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Already voted to S%d", args.CandidateId, rf.votedFor)
		return nil
	}

	// check if candidate's last log is more up to date
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Candidate less up-to-date", args.CandidateId)
		return nil
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persistLocked()
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Vote granted", args.CandidateId)
	return nil
}

// 发送 RPC 请求获取投票
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	err := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return err == nil
}

// 选举函数，对每个节点发送 RPC请求
func (rf *Raft) startElection(term int) {
	votes := 0

	// 回调函数，等待选举结果响应
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)

		// handle the reponse
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Ask vote, Lost or error", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote Reply=%v", peer, reply.String())

		// align term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// check the context
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Lost context, abort RequestVoteReply", peer)
			return
		}

		// count the votes
		if reply.VoteGranted {
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
				go rf.replicationTicker(term)
			}

		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate[T%d] to %s[T%d], abort RequestVote", rf.role, term, rf.currentTerm)
		return
	}

	lastIdx, lastTerm := rf.log.last()
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
			continue
		}

		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote, Args=%v", peer, args.String())

		go askVoteFromPeer(peer, args)
	}
}

// 检测是否发起选举
func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350 milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
