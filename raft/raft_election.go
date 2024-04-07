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

	MyLog(rf.me, rf.currentTerm, DInfo, "Compare last log, Me: Log:[%d] Term:%d, Candidate: Log:[%d] Term%d",
		lastIndex, lastTerm, candidateIndex, candidateTerm)
	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}
	return lastIndex > candidateIndex
}

// 检测是否发起选举
func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			//rf.becomeCandidateLocked()
			//go rf.startElection(rf.currentTerm)

			// PreVote
			rf.becomePreCandidateLocked()
			go rf.startCandidate(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350 milliseconds.
		//ms := 50 + (rand.Int63() % 300)

		// For show
		ms := 500 + (rand.Int63() % 1000)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// For Election =====================================================

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
	return fmt.Sprintf("Candidate:%d, Term:%d, Last:[%d] Term:%d", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("Term:%d, VoteGranted: %v", reply.Term, reply.VoteGranted)
}

// 选举函数，对每个节点发送 RPC请求
func (rf *Raft) startElection(term int) {
	votes := 0

	MyLog(rf.me, rf.currentTerm, DInfo, "\033[32mStart Election... Term: %d\033[0m", term)
	// 回调函数，等待选举结果响应
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)

		// handle the reponse
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			MyLog(rf.me, rf.currentTerm, DVote, "-> S%d, Ask vote, Lost or error", peer)
			return
		}
		MyLog(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote Reply=%v", peer, reply.String())

		// align term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// check the context
		if rf.contextLostLocked(Candidate, term) {
			MyLog(rf.me, rf.currentTerm, DVote, "-> S%d, Lost context, abort Candidate election", peer)
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
		MyLog(rf.me, rf.currentTerm, DInfo, "Lost Candidate[Term:%d] to %s[T%d], abort RequestVote", rf.role, term, rf.currentTerm)
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
		MyLog(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote, Args=%v", peer, args.String())
		go askVoteFromPeer(peer, args)
	}
}

// 发送 RPC 请求获取投票
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	err := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return err == nil
}

// RequestVote ：客户端 RPC 响应投票结果
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	MyLog(rf.me, rf.currentTerm, DDebug, "<- S%d, VoteAsked, Args=%v", args.CandidateId, args.String())

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// align the term
	if args.Term < rf.currentTerm {
		MyLog(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return nil
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// check for votedFor
	if rf.votedFor != -1 {
		MyLog(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Already voted to S%d", args.CandidateId, rf.votedFor)
		return nil
	}

	// check if candidate's last log is more up to date
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		MyLog(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Candidate less up-to-date", args.CandidateId)
		return nil
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persistLocked()
	rf.resetElectionTimerLocked()
	MyLog(rf.me, rf.currentTerm, DVote, "\033[32m<- S%d, Vote granted\033[0m", args.CandidateId)
	return nil
}

// For PreVote: ===========================================================================

// PreVoteRequest :PreVote 请求结构
type PreVoteRequest struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// PreVoteReply :PreVote回复结构
type PreVoteReply struct {
	Term    int
	Granted bool
}

// 试探函数，对每个节点发送 RPC请求
func (rf *Raft) startCandidate(term int) {
	votes := 0
	MyLog(rf.me, rf.currentTerm, DInfo, "\033[32mStart PreVote... Term: %d\033[0m", term)

	// 回调函数，等待选举结果响应
	askPreVoteFromPeer := func(peer int, args *PreVoteRequest) {
		reply := &PreVoteReply{}
		ok := rf.sendRequestPreVote(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			MyLog(rf.me, rf.currentTerm, DVote, "-> S%d, Ask for PreVote, Lost or error", peer)
			return
		}

		// align term
		if reply.Term > rf.currentTerm {
			MyLog(rf.me, rf.currentTerm, DVote, "-> S%d, Ask for PreVote, have bigger term:%d", peer, reply.Term)
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		if rf.contextLostLocked(PreCandidate, term) {
			MyLog(rf.me, rf.currentTerm, DInfo, "-> S%d, Lost context, abort ProCandidate", peer)
			return
		}

		// count the votes
		if reply.Granted {
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeCandidateLocked()
				go rf.startElection(rf.currentTerm)
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(PreCandidate, term) {
		MyLog(rf.me, rf.currentTerm, DInfo, "Lost PreCandidate[T%d] to %s[T%d], abort RequestPreVote",
			term, rf.role, rf.currentTerm)
		return
	}

	lastIdx, lastTerm := rf.log.last()

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
			continue
		}
		args := &PreVoteRequest{
			Term:         rf.currentTerm + 1,
			CandidateId:  rf.me,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		}
		go askPreVoteFromPeer(peer, args)
	}
}

// 发送 RPC 请求获取投票
func (rf *Raft) sendRequestPreVote(server int, args *PreVoteRequest, reply *PreVoteReply) bool {
	err := rf.peers[server].Call("Raft.RequestPreVote", args, reply)
	if err != nil {
		MyLog(rf.me, rf.currentTerm, DDebug, "\033[31mRPC error:%s\033[0m", err)
	}
	return err == nil
}

// RequestPreVote ：客户端 RPC 响应投票结果
func (rf *Raft) RequestPreVote(args *PreVoteRequest, reply *PreVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Granted = false

	// align the term
	if args.Term < rf.currentTerm {
		MyLog(rf.me, rf.currentTerm, DVote, "<- S%d, Reject PreVoted, Higher term, Term:%d>Term:%d",
			args.CandidateId, rf.currentTerm, args.Term)
		return nil
	}
	if args.Term > rf.currentTerm {
		MyLog(rf.me, rf.currentTerm, DVote, "<- S%d, Receive bigger term:%d", args.CandidateId, args.Term)
		rf.becomeFollowerLocked(args.Term)
	}

	// check for votedFor
	if rf.votedFor != -1 {
		MyLog(rf.me, rf.currentTerm, DVote, "<- S%d, Reject ProVoted, Already voted to S%d",
			args.CandidateId, rf.votedFor)
		return nil
	}

	// check if candidate's last log is more up to date
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		MyLog(rf.me, rf.currentTerm, DVote, "<- S%d, Reject PreVoted, PreCandidate less up-to-date", args.CandidateId)
		return nil
	}

	reply.Granted = true
	MyLog(rf.me, rf.currentTerm, DVote, "\033[32m<- S%d, ProVote granted\033[0m", args.CandidateId)
	return nil
}
