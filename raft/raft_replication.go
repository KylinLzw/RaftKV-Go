package raft

/*
raft_replication：日志同步
AppendEntries ： Follower节点回调函数
startReplication：对各个节点的日志同步请求，处理响应
*/

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

type LogEntry struct {
	Term         int         // the log entry's term
	CommandValid bool        // if it should be applied
	Command      interface{} // the command should be applied to the state machine
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// used to probe the match point
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	// used to update the follower's commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConfilictIndex int
	ConfilictTerm  int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader:%d T%d, Prev:[%d]T%d, New:[%d, %d], CommitIdx: %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess: %v, ConflictTerm: [%d]T%d", reply.Term, reply.Success, reply.ConfilictIndex, reply.ConfilictTerm)
}

// AppendEntries ： Follower节点回调函数
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return errors.New("peer is down")
	}

	MyLog(rf.me, rf.currentTerm, DDebug, "<- S%d, Appended, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	reply.Success = false

	// align the term
	if args.Term < rf.currentTerm {
		MyLog(rf.me, rf.currentTerm, DRLog, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return nil
	}
	if args.Term >= rf.currentTerm {
		MyLog(rf.me, rf.currentTerm, DDebug, "receive replication from S%d", args.LeaderId)
		rf.becomeFollowerLocked(args.Term)
	}

	defer func() {
		rf.resetElectionTimerLocked()
		if !reply.Success {
			MyLog(rf.me, rf.currentTerm, DRLog, "\033[34m<- S%d, Follower Conflict: [%d]T%d\033[0m", args.LeaderId, reply.ConfilictIndex, reply.ConfilictTerm)
			MyLog(rf.me, rf.currentTerm, DDebug, "<- S%d, Follower Log=%v", args.LeaderId, rf.log.String())
		}
	}()

	// return failure if prevLog not matched
	if args.PrevLogIndex >= rf.log.size() {
		reply.ConfilictTerm = InvalidTerm
		reply.ConfilictIndex = rf.log.size()
		MyLog(rf.me, rf.currentTerm, DRLog, "\033[34m<- S%d, Reject log, Follower log too short, Len:%d < Prev:%d\033[0m", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		return nil
	}
	if args.PrevLogIndex < rf.log.snapLastIdx {
		reply.ConfilictTerm = rf.log.snapLastTerm
		reply.ConfilictIndex = rf.log.snapLastIdx
		MyLog(rf.me, rf.currentTerm, DRLog, "\033[34m<- S%d, Reject log, Follower log truncated in %d\033[0m", args.LeaderId, rf.log.snapLastIdx)
		return nil
	}
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConfilictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConfilictIndex = rf.log.firstFor(reply.ConfilictTerm)
		MyLog(rf.me, rf.currentTerm, DRLog, "\033[34m<- S%d, Reject log, Prev log not match, [%d]: T%d != T%d\033[0m", args.LeaderId, args.PrevLogIndex, rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		return nil
	}

	// append the leader log entries to local
	reply.Success = true

	if len(args.Entries) != 0 {
		rf.log.appendFrom(args.PrevLogIndex, args.Entries)
		MyLog(rf.me, rf.currentTerm, DRLog, "\033[32mFollower accept logs: [%d, %d]\033[0m", args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries))
		MyLog(rf.me, rf.currentTerm, DRLog, "<- S%d, Appended, Args=%v", args.LeaderId, args.String())
		rf.persistLocked()
	}

	// hanle LeaderCommit
	if args.LeaderCommit > rf.commitIndex {
		MyLog(rf.me, rf.currentTerm, DApply, "\033[32mFollower update the commit index %d->%d\033[0m", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}
	return nil
}

// 调用日志同步 RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	err := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return err == nil
}

// 获取当前已经多数节点同步的日志下标
func (rf *Raft) getMajorityIndexLocked() int {
	tmpIndexes := make([]int, len(rf.peers))
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpIndexes))
	majorityIdx := (len(rf.peers) - 1) / 2
	MyLog(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx]
}

// 对各个节点的日志同步请求，处理响应
func (rf *Raft) startReplication(term int) bool {
	// 日志同步函数
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			MyLog(rf.me, rf.currentTerm, DDebug, "-> S%d, peer is down", peer)
			return
		}
		MyLog(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

		// align the term
		if reply.Term > rf.currentTerm {
			MyLog(rf.me, rf.currentTerm, DInfo, "-> S%d, receive bigger term:%d", peer, reply.Term)
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// check context lost
		if rf.contextLostLocked(Leader, term) {
			MyLog(rf.me, rf.currentTerm, DInfo, "\033[32m-> S%d, Context Lost, T%d:Leader->T%d:%s\033[0m", peer, term, rf.currentTerm, rf.role)
			return
		}

		// hanle the reply
		// probe the lower index if the prevLog not matched
		if !reply.Success {
			prevIndex := rf.nextIndex[peer]
			if reply.ConfilictTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConfilictIndex
			} else {
				firstIndex := rf.log.firstFor(reply.ConfilictTerm)
				if firstIndex != InvalidIndex {
					rf.nextIndex[peer] = firstIndex
				} else {
					rf.nextIndex[peer] = reply.ConfilictIndex
				}
			}
			// avoid unordered reply
			// avoid the late reply move the nextIndex forward again
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}

			nextPrevIndex := rf.nextIndex[peer] - 1
			nextPrevTerm := InvalidTerm
			if nextPrevIndex >= rf.log.snapLastIdx {
				nextPrevTerm = rf.log.at(nextPrevIndex).Term
			}
			MyLog(rf.me, rf.currentTerm, DSLog, "\033[34m-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d\033[0m",
				peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevIndex, nextPrevTerm)
			MyLog(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader log=%v", peer, rf.log.String())
			return
		}

		// update match/next index if log appended successfully
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries) // important
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// update the commitIndex
		majorityMatched := rf.getMajorityIndexLocked()
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == rf.currentTerm {
			MyLog(rf.me, rf.currentTerm, DApply, "\033[32mLeader update the commit index %d->%d\033[0m", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		MyLog(rf.me, rf.currentTerm, DInfo, "\033[32mLost Leader[T%d] to %s[T%d]\033[0m", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		if prevIdx < rf.log.snapLastIdx {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.snapLastIdx,
				LastIncludedTerm:  rf.log.snapLastTerm,
				Snapshot:          rf.log.snapshot,
			}
			MyLog(rf.me, rf.currentTerm, DDebug, "-> S%d, SendSnap, Args=%v", peer, args.String())
			go rf.installToPeer(peer, term, args)
			continue
		}

		prevTerm := rf.log.at(prevIdx).Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log.tail(prevIdx + 1),
			LeaderCommit: rf.commitIndex,
		}
		MyLog(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Args=%v", peer, args.String())
		go replicateToPeer(peer, args)
	}

	return true
}

// could only replcate in the given term
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replicateInterval)
	}
}
