package raft

/*
提供日志压缩功能
Snapshot：进行日志压缩
InstallSnapshot： Follower 接收日志信息进行同步
installToPeer：Leader 发送日志信息请求同步
*/
import "fmt"

// Snapshot ：进行日志压缩
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "Couldn't snapshot before CommitIdx: %d>%d", index, rf.commitIndex)
		return
	}
	if index <= rf.log.snapLastIdx {
		LOG(rf.me, rf.currentTerm, DSnap, "Already snapshot in %d<=%d", index, rf.log.snapLastIdx)
		return
	}
	rf.log.doSnapshot(index, snapshot)
	rf.persistLocked()
}

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Last: [%d]T%d", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

// RPC
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	err := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return err == nil
}

// InstallSnapshot : Follower 接收日志信息进行同步
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, RecvSnapshot, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	// align the term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap, Higher Term: T%d>T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}
	if args.Term >= rf.currentTerm { // = handle the case when the peer is candidate
		rf.becomeFollowerLocked(args.Term)
	}

	// check if there is already a snapshot contains the one in the RPC
	if rf.log.snapLastIdx >= args.LastIncludedIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap, Already installed: %d>%d", args.LeaderId, rf.log.snapLastIdx, args.LastIncludedIndex)
		return
	}

	// install the snapshot in the memory/persister/app
	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	rf.persistLocked()
	rf.snapPending = true
	rf.applyCond.Signal()
}

// installToPeer ：Leader 发送日志信息请求同步已经压缩的日志
func (rf *Raft) installToPeer(peer, term int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
		return
	}
	LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, SendSnapshot, Reply=%v", peer, reply.String())

	// align the term
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}

	// check context lost
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
		return
	}

	// update match and next index
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}
}
