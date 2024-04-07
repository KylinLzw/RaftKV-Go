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
		MyLog(rf.me, rf.currentTerm, DSnap, "\033[34mCouldn't snapshot before CommitIdx: %d>%d\033[0m", index, rf.commitIndex)
		return
	}
	if index <= rf.log.snapLastIdx {
		MyLog(rf.me, rf.currentTerm, DSnap, "\033[34mAlready snapshot in %d<=%d\033[0m", index, rf.log.snapLastIdx)
		return
	}
	MyLog(rf.me, rf.currentTerm, DSnap, "\033[32mDo Snap.....\033[0m")
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
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	MyLog(rf.me, rf.currentTerm, DDebug, "<- S%d, RecvSnapshot, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	// align the term
	if args.Term < rf.currentTerm {
		MyLog(rf.me, rf.currentTerm, DSnap, "\033[34m<- S%d, Reject Snap, Higher Term: T%d>T%d\033[0m", args.LeaderId, rf.currentTerm, args.Term)
		return nil
	}
	if args.Term >= rf.currentTerm { // = handle the case when the peer is candidate
		MyLog(rf.me, rf.currentTerm, DSnap, "\033[32mReceive InstallSnap from S%d\033[0m", args.LeaderId)
		rf.becomeFollowerLocked(args.Term)
	}

	// check if there is already a snapshot contains the one in the RPC
	if rf.log.snapLastIdx >= args.LastIncludedIndex {
		MyLog(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap, Already installed: %d>%d", args.LeaderId, rf.log.snapLastIdx, args.LastIncludedIndex)
		return nil
	}

	// install the snapshot in the memory/persister/app
	MyLog(rf.me, rf.currentTerm, DSnap, "\033[32mDo InstallSnap.....\033[0m")
	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	rf.persistLocked()
	rf.snapPending = true
	rf.applyCond.Signal()
	return nil
}

// installToPeer ：Leader 发送日志信息请求同步已经压缩的日志
func (rf *Raft) installToPeer(peer, term int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		MyLog(rf.me, rf.currentTerm, DSnap, "-> S%d, Lost or crashed", peer)
		return
	}
	MyLog(rf.me, rf.currentTerm, DDebug, "-> S%d, SendSnapshot, Reply=%v", peer, reply.String())

	// align the term
	if reply.Term > rf.currentTerm {
		MyLog(rf.me, rf.currentTerm, DSnap, "\033[32mReceive bigger term:%d\033[0m", reply.Term)
		rf.becomeFollowerLocked(reply.Term)
		return
	}

	// check context lost
	if rf.contextLostLocked(Leader, term) {
		MyLog(rf.me, rf.currentTerm, DInfo, "\033[32m-> S%d, Context Lost, T%d:Leader->T%d:%s\033[0m", peer, term, rf.currentTerm, rf.role)
		return
	}

	// update match and next index
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}
}
