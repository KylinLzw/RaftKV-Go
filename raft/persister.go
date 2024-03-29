package raft

/*
支持 Raft 和 kraft 数据持久化
Raft 状态 (log &c) 和 k/v 服务器快照。
对外接口：
	Save(raftState []byte, snapshot []byte) ：持久化 Raft 状态和 KV 数据快照
	ReadRaftState() []byte ：获取 Raft 状态信息
	ReadSnapshot() []byte ：获取数据快照
*/

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftState []byte
	snapshot  []byte
}

// Save ：将Raft状态和K/V快照保存为单个原子动作，以帮助避免它们不同步。
func (ps *Persister) Save(raftState []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftState = clone(raftState)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftState)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftState = ps.raftState
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftState)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
