package raft

/*
支持 Raft 和 kraft 数据持久化
Raft 状态 (log &c) 和 k/v 服务器快照。

Save(raftState []byte, snapshot []byte) ：持久化 Raft 状态和 KV 数据快照
ReadRaftState() []byte ：获取 Raft 状态信息
ReadSnapshot() []byte ：获取数据快照
*/

import (
	"sync"
)

//var raftStatePath = "./raftData.txt"
//var snapshotPath = "./snapData.txt"

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

	// 将 Raft 状态写入文件
	//err := os.WriteFile(raftStatePath, raftState, 0644)
	//if err != nil {
	//	// 处理错误
	//}
	//
	//// 将快照写入文件
	//err = os.WriteFile(snapshotPath, snapshot, 0644)
	//if err != nil {
	//	// 处理错误
	//}
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftState)

	// 从文件中读取 Raft 状态
	//data, err := os.ReadFile(raftStatePath)
	//if err != nil {
	//	// 处理错误
	//}
	//return clone(data)

}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)

	//data, err := os.ReadFile(snapshotPath)
	//if err != nil {
	// 处理错误
	//}
	//return clone(data)
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

/*
// 把数据持久化到硬盘中

type Persister struct {
	mu sync.Mutex
	// 假设持久化文件的路径是固定的
	raftStatePath string
	snapshotPath  string
}

func (ps *Persister) Save(raftState []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// 将 Raft 状态写入文件
	err := ioutil.WriteFile(ps.raftStatePath, raftState, 0644)
	if err != nil {
		// 处理错误
	}

	// 将快照写入文件
	err = ioutil.WriteFile(ps.snapshotPath, snapshot, 0644)
	if err != nil {
		// 处理错误
	}
}

func (ps *Persister) SaveAppend(raftState []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	file, err := os.OpenFile(ps.raftStatePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// 处理错误
	}
	defer file.Close()

	_, err = file.Write(raftState)
	if err != nil {
		// 处理错误
	}
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// 从文件中读取 Raft 状态
	data, err := ioutil.ReadFile(ps.raftStatePath)
	if err != nil {
		// 处理错误
		return nil
	}
	return data
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// 从文件中读取快照
	data, err := ioutil.ReadFile(ps.snapshotPath)
	if err != nil {
		// 处理错误
		return nil
	}
	return data
}
*/
