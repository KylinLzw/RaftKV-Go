package raft

/*
persistLock() ：持久化 Raft 节点相关的数据
readPersist(data []byte) ：恢复 Raft 节点的状态
*/

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// 格式化存储的数据
func (rf *Raft) persistString() string {
	return fmt.Sprintf("T%d, VotedFor: %d, Log: [0: %d)", rf.currentTerm, rf.votedFor, rf.log.size())
}

// 用于对数据序列化后调用 persister.Save() 保存数据
func (rf *Raft) persistLocked() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.persist(e)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.log.snapshot)
	LOG(rf.me, rf.currentTerm, DPersist, "Persist: %v", rf.persistString())
}

// 对数据反序列化，恢复以前保存的状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var currentTerm int
	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read currentTerm error: %v", err)
		return
	}
	rf.currentTerm = currentTerm

	var votedFor int
	if err := d.Decode(&votedFor); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read votedFor error: %v", err)
		return
	}
	rf.votedFor = votedFor

	if err := rf.log.readPersist(d); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read log error: %v", err)
		return
	}
	rf.log.snapshot = rf.persister.ReadSnapshot()

	if rf.log.snapLastIdx > rf.commitIndex {
		rf.commitIndex = rf.log.snapLastIdx
		rf.lastApplied = rf.log.snapLastIdx
	}
	LOG(rf.me, rf.currentTerm, DPersist, "Read from persist: %v", rf.persistString())
}
