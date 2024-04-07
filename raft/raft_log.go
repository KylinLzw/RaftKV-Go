package raft

/*
抽象日志类: [1, snapLastIdx]   [0][tailLog]
NewLog：根据传入数据创建日志
readPersist：恢复日志信息
persist：序列化日志数据
size：返回日志真实大小，包括压缩日志的内容
at：根据传入 id 返回在 tailLog 中的真实数据
doSnapshot：对日志根据检查点做日志快照
installSnapshot：从日志快照恢复日志数据
*/

import (
	"encoding/gob"
	"fmt"
)

type RaftLog struct {
	snapLastIdx  int
	snapLastTerm int

	// contains [1, snapLastIdx]
	snapshot []byte

	// contains index (snapLastIdx, snapLastIdx+len(tailLog)-1] for real data
	// contains index snapLastIdx for mock log entry
	tailLog []LogEntry
}

func NewLog(snapLastIdx, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIdx:  snapLastIdx,
		snapLastTerm: snapLastTerm,
		snapshot:     snapshot,
	}

	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})
	rl.tailLog = append(rl.tailLog, entries...)

	return rl
}

// all the functions below should be called under the protection of rf.mutex
// 恢复日志信息
func (rl *RaftLog) readPersist(d *gob.Decoder) error {
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapLastIdx = lastIdx

	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last include term failed")
	}
	rl.snapLastTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode tail log failed")
	}
	rl.tailLog = log

	return nil
}

// 序列化日志信息
func (rl *RaftLog) persist(e *gob.Encoder) {
	e.Encode(rl.snapLastIdx)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

// 返回日志真实大小
func (rl *RaftLog) size() int {
	return rl.snapLastIdx + len(rl.tailLog)
}

// 根据传入 id 返回在 tailLog 中的真实下标
func (rl *RaftLog) idx(logicIdx int) int {
	// if the logicIdx fall beyond [snapLastIdx, size()-1]
	if logicIdx < rl.snapLastIdx || logicIdx >= rl.size() {
		panic(fmt.Sprintf("%d is out of [%d, %d]", logicIdx, rl.snapLastIdx, rl.size()-1))
	}
	return logicIdx - rl.snapLastIdx
}

// 根据传入 id 返回在 tailLog 中的真实数据
func (rl *RaftLog) at(logicIdx int) LogEntry {
	return rl.tailLog[rl.idx(logicIdx)]
}

// 返回数据日志尾部信息
func (rl *RaftLog) last() (index, term int) {
	i := len(rl.tailLog) - 1
	return rl.snapLastIdx + i, rl.tailLog[i].Term
}

// 获取第一个和传入 term 相同 term 的日志下标
func (rl *RaftLog) firstFor(term int) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return idx + rl.snapLastIdx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

// 获取日志尾部数据
func (rl *RaftLog) tail(startIdx int) []LogEntry {
	if startIdx >= rl.size() {
		return nil
	}

	return rl.tailLog[rl.idx(startIdx):]
}

// 日志添加数据
func (rl *RaftLog) append(e LogEntry) {
	rl.tailLog = append(rl.tailLog, e)
}

// 日志添加多条数据
func (rl *RaftLog) appendFrom(logicPrevIndex int, entries []LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(logicPrevIndex)+1], entries...)
}

// 方便 debug 日志输出
func (rl *RaftLog) String() string {
	var terms string
	prevTerm := rl.snapLastTerm
	prevStart := rl.snapLastIdx
	for i := 0; i < len(rl.tailLog); i++ {
		if rl.tailLog[i].Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, rl.snapLastIdx+i-1, prevTerm)
			prevTerm = rl.tailLog[i].Term
			prevStart = i
		}
	}
	terms += fmt.Sprintf("[%d, %d]T%d", prevStart, rl.snapLastIdx+len(rl.tailLog)-1, prevTerm)
	return terms
}

// 对日志根据检查点做日志压缩，snapshot 会覆盖原来的数据
func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	if index <= rl.snapLastIdx {
		return
	}

	idx := rl.idx(index)

	rl.snapLastIdx = index
	rl.snapLastTerm = rl.tailLog[idx].Term
	rl.snapshot = snapshot

	newLog := make([]LogEntry, 0, rl.size()-rl.snapLastIdx)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog
}

// 构造一个新的压缩日志，tailLog为空
func (rl *RaftLog) installSnapshot(index, term int, snapshot []byte) {
	rl.snapLastIdx = index
	rl.snapLastTerm = term
	rl.snapshot = snapshot

	// make a new log array
	newLog := make([]LogEntry, 0, 1)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	rl.tailLog = newLog
}
