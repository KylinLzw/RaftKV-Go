package shardkv

import (
	"fmt"
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrWrongConfig = "ErrWrongConfig"
	ErrNotReady    = "ErrNotReady"
	ErrServerDown  = "ErrServerDown"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

//const (
//	ClientRequestTimeout   = 500 * time.Millisecond
//	FetchConfigInterval    = 100 * time.Millisecond
//	ShardMigrationInterval = 50 * time.Millisecond
//	ShardGCInterval        = 50 * time.Millisecond
//)

// For show
const (
	ClientRequestTimeout   = 5000 * time.Millisecond
	FetchConfigInterval    = 2000 * time.Millisecond
	ShardMigrationInterval = 1000 * time.Millisecond
	ShardGCInterval        = 1000 * time.Millisecond
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	OpType   OperationType
	ClientId int64
	SeqId    int64
}

type OpReply struct {
	Value string
	Err   Err
}

type OperationType uint8

const (
	OpGet OperationType = iota
	OpPut
	OpAppend
)

func getOperationType(v string) OperationType {
	switch v {
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	default:
		panic(fmt.Sprintf("unknown operation type %s", v))
	}
}

type LastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}

func (op *LastOperationInfo) copyData() LastOperationInfo {
	return LastOperationInfo{
		SeqId: op.SeqId,
		Reply: &OpReply{
			Err:   op.Reply.Err,
			Value: op.Reply.Value,
		},
	}
}

type RaftCommandType uint8

const (
	ClientOpeartion RaftCommandType = iota
	ConfigChange
	ShardMigration
	ShardGC
)

type RaftCommand struct {
	CmdType RaftCommandType
	Data    interface{}
}

type ShardStatus uint8

const (
	Normal ShardStatus = iota
	MoveIn
	MoveOut
	GC
)

type ShardOperationArgs struct {
	ConfigNum int
	ShardIds  []int
}

type ShardOperationReply struct {
	Err            Err
	ConfigNum      int
	ShardData      map[int]map[string]string
	DuplicateTable map[int64]LastOperationInfo
}

type KillArgs struct {
	ServerId int
}

type KillReply struct {
	Err Err
}

type RestartArgs struct {
	ServerId int
}

type RestartReply struct {
	Err Err
}
