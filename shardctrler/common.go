package shardctrler

import (
	"fmt"
	"log"
	"strings"
	"time"
)

//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//

// NShards The number of shards.
const NShards = 10

// Config A configuration -- an assignment of shards to groups.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ClientId int64
	SeqId    int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int
	ClientId int64
	SeqId    int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	ClientId int64
	SeqId    int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

const ClientRequestTimeout = 500 * time.Millisecond

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
	Servers  map[int][]string // new GID -> servers mappings  -- for Join
	GIDs     []int            // -- for Leave
	Shard    int              // -- for Move
	GID      int              // -- for Move
	Num      int              // desired config number -- for Query
	OpType   OperationType
	ClientId int64
	SeqId    int64
}

type OpReply struct {
	ControllerConfig Config
	Err              Err
}

type OperationType uint8

const (
	OpJoin OperationType = iota
	OpLeave
	OpMove
	OpQuery
)

type LastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}

// String 方法将 Config 结构体格式化为字符串
func (c Config) String() string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("Config #%d {\n", c.Num))
	for i, gid := range c.Shards {
		builder.WriteString(fmt.Sprintf("  Shard %d -> GID %d\n", i, gid))
	}
	for gid, servers := range c.Groups {
		builder.WriteString(fmt.Sprintf("  GID %d -> Servers: %v\n", gid, servers))
	}
	builder.WriteString("}")
	return builder.String()
}
