package RaftKV

import (
	"fmt"
	"log"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

const ClientRequestTimeout = 500 * time.Millisecond

const Debug = false

type Err string

// PutAppendArgs ：Put or Append
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

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
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
