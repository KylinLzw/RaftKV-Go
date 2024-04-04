package shardkv

import (
	"bytes"
	"encoding/gob"
	"github.com/KylinLzw/RaftKV-Go/raft"
	"github.com/KylinLzw/RaftKV-Go/shardctrler"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	make_end     func(string) *rpc.Client
	gid          int
	ctrlers      []*rpc.Client
	maxraftstate int // snapshot if log grows this big

	dead           int32
	lastApplied    int
	shards         map[int]*MemoryKVStateMachine
	notifyChans    map[int]chan *OpReply
	duplicateTable map[int64]LastOperationInfo
	currentConfig  shardctrler.Config
	prevConfig     shardctrler.Config
	mck            *shardctrler.Clerk
}

// StartServer servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//func StartServer(servers []*rpc.Client, me int, persister *raft.Persister, maxraftstate int, gid int,
//	ctrlers []*rpc.Client, make_end func(string) *rpc.Client) *ShardKV {
//	// call gob.Register on structures you want
//	// Go's RPC library to marshall/unmarshall.
//	gob.Register(Op{})
//	gob.Register(RaftCommand{})
//	gob.Register(shardctrler.Config{})
//	gob.Register(ShardOperationArgs{})
//	gob.Register(ShardOperationReply{})
//
//	kv := new(ShardKV)
//	kv.me = me
//	kv.maxraftstate = maxraftstate
//	kv.make_end = make_end
//	kv.gid = gid
//	kv.ctrlers = ctrlers
//
//	// Use something like this to talk to the shardctrler:
//	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
//
//	kv.applyCh = make(chan raft.ApplyMsg)
//	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
//
//	kv.dead = 0
//	kv.lastApplied = 0
//	kv.shards = make(map[int]*MemoryKVStateMachine)
//	kv.notifyChans = make(map[int]chan *OpReply)
//	kv.duplicateTable = make(map[int64]LastOperationInfo)
//	kv.currentConfig = shardctrler.DefaultConfig()
//	kv.prevConfig = shardctrler.DefaultConfig()
//
//	// 从 snapshot 中恢复状态
//	kv.restoreFromSnapshot(persister.ReadSnapshot())
//
//	go kv.applyTask()
//	go kv.fetchConfigTask()
//	go kv.shardMigrationTask()
//	go kv.shardGCTask()
//	return kv
//}

func NewShardKVServer(me int, gid int, maxraftstate int,
	ctrlers []*rpc.Client, make_end func(string) *rpc.Client) *ShardKV {
	gob.Register(Op{})
	gob.Register(RaftCommand{})
	gob.Register(shardctrler.Config{})
	gob.Register(ShardOperationArgs{})
	gob.Register(ShardOperationReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid

	kv.ctrlers = ctrlers
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.dead = 0
	kv.lastApplied = 0
	kv.shards = make(map[int]*MemoryKVStateMachine)
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]LastOperationInfo)
	kv.currentConfig = shardctrler.DefaultConfig()
	kv.prevConfig = shardctrler.DefaultConfig()

	return kv
}

func (kv *ShardKV) StartServer(persister *raft.Persister, servers []*rpc.Client) {
	kv.rf = raft.Make(servers, kv.me, persister, kv.applyCh)
	// 从 snapshot 中恢复状态
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	rpc.RegisterName("Raft", kv.rf)
	go kv.applyTask()
	go kv.fetchConfigTask()
	go kv.shardMigrationTask()
	go kv.shardGCTask()
	for !kv.killed() {
		time.Sleep(5 * time.Second)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// 判断请求 key 是否属于当前 Group
	kv.mu.Lock()
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()

	// 调用 raft，将请求存储到 raft 日志中并进行同步
	index, _, isLeader := kv.rf.Start(RaftCommand{
		ClientOpeartion,
		Op{Key: args.Key, OpType: OpGet},
	})

	// 如果不是 Leader 的话，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}

	// 等待结果
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()

	return nil
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()

	// 判断请求 key 是否所属当前 Group
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return nil
	}

	// 判断请求是否重复
	if kv.requestDuplicated(args.ClientId, args.SeqId) {
		// 如果是重复请求，直接返回结果
		opReply := kv.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()

	// 调用 raft，将请求存储到 raft 日志中并进行同步
	index, _, isLeader := kv.rf.Start(RaftCommand{
		ClientOpeartion,
		Op{
			Key:      args.Key,
			Value:    args.Value,
			OpType:   getOperationType(args.Op),
			ClientId: args.ClientId,
			SeqId:    args.SeqId,
		},
	})

	// 如果不是 Leader 的话，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}

	// 等待结果
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	// 删除通知的 channel
	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
	return nil
}

func (kv *ShardKV) requestDuplicated(clientId, seqId int64) bool {
	info, ok := kv.duplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

func (kv *ShardKV) applyToStateMachine(op Op, shardId int) *OpReply {
	var value string
	var err Err
	switch op.OpType {
	case OpGet:
		value, err = kv.shards[shardId].Get(op.Key)
	case OpPut:
		err = kv.shards[shardId].Put(op.Key, op.Value)
	case OpAppend:
		err = kv.shards[shardId].Append(op.Key, op.Value)
	}
	return &OpReply{Value: value, Err: err}
}

func (kv *ShardKV) getNotifyChannel(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChans[index]
}

func (kv *ShardKV) removeNotifyChannel(index int) {
	delete(kv.notifyChans, index)
}

func (kv *ShardKV) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	_ = enc.Encode(kv.shards)
	_ = enc.Encode(kv.duplicateTable)
	_ = enc.Encode(kv.currentConfig)
	_ = enc.Encode(kv.prevConfig)
	kv.rf.Snapshot(index, buf.Bytes())
}

func (kv *ShardKV) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		for i := 0; i < shardctrler.NShards; i++ {
			if _, ok := kv.shards[i]; !ok {
				kv.shards[i] = NewMemoryKVStateMachine()
			}
		}
		return
	}

	buf := bytes.NewBuffer(snapshot)
	dec := gob.NewDecoder(buf)
	var stateMachine map[int]*MemoryKVStateMachine
	var dupTable map[int64]LastOperationInfo
	var currentConfig shardctrler.Config
	var prevConfig shardctrler.Config
	if dec.Decode(&stateMachine) != nil ||
		dec.Decode(&dupTable) != nil ||
		dec.Decode(&currentConfig) != nil ||
		dec.Decode(&prevConfig) != nil {
		panic("failed to restore state from snapshpt")
	}

	kv.shards = stateMachine
	kv.duplicateTable = dupTable
	kv.currentConfig = currentConfig
	kv.prevConfig = prevConfig
}

func (kv *ShardKV) matchGroup(key string) bool {
	shard := key2shard(key)
	shardStatus := kv.shards[shard].Status
	return kv.currentConfig.Shards[shard] == kv.gid && (shardStatus == Normal || shardStatus == GC)
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
