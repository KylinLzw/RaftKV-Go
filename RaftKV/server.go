package RaftKV

import (
	"bytes"
	"encoding/gob"
	"github.com/KylinLzw/RaftKV-Go/raft"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

// KVServer :包括Raft算法层和状态机应用层
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// 状态机信息
	lastApplied    int
	stateMachine   *MemoryKVStateMachine
	notifyChans    map[int]chan *OpReply
	duplicateTable map[int64]LastOperationInfo
}

// StartKVServer ：初始化KV数据库
func StartKVServer(servers []*rpc.Client, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.dead = 0
	kv.lastApplied = 0
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]LastOperationInfo)

	// 从 snapshot 中恢复状态
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	go kv.applyTask()
	return kv
}

func NewKVServer(me int, maxraftstate int) *KVServer {
	gob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.dead = 0
	kv.lastApplied = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]LastOperationInfo)
	return kv
}

func (kv *KVServer) StartServer(persister *raft.Persister, servers []*rpc.Client) {
	kv.rf = raft.Make(servers, kv.me, persister, kv.applyCh)

	rpc.RegisterName("Raft", kv.rf)
	//从快照中恢复数据
	kv.restoreFromSnapshot(persister.ReadSnapshot())
	go kv.applyTask()
	for !kv.killed() {
		time.Sleep(5 * time.Second)
	}
}

// Get :服务器响应Get请求
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	// 调用 raft，将请求存储到 raft 日志中并进行同步
	index, _, isLeader := kv.rf.Start(Op{Key: args.Key, OpType: OpGet})

	// 如果不是 Leader 的话，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}

	// 同步成功，等待响应结果
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

// PutAppend :服务器响应 Put 和 Append 请求
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// 判断请求是否重复
	kv.mu.Lock()
	if kv.requestDuplicated(args.ClientId, args.SeqId) {
		// 如果是重复请求，直接返回结果
		opReply := kv.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()

	// 调用 raft，将请求存储到 raft 日志中并进行同步
	index, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   getOperationType(args.Op),
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
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

// 处理 apply 任务
func (kv *KVServer) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				// 如果是已经处理过的消息则直接忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				// 取出用户的操作信息
				op := message.Command.(Op)
				var opReply *OpReply
				if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {
					opReply = kv.duplicateTable[op.ClientId].Reply
				} else {
					// 将操作应用状态机中
					opReply = kv.applyToStateMachine(op)
					if op.OpType != OpGet {
						kv.duplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				// 将结果发送回去
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyCh := kv.getNotifyChannel(message.CommandIndex)
					notifyCh <- opReply
				}

				// 判断是否需要 snapshot
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.makeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				kv.restoreFromSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// 判断是否是客户端的重复请求
func (kv *KVServer) requestDuplicated(clientId, seqId int64) bool {
	info, ok := kv.duplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

// 将操作应用到状态机
func (kv *KVServer) applyToStateMachine(op Op) *OpReply {
	var value string
	var err Err
	switch op.OpType {
	case OpGet:
		value, err = kv.stateMachine.Get(op.Key)
	case OpPut:
		err = kv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		err = kv.stateMachine.Append(op.Key, op.Value)
	}
	return &OpReply{Value: value, Err: err}
}

// 获取响应结果对应的管道
func (kv *KVServer) getNotifyChannel(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChans[index]
}

// 删除通知管道
func (kv *KVServer) removeNotifyChannel(index int) {
	delete(kv.notifyChans, index)
}

// 对KV数据生成快照
func (kv *KVServer) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	_ = enc.Encode(kv.stateMachine)
	_ = enc.Encode(kv.duplicateTable)
	kv.rf.Snapshot(index, buf.Bytes())
}

// 根据快照恢复数据库数据
func (kv *KVServer) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	buf := bytes.NewBuffer(snapshot)
	dec := gob.NewDecoder(buf)
	var stateMachine MemoryKVStateMachine
	var dupTable map[int64]LastOperationInfo
	if dec.Decode(&stateMachine) != nil || dec.Decode(&dupTable) != nil {
		panic("failed to restore state from snapshpt")
	}

	kv.stateMachine = &stateMachine
	kv.duplicateTable = dupTable
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
