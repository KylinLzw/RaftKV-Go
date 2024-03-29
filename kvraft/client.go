package kvraft

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
)

type Clerk struct {
	// 保存各个服务器节点信息
	servers []*rpc.Client

	leaderId int // 记录 Leader 节点的 id，避免下一次请求的时候去轮询查找 Leader

	// clientID+seqId 确定一个唯一的命令
	clientId int64
	seqId    int64
}

func MakeClerk(servers []*rpc.Client) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
	}

	for {
		var reply GetReply
		err := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if err != nil || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 请求失败，选择另一个节点重试
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		// 调用成功，返回 value
		return reply.Value
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		var reply PutAppendReply
		err := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if err != nil || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 请求失败，选择另一个节点重试
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		// 调用成功，返回
		ck.seqId++
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
