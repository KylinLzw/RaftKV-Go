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

//func (ck *Clerk) Get(key string) string {
//	args := GetArgs{
//		Key: key,
//	}
//
//	for {
//		var reply GetReply
//		err := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
//
//		if err != nil || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
//			// 请求失败，选择另一个节点重试
//			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
//			continue
//		}
//		// 调用成功，返回 value
//		fmt.Printf("333")
//		return reply.Value
//	}
//}

func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	commandId := ck.seqId + 1
	args := GetArgs{
		Key: key,
	}
	DPrintf("client[%d]: 开始发送Get RPC;args=[%v]\n", ck.clientId, args)
	//第一个发送的目标server是上一次RPC发现的leader
	serverId := ck.leaderId
	serverNum := len(ck.servers)
	for ; ; serverId = (serverId + 1) % serverNum {
		var reply GetReply
		DPrintf("client[%d]: 开始发送Get RPC;args=[%v]到server[%d]\n", ck.clientId, args, serverId)
		err := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		//当发送失败或者返回不是leader时,则继续到下一个server进行尝试
		if err != nil || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
			DPrintf("client[%d]: 发送Get RPC;args=[%v]到server[%d]失败,err = %v,Reply=[%v]\n", ck.clientId, args, serverId, err, reply)
			continue
		}
		DPrintf("client[%d]: 发送Get RPC;args=[%v]到server[%d]成功,Reply=[%v]\n", ck.clientId, args, serverId, reply)
		//若发送成功,则更新最近发现的leader
		ck.leaderId = serverId
		ck.seqId = commandId
		if reply.Err == ErrNoKey {
			return ""
		}
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
