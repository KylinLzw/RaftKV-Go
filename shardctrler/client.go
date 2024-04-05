package shardctrler

/*
MakeClerk ：创建客户端
Query ：询问当前 shardKV 的配置信息
Join ：加入某个 Group
Leave :移除某个 Group
Move  ：移动某些 shard 到其他 Group 中
*/

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
)

type Clerk struct {
	servers []*rpc.Client

	leaderId int
	clientId int64
	seqId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk ：创建客户端
func MakeClerk(servers []*rpc.Client) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

// Query ：询问当前 shardKV 的配置信息
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}

	args.Num = num
	for {
		// try each known server.
		var reply QueryReply
		err := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)

		if err != nil || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return reply.Config
	}
}

// Join ：加入某个 Group
func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	args.Servers = servers

	for {
		var reply JoinReply
		err := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if err != nil || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

// Leave :移除某个 Group
func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	args.GIDs = gids

	for {
		// try each known server.
		var reply LeaveReply
		err := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if err != nil || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

// Move  ：移动 shard 到其他 Group 中
func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		var reply MoveReply
		err := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if err != nil || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}
