package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"fmt"
	"github.com/KylinLzw/RaftKV-Go/shardctrler"
	"math/big"
	"net/rpc"
	"time"
)

// 根据 key 获取所在的 shard
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

// 生成随机数，用于生成客户端编号
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm     *shardctrler.Clerk
	config shardctrler.Config

	make_end  func(string) *rpc.Client
	leaderIds map[int]int // 记录 Leader 节点的 id，避免下一次请求的时候去轮询查找 Leader

	// clientID+seqId 确定一个唯一的命令
	clientId int64
	seqId    int64
}

// MakeClerk the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a rpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*rpc.Client, make_end func(string) *rpc.Client) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end

	ck.leaderIds = make(map[int]int)
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	fmt.Println("Clerk Get...")

	for {
		fmt.Println("Clerk Get1...")

		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		fmt.Println(shard, gid)
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			fmt.Println("Clerk Get111...")
			if _, exist := ck.leaderIds[gid]; !exist {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]

			for {
				srv := ck.make_end(servers[ck.leaderIds[gid]])
				var reply GetReply
				fmt.Println("Clerk Get2...")

				err := srv.Call("ShardKV.Get", &args, &reply)
				if err == nil && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if err == nil && (reply.Err == ErrWrongGroup) {
					break
				}
				if err != nil || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(servers)
					if ck.leaderIds[gid] == oldLeaderId {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		fmt.Println("Clerk Get3...")

		ck.config = ck.sm.Query(-1)
		fmt.Println("Clerk Get4...")

	}
}

// PutAppend shared by Put and Append.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	args.Key = key
	args.Value = value
	args.Op = op

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, exist := ck.leaderIds[gid]; !exist {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]

			for {
				srv := ck.make_end(servers[ck.leaderIds[gid]])
				var reply PutAppendReply
				err := srv.Call("ShardKV.PutAppend", &args, &reply)
				if err == nil && reply.Err == OK {
					ck.seqId++
					return
				}
				if err == nil && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				if err != nil || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(servers)
					if ck.leaderIds[gid] == oldLeaderId {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
