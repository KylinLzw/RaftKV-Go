package main

/*
go build client.go -o client
./client Join 1 127.0.0.1:8000 127.0.0.1:8001 127.0.0.1:8002
./client Query -1
*/

import (
	"fmt"
	"github.com/KylinLzw/RaftKV-Go/shardctrler"
	"net/rpc"
	"os"
	"strconv"
)

// shardctrler 节点信息
var ctrlerAddress = []string{
	"localhost:7000",
	"localhost:7001",
	"localhost:7002",
}

func main() {

	clients := make([]*rpc.Client, 0, len(ctrlerAddress))
	for _, addr := range ctrlerAddress {
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			fmt.Println(".....创建客户端失败.....")
		}
		clients = append(clients, client)
	}
	client := shardctrler.MakeClerk(clients)
	if client == nil {
		fmt.Println(".....连接服务器失败.....")
		return
	}
	switch os.Args[1] {
	case "Join":
		servers := make(map[int][]string)
		gid, _ := strconv.Atoi(os.Args[2])
		for i := 3; i < len(os.Args); i++ {
			servers[gid] = append(servers[gid], os.Args[i])
		}
		client.Join(servers)
		fmt.Println("OK")
	case "Leave":
		gid := make([]int, len(os.Args)-2)
		for i := 2; i < len(os.Args); i++ {
			id, _ := strconv.Atoi(os.Args[2])
			gid[i-2] = id
		}
		client.Leave(gid)
		fmt.Println("OK")
	case "Move":
		shard, _ := strconv.Atoi(os.Args[2])
		gid, _ := strconv.Atoi(os.Args[3])
		client.Move(shard, gid)
		fmt.Println("OK")
	case "Query":
		num := -1
		num, _ = strconv.Atoi(os.Args[2])
		fmt.Println(client.Query(num).String())

	default:
		fmt.Println(".....输入指令有误.....")
		fmt.Println("Query num")
		fmt.Println("Move shard gid")
		fmt.Println("Leave gid1 gid2 ...")
		fmt.Println("Join gid server1 server2 ...")
		fmt.Println("....................")
	}

}
