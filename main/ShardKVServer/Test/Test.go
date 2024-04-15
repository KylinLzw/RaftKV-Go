package main

import (
	"fmt"
	"github.com/KylinLzw/RaftKV-Go/shardkv"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

/*
   go build -o test
   ./client Kill 0/1/2
*/

// KVAddress 节点信息
var KVAddress = []string{
	"localhost:7000",
	"localhost:7001",
	"localhost:7002",
}

// make_end函数的定义
func make_end(servername string) *rpc.Client {
	client, err := rpc.DialHTTP("tcp", servername)
	if err != nil {
		log.Fatalf("Error connecting to server: %v", err)
	}
	return client
}

func main() {

	clients := make([]*rpc.Client, 0, len(KVAddress))
	for _, addr := range KVAddress {
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			fmt.Println(".....创建客户端失败.....")
		}
		clients = append(clients, client)
	}
	client := shardkv.MakeClerk(clients, make_end)
	if client == nil {
		fmt.Println(".....连接服务器失败.....")
		return
	}

	switch os.Args[1] {
	case "Kill":
		gid, _ := strconv.Atoi(os.Args[2])
		num, _ := strconv.Atoi(os.Args[3])
		fmt.Println(client.Kill(gid, num))
	case "Restart":
		gid, _ := strconv.Atoi(os.Args[2])
		num, _ := strconv.Atoi(os.Args[3])
		fmt.Println(client.Restart(gid, num))
	default:
		fmt.Println(".....输入指令有误.....")
		fmt.Println("Kill num")
		fmt.Println("Restart num")
		fmt.Println("....................")
	}
}
