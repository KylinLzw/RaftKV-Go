package main

import (
	"flag"
	"fmt"
	"github.com/KylinLzw/RaftKV-Go/raft"
	"github.com/KylinLzw/RaftKV-Go/shardkv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

/*
启动三个节点：
  go run server.go -port 8005 -id 0
  go run server.go -port 8006 -id 1
  go run server.go -port 8007 -id 2
*/

// 服务节点信息
var address = map[int]string{
	0: "localhost:8005",
	1: "localhost:8006",
	2: "localhost:8007",
}

var serverIp string
var serverPort int
var serverNum int
var serverGid int

// 默认节点信息
func init() {
	flag.StringVar(&serverIp, "ip", "127.0.0.1", "server的ip")
	flag.IntVar(&serverPort, "port", 8005, "server的端口")
	flag.IntVar(&serverNum, "id", 0, "该server在Raft组中的编号")
	flag.IntVar(&serverGid, "gid", 2, "该server所在的gid编号")
}

// shardctrler 节点信息
var ctrlerAddress = []string{
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

// 启动一个服务端(接收client请求)
func main() {

	// 初始化 shardctrler RPC
	ctrlers := make([]*rpc.Client, 0, len(ctrlerAddress))
	for _, addr := range ctrlerAddress {
		ctrler := make_end(addr) // 创建RPC客户端端点
		ctrlers = append(ctrlers, ctrler)
	}

	//解析命令行参数
	flag.Parse()
	persister := raft.MakePersister()
	ShardKV := shardkv.NewShardKVServer(serverNum, serverGid, -1, ctrlers, make_end)

	//在rpc中进行注册
	err := rpc.RegisterName("ShardKV", ShardKV)
	if err != nil {
		return
	}
	//监听自己的ip:port
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", address[serverNum])
	if err != nil {
		log.Fatalf("监听自己的server: %s失败,error: %v\n", address[serverNum], err)
		return
	}
	fmt.Printf("监听成功: %v\n", listener.Addr())
	defer listener.Close()
	serverEnds := make([]*rpc.Client, len(address))
	serverEnds[serverNum] = nil
	delete(address, serverNum)

	//启动http服务(为了rpc调用)
	go http.Serve(listener, nil)

	//等待其他节点上线
	fmt.Println("等待其他节点上线")
	for len(address) > 0 {
		for index, server := range address {
			//和server建立rpc连接
			client, err := rpc.DialHTTP("tcp", server)
			if err != nil {
				fmt.Printf("连接到server: %v失败,error: %v\n", server, err)
			} else {
				//连接成功则加入到serverEnds中
				fmt.Printf("节点:[%v]连接成功\n", server)
				serverEnds[index] = client
				delete(address, index)
			}
		}
		time.Sleep(1 * time.Second)
	}
	fmt.Printf("已连接所有节点,开始服务\n")
	//等节点都上线了,就开启KVServer
	ShardKV.StartServer(persister, serverEnds)
}
