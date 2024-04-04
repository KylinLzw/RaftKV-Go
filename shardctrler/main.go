package shardctrler

import (
	"flag"
	"fmt"
	"net/rpc"
)

//// 客户端地址
//var (
//	ip   string
//	port int
//)
//
//// 默认的 router 信息
//func init() {
//	flag.StringVar(&ip, "ip", "0.0.0.0", "router的ip地址")
//	flag.IntVar(&port, "port", 6380, "router的端口")
//}

// shardctrler 节点信息
var ctrlerAddress = []string{
	"localhost:7000",
	"localhost:7001",
	"localhost:7002",
}

func main() {
	flag.Parse()

	clients := make([]*rpc.Client, 0, len(ctrlerAddress))
	for _, addr := range ctrlerAddress {
		client, _ := rpc.DialHTTP("tcp", addr)
		clients = append(clients, client)
	}
	client := MakeClerk(clients)
	if client == nil {
		fmt.Println("连接服务器失败")
		return
	}
	fmt.Printf("服务器连接成功.....\n")

	fmt.Printf(client.Query(-1).String())
}
