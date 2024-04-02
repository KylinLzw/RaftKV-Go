package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strings"
)

var (
	shardCtrlerAddrs = flag.String("shardCtrlerAddrs", "localhost:8000,localhost:8001,localhost:8002", "Comma-separated list of ShardCtrler addresses")
	shardKVAddrs     = flag.String("shardKVAddrs", "localhost:8003,localhost:8004,localhost:8005,localhost:8006,localhost:8007,localhost:8008,localhost:8009", "Comma-separated list of ShardKV addresses")
)

func main() {
	flag.Parse()

	// 初始化ShardCtrler客户端
	shardCtrlerClients := []*rpc.Client{}
	for _, addr := range strings.Split(*shardCtrlerAddrs, ",") {
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatalf("Error connecting to ShardCtrler server: %v", err)
		}
		shardCtrlerClients = append(shardCtrlerClients, client)
	}

	// 初始化ShardKV客户端
	shardKVClients := []*rpc.Client{}
	for _, addr := range strings.Split(*shardKVAddrs, ",") {
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatalf("Error connecting to ShardKV server: %v", err)
		}
		shardKVClients = append(shardKVClients, client)
	}

	// 处理客户端请求
	for {
		var request Request
		decoder := json.NewDecoder(os.Stdin)
		if err := decoder.Decode(&request); err != nil {
			log.Printf("Error decoding request: %v", err)
			continue
		}

		switch request.Type {
		case "Get":
			value, err := GetKeyValue(shardKVClients, request.Key)
			if err != nil {
				log.Printf("Error getting key-value: %v", err)
			} else {
				fmt.Printf("Value for key '%s': %s\n", request.Key, value)
			}
		case "Put":
			err := PutKeyValue(shardKVClients, request.Key, request.Value)
			if err != nil {
				log.Printf("Error putting key-value: %v", err)
			}
		case "QueryShardCtrler":
			config, err := QueryShardCtrler(shardCtrlerClients)
			if err != nil {
				log.Printf("Error querying ShardCtrler: %v", err)
			} else {
				fmt.Printf("ShardCtrler config: %+v\n", config)
			}
		default:
			fmt.Println("Unknown request type")
		}
	}
}

// GetKeyValue retrieves the value for a given key from the ShardKV service.
func GetKeyValue(clients []*rpc.Client, key string) (string, error) {
	// TODO: Implement logic to determine which ShardKV client to use based on the key.
	// For simplicity, we're using the first client in this example.
	client := clients[0]
	var reply GetReply
	err := client.Call("ShardKV.Get", &GetArgs{Key: key}, &reply)
	if err != nil || reply.Err != "" {
		return "", fmt.Errorf("error getting key-value: %v", reply.Err)
	}
	return reply.Value, nil
}

// PutKeyValue stores a key-value pair in the ShardKV service.
func PutKeyValue(clients []*rpc.Client, key, value string) error {
	// TODO: Implement logic to determine which ShardKV client to use based on the key.
	// For simplicity, we're using the first client in this example.
	client := clients[0]
	var reply PutAppendReply
	err := client.Call("ShardKV.Put", &PutAppendArgs{Key: key, Value: value}, &reply)
	if err != nil || reply.Err != "" {
		return fmt.Errorf("error putting key-value: %v", reply.Err)
	}
	return nil
}

// QueryShardCtrler retrieves the current configuration from the ShardCtrler service.
func QueryShardCtrler(clients []*rpc.Client) (Config, error) {
	// TODO: Implement logic to determine which ShardCtrler client to use.
	// For simplicity, we're using the first client in this example.
	client := clients[0]
	var reply QueryReply
	err := client.Call("ShardCtrler.Query", &QueryArgs{Num: -1}, &reply)
	if err != nil || reply.Err != "" {
		return Config{}, fmt.Errorf("error querying ShardCtrler: %v", reply.Err)
	}
	return reply.Config, nil
}

// Request represents a client request.
type Request struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// GetReply represents the response from a Get operation.
type GetReply struct {
	Value string `json:"value"`
	Err   string `json:"err"`
}

// PutAppendReply represents the response from a Put or Append operation.
type PutAppendReply struct {
	Err string `json:"err"`
}

// Config represents the current configuration from the ShardCtrler service.
type Config struct {
	// Add fields as necessary based on the actual configuration structure.
}

// QueryArgs represents the arguments for a Query operation.
type QueryArgs struct {
	Num int `json:"num"`
}

// QueryReply represents the response from a Query operation.
type QueryReply struct {
	WrongLeader bool   `json:"wrong_leader"`
	Err         string `json:"err"`
	Config      Config `json:"config"`
}
