package main

import (
	"flag"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/peterh/liner"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

//=====================================================================================
// client 基本函数

var commandList = [][]string{
	{"SET", "key value", "STRING"},
	{"GET", "key", "STRING"},
	{"SETNX", "key seconds value", "STRING"},
	{"SETEX", "key value", "STRING"},
	{"GETSET", "key value", "STRING"},
	{"MSET", "[key value...]", "STRING"},
	{"MGET", "[key...]", "STRING"},
	{"APPEND", "key value", "STRING"},
	{"STREXISTS", "key", "STRING"},
	{"REMOVE", "key", "STRING"},
	{"EXPIRE", "key seconds", "STRING"},
	{"PERSIST", "key", "STRING"},
	{"TTL", "key", "STRING"},

	{"LPUSH", "key value [value...]", "LIST"},
	{"RPUSH", "key value [value...]", "LIST"},
	{"LPOP", "key", "LIST"},
	{"RPOP", "key", "LIST"},
	{"LINDEX", "key index", "LIST"},
	{"LREM", "key value count", "LIST"},
	{"LINSERT", "key BEFORE|AFTER pivot element", "LIST"},
	{"LSET", "key index value", "LIST"},
	{"LTRIM", "key start end", "LIST"},
	{"LRANGE", "key start end", "LIST"},
	{"LLEN", "key", "LIST"},
	{"LKEYEXISTS", "key", "LIST"},
	{"LVALEXISTS", "key value", "LIST"},
	{"LClear", "key", "LIST"},
	{"LExpire", "key seconds", "LIST"},
	{"LTTL", "key", "LIST"},

	{"HSET", "key field value", "HASH"},
	{"HSETNX", "key field value", "HASH"},
	{"HGET", "key field", "HASH"},
	{"HMSET", "[key field...]", "HASH"},
	{"HMGET", "[key...]", "HASH"},
	{"HGETALL", "key", "HASH"},
	{"HDEL", "key field [field...]", "HASH"},
	{"HKEYEXISTS", "key", "HASH"},
	{"HEXISTS", "key field", "HASH"},
	{"HLEN", "key", "HASH"},
	{"HKEYS", "key", "HASH"},
	{"HVALS", "key", "HASH"},
	{"HCLEAR", "key", "HASH"},
	{"HEXPIRE", "key seconds", "HASH"},
	{"HTTL", "key", "HASH"},

	{"SADD", "key members [members...]", "SET"},
	{"SPOP", "key count", "SET"},
	{"SISMEMBER", "key member", "SET"},
	{"SRANDMEMBER", "key count", "SET"},
	{"SREM", "key members [members...]", "SET"},
	{"SMOVE", "src dst member", "SET"},
	{"SCARD", "key", "key", "SET"},
	{"SMEMBERS", "key", "SET"},
	{"SUNION", "key [key...]", "SET"},
	{"SDIFF", "key [key...]", "SET"},
	{"SKEYEXISTS", "key", "SET"},
	{"SCLEAR", "key", "SET"},
	{"SEXPIRE", "key seconds", "SET"},
	{"STTL", "key", "SET"},

	{"ZADD", "key score member", "ZSET"},
	{"ZSCORE", "key member", "ZSET"},
	{"ZCARD", "key", "ZSET"},
	{"ZRANK", "key member", "ZSET"},
	{"ZREVRANK", "key member", "ZSET"},
	{"ZINCRBY", "key increment member", "ZSET"},
	{"ZRANGE", "key start stop", "ZSET"},
	{"ZREVRANGE", "key start stop", "ZSET"},
	{"ZREM", "key member", "ZSET"},
	{"ZGETBYRANK", "key rank", "ZSET"},
	{"ZREVGETBYRANK", "key rank", "ZSET"},
	{"ZSCORERANGE", "key min max", "ZSET"},
	{"ZREVSCORERANGE", "key max min", "ZSET"},
	{"ZKEYEXISTS", "key", "ZSET"},
	{"ZCLEAR", "key", "ZSET"},
	{"ZEXPIRE", "key", "ZSET"},
	{"ZTTL", "key", "ZSET"},

	{"MULTI", "Transaction start", "TRANSACTION"},
	{"EXEC", "Transaction end", "TRANSACTION"},
	{"PING"},
	{"AUTH"},
}

// 终端命令历史记录文件路径
var history_fn = filepath.Join(os.TempDir(), ".liner_history")

type Client struct {
	mu         sync.Mutex
	routerIp   string
	routerPort int
	conn       redis.Conn
	line       *liner.State
}

const debug = true

func (c *Client) log(format string, v ...interface{}) {
	if debug {
		log.Printf("client: %v\n", fmt.Sprintf(format, v...))
	}
}

func NewClient(routerIp string, routerPort int) *Client {
	client := new(Client)
	client.routerIp = routerIp
	client.routerPort = routerPort
	//开始连接
	conn, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", routerIp, routerPort))
	if err != nil {
		client.log("连接 %s:%d 失败: %v\n", routerIp, routerPort, err)
		return nil
	}
	client.conn = conn
	client.line = liner.NewLiner()
	//配置liner
	client.configureLiner()
	return client
}

func (c *Client) configureLiner() {
	//设置Ctrl C退出确认
	c.line.SetCtrlCAborts(true)

	//设置命令自动补全(按Tab键)
	c.line.SetCompleter(func(line string) (res []string) {
		for _, cmd := range commandList {
			//当命令列表数组中每个数组的第一个字符串,也就是命令类型,和当前终端输入的如果匹配,就加入结果集
			if strings.HasPrefix(cmd[0], strings.ToUpper(line)) {
				res = append(res, strings.ToLower(cmd[0]))
			}
		}
		return
	})

	//初始化命令历史记录
	if file, err := os.Open(history_fn); err == nil {
		//先从文件中读取之前的历史记录
		c.line.ReadHistory(file)

		fmt.Printf("历史数据加载成功.....\n")

		//关闭文件
		file.Close()
	}

}

func (c *Client) StartClient() {
	defer c.conn.Close()
	defer c.line.Close()
	defer c.writeLineHistory()
	prefix := fmt.Sprintf("%s:%d>", c.routerIp, c.routerPort)
	for {
		//接收消息
		cmd, err := c.line.Prompt(prefix)
		if err != nil {
			fmt.Println("输入错误:", err)
			return
		}
		//删掉多余空格
		cmd = strings.TrimSpace(cmd)
		if len(cmd) == 0 {
			continue
		}
		//记录到命令历史中
		c.line.AppendHistory(cmd)
		//将消息发过去
		if c.applyCommand(cmd) {
			//需要关闭客户端
			return
		}
	}
}

// 结束的时候进行命令历史的文件写入(下一次可以直接从文件中恢复)
func (c *Client) writeLineHistory() {
	if file, err := os.Create(history_fn); err == nil {
		//写到文件中
		c.line.WriteHistory(file)
		file.Close()
	} else {
		c.log("error writing history file: %v\n", err)
	}
}

func (c *Client) applyCommand(cmd string) (exit bool) {
	exit = false
	if cmd == "quit" {
		exit = true
	}
	//解析出命令
	command, args := parseCommand(cmd)
	resp, err := c.conn.Do(command, args...)
	if err != nil {
		fmt.Printf("(error) %v\n", err)
		return
	}
	//响应命令
	switch reply := resp.(type) {
	case string:
		fmt.Println(reply)
	case []byte:
		fmt.Println(string(reply))
	case nil:
		fmt.Println("(nil)")
	case redis.Error:
		fmt.Printf("(error) %v\n", reply)
	case int64:
		fmt.Printf("(integer) %d\n", reply)
	default:
		return
	}
	return
}

func parseCommand(cmd string) (cmdType string, args []interface{}) {
	//以空格分割
	eles := strings.Split(cmd, " ")
	if len(cmd) == 0 {
		return "", nil
	}
	args = make([]interface{}, 0)
	for _, ele := range eles {
		if ele == "" {
			continue
		}
		//否则加入并且转为小写
		args = append(args, strings.ToLower(ele))
	}
	cmdType = fmt.Sprintf("%s", args[0])
	return cmdType, args[1:]
}

//=====================================================================================
// 客户端

// 客户端地址
var (
	ip   string
	port int
)

// 默认的 router 信息
func init() {
	flag.StringVar(&ip, "ip", "0.0.0.0", "router的ip地址")
	flag.IntVar(&port, "port", 6380, "router的端口")
}

func main() {
	flag.Parse()
	line := liner.NewLiner()
	defer line.Close()
	//创建client
	client := NewClient(ip, port)
	if client == nil {
		fmt.Println("连接服务器失败")
		return
	}

	fmt.Printf("服务器连接成功.....\n")

	client.StartClient()

}
