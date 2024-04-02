package main

import (
	"fmt"
	"github.com/KylinLzw/RaftKV-Go/shardctrler"
	"github.com/tidwall/redcon"
	"log"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
)

//=====================================================================================
// router 基本函数

type CommandType string

const (
	PING = "ping"
	QUIT = "quit"
	AUTH = "auth"

	JOIN  = "join"
	LEAVE = "leave"
	MOVE  = "move"
	QUERY = "query"
)

type Router struct {
	ip    string //ip地址
	port  int    //端口号
	clerk *shardctrler.Clerk
	mu    sync.Mutex
	conns map[string]*CliConn
}

type CliConn struct {
	conn  redcon.Conn
	state bool //是否可通过(当验证了密码的时候,就改为可通过)
}

const debug = true

func (r *Router) log(format string, v ...interface{}) {
	if debug {
		log.Printf("router: %v\n", fmt.Sprintf(format, v...))
	}
}

func NewRouter(serversAddress []string, ip string, port int) (*Router, error) {
	router := &Router{}
	router.ip = ip
	router.port = port
	router.conns = make(map[string]*CliConn)
	serverEnds := make([]*rpc.Client, len(serversAddress))
	i := 0
	for _, address := range serversAddress {
		client, err := rpc.DialHTTP("tcp", address)
		if err != nil {
			sprintf := fmt.Sprintf("连接server: %s失败,error: %v\n", address, err)
			return nil, fmt.Errorf(sprintf)
		}
		serverEnds[i] = client
		i++
	}
	clerk := shardctrler.MakeClerk(serverEnds)
	router.clerk = clerk
	return router, nil
}

func (r *Router) StartRouter() {
	//监听本机的端口
	address := fmt.Sprintf("%s:%d", r.ip, r.port)
	err := redcon.ListenAndServe(address,
		func(conn redcon.Conn, cmd redcon.Command) {
			var cmdStr string
			for _, arg := range cmd.Args {
				cmdStr += string(arg) + " "
			}
			r.log("接收到address: %v,command: %v", conn.RemoteAddr(), cmdStr)
			r.handleCmd(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			return r.acceptConn(conn)
		},
		func(conn redcon.Conn, err error) {
			r.closeConn(conn, err)
		})
	if err != nil {
		r.log("listener出错,error: %v", err)
	}
}

func (r *Router) handleCmd(conn redcon.Conn, cmd redcon.Command) {
	//将命令类型转为小写,并判断命令类型
	switch strings.ToLower(string(cmd.Args[0])) {
	case AUTH:
		//1.验证格式
		if len(cmd.Args) != 2 && len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		//2.验证密码
		if (len(cmd.Args) == 2 && string(cmd.Args[1]) == password) || (len(cmd.Args) == 3 && string(cmd.Args[2]) == password) {
			//验证成功
			if cli, ok := r.conns[conn.RemoteAddr()]; ok {
				cli.state = true
			} else {
				r.conns[conn.RemoteAddr()] = &CliConn{conn, true}
			}
			conn.WriteString("OK")
			return
		}
		//3.密码错误
		conn.WriteString("(error) WRONGPASS invalid username-password pair")

	default:
		if cli, ok := r.conns[conn.RemoteAddr()]; ok && cli.state == false {
			//即还未验证密码
			conn.WriteString("(error) NOAUTH Authentication required.")
			return
		}
		switch strings.ToLower(string(cmd.Args[0])) {
		case PING:
			//1.判断命令是否合规
			if len(cmd.Args) != 1 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}
			//2.返回PONG
			conn.WriteString("PONG")
		case QUIT:
			conn.WriteString("OK")
			conn.Close()
		case JOIN:
			//若是join命令
			//1.判断参数个数是否正确
			if len(cmd.Args) < 3 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}
			//2.若正确,则应用命令
			r.log("接收到 Join 请求 : ", string(cmd.Args[0]))
			servers := make(map[int][]string)
			idx, _ := strconv.Atoi(string(cmd.Args[1]))
			for i, arg := range cmd.Args {
				if i == 0 || i == 1 {
					continue
				}
				servers[idx] = append(servers[idx], string(arg))
			}
			r.clerk.Join(servers)
			conn.WriteString("OK")
		case MOVE:
			//1.判断命令是否合规
			if len(cmd.Args) != 3 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}
			//2.若正确,则应用命令
			r.log("接收到 Move 请求 : ", string(cmd.Args[0]))

			shardId, _ := strconv.Atoi(string(cmd.Args[1]))
			gid, _ := strconv.Atoi(string(cmd.Args[1]))
			r.clerk.Move(shardId, gid)

			conn.WriteString("OK")
		case LEAVE:
			//若是 leave 命令
			//1.判断参数个数是否正确
			if len(cmd.Args) < 2 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}
			//2.若正确,则应用命令
			var gid []int
			for i, arg := range cmd.Args {
				if i == 0 {
					continue
				}
				id, _ := strconv.Atoi(string(arg))
				gid = append(gid, id)
			}
			r.clerk.Leave(gid)
			conn.WriteString("OK")
		case QUERY:
			//若是 query 命令
			//1.判断参数个数是否正确
			if len(cmd.Args) != 2 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}
			num, _ := strconv.Atoi(string(cmd.Args[1]))
			//2.若正确,则应用命令
			config := r.clerk.Query(num)
			conn.WriteString(config.String())
		default:
			conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
		}
	}
}

// 当连接接收的时候调用
func (r *Router) acceptConn(conn redcon.Conn) bool {
	r.log("接收到连接: %v\n", conn.RemoteAddr())
	r.mu.Lock()
	defer r.mu.Unlock()
	//加入到连接列表中
	r.conns[conn.RemoteAddr()] = &CliConn{conn, !pwdAble}
	return true
}

// 当连接被关闭的时候被调用
func (r *Router) closeConn(conn redcon.Conn, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.conns[conn.RemoteAddr()]; ok {
		delete(r.conns, conn.RemoteAddr())
	}
	r.log("关闭连接: %v\n", conn.RemoteAddr())
}

//=====================================================================================
// router 服务

var password = "password"

var pwdAble bool = false

// 服务器地址
var (
	address = []string{"localhost:7000", "localhost:7001", "localhost:7002"}
)

// 路由地址
const (
	ip   = "0.0.0.0"
	port = 6380
)

// 路由层
func main() {
	//创建router
	router, err := NewRouter(address, ip, port)
	if err != nil {
		log.Fatalf("router: 创建router错误,err: %v\n", err)
	}
	log.Printf("开始服务,当前地址是: %s:%d,kvserver组的地址是:\n%v\n", ip, port, address)
	//启动router
	router.StartRouter()
}
