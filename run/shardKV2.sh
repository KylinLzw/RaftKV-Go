#!/bin/bash

# 启动三个服务器实例，分别监听 8005, 8006, 8007 端口

# 设置 server.go 文件所在的目录
SERVER_DIR="../main/ShardKvServer/server2"

# 切换到该目录
cd $SERVER_DIR

go build -o server  # 编译 server.go 文件，生成可执行文件 server

# 在后台启动三个服务器实例，使用不同的端口
./server -port 8005 -id 0 -gid 2 & server1_pid=$!
./server -port 8006 -id 1 -gid 2 & server2_pid=$!
./server -port 8007 -id 2 -gid 2 & server3_pid=$!

# 等待一段时间，确保服务器有足够的时间启动
sleep 2 # 捕获 SIGINT 信号，并停止所有服务器实例trap 'kill $server1_pid $server2_pid $server3_pid; echo "所有服务器已停止。"' SIGINT

# 等待服务器实例退出
wait

echo "所有服务器已停止。"