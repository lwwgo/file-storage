// data-node 是分布式文件系统的数据节点二进制。
//
// 用法：
//   ./data-node -addr=:9001 -mds=localhost:9000 -data-dir=/tmp/dn1
//
// 职责：存储实际文件内容，启动时向 MDS 注册自己。
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"

	"github.com/lwwgo/file-storage/internal/datanode"
)

func main() {
	addr := flag.String("addr", ":9001", "RPC listen address")
	mdsAddr := flag.String("mds", "localhost:9000", "metadata server address")
	dataDir := flag.String("data-dir", "/tmp/dn1", "local data directory")
	flag.Parse()

	// 结构化日志
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// 创建数据节点
	dn, err := datanode.NewDataNode(*dataDir, *mdsAddr, *addr, logger)
	if err != nil {
		logger.Error("failed to create data node", "error", err)
		os.Exit(1)
	}

	// 注册 RPC 服务
	if err := dn.RegisterRPC(); err != nil {
		logger.Error("failed to register RPC", "error", err)
		os.Exit(1)
	}

	// 启动 TCP 监听
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Error("failed to listen", "addr", *addr, "error", err)
		os.Exit(1)
	}
	logger.Info("data node starting", "addr", *addr, "mds", *mdsAddr, "data_dir", *dataDir)
	fmt.Printf("Data Node listening on %s\n", *addr)

	// 向 MDS 注册自己（重试几次，MDS 可能还没启动）
	go func() {
		for i := 0; i < 5; i++ {
			if err := dn.RegisterToMDS(); err != nil {
				logger.Warn("failed to register to MDS, retrying...", "attempt", i+1, "error", err)
				continue
			}
			return
		}
		logger.Error("failed to register to MDS after 5 attempts")
	}()

	// 优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-sigCh:
					return
				default:
					continue
				}
			}
			go rpc.ServeConn(conn)
		}
	}()

	<-sigCh
	logger.Info("data node shutting down")
	listener.Close()
}
