// metadata-server 是分布式文件系统的元数据服务器二进制（Raft 集群版）。
//
// 每个 metadata-server 节点同时承担两个角色：
//  1. Raft 集群成员：参与选举、日志复制，保证元数据一致性
//  2. 业务 RPC 服务：对客户端/数据节点提供元数据操作接口
//
// 用法（3 节点集群）：
//
//	# 节点 1
//	./metadata-server -id=localhost:9001 -peers=localhost:9002,localhost:9003 -wal-dir=/tmp/mds1/wal -snap-dir=/tmp/mds1/snap
//	# 节点 2
//	./metadata-server -id=localhost:9002 -peers=localhost:9001,localhost:9003 -wal-dir=/tmp/mds2/wal -snap-dir=/tmp/mds2/snap
//	# 节点 3
//	./metadata-server -id=localhost:9003 -peers=localhost:9001,localhost:9002 -wal-dir=/tmp/mds3/wal -snap-dir=/tmp/mds3/snap
//
// 客户端连任意一个节点都行：写操作会自动转发到 leader，读操作任意节点都可处理。
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lwwgo/file-storage/internal/metadata"
	rafttypes "github.com/lwwgo/goraft/types"
)

func main() {
	id := flag.String("id", "localhost:9001", "本节点 Raft ID，同时也是业务 RPC 监听地址（如 localhost:9001）")
	peersStr := flag.String("peers", "", "其他节点地址，逗号分隔（如 localhost:9002,localhost:9003）")
	walDir := flag.String("wal-dir", "/tmp/mds/wal", "Raft WAL 日志目录")
	snapDir := flag.String("snap-dir", "/tmp/mds/snap", "Raft 快照目录")
	maxIndexSpan := flag.Uint64("max-index-span", 1000, "快照触发阈值（日志数超过此值时做快照）")
	flag.Parse()

	// 结构化日志
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// 解析 peers
	var peers []string
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			p = strings.TrimSpace(p)
			if p != "" && p != *id {
				peers = append(peers, p)
			}
		}
	}

	// 确保目录存在
	os.MkdirAll(*walDir, 0755)
	os.MkdirAll(*snapDir, 0755)

	// 构建 Raft 配置
	raftConfig := rafttypes.Config{
		LocalID:         *id,
		Peers:           peers,
		WalDir:          *walDir,
		SnapDir:         *snapDir,
		MaxIndexSpan:    *maxIndexSpan,
		ElectionTimeout: 3 * time.Second,
	}

	// 创建集成了 Raft 的 MDS
	mds, err := metadata.NewMetadataServer(raftConfig, logger)
	if err != nil {
		logger.Error("failed to create metadata server", "error", err)
		os.Exit(1)
	}

	// 创建统一的 rpc.Server，同时注册 Raft 内部服务和业务服务
	rpcServer := rpc.NewServer()

	// 注册 Raft 内部服务（选举、日志复制用，service name 必须为 "Server"）
	if err := rpcServer.RegisterName("Server", mds.GetRaftNode()); err != nil {
		logger.Error("failed to register raft rpc service", "error", err)
		os.Exit(1)
	}

	// 注册业务服务（客户端/数据节点访问的）
	if err := rpcServer.RegisterName("MetadataService", mds); err != nil {
		logger.Error("failed to register metadata rpc service", "error", err)
		os.Exit(1)
	}

	// 启动 TCP 监听
	listener, err := net.Listen("tcp", *id)
	if err != nil {
		logger.Error("failed to listen", "addr", *id, "error", err)
		os.Exit(1)
	}
	logger.Info("metadata server (raft node) starting",
		"id", *id,
		"peers", peers,
		"wal_dir", *walDir,
		"snap_dir", *snapDir)
	fmt.Printf("Metadata Server (Raft) listening on %s\n", *id)

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
			go rpcServer.ServeConn(conn)
		}
	}()

	<-sigCh
	logger.Info("metadata server shutting down")
	listener.Close()
}
