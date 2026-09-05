// Package datanode 实现分布式文件系统的数据节点。
//
// DataNode 职责：
//  1. 存储实际文件内容（按路径映射到本地磁盘）
//  2. 启动时向 MDS 注册自己
//  3. 通过 net/rpc 提供 StoreData/GetData/DeleteData 服务
//
// 注意：DataNode 不管理目录树、不管理文件映射，这些全在 MDS 上。
package datanode

import (
	"fmt"
	"log/slog"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lwwgo/fstorex/internal/types"
)

// heartbeatInterval is how often the DataNode sends a heartbeat to MDS.
const heartbeatInterval = 5 * time.Minute

// DataNode 是数据节点的核心实现。
type DataNode struct {
	dataDir string       // 本地数据存储根目录
	mdsAddr string       // MDS 的 RPC 地址
	addr    string       // 本节点的 RPC 地址
	mu      sync.RWMutex // 保护并发写入
	logger  *slog.Logger
}

// NewDataNode 创建数据节点实例。
func NewDataNode(dataDir, mdsAddr, addr string, logger *slog.Logger) (*DataNode, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}
	dn := &DataNode{
		dataDir: dataDir,
		mdsAddr: mdsAddr,
		addr:    addr,
		logger:  logger,
	}
	logger.Info("data node initialized", "data_dir", dataDir, "mds_addr", mdsAddr, "addr", addr)
	return dn, nil
}

// RegisterRPC 把 DataService 注册到 net/rpc。
func (dn *DataNode) RegisterRPC() error {
	return rpc.RegisterName("DataService", dn)
}

// StartHeartbeat starts the background heartbeat goroutine.
// The first heartbeat registers the DataNode; subsequent heartbeats keep it alive.
// Supports automatic follower→leader redirect for each heartbeat.
func (dn *DataNode) StartHeartbeat() {
	// 立即发第一次心跳（即注册）
	dn.sendHeartbeat()
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for range ticker.C {
			dn.sendHeartbeat()
		}
	}()
	dn.logger.Info("heartbeat goroutine started", "interval", heartbeatInterval)
}

// sendHeartbeat sends one heartbeat with leader redirect support.
func (dn *DataNode) sendHeartbeat() {
	addr := dn.mdsAddr
	for retries := 0; retries < 3; retries++ {
		rpcClient, err := rpc.Dial("tcp", addr)
		if err != nil {
			dn.logger.Warn("heartbeat: cannot connect to MDS", "addr", addr, "error", err)
			return
		}
		var reply types.HeartbeatReply
		err = rpcClient.Call("MetadataService.Heartbeat", dn.addr, &reply)
		rpcClient.Close()
		if err == nil {
			return
		}
		// follower 重定向
		if leader := extractLeaderAddr(err.Error()); leader != "" && leader != addr {
			dn.logger.Info("heartbeat: not leader, redirecting", "from", addr, "to", leader)
			addr = leader
			continue
		}
		dn.logger.Warn("heartbeat failed", "addr", addr, "error", err)
		return
	}
}

// extractLeaderAddr 从 "not leader, redirect to <addr>" 错误中提取 leader 地址。
func extractLeaderAddr(errMsg string) string {
	const prefix = "not leader, redirect to "
	if idx := strings.Index(errMsg, prefix); idx >= 0 {
		return strings.TrimSpace(errMsg[idx+len(prefix):])
	}
	return ""
}

// ===== DataService RPC 方法实现 =====

// StoreData 存储文件内容。
func (dn *DataNode) StoreData(args *types.StoreArgs, reply *bool) error {
	dn.mu.Lock()
	defer dn.mu.Unlock()

	// 安全路径解析
	realPath, err := dn.resolvePath(args.Path)
	if err != nil {
		return err
	}

	// 确保父目录存在
	if err := os.MkdirAll(filepath.Dir(realPath), 0755); err != nil {
		return fmt.Errorf("failed to create parent dir: %w", err)
	}

	// 原子写入：先写临时文件再 rename
	tmpPath := realPath + ".tmp"
	if err := os.WriteFile(tmpPath, args.Content, 0644); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	if err := os.Rename(tmpPath, realPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	*reply = true
	dn.logger.Info("data stored", "path", args.Path, "size", len(args.Content))
	return nil
}

// GetData 读取文件内容。
func (dn *DataNode) GetData(path string, reply *[]byte) error {
	dn.mu.RLock()
	defer dn.mu.RUnlock()

	realPath, err := dn.resolvePath(path)
	if err != nil {
		return err
	}

	data, err := os.ReadFile(realPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("data not found: %s", path)
		}
		return fmt.Errorf("failed to read data: %w", err)
	}

	*reply = data
	dn.logger.Debug("data retrieved", "path", path, "size", len(data))
	return nil
}

// DeleteData 删除文件内容。
func (dn *DataNode) DeleteData(path string, reply *bool) error {
	dn.mu.Lock()
	defer dn.mu.Unlock()

	realPath, err := dn.resolvePath(path)
	if err != nil {
		return err
	}

	if err := os.Remove(realPath); err != nil {
		if os.IsNotExist(err) {
			*reply = true // 幂等
			return nil
		}
		return fmt.Errorf("failed to delete data: %w", err)
	}

	*reply = true
	dn.logger.Info("data deleted", "path", path)
	return nil
}

// HealthCheck 健康检查。
func (dn *DataNode) HealthCheck(_ struct{}, reply *bool) error {
	*reply = true
	return nil
}

// ListAllPaths 返回该数据节点持有的所有文件元数据（供 MDS GC 用）。
func (dn *DataNode) ListAllPaths(_ struct{}, reply *[]types.NodeFile) error {
	dn.mu.RLock()
	defer dn.mu.RUnlock()

	var files []types.NodeFile
	err := filepath.Walk(dn.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		// 跳过临时文件
		if strings.HasSuffix(path, ".tmp") {
			return nil
		}
		rel, err := filepath.Rel(dn.dataDir, path)
		if err != nil {
			return err
		}
		// 转为虚拟路径格式（/开头），带上修改时间供 GC mtime 保护用
		files = append(files, types.NodeFile{
			Path:    "/" + filepath.ToSlash(rel),
			ModTime: info.ModTime(),
		})
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk data dir failed: %w", err)
	}
	*reply = files
	return nil
}

// resolvePath 把虚拟路径解析为本地磁盘路径，并做路径穿越防护。
func (dn *DataNode) resolvePath(remotePath string) (string, error) {
	// 禁止路径穿越
	clean := filepath.Clean("/" + strings.TrimPrefix(remotePath, "/"))
	if strings.Contains(clean, "..") {
		return "", fmt.Errorf("invalid path: %s", remotePath)
	}
	realPath := filepath.Join(dn.dataDir, clean)
	return realPath, nil
}

// Compile-time assertion: ensure DataNode fully implements DataService.
var _ types.DataService = (*DataNode)(nil)
