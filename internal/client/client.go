// Package client 实现分布式文件系统的客户端。
//
// 客户端职责：
//  1. 对用户暴露简单的 CLI 命令（put/get/ls/mkdir/rm/stat）
//  2. 元数据操作（mkdir/ls/stat/rm）→ 直接 RPC 到 MDS
//  3. 数据操作（put/get）→ 先 RPC 到 MDS 拿位置，再 RPC 到 DataNode 传数据
package client

import (
	"fmt"
	"log/slog"
	"net/rpc"
	"os"
	"strings"

	"github.com/lwwgo/file-storage/internal/types"
)

// Client 分布式文件系统客户端。
type Client struct {
	mdsAddr string
	logger  *slog.Logger
}

// NewClient 创建客户端。
func NewClient(mdsAddr string, logger *slog.Logger) *Client {
	return &Client{mdsAddr: mdsAddr, logger: logger}
}

// dialMDS 连接到 MDS。
func (c *Client) dialMDS() (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", c.mdsAddr)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to metadata server at %s: %w", c.mdsAddr, err)
	}
	return client, nil
}

// callMDSWithRedirect 调用 MDS RPC，自动处理 follower→leader 重定向。
// 写操作（Mkdir/CreateFile/Delete/RegisterDataNode）仅 leader 可处理，
// follower 会返回 "not leader, redirect to <addr>" 错误，此方法自动提取地址并重试。
func (c *Client) callMDSWithRedirect(method string, args, reply any) error {
	addr := c.mdsAddr
	for retries := 0; retries < 3; retries++ {
		rpcClient, err := rpc.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("cannot connect to metadata server at %s: %w", addr, err)
		}
		err = rpcClient.Call(method, args, reply)
		rpcClient.Close()
		if err == nil {
			return nil
		}
		// 检测 not leader 错误并重定向
		if leader := extractLeaderAddr(err.Error()); leader != "" && leader != addr {
			c.logger.Info("not leader, redirecting", "from", addr, "to", leader)
			addr = leader
			continue
		}
		return err
	}
	return fmt.Errorf("redirect retries exhausted for method %s", method)
}

// extractLeaderAddr 从 "not leader, redirect to localhost:9002" 格式的错误中提取 leader 地址。
func extractLeaderAddr(errMsg string) string {
	const prefix = "not leader, redirect to "
	if idx := strings.Index(errMsg, prefix); idx >= 0 {
		return strings.TrimSpace(errMsg[idx+len(prefix):])
	}
	return ""
}

// dialDataNode 连接到指定的数据节点。
func (c *Client) dialDataNode(addr string) (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to data node at %s: %w", addr, err)
	}
	return client, nil
}

// Mkdir 创建目录。
func (c *Client) Mkdir(path string) error {
	var reply bool
	if err := c.callMDSWithRedirect("MetadataService.Mkdir", path, &reply); err != nil {
		return fmt.Errorf("mkdir failed: %w", err)
	}
	c.logger.Info("directory created", "path", path)
	return nil
}

// PutFile uploads a local file to the distributed file system.
// Flow: read local file → RPC MDS create metadata (get all replica locations) →
//
//	parallel RPC to all DataNodes to store content.
func (c *Client) PutFile(localPath, remotePath string) error {
	// 1. Read local file
	content, err := os.ReadFile(localPath)
	if err != nil {
		return fmt.Errorf("read local file failed: %w", err)
	}
	c.logger.Info("local file read", "local", localPath, "size", len(content))

	// 2. RPC MDS create file metadata, get all replica locations (auto-redirect to leader)
	createArgs := &types.CreateFileArgs{
		Path: remotePath,
		Size: int64(len(content)),
	}
	var createReply types.CreateFileReply
	if err := c.callMDSWithRedirect("MetadataService.CreateFile", createArgs, &createReply); err != nil {
		return fmt.Errorf("create metadata failed: %w", err)
	}
	c.logger.Info("metadata created, replicas assigned", "path", remotePath, "replicas", len(createReply.Replicas))

	// 3. Store content to all replica DataNodes in parallel
	type storeResult struct {
		addr string
		err  error
	}
	resultCh := make(chan storeResult, len(createReply.Replicas))
	for _, replica := range createReply.Replicas {
		go func(r types.Replica) {
			dn, err := c.dialDataNode(r.Addr)
			if err != nil {
				resultCh <- storeResult{addr: r.Addr, err: err}
				return
			}
			defer dn.Close()

			storeArgs := &types.StoreArgs{
				Path:    r.RemotePath,
				Content: content,
			}
			var storeReply bool
			if err := dn.Call("DataService.StoreData", storeArgs, &storeReply); err != nil {
				resultCh <- storeResult{addr: r.Addr, err: fmt.Errorf("store data failed: %w", err)}
				return
			}
			resultCh <- storeResult{addr: r.Addr, err: nil}
		}(replica)
	}

	// Wait for all stores, count successes
	successCount := 0
	var firstErr error
	for i := 0; i < len(createReply.Replicas); i++ {
		result := <-resultCh
		if result.err == nil {
			successCount++
		} else if firstErr == nil {
			firstErr = result.err
		}
	}

	// Need at least one successful replica
	if successCount == 0 {
		return fmt.Errorf("all %d replica stores failed: %w", len(createReply.Replicas), firstErr)
	}

	c.logger.Info("file uploaded", "remote", remotePath, "size", len(content), "replicas_ok", successCount, "replicas_total", len(createReply.Replicas))
	return nil
}

// GetFile downloads a file from the distributed file system to local.
// Flow: RPC MDS get all replica locations → try each DataNode in order until one succeeds → write local file.
func (c *Client) GetFile(remotePath, localPath string) error {
	// 1. RPC MDS query file locations
	mds, err := c.dialMDS()
	if err != nil {
		return err
	}
	defer mds.Close()

	var loc types.FileLocation
	if err := mds.Call("MetadataService.GetFileLocation", remotePath, &loc); err != nil {
		return fmt.Errorf("get file location failed: %w", err)
	}
	c.logger.Info("file locations resolved", "remote", remotePath, "replicas", len(loc.Replicas))

	// 2. Try each replica in order until one succeeds
	var content []byte
	var lastErr error
	for _, replica := range loc.Replicas {
		dn, err := c.dialDataNode(replica.Addr)
		if err != nil {
			lastErr = err
			c.logger.Warn("failed to connect to replica, trying next", "data_node", replica.Addr, "error", err)
			continue
		}

		var replicaContent []byte
		if err := dn.Call("DataService.GetData", replica.RemotePath, &replicaContent); err != nil {
			lastErr = err
			c.logger.Warn("failed to read from replica, trying next", "data_node", replica.Addr, "error", err)
			dn.Close()
			continue
		}
		dn.Close()
		content = replicaContent
		break
	}
	if content == nil {
		return fmt.Errorf("all %d replicas failed, last error: %w", len(loc.Replicas), lastErr)
	}

	// 3. Write local file
	if err := os.WriteFile(localPath, content, 0644); err != nil {
		return fmt.Errorf("write local file failed: %w", err)
	}

	c.logger.Info("file downloaded", "remote", remotePath, "local", localPath, "size", len(content))
	return nil
}

// ListDir 列出目录内容。
func (c *Client) ListDir(path string) ([]types.FileInfo, error) {
	mds, err := c.dialMDS()
	if err != nil {
		return nil, err
	}
	defer mds.Close()

	var infos []types.FileInfo
	if err := mds.Call("MetadataService.ListDir", path, &infos); err != nil {
		return nil, fmt.Errorf("list dir failed: %w", err)
	}
	return infos, nil
}

// Stat 获取文件/目录元数据。
func (c *Client) Stat(path string) (*types.FileInfo, error) {
	mds, err := c.dialMDS()
	if err != nil {
		return nil, err
	}
	defer mds.Close()

	var info types.FileInfo
	if err := mds.Call("MetadataService.Stat", path, &info); err != nil {
		return nil, fmt.Errorf("stat failed: %w", err)
	}
	return &info, nil
}

// Delete removes a file or directory.
// If it's a file, also notifies all replica DataNodes to clean up content.
func (c *Client) Delete(path string) error {
	var reply types.DeleteReply
	if err := c.callMDSWithRedirect("MetadataService.Delete", path, &reply); err != nil {
		return fmt.Errorf("delete metadata failed: %w", err)
	}

	// If it's a file, notify all replica DataNodes to clean up content
	if !reply.IsDir && len(reply.Replicas) > 0 {
		for _, replica := range reply.Replicas {
			dn, err := c.dialDataNode(replica.Addr)
			if err != nil {
				c.logger.Warn("failed to connect to replica for cleanup", "data_node", replica.Addr, "error", err)
				continue // metadata already deleted, cleanup failure is non-blocking
			}

			var dnReply bool
			if err := dn.Call("DataService.DeleteData", replica.RemotePath, &dnReply); err != nil {
				c.logger.Warn("failed to delete data from replica", "data_node", replica.Addr, "error", err)
			} else {
				c.logger.Info("data cleaned on replica", "data_node", replica.Addr)
			}
			dn.Close()
		}
	}

	c.logger.Info("path deleted", "path", path)
	return nil
}

// GetReplicas queries and returns all replica locations for a file.
func (c *Client) GetReplicas(path string) ([]types.Replica, error) {
	mds, err := c.dialMDS()
	if err != nil {
		return nil, err
	}
	defer mds.Close()

	var loc types.FileLocation
	if err := mds.Call("MetadataService.GetFileLocation", path, &loc); err != nil {
		return nil, fmt.Errorf("get file location failed: %w", err)
	}
	return loc.Replicas, nil
}

// ListDataNodes 列出所有已注册的数据节点。
func (c *Client) ListDataNodes() ([]string, error) {
	mds, err := c.dialMDS()
	if err != nil {
		return nil, err
	}
	defer mds.Close()

	var nodes []string
	if err := mds.Call("MetadataService.ListDataNodes", struct{}{}, &nodes); err != nil {
		return nil, fmt.Errorf("list data nodes failed: %w", err)
	}
	return nodes, nil
}
