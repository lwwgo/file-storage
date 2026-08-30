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

// PutFile 上传本地文件到分布式文件系统。
// 流程：读本地文件 → RPC MDS 创建元数据（拿数据节点地址）→ RPC DataNode 存内容。
func (c *Client) PutFile(localPath, remotePath string) error {
	// 1. 读本地文件
	content, err := os.ReadFile(localPath)
	if err != nil {
		return fmt.Errorf("read local file failed: %w", err)
	}
	c.logger.Info("local file read", "local", localPath, "size", len(content))

	// 2. RPC MDS 创建文件元数据，拿到数据节点地址（自动重定向到 leader）
	createArgs := &types.CreateFileArgs{
		Path:    remotePath,
		Size:    int64(len(content)),
		Content: "application/octet-stream",
	}
	var createReply types.CreateFileReply
	if err := c.callMDSWithRedirect("MetadataService.CreateFile", createArgs, &createReply); err != nil {
		return fmt.Errorf("create metadata failed: %w", err)
	}
	c.logger.Info("metadata created, assigned to data node", "path", remotePath, "data_node", createReply.DataNodeAddr)

	// 3. RPC DataNode 存内容
	dn, err := c.dialDataNode(createReply.DataNodeAddr)
	if err != nil {
		return err
	}
	defer dn.Close()

	storeArgs := &types.StoreArgs{
		Path:    createReply.RemotePath,
		Content: content,
	}
	var storeReply bool
	if err := dn.Call("DataService.StoreData", storeArgs, &storeReply); err != nil {
		return fmt.Errorf("store data failed: %w", err)
	}

	c.logger.Info("file uploaded", "remote", remotePath, "size", len(content), "data_node", createReply.DataNodeAddr)
	return nil
}

// GetFile 从分布式文件系统下载文件到本地。
// 流程：RPC MDS 拿位置 → RPC DataNode 读内容 → 写本地文件。
func (c *Client) GetFile(remotePath, localPath string) error {
	// 1. RPC MDS 查询文件位置
	mds, err := c.dialMDS()
	if err != nil {
		return err
	}
	defer mds.Close()

	var loc types.FileLocation
	if err := mds.Call("MetadataService.GetFileLocation", remotePath, &loc); err != nil {
		return fmt.Errorf("get file location failed: %w", err)
	}
	c.logger.Info("file location resolved", "remote", remotePath, "data_node", loc.DataNodeAddr)

	// 2. RPC DataNode 读内容
	dn, err := c.dialDataNode(loc.DataNodeAddr)
	if err != nil {
		return err
	}
	defer dn.Close()

	var content []byte
	if err := dn.Call("DataService.GetData", loc.RemotePath, &content); err != nil {
		return fmt.Errorf("get data failed: %w", err)
	}

	// 3. 写本地文件
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

// Delete 删除文件或目录。
// 如果是文件，还会通知对应数据节点清理内容。
func (c *Client) Delete(path string) error {
	var reply types.DeleteReply
	if err := c.callMDSWithRedirect("MetadataService.Delete", path, &reply); err != nil {
		return fmt.Errorf("delete metadata failed: %w", err)
	}

	// 如果是文件，通知数据节点清理内容
	if !reply.IsDir && reply.DataNodeAddr != "" {
		dn, err := c.dialDataNode(reply.DataNodeAddr)
		if err != nil {
			c.logger.Warn("failed to connect to data node for cleanup, will retry later", "data_node", reply.DataNodeAddr, "error", err)
			return nil // 元数据已删，数据节点清理失败不阻塞
		}
		defer dn.Close()

		var dnReply bool
		if err := dn.Call("DataService.DeleteData", reply.RemotePath, &dnReply); err != nil {
			c.logger.Warn("failed to delete data from data node", "data_node", reply.DataNodeAddr, "error", err)
		} else {
			c.logger.Info("data cleaned on data node", "data_node", reply.DataNodeAddr)
		}
	}

	c.logger.Info("path deleted", "path", path)
	return nil
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
