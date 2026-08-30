// Package types 定义三个分布式组件共享的数据结构和 RPC 接口。
//
// 架构总览：
//
//	Client ──RPC──▶ MetadataServer（管目录树 + 文件→数据节点映射）
//	Client ──RPC──▶ DataNode（存实际文件内容）
//	DataNode ──RPC──▶ MetadataServer（启动时注册自己）
package types

import "time"

// FileInfo 描述一个文件或目录的元数据（与本地版本保持一致）。
type FileInfo struct {
	Name      string    `json:"name"`
	Path      string    `json:"path"`
	IsDir     bool      `json:"is_dir"`
	Size      int64     `json:"size"`
	Mode      uint32    `json:"mode"`
	CreatedAt time.Time `json:"created_at"`
	ModTime   time.Time `json:"mod_time"`
}

// Replica describes one copy of a file on a specific data node.
type Replica struct {
	Addr       string `json:"addr"`        // data node RPC address
	RemotePath string `json:"remote_path"` // storage path on that data node
}

// FileLocation describes where all replicas of a file are stored.
type FileLocation struct {
	Replicas []Replica `json:"replicas"` // all replica locations
}

// ===== MetadataServer RPC 接口定义 =====

// MetadataService 元数据服务，管理目录树和文件→数据节点的映射。
type MetadataService interface {
	// RegisterDataNode 数据节点启动时调用，向 MDS 注册自己。
	RegisterDataNode(addr string, reply *bool) error

	// Mkdir 创建目录。
	Mkdir(path string, reply *bool) error

	// CreateFile 创建文件元数据，MDS 会选择多个数据节点分配多副本，返回副本位置。
	CreateFile(args *CreateFileArgs, reply *CreateFileReply) error

	// GetFileLocation 查询文件内容存储在哪些数据节点。
	GetFileLocation(path string, reply *FileLocation) error

	// ListDir 列出目录下的文件和子目录。
	ListDir(path string, reply *[]FileInfo) error

	// Stat 获取文件/目录元数据。
	Stat(path string, reply *FileInfo) error

	// Delete 删除文件或目录，返回需要清理的数据节点列表。
	Delete(path string, reply *DeleteReply) error

	// ListDataNodes 列出所有已注册的数据节点。
	ListDataNodes(_ struct{}, reply *[]string) error
}

// CreateFileArgs 创建文件的请求参数。
type CreateFileArgs struct {
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	Content string `json:"content_type"` // MIME 类型
}

// CreateFileReply is the response for creating a file, containing all assigned replica locations.
type CreateFileReply struct {
	Replicas []Replica `json:"replicas"`
}

// DeleteReply is the response for a delete operation, containing all replica locations to clean up.
type DeleteReply struct {
	Replicas []Replica `json:"replicas"`
	IsDir    bool      `json:"is_dir"`
}

// ===== DataNode RPC 接口定义 =====

// DataService 数据节点服务，存储实际的文件内容。
type DataService interface {
	// StoreData 存储文件内容。
	StoreData(args *StoreArgs, reply *bool) error

	// GetData 读取文件内容。
	GetData(path string, reply *[]byte) error

	// DeleteData 删除文件内容。
	DeleteData(path string, reply *bool) error

	// HealthCheck 健康检查。
	HealthCheck(_ struct{}, reply *bool) error
}

// StoreArgs 存储数据的请求参数。
type StoreArgs struct {
	Path    string `json:"path"`
	Content []byte `json:"content"`
}
