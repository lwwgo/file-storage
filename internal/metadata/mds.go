// Package metadata 实现分布式文件系统的元数据服务器（MDS）。
//
// MDS 职责：
//  1. 管理全局目录树（文件名、目录结构、权限、时间戳）
//  2. 管理文件→数据节点的映射（文件内容存在哪个 DataNode 上）
//  3. 管理 DataNode 注册（DataNode 启动时向 MDS 注册）
//
// 高可用：多个 MDS 节点组成 Raft 集群，通过 goraft 保证元数据一致性。
//   - 写操作（Mkdir/CreateFile/Delete/RegisterDataNode）：仅 leader 可处理，走 Raft 共识复制到多数派后应用。
//   - 读操作（GetFileLocation/ListDir/Stat/ListDataNodes）：所有节点都可处理，直接读本地状态机副本。
//   - follower 收到写请求时返回 leader 地址，客户端自动重定向。
package metadata

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lwwgo/file-storage/internal/types"
	"github.com/lwwgo/goraft/raft"
	rafttypes "github.com/lwwgo/goraft/types"
)

// 写操作类型常量
const (
	OpRegisterDataNode = "register_dn"
	OpMkdir            = "mkdir"
	OpCreateFile       = "create_file"
	OpDelete           = "delete"
)

// commandPayload 是 Raft 日志条目中承载的业务命令载荷。
// 所有字段通过 JSON 序列化后存入 CommandEntry.Data，
// 保证所有副本以相同顺序应用相同命令，达到状态机一致。
type commandPayload struct {
	Op           string `json:"op"`
	Path         string `json:"path,omitempty"`
	Addr         string `json:"addr,omitempty"`           // register_dn: 数据节点地址
	Size         int64  `json:"size,omitempty"`           // create_file: 文件大小
	DataNodeAddr string `json:"data_node_addr,omitempty"` // create_file 选中的 / delete 目标所在的
	RemotePath   string `json:"remote_path,omitempty"`    // create_file/delete: 数据节点上的路径
	IsDir        bool   `json:"is_dir,omitempty"`         // delete: 目标是否为目录
}

// entry 是目录树中的一个节点（文件或目录）。
type entry struct {
	name      string
	isDir     bool
	size      int64
	mode      uint32
	createdAt time.Time
	modTime   time.Time

	// 仅文件节点使用：指向数据节点的映射
	dataNodeAddr string
	remotePath   string

	// 仅目录节点使用：子节点
	children map[string]*entry
}

// MetadataServer 是元数据服务器的核心实现。
// 它同时是 goraft 的 StateMachine 实现：Raft 负责日志复制，
// MetadataServer 负责把日志应用到目录树状态机。
type MetadataServer struct {
	mu         sync.RWMutex
	root       *entry   // 根目录
	dataNodes  []string // 已注册的数据节点列表
	raftServer *raft.Server
	logger     *slog.Logger
}

// NewMetadataServer 创建并启动集成了 Raft 的元数据服务器。
// raftConfig 是 goraft 的配置（必须包含 LocalID、Peers、WalDir、SnapDir）。
// 注意：本方法不启动 RPC listener，由 main.go 统一管理 rpc.Server，
// 将 Raft 内部服务（"Server"）和业务服务（"MetadataService"）注册到同一端口，
// 这样 GetLeader 返回的地址客户端可直接用于业务 RPC。
func NewMetadataServer(raftConfig rafttypes.Config, logger *slog.Logger) (*MetadataServer, error) {
	mds := &MetadataServer{
		root: &entry{
			name:      "/",
			isDir:     true,
			mode:      0755,
			createdAt: time.Now(),
			modTime:   time.Now(),
			children:  make(map[string]*entry),
		},
		dataNodes: make([]string, 0),
		logger:    logger,
	}

	// 把 mds 作为 StateMachine 注入 Raft
	raftConfig.StateMachine = mds
	raftConfig.Logger = logger

	// 初始化 Raft 节点（不自动启动 RPC，由 main.go 统一管理）
	raftNode, err := raft.InitServer(raftConfig)
	if err != nil {
		return nil, fmt.Errorf("init raft server failed: %w", err)
	}
	mds.raftServer = raftNode

	// 启动 Raft 选举和心跳定时器（非阻塞）
	raftNode.Start()

	logger.Info("metadata server with raft initialized",
		"local_id", raftConfig.LocalID,
		"peers", raftConfig.Peers)

	return mds, nil
}

// GetRaftNode 返回底层 Raft 节点，供 main.go 注册 Raft RPC 服务到共享 rpc.Server。
func (mds *MetadataServer) GetRaftNode() *raft.Server {
	return mds.raftServer
}

// IsLeader 返回当前 MDS 节点是否为 Raft leader。
func (mds *MetadataServer) IsLeader() bool {
	return mds.raftServer.IsLeader()
}

// GetLeader 返回当前 leader 的业务 RPC 地址（用于客户端重定向）。
// 注意：Raft 的 LocalID 存的是 raft RPC 地址，这里需要映射到业务地址。
func (mds *MetadataServer) GetLeader() string {
	return mds.raftServer.GetLeader()
}

// ===== 写操作（仅 leader 可处理，走 Raft 共识）=====

// checkLeader 检查当前节点是否为 leader，不是则返回错误并附带 leader 地址。
func (mds *MetadataServer) checkLeader() error {
	if !mds.raftServer.IsLeader() {
		leader := mds.raftServer.GetLeader()
		return &ErrNotLeader{Leader: leader}
	}
	return nil
}

// ErrNotLeader 表示当前节点不是 leader，客户端应重定向到 Leader。
type ErrNotLeader struct {
	Leader string
}

func (e *ErrNotLeader) Error() string {
	return fmt.Sprintf("not leader, redirect to %s", e.Leader)
}

// submitCommand 把命令封装成 Raft 日志并提交。
// 仅 leader 可调用。返回时命令已复制到多数派并应用到状态机。
func (mds *MetadataServer) submitCommand(payload *commandPayload) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal command payload failed: %w", err)
	}
	cmd := rafttypes.CommandEntry{
		Op:   payload.Op,
		Data: data,
	}
	if err := mds.raftServer.Do(cmd); err != nil {
		return fmt.Errorf("raft do failed: %w", err)
	}
	return nil
}

// RegisterDataNode 数据节点向 MDS 注册。
func (mds *MetadataServer) RegisterDataNode(addr string, reply *bool) error {
	if err := mds.checkLeader(); err != nil {
		return err
	}
	payload := &commandPayload{Op: OpRegisterDataNode, Addr: addr}
	if err := mds.submitCommand(payload); err != nil {
		return err
	}
	*reply = true
	mds.logger.Info("data node registered via raft", "addr", addr)
	return nil
}

// Mkdir 创建目录。
func (mds *MetadataServer) Mkdir(path string, reply *bool) error {
	if err := mds.checkLeader(); err != nil {
		return err
	}
	payload := &commandPayload{Op: OpMkdir, Path: path}
	if err := mds.submitCommand(payload); err != nil {
		return err
	}
	*reply = true
	mds.logger.Info("directory created via raft", "path", path)
	return nil
}

// CreateFile 创建文件元数据，MDS 选择一个数据节点分配。
func (mds *MetadataServer) CreateFile(args *types.CreateFileArgs, reply *types.CreateFileReply) error {
	if err := mds.checkLeader(); err != nil {
		return err
	}

	// 预验证：检查数据节点可用 + 父目录存在 + 选数据节点
	mds.mu.RLock()
	if len(mds.dataNodes) == 0 {
		mds.mu.RUnlock()
		return fmt.Errorf("no data nodes registered")
	}
	parts := splitPath(args.Path)
	if len(parts) == 0 {
		mds.mu.RUnlock()
		return fmt.Errorf("invalid path: %s", args.Path)
	}
	dirParts := parts[:len(parts)-1]
	current := mds.root
	for _, part := range dirParts {
		child, exists := current.children[part]
		if !exists || !child.isDir {
			mds.mu.RUnlock()
			return fmt.Errorf("parent directory not found: %s", args.Path)
		}
		current = child
	}
	selectedAddr := mds.selectDataNode(args.Path)
	remotePath := args.Path
	mds.mu.RUnlock()

	payload := &commandPayload{
		Op:           OpCreateFile,
		Path:         args.Path,
		Size:         args.Size,
		DataNodeAddr: selectedAddr,
		RemotePath:   remotePath,
	}
	if err := mds.submitCommand(payload); err != nil {
		return err
	}

	reply.DataNodeAddr = selectedAddr
	reply.RemotePath = remotePath
	mds.logger.Info("file metadata created via raft", "path", args.Path, "data_node", selectedAddr)
	return nil
}

// Delete 删除文件或目录。
func (mds *MetadataServer) Delete(path string, reply *types.DeleteReply) error {
	if err := mds.checkLeader(); err != nil {
		return err
	}

	// 预查找目标，取出需要返回给客户端的清理信息
	mds.mu.RLock()
	target := mds.lookup(path)
	if target == nil {
		mds.mu.RUnlock()
		return fmt.Errorf("path not found: %s", path)
	}
	payload := &commandPayload{
		Op:           OpDelete,
		Path:         path,
		IsDir:        target.isDir,
		DataNodeAddr: target.dataNodeAddr,
		RemotePath:   target.remotePath,
	}
	mds.mu.RUnlock()

	if err := mds.submitCommand(payload); err != nil {
		return err
	}

	reply.IsDir = payload.IsDir
	reply.DataNodeAddr = payload.DataNodeAddr
	reply.RemotePath = payload.RemotePath
	mds.logger.Info("path deleted via raft", "path", path, "is_dir", payload.IsDir)
	return nil
}

// ===== 读操作（所有节点都可处理，直接读本地状态机）=====

// GetFileLocation 查询文件内容在哪个数据节点。
func (mds *MetadataServer) GetFileLocation(path string, reply *types.FileLocation) error {
	mds.mu.RLock()
	defer mds.mu.RUnlock()

	e := mds.lookup(path)
	if e == nil {
		return fmt.Errorf("file not found: %s", path)
	}
	if e.isDir {
		return fmt.Errorf("%s is a directory", path)
	}
	reply.DataNodeAddr = e.dataNodeAddr
	reply.RemotePath = e.remotePath
	return nil
}

// ListDir 列出目录内容。
func (mds *MetadataServer) ListDir(path string, reply *[]types.FileInfo) error {
	mds.mu.RLock()
	defer mds.mu.RUnlock()

	e := mds.lookup(path)
	if e == nil {
		return fmt.Errorf("directory not found: %s", path)
	}
	if !e.isDir {
		return fmt.Errorf("%s is not a directory", path)
	}

	var infos []types.FileInfo
	for name, child := range e.children {
		infos = append(infos, types.FileInfo{
			Name:      name,
			Path:      filepath.Join(path, name),
			IsDir:     child.isDir,
			Size:      child.size,
			Mode:      child.mode,
			CreatedAt: child.createdAt,
			ModTime:   child.modTime,
		})
	}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].IsDir != infos[j].IsDir {
			return infos[i].IsDir
		}
		return infos[i].Name < infos[j].Name
	})
	*reply = infos
	return nil
}

// Stat 获取文件/目录元数据。
func (mds *MetadataServer) Stat(path string, reply *types.FileInfo) error {
	mds.mu.RLock()
	defer mds.mu.RUnlock()

	e := mds.lookup(path)
	if e == nil {
		return fmt.Errorf("path not found: %s", path)
	}
	*reply = types.FileInfo{
		Name:      e.name,
		Path:      path,
		IsDir:     e.isDir,
		Size:      e.size,
		Mode:      e.mode,
		CreatedAt: e.createdAt,
		ModTime:   e.modTime,
	}
	return nil
}

// ListDataNodes 列出已注册的数据节点。
func (mds *MetadataServer) ListDataNodes(_ struct{}, reply *[]string) error {
	mds.mu.RLock()
	defer mds.mu.RUnlock()
	*reply = mds.dataNodes
	return nil
}

// ===== 实现 goraft StateMachine 接口 =====

// Apply 应用一条已提交的日志命令到本地状态机。
// 此方法由 Raft 保证在所有节点上以相同顺序调用，且仅做确定性的内存修改。
func (mds *MetadataServer) Apply(op string, data []byte) error {
	var payload commandPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("unmarshal command payload failed: %w", err)
	}

	mds.mu.Lock()
	defer mds.mu.Unlock()

	switch op {
	case OpRegisterDataNode:
		return mds.applyRegisterDataNode(&payload)
	case OpMkdir:
		return mds.applyMkdir(&payload)
	case OpCreateFile:
		return mds.applyCreateFile(&payload)
	case OpDelete:
		return mds.applyDelete(&payload)
	default:
		return fmt.Errorf("unknown command op: %s", op)
	}
}

// Snapshot 生成当前状态机的快照数据（供 Raft 压缩日志用）。
func (mds *MetadataServer) Snapshot() ([]byte, error) {
	mds.mu.RLock()
	defer mds.mu.RUnlock()
	snap := metaSnapshot{
		Root:      mds.root.toSnapshot(),
		DataNodes: mds.dataNodes,
	}
	return json.Marshal(snap)
}

// Restore 从快照数据恢复状态机（Raft 节点启动时调用）。
func (mds *MetadataServer) Restore(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var snap metaSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("unmarshal snapshot failed: %w", err)
	}
	mds.mu.Lock()
	defer mds.mu.Unlock()
	mds.root = snap.toEntry()
	mds.dataNodes = snap.DataNodes
	mds.logger.Info("state machine restored from snapshot", "data_nodes", len(mds.dataNodes))
	return nil
}

// ===== Apply 内部实现（仅做内存修改，不涉及共识）=====

func (mds *MetadataServer) applyRegisterDataNode(p *commandPayload) error {
	for _, a := range mds.dataNodes {
		if a == p.Addr {
			return nil // 幂等
		}
	}
	mds.dataNodes = append(mds.dataNodes, p.Addr)
	return nil
}

func (mds *MetadataServer) applyMkdir(p *commandPayload) error {
	parts := splitPath(p.Path)
	current := mds.root
	now := time.Now()
	for i, part := range parts {
		child, exists := current.children[part]
		if !exists {
			child = &entry{
				name:      part,
				isDir:     true,
				mode:      0755,
				createdAt: now,
				modTime:   now,
				children:  make(map[string]*entry),
			}
			current.children[part] = child
			current.modTime = now
		} else if !child.isDir {
			return fmt.Errorf("%s is not a directory (at level %d: %s)", p.Path, i, part)
		}
		current = child
	}
	return nil
}

func (mds *MetadataServer) applyCreateFile(p *commandPayload) error {
	parts := splitPath(p.Path)
	if len(parts) == 0 {
		return fmt.Errorf("invalid path: %s", p.Path)
	}
	dirParts := parts[:len(parts)-1]
	fileName := parts[len(parts)-1]

	current := mds.root
	for _, part := range dirParts {
		child, exists := current.children[part]
		if !exists || !child.isDir {
			return fmt.Errorf("parent directory not found: %s", p.Path)
		}
		current = child
	}

	now := time.Now()
	current.children[fileName] = &entry{
		name:         fileName,
		isDir:        false,
		size:         p.Size,
		mode:         0644,
		createdAt:    now,
		modTime:      now,
		dataNodeAddr: p.DataNodeAddr,
		remotePath:   p.RemotePath,
	}
	current.modTime = now
	return nil
}

func (mds *MetadataServer) applyDelete(p *commandPayload) error {
	parts := splitPath(p.Path)
	if len(parts) == 0 {
		return fmt.Errorf("cannot delete root")
	}
	dirParts := parts[:len(parts)-1]
	fileName := parts[len(parts)-1]

	current := mds.root
	for _, part := range dirParts {
		child, exists := current.children[part]
		if !exists || !child.isDir {
			return fmt.Errorf("path not found: %s", p.Path)
		}
		current = child
	}

	if _, exists := current.children[fileName]; !exists {
		return fmt.Errorf("path not found: %s", p.Path)
	}
	delete(current.children, fileName)
	current.modTime = time.Now()
	return nil
}

// ===== 内部辅助方法 =====

func splitPath(path string) []string {
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimSuffix(path, "/")
	if path == "" {
		return nil
	}
	var parts []string
	for _, p := range strings.Split(path, "/") {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

// lookup 在目录树中查找路径，找不到返回 nil。调用方需持读锁。
func (mds *MetadataServer) lookup(path string) *entry {
	parts := splitPath(path)
	current := mds.root
	for _, part := range parts {
		child, exists := current.children[part]
		if !exists {
			return nil
		}
		current = child
	}
	return current
}

// selectDataNode 轮询选择一个数据节点（简单哈希取模）。
func (mds *MetadataServer) selectDataNode(path string) string {
	hash := 0
	for _, c := range path {
		hash = hash*31 + int(c)
	}
	idx := hash % len(mds.dataNodes)
	if idx < 0 {
		idx = -idx
	}
	return mds.dataNodes[idx]
}

// ===== JSON 序列化辅助结构（Snapshot/Restore 用）=====

type metaSnapshot struct {
	Root      *entrySnapshot `json:"root"`
	DataNodes []string       `json:"data_nodes"`
}

func (s *metaSnapshot) toEntry() *entry {
	if s.Root == nil {
		return &entry{
			name:     "/",
			isDir:    true,
			mode:     0755,
			children: make(map[string]*entry),
		}
	}
	return s.Root.toEntry()
}

type entrySnapshot struct {
	Name         string                    `json:"name"`
	IsDir        bool                      `json:"is_dir"`
	Size         int64                     `json:"size"`
	Mode         uint32                    `json:"mode"`
	CreatedAt    time.Time                 `json:"created_at"`
	ModTime      time.Time                 `json:"mod_time"`
	DataNodeAddr string                    `json:"data_node_addr,omitempty"`
	RemotePath   string                    `json:"remote_path,omitempty"`
	Children     map[string]*entrySnapshot `json:"children,omitempty"`
}

func (e *entry) toSnapshot() *entrySnapshot {
	s := &entrySnapshot{
		Name:         e.name,
		IsDir:        e.isDir,
		Size:         e.size,
		Mode:         e.mode,
		CreatedAt:    e.createdAt,
		ModTime:      e.modTime,
		DataNodeAddr: e.dataNodeAddr,
		RemotePath:   e.remotePath,
	}
	if e.isDir {
		s.Children = make(map[string]*entrySnapshot)
		for name, child := range e.children {
			s.Children[name] = child.toSnapshot()
		}
	}
	return s
}

func (s *entrySnapshot) toEntry() *entry {
	e := &entry{
		name:         s.Name,
		isDir:        s.IsDir,
		size:         s.Size,
		mode:         s.Mode,
		createdAt:    s.CreatedAt,
		modTime:      s.ModTime,
		dataNodeAddr: s.DataNodeAddr,
		remotePath:   s.RemotePath,
	}
	if s.IsDir {
		e.children = make(map[string]*entry)
		for name, childSnap := range s.Children {
			e.children[name] = childSnap.toEntry()
		}
	}
	return e
}
