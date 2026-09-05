// Package client_test 提供端到端集成测试，验证 FileHandle 全套能力。
// 测试方式：in-process 启动单节点 MDS（Raft）+ 单 DataNode，用 Client 连接。
package client_test

import (
	"io"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lwwgo/fstorex/internal/client"
	"github.com/lwwgo/fstorex/internal/datanode"
	"github.com/lwwgo/fstorex/internal/metadata"
	rafttypes "github.com/lwwgo/goraft/types"
)

// testCluster 封装一个 in-process 启动的单节点集群。
type testCluster struct {
	mdsAddr string
	dnAddr  string
	cleanup func()
}

// startCluster 启动单节点 MDS + 单 DataNode，返回测试集群。
func startCluster(t *testing.T) *testCluster {
	t.Helper()

	// 拿随机端口
	mdsLn, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen mds: %v", err)
	}
	mdsAddr := mdsLn.Addr().String()

	dnLn, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen dn: %v", err)
	}
	dnAddr := dnLn.Addr().String()

	// 目录
	baseDir := t.TempDir()
	walDir := filepath.Join(baseDir, "mds-wal")
	snapDir := filepath.Join(baseDir, "mds-snap")
	dnDir := filepath.Join(baseDir, "dn-data")
	os.MkdirAll(walDir, 0755)
	os.MkdirAll(snapDir, 0755)
	os.MkdirAll(dnDir, 0755)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// 启动 MDS（单节点 Raft）
	raftConfig := rafttypes.Config{
		LocalID:         mdsAddr,
		Peers:           []string{mdsAddr}, // 单节点：需包含自己才能投票选举
		WalDir:          walDir,
		SnapDir:         snapDir,
		MaxIndexSpan:    1000,
		ElectionTimeout: 500 * time.Millisecond,
	}
	mds, err := metadata.NewMetadataServer(raftConfig, logger)
	if err != nil {
		t.Fatalf("create mds: %v", err)
	}

	mdsRPC := rpc.NewServer()
	mdsRPC.RegisterName("Server", mds.GetRaftNode())
	mdsRPC.RegisterName("MetadataService", mds)

	go func() {
		for {
			conn, err := mdsLn.Accept()
			if err != nil {
				return
			}
			go mdsRPC.ServeConn(conn)
		}
	}()

	// 先等 MDS 选出 leader（轮询直到能成功写）
	c := client.NewClient(mdsAddr, logger)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.Mkdir("/probe"); err == nil {
			c.Delete("/probe")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// leader 就绪后再启动 DataNode，确保第一次心跳就能成功注册
	dn, err := datanode.NewDataNode(dnDir, mdsAddr, dnAddr, logger)
	if err != nil {
		t.Fatalf("create dn: %v", err)
	}
	dnRPC := rpc.NewServer()
	if err := dnRPC.RegisterName("DataService", dn); err != nil {
		t.Fatalf("register dn rpc: %v", err)
	}

	go func() {
		for {
			conn, err := dnLn.Accept()
			if err != nil {
				return
			}
			go dnRPC.ServeConn(conn)
		}
	}()

	dn.StartHeartbeat()

	// 轮询等待 DataNode 注册成功
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		nodes, err := c.ListDataNodes()
		if err == nil && len(nodes) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return &testCluster{
		mdsAddr: mdsAddr,
		dnAddr:  dnAddr,
		cleanup: func() {
			mdsLn.Close()
			dnLn.Close()
		},
	}
}

func (tc *testCluster) newClient(t *testing.T) *client.Client {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return client.NewClient(tc.mdsAddr, logger)
}

// ===== 测试场景 =====

// 场景 1：空文件读（O_CREAT 后未写）返回 0 + EOF
func TestEmptyFileRead(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	fh, err := c.Open("/empty.txt", client.O_CREAT|client.O_RDWR)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer fh.Close()

	buf := make([]byte, 100)
	n, err := fh.ReadAt(buf, 0)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got err=%v n=%d", err, n)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// 场景 2：写后读一致
func TestWriteReadConsistency(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	// 写
	fh, err := c.Open("/hello.txt", client.O_CREAT|client.O_WRONLY)
	if err != nil {
		t.Fatalf("open for write: %v", err)
	}
	data := []byte("hello fstorex")
	n, err := fh.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("writeat: %v", err)
	}
	if n != len(data) {
		t.Fatalf("writeat: expected %d, got %d", len(data), n)
	}
	if err := fh.Close(); err != nil {
		t.Fatalf("close write: %v", err)
	}

	// 读
	fh2, err := c.Open("/hello.txt", client.O_RDONLY)
	if err != nil {
		t.Fatalf("open for read: %v", err)
	}
	defer fh2.Close()

	buf := make([]byte, 100)
	n, err = fh2.ReadAt(buf, 0)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got err=%v", err)
	}
	if string(buf[:n]) != string(data) {
		t.Fatalf("data mismatch: expected %q, got %q", data, buf[:n])
	}
}

// 场景 3：sparse write
func TestSparseWrite(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	fh, err := c.Open("/sparse.txt", client.O_CREAT|client.O_RDWR)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer fh.Close()

	// 写 offset 100 的 "x"
	_, err = fh.WriteAt([]byte("x"), 100)
	if err != nil {
		t.Fatalf("writeat: %v", err)
	}

	// 读 offset 0 的 101 字节（刚好读完整个文件，EOF 可有可无）
	buf := make([]byte, 101)
	n, _ := fh.ReadAt(buf, 0)
	if n != 101 {
		t.Fatalf("expected 101 bytes, got %d", n)
	}
	// 前 100 字节应为 0
	for i := 0; i < 100; i++ {
		if buf[i] != 0 {
			t.Fatalf("byte %d should be 0, got %d", i, buf[i])
		}
	}
	if buf[100] != 'x' {
		t.Fatalf("byte 100 should be 'x', got %q", buf[100])
	}
}

// 场景 4：O_TRUNC 后 FileID 不变
func TestTruncateKeepsFileID(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	// 第一次写
	fh, err := c.Open("/trunc.txt", client.O_CREAT|client.O_WRONLY)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	fh.WriteAt([]byte("first content"), 0)
	fh.Close()

	info1, err := c.Stat("/trunc.txt")
	if err != nil {
		t.Fatalf("stat 1: %v", err)
	}
	fileID1 := info1.FileID
	if fileID1 == "" {
		t.Fatal("FileID should not be empty")
	}

	// O_TRUNC 重写
	fh2, err := c.Open("/trunc.txt", client.O_TRUNC|client.O_WRONLY)
	if err != nil {
		t.Fatalf("open trunc: %v", err)
	}
	fh2.WriteAt([]byte("second"), 0)
	fh2.Close()

	info2, err := c.Stat("/trunc.txt")
	if err != nil {
		t.Fatalf("stat 2: %v", err)
	}
	if info2.FileID != fileID1 {
		t.Fatalf("FileID changed after O_TRUNC: %s -> %s", fileID1, info2.FileID)
	}
	if info2.Size != 6 {
		t.Fatalf("expected size 6, got %d", info2.Size)
	}
}

// 场景 5：Truncate 缩小后读返回截断内容
func TestTruncateShrink(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	fh, err := c.Open("/shrink.txt", client.O_CREAT|client.O_RDWR)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer fh.Close()

	// 写 100 字节
	data := make([]byte, 100)
	for i := range data {
		data[i] = 'A'
	}
	fh.WriteAt(data, 0)

	// 截断到 50
	if err := fh.Truncate(50); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// 读应只能读 50 字节
	buf := make([]byte, 100)
	n, err := fh.ReadAt(buf, 0)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got err=%v", err)
	}
	if n != 50 {
		t.Fatalf("expected 50 bytes after truncate, got %d", n)
	}
}

// 场景 6：Rename 后旧 fd 仍可读
func TestRenameOldFDValid(t *testing.T) {
	tc := startCluster(t)
	defer tc.cleanup()
	c := tc.newClient(t)

	// 打开并写
	fh, err := c.Open("/old.txt", client.O_CREAT|client.O_RDWR)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	data := []byte("persist after rename")
	fh.WriteAt(data, 0)

	// rename
	if err := c.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatalf("rename: %v", err)
	}

	// 旧 fd 仍可读
	buf := make([]byte, 100)
	n, err := fh.ReadAt(buf, 0)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got err=%v", err)
	}
	if string(buf[:n]) != string(data) {
		t.Fatalf("old fd read mismatch: expected %q, got %q", data, buf[:n])
	}

	// 新路径也能读
	fh2, err := c.Open("/new.txt", client.O_RDONLY)
	if err != nil {
		t.Fatalf("open new path: %v", err)
	}
	defer fh2.Close()
	n2, _ := fh2.ReadAt(buf, 0)
	if string(buf[:n2]) != string(data) {
		t.Fatalf("new path read mismatch: expected %q, got %q", data, buf[:n2])
	}

	fh.Close()
}
