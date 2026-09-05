package client

import (
	"fmt"
	"io"
	"sync"

	"github.com/lwwgo/fstorex/internal/types"
)

// ===== FileHandle：随机读写句柄（Phase 1.5，为 FUSE 做准备）=====

// Open flags，对齐 POSIX。
const (
	O_RDONLY = 0x00
	O_WRONLY = 0x01
	O_RDWR   = 0x02
	O_CREAT  = 0x40
	O_EXCL   = 0x80
	O_TRUNC  = 0x200
)

// FileHandle 是一个已打开文件的句柄，支持随机读写。
// 关键设计：保存 FileID（而非只保存 path），这样 open 后即使 path 被
// rename/unlink，fd 仍能正确读写原对象（符合 POSIX inode 语义）。
type FileHandle struct {
	client   *Client
	path     string
	fileID   string
	flags    int
	replicas []types.Replica
	size     int64
	status   string     // "pending" or "complete"
	mu       sync.Mutex // protects size and status for concurrent access from FUSE layer
}

// ReadAt 从 offset 随机读取最多 len(buf) 字节。
// 副本故障转移：选一个副本读，失败自动 try 下一个。
// 到达 EOF 时返回实际读到的字节数 + io.EOF。
func (fh *FileHandle) ReadAt(buf []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("readat: negative offset")
	}
	if len(fh.replicas) == 0 {
		return 0, fmt.Errorf("readat: no replicas for file %s", fh.path)
	}

	var lastErr error
	for _, replica := range fh.replicas {
		n, eof, err := fh.client.rangeRead(replica, offset, buf)
		if err == nil {
			// EOF 是正常情况，返回 io.EOF 让调用方判断
			if eof {
				return n, io.EOF
			}
			return n, nil
		}
		lastErr = err
		fh.client.logger.Warn("readat: replica failed, trying next",
			"data_node", replica.Addr, "error", err)
	}
	return 0, fmt.Errorf("readat: all %d replicas failed, last error: %w", len(fh.replicas), lastErr)
}

// WriteAt 向 offset 随机写入 data，支持 sparse file。
//
// 关键语义：
//   - 所有副本都成功才返回成功（Phase 1.5 不做 quorum，避免副本不一致）
//   - 任一副本失败 → 返回错误，不更新 MDS size
//   - 全部成功后，若写入跨越当前 size 末尾，更新 MDS size（先数据后元数据）
func (fh *FileHandle) WriteAt(data []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("writeat: negative offset")
	}
	if len(fh.replicas) == 0 {
		return 0, fmt.Errorf("writeat: no replicas for file %s", fh.path)
	}

	// 并行写所有副本
	var wg sync.WaitGroup
	errs := make([]error, len(fh.replicas))
	for i, replica := range fh.replicas {
		wg.Add(1)
		go func(idx int, r types.Replica) {
			defer wg.Done()
			errs[idx] = fh.client.partialWrite(r, offset, data)
		}(i, replica)
	}
	wg.Wait()

	// 检查全部结果
	var firstErr error
	successCount := 0
	for _, err := range errs {
		if err == nil {
			successCount++
		} else if firstErr == nil {
			firstErr = err
		}
	}

	if successCount != len(fh.replicas) {
		return 0, fmt.Errorf("writeat: %d/%d replicas failed, first error: %w",
			len(fh.replicas)-successCount, len(fh.replicas), firstErr)
	}

	// 全部成功 → 若跨越 size 末尾，更新 MDS size（先更新本地缓存，RPC 在锁外）
	newEnd := offset + int64(len(data))
	fh.mu.Lock()
	needUpdate := newEnd > fh.size
	if needUpdate {
		fh.size = newEnd
	}
	wasPending := fh.status == "pending"
	if wasPending {
		fh.status = "complete"
	}
	fh.mu.Unlock()

	if needUpdate {
		if err := fh.client.updateSize(fh.path, newEnd); err != nil {
			// 数据已写入所有副本，但元数据更新失败：非致命，日志告警
			fh.client.logger.Warn("writeat: data written but update size failed",
				"path", fh.path, "new_size", newEnd, "error", err)
		}
	}

	return len(data), nil
}

// Close 关闭句柄，释放资源。
// 若文件仍 pending（Open 后未写任何数据），标记为 complete。
func (fh *FileHandle) Close() error {
	if fh == nil {
		return nil
	}
	fh.mu.Lock()
	wasPending := fh.status == "pending"
	size := fh.size
	fh.mu.Unlock()

	if wasPending {
		if err := fh.client.CompleteFile(fh.path); err != nil {
			return fmt.Errorf("close: complete file failed: %w", err)
		}
	}
	fh.client.logger.Info("file closed", "path", fh.path, "file_id", fh.fileID, "size", size)
	return nil
}

// Truncate 调整文件大小（缩小丢弃尾部，扩大补 0）。
// 时序：先全副本物理截断 → 全部成功才更新 MDS 元数据 → 更新本地缓存。
func (fh *FileHandle) Truncate(size int64) error {
	if size < 0 {
		return fmt.Errorf("truncate: negative size")
	}
	if err := fh.client.truncateAllReplicas(fh.path, size); err != nil {
		return fmt.Errorf("truncate: replica truncate failed: %w", err)
	}
	// 先数据后元数据
	if err := fh.client.updateSize(fh.path, size); err != nil {
		return fmt.Errorf("truncate: update size failed: %w", err)
	}
	fh.mu.Lock()
	fh.size = size
	fh.mu.Unlock()
	fh.client.logger.Info("file truncated", "path", fh.path, "size", size)
	return nil
}

// Sync 强制 fsync 所有副本确保持久化。全部成功才返回成功。
func (fh *FileHandle) Sync() error {
	var wg sync.WaitGroup
	errs := make([]error, len(fh.replicas))
	for i, replica := range fh.replicas {
		wg.Add(1)
		go func(idx int, r types.Replica) {
			defer wg.Done()
			errs[idx] = fh.client.syncReplica(r)
		}(i, replica)
	}
	wg.Wait()

	var firstErr error
	successCount := 0
	for _, err := range errs {
		if err == nil {
			successCount++
		} else if firstErr == nil {
			firstErr = err
		}
	}
	if successCount != len(fh.replicas) {
		return fmt.Errorf("sync: %d/%d replicas failed, first error: %w",
			len(fh.replicas)-successCount, len(fh.replicas), firstErr)
	}
	fh.client.logger.Debug("file synced", "path", fh.path)
	return nil
}
