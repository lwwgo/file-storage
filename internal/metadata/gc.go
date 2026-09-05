package metadata

import (
	"net/rpc"
	"time"

	"github.com/lwwgo/file-storage/internal/types"
)

const (
	// gcInterval is how often the leader scans for orphan data on DataNodes.
	gcInterval = 5 * time.Minute

	// gcSafetyWindow is the grace period: files modified within this window before
	// GC start are skipped to avoid deleting files created during the GC scan
	// (which may not yet appear in the validPaths snapshot).
	gcSafetyWindow = 30 * time.Second
)

// startGC starts the background garbage collection goroutine.
// Only the leader should run GC; followers do nothing.
func (mds *MetadataServer) startGC() {
	go func() {
		ticker := time.NewTicker(gcInterval)
		defer ticker.Stop()
		for range ticker.C {
			if !mds.IsLeader() {
				continue
			}
			mds.runGC()
		}
	}()
	mds.logger.Info("background orphan GC started", "interval", gcInterval)
}

// runGC performs one GC cycle: scan all DataNodes and delete orphan files.
func (mds *MetadataServer) runGC() {
	mds.logger.Debug("GC cycle started")

	// gcStart marks the beginning of this cycle. Files modified after
	// gcStart - gcSafetyWindow are protected to avoid deleting files
	// created during the GC scan (they may not yet be in validPaths).
	gcStart := time.Now()

	// 1. Collect all valid file paths from metadata tree
	validPaths := mds.collectValidPaths()
	mds.logger.Debug("collected valid paths", "count", len(validPaths))

	// 2. Get all registered DataNodes
	mds.mu.RLock()
	nodes := make([]string, len(mds.dataNodes))
	copy(nodes, mds.dataNodes)
	mds.mu.RUnlock()

	// 3. For each DataNode, list its files and delete orphans
	for _, addr := range nodes {
		mds.garbageCollectNode(addr, validPaths, gcStart)
	}

	mds.logger.Debug("GC cycle finished")
}

// collectValidPaths walks the metadata tree and returns all file paths.
func (mds *MetadataServer) collectValidPaths() map[string]bool {
	mds.mu.RLock()
	defer mds.mu.RUnlock()

	paths := make(map[string]bool)
	mds.collectPathsRecursive(mds.root, "", paths)
	return paths
}

func (mds *MetadataServer) collectPathsRecursive(e *entry, prefix string, paths map[string]bool) {
	if e == nil {
		return
	}
	if !e.isDir {
		paths[prefix] = true
		return
	}
	for name, child := range e.children {
		childPath := prefix + "/" + name
		mds.collectPathsRecursive(child, childPath, paths)
	}
}

// garbageCollectNode connects to a DataNode, lists its files, and deletes orphans.
// gcStart is the time the current GC cycle started; files modified after
// gcStart - gcSafetyWindow are skipped to protect files created during the scan.
func (mds *MetadataServer) garbageCollectNode(addr string, validPaths map[string]bool, gcStart time.Time) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		mds.logger.Warn("GC: cannot connect to data node, skipping", "node", addr, "error", err)
		return
	}
	defer client.Close()

	var nodeFiles []types.NodeFile
	if err := client.Call("DataService.ListAllPaths", struct{}{}, &nodeFiles); err != nil {
		mds.logger.Warn("GC: failed to list files from data node", "node", addr, "error", err)
		return
	}

	// Files modified after this cutoff are too new to be safe to delete.
	cutoff := gcStart.Add(-gcSafetyWindow)

	deleted := 0
	skippedNew := 0
	for _, nf := range nodeFiles {
		if validPaths[nf.Path] {
			continue // still referenced by metadata, keep it
		}
		// mtime protection: skip recently modified files that may have been
		// created during the GC scan but not yet in validPaths.
		if nf.ModTime.After(cutoff) {
			skippedNew++
			continue
		}
		// Orphan: delete from this DataNode
		var reply bool
		if err := client.Call("DataService.DeleteData", nf.Path, &reply); err != nil {
			mds.logger.Warn("GC: failed to delete orphan", "node", addr, "path", nf.Path, "error", err)
			continue
		}
		deleted++
		mds.logger.Info("GC: deleted orphan file", "node", addr, "path", nf.Path)
	}

	mds.logger.Info("GC: data node cleaned", "node", addr, "orphans_deleted", deleted, "skipped_recent", skippedNew, "total_files", len(nodeFiles))
}

// TriggerGC is the RPC handler for manually triggering one GC cycle.
// Only the leader can run GC; followers return a redirect error.
func (mds *MetadataServer) TriggerGC(_ struct{}, reply *bool) error {
	if err := mds.checkLeader(); err != nil {
		return err
	}
	go mds.runGC()
	*reply = true
	mds.logger.Info("manual GC triggered")
	return nil
}
