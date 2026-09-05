package metadata

import (
	"net/rpc"
	"time"
)

// gcInterval is how often the leader scans for orphan data on DataNodes.
const gcInterval = 5 * time.Minute

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

	// 1. Collect all valid file paths from metadata tree
	validPaths := mds.collectValidPaths()
	mds.logger.Debug("collected valid paths", "count", len(validPaths))

	// 2. Get all registered DataNodes
	mds.mu.RLock()
	nodes := make([]string, len(mds.dataNodes))
	copy(nodes, mds.dataNodes)
	mds.mu.RUnlock()

	// 3. For each DataNode, list its paths and delete orphans
	for _, addr := range nodes {
		mds.garbageCollectNode(addr, validPaths)
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
func (mds *MetadataServer) garbageCollectNode(addr string, validPaths map[string]bool) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		mds.logger.Warn("GC: cannot connect to data node, skipping", "node", addr, "error", err)
		return
	}
	defer client.Close()

	var nodePaths []string
	if err := client.Call("DataService.ListAllPaths", struct{}{}, &nodePaths); err != nil {
		mds.logger.Warn("GC: failed to list paths from data node", "node", addr, "error", err)
		return
	}

	deleted := 0
	for _, path := range nodePaths {
		if validPaths[path] {
			continue // still referenced by metadata, keep it
		}
		// Orphan: delete from this DataNode
		var reply bool
		if err := client.Call("DataService.DeleteData", path, &reply); err != nil {
			mds.logger.Warn("GC: failed to delete orphan", "node", addr, "path", path, "error", err)
			continue
		}
		deleted++
		mds.logger.Info("GC: deleted orphan file", "node", addr, "path", path)
	}

	mds.logger.Info("GC: data node cleaned", "node", addr, "orphans_deleted", deleted, "total_files", len(nodePaths))
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
