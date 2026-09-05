package metadata

import (
	"fmt"
	"time"

	"github.com/lwwgo/file-storage/internal/types"
)

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

	// Existence check in Raft Apply (authoritative, serial) to avoid time of check to time of use race.
	existing, exists := current.children[fileName]
	if exists {
		if existing.isDir {
			return fmt.Errorf("%s is a directory, cannot create file with same name", p.Path)
		}
		// Default mode (CreateIfNotExist): reject if file already exists.
		if p.CreateMode != types.OverwriteIfExists {
			return fmt.Errorf("file already exists: %s", p.Path)
		}
		// Overwrite mode: replace metadata. NOTE: old replicas' data on DataNodes
		// becomes orphaned and requires separate garbage collection (known limitation).
	}

	now := time.Now()
	createdAt := now
	if exists {
		// POSIX O_TRUNC preserves creation time; only modTime updates.
		createdAt = existing.createdAt
	}
	current.children[fileName] = &entry{
		name:      fileName,
		isDir:     false,
		size:      p.Size,
		mode:      0644,
		createdAt: createdAt,
		modTime:   now,
		status:    StatusPending,
		replicas:  p.Replicas,
	}
	current.modTime = now
	return nil
}

func (mds *MetadataServer) applyCompleteFile(p *commandPayload) error {
	e := mds.lookup(p.Path)
	if e == nil {
		return fmt.Errorf("path not found: %s", p.Path)
	}
	if e.isDir {
		return fmt.Errorf("%s is a directory, cannot complete", p.Path)
	}
	e.status = StatusComplete
	e.modTime = time.Now()
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
