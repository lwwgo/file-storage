# File Storage — Distributed File Storage with Raft-based High Availability

A distributed file storage system providing a Unix-like hierarchical directory tree interface. The system consists of three independent binaries communicating via Go's standard `net/rpc` package, depending only on [goraft](https://github.com/lwwgo/goraft) (which itself has zero external dependencies). The metadata server uses the goraft consensus protocol to form a 3-node highly available cluster.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                            Client (CLI)                              │
│  put/get/ls/mkdir/rm/stat/replicas/nodes/gc                          │
│  Writes auto-redirect to leader, reads served by any node           │
└──────────────┬──────────────────────────┬───────────────────────────┘
               │                          │
        Metadata RPC                   Data RPC
               │                          │
               ▼                          ▼
┌───────────────────────────────┐       ┌───────────────────────┐
│   Metadata Server Cluster     │       │     Data Node         │
│   (3-node Raft, 1 leader,     │       │   (multiple,          │
│    2 followers)               │       │    horizontally       │
│                               │       │     scalable)         │
│  ┌───────┐  ┌───────┐  ┌────┐│       │  ┌─────────────────┐  │
│  │ MDS1  │  │ MDS2  │  │MDS3││       │  │  File content    │  │
│  │Leader │  │Follow │  │Fol ││       │  │  storage         │  │
│  └───┬───┘  └───┬───┘  └─┬──┘│       │  │  /dn1/docs/...   │  │
│      │          │        │   │       │  │  /dn2/images/..  │  │
│      └──── Raft log replication ┘    │  └─────────────────┘  │
│                               │       │                       │
│  ┌─────────────────────────┐ │       │  Stores actual        │
│  │  Directory tree          │ │       │  file content only;   │
│  │  (Raft state machine)   │ │       │  does not manage       │
│  │  /docs/readme           │ │       │  the directory tree    │
│  │  /images/logo           │ │       │                       │
│  └─────────────────────────┘ │       └───────────────────────┘
│  ┌─────────────────────────┐ │
│  │ File → DataNode mapping │ │
│  │ Multi-replica support   │ │
│  │ (default 3 replicas)    │ │
│  └─────────────────────────┘ │
└───────────────────────────────┘
```

### Raft High Availability Mechanisms

| Mechanism | Description |
|---|---|
| **Leader Election** | 3 MDS nodes elect a leader via Raft; the rest become followers |
| **Log Replication** | Write operations (Mkdir/CreateFile/CompleteFile/Delete/RegisterDataNode) are submitted by the leader to Raft, replicated to a majority, then applied to the state machine |
| **Local Reads** | Read operations (ListDir/Stat/GetFileLocation) are served directly from each node's local state machine replica |
| **Follower Redirect** | When a follower receives a write request, it returns the leader address; the client automatically retries against the leader |
| **Failover** | After a leader failure, remaining nodes automatically re-elect; no data loss |
| **WAL + Snapshot** | Each node persists state via WAL + Snapshot, automatically recovering after restart |

### Three Components

| Component | Binary | Default Port(s) | Responsibility |
|---|---|---|---|
| **Metadata Server** | `metadata-server` | 9001/9002/9003 | Manages the global directory tree, file→DataNode mapping, multi-replica allocation, and DataNode registration; guarantees multi-node consistency via Raft |
| **Data Node** | `data-node` | 9101+ | Stores actual file content to local disk; registers with MDS on startup (auto-redirects to leader); reports held paths for orphan GC |
| **Client** | `client` | — | CLI tool; metadata operations go through MDS, data operations go MDS→DataNode; writes auto-redirect from follower to leader |

### Data Flow

```
put:  Client ──RPC──→ MDS (leader creates metadata with pending status, selects N DataNodes)
                              │
                              ▼
                    returns N DataNode addresses
                              │
                              ▼
      Client ──Parallel RPC──→ DataNodes (store content to N replicas)
                              │
                              ▼
      Client ──RPC──→ MDS (mark file complete: pending → complete)

get:  Client ──RPC──→ MDS (any node looks up file location + status)
                              │
                              ▼
                    if pending → return empty content directly
                    if complete → return DataNode addresses
                              │
                              ▼
      Client ──RPC──→ DataNode (reads content, tries replicas in order)

mkdir/ls/stat/rm:  Client ──RPC──→ MDS (writes go through Raft consensus, reads served locally)
```

## Core Features

### Multi-Replica Storage

Every file is replicated across multiple DataNodes (default 3) for fault tolerance. The client uploads to all assigned replicas in parallel, and reads try each replica in order until one succeeds.

### File Lifecycle (Pending → Complete)

```
CreateFile ──→ status=pending (metadata exists, no data yet)
                    │
              Client uploads data to DataNodes
                    │
              CompleteFile ──→ status=complete (data ready for reads)
```

- **Pending files are treated as empty** by reads (like POSIX `creat()` before write), avoiding "ghost file" errors
- **CompleteFile marks data as ready** so subsequent reads can fetch from DataNodes
- If all uploads fail, the client performs best-effort metadata deletion to clean up

### Create Mode (POSIX-like)

Two mutually exclusive behaviors when a file already exists:

| Mode | Behavior | POSIX Equivalent |
|---|---|---|
| `CreateIfNotExist` (default) | Error if file exists | `O_CREAT\|O_EXCL` |
| `OverwriteIfExists` | Overwrite if file exists | `O_CREAT\|O_TRUNC` |

CLI usage: `./client put -overwrite local.txt /remote.txt`

### Orphan Data Garbage Collection

The MDS leader runs background GC every 5 minutes (plus manual `gc` command):

1. Walks the metadata tree to collect all valid file paths
2. Asks each DataNode for all paths it holds
3. Deletes any DataNode file not referenced by metadata (orphan)

This handles the case where delete operations fail to reach some DataNodes, ensuring eventual disk space reclamation.

### Ghost File Protection

Three layers of defense against "metadata exists but no data" inconsistencies:

1. **Client compensation**: If all replica uploads fail, the client immediately deletes the newly created metadata
2. **Empty file legality**: Pending files return empty content on read instead of erroring
3. **Background GC**: Orphan data on DataNodes is eventually cleaned up by the GC cycle

## Quick Start

### One-Click Demo

```bash
cd file-storage
chmod +x start.sh
./start.sh

# Stop all nodes
./start.sh clean
```

The script automatically builds and starts **3 MDS nodes (Raft cluster) + 2 DataNodes**, waits for leader election, then demonstrates the full put/get/ls/mkdir/rm flow, including reading from a follower node.

### Build & Test with Make

```bash
cd file-storage
make build    # Build all 3 binaries to bin/
make test     # Run unit tests with race detection + coverage
make lint     # Run golangci-lint (falls back to go vet)
make all      # lint + test + build
```

### Manually Build the Three Binaries

```bash
cd file-storage
mkdir -p bin
go build -o bin/metadata-server ./cmd/metadata-server
go build -o bin/data-node ./cmd/data-node
go build -o bin/client ./cmd/client
```

### Manually Start the Cluster

```bash
# 1. Start 3 MDS nodes (Raft cluster)
./bin/metadata-server -id=localhost:9001 -peers=localhost:9002,localhost:9003 -wal-dir=/tmp/mds1/wal -snap-dir=/tmp/mds1/snap
./bin/metadata-server -id=localhost:9002 -peers=localhost:9001,localhost:9003 -wal-dir=/tmp/mds2/wal -snap-dir=/tmp/mds2/snap
./bin/metadata-server -id=localhost:9003 -peers=localhost:9001,localhost:9002 -wal-dir=/tmp/mds3/wal -snap-dir=/tmp/mds3/snap

# 2. Start data nodes (connect to any MDS; auto-redirects to leader)
./bin/data-node -addr=:9101 -mds=localhost:9001 -data-dir=/tmp/dn1
./bin/data-node -addr=:9102 -mds=localhost:9001 -data-dir=/tmp/dn2

# 3. Use the client (connect to any MDS)
./bin/client -mds=localhost:9001 nodes
```

### Client Commands

```bash
# List registered data nodes
./bin/client -mds=localhost:9001 nodes

# Create a directory
./bin/client -mds=localhost:9001 mkdir /docs

# Upload a file (local path → distributed path)
./bin/client -mds=localhost:9001 put ./local.txt /docs/remote.txt

# Upload with overwrite (error if exists without -overwrite)
./bin/client -mds=localhost:9001 put -overwrite ./local.txt /docs/remote.txt

# Download a file (distributed path → local path)
./bin/client -mds=localhost:9001 get /docs/remote.txt ./downloaded.txt

# List a directory
./bin/client -mds=localhost:9001 ls /docs

# Show file/directory info
./bin/client -mds=localhost:9001 stat /docs/remote.txt

# Show replica locations for a file
./bin/client -mds=localhost:9001 replicas /docs/remote.txt

# Delete a file or directory
./bin/client -mds=localhost:9001 rm /docs/remote.txt

# Manually trigger orphan data garbage collection
./bin/client -mds=localhost:9001 gc
```

## Project Structure

```
file-storage/
├── cmd/                           # Three independent entry points
│   ├── metadata-server/main.go    # Metadata server binary (Raft node)
│   ├── data-node/main.go          # Data node binary
│   └── client/main.go             # Client CLI binary
├── internal/
│   ├── types/types.go             # Shared types + RPC interface definitions
│   ├── metadata/
│   │   ├── mds.go                 # MDS core: RPC handlers, StateMachine struct, snapshot
│   │   ├── state_machine.go       # goraft StateMachine interface (Apply/Snapshot/Restore)
│   │   ├── apply.go               # Apply internal implementations (mkdir/create/complete/delete)
│   │   └── gc.go                  # Background orphan data garbage collection
│   ├── datanode/datanode.go       # DataNode core: content storage + RPC
│   └── client/client.go           # Client logic: RPC wrapper + follower redirect
├── .golangci.yml                  # golangci-lint configuration
├── .gitignore                     # Ignores build artifacts and runtime data
├── Makefile                       # build/test/lint/clean targets
├── go.mod                         # require github.com/lwwgo/goraft v1.0.1
├── go.sum                         # Dependency checksums
├── start.sh                       # One-click demo script (3 MDS + 2 DataNode)
├── LICENSE                        # MIT License
└── README.md
```

The project depends on [goraft](https://github.com/lwwgo/goraft), a standalone Raft consensus protocol Go module published on GitHub. goraft supports:

- Business state machine injection (`StateMachine` interface: Apply/Snapshot/Restore)
- Generic log payloads (`CommandEntry.Data []byte`)
- RPC decoupling (`StartRPC` registered independently, supports custom RPC server integration)
- Configurable election timeout, randomization factor, and heartbeat interval
- WAL + Snapshot persistence with automatic snapshot triggering on log count threshold

## Design Highlights

1. **Separation of concerns**: MDS manages only metadata (directory tree, file location mapping), while DataNode manages only file content. This separation is the classic distributed file system architecture (HDFS NameNode/DataNode, Ceph MDS/OSD follow the same pattern).

2. **Raft high availability**: 3 MDS nodes form a Raft cluster, ensuring metadata consistency via goraft. Write operations go through Raft consensus replicated to a majority; read operations are served from local replicas. After leader failure, re-election happens automatically.

3. **Write consensus + local reads**: Write operations (Mkdir/CreateFile/CompleteFile/Delete/RegisterDataNode) are submitted as Raft log entries by the leader, replicated to a majority, then applied to the state machine. Read operations are served directly from each node's local state machine replica for low latency.

4. **Automatic follower redirect**: When a follower receives a write request, it returns `ErrNotLeader{Leader: addr}`. The client/data node automatically retries against the leader address, transparent to the user.

5. **Unified namespace**: Users see a single directory tree (e.g., `/docs/readme.md`), while the underlying files are distributed across multiple DataNodes with multi-replica copies. Clients learn file locations via MDS.

6. **Multi-replica storage**: Each file is replicated across multiple DataNodes (default 3) for fault tolerance. Client uploads to all replicas in parallel and reads try each replica in order.

7. **File lifecycle with pending/complete state**: Files start in `pending` status after metadata creation and transition to `complete` after successful data upload. Pending files return empty content on reads (like POSIX `creat()`), preventing ghost file errors.

8. **Orphan data garbage collection**: Background GC periodically compares DataNode-held files against metadata tree and cleans up orphans. Manual `gc` command available for on-demand cleanup.

9. **Ghost file protection**: Three-layer defense — client compensation deletion, empty file legality for pending state, and background orphan GC.

10. **Zero third-party dependencies**: Built entirely with the Go standard library (`net/rpc`, `encoding/json`, `log/slog`), depending only on goraft (which itself has zero external dependencies). Compile and run.

11. **WAL + Snapshot persistence**: Each MDS node persists Raft state via WAL + Snapshot. After restart, the state machine is restored from the Snapshot and incremental WAL logs are replayed.

## Testing

```bash
cd file-storage
make build    # or: go build ./...
make test     # or: go test -v -race -cover ./...
make lint     # or: golangci-lint run ./... / go vet ./...
```

End-to-end integration verification is primarily done via `start.sh`, covering: multi-MDS leader election, follower reads, automatic redirect of write operations, multi-replica upload/download, file lifecycle (pending→complete), and the full file upload/download/delete lifecycle.

## Future Enhancements

- **File chunking**: Split large files into chunks stored across different DataNodes for better parallel upload/download
- **DataNode heartbeat**: MDS periodically detects DataNode liveness and automatically removes failed nodes
- **Strongly consistent reads**: Current reads are served from local replicas; introduce ReadIndex for linearizable reads
- **HTTP gateway**: Add an HTTP layer in front of the client to provide a REST API
- **Quota management**: Per-directory/user storage quotas
- **More GC policies**: Time-based cleanup of pending files that never complete (stale pending files)
