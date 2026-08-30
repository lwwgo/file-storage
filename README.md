# File Storage — Distributed File Storage with Raft-based High Availability

A distributed file storage system providing a Unix-like hierarchical directory tree interface. The system consists of three independent binaries communicating via Go's standard `net/rpc` package, depending only on [goraft](https://github.com/lwwgo/goraft) (which itself has zero external dependencies). The metadata server uses the goraft consensus protocol to form a 3-node highly available cluster.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                            Client (CLI)                              │
│  put/get/ls/mkdir/rm/stat                                            │
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
│  │ Persisted via WAL +     │ │
│  │ Snapshot                │ │
│  └─────────────────────────┘ │
└───────────────────────────────┘
```

### Raft High Availability Mechanisms

| Mechanism | Description |
|---|---|
| **Leader Election** | 3 MDS nodes elect a leader via Raft; the rest become followers |
| **Log Replication** | Write operations (Mkdir/CreateFile/Delete/RegisterDataNode) are submitted by the leader to Raft, replicated to a majority, then applied to the state machine |
| **Local Reads** | Read operations (ListDir/Stat/GetFileLocation) are served directly from each node's local state machine replica |
| **Follower Redirect** | When a follower receives a write request, it returns the leader address; the client automatically retries against the leader |
| **Failover** | After a leader failure, remaining nodes automatically re-elect; no data loss |
| **WAL + Snapshot** | Each node persists state via WAL + Snapshot, automatically recovering after restart |

### Three Components

| Component | Binary | Default Port(s) | Responsibility |
|---|---|---|---|
| **Metadata Server** | `metadata-server` | 9001/9002/9003 | Manages the global directory tree, file→DataNode mapping, and DataNode registration; guarantees multi-node consistency via Raft |
| **Data Node** | `data-node` | 9101+ | Stores actual file content to local disk; registers with MDS on startup (auto-redirects to leader) |
| **Client** | `client` | — | CLI tool; metadata operations go through MDS, data operations go MDS→DataNode; writes auto-redirect from follower to leader |

### Data Flow

```
put:  Client ──RPC──→ MDS (leader creates metadata, selects DataNode)
                              │
                              ▼
                    returns DataNode address
                              │
                              ▼
      Client ──RPC──→ DataNode (stores content)
              ↑ replicated to majority via Raft, then applied

get:  Client ──RPC──→ MDS (any node looks up file location)
                              │
                              ▼
                    returns DataNode address
                              │
                              ▼
      Client ──RPC──→ DataNode (reads content)

mkdir/ls/stat/rm:  Client ──RPC──→ MDS (writes go through Raft consensus, reads served locally)
```

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

# Download a file (distributed path → local path)
./bin/client -mds=localhost:9001 get /docs/remote.txt ./downloaded.txt

# List a directory
./bin/client -mds=localhost:9001 ls /docs

# Show file/directory info
./bin/client -mds=localhost:9001 stat /docs/remote.txt

# Delete a file or directory
./bin/client -mds=localhost:9001 rm /docs/remote.txt
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
│   ├── metadata/mds.go            # MDS core: implements goraft StateMachine
│   ├── datanode/datanode.go       # DataNode core: content storage + RPC
│   └── client/client.go           # Client logic: RPC wrapper + follower redirect
├── .gitignore                     # Ignores build artifacts and runtime data
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

3. **Write consensus + local reads**: Write operations (Mkdir/CreateFile/Delete/RegisterDataNode) are submitted as Raft log entries by the leader, replicated to a majority, then applied to the state machine. Read operations are served directly from each node's local state machine replica for low latency.

4. **Automatic follower redirect**: When a follower receives a write request, it returns `ErrNotLeader{Leader: addr}`. The client/data node automatically retries against the leader address, transparent to the user.

5. **Unified namespace**: Users see a single directory tree (e.g., `/docs/readme.md`), while the underlying files are distributed across multiple DataNodes. Clients learn file locations via MDS.

6. **DataNode selection strategy**: Hash modulo based on file path, distributing files evenly across DataNodes (demo version; production would also consider capacity, load, rack awareness, etc.).

7. **Zero third-party dependencies**: Built entirely with the Go standard library (`net/rpc`, `encoding/json`, `log/slog`), depending only on goraft (which itself has zero external dependencies). Compile and run.

8. **WAL + Snapshot persistence**: Each MDS node persists Raft state via WAL + Snapshot. After restart, the state machine is restored from the Snapshot and incremental WAL logs are replayed.

## Testing

```bash
cd file-storage
go build ./...
go vet ./...
```

End-to-end integration verification is primarily done via `start.sh`, covering: multi-MDS leader election, follower reads, automatic redirect of write operations, and the full file upload/download/delete lifecycle.

## Future Enhancements

- **File chunking + replication**: Split large files into chunks stored across different DataNodes, with 3 replicas per chunk (similar to HDFS)
- **DataNode heartbeat**: MDS periodically detects DataNode liveness and automatically removes failed nodes
- **Strongly consistent reads**: Current reads are served from local replicas; introduce ReadIndex for linearizable reads
- **HTTP gateway**: Add an HTTP layer in front of the client to provide a REST API
