# File Storage — 分布式文件存储（Raft 高可用版）

一个分布式文件存储系统，提供类 Unix 文件系统的层次化目录树接口。系统由三个独立的二进制组件组成，通过 Go 标准库 `net/rpc` 通信，零外部依赖。元数据服务器采用 [goraft](https://github.com/lwwgo/goraft) 一致性协议实现 3 节点高可用集群。

## 架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                            Client (CLI)                              │
│  put/get/ls/mkdir/rm/stat                                            │
│  写操作自动重定向到 leader，读操作任意节点均可                        │
└──────────────┬──────────────────────────┬───────────────────────────┘
               │                          │
        元数据 RPC                   数据 RPC
               │                          │
               ▼                          ▼
┌───────────────────────────────┐       ┌───────────────────────┐
│   Metadata Server 集群        │       │     Data Node         │
│   (3 节点 Raft, 1 主 2 从)    │       │   (多个, 水平扩展)     │
│                               │       │                       │
│  ┌───────┐  ┌───────┐  ┌────┐│       │  ┌─────────────────┐  │
│  │ MDS1  │  │ MDS2  │  │MDS3││       │  │  文件内容存储    │  │
│  │Leader │  │Follow │  │Fol ││       │  │  /dn1/docs/...  │  │
│  └───┬───┘  └───┬───┘  └─┬──┘│       │  │  /dn2/images/.. │  │
│      │          │        │   │       │  └─────────────────┘  │
│      └──── Raft 日志复制 ┘   │       │                       │
│                               │       │  仅存储实际内容        │
│  ┌─────────────────────────┐ │       │  不管理目录树           │
│  │  目录树 (Raft 状态机)     │ │       └───────────────────────┘
│  │  /docs/readme            │ │
│  │  /images/logo            │ │
│  └─────────────────────────┘ │
│  ┌─────────────────────────┐ │
│  │ 文件→DataNode映射        │ │
│  │ 通过 WAL + Snapshot 持久化│ │
│  └─────────────────────────┘ │
└───────────────────────────────┘
```

### Raft 高可用机制

| 机制 | 说明 |
|---|---|
| **Leader 选举** | 3 个 MDS 节点通过 Raft 选举产生 leader，其余为 follower |
| **日志复制** | 写操作（Mkdir/CreateFile/Delete/RegisterDataNode）由 leader 提交到 Raft，复制到多数派后应用到状态机 |
| **读本地** | 读操作（ListDir/Stat/GetFileLocation）所有节点直接读本地状态机副本 |
| **Follower 重定向** | follower 收到写请求时返回 leader 地址，客户端自动重定向重试 |
| **故障切换** | leader 故障后，剩余节点自动重新选举，数据不丢失 |
| **WAL + Snapshot** | 每个节点通过 WAL 日志 + Snapshot 持久化状态，重启后自动恢复 |

### 三个组件

| 组件 | 二进制 | 端口(默认) | 职责 |
|---|---|---|---|
| **Metadata Server** | `metadata-server` | 9001/9002/9003 | 管理全局目录树、文件→DataNode 映射、DataNode 注册；通过 Raft 保证多节点一致性 |
| **Data Node** | `data-node` | 9101+ | 存储实际文件内容到本地磁盘；启动时向 MDS 注册（自动重定向到 leader） |
| **Client** | `client` | — | CLI 工具，元数据操作走 MDS，数据操作走 MDS→DataNode；写操作自动 follower→leader 重定向 |

### 数据流

```
put:  Client ──RPC──→ MDS(leader 创建元数据, 选DataNode) ──返回地址──→ Client ──RPC──→ DataNode(存内容)
                        ↑ 通过 Raft 复制到多数派后应用

get:  Client ──RPC──→ MDS(任意节点查文件位置)           ──返回地址──→ Client ──RPC──→ DataNode(读内容)

mkdir/ls/stat/rm:  Client ──RPC──→ MDS(写走 Raft 共识, 读读本地)
```

## 快速开始

### 一键演示

```bash
cd file-storage
chmod +x start.sh
./start.sh

# 停止所有节点
./start.sh clean
```

脚本会自动编译并启动 **3 个 MDS（Raft 集群）+ 2 个 DataNode**，等待选主完成后演示完整的 put/get/ls/mkdir/rm 流程，包括从 follower 节点读数据的验证。

### 手动编译三个二进制

```bash
cd file-storage
mkdir -p bin
go build -o bin/metadata-server ./cmd/metadata-server
go build -o bin/data-node ./cmd/data-node
go build -o bin/client ./cmd/client
```

### 手动启动集群

```bash
# 1. 启动 3 个 MDS 节点（Raft 集群）
./bin/metadata-server -id=localhost:9001 -peers=localhost:9002,localhost:9003 -wal-dir=/tmp/mds1/wal -snap-dir=/tmp/mds1/snap
./bin/metadata-server -id=localhost:9002 -peers=localhost:9001,localhost:9003 -wal-dir=/tmp/mds2/wal -snap-dir=/tmp/mds2/snap
./bin/metadata-server -id=localhost:9003 -peers=localhost:9001,localhost:9002 -wal-dir=/tmp/mds3/wal -snap-dir=/tmp/mds3/snap

# 2. 启动数据节点（连任意 MDS 都行，自动重定向到 leader）
./bin/data-node -addr=:9101 -mds=localhost:9001 -data-dir=/tmp/dn1
./bin/data-node -addr=:9102 -mds=localhost:9001 -data-dir=/tmp/dn2

# 3. 使用客户端（连任意 MDS 都行）
./bin/client -mds=localhost:9001 nodes
```

### 客户端命令

```bash
# 查看已注册的数据节点
./bin/client -mds=localhost:9001 nodes

# 创建目录
./bin/client -mds=localhost:9001 mkdir /docs

# 上传文件（本地路径 → 分布式路径）
./bin/client -mds=localhost:9001 put ./local.txt /docs/remote.txt

# 下载文件（分布式路径 → 本地路径）
./bin/client -mds=localhost:9001 get /docs/remote.txt ./downloaded.txt

# 列出目录
./bin/client -mds=localhost:9001 ls /docs

# 查看文件/目录信息
./bin/client -mds=localhost:9001 stat /docs/remote.txt

# 删除文件或目录
./bin/client -mds=localhost:9001 rm /docs/remote.txt
```

## 项目结构

```
file-storage/
├── cmd/                           # 三个独立入口
│   ├── metadata-server/main.go    # 元数据服务器二进制（Raft 节点）
│   ├── data-node/main.go          # 数据节点二进制
│   └── client/main.go             # 客户端 CLI 二进制
├── internal/
│   ├── types/types.go             # 共享类型 + RPC 接口定义
│   ├── metadata/mds.go            # MDS 核心：实现 goraft StateMachine 接口
│   ├── datanode/datanode.go       # DataNode 核心：内容存储 + RPC
│   └── client/client.go           # 客户端逻辑：封装 RPC 调用 + follower 重定向
├── go.mod                         # replace github.com/lwwgo/goraft => ../goraft
├── start.sh                       # 一键演示脚本（3 MDS + 2 DataNode）
└── README.md
```

依赖的 [goraft](https://github.com/lwwgo/goraft) 项目位于同级目录 `/Users/liubing/codes/gocode/goraft/`，通过 `go.mod` 的 `replace` 指令引用。goraft 已改造为可被 import 的 Go package，支持：

- 业务状态机注入（`StateMachine` 接口：Apply/Snapshot/Restore）
- 日志载荷泛化（`CommandEtnry.Data []byte`）
- RPC 解耦（`StartRPC` 独立注册，避免全局冲突）
- 可配置选举超时和随机因子

## 设计要点

1. **职责分离**：MDS 只管元数据（目录树、文件位置映射），DataNode 只管文件内容。这种分离是分布式文件系统的经典架构（HDFS NameNode/DataNode、Ceph MDS/OSD 同理）。

2. **Raft 高可用**：3 个 MDS 节点组成 Raft 集群，通过 goraft 保证元数据一致性。写操作走 Raft 共识复制到多数派，读操作读本地副本。leader 故障后自动重新选举。

3. **写共识 + 读本地**：写操作（Mkdir/CreateFile/Delete/RegisterDataNode）由 leader 提交 Raft 日志，复制到多数派后 Apply 到状态机；读操作所有节点直接读本地状态机副本，低延迟。

4. **Follower 自动重定向**：follower 收到写请求时返回 `ErrNotLeader{Leader: addr}`，客户端/数据节点自动用 leader 地址重试，对用户透明。

5. **统一命名空间**：用户看到的是单一目录树（如 `/docs/readme.md`），底层文件分散在多个 DataNode 上，客户端通过 MDS 获知位置。

6. **DataNode 选择策略**：基于文件路径的哈希取模，均匀分布到各 DataNode（演示版，生产环境还需考虑容量、负载、机架感知等）。

7. **零外部依赖**：全部使用 Go 标准库（`net/rpc`、`encoding/json`、`log/slog`），编译即用。

8. **WAL + Snapshot 持久化**：每个 MDS 节点通过 WAL 日志 + Snapshot 持久化 Raft 状态，重启后从 Snapshot 恢复状态机并重放 WAL 增量日志。

## 运行测试

```bash
cd file-storage
go build ./...
go vet ./...
```

主要通过 `start.sh` 进行端到端集成验证，覆盖：多 MDS 选主、follower 读、写操作自动重定向、文件上传下载删除全流程。

## 后续可扩展方向

- **文件分块 + 副本**：大文件切块存储到不同 DataNode，每块 3 副本（类似 HDFS）
- **DataNode 心跳**：MDS 定期检测 DataNode 存活，自动摘除故障节点
- **强一致读**：当前读操作读本地副本，可引入 ReadIndex 保证线性一致读
- **HTTP 网关**：在 Client 前加 HTTP 层，提供 REST API
- **goraft 快照触发**：WAL 日志数超过阈值时自动触发 Snapshot 压缩
