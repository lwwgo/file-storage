# FStoreX 文件系统语义契约（Phase 1.5）

> 本文档冻结 FStoreX 在 Phase 1.5 阶段的文件系统语义。
> 它是后续 Batch 1～Batch 4 实现的唯一依据，FUSE（Phase 2）也将直接消费这里定义的语义。
>
> 修订自初版设计，吸收了四条关键修正：
> 1. `O_CREAT` 与 `O_TRUNC` 是独立语义，不合并为 `CreateFile(OverwriteIfExists)`
> 2. `FileID` 从一个字段升级为整个文件的身份模型，GC 同步改为按 FileID 识别
> 3. `WriteAt` 必须所有副本都成功才返回成功（Phase 1.5 不做 quorum）
> 4. `Truncate` / `WriteAt` 遵循「先数据后元数据」的提交顺序
> 5. Phase 1.5 不实现 Lease、chunk、并发写强一致性

---

## 一、核心模型：Path 是名字，FileID 是身份

POSIX 文件系统中，**路径只是指向 inode 的名字**，真正的对象身份是 inode。
FStoreX 用 `FileID`（UUID）承担 inode 的角色。

### 1.1 身份模型

```text
MDS 目录树                          DataNode 物理存储

/foo ──→ FileID: AAA              /data/AAA
                                     ↑
/bar ──→ FileID: BBB              /data/BBB
```

- **Path**：名字，可以变（rename），可以多个 path 指向同一 FileID（硬链接，Phase 1.5 不实现）
- **FileID**：对象身份，创建时生成，终身不变
- **DataNode 物理路径**：`/data/{FileID}`，永远不随 rename 改变

### 1.2 为什么必须这样

| 操作 | Path 模型（旧） | FileID 模型（新） |
|---|---|---|
| rename /foo → /bar | DataNode 物理路径要改，或被 GC 误删 | 只改 MDS 目录树，DataNode 无感知 |
| open 后 rename | fd 会指向新 path 的新文件 | fd 仍指向原 FileID（符合 POSIX inode 语义） |
| overwrite | 换 path 换对象 | FileID 不变，只改 size |
| GC | 按 path 匹配，rename 后误删 | 按 FileID 匹配，rename 安全 |

### 1.3 物理路径规则

- DataNode 上所有文件统一存为 `/data/{FileID}`
- `Replica.RemotePath = "/data/" + FileID`
- 旧数据（path 作为 RemotePath）在 Batch 1 完成后需清理 dev 环境

---

## 二、操作语义总表

| 操作 | 语义定义 | 写/读 | 走 Raft | 数据节点参与 |
|---|---|---|---|---|
| `Open` | 打开/创建/截断文件，返回 FileHandle | 写(创建/截断时) | 创建/截断时 | 截断时 |
| `ReadAt` | 从指定 offset 读随机字节 | 读 | 否 | 是（RangeRead） |
| `WriteAt` | 向指定 offset 写随机字节 | 写 | 是（更新 size） | 是（PartialWrite） |
| `Truncate` | 调整文件大小 | 写 | 是（更新 size） | 是（物理截断） |
| `Sync` | 刷盘确保持久化 | 写 | 否 | 是（fsync） |
| `Close` | 关闭句柄，释放资源 | - | pending→complete 时 | 否 |
| `Rename` | 重命名/移动（FileID 不变） | 写 | 是 | 否 |
| `Unlink` | 删除 path 引用（数据交 GC） | 写 | 是 | 否（GC 后续清） |
| `Mkdir` | 创建目录 | 写 | 是 | 否 |
| `Stat` | 查询元数据 | 读 | 否 | 否 |
| `ListDir` | 列目录 | 读 | 否 | 否 |

---

## 三、各操作详细语义

### 3.1 Open(path, flags)

**语义定义**
- 不存在 + `O_CREAT` → 创建新文件，生成新 FileID，状态 pending
- 不存在 + 无 `O_CREAT` → 返回 `ENOENT`
- 已存在 + `O_CREAT` → **忽略创建**，沿用原 FileID（即使带了 `O_CREAT|O_EXCL` 以外的创建意图）
- 已存在 + `O_TRUNC` → **打开原文件并截断为 0**，FileID 不变
- 已存在 + 无 `O_TRUNC` → 正常打开
- `O_EXCL` + 文件已存在 → 返回错误（排他创建）

**关键原则：`O_CREAT` 和 `O_TRUNC` 是两个独立语义，绝不合并为 `CreateFile(OverwriteIfExists)`。**

错误示范（初版设计）：
```text
open("/foo", O_CREAT|O_TRUNC)
  └─ CreateFile(OverwriteIfExists)  ← 错误：会换 FileID
     /foo: AAA → BBB
     已打开 /foo 的 fd 指向 AAA，路径却指向 BBB
```

正确实现：
```text
open("/foo", O_CREAT|O_TRUNC)
  ├─ lookup: /foo 已存在 (FileID=AAA)
  ├─ O_CREAT: 忽略
  ├─ O_TRUNC: Truncate(AAA, 0)  ← FileID 不变
  └─ 返回 FileHandle{FileID: AAA}
```

**三层调用链**
```text
Client.Open(path, flags)
  │
  ├─ MDS.Stat(path)                  // 检查存在性/类型
  │
  ├─ 文件不存在 && O_CREAT?
  │     └─ MDS.CreateFile(path)      // Raft，生成 FileID，选副本
  │
  ├─ 已存在 && O_TRUNC?
  │     └─ Client.truncateAllReplicas(fileID, 0)  // 先数据
  │     └─ MDS.UpdateSize(path, 0)                 // 后元数据
  │
  ├─ MDS.GetFileLocation(path)       // 缓存副本位置
  │
  └─ return FileHandle{FileID, path, flags, replicas, size}
```

**FileHandle 必须保存 FileID，不能只存 path。**
这样 open 后即使 path 被 rename/unlink，fd 仍能正确读写原对象。

#### CreateMode 的处置（重要）

现有 `types.CreateMode` 有两个值：

```go
CreateIfNotExist  // 不存在才创建，已存在则报错
OverwriteIfExists // 不存在则创建，已存在则覆盖
```

在新的 `O_CREAT`/`O_TRUNC` 解耦模型下，`OverwriteIfExists` 的语义已经不成立：
- Open 层已经把「已存在」拆成独立路径：`O_TRUNC` 走 `Truncate(fileID, 0)`，FileID 不变
- 不应再出现「CreateFile 时覆盖已存在文件并换 FileID」的路径

**Phase 1.5 处置方案（待确认）：**
- `CreateFile` RPC 内部强制等价于 `CreateIfNotExist`：已存在一律报错
- `OverwriteIfExists` 枚举值暂时保留（不删，避免破坏现有 CLI），但在 Phase 1.5 的 Open 链路中不再使用
- 后续可考虑废弃该枚举，或重定义为「原子替换」语义（先创建临时 FileID 再 rename 替换，类似 POSIX `O_TMPFILE`）

---

### 3.2 ReadAt(buf, offset)

**语义定义**
- 从 `offset` 读最多 `len(buf)` 字节
- 返回实际读到的字节数；到达 EOF 返回 `io.EOF`
- offset 超过文件大小 → 返回 0 + `io.EOF`
- 空文件（pending 或 size=0）→ 返回 0 + `io.EOF`

**三层调用链**
```text
FileHandle.ReadAt(buf, offset)
  │
  ├─ 选一个 replica（故障转移）
  │     └─ DataNode.RangeRead(/data/{FileID}, offset, len(buf))
  │           └─ os.Open → Seek(offset) → Read(buf)
  │
  ├─ 失败则 try 下一个 replica
  │
  └─ return (n, err)
```

**注意：这是 random-access I/O，不是 streaming RPC。**
net/rpc 一次请求带 offset+length，响应带 []byte，对 FUSE 的 128KB 读完全够用。

---

### 3.3 WriteAt(data, offset)

**语义定义**
- 向 `offset` 写入 `data`，支持 sparse file（offset 超过当前 size 时中间补 0）
- **所有副本都成功才返回成功**（Phase 1.5 不做 quorum、不做读修复）
- 任一副本失败 → 返回错误，**不更新 MDS size**
- 写入跨越当前 size 末尾 → 提交成功后更新 MDS size = max(旧 size, offset+len(data))

**时序约束：先数据，后元数据。**

```text
FileHandle.WriteAt(data, offset)
  │
  ├─ 并行写所有副本
  │     ├─ DN1: DataNode.PartialWrite(/data/{FileID}, offset, data)
  │     ├─ DN2: DataNode.PartialWrite(...)
  │     └─ DN3: DataNode.PartialWrite(...)
  │           └─ os.OpenFile(O_WRONLY|O_CREATE) → WriteAt(data, offset)
  │
  ├─ 全部成功？
  │     ├─ no  → return error（MDS size 不变）
  │     └─ yes ↓
  │
  ├─ 若 offset+len(data) > fh.size:
  │     └─ MDS.UpdateSize(path, offset+len(data))   // Raft
  │
  └─ fh.size = max(fh.size, offset+len(data))
     return (len(data), nil)
```

**为什么不能「至少一个成功」**
Phase 1.5 没有版本号、读修复、quorum 机制。若允许 2/3 成功，下次 ReadAt 选中失败副本会读到旧数据，同一 offset 不同副本返回不同内容，破坏文件系统基本一致性。宁可写失败也不返回脏数据。

---

### 3.4 Truncate(size)

**语义定义**
- 缩小：截断到 size，丢弃尾部数据
- 扩大：扩展到 size，新增部分补 0（sparse）
- size=0 等价于清空内容（O_TRUNC 走这里）

**时序约束：先数据，后元数据。**

```text
Truncate(path, newSize)
  │
  ├─ MDS.GetFileLocation(path)          // 拿 FileID + replicas
  │
  ├─ 并行截断所有副本
  │     ├─ DN1: DataNode.Truncate(/data/{FileID}, newSize)
  │     ├─ DN2: DataNode.Truncate(...)
  │     └─ DN3: DataNode.Truncate(...)
  │           └─ os.Truncate(path, newSize)
  │
  ├─ 全部成功？
  │     ├─ no  → return error（MDS size 不变）
  │     └─ yes ↓
  │
  └─ MDS.UpdateSize(path, newSize)      // Raft，后元数据
```

**为什么顺序不能反**
若先更新 MDS size=10MB，再截断 DataNode 失败，就会出现「元数据说 10MB，数据实际 100MB」的不一致。Phase 1.5 坚持先物理数据全部就绪，再让元数据对外可见。

---

### 3.5 Sync

**语义定义**
- 强制所有副本 fsync，确保持久化
- 对应 FUSE 的 `fsync` 回调

**调用链**
```text
FileHandle.Sync()
  │
  ├─ 并行所有副本: DataNode.Sync(/data/{FileID})
  │     └─ os.OpenFile → file.Sync() → close
  │
  └─ 全部成功才返回成功
```

---

### 3.6 Close

**语义定义**
- 释放 FileHandle 资源
- 若文件仍 pending（Open 后未写任何数据）→ 保持 pending 还是标记 complete？
  - Phase 1.5：标记 complete（与现有 CLI PutFile 行为一致）
- **不释放 Lease**（Phase 1.5 不做 Lease）

**调用链**
```text
FileHandle.Close()
  │
  ├─ 若 status == pending:
  │     └─ MDS.CompleteFile(path)
  │
  └─ 清空 FileHandle
```

---

### 3.7 Rename(src, dst)

**语义定义**
- 重命名或跨目录移动
- **FileID 不变**，DataNode 物理路径 `/data/{FileID}` 不变
- dst 已存在文件 → 覆盖（先摘 dst 旧 entry，旧 entry 的副本交 GC）
- dst 已存在目录 → 报错
- src 是目录 → 支持目录 rename（Phase 1.5 实现）
- 原子性：走单条 Raft 日志，rename 要么全成功要么全失败

**调用链**
```text
Client.Rename(src, dst)
  │
  └─ MDS.Rename(src, dst)              // Raft 单条日志
        └─ applyRename:
              ├─ 校验 src 存在、dst 父目录存在
              ├─ dst 已存在文件 → 摘下旧 entry（副本记录交 GC）
              ├─ src entry 从旧父目录 children 移除
              ├─ src entry 挂到 dst 父目录 children
              └─ FileID 不变，DataNode 无感知
```

---

### 3.8 Unlink(path)

**语义定义**
- 删除 path 引用
- 若是文件：MDS 摘 entry，物理数据交 GC 删除（不立即通知 DataNode）
- 若是目录且非空 → 报错（对应 `rmdir`）
- FileID 失去目录引用后，GC 按 FileID 识别为孤儿并清理

**调用链**
```text
Client.Delete(path)
  │
  ├─ MDS.Delete(path)                  // Raft
  │     └─ applyDelete: 摘 entry
  │
  └─ （Phase 1.5 简化）best-effort 通知 DataNode 删除
      后续 Batch 1 改由 GC 按 FileID 清理
```

---

## 四、GC 改造方案（FileID 身份模型的必然结果）

### 4.1 旧模型（path-based）

```text
MDS collectValidPaths()  → {/foo, /bar}
DataNode ListAllPaths()  → {/foo, /bar, /xxx}
比对 path → /xxx 是 orphan → 删除
```

**问题**：rename /foo → /bar 后，DataNode 上若仍有旧物理路径会被误删；物理路径必须跟随 path，和 rename 不变的目标冲突。

### 4.2 新模型（FileID-based，Batch 1 实施）

```text
MDS collectValidFileIDs()  → {AAA, BBB}
DataNode ListAllPaths()    → {/data/AAA, /data/BBB, /data/XXX}
比对 FileID → XXX 是 orphan → 删除 /data/XXX
```

**改造点**

| 组件 | 旧行为 | 新行为 |
|---|---|---|
| `entry` | 无 FileID | 新增 `fileID string` |
| `CreateFile` | RemotePath = path | RemotePath = "/data/" + UUID |
| `collectValidPaths` | 收集 path map | 收集 FileID map（`collectValidFileIDs`） |
| `garbageCollectNode` | `validPaths[nf.Path]` 比对 | 从 `nf.Path` 提取 `/data/` 后的 FileID 比对 |
| `NodeFile.Path` | 虚拟路径 `/foo` | 物理路径 `/data/AAA` |
| 覆盖写 | 旧副本成孤儿 | 同上，但 GC 按 FileID 识别 |

**GC 时序保护保留**：仍用 `gcSafetyWindow`（30s）保护 GC 扫描期间新建的文件，避免 TOCTOU 误删。

---

## 五、Phase 1.5 范围边界

### ✅ 本阶段实现

- FileID 身份模型 + GC 按 FileID 识别
- DataNode：RangeRead / PartialWrite / Truncate / Sync
- MDS：Rename / UpdateSize / Truncate 元数据
- Client：FileHandle（Open / ReadAt / WriteAt / Truncate / Sync / Close）+ Rename
- O_CREAT / O_EXCL / O_TRUNC 独立语义
- WriteAt/Truncate 全副本成功 + 先数据后元数据
- FUSE 契约测试（O_CREAT/O_EXCL/O_TRUNC/rename/unlink/read-after-write/random write/sparse write/fsync）

### ❌ 本阶段不做（明确排除）

| 能力 | 原因 |
|---|---|
| **Lease / 并发写强一致性** | 30s 不续约的 lease 会引入大量协议问题；Phase 1.5 诚实声明不承诺多客户端并发写同一文件的强 POSIX 一致性 |
| **Chunk 分块** | 接口已用 offset+length 预留演进空间，DataNode 内部仍是整文件 |
| **分布式并行写** | 单文件写所有副本，不拆分到不同 DataNode |
| **Read repair / quorum** | 写必须全成功，无不一致需修复 |
| **自动 lease 续约** | 不做 Lease |
| **硬链接 / 符号链接** | FileID 模型预留，后续实现 |
| **权限 / owner 完整语义** | 仅保留 mode 字段，不做完整 POSIX permission check |

---

## 六、Batch 实施顺序（修订版）

| Batch | 内容 | 改动文件 |
|---|---|---|
| **Batch 0** | 冻结文件系统契约（本文档） | docs/filesystem-semantics.md |
| **Batch 1a** | FileID 字段 + MDS apply/snapshot 改造（不动 GC/客户端） | types.go, mds.go, apply.go, state_machine.go |
| **Batch 1b** | GC 改为 FileID 模型 + DataNode/Client 适配 | gc.go, datanode.go, client.go |
| **Batch 2** | DataNode random I/O（RangeRead/PartialWrite/Truncate/Sync） | types.go, datanode.go |
| **Batch 3** | MDS Rename / UpdateSize | types.go, mds.go, apply.go, state_machine.go |
| **Batch 4** | Client FileHandle + 端到端测试 | client.go, cmd/fstorex（可选） |
| **Batch 5** | FUSE 接入（Phase 2） | 新增 fuse/ 包 |

> Batch 1 拆成 1a/1b 是为了控制单批改动面：1a 只改 MDS 元数据层（编译可验证），1b 再动 GC 和客户端，每批都能独立回退。

每个 Batch 动代码前都会单独列出改动清单，等用户确认 Y。

---

## 七、待确认项

1. FileID 用 UUID（google/uuid 还是自实现？当前 go.mod 无外部依赖，倾向自实现轻量 UUID 或用 crypto/rand 生成 16 字节 hex）
2. 物理路径 `/data/{FileID}` 是否加目录分桶（如 `/data/AA/AAA`）避免单目录文件过多？Phase 1.5 先不做
3. Unlink 后是否立即通知 DataNode 删除，还是完全交给 GC？倾向交给 GC（简化）
4. WriteAt 是否每次都 UpdateSize（走 Raft），还是累积到 Close/Sync 再更新？倾向每次都更新（保证元数据及时准确）
