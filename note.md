# Raft KV 项目面试复习笔记

---

## Q1：Put / Get / Delete 这三个 RPC 方法在哪个线程跑？

**结论：运行在 RpcServer 的 worker 线程池（ThreadPool）里。**

调用链路：

```
网络数据到达
  └─ muduo IO 线程 → RpcServer::onMessage()
        └─ pool_->Enqueue(processRequest)     ← 卸载到线程池
              └─ processRequest()             [worker 线程]
                    └─ service->CallMethod()  ← protobuf 分发
                          └─ Put/Get/Delete() [worker 线程，同步调用]
```

关键代码（`rpc_server.cpp`）：
```cpp
// Move heavy work off the IO thread.
pool_->Enqueue([this, conn, header, body]() mutable {
    processRequest(conn, std::move(header), std::move(body));
});
```

`processRequest` 在 worker 线程里直接同步调用 `service->CallMethod()`，Put/Get/Delete 不需要二次入队，调用时已经在 worker 线程的栈上了。

**重要后果：**
- `Get`：直接读 KvStore（加 mutex），很快返回。
- `Put` / `Delete`：`Propose()` 后**阻塞等待** `future.wait_for(5s)`，等 Raft commit。每个 worker 线程在等待期间被占用，并发写请求数受 `worker_threads`（默认 4）限制。

---

## Q2：Put/Get/Delete 是什么时候被放进线程池的？

不是 Put/Get/Delete 本身被入队，而是 **`processRequest` 被 enqueue**，然后 `processRequest` 在 worker 线程里同步调用了它们。

入队点在 `onMessage`（IO 线程）：
```cpp
pool_->Enqueue([...] { processRequest(...); });  // ← 这里入队
```

`processRequest` 最后一行：
```cpp
service->CallMethod(methodDesc, controller, rawReq, rawResp, done);
// protobuf 根据 methodDesc 找到方法名，同步调用 Put/Get/Delete
```

---

## Q3：KvServerService 是什么时候注册到 RpcServer 上的？

路径：`main()` → `cfg.extra_services.push_back(&service)` → `raft_node.Start()` → `rpc_server_.RegisterService(service)`

```cpp
// kv_server_main.cpp
cfg.extra_services.push_back(&service);
raft_node.Start();

// raft_node.cpp Start() 内部
rpc_server_.RegisterService(this);  // 先注册 Raft 自身
for (auto* service : cfg_.extra_services) {
    rpc_server_.RegisterService(service);  // 再注册 extra_services
}
```

`RegisterService` 把 service 指针存入 `service_map_`（以 service 全名为 key 的 unordered_map），每次 `processRequest` 根据请求里的 `service_name` 字段查表找到对应的 service，再 `CallMethod`。

---

## Q4：处理过程中 leader 突然变成 follower 会发生什么？

### 写请求（Put / Delete）

按请求走到哪一步分情况：

**1. 还没 `Propose`（刚做完 `IsLeader()` 检查）**
- 若已降级：`IsLeader()` 读原子变量，直接返回 `not leader` + `leaderId`。
- 竞态情况：`IsLeader()` 为 true 但进入 `Propose` 前已降级，`Propose` 在主线程里再次检查 `state_`，返回 false，同样走 `not leader`，命令不会写入日志。

**2. 正在 `Propose`（worker 阻塞在 `fut.get()`）**
- Propose 内部 lambda 在主线程执行时若已变 follower → 返回 false → 客户端收到 `not leader`，**日志里没有这条命令**。

**3. `Propose` 已成功，正在 `future.wait_for(5s)`（最常见）**

命令已在本机日志里，有三种结果：

| 情况 | 对 RPC 的表现 | 对数据的影响 |
|---|---|---|
| 多数派已复制，新 leader 提交并广播 commit | 超时前 future 就绪，`success=true` | 命令已生效 |
| 未复制到多数派 | 5 秒后 `commit timeout` | 命令未生效 |
| 长期无 commit | 同上 `commit timeout` | 未生效 |

超时后客户端用**同一个 `requestId`** 重试（幂等），服务端去重保证不会重复执行。

### 读请求（Get）

Get 不走 Raft 日志，直接读本地 KvStore，**存在陈旧读风险**（项目未实现 ReadIndex/lease read）。

---

## Q5：Promise 和 Future 的关系是什么？

- **`std::promise<T>`**：结果的"生产者/交付者"，用 `set_value()` 填入结果。
- **`std::future<T>`**：结果的"消费者/收件人"，用 `wait_for()` / `get()` 等待并取出结果。
- 两者关系：`future` 必须由 `promise.get_future()` 生成，同一对 promise/future 只能用一次。

**在本项目中的用法（Put/Delete 写入路径）：**

```
CommitWaitRegistry::FutureFor(index):
  创建 promise，把 future 返回给 RPC worker 线程（等待方）
  把 promise 存入 waiters_[index]（等将来被 set_value）

KvApplySink::OnCommitted（Raft 主线程，commit 后调用）:
  store_->Apply(command)   ← 真正写入状态机
  wait_registry_->Notify(index, result)
    → 找到 waiters_[index] 里的 promise，调用 set_value(result)

RPC worker 线程:
  future.wait_for(5s) → ready → future.get() → 返回客户端
```

**为什么既有 `waiters_` 又有 `completed_`**：防止 commit 先于 `FutureFor` 调用到达（先 commit 后等待的竞态），确保通知不丢失。

---

## Q6：每个节点需要持久化哪些东西？在什么时候持久化？

### 1. HardState（currentTerm + votedFor）

**持久化时机：**
- 投票给某个候选人时（`voted_for_` 改变）
- 任期变大转为 follower 时（`term_changed = true`）
- 自己发起选举变 candidate 时（`current_term_++`，`voted_for_=self`）

文件：`data_dir/hard.state`（文本格式：`term voted_for`）

### 2. Raft Log（日志条目）

**追加时机：**
- Leader 本地 `Propose` 追加日志时，立刻持久化
- Follower 收到 `AppendEntries` 并接受新日志时，持久化新 entries

**截断时机：**
- `TruncateSuffix`：follower 日志与 leader 冲突时回滚
- `TruncatePrefix`：snapshot 生效后压缩前缀

文件：`data_dir/log/<index>.entry`（每条日志一个文件）

### 3. Snapshot（快照）

**写盘时机：**
- `KvApplySink` apply 达到阈值（`snapshot_threshold`），触发 `TakeSnapshotAsync`
- Follower 收到 `InstallSnapshot` RPC 时

内容：`last_included_index` + `last_included_term` + KvStore 的序列化字节（所有 kv 对 + 去重表）

文件：`data_dir/snapshot`（二进制）

### 4. Cluster Config（成员配置，本项目额外持久化）

**写盘时机：** 配置变更日志 entry **被 commit 并 apply 后**（不是 propose 时）

文件：`data_dir/cluster.config`（文本格式：每行 `id ip port`）

### 启动时读盘顺序

```
构造函数:
  ReadHardState → 恢复 term/votedFor
  LoadConfig    → 恢复成员列表（覆盖启动参数）
  LoadPersistentLog:
    LoadSnapshot → 恢复状态机，commit_index/last_applied 推进到 snapshot_index
    逐条 EntryAt  → 重建 log_cache（snapshot 之后的部分）
```

---

## Q7：如果 `votedFor` 不持久化会怎样？

**后果：同一个 term 可能选出两个 leader，打破 Raft 最核心的安全保证。**

经典场景：
```
1. A 在 term=5 给 B 投票（B 已得到多数票成为 leader）
2. A 崩溃重启，votedFor 没持久化，还原为 kNoNode
3. C 也在 term=5 发起选举，向 A 要票
4. A 以为自己没投过，满足条件，把票投给 C
5. C 也得到多数票成为 term=5 的 leader
结果：B 和 C 同时是 term=5 的 leader → 数据不一致
```

**关键设计：先持久化，再回复**。代码里的顺序是：
```cpp
voted_for_ = req->candidateid();
PersistHardState();    // ← 先写盘
// done->Run() 之后才发 voteGranted=true 给候选人
```

如果反过来（先回复、再写盘崩溃），候选人已收到票但本节点重启后可以再投一次，等价于同一 term 投出两票。

---

## Q8：面试官问"为什么要做内存池"怎么回答？

**先说一个实情：`Arena` 目前在 skip list 里并没有被实际使用**，`NewNode` 还是直接用 `new Node<K,V>`。

推荐回答思路：

> "跳表在插入时会频繁 `new` 一个个小的 Node 对象，这种场景是教科书级的内存池适用场景——节点多、对象小、生命周期一致。我参考了 LevelDB 的 Arena 实现，提前把这个组件做进来，算是预防性的设计，而不是性能问题倒逼的结果。
>
> Arena 是一个 bump allocator，一次向系统申请一大块内存（4096 字节/块），每次分配只是指针往前移几字节，几乎是 O(1) 的一条加法指令，没有锁、没有 metadata 开销。跳表销毁时 Arena 一次性释放所有 block，不需要逐个 delete。
>
> 不过目前 SkipList 的 NewNode 还是直接 `new` 的，Arena 还没接进去，完整做法是把 Arena 作为 SkipList 的成员，在 NewNode 里用 `arena_.AllocateAligned(sizeof(Node))` 加 placement new。"

**可能被追问的点：**

| 追问 | 回答要点 |
|---|---|
| Arena 线程安全吗？ | 不是，注释里写了，SkipList 有 `shared_mutex` 保护，Arena 被 SkipList 独占 |
| block 大小为什么选 4096？ | 对应操作系统页大小，减少 TLB miss |
| 超过 1/4 block 为什么单独分配？ | 防止一个大对象浪费整个 block 剩余空间（内部碎片） |
| Arena 能释放单个对象吗？ | 不能，只支持批量销毁，适合"同生共死"场景 |

---

## Q9：面试官问"遇到了什么性能问题才做内存池"怎么回答？

**不要编造性能问题**，被追问 profile 数据、复现路径、优化前后对比，一问就穿帮。

推荐回答：

> "我没有遇到具体的性能问题触发这个优化，这个项目是学习和实践系统设计的，数据量还没到需要 profile 的程度。但我在实现跳表时意识到这是教科书级的内存池适用场景，所以参考 LevelDB 提前把组件做进来。
>
> 如果真要上生产，我会先用 valgrind massif 或 perf 采样确认 malloc 是不是热点，再决定是否接入。"

**如果追问"那你知道 malloc 慢在哪吗"：**
- **锁竞争**：glibc ptmalloc 多线程下有 arena 锁，高并发时 malloc/free 互相争抢
- **碎片化**：大量小对象分散在堆里，CPU prefetch 失效，cache miss 增加
- **metadata 开销**：每个 `new` 对象前有 8~16 字节的 chunk header，小对象比例高时浪费可观
- **系统调用**：堆不够时需要 `brk`/`mmap` 向内核申请，有上下文切换开销

---

## Q10：对客户端请求做幂等性检查是什么意思？

**幂等**：同一个操作执行一次和多次，结果完全相同。

**为什么需要：** 客户端收到 `commit timeout` 后会用同一个 `requestId` 重试。如果没有幂等检查，同一条命令可能被执行两次。

**本项目的实现：**

`KvStore` 维护一张表：
```cpp
std::unordered_map<std::string, int64_t> last_request_id_;
// key: clientId, value: 最近一次成功执行的 requestId
```

每次 apply 写命令前先查表：
```cpp
bool IsDuplicateLocked(const std::string& client_id, int64_t request_id) const {
    auto it = last_request_id_.find(client_id);
    return it != last_request_id_.end() && request_id <= it->second;
}
```

如果是重复请求，直接返回 `{success=true, duplicate=true}`，跳过实际写入。

**时序示例：**
```
第一次: Put clientId="c1" requestId=42
  → IsDuplicate? 没记录 → false → 执行写入 → Remember("c1", 42)

第二次（重试）: Put clientId="c1" requestId=42
  → IsDuplicate? last["c1"]=42，42<=42 → true → 直接返回成功，不再写入
```

**一句话总结：** 用 `(clientId, requestId)` 作为唯一标识，让"至少执行一次"升级为"恰好执行一次"的语义。

---

## Q11：如果 `currentTerm` 不持久化会怎样？

**最严重的问题：绕过 `votedFor` 的保护，让它即使持久化了也失效。**

原因：`BecomeFollower` 里有这段逻辑：
```cpp
bool term_changed = (new_term > current_term_);
if (term_changed) {
    current_term_ = new_term;
    voted_for_    = kNoNode;  // ← term 变大时清掉 votedFor！
}
```

如果 `currentTerm` 不持久化，重启后 term=0，任何大于 0 的 term 都会触发 `term_changed=true`，把之前持久化的 `votedFor` 清掉，从而允许再次投票 → 同一 term 两个 leader。

**场景二：接受本该被拒绝的过期 leader**
```
集群 term=10，A 崩溃重启 term 归零
一个被隔离的旧 leader（term=3）发来 AppendEntries
A.term=0 < 3，A 接受了这个过期 leader，开始跟着它走
→ 覆盖掉 term=4~10 期间提交的日志
```

**场景三：发起无意义选举**
```
A 重启 term=0，选举超时后发起 RequestVote(term=1)
其他节点 term=10，全部拒绝（term 1 < 10）
不影响数据，但扰乱集群心跳节奏
```

**一句话总结：** `currentTerm` 是节点的"逻辑时钟"，不持久化会让 `votedFor` 的保护失效。两者必须捆绑一起持久化，这是 Raft 论文的明确要求。

---

## Q12：面试官问"怎么通过测试验证协议正确性"怎么回答？

### 第一层：组件级 smoke test

**`kv_bridge_smoke`（单节点进程内）：**
- 节点未 start 时 Put 应返回 `not leader`
- 单节点启动后 100ms 内自选为 leader
- leader 上 Put → Get 能读到正确值
- 验证 `FileRaftStorage` 确实写了日志（`last_index`/`last_term` 不为零）

**`raft_node_smoke`（纯 Raft 层，3 节点进程内，NullStorage）：**
- 5 秒内选出唯一 leader
- 所有 follower 的 term 不低于 leader 的 term
- leader 上 Propose 成功，follower 上 Propose 被拒绝
- 3 条命令在 5 秒内传播到所有节点的 apply sink

### 第二层：集成测试（多进程，真实网络）

**`raft_cluster_test`（fork + exec 起 3 个真实进程）：**
- 25 秒内集群能选出 leader
- 批量 Put 10 条数据，Get 逐条比对正确值
- 覆盖写（overwrite + read-after-write）
- Delete 后 Get 应该失败

### 第三层：坦诚说局限

> "目前测试的覆盖还不够，有几类情况没有测：
> - **leader 宕机**：kill 掉 leader 进程，验证剩余节点能重选并继续服务
> - **持久化恢复**：节点崩溃重启后能否正确恢复 term、日志、snapshot
> - **幂等性边界**：客户端超时重试同一 requestId，验证不重复执行
> - **并发客户端**：多客户端同时写，验证最终数据一致
>
> 业界更完整的做法是用 Jepsen 做混沌测试，或用 fault injection 模拟磁盘失败、网络丢包。我现在的测试主要验证 happy path 和基本的错误拒绝，还没有系统地测 failure case。"

---

## Q13：面试官问"是基于什么契机开始做这个项目/目的是什么"怎么回答？

**核心原则：真实有说服力，能自然过渡到技术深问。**

### 推荐回答结构（30~60 秒）

**契机**：读 Raft 论文时，觉得只靠阅读理解不够扎实，想通过动手实现来真正掌握每一个细节。

**目标**：想把分布式系统的几个核心模块——共识（Raft）、RPC、存储引擎——在同一个项目里完整串起来，而不是只写一个孤立的算法实现。

**收获**：通过实现才遇到论文里没有明说的工程细节，例如 commit index 推进的边界条件、votedFor 和 currentTerm 必须捆绑持久化的原因。

### 三个可选角度

**角度一：从学习局限出发（最自然）**
> "我在学习分布式系统理论时读了 Raft 论文，理解了算法思路，但总觉得「看懂」和「真正掌握」之间差了一层。论文里很多细节只有真正动手实现才会遇到，所以我决定从零写一遍，把 Raft、RPC、存储引擎全部自己实现。"

**角度二：从工程挑战出发（强调动手能力）**
> "我想练习写一个有工程复杂度的 C++ 项目。分布式 KV 存储这个场景很典型——既涉及并发（跳表的读写锁、协程），又涉及网络通信（RPC 框架），还涉及一致性保证（Raft），几乎把系统编程的核心难点都覆盖了。"

**角度三：对标业界系统（展示视野）**
> "我对 etcd、TiKV 这类系统很感兴趣，它们的底层都是 Raft + 自研存储引擎。我想通过自己实现一个麻雀虽小五脏俱全的版本，理解这类系统的设计取舍——比如为什么用跳表做内存索引、RPC 层怎么和 Raft 状态机解耦。"

---

## Q14：Raft 本质是为了解决什么问题？

**一句话：在一组机器中，让所有机器对「同一份数据的变更历史」达成一致，并且在部分机器故障时系统仍然可用。**

### 根本矛盾

- 需要多台机器来避免单点故障（高可用）
- 但多台机器就会产生数据不一致

Raft 解决的是**分布式共识**，即：

> **让集群中所有节点，以相同的顺序执行相同的操作序列。**

只要操作顺序一致，每台机器状态就一致——这是**复制状态机**模型（Replicated State Machine）。

```
客户端写入
    ↓
[Raft 层]  →  保证所有节点日志顺序完全一致
    ↓
[应用层]   →  每个节点按日志顺序执行命令 → 状态一致
```

### Raft 解决的五个子问题

| 子问题 | Raft 的解法 |
|--------|-------------|
| 谁来决定写入顺序？ | 选出唯一的 Leader，所有写请求只走 Leader |
| Leader 挂了怎么办？ | 选举机制，超时后 Follower 发起投票 |
| 日志怎么算「已提交」？ | 超过半数节点（Quorum）确认后才提交 |
| 新节点/恢复节点怎么追上？ | Leader 向落后节点回放日志或发快照 |
| 脑裂（网络分区）怎么处理？ | 少数派无法凑够 Quorum，无法提交新日志 |

### 与 Paxos 的区别

解决的问题完全相同，但 Raft 的设计目标是**可理解性**——把共识问题拆解成 Leader 选举、日志复制、安全性三个独立子问题，每个子问题有清晰规则，工程实现更友好。

### 一句话总结

> Raft 的本质是：用「强 Leader」模型把分布式写入串行化，从而把「多机一致」问题转化为「日志复制」问题，同时通过 Quorum 机制保证少数故障下系统仍可用。

---

## Q15：如何从总到分向面试官介绍项目架构？

### 第一层：一句话定位

> "这是一个用 C++ 从零实现的分布式 KV 存储系统，核心目标是通过 Raft 共识算法保证多节点数据强一致性，同时具备少数节点故障下的高可用能力。"

### 第二层：整体分层架构

```
┌─────────────────────────────────────┐
│           Client 客户端              │  ← 发起 Put / Get / Delete
└──────────────┬──────────────────────┘
               │  RPC (Protobuf over TCP)
┌──────────────▼──────────────────────┐
│        KvServerService              │  ← 接收请求、幂等校验、重定向非 Leader
│        CommitWaitRegistry           │  ← 阻塞等待日志提交后返回结果
└──────────────┬──────────────────────┘
               │ Propose(command)
┌──────────────▼──────────────────────┐
│           RaftNode                  │  ← 共识核心：选举、日志复制、快照
│   (单线程事件循环 + 协程 fanout RPC)  │
└──────┬──────────────┬───────────────┘
       │ 持久化         │ OnCommitted 回调
┌──────▼──────┐  ┌─────▼──────────────┐
│FileRaftStorage│  │  KvRaftApplySink   │  ← 将已提交日志应用到状态机
│(term/log/snap)│  └─────┬─────────────┘
└─────────────┘        │
                  ┌────▼────────────────┐
                  │      KvStore        │  ← 状态机：SkipList + LRU Cache
                  └─────────────────────┘
```

### 第三层：各模块一句话说清楚

**① RPC 框架**
> "网络层基于 muduo（epoll 事件驱动）+ Protobuf 序列化，自己实现了编解码器和 Channel。Raft 节点间的 RequestVote、AppendEntries 等调用都走这套 RPC，KV 客户端访问服务端也走同一套框架。"

**② Raft 核心（`RaftNode`）**
> "Raft 节点是一个单线程事件循环：所有状态变更都在主线程执行，避免锁竞争。RPC Server 的 worker 线程收到请求后，通过任务队列 Post 到主线程处理。向多个 Peer 发 RPC 时，用协程 fanout——每个 Peer 一个协程，co_await RPC 结果，回调自动 resume 回主线程。"

**③ KV 服务层（`KvServerService` + `CommitWaitRegistry`）**
> "客户端写请求进来后，先做幂等校验，然后调 `RaftNode::Propose()` 把命令写入 Raft 日志，同时在 `CommitWaitRegistry` 里注册一个 future 阻塞等待。Raft 将日志提交并 apply 后，apply 回调通知 registry，future 拿到结果，再向客户端返回响应。"

**④ 存储引擎（`KvStore`）**
> "状态机底层用跳表存储 KV 数据，查找/插入 O(log n)，并发读用 shared_mutex 支持多读单写。跳表上层加 LRU Cache 加速热点读。同时记录每个 client 最近一次 request_id 用于幂等去重。"

**⑤ 持久化（`FileRaftStorage`）**
> "Raft 的 term、votedFor、日志条目、快照都持久化到本地文件，节点重启后可以恢复状态，不会因崩溃丢失已提交的数据。"

### 第四层：亮点句（结尾点睛）

> "整个项目遵循层次解耦原则：RaftNode 通过 `IRaftStorage` 和 `IRaftApplySink` 两个接口与上下层交互，它完全不知道上层是 KV 还是别的应用，也不知道下层是文件存储还是其他介质。这让各模块可以独立测试，也方便后续替换实现。"

### 常见追问速查

| 追问 | 一句话答 |
|------|---------|
| 为什么用跳表不用 B+ 树？ | 内存场景跳表实现简单且并发友好；B+ 树更适合磁盘页对齐场景 |
| 协程怎么实现的？ | 基于 C++20 coroutine，自实现 `Task` 和 `RpcAwaitable`，fanout 时一个 Peer 一个协程，结果 resume 回主线程 |
| 怎么保证幂等？ | 每个客户端带 client_id + request_id，KvStore 记录每个 client 最近一次 id，重复请求直接返回旧结果 |
| 脑裂怎么处理？ | 少数派分区凑不齐 Quorum，Propose 无法提交；Raft 的 term 机制保证旧 Leader 自动降级 |
