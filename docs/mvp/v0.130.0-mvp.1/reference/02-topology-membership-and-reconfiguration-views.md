# 拓扑、成员关系与重配置视图

Author: SqlRush <sqlrush@gmail.com>

适用版本：`v0.130.0-mvp.1`。返回[手册目录](../README.md)。


以下字段表与 `v0.130.0-mvp.1` 的 SQL catalog 逐列对应。跨视图非原子；0 不是通用的未知哨兵，节点 0 合法。计数生命周期、未启用和占位输出需结合各行说明。

## `pg_cluster_nodes`

启动时读取的集群拓扑。

- 行基数：每个声明节点一行；无配置时为本地回退行。
- 刷新来源：启动配置快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | 0..127；0 是有效节点，不是空值 | 此拓扑行对应的节点编号，合法值包含 0；不等于查询节点。 |
| `hostname` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 拓扑中声明的主机名。 |
| `interconnect_addr` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 节点间通信地址。 |
| `public_addr` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 客户端访问地址。 |
| `role` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 节点或实例的当前角色。 |
| `region` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 拓扑中的 region 标签。 |
| `is_self` | `boolean` | 布尔 | false 表示该条件当前不成立 | 该行是否对应当前实例。 |

示例：

```sql
SELECT * FROM pg_cluster_nodes LIMIT 50;
```

## `pg_stat_cluster_nodes`

节点运行身份和版本状态；当前 producer 固定返回本地节点且 state 固定为 online。

- 行基数：当前实现固定一行。
- 刷新来源：启动身份与 build 常量。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | 0..127；0 是有效节点，不是空值 | 产生该行的节点编号。 |
| `role` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 从拓扑读取的本节点角色；无法找到拓扑行时为 unknown。 |
| `state` | `text` | — | 固定 online；不作健康依据 | 当前固定输出 online，不是成员、quorum 或业务健康结论。 |
| `startup_time` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 当前实例的启动时间。 |
| `pgrac_version` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 编译时 PGRAC 版本字符串；本标签可能与发布标签不同，另用 commit/二进制哈希识别。 |
| `pg_version` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 当前实例报告的 PostgreSQL 版本。 |

示例：

```sql
SELECT * FROM pg_stat_cluster_nodes LIMIT 50;
```

## `pg_cluster_cssd_peers`

CSSD 应用层存活判断。

- 行基数：每个声明 peer 一行。
- 刷新来源：CSSD 共享状态实时快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | 0..127；0 是有效节点，不是空值 | 该行所观察的 peer 编号；心跳结论是查询节点的观察。 |
| `state` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | CSSD 应用层状态：alive、suspected 或 dead；与 transport state 分层。 |
| `last_heartbeat_send_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近一次向该 peer 发送 CSSD 心跳的时间。 |
| `last_heartbeat_recv_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近一次收到该 peer 心跳的时间；不是对端墙钟。 |
| `heartbeat_send_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 向该 peer 发送心跳的累计次数。 |
| `heartbeat_recv_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 从该 peer 接收心跳的累计次数。 |
| `suspected_since` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | peer 首次进入 suspected 状态的时间。 |
| `dead_since` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | peer 首次进入 dead 状态的时间。 |
| `suspected_transitions` | `bigint` | 次数 | 未活动/未记录时可能为 0；0 不单独证明健康 | peer 进入 suspected 状态的累计次数。 |

示例：

```sql
SELECT * FROM pg_cluster_cssd_peers LIMIT 50;
```

## `pg_cluster_quorum_state`

本节点 quorum/lease 综合判定。若 QVOTEC 尚未初始化，in_quorum=false，其余字段为 NULL。quorum_size 计算的是投票介质多数，不是成员节点多数。

- 行基数：固定一行。
- 刷新来源：QVOTEC 共享状态实时快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `in_quorum` | `boolean` | 布尔 | false 表示该条件当前不成立 | 综合 lease、磁盘多数和冻结状态后的最终 quorum 布尔结论。 |
| `quorum_size` | `integer` | 个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 投票介质多数门槛：floor(disks_total/2)+1；不是成员节点多数数目。 |
| `disks_ok` | `integer` | 个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 当前成功参与 quorum 的 voting disk 数量。 |
| `disks_total` | `integer` | 个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 配置的 voting disk 总数。 |
| `current_epoch_at_boot` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 本次启动从 quorum 介质读取并采纳的 epoch。 |
| `last_quorum_loss_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 本节点最近一次记录失去 quorum 的时间。 |
| `collision_state` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | voting disk 上是否观察到身份或 epoch 冲突。 |

示例：

```sql
SELECT * FROM pg_cluster_quorum_state LIMIT 50;
```

## `pg_cluster_voting_disks`

voting disk 路径目录；当前 per-disk state/timestamp/counter 尚未接入 driver，固定投影 unknown/NULL/0。

- 行基数：每个配置路径一行。
- 刷新来源：postmaster-frozen GUC 字符串。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `path` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 配置的文件、设备或目录路径。 |
| `state` | `text` | — | 固定 unknown（占位） | 当前固定为 unknown；尚不能作为 per-disk 健康结论。 |
| `last_read_at` | `timestamptz` | 时间戳 | 固定 NULL（占位） | 此标签固定为 NULL，未接入逐盘读取时间；不能判断最近 I/O。 |
| `last_write_at` | `timestamptz` | 时间戳 | 固定 NULL（占位） | 此标签固定为 NULL，未接入逐盘写入时间；不能判断最近 I/O。 |
| `read_count` | `bigint` | 次/个 | 固定 0（占位），不能据此判定无错误 | 当前固定为 0 的占位计数；尚未接入 per-disk driver。 |
| `write_count` | `bigint` | 次/个 | 固定 0（占位），不能据此判定无错误 | 当前固定为 0 的占位计数；尚未接入 per-disk driver。 |
| `io_error_count` | `bigint` | 次/个 | 固定 0（占位），不能据此判定无错误 | 当前固定为 0 的占位计数；尚未接入 per-disk driver。 |

示例：

```sql
SELECT * FROM pg_cluster_voting_disks LIMIT 50;
```

## `pg_cluster_fence_state`

freeze/thaw 与 self-fence 状态。

- 行基数：固定一行。
- 刷新来源：fence 共享状态实时快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `last_freeze_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 本节点最近一次进入写入冻结的时间。 |
| `last_thaw_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 本节点最近一次解除相应冻结的时间，不单独证明所有写入门开放。 |
| `self_fence_pending` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本节点是否处于等待 self-fence 宽限期结束的状态。 |
| `self_fence_grace_remaining_ms` | `integer` | 毫秒 | 未活动/未记录时可能为 0；0 不单独证明健康 | 当前自隔离宽限期剩余毫秒数；需同时看 self_fence_pending。 |
| `freeze_broadcast_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 发出写入冻结广播的累计次数。 |
| `thaw_broadcast_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 发出解除冻结广播的累计次数。 |
| `self_fence_initiated_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 发起本节点自隔离动作的累计次数。 |
| `freeze_signal_received_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 收到写入冻结信号的累计次数。 |

示例：

```sql
SELECT * FROM pg_cluster_fence_state LIMIT 50;
```

## `pg_cluster_reconfig_state`

最近一次重配置 episode。

- 行基数：启用集群时一行，否则零行。
- 刷新来源：reconfig 共享状态快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `event_id` | `bigint` | — | 0：尚无应用过的事件 | 重配置 episode 标识；0 表示从未应用 episode。 |
| `coordinator_node_id` | `integer` | — | -1 可表示无目标/owner；0 是有效节点 | 该操作协调节点的编号，0 是合法节点；无协调者时按当前状态解释。 |
| `old_epoch` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | episode 应用前的 membership epoch。 |
| `new_epoch` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | episode 计划并应用后的 membership epoch。 |
| `dead_bitmap` | `text` | 位图 | 空串/NULL/unknown 不等于成功，见本字段说明 | 本次重配置认定失效节点的十六进制位图。 |
| `applied_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 本节点应用该重配置事件的时间。 |
| `observer_role` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 本节点在该重配置 episode 中承担的角色。 |
| `event_seq` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 本地观察的重配置事件序号。 |
| `cssd_dead_generation` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 触发该 episode 的 CSSD dead evidence generation。 |
| `reconfig_kind` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 本次重配置的事件种类。 |

示例：

```sql
SELECT * FROM pg_cluster_reconfig_state LIMIT 50;
```

## `pg_cluster_membership`

每个节点的 admission/membership 决策。

- 行基数：每个声明节点一行。
- 刷新来源：membership 共享状态快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | 0..127；0 是有效节点，不是空值 | 被观察/准入的节点编号，合法值包含 0。 |
| `declared` | `boolean` | 布尔 | false 表示该条件当前不成立 | 节点是否存在于当前拓扑声明中。 |
| `state` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | decision state：absent、dead、joining、member 或 rejected。 |
| `presented_incarnation` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | QVOTEC 最近观察到的节点 incarnation。 |
| `last_admitted_incarnation` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 该节点已准入 incarnation 的单调 floor。 |
| `admitted_epoch` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 该 membership 行当前观察/准入所绑定的 epoch。 |
| `removed` | `boolean` | 布尔 | false 表示该条件当前不成立 | 节点是否已进入永久移除终态。 |
| `removed_epoch` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 永久移除写入的 epoch；未移除时为 0。 |

示例：

```sql
SELECT * FROM pg_cluster_membership LIMIT 50;
```

## `pg_cluster_clean_leave_state`

本节点协作式离开进度。

- 行基数：启用集群时一行，否则零行。
- 刷新来源：clean-leave 共享状态快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `phase` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | idle/requested/quiescing/ges_draining/gcs_flushing/barrier_wait/committed/aborted 系列阶段。 |
| `leaving_node_id` | `integer` | — | -1 可表示无目标/owner；0 是有效节点 | 正在协作离开的节点编号；idle 时为 -1。 |
| `leave_epoch` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 本次计划离开所绑定的成员代际。 |
| `ges_drained_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本次离开流程已排空的 GES 项目数，不是历史事务总数。 |
| `gcs_flushed_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本次离开流程已完成 GCS 刷写处理的计数。 |
| `shards_remastered` | `bigint` | 个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本次离开流程已完成 master 调整的 shard 数。 |
| `survivor_ack_count` | `integer` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | ack bitmap 的 popcount，而不是独立累加器。 |
| `barrier_deadline` | `timestamptz` | 时间戳 | NULL：idle，没有 deadline | 当前离开屏障的绝对截止时间；idle 时为 NULL。 |
| `escalate_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 协作离开不能按正常流程完成而转入升级处理的计数。 |

示例：

```sql
SELECT * FROM pg_cluster_clean_leave_state LIMIT 50;
```

## `pg_cluster_node_removal_state`

永久移除节点的当前进度。

- 行基数：启用集群时一行，否则零行。
- 刷新来源：node-removal 共享状态快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `phase` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | idle 到 precheck、fence、shrink、cleanup、committed/aborted 的阶段。 |
| `target_node_id` | `integer` | — | -1 可表示无目标/owner；0 是有效节点 | 本次永久移除的目标节点编号；idle 时为 -1。 |
| `coordinator_node_id` | `integer` | — | -1 可表示无目标/owner；0 是有效节点 | 该操作协调节点的编号，0 是合法节点；无协调者时按当前状态解释。 |
| `remove_epoch` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 永久移除操作所绑定的成员代际。 |
| `fence_armed` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本次移除是否已经记录隔离步骤武装；不单独证明物理断电成功。 |
| `membership_shrunk` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本次移除是否已完成成员集合收缩。 |
| `grd_cleaned` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本次移除的 GRD 清理步骤是否已完成。 |
| `pcm_cleaned` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本次移除的块访问状态清理是否已完成。 |
| `ack_count` | `integer` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 当前 removal ack bitmap 的 popcount。 |
| `deadline_us` | `bigint` | 绝对时间微秒 | NULL：idle 或尚未进入 cleanup | 移除 cleanup 阶段的绝对截止读数；PG 时间戳纪元的微秒数，不是剩余时间。 |
| `removal_committed_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 永久移除被提交为终态的累计次数。 |
| `cleanup_blocked_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 永久移除因清理不能完成而被阻断的累计次数。 |
| `leftover_detected_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 清理/核验发现残留资源的累计次数。 |
| `zombie_write_rejected_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 拒绝已失去资格的旧节点写入的累计次数。 |

示例：

```sql
SELECT * FROM pg_cluster_node_removal_state LIMIT 50;
```
