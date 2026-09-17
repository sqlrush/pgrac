# Interconnect、锁与 Cache Fusion 视图

Author: SqlRush <sqlrush@gmail.com>

适用版本：`v0.130.0-mvp.1`。返回[手册目录](../README.md)。


以下字段表与 `v0.130.0-mvp.1` 的 SQL catalog 逐列对应。跨视图非原子；0 不是通用的未知哨兵，节点 0 合法。计数生命周期、未启用和占位输出需结合各行说明。

## `pg_stat_cluster_wait_events`

本地已注册的集群等待事件。

- 行基数：每个等待事件一行。
- 刷新来源：注册表，查询时生成。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `type` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | PostgreSQL wait-event type/class 名称。 |
| `name` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | PGRAC 注册的等待事件名称。 |

示例：

```sql
SELECT * FROM pg_stat_cluster_wait_events LIMIT 50;
```

## `pg_stat_gcluster_wait_events`

带 node_id 的集群等待事件；当前实现返回本节点。

- 行基数：每个可见节点/等待事件一行；当前为本节点。
- 刷新来源：注册表，查询时生成。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | 0..127；0 是有效节点，不是空值 | 产生该行的节点编号。 |
| `type` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | PostgreSQL wait-event type/class 名称。 |
| `name` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | PGRAC 注册的等待事件名称。 |

示例：

```sql
SELECT * FROM pg_stat_gcluster_wait_events LIMIT 50;
```

## `pg_cluster_ic_peers`

Tier1 TCP peer 连接、心跳和流量状态。

- 行基数：每个声明 peer 一行。
- 刷新来源：共享内存实时快照。
- 查询成本：低至中。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | 0..127；0 是有效节点，不是空值 | 该行统计的 peer 编号；采集节点需由采集器另外记录。 |
| `state` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | Tier1 transport 状态：down、connecting、connected 或 rejected；不代表 membership。 |
| `interconnect_addr` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 节点间通信地址。 |
| `last_connect_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近一次成功建立到该 peer 连接的本地记录时间。 |
| `last_send_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近一次向该 peer 发送消息的本地记录时间。 |
| `last_recv_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近一次从该 peer 收到消息的本地记录时间。 |
| `last_heartbeat_sent_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近一次向该 peer 发送 transport 心跳的时间。 |
| `last_heartbeat_recv_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近一次收到该 peer 心跳的时间；不是对端墙钟。 |
| `heartbeat_send_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 向该 peer 发送心跳的累计次数。 |
| `heartbeat_recv_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 从该 peer 接收心跳的累计次数。 |
| `msg_send_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 向该 peer 发送消息的累计次数，不是事务提交数。 |
| `msg_recv_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 从该 peer 接收消息的累计次数，不是事务提交数。 |
| `bytes_send` | `bigint` | 字节 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本节点向该 peer 发送的累计字节数。 |
| `bytes_recv` | `bigint` | 字节 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本节点从该 peer 接收的累计字节数。 |
| `reconnect_count` | `integer` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 到该 peer 的重连累计次数。 |
| `connect_error_count` | `integer` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 连接该 peer 时观察到错误的累计次数。 |
| `last_errno` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 最近一次底层 socket 错误的 errno；0 表示无已记录 errno。 |
| `last_error_code` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 最近一次 PGRAC/SQLSTATE 风格错误码；空串表示无记录。 |
| `last_error` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 最近一次 transport 错误文本；空串表示无记录。 |
| `stale_epoch_drop_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 因入站消息 epoch 陈旧而丢弃的累计次数；结合成员变更解释。 |
| `chunk_reassembly_active` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 当前正在重组的 chunked message 数量。 |
| `chunk_reassembly_timeout_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 分片消息未在重组期限内完成的累计次数。 |
| `lamport_observe_advance_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 接收消息导致本地 Lamport 时钟前移的累计次数。 |

示例：

```sql
SELECT * FROM pg_cluster_ic_peers LIMIT 50;
```

## `pg_stat_cluster_ic`

TCP/RDMA mux、RDMA 与 block-reply lane 状态。特别注意：mr_registered 以及 block_sge、tier3、inline、unsignaled_batch、busypoll 系列读数来自本节点全局状态，在每个 peer 行重复；不能把这些列跨行求和。

- 行基数：每个 peer 一行。
- 刷新来源：共享内存实时快照。
- 查询成本：低至中。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | 0..127；0 是有效节点，不是空值 | 该行对应的拓扑节点/peer 编号；不是默认的采集节点。 |
| `transport` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 当前选中的传输方式。 |
| `rdma_state` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 该 peer 的 RDMA 生命周期状态。 |
| `provider` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 当前 RDMA/transport provider 名称。 |
| `rdma_addr` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 拓扑中配置的 RDMA 地址；未配置时 NULL。 |
| `rdma_gid` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 拓扑中配置的 RDMA GID；未配置时 NULL。 |
| `rdma_port` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 拓扑中配置的 RDMA port。 |
| `mr_registered` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本节点全局 RDMA 内存注册状态，在各 peer 行重复投影。 |
| `cq_depth` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 该 peer completion queue 当前/配置深度投影。 |
| `fallback_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 该 peer 从 RDMA 等优选路径回退的累计次数。 |
| `send_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 该 peer 传输层记录的发送计数，不等于 SQL 次数。 |
| `recv_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 该 peer 传输层记录的接收计数，不等于 SQL 次数。 |
| `bytes_send` | `bigint` | 字节 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本节点向该 peer 发送的累计字节数。 |
| `bytes_recv` | `bigint` | 字节 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本节点从该 peer 接收的累计字节数。 |
| `block_sge_send_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本节点全局 block scatter/gather 发送计数；每个 peer 行重复投影，禁止跨行求和。 |
| `block_sge_fallback_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本节点全局 block scatter/gather 回退计数；每个 peer 行重复投影。 |
| `tier3_send_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本节点全局 tier3 发送计数；每个 peer 行重复投影。 |
| `inline_send_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本节点全局 inline 发送计数；每个 peer 行重复投影。 |
| `unsignaled_batch_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本节点全局未逐笔请求完成通知的发送批次数；每个 peer 行重复投影。 |
| `busypoll_us_burned` | `bigint` | 累计微秒 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本节点全局忙轮询消耗的累计微秒数；每个 peer 行重复投影。 |
| `busypoll_fallback_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 本节点全局忙轮询回退计数；每个 peer 行重复投影。 |
| `block_reply_lane_state` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | block reply 专用 lane 的当前状态。 |
| `block_reply_lane_fallback_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 该 peer 的块回复专用通道回退次数。 |
| `block_reply_lane_error_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 该 peer 的块回复专用通道错误次数。 |
| `latency_us_sum` | `bigint` | 累计微秒 | 未活动/未记录时可能为 0；0 不单独证明健康 | 已采样 RDMA 延迟的累计微秒数；与 sample_count 相除得到均值。 |
| `latency_sample_count` | `bigint` | 样本数 | 未活动/未记录时可能为 0；0 不单独证明健康 | RDMA 延迟样本数。 |
| `last_error_code` | `text` | — | NULL：当前未记录错误 | 该 peer 最近错误代码；未记录时为 NULL。 |
| `last_error` | `text` | — | NULL：当前未记录错误 | 该 peer 最近错误文本；未记录时为 NULL。 |
| `last_block_reply_error` | `text` | — | NULL：当前未记录错误 | block reply lane 最近错误；无记录时 NULL。 |

示例：

```sql
SELECT * FROM pg_stat_cluster_ic LIMIT 50;
```

## `pg_cluster_ic_msg_types`

已注册的 interconnect 消息类型。

- 行基数：每个消息类型一行。
- 刷新来源：进程内 dispatch 注册表。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `msg_type` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | wire message type 的数值编号。 |
| `name` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 注册对象的稳定名称。 |
| `allowed_producer_mask` | `bigint` | 位图 | 未活动/未记录时可能为 0；0 不单独证明健康 | 允许产生该消息的进程角色位图。 |
| `broadcast_ok` | `boolean` | 布尔 | false 表示该条件当前不成立 | 该消息是否允许广播。 |
| `handler_present` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本进程是否注册了入站处理器；false 可表示 send-only 类型。 |

示例：

```sql
SELECT * FROM pg_cluster_ic_msg_types LIMIT 50;
```

## `pg_cluster_grd_shards`

GRD shard 到 master node 的映射。

- 行基数：固定 4096 行。
- 刷新来源：GRD master map 快照。
- 查询成本：中。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `shard_id` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | GRD shard 编号，范围 0..4095。 |
| `master_node_id` | `integer` | — | -1 可表示无目标/owner；0 是有效节点 | 查询时该 shard 映射到的 resource master 节点。 |
| `is_local` | `boolean` | 布尔 | false 表示该条件当前不成立 | master_node_id 是否等于本节点 cluster.node_id。 |

示例：

```sql
SELECT * FROM pg_cluster_grd_shards LIMIT 50;
```

## `pg_cluster_grd_entries`

GRD resource entry 诊断快照。

- 行基数：每个当前 entry 一行。
- 刷新来源：遍历 GRD hash 表。
- 查询成本：高；不要在高频监控中全表扫描。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `shard_id` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 由完整 resource identity hash 得出的 GRD shard。 |
| `field1` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | resource identity 的第 1 个 32 位字段；需与 type/lockmethodid 联合解释。 |
| `field2` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | resource identity 的第 2 个 32 位字段；需与 type/lockmethodid 联合解释。 |
| `field3` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | resource identity 的第 3 个 32 位字段；需与 type/lockmethodid 联合解释。 |
| `field4` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | resource identity 的第 4 个 32 位字段；需与 type/lockmethodid 联合解释。 |
| `type` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | resource identity 类型编号。 |
| `lockmethodid` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | PostgreSQL lock method 编号。 |
| `ngranted` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 当前 grant 数量。 |
| `nwaiters` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 当前 waiter 数量。 |
| `nconverts` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 当前 convert waiter 数量。 |
| `state_flags` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | entry 状态位图；作为诊断值，不能单独授权。 |

示例：

```sql
SELECT * FROM pg_cluster_grd_entries LIMIT 50;
```

## `pg_cluster_lmd`

LMD 生命周期和工作计数。

- 行基数：固定一行。
- 刷新来源：LMD 共享状态实时快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `pid` | `integer` | — | NULL：尚无已启动 PID | 后台进程 PID；未启动时使用该视图定义的空值。 |
| `state` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | disabled/not_started/starting/ready/draining/stopped 生命周期状态。 |
| `reason` | `text` | — | READY 时 NULL；其他状态返回原因文本 | 只有 READY 时为 NULL；其他状态给出 disabled_by_config、lmd_not_ready 或 crashed_unavailable。 |
| `started_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 该操作/进程开始的时间。 |
| `ready_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | LMD 最近进入 READY 的时间；尚未 ready 时为 NULL。 |
| `started_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | LMD 启动计数；与 started_at/ready_at 一起区分当前进程生命周期。 |
| `edge_submission_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 提交给 LMD 的等待图边相关工作计数，不是 SQL 等待秒数。 |
| `wake_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | LMD 被唤醒执行工作的计数。 |
| `idle_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | LMD 观察到无工作/空闲轮次的计数。 |
| `error_count` | `bigint` | 次/个 | 未活动/未记录时可能为 0；0 不单独证明健康 | LMD 记录的错误计数；结合 reason 和服务端日志归因。 |

示例：

```sql
SELECT * FROM pg_cluster_lmd LIMIT 50;
```

## `pg_cluster_shmem`

PGRAC 共享内存 region 注册表。

- 行基数：每个 region 一行。
- 刷新来源：启动注册表快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `name` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 注册对象的稳定名称。 |
| `size_bytes` | `bigint` | 字节 | 未活动/未记录时可能为 0；0 不单独证明健康 | 该共享内存区域注册的大小，单位字节；不是当前驻留集或可用内存。 |
| `lwlock_count` | `integer` | 把/个 | 0：该区域未登记 LWLock | 该共享内存区域注册/预留的 LWLock 数量；不是等待或加锁累计次数。 |
| `owner_subsys` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 注册并拥有该共享内存 region 的子系统。 |

示例：

```sql
SELECT * FROM pg_cluster_shmem LIMIT 50;
```

## `pg_stat_cluster_injections`

当前进程可见的故障注入点状态。

- 行基数：每个编译期注入点一行。
- 刷新来源：查询进程内注册表。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `name` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 注册对象的稳定名称。 |
| `fault_type` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 当前 arm 的 fault 类型；none 表示未武装。 |
| `param` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | fault 类型解释的 64 位参数，例如 sleep 时长或 skip 次数。 |
| `hits` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 当前进程中该注入点的累计命中次数。 |

示例：

```sql
SELECT * FROM pg_stat_cluster_injections LIMIT 50;
```
