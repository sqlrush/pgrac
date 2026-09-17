# 三、新增系统视图与逐字段参考

Author: SqlRush <sqlrush@gmail.com>

适用：`v0.130.0-mvp.1` / `c581f3835a4a9a76ce5a77c0937930f21765725a`。相对原生 PG 16.13，新增 **28 个集群视图、248 个字段**。PG 原有的 `pg_stat_progress_cluster` 是 `CLUSTER` 命令进度视图，不属于本项目新增集群视图。

## 1. 全部视图目录

以下链接直接进入逐字段表；每表给出 SQL 类型、单位/格式、空值/哨兵、含义、行数范围和查询成本。

| 视图 | 字段数 | 用途与范围 | 逐字段说明 |
|---|---:|---|---|
| `pg_cluster_nodes` | 7 | 启动拓扑，不是实时成员资格 | [字段](reference/02-topology-membership-and-reconfiguration-views.md#pg_cluster_nodes) |
| `pg_stat_cluster_nodes` | 6 | 仅本节点身份/版本；state 固定 online | [字段](reference/02-topology-membership-and-reconfiguration-views.md#pg_stat_cluster_nodes) |
| `pg_cluster_cssd_peers` | 9 | 本节点观察的 peer 心跳与存活 | [字段](reference/02-topology-membership-and-reconfiguration-views.md#pg_cluster_cssd_peers) |
| `pg_cluster_quorum_state` | 7 | 本节点多数与租约综合结论 | [字段](reference/02-topology-membership-and-reconfiguration-views.md#pg_cluster_quorum_state) |
| `pg_cluster_voting_disks` | 7 | 投票路径目录，逐盘健康仍为占位值 | [字段](reference/02-topology-membership-and-reconfiguration-views.md#pg_cluster_voting_disks) |
| `pg_cluster_fence_state` | 8 | freeze/thaw 与自隔离状态 | [字段](reference/02-topology-membership-and-reconfiguration-views.md#pg_cluster_fence_state) |
| `pg_cluster_reconfig_state` | 10 | 最近一次重配置事件 | [字段](reference/02-topology-membership-and-reconfiguration-views.md#pg_cluster_reconfig_state) |
| `pg_cluster_membership` | 8 | 声明、incarnation、准入与永久移除 | [字段](reference/02-topology-membership-and-reconfiguration-views.md#pg_cluster_membership) |
| `pg_cluster_clean_leave_state` | 9 | 协作离开阶段与排空进度 | [字段](reference/02-topology-membership-and-reconfiguration-views.md#pg_cluster_clean_leave_state) |
| `pg_cluster_node_removal_state` | 14 | 永久移除、收缩与清理进度 | [字段](reference/02-topology-membership-and-reconfiguration-views.md#pg_cluster_node_removal_state) |
| `pg_stat_cluster_wait_events` | 2 | 本地等待事件名称注册表，不是耗时统计 | [字段](reference/03-interconnect-locking-and-cache-views.md#pg_stat_cluster_wait_events) |
| `pg_stat_gcluster_wait_events` | 3 | 带 node_id，当前仍只返回本地注册表 | [字段](reference/03-interconnect-locking-and-cache-views.md#pg_stat_gcluster_wait_events) |
| `pg_cluster_ic_peers` | 23 | TCP peer 连接与收发计数 | [字段](reference/03-interconnect-locking-and-cache-views.md#pg_cluster_ic_peers) |
| `pg_stat_cluster_ic` | 29 | TCP/RDMA 选择、DATA 通道、流量与错误 | [字段](reference/03-interconnect-locking-and-cache-views.md#pg_stat_cluster_ic) |
| `pg_cluster_ic_msg_types` | 5 | 消息类型注册，不是每类流量统计 | [字段](reference/03-interconnect-locking-and-cache-views.md#pg_cluster_ic_msg_types) |
| `pg_cluster_grd_shards` | 3 | 4096 个资源分片的 master 映射 | [字段](reference/03-interconnect-locking-and-cache-views.md#pg_cluster_grd_shards) |
| `pg_cluster_grd_entries` | 11 | 资源、已授予锁、等待与转换数量 | [字段](reference/03-interconnect-locking-and-cache-views.md#pg_cluster_grd_entries) |
| `pg_cluster_lmd` | 10 | LMD 后台进程生命周期与工作计数 | [字段](reference/03-interconnect-locking-and-cache-views.md#pg_cluster_lmd) |
| `pg_cluster_shmem` | 4 | 共享内存区域、容量与预留锁数 | [字段](reference/03-interconnect-locking-and-cache-views.md#pg_cluster_shmem) |
| `pg_stat_cluster_injections` | 4 | 当前查询进程注入状态 | [字段](reference/03-interconnect-locking-and-cache-views.md#pg_stat_cluster_injections) |
| `pg_stat_cluster_adg` | 13 | 本地备库接收/应用进度 | [字段](reference/04-adg-backup-and-recovery-views.md#pg_stat_cluster_adg) |
| `pg_stat_gcluster_adg` | 13 | 带 node_id，当前仍只返回本地 ADG 状态 | [字段](reference/04-adg-backup-and-recovery-views.md#pg_stat_gcluster_adg) |
| `pg_stat_cluster_backup` | 16 | 当前/最近备份状态与产物路径 | [字段](reference/04-adg-backup-and-recovery-views.md#pg_stat_cluster_backup) |
| `pg_cluster_backup_history` | 11 | 最近保留 manifest 摘要，不是完整历史档案 | [字段](reference/04-adg-backup-and-recovery-views.md#pg_cluster_backup_history) |
| `pg_cluster_restore_points` | 5 | 已记录的集群恢复点 | [字段](reference/04-adg-backup-and-recovery-views.md#pg_cluster_restore_points) |
| `pg_cluster_pitr_status` | 6 | 恢复目标解析与可达性 | [字段](reference/04-adg-backup-and-recovery-views.md#pg_cluster_pitr_status) |
| `pg_cluster_state` | 3 | 子系统 category/key/value 文本投影 | [字段与键字典](reference/05-pg-cluster-state-key-dictionary.md) |
| `pg_stat_cluster_counters` | 2 | 命名计数器注册表 | [字段与计数器字典](reference/06-cluster-counter-dictionary.md) |

SQL 字段及类型分别与标签中的 [system_views.sql](https://github.com/sqlrush/pgrac/blob/v0.130.0-mvp.1/src/backend/catalog/system_views.sql) 和 [pg_proc.dat](https://github.com/sqlrush/pgrac/blob/v0.130.0-mvp.1/src/include/catalog/pg_proc.dat) 对应。监控程序应显式选择需要的列，不依赖 `SELECT *` 的永久列顺序。

## 2. 权限、范围和一致性

这些视图在此标签中向 `PUBLIC` 授予只读 `SELECT`。读取权限不包含注入、备份、节点移除等会改变状态的管理函数权限。视图可能暴露主机地址、设备/备份路径和错误文本；不要向不可信租户提供无约束的数据库访问。

- 带 `gcluster` 的名称**当前不保证自动查询四节点**。需要四节点结果时，采集器逐节点连接并显式附上被采集节点身份与时间。
- `node_id=0` 是合法节点，不是通用的“未初始化”。master/coordinator 缺席常用 `-1`，事件 ID 或 SCN 的 0 则需按字段解释。
- 一个 SELECT 内后台仍可推进状态；跨行、跨视图、跨节点不是原子快照。保留各节点采集开始/结束时间，不能声称同一微秒的全局现场。
- `NULL`、空串、0、`unknown` 含义不同。未知值不能转成“健康”；计数为零也可能是未启用、未采样、被重置或仍为占位。
- epoch/incarnation/generation 是身份与代际，LSN/SCN 是特定范围的进度，不是可以互换的时间或 TPS。
- ADG、备份、PITR、移除相关视图用于观察已存在的接口；不是本 MVP 对相应故障/恢复功能的认证。

## 3. 等待、计数与诊断的正确读法

等待事件视图列出“有哪些名称”，不列出当前等待者或累计耗时。当前会话等待应查询 PG 原生视图：

```sql
SELECT pid, backend_type, state, wait_event_type, wait_event,
       clock_timestamp() - query_start AS query_age, query
  FROM pg_stat_activity
 WHERE pid <> pg_backend_pid()
 ORDER BY query_start NULLS LAST;
```

`pg_stat_cluster_counters` 的部分计数来自当前进程内注册表；另一些由共享状态镜像。必须按附录解释生命周期，不能跨重启或跨 backend 直接相减。当需要事务吞吐时，不使用消息数、head 次数或 grant 数代替成功提交数。

`pg_cluster_state` 提供大量具体子系统读数。键字典包含所有源码静态键和动态键族；某键出现在字典不表示每次查询都返回该键，也不表示模块已经激活。

```sql
SELECT clock_timestamp() AS sampled_at, category, key, value
  FROM pg_cluster_state
 WHERE category IN ('pcm','r4','undo_cleaner','ctrc','write_fence')
 ORDER BY category, key;
```

## 4. 三个容易误判的视图

1. `pg_stat_cluster_nodes.state='online'` 是当前 producer 的固定值，不是完整健康检查；要结合 membership、quorum、写栅栏与实际业务查询。
2. `pg_cluster_voting_disks.state='unknown'`、逐盘计数 0、时间 NULL 是此标签的占位输出。不能拿它证明磁盘从未发生 I/O 错误。
3. `pg_cluster_grd_entries` 会遍历资源表，适合按需排障。SQL 的 `LIMIT`/`WHERE` 不保证底层 SRF 只扫描少量资源，不应每秒全量抓取。`pg_cluster_shmem.lwlock_count` 是区域预留锁数量，不是锁等待累计次数。

## 5. 建议的最小只读检查

四节点分别执行并保留身份/时间：

```sql
SELECT clock_timestamp(), current_setting('cluster.node_id') AS node_id,
       pg_postmaster_start_time();
SELECT * FROM pg_cluster_membership ORDER BY node_id;
SELECT * FROM pg_cluster_quorum_state;
SELECT * FROM pg_cluster_fence_state;
SELECT node_id, state, last_error_code, last_error FROM pg_cluster_ic_peers;
```

结果解释顺序：配置了谁 → 与谁连接 → 谁被准入 → 多数/租约是否有效 → 写入是否允许 → 业务与数据是否正确。某层正常不能覆盖后面一层的失败。查询本身失败或超时应原样留证，不能把无返回值当作零错误。

更多示例见[运维采样与成本附录](reference/12-operations-and-query-cost.md)。本手册不要求为阅读文档启动破坏性注入或重新运行发布验收。
