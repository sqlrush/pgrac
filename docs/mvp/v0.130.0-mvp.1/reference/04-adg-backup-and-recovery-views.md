# ADG、备份与恢复视图

Author: SqlRush <sqlrush@gmail.com>

适用版本：`v0.130.0-mvp.1`。返回[手册目录](../README.md)。


> 本 MVP 未认证 ADG、备份恢复与 PITR 端到端能力；查询接口存在不等于恢复已验证。

以下字段表与 `v0.130.0-mvp.1` 的 SQL catalog 逐列对应。跨视图非原子；0 不是通用的未知哨兵，节点 0 合法。计数生命周期、未启用和占位输出需结合各行说明。

## `pg_stat_cluster_adg`

本节点 ADG 接收、应用和只读水位。

- 行基数：固定一行。
- 刷新来源：ADG 共享状态实时快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | 0..127；0 是有效节点，不是空值 | 产生该行的节点编号。 |
| `dg_role` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | Data Guard 角色。 |
| `dg_mode` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | WAL shipping/acknowledgement 模式。 |
| `adg_enabled` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本实例是否启用 ADG 运行路径。 |
| `apply_master_node_id` | `integer` | — | -1 可表示无目标/owner；0 是有效节点 | 当前负责应用的节点编号；无 owner 时为 -1，0 是合法节点。 |
| `apply_master_term` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | Apply Master 的单调 term，用于区分旧 owner。 |
| `mrp_status` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | managed recovery process 的生命周期状态。 |
| `receive_lsn` | `pg_lsn` | WAL LSN | 按当前功能状态解释无效/空 LSN，不作为可用水位 | 已接收 WAL 的位置，需在对应线程/恢复上下文内比较。 |
| `apply_lsn` | `pg_lsn` | WAL LSN | 按当前功能状态解释无效/空 LSN，不作为可用水位 | 已应用 WAL 的位置，不能单独证明所有线程达到一致读水位。 |
| `standby_consistent_scn` | `bigint` | SCN | 0：通常未建立有效水位，不作为终态证明 | 备库当前可用于一致性判断的 SCN 水位；0 不能当成有效证明。 |
| `lag_bytes` | `bigint` | 字节 | 未活动/未记录时可能为 0；0 不单独证明健康 | 接收与应用进度之间的字节差；未活动/未采样时的 0 不证明无延迟。 |
| `lag_seconds` | `double precision` | 秒 | 未活动/未记录时可能为 0；0 不单独证明健康 | 接收与应用进度之间按时间估算的延迟秒数。 |
| `apply_rate_bytes_per_sec` | `double precision` | 字节/秒 | 未活动/未记录时可能为 0；0 不单独证明健康 | 按当前 lag 样本估算的 WAL 应用速率。 |

示例：

```sql
SELECT * FROM pg_stat_cluster_adg LIMIT 50;
```

## `pg_stat_gcluster_adg`

带节点维度的 ADG 状态；当前实现返回本节点。

- 行基数：当前固定一行。
- 刷新来源：ADG 共享状态实时快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | 0..127；0 是有效节点，不是空值 | 产生该行的节点编号。 |
| `dg_role` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | Data Guard 角色。 |
| `dg_mode` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | WAL shipping/acknowledgement 模式。 |
| `adg_enabled` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本实例是否启用 ADG 运行路径。 |
| `apply_master_node_id` | `integer` | — | -1 可表示无目标/owner；0 是有效节点 | 当前负责应用的节点编号；无 owner 时为 -1，0 是合法节点。 |
| `apply_master_term` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | Apply Master 的单调 term，用于区分旧 owner。 |
| `mrp_status` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | managed recovery process 的生命周期状态。 |
| `receive_lsn` | `pg_lsn` | WAL LSN | 按当前功能状态解释无效/空 LSN，不作为可用水位 | 已接收 WAL 的位置，需在对应线程/恢复上下文内比较。 |
| `apply_lsn` | `pg_lsn` | WAL LSN | 按当前功能状态解释无效/空 LSN，不作为可用水位 | 已应用 WAL 的位置，不能单独证明所有线程达到一致读水位。 |
| `standby_consistent_scn` | `bigint` | SCN | 0：通常未建立有效水位，不作为终态证明 | 备库当前可用于一致性判断的 SCN 水位；0 不能当成有效证明。 |
| `lag_bytes` | `bigint` | 字节 | 未活动/未记录时可能为 0；0 不单独证明健康 | 接收与应用进度之间的字节差；未活动/未采样时的 0 不证明无延迟。 |
| `lag_seconds` | `double precision` | 秒 | 未活动/未记录时可能为 0；0 不单独证明健康 | 接收与应用进度之间按时间估算的延迟秒数。 |
| `apply_rate_bytes_per_sec` | `double precision` | 字节/秒 | 未活动/未记录时可能为 0；0 不单独证明健康 | 按当前 lag 样本估算的 WAL 应用速率。 |

示例：

```sql
SELECT * FROM pg_stat_gcluster_adg LIMIT 50;
```

## `pg_stat_cluster_backup`

当前或最近一次 cluster backup 状态。

- 行基数：固定一行。
- 刷新来源：backup 共享状态快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `in_progress` | `boolean` | 布尔 | false 表示该条件当前不成立 | 操作当前是否仍在进行。 |
| `backup_id` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | backup 的稳定标识/标签。 |
| `coordinator_node_id` | `integer` | — | -1 可表示无目标/owner；0 是有效节点 | 该操作协调节点的编号，0 是合法节点；无协调者时按当前状态解释。 |
| `start_redo_lsn` | `pg_lsn` | WAL LSN | 按当前功能状态解释无效/空 LSN，不作为可用水位 | 本次备份起始 redo 位置。 |
| `checkpoint_lsn` | `pg_lsn` | WAL LSN | 按当前功能状态解释无效/空 LSN，不作为可用水位 | 本次备份记录的检查点 WAL 位置。 |
| `stop_cut_lsn` | `pg_lsn` | WAL LSN | 按当前功能状态解释无效/空 LSN，不作为可用水位 | 本次备份完成 cut 对应的 WAL 位置；未完成时不能作恢复依据。 |
| `consistent_scn` | `bigint` | SCN | 0：通常未建立有效水位，不作为终态证明 | 备份记录的一致性 SCN，须与对应 manifest 和线程集合一起解释。 |
| `manifest_crc` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | manifest 内容的 CRC32C 校验值。 |
| `started_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 该操作/进程开始的时间。 |
| `stopped_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 该操作结束的时间；未记录时为 NULL。 |
| `backup_parallel_channels` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 配置并投影到备份状态的并行 copy channel 数。 |
| `backup_wal_retention` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 配置并投影到备份状态的 WAL 保留提示，单位 MiB。 |
| `restore_points_enabled` | `boolean` | 布尔 | false 表示该条件当前不成立 | 备份状态中投影的恢复点功能配置。 |
| `restore_point_interval_ms` | `integer` | 毫秒 | 未活动/未记录时可能为 0；0 不单独证明健康 | 恢复点生成周期配置，单位毫秒，不是恢复点年龄。 |
| `backup_set_path` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 最近一次 backup set 的文件系统路径。 |
| `manifest_path` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 最近一次成功发布的 cluster manifest 路径。 |

示例：

```sql
SELECT * FROM pg_stat_cluster_backup LIMIT 50;
```

## `pg_cluster_backup_history`

最近保留的 backup manifest 摘要。

- 行基数：零或一行。
- 刷新来源：manifest 摘要快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `backup_id` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | backup 的稳定标识/标签。 |
| `consistent_scn` | `bigint` | SCN | 0：通常未建立有效水位，不作为终态证明 | 备份记录的一致性 SCN，须与对应 manifest 和线程集合一起解释。 |
| `scn_durable_peak` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | backup cut 证明覆盖的最高 durable SCN。 |
| `timeline` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | manifest 记录的 WAL timeline ID。 |
| `catversion` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | manifest 记录的 PostgreSQL CatalogVersionNo。 |
| `storage_id` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | manifest 记录的 shared-storage backend 枚举 ID。 |
| `node_count` | `integer` | 个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 备份 manifest 覆盖的节点数量；不是实时在线节点计数。 |
| `thread_count` | `integer` | 个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 该备份或恢复点覆盖的 WAL 线程数量。 |
| `manifest_crc` | `bigint` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | manifest 内容的 CRC32C 校验值。 |
| `backup_set_path` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 最近一次 backup set 的文件系统路径。 |
| `manifest_path` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 最近一次成功发布的 cluster manifest 路径。 |

示例：

```sql
SELECT * FROM pg_cluster_backup_history LIMIT 50;
```

## `pg_cluster_restore_points`

可用于 cluster PITR 的 restore point。

- 行基数：每个已记录 restore point 一行。
- 刷新来源：restore-point 目录快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `restore_point_name` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 关联的 restore point 名称。 |
| `cut_scn` | `bigint` | SCN | 0：通常未建立有效水位，不作为终态证明 | 恢复点记录的一致性 cut SCN。 |
| `thread_count` | `integer` | 个 | 未活动/未记录时可能为 0；0 不单独证明健康 | 该备份或恢复点覆盖的 WAL 线程数量。 |
| `incarnation` | `integer` | — | 未活动/未记录时可能为 0；0 不单独证明健康 | 创建 restore point 时记录的 cluster incarnation。 |
| `created_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 恢复点被记录的时间。 |

示例：

```sql
SELECT * FROM pg_cluster_restore_points LIMIT 50;
```

## `pg_cluster_pitr_status`

当前 cluster PITR target 的解析结果。

- 行基数：固定一行。
- 刷新来源：PITR 配置与 manifest 的查询时解析。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `target_type` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | PITR target 的类型。 |
| `target_action` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 到达 target 后执行 pause、promote 或 shutdown。 |
| `reachable` | `boolean` | 布尔 | false 表示该条件当前不成立 | 当前 PITR target 是否能由已知证据解析并到达。 |
| `reason` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 当前结论或拒绝的可读原因；空值表示没有附加原因。 |
| `resolved_scn` | `bigint` | SCN | 0：通常未建立有效水位，不作为终态证明 | target 可达时最终选择的 restore-point SCN。 |
| `restore_point_name` | `text` | — | 空串/NULL/unknown 不等于成功，见本字段说明 | 关联的 restore point 名称。 |

示例：

```sql
SELECT * FROM pg_cluster_pitr_status LIMIT 50;
```
