# 存储、WAL、SCN、Undo 与事务参数

每个条目均来自当前 `cluster.*` 注册表。`postmaster` 表示修改后必须重启；`sighup` 表示 reload 生效；
`superuser`/`user` 表示可以在允许的 SQL 配置范围内修改。实际当前值请以 `pg_settings` 为准。

## `cluster.wal_threads_dir`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `空字符串` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Shared-storage root directory of the per-thread WAL layout. |

工作原理与调节影响：设置 WAL / `threads` / `dir`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：空值使用平面 pg_wal；非空值必须是绝对路径并通过启动期 ownership 校验。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.wal_threads_dir';
```

## `cluster.wal_sender_timeout_sec`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `60` |
| 范围/枚举 | 1 .. 3600 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | ADG LNS WAL sender timeout. |

工作原理与调节影响：定义 WAL / `sender` / 超时 / 秒 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.wal_sender_timeout_sec';
```

## `cluster.wal_receiver_timeout_sec`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `60` |
| 范围/枚举 | 1 .. 3600 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | ADG RFS WAL receiver timeout. |

工作原理与调节影响：定义 WAL / `receiver` / 超时 / 秒 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.wal_receiver_timeout_sec';
```

## `cluster.page_scn_shortcut`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable the cross-node visibility resolver terminal-outcome memo. |

工作原理与调节影响：默认开启页面 / SCN / `shortcut` 路径，在同一后端按精确事务身份复用已由权威 TT/undo 路径证明的 COMMITTED（含有效 SCN）或 ABORTED 终态，避免对同一事务逐行重复查询。UNKNOWN、进行中、身份不完整或身份域发生变化的结果不会缓存；安全拒绝条件不因关闭开关而消失。需要兼容旧逐 tuple 路径或做基线对比时才显式设为 `off`。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.page_scn_shortcut';
```

## `cluster.xid_striping`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Stripe xid allocation into per-node congruence classes (当前实现). |

工作原理与调节影响：开关 `xid` / `striping` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.xid_striping';
```

## `cluster.xid_herding_slack`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `4194304` |
| 范围/枚举 | 65536 .. 268435456 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Allowed xid gap between stripe slots before herding jumps (当前实现). |

工作原理与调节影响：设置 `xid` / `herding` / `slack`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.xid_herding_slack';
```

## `cluster.space_affinity`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `off` |
| 范围/枚举 | off, static, dynamic |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Instance space-affinity mode for cluster relation extends. |

工作原理与调节影响：选择 `space` / `affinity` 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

依赖与保护：dynamic 当前被配置校验拒绝；可用值为 off/static。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.space_affinity';
```

## `cluster.space_lease_blocks`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `64` |
| 范围/枚举 | 1 .. 8192 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Blocks handed to a node per HW space lease. |

工作原理与调节影响：设置 `space` / 租约 / `blocks`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.space_lease_blocks';
```

## `cluster.shared_storage_backend`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `stub` |
| 范围/枚举 | stub, local, block_device, cluster_fs, rbd, multi_attach |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cluster shared-storage backend selection. |

工作原理与调节影响：选择 共享 / 存储 / `backend` 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.shared_storage_backend';
```

## `cluster.shared_data_dir`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `空字符串` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Shared data root for the cluster_fs shared-storage backend. |

工作原理与调节影响：设置 共享 / 数据 / `dir`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：非空值必须是绝对路径；所有节点需指向同一共享挂载。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.shared_data_dir';
```

## `cluster.undo_gcs_coherence`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable the shared-undo block GCS data plane (当前实现). |

工作原理与调节影响：开关 undo / `gcs` / `coherence` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：on 要求 cluster.shared_data_dir 非空。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_gcs_coherence';
```

## `cluster.shared_storage_uuid`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `空字符串` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Optional external identity for the cluster_fs shared root. |

工作原理与调节影响：设置 共享 / 存储 / `uuid`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.shared_storage_uuid';
```

## `cluster.block_device_path`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `空字符串` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Raw block-device path for the block_device shared-storage backend. |

工作原理与调节影响：设置 数据块 / `device` / `path`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：非空值必须是绝对路径。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.block_device_path';
```

## `cluster.block_device_use_odirect`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Require direct I/O for the raw block-device backend. |

工作原理与调节影响：开关 数据块 / `device` / `use` / `odirect` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.block_device_use_odirect';
```

## `cluster.smgr_user_relations`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Route permanent relations through cluster_smgr instead of md.c. |

工作原理与调节影响：开关 `smgr` / `user` / `relations` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.smgr_user_relations';
```

## `cluster.shared_catalog`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Route system catalogs through a single shared authority. |

工作原理与调节影响：开关 共享 / `catalog` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：要求 shared relation、shared data root 与 shared control authority 的组合配置通过启动校验。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.shared_catalog';
```

## `cluster.oid_lease_size`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `8192` |
| 范围/枚举 | 1024 .. 1048576 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Number of OIDs a node leases at a time from the shared OID authority. |

工作原理与调节影响：设置 `oid` / 租约 / 大小。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.oid_lease_size';
```

## `cluster.undo_retention_horizon_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Retain committed undo / TT slots until no live reader needs them. |

工作原理与调节影响：开关 undo / 保留 / `horizon` / 启用状态 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_retention_horizon_enabled';
```

## `cluster.undo_buffers`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `2048` |
| 范围/枚举 | 0 .. 1048576 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Number of ordinary DATA frames and R4A block-zero frames. |

工作原理与调节影响：限定 undo / `buffers` 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_buffers';
```

## `cluster.undo_buffer_writeback`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable buffered write-back for the undo buffer pool. |

工作原理与调节影响：开关 undo / 缓冲 / `writeback` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：设置时由 `cluster_undo_buffer_writeback_check_hook` 校验

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_buffer_writeback';
```

## `cluster.undo_writeback_boundary_check`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `on` |
| 范围/枚举 | off, on, strict |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Advisory layer of the undo checkpoint-writeback boundary contract. |

工作原理与调节影响：选择 undo / `writeback` / `boundary` / `check` 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_writeback_boundary_check';
```

## `cluster.undo_cleaner_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30000` |
| 范围/枚举 | 0 .. 3600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Undo Cleaner pass interval in milliseconds. |

工作原理与调节影响：定义 undo / `cleaner` / `interval` / `ms` 的运行周期。调小反应更快但增加 CPU、网络或 I/O 频率；调大降低开销但延长发现/处理延迟。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_cleaner_interval_ms';
```

## `cluster.undo_cleaner_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable proactive undo/TT-slot retention GC passes. |

工作原理与调节影响：控制主动 undo/TT 清理。`cluster.shared_config=on` 时必须保持 `on`；COMMON 配置的创建、变更、启动和 reload 均拒绝 `off`。非 shared 配置允许关闭，进程仍驻留，清理 pass 不执行。

依赖与保护：GUC check hook 拒绝在 shared 配置下关闭；启动及共享配置应用还会检查最终值，不能依靠参数加载顺序绕过约束。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_cleaner_enabled';
```

## `cluster.undo_cleaner_batch_segments`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `8` |
| 范围/枚举 | 1 .. 256 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Max own-instance undo segments scanned per cleaner pass. |

工作原理与调节影响：设置 undo / `cleaner` / `batch` / `segments`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_cleaner_batch_segments';
```

## `cluster.undo_record_segment_commit_on_rollover`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Advance a drained record undo segment ACTIVE -> COMMITTED on rollover. |

工作原理与调节影响：开关 undo / `record` / `segment` / 提交 / `on` / `rollover` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_record_segment_commit_on_rollover';
```

## `cluster.relation_extend_lock_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Extend permanent shared relations through the cluster block-number authority. |

工作原理与调节影响：开关 `relation` / `extend` / 锁 / 启用状态 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.relation_extend_lock_enabled';
```

## `cluster.tablespace_ddl_lock_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Serialise tablespace DDL (CREATE/DROP/ALTER/RENAME) across the cluster. |

工作原理与调节影响：开关 `tablespace` / `ddl` / 锁 / 启用状态 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.tablespace_ddl_lock_enabled';
```

## `cluster.object_reuse_flush_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Flush a relation's buffers on every peer before its storage is removed or truncated. |

工作原理与调节影响：开关 `object` / `reuse` / 刷写 / 启用状态 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.object_reuse_flush_enabled';
```

## `cluster.undo_segments_per_instance`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `16` |
| 范围/枚举 | 1 .. 1024 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Reserved undo segment count per cluster instance. |

工作原理与调节影响：设置 undo / `segments` / 每 / `instance`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_segments_per_instance';
```

## `cluster.undo_tablespace_path`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `pg_undo` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Relative path under PGDATA for the per-instance undo tablespace. |

工作原理与调节影响：设置 undo / `tablespace` / `path`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_tablespace_path';
```

## `cluster.undo_segment_size_mb`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `32` |
| 范围/枚举 | 8 .. 1024 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Per-segment file size in MB. |

工作原理与调节影响：设置 undo / `segment` / 大小 / `mb`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_segment_size_mb';
```

## `cluster.undo_record_inline_max_bytes`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1024` |
| 范围/枚举 | 16 .. 8192 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 字节（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum inline payload size for a single undo record. |

工作原理与调节影响：限定 undo / `record` / 内联 / 上限 / 字节 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_record_inline_max_bytes';
```

## `cluster.undo_extent_blocks`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `4` |
| 范围/枚举 | 1 .. 256 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Undo block extent size claimed per transaction. |

工作原理与调节影响：设置 undo / `extent` / `blocks`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_extent_blocks';
```

## `cluster.undo_segments_max_per_instance`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `256` |
| 范围/枚举 | 16 .. 256 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Hard cap of per-instance undo segment pool size. |

工作原理与调节影响：限制本实例 undo 段池的增长上限。调大允许按需创建更多段，会增加潜在磁盘占用；不会直接增加后台进程数，也不会解除活动事务或保留历史的保护。reload 值低于已分配段数时会给出 WARNING，并以现有池大小为有效下限，不缩池或删除现有段。

依赖与保护：段池到顶时，尚未占槽的新共享事务在原安全执行上下文和清偿进展可证明时可以进入容量等待；其他分配路径或无法证明这些条件时仍可能返回原容量错误。到顶不保证立即报 `53R9E`，也不保证所有调用者都能等待。单段事务槽等待另受当前活动事务限制，增大段池上限不能保证消除此类等待。shared 配置必须保持 `cluster.undo_cleaner_enabled=on`，但开启 cleaner 不表示容量已经可复用。

观察入口：`pg_cluster_state` 的 `undo_cleaner` 容量诊断及 `pg_stat_activity` 的当前等待，见[容量等待采样](12-operations-and-query-cost.md#8-undo-容量等待采样)。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_segments_max_per_instance';
```

## `cluster.undo_segment_create_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Segment file create + initial fsync elapsed-time guard. |

工作原理与调节影响：定义 undo / `segment` / `create` / 超时 / `ms` 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.undo_segment_create_timeout_ms';
```

## `cluster.cr_chain_walk_max_steps`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `4096` |
| 范围/枚举 | 64 .. 65536 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Hard cap on undo chain walk steps per CR block construction. |

工作原理与调节影响：限定 `cr` / `chain` / `walk` / 上限 / `steps` 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cr_chain_walk_max_steps';
```

## `cluster.tt_durable_lookup`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `user` |
| 生效方式 | 普通会话可修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Resolve commit_scn from the durable undo-header TT slot on overlay miss. |

工作原理与调节影响：开关 `tt` / `durable` / `lookup` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.tt_durable_lookup';
```

## `cluster.scn_max_propagation_lag_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | SCN cross-instance propagation lag bound in milliseconds. |

工作原理与调节影响：限定 SCN / 上限 / `propagation` / 延迟 / `ms` 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.scn_max_propagation_lag_ms';
```

## `cluster.tt_status_overlay_max_entries`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `32768` |
| 范围/枚举 | 1024 .. 1048576 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Capacity of cluster Undo TT status overlay HTAB (当前实现 D2). |

工作原理与调节影响：限定 `tt` / 状态 / `overlay` / 上限 / `entries` 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.tt_status_overlay_max_entries';
```

## `cluster.tt_status_overlay_ttl_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30000` |
| 范围/枚举 | 1000 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | TTL in milliseconds for cluster Undo TT status overlay entries. |

工作原理与调节影响：设置 `tt` / 状态 / `overlay` / `ttl` / `ms`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.tt_status_overlay_ttl_ms';
```

## `cluster.subtrans_max_chain_depth`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `32` |
| 范围/枚举 | 4 .. 1024 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Bounded depth for cluster SUBTRANS reader lazy parent_key follow. |

工作原理与调节影响：限定 `subtrans` / 上限 / `chain` / `depth` 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.subtrans_max_chain_depth';
```

## `cluster.multixact_member_overlay_max_members`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `32` |
| 范围/枚举 | 4 .. 256 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Per-message hard cap on V4 sidecar wire member_count. |

工作原理与调节影响：限定 `multixact` / `member` / `overlay` / 上限 / `members` 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.multixact_member_overlay_max_members';
```

## `cluster.multixact_member_overlay_max_entries`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `16384` |
| 范围/枚举 | 1024 .. 1048576 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Capacity of cluster MultiXact member overlay HTAB (当前实现 D2). |

工作原理与调节影响：限定 `multixact` / `member` / `overlay` / 上限 / `entries` 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.multixact_member_overlay_max_entries';
```

## `cluster.multixact_hint_outbound_slots`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1024` |
| 范围/枚举 | 128 .. 8192 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | V4 sidecar outbound queue slot count (当前实现 D4). |

工作原理与调节影响：限定 `multixact` / `hint` / `outbound` / `slots` 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.multixact_hint_outbound_slots';
```

## `cluster.tt_status_hint_outbound_capacity`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `256` |
| 范围/枚举 | 64 .. 4096 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Capacity of cluster TT status hint outbound ring (当前实现 D3). |

工作原理与调节影响：限定 `tt` / 状态 / `hint` / `outbound` / `capacity` 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.tt_status_hint_outbound_capacity';
```

## `cluster.tt_status_hint_emit_mode`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `all_status` |
| 范围/枚举 | disabled, all_status |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Emit mode for cross-node TT status hint propagation (当前实现 D7). |

工作原理与调节影响：选择 `tt` / 状态 / `hint` / `emit` / 模式 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.tt_status_hint_emit_mode';
```

## `cluster.sequence_default_cache`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `100` |
| 范围/枚举 | 1 .. 1000000000 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Default CACHE size injected into new sequences in cluster mode. |

工作原理与调节影响：设置 `sequence` / `default` / 缓存。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.sequence_default_cache';
```

## `cluster.sequence_cache_floor_optin`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `0` |
| 范围/枚举 | 0 .. 1000000000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Opt-in runtime floor for an existing sequence's CACHE size. |

工作原理与调节影响：设置 `sequence` / 缓存 / `floor` / `optin`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.sequence_cache_floor_optin';
```

## `cluster.sequence_refill_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30000` |
| 范围/枚举 | 1000 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum wait for an SQ sequence segment refill before failing closed. |

工作原理与调节影响：定义 `sequence` / `refill` / 超时 / `ms` 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.sequence_refill_timeout_ms';
```
