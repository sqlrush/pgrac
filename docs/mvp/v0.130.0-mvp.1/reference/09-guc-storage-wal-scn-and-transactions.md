# 存储、WAL、SCN、Undo 与事务参数

Author: SqlRush <sqlrush@gmail.com>

适用版本：`v0.130.0-mvp.1`。返回[手册目录](../README.md)。


每个条目固定来自 `v0.130.0-mvp.1` 注册表；测试参数单独标注。`postmaster` 表示修改后必须重启；`sighup` 表示 reload 生效；
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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.wal_receiver_timeout_sec';
```

## `cluster.page_scn_shortcut`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable the cross-node visibility resolver terminal-outcome memo. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

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

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.sequence_refill_timeout_ms';
```
