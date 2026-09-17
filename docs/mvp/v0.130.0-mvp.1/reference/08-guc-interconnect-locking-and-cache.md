# Interconnect、GES/GCS、锁与缓存参数

Author: SqlRush <sqlrush@gmail.com>

适用版本：`v0.130.0-mvp.1`。返回[手册目录](../README.md)。


每个条目固定来自 `v0.130.0-mvp.1` 注册表；测试参数单独标注。`postmaster` 表示修改后必须重启；`sighup` 表示 reload 生效；
`superuser`/`user` 表示可以在允许的 SQL 配置范围内修改。实际当前值请以 `pg_settings` 为准。

## `cluster.interconnect_tier`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `stub` |
| 范围/枚举 | stub, mock, tier1, tier2, tier3 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cluster interconnect tier vtable selection. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_tier';
```

## `cluster.interconnect_rdma_fallback`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `auto` |
| 范围/枚举 | auto, off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Policy for RDMA-to-TCP interconnect fallback. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_rdma_fallback';
```

## `cluster.interconnect_rdma_provider`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `auto` |
| 范围/枚举 | auto, verbs, mlx5 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | RDMA provider selection for tier2/tier3 interconnect. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_rdma_provider';
```

## `cluster.interconnect_rdma_completion`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `event` |
| 范围/枚举 | event, busypoll |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | RDMA completion model for the interconnect. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_rdma_completion';
```

## `cluster.interconnect_rdma_busypoll_us`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `50` |
| 范围/枚举 | 0 .. 10000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 微秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | RDMA busy-poll spin budget in microseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_rdma_busypoll_us';
```

## `cluster.interconnect_rdma_crc_offload`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Reserved RDMA control-plane CRC offload switch. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：预留开关；on 被拒绝，应用层 CRC32C 始终保留。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_rdma_crc_offload';
```

## `cluster.interconnect_rdma_inline_max`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `256` |
| 范围/枚举 | 0 .. 4096 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 字节 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | RDMA inline-send threshold in bytes. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_rdma_inline_max';
```

## `cluster.interconnect_rdma_max_send_wr`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `256` |
| 范围/枚举 | 16 .. 4096 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | RDMA send work request queue depth per peer. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_rdma_max_send_wr';
```

## `cluster.read_scache`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable quiescent-block S-caching via X->S downgrade. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.read_scache';
```

## `cluster.ges_handoff`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Verify the GES release-side deterministic handoff invariants. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_handoff';
```

## `cluster.crossnode_cr_data_plane`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable the cross-instance CR-server data plane (当前实现). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.crossnode_cr_data_plane';
```

## `cluster.crossnode_runtime_visibility`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable active-runtime cross-instance recycled-slot visibility resolution (当前实现). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.crossnode_runtime_visibility';
```

## `cluster.ges_bast`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Send a BAST nudge to a live X holder blocking a peer writer (当前实现). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_bast';
```

## `cluster.past_image`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Keep a Past Image copy when a block is transferred or invalidated (当前实现). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.past_image';
```

## `cluster.grd_max_entries`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1024` |
| 范围/枚举 | 0 .. 1048576 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum number of cluster_grd entry table slots. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.grd_max_entries';
```

## `cluster.grd_entry_reclaim`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable safe cold reclaim for GRD resource entries. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.grd_entry_reclaim';
```

## `cluster.grd_entry_reclaim_max_per_sweep`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `256` |
| 范围/枚举 | 0 .. 65536 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum GRD cold entries reclaimed by one LMON sweep. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.grd_entry_reclaim_max_per_sweep';
```

## `cluster.ges_starvation_max_skips`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `8` |
| 范围/枚举 | 0 .. 1000000 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Skip count after which a starved GES waiter is boosted to head-of-line. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_starvation_max_skips';
```

## `cluster.ges_starvation_protection`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enables GES enqueue lock-starvation fairness protection. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：变更时有 assign hook 同步运行状态

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_starvation_protection';
```

## `cluster.ges_request_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `60000` |
| 范围/枚举 | -1 .. 600000 |
| Context | `user` |
| 生效方式 | 普通会话可修改 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Timeout for cross-node GES grant request (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：-1 只在 cluster.ges_retransmit_max_attempts > 0 时合法。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_request_timeout_ms';
```

## `cluster.cf_enqueue_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30000` |
| 范围/枚举 | 1000 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Timeout for acquiring the shared control-file (CF) enqueue (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cf_enqueue_timeout_ms';
```

## `cluster.ges_retransmit_max_attempts`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5` |
| 范围/枚举 | 0 .. 50 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum GES REQUEST/RELEASE retransmit attempts before fail-closed. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：不能在 ges_request_timeout_ms=-1 时设置为 0。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_retransmit_max_attempts';
```

## `cluster.ges_dedup_max_entries`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `8192` |
| 范围/枚举 | 256 .. 1048576 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | LMS-owned GES retransmit dedup HTAB capacity (entries). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_dedup_max_entries';
```

## `cluster.ges_convert_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30000` |
| 范围/枚举 | 1000 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Finite wait for a cross-node lock-conversion (convert) grant reply. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_convert_timeout_ms';
```

## `cluster.grd_remaster_wait_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `200` |
| 范围/枚举 | 0 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Short wait on a GRD shard frozen by failure-driven remaster (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.grd_remaster_wait_ms';
```

## `cluster.grd_rebuild_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 100 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Holder-rebuild barrier deadline after a failure-driven remaster (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.grd_rebuild_timeout_ms';
```

## `cluster.lmd_probe_collect_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `3000` |
| 范围/枚举 | 100 .. 30000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Coordinator DEADLOCK_REPORT collect deadline (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lmd_probe_collect_timeout_ms';
```

## `cluster.ges_reply_wait_max_entries`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1024` |
| 范围/枚举 | 64 .. 65536 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cap on the cross-node GES reply wait HTAB (5-tuple key). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_reply_wait_max_entries';
```

## `cluster.lmd_cleanup_sweep_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | LMD periodic dead-backend cleanup sweep interval (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lmd_cleanup_sweep_interval_ms';
```

## `cluster.lms_native_lock_probe_max_inflight`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `8` |
| 范围/枚举 | 1 .. 64 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Per-shard LMS native-lock probe collector slot capacity. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lms_native_lock_probe_max_inflight';
```

## `cluster.lms_native_lock_probe_retry_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `500` |
| 范围/枚举 | 50 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | LMS native-lock probe retry-poll cadence when peers return HOLDER_CONFLICT / WAITER_CONFLICT / timeout. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lms_native_lock_probe_retry_interval_ms';
```

## `cluster.lms_native_lock_probe_retry_budget`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `60` |
| 范围/枚举 | 1 .. 3600 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cumulative retry budget per requester before native-lock probe fails closed with 53R83. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lms_native_lock_probe_retry_budget';
```

## `cluster.ges_bast_retry_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `10000` |
| 范围/枚举 | 1000 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | BAST retry interval (ms) when holder is non-responsive. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_bast_retry_interval_ms';
```

## `cluster.ges_bast_max_retries`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `3` |
| 范围/枚举 | 1 .. 10 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum BAST retry attempts before REJECT to new requester. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_bast_max_retries';
```

## `cluster.ges_deadlock_check_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 10000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Deadlock probe periodic interval (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_deadlock_check_interval_ms';
```

## `cluster.ges_deadlock_chunk_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `2000` |
| 范围/枚举 | 500 .. 30000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Deadlock probe chunked reassembly timeout (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_deadlock_chunk_timeout_ms';
```

## `cluster.ges_deadlock_max_edges`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1024` |
| 范围/枚举 | 64 .. 65536 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Deadlock graph max edges per probe. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_deadlock_max_edges';
```

## `cluster.ges_deadlock_max_vertices`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `256` |
| 范围/枚举 | 16 .. 16384 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Deadlock graph max vertices per probe. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_deadlock_max_vertices';
```

## `cluster.ges_deadlock_max_in_flight_probes`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `4` |
| 范围/枚举 | 1 .. 32 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Max concurrent in-flight deadlock probes per coordinator. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_deadlock_max_in_flight_probes';
```

## `cluster.ges_deadlock_tick_budget_us`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 500 .. 50000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 微秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Max time(us)budget for deadlock work per LMON tick. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ges_deadlock_tick_budget_us';
```

## `cluster.pcm_grd_max_entries`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `-1` |
| 范围/枚举 | -1 .. 1048576 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum entries in the PCM GRD master shmem region. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.pcm_grd_max_entries';
```

## `cluster.hang_manager_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enables the DIAG-hosted Hang Manager long-wait sampler. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hang_manager_enabled';
```

## `cluster.hang_sample_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `10000` |
| 范围/枚举 | 100 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Interval between Hang Manager long-wait sampling rounds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hang_sample_interval_ms';
```

## `cluster.hang_threshold_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `60000` |
| 范围/枚举 | 1000 .. 86400000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Wait duration at/over which a backend is reported as a hang. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hang_threshold_ms';
```

## `cluster.hang_dump_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enables Hang Manager long-wait LOG-once and dump accounting. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hang_dump_enabled';
```

## `cluster.hang_max_chain_depth`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `100` |
| 范围/枚举 | 1 .. 10000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum wait-chain depth the Hang Manager walks before stopping. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hang_max_chain_depth';
```

## `cluster.hang_max_sampled`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `64` |
| 范围/枚举 | 1 .. 64 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum long-wait samples kept per round (top-N by duration). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hang_max_sampled';
```

## `cluster.hang_resolution_mode`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `advisory` |
| 范围/枚举 | off, advisory, enforce |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Hang Manager disposition mode (off / advisory / enforce). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hang_resolution_mode';
```

## `cluster.hang_resolution_confirm_rounds`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `2` |
| 范围/枚举 | 1 .. 100 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Consecutive rounds a victim identity must stay an actionable long-wait before disposition (hysteresis). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hang_resolution_confirm_rounds';
```

## `cluster.hang_resolution_soft_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 100 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Grace period between disposition tiers (cancel -> terminate -> degrade). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hang_resolution_soft_timeout_ms';
```

## `cluster.hang_resolution_max_per_round`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1` |
| 范围/枚举 | 1 .. 64 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum number of victims disposed per evaluation round. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hang_resolution_max_per_round';
```

## `cluster.hang_victim_w_age`

| 属性 | 值 |
|---|---|
| 类型 | `real` |
| 默认值 | `0.5` |
| 范围/枚举 | 0.0 .. 1000.0 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Victim score weight for transaction age. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hang_victim_w_age';
```

## `cluster.hang_victim_w_rollback`

| 属性 | 值 |
|---|---|
| 类型 | `real` |
| 默认值 | `0.3` |
| 范围/枚举 | 0.0 .. 1000.0 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Victim score weight for rollback cost (proxied by held lock count). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hang_victim_w_rollback';
```

## `cluster.hang_victim_w_blockers`

| 属性 | 值 |
|---|---|
| 类型 | `real` |
| 默认值 | `0.2` |
| 范围/枚举 | 0.0 .. 1000.0 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Victim score weight for root-ness (number of waiters blocked). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hang_victim_w_blockers';
```

## `cluster.interconnect_heartbeat_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Tier1 IC heartbeat tick interval in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_heartbeat_interval_ms';
```

## `cluster.interconnect_connect_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 1000 .. 60000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Tier1 IC active-connect SO_ERROR poll timeout in ms. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_connect_timeout_ms';
```

## `cluster.interconnect_recv_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30000` |
| 范围/枚举 | 1000 .. 600000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Tier1 IC per-peer recv read deadline in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_recv_timeout_ms';
```

## `cluster.interconnect_payload_max_bytes`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `67108864（64 MiB）` |
| 范围/枚举 | 16 * 1024 * 1024 .. 256 * 1024 * 1024 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 字节 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum cluster_ic_send_envelope_chunked payload bytes. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_payload_max_bytes';
```

## `cluster.interconnect_chunk_reassembly_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `10000` |
| 范围/枚举 | 1000 .. 60000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Chunked reassembly partial-frame timeout in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_chunk_reassembly_timeout_ms';
```

## `cluster.interconnect_tcp_keepidle_sec`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `60` |
| 范围/枚举 | 30 .. 600 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Tier1 TCP_KEEPIDLE socket option in seconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_tcp_keepidle_sec';
```

## `cluster.interconnect_tcp_keepintvl_sec`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `10` |
| 范围/枚举 | 10 .. 60 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Tier1 TCP_KEEPINTVL socket option in seconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_tcp_keepintvl_sec';
```

## `cluster.interconnect_tcp_keepcnt`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `6` |
| 范围/枚举 | 3 .. 20 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Tier1 TCP_KEEPCNT socket option (probe count). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.interconnect_tcp_keepcnt';
```

## `cluster.cr_mvcc_gate`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `user` |
| 生效方式 | 普通会话可修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable the own-instance CR 3-tier MVCC short-circuit gate. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cr_mvcc_gate';
```

## `cluster.cr_gate_no_peer_fastpath`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `user` |
| 生效方式 | 普通会话可修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Skip the CR/SCN cluster visibility fork for a no-peer, session-local snapshot (use the PG-native MVCC body). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cr_gate_no_peer_fastpath';
```

## `cluster.cr_tuple_level_fastpath`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `user` |
| 生效方式 | 普通会话可修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Use the tuple-level / verdict-only CR read fast path for a single-candidate-chain block (compute-only; default off). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cr_tuple_level_fastpath';
```

## `cluster.cf_terminal_authority`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable 当前实现 Cache Fusion durable TT/undo terminal authority. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cf_terminal_authority';
```

## `cluster.cf_delayed_cleanout`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `reader` |
| 范围/枚举 | off, reader, eager |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Choose the 当前实现 delayed ITL cleanout policy. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cf_delayed_cleanout';
```

## `cluster.smart_fusion`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Guarded 当前实现 Smart Fusion early block-transfer dependency tracking. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：on 被配置校验拒绝；当前只能保持 off。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.smart_fusion';
```

## `cluster.smart_fusion_tier_min`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `tier3` |
| 范围/枚举 | tier3 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Minimum interconnect tier that may use Smart Fusion early transfer. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.smart_fusion_tier_min';
```

## `cluster.smart_fusion_commit_brake_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 1 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Timeout for the 当前实现 Smart Fusion pre-commit dependency brake. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.smart_fusion_commit_brake_timeout_ms';
```

## `cluster.smart_fusion_origin_durable_gossip_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `50` |
| 范围/枚举 | 1 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Interval for publishing local durable WAL progress to Smart Fusion peers. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.smart_fusion_origin_durable_gossip_ms';
```

## `cluster.cr_cache_max_blocks`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `64` |
| 范围/枚举 | 0 .. 4096 |
| Context | `user` |
| 生效方式 | 普通会话可修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Backend-local CR block cache capacity in 8 KB blocks (0 disables). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cr_cache_max_blocks';
```

## `cluster.shared_cr_pool_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable the dedicated shared (cross-backend) CR buffer pool (L2). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.shared_cr_pool_enabled';
```

## `cluster.shared_cr_pool_size_blocks`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `0` |
| 范围/枚举 | 0 .. 262144 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Shared CR buffer pool capacity in 8 KB blocks (0 = disabled / zero memory). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.shared_cr_pool_size_blocks';
```

## `cluster.cr_pool_rel_generation_slots`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `0` |
| 范围/枚举 | 0 .. 262144 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Per-relation CR lifecycle generation table size (0 = disabled; coarse). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cr_pool_rel_generation_slots';
```

## `cluster.cr_pool_admission_policy`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `admit_all` |
| 范围/枚举 | admit_all, no_admit, scan_resistant |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Insert-side admission policy for the shared CR buffer pool. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cr_pool_admission_policy';
```

## `cluster.cr_pool_admit_relation_backend_cap`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `0` |
| 范围/枚举 | 0 .. 1048576 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Per-backend cap on CR pool admits for a single relation (0 disables). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cr_pool_admit_relation_backend_cap';
```

## `cluster.cr_pool_admit_pressure_ratio`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `0` |
| 范围/枚举 | 0 .. 100000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | CR pool evict:hit pressure threshold (percent) for admission throttling (0 disables). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cr_pool_admit_pressure_ratio';
```

## `cluster.cross_instance_cr_coordinator`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `boundary` |
| 范围/枚举 | off, boundary, forward |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Observability mode for the cross-instance CR read-path coordinator boundary. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cross_instance_cr_coordinator';
```

## `cluster.cross_instance_cr_probe`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `user` |
| 生效方式 | 普通会话可修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Count class③ runtime-warm cross-instance CR hits for the 当前实现 measure-leg. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cross_instance_cr_probe';
```

## `cluster.lmd_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable the LMD (Lock Manager Daemon — deadlock detection actor) cluster background process. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lmd_enabled';
```

## `cluster.lms_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable the LMS (Lock Master Server) cluster grant decision daemon. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lms_enabled';
```

## `cluster.lms_workers`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `2` |
| 范围/枚举 | 1 .. 8 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Number of LMS DATA-plane workers (including worker 0). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lms_workers';
```

## `cluster.lms_nice`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `0` |
| 范围/枚举 | -20 .. 0 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Nice value applied to the LMS DATA-plane workers (0 = leave alone). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lms_nice';
```

## `cluster.lock_acquire_cluster_path`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable the cluster lock acquire gate path. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lock_acquire_cluster_path';
```

## `cluster.local_fast_path_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable the S3 local-fast-path 5-check (local master + no remote holder/waiter/convert + generation stable). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.local_fast_path_enabled';
```

## `cluster.lmd_max_wait_edges`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1024` |
| 范围/枚举 | 64 .. 65536 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum LMD wait-for graph edges. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lmd_max_wait_edges';
```

## `cluster.lmd_scan_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 50 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | LMD Tarjan scan loop period (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lmd_scan_interval_ms';
```

## `cluster.deadlock_detection_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable coordinator-driven cross-node deadlock detection. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.deadlock_detection_enabled';
```

## `cluster.deadlock_confirm_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `500` |
| 范围/枚举 | 50 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Delay between the two coordinator deadlock-confirm rounds (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.deadlock_confirm_interval_ms';
```

## `cluster.gcs_reply_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | GCS block-ship request reply timeout (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.gcs_reply_timeout_ms';
```

## `cluster.gcs_block_retransmit_max_retries`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `4` |
| 范围/枚举 | 0 .. 8 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum retry attempts after initial GCS block-ship reply timeout. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.gcs_block_retransmit_max_retries';
```

## `cluster.gcs_block_local_cache`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Hold PCM block locks until revoked (node-level cache). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.gcs_block_local_cache';
```

## `cluster.tx_enqueue_wait`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Block on a remote row lock until the holder completes. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.tx_enqueue_wait';
```

## `cluster.crossnode_write_write`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Chain a local write past a terminal remote writer. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.crossnode_write_write';
```

## `cluster.gcs_block_retransmit_initial_backoff_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `10` |
| 范围/枚举 | 1 .. 5000 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Initial backoff before retry 1 (subsequent retries double). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.gcs_block_retransmit_initial_backoff_ms';
```

## `cluster.gcs_block_dedup_max_entries`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `16384` |
| 范围/枚举 | 256 .. 65536 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Master-side GCS block dedup HTAB capacity (entries). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.gcs_block_dedup_max_entries';
```

## `cluster.gcs_block_invalidate_ack_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1500` |
| 范围/枚举 | 100 .. 60000 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | CF 3-way master deadline for a single INVALIDATE_ACK. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.gcs_block_invalidate_ack_timeout_ms';
```

## `cluster.gcs_block_starvation_backoff_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `100` |
| 范围/枚举 | 1 .. 60000 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | S barrier reader backoff base for DENIED_PENDING_X retry. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.gcs_block_starvation_backoff_ms';
```

## `cluster.gcs_block_starvation_max_retries`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `8` |
| 范围/枚举 | 0 .. 64 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Legacy S barrier reader retry budget compatibility setting. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.gcs_block_starvation_max_retries';
```

## `cluster.gcs_block_lost_write_action`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `error` |
| 范围/枚举 | error, warn |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Action when GCS block ship triggers lost-write detection. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.gcs_block_lost_write_action';
```

## `cluster.sinval_broadcast_batch_size`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `32` |
| 范围/枚举 | 1 .. 64 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Outbound sinval batch drain upper bound. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.sinval_broadcast_batch_size';
```

## `cluster.sinval_broadcast_batch_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `10` |
| 范围/枚举 | 1 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | SI Broadcaster main loop WaitLatch timeout (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.sinval_broadcast_batch_timeout_ms';
```

## `cluster.sinval_broadcast_max_queue_size`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1024` |
| 范围/枚举 | 64 .. 65536 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Outbound + inbound queue ring buffer capacity. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.sinval_broadcast_max_queue_size';
```

## `cluster.sinval_ack_mode`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `peer_enqueued` |
| 范围/枚举 | none, peer_enqueued |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Sinval propagation ack/barrier mode. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.sinval_ack_mode';
```

## `cluster.sinval_ack_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Sinval ack wait timeout in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.sinval_ack_timeout_ms';
```

## `cluster.sinval_ack_wait_slots`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `256` |
| 范围/枚举 | 64 .. 4096 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Capacity of ClusterSinvalAckWait HTAB. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.sinval_ack_wait_slots';
```
