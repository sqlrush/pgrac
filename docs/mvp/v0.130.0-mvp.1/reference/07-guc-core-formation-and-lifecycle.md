# 核心、formation 与生命周期参数

Author: SqlRush <sqlrush@gmail.com>

适用版本：`v0.130.0-mvp.1`。返回[手册目录](../README.md)。


每个条目固定来自 `v0.130.0-mvp.1` 注册表；测试参数单独标注。`postmaster` 表示修改后必须重启；`sighup` 表示 reload 生效；
`superuser`/`user` 表示可以在允许的 SQL 配置范围内修改。实际当前值请以 `pg_settings` 为准。

## `cluster.node_id`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `-1` |
| 范围/枚举 | -1 .. 127 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Numeric identifier of this node in the cluster. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.node_id';
```

## `cluster.config_file`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `pgrac.conf` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Path to the pgrac cluster topology configuration file. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.config_file';
```

## `cluster.apply_master_election`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable automatic ADG Apply Master election. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.apply_master_election';
```

## `cluster.apply_master_switch_drain_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 0 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | ADG Apply Master switch drain window. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.apply_master_switch_drain_ms';
```

## `cluster.multi_xmax_remote_resolve`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Resolve foreign multixact xmax through the cluster member overlay (当前实现). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.multi_xmax_remote_resolve';
```

## `cluster.block_self_contained`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Deprecated compatibility setting; changing it has no effect. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：兼容参数；on/off 在当前实现中行为相同。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.block_self_contained';
```

## `cluster.controlfile_shared_authority`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Use a single shared pg_control authority under cluster.shared_data_dir. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.controlfile_shared_authority';
```

## `cluster.shmem_max_regions`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `96` |
| 范围/枚举 | 40（启用 injection 的构建为 41） .. 256 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Capacity of the pgrac cluster shmem region registry. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.shmem_max_regions';
```

## `cluster.tm_convert_mode`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `convert` |
| 范围/枚举 | convert, additive |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | How a same-backend TM table-lock upgrade is routed across nodes. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.tm_convert_mode';
```

## `cluster.hw_remaster_retry_backoff_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Initial backoff before retrying a BLOCKED HW remaster worker (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hw_remaster_retry_backoff_ms';
```

## `cluster.hw_remaster_retry_max_attempts`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `16` |
| 范围/枚举 | 0 .. 1000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum same-episode retries for a BLOCKED HW remaster worker. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hw_remaster_retry_max_attempts';
```

## `cluster.phase1_timeout`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `60` |
| 范围/枚举 | 1 .. 3600 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Phase 1 (cluster basics) transition timeout in seconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.phase1_timeout';
```

## `cluster.phase2_timeout`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30` |
| 范围/枚举 | 1 .. 3600 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Phase 2 (lock services) transition timeout in seconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.phase2_timeout';
```

## `cluster.phase3_timeout`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `600` |
| 范围/枚举 | 60 .. 3600 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Phase 3 (recovery) transition timeout in seconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.phase3_timeout';
```

## `cluster.phase4_timeout`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30` |
| 范围/枚举 | 1 .. 3600 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Phase 4 (normal startup) transition timeout in seconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.phase4_timeout';
```

## `cluster.lmon_main_loop_interval`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | LMON main-loop tick interval in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lmon_main_loop_interval';
```

## `cluster.lmon_slow_iteration_warn_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 0 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | LMON main-loop slow-iteration warning threshold in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lmon_slow_iteration_warn_ms';
```

## `cluster.lck_main_loop_interval`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | LCK main-loop tick interval in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lck_main_loop_interval';
```

## `cluster.diag_main_loop_interval`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | DIAG main-loop tick interval in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.diag_main_loop_interval';
```

## `cluster.cluster_stats_main_loop_interval`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cluster Stats main-loop tick interval in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cluster_stats_main_loop_interval';
```

## `cluster.cssd_main_loop_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | CSSD aux process main-loop tick interval in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cssd_main_loop_interval_ms';
```

## `cluster.cssd_heartbeat_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 10000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | CSSD heartbeat broadcast period in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cssd_heartbeat_interval_ms';
```

## `cluster.cssd_dead_deadband_factor`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `3` |
| 范围/枚举 | 2 .. 10 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | CSSD dead-detection deadband as a multiple of heartbeat interval. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cssd_dead_deadband_factor';
```

## `cluster.voting_disks`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `空值` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Comma-separated list of voting disk file paths. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.voting_disks';
```

## `cluster.quorum_poll_interval_ms`

版本注意：本标签 QVOTEC 实际租约为轮询周期的 30 倍；2000 ms 默认轮询对应 60000 ms。历史注册长说明里的 2 倍已过时。不要把租约当成独立可配置参数，也不要将实验姿态当成生产隔离认证。

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `2000` |
| 范围/枚举 | 500 .. 30000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Quorum voting disk poll period in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.quorum_poll_interval_ms';
```

## `cluster.voting_disk_io_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 500 .. 60000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Voting disk single I/O timeout in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.voting_disk_io_timeout_ms';
```

## `cluster.voting_disk_size_bytes`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `394240` |
| 范围/枚举 | 4096 .. 1048576 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 字节 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Voting disk file size in bytes. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.voting_disk_size_bytes';
```

## `cluster.online_join`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Allow a declared node to join/rejoin live membership online (without a full cluster restart). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.online_join';
```

## `cluster.join_remaster_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | On node rejoin, move the joiner's home-shard GES mastership back from the survivor (optional rebalance). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.join_remaster_enabled';
```

## `cluster.join_convergence_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30000` |
| 范围/枚举 | 5000 .. 120000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Deadline for an online join to converge + commit. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.join_convergence_timeout_ms';
```

## `cluster.online_node_removal`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable permanent removal (decommission) of a declared node. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.online_node_removal';
```

## `cluster.node_removal_cleanup_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30000` |
| 范围/枚举 | 5000 .. 120000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Deadline for the post-shrink cluster-wide removal cleanup. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.node_removal_cleanup_timeout_ms';
```

## `cluster.freeze_writes_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable PROCSIG_CLUSTER_FREEZE_WRITES in-flight transaction abort. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.freeze_writes_enabled';
```

## `cluster.resolver_cache_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable 当前实现 shared resolver cache TRUST mode (skip the by-xid scan on a re-validated + accepted hint). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.resolver_cache_enabled';
```

## `cluster.resolver_cache_measure`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable 当前实现 shared resolver cache MEASURE mode (value gate, no trust). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.resolver_cache_measure';
```

## `cluster.resolver_cache_entries`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `0` |
| 范围/枚举 | 0 .. 1048576 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Shared resolver cache hint-slot count (0 = disabled / zero memory). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.resolver_cache_entries';
```

## `cluster.boc_sweep_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `100` |
| 范围/枚举 | 1 .. 1000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | walwriter BOC sweep staleness target in milliseconds. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.boc_sweep_interval_ms';
```

## `cluster.boc_event_publish`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Publish the durable SCN frontier on commit events. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.boc_event_publish';
```

## `cluster.enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Runtime cluster mode gate (当前版本 Sprint B HC4 闭环). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.enabled';
```

## `cluster.allow_single_node`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Allow pgrac to start in single-node mode (no pgrac.conf or invalid cluster.node_id). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.allow_single_node';
```

## `cluster.advisory_lock_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable cross-node globalization of advisory (user) locks. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.advisory_lock_enabled';
```

## `cluster.global_dd_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `2000` |
| 范围/枚举 | 100 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Coordinator cross-node deadlock scan period (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.global_dd_interval_ms';
```

## `cluster.cancel_ack_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 50 .. 60000 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Coordinator wait for a cross-node deadlock CANCEL_ACK before retransmit (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cancel_ack_timeout_ms';
```

## `cluster.cancel_max_retransmit`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `3` |
| 范围/枚举 | 0 .. 100 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Bounded cross-node deadlock cancel retransmit attempts before escalation. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cancel_max_retransmit';
```

## `cluster.victim_repeat_window_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 0 .. 600000 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Anti-thrash window for repeated deadlock victim selection (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.victim_repeat_window_ms';
```

## `cluster.ic_duty_lazy`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Run lazy-able LMON duty-chain drains on demand instead of every iteration. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ic_duty_lazy';
```
