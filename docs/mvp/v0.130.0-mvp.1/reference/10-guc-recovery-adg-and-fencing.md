# 恢复、ADG、备份与 fencing 参数

Author: SqlRush <sqlrush@gmail.com>

适用版本：`v0.130.0-mvp.1`。返回[手册目录](../README.md)。


每个条目固定来自 `v0.130.0-mvp.1` 注册表；测试参数单独标注。`postmaster` 表示修改后必须重启；`sighup` 表示 reload 生效；
`superuser`/`user` 表示可以在允许的 SQL 配置范围内修改。实际当前值请以 `pg_settings` 为准。

## `cluster.recovery_stale_active_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `10000` |
| 范围/枚举 | 1000 .. 3600000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Staleness window before an ACTIVE WAL-state slot is reported as a crash candidate. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.recovery_stale_active_ms';
```

## `cluster.recovery_workers_max`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `4` |
| 范围/枚举 | 0 .. 16 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum recovery stream-validation workers. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.recovery_workers_max';
```

## `cluster.merged_recovery`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable cold-crash k-way SCN merged recovery. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.merged_recovery';
```

## `cluster.dg_role`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `primary` |
| 范围/枚举 | primary, standby |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cluster Data Guard role for this instance. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.dg_role';
```

## `cluster.dg_mode`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `async` |
| 范围/枚举 | async, sync, max_availability |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cluster Data Guard shipping acknowledgement mode. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.dg_mode';
```

## `cluster.enable_adg`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable ADG standby apply and read-only service. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.enable_adg';
```

## `cluster.adg_lag_threshold_sec`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `10` |
| 范围/枚举 | 1 .. 300 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | ADG apply lag threshold for read-only service errors. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.adg_lag_threshold_sec';
```

## `cluster.max_standby_delay`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30` |
| 范围/枚举 | -1 .. 86400 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum delay before ADG read-only queries yield to apply. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.max_standby_delay';
```

## `cluster.adg_lease_takeover_grace_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 0 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Grace period past apply-master lease expiry before takeover. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.adg_lease_takeover_grace_ms';
```

## `cluster.adg_barrier_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 0 .. 300000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | ADG consistency barrier interval. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.adg_barrier_interval_ms';
```

## `cluster.clean_leave_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable cooperative clean-leave reconfiguration. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.clean_leave_enabled';
```

## `cluster.clean_leave_drain_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30000` |
| 范围/枚举 | 1000 .. 600000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Fail-closed deadline for a clean leave's cooperative drain. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.clean_leave_drain_timeout_ms';
```

## `cluster.recovery_merge_wait_timeout`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `10000` |
| 范围/枚举 | 0 .. 600000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Time to wait for stream-validation workers before merged recovery. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.recovery_merge_wait_timeout';
```

## `cluster.recovery_target_scn`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `空字符串` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cluster PITR target SCN. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.recovery_target_scn';
```

## `cluster.recovery_target_cluster_time`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `空字符串` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cluster PITR target timestamp. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.recovery_target_cluster_time';
```

## `cluster.recovery_target_name`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `空字符串` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cluster PITR named restore point target. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.recovery_target_name';
```

## `cluster.recovery_target_action`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `pause` |
| 范围/枚举 | pause, promote, shutdown |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Action to take when a cluster PITR target is reached. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.recovery_target_action';
```

## `cluster.enable_pitr_restore_points`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable automatic cluster restore point creation. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.enable_pitr_restore_points';
```

## `cluster.pitr_restore_point_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `0` |
| 范围/枚举 | 0 .. 86400000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Interval for automatic cluster PITR restore points. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.pitr_restore_point_interval_ms';
```

## `cluster.restore_point_drain_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30000` |
| 范围/枚举 | 1 .. 600000 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Timeout for cluster restore-point commit drain. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.restore_point_drain_timeout_ms';
```

## `cluster.backup_wal_retention`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `0` |
| 范围/枚举 | 0 .. 2147483647 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | MiB |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cluster backup WAL retention hint in megabytes. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.backup_wal_retention';
```

## `cluster.backup_parallel_channels`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1` |
| 范围/枚举 | 1 .. 128 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum cluster backup copy channels. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.backup_parallel_channels';
```

## `cluster.backup_manifest_checksums`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `crc32c` |
| 范围/枚举 | crc32c |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Checksum mode for cluster backup manifests. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.backup_manifest_checksums';
```

## `cluster.write_fence_lease_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `6000` |
| 范围/枚举 | 1000 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cooperative write-fence token lease duration (milliseconds). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_fence_state / pg_cluster_state(category='write_fence')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.write_fence_lease_ms';
```

## `cluster.external_fence_socket_path`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `/var/run/pgrac/pgrac-fenced.sock` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | External write-exclusion daemon socket path. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：必须是非空绝对 Unix socket 路径，禁止 ..，并受 sockaddr 路径长度限制。

观察入口：`pg_cluster_fence_state / pg_cluster_state(category='write_fence')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.external_fence_socket_path';
```

## `cluster.external_fence_acquire_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `120000（120 秒）` |
| 范围/枚举 | 1 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Overall external write-exclusion acquisition deadline (milliseconds). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_fence_state / pg_cluster_state(category='write_fence')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.external_fence_acquire_timeout_ms';
```

## `cluster.storage_fence_driver`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `auto` |
| 范围/枚举 | disabled, auto, scsi3_pr |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Shared-storage fencing driver selection. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_fence_state / pg_cluster_state(category='write_fence')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.storage_fence_driver';
```

## `cluster.self_fence_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable postmaster self-shutdown on persistent quorum loss. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_fence_state / pg_cluster_state(category='write_fence')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.self_fence_enabled';
```

## `cluster.self_fence_grace_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30000` |
| 范围/枚举 | 1000 .. 300000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Delay before postmaster self-shutdown on persistent quorum loss (ms). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_fence_state / pg_cluster_state(category='write_fence')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.self_fence_grace_ms';
```

## `cluster.fence_audit_log`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `log` |
| 范围/枚举 | off, log, debug |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Verbosity of fence-related log events. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_fence_state / pg_cluster_state(category='write_fence')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.fence_audit_log';
```

## `cluster.tt_recovery_resolve_active`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Resolve crash-left ACTIVE durable TT slots to ABORTED at startup. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.tt_recovery_resolve_active';
```

## `cluster.gcs_block_recovery_wait_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `200` |
| 范围/枚举 | 0 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Bounded wait (ms) on a recovering GCS block resource before 53R9L. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.gcs_block_recovery_wait_ms';
```

## `cluster.online_block_recovery`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Rebuild a corrupt/lost-write block from WAL on read instead of erroring. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.online_block_recovery';
```

## `cluster.block_recovery_on_unrecoverable`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `error` |
| 范围/枚举 | error, panic |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Action when a corrupt block cannot be rebuilt from WAL. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.block_recovery_on_unrecoverable';
```

## `cluster.online_thread_recovery`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Let a survivor online-replay a dead WAL thread's data to shared storage in the reconfig freeze window instead of waiting for the dead node's cold restart. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.online_thread_recovery';
```

## `cluster.thread_recovery_on_unrecoverable`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `keep_frozen` |
| 范围/枚举 | keep_frozen, panic |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Action when a dead thread cannot be online-recovered. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.thread_recovery_on_unrecoverable';
```
