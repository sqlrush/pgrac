# 恢复、ADG、备份与 fencing 参数

每个条目均来自当前 `cluster.*` 注册表。`postmaster` 表示修改后必须重启；`sighup` 表示 reload 生效；
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

工作原理与调节影响：设置 恢复 / 陈旧 / 活动 / `ms`。具体行为以本参数内置说明及关联状态视图为准。

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

工作原理与调节影响：限定 恢复 / `workers` / 上限 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.recovery_workers_max';
```

## `cluster.cold_recovery_plan_memory`

| 属性 | 值 |
|---|---|
| 类型 | `integer` |
| 默认值 | `4GB`（32 位平台受平台上限约束） |
| 范围 | `1MB` 至平台 `MAX_KILOBYTES` 上限 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | `kB` |
| 内置短说明 | Memory budget of the typed cold-recovery plan. |

限制所有实例异常退出后，类型化冷恢复第一遍规划保留的内存，包括记录、页版本和封存临时空间。
这不是预先分配量。规划超出预算时，在修改任何页之前拒绝启动；可增加此参数后重新启动恢复。
应同时给 PostgreSQL 的其他内存使用和操作系统预留空间。

观察当前值及重启状态：

```sql
SELECT name, setting, unit, min_val, max_val, context, source, pending_restart
FROM pg_settings WHERE name = 'cluster.cold_recovery_plan_memory';
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

工作原理与调节影响：开关 `merged` / 恢复 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

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

工作原理与调节影响：选择 `dg` / 角色 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

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

工作原理与调节影响：选择 `dg` / 模式 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

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

工作原理与调节影响：开关 `enable` / `adg` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

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

工作原理与调节影响：设置 `adg` / 延迟 / `threshold` / 秒。具体行为以本参数内置说明及关联状态视图为准。

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

工作原理与调节影响：限定 上限 / `standby` / 延迟 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

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

工作原理与调节影响：定义 `adg` / 租约 / `takeover` / 宽限 / `ms` 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

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

工作原理与调节影响：定义 `adg` / 屏障 / `interval` / `ms` 的运行周期。调小反应更快但增加 CPU、网络或 I/O 频率；调大降低开销但延长发现/处理延迟。

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

工作原理与调节影响：开关 干净 / `leave` / 启用状态 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

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

工作原理与调节影响：定义 干净 / `leave` / 排空 / 超时 / `ms` 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

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

工作原理与调节影响：定义 恢复 / `merge` / 等待 / 超时 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

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

工作原理与调节影响：设置 恢复 / 目标 / SCN。具体行为以本参数内置说明及关联状态视图为准。

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

工作原理与调节影响：设置 恢复 / 目标 / `cluster` / 时间。具体行为以本参数内置说明及关联状态视图为准。

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

工作原理与调节影响：设置 恢复 / 目标 / `name`。具体行为以本参数内置说明及关联状态视图为准。

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

工作原理与调节影响：选择 恢复 / 目标 / `action` 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

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

工作原理与调节影响：开关 `enable` / `pitr` / 恢复 / 点 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

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

工作原理与调节影响：定义 `pitr` / 恢复 / `point` / `interval` / `ms` 的运行周期。调小反应更快但增加 CPU、网络或 I/O 频率；调大降低开销但延长发现/处理延迟。

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

工作原理与调节影响：定义 恢复 / `point` / 排空 / 超时 / `ms` 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

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

工作原理与调节影响：设置 备份 / WAL / 保留。具体行为以本参数内置说明及关联状态视图为准。

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

工作原理与调节影响：设置 备份 / 并行 / channel。具体行为以本参数内置说明及关联状态视图为准。

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

工作原理与调节影响：选择 备份 / manifest / `checksums` 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

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

工作原理与调节影响：设置 写入 / 隔离 / 租约 / `ms`。具体行为以本参数内置说明及关联状态视图为准。

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

工作原理与调节影响：设置 `external` / 隔离 / `socket` / `path`。具体行为以本参数内置说明及关联状态视图为准。

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

工作原理与调节影响：定义 `external` / 隔离 / `acquire` / 超时 / `ms` 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

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

工作原理与调节影响：选择 存储 / 隔离 / `driver` 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

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

工作原理与调节影响：开关 本节点 / 隔离 / 启用状态 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

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

工作原理与调节影响：定义 本节点 / 隔离 / 宽限 / `ms` 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

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

工作原理与调节影响：选择 隔离 / `audit` / `log` 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

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

工作原理与调节影响：开关 `tt` / 恢复 / `resolve` / 活动 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

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

工作原理与调节影响：设置 `gcs` / 数据块 / 恢复 / 等待 / `ms`。具体行为以本参数内置说明及关联状态视图为准。

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

工作原理与调节影响：开关 `online` / 数据块 / 恢复 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

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

工作原理与调节影响：选择 数据块 / 恢复 / `on` / `unrecoverable` 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

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

工作原理与调节影响：开关 `online` / 线程 / 恢复 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

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

工作原理与调节影响：选择 线程 / 恢复 / `on` / `unrecoverable` 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.thread_recovery_on_unrecoverable';
```
