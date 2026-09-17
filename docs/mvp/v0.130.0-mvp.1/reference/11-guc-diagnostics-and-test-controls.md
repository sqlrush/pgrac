# 诊断、性能观测与测试控制参数

Author: SqlRush <sqlrush@gmail.com>

适用版本：`v0.130.0-mvp.1`。返回[手册目录](../README.md)。


每个条目固定来自 `v0.130.0-mvp.1` 注册表；测试参数单独标注。`postmaster` 表示修改后必须重启；`sighup` 表示 reload 生效；
`superuser`/`user` 表示可以在允许的 SQL 配置范围内修改。实际当前值请以 `pg_settings` 为准。

## `cluster.xnode_profile`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable cross-node performance profiling buckets. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.xnode_profile';
```

## `cluster.adg_rfs_conninfos`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `空字符串` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | ADG RFS upstream connection strings. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：不进入示例配置

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.adg_rfs_conninfos';
```

## `cluster.adg_primary_thread_count`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `0` |
| 范围/枚举 | 0 .. 128 |
| Context | `internal` |
| 生效方式 | 只读内部值，不能由用户设置 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Primary ADG WAL thread count. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：内部派生值；由拓扑计算，SHOW 使用专用输出函数。

观察入口：`ADG/backup/PITR 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.adg_primary_thread_count';
```

## `cluster.injection_points`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `空字符串` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Comma-separated list of cluster injection points to auto-arm at startup. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：诊断/测试入口；生产配置应保持空值。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.injection_points';
```

## `cluster.pcm_x_retain_flush_error_target`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `空字符串` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Exact BufferTag selected by the retained-image finish fault. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：设置时由 `cluster_pcm_x_retain_flush_error_target_check_hook` 校验；变更时有 assign hook 同步运行状态；不进入示例配置

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.pcm_x_retain_flush_error_target';
```

## `cluster.write_fence_enforcement`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `on` |
| 范围/枚举 | off, on, dev |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cooperative write-fence enforcement mode. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_fence_state / pg_cluster_state(category='write_fence')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.write_fence_enforcement';
```

## `cluster.ic_suppress_caps_reply`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Test-only: simulate a pre-CAPS_REPLY binary on this node. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：不进入示例配置

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ic_suppress_caps_reply';
```

## `cluster.ic_suppress_gcs_done_cap`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Test-only: simulate a pre-GCS_DONE binary on this node. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：不进入示例配置

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ic_suppress_gcs_done_cap';
```

## `cluster.ic_suppress_xid_flock_cap`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Test-only: suppress the XID_AUTHORITY_FLOCK_V2 HELLO capability. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：不进入示例配置

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ic_suppress_xid_flock_cap';
```

## `cluster.xid_wrap_barrier_force`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Test-only: force the xid wrap barrier to run now. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：不进入示例配置

观察入口：`pg_cluster_state 的 undo/scn/xid_stripe/tt_* 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.xid_wrap_barrier_force';
```

## `cluster.touched_peers_trace`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Log the touched-peers set of each transaction aborted by a fail-stop reconfiguration. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.touched_peers_trace';
```

## `cluster.gcs_block_drop_target_relfilenode`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `0` |
| 范围/枚举 | 0 .. 2147483647 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Test-only: restrict the drop-reply injection to one relfilenode. |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.gcs_block_drop_target_relfilenode';
```

## `cluster_test_force_visibility_cluster_path`

仅 `ENABLE_INJECTION` 条件构建注册；不属于普通构建的 250 个 `cluster.*` 参数。普通二进制查询不到此项是预期情况，不应为了使用它重新编译评估服务。

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Test-only: force HeapTupleSatisfiesMVCC cluster path entry via 当前实现 D5b inject table (overrides placeholder ITL ref reader). |

配置方法：按上述 Context 选择配置文件＋正常重启、reload 或允许的会话 SET；具体作用以该条用途与下述约束为准。不要把诊断阈值、重传节奏、操作期限与安全租约视为同一种参数。

依赖与保护：仅注入测试构建可用，不写入示例配置。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster_test_force_visibility_cluster_path';
```
