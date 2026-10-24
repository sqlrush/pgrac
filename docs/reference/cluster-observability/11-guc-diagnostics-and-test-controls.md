# 诊断、性能观测与测试控制参数

每个条目均来自当前 `cluster.*` 注册表。`postmaster` 表示修改后必须重启；`sighup` 表示 reload 生效；
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

工作原理与调节影响：开关 `xnode` / `profile` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.xnode_profile';
```

## `cluster.trace_tx_identity`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话或配置文件中设置 |
| 输出 | 服务端日志中的 `TX_IDENTITY` 记录 |

采集跨节点事务等待时，在各节点的诊断配置中设置
`cluster.trace_tx_identity = on`，启动诊断实例后核对：

```sql
SHOW cluster.trace_tx_identity;
```

会话内的 `SET` 仅影响当前会话。需要同时采集分配、origin 解析、终态发布和唤醒记录时，
应在启动前为所有参与节点配置此参数。采集结束后关闭。

`tt_known` / `generation_known` 表示 canonical TT 字段是否有效；数值 `0` 本身不表示未知。
`source_segment` / `source_slot_id` 是 SOURCE 字段；`tt_slot` 是从零开始的 canonical 槽偏移。
`uba0` / `uba1` 保存完整 DATA UBA；`itl_wrap` 是 ITL 字段，应与 `tt_wrap` 分开读取。
`WAIT_TARGET` 可能不具备 canonical 字段，应结合 origin 的 `ORIGIN_MAP` 记录、节点启动身份
和同次运行日志关联，不能用同 raw xid 的当前分配器样本补齐。

每个进程、每类事件最多输出 32768 条，随后输出一次 `event=LIMIT`；`ORIGIN_MAP` 会在有界
窗口内合并重复记录。`WAKE_SUMMARY` 的 `count` 是实际唤醒数，`skipped_targets` 是扫描中
跳过的 TARGET 槽数，`omitted` 是未逐条输出的唤醒接收者数。出现 LIMIT 或 omitted 时保留
证据缺口；没有某条记录也不能证明对应事件未发生。请与配置、二进制身份和原始节点日志一起保存。

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

工作原理与调节影响：设置 `adg` / `rfs` / `conninfos`。具体行为以本参数内置说明及关联状态视图为准。

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

工作原理与调节影响：设置 `adg` / `primary` / 线程 / 累计次数。具体行为以本参数内置说明及关联状态视图为准。

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

工作原理与调节影响：设置 `injection` / 点。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：诊断/测试入口；生产配置应保持空值。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.injection_points';
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

工作原理与调节影响：选择 写入 / 隔离 / `enforcement` 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

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

工作原理与调节影响：开关 `ic` / `suppress` / `caps` / 响应 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

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

工作原理与调节影响：开关 `ic` / `suppress` / `gcs` / `done` / `cap` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

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

工作原理与调节影响：开关 `ic` / `suppress` / `xid` / `flock` / `cap` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

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

工作原理与调节影响：开关 `xid` / `wrap` / 屏障 / `force` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

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

工作原理与调节影响：开关 `touched` / `peers` / `trace` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

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

工作原理与调节影响：设置 `gcs` / 数据块 / 丢弃 / 目标 / `relfilenode`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_cluster_state 对应 gcs/ges/grd/lmd/lms/cr/cf 分类`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.gcs_block_drop_target_relfilenode';
```

## `cluster_test_force_visibility_cluster_path`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Test-only: force HeapTupleSatisfiesMVCC cluster path entry via 当前实现 D5b inject table (overrides placeholder ITL ref reader). |

工作原理与调节影响：开关 `cluster` / `test` / `force` / `visibility` / `cluster` / `path` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：仅注入测试构建可用，不写入示例配置。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster_test_force_visibility_cluster_path';
```
