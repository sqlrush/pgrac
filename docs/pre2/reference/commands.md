# PRE2 视图、SQL 函数与命令参考

适用代码版本：`c45bdc5d4392751a33659acf8dcaa2b76254e73a`。本页列出相对原生 PostgreSQL 增加的集群入口；`psql`、`pg_ctl`、`pg_stat_activity` 等原生工具和视图继续使用。接口存在不等于对应功能已纳入技术预览。

交付镜像 `2d857abffc` 的参数注册、等待名称及 SQL 函数／视图定义与本页核对版本一致；动态诊断键与可用功能仍按实际二进制识别。

本预览按[安装手册](../install.md)使用固定成员、全新共享建库及正常全停。在线加入／移除／单节点计划退出、ADG、媒体恢复和测试注入不作为客户操作流程。

## PRE2 新增与变化

与 `v0.132.0` 对照：新增 `pg_cluster_membership_command(text, jsonb)`；28 个集群视图的 SQL 定义没有新增或变更。共享 `ALTER SYSTEM`、全新共享建库和离线状态观察提供了新的使用方式。`pg_cluster_state` 的键集合由二进制版本决定，不能照旧版键清单认定缺少的键值为零。

权限方面，这些视图默认授予 `PUBLIC SELECT`，具体函数仍可能检查权限、功能开关或运行条件。管理／注入函数不能仅靠视图的查询权限调用。使用 `\df+`、`\d+` 或下面的目录查询查看当前安装的签名与权限：

```sql
SELECT p.oid::regprocedure AS signature,
       pg_get_function_result(p.oid) AS result_type,
       has_function_privilege(current_user, p.oid, 'EXECUTE') AS may_execute
FROM pg_proc AS p
JOIN pg_namespace AS n ON n.oid = p.pronamespace
WHERE n.nspname = 'pg_catalog'
  AND (p.proname LIKE 'cluster_%' OR p.proname LIKE 'pg_cluster_%'
       OR p.proname LIKE 'pgrac_%')
ORDER BY p.oid::regprocedure::text;
```

`may_execute` 只表示 SQL 权限，不表示函数的运行前置已经满足。

## 视图

一般查询优先使用下面的结构化视图。各节点分别采集，保留节点号、启动时间及采样时间；不能把一个节点的本地观察当作全局一致快照。

| 视图 | 内容／使用边界 |
|---|---|
| `pg_stat_cluster_wait_events` | 集群等待事件名称目录；不是等待次数或耗时。 |
| `pg_stat_gcluster_wait_events` | 带 node_id 的等待事件名称目录；不是远端实时等待采样。 |
| `pg_cluster_nodes` | 声明的节点、地址、角色及 is_self；不是当前存活成员清单。 |
| `pg_stat_cluster_injections` | 测试注入点名称、状态和命中数；业务部署不使用注入。 |
| `pg_stat_cluster_nodes` | 节点运行状态、启动时间、PGRAC 与 PG 版本。 |
| `pg_stat_cluster_counters` | 具名计数器；保留原名称和增量，不能把缺失值当作零。 |
| `pg_cluster_ic_peers` | 本节点观察到的互联连接、收发、心跳、重连及错误。 |
| `pg_stat_cluster_ic` | 所选 TCP/RDMA 传输的状态、计数及错误；某些字段仅适用于 RDMA。 |
| `pg_cluster_cssd_peers` | 对端成员心跳、怀疑／死亡状态及对应时间。 |
| `pg_cluster_quorum_state` | 本节点的 quorum、可用投票盘数和冲突状态。 |
| `pg_cluster_voting_disks` | 投票盘路径、最近读写时间、读写与 I/O 错误次数。 |
| `pg_cluster_fence_state` | 冻结、解冻、自我隔离的状态和计数；不是隔离服务的物理关机证明。 |
| `pg_cluster_reconfig_state` | 最近重配置事件、协调节点、新旧 epoch 和成员位图。 |
| `pg_cluster_membership` | 声明成员、当前状态、接纳身份和 epoch；不提供在线加入执行能力。 |
| `pg_cluster_clean_leave_state` | 单节点计划退出的状态；本预览不提供相应执行流程。 |
| `pg_cluster_node_removal_state` | 节点移除的状态；本预览不提供相应执行流程。 |
| `pg_stat_cluster_adg` | 本地备用库接收／应用位置和延迟；功能不在本预览范围。 |
| `pg_stat_gcluster_adg` | 带节点编号的备用库状态；功能不在本预览范围。 |
| `pg_stat_cluster_backup` | 备份状态及参数；不能据视图存在推断共享媒体恢复可用。 |
| `pg_cluster_backup_history` | 最近备份摘要。 |
| `pg_cluster_restore_points` | 命名恢复点摘要。 |
| `pg_cluster_pitr_status` | 时间点恢复目标解析状态；本预览不提供媒体恢复流程。 |
| `pg_cluster_ic_msg_types` | 已登记互联消息类型和处理程序状态，供支持诊断。 |
| `pg_cluster_grd_shards` | 全局资源管理节点分布；结果行数较多，低频使用。 |
| `pg_cluster_grd_entries` | 当前全局锁资源及授予、等待、转换数量；遍历资源表，定点诊断使用。 |
| `pg_cluster_lmd` | 死锁检测进程状态、原因和处理计数。 |
| `pg_cluster_state` | category/key/value 全量诊断快照，见下文成本与用法。 |
| `pg_cluster_shmem` | 已登记共享内存区域名称、大小及锁数。 |

### 常用只读查询

```sql
SELECT n.node_id, n.hostname, n.is_self, m.state,
       m.presented_incarnation, m.last_admitted_incarnation,
       m.admitted_epoch, m.removed
FROM pg_cluster_nodes AS n
LEFT JOIN pg_cluster_membership AS m USING (node_id)
ORDER BY n.node_id;

SELECT clock_timestamp() AS sampled_at, * FROM pg_cluster_quorum_state;
SELECT clock_timestamp() AS sampled_at, * FROM pg_cluster_fence_state;
SELECT * FROM pg_cluster_voting_disks ORDER BY path;

SELECT node_id, state, last_heartbeat_recv_at, reconnect_count,
       connect_error_count, last_error, msg_send_count, msg_recv_count
FROM pg_cluster_ic_peers
ORDER BY node_id;
```

`connected` 只说明互联连接状态，不能代替成员接纳、quorum 或 SQL 可用性的检查。空行、`unknown`、`unavailable`、`blocked` 都不能解释为健康。

## pg_cluster_state 的用法

`pg_cluster_state` 是 `cluster_dump_state()` 的视图，三列均为 `text`：`category`（类别）、`key`（键）、`value`（值）。同一类别包含计数器、状态、时间、文本和 JSON；不要无条件把 `value` 转成整数。

**每次调用会先生成所有类别的完整结果。** SQL 的 `WHERE category=...` 或 `LIMIT` 只筛选输出，不会降低前面的全量采集成本。它不是只读几个原子计数器的廉价接口；不要在应用每事务或高频轮询中反复调用。

一次采集后，在客户端或临时表中复用结果：

```sql
-- 在一个诊断会话内执行；会创建本会话的临时表。
CREATE TEMP TABLE pre2_state_sample AS
SELECT clock_timestamp() AS sampled_at, category, key, value
FROM pg_cluster_state;

SELECT category, count(*) FROM pre2_state_sample GROUP BY category ORDER BY category;
SELECT key, value FROM pre2_state_sample WHERE category = 'pcm' ORDER BY key;
SELECT key, value FROM pre2_state_sample WHERE category = 'xnode_profile' ORDER BY key;
DROP TABLE pre2_state_sample;
```

不希望创建临时表时，可用 `psql` 的客户端 `\copy` 导出一次，再离线筛选：

```text
\copy (SELECT category, key, value FROM pg_cluster_state) TO 'pre2-state.csv' WITH (FORMAT csv, HEADER true)
```

常见类别包括 `pcm`、`gcs`、`ges`、`ic`、`lmon`、`lms`、`undo_cleaner`、`ctrc`、`catalog`、`xnode_profile` 和 `lifecycle`；以当次结果为准。该视图逐项读取，不能当作所有类别在同一瞬间的原子快照。

性能类别的 `bucket.<name>.total_nanos` 是累计纳秒，`bucket.<name>.n_events` 是对应计数；两次采集的差值可计算同一窗口内该桶的平均时间：`Δtotal_nanos / Δn_events / 1000000` 得到毫秒。`n_events` 的含义由桶决定，可能是请求、调用或执行步骤，不一律等于事务数或消息数。请求方计时与服务方计时、父计时与嵌套子计时可能重叠，不能直接相加。

- 记录同一节点、同一 postmaster 生命周期和 `reset_generation`；重启／重置后重新建立基线。
- profiling 关闭时，零增量不能解释为没有发生该操作。开关作用域参见[参数参考](parameters.md)。
- `hist.*.le_*us` 的各档是独立分桶计数，不是累计前缀计数。
- 负增量、缺键、采样失败和计数为零的平均值应记为不可计算，不补零。
- 需要每事务耗时时，分母必须是同一窗口、同一节点范围内的成功事务数。

`lifecycle/native_writer` 是服务中的只读身份观察；`unavailable` 表示本次未取得完整观察，不是正常关闭证明。停止后的状态使用下面的离线命令。

## SQL 函数清单

下面列出全部 70 个集群函数的输入签名。`SETOF record`／`record` 的输出列可用 `\df+` 或上面的 `pg_get_function_result()` 查询；与视图对应的函数通常直接查询视图更方便。没有列出默认参数时按完整签名传参。

### 查询与观测

| 函数（输入类型） | 返回类型 | 用途 |
|---|---|---|
| `cluster_get_wait_events()` | `SETOF record` | 集群等待事件名称目录；不是等待次数或耗时。 |
| `cluster_get_gcluster_wait_events()` | `SETOF record` | 带 node_id 的等待事件名称目录；不是远端实时等待采样。 |
| `cluster_get_nodes()` | `SETOF record` | 声明的节点、地址、角色及 is_self；不是当前存活成员清单。 |
| `cluster_get_injection_state()` | `SETOF record` | 测试注入点名称、状态和命中数；业务部署不使用注入。 |
| `cluster_get_stat_nodes()` | `SETOF record` | 节点运行状态、启动时间、PGRAC 与 PG 版本。 |
| `cluster_get_pgstat_counters()` | `SETOF record` | 具名计数器；保留原名称和增量，不能把缺失值当作零。 |
| `cluster_dump_state()` | `SETOF record` | category/key/value 全量诊断快照，见下文成本与用法。 |
| `cluster_shmem_dump_regions()` | `SETOF record` | 已登记共享内存区域名称、大小及锁数。 |
| `cluster_scn_current()` | `bigint` | 读取当前 SCN（以 bigint 表示）。 |
| `cluster_get_ic_peers()` | `SETOF record` | 本节点观察到的互联连接、收发、心跳、重连及错误。 |
| `cluster_get_ic_rdma_peers()` | `SETOF record` | 所选 TCP/RDMA 传输的状态、计数及错误；某些字段仅适用于 RDMA。 |
| `cluster_get_ic_msg_types()` | `SETOF record` | 已登记互联消息类型和处理程序状态，供支持诊断。 |
| `cluster_get_cssd_peers()` | `SETOF record` | 对端成员心跳、怀疑／死亡状态及对应时间。 |
| `cluster_get_quorum_state()` | `SETOF record` | 本节点的 quorum、可用投票盘数和冲突状态。 |
| `cluster_get_voting_disks()` | `SETOF record` | 投票盘路径、最近读写时间、读写与 I/O 错误次数。 |
| `cluster_get_fence_state()` | `SETOF record` | 冻结、解冻、自我隔离的状态和计数；不是隔离服务的物理关机证明。 |
| `cluster_get_reconfig_state()` | `SETOF record` | 最近重配置事件、协调节点、新旧 epoch 和成员位图。 |
| `cluster_get_membership()` | `SETOF record` | 声明成员、当前状态、接纳身份和 epoch；不提供在线加入执行能力。 |
| `cluster_get_grd_shards()` | `SETOF record` | 全局资源管理节点分布；结果行数较多，低频使用。 |
| `cluster_get_grd_entries()` | `SETOF record` | 当前全局锁资源及授予、等待、转换数量；遍历资源表，定点诊断使用。 |
| `cluster_get_lmd_state()` | `SETOF record` | 死锁检测进程状态、原因和处理计数。 |
| `pg_cluster_ges_mode_matrix()` | `SETOF record` | 列出锁模式兼容矩阵。 |
| `cluster_ges_mode_compat(text, text)` | `boolean` | 判断两个 PG 锁模式名称是否兼容。 |
| `cluster_ges_mode_matches_pg()` | `boolean` | 检查集群锁兼容矩阵是否与 PG 锁模式匹配。 |
| `pg_cluster_hang_victims()` | `SETOF record` | 读取长等待候选及评分，不自动取消事务。 |
| `cluster_get_clean_leave_state()` | `SETOF record` | 单节点计划退出的状态；本预览不提供相应执行流程。 |
| `cluster_get_node_removal_state()` | `SETOF record` | 节点移除的状态；本预览不提供相应执行流程。 |
| `cluster_get_backup_state()` | `SETOF record` | 备份状态及参数；不能据视图存在推断共享媒体恢复可用。 |
| `cluster_get_backup_history()` | `SETOF record` | 最近备份摘要。 |
| `cluster_get_restore_points()` | `SETOF record` | 命名恢复点摘要。 |
| `cluster_get_pitr_status()` | `SETOF record` | 时间点恢复目标解析状态；本预览不提供媒体恢复流程。 |
| `cluster_get_adg_state()` | `SETOF record` | 本地备用库接收／应用位置和延迟；功能不在本预览范围。 |
| `cluster_get_gcluster_adg()` | `SETOF record` | 带节点编号的备用库状态；功能不在本预览范围。 |

### 管理入口

这些函数可能改变运行状态、产生错误或终止连接。成员变更和物理备份执行入口不纳入本预览的操作流程；不要把“有 EXECUTE 权限”当作执行许可。

| 函数（输入类型） | 返回类型 | 用途 |
|---|---|---|
| `pg_cluster_membership_command(text, jsonb)` | `jsonb` | 新增的成员命令 JSON 接口；本预览返回 blocked，详见下文。 |
| `pg_cluster_hang_dump(integer)` | `boolean` | 要求指定 backend 记录自身等待诊断；会产生日志。 |
| `pg_cluster_hang_resolve(integer)` | `boolean` | 终止指定的长等待候选；管理员操作，可能中止业务。 |
| `pg_cluster_clean_leave_request()` | `text` | 请求单节点计划退出；不作为本预览操作入口。 |
| `pg_cluster_remove_node(integer)` | `text` | 请求移除节点；不作为本预览操作入口。 |
| `pg_cluster_backup_start(text, boolean)` | `record` | 启动集群物理备份；本预览不提供完整备份／媒体恢复操作支持。 |
| `pg_cluster_backup_stop(boolean)` | `record` | 结束集群物理备份；同上。 |
| `pg_cluster_create_restore_point(text)` | `record` | 创建命名恢复点；本预览不提供媒体恢复操作支持。 |

### 诊断、测试及部署专用入口

下面仅用于识别已有函数名，不能用于业务初始化、修复元数据、伪造事务状态或解除拒绝。有些入口只在测试构建或特定配置中可用；普通构建可能明确拒绝。不要从应用调用。

| 函数（输入类型） | 返回类型 | 用途 |
|---|---|---|
| `cluster_ic_mock_inject(integer, bytea)` | `void` | inject a fake inbound message into the mock interconnect queue |
| `cluster_ic_mock_drain_outbound(integer)` | `SETOF record` | drain queued outbound mock messages for a target node |
| `cluster_ic_mock_clear_all()` | `void` | reset all mock interconnect queues |
| `cluster_ic_mock_recv_test()` | `SETOF record` | pop one inbound mock message via cluster_ic_recv_bytes |
| `cluster_inject_fault(text, text, bigint)` | `boolean` | arm or disarm a cluster injection point |
| `cluster_scn_advance()` | `bigint` | 诊断用：推进 SCN；有状态修改，不用于业务。 |
| `cluster_scn_observe(bigint)` | `void` | 诊断用：输入外部 SCN；有状态修改，不用于业务。 |
| `pg_cluster_lmd_inject_wait_edge(integer, integer, bigint, integer, integer, bigint)` | `boolean` | TEST-ONLY: inject synthetic LMD wait edge |
| `pg_cluster_lmd_remove_wait_edges(integer, integer, bigint)` | `boolean` | TEST-ONLY: remove synthetic LMD wait edges by waiter |
| `pgrac_r4_bit22_cutover_begin()` | `boolean` | 管理用途的服务激活入口；由部署／启动流程负责，不手工调用。 |
| `cluster_test_inject_visibility_tt_ref(xid, integer, integer, integer, integer, bigint, boolean)` | `boolean` | TEST-ONLY: inject remote TT ref for MVCC visibility fork |
| `cluster_test_clear_visibility_injects()` | `integer` | TEST-ONLY: clear MVCC visibility fork inject table |
| `cluster_test_inject_subtrans_subcommitted(xid, xid, integer, integer, integer, integer)` | `boolean` | TEST-ONLY: inject SUBCOMMITTED parent chain for SUBTRANS visibility |
| `cluster_undo_get_record(bytea)` | `bytea` | 按二进制引用读取 undo 记录的诊断入口，不供应用调用。 |
| `cluster_undo_test_force_segment_end()` | `boolean` | TEST-ONLY: force undo segment cursor to last block |
| `cluster_cr_test_construct(regclass, integer, integer, bigint)` | `boolean` | TEST-ONLY: construct own-instance CR block image |
| `cluster_cr_test_image(regclass, integer, bigint)` | `SETOF record` | TEST-ONLY: CR block image rows as-of read_scn |
| `cluster_block_apply_redo_test(regclass, integer, bigint, pg_lsn, pg_lsn)` | `bytea` | TEST-ONLY: single-block redo-apply reconstruction over WAL |
| `cluster_thread_apply_redo_test(regclass, integer, bigint, pg_lsn, pg_lsn, bytea)` | `bytea` | TEST-ONLY: LSN-gated online thread-recovery apply over WAL |
| `cluster_thread_local_complete_test(integer, pg_lsn)` | `boolean` | TEST-ONLY: online thread-recovery D3 local-complete authority gate |
| `cluster_thread_gate_unfreeze_test(integer)` | `boolean` | TEST-ONLY: online thread-recovery D3 reconfig unfreeze gate |
| `cluster_thread_validated_end_test(integer, pg_lsn, pg_lsn)` | `text` | TEST-ONLY: online thread-recovery D4 validated torn-tail boundary |
| `cluster_thread_replay_slot_state_test(integer)` | `integer` | TEST-ONLY: read online thread-recovery 3b-4b replay-state slot |
| `cluster_thread_capability_gate_test(integer)` | `text` | TEST-ONLY: drive the online thread-recovery D7 capability gate |
| `cluster_pi_apply_redo_test(regclass, integer, bigint, integer, pg_lsn, pg_lsn, boolean)` | `text` | TEST-ONLY: PI-base vs zero-base thread-WAL block rebuild |
| `cluster_block_recovery_reconstruct_test(regclass, integer, bigint, pg_lsn, pg_lsn)` | `bytea` | TEST-ONLY: online single-block recovery reconstruction over WAL |
| `cluster_ts_acquire_probe(text, text, text, boolean)` | `text` | TEST-ONLY: drive the TT tablespace-DDL GES acquire |
| `cluster_ts_release_probe()` | `boolean` | TEST-ONLY: release a held TT tablespace-DDL probe claim |
| `cluster_ko_flush_probe(oid, oid, oid)` | `text` | TEST-ONLY: drive the KO object-reuse flush barrier |

### 成员命令的当前行为

`pg_cluster_membership_command(action text, request jsonb)` 要求超级用户；`action` 接受 `precheck`、`execute`、`status`。本预览未提供在线成员操作执行服务，即使输入合法也返回 `status="blocked"`，集群开启时 `reason="membership_authority_unavailable"`。这是受限制的结果，不是命令已接受或已完成。

只读状态示例：

```sql
SELECT pg_cluster_membership_command('status', NULL::jsonb);
```

非空请求必须包含且仅包含以下 8 项：`version=1`、`operation_kind`（`leave/remove/rejoin`）、`target_node`（0–15）、非零规范小写 `guest_uuid`、正整数 `expected_formation`、`operation_generation`、`expected_old_incarnation`，以及 `reserved_new_incarnation`（rejoin 时大于 old；其他操作为 0）。这些字段由受支持的管理流程提供，不能猜测数值后尝试操作。

### 共享 ALTER SYSTEM

PRE2 共享模式使用原生 SQL 语法发布共享参数，例如持久化允许的诊断开关：

```sql
ALTER SYSTEM SET cluster.xnode_profile = on;
-- 诊断结束后恢复已知的原配置，或按部署策略 RESET 此项。
ALTER SYSTEM RESET cluster.xnode_profile;
```

这不是同时在所有现有会话执行 `SET`。COMMON 启动／协议参数需重启生效，实例参数仅针对执行 SQL 的本实例；创建时参数和 `RESET ALL` 被拒绝。使用管理员账号并逐节点检查当前值；完整规则见[参数参考](parameters.md#查看与修改)。

## 命令行工具

### 本预览使用的入口

| 工具／选项 | 用法与限制 | PRE2 |
|---|---|---|
| `initdb --pgrac-initdb-cohort --pgrac-initdb-shared-config=FILE -D NEW_CACHE_PARENT` | 一次准备固定成员的新共享库及本地缓存目录。`FILE` 必须由安装流程提供；需要数据校验和、完整同步及受支持 locale。只能用于全新目标，不用于修复失败数据。 | 新入口 |
| `postgres --pgrac-observe-writer PGDATA SHARED_ROOT WAL_ROOT NODE` | 离线只读观察指定节点的数据状态，输出 JSON；不启动数据库、不进行恢复。 | 新入口 |
| `pg_ctl -D PGDATA status` | 查询本地进程是否运行；返回 3 只表示未运行，不证明上次正常停机。 | 原工具，共享操作限制见下文 |
| `pg_ctl -D PGDATA -m fast -w -t SECONDS stop` | 请求正常快速停机并等待退出。应先停止业务，再按安装流程向全体固定成员发出停机；不得把四节点停机串行拖过内部阶段期限。 | 原工具 |
| `pg_ctl -D PGDATA -w -t SECONDS start` | 按创建时配置启动本地实例；共享库需全体固定成员的受支持启动流程。 | 原工具 |
| `psql` | 普通 SQL、上述状态视图及配置命令。 | 原工具 |

`SECONDS` 是客户端等待上限，不改变数据库内部的停机阶段期限。`pg_ctl stop -w` 返回“已停止”也可能是进程异常退出，必须同时核对日志和离线关闭状态；不要用 `-W` 当作停机成功凭证。

离线观察示例（路径和节点号取自本次安装，不混用不同库）：

```sh
postgres --pgrac-observe-writer "$PGDATA" "$SHARED_ROOT" "$WAL_ROOT" "$NODE_ID"
```

命令退出码 0 表示观察成功，不等于正常关闭；输出状态可能为 `CLOSED`、`OPEN` 或 `OTHER`。正常全停后应逐节点核对 `CLOSED`、一致的数据身份和日志结果。读失败、`OTHER` 或停机 PANIC 时保留 DATA/WAL，不重新 initdb、不删控制文件、不用 `pg_resetwal` 绕过；按技术预览支持流程处理，不能凭进程已退出承诺安全重启。

建库还提供 `--pgrac-initdb-thread`、`--pgrac-initdb-system-identifier`、`--pgrac-initdb-shared-base`、`--pgrac-initdb-storage-uuid`、`--pgrac-initdb-database-incarnation` 等单 writer 参数，以及旧的 `--pgrac-wal-state-root`、`--pgrac-hw-snapshot-root`、`--pgrac-hw-snapshot-owner`。这些是部署入口的组成部分，不应单独拼接来接纳旧库或添加成员。

### 已安装的兼容脚本与隔离服务工具

| 工具 | 内容／本预览用法 |
|---|---|
| `pgrac-init` | 兼容初始化包装脚本。其 `--cluster-seed`／`--cluster-join` 路径不能替代本预览的完整 cohort 建库流程。 |
| `pgrac-start` | 兼容启动包装脚本；本预览以安装手册生成的路径和配置为准。 |
| `pgrac-acceptance` | 随源码提供的兼容验收包装脚本；不是共享库恢复或完整性证明工具。 |
| `pgrac-fenced` | 隔离服务守护进程，按交付的服务配置管理，不由数据库用户手工启动另一份。 |
| `pgrac-fencedctl status --json` | 隔离服务状态查询；需要相应的系统管理员身份。 |
| `pgrac-fencedctl verify-journal PATH` | 检查已有服务日志文件；不能据文件格式有效就判断节点可以重新开机。 |
| `pgrac-fencedctl prepare-rejoin NODE OLD_INCARNATION CANDIDATE_INCARNATION [--timeout-ms MILLISECONDS]` | 服务管理入口；本预览不提供在线重新加入操作，不用于绕过拒启。 |
| `pgrac-fenced-map-verify` | 隔离部署所用的输入校验辅助程序，由交付服务调用。 |
| `pgrac-fenced-drain-sign` | 隔离部署所用的签名辅助程序，由受限服务调用，不能代替实际隔离操作。 |

源码树中的 `scripts/deploy/pre2/membership.py` 是管理 CLI，不是一个已安装的 `pgrac membership` 子命令。诊断状态的调用形式为：

```sh
python3 scripts/deploy/pre2/membership.py status --service pre2_admin
```

`pre2_admin` 是事先配置的 libpq service；使用非交互认证。本预览的 `blocked` 结果会使该 CLI 返回 2，通信／结果不明返回 1；不能将返回 2 解释为操作已提交。不要改用旧 clean-leave SQL 来绕过此限制。

## 与现有手册的关系

旧版[状态键字典](../../reference/cluster-observability/05-pg-cluster-state-key-dictionary.md)和[计数器字典](../../reference/cluster-observability/06-cluster-counter-dictionary.md)可用于查找既有键；新键及可用性以精确安装的二进制为准。旧运维页建议按 `category` 过滤降低采集成本的说法不适用于完整 materialize 的 `pg_cluster_state`：过滤只减少返回给客户端的行。

相关页面：[参数](parameters.md)、[等待事件](wait-events.md)、[安装](../install.md)。
