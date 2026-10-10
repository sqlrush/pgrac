# PRE2 参数参考

目标版本：`v0.135.0`（PRE2 功能评估技术预览，尚未发布）。本页已核对的参数定义来自 `v0.133.1-pre2.1`，不代表新版本或安装包已经发布。

本页列出已核对代码相对原生 PostgreSQL 增加的全部 `cluster.*` 注册项。默认值是**代码默认值**，不是安装向导生成的共享集群配置；运行值以各节点的 `pg_settings` 为准。

普通集群构建注册 254 项 `cluster.*` 参数；启用注入测试的构建另有 1 项 `cluster.*` 参数，本页一起列出并标注。参数存在或取值被接受，不表示相关功能已经纳入本次技术预览。共享部署只使用[安装手册](../install.md)提供的配置，保持固定成员与全新建库；在线加入、移除、单节点计划退出、ADG 和媒体恢复入口不作为本预览的使用流程。

## PRE2 新增与变化

本页的“新增／变化”以 PRE1 稳定版 `v0.132.0` 为对照，而不是以原生 PG 为对照：所有 `cluster.*` 均非原生 PG 参数。

| 项目 | PRE2 变化 |
|---|---|
| `cluster.shared_config` | 新增，共享配置开关，由创建入口设置。 |
| `cluster.storage_quorum_nodes` | 新增，数据库节点与存储集群节点的映射。 |
| `cluster.storage_quorum_cluster` | 新增，存储集群名称。 |
| `cluster.cold_recovery_plan_memory` | 新增，冷恢复计划内存上限。 |
| `cluster.shared_catalog`、`cluster.sinval_ack_mode` | 新增组合检查：共享目录必须启用失效确认，不能选择 `none`。 |
| 共享配置的在线修改 | 新增共享 `ALTER SYSTEM` 行为。许多原本显示为 `sighup` 的集群公共参数（所有节点采用同一值），在共享模式中需要重启才能生效。不能仅据 `pg_settings.context` 判断。 |

表中“—”表示参数的默认值、范围、修改方式和配置检查与上述对照相同，不表示相关功能的全部行为都未变化。

## 查看与修改

```sql
SELECT name, setting, unit, boot_val, reset_val, min_val, max_val,
       enumvals, context, source, pending_restart
FROM pg_settings
WHERE name LIKE 'cluster.%'
ORDER BY name;
```

- **创建时**：只在受支持的全新共享建库输入中指定；共享 `ALTER SYSTEM` 拒绝修改，重启也不能把它变成可修改项。
- **重启**：可以发布允许的共享配置，运行中的实例继续使用原值；按部署流程正常全停并重启后核对各节点。即使 `context='sighup'`，共享模式下也不能靠 `pg_reload_conf()` 使这些集群公共参数生效。
- **SET**：当前会话可以修改；“管理员”表示需要超级用户或该参数的适当 `SET` 权限。`SET LOCAL` 仅作用于当前事务。它不会同时改变其他会话或后台进程。
- **只读**：不能用 `SET` 或 `ALTER SYSTEM` 修改。

下面是会话级诊断示例；读取全部状态有额外成本，参见[命令参考](commands.md#pg_cluster_state-的用法)。

```sql
SET cluster.xnode_profile = on;
SHOW cluster.xnode_profile;
RESET cluster.xnode_profile;
```

共享 `ALTER SYSTEM` 将大多数允许的参数发布为集群公共值；`cluster.external_fence_socket_path` 为当前实例值。共享模式拒绝 `RESET ALL`、创建时参数、未支持的字符串参数和注入配置。不能用 `postgres -c cluster.lms_workers=...` 等高优先级参数覆盖共享创建输入。发布成功与所有节点已采用新值是两回事；修改后在每个节点核对运行值和 `pending_restart`。

字符串中，`""` 表示空字符串，“未设置”表示未提供默认字符串。时间／容量列给出注册单位；名称带 `_ms` 但无注册单位的参数仍按说明使用整数毫秒，不能假定它接受 `1s` 这样的单位后缀。以下容量上限按本预览 64 位构建填写。

## 必须保留的共享配置

- `cluster.undo_cleaner_enabled=on`。
- `cluster.lms_workers=2`。
- `cluster.smart_fusion=off`。
- 保持 `cluster.enabled`、`cluster.merged_recovery`、`cluster.smgr_user_relations`、`cluster.controlfile_shared_authority`、`cluster.shared_config`、`cluster.shared_catalog`、`cluster.undo_gcs_coherence` 和 `cluster.crossnode_runtime_visibility` 全部为 `on`；使用创建入口提供的整组配置，不通过单独切换其中一项迁移已有数据目录。
- 必须使用 `cluster.shared_storage_backend=cluster_fs`，并保留创建时的共享目录、存储身份和路径配置。
- `autovacuum=off`，与本预览的安装模板一致。
- `cluster.storage_quorum_nodes` 使用 `数据库节点号:Corosync节点号` 的逗号分隔映射，例如 `0:1,1:2,2:3,3:4`；实际值必须与固定成员配置一致。不要将示例直接用于不同部署。
- `cluster.voting_disks`、恢复目标、裸设备路径等未列入共享字符串配置的旧入口仍在参数注册表中，但不能作为本预览共享 `ALTER SYSTEM` 的有效输入。投票盘与存储身份按安装流程提供。
- 下表未列出的组合限制仍会在配置或启动时检查；收到拒绝时保留错误详情，不通过关掉一致性或隔离开关绕过。

## 建库后固定的原生参数

下列是 PostgreSQL 原生参数，但在本预览的共享库中必须在全新建库前确定；建库后不能通过 `ALTER SYSTEM`、重载或修改启动配置改变它们：

| 参数 | 使用要求 |
|---|---|
| `max_connections` | 创建前确定连接容量并预留管理连接；创建后保持不变。 |
| `max_worker_processes` | 保留创建时的后台工作进程容量。 |
| `max_wal_senders` | 保留创建时的 WAL 发送进程容量；本预览不提供复制部署流程。 |
| `max_prepared_transactions` | 保留创建时的值；设置非零不代表共享模式支持两阶段事务。 |
| `max_locks_per_transaction` | 保留创建时的锁容量配置。 |
| `wal_level` | 按共享安装模板使用 `replica`；共享目录拒绝 `minimal`，共享配置拒绝 `logical`。 |
| `wal_log_hints` | 保留创建时的值。 |
| `track_commit_timestamp` | 必须为 `off`；共享模式拒绝 `on`。 |

## 参数表

“在线修改”列适用于本预览的共享配置模式；非共享运行的原生上下文见各节点 `pg_settings.context`。作用栏用中文说明本预览常用参数；其余保留部分英文帮助说明，可按参数名与 `pg_settings.short_desc` 对照。

### 创建、存储、成员及启动

| 名称 | 类型 | 默认值 | 范围／单位 | 在线修改 | 作用 | PRE2 |
|---|---|---|---|---|---|---|
| `cluster.node_id` | 整数 | `-1` | -1–127 | 创建时 | 本实例编号；-1 表示未指定集群节点。交付配置必须指定实际节点编号。 | — |
| `cluster.config_file` | 字符串 | `"pgrac.conf"` | 字符串；另受格式／共享配置限制 | 创建时 | 节点拓扑配置文件路径。 | — |
| `cluster.wal_threads_dir` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 创建时 | 共享 WAL 根目录；非空值必须是绝对路径。共享部署使用创建时的同一路径。 | — |
| `cluster.recovery_stale_active_ms` | 整数 | `10000` | 1000–3600000 ms | 重启 | 将仍标记为活动的 WAL 状态列为异常候选前，允许其停止更新的时长。 | — |
| `cluster.recovery_workers_max` | 整数 | `4` | 0–16 | 重启 | 恢复时并行验证日志流的工作进程数上限。 | — |
| `cluster.merged_recovery` | 布尔 | `off` | on / off | 重启 | 启动恢复时，按全局顺序号合并多个节点的日志；共享部署必须保持 on。 | — |
| `cluster.cold_recovery_plan_memory` | 整数 | `4194304` | 1024–2147483647 kB | 重启 | 冷恢复计划的内存上限（kB），不是启动时预分配的内存量。 | 新增 |
| `cluster.xnode_profile` | 布尔 | `off` | on / off | SET（管理员） | 采集跨节点性能计数及计时；在 pg_cluster_state 的 xnode_profile 类读取。 | — |
| `cluster.update_trace` | 布尔 | `off` | on / off | SET（管理员） | 采集 UPDATE 各执行阶段的诊断信息；默认关闭。 | — |
| `cluster.space_affinity` | 枚举 | `off` | off, static, dynamic | 重启 | 关系扩展时的数据空间分配偏好：关闭、静态分配或动态分配。 | — |
| `cluster.space_lease_blocks` | 整数 | `64` | 1–8192 | 重启 | 每次为一个节点分配的数据空间号段包含的块数。 | — |
| `cluster.clean_leave_enabled` | 布尔 | `off` | on / off | 重启 | Enable cooperative clean-leave reconfiguration. | — |
| `cluster.clean_leave_drain_timeout_ms` | 整数 | `30000` | 1000–600000 ms | 重启 | Fail-closed deadline for a clean leave's cooperative drain. | — |
| `cluster.recovery_merge_wait_timeout` | 整数 | `10000` | 0–600000 ms | 重启 | 开始合并恢复前，等待日志流验证工作进程的时长。 | — |
| `cluster.shared_storage_backend` | 枚举 | `stub` | stub, local, block_device, cluster_fs, rbd, multi_attach | 创建时 | 共享存储后端；本预览必须使用 cluster_fs。 | — |
| `cluster.write_fence_enforcement` | 枚举 | `on` | off, on, dev | 重启 | 共享写入隔离的执行模式。 | — |
| `cluster.write_fence_lease_ms` | 整数 | `6000` | 1000–600000 ms | 重启 | 共享写入资格的租约时长，单位毫秒。 | — |
| `cluster.external_fence_socket_path` | 字符串 | `"/var/run/pgrac/pgrac-fenced.sock"` | 字符串；另受格式／共享配置限制 | 重启（本实例） | 本实例连接隔离服务的 Unix socket 绝对路径。 | — |
| `cluster.external_fence_acquire_timeout_ms` | 整数 | `120000` | 1–600000 ms | 重启 | 取得外部写入隔离证明的总等待期限，单位毫秒。 | — |
| `cluster.shared_data_dir` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 创建时 | 共享数据根目录；非空值必须是绝对路径，cluster_fs 要求配置此目录。 | — |
| `cluster.controlfile_shared_authority` | 布尔 | `off` | on / off | 创建时 | 使用共享数据目录中的统一控制信息；共享部署必须保持 on。 | — |
| `cluster.storage_quorum_nodes` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 创建时 | 数据库节点号到 Corosync 节点号的映射，格式见本页“必须保留的共享配置”。 | 新增 |
| `cluster.storage_quorum_cluster` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 创建时 | 预期 Corosync 集群名称，必须与部署中的名称完全一致。 | 新增 |
| `cluster.shared_config` | 布尔 | `off` | on / off | 创建时 | 启用共享配置；由全新共享建库入口设置，不能把已有普通 PG 数据目录改为共享库。 | 新增 |
| `cluster.shared_storage_uuid` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 创建时 | 共享存储的外部身份标识；本预览由创建入口设置并固定。 | — |
| `cluster.block_device_path` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 重启；共享持久配置拒绝 | Raw block-device path for the block_device shared-storage backend. | — |
| `cluster.block_device_use_odirect` | 布尔 | `on` | on / off | 重启 | Require direct I/O for the raw block-device backend. | — |
| `cluster.storage_fence_driver` | 枚举 | `auto` | disabled, auto, scsi3_pr | 重启 | 共享存储隔离驱动的选择。 | — |
| `cluster.smgr_user_relations` | 布尔 | `off` | on / off | 重启 | 让永久关系使用集群共享存储。 | — |
| `cluster.shared_catalog` | 布尔 | `off` | on / off | 创建时 | 启用共享系统目录；必须使用受支持的全新共享建库配置，并保留失效确认。 | 组合检查变化 |
| `cluster.oid_lease_size` | 整数 | `8192` | 1024–1048576 | 重启 | 节点每次从共享分配器取得的 OID 数量。 | — |
| `cluster.shmem_max_regions` | 整数 | `96` | 40（注入构建 41）–256 | 重启 | 集群共享内存区域注册表的容量。 | — |
| `cluster.phase1_timeout` | 整数 | `60` | 1–3600 s | 重启 | 等待集群基础服务启动的期限，单位秒。 | — |
| `cluster.phase2_timeout` | 整数 | `30` | 1–3600 s | 重启 | 等待集群锁服务启动的期限，单位秒。 | — |
| `cluster.phase3_timeout` | 整数 | `600` | 60–3600 s | 重启 | 等待恢复阶段完成的期限，单位秒。 | — |
| `cluster.phase4_timeout` | 整数 | `30` | 1–3600 s | 重启 | 等待正常服务启动阶段完成的期限，单位秒。 | — |
| `cluster.touched_peers_trace` | 布尔 | `off` | on / off | SET（管理员） | Log the touched-peers set of each transaction aborted by a fail-stop reconfiguration. | — |
| `cluster.cssd_main_loop_interval_ms` | 整数 | `1000` | 100–60000 ms | 重启 | 集群成员监控进程的主循环间隔，单位毫秒。 | — |
| `cluster.cssd_heartbeat_interval_ms` | 整数 | `1000` | 100–10000 ms | 重启 | 集群成员心跳广播间隔，单位毫秒。 | — |
| `cluster.cssd_dead_deadband_factor` | 整数 | `3` | 2–10 | 重启 | 判断节点失联所用的等待间隔倍数，以心跳间隔为基准。 | — |
| `cluster.voting_disks` | 字符串 | `未设置` | 字符串；另受格式／共享配置限制 | 重启；共享持久配置拒绝 | 逗号分隔的投票盘路径；本预览按安装流程提供，不通过此旧入口配置。 | — |
| `cluster.quorum_poll_interval_ms` | 整数 | `2000` | 500–30000 ms | 重启 | 轮询投票盘的间隔，单位毫秒。 | — |
| `cluster.voting_disk_io_timeout_ms` | 整数 | `5000` | 500–60000 ms | 重启 | 单次投票盘 I/O 的超时，单位毫秒。 | — |
| `cluster.voting_disk_size_bytes` | 整数 | `394240` | 4096–1048576 B | 重启 | 投票盘文件大小，单位字节。 | — |
| `cluster.online_join` | 布尔 | `off` | on / off | 重启 | Allow a declared node to join/rejoin live membership online (without a full cluster restart). | — |
| `cluster.join_remaster_enabled` | 布尔 | `off` | on / off | 重启 | On node rejoin, move the joiner's home-shard GES mastership back from the survivor (optional rebalance). | — |
| `cluster.join_convergence_timeout_ms` | 整数 | `30000` | 5000–120000 ms | 重启 | Deadline for an online join to converge + commit. | — |
| `cluster.online_node_removal` | 布尔 | `off` | on / off | 重启 | Enable permanent removal (decommission) of a declared node. | — |
| `cluster.node_removal_cleanup_timeout_ms` | 整数 | `30000` | 5000–120000 ms | 重启 | Deadline for the post-shrink cluster-wide removal cleanup. | — |
| `cluster.self_fence_enabled` | 布尔 | `on` | on / off | 重启 | 持续丧失多数资格时，让本节点自行停止服务。 | — |
| `cluster.self_fence_grace_ms` | 整数 | `30000` | 1000–300000 ms | 重启 | 持续丧失多数资格后，本节点停止服务前的宽限时间，单位毫秒。 | — |
| `cluster.freeze_writes_enabled` | 布尔 | `on` | on / off | 重启 | 启用写入冻结通知，使在途事务中止。 | — |
| `cluster.fence_audit_log` | 枚举 | `log` | off, log, debug | 重启 | 隔离相关日志的输出级别。 | — |
| `cluster.enabled` | 布尔 | `on` | on / off | 创建时 | 启用集群运行模式。 | — |
| `cluster.allow_single_node` | 布尔 | `on` | on / off | 重启 | Allow pgrac to start in single-node mode (no pgrac.conf or invalid cluster.node_id). | — |
| `cluster.online_thread_recovery` | 布尔 | `off` | on / off | 重启 | Let a survivor online-replay a dead WAL thread's data to shared storage in the reconfig freeze window instead of waiting for the dead node's cold restart. | — |
| `cluster.thread_recovery_on_unrecoverable` | 枚举 | `keep_frozen` | keep_frozen, panic | 重启 | Action when a dead thread cannot be online-recovered. | — |

### 互联和后台进程

| 名称 | 类型 | 默认值 | 范围／单位 | 在线修改 | 作用 | PRE2 |
|---|---|---|---|---|---|---|
| `cluster.interconnect_tier` | 枚举 | `stub` | stub, mock, tier1, tier2, tier3 | 重启 | 节点互联方式；本预览按安装模板使用 tier1（TCP）。 | — |
| `cluster.interconnect_rdma_fallback` | 枚举 | `auto` | auto, off | 重启 | Policy for RDMA-to-TCP interconnect fallback. | — |
| `cluster.interconnect_rdma_provider` | 枚举 | `auto` | auto, verbs, mlx5 | 重启 | RDMA provider selection for tier2/tier3 interconnect. | — |
| `cluster.interconnect_rdma_completion` | 枚举 | `event` | event, busypoll | 重启 | RDMA completion model for the interconnect. | — |
| `cluster.interconnect_rdma_busypoll_us` | 整数 | `50` | 0–10000 | 重启 | RDMA busy-poll spin budget in microseconds. | — |
| `cluster.interconnect_rdma_crc_offload` | 布尔 | `off` | on / off | 重启 | Reserved RDMA control-plane CRC offload switch. | — |
| `cluster.interconnect_rdma_inline_max` | 整数 | `256` | 0–4096 B | 重启 | RDMA inline-send threshold in bytes. | — |
| `cluster.interconnect_rdma_max_send_wr` | 整数 | `256` | 16–4096 | 重启 | RDMA send work request queue depth per peer. | — |
| `cluster.lmd_probe_collect_timeout_ms` | 整数 | `3000` | 100–30000 ms | 重启 | 收集跨节点死锁检查结果的期限，单位毫秒。 | — |
| `cluster.lmd_cleanup_sweep_interval_ms` | 整数 | `5000` | 100–60000 ms | 重启 | 定期清理已退出会话锁状态的间隔，单位毫秒。 | — |
| `cluster.lms_native_lock_probe_max_inflight` | 整数 | `8` | 1–64 | 重启 | 每个资源分片同时收集的本地锁检查请求数上限。 | — |
| `cluster.lms_native_lock_probe_retry_interval_ms` | 整数 | `500` | 50–60000 ms | 重启 | 对端持锁、排队冲突或响应超时时，重试锁检查的间隔，单位毫秒。 | — |
| `cluster.lms_native_lock_probe_retry_budget` | 整数 | `60` | 1–3600 | 重启 | 每个请求者累计重试锁检查的上限；无法完成检查时返回 53R83。 | — |
| `cluster.lmon_main_loop_interval` | 整数 | `1000` | 100–60000 ms | 重启 | 集群管理进程 LMON 的主循环间隔，单位毫秒。 | — |
| `cluster.lmon_slow_iteration_warn_ms` | 整数 | `1000` | 0–60000 ms | 重启 | LMON 单次主循环的慢处理警告阈值，单位毫秒。 | — |
| `cluster.lck_main_loop_interval` | 整数 | `1000` | 100–60000 ms | 重启 | 锁管理辅助进程 LCK 的主循环间隔，单位毫秒。 | — |
| `cluster.diag_main_loop_interval` | 整数 | `1000` | 100–60000 ms | 重启 | 诊断进程的主循环间隔，单位毫秒。 | — |
| `cluster.interconnect_heartbeat_interval_ms` | 整数 | `1000` | 100–60000 ms | 重启 | TCP 互联的心跳间隔，单位毫秒。 | — |
| `cluster.interconnect_connect_timeout_ms` | 整数 | `5000` | 1000–60000 ms | 重启 | TCP 互联主动建连的等待期限，单位毫秒。 | — |
| `cluster.interconnect_recv_timeout_ms` | 整数 | `30000` | 1000–600000 ms | 重启 | TCP 互联从一个对端接收数据的等待期限，单位毫秒。 | — |
| `cluster.interconnect_payload_max_bytes` | 整数 | `67108864` | 16777216–268435456 B | 重启 | 集群分片传输所接受的消息内容大小上限，单位字节。 | — |
| `cluster.interconnect_chunk_reassembly_timeout_ms` | 整数 | `10000` | 1000–60000 ms | 重启 | 等待分片消息拼接完整的期限，单位毫秒。 | — |
| `cluster.interconnect_tcp_keepidle_sec` | 整数 | `60` | 30–600 s | 重启 | TCP 连接空闲后开始存活探测的时间，单位秒。 | — |
| `cluster.interconnect_tcp_keepintvl_sec` | 整数 | `10` | 10–60 s | 重启 | TCP 存活探测的发送间隔，单位秒。 | — |
| `cluster.interconnect_tcp_keepcnt` | 整数 | `6` | 3–20 | 重启 | TCP 存活探测失败的次数上限。 | — |
| `cluster.cluster_stats_main_loop_interval` | 整数 | `1000` | 100–60000 ms | 重启 | 集群统计进程的主循环间隔，单位毫秒。 | — |
| `cluster.lmd_enabled` | 布尔 | `on` | on / off | 重启 | 启用负责死锁检测的 LMD 后台进程。 | — |
| `cluster.lms_enabled` | 布尔 | `on` | on / off | 重启 | 启用负责跨节点锁授予服务的 LMS 后台进程。 | — |
| `cluster.lms_workers` | 整数 | `2` | 1–8 | 重启 | LMS 数据通道进程数（包括 worker 0）；本预览设置为 2。 | — |
| `cluster.lms_nice` | 整数 | `0` | -20–0 | 重启 | LMS 数据通道进程的调度 nice 值；0 表示不调整。 | — |
| `cluster.lmd_max_wait_edges` | 整数 | `1024` | 64–65536 | 重启 | 死锁检测保存的锁等待关系数上限。 | — |
| `cluster.lmd_scan_interval_ms` | 整数 | `1000` | 50–60000 | 重启 | 死锁检测扫描间隔，单位毫秒。 | — |
| `cluster.ic_duty_lazy` | 布尔 | `on` | on / off | 重启 | 按需处理可延后的集群管理工作，而不是在每次主循环中都处理。 | — |

### 备份、时间点恢复及备用库（本预览不提供完整操作支持）

| 名称 | 类型 | 默认值 | 范围／单位 | 在线修改 | 作用 | PRE2 |
|---|---|---|---|---|---|---|
| `cluster.dg_role` | 枚举 | `primary` | primary, standby | 重启 | Cluster Data Guard role for this instance. | — |
| `cluster.dg_mode` | 枚举 | `async` | async, sync, max_availability | 重启 | Cluster Data Guard shipping acknowledgement mode. | — |
| `cluster.enable_adg` | 布尔 | `off` | on / off | 重启 | Enable ADG standby apply and read-only service. | — |
| `cluster.apply_master_election` | 布尔 | `on` | on / off | 重启 | Enable automatic ADG Apply Master election. | — |
| `cluster.adg_rfs_conninfos` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 重启；共享持久配置拒绝 | ADG RFS upstream connection strings. | — |
| `cluster.adg_primary_thread_count` | 整数 | `0` | 0–128 | 只读 | Primary ADG WAL thread count. | — |
| `cluster.adg_lag_threshold_sec` | 整数 | `10` | 1–300 s | 重启 | ADG apply lag threshold for read-only service errors. | — |
| `cluster.max_standby_delay` | 整数 | `30` | -1–86400 s | 重启 | Maximum delay before ADG read-only queries yield to apply. | — |
| `cluster.apply_master_switch_drain_ms` | 整数 | `5000` | 0–600000 ms | 重启 | ADG Apply Master switch drain window. | — |
| `cluster.adg_lease_takeover_grace_ms` | 整数 | `5000` | 0–600000 ms | 重启 | Grace period past apply-master lease expiry before takeover. | — |
| `cluster.adg_barrier_interval_ms` | 整数 | `1000` | 0–300000 ms | 重启 | ADG consistency barrier interval. | — |
| `cluster.wal_sender_timeout_sec` | 整数 | `60` | 1–3600 s | 重启 | ADG LNS WAL sender timeout. | — |
| `cluster.wal_receiver_timeout_sec` | 整数 | `60` | 1–3600 s | 重启 | ADG RFS WAL receiver timeout. | — |
| `cluster.recovery_target_scn` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 重启；共享持久配置拒绝 | Cluster PITR target SCN. | — |
| `cluster.recovery_target_cluster_time` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 重启；共享持久配置拒绝 | Cluster PITR target timestamp. | — |
| `cluster.recovery_target_name` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 重启；共享持久配置拒绝 | Cluster PITR named restore point target. | — |
| `cluster.recovery_target_action` | 枚举 | `pause` | pause, promote, shutdown | 重启 | Action to take when a cluster PITR target is reached. | — |
| `cluster.enable_pitr_restore_points` | 布尔 | `off` | on / off | 重启 | Enable automatic cluster restore point creation. | — |
| `cluster.pitr_restore_point_interval_ms` | 整数 | `0` | 0–86400000 ms | 重启 | Interval for automatic cluster PITR restore points. | — |
| `cluster.restore_point_drain_timeout_ms` | 整数 | `30000` | 1–600000 ms | SET（管理员） | Timeout for cluster restore-point commit drain. | — |
| `cluster.backup_wal_retention` | 整数 | `0` | 0–2147483647 MB | 重启 | Cluster backup WAL retention hint in megabytes. | — |
| `cluster.backup_parallel_channels` | 整数 | `1` | 1–128 | 重启 | Maximum cluster backup copy channels. | — |
| `cluster.backup_manifest_checksums` | 枚举 | `crc32c` | crc32c | 重启 | Checksum mode for cluster backup manifests. | — |

### 事务、undo、序列及事务状态

| 名称 | 类型 | 默认值 | 范围／单位 | 在线修改 | 作用 | PRE2 |
|---|---|---|---|---|---|---|
| `cluster.page_scn_shortcut` | 布尔 | `off` | on / off | SET（管理员） | 启用跨节点可见性终态结果的会话内复用。 | — |
| `cluster.crossnode_runtime_visibility` | 布尔 | `off` | on / off | SET（管理员） | 启用运行中跨实例事务可见性判定；共享部署保持 on。 | — |
| `cluster.xid_striping` | 布尔 | `off` | on / off | 重启 | 按节点划分事务号分配范围，避免不同节点分配相同事务号。 | — |
| `cluster.multi_xmax_remote_resolve` | 布尔 | `on` | on / off | 重启 | 通过集群中的成员状态记录，判断其他节点的多事务行锁状态。 | — |
| `cluster.xid_herding_slack` | 整数 | `4194304` | 65536–268435456 | 重启 | 各节点事务号分配位置允许相差的最大范围。 | — |
| `cluster.undo_gcs_coherence` | 布尔 | `off` | on / off | 创建时 | 启用共享 undo 块的一致性访问；要求已配置 shared_data_dir。共享部署保持 on。 | — |
| `cluster.undo_retention_horizon_enabled` | 布尔 | `on` | on / off | 重启 | 保留已提交事务的 undo 和事务表槽位，直到活动读者不再需要它们。 | — |
| `cluster.undo_buffers` | 整数 | `2048` | 0–1048576 | 重启 | undo 数据块及段头块的缓冲帧数量。 | — |
| `cluster.undo_buffer_writeback` | 布尔 | `on` | on / off | 重启 | 本地 undo 缓冲写回开关；多节点配置下不生效，使用写穿路径。 | — |
| `cluster.undo_writeback_boundary_check` | 枚举 | `on` | off, on, strict | 重启 | undo 检查点写回的附加检查级别；不能用于关闭必要的持久性检查。 | — |
| `cluster.undo_cleaner_interval_ms` | 整数 | `30000` | 0–3600000 | 重启 | undo 清理进程两轮清理之间的间隔，单位毫秒。 | — |
| `cluster.undo_cleaner_enabled` | 布尔 | `on` | on / off | 重启 | 启用 undo、事务槽及事务引用的清理；本预览设置为 on。 | — |
| `cluster.undo_cleaner_batch_segments` | 整数 | `8` | 1–256 | 重启 | 每轮清理最多扫描的本节点 undo 段数。 | — |
| `cluster.undo_record_segment_commit_on_rollover` | 布尔 | `on` | on / off | 重启 | 切换 undo 记录段时，将已排空段的状态由活动改为已提交。 | — |
| `cluster.undo_segments_per_instance` | 整数 | `16` | 1–1024 | 重启 | 为每个集群实例预留的 undo 段数。 | — |
| `cluster.undo_tablespace_path` | 字符串 | `"pg_undo"` | 字符串；另受格式／共享配置限制 | 创建时 | 本实例 undo 表空间的路径配置；共享建库采用安装入口提供的路径，不自行修改。 | — |
| `cluster.undo_segment_size_mb` | 整数 | `32` | 8–1024 | 重启 | 每个 undo 段文件的大小，单位 MB。 | — |
| `cluster.undo_record_inline_max_bytes` | 整数 | `1024` | 16–8192 | 重启 | 一条 undo 记录中直接存放的数据大小上限，单位字节。 | — |
| `cluster.undo_extent_blocks` | 整数 | `4` | 1–256 | 重启 | 每个事务每次申请的 undo 块数。 | — |
| `cluster.undo_segments_max_per_instance` | 整数 | `256` | 16–256 | 重启 | 每个实例可使用的 undo 段数硬上限。 | — |
| `cluster.undo_segment_create_timeout_ms` | 整数 | `5000` | 100–60000 | 重启 | undo 段文件创建及首次同步的耗时上限，单位毫秒。 | — |
| `cluster.tt_durable_lookup` | 布尔 | `on` | on / off | SET | 内存事务状态未命中时，从 undo 段头中的持久事务表槽位查询提交顺序号。 | — |
| `cluster.tt_recovery_resolve_active` | 布尔 | `on` | on / off | 重启 | 启动时，将崩溃遗留的活动事务槽解析为已中止状态。 | — |
| `cluster.boc_sweep_interval_ms` | 整数 | `100` | 1–1000 ms | 重启 | 日志写进程更新持久提交进度的目标间隔，单位毫秒。 | — |
| `cluster.boc_event_publish` | 布尔 | `on` | on / off | 重启 | 随提交事件发布已持久化的提交进度。 | — |
| `cluster.scn_max_propagation_lag_ms` | 整数 | `5000` | 100–60000 ms | 重启 | 提交顺序号在实例间传播的最大允许延迟，单位毫秒。 | — |
| `cluster.tt_status_overlay_max_entries` | 整数 | `32768` | 1024–1048576 | 重启 | 内存事务状态表的条目数上限。 | — |
| `cluster.tt_status_overlay_ttl_ms` | 整数 | `30000` | 1000–600000 | 重启 | 内存事务状态条目的有效期，单位毫秒。 | — |
| `cluster.subtrans_max_chain_depth` | 整数 | `32` | 4–1024 | 重启 | 查询子事务状态时，沿父事务关系追溯的最大深度。 | — |
| `cluster.multixact_member_overlay_max_members` | 整数 | `32` | 4–256 | 重启 | 单条消息包含的多事务行锁成员数上限。 | — |
| `cluster.multixact_member_overlay_max_entries` | 整数 | `16384` | 1024–1048576 | 重启 | 多事务行锁成员状态表的条目数上限。 | — |
| `cluster.multixact_hint_outbound_slots` | 整数 | `1024` | 128–8192 | 重启 | 待发送的多事务行锁状态队列容量。 | — |
| `cluster.tt_status_hint_outbound_capacity` | 整数 | `256` | 64–4096 | 重启 | 待发送的事务状态提示队列容量。 | — |
| `cluster.tt_status_hint_emit_mode` | 枚举 | `all_status` | disabled, all_status | 重启 | 跨节点事务状态提示的发送方式：关闭或发送全部状态。 | — |
| `cluster.sequence_default_cache` | 整数 | `100` | 1–1000000000 | SET（管理员） | 集群模式中新建序列的默认 CACHE 大小。 | — |
| `cluster.sequence_cache_floor_optin` | 整数 | `0` | 0–1000000000 | 重启 | 已有序列在运行时使用的 CACHE 大小下限；0 表示不启用。 | — |
| `cluster.sequence_refill_timeout_ms` | 整数 | `30000` | 1000–600000 ms | 重启 | 等待补充分配序列号段的期限，无法完成时拒绝分配。 | — |

### 块访问与一致读

| 名称 | 类型 | 默认值 | 范围／单位 | 在线修改 | 作用 | PRE2 |
|---|---|---|---|---|---|---|
| `cluster.read_scache` | 布尔 | `off` | on / off | 重启 | 允许将不再写入的块降为共享读权限，并在本节点缓存该权限。 | — |
| `cluster.crossnode_cr_data_plane` | 布尔 | `off` | on / off | SET（管理员） | 启用跨实例一致读服务。 | — |
| `cluster.block_self_contained` | 布尔 | `off` | on / off | SET（管理员） | 已弃用的兼容参数，修改无效果。 | — |
| `cluster.past_image` | 布尔 | `off` | on / off | SET（管理员） | 块移交或失效时保留该块的历史镜像。 | — |
| `cluster.cr_chain_walk_max_steps` | 整数 | `4096` | 64–65536 | 重启 | 构造一个一致读块时，沿 undo 链回溯的步数上限。 | — |
| `cluster.cr_mvcc_gate` | 布尔 | `on` | on / off | SET | 启用本实例的一致读可见性快速判断。 | — |
| `cluster.cr_gate_no_peer_fastpath` | 布尔 | `on` | on / off | SET | 在无对端且快照仅用于本会话时使用本地 MVCC 判断。 | — |
| `cluster.cr_tuple_level_fastpath` | 布尔 | `off` | on / off | SET | 对只有一条候选版本链的块启用逐行可见性判断快速路径；默认关闭。 | — |
| `cluster.cf_terminal_authority` | 布尔 | `off` | on / off | 重启 | 启用持久事务状态与 undo 的跨节点终态判定。 | — |
| `cluster.cf_delayed_cleanout` | 枚举 | `reader` | off, reader, eager | 重启 | 事务槽提交状态清理策略：关闭、读时清理、主动清理。 | — |
| `cluster.smart_fusion` | 布尔 | `off` | on / off | 重启 | 提前传块功能开关；本预览设置为 off。 | — |
| `cluster.smart_fusion_tier_min` | 枚举 | `tier3` | tier3 | 重启 | Minimum interconnect tier that may use Smart Fusion early transfer. | — |
| `cluster.smart_fusion_commit_brake_timeout_ms` | 整数 | `5000` | 1–600000 ms | 重启 | Timeout for the Smart Fusion pre-commit dependency brake. | — |
| `cluster.smart_fusion_origin_durable_gossip_ms` | 整数 | `50` | 1–60000 ms | 重启 | Interval for publishing local durable WAL progress to Smart Fusion peers. | — |
| `cluster.cr_cache_max_blocks` | 整数 | `64` | 0–4096 | SET | 每个会话的一致读块缓存容量，按 8 KB 块计；0 表示关闭。 | — |
| `cluster.shared_cr_pool_enabled` | 布尔 | `off` | on / off | 重启 | 启用供本实例多个会话共用的一致读缓冲池。 | — |
| `cluster.shared_cr_pool_size_blocks` | 整数 | `0` | 0–262144 | 重启 | 共享一致读缓冲池容量，按 8 KB 块计；0 表示关闭且不分配内存。 | — |
| `cluster.cr_pool_rel_generation_slots` | 整数 | `0` | 0–262144 | 重启 | 一致读缓存中跟踪关系存储代次的表容量；0 表示关闭精细跟踪。 | — |
| `cluster.cr_pool_admission_policy` | 枚举 | `admit_all` | admit_all, no_admit, scan_resistant | 重启 | 共享一致读缓存的写入策略：全部接纳、不接纳、抑制扫描污染。 | — |
| `cluster.cr_pool_admit_relation_backend_cap` | 整数 | `0` | 0–1048576 | 重启 | 每个会话为单个关系写入一致读缓存的数量上限；0 表示不设此限制。 | — |
| `cluster.cr_pool_admit_pressure_ratio` | 整数 | `0` | 0–100000 | 重启 | 限制新块进入一致读缓存的压力阈值，以淘汰数与命中数之比的百分比表示；0 表示关闭此限制。 | — |
| `cluster.resolver_cache_enabled` | 布尔 | `off` | on / off | 重启 | 启用经重新验证后可使用的共享事务状态缓存。 | — |
| `cluster.resolver_cache_measure` | 布尔 | `off` | on / off | 重启 | 仅测量共享事务状态缓存；不使用缓存结果替代原判定。 | — |
| `cluster.resolver_cache_entries` | 整数 | `0` | 0–1048576 | 重启 | 共享事务状态缓存的条目数；0 表示关闭且不分配内存。 | — |
| `cluster.cross_instance_cr_coordinator` | 枚举 | `boundary` | off, boundary, forward | 重启 | 跨实例一致读协调的观测模式。 | — |
| `cluster.cross_instance_cr_probe` | 布尔 | `off` | on / off | SET | 记录跨实例一致读的诊断命中计数。 | — |
| `cluster.gcs_reply_timeout_ms` | 整数 | `5000` | 100–60000 | SET（管理员） | 等待跨节点传块回复的期限，单位毫秒。 | — |
| `cluster.gcs_block_recovery_wait_ms` | 整数 | `200` | 0–60000 | 重启 | 请求的块正在恢复时，返回 53R9L 前的等待时长，单位毫秒。 | — |
| `cluster.gcs_block_retransmit_max_retries` | 整数 | `4` | 0–8 | SET（管理员） | 首次传块回复超时后允许重试的次数上限。 | — |
| `cluster.gcs_block_local_cache` | 布尔 | `on` | on / off | SET（管理员） | 在本节点缓存数据块访问权限，直到被撤回。 | — |
| `cluster.tx_enqueue_wait` | 布尔 | `on` | on / off | SET（管理员） | 遇到远端行锁时等待持锁事务结束。 | — |
| `cluster.crossnode_write_write` | 布尔 | `off` | on / off | SET（管理员） | 远端写入事务已结束后，允许本地写入衔接其行版本。 | — |
| `cluster.gcs_block_retransmit_initial_backoff_ms` | 整数 | `10` | 1–5000 | SET（管理员） | 传块第一次重试前的等待间隔，后续重试间隔加倍，单位毫秒。 | — |
| `cluster.gcs_block_dedup_max_entries` | 整数 | `16384` | 256–65536 | 重启 | 负责管理数据块的节点保存的传块请求去重条目数上限。 | — |
| `cluster.gcs_block_invalidate_ack_timeout_ms` | 整数 | `1500` | 100–60000 | SET（管理员） | 块管理节点等待一次权限失效确认的期限，单位毫秒。 | — |
| `cluster.gcs_block_starvation_backoff_ms` | 整数 | `100` | 1–60000 | SET（管理员） | 已有写请求排队时，读请求被拒后重试的基础等待间隔，单位毫秒。 | — |
| `cluster.gcs_block_starvation_max_retries` | 整数 | `8` | 0–64 | SET（管理员） | 保留的兼容重试预算参数；不要把它当作所有读块等待的总次数或总期限。 | — |
| `cluster.gcs_block_lost_write_action` | 枚举 | `error` | error, warn | SET（管理员） | 传块时检测到已完成的写入未保留时，报错或记录警告。 | — |
| `cluster.online_block_recovery` | 布尔 | `on` | on / off | 重启 | 读到损坏或丢失写入的数据块时，尝试用 WAL 重建该块。 | — |
| `cluster.block_recovery_on_unrecoverable` | 枚举 | `error` | error, panic | 重启 | 无法用 WAL 重建损坏块时的处理方式：报错或使实例异常退出。 | — |

### 全局锁与锁等待

| 名称 | 类型 | 默认值 | 范围／单位 | 在线修改 | 作用 | PRE2 |
|---|---|---|---|---|---|---|
| `cluster.ges_handoff` | 布尔 | `off` | on / off | SET（管理员） | 检查全局锁释放时的锁移交条件。 | — |
| `cluster.ges_bast` | 布尔 | `on` | on / off | SET（管理员） | 向阻塞其他节点写请求的排他锁持有者发送让锁通知。 | — |
| `cluster.grd_max_entries` | 整数 | `1024` | 0–1048576 | 重启 | 全局锁资源表的条目数上限。 | — |
| `cluster.grd_entry_reclaim` | 布尔 | `on` | on / off | 重启 | 启用对不再使用的全局锁资源条目的安全回收。 | — |
| `cluster.grd_entry_reclaim_max_per_sweep` | 整数 | `256` | 0–65536 | 重启 | LMON 一次扫描最多回收的空闲全局锁资源条目数。 | — |
| `cluster.ges_starvation_max_skips` | 整数 | `8` | 0–1000000 | SET（管理员） | 等待者被越过此次数后，提升其在全局锁队列中的优先级。 | — |
| `cluster.ges_starvation_protection` | 布尔 | `on` | on / off | 重启 | 启用全局锁队列的公平性保护，避免请求长期得不到锁。 | — |
| `cluster.ges_request_timeout_ms` | 整数 | `60000` | -1–600000 ms | SET | 跨节点锁授予等待期限（ms）；-1 表示持续等待，要求 retransmit_max_attempts > 0。 | — |
| `cluster.cf_enqueue_timeout_ms` | 整数 | `30000` | 1000–600000 ms | 重启 | 取得共享控制信息访问锁的等待期限，单位毫秒。 | — |
| `cluster.ges_retransmit_max_attempts` | 整数 | `5` | 0–50 | 重启 | 锁请求/释放的重传尝试上限；不能与 ges_request_timeout_ms=-1 同时设为 0。 | — |
| `cluster.ges_dedup_max_entries` | 整数 | `8192` | 256–1048576 | 重启 | LMS 保存的全局锁请求重传去重条目数上限。 | — |
| `cluster.ges_convert_timeout_ms` | 整数 | `30000` | 1000–600000 ms | 重启 | 等待跨节点锁模式转换回复的期限，单位毫秒。 | — |
| `cluster.tm_convert_mode` | 枚举 | `convert` | convert, additive | 重启 | 同一会话升级表锁时，转换已有锁，或另行申请更强的锁。 | — |
| `cluster.grd_remaster_wait_ms` | 整数 | `200` | 0–60000 ms | 重启 | 全局锁资源正在因故障更换管理节点时的短暂等待，单位毫秒。 | — |
| `cluster.grd_rebuild_timeout_ms` | 整数 | `5000` | 100–600000 ms | 重启 | 故障后重新建立全局锁持有信息的等待期限，单位毫秒。 | — |
| `cluster.hw_remaster_retry_backoff_ms` | 整数 | `1000` | 100–60000 ms | 重启 | 空间分配管理节点切换受阻时，首次重试前的等待间隔，单位毫秒。 | — |
| `cluster.hw_remaster_retry_max_attempts` | 整数 | `16` | 0–1000 | 重启 | 一次空间分配管理节点切换受阻后，允许重试的次数上限。 | — |
| `cluster.ges_reply_wait_max_entries` | 整数 | `1024` | 64–65536 | 重启 | 同时等待跨节点全局锁回复的条目数上限。 | — |
| `cluster.ges_bast_retry_interval_ms` | 整数 | `10000` | 1000–60000 ms | 重启 | 持锁者未响应让锁通知时，重新发送通知的间隔，单位毫秒。 | — |
| `cluster.ges_bast_max_retries` | 整数 | `3` | 1–10 | 重启 | 让锁通知的重试次数上限；超过后拒绝新请求。 | — |
| `cluster.ges_deadlock_check_interval_ms` | 整数 | `1000` | 100–10000 ms | 重启 | 执行死锁探测的周期，单位毫秒。 | — |
| `cluster.ges_deadlock_chunk_timeout_ms` | 整数 | `2000` | 500–30000 ms | 重启 | 等待分片死锁探测消息拼接完整的期限，单位毫秒。 | — |
| `cluster.ges_deadlock_max_edges` | 整数 | `1024` | 64–65536 | 重启 | 每次死锁探测可处理的等待关系数上限。 | — |
| `cluster.ges_deadlock_max_vertices` | 整数 | `256` | 16–16384 | 重启 | 每次死锁探测可处理的等待图节点数上限。 | — |
| `cluster.ges_deadlock_max_in_flight_probes` | 整数 | `4` | 1–32 | 重启 | 每个协调节点同时执行的死锁探测数上限。 | — |
| `cluster.ges_deadlock_tick_budget_us` | 整数 | `5000` | 500–50000 | 重启 | LMON 每次主循环执行死锁工作的时间预算，单位微秒。 | — |
| `cluster.pcm_grd_max_entries` | 整数 | `-1` | -1–1048576 | 重启 | 块权限管理所用的共享资源表容量上限。 | — |
| `cluster.hang_manager_enabled` | 布尔 | `on` | on / off | 重启 | Enables the DIAG-hosted Hang Manager long-wait sampler. | — |
| `cluster.hang_sample_interval_ms` | 整数 | `10000` | 100–600000 ms | 重启 | Interval between Hang Manager long-wait sampling rounds. | — |
| `cluster.hang_threshold_ms` | 整数 | `60000` | 1000–86400000 ms | 重启 | Wait duration at/over which a backend is reported as a hang. | — |
| `cluster.hang_dump_enabled` | 布尔 | `on` | on / off | 重启 | Enables Hang Manager long-wait LOG-once and dump accounting. | — |
| `cluster.hang_max_chain_depth` | 整数 | `100` | 1–10000 | 重启 | Maximum wait-chain depth the Hang Manager walks before stopping. | — |
| `cluster.hang_max_sampled` | 整数 | `64` | 1–64 | 重启 | Maximum long-wait samples kept per round (top-N by duration). | — |
| `cluster.hang_resolution_mode` | 枚举 | `advisory` | off, advisory, enforce | 重启 | Hang Manager disposition mode (off / advisory / enforce). | — |
| `cluster.hang_resolution_confirm_rounds` | 整数 | `2` | 1–100 | 重启 | Consecutive rounds a victim identity must stay an actionable long-wait before disposition (hysteresis). | — |
| `cluster.hang_resolution_soft_timeout_ms` | 整数 | `5000` | 100–600000 ms | 重启 | Grace period between disposition tiers (cancel -> terminate -> degrade). | — |
| `cluster.hang_resolution_max_per_round` | 整数 | `1` | 1–64 | 重启 | Maximum number of victims disposed per evaluation round. | — |
| `cluster.hang_victim_w_age` | 实数 | `0.5` | 0.0–1000.0 | 重启 | Victim score weight for transaction age. | — |
| `cluster.hang_victim_w_rollback` | 实数 | `0.3` | 0.0–1000.0 | 重启 | Victim score weight for rollback cost (proxied by held lock count). | — |
| `cluster.hang_victim_w_blockers` | 实数 | `0.2` | 0.0–1000.0 | 重启 | Victim score weight for root-ness (number of waiters blocked). | — |
| `cluster.relation_extend_lock_enabled` | 布尔 | `on` | on / off | SET（管理员） | 扩展永久共享关系时，使用集群统一的块号分配。 | — |
| `cluster.tablespace_ddl_lock_enabled` | 布尔 | `on` | on / off | SET（管理员） | 对表空间 DDL 进行跨节点串行控制；本预览仍拒绝不支持的表空间操作。 | — |
| `cluster.object_reuse_flush_enabled` | 布尔 | `on` | on / off | SET（管理员） | 关系存储删除或截断前，在所有对端刷新该关系的缓冲区。 | — |
| `cluster.lock_acquire_cluster_path` | 布尔 | `on` | on / off | 重启 | 启用集群锁获取检查。 | — |
| `cluster.local_fast_path_enabled` | 布尔 | `on` | on / off | 重启 | 在当前实例可以本地授予锁时启用本地授予路径。 | — |
| `cluster.advisory_lock_enabled` | 布尔 | `on` | on / off | SET（管理员） | 使用户咨询锁在跨节点范围内生效。 | — |
| `cluster.deadlock_detection_enabled` | 布尔 | `on` | on / off | 重启 | 启用由协调节点负责的跨节点死锁检测。 | — |
| `cluster.global_dd_interval_ms` | 整数 | `2000` | 100–600000 | 重启 | 协调节点执行跨节点死锁扫描的间隔，单位毫秒。 | — |
| `cluster.deadlock_confirm_interval_ms` | 整数 | `500` | 50–60000 | 重启 | 两次死锁确认之间的间隔，单位毫秒。 | — |
| `cluster.cancel_ack_timeout_ms` | 整数 | `1000` | 50–60000 ms | SET（管理员） | 死锁处理中，等待对端取消确认再重传的时间，单位毫秒。 | — |
| `cluster.cancel_max_retransmit` | 整数 | `3` | 0–100 | SET（管理员） | 死锁取消请求升级处理前的最大重传次数。 | — |
| `cluster.victim_repeat_window_ms` | 整数 | `5000` | 0–600000 ms | SET（管理员） | 避免在指定窗口内反复选择同一事务作为死锁取消对象，单位毫秒。 | — |

### 诊断与测试入口（不用于业务配置）

| 名称 | 类型 | 默认值 | 范围／单位 | 在线修改 | 作用 | PRE2 |
|---|---|---|---|---|---|---|
| `cluster.injection_points` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | SET（管理员）；共享持久配置拒绝 | Comma-separated list of cluster injection points to auto-arm at startup. | — |
| `cluster.pcm_x_retain_flush_error_target` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | SET（管理员）；共享持久配置拒绝 | 仅启用注入测试的构建注册；指定故障注入的精确块。 | — |
| `cluster.ic_suppress_caps_reply` | 布尔 | `off` | on / off | 重启 | Test-only: simulate a pre-CAPS_REPLY binary on this node. | — |
| `cluster.ic_suppress_gcs_done_cap` | 布尔 | `off` | on / off | 重启 | Test-only: simulate a pre-GCS_DONE binary on this node. | — |
| `cluster.ic_suppress_xid_flock_cap` | 布尔 | `off` | on / off | 重启 | Test-only: suppress the XID_AUTHORITY_FLOCK_V2 HELLO capability. | — |
| `cluster.xid_wrap_barrier_force` | 布尔 | `off` | on / off | 重启 | Test-only: force the xid wrap barrier to run now. | — |
| `cluster.gcs_block_drop_target_relfilenode` | 整数 | `0` | 0–2147483647 | SET（管理员）；共享持久配置拒绝 | Test-only: restrict the drop-reply injection to one relfilenode. | — |

### 目录失效

| 名称 | 类型 | 默认值 | 范围／单位 | 在线修改 | 作用 | PRE2 |
|---|---|---|---|---|---|---|
| `cluster.sinval_broadcast_batch_size` | 整数 | `32` | 1–64 | 重启 | 每批最多发送的目录缓存失效通知数。 | — |
| `cluster.sinval_broadcast_batch_timeout_ms` | 整数 | `10` | 1–60000 | 重启 | 目录缓存失效广播进程主循环的等待间隔，单位毫秒。 | — |
| `cluster.sinval_broadcast_max_queue_size` | 整数 | `1024` | 64–65536 | 重启 | 目录缓存失效通知收发队列的容量。 | — |
| `cluster.sinval_ack_mode` | 枚举 | `peer_enqueued` | none, peer_enqueued | 重启 | 目录失效通知的确认方式；shared_catalog=on 时拒绝 none。 | 组合检查变化 |
| `cluster.sinval_ack_timeout_ms` | 整数 | `5000` | 100–60000 | 重启 | 观察目录缓存失效通知确认结果的间隔，单位毫秒。 | — |
| `cluster.sinval_ack_wait_slots` | 整数 | `256` | 64–4096 | 重启 | 等待目录缓存失效确认的条目数上限。 | — |

## 相关参考

- [等待事件](wait-events.md)
- [视图、SQL 函数与命令](commands.md)
- [现有配置手册](../../user-guide/configuration.md)：阅读其 reload 说明时，以本页的共享模式例外为准。
