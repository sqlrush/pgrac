# PRE2 参数参考

适用代码版本：`c45bdc5d4392751a33659acf8dcaa2b76254e73a`。本页列出该版本相对原生 PostgreSQL 增加的全部 `cluster.*` 注册项。默认值是**代码默认值**，不是安装向导生成的共享集群配置；运行值以各节点的 `pg_settings` 为准。

交付镜像 `2d857abffc` 的参数注册、等待名称及 SQL 函数／视图定义与本页核对版本一致；动态诊断键与可用功能仍按实际二进制识别。

普通集群构建注册 254 项；启用注入测试的构建另有 1 项，本页一起列出并标注。参数存在或取值被接受，不表示相关功能已经纳入本次技术预览。共享部署只使用[安装手册](../install.md)提供的配置，保持固定成员与全新建库；在线加入、移除、单节点计划退出、ADG 和媒体恢复入口不作为本预览的使用流程。

## PRE2 新增与变化

本页的“新增／变化”以 `v0.132.0` 为对照，而不是以原生 PG 为对照：所有 `cluster.*` 均非原生 PG 参数。

| 项目 | PRE2 变化 |
|---|---|
| `cluster.shared_config` | 新增，共享配置开关，由创建入口设置。 |
| `cluster.storage_quorum_nodes` | 新增，数据库节点与存储集群节点的映射。 |
| `cluster.storage_quorum_cluster` | 新增，存储集群名称。 |
| `cluster.cold_recovery_plan_memory` | 新增，冷恢复计划内存上限。 |
| `cluster.shared_catalog`、`cluster.sinval_ack_mode` | 新增组合检查：共享目录必须启用失效确认，不能选择 `none`。 |
| 共享配置的在线修改 | 新增共享 `ALTER SYSTEM` 行为。许多原本显示为 `sighup` 的 COMMON 参数，在共享模式中需要重启才能生效。不能仅据 `pg_settings.context` 判断。 |

表中“—”表示注册项的默认值、范围、上下文和检查挂钩与上述对照相同，不承诺所有使用该参数的功能行为都未变化。

## 查看与修改

```sql
SELECT name, setting, unit, boot_val, reset_val, min_val, max_val,
       enumvals, context, source, pending_restart
FROM pg_settings
WHERE name LIKE 'cluster.%'
ORDER BY name;
```

- **创建时**：只在受支持的全新共享建库输入中指定；共享 `ALTER SYSTEM` 拒绝修改，重启也不能把它变成可修改项。
- **重启**：可以发布允许的共享配置，运行中的实例继续使用原值；按部署流程正常全停并重启后核对各节点。即使 `context='sighup'`，共享模式下也不能靠 `pg_reload_conf()`使这些 COMMON 参数生效。
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

## 必须保留的共享配置与限制

- `cluster.undo_cleaner_enabled=on`；关闭会停止本版本所需的清偿工作，可能导致正常停机失败。`autovacuum` 也应保持开启。
- `cluster.lms_workers=2`。代码范围为 1–8；大于 2 的配置已有启动失败记录，不能因为取值合法就用于本次交付。
- `cluster.smart_fusion=off`，本预览不启用提前传块；设置 `on` 会报错。
- 使用创建入口提供的 `shared_config`、`shared_catalog`、`undo_gcs_coherence` 和 `crossnode_runtime_visibility` 组合；不通过单独切换其中一项迁移已有数据目录。共享目录不能使用 `wal_level=minimal`。
- `cluster.storage_quorum_nodes` 使用 `数据库节点号:Corosync节点号` 的逗号分隔映射，例如 `0:1,1:2,2:3,3:4`；实际值必须与固定成员配置一致。不要将示例直接用于不同部署。
- `cluster.voting_disks`、恢复目标、裸设备路径等未列入共享字符串配置的旧入口仍在参数注册表中，但不能作为本预览共享 `ALTER SYSTEM` 的有效输入。投票盘与存储身份按安装流程提供。
- 下表未列出的组合限制仍会在配置或启动时检查；收到拒绝时保留错误详情，不通过关掉一致性或隔离开关绕过。

## 参数表

“在线修改”列适用于本预览的共享配置模式；非共享运行的原生上下文见各节点 `pg_settings.context`。作用栏保留部分英文参数帮助原文，便于与 `pg_settings.short_desc` 对照。

### 创建、存储、成员及启动

| 名称 | 类型 | 默认值 | 范围／单位 | 在线修改 | 作用 | PRE2 |
|---|---|---|---|---|---|---|
| `cluster.node_id` | 整数 | `-1` | -1–127 | 创建时 | 本实例编号；-1 表示未指定集群节点。交付配置必须指定实际节点编号。 | — |
| `cluster.config_file` | 字符串 | `"pgrac.conf"` | 字符串；另受格式／共享配置限制 | 创建时 | 节点拓扑配置文件路径。 | — |
| `cluster.wal_threads_dir` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 创建时 | 共享 WAL 根目录；非空值必须是绝对路径。共享部署使用创建时的同一路径。 | — |
| `cluster.recovery_stale_active_ms` | 整数 | `10000` | 1000–3600000 ms | 重启 | Staleness window before an ACTIVE WAL-state slot is reported as a crash candidate. | — |
| `cluster.recovery_workers_max` | 整数 | `4` | 0–16 | 重启 | Maximum recovery stream-validation workers. | — |
| `cluster.merged_recovery` | 布尔 | `off` | on / off | 重启 | Enable cold-crash k-way SCN merged recovery. | — |
| `cluster.cold_recovery_plan_memory` | 整数 | `4194304` | 1024–2147483647 kB | 重启 | 冷恢复计划的内存上限（kB），不是启动时预分配的内存量。 | 新增 |
| `cluster.xnode_profile` | 布尔 | `off` | on / off | SET（管理员） | 采集跨节点性能计数及计时；在 pg_cluster_state 的 xnode_profile 类读取。 | — |
| `cluster.update_trace` | 布尔 | `off` | on / off | SET（管理员） | 采集 UPDATE 各执行阶段的诊断信息；默认关闭。 | — |
| `cluster.space_affinity` | 枚举 | `off` | off, static, dynamic | 重启 | Instance space-affinity mode for cluster relation extends. | — |
| `cluster.space_lease_blocks` | 整数 | `64` | 1–8192 | 重启 | Blocks handed to a node per HW space lease. | — |
| `cluster.clean_leave_enabled` | 布尔 | `off` | on / off | 重启 | Enable cooperative clean-leave reconfiguration. | — |
| `cluster.clean_leave_drain_timeout_ms` | 整数 | `30000` | 1000–600000 ms | 重启 | Fail-closed deadline for a clean leave's cooperative drain. | — |
| `cluster.recovery_merge_wait_timeout` | 整数 | `10000` | 0–600000 ms | 重启 | Time to wait for stream-validation workers before merged recovery. | — |
| `cluster.shared_storage_backend` | 枚举 | `stub` | stub, local, block_device, cluster_fs, rbd, multi_attach | 创建时 | Cluster shared-storage backend selection. | — |
| `cluster.write_fence_enforcement` | 枚举 | `on` | off, on, dev | 重启 | Cooperative write-fence enforcement mode. | — |
| `cluster.write_fence_lease_ms` | 整数 | `6000` | 1000–600000 ms | 重启 | Cooperative write-fence token lease duration (milliseconds). | — |
| `cluster.external_fence_socket_path` | 字符串 | `"/var/run/pgrac/pgrac-fenced.sock"` | 字符串；另受格式／共享配置限制 | 重启（本实例） | 本实例连接隔离服务的 Unix socket 绝对路径。 | — |
| `cluster.external_fence_acquire_timeout_ms` | 整数 | `120000` | 1–600000 ms | 重启 | Overall external write-exclusion acquisition deadline (milliseconds). | — |
| `cluster.shared_data_dir` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 创建时 | 共享数据根目录；非空值必须是绝对路径，cluster_fs 要求配置此目录。 | — |
| `cluster.controlfile_shared_authority` | 布尔 | `off` | on / off | 创建时 | Use a single shared pg_control authority under cluster.shared_data_dir. | — |
| `cluster.storage_quorum_nodes` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 创建时 | 数据库节点号到 Corosync 节点号的映射，格式见下文。 | 新增 |
| `cluster.storage_quorum_cluster` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 创建时 | 预期 Corosync 集群名称，必须与部署中的名称完全一致。 | 新增 |
| `cluster.shared_config` | 布尔 | `off` | on / off | 创建时 | 启用共享配置；由全新共享建库入口设置，不能把已有普通 PG 数据目录改为共享库。 | 新增 |
| `cluster.shared_storage_uuid` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 创建时 | Optional external identity for the cluster_fs shared root. | — |
| `cluster.block_device_path` | 字符串 | `""` | 字符串；另受格式／共享配置限制 | 重启；共享持久配置拒绝 | Raw block-device path for the block_device shared-storage backend. | — |
| `cluster.block_device_use_odirect` | 布尔 | `on` | on / off | 重启 | Require direct I/O for the raw block-device backend. | — |
| `cluster.storage_fence_driver` | 枚举 | `auto` | disabled, auto, scsi3_pr | 重启 | Shared-storage fencing driver selection. | — |
| `cluster.smgr_user_relations` | 布尔 | `off` | on / off | 重启 | 让永久关系使用集群共享存储。 | — |
| `cluster.shared_catalog` | 布尔 | `off` | on / off | 创建时 | 启用共享系统目录；必须使用受支持的全新共享建库配置，并保留失效确认。 | 组合检查变化 |
| `cluster.oid_lease_size` | 整数 | `8192` | 1024–1048576 | 重启 | Number of OIDs a node leases at a time from the shared OID authority. | — |
| `cluster.shmem_max_regions` | 整数 | `96` | 40（注入构建 41）–256 | 重启 | Capacity of the pgrac cluster shmem region registry. | — |
| `cluster.phase1_timeout` | 整数 | `60` | 1–3600 s | 重启 | Phase 1 (cluster basics) transition timeout in seconds. | — |
| `cluster.phase2_timeout` | 整数 | `30` | 1–3600 s | 重启 | Phase 2 (lock services) transition timeout in seconds. | — |
| `cluster.phase3_timeout` | 整数 | `600` | 60–3600 s | 重启 | Phase 3 (recovery) transition timeout in seconds. | — |
| `cluster.phase4_timeout` | 整数 | `30` | 1–3600 s | 重启 | Phase 4 (normal startup) transition timeout in seconds. | — |
| `cluster.touched_peers_trace` | 布尔 | `off` | on / off | SET（管理员） | Log the touched-peers set of each transaction aborted by a fail-stop reconfiguration. | — |
| `cluster.cssd_main_loop_interval_ms` | 整数 | `1000` | 100–60000 ms | 重启 | CSSD aux process main-loop tick interval in milliseconds. | — |
| `cluster.cssd_heartbeat_interval_ms` | 整数 | `1000` | 100–10000 ms | 重启 | CSSD heartbeat broadcast period in milliseconds. | — |
| `cluster.cssd_dead_deadband_factor` | 整数 | `3` | 2–10 | 重启 | CSSD dead-detection deadband as a multiple of heartbeat interval. | — |
| `cluster.voting_disks` | 字符串 | `未设置` | 字符串；另受格式／共享配置限制 | 重启；共享持久配置拒绝 | Comma-separated list of voting disk file paths. | — |
| `cluster.quorum_poll_interval_ms` | 整数 | `2000` | 500–30000 ms | 重启 | Quorum voting disk poll period in milliseconds. | — |
| `cluster.voting_disk_io_timeout_ms` | 整数 | `5000` | 500–60000 ms | 重启 | Voting disk single I/O timeout in milliseconds. | — |
| `cluster.voting_disk_size_bytes` | 整数 | `394240` | 4096–1048576 B | 重启 | Voting disk file size in bytes. | — |
| `cluster.online_join` | 布尔 | `off` | on / off | 重启 | Allow a declared node to join/rejoin live membership online (without a full cluster restart). | — |
| `cluster.join_remaster_enabled` | 布尔 | `off` | on / off | 重启 | On node rejoin, move the joiner's home-shard GES mastership back from the survivor (optional rebalance). | — |
| `cluster.join_convergence_timeout_ms` | 整数 | `30000` | 5000–120000 ms | 重启 | Deadline for an online join to converge + commit. | — |
| `cluster.online_node_removal` | 布尔 | `off` | on / off | 重启 | Enable permanent removal (decommission) of a declared node. | — |
| `cluster.node_removal_cleanup_timeout_ms` | 整数 | `30000` | 5000–120000 ms | 重启 | Deadline for the post-shrink cluster-wide removal cleanup. | — |
| `cluster.self_fence_enabled` | 布尔 | `on` | on / off | 重启 | Enable postmaster self-shutdown on persistent quorum loss. | — |
| `cluster.self_fence_grace_ms` | 整数 | `30000` | 1000–300000 ms | 重启 | Delay before postmaster self-shutdown on persistent quorum loss (ms). | — |
| `cluster.freeze_writes_enabled` | 布尔 | `on` | on / off | 重启 | Enable PROCSIG_CLUSTER_FREEZE_WRITES in-flight transaction abort. | — |
| `cluster.fence_audit_log` | 枚举 | `log` | off, log, debug | 重启 | Verbosity of fence-related log events. | — |
| `cluster.enabled` | 布尔 | `on` | on / off | 创建时 | 启用集群运行模式。 | — |
| `cluster.allow_single_node` | 布尔 | `on` | on / off | 重启 | Allow pgrac to start in single-node mode (no pgrac.conf or invalid cluster.node_id). | — |
| `cluster.online_thread_recovery` | 布尔 | `off` | on / off | 重启 | Let a survivor online-replay a dead WAL thread's data to shared storage in the reconfig freeze window instead of waiting for the dead node's cold restart. | — |
| `cluster.thread_recovery_on_unrecoverable` | 枚举 | `keep_frozen` | keep_frozen, panic | 重启 | Action when a dead thread cannot be online-recovered. | — |

### 互联和后台进程

| 名称 | 类型 | 默认值 | 范围／单位 | 在线修改 | 作用 | PRE2 |
|---|---|---|---|---|---|---|
| `cluster.interconnect_tier` | 枚举 | `stub` | stub, mock, tier1, tier2, tier3 | 重启 | Cluster interconnect tier vtable selection. | — |
| `cluster.interconnect_rdma_fallback` | 枚举 | `auto` | auto, off | 重启 | Policy for RDMA-to-TCP interconnect fallback. | — |
| `cluster.interconnect_rdma_provider` | 枚举 | `auto` | auto, verbs, mlx5 | 重启 | RDMA provider selection for tier2/tier3 interconnect. | — |
| `cluster.interconnect_rdma_completion` | 枚举 | `event` | event, busypoll | 重启 | RDMA completion model for the interconnect. | — |
| `cluster.interconnect_rdma_busypoll_us` | 整数 | `50` | 0–10000 | 重启 | RDMA busy-poll spin budget in microseconds. | — |
| `cluster.interconnect_rdma_crc_offload` | 布尔 | `off` | on / off | 重启 | Reserved RDMA control-plane CRC offload switch. | — |
| `cluster.interconnect_rdma_inline_max` | 整数 | `256` | 0–4096 B | 重启 | RDMA inline-send threshold in bytes. | — |
| `cluster.interconnect_rdma_max_send_wr` | 整数 | `256` | 16–4096 | 重启 | RDMA send work request queue depth per peer. | — |
| `cluster.lmd_probe_collect_timeout_ms` | 整数 | `3000` | 100–30000 ms | 重启 | Coordinator DEADLOCK_REPORT collect deadline (ms). | — |
| `cluster.lmd_cleanup_sweep_interval_ms` | 整数 | `5000` | 100–60000 ms | 重启 | LMD periodic dead-backend cleanup sweep interval (ms). | — |
| `cluster.lms_native_lock_probe_max_inflight` | 整数 | `8` | 1–64 | 重启 | Per-shard LMS native-lock probe collector slot capacity. | — |
| `cluster.lms_native_lock_probe_retry_interval_ms` | 整数 | `500` | 50–60000 ms | 重启 | LMS native-lock probe retry-poll cadence when peers return HOLDER_CONFLICT / WAITER_CONFLICT / timeout. | — |
| `cluster.lms_native_lock_probe_retry_budget` | 整数 | `60` | 1–3600 | 重启 | Cumulative retry budget per requester before native-lock probe fails closed with 53R83. | — |
| `cluster.lmon_main_loop_interval` | 整数 | `1000` | 100–60000 ms | 重启 | LMON main-loop tick interval in milliseconds. | — |
| `cluster.lmon_slow_iteration_warn_ms` | 整数 | `1000` | 0–60000 ms | 重启 | LMON main-loop slow-iteration warning threshold in milliseconds. | — |
| `cluster.lck_main_loop_interval` | 整数 | `1000` | 100–60000 ms | 重启 | LCK main-loop tick interval in milliseconds. | — |
| `cluster.diag_main_loop_interval` | 整数 | `1000` | 100–60000 ms | 重启 | DIAG main-loop tick interval in milliseconds. | — |
| `cluster.interconnect_heartbeat_interval_ms` | 整数 | `1000` | 100–60000 ms | 重启 | Tier1 IC heartbeat tick interval in milliseconds. | — |
| `cluster.interconnect_connect_timeout_ms` | 整数 | `5000` | 1000–60000 ms | 重启 | Tier1 IC active-connect SO_ERROR poll timeout in ms. | — |
| `cluster.interconnect_recv_timeout_ms` | 整数 | `30000` | 1000–600000 ms | 重启 | Tier1 IC per-peer recv read deadline in milliseconds. | — |
| `cluster.interconnect_payload_max_bytes` | 整数 | `67108864` | 16777216–268435456 B | 重启 | Maximum cluster_ic_send_envelope_chunked payload bytes. | — |
| `cluster.interconnect_chunk_reassembly_timeout_ms` | 整数 | `10000` | 1000–60000 ms | 重启 | Chunked reassembly partial-frame timeout in milliseconds. | — |
| `cluster.interconnect_tcp_keepidle_sec` | 整数 | `60` | 30–600 s | 重启 | Tier1 TCP_KEEPIDLE socket option in seconds. | — |
| `cluster.interconnect_tcp_keepintvl_sec` | 整数 | `10` | 10–60 s | 重启 | Tier1 TCP_KEEPINTVL socket option in seconds. | — |
| `cluster.interconnect_tcp_keepcnt` | 整数 | `6` | 3–20 | 重启 | Tier1 TCP_KEEPCNT socket option (probe count). | — |
| `cluster.cluster_stats_main_loop_interval` | 整数 | `1000` | 100–60000 ms | 重启 | Cluster Stats main-loop tick interval in milliseconds. | — |
| `cluster.lmd_enabled` | 布尔 | `on` | on / off | 重启 | Enable the LMD (Lock Manager Daemon — deadlock detection actor) cluster background process. | — |
| `cluster.lms_enabled` | 布尔 | `on` | on / off | 重启 | Enable the LMS (Lock Master Server) cluster grant decision daemon. | — |
| `cluster.lms_workers` | 整数 | `2` | 1–8 | 重启 | LMS 数据通道进程数（包括 worker 0）；本预览保持 2，较大值尚未验证可用。 | — |
| `cluster.lms_nice` | 整数 | `0` | -20–0 | 重启 | Nice value applied to the LMS DATA-plane workers (0 = leave alone). | — |
| `cluster.lmd_max_wait_edges` | 整数 | `1024` | 64–65536 | 重启 | Maximum LMD wait-for graph edges. | — |
| `cluster.lmd_scan_interval_ms` | 整数 | `1000` | 50–60000 | 重启 | LMD Tarjan scan loop period (ms). | — |
| `cluster.ic_duty_lazy` | 布尔 | `on` | on / off | 重启 | Run lazy-able LMON duty-chain drains on demand instead of every iteration. | — |

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
| `cluster.page_scn_shortcut` | 布尔 | `off` | on / off | SET（管理员） | 启用跨节点可见性终态结果的会话内复用；不是 VACUUM FREEZE。 | — |
| `cluster.crossnode_runtime_visibility` | 布尔 | `off` | on / off | SET（管理员） | 启用运行中跨实例事务可见性判定；共享部署保持 on。 | — |
| `cluster.xid_striping` | 布尔 | `off` | on / off | 重启 | Stripe xid allocation into per-node congruence classes. | — |
| `cluster.multi_xmax_remote_resolve` | 布尔 | `on` | on / off | 重启 | Resolve foreign multixact xmax through the cluster member overlay. | — |
| `cluster.xid_herding_slack` | 整数 | `4194304` | 65536–268435456 | 重启 | Allowed xid gap between stripe slots before herding jumps. | — |
| `cluster.undo_gcs_coherence` | 布尔 | `off` | on / off | 创建时 | 启用共享 undo 块的一致性访问；要求已配置 shared_data_dir。共享部署保持 on。 | — |
| `cluster.undo_retention_horizon_enabled` | 布尔 | `on` | on / off | 重启 | Retain committed undo / TT slots until no live reader needs them. | — |
| `cluster.undo_buffers` | 整数 | `2048` | 0–1048576 | 重启 | undo 数据块及段头块的缓冲帧数量。 | — |
| `cluster.undo_buffer_writeback` | 布尔 | `on` | on / off | 重启 | 本地 undo 缓冲写回开关；多节点配置下不生效，使用写穿路径。 | — |
| `cluster.undo_writeback_boundary_check` | 枚举 | `on` | off, on, strict | 重启 | undo 检查点写回的附加检查级别；不能用于关闭必要的持久性检查。 | — |
| `cluster.undo_cleaner_interval_ms` | 整数 | `30000` | 0–3600000 | 重启 | Undo Cleaner pass interval in milliseconds. | — |
| `cluster.undo_cleaner_enabled` | 布尔 | `on` | on / off | 重启 | 启用 undo/事务槽清理及待清偿工作的处理。共享模式保持 on；关闭可能使正常全停失败。 | — |
| `cluster.undo_cleaner_batch_segments` | 整数 | `8` | 1–256 | 重启 | Max own-instance undo segments scanned per cleaner pass. | — |
| `cluster.undo_record_segment_commit_on_rollover` | 布尔 | `on` | on / off | 重启 | Advance a drained record undo segment ACTIVE -> COMMITTED on rollover. | — |
| `cluster.undo_segments_per_instance` | 整数 | `16` | 1–1024 | 重启 | Reserved undo segment count per cluster instance. | — |
| `cluster.undo_tablespace_path` | 字符串 | `"pg_undo"` | 字符串；另受格式／共享配置限制 | 创建时 | Relative path under PGDATA for the per-instance undo tablespace. | — |
| `cluster.undo_segment_size_mb` | 整数 | `32` | 8–1024 | 重启 | Per-segment file size in MB. | — |
| `cluster.undo_record_inline_max_bytes` | 整数 | `1024` | 16–8192 | 重启 | Maximum inline payload size for a single undo record. | — |
| `cluster.undo_extent_blocks` | 整数 | `4` | 1–256 | 重启 | Undo block extent size claimed per transaction. | — |
| `cluster.undo_segments_max_per_instance` | 整数 | `256` | 16–256 | 重启 | Hard cap of per-instance undo segment pool size. | — |
| `cluster.undo_segment_create_timeout_ms` | 整数 | `5000` | 100–60000 | 重启 | Segment file create + initial fsync elapsed-time guard. | — |
| `cluster.tt_durable_lookup` | 布尔 | `on` | on / off | SET | Resolve commit_scn from the durable undo-header TT slot on overlay miss. | — |
| `cluster.tt_recovery_resolve_active` | 布尔 | `on` | on / off | 重启 | Resolve crash-left ACTIVE durable TT slots to ABORTED at startup. | — |
| `cluster.boc_sweep_interval_ms` | 整数 | `100` | 1–1000 ms | 重启 | walwriter BOC sweep staleness target in milliseconds. | — |
| `cluster.boc_event_publish` | 布尔 | `on` | on / off | 重启 | Publish the durable SCN frontier on commit events. | — |
| `cluster.scn_max_propagation_lag_ms` | 整数 | `5000` | 100–60000 ms | 重启 | SCN cross-instance propagation lag bound in milliseconds. | — |
| `cluster.tt_status_overlay_max_entries` | 整数 | `32768` | 1024–1048576 | 重启 | Capacity of cluster Undo TT status overlay HTAB. | — |
| `cluster.tt_status_overlay_ttl_ms` | 整数 | `30000` | 1000–600000 | 重启 | TTL in milliseconds for cluster Undo TT status overlay entries. | — |
| `cluster.subtrans_max_chain_depth` | 整数 | `32` | 4–1024 | 重启 | Bounded depth for cluster SUBTRANS reader lazy parent_key follow. | — |
| `cluster.multixact_member_overlay_max_members` | 整数 | `32` | 4–256 | 重启 | Per-message hard cap on MultiXact member_count. | — |
| `cluster.multixact_member_overlay_max_entries` | 整数 | `16384` | 1024–1048576 | 重启 | Capacity of cluster MultiXact member overlay HTAB. | — |
| `cluster.multixact_hint_outbound_slots` | 整数 | `1024` | 128–8192 | 重启 | MultiXact outbound queue slot count. | — |
| `cluster.tt_status_hint_outbound_capacity` | 整数 | `256` | 64–4096 | 重启 | Capacity of cluster TT status hint outbound ring. | — |
| `cluster.tt_status_hint_emit_mode` | 枚举 | `all_status` | disabled, all_status | 重启 | Emit mode for cross-node TT status hint propagation. | — |
| `cluster.sequence_default_cache` | 整数 | `100` | 1–1000000000 | SET（管理员） | Default CACHE size injected into new sequences in cluster mode. | — |
| `cluster.sequence_cache_floor_optin` | 整数 | `0` | 0–1000000000 | 重启 | Opt-in runtime floor for an existing sequence's CACHE size. | — |
| `cluster.sequence_refill_timeout_ms` | 整数 | `30000` | 1000–600000 ms | 重启 | Maximum wait for an SQ sequence segment refill before failing closed. | — |

### 块访问与一致读

| 名称 | 类型 | 默认值 | 范围／单位 | 在线修改 | 作用 | PRE2 |
|---|---|---|---|---|---|---|
| `cluster.read_scache` | 布尔 | `off` | on / off | 重启 | Enable quiescent-block S-caching via X->S downgrade. | — |
| `cluster.crossnode_cr_data_plane` | 布尔 | `off` | on / off | SET（管理员） | Enable the cross-instance CR-server data plane. | — |
| `cluster.block_self_contained` | 布尔 | `off` | on / off | SET（管理员） | 已弃用的兼容参数，修改无效果。 | — |
| `cluster.past_image` | 布尔 | `off` | on / off | SET（管理员） | Keep a Past Image copy when a block is transferred or invalidated. | — |
| `cluster.cr_chain_walk_max_steps` | 整数 | `4096` | 64–65536 | 重启 | Hard cap on undo chain walk steps per CR block construction. | — |
| `cluster.cr_mvcc_gate` | 布尔 | `on` | on / off | SET | Enable the own-instance CR 3-tier MVCC short-circuit gate. | — |
| `cluster.cr_gate_no_peer_fastpath` | 布尔 | `on` | on / off | SET | 在无对端且快照仅用于本会话时使用本地 MVCC 判断。 | — |
| `cluster.cr_tuple_level_fastpath` | 布尔 | `off` | on / off | SET | Use the tuple-level / verdict-only CR read fast path for a single-candidate-chain block (compute-only; default off). | — |
| `cluster.cf_terminal_authority` | 布尔 | `off` | on / off | 重启 | 启用持久事务状态与 undo 的跨节点终态判定。 | — |
| `cluster.cf_delayed_cleanout` | 枚举 | `reader` | off, reader, eager | 重启 | 事务槽提交状态清理策略：关闭、读时清理、主动清理。 | — |
| `cluster.smart_fusion` | 布尔 | `off` | on / off | 重启 | 提前传块功能开关。本预览只接受 off，设置 on 会被拒绝。 | — |
| `cluster.smart_fusion_tier_min` | 枚举 | `tier3` | tier3 | 重启 | Minimum interconnect tier that may use Smart Fusion early transfer. | — |
| `cluster.smart_fusion_commit_brake_timeout_ms` | 整数 | `5000` | 1–600000 ms | 重启 | Timeout for the Smart Fusion pre-commit dependency brake. | — |
| `cluster.smart_fusion_origin_durable_gossip_ms` | 整数 | `50` | 1–60000 ms | 重启 | Interval for publishing local durable WAL progress to Smart Fusion peers. | — |
| `cluster.cr_cache_max_blocks` | 整数 | `64` | 0–4096 | SET | Backend-local CR block cache capacity in 8 KB blocks (0 disables). | — |
| `cluster.shared_cr_pool_enabled` | 布尔 | `off` | on / off | 重启 | Enable the dedicated shared (cross-backend) CR buffer pool (L2). | — |
| `cluster.shared_cr_pool_size_blocks` | 整数 | `0` | 0–262144 | 重启 | Shared CR buffer pool capacity in 8 KB blocks (0 = disabled / zero memory). | — |
| `cluster.cr_pool_rel_generation_slots` | 整数 | `0` | 0–262144 | 重启 | Per-relation CR lifecycle generation table size (0 = disabled; coarse). | — |
| `cluster.cr_pool_admission_policy` | 枚举 | `admit_all` | admit_all, no_admit, scan_resistant | 重启 | 共享一致读缓存的写入策略：全部接纳、不接纳、抑制扫描污染。 | — |
| `cluster.cr_pool_admit_relation_backend_cap` | 整数 | `0` | 0–1048576 | 重启 | Per-backend cap on CR pool admits for a single relation (0 disables). | — |
| `cluster.cr_pool_admit_pressure_ratio` | 整数 | `0` | 0–100000 | 重启 | CR pool evict:hit pressure threshold (percent) for admission throttling (0 disables). | — |
| `cluster.resolver_cache_enabled` | 布尔 | `off` | on / off | 重启 | 启用经重新验证后可使用的共享事务状态缓存。 | — |
| `cluster.resolver_cache_measure` | 布尔 | `off` | on / off | 重启 | 仅测量共享事务状态缓存；不使用缓存结果替代原判定。 | — |
| `cluster.resolver_cache_entries` | 整数 | `0` | 0–1048576 | 重启 | Shared resolver cache hint-slot count (0 = disabled / zero memory). | — |
| `cluster.cross_instance_cr_coordinator` | 枚举 | `boundary` | off, boundary, forward | 重启 | 跨实例一致读协调的观测模式。 | — |
| `cluster.cross_instance_cr_probe` | 布尔 | `off` | on / off | SET | 记录跨实例一致读的诊断命中计数。 | — |
| `cluster.gcs_reply_timeout_ms` | 整数 | `5000` | 100–60000 | SET（管理员） | GCS block-ship request reply timeout (ms). | — |
| `cluster.gcs_block_recovery_wait_ms` | 整数 | `200` | 0–60000 | 重启 | Bounded wait (ms) on a recovering GCS block resource before 53R9L. | — |
| `cluster.gcs_block_retransmit_max_retries` | 整数 | `4` | 0–8 | SET（管理员） | Maximum retry attempts after initial GCS block-ship reply timeout. | — |
| `cluster.gcs_block_local_cache` | 布尔 | `on` | on / off | SET（管理员） | Hold PCM block locks until revoked (node-level cache). | — |
| `cluster.tx_enqueue_wait` | 布尔 | `on` | on / off | SET（管理员） | Block on a remote row lock until the holder completes. | — |
| `cluster.crossnode_write_write` | 布尔 | `off` | on / off | SET（管理员） | Chain a local write past a terminal remote writer. | — |
| `cluster.gcs_block_retransmit_initial_backoff_ms` | 整数 | `10` | 1–5000 | SET（管理员） | Initial backoff before retry 1 (subsequent retries double). | — |
| `cluster.gcs_block_dedup_max_entries` | 整数 | `16384` | 256–65536 | 重启 | Master-side GCS block dedup HTAB capacity (entries). | — |
| `cluster.gcs_block_invalidate_ack_timeout_ms` | 整数 | `1500` | 100–60000 | SET（管理员） | CF 3-way master deadline for a single INVALIDATE_ACK. | — |
| `cluster.gcs_block_starvation_backoff_ms` | 整数 | `100` | 1–60000 | SET（管理员） | S barrier reader backoff base for DENIED_PENDING_X retry. | — |
| `cluster.gcs_block_starvation_max_retries` | 整数 | `8` | 0–64 | SET（管理员） | 保留的兼容重试预算参数；不要把它当作所有读块等待的总次数或总期限。 | — |
| `cluster.gcs_block_lost_write_action` | 枚举 | `error` | error, warn | SET（管理员） | Action when GCS block ship triggers lost-write detection. | — |
| `cluster.online_block_recovery` | 布尔 | `on` | on / off | 重启 | Rebuild a corrupt/lost-write block from WAL on read instead of erroring. | — |
| `cluster.block_recovery_on_unrecoverable` | 枚举 | `error` | error, panic | 重启 | Action when a corrupt block cannot be rebuilt from WAL. | — |

### 全局锁与锁等待

| 名称 | 类型 | 默认值 | 范围／单位 | 在线修改 | 作用 | PRE2 |
|---|---|---|---|---|---|---|
| `cluster.ges_handoff` | 布尔 | `off` | on / off | SET（管理员） | Verify the GES release-side deterministic handoff invariants. | — |
| `cluster.ges_bast` | 布尔 | `on` | on / off | SET（管理员） | Send a BAST nudge to a live X holder blocking a peer writer. | — |
| `cluster.grd_max_entries` | 整数 | `1024` | 0–1048576 | 重启 | Maximum number of cluster_grd entry table slots. | — |
| `cluster.grd_entry_reclaim` | 布尔 | `on` | on / off | 重启 | Enable safe cold reclaim for GRD resource entries. | — |
| `cluster.grd_entry_reclaim_max_per_sweep` | 整数 | `256` | 0–65536 | 重启 | Maximum GRD cold entries reclaimed by one LMON sweep. | — |
| `cluster.ges_starvation_max_skips` | 整数 | `8` | 0–1000000 | SET（管理员） | Skip count after which a starved GES waiter is boosted to head-of-line. | — |
| `cluster.ges_starvation_protection` | 布尔 | `on` | on / off | 重启 | Enables GES enqueue lock-starvation fairness protection. | — |
| `cluster.ges_request_timeout_ms` | 整数 | `60000` | -1–600000 ms | SET | 跨节点锁授予等待期限（ms）；-1 表示持续等待，要求 retransmit_max_attempts > 0。 | — |
| `cluster.cf_enqueue_timeout_ms` | 整数 | `30000` | 1000–600000 ms | 重启 | Timeout for acquiring the shared control-file (CF) enqueue (ms). | — |
| `cluster.ges_retransmit_max_attempts` | 整数 | `5` | 0–50 | 重启 | 锁请求/释放的重传尝试上限；不能与 ges_request_timeout_ms=-1 同时设为 0。 | — |
| `cluster.ges_dedup_max_entries` | 整数 | `8192` | 256–1048576 | 重启 | LMS-owned GES retransmit dedup HTAB capacity (entries). | — |
| `cluster.ges_convert_timeout_ms` | 整数 | `30000` | 1000–600000 ms | 重启 | Finite wait for a cross-node lock-conversion (convert) grant reply. | — |
| `cluster.tm_convert_mode` | 枚举 | `convert` | convert, additive | 重启 | How a same-backend TM table-lock upgrade is routed across nodes. | — |
| `cluster.grd_remaster_wait_ms` | 整数 | `200` | 0–60000 ms | 重启 | Short wait on a GRD shard frozen by failure-driven remaster (ms). | — |
| `cluster.grd_rebuild_timeout_ms` | 整数 | `5000` | 100–600000 ms | 重启 | Holder-rebuild barrier deadline after a failure-driven remaster (ms). | — |
| `cluster.hw_remaster_retry_backoff_ms` | 整数 | `1000` | 100–60000 ms | 重启 | Initial backoff before retrying a BLOCKED HW remaster worker (ms). | — |
| `cluster.hw_remaster_retry_max_attempts` | 整数 | `16` | 0–1000 | 重启 | Maximum same-episode retries for a BLOCKED HW remaster worker. | — |
| `cluster.ges_reply_wait_max_entries` | 整数 | `1024` | 64–65536 | 重启 | Cap on the cross-node GES reply wait HTAB (5-tuple key). | — |
| `cluster.ges_bast_retry_interval_ms` | 整数 | `10000` | 1000–60000 ms | 重启 | BAST retry interval (ms) when holder is non-responsive. | — |
| `cluster.ges_bast_max_retries` | 整数 | `3` | 1–10 | 重启 | Maximum BAST retry attempts before REJECT to new requester. | — |
| `cluster.ges_deadlock_check_interval_ms` | 整数 | `1000` | 100–10000 ms | 重启 | Deadlock probe periodic interval (ms). | — |
| `cluster.ges_deadlock_chunk_timeout_ms` | 整数 | `2000` | 500–30000 ms | 重启 | Deadlock probe chunked reassembly timeout (ms). | — |
| `cluster.ges_deadlock_max_edges` | 整数 | `1024` | 64–65536 | 重启 | Deadlock graph max edges per probe. | — |
| `cluster.ges_deadlock_max_vertices` | 整数 | `256` | 16–16384 | 重启 | Deadlock graph max vertices per probe. | — |
| `cluster.ges_deadlock_max_in_flight_probes` | 整数 | `4` | 1–32 | 重启 | Max concurrent in-flight deadlock probes per coordinator. | — |
| `cluster.ges_deadlock_tick_budget_us` | 整数 | `5000` | 500–50000 | 重启 | Max time(us)budget for deadlock work per LMON tick. | — |
| `cluster.pcm_grd_max_entries` | 整数 | `-1` | -1–1048576 | 重启 | Maximum entries in the PCM GRD master shmem region. | — |
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
| `cluster.relation_extend_lock_enabled` | 布尔 | `on` | on / off | SET（管理员） | Extend permanent shared relations through the cluster block-number authority. | — |
| `cluster.tablespace_ddl_lock_enabled` | 布尔 | `on` | on / off | SET（管理员） | Serialise tablespace DDL (CREATE/DROP/ALTER/RENAME) across the cluster. | — |
| `cluster.object_reuse_flush_enabled` | 布尔 | `on` | on / off | SET（管理员） | Flush a relation's buffers on every peer before its storage is removed or truncated. | — |
| `cluster.lock_acquire_cluster_path` | 布尔 | `on` | on / off | 重启 | Enable the cluster lock acquire gate path. | — |
| `cluster.local_fast_path_enabled` | 布尔 | `on` | on / off | 重启 | 在当前实例可以本地授予锁时启用本地授予路径。 | — |
| `cluster.advisory_lock_enabled` | 布尔 | `on` | on / off | SET（管理员） | Enable cross-node globalization of advisory (user) locks. | — |
| `cluster.deadlock_detection_enabled` | 布尔 | `on` | on / off | 重启 | Enable coordinator-driven cross-node deadlock detection. | — |
| `cluster.global_dd_interval_ms` | 整数 | `2000` | 100–600000 | 重启 | Coordinator cross-node deadlock scan period (ms). | — |
| `cluster.deadlock_confirm_interval_ms` | 整数 | `500` | 50–60000 | 重启 | Delay between the two coordinator deadlock-confirm rounds (ms). | — |
| `cluster.cancel_ack_timeout_ms` | 整数 | `1000` | 50–60000 ms | SET（管理员） | Coordinator wait for a cross-node deadlock CANCEL_ACK before retransmit (ms). | — |
| `cluster.cancel_max_retransmit` | 整数 | `3` | 0–100 | SET（管理员） | Bounded cross-node deadlock cancel retransmit attempts before escalation. | — |
| `cluster.victim_repeat_window_ms` | 整数 | `5000` | 0–600000 ms | SET（管理员） | Anti-thrash window for repeated deadlock victim selection (ms). | — |

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
| `cluster.sinval_broadcast_batch_size` | 整数 | `32` | 1–64 | 重启 | Outbound sinval batch drain upper bound. | — |
| `cluster.sinval_broadcast_batch_timeout_ms` | 整数 | `10` | 1–60000 | 重启 | SI Broadcaster main loop WaitLatch timeout (ms). | — |
| `cluster.sinval_broadcast_max_queue_size` | 整数 | `1024` | 64–65536 | 重启 | Outbound + inbound queue ring buffer capacity. | — |
| `cluster.sinval_ack_mode` | 枚举 | `peer_enqueued` | none, peer_enqueued | 重启 | 目录失效通知的确认方式；shared_catalog=on 时拒绝 none。 | 组合检查变化 |
| `cluster.sinval_ack_timeout_ms` | 整数 | `5000` | 100–60000 | 重启 | Sinval acknowledgement observation interval in milliseconds. | — |
| `cluster.sinval_ack_wait_slots` | 整数 | `256` | 64–4096 | 重启 | Capacity of ClusterSinvalAckWait HTAB. | — |

## 相关参考

- [等待事件](wait-events.md)
- [视图、SQL 函数与命令](commands.md)
- [现有配置手册](../../user-guide/configuration.md)：阅读其 reload 说明时，以本页的共享模式例外为准。
