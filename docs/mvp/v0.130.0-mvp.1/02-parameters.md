# 二、相对 PostgreSQL 16.13 新增的参数

Author: SqlRush <sqlrush@gmail.com>

基线：`v0.130.0-mvp.1` / `c581f3835a4a9a76ce5a77c0937930f21765725a`。本篇与下列五份逐项附录共同构成完整参数手册。

## 1. 增量口径与完整目录

此标签的 `cluster_guc.c` 注册 **250 个 `cluster.*` 参数**；启用专门的 injection 构建时，另注册 `cluster_test_force_visibility_cluster_path`，共 251 个注册点。后者不是普通运行配置。原生 PG 16.13 的核心 GUC 表与此标签相比没有参数条目增删；`shared_buffers`、`max_connections`、`wal_level`、`work_mem` 等仍是 PG 参数，不重复计入新增数量。

| 完整附录 | 主要内容 |
|---|---|
| [核心、编队与生命周期](reference/07-guc-core-formation-and-lifecycle.md) | 身份、拓扑文件、quorum、节点加入、阶段超时、后台进程 |
| [通信、GES/GCS、锁与缓存](reference/08-guc-interconnect-locking-and-cache.md) | TCP/RDMA、LMS、GRD、去重、锁等待、死锁、一致读 |
| [存储、WAL、SCN 与事务](reference/09-guc-storage-wal-scn-and-transactions.md) | 共享根、WAL 线程、undo、TT、XID、目录、序列 |
| [恢复、备份、ADG 与 fencing](reference/10-guc-recovery-adg-and-fencing.md) | 正常离开、恢复接口、备份接口、写栅栏与外部隔离 |
| [诊断与测试控制](reference/11-guc-diagnostics-and-test-controls.md) | 注入、强制分支、丢弃、详细统计；默认不启用 |

每一条附录均给出类型、源码默认值、合法范围/枚举、单位、context、生效方式、用途、校验约束与查询示例。**“参数存在”不等于相应功能已完成发布认证**；尤其是 RDMA、ADG、恢复、外部 fencing 和在线永久移除。

权威注册：[该标签的 cluster_guc.c](https://github.com/sqlrush/pgrac/blob/v0.130.0-mvp.1/src/backend/cluster/cluster_guc.c)。源码中的历史注释可能早于实际消费者；本手册对已发现的不一致单独注明，不把注释当运行证明。

## 2. 默认值、配置值、运行值是三件事

```sql
SELECT name, vartype, boot_val, reset_val, setting, unit,
       min_val, max_val, enumvals, context, source, pending_restart
  FROM pg_settings
 WHERE name LIKE 'cluster.%'
 ORDER BY name;
```

- `boot_val`：该二进制注册的默认值；附录默认值采用此口径。
- `setting`：当前会话可见值；命令行、配置文件、`ALTER SYSTEM`、数据库/角色/会话设置可能覆盖默认值。
- `reset_val`：当前会话执行 `RESET` 后回到的值，不一定是源码默认值。
- `source`：值来自哪里。超级用户还可查询 `sourcefile`、`sourceline`，避免多份配置重复覆盖。
- `pending_restart=true`：文件已改，但 postmaster 参数尚未生效。
- 参数缺失可能表示构建未启用集群或条件功能；写入任意 `cluster.foo` 占位值不代表真的注册了该参数。

## 3. 如何修改

| 附录 Context | 正确操作 | 边界 |
|---|---|---|
| `postmaster` | 修改配置后正常停止、重启该实例/协调集群 | `reload` 不能生效；身份、共享布局、编队配置不能边运行边乱改 |
| `sighup` | 修改配置或 `ALTER SYSTEM SET`，然后 `SELECT pg_reload_conf()` | 每节点分别应用；返回 true 只表示已发 reload 信号 |
| `superuser` | 超级用户 `SET`/`SET LOCAL`，或持久配置 | 会话值不自动改变其他会话/其他节点 |
| `user` | 当前会话 `SET`，事务内可 `SET LOCAL` | 仍受 check hook、范围和业务安全校验 |
| `internal` | 只读观察 | 不允许配置覆盖 |

例如只查询一个参数：

```sql
SELECT name, setting, unit, context, source, pending_restart
FROM pg_settings WHERE name = 'cluster.lms_workers';
```

若确需修改一个 reload 参数，先记录旧值与来源，再修改并查询生效结果。不要把下面的语法模板直接复制成实际配置：

```text
ALTER SYSTEM SET <已核对为可设置的参数> = <经过评估的新值>;
SELECT pg_reload_conf();
SELECT name, setting, source, pending_restart FROM pg_settings WHERE name = '<参数>';
```

`ALTER SYSTEM` 写的是当前节点的配置，不是四节点广播。全局行为相关的参数须在四节点一致；`node_id`、本地地址与本地目录则必须按节点不同。密码、凭据、密钥不放进示例配置或公开配置清单。

## 4. 四节点评估首先配置哪些

| 类别 | 参数 | 配置原则 |
|---|---|---|
| 本地身份 | `cluster.node_id`、`cluster.config_file` | 四节点唯一 ID；拓扑内容一致，路径按本地目录设置 |
| 集群与通信 | `cluster.enabled`、`cluster.interconnect_tier`、`cluster.lms_enabled`、`cluster.lms_workers` | seed 阶段与四节点运行阶段不同；首轮使用 TCP tier1，不擅自切 RDMA |
| 共享数据 | `cluster.shared_storage_backend`、`cluster.shared_data_dir`、`cluster.smgr_user_relations` | 真正共享、相同内容的根；不使用 stub 当多节点后端 |
| 共享目录 | `cluster.controlfile_shared_authority`、`cluster.shared_catalog`、`cluster.merged_recovery` | 按 seed/join 配套设置，不临时关闭来绕过启动校验 |
| WAL 身份 | `cluster.wal_threads_dir` | 所有节点可见同一 WAL 根，每节点独占自己的 thread；非空规范化绝对路径 |
| 编队 | `cluster.voting_disks`、`cluster.voting_disk_size_bytes`、`cluster.allow_single_node` | 介质先正确初始化；严格四节点运行时不放宽单节点准入 |
| 节点生命周期 | `cluster.online_join`、`cluster.clean_leave_enabled` | 接口 opt-in，不等于故障接管/在线扩容已认证 |
| 全局事务 | `cluster.xid_striping`、`cluster.crossnode_runtime_visibility`、`cluster.crossnode_write_write` | 一起核对跨节点事务能力；不能只打开共享文件访问 |
| 一致读与镜像 | `cluster.crossnode_cr_data_plane`、`cluster.undo_gcs_coherence`、`cluster.past_image` | 按验证过的组合启用；不以关闭可见性保护提高吞吐 |
| 写入安全 | `cluster.write_fence_enforcement`、`cluster.write_fence_lease_ms` | enforcement 保留；租约调整涉及隔离姿态，不是普通性能优化 |

具体命令、配置文件与阶段顺序见[部署篇](01-linux-four-node-deployment.md)。此表不是全量参数的替代，也不是独立的可启动配置。

## 5. 容量与性能参数

- `cluster.pcm_grd_max_entries`、GES/GCS 去重容量、reply wait 容量、缓存块数等会占用共享内存或进程内存。增大容量不证明请求生命周期正确，也不能代替清理积压诊断。
- `cluster.lms_workers` 增加 DATA-plane worker；需同时检查 CPU、端口范围、队列与存储负载。不能直接把 worker 数当吞吐倍数。
- `cluster.undo_segments_max_per_instance` 的默认值和注册上限均为 **256**；不能把超过上限的数写入配置来解决回收滞后。
- `cluster.read_scache`、页面 SCN 快路径、一致读缓存等只覆盖各自特定路径，不是“打开即保证所有读取更快”。保留可见性与身份校验。
- PG 原生内存参数仍然适用；四实例同机时总内存是四份实例开销之和。附录不提供脱离实际内存/负载的一套万能调优值。

## 6. 时间参数与本标签的重要细节

时间参数分为轮询周期、重传节奏、观测/诊断阈值、具体操作期限和安全租约。**不能看到 `_timeout` 就认定到期一定给客户端报错，也不能一概认定它会无限重试。** 应结合具体消费者、日志 reason 和等待事件判断。

本标签中特别注意：

1. `cluster.quorum_poll_interval_ms` 注册默认 **2000 ms**；QVOTEC 实际发布租约使用 **30 倍轮询间隔**，默认对应 60000 ms。旧注册长说明中的“2 倍”已经不是此标签的实际代码行为。租约不是另一个可独立设置的 GUC。[QVOTEC 源码](https://github.com/sqlrush/pgrac/blob/v0.130.0-mvp.1/src/backend/cluster/cluster_qvotec.c)
2. `cluster.write_fence_lease_ms` 注册默认仍是 **6000 ms**，不是 60000。60000 是实验运行中的显式配置选择，不能写成产品默认值。
3. 拉长租约容忍停顿的同时，也改变失去通信/多数后隔离的时间窗口。没有真实外部隔离认证时，不应把实验姿态宣传成生产防脑裂保证。
4. `cluster.ges_request_timeout_ms=-1` 与重传设置有组合约束，不能只改变一个值。PG 原生 `lock_timeout`、`statement_timeout`、调用方取消也不因集群参数而自动失效。
5. 不通过不断增大期限掩盖 FATAL/PANIC、身份错误、清理债务或无法证明的事务结果。

## 7. 保留、条件和危险参数

| 参数/类别 | 此标签的使用边界 |
|---|---|
| `cluster.block_self_contained` | 兼容开关；当前 on/off 不改变行为 |
| `cluster.smart_fusion` | `on` 被配置校验拒绝，不是可用优化 |
| `cluster.interconnect_rdma_crc_offload` | `on` 被拒绝；不关闭应用层完整性校验 |
| `cluster.space_affinity` | `dynamic` 被拒绝；枚举注册不等于全部选项都可用 |
| `cluster.adg_primary_thread_count` | 内部派生，只读，不由管理员手填 |
| `cluster.injection_points`、`force`/`suppress`/`drop` 类测试项 | 保持默认未武装；部分接口可以令进程报错、休眠或崩溃 |
| `cluster_test_force_visibility_cluster_path` | 仅 `ENABLE_INJECTION` 条件构建注册，不进入四节点用户模板 |
| `cluster.online_node_removal` | 默认 off；永久移除不可当作正常停机，MVP 未认证端到端运维能力 |

## 8. 配置留档

上线评估前逐节点导出 `pg_settings` 的上述列、完整生效配置（脱敏）与拓扑；记录编译选项。重启后比较 `pending_restart` 与四节点差异。只保存自己写过的几行配置，无法证明最终运行姿态一致。

回退配置不等于可以回退二进制或磁盘格式。正常重启支持的是已经验证的同候选、同数据与相容配置；不要依据本篇推导跨版本原地升级资格。
