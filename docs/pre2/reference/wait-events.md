# PRE2 等待事件参考

适用代码版本：`c45bdc5d4392751a33659acf8dcaa2b76254e73a`。本页只列 PGRAC 增加的事件；原生 PostgreSQL 的 `Lock`、`LWLock`、`IO` 等事件仍然适用。

交付镜像 `2d857abffc` 的参数注册、等待名称及 SQL 函数／视图定义与本页核对版本一致；动态诊断键与可用功能仍按实际二进制识别。

该版本有 **152 个可解码的集群事件名称、13 类**。名称目录 `pg_stat_cluster_wait_events` 当前只列出其中 **123 个**，另有 **29 个**可出现在活动等待中但没有列入该目录。下表明确标出这一差别。目录中有名字并不表示当前配置会执行相应路径；备用库、部分传输模式、测试或未启用功能也保留名称。

## PRE2 变化

与 `v0.132.0` 相比，这 152 个事件名称没有新增、删除或改名。PRE2 扩大了共享目录和共享启动等操作的使用范围；不能把名称未变理解为发生频率、耗时或所有调用位置未变。旧参考页“118 个事件”的总数不适用于本页版本。

## 查询正在等待的进程

使用具备监控权限的账号查询每个节点；查询一端不等于采集了四端。

```sql
SELECT pid, backend_type, state, wait_event_type, wait_event,
       query_start, left(query, 120) AS query
FROM pg_stat_activity
WHERE wait_event_type LIKE 'Cluster:%'
ORDER BY backend_type, pid;
```

`query_start` 是语句开始时间，不是当前等待开始时间。后台进程的正常空闲等待也会被列出，不能把所有等待都视为故障。

```sql
-- 名称目录，不是累计次数或耗时。
SELECT type, name FROM pg_stat_cluster_wait_events ORDER BY type, name;

-- 当前采样中，各事件有多少进程正在等待。
SELECT backend_type, wait_event_type, wait_event, count(*) AS waiting_processes
FROM pg_stat_activity
WHERE wait_event IS NOT NULL
GROUP BY backend_type, wait_event_type, wait_event
ORDER BY waiting_processes DESC;
```

连续采样可估计等待占用比例；单次查询不能给出累计等待毫秒。性能计时及其采集成本见 [pg_cluster_state](commands.md#pg_cluster_state-的用法)。不能把请求方的整段等待直接当作网络时间，或与对端服务计时直接相加。

## 事件目录

“名称视图”列的“是”指 `pg_stat_cluster_wait_events` 有该行，“否”指应从 `pg_stat_activity` 的实际等待观察。本页保留部分英文说明，事件名大小写与 SQL 输出一致。

### Cluster: GES

| 事件名 | 含义 | 名称视图 |
|---|---|---|
| `GesEnqueueAcquire` | Waiting for a GES lock acquire to be granted by the master node | 是 |
| `GesEnqueueConvert` | Waiting for a GES lock conversion (e.g. shared → exclusive) | 是 |
| `GesEnqueueReleaseAck` | Waiting for the master to acknowledge a GES lock release | 是 |
| `GesMasterQuery` | Waiting for a GES master lookup response | 是 |
| `GesLocalFastPath` | Local-only GES fast-path serialisation | 是 |
| `GesGrantWait` | 等待全局锁授予回复。 | 否 |
| `GesConvertWait` | 等待全局锁模式转换回复。 | 否 |
| `GesDrain` | 等待全局锁相关工作排空。 | 否 |
| `GesBastWait` | 等待持锁方处理阻塞通知。 | 否 |
| `GesDeadlockProbeWait` | 等待分布式死锁探测结果。 | 否 |
| `GesCancelDrain` | 等待取消请求及相关锁工作收尾。 | 否 |
| `GesDeadlockReassemblyWait` | 等待死锁探测消息分片组装。 | 否 |
| `GesTxEnqueueWait` | 等待持有行锁的事务结束。 | 否 |

### Cluster: PCM

| 事件名 | 含义 | 名称视图 |
|---|---|---|
| `PcmBlockReadNS` | Waiting for a block read with shared mode (Null → Shared) | 是 |
| `PcmBlockReadNX` | Waiting for a block read with exclusive mode (Null → Exclusive) | 是 |
| `PcmBlockWriteSX` | Waiting for shared → exclusive lock upgrade for write | 是 |
| `PcmBlockConvertWait` | 块访问被暂时拒绝后的本地退避，或转换等待；不等于网络耗时。 | 是 |
| `PcmBlockDowngrade` | Waiting for downgrade ack from current holder | 是 |
| `PcmItlCleanout` | Waiting for ITL slot cleanout / commit-SCN backfill | 是 |
| `ClusterPcmGrdInit` | Initializing the PCM GRD shared-memory hash table | 是 |
| `ClusterPcmTransitionApply` | Waiting while applying a PCM state transition under the entry lock | 是 |
| `ClusterPcmCompatibleStateWait` | 等待块状态满足本次访问／换权要求；包括正在进行的转换和本地并发工作，不能仅解释为远端持锁。 | 是 |
| `ClusterGcsReplyWait` | Waiting for a GCS transition ACK from the master | 是 |
| `ClusterGCSBlockShipWait` | 等待块服务回复；多个调用点共用，包括当前块、一致读块、事务证明及 undo 判定，不保证每次都返回整页。 | 是 |
| `ClusterGCSBlockRequestDispatch` | Dispatch-side wait for a GCS block request | 是 |
| `ClusterGCSBlockReplyDispatch` | Dispatch-side wait for a GCS block reply | 是 |
| `ClusterGCSBlockChecksumFail` | Diagnostic wait path after received block checksum failure | 是 |
| `ClusterGCSBlockRetransmitWait` | Backoff wait before retransmitting a GCS block request | 是 |
| `ClusterGCSBlockEpochStaleRetry` | Retry path after a stale-epoch GCS block reply | 是 |
| `ClusterGCSBlockInvalidateBroadcast` | Master-side invalidate broadcast before granting a writer | 是 |
| `ClusterGCSBlockInvalidateAckWait` | Waiting for invalidate ACKs from all enumerated holders | 是 |
| `ClusterGCSBlockStarvationRetry` | Reader retry backoff while a pending writer barrier exists | 是 |
| `ClusterGCSBlockRecovering` | Waiting while a block resource is fenced as recovering | 是 |
| `ClusterSmartFusionCommitBrake` | 预留的提前传块提交等待；本预览不启用相应功能。 | 是 |
| `ClusterSmartFusionDbwrBrake` | 预留的提前传块写回等待；本预览不启用相应功能。 | 是 |
| `ClusterSmartFusionOriginDurable` | 预留的提前传块持久进度等待；本预览不启用相应功能。 | 是 |
| `ClusterCfTerminalResolve` | 等待跨实例 undo 或事务状态的终态判定。 | 是 |
| `GcsMultixactDescribeWait` | 等待远端 MultiXact 成员说明。 | 否 |
| `GcsMultixactMemberProofWait` | 等待远端 MultiXact 成员状态证明。 | 否 |
| `GcsMultixactStatsWait` | 等待远端 MultiXact 统计回复。 | 否 |

### Cluster: BufferShip

| 事件名 | 含义 | 名称视图 |
|---|---|---|
| `BufferShipCrBuild` | Waiting for consistent-read snapshot construction on the source node | 是 |
| `BufferShipCrSend` | Waiting for the consistent-read block send to complete | 是 |
| `BufferShipCrReceive` | Waiting for an incoming consistent-read block | 是 |
| `BufferShipCurrentSend` | Waiting for the current-version block send to complete | 是 |
| `BufferShipCurrentReceive` | Waiting for an incoming current-version block | 是 |

### Cluster: SCN

| 事件名 | 含义 | 名称视图 |
|---|---|---|
| `ScnBocFlushWait` | Waiting for batch-of-commits SCN flush | 是 |
| `ScnPiggybackMerge` | Waiting for piggyback SCN merge with peer message | 是 |
| `ScnCrossNodeCompare` | Waiting for cross-node SCN compare round-trip | 是 |
| `ScnAdvanceBroadcast` | Waiting for SCN advance broadcast to acknowledge | 是 |

### Cluster: Reconfig

| 事件名 | 含义 | 名称视图 |
|---|---|---|
| `ReconfigGrdRebuild` | Waiting for global resource directory rebuild | 是 |
| `ReconfigLockRecovery` | Waiting for distributed lock recovery | 是 |
| `ReconfigFenceWait` | Waiting for fence (eviction) of a stale node | 是 |
| `ReconfigMasterSelection` | Waiting for new master selection round | 是 |
| `ReconfigBarrierWait` | Waiting at a reconfig protocol barrier | 是 |
| `ClusterGrdShardRemaster` | Waiting during GRD shard remaster coordination | 是 |
| `ClusterWriteFenceMarkerWrite` | Waiting for durable fence-marker majority write | 是 |
| `ReconfigJoinConvergence` | 等待节点加入过程收敛；在线加入不在本预览的操作范围。 | 否 |
| `ReconfigNodeRemoveCleanupWait` | Waiting for survivor cleanup ACKs during node removal | 是 |

### Cluster: Recovery

| 事件名 | 含义 | 名称视图 |
|---|---|---|
| `RecoveryWalFetch` | Waiting for WAL fetch from peer node | 是 |
| `RecoveryKwayMerge` | Waiting for k-way WAL merge from multiple peers | 是 |
| `RecoveryApplyPerThread` | Waiting for per-thread WAL apply slot | 是 |
| `RecoveryUndoReplay` | Waiting for undo segment replay | 是 |
| `RecoveryPcmStateRestore` | Waiting for PCM lock state restoration | 是 |
| `ClusterThreadRecovery` | Waiting during cluster WAL-thread recovery orchestration | 是 |
| `ClusterWriteFenceVerify` | Waiting while verifying a durable fence marker | 是 |

### Cluster: Sinval

| 事件名 | 含义 | 名称视图 |
|---|---|---|
| `SinvalBroadcastSend` | Waiting for sinval broadcast send to all peers | 是 |
| `SinvalBroadcastReceive` | Waiting for incoming sinval broadcast | 是 |
| `SinvalInjectLocalQueue` | Waiting to inject received sinval into local queue | 是 |
| `SinvalAckWait` | Waiting for sinval ACK barrier completion | 是 |
| `SinvalAckSend` | Waiting while sending a sinval ACK | 是 |
| `SinvalAckReceive` | Waiting while receiving a sinval ACK | 是 |

### Cluster: Interconnect

| 事件名 | 含义 | 名称视图 |
|---|---|---|
| `InterconnectRdmaSend` | Waiting for an RDMA send completion | 是 |
| `InterconnectRdmaRecv` | Waiting for an RDMA receive | 是 |
| `ClusterICRdmaPoll` | Waiting for RDMA completion-queue polling | 是 |
| `InterconnectRdmaBusypoll` | Waiting while bounded busypoll completion draining is active | 是 |
| `InterconnectRdmaInlineSend` | Waiting for an inline RDMA send completion | 是 |
| `ClusterICRdmaConnect` | Waiting for RDMA connection setup | 是 |
| `ClusterICRdmaFallback` | Waiting on the TCP fallback transport selected by the RDMA mux | 是 |
| `InterconnectTierSwitch` | Waiting for transport tier switch (e.g. RDMA → TCP fallback) | 是 |
| `InterconnectConnectRetry` | Waiting for an interconnect reconnection attempt | 是 |
| `ClusterICTcpAccept` | 等待 TCP 连接接入。 | 否 |
| `ClusterICTcpConnect` | 等待 TCP 连接建立。 | 否 |
| `ClusterICTcpRecv` | 等待 TCP 接收。 | 否 |
| `ClusterICTcpSend` | 等待 TCP 发送。 | 否 |
| `ClusterICHeartbeatWait` | 等待集群互联心跳。 | 否 |
| `ClusterICReconnect` | 等待集群互联重连。 | 否 |
| `ClusterLmsDataRecv` | LMS 数据通道等待接收或接收就绪。 | 是 |
| `ClusterLmsDataSend` | LMS 数据通道等待发送或发送就绪。 | 是 |

### Cluster: Undo

| 事件名 | 含义 | 名称视图 |
|---|---|---|
| `UndoRemoteRead` | Waiting for a remote undo segment read | 是 |
| `UndoTtLookupRemote` | Waiting for a remote transaction-table lookup | 是 |
| `UndoSegmentFetch` | Waiting for an undo segment fetch | 是 |
| `UndoRetentionWait` | Waiting on undo retention to expire | 是 |
| `ClusterCRConstruct` | 构造一致读数据块。 | 否 |
| `ClusterTTDurableIO` | 读取或更新持久事务状态的 I/O；不能据名称认定全为 fsync。 | 否 |
| `ClusterUndoBufFlush` | undo 缓冲写出或等待写出完成。 | 否 |
| `ClusterUndoExtentClaim` | 等待 undo 空间分配。 | 否 |
| `UndoBlockGrantWait` | 等待 undo 块访问权限授予。 | 是 |
| `UndoBlockInvalidateWait` | 等待 undo 块缓存失效。 | 是 |
| `UndoBlockRemasterWait` | 等待 undo 块管理节点切换完成。 | 是 |

### Cluster: ADG

| 事件名 | 含义 | 名称视图 |
|---|---|---|
| `AdgMrpApplyWait` | Waiting for the managed recovery process apply | 是 |
| `AdgWalReceiveLag` | Waiting for WAL receive to catch up | 是 |
| `AdgReadSnapshotWait` | Waiting for a read snapshot to be released | 是 |
| `AdgScnSyncWait` | Waiting for SCN sync between primary and standby | 是 |

### Cluster: SharedFs

| 事件名 | 含义 | 名称视图 |
|---|---|---|
| `ClusterSharedFsRead` | Waiting for generic shared-storage read | 是 |
| `ClusterSharedFsWrite` | Waiting for generic shared-storage write | 是 |
| `ClusterSharedFsExtend` | Waiting for generic shared-storage extend | 是 |
| `ClusterSharedFsTruncate` | Waiting for generic shared-storage truncate | 是 |
| `ClusterSharedFsFsync` | Waiting for generic shared-storage fsync | 是 |
| `ClusterBlockDeviceRead` | Waiting for raw block-device read | 是 |
| `ClusterBlockDeviceWrite` | Waiting for raw block-device write | 是 |
| `ClusterBlockDevicePrefetch` | Waiting for raw block-device prefetch hint | 是 |
| `ClusterBlockDeviceWriteback` | Waiting for raw block-device writeback hint | 是 |
| `ClusterBlockDeviceSync` | Waiting for raw block-device barrier sync | 是 |
| `ClusterBlockDevicePrProbe` | Waiting for SCSI-3 PR capability probe | 是 |
| `ClusterBlockDevicePrRegister` | Waiting for SCSI-3 PR own-key registration | 是 |

### Cluster: StartupPhase

| 事件名 | 含义 | 名称视图 |
|---|---|---|
| `ClusterStartupPhase0Wait` | Waiting in startup phase 0 | 是 |
| `ClusterStartupPhase1Wait` | Waiting in startup phase 1 | 是 |
| `ClusterStartupPhase2Wait` | Waiting in startup phase 2 | 是 |
| `ClusterStartupPhase3Wait` | Waiting in startup phase 3 | 是 |
| `ClusterStartupPhase4Wait` | Waiting in startup phase 4 | 是 |

### Cluster: BgProc

| 事件名 | 含义 | 名称视图 |
|---|---|---|
| `ClusterBgProcLmonMainLoop` | LMON main loop wait | 是 |
| `ClusterBgProcLckMainLoop` | LCK main loop wait | 是 |
| `ClusterBgProcDiagMainLoop` | DIAG main loop wait | 是 |
| `ClusterBgProcClusterStatsMainLoop` | cluster_stats main loop wait | 是 |
| `ClusterBgProcCssdMainLoop` | CSSD main loop wait | 是 |
| `ClusterBgProcQvotecMainLoop` | quorum-vote coordinator main loop wait | 是 |
| `ClusterVotingDiskRead` | Voting-disk read wait | 是 |
| `ClusterVotingDiskWrite` | Voting-disk write wait | 是 |
| `ClusterWalThreadClaimRead` | WAL-thread claim-file read wait | 是 |
| `ClusterWalThreadClaimWrite` | WAL-thread claim-file write wait | 是 |
| `ClusterWalStateRead` | WAL-state registry read wait | 是 |
| `ClusterWalStateWrite` | WAL-state registry write wait | 是 |
| `ClusterFenceBackendInterruptCheck` | Fence backend interrupt-check wait | 是 |
| `BgProcLmonReconfigTick` | LMON reconfiguration tick wait | 是 |
| `ClusterLmdStartup` | LMD startup wait | 是 |
| `ClusterLmdScan` | LMD wait-for-graph scan wait | 是 |
| `ClusterLmdIdle` | LMD idle wait | 是 |
| `ClusterGesS4Wait` | GES S4 caller-side wait | 是 |
| `ClusterLmdProbe` | LMD deadlock probe handling wait | 是 |
| `ClusterGesReplyWait` | Cross-node GES reply wait | 是 |
| `ClusterLmdProbeCollect` | LMD probe result collection wait | 是 |
| `ClusterLmsNativeProbeWait` | LMS native-lock probe wait | 是 |
| `ClusterNativeProbeReplyWait` | Native-lock probe reply wait | 是 |
| `ClusterBgProcUndoCleanerMainLoop` | undo cleaner main loop wait | 是 |
| `ClusterUndoCleanerSegmentScan` | Undo cleaner segment-scan wait | 是 |
| `ClusterSqRefillWait` | 等待序列号段补充。 | 否 |
| `ClusterCfEnqueueWait` | 等待共享控制信息访问锁。 | 否 |
| `ClusterRelExtendWait` | 等待关系扩展锁。 | 否 |
| `ClusterObjectFlushWait` | 等待对象移除或截断前的跨节点缓冲刷新。 | 否 |
| `ClusterOidLease` | 等待全局 OID 号段分配。 | 否 |
| `ClusterRelmapWrite` | 等待共享关系映射写入。 | 否 |
| `ClusterCatalogVisResolve` | 等待共享目录的事务可见性判定。 | 否 |

## 读数的使用边界

- `PcmBlockConvertWait` 的样本表示本地等待／退避时间；不能由此计算被拒原因的比例或断定必须扩容网络。
- `ClusterPcmCompatibleStateWait` 与 `ClusterGCSBlockShipWait` 都有多个用途。定位时同时保留节点、进程类型、时间、SQL 和同期日志。
- `ClusterTTDurableIO`、`ClusterSharedFsRead`、`ClusterSharedFsFsync` 等可能属于后台处理；业务连接与后台进程应分别统计。
- 看到心跳、重连、恢复或启动事件时，先查看对应阶段和错误详情；事件名称本身不证明成员已失效，也不证明恢复已完成。
- 需要比较两次采样时，保留同一进程生命周期和统计窗口。重启后的计数不能直接减去重启前的计数。

相关页面：[参数](parameters.md)、[视图与命令](commands.md)。
