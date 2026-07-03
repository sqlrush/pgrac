# Cluster wait events

linkdb registers 116 cluster-specific wait events distributed across
13 classes.  Each row in `pg_stat_cluster_wait_events` corresponds
to one entry in this table.

The values appear in the standard `pg_stat_activity.wait_event_type`
and `pg_stat_activity.wait_event` columns when a backend is waiting
on a cluster operation.

In the current release some events are registered before their full
runtime call sites are active.  They become observable as the
corresponding subsystem ships or as a selected transport path reaches
the relevant wait.

## Cluster: GES (5 events)

Global Enqueue Service — distributed lock manager events.

| Name | Description |
|---|---|
| `GesEnqueueAcquire` | Waiting for a GES lock acquire to be granted by the master node |
| `GesEnqueueConvert` | Waiting for a GES lock conversion (e.g. shared → exclusive) |
| `GesEnqueueReleaseAck` | Waiting for the master to acknowledge a GES lock release |
| `GesMasterQuery` | Waiting for a GES master lookup response |
| `GesLocalFastPath` | Local-only GES fast-path serialisation |

## Cluster: PCM (24 events)

Parallel Cache Management — block-level cluster locks, GCS block
shipping, and Smart Fusion terminal-authority waits.

| Name | Description |
|---|---|
| `PcmBlockReadNS` | Waiting for a block read with shared mode (Null → Shared) |
| `PcmBlockReadNX` | Waiting for a block read with exclusive mode (Null → Exclusive) |
| `PcmBlockWriteSX` | Waiting for shared → exclusive lock upgrade for write |
| `PcmBlockConvertWait` | Waiting for a PCM lock conversion to complete |
| `PcmBlockDowngrade` | Waiting for downgrade ack from current holder |
| `PcmItlCleanout` | Waiting for ITL slot cleanout / commit-SCN backfill |
| `ClusterPcmGrdInit` | Initializing the PCM GRD shared-memory hash table |
| `ClusterPcmTransitionApply` | Waiting while applying a PCM state transition under the entry lock |
| `ClusterPcmCompatibleStateWait` | Waiting for an incompatible PCM holder to release |
| `ClusterGcsReplyWait` | Waiting for a GCS transition ACK from the master |
| `ClusterGCSBlockShipWait` | Waiting for a GCS block reply with page payload |
| `ClusterGCSBlockRequestDispatch` | Dispatch-side wait for a GCS block request |
| `ClusterGCSBlockReplyDispatch` | Dispatch-side wait for a GCS block reply |
| `ClusterGCSBlockChecksumFail` | Diagnostic wait path after received block checksum failure |
| `ClusterGCSBlockRetransmitWait` | Backoff wait before retransmitting a GCS block request |
| `ClusterGCSBlockEpochStaleRetry` | Retry path after a stale-epoch GCS block reply |
| `ClusterGCSBlockInvalidateBroadcast` | Master-side invalidate broadcast before granting a writer |
| `ClusterGCSBlockInvalidateAckWait` | Waiting for invalidate ACKs from all enumerated holders |
| `ClusterGCSBlockStarvationRetry` | Reader retry backoff while a pending writer barrier exists |
| `ClusterGCSBlockRecovering` | Waiting while a block resource is fenced as recovering |
| `ClusterSmartFusionCommitBrake` | Reserved Smart Fusion pre-commit dependency wait; the enabled path is currently guarded off |
| `ClusterSmartFusionDbwrBrake` | Reserved Smart Fusion writeback-brake wait; the enabled path is currently guarded off |
| `ClusterSmartFusionOriginDurable` | Reserved Smart Fusion durable-LSN gossip wait; the enabled path is currently guarded off |
| `ClusterCfTerminalResolve` | Waiting while terminal authority resolves cross-instance undo / TT evidence |

## Cluster: BufferShip (5 events)

Cache Fusion buffer transfer between nodes.

| Name | Description |
|---|---|
| `BufferShipCrBuild` | Waiting for consistent-read snapshot construction on the source node |
| `BufferShipCrSend` | Waiting for the consistent-read block send to complete |
| `BufferShipCrReceive` | Waiting for an incoming consistent-read block |
| `BufferShipCurrentSend` | Waiting for the current-version block send to complete |
| `BufferShipCurrentReceive` | Waiting for an incoming current-version block |

## Cluster: SCN (4 events)

System Change Number propagation across nodes.

| Name | Description |
|---|---|
| `ScnBocFlushWait` | Waiting for batch-of-commits SCN flush |
| `ScnPiggybackMerge` | Waiting for piggyback SCN merge with peer message |
| `ScnCrossNodeCompare` | Waiting for cross-node SCN compare round-trip |
| `ScnAdvanceBroadcast` | Waiting for SCN advance broadcast to acknowledge |

## Cluster: Reconfig (5 events)

Cluster reconfiguration after membership changes.

| Name | Description |
|---|---|
| `ReconfigGrdRebuild` | Waiting for global resource directory rebuild |
| `ReconfigLockRecovery` | Waiting for distributed lock recovery |
| `ReconfigFenceWait` | Waiting for fence (eviction) of a stale node |
| `ReconfigMasterSelection` | Waiting for new master selection round |
| `ReconfigBarrierWait` | Waiting at a reconfig protocol barrier |

## Cluster: Recovery (5 events)

Cluster-level recovery / WAL apply.

| Name | Description |
|---|---|
| `RecoveryWalFetch` | Waiting for WAL fetch from peer node |
| `RecoveryKwayMerge` | Waiting for k-way WAL merge from multiple peers |
| `RecoveryApplyPerThread` | Waiting for per-thread WAL apply slot |
| `RecoveryUndoReplay` | Waiting for undo segment replay |
| `RecoveryPcmStateRestore` | Waiting for PCM lock state restoration |

## Cluster: Sinval (3 events)

Cross-node shared invalidation broadcast.

| Name | Description |
|---|---|
| `SinvalBroadcastSend` | Waiting for sinval broadcast send to all peers |
| `SinvalBroadcastReceive` | Waiting for incoming sinval broadcast |
| `SinvalInjectLocalQueue` | Waiting to inject received sinval into local queue |

## Cluster: Interconnect (13 events)

Network transport layer.

| Name | Description |
|---|---|
| `InterconnectRdmaSend` | Waiting for an RDMA send completion |
| `InterconnectRdmaRecv` | Waiting for an RDMA receive |
| `ClusterICRdmaPoll` | Waiting for RDMA completion-queue polling |
| `ClusterICRdmaConnect` | Waiting for RDMA connection setup |
| `ClusterICRdmaFallback` | Waiting on the TCP fallback transport selected by the RDMA mux |
| `InterconnectTierSwitch` | Waiting for transport tier switch (e.g. RDMA → TCP fallback) |
| `InterconnectConnectRetry` | Waiting for an interconnect reconnection attempt |
| `ClusterICTcpAccept` | LMON waiting on the Tier 1 listener for an incoming peer connection |
| `ClusterICTcpConnect` | LMON waiting for an outbound nonblocking `connect(2)` to a peer to complete |
| `ClusterICTcpRecv` | LMON waiting to read bytes from a peer socket |
| `ClusterICTcpSend` | LMON waiting to write bytes to a peer socket |
| `ClusterICHeartbeatWait` | LMON main loop is idle, waiting for the next heartbeat tick |
| `ClusterICReconnect` | LMON waiting before re-attempting a connection to a peer that is currently `down` |

## Cluster: Undo (8 events)

Undo segment access, durable transaction-table I/O, and the local undo buffer pool.

| Name | Description |
|---|---|
| `UndoRemoteRead` | Waiting for a remote undo segment read |
| `UndoTtLookupRemote` | Waiting for a remote transaction-table lookup |
| `UndoSegmentFetch` | Waiting for an undo segment fetch |
| `UndoRetentionWait` | Waiting on undo retention to expire |
| `ClusterCRConstruct` | Waiting to construct a consistent-read block image |
| `ClusterTTDurableIO` | Waiting on durable transaction-table slot header I/O |
| `ClusterUndoBufFlush` | Waiting on an undo buffer write-back to storage |
| `ClusterUndoExtentClaim` | Waiting to extend an undo segment while claiming an extent |

## Cluster: ADG (4 events)

Active Data Guard / read-only standby coordination.

| Name | Description |
|---|---|
| `AdgMrpApplyWait` | Waiting for the managed recovery process apply |
| `AdgWalReceiveLag` | Waiting for WAL receive to catch up |
| `AdgReadSnapshotWait` | Waiting for a read snapshot to be released |
| `AdgScnSyncWait` | Waiting for SCN sync between primary and standby |

## Cluster: SharedFs (12 events)

Shared-storage provider and raw block-device I/O.

| Name | Description |
|---|---|
| `ClusterSharedFsRead` | Waiting for generic shared-storage read |
| `ClusterSharedFsWrite` | Waiting for generic shared-storage write |
| `ClusterSharedFsExtend` | Waiting for generic shared-storage extend |
| `ClusterSharedFsTruncate` | Waiting for generic shared-storage truncate |
| `ClusterSharedFsFsync` | Waiting for generic shared-storage fsync |
| `ClusterBlockDeviceRead` | Waiting for raw block-device read |
| `ClusterBlockDeviceWrite` | Waiting for raw block-device write |
| `ClusterBlockDevicePrefetch` | Waiting for raw block-device prefetch hint |
| `ClusterBlockDeviceWriteback` | Waiting for raw block-device writeback hint |
| `ClusterBlockDeviceSync` | Waiting for raw block-device barrier sync |
| `ClusterBlockDevicePrProbe` | Waiting for SCSI-3 PR capability probe |
| `ClusterBlockDevicePrRegister` | Waiting for SCSI-3 PR own-key registration |

## Querying

```sql
-- Total registered (116):
SELECT count(*) FROM pg_stat_cluster_wait_events;

-- Per-class counts:
SELECT type, count(*)
  FROM pg_stat_cluster_wait_events
 GROUP BY type ORDER BY type;

-- Find an event by name:
SELECT * FROM pg_stat_cluster_wait_events
 WHERE name = 'PcmBlockReadNS';

-- Currently-active waits (joins with pg_stat_activity):
SELECT pid, wait_event_type, wait_event, query
  FROM pg_stat_activity
 WHERE wait_event_type LIKE 'Cluster:%';
```

## Reporting issues

File issues at <https://github.com/sqlrush/linkdb/issues>.
