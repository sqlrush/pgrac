# System views

linkdb adds cluster-aware system views to the standard PostgreSQL
catalog.  These views are present in `--enable-cluster` builds; in
`--disable-cluster` builds the backing functions are unavailable or
return zero rows, depending on whether the function is read-only or
operator-facing.

| View | Purpose |
|---|---|
| `pg_cluster_nodes` | Cluster topology (the parsed `pgrac.conf`) |
| `pg_stat_cluster_wait_events` | Cluster-specific wait events on the local node |
| `pg_stat_gcluster_wait_events` | Cluster wait events globally (cross-node placeholder) |
| `pg_cluster_ic_peers` | Tier1 TCP interconnect peer state and counters |
| `pg_stat_cluster_ic` | TCP/RDMA mux and RDMA transport observability |
| `pg_stat_cluster_backup` | Current cluster backup state on the local node |
| `pg_cluster_backup_history` | Latest cluster backup manifest summary |
| `pg_cluster_restore_points` | Cluster restore points visible to PITR status |
| `pg_cluster_pitr_status` | Cluster PITR target reachability status |
| `pg_cluster_state` | Diagnostic snapshot of cluster GUCs, counters, and subsystem state |

## Cluster Backup / PITR Views

The cluster backup surface exposes the manifest and target-resolution
state used by `pg_cluster_backup_start`, `pg_cluster_backup_stop`, and
`pg_cluster_create_restore_point`.

Current 6.5 scope is conservative:

- The views, catalog entries, manifest validators, PITR target resolver,
  shared-memory state, and IC wire format are present as substrate.
- Mutating physical backup and restore-point entry points fail closed
  with `feature_not_supported` until the cluster physical capture,
  durable WAL pin, restore-point commit-drain barrier, restore, and PITR
  replay paths are implemented.
- No manifest is published unless WAL, undo, transaction-table, SCN, and
  control-file inclusion are proven.  The current substrate therefore
  refuses to create a manifest instead of reporting a partial or unsound
  backup as complete.

### `pg_stat_cluster_backup`

One row describing the current or most recent cluster backup on this
node.

| Column | Type | Description |
|---|---|---|
| `in_progress` | `bool` | True while this session has an active cluster backup. |
| `backup_id` | `text` | Backup label/id, or NULL before the first backup. |
| `coordinator_node_id` | `int4` | Local node id that started the backup. |
| `start_redo_lsn` | `pg_lsn` | Checkpoint redo LSN used as the backup start contract. |
| `checkpoint_lsn` | `pg_lsn` | Checkpoint record LSN captured at backup start. |
| `stop_cut_lsn` | `pg_lsn` | WAL cut LSN captured at backup stop. |
| `consistent_scn` | `int8` | Cluster SCN selected for the backup cut. |
| `manifest_crc` | `int8` | CRC32C of the latest manifest image. |
| `started_at` | `timestamptz` | Local timestamp when the backup started. |
| `stopped_at` | `timestamptz` | Local timestamp when the backup stopped. |
| `backup_parallel_channels` | `int4` | Configured copy-channel capacity for the backup substrate. |
| `backup_wal_retention` | `int4` | Configured WAL retention hint, in MB. |
| `restore_points_enabled` | `bool` | Whether automatic PITR restore-point scheduling is enabled. |
| `restore_point_interval_ms` | `int4` | Automatic restore-point scheduling interval, in milliseconds. |

### `pg_cluster_backup_history`

Returns the latest cluster backup manifest summary retained in shared
memory.

| Column | Type | Description |
|---|---|---|
| `backup_id` | `text` | Backup label/id. |
| `consistent_scn` | `int8` | SCN that defines the backup cut. |
| `scn_durable_peak` | `int8` | Highest durable SCN covered by the cut. |
| `timeline` | `int4` | WAL timeline recorded at backup stop. |
| `catversion` | `int8` | Catalog version used to reject incompatible restores. |
| `storage_id` | `int4` | Cluster shared-storage backend id. |
| `node_count` | `int4` | Number of nodes proven in the manifest. |
| `thread_count` | `int4` | Number of WAL threads proven in the manifest. |
| `manifest_crc` | `int8` | CRC32C of the manifest image. |

### `pg_cluster_restore_points`

Shows restore points created by the cluster-aware restore-point entry
point.

| Column | Type | Description |
|---|---|---|
| `restore_point_name` | `text` | Restore point name. |
| `cut_scn` | `int8` | SCN selected for the restore point cut. |
| `thread_count` | `int4` | WAL threads covered by the cut. |
| `incarnation` | `int4` | Cluster incarnation recorded with the cut. |
| `created_at` | `timestamptz` | Local timestamp when the point was recorded. |

### `pg_cluster_pitr_status`

Resolves the configured cluster PITR target against known restore
points and the latest manifest.

| Column | Type | Description |
|---|---|---|
| `target_type` | `text` | `latest` when no target is configured, `scn`, `name`, or `cluster_time`. |
| `target_action` | `text` | Configured PITR action: `pause`, `promote`, or `shutdown`. |
| `reachable` | `bool` | True if the configured target is reachable. |
| `reason` | `text` | `ok` or the fail-closed reason. |
| `resolved_scn` | `int8` | Restore-point SCN selected for the target, when reachable. |
| `restore_point_name` | `text` | Restore point used for the target, when reachable. |

Mutating function execution is revoked from PUBLIC:

```sql
SELECT * FROM pg_cluster_backup_start('b1', true);
SELECT * FROM pg_cluster_backup_stop(true);
SELECT * FROM pg_cluster_create_restore_point('rp1');
```

## pg_cluster_state

Diagnostic view over `cluster_dump_state()`.  It returns `(category,
key, value)` text rows and is intended for TAP smoke tests, operator
triage, and perf-gate snapshots.  The view never uses NULL values; an
unknown or inactive counter is reported as `0`, `off`, or another
explicit string.

Spec-6.2 adds the Smart Fusion terminal-authority surface:

| Category | Key | Meaning |
|---|---|---|
| `guc` | `cluster.cf_terminal_authority` | Live GUC value; `t` means terminal authority is enabled. |
| `guc` | `cluster.cf_delayed_cleanout` | Terminal-authority cleanout policy: `off`, `reader`, or `eager`. |
| `guc` | `cluster.smart_fusion` | Live GUC value; `t` means Smart Fusion early-transfer dependency tracking is enabled. |
| `guc` | `cluster.smart_fusion_tier_min` | Minimum negotiated interconnect tier required for v2 Smart Fusion block replies. |
| `guc` | `cluster.smart_fusion_commit_brake_timeout_ms` | Pre-commit dependency brake timeout. |
| `guc` | `cluster.smart_fusion_origin_durable_gossip_ms` | Durable-LSN gossip interval for dependency release. |
| `undo` | `terminal_authority_check_count` | Total authority decisions evaluated. |
| `undo` | `terminal_authority_ok_count` | Decisions that had complete authority evidence. |
| `undo` | `terminal_authority_failclosed_count` | Decisions rejected fail-closed for any reason. |
| `undo` | `terminal_authority_epoch_failclosed_count` | Missing or changed membership epoch evidence. |
| `undo` | `terminal_authority_ownership_failclosed_count` | Invalid origin/xid or origin ownership mismatch. |
| `undo` | `terminal_authority_unknown_failclosed_count` | Disabled authority, missing terminal state, or missing commit SCN. |
| `undo` | `terminal_authority_nonterminal_failclosed_count` | In-progress/non-terminal remote outcome. |
| `undo` | `terminal_authority_durable_failclosed_count` | Missing or mismatched durable-TT commit proof. |
| `undo` | `terminal_authority_retention_failclosed_count` | Retention proof was required but unavailable. |
| `smart_fusion` | `dep_install_count` | Early-transfer dependency vectors installed for received block images. |
| `smart_fusion` | `dep_touch_count` | Transaction-local touches of buffers with pending Smart Fusion dependencies. |
| `smart_fusion` | `dbwr_brake_count` | Buffer writeback attempts blocked because origin redo durability is unproven. |
| `smart_fusion` | `commit_brake_count` | Transactions that entered the pre-commit dependency brake. |
| `smart_fusion` | `commit_brake_wait_us` | Total microseconds spent waiting in the pre-commit dependency brake. |
| `smart_fusion` | `origin_suspect_count` | Pending dependencies associated with an origin suspected dead or unavailable. |
| `smart_fusion` | `dep_lost_failclosed_count` | Missing or malformed dependency evidence rejected fail-closed. |
| `smart_fusion` | `retry_failclosed_count` | Retryable Smart Fusion fail-closed outcomes, primarily commit-brake timeout. |

Example:

```sql
SELECT key, value
  FROM pg_cluster_state
 WHERE category = 'undo'
   AND key LIKE 'terminal_authority_%'
 ORDER BY key;

SELECT key, value
  FROM pg_cluster_state
 WHERE category = 'smart_fusion'
 ORDER BY key;
```

## pg_cluster_nodes

Returns one row per node declared in `pgrac.conf`.  In single-node
fallback mode (no `pgrac.conf`) returns one row for the local node.

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `node_id` | integer | NO | Numeric node id `[0, 127]` |
| `hostname` | text | YES | Short hostname (NULL when not declared in pgrac.conf) |
| `interconnect_addr` | text | NO | `"host:port"` of the node's interconnect endpoint.  Empty string in single-node fallback. |
| `public_addr` | text | YES | Client-facing `"host:port"` (NULL when not declared) |
| `role` | text | NO | `primary` / `standby` / `arbiter` |
| `region` | text | YES | Free-form region tag (NULL when not declared) |
| `is_self` | boolean | NO | True if `node_id` matches the local `cluster.node_id` GUC |

### Example queries

```sql
-- All nodes:
SELECT * FROM pg_cluster_nodes ORDER BY node_id;

-- Just this node's row:
SELECT * FROM pg_cluster_nodes WHERE is_self;

-- Counts by role:
SELECT role, count(*) FROM pg_cluster_nodes GROUP BY role;
```

### Sample output (3-node cluster)

```
 node_id |  hostname    | interconnect_addr |    public_addr     |  role   |  region    | is_self
---------+--------------+-------------------+--------------------+---------+------------+---------
       0 | db-1         | 10.0.0.1:6432     | 192.168.1.1:5432   | primary | us-east-1a | t
       1 | db-2         | 10.0.0.2:6432     |                    | standby | us-east-1b | f
       2 | db-3         | 10.0.0.3:6432     |                    | standby | us-east-1c | f
```

## pg_stat_cluster_wait_events

Lists the cluster-specific wait event registry on the local node.
Always returns 116 rows in `--enable-cluster` builds (one per
registered cluster wait event).

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `type` | text | NO | Wait event class.  Always begins with `Cluster: ` (e.g. `Cluster: GES`, `Cluster: PCM`). |
| `name` | text | NO | Wait event name (e.g. `GesEnqueueAcquire`, `PcmBlockReadNS`). |

### Example queries

```sql
-- Total registered events:
SELECT count(*) FROM pg_stat_cluster_wait_events;

-- Distinct classes:
SELECT DISTINCT type FROM pg_stat_cluster_wait_events ORDER BY type;

-- Per-class counts:
SELECT type, count(*) FROM pg_stat_cluster_wait_events GROUP BY type ORDER BY type;
```

See [Wait events](wait-events.md) for the full event roster.

## pg_stat_gcluster_wait_events

Cross-node placeholder for cluster-wide wait events.  In the
current release returns 116 rows for the local node only;
`node_id` is always the value of the local `cluster.node_id` GUC.

The column shape `(node_id, type, name)` is the public contract
and will not change when full multi-node fan-out is added in a
future release.

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `node_id` | integer | NO | Node that observed this wait event.  Currently always equal to local `cluster.node_id`. |
| `type` | text | NO | Same semantics as `pg_stat_cluster_wait_events.type`. |
| `name` | text | NO | Same semantics as `pg_stat_cluster_wait_events.name`. |

### Example queries

```sql
-- Distinct nodes seen in the global view (currently always 1):
SELECT count(DISTINCT node_id) FROM pg_stat_gcluster_wait_events;

-- The (type, name) projection equals pg_stat_cluster_wait_events
-- exactly while the global view contains only local data:
SELECT count(*) FROM (
    SELECT type, name FROM pg_stat_gcluster_wait_events
    EXCEPT
    SELECT type, name FROM pg_stat_cluster_wait_events
) d;
-- 0
```

## pg_cluster_ic_peers

Per-peer state of the Tier 1 (TCP) interconnect.  Returns one row for
every peer declared in `pgrac.conf`, regardless of whether the peer is
currently reachable.  Always returns zero rows when
`cluster.interconnect_tier` is not `tier1`.

### Columns

| Column | Type | Notes |
|---|---|---|
| `node_id` | `int4` | Peer's `cluster.node_id`. |
| `state` | `text` | One of `down`, `connecting`, `connected`, `rejected`. |
| `interconnect_addr` | `text` | `host:port` learned from `pgrac.conf`. |
| `last_connect_at` | `timestamptz` | Most recent transition into `connected`.  `NULL` if never connected. |
| `last_send_at` | `timestamptz` | Most recent successful socket send (any frame). |
| `last_recv_at` | `timestamptz` | Most recent successful socket recv (any frame). |
| `last_heartbeat_sent_at` | `timestamptz` | Most recent heartbeat emitted to this peer. |
| `last_heartbeat_recv_at` | `timestamptz` | Most recent heartbeat received from this peer. |
| `heartbeat_send_count` | `int8` | Cumulative heartbeats sent. |
| `heartbeat_recv_count` | `int8` | Cumulative heartbeats received. |
| `msg_send_count` | `int8` | Reserved for future use. |
| `msg_recv_count` | `int8` | Reserved for future use. |
| `bytes_send` | `int8` | Cumulative bytes written to the peer socket. |
| `bytes_recv` | `int8` | Cumulative bytes read from the peer socket. |
| `reconnect_count` | `int4` | Times this peer has been re-established after a drop. |
| `connect_error_count` | `int4` | `connect(2)` failures. |
| `last_errno` | `int4` | Last `errno` recorded; `0` if no error or never failed. |
| `last_error_code` | `text` | Last SQLSTATE-style code recorded (e.g. `08001`, `08006`, `08P01`).  Empty when no error. |
| `last_error` | `text` | Free-form description of the last error. |
| `stale_epoch_drop_count` | `int8` | Cumulative envelopes dropped because the carried membership epoch did not match the local current epoch.  Always zero outside of reconfig windows. |
| `chunk_reassembly_active` | `int4` | Number of large-payload chunks the cluster is currently waiting on from this peer.  Zero when no chunked send is in flight. |
| `chunk_reassembly_timeout_count` | `int8` | Cumulative chunked-payload reassemblies that exceeded the configured timeout and were aborted (peer reconnected). |
| `lamport_observe_advance_count` | `int8` | Cumulative envelopes whose carried SCN advanced the local SCN clock (Lamport piggyback).  Zero before high-frequency SCN traffic begins. |

### Example queries

```sql
-- Are all declared peers connected?
SELECT node_id, state, interconnect_addr,
       heartbeat_recv_count, last_heartbeat_recv_at
  FROM pg_cluster_ic_peers
 ORDER BY node_id;
```

```sql
-- Peers that have ever flapped.
SELECT node_id, state, reconnect_count, connect_error_count,
       last_error_code, last_error
  FROM pg_cluster_ic_peers
 WHERE reconnect_count > 0 OR connect_error_count > 0;
```

### Sample output (2-node cluster, both connected)

```text
 node_id |  state    | interconnect_addr | heartbeat_send_count | heartbeat_recv_count
---------+-----------+-------------------+----------------------+----------------------
       0 | connected | 10.0.0.1:6432     |                  342 |                  341
       1 | connected | 10.0.0.2:6432     |                  342 |                  342
```

## pg_cluster_ic_msg_types

Catalog of every interconnect message type the running postmaster
knows how to send or receive.  The list is fixed for the lifetime of
the process: every message type is registered once during postmaster
startup (phase 1, before the first backend forks) and never changes
afterward.  Diagnostic / observability only.

The view is non-empty even on single-node builds and even when
`cluster.interconnect_tier` is not `tier1`, because registration is
independent of the active transport.

| Column | Type | Description |
|---|---|---|
| `msg_type` | `int4` | Stable wire-protocol identifier (1 byte on the wire, but exposed as `int4` for SQL ergonomics).  Currently registered: `1` = `heartbeat`. |
| `name` | `text` | Symbolic short name. |
| `allowed_producer_mask` | `int8` | Bitmask of backend types permitted to emit this message.  Treated as opaque diagnostic — non-zero means the message has at least one declared producer. |
| `broadcast_ok` | `bool` | `t` if the message may be sent with `dest_node_id = -1` (broadcast).  Heartbeats are point-to-point so this is `f` for `heartbeat`. |
| `handler_present` | `bool` | `t` if the receiving side has a registered handler (i.e. the message is dispatched to processing logic on receipt) — `f` for send-only message types. |

Example:

```
SELECT * FROM pg_cluster_ic_msg_types ORDER BY msg_type;

 msg_type |   name    | allowed_producer_mask | broadcast_ok | handler_present
----------+-----------+-----------------------+--------------+-----------------
        1 | heartbeat |               1048576 | f            | t
```

The wire format of every message is the 36-byte cluster
interconnect envelope (4-byte aligned little-endian fixed header,
followed by a per-message-type payload).  The envelope is the same
for every message type and is independent of the active interconnect
tier.

## pg_stat_cluster_ic

Per-peer TCP/RDMA mux observability for `tier2` and `tier3`.  The view
is also populated in single-node and TCP-fallback modes so operators can
see why RDMA was not selected.

| Column | Type | Description |
|---|---|---|
| `node_id` | `int4` | Declared peer id from `pgrac.conf`. |
| `transport` | `text` | Current mux transport for the peer: `tcp` or `rdma`. |
| `rdma_state` | `text` | RDMA bring-up state: `disabled`, `fallback_tcp`, `connecting`, `connected`, or `error`. |
| `provider` | `text` | Selected provider (`auto`, `verbs-generic`, or `mlx5-direct`). |
| `rdma_addr` | `text` | Optional RDMA CM address from `pgrac.conf`. |
| `rdma_gid` | `text` | Optional GID label from `pgrac.conf`. |
| `rdma_port` | `int4` | HCA port number, default `1`. |
| `mr_registered` | `bool` | Whether RDMA memory registration resources completed in this postmaster. |
| `cq_depth` | `int4` | Last completion batch depth observed by LMON. |
| `fallback_count` | `int8` | Per-peer RDMA-to-TCP fallback decisions. |
| `send_count` / `recv_count` | `int8` | Mux-level send/receive handoffs counted by transport. |
| `bytes_send` / `bytes_recv` | `int8` | Mux-level bytes handed to/from the selected transport. |
| `block_sge_send_count` | `int8` | SEND-with-SGE block ship attempts posted on the RDMA path.  Spec-6.1 block replies use registered per-peer scratch MR, not live shared_buffers DMA. |
| `block_sge_fallback_count` | `int8` | SEND-with-SGE block ship attempts that materialized the SGEs and used TCP fallback. |
| `latency_us_sum` / `latency_sample_count` | `int8` | Reserved latency aggregation counters for tier2/tier3 completion timing. |
| `last_error_code` | `text` | Last SQLSTATE-style RDMA/mux error for this peer. |
| `last_error` | `text` | Last human-readable RDMA/mux error. |

Example:

```sql
SELECT node_id, transport, rdma_state, provider, fallback_count, last_error
  FROM pg_stat_cluster_ic
 ORDER BY node_id;
```

## pg_cluster_quorum_state

> **EXPERIMENTAL.** The voting-disk + quorum-lite feature is not
> production-ready in this release.  The view is queryable and the
> catalog surface is stable, but the background coordinator is not yet
> driven by postmaster startup.  `in_quorum` reports the fail-closed
> default until the coordinator integration ships.

Single-row view exposing the cluster's current quorum decision.  Backed
by `cluster_get_quorum_state()`.

| Column | Type | Description |
|---|---|---|
| `in_quorum` | `bool` | `t` only when the coordinator's quorum view is `OK` and the lease window has not expired (the lease window is `2 × cluster.quorum_poll_interval_ms`).  Backends gate `COMMIT` of writable transactions on this column. |
| `quorum_size` | `int4` | Majority threshold for the configured disk count: `(disks_total / 2) + 1`. |
| `disks_ok` | `int4` | Voting disks that responded successfully on the last poll. |
| `disks_total` | `int4` | Total voting disks configured in `cluster.voting_disks`. |
| `current_epoch_at_boot` | `int8` | Cluster epoch observed at coordinator startup (`max + 1` of any prior surviving epoch). |
| `last_quorum_loss_at` | `timestamptz` | Wall-clock timestamp of the most recent quorum-loss transition; `NULL` if no loss has occurred since the coordinator started. |
| `collision_state` | `text` | `none` / `detected_other` / `fatal_newer_self` per the cross-instance node-id collision detector.  `(uninitialised)` is returned before the coordinator's first poll. |

Example:

```sql
SELECT * FROM pg_cluster_quorum_state;
 in_quorum | quorum_size | disks_ok | disks_total | current_epoch_at_boot | last_quorum_loss_at | collision_state
-----------+-------------+----------+-------------+-----------------------+---------------------+-----------------
 f         |             |          |             |                     0 |                     | (uninitialised)
```

## pg_cluster_voting_disks

One row per voting disk configured in `cluster.voting_disks`.  Backed
by `cluster_get_voting_disks()`.

| Column | Type | Description |
|---|---|---|
| `path` | `text` | Filesystem path of the voting disk (verbatim from `cluster.voting_disks`, trimmed). |
| `state` | `text` | Per-disk health: `ok` / `degraded` / `unreachable`.  `unknown` is reported until the coordinator has completed at least one poll. |
| `last_read_at` | `timestamptz` | Wall-clock timestamp of the most recent successful read from this disk; `NULL` until the first read completes. |
| `last_write_at` | `timestamptz` | Wall-clock timestamp of the most recent successful write to this disk; `NULL` until the first write completes. |
| `read_count` | `int8` | Total successful reads since coordinator startup. |
| `write_count` | `int8` | Total successful writes since coordinator startup. |
| `io_error_count` | `int8` | Total I/O failures (timeout / EIO / CRC mismatch / partial read). |

Example:

```sql
SELECT path, state, read_count, io_error_count FROM pg_cluster_voting_disks;
        path        |  state  | read_count | io_error_count
--------------------+---------+------------+----------------
 /srv/voting/disk1  | unknown |          0 |              0
 /srv/voting/disk2  | unknown |          0 |              0
 /srv/voting/disk3  | unknown |          0 |              0
```

## --disable-cluster builds

In binaries built with `--disable-cluster`:

- All five views still exist in the catalog.
- All five return zero rows.
- The underlying `cluster_get_*` SRFs are present as no-op symbols.

This means SQL written against these views works on both build
modes; the cluster-specific data is simply absent on a vanilla
build.

## Reporting issues

File issues at <https://github.com/sqlrush/linkdb/issues>.
