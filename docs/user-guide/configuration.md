# Configuration reference

linkdb uses two configuration mechanisms layered on top of standard
PostgreSQL configuration:

1. **`postgresql.conf`** — standard PG config plus three new
   `cluster.*` GUCs added by linkdb's cluster subsystem.
2. **`pgrac.conf`** — INI-style file describing the cluster
   topology (the list of nodes that participate in the cluster).

## cluster.* GUCs

All three GUCs require server restart to change (PGC_POSTMASTER).

### `cluster.node_id`

| | |
|---|---|
| Type | integer |
| Default | -1 (unconfigured) |
| Range | -1 .. 127 |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

Numeric identifier of this node within the cluster.  Set per
node — every node in the same cluster has a distinct
`cluster.node_id`.  The default of -1 means "not part of a
cluster" and is the value used by single-node development /
test deployments.

```text
# postgresql.conf
cluster.node_id = 0
```

Verify:

```sql
SHOW "cluster.node_id";
-- 0
```

### `cluster.interconnect_tier`

| | |
|---|---|
| Type | enum |
| Allowed values | `stub` (default), `tier1`, `tier2`, `tier3` |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

Selects the cluster interconnect transport.

| Value | Behavior |
|---|---|
| `stub` (default) | Same-node operation is a no-op success.  Cross-node send raises `ERRCODE_FEATURE_NOT_SUPPORTED`.  No real wire traffic.  Suitable for single-node deployments and CI. |
| `tier1` | TCP transport for the LMON heartbeat path between cluster nodes.  Requires every peer (including self) to be listed in `pgrac.conf` with an `interconnect_addr`.  See `pg_cluster_ic_peers` for runtime peer state. |
| `tier2` | Currently not supported.  Setting this causes the postmaster to refuse to start with `ERRCODE_FEATURE_NOT_SUPPORTED`. |
| `tier3` | Same as `tier2`: not supported. |

```text
# postgresql.conf
cluster.interconnect_tier = stub
```

Verify:

```sql
SELECT name, vartype, context, setting
  FROM pg_settings
 WHERE name = 'cluster.interconnect_tier';
--          name           | vartype |  context   | setting
-- ------------------------+---------+------------+---------
--  cluster.interconnect_tier | enum    | postmaster | stub
```

### `cluster.interconnect_heartbeat_interval_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `1000` (1 s) |
| Range | `100` – `60000` |
| Context | postmaster |

How often the LMON aux process emits a heartbeat to each connected peer in `tier1` mode, and (because the LMON main loop uses one timer for both jobs) how often it retries a connection to a peer that is currently down.  Has no effect when `cluster.interconnect_tier = stub`.

### `cluster.interconnect_connect_timeout_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `5000` (5 s) |
| Range | `1000` – `60000` |
| Context | postmaster |

Reserved for future timeout enforcement on outbound `tier1` TCP `connect(2)` calls.  Currently informational; LMON re-attempts on each heartbeat tick.

### `cluster.interconnect_recv_timeout_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `30000` (30 s) |
| Range | `1000` – `600000` |
| Context | postmaster |

Reserved for future timeout enforcement on `tier1` peer `recv(2)`.  Currently informational; the connection is held open until the peer closes or the postmaster shuts down.

### `cluster.interconnect_payload_max_bytes`

| | |
|---|---|
| Type | integer (bytes) |
| Default | `67108864` (64 MB) |
| Range | `16777216` (16 MB) – `268435456` (256 MB) |
| Context | postmaster |

Upper bound on the payload size accepted by the chunked-send API.  A caller asking to send a larger payload is rejected outright with `ERRCODE_PROGRAM_LIMIT_EXCEEDED` rather than silently truncating.  Increase this when the workload expects larger cross-node messages; the hard cap is 256 MB.

### `cluster.interconnect_chunk_reassembly_timeout_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `10000` (10 s) |
| Range | `1000` – `60000` |
| Context | postmaster |

How long the receiver waits for the remaining chunks of a chunked payload before declaring the reassembly stuck and dropping the peer.  The peer is then reconnected on the next tick.

### `cluster.interconnect_tcp_keepidle_sec`

| | |
|---|---|
| Type | integer (seconds) |
| Default | `60` |
| Range | `30` – `600` |
| Context | postmaster |

Idle seconds before the kernel begins probing a quiet TCP connection (`TCP_KEEPIDLE` on Linux, `TCP_KEEPALIVE` on macOS).  The application-level heartbeat (~3 s deadline) is the primary liveness path; this kernel-level keepalive is a fallback for the rare case where the application stalls but the socket remains open.

### `cluster.interconnect_tcp_keepintvl_sec`

| | |
|---|---|
| Type | integer (seconds) |
| Default | `10` |
| Range | `10` – `60` |
| Context | postmaster |

Interval between successive kernel keepalive probes once probing has begun (`TCP_KEEPINTVL`).

### `cluster.interconnect_tcp_keepcnt`

| | |
|---|---|
| Type | integer |
| Default | `6` |
| Range | `3` – `20` |
| Context | postmaster |

Number of unacked keepalive probes before the kernel declares the connection dead (`TCP_KEEPCNT`).  With the defaults the kernel-level worst-case half-open detection is `60 + 6 × 10 = 120 s`, well within the application-level path.

### `cluster.shared_storage_backend`

| | |
|---|---|
| Type | enum |
| Allowed values | `stub` (default), `local` |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

Selects the backend that the cluster-aware storage manager
(`cluster_smgr`) dispatches into when `cluster.smgr_user_relations`
is enabled.

| Value | Behavior |
|---|---|
| `stub` (default) | The cluster_smgr vtable is wired but no I/O paths route through it.  Setting `cluster.smgr_user_relations = on` while this is `stub` causes the postmaster to refuse to start. |
| `local` | Routes cluster-aware relation I/O through the local filesystem under PGDATA.  Useful for single-host smoke testing and for the two-instance concurrent-open path that exercises every relation through the cluster_smgr stack. |

```text
# postgresql.conf
cluster.shared_storage_backend = local
```

### `cluster.smgr_user_relations`

| | |
|---|---|
| Type | bool |
| Default | `off` |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

When `on`, permanent (non-temporary) user relations are routed
through `cluster_smgr` instead of PostgreSQL's stock `md.c`
storage manager.  Two cluster instances may have a backend each
open the same relation at the same time; per-instance
`SMgrRelation` state and per-instance file handles are tracked
independently.

This GUC is **experimental**.  Two-instance concurrent open is
supported, but cross-instance cache invalidation across the
cluster and `md.c`-equivalent fsync registration are not yet
activated.  Enabling it triggers a postmaster startup `WARNING`
and is unsuitable for production workloads — stale-cache behavior
across cluster peers and crash-recovery durability are not
guaranteed.

```text
# postgresql.conf
cluster.shared_storage_backend = local
cluster.smgr_user_relations = on
```

Verify the GUC and the live SMgrRelation count via
`pg_cluster_state`:

```sql
SHOW "cluster.smgr_user_relations";
SELECT key, value
  FROM cluster_dump_state()
 WHERE category = 'shared_fs'
   AND key IN ('smgr_user_relations', 'smgr_active_relations');
```

The cross-instance invalidation hook firing rate is observable
through `pg_stat_cluster_counters`:

```sql
SELECT name, value
  FROM pg_stat_cluster_counters
 WHERE name = 'cluster.smgr.remote_invalidation_stub_call_count';
```

The counter advances on every relation extend, truncate, or
unlink while `cluster.enabled = on` and the GUC is `on`; it stays
at zero in `cluster.enabled = off` mode regardless of GUC.

### `cluster.config_file`

| | |
|---|---|
| Type | string |
| Default | `pgrac.conf` (resolved relative to PGDATA) |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

Path to the cluster topology file (`pgrac.conf` by default).
Use an absolute path to point at shared storage in multi-node
deployments.

```text
# postgresql.conf
cluster.config_file = '/srv/shared/pgrac.conf'
```

### `cluster.voting_disks`

> **EXPERIMENTAL.** The voting-disk + quorum-lite feature is not
> production-ready in this release.  The catalog surface (GUCs / views /
> wait events / counters / SQLSTATEs) is wired and visible, but the
> background coordinator process is not yet driven by postmaster
> startup; views report the fail-closed default state.  Use only for
> evaluation against the documented surface.

| | |
|---|---|
| Type | string (comma-separated path list) |
| Default | empty (disabled) |
| Context | postmaster |
| Boot setting | `postgresql.conf` |

Comma-separated list of voting-disk file paths.  Each disk is a
pre-allocated file on shared storage that holds one slot per cluster
instance; the cluster coordinator polls all disks every
`cluster.quorum_poll_interval_ms` and computes a majority view.

Empty disables the voting-disk path entirely.  In multi-node mode,
combining empty with `cluster.allow_single_node = off` triggers a
postmaster startup error to prevent silent fail-open.

```text
# postgresql.conf
cluster.voting_disks = '/srv/voting/disk1,/srv/voting/disk2,/srv/voting/disk3'
```

Three disks across distinct failure domains is the recommended
production configuration; one, five, and seven are also valid odd-only
sizes.  A startup heuristic emits a warning when the configured paths
share a common parent directory (likely co-located on one volume).

### `cluster.quorum_poll_interval_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `2000` (2 s) |
| Range | `500` – `30000` |
| Context | postmaster |

Coordinator main-loop tick interval.  The lease window backends use to
gate writes is `2 ×` this value: a coordinator stalled for more than
two ticks is treated as failed and the cluster fails closed without
waiting for an explicit signal.

### `cluster.voting_disk_io_timeout_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `5000` (5 s) |
| Range | `500` – `60000` |
| Context | postmaster |

Per-I/O deadline for voting-disk read / write / fsync.  An I/O that
exceeds this deadline counts as a disk failure and decrements the
healthy-disk count surfaced by `pg_cluster_quorum_state.disks_ok`.

### `cluster.voting_disk_size_bytes`

| | |
|---|---|
| Type | integer (bytes; multiple of 512) |
| Default | `65536` |
| Range | `4096` – `1048576` |
| Context | postmaster |

Size of each pre-allocated voting-disk file.  Each cluster instance
owns one 512-byte slot at offset `node_id × 512`; the default holds
slots for 128 instances.

### `cluster.self_fence_enabled`

> **EXPERIMENTAL.** Fence-lite path requires the voting-disk + quorum
> coordinator;treat together with the EXPERIMENTAL surface above.

| | |
|---|---|
| Type | bool |
| Default | `on` (production fail-safe) |
| Context | postmaster |

Whether the postmaster initiates an orderly fast-shutdown when the
cluster has lost quorum for longer than `cluster.self_fence_grace_ms`.
When `on` (default), the postmaster self-signals SIGINT after the
grace window, driving PG's standard fast-shutdown path; when `off`,
operators must stop the instance manually.

In-flight transaction abort on quorum loss is controlled separately
by `cluster.freeze_writes_enabled` — this GUC only affects the
postmaster shutdown decision, not the per-backend abort behaviour.

The dev / test escape is `cluster.allow_single_node = on`:  in single-
node mode the coordinator never enters the LOST quorum state, so
self-fence cannot trigger regardless of this GUC's value.

### `cluster.self_fence_grace_ms`

| | |
|---|---|
| Type | integer (milliseconds) |
| Default | `30000` (30 s) |
| Range | `1000` – `300000` |
| Context | postmaster |

Delay between the cluster's quorum-loss broadcast and the postmaster's
self-fence shutdown.  This delay applies **only** to the postmaster
shutdown decision — the in-flight transaction freeze broadcast happens
immediately when quorum is lost.

The default (30 s ≈ 7.5× the lease window) absorbs short network
flaps without triggering a self-shutdown.  Lower it for more
aggressive shutdown on flap;raise it to give operators more time to
intervene.

### `cluster.freeze_writes_enabled`

| | |
|---|---|
| Type | bool |
| Default | `on` (production fail-safe) |
| Context | postmaster |

Whether backends abort in-flight transactions on receipt of the cluster
quorum-loss broadcast (`PROCSIG_CLUSTER_FREEZE_WRITES`).  When `on`
(default), the next `CHECK_FOR_INTERRUPTS` after the signal raises
`ERRCODE_CLUSTER_QUORUM_LOST_BACKEND` (SQLSTATE `53R50`) and rolls
back the transaction.  When `off`, the signal is silently absorbed
and only the commit-boundary fail-closed gate (controlled by
`cluster.voting_disks` / quorum lease) prevents writes.

Off is for diagnosing fence-induced abort behaviour without losing
in-flight work — production should keep on.  This setting does **not**
bypass the commit-boundary gate either way.

### `cluster.fence_audit_log`

| | |
|---|---|
| Type | enum (`off` / `log` / `debug`) |
| Default | `log` |
| Context | postmaster |

Verbosity of fence-related entries in the postmaster log.

* `off` — silent;rely on `pg_cluster_fence_state` view + counters.
* `log` — one LOG line per broadcast (freeze / thaw / self-fence
  initiated).  Default;recommended for production.
* `debug` — adds DEBUG2 entries for each backend that received the
  freeze signal.  Verbose, dev / test only.

## pgrac.conf format

INI-style: section headers in `[brackets]` and `key = value`
lines.  Comments start with `#` or `;`.

### Example

```ini
# pgrac.conf
[cluster]
name = pgrac-prod-01

[node.0]
interconnect_addr = 10.0.0.1:6432
hostname          = db-1.internal
public_addr       = 192.168.1.1:5432
role              = primary
region            = us-east-1a

[node.1]
interconnect_addr = 10.0.0.2:6432
hostname          = db-2.internal
role              = standby
```

### Section types

| Section | Purpose |
|---|---|
| `[cluster]` | Cluster-wide diagnostic metadata.  Currently only `name` is recognised; unknown keys produce a `WARNING` and are ignored. |
| `[node.<N>]` | Per-node entry.  `<N>` is a decimal integer in `[0, 127]` (matches the `cluster.node_id` range).  Each node id appears at most once. |

### Recognised keys inside `[node.<N>]`

| Key | Required | Format | Notes |
|---|---|---|---|
| `interconnect_addr` | yes | `host:port` | Where peers contact this node.  `host` is non-empty (no DNS validation at parse time); `port` in `[1, 65535]`. |
| `hostname` | no | string | Free-form short hostname; appears in `pg_cluster_nodes.hostname`. |
| `public_addr` | no | `host:port` | Client-facing address (for load balancer use). |
| `role` | no | `primary` / `standby` / `arbiter` | Defaults to `primary` if absent. |
| `region` | no | string | Free-form region tag for diagnostic display. |

### Comment forms

```ini
[node.0]
# A full-line comment.
interconnect_addr = 10.0.0.1:6432  ; trailing semicolon comment also works
hostname = db-1.internal           # or '#' style
```

Anything from the first `#` or `;` on a line is treated as comment.

### Validation rules at startup

The postmaster parses `pgrac.conf` and FATALs (refuses to start)
on any of:

- malformed section header / `key = value` line (errcontext includes
  the offending line number)
- duplicate `[node.<N>]` for the same `<N>`
- node id outside `[0, 127]`
- `[node.<N>]` missing the required `interconnect_addr` key
- `interconnect_addr` not in `host:port` form
- `role` not one of `primary` / `standby` / `arbiter`
- `cluster.node_id` (the GUC) is not present in any `[node.<N>]` section

If `pgrac.conf` is missing entirely the server falls back to a
single-node topology — see [Bootstrap](bootstrap.md) for details.

### Maximum cluster size

`pgrac.conf` may declare up to **128 nodes** (`node_id` range
`[0, 127]`).  Larger clusters are not currently supported.

### Reload semantics

`cluster.config_file` is a `PGC_POSTMASTER` GUC; topology changes
require a server restart.  There is no online "reload pgrac.conf"
path in this version.

## Reporting issues

File issues at <https://github.com/sqlrush/linkdb/issues>.
