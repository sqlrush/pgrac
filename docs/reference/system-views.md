# System views

linkdb adds three cluster-aware system views to the standard
PostgreSQL catalog.  All three are present in `--enable-cluster`
builds; in `--disable-cluster` builds they return zero rows.

| View | Purpose |
|---|---|
| `pg_cluster_nodes` | Cluster topology (the parsed `pgrac.conf`) |
| `pg_stat_cluster_wait_events` | Cluster-specific wait events on the local node |
| `pg_stat_gcluster_wait_events` | Cluster wait events globally (cross-node placeholder) |

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
Always returns 46 rows in `--enable-cluster` builds (one per
registered cluster wait event).

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `type` | text | NO | Wait event class.  Always begins with `Cluster: ` (e.g. `Cluster: GES`, `Cluster: PCM`). |
| `name` | text | NO | Wait event name (e.g. `GesEnqueueAcquire`, `PcmBlockReadNS`). |

### Example queries

```sql
-- Total registered events (must be 46):
SELECT count(*) FROM pg_stat_cluster_wait_events;

-- Distinct classes (must be 10):
SELECT DISTINCT type FROM pg_stat_cluster_wait_events ORDER BY type;

-- Per-class counts:
SELECT type, count(*) FROM pg_stat_cluster_wait_events GROUP BY type ORDER BY type;
```

See [Wait events](wait-events.md) for the full event roster.

## pg_stat_gcluster_wait_events

Cross-node placeholder for cluster-wide wait events.  In the
current release returns 46 rows for the local node only;
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
