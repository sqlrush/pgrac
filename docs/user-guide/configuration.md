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
