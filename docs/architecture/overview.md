# Architecture overview

linkdb extends PostgreSQL 16.13 with a cluster subsystem that
targets shared-disk multi-node deployment patterns (akin to Oracle
Real Application Clusters).  This document gives a 30,000-foot
view of the components present today and the boundary between
cluster code and stock PostgreSQL.

## High-level component map

```
+-----------------------------------------------------------+
|  PostgreSQL 16.13 backend (parser / planner / executor)   |
|                          ...                              |
+-----------------------------------------------------------+
|  PostgreSQL infrastructure (shmem / WAL / lock / buffer)  |
+-----------+----------+----------+--------------+----------+
            |          |          |              |
            v          v          v              v
+-----------+ +--------+ +--------+ +------------+
| cluster   | | cluster| | cluster| | cluster    |
| _guc      | | _shmem | | _ic    | | _conf      |
| (GUCs)    | |(shared | |(IPC    | |(topology   |
|           | | memory)| | API)   | | from       |
|           | |        | |        | | pgrac.conf)|
+-----------+ +--------+ +--------+ +------------+
            |          |          |              |
            +----------+----------+--------------+
                          |
                          v
            +-----------------------------+
            | cluster_views (SQL surface) |
            |  pg_cluster_nodes           |
            |  pg_stat_cluster_wait_events|
            |  pg_stat_gcluster_wait_events|
            +-----------------------------+
```

## Modules at a glance

| Module | Source | Purpose |
|---|---|---|
| `cluster` | `src/backend/cluster/cluster.c` | Top-level entry point (init / shutdown stubs) |
| `cluster_guc` | `src/backend/cluster/cluster_guc.c` | Registers `cluster.*` GUCs (node_id / interconnect_tier / config_file) |
| `cluster_shmem` | `src/backend/cluster/cluster_shmem.c` | Cluster shared-memory control block + per-subsystem allocators |
| `cluster_ic` | `src/backend/cluster/cluster_ic.c` | Inter-node IPC abstraction (byte-stream + protocol layers; vtable per tier) |
| `cluster_conf` | `src/backend/cluster/cluster_conf.c` | Parses `pgrac.conf` into the topology shmem region |
| `cluster_signal` | `src/backend/cluster/cluster_signal.c` | Cluster ProcSignal handlers (extends PG SIGUSR1 multiplexer) |
| `cluster_views` | `src/backend/cluster/cluster_views.c` | Set-returning functions backing the three cluster system views |
| `cluster_elog` | `src/backend/cluster/cluster_elog.c` | `CLUSTER_LOG()` macro and the `cluster_phase` lifecycle marker |

## Postmaster startup flow

```
postmaster main
  |
  +- ProcessConfigFile (postgresql.conf)
  |     |
  |     +- cluster.* GUCs registered via cluster_init_guc
  |        (process_shared_preload_libraries phase)
  |
  +- CreateSharedMemoryAndSemaphores
        |
        +- cluster_init_shmem
              |
              +- cluster_ctl_shmem_init      (allocate cluster control block)
              +- cluster_conf_shmem_init     (allocate topology shmem)
              +- cluster_conf_load           (parse pgrac.conf or fall back single-node)
              +- cluster_ic_init             (bind interconnect vtable based on tier GUC)
```

## Build modes

| Build flag | Behavior |
|---|---|
| `--enable-cluster` | Cluster subsystem active.  All cluster GUCs / views / SRFs present.  `cluster_*.o` objects linked into `postgres`. |
| `--disable-cluster` (default) | Cluster subsystem absent.  Cluster GUCs not recognised.  Cluster system views return zero rows.  `postgres` binary is symbol-equivalent to upstream PostgreSQL 16.13 (no `cluster_init` / `cluster_shutdown` / `pgrac_version_string` symbols). |

The `--disable-cluster` mode is useful when running linkdb as a
drop-in PostgreSQL replacement without enabling cluster features —
e.g. on a standalone development machine.

## Interconnect tiers

The `cluster_ic` module defines four tier slots.  The first two are
populated in the current release.

| Tier | Status |
|---|---|
| `stub` | **Active** — same-node ops are no-ops, cross-node ops raise `ERRCODE_FEATURE_NOT_SUPPORTED` |
| `tier1` | **Active** — TCP transport carrying the LMON aux-process heartbeat between every pair of nodes declared in `pgrac.conf`.  Per-peer state is exposed via [`pg_cluster_ic_peers`](../reference/system-views.md#pg_cluster_ic_peers). |
| `tier2` | Not supported |
| `tier3` | Not supported |

Switching tiers is controlled via the `cluster.interconnect_tier`
GUC.  See [Configuration](../user-guide/configuration.md) for
runtime details.

## Topology

The cluster topology — list of nodes plus their addresses — is
declared in a single file (`pgrac.conf`) loaded at postmaster
startup.  The parsed result lives in shared memory so every backend
sees an identical view.  Updates require a restart in the current
release.

See [Configuration](../user-guide/configuration.md) for the
`pgrac.conf` format and [System views](../reference/system-views.md)
for the `pg_cluster_nodes` runtime projection.

## What's not in the picture yet

Several subsystems referenced in the wait event registry
(see [Wait events](../reference/wait-events.md)) are scaffolded
but not active in the current release.  Operations they would
serve return `ERRCODE_FEATURE_NOT_SUPPORTED` rather than blocking
on a wait.

| Subsystem | Status |
|---|---|
| GES (distributed lock manager) | Not active |
| PCM (parallel cache management) | Not active |
| Cache Fusion (cross-node block transfer) | Not active |
| SCN propagation | Not active |
| Reconfiguration / Heartbeat | Not active |
| Cluster-aware recovery | Not active |
| Cross-node sinval | Not active |
| Cross-node undo | Not active |
| Active Data Guard | Not active |

## Reporting issues

File issues at <https://github.com/sqlrush/linkdb/issues>.
