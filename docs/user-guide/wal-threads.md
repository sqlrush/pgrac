# Per-thread WAL layout

In a cluster, every node writes its own WAL stream ("WAL thread").
Each stream lives in its own directory under one shared root so that
any node can read any stream:

```
<wal_threads_dir>/
├── thread_1/            # node 0's stream (thread id = node_id + 1)
│   ├── 000000010000000000000001
│   ├── ...
│   └── pgrac_thread.claim
└── thread_2/            # node 1's stream
    └── ...
```

Inside each thread directory the files are standard PostgreSQL WAL
segments; `pg_waldump` and all other WAL tooling work unchanged.  Every
WAL page is stamped with the writing node's thread id, shown by
`pg_waldump` as `thread: N` on each record (`thread: 0` means the page
was written outside cluster mode, e.g. by `initdb` or `pg_resetwal`;
mixed pages are normal).

## Setting it up

Pass `--wal-threads-dir` to `pgrac-init` when creating the node:

```bash
pgrac-init -D /data/node0 --node-id=0 --wal-threads-dir=/shared/walroot
pgrac-init -D /data/node1 --node-id=1 --wal-threads-dir=/shared/walroot
```

This relocates `pg_wal` via the standard `initdb -X` mechanism (a
symlink from `$PGDATA/pg_wal` to `<dir>/thread_<id>`) and sets
`cluster.wal_threads_dir` in `postgresql.conf`.  The root must be an
absolute path on storage that all nodes can reach.

An already-initialised data directory cannot be relocated by
`pgrac-init`; stop the server, move the contents of `pg_wal` into the
thread directory, replace `pg_wal` with a symlink to it, and set the
GUC.

## Startup validation

When `cluster.wal_threads_dir` is set, the server refuses to start
(fail-closed) unless the routing is coherent:

| Error | Meaning | Fix |
|---|---|---|
| SQLSTATE `53RA0` | `pg_wal` does not resolve to this node's thread directory, the directory is missing, or the configuration is contradictory (`cluster.enabled=off`, or `cluster.node_id` unset) | Re-link `pg_wal`, create the node with `pgrac-init --wal-threads-dir`, or fix the GUCs |
| SQLSTATE `53RA1` | The thread directory is claimed by a different node, or the claim file is corrupt / cannot be created | Each thread directory belongs to exactly one node.  After confirming ownership, remove `pgrac_thread.claim` and restart; it is never rebuilt automatically |

The claim file is written once on the first successful start and only
ever read back afterwards.

## Observability

`pg_cluster_state` exposes a `wal_thread` category:

| key | meaning |
|---|---|
| `thread_id` | this node's thread id (0 when not clustered) |
| `dir_configured` | `cluster.wal_threads_dir` is set |
| `dir_validated` | startup validation passed |
| `claim_created` | this boot created the claim file (first boot only) |
| `page_stamp_count` | WAL pages stamped with the real thread id since startup |
| `registry_ready` | the shared WAL-state registry file is present and valid |
| `registry_slot_state` | this node's registry slot: `active` / `stopped` / `empty` / `unknown` |
| `registry_last_updated` | liveness stamp of this node's slot (refreshed periodically) |
| `registry_highest_lsn` | WAL write position recorded at the last refresh |
| `registry_highest_scn` | cluster SCN recorded at the last refresh |

When `cluster.wal_threads_dir` is set, the cluster also keeps a small
registry file `<dir>/pgrac_wal_state` recording, for every node, whether
its WAL stream is in use (`active`), was shut down cleanly (`stopped`),
and how far it had written.  A node marks itself `active` only once it
has finished recovery and is about to serve; a crashed node simply stops
refreshing its slot.  The file is created automatically on first start
and never needs manual editing; if it reports as corrupt, has the wrong
size (it is a fixed 66048 bytes and is never resized in place), or this
node's slot turns out to be owned by a different node id (all SQLSTATE
`53RA2`), startup is refused and the file is left untouched.  A
foreign-owned slot usually means `cluster.wal_threads_dir` points at the
wrong shared directory or two nodes share the same `cluster.node_id` —
fix the configuration.  For corruption or a wrong-sized file, confirm
the shared storage is healthy, remove the file, and restart — it is
rebuilt empty and repopulates as nodes start.

Four wait events cover the shared-storage bookkeeping I/O:
`ClusterWalThreadClaimRead`/`Write` (claim file) and
`ClusterWalStateRead`/`Write` (registry).

## Recovery plan (informational)

On every plain local startup (clean or crash; not as a standby and not
during archive recovery), a node with `cluster.wal_threads_dir` set
scans the registry and reports, per WAL thread, whether it looks
cleanly stopped, live on another node, a *crash candidate* (still
marked active but not refreshed within
`cluster.recovery_stale_active_ms`, default 10s), or unreadable.  The
result appears in the startup log ("recovery plan (not acted upon)")
and under the `recovery` category of `pg_cluster_state` (`plan_*`
keys).  When crash candidates are found, up to
`cluster.recovery_workers_max` (default 4, 0 disables) background
workers additionally validate each candidate stream — the ownership
claim file and the last written WAL page located from the registry
watermark — and report per-thread results under the same category
(`stream_ok_threads`, `stream_suspect_or_unreadable_threads`).  The
workers never delay startup and never read a live node's stream.
When `cluster.merged_recovery` is on and a crash candidate is found,
the node currently refuses to start with a clear error: cross-node
merged recovery needs a shared data-file storage backend, which this
release does not provide (the cluster filesystem is per-node).
Recover such a node with `cluster.merged_recovery = off` (its own WAL
stream); merged recovery across nodes is a later milestone.

This release only reports the classification — cross-thread
merged recovery is not performed.  An `unknown` thread is never treated
as crashed; if unknowns persist, check the shared WAL storage.  If
peer nodes run a larger `cluster.cluster_stats_main_loop_interval`,
raise `cluster.recovery_stale_active_ms` accordingly so live peers are
not reported as crash candidates.

## Compatibility notes

- Setting `cluster.wal_threads_dir` is optional.  Without it the flat
  `pg_wal` layout is unchanged; WAL pages are still stamped with the
  node's thread id whenever `cluster.enabled` is on and
  `cluster.node_id` is set.
- **Once a clustered node has written thread-stamped WAL, older pgrac
  binaries (pre per-thread WAL) refuse to read that WAL and fail
  closed at recovery.  Downgrading the binary after enabling cluster
  mode is not supported.**  Vanilla PostgreSQL binaries cannot read
  pgrac data directories in any case.
- Physical streaming replication is unaffected: a standby replays the
  primary's stamped stream regardless of its own `cluster.node_id`.
- `pg_basebackup` from a relocated node produces a backup with a plain
  (empty) `pg_wal` directory, as with any symlinked `pg_wal`.
  **The backup also copies the primary's `postgresql.conf`, including
  its `cluster.wal_threads_dir` — before starting a standby from such
  a backup, set `cluster.wal_threads_dir = ''` (or relocate the
  standby's `pg_wal` into its own thread directory).**  Left as
  copied, the startup validator refuses to start (SQLSTATE `53RA0`):
  the backup's plain `pg_wal` does not resolve to the configured
  thread directory.
- `pg_resetwal` writes pages without a thread stamp (`thread: 0`); the
  server resumes stamping on the next start.  Mixed segments are
  valid.
