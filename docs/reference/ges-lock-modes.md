# GES lock modes and compatibility matrix

The Global Enqueue Service (GES) coordinates database-level locks across
cluster nodes. GES uses the eight standard PostgreSQL lock modes as its
enqueue mode space, so the compatibility rules match single-node
PostgreSQL exactly.

These modes are distinct from PCM block-lock states (`N`/`S`/`X`), which
coordinate buffer blocks rather than database objects.

## Lock modes

| Mode | Typical statement | Oracle DLM alias |
|---|---|---|
| `AccessShareLock` | `SELECT` | CR |
| `RowShareLock` | `SELECT ... FOR UPDATE` / `FOR SHARE` | CR |
| `RowExclusiveLock` | `INSERT`, `UPDATE`, `DELETE` | CW |
| `ShareUpdateExclusiveLock` | `VACUUM`, `ANALYZE`, `CREATE INDEX CONCURRENTLY` | CW |
| `ShareLock` | `CREATE INDEX` | PR |
| `ShareRowExclusiveLock` | `CREATE TRIGGER`, some `ALTER TABLE` | PW |
| `ExclusiveLock` | `REFRESH MATERIALIZED VIEW CONCURRENTLY` | PW |
| `AccessExclusiveLock` | `DROP TABLE`, `TRUNCATE`, `VACUUM FULL`, most `ALTER TABLE` | EX |

The Oracle DLM alias column is informational only. The mapping is
approximate: `ShareUpdateExclusiveLock` and `ExclusiveLock` have no exact
Oracle DLM equivalent, and several PostgreSQL modes share an alias. Aliases
are display-only and are not accepted as input to `cluster_ges_mode_compat()`.

## Compatibility matrix

`✓` = the two modes may be held at the same time (across nodes); `✗` = they
conflict and one waits.

| held \ wanted | AS | RS | RE | SUE | S | SRE | E | AE |
|---|---|---|---|---|---|---|---|---|
| AccessShareLock | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ |
| RowShareLock | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | ✗ |
| RowExclusiveLock | ✓ | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ | ✗ |
| ShareUpdateExclusiveLock | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ |
| ShareLock | ✓ | ✓ | ✗ | ✗ | ✓ | ✗ | ✗ | ✗ |
| ShareRowExclusiveLock | ✓ | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| ExclusiveLock | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| AccessExclusiveLock | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |

The matrix is symmetric.

## Querying the matrix

`pg_cluster_ges_mode_matrix()` returns all 64 cells:

```sql
SELECT held, wanted, compatible, held_dlm, wanted_dlm
FROM pg_cluster_ges_mode_matrix()
WHERE held = 'RowExclusiveLock';
```

`cluster_ges_mode_compat(held text, wanted text)` returns a single cell.
It accepts only the canonical mode names above (case-insensitive); an
unknown name or a DLM alias raises `ERRCODE_INVALID_PARAMETER_VALUE`:

```sql
SELECT cluster_ges_mode_compat('RowExclusiveLock', 'ShareLock');  -- f
SELECT cluster_ges_mode_compat('accesssharelock', 'ShareLock');   -- t
SELECT cluster_ges_mode_compat('CR', 'EX');                       -- ERROR
```

`cluster_ges_mode_matches_pg()` returns `true` when the matrix agrees with
the server's lock conflict table.

## Lock conversion

Cross-node *lock conversion* — changing the mode of an enqueue lock that is
already held on a GES-managed resource — is **currently not supported**. A
cross-node conversion request is rejected with
`ERRCODE_FEATURE_NOT_SUPPORTED` (`0A000`) rather than being silently ignored.
Ordinary lock acquisition, waiting, and release across nodes are unaffected.

The `nconverts` column of `pg_cluster_grd_entries` reports the number of
pending conversion requests queued on a resource; it is `0` in normal
operation. The diagnostic view snapshots entry keys by walking shard-local
entry lists under each shard lock and then re-looks up each entry through the
normal pin/release API before taking the per-entry snapshot, so cold reclaim
can safely remove holderless entries while diagnostics are active.

## Blocking notifications

When a request on one node conflicts with a lock held on another node, the
holder is sent a *blocking notification*. This notification is **advisory**:
the holder is informed that it is blocking another node, but it is **not**
forced to release early — an enqueue lock is held until the holder's
transaction ends, at which point the lock is released naturally and the
waiting request proceeds. A holder never gives up a lock mid-transaction in
response to a blocking notification.

Blocking-notification activity is observable in the cluster state dump
(`pg_cluster_state`, category `grd`):

| Key | Meaning |
|---|---|
| `grd_bast_sent_count` | Blocking notifications sent to holders |
| `grd_bast_received_count` | Blocking notifications received by this node's holders |
| `grd_bast_ack_count` | Notifications acknowledged via the holder's natural release |
| `grd_bast_stale_drop_count` | Notifications dropped because the target backend had already exited |

These counters are `0` on a single node (no cross-node contention occurs).

## Startup self-check

At startup the server verifies that its frozen GES compatibility matrix
matches its lock conflict table. This check is automatic and mandatory: if
the two ever diverge (which would indicate a build inconsistency) the server
refuses to start. There is no configuration parameter for this check.
