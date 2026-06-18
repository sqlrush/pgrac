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

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `cluster.ges_mode_selfcheck` | `fatal` | Severity when the matrix diverges from the server lock conflict table at startup: `fatal` refuses to start, `warn` logs and continues, `off` skips the check. Requires restart. |
