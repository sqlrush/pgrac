# Cross-node transaction safety

PGRAC refuses a statement when it cannot prove the cluster-wide transaction
state or row-version relationship required for a safe visibility, update,
delete, or row-lock decision. The server does not guess that an unresolved
transaction committed, aborted, or stopped conflicting.

This page describes the user-visible safety and troubleshooting contract. It
does not describe internal storage formats or coordination protocols.

## Safety contract

- A cross-node result is accepted only when the installed build can prove the
  required transaction identity and state.
- Before modifying a row, the server confirms that the row version and its
  cluster ownership have not changed while remote information was obtained.
- Missing, stale, conflicting, malformed, or changing evidence fails closed
  before the statement may use it as a successful result.
- Read evidence does not grant write permission. A modification still requires
  the cluster's exclusive current-block ownership.
- Background cleanup must not change an uncertain result into success merely
  to free capacity or reduce latency.

## Operator-visible failures

Depending on the operation and installed release, an unproved transaction or
MultiXact result can surface as SQLSTATE `53R97` or `53R9C`. These errors mean
that the statement was refused. By themselves, they do not prove committed-data
loss or on-disk corruption.

There is no supported GUC, timeout increase, SQL procedure, or manual cleanup
that makes an unproved result safe.

## Safe response

1. Preserve the complete client error, including SQLSTATE, detail, hint,
   context, backend PID, node, and timestamp.
2. Roll back the failed transaction. Do not retry one statement inside an
   already-aborted transaction.
3. Confirm that every node is ready and uses the expected build and cluster
   configuration.
4. Collect server logs and wait information from every node for the same time
   window.
5. Retry the complete application transaction only under a bounded retry policy
   that is safe for the application.

Useful first checks include:

```sql
SELECT pid,
       application_name,
       state,
       wait_event_type,
       wait_event,
       xact_start,
       query_start
  FROM pg_stat_activity
 ORDER BY query_start NULLS LAST, pid;

SELECT *
  FROM pg_cluster_state
 ORDER BY category, key;
```

See [System views](system-views.md), [cluster wait events](wait-events.md), and
the [verification guide](../user-guide/verification.md) for the supported
diagnostic surfaces.

## Release and deployment boundary

Shared DROP physical completion requires `data_sync_retry = off` (the
default). With `data_sync_retry = on`, the committed SPACE completion phase
logs the unsupported setting and refuses before storage I/O. Physical cleanup
remains pending; this does not roll back an already committed transaction.
Changing `data_sync_retry` requires an instance restart.

For current four-node validation, install the same eligible build on every
node. Mixed-version rolling compatibility for this transaction path is not a
current validation claim; a protocol mismatch must remain fail closed rather
than be bypassed.

Documentation alone is not release certification. Use the release notes and
verification results shipped with the installed artifact to determine whether
a particular cross-node path is included in the validated release surface.
