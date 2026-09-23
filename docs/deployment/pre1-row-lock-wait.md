# PRE1 candidate: waiting for a remote row lock

Author: SqlRush <sqlrush@gmail.com>

This describes the PRE1 development candidate, not the unmodified v0.131.0 image.
The candidate fixes propagation of the existing `-1` setting through ordinary
remote row waits. Four-VM acceptance and release CI must pass before it is a
qualified deployment. Do not replace running nodes with a mixed-version cluster.

## Session setting

In the session that will wait for a row locked by another node:

```sql
SHOW cluster.ges_retransmit_max_attempts;
SET cluster.ges_request_timeout_ms = -1;
SHOW cluster.ges_request_timeout_ms;
SHOW statement_timeout;
SHOW lock_timeout;
```

The existing configuration validation requires retransmission attempts greater
than zero before accepting `-1`. If it refuses, resolve the deployment
configuration first; do not bypass validation. Statement cancellation (including
`statement_timeout`), deadlock detection and cluster safety checks are not disabled.
An authority error is not converted into indefinite waiting or success.

The shipped default is still 60000 ms. A positive value selects a finite wait;
do not use zero to request infinity. This change does not change MultiXact or
ITL-capacity policies, quorum leases, write fencing or crash recovery support.

## Manual two-session check

Use a dedicated table already initialized consistently on the nodes. For the
demo table, first confirm that `demo_account` contains `id = 1`. Do not run this
check alongside a benchmark or on an application row.

Node 0:

```sql
BEGIN;
UPDATE demo_account SET value = 200 WHERE id = 1;
-- Keep this transaction open while starting the second session.
```

Node 1, in a separate psql connection:

```sql
SET cluster.ges_request_timeout_ms = -1;
BEGIN;
UPDATE demo_account SET value = value + 1 WHERE id = 1 RETURNING value;
-- This statement waits while node 0 owns the row lock.
```

After confirming the actual blocker, release it on node 0:

```sql
COMMIT;
```

Node 1 should then return 201. Finish its transaction:

```sql
COMMIT;
```

For the rollback leg, start with 300, let node 0 temporarily write 400, then
ROLLBACK node 0 while node 1 waits to add 1. Node 1 must return 301, not 401.
The automated qualification holds the verified blocker beyond the old 60-second
budget and separately checks cancellation and absence of leftover wait state.
Merely seeing psql wait is not evidence that those checks passed.

If a statement fails, PostgreSQL's transaction remains aborted until ROLLBACK.
Do not mistake that behavior for another lock timeout.
