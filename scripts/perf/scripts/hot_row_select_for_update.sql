-- spec-3.4e workload class 4: 2node-hot-row-lock (PARTIAL COVERAGE)
-- Same row SELECT FOR UPDATE + D5b 7-arg is_lock_only=true inject ACTIVE
-- state (pre-flight by runner) simulates remote ITL lock.
-- pgrac counter `cluster_remote_row_lock_fail_closed_count` delta = events/sec rate.
BEGIN;
SELECT abalance FROM pgbench_accounts WHERE aid = 1 FOR UPDATE;
END;
