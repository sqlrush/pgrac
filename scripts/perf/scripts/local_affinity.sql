-- spec-3.4e workload class 2: 2node-local-affinity
-- Each node updates own key range; no cross-node touch.
-- pgbench var :scale set by run_2node_baseline.pl based on --scale arg.
-- pgbench var :node_id (0 or 1) set per-node to partition key range.
\set aid_range_start (:node_id * 100000 * :scale / 2)
\set aid_range_end ((:node_id + 1) * 100000 * :scale / 2)
\set aid random(:aid_range_start, :aid_range_end - 1)
\set delta random(-5000, 5000)
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
END;
