-- spec-3.4e workload class 3: 2node-cross-node-visibility (PARTIAL COVERAGE)
-- Same-node D5b COMMITTED inject + forced cluster path via
-- cluster_test_force_visibility_cluster_path = on (pre-flight by runner).
-- Heavy SELECT triggers visibility decide_by_scn path frequency;
-- pgrac counter `cluster_tt_status_lookup_hit_count` delta verified.
\set aid random(1, 100000 * :scale)
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
