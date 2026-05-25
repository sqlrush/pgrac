-- spec-3.4e workload class 1: single-node-no-peer warmup
-- Standard pgbench builtin TPC-B; cluster.enabled=on w/o peers
-- vs cluster.enabled=off (disable build). L195 cluster_conf_has_peers()
-- gate verifies no RAC tax paid.
-- (Use pgbench builtin -b tpcb-like; this file is documentation placeholder.)
SELECT 1;  -- placeholder; actual workload is pgbench builtin
