-- PGRAC PRE2 v0.135.0 VM 镜像测试手册（docs/pre2/vm-test-guide.md）
-- 实验 1 集群状态检查：在任一节点执行（pgnode 0，然后 \i ~/cluster-status.sql）。

\echo '== 成员状态'
SELECT n.node_id, n.is_self, m.state, m.admitted_epoch, m.removed
FROM pg_cluster_nodes AS n
LEFT JOIN pg_cluster_membership AS m USING (node_id)
ORDER BY n.node_id;

\echo '== quorum'
SELECT in_quorum, quorum_size, disks_ok, disks_total, collision_state
FROM pg_cluster_quorum_state;

\echo '== 实例间心跳'
SELECT node_id, state, heartbeat_send_count, heartbeat_recv_count
FROM pg_cluster_cssd_peers
ORDER BY node_id;

\echo '== 互联连接'
SELECT node_id, state, heartbeat_recv_count, bytes_recv > 0 AS has_traffic
FROM pg_cluster_ic_peers
WHERE node_id <> current_setting('cluster.node_id')::int
ORDER BY node_id;

\echo '== 集群后台进程'
SELECT backend_type, count(*) AS processes
FROM pg_stat_activity
WHERE backend_type IN ('lmon', 'lms', 'lms worker', 'lmd', 'lck', 'cssd', 'qvotec',
                       'diag', 'cluster stats', 'sinval broadcaster', 'undo cleaner')
GROUP BY backend_type
ORDER BY backend_type;
