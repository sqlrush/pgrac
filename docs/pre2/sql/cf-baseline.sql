-- PGRAC PRE2 v0.135.0 VM 镜像测试手册（docs/pre2/vm-test-guide.md）
-- 实验 3：在每个节点的会话中执行，记录 Cache Fusion 计数基线（保存为 psql 变量 :cf_*）。

SELECT max(value) FILTER (WHERE key = 'block_request_count')::bigint          AS req,
       max(value) FILTER (WHERE key = 'install_copy_count')::bigint           AS recv,
       max(value) FILTER (WHERE key = 'block_ship_bytes_total')::bigint / 8192 AS shipped,
       max(value) FILTER (WHERE key = 'block_forward_sent_count')::bigint     AS fwd,
       max(value) FILTER (WHERE key = 'block_from_holder_ship_count')::bigint
     + max(value) FILTER (WHERE key = 'cf_xheld_read_ship_count')::bigint     AS holder_ship,
       max(value) FILTER (WHERE key = 'trans_n_to_s_count')::bigint           AS to_s,
       max(value) FILTER (WHERE key = 'trans_n_to_x_count')::bigint
     + max(value) FILTER (WHERE key = 'trans_s_to_x_upgrade_count')::bigint   AS to_x,
       max(value) FILTER (WHERE key = 'trans_x_to_n_downgrade_count')::bigint
     + max(value) FILTER (WHERE key = 'trans_x_to_s_downgrade_count')::bigint AS x_down
FROM pg_cluster_state
WHERE category IN ('gcs', 'pcm') \gset cf_
