-- PGRAC PRE2 v0.135.0 VM 镜像测试手册（docs/pre2/vm-test-guide.md）
-- 实验 3：在执行过 cf-baseline.sql 的同一会话中执行，显示自基线以来的增量。

SELECT current_setting('cluster.node_id') AS node,
       max(value) FILTER (WHERE key = 'block_request_count')::bigint - :cf_req              AS req,
       max(value) FILTER (WHERE key = 'install_copy_count')::bigint - :cf_recv              AS recv,
       max(value) FILTER (WHERE key = 'block_ship_bytes_total')::bigint / 8192 - :cf_shipped AS shipped,
       max(value) FILTER (WHERE key = 'block_forward_sent_count')::bigint - :cf_fwd         AS fwd,
       max(value) FILTER (WHERE key = 'block_from_holder_ship_count')::bigint
     + max(value) FILTER (WHERE key = 'cf_xheld_read_ship_count')::bigint - :cf_holder_ship AS holder_ship,
       max(value) FILTER (WHERE key = 'trans_n_to_s_count')::bigint - :cf_to_s             AS to_s,
       max(value) FILTER (WHERE key = 'trans_n_to_x_count')::bigint
     + max(value) FILTER (WHERE key = 'trans_s_to_x_upgrade_count')::bigint - :cf_to_x     AS to_x,
       max(value) FILTER (WHERE key = 'trans_x_to_n_downgrade_count')::bigint
     + max(value) FILTER (WHERE key = 'trans_x_to_s_downgrade_count')::bigint - :cf_x_down AS x_down
FROM pg_cluster_state
WHERE category IN ('gcs', 'pcm');
