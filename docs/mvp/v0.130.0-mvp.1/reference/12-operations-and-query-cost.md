# 运维查询、采样方法与查询成本

Author: SqlRush <sqlrush@gmail.com>

适用版本：`v0.130.0-mvp.1`。返回[手册目录](../README.md)。


## 1. 节点与成员关系

```sql
SELECT n.node_id, n.hostname, n.role, m.state,
       m.presented_incarnation, m.last_admitted_incarnation,
       m.admitted_epoch, m.removed
  FROM pg_cluster_nodes AS n
  LEFT JOIN pg_cluster_membership AS m USING (node_id)
 ORDER BY n.node_id;
```

先比较 declared topology，再比较 membership。TCP `connected` 只表示传输连接；它不能替代
CSSD alive、quorum、membership MEMBER 或 write-fence authority。

## 2. Quorum 与 fence

```sql
SELECT clock_timestamp() AS sampled_at, * FROM pg_cluster_quorum_state;
SELECT clock_timestamp() AS sampled_at, * FROM pg_cluster_fence_state;
SELECT * FROM pg_cluster_voting_disks ORDER BY path;
```

`in_quorum=false` 是综合结论。排障时同时查看 voting disk 状态、collision、freeze/self-fence，
不要仅凭 peer 心跳恢复就判断写入可以恢复。

## 3. Interconnect 吞吐与错误增量

```sql
SELECT node_id, state, msg_send_count, msg_recv_count,
       bytes_send, bytes_recv, reconnect_count,
       connect_error_count, stale_epoch_drop_count
  FROM pg_cluster_ic_peers
 ORDER BY node_id;
```

对两次采样做增量。`bytes_send/recv` 有增长而业务无进展时，再查 GCS/GES wait、重试、drop、
not-admitted 和 queue gauge；不要只以网络字节判定协议成功。

## 4. GRD 查询成本

`pg_cluster_grd_shards` 固定返回 4096 行，适合低频核对分布；`pg_cluster_grd_entries` 会遍历
resource hash 表，可能持有短时共享锁并复制大量行。在线监控优先读取 `pg_cluster_state`
中 `grd` 分类的 O(1) 汇总键，只在定点诊断时扫描 entry 视图。

```sql
SELECT category, key, value
  FROM pg_cluster_state
 WHERE category IN ('grd', 'gcs', 'ges', 'lmd', 'lms')
 ORDER BY category, key;
```

## 5. ADG 与备份

```sql
SELECT node_id, dg_role, mrp_status, receive_lsn, apply_lsn,
       lag_bytes, lag_seconds, standby_consistent_scn
  FROM pg_stat_cluster_adg;

SELECT in_progress, backup_id, consistent_scn, stop_cut_lsn,
       manifest_crc, backup_set_path, manifest_path
  FROM pg_stat_cluster_backup;
```

`lag_bytes=0` 只表示 receive/apply LSN 当前相等，不自动证明读服务已开放；仍要结合 MRP status、
consistent SCN 和相关 admission 状态。

## 6. 参数差异与重启待办

```sql
SELECT name, setting, boot_val, reset_val, unit, context,
       source, sourcefile, sourceline, pending_restart
  FROM pg_settings
 WHERE name LIKE 'cluster.%'
 ORDER BY pending_restart DESC, name;
```

四节点配置比较时必须从每个实例分别采集。`pending_restart=true` 表示配置文件值尚未成为当前
运行值；在比较运行行为时使用 `setting`，在安排重启时再看 sourcefile 和 pending_restart。

## 7. 采样纪律

1. 每份样本记录节点号、postmaster 启动时间、membership incarnation/epoch 和采样时间。
2. counter 用增量，gauge 用瞬时值，timestamp 用新鲜度；三者不能混算。
3. 采样失败、超时或节点缺行必须保留为失败样本，不能当作 0。
4. 视图返回 `unknown`/`blocked` 时保持该结论，不用其他计数器推测成功。
5. 高频采集优先结构化单行视图和 `pg_cluster_state` 汇总键，避免反复扫描 GRD entry。
