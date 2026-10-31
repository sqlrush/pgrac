# 运维查询、采样方法与查询成本

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

## 8. Undo 容量等待采样

段池达到 `cluster.undo_segments_max_per_instance` 后，新共享写可能进入容量等待。是否能等待仍取决于原执行上下文与清偿证据；已有强锁、已发生写副作用或证据不足的调用者保留原拒绝。单段事务槽满时还可能等待当前活动事务结束，调大段池上限不能保证解除这种等待。

先观察当前会话等待，再低频采集同一实例的计数增量：

```sql
SELECT pid, state, wait_event_type, wait_event
  FROM pg_stat_activity
 WHERE wait_event IN ('ClusterUndoCapacityWait', 'ClusterUndoTxSlotWait');

SELECT statement_timestamp() AS sampled_at,
       pg_postmaster_start_time() AS postmaster_started_at,
       category, key, value
  FROM pg_cluster_state
 WHERE category = 'undo_cleaner'
   AND key IN ('tt_capacity_stats_available', 'tt_capacity_origin_node_id',
               'tt_slot_active_full_wait_count', 'tt_capacity_pool_wait_count',
               'tt_capacity_wakeup_recheck_count', 'tt_capacity_cancel_count',
               'tt_capacity_refuse_by_reason.context',
               'tt_capacity_refuse_by_reason.proof')
 ORDER BY key;
```

`pg_cluster_state` 先生成诊断快照，`WHERE` 只过滤返回结果，不减少其底层采样成本；其中包含共享状态扫描，勿把这个查询当作 O(1) 高频探针。没有等待行也不证明等待从未发生。统计不可用时保留 `tt_capacity_stats_available=false`，不要把缺少的键补零。按来源和 postmaster 启动时间分组比较，重启前后的累计值不能直接相减。

`tt_capacity_wakeup_recheck_count` 是重查次数，包含周期性返回，不代表新槽、成功提交或 cleaner 实际完成数。原 `capacity_wait_repolled` 仍只计容量 CV，不能与它等同。拒绝原因计数和 `undo_retain_by_reason.*` 是诊断观察，不能单凭 `history_pin` 或池满断定长读者是根因。

周期性容量重查不会自行产生新的事务超时。调用者取消/statement timeout (`57014`)、原 lock timeout (`55P03`)、死锁 (`40P01`) 和准入变化 (`53R60`) 保持各自含义，排查时保留原 SQLSTATE 与日志。shared 配置下 cleaner 必须开启；不要通过关闭 cleaner、释放用户锁或重置保留信息来消除等待。
