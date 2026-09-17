# `pg_cluster_state` 字段与完整静态键目录

Author: SqlRush <sqlrush@gmail.com>

基线：`v0.130.0-mvp.1` / `c581f3835a4a9a76ce5a77c0937930f21765725a`。

该视图把不同子系统状态统一投影为 `(category, key, value)` 三个 text 字段。
本目录覆盖该标签的 1038 个不同静态 category/key 组合，并列举动态键族。静态键名不代表每次查询都产生该行。

键目录用于定位接口，不按名称猜测完整行为：`*_count` 既可能是累计事件，也可能是当前队列/对象数量；`*_us` 既可能是耗时，也可能是绝对时间。未核对具体读数契约时，不自动做增量、时间换算或健康判定。三个 SQL 字段始终是 text；保留原值用于与日志关联。

## `pg_cluster_state`

| 字段 | SQL 类型 | 含义 |
|---|---|---|
| `category` | `text` | 状态所属子系统分类；用于先收窄 owner 与生命周期。 |
| `key` | `text` | 分类内稳定键名；动态键族会把 region、worker、node 或 histogram 边界编码进名称。 |
| `value` | `text` | 查询时格式化的值；`(unset)`、`(null)`、`(empty)` 是文本哨兵，不是 SQL NULL。 |

行基数由编译选项、注册 region、worker 数、节点证据和动态 histogram 共同决定；查询会顺序读取多个子系统，因此全表不是原子 authority snapshot。

## 动态键族示例

| category | 键模式 | 行基数 | 含义 |
|---|---|---|---|
| `shmem` | `region.<name>.bytes`, `region.<name>.owner` | 每个注册 region 两行 | region 大小和 owner 子系统 |
| `inject` | `<point>.fault_type`, `<point>.hits` | 每个注入点两行 | 当前故障类型与命中次数 |
| `pgstat` | `<counter-name>` | 每个 pgstat counter 一行 | 与计数器视图相同的值文本 |
| `scn` | `scn_remote_durable_node<N>`, `scn_remote_durable_epoch_node<N>` | 每个有远端 durable 证据的节点两行 | 远端 durable SCN 与其 epoch |
| `lms` | `*_w<N>` | 每个活动 LMS worker | worker 级 dispatch/reply/reset/serve/drop 计数 |
| `lms` | `lms_serve_hist_us_le_<bound>[_w<N>]`, `..._inf...` | 聚合 16 桶并按 worker 展开 | inline serve 耗时直方图 |
| `gcs` | `ship_hist_us_le_<bound>`, `ship_hist_us_inf` | 16 桶 | block ship 延迟直方图 |
| `xnode_profile` | `bucket.<name>.total_nanos`, `bucket.<name>.n_events` | 每个 profile bucket 两行 | 累计纳秒与事件数 |
| `xnode_profile` | `hist.<component>.le_<edge>us`, `...le_inf` | 每组件多桶 | commit component 延迟直方图 |
| `xnode_lever` | 编译期字段名 | 每个 lever counter 一行 | 各优化 lever 的命中/拒绝计数 |
| `multixact_current` | 运行时名称表条目 | 每个统计项一行 | current MultiXact 等待、重组和 fail-closed 计数 |

动态键的 `<N>`、`<name>` 和 histogram 边界属于键的一部分；采集器应按模式匹配，不能假设固定行数。

## category = `advisory`

| 静态 key | 读取范围 |
|---|---|
| `advisory_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `advisory_globalize_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `advisory_session_release_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `advisory_try_grant_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `advisory_try_notavail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `block_format`

| 静态 key | 读取范围 |
|---|---|
| `invalid_scn_value` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `itl_array_bytes` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `itl_initrans_default` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `itl_location` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `itl_slot_size_bytes` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `page_header_size` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `page_layout_version` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_size_bytes` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tuple_header_extra_bytes` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `buffer_format`

| 静态 key | 读取范围 |
|---|---|
| `buffer_cold_field_offset` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `buffer_desc_pad_to_size` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `buffer_desc_size_bytes` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `buffer_hot_field_offset` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `buffer_type_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_state_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `catalog`

| 静态 key | 读取范围 |
|---|---|
| `buf_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `buf_miss_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `oid_lease_acquire_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `oid_lease_remaining` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_side_effect_drop_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_side_effect_record_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `relmap_shared_committed_generation` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `relmap_shared_pending_generation` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `relmap_shared_pending_owner_node` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `shared_catalog_enabled` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis_resolve_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis_unknown_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_authority_native_hw` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_authority_sealed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_epoch_gate_admitted` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_native_prehistory_covered_hw` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_native_prehistory_disabled` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_native_raw_reused` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_prehistory_adopted` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_wrap_barrier_done` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `cf`

| 静态 key | 读取范围 |
|---|---|
| `cf_bak_fallback` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cf_failclosed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cf_s_acquire` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cf_single_node_authority` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cf_x_acquire` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_anchor_boot_adopt_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_anchor_write_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `cluster_cssd`

| 静态 key | 读取范围 |
|---|---|
| `cluster_cssd_last_liveness_tick_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster_cssd_main_loop_iters` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster_cssd_pid` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster_cssd_ready_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster_cssd_spawned_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster_cssd_status` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster_cssd_status_enum_value` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cssd.declared_alive_bitmap` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cssd.declared_alive_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `cluster_stats`

| 静态 key | 读取范围 |
|---|---|
| `cluster_stats_last_liveness_tick_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster_stats_main_loop_iters` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster_stats_pid` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster_stats_ready_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster_stats_spawned_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster_stats_status` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster_stats_status_enum_value` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `conf`

| 静态 key | 读取范围 |
|---|---|
| `node_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `self_in_topology` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `cr`

| 静态 key | 读取范围 |
|---|---|
| `cr_base_lsn_mismatch_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_cache_evict_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_cache_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_cache_install_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_cache_miss_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_chain_walk_steps_sum` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_construct_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_corruption_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_cross_instance_unsupported_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_epoch_mismatch_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_generation_mismatch_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_global_epoch_fallback_bump_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_inverse_delete_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_inverse_insert_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_inverse_itl_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_inverse_update_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_key_mismatch_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_locator_reuse_reject_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_reconfig_intra_survived_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_rel_gen_bump_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_rel_gen_table_overflow_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_remote_failed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_remote_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_remote_partial_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_retention_horizon_advance_noted_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_server_denied_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_server_fence_refused_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_server_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_server_multi_verdict_denied_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_server_multi_verdict_served_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_server_partial_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_server_undo_denied_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_server_undo_served_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_server_verdict_denied_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_server_verdict_served_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_snapshot_too_old_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_tuple_fallback_cliff_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_tuple_fallback_cross_block_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_tuple_fallback_identity_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_tuple_fallback_multichain_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_tuple_fallback_recycle_wm_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_tuple_fallback_remote_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_tuple_fallback_uncertain_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_tuple_verdict_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_xmax_invalid_or_ambiguous_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_xmax_recycled_invisible_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_xmax_resolved_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_xmax_scan_unavail_or_no_proof_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rtvis_native_prehistory_local_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rtvis_resolve_aborted_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rtvis_resolve_committed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rtvis_resolve_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rtvis_underivable_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rtvis_undo_fetch_cache_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rtvis_undo_fetch_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rtvis_undo_fetch_wire_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rtvis_verdict_below_horizon_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rtvis_verdict_exact_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rtvis_verdict_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rtvis_verdict_inadmissible_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rtvis_verdict_wire_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_authority_epoch_stale_reject_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_authority_fail_closed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_authority_multi_match_reject_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_authority_scan_incomplete_reject_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_authority_serve_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis53r97_leg_covers_refuse_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis53r97_leg_invalid_scn_refuse_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis53r97_leg_live_upgrade_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis53r97_leg_multi_member_serve_ask_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis53r97_leg_multi_member_serve_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis53r97_leg_multi_unresolvable_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis53r97_leg_srv_other_refuse_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis53r97_leg_xmax_unprovable_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis53r97_leg_xmin_overlay_verdict_ask_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis53r97_leg_xmin_overlay_verdict_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis53r97_leg_zero_match_refuse_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis_freshref_verdict_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis_freshref_verdict_resolved_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `cr_pool`

| 静态 key | 读取范围 |
|---|---|
| `abort_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `admit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `admit_reject_bulk` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `admit_reject_no_admit` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `admit_reject_nonmain_fork` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `admit_reject_parallel` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `admit_reject_pressure` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `admit_reject_relcap` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `admit_reject_volatile` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `current_epoch` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `epoch_bump_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `evict_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `live_entries` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `miss_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `publish_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `publish_stale_release_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `reserve_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `ctrc`

| 静态 key | 读取范围 |
|---|---|
| `cleaner_reason` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `full_refusal_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `origin_blocked` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `origin_open` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `origin_release_proven` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `origin_sealing` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `participant_ack_frozen` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `participant_ack_ready` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `participant_blocked` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `participant_draining` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `participant_open` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `receipt_ack_frozen` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `receipt_applied` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `receipt_blocked` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `receipt_cancelled` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `receipt_cleaned` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `receipt_prepared` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `test_barrier_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `test_barrier_phase` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `diag`

| 静态 key | 读取范围 |
|---|---|
| `diag_last_liveness_tick_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `diag_main_loop_iters` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `diag_pid` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `diag_ready_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `diag_spawned_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `diag_status` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `diag_status_enum_value` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `dl`

| 静态 key | 读取范围 |
|---|---|
| `failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lease_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `native_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `release_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `gcs`

| 静态 key | 读取范围 |
|---|---|
| `api_state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_checksum_fail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_family_plane` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_forward_holder_evicted_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_forward_received_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_forward_sent_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_from_holder_ship_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_invalidate_ack_received_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_invalidate_broadcast_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_invalidate_timeout_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_master_not_holder_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_reply_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_request_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_ship_bytes_total` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_storage_fallback_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_timeout_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_wal_flush_before_ship_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_x_forward_sent_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_x_granted_from_holder_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_x_self_ship_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_x_transfer_ship_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cf_xheld_read_ship_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `clean_page_xfer_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `clean_page_xfer_fail_closed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `clean_page_xfer_stale_holder_recover_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `clean_page_xfer_storage_fallback_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `clean_page_xfer_third_party_denied_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `decode_payload_bytes` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dedup_collision_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dedup_done_marked_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dedup_done_mismatch_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dedup_entry_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dedup_evict_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dedup_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dedup_hint_violation_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dedup_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dedup_legacy_pin_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dedup_max_entries` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dedup_misroute_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dedup_miss_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `direct_install_abort_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `direct_install_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dispatch_loop_iterations` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `done_enqueue_drop_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `done_sent_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `drop_pinned_deny_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `encode_payload_bytes` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `epoch_invalidate_wake_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `fallback_scn_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `fallback_scn_refresh_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `fallback_scn_verify_pass_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `forward_replay_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `forward_send_not_admitted_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `forward_send_queued_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `handle_reply_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `handle_request_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `install_copy_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `invalidate_busy_received_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `invalidate_busy_sent_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `invalidate_park_expired_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `invalidate_park_overflow_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `invalidate_parked_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `invalidate_passive_s_release_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `invalidate_send_not_admitted_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `invalidate_send_queued_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `live_sge_fallback_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `live_sge_send_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `local_s_upgrade_grant_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lookup_master_remote_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lookup_master_self_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lost_write_avoid_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lost_write_detected_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lost_write_invalidscn_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lost_write_master_direct_storage_fallback_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lost_write_not_scn_tracked_skip_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `master_holder_lifecycle_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `max_outstanding` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `max_outstanding_per_backend` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `outstanding_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_x_self_handoff_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_x_self_handoff_drain_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pi_durable_note_apply_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pi_master_metadata_retire_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pi_watermark_advance_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pi_watermark_retire_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plane_misroute_reject` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `reply_late_drop_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `reply_send_not_admitted_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `reply_send_queued_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `reply_timeout_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `retransmit_attempt_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `retransmit_exhausted_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `retransmit_send_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `s_holders_bitmap_redirect_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scratch_copy_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `send_request_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `stale_reply_drop_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `starvation_denied_pending_x_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `x_vs_s_no_carrier_denied_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `x_vs_s_nonholder_grant_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xfer_stale_deny_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `gcs_recovery`

| 静态 key | 读取范围 |
|---|---|
| `ambiguous_owner_failclosed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `before_boundary_failclosed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_resources_recovering` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_state_rebuilt` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `buffers_redeclared` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `redo_boundary_reached` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `redo_boundary_waits` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `redo_coverage_gate_block_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `redo_coverage_required_lsn_zero_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `stale_block_drop` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `ges`

| 静态 key | 读取范围 |
|---|---|
| `ges_release_ack_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `ges_reply_defer_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `ges_reply_late_drop_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `ges_reply_wait_table_active` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `ges_request_defer_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `ges_timeout_capacity_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `ges_timeout_master_reject_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `ges_timeout_native_probe_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `ges_timeout_retransmit_exhausted_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `ges_timeout_send_fail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `ges_timeout_true_wait_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tx_enqueue_timeout_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tx_enqueue_wait_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tx_enqueue_wakeup_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `grd`

| 静态 key | 读取范围 |
|---|---|
| `grd_allocated_bytes` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_bast_ack_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_bast_received_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_bast_reject_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_bast_retry_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_bast_sent_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_bast_stale_drop_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_cleanup_skip_stale_cancel_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_convert_enqueued_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_convert_granted_inplace_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_convert_illegal_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_converts_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_deadlock_chunk_oo_buffer_overflow_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_deadlock_probe_collision_drop_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_deadlock_probe_drop_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_entries_reclaimed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_entry_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_entry_create_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_entry_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_entry_lookup_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_ges_cleanup_deferred_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_ges_inbound_validation_fail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_ges_reply_deferred_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_ges_reply_dropped_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_ges_work_queue_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_holders_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_local_master_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_local_master_lookup_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_master_map_refresh_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_max_entries` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_ngranted_promoted_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_outbound_cleanup_dirty_depth` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_outbound_cleanup_retry_warn50_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_outbound_cleanup_retry_warn90_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_outbound_reply_dirty_depth` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_outbound_ring_depth` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_pending_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_pin_high_water` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_reclaim_skipped_pinned_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_relation_object_cluster_path_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_remote_master_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_remote_master_lookup_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_resid_encode_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_shard_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_shard_lookup_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_starvation_barrier_enqueued_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_starvation_barrier_publish_fail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_starvation_boost_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_starvation_max_skip_observed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_sweep_runs` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_transaction_cluster_path_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_waiters_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `grd_work_queue_depth` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `grd_recovery`

| 静态 key | 读取范围 |
|---|---|
| `block_path_failclosed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_redeclare_cursor` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_redeclare_done` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_redeclare_epoch` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster_gate_timeout` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `converts_requeued` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `done_self_bitmap_hash` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `done_self_epoch` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `episode_epoch` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `event_coordinator` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `event_old_epoch` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `holders_rebound` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `holders_redeclared` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `join_block_recovering_failclosed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `join_block_views_rebuilt` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `join_remaster_done` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `join_remaster_started` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `join_shards_remastered` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `last_event_id` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `offpath_crash_rejoin_fenced` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rebuild_timeout` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remaster_done` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remaster_failed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remaster_started` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `shards_remastered` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `stale_holder_swept` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `stale_request_drop` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `state_enum_value` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `unaffected_holder_survived` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `wait_epoch_escape` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `waiters_requeued` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `guc`

| 静态 key | 读取范围 |
|---|---|
| `cluster.cf_delayed_cleanout` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster.cf_terminal_authority` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster.config_file` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster.injection_points` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster.interconnect_tier` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster.node_id` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster.shared_storage_backend` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster.shmem_max_regions` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster.smart_fusion` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster.smart_fusion_commit_brake_timeout_ms` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster.smart_fusion_origin_durable_gossip_ms` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster.smart_fusion_tier_min` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cluster.smgr_user_relations` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `hang`

| 静态 key | 读取范围 |
|---|---|
| `hang_aba_revalidate_failed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_advisory_recommendations` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_available` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_cycle_detected_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_deadlock_confirmed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_degraded_to_timeout` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_dump_enabled` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_dumps_emitted` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_error_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_excluded_bgworker_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_excluded_deadlock_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_excluded_idle_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_hard_skipped` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_incomplete_sample_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_last_dump_emitted_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_last_sample_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_long_wait_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_long_waits_seen` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_longest_wait_us` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_manager_enabled` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_max_sampled` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_n_samples` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_no_safe_victim` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_non_actionable_skipped` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_not_confirmed_yet` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_over_excluded` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_proc_signal_dump_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_resolution_failed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_resolution_mode` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_resolve_evaluations` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_resolve_last_action` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_resolve_last_victim_pid` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_resolved_confirmed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_sample%d_blocker_pid` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_sample%d_blocker_remote_node` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_sample%d_duration_kind` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_sample%d_in_confirmed_deadlock` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_sample%d_pid` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_sample%d_quality` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_sample%d_source` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_sample%d_wait_event` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_sample%d_wait_ms` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_sample_epoch` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_sample_interval_ms` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_samples_taken` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_soft_cancels_issued` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_terminates_issued` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_threshold_ms` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_truncated` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_unprovable_root_skipped` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hang_victims_selected` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `hw`

| 静态 key | 读取范围 |
|---|---|
| `alloc_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `authority_create_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cold_boot_mode` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cold_boot_state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lease_consumed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lease_grants` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lease_leased_total` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lease_orphan_zero` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lease_outstanding` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `not_ready_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rebuild_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remaster_blocked_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remaster_done_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remaster_recoverable` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remaster_retry_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remaster_retry_exhausted_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `reserve_wal_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `ic`

| 静态 key | 读取范围 |
|---|---|
| `active_tier_name` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `caps_reply_reject_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `peer_capabilities` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tier1_fifo_admitted_control` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tier1_fifo_admitted_data` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tier1_fifo_dropped_close_control` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tier1_fifo_dropped_close_data` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tier1_fifo_promoted_control` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tier1_fifo_promoted_data` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tier1_listener_incarnation` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tier1_listener_pid` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tier1_listener_port` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tier1_send_not_admitted_control` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tier1_send_not_admitted_data` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tier1_writable_drain_control` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tier1_writable_drain_data` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `inject`

| 静态 key | 读取范围 |
|---|---|
| `armed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `ir`

| 静态 key | 读取范围 |
|---|---|
| `recovery_serial_busy_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_serial_capability_denied_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_serial_cold_set_grant_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_serial_grant_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_serial_native_result_rejected_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_serial_node_cleanup_wait_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_serial_release_confirmed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_serial_release_unconfirmed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_serial_retry_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_serial_revalidate_reject_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `ko`

| 静态 key | 读取范围 |
|---|---|
| `ack_received_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `flush_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `inbound_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lockfail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `native_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `peer_apply_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `lck`

| 静态 key | 读取范围 |
|---|---|
| `lck_last_liveness_tick_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lck_main_loop_iters` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lck_pid` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lck_ready_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lck_spawned_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lck_status` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lck_status_enum_value` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `lmd`

| 静态 key | 读取范围 |
|---|---|
| `cancel_ack_mismatch_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cancel_ack_received_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cancel_consumed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cancel_escalated_alternate_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cancel_exhausted_timeout_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cancel_no_safe_victim_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cancel_retransmit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cancel_stale_cleared_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cancel_token_installed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cancel_wait_stale_rejected_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cleanup_lmd_sweep_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cleanup_on_backend_exit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cleanup_orphan_edge_swept_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cleanup_skip_other_owner_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `confirm_unconfirmed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cross_node_cancel_queue_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cross_node_cancel_received_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cross_node_victim_cancel_sent_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cross_node_victim_pending_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cycle_detected_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `deadlock_confirmed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `graph_generation` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `inject_call_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmd_edge_submission_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmd_error_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmd_idle_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmd_ready_at_us` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmd_started_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmd_state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmd_wake_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `member_incomplete_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_convert_wfg_exact_remove_stale_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_convert_wfg_remove_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_convert_wfg_replace_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_convert_wfg_replace_fail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `probe_broadcast_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `probe_drop_duplicate_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `probe_drop_stale_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `probe_partial_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `probe_partial_report_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `probe_queue_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `probe_report_enqueue_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `reconfig_cancel_discarded_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `reconfig_discard_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `revalidate_fail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tarjan_scan_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `victim_cancel_sent_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `victim_protected_skip_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `victim_repeat_avoided_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `wait_edge_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `wait_edge_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `lmon`

| 静态 key | 读取范围 |
|---|---|
| `lmon_last_iter_us` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmon_last_liveness_tick_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmon_main_loop_iters` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmon_max_iter_us` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmon_pid` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmon_ready_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmon_slow_iter_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmon_spawned_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmon_status` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmon_status_enum_value` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmon_timed_duty_sample_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lmon_total_iter_us` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `lms`

| 静态 key | 读取范围 |
|---|---|
| `lms_conn_reset_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_data_dispatch_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_decision_convert_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_decision_grant_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_decision_reject_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_direct_reply_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_drain_empty_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_error_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_inline_serve_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_outbound_cap_guard_drop_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_outbound_not_admitted_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_outbound_requeue_drop_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_started_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lms_work_drained_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `native_probe_aggregate_holder_conflict_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `native_probe_aggregate_waiter_conflict_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `native_probe_collector_slot_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `native_probe_reply_recv_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `native_probe_retry_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `native_probe_sent_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `native_probe_timeout_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `priority_starvation_observed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `normal_start`

| 静态 key | 读取范围 |
|---|---|
| `boot_incarnation` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `census_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `completion_bytes` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `epoch` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `node_id` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `own_checkpoint_lsn` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `own_checkpoint_scn` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `own_next_full_xid` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `own_redo_lsn` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pgrd_hex` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pgsa_hex` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `ready_snapshot` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `reserved` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `system_identifier` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tt_commit_scn_max` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `pcm`

| 静态 key | 读取范围 |
|---|---|
| `block0_reply_wait_stats_available` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `convert_queue_active` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dead_cleanup_entries` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `evict_release_deferred_aux_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `head_no_progress_expire_reason` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `invalidate_parked_grant_pending_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `last_remote_t_grant_us` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `last_remote_t_image_us` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `last_remote_t_install_us` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `local_s_revoke_nonholder_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `master_state_n_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `master_state_s_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `master_state_x_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_api_state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_active_entries` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_allocated_bytes` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_binding_generation` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_capacity_fail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_capacity_retry_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_live_entries` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_max_entries` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_peak_live_entries` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_reclaim_attempt_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_reclaim_reuse_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_reclaim_success_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_tombstone_slots` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_transport_refcount` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_grd_wait_refcount` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_lock_mode_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pcm_transition_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `pi_holders_total_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remote_episode_excluded_missing_grant` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remote_episode_excluded_missing_image` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remote_episode_excluded_no_install` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remote_grant_after_image_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remote_image_at_or_after_grant_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remote_install_observed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `resource_x_active_debt_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `resource_x_evicting_debt_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `resource_x_gate_formation` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `resource_x_gate_phase` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `resource_x_invalid_debt_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `resource_x_local_owner_debt_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `resource_x_proof_readiness` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `resource_x_retained_debt_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `resource_x_writer_path` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `resource_x_writer_r4_generation` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `restore_aba_detected_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `rx_stats_available` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `trans_n_to_s_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `trans_n_to_x_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `trans_s_to_n_invalidate_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `trans_s_to_n_release_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `trans_s_to_x_cleanout_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `trans_s_to_x_upgrade_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `trans_x_to_n_downgrade_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `trans_x_to_n_release_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `trans_x_to_s_downgrade_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vm_clear_bitmap_word_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vm_observation_scope` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vm_observed_tag` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vm_observed_tag_available` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vm_other_tag_observations` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vm_stats_available` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vm_unique_heap_blocks_cleared` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `wait_margin_stats_available` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `wm_prov_insert_fail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `writer_cover_stale_detected_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `writer_reverify_reacquire_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `phase`

| 静态 key | 读取范围 |
|---|---|
| `cluster_phase` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `phase_elapsed_seconds` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `phase_enum_value` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `phase_history` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `phase_started_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `r4`

| 静态 key | 读取范围 |
|---|---|
| `cr_holder_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_holder_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_holder_retry_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_requester_backpressure_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_requester_reply_period_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_requester_terminal_retry_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cr_route_started_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `multi_resolve_served_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `multi_resolve_unknown_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `slot_capacity_retry_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tx_resolve_aborted_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tx_resolve_committed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tx_resolve_in_progress_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tx_resolve_prepared_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tx_resolve_unknown_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_data_fetch_denied_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_data_fetch_served_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `reconfig`

| 静态 key | 读取范围 |
|---|---|
| `marker_slow_ack_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `marker_timeout_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `reconfig_join`

| 静态 key | 读取范围 |
|---|---|
| `clean_departed_cleared_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `join_apply_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `join_pending_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `join_reject_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `join_timeout_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `reconfig_touched`

| 静态 key | 读取范围 |
|---|---|
| `abort_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `clean_leave_rejected` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `self_touched_hex` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `stamp_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `stamp_gcs_block` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `stamp_ges` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `stamp_scn` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `stamp_sinval` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `stamp_vis` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `recovery`

| 静态 key | 读取范围 |
|---|---|
| `block_recovery_blocks_recovered` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_recovery_failclosed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `materialized_remote_instances` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `merged_own_bound_skips` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `merged_records_applied` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `merged_skipped_local` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plan_crashed_candidates` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plan_dbstate_at_startup` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plan_generated_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plan_local_recovery_needed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plan_n_alive` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plan_n_clean` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plan_n_crashed_candidate` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plan_n_empty` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plan_n_unknown` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plan_own_thread` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plan_state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plan_threads_scanned` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `plan_unknown_threads` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_2pc_standby_rebuilds` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_overlay_rebuild_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_undo_redo_applies` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_undo_redo_skips` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remote_authority_53ra` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remote_outcome_aborted` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remote_outcome_committed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remote_uba_resolved` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `stream_ok_threads` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `stream_suspect_or_unreadable_threads` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `thread_recovery_recovered_through_lsn` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `thread_recovery_replay_failclosed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `thread_recovery_state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `thread_recovery_threads_recovered` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `worker_generation` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `worker_pool_state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `workers_done` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `workers_failed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `workers_requested` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `workers_started` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `resolver_cache`

| 静态 key | 读取范围 |
|---|---|
| `acceptance_failclosed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `acceptance_pass` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `epoch_miss` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `evict` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hit` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `install` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `key_present` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `live_entries` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lookup` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `nonown_skip` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `nonterminal_skip` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `revalidate_miss` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `scn`

| 静态 key | 读取范围 |
|---|---|
| `scn_abort_advance_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_boc_broadcast_fanout_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_boc_event_publish_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_boc_last_sweep_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_boc_max_batch_size` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_boc_payload_accept_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_boc_payload_bad_length_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_boc_payload_node_mismatch_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_boc_payload_regression_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_boc_pending_at_last_sweep` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_boc_sweep_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_boc_sweep_fallback_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_commit_advance_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_commit_lookup_defer_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_current_encoded` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_current_local` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_durable_frontier_frozen` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_durable_frontier_overflow_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_durable_frontier_regression_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_durable_pending_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_durable_safe_scn` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_initialized_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_last_advance_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_last_observe_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_max_observed_remote` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_node_id` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_observe_bump_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_observed_max_observe_gap_ms` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_seconds_since_last_observe` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_total_advance_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `sequence`

| 静态 key | 读取范围 |
|---|---|
| `sq_cycle_rejected_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `sq_dup_guard_fail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `sq_failover_fail_closed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `sq_page_writeback_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `sq_refill_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `sq_refill_wait_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `shared_fs`

| 静态 key | 读取范围 |
|---|---|
| `active_backend` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `registered_backends` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `smgr_active_relations` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `smgr_inval_bcast_sent_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `smgr_user_relations` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `shmem`

| 静态 key | 读取范围 |
|---|---|
| `created_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `magic` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `node_id_at_init` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `region_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `total_bytes` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `version_packed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `sinval`

| 静态 key | 读取范围 |
|---|---|
| `ack_orphan_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `ack_received_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `ack_timeout_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `broadcast_receive_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `broadcast_send_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `echo_dropped_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `fanout_hard_error_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `fanout_peer_down_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `fanout_would_block_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `inbound_overflow_reset_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `inbound_queue_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `inject_local_queue_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `outbound_queue_full_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `smgr_inval_applied_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `stale_epoch_drop_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `validation_drop_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `smart_fusion`

| 静态 key | 读取范围 |
|---|---|
| `commit_brake_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `commit_brake_wait_us` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dbwr_brake_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dep_install_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dep_lost_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dep_touch_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `origin_suspect_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `retry_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `ts`

| 静态 key | 读取范围 |
|---|---|
| `failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `native_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `s_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `x_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `tt_2pc`

| 静态 key | 读取范围 |
|---|---|
| `twopc_postprepare_transfers` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `twopc_prefinish_aborts` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `twopc_prefinish_commits` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `twopc_prepare_records` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `twopc_prepare_undo_flushes` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `twopc_recover_rebinds` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `tt_recovery`

| 静态 key | 读取范围 |
|---|---|
| `active_slots_resolved_aborted` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `heap_tuples_physically_reverted` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recovery_verdict_failclosed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `recycled_liveness_relaxed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `remote_active_failclosed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `scn_highwater_recovered` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_revert_failclosed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `wrap_generation_disambiguated` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `tt_status`

| 静态 key | 读取范围 |
|---|---|
| `evict_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `evict_fail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `flush_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `install_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lookup_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `lookup_miss_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `self_consumer_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `tt_status_hint`

| 静态 key | 读取范围 |
|---|---|
| `drop_invalid_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `drop_stale_epoch_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `drop_unknown_version_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `drop_v1_compat_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `emit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `install_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `receive_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `undo`

| 静态 key | 读取范围 |
|---|---|
| `autoextend_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_flush_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `block_write_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cleaner_header_tt_slots_below_horizon` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cleaner_pass_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cleaner_segments_marked_recyclable` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cleaner_shmem_tt_slots_gcd` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `cleaner_stale_active_skipped` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `commit_fsync_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `commit_fsync_failure_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `commit_fsync_segment_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `extent_claim_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `horizon_admission_refuse_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `horizon_idle_sentinel_sent_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `horizon_last_floor_scn` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `horizon_pass_abort_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `horizon_peer_reports` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `horizon_peer_stale_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `horizon_stall_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `horizon_wire_reject_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `reader_lookup_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `receipt_stats_available` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `record_alloc_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `record_seg_commit_skipped_inflight` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `record_seg_residual_revalidate_drops` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `record_segments_committed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `retention_horizon_scn` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `retention_max_recycle_horizon` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `retention_off_recycle_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `retention_recycle_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `segment_allocated_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `segment_allocated_high_water` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `segment_claim_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `segment_create_fail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `segment_effective_cap` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `segment_hard_cap_fail_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `segment_observation_status` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `segment_retain_skip_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `segment_reuse_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `segment_switch_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `smgr_close_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `smgr_open_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `smgr_pread_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `smgr_pwrite_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `terminal_authority_check_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `terminal_authority_durable_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `terminal_authority_epoch_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `terminal_authority_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `terminal_authority_nonterminal_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `terminal_authority_ok_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `terminal_authority_ownership_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `terminal_authority_retention_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `terminal_authority_unknown_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tt_durable_by_xid_scan_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tt_durable_commit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tt_durable_lookup_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tt_durable_lookup_miss_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tt_durable_redo_apply_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tt_retention_rollover_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tt_rollover_fail_activate_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tt_rollover_fail_extend_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tt_rollover_fail_hard_cap_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tt_slot_retain_skip_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `tt_slot_wrap_retired_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_buf_boundary_violations` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_buf_held_evidence` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_buf_held_wal` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_buf_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_buf_miss_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_buf_remote_evidence_holds` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_buf_writeback_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_gcs_grant_exclusive_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_gcs_grant_shared_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_gcs_invalidate_notify_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_gcs_local_fast_path_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_gcs_remaster_deny_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_gcs_ship_bytes` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `undo_cleaner`

| 静态 key | 读取范围 |
|---|---|
| `capacity_wait_entered` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `capacity_wait_refused_context` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `capacity_wait_refused_proof` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `capacity_wait_repolled` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_cleaner_last_liveness_tick_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_cleaner_main_loop_iters` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_cleaner_pid` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_cleaner_ready_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_cleaner_spawned_at` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_cleaner_status` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `undo_cleaner_status_enum_value` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `visibility`

| 静态 key | 读取范围 |
|---|---|
| `covers_scn_refuse_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `evidence_stats_available` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `overlay_refresh_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `prune_remote_keep_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis_conflict_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis_dirty_fork_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis_selftoast_fork_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis_update_fork_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `vis_variant_unknown_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `writer_chain_failclosed_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `writer_chain_resolved_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xmax_resolved_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `wal_thread`

| 静态 key | 读取范围 |
|---|---|
| `claim_created` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dir_configured` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `dir_validated` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `page_stamp_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `registry_highest_lsn` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `registry_highest_scn` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `registry_last_updated` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `registry_ready` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `registry_slot_state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `thread_id` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `write_fence`

| 静态 key | 读取范围 |
|---|---|
| `baseline_author_is_self` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `baseline_authority_age_us` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `baseline_published` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `baseline_stale_rejected` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `durable_check_blocked` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `external_admit_requested` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `external_daemon_disconnect` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `external_expired` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `external_identity_mismatch` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `external_last_journal_seq` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `external_last_proof_age_ms` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `external_mutation_gate_blocked` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `external_publish_gate_blocked` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `external_rejected` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `external_unavailable` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `external_unknown` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `external_write_excluded` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hot_gate_blocked` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `marker_write_failed` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `minority_marker_ignored` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `xid_stripe`

| 静态 key | 读取范围 |
|---|---|
| `mxid_stripe_activated_floor` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `mxid_stripe_disk_state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `mxid_stripe_halfspace_refusals` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `mxid_stripe_underivable_reads` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_stripe_activated_floor` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_stripe_cluster_max_hwm` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_stripe_cluster_min_hwm` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_stripe_disk_state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_stripe_herding_floor` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_stripe_mode_epoch` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_stripe_my_hwm_promise` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_stripe_my_slot_floor` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_stripe_replay_active_bitmap` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_stripe_replay_floor` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `xid_stripe_slot_state` | 查询节点的文本投影；与同分类状态和日志联合解释 |
## category = `xnode_profile`

| 静态 key | 读取范围 |
|---|---|
| `hw_extend_local_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `hw_extend_remote_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `read_reship_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `read_sholder_hit_count` | 查询节点的文本投影；与同分类状态和日志联合解释 |
| `reset_generation` | 查询节点的文本投影；与同分类状态和日志联合解释 |

原始名称来自本标签 `src/backend/cluster/cluster_debug.c`。监控程序不得把未返回的键补零后认定正常。
