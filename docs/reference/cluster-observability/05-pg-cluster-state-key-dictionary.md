# `pg_cluster_state` 完整键字典

该视图把不同子系统状态统一投影为 `(category, key, value)` 三个 text 字段。
下面逐项覆盖当前实现中的全部静态键；随后列出运行时展开的动态键族。

## `pg_cluster_state`

| 字段 | SQL 类型 | 含义 |
|---|---|---|
| `category` | `text` | 状态所属子系统分类；用于先收窄 owner 与生命周期。 |
| `key` | `text` | 分类内稳定键名；动态键族会把 region、worker、node 或 histogram 边界编码进名称。 |
| `value` | `text` | 查询时格式化的值；`(unset)`、`(null)`、`(empty)` 是文本哨兵，不是 SQL NULL。 |

行基数由编译选项、注册 region、worker 数、节点证据和动态 histogram 共同决定；查询会顺序读取多个子系统，因此全表不是原子 authority snapshot。

## 动态键族

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

## category = `shmem`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `magic` | 枚举/名称/格式化文本 | `magic`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `version_packed` | 枚举/名称/格式化文本 | 版本 / `packed`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `node_id_at_init` | 枚举/名称/格式化文本 | 节点 / `id` / `at` / `init`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `created_at` | 时间戳文本或 `(unset)` | `created`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `region_count` | 十进制整数文本 | `region`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `total_bytes` | 十进制整数文本 | 总量，单位字节。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `guc`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `cluster.config_file` | 枚举/名称/格式化文本 | `config` / `file`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster.injection_points` | 枚举/名称/格式化文本 | `injection` / 点的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster.interconnect_tier` | 枚举/名称/格式化文本 | `interconnect` / `tier`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster.node_id` | 十进制整数文本 | 节点的标识符。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster.shared_storage_backend` | 枚举/名称/格式化文本 | 共享 / 存储 / `backend`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster.smgr_user_relations` | `t` / `f` 文本 | `smgr` / `user` / `relations`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster.shmem_max_regions` | 十进制整数文本 | `shmem` / 上限 / `regions`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster.cf_terminal_authority` | `t` / `f` 文本 | `cf` / `terminal` / `authority`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster.cf_delayed_cleanout` | 枚举/名称/格式化文本 | `cf` / `delayed` / `cleanout`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster.smart_fusion` | `t` / `f` 文本 | `smart` / `fusion`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster.smart_fusion_tier_min` | 枚举/名称/格式化文本 | `smart` / `fusion` / `tier` / `min`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster.smart_fusion_commit_brake_timeout_ms` | 十进制整数文本 | `smart` / `fusion` / 提交 / `brake` / 超时，单位毫秒。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `cluster.smart_fusion_origin_durable_gossip_ms` | 十进制整数文本 | `smart` / `fusion` / 来源 / `durable` / `gossip`，单位毫秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `ic`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `active_tier_name` | 枚举/名称/格式化文本 | 活动 / `tier` / `name`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tier1_listener_pid` | 十进制整数文本 | `tier1` / 监听器 / `pid`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tier1_listener_incarnation` | 十进制整数文本 | `tier1` / 监听器 / `incarnation`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tier1_listener_port` | 十进制整数文本 | `tier1` / 监听器 / `port`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tier1_writable_drain_control` | 十进制整数文本 | `tier1` / `writable` / 排空 / `control`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tier1_writable_drain_data` | 十进制整数文本 | `tier1` / `writable` / 排空 / 数据的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tier1_fifo_admitted_control` | 十进制整数文本 | `tier1` / `fifo` / 已准入 / `control`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tier1_fifo_admitted_data` | 十进制整数文本 | `tier1` / `fifo` / 已准入 / 数据的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tier1_fifo_promoted_control` | 十进制整数文本 | `tier1` / `fifo` / `promoted` / `control`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tier1_fifo_promoted_data` | 十进制整数文本 | `tier1` / `fifo` / `promoted` / 数据的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tier1_send_not_admitted_control` | 十进制整数文本 | `tier1` / 发送 / `not` / 已准入 / `control`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tier1_send_not_admitted_data` | 十进制整数文本 | `tier1` / 发送 / `not` / 已准入 / 数据的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tier1_fifo_dropped_close_control` | 十进制整数文本 | `tier1` / `fifo` / `dropped` / `close` / `control`的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `tier1_fifo_dropped_close_data` | 十进制整数文本 | `tier1` / `fifo` / `dropped` / `close` / 数据的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `peer_capabilities` | 枚举/名称/格式化文本 | 对端 / `capabilities`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `caps_reply_reject_count` | 十进制整数文本 | `caps` / 响应 / `reject`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `inject`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `armed_count` | 十进制整数文本 | `armed`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `conf`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `node_count` | 十进制整数文本 | 节点的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `self_in_topology` | `t` / `f` 文本 | 本节点 / `in` / `topology`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `phase`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `cluster_phase` | 枚举/名称/格式化文本 | `cluster` / 阶段的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `phase_enum_value` | 十进制整数文本 | 阶段 / `enum` / `value`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `phase_started_at` | 时间戳文本或 `(unset)` | 阶段 / 开始的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `phase_elapsed_seconds` | 十进制整数文本 | 阶段 / `elapsed` / `seconds`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `phase_history` | 枚举/名称/格式化文本 | 阶段 / 历史的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `lmon`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `lmon_status` | 枚举/名称/格式化文本 | `lmon` / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lmon_status_enum_value` | 十进制整数文本 | `lmon` / 状态 / `enum` / `value`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lmon_pid` | 十进制整数文本 | `lmon` / `pid`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lmon_spawned_at` | 时间戳文本或 `(unset)` | `lmon` / `spawned`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lmon_ready_at` | 时间戳文本或 `(unset)` | `lmon` / `ready`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lmon_last_liveness_tick_at` | 时间戳文本或 `(unset)` | `lmon` / 最近 / `liveness` / `tick`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lmon_main_loop_iters` | 十进制整数文本 | `lmon` / `main` / `loop` / `iters`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lmon_last_iter_us` | 十进制整数文本 | `lmon` / 最近 / `iter`，单位微秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lmon_max_iter_us` | 十进制整数文本 | `lmon` / 上限 / `iter`，单位微秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lmon_slow_iter_count` | 十进制整数文本 | `lmon` / `slow` / `iter`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lmon_timed_duty_sample_count` | 十进制整数文本 | `lmon` / `timed` / `duty` / `sample`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lmon_total_iter_us` | 十进制整数文本 | `lmon` / 总量 / `iter`，单位微秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `lck`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `lck_status` | 枚举/名称/格式化文本 | `lck` / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lck_status_enum_value` | 十进制整数文本 | `lck` / 状态 / `enum` / `value`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lck_pid` | 十进制整数文本 | `lck` / `pid`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lck_spawned_at` | 时间戳文本或 `(unset)` | `lck` / `spawned`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lck_ready_at` | 时间戳文本或 `(unset)` | `lck` / `ready`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lck_last_liveness_tick_at` | 时间戳文本或 `(unset)` | `lck` / 最近 / `liveness` / `tick`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lck_main_loop_iters` | 十进制整数文本 | `lck` / `main` / `loop` / `iters`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `diag`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `diag_status` | 枚举/名称/格式化文本 | `diag` / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `diag_status_enum_value` | 十进制整数文本 | `diag` / 状态 / `enum` / `value`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `diag_pid` | 十进制整数文本 | `diag` / `pid`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `diag_spawned_at` | 时间戳文本或 `(unset)` | `diag` / `spawned`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `diag_ready_at` | 时间戳文本或 `(unset)` | `diag` / `ready`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `diag_last_liveness_tick_at` | 时间戳文本或 `(unset)` | `diag` / 最近 / `liveness` / `tick`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `diag_main_loop_iters` | 十进制整数文本 | `diag` / `main` / `loop` / `iters`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `hang`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `hang_manager_enabled` | `t` / `f` 文本 | `hang` / `manager` / 启用状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_dump_enabled` | `t` / `f` 文本 | `hang` / `dump` / 启用状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_threshold_ms` | 十进制整数文本 | `hang` / `threshold`，单位毫秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_sample_interval_ms` | 十进制整数文本 | `hang` / `sample` / `interval`，单位毫秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_max_sampled` | 十进制整数文本 | `hang` / 上限 / `sampled`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_available` | `t` / `f` 文本 | `hang` / `available`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_sample_epoch` | 十进制整数文本 | `hang` / `sample` / epoch的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_last_sample_at` | 时间戳文本或 `(unset)` | `hang` / 最近 / `sample`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_last_dump_emitted_at` | 时间戳文本或 `(unset)` | `hang` / 最近 / `dump` / `emitted`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_long_wait_count` | 十进制整数文本 | `hang` / `long` / 等待的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `hang_longest_wait_us` | 十进制整数文本 | `hang` / `longest` / 等待，单位微秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_truncated` | `t` / `f` 文本 | `hang` / `truncated`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_n_samples` | 十进制整数文本 | `hang` / `n` / `samples`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_samples_taken` | 十进制整数文本 | `hang` / `samples` / `taken`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_long_waits_seen` | 十进制整数文本 | `hang` / `long` / `waits` / `seen`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_dumps_emitted` | 十进制整数文本 | `hang` / `dumps` / `emitted`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_incomplete_sample_count` | 十进制整数文本 | `hang` / `incomplete` / `sample`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `hang_excluded_deadlock_count` | 十进制整数文本 | `hang` / `excluded` / `deadlock`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `hang_excluded_idle_count` | 十进制整数文本 | `hang` / `excluded` / `idle`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `hang_excluded_bgworker_count` | 十进制整数文本 | `hang` / `excluded` / `bgworker`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `hang_proc_signal_dump_count` | 十进制整数文本 | `hang` / `proc` / 信号 / `dump`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `hang_error_count` | 十进制整数文本 | `hang` / 错误的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `hang_deadlock_confirmed_count` | 十进制整数文本 | `hang` / `deadlock` / `confirmed`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `hang_cycle_detected_count` | 十进制整数文本 | `hang` / `cycle` / `detected`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `hang_resolution_mode` | 枚举/名称/格式化文本 | `hang` / `resolution` / 模式的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_resolve_evaluations` | 十进制整数文本 | `hang` / `resolve` / `evaluations`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_victims_selected` | 十进制整数文本 | `hang` / `victims` / `selected`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_soft_cancels_issued` | 十进制整数文本 | `hang` / `soft` / `cancels` / `issued`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_terminates_issued` | 十进制整数文本 | `hang` / `terminates` / `issued`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_resolved_confirmed` | 十进制整数文本 | `hang` / `resolved` / `confirmed`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_resolution_failed` | 十进制整数文本 | `hang` / `resolution` / 失败的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `hang_hard_skipped` | 十进制整数文本 | `hang` / `hard` / `skipped`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_non_actionable_skipped` | 十进制整数文本 | `hang` / `non` / `actionable` / `skipped`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_over_excluded` | 十进制整数文本 | `hang` / `over` / `excluded`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_unprovable_root_skipped` | 十进制整数文本 | `hang` / `unprovable` / `root` / `skipped`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_aba_revalidate_failed` | 十进制整数文本 | `hang` / `aba` / `revalidate` / 失败的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `hang_not_confirmed_yet` | 十进制整数文本 | `hang` / `not` / `confirmed` / `yet`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_no_safe_victim` | 十进制整数文本 | `hang` / `no` / `safe` / `victim`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_degraded_to_timeout` | 十进制整数文本 | `hang` / `degraded` / `to` / 超时的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `hang_advisory_recommendations` | 十进制整数文本 | `hang` / `advisory` / `recommendations`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_resolve_last_victim_pid` | 十进制整数文本 | `hang` / `resolve` / 最近 / `victim` / `pid`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_resolve_last_action` | 枚举/名称/格式化文本 | `hang` / `resolve` / 最近 / `action`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_sample%d_pid` | 十进制整数文本 | `hang` / `sample%d` / `pid`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_sample%d_wait_event` | 枚举/名称/格式化文本 | `hang` / `sample%d` / 等待 / 事件的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_sample%d_wait_ms` | 十进制整数文本 | `hang` / `sample%d` / 等待，单位毫秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_sample%d_duration_kind` | 枚举/名称/格式化文本 | `hang` / `sample%d` / 耗时 / `kind`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_sample%d_source` | 枚举/名称/格式化文本 | `hang` / `sample%d` / 来源的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_sample%d_quality` | 枚举/名称/格式化文本 | `hang` / `sample%d` / `quality`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_sample%d_blocker_pid` | 十进制整数文本 | `hang` / `sample%d` / `blocker` / `pid`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_sample%d_blocker_remote_node` | 十进制整数文本 | `hang` / `sample%d` / `blocker` / `remote` / 节点的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hang_sample%d_in_confirmed_deadlock` | `t` / `f` 文本 | `hang` / `sample%d` / `in` / `confirmed` / `deadlock`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `cluster_stats`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `cluster_stats_status` | 枚举/名称/格式化文本 | `cluster` / `stats` / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster_stats_status_enum_value` | 十进制整数文本 | `cluster` / `stats` / 状态 / `enum` / `value`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster_stats_pid` | 十进制整数文本 | `cluster` / `stats` / `pid`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster_stats_spawned_at` | 时间戳文本或 `(unset)` | `cluster` / `stats` / `spawned`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster_stats_ready_at` | 时间戳文本或 `(unset)` | `cluster` / `stats` / `ready`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster_stats_last_liveness_tick_at` | 时间戳文本或 `(unset)` | `cluster` / `stats` / 最近 / `liveness` / `tick`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster_stats_main_loop_iters` | 十进制整数文本 | `cluster` / `stats` / `main` / `loop` / `iters`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `cluster_cssd`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `cluster_cssd_status` | 枚举/名称/格式化文本 | `cluster` / `cssd` / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster_cssd_status_enum_value` | 十进制整数文本 | `cluster` / `cssd` / 状态 / `enum` / `value`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster_cssd_pid` | 十进制整数文本 | `cluster` / `cssd` / `pid`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster_cssd_spawned_at` | 时间戳文本或 `(unset)` | `cluster` / `cssd` / `spawned`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster_cssd_ready_at` | 时间戳文本或 `(unset)` | `cluster` / `cssd` / `ready`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster_cssd_last_liveness_tick_at` | 时间戳文本或 `(unset)` | `cluster` / `cssd` / 最近 / `liveness` / `tick`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cluster_cssd_main_loop_iters` | 十进制整数文本 | `cluster` / `cssd` / `main` / `loop` / `iters`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cssd.declared_alive_count` | 十进制整数文本 | `cssd` / 已声明 / `alive`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cssd.declared_alive_bitmap` | 枚举/名称/格式化文本 | `cssd` / 已声明 / `alive` / 位图的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `undo_cleaner`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `undo_cleaner_status` | 枚举/名称/格式化文本 | undo / `cleaner` / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `undo_cleaner_status_enum_value` | 十进制整数文本 | undo / `cleaner` / 状态 / `enum` / `value`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `undo_cleaner_pid` | 十进制整数文本 | undo / `cleaner` / `pid`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `undo_cleaner_spawned_at` | 时间戳文本或 `(unset)` | undo / `cleaner` / `spawned`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `undo_cleaner_ready_at` | 时间戳文本或 `(unset)` | undo / `cleaner` / `ready`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `undo_cleaner_last_liveness_tick_at` | 时间戳文本或 `(unset)` | undo / `cleaner` / 最近 / `liveness` / `tick`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `undo_cleaner_main_loop_iters` | 十进制整数文本 | undo / `cleaner` / `main` / `loop` / `iters`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `xid_stripe`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `xid_stripe_disk_state` | 十进制整数文本 | `xid` / `stripe` / 磁盘 / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_stripe_slot_state` | 十进制整数文本 | `xid` / `stripe` / 槽位 / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_stripe_activated_floor` | 十进制整数文本 | `xid` / `stripe` / `activated` / `floor`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_stripe_mode_epoch` | 十进制整数文本 | `xid` / `stripe` / 模式 / epoch的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_stripe_my_slot_floor` | 十进制整数文本 | `xid` / `stripe` / `my` / 槽位 / `floor`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_stripe_my_hwm_promise` | 十进制整数文本 | `xid` / `stripe` / `my` / `hwm` / `promise`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_stripe_herding_floor` | 十进制整数文本 | `xid` / `stripe` / `herding` / `floor`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_stripe_cluster_min_hwm` | 十进制整数文本 | `xid` / `stripe` / `cluster` / `min` / `hwm`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_stripe_cluster_max_hwm` | 十进制整数文本 | `xid` / `stripe` / `cluster` / 上限 / `hwm`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_stripe_replay_floor` | 十进制整数文本 | `xid` / `stripe` / `replay` / `floor`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_stripe_replay_active_bitmap` | 十六进制文本 | `xid` / `stripe` / `replay` / 活动 / 位图的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `mxid_stripe_activated_floor` | 十进制整数文本 | `mxid` / `stripe` / `activated` / `floor`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `mxid_stripe_disk_state` | 十进制整数文本 | `mxid` / `stripe` / 磁盘 / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `mxid_stripe_halfspace_refusals` | 十进制整数文本 | `mxid` / `stripe` / `halfspace` / `refusals`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `mxid_stripe_underivable_reads` | 十进制整数文本 | `mxid` / `stripe` / `underivable` / `reads`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `scn`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `scn_node_id` | 十进制整数文本 | SCN / 节点的标识符。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_current_local` | 十进制整数文本 | SCN / 当前 / 本地的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_current_encoded` | 十六进制文本 | SCN / 当前 / `encoded`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_max_observed_remote` | 十进制整数文本 | SCN / 上限 / `observed` / `remote`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_total_advance_count` | 十进制整数文本 | SCN / 总量 / `advance`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `scn_initialized_at` | 时间戳文本或 `(unset)` | SCN / `initialized`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_last_advance_at` | 时间戳文本或 `(unset)` | SCN / 最近 / `advance`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_commit_advance_count` | 十进制整数文本 | SCN / 提交 / `advance`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `scn_abort_advance_count` | 十进制整数文本 | SCN / `abort` / `advance`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `scn_observe_bump_count` | 十进制整数文本 | SCN / `observe` / `bump`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `scn_boc_sweep_count` | 十进制整数文本 | SCN / `boc` / `sweep`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `scn_boc_last_sweep_at` | 时间戳文本或 `(unset)` | SCN / `boc` / 最近 / `sweep`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_boc_pending_at_last_sweep` | 十进制整数文本 | SCN / `boc` / 待处理 / `at` / 最近 / `sweep`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_boc_max_batch_size` | 十进制整数文本 | SCN / `boc` / 上限 / `batch` / 大小的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_boc_broadcast_fanout_count` | 十进制整数文本 | SCN / `boc` / 广播 / `fanout`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `scn_boc_event_publish_count` | 十进制整数文本 | SCN / `boc` / 事件 / `publish`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `scn_boc_sweep_fallback_count` | 十进制整数文本 | SCN / `boc` / `sweep` / 回退的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `scn_commit_lookup_defer_count` | 十进制整数文本 | SCN / 提交 / `lookup` / `defer`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `scn_last_observe_at` | 时间戳文本或 `(unset)` | SCN / 最近 / `observe`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_seconds_since_last_observe` | 枚举/名称/格式化文本 | SCN / `seconds` / 起始时间 / 最近 / `observe`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_observed_max_observe_gap_ms` | 十进制整数文本 | SCN / `observed` / 上限 / `observe` / `gap`，单位毫秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_durable_safe_scn` | 十六进制文本 | SCN / `durable` / `safe`对应的集群 SCN。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_durable_pending_count` | 十进制整数文本 | SCN / `durable` / 待处理的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `scn_durable_frontier_frozen` | 十进制整数文本 | SCN / `durable` / `frontier` / `frozen`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_durable_frontier_overflow_count` | 十进制整数文本 | SCN / `durable` / `frontier` / `overflow`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `scn_durable_frontier_regression_count` | 十进制整数文本 | SCN / `durable` / `frontier` / `regression`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `scn_boc_payload_accept_count` | 十进制整数文本 | SCN / `boc` / `payload` / `accept`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `scn_boc_payload_bad_length_count` | 十进制整数文本 | SCN / `boc` / `payload` / `bad` / `length`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `scn_boc_payload_node_mismatch_count` | 十进制整数文本 | SCN / `boc` / `payload` / 节点 / `mismatch`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `scn_boc_payload_regression_count` | 十进制整数文本 | SCN / `boc` / `payload` / `regression`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `grd`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `grd_shard_count` | 十进制整数文本 | `grd` / 分片的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_local_master_count` | 十进制整数文本 | `grd` / 本地 / master的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_remote_master_count` | 十进制整数文本 | `grd` / `remote` / master的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_shard_lookup_count` | 十进制整数文本 | `grd` / 分片 / `lookup`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_local_master_lookup_count` | 十进制整数文本 | `grd` / 本地 / master / `lookup`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_remote_master_lookup_count` | 十进制整数文本 | `grd` / `remote` / master / `lookup`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_resid_encode_count` | 十进制整数文本 | `grd` / `resid` / `encode`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_master_map_refresh_count` | 十进制整数文本 | `grd` / master / `map` / `refresh`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_max_entries` | 十进制整数文本 | `grd` / 上限 / `entries`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `grd_entry_count` | 十进制整数文本 | `grd` / `entry`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_allocated_bytes` | 十进制整数文本 | `grd` / `allocated`，单位字节。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `grd_entry_create_count` | 十进制整数文本 | `grd` / `entry` / `create`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_entry_lookup_hit_count` | 十进制整数文本 | `grd` / `entry` / `lookup` / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_entry_full_count` | 十进制整数文本 | `grd` / `entry` / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_entries_reclaimed_count` | 十进制整数文本 | `grd` / `entries` / `reclaimed`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_reclaim_skipped_pinned_count` | 十进制整数文本 | `grd` / `reclaim` / `skipped` / `pinned`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_pin_high_water` | 十进制整数文本 | `grd` / `pin` / `high` / `water`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `grd_sweep_runs` | 十进制整数文本 | `grd` / `sweep` / `runs`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `grd_holders_full_count` | 十进制整数文本 | `grd` / `holders` / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_waiters_full_count` | 十进制整数文本 | `grd` / `waiters` / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_converts_full_count` | 十进制整数文本 | `grd` / `converts` / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_convert_granted_inplace_count` | 十进制整数文本 | `grd` / `convert` / 已授予 / `inplace`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_convert_enqueued_count` | 十进制整数文本 | `grd` / `convert` / `enqueued`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_convert_illegal_count` | 十进制整数文本 | `grd` / `convert` / `illegal`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_ngranted_promoted_count` | 十进制整数文本 | `grd` / `ngranted` / `promoted`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_ges_work_queue_full_count` | 十进制整数文本 | `grd` / `ges` / `work` / `queue` / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_ges_cleanup_deferred_count` | 十进制整数文本 | `grd` / `ges` / 清理 / `deferred`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_ges_inbound_validation_fail_count` | 十进制整数文本 | `grd` / `ges` / `inbound` / `validation` / `fail`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `grd_ges_reply_deferred_count` | 十进制整数文本 | `grd` / `ges` / 响应 / `deferred`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_ges_reply_dropped_count` | 十进制整数文本 | `grd` / `ges` / 响应 / `dropped`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `grd_cleanup_skip_stale_cancel_count` | 十进制整数文本 | `grd` / 清理 / `skip` / 陈旧 / `cancel`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `grd_relation_object_cluster_path_count` | 十进制整数文本 | `grd` / `relation` / `object` / `cluster` / `path`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_transaction_cluster_path_count` | 十进制整数文本 | `grd` / `transaction` / `cluster` / `path`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_outbound_ring_depth` | 十进制整数文本 | `grd` / `outbound` / `ring` / `depth`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `grd_outbound_reply_dirty_depth` | 十进制整数文本 | `grd` / `outbound` / 响应 / `dirty` / `depth`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `grd_outbound_cleanup_dirty_depth` | 十进制整数文本 | `grd` / `outbound` / 清理 / `dirty` / `depth`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `grd_outbound_cleanup_retry_warn50_count` | 十进制整数文本 | `grd` / `outbound` / 清理 / 重试 / `warn50`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_outbound_cleanup_retry_warn90_count` | 十进制整数文本 | `grd` / `outbound` / 清理 / 重试 / `warn90`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_work_queue_depth` | 十进制整数文本 | `grd` / `work` / `queue` / `depth`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `grd_pending_count` | 十进制整数文本 | `grd` / 待处理的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_bast_sent_count` | 十进制整数文本 | `grd` / `bast` / `sent`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_bast_received_count` | 十进制整数文本 | `grd` / `bast` / 已接收的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_bast_ack_count` | 十进制整数文本 | `grd` / `bast` / 确认的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_bast_retry_count` | 十进制整数文本 | `grd` / `bast` / 重试的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_bast_reject_count` | 十进制整数文本 | `grd` / `bast` / `reject`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `grd_bast_stale_drop_count` | 十进制整数文本 | `grd` / `bast` / 陈旧 / 丢弃的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `grd_deadlock_probe_drop_count` | 十进制整数文本 | `grd` / `deadlock` / `probe` / 丢弃的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `grd_deadlock_probe_collision_drop_count` | 十进制整数文本 | `grd` / `deadlock` / `probe` / 冲突 / 丢弃的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `grd_deadlock_chunk_oo_buffer_overflow_count` | 十进制整数文本 | `grd` / `deadlock` / 分片 / `oo` / 缓冲 / `overflow`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `grd_starvation_boost_count` | 十进制整数文本 | `grd` / `starvation` / `boost`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_starvation_barrier_enqueued_count` | 十进制整数文本 | `grd` / `starvation` / 屏障 / `enqueued`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `grd_starvation_barrier_publish_fail_count` | 十进制整数文本 | `grd` / `starvation` / 屏障 / `publish` / `fail`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `grd_starvation_max_skip_observed` | 十进制整数文本 | `grd` / `starvation` / 上限 / `skip` / `observed`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `grd_recovery`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `state` | 枚举/名称/格式化文本 | 该对象当前状态；枚举域由所属视图定义。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `state_enum_value` | 十进制整数文本 | 状态 / `enum` / `value`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `last_event_id` | 十进制整数文本 | 最近 / 事件的标识符。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `event_old_epoch` | 十进制整数文本 | 事件 / `old` / epoch的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `episode_epoch` | 十进制整数文本 | `episode` / epoch的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `event_coordinator` | 十进制整数文本 | 事件 / `coordinator`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `done_self_epoch` | 十进制整数文本 | `done` / 本节点 / epoch的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `done_self_bitmap_hash` | 十进制整数文本 | `done` / 本节点 / 位图 / `hash`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `block_redeclare_cursor` | 十进制整数文本 | 数据块 / `redeclare` / `cursor`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `block_redeclare_epoch` | 十进制整数文本 | 数据块 / `redeclare` / epoch的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `block_redeclare_done` | `t` / `f` 文本 | 数据块 / `redeclare` / `done`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `remaster_started` | 十进制整数文本 | remaster / 开始的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `remaster_done` | 十进制整数文本 | remaster / `done`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `remaster_failed` | 十进制整数文本 | remaster / 失败的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `shards_remastered` | 十进制整数文本 | `shards` / `remastered`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `holders_redeclared` | 十进制整数文本 | `holders` / `redeclared`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `holders_rebound` | 十进制整数文本 | `holders` / `rebound`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `waiters_requeued` | 十进制整数文本 | `waiters` / `requeued`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `converts_requeued` | 十进制整数文本 | `converts` / `requeued`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `stale_request_drop` | 十进制整数文本 | 陈旧 / 请求 / 丢弃的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `rebuild_timeout` | 十进制整数文本 | `rebuild` / 超时的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `block_path_failclosed` | 十进制整数文本 | 数据块 / `path` / fail-closed的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `unaffected_holder_survived` | 十进制整数文本 | `unaffected` / 持有者 / `survived`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `stale_holder_swept` | 十进制整数文本 | 陈旧 / 持有者 / `swept`的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `cluster_gate_timeout` | 十进制整数文本 | `cluster` / `gate` / 超时的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `wait_epoch_escape` | 十进制整数文本 | 等待 / epoch / `escape`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `join_remaster_started` | 十进制整数文本 | 加入 / remaster / 开始的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `join_remaster_done` | 十进制整数文本 | 加入 / remaster / `done`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `join_shards_remastered` | 十进制整数文本 | 加入 / `shards` / `remastered`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `join_block_views_rebuilt` | 十进制整数文本 | 加入 / 数据块 / `views` / `rebuilt`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `join_block_recovering_failclosed` | 十进制整数文本 | 加入 / 数据块 / `recovering` / fail-closed的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `offpath_crash_rejoin_fenced` | 十进制整数文本 | `offpath` / `crash` / `rejoin` / `fenced`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `pcm`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `dead_cleanup_entries` | 十进制整数文本 | 失效 / 清理 / `entries`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `pcm_grd_max_entries` | 十进制整数文本 | `pcm` / `grd` / 上限 / `entries`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `pcm_grd_allocated_bytes` | 十进制整数文本 | `pcm` / `grd` / `allocated`，单位字节。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `pcm_grd_active_entries` | 十进制整数文本 | `pcm` / `grd` / 活动 / `entries`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `pcm_lock_mode_count` | 十进制整数文本 | `pcm` / 锁 / 模式的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `pcm_transition_count` | 十进制整数文本 | `pcm` / 转换的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `pcm_api_state` | 枚举/名称/格式化文本 | `pcm` / `api` / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `resource_x_proof_readiness` | 枚举/名称/格式化文本 | `resource` / `x` / `proof` / `readiness`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `remote_install_observed_count` | 十进制整数文本 | `remote` / `install` / `observed`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `remote_grant_after_image_count` | 十进制整数文本 | `remote` / `grant` / `after` / `image`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `remote_image_at_or_after_grant_count` | 十进制整数文本 | `remote` / `image` / `at` / `or` / `after` / `grant`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `remote_episode_excluded_no_install` | 十进制整数文本 | `remote` / `episode` / `excluded` / `no` / `install`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `remote_episode_excluded_missing_grant` | 十进制整数文本 | `remote` / `episode` / `excluded` / `missing` / `grant`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `remote_episode_excluded_missing_image` | 十进制整数文本 | `remote` / `episode` / `excluded` / `missing` / `image`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `last_remote_t_image_us` | 十进制整数文本 | 最近 / `remote` / `t` / `image`，单位微秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `last_remote_t_grant_us` | 十进制整数文本 | 最近 / `remote` / `t` / `grant`，单位微秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `last_remote_t_install_us` | 十进制整数文本 | 最近 / `remote` / `t` / `install`，单位微秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `master_state_n_count` | 十进制整数文本 | master / 状态 / `n`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `master_state_s_count` | 十进制整数文本 | master / 状态 / `s`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `master_state_x_count` | 十进制整数文本 | master / 状态 / `x`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `pi_holders_total_count` | 十进制整数文本 | `pi` / `holders` / 总量的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `convert_queue_active` | 十进制整数文本 | `convert` / `queue` / 活动的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `trans_n_to_s_count` | 十进制整数文本 | `trans` / `n` / `to` / `s`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `trans_n_to_x_count` | 十进制整数文本 | `trans` / `n` / `to` / `x`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `trans_s_to_x_upgrade_count` | 十进制整数文本 | `trans` / `s` / `to` / `x` / `upgrade`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `trans_x_to_s_downgrade_count` | 十进制整数文本 | `trans` / `x` / `to` / `s` / `downgrade`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `trans_x_to_n_downgrade_count` | 十进制整数文本 | `trans` / `x` / `to` / `n` / `downgrade`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `trans_x_to_n_release_count` | 十进制整数文本 | `trans` / `x` / `to` / `n` / `release`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `trans_s_to_n_invalidate_count` | 十进制整数文本 | `trans` / `s` / `to` / `n` / `invalidate`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `trans_s_to_n_release_count` | 十进制整数文本 | `trans` / `s` / `to` / `n` / `release`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `trans_s_to_x_cleanout_count` | 十进制整数文本 | `trans` / `s` / `to` / `x` / `cleanout`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `local_s_revoke_nonholder_failclosed_count` | 十进制整数文本 | 本地 / `s` / `revoke` / `nonholder` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `evict_release_deferred_aux_count` | 十进制整数文本 | `evict` / `release` / `deferred` / `aux`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `writer_cover_stale_detected_count` | 十进制整数文本 | `writer` / `cover` / 陈旧 / `detected`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `writer_reverify_reacquire_count` | 十进制整数文本 | `writer` / `reverify` / `reacquire`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `restore_aba_detected_count` | 十进制整数文本 | 恢复 / `aba` / `detected`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `invalidate_parked_grant_pending_count` | 十进制整数文本 | `invalidate` / `parked` / `grant` / 待处理的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `wm_prov_insert_fail_count` | 十进制整数文本 | `wm` / `prov` / `insert` / `fail`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `lms`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `lms_state` | 枚举/名称/格式化文本 | `lms` / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lms_started_count` | 十进制整数文本 | `lms` / 开始的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lms_work_drained_count` | 十进制整数文本 | `lms` / `work` / `drained`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lms_decision_grant_count` | 十进制整数文本 | `lms` / `decision` / `grant`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lms_decision_reject_count` | 十进制整数文本 | `lms` / `decision` / `reject`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `lms_decision_convert_count` | 十进制整数文本 | `lms` / `decision` / `convert`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lms_drain_empty_count` | 十进制整数文本 | `lms` / 排空 / `empty`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lms_error_count` | 十进制整数文本 | `lms` / 错误的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `native_probe_sent_count` | 十进制整数文本 | `native` / `probe` / `sent`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `native_probe_reply_recv_count` | 十进制整数文本 | `native` / `probe` / 响应 / 接收的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `native_probe_collector_slot_full_count` | 十进制整数文本 | `native` / `probe` / `collector` / 槽位 / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `native_probe_aggregate_holder_conflict_count` | 十进制整数文本 | `native` / `probe` / `aggregate` / 持有者 / `conflict`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `native_probe_aggregate_waiter_conflict_count` | 十进制整数文本 | `native` / `probe` / `aggregate` / `waiter` / `conflict`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `native_probe_retry_count` | 十进制整数文本 | `native` / `probe` / 重试的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `native_probe_timeout_count` | 十进制整数文本 | `native` / `probe` / 超时的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `priority_starvation_observed_count` | 十进制整数文本 | `priority` / `starvation` / `observed`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lms_data_dispatch_count` | 十进制整数文本 | `lms` / 数据 / `dispatch`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lms_direct_reply_count` | 十进制整数文本 | `lms` / `direct` / 响应的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lms_conn_reset_count` | 十进制整数文本 | `lms` / `conn` / `reset`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lms_inline_serve_count` | 十进制整数文本 | `lms` / 内联 / `serve`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lms_outbound_not_admitted_count` | 十进制整数文本 | `lms` / `outbound` / `not` / 已准入的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lms_outbound_requeue_drop_count` | 十进制整数文本 | `lms` / `outbound` / `requeue` / 丢弃的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `lms_outbound_cap_guard_drop_count` | 十进制整数文本 | `lms` / `outbound` / `cap` / `guard` / 丢弃的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `lmd`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `lmd_state` | 枚举/名称/格式化文本 | `lmd` / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lmd_started_count` | 十进制整数文本 | `lmd` / 开始的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lmd_ready_at_us` | 十进制整数文本 | `lmd` / `ready` / `at`，单位微秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lmd_edge_submission_count` | 十进制整数文本 | `lmd` / `edge` / `submission`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lmd_wake_count` | 十进制整数文本 | `lmd` / 唤醒的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lmd_idle_count` | 十进制整数文本 | `lmd` / `idle`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lmd_error_count` | 十进制整数文本 | `lmd` / 错误的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `wait_edge_count` | 十进制整数文本 | 等待 / `edge`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `wait_edge_full_count` | 十进制整数文本 | 等待 / `edge` / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `graph_generation` | 十进制整数文本 | `graph` / 代际的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tarjan_scan_count` | 十进制整数文本 | `tarjan` / `scan`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cycle_detected_count` | 十进制整数文本 | `cycle` / `detected`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `victim_cancel_sent_count` | 十进制整数文本 | `victim` / `cancel` / `sent`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `revalidate_fail_count` | 十进制整数文本 | `revalidate` / `fail`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `pcm_convert_wfg_replace_count` | 十进制整数文本 | `pcm` / `convert` / `wfg` / `replace`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `pcm_convert_wfg_remove_count` | 十进制整数文本 | `pcm` / `convert` / `wfg` / `remove`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `pcm_convert_wfg_replace_fail_count` | 十进制整数文本 | `pcm` / `convert` / `wfg` / `replace` / `fail`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `pcm_convert_wfg_exact_remove_stale_count` | 十进制整数文本 | `pcm` / `convert` / `wfg` / `exact` / `remove` / 陈旧的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `cross_node_victim_pending_count` | 十进制整数文本 | `cross` / 节点 / `victim` / 待处理的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `inject_call_count` | 十进制整数文本 | `inject` / `call`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `probe_broadcast_count` | 十进制整数文本 | `probe` / 广播的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `probe_partial_count` | 十进制整数文本 | `probe` / `partial`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cleanup_lmd_sweep_count` | 十进制整数文本 | 清理 / `lmd` / `sweep`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cleanup_on_backend_exit_count` | 十进制整数文本 | 清理 / `on` / `backend` / `exit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cleanup_skip_other_owner_count` | 十进制整数文本 | 清理 / `skip` / `other` / owner的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cross_node_cancel_queue_full_count` | 十进制整数文本 | `cross` / 节点 / `cancel` / `queue` / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cross_node_cancel_received_count` | 十进制整数文本 | `cross` / 节点 / `cancel` / 已接收的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cross_node_victim_cancel_sent_count` | 十进制整数文本 | `cross` / 节点 / `victim` / `cancel` / `sent`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `deadlock_confirmed_count` | 十进制整数文本 | `deadlock` / `confirmed`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `confirm_unconfirmed_count` | 十进制整数文本 | `confirm` / `unconfirmed`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `reconfig_discard_count` | 十进制整数文本 | 重配置 / `discard`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `member_incomplete_count` | 十进制整数文本 | `member` / `incomplete`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `victim_protected_skip_count` | 十进制整数文本 | `victim` / `protected` / `skip`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `victim_repeat_avoided_count` | 十进制整数文本 | `victim` / `repeat` / `avoided`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cancel_token_installed_count` | 十进制整数文本 | `cancel` / `token` / `installed`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cancel_consumed_count` | 十进制整数文本 | `cancel` / `consumed`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cancel_stale_cleared_count` | 十进制整数文本 | `cancel` / 陈旧 / `cleared`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `cancel_wait_stale_rejected_count` | 十进制整数文本 | `cancel` / 等待 / 陈旧 / 拒绝的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `cancel_ack_received_count` | 十进制整数文本 | `cancel` / 确认 / 已接收的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cancel_retransmit_count` | 十进制整数文本 | `cancel` / `retransmit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cancel_escalated_alternate_count` | 十进制整数文本 | `cancel` / `escalated` / `alternate`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cancel_exhausted_timeout_count` | 十进制整数文本 | `cancel` / `exhausted` / 超时的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `cancel_no_safe_victim_count` | 十进制整数文本 | `cancel` / `no` / `safe` / `victim`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cleanup_orphan_edge_swept_count` | 十进制整数文本 | 清理 / `orphan` / `edge` / `swept`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `reconfig_cancel_discarded_count` | 十进制整数文本 | 重配置 / `cancel` / `discarded`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cancel_ack_mismatch_count` | 十进制整数文本 | `cancel` / 确认 / `mismatch`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `probe_report_enqueue_count` | 十进制整数文本 | `probe` / `report` / `enqueue`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `probe_drop_stale_count` | 十进制整数文本 | `probe` / 丢弃 / 陈旧的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `probe_drop_duplicate_count` | 十进制整数文本 | `probe` / 丢弃 / `duplicate`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `probe_queue_full_count` | 十进制整数文本 | `probe` / `queue` / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `probe_partial_report_count` | 十进制整数文本 | `probe` / `partial` / `report`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `advisory`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `advisory_globalize_count` | 十进制整数文本 | `advisory` / `globalize`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `advisory_session_release_count` | 十进制整数文本 | `advisory` / 会话 / `release`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `advisory_try_grant_count` | 十进制整数文本 | `advisory` / `try` / `grant`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `advisory_try_notavail_count` | 十进制整数文本 | `advisory` / `try` / `notavail`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `advisory_failclosed_count` | 十进制整数文本 | `advisory` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `reconfig_touched`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `abort_count` | 十进制整数文本 | `abort`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `stamp_count` | 十进制整数文本 | `stamp`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `stamp_ges` | 十进制整数文本 | `stamp` / `ges`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `stamp_gcs_block` | 十进制整数文本 | `stamp` / `gcs` / 数据块的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `stamp_scn` | 十进制整数文本 | `stamp`对应的集群 SCN。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `stamp_vis` | 十进制整数文本 | `stamp` / `vis`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `stamp_sinval` | 十进制整数文本 | `stamp` / `sinval`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `clean_leave_rejected` | 十进制整数文本 | 干净 / `leave` / 拒绝的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `self_touched_hex` | 枚举/名称/格式化文本 | 本节点 / `touched` / `hex`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `reconfig_join`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `join_pending_count` | 十进制整数文本 | 加入 / 待处理的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `join_apply_count` | 十进制整数文本 | 加入 / 应用的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `join_reject_count` | 十进制整数文本 | 加入 / `reject`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `join_timeout_count` | 十进制整数文本 | 加入 / 超时的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `clean_departed_cleared_count` | 十进制整数文本 | 干净 / `departed` / `cleared`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `reconfig`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `marker_slow_ack_count` | 十进制整数文本 | `marker` / `slow` / 确认的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `marker_timeout_count` | 十进制整数文本 | `marker` / 超时的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `ges`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `ges_request_defer_count` | 十进制整数文本 | `ges` / 请求 / `defer`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `ges_reply_defer_count` | 十进制整数文本 | `ges` / 响应 / `defer`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `ges_reply_wait_table_active` | 十进制整数文本 | `ges` / 响应 / 等待 / `table` / 活动的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `ges_reply_late_drop_count` | 十进制整数文本 | `ges` / 响应 / `late` / 丢弃的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `ges_release_ack_count` | 十进制整数文本 | `ges` / `release` / 确认的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tx_enqueue_wait_count` | 十进制整数文本 | `tx` / `enqueue` / 等待的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tx_enqueue_wakeup_count` | 十进制整数文本 | `tx` / `enqueue` / `wakeup`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tx_enqueue_timeout_count` | 十进制整数文本 | `tx` / `enqueue` / 超时的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `ges_timeout_true_wait_count` | 十进制整数文本 | `ges` / 超时 / `true` / 等待的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `ges_timeout_capacity_count` | 十进制整数文本 | `ges` / 超时 / `capacity`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `ges_timeout_send_fail_count` | 十进制整数文本 | `ges` / 超时 / 发送 / `fail`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `ges_timeout_retransmit_exhausted_count` | 十进制整数文本 | `ges` / 超时 / `retransmit` / `exhausted`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `ges_timeout_native_probe_count` | 十进制整数文本 | `ges` / 超时 / `native` / `probe`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `ges_timeout_master_reject_count` | 十进制整数文本 | `ges` / 超时 / master / `reject`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `shared_fs`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `active_backend` | 枚举/名称/格式化文本 | 活动 / `backend`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `registered_backends` | 枚举/名称/格式化文本 | `registered` / `backends`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `smgr_user_relations` | `t` / `f` 文本 | `smgr` / `user` / `relations`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `smgr_active_relations` | 十进制整数文本 | `smgr` / 活动 / `relations`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `smgr_inval_bcast_sent_count` | 十进制整数文本 | `smgr` / `inval` / `bcast` / `sent`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `block_format`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `page_layout_version` | 十进制整数文本 | 页面 / `layout` / 版本的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `page_header_size` | 十进制整数文本 | 页面 / `header` / 大小的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_size_bytes` | 十进制整数文本 | SCN / 大小，单位字节。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `invalid_scn_value` | 枚举/名称/格式化文本 | 无效 / SCN / `value`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `itl_slot_size_bytes` | 十进制整数文本 | `itl` / 槽位 / 大小，单位字节。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `itl_initrans_default` | 十进制整数文本 | `itl` / `initrans` / `default`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `itl_array_bytes` | 十进制整数文本 | `itl` / `array`，单位字节。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tuple_header_extra_bytes` | 枚举/名称/格式化文本 | `tuple` / `header` / `extra`，单位字节。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `itl_location` | 枚举/名称/格式化文本 | `itl` / `location`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `buffer_format`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `buffer_desc_size_bytes` | 十进制整数文本 | 缓冲 / `desc` / 大小，单位字节。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `buffer_desc_pad_to_size` | 十进制整数文本 | 缓冲 / `desc` / `pad` / `to` / 大小的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `buffer_hot_field_offset` | 十进制整数文本 | 缓冲 / `hot` / `field` / `offset`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `buffer_cold_field_offset` | 十进制整数文本 | 缓冲 / `cold` / `field` / `offset`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `buffer_type_count` | 十进制整数文本 | 缓冲 / `type`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `pcm_state_count` | 十进制整数文本 | `pcm` / 状态的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `cf`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `cf_x_acquire` | 十进制整数文本 | `cf` / `x` / `acquire`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cf_s_acquire` | 十进制整数文本 | `cf` / `s` / `acquire`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cf_failclosed` | 十进制整数文本 | `cf` / fail-closed的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `cf_single_node_authority` | 十进制整数文本 | `cf` / `single` / 节点 / `authority`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cf_bak_fallback` | 十进制整数文本 | `cf` / `bak` / 回退的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `recovery_anchor_write_count` | 十进制整数文本 | 恢复 / `anchor` / 写入的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `recovery_anchor_boot_adopt_count` | 十进制整数文本 | 恢复 / `anchor` / 启动 / `adopt`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `smart_fusion`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `dep_install_count` | 十进制整数文本 | `dep` / `install`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `dep_touch_count` | 十进制整数文本 | `dep` / `touch`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `dbwr_brake_count` | 十进制整数文本 | `dbwr` / `brake`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `commit_brake_count` | 十进制整数文本 | 提交 / `brake`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `commit_brake_wait_us` | 十进制整数文本 | 提交 / `brake` / 等待，单位微秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `origin_suspect_count` | 十进制整数文本 | 来源 / `suspect`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `dep_lost_failclosed_count` | 十进制整数文本 | `dep` / `lost` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `retry_failclosed_count` | 十进制整数文本 | 重试 / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `gcs`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `api_state` | 枚举/名称/格式化文本 | `api` / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lookup_master_self_count` | 十进制整数文本 | `lookup` / master / 本节点的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lookup_master_remote_count` | 十进制整数文本 | `lookup` / master / `remote`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `send_request_count` | 十进制整数文本 | 发送 / 请求的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `handle_request_count` | 十进制整数文本 | `handle` / 请求的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `handle_reply_count` | 十进制整数文本 | `handle` / 响应的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `reply_late_drop_count` | 十进制整数文本 | 响应 / `late` / 丢弃的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `reply_timeout_count` | 十进制整数文本 | 响应 / 超时的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `encode_payload_bytes` | 十进制整数文本 | `encode` / `payload`，单位字节。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `decode_payload_bytes` | 十进制整数文本 | `decode` / `payload`，单位字节。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `dispatch_loop_iterations` | 十进制整数文本 | `dispatch` / `loop` / `iterations`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `outstanding_count` | 十进制整数文本 | `outstanding`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `max_outstanding` | 十进制整数文本 | 上限 / `outstanding`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `max_outstanding_per_backend` | 十进制整数文本 | 上限 / `outstanding` / 每 / `backend`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `block_request_count` | 十进制整数文本 | 数据块 / 请求的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_reply_count` | 十进制整数文本 | 数据块 / 响应的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_timeout_count` | 十进制整数文本 | 数据块 / 超时的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `block_checksum_fail_count` | 十进制整数文本 | 数据块 / `checksum` / `fail`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `block_storage_fallback_count` | 十进制整数文本 | 数据块 / 存储 / 回退的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_master_not_holder_count` | 十进制整数文本 | 数据块 / master / `not` / 持有者的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_wal_flush_before_ship_count` | 十进制整数文本 | 数据块 / WAL / 刷写 / `before` / `ship`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_ship_bytes_total` | 十进制整数文本 | 数据块 / `ship` / 字节 / 总量的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `block_family_plane` | 十进制整数文本 | 数据块 / `family` / `plane`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `plane_misroute_reject` | 十进制整数文本 | `plane` / `misroute` / `reject`的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `scratch_copy_count` | 十进制整数文本 | `scratch` / `copy`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `live_sge_send_count` | 十进制整数文本 | `live` / `sge` / 发送的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `live_sge_fallback_count` | 十进制整数文本 | `live` / `sge` / 回退的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `direct_install_count` | 十进制整数文本 | `direct` / `install`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `direct_install_abort_count` | 十进制整数文本 | `direct` / `install` / `abort`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `install_copy_count` | 十进制整数文本 | `install` / `copy`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `retransmit_attempt_count` | 十进制整数文本 | `retransmit` / `attempt`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `retransmit_send_count` | 十进制整数文本 | `retransmit` / 发送的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `retransmit_exhausted_count` | 十进制整数文本 | `retransmit` / `exhausted`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `dedup_hit_count` | 十进制整数文本 | 去重 / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `dedup_miss_count` | 十进制整数文本 | 去重 / `miss`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `dedup_collision_count` | 十进制整数文本 | 去重 / 冲突的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `dedup_full_count` | 十进制整数文本 | 去重 / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `dedup_entry_count` | 十进制整数文本 | 去重 / `entry`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `dedup_evict_count` | 十进制整数文本 | 去重 / `evict`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `dedup_max_entries` | 十进制整数文本 | 去重 / 上限 / `entries`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `dedup_misroute_failclosed_count` | 十进制整数文本 | 去重 / `misroute` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `epoch_invalidate_wake_count` | 十进制整数文本 | epoch / `invalidate` / 唤醒的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `stale_reply_drop_count` | 十进制整数文本 | 陈旧 / 响应 / 丢弃的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `done_sent_count` | 十进制整数文本 | `done` / `sent`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `dedup_done_marked_count` | 十进制整数文本 | 去重 / `done` / `marked`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `dedup_done_mismatch_count` | 十进制整数文本 | 去重 / `done` / `mismatch`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `dedup_hint_violation_count` | 十进制整数文本 | 去重 / `hint` / `violation`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `dedup_legacy_pin_count` | 十进制整数文本 | 去重 / `legacy` / `pin`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `done_enqueue_drop_count` | 十进制整数文本 | `done` / `enqueue` / 丢弃的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `reply_send_queued_count` | 十进制整数文本 | 响应 / 发送 / `queued`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `reply_send_not_admitted_count` | 十进制整数文本 | 响应 / 发送 / `not` / 已准入的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `forward_send_queued_count` | 十进制整数文本 | `forward` / 发送 / `queued`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `forward_send_not_admitted_count` | 十进制整数文本 | `forward` / 发送 / `not` / 已准入的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `invalidate_send_queued_count` | 十进制整数文本 | `invalidate` / 发送 / `queued`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `invalidate_send_not_admitted_count` | 十进制整数文本 | `invalidate` / 发送 / `not` / 已准入的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `invalidate_parked_count` | 十进制整数文本 | `invalidate` / `parked`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `invalidate_busy_sent_count` | 十进制整数文本 | `invalidate` / `busy` / `sent`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `invalidate_busy_received_count` | 十进制整数文本 | `invalidate` / `busy` / 已接收的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `invalidate_passive_s_release_count` | 十进制整数文本 | `invalidate` / `passive` / `s` / `release`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `pcm_x_self_handoff_count` | 十进制整数文本 | `pcm` / `x` / 本节点 / `handoff`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `pcm_x_self_handoff_drain_count` | 十进制整数文本 | `pcm` / `x` / 本节点 / `handoff` / 排空的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `invalidate_park_expired_count` | 十进制整数文本 | `invalidate` / `park` / `expired`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `invalidate_park_overflow_count` | 十进制整数文本 | `invalidate` / `park` / `overflow`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `drop_pinned_deny_count` | 十进制整数文本 | 丢弃 / `pinned` / `deny`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `xfer_stale_deny_count` | 十进制整数文本 | `xfer` / 陈旧 / `deny`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `fallback_scn_verify_pass_count` | 十进制整数文本 | 回退 / SCN / `verify` / `pass`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `fallback_scn_refresh_count` | 十进制整数文本 | 回退 / SCN / `refresh`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `fallback_scn_failclosed_count` | 十进制整数文本 | 回退 / SCN / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `block_forward_sent_count` | 十进制整数文本 | 数据块 / `forward` / `sent`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_forward_received_count` | 十进制整数文本 | 数据块 / `forward` / 已接收的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_from_holder_ship_count` | 十进制整数文本 | 数据块 / `from` / 持有者 / `ship`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_forward_holder_evicted_count` | 十进制整数文本 | 数据块 / `forward` / 持有者 / `evicted`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `s_holders_bitmap_redirect_count` | 十进制整数文本 | `s` / `holders` / 位图 / `redirect`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `master_holder_lifecycle_count` | 十进制整数文本 | master / 持有者 / `lifecycle`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `forward_replay_count` | 十进制整数文本 | `forward` / `replay`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_invalidate_broadcast_count` | 十进制整数文本 | 数据块 / `invalidate` / 广播的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_invalidate_ack_received_count` | 十进制整数文本 | 数据块 / `invalidate` / 确认 / 已接收的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_invalidate_timeout_count` | 十进制整数文本 | 数据块 / `invalidate` / 超时的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `block_x_forward_sent_count` | 十进制整数文本 | 数据块 / `x` / `forward` / `sent`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_x_granted_from_holder_count` | 十进制整数文本 | 数据块 / `x` / 已授予 / `from` / 持有者的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `starvation_denied_pending_x_count` | 十进制整数文本 | `starvation` / `denied` / 待处理 / `x`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `local_s_upgrade_grant_count` | 十进制整数文本 | 本地 / `s` / `upgrade` / `grant`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `x_vs_s_nonholder_grant_count` | 十进制整数文本 | `x` / `vs` / `s` / `nonholder` / `grant`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `x_vs_s_no_carrier_denied_count` | 十进制整数文本 | `x` / `vs` / `s` / `no` / `carrier` / `denied`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `pi_watermark_advance_count` | 十进制整数文本 | `pi` / 水位 / `advance`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `pi_watermark_retire_count` | 十进制整数文本 | `pi` / 水位 / `retire`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `pi_master_metadata_retire_count` | 十进制整数文本 | `pi` / master / `metadata` / `retire`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `pi_durable_note_apply_count` | 十进制整数文本 | `pi` / `durable` / `note` / 应用的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lost_write_detected_count` | 十进制整数文本 | `lost` / 写入 / `detected`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lost_write_avoid_count` | 十进制整数文本 | `lost` / 写入 / `avoid`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lost_write_invalidscn_failclosed_count` | 十进制整数文本 | `lost` / 写入 / `invalidscn` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `lost_write_not_scn_tracked_skip_count` | 十进制整数文本 | `lost` / 写入 / `not` / SCN / `tracked` / `skip`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lost_write_master_direct_storage_fallback_count` | 十进制整数文本 | `lost` / 写入 / master / `direct` / 存储 / 回退的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cf_xheld_read_ship_count` | 十进制整数文本 | `cf` / `xheld` / 读取 / `ship`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_x_transfer_ship_count` | 十进制整数文本 | 数据块 / `x` / `transfer` / `ship`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_x_self_ship_count` | 十进制整数文本 | 数据块 / `x` / 本节点 / `ship`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `clean_page_xfer_count` | 十进制整数文本 | 干净 / 页面 / `xfer`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `clean_page_xfer_storage_fallback_count` | 十进制整数文本 | 干净 / 页面 / `xfer` / 存储 / 回退的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `clean_page_xfer_fail_closed_count` | 十进制整数文本 | 干净 / 页面 / `xfer` / `fail` / 关闭的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `clean_page_xfer_stale_holder_recover_count` | 十进制整数文本 | 干净 / 页面 / `xfer` / 陈旧 / 持有者 / `recover`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `clean_page_xfer_third_party_denied_count` | 十进制整数文本 | 干净 / 页面 / `xfer` / `third` / `party` / `denied`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `sequence`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `sq_refill_count` | 十进制整数文本 | `sq` / `refill`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `sq_refill_wait_count` | 十进制整数文本 | `sq` / `refill` / 等待的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `sq_dup_guard_fail_count` | 十进制整数文本 | `sq` / `dup` / `guard` / `fail`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `sq_failover_fail_closed_count` | 十进制整数文本 | `sq` / `failover` / `fail` / 关闭的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `sq_page_writeback_count` | 十进制整数文本 | `sq` / 页面 / `writeback`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `sq_cycle_rejected_count` | 十进制整数文本 | `sq` / `cycle` / 拒绝的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `gcs_recovery`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `block_resources_recovering` | 十进制整数文本 | 数据块 / `resources` / `recovering`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `buffers_redeclared` | 十进制整数文本 | `buffers` / `redeclared`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `block_state_rebuilt` | 十进制整数文本 | 数据块 / 状态 / `rebuilt`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `redo_boundary_waits` | 十进制整数文本 | redo / `boundary` / `waits`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `redo_boundary_reached` | 十进制整数文本 | redo / `boundary` / `reached`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `redo_coverage_required_lsn_zero_count` | 十进制整数文本 | redo / `coverage` / `required` / `lsn` / `zero`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `redo_coverage_gate_block_count` | 十进制整数文本 | redo / `coverage` / `gate` / 数据块的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `stale_block_drop` | 十进制整数文本 | 陈旧 / 数据块 / 丢弃的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `ambiguous_owner_failclosed` | 十进制整数文本 | `ambiguous` / owner / fail-closed的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `before_boundary_failclosed` | 十进制整数文本 | `before` / `boundary` / fail-closed的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `tt_recovery`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `active_slots_resolved_aborted` | 十进制整数文本 | 活动 / `slots` / `resolved` / `aborted`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `remote_active_failclosed` | 十进制整数文本 | `remote` / 活动 / fail-closed的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `wrap_generation_disambiguated` | 十进制整数文本 | `wrap` / 代际 / `disambiguated`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `recycled_liveness_relaxed` | 十进制整数文本 | `recycled` / `liveness` / `relaxed`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `scn_highwater_recovered` | 十进制整数文本 | SCN / `highwater` / `recovered`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `recovery_verdict_failclosed` | 十进制整数文本 | 恢复 / `verdict` / fail-closed的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `heap_tuples_physically_reverted` | 十进制整数文本 | `heap` / `tuples` / `physically` / `reverted`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `undo_revert_failclosed` | 十进制整数文本 | undo / `revert` / fail-closed的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `sinval`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `broadcast_send_count` | 十进制整数文本 | 广播 / 发送的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `broadcast_receive_count` | 十进制整数文本 | 广播 / 接收的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `inject_local_queue_count` | 十进制整数文本 | `inject` / 本地 / `queue`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `outbound_queue_full_count` | 十进制整数文本 | `outbound` / `queue` / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `inbound_queue_full_count` | 十进制整数文本 | `inbound` / `queue` / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `inbound_overflow_reset_count` | 十进制整数文本 | `inbound` / `overflow` / `reset`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `validation_drop_count` | 十进制整数文本 | `validation` / 丢弃的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `stale_epoch_drop_count` | 十进制整数文本 | 陈旧 / epoch / 丢弃的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `echo_dropped_count` | 十进制整数文本 | `echo` / `dropped`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `fanout_would_block_count` | 十进制整数文本 | `fanout` / `would` / 数据块的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `fanout_hard_error_count` | 十进制整数文本 | `fanout` / `hard` / 错误的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `fanout_peer_down_count` | 十进制整数文本 | `fanout` / 对端 / `down`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `ack_received_count` | 十进制整数文本 | 确认 / 已接收的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `ack_timeout_count` | 十进制整数文本 | 确认 / 超时的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `ack_orphan_count` | 十进制整数文本 | 确认 / `orphan`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `smgr_inval_applied_count` | 十进制整数文本 | `smgr` / `inval` / `applied`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `tt_status`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `install_count` | 十进制整数文本 | `install`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lookup_hit_count` | 十进制整数文本 | `lookup` / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lookup_miss_count` | 十进制整数文本 | `lookup` / `miss`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `evict_count` | 十进制整数文本 | `evict`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `flush_count` | 十进制整数文本 | 刷写的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `self_consumer_hit_count` | 十进制整数文本 | 本节点 / `consumer` / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `evict_fail_count` | 十进制整数文本 | `evict` / `fail`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `tt_status_hint`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `emit_count` | 十进制整数文本 | `emit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `receive_count` | 十进制整数文本 | 接收的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `drop_invalid_count` | 十进制整数文本 | 丢弃 / 无效的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `drop_stale_epoch_count` | 十进制整数文本 | 丢弃 / 陈旧 / epoch的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `drop_unknown_version_count` | 十进制整数文本 | 丢弃 / 未知 / 版本的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `install_count` | 十进制整数文本 | `install`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `drop_v1_compat_count` | 十进制整数文本 | 丢弃 / `v1` / `compat`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `recovery`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `recovery_undo_redo_applies` | 十进制整数文本 | 恢复 / undo / redo / `applies`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `recovery_undo_redo_skips` | 十进制整数文本 | 恢复 / undo / redo / `skips`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `recovery_2pc_standby_rebuilds` | 十进制整数文本 | 恢复 / `2pc` / `standby` / `rebuilds`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `recovery_overlay_rebuild_count` | 十进制整数文本 | 恢复 / `overlay` / `rebuild`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_recovery_blocks_recovered` | 十进制整数文本 | 数据块 / 恢复 / `blocks` / `recovered`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `block_recovery_failclosed` | 十进制整数文本 | 数据块 / 恢复 / fail-closed的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `thread_recovery_state` | 枚举/名称/格式化文本 | 线程 / 恢复 / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `thread_recovery_threads_recovered` | 十进制整数文本 | 线程 / 恢复 / `threads` / `recovered`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `thread_recovery_replay_failclosed` | 十进制整数文本 | 线程 / 恢复 / `replay` / fail-closed的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `thread_recovery_recovered_through_lsn` | 十六进制文本 | 线程 / 恢复 / `recovered` / `through`对应的 WAL LSN。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `plan_state` | 枚举/名称/格式化文本 | `plan` / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `plan_generated_at` | 时间戳文本或 `(unset)` | `plan` / `generated`的时间戳。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `plan_own_thread` | 十进制整数文本 | `plan` / `own` / 线程的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `plan_threads_scanned` | 十进制整数文本 | `plan` / `threads` / `scanned`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `plan_crashed_candidates` | 枚举/名称/格式化文本 | `plan` / `crashed` / `candidates`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `plan_n_clean` | 十进制整数文本 | `plan` / `n` / 干净的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `plan_n_empty` | 十进制整数文本 | `plan` / `n` / `empty`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `plan_n_crashed_candidate` | 十进制整数文本 | `plan` / `n` / `crashed` / 候选的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `plan_n_alive` | 十进制整数文本 | `plan` / `n` / `alive`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `plan_n_unknown` | 十进制整数文本 | `plan` / `n` / 未知的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `plan_unknown_threads` | 枚举/名称/格式化文本 | `plan` / 未知 / `threads`的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `plan_dbstate_at_startup` | 枚举/名称/格式化文本 | `plan` / `dbstate` / `at` / 启动的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `plan_local_recovery_needed` | `t` / `f` 文本 | `plan` / 本地 / 恢复 / `needed`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `worker_pool_state` | 枚举/名称/格式化文本 | worker / `pool` / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `worker_generation` | 十进制整数文本 | worker / 代际的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `workers_requested` | 十进制整数文本 | `workers` / `requested`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `workers_started` | 十进制整数文本 | `workers` / 开始的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `workers_done` | 十进制整数文本 | `workers` / `done`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `workers_failed` | 十进制整数文本 | `workers` / 失败的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `stream_ok_threads` | 枚举/名称/格式化文本 | `stream` / 正常 / `threads`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `stream_suspect_or_unreadable_threads` | 枚举/名称/格式化文本 | `stream` / `suspect` / `or` / `unreadable` / `threads`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `merged_records_applied` | 十进制整数文本 | `merged` / `records` / `applied`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `merged_skipped_local` | 十进制整数文本 | `merged` / `skipped` / 本地的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `merged_own_bound_skips` | 十进制整数文本 | `merged` / `own` / `bound` / `skips`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `materialized_remote_instances` | 枚举/名称/格式化文本 | `materialized` / `remote` / `instances`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `remote_uba_resolved` | 十进制整数文本 | `remote` / `uba` / `resolved`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `remote_outcome_committed` | 十进制整数文本 | `remote` / `outcome` / 已提交的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `remote_outcome_aborted` | 十进制整数文本 | `remote` / `outcome` / `aborted`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `remote_authority_53ra` | 十进制整数文本 | `remote` / `authority` / `53ra`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `tt_2pc`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `twopc_prepare_records` | 十进制整数文本 | `twopc` / `prepare` / `records`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `twopc_prepare_undo_flushes` | 十进制整数文本 | `twopc` / `prepare` / undo / `flushes`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `twopc_postprepare_transfers` | 十进制整数文本 | `twopc` / `postprepare` / `transfers`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `twopc_prefinish_commits` | 十进制整数文本 | `twopc` / `prefinish` / `commits`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `twopc_prefinish_aborts` | 十进制整数文本 | `twopc` / `prefinish` / `aborts`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `twopc_recover_rebinds` | 十进制整数文本 | `twopc` / `recover` / `rebinds`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `visibility`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `vis_update_fork_count` | 十进制整数文本 | `vis` / `update` / `fork`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis_dirty_fork_count` | 十进制整数文本 | `vis` / `dirty` / `fork`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis_selftoast_fork_count` | 十进制整数文本 | `vis` / `selftoast` / `fork`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis_conflict_failclosed_count` | 十进制整数文本 | `vis` / `conflict` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `prune_remote_keep_count` | 十进制整数文本 | `prune` / `remote` / `keep`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis_variant_unknown_failclosed_count` | 十进制整数文本 | `vis` / `variant` / 未知 / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `writer_chain_resolved_count` | 十进制整数文本 | `writer` / `chain` / `resolved`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `writer_chain_failclosed_count` | 十进制整数文本 | `writer` / `chain` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `xmax_resolved_count` | 十进制整数文本 | `xmax` / `resolved`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `overlay_refresh_count` | 十进制整数文本 | `overlay` / `refresh`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `covers_scn_refuse_count` | 十进制整数文本 | `covers` / SCN / `refuse`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `undo`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `record_alloc_count` | 十进制整数文本 | `record` / `alloc`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `segment_claim_count` | 十进制整数文本 | `segment` / `claim`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `extent_claim_count` | 十进制整数文本 | `extent` / `claim`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `undo_buf_hit_count` | 十进制整数文本 | undo / `buf` / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `undo_buf_miss_count` | 十进制整数文本 | undo / `buf` / `miss`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `undo_buf_writeback_count` | 十进制整数文本 | undo / `buf` / `writeback`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_write_count` | 十进制整数文本 | 数据块 / 写入的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `block_flush_count` | 十进制整数文本 | 数据块 / 刷写的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `reader_lookup_count` | 十进制整数文本 | `reader` / `lookup`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `autoextend_count` | 十进制整数文本 | `autoextend`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `segment_switch_count` | 十进制整数文本 | `segment` / `switch`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `segment_create_fail_count` | 十进制整数文本 | `segment` / `create` / `fail`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `segment_hard_cap_fail_count` | 十进制整数文本 | `segment` / `hard` / `cap` / `fail`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `segment_observation_status` | 枚举/名称/格式化文本 | `segment` / `observation` / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `segment_allocated_count` | 十进制整数文本 | `segment` / `allocated`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `segment_allocated_high_water` | 十进制整数文本 | `segment` / `allocated` / `high` / `water`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `segment_effective_cap` | 十进制整数文本 | `segment` / `effective` / `cap`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `record_segments_committed` | 十进制整数文本 | `record` / `segments` / 已提交的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `record_seg_commit_skipped_inflight` | 十进制整数文本 | `record` / `seg` / 提交 / `skipped` / 在途的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `record_seg_residual_revalidate_drops` | 十进制整数文本 | `record` / `seg` / `residual` / `revalidate` / `drops`的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `commit_fsync_count` | 十进制整数文本 | 提交 / `fsync`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `commit_fsync_segment_count` | 十进制整数文本 | 提交 / `fsync` / `segment`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `commit_fsync_failure_count` | 十进制整数文本 | 提交 / `fsync` / `failure`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `smgr_open_count` | 十进制整数文本 | `smgr` / 开放的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `smgr_close_count` | 十进制整数文本 | `smgr` / `close`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `smgr_pread_count` | 十进制整数文本 | `smgr` / `pread`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `smgr_pwrite_count` | 十进制整数文本 | `smgr` / `pwrite`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tt_durable_commit_count` | 十进制整数文本 | `tt` / `durable` / 提交的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tt_durable_lookup_hit_count` | 十进制整数文本 | `tt` / `durable` / `lookup` / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tt_durable_lookup_miss_count` | 十进制整数文本 | `tt` / `durable` / `lookup` / `miss`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tt_durable_by_xid_scan_count` | 十进制整数文本 | `tt` / `durable` / `by` / `xid` / `scan`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tt_durable_redo_apply_count` | 十进制整数文本 | `tt` / `durable` / redo / 应用的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tt_inventory_hit_count` | 十进制整数文本或 `unavailable` | 实际开始的有限段集合扫描遍数，不表示找到了事务。 | 同窗口 `fallback_count / hit_count` 表示有限扫描回退比例；分母为零时不可计算。 |
| `tt_inventory_full_scan_count` | 十进制整数文本或 `unavailable` | 实际开始的全范围扫描遍数，包括首次、不可用作用域和有限扫描回退。 | 不能把全扫次数直接当作回退次数；失败扫描也计数。 |
| `tt_inventory_disabled_count` | 十进制整数文本或 `unavailable` | 本共享内存代次中位图首次进入 disabled 的次数。 | 同一代次重复禁用不重复计数，共享内存重建后归零。 |
| `tt_inventory_bitmap_zero_count` | 十进制整数文本或 `unavailable` | 运行时完整性核验通过后，有限扫描完成的零匹配结果数。 | 汇总 resolve 和 locate 两个入口；零匹配不等于事务已提交或已回收。 |
| `tt_inventory_full_zero_count` | 十进制整数文本或 `unavailable` | 全范围扫描完成的零匹配结果数。 | 不包含扫描不完整、读失败或歧义结果；与有限扫描零匹配分开计数。 |
| `tt_inventory_fallback_count` | 十进制整数文本或 `unavailable` | 有限扫描失去完整性证明后，改为全范围重扫的次数。 | 每次调用最多重扫一次；不包含直接从全范围开始的扫描。各键独立采样，比较同代次时间窗增量。 |
| `retention_horizon_scn` | 十进制整数文本 | 保留 / `horizon`对应的集群 SCN。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `retention_max_recycle_horizon` | 十进制整数文本 | 保留 / 上限 / `recycle` / `horizon`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `tt_slot_retain_skip_count` | 十进制整数文本 | `tt` / 槽位 / `retain` / `skip`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `segment_retain_skip_count` | 十进制整数文本 | `segment` / `retain` / `skip`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `retention_recycle_count` | 十进制整数文本 | 保留 / `recycle`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `retention_off_recycle_count` | 十进制整数文本 | 保留 / `off` / `recycle`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tt_retention_rollover_count` | 十进制整数文本 | `tt` / 保留 / `rollover`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tt_rollover_fail_hard_cap_count` | 十进制整数文本 | `tt` / `rollover` / `fail` / `hard` / `cap`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `tt_rollover_fail_extend_count` | 十进制整数文本 | `tt` / `rollover` / `fail` / `extend`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `tt_rollover_fail_activate_count` | 十进制整数文本 | `tt` / `rollover` / `fail` / `activate`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `terminal_authority_check_count` | 十进制整数文本 | `terminal` / `authority` / `check`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `terminal_authority_ok_count` | 十进制整数文本 | `terminal` / `authority` / 正常的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `terminal_authority_failclosed_count` | 十进制整数文本 | `terminal` / `authority` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `terminal_authority_epoch_failclosed_count` | 十进制整数文本 | `terminal` / `authority` / epoch / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `terminal_authority_ownership_failclosed_count` | 十进制整数文本 | `terminal` / `authority` / `ownership` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `terminal_authority_unknown_failclosed_count` | 十进制整数文本 | `terminal` / `authority` / 未知 / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `terminal_authority_nonterminal_failclosed_count` | 十进制整数文本 | `terminal` / `authority` / `nonterminal` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `terminal_authority_durable_failclosed_count` | 十进制整数文本 | `terminal` / `authority` / `durable` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `terminal_authority_retention_failclosed_count` | 十进制整数文本 | `terminal` / `authority` / 保留 / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `cleaner_pass_count` | 十进制整数文本 | `cleaner` / `pass`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cleaner_shmem_tt_slots_gcd` | 十进制整数文本 | `cleaner` / `shmem` / `tt` / `slots` / `gcd`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cleaner_segments_marked_recyclable` | 十进制整数文本 | `cleaner` / `segments` / `marked` / `recyclable`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cleaner_stale_active_skipped` | 十进制整数文本 | `cleaner` / 陈旧 / 活动 / `skipped`的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `segment_reuse_count` | 十进制整数文本 | `segment` / `reuse`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tt_slot_wrap_retired_count` | 十进制整数文本 | `tt` / 槽位 / `wrap` / `retired`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `horizon_stall_count` | 十进制整数文本 | `horizon` / `stall`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `horizon_peer_stale_count` | 十进制整数文本 | `horizon` / 对端 / 陈旧的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `horizon_pass_abort_count` | 十进制整数文本 | `horizon` / `pass` / `abort`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `horizon_wire_reject_count` | 十进制整数文本 | `horizon` / `wire` / `reject`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `horizon_admission_refuse_count` | 十进制整数文本 | `horizon` / 准入 / `refuse`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `horizon_last_floor_scn` | 十进制整数文本 | `horizon` / 最近 / `floor`对应的集群 SCN。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `horizon_idle_sentinel_sent_count` | 十进制整数文本 | `horizon` / `idle` / `sentinel` / `sent`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `horizon_peer_reports` | 枚举/名称/格式化文本 | `horizon` / 对端 / `reports`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cleaner_header_tt_slots_below_horizon` | 十进制整数文本 | `cleaner` / `header` / `tt` / `slots` / `below` / `horizon`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `undo_buf_held_wal` | 十进制整数文本 | undo / `buf` / `held` / WAL的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `undo_buf_held_evidence` | 十进制整数文本 | undo / `buf` / `held` / `evidence`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `undo_buf_boundary_violations` | 十进制整数文本 | undo / `buf` / `boundary` / `violations`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `undo_buf_remote_evidence_holds` | 十进制整数文本 | undo / `buf` / `remote` / `evidence` / `holds`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `undo_gcs_grant_shared_count` | 十进制整数文本 | undo / `gcs` / `grant` / 共享的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `undo_gcs_grant_exclusive_count` | 十进制整数文本 | undo / `gcs` / `grant` / `exclusive`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `undo_gcs_ship_bytes` | 十进制整数文本 | undo / `gcs` / `ship`，单位字节。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `undo_gcs_invalidate_notify_count` | 十进制整数文本 | undo / `gcs` / `invalidate` / `notify`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `undo_gcs_remaster_deny_count` | 十进制整数文本 | undo / `gcs` / remaster / `deny`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `undo_gcs_local_fast_path_count` | 十进制整数文本 | undo / `gcs` / 本地 / `fast` / `path`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `r4`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `cr_route_started_count` | 十进制整数文本 | `cr` / `route` / 开始的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_holder_full_count` | 十进制整数文本 | `cr` / 持有者 / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_holder_retry_count` | 十进制整数文本 | `cr` / 持有者 / 重试的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_holder_failclosed_count` | 十进制整数文本 | `cr` / 持有者 / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `undo_data_fetch_served_count` | 十进制整数文本 | undo / 数据 / `fetch` / `served`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `undo_data_fetch_denied_count` | 十进制整数文本 | undo / 数据 / `fetch` / `denied`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tx_resolve_unknown_count` | 十进制整数文本 | `tx` / `resolve` / 未知的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `tx_resolve_in_progress_count` | 十进制整数文本 | `tx` / `resolve` / `in` / 进度的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tx_resolve_prepared_count` | 十进制整数文本 | `tx` / `resolve` / 已准备的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tx_resolve_committed_count` | 十进制整数文本 | `tx` / `resolve` / 已提交的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `tx_resolve_aborted_count` | 十进制整数文本 | `tx` / `resolve` / `aborted`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `multi_resolve_served_count` | 十进制整数文本 | `multi` / `resolve` / `served`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `multi_resolve_unknown_count` | 十进制整数文本 | `multi` / `resolve` / 未知的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `slot_capacity_retry_count` | 十进制整数文本 | 槽位 / `capacity` / 重试的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `cr`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `cr_construct_count` | 十进制整数文本 | `cr` / `construct`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_snapshot_too_old_count` | 十进制整数文本 | `cr` / `snapshot` / `too` / `old`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_cross_instance_unsupported_count` | 十进制整数文本 | `cr` / `cross` / `instance` / `unsupported`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_corruption_count` | 十进制整数文本 | `cr` / `corruption`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_chain_walk_steps_sum` | 十进制整数文本 | `cr` / `chain` / `walk` / `steps` / `sum`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `cr_inverse_insert_count` | 十进制整数文本 | `cr` / `inverse` / `insert`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_inverse_update_count` | 十进制整数文本 | `cr` / `inverse` / `update`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_inverse_delete_count` | 十进制整数文本 | `cr` / `inverse` / `delete`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_inverse_itl_count` | 十进制整数文本 | `cr` / `inverse` / `itl`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_cache_hit_count` | 十进制整数文本 | `cr` / 缓存 / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_cache_miss_count` | 十进制整数文本 | `cr` / 缓存 / `miss`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_cache_evict_count` | 十进制整数文本 | `cr` / 缓存 / `evict`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_cache_install_count` | 十进制整数文本 | `cr` / 缓存 / `install`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_remote_full_count` | 十进制整数文本 | `cr` / `remote` / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_remote_partial_count` | 十进制整数文本 | `cr` / `remote` / `partial`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_remote_failed_count` | 十进制整数文本 | `cr` / `remote` / 失败的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `cr_server_full_count` | 十进制整数文本 | `cr` / `server` / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_server_partial_count` | 十进制整数文本 | `cr` / `server` / `partial`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_server_denied_count` | 十进制整数文本 | `cr` / `server` / `denied`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `rtvis_undo_fetch_wire_count` | 十进制整数文本 | `rtvis` / undo / `fetch` / `wire`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `rtvis_undo_fetch_cache_hit_count` | 十进制整数文本 | `rtvis` / undo / `fetch` / 缓存 / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `rtvis_undo_fetch_failclosed_count` | 十进制整数文本 | `rtvis` / undo / `fetch` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `cr_server_undo_served_count` | 十进制整数文本 | `cr` / `server` / undo / `served`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_server_undo_denied_count` | 十进制整数文本 | `cr` / `server` / undo / `denied`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `rtvis_resolve_committed_count` | 十进制整数文本 | `rtvis` / `resolve` / 已提交的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `rtvis_resolve_aborted_count` | 十进制整数文本 | `rtvis` / `resolve` / `aborted`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `rtvis_resolve_failclosed_count` | 十进制整数文本 | `rtvis` / `resolve` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `rtvis_verdict_wire_count` | 十进制整数文本 | `rtvis` / `verdict` / `wire`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `rtvis_verdict_failclosed_count` | 十进制整数文本 | `rtvis` / `verdict` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `rtvis_verdict_exact_count` | 十进制整数文本 | `rtvis` / `verdict` / `exact`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `rtvis_verdict_below_horizon_count` | 十进制整数文本 | `rtvis` / `verdict` / `below` / `horizon`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `rtvis_verdict_inadmissible_count` | 十进制整数文本 | `rtvis` / `verdict` / `inadmissible`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_server_verdict_served_count` | 十进制整数文本 | `cr` / `server` / `verdict` / `served`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_server_verdict_denied_count` | 十进制整数文本 | `cr` / `server` / `verdict` / `denied`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_server_multi_verdict_served_count` | 十进制整数文本 | `cr` / `server` / `multi` / `verdict` / `served`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_server_multi_verdict_denied_count` | 十进制整数文本 | `cr` / `server` / `multi` / `verdict` / `denied`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_server_fence_refused_count` | 十进制整数文本 | `cr` / `server` / 隔离 / `refused`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `rtvis_underivable_failclosed_count` | 十进制整数文本 | `rtvis` / `underivable` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `rtvis_native_prehistory_local_count` | 十进制整数文本 | `rtvis` / `native` / `prehistory` / 本地的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis_freshref_verdict_resolved_count` | 十进制整数文本 | `vis` / `freshref` / `verdict` / `resolved`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis_freshref_verdict_failclosed_count` | 十进制整数文本 | `vis` / `freshref` / `verdict` / fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `undo_authority_serve_hit_count` | 十进制整数文本 | undo / `authority` / `serve` / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `undo_authority_fail_closed_count` | 十进制整数文本 | undo / `authority` / `fail` / 关闭的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `undo_authority_epoch_stale_reject_count` | 十进制整数文本 | undo / `authority` / epoch / 陈旧 / `reject`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `undo_authority_scan_incomplete_reject_count` | 十进制整数文本 | undo / `authority` / `scan` / `incomplete` / `reject`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `undo_authority_multi_match_reject_count` | 十进制整数文本 | undo / `authority` / `multi` / `match` / `reject`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `vis53r97_leg_invalid_scn_refuse_count` | 十进制整数文本 | `vis53r97` / `leg` / 无效 / SCN / `refuse`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis53r97_leg_zero_match_refuse_count` | 十进制整数文本 | `vis53r97` / `leg` / `zero` / `match` / `refuse`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis53r97_leg_srv_other_refuse_count` | 十进制整数文本 | `vis53r97` / `leg` / `srv` / `other` / `refuse`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis53r97_leg_covers_refuse_count` | 十进制整数文本 | `vis53r97` / `leg` / `covers` / `refuse`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis53r97_leg_multi_unresolvable_count` | 十进制整数文本 | `vis53r97` / `leg` / `multi` / `unresolvable`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis53r97_leg_xmax_unprovable_count` | 十进制整数文本 | `vis53r97` / `leg` / `xmax` / `unprovable`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis53r97_leg_xmin_overlay_verdict_ask_count` | 十进制整数文本 | `vis53r97` / `leg` / `xmin` / `overlay` / `verdict` / `ask`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis53r97_leg_xmin_overlay_verdict_hit_count` | 十进制整数文本 | `vis53r97` / `leg` / `xmin` / `overlay` / `verdict` / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis53r97_leg_multi_member_serve_ask_count` | 十进制整数文本 | `vis53r97` / `leg` / `multi` / `member` / `serve` / `ask`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis53r97_leg_multi_member_serve_hit_count` | 十进制整数文本 | `vis53r97` / `leg` / `multi` / `member` / `serve` / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis53r97_leg_live_upgrade_hit_count` | 十进制整数文本 | `vis53r97` / `leg` / `live` / `upgrade` / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_xmax_resolved_count` | 十进制整数文本 | `cr` / `xmax` / `resolved`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_xmax_recycled_invisible_count` | 十进制整数文本 | `cr` / `xmax` / `recycled` / `invisible`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_xmax_invalid_or_ambiguous_count` | 十进制整数文本 | `cr` / `xmax` / 无效 / `or` / `ambiguous`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_xmax_scan_unavail_or_no_proof_count` | 十进制整数文本 | `cr` / `xmax` / `scan` / `unavail` / `or` / `no` / `proof`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_key_mismatch_count` | 十进制整数文本 | `cr` / `key` / `mismatch`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_epoch_mismatch_count` | 十进制整数文本 | `cr` / epoch / `mismatch`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_generation_mismatch_count` | 十进制整数文本 | `cr` / 代际 / `mismatch`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_base_lsn_mismatch_count` | 十进制整数文本 | `cr` / `base` / `lsn` / `mismatch`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_locator_reuse_reject_count` | 十进制整数文本 | `cr` / `locator` / `reuse` / `reject`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `cr_tuple_verdict_count` | 十进制整数文本 | `cr` / `tuple` / `verdict`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_tuple_fallback_remote_count` | 十进制整数文本 | `cr` / `tuple` / 回退 / `remote`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_tuple_fallback_recycle_wm_count` | 十进制整数文本 | `cr` / `tuple` / 回退 / `recycle` / `wm`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_tuple_fallback_multichain_count` | 十进制整数文本 | `cr` / `tuple` / 回退 / `multichain`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_tuple_fallback_cliff_count` | 十进制整数文本 | `cr` / `tuple` / 回退 / `cliff`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_tuple_fallback_identity_count` | 十进制整数文本 | `cr` / `tuple` / 回退 / `identity`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_tuple_fallback_cross_block_count` | 十进制整数文本 | `cr` / `tuple` / 回退 / `cross` / 数据块的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_tuple_fallback_uncertain_count` | 十进制整数文本 | `cr` / `tuple` / 回退 / `uncertain`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_global_epoch_fallback_bump_count` | 十进制整数文本 | `cr` / `global` / epoch / 回退 / `bump`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_rel_gen_bump_count` | 十进制整数文本 | `cr` / `rel` / `gen` / `bump`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_rel_gen_table_overflow_count` | 十进制整数文本 | `cr` / `rel` / `gen` / `table` / `overflow`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `cr_retention_horizon_advance_noted_count` | 十进制整数文本 | `cr` / 保留 / `horizon` / `advance` / `noted`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `cr_reconfig_intra_survived_count` | 十进制整数文本 | `cr` / 重配置 / `intra` / `survived`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `cr_pool`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `current_epoch` | 十进制整数文本 | 当前 / epoch的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `live_entries` | 十进制整数文本 | `live` / `entries`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hit_count` | 十进制整数文本 | `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `miss_count` | 十进制整数文本 | `miss`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `reserve_count` | 十进制整数文本 | `reserve`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `publish_count` | 十进制整数文本 | `publish`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `abort_count` | 十进制整数文本 | `abort`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `evict_count` | 十进制整数文本 | `evict`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `epoch_bump_count` | 十进制整数文本 | epoch / `bump`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `publish_stale_release_count` | 十进制整数文本 | `publish` / 陈旧 / `release`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `admit_count` | 十进制整数文本 | `admit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `admit_reject_no_admit` | 十进制整数文本 | `admit` / `reject` / `no` / `admit`的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `admit_reject_bulk` | 十进制整数文本 | `admit` / `reject` / `bulk`的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `admit_reject_parallel` | 十进制整数文本 | `admit` / `reject` / 并行的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `admit_reject_nonmain_fork` | 十进制整数文本 | `admit` / `reject` / `nonmain` / `fork`的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `admit_reject_volatile` | 十进制整数文本 | `admit` / `reject` / `volatile`的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `admit_reject_relcap` | 十进制整数文本 | `admit` / `reject` / `relcap`的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `admit_reject_pressure` | 十进制整数文本 | `admit` / `reject` / `pressure`的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `resolver_cache`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `lookup` | 十进制整数文本 | `lookup`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `key_present` | 十进制整数文本 | `key` / `present`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `epoch_miss` | 十进制整数文本 | epoch / `miss`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `hit` | 十进制整数文本 | `hit`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `revalidate_miss` | 十进制整数文本 | `revalidate` / `miss`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `acceptance_pass` | 十进制整数文本 | `acceptance` / `pass`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `acceptance_failclosed` | 十进制整数文本 | `acceptance` / fail-closed的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `install` | 十进制整数文本 | `install`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `evict` | 十进制整数文本 | `evict`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `nonown_skip` | 十进制整数文本 | `nonown` / `skip`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `nonterminal_skip` | 十进制整数文本 | `nonterminal` / `skip`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `live_entries` | 十进制整数文本 | `live` / `entries`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `wal_thread`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `thread_id` | 十进制整数文本 | 线程的标识符。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `dir_configured` | `t` / `f` 文本 | `dir` / `configured`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `dir_validated` | `t` / `f` 文本 | `dir` / `validated`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `claim_created` | `t` / `f` 文本 | `claim` / `created`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `page_stamp_count` | 十进制整数文本 | 页面 / `stamp`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `registry_ready` | `t` / `f` 文本 | `registry` / `ready`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `registry_slot_state` | 枚举/名称/格式化文本 | `registry` / 槽位 / 状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `registry_last_updated` | 十进制整数文本 | `registry` / 最近 / `updated`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `registry_highest_lsn` | 十六进制文本 | `registry` / `highest`对应的 WAL LSN。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `registry_highest_scn` | 十进制整数文本 | `registry` / `highest`对应的集群 SCN。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `write_fence`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `hot_gate_blocked` | 十进制整数文本 | `hot` / `gate` / 阻塞的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `durable_check_blocked` | 十进制整数文本 | `durable` / `check` / 阻塞的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `minority_marker_ignored` | 十进制整数文本 | `minority` / `marker` / `ignored`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `marker_write_failed` | 十进制整数文本 | `marker` / 写入 / 失败的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `baseline_published` | 十进制整数文本 | `baseline` / `published`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `baseline_stale_rejected` | 十进制整数文本 | `baseline` / 陈旧 / 拒绝的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `baseline_author_is_self` | 十进制整数文本 | `baseline` / `author` / `is` / 本节点的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `baseline_authority_age_us` | 十进制整数文本 | `baseline` / `authority` / `age`，单位微秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `external_admit_requested` | 十进制整数文本 | `external` / `admit` / `requested`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `external_write_excluded` | 十进制整数文本 | `external` / 写入 / `excluded`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `external_rejected` | 十进制整数文本 | `external` / 拒绝的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `external_unknown` | 十进制整数文本 | `external` / 未知的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `external_unavailable` | 十进制整数文本 | `external` / `unavailable`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `external_identity_mismatch` | 十进制整数文本 | `external` / `identity` / `mismatch`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `external_expired` | 十进制整数文本 | `external` / `expired`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `external_daemon_disconnect` | 十进制整数文本 | `external` / `daemon` / `disconnect`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `external_mutation_gate_blocked` | 十进制整数文本 | `external` / `mutation` / `gate` / 阻塞的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `external_publish_gate_blocked` | 十进制整数文本 | `external` / `publish` / `gate` / 阻塞的当前快照值。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `external_last_journal_seq` | 十进制整数文本 | `external` / 最近 / `journal` / `seq`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `external_last_proof_age_ms` | 十进制整数文本 | `external` / 最近 / `proof` / `age`，单位毫秒。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `hw`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `alloc_count` | 十进制整数文本 | `alloc`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `authority_create_count` | 十进制整数文本 | `authority` / `create`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `reserve_wal_count` | 十进制整数文本 | `reserve` / WAL的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `rebuild_count` | 十进制整数文本 | `rebuild`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `failclosed_count` | 十进制整数文本 | fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `not_ready_count` | 十进制整数文本 | `not` / `ready`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `remaster_done_count` | 十进制整数文本 | remaster / `done`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `remaster_blocked_count` | 十进制整数文本 | remaster / 阻塞的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `remaster_retry_count` | 十进制整数文本 | remaster / 重试的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `remaster_retry_exhausted_count` | 十进制整数文本 | remaster / 重试 / `exhausted`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `remaster_recoverable` | 十进制整数文本 | remaster / `recoverable`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lease_leased_total` | 十进制整数文本 | 租约 / `leased` / 总量的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lease_consumed` | 十进制整数文本 | 租约 / `consumed`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lease_orphan_zero` | 十进制整数文本 | 租约 / `orphan` / `zero`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lease_grants` | 十进制整数文本 | 租约 / `grants`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `lease_outstanding` | 十进制整数文本 | 租约 / `outstanding`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |

## category = `dl`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `lease_count` | 十进制整数文本 | 租约的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `native_count` | 十进制整数文本 | `native`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `failclosed_count` | 十进制整数文本 | fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `release_count` | 十进制整数文本 | `release`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `ir`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `recovery_serial_grant_count` | 十进制整数文本 | 恢复 / `serial` / `grant`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `recovery_serial_busy_count` | 十进制整数文本 | 恢复 / `serial` / `busy`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `recovery_serial_retry_count` | 十进制整数文本 | 恢复 / `serial` / 重试的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `recovery_serial_revalidate_reject_count` | 十进制整数文本 | 恢复 / `serial` / `revalidate` / `reject`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `recovery_serial_node_cleanup_wait_count` | 十进制整数文本 | 恢复 / `serial` / 节点 / 清理 / 等待的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `recovery_serial_release_confirmed_count` | 十进制整数文本 | 恢复 / `serial` / `release` / `confirmed`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `recovery_serial_release_unconfirmed_count` | 十进制整数文本 | 恢复 / `serial` / `release` / `unconfirmed`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `recovery_serial_cold_set_grant_count` | 十进制整数文本 | 恢复 / `serial` / `cold` / 集合 / `grant`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `recovery_serial_capability_denied_count` | 十进制整数文本 | 恢复 / `serial` / `capability` / `denied`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `recovery_serial_native_result_rejected_count` | 十进制整数文本 | 恢复 / `serial` / `native` / `result` / 拒绝的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `ts`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `x_count` | 十进制整数文本 | `x`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `s_count` | 十进制整数文本 | `s`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `native_count` | 十进制整数文本 | `native`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `failclosed_count` | 十进制整数文本 | fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |

## category = `ko`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `flush_count` | 十进制整数文本 | 刷写的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `ack_received_count` | 十进制整数文本 | 确认 / 已接收的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `failclosed_count` | 十进制整数文本 | fail-closed的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `native_count` | 十进制整数文本 | `native`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `lockfail_count` | 十进制整数文本 | `lockfail`的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `peer_apply_count` | 十进制整数文本 | 对端 / 应用的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `inbound_full_count` | 十进制整数文本 | `inbound` / `full`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `xnode_profile`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `reset_generation` | 十进制整数文本 | `reset` / 代际的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `read_reship_count` | 十进制整数文本 | 读取 / `reship`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `read_sholder_hit_count` | 十进制整数文本 | 读取 / `sholder` / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `hw_extend_local_count` | 十进制整数文本 | `hw` / `extend` / 本地的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `hw_extend_remote_count` | 十进制整数文本 | `hw` / `extend` / `remote`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |

## category = `catalog`

| key | 值形态 | 含义 | 运维解释 |
|---|---|---|---|
| `shared_catalog_enabled` | `t` / `f` 文本 | 共享 / `catalog` / 启用状态的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `oid_lease_acquire_count` | 十进制整数文本 | `oid` / 租约 / `acquire`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `oid_lease_remaining` | 十进制整数文本 | `oid` / 租约 / 剩余的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `recovery_side_effect_record_count` | 十进制整数文本 | 恢复 / `side` / `effect` / `record`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `recovery_side_effect_drop_count` | 十进制整数文本 | 恢复 / `side` / `effect` / 丢弃的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `relmap_shared_committed_generation` | 十进制整数文本 | `relmap` / 共享 / 已提交 / 代际的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `relmap_shared_pending_generation` | 十进制整数文本 | `relmap` / 共享 / 待处理 / 代际的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `relmap_shared_pending_owner_node` | 十进制整数文本 | `relmap` / 共享 / 待处理 / owner / 节点的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_authority_native_hw` | 十进制整数文本 | `xid` / `authority` / `native` / `hw`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_authority_sealed` | `t` / `f` 文本 | `xid` / `authority` / `sealed`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_prehistory_adopted` | `t` / `f` 文本 | `xid` / `prehistory` / `adopted`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_native_prehistory_covered_hw` | 十进制整数文本 | `xid` / `native` / `prehistory` / `covered` / `hw`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_native_raw_reused` | `t` / `f` 文本 | `xid` / `native` / `raw` / `reused`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_native_prehistory_disabled` | `t` / `f` 文本 | `xid` / `native` / `prehistory` / `disabled`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_wrap_barrier_done` | `t` / `f` 文本 | `xid` / `wrap` / 屏障 / `done`的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `xid_epoch_gate_admitted` | `t` / `f` 文本 | `xid` / epoch / `gate` / 已准入的当前快照值。 | 与同分类其他键组成快照，不单独作为 authority 证明。 |
| `vis_resolve_count` | 十进制整数文本 | `vis` / `resolve`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `vis_unknown_count` | 十进制整数文本 | `vis` / 未知的累计计数。 | 非零或异常枚举需要与同类成功计数、epoch 和日志关联分析。 |
| `buf_hit_count` | 十进制整数文本 | `buf` / `hit`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
| `buf_miss_count` | 十进制整数文本 | `buf` / `miss`的累计计数。 | 先做时间窗增量；单个绝对值不能直接判定当前故障。 |
