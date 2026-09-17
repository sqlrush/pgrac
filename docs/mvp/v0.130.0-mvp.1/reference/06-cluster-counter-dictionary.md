# `pg_stat_cluster_counters` 计数器字典

Author: SqlRush <sqlrush@gmail.com>

基线：`v0.130.0-mvp.1`。本标签注册 33 个名称。存储注册表是**进程本地**的；部分值在查询入口从真实 owner 同步镜像。查询 backend 的 0 不能推导另一个后台进程从未发生事件。

## `pg_stat_cluster_counters`

| 字段 | SQL 类型 | 单位/空值 | 含义 |
|---|---|---|---|
| `name` | `text` | 稳定名称 | 编译期注册的计数器名字，不是可修改配置。 |
| `value` | `bigint` | 按下表；未发布通常为 0 | 查询时当前进程中的计数，或同步镜像后的值；不自动聚合四节点。 |

累计事件应在同一 owner 生命周期内取增量；gauge 直接读当前值，允许正常下降；时间戳不是事件数量。以下 `cluster.page.*` 属于页面恢复观测接口，其存在不代表本 MVP 已认证故障恢复。

对于 quorum 和 fence 的跨进程健康，优先读 `pg_cluster_quorum_state`、`pg_cluster_fence_state`；不能把此表里本 backend 的 0 当成这些后台服务的总量。

| 计数器 | 类型/单位 | 含义 | 使用方法 |
|---|---|---|---|
| `cluster.inject.armed_count` | gauge / 个 | 当前查询进程中已武装的注入点数量。 | 直接记录当前量；下降不自动等于重置。 |
| `cluster.smgr.remote_invalidation_stub_call_count` | 累计事件 / 次 | 远端失效兼容 stub 的调用次数；不是实际网络失效已完成的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.qvotec.poll_cycle_count` | 累计事件 / 次 | 投票轮询循环次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.qvotec.quorum_loss_event_count` | 累计事件 / 次 | 记录的多数丢失事件次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.qvotec.collision_detect_event_count` | 累计事件 / 次 | 投票身份/代际碰撞检测事件次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.qvotec.disk_io_failure_count` | 累计事件 / 次 | 投票介质 I/O 失败次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.fence.freeze_broadcast_count` | 累计事件 / 次 | 冻结写入广播次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.fence.thaw_broadcast_count` | 累计事件 / 次 | 解除冻结广播次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.fence.self_fence_initiated_count` | 累计事件 / 次 | 发起本节点自隔离次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.fence.freeze_signal_received_count` | 累计事件 / 次 | 当前 backend 收到冻结信号的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.source_selected_current` | 累计事件 / 次 | 页面恢复选择 current 镜像的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.source_selected_pi` | 累计事件 / 次 | 页面恢复选择 PI 镜像的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.source_selected_storage` | 累计事件 / 次 | 页面恢复选择存储内容作为来源的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.source_invalid` | 累计事件 / 次 | 页面来源被判无效的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.source_missing` | 累计事件 / 次 | 页面来源缺失的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.source_conflict` | 累计事件 / 次 | 不同来源证据冲突的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.result_skip` | 累计事件 / 次 | 页面恢复按结果判定无需应用的次数；不是测试被跳过的计数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.apply_count` | 累计事件 / 次 | 页面恢复实际执行应用的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.version_mismatch` | 累计事件 / 次 | 页面恢复发现版本不匹配的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.unknown_class_blocked` | 累计事件 / 次 | 因无法识别页面类别而阻断的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.authority_stale_rejected` | 累计事件 / 次 | 因页面权威过期/陈旧而拒绝的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.resource_early_release` | 累计事件 / 次 | 页面恢复检测到资源过早释放的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.retire_denied` | 累计事件 / 次 | 页面恢复资源退休请求被拒绝的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.d3_rebuild` | 累计事件 / 次 | 页面恢复重建分支执行次数；按观测接口名称保留。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.d3_optimization_hit` | 累计事件 / 次 | 页面恢复对应重建优化命中次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.stable_base_unresolved` | 累计事件 / 次 | 页面恢复不能解析稳定基线的次数。 | 同一 owner 内计算增量；重启/换进程后重新建基线。 |
| `cluster.page.contributor_records` | gauge / 个 | 最近一次贡献者统计中的记录数。 | 直接记录当前量；下降不自动等于重置。 |
| `cluster.page.contributor_threads` | gauge / 个 | 最近一次贡献者统计中的 WAL 线程数。 | 直接记录当前量；下降不自动等于重置。 |
| `cluster.page.contributor_gaps` | gauge / 个 | 最近一次贡献者统计中的缺口数。 | 直接记录当前量；下降不自动等于重置。 |
| `cluster.page.retained_pinned_bytes` | gauge / 字节 | 当前/最近公布的保留 pin 字节量。 | 直接记录当前量；下降不自动等于重置。 |
| `cluster.page.last_page_write_ts` | PG 纪元微秒 | 最近页面写入时刻，PG 纪元微秒数。 | 0 表示尚未发布；不是 Unix 秒，勿直接送入 to_timestamp(value)。 |
| `cluster.page.last_durability_barrier_ts` | PG 纪元微秒 | 最近完成持久化屏障的时刻，PG 纪元微秒数。 | 0 表示尚未发布；不是 Unix 秒，勿直接送入 to_timestamp(value)。 |
| `cluster.page.last_post_read_ts` | PG 纪元微秒 | 最近完成写后回读的时刻，PG 纪元微秒数。 | 0 表示尚未发布；不是 Unix 秒，勿直接送入 to_timestamp(value)。 |

示例：

```sql
SELECT name, value FROM pg_stat_cluster_counters ORDER BY name;
```
