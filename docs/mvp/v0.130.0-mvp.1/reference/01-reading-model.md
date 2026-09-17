# 读取模型、权限、刷新和哨兵值

Author: SqlRush <sqlrush@gmail.com>

适用版本：`v0.130.0-mvp.1`。返回[手册目录](../README.md)。


## 1. 三类数据面

| 数据面 | 典型视图 | 更新方式 | 读取注意事项 |
|---|---|---|---|
| 启动配置快照 | `pg_cluster_nodes`、`pg_cluster_shmem` | postmaster 启动时解析/注册 | 修改配置文件后，通常要重启才改变 |
| 共享运行状态 | membership、quorum、GCS/GES/LMD、ADG 视图 | 后台进程或请求路径持续发布 | 一次 SELECT 内逐行读取，不承诺跨子系统原子一致 |
| 当前进程注册表 | wait event、injection、部分 counter | 编译期注册，进程内计数 | 不应把本进程计数误当成全节点聚合值 |

## 2. 权限

本手册列出的视图均为只读，并向 PUBLIC 授予 SELECT。执行 clean leave、永久移除节点、
backup start/stop 或创建 restore point 的函数不是视图；这些会改变系统状态的入口不会因为
视图可读而向 PUBLIC 开放。

## 3. 一致性

- 每一行是查询时刻的状态投影，不是持久业务事实。
- 后台进程可在扫描期间继续更新计数器和状态，因此两个相关字段可能来自相邻时刻。
- 需要计算增量时，保存 `clock_timestamp()`、节点号和原值；不要只保存差值。
- epoch、generation、incarnation、session 等字段必须按完整 tuple 比较，不能只比较其中一个数。

## 4. 哨兵值

| 值 | 通常含义 | 使用规则 |
|---|---|---|
| `NULL` | 尚未发生、尚无对象、当前路径不适用 | 不要自动替换成 0；NULL 与“发生在 epoch 0”不同 |
| `-1` | 未配置、无 owner、idle 时无目标节点 | 先读同一行的 state/phase 再解释 |
| `0` | 合法节点 0、初始水位、无事件、空容量或合法 epoch 0 | 由字段说明判定；0 既不是通用未知值，也不自动等于成功 |
| `unknown` | 当前证据不足以给出肯定结论 | 按 fail-closed 处理，不得推断健康 |
| `blocked` / `unavailable` | 条件不满足或 provider 不可用 | 查相邻 reason/counter 和相关视图 |
| `(unset)` / `(null)` / `(empty)` | `pg_cluster_state` 的显式文本哨兵 | 这是文本，不是 SQL NULL |

## 5. 计数器生命周期

计数器可能由共享内存、进程本地原子变量或查询时镜像产生。`*_count` 也可能是当前数量，
不能仅凭名称判为累计事件；先核对字段定义。postmaster 重启、共享内存重建或进程替换可能让读数重新从 0 开始。
采集器应同时记录 `pg_postmaster_start_time()` 和节点 incarnation，检测重置后重新建立基线。
