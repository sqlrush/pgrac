# PGRAC v0.135.0 PRE2 产品概览

Author: SqlRush <sqlrush@gmail.com>

> 文档目标版本：**v0.135.0（待发布）**。功能评估技术预览，更新于 2026-10-10。本文描述 PRE2 共享模式的使用范围，不构成生产可用性或性能承诺。安装与启停步骤见[安装手册](install.md)。

**发布状态：** 2026-10-10 核查时，`v0.135.0` 的 tag 和 GitHub Release 尚未发布。本文不表示该版本的二进制、源码包或校验和文件已经可下载；实际资产及构建类型须以发布后的文件清单为准。

PGRAC 在 PostgreSQL 上提供共享存储、多实例读写和跨实例缓冲区协同。多个节点访问同一份数据库数据，应用可以连接不同节点执行 SQL。PRE2 基于 PostgreSQL **16.13**，当前评估配置为**固定四节点、相同版本、每节点不超过 16 个并发业务连接**；该并发范围只完成过短时验证。

PRE2 适合验证应用连接、基本 SQL、共享数据字典和跨节点读写。高可用、自动故障切换、在线成员变更、持续运行稳定性及生产备份恢复，不在本预览的承诺范围内。四台虚拟机的单行更新实验中，集群合计约 **100 TPS**。一次每节点 16 连接的持续更新测试在约 20 分钟后出现 SQLSTATE `53R97`（错误详情含 `PGRAC_REASON=RECYCLED_AUTHORITY_UNPROVABLE`），同一轮还发生了节点虚拟机被关机。正常全停也可能以 FATAL / PANIC 失败，失败后再次启动未经验证；不能把短时功能通过理解为长期可用。

## 1. 核心架构

每个节点运行自己的 PostgreSQL 实例，具有独立的进程、内存缓冲区和本地启动目录。表、索引和共享控制信息位于共享存储；节点之间通过专用互联交换缓存块、事务状态信息和锁消息。

```mermaid
flowchart TB
    APP["应用 / 连接池<br/>PostgreSQL 客户端协议"]
    subgraph CLUSTER["固定四节点 PGRAC 集群"]
        direction LR
        N0["节点 0<br/>SQL 后端 + 本地缓冲区<br/>GCS / GES 与集群后台"]
        N1["节点 1<br/>SQL 后端 + 本地缓冲区<br/>GCS / GES 与集群后台"]
        N2["节点 2<br/>SQL 后端 + 本地缓冲区<br/>GCS / GES 与集群后台"]
        N3["节点 3<br/>SQL 后端 + 本地缓冲区<br/>GCS / GES 与集群后台"]
        IC["专用互联<br/>Cache Fusion 块传输、事务状态、锁协调"]
        N0 <--> IC
        N1 <--> IC
        N2 <--> IC
        N3 <--> IC
    end
    STORE[("共享存储<br/>表 / 索引 / 数据字典 / undo<br/>共享控制与配置、各写者的 WAL")]
    CW["集群基础设施<br/>成员通信、投票、共享文件系统与存储"]
    APP --> N0
    APP --> N1
    APP --> N2
    APP --> N3
    N0 <--> STORE
    N1 <--> STORE
    N2 <--> STORE
    N3 <--> STORE
    CW --- CLUSTER
```

**共享存储。** 四个节点使用同一份表和索引，不需要由应用把数据按节点切成四份。各节点的 `PGDATA` 仍分别管理，不能把一个普通 PostgreSQL 数据目录交给多个 postmaster 同时启动。首次建库需要配套的共享集群初始化步骤，不能只在四台机器上分别执行普通 `initdb`。

**Cache Fusion。** 节点可以从另一节点的缓冲区取得数据块或一致读镜像。GCS（Global Cache Service）协调块的访问权限；GES（Global Enqueue Service）协调跨节点锁及等待。当前预览在转移写权限前仍要求将修改过的块写入共享存储，**并非所有块传输都能省去数据文件写入**。

**事务与恢复记录。** PostgreSQL SQL 事务接口保留；集群增加事务状态、undo 和跨节点一致读处理。WAL 按写者分别记录，共享控制文件及 checkpoint 协调共同参与启动和持久化。共享存储不等于取消 WAL、checkpoint 或正常停机要求。

**部署后端。** 当前评估环境使用 Linux、TCP 互联和 `cluster_fs` 共享文件后端，配合已验证的 GFS2/DLM 存储配置及直接 I/O。安装包中存在其他传输或存储相关代码，不表示这些组合已经完成 PRE2 验收。各节点必须按安装手册使用一致的存储视图和配置。

## 2. 后台进程做什么

下面按运行角色介绍进程。数据库后台名称使用 `pg_stat_activity.backend_type` 对应的名称；`postmaster` 是实例主进程，不作为该视图中的客户端或后台记录。实际可见的进程取决于配置与阶段；启动、退出或恢复期间的进程集合可能不同。

| 进程或角色 | 作用 |
|---|---|
| `postmaster` | 管理本实例的连接、子进程及启动、关闭流程。 |
| `startup` | 启动期间读取控制信息、执行需要的恢复并参与开库流程；通常不常驻于正常服务阶段。 |
| `client backend` | 执行一个客户端会话的 SQL、事务、缓存访问和锁请求。 |
| `lmon` | 处理集群成员和重配置协调，并承担相关控制消息的发送与处理。 |
| `cssd` | 处理数据库侧节点心跳及对端状态。它与操作系统集群服务配合，不替代外部存储和节点隔离设施。 |
| `qvotec` | 处理投票、法定多数与存储服务资格检查。 |
| `lms` / `lms worker` | 处理跨节点块访问、块镜像及事务状态请求。**必须保持 `cluster.lms_workers=2`**；设置大于 2 时，额外的 LMS worker 无法建立连接，并导致正常停机失败。 |
| `lmd` | 处理分布式锁等待图、死锁检测及相应取消工作。 |
| `lck` | 锁辅助进程的生命周期、就绪和状态管理；不能据其存在推断所有锁操作都在该进程执行。 |
| `sinval broadcaster` | 接收并应用跨节点共享缓存失效通知，供数据字典等缓存保持一致。 |
| `undo cleaner` | 清理可回收的 undo、事务表及相关残留事务引用；正常停机也依赖这些工作完成。必须保留配套配置中的清理功能。 |
| `cluster stats` | 汇集集群运行统计，供诊断视图使用。 |
| `diag` | 执行集群诊断与长等待采样。 |
| `checkpointer` / `background writer` | 执行 checkpoint、写回脏缓冲区，并参与共享模式下的持久化处理。 |
| `walwriter` | 将 WAL 缓冲内容写向日志文件；事务所需的持久提交仍按提交设置执行。 |
| `autovacuum launcher` / `autovacuum worker` | 执行 PostgreSQL 表维护任务；共享模式下的回收和冻结受当前限制约束，见下文。 |
| `logger` / `archiver` | 按配置收集日志或归档 WAL；存在归档进程不代表已完成多节点备份恢复验收。 |
| `cluster recovery worker` | 在启用并进入相应恢复路径时处理恢复任务，不是自动故障切换已获验证的标志。 |
| `cluster thread recovery` / `cluster hw remaster` | 恢复或资源管理节点变更期间可能出现的后台，分别处理写者恢复和空间分配状态接管；不表示本预览支持这些流程的自动故障恢复。 |

Corosync、Pacemaker、DLM、qdevice 和共享存储服务属于数据库之外的部署组件，不能用数据库后台进程状态代替它们的健康检查。

## 3. 功能与 SQL 兼容范围

PRE2 保留 PostgreSQL 的 SQL 解析器、执行器和客户端协议，同时改变了共享存储、事务状态和缓存协调部分。**语法能够解析、单机功能存在、集群场景已验证，是三个不同范围。**

| 能力 | 本预览的使用边界 |
|---|---|
| 多节点访问同一数据库 | 固定四节点可分别建立业务连接，读写共享表；不提供自动会话迁移承诺。 |
| 普通 SQL | 以普通 heap 表、B-tree 索引上的 `SELECT`、`INSERT`、`UPDATE`、`DELETE` 和简单事务为主要评估范围。 |
| 事务控制 | 使用 `BEGIN`、`COMMIT`、`ROLLBACK`；建议先以短事务、默认 `READ COMMITTED` 验证应用。其他隔离级别和复杂并发组合需专项验证。 |
| 共享数据字典与 DDL | 提供共享模式的数据字典与常用建表、建索引、角色管理等路径。大型 DDL、复杂对象依赖和并发 DDL/DML 应先在评估库验证，不按原生 PG 的全部 DDL 场景推定支持。 |
| 锁和一致读 | 跨节点协调块权限、事务可见性及相关全局锁；无法可靠判断状态时会返回错误，而不会把未知事务当作已提交。 |
| 客户端 | 使用配套 `psql` 或 PostgreSQL 协议驱动，例如 libpq、JDBC；驱动的连接、重试及故障处理需按应用验证。 |
| 参数管理 | 使用配套共享配置和安装手册中的修改、重启流程；不能只修改某一节点就假定全体生效。 |
| 正常启动、全停和原数据重启 | 固定成员的短时功能场景已有成功记录，但正常全停可能以 FATAL / PANIC 失败。正常停机失败后再次启动未经验证；此时保留四节点数据和日志，联系维护人员，不要自行重启。 |
| 两阶段事务 | 共享模式拒绝 `PREPARE TRANSACTION`、`COMMIT PREPARED` 和 `ROLLBACK PREPARED`，不支持 XA/2PC 集成。SQL `PREPARE` 预备语句不是两阶段事务。 |
| 扩展与存储访问方法 | 共享模式拒绝 `CREATE EXTENSION` / `DROP EXTENSION`；第三方 TableAM、页检查工具和 C ABI 不在本预览的兼容承诺内。 |
| 复制、备份及升级 | 不把原生单实例流复制、PITR、逻辑解码或 `pg_upgrade` 直接等同为共享集群可用方案；本预览不承诺其端到端兼容性。 |

### 共享模式明确拒绝的操作

以下 SQL 操作或参数设置在共享模式下会被拒绝，不能通过重新编译扩展或重复执行来启用。不要关闭共享模式检查来绕过限制。

| 类别 | 明确拒绝的操作或设置 |
|---|---|
| 数据库与表空间 | `CREATE DATABASE` / `DROP DATABASE`；`CREATE TABLESPACE` / `DROP TABLESPACE`；`ALTER TABLE ... SET TABLESPACE`。 |
| 扩展与自定义对象 | `CREATE EXTENSION` / `DROP EXTENSION`；自定义类型（含复合类型、domain、enum、range），自定义 operator、opclass、opfamily 的相关 DDL。 |
| 表对象 | 物化视图；分区和继承关系的建立、附加或分离（如 `PARTITION OF`、`ATTACH/DETACH PARTITION`、`INHERITS/INHERIT`）；`UNLOGGED` 表。 |
| 表维护与重写 | `CLUSTER`、`VACUUM FULL`，以及需要重写表的 `ALTER TABLE`。普通 `VACUUM` 虽可执行，仍有下文所述的冻结和回收限制。 |
| 索引 | `REINDEX`；`CREATE INDEX CONCURRENTLY` / `DROP INDEX CONCURRENTLY`；非 B-tree 索引。 |
| 逻辑复制与日志 | `CREATE SUBSCRIPTION`、replication origin 操作、逻辑消息；`wal_level=logical`、`track_commit_timestamp=on`。 |
| 两阶段事务 | `PREPARE TRANSACTION`、`COMMIT PREPARED`、`ROLLBACK PREPARED`。 |

PGRAC 采用 Cache Fusion 架构，不意味着兼容 Oracle SQL、PL/SQL、Oracle 驱动或 Oracle 管理工具。

## 4. 一个跨节点功能检查

先按[安装手册](install.md)完成整个固定成员集群的初始化和启动，确认各节点均可接受业务连接。四端均连接**初始化时已经创建的同一个数据库**，例如 `postgres`，使用有建表权限的用户。共享模式不支持 `CREATE DATABASE`，不要为本例另建数据库。下面的节点 0、节点 1 指本次部署中的不同实例。

在节点 0 的连接中执行：

```sql
CREATE TABLE pre2_example (
    id integer PRIMARY KEY,
    value integer NOT NULL
);
INSERT INTO pre2_example VALUES (1, 10);
```

在节点 1 的连接中执行并提交：

```sql
BEGIN;
UPDATE pre2_example SET value = value + 1 WHERE id = 1;
COMMIT;
```

再在节点 0 的一个新语句中读回：

```sql
SELECT id, value FROM pre2_example WHERE id = 1;
-- 预期：1, 11
```

先完成这样的少量功能检查，再逐步增加并发。各节点连接池分别限流；每节点最多 16 个并发业务连接仅有短时验证记录，不是持续运行保证。不要通过关闭事务检查、undo cleaner、WAL 同步或共享存储保护来提高测量值。

## 5. 与原生 PostgreSQL 的主要差异

| 原生 PostgreSQL 单实例使用习惯 | PRE2 共享模式 |
|---|---|
| 一个实例管理一个数据目录 | 每节点有独立启动目录和进程，访问一份共享数据库；需要整体初始化与配置。 |
| 本地缓冲区和锁管理 | 跨节点读写可能产生块传输、事务状态查询及全局锁等待。 |
| 原生 heap 页、事务状态文件 | 存在集群扩展的页和事务状态内容；不能直接用普通 PG 二进制打开 PRE2 数据。 |
| 单实例 WAL/checkpoint 与启停 | 各写者的 WAL 与共享控制信息需要协调，正常全停应执行集群流程。 |
| 常规 VACUUM/freeze 持续推进 | 当前 VACUUM 不能冻结或回收旧行版本；持续更新时表和索引会膨胀。 |
| 按 PostgreSQL 版本升级或复制 | 需要 PGRAC 对应版本的明确迁移、备份与恢复支持，不能直接套用原生操作。 |

## 6. 当前限制与评估建议

### 固定成员及故障处理

本预览使用固定四节点、同构软件和配置。不将在线加节点、计划退出、滚动升级、故障节点在线重新加入作为支持范围；自动故障切换、故障后其余节点持续服务和全体崩溃冷启动恢复也尚未作为对外交付能力完成验收。

发生节点失联、存储异常或进程异常退出时，停止该轮业务测试，保留数据与日志，按配套故障处理说明操作。不要强行重新挂载存储、绕过投票/隔离检查或把单节点重启当作通用恢复步骤。

### 并发与性能

四台虚拟机、每节点 16 个并发连接的普通单行 `UPDATE` + `COMMIT` 实验中，**集群合计约 100 TPS**。这是该实验环境中的读数，不是性能或线性扩展承诺。

每节点 **≤16 个并发业务连接只完成过短时验证**，不是 `max_connections` 的硬编码上限，也不代表任意 SQL 或持续运行都稳定。每节点 32/64 连接、长事务、复杂查询及批量写入未获得本预览的稳定性保证。**必须保持 `cluster.lms_workers=2`**；大于 2 时额外 worker 无法建立连接，并导致正常停机失败。

### 长时间运行、VACUUM 与空间

**无论 autovacuum 开关如何，当前共享模式下的 VACUUM / VACUUM FREEZE 都不能冻结或回收旧行版本；持续更新时表和索引会膨胀。** 正常全停重启不会重置事务号年龄，反复执行这些维护命令也不能解除该限制。

一次每节点 16 连接的持续更新测试在约 20 分钟后出现 SQLSTATE `53R97`，错误详情为 `PGRAC_REASON=RECYCLED_AUTHORITY_UNPROVABLE`；同一轮还发生节点虚拟机被关机。**这不是安全运行 20 分钟的保证**，也不能只凭先后顺序认定两者的根因相同。当前没有经过验证的安全连续运行时长。

持续写入还需关注 undo、WAL 和剩余磁盘空间。请使用可重建的功能评估数据，避免承载唯一的重要数据或安排无人值守的持续写入。

评估时记录业务提交数、错误、磁盘剩余空间，以及下列原生监测量。它们用于观察趋势，不替代共享事务状态的正确性检查：

```sql
SELECT datname, age(datfrozenxid) AS xid_age
FROM pg_database
ORDER BY xid_age DESC;

SELECT pg_size_pretty(pg_relation_size('pre2_example')) AS heap_size,
       pg_size_pretty(pg_total_relation_size('pre2_example')) AS total_size;
```

共享同一张表的物理尺寸不要把四节点结果相加。`n_dead_tup` 是估计值，不能单凭它为零就认为旧版本已全部回收。出现事务年龄告警、反复事务状态错误或持续空间增长时，应停止写入评估并联系维护人员；不要修改内部水位、执行 `pg_resetwal` 或关闭必要清理功能。

### 错误与正常全停

遇到可见性或事务状态无法确认的错误，应回滚失败事务，记录 SQLSTATE、节点、时间和对应日志。是否重试应由应用与维护人员结合错误原因决定；断连后的提交结果不确定时，不要盲目重放非幂等业务。相关说明见[跨节点事务安全](../reference/cluster-transaction-safety.md)。

测试结束时按[安装手册](install.md)同时向四节点发出正常停机。**正常全停可能因 FATAL / PANIC 或进程提前退出而失败；失败后再次启动未经验证。** 此时保留四节点的数据、WAL 和日志，联系维护人员，不要自行重启、重新初始化或删除数据。

只有确认全部节点正常停止后，才按安装手册操作共享存储。不能以强制杀进程后的退出状态替代正常停机成功；也不能把一轮正常停机成功扩大为高可用或任意崩溃恢复保证。
