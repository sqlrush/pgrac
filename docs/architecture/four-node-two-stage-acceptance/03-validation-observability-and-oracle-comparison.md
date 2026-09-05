# 验证层次、可观测性与 Oracle RAC 边界

## 1. 四层验证分别回答什么

```mermaid
flowchart LR
    U[Focused tests<br/>局部合同] --> T430[t/430<br/>资源容量与复用]
    T430 --> T400[t/400<br/>四节点事务正确性]
    T400 --> T406[t/406<br/>精确 finish-error 负向证据]
    T406 --> PRE[8.15-PRE<br/>性能基线]
    PRE --> PERF[自适应饱和测试<br/>最终 TPS 门]
```

| 层次 | 核心问题 | 不回答的问题 |
|---|---|---|
| Focused tests | 单个状态转换、设备认证、乱序和清理规则是否正确 | 四节点端到端是否闭环 |
| `t/430` | 更宽资源集合下目录容量是否有界、能否真实 retire/reuse | 完整事务正确性和最终 TPS |
| `t/400` | 四节点普通 `UPDATE+COMMIT`、R4/Resource-X 和终态守恒是否正确 | 高并发吞吐能力 |
| `t/406` | 精确 retained source finish-Flush ERROR 是否保留 pending pair、拒绝同 attempt ACK 并 fail closed | 正向事务吞吐或故障后的继续服务 |
| 8.15-PRE | 与后续对比一致的初始性能和瓶颈证据 | Stage 8 最终性能 verdict |
| 自适应饱和 | 动态增加连接后最佳稳定总 TPS 是否达标 | 更广泛 Stage 9 HA 场景 |

## 2. `t/430` 的终态应该怎么看

资源生命周期测试不能简单要求所有缓存对象都归零。正常结束后，身份和代际仍然精确的 cached S、cached X 或有持久来源支撑的 PI 可以合法驻留。

真正必须归零的是协议债务，例如：

- 未完成 waiter/convert/wait-for edge；
- 未结算 proof/image；
- 正在驱逐或尚未闭合的回收计划；
- 未完成 transport；
- 未结束的 Resource-X round、requester join 或 source settlement。

与此同时，`t/430` 仍必须证明：

- 在较小配置容量下触达足够多的不同资源；
- 至少发生真实 retire 与 reuse；
- live、tombstone 和 slot 峰值有界；
- 旧帧不能通过 ABA 重新命中新对象；
- 无目录满、client error 或全局服务门关闭。

## 3. `t/400` 是正确性锚

`t/400` 启动四个真实 PostgreSQL 实例，在共享数据根上执行普通更新与提交，并检查：

- 四节点都有成功事务；
- 远端块安装和全局资源授权发生；
- 请求、参与者、资源槽和等待关系最终闭合；
- 新 Resource-X 路径工作；
- 旧 ticket/source 正常路径保持不可达；
- 四个实例没有 client error。

它的断言数量较多，是因为同时覆盖 workload 结果、跨节点资源流、错误极性和终态守恒。它不以事务数或 TPS 作为主要得分，因此通过 `t/400` 不等于性能已经达到目标。

破坏性的 retained source finish-Flush ERROR 由独立 `t/406` 承担。t/406 先证明非目标 decoy 正常完成且不消费 one-shot，再证明精确目标在 `smgrwrite` 前失败、pending pair 保留、同 requester/attempt 没有 settlement ACK、运行时 fail closed、postmaster 存活且 BufferIO 清理完整。该失败集群不会被复用于 PRE。

## 4. 8.15-PRE 与最终性能门

PRE 在第二阶段的当前四节点身份上运行，目的是采集后续优化的可信基线。典型基线包含每节点多条持久连接和分散热块，但最终性能验收不会把连接数固定死。

正式饱和测试会逐步增加每节点连接数，直到连续更高并发点不能显著提高稳定中位数。失败、超时、被取消、零操作或进程非零退出的样本不计有效样本，也不能删除后重新包装结果。

## 5. 建议观察的证据

### 5.1 生命周期日志

- 四个 native start/stop 返回值；
- 每个节点的四成员 formation 与 quorum；
- shutdown checkpoint 开始/完成；
- 当前启动周期终止状态；
- 成员间停机确认和最终交付确认；
- Phase 2 使用的 voting device 列表。

### 5.2 设备认证

- backing file 与 device 对应关系；
- 容量；
- direct I/O 状态；
- byte comparison；
- voting 序号；
- teardown 后是否仍有 loop binding。

静态认证还应与动态资格检查分开记录：

- `DEVICE_STATIC_ATTESTED`：类型、容量、DIO、内容与序号匹配；
- `DEVICE_IO_QUALIFIED`：独立 scratch device 的有界 direct I/O 探针完整通过；
- `BLOCK_DEVICE_UNQUALIFIED`：探针错码、校验差异、超时或进程进入不可中断 I/O；
- `OPERATOR_CLEANUP_REQUIRED`：精确进程/FD 尚未退出，设备必须保留等待显式回收。

不能用后一层超时覆盖前一层首错，也不能把清理成功改写成原测试成功。

### 5.3 资源与事务

- PCM/GRD live、retire、reuse、tombstone；
- Resource-X waiter、active round、retained item；
- remote install；
- terminal participant 和 wait-for graph；
- legacy path 使用计数；
- 每节点成功/错误事务数。

## 6. 与 Oracle RAC 的相同点和差异

### 6.1 公开行为上对齐的部分

Oracle 官方文档确认：

- voting files 属于 Clusterware 节点成员关系基础；
- RAC 实例启动前需要 Grid Infrastructure；
- 多实例可由 Clusterware/SRVCTL 统一启动和停止；
-正常或 immediate shutdown 后不需要 instance recovery；
- GCS、GES 和 GRD 协调缓存块、全局资源与 Cache Fusion。

PGRAC 的两阶段基板保持了同样的职责边界：先证明成员与存储就绪，再激活数据库全局资源；正常停机必须留下可验证的 clean terminal，不能用进程消失代替。

Oracle 还明确把 voting file 可访问性作为 Clusterware 可用性的基础；进一步丢失 voting access 可能导致节点驱逐或功能丧失。因此，PGRAC 在 voting I/O 不可信时停止数据库资源激活，与这一外部安全边界一致。

### 6.2 PGRAC 自研适配

Oracle 没有公开或要求以下测试实现：

- 普通文件原位映射为 DIO loop device；
- 测试中的固定启动偏移；
- PGRAC 的停机交付确认细节；
- R4 早到消息的本地有界保留算法；
- `t/430`、`t/400`、`t/406` 和 PRE 的断言结构。
- loop scratch I/O 资格检查、不可中断进程清理清单和显式 reaper。

因此这些只能描述为 PGRAC 的自动化验证实现，不能称为 Oracle 内部协议复刻。

## 7. Oracle 官方资料

- [Administering Database Instances and Cluster Databases](https://docs.oracle.com/en/database/oracle/oracle-database/26/racad/administering-database-instances-and-cluster-databases.html)
- [Managing Oracle Cluster Registry and Voting Files](https://docs.oracle.com/en/database/oracle/oracle-database/19/cwadd/managing-oracle-cluster-registry-and-voting-files.html)
- [Starting Up and Shutting Down](https://docs.oracle.com/en/database/oracle/oracle-database/19/admin/starting-up-and-shutting-down.html)
- [Introduction to Oracle RAC](https://docs.oracle.com/en/database/oracle/oracle-database/26/racad/introduction-to-oracle-rac.html)
- [Cache Fusion and the Global Cache Service](https://docs.oracle.com/cd/A91202_01/901_doc/rac.901/a89867/pslkgdtl.htm)
- [CRS-01672](https://docs.oracle.com/en/error-help/db/crs-01672/)
