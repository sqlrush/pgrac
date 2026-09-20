# PGRAC MVP 性能优化版使用入口

Author: SqlRush <sqlrush@gmail.com>

版本：`v0.131.0`；PostgreSQL 基线：16.13。

本版改善 undo 段头资源的本地性，包含资格化期间的正确性修复，并增加默认关闭的
UPDATE 耗时观测。稳定发布必须通过四轮正式正确性 PRE 与精确提交的完整 CI；
最终读数及提交身份见 [GitHub Release](https://github.com/sqlrush/pgrac/releases/tag/v0.131.0)。
能力边界和对照读数见[发布说明](../../release-notes/v0.131.0.md)，不把实验室吞吐当作生产保证。

## 安装

按[单机四实例 Quick Start（Linux）](quickstart-linux-single-host.md)拉取标签、编译、初始化及连接。
使用本机文件系统上的四个独立 PGDATA，共享业务数据；不是四台主机，不需要容器或 GFS2。
安装步骤沿用已演练示例，本轮没有重新运行 Quick Start 演练。

免编译演示请用[单机四 Pod 安装与演示手册](../../../deploy/demo/README.md)：
Podman 镜像、环境预检、初始化和压测脚本、正常停机与原数据重启。
该独立演示包不是多机共享盘或生产高可用认证。
共享存储目录、投票设备映射、实际参数与安装后核验命令见手册的
[存储要求](../../../deploy/demo/README.md#存储要求)。
PGDATA、共享控制文件软链接、分线程 WAL 的含义，以及代码已有能力与当前演示配置的区别，
见[控制文件与 WAL](../../../deploy/demo/README.md#control-files-and-wal)；不能由目录共享推断跨节点故障恢复已通过验收。
当前演示镜像因发布复测的正常停机失败暂停发布，尚不可作为客户交付包；
不要将已有稳定源码发布与演示镜像可用混为一谈。

已有集群不得逐节点热替换：先全体正常关闭、保留原干净数据，再使用同版本二进制正常启动。
没有认证混版本、滚动升级、崩溃恢复、通用迁移或降级；异常退出现场不能当作干净数据接管。

## 参考手册

| 内容 | 文档 |
|---|---|
| 新增 UPDATE 计时开关、记录与分析工具 | [UPDATE tracing](../../user-guide/update-tracing.md) |
| 共享存储准备、检查与四机边界 | [存储准备](../v0.130.0-mvp.1/storage-preparation.md)、[部署参考](../v0.130.0-mvp.1/01-linux-four-node-deployment.md) |
| 既有参数与配置组合 | [参数手册](../v0.130.0-mvp.1/02-parameters.md) |
| 既有系统视图与字段 | [系统视图](../v0.130.0-mvp.1/03-system-views.md) |
| 核心能力 | [功能与运行机制](../v0.130.0-mvp.1/04-core-capabilities.md) |
| 编译安装选项 | [安装指南](../../user-guide/install.md) |

旧参考目录内的版本和验收声明仅属于旧版本；本版新增观测接口以 tracing 指南为准。
发布为源码，不提供生产认证二进制。保留标签、完整 commit、编译参数、配置和二进制 SHA-256，
不要仅用旧的 `pgrac_version()` 字符串识别版本。历史标签不移动、不重用。
