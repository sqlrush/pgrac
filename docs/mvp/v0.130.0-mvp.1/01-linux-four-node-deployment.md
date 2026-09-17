# 一、Linux 四节点部署与共享存储

Author: SqlRush <sqlrush@gmail.com>

适用：`v0.130.0-mvp.1`，源码 `c581f3835a4a9a76ce5a77c0937930f21765725a`。仅用于隔离评估环境。

## 1. 先选择部署目标

| 目标 | 本版能够提供的依据 | 本文处理方式 |
|---|---|---|
| 同一 Linux 环境中的四个实例 | 发布验收中的受控四节点夹具、共享路径与投票设备配置 | 可参考源码复现；这是测试环境，不是四机高可用 |
| 四台 Linux 主机，各运行一个实例 | TCP、共享文件路径、节点身份和 seed/join 接口已存在 | 给出安装与接入清单；共享介质、投票设备初始化及整套编排仍须完成部署验证 |
| 故障后自动接管的生产集群 | 本 MVP 未提供该部署认证 | 不适用；不要以心跳正常或四节点可查询代替认证 |

**当前没有经过本 MVP 发布认证的“四台新机器从零到业务可写”的一键安装器。** `pgrac-init` 初始化目录，`pgrac-start` 启动一个 postmaster；两者都不是完整的集群部署控制器。下面各步骤标明可以直接使用的命令和必须停留在接入验证的边界。

## 2. 四节点与路径规划

以下是示例规划，地址必须替换为实际隔离网络地址。四机可以使用相同端口；单机四实例必须各自使用不同端口。

| 项目 | node0 | node1 | node2 | node3 |
|---|---|---|---|---|
| `cluster.node_id` | 0 | 1 | 2 | 3 |
| 主机名 | pgrac0 | pgrac1 | pgrac2 | pgrac3 |
| SQL 地址 | 10.20.0.10:5432 | 10.20.0.11:5432 | 10.20.0.12:5432 | 10.20.0.13:5432 |
| CONTROL 地址 | 10.30.0.10:7000 | 10.30.0.11:7000 | 10.30.0.12:7000 | 10.30.0.13:7000 |
| DATA 基地址 | 10.30.0.10:7100 | 10.30.0.11:7100 | 10.30.0.12:7100 | 10.30.0.13:7100 |
| 本地 PGDATA | `/var/lib/pgrac/node0` | `/var/lib/pgrac/node1` | `/var/lib/pgrac/node2` | `/var/lib/pgrac/node3` |
| 独占 WAL 写入目录 | `thread_1` | `thread_2` | `thread_3` | `thread_4` |

公共安装前缀示例：`/opt/pgrac/v0.130.0-mvp.1`；共享挂载示例：`/srv/pgrac-shared`。

```text
每台主机本地                         四台主机看到同一组字节
 /opt/pgrac/v0.130.0-mvp.1            /srv/pgrac-shared/
 /var/lib/pgrac/nodeN/                  data/      共享关系、目录、控制与 undo
   postgresql.conf                     wal/       WAL 线程根与注册信息
   pg_hba.conf                           thread_1/  仅 node0 写
   pgrac.conf                            thread_2/  仅 node1 写
   pg_wal -> .../wal/thread_(N+1)         thread_3/  仅 node2 写
 /var/log/pgrac/                          thread_4/  仅 node3 写
                                      voting/     已正确初始化的投票介质
```

四个实例**不能共用一个 PGDATA**，也不能同时写同一个 `pg_wal`。WAL thread ID 是 `node_id + 1`。路径相同但各自落在本地磁盘上，不是共享存储。

## 3. 共享存储必须满足什么

### 3.1 文件系统后端

本 MVP 的四实例验收使用 `cluster.shared_storage_backend = 'cluster_fs'`，但它只是数据库后端名称，不是用户要安装的文件系统名称。

**四台独立主机的具体准备参考路线：RHEL 9 x86_64 + 共享块存储 + 共享 LVM + GFS2。** 先完成存储集群、DLM、fencing 和四机同卷挂载，再配置 `cluster.shared_data_dir`。这是待验证的四机准备路线，不是已完成的 PGRAC 兼容认证。同一 Linux 内核下四实例共用本地目录，不需要 GFS2，不能混为一种部署。

安装前必须先读并完成[共享存储准备与校验附录](storage-preparation.md)：组件/设备清单、配置值、权限与挂载检查、保持文件打开的跨节点读写检查、锁与持久化边界、停止条件。它不是仅要求“准备一个共享目录”。

| 要求 | 检查内容 | 不满足时的风险 |
|---|---|---|
| 同一存储身份 | 四节点读取同一个卷、同一目录内容；不是四份 rsync 副本 | 同名文件不等于一个数据库 |
| 可见性与顺序 | 多主机打开/读取能观察到另一个节点已完成的更新；缓存语义经过验证 | 读取旧控制信息或旧数据 |
| 持久化 | `fsync`/同步写的完成语义、设备写缓存和掉电保护明确 | 已确认写入丢失 |
| 文件操作 | create、rename、目录持久化及错误返回的语义可用 | 控制文件/目录发布不完整 |
| 权限 | 相同服务 UID/GID，数据库用户可读写指定树，其他用户不能任意改写 | 权限拒绝或状态被篡改 |
| 独立命名空间 | 数据、WAL 根、PGDATA 互不覆盖；绝对、规范化路径 | 初始化覆盖或错误接管 |
| 故障域 | 数据卷、WAL、投票设备的故障相关性有记录 | 多个路径可能仍是同一故障点 |

不能把普通 ext4/XFS 文件系统在四台主机上同时读写挂载同一块 LUN。使用集群文件系统或经过验证的共享文件服务；**本版未给出已认证的 NFS/GFS2/OCFS2/存储阵列兼容列表**，不能仅凭产品名称宣布兼容。网络文件系统的挂载选项需要按服务端与客户端实际组合验证，不提供一个通用的“安全 NFS 参数”。

只读检查示例，在四节点分别执行：

```sh
findmnt -T /srv/pgrac-shared
stat -c '%u:%g %a %n' /srv/pgrac-shared
df -h /srv/pgrac-shared
realpath /srv/pgrac-shared
```

在数据库初始化前，使用**专门的新建 scratch 子目录**验证跨主机写后读、rename 与持久化；不要用真实数据文件或投票盘做写探测。本次文档编写没有替目标存储执行上述资格化。

### 3.2 裸块设备后端不是“把 PGDATA 放在 SAN 上”

`block_device` 是另一种存储后端，使用 `cluster.block_device_path`，涉及直接 I/O、块对齐和存储布局。它不是 ASM，也不是自动创建共享文件系统的工具。目录/控制信息、WAL、投票介质仍有各自要求，不能只填一个 LUN 路径就认为整个数据库布局完成。

本版不提供面向陌生数据盘的格式化命令。不应对现有盘运行 `mkfs`、`dd`、`truncate` 或测试 formatter。常规文件模拟块设备只说明实验路径，不证明多主机持久化、设备预留或 fencing 已成立。

### 3.3 投票介质是独立前提

四节点配置应使用正确初始化、四节点可访问的投票介质并启用严格多数判定，不能以 `cluster.allow_single_node=on` 代替。

- `cluster.voting_disks` 是按固定顺序配置的路径列表。介质内容、索引、身份和长度必须匹配；空文件、全零文件或刚创建的 loop 设备不合格。
- 发布测试使用受控的文件初始化、全体正常关闭、同一内容映射为 Linux 直接 I/O 设备的流程。**单主机 loop 设备不是可跨四台主机共享的投票盘。**
- 源码中 `PostgreSQL::Test::ClusterVotingDisk` 是**会覆写目标的测试 formatter**，不是运维工具。不要把它用于生产设备。
- MVP 未提供经过四机认证的投票介质制备/设备接入 CLI。这是四机从零部署尚需补齐的交付环节；应在此记录“部署前提未满足”，不能继续开启业务。
- `pg_cluster_voting_disks` 的逐盘状态/计数部分仍是占位输出；`unknown`/0 不等于磁盘健康。需结合 quorum 视图、日志和设备检查。

数据共享与节点隔离是两件事。看不到旧节点心跳，并不能证明它不再写共享盘。外部 fencing 尚未认证时，不执行依赖它的故障接管。

## 4. Linux 编译安装：四节点保持一致

建议使用相同 Linux 发行版、CPU 架构、编译器与依赖版本。GFS2 四机参考路线使用 RHEL 9；下列开发依赖和存储附录的 HA/文件系统组件是两组不同依赖，都需准备。包可用性依赖所选发行版的软件渠道，不构成 PGRAC 发行版认证。

RHEL 9 管理员安装编译依赖（先启用适用的开发软件渠道）：

```sh
sudo dnf install gcc make git pkgconf-pkg-config bison flex perl perl-IPC-Run \
  readline-devel zlib-devel libicu-devel lz4-devel libzstd-devel
```

Debian/Ubuntu 本机评估环境的对应示例；不要把它当作已经验证的 Ubuntu/GFS2 四机方案：

```sh
sudo apt-get update
sudo apt-get install build-essential git pkg-config bison flex perl \
  libreadline-dev zlib1g-dev libicu-dev liblz4-dev libzstd-dev libipc-run-perl
```

使用专用非 root 账号 `pgrac` 运行数据库；四机保持相同 UID/GID。管理员创建独立安装前缀、本地数据父目录和日志目录并授权该账号。**不要递归修改已有共享卷的所有权**。共享 `data/` 和 `wal/` 应为待初始化的新路径，不提前填充文件。

### 4.1 拉取不可变 MVP 标签

四机以相同账号执行，或从同一受控源码包分发：

```sh
git clone --branch v0.130.0-mvp.1 --single-branch \
  https://github.com/sqlrush/pgrac.git pgrac-mvp1
cd pgrac-mvp1
git rev-parse HEAD
git status --short
cat PGRAC_VERSION
```

预期 HEAD 为 `c581f3835a4a9a76ce5a77c0937930f21765725a`，源码无本地改动。不使用浮动 `main` 混装，不移动标签。`postgres --version` 的旧编译版本字符串不能代替标签、commit 与二进制哈希。

### 4.2 评估构建

本标签存在启用 OpenSSL 后集群 server SHA2 链接缺符号的问题，CI 修复不属于此标签。下面给出**不启用 OpenSSL 的隔离评估构建**；不要追加 `--with-openssl`/`--with-ssl=openssl` 后忽略构建失败，也不要关闭集群功能来冒充修好。

```sh
mkdir build-mvp1
cd build-mvp1
../configure --prefix=/opt/pgrac/v0.130.0-mvp.1 \
  --enable-cluster --enable-cassert --enable-debug --enable-tap-tests \
  --with-icu --with-lz4 --with-zstd
make -j4
make install
export PATH=/opt/pgrac/v0.130.0-mvp.1/bin:$PATH
pg_config --configure
sha256sum /opt/pgrac/v0.130.0-mvp.1/bin/postgres
ldd /opt/pgrac/v0.130.0-mvp.1/bin/postgres
```

前缀须已由管理员创建并授予安装权限。记录 commit、configure 输出、编译器/依赖版本和四台机器的二进制哈希。独立编译的哈希可能因构建路径等不同；不能仅以源码标签相同掩盖编译选项差异。使用同一构建产物分发时还必须确认动态库 ABI 一致，不能跨架构复制。

不启用 TLS 的数据库连接只允许隔离实验网络或经过 SSH/VPN 保护的通道；SCRAM 验证口令不等于连接加密。CONTROL/DATA 互联也不应暴露到公网。需要原生 TLS 的外部使用，应等待修复后的新版本完成构建验证，不把无 TLS 方案当成生产推荐。[PG 16 构建选项](https://www.postgresql.org/docs/16/install-make.html)

## 5. 初始化：一个数据库身份，四个实例

### 5.1 node0 创建 seed

以下由数据库账号在 node0 执行，只能指向确认过的空路径。`/srv/pgrac-shared` 已完成挂载/权限检查；`data` 和 `wal` 是新根。

```sh
pgrac-init -D /var/lib/pgrac/node0 --node-id=0 \
  --cluster-name=mvp4 --cluster-seed \
  --shared-data-dir=/srv/pgrac-shared/data \
  --wal-threads-dir=/srv/pgrac-shared/wal \
  --initdb-options='--encoding=UTF8 --auth-local=peer --auth-host=scram-sha-256'
```

CLI 会调用本版本 `initdb`，设置本节点身份、独占 WAL 线程及共享系统目录所需配置。不要四台机器各自运行独立 `initdb` 再拼到一起；那样会得到四个不同的 `system_identifier`。

**第一次启动是单 seed 阶段**。在 node0 的 `postgresql.conf` 中设置下列值；暂不写四节点拓扑，不开放业务：

```conf
cluster.enabled = off
cluster.lms_enabled = off
listen_addresses = '127.0.0.1,10.20.0.10'
port = 5432
ssl = off
password_encryption = 'scram-sha-256'
```

CLI 已写入的 `shared_storage_backend`、`shared_data_dir`、`smgr_user_relations`、`controlfile_shared_authority`、`shared_catalog`、`merged_recovery` 不要删除。`cluster.enabled=off` 不意味着所有共享目录逻辑关闭；首次启动仍负责初始化共享目录权威。

```sh
pgrac-start -D /var/lib/pgrac/node0 -l /var/log/pgrac/node0.log -w
psql -X -v ON_ERROR_STOP=1 -d postgres -c 'SELECT system_identifier FROM pg_control_system();'
```

确认共享根出现 `global/pg_control` 与 `global/pgrac_catalog_authority`，日志无 FATAL/PANIC。此时只有 seed 阶段完成，**不是四节点业务已经开放**。

### 5.2 joiner 从受支持的备份来源初始化

发布 CLI 提供 `--cluster-join`，接受 `--join-from` 或 `--join-from-backup`。这是初始化接口说明，不是本版已经完成四机在线扩容认证的承诺。

在 seed 创建只用于备份的账号，通过 `psql` 的交互 `\password` 设置口令，避免命令行或文档保存密码：

```sql
CREATE ROLE pgrac_backup LOGIN REPLICATION;
\password pgrac_backup
```

在 seed `pg_hba.conf` 中只允许三个 joiner 的精确地址，使用 `scram-sha-256`，例如：

```conf
host replication pgrac_backup 10.20.0.11/32 scram-sha-256
host replication pgrac_backup 10.20.0.12/32 scram-sha-256
host replication pgrac_backup 10.20.0.13/32 scram-sha-256
```

检查 `wal_level=replica`、`max_wal_senders` 足够并按 `pg_settings.context` 生效；不应以 `trust` 或 `0.0.0.0/0` 简化认证。joiner 使用权限为 0600 的 passfile 保存凭据。连接应处于上一节限定的加密通道或隔离网络中。

以 node1 为例：

```sh
pgrac-init -D /var/lib/pgrac/node1 --node-id=1 --cluster-join \
  --shared-data-dir=/srv/pgrac-shared/data \
  --wal-threads-dir=/srv/pgrac-shared/wal \
  --join-from='host=10.20.0.10 port=5432 user=pgrac_backup passfile=/var/lib/pgrac/backup.pass'
```

node2、node3 分别替换本地目录与 node ID。完成后先保持关闭，检查：

1. 四份 `system_identifier` 相同；四个 `cluster.node_id` 不同。
2. `pg_wal` 分别指向自身 `thread_1..4`，没有两个写者共用线程。
3. joiner 保留合法 `backup_label` 和所需 WAL。**不删除或伪造 label，不把普通 PGDATA 拷贝伪装成 base backup。**
4. CLI 会拒绝指回来源 WAL 线程的符号链接；离线备份必须实际包含所需 WAL，不是只有链接。
5. 备份中的 seed 配置要按节点修订；不能把 seed 的 `cluster.enabled=off`、监听地址、拓扑或 HBA 原封不动用于正式四节点启动。

**停止边界：** CLI 的旧备份初始化路径与 MVP 四实例验收的受控 cold-seed 路径不同；本次未重跑并认证“本标签 seed 后在线 base backup → 三个远端 joiner → 四机业务开放”的完整组合。该链或存储/投票接入未验证时，不继续接入业务。不要用删除文件、关闭共享目录检查或生成独立身份修补启动错误。

## 6. 四节点配置与开放检查

### 6.1 拓扑文件

将下面的 `pgrac.conf` 内容按实际地址填写，并在四节点保持一致。示例采用两个 LMS worker；每个 worker 使用 DATA 基端口加 worker_id，因此示例需放通节点间 TCP 7000、7100–7101。SQL 5432 只向获准的客户端开放，不能只放通 DATA 基端口。

```ini
[cluster]
name = mvp4

[node.0]
hostname = pgrac0
interconnect_addr = 10.30.0.10:7000
data_addr = 10.30.0.10:7100
public_addr = 10.20.0.10:5432
role = primary

[node.1]
hostname = pgrac1
interconnect_addr = 10.30.0.11:7000
data_addr = 10.30.0.11:7100
public_addr = 10.20.0.11:5432
role = primary

[node.2]
hostname = pgrac2
interconnect_addr = 10.30.0.12:7000
data_addr = 10.30.0.12:7100
public_addr = 10.20.0.12:5432
role = primary

[node.3]
hostname = pgrac3
interconnect_addr = 10.30.0.13:7000
data_addr = 10.30.0.13:7100
public_addr = 10.20.0.13:5432
role = primary
```

### 6.2 运行配置的必要组成

下面是配置项对照，不是可以绕过投票介质制备和集群开放步骤的完整安装脚本：

```conf
cluster.enabled = on
cluster.interconnect_tier = 'tier1'
cluster.lms_enabled = on
cluster.lms_workers = 2
cluster.allow_single_node = off
cluster.shared_storage_backend = 'cluster_fs'
cluster.shared_data_dir = '/srv/pgrac-shared/data'
cluster.wal_threads_dir = '/srv/pgrac-shared/wal'
cluster.smgr_user_relations = on
cluster.controlfile_shared_authority = on
cluster.shared_catalog = on
cluster.merged_recovery = on
cluster.xid_striping = on
cluster.online_join = on
cluster.clean_leave_enabled = on
cluster.quorum_poll_interval_ms = 2000
cluster.write_fence_enforcement = 'on'
cluster.crossnode_runtime_visibility = on
cluster.crossnode_write_write = on
cluster.crossnode_cr_data_plane = on
cluster.undo_gcs_coherence = on
cluster.past_image = on
cluster.read_scache = on
cluster.page_scn_shortcut = on
```

另行配置真实、已验证的 `cluster.voting_disks`、设备大小和节点监听信息。**不能照抄不存在的设备路径**。`cluster.node_id` 每节点唯一，其他正确性开关需形成一致配置。不要自动开启 RDMA、Smart Fusion、永久移除、故障恢复或注入开关。

`shared_buffers`、连接数和各共享表容量应按总主机内存规划；同机四实例要把四份实例内存相加。源默认值不等于发布验收的完整运行配置。特别是写栅栏默认租约 6000 ms，而实验验收曾使用 60000 ms；后者会延长隔离反应窗口，**不是未经故障隔离认证就可套用的生产推荐**。详见[参数篇](02-parameters.md)。

### 6.3 启动与状态门

四节点启动必须由协调方发出启动请求并收集所有结果，不宜等待 node0 完全 ready 后才允许其他节点启动。节点可能等待其他成员建立 formation。保留每节点日志；超时只记录失败，不自动强杀后重建。

四机准备/投票介质/冷启动编排验证完成后，分别查询：

```sql
SELECT * FROM pg_cluster_nodes;
SELECT node_id, state, interconnect_addr FROM pg_cluster_ic_peers;
SELECT * FROM pg_cluster_membership ORDER BY node_id;
SELECT * FROM pg_cluster_quorum_state;
SELECT * FROM pg_cluster_fence_state;
SELECT category, key, value FROM pg_cluster_state
 WHERE category IN ('phase','cf','pcm','normal_start','write_fence')
 ORDER BY category, key;
```

启动成功、SQL 端口开放、TCP `connected`、CSSD `alive`、membership `member`、quorum 有效和允许共享写入是**不同层的条件**。每个节点都要核对，单节点结果不能代表全集群。

源码提供管理命令 `ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL` 推进集群能力开放，但该命令涉及持久状态变化，并非“启动后总是执行一次即可”。`RF_DEFERRED`/`CONDITION_NOT_YET_MET` 表示尚未满足条件，不是成功；连接丢失后结果可能未知，必须先查状态。MVP 的受控夹具负责分阶段制备与开放；本文不把旧测试中的次数、时间和设备转换流程写成未经验证的四机运维 SOP。

投票制备、四机 cold-seed、开放编排三项未完成验证之前，部署应停在这里。不能用 `allow_single_node=on`、关闭写栅栏、手改控制文件或强制 feature bit 使检查变绿。

## 7. 安装后如何“建库”

PG 的 `initdb` 创建数据目录和 `postgres`、`template0`、`template1` 等初始数据库。PGRAC 的四节点共享的是同一个数据库身份，不是四次 `CREATE DATABASE`。[PG 16 初始化说明](https://www.postgresql.org/docs/16/creating-cluster.html)

**本版在 `cluster.shared_catalog=on` 时拒绝 `CREATE DATABASE` 和 `CREATE TABLESPACE`，不是管理员权限问题。** 不要暂时关闭该开关建库后再打开；已存在共享目录权威时这样做会被校验拒绝，且不属于安全迁移流程。

在四节点业务开放已验证的评估环境中，使用已有 `postgres` 库，为应用创建独立角色和 schema。仅在一个节点执行一次：

```sql
CREATE ROLE app_owner LOGIN;
\password app_owner
CREATE SCHEMA app AUTHORIZATION app_owner;
GRANT CONNECT ON DATABASE postgres TO app_owner;
```

之后以 `app_owner` 连接 `postgres`，创建业务表：

```sql
CREATE TABLE app.counter (
    id bigint PRIMARY KEY,
    value bigint NOT NULL
);
INSERT INTO app.counter VALUES (1, 0);
```

从另一个节点读取同一表，再分别执行一次普通 `UPDATE ... COMMIT`，验证最终值；不要在四节点重复创建同名表或重复导入初始数据。

```sql
BEGIN;
UPDATE app.counter SET value = value + 1 WHERE id = 1;
COMMIT;
SELECT * FROM app.counter ORDER BY id;
```

这只是部署烟测，不替代完整业务兼容性、并发与全行核对。不要向此预发布集群导入唯一生产数据副本。`UNLOGGED`、数据库/表空间级操作等限制见[核心能力篇](04-core-capabilities.md)。

## 8. 正常关机与同数据重启

已验证能力是**全体协调正常关闭后的同候选重启**，不是 `kill -9`、宿主机断电或强制停止后恢复。

1. 停止新业务流量，等待或按正常数据库机制取消现有业务。
2. 协调方在四台主机上尽快提交 `pg_ctl -D <本节点目录> -m fast -W stop`；不能停完一个再长时间等待才通知其他节点。
3. 保留全部退出状态与日志，等待每个 postmaster 正常退出；超时不自动升级为 immediate/kill。
4. 实例停止后，用相同安装的 `pg_controldata <PGDATA>` 逐一确认 `Database cluster state` 为 `shut down`，并核对正常关闭协议/债务记录。单条“shut down”日志不能代替四份证据。
5. 保留共享数据、WAL、投票介质及节点本地持久身份；不要重新运行 initdb、formatter，也不要删除 ALIVE/控制文件来解除拒绝。
6. 使用同二进制、原配置、原数据协调重启；重新做四节点身份、quorum、写入准入、全行一致性与健康检查。

若任一控制状态仍为 `in production`，或日志显示 PANIC/崩溃恢复要求，保留现场，不把它当正常重启继续。投票设备由部署所有者管理；数据库全部正常退出前不得卸载共享卷或拆除 loop/设备映射。

## 9. 常见阻断与交付清单

| 现象 | 首先检查 | 禁止的处理 |
|---|---|---|
| OpenSSL 构建缺 `pg_sha256_*` | 是否使用此标签已知失败的 SSL 组合 | 忽略链接失败、换成非集群构建冒充通过 |
| `system_identifier` 不同 | 是否各节点独立 initdb | 手改控制文件的身份 |
| join 缺 `backup_label` | 是否真实 base backup | 伪造 label、使用运行中 PGDATA 裸拷贝 |
| 启动缺 `data_addr` | 拓扑四节点的 DATA 地址和 worker 端口 | 把 CONTROL 地址当自动兜底 |
| quorum 不成立 | 投票介质身份、权限、大小、I/O、日志 | 放宽为单节点模式 |
| `CREATE DATABASE` 不支持 | 本版共享目录限制 | 临时关闭 shared_catalog |
| 连接成功但不能共享写 | formation、membership、quorum、能力开放与写栅栏 | 仅凭 `SELECT 1` 放行业务 |

交付记录至少包含四机软件清单、源码/二进制身份、完整有效配置、存储与设备身份、初始化来源、四节点开放证据、业务烟测、全行核对和正常重启记录。缺项应明确标为未验证。完整物理四机安装器、投票设备运维制备、TLS 构建及外部隔离认证尚未由本 MVP 手册变成已完成能力。
