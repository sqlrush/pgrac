# PRE2 ARM64 单机四实例 VM 镜像

Author: SqlRush <sqlrush@gmail.com>

本镜像是 `v0.135.0` 功能评估技术预览。在一台 Ubuntu 24.04 ARM64 VM 中运行四个 PGRAC 实例，共享 VM 内的本地文件系统。数据库代码固定为 `2d857abffc76a5cbfbc1b01c7d99b82ab0375f46`，使用 release 构建，未启用 cassert/debug。镜像不需要嵌套虚拟化、iSCSI 或 GFS2；独立部署仍使用[安装手册](install.md)中的 iSCSI + GFS2 方案。

## Mac 最低配置

| 项目 | 本镜像要求 |
|---|---|
| CPU | Apple Silicon ARM64，至少 16 个 CPU 核心 |
| 内存 | 至少 96 GiB；建议 128 GiB，启动前为 VM 留出 64 GiB |
| 磁盘 | 至少 160 GB 可用空间，用于交付件、导入副本及数据增长 |
| macOS | macOS 15 或更新版本 |
| 工具 | Lima 2.2.0 或更新版本、QEMU 磁盘工具、zstd |
| VM 配置 | 16 vCPU、64 GiB 内存、128 GiB 稀疏虚拟盘 |

本方案使用 Apple Virtualization (`vz`) 和同架构 ARM64 guest，不要求 M3 的嵌套虚拟化功能。Lima 平台说明见 [VZ 文档](https://lima-vm.io/docs/config/vmtype/vz/)。最低资源要求针对本镜像的固定配置；请预留 macOS 和其他应用的内存。

在 Mac 终端安装工具：

```bash
brew install lima qemu zstd
limactl --version
sysctl -n hw.logicalcpu hw.memsize
```

## 镜像内容

四个实例使用同一份数据库程序，节点编号为 `0`–`3`。数据目录位于 `/srv/pgrac/demo/`，实例端口依次为 `5540`–`5543`。默认不向 Mac 或外部网络转发数据库端口；SQL 通过 VM 内的 Unix socket 和本地 `pgrac` 用户执行，使用 peer 身份认证，不预置数据库密码；TCP SQL 连接被拒绝。

预装 `demo.idx_lookup` 表，包含 50,000 行合成示例数据：`id integer PRIMARY KEY`、`payload varchar(100)`。建表 SQL 位于 `/opt/pgrac-single/example.sql`。四实例使用 `lms_workers=2`、cleaner ON、退避 10 ms、`xnode_profile=off`、`update_trace=off`，每实例 `shared_buffers=512MB`、`max_connections=100`。跨节点点更新使用预置的 `cluster.crossnode_write_write=on`。数据校验和、`fsync`、`full_page_writes` 和同步提交保持开启。

SSH 主机密钥在新副本首次启动时生成，集群服务凭据在该副本启动服务时生成。镜像不携带制作机的私钥、用户公钥授权、测试驱动、私有数据或历史目录。随包的示例与 pgbench 入口用于用户本地评估。每个副本作为一个完整的独立集群使用，不与其他副本或现有集群混接。

## 下载、校验与合并

从交付渠道下载全部分卷、`SHA256SUMS` 和 `ARCHIVE-SHA256SUMS` 到一个新目录。每卷不超过 2 GiB。

```bash
# 校验全部分卷，再按固定宽度数字后缀顺序合并。
shasum -a 256 -c SHA256SUMS
cat pre2-single-v0.135.0-arm64.tar.zst.part-* > pre2-single-v0.135.0-arm64.tar.zst
shasum -a 256 -c ARCHIVE-SHA256SUMS

# 解压，保留目录结构。
zstd -d -c pre2-single-v0.135.0-arm64.tar.zst | tar -xf -
cd pre2-single-v0.135.0-arm64
shasum -a 256 -c IMAGE-SHA256SUMS
```

解压目录包含独立磁盘 `pre2-single-arm64.qcow2`、Lima 配置、启动/测试/测速/停止脚本及产品清单。磁盘不依赖制作机路径或外部 backing file。保留原始交付件，以便创建新的评估副本。

## 本地安装与启动

在解压目录执行：

```bash
./start.sh
./control.sh status
./test.sh
```

默认创建独立 Lima 实例 `pgrac-pre2-single`，启动四个数据库实例，成功返回 `STARTED`。`test.sh` 检查四端行数、主键唯一性以及普通更新的结果一致性，成功返回 `EXAMPLE_PASS`。它只修改合成示例行，可再次执行。

如需另一个全新副本，选择新名称，并在该副本所有命令中保持同一环境变量：

```bash
export PGRAC_VM=pgrac-pre2-single-eval02
./start.sh
```

脚本只接受 `pgrac-pre2-single` 或 `pgrac-pre2-single-<后缀>`。Homebrew 以外安装 Lima 时，将 `PGRAC_LIMACTL` 设置为 `limactl` 的绝对路径。启动脚本不会重建已有 DATA。

执行单条 SQL：

```bash
/opt/homebrew/bin/limactl shell "${PGRAC_VM:-pgrac-pre2-single}" -- \
  sudo /usr/local/sbin/pgrac-single sql --node 0 \
  --query 'SELECT id,payload FROM demo.idx_lookup WHERE id BETWEEN 1 AND 4 ORDER BY id'
```

## 用 pgbench 测本机峰值

先完成上面的启动和示例检查。测量期间保持相同 VM 配置，尽量关闭其他耗 CPU、内存或磁盘的应用，并记录供电状态。

负载脚本 `/opt/pgrac-single/point-update.sql` 为随机主键点更新，每次事务包含一次 UPDATE 和 COMMIT：

```sql
\set key random(1, 50000)
BEGIN;
UPDATE demo.idx_lookup
SET payload = CASE WHEN left(payload, 1) = 'x'
                   THEN repeat('y', 80) ELSE repeat('x', 80) END
WHERE id = :key;
COMMIT;
```

一键顺序测量每实例 `16 → 32 → 48 → 64` clients，四实例同时产生负载，每档默认 60 秒：

```bash
./benchmark.sh
```

测速驱动在 VM 内独立运行，终端仅轮询结果；断开终端不会取消已启动的测量。命令先打印 job ID，结束后把结果写入当前 Mac 目录的 `benchmark-<job ID>.json`。VM 内的完整 pgbench 输出和数据库日志增量位于 `/var/log/pgrac-single/bench-*/`，任务输出位于 `/var/log/pgrac-single/jobs/<job ID>/`。

中断查看后可用打印过的 job ID 读取结果：

```bash
/opt/homebrew/bin/limactl shell "${PGRAC_VM:-pgrac-pre2-single}" -- \
  sudo /usr/local/sbin/pgrac-single job-status --job <job-ID>
```

如需统一改为每档 180 秒，请在新一组测量前设置 `PGRAC_SECONDS=180`。不要把不同时长或不同配置的样本混在同一组比较。

单独运行某一档，例如每实例 32 clients：

```bash
/opt/homebrew/bin/limactl shell "${PGRAC_VM:-pgrac-pre2-single}" -- \
  sudo /usr/local/sbin/pgrac-single bench --clients 32 --seconds 60
```

该入口为四个实例各运行一次下列 pgbench 命令，端口分别替换为 `5540`、`5541`、`5542`、`5543`，每实例固定 4 个 pgbench 工作线程。若手动执行，应在 VM 内以 `pgrac` 用户同时启动四条命令，而不是顺序测四个实例：

```bash
LD_LIBRARY_PATH=/opt/pgrac/lib /opt/pgrac/bin/pgbench \
  -n -M prepared -U pgrac \
  -h /srv/pgrac/demo/sockets -p 5540 \
  -c 32 -j 4 -T 60 -P 10 \
  -f /opt/pgrac-single/point-update.sql postgres
```

结果字段：

| 字段 | 如何读取 |
|---|---|
| `clients_per_instance` | 每实例并发数；集群总客户端数是它的四倍 |
| `valid` | 四个 pgbench 都自然结束、返回 0、都有成功事务且失败事务为 0，数据库窗口内没有 ERROR/FATAL/PANIC，才为 true |
| `transactions` | 四实例成功完成的事务总数；只用有效样本计分 |
| `common_window_tps` | 总事务数 ÷ 从第一个客户端开始到最后一个客户端结束的共同测量窗口；用它比较各档 |
| `sum_pgbench_tps` | 四个 pgbench 各自报告 TPS 的和；因连接建立时间及窗口端点不同，与共同窗口值可能不同 |
| `best_observed_clients` / `best_observed_tps` | 本次有效档位中读数最高的一档；是本机、本次扫描的观测峰值 |

遇到无效档位时，脚本保留原件并停止后续档位。每档一轮只能得到一次观测；若要判断结果是否稳定，可在相同配置、相同时长下再次完整测量并比较各档分布。文档不预设应达到的 TPS。

## 正常停止

确认测速任务已结束后执行：

```bash
./stop.sh
```

脚本并发对四实例执行 `pg_ctl -m fast -w -t 30 stop`，核对四端退出状态和本次正常关闭记录，然后停止集群服务和 VM。`fast` 会结束现有客户端事务，不是立即终止数据库进程。下次执行 `start.sh` 会启动同一副本的数据。

遇到异常时，保留数据和日志，联系技术支持。

操作输出位于 `/var/log/pgrac-single/`，数据库日志位于 `/srv/pgrac/demo/nodeN/postgres.log`；版本与包身份在 `/etc/pgrac-single/manifest.json`。不要公开上传含用户数据或新生成凭据的运行副本。产品使用范围见[产品说明](product-overview.md)。
