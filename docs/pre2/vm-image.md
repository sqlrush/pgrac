# PRE2 ARM64 一体化 VM 镜像

Author: SqlRush <sqlrush@gmail.com>

本镜像用于隔离环境中的功能评估，包含一个 Ubuntu 24.04 ARM64 父机和四个独立 Linux guest、共享存储、集群服务及 50,000 行合成示例数据。产品固定为 `v0.133.1-pre2.1`，源码 commit `2d857abffc76a5cbfbc1b01c7d99b82ab0375f46`，release 构建，未启用 cassert/debug。

## 硬件与运行平台

| 项目 | 要求或配置 |
|---|---|
| 处理器 | 支持嵌套虚拟化的 Apple Silicon M3 或更新型号，ARM64；M1/M2 不属于本方案 |
| 系统与工具 | macOS 15 或以上、Lima 2.2.0 或以上；使用 Apple Virtualization (`vz`) |
| 内存 | 父机分配 40 GiB；建议宿主至少 64 GiB，预留宿主和其他应用的空间 |
| CPU | 父机 8 vCPU；四个 guest 各 2 vCPU |
| guest 内存 | 每节点 8 GiB |
| 磁盘 | 父机 128 GiB 稀疏虚拟盘；建议宿主至少 160 GB 可用空间用于解压、导入和后续增长 |

Lima 的嵌套虚拟化支持取决于宿主硬件及系统版本，参见 [Lima 配置模板的 nestedVirtualization 说明](https://github.com/lima-vm/lima/blob/master/templates/default.yaml)。本镜像面向上述 ARM64 Mac 环境，不提供 x86_64、Windows 或其他虚拟化平台的导入入口。

## 镜像内容与边界

- 四节点名称为 `pgrac-n0` 至 `pgrac-n3`，私有网络 `192.168.124.0/24`。父机 `.1`，四节点 `.111` 至 `.114`。
- 父机提供 libvirt/KVM、LIO iSCSI、qnetd 和时间服务。四节点运行 Corosync/qdevice、Pacemaker、DLM/GFS2 和同一 PGRAC 二进制。
- 使用新建的数据盘和三份投票介质。四成员以一个完整 appliance 为单位使用；不要把不同副本的节点、投票盘或共享数据盘混接，也不要桥接到已有集群。
- 预装表 `demo.accounts`：`id integer PRIMARY KEY`、`balance bigint`、`label text`，含 50,000 行合成数据。公开建表 SQL 在 `/opt/pgrac-appliance/example.sql`。
- SSH 主机密钥在新副本启动时重新生成；集群认证和 fencing 凭据由首次启动入口在该副本内部生成。镜像不携带制作机的私钥、用户公钥授权、历史日志或私有开发资料。
- SQL 管理通过父机的 QEMU guest agent 入口和 guest 本地 peer 认证执行。默认不向 Mac 或外部网络转发数据库端口，不提供默认数据库密码。

四节点采用 `lms_workers=2`、cleaner ON、退避 10 ms、`xnode_profile=off`、`update_trace=off`。校验和、`fsync`、`full_page_writes`、同步提交保持开启。适配每 guest 8 GiB 的缓冲配置为 `shared_buffers=512MB`、`max_connections=100`；连接数设置不代表建议的业务并发数。

## 下载、校验与合并

镜像分卷和 `SHA256SUMS` 由交付渠道一并提供，每卷不超过 2 GiB。请下载全部文件到同一新目录，不覆盖已有评估目录。

```bash
# 先核对每一卷。
shasum -a 256 -c SHA256SUMS

# 文件名含固定宽度数字后缀，按名称顺序合并。
cat pre2-arm64-2d857.tar.zst.part-* > pre2-arm64-2d857.tar.zst
shasum -a 256 -c ARCHIVE-SHA256SUMS

# 需要 zstd；解压后保留整个目录结构。
zstd -d -c pre2-arm64-2d857.tar.zst | tar -xf -
cd pre2-arm64-2d857
shasum -a 256 -c IMAGE-SHA256SUMS
```

解压目录包含独立磁盘、Lima 配置、`start.sh`、`test.sh`、`stop.sh`、`control.sh`、产品清单及校验文件。运行镜像不依赖制作机的磁盘路径或外部 backing file。请保留原始只读交付件，以便另建新副本。

## 一键启动

先安装 Lima、QEMU 的磁盘转换工具和 zstd（使用 Homebrew 时为 `brew install lima qemu zstd`）。在解压目录执行：

```bash
./start.sh
```

默认创建并启动独立 Lima 实例 `pgrac-pre2-demo`。首次启动会生成凭据，依次启动四个 guest、共享存储和集群服务，再启动四个数据库。成功后返回 `STARTED` 及节点列表。之后再次执行会启动同一副本的数据。

如需另一个全新副本，可选新的受限名称；对该副本的所有操作都使用同一环境变量：

```bash
export PGRAC_VM=pgrac-pre2-demo-eval02
./start.sh
```

脚本只接受 `pgrac-pre2-demo` 或 `pgrac-pre2-demo-<后缀>`，不会自动选择、清理或覆盖其他 Lima 实例。Homebrew 以外安装 Lima 时，设置 `PGRAC_LIMACTL` 为 `limactl` 的绝对路径。

## 示例测试与查询

```bash
./test.sh
./control.sh status
```

示例先检查四端的行数和主键唯一性，再由四端执行普通 `UPDATE+COMMIT`，最后核对四端数据及预期余额。它会改变合成示例的 `balance`，允许在同一副本再次运行；不会创建用户业务数据。成功返回 `EXAMPLE_PASS`。

执行单条查询（节点编号为 0–3）：

```bash
/opt/homebrew/bin/limactl shell "${PGRAC_VM:-pgrac-pre2-demo}" -- \
  sudo /usr/local/sbin/pgrac-demo sql --node 0 \
  --query 'SELECT id,balance,label FROM demo.accounts WHERE id BETWEEN 1 AND 4 ORDER BY id'
```

## 正常停止

```bash
./stop.sh
```

停止脚本并发向四个 PostgreSQL 实例发送正常 fast shutdown，各自等待 30 秒，并检查退出结果、正常关闭记录和停机日志。成功后按顺序停止共享文件系统、DLM、集群服务和四个 guest，再停止父机。`fast` 会结束现有客户端事务，不是立即终止进程。

若正常停止失败，脚本返回非零并保留父机和现场。此时保留 DATA、WAL 和日志并联系支持；不要直接删除虚拟盘或使用强制停止代替正常关闭。

## 日志与功能范围

父机操作结果位于 `/var/log/pgrac-appliance/`，四个 guest 的数据库日志位于 `/srv/pgrac/node/log/postgres.log`，版本、四节点身份、配置和原始包标识在父机 `/etc/pgrac-appliance/manifest.json`。从 Mac 可用 `limactl shell <实例名>` 查看父机；不要把含客户数据或新生成凭据的整个运行副本公开上传。

本版本是功能评估技术预览，不提供 HA 或故障切换能力，不用于生产部署。SQL 支持范围及拒绝行为见 [产品说明](product-overview.md)；独立安装步骤见 [安装手册](install.md)。
