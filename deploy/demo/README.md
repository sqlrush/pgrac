# PGRAC 单机四 Pod 安装与演示手册

本包在**一台 Linux 主机**上运行四个独立 PGRAC 实例，四个 Pod 共同访问该主机的持久目录和三个演示投票设备。
数据库版本为 **v0.131.0**，演示镜像版本为 **v0.131.0-demo.1**。

不需要编译数据库，不需要 Kubernetes，不需要购买共享 SAN，也不需要 GFS2。
Git 下载的是部署脚本和 Pod 清单生成器；数据库镜像从 GHCR 下载。
这是功能演示环境，不是四台物理机共享 LUN 的生产部署认证。

## 1. 安装前检查

建议专用 Linux 主机或 Linux 虚拟机：4 核以上、8 GiB 以上内存、20 GiB 以上可用磁盘。
脚本硬检查至少 4 GiB **可用**内存、10 GiB 可用磁盘；这不是性能容量保证。
需要 root/sudo、Podman、Python 3、util-linux，以及 Linux loop 设备。
禁止放在容器套容器、rootless Podman 或不提供 loop 设备的受限环境中。

以下按 Ubuntu 24.04 安装：

```bash
sudo apt-get update
sudo apt-get install -y podman python3 git util-linux
sudo modprobe loop
uname -m
podman --version
python3 --version
free -h
df -h /var/lib
findmnt -T /var/lib
test -c /dev/loop-control
```

Rocky Linux 9 的依赖命令：

```bash
sudo dnf install -y podman python3 git util-linux
sudo modprobe loop
```

### 存储要求

- `/var/lib/pgrac-demo` 必须位于主机的本地 ext4、XFS 或 Btrfs 文件系统；不得用 NFS、CIFS、FUSE、对象存储或跨主机共享挂载替代。
- **无需格式化磁盘、修改 fstab、配置 GFS2 或手工创建 loop 设备。**已有本地文件系统即可。
- 主机没有独立磁盘时，直接使用 `/var/lib` 所在文件系统。若要换磁盘，请由管理员先完成正常挂载，部署开始后不要变更挂载或目录身份。
- 工具只在 `/var/lib/pgrac-demo/<名称>` 下创建数据。已有同名非受管目录、符号链接、旧数据身份不匹配均拒绝，不会自动覆盖。
- 用户表数据以普通文件保存在共享目录；投票介质是三个专属文件映射出来的 loop **块设备**，工具验证 direct I/O 后才交给四个 Pod。不是把业务数据库放到裸盘。
- 三个投票设备仍在同一主机/磁盘上，没有三个独立故障域，也不提供存储高可用。

### 网络要求

默认只监听主机 `127.0.0.1`，不开放公网：

| 用途 | 默认端口 |
|---|---|
| 四个节点 SQL | 15432–15435 |
| 节点间控制通信 | 15532–15535 |
| 节点间数据通信 | 15632–15639 |

这些端口必须空闲。不需要开防火墙入站规则。不要把互联端口暴露给其他主机。
`--port-base` 可在首次部署时改 SQL 基础端口，控制端口偏移 +100、数据端口偏移 +200；同一部署后续保持原值。
所有 Pod 使用 host network，数据库进程使用 UID/GID 10001，**不是 privileged 容器**；部署工具使用 sudo 管理本演示专属设备。

镜像入口通过 `setpriv --no-new-privs` 禁止数据库及其子进程提权，并实际校验
UID 10001、零 capabilities、seccomp 开启。此标志在容器 AppArmor 配置生效之后设置，
避免部分 Ubuntu 24.04/crun 组合的 TCP 权限缺陷；不关闭主机 AppArmor/SELinux。
`up` 会在接触数据库目录前实际测试受限容器的 TCP socket；失败时先处理运行环境，不添加 privileged。

## 2. 下载部署包与镜像

```bash
git clone --depth 1 https://github.com/sqlrush/pgrac.git
cd pgrac/deploy/demo
sudo ./pgrac-demo check
sudo ./pgrac-demo pull
```

预检输出应包含 `"check": "PASS"`；拉取输出包含镜像名和实际 `image_id`。
默认镜像为 `ghcr.io/sqlrush/pgrac-demo:v0.131.0-demo.1`。
不需要 GitHub 账号或 `podman login` 才能拉取公开镜像。
同一部署会绑定实际镜像 ID；不能用重打标签悄悄给旧数据换版本。

离线客户现场：在相同 CPU 架构的联网 Linux 主机上准备：

```bash
sudo podman pull ghcr.io/sqlrush/pgrac-demo:v0.131.0-demo.1
sudo podman save --format oci-archive -o pgrac-demo-v0.131.0-demo.1.tar \
  ghcr.io/sqlrush/pgrac-demo:v0.131.0-demo.1
sha256sum pgrac-demo-v0.131.0-demo.1.tar > pgrac-demo-v0.131.0-demo.1.tar.sha256
```

把整个 `deploy/demo` 目录、镜像 tar、校验文件传到客户主机，安装依赖后执行：

```bash
sha256sum -c pgrac-demo-v0.131.0-demo.1.tar.sha256
sudo podman load -i pgrac-demo-v0.131.0-demo.1.tar
```

AMD64 镜像用于 x86_64，ARM64 镜像用于 aarch64；不要使用模拟运行的结果做性能比较。

## 3. 安装和启动四个节点

```bash
sudo ./pgrac-demo up
sudo ./pgrac-demo status
sudo podman pod ps
sudo podman ps
```

`up` 自动完成创建数据库目录、同构四节点初始化、投票设备配置、生成四份 Pod 定义、启动和集群就绪检查。
第一次初始化只创建表结构；业务测试数据在下一步插入。
启动成功时 `phase` 为 `READY`，`running_nodes` 为 `[0,1,2,3]`。
再次执行 `up` 只验证现有正常集群，不重新建库。

```text
Linux 主机
  ├─ Pod node0 → SQL 127.0.0.1:15432 ┐
  ├─ Pod node1 → SQL 127.0.0.1:15433 ├─ 同一共享用户数据目录
  ├─ Pod node2 → SQL 127.0.0.1:15434 ┤  + 三个演示投票块设备
  └─ Pod node3 → SQL 127.0.0.1:15435 ┘
     每节点有独立 PGDATA、WAL 和进程空间
```

生成的可检查 Pod 清单：

```bash
sudo less /var/lib/pgrac-demo/demo/pods.yaml
```

不要跳过控制器直接 `podman kube play`：设备编号、数据身份和正常停机记录必须先校验。
Pod 清单为 JSON 形式的 YAML 文档，由代码按实际设备生成，不要求客户修改 `/dev/loopN`。

## 4. 初始化测试数据

默认初始化 10,000 行：

```bash
sudo ./init-data.sh --rows 10000
sudo ./pgrac-demo verify
```

`init-data.sh` 创建 `postgres.public.demo_account` 的测试数据，另初始化跨节点写入探针 `demo_smoke`。
表结构已经由镜像初始化程序准备好：

| 列 | 含义 |
|---|---|
| `id integer` | 连续主键，1 到指定行数 |
| `value bigint` | 初始为 0；每笔已提交压测事务加 1 |
| `pad text` | 固定 64 字符载荷，校验期间不得改变 |

插入后执行 `VACUUM (FREEZE, ANALYZE)`，然后从四节点完整读取所有行并比较。
可选行数为 1,000–1,000,000。**已有数据时初始化脚本拒绝执行，不 DROP/TRUNCATE。**
`verify` 做全行一致性和四节点顺序提交可见性检查；会使 `demo_smoke.value` 增加 4。

查询任意节点，不必安装宿主机 psql，也不必输出密码：

```bash
printf '%s\n' 'SELECT count(*),sum(value) FROM demo_account;' | sudo ./pgrac-demo --node 0 sql
printf '%s\n' 'SELECT count(*),sum(value) FROM demo_account;' | sudo ./pgrac-demo --node 3 sql
printf '%s\n' 'SELECT * FROM pg_cluster_membership;' | sudo ./pgrac-demo --node 0 sql
```

不要在本演示环境执行任意 DDL 或 `pgbench -i`；这些不是本包的初始化流程。

## 5. 四节点压测

```bash
sudo ./bench.sh --clients 2 --seconds 30
```

含义：每节点 2 条连接，四节点合计 8 条连接，业务发起窗口 30 秒。
每笔事务随机选择已有主键，执行一次 `UPDATE value=value+1` 并 `COMMIT`。
脚本为 [sql/update.sql](sql/update.sql)，调用镜像内的 `pgbench`。

允许 `--clients 1..16`、`--seconds 1..600`。例如：

```bash
sudo ./bench.sh --clients 4 --seconds 60
```

业务窗口结束后等待已发出的事务自然结束，再进行全行对账和健康检查，因此**总耗时大于 `--seconds`**。
脚本不会把强制取消或客户端错误算作成功，也不承诺不同硬件上的 TPS。

`summary.json` 中关键字段：

| 字段 | 含义 |
|---|---|
| `verdict` | `PASS` 或 `FAIL`；不能只看 TPS |
| `nodes` | 每节点提交数、失败数、进程返回码和 TPS |
| `reported_tps_sum` | 四个 pgbench 报告 TPS 之和 |
| `wall_tps` | 总提交数 / 四客户端从启动到全部退出的墙钟时间 |
| `before` / `after` | 四节点行数、数值总和、完整数据及固定列摘要 |
| `server_errors` | 每节点本轮新增 ERROR/FATAL/PANIC 数 |
| `healthy` | 负载后四节点实际健康结果 |
| `image_id` / `source_revision` | 对应二进制镜像和源码身份 |
| `is_formal_pre` | 恒为 false：这是客户演示检查，不冒充正式 PRE |

PASS 要求四节点均有提交、客户端零错误/零非零 RC、服务端零新增错误、完整行内容一致、固定列不变、`sum(value)` 增量等于成功提交总数、负载后健康检查通过。
报告路径会直接打印，例如 `/var/lib/pgrac-demo/demo/reports/bench-.../`。

## 6. 正常停机与原数据重启

先停止外部业务，等待 `bench.sh` 结束，再执行：

```bash
sudo ./pgrac-demo stop
sudo ./pgrac-demo status
sudo ./pgrac-demo up
sudo ./pgrac-demo verify
sudo ./bench.sh --clients 2 --seconds 30
```

`stop` 同时请求四实例正常关闭，确认四份控制文件均已正常关闭及日志闭合后才释放本演示设备，成功输出 `CLEAN`。
`up` 复用原数据，**不用再次执行 init-data.sh**。
支持的是**四节点协调正常关闭/重启**；不保证随意单 Pod 重启、掉电、强杀后的恢复。

不要使用 `podman stop -a`、`podman rm -f`、`podman kube down --force`、`kill -9`，也不要删除 `postmaster.pid`。
主机需要关机/重启或虚拟机需要休眠时，先确认 `stop` 成功且状态为 CLEAN。
正常停机不成功时工具保留数据和设备，不会强杀、重置或悄悄恢复。

## 7. 日常检查与故障处理

```bash
sudo ./pgrac-demo status
sudo tail -n 100 /var/lib/pgrac-demo/demo/storage/server-log/node0.log
sudo find /var/lib/pgrac-demo/demo/reports -name summary.json -print
```

| 现象 | 处理 |
|---|---|
| 缺少 Podman/Python/loop-control | 按第 1 节安装、加载 loop；不要改成 privileged 容器绕过 |
| 端口已被监听 | 查 `sudo ss -ltnp`；选择空闲 `--port-base` 创建新部署，不杀其他服务 |
| 空间/内存不足 | 先正常停止本演示，扩容资源；不要删数据库文件 |
| 镜像 ID 与旧部署不符 | 使用原镜像；本包不支持原地升级 |
| INITIALIZING/STARTING/STOPPING 中断或缺节点 | 保留目录和日志，联系维护者；不把状态手工改为 CLEAN |
| 原数据非正常关闭 | 本包拒绝自动接管，不做崩溃恢复、不重建覆盖原目录 |
| 压测 FAIL | 保存 summary、四份 pgbench 日志和本轮服务器日志；TPS 不构成通过 |
| 校验需要较久 | SQL 验证上限 600 秒；超时返回失败，不会跳过校验 |

每次安装自动生成随机 SCRAM 密码，文件权限 0600；凭据留在本机，不提交 Git、不放镜像、不输出到状态报告。
不要公开打包整个 `storage`、bootstrap 日志或控制目录。提供报告前请检查是否含用户自行输入的 SQL/数据。
没有自动删除/reset 命令；本包不会代替管理员清理旧场景。

## 8. 从源码重建镜像（可选）

客户常规演示无需此步：

```bash
sudo podman build --network=host -t localhost/pgrac-demo:v0.131.0-demo.1 .
sudo ./pgrac-demo --name source-demo --image localhost/pgrac-demo:v0.131.0-demo.1 up
sudo ./init-data.sh --name source-demo --rows 10000
sudo ./bench.sh --name source-demo --clients 2 --seconds 30
sudo ./pgrac-demo --name source-demo stop
```

构建固定下载公开稳定源码 `83d8c5a002643581f153058b7a766afe1ffa1eb0`，校验压缩包 SHA-256，镜像包含 PostgreSQL 版权声明。
官方发布流程原生构建 AMD64/ARM64，并运行 `ci-smoke.sh` 的完整启动、数据、压测、正常重启链；只发布实际通过的镜像。

参考：[Podman kube play](https://docs.podman.io/en/latest/markdown/podman-kube-play.1.html)、[GHCR 镜像下载](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)、[数据库稳定版说明](../../docs/release-notes/v0.131.0.md)。
