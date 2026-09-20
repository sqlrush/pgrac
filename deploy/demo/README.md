# PGRAC 单机四 Pod 安装与演示手册

> 发布状态（2026-09-20）：部署包已合入 main，预发布容器演练曾在 AMD64/ARM64
> 两种架构通过；但最终镜像发布复测在 AMD64 正常停机阶段发现 FATAL/PANIC，
> 因此 **v0.131.0-demo.1 镜像尚未发布，暂不可作为客户交付包使用**。
> 下列镜像拉取命令需等待发布解除阻塞后再执行；不要通过重跑碰运气、忽略错误
> 或强制重置数据绕过停机检查。失败记录见
> [发布验证](https://github.com/sqlrush/pgrac/actions/runs/35500993944)。

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

#### 1.1 采用什么共享存储

本包的“共享”是**同一台 Linux 主机上的四个 Pod 读写同一份持久数据**，不是四台主机同时挂载同一个 LUN。
主机文件系统只挂载一次；Pod 通过 `hostPath` 访问同一个目录，因此不需要 GFS2、NFS、SAN 或 Kubernetes PVC。

| 对象 | 宿主机上的存放方式 | 四个 Pod 中的访问方式 |
|---|---|---|
| 部署持久目录 | `/var/lib/pgrac-demo/demo/storage` | 四个 Pod 均映射为 `/demo`，读写同一份宿主机内容 |
| 共享业务数据 | `storage/tap/<生成的共享目录>` 中的普通数据库文件 | 四节点的 `cluster.shared_data_dir` 指向同一个 `/demo/tap/...` |
| PGDATA、WAL | 四个独立节点目录 | 每个实例使用自己的 PGDATA 和 WAL，不共用一个 PGDATA |
| 投票介质 | 三个专属文件，各映射到一个 loop 块设备 | 四节点访问同一组 `/dev/pgrac-vote0`、`1`、`2` |
| 管理与校验记录 | 部署根目录下的 `control.json`、`pods.yaml`、`reports/` | 不作为数据库共享关系目录使用 |

默认部署名称是 `demo`。使用 `--name customer1` 时，根目录变为 `/var/lib/pgrac-demo/customer1`；
下文示例中的名称和 Pod 名也应相应替换，后续所有命令保持同一名称。

```text
宿主机 /var/lib/pgrac-demo/demo/
├─ control.json                     root 管理的设备、镜像与数据身份记录
├─ pods.yaml                        四份 Pod 定义，含实际 hostPath / loop 映射
├─ reports/                         初始化、校验、压测结果
└─ storage/  ─────────────────────→ 四个 Pod 的 /demo
   ├─ bootstrap.json                实际共享目录、四节点 PGDATA、投票文件路径
   └─ tap/
      ├─ t_bootstrap_pgrac_demo_node0_data/pgdata/   节点 0
      ├─ t_bootstrap_pgrac_demo_node1_data/pgdata/   节点 1
      ├─ t_bootstrap_pgrac_demo_node2_data/pgdata/   节点 2
      ├─ t_bootstrap_pgrac_demo_node3_data/pgdata/   节点 3
      ├─ <生成的共享目录>/                         四节点共享业务数据
      └─ <生成的投票目录>/disk0、disk1、disk2        三个投票 backing 文件
```

两个生成目录通常名为 `tmp_test_XXXX`，后缀每次初始化不同，以上尖括号不是需要创建的目录名。
**这些目录虽然带 `tmp_test`，仍是本演示的持久数据，不可按临时文件清理。**
实际路径以 `storage/bootstrap.json` 为准，不能复制另一场景的随机后缀。

#### 1.2 安装前准备文件系统、容量与权限

1. 使用主机已有的本地 **ext4、XFS 或 Btrfs** 文件系统；脚本拒绝 NFS、CIFS、FUSE 等其他类型。
   已有演练在 Ubuntu 24.04/ext4（AMD64、ARM64）及 Rocky Linux/Btrfs（ARM64）上运行；
   XFS 在预检允许列表中，但本交付尚未做 XFS 专项实测。整包发布状态仍以上方警示为准。
2. 建议至少 20 GiB 可用空间，预检硬下限为 10 GiB。这个空间供共享数据、四份 PGDATA/WAL、
   日志和报告共同使用；镜像缓存还会占用 Podman 自己的存储空间，测试数据越多所需空间越大。
3. 使用默认磁盘时，**无需分区、格式化、修改 fstab 或手工创建 loop 设备**。
   如果要使用独立数据盘，请管理员在**首次部署之前**将已准备好的本地文件系统持久挂载到
   `/var/lib/pgrac-demo`，确认重启后仍会挂载同一文件系统；不要在已有场景上覆盖挂载、搬目录或换盘。
   本包不提供任意数据根路径参数，也不提供数据盘迁移功能。
4. 挂载根目录须由 root 管理，不允许组或其他用户写入。不要预建 `/var/lib/pgrac-demo/demo`：
   `up` 必须自己创建新的部署目录；碰到已有非受管同名目录会拒绝，而不是覆盖。
5. 不手工递归 `chmod/chown`。脚本将部署根目录设为 root 管理、0700，`storage` 设为
   UID/GID `10001:10001`、0700，并为数据库安排设备访问权限。不要改为 0777 或 privileged 容器。

在第 2 节下载脚本后，先运行只做预检的命令；它可能创建管理根目录，但不会初始化数据库或挂接投票设备：

```bash
sudo modprobe loop
test -c /dev/loop-control
sudo ./pgrac-demo check
findmnt -T /var/lib/pgrac-demo -o TARGET,SOURCE,FSTYPE,OPTIONS
df -h /var/lib/pgrac-demo
sudo stat -c '%U:%G %a %n' /var/lib/pgrac-demo
```

应看到 `"check": "PASS"`、预期的本地文件系统和足够空间。新建默认管理根目录为 `root:root 755`；
已有合规根目录可以更严格，但必须 root 所有且组/其他用户不可写。若另挂了数据盘，
还需人工核对 `findmnt` 的 `SOURCE` 确实是预定磁盘，预检 PASS 不替代这项检查。
不要直接使用普通 ext4/XFS/Btrfs 同时在多台主机上读写挂载同一个 LUN。

#### 1.3 Voting disk 如何制备与映射

`up` 对**全新部署**自动执行以下步骤，不需要客户运行 `mkfs`、`dd` 或手工 `losetup`：

1. 初始化程序创建 `disk0`、`disk1`、`disk2`，写入 PGRAC 所需的投票介质格式；
   不是仅创建三个空文件，也不是给它们格式化 ext4。
2. 每个文件固定为 **525,824 字节**，在映射前完成制备；这是本演示的固定介质大小，
   不是生产共享盘的容量建议，不能自行扩容后继续沿用原身份记录。
3. 控制器使用 `losetup --find --show --direct-io=on` 为三个文件分配三个空闲 loop 设备，
   再检查实际 direct I/O 状态、块设备容量、backing 文件和设备身份；不通过就拒绝启动。
4. 三个实际设备同时映射给四个 Pod。不是每个 Pod 各创建一份，也不把宿主机整个 `/dev` 挂进去。

```text
宿主机 backing 文件           宿主机设备（动态分配）       四个 Pod 内一致的设备名
<投票目录>/disk0 ───────────→ /dev/loopN ───────────────→ /dev/pgrac-vote0
<投票目录>/disk1 ───────────→ /dev/loopM ───────────────→ /dev/pgrac-vote1
<投票目录>/disk2 ───────────→ /dev/loopK ───────────────→ /dev/pgrac-vote2
```

`N/M/K` 代表三个不同的实际编号，不能在脚本或运维命令中假定总是 `loop0/1/2`。
数据库运行期间访问的是容器内的**块设备**，不是把 backing 文件路径直接当作最终投票配置。
`bootstrap.json` 的 `voting` 字段记录 backing 文件，而 `pods.yaml` 记录设备映射，二者用途不同。
控制器管理的设备访问权限为 UID/GID `10001:10001`、0600；成功正常停机后恢复原权限再解除映射。

**三个设备仍位于同一主机、同一底层文件系统，不是三个独立故障域。**
它们用于演示投票介质访问和四节点协同，不提供存储高可用，也没有替代生产外部 fencing。
本控制器没有接入客户现有 SAN/LUN 的配置入口，不要将真实共享盘设备替换进生成清单。

#### 1.4 四节点实际数据库配置

以下为脚本生成的配置示意，**无需手工粘贴到现有数据库**：

```conf
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '/demo/tap/<实际生成的共享目录>'
cluster.smgr_user_relations = on

cluster.voting_disks = '/dev/pgrac-vote0,/dev/pgrac-vote1,/dev/pgrac-vote2'
cluster.voting_disk_size_bytes = 525824

fsync = on
full_page_writes = on
synchronous_commit = on
```

| 参数 | 本包中的含义与核对要求 |
|---|---|
| `cluster.shared_storage_backend` | `cluster_fs`，通过文件系统访问共享关系数据 |
| `cluster.shared_data_dir` | 初始化时生成的容器内共享目录；四节点必须完全相同 |
| `cluster.smgr_user_relations` | `on`，用户关系使用共享存储路径 |
| `cluster.voting_disks` | 三个容器内块设备路径；顺序与 disk0/1/2 映射一致，四节点相同 |
| `cluster.voting_disk_size_bytes` | `525824`；与三个 backing 文件及设备实际容量一致 |
| 三个持久性开关 | 全部为 `on`；不要为提高演示 TPS 关闭 |

初始化遗留的较早配置行可能仍包含 backing 文件名；**以运行实例的 `SHOW` 结果为准**，
不要只取配置文件中第一次出现的 `cluster.voting_disks`。

#### 1.5 启动后逐项核验（完成第 3 节后执行）

先查路径、Pod 清单和设备映射，以下命令不修改数据：

```bash
sudo ./pgrac-demo status
sudo python3 -m json.tool /var/lib/pgrac-demo/demo/storage/bootstrap.json
sudo less /var/lib/pgrac-demo/demo/pods.yaml
sudo losetup --list --output NAME,BACK-FILE,DIO
```

`status` 应为 `READY` 且四节点运行；`bootstrap.json` 给出真实目录，
`pods.yaml` 中四个 Pod 的 storage `hostPath` 必须相同，均挂入 `/demo`。
在 `losetup` 输出中只核对 backing 路径属于本部署的三行，其 `DIO` 应均为 `1`；
其他行可能属于宿主机或其他应用，禁止对它们进行解绑或改权限。

然后逐节点查看数据库生效配置及 quorum（只读 SQL）：

```bash
for node in 0 1 2 3; do
  printf '\nnode%s\n' "$node"
  printf '%s\n' \
    'SHOW cluster.shared_storage_backend;' \
    'SHOW cluster.shared_data_dir;' \
    'SHOW cluster.smgr_user_relations;' \
    'SHOW cluster.voting_disks;' \
    'SHOW cluster.voting_disk_size_bytes;' \
    'SHOW fsync;' \
    'SHOW full_page_writes;' \
    'SHOW synchronous_commit;' \
    'SELECT in_quorum FROM pg_cluster_quorum_state;' \
    | sudo ./pgrac-demo --node "$node" sql || break
done
```

四个节点应分别输出：`cluster_fs`、同一个实际共享目录、`on`、相同的三个 `/dev/pgrac-vote*`
路径、`525824`、三个 `on`，最后 quorum 为 `t`。若某节点命令失败、输出缺失或不一致，
不要继续初始化/压测，也不要修改阈值让它通过。
这些配置检查不能代替数据校验：第 4 节初始化后还需运行 `verify`，第 5 节压测还需完整 PASS。

#### 1.6 正常停机、重启与数据保留

| 动作 | 数据和投票设备如何处理 |
|---|---|
| 首次 `up` | 新建数据与三个投票文件，校验并挂接设备，启动四节点 |
| 已是 READY 时再次 `up` | 检查已有四节点健康，不重新初始化 |
| `stop` 成功 | 四节点正常关闭、控制文件和协议收尾日志都通过后，恢复设备原权限并解除本部署的三个 loop 映射；保留文件和数据，标记 CLEAN |
| CLEAN 后 `up` | 使用相同镜像和原数据、原投票文件，重新核验身份并挂接；宿主机 loop 编号可能变化，容器内路径不变 |
| 停机/启动失败或异常退出 | 保留现场，不强杀、不重置、不自动恢复，不手工改成 CLEAN |

停机后 `losetup` 中本部署的三行消失是正常的，**不表示投票文件或数据库被删除**。
持久数据位于宿主机目录，不在可丢弃的容器可写层；但这不等于支持崩溃恢复。
只看到 `pg_control` 已 shutdown 也不够，必须由控制器确认完整正常停机条件。
当前发布复测发现的停机缺陷尚未解决，不能凭本节描述认定该场景已具备客户交付资格。

不要对现有场景重新制备投票文件、执行 `losetup -D`、删除 `tmp_test_*`，或移动/覆盖整个 `storage`。
若部署身份、设备映射或停机检查不通过，保存报告与日志，按第 7 节处理。

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
