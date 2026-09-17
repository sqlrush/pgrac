# 共享存储安装前准备与校验

Author: SqlRush <sqlrush@gmail.com>

适用：`v0.130.0-mvp.1`。本附录属于[Linux 部署篇](01-linux-four-node-deployment.md)，应在数据库初始化和首次共享写入之前执行。

**四台独立 Linux 主机的具体参考路线是：共享块存储 → 共享 LVM → GFS2 → PGRAC `cluster_fs`。这是待做 PGRAC 四机验证的准备方案，不是已经认证的部署组合。** 本文不会把本机四实例验收改称四机存储验收，也不会替你格式化磁盘或执行隔离操作。若需要拿来即用的已认证四机版本，当前 MVP 尚不满足。

## 1. 先选对场景和文件系统

| 场景 | 准备什么 | 当前证据与限制 |
|---|---|---|
| 同一 Linux 内核内运行四个实例 | 同一个本地持久文件系统目录，四实例指向同一根；不需要 GFS2 | 现有实验现场的数据根只读检查为本机 Btrfs；这是现场当前状态，不是任意 Btrfs 配置认证 |
| 四台独立主机，本文参考路线 | 四机可同时访问的共享 LUN、共享 LVM、GFS2、配套 DLM/集群管理与 fencing | 采用 RHEL 9 x86_64 厂商文档作为环境准备参考；尚需 PGRAC 四机实测 |
| 已有 OCFS2 或其他集群文件系统 | 供应商支持的完整配置，加 PGRAC 的同等验证 | 本文不把它列为已经支持的替代路线 |
| 普通 NFS/NAS 共享目录 | 不能仅凭能挂载、`noac` 或 `actimeo=0` 就放行 | 不作为本 MVP 的默认四机准备路线；客户端缓存、锁与同步写语义仍需专门验证 |
| 裸设备 `block_device` | 独立的设备布局、初始化与隔离方案 | 不与本附录混用；有后端代码不等于整个数据库都能只配置一个 LUN |

**禁止四机同时读写挂载同一 LUN 上的 ext4、XFS 或 Btrfs。** 它们用于单机本地目录不等于能直接多机共享挂载。四份本地目录经 rsync 同步也不满足共享存储要求。

GFS2 管理多节点共享块设备上的文件系统一致性；它不是 PGRAC 自带组件。[GFS2 官方规划说明](https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/configuring_gfs2_file_systems/assembly_planning-gfs2-deployment-configuring-gfs2-file-systems)。NFS 的 `noac` 不等于取消数据缓存，不能用它替代实测。[Linux NFS 数据一致性说明](https://man7.org/linux/man-pages/man5/nfs.5.html#DATA_AND_METADATA_COHERENCE)

## 2. 交给存储管理员的准备清单

```text
四台主机：pgrac0 / pgrac1 / pgrac2 / pgrac3
    │  均能识别同一共享存储身份，不能只比较 /dev/sdX 名字
    ▼
共享 LUN（FC/iSCSI 等，需目标环境支持；按需配置多路径）
    ▼
共享 VG / LV（示例 vg_pgrac / lv_shared）
    ▼
GFS2：lock_dlm，四个挂载节点至少四份 journal
    ▼
四机同挂载点 /srv/pgrac-shared
    ├─ data/：由 seed 初始化共享数据根
    ├─ wal/ ：由初始化工具建立每节点独占写入的 WAL 线程
    └─ .pgrac-preflight.随机值/：仅用于安装前可丢弃文件检查
```

投票介质另行准备，不能用上述测试文件充当投票盘。每机 PGDATA 仍放独立的本地目录。不要把四机 PGDATA 都指向 GFS2 上的同一目录。

| 必须交付的项目 | 具体要求 |
|---|---|
| 操作系统 | 四机同发行版系列、架构、内核与存储软件版本；本参考路线固定为 RHEL 9 x86_64，不把它的包名或支持范围外推到 Ubuntu/ARM |
| 订阅与组件 | 可用的 Resilient Storage/HA 软件渠道；`pcs`、Pacemaker、Corosync、`dlm`、`lvm2-lockd`、`gfs2-utils`；按实际存储准备多路径、iSCSI 与 fence agent |
| 设备身份 | 四机核对同一 LUN 的 WWID/序列号、容量、逻辑/物理扇区、共享 VG/LV UUID；多路径别名与每机设备号不要求相同 |
| 容量 | 数据和索引增长、Undo 与保留空间、四条 WAL 线程及保留量、文件系统元数据/journal、运维余量分别预算；不能只按初始导入数据量配盘 |
| 存储持久化 | 阵列/磁盘写缓存、掉电保护、同步写和 flush 完成语义有记录；不得关闭屏障或以易失缓存冒充持久完成 |
| 网络与时钟 | 存储网、集群管理网按目标环境规划；四机解析一致、时钟同步；管理和 fence 端口只向授权网络开放 |
| 账号与目录 | 数据库账号 `pgrac` 的数字 UID/GID 四机一致；共享挂载点及指定新目录允许该账号访问；不使用 `chmod 777` |
| 隔离 | 文件系统集群的每个节点都有正确映射、真实可用的 STONITH/fencing；凭据不放在命令历史、公开日志或仓库 |
| 交付记录 | 软件版本、设备身份、挂载/资源配置、权限、四机检查输出和维护责任人；对外提供记录前脱敏 |

GFS2 的存储侧 fencing 与 PGRAC 的外部 fencing 接入/认证是两层能力；完成前者不能把后者标成通过。

## 3. 安装数据库前的配置顺序

### 3.1 先完成系统与存储栈

由管理员在专用新环境完成，不要在已有生产集群上直接套用：

1. 配齐同版本软件渠道及上表组件，确认该发行版/架构确实支持 GFS2 集群部署。
2. 完成共享 LUN 映射和多路径策略。四机必须看到同一块介质；不把同容量的四块独立盘当成共享盘。
3. 建立四节点 Pacemaker/Corosync 集群及实际 fence 映射。先在数据库不存在的维护窗口完成厂商要求的隔离验证；本文不提供对现有主机的断电命令。
4. 再建立锁服务、共享卷与文件系统资源。iSCSI/多路径等底层依赖必须在依赖它的集群资源之前可用。
5. 四机文件系统、权限、下述检查通过后，才开始数据库 seed/join；不得在挂载缺失时让程序在同名本地目录初始化。

参考路线应由存储管理员交付下列配置结果，而不是只交付一个“可以 ls 的目录”：

| 配置对象 | 参考值/必须满足的结果 |
|---|---|
| `/etc/lvm/lvm.conf` | `use_lvmlockd = 1`；若启用 LVM devices file，四机均纳入正确的共享设备 |
| Pacemaker 属性 | `stonith-enabled=true`；GFS2 路线 `no-quorum-policy=freeze`，不是 `ignore` |
| DLM 与 lvmlockd | 四机均运行，集群资源有失败处置和正确顺序 |
| 共享 VG/LV | 由 lvmlockd 协调；`LVM-activate` 使用 `activation_mode=shared`、`vg_access_mode=lvmlockd` |
| GFS2 创建参数 | `lock_dlm`；锁表名为实际 `存储集群名:唯一文件系统名`；四个挂载节点至少 4 个 journal；不是 `lock_nolock` |
| 文件系统块大小 | 保留所选平台的厂商建议值并记录；GFS2 常用 4KB，不能因为数据库页为 8KB 就强行改成 8KB |
| 文件系统资源 | 指向已核对的共享 LV；`fstype=gfs2`；挂载点 `/srv/pgrac-shared`；资源在四机运行 |
| 顺序/共置 | DLM/lvmlockd → 共享 LV 激活 → GFS2；相关资源 clone、order、colocation 一致 |
| 挂载管理 | 此路线由 Pacemaker 管理；不再让 `/etc/fstab`、独立 systemd mount 和手工脚本重复争管 |
| 数据库生命周期 | 只在准确共享卷已挂载且存储集群健康时启动；卸载/停存储栈之前，先让全部数据库实例正常退出 |

以上是配置交付项。完整资源创建语法、厂商支持约束及不同版本差异遵循[官方 GFS2 集群配置步骤](https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/configuring_gfs2_file_systems/assembly_configuring-gfs2-in-a-cluster-configuring-gfs2-file-systems)。**共享 VG/LV 创建和格式化只能针对经批准的全新目标，由一个节点执行一次；不能四机各自格式化。** 本文不生成可误覆盖已有磁盘的 `mkfs`、`pvcreate` 或 `vgcreate` 命令。

不能通过关闭 SELinux、防火墙、quorum 或 fencing 来让检查变绿。安全策略不兼容时按发行版支持方案处理，并重新核对权限/审计记录；不要直接复制本机实验环境的挂载选项。

### 3.2 四机分别执行的只读检查

以下命令不创建文件、不改变挂载和集群属性。需要管理权限的查询由管理员运行。`pcs` 子命令以安装版本的帮助为准；命令不存在不能记为通过。

```sh
uname -r
uname -m
rpm -q pacemaker corosync pcs dlm lvm2-lockd gfs2-utils
lsblk -o NAME,TYPE,SIZE,FSTYPE,UUID,WWN,MOUNTPOINTS
sudo multipath -ll
sudo vgs -o +locktype
sudo lvs -a -o +devices
sudo lvmconfig --type current global/use_lvmlockd
sudo pcs status --full
sudo pcs quorum status
sudo pcs property config
sudo pcs constraint config
chronyc tracking
```

没有多路径的已批准配置，将 `multipath` 项记为 N/A 并说明原因，不安装假的路径来满足表格。上面输出要核对实际四个节点、quorum、资源运行位置及失败记录；只有进程存在不够。

挂载、空间和权限检查：

```sh
mountpoint -q /srv/pgrac-shared
findmnt --mountpoint /srv/pgrac-shared -o TARGET,SOURCE,FSTYPE,OPTIONS,UUID
realpath -e /srv/pgrac-shared
stat -c '%u:%g %a %n' /srv/pgrac-shared
id pgrac
df -h /srv/pgrac-shared
df -i /srv/pgrac-shared
sudo -u pgrac test -r /srv/pgrac-shared
sudo -u pgrac test -w /srv/pgrac-shared
sudo -u pgrac test -x /srv/pgrac-shared
sudo journalctl -k --since '1 hour ago' --no-pager
```

每条命令单独检查退出码；不能只看最后一条的结果。四机路线必须满足：

- `mountpoint` 返回 0，且 `findmnt --mountpoint` 的 **TARGET 恰为指定挂载点、FSTYPE 为 gfs2、含 rw**。不能以 `findmnt -T` 显示的父目录本地文件系统代替挂载成功。
- 四机文件系统 UUID、底层 LUN 身份和 LV 映射相符；不能只比较路径字符串，也不要求各机内核设备号相等。
- UID/GID 与权限一致；空间/inode 满足容量预算；无未解释的 GFS2 withdraw、I/O error、只读重挂载、DLM 或 fence 失败。
- `data/`、`wal/` 和四机 PGDATA 为本次初始化预留的新路径；已有内容或符号链接必须先识别，不删除、覆盖或重新格式化。
- 故障/恢复会引起阻塞的资源当前处于健康状态；文件系统预检查没有豁免数据库自身的投票、成员准入和写栅栏检查。

## 4. 只在新建测试目录做跨节点功能检查

这些是安装前的最小 smoke，不是完整四机数据库验收或掉电持久性认证。数据库尚未初始化，所有写入仅限本节新建目录；不得使用数据文件、WAL、投票介质或裸设备。

### 4.1 建立唯一测试目录

在写者节点，以 `pgrac` 账号运行；以下护栏只用于本附录的 GFS2 四机路线：

```sh
set -eu
PGRAC_SHARED_ROOT=/srv/pgrac-shared
test "$(id -un)" = pgrac
mountpoint -q "$PGRAC_SHARED_ROOT"
test "$(findmnt -rn --mountpoint "$PGRAC_SHARED_ROOT" -o FSTYPE)" = gfs2
test "$(realpath -e "$PGRAC_SHARED_ROOT")" = "$PGRAC_SHARED_ROOT"
umask 077
PGRAC_PREFLIGHT_DIR=$(mktemp -d "$PGRAC_SHARED_ROOT/.pgrac-preflight.XXXXXXXX")
export PGRAC_PREFLIGHT_DIR
printf '%s\n' "$PGRAC_PREFLIGHT_DIR"
```

把输出的**完整实际路径**传给另外三节点，在对应 `pgrac` shell 中设置同一个 `PGRAC_PREFLIGHT_DIR`；不在各节点另建同名目录。核对它直接位于已验证挂载点之下、没有符号链接，且是本轮新目录。下面的 Python 3 仅是检查工具，不是数据库运行依赖。

### 4.2 写后读：必须覆盖已打开的文件描述符

这项不能用“写者关闭文件，读者重新打开后看见新数据”代替。每一有向节点对都用一个全新的测试目录重复以下三步，共 12 对；记录写者、读者、路径和结果。

**步骤 A：写者创建 8KB 内容 A 并同步。** 使用排他创建，已有文件时直接失败：

```sh
python3 - <<'PY'
import os
from pathlib import Path
p = Path(os.environ['PGRAC_PREFLIGHT_DIR'])
if (p.is_symlink() or p.resolve().parent != Path('/srv/pgrac-shared')
        or not p.name.startswith('.pgrac-preflight.')):
    raise SystemExit('unexpected scratch directory')
with (p / 'visibility.bin').open('xb') as f:
    if f.write(b'A' * 8192) != 8192:
        raise SystemExit('short write')
    f.flush()
    os.fsync(f.fileno())
fd = os.open(p, os.O_RDONLY | os.O_DIRECTORY)
os.fsync(fd)
os.close(fd)
print('A_SYNCED')
PY
```

**步骤 B：读者先打开同一文件并读 A，停在提示处，暂不按回车。** 文件描述符一直保持打开：

```sh
python3 - <<'PY'
import os
from pathlib import Path
p = Path(os.environ['PGRAC_PREFLIGHT_DIR'])
if (p.is_symlink() or p.resolve().parent != Path('/srv/pgrac-shared')
        or not p.name.startswith('.pgrac-preflight.')):
    raise SystemExit('unexpected scratch directory')
fd = os.open(p / 'visibility.bin', os.O_RDONLY | os.O_NOFOLLOW)
if os.pread(fd, 8192, 0) != b'A' * 8192:
    raise SystemExit('initial read mismatch')
print('A_READ: wait for writer B_SYNCED, then press Enter here', flush=True)
with open('/dev/tty') as tty:
    tty.readline()
if os.pread(fd, 8192, 0) != b'B' * 8192:
    raise SystemExit('open-fd coherence mismatch')
os.close(fd)
print('OPEN_FD_COHERENCE_PASS')
PY
```

**步骤 C：回到写者，原位置覆盖成 B 并同步。** 看到 `B_SYNCED` 后才去读者终端按回车；不得在两步间 drop_caches、重新挂载或重开读者文件：

```sh
python3 - <<'PY'
import os
from pathlib import Path
p = Path(os.environ['PGRAC_PREFLIGHT_DIR'])
if (p.is_symlink() or p.resolve().parent != Path('/srv/pgrac-shared')
        or not p.name.startswith('.pgrac-preflight.')):
    raise SystemExit('unexpected scratch directory')
fd = os.open(p / 'visibility.bin', os.O_RDWR | os.O_NOFOLLOW)
if os.fstat(fd).st_size != 8192 or os.pread(fd, 8192, 0) != b'A' * 8192:
    raise SystemExit('unexpected scratch content')
if os.pwrite(fd, b'B' * 8192, 0) != 8192:
    raise SystemExit('short write')
os.fsync(fd)
os.close(fd)
print('B_SYNCED')
PY
```

三个命令均需退出码 0，且读者输出 `OPEN_FD_COHERENCE_PASS`。阻塞、超时、旧字节或短读写均不得记为成功；保留现场，不通过加等待、取消校验或 drop_caches 制造通过。本步骤只有 8KB smoke，不能推出任意并发/故障下都正确。

### 4.3 跨节点文件锁检查

同一新测试目录内，写者先持锁，保持终端停在提示处：

```sh
set -eu
test -d "${PGRAC_PREFLIGHT_DIR:?set the exact scratch directory first}"
case "$PGRAC_PREFLIGHT_DIR" in
  /srv/pgrac-shared/.pgrac-preflight.*) ;;
  *) exit 1 ;;
esac
test "$(dirname "$(realpath -e "$PGRAC_PREFLIGHT_DIR")")" = /srv/pgrac-shared
flock -x "$PGRAC_PREFLIGHT_DIR/lock" sh -c \
  'printf "LOCK_HELD; press Enter to release\n"; read -r answer'
```

另一节点使用相同账号/路径执行，持锁期间预期退出码 **75**；写者按回车退出后，再执行预期 **0**：

```sh
set -eu
test -f "${PGRAC_PREFLIGHT_DIR:?set the exact scratch directory first}/lock"
test "$(dirname "$(realpath -e "$PGRAC_PREFLIGHT_DIR")")" = /srv/pgrac-shared
if flock -n -E 75 "$PGRAC_PREFLIGHT_DIR/lock" true; then
    printf 'ACQUIRED rc=0\n'
else
    PGRAC_LOCK_RC=$?
    printf 'NOT_ACQUIRED rc=%s\n' "$PGRAC_LOCK_RC"
fi
```

所有非 0 都不能一概当作锁冲突通过，例如权限、I/O 或路径错误也会失败。四机交叉检查并记录；禁止把本地锁误当跨机互斥。这个锁测试也不替代 PGRAC GES 业务验证。

### 4.4 还必须完成的存储交付检查

| 项目 | 如何检查 | 通过条件/边界 |
|---|---|---|
| 创建与扩展 | 各节点在新目录创建自己独占命名的文件，分块写入和扩展、同步后交给其他节点校验长度及 SHA-256 | 四写者均成功，其余节点内容一致；不并发覆盖同一字节范围 |
| 目录与原子 rename | 测试工具写新临时文件并 fsync，再同目录 rename，fsync 父目录；其他节点读取公布后的文件 | 无部分内容、旧名称/新名称异常；跨文件系统 rename 不是此项 |
| 正常持久化 | 保存已 fsync 的测试文件清单和哈希；只在无数据库且获准的维护窗口，由存储管理员协调正常卸载/重挂后复核 | 内容一致；这仍不是断电/控制器故障认证 |
| 隔离验证 | 使用独立空白存储环境和厂商流程，核对实际 victim→设备/电源映射及隔离后旧节点不能访问共享盘 | 必须有真实证据；资源配置存在、ping 不通或软件标记不等于通过 |
| 启停顺序 | 冷启动和正常停止演练时检查存储依赖、锁资源、挂载的顺序；检查数据库不会误写本地同名目录 | 一份明确的存储生命周期管理权；禁止双重自动挂载和带数据库强制卸载 |

`fsync` 返回 0 只能证明接口调用成功；掉电后不丢已确认写入仍需存储设备保证与单独认证。`fio` 吞吐数字、同机测试、文件锁通过或本节 smoke 都不替代该证明。

测试目录先保留为证据。清理时由管理员根据本轮记录核对**精确目录**后处理；不提供针对共享根的递归删除命令。

## 5. 安装准入记录与明确停止条件

每台主机保存：时间、主机身份、OS/内核/包版本、LUN/VG/LV/文件系统身份、挂载选项、空间和权限、集群状态、12 对可见性/锁检查结果、持久化与隔离交付记录。凭据和管理网络信息只保留在受控位置。

- **允许进行数据库安装/接入验证**：存储管理员确认上述准备项通过，且 PGRAC 的投票介质、初始化来源和四机接入流程也已准备好。只证明“可以开始下一阶段”，不是“PGRAC 四机部署完成”。
- **必须停止**：挂错盘或挂载缺失、四机存储身份不同、普通本地文件系统多机挂同一盘、缺真实 fence、quorum/锁资源不健康、出现旧字节/短读写/I/O 错误、同步或目录操作未验证。
- **不得宣传为已认证**：只有本机四实例结果、只有厂商支持 GFS2、仅本节 smoke 通过，或仍缺 PGRAC 四机全行/健康/正常重启证据。

本次文档补充只核对厂商资料和代码/现场只读信息；没有执行 GFS2 配置、格式化、跨机写入、故障注入或数据库重新资格化。
