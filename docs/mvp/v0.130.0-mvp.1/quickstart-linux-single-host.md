# 单机四实例 Quick Start（Linux）

Author: SqlRush <sqlrush@gmail.com>

目标：一台 Linux 主机、四个 PGRAC 进程、同一份共享业务数据。不是四台主机，也不需要容器或 GFS2。

版本固定为 `v0.130.0-mvp.1`。以下在 **Rocky Linux 9 / Btrfs** 上完成了编译、四实例共享行读写和全体正常关机演练。仅用于隔离评估，保留本版本的 CI 和非生产限制。

## 1. 准备 Linux 主机

使用有 `sudo` 权限的普通账号，预留充足内存和至少 20 GiB 磁盘空间。全程在同一个 Bash 终端执行；不要用 root 运行数据库。

```bash
bash
set -euo pipefail
test "$(id -u)" -ne 0
umask 077

sudo dnf install -y dnf-plugins-core
sudo dnf config-manager --set-enabled crb
sudo dnf install -y gcc make git curl pkgconf-pkg-config bison flex \
  perl perl-IPC-Run perl-Test-Simple perl-Time-HiRes \
  readline-devel zlib-devel libicu-devel lz4-devel libzstd-devel \
  util-linux procps-ng tar kmod
test -c /dev/loop-control || sudo modprobe loop
sudo losetup --find

export PGRAC_QS_ROOT="$(mktemp -d /var/tmp/pgrac-quickstart.XXXXXX)"
findmnt -T "$PGRAC_QS_ROOT"
df -h "$PGRAC_QS_ROOT"
printf '本次安装目录：%s\n' "$PGRAC_QS_ROOT"
```

该目录必须位于本机磁盘，不能使用 NFS 或主机共享映射目录。本机 ext4/XFS 不涉及跨主机挂载，但本次演练文件系统为 Btrfs。示例只创建自己的三个投票文件及对应 loop 设备，不要自行格式化现有盘。

## 2. 拉取 MVP 源码

```bash
git clone --depth 1 --branch v0.130.0-mvp.1 --single-branch \
  https://github.com/sqlrush/pgrac.git "$PGRAC_QS_ROOT/source"
test "$(git -C "$PGRAC_QS_ROOT/source" rev-parse HEAD)" = \
  c581f3835a4a9a76ce5a77c0937930f21765725a
```

## 3. 编译安装

本标签采用不启用 OpenSSL 的本机评估构建；SQL 只使用本机 Unix socket，不开放外部 SQL 端口。

```bash
mkdir "$PGRAC_QS_ROOT/build"
cd "$PGRAC_QS_ROOT/build"
../source/configure --prefix="$PGRAC_QS_ROOT/install" \
  --enable-cluster --enable-cassert --enable-debug --enable-tap-tests \
  --with-icu --with-lz4 --with-zstd
make -j4
make install
make -C src/test/cluster_tap all
make -C src/test/regress pg_regress

export PATH="$PGRAC_QS_ROOT/install/bin:$PATH"
pg_config --configure
```

## 4. 下载初始化示例

示例调用该标签已有的测试支持模块，源码版本不变。下载后先验证哈希，再执行。

```bash
curl -fL \
  https://raw.githubusercontent.com/sqlrush/pgrac/main/docs/mvp/v0.130.0-mvp.1/quickstart-single-host.pl \
  -o "$PGRAC_QS_ROOT/run-quad.pl"
printf '%s  %s\n' \
  0e5fe8670349c33485474f4757744507c9939a4149d2adef06a23f538ae9f4c9 \
  "$PGRAC_QS_ROOT/run-quad.pl" | sha256sum -c -

mkdir "$PGRAC_QS_ROOT/data" "$PGRAC_QS_ROOT/log"
cat > "$PGRAC_QS_ROOT/seed.conf" <<'CONF'
fsync = on
full_page_writes = on
synchronous_commit = on
CONF

export LC_ALL=C
export PERL5LIB="$PGRAC_QS_ROOT/source/src/test/perl"
export PG_REGRESS="$PGRAC_QS_ROOT/build/src/test/regress/pg_regress"
export PGRAC_DIRECT_IO_PROBE="$PGRAC_QS_ROOT/build/src/test/cluster_tap/pgrac_direct_io_probe"
export top_builddir="$PGRAC_QS_ROOT/build"
export TEMP_CONFIG="$PGRAC_QS_ROOT/seed.conf"
export TESTDATADIR="$PGRAC_QS_ROOT/data"
export TESTLOGDIR="$PGRAC_QS_ROOT/log"
export PG_TEST_NOCLEAN=1 PG_TEST_TIMEOUT_DEFAULT=180
export PGRAC_STAGE8_HAPPY_PATH_ONLY=1
unset PGRAC_TEST_TWO_STAGE_VOTING_LOOP
```

## 5. 初始化并启动四实例

脚本建立一个数据库身份及四个独立 PGDATA，共享业务数据文件；自动分配端口、配置互联与投票设备，创建 `postgres` 数据库中的 `quickstart_demo` 表。表结构在 seed 阶段创建后克隆；本示例不启用共享系统目录，请勿在运行后单独建表、改表或执行 `CREATE DATABASE`。

```bash
sudo -v
perl "$PGRAC_QS_ROOT/run-quad.pl" > "$PGRAC_QS_ROOT/launcher.out" 2>&1 &
export PGRAC_QS_PID=$!

for attempt in $(seq 1 360); do
  test ! -f "$PGRAC_QS_ROOT/READY" || break
  if ! kill -0 "$PGRAC_QS_PID" 2>/dev/null; then
    tail -n 60 "$PGRAC_QS_ROOT/launcher.out"
    tail -n 60 "$PGRAC_QS_ROOT/log/regress_log_run-quad"
    exit 1
  fi
  sleep 1
done
test -f "$PGRAC_QS_ROOT/READY"
source "$PGRAC_QS_ROOT/connect.env"
```

出现 `READY` 表示四实例已依次更新同一行，并都读到 `value=4`，不是仅进程启动成功。数据与日志路径在 `$PGRAC_QS_ROOT` 下，连接端口和四份 PGDATA 路径见 `connect.env`。

## 6. 连接并验证共享读写

```bash
# 四个实例都应读到 id=1、value=4。
for port in "$PGPORT_0" "$PGPORT_1" "$PGPORT_2" "$PGPORT_3"; do
  psql -X -v ON_ERROR_STOP=1 -p "$port" -c 'TABLE quickstart_demo'
done

# 在 node3 修改，随后在 node0 读取，应为 14。
psql -X -v ON_ERROR_STOP=1 -p "$PGPORT_3" \
  -c 'UPDATE quickstart_demo SET value=value+10 WHERE id=1 RETURNING *'
psql -X -v ON_ERROR_STOP=1 -p "$PGPORT_0" \
  -c 'TABLE quickstart_demo'

# 需要交互操作时使用；输入 \q 退出。
psql -X -p "$PGPORT_0"
```

## 7. 全体正常关机

退出交互 SQL、保持原终端打开，再执行：

```bash
sudo -v
touch "$PGRAC_QS_ROOT/STOP"
wait "$PGRAC_QS_PID"
test -f "$PGRAC_QS_ROOT/STOPPED"

for datadir in "$PGDATA_0" "$PGDATA_1" "$PGDATA_2" "$PGDATA_3"; do
  pg_controldata "$datadir" | grep 'Database cluster state'
done
```

预期四行均为 `shut down`。脚本先向四实例发送 fast shutdown，验证控制文件与关机日志，再释放自己创建的 loop 设备；不删除数据目录。

不要直接杀进程、运行 `losetup -D`，也不要在原目录重复运行初始化脚本。它是新建示例，不是原数据重启工具。失败时保留整个目录与日志；失败清理可能立即停止示例进程，不能把失败现场当成正常关机数据。

更多配置见[参数手册](02-parameters.md)；四机部署边界见[Linux 四节点部署](01-linux-four-node-deployment.md)。
