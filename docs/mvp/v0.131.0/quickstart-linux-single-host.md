# 单机四实例 Quick Start（Linux，MVP 稳定版）

Author: SqlRush <sqlrush@gmail.com>

目标：一台 Linux 主机、四个 PGRAC 实例、同一份共享业务数据。版本固定为 `v0.131.0`。这是隔离环境中的源码安装示例，不是四机共享 LUN 或生产 HA 安装器。

沿用已演练的 Rocky Linux 9 / Btrfs 单机步骤与初始化示例；本次更新版本标签及文档链接，不声称重新运行了 Quick Start 演练。稳定版 CI 与 PRE 范围见[发布说明](../../release-notes/v0.131.0.md)。

已有集群切换本版时必须先全体正常关闭，再同构启动；不支持混版本或逐节点热替换。以下命令只创建独立的新示例，不覆盖旧数据。

## 1. 准备主机

使用有 `sudo` 权限的普通账号，预留充足内存和至少 20 GiB 磁盘空间。在同一个 Bash 终端执行，不要用 root 运行数据库。

```bash
bash
set -euo pipefail
test "$(id -u)" -ne 0
umask 077

sudo dnf install -y dnf-plugins-core
sudo dnf config-manager --set-enabled crb
sudo dnf install -y gcc make git pkgconf-pkg-config bison flex \
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

目录必须位于本机磁盘，不能使用 NFS 或主机共享映射目录。本机 ext4/XFS 不涉及跨主机挂载，但既有演练使用 Btrfs。示例仅创建自己的三个投票文件及对应 loop 设备，不格式化现有盘。

## 2. 拉取稳定版源码

```bash
git clone --depth 1 --branch v0.131.0 --single-branch \
  https://github.com/sqlrush/pgrac.git "$PGRAC_QS_ROOT/source"
git -C "$PGRAC_QS_ROOT/source" describe --exact-match --tags
git -C "$PGRAC_QS_ROOT/source" rev-parse HEAD
test "$(cat "$PGRAC_QS_ROOT/source/PGRAC_VERSION")" = 0.131.0
```

## 3. 编译安装

示例继续使用不启用 OpenSSL 的本机构建；SQL 只使用 Unix socket，不开放外部 SQL 端口。这不是稳定版的 OpenSSL 构建限制。

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

## 4. 准备初始化示例

`run-quad.pl` 来自刚拉取的稳定标签中的文档示例，不是额外下载的未知脚本。目录名保留其首次发布版本；示例字节未修改。

```bash
cp "$PGRAC_QS_ROOT/source/docs/mvp/v0.130.0-mvp.1/quickstart-single-host.pl" \
  "$PGRAC_QS_ROOT/run-quad.pl"
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

脚本建立一个数据库身份及四个独立 PGDATA，共享业务数据，自动配置端口、互联与投票设备。`postgres` 数据库中的 `quickstart_demo` 表在 seed 阶段创建后克隆。本示例不启用共享系统目录，运行后不要单独建表、改表或执行 `CREATE DATABASE`。

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

`READY` 表示四实例已依次更新同一行并都读到 `value=4`。连接端口和四份 PGDATA 路径见 `connect.env`。

## 6. 验证共享读写

```bash
for port in "$PGPORT_0" "$PGPORT_1" "$PGPORT_2" "$PGPORT_3"; do
  psql -X -v ON_ERROR_STOP=1 -p "$port" -c 'TABLE quickstart_demo'
done

psql -X -v ON_ERROR_STOP=1 -p "$PGPORT_3" \
  -c 'UPDATE quickstart_demo SET value=value+10 WHERE id=1 RETURNING *'
psql -X -v ON_ERROR_STOP=1 -p "$PGPORT_0" -c 'TABLE quickstart_demo'
```

第一次四次查询均应为 `id=1, value=4`；最后 node0 应读到 `value=14`。

## 7. 全体正常关机

```bash
sudo -v
touch "$PGRAC_QS_ROOT/STOP"
wait "$PGRAC_QS_PID"
test -f "$PGRAC_QS_ROOT/STOPPED"

for datadir in "$PGDATA_0" "$PGDATA_1" "$PGDATA_2" "$PGDATA_3"; do
  pg_controldata "$datadir" | grep 'Database cluster state'
done
```

预期四行均为 `shut down`。脚本完成正常关机验证后仅释放自己创建的 loop 设备，不删除数据。不要强杀、执行 `losetup -D` 或在原目录重复初始化。该脚本只用于新建示例，不是原数据重启工具；失败时保留目录与日志，不能把失败现场当作干净关机数据。
