# Install

> MVP 1 用户请优先阅读[固定版本部署手册](../mvp/v0.130.0-mvp.1/01-linux-four-node-deployment.md)。下文含历史示例，不能作为当前四节点共享数据库的完整部署步骤；MVP 标签的 OpenSSL 构建与多主机接入边界见新手册。


linkdb is a PostgreSQL 16.13 fork that adds cluster-aware features.
Building it follows the standard PG `configure` / `make` / `make install`
flow with extra `--enable-cluster` and `--enable-tap-tests` flags.

## Dependencies

### macOS (Homebrew)

```bash
brew install readline icu4c lz4 zstd openssl@3 libxml2 cpanminus pkg-config

# Perl modules required for TAP tests:
cpanm --local-lib=$HOME/perl5 --notest IPC::Run IO::Tty
export PERL5LIB=$HOME/perl5/lib/perl5
```

### Linux (Debian / Ubuntu)

```bash
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
    build-essential libreadline-dev zlib1g-dev libicu-dev \
    liblz4-dev libzstd-dev libssl-dev libxml2-dev \
    libipc-run-perl pkg-config
```

## Configure + build (cluster mode)

```bash
git clone https://github.com/sqlrush/linkdb.git
cd linkdb

./configure \
    --prefix=$HOME/linkdb-install \
    --enable-cassert \
    --enable-debug \
    --with-openssl --with-icu --with-lz4 --with-zstd \
    --enable-cluster \
    --enable-tap-tests

make -j$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu)
make install
```

Notes on flags:

- `--enable-cluster` activates the cluster subsystem and registers
  the `cluster.*` GUCs and the cluster system views (see the
  Configuration and System Views references).  Without this flag
  the build produces a binary equivalent to vanilla PostgreSQL.
- `--enable-tap-tests` is required to run the TAP test suites under
  `src/test/cluster_tap` and `src/bin/pgrac/t`.
- `--enable-cassert` enables `Assert()` macros.  Recommended for
  development; turn off for benchmarking (cassert adds ~30-40 %
  overhead on small-row OLTP workloads).

## Verify the install

```bash
$HOME/linkdb-install/bin/postgres --version
# postgres (PostgreSQL) 16.13 (...)

$HOME/linkdb-install/bin/pgrac-init --version
# pgrac-init (pgrac) 0.1.0-stage0.25
```

If both commands report a version line, the install is ready.
Continue with the [Bootstrap](bootstrap.md) guide to start a node.

## Run the test suites (optional)

```bash
# PG core regression (must remain 219/219 on a clean tree):
make check

# pgrac cluster-specific tests (unit + tap + regress):
make -C src/test cluster-check

# pgrac client tools:
make -C src/bin/pgrac check
```

## Disable-cluster build (vanilla-equivalent)

```bash
./configure \
    --prefix=$HOME/linkdb-vanilla-install \
    --disable-cluster \
    --enable-debug \
    --with-openssl --with-icu --with-lz4 --with-zstd

make -j$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu)
make install
```

The `--disable-cluster` build does not include the cluster subsystem;
it is binary-compatible with upstream PostgreSQL 16.13 behavior and
passes the standard 219-test regression suite unchanged.

## Reporting issues

File issues at <https://github.com/sqlrush/linkdb/issues>.
