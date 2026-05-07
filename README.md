# linkdb

linkdb is a PostgreSQL 16.13 fork that adds cluster-aware features:
shared-disk multi-node deployment, a cluster topology layer, an
inter-node IPC abstraction, and the SQL surface (system views,
GUCs, wait events, error codes) needed to operate them.  It targets
the same problem space as Oracle Real Application Clusters.

The disable-cluster build is binary-equivalent to upstream
PostgreSQL 16.13 and passes the standard 219-test regression suite
unchanged.

## Status

**Stage 2 Phase 2.A in progress.**  Stage 0 (cluster subsystem scaffolding --
GUCs / shmem / IPC abstraction / topology / system views / wait events /
pgrac-init bootstrap tools) is complete.  Stage 1 has shipped block
format extensions (PageHeader pd_block_scn / ITL slot array / BufferDesc
cluster fields), commit local_scn maintenance, walwriter BOC, WAL
record xl_scn, WAL Page Header xlp_thread_id, and the dedicated undo
tablespace (`pg_undo` OID 9100) atomic batch.  Stage 2 has shipped
multi-node `pgrac.conf` activation and the Tier 1 (TCP) inter-node
interconnect carrying the LMON heartbeat: every pair of declared peers
establishes a full-duplex socket, exchanges a HELLO handshake, and
trades 1 Hz heartbeats; runtime per-peer state is exposed via
`pg_cluster_ic_peers`.  Higher-level cross-node functionality (GES /
PCM / Cache Fusion / Reconfiguration / Recovery) is scaffolded but not
yet active; operations that would require it return
`ERRCODE_FEATURE_NOT_SUPPORTED`.

Stage 1 OLTP performance regression vs vanilla PG 16.13 is verified by
`scripts/perf/run-stage1-oltp-baseline.sh` (manual; ~4.5 hour `--full`
run; pgbench TPC-B 27 combos across 3 scales × 3 modes × 3 client
levels).  See pgrac private docs for measured baselines.

## Documentation

User-facing manual:

| Topic | File |
|---|---|
| Installation | [docs/user-guide/install.md](docs/user-guide/install.md) |
| Bootstrap a node | [docs/user-guide/bootstrap.md](docs/user-guide/bootstrap.md) |
| Configuration (`cluster.*` GUCs + `pgrac.conf`) | [docs/user-guide/configuration.md](docs/user-guide/configuration.md) |
| System views reference | [docs/reference/system-views.md](docs/reference/system-views.md) |
| Wait events reference | [docs/reference/wait-events.md](docs/reference/wait-events.md) |
| Architecture overview | [docs/architecture/overview.md](docs/architecture/overview.md) |

PostgreSQL upstream documentation lives under `doc/` and is
shipped unchanged from the upstream tree.

## Quick start

```bash
git clone https://github.com/sqlrush/linkdb.git
cd linkdb

./configure --prefix=$HOME/linkdb-install \
            --enable-cluster --enable-tap-tests \
            --with-openssl --with-icu --with-lz4 --with-zstd
make -j$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu)
make install

export PATH=$HOME/linkdb-install/bin:$PATH

pgrac-init -D /tmp/linkdb-demo --node-id=0 --cluster-name=demo
echo "port = 65433"                                  >> /tmp/linkdb-demo/postgresql.conf
echo "unix_socket_directories = '/tmp'"              >> /tmp/linkdb-demo/postgresql.conf
echo "listen_addresses = ''"                         >> /tmp/linkdb-demo/postgresql.conf

pgrac-start -D /tmp/linkdb-demo -l /tmp/linkdb-demo.log -w
psql -h /tmp -p 65433 -d postgres -c 'SELECT * FROM pg_cluster_nodes;'
```

See [docs/user-guide/bootstrap.md](docs/user-guide/bootstrap.md)
for details.

## Building from source

The build follows the standard PostgreSQL `configure` + `make` +
`make install` flow.  Two extra flags are linkdb-specific:

- `--enable-cluster` activates the cluster subsystem.
- `--enable-tap-tests` enables the TAP test suites (Perl).

See [docs/user-guide/install.md](docs/user-guide/install.md) for
the complete dependency list and step-by-step instructions on
macOS and Linux.

## License

PostgreSQL License (BSD-style).  See `LICENSE` and `COPYRIGHT`.

## Reporting issues

File issues at <https://github.com/sqlrush/linkdb/issues>.

## Upstream

Forked from PostgreSQL 16.13 (<https://www.postgresql.org>).
