# Verification cheatsheet

Commands you can run against an installed linkdb (or a source build) to
sanity-check the cluster surface — version flag, catalog views,
injection points, and the test suites.

> Assumes the install bin/ is on `PATH`.  If not, prefix every command
> with the absolute path (e.g. `/Users/me/linkdb-install/bin/postgres`).

---

## 1. Binary / CLI tools (no running cluster needed)

```bash
# linkdb / pgrac version (cluster build only)
postgres --pgrac-version
# → pgrac v0.1.0-stage0.30 (based on PostgreSQL 16.13)

# vanilla PG version
postgres --version
postgres -V

# pgrac CLI tools — help / version
pgrac-init       --help
pgrac-start      --help
pgrac-acceptance --help
pgrac-init       --version
pgrac-acceptance --version
```

In a `--disable-cluster` build the `--pgrac-version` flag is not
recognised and `postgres` exits with `FATAL: --pgrac-version requires a
value`.  This is intentional: disable-mode binaries contain no cluster
symbols at all.  Use `postgres --version` instead for a vanilla PG
build.

---

## 2. Lifecycle smoke check (init → start → verify → stop)

A full local round-trip in under 10 seconds:

```bash
PGDATA=/tmp/pgrac-smoke
rm -rf "$PGDATA"

pgrac-init  -D "$PGDATA" --node-id 1 -o "-A trust"
pgrac-start -D "$PGDATA" -o "-p 65432" -l "$PGDATA/postmaster.log"

pgrac-acceptance --port 65432
# → 10/10 checks passed.  Stage 1+/2.1 acceptance: GREEN.

pg_ctl -D "$PGDATA" stop -m fast
rm -rf "$PGDATA"
```

`pgrac-acceptance` prints a coloured PASS/FAIL table covering: postgres
binary present, `--pgrac-version` works, `cluster.node_id` GUC visible,
and every catalog view returns the expected row count.  Exit code 0 =
all green, 1 = at least one check failed, 2 = couldn't connect.

---

## 3. SQL surface (against a running instance)

Connect first: `psql -p 65432 -d postgres`.

### 3.1 Catalog views

```sql
-- Wait events (local + global registries)
SELECT * FROM pg_stat_cluster_wait_events ORDER BY type, name LIMIT 5;
SELECT * FROM pg_stat_gcluster_wait_events;

-- Cluster topology
SELECT * FROM pg_cluster_nodes;

-- Per-node performance counters
SELECT * FROM pg_stat_cluster_nodes;
SELECT * FROM pg_stat_cluster_counters;

-- Error injection registry
SELECT * FROM pg_stat_cluster_injections;

-- One-stop diagnostic snapshot (~26 rows across 7 categories)
SELECT * FROM pg_cluster_state ORDER BY category, key;
```

### 3.2 Cluster GUCs

```sql
SHOW cluster.node_id;                    -- this node's id
SHOW cluster.config_file;
SHOW cluster.interconnect_tier;          -- stub / mock / tier1 / tier2 / tier3
SHOW cluster.injection_points;           -- startup-time batch arm list
```

### 3.3 Underlying SRFs (set-returning functions)

The views above are thin wrappers over these SRFs; you can call them
directly:

```sql
SELECT * FROM cluster_get_wait_events();
SELECT * FROM cluster_get_global_wait_events();
SELECT * FROM cluster_get_nodes();
SELECT * FROM cluster_get_injection_state();
SELECT * FROM cluster_get_stat_nodes();
SELECT * FROM cluster_get_pgstat_counters();
SELECT * FROM cluster_dump_state();

-- Mock-tier interconnect helpers (only useful when
-- cluster.interconnect_tier = 'mock').  Function names updated per
-- Hardening v1.0.1 codex review P2-5; pre-Hardening doc referenced
-- the older drain/clear shorthand which no longer matches the
-- current SQL surface.
SELECT * FROM cluster_ic_mock_inject(1, '\x01\x02');
SELECT * FROM cluster_ic_mock_drain_outbound();
SELECT * FROM cluster_ic_mock_clear_all();
SELECT * FROM cluster_ic_mock_recv_test();
```

### 3.4 Error injection

```sql
-- Arm an injection point with a fault type.
-- fault_type ∈ {warning, error, sleep, crash, skip, none}.
SELECT cluster_inject_fault('cluster-init-pre-shmem', 'warning', 0);

-- Disarm.
SELECT cluster_inject_fault('cluster-init-pre-shmem', 'none', 0);

-- Inspect arm state + hit counter.
SELECT name, fault_type, hits
  FROM pg_stat_cluster_injections
 WHERE name = 'cluster-init-pre-shmem';
```

The current build registers 83 injection points covering every
cluster init / shutdown / shmem / GUC / conf / pgstat / views / debug /
SCN / undo / WAL / IC / phase / lmon / lck / diag / stats /
acceptance / pgrac.conf-multi-node-activation entry.  Dump the full
list with `SELECT name FROM pg_stat_cluster_injections ORDER BY name;`.
The exact count is locked by `030_acceptance.pl` and the installed
`pgrac-acceptance` smoke check (Hardening v1.0.1 D-H8 / codex review
P2-5; sync'd from Stage 0 = 14 to current).

### 3.5 SQLSTATE error codes

The cluster code path defines extra SQLSTATEs in the `YR0xx`-`YR3xx`
range.  Trigger one to verify it's wired:

```sql
DO $$ BEGIN RAISE EXCEPTION SQLSTATE 'YR001'; END $$;
-- → ERRCODE_PGRAC_CLUSTER_NOT_READY

DO $$ BEGIN RAISE EXCEPTION SQLSTATE 'YR101'; END $$;
-- → ERRCODE_PGRAC_INTERCONNECT_TIMEOUT
```

---

## 4. Test suites (require the source tree)

These commands assume you cloned the repo and built with
`--enable-cluster --enable-tap-tests`.  Run them from the repo root.

```bash
# A. PG vanilla regression (must remain 219/219).
make check

# B. Cluster unit tests — compile-time and link-time assertions.
make -C src/test/cluster_unit check

# C. Cluster TAP — multi-process integration tests.
PERL5LIB="$HOME/perl5/lib/perl5" \
    make -C src/test/cluster_tap installcheck

# Run a single TAP file:
PERL5LIB="$HOME/perl5/lib/perl5" \
    make -C src/test/cluster_tap installcheck \
    PROVE_TESTS=t/030_acceptance.pl

# D. Cluster regression — SQL-level behaviour pin-down.
make -C src/test/cluster_regress check

# E. pgrac-init / pgrac-start TAP coverage.
PERL5LIB="$HOME/perl5/lib/perl5" \
    make -C src/bin/pgrac check

# F. Everything (A + B + C + D in one shot):
make check && make -C src/test cluster-check
```

### Lint / style gates

```bash
bash scripts/ci/check-format.sh           # clang-format diff
bash scripts/ci/check-comment-headers.sh  # PGRAC MODIFICATIONS / Author rules
```

---

## 5. Verifying the `--disable-cluster` build

When linkdb is configured with `--disable-cluster` it must compile to
a binary indistinguishable from upstream PostgreSQL: no cluster
symbols, no cluster GUCs, no `--pgrac-version` flag.

```bash
# Build a fresh disable-mode tree side-by-side.
cd /tmp && rm -rf linkdb-disable
cp -r /path/to/linkdb linkdb-disable
cd linkdb-disable && make distclean
./configure --prefix=/tmp/pgrac-disable --enable-debug \
            --without-icu --disable-cluster
make -j8 && make install

# 1. No cluster symbols leak in (output must be empty).
nm /tmp/pgrac-disable/bin/postgres \
    | grep -E '_cluster_init|_cluster_shutdown|_pgrac_version_string'

# 2. --pgrac-version is unrecognised.
/tmp/pgrac-disable/bin/postgres --pgrac-version
# → FATAL:  --pgrac-version requires a value         (exit 1)

# 3. PG vanilla regression still 219/219.
cd /tmp/linkdb-disable && make check
```

Cleanup: `rm -rf /tmp/linkdb-disable /tmp/pgrac-disable`.

---

## 6. Performance baseline (manual / local)

There are now two performance baseline runners; pick the one that
matches what you want to measure.

### 6.1 Stage 1 OLTP TPC-B baseline (`run-stage1-oltp-baseline.sh`)

This is the spec-1.23 frozen runner.  Compares vanilla PG 16.13
against pgrac (cluster-on + cluster-off) across a 27-combo matrix
(scale 10/50/100 × clients 1/8/16) using the standard pgbench TPC-B
workload.  Used to gate Stage 1 → Stage 2 entry.

```bash
# Required env vars (no defaults; codex review P1-3 fix removed the
# personal-path default that previously broke on fresh installs):
export VANILLA_BINDIR=/path/to/vanilla-pg-16.13/bin
export PGRAC_BINDIR=/tmp/pgrac-no-cassert-install/bin   # or your install

# Full baseline (27 combos × 720s ≈ 5.4 hour 挂机):
bash scripts/perf/run-stage1-oltp-baseline.sh --full

# Quick sanity (9 combos × 720s ≈ 1.8 hour):
bash scripts/perf/run-stage1-oltp-baseline.sh --quick
```

Outputs land under `scripts/perf/results/stage1-oltp-<TS>/`
(`summary.csv` raw TPS / `regression.txt` analysis with `>5% RCA`
markers / `provenance.txt` host + bin versions / 27 `*.log` per-combo
pgbench output).  The 2026-05-06 reference run + Disposition decision
is preserved at `pgrac/docs/perf-baseline.md §9.4/§9.5`.

### 6.2 Quick smoke baseline (`run-baseline.sh`)

Original Stage 0 quick smoke runner.  Builds and runs pgbench
against both an --enable-cluster install ($HOME/linkdb-install by
default) and a --disable-cluster install ($HOME/linkdb-disable-install),
then prints a summary line per mode.  Default 30-second runs with
scale=10, 4 clients, 2 jobs.  Useful for fast sanity checks but does
NOT gate Stage 1 acceptance (use 6.1 for that).

```bash
scripts/perf/run-baseline.sh

# Just one mode, smaller workload (handy for smoke tests):
scripts/perf/run-baseline.sh --mode=enable --duration=5 --scale=1

# Override install prefixes via env or flags:
PGRAC_ENABLE_INSTALL=/opt/linkdb scripts/perf/run-baseline.sh --mode=enable
scripts/perf/run-baseline.sh --enable-install=/opt/linkdb --mode=enable

# Append one markdown row per mode to a ledger you already keep:
scripts/perf/run-baseline.sh --append-md=$HOME/perf-history.md
```

Per-run pgbench output lands in `scripts/perf/results/<timestamp>-<mode>-<rw|ro>.txt`
and is gitignored.  Read-write and read-only runs are timed
separately, so a single invocation produces four output files (two
modes × two read patterns) when run with `--mode=both` (the default).
