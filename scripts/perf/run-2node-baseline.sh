#!/bin/bash
#-------------------------------------------------------------------------
#
# run-2node-baseline.sh -- spec-3.4e Stage 3 multi-node perf baseline
#                          shell wrapper(CLI + arg validation)
#
# Thin CLI wrapper.  ClusterPair lifecycle is owned by the Perl runner
# `run_2node_baseline.pl` (spec-3.4e D2 / F4 — bash cannot directly use
# PostgreSQL::Test::ClusterPair.pm).
#
# Modes(spec-3.4e v0.2 §2.1 workload classes):
#   single-node-no-peer   class 1 (★ ship-blocking GREEN ≤ 15%)
#                         → delegates to existing run-baseline.sh
#                         (no ClusterPair needed)
#   2node-local-affinity  class 2 (warn-only) — ClusterPair + own key range
#   2node-cross-node-visibility  class 3 (PARTIAL coverage) —
#                         D5b COMMITTED inject + force cluster path
#   2node-hot-row-lock    class 4 (PARTIAL coverage) — D5b ACTIVE inject
#                         + SELECT FOR UPDATE on same row
#   all                   run all 4 classes
#
# Output:
#   scripts/perf/results/3-4e-<class>-<TS>.txt   per-run TPS + p95 + counter delta
#   scripts/perf/results/3-4e-<class>-<TS>.json  parsed summary
#   scripts/perf/results/3-4e-<class>-<TS>.log   raw pgbench --log transaction logs
#
# Manual / perf workflow invocation only.  Not wired into fast-gate CI
# (L91 perf-tier separation;weekly schedule + workflow_dispatch only).
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-3.4e-stage3-multinode-perf-hardening.md (v0.2 FROZEN 2026-05-25)
#
# IDENTIFICATION
#    scripts/perf/run-2node-baseline.sh
#
#-------------------------------------------------------------------------
set -euo pipefail

PROGNAME="run-2node-baseline.sh"

# ---- defaults ---------------------------------------------------------
MODE="single-node-no-peer"
SCALE=10
DURATION=30
CLIENTS=4
JOBS=2
ENABLE_INSTALL="${PGRAC_ENABLE_INSTALL:-$HOME/linkdb-install}"
DISABLE_INSTALL="${PGRAC_DISABLE_INSTALL:-$HOME/linkdb-disable-install}"
KEEP_PGDATA="no"
APPEND_MD=""

REPO_ROOT="$(cd "$(dirname "$(realpath "$0")")/../.." && pwd)"
RESULTS_DIR="$REPO_ROOT/scripts/perf/results"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
PERL_RUNNER="$REPO_ROOT/scripts/perf/run_2node_baseline.pl"

die() { echo "$PROGNAME: $*" 1>&2; exit 1; }

usage() {
    cat <<EOF
$PROGNAME spec-3.4e Stage 3 multi-node perf baseline shell wrapper.

Usage:
  $PROGNAME --mode=<class> [OPTION]...

Modes:
  single-node-no-peer           class 1 (★ ship-blocking GREEN ≤ 15%)
  2node-local-affinity          class 2 (warn-only)
  2node-cross-node-visibility   class 3 (PARTIAL — inject-based)
  2node-hot-row-lock            class 4 (PARTIAL — inject-based)
  2node-subxact-nesting         class 5 (spec-3.5 PARTIAL — savepoint depth=5)
  all                           run all 4 classes

Options:
  --scale=N             pgbench -s scale (default: $SCALE)
  --duration=SECS       pgbench -T duration (default: $DURATION)
  --clients=N           pgbench -c (default: $CLIENTS)
  --jobs=N              pgbench -j (default: $JOBS)
  --enable-install=DIR  --enable-cluster install prefix
                        (default: $ENABLE_INSTALL,
                         override via PGRAC_ENABLE_INSTALL)
  --disable-install=DIR --disable-cluster install prefix (class 1 only)
                        (default: $DISABLE_INSTALL,
                         override via PGRAC_DISABLE_INSTALL)
  --keep-pgdata         keep PGDATA after run (default: clean up)
  --append-md=PATH      append result rows to docs/perf-baseline.md §8
                        (PATH must exist;reserved for Hardening v1.0.X)
  -h, --help            show this help

Architecture:
  This shell wrapper validates args + delegates to Perl runner for class
  2/3/4 (which owns PostgreSQL::Test::ClusterPair lifecycle).
  Class 1 reuses existing run-baseline.sh single-node infrastructure.

Spec-3.4e v0.2 §2.4 + F4:bash 不能直接调 ClusterPair.pm — Perl runner
owns 2-postmaster lifecycle + pgbench log parsing + metric collection。

For real CI invocation, prefer:
  gh workflow run perf.yml -R sqlrush/pgrac-PostgreSQL-run-as-OracleRAC
EOF
}

# ---- arg parse --------------------------------------------------------
for arg in "$@"; do
    case "$arg" in
        --mode=*)             MODE="${arg#--mode=}" ;;
        --scale=*)            SCALE="${arg#--scale=}" ;;
        --duration=*)         DURATION="${arg#--duration=}" ;;
        --clients=*)          CLIENTS="${arg#--clients=}" ;;
        --jobs=*)              JOBS="${arg#--jobs=}" ;;
        --enable-install=*)   ENABLE_INSTALL="${arg#--enable-install=}" ;;
        --disable-install=*)  DISABLE_INSTALL="${arg#--disable-install=}" ;;
        --keep-pgdata)        KEEP_PGDATA="yes" ;;
        --append-md=*)        APPEND_MD="${arg#--append-md=}" ;;
        -h|--help)            usage; exit 0 ;;
        *) die "unknown option: $arg (try --help)" ;;
    esac
done

case "$MODE" in
    single-node-no-peer|2node-local-affinity|2node-cross-node-visibility|2node-hot-row-lock|2node-subxact-nesting|2node-multixact-shared-lock|all) ;;
    *) die "--mode must be one of:single-node-no-peer | 2node-local-affinity | 2node-cross-node-visibility | 2node-hot-row-lock | 2node-subxact-nesting | 2node-multixact-shared-lock | all" ;;
esac

mkdir -p "$RESULTS_DIR"

# ---- class 1 delegation -----------------------------------------------
run_single_node_no_peer() {
    [ -x "$ENABLE_INSTALL/bin/postgres" ] \
        || die "enable postgres not found at $ENABLE_INSTALL/bin/postgres (set PGRAC_ENABLE_INSTALL or --enable-install=DIR)"
    [ -x "$DISABLE_INSTALL/bin/postgres" ] \
        || die "disable postgres not found at $DISABLE_INSTALL/bin/postgres (set PGRAC_DISABLE_INSTALL or --disable-install=DIR)"

    echo "$PROGNAME: class 1 single-node-no-peer — delegating to run-baseline.sh"
    PGRAC_ENABLE_INSTALL="$ENABLE_INSTALL" \
    PGRAC_DISABLE_INSTALL="$DISABLE_INSTALL" \
    "$REPO_ROOT/scripts/perf/run-baseline.sh" \
        --mode=both \
        --scale="$SCALE" \
        --duration="$DURATION" \
        --clients="$CLIENTS" \
        --jobs="$JOBS" \
        ${KEEP_PGDATA:+--keep-pgdata}
}

# ---- class 2/3/4 delegation to Perl runner ----------------------------
run_2node_class() {
    local class="$1"

    [ -x "$ENABLE_INSTALL/bin/postgres" ] \
        || die "enable postgres not found at $ENABLE_INSTALL/bin/postgres"
    [ -f "$PERL_RUNNER" ] \
        || die "Perl runner not found at $PERL_RUNNER"

    echo "$PROGNAME: class $class — invoking Perl runner"

    PGRAC_ENABLE_INSTALL="$ENABLE_INSTALL" \
    perl "$PERL_RUNNER" \
        --mode="$class" \
        --scale="$SCALE" \
        --duration="$DURATION" \
        --clients="$CLIENTS" \
        --jobs="$JOBS" \
        --enable-install="$ENABLE_INSTALL" \
        --results-dir="$RESULTS_DIR" \
        --timestamp="$TS"
}

# ---- main dispatch ----------------------------------------------------
echo "$PROGNAME: spec-3.4e mode=$MODE scale=$SCALE duration=${DURATION}s clients=$CLIENTS jobs=$JOBS"

case "$MODE" in
    single-node-no-peer)
        run_single_node_no_peer
        ;;
    2node-local-affinity|2node-cross-node-visibility|2node-hot-row-lock|2node-subxact-nesting|2node-multixact-shared-lock)
        run_2node_class "$MODE"
        ;;
    all)
        run_single_node_no_peer
        run_2node_class "2node-local-affinity"
        run_2node_class "2node-cross-node-visibility"
        run_2node_class "2node-hot-row-lock"
        run_2node_class "2node-subxact-nesting"
        run_2node_class "2node-multixact-shared-lock"
        ;;
esac

if [ -n "$APPEND_MD" ]; then
    [ -f "$APPEND_MD" ] || die "$APPEND_MD does not exist"
    echo "$PROGNAME: results in $RESULTS_DIR;manual append into $APPEND_MD §8.4 table"
    echo "  (auto-append deferred to spec-3.4e Hardening v1.0.X)"
fi

echo "$PROGNAME: done"
