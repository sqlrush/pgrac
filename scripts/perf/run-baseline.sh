#!/bin/bash
#-------------------------------------------------------------------------
#
# run-baseline.sh -- pgbench-driven performance baseline for linkdb
#
# Drives pgbench against a freshly bootstrapped --enable-cluster
# instance and (optionally) a --disable-cluster instance, captures
# TPS and average-latency numbers, writes one results txt per run
# under scripts/perf/results/, and (opt-in via --append-md) appends
# a row to a markdown ledger.
#
# Manual / local invocation only.  Not wired into CI: shared runners
# exhibit 10-20% variance that would cause noisy regression alerts;
# perf gating is deferred until a self-hosted runner becomes available.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-0.23-perf-baseline.md
#
# IDENTIFICATION
#    scripts/perf/run-baseline.sh
#
#-------------------------------------------------------------------------
set -euo pipefail

PROGNAME="run-baseline.sh"

# ---- defaults ---------------------------------------------------------
MODE="both"
SCALE=10
DURATION=30
CLIENTS=4
JOBS=2
VANILLA="no"
PGDATA_BASE=""
KEEP_PGDATA="no"
APPEND_MD=""    # empty by default; set via --append-md=PATH opt-in.
ENABLE_INSTALL="${PGRAC_ENABLE_INSTALL:-$HOME/linkdb-install}"
DISABLE_INSTALL="${PGRAC_DISABLE_INSTALL:-$HOME/linkdb-disable-install}"

REPO_ROOT="$(cd "$(dirname "$(realpath "$0")")/../.." && pwd)"
RESULTS_DIR="$REPO_ROOT/scripts/perf/results"
TS="$(date -u +%Y%m%dT%H%M%SZ)"

# ---- helpers ----------------------------------------------------------

die() {
    echo "$PROGNAME: $*" 1>&2
    exit 1
}

usage() {
    cat <<EOF
$PROGNAME runs pgbench against linkdb and records the baseline.

Usage:
  $PROGNAME [OPTION]...

Options:
  --mode=MODE           enable / disable / both (default: both)
  --scale=N             pgbench -s scale factor (default: $SCALE)
  --duration=SECS       pgbench -T duration (default: $DURATION)
  --clients=N           pgbench -c (default: $CLIENTS)
  --jobs=N              pgbench -j (default: $JOBS)
  --vanilla             force initdb (skip pgrac-init) in enable mode
  --pgdata-base=DIR     PGDATA root (default: /tmp/pgrac-perf-<mode>)
  --keep-pgdata         keep PGDATA after the run (default: clean up)
  --append-md=PATH      append one row per mode to PATH (markdown
                        table; PATH must already exist).  Default
                        empty: no markdown is written.
  --enable-install=DIR  --enable-cluster install prefix
                        (default: $ENABLE_INSTALL,
                         override via PGRAC_ENABLE_INSTALL env)
  --disable-install=DIR --disable-cluster install prefix
                        (default: $DISABLE_INSTALL,
                         override via PGRAC_DISABLE_INSTALL env)
  -h, --help            show this help, then exit

Outputs:
  stdout: human-readable summary line per mode
  $RESULTS_DIR/<ts>-<mode>-<rw|ro>.txt  (full pgbench output)
  PATH passed via --append-md            (one row per mode, opt-in)
EOF
}

# ---- argument parsing -------------------------------------------------

while [ $# -gt 0 ]; do
    case "$1" in
        --mode=*)            MODE="${1#*=}" ;;
        --scale=*)           SCALE="${1#*=}" ;;
        --duration=*)        DURATION="${1#*=}" ;;
        --clients=*)         CLIENTS="${1#*=}" ;;
        --jobs=*)             JOBS="${1#*=}" ;;
        --vanilla)           VANILLA="yes" ;;
        --pgdata-base=*)     PGDATA_BASE="${1#*=}" ;;
        --keep-pgdata)       KEEP_PGDATA="yes" ;;
        --append-md=*)       APPEND_MD="${1#*=}" ;;
        --enable-install=*)  ENABLE_INSTALL="${1#*=}" ;;
        --disable-install=*) DISABLE_INSTALL="${1#*=}" ;;
        -h|--help)           usage; exit 0 ;;
        -*)
            echo "$PROGNAME: invalid option \"$1\"" 1>&2
            echo "Try \"$PROGNAME --help\" for more information." 1>&2
            exit 1
            ;;
        *)
            die "unexpected positional argument \"$1\""
            ;;
    esac
    shift
done

case "$MODE" in
    enable|disable|both) ;;
    *) die "--mode must be one of: enable, disable, both (got \"$MODE\")" ;;
esac

mkdir -p "$RESULTS_DIR"

# ---- per-mode runner --------------------------------------------------

# Globals filled by run_one_mode for the trailing markdown append.
declare -a SUMMARY_ROWS=()
ENABLE_TPS_RW=""
ENABLE_TPS_RO=""
DISABLE_TPS_RW=""
DISABLE_TPS_RO=""

run_one_mode() {
    local mode="$1"
    local install_prefix
    local pgdata
    local port
    local logfile
    local rw_out
    local ro_out
    local tps_rw tps_ro lat_rw lat_ro

    if [ "$mode" = "enable" ]; then
        install_prefix="$ENABLE_INSTALL"
        port=65500
    else
        install_prefix="$DISABLE_INSTALL"
        port=65501
    fi

    [ -d "$install_prefix/bin" ] \
        || die "$mode-cluster install not found at $install_prefix/bin (run \"make install\" first)"

    [ -x "$install_prefix/bin/pgbench" ] \
        || die "$install_prefix/bin/pgbench is missing"

    pgdata="${PGDATA_BASE:-/tmp/pgrac-perf-$mode}"
    logfile="$pgdata.log"
    rw_out="$RESULTS_DIR/$TS-$mode-rw.txt"
    ro_out="$RESULTS_DIR/$TS-$mode-ro.txt"

    rm -rf "$pgdata" "$logfile"

    echo "=== mode=$mode  prefix=$install_prefix  port=$port  scale=$SCALE  duration=${DURATION}s ==="

    PATH="$install_prefix/bin:$PATH"

    if [ "$mode" = "enable" ] && [ "$VANILLA" != "yes" ]; then
        echo "$PROGNAME: bootstrapping via pgrac-init"
        "$install_prefix/bin/pgrac-init" -D "$pgdata" --node-id=0 \
            --cluster-name=perf-baseline --force >/dev/null
    else
        echo "$PROGNAME: bootstrapping via initdb"
        "$install_prefix/bin/initdb" -D "$pgdata" -A trust -N >/dev/null
    fi

    {
        echo ""
        echo "# perf-baseline overrides"
        echo "port = $port"
        echo "unix_socket_directories = '/tmp'"
        echo "listen_addresses = ''"
    } >>"$pgdata/postgresql.conf"

    "$install_prefix/bin/pg_ctl" -D "$pgdata" -l "$logfile" -w -t 60 start >/dev/null

    # Trap to make sure the server is stopped even if pgbench fails.
    # shellcheck disable=SC2064 # we want $pgdata expanded now
    trap "PATH=\"$install_prefix/bin:\$PATH\" \"$install_prefix/bin/pg_ctl\" -D \"$pgdata\" -m fast -w stop >/dev/null 2>&1 || true" EXIT

    # Initialise pgbench (--quiet keeps stdout clean).
    "$install_prefix/bin/pgbench" -h /tmp -p "$port" -i -s "$SCALE" --quiet postgres

    # Read-write run.
    "$install_prefix/bin/pgbench" -h /tmp -p "$port" \
        -c "$CLIENTS" -j "$JOBS" -T "$DURATION" -P 5 \
        postgres >"$rw_out"

    # Read-only run.
    "$install_prefix/bin/pgbench" -h /tmp -p "$port" -S \
        -c "$CLIENTS" -j "$JOBS" -T "$DURATION" -P 5 \
        postgres >"$ro_out"

    "$install_prefix/bin/pg_ctl" -D "$pgdata" -m fast -w stop >/dev/null
    trap - EXIT

    # Parse "tps = NNN.NNN" from the standard pgbench summary block.
    tps_rw=$(awk '/^tps = / { print $3; exit }' "$rw_out")
    tps_ro=$(awk '/^tps = / { print $3; exit }' "$ro_out")
    lat_rw=$(awk '/^latency average = / { print $4; exit }' "$rw_out")
    lat_ro=$(awk '/^latency average = / { print $4; exit }' "$ro_out")

    [ -n "$tps_rw" ] || die "could not parse tps from $rw_out"
    [ -n "$tps_ro" ] || die "could not parse tps from $ro_out"

    # Print human-readable summary line.
    printf "$PROGNAME: %-7s tps_rw=%s tps_ro=%s lat_avg_rw=%s ms lat_avg_ro=%s ms\n" \
        "$mode" "$tps_rw" "$tps_ro" "$lat_rw" "$lat_ro"

    # Stash a row for later markdown append.
    SUMMARY_ROWS+=("| $(date -u +%Y-%m-%d) | $(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown) | $(uname -srm) | $mode | $tps_rw | $tps_ro | $lat_rw | $lat_ro |")

    if [ "$mode" = "enable" ]; then
        ENABLE_TPS_RW="$tps_rw"
        ENABLE_TPS_RO="$tps_ro"
    else
        DISABLE_TPS_RW="$tps_rw"
        DISABLE_TPS_RO="$tps_ro"
    fi

    if [ "$KEEP_PGDATA" != "yes" ]; then
        rm -rf "$pgdata" "$logfile"
    fi
}

# ---- run modes --------------------------------------------------------

case "$MODE" in
    enable)  run_one_mode enable ;;
    disable) run_one_mode disable ;;
    both)
        run_one_mode enable
        run_one_mode disable
        ;;
esac

if [ "$MODE" = "both" ] \
   && [ -n "$ENABLE_TPS_RW" ] && [ -n "$ENABLE_TPS_RO" ] \
   && [ -n "$DISABLE_TPS_RW" ] && [ -n "$DISABLE_TPS_RO" ]; then
    rw_regression=$(awk -v enabled="$ENABLE_TPS_RW" -v disabled="$DISABLE_TPS_RW" \
        'BEGIN { if (disabled > 0) printf "%.1f", ((disabled - enabled) * 100.0 / disabled); else printf "nan" }')
    ro_regression=$(awk -v enabled="$ENABLE_TPS_RO" -v disabled="$DISABLE_TPS_RO" \
        'BEGIN { if (disabled > 0) printf "%.1f", ((disabled - enabled) * 100.0 / disabled); else printf "nan" }')

    printf "$PROGNAME: off/on regression rw=%s%% ro=%s%% (disable_rw=%s enable_rw=%s disable_ro=%s enable_ro=%s)\n" \
        "$rw_regression" "$ro_regression" \
        "$DISABLE_TPS_RW" "$ENABLE_TPS_RW" "$DISABLE_TPS_RO" "$ENABLE_TPS_RO"
fi

# ---- markdown append (opt-in) -----------------------------------------

if [ -n "$APPEND_MD" ]; then
    if [ -f "$APPEND_MD" ]; then
        {
            for row in "${SUMMARY_ROWS[@]}"; do
                echo "$row"
            done
        } >>"$APPEND_MD"
        echo "$PROGNAME: appended ${#SUMMARY_ROWS[@]} row(s) to $APPEND_MD"
        echo "$PROGNAME: please review the diff and commit if the numbers look sensible"
    else
        die "--append-md target $APPEND_MD does not exist"
    fi
fi

echo "$PROGNAME: done (results in $RESULTS_DIR/$TS-*.txt)"
