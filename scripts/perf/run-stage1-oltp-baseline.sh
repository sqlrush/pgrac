#!/bin/bash
#-------------------------------------------------------------------------
#
# run-stage1-oltp-baseline.sh -- spec-1.23 Stage 1 OLTP pgbench TPC-B baseline
#
# Drives pgbench TPC-B against 3 modes (vanilla PG 16.13 / pgrac
# --enable-cluster GUC=on / pgrac --enable-cluster GUC=off) across 3
# client levels (1/8/16) and either 1 scale (--quick = scale 50 only,
# 9 combos) or 3 scales (--full = [10, 50, 100], 27 combos).
#
# Each combo gets a *fresh* DB initdb + pgbench -i (no shared state
# between client levels: pgbench_history accumulates across runs and
# would pollute later combos).  Each combo runs 600s with -P 30
# interval reporting; post-processing discards the first 120s as
# warmup and computes TPS / latency from the remaining 480s.
#
# *** FAIL-FAST ***  set -euo pipefail; pgbench failures, missing
# progress samples, or zero-tps results abort the run early.  This
# prevents the user from coming back to a 4.5-hour run only to find
# all 0.00 fake data.
#
# *** BUILD PROVENANCE ***  At startup the script logs `pg_config
# --version` + `pg_config --configure` for both bindirs into
# summary.csv header.  Per Stage 0.1 docs/perf-baseline.md cassert
# adds 40-42% overhead; baseline runs MUST use no-cassert builds for
# both vanilla and pgrac.  Script warns (does not fail) if cassert
# detected; user must explicitly accept.
#
# Manual / local invocation only.  CI runs a warn-only short version
# via scripts/ci/run-perf-baseline-check.sh (D6).  Per spec-1.23 Q8
# B-WARN, full PR-level blocking gate is deferred until self-hosted
# runner is available.
#
# Output:
#   scripts/perf/results/stage1-oltp-<YYYYMMDD-HHMM>/
#     <mode>-s<scale>-c<clients>.log     (raw pgbench output)
#     summary.csv                         (TPS / provenance header / rows)
#     regression.txt                      (vanilla vs pgrac diff %; markers)
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-1.23-stage1-oltp-baseline.md §D1 (+ pre-ship fix round)
#
# IDENTIFICATION
#    scripts/perf/run-stage1-oltp-baseline.sh
#
#-------------------------------------------------------------------------
set -euo pipefail

PROGNAME="run-stage1-oltp-baseline.sh"

# ---- defaults --------------------------------------------------------
MODE_FLAG="--quick"
DURATION="${DURATION:-600}"
WARMUP="${WARMUP:-120}"
REPORT_INTERVAL=30
PORT_VANILLA="${PORT_VANILLA:-55432}"
PORT_PGRAC="${PORT_PGRAC:-55433}"
SHARED_BUFFERS="${SHARED_BUFFERS:-4GB}"

VANILLA_BINDIR="${VANILLA_BINDIR:-}"
PGRAC_BINDIR="${PGRAC_BINDIR:-}"


usage() {
    cat <<EOF
Usage: $PROGNAME [--quick | --full]

Modes:
  --quick (default)  9 combos: scale 50 × 3 modes × 3 clients (~1.5 hour)
  --full             27 combos: [10,50,100] × 3 modes × 3 clients (~4.5 hour)

Required env (no defaults; both must be set explicitly):
  VANILLA_BINDIR    absolute path to vanilla PG 16.13 install bin/ dir
  PGRAC_BINDIR      absolute path to pgrac --enable-cluster install bin/ dir

Optional env:
  DURATION          per-combo wallclock seconds (default: 600)
  WARMUP            warmup seconds discarded from analysis (default: 120)
  PORT_VANILLA      vanilla PG postmaster port (default: 55432)
  PORT_PGRAC        pgrac postmaster port (default: 55433)
  SHARED_BUFFERS    shared_buffers GUC (default: 4GB)

Output: scripts/perf/results/stage1-oltp-<YYYYMMDD-HHMM>/

For dry run:  DURATION=30 WARMUP=0 $PROGNAME --quick
EOF
}


# ---- parse CLI -------------------------------------------------------
case "${1:-}" in
    --quick) MODE_FLAG="--quick"; SCALES=(50) ;;
    --full)  MODE_FLAG="--full";  SCALES=(10 50 100) ;;
    -h|--help) usage; exit 0 ;;
    "")      SCALES=(50) ;;
    *)       echo "$PROGNAME: unknown flag: $1" >&2; usage; exit 2 ;;
esac

if [[ -z "$VANILLA_BINDIR" ]]; then
    echo "$PROGNAME: VANILLA_BINDIR env var required" >&2
    usage
    exit 2
fi

# Hardening v1.0.2 D-I7 (codex review P3 post-Sprint B): symmetric
# enforcement.  Pre-v1.0.2 only VANILLA_BINDIR was checked; PGRAC_BINDIR
# fell through to a stale personal-path default (/Users/yingjiewang/...)
# which broke on every fresh install.  v1.0.1 cleared the default but
# left the asymmetric check.
if [[ -z "$PGRAC_BINDIR" ]]; then
    echo "$PROGNAME: PGRAC_BINDIR env var required" >&2
    usage
    exit 2
fi

# ---- bindir validation + provenance ---------------------------------
validate_bindir() {
    local label="$1"
    local bindir="$2"
    local pg_config="$bindir/pg_config"

    if [[ ! -x "$pg_config" ]]; then
        echo "$PROGNAME: $label: $pg_config not executable" >&2
        exit 2
    fi
    if [[ ! -x "$bindir/initdb" || ! -x "$bindir/postgres" || ! -x "$bindir/pgbench" ]]; then
        echo "$PROGNAME: $label: missing initdb / postgres / pgbench in $bindir" >&2
        exit 2
    fi
}

validate_bindir vanilla "$VANILLA_BINDIR"
validate_bindir pgrac "$PGRAC_BINDIR"

VANILLA_VERSION=$("$VANILLA_BINDIR/pg_config" --version)
VANILLA_CONFIGURE=$("$VANILLA_BINDIR/pg_config" --configure)
PGRAC_VERSION=$("$PGRAC_BINDIR/pg_config" --version)
PGRAC_CONFIGURE=$("$PGRAC_BINDIR/pg_config" --configure)

# Warn (not fail) if cassert detected -- Stage 0.1 baseline noted
# 40-42% overhead from cassert.
warn_cassert=""
if [[ "$VANILLA_CONFIGURE" == *--enable-cassert* ]]; then
    warn_cassert+=$'\n  * VANILLA build has --enable-cassert (Stage 0.1 baseline:'
    warn_cassert+=$' cassert adds ~40% overhead; baseline data will be cassert-dominated)'
fi
if [[ "$PGRAC_CONFIGURE" == *--enable-cassert* ]]; then
    warn_cassert+=$'\n  * PGRAC build has --enable-cassert (same 40% overhead concern)'
fi
if [[ -n "$warn_cassert" ]]; then
    cat >&2 <<EOF
$PROGNAME: WARNING: build provenance check
$warn_cassert

  Both bindirs SHOULD use no-cassert no-debug builds for trustworthy
  baseline data.  See pgrac:docs/perf-baseline.md §1 for the Stage 0.1
  cassert overhead measurement (40-42%).

  Set CONTINUE_WITH_CASSERT=1 to bypass this warning.
EOF
    if [[ "${CONTINUE_WITH_CASSERT:-0}" != "1" ]]; then
        echo "$PROGNAME: aborting (set CONTINUE_WITH_CASSERT=1 to override)" >&2
        exit 2
    fi
fi


# ---- output dir + summary header ------------------------------------
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TS="$(date +%Y%m%d-%H%M)"
OUTDIR="$SCRIPT_DIR/results/stage1-oltp-$TS"
mkdir -p "$OUTDIR"

CLIENTS=(1 8 16)
MODES=(vanilla pgrac-cluster-on pgrac-cluster-off)

# Provenance log -- written before any combos run
PROVENANCE="$OUTDIR/provenance.txt"
{
    echo "# spec-1.23 Stage 1 OLTP baseline provenance"
    echo "# generated: $(date)"
    echo "# spec-1.23 v0.2 + pre-ship fix round (D1 fail-fast + per-combo fresh init + provenance log)"
    echo
    echo "## host"
    uname -a
    echo
    echo "## vanilla bindir: $VANILLA_BINDIR"
    echo "version: $VANILLA_VERSION"
    echo "configure: $VANILLA_CONFIGURE"
    echo
    echo "## pgrac bindir: $PGRAC_BINDIR"
    echo "version: $PGRAC_VERSION"
    echo "configure: $PGRAC_CONFIGURE"
    echo
    echo "## run config"
    echo "mode_flag: $MODE_FLAG"
    echo "scales: ${SCALES[*]}"
    echo "clients: ${CLIENTS[*]}"
    echo "modes: ${MODES[*]}"
    echo "duration: ${DURATION}s"
    echo "warmup: ${WARMUP}s"
    echo "report_interval: ${REPORT_INTERVAL}s"
    echo "shared_buffers: $SHARED_BUFFERS"
    echo "port_vanilla: $PORT_VANILLA"
    echo "port_pgrac: $PORT_PGRAC"
} > "$PROVENANCE"

echo "# spec-1.23 Stage 1 OLTP baseline ($MODE_FLAG)"
echo "#   provenance: $PROVENANCE"
echo "#   scales: ${SCALES[*]}"
echo "#   clients: ${CLIENTS[*]}"
echo "#   modes: ${MODES[*]}"
echo "#   duration: ${DURATION}s (discarding first ${WARMUP}s warmup)"
echo "#   output: $OUTDIR"
echo
echo "# vanilla: $VANILLA_VERSION"
echo "# pgrac:   $PGRAC_VERSION"
echo


# ---- helpers ---------------------------------------------------------

# Start a fresh postmaster + pgbench-init for this (mode, scale).
# Echoes "BINDIR|PGDATA|PGPORT" for caller.  Per-combo fresh init
# (NOT shared across client levels) prevents pgbench_history pollution.
prepare_combo() {
    local mode="$1"
    local scale="$2"
    local clients="$3"
    local bindir pgdata pgport conf_extra

    case "$mode" in
        vanilla)
            bindir="$VANILLA_BINDIR"
            pgport="$PORT_VANILLA"
            conf_extra=""
            ;;
        pgrac-cluster-on)
            bindir="$PGRAC_BINDIR"
            pgport="$PORT_PGRAC"
            conf_extra=$'cluster.enabled = on\ncluster.node_id = 0\n'
            ;;
        pgrac-cluster-off)
            bindir="$PGRAC_BINDIR"
            pgport="$PORT_PGRAC"
            conf_extra=$'cluster.enabled = off\n'
            ;;
        *) echo "unknown mode: $mode" >&2; exit 1 ;;
    esac

    pgdata="$OUTDIR/datadir-$mode-s$scale-c$clients"
    rm -rf "$pgdata"
    "$bindir/initdb" -D "$pgdata" --no-clean --no-locale --auth=trust >/dev/null 2>&1

    cat >> "$pgdata/postgresql.conf" <<CONF
shared_buffers = $SHARED_BUFFERS
port = $pgport
checkpoint_timeout = 30min
max_wal_size = 8GB
$conf_extra
CONF

    "$bindir/pg_ctl" -D "$pgdata" -l "$pgdata/postmaster.log" -w start >/dev/null

    "$bindir/pgbench" -i -s "$scale" -h /tmp -p "$pgport" -U "$USER" postgres \
        >/dev/null 2>&1

    echo "$bindir|$pgdata|$pgport"
}


# Run one pgbench combo; returns post-warmup mean TPS via stdout.
# *** FAIL-FAST ***: pgbench failure (non-zero exit) aborts (set -e).
# awk also asserts at least 1 progress sample beyond warmup.
run_one_combo() {
    local logfile="$1"
    local bindir="$2"
    local pgport="$3"
    local clients="$4"

    "$bindir/pgbench" \
        -h /tmp -p "$pgport" -U "$USER" \
        -c "$clients" -j "$clients" \
        -T "$DURATION" -P "$REPORT_INTERVAL" \
        --no-vacuum \
        postgres > "$logfile" 2>&1
    # NB: no '|| true' -- pgbench non-zero exit aborts via set -e

    awk -v warmup="$WARMUP" -v lf="$logfile" '
        /^progress:/ {
            t = $2 + 0
            if (t > warmup) {
                tps_sum += $4 + 0
                n++
            }
        }
        END {
            if (n == 0) {
                print "ERROR: no pgbench progress samples beyond warmup in " lf > "/dev/stderr"
                exit 1
            }
            tps = tps_sum / n
            if (tps <= 0) {
                print "ERROR: post-warmup mean tps <= 0 (" tps ") in " lf > "/dev/stderr"
                exit 1
            }
            printf("%.2f\n", tps)
        }' "$logfile"
}


stop_instance() {
    local bindir="$1"
    local pgdata="$2"
    "$bindir/pg_ctl" -D "$pgdata" -m fast stop >/dev/null 2>&1 || true
}


# ---- main loop -------------------------------------------------------
SUMMARY_CSV="$OUTDIR/summary.csv"
{
    echo "# spec-1.23 Stage 1 OLTP baseline summary"
    echo "# generated: $(date)"
    echo "# vanilla: $VANILLA_VERSION (no-cassert: $([[ "$VANILLA_CONFIGURE" == *--enable-cassert* ]] && echo NO || echo YES))"
    echo "# pgrac:   $PGRAC_VERSION (no-cassert: $([[ "$PGRAC_CONFIGURE" == *--enable-cassert* ]] && echo NO || echo YES))"
    echo "# mode_flag=$MODE_FLAG duration=${DURATION}s warmup=${WARMUP}s shared_buffers=$SHARED_BUFFERS"
    echo "mode,scale,clients,tps_post_warmup,duration_s,warmup_s"
} > "$SUMMARY_CSV"

# Per-combo fresh init: outermost loop is scale (one combo per row).
for scale in "${SCALES[@]}"; do
    for mode in "${MODES[@]}"; do
        for clients in "${CLIENTS[@]}"; do
            echo "[$(date +%H:%M:%S)] running $mode scale=$scale clients=$clients (${DURATION}s, fresh DB) ..."
            info=$(prepare_combo "$mode" "$scale" "$clients")
            bindir=$(echo "$info" | cut -d'|' -f1)
            pgdata=$(echo "$info" | cut -d'|' -f2)
            pgport=$(echo "$info" | cut -d'|' -f3)

            logfile="$OUTDIR/$mode-s$scale-c$clients.log"
            tps=$(run_one_combo "$logfile" "$bindir" "$pgport" "$clients")
            echo "$mode,$scale,$clients,$tps,$DURATION,$WARMUP" >> "$SUMMARY_CSV"
            echo "  -> tps (post-warmup mean) = $tps"

            stop_instance "$bindir" "$pgdata"
        done
    done
done


# ---- regression analysis ---------------------------------------------
REGRESSION_TXT="$OUTDIR/regression.txt"
{
    echo "# spec-1.23 Stage 1 OLTP regression analysis"
    echo "# generated: $(date)"
    echo "# baseline: $VANILLA_VERSION"
    echo "# threshold semantics (per spec-1.23 Q3 REVISED):"
    echo "#   > 1%   investigation trigger (non-fail)"
    echo "#   > 5%   triggers RCA + disposition recorded (per CLAUDE.md rule 7)"
    echo "#"
    echo "# scale,clients,vanilla_tps,pgrac_on_tps,pgrac_off_tps,delta_on%,delta_off%"
    for scale in "${SCALES[@]}"; do
        for clients in "${CLIENTS[@]}"; do
            v=$(awk -F, -v s="$scale" -v c="$clients" '$1=="vanilla" && $2==s && $3==c {print $4}' "$SUMMARY_CSV")
            on=$(awk -F, -v s="$scale" -v c="$clients" '$1=="pgrac-cluster-on" && $2==s && $3==c {print $4}' "$SUMMARY_CSV")
            off=$(awk -F, -v s="$scale" -v c="$clients" '$1=="pgrac-cluster-off" && $2==s && $3==c {print $4}' "$SUMMARY_CSV")
            d_on=$(awk -v v="$v" -v p="$on" 'BEGIN{ if (v>0) printf("%.2f", (p-v)/v*100); else print "NaN"}')
            d_off=$(awk -v v="$v" -v p="$off" 'BEGIN{ if (v>0) printf("%.2f", (p-v)/v*100); else print "NaN"}')
            line="$scale,$clients,$v,$on,$off,$d_on,$d_off"
            tag=""
            if awk -v x="$d_on" 'BEGIN{ if (x+0 < -5 || x+0 > 5) exit 0; exit 1 }'; then
                tag=" *** >5% RCA TRIGGER (cluster-on) ***"
            elif awk -v x="$d_on" 'BEGIN{ if (x+0 < -1 || x+0 > 1) exit 0; exit 1 }'; then
                tag=" * >1% investigation (cluster-on)"
            fi
            echo "$line$tag"
        done
    done
} > "$REGRESSION_TXT"

echo
echo "[$(date +%H:%M:%S)] DONE"
echo "  provenance: $PROVENANCE"
echo "  summary:    $SUMMARY_CSV"
echo "  regression: $REGRESSION_TXT"
echo
echo "Per spec-1.23 Q3 REVISED: > 5% deltas require RCA + disposition"
echo "recorded in pgrac:docs/perf-baseline.md §9.5 before shipping."
