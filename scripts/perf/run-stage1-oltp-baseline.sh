#!/bin/bash
#-------------------------------------------------------------------------
#
# run-stage1-oltp-baseline.sh -- spec-1.23 Stage 1 OLTP pgbench TPC-B baseline
#
# Drives pgbench TPC-B (NOT select-only like spec-0.23 baseline) against
# 3 modes -- vanilla PG 16.13 / pgrac --enable-cluster GUC=on / pgrac
# --enable-cluster GUC=off -- across 3 client levels (1/8/16) and either
# 1 scale (--quick mode default = scale 50 only, 9 combos) or 3 scales
# (--full = [10, 50, 100], 27 combos).  Each combo runs 600s with
# -P 30 interval reporting; post-processing discards the first 120s as
# warmup and computes TPS / latency from the remaining 480s.
#
# Manual / local invocation only.  CI runs a warn-only short version
# via scripts/ci/run-perf-baseline-check.sh (D6).  Per spec-1.23 Q8
# B-WARN, full PR-level blocking gate is deferred until self-hosted
# runner is available.
#
# Output:
#   scripts/perf/results/stage1-oltp-<YYYYMMDD-HHMM>/
#     <mode>-<scale>-<clients>.log     (raw pgbench output)
#     summary.csv                       (TPS / latency / WAL / pg_undo / RSS)
#     regression.txt                    (vanilla vs pgrac diff %; markers)
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-1.23-stage1-oltp-baseline.md §D1
#
# IDENTIFICATION
#    scripts/perf/run-stage1-oltp-baseline.sh
#
#-------------------------------------------------------------------------
set -euo pipefail

PROGNAME="run-stage1-oltp-baseline.sh"

# ---- defaults --------------------------------------------------------
MODE_FLAG="--quick"      # --quick (9 combos) or --full (27 combos)
DURATION="${DURATION:-600}"
WARMUP="${WARMUP:-120}"
REPORT_INTERVAL=30
PORT_VANILLA="${PORT_VANILLA:-55432}"
PORT_PGRAC="${PORT_PGRAC:-55433}"
SHARED_BUFFERS="${SHARED_BUFFERS:-4GB}"

# Binaries: caller sets VANILLA_BINDIR + PGRAC_BINDIR (absolute paths).
VANILLA_BINDIR="${VANILLA_BINDIR:-}"
PGRAC_BINDIR="${PGRAC_BINDIR:-/Users/yingjiewang/linkdb-install/bin}"


usage() {
    cat <<EOF
Usage: $PROGNAME [--quick | --full]

Modes:
  --quick (default)  9 combos: scale 50 × 3 modes × 3 clients (~1.5 hour)
  --full             27 combos: [10,50,100] × 3 modes × 3 clients (~4.5 hour)

Required env:
  VANILLA_BINDIR    absolute path to vanilla PG 16.13 install bin/ dir
  PGRAC_BINDIR      absolute path to pgrac --enable-cluster install bin/
                    dir (default: /Users/yingjiewang/linkdb-install/bin)

Optional env:
  DURATION          per-combo wallclock seconds (default: 600)
  WARMUP            warmup seconds discarded from analysis (default: 120)
  PORT_VANILLA      vanilla PG postmaster port (default: 55432)
  PORT_PGRAC        pgrac postmaster port (default: 55433)
  SHARED_BUFFERS    shared_buffers GUC (default: 4GB)

Output: scripts/perf/results/stage1-oltp-<YYYYMMDD-HHMM>/
EOF
}


# ---- parse CLI -------------------------------------------------------
case "${1:-}" in
    --quick) MODE_FLAG="--quick"; SCALES=(50) ;;
    --full)  MODE_FLAG="--full";  SCALES=(10 50 100) ;;
    -h|--help) usage; exit 0 ;;
    "")      SCALES=(50) ;;       # default --quick
    *)       echo "$PROGNAME: unknown flag: $1" >&2; usage; exit 2 ;;
esac

if [[ -z "$VANILLA_BINDIR" ]]; then
    echo "$PROGNAME: VANILLA_BINDIR env var required" >&2
    usage
    exit 2
fi


# ---- output dir ------------------------------------------------------
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TS="$(date +%Y%m%d-%H%M)"
OUTDIR="$SCRIPT_DIR/results/stage1-oltp-$TS"
mkdir -p "$OUTDIR"

CLIENTS=(1 8 16)
MODES=(vanilla pgrac-cluster-on pgrac-cluster-off)

echo "# spec-1.23 Stage 1 OLTP baseline ($MODE_FLAG)"
echo "#   scales: ${SCALES[*]}"
echo "#   clients: ${CLIENTS[*]}"
echo "#   modes: ${MODES[*]}"
echo "#   duration: ${DURATION}s (discarding first ${WARMUP}s warmup)"
echo "#   output: $OUTDIR"
echo


# ---- helpers ---------------------------------------------------------

# Start a postmaster for the given mode at the given scale.
# Returns: BINDIR, PGDATA, PGPORT echoed as "BINDIR|PGDATA|PGPORT".
prepare_instance() {
    local mode="$1"
    local scale="$2"
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

    pgdata="$OUTDIR/datadir-$mode-s$scale"
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

    # Init pgbench
    "$bindir/pgbench" -i -s "$scale" -h /tmp -p "$pgport" -U "$USER" postgres \
        >/dev/null 2>&1

    echo "$bindir|$pgdata|$pgport"
}


# Run one pgbench combo; returns TPS to stdout (post-warmup mean).
# Logs full output to $1 (logfile path).
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
        postgres > "$logfile" 2>&1 || true

    # Post-process: discard first WARMUP seconds, compute mean TPS from
    # remaining -P 30 progress samples.  pgbench -P 30 prints lines like:
    #   progress: 30.0 s, 1234.5 tps, lat 6.5 ms stddev 1.2
    awk -v warmup="$WARMUP" '
        /^progress:/ {
            t = $2 + 0
            if (t > warmup) {
                tps_sum += $4 + 0
                n++
            }
        }
        END {
            if (n > 0) printf("%.2f\n", tps_sum / n)
            else        printf("0.00\n")
        }' "$logfile"
}


stop_instance() {
    local bindir="$1"
    local pgdata="$2"
    "$bindir/pg_ctl" -D "$pgdata" -m fast stop >/dev/null 2>&1 || true
}


# ---- main loop -------------------------------------------------------
SUMMARY_CSV="$OUTDIR/summary.csv"
echo "mode,scale,clients,tps_post_warmup,duration_s,warmup_s" > "$SUMMARY_CSV"

for scale in "${SCALES[@]}"; do
    for mode in "${MODES[@]}"; do
        info=$(prepare_instance "$mode" "$scale")
        bindir=$(echo "$info" | cut -d'|' -f1)
        pgdata=$(echo "$info" | cut -d'|' -f2)
        pgport=$(echo "$info" | cut -d'|' -f3)

        for clients in "${CLIENTS[@]}"; do
            logfile="$OUTDIR/$mode-s$scale-c$clients.log"
            echo "[$(date +%H:%M:%S)] running $mode scale=$scale clients=$clients (${DURATION}s) ..."
            tps=$(run_one_combo "$logfile" "$bindir" "$pgport" "$clients")
            echo "$mode,$scale,$clients,$tps,$DURATION,$WARMUP" >> "$SUMMARY_CSV"
            echo "  -> tps (post-warmup mean) = $tps"
        done

        stop_instance "$bindir" "$pgdata"
    done
done


# ---- regression analysis ---------------------------------------------
REGRESSION_TXT="$OUTDIR/regression.txt"
{
    echo "# spec-1.23 Stage 1 OLTP regression analysis"
    echo "# generated: $(date)"
    echo "# baseline: vanilla PG 16.13"
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
            d_on=$(awk -v v="$v" -v p="$on" 'BEGIN{ if (v>0) printf("%.2f", (p-v)/v*100); else print "0.00"}')
            d_off=$(awk -v v="$v" -v p="$off" 'BEGIN{ if (v>0) printf("%.2f", (p-v)/v*100); else print "0.00"}')
            line="$scale,$clients,$v,$on,$off,$d_on,$d_off"
            tag=""
            if awk -v x="$d_on" 'BEGIN{ if (x+0 < -5 || x+0 > 5) exit 0; exit 1 }'; then
                tag=" *** >5% RCA TRIGGER ***"
            elif awk -v x="$d_on" 'BEGIN{ if (x+0 < -1 || x+0 > 1) exit 0; exit 1 }'; then
                tag=" * >1% investigation"
            fi
            echo "$line$tag"
        done
    done
} > "$REGRESSION_TXT"

echo
echo "[$(date +%H:%M:%S)] DONE"
echo "  summary:    $SUMMARY_CSV"
echo "  regression: $REGRESSION_TXT"
echo
echo "Per spec-1.23 Q3 REVISED: > 5% deltas require RCA + disposition"
echo "recorded in pgrac:docs/perf-baseline.md §3 before shipping."
