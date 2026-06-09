#!/bin/bash
#-------------------------------------------------------------------------
#
# run-quadrant-matrix.sh
#    spec-3.23 D0/D1 — write-path perf four-quadrant matrix (CI-runnable).
#
#    The single "default cluster=on" number (run-baseline.sh) conflates three
#    taxes: the CR visibility gate (cluster.cr_mvcc_gate), the spec-3.18 undo
#    write-back optimization (cluster.undo_buffer_writeback), and fsync I/O
#    amplification.  This script separates them into a 2x2 GUC matrix so the
#    ≤10% write-path judgment lands on the RIGHT口径 (quadrant C) and the CR-gate
#    cost (A-B / D-C) is isolated for the spec-3.23 hot-path investigation.
#
#      native       = --disable-cluster build (true native PG baseline)
#      A gate=on  wb=off   default product form (worst quadrant)
#      B gate=off wb=off   structural + fsync, no CR gate   (CR gate tax = A - B)
#      C gate=off wb=on    spec-3.18 optimized PURE structural write-path tax
#                          (≤10% judgment lands here; writeback benefit = B - C)
#      D gate=on  wb=on    product-optimized form (gate + optimized writeback)
#
#    Each cell: pgbench rw + ro, REPS repetitions, MEDIAN tps (variance defence —
#    a single run is never conclusive, 规则 23 + spec-3.23 §2.6).  Cells with any
#    failed transaction are rejected (pollution).  fsync=on is the production
#    semantics; FSYNC_MATRIX=yes adds an fsync=off pass to split I/O vs CPU tax.
#
#    Env:
#      PGRAC_ENABLE_INSTALL   --enable-cluster prefix (required)
#      PGRAC_DISABLE_INSTALL  --disable-cluster prefix (required; native baseline)
#      QM_SCALE (10) QM_DURATION (20) QM_REPS (3) QM_CLIENTS (8) QM_JOBS (4)
#      FSYNC_MATRIX (no)      yes -> also run an fsync=off pass
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Spec: spec-3.23-write-path-perf-retest.md (Hardening v1.0.1 §C four-quadrant)
#
#-------------------------------------------------------------------------
set -u

PROG=run-quadrant-matrix.sh
EN="${PGRAC_ENABLE_INSTALL:?set PGRAC_ENABLE_INSTALL}"
DIS="${PGRAC_DISABLE_INSTALL:?set PGRAC_DISABLE_INSTALL}"
SCALE="${QM_SCALE:-10}"
DUR="${QM_DURATION:-20}"
REPS="${QM_REPS:-3}"
CL="${QM_CLIENTS:-8}"
J="${QM_JOBS:-4}"
FSYNC_MATRIX="${FSYNC_MATRIX:-no}"
PORT=55401
TS="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS="${QM_RESULTS_DIR:-/tmp/qm-$TS}"
mkdir -p "$RESULTS"

die() { echo "$PROG: ERROR: $*" >&2; exit 1; }

# median of a space-separated list of numbers (bash; integer-ish floats).
median() {
  local sorted n mid
  sorted=$(printf '%s\n' "$@" | sort -n)
  n=$#; mid=$(( (n + 1) / 2 ))
  printf '%s\n' "$sorted" | sed -n "${mid}p"
}

# run one cell: prefix datadir node-on(0|1) gate wb fsync  -> echoes "rw_med ro_med rw_all ro_all"
run_cell() {
  local prefix="$1" dd="$2" clon="$3" gate="$4" wb="$5" fsync="$6"
  local log="$dd.log"
  export PATH="$prefix/bin:$PATH"
  rm -rf "$dd" "$log"
  if [ "$clon" = 1 ]; then
    "$prefix/bin/pgrac-init" -D "$dd" --node-id=0 --cluster-name=qm --force >/dev/null 2>&1 \
      || die "pgrac-init failed ($dd)"
  else
    "$prefix/bin/initdb" -D "$dd" -A trust -N >/dev/null 2>&1 || die "initdb failed ($dd)"
  fi
  {
    echo "port = $PORT"
    echo "unix_socket_directories = '/tmp'"
    echo "listen_addresses = ''"
    echo "fsync = $fsync"
    echo "synchronous_commit = $fsync"
    if [ "$clon" = 1 ]; then
      echo "cluster.cr_mvcc_gate = $gate"
      echo "cluster.undo_buffer_writeback = $wb"
    fi
  } >> "$dd/postgresql.conf"
  "$prefix/bin/pg_ctl" -D "$dd" -l "$log" -w -t 60 start >/dev/null 2>&1 || die "start failed ($dd; see $log)"
  "$prefix/bin/pgbench" -h /tmp -p "$PORT" -i -s "$SCALE" --quiet postgres >/dev/null 2>&1

  local rw_all=() ro_all=()
  local r out tps failed
  for r in $(seq 1 "$REPS"); do
    out=$("$prefix/bin/pgbench" -h /tmp -p "$PORT" -c "$CL" -j "$J" -T "$DUR" postgres 2>&1)
    failed=$(printf '%s\n' "$out" | awk -F'[ (]' '/number of failed/ {print $0}' | grep -oE '[0-9]+ \(' | grep -oE '^[0-9]+' | head -1)
    tps=$(printf '%s\n' "$out" | awk '/^tps = /{print $3; exit}')
    [ -n "$tps" ] || die "no rw tps ($dd rep $r)"
    [ "${failed:-0}" = 0 ] || echo "$PROG: WARN rw cell gate=$gate wb=$wb fsync=$fsync rep=$r failed=$failed (污染,记但标)" >&2
    rw_all+=("$tps")
  done
  for r in $(seq 1 "$REPS"); do
    out=$("$prefix/bin/pgbench" -h /tmp -p "$PORT" -S -c "$CL" -j "$J" -T "$DUR" postgres 2>&1)
    tps=$(printf '%s\n' "$out" | awk '/^tps = /{print $3; exit}')
    [ -n "$tps" ] || die "no ro tps ($dd rep $r)"
    ro_all+=("$tps")
  done
  "$prefix/bin/pg_ctl" -D "$dd" -m fast -w stop >/dev/null 2>&1
  rm -rf "$dd" "$log"
  local rw_med ro_med
  rw_med=$(median "${rw_all[@]}"); ro_med=$(median "${ro_all[@]}")
  echo "$rw_med|$ro_med|${rw_all[*]}|${ro_all[*]}"
}

# tax% = (native - cell) / native * 100, integer-rounded.
tax() { awk -v n="$1" -v c="$2" 'BEGIN{ if(n<=0){print "NA"} else {printf "%.1f", (n-c)/n*100} }'; }

run_one_fsync() {
  local fsync="$1"
  echo "============================================================"
  echo "$PROG: fsync=$fsync synchronous_commit=$fsync  scale=$SCALE dur=${DUR}s reps=$REPS clients=$CL"
  echo "============================================================"
  local nat A B C D
  nat=$(run_cell "$DIS" "/tmp/qm_native" 0 -   -   "$fsync")
  A=$(run_cell  "$EN" "/tmp/qm_A" 1 on  off "$fsync")
  B=$(run_cell  "$EN" "/tmp/qm_B" 1 off off "$fsync")
  C=$(run_cell  "$EN" "/tmp/qm_C" 1 off on  "$fsync")
  D=$(run_cell  "$EN" "/tmp/qm_D" 1 on  on  "$fsync")
  local nrw; nrw=$(echo "$nat" | cut -d'|' -f1)
  local nro; nro=$(echo "$nat" | cut -d'|' -f2)
  echo ""
  echo "| cell | gate | wb | rw_tps(med) | rw_tax% | ro_tps(med) | ro_tax% | rw_all |"
  echo "|---|---|---|---|---|---|---|---|"
  printf "| native | - | - | %s | 0 | %s | 0 | %s |\n" "$nrw" "$nro" "$(echo "$nat"|cut -d'|' -f3)"
  for row in "A|on|off|$A" "B|off|off|$B" "C|off|on|$C" "D|on|on|$D"; do
    local lbl g w cell rw ro rwall
    lbl=$(echo "$row"|cut -d'|' -f1); g=$(echo "$row"|cut -d'|' -f2); w=$(echo "$row"|cut -d'|' -f3)
    cell=$(echo "$row"|cut -d'|' -f4-); rw=$(echo "$cell"|cut -d'|' -f1); ro=$(echo "$cell"|cut -d'|' -f2)
    rwall=$(echo "$cell"|cut -d'|' -f3)
    printf "| %s | %s | %s | %s | %s | %s | %s | %s |\n" \
      "$lbl" "$g" "$w" "$rw" "$(tax "$nrw" "$rw")" "$ro" "$(tax "$nro" "$ro")" "$rwall"
  done
  local arw brw crw drw
  arw=$(echo "$A"|cut -d'|' -f1); brw=$(echo "$B"|cut -d'|' -f1)
  crw=$(echo "$C"|cut -d'|' -f1); drw=$(echo "$D"|cut -d'|' -f1)
  echo ""
  echo "$PROG: fsync=$fsync DECOMPOSITION (rw):"
  echo "  C (gate-off, wb-on) = 纯结构写路径税 = $(tax "$nrw" "$crw")%  <-- ≤10% 终判看这个"
  echo "  CR gate tax (A vs B) = $(tax "$brw" "$arw")% extra slowdown of gate-on at wb-off"
  echo "  CR gate tax (D vs C) = $(tax "$crw" "$drw")% extra slowdown of gate-on at wb-on"
  echo "  writeback benefit (B->C) = native-tax drops $(tax "$nrw" "$brw")% -> $(tax "$nrw" "$crw")% (AD-014)"
  echo "  A (default product) tax = $(tax "$nrw" "$arw")% ; D (product-optimized) tax = $(tax "$nrw" "$drw")%"
}

echo "$PROG: enable=$EN disable=$DIS  $(uname -srm)  $(git -C "$(dirname "$0")/../.." rev-parse --short HEAD 2>/dev/null || echo ?)"
run_one_fsync on
if [ "$FSYNC_MATRIX" = yes ]; then
  run_one_fsync off
fi
echo "$PROG: done. ($(uname -s) 本机数仅探路;终判只信 clean Linux CI artifact — 规则 23)"
