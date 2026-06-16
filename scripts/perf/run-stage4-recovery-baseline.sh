#!/usr/bin/env bash
#-------------------------------------------------------------------------
#
# run-stage4-recovery-baseline.sh
#    spec-4.13 D1 — Stage 4 recovery + write-fence perf baseline driver.
#
#    Measure-only harness (no optimization).  Emits one JSON artifact with two
#    arrays:
#      bands[]            steady-state / feature-toggle / write-fence in-path
#                         TPS bands (median of N reps, failed-tx rejected).
#      recovery_timings[] block / thread / cold recovery latency segments.
#
#    Bands wired in D1 (AUTO, single-node smoke runnable):
#      steady   native(--disable-cluster) vs cluster-on default GUCs (rw + ro).
#      toggles  per-feature idle tax: online_block_recovery / online_thread_
#               recovery off->on, plus all-on (write fence enforcement stays off).
#      fence    write_fence_enforcement=off in-path tax; asserts hot_gate_blocked
#               counter stays 0 (the OFF-mode gate is a no-op).
#
#    Recovery latency bands (block / thread / cold) consume the D4/D5/D6 LOG-only
#    timing surfaces; until those land this driver emits verdict=SKIP with a
#    reason (no fabricated numbers — 实现完整性, explicit refusal not fake stub).
#    The real 2-node steady-state band (P0-1) routes through the ClusterPair Perl
#    harness (D2); single-node here is the harness smoke + structural reference.
#
#    Tiers (per-band -T seconds, override with --duration):
#      smoke = 30   medium = 300   long = 1800
#    Band selector: all | steady | toggles | fence | block | thread | cold
#
#    Clean-run contract:  PGRAC_ENABLE_INSTALL / PGRAC_DISABLE_INSTALL MUST be
#    non-cassert, non-injection-points release builds (cassert ~40% noise).  This
#    driver greps pg_config --configure and WARNs if --enable-cassert is present.
#    本机数仅探路;closure 终判只信 clean Linux CI / dedicated runner artifact (规则 23).
#
#    Env:
#      PGRAC_ENABLE_INSTALL    --enable-cluster prefix (required)
#      PGRAC_DISABLE_INSTALL   --disable-cluster prefix (required; native baseline)
#      STAGE4_REPS (3) STAGE4_CLIENTS (4) STAGE4_JOBS (2) STAGE4_PORT (55411)
#
#    Usage:
#      PGRAC_ENABLE_INSTALL=/p/enable PGRAC_DISABLE_INSTALL=/p/disable \
#        scripts/perf/run-stage4-recovery-baseline.sh \
#          --tier=smoke --bands=all --nodes=1 --scale=10 --output=PATH
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
# Spec: spec-4.13-stage4-recovery-write-fence-perf-baseline.md
#
#-------------------------------------------------------------------------
set -u
set -o pipefail

PROG=run-stage4-recovery-baseline.sh

# ---- args -------------------------------------------------------------------
TIER=smoke
BANDS=all
NODES=1
SCALE=10
DURATION=
OUTPUT=

for arg in "$@"; do
	case "$arg" in
		--tier=*)     TIER="${arg#*=}" ;;
		--bands=*)    BANDS="${arg#*=}" ;;
		--nodes=*)    NODES="${arg#*=}" ;;
		--scale=*)    SCALE="${arg#*=}" ;;
		--duration=*) DURATION="${arg#*=}" ;;
		--output=*)   OUTPUT="${arg#*=}" ;;
		-h|--help)
			sed -n '2,48p' "$0"; exit 0 ;;
		*) echo "$PROG: unknown arg: $arg" >&2; exit 2 ;;
	esac
done

case "$TIER" in
	smoke)  DUR_DEFAULT=30 ;;
	medium) DUR_DEFAULT=300 ;;
	long)   DUR_DEFAULT=1800 ;;
	*) echo "$PROG: --tier must be smoke|medium|long, got: $TIER" >&2; exit 2 ;;
esac
DUR="${DURATION:-$DUR_DEFAULT}"

case "$BANDS" in
	all|steady|toggles|fence|block|thread|cold) : ;;
	*) echo "$PROG: --bands must be all|steady|toggles|fence|block|thread|cold" >&2; exit 2 ;;
esac

EN="${PGRAC_ENABLE_INSTALL:?set PGRAC_ENABLE_INSTALL (non-cassert --enable-cluster prefix)}"
DIS="${PGRAC_DISABLE_INSTALL:?set PGRAC_DISABLE_INSTALL (non-cassert --disable-cluster prefix)}"
REPS="${STAGE4_REPS:-3}"
CL="${STAGE4_CLIENTS:-4}"
J="${STAGE4_JOBS:-2}"
PORT="${STAGE4_PORT:-55411}"

[ -x "$EN/bin/pgbench" ]  || { echo "$PROG: pgbench not found at $EN/bin" >&2; exit 3; }
[ -x "$DIS/bin/pgbench" ] || { echo "$PROG: pgbench not found at $DIS/bin" >&2; exit 3; }

TS="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_DIR="$(cd "$(dirname "$0")" && pwd)/results"
mkdir -p "$RESULTS_DIR"
OUT_JSON="${OUTPUT:-$RESULTS_DIR/perf-stage4-$TIER-$TS.json}"
RAW_LOG="${OUT_JSON%.json}.raw.log"
: > "$RAW_LOG"

die()  { echo "$PROG: ERROR: $*" >&2; exit 1; }
log()  { echo "$PROG: $*"; echo "$PROG: $*" >> "$RAW_LOG"; }
raw()  { echo "$@" >> "$RAW_LOG"; }

# ---- helpers (median / tax reuse the quadrant-matrix conventions) -----------
median() {
	local sorted n mid
	sorted=$(printf '%s\n' "$@" | sort -n)
	n=$#; mid=$(( (n + 1) / 2 ))
	printf '%s\n' "$sorted" | sed -n "${mid}p"
}
# tax% = (base - test) / base * 100, 1 decimal; NA when base<=0.
tax() { awk -v b="$1" -v t="$2" 'BEGIN{ if(b<=0){print "NA"} else {printf "%.1f", (b-t)/b*100} }'; }
# unstable: max-min spread over median > 5% -> echo "1" else "0".
unstable() {
	awk 'BEGIN{ mn=1e18; mx=-1e18 }
	     { v=$1+0; if(v<mn)mn=v; if(v>mx)mx=v; s+=v; n++ }
	     END{ if(n==0||s<=0){print 0; exit} med=s/n; print ((mx-mn)/med>0.05)?1:0 }' \
	    <(printf '%s\n' "$@")
}

# read one pg_cluster_state counter (category.key) -> integer (0 if absent).
get_counter() {
	local prefix="$1" cat="$2" key="$3" v
	v=$("$prefix/bin/psql" -h /tmp -p "$PORT" -At postgres \
		-c "SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'" 2>/dev/null)
	echo "${v:-0}"
}

cassert_guard() {
	local prefix="$1" cfg
	cfg=$("$prefix/bin/pg_config" --configure 2>/dev/null)
	# WARN to stderr + raw log only — stdout is captured by the caller (must
	# stay a clean release|cassert token).
	if printf '%s' "$cfg" | grep -q -- '--enable-cassert'; then
		echo "$PROG: WARN clean-run contract: $prefix has --enable-cassert (≈40% noise) — 仅探路, 不可作 closure 终判 (规则 23)" >&2
		raw "WARN cassert build at $prefix"
		echo cassert
	else
		echo release
	fi
}

# ---- band runner ------------------------------------------------------------
# run_cell prefix clon label guc_extra_lines... -> echoes
#   "rw_med|ro_med|failed_total|hot_gate_blocked|rw_all|ro_all"
# clon=1 -> pgrac-init (cluster-on); clon=0 -> initdb (native).
run_cell() {
	local prefix="$1" clon="$2" label="$3"; shift 3
	local dd="/tmp/s4_${label}" clog="/tmp/s4_${label}.log"
	rm -rf "$dd" "$clog"
	if [ "$clon" = 1 ]; then
		"$prefix/bin/pgrac-init" -D "$dd" --node-id=0 --cluster-name=s4 --force >/dev/null 2>&1 \
			|| die "pgrac-init failed ($dd)"
	else
		"$prefix/bin/initdb" -D "$dd" -A trust -N >/dev/null 2>&1 || die "initdb failed ($dd)"
	fi
	{
		echo "port = $PORT"
		echo "unix_socket_directories = '/tmp'"
		echo "listen_addresses = ''"
		echo "fsync = on"
		echo "synchronous_commit = on"
		local line
		for line in "$@"; do echo "$line"; done
	} >> "$dd/postgresql.conf"
	"$prefix/bin/pg_ctl" -D "$dd" -l "$clog" -w -t 60 start >/dev/null 2>&1 \
		|| { cat "$clog" >> "$RAW_LOG"; die "start failed ($dd; see $clog)"; }
	"$prefix/bin/pgbench" -h /tmp -p "$PORT" -i -s "$SCALE" --quiet postgres >/dev/null 2>&1 \
		|| die "pgbench -i failed ($dd)"

	local rw_all=() ro_all=() failed_total=0
	local r out tps failed
	for r in $(seq 1 "$REPS"); do
		out=$("$prefix/bin/pgbench" -h /tmp -p "$PORT" -c "$CL" -j "$J" -T "$DUR" -n postgres 2>&1)
		raw "--- cell=$label rw rep=$r ---"; raw "$out"
		tps=$(printf '%s\n' "$out" | awk '/^tps = /{print $3; exit}')
		[ -n "$tps" ] || { raw "$out"; die "no rw tps ($label rep $r)"; }
		failed=$(printf '%s\n' "$out" | awk -F'[ (]' '/number of failed/{print $0}' \
			| grep -oE '[0-9]+ \(' | grep -oE '^[0-9]+' | head -1)
		failed_total=$(( failed_total + ${failed:-0} ))
		rw_all+=("$tps")
	done
	for r in $(seq 1 "$REPS"); do
		out=$("$prefix/bin/pgbench" -h /tmp -p "$PORT" -S -c "$CL" -j "$J" -T "$DUR" -n postgres 2>&1)
		tps=$(printf '%s\n' "$out" | awk '/^tps = /{print $3; exit}')
		[ -n "$tps" ] || die "no ro tps ($label rep $r)"
		ro_all+=("$tps")
	done

	local hgb=0
	[ "$clon" = 1 ] && hgb=$(get_counter "$prefix" write_fence hot_gate_blocked)

	"$prefix/bin/pg_ctl" -D "$dd" -m fast -w stop >/dev/null 2>&1
	rm -rf "$dd" "$clog"
	local rw_med ro_med
	rw_med=$(median "${rw_all[@]}"); ro_med=$(median "${ro_all[@]}")
	echo "$rw_med|$ro_med|$failed_total|$hgb|${rw_all[*]}|${ro_all[*]}"
}

# verdict for a TPS band given tax% and green/yellow thresholds + failed/unstable.
band_verdict() {
	local taxpct="$1" green="$2" yellow="$3" failed="$4" unst="$5"
	[ "$failed" != 0 ] && { echo POLLUTED; return; }
	[ "$unst" = 1 ]    && { echo UNSTABLE; return; }
	[ "$taxpct" = NA ] && { echo SKIP; return; }
	awk -v t="$taxpct" -v g="$green" -v y="$yellow" \
		'BEGIN{ if(t<=g)print"GREEN"; else if(t<=y)print"YELLOW"; else print"RED" }'
}

# ---- JSON accumulation ------------------------------------------------------
BANDS_JSON=()
RECOV_JSON=()

emit_band() { BANDS_JSON+=("$1"); }
emit_recov() { RECOV_JSON+=("$1"); }

skip_recov() {  # id reason
	emit_recov "$(printf '{"id":"%s","verdict":"SKIP","reason":"%s"}' "$1" "$2")"
	log "recovery band $1: SKIP ($2)"
}

# ---- bands ------------------------------------------------------------------
CLUSTER_RW=
run_steady() {
	log "band steady: native(--disable-cluster) vs cluster-on default GUCs"
	local nat clu nrw nro crw cro nfail cfail crwall taxrw taxro unst vr
	nat=$(run_cell "$DIS" 0 "native")
	clu=$(run_cell "$EN" 1 "cluster_on" \
		"cluster.online_block_recovery = off" \
		"cluster.online_thread_recovery = off" \
		"cluster.write_fence_enforcement = off")
	nrw=$(echo "$nat"|cut -d'|' -f1); nro=$(echo "$nat"|cut -d'|' -f2); nfail=$(echo "$nat"|cut -d'|' -f3)
	crw=$(echo "$clu"|cut -d'|' -f1); cro=$(echo "$clu"|cut -d'|' -f2); cfail=$(echo "$clu"|cut -d'|' -f3)
	crwall=$(echo "$clu"|cut -d'|' -f5)
	CLUSTER_RW="$crw"
	taxrw=$(tax "$nrw" "$crw"); taxro=$(tax "$nro" "$cro")
	unst=$(unstable $crwall)
	vr=$(band_verdict "$taxrw" 10 20 "$cfail" "$unst")
	log "  P0-1(single-node structural) rw_tax=$taxrw% ro_tax=$taxro% verdict=$vr (real P0-1 2-node 走 ClusterPair=D2)"
	emit_band "$(printf '{"id":"P0-1-single","name":"steady-state single-node structural (cluster-on vs native)","config":{"nodes":1,"workload":"pgbench-rw+ro","note":"real 2-node P0-1 via ClusterPair=D2"},"native":{"rw_tps":%s,"ro_tps":%s,"failed":%s},"cluster":{"rw_tps":%s,"ro_tps":%s,"failed":%s},"rw_tax_pct":"%s","ro_tax_pct":"%s","rw_runs":[%s],"verdict":"%s"}' \
		"$nrw" "$nro" "$nfail" "$crw" "$cro" "$cfail" "$taxrw" "$taxro" "$(echo "$crwall"|tr ' ' ',')" "$vr")"
}

run_toggles() {
	log "band toggles: feature idle tax (baseline = all-off cluster-on)"
	local base off_rw
	# baseline: all features off
	base=$(run_cell "$EN" 1 "tg_off" \
		"cluster.online_block_recovery = off" \
		"cluster.online_thread_recovery = off" \
		"cluster.write_fence_enforcement = off")
	off_rw=$(echo "$base"|cut -d'|' -f1)
	local id name cell crw cfail taxv unst vr crwall
	for spec in \
		"P0-12-block|online_block_recovery on|cluster.online_block_recovery = on|cluster.online_thread_recovery = off" \
		"P0-12-thread|online_thread_recovery on|cluster.online_block_recovery = off|cluster.online_thread_recovery = on" \
		"P0-2-allon|block+thread all-on|cluster.online_block_recovery = on|cluster.online_thread_recovery = on" ; do
		id=$(echo "$spec"|cut -d'|' -f1); name=$(echo "$spec"|cut -d'|' -f2)
		local g1 g2; g1=$(echo "$spec"|cut -d'|' -f3); g2=$(echo "$spec"|cut -d'|' -f4)
		cell=$(run_cell "$EN" 1 "tg_${id}" "$g1" "$g2" "cluster.write_fence_enforcement = off")
		crw=$(echo "$cell"|cut -d'|' -f1); cfail=$(echo "$cell"|cut -d'|' -f3); crwall=$(echo "$cell"|cut -d'|' -f5)
		taxv=$(tax "$off_rw" "$crw"); unst=$(unstable $crwall)
		# all-on threshold 5%, single feature 2%
		case "$id" in *allon*) vr=$(band_verdict "$taxv" 5 10 "$cfail" "$unst") ;; *) vr=$(band_verdict "$taxv" 2 5 "$cfail" "$unst") ;; esac
		log "  $id idle_tax=$taxv% verdict=$vr (baseline_rw=$off_rw test_rw=$crw)"
		emit_band "$(printf '{"id":"%s","name":"%s","config":{"nodes":1,"write_fence":"off"},"baseline_rw_tps":%s,"test_rw_tps":%s,"failed":%s,"idle_tax_pct":"%s","rw_runs":[%s],"verdict":"%s"}' \
			"$id" "$name" "$off_rw" "$crw" "$cfail" "$taxv" "$(echo "$crwall"|tr ' ' ',')" "$vr")"
	done
}

run_fence() {
	log "band fence: P0-3 write_fence_enforcement=off in-path tax + hot_gate_blocked==0 assert"
	local cell crw cfail hgb base_rw taxv unst vr crwall
	cell=$(run_cell "$EN" 1 "fence_off" \
		"cluster.online_block_recovery = off" \
		"cluster.online_thread_recovery = off" \
		"cluster.write_fence_enforcement = off")
	crw=$(echo "$cell"|cut -d'|' -f1); cfail=$(echo "$cell"|cut -d'|' -f3)
	hgb=$(echo "$cell"|cut -d'|' -f4); crwall=$(echo "$cell"|cut -d'|' -f5)
	# The OFF-mode gate is always compiled on-path (6 smgr entries); it cannot be
	# toggled out, so the in-path tax is measured against the cluster-on steady
	# baseline (also fence-off) — both configs identical except for re-measure
	# noise, so a non-trivial delta here means measurement instability, not gate
	# cost.  The meaningful assertion is hot_gate_blocked==0 (gate never fires in
	# OFF mode).  True enforcement=on overhead is untestable in steady state
	# (no durable marker → all writes fail-closed) → forward 4.12b.
	base_rw="${CLUSTER_RW:-0}"
	taxv=$(tax "$base_rw" "$crw"); unst=$(unstable $crwall)
	vr=$(band_verdict "$taxv" 2 5 "$cfail" "$unst")
	local hgb_ok=PASS; [ "$hgb" = 0 ] || hgb_ok=FAIL
	log "  P0-3 in-path_tax=$taxv% (vs cluster-on steady) hot_gate_blocked=$hgb [$hgb_ok] verdict=$vr"
	emit_band "$(printf '{"id":"P0-3","name":"write-fence in-path tax (enforcement=off no-op gate)","config":{"nodes":1,"write_fence_enforcement":"off"},"baseline":"cluster-on steady (fence-off)","cluster_steady_rw_tps":%s,"fence_off_rw_tps":%s,"failed":%s,"hot_gate_blocked":%s,"hot_gate_blocked_assert":"%s","in_path_tax_pct":"%s","rw_runs":[%s],"verdict":"%s","note":"enforcement=on 稳态全 fail-closed 不可测 → forward 4.12b"}' \
		"$base_rw" "$crw" "$cfail" "$hgb" "$hgb_ok" "$taxv" "$(echo "$crwall"|tr ' ' ',')" "$vr")"
	# P0-4 qvotec tick needs 2-node quorum + durable marker authority → not single-node.
	skip_recov "P0-4-qvotec" "qvotec tick refresh needs 2-node quorum harness (D3 ClusterPair); single-node has no authority marker"
}

run_block_recovery() {
	skip_recov "P0-5-near-fpi" "block recovery latency needs D4 LOG-only trigger->return timing + t/257 corrupt-block driver (dedicated runner)"
	skip_recov "P0-6-far-fpi"  "as P0-5; large WAL window variant (D4)"
	skip_recov "P0-7-failclosed" "block recovery fail-closed latency; D4 (verify no page install, data_corrupted)"
}
run_thread_recovery() {
	skip_recov "P0-8-small"  "thread recovery freeze/replay/publish/post segments need D5 LOG/counter; visibility folded into replay (audit §5.2); SRF cluster_thread_replay_one_test driver"
	skip_recov "P0-9-medlarge" "as P0-8; medium/large WAL window throughput (D5)"
	skip_recov "P0-10-failclosed" "thread recovery fail-closed (injection cluster-thread-recovery-drive); keep-frozen, 0 authority (D5)"
}
run_cold_recovery() {
	skip_recov "P0-11-cold" "cold recovery / k-way merge startup time needs crash-restart + run-sharedfs-bench.sh merged-recovery driver (D6, dedicated runner)"
}

# ---- provenance + dispatch --------------------------------------------------
EN_BUILD=$(cassert_guard "$EN")
DIS_BUILD=$(cassert_guard "$DIS")
COMMIT=$(git -C "$(dirname "$0")/../.." rev-parse --short HEAD 2>/dev/null || echo unknown)
OS=$(uname -s); ARCH=$(uname -m); CPU=$(uname -srm)

log "spec-4.13 D1  tier=$TIER bands=$BANDS nodes=$NODES scale=$SCALE dur=${DUR}s reps=$REPS  commit=$COMMIT  $CPU"
log "enable=$EN ($EN_BUILD)  disable=$DIS ($DIS_BUILD)"
[ "$NODES" = 1 ] || log "WARN nodes=$NODES requested; D1 single-node only — 2-node steady (P0-1) + recovery bands route through ClusterPair/dedicated (D2+)"

case "$BANDS" in
	all)     run_steady; run_toggles; run_fence; run_block_recovery; run_thread_recovery; run_cold_recovery ;;
	steady)  run_steady ;;
	toggles) run_toggles ;;
	fence)   run_steady; run_fence ;;   # fence in-path tax references native steady baseline
	block)   run_block_recovery ;;
	thread)  run_thread_recovery ;;
	cold)    run_cold_recovery ;;
esac

# ---- emit JSON --------------------------------------------------------------
# join objects passed as "$@" into indented comma-separated JSON lines
# (bash 3.2-safe: no namerefs; empty case handled by the caller guard).
json_array() {
	local n=$# i=0 obj
	for obj in "$@"; do
		i=$(( i + 1 ))
		printf '    %s' "$obj"
		[ "$i" -lt "$n" ] && printf ','
		printf '\n'
	done
}
{
	printf '{\n'
	printf '  "spec": "4.13",\n'
	printf '  "commit": "%s",\n' "$COMMIT"
	printf '  "tier": "%s",\n' "$TIER"
	printf '  "bands_selector": "%s",\n' "$BANDS"
	printf '  "runner": {"os":"%s","arch":"%s","cpu":"%s","timestamp":"%s"},\n' "$OS" "$ARCH" "$CPU" "$TS"
	printf '  "build_flags": {"enable":"%s","disable":"%s"},\n' "$EN_BUILD" "$DIS_BUILD"
	printf '  "params": {"scale":%s,"duration_s":%s,"reps":%s,"clients":%s,"jobs":%s,"nodes":%s},\n' \
		"$SCALE" "$DUR" "$REPS" "$CL" "$J" "$NODES"
	printf '  "bands": [\n'
	[ "${#BANDS_JSON[@]}" -gt 0 ] && json_array "${BANDS_JSON[@]}"
	printf '  ],\n'
	printf '  "recovery_timings": [\n'
	[ "${#RECOV_JSON[@]}" -gt 0 ] && json_array "${RECOV_JSON[@]}"
	printf '  ]\n'
	printf '}\n'
} > "$OUT_JSON"

log "JSON artifact: $OUT_JSON"
log "raw log:       $RAW_LOG"
if command -v python3 >/dev/null 2>&1; then
	python3 -m json.tool "$OUT_JSON" >/dev/null \
		&& log "JSON validated (python3 -m json.tool OK)" \
		|| die "emitted JSON is invalid: $OUT_JSON"
fi
log "done. ($OS 本机数仅探路;终判只信 clean Linux CI / dedicated runner artifact — 规则 23)"
