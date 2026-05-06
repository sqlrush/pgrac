#!/bin/bash
#-------------------------------------------------------------------------
#
# run-perf-baseline-check.sh -- spec-1.23 D6 short-version perf check (CI)
#
# Runs a 60-second pgbench TPC-B at scale 10, 1 client, against the
# pgrac --enable-cluster instance (cluster.enabled = on).  Captures
# TPS to a CI artifact for manual review.
#
# *** WARN-ONLY MODE (per spec-1.23 Q8 B-WARN REVISED) ***
#
# This script DOES NOT block PRs on regression.  GitHub Actions shared
# runners exhibit 10-20% variance (per scripts/perf/run-baseline.sh:12);
# any threshold below variance generates false-positive PR blocks that
# disrupt development flow.  The full PR-level blocking gate is deferred
# until self-hosted runner OR same-run vanilla/pgrac ratio comparison
# is available (future Stage 6 spec).
#
# CI integration: invoked from .github/workflows/ci.yml after the
# Build+Test step on the pgrac --enable-cluster job.  Exit 0 always
# (no pass/fail signal); output goes to artifact upload.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-1.23-stage1-oltp-baseline.md §D6
#
# IDENTIFICATION
#    scripts/ci/run-perf-baseline-check.sh
#
#-------------------------------------------------------------------------
set -uo pipefail   # NB: not -e; we never fail this script

PROGNAME="run-perf-baseline-check.sh"

DURATION="${PERF_DURATION:-60}"
SCALE="${PERF_SCALE:-10}"
CLIENTS="${PERF_CLIENTS:-1}"
PGRAC_BINDIR="${PGRAC_BINDIR:-$(pwd)/tmp_install/usr/local/pgsql/bin}"

# Find pgrac install bindir (CI-style or local-style)
if [[ ! -x "$PGRAC_BINDIR/initdb" ]]; then
    # Try alternate CI install path
    for cand in \
        /home/runner/work/linkdb/linkdb/tmp_install/usr/local/pgsql/bin \
        $(find tmp_install -name initdb -type f 2>/dev/null | head -1 | xargs dirname 2>/dev/null) \
        ; do
        if [[ -x "$cand/initdb" ]]; then
            PGRAC_BINDIR="$cand"
            break
        fi
    done
fi

if [[ ! -x "$PGRAC_BINDIR/initdb" ]]; then
    echo "$PROGNAME: WARN-ONLY: could not locate pgrac initdb binary" >&2
    echo "$PROGNAME: WARN-ONLY: PGRAC_BINDIR=$PGRAC_BINDIR" >&2
    echo "$PROGNAME: WARN-ONLY: skipping perf check (no fail)" >&2
    exit 0
fi

OUTDIR="${PERF_OUTDIR:-perf-baseline-artifact}"
mkdir -p "$OUTDIR"

PGDATA="$OUTDIR/datadir"
PGPORT="${PERF_PORT:-55590}"

cleanup() {
    "$PGRAC_BINDIR/pg_ctl" -D "$PGDATA" -m fast stop >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "$PROGNAME: WARN-ONLY mode (spec-1.23 Q8 B-WARN)"
echo "  pgrac bindir: $PGRAC_BINDIR"
echo "  duration: ${DURATION}s, scale: $SCALE, clients: $CLIENTS"

rm -rf "$PGDATA"
"$PGRAC_BINDIR/initdb" -D "$PGDATA" --no-clean --no-locale --auth=trust >/dev/null 2>&1 \
    || { echo "WARN-ONLY: initdb failed; skipping" >&2; exit 0; }

cat >> "$PGDATA/postgresql.conf" <<CONF
shared_buffers = 256MB
port = $PGPORT
checkpoint_timeout = 30min
cluster.enabled = on
cluster.node_id = 0
CONF

"$PGRAC_BINDIR/pg_ctl" -D "$PGDATA" -l "$OUTDIR/postmaster.log" -w start >/dev/null \
    || { echo "WARN-ONLY: postgres start failed; skipping" >&2; exit 0; }

"$PGRAC_BINDIR/pgbench" -i -s "$SCALE" -h /tmp -p "$PGPORT" -U "$USER" postgres \
    >/dev/null 2>&1 \
    || { echo "WARN-ONLY: pgbench init failed; skipping" >&2; exit 0; }

echo "Running pgbench TPC-B (${DURATION}s warn-only check)..."
"$PGRAC_BINDIR/pgbench" \
    -h /tmp -p "$PGPORT" -U "$USER" \
    -c "$CLIENTS" -j "$CLIENTS" \
    -T "$DURATION" -P 10 \
    --no-vacuum \
    postgres > "$OUTDIR/pgbench-output.log" 2>&1 || true

# Extract final tps line for the artifact summary.
tps_line=$(grep "^tps = " "$OUTDIR/pgbench-output.log" | head -1 || echo "tps = (unavailable)")

cat > "$OUTDIR/SUMMARY.md" <<EOF
# spec-1.23 D6 perf baseline check (warn-only artifact)

**Mode**: warn-only (Q8 B-WARN);non-blocking;shared runner variance 10-20%
unsuitable for blocking gate.

**Configuration**:
- duration: ${DURATION}s
- scale: $SCALE
- clients: $CLIENTS
- mode: pgrac --enable-cluster + cluster.enabled = on

**Result**: \`$tps_line\`

**Reviewer**: download artifact + manually inspect against historical
trend.  Significant unexplained drops (>30%) may indicate hot-path
regression; cross-reference with full baseline at user-side
\`scripts/perf/run-stage1-oltp-baseline.sh\`.

**Future**: this check upgrades to PR-level blocking gate once
self-hosted runner or same-run vanilla/pgrac ratio comparison is
available (deferred to Stage 6 spec).
EOF

echo "$PROGNAME: artifact: $OUTDIR/SUMMARY.md"
echo "  $tps_line"
exit 0
