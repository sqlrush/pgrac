#!/bin/bash
#-------------------------------------------------------------------------
# measure-hang-latency.sh -- spec-5.20 D6 (MG-B) wrapper.
#
# Sets up the --enable-cluster install on PATH + IPC::Run on PERL5LIB, then
# delegates to measure-hang-latency.pl (which drives PostgreSQL::Test::Cluster).
# Report-only measure-and-decide: writes a JSON fragment under
# scripts/perf/results/.  NOT a ship gate (perf.yml Tier-3 weekly/manual).
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
# Spec: spec-5.20-hang-manager-acceptance.md (§1.2 D6)
# IDENTIFICATION
#    scripts/perf/measure-hang-latency.sh
#-------------------------------------------------------------------------
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$(realpath "$0")")/../.." && pwd)"
ENABLE_INSTALL="${PGRAC_ENABLE_INSTALL:-$HOME/linkdb-install}"
RESULTS_DIR="${PGRAC_RESULTS_DIR:-$REPO_ROOT/scripts/perf/results}"
TS="$(date -u +%Y%m%dT%H%M%SZ)"

[ -x "$ENABLE_INSTALL/bin/postgres" ] \
	|| { echo "enable postgres not found at $ENABLE_INSTALL/bin/postgres (set PGRAC_ENABLE_INSTALL)"; exit 1; }
mkdir -p "$RESULTS_DIR"

export PATH="$ENABLE_INSTALL/bin:$PATH"
export PERL5LIB="${PERL5LIB:-}${PERL5LIB:+:}$HOME/perl5/lib/perl5"
# PostgreSQL::Test::Cluster runs "$PG_REGRESS --config-auth" during init; set it
# to the built pg_regress (make check sets this automatically, standalone must).
export PG_REGRESS="${PG_REGRESS:-$REPO_ROOT/src/test/regress/pg_regress}"

echo "measure-hang-latency: install=$ENABLE_INSTALL ts=$TS"
perl "$REPO_ROOT/scripts/perf/measure-hang-latency.pl" \
	--enable-install="$ENABLE_INSTALL" \
	--results-dir="$RESULTS_DIR" \
	--timestamp="$TS"
