#!/bin/bash
#-------------------------------------------------------------------------
#
# run-storage-io-matrix.sh
#	  spec-6.0a storage I/O conformance/perf report-only matrix.
#
#	  Runs a small single-node pgbench sample through the normal local
#	  backend and the raw block_device backend over a CI-portable regular
#	  file image.  The block_device leg disables O_DIRECT unless the caller
#	  opts in, so loopback numbers are report-only and carry a soundness
#	  marker instead of pretending to be hardware O_DIRECT measurements.
#
# IDENTIFICATION
#	  scripts/perf/run-storage-io-matrix.sh
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-6.0a-production-shared-storage-backend-matrix.md (D7)
#
#-------------------------------------------------------------------------
set -euo pipefail

INSTALL="${PGRAC_ENABLE_INSTALL:-$HOME/linkdb-install}"
SCALE="${STORAGE_IO_SCALE:-5}"
DURATION="${STORAGE_IO_DURATION:-10}"
CLIENTS="${STORAGE_IO_CLIENTS:-2}"
JOBS="${STORAGE_IO_JOBS:-2}"
RAW_MB="${STORAGE_IO_RAW_MB:-192}"
ODIRECT="${STORAGE_IO_ODIRECT:-off}"
OUTDIR="$(cd "$(dirname "$0")" && pwd)/results"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="$OUTDIR/storage-io-matrix-$STAMP.json"
WORK="$(mktemp -d /tmp/pgrac-storage-io.XXXXXX)"

cleanup() {
	rm -rf "$WORK"
}
trap cleanup EXIT

mkdir -p "$OUTDIR"

if [ ! -x "$INSTALL/bin/initdb" ]; then
	cat > "$OUT" <<EOF
{"status":"unavailable","reason":"install prefix not found","install":"$INSTALL"}
EOF
	echo "storage I/O matrix unavailable: install prefix not found at $INSTALL" >&2
	echo "results: $OUT"
	exit 0
fi

PATH="$INSTALL/bin:$PATH"
export PGHOST="$WORK"

write_unavailable() {
	local reason="$1"

	cat > "$OUT" <<EOF
{"status":"unavailable","reason":"$(json_escape "$reason")","install":"$(json_escape "$INSTALL")"}
EOF
	echo "storage I/O matrix unavailable: $reason" >&2
	echo "results: $OUT"
	exit 0
}

json_escape() {
	printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

bench_backend() {
	local backend="$1"
	local port="$2"
	local pgdata="$WORK/pgdata_$backend"
	local raw_image="$WORK/raw_$backend.img"
	local log="$WORK/log_$backend"
	local tps

	initdb -D "$pgdata" -A trust -N > /dev/null || return 1
	{
		echo "port = $port"
		echo "unix_socket_directories = '$WORK'"
		echo "listen_addresses = ''"
		echo "cluster.enabled = on"
		echo "cluster.node_id = 0"
		echo "cluster.allow_single_node = on"
		echo "cluster.smgr_user_relations = on"
		echo "autovacuum = off"
		echo "shared_buffers = '128MB'"
		echo "cluster.shared_storage_backend = $backend"
		if [ "$backend" = "block_device" ]; then
			truncate -s "${RAW_MB}M" "$raw_image"
			echo "cluster.block_device_path = '$raw_image'"
			echo "cluster.block_device_use_odirect = $ODIRECT"
		fi
	} >> "$pgdata/postgresql.conf"

	pg_ctl -D "$pgdata" -l "$log" -w start > /dev/null || return 1
	if ! pgbench -p "$port" -i -s "$SCALE" postgres > /dev/null 2>&1; then
		pg_ctl -D "$pgdata" -m fast -w stop > /dev/null || true
		return 1
	fi
	tps=$(pgbench -p "$port" -c "$CLIENTS" -j "$JOBS" -T "$DURATION" postgres 2>/dev/null \
		| awk '/tps =/ {print $3; exit}') || {
		pg_ctl -D "$pgdata" -m fast -w stop > /dev/null || true
		return 1
	}
	if [ -z "$tps" ]; then
		pg_ctl -D "$pgdata" -m fast -w stop > /dev/null || true
		return 1
	fi
	pg_ctl -D "$pgdata" -m fast -w stop > /dev/null || return 1

	printf '%s' "$tps"
}

if ! bench_backend local 54601 > "$WORK/tps_local"; then
	write_unavailable "local backend benchmark failed"
fi
if ! bench_backend block_device 54602 > "$WORK/tps_block"; then
	write_unavailable "block_device backend benchmark failed"
fi

TPS_LOCAL="$(cat "$WORK/tps_local")"
TPS_BLOCK="$(cat "$WORK/tps_block")"

cat > "$OUT" <<EOF
{
  "status": "ok",
  "soundness": {
    "block_device_odirect": "$(json_escape "$ODIRECT")",
    "ci_shape": "regular-file raw image; report-only unless STORAGE_IO_ODIRECT=on on a verified block device"
  },
  "settings": {
    "scale": $SCALE,
    "duration_seconds": $DURATION,
    "clients": $CLIENTS,
    "jobs": $JOBS,
    "raw_mb": $RAW_MB
  },
  "results": {
    "local_tps": "$TPS_LOCAL",
    "block_device_tps": "$TPS_BLOCK"
  }
}
EOF

echo "storage I/O matrix results: $OUT"
