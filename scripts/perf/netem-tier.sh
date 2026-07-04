#!/usr/bin/env bash
#-------------------------------------------------------------------------
#
# netem-tier.sh
#    spec-6.12 D0-shared -- loopback latency-injection tier (Linux tc).
#
#    Single-machine loopback RTT (~0.02ms) is NOT representative of a
#    real interconnect; any cross-node coordination benefit measured
#    without a latency tier is an artifact.  This helper arms/disarms a
#    fixed netem delay on the loopback device so the 6.12 value-gate
#    runs (run_612_value_gate.pl / run_2node_xnode_profile.pl) can be
#    taken at a declared RTT.  Linux-only by design: macOS has no tc;
#    the runner records the netem state in its report header and local
#    macOS numbers stay trend-only.
#
#    Usage:
#      netem-tier.sh setup <delay_ms> [iface]   # arm netem (root)
#      netem-tier.sh teardown [iface]           # disarm (root)
#      netem-tier.sh status [iface]             # show current qdisc
#
#    Default iface: lo.  Exit codes: 0 ok; 2 unsupported platform /
#    missing tc; 3 needs root; 4 bad usage.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (D0-shared)
#
# IDENTIFICATION
#    scripts/perf/netem-tier.sh
#
#-------------------------------------------------------------------------
set -euo pipefail

cmd="${1:-}"

if [ "$(uname -s)" != "Linux" ]; then
    echo "netem-tier: unsupported platform $(uname -s) (Linux tc only);" >&2
    echo "netem-tier: local numbers without a latency tier are trend-only." >&2
    exit 2
fi

if ! command -v tc >/dev/null 2>&1; then
    echo "netem-tier: 'tc' not found (install iproute2)" >&2
    exit 2
fi

case "$cmd" in
    setup)
        delay_ms="${2:-}"
        iface="${3:-lo}"
        case "$delay_ms" in
            '' | *[!0-9]*)
                echo "usage: netem-tier.sh setup <delay_ms> [iface]" >&2
                exit 4
                ;;
        esac
        if [ "$(id -u)" != "0" ]; then
            echo "netem-tier: setup needs root (sudo)" >&2
            exit 3
        fi
        tc qdisc replace dev "$iface" root netem delay "${delay_ms}ms"
        echo "netem-tier: armed ${delay_ms}ms on ${iface}"
        tc qdisc show dev "$iface"
        ;;
    teardown)
        iface="${2:-lo}"
        if [ "$(id -u)" != "0" ]; then
            echo "netem-tier: teardown needs root (sudo)" >&2
            exit 3
        fi
        tc qdisc del dev "$iface" root 2>/dev/null || true
        echo "netem-tier: disarmed on ${iface}"
        tc qdisc show dev "$iface"
        ;;
    status)
        iface="${2:-lo}"
        tc qdisc show dev "$iface"
        ;;
    *)
        echo "usage: netem-tier.sh {setup <delay_ms> [iface]|teardown [iface]|status [iface]}" >&2
        exit 4
        ;;
esac
