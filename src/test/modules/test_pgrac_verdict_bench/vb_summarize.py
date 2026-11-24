#!/usr/bin/env python3
# -------------------------------------------------------------------------
#
# vb_summarize.py
#     Summarize one or more run_bench.sh result files.
#
#     For every (pool size, kind) it reports the median over warm rounds of
#     each round's p50 / p99 / max, the spread of the p50s, the implied
#     serial calls per second, and the per-call I/O counts.  Cold cycles,
#     the libc-level counters and the concurrent phases are summarized
#     when present.
#
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#     src/test/modules/test_pgrac_verdict_bench/vb_summarize.py
#
# NOTES
#     Usage: vb_summarize.py results-<tag>.jsonl [...]
#
# -------------------------------------------------------------------------
from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Sequence

LIBC_SLOTS = 16


@dataclass(frozen=True)
class Row:
    """One result line: its run coordinates and the JSON payload."""

    tier: int
    phase: str
    round: str
    kind: str
    data: dict[str, Any]
    source: str = ""


def emit(text: str = "") -> None:
    sys.stdout.write(text + "\n")


def load(paths: Sequence[str]) -> list[Row]:
    rows: list[Row] = []
    for path in paths:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or " {" not in line:
                    continue
                head, body = line.split(" {", 1)
                fields = dict(part.split("=", 1) for part in head.split())
                rows.append(
                    Row(
                        tier=int(fields["tier"]),
                        phase=fields["phase"],
                        round=fields["round"],
                        kind=fields["kind"],
                        data=json.loads("{" + body),
                        source=path,
                    )
                )
    return rows


def us(nanoseconds: float) -> float:
    return nanoseconds / 1000.0


def median(values: Sequence[float]) -> float:
    return statistics.median(values) if values else float("nan")


def warm(rows: Sequence[Row]) -> None:
    groups: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.phase == "hot":
            groups[(row.tier, row.kind)].append(row.data)
    emit("== warm (median over rounds; microseconds) ==")
    emit(
        "segs kind                      rounds p50     p50_min p50_max p99     max     "
        "mean    calls/s  pread/call open/call close/call results"
    )
    for (tier, kind), data in sorted(groups.items()):
        p50 = [us(d["p50_ns"]) for d in data]
        p99 = [us(d["p99_ns"]) for d in data]
        peak = [us(d["max_ns"]) for d in data]
        mean = [us(d["mean_ns"]) for d in data]
        calls = sum(d["iterations"] for d in data)
        total = sum(d["total_ns"] for d in data)
        results = [sum(d["results"][i] for d in data) for i in range(len(data[0]["results"]))]
        emit(
            f"{tier:4d} {kind:25s} {len(data):6d} {median(p50):7.1f} {min(p50):7.1f} "
            f"{max(p50):7.1f} {median(p99):7.1f} {max(peak):7.1f} {median(mean):7.1f} "
            f"{calls / (total / 1e9):8.0f} "
            f"{sum(d['smgr_pread'] for d in data) / calls:10.2f} "
            f"{sum(d['smgr_open'] for d in data) / calls:9.2f} "
            f"{sum(d['smgr_close'] for d in data) / calls:10.2f} {results}"
        )


def cold(rows: Sequence[Row]) -> None:
    first: dict[int, list[Row]] = defaultdict(list)
    again: dict[tuple[str, int, str], dict[str, Any]] = {}
    evicted: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in rows:
        if row.phase == "cold":
            first[row.tier].append(row)
        elif row.phase == "recheck":
            again[(row.source, row.tier, row.round)] = row.data
        elif row.phase == "evict":
            evicted[(row.source, row.tier, row.round)] = row.data
    if not first:
        return
    emit("== cold (verified header eviction only; microseconds) ==")
    emit(
        "segs cycles verified excluded cold_p50 cold_min cold_max disk_bytes/call "
        "ctx_switch/call warm_again_p50"
    )
    for tier, candidates in sorted(first.items()):
        eligible = []
        for row in candidates:
            eviction = evicted.get((row.source, tier, row.round), {})
            if (eviction.get("failed") == 0 and eviction.get("files") == tier
                    and eviction.get("resident_before") == tier
                    and eviction.get("resident_after") == 0):
                eligible.append(row)
        if not eligible:
            emit(f"{tier:4d} {len(candidates):6d} {0:8d} {len(candidates):8d} n/a")
            continue
        data = [row.data for row in eligible]
        cost = [us(d["total_ns"]) for d in data]
        warm_again = [us(again[(row.source, tier, row.round)]["total_ns"]) for row in eligible
                      if (row.source, tier, row.round) in again]
        disk = [d["disk_bytes_read"] for d in data if d["disk_bytes_read"] is not None]
        emit(
            f"{tier:4d} {len(candidates):6d} {len(data):8d} "
            f"{len(candidates) - len(data):8d} {median(cost):8.1f} {min(cost):8.1f} "
            f"{max(cost):8.1f} {median(disk):15.0f} "
            f"{median([d['ru_nvcsw'] for d in data]):15.0f} {median(warm_again):14.1f}"
        )


def libc(rows: Sequence[Row]) -> None:
    groups: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.phase == "hot" and row.data.get("libc"):
            groups[(row.tier, row.kind)].append(row.data)
    if not groups:
        return
    emit("== libc-level counters per call (interposed runs) ==")
    emit(
        "segs kind                      pread  bytes    open_ok open_fail close  access "
        "| ns/pread ns/open_ok ns/open_fail ns/close ns/access | syscall_us/call"
    )
    for (tier, kind), data in sorted(groups.items()):
        calls = sum(d["iterations"] for d in data)
        total = [sum(d["libc"][i] for d in data) for i in range(LIBC_SLOTS)]
        each = [
            total[time_slot] / total[count_slot] if total[count_slot] else 0.0
            for time_slot, count_slot in ((8, 0), (9, 2), (10, 3), (11, 4), (12, 5))
        ]
        emit(
            f"{tier:4d} {kind:25s} {total[0] / calls:6.1f} {total[1] / calls:8.0f} "
            f"{total[2] / calls:7.1f} {total[3] / calls:9.1f} {total[4] / calls:6.1f} "
            f"{total[5] / calls:6.1f} | {each[0]:8.0f} {each[1]:10.0f} {each[2]:12.0f} "
            f"{each[3]:8.0f} {each[4]:9.0f} | {sum(total[8:13]) / calls / 1000.0:10.1f}"
        )
        emit(f"     undo pread LWLock observations: inspected={total[13]} "
             f"held={total[14]} unavailable={total[15]}")


def concurrent(rows: Sequence[Row]) -> None:
    commits: dict[str, list[float]] = defaultdict(list)
    loops: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.phase == "commit":
            commits[row.kind.rsplit("_r", 1)[0]].append(row.data["tps"])
        elif row.phase.startswith(("parallel", "mixed")):
            loops[row.phase].append(row.data)
    if commits:
        emit("== side workload (tps) ==")
        for kind, tps in sorted(commits.items()):
            emit(
                f"{kind:44s} median {median(tps):10.0f}  min {min(tps):10.0f}  "
                f"max {max(tps):10.0f}  n={len(tps)}"
            )
    if loops:
        emit("== concurrent verdict loops (microseconds per call, per process) ==")
        for phase, data in sorted(loops.items()):
            p50 = [us(d["p50_ns"]) for d in data]
            p99 = [us(d["p99_ns"]) for d in data]
            rate = [d["iterations"] / (d["total_ns"] / 1e9) for d in data]
            emit(
                f"{phase:24s} processes*rounds={len(data):3d} p50 median {median(p50):8.1f} "
                f"p99 median {median(p99):9.1f} max {max(us(d['max_ns']) for d in data):9.1f} "
                f"calls/s per process {median(rate):7.1f}"
            )


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit("usage: vb_summarize.py results.jsonl [...]")
    rows = load(sys.argv[1:])
    for row in rows:
        if row.phase == "env":
            emit(f"load at segs={row.tier}: {row.data['uptime']}")
    warm(rows)
    cold(rows)
    libc(rows)
    concurrent(rows)


if __name__ == "__main__":
    main()
