#!/usr/bin/env python3
"""Analyze PGRAC per-update diagnostic records.

Author: SqlRush <sqlrush@gmail.com>

Input is JSON Lines.  A record has the following stable fields::

    {"op_id": 17, "start_ns": 100, "end_ns": 200,
     "total_ns": 100, "status": "ok",
     "phases": {"r_gcs_s_request": 31}}

``total_ns`` is optional when start/end are present.  Phase values are
inclusive timer sums. ``exclusive_phases`` subtracts nested intervals;
exclusive sums plus ``unattributed_ns`` must conserve operation wall time.
The inclusive ranking is a call-cost view, never an additive time budget.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List


def _number(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    if value < 0 or not math.isfinite(float(value)):
        raise ValueError(f"{field} must be a finite non-negative number")
    return int(value)


def _normalize(record: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(record, dict):
        raise ValueError("trace record must be an object")
    if "op_id" not in record:
        raise ValueError("trace record is missing op_id")

    op_id = _number(record["op_id"], "op_id")
    status = record.get("status", "ok")
    if not isinstance(status, str) or not status:
        raise ValueError("status must be a non-empty string")

    if "total_ns" in record:
        total_ns = _number(record["total_ns"], "total_ns")
    elif "start_ns" in record and "end_ns" in record:
        start_ns = _number(record["start_ns"], "start_ns")
        end_ns = _number(record["end_ns"], "end_ns")
        if end_ns < start_ns:
            raise ValueError("end_ns precedes start_ns")
        total_ns = end_ns - start_ns
    else:
        raise ValueError("trace record needs total_ns or start_ns/end_ns")

    phases = record.get("phases", {})
    if not isinstance(phases, dict):
        raise ValueError("phases must be an object")
    normalized_phases = {}
    for name, value in phases.items():
        if not isinstance(name, str) or not name:
            raise ValueError("phase names must be non-empty strings")
        normalized_phases[name] = _number(value, f"phases.{name}")

    normalized = dict(record)
    normalized.update({"op_id": op_id, "status": status,
                       "total_ns": total_ns, "phases": normalized_phases})
    normalized["accounting_errors"] = _number(record.get("accounting_errors", 0), "accounting_errors")
    if "exclusive_phases" in record:
        if not isinstance(record["exclusive_phases"], dict):
            raise ValueError("exclusive_phases must be an object")
        normalized["exclusive_phases"] = {
            name: _number(value, f"exclusive_phases.{name}")
            for name, value in record["exclusive_phases"].items()
        }
        normalized["unattributed_ns"] = _number(record.get("unattributed_ns", 0), "unattributed_ns")
        if (status == "ok" and normalized["accounting_errors"] == 0
                and sum(normalized["exclusive_phases"].values()) + normalized["unattributed_ns"] != total_ns):
            raise ValueError("exclusive phase conservation failed")
    return normalized


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with path.open(encoding="utf-8") as source:
        for line_no, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"line {line_no}: invalid JSON: {exc}") from exc
            try:
                records.append(_normalize(raw))
            except ValueError as exc:
                raise ValueError(f"line {line_no}: {exc}") from exc
    return records


def _phase_rank(records: Iterable[Dict[str, Any]], wall_ns: int,
                field: str = "phases") -> List[Dict[str, Any]]:
    totals: Counter[str] = Counter()
    events: Counter[str] = Counter()
    for record in records:
        for phase, nanos in record[field].items():
            totals[phase] += nanos
            events[phase] += record.get("phase_events", {}).get(phase, 1)
    result = []
    for phase, total in sorted(totals.items(), key=lambda item: (-item[1], item[0])):
        result.append({
            "phase": phase,
            "total_ns": total,
            "events": events[phase],
            "mean_ns": total / events[phase] if events[phase] else None,
            "share_of_completed_wall": total / wall_ns if wall_ns else 0.0,
        })
    return result


def analyze_records(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    normalized = [_normalize(record) for record in records]
    status_counts = dict(sorted(Counter(record["status"] for record in normalized).items()))
    completed = [record for record in normalized if record["status"] == "ok"]
    totals = sorted(record["total_ns"] for record in completed)

    if totals:
        p80_index = max(0, math.ceil(len(totals) * 0.80) - 1)
        p80_total_ns = totals[p80_index]
        tail_count = max(1, math.ceil(len(totals) * 0.20))
        tail = sorted(completed, key=lambda record: record["total_ns"])[-tail_count:]
    else:
        p80_total_ns = None
        tail = []

    completed_wall_ns = sum(record["total_ns"] for record in completed)
    tail_wall_ns = sum(record["total_ns"] for record in tail)
    phase_rank = _phase_rank(completed, completed_wall_ns)
    tail_phase_rank = _phase_rank(tail, tail_wall_ns)
    accounted = [r for r in completed if "exclusive_phases" in r and r["accounting_errors"] == 0]
    tail_accounted = [r for r in tail if "exclusive_phases" in r and r["accounting_errors"] == 0]
    accounted_wall = sum(r["total_ns"] for r in accounted)
    tail_accounted_wall = sum(r["total_ns"] for r in tail_accounted)

    return {
        "records_seen": len(normalized),
        "completed_ops": len(completed),
        "non_completed_ops": len(normalized) - len(completed),
        "status_counts": status_counts,
        "total_wall_ns": completed_wall_ns,
        "p80_total_ns": p80_total_ns,
        "tail_ops": len(tail),
        "tail_wall_ns": tail_wall_ns,
        "tail_wall_share": tail_wall_ns / completed_wall_ns if completed_wall_ns else 0.0,
        "phase_rank": phase_rank,
        "tail_phase_rank": tail_phase_rank,
        "latency_basis": "server_executor",
        "accounted_ops": len(accounted),
        "accounting_fault_ops": sum(r["accounting_errors"] != 0 for r in normalized),
        "accounted_wall_ns": accounted_wall,
        "unattributed_ns": sum(r["unattributed_ns"] for r in accounted),
        "exclusive_phase_rank": _phase_rank(accounted, accounted_wall, "exclusive_phases"),
        "tail_exclusive_phase_rank": _phase_rank(tail_accounted, tail_accounted_wall, "exclusive_phases"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="JSONL trace file")
    parser.add_argument("-o", "--output", type=Path, help="write report JSON here")
    args = parser.parse_args()

    report = analyze_records(load_jsonl(args.input))
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
