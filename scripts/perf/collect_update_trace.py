#!/usr/bin/env python3
"""Normalize ``pg_cluster_state`` update-trace dumps to JSONL.

Author: SqlRush <sqlrush@gmail.com>

The server-side diagnostic surface is intentionally a stable category/key/value
row.  This adapter keeps the raw dump untouched and emits one numeric record
per UPDATE execution, so the latency analyzer can be used without a running
server. Legacy storage-attempt dumps remain readable, but must not be mixed
with whole-execution captures. Incomplete/error operations are preserved.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List


_FIELD_RE = re.compile(r"(?P<key>[^=;]+)=(?P<value>[^;]*)")
_TM_RESULTS = {
    0: ("result_tm_ok", "TM_Ok"),
    1: ("result_tm_invisible", "TM_Invisible"),
    2: ("result_tm_self_modified", "TM_SelfModified"),
    3: ("retry_tm_updated", "TM_Updated"),
    4: ("result_tm_deleted", "TM_Deleted"),
    5: ("result_tm_being_modified", "TM_BeingModified"),
    6: ("result_tm_would_block", "TM_WouldBlock"),
}


def _parse_fields(value: str) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    consumed = 0
    for match in _FIELD_RE.finditer(value):
        if match.start() != consumed and value[consumed:match.start()] not in ("", ";"):
            raise ValueError(f"malformed trace value near {value[consumed:match.start()]!r}")
        fields[match.group("key")] = match.group("value")
        consumed = match.end()
    if consumed != len(value):
        raise ValueError(f"malformed trace value suffix {value[consumed:]!r}")
    return fields


def _uint(fields: Dict[str, str], key: str) -> int:
    raw = fields.get(key)
    if raw is None:
        raise ValueError(f"trace row missing {key}")
    try:
        value = int(raw, 10)
    except ValueError as exc:
        raise ValueError(f"trace field {key} is not an integer: {raw!r}") from exc
    if value < 0:
        raise ValueError(f"trace field {key} is negative")
    return value


def _status(raw: int) -> tuple[str, str | None]:
    if raw == 0:
        return "ok", None
    if raw == 1:
        return "error", None
    if raw == 2:
        return "incomplete", None
    if raw >= 100:
        result = raw - 100
        return _TM_RESULTS.get(result, (f"result_{result}", None))
    return f"status_{raw}", None


def _record(row: Dict[str, str], node_hint: int | None, ordinal: int) -> Dict[str, object]:
    raw_op = _uint(row, "op_id")
    node_id = _uint(row, "node") if "node" in row else node_hint
    if node_id is None:
        raise ValueError("trace row has no node id")
    raw_status = _uint(row, "status")
    status, result = _status(raw_status)
    start_ns = _uint(row, "start_ns")
    end_ns = _uint(row, "end_ns")
    if end_ns < start_ns and status != "incomplete":
        raise ValueError("trace end_ns precedes start_ns")

    phases: Dict[str, int] = {}
    phase_events: Dict[str, int] = {}
    for key, raw in row.items():
        if not key.startswith("phase."):
            continue
        phase = key[6:]
        pieces = raw.split(",")
        if len(pieces) != 2:
            raise ValueError(f"phase {phase} must be nanos,event_count")
        try:
            nanos, events = (int(piece, 10) for piece in pieces)
        except ValueError as exc:
            raise ValueError(f"phase {phase} has non-numeric value {raw!r}") from exc
        if nanos < 0 or events < 0:
            raise ValueError(f"phase {phase} is negative")
        phases[phase] = nanos
        phase_events[phase] = events

    # op_id is per node in the finite capture. Keep a stable numeric id for the
    # analyzer while retaining the original id for auditability.
    op_id = (int(node_id) << 32) | raw_op
    record: Dict[str, object] = {
        "op_id": op_id,
        "source_op_id": raw_op,
        "node_id": int(node_id),
        "backend_id": _uint(row, "backend"),
        "pid": _uint(row, "pid"),
        "start_ns": start_ns,
        "end_ns": end_ns,
        "total_ns": end_ns - start_ns if status != "incomplete" else 0,
        "status": status,
        "raw_status": raw_status,
        "phases": phases,
        "phase_events": phase_events,
        "ordinal": ordinal,
    }
    if result is not None:
        record["result"] = result
    for key in ("backend_sequence", "affected_rows", "unattributed_ns", "accounting_errors"):
        if key in row:
            record[key] = _uint(row, key)
    if "unattributed_ns" in row:
        record["exclusive_phases"] = {
            key[10:]: _uint(row, key) for key in row if key.startswith("exclusive.")
        }
    return record


def load_dump(path: Path) -> List[Dict[str, object]]:
    """Load one JSON array emitted by ``pg_cluster_state``."""
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path}: invalid JSON: {exc}") from exc
    if not isinstance(rows, list):
        raise ValueError(f"{path}: expected a JSON array")

    records: List[Dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict) or row.get("category") != "update_trace":
            continue
        key = row.get("key")
        value = row.get("value")
        if not isinstance(key, str) or not key.startswith("op."):
            continue
        if not isinstance(value, str):
            raise ValueError(f"{path}: {key} has non-string value")
        fields = _parse_fields(value)
        try:
            raw_op = int(key[3:], 10)
        except ValueError as exc:
            raise ValueError(f"{path}: invalid op key {key!r}") from exc
        fields["op_id"] = str(raw_op)
        node_hint = None
        match = re.search(r"node=(\d+)(?:;|$)", value)
        if match:
            node_hint = int(match.group(1), 10)
        records.append(_record(fields, node_hint, len(records)))
    return records


def collect(paths: Iterable[Path]) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for path in paths:
        records.extend(load_dump(path))
    return sorted(records, key=lambda row: (int(row["start_ns"]), int(row["op_id"])))


def collect_window(before_paths: Iterable[Path], after_paths: Iterable[Path]) -> List[Dict[str, object]]:
    """Keep new records on a running instance; never reset or overwrite capture.

    The boundary must be idle. Prior immutable records must still be present
    and identical, otherwise a restart or capture corruption has broken the
    window identity. Errors inside the new window remain in the result.
    """
    before = collect(before_paths)
    after = collect(after_paths)
    previous = {r['op_id']: r for r in before}
    current = {r['op_id']: r for r in after}
    if len(previous) != len(before) or len(current) != len(after):
        raise ValueError('duplicate capture identity')
    for op_id, row in previous.items():
        if row['status'] == 'incomplete':
            raise ValueError('incomplete operation at window start')
        # ordinal is only the parser position, not producer identity.
        old = {k: v for k, v in row.items() if k != 'ordinal'}
        new = {k: v for k, v in current.get(op_id, {}).items() if k != 'ordinal'}
        if old != new:
            raise ValueError('prior capture changed or disappeared')
    return [r for r in after if r['op_id'] not in previous]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dumps", nargs="+", type=Path, help="pg_cluster_state JSON dumps")
    parser.add_argument("-o", "--output", type=Path, required=True, help="normalized JSONL output")
    args = parser.parse_args()
    records = collect(args.dumps)
    with args.output.open("w", encoding="utf-8") as output:
        for record in records:
            output.write(json.dumps(record, sort_keys=True) + "\n")
    print(json.dumps({"records": len(records), "output": str(args.output)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
