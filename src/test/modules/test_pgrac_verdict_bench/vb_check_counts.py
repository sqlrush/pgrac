#!/usr/bin/env python3
"""Verify serial libc observations, including a real held-LWLock control."""
from __future__ import annotations

import sys

from vb_summarize import load


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def main() -> None:
    require(len(sys.argv) > 1, "usage: vb_check_counts.py results.jsonl [...]")
    normal: set[int] = set()
    control: set[int] = set()
    for row in load(sys.argv[1:]):
        if row.phase == "hot" and row.kind == "leaf_ordinary_committed":
            selected = normal
        elif row.phase == "control" and row.kind == "scan_lock_control":
            selected = control
        else:
            continue
        data = row.data
        counts = data.get("libc")
        require(isinstance(counts, list) and len(counts) == 16,
                f"tier {row.tier}: libc observer unavailable")
        expected = row.tier * data["iterations"]
        require(counts[0] == data["smgr_pread"] == expected,
                f"tier {row.tier}: pread coverage differs: {counts[0]}, "
                f"{data['smgr_pread']}, expected {expected}")
        require(counts[13] == expected and counts[15] == 0,
                f"tier {row.tier}: missing actual held-lock observations: {counts[13:16]}")
        if selected is control:
            require(counts[14] == expected,
                    f"tier {row.tier}: known shared lock was not observed")
        else:
            require(data["results"][2] == data["iterations"],
                    f"tier {row.tier}: ordinary committed leaf did not return bounds")
            print(f"tier={row.tier} calls={data['iterations']} pread={counts[0]} "
                  f"observed={counts[13]} held={counts[14]} unavailable={counts[15]}")
        selected.add(row.tier)
    require(normal == control == {1, 16, 64, 256},
            f"missing tiers: normal={normal}, positive_control={control}")
    print("PASS: four tiers, independent pread counts and real held-lock controls")


if __name__ == "__main__":
    main()
