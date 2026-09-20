#!/usr/bin/env python3
"""Publish only after protected CI succeeds. Author: SqlRush <sqlrush@gmail.com>"""

import json
from pathlib import Path
import sys

REQUIRED = (
    "Validate (comment headers + format + tidy + commit msg)",
    "Linux enable-cluster (unit + regress + smoke TAP)",
    "Linux --disable-cluster regression",
    "macOS build-only (compile guard)",
    "Security (cppcheck + scan-build)",
)


def failures(checks):
    latest = {}
    for check in sorted(checks, key=lambda item: item.get("started_at") or ""):
        latest[check["name"]] = check.get("conclusion")
    return [name for name in REQUIRED if latest.get(name) != "success"]


if __name__ == "__main__":
    pending = failures([json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines()])
    if pending:
        sys.exit("Image publication blocked by: " + ", ".join(pending))
    print("All protected checks succeeded on the queried commit.")
