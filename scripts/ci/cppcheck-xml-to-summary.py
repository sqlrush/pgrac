#!/usr/bin/env python3
# -----------------------------------------------------------------------
#
# cppcheck-xml-to-summary.py
#    Convert cppcheck XML v2 output into a human-readable summary.
#
#    Spec:   pgrac/specs/spec-0.27.5-static-analysis.md
#    Design: pgrac/docs/ci-static-analysis.md
#
#    Reads cppcheck XML on argv[1], writes summary to stdout.  Counts
#    findings by severity and by file, surfaces top 10 issues, and
#    keeps output compact enough for CI logs (~30 lines).
#
# IDENTIFICATION
#    scripts/ci/cppcheck-xml-to-summary.py
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    Exit codes:
#      0  - summary printed (regardless of finding count)
#      1  - input file missing or malformed
#
# -----------------------------------------------------------------------

import sys
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: cppcheck-xml-to-summary.py <cppcheck.xml>",
              file=sys.stderr)
        return 1

    xml_path = Path(sys.argv[1])
    if not xml_path.exists():
        print(f"ERROR: file not found: {xml_path}", file=sys.stderr)
        return 1

    try:
        tree = ET.parse(xml_path)
    except ET.ParseError as exc:
        print(f"ERROR: malformed XML: {exc}", file=sys.stderr)
        return 1

    root = tree.getroot()
    errors = root.findall(".//error")

    if not errors:
        print("cppcheck summary (file: %s)" % xml_path)
        print("=" * 50)
        print("Total warnings: 0")
        print("(no findings)")
        return 0

    severity_counter: Counter[str] = Counter()
    file_counter: Counter[str] = Counter()
    findings: list[tuple[str, int, str, str, str]] = []

    for err in errors:
        severity = err.get("severity", "unknown")
        rule_id = err.get("id", "unknown")
        msg = err.get("msg", "")
        loc = err.find("location")
        if loc is None:
            file_path = "(unknown)"
            line_num = 0
        else:
            file_path = loc.get("file", "(unknown)")
            try:
                line_num = int(loc.get("line", "0"))
            except ValueError:
                line_num = 0

        severity_counter[severity] += 1
        file_counter[file_path] += 1
        findings.append((file_path, line_num, severity, rule_id, msg))

    print("cppcheck summary (file: %s)" % xml_path)
    print("=" * 50)
    print("Total warnings: %d" % len(errors))
    print()

    print("By severity:")
    for sev, count in severity_counter.most_common():
        print("  %-12s %d" % (sev + ":", count))
    print()

    print("By file (top 10):")
    for file_path, count in file_counter.most_common(10):
        print("  %-50s %d" % (file_path, count))
    print()

    print("Top issues (first 10):")
    for i, (file_path, line_num, severity, rule_id, msg) in enumerate(
            findings[:10], start=1):
        print("%2d. %s:%d (%s/%s) %s" % (
            i, file_path, line_num, severity, rule_id, msg))

    return 0


if __name__ == "__main__":
    sys.exit(main())
