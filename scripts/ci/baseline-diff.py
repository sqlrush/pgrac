#!/usr/bin/env python3
# -----------------------------------------------------------------------
#
# baseline-diff.py
#    Compare the current cppcheck XML output against the stage-0
#    baseline (currently 0 findings, established at spec-0.27.5 §6).
#
#    Spec:    pgrac/specs/spec-0.30-stage0-acceptance.md §2.3
#    Design:  pgrac/docs/ci-static-analysis.md (strict-mode contract)
#    Baseline: pgrac/docs/ci-static-analysis-baseline-stage0.27.md
#
#    Usage (from CI Security job):
#        python3 scripts/ci/baseline-diff.py \
#            --baseline pgrac/docs/ci-static-analysis-baseline-stage0.27.md \
#            --current cppcheck.xml
#
#    Stage 0.30 simplification: the stage-0 baseline is empty (0 findings
#    after spec-0.27.5 §3.2 suppression sweep), so any new finding in
#    cppcheck.xml is a regression.  We compare current findings as
#    (file, line, rule, message) tuples; the --baseline argument exists
#    for forward compatibility (stages 1+ may have non-empty baselines)
#    but is currently only used for documentation purposes -- the
#    baseline file is treated as "0 findings" regardless of contents.
#
#    Stage 1+ extension: parse the baseline markdown's "Reported" tables
#    to extract the known-finding tuple set; subtract from current; only
#    report the residual as new findings.
#
# IDENTIFICATION
#    scripts/ci/baseline-diff.py
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    Exit codes:
#      0  no new findings (current matches baseline)
#      1  new findings detected (CI step fails)
#      2  argument or parse error
#
# -----------------------------------------------------------------------

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def parse_cppcheck_xml(path: Path) -> list[tuple[str, int, str, str]]:
    """Return a list of (file, line, rule_id, message) tuples."""
    if not path.exists():
        print(f"ERROR: cppcheck XML not found: {path}", file=sys.stderr)
        sys.exit(2)

    try:
        tree = ET.parse(path)
    except ET.ParseError as exc:
        print(f"ERROR: malformed cppcheck XML: {exc}", file=sys.stderr)
        sys.exit(2)

    findings = []
    for err in tree.getroot().findall(".//error"):
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
        findings.append((file_path, line_num, rule_id, msg))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare cppcheck output against the stage-0 baseline.")
    parser.add_argument("--baseline", required=True, type=Path,
                        help="Path to baseline markdown (forward compat).")
    parser.add_argument("--current", required=True, type=Path,
                        help="Path to current cppcheck.xml.")
    args = parser.parse_args()

    if not args.baseline.exists():
        print(f"WARNING: baseline file not found: {args.baseline}",
              file=sys.stderr)
        print("Stage 0.30: treating as 0-findings baseline regardless.",
              file=sys.stderr)

    findings = parse_cppcheck_xml(args.current)

    if not findings:
        print("baseline-diff: 0 cppcheck findings (matches baseline).")
        return 0

    # Stage 0.30 baseline = 0 findings; any current finding is a regression.
    print("baseline-diff: NEW cppcheck findings detected (regression):")
    print("=" * 70)
    for file_path, line_num, rule_id, msg in findings:
        print(f"  {file_path}:{line_num}  [{rule_id}]  {msg}")
    print("=" * 70)
    print(f"Total new findings: {len(findings)}")
    print()
    print("If these are intentional, suppress them in")
    print("scripts/ci/cppcheck-suppressions.txt (with Reason: comments)")
    print("AND update the run-cppcheck.sh CLI flags to match.")
    print("Then update pgrac:docs/ci-static-analysis-baseline-stage*.md")
    print("with the new baseline.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
