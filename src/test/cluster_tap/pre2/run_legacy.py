#!/usr/bin/env python3
"""Run each clean-leave TAP separately; no bailout or skip counts as PASS."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess

CASES = {
    "310": "310_cluster_5_13_clean_leave.pl",
    "313": "313_cluster_5_13_clean_leave_concurrent.pl",
    "314": "314_cluster_5_13_clean_leave_preflight_3node.pl",
    "316": "316_cluster_5_13_clean_leave_preflight_failclosed.pl",
    "324": "324_cluster_5_13_clean_leave_overlap.pl",
}


def classify(rc, tap, server_logs):
    # A known error name or skip does not prove an unavailable dependency.
    # Report failures here; any BLOCKED adjudication needs separate evidence.
    skipped = bool(re.search(r"#\s*skip\b", tap, re.IGNORECASE))
    failed_assertion = bool(re.search(r"^\s*not ok\s+\d+", tap + "\n" + server_logs, re.MULTILINE))
    diagnostic = next((line for line in server_logs.splitlines() if "FATAL:" in line), "")
    if rc == 0 and "Result: PASS" in tap and not skipped and not failed_assertion:
        return "PASS", ""
    return "FAIL", diagnostic or ("TAP skipped a required scenario" if skipped else "")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[4]
    build = args.build.resolve(strict=True)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    binary = build / "install/bin/postgres"
    identity = {
        "source": subprocess.check_output(
            ["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "note": "source HEAD identifies the worktree; binary SHA identifies the installed build",
    }
    results = []
    for number, name in CASES.items():
        root = output / number
        (root / "log").mkdir(parents=True)
        (root / "data").mkdir()
        env = dict(os.environ, TESTLOGDIR=str(root / "log"),
                   TESTDATADIR=str(root / "data"), PGPORT="65432",
                   PG_REGRESS=str(build / "src/test/regress/pg_regress"),
                   top_builddir=str(build),
                   PATH=str(build / "install/bin") + os.pathsep + os.environ["PATH"])
        log = root / "prove.log"
        with log.open("wb") as stream:
            process = subprocess.Popen(
                ["prove", "-v", "-I", str(source / "src/test/perl"),
                 str(source / "src/test/cluster_tap/t" / name)],
                env=env, cwd=root, stdout=stream, stderr=subprocess.STDOUT,
                start_new_session=True)
            try:
                rc = process.wait(timeout=1800)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                rc = 124
        text = log.read_text(errors="replace")
        logs = "\n".join(p.read_text(errors="replace") for p in (root / "log").glob("*.log"))
        status, blocker = classify(rc, text, logs)
        record = dict(case=number, status=status, rc=rc, dependency=blocker,
                      log=str(log), config_sha256={str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                                                  for p in (root / "data").rglob("postgresql.conf")}, **identity)
        results.append(record)
        print(json.dumps(record), flush=True)
        (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    return 0 if all(r["status"] == "PASS" for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
