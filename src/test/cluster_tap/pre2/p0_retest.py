"""Prepare, run, or judge one targeted four-member regression capture.

This command uses an already qualified PRE2 entry file. It never provisions or
starts a VM. A successful SQL workload does not close the historical defect.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

from p0_retest_evidence import request_evidence, itl_evidence
from p0_retest_image_evidence import image_evidence
from p0_retest_hot_evidence import hot_evidence

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parents[3]
CASES = {
    "P0-5": "held snapshot, old creator, bounded remote transaction churn, old-row readback",
    "P0-8": "nine ordinary UPDATE transactions on one block, exact ITL wait and owner release",
    "P0-10": "ordinary remote snapshot SELECT after a remote committed UPDATE, all-block FULL control",
    "P0-12": "HOT, indexed-key UPDATE, DELETE and VACUUM under a held snapshot, tuple/FULL comparisons",
}


def source_commit():
    return subprocess.check_output(["git", "-C", str(SOURCE), "rev-parse", "HEAD"], text=True).strip()


def judge(case, capture, tap, logs):
    """Keep workload completion, original-path coverage and issue closure apart."""
    if case not in CASES:
        raise ValueError("unknown case")
    result = dict(case=case, status="NOT_REACHED", original_path="UNKNOWN",
                  p0_closed=False, errors=[], notes=[], request_evidence=request_evidence(logs))
    if case == "P0-10":
        result["image_evidence"] = image_evidence(capture, logs)
    if case == "P0-12":
        result["hot_evidence"] = hot_evidence(capture, logs)
    if capture.get("version") != 1 or capture.get("case") != case:
        result["notes"].append("missing or wrong case result")
        return result
    for node, text in enumerate(logs):
        for number, line in enumerate(text.splitlines(), 1):
            if re.search(r"\b(?:ERROR|FATAL|PANIC):|RECYCLED_AUTHORITY_UNPROVABLE|"
                         r"ordinary TT exchange refused|HISTORY_REFUSED", line):
                result["errors"].append(dict(node=node, line=number, text=line))
    checks = {
        "client_rc": type(capture.get("rc")) is int and capture["rc"] == 0,
        "sql_completed": capture.get("sql_completed") is True,
        "normal_stop": capture.get("normal_stop") is True,
        "client_cleanup": capture.get("cleanup_errors") == [],
        "four_members": capture.get("nodes") == [0, 1, 2, 3] and len(logs) == 4,
        "member_logs_retained": len(logs) == 4 and all(text.strip() for text in logs),
        "source_recorded": bool(re.fullmatch(r"[0-9a-f]{40}", capture.get("source_commit", ""))),
    }
    plans = re.findall(r"^1\.\.([0-9]+)\s*$", tap, re.M)
    assertions = re.findall(r"^ok\s+([0-9]+)\b", tap, re.M)
    checks["strict_tap"] = (len(plans) == 1 and int(plans[0]) > 0
                            and [int(n) for n in assertions] == list(range(1, int(plans[0]) + 1))
                            and not re.search(r"^\s*not ok\b|Bail out!|#\s*(?:skip|todo)\b", tap, re.I | re.M))
    result["checks"] = checks
    result["status"] = "WORKLOAD_PASS" if all(checks.values()) and not result["errors"] else "FAIL"
    result["notes"].append("Original failure-path identity/producer evidence requires separate review; silence is not coverage.")
    if case == "P0-8":
        result["notes"].append("Capacity wait coverage does not identify the historical UNKNOWN slot producer; VM-pair coverage is excluded.")
    return result


def prepare(case, output):
    output.mkdir(parents=True, exist_ok=False)
    script = HERE / (case.lower().replace("-", "_") + "_retest.py")
    plan = dict(version=1, case=case, scenario=CASES[case], source_commit=source_commit(),
                database_started=False, p0_closed=False,
                prerequisite="qualified normal four-member PRE2 cohort; queue/authorization outside this command",
                frozen_chain=case == "P0-5",
                workload_judgement="strict TAP, all SQL comparisons, four logs, client cleanup and normal stop",
                coverage_judgement="separate original-path/producer evidence; no automatic P0 closure")
    (output / "preparation.json").write_text(json.dumps(plan, indent=2) + "\n")
    command = ("#!/bin/sh\nset -eu\n"
               ': "${P0_BUILD:?set the independent candidate build directory}"\n'
               ': "${P0_ENTRY_FILE:?set the qualified live PRE2 entry file}"\n'
               ': "${P0_OUTPUT:?set a new result directory}"\n'
               f'exec {shlex.quote(sys.executable)} {shlex.quote(str(script))} run '
               '--build "$P0_BUILD" --entry-file "$P0_ENTRY_FILE" --output "$P0_OUTPUT"\n')
    (output / "run.sh").write_text(command)
    (output / "run.sh").chmod(0o755)
    return plan


def judge_directory(case, output):
    result_file = output / "log/p0-result.json"
    capture = json.loads(result_file.read_text()) if result_file.exists() else {}
    tap = (output / "tap.log").read_text(errors="replace")
    # Keep node ordinals even on a partial export. Empty/missing bytes fail the
    # member_logs_retained check; no replacement log file is written.
    logs = [(output / f"log/node{n}.log").read_text(errors="replace")
            if (output / f"log/node{n}.log").is_file() else "" for n in range(4)]
    result = judge(case, capture, tap, logs)
    native_copy = output / "log/m2-b07-candidate-latest.json"
    if case == "P0-8" and native_copy.is_file():
        try:
            document = json.loads(native_copy.read_text())
            if not isinstance(document, dict):
                raise ValueError("native copy export must be an object")
            result["itl_evidence"] = itl_evidence(document.get("trace"))
        except (ValueError, OSError) as error:
            result["itl_evidence"] = dict(itl_evidence(None), error=str(error))
    (output / "judgement.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def run_case(case, args):
    build = args.build.resolve(strict=True)
    entry = args.entry_file.resolve(strict=True)
    for path in (build / "install/bin/postgres", build / "install/bin/psql",
                 build / "src/test/regress/pg_regress"):
        if not path.is_file():
            raise ValueError("missing build prerequisite: " + str(path))
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / "log").mkdir()
    (output / "data").mkdir()
    commit = source_commit()
    env = dict(os.environ, TESTLOGDIR=str(output / "log"), TESTDATADIR=str(output / "data"),
               PG_TEST_NOCLEAN="1", PGRAC_PRE2_BINDIR=str(build / "install/bin"),
               PG_REGRESS=str(build / "src/test/regress/pg_regress"), top_builddir=str(build),
               PATH=str(build / "install/bin") + os.pathsep + os.environ["PATH"],
               PGRAC_PRE2_ENTRY_FILE=str(entry), PGRAC_P0_RETEST_CASE=case,
               PGRAC_P0_SOURCE_COMMIT=commit)
    env.pop("PGRAC_PRE2_TEST_ADAPTER", None)
    tap = SOURCE / "src/test/cluster_tap/t/483_pre2_p0_retest.pl"
    with (output / "tap.log").open("wb") as log:
        completed = subprocess.run(["prove", "-v", "-I", str(SOURCE / "src/test/perl"), str(tap)],
                                   env=env, cwd=output, stdout=log, stderr=subprocess.STDOUT)
    result_file = output / "log/p0-result.json"
    capture = json.loads(result_file.read_text()) if result_file.exists() else dict(version=1, case=case)
    capture.update(rc=completed.returncode, source_commit=commit)
    result_file.write_text(json.dumps(capture, indent=2) + "\n")
    return judge_directory(case, output)


def main(case=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "run", "judge"))
    if case is None:
        parser.add_argument("--case", choices=CASES, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--build", type=Path)
    parser.add_argument("--entry-file", type=Path)
    args = parser.parse_args()
    case = case or args.case
    if args.action == "run" and (not args.build or not args.entry_file):
        parser.error("run requires --build and --entry-file")
    try:
        result = (prepare(case, args.output) if args.action == "prepare" else
                  run_case(case, args) if args.action == "run" else judge_directory(case, args.output))
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2))
    return 0 if args.action == "prepare" or result["status"] == "WORKLOAD_PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
