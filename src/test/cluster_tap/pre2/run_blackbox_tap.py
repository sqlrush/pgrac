#!/usr/bin/env python3
"""Run each first-chain TAP separately, preserving every nonpassing result."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

CASES = ["431_pre2_start_stop_restart", "432_pre2_two_recoverers", "433_pre2_remote_x_scache"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--entry-file", type=Path)
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[4]
    build = args.build.resolve(strict=True)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    identity = dict(test_source_commit=subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip(),
        binary_sha256=hashlib.sha256((build / "install/bin/postgres").read_bytes()).hexdigest(),
        build_identity=json.loads((build / "identity.json").read_text())
        if (build / "identity.json").exists() else None,
        test_files={str(path.relative_to(source)): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in [source / "src/test/perl/PostgreSQL/Test/ClusterPRE2.pm",
                                 *Path(__file__).parent.glob("*.py"),
                                 Path(__file__).parent / "Scenario.pm",
                                 *(Path(__file__).parent.parent / "t" / (case + ".pl") for case in CASES)]})
    results = []
    for case in CASES:
        root = output / case
        (root / "log").mkdir(parents=True)
        (root / "data").mkdir()
        env = dict(os.environ, TESTLOGDIR=str(root / "log"), TESTDATADIR=str(root / "data"),
                   PG_TEST_NOCLEAN="1", PGRAC_PRE2_BINDIR=str(build / "install/bin"),
                   PG_REGRESS=str(build / "src/test/regress/pg_regress"), top_builddir=str(build),
                   PATH=str(build / "install/bin") + os.pathsep + os.environ["PATH"])
        for key in ("PGRAC_PRE2_TEST_ADAPTER", "PGRAC_PRE2_ENTRY_FILE"):
            env.pop(key, None)
        if args.entry_file:
            env["PGRAC_PRE2_ENTRY_FILE"] = str(args.entry_file.resolve(strict=True))
        with (root / "prove.log").open("wb") as log:
            result = subprocess.run(["prove", "-v", "-I", str(source / "src/test/perl"),
                                     str(source / "src/test/cluster_tap/t" / (case + ".pl"))],
                                    env=env, cwd=root, stdout=log, stderr=subprocess.STDOUT)
        text = (root / "prove.log").read_text()
        passed = result.returncode == 0 and "Result: PASS" in text and not re.search(r"#\s*skip\b", text, re.I)
        # Only this documented absent S11/S12 entry, detected BEFORE DB creation,
        # is automatically BLOCKED. Other failures retain FAIL for diagnosis.
        dependency = "A S11/S12: supported fresh PRE2 member initialization"
        blocked = not args.entry_file and f"# BLOCKED: {dependency}" in text
        record = dict(case=case, status="PASS" if passed else "BLOCKED" if blocked else "FAIL",
                      raw_tap="PASS" if passed else "FAIL", rc=result.returncode,
                      dependency=dependency if blocked else None,
                      configuration_sha256={str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                                            for name in ("postgresql.conf", "pgrac.conf")
                                            for p in (root / "data").rglob(name)},
                      log=str(root / "prove.log"), **identity)
        results.append(record)
        (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps({key: record[key] for key in ("case", "status", "raw_tap", "rc", "dependency")}))
    return 0 if all(result["status"] == "PASS" for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
