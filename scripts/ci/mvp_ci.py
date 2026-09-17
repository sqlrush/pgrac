#!/usr/bin/env python3
"""Select MVP smoke tests or verify exact-commit release CI evidence.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
EXACT_CHECKOUT_EVENTS = {"push", "workflow_dispatch", "schedule"}


def load_policy():
    policy = json.loads((ROOT / "scripts/ci/mvp-policy.json").read_text())
    if policy.get("schema_version") != 1:
        raise ValueError("unsupported MVP CI policy version")
    for workflow, names in policy["required_workflows"].items():
        if not (ROOT / ".github/workflows" / workflow).is_file():
            raise ValueError(f"missing required workflow: {workflow}")
        if not names or len(names) != len(set(names)):
            raise ValueError(f"empty or duplicate required jobs: {workflow}")
    smoke = policy["smoke_tests"]
    if not smoke or len(smoke) != len(set(smoke)):
        raise ValueError("empty or duplicate MVP smoke selection")
    if set(smoke) & set(policy["historical_fast_tests"]):
        raise ValueError("a smoke test cannot also be historical")
    for name in [*smoke, *policy["historical_fast_tests"]]:
        if not re.fullmatch(r"t/[0-9]{3}_[A-Za-z0-9_]+\.pl", name):
            raise ValueError(f"invalid TAP path: {name}")
        if not (ROOT / "src/test/cluster_tap" / name).is_file():
            raise ValueError(f"missing retained TAP test: {name}")
    if not (ROOT / ".github/workflows" / policy["historical_workflow"]).is_file():
        raise ValueError("missing historical workflow")
    return policy


def latest_run(runs, sha):
    # PR head_sha identifies the branch, but checkout normally tests a merge.
    exact = [run for run in runs if run.get("head_sha") == sha
             and run.get("event") in EXACT_CHECKOUT_EVENTS]
    if not exact:
        raise ValueError(f"no CI run for exact commit {sha}")
    return max(exact, key=lambda run: run["id"])


def validate_run(workflow, sha, run, jobs, required):
    if run.get("event") not in EXACT_CHECKOUT_EVENTS:
        raise ValueError(f"{workflow}: event does not prove exact-commit checkout")
    if run.get("head_sha") != sha or run.get("path") != f".github/workflows/{workflow}":
        raise ValueError(f"{workflow}: evidence identity mismatch")
    if run.get("status") != "completed" or run.get("conclusion") != "success":
        raise ValueError(f"{workflow}: latest exact-commit run is not successful")
    for name in required:
        matches = [job for job in jobs if job.get("name") == name]
        if len(matches) != 1:
            raise ValueError(f"{workflow}: expected exactly one executed job: {name}")
        job = matches[0]
        if job.get("status") != "completed" or job.get("conclusion") != "success":
            raise ValueError(f"{workflow}: required job did not pass: {name}")


def gh_json(endpoint, paginate=False):
    args = ["gh", "api", endpoint]
    if paginate:
        args += ["--paginate", "--slurp"]
    return json.loads(subprocess.run(args, check=True, capture_output=True, text=True).stdout)


def verify_release(repository, sha, policy):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository or ""):
        raise ValueError("--repository must be owner/repository")
    if not re.fullmatch(r"[0-9a-f]{40}", sha or ""):
        raise ValueError("--sha must be the full lowercase commit SHA")
    evidence = {"repository": repository, "sha": sha, "workflows": []}
    for workflow, required in policy["required_workflows"].items():
        runs = gh_json(
            f"repos/{repository}/actions/workflows/{workflow}/runs?head_sha={sha}&per_page=100"
        )["workflow_runs"]
        run = latest_run(runs, sha)
        pages = gh_json(
            f"repos/{repository}/actions/runs/{run['id']}/jobs?filter=latest&per_page=100",
            paginate=True,
        )
        jobs = [job for page in pages for job in page["jobs"]]
        validate_run(workflow, sha, run, jobs, required)
        evidence["workflows"].append({
            "workflow": workflow, "run_id": run["id"],
            "event": run["event"],
            "run_attempt": run["run_attempt"], "url": run["html_url"],
            "required_jobs": required,
        })
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--smoke", action="store_true")
    mode.add_argument("--verify-release", action="store_true")
    parser.add_argument("--repository")
    parser.add_argument("--sha")
    args = parser.parse_args()
    try:
        policy = load_policy()
        if args.smoke:
            print(" ".join(policy["smoke_tests"]))
        else:
            print(json.dumps(verify_release(args.repository, args.sha, policy), indent=2))
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as exc:
        print(f"MVP CI evidence refused: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
