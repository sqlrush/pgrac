# scripts/ci/ — CI helper scripts

> Author: SqlRush \<sqlrush@gmail.com\>

CI/CD helpers for the current MVP gates in `fast.yml` and `nightly.yml`.
Historical extended qualification remains in `legacy-extended.yml`.

## Current MVP release gate

The release policy is [mvp-policy.json](mvp-policy.json). Both workflows must
pass on the **exact release commit**, with every required job actually run:

| Workflow | Required coverage |
|---|---|
| `fast.yml` | Validate; strict cppcheck and completed Clang analysis; enabled Linux build, all registered C units, 13 cluster SQL tests and MVP TAP smoke; disabled Linux build and 219 native PG tests; macOS build and all registered C units |
| `nightly.yml` | Enabled Linux 219 native PG tests; macOS 219 native PG tests, all registered C units, 13 cluster SQL tests and the same MVP TAP smoke |

The six smoke files are `010`, `030`, `031`, `050`, `200`, and `332`.
They cover views, acceptance observability, safe undo-publication refusal,
shared-storage initialization, basic cluster capability and single-node raw
backend checks. This gate does not claim four-host shared-LUN deployment,
positive crash recovery, prepared transactions, or performance certification.
It does not replace the separately recorded C8/soak/micro/D7/PRE results.

All registered unit tests remain mandatory, including safety and negative tests.
Compilation may run in parallel; the unit runner still executes every binary
and propagates failures. No product timeout is changed. `cppcheck` remains
strict with a zero-report baseline and narrowly documented suppressions.
Clang findings retain the existing warn-only policy and published reports;
a failed compiler, analyzer command or cleanup is a CI failure.

### Historical extended checks

The maintainer approved this MVP scope on 2026-09-17. The previous full nightly
workflow is preserved as `legacy-extended.yml`, with the same test commands,
failure exits, schedule and log uploads. It can fail independently; it is not
converted into a successful or skipped MVP check.

Former fast files `226` and `273` include legacy positive 2PC/recovery
qualification; `333` uses an old multi-node raw-device deployment fixture.
They remain in the historical workflow. Other historical shards cover old
setup/protocol expectations, recovery, faults, deployment and performance.
Non-blocking does not mean every failure is obsolete or safe to ignore:
supported-path defects still require triage and repair.

The [31-job historical result record](../../docs/ci/historical-results-2026-09-17.json)
preserves the source commit, 3 PASS / 28 FAIL outcomes and original job links.
Original artifacts remain subject to GitHub retention. No historical test
source was removed by the scope change.

### Before packaging

Run the MVP nightly on the candidate, wait for both workflows, then verify the
full immutable commit (requires authenticated `gh`):

```bash
gh workflow run fast.yml --ref <candidate-branch>
gh workflow run nightly.yml --ref <candidate-branch>
python3 scripts/ci/mvp_ci.py --verify-release \
  --repository sqlrush/pgrac --sha <40-character-commit-sha>
```

The checker rejects missing/incomplete runs, skipped required jobs, an older
commit, a different workflow, and fallback to an earlier green after a newer
failure. Documentation-only fast runs do not qualify a release. Historical
results are not counted as MVP passes. PR runs test a synthetic merge, so they
do not certify the head commit for release; use the full manual runs above,
or completed push/scheduled runs that check out the exact commit.
Packaging in `release.yml` depends on
this checker and preserves its JSON evidence; it does not publish a Release.
If a tag-triggered package starts before CI finishes, it fails closed and must
be rerun after the required checks finish.

```bash
python3 scripts/ci/test_mvp_ci.py
python3 scripts/ci/test_scan_build.py
```

## Scripts

### `check-comment-headers.sh`

Enforces CLAUDE.md rule 11 (comment conventions) on changed files:

1. **Hard errors** (CI fails):
   - Newly-added cluster source files (`src/src/(backend|include)/cluster/**.{c,h}`)
     must contain `Author: SqlRush <sqlrush@gmail.com>`
   - Newly-added cluster test files (`src/src/test/cluster_(unit|tap|regress)/**.{c,h,pl,pm}`)
     must contain the same Author line

2. **Warnings** (CI not failed in stage 0.6, will tighten in 0.7):
   - Modified PG-original files (under `src/src/` outside cluster
     directories) should contain a `PGRAC` marker

#### Local invocation

```bash
# from repo root
./scripts/ci/check-comment-headers.sh
```

The script picks the diff base automatically:
- PR event: `origin/$GITHUB_BASE_REF`
- Push event: `merge-base origin/main HEAD`
- Local: same as push

### `check-format.sh` (stage 0.7)

Enforces `.clang-format` compliance on pgrac cluster sources.  Iterates
the cluster paths (`src/src/backend/cluster/`, `src/src/include/cluster/`,
`src/src/test/cluster_unit/`) and diffs each `.c` / `.h` file against
the output of `clang-format`.  Fails CI if any file has a violation.

PG-original files are NOT checked (they remain pgindent-styled per
upstream convention).

To fix locally: `clang-format -i <file>`.

### `check-tidy.sh` (stage 0.7, warn-only)

Runs `clang-tidy` on cluster `.c` files using the rules in `.clang-tidy`.
Stage 0.7 is **warn-only** -- the script always exits 0 even on
warnings.  Stage 0.27 will tighten this to fail on new warnings.

## Future scripts (placeholder)

| Script | Stage | Purpose |
|---|---|---|
| `check-perf-baseline.sh` | 0.23 | Run pgbench and compare against perf-baseline.md |
| `check-secrets.sh` | 0.27 | git-secrets / detect-secrets wrapper |
| `check-static-analysis.sh` | 0.27 | cppcheck / scan-build wrapper |
| `run-standard-pipeline.sh` | 0.30 | One-shot local equivalent of CI standard pipeline |

## Versioning

Scripts here are part of the pgrac codebase under PostgreSQL License.
Each script's header documents what changed and why.
