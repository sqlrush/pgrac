# cppcheck Baseline — Stage 0.x

> Marker file for `scripts/ci/baseline-diff.py` strict-mode comparison.
> The authoritative baseline retrospective lives in
> `pgrac:docs/ci-static-analysis-baseline-stage0.27.md` (private repo).
>
> Stage 0 baseline at v0.1.0-stage0.30: **0 cppcheck findings**.
> Established at spec-0.27.5 §6 after suppression sweep; verified
> green at every subsequent spec.
>
> Strict mode behaviour: any cppcheck finding in CI run is treated
> as a regression and fails the Security job.  See spec-0.30 §2.3.
>
> Stage 1+: when baseline becomes non-empty, this file's known-finding
> table will be parsed by baseline-diff.py to subtract from current.
> Currently empty (sentinel).

## Known findings (suppressed in baseline; future-extension)

_(none at stage 0 -- all findings either fixed or in
scripts/ci/cppcheck-suppressions.txt)_
