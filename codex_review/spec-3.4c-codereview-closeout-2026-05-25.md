# spec-3.4c codereview closeout — 2026-05-25

## Verdict

spec-3.4c functional codereview is green after hardening. The main correctness and performance fixes are in the no-peer fast path: a single-node `cluster_enabled=on` deployment with no declared peer must not pay RAC write-side costs.

## Fixes landed

- Replaced the remaining single-node write-side RAC tax with `cluster_conf_has_peers()` gates across ITL write path, transaction-end SCN/TT/ITL finish, cluster snapshot source, sinval enqueue, PCM, and GCS paths.
- Restored hard perf gates in `t/202_stage2_acceptance_perf_smoke.pl` instead of weakening thresholds.
- Added `.github/workflows/perf.yml` plus `scripts/perf/run-baseline.sh --mode=both` CI coverage for enable/disable pgbench comparison.
- Fixed perf workflow masking: `continue-on-error` removed, and `pgrac-init --force` added so script failures fail CI instead of producing false-green runs.
- Updated old TAPs whose assumptions conflicted with the new no-peer contract:
  - `068_wal_xl_scn.pl` now declares a two-node topology when it needs SCN WAL emission.
  - `109_bufmgr_pcm_integration.pl` now asserts no-peer PCM counters stay zero.
  - `110_gcs_loopback.pl` now asserts no-peer GCS lookup stays zero.

## Performance proof

Local targeted perf:

- select-only: off `149412.865946` TPS / on `145160.333845` TPS, regression `2.8% GREEN`
- full: off `16950.323751` TPS / on `15484.639952` TPS, regression `8.6% GREEN`

GitHub perf workflow `26382240734` on latest `4b8f607561`:

- Ubuntu full: off `1298.658351` TPS / on `1572.037829` TPS, regression `-21.1% GREEN`
- Ubuntu select-only: off `33695.494883` TPS / on `30746.218315` TPS, regression `8.8% GREEN`
- macOS full: off `7262.682131` TPS / on `6892.083421` TPS, regression `5.1% GREEN`
- macOS select-only: off `64094.105761` TPS / on `58491.843579` TPS, regression `8.7% GREEN`

## Verification

- `make -C src/backend` PASS
- `make -C src/test/cluster_unit check` PASS
- targeted TAP PASS:
  - `t/068_wal_xl_scn.pl`
  - `t/109_bufmgr_pcm_integration.pl`
  - `t/110_gcs_loopback.pl`
  - `t/206_cluster_itl_writable_2node.pl`
  - `t/207_cluster_production_visibility_2node.pl`
  - `t/208_cluster_d5b_decide_by_scn_real_visible_invisible.pl`
- fast-gate run `26382233920` PASS on `4b8f607561`.

## Follow-up

The no-peer optimization is not a substitute for real multi-node performance work. The separate tracked TODO `spec-3.4c-multinode-performance-codereview-todo-2026-05-25.md` remains mandatory for later specs: future ITL/TT/Undo/CR and visibility specs need real 2-node local-affinity, hot-block/hot-row, and cross-node visibility perf gates.
