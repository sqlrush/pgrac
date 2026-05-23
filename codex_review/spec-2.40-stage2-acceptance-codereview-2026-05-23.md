# Codex Review: spec-2.40 Stage 2 acceptance

日期：2026-05-23
仓库：`sqlrush/linkdb`
范围：spec-2.40 Stage 2 acceptance / perf baseline / fault matrix 实装后的 codereview 结论

## Review 范围

本次 review 覆盖 spec-2.40 的 Stage 2 收官接受层：

- `t/200_stage2_acceptance_capability.pl`
- `t/201_stage2_acceptance_fault_matrix.pl`
- `t/202_stage2_acceptance_perf_smoke.pl`
- `scripts/perf/run-stage2-cluster-baseline.sh`
- `scripts/perf/run-stage2-fault-matrix.sh`
- Stage 2 acceptance helper modules
- fast-gate / nightly workflow 接入

## 结论

spec-2.40 的方向正确：它不是新增数据库功能，而是把 Stage 2 已 ship 的 GES/GCS/SINVAL/SCN/reconfig/fault/perf 能力组织成可重复的 acceptance layer。

最终形态在 `v0.46.2-stage2.40` 收口：

- capability matrix 有真实 TAP 覆盖。
- fault matrix 有独立脚本与 TAP 入口。
- perf smoke 和 stage2 baseline 脚本落地。
- nightly 增加 Stage 2 acceptance medium job。
- perf smoke 已隔离 Cache Fusion GCS invalidate timeout，避免 acceptance 本身被非目标 GCS timeout 污染。

## Findings And Fixes

### HIGH-1: acceptance helper 初版断言偏浅

初版 `t/200_stage2_acceptance_capability.pl` 更接近“surface exists”检查，缺少对 Stage 2 核心能力的更细粒度断言。

修复：

- `5f3aa8282d` 深化 `t/200` assertions。
- 同步调整 `PgracClusterContention.pm` / `PgracClusterDdlLoop.pm`，让 helper 输出更稳定、更适合 CI 解析。

### HIGH-2: Stage 2 acceptance medium 没有稳定纳入 nightly

初版只提供本地/脚本入口，不足以作为“Stage 2 done”接受门。

修复：

- `02a574ed04` 在 `.github/workflows/nightly.yml` 增加 Stage 2 acceptance medium job。
- 同时 harden D4 parse，避免报告解析对文本格式过敏。

### MED-1: perf smoke 会被 GCS invalidate timeout 干扰

`t/202_stage2_acceptance_perf_smoke.pl` 初版可能被 Cache Fusion invalidate timeout 影响，导致 acceptance perf smoke 红点不代表 Stage 2 acceptance failure。

修复：

- `e161993845` 隔离 acceptance perf smoke 与 GCS invalidate timeout。
- 该修复使 t/202 更像 acceptance smoke，而不是 GCS timeout hardening 测试。

## Final Deliverable State

核心落地提交：

- `6d9a569f82` feat(2.40): Stage 2 acceptance + perf baseline
- `5f3aa8282d` fix(2.40): refine acceptance helpers + t/200 deeper assertions
- `02a574ed04` feat(2.40): land D9 stage2-acceptance-medium nightly job + harden D4 parse
- `e161993845` fix(2.40): isolate acceptance perf smoke from GCS invalidate timeout

最终 tag：

- `v0.46.2-stage2.40`

## Residual Risk

- perf gates 仍是 smoke/medium 级别，不等于长时间 soak。
- Stage 2 acceptance 证明“能力闭环可运行”，不证明极致性能或所有 crash/recovery 组合。
- 4-node / longer fault matrix 仍应保留为后续 hardening / release precheck。

## Verdict

通过。spec-2.40 已把 Stage 2 从“多个独立 spec 都绿”提升为“成体系 acceptance 可复跑”。后续 Stage 3 可以基于该 acceptance layer 继续累加 MVCC/cross-node visibility 能力。
