# Codex Review: spec-3.1 TT status foundation

日期：2026-05-23
仓库：`sqlrush/linkdb`
范围：spec-3.1 Undo TT status foundation / ITL reader / exact-key API 实装后的 codereview 结论

## Review 范围

本次 review 覆盖 spec-3.1 从草案纠偏到最终 ship 的实现路径：

- 禁止 CLOG overlay 的设计回退。
- `ClusterTTStatusKey` exact-key API。
- in-memory TT status overlay。
- read-only ITL reader。
- xact commit/abort local install hook。
- reconfig flush。
- shmem/GUC/TAP baseline ripple。
- `t/203_cluster_tt_status_foundation.pl`。

## 结论

spec-3.1 最关键的 review 结论是：Stage 3 的事务状态底座必须是 **Undo TT / ITL exact-key**，不能回到 PG CLOG overlay。

v0.1 的 CLOG cache coherence 方向被 review 判定为错误，因为 pgrac 后续 MVCC 目标是通过 ITL + Undo + TT 替代 CLOG 作为 cluster visibility 的主路径。最终实现已改为：

- 不改 `TransactionIdDidCommit()` cluster 语义。
- 不扩 `SharedInvalidationMessage` 做 CLOG status。
- 不引入 `SharedInvalCLOGStatusMsg`。
- 通过 `ClusterTTStatusKey` 精确定位 TT status。
- 通过 read-only ITL reader 取得 TT ref。
- 通过 local in-memory overlay 建立 self-consumer contract。

最终形态在 `v0.47.0-stage3.1` 收口。

## Findings And Fixes

### P0-1: v0.1 CLOG foundation 方向错误

问题：

- CLOG overlay 会让 Stage 3 又回到 PG xid/CLOG 语义。
- 这违反 AD-006 / feature-069 的 ITL + Undo + TT 方向。
- 也会破坏 spec-3.2 需要的 exact-key cluster visibility contract。

修复：

- spec-3.1 切换为 TT status foundation。
- 增加 `scripts/ci/check-no-clog-overlay.sh`，在 fast-gate 中防止 CLOG overlay 回归。

### P1-1: commit hook 初版缺少真实 consumer

问题：

- 只安装 overlay，但没有能证明刚安装的 key 可读的 consumer。
- 这会形成 helper-defined-but-never-wired 风险。

修复：

- `118720a5b4` 增加 self-consumer lookup。
- `cluster_tt_local_record_commit()` 安装后立即 `lookup_exact()`，并 bump `self_consumer_hit_count`。
- `t/203` 覆盖 SQL commit / rollback 后的 install + self-consumer counter。

### P1-2: bootstrap / disable-cluster / cppcheck 边界需要补齐

修复：

- `c13c5c6b6e` 补 disable-cluster guard、cppcheck 抑制与 category snapshot。
- `4a46254f08` 修 `cluster_tt_status_shmem_init` 中 `MemSet` 使用问题。

### P1-3: shmem region baseline ripple 不完整

问题：

- 新增 TT status 相关 shmem 后，`020_shmem_registry` 和 GUC lower-bound baseline 需要同步。

修复：

- `76352352e8` 更新 shmem registry baseline。
- `485184f6da` 同步剩余 TAP baseline。

### P2-1: banned identifier 文本触发 lint

问题：

- 测试文本中出现禁止的 CLOG overlay identifier，触发 no-clog-overlay gate。

修复：

- `c9c56e5b29` 重写测试中的 banned identifier mention，保留 guardrail 语义但不触发 lint。

## Final Deliverable State

核心落地提交：

- `a6347a41e7` feat(3.1): undo TT status foundation
- `c9c56e5b29` fix(3.1): rephrase banned identifier mentions
- `c13c5c6b6e` fix(3.1): fast-gate CI fixes
- `4a46254f08` fix(3.1): replace MemSet with memset
- `118720a5b4` fix(3.1): harden TT status self-consumer and gates
- `76352352e8` fix(3.1): update shmem registry baseline
- `485184f6da` fix(3.1): sync remaining TAP baselines

最终 tag：

- `v0.47.0-stage3.1`

## Residual Risk

- spec-3.1 只做 local in-memory overlay，不做 durable TT slot storage。
- `commit_scn` 仍是 `InvalidScn`，真实 SCN visibility 判定推给 spec-3.3+。
- production tuple 进入 cluster visibility path 仍依赖后续 ITL.origin / ITL writable activation。

## Verdict

通过。spec-3.1 已把 Stage 3 的事务状态底座从错误的 CLOG 方向拉回 Undo TT / ITL exact-key 方向，并用 CI gate 防止 CLOG overlay 回归。
