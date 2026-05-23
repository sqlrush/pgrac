# Codex Review: spec-3.2 MVCC cluster path + TT status wire

日期：2026-05-23
仓库：`sqlrush/linkdb`
范围：spec-3.2 MVCC visibility fork / TT_STATUS_HINT wire / D5b inject / TAP 204 实装后的 codereview 与修复结论

## Review 范围

本次 review 覆盖 spec-3.2 代码完成后的真实接线层：

- `HeapTupleSatisfiesMVCC()` cluster visibility fork。
- `PGRAC_IC_MSG_TT_STATUS_HINT = 20` wire propagation。
- TT status hint outbound ring / LMON drain / receiver install。
- visibility inject shmem + SQL UDF。
- `t/204_cluster_visibility_fork_2node.pl`。
- disable-cluster build compatibility。
- nightly full CI baseline ripple。

## 结论

spec-3.2 的主线是正确的：它把 spec-3.1 的 exact-key TT status foundation 消费到 `HeapTupleSatisfiesMVCC()` 的 cluster fork，并补上 TT_STATUS_HINT wire propagation。

review 后最重要的修正是：**COMMITTED/CLEANED_OUT 不能在 spec-3.2 直接返回 visible**。因为此时还没有 snapshot/read_scn 语义，只有“远端事务已提交”的 TT status，不足以判断当前 snapshot 是否可见。因此 spec-3.2 正确行为是：

- ABORTED -> invisible
- IN_PROGRESS -> invisible
- COMMITTED/CLEANED_OUT -> fail-closed `53R97`
- missing remote exact key -> fail-closed `53R97`
- production placeholder path 不进入 cluster fork，保持 silent PG-native fallback，直到 spec-3.4 ITL writable/origin 激活

最终修复已通过 fast-gate 与 nightly full CI。

## Findings And Fixes

### P0-1: COMMITTED/CLEANED_OUT 不能在缺少 snapshot 协议时返回 visible

问题：

- 初版 cluster fork 在拿到 COMMITTED/CLEANED_OUT 后存在过早返回 visible 的风险。
- 这会把“事务已提交”错误等同于“对当前 snapshot 可见”。

修复：

- `1517bffb8e` 修改 `heapam_visibility.c`。
- COMMITTED/CLEANED_OUT 继续 fall-through 到 fail-closed `53R97`。
- 等 spec-3.3 提供 `read_scn` / snapshot consistency 后再 reverse 该 fail-closed。

### P0-2: TT_STATUS_HINT receiver 必须校验 payload length

问题：

- wire receiver 如果不检查 `env->payload_length == sizeof(ClusterTTStatusHintMsg)`，可能读取短 frame 或接受带 unaudited tail 的长 frame。

修复：

- `1517bffb8e` 增加 exact payload ABI validation。
- 短/长 frame 均 drop，并 bump invalid counter。

### P1-1: D5b visibility inject UDF 缺 SQL-visible pg_proc rows

问题：

- TAP 204 需要通过 SQL 开关强制进入 cluster visibility path。
- 初版 C helper 存在，但 SQL 层不可调用，测试只能间接覆盖。

修复：

- `1517bffb8e` 增加 `cluster_test_inject_visibility_tt_ref` / `cluster_test_clear_visibility_injects` pg_proc rows。
- 同步 catversion bump 到 `202605470`。

### P1-2: disable-cluster build 链接失败

问题：

- 新 pg_proc rows 引用 visibility inject C symbol。
- `cluster_visibility_inject.o` 不进入 disable-cluster vanilla binary，导致 disable-cluster build 链接失败。

修复：

- `209e87585e` 在 always-linked `cluster_ic.c` 中提供 `#ifndef USE_PGRAC_CLUSTER` stubs。
- fast-gate disable-cluster regression 通过。

### P1-3: shmem region baseline ripple 漏掉 visibility inject region

问题：

- spec-3.2 新增 `pgrac cluster visibility inject` shmem region。
- nightly full TAP 中 `020_shmem_registry.pl` 仍以 35 region 为 baseline。

修复：

- `8bd12fe878` 更新 `cluster.shmem_max_regions` lower-bound 35 -> 36。
- 同步 `020_shmem_registry.pl` region count / owner count / bytes count / restart baseline。

### P1-4: pg_cluster_state category baseline ripple 漏掉 `024_pcm_lock.pl`

问题：

- `023`、`030`、`204` 已经期待 25 categories。
- `024_pcm_lock.pl` 仍写死 24，nightly Ubuntu/macOS full cluster-check 同步失败。

修复：

- `4816235737` 更新 `024_pcm_lock.pl` 24 -> 25。
- targeted TAP `t/024_pcm_lock.pl` 本地 PASS。

## Final Deliverable State

核心落地提交：

- `afe387d87e` feat(3.2): MVCC cluster path + TT status wire propagation
- `61516b1012` feat(3.2): visibility fork unit + t/204 TAP
- `05f08b8e0c` fix(3.2): clang-format test_cluster_visibility_fork.c
- `1517bffb8e` fix: harden spec 3.2 visibility fork tests
- `209e87585e` fix: link visibility inject stubs without cluster
- `8bd12fe878` fix: update shmem region baseline for visibility inject
- `4816235737` fix: update cluster state category baseline

最终 tag：

- `v0.48.0-stage3.2`

## Verification

本地验证覆盖：

- `make -C src/backend -j4`
- `make -C src/test/cluster_unit check`
- targeted TAP `t/203_cluster_tt_status_foundation.pl`
- targeted TAP `t/204_cluster_visibility_fork_2node.pl`
- targeted TAP `t/020_shmem_registry.pl`
- targeted TAP `t/024_pcm_lock.pl`
- `git diff --check`

GitHub CI:

- fast-gate `26301192752`: PASS
- nightly full CI `26301925645`: PASS
  - Ubuntu full build/test: PASS
  - macOS full build/test: PASS
  - Stage 2 acceptance medium: PASS

## Residual Risk

- spec-3.2 deliberately does not solve snapshot consistency; COMMITTED/CLEANED_OUT remains `53R97`.
- production cross-node visibility path still depends on spec-3.4 ITL.origin / ITL writable activation.
- `commit_scn` is still not propagated in TT_STATUS_HINT V1; spec-3.3 must address this before COMMITTED can become visible.

## Verdict

通过。spec-3.2 correctly wires the MVCC cluster fork and TT status hint propagation while preserving fail-closed semantics until spec-3.3 snapshot consistency exists.
