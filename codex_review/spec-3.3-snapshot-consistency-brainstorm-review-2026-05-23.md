# Codex Review: spec-3.3 snapshot consistency brainstorm

日期：2026-05-23
仓库：`sqlrush/linkdb`
范围：spec-3.3 brainstorm（Stage 3 第 3 sub-spec: snapshot consistency 跨节点）

## Review 范围

本次 review 覆盖 spec-3.3 brainstorm 中的 8 个关键决策点，重点检查：

- 是否正确承接 spec-3.1 TT status foundation 与 spec-3.2 MVCC visibility fork。
- Snapshot / SCN / TT status hint 三条路径是否能闭环。
- 是否误判已有 BOC broadcast 实现状态。
- 是否会把 `InvalidScn` 静默解释为 invisible。
- 是否遗漏 `SnapshotData` ABI ripple。

## 结论

不建议直接 `approve all★`。

主方向正确：spec-3.3 应走 SCN-based snapshot，对齐 AD-008 / Oracle SCN 模型，并用 `commit_scn <= read_scn` 作为跨节点 committed tuple 的可见性判定。

但当前 brainstorm 有 3 个必须修订的问题：

1. `cluster_visibility_decide_by_scn()` 不能返回 bool，必须能表达 UNKNOWN。
2. spec-3.3 必须让 TT overlay / TT_STATUS_HINT 携带真实 `commit_scn`，否则 COMMITTED 仍然无法按 SCN 判可见。
3. `PGRAC_IC_MSG_BOC_BROADCAST=3` 已在 spec-2.9 真激活，spec-3.3 不应重新设计/声明“真激活 BOC”。

## Findings

### P0-1: `InvalidScn -> false` 会把未知状态静默当成不可见

brainstorm Q3 提议：

```c
cluster_visibility_decide_by_scn(commit_scn, read_scn) -> bool
```

并描述 `InvalidScn commit_scn` 返回 `false fail-closed`。

这个语义不成立。`false` 在 `HeapTupleSatisfiesMVCC()` 中表示 tuple invisible，而不是 fail-closed。若 `COMMITTED/CLEANED_OUT` 的 status 已经存在但 `commit_scn == InvalidScn`，直接返回 `false` 会制造 silent invisible 行为，隐藏缺失的 SCN 传播/落地问题。

修订建议：改为三态 enum。

```c
typedef enum ClusterVisibilityDecision
{
    CLUSTER_VISIBILITY_VISIBLE,
    CLUSTER_VISIBILITY_INVISIBLE,
    CLUSTER_VISIBILITY_UNKNOWN
} ClusterVisibilityDecision;
```

判定规则：

- `commit_scn <= read_scn` -> `VISIBLE`
- `commit_scn > read_scn` -> `INVISIBLE`
- `commit_scn == InvalidScn` -> `UNKNOWN`，由 caller 抛 `53R97`

### P0-2: COMMITTED 可见性需要真实 `commit_scn`，但当前 TT overlay / hint 仍写 `InvalidScn`

实证代码路径：

- `xact.c` 已有 `commit_scn = cluster_scn_advance_for_commit()`。
- `cluster_tt_local_record_commit(xid)` 当前没有接收 `commit_scn` 参数。
- `cluster_tt_local.c` 里 `cluster_tt_status_install_local(&key, status, InvalidScn)` 固定写 `InvalidScn`。
- `ClusterTTStatusHintMsg` 当前 32B payload 只含 `status + key`，没有 `commit_scn`。
- receiver 端也是 `cluster_tt_status_install_local(..., InvalidScn)`。

这意味着 spec-3.3 即使给 Snapshot 加 `read_scn`，`COMMITTED/CLEANED_OUT` 仍然没有可比较的 SCN。

修订建议：spec-3.3 scope 中必须新增一组 D：

- `cluster_tt_local_record_commit(TransactionId xid, SCN commit_scn)`
- `xact.c` 在现有 commit hook 中传入已捕获的 `commit_scn`
- `ClusterTTStatusHintMsg` 升级为 V2，携带 `commit_scn`
- receiver install remote status 时写入真实 `commit_scn`
- abort path 可继续使用 `InvalidScn`

这不等同于 spec-3.4 的 ITL persistent write。spec-3.3 只需要 in-memory TT overlay + wire hint 携带真实 `commit_scn`，仍可把 ITL page durable commit_scn 写入推给 spec-3.4。

### P1-1: BOC broadcast 当前不是 reserved/stub，spec-2.9 已真激活

brainstorm Q5 写法暗示 spec-3.3 要“真激活 `PGRAC_IC_MSG_BOC_BROADCAST=3`”。实证不符。

当前代码已经有：

- `PGRAC_IC_MSG_BOC_BROADCAST = 3`
- walwriter BOC tick
- LMON-mediated fanout
- receiver 通过 envelope SCN 观察远端 SCN
- handler intentionally NO-OP，因为 `cluster_ic_envelope_verify()` 已经 observe envelope SCN

修订建议：

- spec-3.3 改为“复用 spec-2.9 BOC broadcast + envelope SCN observe”。
- 不新增 `cluster_scn_broadcast.c`，不重新声明 commit/abort hook emit BOC。
- 如果需要更强的 read-after-commit freshness，应另列 risk / forward link，而不是在 spec-3.3 重做 BOC。

### P1-2: `SnapshotData` ABI ripple 低估

brainstorm Q4 提议给 `SnapshotData` 增加 `source/read_scn`，方向可行，但必须显式列 ABI / serialization ripple。

需要审计的路径包括：

- `src/include/utils/snapshot.h` `SnapshotData`
- `src/backend/utils/time/snapmgr.c` static snapshot initialization / `CopySnapshot`
- logical decoding `snapbuild.c` / `reorderbuffer.c` 中基于 `sizeof(SnapshotData)` 的 snapshot copy / serialization
- `CatalogSnapshotData` / `SnapshotSelfData` / `SnapshotAnyData` 等 special snapshot 的初始化语义
- catalog scan 是否强制 LOCAL，不能只靠 `cluster_enabled` 推导

修订建议：

- Snapshot 增加明确字段，例如 `uint8 cluster_source` + `SCN read_scn`，必要时加 `uint32 read_epoch`。
- `GetSnapshotData()` only for MVCC cluster snapshot 拍 `read_scn`。
- catalog/local/special snapshots 必须显式保持 LOCAL + `InvalidScn`。

## Recommended Verdict

建议修订后的 Q verdict：

| Q | Verdict |
|---|---------|
| Q1 protocol | A: SCN-based |
| Q2 read_scn 时机 | A: `GetSnapshotData()` 拍取 |
| Q3 helper | 改为三态 enum，不要 bool |
| Q4 snapshot.source | A，但补 SnapshotData ABI ripple + catalog/local snapshot 审计 |
| Q5 broadcast | 改为复用 spec-2.9 BOC，不重新真激活 |
| Q6 spec-3.2 D5 amend | A，但前置要求 TT overlay/hint 携带真实 `commit_scn` |
| Q7 VACUUM horizon | A: 推后续独立 spec |
| Q8 fault tolerance | A: 复用 epoch fence；建议 snapshot 带 `read_epoch` 并定义 reconfig 后旧 snapshot 行为 |

## DRAFT v0.1 必须新增/修订的 Deliverables

建议在 v0.1 draft 中把 deliverables 改成更真实的链路：

1. D1: `SnapshotData` 增加 `cluster_source/read_scn`，并完成 snapmgr/logical decoding ripple。
2. D2: `GetSnapshotData()` 在 cluster MVCC snapshot 下设置 `read_scn`。
3. D3: 增加 `ClusterVisibilityDecision` 三态 helper。
4. D4: `cluster_tt_local_record_commit(xid, commit_scn)`，本地 TT overlay 写真实 `commit_scn`。
5. D5: `TT_STATUS_HINT` V2 payload 携带 `commit_scn`。
6. D6: receiver install remote TT status 时保留 `commit_scn`。
7. D7: `HeapTupleSatisfiesMVCC()` 在 `COMMITTED/CLEANED_OUT` 分支按三态 helper 判定。
8. D8: 复用现有 BOC broadcast，不新增 BOC wire。
9. D9: TAP 205 覆盖 D5b inject 的 COMMITTED visible / future commit invisible / InvalidScn -> 53R97。

## 最终建议

不要按原 brainstorm 直接生成 DRAFT。先按上面 4 个 finding 修订 scope，再起 spec-3.3 v0.1。

核心原则：spec-3.3 要解决的是“snapshot/read_scn consumer + committed status 按 SCN 判定”，但它必须同步补齐 in-memory TT status 中的 `commit_scn` 传播。否则 `COMMITTED` 分支没有数据可判定，只能继续 53R97 或错误地 silent invisible。
