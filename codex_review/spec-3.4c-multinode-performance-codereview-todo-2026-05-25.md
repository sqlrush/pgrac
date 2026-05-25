# spec-3.4c+ Codereview TODO: Real Multi-Node Performance Gate

Date: 2026-05-25

## P0 Review Gate

Do not treat single-node / no-peer fast-path recovery as proof that RAC multi-node performance is acceptable.

The current single-node fixes are necessary to keep `cluster.enabled=on` from polluting PG-native performance when no peer exists, but they are not sufficient for the real RAC path.  Starting with spec-3.4c codereview and every follow-up Stage 3/4 spec that touches ITL, TT, SCN, Undo, Cache Fusion, or visibility, the review must explicitly separate:

1. single-node `cluster.enabled=off` vs `cluster.enabled=on`
2. 2-node local-affinity workload
3. 2-node hot-block / hot-row workload
4. 2-node cross-node visibility workload

## Required Strategy

- Keep no-peer fast paths, but do not count them as multi-node optimization.
- Add a 2-node local-affinity pgbench or microbench where each node mostly writes its own key/page range.
- Add a 2-node hot-block / hot-row microbench that intentionally forces ownership transfer and measures the real Cache Fusion cost.
- Use separate thresholds for local-affinity and hot-block workloads; a single mixed TPS number hides the bottleneck.
- Optimize the real multi-node path with:
  - locality-aware ownership and partition/key affinity
  - on-demand RAC metadata activation, not blanket ITL/TT work on every local-only DML
  - batched TT status hint / SCN broadcast / ITL finish WAL
  - partitioned or per-backend caches for TT allocator, TT overlay, and hot counters
  - deferred/folded observability counters outside commit hot paths

## Codereview Checklist

- [ ] The spec/code distinguishes no-peer fast path from 2-node enabled path.
- [ ] There is at least one 2-node local-affinity performance run.
- [ ] There is at least one 2-node hot-block or hot-row performance run.
- [ ] The review report includes the exact TPS/latency numbers for all three classes: off/on single-node, 2-node local-affinity, 2-node hot-block.
- [ ] Any optimization claimed as “multi-node” is exercised under a real 2-node topology, not inferred from single-node `cluster.enabled=on`.
- [ ] If 2-node hot path remains yellow, the follow-up spec must identify the concrete bottleneck: ownership transfer, TT lookup, WAL, SCN broadcast, allocator contention, or counter contention.

## Applies To

- spec-3.4c codereview
- spec-3.4d / heap_lock_tuple if created
- spec-3.5 SUBTRANS / subxact ITL status
- future Undo / CR block construction specs
- Stage 4 internal-run acceptance
- Stage 5 core-functional acceptance
