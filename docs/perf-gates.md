# pgrac Performance Gates — User Reference

> **如何用 perf gate 跑 pgrac 性能 baseline 测量** — 5-tier × 4 workload class 阈值参考表。
>
> **设计 reasoning**(为啥 5 tier / 为啥 single-node-no-peer 唯一阻塞 / 为啥 class 3/4 partial coverage)在 spec / design docs(私有)— 本文件仅 user-facing 用法。
>
> **Spec**: spec-3.4e (Stage 3 multi-node perf hardening)

---

## 1. 5-tier 性能门 × 4 workload class 阈值表

| Tier | single-node-no-peer | 2node-local-affinity | 2node-cross-node-visibility | 2node-hot-row-lock |
|---|---|---|---|---|
| **GREEN** | ≤ 15% regression | ≤ 25% | p95 ≤ 1.5× / lookup_hit > 0.9 | fail_closed > 0 + p95 ≤ 2× |
| **YELLOW** | ≤ 25% | ≤ 40% | p95 ≤ 2× / lookup_hit > 0.8 | fail_closed > 0 + p95 ≤ 3× |
| **ORANGE** | ≤ 40% | ≤ 60% | p95 ≤ 3× / lookup_hit > 0.7 | p95 ≤ 4× |
| **RED** | ≤ 60% | ≤ 80% | p95 ≤ 5× / lookup_hit > 0.5 | p95 ≤ 5× |
| **CATASTROPHIC** | > 60% | > 80% | p95 > 5× / lookup_hit ≤ 0.5 | p95 > 5× |
| **Ship-blocking?** | **GREEN required** | warn-only | warn-only(partial coverage)| warn-only(partial coverage)|

---

## 2. 4 workload class — 怎么用 + 跑什么

### Class 1: `single-node-no-peer`(★ 唯一 ship-blocking gate)

验证单节点 `cluster.enabled=on` 没配置 peer 时 NOT pay RAC tax(spec-3.4c `cluster_conf_has_peers()` gate)。pgbench builtin TPC-B,`enable build (cluster.enabled=on)` vs `disable build (cluster.enabled=off)` regression。

```bash
./scripts/perf/run-2node-baseline.sh --mode=single-node-no-peer \
    --enable-install=$HOME/linkdb-install \
    --disable-install=$HOME/linkdb-disable-install \
    --scale=10 --duration=30
```

### Class 2: `2node-local-affinity`(warn-only)

ClusterPair 2-postmaster fixture,each node updates own key range — no IC traffic / cross-node visibility / lock contention 触发。期望接近 single-node enable=on perf。

```bash
./scripts/perf/run-2node-baseline.sh --mode=2node-local-affinity \
    --enable-install=$HOME/linkdb-install
```

### Class 3: `2node-cross-node-visibility`(warn-only,partial coverage)

**Partial coverage**:ClusterPair 不真 share heap;class 3 是 inject-based(`cluster_test_inject_visibility_tt_ref` D5b 7-arg + `cluster_test_force_visibility_cluster_path = on` GUC),测 visibility decision path latency + TT lookup hit/miss + 53R97 触发率。**真 cross-node shared heap SELECT TPS contention 需 shared-storage harness(feature-117 / Stage 4+)**。

```bash
# 注意:ENABLE_INJECTION build 必需(--enable-injection-points)
./scripts/perf/run-2node-baseline.sh --mode=2node-cross-node-visibility \
    --enable-install=$HOME/linkdb-install
```

### Class 4: `2node-hot-row-lock`(warn-only,partial coverage)

**Partial coverage**:`cluster_test_inject_visibility_tt_ref` D5b 7-arg `is_lock_only=true` 模拟 remote ITL ACTIVE state + pgbench `SELECT ... FOR UPDATE` 同行,测 `cluster_remote_row_lock_fail_closed_count` 触发率 + WAL bytes/sec。**真 hot-row 跨节点 TPS contention 需 shared-storage harness**。

```bash
./scripts/perf/run-2node-baseline.sh --mode=2node-hot-row-lock \
    --enable-install=$HOME/linkdb-install
```

### 全 4 class 一次跑

```bash
./scripts/perf/run-2node-baseline.sh --mode=all \
    --enable-install=$HOME/linkdb-install \
    --disable-install=$HOME/linkdb-disable-install
```

或 GitHub Actions 触发(weekly + manual):

```bash
gh workflow run perf.yml -R sqlrush/linkdb
```

---

## 3. 5 metric set

| Metric | 来源 | 单位 |
|---|---|---|
| TPS | pgbench summary `tps =` 字段 | tps |
| p95 latency | pgbench `--log --sampling-rate=1.0` per-transaction log parse | ms |
| WAL bytes/sec | `pg_stat_wal.wal_bytes` delta / duration | bytes/sec |
| fail_closed_count rate | `cluster_remote_row_lock_fail_closed_count` delta / duration(shmem-aggregated)| events/sec |
| lookup_hit_miss ratio | `cluster_tt_status_lookup_hit_count / (_hit + _miss)` | ratio 0..1 |
| terminal_authority_failclosed rate | `pg_cluster_state.undo.terminal_authority_failclosed_count` delta / duration | events/sec |
| smart_fusion_failclosed rate | `pg_cluster_state.smart_fusion.dep_lost_failclosed_count + retry_failclosed_count` delta / duration | events/sec |
| smart_fusion_brake wait | `pg_cluster_state.smart_fusion.commit_brake_wait_us` delta / committed txns | us/txn |

Spec-6.2 Smart Fusion terminal authority is a correctness substrate,
not a performance optimization.  It adds the
`terminal_authority_*` and `smart_fusion` counters under
`pg_cluster_state` so class 3/4 runs can distinguish epoch, ownership,
terminal-state, durable-TT, retention, dependency-loss, and retryable
brake fail-closed causes.  The current post-ship guardrail rejects
`cluster.smart_fusion=on`, so Smart Fusion brake counters are reserved
and should stay zero in supported runs.  It does not relax the existing
class thresholds, and a green result must not be obtained by disabling
authority, dropping dependency evidence, or accepting
UNKNOWN-visible/native fallback behavior.

---

## 4. 结果文件位置

`scripts/perf/results/3-4e-<class>-<TS>.{txt,json,log}`:
- `.txt` — pgbench stdout/stderr captured
- `.json` — parsed 5 metric summary(machine-readable)
- `.log` — pgbench `--log` raw transaction logs(p95 计算源)

CI(GitHub Actions perf workflow)上传 artifact `perf-2node-baseline-{ubuntu,macos}-<run_id>`,retention 60 days。

### Storage I/O Matrix (spec-6.0a, report-only)

Production shared-storage backend work adds a storage I/O report under:

```bash
PGRAC_ENABLE_INSTALL=$HOME/linkdb-install \
./scripts/perf/run-storage-io-matrix.sh
```

Default CI shape uses a regular-file raw image with `cluster.block_device_use_odirect=off`, so the artifact is a conformance/trend signal, not a hardware O_DIRECT claim. Set `STORAGE_IO_ODIRECT=on` only on a verified block-device environment where the soundness gate has confirmed direct-I/O alignment behavior.

---

## 5. ship 决策树(简化版)

```
class 1 < GREEN(> 15% regression)? → BLOCK ship + root-cause analysis
class 1 GREEN + class 2/3/4 任何 ≤ ORANGE → ship eligible(warn-only)
class 1 GREEN + class 2/3/4 RED/CATASTROPHIC → ship 仍允许但 review 强制 enumerate
```

详细 reasoning 见 spec design docs(私有)。
