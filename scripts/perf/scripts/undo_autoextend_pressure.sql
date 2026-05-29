-- spec-3.8 D14 — perf workload class 8:  2node-undo-autoextend-pressure.
--
-- Goal: stress autoextend lazy at exhaustion path + lifecycle_lock +
-- double-checked locking pattern;  measure file create + fsync latency
-- tax on hot path.
--
-- Per spec-3.8 v0.3 §4.5 + F4 codex review: workload uses **test hook /
-- debug helper** to move active cursor to segment end deterministically;
-- DOES NOT rely on hope-based natural fillup of 64MB segment (avoiding
-- the v0.1 mistake).
--
-- Expected metrics (per spec §4.5 + perf-gates §2.8):
--   - p95 latency ≤ 2× class 1 single-node baseline
--   - autoextend_count == segment_switch_count (paired)
--   - segment_create_fail_count = 0 (default 5sec timeout 在 production OK)
--   - segment_hard_cap_fail_count = 0 (default 256 不在 test workload 触发)
--
-- pgbench vars:
--   :scale   pgbench scale factor
--   :node_id 0 or 1 (per-node partitioning)

\set aid random(1, 100000)
\set val random(1, 9999)

BEGIN;
INSERT INTO pgbench_history (aid, bid, delta, mtime, filler)
    VALUES (:aid, 1, :val, CURRENT_TIMESTAMP, repeat('x', 600));  -- 600B filler reserved;真 trigger 靠 test hook (see fixture)
COMMIT;
