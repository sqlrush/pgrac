-- spec-3.7 D14 — perf workload class 7:  2node-undo-write-pressure.
--
-- 4 op mix (INSERT + UPDATE + SELECT FOR SHARE + DELETE) to exercise:
--   - D5 cluster_undo_record_alloc record-level allocator
--   - D6 DML 4 op emit hook (heap_insert real allocation; heap_update /
--     heap_delete / heap_lock_tuple deferred to Hardening v1.0.2 H-4/5/6
--     so they fall through to TT-only path under MVP)
--   - D7 cluster_smgr direct write + durable flush per W2 self-contained
--     no-WAL ordering (block-level fsync per record)
--
-- Workload notes (spec-3.7 v0.4 Sprint A Step 11):
--   - INSERT into pgbench_history → triggers D6 heap_insert undo emit
--     (UndoInsertPayload + cluster_undo_record_alloc + UBA encoded via
--     spec-3.4b uba_encode);  counters bump:
--       cluster_undo_record_alloc_count +1
--       cluster_undo_block_write_count +1 per block fill
--       cluster_undo_block_flush_count +1 per durable fsync
--   - UPDATE / SELECT FOR SHARE / DELETE under MVP fall through TT-only
--     path (cluster_itl_uba = spec-3.4b TT-only encoding;  no
--     cluster_undo_record_alloc call).  After H-4/5/6 amend in Hardening
--     v1.0.2, all 4 op will emit undo records and counters reflect full
--     workload contribution.
--   - Hot key span 1..100 to balance allocator contention vs ITL slot
--     wraparound (INITRANS=8 cap).
--
-- Expected baseline (per spec §4.5 + perf-gates §2.7):
--   - TPS ≥ 50% class 1 baseline (4 op undo path overhead; durable fsync
--     per record is hot path tax)
--   - p95 ≤ 3× class 1 single-node (fsync cost)
--   - cluster_undo_record_alloc_count : cluster_undo_block_write_count
--     ratio ≈ 1:64 (8KB block / 32B record header + 4B insert payload =
--     ~218 records per block;  but heap_insert produces 1 record per tx
--     so actually 1:1 at slow rate)
--
-- pgbench vars:
--   :scale     pgbench scale factor
--   :node_id   0 or 1 (per-node partitioning)

\set aid random(1, 100000)
\set lock_aid random(1, 100000)
\set bal random(-5000, 5000)

BEGIN;
INSERT INTO pgbench_history (aid, bid, delta, mtime, filler)
    VALUES (:aid, 1, :bal, CURRENT_TIMESTAMP, NULL);          -- D6 INSERT undo path 真激活
UPDATE pgbench_accounts SET abalance = abalance + :bal
    WHERE aid = :aid;                                          -- D6 UPDATE undo path (MVP TT-only, Hardening v1.0.2 H-4)
SELECT abalance FROM pgbench_accounts WHERE aid = :lock_aid
    FOR SHARE;                                                 -- D6 heap_lock_tuple ITL undo path (MVP TT-only, H-6)
DELETE FROM pgbench_history WHERE aid = :aid AND delta = :bal; -- D6 DELETE undo path (MVP TT-only, H-5)
COMMIT;
