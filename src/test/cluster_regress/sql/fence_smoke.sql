-- spec-2.28 D16: Fence-lite smoke test (cluster_regress).
--
--   Verifies the catalog surface added by spec-2.28 Step 4:
--     - 4 NEW PGC_POSTMASTER GUCs (cluster.self_fence_enabled +
--       cluster.self_fence_grace_ms +
--       cluster.freeze_writes_enabled +
--       cluster.fence_audit_log)
--     - 1 NEW SQLSTATE 53R50 ERRCODE_CLUSTER_QUORUM_LOST_BACKEND
--     - pg_cluster_fence_state view (8 columns)
--     - 1 NEW wait event (ClusterFenceBackendInterruptCheck)
--     - 4 NEW pgstat counters (cluster.fence.*)
--     - 3 NEW inject points (cluster-fence-pre-freeze-broadcast +
--       cluster-fence-pre-self-fence-shutdown +
--       cluster-fence-post-thaw-broadcast)
--
-- All assertions are catalog-level (no fence runtime exercise);
-- runtime broadcast / backend abort / postmaster self-fence are
-- TAP-tested in t/098_fence_freeze_writes_2node.pl (Step 5 D14)
-- + t/096_quorum_2node_round_trip.pl L4-L5 unblock (Step 5 D15).
--
-- ----------
-- Block 1: 4 NEW fence GUCs registered.
-- ----------
SELECT name, vartype, context
  FROM pg_settings
 WHERE name IN ('cluster.self_fence_enabled',
                'cluster.self_fence_grace_ms',
                'cluster.freeze_writes_enabled',
                'cluster.fence_audit_log')
 ORDER BY name;

-- ----------
-- Block 2: pg_cluster_fence_state view exists with 8 columns.
-- ----------
SELECT count(*) AS column_count
  FROM information_schema.columns
 WHERE table_name = 'pg_cluster_fence_state';

-- ----------
-- Block 3: pg_cluster_fence_state SRF returns single row;
-- counters init=0 in fresh cluster + self_fence_pending=false +
-- last_freeze_at + last_thaw_at NULL (never broadcast).
-- ----------
SELECT last_freeze_at IS NULL AS last_freeze_null,
       last_thaw_at IS NULL AS last_thaw_null,
       self_fence_pending,
       freeze_broadcast_count,
       thaw_broadcast_count,
       self_fence_initiated_count
  FROM pg_cluster_fence_state;

-- ----------
-- Block 4: 1 NEW wait event visible + 4 NEW pgstat counters visible
-- + 3 NEW inject points visible.
-- ----------
SELECT count(*)
  FROM pg_stat_cluster_wait_events
 WHERE name = 'ClusterFenceBackendInterruptCheck';

SELECT count(*)
  FROM pg_stat_cluster_counters
 WHERE name LIKE 'cluster.fence.%';

SELECT count(*)
  FROM pg_stat_cluster_injections
 WHERE name LIKE 'cluster-fence-%';
