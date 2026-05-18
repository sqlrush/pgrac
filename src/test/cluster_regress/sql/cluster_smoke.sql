-- ============================================================
-- cluster_smoke -- pgrac Stage 0 cluster surface smoke test.
--
--	Locks the column contracts and stable metadata of the views
--	and GUCs registered through stage 0.13 / 0.16 / 0.17 / 0.18 /
--	0.19.  Output must be deterministic across host / port /
--	OS; see spec-0.21-cluster-regress.md §2.2 for the stability
--	rules every query in this file follows.
--
--	Author: SqlRush <sqlrush@gmail.com>
--	Portions Copyright (c) 2026, pgrac contributors
-- ============================================================

-- ----------
-- 1. Three pgrac views exist (regclass cast asserts presence).
-- ----------
SELECT 'pg_stat_cluster_wait_events'::regclass;
SELECT 'pg_stat_gcluster_wait_events'::regclass;
SELECT 'pg_cluster_nodes'::regclass;


-- ----------
-- 2. View column contracts -- locked from 0.16 / 0.17 / 0.19.
--    attnum ORDER fixes column ordering; format_type masks
--    type oid drift across PG versions.
-- ----------
SELECT attname, format_type(atttypid, atttypmod)
  FROM pg_attribute
 WHERE attrelid = 'pg_stat_cluster_wait_events'::regclass
   AND attnum > 0 AND NOT attisdropped
 ORDER BY attnum;

SELECT attname, format_type(atttypid, atttypmod)
  FROM pg_attribute
 WHERE attrelid = 'pg_stat_gcluster_wait_events'::regclass
   AND attnum > 0 AND NOT attisdropped
 ORDER BY attnum;

SELECT attname, format_type(atttypid, atttypmod)
  FROM pg_attribute
 WHERE attrelid = 'pg_cluster_nodes'::regclass
   AND attnum > 0 AND NOT attisdropped
 ORDER BY attnum;


-- ----------
-- 3. Cluster wait events: 78 rows (anchored by
--    CLUSTER_WAIT_EVENTS_COUNT, spec-0.11 + StaticAssertDecl
--    in cluster_views.c).
-- ----------
SELECT count(*) FROM pg_stat_cluster_wait_events;


-- ----------
-- 4. Cluster wait events: 13 distinct types
--    (docs/wait-events-design.md §2.1 categories:
--    GES / PCM / BufferShip / SCN / Reconfig / Recovery /
--    Sinval / Interconnect / Undo / ADG / SharedFs / StartupPhase).
-- ----------
SELECT count(DISTINCT type) FROM pg_stat_cluster_wait_events;


-- ----------
-- 5. Global view returns same row count as local at 0.17
--    (single-node placeholder; Stage 6+ AD-007 fan-out will
--    multiply this by N).
-- ----------
SELECT count(*) FROM pg_stat_gcluster_wait_events;


-- ----------
-- 6. pg_cluster_nodes returns 1 row in single-node degraded
--    mode (0.19 fallback when pgrac.conf is absent, which is
--    the case under pg_regress's --temp-instance).
-- ----------
SELECT count(*) FROM pg_cluster_nodes;


-- ----------
-- 7. Cluster GUC metadata -- vartype + context are part of the
--    public contract (PGC_POSTMASTER vs PGC_USERSET etc shapes
--    operator workflow).  setting/min/max omitted because dev
--    overrides shift them.
-- ----------
SELECT name, vartype, context
  FROM pg_settings
 WHERE name IN ('cluster.node_id',
                'cluster.interconnect_tier',
                'cluster.config_file')
 ORDER BY name;


-- ----------
-- 8. cluster.interconnect_tier default = 'stub' (boot value
--    fixed in cluster_guc.c at 0.18; safe to assert because
--    this GUC has no override path under --temp-instance).
-- ----------
SELECT setting FROM pg_settings WHERE name = 'cluster.interconnect_tier';
