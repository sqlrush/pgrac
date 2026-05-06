-- ============================================================
-- conf_multi_node_activation -- spec-2.1 GUC + boundary contract.
--
--	Verifies the SQL surface of cluster.allow_single_node:
--	- GUC exists with the documented vartype / context / default
--	- pg_cluster_nodes view still works in single-node fallback
--	  mode (regression for spec-0.16 / 0.19 columns)
--	- cluster.node_id and cluster.allow_single_node both PGC_POSTMASTER
--
--	Behavioural FATAL paths (allow=off + conf absent etc.) live in
--	cluster_tap t/072_pgrac_conf_multi_node_activation.pl because
--	they require server restart, which cluster_regress cannot do.
--
--	Author: SqlRush <sqlrush@gmail.com>
--	Portions Copyright (c) 2026, pgrac contributors
-- ============================================================

-- ----------
-- 1. cluster.allow_single_node GUC exists with expected metadata.
-- ----------
SELECT name, vartype, context, boot_val
  FROM pg_settings
 WHERE name = 'cluster.allow_single_node';


-- ----------
-- 2. Default value at runtime is on (Stage 2.1 backward-compat).
-- ----------
SHOW "cluster.allow_single_node";


-- ----------
-- 3. Runtime SET is rejected (PGC_POSTMASTER).
-- ----------
SET "cluster.allow_single_node" = off;


-- ----------
-- 4. cluster.node_id is also PGC_POSTMASTER (regression spec-0.13).
-- ----------
SELECT context FROM pg_settings WHERE name = 'cluster.node_id';


-- ----------
-- 5. pg_cluster_nodes view still has 7 columns in single-node
--    fallback mode (regression spec-0.19 column contract).
-- ----------
SELECT attname, format_type(atttypid, atttypmod)
  FROM pg_attribute
 WHERE attrelid = 'pg_cluster_nodes'::regclass
   AND attnum > 0 AND NOT attisdropped
 ORDER BY attnum;
