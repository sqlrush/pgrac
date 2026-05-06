-- ============================================================
-- undo_tablespace -- pgrac Stage 1.22 cluster surface regression.
--
--	Locks the catalog row + GUC default introduced by spec-1.22
--	atomic batch.  Output must be deterministic across host /
--	port / OS.  See specs/spec-1.22-undo-tablespace-bootstrap.md
--	§D12 + spec-0.21-cluster-regress.md §2.2.
--
--	Author: SqlRush <sqlrush@gmail.com>
--	Portions Copyright (c) 2026, pgrac contributors
-- ============================================================

-- ----------
-- 1. pg_undo entry exists in pg_tablespace with OID 9100.
--    (Spec-1.22 v0.2 Hardening v1.0.2: 1665 conflicted with
--    pg_get_serial_sequence in pg_proc.dat; fallback per §6 R2.)
-- ----------
SELECT oid, spcname FROM pg_tablespace WHERE spcname = 'pg_undo';


-- ----------
-- 2. Three system tablespaces total: pg_default + pg_global + pg_undo.
--    Vanilla PG has 2; spec-1.22 adds the third.
-- ----------
SELECT count(*) FROM pg_tablespace;


-- ----------
-- 3. pg_tablespace_location(9100) returns '' (empty string).
--    D14b + Hardening v1.0.3 P2-B special case: pg_undo follows the
--    pg_default/pg_global PG convention for system-internal tablespaces
--    (return empty string, not the literal path).  Without this special
--    case the call would readlink(pg_tblspc/9100) which doesn't exist.
-- ----------
SELECT pg_tablespace_location(9100);


-- ----------
-- 4. cluster.undo_segments_per_instance GUC default = 16
--    (D7; reserved per-instance segment count).
-- ----------
SELECT setting, vartype, context
  FROM pg_settings
 WHERE name = 'cluster.undo_segments_per_instance';
