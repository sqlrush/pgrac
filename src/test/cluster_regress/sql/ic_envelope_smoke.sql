-- ============================================================
-- ic_envelope_smoke -- spec-2.3 D12 envelope/router SQL surface smoke.
--
--	Verifies the spec-2.3 SQL surface in stub-tier mode (default for
--	cluster_regress):
--	- 3 spec-2.2 D7 PGC_POSTMASTER GUCs still exist (unchanged by
--	  spec-2.3 envelope pivot;regression check that wire change
--	  did not perturb the GUC layer)
--	- pg_cluster_ic_msg_types view exists + returns >= 1 row
--	  (HEARTBEAT registered at postmaster phase 1 by
--	  cluster_lmon_shmem_init regardless of tier)
--	- View has the spec-2.3 §2.6 frozen 5-column layout
--	- HEARTBEAT row metadata frozen: msg_type=1, name='heartbeat',
--	  handler_present=true, broadcast_ok=false
--
--	Behavioural envelope round-trip + bytes_send 36-byte frame
--	tests live in cluster_tap/t/081_envelope_round_trip.pl because
--	they require ClusterPair 2-postmaster orchestration, which
--	cluster_regress cannot do.
--
--	Author: SqlRush <sqlrush@gmail.com>
--	Portions Copyright (c) 2026, pgrac contributors
-- ============================================================

-- ----------
-- 1. 3 spec-2.2 D7 PGC_POSTMASTER GUCs still exist (regression
--	check that spec-2.3 envelope pivot did not perturb GUCs).
-- ----------
SELECT name, vartype, context
  FROM pg_settings
 WHERE name IN ('cluster.interconnect_heartbeat_interval_ms',
                'cluster.interconnect_connect_timeout_ms',
                'cluster.interconnect_recv_timeout_ms')
 ORDER BY name;


-- ----------
-- 2. pg_cluster_ic_msg_types view exists + returns >= 1 row.
--	HEARTBEAT registration happens at postmaster phase 1 in
--	cluster_lmon_shmem_init regardless of cluster.interconnect_tier
--	(stub / tier1).
-- ----------
SELECT count(*) >= 1 AS has_msg_types FROM pg_cluster_ic_msg_types;


-- ----------
-- 3. View has the spec-2.3 §2.6 frozen 5-column layout.
-- ----------
SELECT count(*) AS column_count
  FROM information_schema.columns
 WHERE table_name = 'pg_cluster_ic_msg_types';


-- ----------
-- 4. HEARTBEAT row metadata frozen.
-- ----------
SELECT msg_type, name, handler_present, broadcast_ok
  FROM pg_cluster_ic_msg_types
 WHERE msg_type = 1;


-- ----------
-- 5. spec-2.4 D9 NEW 5 PGC_POSTMASTER GUCs (chunked + KeepAlive).
-- ----------
SELECT name, vartype, context
  FROM pg_settings
 WHERE name IN ('cluster.interconnect_payload_max_bytes',
                'cluster.interconnect_chunk_reassembly_timeout_ms',
                'cluster.interconnect_tcp_keepidle_sec',
                'cluster.interconnect_tcp_keepintvl_sec',
                'cluster.interconnect_tcp_keepcnt')
 ORDER BY name;


-- ----------
-- 6. spec-2.4 D11 pg_cluster_ic_peers view extended to 23 columns
--	(was 19 in spec-2.2 D9).  4 NEW columns: stale_epoch_drop_count /
--	chunk_reassembly_active / chunk_reassembly_timeout_count /
--	lamport_observe_advance_count.
-- ----------
SELECT count(*) AS column_count
  FROM information_schema.columns
 WHERE table_name = 'pg_cluster_ic_peers';
