/*-------------------------------------------------------------------------
 *
 * cluster_node_remove_views.c
 *	  pgrac online node leave: fence + cluster-wide cleanup (spec-5.18) —
 *	  catalog-facing SQL.
 *
 *	  The operator entry point + observability SRF for permanent node removal
 *	  (D15 / D16):
 *	    - cluster_get_node_removal_state(): the always-1-row removal-progress SRF
 *	      backing the pg_cluster_node_removal_state view (phase / target_node_id /
 *	      coordinator_node_id / remove_epoch / fence_armed / membership_shrunk /
 *	      grd_cleaned / pcm_cleaned / ack_count / deadline + lifetime counters).
 *	      read-only -> public (the view GRANTs SELECT).
 *	    - pg_cluster_remove_node(int): the operator UDF that permanently removes a
 *	      declared node.  Mutating -> superuser-only (the C superuser() gate +
 *	      REVOKE EXECUTE FROM PUBLIC in system_views.sql, L7).  Returns a text
 *	      status from the behaviour matrix: accepted / noop:already_removed /
 *	      resume:cleanup_pending / rejected:<reason>.
 *
 *	  Kept separate from the runtime cluster_node_remove.c so this file can be
 *	  linked unconditionally (the pg_proc.dat entries are unconditional, so the
 *	  fmgr symbols must resolve in --disable-cluster builds too); the real bodies
 *	  are #ifdef USE_PGRAC_CLUSTER and the disable-mode stubs raise
 *	  ERRCODE_FEATURE_NOT_SUPPORTED — the same pattern as cluster_clean_leave_views.c.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_node_remove_views.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linked in both build modes (SRF/UDF
 *	  symbols referenced unconditionally by pg_proc.dat); the bodies are
 *	  --enable-cluster only.
 *	  Spec: spec-5.18-online-node-leave-fence-cleanup.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"		/* superuser() */
#include "utils/builtins.h" /* cstring_to_text */

#include "cluster/cluster_guc.h" /* cluster_enabled */
#include "cluster/cluster_node_remove.h"

PG_FUNCTION_INFO_V1(cluster_get_node_removal_state);
PG_FUNCTION_INFO_V1(pg_cluster_remove_node);

#ifdef USE_PGRAC_CLUSTER

/* count set bits in the survivor-ack bitmap (ack_count column). */
static int
nr_views_popcount(const uint8 *bmp, int nbytes)
{
	int i, n = 0;

	for (i = 0; i < nbytes; i++) {
		uint8 b = bmp[i];

		while (b) {
			n += (b & 1);
			b >>= 1;
		}
	}
	return n;
}

/*
 * cluster_get_node_removal_state -- always-1-row removal progress (D15).
 * cluster.enabled=off returns 0 rows (distinguishes "feature off" from "on, idle"
 * like pg_cluster_reconfig_state).  Idle surfaces as phase='idle',
 * target_node_id=-1, deadline_us NULL.
 */
Datum
cluster_get_node_removal_state(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ClusterNodeRemoveState st;
	Datum values[14];
	bool nulls[14];

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (!cluster_enabled)
		return (Datum)0; /* disabled — 0 rows */

	cluster_node_remove_get_state(&st); /* consistent snapshot under the LWLock */

	memset(nulls, false, sizeof(nulls));
	values[0] = PointerGetDatum(
		cstring_to_text(cluster_node_remove_phase_str((int)pg_atomic_read_u32(&st.phase))));
	values[1] = Int32GetDatum(st.target_node_id);
	values[2] = Int32GetDatum(st.coordinator_node_id);
	values[3] = Int64GetDatum((int64)st.remove_epoch);
	values[4] = BoolGetDatum(st.fence_armed);
	values[5] = BoolGetDatum(st.membership_shrunk);
	values[6] = BoolGetDatum(st.grd_cleaned);
	values[7] = BoolGetDatum(st.pcm_cleaned);
	values[8]
		= Int32GetDatum(nr_views_popcount(st.ack_bitmap, CLUSTER_NODE_REMOVE_ACK_BITMAP_BYTES));
	if (st.cleanup_deadline_us == 0)
		nulls[9] = true; /* idle / pre-cleanup — no deadline */
	else
		values[9] = Int64GetDatum((int64)st.cleanup_deadline_us);
	values[10] = Int64GetDatum((int64)pg_atomic_read_u64(&st.removal_committed_count));
	values[11] = Int64GetDatum((int64)pg_atomic_read_u64(&st.cleanup_blocked_count));
	values[12] = Int64GetDatum((int64)pg_atomic_read_u64(&st.leftover_detected_count));
	values[13] = Int64GetDatum((int64)pg_atomic_read_u64(&st.zombie_write_rejected_count));

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum)0;
}

/*
 * pg_cluster_remove_node(node_id int) -- operator entry: permanently remove a
 * declared node (D16).  Superuser-only (mutating, L7).  Maps the C driver's
 * ClusterRemoveRequestResult to the behaviour-matrix text.
 */
Datum
pg_cluster_remove_node(PG_FUNCTION_ARGS)
{
	int32 node_id = PG_GETARG_INT32(0);

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to remove a cluster node")));
	if (!cluster_enabled)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster.enabled is off; node removal is not available")));

	PG_RETURN_TEXT_P(
		cstring_to_text(cluster_node_remove_request_result_str(cluster_node_remove_request(node_id))));
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_get_node_removal_state(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_get_node_removal_state requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
pg_cluster_remove_node(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_remove_node requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
