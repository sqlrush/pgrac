/*-------------------------------------------------------------------------
 *
 * cluster_clean_leave_views.c
 *	  pgrac clean leave reconfiguration (spec-5.13) — catalog-facing SQL.
 *
 *	  The operator entry point + observability SRF for clean leave (D13 / D13b):
 *	    - cluster_get_clean_leave_state(): the always-1-row drain-progress SRF
 *	      backing the pg_cluster_clean_leave_state view (phase / leaving_node_id /
 *	      leave_epoch / ges_drained / gcs_flushed / shards_remastered /
 *	      survivor_ack_count / barrier_deadline / escalate_count).  read-only →
 *	      public (the view GRANTs SELECT).
 *	    - pg_cluster_clean_leave_request(): the operator UDF that asks THIS node to
 *	      leave the cluster cooperatively.  Mutating → superuser-only (the C
 *	      superuser() gate + REVOKE EXECUTE FROM PUBLIC in system_views.sql, L7).
 *	      Returns a text status from the behaviour matrix: accepted /
 *	      rejected:<reason> / noop:<reason>.
 *
 *	  Kept separate from the runtime cluster_clean_leave.c so this file can be
 *	  linked unconditionally (the pg_proc.dat entries are unconditional, so the
 *	  fmgr symbols must resolve in --disable-cluster builds too); the real bodies
 *	  are #ifdef USE_PGRAC_CLUSTER and the disable-mode stubs raise
 *	  ERRCODE_FEATURE_NOT_SUPPORTED — the same pattern as cluster_hang_resolve.c.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_clean_leave_views.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linked in both build modes (SRF/UDF
 *	  symbols referenced unconditionally by pg_proc.dat); the bodies are
 *	  --enable-cluster only.
 *	  Spec: spec-5.13-clean-leave-reconfig.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"		/* superuser() */
#include "utils/builtins.h" /* cstring_to_text */

#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_guc.h" /* cluster_enabled */

PG_FUNCTION_INFO_V1(cluster_get_clean_leave_state);
PG_FUNCTION_INFO_V1(pg_cluster_clean_leave_request);

#ifdef USE_PGRAC_CLUSTER

#include "utils/timestamp.h" /* TimestampTz + TimestampTzGetDatum */

/* count set bits in the survivor-ack bitmap (survivor_ack_count column). */
static int
cl_views_popcount(const uint8 *bmp, int nbytes)
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
 * cluster_get_clean_leave_state -- always-1-row clean-leave drain progress (D13).
 * cluster.enabled=off returns 0 rows (distinguishes "feature off" from "on, idle"
 * like pg_cluster_reconfig_state).  Idle surfaces as phase='idle',
 * leaving_node_id=-1, barrier_deadline NULL.
 */
Datum
cluster_get_clean_leave_state(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ClusterLeaveState st;
	Datum values[9];
	bool nulls[9];

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (!cluster_enabled)
		return (Datum)0; /* disabled — 0 rows */

	cluster_clean_leave_get_state(&st); /* consistent snapshot under the LWLock */

	memset(nulls, false, sizeof(nulls));
	values[0] = PointerGetDatum(
		cstring_to_text(cluster_clean_leave_phase_str((int)pg_atomic_read_u32(&st.phase))));
	values[1] = Int32GetDatum(st.leaving_node_id);
	values[2] = Int64GetDatum((int64)st.leave_epoch);
	values[3] = Int64GetDatum((int64)pg_atomic_read_u64(&st.ges_drained_count));
	values[4] = Int64GetDatum((int64)pg_atomic_read_u64(&st.gcs_flushed_count));
	values[5] = Int64GetDatum((int64)pg_atomic_read_u64(&st.shards_remastered));
	values[6]
		= Int32GetDatum(cl_views_popcount(st.ack_bitmap, CLUSTER_CLEAN_LEAVE_ACK_BITMAP_BYTES));
	if (st.barrier_deadline_us == 0)
		nulls[7] = true; /* idle — no deadline */
	else
		values[7] = TimestampTzGetDatum((TimestampTz)st.barrier_deadline_us);
	values[8] = Int64GetDatum((int64)pg_atomic_read_u64(&st.escalate_count));

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum)0;
}

/*
 * pg_cluster_clean_leave_request -- operator entry: ask THIS node to leave the
 * cluster cooperatively (D13b).  Superuser-only (mutating, L7).  Maps the C
 * driver's ClusterLeaveRequestResult to the behaviour-matrix text.
 */
Datum
pg_cluster_clean_leave_request(PG_FUNCTION_ARGS)
{
	const char *txt;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to request a cluster clean leave")));
	if (!cluster_enabled)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster.enabled is off; clean leave is not available")));

	switch (cluster_clean_leave_request()) {
	case CLUSTER_LEAVE_REQ_ACCEPTED:
		txt = "accepted";
		break;
	case CLUSTER_LEAVE_REQ_REJECTED_DISABLED:
		txt = "rejected:feature_disabled";
		break;
	case CLUSTER_LEAVE_REQ_REJECTED_NOT_IN_QUORUM:
		txt = "rejected:not_in_quorum";
		break;
	case CLUSTER_LEAVE_REQ_NOOP_NO_PEER:
		txt = "noop:no_peer";
		break;
	case CLUSTER_LEAVE_REQ_REJECTED_IN_PROGRESS:
		txt = "rejected:leave_in_progress";
		break;
	case CLUSTER_LEAVE_REQ_REJECTED_PEERS_NOT_ENABLED:
		txt = "rejected:peers_not_all_enabled";
		break;
	case CLUSTER_LEAVE_REQ_REJECTED_NOT_DURABLE:
		txt = "rejected:marker_not_durable";
		break;
	case CLUSTER_LEAVE_REQ_REJECTED_PREFLIGHT_INCOMPLETE:
		txt = "rejected:preflight_incomplete";
		break;
	default:
		txt = "rejected:unknown";
		break;
	}

	PG_RETURN_TEXT_P(cstring_to_text(txt));
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_get_clean_leave_state(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_get_clean_leave_state requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
pg_cluster_clean_leave_request(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_clean_leave_request requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
