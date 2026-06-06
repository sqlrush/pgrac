/*-------------------------------------------------------------------------
 *
 * cluster_tt_2pc.c
 *	  pgrac cluster TT state in two-phase commit (spec-3.15).
 *
 *	  Layering (L212): the serialize/parse pair below is pure (no
 *	  allocation, no ereport) and cluster_unit-enumerable; the
 *	  AtPrepare/PostPrepare shells and the twophase callbacks stay thin
 *	  and never hand-copy the record layout.
 *
 *	  Step 2 scope: record serialize/parse + AtPrepare/PostPrepare
 *	  shells.  The rmgr callbacks (recover/postcommit/postabort) land at
 *	  step 3; the prefinish resolve lands at steps 6-7.  Until step 4
 *	  wires the shells into xact.c, nothing here is reachable (the
 *	  spec-3.5/3.7 PREPARE guards stay in place -- C-P1).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tt_2pc.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-3.15-2pc-prepared-visibility.md (FROZEN v0.2).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/twophase.h"
#include "access/twophase_rmgr.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_itl_touch.h" /* PostPrepare touch-list drop (V-2) */
#include "cluster/cluster_subtrans.h"  /* sub-link export/reset (D7) */
#include "cluster/cluster_tt_2pc.h"
#include "cluster/cluster_tt_local.h" /* binding export/reset */


/*
 * AtPrepare_ClusterTT -- spec-3.15 D1/D3 (PREPARE serialize-only shell).
 *
 *	PG two-phase contract: AtPrepare routines may error out but MUST NOT
 *	mutate backend-local state (an EndPrepare failure aborts the xact,
 *	and the abort path still needs the bindings to release shmem slots).
 *	State transfer happens in PostPrepare_ClusterTT after EndPrepare
 *	succeeded.
 *
 *	Read-only / zero-cluster-state PREPARE registers nothing (Q5).
 */
void
AtPrepare_ClusterTT(void)
{
	ClusterTT2PCBinding bindings[CLUSTER_TT_2PC_MAX_BINDINGS];
	ClusterTT2PCSubLink sublinks[CLUSTER_TT_2PC_MAX_SUBLINKS];
	uint16 nbindings;
	uint32 nsublinks;
	uint32 len;
	char *buf;

	if (!cluster_enabled || cluster_node_id < 0)
		return;

	nbindings = cluster_tt_local_export_bindings(bindings, CLUSTER_TT_2PC_MAX_BINDINGS + 1);
	nsublinks = cluster_subtrans_export_links(sublinks, CLUSTER_TT_2PC_MAX_SUBLINKS + 1);

	if (nbindings == 0 && nsublinks == 0)
		return; /* Q5: nothing to carry */

	/*
	 * §1.4-4 capacity: export functions return count+1 saturated when the
	 * backend-local state exceeds the cap, which serialize() rejects --
	 * surface it as a clean PREPARE failure, not silent truncation.
	 */
	len = cluster_tt_2pc_record_size(Min(nbindings, CLUSTER_TT_2PC_MAX_BINDINGS),
									 Min(nsublinks, CLUSTER_TT_2PC_MAX_SUBLINKS));
	buf = palloc(len);
	if (cluster_tt_2pc_serialize(bindings, nbindings, sublinks, nsublinks, buf, len) == 0)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("cannot PREPARE a transaction with more than %d cluster TT bindings "
							   "or %d subtransaction links",
							   CLUSTER_TT_2PC_MAX_BINDINGS, CLUSTER_TT_2PC_MAX_SUBLINKS),
						errhint("Split the transaction, or resolve it without two-phase commit.")));

	RegisterTwoPhaseRecord(TWOPHASE_RM_CLUSTER_TT_ID, 0, buf, len);
	pfree(buf);
}


/*
 * PostPrepare_ClusterTT -- ownership transfer after EndPrepare succeeded.
 *
 *	The 2PC record is now the single authority (C-P4): drop the backend-
 *	local TT bindings, the SUBCOMMITTED link list, and the ITL touch
 *	list (V-2: droppable -- overlay/durable TT are authoritative and the
 *	3.4c lazy cleanout re-stamps page ITLs on later reads).  Nothing
 *	here touches shmem or durable state.
 */
void
PostPrepare_ClusterTT(void)
{
	if (!cluster_enabled || cluster_node_id < 0)
		return;

	cluster_tt_local_reset_binding();
	cluster_subtrans_reset_local_links();
	cluster_itl_touch_reset_at_end_xact();
}

#endif /* USE_PGRAC_CLUSTER */
