/*-------------------------------------------------------------------------
 *
 * cluster_writer_chain.c
 *	  Cross-node terminal-writer chaining: pure outcome -> TM_Result mapping.
 *
 *	  Split out as a pure policy file (mirroring cluster_visibility_verdict.c)
 *	  so the spec-7.1a D0 truth table carries ZERO PG-backend dependency and
 *	  is unit-enumerable.  The heapam.c writer-wait bridge is the only
 *	  production caller; it resolves the remote writer's terminal outcome
 *	  from cluster TT / live-IC verdict authority and then maps it here.
 *
 *	  Fail-closed discipline: this function can only ever LOOSEN nothing.
 *	  Any outcome that is not affirmatively provable maps to "not mappable"
 *	  (false), and the caller raises the pre-existing 53R9H floor.  It never
 *	  fabricates failure data: on the false path res/tmfd are left unwritten.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_writer_chain.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-7.1a-cross-instance-write-write-mvcc-coordination.md.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/transam.h"
#include "cluster/cluster_writer_chain.h"

/*
 * cluster_writer_chain_decide -- map a resolved terminal-writer outcome to
 *	the TM_Result the native caller contract expects.
 *
 * Inputs:
 *	wo:			the authoritatively-resolved outcome (never trusted beyond
 *				its own consistency: UPDATED without a probed next version
 *				is NOT mappable)
 *	old_t_ctid:	the old version's on-page t_ctid, read under the reacquired
 *				buffer content lock (self tid for DELETE)
 *	update_xid:	the raw update xid of the old version (== the xwait the
 *				caller waited on; the multixact branch fails closed earlier)
 *	res:		out: TM_Ok / TM_Updated / TM_Deleted
 *	tmfd:		out, optional: failure data filled per the native contract
 *				(heap_delete / heap_update / heap_lock_tuple fill sites):
 *				ctid = chase target, xmax = update_xid,
 *				cmax = InvalidCommandId (native value for any failure that
 *				is not TM_SelfModified; a remote writer can never be
 *				self-modified).  NULL when the caller reuses the native
 *				fill block.  Never written for TM_Ok.
 *
 * Returns:
 *	true  -- outcome mapped; res (and tmfd when given and not TM_Ok) valid
 *	false -- outcome not soundly mappable; caller must fail closed (53R9H)
 *	         and must not consume res/tmfd (left unwritten)
 */
bool
cluster_writer_chain_decide(const ClusterWriterOutcome *wo, const ItemPointerData *old_t_ctid,
							TransactionId update_xid, TM_Result *res, TM_FailureData *tmfd)
{
	switch (wo->kind) {
	case CWO_ABORTED:
		/* Row unchanged; caller rechecks and proceeds.  No failure data. */
		*res = TM_Ok;
		return true;

	case CWO_DELETED:
		if (!TransactionIdIsValid(update_xid))
			return false;
		*res = TM_Deleted;
		if (tmfd != NULL) {
			tmfd->ctid = *old_t_ctid;
			tmfd->xmax = update_xid;
			tmfd->cmax = InvalidCommandId;
		}
		return true;

	case CWO_UPDATED:
		/*
			 * TM_Updated is only sound when the single-hop next version was
			 * actually probed (spec-7.1a Q9): a chase target the local node
			 * cannot reach or judge must fail closed, never be surfaced to
			 * EvalPlanQual.
			 */
		if (!TransactionIdIsValid(update_xid) || wo->new_tuple == NULL
			|| !ItemPointerIsValid(&wo->new_ctid))
			return false;
		*res = TM_Updated;
		if (tmfd != NULL) {
			tmfd->ctid = wo->new_ctid;
			tmfd->xmax = update_xid;
			tmfd->cmax = InvalidCommandId;
		}
		return true;

	case CWO_UNRESOLVABLE:
		return false;
	}

	/* Unreachable; keep the compiler's enum-coverage check honest. */
	return false;
}

#endif /* USE_PGRAC_CLUSTER */
