/*-------------------------------------------------------------------------
 *
 * cluster_writer_chain.h
 *	  Public interface for cross-node terminal-writer chaining (write-write).
 *
 *	  When a local DML (heap_update / heap_delete / heap_lock_tuple) runs
 *	  into a REMOTE writer holder that is already terminal, the pre-7.1a
 *	  bridge unconditionally failed closed (53R9H).  This module maps the
 *	  authoritatively-resolved terminal outcome onto the sound TM_Result
 *	  the native caller contract expects:
 *
 *	    remote UPDATE committed  -> TM_Updated  (chain to the next version)
 *	    remote DELETE committed  -> TM_Deleted
 *	    remote writer aborted    -> TM_Ok       (row unchanged; recheck applies)
 *	    outcome not provable     -> not mappable; caller MUST fail closed
 *
 *	  The decide function is pure (no buffer, no page, no backend state)
 *	  so the full truth table is unit-enumerable.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_writer_chain.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-7.1a-cross-instance-write-write-mvcc-coordination.md.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_WRITER_CHAIN_H
#define CLUSTER_WRITER_CHAIN_H

#include "access/htup.h"
#include "access/tableam.h"
#include "storage/itemptr.h"

/*
 * Authoritatively-resolved terminal outcome of a remote writer holder.
 * The resolver (heapam.c bridge) only produces CWO_UPDATED / CWO_DELETED
 * when the outcome is proven from cluster TT / live-IC verdict authority;
 * anything unproven is CWO_UNRESOLVABLE (fail-closed, never guessed).
 */
typedef enum ClusterWriterOutcomeKind {
	CWO_UPDATED = 0, /* remote UPDATE committed: next version exists */
	CWO_DELETED,	 /* remote DELETE committed: row is gone */
	CWO_ABORTED,	 /* remote writer aborted: row unchanged */
	CWO_UNRESOLVABLE /* not provable: caller must fail closed (53R9H) */
} ClusterWriterOutcomeKind;

typedef struct ClusterWriterOutcome {
	ClusterWriterOutcomeKind kind;
	ItemPointerData new_ctid; /* CWO_UPDATED: single-hop next-version tid */
	HeapTuple new_tuple;	  /* CWO_UPDATED: probe descriptor of the next
								 * version (t_self / t_tableOid set; t_data
								 * NULL -- EvalPlanQual re-fetches the
								 * version itself); NULL = not probed */
} ClusterWriterOutcome;

/*
 * Pure mapping from a resolved outcome to the caller-visible TM_Result and
 * TM_FailureData, mirroring the native failure-exit contract exactly
 * (heap_delete / heap_update / heap_lock_tuple fill sites): tmfd->ctid,
 * tmfd->xmax = the old version's update xid, tmfd->cmax = InvalidCommandId
 * (the native value for any not-self-modified failure; a remote writer can
 * never be TM_SelfModified).  Returns false when the outcome cannot be
 * soundly mapped (CWO_UNRESOLVABLE, missing next version, invalid update
 * xid); the caller must then fail closed and MUST NOT consume res/tmfd.
 * tmfd may be NULL when the caller fills failure data itself (the heapam
 * callers reuse the native fill blocks); on TM_Ok tmfd is never written.
 */
extern bool cluster_writer_chain_decide(const ClusterWriterOutcome *wo,
										const ItemPointerData *old_t_ctid, TransactionId update_xid,
										TM_Result *res, TM_FailureData *tmfd);

#endif /* CLUSTER_WRITER_CHAIN_H */
