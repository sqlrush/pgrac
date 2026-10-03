/*-------------------------------------------------------------------------
 *
 * cluster_wal_retained_cut.h
 *	  Per-thread physical retention lower computed from the complete
 *	  retained WAL input (S07).
 *
 *	  Every selected source has a completion position: the records ending
 *	  at or before it are durable history, the rest are recovery
 *	  obligations.  A thread may stop retaining its history up to the
 *	  earliest record that another obligation still needs as ancestry.
 *	  One census over the complete input finds that bound with fixed
 *	  memory; it publishes nothing.  The ROOT owner publishes the bound
 *	  only if the ROOT is unchanged since the census.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_wal_retained_cut.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_WAL_RETAINED_CUT_H
#define CLUSTER_WAL_RETAINED_CUT_H

#include "access/xlogdefs.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_page_detached.h"
#include "cluster/cluster_wal_source.h"

/* What kept the lower below this thread's completion. */
typedef enum ClusterWalRetainedPinV1 {
	CLUSTER_WAL_RETAINED_PIN_NONE = 0,
	/* A page or SPACE block with an obligation still needs this history. */
	CLUSTER_WAL_RETAINED_PIN_PAGE = 1,
	/* A SIDE owner class without per-key ancestry had an obligation. */
	CLUSTER_WAL_RETAINED_PIN_SIDE = 2
} ClusterWalRetainedPinV1;

typedef struct ClusterWalRetainedCutV1 {
	/* Whole ROOT file the input was selected and revalidated under. */
	ClusterControlRootFileToken root_token;
	/* The current writer generation's published lower and native redo. */
	XLogRecPtr old_lower;
	XLogRecPtr native_redo;
	/* Proposed lower: old_lower <= lower <= native_redo, a record start. */
	XLogRecPtr lower;
	uint64 records;
	uint64 history_edges;
	uint64 retained_edges;
	/* SIDE contribution owner bits whose obligations pinned history. */
	uint32 side_classes;
	ClusterWalRetainedPinV1 pin;
} ClusterWalRetainedCutV1;

/*
 * Checkpointer (or bgwriter) only, outside CF.  Select the complete retained
 * input, census every record once and return the bound for this node's
 * current writer generation.  Live OPEN ends follow the original writer;
 * a peer cut that is not confirmed yet returns RECONFIG_WAIT.  On any refusal
 * the output is zeroed and detail names a PAGE/SIDE decode refusal.
 * Publishing is a separate ROOT operation that must compare root_token.
 */
extern ClusterControlRootResult cluster_wal_retained_cut_compute_v1(const ClusterWalSourceRef *self,
																	ClusterWalRetainedCutV1 *out,
																	RfPageProofDetailV1 *detail);

/*
 * Checkpointer, after its own online checkpoint is published and before WAL
 * cleanup: compute the bound and, when it advances, publish it through the
 * ROOT owner.  Every refusal keeps the published lower (nothing is retired
 * without proof) and is logged once per distinct reason.
 */
extern void cluster_wal_retained_cut_after_checkpoint_v1(void);

#endif /* CLUSTER_WAL_RETAINED_CUT_H */
