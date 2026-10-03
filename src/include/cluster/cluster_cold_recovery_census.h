/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_census.h
 *	  Complete participant census of typed cold-crash replay.
 *
 *	  When every instance has failed, the founder must take part in every
 *	  writer generation that still has retained WAL, not only the crashed
 *	  generations the recovery fence plan names: older generations of each
 *	  thread kept in its ROOT history, and closed or already recovered
 *	  generations whose retained range still precedes later changes.  Those
 *	  are history only: every retained record of theirs is durable and may
 *	  only prove page ancestry.
 *
 *	  The selection works on the inputs of one cold read scope
 *	  (cluster_wal_inputs_cold_begin_v1) and is pure; the startup adapter
 *	  owns the scope, its pins and its release.  Implemented with the other
 *	  pass-1 I/O in cluster_cold_recovery_io.c.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cold_recovery_census.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_COLD_RECOVERY_CENSUS_H
#define CLUSTER_COLD_RECOVERY_CENSUS_H

#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_wal_inputs.h"

typedef enum ClusterColdCensusDetailV1 {
	CLUSTER_COLD_CENSUS_OK = 0,
	CLUSTER_COLD_CENSUS_INVALID_ARGUMENT = 1,
	CLUSTER_COLD_CENSUS_LIVE_WRITER = 2, /* a current generation is still OPEN */
	CLUSTER_COLD_CENSUS_LIFECYCLE = 3,	 /* a lifecycle the census does not know */
	CLUSTER_COLD_CENSUS_TERMINAL = 4,	 /* initializer terminal with records: no cold owner */
	CLUSTER_COLD_CENSUS_RANGE = 5,		 /* unvalidated tail or bounds out of order */
	CLUSTER_COLD_CENSUS_UNCOVERED = 6,	 /* crashed generation neither founder nor fence origin */
	CLUSTER_COLD_CENSUS_ORIGIN_MISSING = 7, /* a fence origin is not a crashed generation */
	CLUSTER_COLD_CENSUS_CAPACITY = 8
} ClusterColdCensusDetailV1;

typedef enum ClusterColdCensusRoleV1 {
	CLUSTER_COLD_CENSUS_REPLAY = 1, /* crashed (RECOVERY_REQUIRED) current generation */
	CLUSTER_COLD_CENSUS_HISTORY = 2 /* retained range is history only */
} ClusterColdCensusRoleV1;

/* One generation the founder takes part in.  A history entry's cut ends its
 * history at its tail (native_redo == tail_end): nothing of it is replayed. */
typedef struct ClusterColdCensusEntryV1 {
	uint32 input_index; /* in the cold read scope */
	uint8 role;			/* ClusterColdCensusRoleV1 */
	ClusterColdParticipantV1 cut;
	ClusterControlRootIdentity identity;
	ClusterWalSourceRef source;
} ClusterColdCensusEntryV1;

/*
 * Select every generation with retained WAL from the scope's inputs, in
 * input order.  A current OPEN generation is refused (a live writer, or a
 * crashed one nobody sealed); a current RECOVERY_REQUIRED generation is a
 * replay entry; history records and closed or recovered current
 * generations with a non-empty retained range are history entries, empty
 * ones are left out; an initializer terminal is left out only when empty.
 * On refusal *bad_index names the input.
 */
extern ClusterColdCensusDetailV1
cluster_cold_census_select_v1(const ClusterWalInputV1 *const *inputs, uint32 count,
							  ClusterColdCensusEntryV1 *entries, uint32 capacity, uint32 *out_count,
							  uint32 *bad_index);

/*
 * The replay entries must be exactly the founder's generation and the
 * fence plan's origins (same identity, crashed): a crashed generation the
 * plan leaves out would be skipped entirely.  crashed[] holds the founder's
 * root first, then each fence origin's root.  On refusal *bad_thread names
 * the thread.
 */
extern ClusterColdCensusDetailV1
cluster_cold_census_cover_v1(const ClusterColdCensusEntryV1 *entries, uint32 count,
							 const ClusterControlRootSnapshot *crashed, uint32 crashed_count,
							 uint16 *bad_thread);

/*
 * Pass-1 scan of one history-only generation: visit every retained record
 * of input `index` of the cold read scope and feed the plan as participant
 * `participant`, as cluster_cold_scan_root_v1 does for a crashed root.
 * Accepted only when the visit and its observed cut (record count, complete
 * end) match the input's ROOT record.
 */
extern ClusterColdDetailV1 cluster_cold_scan_input_v1(ClusterColdPlanV1 *plan, uint32 participant,
													  ClusterWalInputsV1 *inputs, uint32 index,
													  bool space_active, bool foreign,
													  ClusterColdScanResultV1 *result);

#endif /* CLUSTER_COLD_RECOVERY_CENSUS_H */
