/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery.h
 *	  Typed cold-crash replay plan over every retained writer generation.
 *
 *	  When every instance has failed, the founder replays all retained
 *	  writer generations.  The retained physical prefix [lower, native redo)
 *	  of each generation is history: it may only prove page ancestry and is
 *	  never replayed.  This header is the pure planning core used by the
 *	  first, read-only pass:
 *
 *	    - participants: exact (thread, owner incarnation) generations with
 *	      their physical lower, native redo start and validated tail;
 *	    - records: identity plus the ordinary-page PageVersion components
 *	      (expected-before -> result) decoded from each WAL record;
 *	    - seal: links every page's components into one exact version chain,
 *	      places the observed DATA version on that chain, assigns one
 *	      verdict per component and computes a deterministic dependency
 *	      schedule.  Any missing ancestor, branch, cycle, incarnation
 *	      mismatch, history gap or cross-generation deadlock is refused
 *	      before the caller mutates anything.
 *
 *	  The second pass replays records in schedule order and consults the
 *	  per-block verdicts.  Ordering never uses foreign LSNs or numeric
 *	  mutation tokens; xl_scn is only a deterministic tie-break among
 *	  records whose dependencies are already satisfied.
 *
 *	  This file holds no I/O, locks, authority or WAL access.  The backend
 *	  adapter supplies decoded records and DATA observations and owns all
 *	  isolation, retention and serialization.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cold_recovery.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_COLD_RECOVERY_H
#define CLUSTER_COLD_RECOVERY_H

#include "access/xlogrecord.h"
#include "cluster/cluster_page_stable_base.h"

#define CLUSTER_COLD_RECOVERY_INTERFACE_V1 1
#define CLUSTER_COLD_MAX_PARTICIPANTS RF_PAGE_STABLE_MAX_PARTICIPANTS
#define CLUSTER_COLD_MAX_COMPONENTS XLR_PAGE_VERSION_EDGE_MAX_ENTRIES
#define CLUSTER_COLD_NO_INDEX UINT32_MAX

typedef enum ClusterColdDetailV1 {
	CLUSTER_COLD_OK = 0,
	CLUSTER_COLD_INVALID_ARGUMENT = 1,
	CLUSTER_COLD_PARTICIPANT_INVALID = 2, /* bad cut or repeated generation */
	CLUSTER_COLD_SOURCE_GAP = 3,		  /* order, redo straddle, incomplete cut */
	CLUSTER_COLD_COMPONENT_INVALID = 4,	  /* malformed PageVersion component */
	CLUSTER_COLD_EDGE_BRANCH = 5,		  /* two successors of one version */
	CLUSTER_COLD_EDGE_CYCLE = 6,		  /* a version is produced twice or loops */
	CLUSTER_COLD_CHAIN_AMBIGUOUS = 7,	  /* second chain start at one address */
	CLUSTER_COLD_ANCESTOR_MISSING = 8,	  /* DATA version not on the chain */
	CLUSTER_COLD_INCARNATION_MISMATCH = 9,
	CLUSTER_COLD_HISTORY_GAP = 10, /* DATA behind a never-replayed record */
	CLUSTER_COLD_ANCHOR_MISSING = 11,
	CLUSTER_COLD_OBSERVATION_FAILED = 12,
	CLUSTER_COLD_DEADLOCK = 13,
	CLUSTER_COLD_CAPACITY = 14,
	CLUSTER_COLD_OOM = 15,
	CLUSTER_COLD_STATE = 16 /* wrong phase or already failed */
} ClusterColdDetailV1;

/* Exact physical cut of one writer generation, all from one ROOT token. */
typedef struct ClusterColdParticipantV1 {
	uint16 thread_id;
	uint16 reserved_zero;
	TimeLineID timeline;
	uint64 owner_incarnation;
	XLogRecPtr physical_lower; /* first retained record, inclusive */
	XLogRecPtr native_redo;	   /* native checkpoint redo of the same anchor */
	XLogRecPtr tail_end;	   /* validated complete end, exclusive */
} ClusterColdParticipantV1;

/* One ordinary page component of a decoded record. */
typedef struct ClusterColdComponentV1 {
	RfPageIdentityV1 page;
	uint8 block_id;
	uint8 page_class;  /* RF_PAGE_CLASS_ORDINARY */
	uint8 before_kind; /* RfPageStateKindV1 */
	uint8 result_kind; /* RF_PAGE_STATE_PRESENT */
	uint16 edge_flags; /* RF_PAGE_EDGE_* */
	uint16 component_ordinal;
	RfPageVersionV1 before;
	RfPageVersionV1 result;
} ClusterColdComponentV1;

/* Every decoded record of a participant is fed, in its LSN order.  Records
 * without ordinary page components only advance the participant cursor. */
typedef struct ClusterColdRecordV1 {
	XLogRecPtr read_rec_ptr;
	XLogRecPtr end_rec_ptr;
	uint64 scn; /* xl_scn; deterministic tie-break only */
	uint32 record_crc;
	uint8 rmid;
	uint8 info;
	uint16 component_count;
	const ClusterColdComponentV1 *components;
} ClusterColdRecordV1;

typedef enum ClusterColdDataKindV1 {
	CLUSTER_COLD_DATA_INVALID = 0, /* unreadable or failed verification */
	CLUSTER_COLD_DATA_PRESENT = 1,
	CLUSTER_COLD_DATA_UNFORMATTED = 2, /* all-zero page inside the file */
	CLUSTER_COLD_DATA_ABSENT = 3	   /* block beyond EOF or no file */
} ClusterColdDataKindV1;

/* Observed shared DATA state of one page.  PRESENT carries the exact
 * version; UNFORMATTED carries only the segment incarnation. */
typedef struct ClusterColdDataV1 {
	uint8 kind;
	uint8 reserved_zero[7];
	RfPageVersionV1 version;
} ClusterColdDataV1;

/* Read-only DATA observation.  False means the observation itself failed
 * (I/O, identity); the plan then refuses instead of guessing. */
typedef bool (*ClusterColdObserveV1)(void *arg, const RfPageIdentityV1 *page,
									 ClusterColdDataV1 *out);

typedef enum ClusterColdVerdictV1 {
	CLUSTER_COLD_BLOCK_NONE = 0, /* block is not an ordinary page component */
	CLUSTER_COLD_BLOCK_SKIP = 1, /* DATA already holds this result or a successor */
	CLUSTER_COLD_BLOCK_APPLY_DELTA = 2,
	CLUSTER_COLD_BLOCK_APPLY_IMAGE = 3,
	CLUSTER_COLD_BLOCK_APPLY_INIT = 4
} ClusterColdVerdictV1;

/* expected_kind INVALID on an APPLY_IMAGE/APPLY_INIT block means the anchor
 * replaces unreadable or unrelated DATA; otherwise the page must hold the
 * exact expected_before state before the block is applied. */
typedef struct ClusterColdBlockStepV1 {
	uint8 verdict;
	uint8 expected_kind; /* ClusterColdDataKindV1 */
	uint8 reserved_zero[6];
	RfPageVersionV1 expected_before;
	RfPageVersionV1 result;
} ClusterColdBlockStepV1;

typedef struct ClusterColdStepV1 {
	uint32 participant; /* caller's participant index */
	uint32 reserved_zero;
	XLogRecPtr read_rec_ptr;
	XLogRecPtr end_rec_ptr;
	uint32 record_crc;
	uint8 rmid;
	uint8 info;
	bool all_skip;
	bool mixed;
	ClusterColdBlockStepV1 blocks[XLR_MAX_BLOCK_ID + 1];
} ClusterColdStepV1;

typedef struct ClusterColdDiagV1 {
	uint8 detail; /* ClusterColdDetailV1 */
	bool has_record;
	bool has_page;
	bool has_dependency;
	uint32 participant; /* caller's participant index */
	XLogRecPtr read_rec_ptr;
	RfPageIdentityV1 page;
	RfPageVersionV1 version; /* offending DATA or expected version */
	uint32 dependency_participant;
	XLogRecPtr dependency_read_rec_ptr;
} ClusterColdDiagV1;

typedef struct ClusterColdPlanV1 ClusterColdPlanV1;

/* Participants are copied and ordered canonically by (thread, owner
 * incarnation), so input enumeration order never changes the schedule.
 * memory_budget bounds every allocation owned by the plan. */
extern ClusterColdDetailV1 cluster_cold_plan_create_v1(const ClusterColdParticipantV1 *participants,
													   uint32 participant_count, Size memory_budget,
													   ClusterColdPlanV1 **out_plan);

/* Feed one decoded record of the participant at caller index `participant`.
 * Records of one participant must arrive in increasing LSN order; a record
 * straddling the native redo start is refused. */
extern ClusterColdDetailV1 cluster_cold_plan_feed_v1(ClusterColdPlanV1 *plan, uint32 participant,
													 const ClusterColdRecordV1 *record);

/* Close the input, observe DATA for every page with a replayable component
 * (in canonical page order, once each) and build the schedule.  On failure
 * diag names the exact record, page and dependency; the plan stays failed. */
extern ClusterColdDetailV1 cluster_cold_plan_seal_v1(ClusterColdPlanV1 *plan,
													 ClusterColdObserveV1 observe, void *arg,
													 ClusterColdDiagV1 *diag);

/* Sealed schedule: non-history records with ordinary page components, in
 * replay order.  Each participant's records keep their LSN order. */
extern uint32 cluster_cold_plan_step_count_v1(const ClusterColdPlanV1 *plan);
extern bool cluster_cold_plan_step_v1(const ClusterColdPlanV1 *plan, uint32 index,
									  ClusterColdStepV1 *out);

extern void cluster_cold_plan_destroy_v1(ClusterColdPlanV1 **plan);

#endif /* CLUSTER_COLD_RECOVERY_H */
