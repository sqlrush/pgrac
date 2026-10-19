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
#include "cluster/cluster_page_detached.h"
#include "cluster/cluster_wal_source.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_recovery_tt_record.h"
#include "cluster/cluster_recovery_reference_graph.h"

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
	CLUSTER_COLD_STATE = 16,				  /* wrong phase or already failed */
	CLUSTER_COLD_STRUCTURAL_UNSUPPORTED = 17, /* lifecycle record needs its owner */
	CLUSTER_COLD_OPCODE_UNSUPPORTED = 18,	  /* outside the closed route registry */
	CLUSTER_COLD_SIDE_OWNER_MISSING = 19,	  /* side effect without a cold owner */
	CLUSTER_COLD_CONTENT_UNPROVEN = 20,		  /* unverified DATA and no anchor rebuilds it */
	CLUSTER_COLD_SPACE_INVALID = 21,		  /* malformed or inconsistent SPACE effects */
	CLUSTER_COLD_IDENTITY_MISSING = 22, /* no SPACE identity for a relation not created here */
	CLUSTER_COLD_SPACE_REFUSED = 23		/* the SPACE owner refused a relation's effects */
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

/*
 * SPACE (relation storage identity) effect of a record.  CREATE and TRUNCATE
 * give the relation a new incarnation, which the pages' versions carry.
 * TRUNCATE also retires the blocks at or past nblocks and, as an object
 * checkpoint, makes every earlier change of the relation durable.  DROP (a
 * commit or abort that drops the relation) makes the dropped incarnation's
 * changes irrelevant.  ADVANCE only moves the reservation.  payload is the
 * exact input of the SPACE owner; feed copies it.
 */
typedef enum ClusterColdSpaceKindV1 {
	CLUSTER_COLD_SPACE_CREATE = 1,
	CLUSTER_COLD_SPACE_TRUNCATE = 2,
	CLUSTER_COLD_SPACE_ADVANCE = 3,
	CLUSTER_COLD_SPACE_DROP = 4
} ClusterColdSpaceKindV1;

typedef struct ClusterColdSpaceOpV1 {
	uint8 kind; /* ClusterColdSpaceKindV1 */
	uint8 reserved_zero[3];
	BlockNumber nblocks; /* TRUNCATE: first retired block */
	RelFileLocator locator;
	uint8 before_incarnation[16]; /* TRUNCATE, DROP: the replaced or dropped one */
	uint8 result_incarnation[16]; /* CREATE, TRUNCATE: the new one; ADVANCE: the live one */
	const void *payload;
	uint32 payload_length;
} ClusterColdSpaceOpV1;

/*
 * Record flags set by the decoder.  STRUCTURAL marks a relation lifecycle
 * change (truncate, drop, SPACE identity or reservation, database/tablespace,
 * relation map) whose ordering against other generations' page records needs its own
 * typed owner; it is refused after the native redo start and accepted as
 * already-durable history before it.  UNSUPPORTED marks input outside the
 * supported profile (prepared transactions) and is refused anywhere.
 * SIDE_UNOWNED marks a non-page effect of another generation for which no
 * typed cold owner exists yet (whole-source transaction outcomes, undo); like
 * STRUCTURAL it is refused after native redo and is never replayed as a
 * no-op.  FOREIGN_CONTROL marks another generation's native control record
 * (XLOG checkpoint, NEXTOID, parameter, FPW, timeline and segment records,
 * standby locks/running transactions, logical messages): its typed effect is
 * none, as in crash recovery -- they describe that writer's own control file
 * and counters -- so it is consumed without native replay.
 */
#define CLUSTER_COLD_RECORD_STRUCTURAL UINT8_C(0x01)
#define CLUSTER_COLD_RECORD_UNSUPPORTED UINT8_C(0x02)
#define CLUSTER_COLD_RECORD_SIDE_UNOWNED UINT8_C(0x04)
#define CLUSTER_COLD_RECORD_FOREIGN_CONTROL UINT8_C(0x08)
/* Original-origin record-local consumer, never founder native redo/no-op. */
#define CLUSTER_COLD_RECORD_FOREIGN_SIDE UINT8_C(0x10)
#define CLUSTER_COLD_RECORD_KNOWN_FLAGS UINT8_C(0x1F)

/* Every decoded record of a participant is fed, in its LSN order.  Records
 * without ordinary page components only advance the participant cursor. */
typedef struct ClusterColdRecordV1 {
	XLogRecPtr read_rec_ptr;
	XLogRecPtr end_rec_ptr;
	XLogRecPtr prev_rec_ptr; /* xl_prev: must equal the previous fed read_rec_ptr */
	uint64 scn;				 /* xl_scn; deterministic tie-break only */
	uint32 record_crc;
	uint8 rmid;
	uint8 info;
	uint8 record_flags; /* CLUSTER_COLD_RECORD_* */
	uint8 reserved_zero;
	uint16 component_count;
	const ClusterColdComponentV1 *components;
	uint32 space_count; /* a record has SPACE effects or page components, not both */
	const ClusterColdSpaceOpV1 *space_ops;
} ClusterColdRecordV1;

typedef enum ClusterColdDataKindV1 {
	CLUSTER_COLD_DATA_INVALID = 0, /* unreadable or failed verification */
	CLUSTER_COLD_DATA_PRESENT = 1,
	CLUSTER_COLD_DATA_UNFORMATTED = 2, /* all-zero page inside the file */
	CLUSTER_COLD_DATA_ABSENT = 3	   /* block beyond EOF or no file */
} ClusterColdDataKindV1;

/* Observed shared DATA state of one page.  PRESENT carries the exact
 * version; UNFORMATTED carries only the segment incarnation.  CONTENT_VERIFIED
 * means the whole page was proven (page checksum, all-zero page or no
 * block); without it only the header is known, and a torn write can leave
 * that header over another version's body.  NO_IDENTITY marks a PRESENT page
 * whose relation has no readable SPACE identity yet (zero incarnation); it is
 * placed by its token only, and only when the relation was created after a
 * native redo start. */
typedef struct ClusterColdDataV1 {
	uint8 kind;
	uint8 flags; /* CLUSTER_COLD_DATA_FLAG_* */
	uint8 reserved_zero[6];
	RfPageVersionV1 version;
} ClusterColdDataV1;

#define CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED UINT8_C(0x01)
#define CLUSTER_COLD_DATA_FLAG_NO_IDENTITY UINT8_C(0x02)
#define CLUSTER_COLD_DATA_KNOWN_FLAGS UINT8_C(0x03)

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

/* A scheduled step replays a page record, or hands a record's SPACE effects
 * (a CREATE or TRUNCATE before the pages of the incarnation it makes, or a
 * commit's relation drops) to the SPACE owner. */
typedef enum ClusterColdStepKindV1 {
	CLUSTER_COLD_STEP_PAGE = 0,
	CLUSTER_COLD_STEP_SPACE = 1
} ClusterColdStepKindV1;

typedef struct ClusterColdStepV1 {
	uint32 step_index;	/* position in the schedule */
	uint32 participant; /* caller's participant index */
	uint8 step_kind;	/* ClusterColdStepKindV1 */
	uint8 space_kind;	/* SPACE step: CREATE, TRUNCATE or DROP */
	uint16 reserved_zero;
	uint32 space_count;	   /* SPACE step: effects (a commit may drop several) */
	uint32 space_relation; /* SPACE step: first effect's relation (space_relation_v1) */
	uint32 space_input;	   /* SPACE step: first effect's position among its inputs */
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
/* Relation and input position of one effect of a SPACE step. */
extern bool cluster_cold_plan_step_space_v1(const ClusterColdPlanV1 *plan, uint32 index,
											uint32 effect, uint32 *relation, uint32 *input);


/* Records at or after the participant's native redo start (pass-2 input),
 * page or not, as fed; pass 2 must consume exactly this many. */
extern uint64 cluster_cold_plan_replay_record_count_v1(const ClusterColdPlanV1 *plan,
													   uint32 participant);

/* The highest xl_scn of every record the plan accepted, of every
 * participant and history included: the founder's SCN must pass it. */
extern uint64 cluster_cold_plan_max_scn_v1(const ClusterColdPlanV1 *plan);

extern void cluster_cold_plan_destroy_v1(ClusterColdPlanV1 **plan);

/*
 * SPACE inputs for the SPACE owner: relations with SPACE effects after their
 * participant's native redo start (as the owner receives them online), in
 * canonical locator order; each relation's inputs in the order the SPACE
 * owner's check returned.  Payloads are owned by the plan.  Pass 2 installs
 * a relation's inputs through a SPACE step's position, never past a later
 * CREATE, TRUNCATE or drop, and all of them at the end.
 *
 * A TRUNCATE shrank its relation's files before the instances failed, but a
 * shrink is durable only once the file is synced.  shrink_forks names the
 * forks for which nothing later proves it (no later TRUNCATE or DROP, whose
 * preparation syncs every fork, and no change of the new incarnation in
 * history, whose checkpoint synced the file): pass 2 shrinks them again at
 * the TRUNCATE's step, whether or not the SPACE pages already hold it, and
 * trusts no page of them past the size.  Every other fork keeps its files:
 * shrinking again would remove pages whose changes are never replayed.
 */
#define CLUSTER_COLD_SHRINK_FORKS                                                                  \
	((uint8)((1 << MAIN_FORKNUM) | (1 << FSM_FORKNUM) | (1 << VISIBILITYMAP_FORKNUM)))

typedef struct ClusterColdSpaceInputV1 {
	uint8 kind;			/* ClusterColdSpaceKindV1 */
	uint8 shrink_forks; /* TRUNCATE: forks (1 << ForkNumber) to shrink at its step */
	uint32 participant; /* caller index */
	XLogRecPtr read_rec_ptr;
	XLogRecPtr end_rec_ptr;
	const void *payload;
	uint32 payload_length;
} ClusterColdSpaceInputV1;

extern uint32 cluster_cold_plan_space_relation_count_v1(const ClusterColdPlanV1 *plan);
extern bool cluster_cold_plan_space_relation_v1(const ClusterColdPlanV1 *plan, uint32 index,
												RelFileLocator *locator, uint32 *input_count);
extern bool cluster_cold_plan_space_input_v1(const ClusterColdPlanV1 *plan, uint32 relation,
											 uint32 input, ClusterColdSpaceInputV1 *out);

/*
 * The SPACE owner's pass-1 check of one relation: its inputs (in canonical
 * participant and LSN order) must form one chain from the relation's
 * current SPACE pages.  order receives count input indexes in the owner's
 * canonical order.  Required before seal when any relation has inputs; a
 * refusal fails seal with SPACE_REFUSED before anything is modified.
 */
typedef bool (*ClusterColdSpaceCheckV1)(void *arg, const RelFileLocator *locator,
										const ClusterColdSpaceInputV1 *inputs, uint32 count,
										uint32 *order);

extern ClusterColdDetailV1 cluster_cold_plan_set_space_check_v1(ClusterColdPlanV1 *plan,
																ClusterColdSpaceCheckV1 check,
																void *arg);

/* Participants of a sealed plan; 0 unless seal() succeeded. */
extern uint32 cluster_cold_plan_participant_count_v1(const ClusterColdPlanV1 *plan);

/* Exact native cut of a sealed participant, in create()'s caller order. */
extern bool cluster_cold_plan_participant_matches_v1(const ClusterColdPlanV1 *plan,
													 uint32 participant,
													 const ClusterColdParticipantV1 *cut);

/* Pass-2 action for one scheduled page record.  A fully skipped record of
 * the founder's own thread still advances nextXid past its xid. */
typedef enum ClusterColdPageActionV1 {
	CLUSTER_COLD_PAGE_APPLY = 0,
	CLUSTER_COLD_PAGE_SKIP = 1,
	CLUSTER_COLD_PAGE_SKIP_ADVANCE_XID = 2,
	CLUSTER_COLD_SPACE_STEP = 3 /* hand the step's SPACE effects to the SPACE owner */
} ClusterColdPageActionV1;

extern ClusterColdPageActionV1 cluster_cold_page_action_v1(const ClusterColdStepV1 *step, bool own);

/*
 * Pass-2 handling of a decoded record without a scheduled step.  SPACE
 * effects are never replayed natively: the SPACE owner installs them, or the
 * SPACE pages on disk already cover them.  Another generation's native
 * control record is consumed without replay; record-local SIDE effects use
 * their original-origin consumer. Page versions, refused flags
 * or a commit's drops (always a step) mean pass 1 saw other input.
 */
typedef enum ClusterColdUnscheduledV1 {
	CLUSTER_COLD_UNSCHEDULED_NATIVE = 0,
	CLUSTER_COLD_UNSCHEDULED_SPACE_SKIP = 1,
	CLUSTER_COLD_UNSCHEDULED_REFUSE = 2,
	CLUSTER_COLD_UNSCHEDULED_FOREIGN_NOOP = 3,
	CLUSTER_COLD_UNSCHEDULED_FOREIGN_SIDE = 4
} ClusterColdUnscheduledV1;

extern ClusterColdUnscheduledV1 cluster_cold_unscheduled_v1(const ClusterColdRecordV1 *record);

/*
 * Pass-2 sequencer.  Drives every participant's reader through the sealed
 * schedule: records before a scheduled page record replay as unscheduled
 * records in stream order; the scheduled record must carry its pass-1
 * identity; afterwards each participant is drained to its validated tail
 * and must have consumed exactly its pass-1 record count.  The callbacks
 * read and apply; the sequencer only decides order and proves the cut.
 */
typedef struct ClusterColdReplayRecordV1 {
	XLogRecPtr read_rec_ptr;
	XLogRecPtr end_rec_ptr;
	uint32 record_crc;
	uint8 rmid;
	uint8 info;
	uint8 reserved_zero[2];
} ClusterColdReplayRecordV1;

typedef enum ClusterColdReplayDetailV1 {
	CLUSTER_COLD_REPLAY_OK = 0,
	CLUSTER_COLD_REPLAY_INVALID_ARGUMENT = 1,
	CLUSTER_COLD_REPLAY_STEP_UNUSABLE = 2, /* unreadable step or unknown participant */
	CLUSTER_COLD_REPLAY_SOURCE_ENDED = 3,  /* stream ended or failed before the step */
	CLUSTER_COLD_REPLAY_TARGET_PASSED = 4, /* the scheduled record start was passed */
	CLUSTER_COLD_REPLAY_IDENTITY = 5,	   /* the scheduled record identity differs */
	CLUSTER_COLD_REPLAY_PAST_TAIL = 6,	   /* a record lies past the validated tail */
	CLUSTER_COLD_REPLAY_CUT_DIFFERS = 7,   /* source, cut or record count differs from pass 1 */
	CLUSTER_COLD_REPLAY_CALLBACK = 8	   /* a callback refused the record */
} ClusterColdReplayDetailV1;

struct ClusterColdReplayResultV1;

typedef struct ClusterColdReplayOpsV1 {
	/* Next record of a participant; false at end of stream or on failure. */
	bool (*next)(void *arg, uint32 participant, ClusterColdReplayRecordV1 *out);
	/* Replay the record just returned, which has no scheduled step. */
	bool (*unscheduled)(void *arg, uint32 participant);
	/* Consume the scheduled record just returned with its page action. */
	bool (*scheduled)(void *arg, uint32 participant, const ClusterColdStepV1 *step,
					  ClusterColdPageActionV1 action);
	/* Finalize one SPACE relation only after every scheduled callback and
	 * every source drain succeeded in this invocation. Install remaining
	 * inputs, or verify the physical result of an already executed DROP;
	 * required when the plan has SPACE relations. */
	bool (*space_final)(void *arg, uint32 relation);
	/* Optional owner notification after this source's scheduled steps and
	 * exact counted drain. Other sources may still have scheduled work;
	 * this does not grant global completion. Refusal stops further replay. */
	bool (*source_drained)(void *arg, uint32 participant,
						   const struct ClusterColdReplayResultV1 *progress, uint64 records);
	/* Optional complete remainder-drain permutation, validated before any
	 * record callback. Scheduled steps and each stream order stay unchanged. */
	bool (*source_order)(void *arg, uint32 *order, uint32 count);
	/* Optional exact dependency of the current unscheduled record. The owner
	 * returns NO_INDEX for none; a different source must finish its original
	 * scheduled steps and counted drain before this record is consumed. */
	bool (*record_dependency)(void *arg, uint32 participant,
							  const ClusterColdReplayRecordV1 *record, uint32 *target);
} ClusterColdReplayOpsV1;

typedef struct ClusterColdReplayResultV1 {
	uint8 detail;		/* ClusterColdReplayDetailV1 */
	uint32 participant; /* where replay stopped */
	XLogRecPtr rec_ptr; /* the target, offending record or reached end */
	uint32 steps_done;
	uint32 space_steps;
	uint32 space_relations_finished;
	uint64 pages_skipped;
	uint64 pages_applied;
	XLogRecPtr own_read; /* last consumed record of the own participant */
	XLogRecPtr own_end;
	/* Each participant's last consumed record: where its cut ended (zero
	 * when nothing of it was replayed). */
	XLogRecPtr last_read[CLUSTER_COLD_MAX_PARTICIPANTS];
	XLogRecPtr last_end[CLUSTER_COLD_MAX_PARTICIPANTS];
	uint32 last_crc[CLUSTER_COLD_MAX_PARTICIPANTS];
} ClusterColdReplayResultV1;

/* participants/participant_count must be the plan's caller-order cuts,
 * verified before the first callback; own is the founder's caller index. */
extern ClusterColdReplayDetailV1
cluster_cold_replay_run_v1(const ClusterColdPlanV1 *plan,
						   const ClusterColdParticipantV1 *participants, uint32 participant_count,
						   uint32 own, const ClusterColdReplayOpsV1 *ops, void *arg,
						   ClusterColdReplayResultV1 *result);

#ifndef FRONTEND

#include "utils/guc.h"

/*
 * cluster.cold_recovery_plan_memory (kB, PGC_POSTMASTER): everything one
 * typed cold plan may own during pass 1 (records, page versions, seal
 * scratch; never WAL payload).  Exhausting it refuses startup before any
 * page is modified.
 */
#define CLUSTER_COLD_PLAN_MEMORY_DEFAULT_KB Min(4 * 1024 * 1024, MAX_KILOBYTES)
#define CLUSTER_COLD_PLAN_MEMORY_MIN_KB 1024
#define CLUSTER_COLD_PLAN_MEMORY_MAX_KB MAX_KILOBYTES

extern PGDLLIMPORT int cluster_cold_recovery_plan_memory;

struct XLogReaderState;

/* Per-record effect families, including every item of a native counted
 * array. Native redo consumes the entire array, never only its first item.
 * These are in-memory routes, not a WAL format or completion certificate. */
typedef enum ClusterColdEffectV1 {
	CLUSTER_COLD_EFFECT_RECORD = 1U << 0,
	CLUSTER_COLD_EFFECT_PAGE = 1U << 1,
	CLUSTER_COLD_EFFECT_FSM = 1U << 2,
	CLUSTER_COLD_EFFECT_SPACE = 1U << 3,
	CLUSTER_COLD_EFFECT_TERMINAL = 1U << 4,
	CLUSTER_COLD_EFFECT_TT = 1U << 5,
	CLUSTER_COLD_EFFECT_CHILDREN = 1U << 6,
	CLUSTER_COLD_EFFECT_RELATIONS = 1U << 7,
	CLUSTER_COLD_EFFECT_STATS = 1U << 8,
	CLUSTER_COLD_EFFECT_INVALIDATIONS = 1U << 9,
	CLUSTER_COLD_EFFECT_RELCACHE = 1U << 10,
	CLUSTER_COLD_EFFECT_ORIGIN = 1U << 11,
	CLUSTER_COLD_EFFECT_LOCKS = 1U << 12,
	CLUSTER_COLD_EFFECT_SYNC = 1U << 13,
	CLUSTER_COLD_EFFECT_FEEDBACK = 1U << 14,
	CLUSTER_COLD_EFFECT_SCN = 1U << 15
} ClusterColdEffectV1;

/* Disjoint routes must cover required exactly. APPLY names an existing
 * consumer, not permission to invoke it: the original cold readiness,
 * source/base, full-record and mutation gates still apply. In particular
 * foreign terminal/SLRU effects remain refused until their cold adapter is
 * wired. A compound record with any refused effect cannot replay partially. */
typedef struct ClusterColdEffectRoutesV1 {
	uint32 required;
	uint32 native_apply;
	uint32 page_apply;
	uint32 space_apply;
	uint32 no_effect;
	uint32 refused;
} ClusterColdEffectRoutesV1;

/* Original-thread native cold-replay counter inputs, applied in this order:
 * advance past record_xid, advance past payload_xid (if present), then apply
 * checkpoint_next_xid as an ONLINE minimum or a SHUTDOWN exact value.
 * complete only describes this record's inputs. It proves no replay cut,
 * per-thread endpoint, allocator authority, FullXid epoch or loser identity. */
typedef struct ClusterColdXidInputV1 {
	TransactionId record_xid;
	TransactionId payload_xid;
	uint64 checkpoint_next_xid; /* Native FullTransactionId value; zero if absent. */
	bool checkpoint_shutdown;
	bool complete;
} ClusterColdXidInputV1;

/* Pure counter fold over one original source's post-checkpoint input cut.
 * A finished value is only an input-derived candidate. The original owner
 * must still prove actual effect replay, freshness and all-thread coverage
 * before using any endpoint; this state grants no allocator/loser authority. */
typedef struct ClusterColdXidProgressV1 {
	ClusterColdParticipantV1 source;
	uint64 next_xid;
	uint64 expected_records;
	uint64 records;
	XLogRecPtr last_read;
	XLogRecPtr last_end;
	bool begun;
	bool failed;
} ClusterColdXidProgressV1;

/* Attempt-local counter inputs published only by the original endpoint
 * owner after every source and the post-redo inventory pass their final
 * readback. The sealed plan binds the physical cuts; the copied sources
 * bind the selected claims/namespaces. This is not fresh execution authority. */
typedef struct ClusterColdXidReceiptV1 {
	const ClusterColdPlanV1 *plan;
	uint64 system_identifier;
	uint32 participant_count;
	uint32 replay_count;
	ClusterWalSourceRef sources[CLUSTER_COLD_MAX_PARTICIPANTS];
	uint64 next_xid[CLUSTER_COLD_MAX_PARTICIPANTS];
} ClusterColdXidReceiptV1;

extern bool cluster_cold_xid_begin_v1(ClusterColdXidProgressV1 *progress,
									 const ClusterColdParticipantV1 *source,
									 uint64 checkpoint_next_xid, uint64 expected_records);
extern bool cluster_cold_xid_record_v1(ClusterColdXidProgressV1 *progress,
									  const ClusterColdParticipantV1 *source,
									  const ClusterColdRecordV1 *record,
									  const ClusterColdXidInputV1 *input);
extern bool cluster_cold_xid_candidate_v1(const ClusterColdXidProgressV1 *progress,
										 const ClusterColdParticipantV1 *source, uint64 *next_xid);

/*
 * One native record mapped to plan input by the shared closed route
 * registry.  record.components points into components[] and
 * record.space_ops into space_ops, whose payloads point into the reader's
 * record; never copy a filled value, pass it by pointer.  Zero it before
 * the first decode; space_ops grows as needed and is kept across decodes
 * until cluster_cold_decoded_release_v1.
 */
typedef struct ClusterColdDecodedV1 {
	ClusterColdRecordV1 record;
	ClusterColdComponentV1 components[CLUSTER_COLD_MAX_COMPONENTS];
	ClusterColdEffectRoutesV1 effects;
	ClusterColdXidInputV1 xid_input;
	/* Borrowed from this decode only; capture before advancing the reader. */
	RfDetachedRecordPlanV1 detached;
	uint8 route_owner;	/* RfRecordRouteOwnerV1 */
	uint8 route_detail; /* RfPageProofDetailV1 of a refused record */
	ClusterColdSpaceOpV1 *space_ops;
	uint32 space_capacity;
} ClusterColdDecodedV1;

/* Classify one decoded record of the exact source namespace.  Unsupported
 * opcodes and routed side components fail with OPCODE_UNSUPPORTED; SPACE
 * identity and reservation changes and a commit's relation drops become
 * typed SPACE effects (SPACE_INVALID when malformed or of another
 * namespace); other lifecycle, prepared-transaction and (for another
 * generation, `foreign`) unowned side records are flagged for the plan to
 * judge. */
extern ClusterColdDetailV1 cluster_cold_recovery_decode_v1(struct XLogReaderState *reader,
														   uint64 system_identifier,
														   const uint8 storage_uuid[16],
														   bool space_active, bool foreign,
														   ClusterColdDecodedV1 *out);
extern void cluster_cold_decoded_release_v1(ClusterColdDecodedV1 *decoded);

/* Before a pass-2 consumer or skip, prove that its original dispatch covers
 * the entire decoded record. NULL step means unscheduled. This is not
 * source qualification, consumer readiness or a completion certificate. */
extern bool cluster_cold_replay_effects_v1(const ClusterColdDecodedV1 *decoded,
										   const ClusterColdStepV1 *step, bool own);

/* Attempt-local proof that every pass-2 record reached the same complete
 * effect matrix that the original owner classified.  This is neither a WAL
 * field nor a completion authority: the caller still owns source, freshness,
 * durability and fence checks. */
typedef struct ClusterColdEffectsProofV1 {
	uint64 records_seen[CLUSTER_COLD_MAX_PARTICIPANTS];
	uint64 records_routed;
	uint32 required_union;
	uint32 routed_union;
	bool invalid;
} ClusterColdEffectsProofV1;

/* Volatile pass-2 output owned by the original typed context. The copied
 * counts/result remain tied to its sealed plan; this is not durability,
 * checkpoint coverage, top ownership or old-writer isolation authority. */
typedef struct ClusterColdEffectsReceiptV1 {
	const ClusterColdPlanV1 *plan;
	ClusterColdEffectsProofV1 proof;
	ClusterColdReplayResultV1 result;
} ClusterColdEffectsReceiptV1;

extern void cluster_cold_effects_proof_init_v1(ClusterColdEffectsProofV1 *proof);
extern bool cluster_cold_effects_proof_record_v1(ClusterColdEffectsProofV1 *proof,
										 uint32 participant,
										 const ClusterColdDecodedV1 *decoded,
										 const ClusterColdStepV1 *step, bool own);
extern bool cluster_cold_effects_proof_finish_v1(const ClusterColdEffectsProofV1 *proof,
										 const ClusterColdPlanV1 *plan,
										 const ClusterColdReplayResultV1 *result,
										 uint32 participant_count);

/*
 * Pass-2 reader over one ROOT-selected writer generation.  Segments are
 * opened through the selected restart-input opener (claim, namespace and
 * file identity); the native reader validates pages, records and xl_prev.
 * Read-only and never authority: the caller compares every returned record
 * with its pass-1 identity before applying it.
 */
typedef struct ClusterColdReaderV1 ClusterColdReaderV1;

extern ClusterColdReaderV1 *cluster_cold_reader_open_v1(const ClusterWalSourceRef *source,
														uint64 system_identifier, XLogRecPtr start);
extern struct XLogReaderState *cluster_cold_reader_next_v1(ClusterColdReaderV1 *reader,
														   char **errormsg);
extern void cluster_cold_reader_close_v1(ClusterColdReaderV1 **reader);

/*
 * Read-only DATA observation for seal().  Reads storage directly, before any
 * replay touches shared buffers, and resolves the segment incarnation from
 * the relation's persisted SPACE identity; a page whose relation has none
 * readable is reported with NO_IDENTITY.  arg is a ClusterColdObserverV1.
 */
typedef struct ClusterColdObserverV1 {
	RelFileLocator cached_locator;
	bool cached_valid;
	bool cached_no_identity;
	uint8 cached_incarnation[16];
	uint64 pages_observed;
	uint64 pages_invalid;
	/* SPACE namespace of every participant (cluster_cold_space_check_v1). */
	uint64 system_identifier;
	uint64 database_incarnation;
	uint8 storage_uuid[16];
	uint64 space_relations_checked;
} ClusterColdObserverV1;

extern bool cluster_cold_observe_data_v1(void *arg, const RfPageIdentityV1 *page,
										 ClusterColdDataV1 *out);

/*
 * Pass-1 SPACE check (ClusterColdSpaceCheckV1, arg a ClusterColdObserverV1):
 * reads the relation's two SPACE pages directly from storage (zero pages
 * when the SPACE fork or a block is absent) and has the SPACE owner's pure
 * cluster_space_recovery_prepare prove and order the inputs against them.
 */
extern bool cluster_cold_space_check_v1(void *arg, const RelFileLocator *locator,
										const ClusterColdSpaceInputV1 *inputs, uint32 count,
										uint32 *order);

/*
 * Classify one block read from storage (the caller handles a missing block
 * and fills the segment incarnation).  header_valid is the native page
 * verification.  With data checksums the stored checksum is compared to the
 * content here, whatever ignore_checksum_failure says: a mismatch is a torn
 * or corrupt page (INVALID), a match proves the content.  False: a
 * formatted page without a version token.
 */
extern bool cluster_cold_classify_page_v1(const char *page, BlockNumber blkno, bool header_valid,
										  bool checksums, ClusterColdDataV1 *out);

/* Operator hint for a pass-1 refusal. */
extern const char *cluster_cold_refusal_hint_v1(ClusterColdDetailV1 detail);

/* Pass-1 scan of one RECOVERY_REQUIRED root through the sealed recovery
 * visitor, feeding the plan participant at caller index `participant`.
 * Every visited record is provisional until the visit and the observed cut
 * (complete end, record count) match the ROOT. Optional side_plan is an
 * unsealed single-source plan for this ROOT; it captures every complete
 * decoded record without changing any original ownership/refusal flags.
 * If its execution profile is narrower than native/history acceptance,
 * side_capture_detail marks the entire optional capture unusable. The
 * caller discards it, and all captures on later whole-input failure. */
typedef struct ClusterColdScanResultV1 {
	uint64 records;
	/* Read-only history checkpoint-to-tail counter input. Zero unless the
	 * complete retained visit proves its cut; not an effect/freshness proof. */
	ClusterColdXidProgressV1 history_xid;
	int root_result; /* ClusterControlRootResult of the visit */
	XLogRecPtr failed_read_rec_ptr;
	uint8 route_detail;
	/* Capture profile differs from native/history acceptance. Nonzero means
	 * discard the entire optional SIDE plan; it cannot authorize replay. */
	uint8 side_capture_detail;
	XLogRecPtr side_capture_read_rec_ptr;
} ClusterColdScanResultV1;

struct RfSideOnlinePlanV1;

extern ClusterColdDetailV1 cluster_cold_scan_root_v1(ClusterColdPlanV1 *plan, uint32 participant,
													 const ClusterControlRootSnapshot *root,
													 const ClusterControlRootReadToken *token,
													 bool space_active, bool foreign,
													 struct RfSideOnlinePlanV1 *side_plan,
													 ClusterColdScanResultV1 *result);

/* Pass 2 consumed exactly the ROOT-sealed cut of replayed generation
 * `participant`: replay finished every step, and its last record is the
 * root's validated last record (start and CRC) ending at the validated
 * tail.  Only then may the generation be published recovered. */
extern bool cluster_cold_completion_proven_v1(const ClusterColdPlanV1 *plan,
											  const ClusterControlRootSnapshot *root,
											  const ClusterColdReplayResultV1 *result,
											  uint32 participant);

/*
 * Relation forks whose files pass 2 changed (block references, relation
 * creation and truncation of every replayed record, and every SPACE
 * relation), kept sorted.  Before a replayed generation is published
 * recovered they are made durable as PostgreSQL's end-of-recovery
 * checkpoint would make them: their dirty buffers are written and each
 * existing fork is fsynced.  A failed fsync raises an error.
 */
typedef struct ClusterColdTouchedRelV1 {
	RelFileLocator locator;
	uint32 forks; /* bit (1 << ForkNumber) */
} ClusterColdTouchedRelV1;

typedef struct ClusterColdTouchedV1 {
	uint32 count;
	uint32 capacity;
	ClusterColdTouchedRelV1 *rels;
} ClusterColdTouchedV1;

extern void cluster_cold_touched_add_v1(ClusterColdTouchedV1 *touched,
										const RelFileLocator *locator, ForkNumber fork);
extern void cluster_cold_touched_add_record_v1(ClusterColdTouchedV1 *touched,
											   struct XLogReaderState *record);
/* Startup-only: also write the founder's native SLRU/undo caches, then use
 * the original checkpointer sync handshake. An empty relation set does not
 * prove those caches clean. False leaves counter/completion publication
 * blocked; errors from the original write/sync owners propagate. */
extern bool cluster_cold_durable_barrier_v1(const ClusterColdTouchedV1 *touched);

/*
 * Typed cold replay driver state (startup process).  prepare() runs pass 1
 * with external admissions held and before the serial set is taken; refusal
 * is returned in `refusal`/`refusal_detail` so the caller can release what
 * it holds before failing startup.  Participants are the founder's crashed
 * generation, the fence plan's origins, then every history-only generation
 * of the participant census (cluster_cold_recovery_census.h); the census
 * read scope is released before prepare() returns.
 */
struct ClusterRecoveryFencePlan;

struct ClusterWalInputsV1;
struct ClusterColdReplayRootV1;
struct ClusterColdTtBaseV1;
struct UndoSegmentHeaderData;
struct ClusterControlRootSnapshot;
struct ClusterWalRecoveryCheckpointV1;

typedef struct ClusterColdTypedV1 {
	MemoryContext context;
	ClusterColdPlanV1 *plan;
	ClusterColdDetailV1 refusal; /* CLUSTER_COLD_OK when sealed */
	char refusal_detail[512];
	uint32 participant_count;
	uint32 replay_count; /* [0, replay_count): crashed; the rest history only */
	uint32 own_participant;
	/* Original checkpoints for all participants; only [0, replay_root_count)
	 * have the original ROOT tokens permitting replay-source revalidation. */
	struct ClusterColdReplayRootV1 *replay_roots;
	uint32 replay_root_count;
	struct ClusterColdTtBaseV1 *tt_base; /* raw original-owner input, no outcome authority */
	struct ClusterColdTtBaseV1 *tt_after_redo; /* separate physical input, same TT budget */
	bool source_drained[CLUSTER_COLD_MAX_PARTICIPANTS]; /* this pass-2 attempt only */
	/* Provisional native counters after counted pass-2 drain. Zero means
	 * missing; these do not prove global effects/durability or all-thread N. */
	uint64 xid_next_at_drain[CLUSTER_COLD_MAX_PARTICIPANTS];
	/* Full-width maximum of every original counter endpoint after replay
	 * and the caller's durability barrier. No TT/top/loser authority. */
	uint64 xid_next_after_redo;
	ClusterColdXidReceiptV1 xid_receipt;
	ClusterColdEffectsReceiptV1 effects_receipt;
	/* Read-only pass-1 inputs for history generations. Their source redo is
	 * the original checkpoint's, not the history mutation cut at tail. No
	 * pass-2 endpoint: final source freshness/durability still needs proof. */
	ClusterColdXidProgressV1 history_xid[CLUSTER_COLD_MAX_PARTICIPANTS];
	/* Per selected source, including its retained pre-redo records. Each plan
	 * binds the complete retained source set; that does not authorize target
	 * mutation. History-only generations keep their redo cut at the tail. */
	struct RfSideOnlinePlanV1 *side_plans[CLUSTER_COLD_MAX_PARTICIPANTS];
	uint8 side_capture_detail[CLUSTER_COLD_MAX_PARTICIPANTS];
	XLogRecPtr side_capture_read_rec_ptr[CLUSTER_COLD_MAX_PARTICIPANTS];
	struct ClusterWalInputsV1 *inputs;				   /* census read scope, released by pass 1 */
	ClusterControlRootFileToken census_root; /* metadata only, never an execution pin */
	bool census_root_captured;
	uint32 input_index[CLUSTER_COLD_MAX_PARTICIPANTS]; /* history: index in that scope */
	uint64 system_identifier;
	uint64 scanned_records;
	ClusterColdObserverV1 observer;
	ClusterColdParticipantV1 participants[CLUSTER_COLD_MAX_PARTICIPANTS];
	ClusterWalSourceRef sources[CLUSTER_COLD_MAX_PARTICIPANTS];
} ClusterColdTypedV1;

/* Read the retained original all-source counter maximum. Refuse a missing,
 * partial or mismatched receipt and zero the output. This only checks the
 * retained inputs; it does not reacquire a fence or revalidate disk sources. */
extern bool cluster_cold_typed_xid_maximum_v1(const ClusterColdTypedV1 *typed, uint64 *next_xid);

extern ClusterColdTypedV1 *cluster_cold_typed_prepare_v1(struct ClusterRecoveryFencePlan *fence,
														 uint16 own_thread, XLogRecPtr own_redo);
extern void cluster_cold_typed_destroy_v1(ClusterColdTypedV1 **typed);

/* Capture actual pass-2 coverage before its adapter is released. A failed
 * completion clears the previous output; consumers recheck the same plan. */
extern bool cluster_cold_typed_effects_complete_v1(ClusterColdTypedV1 *typed,
													const ClusterColdEffectsProofV1 *proof,
													const ClusterColdReplayResultV1 *result);
extern bool cluster_cold_typed_effects_ready_v1(const ClusterColdTypedV1 *typed);

/* Startup before execution pin/IR only. Reopen the original cold input owner,
 * compare the complete selected ROOT and history checkpoints, then release
 * all read pins. Success is freshness input, not execution/durability proof. */
extern bool cluster_cold_typed_recheck_inputs_v1(ClusterColdTypedV1 *typed);
/* Recheck/release the original census, then retain every selected physical
 * thread in the original execution owner before its unchanged foreign IR set.
 * This does not qualify history durability, final freshness or endpoints. */
extern bool cluster_cold_typed_acquire_serial_v1(ClusterColdTypedV1 *typed,
												 struct ClusterRecoveryFencePlan *fence);

/* Original native checkpoint input for any selected generation, including
 * history. Copies only sealed input; failure clears both outputs. This does
 * not establish TT era, checkpoint coverage, replay or loser authority. */
extern bool cluster_cold_typed_checkpoint_v1(const ClusterColdTypedV1 *typed, uint32 participant,
											 struct ClusterControlRootSnapshot *out_root,
											 struct ClusterWalRecoveryCheckpointV1 *out_checkpoint);

/* Read-only TT base input owned by the original cold attempt. Every segment
 * address of every participant origin (including history) is inspected.
 * These bytes prove neither FullXid, checkpoint coverage nor loser status. */
extern ClusterColdDetailV1 cluster_cold_tt_base_capture_v1(ClusterColdTypedV1 *typed, Size budget);
extern ClusterColdDetailV1 cluster_cold_tt_base_recheck_v1(ClusterColdTypedV1 *typed);
extern void cluster_cold_tt_base_destroy_v1(ClusterColdTypedV1 *typed);
extern uint32 cluster_cold_tt_base_count_v1(const ClusterColdTypedV1 *typed);
extern const struct UndoSegmentHeaderData *
cluster_cold_tt_base_header_v1(const ClusterColdTypedV1 *typed, uint32 index, uint8 *owner,
							   uint32 *segment);

/* Match raw namespace input against the retained capture and current caller.
 * This is neither source isolation nor checkpoint or transaction authority. */
extern bool cluster_cold_tt_base_namespace_matches_v1(const ClusterColdTypedV1 *typed,
													  uint64 system_identifier,
													  uint64 database_incarnation);

/* Physical input identity only: xid is raw, not FullXid, and there is no
 * cluster epoch or checkpoint/terminal proof. Generation zero is valid. */
typedef struct ClusterColdTtSlotKeyV1 {
	uint8 owner_instance;
	uint32 segment_id;
	uint32 segment_generation;
	uint32 slot_offset;
	TransactionId xid;
	uint16 slot_wrap;
} ClusterColdTtSlotKeyV1;

typedef struct ClusterColdTtSlotInputV1 {
	ClusterColdTtSlotKeyV1 key;
	TTSlot slot;
} ClusterColdTtSlotInputV1;

/* Physical identity of a slot that remains ACTIVE after sealed redo. The
 * key already carries the raw xid; this value deliberately has no FullXid,
 * cluster epoch, top proof, effects coverage or mutation authority. */
typedef struct ClusterColdTtActiveIdentityV1 {
	ClusterColdTtSlotKeyV1 key;
} ClusterColdTtActiveIdentityV1;

/* Attempt-local top proof assembled by the original source/effects owners.
 * The six gates are deliberately independent: a physical ACTIVE slot alone
 * never proves that it is the original top transaction.  FullXid, nextXid,
 * and cluster epoch are absent because they are not loser gates. */
typedef struct ClusterColdTopTxProofV1 {
	ClusterColdTtSlotKeyV1 key;
	bool producer_top;
	bool qualified_base;
	bool sealed_redo;
	bool ordinary_effects;
	bool old_writer_isolated;
	bool terminal_match;
} ClusterColdTopTxProofV1;

/* Physical E1 loser identity. This is not a persistent record or authority
 * grant; C0 derivation consumes it only after all physical fields match. */
typedef struct ClusterColdLoserIdentityV1 {
	ClusterColdTtSlotKeyV1 key;
} ClusterColdLoserIdentityV1;

/* Attempt-local producer result.  The source owner supplies the proof and
 * shape; this value only records the exact post-redo physical fact, the
 * resulting loser key and the derived C0 fields.  It is never persisted. */
typedef struct ClusterColdTopTxC0ProducerResultV1 {
	ClusterColdTtActiveIdentityV1 active;
	ClusterColdLoserIdentityV1 loser;
	ClusterRecoveryTTAbortDerivationV1 c0;
} ClusterColdTopTxC0ProducerResultV1;

/* The loser gate is independent from FullXid/epoch derivation. C0 refuses
 * incomplete all-thread endpoints or any exact physical mismatch, while a
 * C0 derivation failure never turns an already-proven physical loser into an
 * abort. These are attempt-local helpers and add no persistent ABI. */
extern bool cluster_cold_top_tx_loser_v1(const ClusterColdTtActiveIdentityV1 *active,
										 const ClusterColdTopTxProofV1 *proof,
										 ClusterColdLoserIdentityV1 *out);
extern bool cluster_cold_top_tx_c0_v1(const ClusterColdLoserIdentityV1 *loser,
									 const ClusterRecoveryTTAbortV1 *shape, uint64 next_xid,
									 bool all_endpoints_complete, bool has_oldest_xid,
									 TransactionId oldest_xid, uint64 recoverer_epoch,
									 ClusterRecoveryTTAbortDerivationV1 *out);

/* Producer boundary for one original TT owner.  Locate the post-redo header
 * by physical owner/segment, require the complete ordinary TT/UNDO inventory
 * and all original sources, then verify the selected sealed fold and exact
 * slot before-image. Require this original owner's captured native checkpoint
 * to agree with its exact source, ROOT and selected redo interval. This input
 * check does not qualify TT base coverage; qualified_base remains independent.
 * Derive producer_top from this source's exact native
 * canonical BIND; the caller's producer_top bit is ignored. Other proof/shape
 * gates remain independent and required. An older ACTIVE without a retained
 * BIND remains blocked pending the original qualified canonical-base producer;
 * this does not introduce a BIND lifetime retention requirement.
 * Reject exact native COMMIT/TT_ABORT conflicts in every sealed source,
 * including history-only retained incarnations of the same thread. Absence
 * of those conflicts does not supply the independent terminal_match proof.
 * Target incarnation/claim remain bound to the original source.  A new
 * header with no original base requires the native full-image proof.
 * The original endpoint receipt must cover the unchanged complete source
 * set and agree with typed->xid_next_after_redo; a missing endpoint,
 * post-redo image, proof gate or exact shape refuses.
 * No proof bit is synthesized from source-drained state, current epoch or
 * graph shape, and no old C0 replay consumer is called here. */
extern bool cluster_cold_typed_top_tx_c0_producer_v1(
	const ClusterColdTypedV1 *typed, uint32 participant, const ClusterColdTopTxProofV1 *proof,
	const ClusterRecoveryTTAbortV1 *shape, bool has_oldest_xid, TransactionId oldest_xid,
	uint64 recoverer_epoch, ClusterColdTopTxC0ProducerResultV1 *out);

/* Re-enumerate physical TT after complete redo and the original durability
 * barrier, without overwriting the pre-redo base. Share its remaining budget.
 * Retain post-redo images of UNDO blocks already cached in the original base;
 * recheck them against disk along with the post-redo headers.
 * The original cold owner must revalidate all sources around capture/readback.
 * These raw bytes grant no checkpoint, top, loser, FullXid or C0 authority. */
extern ClusterColdDetailV1 cluster_cold_tt_after_redo_capture_v1(ClusterColdTypedV1 *typed);
extern ClusterColdDetailV1 cluster_cold_tt_after_redo_recheck_v1(ClusterColdTypedV1 *typed);
extern void cluster_cold_tt_after_redo_reset_v1(ClusterColdTypedV1 *typed);
extern uint32 cluster_cold_tt_after_redo_count_v1(const ClusterColdTypedV1 *typed);
/* Distinguish a captured empty inventory from absent or refused input. */
extern bool cluster_cold_tt_after_redo_namespace_matches_v1(const ClusterColdTypedV1 *typed);
/* Original inventory consumer: compare every retained UNDO block pair with
 * the sealed current source's native fold, or unchanged history-only bytes.
 * Does not reread disk or qualify base coverage. Final source/header/block
 * rechecks remain with the original owner. */
extern bool cluster_cold_tt_after_redo_undo_matches_v1(const ClusterColdTypedV1 *typed);
/* Exact physical lookup in one valid captured inventory. Clears index on
 * absence/refusal; no disk access, reconstruction or recovery authority. */
extern const UndoSegmentHeaderData *cluster_cold_tt_header_find_v1(const ClusterColdTypedV1 *typed,
																   bool after_redo, uint8 owner,
																   uint32 segment, uint32 *index);
/* Complete retained post-redo header input, indexed in its own inventory.
 * Failure clears available identity outputs. No original-base coverage. */
extern const struct UndoSegmentHeaderData *
cluster_cold_tt_after_redo_header_v1(const ClusterColdTypedV1 *typed, uint32 index, uint8 *owner,
									 uint32 *segment);
extern bool cluster_cold_tt_after_redo_slot_v1(const ClusterColdTypedV1 *typed,
											   uint32 segment_index, uint32 slot_offset,
											   ClusterColdTtSlotInputV1 *out);

/* Enumerate every physical slot without filtering status, then optionally
 * match an expected physical key. Both copy input only and clear output on
 * failure. A match grants no top binding, outcome or mutation authority. */
extern bool cluster_cold_tt_base_slot_v1(const ClusterColdTypedV1 *typed, uint32 segment_index,
										 uint32 slot_offset, ClusterColdTtSlotInputV1 *out);
extern bool cluster_cold_tt_base_match_v1(const ClusterColdTypedV1 *typed,
										  const ClusterColdTtSlotKeyV1 *expected,
										  ClusterColdTtSlotInputV1 *out);

struct ClusterRecoveryUndoRecord;
/* Retain and copy a whole original physical UNDO block within the TT budget,
 * bound to its captured namespace, file identity and complete block zero.
 * Reread on every call and in final base_recheck. Raw bytes and generation
 * alone prove no record validity, LSN coverage or transaction authority.
 * Failure clears outputs; source/allocation refusal invalidates TT input. */
extern bool cluster_cold_tt_base_block_v1(ClusterColdTypedV1 *typed, uint32 segment_index,
										  uint32 blockno, PGAlignedBlock *out, uint32 *generation);
/* Copy raw original UNDO from a captured physical segment, which may differ
 * from its canonical TT segment. Retain each whole block once within the TT
 * budget and reread it in base_recheck before pass-2. No canonical transaction,
 * generation ancestry, checksum, FullXid/epoch, top, complete-chain or terminal
 * proof is created; the graph owner must check those independently. Failure
 * clears outputs; source, parse or allocation refusal invalidates TT input. */
extern bool cluster_cold_tt_undo_record_v1(ClusterColdTypedV1 *typed, UBA address,
										   struct ClusterRecoveryUndoRecord *out,
										   uint32 *generation);

/* Read callback for the existing reference graph over an exact captured
 * namespace and canonical physical TT slot key. Reread cached source bytes
 * on every call, including graph verification. The original cold owner must
 * still provide isolation and final base_recheck; neither a matching raw key
 * nor a drained supplied graph proves FullXid/epoch/top binding, complete
 * producer coverage, loser eligibility or C0/OPEN authority. Source/parse
 * refusals return GRAPH_INVALID, allocation refusals GRAPH_CAPACITY; every
 * refusal clears available outputs and invalidates the retained TT input. */
extern ClusterRecoveryGraphResult
cluster_cold_tt_graph_read_v1(void *arg, const ClusterRecoveryTxnKey *key, UBA address,
							  struct ClusterRecoveryUndoRecord *out, uint32 *generation);

struct RfSideUndoHeaderImageV1;
/* Fold one selected original source's ordinary redo into a private copy of
 * its captured TT header. This is input reconstruction, not FullXid/epoch,
 * top binding, checkpoint coverage, loser or completion authority. C0 and
 * prepared transactions are unsupported here. Failure leaves out unchanged. */
extern bool cluster_cold_typed_tt_redo_header_v1(const ClusterColdTypedV1 *typed,
												 uint32 participant, uint32 segment_index,
												 struct RfSideUndoHeaderImageV1 *out);

/* Match this original header's complete sealed ordinary fold against the
 * separately captured physical post-redo header, by owner/segment rather
 * than inventory index. Copies the folded image only on exact equality;
 * failure clears out. This is selected-header readback input, not global
 * inventory, checkpoint/base qualification, top, loser or C0 authority.
 * Original source isolation and final freshness/readback owners still apply. */
extern bool cluster_cold_typed_tt_redo_readback_v1(const ClusterColdTypedV1 *typed,
												   uint32 participant, uint32 segment_index,
												   struct RfSideUndoHeaderImageV1 *out);

/* Select a physical post-redo header, including one absent from the original
 * inventory. Such a header requires the sealed native full-image fold with
 * no base; observed post bytes never supply missing input. Exact full-block
 * equality and the original-source guards still apply. Failure clears out.
 * This is selected-header input, not global inventory or recovery authority. */
extern bool cluster_cold_typed_tt_after_redo_readback_v1(const ClusterColdTypedV1 *typed,
														 uint32 participant,
														 uint32 post_segment_index,
														 struct RfSideUndoHeaderImageV1 *out);

/* Check both captured inventories and every current source's ordinary TT
 * target. Current origins use the sealed native fold; history-only origins
 * must retain their original full header bytes and are never replayed here.
 * Every already-cached UNDO data block also needs the exact native after
 * image, including unchanged bytes and history-only blocks. Every actual
 * current WAL block-write target absent from that cache needs the complete
 * native FPI fold and a separate retained post image; no observed-base fallback.
 * This read-only comparison grants no checkpoint/base, history-effect, top,
 * loser or C0 authority. Original source isolation and final rechecks remain
 * the caller's obligations; no completion flag is stored. */
extern bool cluster_cold_typed_tt_redo_inventory_v1(const ClusterColdTypedV1 *typed);

/* Copy any physical slot after the same selected source's sealed ordinary
 * redo, or match its resulting physical key. No status is filtered during
 * enumeration, and generation zero is valid. A pre-redo key is insufficient
 * after slot/segment reuse. These are raw input copies, not FullXid/epoch,
 * canonical top, checkpoint coverage, loser or C0/OPEN authority. History,
 * C0 and prepared inputs retain the header fold's refusal. Failure clears
 * available output; expected may point into out. Original source isolation
 * and final rechecks remain required. The existing raw graph callback still
 * matches only the captured pre-redo key. */
extern bool cluster_cold_typed_tt_redo_slot_v1(const ClusterColdTypedV1 *typed, uint32 participant,
											   uint32 segment_index, uint32 slot_offset,
											   ClusterColdTtSlotInputV1 *out);
extern bool cluster_cold_typed_tt_redo_match_v1(const ClusterColdTypedV1 *typed, uint32 participant,
												uint32 segment_index,
												const ClusterColdTtSlotKeyV1 *expected,
												ClusterColdTtSlotInputV1 *out);
/* Retain only the exact physical key of a post-redo ACTIVE slot. Terminal
 * status, raw xid and wrap must agree with that key. This is an input fact
 * for a later TopTxProof/loser consumer, never a loser decision itself; no
 * FullXid high bits or bind-time epoch are read. Failure clears out. */
extern bool cluster_cold_typed_tt_active_identity_v1(const ClusterColdTypedV1 *typed,
															uint32 participant,
															uint32 segment_index,
															const ClusterColdTtSlotKeyV1 *expected,
															ClusterColdTtActiveIdentityV1 *out);

/* Copy one raw record from a sealed ordinary source's final FPI-backed UNDO
 * block image and return its folded physical segment generation (zero is
 * valid). segment_index selects the captured physical segment, which may
 * differ from the canonical TT segment. No DATA fallback is attempted when
 * that incarnation lacks an FPI or has an unanchored delta. Original source
 * isolation and final rechecks remain required. This creates no canonical
 * transaction, FullXid/epoch/top, checkpoint coverage, complete-chain, loser
 * or C0/OPEN proof. History, C0 and prepared inputs retain the header fold's
 * refusal. Failure clears available outputs; successful output owns bytes. */
extern bool cluster_cold_typed_undo_redo_record_v1(const ClusterColdTypedV1 *typed,
												   uint32 participant, uint32 segment_index,
												   UBA address,
												   struct ClusterRecoveryUndoRecord *out,
												   uint32 *generation);

/* Distinct original-base input: same captured generation, no INIT/REUSE,
 * and a nonzero base block LSN no later than this source's native redo start.
 * Cache and reread the entire original block, fold ordinary sealed redo in a
 * private copy, then parse. No fallback or change to the FPI-only reader or
 * graph. Final base/source checks, isolation and all transaction proofs remain
 * the original owner's duty. Failure clears available outputs. */
extern bool cluster_cold_typed_undo_base_redo_record_v1(ClusterColdTypedV1 *typed,
														uint32 participant, uint32 segment_index,
														UBA address,
														struct ClusterRecoveryUndoRecord *out,
														uint32 *generation);

/* Fold a retained original UNDO block without rereading pre-redo disk bytes.
 * Uses the record reader's exact source, generation, lifecycle and LSN guards.
 * Successful output owns bytes; failure clears out. Base may alias out.
 * No checkpoint/base coverage, transaction or completion authority. */
extern bool cluster_cold_typed_undo_base_redo_block_v1(const ClusterColdTypedV1 *typed,
													   uint32 participant, uint32 segment_index,
													   uint32 block_no, const PGAlignedBlock *base,
													   PGAlignedBlock *out);

/* One explicitly selected original replay participant, never a history or
 * owner-id shortcut. The caller retains this context throughout graph use. */
typedef struct ClusterColdTtRedoGraphInputV1 {
	const ClusterColdTypedV1 *typed;
	uint32 participant;
} ClusterColdTtRedoGraphInputV1;

/* Create an input graph with every nonzero after-redo TT head in every
 * captured segment of this participant's physical owner. Terminal status
 * does not retire a dependency. No BIND inventory or zero-head inference.
 * The caller retains input and typed through graph destruction. Failure
 * clears out and destroys any partially seeded graph. MORE only means the
 * TT roots were supplied; traversal, page/native roots, all-source and final
 * base/source checks remain required. No top/loser/C0 or OPEN authority. */
extern ClusterRecoveryGraphResult
cluster_cold_typed_tt_redo_graph_begin_v1(ClusterColdTtRedoGraphInputV1 *input,
										  size_t memory_budget,
										  struct ClusterRecoveryReferenceGraph **out);

/* Existing reference-graph callback over an exact after-redo TT key and the
 * same source's final FPI-backed records. No DATA fallback or install. Every
 * call, including graph verification, repeats the source and identity checks;
 * refusal clears outputs and the graph retains its normal sticky failure.
 * A drained supplied graph proves no FullXid/epoch/top, producer/checkpoint
 * coverage, loser, C0 or OPEN authority. Original source isolation and final
 * base/source rechecks remain required. The raw pre-redo callback is unchanged. */
extern ClusterRecoveryGraphResult
cluster_cold_typed_tt_redo_graph_read_v1(void *arg, const ClusterRecoveryTxnKey *key, UBA address,
										 struct ClusterRecoveryUndoRecord *out, uint32 *generation);

/* Explicit original-base alternative, never an FPI-only graph fallback.
 * The original bounded block cache is mutable; retain this context and typed
 * through graph destruction. Roots and exact after-redo key checks have the
 * same contract as the FPI-only graph above. Records use the separate
 * same-generation/no-lifecycle original-base reader and its full-byte reread.
 * Cache/source/parse refusal returns GRAPH_INVALID and clears outputs; graph
 * allocation still returns GRAPH_CAPACITY. Final base/source checks and
 * isolation remain owner duties. No qualified checkpoint/source coverage,
 * FullXid/epoch/top, loser, C0 or OPEN authority, even after DRAINED. */
typedef struct ClusterColdTtBaseRedoGraphInputV1 {
	ClusterColdTypedV1 *typed;
	uint32 participant;
} ClusterColdTtBaseRedoGraphInputV1;

extern ClusterRecoveryGraphResult
cluster_cold_typed_tt_base_redo_graph_begin_v1(ClusterColdTtBaseRedoGraphInputV1 *input,
											   size_t memory_budget,
											   struct ClusterRecoveryReferenceGraph **out);
extern ClusterRecoveryGraphResult
cluster_cold_typed_tt_base_redo_graph_read_v1(void *arg, const ClusterRecoveryTxnKey *key,
											  UBA address, struct ClusterRecoveryUndoRecord *out,
											  uint32 *generation);

/* Sync the selected crashed sources before pass-2 mutations. No replay or
 * completion authority is created; a refusal invalidates this typed attempt. */
extern bool cluster_cold_typed_sync_sources_v1(ClusterColdTypedV1 *typed,
											   struct ClusterRecoveryFencePlan *fence);

/* Call after the original sequencer and physical durability barrier, while
 * the original execution owner remains held. No allocator is advanced. */
extern bool cluster_cold_typed_xid_endpoints_v1(ClusterColdTypedV1 *typed,
												struct ClusterRecoveryFencePlan *fence,
												const ClusterColdReplayResultV1 *result,
												const ClusterColdXidProgressV1 *progress);

/* Bind the original sequencer's completed source to its captured ROOT.
 * This is redo ordering only, not terminal, service or OPEN authority. */
extern bool cluster_cold_typed_source_drained_v1(ClusterColdTypedV1 *typed,
												 struct ClusterRecoveryFencePlan *fence,
												 uint32 participant,
												 const ClusterColdReplayResultV1 *progress,
												 uint64 records);

/* Order only the remainder drains by exact captured C0 dependencies.
 * Failure exports no partial order or completion authority. */
extern bool cluster_cold_typed_drain_order_v1(ClusterColdTypedV1 *typed,
											  struct ClusterRecoveryFencePlan *fence, uint32 *order,
											  uint32 count);

/* Resolve one already-decoded, captured C0 dependency. This returns only
 * an exact source index, never a completion or mutation permission. */
extern bool cluster_cold_typed_record_target_v1(ClusterColdTypedV1 *typed,
												struct ClusterRecoveryFencePlan *fence,
												uint32 participant,
												const ClusterColdRecordV1 *record, uint32 *target);

/* Consume one original foreign control or record-local native SIDE effect
 * under its existing cold authority. Selection never authorizes a full source
 * or recovery completion. */
extern bool cluster_cold_typed_side_record_v1(ClusterColdTypedV1 *typed,
											  struct ClusterRecoveryFencePlan *fence,
											  uint32 participant,
											  const ClusterColdRecordV1 *record);

/*
 * Before the fence plan publishes its origins recovered: each origin is a
 * replayed participant (not the founder, the same owner incarnation) whose
 * pass 2 ended exactly at its sealed tail (cluster_cold_completion_proven_v1),
 * and every SPACE relation of the plan joins `touched`.  On refusal *thread
 * names the origin (0 when none can be named).
 */
extern bool cluster_cold_completion_ready_v1(const ClusterColdTypedV1 *typed,
											 const struct ClusterRecoveryFencePlan *fence,
											 const ClusterColdReplayResultV1 *result,
											 ClusterColdTouchedV1 *touched, uint16 *thread);

/*
 * Pass-2 SPACE install through the SPACE owner: bring one relation of the
 * sealed plan through its input at position `through` (never further).  At
 * that input's own SPACE step (`step`), a TRUNCATE's shrink_forks are also
 * shrunk again.  False refuses; so does every call while this build has no
 * cold SPACE install (cluster_cold_space_owner_v1, the handshake's
 * space_owner).
 */
/* Post-drain foreign DROP check under its original exact cold source scope;
 * no reinstall of absent SPACE or recovery completion publication. */
extern bool cluster_cold_typed_drop_final_v1(ClusterColdTypedV1 *typed,
											 struct ClusterRecoveryFencePlan *fence,
											 uint32 relation);

/* Original sequencer final callback only, after every scheduled record and
 * source drain succeeded. Bind the exact terminal cold SPACE input to its
 * original sealed native ABORT, then ask its physical owner for the result.
 * No match leaves aborted false; refusal preserves it. No completion grant. */
extern bool cluster_cold_typed_space_abort_final_v1(const ClusterColdTypedV1 *typed,
													uint32 relation, bool *aborted);

extern bool cluster_cold_space_owner_v1(void);
extern bool cluster_cold_typed_space_install_v1(const ClusterColdTypedV1 *typed, uint32 relation,
												uint32 through, bool step);

/*
 * Cold crash route.  The shared profile recovers an engaged multi-generation
 * cold crash only through the typed plan; the unshared profile refuses it
 * (the merge preflight already does so before any fence or claim action);
 * everything else keeps native single-stream recovery.
 */
typedef enum ClusterColdRouteV1 {
	CLUSTER_COLD_ROUTE_NATIVE = 0,
	CLUSTER_COLD_ROUTE_TYPED = 1,
	CLUSTER_COLD_ROUTE_REFUSE = 2
} ClusterColdRouteV1;

extern ClusterColdRouteV1 cluster_cold_route_v1(bool shared_config, bool merge_engaged);

/*
 * Consumers pass 2 needs from their owners: the per-block redo consultation
 * (R-A2), the complete participant census (R-A4), typed owners for other
 * generations' non-page effects and the XID/OID/MX/SCN bounds (R-A5),
 * recovery completion publication (R-A7), a restartpoint owner that holds
 * own-thread checkpoints inside the cold replay window (R-A9), and the cold
 * SPACE install for relations with SPACE effects (R-A11).  Each flag is set
 * only once that consumer exists.
 */
typedef struct ClusterColdHandshakeV1 {
	bool redo_block_hook;
	bool participant_census;
	bool side_owners;
	bool completion_publish;
	bool restartpoint_hold;
	bool space_owner; /* cold SPACE install (R-A11), needed when SPACE effects exist */
} ClusterColdHandshakeV1;

/* True only when every consumer exists; with no redo consultation, any page
 * record that must be applied also refuses.  Decided after pass 1, before
 * the serial set is taken, so a refusal mutates nothing. */
extern bool cluster_cold_typed_ready_v1(const ClusterColdTypedV1 *typed,
										const ClusterColdHandshakeV1 *handshake, char *reason,
										Size reason_size);

/*
 * Typed cold replay window of the startup process (pass 2, from the first
 * replayed record to completion).  The cold cut must not move while it is
 * open, so the restartpoint owner does not adopt own-thread checkpoint
 * records replayed inside it (handshake restartpoint_hold).
 */
extern void cluster_cold_replay_window_enter_v1(void);
extern void cluster_cold_replay_window_leave_v1(void);
extern bool cluster_cold_replay_window_active_v1(void);


/*
 * Per-block decision consumed by the typed cold redo consultation in
 * XLogReadBufferForRedoExtended (whose owner also stamps the result
 * version).  NATIVE outside a published step and for blocks without a
 * PageVersion component; SKIP never reads the block; APPLY restores the
 * image or redoes without LSN/SCN freshness checks after verifying
 * expected_before.  False means the request does not match the published
 * record and replay must stop.
 */
typedef enum ClusterColdRedoBlockActionV1 {
	CLUSTER_COLD_REDO_NATIVE = 0,
	CLUSTER_COLD_REDO_SKIP = 1,
	CLUSTER_COLD_REDO_APPLY = 2
} ClusterColdRedoBlockActionV1;

typedef struct ClusterColdRedoBlockV1 {
	ClusterColdRedoBlockActionV1 action;
	uint8 expected_kind; /* ClusterColdDataKindV1; INVALID = any content */
	uint8 reserved_zero[3];
	RfPageVersionV1 expected_before;
	RfPageVersionV1 result;
} ClusterColdRedoBlockV1;

extern void cluster_cold_redo_step_enter_v1(const ClusterColdStepV1 *step);
extern void cluster_cold_redo_step_leave_v1(void);
extern bool cluster_cold_redo_block_decision_v1(struct XLogReaderState *record, uint8 block_id,
												ClusterColdRedoBlockV1 *out);

/*
 * Apply one scheduled page record inside its published step, bracketed by
 * the native block consumer (cluster_page_cold_redo_begin_v1/end_v1), so
 * every block the redo routine reads is decided by this record's verdicts
 * and every applied block is proven dirty with its result version.
 */
typedef void (*ClusterColdApplyRecordV1)(struct XLogReaderState *record, void *arg);

extern void cluster_cold_apply_step_v1(const ClusterColdStepV1 *step,
									   struct XLogReaderState *record,
									   ClusterColdApplyRecordV1 apply, void *arg);

#endif /* !FRONTEND */

#endif /* CLUSTER_COLD_RECOVERY_H */
