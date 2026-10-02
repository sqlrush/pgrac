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
#include "cluster/cluster_wal_source.h"

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
	CLUSTER_COLD_CONTENT_UNPROVEN = 20		  /* unverified DATA and no anchor rebuilds it */
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
 * Record flags set by the decoder.  STRUCTURAL marks a relation lifecycle
 * change (truncate, drop, SPACE identity or reservation, database/tablespace,
 * relation map) whose ordering against other generations' page records needs its own
 * typed owner; it is refused after the native redo start and accepted as
 * already-durable history before it.  UNSUPPORTED marks input outside the
 * supported profile (prepared transactions) and is refused anywhere.
 * SIDE_UNOWNED marks a non-page effect of another generation for which no
 * typed cold owner exists yet (transaction outcomes, checkpoints, XID/OID/MX
 * counters, undo, SLRU); like STRUCTURAL it is refused after native redo and
 * is never replayed as a no-op.
 */
#define CLUSTER_COLD_RECORD_STRUCTURAL UINT8_C(0x01)
#define CLUSTER_COLD_RECORD_UNSUPPORTED UINT8_C(0x02)
#define CLUSTER_COLD_RECORD_SIDE_UNOWNED UINT8_C(0x04)
#define CLUSTER_COLD_RECORD_KNOWN_FLAGS UINT8_C(0x07)

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
 * that header over another version's body. */
typedef struct ClusterColdDataV1 {
	uint8 kind;
	uint8 flags; /* CLUSTER_COLD_DATA_FLAG_* */
	uint8 reserved_zero[6];
	RfPageVersionV1 version;
} ClusterColdDataV1;

#define CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED UINT8_C(0x01)
#define CLUSTER_COLD_DATA_KNOWN_FLAGS UINT8_C(0x01)

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


/* Records at or after the participant's native redo start (pass-2 input),
 * page or not, as fed; pass 2 must consume exactly this many. */
extern uint64 cluster_cold_plan_replay_record_count_v1(const ClusterColdPlanV1 *plan,
													   uint32 participant);

extern void cluster_cold_plan_destroy_v1(ClusterColdPlanV1 **plan);

/* Participants of a sealed plan; 0 unless seal() succeeded. */
extern uint32 cluster_cold_plan_participant_count_v1(const ClusterColdPlanV1 *plan);

/* Pass-2 action for one scheduled page record.  A fully skipped record of
 * the founder's own thread still advances nextXid past its xid. */
typedef enum ClusterColdPageActionV1 {
	CLUSTER_COLD_PAGE_APPLY = 0,
	CLUSTER_COLD_PAGE_SKIP = 1,
	CLUSTER_COLD_PAGE_SKIP_ADVANCE_XID = 2
} ClusterColdPageActionV1;

extern ClusterColdPageActionV1 cluster_cold_page_action_v1(const ClusterColdStepV1 *step, bool own);

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
	CLUSTER_COLD_REPLAY_CUT_DIFFERS = 7,   /* end or record count differs from pass 1 */
	CLUSTER_COLD_REPLAY_CALLBACK = 8	   /* a callback refused the record */
} ClusterColdReplayDetailV1;

typedef struct ClusterColdReplayOpsV1 {
	/* Next record of a participant; false at end of stream or on failure. */
	bool (*next)(void *arg, uint32 participant, ClusterColdReplayRecordV1 *out);
	/* Replay the record just returned, which has no scheduled step. */
	bool (*unscheduled)(void *arg, uint32 participant);
	/* Consume the scheduled record just returned with its page action. */
	bool (*scheduled)(void *arg, uint32 participant, const ClusterColdStepV1 *step,
					  ClusterColdPageActionV1 action);
} ClusterColdReplayOpsV1;

typedef struct ClusterColdReplayResultV1 {
	uint8 detail;		/* ClusterColdReplayDetailV1 */
	uint32 participant; /* where replay stopped */
	XLogRecPtr rec_ptr; /* the target, offending record or reached end */
	uint32 steps_done;
	uint64 pages_skipped;
	uint64 pages_applied;
	XLogRecPtr own_read; /* last consumed record of the own participant */
	XLogRecPtr own_end;
} ClusterColdReplayResultV1;

/* participants/participant_count must be the plan's caller-order cuts; own
 * is the founder's caller index. */
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

/*
 * One native record mapped to plan input by the shared closed route
 * registry.  record.components points into components[]; never copy a
 * filled value, pass it by pointer.
 */
typedef struct ClusterColdDecodedV1 {
	ClusterColdRecordV1 record;
	ClusterColdComponentV1 components[CLUSTER_COLD_MAX_COMPONENTS];
	uint8 route_owner;	/* RfRecordRouteOwnerV1 */
	uint8 route_detail; /* RfPageProofDetailV1 of a refused record */
} ClusterColdDecodedV1;

/* Classify one decoded record of the exact source namespace.  Unsupported
 * opcodes and routed side components fail with OPCODE_UNSUPPORTED; lifecycle,
 * prepared-transaction and (for another generation, `foreign`) unowned side
 * records are flagged for the plan to judge. */
extern ClusterColdDetailV1 cluster_cold_recovery_decode_v1(struct XLogReaderState *reader,
														   uint64 system_identifier,
														   const uint8 storage_uuid[16],
														   bool space_active, bool foreign,
														   ClusterColdDecodedV1 *out);

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
 * the relation's persisted SPACE identity.  arg is a ClusterColdObserverV1.
 */
typedef struct ClusterColdObserverV1 {
	RelFileLocator cached_locator;
	bool cached_valid;
	uint8 cached_incarnation[16];
	uint64 pages_observed;
	uint64 pages_invalid;
} ClusterColdObserverV1;

extern bool cluster_cold_observe_data_v1(void *arg, const RfPageIdentityV1 *page,
										 ClusterColdDataV1 *out);

/*
 * Classify one block read from storage (the caller handles a missing block
 * and fills the segment incarnation).  verified is PageIsVerifiedExtended's
 * answer; content_proven says whether it covered the whole page (an
 * enforced checksum).  False: a formatted page without a version token.
 */
extern bool cluster_cold_classify_page_v1(const char *page, bool verified, bool content_proven,
										  ClusterColdDataV1 *out);

/* Operator hint for a pass-1 refusal. */
extern const char *cluster_cold_refusal_hint_v1(ClusterColdDetailV1 detail);

/* Pass-1 scan of one RECOVERY_REQUIRED root through the sealed recovery
 * visitor, feeding the plan participant at caller index `participant`.
 * Every visited record is provisional until the visit and the observed cut
 * (complete end, record count) match the ROOT. */
typedef struct ClusterColdScanResultV1 {
	uint64 records;
	int root_result; /* ClusterControlRootResult of the visit */
	XLogRecPtr failed_read_rec_ptr;
	uint8 route_detail;
} ClusterColdScanResultV1;

extern ClusterColdDetailV1 cluster_cold_scan_root_v1(ClusterColdPlanV1 *plan, uint32 participant,
													 const ClusterControlRootSnapshot *root,
													 const ClusterControlRootReadToken *token,
													 bool space_active, bool foreign,
													 ClusterColdScanResultV1 *result);

/*
 * Typed cold replay driver state (startup process).  prepare() runs pass 1
 * with external admissions held and before the serial set is taken; refusal
 * is returned in `refusal`/`refusal_detail` so the caller can release what
 * it holds before failing startup.
 */
struct ClusterRecoveryFencePlan;

typedef struct ClusterColdTypedV1 {
	MemoryContext context;
	ClusterColdPlanV1 *plan;
	ClusterColdDetailV1 refusal; /* CLUSTER_COLD_OK when sealed */
	char refusal_detail[512];
	uint32 participant_count;
	uint32 own_participant;
	uint64 system_identifier;
	uint64 scanned_records;
	ClusterColdObserverV1 observer;
	ClusterColdParticipantV1 participants[CLUSTER_COLD_MAX_PARTICIPANTS];
	ClusterWalSourceRef sources[CLUSTER_COLD_MAX_PARTICIPANTS];
} ClusterColdTypedV1;

extern ClusterColdTypedV1 *cluster_cold_typed_prepare_v1(struct ClusterRecoveryFencePlan *fence,
														 uint16 own_thread, XLogRecPtr own_redo);
extern void cluster_cold_typed_destroy_v1(ClusterColdTypedV1 **typed);

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
 * recovery completion publication (R-A7), and a restartpoint owner that
 * holds own-thread checkpoints inside the cold replay window (R-A9).  Each
 * flag is set only once that consumer exists.
 */
typedef struct ClusterColdHandshakeV1 {
	bool redo_block_hook;
	bool participant_census;
	bool side_owners;
	bool completion_publish;
	bool restartpoint_hold;
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

/* True when a page that passed verification has proven content: data
 * checksums are on and a checksum failure is not being ignored. */
extern bool cluster_cold_checksum_proves_content_v1(bool checksums_enabled,
													bool ignore_checksum_failure);

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
