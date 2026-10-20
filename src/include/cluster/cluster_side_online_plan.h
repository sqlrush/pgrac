/*-------------------------------------------------------------------------
 * cluster_side_online_plan.h
 *    RF-SIDE immutable online operation plan.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIDE_ONLINE_PLAN_H
#define CLUSTER_SIDE_ONLINE_PLAN_H

#include "cluster/cluster_page_online_plan.h"
#include "cluster/cluster_side_projection.h"
#include "cluster/cluster_recovery_tt_record.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_space_reservation.h"

#define CLUSTER_SIDE_ONLINE_PLAN_INTERFACE_V1 1
#define RF_SIDE_ONLINE_PLAN_MAX_BYTES (4 * 1024 * 1024)

typedef struct RfSideOnlinePlanV1 RfSideOnlinePlanV1;

typedef enum RfSidePlanPurposeV1 {
	RF_SIDE_PLAN_RECOVERY = 0,
	RF_SIDE_PLAN_PI_CONTRIBUTIONS = 1
} RfSidePlanPurposeV1;

typedef enum RfSideOnlineOperationKindV1 {
	RF_SIDE_ONLINE_OPERATION_INVALID = 0,
	RF_SIDE_ONLINE_OPERATION_XACT = 1,
	RF_SIDE_ONLINE_OPERATION_UNDO = 2,
	RF_SIDE_ONLINE_OPERATION_PROJECTION = 3,
	RF_SIDE_ONLINE_OPERATION_SPACE = 4,
	/* Original RM_XLOG / RM_STANDBY / RM_CLUSTER_XID_STRIPE payload. Retained as an obligation,
	 * never inferred to be a no-op or projection. Execution (including
	 * standby invalidation/reset) needs the original native control owner. */
	RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL = 5,
	/* Native structural WAL retained for its original owner, never applied
	 * by a PI plan or substituted for a typed SPACE version. */
	RF_SIDE_ONLINE_OPERATION_NATIVE_STRUCTURE = 6,
	/* Dual-identity C0, retained separately; no ordinary UNDO apply permission. */
	RF_SIDE_ONLINE_OPERATION_RECOVERY_TT_ABORT = 7
} RfSideOnlineOperationKindV1;

typedef struct RfSideOnlinePlanRequestV1 {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	const RfContributorStreamCutV1 *physical_cuts;
	uint32 participant_count;
	Size memory_budget;
	/* Bound before feed by the original source owner; NULL means full cut. */
	const XLogRecPtr *redo_starts;
	/* Fixed at creation; a PI contribution plan never authorizes execution. */
	RfSidePlanPurposeV1 purpose;
} RfSideOnlinePlanRequestV1;

typedef struct RfSideOnlineOperationV1 {
	RfPageOnlineRecordIdentityV1 identity;
	RfOpcodeRouteV1 route;
	RfSideOnlineOperationKindV1 kind;
	uint32 owned_payload_offset;
	uint32 owned_payload_length;
	const uint8 *owned_payload;
	RfSideXactOperationV1 xact;
	ClusterUndoDecoded undo;
	ClusterSideProjectionOperationV1 projection;
	ClusterSpaceIdentityKey space_key;
	/* Kept for reconstruction/dependency checks; never dispatched as redo. */
	bool history_only;
	/* Original header SCN, not a page version or control-file authority. */
	SCN control_scn;
	RfSidePlanPurposeV1 purpose;
	TransactionId source_xid;
	/* Original WAL frame metadata, owned before the reader is reused.
	 * An absent top is not proof of canonical operation ownership. */
	TransactionId source_top_xid;
	ClusterRecoveryTTAbortV1 recovery_abort;
	uint16 recovery_target_source; /* retained source-set index, not streamed participant */
} RfSideOnlineOperationV1;

typedef bool (*RfSideOnlineApplyXactV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlineApplyUndoV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlineApplyProjectionV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlinePreflightXactV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlinePreflightUndoV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlinePreflightProjectionV1)(void *arg,
												  const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlineSpaceV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlineControlV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlineBeginProtectedSetV1)(void *arg);
typedef void (*RfSideOnlineEndProtectedSetV1)(void *arg, bool complete);

typedef struct RfSideOnlineApplyOpsV1 {
	void *arg;
	RfSideOnlineBeginProtectedSetV1 begin_protected_set;
	RfSideOnlineEndProtectedSetV1 end_protected_set;
	/* Required per present kind; all preflights run before any apply. */
	RfSideOnlinePreflightXactV1 preflight_xact;
	RfSideOnlinePreflightUndoV1 preflight_undo;
	RfSideOnlinePreflightProjectionV1 preflight_projection;
	RfSideOnlineApplyXactV1 apply_xact;
	RfSideOnlineApplyUndoV1 apply_undo;
	RfSideOnlineApplyProjectionV1 apply_projection;
	RfSideOnlineSpaceV1 preflight_space;
	RfSideOnlineSpaceV1 apply_space;
	RfSideOnlineControlV1 preflight_control;
	RfSideOnlineControlV1 apply_control;
	/* Zero selects the whole plan. A nonzero original thread selects only
	 * its operations; this is a selector, never recovery authority. */
	uint16 source_thread;
	/* Optional owner barrier after all target preflights and before any
	 * apply. Not called for preflight-only or an empty selected set. */
	bool (*prepare_apply)(void *arg);
	/* A whole-source undo consumer must not be used for one record. */
	bool (*apply_undo_record)(void *arg, uint32 index, const RfSideOnlineOperationV1 *operation);
	/* A selected COMMIT/ABORT must not install later DATA or TT terminals. */
	bool (*apply_xact_record)(void *arg, uint32 index, const RfSideOnlineOperationV1 *operation);
} RfSideOnlineApplyOpsV1;

/* Production owner for the RF-SIDE v2 non-authoritative projections.  The
 * retained-source certificate belongs to the immutable failed-origin replay
 * cut; projection bytes themselves never establish PREPARED, terminal truth,
 * readiness, or OPEN. */
typedef struct RfSideOnlineProjectionOwnerV1 {
	uint32 cluster_epoch;
	bool failed_origin_redo_retained;
	uint8 reserved5[3];
	ClusterSideProjectionApplyOpsV1 projection_ops;
} RfSideOnlineProjectionOwnerV1;

extern RfPageProofDetailV1 rf_side_online_plan_create_v1(const RfSideOnlinePlanRequestV1 *request,
														 RfSideOnlinePlanV1 **out_plan);
/* Copy the original retained full source set before feed. Every streamed cut
 * must match exactly one source; other retained generations may be C0 targets.
 * Order is immaterial. Pure binding, not ROOT selection, fencing, retention or
 * mutation authority. Failure poisons the plan; rebinding/late binding refuses. */
extern bool rf_side_online_plan_bind_sources_v1(RfSideOnlinePlanV1 *plan,
												const ClusterWalSourceRef *sources, uint32 count);
/* Complete immutable target/source values for a sealed C0 operation only. */
extern bool rf_side_online_plan_recovery_abort_sources_v1(const RfSideOnlinePlanV1 *plan,
														  uint32 index, ClusterWalSourceRef *writer,
														  ClusterWalSourceRef *target);
extern RfPageProofDetailV1
rf_side_online_plan_feed_record_v1(RfSideOnlinePlanV1 *plan,
								   const RfDetachedRecordPlanV1 *record_plan,
								   const RfPageOnlineRecordIdentityV1 *identity);
/* Exact same-source native MAIN CREATE / typed CREATE pair. This is an
 * immutable dependency lookup, never an execution or file-existence proof. */
extern bool rf_side_online_plan_native_creation_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
												   ClusterSpaceStructureChange *change);
/* Original native ABORT pending-delete ordinal -> exact preceding typed
 * CREATE operation. Reuses the complete sealed same-source/xid proof;
 * duplicates may select the same CREATE. Not permission to delete files. */
extern bool rf_side_online_plan_abort_creation_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
												  uint32 relation, uint32 *creation_index);
/* Check retained native creation boundaries against an exact PAGE graph.
 * This is a read-only dependency check, never replay eligibility. */
extern bool rf_side_online_plan_page_lifecycle_v1(const RfSideOnlinePlanV1 *plan,
												const RfPageOnlineTargetViewV1 *target);
extern RfPageProofDetailV1 rf_side_online_plan_seal_v1(RfSideOnlinePlanV1 *plan);
/* The physical source owner supplies this after the entire scan and root
 * revalidation, never by copying a provisional SPACE payload's namespace. */
extern bool rf_side_online_plan_bind_database_v1(RfSideOnlinePlanV1 *plan,
												 uint64 database_incarnation);
/* Match the sealed, physically observed source, including its exact cut. */
/* A legacy zero-generation cut never selects an explicit generation. */
extern bool rf_side_online_plan_source_matches_v1(const RfSideOnlinePlanV1 *plan,
												  uint64 system_identifier,
												  const uint8 storage_uuid[16],
												  const RfContributorStreamCutV1 *cut);
/* Same complete source check, additionally rejecting inspect-only plans.
 * This is a plan-purpose check, never independent mutation authority. */
extern bool rf_side_online_plan_recovery_source_matches_v1(const RfSideOnlinePlanV1 *plan,
														   uint64 system_identifier,
														   const uint8 storage_uuid[16],
														   const RfContributorStreamCutV1 *cut);
/* Match the original bound ROOT/claim of a physical recovery participant.
 * Retained-only inputs cannot match. Cut, redo coverage, checkpoint/base and
 * final source revalidation remain separate owner obligations. */
extern bool rf_side_online_plan_recovery_claim_matches_v1(const RfSideOnlinePlanV1 *plan,
														  const ClusterWalSourceRef *source);
/* Match any original bound recovery claim, including retained-only inputs.
 * This supplies neither physical participation nor cut/base/owner authority. */
extern bool rf_side_online_plan_recovery_bound_claim_matches_v1(const RfSideOnlinePlanV1 *plan,
																const ClusterWalSourceRef *source);
/* UINT32_MAX for an invalid or unsealed plan. */
extern uint32 rf_side_online_plan_participant_count_v1(const RfSideOnlinePlanV1 *plan);
/* The physical lower alone must not authorize replay of retained history. */
extern bool rf_side_online_plan_replay_start_matches_v1(const RfSideOnlinePlanV1 *plan,
														uint32 thread, uint64 incarnation,
														uint64 database_incarnation,
														XLogRecPtr native_redo);
extern bool rf_side_online_plan_contains_commit_v1(const RfSideOnlinePlanV1 *plan,
												   const RfSideXactOperationV1 *operation);
/* Identify the preceding versioned TT abort in the same immutable source.
 * Target reconstruction, durability and terminal publication remain separate. */
extern bool rf_side_online_plan_abort_target_v1(const RfSideOnlinePlanV1 *plan,
												const RfSideOnlineOperationV1 *operation,
												ClusterUndoDecoded *out);
/* Only later native TRUNCATE in this exact sealed source can justify an
 * absent CREATE page. No visibility or mutation authority is returned. */
extern bool rf_side_online_plan_multixact_page_retired_v1(const RfSideOnlinePlanV1 *plan,
														  uint32 origin_thread,
														  XLogRecPtr source_lsn,
														  XLogRecPtr source_end_lsn, bool members,
														  uint32 page);
/* Private preparation only, under the caller's independently protected target
 * read. Returned order contains SIDE operation indices for every exact input
 * on this locator; it does not retire any structural or durability obligation. */
extern RfPageProofDetailV1 rf_side_online_plan_prepare_space_v1(
	const RfSideOnlinePlanV1 *plan, const ClusterSpaceIdentityKey *expected,
	const void *identity_page, const void *reservation_page, uint32 *order, uint32 capacity,
	uint32 *out_count, ClusterSpaceRecoveryImage *out);
/* through is a position in the complete returned order, not a SIDE operation
 * index. Later proven target components retain their bytes and no WAL source. */
extern RfPageProofDetailV1 rf_side_online_plan_prepare_space_through_v1(
	const RfSideOnlinePlanV1 *plan, const ClusterSpaceIdentityKey *expected,
	const void *identity_page, const void *reservation_page, uint32 through, uint32 *order,
	uint32 capacity, uint32 *out_count, ClusterSpaceRecoveryImage *out);

typedef struct RfSideUndoHeaderImageV1 {
	PGAlignedBlock page;
	uint32 source_index;
	uint32 operation_count;
	bool has_full_image;
} RfSideUndoHeaderImageV1;

typedef RfSideUndoHeaderImageV1 RfSideUndoBlockImageV1;
/* Reconstruct only the referenced DATA before this exact SET_HEAD record.
 * Requires a retained FPI in that segment incarnation; never reads DATA or
 * grants permission to install a header or a later source image. */
extern RfPageProofDetailV1
rf_side_online_plan_prepare_undo_head_block_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
											   const RfPageOnlineRecordIdentityV1 *identity,
											   RfSideUndoBlockImageV1 *out);
/* Physical replay starts at a source FPI in each segment incarnation, never
 * at an inferred DATA LSN. Installation remains the original owner's duty. */
extern RfPageProofDetailV1 rf_side_online_plan_prepare_undo_block_v1(const RfSideOnlinePlanV1 *plan,
																	 uint8 instance,
																	 uint32 segment_id,
																	 uint32 block_no,
																	 RfSideUndoBlockImageV1 *out);

/* Evolve a private segment header through its source-ordered lifecycle and
 * TT records, including folded XACT COMMIT. The target read, physical UNDO
 * blocks, installation and durability remain separate protected obligations.
 * No output change on refusal. UINT32_MAX source means no byte change. */
extern RfPageProofDetailV1
rf_side_online_plan_prepare_undo_header_v1(const RfSideOnlinePlanV1 *plan, uint8 instance,
										   uint32 segment_id, const char *base,
										   RfSideUndoHeaderImageV1 *out);
/* Prepare only the selected TT/undo record's source prefix. Full identity,
 * source uniqueness and recovery purpose are required. This is a private
 * image, not an apply/terminal or complete-source proof; the original owner
 * still owes whole-source preflight, target binding and durability. A header
 * base outside the prefix is refused, never inferred covered by later WAL.
 * Block images require a prefix FPI and do not inspect target DATA. No output
 * change on refusal. */
extern RfPageProofDetailV1
rf_side_online_plan_prepare_undo_record_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
										   const RfPageOnlineRecordIdentityV1 *identity,
										   const char *base, RfSideUndoHeaderImageV1 *out);
/* The same exact prefix's segment header, including for a block record or
 * native ABORT bound to its unique preceding exact TT_ABORT.
 * This is private byte preparation, not permission to install a header. */
extern RfPageProofDetailV1
rf_side_online_plan_prepare_undo_record_header_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
												  const RfPageOnlineRecordIdentityV1 *identity,
												  const char *base, RfSideUndoHeaderImageV1 *out);
/* Read-only header input at the same exact prefix, also allowing a retained
 * history record as its endpoint. Original source/identity and lifecycle/base
 * checks still apply. This grants no apply, durability or coverage authority;
 * the execution preparation APIs above continue to reject history endpoints.
 * No output change on refusal. */
extern RfPageProofDetailV1 rf_side_online_plan_prepare_undo_record_input_header_v1(
	const RfSideOnlinePlanV1 *plan, uint32 index, const RfPageOnlineRecordIdentityV1 *identity,
	const char *base, RfSideUndoHeaderImageV1 *out);
extern uint32 rf_side_online_plan_operation_count_v1(const RfSideOnlinePlanV1 *plan);
typedef struct RfSideSpaceContributionV1 {
	ClusterSpaceIdentity result;
	uint64 result_token[2];
	uint8 page_mask;
} RfSideSpaceContributionV1;

typedef enum RfSideContributionOwnerV1 {
	RF_SIDE_CONTRIBUTION_SPACE = 1u << 0,
	RF_SIDE_CONTRIBUTION_UNDO_HEADER = 1u << 1,
	RF_SIDE_CONTRIBUTION_UNDO_BLOCK = 1u << 2,
	RF_SIDE_CONTRIBUTION_TERMINAL = 1u << 3,
	RF_SIDE_CONTRIBUTION_PREPARED = 1u << 4,
	RF_SIDE_CONTRIBUTION_CLOG = 1u << 5,
	RF_SIDE_CONTRIBUTION_MULTIXACT = 1u << 6,
	RF_SIDE_CONTRIBUTION_COMMIT_TS = 1u << 7,
	RF_SIDE_CONTRIBUTION_NATIVE_CONTROL = 1u << 8,
	RF_SIDE_CONTRIBUTION_INVALIDATION = 1u << 9,
	RF_SIDE_CONTRIBUTION_NATIVE_STRUCTURE = 1u << 10,
	RF_SIDE_CONTRIBUTION_COMPLETION = 1u << 11
} RfSideContributionOwnerV1;

typedef struct RfSideContributionOwnersV1 {
	uint32 owners;
	uint32 space_locator_count;
} RfSideContributionOwnersV1;

/* Classify every original typed owner of a sealed operation, including
 * retained history. Only SPACE contributes PCM BufferTags; UNDO headers and
 * blocks, terminal/prepared state, per-origin projections and native control
 * retain their independent owners. This is a census, never a durability,
 * replay-completion or WAL-retirement proof. Failure preserves output. */
extern bool rf_side_online_plan_contribution_owners_v1(const RfSideOnlinePlanV1 *plan,
													   uint32 operation,
													   RfSideContributionOwnersV1 *out);

/* All physical SPACE contributions of this original operation, including
 * retained history and every locator in a native COMMIT. Bit zero/one selects
 * SPACE block zero/one. Only the sealed plan's bound namespace is accepted.
 * Enumeration records obligations; it grants no DATA, redo or retirement
 * authority. UINT32_MAX means invalid/unsealed; failure preserves output. */
extern uint32 rf_side_online_plan_space_contribution_count_v1(const RfSideOnlinePlanV1 *plan,
															  uint32 operation);
extern bool rf_side_online_plan_space_contribution_v1(const RfSideOnlinePlanV1 *plan,
													  uint32 operation, uint32 locator_index,
													  RfSideSpaceContributionV1 *out);

/* Pure typed ancestry within the complete retained input, including history.
 * Both operation indices must contribute to this exact key/block and retain
 * the same incarnation. terminal describes this block, not other effects of
 * a structural/COMMIT record. Refusal preserves terminal. The DATA owner must
 * separately bind both indices to full original claims and record identities;
 * this query alone grants no durability, physical disposal or WAL retirement. */
extern bool rf_side_online_plan_space_covers_v1(const RfSideOnlinePlanV1 *plan,
												const ClusterSpaceIdentityKey *key,
												BlockNumber block, uint32 ancestor_operation,
												uint32 completed_operation, bool *terminal);

typedef struct RfSideSpaceTerminalV1 {
	uint32 operation;
	RfSideSpaceContributionV1 contribution;
} RfSideSpaceTerminalV1;

/* Select this block's terminal contribution using the same complete, strict
 * retained ancestry proof. No source/durability authority; refusal preserves
 * output, including when no retained operation contributes to this block. */
extern bool rf_side_online_plan_space_terminal_v1(const RfSideOnlinePlanV1 *plan,
												  const ClusterSpaceIdentityKey *key,
												  BlockNumber block, RfSideSpaceTerminalV1 *out);

/* Classify the terminal current CREATE/ADVANCE prefix in the original sealed
 * recovery plan. UINT32_MAX in both outputs means no native ABORT closes it;
 * otherwise select the exact original ABORT and its first matching native
 * pending-delete ordinal (duplicates share that binding). Refusal preserves
 * outputs. This is an immutable input lookup, never proof of execution,
 * durable deletion, file absence, source authority or replay completion. */
extern bool rf_side_online_plan_space_abort_v1(const RfSideOnlinePlanV1 *plan,
											   uint32 space_operation, uint32 *abort_operation,
											   uint32 *abort_relation);

typedef struct RfSideSpaceIncarnationEndV1 {
	uint32 operation;
	ClusterSpaceStructureChange change;
	bool terminal;
} RfSideSpaceIncarnationEndV1;

/* Find the original TRUNCATE or committed TOMBSTONE ending this incarnation
 * in the complete strict retained chain. completed_operation must contribute
 * SPACE0 at or after that end; terminal describes that completed SPACE0.
 * This is not ordinary DATA ancestry across incarnations, nor permission to
 * dispose of PI. The original owner must separately prove durable structural
 * completion, surviving base, lifecycle isolation and the exact source/master
 * cut. No I/O or authority is granted. Refusal preserves output. */
extern bool rf_side_online_plan_space_incarnation_end_v1(const RfSideOnlinePlanV1 *plan,
														 const ClusterSpaceIdentityKey *key,
														 const uint8 incarnation[16],
														 uint32 completed_operation,
														 RfSideSpaceIncarnationEndV1 *out);

/* One decoded record, no retained operation array or payload allocation.
 * Reuses the replay decoder but returns only provisional contribution owners.
 * The caller must finish physical WAL/source validation before completing its
 * census; these callbacks never prove an ancestor, DATA, replay or retirement.
 * identity uses participant zero in the supplied original source cut. */
typedef bool (*RfSideCensusSpaceVisitorV1)(void *arg, const RfSideSpaceContributionV1 *space);
extern RfPageProofDetailV1 rf_side_record_census_v1(const RfDetachedRecordPlanV1 *record_plan,
													const RfPageOnlineRecordIdentityV1 *identity,
													const RfContributorStreamCutV1 *cut,
													uint64 database_incarnation,
													RfSideCensusSpaceVisitorV1 visit_space,
													void *arg, RfSideContributionOwnersV1 *out);

/* Validate one record-local CLOG/MultiXact/commit_ts projection with the same
 * decoder used by the retained plan. Shape only: no plan, authority, or
 * execution permission is created. bounded_members retains the SIDE consumer
 * member-count profile; native redo does not have that profile limit. */
extern bool rf_side_online_projection_record_shape_v1(XLogReaderState *record, bool bounded_members);

/* Reuse the retained plan's complete native standby shape validation.
 * Shape only: grants no source, no-effect or execution authority. */
extern bool rf_side_online_standby_record_shape_v1(XLogReaderState *record);

/* Complete sorted unique namespace/locator set for typed SPACE, including
 * each original COMMIT-DROP participant. Sealing checks every source chain;
 * enumeration grants no target, lifecycle or durability authority.
 * UINT32_MAX denotes an invalid/unsealed plan. */
extern uint32 rf_side_online_plan_space_target_count_v1(const RfSideOnlinePlanV1 *plan);
extern bool rf_side_online_plan_space_target_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
												ClusterSpaceIdentityKey *out);
/* UINT32_MAX denotes an invalid plan or absent source; zero is a proven
 * empty projection of a source that belongs to this sealed plan. */
extern uint32 rf_side_online_plan_origin_operation_count_v1(const RfSideOnlinePlanV1 *plan,
															uint16 source_thread);
extern Size rf_side_online_plan_scratch_available_v1(const RfSideOnlinePlanV1 *plan);
extern bool rf_side_online_plan_operation_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
											 RfSideOnlineOperationV1 *out_operation);
extern RfPageProofDetailV1 rf_side_online_plan_preflight_v1(const RfSideOnlinePlanV1 *plan,
															const RfSideOnlineApplyOpsV1 *ops);
extern RfPageProofDetailV1 rf_side_online_plan_apply_v1(const RfSideOnlinePlanV1 *plan,
														const RfSideOnlineApplyOpsV1 *ops);
/* Dispatch one exact control/projection or dedicated undo-block operation after the
 * original whole-source preflight. The enclosing sequencer owns WAL order
 * and final completion; end_protected_set receives complete=false even on
 * success. XACT, undo headers and SPACE still require ordered target preparation
 * and refuse. */
extern RfPageProofDetailV1
rf_side_online_plan_apply_record_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
									const RfPageOnlineRecordIdentityV1 *identity,
									const RfSideOnlineApplyOpsV1 *ops);

extern bool rf_side_online_projection_owner_init_v1(RfSideOnlineProjectionOwnerV1 *owner,
													uint32 cluster_epoch,
													bool failed_origin_redo_retained);
extern bool rf_side_online_projection_preflight_owned_v1(void *arg,
														 const RfSideOnlineOperationV1 *operation);
extern bool rf_side_online_projection_apply_owned_v1(void *arg,
													 const RfSideOnlineOperationV1 *operation);
extern void rf_side_online_plan_destroy_v1(RfSideOnlinePlanV1 **plan);

#endif
