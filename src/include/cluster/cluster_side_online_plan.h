/*-------------------------------------------------------------------------
 * cluster_side_online_plan.h
 *    RF-SIDE immutable online operation plan.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIDE_ONLINE_PLAN_H
#define CLUSTER_SIDE_ONLINE_PLAN_H

#include "cluster/cluster_page_online_plan.h"
#include "cluster/cluster_side_projection.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_space_reservation.h"

#define CLUSTER_SIDE_ONLINE_PLAN_INTERFACE_V1 1
#define RF_SIDE_ONLINE_PLAN_MAX_BYTES (4 * 1024 * 1024)

typedef struct RfSideOnlinePlanV1 RfSideOnlinePlanV1;

typedef enum RfSideOnlineOperationKindV1 {
	RF_SIDE_ONLINE_OPERATION_INVALID = 0,
	RF_SIDE_ONLINE_OPERATION_XACT = 1,
	RF_SIDE_ONLINE_OPERATION_UNDO = 2,
	RF_SIDE_ONLINE_OPERATION_PROJECTION = 3,
	RF_SIDE_ONLINE_OPERATION_SPACE = 4
} RfSideOnlineOperationKindV1;

typedef struct RfSideOnlinePlanRequestV1 {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	const RfContributorStreamCutV1 *physical_cuts;
	uint32 participant_count;
	Size memory_budget;
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
} RfSideOnlineOperationV1;

typedef bool (*RfSideOnlineApplyXactV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlineApplyUndoV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlineApplyProjectionV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlinePreflightXactV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlinePreflightUndoV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlinePreflightProjectionV1)(void *arg,
												  const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlineSpaceV1)(void *arg, const RfSideOnlineOperationV1 *operation);
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
extern RfPageProofDetailV1
rf_side_online_plan_feed_record_v1(RfSideOnlinePlanV1 *plan,
								   const RfDetachedRecordPlanV1 *record_plan,
								   const RfPageOnlineRecordIdentityV1 *identity);
extern RfPageProofDetailV1 rf_side_online_plan_seal_v1(RfSideOnlinePlanV1 *plan);
/* The physical source owner supplies this after the entire scan and root
 * revalidation, never by copying a provisional SPACE payload's namespace. */
extern bool rf_side_online_plan_bind_database_v1(RfSideOnlinePlanV1 *plan,
												uint64 database_incarnation);
/* Match the sealed, physically observed source, including its exact cut. */
extern bool rf_side_online_plan_source_matches_v1(const RfSideOnlinePlanV1 *plan,
	uint64 system_identifier, const uint8 storage_uuid[16],
	const RfContributorStreamCutV1 *cut);
extern bool rf_side_online_plan_contains_commit_v1(const RfSideOnlinePlanV1 *plan,
	const RfSideXactOperationV1 *operation);
/* Only later native TRUNCATE in this exact sealed source can justify an
 * absent CREATE page. No visibility or mutation authority is returned. */
extern bool rf_side_online_plan_multixact_page_retired_v1(const RfSideOnlinePlanV1 *plan,
	uint32 origin_thread, XLogRecPtr source_lsn, XLogRecPtr source_end_lsn,
	bool members, uint32 page);
/* Private preparation only, under the caller's independently protected target
 * read. Returned order contains SIDE operation indices for every exact input
 * on this locator; it does not retire any structural or durability obligation. */
extern RfPageProofDetailV1 rf_side_online_plan_prepare_space_v1(
	const RfSideOnlinePlanV1 *plan, const ClusterSpaceIdentityKey *expected,
	const void *identity_page, const void *reservation_page, uint32 *order,
	uint32 capacity, uint32 *out_count, ClusterSpaceRecoveryImage *out);

typedef struct RfSideUndoHeaderImageV1 {
	PGAlignedBlock page;
	uint32 source_index;
	uint32 operation_count;
	bool has_full_image;
} RfSideUndoHeaderImageV1;

typedef RfSideUndoHeaderImageV1 RfSideUndoBlockImageV1;
/* Physical replay starts at a source FPI in each segment incarnation, never
 * at an inferred DATA LSN. Installation remains the original owner's duty. */
extern RfPageProofDetailV1 rf_side_online_plan_prepare_undo_block_v1(
	const RfSideOnlinePlanV1 *plan, uint8 instance, uint32 segment_id,
	uint32 block_no, RfSideUndoBlockImageV1 *out);

/* Evolve a private segment header through its source-ordered lifecycle and
 * TT records, including folded XACT COMMIT. The target read, physical UNDO
 * blocks, installation and durability remain separate protected obligations.
 * No output change on refusal. UINT32_MAX source means no byte change. */
extern RfPageProofDetailV1 rf_side_online_plan_prepare_undo_header_v1(
	const RfSideOnlinePlanV1 *plan, uint8 instance, uint32 segment_id,
	const char *base, RfSideUndoHeaderImageV1 *out);
extern uint32 rf_side_online_plan_operation_count_v1(const RfSideOnlinePlanV1 *plan);
extern Size rf_side_online_plan_scratch_available_v1(const RfSideOnlinePlanV1 *plan);
extern bool rf_side_online_plan_operation_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
											 RfSideOnlineOperationV1 *out_operation);
extern RfPageProofDetailV1 rf_side_online_plan_preflight_v1(const RfSideOnlinePlanV1 *plan,
															const RfSideOnlineApplyOpsV1 *ops);
extern RfPageProofDetailV1 rf_side_online_plan_apply_v1(const RfSideOnlinePlanV1 *plan,
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
