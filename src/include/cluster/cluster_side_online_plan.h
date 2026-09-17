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

#define CLUSTER_SIDE_ONLINE_PLAN_INTERFACE_V1 1
#define RF_SIDE_ONLINE_PLAN_MAX_BYTES (4 * 1024 * 1024)

typedef struct RfSideOnlinePlanV1 RfSideOnlinePlanV1;

typedef enum RfSideOnlineOperationKindV1 {
	RF_SIDE_ONLINE_OPERATION_INVALID = 0,
	RF_SIDE_ONLINE_OPERATION_XACT = 1,
	RF_SIDE_ONLINE_OPERATION_UNDO = 2,
	RF_SIDE_ONLINE_OPERATION_PROJECTION = 3
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
} RfSideOnlineOperationV1;

typedef bool (*RfSideOnlineApplyXactV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlineApplyUndoV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlineApplyProjectionV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlinePreflightXactV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlinePreflightUndoV1)(void *arg, const RfSideOnlineOperationV1 *operation);
typedef bool (*RfSideOnlinePreflightProjectionV1)(void *arg,
												  const RfSideOnlineOperationV1 *operation);
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
extern uint32 rf_side_online_plan_operation_count_v1(const RfSideOnlinePlanV1 *plan);
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
