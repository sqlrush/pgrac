/*-------------------------------------------------------------------------
 * cluster_thread_recovery_fabric_plan.c
 *    Immutable PAGE/SIDE plan assembled before online recovery mutation.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_thread_recovery_fabric.h"

#ifdef USE_CLUSTER_UNIT
#define fabric_alloc0(size_) calloc(1, (size_))
#define fabric_free(pointer_) free((pointer_))
#else
#define fabric_alloc0(size_) palloc0(size_)
#define fabric_free(pointer_) pfree(pointer_)
#endif

#define CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_MAGIC UINT32_C(0x52465031)

struct ClusterThreadRecoveryFabricPlanV1 {
	uint32 magic;
	bool sealed;
	bool failed;
	bool space_active;
	uint8 reserved1;
	uint64 system_identifier;
	uint8 storage_uuid[16];
	uint32 participant_count;
	uint32 record_count;
	RfContributorStreamCutV1 *physical_cuts;
	RfPageOnlinePlanV1 *page_plan;
	RfSideOnlinePlanV1 *side_plan;
};

static RfPageProofDetailV1
fabric_preflight_side_record(void *arg, const RfOpcodeRouteV1 *route,
							 const RfPageVersionEdgeEntryV1 *edge, const DecodedBkpBlock *block)
{
	(void)arg;
	return route != NULL
				   && (route->record_owner == RF_ROUTE_OWNER_SIDE_TYPED
					   || route->record_owner == RF_ROUTE_OWNER_LOGICAL_NOOP)
				   && edge == NULL && block == NULL
			   ? RF_PAGE_PROOF_DETAIL_OK
			   : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

static RfPageProofDetailV1
fabric_preflight_side_component(void *arg, const RfOpcodeRouteV1 *route,
								const RfPageVersionEdgeEntryV1 *edge, const DecodedBkpBlock *block)
{
	(void)arg;
	return route != NULL && route->record_owner == RF_ROUTE_OWNER_PAGE_CODEC && edge != NULL
				   && block != NULL
				   && (edge->page_class == RF_PAGE_CLASS_ROUTED_HEADER
					   || edge->page_class == RF_PAGE_CLASS_ROUTED_SIDE
					   || edge->page_class == RF_PAGE_CLASS_ROUTED_SPACE)
			   ? RF_PAGE_PROOF_DETAIL_OK
			   : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

static RfPageProofDetailV1
fabric_preflight_rebuildable_component(void *arg, const RfOpcodeRouteV1 *route,
									   const RfPageVersionEdgeEntryV1 *edge,
									   const DecodedBkpBlock *block)
{
	(void)arg;
	return route != NULL && route->record_owner == RF_ROUTE_OWNER_PAGE_CODEC && edge != NULL
				   && block != NULL && edge->page_class == RF_PAGE_CLASS_REBUILDABLE_FSM
			   ? RF_PAGE_PROOF_DETAIL_OK
			   : RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN;
}

static void
fabric_plan_free(ClusterThreadRecoveryFabricPlanV1 *plan)
{
	if (plan == NULL)
		return;
	if (plan->side_plan != NULL)
		rf_side_online_plan_destroy_v1(&plan->side_plan);
	if (plan->page_plan != NULL)
		rf_page_online_plan_destroy_v1(&plan->page_plan);
	if (plan->physical_cuts != NULL)
		fabric_free(plan->physical_cuts);
	fabric_free(plan);
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_plan_create_v1(
	const ClusterThreadRecoveryFabricPlanRequestV1 *request,
	ClusterThreadRecoveryFabricPlanV1 **out_plan)
{
	ClusterThreadRecoveryFabricPlanV1 *plan;
	RfPageOnlinePlanRequestV1 page_request;
	RfSideOnlinePlanRequestV1 side_request;
	RfPageProofDetailV1 detail;
	static const uint8 zero_reserved[7];

	if (out_plan == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	*out_plan = NULL;
	if (request == NULL || request->physical_cuts == NULL || request->participant_count == 0
		|| request->participant_count > RF_PAGE_STABLE_MAX_PARTICIPANTS
		|| memcmp(request->reserved_zero, zero_reserved, sizeof(zero_reserved)) != 0)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	plan = (ClusterThreadRecoveryFabricPlanV1 *)fabric_alloc0(sizeof(*plan));
	if (plan == NULL)
		return RF_PAGE_PROOF_DETAIL_OOM;
	plan->physical_cuts = (RfContributorStreamCutV1 *)fabric_alloc0((Size)request->participant_count
																	* sizeof(*plan->physical_cuts));
	if (plan->physical_cuts == NULL) {
		fabric_plan_free(plan);
		return RF_PAGE_PROOF_DETAIL_OOM;
	}
	memcpy(plan->physical_cuts, request->physical_cuts,
		   (Size)request->participant_count * sizeof(*plan->physical_cuts));
	memset(&page_request, 0, sizeof(page_request));
	page_request.system_identifier = request->system_identifier;
	memcpy(page_request.storage_uuid, request->storage_uuid, 16);
	page_request.physical_cuts = plan->physical_cuts;
	page_request.participant_count = request->participant_count;
	page_request.retention_binding_cookie = request->retention_binding_cookie;
	page_request.memory_budget = request->page_memory_budget;
	detail = rf_page_online_plan_create_v1(&page_request, &plan->page_plan);
	if (detail != RF_PAGE_PROOF_DETAIL_OK) {
		fabric_plan_free(plan);
		return detail;
	}
	memset(&side_request, 0, sizeof(side_request));
	side_request.system_identifier = request->system_identifier;
	memcpy(side_request.storage_uuid, request->storage_uuid, 16);
	side_request.physical_cuts = plan->physical_cuts;
	side_request.participant_count = request->participant_count;
	side_request.memory_budget = request->side_memory_budget;
	detail = rf_side_online_plan_create_v1(&side_request, &plan->side_plan);
	if (detail != RF_PAGE_PROOF_DETAIL_OK) {
		fabric_plan_free(plan);
		return detail;
	}
	plan->magic = CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_MAGIC;
	plan->space_active = request->space_active;
	plan->system_identifier = request->system_identifier;
	memcpy(plan->storage_uuid, request->storage_uuid, 16);
	plan->participant_count = request->participant_count;
	*out_plan = plan;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_plan_feed_record_v1(ClusterThreadRecoveryFabricPlanV1 *plan,
												   XLogReaderState *record,
												   uint16 participant_index)
{
	RfDetachedOwnerOpsV1 owner_ops;
	RfDetachedRecordPlanV1 record_plan;
	RfPageOnlineRecordIdentityV1 identity;
	RfPageProofDetailV1 detail;
	DecodedXLogRecord *decoded;

	if (plan == NULL || plan->magic != CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_MAGIC || plan->sealed
		|| plan->failed || record == NULL || record->record == NULL
		|| participant_index >= plan->participant_count)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	decoded = record->record;
	memset(&owner_ops, 0, sizeof(owner_ops));
	owner_ops.arg = plan;
	owner_ops.preflight_side_record = fabric_preflight_side_record;
	owner_ops.preflight_side_component = fabric_preflight_side_component;
	owner_ops.preflight_rebuildable_component = fabric_preflight_rebuildable_component;
	memset(&record_plan, 0, sizeof(record_plan));
	detail = rf_page_detached_preflight_v1(record, plan->space_active, &owner_ops, &record_plan);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		goto fail;
	memset(&identity, 0, sizeof(identity));
	identity.record.system_identifier = plan->system_identifier;
	memcpy(identity.record.storage_uuid, plan->storage_uuid, 16);
	identity.record.origin_thread = plan->physical_cuts[participant_index].failed_thread;
	identity.record.timeline_id = plan->physical_cuts[participant_index].timeline_id;
	identity.record.read_rec_ptr = record->ReadRecPtr;
	identity.record.end_rec_ptr = record->EndRecPtr;
	identity.record.record_crc = (uint32)decoded->header.xl_crc;
	identity.record.rmid = decoded->header.xl_rmid;
	identity.record.info = decoded->header.xl_info;
	identity.participant_index = participant_index;
	detail = rf_page_online_plan_feed_record_v1(plan->page_plan, &record_plan, &identity);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		goto fail;
	detail = rf_side_online_plan_feed_record_v1(plan->side_plan, &record_plan, &identity);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		goto fail;
	plan->record_count++;
	return RF_PAGE_PROOF_DETAIL_OK;

fail:
	plan->failed = true;
	return detail;
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_plan_seal_v1(ClusterThreadRecoveryFabricPlanV1 *plan)
{
	RfPageProofDetailV1 detail;

	if (plan == NULL || plan->magic != CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_MAGIC || plan->sealed)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	if (plan->failed)
		return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
	detail = rf_page_online_plan_seal_v1(plan->page_plan);
	if (detail != RF_PAGE_PROOF_DETAIL_OK) {
		plan->failed = true;
		return detail;
	}
	detail = rf_side_online_plan_seal_v1(plan->side_plan);
	if (detail != RF_PAGE_PROOF_DETAIL_OK) {
		plan->failed = true;
		return detail;
	}
	plan->sealed = true;
	return RF_PAGE_PROOF_DETAIL_OK;
}

const RfPageOnlinePlanV1 *
cluster_thread_recovery_fabric_page_plan_v1(const ClusterThreadRecoveryFabricPlanV1 *plan)
{
	return plan != NULL && plan->magic == CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_MAGIC && plan->sealed
				   && !plan->failed
			   ? plan->page_plan
			   : NULL;
}

const RfSideOnlinePlanV1 *
cluster_thread_recovery_fabric_side_plan_v1(const ClusterThreadRecoveryFabricPlanV1 *plan)
{
	return plan != NULL && plan->magic == CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_MAGIC && plan->sealed
				   && !plan->failed
			   ? plan->side_plan
			   : NULL;
}

uint32
cluster_thread_recovery_fabric_participant_count_v1(const ClusterThreadRecoveryFabricPlanV1 *plan)
{
	return plan != NULL && plan->magic == CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_MAGIC && plan->sealed
				   && !plan->failed
			   ? plan->participant_count
			   : 0;
}

bool
cluster_thread_recovery_fabric_identity_matches_v1(const ClusterThreadRecoveryFabricPlanV1 *plan,
												   uint64 system_identifier,
												   const uint8 storage_uuid[16])
{
	return storage_uuid != NULL && plan != NULL
		   && plan->magic == CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_MAGIC && plan->sealed
		   && !plan->failed && plan->system_identifier == system_identifier
		   && memcmp(plan->storage_uuid, storage_uuid, 16) == 0;
}

bool
cluster_thread_recovery_fabric_cut_v1(const ClusterThreadRecoveryFabricPlanV1 *plan, uint32 index,
									  RfContributorStreamCutV1 *out_cut)
{
	if (out_cut == NULL || plan == NULL || plan->magic != CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_MAGIC
		|| !plan->sealed || plan->failed || index >= plan->participant_count)
		return false;
	*out_cut = plan->physical_cuts[index];
	return true;
}

void
cluster_thread_recovery_fabric_plan_destroy_v1(ClusterThreadRecoveryFabricPlanV1 **plan_address)
{
	ClusterThreadRecoveryFabricPlanV1 *plan;

	if (plan_address == NULL || *plan_address == NULL)
		return;
	plan = *plan_address;
	if (plan->magic != CLUSTER_THREAD_RECOVERY_FABRIC_PLAN_MAGIC)
		return;
	plan->magic = 0;
	fabric_plan_free(plan);
	*plan_address = NULL;
}

#endif
