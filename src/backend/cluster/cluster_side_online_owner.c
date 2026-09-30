/*-------------------------------------------------------------------------
 * cluster_side_online_owner.c
 *    RF-SIDE production owner for immutable protected-set apply.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_side_online_owner.h"
#include "cluster/cluster_undo_smgr.h"

#ifdef RF_SIDE_OWNER_TESTING
#define owner_alloc(bytes_) calloc(1, (bytes_))
#define owner_free(pointer_) free(pointer_)
#else
#define owner_alloc(bytes_) palloc0(bytes_)
#define owner_free(pointer_) pfree(pointer_)
#endif

typedef struct SideUndoHeader {
	struct SideUndoHeader *next;
	uint32 segment_id;
	uint8 instance;
	PGAlignedBlock page;
} SideUndoHeader;

static SideUndoHeader *
side_owner_undo_header(RfSideOnlineProductionOwnerV1 *owner, uint8 instance, uint32 segment_id)
{
	SideUndoHeader *header;

	for (header = owner->undo_headers; header != NULL; header = header->next)
		if (header->instance == instance && header->segment_id == segment_id)
			return header;
	/* The two private pages used by typed preparation share this budget. */
	if (owner->undo_bytes_remaining < sizeof(*header) + 2 * BLCKSZ)
		return NULL;
	header = owner_alloc(sizeof(*header));
	if (header == NULL)
		return NULL;
	header->instance = instance;
	header->segment_id = segment_id;
	header->next = owner->undo_headers;
	owner->undo_headers = header;
	owner->undo_bytes_remaining -= sizeof(*header);
	/* Link before I/O so the protected-set ERROR cleanup also owns it. */
	if (!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RECOVERY_SHARED, segment_id,
		instance, 0, header->page.data))
		return NULL;
	return header;
}

static void
side_owner_undo_headers_free(RfSideOnlineProductionOwnerV1 *owner)
{
	SideUndoHeader *header = owner->undo_headers;

	owner->undo_headers = NULL;
	owner->undo_bytes_remaining = 0;
	while (header != NULL) {
		SideUndoHeader *next = header->next;

		owner_free(header);
		header = next;
	}
}

static bool
side_owner_authority_fresh(RfSideOnlineProductionOwnerV1 *owner)
{
	return owner != NULL && owner->revalidate_authority != NULL
		   && owner->revalidate_authority(owner->authority_arg);
}

static bool
side_owner_begin_protected_set(void *arg)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	if (owner == NULL || owner->protected_set_active || !side_owner_authority_fresh(owner))
		return false;
	if (owner->undo_authority != NULL) {
		if (!cluster_undo_recovery_scope_enter_v1(&owner->undo_scope,
			owner->undo_authority, owner->protected_plan))
			return false;
		owner->undo_bytes_remaining = rf_side_online_plan_scratch_available_v1(owner->protected_plan);
	}
	cluster_remote_xact_online_writer_push();
	owner->protected_set_active = true;
	owner->protected_set_complete = false;
	return true;
}

static void
side_owner_end_protected_set(void *arg, bool complete)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	if (owner == NULL || !owner->protected_set_active)
		return;
	/* Drop writer scope even if the final authority observation throws. */
	owner->protected_set_complete = false;
	owner->protected_set_active = false;
	side_owner_undo_headers_free(owner);
	cluster_undo_recovery_scope_leave_v1(&owner->undo_scope);
	owner->protected_plan = NULL;
	cluster_remote_xact_online_writer_pop();
	owner->protected_set_complete = complete && side_owner_authority_fresh(owner);
}

static bool
side_owner_preflight_xact(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	if (owner == NULL || !owner->protected_set_active || !side_owner_authority_fresh(owner)
		|| operation == NULL || operation->kind != RF_SIDE_ONLINE_OPERATION_XACT
		|| rf_side_xact_target_preflight_owned_v1(&operation->xact, operation->owned_payload,
			operation->owned_payload_length) != RF_SIDE_XACT_APPLY_OK)
		return false;
	if (owner->undo_authority != NULL && operation->xact.kind == RF_SIDE_XACT_COMMIT) {
		const xl_xact_tt_commit *delta = &operation->xact.tt_delta;
		SideUndoHeader *header;

		if (!operation->xact.has_tt_delta)
			return false;
		header = side_owner_undo_header(owner, delta->instance, delta->segment_id);
		return header != NULL && cluster_undo_prepare_commit_v1(delta->instance, delta->segment_id,
			delta->segment_generation, delta->slot_offset, delta->wrap, delta->xid,
			delta->commit_scn, header->page.data, header->page.data) != CLUSTER_UNDO_HEADER_BLOCKED;
	}
	return true;
}

static bool
side_owner_preflight_undo(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;
	ClusterUndoTargetPreflightV1 target;

	if (owner == NULL || !owner->protected_set_active || !side_owner_authority_fresh(owner)
		|| operation == NULL || operation->kind != RF_SIDE_ONLINE_OPERATION_UNDO)
		return false;
	if (owner->undo_authority != NULL) {
		const ClusterUndoDecoded *undo = &operation->undo;
		SideUndoHeader *header;

		/* Physical blocks and lifecycle installation keep their separate gate. */
		if (undo->kind != CLUSTER_UNDO_KIND_TT_BIND && undo->kind != CLUSTER_UNDO_KIND_TT_COMMIT
			&& undo->kind != CLUSTER_UNDO_KIND_TT_ABORT && undo->kind != CLUSTER_UNDO_KIND_TT_SET_HEAD
			&& undo->kind != CLUSTER_UNDO_KIND_TT_CTRC_RELEASE)
			return false;
		if (operation->owned_payload != NULL || operation->owned_payload_length != 0)
			return false;
		header = side_owner_undo_header(owner, undo->instance, undo->segment_id);
		if (header != NULL && (undo->kind == CLUSTER_UNDO_KIND_TT_COMMIT
			|| undo->kind == CLUSTER_UNDO_KIND_TT_SET_HEAD
			|| (undo->kind == CLUSTER_UNDO_KIND_TT_ABORT && undo->format_version == 0))) {
			const UndoSegmentHeaderData *data = (const UndoSegmentHeaderData *)header->page.data;

			if (undo->slot_offset >= TT_SLOTS_PER_SEGMENT
				|| cluster_undo_preflight_legacy_slot_v1(undo, &data->tt_slots[undo->slot_offset])
					== CLUSTER_UNDO_TARGET_BLOCKED)
				return false;
		}
		return header != NULL && cluster_undo_prepare_header_v1(undo, NULL, 0,
			header->page.data, header->page.data) != CLUSTER_UNDO_HEADER_BLOCKED;
	}
	target = cluster_undo_preflight_tt_target_v1(&operation->undo);
	return target == CLUSTER_UNDO_TARGET_APPLY || target == CLUSTER_UNDO_TARGET_PROVED_NOOP;
}

static bool
side_owner_preflight_projection(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	return owner != NULL && owner->protected_set_active && side_owner_authority_fresh(owner)
		   && rf_side_online_projection_preflight_owned_v1(&owner->projection, operation);
}

static bool
side_owner_apply_xact(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	return owner != NULL && owner->protected_set_active && side_owner_authority_fresh(owner)
		   && operation != NULL && operation->kind == RF_SIDE_ONLINE_OPERATION_XACT
		   && rf_side_xact_apply_owned_v1(&operation->xact, operation->owned_payload,
										  operation->owned_payload_length)
				  == RF_SIDE_XACT_APPLY_OK;
}

static bool
side_owner_apply_undo(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	return owner != NULL && owner->protected_set_active && side_owner_authority_fresh(owner)
		   && operation != NULL && operation->kind == RF_SIDE_ONLINE_OPERATION_UNDO
		   && cluster_undo_apply_tt_v1(&operation->undo) == CLUSTER_UNDO_APPLY_OK;
}

static bool
side_owner_apply_projection(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	return owner != NULL && owner->protected_set_active && side_owner_authority_fresh(owner)
		   && rf_side_online_projection_apply_owned_v1(&owner->projection, operation);
}

bool
rf_side_online_production_owner_init_v1(RfSideOnlineProductionOwnerV1 *owner, void *authority_arg,
										RfSideOnlineFreshAuthorityV1 revalidate_authority,
										uint32 cluster_epoch, bool failed_origin_redo_retained)
{
	if (owner == NULL || revalidate_authority == NULL || cluster_epoch == 0)
		return false;
	memset(owner, 0, sizeof(*owner));
	owner->authority_arg = authority_arg;
	owner->revalidate_authority = revalidate_authority;
	return rf_side_online_projection_owner_init_v1(&owner->projection, cluster_epoch,
												   failed_origin_redo_retained);
}

bool
rf_side_online_production_bind_undo_v1(RfSideOnlineProductionOwnerV1 *owner,
	const ClusterThreadRecoveryAuthorityV1 *authority)
{
	if (owner == NULL || owner->protected_set_active || authority == NULL
		|| owner->authority_arg != authority)
		return false;
	owner->undo_authority = authority;
	return true;
}

RfPageProofDetailV1
rf_side_online_production_preflight_v1(const RfSideOnlinePlanV1 *plan,
									   RfSideOnlineProductionOwnerV1 *owner)
{
	RfSideOnlineApplyOpsV1 ops;
	RfPageProofDetailV1 detail;

	if (plan == NULL || owner == NULL || owner->protected_set_active)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	owner->protected_set_complete = false;
	owner->protected_plan = plan;
	memset(&ops, 0, sizeof(ops));
	ops.arg = owner;
	ops.begin_protected_set = side_owner_begin_protected_set;
	ops.end_protected_set = side_owner_end_protected_set;
	ops.preflight_xact = side_owner_preflight_xact;
	ops.preflight_undo = side_owner_preflight_undo;
	ops.preflight_projection = side_owner_preflight_projection;
	ops.apply_xact = side_owner_apply_xact;
	ops.apply_undo = side_owner_apply_undo;
	ops.apply_projection = side_owner_apply_projection;
	detail = rf_side_online_plan_preflight_v1(plan, &ops);
	owner->protected_plan = NULL;
	return detail;
}

RfPageProofDetailV1
rf_side_online_production_apply_v1(const RfSideOnlinePlanV1 *plan,
								   RfSideOnlineProductionOwnerV1 *owner)
{
	RfSideOnlineApplyOpsV1 ops;
	RfPageProofDetailV1 detail;

	if (plan == NULL || owner == NULL || owner->protected_set_active)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	owner->protected_set_complete = false;
	owner->protected_plan = plan;
	memset(&ops, 0, sizeof(ops));
	ops.arg = owner;
	ops.begin_protected_set = side_owner_begin_protected_set;
	ops.end_protected_set = side_owner_end_protected_set;
	ops.preflight_xact = side_owner_preflight_xact;
	ops.preflight_undo = side_owner_preflight_undo;
	ops.preflight_projection = side_owner_preflight_projection;
	ops.apply_xact = side_owner_apply_xact;
	ops.apply_undo = side_owner_apply_undo;
	ops.apply_projection = side_owner_apply_projection;
	detail = rf_side_online_plan_apply_v1(plan, &ops);
	owner->protected_plan = NULL;
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	if (rf_side_online_plan_operation_count_v1(plan) == 0) {
		/*
		 * A sealed empty SIDE proof set closes without taking the online
		 * writer barrier: there are no SIDE bytes to protect or mutate.  It
		 * still needs a fresh authority observation at the closure point.
		 */
		owner->protected_set_complete = side_owner_authority_fresh(owner);
	}
	return owner->protected_set_complete ? RF_PAGE_PROOF_DETAIL_OK
										 : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

#endif
