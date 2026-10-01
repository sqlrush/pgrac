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
	uint32 block_no;
	uint8 instance;
	bool installed;
	bool file_ready;
	ClusterUndoSmgrRecoveryFileV1 file;
	PGAlignedBlock base;
	RfSideUndoHeaderImageV1 final;
} SideUndoHeader;

static SideUndoHeader *
side_owner_undo_target(RfSideOnlineProductionOwnerV1 *owner, uint8 instance,
	uint32 segment_id, uint32 block_no)
{
	SideUndoHeader *header;

	for (header = owner->undo_headers; header != NULL; header = header->next)
		if (header->instance == instance && header->segment_id == segment_id && header->block_no == block_no)
			return header;
	/* Include typed preparation, stable reads and their bounded stack pages. */
	if (owner->undo_bytes_remaining < sizeof(*header) + 8 * BLCKSZ)
		return NULL;
	header = owner_alloc(sizeof(*header));
	if (header == NULL)
		return NULL;
	header->instance = instance;
	header->segment_id = segment_id;
	header->block_no = block_no;
	header->next = owner->undo_headers;
	owner->undo_headers = header;
	owner->undo_bytes_remaining -= sizeof(*header);
	/* Link before I/O so the protected-set ERROR cleanup also owns it. */
	if (block_no == 0) {
		if (!cluster_undo_smgr_recovery_probe_v1(segment_id, instance, &header->file, header->base.data))
			return NULL;
	} else {
		SideUndoHeader *segment = side_owner_undo_target(owner, instance, segment_id, 0);

		if (segment == NULL || !cluster_undo_smgr_recovery_read_block_v1(segment_id,
			instance, block_no, &segment->file, header->base.data))
			return NULL;
	}
	if ((block_no == 0
		? rf_side_online_plan_prepare_undo_header_v1(owner->protected_plan, instance,
			segment_id, header->base.data, &header->final)
		: rf_side_online_plan_prepare_undo_block_v1(owner->protected_plan, instance,
			segment_id, block_no, &header->final)) != RF_PAGE_PROOF_DETAIL_OK)
		return NULL;
	if (block_no == 0 && !header->final.has_full_image
		&& (!header->file.exists || header->file.size != UNDO_SEGMENT_SIZE_BYTES))
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
side_owner_install_target(RfSideOnlineProductionOwnerV1 *owner, SideUndoHeader *header)
{
	PGAlignedBlock current;

	if (header == NULL || !side_owner_authority_fresh(owner)
		|| !cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RECOVERY_SHARED, header->segment_id,
			header->instance, header->block_no, current.data))
		return false;
	if (header->installed)
		return memcmp(current.data, header->final.page.data, BLCKSZ) == 0;
	if (memcmp(current.data, header->base.data, BLCKSZ) != 0
		|| !side_owner_authority_fresh(owner))
		return false;
	if (memcmp(current.data, header->final.page.data, BLCKSZ) != 0) {
		if (!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RECOVERY_SHARED, header->segment_id,
			header->instance, header->block_no, header->final.page.data, true))
			return false;
	} else if (!cluster_undo_smgr_fsync_segment_file(header->segment_id, header->instance))
		return false;
	if (!side_owner_authority_fresh(owner)
		|| !cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RECOVERY_SHARED, header->segment_id,
			header->instance, header->block_no, current.data)
		|| memcmp(current.data, header->final.page.data, BLCKSZ) != 0)
		return false;
	header->installed = true;
	return true;
}

static bool
side_owner_install_data(RfSideOnlineProductionOwnerV1 *owner)
{
	SideUndoHeader *target;

	for (target = owner->undo_headers; target != NULL; target = target->next) {
		if (target->block_no != 0 || target->file_ready)
			continue;
		if (!side_owner_authority_fresh(owner))
			return false;
		if (target->final.has_full_image && !cluster_undo_smgr_recovery_materialize_v1(
			target->segment_id, target->instance, &target->file, target->base.data,
			target->final.page.data))
			return false;
		target->file_ready = true;
	}
	for (target = owner->undo_headers; target != NULL; target = target->next)
		if (target->block_no != 0 && !side_owner_install_target(owner, target))
			return false;
	return true;
}

static bool
side_owner_commit_covered(void *arg, const RfSideXactOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = arg;
	SideUndoHeader *header;

	if (owner == NULL || !owner->protected_set_active || owner->undo_authority == NULL
		|| !rf_side_online_plan_contains_commit_v1(owner->protected_plan, operation))
		return false;
	for (header = owner->undo_headers; header != NULL; header = header->next)
		if (header->block_no == 0 && header->instance == operation->tt_delta.instance
			&& header->segment_id == operation->tt_delta.segment_id)
			return header->installed && side_owner_install_target(owner, header);
	return false;
}

static bool
side_owner_begin_protected_set(void *arg)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	if (owner == NULL || owner->protected_set_active || !side_owner_authority_fresh(owner))
		return false;
	if (owner->undo_authority != NULL) {
		Size available = rf_side_online_plan_scratch_available_v1(owner->protected_plan);

		if (owner->borrowed_scratch_bytes > available)
			return false;
		if (!cluster_undo_recovery_scope_enter_v1(&owner->undo_scope,
			owner->undo_authority, owner->protected_plan))
			return false;
		owner->undo_bytes_remaining = available - owner->borrowed_scratch_bytes;
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
		if (!operation->xact.has_tt_delta)
			return false;
		return side_owner_undo_target(owner, delta->instance, delta->segment_id, 0) != NULL;
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

		if (undo->kind == CLUSTER_UNDO_KIND_BLOCK_WRITE || undo->kind == CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI)
			return side_owner_undo_target(owner, undo->instance, undo->segment_id, 0) != NULL
				&& side_owner_undo_target(owner, undo->instance, undo->segment_id, undo->block_no) != NULL;
		if (undo->kind == CLUSTER_UNDO_KIND_SEGMENT_INIT
			|| undo->kind == CLUSTER_UNDO_KIND_SEGMENT_REUSE
			|| undo->kind == CLUSTER_UNDO_KIND_SEGMENT_RECYCLE)
			return side_owner_undo_target(owner, undo->instance, undo->segment_id, 0) != NULL;
		if (undo->kind != CLUSTER_UNDO_KIND_TT_BIND && undo->kind != CLUSTER_UNDO_KIND_TT_COMMIT
			&& undo->kind != CLUSTER_UNDO_KIND_TT_ABORT && undo->kind != CLUSTER_UNDO_KIND_TT_SET_HEAD
			&& undo->kind != CLUSTER_UNDO_KIND_TT_CTRC_RELEASE)
			return false;
		if (operation->owned_payload != NULL || operation->owned_payload_length != 0)
			return false;
		return side_owner_undo_target(owner, undo->instance, undo->segment_id, 0) != NULL;
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

	if (owner != NULL && owner->protected_set_active && owner->undo_authority != NULL
		&& operation != NULL && operation->kind == RF_SIDE_ONLINE_OPERATION_XACT
		&& operation->xact.kind == RF_SIDE_XACT_COMMIT) {
		const xl_xact_tt_commit *d = &operation->xact.tt_delta;

		if (!side_owner_install_data(owner)
			|| !side_owner_install_target(owner, side_owner_undo_target(owner, d->instance, d->segment_id, 0)))
			return false;
		return rf_side_xact_apply_covered_commit_v1(&operation->xact, owner, side_owner_commit_covered)
			== RF_SIDE_XACT_APPLY_OK;
	}
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

	if (owner != NULL && owner->protected_set_active && owner->undo_authority != NULL
		&& operation != NULL && operation->kind == RF_SIDE_ONLINE_OPERATION_UNDO) {
		if (!side_owner_install_data(owner))
			return false;
		if (operation->undo.kind == CLUSTER_UNDO_KIND_BLOCK_WRITE
			|| operation->undo.kind == CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI)
			return true;
		return side_owner_install_target(owner, side_owner_undo_target(owner,
			operation->undo.instance, operation->undo.segment_id, 0));
	}
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
		|| owner->authority_arg != authority || authority->duty == NULL
		|| authority->duty->origin_thread_id == 0 || authority->duty->origin_thread_id > 128)
		return false;
	owner->undo_authority = authority;
	return true;
}

static uint16
side_owner_source_thread(const RfSideOnlineProductionOwnerV1 *owner)
{
	if (owner->undo_authority == NULL)
		return 0;
	if (owner->undo_authority->duty == NULL || owner->undo_authority->duty->origin_thread_id == 0
		|| owner->undo_authority->duty->origin_thread_id > 128)
		return UINT16_MAX;
	return owner->undo_authority->duty->origin_thread_id;
}

static bool
side_owner_preflight_space(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = arg;

	return owner->protected_set_active && owner->preflight_space != NULL
		   && side_owner_authority_fresh(owner)
		   && owner->preflight_space(owner->space_arg, operation);
}

static bool
side_owner_apply_space(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = arg;

	return owner->protected_set_active && owner->apply_space != NULL
		   && side_owner_authority_fresh(owner) && owner->apply_space(owner->space_arg, operation);
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
	ops.source_thread = side_owner_source_thread(owner);
	ops.begin_protected_set = side_owner_begin_protected_set;
	ops.end_protected_set = side_owner_end_protected_set;
	ops.preflight_xact = side_owner_preflight_xact;
	ops.preflight_undo = side_owner_preflight_undo;
	ops.preflight_projection = side_owner_preflight_projection;
	ops.apply_xact = side_owner_apply_xact;
	ops.apply_undo = side_owner_apply_undo;
	ops.apply_projection = side_owner_apply_projection;
	if (owner->preflight_space != NULL && owner->apply_space != NULL) {
		ops.preflight_space = side_owner_preflight_space;
		ops.apply_space = side_owner_apply_space;
	}
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
	ops.source_thread = side_owner_source_thread(owner);
	ops.begin_protected_set = side_owner_begin_protected_set;
	ops.end_protected_set = side_owner_end_protected_set;
	ops.preflight_xact = side_owner_preflight_xact;
	ops.preflight_undo = side_owner_preflight_undo;
	ops.preflight_projection = side_owner_preflight_projection;
	ops.apply_xact = side_owner_apply_xact;
	ops.apply_undo = side_owner_apply_undo;
	ops.apply_projection = side_owner_apply_projection;
	if (owner->preflight_space != NULL && owner->apply_space != NULL) {
		ops.preflight_space = side_owner_preflight_space;
		ops.apply_space = side_owner_apply_space;
	}
	detail = rf_side_online_plan_apply_v1(plan, &ops);
	owner->protected_plan = NULL;
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	if (rf_side_online_plan_origin_operation_count_v1(plan, ops.source_thread) == 0) {
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
