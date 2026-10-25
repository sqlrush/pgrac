/*-------------------------------------------------------------------------
 * cluster_side_online_owner.c
 *    RF-SIDE production owner for immutable protected-set apply.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/clog.h"
#include "access/subtrans.h"
#include "miscadmin.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_native_startup.h"
#include "cluster/cluster_multixact.h"
#include "cluster/cluster_recovery_undo_scan.h"
#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_side_online_owner.h"
#include "cluster/cluster_undo_format.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "cluster/cluster_xid_stripe_xlog.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "storage/standbydefs.h"
#include "utils/inval.h"

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
side_owner_undo_target(RfSideOnlineProductionOwnerV1 *owner, uint8 instance, uint32 segment_id,
					   uint32 block_no)
{
	SideUndoHeader *header;

	for (header = owner->undo_headers; header != NULL; header = header->next)
		if (header->instance == instance && header->segment_id == segment_id
			&& header->block_no == block_no)
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
		if (!cluster_undo_smgr_recovery_probe_v1(segment_id, instance, &header->file,
												 header->base.data))
			return NULL;
	} else {
		SideUndoHeader *segment = side_owner_undo_target(owner, instance, segment_id, 0);

		if (segment == NULL
			|| !cluster_undo_smgr_recovery_read_block_v1(segment_id, instance, block_no,
														 &segment->file, header->base.data))
			return NULL;
	}
	if ((block_no == 0
			 ? rf_side_online_plan_prepare_undo_header_v1(
				   owner->protected_plan, instance, segment_id, header->base.data, &header->final)
			 : rf_side_online_plan_prepare_undo_block_v1(owner->protected_plan, instance,
														 segment_id, block_no, &header->final))
		!= RF_PAGE_PROOF_DETAIL_OK)
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
	if (memcmp(current.data, header->base.data, BLCKSZ) != 0 || !side_owner_authority_fresh(owner))
		return false;
	if (memcmp(current.data, header->final.page.data, BLCKSZ) != 0) {
		if (!cluster_undo_smgr_write_block(CLUSTER_UNDO_PATH_RECOVERY_SHARED, header->segment_id,
										   header->instance, header->block_no,
										   header->final.page.data, true))
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
		if (target->final.has_full_image
			&& !cluster_undo_smgr_recovery_materialize_v1(target->segment_id, target->instance,
														  &target->file, target->base.data,
														  target->final.page.data))
			return false;
		target->file_ready = true;
	}
	for (target = owner->undo_headers; target != NULL; target = target->next)
		if (target->block_no != 0 && !side_owner_install_target(owner, target))
			return false;
	return true;
}

/* Select by the original serial mode, never by a failed authorization read:
 * a stale cold scope must refuse, not fall through to the online consumer. */
static bool
side_owner_cold_formed(const RfSideOnlineProductionOwnerV1 *owner)
{
	return owner->undo_authority != NULL && owner->undo_authority->serial_guard != NULL
		   && owner->undo_authority->serial_guard->mode == CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
}

/* Accept either the original complete top-only COMMIT or the separately
 * bounded completion payload. Other effects still need their actual consumers;
 * no PI inspection operation can authorize native storage. */
static bool
side_owner_cold_commit_ready(RfSideOnlineProductionOwnerV1 *owner,
							 const RfSideXactOperationV1 *operation)
{
	const uint32 supported = XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT;

	return operation->kind == RF_SIDE_XACT_COMMIT && !operation->pi_read_only
		   && operation->has_tt_delta
		   && (operation->cold_completion_effects
			   || ((operation->xinfo & ~supported) == 0
				   && operation->completion_payload_length == 0))
		   && operation->origin_thread == owner->undo_authority->duty->origin_thread_id
		   && rf_side_online_plan_contains_commit_v1(owner->protected_plan, operation)
		   && cluster_undo_recovery_cold_origin_authorized_v1(operation->origin_thread - 1);
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
			&& header->segment_id == operation->tt_delta.segment_id) {
			if (!header->installed || !side_owner_install_target(owner, header))
				return false;
			if (!side_owner_cold_formed(owner))
				return true;
			/* Complete-payload replay writes and verifies the native CLOG tree
			 * after this exact TT proof, using its single aligned payload.
			 * Strict plain COMMIT still completes top-only CLOG here. */
			if (!side_owner_cold_commit_ready(owner, operation))
				return false;
			if (operation->cold_completion_effects)
				return side_owner_authority_fresh(owner);
			return cluster_clog_native_recovery_apply_v1(operation->origin_thread - 1,
														 operation->xid, 0, NULL,
														 TRANSACTION_STATUS_COMMITTED)
				   && cluster_clog_native_recovery_verify_v1(operation->origin_thread - 1,
															 operation->xid, 0, NULL,
															 TRANSACTION_STATUS_COMMITTED)
				   && side_owner_cold_commit_ready(owner, operation)
				   && side_owner_authority_fresh(owner);
		}
	return false;
}

static bool
side_owner_cold_abort_ready(RfSideOnlineProductionOwnerV1 *owner,
							const RfSideOnlineOperationV1 *operation, ClusterUndoDecoded *target)
{
	return side_owner_cold_formed(owner)
		   && (operation->xact.cold_completion_effects
				   ? rf_side_xact_cold_completion_preflight_v1(&operation->xact,
															   operation->owned_payload,
															   operation->owned_payload_length)
				   : (operation->owned_payload == NULL && operation->owned_payload_length == 0
					  && rf_side_xact_preflight_covered_abort_v1(&operation->xact)))
		   && operation->xact.origin_thread == owner->undo_authority->duty->origin_thread_id
		   && rf_side_online_plan_abort_target_v1(owner->protected_plan, operation, target)
		   && cluster_undo_recovery_cold_origin_authorized_v1(operation->xact.origin_thread - 1);
}

typedef struct SideAbortCoverage {
	RfSideOnlineProductionOwnerV1 *owner;
	const RfSideOnlineOperationV1 *operation;
} SideAbortCoverage;

static bool
side_owner_abort_covered(void *arg, const RfSideXactOperationV1 *operation)
{
	SideAbortCoverage *coverage = arg;
	RfSideOnlineProductionOwnerV1 *owner = coverage->owner;
	SideUndoHeader *header;
	ClusterUndoDecoded target;

	if (!owner->protected_set_active || !side_owner_authority_fresh(owner)
		|| operation != &coverage->operation->xact
		|| !side_owner_cold_abort_ready(owner, coverage->operation, &target))
		return false;
	for (header = owner->undo_headers; header != NULL; header = header->next)
		if (header->block_no == 0 && header->instance == target.instance
			&& header->segment_id == target.segment_id)
			return header->installed && side_owner_install_target(owner, header)
				   && (operation->cold_completion_effects
						   ? rf_side_xact_cold_abort_clog_v1(
								 operation, coverage->operation->owned_payload,
								 coverage->operation->owned_payload_length)
						   : (cluster_clog_native_recovery_apply_v1(operation->origin_thread - 1,
																	operation->xid, 0, NULL,
																	TRANSACTION_STATUS_ABORTED)
							  && cluster_clog_native_recovery_verify_v1(
								  operation->origin_thread - 1, operation->xid, 0, NULL,
								  TRANSACTION_STATUS_ABORTED)))
				   && side_owner_cold_abort_ready(owner, coverage->operation, &target)
				   && side_owner_authority_fresh(owner);
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
		if (!cluster_undo_recovery_scope_enter_v1(&owner->undo_scope, owner->undo_authority,
												  owner->protected_plan))
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
side_owner_prepare_apply(void *arg)
{
	RfSideOnlineProductionOwnerV1 *owner = arg;

	if (owner == NULL || !owner->protected_set_active)
		return false;
	if (!side_owner_cold_formed(owner))
		return true;
	/* Every cold native/TT consumer can make target bytes durable. Bind the
	 * original source WAL and claim before the first such effect, including
	 * operations without completion flags. The original helper caches only
	 * inside this exact scope and revalidates its ROOT/source after IO. */
	return side_owner_authority_fresh(owner) && owner->undo_authority->duty != NULL
		   && cluster_undo_recovery_cold_source_sync_v1(
			   (int)owner->undo_authority->duty->origin_thread_id - 1)
		   && side_owner_authority_fresh(owner);
}

static bool
side_owner_preflight_xact_selected(void *arg, const RfSideOnlineOperationV1 *operation,
								   bool selected_record)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	if (owner == NULL || !owner->protected_set_active || !side_owner_authority_fresh(owner)
		|| operation == NULL || operation->kind != RF_SIDE_ONLINE_OPERATION_XACT)
		return false;
	/* Whole-source apply cannot stand in for ordered compound replay. A
	 * selected record validates structural targets before its first TT write. */
	if ((operation->xact.relation_count != 0 || operation->xact.space_drop_count != 0)
		&& (!selected_record || !operation->xact.cold_completion_effects
			|| operation->xact.relation_count == 0
			|| (operation->xact.kind == RF_SIDE_XACT_ABORT
					? operation->xact.space_drop_count != 0
					: operation->xact.space_drop_count == 0)))
		return false;
	if (side_owner_cold_formed(owner) && operation->xact.kind == RF_SIDE_XACT_ABORT) {
		ClusterUndoDecoded target;

		return side_owner_cold_abort_ready(owner, operation, &target)
			   && (!operation->xact.cold_completion_effects
				   || cluster_undo_recovery_cold_source_sync_v1(operation->xact.origin_thread - 1))
			   && side_owner_undo_target(owner, target.instance, target.segment_id, 0) != NULL;
	}
	if (operation->xact.cold_completion_effects)
		return side_owner_cold_formed(owner) && DatabasePath == NULL
			   && side_owner_cold_commit_ready(owner, &operation->xact)
			   && rf_side_xact_cold_completion_preflight_v1(
				   &operation->xact, operation->owned_payload, operation->owned_payload_length)
			   && (((operation->xact.xinfo
					 & (XACT_XINFO_HAS_DROPPED_STATS | XACT_COMPLETION_FORCE_SYNC_COMMIT))
					== 0)
				   || cluster_undo_recovery_cold_source_sync_v1(operation->xact.origin_thread - 1))
			   && side_owner_undo_target(owner, operation->xact.tt_delta.instance,
										 operation->xact.tt_delta.segment_id, 0)
					  != NULL;
	if (rf_side_xact_target_preflight_owned_v1(&operation->xact, operation->owned_payload,
											   operation->owned_payload_length)
		!= RF_SIDE_XACT_APPLY_OK)
		return false;
	if (side_owner_cold_formed(owner) && !side_owner_cold_commit_ready(owner, &operation->xact))
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
side_owner_preflight_xact(void *arg, const RfSideOnlineOperationV1 *operation)
{
	return side_owner_preflight_xact_selected(arg, operation, false);
}

static bool
side_owner_preflight_xact_record(void *arg, const RfSideOnlineOperationV1 *operation)
{
	return side_owner_preflight_xact_selected(arg, operation, true);
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

		if (undo->kind == CLUSTER_UNDO_KIND_BLOCK_WRITE
			|| undo->kind == CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI)
			return side_owner_undo_target(owner, undo->instance, undo->segment_id, 0) != NULL
				   && side_owner_undo_target(owner, undo->instance, undo->segment_id,
											 undo->block_no)
						  != NULL;
		if (undo->kind == CLUSTER_UNDO_KIND_SEGMENT_INIT
			|| undo->kind == CLUSTER_UNDO_KIND_SEGMENT_REUSE
			|| undo->kind == CLUSTER_UNDO_KIND_SEGMENT_RECYCLE)
			return side_owner_undo_target(owner, undo->instance, undo->segment_id, 0) != NULL;
		if (undo->kind != CLUSTER_UNDO_KIND_TT_BIND && undo->kind != CLUSTER_UNDO_KIND_TT_COMMIT
			&& undo->kind != CLUSTER_UNDO_KIND_TT_ABORT
			&& undo->kind != CLUSTER_UNDO_KIND_TT_SET_HEAD
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
side_owner_cold_clog_ready(RfSideOnlineProductionOwnerV1 *owner,
						   const RfSideOnlineOperationV1 *operation)
{
	const ClusterSideProjectionOperationV1 *clog = &operation->projection;
	uint16 thread = operation->identity.record.origin_thread;

	if (operation->kind != RF_SIDE_ONLINE_OPERATION_PROJECTION
		|| clog->kind != CLUSTER_SIDE_PROJECTION_CLOG
		|| thread != owner->undo_authority->duty->origin_thread_id
		|| operation->owned_payload_length != 0 || operation->owned_payload != NULL
		|| clog->page_number < 0
		|| (uint32)clog->page_number > UINT32_MAX / CLUSTER_NATIVE_CLOG_PER_PAGE
		|| !cluster_undo_recovery_cold_origin_authorized_v1(thread - 1))
		return false;
	if (clog->action == CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE)
		return clog->normalized_info == CLOG_ZEROPAGE && !TransactionIdIsValid(clog->oldest_xid)
			   && !OidIsValid(clog->oldest_database);
	return clog->action == CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE
		   && clog->normalized_info == CLOG_TRUNCATE && TransactionIdIsNormal(clog->oldest_xid)
		   && OidIsValid(clog->oldest_database)
		   && clog->oldest_xid / CLUSTER_NATIVE_CLOG_PER_PAGE == clog->page_number;
}

static bool
side_owner_cold_multixact_ready(RfSideOnlineProductionOwnerV1 *owner,
								const RfSideOnlineOperationV1 *operation)
{
	uint16 thread = operation->identity.record.origin_thread;

	return operation->kind == RF_SIDE_ONLINE_OPERATION_PROJECTION
		   && operation->projection.kind == CLUSTER_SIDE_PROJECTION_MULTIXACT
		   && thread == owner->undo_authority->duty->origin_thread_id
		   && cluster_undo_recovery_cold_origin_authorized_v1(thread - 1)
		   && cluster_multixact_native_recovery_preflight(
			   thread - 1, &operation->projection, operation->owned_payload,
			   operation->owned_payload_length, operation->identity.record.read_rec_ptr,
			   operation->identity.record.end_rec_ptr);
}

static bool
side_owner_preflight_projection(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	if (owner == NULL || !owner->protected_set_active || !side_owner_authority_fresh(owner)
		|| operation == NULL || operation->kind != RF_SIDE_ONLINE_OPERATION_PROJECTION)
		return false;
	/* Validate the whole protected set before its first storage mutation. */
	if (side_owner_cold_formed(owner) && operation->projection.kind == CLUSTER_SIDE_PROJECTION_CLOG
		&& !side_owner_cold_clog_ready(owner, operation))
		return false;
	if (side_owner_cold_formed(owner)
		&& operation->projection.kind == CLUSTER_SIDE_PROJECTION_MULTIXACT
		&& !side_owner_cold_multixact_ready(owner, operation))
		return false;
	return rf_side_online_projection_preflight_owned_v1(&owner->projection, operation);
}

static bool
side_owner_finish_commit(RfSideOnlineProductionOwnerV1 *owner,
						 const RfSideOnlineOperationV1 *operation)
{
	if (operation->xact.cold_completion_effects)
		return side_owner_cold_formed(owner)
			   && rf_side_xact_apply_cold_completion_v1(&operation->xact, operation->owned_payload,
														operation->owned_payload_length, owner,
														side_owner_commit_covered)
					  == RF_SIDE_XACT_APPLY_OK;
	return rf_side_xact_apply_covered_commit_v1(&operation->xact, owner, side_owner_commit_covered)
		   == RF_SIDE_XACT_APPLY_OK;
}

static bool
side_owner_apply_xact(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	if (owner != NULL && owner->protected_set_active && operation != NULL
		&& operation->kind == RF_SIDE_ONLINE_OPERATION_XACT
		&& operation->xact.kind == RF_SIDE_XACT_ABORT && side_owner_cold_formed(owner)) {
		ClusterUndoDecoded target;
		SideAbortCoverage coverage = { .owner = owner, .operation = operation };

		if (!side_owner_authority_fresh(owner)
			|| !side_owner_cold_abort_ready(owner, operation, &target)
			|| side_owner_undo_target(owner, target.instance, target.segment_id, 0) == NULL)
			return false;
		if (operation->xact.cold_completion_effects)
			return rf_side_xact_apply_cold_abort_completion_v1(
					   &operation->xact, operation->owned_payload, operation->owned_payload_length,
					   &coverage, side_owner_abort_covered)
				   == RF_SIDE_XACT_APPLY_OK;
		return rf_side_xact_apply_covered_abort_v1(&operation->xact, &coverage,
												   side_owner_abort_covered)
			   == RF_SIDE_XACT_APPLY_OK;
	}

	if (owner != NULL && owner->protected_set_active && owner->undo_authority != NULL
		&& operation != NULL && operation->kind == RF_SIDE_ONLINE_OPERATION_XACT
		&& operation->xact.kind == RF_SIDE_XACT_COMMIT) {
		const xl_xact_tt_commit *d = &operation->xact.tt_delta;

		if (!side_owner_install_data(owner)
			|| !side_owner_install_target(
				owner, side_owner_undo_target(owner, d->instance, d->segment_id, 0)))
			return false;
		return side_owner_finish_commit(owner, operation);
	}
	return owner != NULL && owner->protected_set_active && side_owner_authority_fresh(owner)
		   && operation != NULL && operation->kind == RF_SIDE_ONLINE_OPERATION_XACT
		   && rf_side_xact_apply_owned_v1(&operation->xact, operation->owned_payload,
										  operation->owned_payload_length)
				  == RF_SIDE_XACT_APPLY_OK;
}

static bool
side_owner_abort_record_tt_covered(void *arg, const RfSideXactOperationV1 *operation)
{
	SideAbortCoverage *coverage = arg;
	RfSideOnlineProductionOwnerV1 *owner = coverage->owner;
	ClusterUndoDecoded aborted;
	SideUndoHeader *target;
	PGAlignedBlock current;

	if (!owner->protected_set_active || !side_owner_authority_fresh(owner)
		|| memcmp(operation, &coverage->operation->xact, sizeof(*operation)) != 0
		|| !side_owner_cold_abort_ready(owner, coverage->operation, &aborted))
		return false;
	target = side_owner_undo_target(owner, aborted.instance, aborted.segment_id, 0);
	return target != NULL && side_owner_install_target(owner, target)
		   && cluster_undo_smgr_recovery_read_block_v1(aborted.segment_id, aborted.instance, 0,
													   &target->file, current.data)
		   && memcmp(current.data, target->final.page.data, BLCKSZ) == 0
		   && side_owner_cold_abort_ready(owner, coverage->operation, &aborted)
		   && side_owner_authority_fresh(owner);
}

static bool
side_owner_apply_abort_record(RfSideOnlineProductionOwnerV1 *owner, uint32 index,
							  const RfSideOnlineOperationV1 *operation)
{
	ClusterUndoDecoded aborted;
	SideUndoHeader *target;
	RfSideUndoHeaderImageV1 prefix;
	PGAlignedBlock current;

	if (!side_owner_cold_abort_ready(owner, operation, &aborted))
		return false;
	target = side_owner_undo_target(owner, aborted.instance, aborted.segment_id, 0);
	/* The earlier TT_ABORT must already be installed. This native record may
	 * only sync and verify that exact prefix, never repair a missing TT step
	 * or install later DATA, a rebind, or the full-source final header. */
	if (target == NULL || !target->file.exists || target->file.size != UNDO_SEGMENT_SIZE_BYTES
		|| owner->undo_bytes_remaining < 8 * BLCKSZ
		|| rf_side_online_plan_prepare_undo_record_header_v1(
			   owner->protected_plan, index, &operation->identity, target->base.data, &prefix)
			   != RF_PAGE_PROOF_DETAIL_OK
		|| memcmp(target->base.data, prefix.page.data, BLCKSZ) != 0
		|| !side_owner_authority_fresh(owner)
		|| !cluster_undo_smgr_recovery_read_block_v1(aborted.segment_id, aborted.instance, 0,
													 &target->file, current.data)
		|| memcmp(current.data, target->base.data, BLCKSZ) != 0)
		return false;
	target->final = prefix;
	if (operation->xact.relation_count != 0) {
		Size available = rf_side_online_plan_scratch_available_v1(owner->protected_plan);
		SideAbortCoverage coverage = { owner, operation };

		/* Include every live SIDE allocation (and borrowed scratch), plus the
		 * conservative native stack peak, before SPACE grants its first hook. */
		if (owner->undo_bytes_remaining > available)
			return false;
		return rf_side_xact_apply_cold_abort_delete_completion_v1(
				   owner->protected_plan, index,
				   available - owner->undo_bytes_remaining + 8 * BLCKSZ, &coverage,
				   side_owner_abort_record_tt_covered)
			   == RF_SIDE_XACT_APPLY_OK;
	}
	return side_owner_install_target(owner, target)
		   && cluster_undo_smgr_recovery_read_block_v1(aborted.segment_id, aborted.instance, 0,
													   &target->file, current.data)
		   && memcmp(current.data, prefix.page.data, BLCKSZ) == 0
		   && side_owner_authority_fresh(owner) && side_owner_apply_xact(owner, operation);
}

static bool
side_owner_record_commit_covered(void *arg, const RfSideXactOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = arg;
	SideUndoHeader *target = side_owner_undo_target(owner, operation->tt_delta.instance,
													operation->tt_delta.segment_id, 0);
	PGAlignedBlock current;

	/* Called only after the structural owner proved every target. The
	 * selected prefix has already been compared with this one transition.
	 * SPACE proof may perform IO: preserve the original selected-record
	 * binding/readback on both sides of installation, not cached-fd reads. */
	return target != NULL && side_owner_authority_fresh(owner)
		   && cluster_undo_smgr_recovery_read_block_v1(target->segment_id, target->instance, 0,
													   &target->file, current.data)
		   && memcmp(current.data, target->base.data, BLCKSZ) == 0
		   && side_owner_install_target(owner, target)
		   && cluster_undo_smgr_recovery_read_block_v1(target->segment_id, target->instance, 0,
													   &target->file, current.data)
		   && memcmp(current.data, target->final.page.data, BLCKSZ) == 0
		   && side_owner_authority_fresh(owner) && side_owner_commit_covered(owner, operation);
}

static bool
side_owner_finish_drop_record(RfSideOnlineProductionOwnerV1 *owner,
							  const RfSideOnlineOperationV1 *operation)
{
	XLogReaderState record = { 0 };
	DecodedXLogRecord decoded = { 0 };
	const RfPageReplayRecordIdentityV1 *ref = &operation->identity.record;

	decoded.header.xl_rmid = ref->rmid;
	decoded.header.xl_info = ref->info;
	decoded.header.xl_crc = ref->record_crc;
	decoded.header.xl_xid = operation->xact.xid;
	decoded.max_block_id = -1;
	decoded.main_data = (char *)operation->owned_payload;
	decoded.main_data_len = operation->owned_payload_length;
	record.record = &decoded;
	record.ReadRecPtr = ref->read_rec_ptr;
	record.EndRecPtr = ref->end_rec_ptr;
	record.system_identifier = operation->xact.system_identifier;
	record.cluster_expected_thread_id = ref->origin_thread;
	return rf_side_xact_apply_cold_drop_completion_v1(&operation->xact, &record, owner,
													  side_owner_record_commit_covered)
		   == RF_SIDE_XACT_APPLY_OK;
}

static bool
side_owner_apply_commit_record(RfSideOnlineProductionOwnerV1 *owner, uint32 index,
							   const RfSideOnlineOperationV1 *operation)
{
	const xl_xact_tt_commit *delta;
	SideUndoHeader *target;
	RfSideUndoHeaderImageV1 prefix;
	PGAlignedBlock current;
	ClusterUndoHeaderPrepareResultV1 result;

	if (!side_owner_cold_commit_ready(owner, &operation->xact))
		return false;
	delta = &operation->xact.tt_delta;
	target = side_owner_undo_target(owner, delta->instance, delta->segment_id, 0);
	/* A COMMIT cannot materialize a segment from a different record's image,
	 * install future DATA, or consume the full-source final TT header. */
	if (target == NULL || !target->file.exists || target->file.size != UNDO_SEGMENT_SIZE_BYTES
		|| owner->undo_bytes_remaining < 8 * BLCKSZ
		|| rf_side_online_plan_prepare_undo_record_v1(
			   owner->protected_plan, index, &operation->identity, target->base.data, &prefix)
			   != RF_PAGE_PROOF_DETAIL_OK)
		return false;
	current = target->base;
	result = cluster_undo_prepare_commit_v1(
		delta->instance, delta->segment_id, delta->segment_generation, delta->slot_offset,
		delta->wrap, delta->xid, delta->commit_scn, target->base.data, current.data);
	/* Match the physical checksum of this record's changed whole-page write. */
	if (result == CLUSTER_UNDO_HEADER_APPLY
		&& memcmp(current.data, target->base.data, BLCKSZ) != 0
		&& !cluster_undo_segment_header_seal(current.data))
		return false;
	/* Reconstruction is a check, not permission to replay missing earlier TT
	 * steps here. Only the original single-record transition may change DATA. */
	if ((result != CLUSTER_UNDO_HEADER_APPLY && result != CLUSTER_UNDO_HEADER_ALREADY)
		|| memcmp(current.data, prefix.page.data, BLCKSZ) != 0 || !side_owner_authority_fresh(owner)
		|| !cluster_undo_smgr_recovery_read_block_v1(delta->segment_id, delta->instance, 0,
													 &target->file, current.data)
		|| memcmp(current.data, target->base.data, BLCKSZ) != 0)
		return false;
	target->final = prefix;
	if (operation->xact.space_drop_count != 0)
		return side_owner_finish_drop_record(owner, operation);
	return side_owner_install_target(owner, target)
		   && cluster_undo_smgr_recovery_read_block_v1(delta->segment_id, delta->instance, 0,
													   &target->file, current.data)
		   && memcmp(current.data, prefix.page.data, BLCKSZ) == 0
		   && side_owner_authority_fresh(owner) && side_owner_finish_commit(owner, operation);
}

/* Keep the mutually exclusive page scratch in each consumer, not in this
 * dispatcher: the eight-page bound must not depend on tail-call optimization. */
static bool
side_owner_apply_xact_record(void *arg, uint32 index, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = arg;

	if (owner == NULL || !owner->protected_set_active || !side_owner_cold_formed(owner)
		|| !side_owner_authority_fresh(owner) || operation == NULL
		|| operation->kind != RF_SIDE_ONLINE_OPERATION_XACT)
		return false;
	if (operation->xact.kind == RF_SIDE_XACT_ABORT)
		return side_owner_apply_abort_record(owner, index, operation);
	return side_owner_apply_commit_record(owner, index, operation);
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
		return side_owner_install_target(
			owner,
			side_owner_undo_target(owner, operation->undo.instance, operation->undo.segment_id, 0));
	}
	return owner != NULL && owner->protected_set_active && side_owner_authority_fresh(owner)
		   && operation != NULL && operation->kind == RF_SIDE_ONLINE_OPERATION_UNDO
		   && cluster_undo_apply_tt_v1(&operation->undo) == CLUSTER_UNDO_APPLY_OK;
}

/* The record-local header callback cannot make an absent or later undo page
 * into the selected head's DATA. Bind the complete existing block first. */
static bool
side_owner_head_data(RfSideOnlineProductionOwnerV1 *owner, const ClusterUndoDecoded *undo,
					 const ClusterUndoSmgrRecoveryFileV1 *file,
					 const RfSideUndoBlockImageV1 *expected, bool sync)
{
	PGAlignedBlock current;
	ClusterRecoveryUndoRecord record;
	uint32 segment, block;
	uint16 slot, row;

	if (!side_owner_authority_fresh(owner)
		|| !uba_decode_record(undo->first_undo_block, &segment, &block, &slot, &row)
		|| segment != undo->segment_id || slot != undo->slot_offset || !file->exists
		|| file->size != UNDO_SEGMENT_SIZE_BYTES
		|| !cluster_undo_smgr_recovery_read_block_v1(segment, undo->instance, block, file,
													 current.data)
		|| memcmp(current.data, expected->page.data, BLCKSZ) != 0
		|| cluster_recovery_undo_record_read(current.data, BLCKSZ, segment, block, undo->instance,
											 undo->first_undo_block, &record)
			   != CLUSTER_RECOVERY_UNDO_ENTRY
		|| record.header.xid != undo->xid || record.header.tt_wrap_plus1 == 0
		|| record.header.tt_wrap_plus1 - 1 != undo->wrap
		|| record.header.tt_slot_segment_id != segment
		|| record.header.tt_slot_id != (uint32)slot + 1)
		return false;
	if (sync
		&& (!cluster_undo_smgr_fsync_segment_file(segment, undo->instance)
			|| !cluster_undo_smgr_recovery_read_block_v1(segment, undo->instance, block, file,
														 current.data)
			|| memcmp(current.data, expected->page.data, BLCKSZ) != 0))
		return false;
	return side_owner_authority_fresh(owner);
}

static bool
side_owner_apply_undo_header_record(RfSideOnlineProductionOwnerV1 *owner, uint32 index,
									const RfSideOnlineOperationV1 *operation)
{
	const ClusterUndoDecoded *undo = &operation->undo;
	SideUndoHeader *target;
	RfSideUndoHeaderImageV1 prefix;
	RfSideUndoBlockImageV1 head_data;
	PGAlignedBlock current;
	bool image;

	if (undo->kind != CLUSTER_UNDO_KIND_SEGMENT_INIT
		&& undo->kind != CLUSTER_UNDO_KIND_SEGMENT_REUSE
		&& undo->kind != CLUSTER_UNDO_KIND_SEGMENT_RECYCLE
		&& undo->kind != CLUSTER_UNDO_KIND_TT_BIND && undo->kind != CLUSTER_UNDO_KIND_TT_SET_HEAD
		&& undo->kind != CLUSTER_UNDO_KIND_TT_CTRC_RELEASE
		&& !(undo->kind == CLUSTER_UNDO_KIND_TT_ABORT
			 && undo->format_version == CLUSTER_UNDO_TT_ABORT_EXACT_VERSION))
		return false;
	target = side_owner_undo_target(owner, undo->instance, undo->segment_id, 0);
	if (target == NULL
		|| rf_side_online_plan_prepare_undo_record_v1(
			   owner->protected_plan, index, &operation->identity, target->base.data, &prefix)
			   != RF_PAGE_PROOF_DETAIL_OK)
		return false;
	image = undo->kind == CLUSTER_UNDO_KIND_SEGMENT_INIT
			|| undo->kind == CLUSTER_UNDO_KIND_SEGMENT_REUSE;
	/* Only this record's full header may create/extend its segment. A delta
	 * cannot borrow a preceding or future image to materialize missing DATA. */
	if (image) {
		ClusterUndoSmgrRecoveryFileV1 materialized;

		if (!prefix.has_full_image || !side_owner_authority_fresh(owner)
			|| !cluster_undo_smgr_recovery_materialize_v1(undo->segment_id, undo->instance,
														  &target->file, target->base.data,
														  prefix.page.data)
			|| !cluster_undo_smgr_recovery_probe_v1(undo->segment_id, undo->instance, &materialized,
													current.data)
			|| !materialized.exists || materialized.size != UNDO_SEGMENT_SIZE_BYTES
			|| (target->file.exists
				&& (target->file.device != materialized.device
					|| target->file.inode != materialized.inode))
			|| memcmp(current.data, target->base.data, BLCKSZ) != 0)
			return false;
		target->file = materialized;
	} else if (!target->file.exists || target->file.size != UNDO_SEGMENT_SIZE_BYTES)
		return false;
	if (undo->kind == CLUSTER_UNDO_KIND_TT_CTRC_RELEASE) {
		ClusterUndoHeaderPrepareResultV1 result;

		/* Redo this retained release only. Its original exact transition
		 * must explain the entire selected prefix from the observed DATA;
		 * release cannot install a missing earlier terminal or another slot. */
		current = target->base;
		result = cluster_undo_prepare_header_v1(undo, NULL, 0, target->base.data, current.data);
		/* Match the physical checksum of this record's changed whole-page write. */
		if (result == CLUSTER_UNDO_HEADER_APPLY
			&& memcmp(current.data, target->base.data, BLCKSZ) != 0
			&& !cluster_undo_segment_header_seal(current.data))
			return false;
		if ((result != CLUSTER_UNDO_HEADER_APPLY && result != CLUSTER_UNDO_HEADER_ALREADY)
			|| memcmp(current.data, prefix.page.data, BLCKSZ) != 0)
			return false;
	}
	if (!side_owner_authority_fresh(owner)
		|| !cluster_undo_smgr_recovery_read_block_v1(undo->segment_id, undo->instance, 0,
													 &target->file, current.data)
		|| memcmp(current.data, target->base.data, BLCKSZ) != 0)
		return false;
	if (undo->kind == CLUSTER_UNDO_KIND_TT_SET_HEAD
		&& (rf_side_online_plan_prepare_undo_head_block_v1(owner->protected_plan, index,
														   &operation->identity, &head_data)
				!= RF_PAGE_PROOF_DETAIL_OK
			|| !side_owner_head_data(owner, undo, &target->file, &head_data, true)))
		return false;
	/* Do not install other targets or the whole-source final header. An
	 * exact TT_ABORT restores only its canonical slot; the ordered native
	 * ABORT still owns CLOG and the remaining transaction effects. */
	target->final = prefix;
	return side_owner_install_target(owner, target)
		   && cluster_undo_smgr_recovery_read_block_v1(undo->segment_id, undo->instance, 0,
													   &target->file, current.data)
		   && memcmp(current.data, prefix.page.data, BLCKSZ) == 0
		   && (undo->kind != CLUSTER_UNDO_KIND_TT_SET_HEAD
			   || side_owner_head_data(owner, undo, &target->file, &head_data, false))
		   && side_owner_authority_fresh(owner);
}

static bool
side_owner_apply_undo_record(void *arg, uint32 index, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = arg;
	const ClusterUndoDecoded *undo;
	SideUndoHeader *segment, *target;
	RfSideUndoHeaderImageV1 header_prefix, block_prefix;
	PGAlignedBlock current;
	UndoBlockHeader observed;

	if (owner == NULL || !owner->protected_set_active || !side_owner_cold_formed(owner)
		|| !side_owner_authority_fresh(owner) || operation == NULL
		|| operation->kind != RF_SIDE_ONLINE_OPERATION_UNDO)
		return false;
	undo = &operation->undo;
	/* Include the selected header/DATA images, nested header preparation and
	 * the bounded complete-record reader, not only cached target allocations. */
	if ((undo->kind == CLUSTER_UNDO_KIND_TT_SET_HEAD
		 || undo->kind == CLUSTER_UNDO_KIND_TT_CTRC_RELEASE)
		&& owner->undo_bytes_remaining < 12 * BLCKSZ)
		return false;
	if (undo->kind != CLUSTER_UNDO_KIND_BLOCK_WRITE
		&& undo->kind != CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI)
		return side_owner_apply_undo_header_record(owner, index, operation);
	segment = side_owner_undo_target(owner, undo->instance, undo->segment_id, 0);
	target = side_owner_undo_target(owner, undo->instance, undo->segment_id, undo->block_no);
	/* Earlier segment lifecycle/TT records must already be durable. Do not
	 * create a file or install the full-source header on a block callback. */
	if (segment == NULL || target == NULL || !segment->file.exists
		|| segment->file.size != UNDO_SEGMENT_SIZE_BYTES
		|| rf_side_online_plan_prepare_undo_record_header_v1(owner->protected_plan, index,
															 &operation->identity,
															 segment->base.data, &header_prefix)
			   != RF_PAGE_PROOF_DETAIL_OK
		|| memcmp(segment->base.data, header_prefix.page.data, BLCKSZ) != 0
		|| rf_side_online_plan_prepare_undo_record_v1(owner->protected_plan, index,
													  &operation->identity, NULL, &block_prefix)
			   != RF_PAGE_PROOF_DETAIL_OK)
		return false;
	/* A later observed block cannot be rewound by this record-local entry.
	 * Equal LSN alone is not an idempotence proof: all bytes must match. */
	memcpy(&observed, target->base.data, sizeof(observed));
	if (observed.block_lsn >= operation->identity.record.end_rec_ptr
		&& memcmp(target->base.data, block_prefix.page.data, BLCKSZ) != 0)
		return false;
	if (!side_owner_authority_fresh(owner)
		|| !cluster_undo_smgr_recovery_read_block_v1(undo->segment_id, undo->instance, 0,
													 &segment->file, current.data)
		|| memcmp(current.data, header_prefix.page.data, BLCKSZ) != 0)
		return false;
	/* The full-source preflight remains intact; replace only this private
	 * target's result. Original exact read/write/sync/readback owns the IO. */
	target->final = block_prefix;
	return side_owner_install_target(owner, target)
		   && cluster_undo_smgr_recovery_read_block_v1(undo->segment_id, undo->instance, 0,
													   &segment->file, current.data)
		   && memcmp(current.data, header_prefix.page.data, BLCKSZ) == 0
		   && side_owner_authority_fresh(owner);
}

static bool
side_owner_apply_projection(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = (RfSideOnlineProductionOwnerV1 *)arg;

	if (owner == NULL || !owner->protected_set_active || !side_owner_authority_fresh(owner)
		|| operation == NULL || operation->kind != RF_SIDE_ONLINE_OPERATION_PROJECTION)
		return false;
	if (side_owner_cold_formed(owner)
		&& operation->projection.kind == CLUSTER_SIDE_PROJECTION_CLOG) {
		int origin = operation->identity.record.origin_thread - 1;

		/* A derived reset/truncation cannot stand in for the native record.
		 * Keep the original WAL order, sync/readback and cold qualification. */
		if (!side_owner_cold_clog_ready(owner, operation)
			|| !cluster_clog_native_recovery_record_apply_v1(origin, &operation->projection)
			|| !cluster_clog_native_recovery_record_verify_v1(origin, &operation->projection)
			|| !side_owner_cold_clog_ready(owner, operation) || !side_owner_authority_fresh(owner))
			return false;
	}
	/* The original MX consumer owns native apply/readback and only then
	 * publishes the derived overlay. Recheck cold input before entering it. */
	if (side_owner_cold_formed(owner)
		&& operation->projection.kind == CLUSTER_SIDE_PROJECTION_MULTIXACT
		&& !side_owner_cold_multixact_ready(owner, operation))
		return false;
	return rf_side_online_projection_apply_owned_v1(&owner->projection, operation);
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

/*
 * These records describe the failed writer's native control/standby state.
 * The survivor retains its own ControlFile and striped/leased allocators.
 * This is an explicit original-source consumer, not permission to discard
 * unknown SIDE work. In particular cache invalidations need the RESET owner.
 */
static bool
side_owner_assignment(const RfSideOnlineOperationV1 *operation, bool apply)
{
	int origin = operation->identity.record.origin_thread - 1;
	xl_xact_assignment *copy;
	volatile bool ok = false;

	if (!rf_side_xact_assignment_preflight_v1(operation->owned_payload,
											  operation->owned_payload_length))
		return false;
	copy = owner_alloc(operation->owned_payload_length);
	if (copy == NULL)
		return false;
	memcpy(copy, operation->owned_payload, operation->owned_payload_length);
	PG_TRY();
	{
		ok = cluster_subtrans_native_recovery_preflight_v1(origin, copy->xtop, copy->nsubxacts,
														   copy->xsub);
		if (ok && apply)
			ok = cluster_undo_recovery_cold_source_sync_v1(origin)
				 && cluster_subtrans_native_recovery_apply_v1(origin, copy->xtop, copy->nsubxacts,
															  copy->xsub)
				 && cluster_subtrans_native_recovery_verify_v1(origin, copy->xtop, copy->nsubxacts,
															   copy->xsub);
	}
	PG_FINALLY();
	{
		owner_free(copy);
	}
	PG_END_TRY();
	return ok;
}

/* These native controls describe their source's control file/allocators or
 * standby readers. They do not replace the founder's own native redo. */
static bool
side_owner_native_control_kind(const RfSideOnlineOperationV1 *operation)
{
	const RfPageReplayRecordIdentityV1 *record = &operation->identity.record;
	uint8 info = record->info & ~XLR_INFO_MASK;

	if (record->rmid == RM_STANDBY_ID)
		return info == XLOG_STANDBY_LOCK || info == XLOG_RUNNING_XACTS;
	if (record->rmid != RM_XLOG_ID)
		return false;
	switch (info) {
	case XLOG_CHECKPOINT_SHUTDOWN:
	case XLOG_CHECKPOINT_ONLINE:
	case XLOG_NEXTOID:
	case XLOG_PARAMETER_CHANGE:
	case XLOG_FPW_CHANGE:
	case XLOG_SWITCH:
	case XLOG_NOOP:
	case XLOG_BACKUP_END:
	case XLOG_RESTORE_POINT:
	case XLOG_END_OF_RECOVERY:
	case XLOG_OVERWRITE_CONTRECORD:
		return true;
	default:
		return false;
	}
}

static bool
side_owner_cold_control_no_effect(const RfSideOnlineProductionOwnerV1 *owner,
								  const RfSideOnlineOperationV1 *operation)
{
	return side_owner_cold_formed(owner) && cluster_node_id >= 0
		   && cluster_node_id <= SCN_MAX_VALID_NODE_ID
		   && operation->identity.record.origin_thread != cluster_node_id + 1
		   && standbyState == STANDBY_DISABLED
		   && (side_owner_native_control_kind(operation)
			   || (operation->identity.record.rmid == RM_XACT_ID
				   && operation->identity.record.info == XLOG_XACT_INVALIDATIONS));
}

static bool
side_owner_cold_stripe_join(const RfSideOnlineProductionOwnerV1 *owner,
						   const RfSideOnlineOperationV1 *operation, bool apply)
{
	uint64 floor, epoch;
	const RfPageReplayRecordIdentityV1 *record = &operation->identity.record;

	return side_owner_cold_formed(owner) && cluster_node_id >= 0
		   && cluster_node_id <= SCN_MAX_VALID_NODE_ID
		   && record->origin_thread != cluster_node_id + 1
		   && standbyState == STANDBY_DISABLED
		   && record->rmid == RM_CLUSTER_XID_STRIPE_ID
		   && record->info == XLOG_CLUSTER_XID_STRIPE_JOIN
		   && !TransactionIdIsValid(operation->source_xid)
		   && !TransactionIdIsValid(operation->source_top_xid)
		   && rf_side_online_stripe_join_shape_v1(operation->owned_payload,
													 operation->owned_payload_length,
													 record->origin_thread, &floor, &epoch)
		   && cluster_xid_stripe_replay_join_v1(floor, epoch, record->origin_thread - 1, apply);
}

static bool
side_owner_preflight_control(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = arg;
	const ClusterUndoRecoveryScopeV1 *scope;
	const RfPageReplayRecordIdentityV1 *record;
	uint8 info;

	if (owner == NULL || !owner->protected_set_active || owner->undo_authority == NULL
		|| operation == NULL || operation->kind != RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL
		|| operation->history_only || !side_owner_authority_fresh(owner))
		return false;
	scope = &owner->undo_scope;
	record = &operation->identity.record;
	if (scope->authority != owner->undo_authority || scope->plan != owner->protected_plan
		|| scope->authority->serial_guard == NULL
		|| record->system_identifier != scope->duty.system_identifier
		|| memcmp(record->storage_uuid, scope->duty.storage_uuid, 16) != 0
		|| record->origin_thread == 0 || record->origin_thread != scope->cut.failed_thread
		|| record->origin_thread != scope->duty.origin_thread_id
		|| scope->cut.origin_owner_incarnation == 0
		|| scope->cut.origin_owner_incarnation != scope->duty.origin_owner_incarnation
		|| scope->cut.flags != RF_CONTRIBUTOR_CUT_COMPLETE
		|| record->timeline_id != scope->cut.timeline_id
		|| record->read_rec_ptr == InvalidXLogRecPtr
		|| record->read_rec_ptr < scope->cut.scan_begin_inclusive
		|| record->end_rec_ptr <= record->read_rec_ptr
		|| record->end_rec_ptr > scope->cut.scan_end_exclusive
		|| !cluster_undo_recovery_origin_authorized_v1(record->origin_thread - 1))
		return false;
	info = record->info & ~XLR_INFO_MASK;
	if (side_owner_cold_formed(owner)) {
		if (!cluster_undo_recovery_cold_origin_authorized_v1(record->origin_thread - 1))
			return false;
		if (record->rmid == RM_CLUSTER_XID_STRIPE_ID)
			return side_owner_cold_stripe_join(owner, operation, false);
		if (record->rmid == RM_XACT_ID && info == XLOG_XACT_ASSIGNMENT)
			return side_owner_assignment(operation, false);
		if (record->rmid == RM_STANDBY_ID && info == XLOG_INVALIDATIONS)
			return DatabasePath == NULL
				   && rf_side_standby_invalidation_preflight_v1(operation->owned_payload,
																operation->owned_payload_length);
		return side_owner_cold_control_no_effect(owner, operation);
	}
	if (scope->authority->serial_guard->mode != CLUSTER_RECOVERY_SERIAL_ONLINE)
		return false;
	return side_owner_native_control_kind(operation);
}

static bool
side_owner_apply_control(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProductionOwnerV1 *owner = arg;
	bool cold_no_effect;
	bool cold_join;

	if (!side_owner_preflight_control(arg, operation))
		return false;
	cold_no_effect = side_owner_cold_control_no_effect(owner, operation);
	cold_join = side_owner_cold_formed(owner)
				&& operation->identity.record.rmid == RM_CLUSTER_XID_STRIPE_ID;
	if (cold_join) {
		if (!side_owner_cold_stripe_join(owner, operation, true)
			|| !side_owner_preflight_control(arg, operation))
			return false;
	} else if (side_owner_cold_formed(owner) && operation->identity.record.rmid == RM_XACT_ID
		&& (operation->identity.record.info & ~XLR_INFO_MASK) == XLOG_XACT_ASSIGNMENT) {
		if (!side_owner_assignment(operation, true)
			|| !side_owner_preflight_control(arg, operation))
			return false;
	} else if (side_owner_cold_formed(owner) && operation->identity.record.rmid == RM_STANDBY_ID
			   && (operation->identity.record.info & ~XLR_INFO_MASK) == XLOG_INVALIDATIONS) {
		xl_invalidations *copy = owner_alloc(operation->owned_payload_length);

		if (copy == NULL)
			return false;
		memcpy(copy, operation->owned_payload, operation->owned_payload_length);
		PG_TRY();
		{
			/* The original native redo consumer, not a no-op or a terminal
			 * projection. Delivery here does not certify all-cohort RESET. */
			ProcessCommittedInvalidationMessages(
				copy->msgs, copy->nmsgs, copy->relcacheInitFileInval, copy->dbId, copy->tsId);
		}
		PG_FINALLY();
		{
			if (DatabasePath != NULL) {
				owner_free(DatabasePath);
				DatabasePath = NULL;
			}
			owner_free(copy);
		}
		PG_END_TRY();
		/* Free execution scratch before the full reparse; only one payload
		 * copy may be live. Never mark a scope lost during native IO complete. */
		if (!side_owner_preflight_control(arg, operation))
			return false;
	}
	cluster_scn_recovery_replay_observe(operation->control_scn);
	/* A source/role change cannot convert the cold no-effect classification
	 * into an online success after observing the original SCN. */
	if ((cold_no_effect || cold_join)
		&& ((cold_no_effect && !side_owner_cold_control_no_effect(owner, operation))
			|| !side_owner_preflight_control(arg, operation)))
		return false;
	return true;
}

static void
side_owner_apply_ops(RfSideOnlineProductionOwnerV1 *owner, RfSideOnlineApplyOpsV1 *ops)
{
	memset(ops, 0, sizeof(*ops));
	ops->arg = owner;
	ops->source_thread = side_owner_source_thread(owner);
	ops->begin_protected_set = side_owner_begin_protected_set;
	ops->end_protected_set = side_owner_end_protected_set;
	ops->prepare_apply = side_owner_prepare_apply;
	ops->preflight_xact = side_owner_preflight_xact;
	ops->preflight_undo = side_owner_preflight_undo;
	ops->preflight_projection = side_owner_preflight_projection;
	ops->apply_xact = side_owner_apply_xact;
	ops->apply_xact_record = side_owner_apply_xact_record;
	ops->apply_undo = side_owner_apply_undo;
	ops->apply_undo_record = side_owner_apply_undo_record;
	ops->apply_projection = side_owner_apply_projection;
	ops->preflight_control = side_owner_preflight_control;
	ops->apply_control = side_owner_apply_control;
	if (owner->preflight_space != NULL && owner->apply_space != NULL) {
		ops->preflight_space = side_owner_preflight_space;
		ops->apply_space = side_owner_apply_space;
	}
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
	side_owner_apply_ops(owner, &ops);

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
	side_owner_apply_ops(owner, &ops);

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

RfPageProofDetailV1
rf_side_online_production_apply_record_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
										  const RfPageOnlineRecordIdentityV1 *identity,
										  RfSideOnlineProductionOwnerV1 *owner)
{
	RfSideOnlineApplyOpsV1 ops;
	RfPageProofDetailV1 detail;

	if (owner == NULL || owner->protected_set_active)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	owner->protected_set_complete = false;
	if (plan == NULL || identity == NULL || !side_owner_cold_formed(owner))
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	owner->protected_plan = plan;
	side_owner_apply_ops(owner, &ops);
	ops.preflight_xact = side_owner_preflight_xact_record;
	PG_TRY();
	{
		detail = rf_side_online_plan_apply_record_v1(plan, index, identity, &ops);
	}
	PG_FINALLY();
	{
		owner->protected_plan = NULL;
	}
	PG_END_TRY();
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	/* This is a record result, never protected_set_complete. Recheck the
	 * original owner after scope cleanup just as the full-source path does. */
	return side_owner_cold_formed(owner) && side_owner_authority_fresh(owner)
			   ? RF_PAGE_PROOF_DETAIL_OK
			   : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

#endif
