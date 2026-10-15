/*-------------------------------------------------------------------------
 * cluster_recovery_wal_targets.c
 *    Preserve retained WAL targets in the cold physical namespace union.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_page_detached.h"
#include "cluster/cluster_recovery_wal_targets.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_uba.h"

#ifdef USE_PGRAC_CLUSTER

static bool
targets_record_in_cut(const XLogReaderState *reader, const ClusterWalSourceRef *source,
					  const RfContributorStreamCutV1 *cut)
{
	return reader != NULL && reader->record != NULL && source != NULL && cut != NULL
		   && cluster_wal_claim_v2_ref_valid(&source->claim) && source->timeline != 0
		   && reader->system_identifier == source->claim.identity.system_identifier
		   && reader->ReadRecPtr != InvalidXLogRecPtr && reader->EndRecPtr > reader->ReadRecPtr
		   && reader->record->lsn == reader->ReadRecPtr
		   && reader->record->next_lsn == reader->EndRecPtr
		   && reader->record->header.xl_prev < reader->ReadRecPtr
		   && reader->record->max_block_id >= -1 && reader->record->max_block_id <= XLR_MAX_BLOCK_ID
		   && (reader->record->main_data_len == 0 || reader->record->main_data != NULL)
		   && cut->flags == RF_CONTRIBUTOR_CUT_COMPLETE
		   && cut->failed_thread == source->claim.identity.origin_thread_id
		   && cut->timeline_id == source->timeline && cut->origin_owner_incarnation != 0
		   && cut->origin_owner_incarnation == source->claim.identity.origin_owner_incarnation
		   && cut->scan_begin_inclusive != InvalidXLogRecPtr
		   && cut->scan_begin_inclusive <= reader->ReadRecPtr
		   && cut->scan_end_exclusive >= reader->EndRecPtr;
}

/* These are the original cold target policies, not SIDE apply owners.
 * An accepted non-page route is explicitly returned as unowned below.
 * Mixed routed SIDE components remain refused, never silently skipped. */
static RfPageProofDetailV1
targets_side_record(void *arg, const RfOpcodeRouteV1 *route, const RfPageVersionEdgeEntryV1 *edge,
					const DecodedBkpBlock *block)
{
	(void)arg;
	return route != NULL && edge == NULL && block == NULL
				   && (route->record_owner == RF_ROUTE_OWNER_SIDE_TYPED
					   || route->record_owner == RF_ROUTE_OWNER_LOGICAL_NOOP)
			   ? RF_PAGE_PROOF_DETAIL_OK
			   : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

static RfPageProofDetailV1
targets_rebuildable(void *arg, const RfOpcodeRouteV1 *route, const RfPageVersionEdgeEntryV1 *edge,
					const DecodedBkpBlock *block)
{
	(void)arg;
	return route != NULL && route->record_owner == RF_ROUTE_OWNER_PAGE_CODEC && edge != NULL
				   && block != NULL && edge->page_class == RF_PAGE_CLASS_REBUILDABLE_FSM
			   ? RF_PAGE_PROOF_DETAIL_OK
			   : RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN;
}

static bool
targets_key_matches(const ClusterSpaceIdentityKey *key, const ClusterWalSourceRef *source)
{
	return key->system_identifier == source->claim.identity.system_identifier
		   && key->database_incarnation == source->claim.database_incarnation
		   && memcmp(key->storage_uuid, source->claim.identity.storage_uuid, 16) == 0;
}

static bool
targets_all_forks(ClusterRecoveryRootSet *roots, const ClusterSpaceIdentityKey *key,
				  ClusterRecoveryWalTargetsDiag *diag)
{
	const ForkNumber forks[] = { MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM, SPACE_FORKNUM };

	for (unsigned i = 0; i < lengthof(forks); i++) {
		diag->roots_result = cluster_recovery_root_set_add_fork(roots, key, forks[i],
																forks[i] == SPACE_FORKNUM ? 2 : 0);
		if (diag->roots_result != CLUSTER_RECOVERY_ROOTS_MORE)
			return false;
	}
	diag->structural_effects++;
	return true;
}

static bool
targets_structure(ClusterRecoveryRootSet *roots, const ClusterWalSourceRef *source,
				  const void *bytes, size_t length, bool tombstone,
				  ClusterRecoveryWalTargetsDiag *diag)
{
	ClusterSpaceStructureChange change;

	if (!cluster_space_structure_wal_decode(bytes, length, &change)
		|| (tombstone ? change.identity.action != CLUSTER_SPACE_WAL_TOMBSTONE
					  : change.identity.action != CLUSTER_SPACE_WAL_CREATE
							&& change.identity.action != CLUSTER_SPACE_WAL_TRUNCATE)
		|| !targets_key_matches(&change.identity.result.key, source)) {
		diag->route_detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		return false;
	}
	return targets_all_forks(roots, &change.identity.result.key, diag);
}

static bool
targets_undo(ClusterRecoveryRootSet *roots, XLogReaderState *reader,
			 const ClusterWalSourceRef *source, ClusterRecoveryWalTargetsDiag *diag)
{
	ClusterUndoDecoded undo;
	uint64 required = 1;

	/* The original pure historical classifier accepts generation zero.
	 * Reusing its syntax checks does not create a PI plan or permit apply.
	 * Full payload/lifecycle effects remain unowned graph obligations. */
	if (!cluster_undo_decode(reader, &undo) || !cluster_undo_inspect_pi(&undo))
		return false;
	if (undo.kind == CLUSTER_UNDO_KIND_BLOCK_WRITE
		|| undo.kind == CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI)
		required = (uint64)undo.block_no + 1;
	else if (undo.kind == CLUSTER_UNDO_KIND_TT_SET_HEAD) {
		uint32 segment, block;
		uint16 slot, row;

		if (!uba_decode_record(undo.first_undo_block, &segment, &block, &slot, &row)
			|| segment != undo.segment_id || slot != undo.slot_offset)
			return false;
		required = (uint64)block + 1;
	}
	diag->roots_result = cluster_recovery_root_set_add_undo(
		roots, source->claim.identity.system_identifier, source->claim.database_incarnation,
		source->claim.identity.storage_uuid, undo.instance, undo.segment_id, required);
	if (diag->roots_result != CLUSTER_RECOVERY_ROOTS_MORE)
		return false;
	diag->undo_targets++;
	diag->unowned_records = 1;
	return true;
}

static bool
targets_native(ClusterRecoveryRootSet *roots, XLogReaderState *reader,
			   const ClusterWalSourceRef *source, const RfContributorStreamCutV1 *cut,
			   size_t memory_budget, ClusterRecoveryWalTargetsDiag *diag)
{
	ClusterRecoveryNativeScan *scan = NULL;
	ClusterRecoveryNativeEffect effect;
	bool result = false;

	diag->native_result
		= cluster_recovery_native_scan_begin(reader, source, cut, memory_budget, &scan);
	if (diag->native_result != CLUSTER_RECOVERY_NATIVE_MORE)
		return false;
	for (;;) {
		diag->native_result = cluster_recovery_native_scan_next(scan, &effect);
		if (diag->native_result == CLUSTER_RECOVERY_NATIVE_END) {
			result = true;
			break;
		}
		if (diag->native_result != CLUSTER_RECOVERY_NATIVE_EFFECT)
			break;
		diag->native_effects++;
		if (effect.kind == CLUSTER_RECOVERY_NATIVE_CREATE
			|| effect.kind == CLUSTER_RECOVERY_NATIVE_RELATION_DROP) {
			diag->roots_result = cluster_recovery_root_set_add_native(roots, &effect);
			if (diag->roots_result != CLUSTER_RECOVERY_ROOTS_MORE)
				break;
			diag->structural_effects++;
		} else if (effect.kind == CLUSTER_RECOVERY_NATIVE_TT_COMMIT) {
			diag->roots_result = cluster_recovery_root_set_add_native(roots, &effect);
			if (diag->roots_result != CLUSTER_RECOVERY_ROOTS_MORE)
				break;
			diag->undo_targets++;
			diag->other_native_effects++;
		} else if (effect.kind == CLUSTER_RECOVERY_NATIVE_SPACE_DROP) {
			if (!targets_structure(roots, source, effect.bytes, effect.length, true, diag))
				break;
		} else
			diag->other_native_effects++;
	}
	cluster_recovery_native_scan_destroy(scan);
	return result;
}

bool
cluster_recovery_wal_targets_collect(ClusterRecoveryRootSet *roots, XLogReaderState *reader,
									 const ClusterWalSourceRef *source,
									 const RfContributorStreamCutV1 *cut, size_t memory_budget,
									 ClusterRecoveryWalTargetsDiag *diag)
{
	RfDetachedOwnerOpsV1 ops = { 0 };
	RfDetachedRecordPlanV1 plan;
	ClusterSpaceIdentityKey key = { 0 };
	uint8 rmid, opcode;

	if (diag != NULL)
		memset(diag, 0, sizeof(*diag));
	if (diag == NULL || !targets_record_in_cut(reader, source, cut))
		goto fail;
	if (!cluster_recovery_root_set_targets_match(roots, source->claim.identity.system_identifier,
												 source->claim.database_incarnation,
												 source->claim.identity.storage_uuid)) {
		diag->route_detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		goto fail;
	}
	rmid = XLogRecGetRmid(reader);
	opcode = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
	if (rmid == RM_XACT_ID || (rmid == RM_SMGR_ID && opcode == XLOG_SMGR_CREATE)) {
		if (!targets_native(roots, reader, source, cut, memory_budget, diag))
			goto fail;
		return true;
	}
	ops.preflight_side_record = targets_side_record;
	ops.preflight_rebuildable_component = targets_rebuildable;
	diag->route_detail = rf_page_detached_preflight_v1(reader, true, &ops, &plan);
	if (diag->route_detail != RF_PAGE_PROOF_DETAIL_OK)
		goto fail;
	key.system_identifier = source->claim.identity.system_identifier;
	key.database_incarnation = source->claim.database_incarnation;
	memcpy(key.storage_uuid, source->claim.identity.storage_uuid, 16);
	for (uint32 i = 0; i < plan.component_count; i++) {
		const RfDetachedComponentPlanV1 *component = &plan.components[i];
		const DecodedBkpBlock *block = &reader->record->blocks[component->block_id];

		/* Unlike the replay plan, D1 must keep the rebuildable file too. */
		if ((component->owner != RF_DETACHED_COMPONENT_PAGE_CODEC
			 && component->owner != RF_DETACHED_COMPONENT_REBUILDABLE)
			|| block->blkno == InvalidBlockNumber)
			goto fail;
		key.locator = block->rlocator;
		diag->roots_result = cluster_recovery_root_set_add_fork(roots, &key, block->forknum,
																(uint64)block->blkno + 1);
		if (diag->roots_result != CLUSTER_RECOVERY_ROOTS_MORE)
			goto fail;
		diag->page_targets++;
		if (component->owner == RF_DETACHED_COMPONENT_REBUILDABLE)
			diag->rebuildable_targets++;
	}
	if (plan.route.record_owner == RF_ROUTE_OWNER_PAGE_CODEC)
		return true;
	if (rmid == RM_CLUSTER_UNDO_ID) {
		if (!targets_undo(roots, reader, source, diag))
			goto fail;
		return true;
	}
	if (rmid == RM_SMGR_ID && opcode == XLOG_SMGR_SPACE_IDENTITY) {
		if (!targets_structure(roots, source, XLogRecGetData(reader), XLogRecGetDataLen(reader),
							   false, diag))
			goto fail;
	} else if (rmid == RM_SMGR_ID && opcode == XLOG_SMGR_SPACE_RESERVATION) {
		ClusterSpaceReservationChange change;

		if (!cluster_space_reservation_wal_decode(XLogRecGetData(reader), XLogRecGetDataLen(reader),
												  &change)
			|| change.action != CLUSTER_SPACE_RESERVATION_ADVANCE
			|| !targets_key_matches(&change.result.identity.key, source)
			|| !targets_all_forks(roots, &change.result.identity.key, diag))
			goto fail;
	} else if (rmid == RM_SMGR_ID && opcode == XLOG_SMGR_TRUNCATE) {
		xl_smgr_truncate truncate;
		const ForkNumber forks[]
			= { MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM, SPACE_FORKNUM };
		const int flags[]
			= { SMGR_TRUNCATE_HEAP, SMGR_TRUNCATE_FSM, SMGR_TRUNCATE_VM, SMGR_TRUNCATE_ALL };

		if (XLogRecGetDataLen(reader) != sizeof(truncate))
			goto fail;
		memcpy(&truncate, XLogRecGetData(reader), sizeof(truncate));
		if (truncate.blkno == InvalidBlockNumber || truncate.flags == 0
			|| (truncate.flags & ~SMGR_TRUNCATE_ALL) != 0)
			goto fail;
		key.locator = truncate.rlocator;
		for (unsigned i = 0; i < lengthof(forks); i++)
			if (truncate.flags & flags[i]) {
				diag->roots_result = cluster_recovery_root_set_add_fork(roots, &key, forks[i], 0);
				if (diag->roots_result != CLUSTER_RECOVERY_ROOTS_MORE)
					goto fail;
			}
		diag->structural_effects++;
	} else
		diag->unowned_records = 1;
	return true;

fail:
	cluster_recovery_root_set_invalidate(roots);
	return false;
}
#endif
