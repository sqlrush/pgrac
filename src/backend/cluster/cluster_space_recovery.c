/*-------------------------------------------------------------------------
 *
 * cluster_space_recovery.c
 *    Protected canonical SPACE reservation recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_space_recovery.c
 *
 * NOTES
 *    Original HW/GCS and retained sources own the native buffer installation.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_space_recovery.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/smgr.h"
#include "storage/checksum.h"
#include "utils/resowner.h"

#ifdef USE_CLUSTER_UNIT
#define space_alloc(size_) calloc(1, (size_))
#define space_free(ptr_) free(ptr_)
#else
#define space_alloc(size_) palloc0(size_)
#define space_free(ptr_) pfree(ptr_)
#endif

typedef struct SpaceRecoveryTarget {
	ClusterSpaceIdentityKey key;
	PGAlignedBlock before[2];
	ClusterSpaceRecoveryImage final;
} SpaceRecoveryTarget;

struct ClusterSpaceRecoveryBatchV1 {
	const ClusterThreadRecoveryFabricPlanV1 *fabric;
	const RfSideOnlinePlanV1 *side;
	const ClusterThreadRecoveryAuthorityV1 *sources;
	uint32 source_count;
	uint32 target_count;
	uint32 operation_count;
	Size allocated_bytes;
	SpaceRecoveryTarget *targets;
	uint32 *order;
	SpaceRecoveryTarget *active;
	Buffer buffers[2];
	HwLock hw;
	ResourceOwner source_owner;
	ResourceOwner io_owner;
	ResourceOwner previous_owner;
	bool preflight_complete;
	bool installed;
};

static bool
space_sources_fresh_current_owner(const ClusterSpaceRecoveryBatchV1 *batch)
{
	if (batch == NULL || batch->source_count == 0
		|| batch->source_count
			   != cluster_thread_recovery_fabric_participant_count_v1(batch->fabric))
		return false;
	for (uint32 i = 0; i < batch->source_count; i++) {
		const ClusterThreadRecoveryAuthorityV1 *a = &batch->sources[i];
		RfContributorStreamCutV1 cut;

		if (cluster_thread_recovery_authority_revalidate_nowait_v1(a) != CLUSTER_THREAD_AUTHORITY_OK
			|| a->duty == NULL || a->root_snapshot == NULL || a->retention_pin == NULL
			|| a->retention_pin != batch->sources[0].retention_pin
			|| !cluster_thread_recovery_fabric_identity_matches_v1(
				batch->fabric, a->duty->system_identifier, a->duty->storage_uuid)
			|| !cluster_thread_recovery_fabric_identity_matches_v1(
				batch->fabric, a->root_snapshot->identity.system_identifier,
				a->root_snapshot->identity.storage_uuid)
			|| !cluster_thread_recovery_fabric_cut_v1(batch->fabric, i, &cut)
			|| cut.failed_thread == 0 || cut.failed_thread != a->duty->origin_thread_id
			|| cut.failed_thread > PGRAC_PAGE_LSN_ORIGIN_MAX + 1
			|| cut.failed_thread != a->root_snapshot->identity.origin_thread_id
			|| (i > 0 && cut.failed_thread <= batch->sources[i - 1].duty->origin_thread_id)
			|| cut.timeline_id == 0 || cut.timeline_id != a->root_snapshot->checkpoint_tli
			|| cut.timeline_id != a->root_snapshot->tail_tli
			|| cut.flags != RF_CONTRIBUTOR_CUT_COMPLETE
			|| cut.scan_begin_inclusive != a->root_snapshot->checkpoint_lower_lsn
			|| cut.scan_end_exclusive != a->root_snapshot->validated_tail_lsn_exclusive
			|| !cluster_thread_recovery_authority_covers_window_v1(
				a, cut.failed_thread, cut.scan_begin_inclusive, cut.scan_end_exclusive))
			return false;
	}
	return true;
}

/* Retention pins belong to the original source owner.  Native buffer I/O
 * uses our child so a caught ERROR can release its pins without releasing
 * the borrowed source authority.  Only this exact child can borrow the
 * original owner for the synchronous, non-acquiring authority check. */
static bool
space_sources_fresh(const ClusterSpaceRecoveryBatchV1 *batch)
{
	ResourceOwner saved;
	bool ok = false;

	if (batch == NULL || batch->source_owner == NULL)
		return false;
	if (CurrentResourceOwner == batch->source_owner)
		return space_sources_fresh_current_owner(batch);
	if (batch->io_owner == NULL || CurrentResourceOwner != batch->io_owner
		|| batch->previous_owner != batch->source_owner)
		return false;
	saved = CurrentResourceOwner;
	PG_TRY();
	{
		CurrentResourceOwner = batch->source_owner;
		ok = space_sources_fresh_current_owner(batch);
	}
	PG_FINALLY();
	{
		CurrentResourceOwner = saved;
	}
	PG_END_TRY();
	return ok;
}

/* Drop known content locks before the native child ResourceOwner aborts any
 * BufferIO/pin acquired by a ReadBuffer call that threw before returning. */
static void
space_target_release(ClusterSpaceRecoveryBatchV1 *batch)
{
	for (int i = 1; i >= 0; i--) {
		Buffer buffer = batch->buffers[i];

		if (!BufferIsValid(buffer))
			continue;
		if (!BufferIsLocal(buffer)
			&& LWLockHeldByMe(BufferDescriptorGetContentLock(GetBufferDescriptor(buffer - 1)))) {
			/* ERROR reset the hold count, but did not release this lock. */
			if (InterruptHoldoffCount == 0)
				HOLD_INTERRUPTS();
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		}
		ReleaseBuffer(buffer);
		batch->buffers[i] = InvalidBuffer;
	}
	if (batch->io_owner != NULL) {
		ResourceOwnerRelease(batch->io_owner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
		ResourceOwnerRelease(batch->io_owner, RESOURCE_RELEASE_LOCKS, false, false);
		ResourceOwnerRelease(batch->io_owner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);
		CurrentResourceOwner = batch->previous_owner;
		ResourceOwnerDelete(batch->io_owner);
		batch->io_owner = NULL;
	}
	if (batch->hw.held)
		cluster_hw_unlock(&batch->hw);
	batch->active = NULL;
}

static bool
space_disk_matches(SMgrRelation rel, BlockNumber block, const char *expected)
{
	PGAlignedBlock disk;

	smgrread(rel, SPACE_FORKNUM, block, disk.data);
	if (PageIsNew(disk.data) || !PageIsVerifiedForFork(disk.data, SPACE_FORKNUM, block, 0)
		|| (DataChecksumsEnabled()
			&& ((PageHeader)disk.data)->pd_checksum != pg_checksum_page(disk.data, block)))
		return false;
	/* Completion must not inherit ignore_checksum_failure.  Only after
	 * strict verification may the writer's private-copy checksum differ. */
	((PageHeader)disk.data)->pd_checksum = ((const PageHeaderData *)expected)->pd_checksum;
	return memcmp(disk.data, expected, BLCKSZ) == 0;
}

static bool
space_target_run(ClusterSpaceRecoveryBatchV1 *batch, uint32 index, bool apply)
{
	SpaceRecoveryTarget *target = &batch->targets[index];
	ClusterSpaceRecoveryImage prepared;
	RfSideOnlineOperationV1 source;
	ClusterResId resid;
	SMgrRelation rel;
	uint32 count;
	bool ok = false;
	volatile bool mutated = false, write_attempted = false;
	volatile uint32 before_state = 0;

	if (!space_sources_fresh(batch)
		|| cluster_smgr_which_for(target->key.locator, InvalidBackendId) != 1)
		return false;
	cluster_hw_resid_encode(target->key.locator, MAIN_FORKNUM, &resid);
	PG_TRY();
	{
		if (!cluster_hw_lock(&resid, &batch->hw) || !space_sources_fresh(batch))
			goto done;
		batch->previous_owner = CurrentResourceOwner;
		batch->io_owner = ResourceOwnerCreate(CurrentResourceOwner, "SPACE recovery I/O");
		CurrentResourceOwner = batch->io_owner;
		batch->active = target;
		rel = smgropen(target->key.locator, InvalidBackendId);
		if (!smgrexists(rel, SPACE_FORKNUM) || smgrnblocks(rel, SPACE_FORKNUM) != 2)
			goto done;
		for (BlockNumber i = 0; i < 2; i++) {
			batch->buffers[i] = ReadBufferWithoutRelcache(target->key.locator, SPACE_FORKNUM, i,
														  RBM_NORMAL, NULL, true);
			if (!BufferIsValid(batch->buffers[i]) || BufferIsLocal(batch->buffers[i]))
				goto done;
			LockBuffer(batch->buffers[i], i == 0 ? BUFFER_LOCK_SHARE : BUFFER_LOCK_EXCLUSIVE);
			if (BufferGetBlockNumber(batch->buffers[i]) != i)
				goto done;
		}
		if (!space_sources_fresh(batch)
			|| !cluster_bufmgr_pcm_x_content_holder_write_permitted(
				GetBufferDescriptor(batch->buffers[1] - 1))
			|| rf_side_online_plan_prepare_space_v1(batch->side, &target->key,
													BufferGetPage(batch->buffers[0]),
													BufferGetPage(batch->buffers[1]), batch->order,
													batch->operation_count, &count, &prepared)
				   != RF_PAGE_PROOF_DETAIL_OK
			|| prepared.source_index[0] != UINT32_MAX)
			goto done;
		if (prepared.source_index[1] == UINT32_MAX) {
			if (prepared.covered_by_successor_mask != 2 || prepared.apply_mask != 0)
				goto done;
		} else {
			if (!rf_side_online_plan_operation_v1(batch->side, prepared.source_index[1], &source))
				goto done;
			PageSetLSNPreserveOrigin(prepared.pages[1].data, source.identity.record.end_rec_ptr);
			if (!PageSetLSNOrigin(prepared.pages[1].data, source.identity.record.origin_thread - 1))
				goto done;
		}
		/* A successor's bytes cannot borrow a failed origin's WAL owner.
		 * Require its canonical physical image before qualifying coverage. */
		if (prepared.covered_by_successor_mask != 0
			&& !space_disk_matches(rel, 1, BufferGetPage(batch->buffers[1])))
			goto done;
		/* ADVANCE cannot supply missing identity-page bytes or their WAL. */
		if (!space_disk_matches(rel, 0, prepared.pages[0].data))
			goto done;
		/* Re-prepare under this HW/current-X hold. A legitimate survivor
		 * extension since preflight may cover the old cut; never overwrite
		 * it with the previously prepared lower HWM. Rollback uses this
		 * apply's own original bytes, not the earlier preflight snapshot. */
		for (int i = 0; i < 2; i++)
			memcpy(target->before[i].data, BufferGetPage(batch->buffers[i]), BLCKSZ);
		target->final = prepared;
		if (!apply) {
			ok = true;
			goto done;
		}
		if (prepared.covered_by_successor_mask != 0) {
			smgrimmedsync(rel, SPACE_FORKNUM);
			if (!space_sources_fresh(batch) || !space_disk_matches(rel, 0, target->before[0].data)
				|| !space_disk_matches(rel, 1, target->before[1].data))
				goto done;
			if (prepared.source_index[1] == UINT32_MAX) {
				ok = true;
				goto done;
			}
		}
		if (!space_sources_fresh(batch))
			goto done;
		/* The strict MarkBufferDirty predicate was checked before entering
 * the critical section. Content-X prevents a revoke from changing it. */
		START_CRIT_SECTION();
		before_state = pg_atomic_read_u32(&GetBufferDescriptor(batch->buffers[1] - 1)->state);
		memcpy(BufferGetPage(batch->buffers[1]), prepared.pages[1].data, BLCKSZ);
		MarkBufferDirty(batch->buffers[1]);
		mutated = true;
		END_CRIT_SECTION();
		if (!FlushOneBufferForSpaceRecovery(batch->buffers[1], batch, &write_attempted))
			goto done;
		smgrimmedsync(rel, SPACE_FORKNUM);
		ok = space_sources_fresh(batch) && space_disk_matches(rel, 0, prepared.pages[0].data)
			 && space_disk_matches(rel, 1, prepared.pages[1].data);
	done:;
	}
	PG_FINALLY();
	{
		if (mutated && !write_attempted) {
			BufferDesc *buf = GetBufferDescriptor(batch->buffers[1] - 1);
			const uint32 mask = BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR;
			uint32 state;

			/* Native I/O has already unwound. Until the first write attempt
			 * it is safe to restore exactly what this content-X owner saw.
			 * After a possible write, keep the conservative higher HWM;
			 * the recovery/isolation owner still has no completion proof. */
			Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE));
			memcpy(BufferGetPage(batch->buffers[1]), target->before[1].data, BLCKSZ);
			Assert(memcmp(BufferGetPage(batch->buffers[1]), target->before[1].data, BLCKSZ) == 0);
			state = LockBufHdr(buf);
			Assert((state & BM_IO_IN_PROGRESS) == 0);
			/* Checkpointer can select a previously dirty page while we hold
			 * content-X. Restoring its old bytes must retain that new duty;
			 * otherwise its redo cut could pass the original SPACE WAL. */
			if (before_state & BM_DIRTY)
				before_state |= state & BM_CHECKPOINT_NEEDED;
			state = (state & ~mask) | (before_state & mask);
			UnlockBufHdr(buf, state);
		}
		space_target_release(batch);
	}
	PG_END_TRY();
	return ok;
}

bool
cluster_space_recovery_preflight_v1(const ClusterThreadRecoveryFabricPlanV1 *plan,
									const ClusterThreadRecoveryAuthorityV1 *sources, uint32 count,
									ClusterSpaceRecoveryBatchV1 **out)
{
	ClusterSpaceRecoveryBatchV1 *batch;
	const RfSideOnlinePlanV1 *side;
	uint32 targets, operations;
	Size bytes;
	size_t scratch;
	volatile bool ok = false;

	if (out == NULL)
		return false;
	*out = NULL;
	if (!cluster_enabled || !cluster_shared_config || plan == NULL || sources == NULL || count == 0
		|| count > RF_PAGE_STABLE_MAX_PARTICIPANTS)
		return false;
	side = cluster_thread_recovery_fabric_side_plan_v1(plan);
	if (side == NULL)
		return false;
	targets = rf_side_online_plan_space_target_count_v1(side);
	operations = rf_side_online_plan_operation_count_v1(side);
	if (targets == UINT32_MAX || operations == UINT32_MAX
		|| targets > RF_SIDE_ONLINE_PLAN_MAX_BYTES / sizeof(SpaceRecoveryTarget)
		|| operations > RF_SIDE_ONLINE_PLAN_MAX_BYTES / sizeof(uint32))
		return false;
	bytes = sizeof(*batch) + (Size)targets * sizeof(SpaceRecoveryTarget)
			+ (Size)operations * sizeof(uint32) + 4 * BLCKSZ;
	scratch = targets == 0 ? 0 : cluster_space_recovery_scratch_bytes(operations);
	/* Preparation temporarily owns input pointers, two index arrays and
 * the common codec's scratch while our complete batch remains alive. */
	if ((targets != 0 && scratch == 0) || scratch > RF_SIDE_ONLINE_PLAN_MAX_BYTES
		|| bytes + scratch
				   + (Size)operations * (sizeof(ClusterSpaceRecoveryInput) + 2 * sizeof(uint32))
			   > rf_side_online_plan_scratch_available_v1(side))
		return false;
	/* No structural permission is inferred from the typed subrecord. */
	for (uint32 i = 0; i < operations; i++) {
		RfSideOnlineOperationV1 op;
		ClusterSpaceReservationChange change;

		if (!rf_side_online_plan_operation_v1(side, i, &op)
			|| (op.kind == RF_SIDE_ONLINE_OPERATION_XACT && op.xact.space_drop_count != 0))
			return false;
		if (op.kind == RF_SIDE_ONLINE_OPERATION_SPACE
			&& (op.identity.record.rmid != RM_SMGR_ID
				|| (op.identity.record.info & ~XLR_INFO_MASK) != XLOG_SMGR_SPACE_RESERVATION
				|| !cluster_space_reservation_wal_decode(op.owned_payload, op.owned_payload_length,
														 &change)
				|| change.action != CLUSTER_SPACE_RESERVATION_ADVANCE))
			return false;
	}
	batch = space_alloc(bytes - 4 * BLCKSZ);
	if (batch == NULL)
		return false;
	batch->fabric = plan;
	batch->side = side;
	batch->sources = sources;
	batch->source_owner = CurrentResourceOwner;
	batch->source_count = count;
	batch->target_count = targets;
	batch->operation_count = operations;
	batch->allocated_bytes = bytes - 4 * BLCKSZ;
	batch->targets = (SpaceRecoveryTarget *)(batch + 1);
	batch->order = (uint32 *)(batch->targets + targets);
	PG_TRY();
	{
		if (!space_sources_fresh(batch))
			goto done;
		for (uint32 i = 0; i < targets; i++)
			if (!rf_side_online_plan_space_target_v1(side, i, &batch->targets[i].key)
				|| !space_target_run(batch, i, false))
				goto done;
		ok = batch->preflight_complete = space_sources_fresh(batch);
	done:;
	}
	PG_FINALLY();
	{
		if (!ok)
			cluster_space_recovery_destroy_v1(&batch);
	}
	PG_END_TRY();
	if (ok)
		*out = batch;
	return ok;
}

bool
cluster_space_recovery_apply_v1(ClusterSpaceRecoveryBatchV1 *batch)
{
	if (batch == NULL || !batch->preflight_complete || batch->installed
		|| !space_sources_fresh(batch))
		return false;
	for (uint32 i = 0; i < batch->target_count; i++)
		if (!space_target_run(batch, i, true))
			return false;
	batch->installed = space_sources_fresh(batch);
	return batch->installed;
}

void
cluster_space_recovery_destroy_v1(ClusterSpaceRecoveryBatchV1 **batch)
{
	if (batch == NULL || *batch == NULL)
		return;
	space_target_release(*batch);
	space_free(*batch);
	*batch = NULL;
}

bool
cluster_space_recovery_flush_permitted_v1(const ClusterSpaceRecoveryBatchV1 *batch, Buffer buffer)
{
	RelFileLocator locator;
	ForkNumber fork;
	BlockNumber block;

	if (batch == NULL || batch->active == NULL || !batch->preflight_complete || !batch->hw.held
		|| !batch->hw.coordinated || batch->hw.req.lockmode != ExclusiveLock
		|| !BufferIsValid(buffer) || buffer != batch->buffers[1] || BufferIsLocal(buffer)
		|| !space_sources_fresh(batch))
		return false;
	BufferGetTag(buffer, &locator, &fork, &block);
	return RelFileLocatorEquals(locator, batch->active->key.locator) && fork == SPACE_FORKNUM
		   && block == CLUSTER_SPACE_RESERVATION_BLOCK
		   && memcmp(BufferGetPage(buffer), batch->active->final.pages[1].data, BLCKSZ) == 0;
}

Size
cluster_space_recovery_scratch_held_v1(const ClusterSpaceRecoveryBatchV1 *batch)
{
	return batch != NULL ? batch->allocated_bytes : 0;
}

bool
cluster_space_recovery_preflight_operation_v1(void *arg, const RfSideOnlineOperationV1 *operation)
{
	const ClusterSpaceRecoveryBatchV1 *batch = arg;

	if (batch == NULL || !batch->preflight_complete || operation == NULL
		|| operation->kind != RF_SIDE_ONLINE_OPERATION_SPACE || !space_sources_fresh(batch))
		return false;
	for (uint32 i = 0; i < batch->operation_count; i++) {
		RfSideOnlineOperationV1 expected;

		if (!rf_side_online_plan_operation_v1(batch->side, i, &expected))
			return false;
		if (memcmp(&operation->identity, &expected.identity, sizeof(expected.identity)) == 0)
			return expected.kind == RF_SIDE_ONLINE_OPERATION_SPACE
				   && operation->owned_payload_length == expected.owned_payload_length
				   && operation->owned_payload != NULL && expected.owned_payload != NULL
				   && memcmp(operation->owned_payload, expected.owned_payload,
							 expected.owned_payload_length)
						  == 0;
	}
	return false;
}

bool
cluster_space_recovery_applied_operation_v1(void *arg, const RfSideOnlineOperationV1 *operation)
{
	const ClusterSpaceRecoveryBatchV1 *batch = arg;

	return batch != NULL && batch->installed
		   && cluster_space_recovery_preflight_operation_v1(arg, operation);
}
