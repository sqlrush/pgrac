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
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_space_recovery.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/smgr.h"
#include "storage/checksum.h"
#include "utils/resowner.h"
#include "utils/memutils.h"

#ifdef USE_CLUSTER_UNIT
#define space_alloc(size_) calloc(1, (size_))
#define space_free(ptr_) free(ptr_)
#else
#define space_alloc(size_) palloc0(size_)
#define space_free(ptr_) pfree(ptr_)
#endif

typedef struct SpaceRecoveryTarget {
	ClusterSpaceIdentityKey key;
	bool has_create;
	bool has_structure;
	uint32 operation_count;
	PGAlignedBlock before[2];
	ClusterSpaceRecoveryImage final;
} SpaceRecoveryTarget;

struct ClusterSpaceRecoveryBatchV1 {
	const ClusterThreadRecoveryFabricPlanV1 *fabric;
	ClusterRecoveryFencePlan *cold_fence;
	const RfSideOnlinePlanV1 *side;
	const ClusterSpaceRecoveryInput *cold_inputs;
	const ClusterSpaceColdSourceV1 *cold_origins;
	const ClusterThreadRecoveryAuthorityV1 *sources;
	uint32 source_count;
	uint32 target_count;
	uint32 operation_count;
	Size allocated_bytes;
	SpaceRecoveryTarget *targets;
	uint32 *order;
	SpaceRecoveryTarget *active;
	uint32 active_step;
	uint32 cold_shrink_step;
	uint8 cold_shrink_forks;
	Buffer buffers[2];
	HwLock hw;
	ResourceOwner source_owner;
	ResourceOwner io_owner;
	ResourceOwner previous_owner;
	bool cold_drop_already;
	bool preflight_complete;
	bool installed;
};

typedef struct SpaceColdAuthority {
	ClusterControlRootSnapshot root;
	ClusterControlRootReadToken token;
	XLogRecPtr native_redo;
} SpaceColdAuthority;

static bool
space_cold_phase(void)
{
	return cluster_enabled && cluster_shared_config && AmStartupProcess() && RecoveryInProgress()
		   && cluster_cold_replay_window_active_v1() && cluster_recovery_merge_claim_is_held();
}

static bool
space_batch_cold(const ClusterSpaceRecoveryBatchV1 *batch)
{
	return batch->cold_fence != NULL || batch->cold_inputs != NULL;
}

static bool
space_sources_fresh_current_owner(const ClusterSpaceRecoveryBatchV1 *batch)
{
	if (batch == NULL || batch->source_count == 0)
		return false;
	if (batch->cold_inputs != NULL) {
		ClusterRecoverySerialGuard *guards[CLUSTER_WAL_RETENTION_MAX_THREADS];
		ClusterWalRetentionPin *pin = NULL;
		uint16 count = 0;

		if (!space_cold_phase() || batch->fabric != NULL || batch->side != NULL
			|| batch->cold_fence != NULL || batch->cold_origins == NULL
			|| cluster_wal_retention_pin_borrow_cold_v1(&pin, guards, lengthof(guards), &count)
				   != CLUSTER_WAL_PIN_OK
			|| count != batch->source_count || pin != batch->sources[0].retention_pin)
			return false;
		for (uint16 i = 0; i < count; i++)
			if (guards[i] != batch->sources[i].serial_guard)
				return false;
	} else if (batch->cold_fence != NULL) {
		if (!AmStartupProcess() || !RecoveryInProgress() || batch->fabric != NULL
			|| !cluster_recovery_merge_fence_plan_revalidate_nowait(batch->cold_fence)
			|| batch->source_count
				   != cluster_recovery_merge_fence_plan_origin_count(batch->cold_fence)
			|| batch->source_count != rf_side_online_plan_participant_count_v1(batch->side))
			return false;
	} else if (batch->source_count
			   != cluster_thread_recovery_fabric_participant_count_v1(batch->fabric))
		return false;
	for (uint32 i = 0; i < batch->source_count; i++) {
		const ClusterThreadRecoveryAuthorityV1 *a = &batch->sources[i];
		RfContributorStreamCutV1 cut;

		if (cluster_thread_recovery_authority_revalidate_nowait_v1(a) != CLUSTER_THREAD_AUTHORITY_OK
			|| a->duty == NULL || a->root_snapshot == NULL || a->retention_pin == NULL
			|| a->retention_pin != batch->sources[0].retention_pin)
			return false;
		if (space_batch_cold(batch)) {
			if (a->serial_guard == NULL || !a->serial_guard->held
				|| a->serial_guard->mode != CLUSTER_RECOVERY_SERIAL_COLD_FORMED)
				return false;
			cut = (RfContributorStreamCutV1){
				.failed_thread = a->duty->origin_thread_id,
				.origin_owner_incarnation = a->duty->origin_owner_incarnation,
				.timeline_id = a->root_snapshot->checkpoint_tli,
				.flags = RF_CONTRIBUTOR_CUT_COMPLETE,
				.scan_begin_inclusive = a->root_snapshot->checkpoint_lower_lsn,
				.scan_end_exclusive = a->root_snapshot->validated_tail_lsn_exclusive
			};
			if (batch->cold_inputs == NULL
				&& (!rf_side_online_plan_source_matches_v1(batch->side, a->duty->system_identifier,
														   a->duty->storage_uuid, &cut)
					|| !rf_side_online_plan_source_matches_v1(
						batch->side, a->root_snapshot->identity.system_identifier,
						a->root_snapshot->identity.storage_uuid, &cut)))
				return false;
		} else if (!cluster_thread_recovery_fabric_identity_matches_v1(
					   batch->fabric, a->duty->system_identifier, a->duty->storage_uuid)
				   || !cluster_thread_recovery_fabric_identity_matches_v1(
					   batch->fabric, a->root_snapshot->identity.system_identifier,
					   a->root_snapshot->identity.storage_uuid)
				   || !cluster_thread_recovery_fabric_cut_v1(batch->fabric, i, &cut))
			return false;
		if (cut.failed_thread == 0 || cut.failed_thread != a->duty->origin_thread_id
			|| (cluster_shared_config && cut.origin_owner_incarnation == 0)
			|| (cut.origin_owner_incarnation != 0
				&& cut.origin_owner_incarnation != a->duty->origin_owner_incarnation)
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
	batch->active_step = UINT32_MAX;
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
space_prepare_target(ClusterSpaceRecoveryBatchV1 *batch, SpaceRecoveryTarget *target,
					 uint32 through, uint32 *count, ClusterSpaceRecoveryImage *prepared)
{
	if (batch->cold_inputs != NULL) {
		bool ok = through == UINT32_MAX
					  ? cluster_space_recovery_prepare(
							batch->cold_inputs, batch->operation_count, &target->key,
							target->before[0].data, target->before[1].data, batch->order, prepared)
					  : cluster_space_recovery_prepare_through(
							batch->cold_inputs, batch->operation_count, through, &target->key,
							target->before[0].data, target->before[1].data, batch->order, prepared);

		if (!ok)
			return false;
		for (uint32 i = 0; i < batch->operation_count; i++)
			if (batch->order[i] != i)
				return false;
		*count = batch->operation_count;
		return true;
	}
	return (through == UINT32_MAX
				? rf_side_online_plan_prepare_space_v1(
					  batch->side, &target->key, target->before[0].data, target->before[1].data,
					  batch->order, batch->operation_count, count, prepared)
				: rf_side_online_plan_prepare_space_through_v1(
					  batch->side, &target->key, target->before[0].data, target->before[1].data,
					  through, batch->order, batch->operation_count, count, prepared))
		   == RF_PAGE_PROOF_DETAIL_OK;
}

/* ForkNumber bits are the cold planner ABI. Native SMGR flag values use a
 * different order for FSM and VM; never copy this mask into xlrec->flags. */
static int
space_cold_shrink_flags(uint8 forks)
{
	return ((forks & (1 << MAIN_FORKNUM)) ? SMGR_TRUNCATE_HEAP : 0)
		   | ((forks & (1 << FSM_FORKNUM)) ? SMGR_TRUNCATE_FSM : 0)
		   | ((forks & (1 << VISIBILITYMAP_FORKNUM)) ? SMGR_TRUNCATE_VM : 0);
}

static bool
space_cold_shrink(ClusterSpaceRecoveryBatchV1 *batch, uint32 through, bool apply)
{
	ClusterSpaceStructureChange change;
	xl_smgr_truncate truncate;
	bool ok;

	if (batch->cold_inputs == NULL || batch->cold_shrink_forks == 0
		|| through != batch->cold_shrink_step)
		return true;
	if (through >= batch->operation_count
		|| !cluster_space_structure_wal_decode(batch->cold_inputs[through].data,
											   batch->cold_inputs[through].length, &change)
		|| change.identity.action != CLUSTER_SPACE_WAL_TRUNCATE)
		return false;
	truncate = (xl_smgr_truncate){ change.identity.nblocks, batch->active->key.locator,
								   space_cold_shrink_flags(batch->cold_shrink_forks) };
	batch->active_step = through;
	ok = (apply ? smgr_redo_cold_truncate(&truncate, batch)
				: smgr_cold_truncate_preflight(&truncate, batch))
		 && space_sources_fresh(batch);
	batch->active_step = UINT32_MAX;
	return ok;
}

static bool
space_target_run(ClusterSpaceRecoveryBatchV1 *batch, uint32 index, bool apply, uint32 through)
{
	static const PGAlignedBlock zero;
	SpaceRecoveryTarget *target = &batch->targets[index];
	ClusterSpaceRecoveryImage prepared;
	ClusterResId resid;
	SMgrRelation rel;
	BlockNumber blocks;
	uint32 count;
	bool exists, ok = false;
	volatile uint8 mutated_mask = 0;
	volatile bool write_attempted[2] = { false, false };
	volatile uint32 before_state[2] = { 0, 0 };

	if (!space_sources_fresh(batch)
		|| cluster_smgr_which_for(target->key.locator, InvalidBackendId) != 1)
		return false;
	cluster_hw_resid_encode(target->key.locator, MAIN_FORKNUM, &resid);
	PG_TRY();
	{
		/* A committed cold plan holds the complete formed recovery isolation.
		 * Serving HW cannot be acquired during that startup phase. Online
		 * callers still acquire HW before taking any target buffer locks. */
		if ((!space_batch_cold(batch) && !cluster_hw_lock(&resid, &batch->hw))
			|| !space_sources_fresh(batch))
			goto done;
		batch->previous_owner = CurrentResourceOwner;
		batch->io_owner = ResourceOwnerCreate(CurrentResourceOwner, "SPACE recovery I/O");
		CurrentResourceOwner = batch->io_owner;
		batch->active = target;
		rel = smgropen(target->key.locator, InvalidBackendId);
		exists = smgrexists(rel, SPACE_FORKNUM);
		blocks = exists ? smgrnblocks(rel, SPACE_FORKNUM) : 0;
		if (blocks > 2 || (!target->has_create && (!exists || blocks != 2)))
			goto done;
		/* Missing CREATE components remain private zero predecessors until
		 * the whole retained chain has passed, without creating the fork. */
		memset(target->before, 0, sizeof(target->before));
		for (BlockNumber i = 0; i < blocks; i++) {
			batch->buffers[i] = ReadBufferWithoutRelcache(target->key.locator, SPACE_FORKNUM, i,
														  RBM_NORMAL, NULL, true);
			if (!BufferIsValid(batch->buffers[i]) || BufferIsLocal(batch->buffers[i]))
				goto done;
			LockBuffer(batch->buffers[i], i == 0 && !target->has_structure ? BUFFER_LOCK_SHARE
																		   : BUFFER_LOCK_EXCLUSIVE);
			if (BufferGetBlockNumber(batch->buffers[i]) != i)
				goto done;
			if ((i == 1 || target->has_structure)
				&& !cluster_bufmgr_pcm_x_content_holder_write_permitted(
					GetBufferDescriptor(batch->buffers[i] - 1)))
				goto done;
			memcpy(target->before[i].data, BufferGetPage(batch->buffers[i]), BLCKSZ);
		}
		if (!space_sources_fresh(batch))
			goto done;
		if (!space_prepare_target(batch, target, through, &count, &prepared)
			|| (!target->has_structure && prepared.source_index[0] != UINT32_MAX)
			|| (prepared.source_index[0] == UINT32_MAX && (prepared.apply_mask & 1)))
			goto done;
		target->operation_count = count;
		if (prepared.source_index[1] == UINT32_MAX) {
			if (!(prepared.covered_by_successor_mask & 2) || (prepared.apply_mask & 2))
				goto done;
		}
		for (int i = 0; i < 2; i++) {
			ClusterSpaceColdSourceV1 stamp;

			if (prepared.source_index[i] == UINT32_MAX)
				continue;
			if (batch->cold_inputs != NULL) {
				if (prepared.source_index[i] >= batch->operation_count)
					goto done;
				stamp = batch->cold_origins[prepared.source_index[i]];
			} else {
				RfSideOnlineOperationV1 source;

				if (!rf_side_online_plan_operation_v1(batch->side, prepared.source_index[i],
													  &source))
					goto done;
				stamp = (ClusterSpaceColdSourceV1){ source.identity.record.origin_thread,
													source.identity.record.end_rec_ptr };
			}
			PageSetLSNPreserveOrigin(prepared.pages[i].data, stamp.end_rec_ptr);
			if (!PageSetLSNOrigin(prepared.pages[i].data, stamp.origin_thread - 1))
				goto done;
		}
		/* A successor's bytes cannot borrow a failed origin's WAL owner.
		 * Require its canonical physical image before qualifying coverage. */
		for (int i = 0; i < 2; i++)
			if ((prepared.covered_by_successor_mask & (1 << i))
				&& (blocks <= i || !space_disk_matches(rel, i, target->before[i].data)))
				goto done;
		/* ADVANCE cannot supply missing identity-page bytes or their WAL. */
		if (prepared.source_index[0] == UINT32_MAX
			&& !space_disk_matches(rel, 0, prepared.pages[0].data))
			goto done;
		/* Re-prepare under this HW/current-X hold. A legitimate survivor
		 * extension since preflight may cover the old cut; never overwrite
		 * it with the previously prepared lower HWM. Rollback uses this
		 * apply's own original bytes, not the earlier preflight snapshot. */
		target->final = prepared;
		if (batch->cold_drop_already) {
			/* COMMIT may delete only its exact installed result, never a
			 * LIVE predecessor or a reused locator. This is a read-only
			 * proof: a cached result cannot be made durable on this path. */
			if (apply || prepared.apply_mask != 0 || prepared.covered_by_successor_mask != 0
				|| prepared.source_index[0] != 0 || prepared.source_index[1] != 0)
				goto done;
			for (int i = 0; i < 2; i++)
				if (memcmp(target->before[i].data, prepared.pages[i].data, BLCKSZ) != 0
					|| !space_disk_matches(rel, i, target->before[i].data))
					goto done;
			smgrimmedsync(rel, SPACE_FORKNUM);
			if (!space_sources_fresh(batch))
				goto done;
			for (int i = 0; i < 2; i++)
				if (!space_disk_matches(rel, i, target->before[i].data))
					goto done;
			ok = space_sources_fresh(batch);
			goto done;
		}
		/* A SPACE successor does not certify physical shrink. The committed
		 * planner names exactly the forks still needing it at this step;
		 * catch-up prefixes and unrequested forks never repeat ftruncate. */
		if (!space_cold_shrink(batch, through, apply))
			goto done;
		if (apply && prepared.covered_by_successor_mask != 0) {
			smgrimmedsync(rel, SPACE_FORKNUM);
			if (!space_sources_fresh(batch))
				goto done;
			for (int i = 0; i < 2; i++)
				if (((prepared.covered_by_successor_mask & (1 << i))
					 || prepared.source_index[i] == UINT32_MAX)
					&& !space_disk_matches(rel, i, target->before[i].data))
					goto done;
			if (prepared.source_index[0] == UINT32_MAX && prepared.source_index[1] == UINT32_MAX) {
				ok = true;
				goto done;
			}
		}
		if (!space_sources_fresh(batch))
			goto done;
		if (!apply) {
			ok = true;
			goto done;
		}
		if (!exists)
			smgrcreate(rel, SPACE_FORKNUM, true);
		for (BlockNumber i = blocks; i < 2; i++) {
			batch->buffers[i] = ReadBufferWithoutRelcache(target->key.locator, SPACE_FORKNUM, P_NEW,
														  RBM_ZERO_AND_LOCK, NULL, true);
			if (!BufferIsValid(batch->buffers[i]) || BufferIsLocal(batch->buffers[i])
				|| BufferGetBlockNumber(batch->buffers[i]) != i
				|| memcmp(BufferGetPage(batch->buffers[i]), zero.data, BLCKSZ) != 0)
				goto done;
		}
		if (!space_sources_fresh(batch))
			goto done;
		for (int i = 0; i < 2; i++)
			if (prepared.source_index[i] != UINT32_MAX
				&& !cluster_bufmgr_pcm_x_content_holder_write_permitted(
					GetBufferDescriptor(batch->buffers[i] - 1)))
				goto done;
		/* The strict MarkBufferDirty predicate was checked before entering
 * the critical section. Content-X prevents a revoke from changing it. */
		START_CRIT_SECTION();
		for (int i = 0; i < 2; i++) {
			if (prepared.source_index[i] == UINT32_MAX)
				continue;
			before_state[i]
				= pg_atomic_read_u32(&GetBufferDescriptor(batch->buffers[i] - 1)->state);
			memcpy(BufferGetPage(batch->buffers[i]), prepared.pages[i].data, BLCKSZ);
			MarkBufferDirty(batch->buffers[i]);
			mutated_mask |= 1 << i;
		}
		END_CRIT_SECTION();
		for (int i = 0; i < 2; i++)
			if (prepared.source_index[i] != UINT32_MAX
				&& !FlushOneBufferForSpaceRecovery(batch->buffers[i], batch, &write_attempted[i]))
				goto done;
		smgrimmedsync(rel, SPACE_FORKNUM);
		ok = space_sources_fresh(batch) && space_disk_matches(rel, 0, prepared.pages[0].data)
			 && space_disk_matches(rel, 1, prepared.pages[1].data);
	done:;
	}
	PG_FINALLY();
	{
		if (!write_attempted[0] && !write_attempted[1]) {
			/* Native I/O has already unwound. Once either component may have
			 * reached storage, keep both repeatable result components. */
			for (int i = 0; i < 2; i++) {
				BufferDesc *buf;
				const uint32 mask = BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR;
				uint32 state;

				if (!(mutated_mask & (1 << i)))
					continue;
				buf = GetBufferDescriptor(batch->buffers[i] - 1);
				Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE));
				memcpy(BufferGetPage(batch->buffers[i]), target->before[i].data, BLCKSZ);
				Assert(memcmp(BufferGetPage(batch->buffers[i]), target->before[i].data, BLCKSZ)
					   == 0);
				state = LockBufHdr(buf);
				Assert((state & BM_IO_IN_PROGRESS) == 0);
				/* A concurrent checkpoint can select an already dirty page
				 * despite content-X; restoring bytes must retain that duty. */
				if (before_state[i] & BM_DIRTY)
					before_state[i] |= state & BM_CHECKPOINT_NEEDED;
				state = (state & ~mask) | (before_state[i] & mask);
				UnlockBufHdr(buf, state);
			}
		}
		space_target_release(batch);
	}
	PG_END_TRY();
	return ok;
}

static bool
space_recovery_preflight(const ClusterThreadRecoveryFabricPlanV1 *plan,
						 const RfSideOnlinePlanV1 *side, ClusterRecoveryFencePlan *cold_fence,
						 const ClusterThreadRecoveryAuthorityV1 *sources, uint32 count,
						 ClusterSpaceRecoveryBatchV1 **out)
{
	ClusterSpaceRecoveryBatchV1 *batch;
	ClusterThreadRecoveryAuthorityV1 *cold_sources;
	uint32 targets, operations;
	Size bytes;
	size_t scratch;
	volatile bool ok = false;

	if (out == NULL)
		return false;
	*out = NULL;
	if (!cluster_enabled || !cluster_shared_config || count == 0
		|| count > RF_PAGE_STABLE_MAX_PARTICIPANTS
		|| (cold_fence == NULL
				? (plan == NULL || sources == NULL)
				: (plan != NULL || sources != NULL || !AmStartupProcess() || !RecoveryInProgress()
				   || !cluster_recovery_merge_fence_plan_revalidate_nowait(cold_fence)
				   || count != rf_side_online_plan_participant_count_v1(side))))
		return false;
	if (side == NULL)
		return false;
	targets = rf_side_online_plan_space_target_count_v1(side);
	operations = rf_side_online_plan_operation_count_v1(side);
	if (targets == UINT32_MAX || operations == UINT32_MAX
		|| targets > RF_SIDE_ONLINE_PLAN_MAX_BYTES / sizeof(SpaceRecoveryTarget)
		|| operations > RF_SIDE_ONLINE_PLAN_MAX_BYTES / sizeof(uint32))
		return false;
	bytes = sizeof(*batch) + (Size)targets * sizeof(SpaceRecoveryTarget)
			+ (Size)operations * sizeof(uint32) + 4 * BLCKSZ
			+ (cold_fence != NULL ? (Size)count * sizeof(*cold_sources) : 0);
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
		ClusterSpaceStructureChange structure;

		if (!rf_side_online_plan_operation_v1(side, i, &op))
			return false;
		if (op.history_only)
			continue;
		if (op.kind == RF_SIDE_ONLINE_OPERATION_XACT && op.xact.space_drop_count != 0)
			return false;
		if (op.kind == RF_SIDE_ONLINE_OPERATION_SPACE) {
			if (op.identity.record.rmid != RM_SMGR_ID)
				return false;
			if ((op.identity.record.info & ~XLR_INFO_MASK) == XLOG_SMGR_SPACE_RESERVATION) {
				if (!cluster_space_reservation_wal_decode(op.owned_payload, op.owned_payload_length,
														  &change)
					|| change.action != CLUSTER_SPACE_RESERVATION_ADVANCE)
					return false;
			} else if ((op.identity.record.info & ~XLR_INFO_MASK) != XLOG_SMGR_SPACE_IDENTITY
					   || !cluster_space_structure_wal_decode(op.owned_payload,
															  op.owned_payload_length, &structure)
					   || structure.identity.action != CLUSTER_SPACE_WAL_CREATE)
				return false;
		}
	}
	batch = space_alloc(bytes - 4 * BLCKSZ);
	if (batch == NULL)
		return false;
	batch->fabric = plan;
	batch->cold_fence = cold_fence;
	batch->side = side;
	batch->sources = sources;
	batch->source_owner = CurrentResourceOwner;
	batch->source_count = count;
	batch->target_count = targets;
	batch->operation_count = operations;
	batch->allocated_bytes = bytes - 4 * BLCKSZ;
	cold_sources = (ClusterThreadRecoveryAuthorityV1 *)(batch + 1);
	batch->targets = (SpaceRecoveryTarget *)(cold_fence != NULL
												 ? cold_sources + count
												 : (ClusterThreadRecoveryAuthorityV1 *)(batch + 1));
	batch->order = (uint32 *)(batch->targets + targets);
	PG_TRY();
	{
		if (cold_fence != NULL) {
			batch->sources = cold_sources;
			for (uint32 i = 0; i < count; i++) {
				ClusterControlRootSnapshot root;
				ClusterControlRootReadToken token;
				ClusterWalSourceRef source;
				XLogRecPtr native_redo;
				uint16 thread;

				if (!cluster_recovery_merge_fence_plan_origin(cold_fence, i, &thread, &root, &token)
					|| !cluster_recovery_merge_fence_plan_authority(cold_fence, thread,
																	&cold_sources[i]))
					goto done;
				/* Read the same token's native anchor before any target lock.
				 * Later I/O revalidates that original authority; a sealed plan
				 * may not substitute its physical lower for this redo start. */
				if (cluster_control_root_recovery_source_v1(cold_sources[i].root_snapshot,
															cold_sources[i].root_token, &source,
															&native_redo)
						!= CLUSTER_CONTROL_ROOT_OK_PRIMARY
					|| !rf_side_online_plan_replay_start_matches_v1(
						side, thread, cold_sources[i].duty->origin_owner_incarnation,
						source.claim.database_incarnation, native_redo))
					goto done;
			}
		}
		if (!space_sources_fresh(batch))
			goto done;
		for (uint32 i = 0; i < targets; i++) {
			SpaceRecoveryTarget *target = &batch->targets[i];
			if (!rf_side_online_plan_space_target_v1(side, i, &target->key))
				goto done;
			for (uint32 j = 0; j < operations; j++) {
				RfSideOnlineOperationV1 op;
				if (!rf_side_online_plan_operation_v1(side, j, &op))
					goto done;
				if (!op.history_only && op.kind == RF_SIDE_ONLINE_OPERATION_SPACE
					&& (op.identity.record.info & ~XLR_INFO_MASK) == XLOG_SMGR_SPACE_IDENTITY
					&& RelFileLocatorEquals(op.space_key.locator, target->key.locator))
					target->has_create = target->has_structure = true;
			}
			if (!space_target_run(batch, i, false, UINT32_MAX))
				goto done;
		}
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
cluster_space_recovery_preflight_v1(const ClusterThreadRecoveryFabricPlanV1 *plan,
									const ClusterThreadRecoveryAuthorityV1 *sources, uint32 count,
									ClusterSpaceRecoveryBatchV1 **out)
{
	return space_recovery_preflight(
		plan, plan != NULL ? cluster_thread_recovery_fabric_side_plan_v1(plan) : NULL, NULL,
		sources, count, out);
}

static bool
space_cold_relation_run(const ClusterSpaceIdentityKey *key, const ClusterSpaceRecoveryInput *inputs,
						const ClusterSpaceColdSourceV1 *origins, uint32 count, uint32 through,
						uint8 shrink_forks, bool drop_already)
{
	ClusterRecoverySerialGuard *guards[CLUSTER_WAL_RETENTION_MAX_THREADS];
	ClusterWalRetentionPin *pin = NULL;
	ClusterSpaceRecoveryBatchV1 *batch = NULL;
	ClusterThreadRecoveryAuthorityV1 *authorities;
	SpaceColdAuthority *owners;
	uint16 source_count = 0;
	Size bytes, scratch;
	bool ok = false;

	if (shrink_forks != 0) {
		ClusterSpaceStructureChange change;
		if (inputs == NULL || through >= count || drop_already
			|| (shrink_forks
				& ~((1 << MAIN_FORKNUM) | (1 << FSM_FORKNUM) | (1 << VISIBILITYMAP_FORKNUM)))
				   != 0
			|| !cluster_space_structure_wal_decode(inputs[through].data, inputs[through].length,
												   &change)
			|| change.identity.action != CLUSTER_SPACE_WAL_TRUNCATE)
			return false;
	}
	if (drop_already) {
		ClusterSpaceStructureChange drop;
		if (inputs == NULL || count != 1 || through != 0
			|| !cluster_space_structure_wal_decode(inputs[0].data, inputs[0].length, &drop)
			|| drop.identity.action != CLUSTER_SPACE_WAL_TOMBSTONE)
			return false;
	}
	if (!space_cold_phase() || key == NULL || inputs == NULL || origins == NULL || count == 0
		|| through >= count || (scratch = cluster_space_recovery_scratch_bytes(count)) == 0
		|| cluster_wal_retention_pin_borrow_cold_v1(&pin, guards, lengthof(guards), &source_count)
			   != CLUSTER_WAL_PIN_OK
		|| source_count == 0)
		return false;
	bytes = sizeof(*batch) + sizeof(SpaceRecoveryTarget)
			+ (Size)source_count * (sizeof(*authorities) + sizeof(*owners));
	if (scratch > MaxAllocSize - bytes - 4 * BLCKSZ
		|| count > (MaxAllocSize - bytes - scratch - 4 * BLCKSZ) / sizeof(uint32))
		return false;
	bytes += (Size)count * sizeof(uint32);
	batch = space_alloc(bytes);
	if (batch == NULL)
		return false;
	authorities = (ClusterThreadRecoveryAuthorityV1 *)(batch + 1);
	owners = (SpaceColdAuthority *)(authorities + source_count);
	batch->targets = (SpaceRecoveryTarget *)(owners + source_count);
	batch->order = (uint32 *)(batch->targets + 1);
	batch->sources = authorities;
	batch->cold_inputs = inputs;
	batch->cold_drop_already = drop_already;
	batch->cold_shrink_step = through;
	batch->cold_shrink_forks = shrink_forks;
	batch->cold_origins = origins;
	batch->source_owner = CurrentResourceOwner;
	batch->source_count = source_count;
	batch->target_count = 1;
	batch->operation_count = count;
	batch->allocated_bytes = bytes;
	batch->targets[0].key = *key;
	PG_TRY();
	{
		/* Read every exact original ROOT/native anchor before any target I/O.
		 * No current-writer lookup and no local flush of a foreign coordinate. */
		for (uint16 i = 0; i < source_count; i++) {
			ClusterRecoverySerialGuard *guard = guards[i];
			ClusterWalSourceRef source;
			ClusterControlRootResult result;

			result = cluster_control_root_read_canonical(guard->duty.origin_thread_id, &guard->duty,
														 CLUSTER_CONTROL_ROOT_READ_STRONG,
														 &owners[i].root, &owners[i].token);
			if ((result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
				 && result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
				|| memcmp(&owners[i].token, &guard->root_read_token, sizeof(owners[i].token)) != 0
				|| memcmp(&owners[i].root.identity, &guard->duty, sizeof(guard->duty)) != 0
				|| cluster_control_root_recovery_source_v1(&owners[i].root, &owners[i].token,
														   &source, &owners[i].native_redo)
					   != CLUSTER_CONTROL_ROOT_OK_PRIMARY
				|| memcmp(&source.claim.identity, &guard->duty, sizeof(guard->duty)) != 0
				|| source.claim.identity.system_identifier != key->system_identifier
				|| memcmp(source.claim.identity.storage_uuid, key->storage_uuid, 16) != 0
				|| source.claim.database_incarnation != key->database_incarnation
				|| owners[i].native_redo < owners[i].root.checkpoint_lower_lsn
				|| owners[i].native_redo > owners[i].root.validated_tail_lsn_exclusive)
				goto done;
			authorities[i]
				= (ClusterThreadRecoveryAuthorityV1){ .duty = &guard->duty,
													  .root_snapshot = &owners[i].root,
													  .root_token = &owners[i].token,
													  .formation = guard->formation,
													  .fence_need_set = guard->fence_need_set,
													  .fence_admission_set
													  = guard->fence_admission_set,
													  .retention_pin = pin,
													  .serial_guard = guard };
		}
		if (!space_sources_fresh(batch)
			|| !cluster_space_recovery_order(inputs, count, key, batch->order))
			goto done;
		for (uint32 i = 0; i < count; i++) {
			uint16 source;

			if (batch->order[i] != i)
				goto done;
			for (source = 0; source < source_count; source++)
				if (origins[i].origin_thread == authorities[source].duty->origin_thread_id)
					break;
			if (source == source_count || origins[i].end_rec_ptr <= owners[source].native_redo
				|| origins[i].end_rec_ptr > owners[source].root.validated_tail_lsn_exclusive)
				goto done;
			if (inputs[i].length == CLUSTER_SPACE_STRUCTURE_WAL_BYTES) {
				ClusterSpaceStructureChange structure;

				if (!cluster_space_structure_wal_decode(inputs[i].data, inputs[i].length,
														&structure))
					goto done;
				batch->targets[0].has_structure = true;
				batch->targets[0].has_create
					|= structure.identity.action == CLUSTER_SPACE_WAL_CREATE;
			}
		}
		if (!space_target_run(batch, 0, false, UINT32_MAX) || !space_sources_fresh(batch))
			goto done;
		if (drop_already) {
			ok = true;
			goto done;
		}
		batch->preflight_complete = true;
		/* Qualify the requested shrink before even an earlier CREATE can
		 * publish a component. Other steps/forks have no physical action. */
		if (shrink_forks != 0 && !space_target_run(batch, 0, false, through))
			goto done;
		/* Install structural SPACE results in order without replaying an
		 * intermediate shrink against DATA already covered by history. */
		for (uint32 i = 0; i < through; i++)
			if (inputs[i].length == CLUSTER_SPACE_STRUCTURE_WAL_BYTES
				&& !space_target_run(batch, 0, true, i))
				goto done;
		ok = space_target_run(batch, 0, true, through);
	done:;
	}
	PG_FINALLY();
	{
		cluster_space_recovery_destroy_v1(&batch);
	}
	PG_END_TRY();
	return ok;
}

bool
cluster_space_recovery_cold_relation_install_v1(const ClusterSpaceIdentityKey *key,
												const ClusterSpaceRecoveryInput *inputs,
												const ClusterSpaceColdSourceV1 *origins,
												uint32 count, uint32 through, uint8 shrink_forks)
{
	return space_cold_relation_run(key, inputs, origins, count, through, shrink_forks, false);
}

bool
cluster_space_recovery_cold_drop_already_v1(const ClusterSpaceIdentityKey *key,
											const ClusterSpaceRecoveryInput *input,
											const ClusterSpaceColdSourceV1 *source)
{
	return space_cold_relation_run(key, input, source, 1, 0, 0, true);
}

bool
cluster_space_recovery_cold_preflight_v1(const RfSideOnlinePlanV1 *side,
										 ClusterRecoveryFencePlan *fence,
										 ClusterSpaceRecoveryBatchV1 **out)
{
	return space_recovery_preflight(
		NULL, side, fence, NULL,
		fence != NULL ? cluster_recovery_merge_fence_plan_origin_count(fence) : 0, out);
}

bool
cluster_space_recovery_apply_through_v1(ClusterSpaceRecoveryBatchV1 *batch, uint32 target,
										uint32 through)
{
	if (batch == NULL || !batch->preflight_complete || batch->installed
		|| target >= batch->target_count || through >= batch->targets[target].operation_count
		|| !space_sources_fresh(batch))
		return false;
	return space_target_run(batch, target, true, through);
}

bool
cluster_space_recovery_apply_v1(ClusterSpaceRecoveryBatchV1 *batch)
{
	if (batch == NULL || !batch->preflight_complete || batch->installed
		|| !space_sources_fresh(batch))
		return false;
	for (uint32 i = 0; i < batch->target_count; i++)
		if (!space_target_run(batch, i, true, UINT32_MAX))
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

	if (batch == NULL || batch->active == NULL || !batch->preflight_complete
		|| (!space_batch_cold(batch)
			&& (!batch->hw.held || !batch->hw.coordinated
				|| batch->hw.req.lockmode != ExclusiveLock))
		|| !BufferIsValid(buffer) || BufferIsLocal(buffer) || !space_sources_fresh(batch))
		return false;
	BufferGetTag(buffer, &locator, &fork, &block);
	return RelFileLocatorEquals(locator, batch->active->key.locator) && fork == SPACE_FORKNUM
		   && block < 2 && buffer == batch->buffers[block]
		   && batch->active->final.source_index[block] != UINT32_MAX
		   && (block == CLUSTER_SPACE_RESERVATION_BLOCK || batch->active->has_structure)
		   && memcmp(BufferGetPage(buffer), batch->active->final.pages[block].data, BLCKSZ) == 0;
}

static bool
space_truncate_permitted(const ClusterSpaceRecoveryBatchV1 *batch, const xl_smgr_truncate *truncate,
						 bool readonly)
{
	static const PGAlignedBlock zero;
	ClusterSpaceStructureChange change;

	if (batch == NULL || truncate == NULL || batch->cold_inputs == NULL
		|| !batch->preflight_complete || batch->active == NULL
		|| batch->active_step >= batch->operation_count || !space_sources_fresh(batch)
		|| batch->cold_shrink_forks == 0 || batch->active_step != batch->cold_shrink_step
		|| !cluster_space_structure_wal_decode(batch->cold_inputs[batch->active_step].data,
											   batch->cold_inputs[batch->active_step].length,
											   &change)
		|| change.identity.action != CLUSTER_SPACE_WAL_TRUNCATE
		|| !RelFileLocatorEquals(truncate->rlocator, batch->active->key.locator)
		|| truncate->flags != space_cold_shrink_flags(batch->cold_shrink_forks)
		|| truncate->blkno != change.identity.nblocks)
		return false;
	for (int i = 0; i < 2; i++) {
		Buffer buffer = batch->buffers[i];
		RelFileLocator locator;
		ForkNumber fork;
		BlockNumber block;

		if (readonly && !BufferIsValid(buffer) && batch->active->has_create
			&& memcmp(batch->active->before[i].data, zero.data, BLCKSZ) == 0)
			continue;
		if (!BufferIsValid(buffer) || BufferIsLocal(buffer)
			|| !LWLockHeldByMeInMode(
				BufferDescriptorGetContentLock(GetBufferDescriptor(buffer - 1)), LW_EXCLUSIVE)
			|| memcmp(BufferGetPage(buffer), batch->active->before[i].data, BLCKSZ) != 0)
			return false;
		BufferGetTag(buffer, &locator, &fork, &block);
		if (!RelFileLocatorEquals(locator, truncate->rlocator) || fork != SPACE_FORKNUM
			|| block != i)
			return false;
	}
	return true;
}

bool
cluster_space_recovery_truncate_preflight_permitted_v1(const ClusterSpaceRecoveryBatchV1 *batch,
													   const xl_smgr_truncate *truncate)
{
	return space_truncate_permitted(batch, truncate, true);
}

bool
cluster_space_recovery_truncate_permitted_v1(const ClusterSpaceRecoveryBatchV1 *batch,
											 const xl_smgr_truncate *truncate)
{
	return space_truncate_permitted(batch, truncate, false);
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

	if (batch == NULL || !batch->preflight_complete || operation == NULL || operation->history_only
		|| operation->kind != RF_SIDE_ONLINE_OPERATION_SPACE || !space_sources_fresh(batch))
		return false;
	for (uint32 i = 0; i < batch->operation_count; i++) {
		RfSideOnlineOperationV1 expected;

		if (!rf_side_online_plan_operation_v1(batch->side, i, &expected))
			return false;
		if (memcmp(&operation->identity, &expected.identity, sizeof(expected.identity)) == 0)
			return !expected.history_only && expected.kind == RF_SIDE_ONLINE_OPERATION_SPACE
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
