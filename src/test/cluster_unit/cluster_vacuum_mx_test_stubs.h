/*-------------------------------------------------------------------------
 *
 * cluster_vacuum_mx_test_stubs.h
 *    Storage and scheduling leaves for the actual VACUUM scan consumer.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/cluster_vacuum_mx_test_stubs.h
 *
 * NOTES
 *    The scan loop, relation binding and cutoff publication are extracted
 *    unchanged. I/O and native prune leaves expose calls and mutations; this
 *    does not simulate cross-node locking or replace the physical TAP case.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_VACUUM_MX_TEST_STUBS_H
#define CLUSTER_VACUUM_MX_TEST_STUBS_H

#define FAILSAFE_EVERY_PAGES 524288
#define VACUUM_FSM_EVERY_PAGES 1048576
#define FIXTURE_VM_BUFFER 1000
#define pgstat_progress_update_param(...) ((void)0)
#define pgstat_progress_update_multi_param(n, index, values)                                       \
	((void)(n), (void)(index), (void)(values))
#define vacuum_delay_point() ((void)0)
#define lazy_check_wraparound_failsafe(vacrel) ((void)0)
#define update_vacuum_error_info(...) ((void)0)
#define cluster_peer_mode_enabled() fixture_peer
#define cluster_smgr_which_for(locator, backend) fixture_smgr
#define cluster_space_relation_get_identity(rel, identity) fixture_identity
#undef BufferGetPage
#define BufferGetPage(buf) ((Page)fixture_pages[(buf) - 1].data)
#undef BufferIsValid
#define BufferIsValid(buf) ((buf) != InvalidBuffer)
#undef VM_ALL_FROZEN
#define VM_ALL_FROZEN(...) false

static ClusterHeapFreezeProofStatusV1
lazy_collect_shared_freeze_proofs(LVRelState *vacrel, Buffer recent_buffer)
{
	(void)recent_buffer;
	vacrel->shared_freeze_proofs.proof_count = 0;
	vacrel->shared_freeze_fence_valid = false;
	vacrel->shared_freeze_apply_status = CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN;
	return CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN;
}

static BlockNumber
lazy_scan_skip(LVRelState *vacrel, Buffer *vm, BlockNumber next, bool *allvis, bool *skip)
{
	(void)vacrel;
	(void)vm;
	*allvis = false;
	*skip = false;
	return next;
}

void
visibilitymap_pin(Relation rel, BlockNumber block, Buffer *vm)
{
	(void)rel;
	(void)block;
	if (!BufferIsValid(*vm)) {
		*vm = FIXTURE_VM_BUFFER;
		fixture_vm_pins++;
		fixture_vm_acquires++;
	}
}

Buffer
ReadBufferExtended(Relation rel, ForkNumber fork, BlockNumber block, ReadBufferMode mode,
				   BufferAccessStrategy strategy)
{
	(void)rel;
	(void)fork;
	(void)mode;
	(void)strategy;
	if (fixture_read_error)
		longjmp(fixture_error, 1);
	fixture_reads++;
	fixture_pins++;
	return block + 1;
}

bool
ConditionalLockBufferForCleanup(Buffer buf)
{
	(void)buf;
	if (fixture_cleanup)
		fixture_locks++;
	return fixture_cleanup;
}

void
LockBuffer(Buffer buf, int mode)
{
	(void)buf;
	fixture_locks += mode == BUFFER_LOCK_UNLOCK ? -1 : 1;
}

void
LockBufferForCleanup(Buffer buf)
{
	fixture_locks++;
	if (fixture_cleanup_empty)
		((PageHeader)BufferGetPage(buf))->pd_lower = SizeOfPageHeaderData;
	if (TransactionIdIsValid(fixture_cleanup_xmin)) {
		Page page = BufferGetPage(buf);
		HeapTupleHeader tuple = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1));

		HeapTupleHeaderSetXmin(tuple, fixture_cleanup_xmin);
	}
}

void
UnlockReleaseBuffer(Buffer buf)
{
	fixture_locks--;
	fixture_pins--;
	if (fixture_clear_released_page)
		memset(BufferGetPage(buf), 0, BLCKSZ);
}

void
ReleaseBuffer(Buffer buf)
{
	if (buf == FIXTURE_VM_BUFFER) {
		fixture_vm_pins--;
		fixture_vm_releases++;
	} else
		fixture_pins--;
}

static bool
lazy_scan_new_or_empty(LVRelState *vacrel, Buffer buf, BlockNumber block, Page page, bool share,
					   Buffer vm)
{
	(void)vacrel;
	(void)block;
	(void)share;
	(void)vm;
	if (PageIsEmpty(page)) {
		UnlockReleaseBuffer(buf);
		return true;
	}
	return false;
}

static bool
lazy_scan_noprune(LVRelState *vacrel, Buffer buf, BlockNumber block, Page page, bool *hastup,
				  bool *recordspace)
{
	(void)buf;
	(void)block;
	(void)page;
	fixture_noprune++;
	if (!fixture_finish_noprune)
		return false;
	if (fixture_noprune_hint)
		((HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1)))->t_infomask
			|= HEAP_XMIN_COMMITTED;
	vacrel->live_tuples++;
	*hastup = true;
	*recordspace = fixture_recordspace;
	return true;
}

static void
lazy_scan_prune(LVRelState *vacrel, Buffer buf, BlockNumber block, Page page,
				LVPagePruneState *state)
{
	(void)buf;
	(void)block;
	(void)page;
	fixture_prune++;
	if (fixture_prune_change == 1)
		ItemIdSetUnused(PageGetItemId(page, 1));
	else if (fixture_prune_change == 2)
		((PageHeader)page)->pd_upper = BLCKSZ;
	vacrel->live_tuples++;
	memset(state, 0, sizeof(*state));
	state->hastup = true;
	state->has_lpdead_items = fixture_prune_lpdead;
}

static void
lazy_vacuum(LVRelState *vacrel)
{
	fixture_consumed_dead += vacrel->dead_items->num_items;
	vacrel->dead_items->num_items = 0;
}

static void
lazy_vacuum_heap_page(LVRelState *vacrel, BlockNumber block, Buffer buf, int offset, Buffer vm)
{
	(void)block;
	(void)buf;
	(void)offset;
	(void)vm;
	fixture_consumed_dead += vacrel->dead_items->num_items;
	if (fixture_vacuum_change)
		ItemIdSetUnused(PageGetItemId(BufferGetPage(buf), 3));
}

static void
lazy_clear_visibility_versioned(Relation rel, const ClusterSpaceIdentity *identity, Buffer buf,
								BlockNumber block, Buffer vm)
{
	(void)rel;
	(void)identity;
	(void)buf;
	(void)block;
	(void)vm;
	fixture_vm_writes++;
}

#define MarkBufferDirty(buf) (fixture_dirties++)
#define RecordPageWithFreeSpace(rel, block, space) ((void)(space), fixture_fsm_writes++)
#define FreeSpaceMapVacuumRange(...) ((void)0)
#define visibilitymap_get_status(...) 0
#define visibilitymap_clear(...) (fixture_vm_writes++)
#define visibilitymap_set(rel, block, buf, lsn, vm, cutoff, flags, identity)                       \
	((void)(flags), fixture_vm_writes++)
#define PageGetHeapFreeSpace(page) 128
#define lazy_cleanup_all_indexes(vacrel) ((void)0)
#define vac_estimate_reltuples(rel, pages, scanned, tuples) ((double)(tuples))

#endif /* CLUSTER_VACUUM_MX_TEST_STUBS_H */

/* Existing scan locks are controlled leaves, never Resource-X writer grants. */
bool
ClusterBufferCaptureMutationContext(Buffer buffer, ResourceXWriterUseContext *out,
	uint64 *object_revision, bool *object_checked)
{
	(void) buffer;
	(void) out;
	(void) object_revision;
	(void) object_checked;
	UT_ASSERT(fixture_locks > 0);
	return false;
}
