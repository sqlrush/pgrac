/* Original empty publisher -> qualified VM reader -> complete statistics scan.
 * I/O, tuple status and snapshot services are controlled leaves.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
int statapprox_inherited_main(void);
#define VM_QUALIFIED_IOS_MAIN statapprox_inherited_main
#include "test_cluster_vm_qualified_ios.c"
#undef VM_QUALIFIED_IOS_MAIN

#include "access/heapam.h"

static bool stat_active, stat_heap_locked, stat_fsm_error;
static unsigned stat_heap_reads, stat_heap_unlocks, stat_fsm_reads, stat_checks;
static unsigned stat_cancel_at, stat_cut_after;
static BlockNumber stat_nblocks;
static HTSV_Result stat_tuple_status;

static void
stat_outside_vm(void)
{
	UT_ASSERT(!ios_locked && ios_evaluators == 0);
	if (cluster_shared_config && RelationIsPermanent(&relation) && !ios_missing)
		UT_ASSERT_EQ(ios_reads, ios_releases);
}

static void
stat_interrupt(void)
{
	stat_outside_vm();
	stat_checks++;
	if (stat_cancel_at && stat_checks == stat_cancel_at)
		elog(ERROR, "controlled statistics cancellation");
}

static Buffer
stat_heap_read(Relation rel, ForkNumber fork, BlockNumber block,
			   ReadBufferMode mode, BufferAccessStrategy strategy)
{
	stat_outside_vm();
	UT_ASSERT(rel == &relation && fork == MAIN_FORKNUM && block < stat_nblocks);
	UT_ASSERT(mode == RBM_NORMAL && strategy == (BufferAccessStrategy)1);
	UT_ASSERT(!stat_heap_locked);
	stat_heap_reads++;
	return 1;
}

static void
stat_heap_lock(Buffer buffer, int mode)
{
	stat_outside_vm();
	UT_ASSERT(buffer == 1 && mode == BUFFER_LOCK_SHARE && !stat_heap_locked);
	stat_heap_locked = true;
}

static void
stat_heap_unlock(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && stat_heap_locked);
	stat_heap_locked = false;
	stat_heap_unlocks++;
}

static Size
stat_fsm(Relation rel, BlockNumber block)
{
	stat_outside_vm();
	UT_ASSERT(rel == &relation && block < stat_nblocks && !stat_heap_locked);
	stat_fsm_reads++;
	if (stat_fsm_error)
		elog(ERROR, "controlled FSM failure after VM scope");
	if (stat_cut_after && stat_fsm_reads == stat_cut_after)
		ios_admission = false;
	return 128 * (block + 1);
}

static HTSV_Result
stat_tuple(HeapTuple tuple, TransactionId oldest, Buffer buffer)
{
	stat_outside_vm();
	UT_ASSERT(stat_heap_locked && buffer == 1 && oldest == 100);
	UT_ASSERT(tuple->t_len == SizeofHeapTupleHeader && tuple->t_tableOid == relation.rd_id);
	return stat_tuple_status;
}

static Size
stat_heap_free(Page page)
{
	UT_ASSERT(page == pages[0].data && stat_heap_locked);
	return 256;
}

#undef LockBuffer
#define LockBuffer stat_heap_lock
#undef ReleaseBuffer
#define ReleaseBuffer ios_release
#define UnlockReleaseBuffer stat_heap_unlock
#define ReadBufferExtended stat_heap_read
#define GetRecordedFreeSpace stat_fsm
#define GetOldestNonRemovableTransactionId(rel) ((void)(rel), (TransactionId)100)
#define GetAccessStrategy(kind) ((void)(kind), (BufferAccessStrategy)1)
#undef RelationGetNumberOfBlocks
#define RelationGetNumberOfBlocks(rel) ((void)(rel), stat_nblocks)
#define HeapTupleSatisfiesVacuum stat_tuple
#define PageGetHeapFreeSpace stat_heap_free
#define ActiveSnapshotSet() stat_active
#define GetActiveSnapshot() (&ios_snapshot)
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() stat_interrupt()
#ifdef FIXTURE_NO_ASSERTS
#undef USE_ASSERT_CHECKING
#undef Assert
#define Assert(condition) ((void)0)
#endif
#include "test_cluster_vm_statapprox.inc"

static void
stat_reset(void)
{
	ios_reset(true);
	stat_active = true;
	stat_heap_locked = stat_fsm_error = false;
	stat_heap_reads = stat_heap_unlocks = stat_fsm_reads = stat_checks = 0;
	stat_cancel_at = stat_cut_after = 0;
	stat_nblocks = 1;
	stat_tuple_status = HEAPTUPLE_LIVE;
	relation.rd_rel->reltuples = 5;
	relation.rd_rel->relpages = 1;
}

static output_type
stat_run(void)
{
	output_type out = {0};
	statapprox_heap(&relation, &out);
	UT_ASSERT(!ios_locked && !stat_heap_locked && ios_evaluators == 0);
	UT_ASSERT_EQ(ios_enters, ios_exits);
	UT_ASSERT_EQ(stat_heap_reads, stat_heap_unlocks);
	return out;
}

static void
stat_expect_heap(void)
{
	output_type out = stat_run();
	UT_ASSERT_EQ(stat_heap_reads, 1);
	UT_ASSERT_EQ(stat_fsm_reads, 0);
	UT_ASSERT_EQ(out.scanned_percent, 100);
	UT_ASSERT_EQ(out.free_space, 256);
	UT_ASSERT_EQ(out.tuple_count, 0);
}

UT_TEST(qualified_vm_skips_heap_but_retains_native_fsm_estimation)
{
	output_type out;
	stat_reset();
	out = stat_run();
	UT_ASSERT_EQ(stat_heap_reads, 0);
	UT_ASSERT_EQ(stat_fsm_reads, 1);
	UT_ASSERT_EQ(out.scanned_percent, 0);
	UT_ASSERT_EQ(out.table_len, 8192);
	UT_ASSERT_EQ(out.tuple_len, 8064);
	UT_ASSERT_EQ(out.free_space, 128);
	UT_ASSERT_EQ(out.tuple_count, 5); /* Native catalog estimate, not an exact count. */
	UT_ASSERT_EQ(out.dead_tuple_count + out.dead_tuple_len, 0);
	UT_ASSERT_EQ(ios_grants, 1);
	UT_ASSERT_EQ(ios_unlocks, 1);
	UT_ASSERT_EQ(ios_releases, 1);
}

UT_TEST(av_without_af_is_enough_for_statistics_only)
{
	output_type out;
	stat_reset();
	*PageGetContents(ios_current.data) = VISIBILITYMAP_ALL_VISIBLE;
	out = stat_run();
	UT_ASSERT_EQ(stat_heap_reads, 0);
	UT_ASSERT_EQ(stat_fsm_reads, 1);
	UT_ASSERT_EQ(out.tuple_count, 5);
}

UT_TEST(old_disk_positive_current_clear_uses_original_dead_tuple_count)
{
	output_type out;
	stat_reset();
	*PageGetContents(ios_current.data) = 0;
	empty_insert_row_at(1);
	stat_tuple_status = HEAPTUPLE_DEAD;
	out = stat_run();
	UT_ASSERT_EQ(stat_heap_reads, 1);
	UT_ASSERT_EQ(stat_fsm_reads, 0);
	UT_ASSERT_EQ(out.scanned_percent, 100);
	UT_ASSERT_EQ(out.tuple_count, 0);
	UT_ASSERT_EQ(out.dead_tuple_count, 1);
	UT_ASSERT_EQ(out.dead_tuple_len, SizeofHeapTupleHeader);
	UT_ASSERT_EQ(ios_grants, 1);
}

UT_TEST(old_unknown_catalog_or_absent_owned_snapshot_falls_back)
{
	for (int missing = 0; missing < 4; missing++) {
		stat_reset();
		switch (missing) {
		case 0: control.catalog_version_no = PGRAC_VM_QUALIFIED_CATALOG_VERSION - 1; break;
		case 1: control.catalog_version_no = 0; break;
		case 2: ControlFile = NULL; break;
		case 3: stat_active = false; break;
		}
		stat_expect_heap();
		UT_ASSERT_EQ(ios_grants + ios_reads, 0);
	}
}

UT_TEST(changed_space_membership_snapshot_cid_or_exact_target_cannot_skip)
{
	for (int drift = 1; drift <= 5; drift++) {
		stat_reset();
		ios_drift = drift;
		stat_expect_heap();
		UT_ASSERT_EQ(ios_grants, 1);
		UT_ASSERT_EQ(ios_unlocks, 1);
	}
}

UT_TEST(unavailable_or_unproven_vm_never_replaces_heap_inspection)
{
	for (int missing = 0; missing < 8; missing++) {
		stat_reset();
		switch (missing) {
		case 0: ios_pcm = false; break;
		case 1: ios_grant = false; break;
		case 2: ios_space = false; break;
		case 3: ios_admission = false; break;
		case 4: ios_snapshot_live = false; break;
		case 5: ios_missing = true; break;
		case 6: ((PageHeader)ios_current.data)->pd_block_scn = 0; break;
		case 7: *PageGetContents(ios_current.data) = VISIBILITYMAP_ALL_FROZEN; break;
		}
		stat_expect_heap();
	}
}

UT_TEST(non_mvcc_snapshots_cannot_borrow_a_positive_vm_decision)
{
	const SnapshotType kinds[] = {SNAPSHOT_ANY, SNAPSHOT_SELF, SNAPSHOT_NON_VACUUMABLE};
	for (unsigned i = 0; i < lengthof(kinds); i++) {
		stat_reset();
		ios_snapshot.snapshot_type = kinds[i];
		stat_expect_heap();
		UT_ASSERT_EQ(ios_grants, 0);
	}
}

UT_TEST(admission_loss_between_pages_does_not_reuse_the_first_decision)
{
	output_type out;
	stat_reset();
	stat_nblocks = 2;
	*PageGetContents(ios_current.data) |= VISIBILITYMAP_VALID_BITS << 2;
	stat_cut_after = 1;
	out = stat_run();
	UT_ASSERT_EQ(stat_fsm_reads, 1);
	UT_ASSERT_EQ(stat_heap_reads, 1);
	UT_ASSERT_EQ(out.scanned_percent, 50);
	UT_ASSERT_EQ(out.table_len, 16384);
	UT_ASSERT_EQ(out.tuple_len, 8064);
	UT_ASSERT_EQ(out.free_space, 384);
	UT_ASSERT_EQ(out.tuple_count, 5);
}

UT_TEST(same_byte_neighbour_clear_keeps_only_the_still_qualified_skip)
{
	output_type out;
	stat_reset();
	stat_nblocks = 2;
	*PageGetContents(pages[1].data) |= VISIBILITYMAP_VALID_BITS << 2;
	CritSectionCount = 1;
	UT_ASSERT(visibilitymap_clear_locked(&relation, 1, 2, VISIBILITYMAP_VALID_BITS));
	CritSectionCount = 0;
	ios_current = pages[1];
	out = stat_run();
	UT_ASSERT_EQ(stat_fsm_reads, 1);
	UT_ASSERT_EQ(stat_heap_reads, 1);
	UT_ASSERT_EQ(out.scanned_percent, 50);
	UT_ASSERT_EQ(ios_grants, 2);
}

UT_TEST(original_live_and_dead_classification_accounting_is_preserved)
{
	const HTSV_Result kinds[] = {HEAPTUPLE_LIVE, HEAPTUPLE_DELETE_IN_PROGRESS,
		HEAPTUPLE_DEAD, HEAPTUPLE_RECENTLY_DEAD, HEAPTUPLE_INSERT_IN_PROGRESS};
	for (unsigned i = 0; i < lengthof(kinds); i++) {
		output_type out;
		stat_reset();
		*PageGetContents(ios_current.data) = 0;
		empty_insert_row_at(1);
		stat_tuple_status = kinds[i];
		out = stat_run();
		UT_ASSERT_EQ(out.tuple_count, i < 2 ? 1 : 0);
		UT_ASSERT_EQ(out.dead_tuple_count, i < 2 ? 0 : 1);
		UT_ASSERT_EQ(out.tuple_len, i < 2 ? SizeofHeapTupleHeader : 0);
		UT_ASSERT_EQ(out.dead_tuple_len, i < 2 ? 0 : SizeofHeapTupleHeader);
		UT_ASSERT_EQ(stat_fsm_reads, 0);
	}
}

UT_TEST(empty_relation_has_no_vm_fsm_or_heap_reads)
{
	output_type out;
	stat_reset();
	stat_nblocks = 0;
	out = stat_run();
	UT_ASSERT_EQ(out.table_len + out.tuple_count + out.free_space, 0);
	UT_ASSERT_EQ(stat_heap_reads + stat_fsm_reads + ios_reads + ios_grants, 0);
}

UT_TEST(native_and_temporary_relation_raw_shortcuts_are_unchanged)
{
	for (int temporary = 0; temporary <= 1; temporary++) {
		output_type out;
		stat_reset();
		if (temporary)
			relation.rd_rel->relpersistence = RELPERSISTENCE_TEMP;
		else
			cluster_enabled = cluster_shared_config = false;
		stat_active = false;
		out = stat_run();
		UT_ASSERT_EQ(stat_fsm_reads, 1);
		UT_ASSERT_EQ(stat_heap_reads + ios_reads + ios_grants, 0);
		UT_ASSERT_EQ(out.tuple_count, 5);
	}
}

UT_TEST(read_error_fsm_error_and_cancellation_leave_no_vm_scope)
{
	for (int error = 0; error < 3; error++) {
		volatile bool caught = false;
		stat_reset();
		ios_error = error == 0 ? 1 : 0;
		stat_fsm_error = error == 1;
		if (error == 2) {
			stat_nblocks = 2;
			stat_cancel_at = 2;
		}
		expecting_error = true;
		PG_TRY();
		{
			(void)stat_run();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT(!ios_locked && !stat_heap_locked && ios_evaluators == 0);
		UT_ASSERT_EQ(ios_grants, ios_unlocks);
		UT_ASSERT_EQ(ios_reads, ios_releases);
		UT_ASSERT_EQ(stat_fsm_reads, error == 0 ? 0 : 1);
	}
}

UT_TEST(zero_threshold_cannot_be_enabled_by_a_large_loaded_catalog)
{
	stat_reset();
	control.catalog_version_no = PG_UINT32_MAX;
	stat_expect_heap();
	UT_ASSERT_EQ(ios_reads + ios_grants, 0);
}

int
main(void)
{
	if (PGRAC_VM_QUALIFIED_CATALOG_VERSION == 0) {
		UT_PLAN(3);
		UT_RUN(zero_threshold_cannot_be_enabled_by_a_large_loaded_catalog);
		UT_RUN(original_live_and_dead_classification_accounting_is_preserved);
		UT_RUN(empty_relation_has_no_vm_fsm_or_heap_reads);
	} else {
		UT_PLAN(13);
		UT_RUN(qualified_vm_skips_heap_but_retains_native_fsm_estimation);
		UT_RUN(av_without_af_is_enough_for_statistics_only);
		UT_RUN(old_disk_positive_current_clear_uses_original_dead_tuple_count);
		UT_RUN(old_unknown_catalog_or_absent_owned_snapshot_falls_back);
		UT_RUN(changed_space_membership_snapshot_cid_or_exact_target_cannot_skip);
		UT_RUN(unavailable_or_unproven_vm_never_replaces_heap_inspection);
		UT_RUN(non_mvcc_snapshots_cannot_borrow_a_positive_vm_decision);
		UT_RUN(admission_loss_between_pages_does_not_reuse_the_first_decision);
		UT_RUN(same_byte_neighbour_clear_keeps_only_the_still_qualified_skip);
		UT_RUN(original_live_and_dead_classification_accounting_is_preserved);
		UT_RUN(empty_relation_has_no_vm_fsm_or_heap_reads);
		UT_RUN(native_and_temporary_relation_raw_shortcuts_are_unchanged);
		UT_RUN(read_error_fsm_error_and_cancellation_leave_no_vm_scope);
	}
	UT_DONE();
	return ut_failed_count != 0;
}
