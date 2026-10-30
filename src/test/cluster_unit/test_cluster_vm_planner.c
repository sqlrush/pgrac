/* Original publisher/read bodies and the complete planner endpoint consumer.
 * Index AM, datum services and heap visibility are controlled leaves.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
int planner_inherited_main(void);
#define VM_QUALIFIED_IOS_MAIN planner_inherited_main
#include "test_cluster_vm_qualified_ios.c"
#undef VM_QUALIFIED_IOS_MAIN

#include "utils/datum.h"

static RelationData planner_index;
static IndexScanDescData planner_scan;
static IndexTupleData planner_itup;
static TupleDescData planner_desc;
static TupleTableSlot planner_slot;
static ScanKeyData planner_key;
static Datum planner_slot_values[1];
static char planner_index_bytes[8], planner_saved_bytes[8];
static MemoryContext planner_context;
static ScanDirection planner_direction;
static unsigned planner_nexts, planner_fetches, planner_copies, planner_ends;
static unsigned planner_scoped_copies, planner_clears, planner_n_tids;
static bool planner_active, planner_heap_visible, planner_byval, planner_repeat_first;
static bool planner_copy_error;
static int planner_bad_shape;

static void
planner_outside_vm(void)
{
	UT_ASSERT(!ios_locked && ios_evaluators == 0);
	if (cluster_shared_config && RelationIsPermanent(&relation) && !ios_missing)
		UT_ASSERT_EQ(ios_reads, ios_releases);
}

static IndexScanDesc
planner_begin(Relation heap, Relation index, Snapshot snapshot, int keys, int orders)
{
	planner_outside_vm();
	UT_ASSERT(heap == &relation && index == &planner_index && keys == 1 && orders == 0);
	UT_ASSERT(snapshot->snapshot_type == SNAPSHOT_NON_VACUUMABLE);
	planner_scan.heapRelation = heap;
	planner_scan.indexRelation = index;
	planner_scan.xs_snapshot = snapshot;
	planner_scan.xs_itupdesc = &planner_desc;
	planner_scan.xs_itup = planner_bad_shape == 1 ? NULL : &planner_itup;
	planner_scan.xs_recheck = planner_bad_shape == 2;
	return &planner_scan;
}

static void
planner_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey order, int norders)
{
	planner_outside_vm();
	UT_ASSERT(scan == &planner_scan && scan->xs_want_itup);
	UT_ASSERT(keys == &planner_key && nkeys == 1 && order == NULL && norders == 0);
}

static ItemPointer
planner_next(IndexScanDesc scan, ScanDirection direction)
{
	unsigned n = planner_nexts++;
	BlockNumber block = planner_repeat_first && n > 0 ? n - 1 : n;

	planner_outside_vm();
	UT_ASSERT(scan == &planner_scan && direction == planner_direction);
	UT_ASSERT(scan->xs_snapshot->snapshot_type == SNAPSHOT_NON_VACUUMABLE);
	if (n >= planner_n_tids)
		return NULL;
	ItemPointerSet(&ios_tid, block, 1);
	return &ios_tid;
}

static bool
planner_fetch(IndexScanDesc scan, TupleTableSlot *slot)
{
	planner_outside_vm();
	UT_ASSERT(scan == &planner_scan && slot == &planner_slot);
	UT_ASSERT(scan->xs_snapshot->snapshot_type == SNAPSHOT_NON_VACUUMABLE);
	planner_fetches++;
	slot->tts_values[0] = (Datum)99; /* Deliberately differs from the index value. */
	return planner_heap_visible;
}

static TupleTableSlot *
planner_clear(TupleTableSlot *slot)
{
	planner_outside_vm();
	UT_ASSERT(slot == &planner_slot && planner_fetches > 0);
	planner_clears++;
	return slot;
}

static void
planner_deform(IndexTuple tuple, TupleDesc desc, Datum *values, bool *isnull)
{
	UT_ASSERT(tuple == &planner_itup && desc == &planner_desc && ios_evaluators == 0);
	UT_ASSERT(!cluster_shared_config || ios_locked || planner_fetches > 0 ||
		!RelationIsPermanent(&relation));
	values[0] = planner_byval ? (Datum)17 : PointerGetDatum(planner_index_bytes);
	isnull[0] = planner_bad_shape == 3;
}

static MemoryContext
planner_switch(MemoryContext next)
{
	MemoryContext old = planner_context;
	UT_ASSERT(next == (MemoryContext)1 || next == (MemoryContext)2);
	planner_context = next;
	return old;
}

static Datum
planner_copy(Datum value, bool byval, int len)
{
	UT_ASSERT(planner_context == (MemoryContext)2 && byval == planner_byval && len == 8);
	UT_ASSERT(ios_evaluators == 0);
	planner_copies++;
	if (ios_locked)
		planner_scoped_copies++;
	if (planner_copy_error)
		elog(ERROR, "controlled endpoint allocation failure");
	if (byval)
		return value;
	memcpy(planner_saved_bytes, DatumGetPointer(value), 8);
	return PointerGetDatum(planner_saved_bytes);
}

static void
planner_end(IndexScanDesc scan)
{
	planner_outside_vm();
	UT_ASSERT(scan == &planner_scan && planner_context == (MemoryContext)1);
	planner_ends++;
	memset(planner_index_bytes, '!', 8);
	scan->xs_snapshot = NULL;
}

#undef index_beginscan
#define index_beginscan planner_begin
#undef index_rescan
#define index_rescan planner_rescan
#undef index_getnext_tid
#define index_getnext_tid planner_next
#undef index_fetch_heap
#define index_fetch_heap planner_fetch
#undef ExecClearTuple
#define ExecClearTuple planner_clear
#define index_endscan planner_end
#define index_deform_tuple planner_deform
#define datumCopy planner_copy
#define MemoryContextSwitchTo planner_switch
#define GlobalVisTestFor(rel) ((void)(rel), (GlobalVisState *)1)
#define ActiveSnapshotSet() planner_active
#define GetActiveSnapshot() (&ios_snapshot)
#ifdef FIXTURE_NO_ASSERTS
#undef USE_ASSERT_CHECKING
#undef Assert
#define Assert(condition) ((void)0)
#endif
#include "test_cluster_vm_planner.inc"

static void
planner_reset(void)
{
	ios_reset(true);
	memset(&planner_index, 0, sizeof(planner_index));
	memset(&planner_scan, 0, sizeof(planner_scan));
	memset(&planner_slot, 0, sizeof(planner_slot));
	planner_slot.tts_values = planner_slot_values;
	planner_index.rd_rel = relation.rd_rel;
	memcpy(planner_index_bytes, "index-V", 8);
	memset(planner_saved_bytes, 0, sizeof(planner_saved_bytes));
	planner_context = (MemoryContext)1;
	planner_direction = ForwardScanDirection;
	planner_nexts = planner_fetches = planner_copies = planner_ends = 0;
	planner_scoped_copies = planner_clears = 0;
	planner_n_tids = 1;
	planner_active = planner_heap_visible = planner_byval = true;
	planner_repeat_first = planner_copy_error = false;
	planner_bad_shape = 0;
}

static bool
planner_run(Datum *out)
{
	bool found = get_actual_variable_endpoint(&relation, &planner_index, planner_direction,
		&planner_key, 8, planner_byval, &planner_slot, (MemoryContext)2, out);

	planner_outside_vm();
	UT_ASSERT_EQ(ios_enters, ios_exits);
	UT_ASSERT_EQ(planner_ends, 1);
	return found;
}

static void
planner_expect_fallback(void)
{
	Datum value = 88;
	UT_ASSERT(planner_run(&value));
	UT_ASSERT_EQ(value, 17);
	UT_ASSERT_EQ(planner_fetches, 1);
	UT_ASSERT_EQ(planner_clears, 1);
	UT_ASSERT_EQ(planner_scoped_copies, 0);
}

UT_TEST(qualified_av_and_af_use_index_data_in_both_directions_without_heap)
{
	for (int frozen = 0; frozen <= 1; frozen++) {
		for (int backward = 0; backward <= 1; backward++) {
			Datum value = 88;
			planner_reset();
			*PageGetContents(ios_current.data) = frozen ? VISIBILITYMAP_VALID_BITS : VISIBILITYMAP_ALL_VISIBLE;
			planner_direction = backward ? BackwardScanDirection : ForwardScanDirection;
			UT_ASSERT(planner_run(&value));
			UT_ASSERT_EQ(value, 17);
			UT_ASSERT_EQ(planner_fetches, 0);
			UT_ASSERT_EQ(planner_scoped_copies, 1);
			UT_ASSERT_EQ(planner_nexts, 1);
			UT_ASSERT_EQ(ios_grants, 1);
			UT_ASSERT_EQ(ios_unlocks, 1);
			UT_ASSERT_EQ(ios_releases, 1);
		}
	}
}

UT_TEST(by_reference_endpoint_survives_index_scan_release)
{
	Datum value = 88;
	planner_reset();
	planner_byval = false;
	UT_ASSERT(planner_run(&value));
	UT_ASSERT_EQ(planner_fetches, 0);
	UT_ASSERT_EQ(planner_scoped_copies, 1);
	UT_ASSERT(DatumGetPointer(value) != planner_index_bytes);
	UT_ASSERT(memcmp(DatumGetPointer(value), "index-V", 8) == 0);
}

UT_TEST(old_disk_positive_current_clear_preserves_precise_fallback)
{
	for (int visible = 0; visible <= 1; visible++) {
		Datum value = 88;
		planner_reset();
		*PageGetContents(ios_current.data) = 0;
		planner_heap_visible = visible;
		UT_ASSERT_EQ(planner_run(&value), visible);
		UT_ASSERT_EQ(value, visible ? 17 : 88);
		UT_ASSERT_EQ(planner_fetches, 1);
		UT_ASSERT_EQ(planner_scoped_copies, 0);
		UT_ASSERT_EQ(ios_grants, 1);
	}
}

UT_TEST(old_unknown_catalog_or_absent_active_snapshot_uses_original_heap_snapshot)
{
	for (int missing = 0; missing < 4; missing++) {
		planner_reset();
		switch (missing) {
		case 0: control.catalog_version_no = PGRAC_VM_QUALIFIED_CATALOG_VERSION - 1; break;
		case 1: control.catalog_version_no = 0; break;
		case 2: ControlFile = NULL; break;
		case 3: planner_active = false; break;
		}
		planner_expect_fallback();
		UT_ASSERT_EQ(ios_grants + ios_reads, 0);
	}
}

UT_TEST(space_admission_snapshot_cid_or_target_drift_cannot_skip_heap)
{
	for (int drift = 1; drift <= 5; drift++) {
		planner_reset();
		ios_drift = drift;
		planner_expect_fallback();
		UT_ASSERT_EQ(ios_grants, 1);
		UT_ASSERT_EQ(ios_unlocks, 1);
	}
}

UT_TEST(unavailable_or_unproven_vm_preserves_heap_fallback)
{
	for (int missing = 0; missing < 8; missing++) {
		planner_reset();
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
		planner_expect_fallback();
	}
}

UT_TEST(non_mvcc_active_snapshot_cannot_borrow_vm_qualification)
{
	const SnapshotType kinds[] = {SNAPSHOT_ANY, SNAPSHOT_SELF, SNAPSHOT_NON_VACUUMABLE};
	for (unsigned i = 0; i < lengthof(kinds); i++) {
		planner_reset();
		ios_snapshot.snapshot_type = kinds[i];
		planner_expect_fallback();
		UT_ASSERT_EQ(ios_grants, 0);
	}
}

UT_TEST(same_byte_neighbour_clear_keeps_the_original_qualified_bit)
{
	Datum value = 88;
	planner_reset();
	*PageGetContents(pages[1].data) |= VISIBILITYMAP_VALID_BITS << 2;
	CritSectionCount = 1;
	UT_ASSERT(visibilitymap_clear_locked(&relation, 1, 2, VISIBILITYMAP_VALID_BITS));
	CritSectionCount = 0;
	ios_current = pages[1];
	UT_ASSERT(planner_run(&value));
	UT_ASSERT_EQ(value, 17);
	UT_ASSERT_EQ(planner_fetches, 0);
	UT_ASSERT_EQ(planner_scoped_copies, 1);
}

UT_TEST(rejected_tid_then_qualified_tid_gets_a_fresh_vm_scope)
{
	Datum value = 88;
	planner_reset();
	*PageGetContents(ios_current.data) = VISIBILITYMAP_VALID_BITS << 2;
	planner_n_tids = 2;
	planner_heap_visible = false;
	UT_ASSERT(planner_run(&value));
	UT_ASSERT_EQ(value, 17);
	UT_ASSERT_EQ(planner_nexts, 2);
	UT_ASSERT_EQ(planner_fetches, 1);
	UT_ASSERT_EQ(planner_scoped_copies, 1);
	UT_ASSERT_EQ(ios_grants, 2);
}

UT_TEST(failed_heap_page_limit_deduplicates_repeated_tids)
{
	Datum value = 88;
	planner_reset();
	*PageGetContents(ios_current.data) = 0;
	planner_n_tids = 103;
	planner_repeat_first = true;
	planner_heap_visible = false;
	UT_ASSERT(!planner_run(&value));
	UT_ASSERT_EQ(value, 88);
	/* Two TIDs on page 0, then pages 1..100: 101 distinct failed pages. */
	UT_ASSERT_EQ(planner_nexts, 102);
	UT_ASSERT_EQ(planner_fetches, 102);
	UT_ASSERT_EQ(planner_copies, 0);
}

UT_TEST(native_and_temporary_raw_vm_shortcuts_are_unchanged)
{
	for (int temporary = 0; temporary <= 1; temporary++) {
		Datum value = 88;
		planner_reset();
		if (temporary)
			relation.rd_rel->relpersistence = RELPERSISTENCE_TEMP;
		else
			cluster_enabled = cluster_shared_config = false;
		planner_active = false;
		UT_ASSERT(planner_run(&value));
		UT_ASSERT_EQ(value, 17);
		UT_ASSERT_EQ(planner_fetches + ios_reads + ios_grants, 0);
		UT_ASSERT_EQ(planner_scoped_copies, 0);
	}
}

UT_TEST(empty_index_preserves_output_without_acquiring_vm)
{
	Datum value = 88;
	planner_reset();
	planner_n_tids = 0;
	UT_ASSERT(!planner_run(&value));
	UT_ASSERT_EQ(value, 88);
	UT_ASSERT_EQ(planner_fetches + ios_reads + ios_grants + planner_copies, 0);
}

UT_TEST(read_and_index_data_errors_release_vm_scope_and_pin)
{
	for (int error = 0; error < 5; error++) {
		Datum value = 88;
		volatile bool caught = false;
		planner_reset();
		ios_error = error == 0 ? 1 : 0;
		planner_bad_shape = error >= 1 && error <= 3 ? error : 0;
		planner_copy_error = error == 4;
		expecting_error = true;
		PG_TRY();
		{
			(void)planner_run(&value);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT(!ios_locked && ios_evaluators == 0);
		UT_ASSERT_EQ(ios_enters, ios_exits);
		UT_ASSERT_EQ(ios_grants, 1);
		UT_ASSERT_EQ(ios_unlocks, 1);
		UT_ASSERT_EQ(ios_reads, ios_releases);
	}
}

UT_TEST(zero_threshold_does_not_admit_a_maximum_loaded_catalog)
{
	planner_reset();
	control.catalog_version_no = PG_UINT32_MAX;
	planner_expect_fallback();
	UT_ASSERT_EQ(ios_reads + ios_grants, 0);
}

int
main(void)
{
	if (PGRAC_VM_QUALIFIED_CATALOG_VERSION == 0) {
		UT_PLAN(3);
		UT_RUN(zero_threshold_does_not_admit_a_maximum_loaded_catalog);
		UT_RUN(failed_heap_page_limit_deduplicates_repeated_tids);
		UT_RUN(empty_index_preserves_output_without_acquiring_vm);
	} else {
		UT_PLAN(13);
		UT_RUN(qualified_av_and_af_use_index_data_in_both_directions_without_heap);
		UT_RUN(by_reference_endpoint_survives_index_scan_release);
		UT_RUN(old_disk_positive_current_clear_preserves_precise_fallback);
		UT_RUN(old_unknown_catalog_or_absent_active_snapshot_uses_original_heap_snapshot);
		UT_RUN(space_admission_snapshot_cid_or_target_drift_cannot_skip_heap);
		UT_RUN(unavailable_or_unproven_vm_preserves_heap_fallback);
		UT_RUN(non_mvcc_active_snapshot_cannot_borrow_vm_qualification);
		UT_RUN(same_byte_neighbour_clear_keeps_the_original_qualified_bit);
		UT_RUN(rejected_tid_then_qualified_tid_gets_a_fresh_vm_scope);
		UT_RUN(failed_heap_page_limit_deduplicates_repeated_tids);
		UT_RUN(native_and_temporary_raw_vm_shortcuts_are_unchanged);
		UT_RUN(empty_index_preserves_output_without_acquiring_vm);
		UT_RUN(read_and_index_data_errors_release_vm_scope_and_pin);
	}
	UT_DONE();
	return ut_failed_count != 0;
}
