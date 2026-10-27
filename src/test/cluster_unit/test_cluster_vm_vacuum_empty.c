/*-------------------------------------------------------------------------
 *
 * test_cluster_vm_vacuum_empty.c
 *    Native LP_DEAD cleanup followed by qualified empty-page publication.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_vm_vacuum_empty.c
 *
 * NOTES
 *    Original VACUUM, page helpers and visible publisher; scheduling and WAL
 *    I/O are fixture leaves. No physical durability or granting-read claim.
 *
 *-------------------------------------------------------------------------
 */
int vacuum_inherited_main(void);
#define main vacuum_inherited_main
#include "test_cluster_vm_visible_version.c"
#undef main

#include "commands/progress.h"

typedef int LVSavedErrInfo;
enum { VACUUM_ERRCB_PHASE_VACUUM_HEAP = 3 };

static unsigned vacuum_records, vacuum_updates, vacuum_restores, vacuum_classifications;
static RfPageVersionEdgeEntryV1 vacuum_edge;
static uint64 vacuum_token;
static xl_heap_vacuum vacuum_payload;
static OffsetNumber vacuum_offsets[MaxHeapTuplesPerPage];
static uint16 vacuum_offset_count;
static uint8 vacuum_buffer_flags;
static PGAlignedBlock vacuum_image;

void
pgstat_progress_update_param(int index, int64 value)
{
	UT_ASSERT_EQ(index, PROGRESS_VACUUM_HEAP_BLKS_VACUUMED);
	UT_ASSERT_EQ(value, 0);
	UT_ASSERT(held[0] && pinned[0]);
}

static void
update_vacuum_error_info(LVRelState *state, LVSavedErrInfo *saved, int phase, BlockNumber block,
						 OffsetNumber offset)
{
	UT_ASSERT(state->rel == &relation && saved != NULL);
	UT_ASSERT_EQ(phase, VACUUM_ERRCB_PHASE_VACUUM_HEAP);
	UT_ASSERT_EQ(block, 0);
	UT_ASSERT_EQ(offset, InvalidOffsetNumber);
	*saved = 19;
	vacuum_updates++;
}

static void
restore_vacuum_error_info(LVRelState *state, const LVSavedErrInfo *saved)
{
	UT_ASSERT(state->rel == &relation && saved != NULL && *saved == 19);
	UT_ASSERT(held[0] && pinned[0] && !held[1]);
	UT_ASSERT_EQ(CritSectionCount, 0);
	vacuum_restores++;
}

/* Nonshared native classification is outside this empty shared-page slice. */
static bool
heap_page_is_all_visible(LVRelState *state, Buffer buffer, TransactionId *cutoff, bool *frozen)
{
	UT_ASSERT(!state->shared_maintenance_restricted && buffer == 1);
	UT_ASSERT(cutoff != NULL && frozen != NULL && held[0]);
	vacuum_classifications++;
	return false;
}

static void
vacuum_register_data(char *data, uint32 length)
{
	UT_ASSERT(begun && held[0] && writer[0]);
	UT_ASSERT_EQ(length, SizeOfHeapVacuum);
	memset(&vacuum_payload, 0, sizeof(vacuum_payload));
	memcpy(&vacuum_payload, data, length);
}

static void
vacuum_register_buffer(uint8 id, Buffer buffer, uint8 flags)
{
	UT_ASSERT(begun && held[0] && writer[0]);
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT_EQ(buffer, 1);
	vacuum_buffer_flags = flags;
	vacuum_image = pages[0];
}

static bool
vacuum_register_versions(const RfPageProducerBatchV1 *batch)
{
	bool result;

	/* Reuse the original registration leaf's one-component capture mode. */
	repairing_vm_only = true;
	result = rf_page_producer_register_wal_v1(batch);
	repairing_vm_only = false;
	UT_ASSERT_EQ(edge_count, 1);
	vacuum_edge = edges[0];
	vacuum_token = edge_token;
	return result;
}

static void
vacuum_register_buffer_data(uint8 id, char *data, uint32 length)
{
	UT_ASSERT(begun);
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT(length > 0 && length <= sizeof(vacuum_offsets));
	UT_ASSERT_EQ(length % sizeof(OffsetNumber), 0);
	vacuum_offset_count = length / sizeof(OffsetNumber);
	memcpy(vacuum_offsets, data, length);
}

static XLogRecPtr
vacuum_insert(RmgrId rmgr, uint8 info)
{
	const XLogRecPtr lsn = UINT64_C(0x8000);

	UT_ASSERT(begun && CritSectionCount > 0 && held[0] && !held[1]);
	UT_ASSERT(dirty[0] > 0 && dirty[1] == 0);
	UT_ASSERT_EQ(rmgr, RM_HEAP2_ID);
	UT_ASSERT_EQ(info, XLOG_HEAP2_VACUUM);
	UT_ASSERT_EQ(vacuum_payload.nunused, vacuum_offset_count);
	UT_ASSERT_EQ(vacuum_buffer_flags, REGBUF_STANDARD);
	begun = false;
	vacuum_records++;
	/* The native caller stamps this exact LSN before the empty helper runs.
	 * The next native version allocator must see that full post-vacuum image. */
	memcpy(before, pages, sizeof(pages));
	PageSetLSN(before[0].data, lsn);
	return lsn;
}

#define XLogRegisterData vacuum_register_data
#define XLogRegisterBuffer vacuum_register_buffer
#define XLogRegisterBufData vacuum_register_buffer_data
#define XLogInsert vacuum_insert
#define rf_page_producer_register_wal_v1 vacuum_register_versions
#include "test_cluster_vm_vacuum_empty.inc"
#undef rf_page_producer_register_wal_v1
#undef XLogInsert
#undef XLogRegisterBufData
#undef XLogRegisterBuffer
#undef XLogRegisterData

static int
vacuum_page(LVRelState *state, Buffer *vm, bool cleanup)
{
#ifdef FIXTURE_VACUUM_VM_POINTER
	return lazy_vacuum_heap_page(state, 0, 1, 0, vm, cleanup);
#else
	(void)cleanup;
	return lazy_vacuum_heap_page(state, 0, 1, 0, *vm);
#endif
}

static void
vacuum_reset(LVRelState *state, uint16 count, bool indexed)
{
	static union {
		max_align_t align;
		char bytes[offsetof(VacDeadItems, items)
				   + (MaxHeapTuplesPerPage + 1) * sizeof(ItemPointerData)];
	} storage;
	PageHeader header;

	*state = empty_reset(false, VISIBILITYMAP_VALID_BITS << 2);
	state->nindexes = indexed ? 1 : 0;
	state->do_index_vacuuming = indexed;
	state->dead_items = (VacDeadItems *)storage.bytes;
	state->dead_items->max_items = MaxHeapTuplesPerPage + 1;
	state->dead_items->num_items = count + 1;
	for (uint16 i = 0; i < count; i++)
		ItemPointerSet(&state->dead_items->items[i], 0, i + 1);
	/* A following block must remain outside this cleanup record. */
	ItemPointerSet(&state->dead_items->items[count], 1, 1);
	header = (PageHeader)pages[0].data;
	header->pd_lower = SizeOfPageHeaderData + count * sizeof(ItemIdData);
	for (uint16 i = 0; i < count; i++)
		ItemIdSetDead(PageGetItemId(pages[0].data, i + 1));
	writer[0] = true;
	vacuum_records = vacuum_updates = vacuum_restores = vacuum_classifications = 0;
	vacuum_offset_count = vacuum_buffer_flags = 0;
	vacuum_token = 0;
	memset(&vacuum_edge, 0, sizeof(vacuum_edge));
	memcpy(before, pages, sizeof(pages));
}

static void
vacuum_release(void)
{
	UT_ASSERT(held[0] && pinned[0] && pinned[1] && !held[1]);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UnlockReleaseBuffer(1);
	ReleaseBuffer(2);
}

UT_TEST(test_last_dead_cleanup_publishes_both_versions_in_each_lock_mode)
{
	const uint16 counts[] = { 1, 8, MaxHeapTuplesPerPage };

	for (unsigned mode = 0; mode < 2; mode++)
		for (unsigned n = 0; n < lengthof(counts); n++) {
			LVRelState state;
			Buffer vm = 2;

			vacuum_reset(&state, counts[n], mode == 0);
			UT_ASSERT_EQ(vacuum_page(&state, &vm, mode != 0), counts[n]);
			UT_ASSERT_EQ(vacuum_records, 1);
			UT_ASSERT_EQ(vacuum_edge.block_id, 0);
			UT_ASSERT_EQ(vacuum_edge.before.mutation_token, 20);
			UT_ASSERT_EQ(vacuum_token, 101);
			UT_ASSERT_EQ(((PageHeader)vacuum_image.data)->pd_block_scn, 101);
			UT_ASSERT_EQ(vacuum_offset_count, counts[n]);
			UT_ASSERT_EQ(vacuum_offsets[counts[n] - 1], counts[n]);
			UT_ASSERT_EQ(inserts, 1);
			UT_ASSERT_EQ(edge_count, 2);
			UT_ASSERT_EQ(edges[0].before.mutation_token, 30);
			UT_ASSERT_EQ(edges[1].before.mutation_token, 101);
			UT_ASSERT_EQ(next_token, 102);
			UT_ASSERT_EQ(PageGetLSN(pages[0].data), UINT64_C(0x9000));
			UT_ASSERT_EQ(PageGetLSN(pages[1].data), UINT64_C(0x9000));
			UT_ASSERT(PageIsAllVisible(pages[0].data));
			UT_ASSERT(visibilitymap_heap_page_is_empty(pages[0].data));
			UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 15);
			UT_ASSERT_EQ(empty_aux_calls, 1);
			UT_ASSERT_EQ(empty_cleanup_waits, mode != 0);
			UT_ASSERT_EQ(empty_heap_waits, 1);
			UT_ASSERT_EQ(vacuum_updates, 1);
			UT_ASSERT_EQ(vacuum_restores, 1);
			UT_ASSERT_EQ(empty_fsm + vacuum_classifications, 0);
			vacuum_release();
		}
}

UT_TEST(test_remaining_used_items_do_not_enter_empty_publication)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		LVRelState state;
		Buffer vm = 2;
		ItemId item;

		vacuum_reset(&state, 1, true);
		((PageHeader)pages[0].data)->pd_lower += sizeof(ItemIdData);
		item = PageGetItemId(pages[0].data, 2);
		if (kind == 0)
			empty_insert_row_at(2);
		else if (kind == 1)
			ItemIdSetRedirect(item, 1);
		else
			ItemIdSetDead(item);
		memcpy(before, pages, sizeof(pages));
		UT_ASSERT_EQ(vacuum_page(&state, &vm, false), 1);
		UT_ASSERT_EQ(vacuum_records, 1);
		UT_ASSERT_EQ(next_token, 101);
		UT_ASSERT_EQ(inserts + empty_aux_calls + empty_identity_captures, 0);
		UT_ASSERT(!PageIsAllVisible(pages[0].data));
		UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 12);
		UT_ASSERT_EQ(PageGetLSN(pages[0].data), UINT64_C(0x8000));
		vacuum_release();
	}
}

UT_TEST(test_concurrent_insert_after_cleanup_prevents_positive_publication)
{
	LVRelState state;
	Buffer vm = 2;

	vacuum_reset(&state, 8, true);
	empty_race = 1;
	UT_ASSERT_EQ(vacuum_page(&state, &vm, false), 8);
	UT_ASSERT_EQ(vacuum_records, 1);
	UT_ASSERT_EQ(empty_heap_waits, 1);
	UT_ASSERT_EQ(inserts + empty_publication_notes, 0);
	UT_ASSERT(!visibilitymap_heap_page_is_empty(pages[0].data));
	UT_ASSERT(!PageIsAllVisible(pages[0].data));
	UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 12);
	UT_ASSERT_EQ(next_token, 101);
	vacuum_release();
}

UT_TEST(test_identity_or_vm_drift_keeps_completed_cleanup_without_av)
{
	for (unsigned variant = 0; variant < 5; variant++) {
		LVRelState state;
		Buffer vm = 2;

		vacuum_reset(&state, 1, true);
		if (variant < 3)
			empty_identity_change_phase = variant + 1;
		else if (variant == 3)
			empty_vm_race = 1;
		else
			empty_resume = false;
		UT_ASSERT_EQ(vacuum_page(&state, &vm, false), 1);
		UT_ASSERT_EQ(vacuum_records, 1);
		UT_ASSERT_EQ(next_token, 101);
		UT_ASSERT_EQ(inserts + empty_publication_notes, 0);
		UT_ASSERT(visibilitymap_heap_page_is_empty(pages[0].data));
		UT_ASSERT(!PageIsAllVisible(pages[0].data));
		UT_ASSERT_EQ(PageGetLSN(pages[0].data), UINT64_C(0x8000));
		UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 12);
		vacuum_release();
	}
}

UT_TEST(test_resident_misses_restore_cleanup_before_native_publication)
{
	LVRelState state;
	Buffer vm = 2;

	vacuum_reset(&state, 8, false);
	empty_repin_misses = 2;
	UT_ASSERT_EQ(vacuum_page(&state, &vm, true), 8);
	UT_ASSERT_EQ(vacuum_records, 1);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(empty_aux_calls, 1);
	UT_ASSERT_EQ(empty_heap_waits, 3);
	UT_ASSERT_EQ(empty_cleanup_waits, 3);
	UT_ASSERT_EQ(empty_repins, 3);
	UT_ASSERT_EQ(empty_pin_calls, 2);
	UT_ASSERT_EQ(next_token, 102);
	vacuum_release();
}

UT_TEST(test_nonshared_cleanup_keeps_original_classifier_and_vm_pin)
{
	LVRelState state;
	Buffer vm = 2;

	vacuum_reset(&state, 1, true);
	state.shared_maintenance_restricted = state.versioned = false;
	cluster_shared_config = false;
	UT_ASSERT_EQ(vacuum_page(&state, &vm, false), 1);
	UT_ASSERT_EQ(vacuum_records, 1);
	UT_ASSERT_EQ(vacuum_classifications, 1);
	UT_ASSERT_EQ(inserts + empty_aux_calls + empty_identity_captures + empty_heap_waits, 0);
	UT_ASSERT_EQ(next_token, 100);
	UT_ASSERT_EQ(vm, 2);
	UT_ASSERT(!PageIsAllVisible(pages[0].data));
	vacuum_release();
}

UT_TEST(test_cancel_after_cleanup_wal_releases_only_publication_scope)
{
	LVRelState state;
	Buffer vm = 2;
	volatile bool caught = false;

	vacuum_reset(&state, 8, false);
	empty_cancel_after_capture = true;
	expecting_error = true;
	PG_TRY();
	{
		(void)vacuum_page(&state, &vm, true);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(vacuum_records, 1);
	UT_ASSERT_EQ(inserts + empty_publication_notes + empty_heap_waits, 0);
	UT_ASSERT_EQ(next_token, 101);
	UT_ASSERT_EQ(PageGetLSN(pages[0].data), UINT64_C(0x8000));
	UT_ASSERT(pinned[0] && !pinned[1] && !held[0] && !held[1]);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(assertion_failures, 0);
	release_empty_cancel_fixture();
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_last_dead_cleanup_publishes_both_versions_in_each_lock_mode);
	UT_RUN(test_remaining_used_items_do_not_enter_empty_publication);
	UT_RUN(test_concurrent_insert_after_cleanup_prevents_positive_publication);
	UT_RUN(test_identity_or_vm_drift_keeps_completed_cleanup_without_av);
	UT_RUN(test_resident_misses_restore_cleanup_before_native_publication);
	UT_RUN(test_nonshared_cleanup_keeps_original_classifier_and_vm_pin);
	UT_RUN(test_cancel_after_cleanup_wal_releases_only_publication_scope);
	UT_DONE();
	return ut_failed_count != 0;
}
