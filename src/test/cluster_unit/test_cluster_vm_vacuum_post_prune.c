/*-------------------------------------------------------------------------
 *
 * test_cluster_vm_vacuum_post_prune.c
 *    Native prune execution followed by VACUUM's retained empty publisher.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_vm_vacuum_post_prune.c
 *
 * NOTES
 *    The original caller suffix, prune executor and empty publisher execute.
 *    Tuple eligibility, prune WAL, freeze-proof services, deferred-clear
 *    dispatch and FSM IO remain controlled leaves. No granting-read claim.
 *-------------------------------------------------------------------------
 */
int post_prune_inherited_main(void);
#define main post_prune_inherited_main
#include "test_cluster_vm_visible_version.c"
#undef main

#include "access/heapam.h"

#ifdef FIXTURE_NO_ASSERTS
/* Match native release preprocessing for the following original helpers. */
#undef USE_ASSERT_CHECKING
#undef PG_USED_FOR_ASSERTS_ONLY
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
#endif

static void page_verify_redirects(Page page);
#include "test_cluster_heap_prune_helpers.inc"

static unsigned post_validations, post_preparations, post_clears;
static bool post_input_changed;
static uint64 post_input_token;

static void
post_validate(LVRelState *state, Page page, BlockNumber block, Buffer buffer)
{
	UT_ASSERT(state->rel == &relation && page == pages[0].data && block == 0 && buffer == 1);
	UT_ASSERT(held[0] && pinned[0] && !held[1]);
	UT_ASSERT_EQ(CritSectionCount, 0);
	post_validations++;
	/* This leaf observes invalidation only; real F proof validation is tested
	 * separately. A new publisher token cannot certify the earlier input. */
	post_input_changed = ((PageHeader)page)->pd_block_scn != post_input_token;
}

static void
post_prepare(LVRelState *state, Buffer recent, Buffer *vm)
{
	UT_ASSERT(state->rel == &relation && recent == 1 && vm != NULL);
	UT_ASSERT(!held[0] && !held[1] && !pinned[0]);
	UT_ASSERT_EQ(post_validations, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	post_preparations++;
}

static void
post_record(Relation rel, BlockNumber block, Size space)
{
	UT_ASSERT_EQ(post_preparations, 1);
	RecordPageWithFreeSpace(rel, block, space);
}

static void
post_clear(Relation rel, const ClusterSpaceIdentity *original, BlockNumber block, Buffer *vm)
{
	UT_ASSERT(rel == &relation && original != NULL && block == 0 && vm != NULL);
	UT_ASSERT(!held[0] && !held[1] && !pinned[0]);
	UT_ASSERT_EQ(post_preparations, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	post_clears++; /* Dispatch only; original deferred-clear body has its own fixture. */
}

static void
post_finish(LVRelState *vacrel, Buffer *map, uint8 shared_vm_status)
{
	Buffer buf = 1, vmbuffer = *map;
	BlockNumber blkno = 0;
	Page page = pages[0].data;
	bool shared_vm_clear = vacrel->versioned && (shared_vm_status & VISIBILITYMAP_ALL_VISIBLE) != 0
						   && !PageIsAllVisible(page);
	struct {
		bool has_lpdead_items;
	} prunestate = { false };

#define lazy_validate_shared_freeze post_validate
#define lazy_prepare_shared_freeze post_prepare
#define RecordPageWithFreeSpace post_record
#define lazy_clear_vm_after_heap post_clear
#include "test_cluster_vm_vacuum_post_prune.inc"
#undef lazy_clear_vm_after_heap
#undef RecordPageWithFreeSpace
#undef lazy_prepare_shared_freeze
#undef lazy_validate_shared_freeze
	*map = vmbuffer;
}

static LVRelState
post_reset(bool shared, int indexes, uint8 status, OffsetNumber offset)
{
	LVRelState state = empty_reset(false, (VISIBILITYMAP_VALID_BITS << 2) | status);
	HeapTupleHeader tuple;

	state.shared_maintenance_restricted = state.versioned = shared;
	state.nindexes = indexes;
	state.do_index_vacuuming = indexes != 0;
	state.nonempty_pages = 0;
	post_validations = post_preparations = post_clears = 0;
	post_input_changed = false;
	empty_insert_row_at(offset);
	tuple = (HeapTupleHeader)PageGetItem(pages[0].data, PageGetItemId(pages[0].data, offset));
	HeapTupleHeaderSetHeapOnly(tuple);
	/* The existing pruner supplies eligibility and WAL responsibility. Run its
	 * original complete physical executor here, not a fake LP transition. */
	heap_page_prune_execute(1, NULL, 0, NULL, 0, &offset, 1);
	post_input_token = ((PageHeader)pages[0].data)->pd_block_scn;
	memcpy(before, pages, sizeof(pages)); /* Original publisher input is post-prune. */
	UT_ASSERT(visibilitymap_heap_page_is_empty(pages[0].data));
	return state;
}

UT_TEST(test_post_prune_unused_page_publishes_native_two_page_wal)
{
	const OffsetNumber offsets[] = { 1, 8, MaxHeapTuplesPerPage };

	for (int indexes = 0; indexes < 2; indexes++)
		for (unsigned i = 0; i < lengthof(offsets); i++) {
			LVRelState state = post_reset(true, indexes, 0, offsets[i]);
			Buffer vm = 2;

			post_finish(&state, &vm, 0);
			UT_ASSERT(PageIsAllVisible(pages[0].data));
			UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 15);
			UT_ASSERT_EQ(inserts, 1);
			UT_ASSERT_EQ(edge_count, 2);
			UT_ASSERT_EQ(edges[0].before.mutation_token, 30);
			UT_ASSERT_EQ(edges[1].before.mutation_token, post_input_token);
			UT_ASSERT_EQ(empty_cleanup_waits, 1);
			UT_ASSERT_EQ(empty_heap_waits, 1);
			UT_ASSERT_EQ(post_validations + post_preparations + empty_fsm, 3);
			UT_ASSERT(post_input_changed);
			UT_ASSERT_EQ(post_clears + state.nonempty_pages, 0);
			UT_ASSERT(!held[0] && !held[1] && !pinned[0] && pinned[1]);
		}
}

UT_TEST(test_post_prune_publication_retires_obsolete_deferred_clear)
{
	LVRelState state = post_reset(true, 1, VISIBILITYMAP_VALID_BITS, 8);
	Buffer vm = 2;

	post_finish(&state, &vm, VISIBILITYMAP_VALID_BITS);
	UT_ASSERT(PageIsAllVisible(pages[0].data));
	UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), 15);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(post_clears, 0);
	UT_ASSERT_EQ(empty_fsm, 1);
}

UT_TEST(test_post_prune_concurrent_insert_keeps_page_nonempty_and_unpublished)
{
	for (int indexes = 0; indexes < 2; indexes++)
		for (BlockNumber prior = 0; prior <= 9; prior += 9) {
			LVRelState state = post_reset(true, indexes, VISIBILITYMAP_VALID_BITS, 8);
			Buffer vm = 2;

			state.nonempty_pages = prior;
			empty_race = 1;
			post_finish(&state, &vm, VISIBILITYMAP_VALID_BITS);
			UT_ASSERT(!visibilitymap_heap_page_is_empty(pages[0].data));
			UT_ASSERT(!PageIsAllVisible(pages[0].data));
			UT_ASSERT_EQ(state.nonempty_pages, Max(prior, 1));
			UT_ASSERT_EQ(inserts + edge_count + empty_publication_notes, 0);
			UT_ASSERT_EQ(post_clears, 1);
			UT_ASSERT_EQ(post_validations + post_preparations + empty_fsm, 3);
			UT_ASSERT(!held[0] && !held[1] && !pinned[0] && pinned[1]);
		}
}

UT_TEST(test_post_prune_remaining_used_items_do_not_enter_empty_publisher)
{
	for (int kind = 0; kind < 3; kind++) {
		LVRelState state = post_reset(true, 1, 0, 8);
		Buffer vm = 2;

		empty_insert_row_at(1);
		if (kind == 1)
			ItemIdSetDead(PageGetItemId(pages[0].data, 1));
		if (kind == 2) {
			empty_insert_row_at(2);
			HeapTupleHeaderSetHeapOnly(
				(HeapTupleHeader)PageGetItem(pages[0].data, PageGetItemId(pages[0].data, 2)));
			ItemIdSetRedirect(PageGetItemId(pages[0].data, 1), 2);
		}
		state.nonempty_pages = 1; /* Already established by native prunestate. */
		post_finish(&state, &vm, 0);
		UT_ASSERT_EQ(empty_identity_captures + empty_aux_calls + inserts + edge_count, 0);
		UT_ASSERT_EQ(state.nonempty_pages, 1);
		UT_ASSERT_EQ(post_validations + post_preparations + empty_fsm, 3);
	}
}

UT_TEST(test_post_prune_refusals_keep_existing_deferred_clear_responsibility)
{
	for (int reason = 0; reason < 4; reason++) {
		LVRelState state = post_reset(true, 0, VISIBILITYMAP_VALID_BITS, 8);
		Buffer vm = 2;

		if (reason == 0)
			empty_identity_available = false;
		if (reason == 1)
			empty_identity_change_phase = 2;
		if (reason == 2)
			empty_vm_writer = false;
		if (reason == 3)
			empty_resume = false;
		post_finish(&state, &vm, VISIBILITYMAP_VALID_BITS);
		UT_ASSERT(!PageIsAllVisible(pages[0].data));
		UT_ASSERT_EQ(inserts + edge_count + empty_publication_notes, 0);
		UT_ASSERT_EQ(post_clears, 1);
		UT_ASSERT_EQ(post_validations + post_preparations + empty_fsm, 3);
		UT_ASSERT(!held[0] && !held[1] && !pinned[0] && pinned[1]);
	}
}

UT_TEST(test_post_prune_cancel_does_not_start_freeze_or_fsm_after_publication_attempt)
{
	LVRelState state = post_reset(true, 0, 0, 8);
	Buffer vm = 2;
	volatile bool caught = false;

	empty_cancel_after_capture = expecting_error = true;
	if (setjmp(error_jump) == 0)
		post_finish(&state, &vm, 0);
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT(visibilitymap_heap_page_is_empty(pages[0].data));
	UT_ASSERT_EQ(inserts + edge_count + post_validations + post_preparations + empty_fsm, 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

UT_TEST(test_post_prune_native_control_keeps_original_final_steps)
{
	LVRelState state = post_reset(false, 0, 0, 8);
	Buffer vm = 2;

	post_finish(&state, &vm, 0);
	UT_ASSERT_EQ(empty_aux_calls + empty_identity_captures + inserts + edge_count, 0);
	UT_ASSERT_EQ(post_validations + post_preparations + empty_fsm, 3);
	UT_ASSERT(!held[0] && !held[1] && !pinned[0] && pinned[1]);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_post_prune_unused_page_publishes_native_two_page_wal);
	UT_RUN(test_post_prune_publication_retires_obsolete_deferred_clear);
	UT_RUN(test_post_prune_concurrent_insert_keeps_page_nonempty_and_unpublished);
	UT_RUN(test_post_prune_remaining_used_items_do_not_enter_empty_publisher);
	UT_RUN(test_post_prune_refusals_keep_existing_deferred_clear_responsibility);
	UT_RUN(test_post_prune_cancel_does_not_start_freeze_or_fsm_after_publication_attempt);
	UT_RUN(test_post_prune_native_control_keeps_original_final_steps);
	UT_DONE();
	return ut_failed_count != 0;
}
