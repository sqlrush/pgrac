/* Original empty publisher, qualified VM reader and complete range selector.
 * Remaining range bits and external services are controlled image inputs.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
int vacuum_skip_inherited_main(void);
#define VM_QUALIFIED_IOS_MAIN vacuum_skip_inherited_main
#include "test_cluster_vm_qualified_ios.c"
#undef VM_QUALIFIED_IOS_MAIN

static bool skip_active, skip_delay_error;
static unsigned skip_delays, skip_raw_reads, skip_cut_after;

static void
skip_delay(void)
{
	UT_ASSERT(!ios_locked && ios_evaluators == 0);
	if (ios_pin_watch)
		UT_ASSERT_EQ(ios_live_pins, 0);
	skip_delays++;
	if (skip_delay_error)
		elog(ERROR, "controlled cancellation at vacuum delay point");
	if (skip_cut_after && skip_delays == skip_cut_after)
		ios_admission = false;
}

static uint8
skip_raw_status(Relation rel, BlockNumber block, Buffer *buffer)
{
	UT_ASSERT(rel == &relation && !ios_locked && block < 64);
	*buffer = 2;
	skip_raw_reads++;
	return ((uint8 *)PageGetContents(pages[1].data))[block / 4]
		>> ((block % 4) * 2) & VISIBILITYMAP_VALID_BITS;
}

#undef visibilitymap_get_status
#define visibilitymap_get_status skip_raw_status
#define ActiveSnapshotSet() skip_active
#define GetActiveSnapshot() (&ios_snapshot)
#define vacuum_delay_point() skip_delay()
#ifdef FIXTURE_NO_ASSERTS
#undef USE_ASSERT_CHECKING
#undef Assert
#define Assert(condition) ((void)0)
#endif
#include "test_cluster_vm_vacuum_skip.inc"

static LVRelState
skip_reset(uint8 bits)
{
	LVRelState state = {0};
	uint8 byte = bits | (bits << 2) | (bits << 4) | (bits << 6);

	ios_reset(true);
	/* Bit 0/header came from the original publisher/logger. These remaining
	 * bits are controlled current-image inputs, not 34 real publications. */
	memset(PageGetContents(ios_current.data), byte, 16);
	pages[1] = ios_current;
	skip_active = true;
	skip_delay_error = false;
	skip_delays = skip_raw_reads = skip_cut_after = 0;
	state.rel = &relation;
	state.shared_maintenance_restricted = true;
	state.rel_pages = 34;
	state.skipwithvm = true;
	return state;
}

static BlockNumber
skip_run(LVRelState *state, bool *all_visible, bool *range)
{
	Buffer buffer = InvalidBuffer;
	BlockNumber result = lazy_scan_skip(state, &buffer, 0, all_visible, range);

	UT_ASSERT(!ios_locked && ios_evaluators == 0);
	UT_ASSERT_EQ(ios_enters, ios_exits);
	return result;
}

UT_TEST(qualified_af_range_is_skipped_in_both_vacuum_modes)
{
	for (int aggressive = 0; aggressive <= 1; aggressive++) {
		LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
		bool all_visible = false, range = false;
		state.aggressive = aggressive;
		UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), 33);
		UT_ASSERT(range && all_visible && !state.skippedallvis);
		UT_ASSERT_EQ(skip_delays, 33);
		UT_ASSERT_EQ(ios_grants, 34);
		UT_ASSERT_EQ(ios_unlocks, 34);
		UT_ASSERT_EQ(ios_reads, 34);
		UT_ASSERT_EQ(skip_raw_reads, 0);
	}
}

UT_TEST(av_only_range_sets_skippedallvis_only_at_the_original_threshold)
{
	for (int pages = 32; pages <= 34; pages++) {
		LVRelState state = skip_reset(VISIBILITYMAP_ALL_VISIBLE);
		bool all_visible = false, range = true;
		state.rel_pages = pages;
		UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), pages - 1);
		UT_ASSERT(all_visible);
		UT_ASSERT_EQ(range, pages >= 33);
		UT_ASSERT_EQ(state.skippedallvis, pages >= 33);
	}
}

UT_TEST(aggressive_vacuum_does_not_skip_av_only_pages)
{
	LVRelState state = skip_reset(VISIBILITYMAP_ALL_VISIBLE);
	bool all_visible = false, range = true;
	state.aggressive = true;
	UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), 0);
	UT_ASSERT(all_visible && !range && !state.skippedallvis);
	UT_ASSERT_EQ(skip_delays, 0);
	state = skip_reset(VISIBILITYMAP_VALID_BITS);
	state.aggressive = true;
	/* Page 32 is AV only; exactly 32 AF pages precede it. */
	((uint8 *)PageGetContents(ios_current.data))[8] &= ~VISIBILITYMAP_ALL_FROZEN;
	UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), 32);
	UT_ASSERT(all_visible && range && !state.skippedallvis);
}

UT_TEST(disable_skipping_and_the_last_page_still_require_heap_work)
{
	LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
	bool all_visible = false, range = true;
	state.skipwithvm = false;
	UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), 0);
	UT_ASSERT(all_visible && !range);
	UT_ASSERT_EQ(skip_delays, 0);
	state = skip_reset(VISIBILITYMAP_VALID_BITS);
	state.rel_pages = 1;
	UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), 0);
	UT_ASSERT(all_visible && !range && !state.skippedallvis);
}

UT_TEST(old_unknown_format_or_missing_original_active_snapshot_never_skip)
{
	for (int missing = 0; missing < 4; missing++) {
		LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
		bool all_visible = true, range = true;
		switch (missing) {
		case 0: control.catalog_version_no = PGRAC_VM_QUALIFIED_CATALOG_VERSION - 1; break;
		case 1: control.catalog_version_no = 0; break;
		case 2: ControlFile = NULL; break;
		case 3: skip_active = false; break;
		}
		UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), 0);
		UT_ASSERT(!all_visible && !range && !state.skippedallvis);
		UT_ASSERT_EQ(ios_grants + skip_delays + skip_raw_reads, 0);
	}
}

UT_TEST(old_disk_positive_current_clear_stops_before_skipping)
{
	LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
	bool all_visible = true, range = true;
	*PageGetContents(ios_current.data) &= ~VISIBILITYMAP_VALID_BITS;
	UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), 0);
	UT_ASSERT(!all_visible && !range && !state.skippedallvis);
	UT_ASSERT_EQ(ios_grants, 1);
	UT_ASSERT_EQ(skip_delays + skip_raw_reads, 0);
}

UT_TEST(changed_space_membership_snapshot_cid_or_target_cannot_skip)
{
	for (int drift = 1; drift <= 5; drift++) {
		LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
		bool all_visible = true, range = true;
		ios_drift = drift;
		UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), 0);
		UT_ASSERT(!all_visible && !range && !state.skippedallvis);
		UT_ASSERT_EQ(ios_grants, 1);
		UT_ASSERT_EQ(ios_unlocks, 1);
		UT_ASSERT_EQ(skip_delays, 0);
	}
}

UT_TEST(unavailable_or_unproven_vm_source_never_makes_a_range)
{
	for (int missing = 0; missing < 8; missing++) {
		LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
		bool all_visible = true, range = true;
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
		UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), 0);
		UT_ASSERT(!all_visible && !range && !state.skippedallvis);
		UT_ASSERT_EQ(skip_delays + skip_raw_reads, 0);
	}
}

UT_TEST(clear_of_a_neighbour_bit_keeps_the_verified_prefix)
{
	LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
	bool all_visible = true, range = false;
	CritSectionCount = 1;
	UT_ASSERT(visibilitymap_clear_locked(&relation, 33, 2, VISIBILITYMAP_VALID_BITS));
	CritSectionCount = 0;
	ios_current = pages[1];
	UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), 33);
	UT_ASSERT(range && !all_visible && !state.skippedallvis);
	UT_ASSERT_EQ(skip_delays, 33);
}

UT_TEST(later_identity_loss_stops_the_range_without_claiming_unverified_pages)
{
	for (unsigned prefix = 31; prefix <= 32; prefix++) {
		LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
		bool all_visible = true, range = true;
		skip_cut_after = prefix;
		UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), prefix);
		UT_ASSERT(!all_visible && !state.skippedallvis);
		UT_ASSERT_EQ(range, prefix == 32);
		UT_ASSERT_EQ(skip_delays, prefix);
	}
}

UT_TEST(delay_cancel_and_read_recheck_error_leave_no_vm_scope)
{
	for (int error = 0; error < 2; error++) {
		LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
		bool all_visible = false, range = false;
		volatile bool caught = false;
		ios_error = error == 0 ? 1 : 0;
		skip_delay_error = error == 1;
		expecting_error = true;
		PG_TRY();
		{
			(void)skip_run(&state, &all_visible, &range);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT(!ios_locked && ios_evaluators == 0);
		UT_ASSERT_EQ(ios_grants, ios_unlocks);
		UT_ASSERT_EQ(ios_enters, ios_exits);
		UT_ASSERT_EQ(skip_delays, error);
	}
}

UT_TEST(native_range_selection_and_prior_skippedallvis_are_preserved)
{
	LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
	bool all_visible = false, range = false;
	state.shared_maintenance_restricted = false;
	state.skippedallvis = true;
	skip_active = false;
	cluster_enabled = cluster_shared_config = false;
	UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), 33);
	UT_ASSERT(all_visible && range && state.skippedallvis);
	UT_ASSERT_EQ(skip_raw_reads, 34);
	UT_ASSERT_EQ(ios_grants + ios_reads + ios_enters, 0);
}

UT_TEST(zero_catalog_threshold_never_admits_even_the_largest_loaded_version)
{
	LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
	bool all_visible = true, range = true;
	control.catalog_version_no = PG_UINT32_MAX;
	UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), 0);
	UT_ASSERT(!all_visible && !range && !state.skippedallvis);
	UT_ASSERT_EQ(ios_grants + ios_reads + ios_enters + skip_delays, 0);
}

UT_TEST(no_page_in_the_requested_range_cannot_claim_shared_visibility)
{
	for (int empty = 0; empty <= 1; empty++) {
		LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
		Buffer buffer = InvalidBuffer;
		bool all_visible = true, range = true;
		BlockNumber next = empty ? 0 : 34;
		if (empty)
			state.rel_pages = 0;
		UT_ASSERT_EQ(lazy_scan_skip(&state, &buffer, next, &all_visible, &range), next);
		UT_ASSERT(!all_visible && !range && !state.skippedallvis);
		UT_ASSERT_EQ(ios_grants + ios_reads + ios_enters + skip_delays + skip_raw_reads, 0);
	}
}

UT_TEST(shared_range_unpins_before_every_delay_and_final_heap_work)
{
	LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
	bool all_visible = false, range = false;
	ios_pin_watch = true;
	UT_ASSERT_EQ(skip_run(&state, &all_visible, &range), 33);
	UT_ASSERT(all_visible && range && !state.skippedallvis);
	UT_ASSERT_EQ(ios_live_pins, 0);
	UT_ASSERT_EQ(ios_reads, 34);
	UT_ASSERT_EQ(ios_releases, 34);
}

UT_TEST(shared_no_skip_refusals_release_a_preexisting_vm_pin)
{
	for (int fault = 0; fault < 5; fault++) {
		LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
		Buffer buffer = 2;
		bool all_visible = false, skipsallvis = false;
		ios_pin_watch = true; ios_live_pins = 1;
		switch (fault) {
		case 0: state.skipwithvm = false; break;
		case 1: *PageGetContents(ios_current.data) = 0; break;
		case 2: ios_grant = false; break;
		case 3: ios_drift = 4; break;
		case 4: skip_active = false; break;
		}
		UT_ASSERT(!lazy_scan_skip_shared_page(&state, &buffer, 0, &all_visible, &skipsallvis));
		UT_ASSERT_EQ(buffer, InvalidBuffer);
		UT_ASSERT_EQ(ios_live_pins, 0);
		UT_ASSERT_EQ(ios_releases, 1);
	}
}

UT_TEST(shared_range_errors_retire_pin_before_propagation)
{
	for (int error = 0; error < 2; error++) {
		LVRelState state = skip_reset(VISIBILITYMAP_VALID_BITS);
		bool all_visible = false, range = false;
		volatile bool caught = false;
		ios_pin_watch = true;
		ios_error = error == 0; skip_delay_error = error == 1;
		expecting_error = true;
		PG_TRY();
		{
			(void)skip_run(&state, &all_visible, &range);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT(!ios_locked && ios_evaluators == 0);
		UT_ASSERT_EQ(ios_live_pins, 0);
		UT_ASSERT_EQ(ios_releases, 1);
	}
}

int
main(void)
{
	if (PGRAC_VM_QUALIFIED_CATALOG_VERSION == 0) {
		UT_PLAN(4);
		UT_RUN(zero_catalog_threshold_never_admits_even_the_largest_loaded_version);
		UT_RUN(unavailable_or_unproven_vm_source_never_makes_a_range);
		UT_RUN(no_page_in_the_requested_range_cannot_claim_shared_visibility);
		UT_RUN(shared_no_skip_refusals_release_a_preexisting_vm_pin);
	} else {
		UT_PLAN(16);
		UT_RUN(qualified_af_range_is_skipped_in_both_vacuum_modes);
		UT_RUN(av_only_range_sets_skippedallvis_only_at_the_original_threshold);
		UT_RUN(aggressive_vacuum_does_not_skip_av_only_pages);
		UT_RUN(disable_skipping_and_the_last_page_still_require_heap_work);
		UT_RUN(old_unknown_format_or_missing_original_active_snapshot_never_skip);
		UT_RUN(old_disk_positive_current_clear_stops_before_skipping);
		UT_RUN(changed_space_membership_snapshot_cid_or_target_cannot_skip);
		UT_RUN(unavailable_or_unproven_vm_source_never_makes_a_range);
		UT_RUN(clear_of_a_neighbour_bit_keeps_the_verified_prefix);
		UT_RUN(later_identity_loss_stops_the_range_without_claiming_unverified_pages);
		UT_RUN(delay_cancel_and_read_recheck_error_leave_no_vm_scope);
		UT_RUN(native_range_selection_and_prior_skippedallvis_are_preserved);
		UT_RUN(no_page_in_the_requested_range_cannot_claim_shared_visibility);
		UT_RUN(shared_range_unpins_before_every_delay_and_final_heap_work);
		UT_RUN(shared_no_skip_refusals_release_a_preexisting_vm_pin);
		UT_RUN(shared_range_errors_retire_pin_before_propagation);
	}
	UT_DONE();
	return ut_failed_count != 0;
}
