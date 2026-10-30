/*-------------------------------------------------------------------------
 * test_cluster_heap_insert_vm_authority.c
 *    Shared INSERT takes ordinary VM authority outside heap content locks.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
int insert_authority_inherited_main(void);
#define main insert_authority_inherited_main
#include "test_cluster_heap_insert_version.c"
#undef main

/* Actual single INSERT VM bracket. Target selection and the no-APPLY census
 * are controlled; the independent original publication tests retain WAL. */
static void
execute_single_bracket(bool versioned)
{
	Relation relation = &relation_data;
	Buffer buffer, vmbuffer = InvalidBuffer;
	bool vm_locked, cluster_page_versioned pg_attribute_unused() = versioned;
	ResourceXAuxiliaryAcquireContext cluster_vm_context = { 0 };
#ifdef FIXTURE_HAS_INSERT_VM_STATE
	ClusterHeapVmInsertState cluster_vm_state = { 0 };
#endif

cluster_heap_insert_retry:
	CHECK_FOR_INTERRUPTS();
	buffer = RelationGetBufferForTuple(relation, tuple.t_len, NULL, 0, NULL, &vmbuffer, NULL, 0);
#include "test_cluster_heap_vm_insert.inc"
	UT_ASSERT(vm_locked && multi_heap_locked && multi_vm_locked);
	LockBuffer(vmbuffer, BUFFER_LOCK_UNLOCK);
	UnlockReleaseBuffer(buffer);
	ReleaseBuffer(vmbuffer);
}

static bool
try_insert_authority(bool batch, bool versioned)
{
	if (sigsetjmp(multi_jump, 0) != 0)
		return false;
	if (batch)
		execute_multi_loop(1, versioned, false);
	else
		execute_single_bracket(versioned);
	return true;
}

UT_TEST(test_insert_both_owners_prepare_first_vm_outside_heap)
{
	for (int batch = 0; batch < 2; batch++) {
		prepare_multi_loop(true, true);
		multi_forbid_nested = true;
		UT_ASSERT(try_insert_authority(batch != 0, true));
		UT_ASSERT_EQ(multi_selector_calls, 2);
		UT_ASSERT_EQ(multi_aux_calls, 1);
		UT_ASSERT_EQ(multi_captures, 1);
		UT_ASSERT_EQ(multi_resumes, 1);
		UT_ASSERT_EQ(inserts, batch);
		UT_ASSERT(!multi_heap_locked && !multi_vm_locked && !multi_vm_pins[1]);
	}
}

UT_TEST(test_insert_both_owners_keep_aux_cursor_across_refusals)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int replacement = 0; replacement < 2; replacement++) {
			prepare_multi_loop(true, true);
			multi_forbid_nested = true;
			multi_refusals = 4;
			multi_replacements = replacement ? 4 : 0;
			UT_ASSERT(try_insert_authority(batch != 0, true));
			UT_ASSERT_EQ(multi_context_starts, 1);
			UT_ASSERT_EQ(multi_aux_calls, 5);
			UT_ASSERT_EQ(multi_selector_calls, 6);
			UT_ASSERT_EQ(multi_captures, 1);
			UT_ASSERT_EQ(multi_resumes, 1);
			UT_ASSERT_EQ(inserts, batch);
		}
	}
}

UT_TEST(test_insert_both_owners_reprepare_stale_or_busy_vm)
{
	for (int batch = 0; batch < 2; batch++) {
		prepare_multi_loop(true, true);
		multi_forbid_nested = true;
		multi_resume_refusals = 4;
		UT_ASSERT(try_insert_authority(batch != 0, true));
		UT_ASSERT_EQ(multi_selector_calls, 6);
		UT_ASSERT_EQ(multi_aux_calls, 5);
		UT_ASSERT_EQ(multi_captures, 5);
		UT_ASSERT_EQ(multi_resumes, 5);
		UT_ASSERT_EQ(inserts, batch);
	}
}

UT_TEST(test_batch_reuses_original_vm_writer_only_after_each_conditional_check)
{
	prepare_multi_loop(true, true);
	multi_forbid_nested = multi_same_vm = true;
	blocks[2] = 1; /* Both heap pages map to the same physical VM page. */
	pages[1].data[SizeOfPageHeaderData] = VISIBILITYMAP_VALID_BITS
		| (VISIBILITYMAP_VALID_BITS << 2);
	UT_ASSERT(try_multi_loop(2, true, false));
	UT_ASSERT_EQ(multi_selector_calls, 3);
	UT_ASSERT_EQ(multi_aux_calls, 1);
	UT_ASSERT_EQ(multi_captures, 1);
	UT_ASSERT_EQ(multi_resumes, 2);
	UT_ASSERT_EQ(multi_done, 2);
	UT_ASSERT_EQ(multi_pages_used, 1);
	UT_ASSERT_EQ(inserts, 2);
	UT_ASSERT_EQ(vm_clears, 2);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 102);
	UT_ASSERT_EQ(pages[1].data[SizeOfPageHeaderData], 0);
}

UT_TEST(test_insert_capture_failure_never_publishes_or_leaves_vm_content)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int missing = 0; missing < 2; missing++) {
			prepare_multi_loop(true, true);
			multi_forbid_nested = true;
			multi_missing_writer = missing != 0;
			multi_writer_error = missing ? 0 : 1;
			UT_ASSERT(!try_insert_authority(batch != 0, true));
			UT_ASSERT_EQ(multi_captures, 1);
			UT_ASSERT(!multi_heap_locked && !multi_vm_locked);
			UT_ASSERT(multi_vm_pins[1]); /* Original pin remains caller-owned at ERROR. */
			UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
			UT_ASSERT_EQ(inserts + vm_clears, 0);
			UT_ASSERT_EQ(next_token, 100);
			ReleaseBuffer(2);
		}
	}
}

UT_TEST(test_insert_resume_error_cannot_publish)
{
	for (int batch = 0; batch < 2; batch++) {
		prepare_multi_loop(true, true);
		multi_forbid_nested = true;
		multi_writer_error = 2;
		UT_ASSERT(!try_insert_authority(batch != 0, true));
		UT_ASSERT_EQ(multi_captures, 1);
		UT_ASSERT_EQ(multi_resumes, 1);
		UT_ASSERT(multi_heap_locked && !multi_vm_locked && multi_vm_pins[1]);
		UT_ASSERT_EQ(inserts + vm_clears, 0);
		UnlockReleaseBuffer(1);
		ReleaseBuffer(2);
	}
}

UT_TEST(test_insert_cancel_after_aux_refusal_precedes_target_repick)
{
	for (int batch = 0; batch < 2; batch++) {
		prepare_multi_loop(true, true);
		multi_forbid_nested = true;
		multi_refusals = multi_replacements = multi_fault = 1;
		UT_ASSERT(!try_insert_authority(batch != 0, true));
		UT_ASSERT_EQ(multi_aux_calls, 1);
		UT_ASSERT_EQ(multi_selector_calls, 1);
		UT_ASSERT(!multi_heap_locked && !multi_vm_locked && !multi_vm_pins[1]);
		UT_ASSERT(!InterruptPending);
		UT_ASSERT_EQ(inserts + vm_clears, 0);
	}
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_insert_both_owners_prepare_first_vm_outside_heap);
	UT_RUN(test_insert_both_owners_keep_aux_cursor_across_refusals);
	UT_RUN(test_insert_both_owners_reprepare_stale_or_busy_vm);
	UT_RUN(test_batch_reuses_original_vm_writer_only_after_each_conditional_check);
	UT_RUN(test_insert_capture_failure_never_publishes_or_leaves_vm_content);
	UT_RUN(test_insert_resume_error_cannot_publish);
	UT_RUN(test_insert_cancel_after_aux_refusal_precedes_target_repick);
	UT_RUN(test_multi_vm_refusal_reselects_before_any_batch_publication);
	UT_RUN(test_multi_vm_retarget_preserves_or_rejects_original_context);
	UT_RUN(test_multi_vm_retries_do_not_advance_page_accounting);
	UT_RUN(test_multi_native_and_frozen_controls_keep_the_original_batch);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
