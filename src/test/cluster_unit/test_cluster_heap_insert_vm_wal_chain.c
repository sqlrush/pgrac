/*-------------------------------------------------------------------------
 * test_cluster_heap_insert_vm_wal_chain.c
 *    Original VM acquisition through INSERT's versioned clear and WAL.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * The original windows execute in order. Transport, heap target selection,
 * writer validation and receipt/ITL admission between them are controlled;
 * this does not execute a full INSERT or prove physical ACK/durability.
 *-------------------------------------------------------------------------
 */
int insert_vm_wal_inherited_main(void);
#define main insert_vm_wal_inherited_main
#include "test_cluster_heap_insert_version.c"
#undef main

static void
execute_single_wal_chain(bool versioned)
{
	Relation relation = &relation_data;
	HeapTuple heaptup = &tuple;
	Buffer buffer, vmbuffer = InvalidBuffer;
	bool vm_locked, all_visible_cleared = false;
	bool cluster_itl_active = false;
	uint8 cluster_itl_slot = 0;
	OffsetNumber cluster_itl_insert_offnum = FirstOffsetNumber;
	TransactionId canonical_xid = 500;
	SCN cluster_itl_write_scn = 800;
	UBA cluster_itl_uba = InvalidUba_init;
	int options = 0;
	bool cluster_page_versioned = versioned;
	ClusterSpaceIdentity cluster_page_identity = identity;
	RfPageProducerBatchV1 cluster_page_versions = { 0 };
	ResourceXAuxiliaryAcquireContext cluster_vm_context = { 0 };
	ClusterHeapVmMutationState cluster_vm_state = { 0 };

	multi_inserting = false;
cluster_heap_insert_retry:
	CHECK_FOR_INTERRUPTS();
	buffer = RelationGetBufferForTuple(relation, heaptup->t_len, NULL,
		options, NULL, &vmbuffer, NULL, 0);
#include "test_cluster_heap_vm_insert.inc"
#include "test_cluster_heap_insert_capture.inc"
	/* The original no-retry/receipt boundary is a controlled input. Only
	 * acquisition, exact version capture and native publication execute. */
	UT_ASSERT(multi_heap_locked && (!vm_locked || multi_vm_locked));
	UT_ASSERT_EQ(inserts + vm_clears, 0);
#include "test_cluster_heap_insert_version.inc"
	if (vm_locked)
		LockBuffer(vmbuffer, BUFFER_LOCK_UNLOCK);
	UnlockReleaseBuffer(buffer);
	if (BufferIsValid(vmbuffer))
		ReleaseBuffer(vmbuffer);
}

static bool
try_wal_chain(bool batch, bool versioned)
{
	if (sigsetjmp(multi_jump, 0) != 0)
		return false;
	if (batch)
		execute_multi_loop(1, versioned, false);
	else
		execute_single_wal_chain(versioned);
	return true;
}

static void
prepare_wal_chain(bool shared, bool visible)
{
	prepare_multi_loop(shared, visible);
	multi_forbid_nested = true;
	pages[1].data[BLCKSZ - 1] = pages[3].data[BLCKSZ - 1] = 42;
}

static void
check_clear_publication(Buffer heap, Buffer map)
{
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(vm_clears, 1);
	UT_ASSERT_EQ(edge_count, 2);
	UT_ASSERT_EQ(edge_token, 101);
	UT_ASSERT_EQ(edges[0].before.mutation_token, heap == 1 ? 17 : 41);
	UT_ASSERT_EQ(edges[1].before.mutation_token, map == 2 ? 31 : 61);
	UT_ASSERT_EQ(edges[1].block_id, 1);
	UT_ASSERT_EQ(registered_buffers[0], heap);
	UT_ASSERT_EQ(registered_buffers[1], map);
	UT_ASSERT_EQ(register_flags[1], REGBUF_FORCE_IMAGE);
	UT_ASSERT(dirty[heap - 1] && dirty[map - 1]);
	UT_ASSERT_EQ(((PageHeader)pages[heap - 1].data)->pd_block_scn, 101);
	UT_ASSERT_EQ(((PageHeader)pages[map - 1].data)->pd_block_scn, 101);
	/* Captured WAL images are indexed by block ID, independently of the
	 * physical Buffer selected after a retry. */
	UT_ASSERT_EQ(((PageHeader)images[0].data)->pd_block_scn, 101);
	UT_ASSERT_EQ(((PageHeader)images[1].data)->pd_block_scn, 101);
	UT_ASSERT_EQ(PageGetLSN(pages[heap - 1].data), 200);
	UT_ASSERT_EQ(PageGetLSN(pages[map - 1].data), 200);
	UT_ASSERT(!PageIsAllVisible(pages[heap - 1].data));
	UT_ASSERT(!PageIsAllVisible(images[0].data));
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(pages[heap - 1].data), 1);
	UT_ASSERT_EQ(PageGetMaxOffsetNumber(images[0].data), 1);
	UT_ASSERT_EQ(images[1].data[SizeOfPageHeaderData] & 3, 0);
	UT_ASSERT_EQ(pages[map - 1].data[SizeOfPageHeaderData] & 3, 0);
	UT_ASSERT_EQ(images[1].data[BLCKSZ - 1], 42);
	UT_ASSERT(!multi_heap_locked && !multi_vm_locked);
	UT_ASSERT(!multi_vm_pins[1] && !multi_vm_pins[3]);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

UT_TEST(cold_writer_reaches_actual_clear_and_two_page_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int bit = 0; bit < 2; bit++) {
			prepare_wal_chain(true, true);
			pages[1].data[SizeOfPageHeaderData] = bit ? 3 : 0;
			UT_ASSERT(try_wal_chain(batch, true));
			check_clear_publication(1, 2);
			UT_ASSERT_EQ(multi_aux_calls, 1);
			UT_ASSERT_EQ(multi_captures, 1);
			UT_ASSERT_EQ(multi_resumes, 1);
			UT_ASSERT_EQ(multi_selector_calls, 2);
		}
	}
}
UT_TEST(clear_preserves_other_heap_bits_and_far_wal_image_bytes)
{
	for (int batch = 0; batch < 2; batch++) {
		prepare_wal_chain(true, true);
		pages[1].data[SizeOfPageHeaderData] = (char)0xff;
		UT_ASSERT(try_wal_chain(batch, true));
		check_clear_publication(1, 2);
		UT_ASSERT_EQ((uint8)pages[1].data[SizeOfPageHeaderData], 0xfc);
		UT_ASSERT_EQ((uint8)images[1].data[SizeOfPageHeaderData], 0xfc);
	}
}
UT_TEST(refusal_and_pin_replacement_publish_only_after_final_requalification)
{
	for (int bits = 0; bits < 4; bits++) {
		prepare_wal_chain(true, true);
		multi_refusals = 4; multi_replacements = bits & 1 ? 4 : 0;
		UT_ASSERT(try_wal_chain(bits & 2, true));
		check_clear_publication(1, 2);
		UT_ASSERT_EQ(multi_selector_calls, 6);
		UT_ASSERT_EQ(multi_aux_calls, 5);
		UT_ASSERT_EQ(multi_context_starts, 1);
		UT_ASSERT_EQ(next_token, 101);
	}
}
UT_TEST(busy_original_writer_reprepares_without_publishing_abandoned_attempts)
{
	for (int batch = 0; batch < 2; batch++) {
		prepare_wal_chain(true, true); multi_resume_refusals = 4;
		UT_ASSERT(try_wal_chain(batch, true));
		check_clear_publication(1, 2);
		UT_ASSERT_EQ(multi_captures, 5); UT_ASSERT_EQ(multi_resumes, 5);
		UT_ASSERT_EQ(next_token, 101);
	}
}
UT_TEST(repicked_target_wal_covers_only_the_new_heap_and_vm)
{
	for (int batch = 0; batch < 2; batch++) {
		prepare_wal_chain(true, true);
		multi_refusals = multi_replacements = 1; multi_change = 2;
		UT_ASSERT(try_wal_chain(batch, true));
		check_clear_publication(3, 4);
		UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 17);
		UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 31);
		UT_ASSERT_EQ(pages[1].data[SizeOfPageHeaderData], 3);
		UT_ASSERT(!dirty[0] && !dirty[1]);
	}
}
UT_TEST(failed_history_or_r4_change_cannot_publish_a_new_target)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int change = 3; change <= 4; change++) {
			prepare_wal_chain(true, true);
			multi_refusals = multi_replacements = 1; multi_change = change;
			UT_ASSERT(!try_wal_chain(batch, true));
			UT_ASSERT_EQ(inserts + vm_clears, 0); UT_ASSERT_EQ(next_token, 100);
			UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 17);
			UT_ASSERT_EQ(((PageHeader)pages[2].data)->pd_block_scn, 41);
		}
	}
}
UT_TEST(missing_or_error_writer_preserves_pages_before_any_wal)
{
	for (int batch = 0; batch < 2; batch++) {
		for (int fault = 0; fault < 3; fault++) {
			PGAlignedBlock before[4];

			prepare_wal_chain(true, true);
			memcpy(before, pages, sizeof(before));
			multi_missing_writer = fault == 0;
			multi_writer_error = fault;
			UT_ASSERT(!try_wal_chain(batch, true));
			UT_ASSERT_EQ(inserts + vm_clears, 0); UT_ASSERT_EQ(next_token, 100);
			UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
			UT_ASSERT(!registered[0] && !registered[1]);
		}
	}
}
UT_TEST(cancel_before_target_repick_never_stamps_clears_or_logs)
{
	for (int batch = 0; batch < 2; batch++) {
		PGAlignedBlock before[4];

		prepare_wal_chain(true, true); memcpy(before, pages, sizeof(before));
		multi_refusals = multi_replacements = multi_fault = 1;
		UT_ASSERT(!try_wal_chain(batch, true));
		UT_ASSERT_EQ(inserts + vm_clears, 0); UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
		UT_ASSERT(!multi_heap_locked && !multi_vm_locked && !multi_vm_pins[1]);
		UT_ASSERT(!InterruptPending);
	}
}
UT_TEST(no_visible_hint_publishes_only_heap_without_vm_authority)
{
	for (int batch = 0; batch < 2; batch++) {
		prepare_wal_chain(true, false);
		UT_ASSERT(try_wal_chain(batch, true));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(vm_clears, 0);
		UT_ASSERT_EQ(edge_count, 1); UT_ASSERT_EQ(edge_token, 101);
		UT_ASSERT_EQ(multi_aux_calls + multi_captures + multi_resumes, 0);
		UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 31);
		UT_ASSERT(!registered[1] && !dirty[1]);
	}
}
UT_TEST(native_owner_keeps_original_wal_without_shared_writer_calls)
{
	for (int batch = 0; batch < 2; batch++) {
		prepare_wal_chain(false, true);
		UT_ASSERT(try_wal_chain(batch, false));
		UT_ASSERT_EQ(inserts, 1); UT_ASSERT_EQ(vm_clears, 1);
		UT_ASSERT_EQ(edge_count, 0);
		UT_ASSERT_EQ(multi_aux_calls + multi_captures + multi_resumes, 0);
		UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 31);
		UT_ASSERT_EQ(pages[1].data[SizeOfPageHeaderData], 0);
		UT_ASSERT(!registered[1]);
	}
}
#ifndef INSERT_VM_WAL_MAIN
#define INSERT_VM_WAL_MAIN main
#endif
int
INSERT_VM_WAL_MAIN(void)
{
	UT_PLAN(10);
	UT_RUN(cold_writer_reaches_actual_clear_and_two_page_wal);
	UT_RUN(clear_preserves_other_heap_bits_and_far_wal_image_bytes);
	UT_RUN(refusal_and_pin_replacement_publish_only_after_final_requalification);
	UT_RUN(busy_original_writer_reprepares_without_publishing_abandoned_attempts);
	UT_RUN(repicked_target_wal_covers_only_the_new_heap_and_vm);
	UT_RUN(failed_history_or_r4_change_cannot_publish_a_new_target);
	UT_RUN(missing_or_error_writer_preserves_pages_before_any_wal);
	UT_RUN(cancel_before_target_repick_never_stamps_clears_or_logs);
	UT_RUN(no_visible_hint_publishes_only_heap_without_vm_authority);
	UT_RUN(native_owner_keeps_original_wal_without_shared_writer_calls);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
