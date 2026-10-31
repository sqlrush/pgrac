/*-------------------------------------------------------------------------
 * test_cluster_heap_dml_vm_wal_chain.c
 *    Original DML VM writer helpers through final capture and clear/WAL.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * The driver and transport/KO/receipt admission are controlled. Actual writer
 * helpers, final captures, bitmap clear and complete mutation/WAL regions run;
 * this is not a full DELETE/UPDATE, remote ACK or durability test.
 *-------------------------------------------------------------------------
 */
int dml_wal_inherited_main(void);
#define main dml_wal_inherited_main
#include "test_cluster_heap_insert_version.c"
#undef main

static bool chain_heap, chain_held[4], chain_cancel, chain_missing;
static bool chain_rotate, chain_capture_error;
static unsigned chain_warms, chain_captures, chain_resumes, chain_attempts;
static unsigned chain_refusals, chain_resume_refusals, chain_initial_wal;
static uint64 chain_revision[4], chain_initial_token;

static void
chain_unpublished(void)
{
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(inserts, chain_initial_wal);
	UT_ASSERT_EQ(next_token, chain_initial_token);
}

static void
chain_unlock(Buffer buffer, int mode)
{
	UT_ASSERT((buffer == 2 || buffer == 4) && chain_held[buffer - 1]);
	UT_ASSERT_EQ(mode, BUFFER_LOCK_UNLOCK);
	UT_ASSERT(multi_vm_pins[buffer - 1]);
	chain_held[buffer - 1] = false;
	multi_vm_locked = chain_held[1] || chain_held[3];
}

static bool
chain_aux(Buffer *buffer, Buffer *alias, const ClusterBufferBarrierSiteId *site,
		  ResourceXAuxiliaryAcquireContext *context)
{
	BufferTag tag;

	chain_unpublished();
	if (chain_heap && cluster_shared_config)
		elog(ERROR, "ordinary VM acquisition below heap content");
	if (cluster_shared_config)
		UT_ASSERT(!chain_held[1] && !chain_held[3]);
	else
		UT_ASSERT(!chain_held[*buffer - 1]);
	UT_ASSERT(alias == NULL && site == NULL);
	UT_ASSERT((*buffer == 2 || *buffer == 4) && multi_vm_pins[*buffer - 1]);
	InitBufferTag(&tag, &tags[*buffer - 1], forks[*buffer - 1], blocks[*buffer - 1]);
	if (context->active) {
		UT_ASSERT(BufferTagsEqual(&tag, &context->resource));
		UT_ASSERT_EQ(context->absolute_deadline_us, 4242);
	} else {
		context->active = context->reobserve = true;
		context->resource = tag;
		context->absolute_deadline_us = 4242;
	}
	chain_warms++;
	if (chain_refusals) {
		chain_refusals--;
		multi_vm_pins[*buffer - 1] = false;
		*buffer = InvalidBuffer;
		return false;
	}
	if (chain_rotate && *buffer == 4) {
		chain_revision[1]++;
		chain_rotate = false;
	}
	chain_held[*buffer - 1] = multi_vm_locked = true;
	memset(context, 0, sizeof(*context));
	return true;
}

static bool
chain_capture(Buffer buffer, ResourceXWriterUseContext *writer,
			  uint64 *revision, bool *checked)
{
	chain_unpublished();
	UT_ASSERT(!chain_heap && chain_held[buffer - 1]);
	chain_captures++;
	if (chain_capture_error)
		elog(ERROR, "controlled DML VM capture error");
	if (chain_missing)
		return false;
	memset(writer, 0, sizeof(*writer));
	writer->writer_activation_token = 80 + buffer;
	writer->r4_record_generation = 7;
	*revision = chain_revision[buffer - 1];
	*checked = true;
	return true;
}

static bool
chain_resume(Buffer buffer, const ResourceXWriterUseContext *writer,
			 uint64 revision, bool checked)
{
	chain_unpublished();
	UT_ASSERT(chain_heap && !chain_held[buffer - 1] && multi_vm_pins[buffer - 1]);
	chain_resumes++;
	if (chain_resume_refusals) {
		chain_resume_refusals--;
		return false;
	}
	if (!checked || revision != chain_revision[buffer - 1]
		|| writer->writer_activation_token != (uint64)(80 + buffer)
		|| writer->r4_record_generation != 7)
		return false;
	chain_held[buffer - 1] = multi_vm_locked = true;
	return true;
}

/* Recompile the exact same helpers against this scheduling/ownership ledger.
 * The inherited publication fixture's helpers retain their original leaves. */
#define ClusterHeapVmMutationState ChainVmState
#define cluster_heap_vm_mutation_lock chain_vm_lock
#define cluster_heap_vm_mutation_prepare chain_vm_prepare
#define ClusterLockBufferExclusiveAuxiliaryAliasAware chain_aux
#define ClusterBufferCaptureMutationContext chain_capture
#define ClusterConditionalLockBufferWithMutationContext chain_resume
#define LockBuffer chain_unlock
#include "test_cluster_heap_insert_vm_authority.inc"
#undef LockBuffer
#undef ClusterConditionalLockBufferWithMutationContext
#undef ClusterBufferCaptureMutationContext
#undef ClusterLockBufferExclusiveAuxiliaryAliasAware
#undef cluster_heap_vm_mutation_prepare
#undef cluster_heap_vm_mutation_lock
#undef ClusterHeapVmMutationState

enum ChainOperation { CHAIN_DELETE, CHAIN_TEMP, CHAIN_UPDATE };

/* Original caller retries/target and receipt admission are controlled here.
 * Each unique map needs its original writer; final capture runs only once all
 * of them have resumed. No state from an abandoned attempt is applied. */
static void
execute_chain(enum ChainOperation operation, bool versioned, bool crosspage,
			  int vm_mode, bool hot, bool recomposed)
{
	Buffer buffer = 1, newbuf = crosspage ? 3 : 1;
	Buffer vmbuffer = vm_mode == 0 || vm_mode == 3 ? InvalidBuffer : 2;
	Buffer vmbuffer_new = vm_mode >= 2 ? 4 : vmbuffer;
	bool vm_locked = false, vm_locked_new = false;
	bool cluster_page_versioned = versioned;
	ClusterSpaceIdentity cluster_page_identity = identity;
	RfPageProducerBatchV1 cluster_page_versions = { 0 }, cluster_temp_lock_versions = { 0 };
	ChainVmState states[2] = { 0 };
	ResourceXAuxiliaryAcquireContext contexts[2] = { 0 };
	Buffer maps[2] = {vmbuffer, operation == CHAIN_UPDATE && vm_mode >= 2
		? vmbuffer_new : InvalidBuffer};

	chain_initial_wal = inserts;
	chain_initial_token = next_token;
	for (;;) {
		bool ready = true;

		UT_ASSERT(++chain_attempts <= 32); /* Controlled driver, no product deadline. */
		chain_heap = multi_heap_locked = true;
		for (int i = 0; i < 2; i++) {
			if (!BufferIsValid(maps[i]))
				continue;
			multi_vm_pins[maps[i] - 1] = true;
			if (!chain_vm_lock(&maps[i], versioned, &states[i], &contexts[i])) {
				Buffer current = i == 0 ? vmbuffer : vmbuffer_new;

				for (int j = 0; j < 2; j++)
					if (BufferIsValid(maps[j]) && chain_held[maps[j] - 1])
						chain_unlock(maps[j], BUFFER_LOCK_UNLOCK);
				chain_heap = multi_heap_locked = false;
				memset(multi_vm_pins, 0, sizeof(multi_vm_pins));
				multi_vm_pins[current - 1] = true;
				chain_vm_prepare(&current, NULL, &states[i], &contexts[i]);
				memset(multi_vm_pins, 0, sizeof(multi_vm_pins));
				if (chain_cancel)
					InterruptPending = true;
				CHECK_FOR_INTERRUPTS();
				ready = false;
				break;
			}
		}
		if (ready)
			break;
	}
	vm_locked = BufferIsValid(vmbuffer);
	vm_locked_new = operation == CHAIN_UPDATE && vm_mode >= 2;
	chain_unpublished();
	if (operation == CHAIN_DELETE) {
#include "test_cluster_heap_delete_capture.inc"
	} else if (operation == CHAIN_TEMP) {
#include "test_cluster_heap_update_temp_capture.inc"
		cluster_page_versions = cluster_temp_lock_versions;
	} else {
#include "test_cluster_heap_update_capture.inc"
	}
	prepared = cluster_page_versions;
	if (operation == CHAIN_DELETE)
		run_delete(versioned, vm_locked, recomposed);
	else if (operation == CHAIN_TEMP)
		run_temp_lock(versioned);
	else
		run_update(versioned, crosspage, vm_mode, hot);
	for (int i = 0; i < 2; i++)
		if (BufferIsValid(maps[i]) && chain_held[maps[i] - 1])
			chain_unlock(maps[i], BUFFER_LOCK_UNLOCK);
	memset(multi_vm_pins, 0, sizeof(multi_vm_pins));
	chain_heap = multi_heap_locked = false;
}

static bool
try_chain(enum ChainOperation operation, bool versioned, bool crosspage,
		  int vm_mode, bool hot, bool recomposed)
{
	volatile bool done = false;

	PG_TRY();
	{
		execute_chain(operation, versioned, crosspage, vm_mode, hot, recomposed);
		done = true;
	}
	PG_CATCH();
	{
		/* Inspect the actual helper's unwind, not simulated ResourceOwner cleanup. */
	}
	PG_END_TRY();
	return done;
}

static void
prepare_chain(bool versioned, bool crosspage, int vm_mode)
{
	/* The old publication setup captures early and allocates a token. Build
	 * the input directly so only the post-acquisition capture reserves one. */
	reset(versioned, false, false);
	blocks[0] = 0;
	blocks[2] = vm_mode >= 2 ? HEAPBLOCKS_PER_PAGE : 1;
	run_insert(versioned, true, 0);
	want_vm = vm_mode != 0;
	if (want_vm) {
		if (vm_mode != 3)
			PageSetAllVisible(pages[0].data);
		if (crosspage)
			PageSetAllVisible(pages[2].data);
	}
	pages[1].data[SizeOfPageHeaderData] = (char)0xff;
	pages[3].data[SizeOfPageHeaderData] = (char)0xff;
	pages[3].data[BLCKSZ - 1] = 73;
	memset(registered, 0, sizeof(registered));
	memset(dirty, 0, sizeof(dirty));
	memset(&prepared, 0, sizeof(prepared));
	inserts = data_calls = edge_count = edge_token = vm_clears = itl_stamps = 0;
	chain_heap = chain_cancel = chain_missing = chain_rotate = chain_capture_error = false;
	memset(chain_held, 0, sizeof(chain_held));
	chain_warms = chain_captures = chain_resumes = chain_attempts = 0;
	chain_refusals = chain_resume_refusals = 0;
	chain_revision[1] = 31;
	chain_revision[3] = 61;
	/* Actual bitmap clear and WAL regions; acquisition leaves above are controlled. */
	multi_loop_active = true;
}

static void
check_wal(unsigned count, uint64 token)
{
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(edge_count, count);
	UT_ASSERT_EQ(edge_token, token);
	if (inserts != 1 || edge_count != count)
		return;
	for (unsigned i = 0; i < count; i++) {
		Buffer target = registered_buffers[edges[i].block_id];

		UT_ASSERT_EQ(((PageHeader)pages[target - 1].data)->pd_block_scn, token);
		UT_ASSERT_EQ(PageGetLSN(pages[target - 1].data), 200);
		UT_ASSERT_EQ(((PageHeader)images[edges[i].block_id].data)->pd_block_scn, token);
		UT_ASSERT(dirty[target - 1]);
		if (forks[target - 1] == VISIBILITYMAP_FORKNUM)
			UT_ASSERT_EQ(register_flags[edges[i].block_id], REGBUF_FORCE_IMAGE);
	}
	UT_ASSERT(!chain_heap && !chain_held[1] && !chain_held[3]);
	UT_ASSERT(!multi_vm_pins[1] && !multi_vm_pins[3]);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

UT_TEST(delete_acquires_then_captures_and_logs_exact_heap_vm)
{
	for (int old_bit = 0; old_bit < 2; old_bit++) {
		prepare_chain(true, false, 1);
		pages[1].data[SizeOfPageHeaderData] = old_bit ? (char)0xff : (char)0xfc;
		UT_ASSERT(try_chain(CHAIN_DELETE, true, false, 1, false, old_bit == 0));
		check_wal(2, 101);
		UT_ASSERT_EQ(vm_clears, 1);
		UT_ASSERT_EQ((uint8)images[1].data[SizeOfPageHeaderData], 0xfc);
		UT_ASSERT_EQ(edges[0].before.mutation_token, 100);
		UT_ASSERT_EQ(edges[1].before.mutation_token, 31);
		UT_ASSERT_EQ(chain_warms, 1);
		UT_ASSERT_EQ(chain_captures, 1);
	}
}

UT_TEST(temp_lock_clears_only_af_after_original_writer_resume)
{
	for (int old_bit = 0; old_bit < 2; old_bit++) {
		prepare_chain(true, false, 1);
		pages[1].data[SizeOfPageHeaderData] = old_bit ? (char)0xff : (char)0xfd;
		UT_ASSERT(try_chain(CHAIN_TEMP, true, false, 1, false, false));
		check_wal(2, 101);
		UT_ASSERT_EQ((uint8)images[1].data[SizeOfPageHeaderData], 0xfd);
		UT_ASSERT(PageIsAllVisible(pages[0].data));
		UT_ASSERT_EQ(chain_captures, 1);
		UT_ASSERT(((heap_flags & XLH_LOCK_ALL_FROZEN_CLEARED) != 0) == (old_bit != 0));
	}
}

UT_TEST(final_update_captures_shared_vm_once_for_same_or_two_heap_pages)
{
	for (int shape = 0; shape < 3; shape++) {
		for (int old_bit = 0; old_bit < 2; old_bit++) {
			prepare_chain(true, shape == 2, 1);
			pages[1].data[SizeOfPageHeaderData] = old_bit ? (char)0xff
				: shape == 2 ? (char)0xf0 : (char)0xfc;
			UT_ASSERT(try_chain(CHAIN_UPDATE, true, shape == 2, 1, shape == 1, false));
			check_wal(shape == 2 ? 3 : 2, 101);
			UT_ASSERT_EQ(registered_buffers[2], 2);
			UT_ASSERT_EQ((uint8)images[2].data[SizeOfPageHeaderData], shape == 2 ? 0xf0 : 0xfc);
			UT_ASSERT_EQ(chain_captures, 1);
		}
	}
}

UT_TEST(final_update_covers_distinct_maps_and_new_only_block_id_gap)
{
	for (int mode = 1; mode <= 3; mode++) {
		for (int old_bit = 0; old_bit < 2; old_bit++) {
			prepare_chain(true, true, mode);
			/* Old-only still has a distinct heap target, but no new VM obligation. */
			if (mode == 1)
				PageClearAllVisible(pages[2].data);
			pages[1].data[SizeOfPageHeaderData] = old_bit ? (char)0xff : (char)0xfc;
			pages[3].data[SizeOfPageHeaderData] = old_bit ? (char)0xff : (char)0xfc;
			UT_ASSERT(try_chain(CHAIN_UPDATE, true, true, mode, false, false));
			check_wal(mode == 2 ? 4 : 3, 101);
			UT_ASSERT_EQ(chain_captures, mode == 2 ? 2 : 1);
			UT_ASSERT(registered[2] == (mode != 3));
			UT_ASSERT(registered[3] == (mode != 1));
			if (mode != 3) {
				UT_ASSERT_EQ(registered_buffers[2], 2);
				UT_ASSERT_EQ((uint8)images[2].data[SizeOfPageHeaderData], 0xfc);
				UT_ASSERT_EQ(images[2].data[BLCKSZ - 1], 42);
			}
			if (mode != 1) {
				UT_ASSERT_EQ(registered_buffers[3], 4);
				UT_ASSERT_EQ((uint8)images[3].data[SizeOfPageHeaderData], 0xfc);
				UT_ASSERT_EQ(images[3].data[BLCKSZ - 1], 73);
			}
		}
	}
}

UT_TEST(final_update_recaptures_actual_temp_lock_result)
{
	prepare_chain(true, false, 1);
	UT_ASSERT(try_chain(CHAIN_TEMP, true, false, 1, false, false));
	chain_attempts = 0;
	UT_ASSERT(try_chain(CHAIN_UPDATE, true, false, 1, true, false));
	UT_ASSERT_EQ(inserts, 2);
	UT_ASSERT_EQ(edge_token, 102);
	UT_ASSERT_EQ(edges[0].before.mutation_token, 101);
	UT_ASSERT_EQ(edges[1].before.mutation_token, 101);
	UT_ASSERT_EQ((uint8)images[2].data[SizeOfPageHeaderData], 0xfc);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 102);
}

UT_TEST(refusals_and_other_map_rotation_do_not_publish_abandoned_versions)
{
	for (int operation = CHAIN_DELETE; operation <= CHAIN_UPDATE; operation++) {
		prepare_chain(true, operation == CHAIN_UPDATE, operation == CHAIN_UPDATE ? 2 : 1);
		chain_refusals = chain_resume_refusals = 3;
		chain_rotate = operation == CHAIN_UPDATE;
		UT_ASSERT(try_chain(operation, true, operation == CHAIN_UPDATE,
			operation == CHAIN_UPDATE ? 2 : 1, false, false));
		check_wal(operation == CHAIN_UPDATE ? 4 : 2, 101);
		UT_ASSERT(chain_attempts > 4 && chain_captures > 3);
	}
}

UT_TEST(unknown_writer_capture_error_and_bad_space_do_not_publish)
{
	for (int operation = CHAIN_DELETE; operation <= CHAIN_UPDATE; operation++) {
		for (int fault = 0; fault < 3; fault++) {
			PGAlignedBlock before[4];

			prepare_chain(true, operation == CHAIN_UPDATE, operation == CHAIN_UPDATE ? 2 : 1);
			chain_missing = fault == 0;
			chain_capture_error = fault == 1;
			if (fault == 2)
				identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
			memcpy(before, pages, sizeof(before));
			UT_ASSERT(!try_chain(operation, true, operation == CHAIN_UPDATE,
				operation == CHAIN_UPDATE ? 2 : 1, false, false));
			UT_ASSERT_EQ(inserts + vm_clears + edge_count, 0);
			UT_ASSERT_EQ(next_token, 100);
			UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
			if (fault < 2)
				UT_ASSERT(!chain_held[1] && !chain_held[3]);
		}
	}
}

UT_TEST(cancel_after_prepare_retires_pin_without_clearing_or_wal)
{
	for (int operation = CHAIN_DELETE; operation <= CHAIN_UPDATE; operation++) {
		PGAlignedBlock before[4];

		prepare_chain(true, operation == CHAIN_UPDATE, operation == CHAIN_UPDATE ? 2 : 1);
		chain_cancel = true;
		memcpy(before, pages, sizeof(before));
		UT_ASSERT(!try_chain(operation, true, operation == CHAIN_UPDATE,
			operation == CHAIN_UPDATE ? 2 : 1, false, false));
		UT_ASSERT_EQ(inserts + vm_clears + edge_count, 0);
		UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
		UT_ASSERT(!chain_heap && !chain_held[1] && !chain_held[3]);
		UT_ASSERT(!multi_vm_pins[1] && !multi_vm_pins[3]);
		UT_ASSERT(!InterruptPending);
	}
}

UT_TEST(no_visible_page_needs_no_vm_writer_or_wal_component)
{
	for (int operation = CHAIN_DELETE; operation <= CHAIN_UPDATE; operation++) {
		prepare_chain(true, operation == CHAIN_UPDATE, 0);
		UT_ASSERT(try_chain(operation, true, operation == CHAIN_UPDATE, 0, false, false));
		check_wal(operation == CHAIN_UPDATE ? 2 : 1, 101);
		UT_ASSERT_EQ(chain_warms + chain_captures + chain_resumes + vm_clears, 0);
		UT_ASSERT(!registered[2] && !registered[3]);
	}
}

UT_TEST(nonshared_owners_keep_implicit_vm_and_native_wal_shape)
{
	for (int operation = CHAIN_DELETE; operation <= CHAIN_UPDATE; operation++) {
		prepare_chain(false, operation == CHAIN_UPDATE, operation == CHAIN_UPDATE ? 2 : 1);
		UT_ASSERT(try_chain(operation, false, operation == CHAIN_UPDATE,
			operation == CHAIN_UPDATE ? 2 : 1, false, false));
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(edge_count, 0);
		UT_ASSERT_EQ(chain_captures + chain_resumes, 0);
		UT_ASSERT(!registered[2] && !registered[3]);
	}
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(delete_acquires_then_captures_and_logs_exact_heap_vm);
	UT_RUN(temp_lock_clears_only_af_after_original_writer_resume);
	UT_RUN(final_update_captures_shared_vm_once_for_same_or_two_heap_pages);
	UT_RUN(final_update_covers_distinct_maps_and_new_only_block_id_gap);
	UT_RUN(final_update_recaptures_actual_temp_lock_result);
	UT_RUN(refusals_and_other_map_rotation_do_not_publish_abandoned_versions);
	UT_RUN(unknown_writer_capture_error_and_bad_space_do_not_publish);
	UT_RUN(cancel_after_prepare_retires_pin_without_clearing_or_wal);
	UT_RUN(no_visible_page_needs_no_vm_writer_or_wal_component);
	UT_RUN(nonshared_owners_keep_implicit_vm_and_native_wal_shape);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
