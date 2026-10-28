/*-------------------------------------------------------------------------
 *
 * test_cluster_savepoint_origin_worker.inc
 *   Real holder builder plus original stepwise origin and inverse.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_savepoint_origin_worker.inc
 *
 * NOTES
 *   Pgrac-original tests; storage/current/native and diagnostics are doubles.
 *-------------------------------------------------------------------------
 */
#include "utils/memutils.h"

int cluster_cr_chain_walk_max_steps = 4096;
int MyProcPid = 1;
MemoryContext TopMemoryContext = (MemoryContext)1;
static int operation_worker_allocations;

void *
MemoryContextAllocZero(MemoryContext context pg_attribute_unused(), Size size)
{
	operation_worker_allocations++;
	return calloc(1, size);
}

void
pfree(void *pointer)
{
	operation_worker_allocations--;
	free(pointer);
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	UT_ASSERT(false);
	return false;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *function pg_attribute_unused())
{
	abort();
}

int
errcode(int code pg_attribute_unused())
{
	return 0;
}
int
errmsg(const char *format pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *format pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *format pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *format pg_attribute_unused(), ...)
{
	return 0;
}


typedef struct OperationWorkerFixture {
	OperationCrFixture cr;
	ClusterR4CrSlotExtension extension;
	PGAlignedBlock foreign;
	ClusterCrBuildReason reason;
} OperationWorkerFixture;

static void
operation_worker_init(OperationWorkerFixture *f, bool same_segment)
{
	UndoRecordHeader *record;
	UndoItlHistoryTrailer history;
	UndoSlotDirEntry slot;
	char *body;

	memset(f, 0, sizeof(*f));
	operation_cr_init(&f->cr, same_segment, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	test_operation_worker_enabled = true;
	test_operation_release_observer = NULL;
	record = (UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader));
	record->flags |= UNDO_REC_FLAG_FIRST_IN_TX;
	record->prev_uba = (UBA)InvalidUba_init;
	body = (char *)(record + 1);
	memcpy(&history, body + sizeof(UndoInsertPayload), sizeof(history));
	memset(&history.entries[0].prior, 0, sizeof(history.entries[0].prior));
	history.entries[0].prior.wrap = 8;
	memcpy(body + sizeof(UndoInsertPayload), &history, sizeof(history));
	memcpy(&slot, test_operation_page.data + BLCKSZ - sizeof(slot), sizeof(slot));
	slot.flags = record->flags;
	memcpy(test_operation_page.data + BLCKSZ - sizeof(slot), &slot, sizeof(slot));
	f->extension.slot_generation = 1000;
	f->extension.owner.builder_incarnation = 9000;
	f->extension.route_proof.tag = f->cr.tag;
	f->extension.route_proof.read_scn = 69;
	f->extension.route_proof.formation_epoch = f->cr.origin.admission.formation_epoch;
	f->extension.route_proof.activation_generation = f->cr.origin.admission.record_generation;
}

static ClusterR4CrBuildStepResult
operation_worker_step(OperationWorkerFixture *f)
{
	return cluster_cr_build_on_holder_step_admitted(0, 1000, false, &f->extension,
													f->cr.source.data, f->foreign.data,
													&f->cr.origin.admission, &f->reason);
}

UT_TEST(test_operation_worker_real_builder_yields_before_visibility)
{
	OperationWorkerFixture f;
	PGAlignedBlock original;

	operation_worker_init(&f, false);
	memcpy(original.data, f.cr.source.data, BLCKSZ);
	UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
	UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(f.extension.build_steps, 1);
	cluster_cr_build_on_holder_forget(0, 1000);
}

UT_TEST(test_operation_worker_real_origin_completes_private_inverse)
{
	OperationWorkerFixture f;
	ClusterR4CrBuildStepResult step;

	operation_worker_init(&f, true);
	step = operation_worker_step(&f);
	for (int i = 0; i < 24 && step == CLUSTER_R4_CR_STEP_PENDING; i++)
		step = operation_worker_step(&f);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_wait_calls, 0);
	UT_ASSERT_EQ(test_operation_release_wait_count, 0);
	UT_ASSERT(!operation_cr_has_child(f.cr.source.data));
	UT_ASSERT_EQ(ClusterPageGetItlSlots((Page)f.cr.source.data)[2].flags, ITL_FLAG_FREE);
	cluster_cr_build_on_holder_forget(0, 1000);
}

static ClusterR4CrBuildStepResult
operation_worker_finish(OperationWorkerFixture *f)
{
	ClusterR4CrBuildStepResult step = CLUSTER_R4_CR_STEP_PENDING;

	for (int i = 0; i < 64 && step == CLUSTER_R4_CR_STEP_PENDING; i++) {
		int acquisitions = test_candidate_acquire_calls;
		int acquire_polls = test_candidate_poll_calls;
		int releases = test_candidate_release_calls;
		int release_polls = test_operation_release_poll_count;

		step = operation_worker_step(f);
		UT_ASSERT(test_candidate_acquire_calls - acquisitions + test_candidate_poll_calls
					  - acquire_polls + test_candidate_release_calls - releases
					  + test_operation_release_poll_count - release_polls
				  <= 1);
		UT_ASSERT_EQ(test_candidate_wait_calls, 0);
		UT_ASSERT_EQ(test_operation_release_wait_count, 0);
	}
	UT_ASSERT(step != CLUSTER_R4_CR_STEP_PENDING);
	return step;
}

UT_TEST(test_operation_worker_pending_current_is_bounded_and_counts_once)
{
	for (int release = 0; release <= 3; release++) {
		OperationWorkerFixture f;

		operation_worker_init(&f, false);
		if (release)
			test_operation_release_pending_call = release;
		else {
			test_candidate_acquire_step = CLUSTER_UNDO_BLOCK0_CURRENT_PENDING;
			test_candidate_poll_step = CLUSTER_UNDO_BLOCK0_CURRENT_HELD;
		}
		UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
		UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT_EQ(f.extension.build_steps, 1);
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
		cluster_cr_build_on_holder_forget(0, 1000);
	}
}

UT_TEST(test_operation_worker_exact_forget_cancels_every_reader_phase)
{
	for (int stop = 0; stop <= 9; stop++) {
		OperationWorkerFixture f;
		int expected = stop % 3 != 0;

		operation_worker_init(&f, false);
		UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
		for (int i = 0; i < stop; i++)
			UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
		UT_ASSERT_EQ(cluster_cr_build_on_holder_wait_timeout(100), 1);
		UT_ASSERT_EQ(cluster_cr_build_on_holder_wait_timeout(0), 0);
		UT_ASSERT_EQ(cluster_cr_build_on_holder_wait_timeout(-1), -1);
		cluster_cr_build_on_holder_forget(0, 1001);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 0);
		cluster_cr_build_on_holder_forget(0, 1000);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(test_candidate_cancel_calls, expected);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(cluster_cr_build_on_holder_wait_timeout(100), 100);
	}
}

UT_TEST(test_operation_worker_stale_admission_cancels_original_guard)
{
	OperationWorkerFixture f;

	operation_worker_init(&f, false);
	UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
	UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
	UT_ASSERT_EQ(test_candidate_held_count, 1);
	test_admission_revoked = true;
	UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_PROTOCOL);
	UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT(operation_cr_has_child(f.cr.source.data));
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
}

UT_TEST(test_operation_worker_wrong_owner_does_not_cancel_other_key)
{
	OperationWorkerFixture f;
	ClusterR4CrSlotExtension changed;

	operation_worker_init(&f, false);
	UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
	UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
	changed = f.extension;
	changed.owner.builder_incarnation++;
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step_admitted(0, 1000, false, &changed,
														  f.cr.source.data, f.foreign.data,
														  &f.cr.origin.admission, &f.reason),
				 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(test_candidate_cancel_calls, 0);
	UT_ASSERT_EQ(test_candidate_held_count, 1);
	UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FULL);
	cluster_cr_build_on_holder_forget(0, 1000);
}

UT_TEST(test_operation_worker_changed_route_cancels_exact_key)
{
	OperationWorkerFixture f;

	operation_worker_init(&f, false);
	UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
	UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
	f.extension.route_proof.tag.blockNum++;
	UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	cluster_cr_build_on_holder_forget(0, 1000);
}

UT_TEST(test_operation_worker_changed_private_source_never_publishes_full)
{
	OperationWorkerFixture f;
	PGAlignedBlock changed;

	operation_worker_init(&f, false);
	UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
	f.cr.source.data[BLCKSZ - 1] ^= 1;
	memcpy(changed.data, f.cr.source.data, BLCKSZ);
	UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(memcmp(f.cr.source.data, changed.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	cluster_cr_build_on_holder_forget(0, 1000);
}

UT_TEST(test_operation_worker_unknown_prepared_and_cold_refuse)
{
	for (int fault = 0; fault < 4; fault++) {
		OperationWorkerFixture f;
		PGAlignedBlock original;

		operation_worker_init(&f, false);
		if (fault == 0)
			test_tt_slot.xid++;
		else if (fault == 1) {
			test_tt_slot.status = TT_SLOT_ACTIVE;
			test_tt_slot.commit_scn = InvalidScn;
			test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
			test_twophase_prepared = true;
			test_twophase_xid = TEST_ORIGIN_XID;
			test_current_owner_available = true;
		} else
			test_candidate_cold_copy_mask = fault == 2 ? 1 : 2;
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
		UT_ASSERT_EQ(memcmp(f.cr.source.data, original.data, BLCKSZ), 0);
		UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		cluster_cr_build_on_holder_forget(0, 1000);
	}
}

/* Two exact records in the original resident DATA fixture. The second inverse
 * must reselect the restored write-SCN 60 head below read-SCN 69. */
static void
operation_worker_two_records(OperationWorkerFixture *f)
{
	UndoBlockHeader *block = (UndoBlockHeader *)test_operation_page.data;
	UndoSlotDirEntry directory;
	UndoRecordHeader *older;
	UndoRecordHeader newer;
	char *newer_bytes;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
	ClusterItlSlotData *head = &ClusterPageGetItlSlots((Page)f->cr.source.data)[2];
	HeapTupleHeader tuple;
	char *body;

	memcpy(&directory, test_operation_page.data + BLCKSZ - sizeof(directory), sizeof(directory));
	older = (UndoRecordHeader *)(test_operation_page.data + directory.record_offset);
	newer_bytes = (char *)older + directory.record_length;
	memcpy(newer_bytes, older, directory.record_length);
	memcpy(&newer, newer_bytes, sizeof(newer));
	newer.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
	newer.prev_uba = test_origin_locator.uba;
	memcpy(newer_bytes, &newer, sizeof(newer));
	body = newer_bytes + sizeof(newer);
	memcpy(&history, body + sizeof(UndoInsertPayload), sizeof(history));
	history.entries[0].prior = *head;
	history.entries[0].prior.flags = ITL_FLAG_ACTIVE;
	history.entries[0].prior.commit_scn = InvalidScn;
	history.entries[0].prior.write_scn = 60;
	memcpy(body + sizeof(UndoInsertPayload), &history, sizeof(history));
	directory.record_offset += directory.record_length;
	directory.flags = newer.flags;
	memcpy(test_operation_page.data + BLCKSZ - 2 * sizeof(directory), &directory,
		   sizeof(directory));
	block->slot_count = 2;
	block->free_offset = directory.record_offset + directory.record_length;
	older->target_offset = 2;
	older->write_scn = 60;
	body = (char *)(older + 1);
	memcpy(&history, body + sizeof(UndoInsertPayload), sizeof(history));
	history.entries[0].after_write_scn = 60;
	memcpy(body + sizeof(UndoInsertPayload), &history, sizeof(history));
	memcpy(&operation, body + older->payload_length - sizeof(operation), sizeof(operation));
	operation.operation_sequence--;
	memcpy(body + older->payload_length - sizeof(operation), &operation, sizeof(operation));
	head->undo_segment_head = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, 1);
	tuple = (HeapTupleHeader)PageGetItem((Page)f->cr.source.data,
										 PageGetItemId((Page)f->cr.source.data, 2));
	HeapTupleHeaderSetXmin(tuple, TEST_ORIGIN_XID + 1);
}

UT_TEST(test_operation_worker_reselects_discarded_head_below_read_scn)
{
	OperationWorkerFixture f;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(f.extension.build_steps, 2);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	UT_ASSERT_EQ(test_candidate_release_calls, 6);
	UT_ASSERT(ItemIdIsNormal(PageGetItemId((Page)f.cr.source.data, 1)));
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)f.cr.source.data, 2)));
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)f.cr.source.data, 3)));
	UT_ASSERT_EQ(ClusterPageGetItlSlots((Page)f.cr.source.data)[2].flags, ITL_FLAG_FREE);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(cluster_cr_build_on_holder_wait_timeout(100), 100);
}

UT_TEST(test_operation_worker_sequence_regression_and_cycle_refuse)
{
	for (int fault = 0; fault < 3; fault++) {
		OperationWorkerFixture f;
		UndoRecordHeader *record;
		UndoOperationTrailer tail;
		UndoItlHistoryTrailer history;
		char *body;

		operation_worker_init(&f, false);
		operation_worker_two_records(&f);
		record = (UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader));
		body = (char *)(record + 1);
		if (fault < 2) {
			memcpy(&tail, body + record->payload_length - sizeof(tail), sizeof(tail));
			if (fault == 0)
				tail.operation_sequence = 12;
			else
				tail.canonical_top_fxid += UINT64CONST(1) << 32;
			memcpy(body + record->payload_length - sizeof(tail), &tail, sizeof(tail));
		} else {
			memcpy(&history, body + sizeof(UndoInsertPayload), sizeof(history));
			history.entries[0].prior = ClusterPageGetItlSlots((Page)f.cr.source.data)[2];
			memcpy(body + sizeof(UndoInsertPayload), &history, sizeof(history));
		}
		UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		cluster_cr_build_on_holder_forget(0, 1000);
	}
}

UT_TEST(test_operation_worker_chain_limit_never_counts_pending_ticks)
{
	OperationWorkerFixture f;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	cluster_cr_chain_walk_max_steps = 1;
	UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_CHAIN_LIMIT);
	UT_ASSERT_EQ(f.extension.build_steps, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	cluster_cr_build_on_holder_forget(0, 1000);
	cluster_cr_chain_walk_max_steps = 4096;
}

UT_TEST(test_operation_worker_unadmitted_operation_has_no_authority)
{
	OperationWorkerFixture f;

	operation_worker_init(&f, false);
	UT_ASSERT_EQ(cluster_cr_build_on_holder_step(0, 1000, false, &f.extension, f.cr.source.data,
												 f.foreign.data, &f.reason),
				 CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 0);
	UT_ASSERT(operation_cr_has_child(f.cr.source.data));
	cluster_cr_build_on_holder_forget(0, 1000);
}

UT_TEST(test_operation_worker_active_sample_survives_later_commit)
{
	OperationWorkerFixture f;
	UndoRecordHeader *older;
	char *tail;
	ClusterR4CrBuildStepResult step;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	older = (UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader));
	tail = (char *)(older + 1) + older->payload_length - sizeof(UndoOperationTrailer);
	tail[offsetof(UndoOperationTrailer, effect_state)] = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_current_owner_available = true;
	step = operation_worker_step(&f);
	for (int i = 0; i < 16 && operation_cr_has_child(f.cr.source.data); i++) {
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_PENDING);
		step = operation_worker_step(&f);
	}
	UT_ASSERT(!operation_cr_has_child(f.cr.source.data));
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_PENDING);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	/* Deliberately challenge the visibility branch with a visible terminal
     * sample. These native/SCN boundaries are a model, not a commit race run. */
	test_tt_slot.status = TT_SLOT_COMMITTED;
	test_tt_slot.commit_scn = 65;
	test_native_status = TRANSACTION_STATUS_COMMITTED;
	UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)f.cr.source.data, 2)));
	cluster_cr_build_on_holder_forget(0, 1000);
}

/* Stop after the actual newer inverse, before the next TT observation. */
static void
operation_worker_first_inverse(OperationWorkerFixture *f)
{
	for (int tick = 0; tick < 32 && operation_cr_has_child(f->cr.source.data); tick++)
		UT_ASSERT_EQ(operation_worker_step(f), CLUSTER_R4_CR_STEP_PENDING);
	UT_ASSERT(!operation_cr_has_child(f->cr.source.data));
	UT_ASSERT(ItemIdIsNormal(PageGetItemId((Page)f->cr.source.data, 2)));
	UT_ASSERT_EQ(f->extension.build_steps, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_changed_commit_scn_refuses_older_inverse)
{
	OperationWorkerFixture f;
	PGAlignedBlock remaining;
	ClusterR4CrBuildStepResult step;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	operation_worker_first_inverse(&f);
	memcpy(remaining.data, f.cr.source.data, BLCKSZ);
	test_tt_slot.commit_scn++;
	step = operation_worker_finish(&f);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(memcmp(remaining.data, f.cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_committed_regression_refuses_older_inverse)
{
	for (int aborted = 0; aborted < 2; aborted++) {
		OperationWorkerFixture f;
		PGAlignedBlock remaining;
		ClusterR4CrBuildStepResult step;

		operation_worker_init(&f, false);
		operation_worker_two_records(&f);
		operation_worker_first_inverse(&f);
		memcpy(remaining.data, f.cr.source.data, BLCKSZ);
		test_tt_slot.status = aborted ? TT_SLOT_ABORTED : TT_SLOT_ACTIVE;
		test_tt_slot.commit_scn = InvalidScn;
		test_native_status = aborted ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_IN_PROGRESS;
		test_twophase_xid = TEST_ORIGIN_XID;
		test_current_owner_available = true;
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
		UT_ASSERT_EQ(memcmp(remaining.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_worker_aborted_to_commit_refuses_older_inverse)
{
	OperationWorkerFixture f;
	PGAlignedBlock remaining;
	ClusterR4CrBuildStepResult step;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_ABORTED;
	operation_worker_first_inverse(&f);
	memcpy(remaining.data, f.cr.source.data, BLCKSZ);
	test_tt_slot.status = TT_SLOT_COMMITTED;
	test_tt_slot.commit_scn = 80;
	test_native_status = TRANSACTION_STATUS_COMMITTED;
	step = operation_worker_finish(&f);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(memcmp(remaining.data, f.cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_stable_abort_allows_older_inverse)
{
	OperationWorkerFixture f;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_ABORTED;
	UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)f.cr.source.data, 2)));
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)f.cr.source.data, 3)));
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	cluster_cr_build_on_holder_forget(0, 1000);
}

UT_TEST(test_operation_worker_active_to_abort_remains_conservative)
{
	OperationWorkerFixture f;
	UndoRecordHeader *older;
	char *tail;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	older = (UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader));
	tail = (char *)(older + 1) + older->payload_length - sizeof(UndoOperationTrailer);
	tail[offsetof(UndoOperationTrailer, effect_state)] = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_current_owner_available = true;
	operation_worker_first_inverse(&f);
	test_tt_slot.status = TT_SLOT_ABORTED;
	test_native_status = TRANSACTION_STATUS_ABORTED;
	UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)f.cr.source.data, 2)));
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	cluster_cr_build_on_holder_forget(0, 1000);
}

static int operation_worker_error_release;

static void
operation_worker_error(void)
{
	if (test_candidate_release_calls == operation_worker_error_release)
		pg_re_throw();
}

UT_TEST(test_operation_worker_error_then_original_forget_is_idempotent)
{
	for (int phase = 1; phase <= 3; phase++) {
		OperationWorkerFixture f;
		volatile bool caught = false;

		operation_worker_init(&f, false);
		operation_worker_error_release = phase;
		test_operation_release_observer = operation_worker_error;
		PG_TRY();
		{
			(void)operation_worker_finish(&f);
		}
		PG_CATCH();
		{
			caught = true;
			cluster_cr_build_on_holder_forget(0, 1000);
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(operation_cr_has_child(f.cr.source.data));
		UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
		UT_ASSERT_EQ(cluster_cr_build_on_holder_wait_timeout(100), 100);
	}
}

UT_TEST(test_operation_worker_single_surviving_live_preserves_source)
{
	OperationWorkerFixture f;
	PGAlignedBlock original;

	operation_worker_init(&f, false);
	/* Isolate the surviving-LIVE branch; the SCN/native state is synthetic. */
	operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.commit_scn = 65;
	memcpy(original.data, f.cr.source.data, BLCKSZ);
	UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
	cluster_cr_build_on_holder_forget(0, 1000);
}

static ClusterR4CrBuildStepResult
operation_worker_step_at(OperationWorkerFixture *f, uint32 index)
{
	return cluster_cr_build_on_holder_step_admitted(
		index, f->extension.slot_generation, false, &f->extension, f->cr.source.data,
		f->foreign.data, &f->cr.origin.admission, &f->reason);
}

UT_TEST(test_operation_worker_same_segment_cohort_waits_and_cancel_hands_off)
{
	for (int cancel = 0; cancel < 2; cancel++) {
		OperationWorkerFixture f[2];
		ClusterR4CrBuildStepResult step[2];

		operation_worker_init(&f[0], false);
		f[1] = f[0];
		f[1].extension.slot_generation++;
		f[1].extension.owner.builder_incarnation++;
		test_operation_worker_current_busy = true;
		for (int index = 0; index < 2; index++)
			UT_ASSERT_EQ(operation_worker_step_at(&f[index], index), CLUSTER_R4_CR_STEP_PENDING);
		step[0] = operation_worker_step_at(&f[0], 0);
		step[1] = operation_worker_step_at(&f[1], 1);
		UT_ASSERT_EQ(step[0], CLUSTER_R4_CR_STEP_PENDING);
		UT_ASSERT_EQ(step[1], CLUSTER_R4_CR_STEP_PENDING);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 1);
		if (cancel) {
			cluster_cr_build_on_holder_forget(0, 1000);
			step[0] = CLUSTER_R4_CR_STEP_FAIL; /* do not poll the cancelled construction */
		}
		for (int tick = 0; tick < 40; tick++) {
			for (int index = 0; index < 2; index++)
				if (step[index] == CLUSTER_R4_CR_STEP_PENDING)
					step[index] = operation_worker_step_at(&f[index], index);
		}
		UT_ASSERT_EQ(step[1], CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT(!operation_cr_has_child(f[1].cr.source.data));
		if (!cancel) {
			UT_ASSERT_EQ(step[0], CLUSTER_R4_CR_STEP_FULL);
			UT_ASSERT(!operation_cr_has_child(f[0].cr.source.data));
		}
		UT_ASSERT_EQ(test_candidate_max_held_count, 1);
		UT_ASSERT_EQ(test_candidate_wait_calls, 0);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		for (int index = 0; index < 2; index++)
			cluster_cr_build_on_holder_forget(index, 1000 + index);
	}
}

static void
operation_worker_row_effect(unsigned row, uint8 effect)
{
	UndoSlotDirEntry directory;

	memcpy(&directory, test_operation_page.data + BLCKSZ - (row + 1) * sizeof(directory),
		   sizeof(directory));
	test_operation_page
		.data[directory.record_offset + directory.record_length - sizeof(UndoOperationTrailer)
			  + offsetof(UndoOperationTrailer, effect_state)]
		= effect;
}

/* The next record is selected, but has not acquired current DATA yet. */
static void
operation_worker_after_live_head(OperationWorkerFixture *f)
{
	for (int tick = 0; tick < 32 && f->extension.build_steps < 2; tick++) {
		ClusterR4CrBuildStepResult step = operation_worker_step(f);

		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_PENDING);
		if (step != CLUSTER_R4_CR_STEP_PENDING)
			return;
	}
	UT_ASSERT_EQ(f->extension.build_steps, 2);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

static uint16
operation_worker_modify_body(OperationWorkerFixture *f, uint8 kind, char *body)
{
	Page page = (Page)f->cr.source.data;
	HeapTupleHeader tuple = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 2));
	HeapTupleHeaderData before;
	uint16 length;

	memcpy(&before, tuple, SizeofHeapTupleHeader);
	HeapTupleHeaderSetXmin(&before, TEST_ORIGIN_XID - 5);
	HeapTupleHeaderSetXmax(&before, InvalidTransactionId);
	before.t_infomask = HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tuple, TEST_ORIGIN_XID - 5);
	HeapTupleHeaderSetXmax(tuple, TEST_ORIGIN_XID + 1);
	tuple->t_infomask &= ~HEAP_XMAX_INVALID;
	if (kind == UNDO_RECORD_ITL) {
		UndoItlPayload lock = { .itl_slot_idx = 2,
								.prev_flags = ITL_FLAG_ACTIVE,
								.new_flags = ITL_FLAG_LOCK_ONLY_ACTIVE,
								.lock_xid = TEST_ORIGIN_XID + 1,
								.prev_xmax = InvalidTransactionId,
								.prev_infomask = HEAP_XMAX_INVALID };

		tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
		memcpy(body, &lock, sizeof(lock));
		return sizeof(lock);
	}
	if (kind == UNDO_RECORD_UPDATE) {
		UndoUpdatePayload update = { .new_block = 23,
									 .new_offset = 1,
									 .old_tuple_length = 60,
									 .old_tuple_offset = sizeof(update) };
		HeapTupleHeader replacement = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1));

		memcpy(body, &update, sizeof(update));
		length = sizeof(update);
		HeapTupleHeaderSetXmin(replacement, TEST_ORIGIN_XID + 1);
		ItemPointerSet(&tuple->t_ctid, 23, 1);
		tuple->t_infomask2 |= HEAP_HOT_UPDATED;
	} else {
		UndoDeletePayload deleted
			= { .full_tuple_length = 60, .full_tuple_offset = sizeof(deleted) };

		memcpy(body, &deleted, sizeof(deleted));
		length = sizeof(deleted);
	}
	memcpy(body + length, tuple, 60);
	memcpy(body + length, &before, SizeofHeapTupleHeader);
	return length + 60;
}

/* Repack the two resident records without typed writes to unaligned bytes. */
static void
operation_worker_older_modify(OperationWorkerFixture *f, uint8 kind)
{
	PGAlignedBlock newer;
	UndoRecordHeader record;
	UndoItlHistoryTrailer history, newer_history;
	UndoOperationTrailer operation;
	UndoSlotDirEntry older_slot, newer_slot;
	UndoBlockHeader *block = (UndoBlockHeader *)test_operation_page.data;
	char body[256] = { 0 };
	char *older;
	uint16 body_length;

	memcpy(&older_slot, test_operation_page.data + BLCKSZ - sizeof(older_slot), sizeof(older_slot));
	memcpy(&newer_slot, test_operation_page.data + BLCKSZ - 2 * sizeof(newer_slot),
		   sizeof(newer_slot));
	older = test_operation_page.data + older_slot.record_offset;
	memcpy(&record, older, sizeof(record));
	memcpy(&history, older + sizeof(record) + sizeof(UndoInsertPayload), sizeof(history));
	memcpy(&operation, older + older_slot.record_length - sizeof(operation), sizeof(operation));
	memcpy(newer.data, test_operation_page.data + newer_slot.record_offset,
		   newer_slot.record_length);
	body_length = operation_worker_modify_body(f, kind, body);
	if (kind == UNDO_RECORD_ITL) {
		history.entries[0].after_kind = ITL_FLAG_LOCK_ONLY_ACTIVE;
		history.entries[0].prior.wrap = 7;
		memcpy(&newer_history, newer.data + sizeof(record) + sizeof(UndoInsertPayload),
			   sizeof(newer_history));
		newer_history.entries[0].prior.flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
		newer_history.entries[0].prior.wrap = 7;
		memcpy(newer.data + sizeof(record) + sizeof(UndoInsertPayload), &newer_history,
			   sizeof(newer_history));
	}
	record.record_type = kind;
	older_slot.record_type = kind;
	record.payload_length = body_length + sizeof(history) + sizeof(operation);
	memcpy(older, &record, sizeof(record));
	memcpy(older + sizeof(record), body, body_length);
	memcpy(older + sizeof(record) + body_length, &history, sizeof(history));
	memcpy(older + sizeof(record) + body_length + sizeof(history), &operation, sizeof(operation));
	older_slot.record_length = sizeof(record) + record.payload_length;
	newer_slot.record_offset = older_slot.record_offset + older_slot.record_length;
	memcpy(test_operation_page.data + newer_slot.record_offset, newer.data,
		   newer_slot.record_length);
	memcpy(test_operation_page.data + BLCKSZ - sizeof(older_slot), &older_slot, sizeof(older_slot));
	memcpy(test_operation_page.data + BLCKSZ - 2 * sizeof(newer_slot), &newer_slot,
		   sizeof(newer_slot));
	block->free_offset = newer_slot.record_offset + newer_slot.record_length;
}

UT_TEST(test_operation_worker_live_head_keeps_result_while_older_insert_is_discarded)
{
	OperationWorkerFixture f;
	ClusterItlSlotData kept;
	char tuple[60];
	ClusterR4CrBuildStepResult step;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.commit_scn = 65; /* Model the visibility branch, not a SQL commit. */
	kept = ClusterPageGetItlSlots((Page)f.cr.source.data)[2];
	memcpy(tuple, PageGetItem((Page)f.cr.source.data, PageGetItemId((Page)f.cr.source.data, 3)),
		   sizeof(tuple));
	step = operation_worker_finish(&f);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)f.cr.source.data, 2)));
	UT_ASSERT(operation_cr_has_child(f.cr.source.data));
	UT_ASSERT_EQ(
		memcmp(tuple, PageGetItem((Page)f.cr.source.data, PageGetItemId((Page)f.cr.source.data, 3)),
			   sizeof(tuple)),
		0);
	UT_ASSERT_EQ(memcmp(&kept, &ClusterPageGetItlSlots((Page)f.cr.source.data)[2], sizeof(kept)),
				 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_discarded_head_keeps_older_live_history)
{
	OperationWorkerFixture f;
	ClusterItlSlotData kept;
	ClusterR4CrBuildStepResult step;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	operation_worker_row_effect(0, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.commit_scn = 65;
	operation_worker_first_inverse(&f);
	kept = ClusterPageGetItlSlots((Page)f.cr.source.data)[2];
	step = operation_worker_finish(&f);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT(ItemIdIsNormal(PageGetItemId((Page)f.cr.source.data, 2)));
	UT_ASSERT(!operation_cr_has_child(f.cr.source.data));
	UT_ASSERT_EQ(memcmp(&kept, &ClusterPageGetItlSlots((Page)f.cr.source.data)[2], sizeof(kept)),
				 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
}

UT_TEST(test_operation_worker_live_result_survives_disjoint_update_delete_and_lock)
{
	const uint8 kinds[] = { UNDO_RECORD_UPDATE, UNDO_RECORD_DELETE, UNDO_RECORD_ITL };

	for (unsigned i = 0; i < lengthof(kinds); i++) {
		OperationWorkerFixture f;
		ClusterItlSlotData kept;
		HeapTupleHeader tuple;
		ClusterR4CrBuildStepResult step;

		operation_worker_init(&f, false);
		operation_worker_two_records(&f);
		operation_worker_older_modify(&f, kinds[i]);
		operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		test_tt_slot.commit_scn = 65;
		kept = ClusterPageGetItlSlots((Page)f.cr.source.data)[2];
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT(operation_cr_has_child(f.cr.source.data));
		UT_ASSERT(ItemIdIsNormal(PageGetItemId((Page)f.cr.source.data, 2)));
		tuple = (HeapTupleHeader)PageGetItem((Page)f.cr.source.data,
											 PageGetItemId((Page)f.cr.source.data, 2));
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple), TEST_ORIGIN_XID - 5);
		UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(tuple), InvalidTransactionId);
		UT_ASSERT_EQ(ItemIdIsNormal(PageGetItemId((Page)f.cr.source.data, 1)),
					 kinds[i] != UNDO_RECORD_UPDATE);
		UT_ASSERT_EQ(
			memcmp(&kept, &ClusterPageGetItlSlots((Page)f.cr.source.data)[2], sizeof(kept)), 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_worker_surviving_tid_overlap_refuses_older_projection)
{
	for (int update = 0; update < 2; update++) {
		OperationWorkerFixture f;
		PGAlignedBlock original;
		UndoRecordHeader record;
		char *body;
		ClusterR4CrBuildStepResult step;

		operation_worker_init(&f, false);
		operation_worker_two_records(&f);
		if (update)
			operation_worker_older_modify(&f, UNDO_RECORD_UPDATE);
		memcpy(&record, test_operation_page.data + sizeof(UndoBlockHeader), sizeof(record));
		body = test_operation_page.data + sizeof(UndoBlockHeader) + sizeof(record);
		if (update) {
			UndoUpdatePayload payload;

			memcpy(&payload, body, sizeof(payload));
			payload.new_offset = 3;
			memcpy(body, &payload, sizeof(payload));
		} else {
			record.target_offset = 3;
			memcpy(body - sizeof(record), &record, sizeof(record));
		}
		operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		test_tt_slot.commit_scn = 65;
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
		UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_worker_surviving_result_rejects_unknown_older_top)
{
	OperationWorkerFixture f;
	PGAlignedBlock original;
	ClusterR4CrBuildStepResult step;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.commit_scn = 65;
	memcpy(original.data, f.cr.source.data, BLCKSZ);
	operation_worker_after_live_head(&f);
	test_tt_slot.xid++;
	step = operation_worker_finish(&f);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_surviving_result_mutation_refuses_publication)
{
	OperationWorkerFixture f;
	PGAlignedBlock changed;
	ClusterR4CrBuildStepResult step;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.commit_scn = 65;
	operation_worker_after_live_head(&f);
	f.cr.source.data[BLCKSZ - 1] ^= 1;
	memcpy(changed.data, f.cr.source.data, BLCKSZ);
	step = operation_worker_finish(&f);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(memcmp(changed.data, f.cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_exact_forget_frees_survivor_walk_and_original_guard)
{
	OperationWorkerFixture f;
	int allocations = operation_worker_allocations;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.commit_scn = 65;
	operation_worker_after_live_head(&f);
	UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
	UT_ASSERT_EQ(test_candidate_held_count, 1);
	cluster_cr_build_on_holder_forget(0, 1001);
	UT_ASSERT_EQ(test_candidate_held_count, 1);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_candidate_cancel_calls, 1);
	UT_ASSERT_EQ(operation_worker_allocations, allocations);
	operation_worker_init(&f, false);
	UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FULL);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(operation_worker_allocations, allocations);
}

UT_TEST(test_operation_worker_cohort_keeps_separate_survivor_pages)
{
	OperationWorkerFixture f[2];
	ClusterR4CrBuildStepResult step[2] = { CLUSTER_R4_CR_STEP_PENDING, CLUSTER_R4_CR_STEP_PENDING };
	int allocations = operation_worker_allocations;

	operation_worker_init(&f[0], false);
	operation_worker_two_records(&f[0]);
	operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.commit_scn = 65;
	f[1] = f[0];
	f[1].extension.slot_generation++;
	f[1].extension.owner.builder_incarnation++;
	test_operation_worker_current_busy = true;
	for (int tick = 0; tick < 80; tick++)
		for (unsigned i = 0; i < 2; i++)
			if (step[i] == CLUSTER_R4_CR_STEP_PENDING)
				step[i] = operation_worker_step_at(&f[i], i);
	for (unsigned i = 0; i < 2; i++) {
		cluster_cr_build_on_holder_forget(i, f[i].extension.slot_generation);
		UT_ASSERT_EQ(step[i], CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT(operation_cr_has_child(f[i].cr.source.data));
		UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)f[i].cr.source.data, 2)));
	}
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(operation_worker_allocations, allocations);
}

/* Replace the newer INSERT with a mutation of the older INSERT's tuple.
 * The captured before-image is the exact predecessor exposed by its inverse. */
static void
operation_worker_live_lineage(OperationWorkerFixture *f, uint8 kind, bool hot)
{
	UndoSlotDirEntry directory;
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
	Page page = (Page)f->cr.source.data;
	ClusterItlSlotData *head = &ClusterPageGetItlSlots(page)[2];
	HeapTupleHeader tuple = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 2));
	char body[256] = { 0 };
	char *bytes;
	uint16 length;

	memcpy(&directory, test_operation_page.data + BLCKSZ - 2 * sizeof(directory),
		   sizeof(directory));
	bytes = test_operation_page.data + directory.record_offset;
	memcpy(&record, bytes, sizeof(record));
	memcpy(&history, bytes + sizeof(record) + sizeof(UndoInsertPayload), sizeof(history));
	memcpy(&operation, bytes + directory.record_length - sizeof(operation), sizeof(operation));
	length = operation_worker_modify_body(f, kind, body);
	HeapTupleHeaderSetXmin(tuple, TEST_ORIGIN_XID + 1);
	HeapTupleHeaderSetXmax(tuple, TEST_ORIGIN_XID + 2);
	record.xid = TEST_ORIGIN_XID + 2;
	operation.operation_fxid++;
	if (kind == UNDO_RECORD_ITL) {
		UndoItlPayload lock;

		memcpy(&lock, body, sizeof(lock));
		lock.lock_xid = record.xid;
		memcpy(body, &lock, sizeof(lock));
		history.entries[0].after_kind = ITL_FLAG_LOCK_ONLY_ACTIVE;
		head->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
		head->wrap++;
	} else {
		HeapTupleHeaderData before;
		uint16 prefix
			= kind == UNDO_RECORD_UPDATE ? sizeof(UndoUpdatePayload) : sizeof(UndoDeletePayload);

		memcpy(&before, body + prefix, SizeofHeapTupleHeader);
		HeapTupleHeaderSetXmin(&before, TEST_ORIGIN_XID + 1);
		memcpy(body + prefix, &before, SizeofHeapTupleHeader);
	}
	if (kind == UNDO_RECORD_UPDATE) {
		UndoUpdatePayload update;
		HeapTupleHeader replacement = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 3));
		HeapTupleHeader unrelated = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1));

		memcpy(&update, body, sizeof(update));
		update.new_offset = 3;
		memcpy(body, &update, sizeof(update));
		HeapTupleHeaderSetXmin(replacement, record.xid);
		HeapTupleHeaderSetXmin(unrelated, TEST_ORIGIN_XID - 5);
		ItemPointerSet(&tuple->t_ctid, 23, 3);
		if (hot)
			replacement->t_infomask2 |= HEAP_ONLY_TUPLE;
		else
			tuple->t_infomask2 &= ~HEAP_HOT_UPDATED;
	} else
		ItemIdSetUnused(PageGetItemId(page, 3));
	record.record_type = kind;
	record.target_offset = 2;
	record.payload_length = length + sizeof(history) + sizeof(operation);
	directory.record_type = kind;
	directory.record_length = sizeof(record) + record.payload_length;
	memcpy(bytes, &record, sizeof(record));
	memcpy(bytes + sizeof(record), body, length);
	memcpy(bytes + sizeof(record) + length, &history, sizeof(history));
	memcpy(bytes + sizeof(record) + length + sizeof(history), &operation, sizeof(operation));
	memcpy(test_operation_page.data + BLCKSZ - 2 * sizeof(directory), &directory,
		   sizeof(directory));
	((UndoBlockHeader *)test_operation_page.data)->free_offset
		= directory.record_offset + directory.record_length;
	operation_worker_row_effect(0, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.commit_scn = 65; /* Isolate visibility; not a native commit chronology. */
}

UT_TEST(test_operation_worker_live_insert_update_preserves_same_tid_and_hot_chain)
{
	for (int hot = 0; hot < 2; hot++) {
		OperationWorkerFixture f;
		PGAlignedBlock original;
		ClusterR4CrBuildStepResult step;

		operation_worker_init(&f, false);
		operation_worker_two_records(&f);
		operation_worker_live_lineage(&f, UNDO_RECORD_UPDATE, hot);
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_NONE);
		UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(f.extension.build_steps, 2);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_worker_live_insert_delete_and_lock_keep_same_tid)
{
	const uint8 kinds[] = { UNDO_RECORD_DELETE, UNDO_RECORD_ITL };

	for (unsigned i = 0; i < lengthof(kinds); i++) {
		OperationWorkerFixture f;
		PGAlignedBlock original;
		ClusterR4CrBuildStepResult step;

		operation_worker_init(&f, false);
		operation_worker_two_records(&f);
		operation_worker_live_lineage(&f, kinds[i], false);
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(f.extension.build_steps, 2);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_worker_overlapping_live_missing_creation_is_not_lineage)
{
	OperationWorkerFixture f;
	UndoRecordHeader record;
	PGAlignedBlock original;
	ClusterR4CrBuildStepResult step;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	memcpy(&record, test_operation_page.data + sizeof(UndoBlockHeader), sizeof(record));
	record.target_offset = 3; /* Newer INSERT removes this TID from the private walk. */
	memcpy(test_operation_page.data + sizeof(UndoBlockHeader), &record, sizeof(record));
	operation_worker_row_effect(0, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.commit_scn = 65;
	memcpy(original.data, f.cr.source.data, BLCKSZ);
	step = operation_worker_finish(&f);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_overlapping_live_validates_raw_predecessor_and_length)
{
	for (int fault = 0; fault < 2; fault++) {
		OperationWorkerFixture f;
		UndoRecordHeader record;
		UndoInsertPayload inserted;
		PGAlignedBlock original;
		ClusterR4CrBuildStepResult step;
		char *body = test_operation_page.data + sizeof(UndoBlockHeader) + sizeof(record);

		operation_worker_init(&f, false);
		operation_worker_two_records(&f);
		operation_worker_live_lineage(&f, UNDO_RECORD_UPDATE, true);
		if (fault == 0) {
			UndoOperationTrailer operation;
			char *tail;

			memcpy(&record, body - sizeof(record), sizeof(record));
			record.xid++;
			memcpy(body - sizeof(record), &record, sizeof(record));
			tail = body + record.payload_length - sizeof(operation);
			memcpy(&operation, tail, sizeof(operation));
			operation.operation_fxid++;
			memcpy(tail, &operation, sizeof(operation));
		} else {
			memcpy(&inserted, body, sizeof(inserted));
			inserted.inserted_tuple_len++;
			memcpy(body, &inserted, sizeof(inserted));
		}
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
		UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_worker_discarded_overlap_does_not_erase_live_successor)
{
	OperationWorkerFixture f;
	PGAlignedBlock original;
	ClusterR4CrBuildStepResult step;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	operation_worker_live_lineage(&f, UNDO_RECORD_UPDATE, true);
	operation_worker_row_effect(0, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	memcpy(original.data, f.cr.source.data, BLCKSZ);
	step = operation_worker_finish(&f);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_unqualified_multi_lock_never_publishes_survivor)
{
	for (int discard = 0; discard < 2; discard++) {
		for (int alias = 0; alias < 2; alias++) {
			OperationWorkerFixture f;
			PGAlignedBlock original;
			HeapTupleHeader tuple;
			ClusterR4CrBuildStepResult step;

			operation_worker_init(&f, false);
			operation_worker_two_records(&f);
			operation_worker_live_lineage(&f, UNDO_RECORD_ITL, false);
			tuple = (HeapTupleHeader)PageGetItem((Page)f.cr.source.data,
												 PageGetItemId((Page)f.cr.source.data, 2));
			tuple->t_infomask |= HEAP_XMAX_IS_MULTI;
			if (!alias)
				HeapTupleHeaderSetXmax(tuple, TEST_ORIGIN_XID + 3);
			if (discard) {
				operation_worker_row_effect(0, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
				operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
			}
			memcpy(original.data, f.cr.source.data, BLCKSZ);
			step = operation_worker_finish(&f);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
			UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
			UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
			UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
		}
	}
}

/* Capture a later mutation of the same root after the older child changed it. */
static uint16
operation_worker_superseding_body(OperationWorkerFixture *f, uint8 kind, uint8 prior_kind, bool hot,
								  char *body)
{
	Page page = (Page)f->cr.source.data;
	HeapTupleHeader tuple = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 2));
	uint16 prefix;

	if (kind == UNDO_RECORD_ITL) {
		UndoItlPayload lock = { .itl_slot_idx = 2,
								.prev_flags = prior_kind,
								.new_flags = ITL_FLAG_LOCK_ONLY_ACTIVE,
								.lock_xid = TEST_ORIGIN_XID + 2,
								.prev_xmax = HeapTupleHeaderGetRawXmax(tuple),
								.prev_infomask = tuple->t_infomask };

		memcpy(body, &lock, sizeof(lock));
		tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
		prefix = sizeof(lock);
	} else {
		if (kind == UNDO_RECORD_UPDATE) {
			UndoUpdatePayload update = { .new_block = 23,
										 .new_offset = 3,
										 .old_tuple_length = 60,
										 .old_tuple_offset = sizeof(update) };

			memcpy(body, &update, sizeof(update));
			prefix = sizeof(update);
		} else {
			UndoDeletePayload deleted
				= { .full_tuple_length = 60, .full_tuple_offset = sizeof(deleted) };

			memcpy(body, &deleted, sizeof(deleted));
			prefix = sizeof(deleted);
		}
		memcpy(body + prefix, tuple, 60);
		prefix += 60;
		tuple->t_infomask = 0;
		tuple->t_infomask2 &= ~HEAP_HOT_UPDATED;
		ItemPointerSet(&tuple->t_ctid, 23, kind == UNDO_RECORD_UPDATE ? 3 : 2);
	}
	HeapTupleHeaderSetXmax(tuple, TEST_ORIGIN_XID + 2);
	if (kind == UNDO_RECORD_UPDATE) {
		HeapTupleHeader replacement = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 3));

		HeapTupleHeaderSetXmin(replacement, TEST_ORIGIN_XID + 2);
		if (hot) {
			tuple->t_infomask2 |= HEAP_HOT_UPDATED;
			replacement->t_infomask2 |= HEAP_ONLY_TUPLE;
		}
	} else
		ItemIdSetUnused(PageGetItemId(page, 3));
	return prefix;
}

static void
operation_worker_superseded(OperationWorkerFixture *f, uint8 older_kind, uint8 newer_kind, bool hot)
{
	UndoSlotDirEntry directory;
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
	ClusterItlSlotData *head = &ClusterPageGetItlSlots((Page)f->cr.source.data)[2];
	char body[256] = { 0 };
	char *bytes;
	uint16 length;

	operation_worker_older_modify(f, older_kind);
	memcpy(&directory, test_operation_page.data + BLCKSZ - 2 * sizeof(directory),
		   sizeof(directory));
	bytes = test_operation_page.data + directory.record_offset;
	memcpy(&record, bytes, sizeof(record));
	memcpy(&history, bytes + sizeof(record) + sizeof(UndoInsertPayload), sizeof(history));
	memcpy(&operation, bytes + directory.record_length - sizeof(operation), sizeof(operation));
	length = operation_worker_superseding_body(f, newer_kind, history.entries[0].prior.flags, hot,
											   body);
	history.entries[0].after_kind
		= newer_kind == UNDO_RECORD_ITL ? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_ACTIVE;
	head->flags = newer_kind == UNDO_RECORD_ITL ? ITL_FLAG_LOCK_ONLY_COMMITTED : ITL_FLAG_COMMITTED;
	head->wrap = history.entries[0].prior.wrap
				 + (history.entries[0].prior.flags != history.entries[0].after_kind);
	record.record_type = newer_kind;
	record.target_offset = 2;
	record.xid = TEST_ORIGIN_XID + 2;
	record.payload_length = length + sizeof(history) + sizeof(operation);
	operation.operation_fxid++;
	directory.record_type = newer_kind;
	directory.record_length = sizeof(record) + record.payload_length;
	memcpy(bytes, &record, sizeof(record));
	memcpy(bytes + sizeof(record), body, length);
	memcpy(bytes + sizeof(record) + length, &history, sizeof(history));
	memcpy(bytes + sizeof(record) + length + sizeof(history), &operation, sizeof(operation));
	memcpy(test_operation_page.data + BLCKSZ - 2 * sizeof(directory), &directory,
		   sizeof(directory));
	((UndoBlockHeader *)test_operation_page.data)->free_offset
		= directory.record_offset + directory.record_length;
	operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.commit_scn = 65; /* Controlled visibility branch, not SQL chronology. */
}

static void
operation_worker_assert_superseding_result(OperationWorkerFixture *f, const char *original,
										   uint8 older_kind, uint8 newer_kind)
{
	Page page = (Page)f->cr.source.data;
	Page source = (Page)original;

	UT_ASSERT_EQ(memcmp(PageGetItem(page, PageGetItemId(page, 2)),
						PageGetItem(source, PageGetItemId(source, 2)), 60),
				 0);
	UT_ASSERT_EQ(ItemIdIsNormal(PageGetItemId(page, 1)), older_kind != UNDO_RECORD_UPDATE);
	UT_ASSERT_EQ(ItemIdIsNormal(PageGetItemId(page, 3)), newer_kind == UNDO_RECORD_UPDATE);
	if (newer_kind == UNDO_RECORD_UPDATE)
		UT_ASSERT_EQ(memcmp(PageGetItem(page, PageGetItemId(page, 3)),
							PageGetItem(source, PageGetItemId(source, 3)), 60),
					 0);
	UT_ASSERT_EQ(memcmp(&ClusterPageGetItlSlots(page)[2], &ClusterPageGetItlSlots(source)[2],
						sizeof(ClusterItlSlotData)),
				 0);
	UT_ASSERT_EQ(f->extension.build_steps, 2);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_live_update_preserves_root_over_discarded_update)
{
	for (int hot = 0; hot < 2; hot++) {
		OperationWorkerFixture f;
		PGAlignedBlock original;
		ClusterR4CrBuildStepResult step;

		operation_worker_init(&f, false);
		operation_worker_two_records(&f);
		operation_worker_superseded(&f, UNDO_RECORD_UPDATE, UNDO_RECORD_UPDATE, hot);
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		operation_worker_assert_superseding_result(&f, original.data, UNDO_RECORD_UPDATE,
												   UNDO_RECORD_UPDATE);
	}
}

UT_TEST(test_operation_worker_live_delete_preserves_root_over_discarded_mutations)
{
	const uint8 kinds[] = { UNDO_RECORD_UPDATE, UNDO_RECORD_DELETE, UNDO_RECORD_ITL };

	for (unsigned i = 0; i < lengthof(kinds); i++) {
		OperationWorkerFixture f;
		PGAlignedBlock original;
		ClusterR4CrBuildStepResult step;

		operation_worker_init(&f, false);
		operation_worker_two_records(&f);
		operation_worker_superseded(&f, kinds[i], UNDO_RECORD_DELETE, false);
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		operation_worker_assert_superseding_result(&f, original.data, kinds[i], UNDO_RECORD_DELETE);
	}
}

UT_TEST(test_operation_worker_live_update_covers_discarded_delete_and_lock)
{
	const uint8 kinds[] = { UNDO_RECORD_DELETE, UNDO_RECORD_ITL };

	for (unsigned i = 0; i < lengthof(kinds); i++) {
		OperationWorkerFixture f;
		PGAlignedBlock original;
		ClusterR4CrBuildStepResult step;

		operation_worker_init(&f, false);
		operation_worker_two_records(&f);
		operation_worker_superseded(&f, kinds[i], UNDO_RECORD_UPDATE, true);
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		operation_worker_assert_superseding_result(&f, original.data, kinds[i], UNDO_RECORD_UPDATE);
	}
}

UT_TEST(test_operation_worker_live_root_projection_requires_identical_creation_bytes)
{
	for (int fault = 0; fault < 4; fault++) {
		OperationWorkerFixture f;
		PGAlignedBlock original;
		UndoSlotDirEntry directory;
		HeapTupleHeaderData before;
		char *tuple;
		ClusterR4CrBuildStepResult step;

		operation_worker_init(&f, false);
		operation_worker_two_records(&f);
		operation_worker_superseded(&f, UNDO_RECORD_UPDATE, UNDO_RECORD_UPDATE, true);
		memcpy(&directory, test_operation_page.data + BLCKSZ - 2 * sizeof(directory),
			   sizeof(directory));
		tuple = test_operation_page.data + directory.record_offset + sizeof(UndoRecordHeader)
				+ sizeof(UndoUpdatePayload);
		memcpy(&before, tuple, SizeofHeapTupleHeader);
		if (fault == 0)
			tuple[59] ^= 1;
		else if (fault == 1)
			before.t_hoff++;
		else if (fault == 2)
			before.t_infomask2 ^= 1;
		else
			before.t_infomask ^= HEAP_HASVARWIDTH;
		memcpy(tuple, &before, SizeofHeapTupleHeader);
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
		UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_worker_kept_lock_does_not_grant_full_modify_coverage)
{
	OperationWorkerFixture f;
	PGAlignedBlock original;
	ClusterR4CrBuildStepResult step;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	operation_worker_superseded(&f, UNDO_RECORD_UPDATE, UNDO_RECORD_ITL, false);
	memcpy(original.data, f.cr.source.data, BLCKSZ);
	step = operation_worker_finish(&f);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_root_overlap_never_bypasses_unknown_older_top)
{
	OperationWorkerFixture f;
	PGAlignedBlock original;
	ClusterR4CrBuildStepResult step;

	operation_worker_init(&f, false);
	operation_worker_two_records(&f);
	operation_worker_superseded(&f, UNDO_RECORD_UPDATE, UNDO_RECORD_UPDATE, true);
	memcpy(original.data, f.cr.source.data, BLCKSZ);
	operation_worker_after_live_head(&f);
	test_tt_slot.xid++;
	step = operation_worker_finish(&f);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

/* One physical record addresses independently incarnated old/new page slots. */
static void
operation_worker_cross_page_update(OperationWorkerFixture *f, bool new_page, bool same_segment)
{
	UndoSlotDirEntry directory;
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
	UndoUpdatePayload update = { .new_block = 24,
								 .new_offset = 3,
								 .old_tuple_length = 60,
								 .old_tuple_offset = sizeof(update) };
	Page page;
	HeapTupleHeader tuple;
	char body[sizeof(update) + 60];
	char *bytes;

	operation_worker_init(f, same_segment);
	page = (Page)f->cr.source.data;
	memcpy(&directory, test_operation_page.data + BLCKSZ - sizeof(directory), sizeof(directory));
	bytes = test_operation_page.data + directory.record_offset;
	memcpy(&record, bytes, sizeof(record));
	memcpy(&history, bytes + sizeof(record) + sizeof(UndoInsertPayload), sizeof(history));
	memcpy(&operation, bytes + directory.record_length - sizeof(operation), sizeof(operation));
	memcpy(body, &update, sizeof(update));
	memcpy(body + sizeof(update), PageGetItem(page, PageGetItemId(page, 2)), 60);
	record.record_type = UNDO_RECORD_UPDATE;
	record.target_offset = 2;
	record.payload_length = sizeof(body) + sizeof(history) + sizeof(operation);
	history.count = 2;
	history.entries[1] = history.entries[0];
	history.entries[1].block = 24;
	history.entries[1].slot_index = 4;
	history.entries[1].prior.wrap = 17;
	directory.record_type = UNDO_RECORD_UPDATE;
	directory.record_length = sizeof(record) + record.payload_length;
	memcpy(bytes, &record, sizeof(record));
	memcpy(bytes + sizeof(record), body, sizeof(body));
	memcpy(bytes + sizeof(record) + sizeof(body), &history, sizeof(history));
	memcpy(bytes + sizeof(record) + sizeof(body) + sizeof(history), &operation, sizeof(operation));
	memcpy(test_operation_page.data + BLCKSZ - sizeof(directory), &directory, sizeof(directory));
	((UndoBlockHeader *)test_operation_page.data)->free_offset
		= directory.record_offset + directory.record_length;
	if (new_page) {
		ClusterItlSlotData *slots = ClusterPageGetItlSlots(page);

		slots[4] = slots[2];
		slots[4].wrap = 17;
		memset(&slots[2], 0, sizeof(slots[2]));
		f->cr.tag.blockNum = 24;
		f->extension.route_proof.tag = f->cr.tag;
		for (int offset = 1; offset <= 3; offset++) {
			tuple = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, offset));
			ItemPointerSet(&tuple->t_ctid, 24, offset);
		}
	} else {
		tuple = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 2));
		HeapTupleHeaderSetXmax(tuple, TEST_ORIGIN_XID + 1);
		tuple->t_infomask &= ~HEAP_XMAX_INVALID;
		ItemPointerSet(&tuple->t_ctid, 24, 3);
		ItemIdSetUnused(PageGetItemId(page, 3));
	}
}

static void
operation_worker_assert_cross_inverse(OperationWorkerFixture *f, const char *original,
									  bool new_page, int acquisitions)
{
	Page page = (Page)f->cr.source.data;
	Page source = (Page)original;
	HeapTupleHeader root = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 2));
	ClusterItlSlotData *slot = &ClusterPageGetItlSlots(page)[new_page ? 4 : 2];
	UndoSlotDirEntry directory;
	UndoItlHistoryTrailer history;
	const char *body;

	memcpy(&directory, test_operation_page.data + BLCKSZ - sizeof(directory), sizeof(directory));
	body = test_operation_page.data + directory.record_offset + sizeof(UndoRecordHeader);
	memcpy(&history, body + sizeof(UndoUpdatePayload) + 60, sizeof(history));

	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(root), TEST_ORIGIN_XID - 5);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(root), InvalidTransactionId);
	UT_ASSERT((root->t_infomask & HEAP_XMAX_INVALID) != 0);
	UT_ASSERT_EQ(ItemPointerGetBlockNumber(&root->t_ctid), new_page ? 24 : 23);
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&root->t_ctid), 2);
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId(page, 3)));
	UT_ASSERT_EQ(memcmp(PageGetItem(page, PageGetItemId(page, 1)),
						PageGetItem(source, PageGetItemId(source, 1)), 60),
				 0);
	UT_ASSERT_EQ(slot->flags, ITL_FLAG_FREE);
	UT_ASSERT_EQ(slot->wrap, new_page ? 17 : 8);
	UT_ASSERT_EQ(memcmp(slot, &history.entries[new_page ? 1 : 0].prior, sizeof(*slot)), 0);
	UT_ASSERT_EQ(memcmp(root,
						new_page ? PageGetItem(source, PageGetItemId(source, 2))
								 : body + sizeof(UndoUpdatePayload),
						60),
				 0);
	UT_ASSERT_EQ(f->extension.build_steps, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, acquisitions);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_cross_page_discard_restores_each_exact_role)
{
	for (int new_page = 0; new_page < 2; new_page++) {
		for (int same = 0; same < 2; same++) {
			OperationWorkerFixture f;
			PGAlignedBlock original;
			ClusterR4CrBuildStepResult step;

			operation_worker_cross_page_update(&f, new_page, same);
			memcpy(original.data, f.cr.source.data, BLCKSZ);
			step = operation_worker_finish(&f);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
			UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_NONE);
			operation_worker_assert_cross_inverse(&f, original.data, new_page, 3);
		}
	}
}

UT_TEST(test_operation_worker_cross_page_live_keeps_both_roles)
{
	for (int new_page = 0; new_page < 2; new_page++) {
		OperationWorkerFixture f;
		PGAlignedBlock original;
		ClusterR4CrBuildStepResult step;

		operation_worker_cross_page_update(&f, new_page, false);
		operation_worker_row_effect(0, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		test_tt_slot.commit_scn = 65; /* Isolate visible LIVE, not native chronology. */
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_worker_cross_page_late_live_inverses_both_roles)
{
	for (int new_page = 0; new_page < 2; new_page++) {
		OperationWorkerFixture f;
		PGAlignedBlock original;
		ClusterR4CrBuildStepResult step;

		operation_worker_cross_page_update(&f, new_page, false);
		operation_worker_row_effect(0, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		operation_worker_assert_cross_inverse(&f, original.data, new_page, 3);
	}
}

UT_TEST(test_operation_worker_cross_page_role_and_tuple_mismatches_refuse)
{
	for (int new_page = 0; new_page < 2; new_page++) {
		for (int fault = 0; fault < 6; fault++) {
			OperationWorkerFixture f;
			PGAlignedBlock original;
			UndoItlHistoryTrailer history;
			char *bytes;
			ClusterR4CrBuildStepResult step;

			operation_worker_cross_page_update(&f, new_page, false);
			bytes = test_operation_page.data + sizeof(UndoBlockHeader) + sizeof(UndoRecordHeader)
					+ sizeof(UndoUpdatePayload) + 60;
			memcpy(&history, bytes, sizeof(history));
			if (fault == 0)
				history.entries[new_page].slot_index++;
			else if (fault == 1)
				history.entries[new_page].prior.wrap++;
			else if (fault == 2)
				history.entries[new_page].after_write_scn--;
			else if (fault == 3)
				history.entries[!new_page].block = 25;
			else if (fault == 4)
				test_tt_slot.xid++;
			else {
				HeapTupleHeader tuple = (HeapTupleHeader)PageGetItem(
					(Page)f.cr.source.data,
					PageGetItemId((Page)f.cr.source.data, new_page ? 3 : 2));

				if (new_page)
					HeapTupleHeaderSetXmin(tuple, TEST_ORIGIN_XID + 2);
				else
					HeapTupleHeaderSetXmax(tuple, TEST_ORIGIN_XID + 2);
			}
			memcpy(bytes, &history, sizeof(history));
			memcpy(original.data, f.cr.source.data, BLCKSZ);
			step = operation_worker_finish(&f);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
			UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
			UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
		}
	}
}

UT_TEST(test_operation_worker_cross_page_cohort_keeps_role_proofs_separate)
{
	OperationWorkerFixture f[2];
	PGAlignedBlock original[2];
	ClusterR4CrBuildStepResult step[2] = { CLUSTER_R4_CR_STEP_PENDING, CLUSTER_R4_CR_STEP_PENDING };
	int allocations = operation_worker_allocations;

	operation_worker_cross_page_update(&f[0], false, false);
	operation_worker_cross_page_update(&f[1], true, false);
	f[1].extension.slot_generation++;
	f[1].extension.owner.builder_incarnation++;
	for (unsigned i = 0; i < 2; i++)
		memcpy(original[i].data, f[i].cr.source.data, BLCKSZ);
	test_operation_worker_current_busy = true;
	for (int tick = 0; tick < 64; tick++)
		for (unsigned i = 0; i < 2; i++)
			if (step[i] == CLUSTER_R4_CR_STEP_PENDING)
				step[i] = operation_worker_step_at(&f[i], i);
	for (unsigned i = 0; i < 2; i++) {
		cluster_cr_build_on_holder_forget(i, 1000 + i);
		UT_ASSERT_EQ(step[i], CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT_EQ(f[i].reason, CLUSTER_CR_BUILD_NONE);
		operation_worker_assert_cross_inverse(&f[i], original[i].data, i, 6);
	}
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(operation_worker_allocations, allocations);
}
