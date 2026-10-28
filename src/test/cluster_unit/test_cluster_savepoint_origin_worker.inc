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
static int operation_worker_expected_logs;

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
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel == LOG && operation_worker_expected_logs > 0) {
		operation_worker_expected_logs--;
		return false;
	}
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

/* Transaction ancestry terminates here; the page prior names another top. */
static void
operation_worker_mixed_top(OperationWorkerFixture *f, bool reuse, bool same_segment)
{
	UndoSlotDirEntry directory;
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer tail;
	ClusterItlSlotData *slots;
	ClusterItlSlotData previous, newest;
	char *bytes;

	operation_worker_init(f, same_segment);
	operation_worker_two_records(f);
	slots = ClusterPageGetItlSlots((Page)f->cr.source.data);
	memcpy(&directory, test_operation_page.data + BLCKSZ - 2 * sizeof(directory),
		   sizeof(directory));
	bytes = test_operation_page.data + directory.record_offset;
	memcpy(&record, bytes, sizeof(record));
	memcpy(&history, bytes + sizeof(record) + sizeof(UndoInsertPayload), sizeof(history));
	previous = history.entries[0].prior;
	previous.flags = ITL_FLAG_COMMITTED;
	previous.commit_scn = 65;
	newest = slots[2];
	newest.xid = TEST_ORIGIN_XID + 2;
	newest.wrap = reuse ? 9 : 14;
	newest.undo_segment_head = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET + 1, 1);
	slots[reuse ? 2 : 4] = newest;
	if (!reuse)
		slots[2] = previous;
	history.entries[0].slot_index = reuse ? 2 : 4;
	if (reuse)
		history.entries[0].prior = previous;
	else {
		memset(&history.entries[0].prior, 0, sizeof(previous));
		history.entries[0].prior.wrap = 14;
	}
	record.flags |= UNDO_REC_FLAG_FIRST_IN_TX;
	record.prev_uba = (UBA)InvalidUba_init;
	record.xid = TEST_ORIGIN_XID + 3;
	record.tt_slot_id = cluster_tt_slot_offset_to_id(TEST_TT_OFFSET + 1);
	record.tt_wrap_plus1 = 12;
	directory.flags = record.flags;
	memcpy(&tail, bytes + directory.record_length - sizeof(tail), sizeof(tail));
	tail.canonical_top_fxid
		= U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(9, TEST_ORIGIN_XID + 2));
	tail.operation_fxid
		= U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(9, TEST_ORIGIN_XID + 3));
	memcpy(bytes, &record, sizeof(record));
	memcpy(bytes + sizeof(record) + sizeof(UndoInsertPayload), &history, sizeof(history));
	memcpy(bytes + directory.record_length - sizeof(tail), &tail, sizeof(tail));
	memcpy(test_operation_page.data + BLCKSZ - 2 * sizeof(directory), &directory,
		   sizeof(directory));
	HeapTupleHeaderSetXmin((HeapTupleHeader)PageGetItem((Page)f->cr.source.data,
														PageGetItemId((Page)f->cr.source.data, 3)),
						   TEST_ORIGIN_XID + 3);
	memcpy(&directory, test_operation_page.data + BLCKSZ - sizeof(directory), sizeof(directory));
	bytes = test_operation_page.data + directory.record_offset;
	memcpy(&tail, bytes + directory.record_length - sizeof(tail), sizeof(tail));
	tail.operation_sequence = 500; /* A different top has its own sequence. */
	memcpy(bytes + directory.record_length - sizeof(tail), &tail, sizeof(tail));
	test_mixed_top_enabled = true;
	test_mixed_top_slot = test_tt_slot;
	test_mixed_top_slot.xid = TEST_ORIGIN_XID + 2;
	test_mixed_top_slot.wrap = 11;
	test_tt_slot.commit_scn = 65;
	test_mixed_top_native_status = TRANSACTION_STATUS_COMMITTED;
}

static void
operation_worker_assert_mixed_top(OperationWorkerFixture *f, const char original[BLCKSZ],
								  bool reuse, bool keep_old, unsigned acquires)
{
	Page page = (Page)f->cr.source.data;
	Page source = (Page)original;
	ClusterItlSlotData *slots = ClusterPageGetItlSlots(page);
	ClusterItlSlotData expected = { .wrap = 8 };

	UT_ASSERT_EQ(ItemIdIsNormal(PageGetItemId(page, 2)), keep_old);
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId(page, 3)));
	UT_ASSERT_EQ(memcmp(PageGetItem(page, PageGetItemId(page, 1)),
						PageGetItem(source, PageGetItemId(source, 1)), 60),
				 0);
	if (keep_old) {
		UT_ASSERT_EQ(memcmp(PageGetItem(page, PageGetItemId(page, 2)),
							PageGetItem(source, PageGetItemId(source, 2)), 60),
					 0);
		UT_ASSERT_EQ(slots[2].xid, TEST_ORIGIN_XID);
		UT_ASSERT_EQ(slots[2].flags, ITL_FLAG_COMMITTED);
		UT_ASSERT_EQ(slots[2].commit_scn, 65);
		expected = (ClusterItlSlotData){ .xid = TEST_ORIGIN_XID,
										 .wrap = 8,
										 .flags = ITL_FLAG_COMMITTED,
										 .write_scn = 60,
										 .commit_scn = 65,
										 .undo_segment_head
										 = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, 0) };
	} else
		UT_ASSERT_EQ(slots[2].flags, ITL_FLAG_FREE);
	UT_ASSERT_EQ(slots[2].wrap, 8);
	UT_ASSERT_EQ(memcmp(&slots[2], &expected, sizeof(expected)), 0);
	if (!reuse) {
		UT_ASSERT_EQ(slots[4].flags, ITL_FLAG_FREE);
		UT_ASSERT_EQ(slots[4].wrap, 14);
		memset(&expected, 0, sizeof(expected));
		expected.wrap = 14;
		UT_ASSERT_EQ(memcmp(&slots[4], &expected, sizeof(expected)), 0);
	}
	UT_ASSERT_EQ(f->extension.build_steps, 2);
	UT_ASSERT_EQ(test_candidate_acquire_calls, acquires);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_mixed_top_prior_restores_previous_owner)
{
	for (int reuse = 0; reuse < 2; reuse++)
		for (int same = 0; same < 2; same++) {
			OperationWorkerFixture f;
			PGAlignedBlock original;
			ClusterR4CrBuildStepResult step;

			operation_worker_mixed_top(&f, reuse, same);
			memcpy(original.data, f.cr.source.data, BLCKSZ);
			step = operation_worker_finish(&f);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
			UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_NONE);
			operation_worker_assert_mixed_top(&f, original.data, reuse, false, 6);
		}
}

UT_TEST(test_operation_worker_mixed_top_preserves_old_live_owner)
{
	for (int aborted = 0; aborted < 2; aborted++)
		for (int reuse = 0; reuse < 2; reuse++) {
			OperationWorkerFixture f;
			PGAlignedBlock original;
			ClusterR4CrBuildStepResult step;

			operation_worker_mixed_top(&f, reuse, false);
			operation_worker_row_effect(0, CLUSTER_SAVEPOINT_EFFECT_LIVE);
			operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_LIVE);
			if (aborted) {
				test_mixed_top_slot.status = TT_SLOT_ABORTED;
				test_mixed_top_slot.commit_scn = InvalidScn;
				test_mixed_top_native_status = TRANSACTION_STATUS_ABORTED;
			}
			memcpy(original.data, f.cr.source.data, BLCKSZ);
			step = operation_worker_finish(&f);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
			UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_NONE);
			operation_worker_assert_mixed_top(&f, original.data, reuse, true, 6);
		}
}

UT_TEST(test_operation_worker_mixed_top_late_live_keeps_discard_independent)
{
	for (int reuse = 0; reuse < 2; reuse++)
		for (int same = 0; same < 2; same++) {
			OperationWorkerFixture f;
			PGAlignedBlock original;
			ClusterR4CrBuildStepResult step;

			operation_worker_mixed_top(&f, reuse, same);
			operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_LIVE);
			memcpy(original.data, f.cr.source.data, BLCKSZ);
			step = operation_worker_finish(&f);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
			operation_worker_assert_mixed_top(&f, original.data, reuse, false, 6);
		}
}

UT_TEST(test_operation_worker_mixed_top_old_active_is_conservative)
{
	for (int same = 0; same < 2; same++) {
		OperationWorkerFixture f;
		PGAlignedBlock original;
		ClusterR4CrBuildStepResult step;

		operation_worker_mixed_top(&f, false, same);
		operation_worker_row_effect(0, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		ClusterPageGetItlSlots((Page)f.cr.source.data)[2].flags = ITL_FLAG_ACTIVE;
		ClusterPageGetItlSlots((Page)f.cr.source.data)[2].commit_scn = InvalidScn;
		test_tt_slot.status = TT_SLOT_ACTIVE;
		test_tt_slot.commit_scn = InvalidScn;
		test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
		test_twophase_xid = TEST_ORIGIN_XID;
		test_current_owner_available = true;
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		operation_worker_assert_mixed_top(&f, original.data, false, false, 6);
	}
}

UT_TEST(test_operation_worker_mixed_top_exact_identity_refusals)
{
	for (int fault = 0; fault < 6; fault++) {
		OperationWorkerFixture f;
		PGAlignedBlock intermediate;
		UndoRecordHeader *older;
		UndoOperationTrailer tail;
		char *bytes;
		ClusterR4CrBuildStepResult step;

		operation_worker_mixed_top(&f, true, false);
		operation_worker_first_inverse(&f);
		older = (UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader));
		bytes = (char *)(older + 1) + older->payload_length - sizeof(tail);
		memcpy(&tail, bytes, sizeof(tail));
		if (fault == 0)
			test_tt_slot.xid++;
		else if (fault == 1)
			test_tt_slot.wrap++;
		else if (fault == 2)
			tail.operation_fxid++;
		else if (fault == 3)
			tail.canonical_segment_generation++;
		else if (fault == 4)
			ClusterPageGetItlSlots((Page)f.cr.source.data)[2].wrap++;
		else
			HeapTupleHeaderSetXmin(
				(HeapTupleHeader)PageGetItem((Page)f.cr.source.data,
											 PageGetItemId((Page)f.cr.source.data, 2)),
				TEST_ORIGIN_XID + 3);
		memcpy(bytes, &tail, sizeof(tail));
		memcpy(intermediate.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
		UT_ASSERT_EQ(memcmp(intermediate.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_worker_mixed_top_cohort_has_private_observations)
{
	OperationWorkerFixture f[2];
	PGAlignedBlock original;
	ClusterR4CrBuildStepResult step[2] = { CLUSTER_R4_CR_STEP_PENDING, CLUSTER_R4_CR_STEP_PENDING };
	int allocations = operation_worker_allocations;

	operation_worker_mixed_top(&f[0], true, false);
	operation_worker_row_effect(0, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	f[1] = f[0];
	f[1].extension.slot_generation++;
	f[1].extension.owner.builder_incarnation++;
	memcpy(original.data, f[0].cr.source.data, BLCKSZ);
	test_operation_worker_current_busy = true;
	for (int tick = 0; tick < 100; tick++)
		for (unsigned i = 0; i < 2; i++)
			if (step[i] == CLUSTER_R4_CR_STEP_PENDING)
				step[i] = operation_worker_step_at(&f[i], i);
	for (unsigned i = 0; i < 2; i++) {
		cluster_cr_build_on_holder_forget(i, 1000 + i);
		UT_ASSERT_EQ(step[i], CLUSTER_R4_CR_STEP_FULL);
		operation_worker_assert_mixed_top(&f[i], original.data, true, true, 12);
	}
	UT_ASSERT_EQ(test_candidate_max_held_count, 1);
	UT_ASSERT_EQ(operation_worker_allocations, allocations);
}

UT_TEST(test_operation_worker_malformed_transaction_prev_preserves_output)
{
	UBA malformed[] = { uba_encode(TEST_RECORD_SEGMENT, 0, TEST_TT_OFFSET, 0),
						uba_encode(TEST_RECORD_SEGMENT, UNDO_BLOCKS_PER_SEGMENT, TEST_TT_OFFSET, 0),
						uba_encode(TEST_RECORD_SEGMENT, 6, TEST_TT_OFFSET + 1, 0),
						uba_encode(CLUSTER_UNDO_SEGS_PER_INSTANCE + 1, 6, TEST_TT_OFFSET, 0),
						uba_encode(TEST_RECORD_SEGMENT, 6, TEST_TT_OFFSET,
								   (BLCKSZ - sizeof(UndoBlockHeader)) / sizeof(UndoSlotDirEntry)),
						uba_encode(TEST_RECORD_SEGMENT, 8, TEST_TT_OFFSET, 0),
						uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, 2) };

	for (int same = 0; same < 2; same++)
		for (unsigned i = 0; i < lengthof(malformed); i++) {
			OperationWorkerFixture f;
			PGAlignedBlock before;
			UndoSlotDirEntry directory;
			UndoRecordHeader record;
			ClusterR4CrBuildStepResult step;
			char *bytes;

			operation_worker_init(&f, same);
			operation_worker_two_records(&f);
			memcpy(&directory, test_operation_page.data + BLCKSZ - 2 * sizeof(directory),
				   sizeof(directory));
			bytes = test_operation_page.data + directory.record_offset;
			memcpy(&record, bytes, sizeof(record));
			record.prev_uba = malformed[i];
			memcpy(bytes, &record, sizeof(record));
			memcpy(before.data, f.cr.source.data, BLCKSZ);
			step = operation_worker_finish(&f);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
			UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
			UT_ASSERT_EQ(memcmp(before.data, f.cr.source.data, BLCKSZ), 0);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
		}
}

/* Two different retained creations name one physical TID. This fixture does
 * not model native pruning or provide permission to recycle that TID. */
static void
operation_worker_reused_creation(OperationWorkerFixture *f, bool reuse, bool same)
{
	UndoSlotDirEntry directory;
	UndoRecordHeader record;
	char *bytes;
	Page page;

	operation_worker_mixed_top(f, reuse, same);
	operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_mixed_top_slot.commit_scn = 65; /* Isolate the visibility branch. */
	memcpy(&directory, test_operation_page.data + BLCKSZ - 2 * sizeof(directory),
		   sizeof(directory));
	bytes = test_operation_page.data + directory.record_offset;
	memcpy(&record, bytes, sizeof(record));
	record.target_offset = 2;
	memcpy(bytes, &record, sizeof(record));
	page = (Page)f->cr.source.data;
	HeapTupleHeaderSetXmin((HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 2)),
						   TEST_ORIGIN_XID + 3);
	ItemIdSetUnused(PageGetItemId(page, 3));
}

static void
operation_worker_creation_first(OperationWorkerFixture *f)
{
	for (int tick = 0; tick < 64 && test_candidate_release_calls < 3; tick++)
		UT_ASSERT_EQ(operation_worker_step(f), CLUSTER_R4_CR_STEP_PENDING);
	UT_ASSERT_EQ(f->extension.build_steps, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

static void
operation_worker_assert_reused_creation(OperationWorkerFixture *f, const char original[BLCKSZ],
										bool reuse, bool old_live)
{
	PGAlignedBlock expected;
	ClusterItlSlotData *slot;

	memcpy(expected.data, original, BLCKSZ);
	if (!reuse && !old_live) {
		slot = &ClusterPageGetItlSlots((Page)expected.data)[2];
		memset(slot, 0, sizeof(*slot));
		slot->wrap = 8;
	}
	UT_ASSERT_EQ(memcmp(f->cr.source.data, expected.data, BLCKSZ), 0);
	UT_ASSERT_EQ(f->extension.build_steps, 2);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_kept_insert_covers_discarded_reused_creation)
{
	for (int reuse = 0; reuse < 2; reuse++)
		for (int same = 0; same < 2; same++) {
			OperationWorkerFixture f;
			PGAlignedBlock original;
			ClusterR4CrBuildStepResult step;
			int allocations = operation_worker_allocations;

			operation_worker_reused_creation(&f, reuse, same);
			memcpy(original.data, f.cr.source.data, BLCKSZ);
			step = operation_worker_finish(&f);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
			operation_worker_assert_reused_creation(&f, original.data, reuse, false);
			UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
			UT_ASSERT_EQ(operation_worker_allocations, allocations);
		}
}

UT_TEST(test_operation_worker_kept_insert_covers_visible_reused_creation)
{
	for (int reuse = 0; reuse < 2; reuse++) {
		OperationWorkerFixture f;
		PGAlignedBlock original;
		ClusterR4CrBuildStepResult step;

		operation_worker_reused_creation(&f, reuse, false);
		operation_worker_row_effect(0, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		operation_worker_assert_reused_creation(&f, original.data, reuse, true);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	}
}

UT_TEST(test_operation_worker_unkept_reused_creation_needs_no_witness)
{
	for (int discarded = 0; discarded < 2; discarded++) {
		OperationWorkerFixture f;
		ClusterR4CrBuildStepResult step;

		operation_worker_reused_creation(&f, true, false);
		if (discarded)
			operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
		else
			test_mixed_top_slot.commit_scn = 80;
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)f.cr.source.data, 2)));
		UT_ASSERT_EQ(ClusterPageGetItlSlots((Page)f.cr.source.data)[2].flags, ITL_FLAG_FREE);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_worker_reused_creation_witness_mismatch_refuses)
{
	for (int fault = 0; fault < 6; fault++) {
		OperationWorkerFixture f;
		PGAlignedBlock changed;
		Page page;
		ItemId item;
		HeapTupleHeader tuple;
		ClusterR4CrBuildStepResult step;

		operation_worker_reused_creation(&f, true, false);
		operation_worker_creation_first(&f);
		page = (Page)f.cr.source.data;
		item = PageGetItemId(page, 2);
		tuple = (HeapTupleHeader)PageGetItem(page, item);
		if (fault == 0)
			HeapTupleHeaderSetXmin(tuple, TEST_ORIGIN_XID + 1);
		else if (fault == 1)
			ItemIdSetNormal(item, ItemIdGetOffset(item), ItemIdGetLength(item) - 1);
		else if (fault == 2)
			tuple->t_hoff = 0;
		else if (fault == 3)
			tuple->t_infomask |= HEAP_XMAX_IS_MULTI;
		else if (fault == 4)
			ItemIdSetUnused(item);
		else {
			UndoRecordHeader *older
				= (UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader));
			UndoOperationTrailer tail;
			char *bytes = (char *)(older + 1) + older->payload_length - sizeof(tail);

			older->xid = TEST_ORIGIN_XID + 3;
			memcpy(&tail, bytes, sizeof(tail));
			tail.operation_fxid = U64FromFullTransactionId(
				FullTransactionIdFromEpochAndXid(9, TEST_ORIGIN_XID + 3));
			memcpy(bytes, &tail, sizeof(tail));
		}
		memcpy(changed.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
		UT_ASSERT_EQ(memcmp(changed.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_worker_reused_creation_never_bypasses_unknown_top)
{
	OperationWorkerFixture f;
	PGAlignedBlock before;
	ClusterR4CrBuildStepResult step;

	operation_worker_reused_creation(&f, true, false);
	operation_worker_creation_first(&f);
	test_tt_slot.xid++;
	memcpy(before.data, f.cr.source.data, BLCKSZ);
	step = operation_worker_finish(&f);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
	UT_ASSERT_EQ(memcmp(before.data, f.cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

UT_TEST(test_operation_worker_reused_creation_cohort_has_private_witnesses)
{
	OperationWorkerFixture f[2];
	PGAlignedBlock original;
	ClusterR4CrBuildStepResult step[2] = { CLUSTER_R4_CR_STEP_PENDING, CLUSTER_R4_CR_STEP_PENDING };
	int allocations = operation_worker_allocations;

	operation_worker_reused_creation(&f[0], true, false);
	f[1] = f[0];
	f[1].extension.slot_generation++;
	f[1].extension.owner.builder_incarnation++;
	memcpy(original.data, f[0].cr.source.data, BLCKSZ);
	test_operation_worker_current_busy = true;
	for (int tick = 0; tick < 100; tick++)
		for (unsigned i = 0; i < 2; i++)
			if (step[i] == CLUSTER_R4_CR_STEP_PENDING)
				step[i] = operation_worker_step_at(&f[i], i);
	for (unsigned i = 0; i < 2; i++) {
		cluster_cr_build_on_holder_forget(i, 1000 + i);
		UT_ASSERT_EQ(step[i], CLUSTER_R4_CR_STEP_FULL);
		operation_worker_assert_reused_creation(&f[i], original.data, true, false);
	}
	UT_ASSERT_EQ(test_candidate_acquire_calls, 12);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(operation_worker_allocations, allocations);
}

/* Roles: same-page ordinary/HOT, cross-page new, cross-page old only.
 * Both retained creators remain available; this grants no native TID reuse. */
static void
operation_worker_reused_update_creation(OperationWorkerFixture *f, unsigned role, bool reuse,
										bool same)
{
	UndoSlotDirEntry directory;
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer tail;
	UndoUpdatePayload update = { .new_block = role == 3 ? 24 : 23,
								 .new_offset = 2,
								 .old_tuple_length = 60,
								 .old_tuple_offset = sizeof(update) };
	char body[sizeof(update) + 60];
	char *bytes;
	Page page;
	HeapTupleHeader root;

	operation_worker_reused_creation(f, reuse, same);
	page = (Page)f->cr.source.data;
	memcpy(&directory, test_operation_page.data + BLCKSZ - 2 * sizeof(directory),
		   sizeof(directory));
	bytes = test_operation_page.data + directory.record_offset;
	memcpy(&record, bytes, sizeof(record));
	memcpy(&history, bytes + sizeof(record) + sizeof(UndoInsertPayload), sizeof(history));
	memcpy(&tail, bytes + directory.record_length - sizeof(tail), sizeof(tail));
	root = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1));
	memcpy(body, &update, sizeof(update));
	memcpy(body + sizeof(update), root, 60);
	record.record_type = UNDO_RECORD_UPDATE;
	record.target_block = role == 2 ? 24 : 23;
	record.target_offset = 1;
	record.payload_length = sizeof(body) + sizeof(history) + sizeof(tail);
	if (role >= 2) {
		history.count = 2;
		history.entries[1] = history.entries[0];
		history.entries[1].block = 24;
		history.entries[1].slot_index = 6;
		memset(&history.entries[1].prior, 0, sizeof(history.entries[1].prior));
		history.entries[1].prior.wrap = 19;
	}
	if (role == 2) {
		UndoItlHistoryEntry current = history.entries[0];

		history.entries[0] = history.entries[1];
		history.entries[1] = current;
		ItemPointerSet(&((HeapTupleHeader)(body + sizeof(update)))->t_ctid, 24, 1);
	} else {
		HeapTupleHeaderSetXmax(root, TEST_ORIGIN_XID + 3);
		root->t_infomask &= ~HEAP_XMAX_INVALID;
		ItemPointerSet(&root->t_ctid, update.new_block, 2);
		if (role == 1)
			root->t_infomask2 |= HEAP_HOT_UPDATED;
	}
	if (role == 3)
		HeapTupleHeaderSetXmin((HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 2)),
							   TEST_ORIGIN_XID + 1);
	directory.record_type = record.record_type;
	directory.record_length = sizeof(record) + record.payload_length;
	memcpy(bytes, &record, sizeof(record));
	memcpy(bytes + sizeof(record), body, sizeof(body));
	memcpy(bytes + sizeof(record) + sizeof(body), &history, sizeof(history));
	memcpy(bytes + sizeof(record) + sizeof(body) + sizeof(history), &tail, sizeof(tail));
	memcpy(test_operation_page.data + BLCKSZ - 2 * sizeof(directory), &directory,
		   sizeof(directory));
	((UndoBlockHeader *)test_operation_page.data)->free_offset
		= directory.record_offset + directory.record_length;
}

UT_TEST(test_operation_worker_kept_update_covers_discarded_reused_creation)
{
	for (unsigned role = 0; role < 3; role++)
		for (int reuse = 0; reuse < 2; reuse++)
			for (int same = 0; same < 2; same++) {
				OperationWorkerFixture f;
				PGAlignedBlock original;
				ClusterR4CrBuildStepResult step;
				int allocations = operation_worker_allocations;

				operation_worker_reused_update_creation(&f, role, reuse, same);
				memcpy(original.data, f.cr.source.data, BLCKSZ);
				operation_worker_creation_first(&f);
				step = operation_worker_finish(&f);
				cluster_cr_build_on_holder_forget(0, 1000);
				UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
				operation_worker_assert_reused_creation(&f, original.data, reuse, false);
				UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
				UT_ASSERT_EQ(operation_worker_allocations, allocations);
			}
}

UT_TEST(test_operation_worker_kept_update_covers_visible_reused_creation)
{
	for (unsigned role = 0; role < 3; role++)
		for (int reuse = 0; reuse < 2; reuse++) {
			OperationWorkerFixture f;
			PGAlignedBlock original;
			ClusterR4CrBuildStepResult step;

			operation_worker_reused_update_creation(&f, role, reuse, false);
			operation_worker_row_effect(0, CLUSTER_SAVEPOINT_EFFECT_LIVE);
			memcpy(original.data, f.cr.source.data, BLCKSZ);
			operation_worker_creation_first(&f);
			step = operation_worker_finish(&f);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
			operation_worker_assert_reused_creation(&f, original.data, reuse, true);
			UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
		}
}

UT_TEST(test_operation_worker_kept_update_old_page_does_not_create_replacement)
{
	for (int reuse = 0; reuse < 2; reuse++)
		for (int same = 0; same < 2; same++) {
			OperationWorkerFixture f;
			PGAlignedBlock expected;
			ClusterR4CrBuildStepResult step;

			operation_worker_reused_update_creation(&f, 3, reuse, same);
			memcpy(expected.data, f.cr.source.data, BLCKSZ);
			ItemIdSetUnused(PageGetItemId((Page)expected.data, 2));
			step = operation_worker_finish(&f);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
			operation_worker_assert_reused_creation(&f, expected.data, reuse, false);
			UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
		}
}

UT_TEST(test_operation_worker_unkept_update_removes_reused_creation)
{
	for (unsigned role = 0; role < 3; role++)
		for (int discarded = 0; discarded < 2; discarded++) {
			OperationWorkerFixture f;
			PGAlignedBlock expected;
			UndoSlotDirEntry directory;
			ClusterItlSlotData *slot;
			ClusterR4CrBuildStepResult step;

			operation_worker_reused_update_creation(&f, role, true, false);
			if (discarded)
				operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
			else
				test_mixed_top_slot.commit_scn = 80;
			memcpy(expected.data, f.cr.source.data, BLCKSZ);
			ItemIdSetUnused(PageGetItemId((Page)expected.data, 2));
			memcpy(&directory, test_operation_page.data + BLCKSZ - 2 * sizeof(directory),
				   sizeof(directory));
			if (role != 2)
				memcpy(PageGetItem((Page)expected.data, PageGetItemId((Page)expected.data, 1)),
					   test_operation_page.data + directory.record_offset + sizeof(UndoRecordHeader)
						   + sizeof(UndoUpdatePayload),
					   60);
			slot = &ClusterPageGetItlSlots((Page)expected.data)[2];
			memset(slot, 0, sizeof(*slot));
			slot->wrap = 8;
			step = operation_worker_finish(&f);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
			UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
			UT_ASSERT_EQ(f.extension.build_steps, 2);
			UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
		}
}

UT_TEST(test_operation_worker_update_creation_witness_mismatch_refuses)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		OperationWorkerFixture f;
		PGAlignedBlock changed;
		ItemId item;
		HeapTupleHeader tuple;
		ClusterR4CrBuildStepResult step;

		operation_worker_reused_update_creation(&f, 2, true, false);
		operation_worker_creation_first(&f);
		item = PageGetItemId((Page)f.cr.source.data, 2);
		tuple = (HeapTupleHeader)PageGetItem((Page)f.cr.source.data, item);
		if (fault == 0)
			HeapTupleHeaderSetXmin(tuple, TEST_ORIGIN_XID + 1);
		else if (fault == 1)
			item->lp_len--;
		else if (fault == 2)
			tuple->t_hoff = 0;
		else if (fault == 3)
			tuple->t_infomask |= HEAP_XMAX_IS_MULTI;
		else if (fault == 4)
			ItemIdSetUnused(item);
		else {
			UndoRecordHeader *old
				= (UndoRecordHeader *)(test_operation_page.data + sizeof(UndoBlockHeader));
			UndoOperationTrailer tail;
			char *bytes = (char *)(old + 1) + old->payload_length - sizeof(tail);

			old->xid = TEST_ORIGIN_XID + 3;
			memcpy(&tail, bytes, sizeof(tail));
			tail.operation_fxid = U64FromFullTransactionId(
				FullTransactionIdFromEpochAndXid(9, TEST_ORIGIN_XID + 3));
			memcpy(bytes, &tail, sizeof(tail));
		}
		memcpy(changed.data, f.cr.source.data, BLCKSZ);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
		UT_ASSERT_EQ(memcmp(changed.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
	}
}

UT_TEST(test_operation_worker_update_creation_cohort_is_private)
{
	OperationWorkerFixture f[2];
	PGAlignedBlock original;
	ClusterR4CrBuildStepResult step[2] = { CLUSTER_R4_CR_STEP_PENDING, CLUSTER_R4_CR_STEP_PENDING };
	int allocations = operation_worker_allocations;

	operation_worker_reused_update_creation(&f[0], 2, true, false);
	f[1] = f[0];
	f[1].extension.slot_generation++;
	f[1].extension.owner.builder_incarnation++;
	memcpy(original.data, f[0].cr.source.data, BLCKSZ);
	test_operation_worker_current_busy = true;
	for (int tick = 0; tick < 100; tick++)
		for (unsigned i = 0; i < 2; i++)
			if (step[i] == CLUSTER_R4_CR_STEP_PENDING)
				step[i] = operation_worker_step_at(&f[i], i);
	for (unsigned i = 0; i < 2; i++) {
		cluster_cr_build_on_holder_forget(i, 1000 + i);
		UT_ASSERT_EQ(step[i], CLUSTER_R4_CR_STEP_FULL);
		operation_worker_assert_reused_creation(&f[i], original.data, true, false);
	}
	UT_ASSERT_EQ(test_candidate_acquire_calls, 12);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(operation_worker_allocations, allocations);
}

#define OPERATION_WORKER_LONG_RECORDS 24

/* One DATA block, twenty-four distinct tuple creations and exact page edges.
 * Modes are all discarded, all LIVE, and alternating even-row LIVE. */
static void
operation_worker_long_chain(OperationWorkerFixture *f, bool same, unsigned mode)
{
	UndoSlotDirEntry directory;
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer tail;
	PGAlignedBlock prototype;
	ClusterItlSlotData *head;
	char tuple_bytes[60];
	HeapTupleHeader tuple = (HeapTupleHeader)tuple_bytes;
	Page page;

	operation_worker_init(f, same);
	memcpy(&directory, test_operation_page.data + BLCKSZ - sizeof(directory), sizeof(directory));
	memcpy(prototype.data, test_operation_page.data + directory.record_offset,
		   directory.record_length);
	memcpy(&record, prototype.data, sizeof(record));
	memcpy(&history, prototype.data + sizeof(record) + sizeof(UndoInsertPayload), sizeof(history));
	memcpy(&tail, prototype.data + directory.record_length - sizeof(tail), sizeof(tail));
	for (unsigned row = 0; row < OPERATION_WORKER_LONG_RECORDS; row++) {
		char *bytes
			= test_operation_page.data + sizeof(UndoBlockHeader) + row * directory.record_length;

		record.target_offset = row + 2;
		record.write_scn = 60 + row;
		record.flags = (record.flags & ~UNDO_REC_FLAG_FIRST_IN_TX)
					   | (row == 0 ? UNDO_REC_FLAG_FIRST_IN_TX : 0);
		record.prev_uba = row == 0 ? (UBA)InvalidUba_init
								   : uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, row - 1);
		memset(&history.entries[0].prior, 0, sizeof(history.entries[0].prior));
		history.entries[0].prior.wrap = 8;
		if (row != 0)
			history.entries[0].prior = (ClusterItlSlotData){ .xid = TEST_ORIGIN_XID,
															 .wrap = 8,
															 .flags = ITL_FLAG_ACTIVE,
															 .write_scn = 59 + row,
															 .undo_segment_head = record.prev_uba };
		history.entries[0].after_write_scn = record.write_scn;
		tail.operation_sequence = 100 + row;
		tail.effect_state = mode == 1 || (mode == 2 && row % 2 == 0)
								? CLUSTER_SAVEPOINT_EFFECT_LIVE
								: CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
		memcpy(bytes, prototype.data, directory.record_length);
		memcpy(bytes, &record, sizeof(record));
		memcpy(bytes + sizeof(record) + sizeof(UndoInsertPayload), &history, sizeof(history));
		memcpy(bytes + directory.record_length - sizeof(tail), &tail, sizeof(tail));
		directory.record_offset = sizeof(UndoBlockHeader) + row * directory.record_length;
		directory.flags = record.flags;
		memcpy(test_operation_page.data + BLCKSZ - (row + 1) * sizeof(directory), &directory,
			   sizeof(directory));
	}
	((UndoBlockHeader *)test_operation_page.data)->slot_count = OPERATION_WORKER_LONG_RECORDS;
	((UndoBlockHeader *)test_operation_page.data)->free_offset
		= directory.record_offset + directory.record_length;
	UT_ASSERT(directory.record_offset + directory.record_length
			  < BLCKSZ - OPERATION_WORKER_LONG_RECORDS * sizeof(directory));
	page = (Page)f->cr.source.data;
	memcpy(tuple_bytes, PageGetItem(page, PageGetItemId(page, 1)), sizeof(tuple_bytes));
	PageInit(page, BLCKSZ, CLUSTER_ITL_SPECIAL_SIZE);
	((PageHeader)page)->pd_flags |= PD_HAS_ITL;
	for (unsigned offset = 1; offset <= OPERATION_WORKER_LONG_RECORDS + 1; offset++) {
		HeapTupleHeaderSetXmin(tuple, offset == 1 ? TEST_ORIGIN_XID - 5 : TEST_ORIGIN_XID + 1);
		ItemPointerSet(&tuple->t_ctid, 23, offset);
		tuple_bytes[59] = offset;
		UT_ASSERT_EQ(PageAddItem(page, tuple_bytes, sizeof(tuple_bytes), offset, false, true),
					 offset);
	}
	head = &ClusterPageGetItlSlots(page)[2];
	*head = (ClusterItlSlotData){ .xid = TEST_ORIGIN_XID,
								  .wrap = 8,
								  .flags = ITL_FLAG_COMMITTED,
								  .commit_scn = 65,
								  .write_scn = 83,
								  .undo_segment_head
								  = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, 23) };
	test_tt_slot.commit_scn = 65;
}

static ClusterR4CrBuildStepResult
operation_worker_long_finish(OperationWorkerFixture *f)
{
	ClusterR4CrBuildStepResult step = CLUSTER_R4_CR_STEP_PENDING;

	for (unsigned tick = 0; tick < 512 && step == CLUSTER_R4_CR_STEP_PENDING; tick++) {
		int before = test_candidate_acquire_calls + test_candidate_poll_calls
					 + test_candidate_release_calls + test_operation_release_poll_count;

		step = operation_worker_step(f);
		UT_ASSERT(test_candidate_acquire_calls + test_candidate_poll_calls
					  + test_candidate_release_calls + test_operation_release_poll_count - before
				  <= 1);
		UT_ASSERT_EQ(test_candidate_wait_calls, 0);
		UT_ASSERT_EQ(test_operation_release_wait_count, 0);
	}
	UT_ASSERT(step != CLUSTER_R4_CR_STEP_PENDING);
	return step;
}

static void
operation_worker_long_advance(OperationWorkerFixture *f, unsigned completed)
{
	OffsetNumber last = OPERATION_WORKER_LONG_RECORDS + 2 - completed;

	UT_ASSERT(completed > 0 && completed < OPERATION_WORKER_LONG_RECORDS);
	/* Release begin precedes the inverse; wait for its distinct tuple effect. */
	for (unsigned tick = 0;
		 tick < 512 && ItemIdIsUsed(PageGetItemId((Page)f->cr.source.data, last)); tick++)
		UT_ASSERT_EQ(operation_worker_step(f), CLUSTER_R4_CR_STEP_PENDING);
	UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)f->cr.source.data, last)));
	UT_ASSERT_EQ(f->extension.build_steps, completed);
	UT_ASSERT_EQ(test_candidate_acquire_calls, completed * 3);
	UT_ASSERT_EQ(test_candidate_release_calls, completed * 3);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

/* Literal survivor masks, not a second inverse or a decoded expectation. */
static void
operation_worker_long_expected(char expected[BLCKSZ], const char source[BLCKSZ], uint32 kept)
{
	ClusterItlSlotData *head;

	memcpy(expected, source, BLCKSZ);
	for (unsigned row = 0; row < OPERATION_WORKER_LONG_RECORDS; row++)
		if ((kept & (1U << row)) == 0)
			ItemIdSetUnused(PageGetItemId((Page)expected, row + 2));
	head = &ClusterPageGetItlSlots((Page)expected)[2];
	if (kept == 0) {
		memset(head, 0, sizeof(*head));
		head->wrap = 8;
	} else if (kept == 0x555555U) {
		head->flags = ITL_FLAG_ACTIVE;
		head->commit_scn = InvalidScn;
		head->write_scn = 82;
		head->undo_segment_head = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, 22);
	} else
		UT_ASSERT_EQ(kept, 0xffffffU);
}

UT_TEST(test_operation_worker_long_discarded_chain_reaches_original_end)
{
	for (int same = 0; same < 2; same++) {
		OperationWorkerFixture f;
		PGAlignedBlock expected;
		ClusterR4CrBuildStepResult step;
		int allocations = operation_worker_allocations;

		operation_worker_long_chain(&f, same, 0);
		operation_worker_long_expected(expected.data, f.cr.source.data, 0);
		step = operation_worker_long_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(f.extension.build_steps, 24);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 72);
		UT_ASSERT_EQ(test_candidate_release_calls, 72);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(operation_worker_allocations, allocations);
	}
}

UT_TEST(test_operation_worker_long_live_chain_preserves_literal_survivors)
{
	for (unsigned mode = 1; mode <= 2; mode++)
		for (int same = 0; same < 2; same++) {
			OperationWorkerFixture f;
			PGAlignedBlock expected;
			ClusterR4CrBuildStepResult step;
			int allocations = operation_worker_allocations;

			operation_worker_long_chain(&f, same, mode);
			operation_worker_long_expected(expected.data, f.cr.source.data,
										   mode == 1 ? 0xffffffU : 0x555555U);
			step = operation_worker_long_finish(&f);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
			UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
			UT_ASSERT_EQ(f.extension.build_steps, 24);
			UT_ASSERT_EQ(test_candidate_acquire_calls, 72);
			UT_ASSERT_EQ(test_candidate_release_calls, 72);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
			UT_ASSERT_EQ(operation_worker_allocations, allocations);
		}
}

UT_TEST(test_operation_worker_long_active_sample_never_promotes)
{
	OperationWorkerFixture f;
	PGAlignedBlock expected;
	ClusterR4CrBuildStepResult step;
	int allocations = operation_worker_allocations;

	operation_worker_long_chain(&f, false, 1);
	operation_worker_long_expected(expected.data, f.cr.source.data, 0);
	test_tt_slot.status = TT_SLOT_ACTIVE;
	test_tt_slot.commit_scn = InvalidScn;
	test_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID;
	test_current_owner_available = true;
	operation_worker_long_advance(&f, 1);
	test_tt_slot.status = TT_SLOT_COMMITTED;
	test_tt_slot.commit_scn = 65;
	test_native_status = TRANSACTION_STATUS_COMMITTED;
	step = operation_worker_long_finish(&f);
	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(f.extension.build_steps, 24);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 72);
	UT_ASSERT_EQ(test_candidate_release_calls, 72);
	UT_ASSERT_EQ(operation_worker_allocations, allocations);
}

UT_TEST(test_operation_worker_long_chain_limit_is_exact)
{
	for (int limit = 23; limit <= 24; limit++) {
		OperationWorkerFixture f;
		PGAlignedBlock expected;
		ClusterItlSlotData *head;
		ClusterR4CrBuildStepResult step;
		int allocations = operation_worker_allocations;

		operation_worker_long_chain(&f, false, 0);
		operation_worker_long_expected(expected.data, f.cr.source.data, 0);
		if (limit == 23) {
			*PageGetItemId((Page)expected.data, 2) = *PageGetItemId((Page)f.cr.source.data, 2);
			head = &ClusterPageGetItlSlots((Page)expected.data)[2];
			head->xid = TEST_ORIGIN_XID;
			head->flags = ITL_FLAG_ACTIVE;
			head->write_scn = 60;
			head->undo_segment_head = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, 0);
		}
		cluster_cr_chain_walk_max_steps = limit;
		step = operation_worker_long_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		cluster_cr_chain_walk_max_steps = 4096;
		UT_ASSERT_EQ(step, limit == 24 ? CLUSTER_R4_CR_STEP_FULL : CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(f.reason, limit == 24 ? CLUSTER_CR_BUILD_NONE : CLUSTER_CR_BUILD_CHAIN_LIMIT);
		UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(f.extension.build_steps, limit);
		UT_ASSERT_EQ(test_candidate_acquire_calls, limit * 3);
		UT_ASSERT_EQ(test_candidate_release_calls, limit * 3);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(operation_worker_allocations, allocations);
	}
}

UT_TEST(test_operation_worker_long_deep_history_refusals)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		OperationWorkerFixture f;
		PGAlignedBlock remaining;
		UndoSlotDirEntry directory;
		UndoRecordHeader *record;
		UndoOperationTrailer tail;
		UndoItlHistoryTrailer history;
		char *body;
		ClusterR4CrBuildStepResult step;
		int allocations = operation_worker_allocations;

		operation_worker_long_chain(&f, false, 0);
		operation_worker_long_advance(&f, 16);
		memcpy(&directory, test_operation_page.data + BLCKSZ - 8 * sizeof(directory),
			   sizeof(directory));
		record = (UndoRecordHeader *)(test_operation_page.data + directory.record_offset);
		body = (char *)(record + 1);
		memcpy(&tail, body + record->payload_length - sizeof(tail), sizeof(tail));
		if (fault == 0)
			tail.operation_sequence = 108;
		else if (fault == 1) {
			directory.record_length = 0;
			memcpy(test_operation_page.data + BLCKSZ - 8 * sizeof(directory), &directory,
				   sizeof(directory));
		} else if (fault == 2) {
			memcpy(&history, body + sizeof(UndoInsertPayload), sizeof(history));
			history.entries[0].prior.undo_segment_head
				= uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET, 23);
			memcpy(body + sizeof(UndoInsertPayload), &history, sizeof(history));
		} else
			tail.canonical_segment_generation++;
		memcpy(body + record->payload_length - sizeof(tail), &tail, sizeof(tail));
		if (fault == 2)
			operation_worker_long_advance(&f, 17);
		memcpy(remaining.data, f.cr.source.data, BLCKSZ);
		operation_worker_expected_logs = fault == 1 ? 1 : 0;
		step = operation_worker_long_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(operation_worker_expected_logs, 0);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
		UT_ASSERT_EQ(f.reason,
					 fault == 1 ? CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD : CLUSTER_CR_BUILD_BAD_UNDO);
		UT_ASSERT_EQ(memcmp(remaining.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(f.extension.build_steps, fault == 2 ? 18 : 17);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(operation_worker_allocations, allocations);
	}
}

UT_TEST(test_operation_worker_long_cohort_has_private_progress)
{
	OperationWorkerFixture f[2];
	PGAlignedBlock expected;
	ClusterR4CrBuildStepResult step[2] = { CLUSTER_R4_CR_STEP_PENDING, CLUSTER_R4_CR_STEP_PENDING };
	int allocations = operation_worker_allocations;

	operation_worker_long_chain(&f[0], false, 2);
	f[1] = f[0];
	f[1].extension.slot_generation++;
	f[1].extension.owner.builder_incarnation++;
	operation_worker_long_expected(expected.data, f[0].cr.source.data, 0x555555U);
	test_operation_worker_current_busy = true;
	for (unsigned tick = 0; tick < 1000; tick++)
		for (unsigned i = 0; i < 2; i++)
			if (step[i] == CLUSTER_R4_CR_STEP_PENDING)
				step[i] = operation_worker_step_at(&f[i], i);
	for (unsigned i = 0; i < 2; i++) {
		cluster_cr_build_on_holder_forget(i, 1000 + i);
		UT_ASSERT_EQ(step[i], CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT_EQ(memcmp(expected.data, f[i].cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(f[i].extension.build_steps, 24);
	}
	UT_ASSERT_EQ(test_candidate_acquire_calls, 144);
	UT_ASSERT_EQ(test_candidate_release_calls, 144);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(operation_worker_allocations, allocations);
}

/* Kept lock effects and retained tuple creation are separate observations. */
static void
creation_challenge_lock(OperationWorkerFixture *f, bool same, bool discarded,
						char before_lock[BLCKSZ])
{
	UndoSlotDirEntry directory;
	UndoRecordHeader record;
	UndoOperationTrailer tail;
	UndoItlHistoryTrailer history;
	UndoItlPayload lock;
	ClusterItlSlotData *head;
	HeapTupleHeader tuple;
	UndoBlockHeader *block;
	char *bytes;
	uint16 offset;

	operation_worker_reused_creation(f, true, same);
	memcpy(before_lock, f->cr.source.data, BLCKSZ);
	memcpy(&directory, test_operation_page.data + BLCKSZ - 2 * sizeof(directory),
		   sizeof(directory));
	bytes = test_operation_page.data + directory.record_offset;
	memcpy(&record, bytes, sizeof(record));
	memcpy(&tail, bytes + directory.record_length - sizeof(tail), sizeof(tail));
	head = &ClusterPageGetItlSlots((Page)f->cr.source.data)[2];
	tuple = (HeapTupleHeader)PageGetItem((Page)f->cr.source.data,
										 PageGetItemId((Page)f->cr.source.data, 2));
	lock = (UndoItlPayload){ .itl_slot_idx = 2,
							 .prev_flags = head->flags,
							 .new_flags = ITL_FLAG_LOCK_ONLY_ACTIVE,
							 .lock_xid = record.xid,
							 .prev_xmax = HeapTupleHeaderGetRawXmax(tuple),
							 .prev_infomask = tuple->t_infomask };
	memset(&history, 0, sizeof(history));
	history.magic = UNDO_ITL_HISTORY_MAGIC;
	history.version = UNDO_ITL_HISTORY_VERSION;
	history.count = 1;
	history.entries[0].block = 23;
	history.entries[0].slot_index = 2;
	history.entries[0].after_kind = ITL_FLAG_LOCK_ONLY_ACTIVE;
	history.entries[0].after_write_scn = 71;
	history.entries[0].prior = *head;
	record.record_type = UNDO_RECORD_ITL;
	record.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
	record.prev_uba = head->undo_segment_head;
	record.write_scn = 71;
	record.payload_length = sizeof(lock) + sizeof(history) + sizeof(tail);
	tail.operation_sequence++;
	tail.effect_state
		= discarded ? CLUSTER_SAVEPOINT_EFFECT_DISCARDED : CLUSTER_SAVEPOINT_EFFECT_LIVE;
	block = (UndoBlockHeader *)test_operation_page.data;
	offset = block->free_offset;
	bytes = test_operation_page.data + offset;
	memcpy(bytes, &record, sizeof(record));
	memcpy(bytes + sizeof(record), &lock, sizeof(lock));
	memcpy(bytes + sizeof(record) + sizeof(lock), &history, sizeof(history));
	memcpy(bytes + sizeof(record) + sizeof(lock) + sizeof(history), &tail, sizeof(tail));
	directory.record_offset = offset;
	directory.record_length = sizeof(record) + record.payload_length;
	directory.record_type = record.record_type;
	directory.flags = record.flags;
	memcpy(test_operation_page.data + BLCKSZ - 3 * sizeof(directory), &directory,
		   sizeof(directory));
	block->slot_count = 3;
	block->free_offset = offset + directory.record_length;
	head->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	head->wrap++;
	head->write_scn = 71;
	head->undo_segment_head = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET + 1, 2);
	HeapTupleHeaderSetXmax(tuple, record.xid);
	tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
}

UT_TEST(test_creation_challenge_latest_live_lock_keeps_creator)
{
	for (int same = 0; same < 2; same++) {
		OperationWorkerFixture f;
		PGAlignedBlock original, before_lock;
		ClusterR4CrBuildStepResult step = CLUSTER_R4_CR_STEP_PENDING;
		int allocations = operation_worker_allocations;

		creation_challenge_lock(&f, same, false, before_lock.data);
		memcpy(original.data, f.cr.source.data, BLCKSZ);
		for (unsigned tick = 0; tick < 128 && f.extension.build_steps < 3; tick++) {
			step = operation_worker_step(&f);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_PENDING);
		}
		UT_ASSERT_EQ(f.extension.build_steps, 3);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
		UT_ASSERT_EQ(test_candidate_release_calls, 6);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(operation_worker_allocations, allocations);
	}
}

UT_TEST(test_creation_challenge_discarded_lock_does_not_hide_creator)
{
	for (int same = 0; same < 2; same++) {
		OperationWorkerFixture f;
		PGAlignedBlock expected;
		ClusterR4CrBuildStepResult step;
		int allocations = operation_worker_allocations;

		creation_challenge_lock(&f, same, true, expected.data);
		step = operation_worker_finish(&f);
		cluster_cr_build_on_holder_forget(0, 1000);
		UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
		UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
		UT_ASSERT_EQ(f.extension.build_steps, 3);
		UT_ASSERT_EQ(test_candidate_acquire_calls, 9);
		UT_ASSERT_EQ(test_candidate_release_calls, 9);
		UT_ASSERT_EQ(test_candidate_held_count, 0);
		UT_ASSERT_EQ(operation_worker_allocations, allocations);
	}
}

/* Missing old targets may be restored on WALK without qualifying a creator. */
static void
operation_worker_missing_old_target(OperationWorkerFixture *f, unsigned role, unsigned state,
									bool same_segment)
{
	UndoSlotDirEntry directory;
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
	UndoUpdatePayload update;
	char old_tuple[60];
	char *bytes;
	char *body;
	uint16 body_length;
	Page page;

	operation_worker_cross_page_update(f, false, same_segment);
	page = (Page)f->cr.source.data;
	memcpy(&directory, test_operation_page.data + BLCKSZ - sizeof(directory), sizeof(directory));
	bytes = test_operation_page.data + directory.record_offset;
	body = bytes + sizeof(record);
	memcpy(&record, bytes, sizeof(record));
	memcpy(&update, body, sizeof(update));
	memcpy(old_tuple, body + sizeof(update), sizeof(old_tuple));
	memcpy(&history, body + sizeof(update) + sizeof(old_tuple), sizeof(history));
	memcpy(&operation, bytes + directory.record_length - sizeof(operation), sizeof(operation));
	if (role == 0) {
		UndoDeletePayload deleted
			= { .full_tuple_length = sizeof(old_tuple), .full_tuple_offset = sizeof(deleted) };

		record.record_type = UNDO_RECORD_DELETE;
		history.count = 1;
		memcpy(body, &deleted, sizeof(deleted));
		body_length = sizeof(deleted);
	} else {
		if (role == 2) {
			HeapTupleHeader replacement
				= (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1));

			update.new_block = 23;
			update.new_offset = 1;
			history.count = 1;
			HeapTupleHeaderSetXmin(replacement, record.xid);
		}
		memcpy(body, &update, sizeof(update));
		body_length = sizeof(update);
	}
	if (history.count == 1)
		memset(&history.entries[1], 0, sizeof(history.entries[1]));
	memcpy(body + body_length, old_tuple, sizeof(old_tuple));
	body_length += sizeof(old_tuple);
	memcpy(body + body_length, &history, sizeof(history));
	memcpy(body + body_length + sizeof(history), &operation, sizeof(operation));
	record.payload_length = body_length + sizeof(history) + sizeof(operation);
	memcpy(bytes, &record, sizeof(record));
	directory.record_type = record.record_type;
	directory.record_length = sizeof(record) + record.payload_length;
	memcpy(test_operation_page.data + BLCKSZ - sizeof(directory), &directory, sizeof(directory));
	((UndoBlockHeader *)test_operation_page.data)->free_offset
		= directory.record_offset + directory.record_length;
	if (state == 0)
		ItemIdSetUnused(PageGetItemId(page, 2));
	else if (state == 1)
		ItemIdSetDead(PageGetItemId(page, 2));
	else
		((PageHeader)page)->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	operation_worker_row_effect(0, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.commit_scn = 65; /* Synthetic visibility, not native pruning permission. */
}

UT_TEST(test_operation_worker_kept_missing_old_target_uses_original_inverse)
{
	for (unsigned role = 0; role < 3; role++)
		for (unsigned state = 0; state < 3; state++)
			for (int same = 0; same < 2; same++) {
				OperationWorkerFixture f;
				PGAlignedBlock original;
				ClusterR4CrBuildStepResult step;
				int allocations = operation_worker_allocations;

				operation_worker_missing_old_target(&f, role, state, same);
				memcpy(original.data, f.cr.source.data, BLCKSZ);
				step = operation_worker_finish(&f);
				cluster_cr_build_on_holder_forget(0, 1000);
				UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
				UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_NONE);
				UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
				UT_ASSERT_EQ(f.extension.build_steps, 1);
				UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
				UT_ASSERT_EQ(test_candidate_release_calls, 3);
				UT_ASSERT_EQ(test_candidate_held_count, 0);
				UT_ASSERT_EQ(operation_worker_allocations, allocations);
			}
}

/* Retained UPDATE projections preserve each local role independently. */
static void
update_role_challenge(OperationWorkerFixture *f, unsigned role, bool reuse, bool same, bool keep,
					  char old_root[60])
{
	UndoSlotDirEntry older_dir, newer_dir;
	UndoRecordHeader record;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer tail;
	UndoUpdatePayload update = { .new_block = 23,
								 .new_offset = 2,
								 .old_tuple_length = 60,
								 .old_tuple_offset = sizeof(update) };
	PGAlignedBlock newer;
	HeapTupleHeader root;
	UndoBlockHeader *block;
	char *bytes;

	operation_worker_reused_creation(f, reuse, same);
	if (!keep)
		operation_worker_row_effect(1, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	root = (HeapTupleHeader)PageGetItem((Page)f->cr.source.data,
										PageGetItemId((Page)f->cr.source.data, 1));
	memcpy(old_root, root, 60);
	memcpy(&older_dir, test_operation_page.data + BLCKSZ - sizeof(older_dir), sizeof(older_dir));
	memcpy(&newer_dir, test_operation_page.data + BLCKSZ - 2 * sizeof(newer_dir),
		   sizeof(newer_dir));
	memcpy(newer.data, test_operation_page.data + newer_dir.record_offset, newer_dir.record_length);
	bytes = test_operation_page.data + older_dir.record_offset;
	memcpy(&record, bytes, sizeof(record));
	memcpy(&history, bytes + sizeof(record) + sizeof(UndoInsertPayload), sizeof(history));
	memcpy(&tail, bytes + older_dir.record_length - sizeof(tail), sizeof(tail));
	record.record_type = UNDO_RECORD_UPDATE;
	record.target_block = role == 2 ? 24 : 23;
	record.target_offset = 1;
	record.payload_length = sizeof(update) + 60 + sizeof(history) + sizeof(tail);
	if (role == 2) {
		history.count = 2;
		history.entries[1] = history.entries[0];
		history.entries[0].block = 24;
		history.entries[0].slot_index = 6;
		memset(&history.entries[0].prior, 0, sizeof(history.entries[0].prior));
		history.entries[0].prior.wrap = 19;
	} else {
		HeapTupleHeaderSetXmax(root, record.xid);
		root->t_infomask &= ~HEAP_XMAX_INVALID;
		ItemPointerSet(&root->t_ctid, 23, 2);
		if (role == 1)
			root->t_infomask2 |= HEAP_HOT_UPDATED;
	}
	memcpy(bytes, &record, sizeof(record));
	memcpy(bytes + sizeof(record), &update, sizeof(update));
	memcpy(bytes + sizeof(record) + sizeof(update), old_root, 60);
	if (role == 2)
		ItemPointerSet(&((HeapTupleHeader)(bytes + sizeof(record) + sizeof(update)))->t_ctid, 24,
					   1);
	memcpy(bytes + sizeof(record) + sizeof(update) + 60, &history, sizeof(history));
	memcpy(bytes + sizeof(record) + sizeof(update) + 60 + sizeof(history), &tail, sizeof(tail));
	older_dir.record_type = record.record_type;
	older_dir.record_length = sizeof(record) + record.payload_length;
	newer_dir.record_offset = older_dir.record_offset + older_dir.record_length;
	memcpy(test_operation_page.data + newer_dir.record_offset, newer.data, newer_dir.record_length);
	memcpy(test_operation_page.data + BLCKSZ - sizeof(older_dir), &older_dir, sizeof(older_dir));
	memcpy(test_operation_page.data + BLCKSZ - 2 * sizeof(newer_dir), &newer_dir,
		   sizeof(newer_dir));
	block = (UndoBlockHeader *)test_operation_page.data;
	block->free_offset = newer_dir.record_offset + newer_dir.record_length;
}

static void
update_role_expected(char expected[BLCKSZ], const char source[BLCKSZ], const char old_root[60],
					 bool reuse, bool keep)
{
	ClusterItlSlotData *slots;

	memcpy(expected, source, BLCKSZ);
	memcpy(PageGetItem((Page)expected, PageGetItemId((Page)expected, 1)), old_root, 60);
	if (!keep)
		ItemIdSetUnused(PageGetItemId((Page)expected, 2));
	slots = ClusterPageGetItlSlots((Page)expected);
	if (!keep || !reuse)
		slots[2] = (ClusterItlSlotData){ .wrap = 8 };
	if (!keep && !reuse)
		slots[4] = (ClusterItlSlotData){ .wrap = 14 };
}

UT_TEST(test_update_role_kept_creation_preserves_only_new_role)
{
	for (unsigned role = 0; role < 3; role++)
		for (int reuse = 0; reuse < 2; reuse++)
			for (int same = 0; same < 2; same++) {
				OperationWorkerFixture f;
				PGAlignedBlock expected;
				char old_root[60];
				ClusterR4CrBuildStepResult step = CLUSTER_R4_CR_STEP_PENDING;
				int allocations = operation_worker_allocations;

				update_role_challenge(&f, role, reuse, same, true, old_root);
				update_role_expected(expected.data, f.cr.source.data, old_root, reuse, true);
				for (unsigned tick = 0; tick < 64 && f.extension.build_steps < 2; tick++) {
					step = operation_worker_step(&f);
					UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_PENDING);
				}
				UT_ASSERT_EQ(f.extension.build_steps, 2);
				UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
				UT_ASSERT_EQ(test_candidate_release_calls, 3);
				step = operation_worker_finish(&f);
				cluster_cr_build_on_holder_forget(0, 1000);
				UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
				UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
				UT_ASSERT_EQ(test_candidate_held_count, 0);
				UT_ASSERT_EQ(operation_worker_allocations, allocations);
			}
}

UT_TEST(test_update_role_unkept_creation_runs_whole_inverse)
{
	for (unsigned role = 0; role < 3; role++)
		for (int reuse = 0; reuse < 2; reuse++)
			for (int same = 0; same < 2; same++) {
				OperationWorkerFixture f;
				PGAlignedBlock expected;
				char old_root[60];
				ClusterR4CrBuildStepResult step;
				int allocations = operation_worker_allocations;

				update_role_challenge(&f, role, reuse, same, false, old_root);
				update_role_expected(expected.data, f.cr.source.data, old_root, reuse, false);
				step = operation_worker_finish(&f);
				cluster_cr_build_on_holder_forget(0, 1000);
				UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
				UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
				UT_ASSERT_EQ(f.extension.build_steps, 2);
				UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
				UT_ASSERT_EQ(test_candidate_release_calls, 6);
				UT_ASSERT_EQ(test_candidate_held_count, 0);
				UT_ASSERT_EQ(operation_worker_allocations, allocations);
			}
}

/* Add an independent retained effect on the UPDATE's old target. */
static void
update_role_later_root(OperationWorkerFixture *f, unsigned role, bool reuse, bool same,
					   bool lock_only)
{
	UndoSlotDirEntry directory;
	UndoRecordHeader record;
	UndoItlHistoryTrailer history = { 0 };
	UndoOperationTrailer tail;
	UndoBlockHeader *block;
	ClusterItlSlotData *head;
	HeapTupleHeader root;
	char old_root[60];
	char body[sizeof(UndoDeletePayload) + 60 + sizeof(UndoItlPayload)];
	char *bytes;
	uint16 length, offset;

	update_role_challenge(f, role, reuse, same, true, old_root);
	memcpy(&directory, test_operation_page.data + BLCKSZ - 2 * sizeof(directory),
		   sizeof(directory));
	bytes = test_operation_page.data + directory.record_offset;
	memcpy(&record, bytes, sizeof(record));
	memcpy(&tail, bytes + directory.record_length - sizeof(tail), sizeof(tail));
	head = &ClusterPageGetItlSlots((Page)f->cr.source.data)[reuse ? 2 : 4];
	root = (HeapTupleHeader)PageGetItem((Page)f->cr.source.data,
										PageGetItemId((Page)f->cr.source.data, 1));
	if (lock_only) {
		UndoItlPayload lock = { .itl_slot_idx = reuse ? 2 : 4,
								.prev_flags = head->flags,
								.new_flags = ITL_FLAG_LOCK_ONLY_ACTIVE,
								.lock_xid = record.xid,
								.prev_xmax = HeapTupleHeaderGetRawXmax(root),
								.prev_infomask = root->t_infomask,
								.prev_infomask2 = root->t_infomask2 };

		memcpy(body, &lock, sizeof(lock));
		length = sizeof(lock);
		root->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	} else {
		UndoDeletePayload deleted
			= { .full_tuple_length = 60, .full_tuple_offset = sizeof(deleted) };

		memcpy(body, &deleted, sizeof(deleted));
		memcpy(body + sizeof(deleted), root, 60);
		length = sizeof(deleted) + 60;
		root->t_infomask = 0;
		root->t_infomask2 &= ~HEAP_HOT_UPDATED;
		ItemPointerSet(&root->t_ctid, 23, 1);
	}
	HeapTupleHeaderSetXmax(root, record.xid);
	history.magic = UNDO_ITL_HISTORY_MAGIC;
	history.version = UNDO_ITL_HISTORY_VERSION;
	history.count = 1;
	history.entries[0].block = 23;
	history.entries[0].slot_index = reuse ? 2 : 4;
	history.entries[0].after_kind = lock_only ? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_ACTIVE;
	history.entries[0].after_write_scn = 71;
	history.entries[0].prior = *head;
	record.record_type = lock_only ? UNDO_RECORD_ITL : UNDO_RECORD_DELETE;
	record.target_offset = 1;
	record.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
	record.prev_uba = head->undo_segment_head;
	record.write_scn = 71;
	record.payload_length = length + sizeof(history) + sizeof(tail);
	tail.operation_sequence++;
	block = (UndoBlockHeader *)test_operation_page.data;
	offset = block->free_offset;
	bytes = test_operation_page.data + offset;
	memcpy(bytes, &record, sizeof(record));
	memcpy(bytes + sizeof(record), body, length);
	memcpy(bytes + sizeof(record) + length, &history, sizeof(history));
	memcpy(bytes + sizeof(record) + length + sizeof(history), &tail, sizeof(tail));
	directory.record_offset = offset;
	directory.record_length = sizeof(record) + record.payload_length;
	directory.record_type = record.record_type;
	directory.flags = record.flags;
	memcpy(test_operation_page.data + BLCKSZ - 3 * sizeof(directory), &directory,
		   sizeof(directory));
	block->slot_count = 3;
	block->free_offset = offset + directory.record_length;
	head->flags = lock_only ? ITL_FLAG_LOCK_ONLY_COMMITTED : ITL_FLAG_COMMITTED;
	head->wrap++;
	head->write_scn = 71;
	head->undo_segment_head = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET + 1, 2);
}

UT_TEST(test_update_role_kept_target_and_creation_are_independent)
{
	for (unsigned hot = 0; hot < 2; hot++)
		for (int reuse = 0; reuse < 2; reuse++)
			for (int same = 0; same < 2; same++) {
				OperationWorkerFixture f;
				PGAlignedBlock expected;
				ClusterR4CrBuildStepResult step = CLUSTER_R4_CR_STEP_PENDING;
				int allocations = operation_worker_allocations;

				update_role_later_root(&f, hot, reuse, same, false);
				memcpy(expected.data, f.cr.source.data, BLCKSZ);
				if (!reuse)
					ClusterPageGetItlSlots((Page)expected.data)[2]
						= (ClusterItlSlotData){ .wrap = 8 };
				for (unsigned tick = 0; tick < 128 && f.extension.build_steps < 3; tick++)
					UT_ASSERT_EQ(operation_worker_step(&f), CLUSTER_R4_CR_STEP_PENDING);
				UT_ASSERT_EQ(f.extension.build_steps, 3);
				UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
				UT_ASSERT_EQ(test_candidate_release_calls, 6);
				step = operation_worker_finish(&f);
				cluster_cr_build_on_holder_forget(0, 1000);
				UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
				UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
				UT_ASSERT_EQ(test_candidate_acquire_calls, 9);
				UT_ASSERT_EQ(test_candidate_release_calls, 9);
				UT_ASSERT_EQ(test_candidate_held_count, 0);
				UT_ASSERT_EQ(operation_worker_allocations, allocations);
			}
}

UT_TEST(test_update_role_kept_lock_is_not_full_target_coverage)
{
	for (unsigned hot = 0; hot < 2; hot++)
		for (int reuse = 0; reuse < 2; reuse++)
			for (int same = 0; same < 2; same++) {
				OperationWorkerFixture f;
				PGAlignedBlock original;
				ClusterR4CrBuildStepResult step;
				int allocations = operation_worker_allocations;

				update_role_later_root(&f, hot, reuse, same, true);
				memcpy(original.data, f.cr.source.data, BLCKSZ);
				step = operation_worker_finish(&f);
				cluster_cr_build_on_holder_forget(0, 1000);
				UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
				UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
				UT_ASSERT_EQ(memcmp(original.data, f.cr.source.data, BLCKSZ), 0);
				UT_ASSERT_EQ(f.extension.build_steps, 3);
				UT_ASSERT_EQ(test_candidate_acquire_calls, 9);
				UT_ASSERT_EQ(test_candidate_release_calls, 9);
				UT_ASSERT_EQ(test_candidate_held_count, 0);
				UT_ASSERT_EQ(operation_worker_allocations, allocations);
			}
}

/* These retained chains grant neither native pruning nor TID reuse. */
static void
update_creation_latest_lock(OperationWorkerFixture *f, unsigned role, bool reuse, bool same,
							bool discarded, char before_lock[BLCKSZ])
{
	UndoSlotDirEntry directory;
	UndoRecordHeader record;
	UndoOperationTrailer tail;
	UndoItlHistoryTrailer history = { 0 };
	UndoItlPayload lock;
	ClusterItlSlotData *head;
	HeapTupleHeader tuple;
	UndoBlockHeader *block;
	Page page;
	char *bytes;
	uint16 offset;
	OffsetNumber target = role == 3 ? 1 : 2;

	operation_worker_reused_update_creation(f, role, reuse, same);
	page = (Page)f->cr.source.data;
	tuple = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, target));
	if (role == 1)
		tuple->t_infomask2 |= HEAP_ONLY_TUPLE;
	memcpy(before_lock, page, BLCKSZ);
	memcpy(&directory, test_operation_page.data + BLCKSZ - 2 * sizeof(directory),
		   sizeof(directory));
	bytes = test_operation_page.data + directory.record_offset;
	memcpy(&record, bytes, sizeof(record));
	memcpy(&tail, bytes + directory.record_length - sizeof(tail), sizeof(tail));
	head = &ClusterPageGetItlSlots(page)[reuse ? 2 : 4];
	lock = (UndoItlPayload){ .itl_slot_idx = reuse ? 2 : 4,
							 .prev_flags = head->flags,
							 .new_flags = ITL_FLAG_LOCK_ONLY_ACTIVE,
							 .lock_xid = record.xid,
							 .prev_xmax = HeapTupleHeaderGetRawXmax(tuple),
							 .prev_infomask = tuple->t_infomask,
							 .prev_infomask2 = tuple->t_infomask2 };
	history.magic = UNDO_ITL_HISTORY_MAGIC;
	history.version = UNDO_ITL_HISTORY_VERSION;
	history.count = 1;
	history.entries[0].block = 23;
	history.entries[0].slot_index = reuse ? 2 : 4;
	history.entries[0].after_kind = ITL_FLAG_LOCK_ONLY_ACTIVE;
	history.entries[0].after_write_scn = 71;
	history.entries[0].prior = *head;
	record.record_type = UNDO_RECORD_ITL;
	record.target_block = 23;
	record.target_offset = target;
	record.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
	record.prev_uba = head->undo_segment_head;
	record.write_scn = 71;
	record.payload_length = sizeof(lock) + sizeof(history) + sizeof(tail);
	tail.operation_sequence++;
	tail.effect_state
		= discarded ? CLUSTER_SAVEPOINT_EFFECT_DISCARDED : CLUSTER_SAVEPOINT_EFFECT_LIVE;
	block = (UndoBlockHeader *)test_operation_page.data;
	offset = block->free_offset;
	bytes = test_operation_page.data + offset;
	memcpy(bytes, &record, sizeof(record));
	memcpy(bytes + sizeof(record), &lock, sizeof(lock));
	memcpy(bytes + sizeof(record) + sizeof(lock), &history, sizeof(history));
	memcpy(bytes + sizeof(record) + sizeof(lock) + sizeof(history), &tail, sizeof(tail));
	directory.record_offset = offset;
	directory.record_length = sizeof(record) + record.payload_length;
	directory.record_type = record.record_type;
	directory.flags = record.flags;
	memcpy(test_operation_page.data + BLCKSZ - 3 * sizeof(directory), &directory,
		   sizeof(directory));
	block->slot_count = 3;
	block->free_offset = offset + directory.record_length;
	head->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	head->wrap++;
	head->write_scn = 71;
	head->undo_segment_head = uba_encode(TEST_RECORD_SEGMENT, 7, TEST_TT_OFFSET + 1, 2);
	HeapTupleHeaderSetXmax(tuple, record.xid);
	tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
}

/* Reach the older creation only after both newer records were admitted. */
static void
update_lock_before_older_creation(OperationWorkerFixture *f)
{
	for (unsigned tick = 0; tick < 128 && f->extension.build_steps < 3; tick++)
		UT_ASSERT_EQ(operation_worker_step(f), CLUSTER_R4_CR_STEP_PENDING);
	UT_ASSERT_EQ(f->extension.build_steps, 3);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	UT_ASSERT_EQ(test_candidate_release_calls, 6);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
}

static void
update_lock_assert_full(OperationWorkerFixture *f, const char expected[BLCKSZ], int allocations)
{
	ClusterR4CrBuildStepResult step = operation_worker_finish(f);

	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(f->reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(memcmp(expected, f->cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(f->extension.build_steps, 3);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 9);
	UT_ASSERT_EQ(test_candidate_release_calls, 9);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(operation_worker_allocations, allocations);
}

UT_TEST(test_update_creation_latest_live_lock_keeps_creator)
{
	for (unsigned role = 0; role < 3; role++)
		for (int reuse = 0; reuse < 2; reuse++)
			for (int same = 0; same < 2; same++) {
				OperationWorkerFixture f;
				PGAlignedBlock expected, before_lock;
				int allocations = operation_worker_allocations;

				update_creation_latest_lock(&f, role, reuse, same, false, before_lock.data);
				memcpy(expected.data, f.cr.source.data, BLCKSZ);
				update_lock_before_older_creation(&f);
				UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
				if (!reuse)
					ClusterPageGetItlSlots((Page)expected.data)[2]
						= (ClusterItlSlotData){ .wrap = 8 };
				update_lock_assert_full(&f, expected.data, allocations);
			}
}

UT_TEST(test_update_creation_discarded_lock_does_not_hide_creator)
{
	for (unsigned role = 0; role < 3; role++)
		for (int reuse = 0; reuse < 2; reuse++)
			for (int same = 0; same < 2; same++) {
				OperationWorkerFixture f;
				PGAlignedBlock expected;
				int allocations = operation_worker_allocations;

				update_creation_latest_lock(&f, role, reuse, same, true, expected.data);
				update_lock_before_older_creation(&f);
				UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
				if (!reuse)
					ClusterPageGetItlSlots((Page)expected.data)[2]
						= (ClusterItlSlotData){ .wrap = 8 };
				update_lock_assert_full(&f, expected.data, allocations);
			}
}

UT_TEST(test_update_creation_late_lock_and_creator_run_whole_inverse)
{
	for (unsigned role = 0; role < 3; role++)
		for (int reuse = 0; reuse < 2; reuse++)
			for (int same = 0; same < 2; same++) {
				OperationWorkerFixture f;
				PGAlignedBlock expected;
				UndoSlotDirEntry directory;
				int allocations = operation_worker_allocations;

				update_creation_latest_lock(&f, role, reuse, same, false, expected.data);
				test_mixed_top_slot.commit_scn = 80;
				ItemIdSetUnused(PageGetItemId((Page)expected.data, 2));
				memcpy(&directory, test_operation_page.data + BLCKSZ - 2 * sizeof(directory),
					   sizeof(directory));
				if (role != 2)
					memcpy(PageGetItem((Page)expected.data, PageGetItemId((Page)expected.data, 1)),
						   test_operation_page.data + directory.record_offset
							   + sizeof(UndoRecordHeader) + sizeof(UndoUpdatePayload),
						   60);
				ClusterPageGetItlSlots((Page)expected.data)[2] = (ClusterItlSlotData){ .wrap = 8 };
				if (!reuse)
					ClusterPageGetItlSlots((Page)expected.data)[4]
						= (ClusterItlSlotData){ .wrap = 14 };
				update_lock_before_older_creation(&f);
				update_lock_assert_full(&f, expected.data, allocations);
			}
}

UT_TEST(test_update_creation_locked_old_page_does_not_bind_new_role)
{
	for (int discarded = 0; discarded < 2; discarded++)
		for (int reuse = 0; reuse < 2; reuse++)
			for (int same = 0; same < 2; same++) {
				OperationWorkerFixture f;
				PGAlignedBlock expected, before_lock;
				int allocations = operation_worker_allocations;

				update_creation_latest_lock(&f, 3, reuse, same, discarded, before_lock.data);
				memcpy(expected.data, discarded ? before_lock.data : f.cr.source.data, BLCKSZ);
				update_lock_before_older_creation(&f);
				UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
				ItemIdSetUnused(PageGetItemId((Page)expected.data, 2));
				if (!reuse)
					ClusterPageGetItlSlots((Page)expected.data)[2]
						= (ClusterItlSlotData){ .wrap = 8 };
				update_lock_assert_full(&f, expected.data, allocations);
			}
}

/* ACTIVE is sampled through the original exact canonical TT/CLOG path.
 * These SCN/native boundaries are a model, not a native commit race. */
static void
update_creation_active_lock(OperationWorkerFixture *f, unsigned role, bool reuse, bool same,
							bool discarded, char before_lock[BLCKSZ], char expected[BLCKSZ])
{
	UndoSlotDirEntry directory;
	UndoItlHistoryTrailer history;
	UndoItlPayload lock;
	ClusterItlSlotData *slot;
	char *bytes;

	update_creation_latest_lock(f, role, reuse, same, discarded, before_lock);
	test_mixed_top_slot.status = TT_SLOT_ACTIVE;
	test_mixed_top_slot.commit_scn = InvalidScn;
	test_mixed_top_native_status = TRANSACTION_STATUS_IN_PROGRESS;
	test_twophase_xid = TEST_ORIGIN_XID + 2;
	slot = &ClusterPageGetItlSlots((Page)f->cr.source.data)[reuse ? 2 : 4];
	slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	slot->commit_scn = InvalidScn;
	slot = &ClusterPageGetItlSlots((Page)before_lock)[reuse ? 2 : 4];
	slot->flags = ITL_FLAG_ACTIVE;
	slot->commit_scn = InvalidScn;
	memcpy(&directory, test_operation_page.data + BLCKSZ - 3 * sizeof(directory),
		   sizeof(directory));
	bytes = test_operation_page.data + directory.record_offset + sizeof(UndoRecordHeader);
	memcpy(&lock, bytes, sizeof(lock));
	lock.prev_flags = ITL_FLAG_ACTIVE;
	memcpy(bytes, &lock, sizeof(lock));
	bytes += sizeof(lock);
	memcpy(&history, bytes, sizeof(history));
	history.entries[0].prior.flags = ITL_FLAG_ACTIVE;
	history.entries[0].prior.commit_scn = InvalidScn;
	memcpy(bytes, &history, sizeof(history));
	memcpy(expected, before_lock, BLCKSZ);
	ItemIdSetUnused(PageGetItemId((Page)expected, 2));
	memcpy(&directory, test_operation_page.data + BLCKSZ - 2 * sizeof(directory),
		   sizeof(directory));
	if (role != 2)
		memcpy(PageGetItem((Page)expected, PageGetItemId((Page)expected, 1)),
			   test_operation_page.data + directory.record_offset + sizeof(UndoRecordHeader)
				   + sizeof(UndoUpdatePayload),
			   60);
	ClusterPageGetItlSlots((Page)expected)[2] = (ClusterItlSlotData){ .wrap = 8 };
	if (!reuse)
		ClusterPageGetItlSlots((Page)expected)[4] = (ClusterItlSlotData){ .wrap = 14 };
}

/* Release completes before apply; wait for the actual full first inverse. */
static void
update_creation_active_first_inverse(OperationWorkerFixture *f, const char before_lock[BLCKSZ])
{
	for (unsigned tick = 0; tick < 128 && memcmp(before_lock, f->cr.source.data, BLCKSZ) != 0;
		 tick++)
		UT_ASSERT_EQ(operation_worker_step(f), CLUSTER_R4_CR_STEP_PENDING);
	UT_ASSERT_EQ(memcmp(before_lock, f->cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(f->extension.build_steps, 1);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 3);
	UT_ASSERT_EQ(test_candidate_release_calls, 3);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(test_twophase_calls, 1);
	UT_ASSERT_EQ(test_current_owner_calls, 0);
	UT_ASSERT_EQ(test_local_binding_calls, 0);
}

UT_TEST(test_update_creation_active_lock_and_creator_remain_invisible)
{
	for (unsigned role = 0; role < 4; role++)
		for (unsigned layout = 0; layout < 4; layout++)
			for (int discarded = 0; discarded < 2; discarded++) {
				OperationWorkerFixture f;
				PGAlignedBlock before_lock, expected;
				int allocations = operation_worker_allocations;

				update_creation_active_lock(&f, role, layout & 1, layout & 2, discarded,
											before_lock.data, expected.data);
				update_creation_active_first_inverse(&f, before_lock.data);
				update_lock_assert_full(&f, expected.data, allocations);
				UT_ASSERT_EQ(test_twophase_calls, 2);
			}
}

UT_TEST(test_update_creation_active_sample_survives_visible_commit)
{
	for (unsigned role = 0; role < 4; role++)
		for (unsigned layout = 0; layout < 4; layout++)
			for (int discarded = 0; discarded < 2; discarded++) {
				OperationWorkerFixture f;
				PGAlignedBlock before_lock, expected;
				int allocations = operation_worker_allocations;

				update_creation_active_lock(&f, role, layout & 1, layout & 2, discarded,
											before_lock.data, expected.data);
				update_creation_active_first_inverse(&f, before_lock.data);
				test_mixed_top_slot.status = TT_SLOT_COMMITTED;
				test_mixed_top_slot.commit_scn = 65;
				test_mixed_top_native_status = TRANSACTION_STATUS_COMMITTED;
				update_lock_assert_full(&f, expected.data, allocations);
				UT_ASSERT_EQ(test_twophase_calls, 1);
			}
}

UT_TEST(test_update_creation_active_canonical_drift_preserves_last_inverse)
{
	for (unsigned layout = 0; layout < 4; layout++)
		for (unsigned fault = 0; fault < 4; fault++) {
			OperationWorkerFixture f;
			PGAlignedBlock before_lock, expected;
			ClusterR4CrBuildStepResult step;
			int allocations = operation_worker_allocations;

			update_creation_active_lock(&f, 2, layout & 1, layout & 2, false, before_lock.data,
										expected.data);
			update_creation_active_first_inverse(&f, before_lock.data);
			if (fault == 0)
				test_mixed_top_slot.wrap++;
			else if (fault == 1)
				test_mixed_top_slot.xid++;
			else if (fault == 2)
				test_mixed_top_slot.status = TT_SLOT_UNUSED;
			else
				operation_worker_row_effect(1, 0); /* No valid disposition. */
			step = operation_worker_finish(&f);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
			UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
			UT_ASSERT_EQ(memcmp(before_lock.data, f.cr.source.data, BLCKSZ), 0);
			UT_ASSERT_EQ(f.extension.build_steps, 2);
			UT_ASSERT_EQ(test_candidate_acquire_calls, fault == 3 ? 4 : 5);
			UT_ASSERT_EQ(test_candidate_release_calls, test_candidate_acquire_calls);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
			UT_ASSERT_EQ(operation_worker_allocations, allocations);
		}
}

/* Controlled page geometry, without granting native pruning or TID reuse.
 * Compact inputs have room for the root only after removing replacement2. */
static void
update_repack_fixture(OperationWorkerFixture *f, unsigned role, unsigned layout, bool compact,
					  bool keep, bool dead, char old_root[60])
{
	PGAlignedBlock filler;
	HeapTupleHeader tuple = (HeapTupleHeader)filler.data;
	Page page;
	PageHeader header;
	Size length;

	update_role_challenge(f, role, layout & 1, layout & 2, keep, old_root);
	page = (Page)f->cr.source.data;
	if (dead)
		ItemIdSetDead(PageGetItemId(page, 1));
	else
		ItemIdSetUnused(PageGetItemId(page, 1));
	header = (PageHeader)page;
	if (compact) {
		PageRepairFragmentation(page);
		UT_ASSERT_EQ(PageGetMaxOffsetNumber(page), 2);
		ItemIdSetUnused((ItemId)((char *)page + header->pd_lower));
		header->pd_lower += sizeof(ItemIdData); /* Restore explicit placeholder3. */
	}
	length = MAXALIGN_DOWN(header->pd_upper - header->pd_lower - sizeof(ItemIdData) - 16);
	memset(filler.data, 0x5a, BLCKSZ);
	memset(tuple, 0, SizeofHeapTupleHeader);
	tuple->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	tuple->t_infomask = HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tuple, TEST_ORIGIN_XID - 10);
	ItemPointerSet(&tuple->t_ctid, 23, 4);
	UT_ASSERT_EQ(PageAddItem(page, filler.data, length, 4, false, true), 4);
	UT_ASSERT_EQ(header->pd_upper - header->pd_lower, 16);
	UT_ASSERT_EQ(ItemIdGetLength(PageGetItemId(page, 2)), 60);
}

/* Fixed fixture layout: live replacement2, filler4, then restored root1.
 * Derive no expected bytes through an inverse or the compaction routine. */
static void
update_repack_expected(char expected[BLCKSZ], const char source[BLCKSZ], const char old_root[60],
					   bool reuse, bool keep)
{
	Page original = (Page)source;
	Page page = (Page)expected;
	ItemId filler = PageGetItemId(original, 4);
	PageHeader header;
	Offset upper;

	memcpy(expected, source, BLCKSZ);
	header = (PageHeader)page;
	upper = header->pd_special;
	if (keep) {
		upper -= MAXALIGN(60);
		memcpy(expected + upper, PageGetItem(original, PageGetItemId(original, 2)), MAXALIGN(60));
		ItemIdSetNormal(PageGetItemId(page, 2), upper, 60);
	} else
		ItemIdSetUnused(PageGetItemId(page, 2));
	upper -= ItemIdGetLength(filler);
	memcpy(expected + upper, PageGetItem(original, filler), ItemIdGetLength(filler));
	ItemIdSetNormal(PageGetItemId(page, 4), upper, ItemIdGetLength(filler));
	upper -= MAXALIGN(60);
	memcpy(expected + upper, old_root, 60);
	ItemIdSetNormal(PageGetItemId(page, 1), upper, 60);
	header->pd_upper = upper;
	PageSetHasFreeLinePointers(page);
	if (!keep || !reuse)
		ClusterPageGetItlSlots(page)[2] = (ClusterItlSlotData){ .wrap = 8 };
	if (!keep && !reuse)
		ClusterPageGetItlSlots(page)[4] = (ClusterItlSlotData){ .wrap = 14 };
}

static void
update_repack_assert_full(OperationWorkerFixture *f, const char expected[BLCKSZ], int allocations)
{
	ClusterR4CrBuildStepResult step = operation_worker_finish(f);

	cluster_cr_build_on_holder_forget(0, 1000);
	UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FULL);
	UT_ASSERT_EQ(f->reason, CLUSTER_CR_BUILD_NONE);
	UT_ASSERT_EQ(memcmp(expected, f->cr.source.data, BLCKSZ), 0);
	UT_ASSERT_EQ(f->extension.build_steps, 2);
	UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
	UT_ASSERT_EQ(test_candidate_release_calls, 6);
	UT_ASSERT_EQ(test_candidate_held_count, 0);
	UT_ASSERT_EQ(operation_worker_allocations, allocations);
}

UT_TEST(test_update_repack_kept_creation_uses_result_tuple_positions)
{
	for (unsigned role = 0; role < 2; role++)
		for (unsigned layout = 0; layout < 4; layout++)
			for (int dead = 0; dead < 2; dead++) {
				OperationWorkerFixture f;
				PGAlignedBlock expected;
				char old_root[60];
				Offset before;
				int allocations = operation_worker_allocations;

				update_repack_fixture(&f, role, layout, false, true, dead, old_root);
				before = ItemIdGetOffset(PageGetItemId((Page)f.cr.source.data, 2));
				update_repack_expected(expected.data, f.cr.source.data, old_root, layout & 1, true);
				operation_worker_creation_first(&f);
				update_repack_assert_full(&f, expected.data, allocations);
				UT_ASSERT_NE(ItemIdGetOffset(PageGetItemId((Page)f.cr.source.data, 2)), before);
			}
}

UT_TEST(test_update_repack_full_result_refuses_without_partial_publish)
{
	for (unsigned role = 0; role < 2; role++)
		for (unsigned layout = 0; layout < 4; layout++)
			for (int dead = 0; dead < 2; dead++) {
				OperationWorkerFixture f;
				PGAlignedBlock before;
				char old_root[60];
				ClusterR4CrBuildStepResult step;
				int allocations = operation_worker_allocations;

				update_repack_fixture(&f, role, layout, true, true, dead, old_root);
				memcpy(before.data, f.cr.source.data, BLCKSZ);
				operation_worker_creation_first(&f);
				step = operation_worker_finish(&f);
				cluster_cr_build_on_holder_forget(0, 1000);
				UT_ASSERT_EQ(step, CLUSTER_R4_CR_STEP_FAIL);
				UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
				UT_ASSERT_EQ(memcmp(before.data, f.cr.source.data, BLCKSZ), 0);
				UT_ASSERT_EQ(f.extension.build_steps, 2);
				UT_ASSERT_EQ(test_candidate_acquire_calls, 6);
				UT_ASSERT_EQ(test_candidate_release_calls, 6);
				UT_ASSERT_EQ(test_candidate_held_count, 0);
				UT_ASSERT_EQ(operation_worker_allocations, allocations);
			}
}

UT_TEST(test_update_repack_unkept_creation_leaves_restore_capacity)
{
	for (unsigned role = 0; role < 2; role++)
		for (unsigned layout = 0; layout < 4; layout++)
			for (int dead = 0; dead < 2; dead++) {
				OperationWorkerFixture f;
				PGAlignedBlock expected;
				char old_root[60];
				int allocations = operation_worker_allocations;

				update_repack_fixture(&f, role, layout, true, false, dead, old_root);
				update_repack_expected(expected.data, f.cr.source.data, old_root, layout & 1,
									   false);
				update_repack_assert_full(&f, expected.data, allocations);
				UT_ASSERT(!ItemIdIsUsed(PageGetItemId((Page)f.cr.source.data, 2)));
			}
}

static UBA
operation_worker_multidata_address(unsigned row)
{
	return uba_encode(TEST_RECORD_SEGMENT, 7 + row / 8, TEST_TT_OFFSET, row % 8);
}

/* Preserve each original record; only repack its DATA address and prior head. */
static void
operation_worker_multidata(OperationWorkerFixture *f, unsigned mode, unsigned layout)
{
	PGAlignedBlock original;

	operation_worker_long_chain(f, layout & 1, mode);
	memcpy(original.data, test_operation_page.data, BLCKSZ);
	memset(test_operation_page.data, 0, BLCKSZ);
	memset(test_operation_multidata_pages, 0, sizeof(test_operation_multidata_pages));
	for (unsigned row = 0; row < OPERATION_WORKER_LONG_RECORDS; row++) {
		unsigned index = row / 8, local = row % 8;
		char *page = index == 0 ? test_operation_page.data
								: test_operation_multidata_pages[index - 1].data;
		UndoBlockHeader *header = (UndoBlockHeader *)page;
		UndoSlotDirEntry directory;
		UndoRecordHeader record;
		UndoItlHistoryTrailer history;
		char *bytes;

		memcpy(&directory, original.data + BLCKSZ - (row + 1) * sizeof(directory),
			   sizeof(directory));
		if (local == 0)
			memcpy(page, original.data, sizeof(*header));
		bytes = page + sizeof(*header) + local * directory.record_length;
		memcpy(bytes, original.data + directory.record_offset, directory.record_length);
		memcpy(&record, bytes, sizeof(record));
		memcpy(&history, bytes + sizeof(record) + sizeof(UndoInsertPayload), sizeof(history));
		record.prev_uba
			= row == 0 ? (UBA)InvalidUba_init : operation_worker_multidata_address(row - 1);
		if (row != 0)
			history.entries[0].prior.undo_segment_head = record.prev_uba;
		memcpy(bytes, &record, sizeof(record));
		memcpy(bytes + sizeof(record) + sizeof(UndoInsertPayload), &history, sizeof(history));
		directory.record_offset = bytes - page;
		memcpy(page + BLCKSZ - (local + 1) * sizeof(directory), &directory, sizeof(directory));
		header->slot_count = local + 1;
		header->free_offset = directory.record_offset + directory.record_length;
		UT_ASSERT(header->free_offset < BLCKSZ - header->slot_count * sizeof(directory));
	}
	ClusterPageGetItlSlots((Page)f->cr.source.data)[2].undo_segment_head
		= operation_worker_multidata_address(23);
	test_operation_multidata_enabled = true;
	test_operation_multidata_cold = false;
	test_operation_multidata_mask = 7;
	test_operation_multidata_request_count = 0;
}

static void
operation_worker_multidata_requests(unsigned records)
{
	UT_ASSERT_EQ(test_operation_multidata_request_count, records * 2);
	for (unsigned i = 0; i < records * 2; i++)
		UT_ASSERT_EQ(test_operation_multidata_requests[i], 9 - i / 16);
	UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
	UT_ASSERT_EQ(test_candidate_data_unpin_calls, test_candidate_data_pin_calls);
	UT_ASSERT_EQ(test_operation_pin_depth, 0);
	UT_ASSERT_EQ(test_undo_read_calls, records);
}

UT_TEST(test_operation_worker_multidata_preserves_literal_survivors)
{
	for (unsigned mode = 0; mode < 3; mode++)
		for (unsigned layout = 0; layout < 2; layout++) {
			OperationWorkerFixture f;
			PGAlignedBlock expected;
			int allocations = operation_worker_allocations;
			uint32 kept = mode == 0 ? 0 : mode == 1 ? 0xffffffU : 0x555555U;

			operation_worker_multidata(&f, mode, layout);
			operation_worker_long_expected(expected.data, f.cr.source.data, kept);
			if (mode == 2)
				ClusterPageGetItlSlots((Page)expected.data)[2].undo_segment_head
					= operation_worker_multidata_address(22);
			UT_ASSERT_EQ(operation_worker_long_finish(&f), CLUSTER_R4_CR_STEP_FULL);
			UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_NONE);
			UT_ASSERT_EQ(memcmp(expected.data, f.cr.source.data, BLCKSZ), 0);
			UT_ASSERT_EQ(f.extension.build_steps, 24);
			UT_ASSERT_EQ(test_candidate_acquire_calls, 72);
			UT_ASSERT_EQ(test_candidate_release_calls, 72);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
			UT_ASSERT_EQ(test_candidate_max_held_count, 1);
			operation_worker_multidata_requests(24);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(operation_worker_allocations, allocations);
		}
}

static void
operation_worker_multidata_break_middle(unsigned fault)
{
	char *page = test_operation_multidata_pages[0].data;
	UndoSlotDirEntry directory;
	UndoRecordHeader record;

	if (fault == 0)
		test_operation_multidata_mask &= ~2U;
	else if (fault == 1)
		((UndoBlockHeader *)page)->magic = 0;
	else if (fault == 2)
		((UndoBlockHeader *)page)->slot_count = 7;
	else if (fault == 4)
		test_operation_multidata_cold = true;
	else {
		memcpy(&directory, page + BLCKSZ - 8 * sizeof(directory), sizeof(directory));
		memcpy(&record, page + directory.record_offset, sizeof(record));
		record.prev_uba = operation_worker_multidata_address(16);
		memcpy(page + directory.record_offset, &record, sizeof(record));
	}
}

static void
operation_worker_multidata_stage_middle(OperationWorkerFixture *f, bool fresh)
{
	operation_worker_long_advance(f, 8);
	operation_worker_multidata_requests(8);
	/* The preliminary bytes never substitute for either held DATA read. */
	for (unsigned tick = 0; tick < 16 && f->extension.build_steps < 9; tick++)
		UT_ASSERT_EQ(operation_worker_step(f), CLUSTER_R4_CR_STEP_PENDING);
	if (fresh)
		for (unsigned tick = 0; tick < 16 && test_candidate_acquire_calls < 27; tick++)
			UT_ASSERT_EQ(operation_worker_step(f), CLUSTER_R4_CR_STEP_PENDING);
	UT_ASSERT_EQ(f->extension.build_steps, 9);
	UT_ASSERT_EQ(test_undo_read_calls, 9);
	UT_ASSERT_EQ(test_candidate_acquire_calls, fresh ? 27 : 24);
	UT_ASSERT_EQ(test_candidate_release_calls, fresh ? 26 : 24);
	UT_ASSERT_EQ(test_candidate_held_count, fresh ? 1 : 0);
	UT_ASSERT_EQ(test_operation_multidata_request_count, fresh ? 17 : 16);
}

UT_TEST(test_operation_worker_multidata_refuses_broken_middle_page)
{
	for (unsigned fault = 0; fault < 5; fault++)
		for (unsigned layout = 0; layout < 4; layout++) {
			OperationWorkerFixture f;
			PGAlignedBlock remaining;
			int allocations = operation_worker_allocations;
			unsigned requests;

			operation_worker_multidata(&f, 0, layout);
			operation_worker_multidata_stage_middle(&f, layout & 2);
			requests = test_operation_multidata_request_count;
			memcpy(remaining.data, f.cr.source.data, BLCKSZ);
			operation_worker_multidata_break_middle(fault);
			UT_ASSERT_EQ(operation_worker_long_finish(&f), CLUSTER_R4_CR_STEP_FAIL);
			UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
			UT_ASSERT_EQ(memcmp(remaining.data, f.cr.source.data, BLCKSZ), 0);
			UT_ASSERT_EQ(f.extension.build_steps, 9);
			UT_ASSERT_EQ(test_candidate_acquire_calls, layout & 2 ? 27 : 25);
			UT_ASSERT_EQ(test_candidate_release_calls, layout & 2 ? 27 : 25);
			UT_ASSERT_EQ(test_candidate_held_count, 0);
			UT_ASSERT_EQ(test_candidate_max_held_count, 1);
			UT_ASSERT_EQ(test_operation_multidata_request_count - requests, 1);
			for (unsigned i = requests; i < test_operation_multidata_request_count; i++)
				UT_ASSERT_EQ(test_operation_multidata_requests[i], 8);
			UT_ASSERT_EQ(test_candidate_data_pin_calls, 0);
			UT_ASSERT_EQ(test_candidate_data_unpin_calls, 0);
			UT_ASSERT_EQ(test_operation_pin_depth, 0);
			UT_ASSERT_EQ(test_undo_read_calls, 9);
			cluster_cr_build_on_holder_forget(0, 1000);
			UT_ASSERT_EQ(operation_worker_allocations, allocations);
		}
}
