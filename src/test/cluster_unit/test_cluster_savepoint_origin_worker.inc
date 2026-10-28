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

void *
MemoryContextAllocZero(MemoryContext context pg_attribute_unused(), Size size)
{
	return calloc(1, size);
}

void
pfree(void *pointer)
{
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

UT_TEST(test_operation_worker_surviving_live_refuses_full)
{
	OperationWorkerFixture f;
	PGAlignedBlock original;

	operation_worker_init(&f, false);
	/* Isolate the surviving-LIVE branch; the SCN/native state is synthetic. */
	operation_plan_effect(CLUSTER_SAVEPOINT_EFFECT_LIVE);
	test_tt_slot.commit_scn = 65;
	memcpy(original.data, f.cr.source.data, BLCKSZ);
	UT_ASSERT_EQ(operation_worker_finish(&f), CLUSTER_R4_CR_STEP_FAIL);
	UT_ASSERT_EQ(f.reason, CLUSTER_CR_BUILD_BAD_UNDO);
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
