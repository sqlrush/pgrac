/*-------------------------------------------------------------------------
 * test_cluster_heap_prepare_diagnostic.c
 *    Exact production prepare refusals and capacity-wait requalification.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * The extracted production helper is the unit under test. Capacity/census,
 * PCM and receipt services are explicit boundaries, not a live replay.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/heapam.h"
#include "cluster/cluster_undo_record_api.h"
#include "../../backend/access/heap/heapam_r4_private.h"

#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

/* Only passed to the controlled guard/target services by this helper. */
typedef struct ClusterHeapDmlAuthorityGuard {
	int unused;
} ClusterHeapDmlAuthorityGuard;
static int fault;
static bool capacity_requested;
static bool request_lock_only = true;
static int wait_calls;
static bool content_unlocked;
static ClusterTxwResult wait_result;
static jmp_buf wait_error_jump;
static bool wait_error;

/* Error reporting is an explicit fixture boundary, not a new product policy. */
#undef ereport
#define ereport(level, rest)                                                                       \
	do {                                                                                           \
		wait_error = true;                                                                         \
		longjmp(wait_error_jump, 1);                                                               \
	} while (0)

static ClusterTxwResult
cluster_heap_itl_wait_capacity_after_census(Buffer old_buffer, Buffer new_buffer,
											Buffer full_buffer, TransactionId xid, bool lock_only,
											uint64 *deadline, const char **reason)
{
	wait_calls++;
	UT_ASSERT_EQ(old_buffer, 1);
	UT_ASSERT_EQ(new_buffer, old_buffer);
	UT_ASSERT_EQ(full_buffer, old_buffer);
	UT_ASSERT_EQ(xid, 700);
	UT_ASSERT(lock_only);
	UT_ASSERT_EQ(*deadline, 12345);
	*reason = "CONTROLLED_WAIT_RESULT";
	return wait_result;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}

static ClusterHeapItlCapacityResult
cluster_heap_itl_ensure_capacity_with_terminal_census(Buffer buffer, TransactionId xid,
													  bool lock_only)
{
	return fault == 1	? CLUSTER_HEAP_ITL_CAPACITY_EXHAUSTED_OR_REFUSED
		   : fault == 2 ? CLUSTER_HEAP_ITL_CAPACITY_RETRY_REQUALIFY
						: CLUSTER_HEAP_ITL_CAPACITY_READY;
}

static bool
cluster_heap_dml_authority_guard_capture(Buffer buffer, HeapTuple tuple,
										 ClusterHeapDmlAuthorityGuard *guard)
{
	return fault != 3;
}

bool
cluster_undo_record_prepared_recheck(const ClusterUndoRecordPrepareReceipt *receipt,
									 uint16 payload_len)
{
	return fault != 4;
}

static bool
cluster_heap_ctrc_pending_itl_target(Relation relation, Buffer buffer,
									 const ClusterHeapDmlAuthorityGuard *guard, uint8 record_type,
									 ClusterCtrcTargetV1 *target)
{
	return fault != 5;
}

bool
cluster_undo_record_ctrc_stage_pending(ClusterUndoRecordPrepareReceipt *receipt, uint8 ordinal,
									   const ClusterCtrcTargetV1 *target)
{
	if (fault == 6)
		return false;
	receipt->ctrc_pending_mask |= UINT8_C(1);
	return true;
}

bool
cluster_undo_record_ctrc_pending_recheck(const ClusterUndoRecordPrepareReceipt *receipt,
										 uint8 ordinal, const ClusterCtrcTargetV1 *target)
{
	return fault != 7;
}

static bool
cluster_heap_ctrc_stage_reusable_itl_receipt(Relation relation, Buffer buffer, TransactionId xid,
											 bool lock_only,
											 ClusterUndoRecordPrepareReceipt *receipt,
											 uint8 ordinal, const ClusterCtrcTargetV1 *target)
{
	return fault != 8;
}

bool
cluster_undo_record_ctrc_pending_matches(const ClusterUndoRecordPrepareReceipt *receipt,
										 uint8 ordinal, const ClusterCtrcTargetV1 *target)
{
	return fault != 9;
}

#include "test_cluster_heap_prepare_diagnostic.inc"

static void
check_prepare(int cause, bool applied, int expected, const char *reason)
{
	ClusterUndoRecordPrepareReceipt receipt = { 0 };
	const char *observed = NULL;
	bool invalidated = false;
	ClusterHeapPreparedUndoResult result;
	uint64 capacity_deadline = 12345;

	fault = cause;
	receipt.ctrc_applied_mask = applied ? 1 : 0;
	if (cause == 7 || cause == 9)
		receipt.ctrc_pending_mask = 1;
	if (cause == 9)
		receipt.ctrc_prepared_mask = 1;
#ifdef PREPARE_HAS_CAPACITY_WAIT
	observed = "old cause must not leak";
	result = cluster_heap_itl_prepare_prepared_undo(
		NULL, 1, NULL, 700, request_lock_only, &receipt, 64, &invalidated, &observed,
		capacity_requested ? &capacity_deadline : NULL, &content_unlocked);
#elif defined(PREPARE_HAS_DIAGNOSTIC)
	observed = "old cause must not leak";
	result = cluster_heap_itl_prepare_prepared_undo(NULL, 1, NULL, 700, true, &receipt, 64,
													&invalidated, &observed);
#else
	/* Baseline executes the same production decision but has no reason output. */
	result = cluster_heap_itl_prepare_prepared_undo(NULL, 1, NULL, 700, true, &receipt, 64,
													&invalidated);
#endif
	UT_ASSERT_EQ(result, expected);
	UT_ASSERT_EQ(invalidated, cause == 7 || cause == 9);
	UT_ASSERT_EQ(receipt.ctrc_applied_mask, applied ? 1 : 0);
	if (reason != NULL) {
		UT_ASSERT(observed != NULL);
		if (observed != NULL)
			UT_ASSERT_EQ(strcmp(observed, reason), 0);
	} else
		UT_ASSERT(observed == NULL);
}

UT_TEST(full_lock_capacity_waits_then_requalifies_without_apply)
{
	capacity_requested = true;
	for (int retry = 0; retry < 2; retry++) {
		wait_calls = 0;
		content_unlocked = false;
		wait_result = retry ? CLUSTER_TXW_RETRY : CLUSTER_TXW_RESOLVED;
		check_prepare(1, false, CLUSTER_HEAP_PREPARED_UNDO_RETRY_REQUIRED, NULL);
		UT_ASSERT_EQ(wait_calls, 1);
		UT_ASSERT(content_unlocked);
	}
	/* APPLY cannot be cancelled by this waiting branch. */
	wait_calls = 0;
	content_unlocked = false;
	check_prepare(1, true, CLUSTER_HEAP_PREPARED_UNDO_REFUSED, "ITL_CAPACITY_REFUSED");
	UT_ASSERT_EQ(wait_calls, 0);
	UT_ASSERT(!content_unlocked);
	request_lock_only = false;
	check_prepare(1, false, CLUSTER_HEAP_PREPARED_UNDO_REFUSED, "ITL_CAPACITY_REFUSED");
	UT_ASSERT_EQ(wait_calls, 0);
	UT_ASSERT(!content_unlocked);
	request_lock_only = true;
	capacity_requested = false;
}

UT_TEST(unproved_or_failed_wait_is_not_a_retry_success)
{
	const ClusterTxwResult failures[]
		= { CLUSTER_TXW_UNPROVABLE, CLUSTER_TXW_TIMEOUT, CLUSTER_TXW_DEADLOCK };
	capacity_requested = true;
	for (unsigned i = 0; i < lengthof(failures); i++) {
		wait_calls = 0;
		wait_error = content_unlocked = false;
		wait_result = failures[i];
		if (setjmp(wait_error_jump) == 0)
			check_prepare(1, false, CLUSTER_HEAP_PREPARED_UNDO_REFUSED, NULL);
		UT_ASSERT(wait_error);
		UT_ASSERT(content_unlocked);
		UT_ASSERT_EQ(wait_calls, 1);
	}
	capacity_requested = false;
}

UT_TEST(capacity_wait_preserves_receipt_and_original_budgets)
{
	const ClusterTxwResult wakes[] = { CLUSTER_TXW_RESOLVED, CLUSTER_TXW_RETRY };

	for (unsigned i = 0; i < lengthof(wakes); i++) {
		ClusterUndoRecordPrepareReceipt receipt = { 0 }, before;
		const char *reason = "old cause must not leak";
		uint64 capacity_deadline = 12345;
		bool invalidated = true;

		/* The wait must not cancel or renew this previously prepared target.
		 * Real expired-budget requalification is covered by undo_record. */
		receipt.absolute_deadline_us = 100;
		receipt.reservation_sequence = 17;
		receipt.ctrc_pending_mask = receipt.ctrc_prepared_mask = 1;
		before = receipt;
		fault = 1;
		wait_result = wakes[i];
		wait_calls = 0;
		content_unlocked = false;
		UT_ASSERT_EQ(cluster_heap_itl_prepare_prepared_undo(NULL, 1, NULL, 700, true, &receipt, 64,
															&invalidated, &reason,
															&capacity_deadline, &content_unlocked),
					 CLUSTER_HEAP_PREPARED_UNDO_RETRY_REQUIRED);
		UT_ASSERT(!invalidated);
		UT_ASSERT(content_unlocked);
		UT_ASSERT_EQ(wait_calls, 1);
		UT_ASSERT(reason == NULL);
		UT_ASSERT_EQ(capacity_deadline, 12345);
		UT_ASSERT_EQ(memcmp(&receipt, &before, sizeof(receipt)), 0);
	}
}

UT_TEST(capacity_wake_does_not_bypass_fresh_target_recheck)
{
	const int rechecks[] = { 0, 7, 9 };

	for (unsigned i = 0; i < lengthof(rechecks); i++) {
		ClusterUndoRecordPrepareReceipt receipt = { 0 };
		const char *reason = NULL;
		uint64 capacity_deadline = 12345;
		bool invalidated = false;

		receipt.ctrc_pending_mask = receipt.ctrc_prepared_mask = 1;
		fault = 1;
		wait_result = CLUSTER_TXW_RESOLVED;
		UT_ASSERT_EQ(cluster_heap_itl_prepare_prepared_undo(NULL, 1, NULL, 700, true, &receipt, 64,
															&invalidated, &reason,
															&capacity_deadline, &content_unlocked),
					 CLUSTER_HEAP_PREPARED_UNDO_RETRY_REQUIRED);
		UT_ASSERT(!invalidated);
		/* Simulate the caller's fresh page bracket. Actual pending-target
		 * mismatch still invalidates; a wake alone never publishes READY. */
		fault = rechecks[i];
		UT_ASSERT_EQ(cluster_heap_itl_prepare_prepared_undo(NULL, 1, NULL, 700, true, &receipt, 64,
															&invalidated, &reason,
															&capacity_deadline, &content_unlocked),
					 i == 0 ? CLUSTER_HEAP_PREPARED_UNDO_READY
							: CLUSTER_HEAP_PREPARED_UNDO_RETRY_REQUIRED);
		UT_ASSERT_EQ(invalidated, i != 0);
		UT_ASSERT(!content_unlocked);
		UT_ASSERT_EQ(receipt.ctrc_applied_mask, 0);
		UT_ASSERT_EQ(capacity_deadline, 12345);
	}
}

UT_TEST(refusals_have_unique_exact_causes)
{
	check_prepare(1, false, CLUSTER_HEAP_PREPARED_UNDO_REFUSED, "ITL_CAPACITY_REFUSED");
	check_prepare(4, true, CLUSTER_HEAP_PREPARED_UNDO_REFUSED, "POST_APPLY_PREPARED_RECHECK");
	check_prepare(5, false, CLUSTER_HEAP_PREPARED_UNDO_REFUSED, "PENDING_TARGET_INVALID");
	check_prepare(6, false, CLUSTER_HEAP_PREPARED_UNDO_REFUSED, "PENDING_STAGE_REFUSED");
	check_prepare(7, true, CLUSTER_HEAP_PREPARED_UNDO_REFUSED, "POST_APPLY_PENDING_RECHECK");
	check_prepare(9, true, CLUSTER_HEAP_PREPARED_UNDO_REFUSED, "POST_APPLY_PENDING_MISMATCH");
}

UT_TEST(success_and_preapply_retries_remain_unchanged)
{
	check_prepare(0, false, CLUSTER_HEAP_PREPARED_UNDO_READY, NULL);
	check_prepare(2, false, CLUSTER_HEAP_PREPARED_UNDO_RETRY_REQUIRED, NULL);
	check_prepare(3, false, CLUSTER_HEAP_PREPARED_UNDO_RETRY_REQUIRED, NULL);
	check_prepare(4, false, CLUSTER_HEAP_PREPARED_UNDO_RETRY_REQUIRED, NULL);
	check_prepare(7, false, CLUSTER_HEAP_PREPARED_UNDO_RETRY_REQUIRED, NULL);
	check_prepare(8, false, CLUSTER_HEAP_PREPARED_UNDO_RETRY_REQUIRED, NULL);
	check_prepare(9, false, CLUSTER_HEAP_PREPARED_UNDO_RETRY_REQUIRED, NULL);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(refusals_have_unique_exact_causes);
	UT_RUN(success_and_preapply_retries_remain_unchanged);
	UT_RUN(full_lock_capacity_waits_then_requalifies_without_apply);
	UT_RUN(unproved_or_failed_wait_is_not_a_retry_success);
	UT_RUN(capacity_wait_preserves_receipt_and_original_budgets);
	UT_RUN(capacity_wake_does_not_bypass_fresh_target_recheck);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
