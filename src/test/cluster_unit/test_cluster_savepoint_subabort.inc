/*-------------------------------------------------------------------------
 *
 * test_cluster_savepoint_subabort.inc
 *    Exercise live frame rollback before the native abort state transition.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_savepoint_subabort.inc
 *
 * NOTES
 *    PGRAC-original tests execute actual frame/name/native transition code.
 *    The record rollback and later native resource cleanup are boundaries.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CLUSTER_SAVEPOINT_SUBABORT_PRESENT
static void
cluster_savepoint_subabort(SubTransactionId subid)
{
	(void)subid;
}
#endif

static unsigned subabort_test_rollbacks, subabort_test_deadlines;
static bool subabort_test_refuse;
static SubTransactionId subabort_test_last;

bool cluster_undo_record_rollback_savepoint_current(ClusterUndoSavepointControl *, uint64,
													XLogRecPtr *);

uint64
cluster_undo_record_prepare_deadline_us(void)
{
	subabort_test_deadlines++;
	return 100;
}

bool
cluster_undo_record_rollback_savepoint_current(ClusterUndoSavepointControl *control,
											   uint64 deadline, XLogRecPtr *cut)
{
	subabort_test_rollbacks++;
	UT_ASSERT(IsTransactionState());
	UT_ASSERT_EQ(deadline, 100);
	UT_ASSERT(control != NULL && control->owned);
	UT_ASSERT(cluster_savepoint_operation_matches_frame(&control->operation));
	UT_ASSERT_EQ(control->operation.subid, GetCurrentSubTransactionId());
	subabort_test_last = control->operation.subid;
	*cut = subabort_test_refuse ? InvalidXLogRecPtr : 603;
	return !subabort_test_refuse;
}

static void
subabort_test_fixture(void)
{
	frame_test_fixture();
	subabort_test_rollbacks = subabort_test_deadlines = 0;
	subabort_test_last = InvalidSubTransactionId;
	subabort_test_refuse = false;
}

UT_TEST(test_subabort_runs_rollback_before_native_state_and_frame_cleanup)
{
	SubTransactionId subid;
	unsigned allocated;

	subabort_test_fixture();
	BeginInternalSubTransaction(NULL);
	subid = GetCurrentSubTransactionId();
	UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
	allocated = xid_assignments;
	RollbackAndReleaseCurrentSubTransaction();
	UT_ASSERT_EQ(subabort_test_rollbacks, 1);
	UT_ASSERT_EQ(subabort_test_deadlines, 1);
	UT_ASSERT_EQ(subabort_test_last, subid);
	UT_ASSERT_EQ(GetCurrentSubTransactionId(), TopSubTransactionId);
	UT_ASSERT_EQ(xid_assignments, allocated);
	UT_ASSERT(IsTransactionState());
}

UT_TEST(test_subabort_empty_and_readonly_frames_do_not_assign_xid)
{
	subabort_test_fixture();
	savepoint("empty");
	BeginInternalSubTransaction(NULL);
	RollbackAndReleaseCurrentSubTransaction();
	RollbackToSavepoint("empty");
	CommitTransactionCommand();
	UT_ASSERT_EQ(subabort_test_rollbacks + subabort_test_deadlines, 0);
	UT_ASSERT_EQ(xid_assignments, 0);
	UT_ASSERT(IsTransactionState());
}

UT_TEST(test_subabort_partial_metadata_start_has_no_effects)
{
	subabort_test_fixture();
	fail_allocation = 2; /* Native frame exists; metadata allocation fails. */
	expect_error = true;
	if (setjmp(error_jump) == 0) {
		BeginInternalSubTransaction(NULL);
		UT_ASSERT(false);
	}
	expect_error = false;
	UT_ASSERT(pending_start != InvalidSubTransactionId);
	AbortSubTransaction();
	CleanupSubTransaction();
	UT_ASSERT_EQ(pending_start, InvalidSubTransactionId);
	UT_ASSERT_EQ(subabort_test_rollbacks + subabort_test_deadlines, 0);
	UT_ASSERT_EQ(xid_assignments, 0);
	UT_ASSERT_EQ(GetCurrentSubTransactionId(), TopSubTransactionId);
}

UT_TEST(test_subabort_nested_release_keeps_original_ancestor_boundary)
{
	SubTransactionId outer;
	ClusterUndoSavepointControl *control;

	subabort_test_fixture();
	savepoint("outer");
	outer = GetCurrentSubTransactionId();
	BeginInternalSubTransaction(NULL);
	UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
	control = savepoint_frames->parent->control;
	ReleaseCurrentSubTransaction();
	UT_ASSERT_EQ(subabort_test_rollbacks, 0);
	UT_ASSERT(savepoint_frames->control == control);
	BeginInternalSubTransaction(NULL); /* Readonly descendant after RELEASE. */
	RollbackAndReleaseCurrentSubTransaction();
	UT_ASSERT_EQ(subabort_test_rollbacks, 0);
	RollbackToSavepoint("outer");
	CommitTransactionCommand();
	UT_ASSERT_EQ(subabort_test_rollbacks, 1);
	UT_ASSERT_EQ(subabort_test_last, outer);
	UT_ASSERT(savepoint_frames->subid != outer);
	UT_ASSERT(savepoint_frames->control == NULL);
}

UT_TEST(test_subabort_failed_or_unknown_owner_cannot_resume_parent)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		TransactionState child;

		subabort_test_fixture();
		BeginInternalSubTransaction(NULL);
		UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
		child = CurrentTransactionState;
		subabort_test_refuse = fault == 0;
		if (fault == 1)
			savepoint_frames->subid++;
		if (fault == 2)
			pending_start = child->subTransactionId + 1;
		error_level = 0;
		expect_error = true;
		if (setjmp(error_jump) == 0) {
			if (fault == 0)
				AbortSubTransaction();
			else
				cluster_savepoint_subabort(child->subTransactionId);
			UT_ASSERT(false);
		}
		expect_error = false;
		UT_ASSERT_EQ(error_level, PANIC);
		UT_ASSERT(CurrentTransactionState == child && child->state == TRANS_INPROGRESS);
		UT_ASSERT_EQ(subabort_test_rollbacks, fault == 0);
		savepoint_frames->subid = child->subTransactionId;
		pending_start = InvalidSubTransactionId;
	}
}

UT_TEST(test_subabort_disabled_storage_leaves_native_lifecycle)
{
	subabort_test_fixture();
	cluster_enabled = false;
	BeginInternalSubTransaction(NULL);
	RollbackAndReleaseCurrentSubTransaction();
	UT_ASSERT_EQ(subabort_test_rollbacks + subabort_test_deadlines, 0);
	UT_ASSERT_EQ(xid_assignments, 0);
	UT_ASSERT(IsTransactionState());
	cluster_enabled = true;
}
