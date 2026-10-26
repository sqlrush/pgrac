/* Native frame orchestration with explicit original-storage boundary fixtures.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#ifndef CLUSTER_SAVEPOINT_RESERVE_PRESENT
static bool
cluster_savepoint_reserve_current_frames(uint64 deadline)
{
	(void)deadline;
	return false;
}
#endif
#ifndef CLUSTER_UNDO_SAVEPOINT_RESERVE_PRESENT
typedef struct ClusterUndoSavepointControl ClusterUndoSavepointControl;
#endif
struct ClusterUndoSavepointControl {
	ClusterSavepointOperation operation;
	bool owned;
};
static struct ClusterUndoSavepointControl frame_test_controls[64];
static unsigned frame_test_calls, frame_test_count, frame_test_releases;
static bool frame_test_fail, frame_test_stale;

bool cluster_undo_record_reserve_savepoint_controls(const ClusterSavepointOperation *, uint32,
													uint64, ClusterUndoSavepointControl **);
void cluster_undo_record_release_savepoint_control(ClusterUndoSavepointControl *);
bool cluster_undo_record_savepoint_control_owned(const ClusterUndoSavepointControl *);
void cluster_undo_record_release_savepoint_controls_at_xact_end(void);

bool
cluster_undo_record_reserve_savepoint_controls(const ClusterSavepointOperation *operations,
											   uint32 count, uint64 deadline,
											   ClusterUndoSavepointControl **out)
{
	UT_ASSERT_EQ(deadline, 100);
	frame_test_calls++;
	for (uint32 i = 0; i < count; i++)
		out[i] = NULL;
	if (frame_test_fail)
		return false;
	for (uint32 i = 0; i < count; i++) {
		UT_ASSERT(cluster_savepoint_operation_matches_frame(&operations[i]));
		UT_ASSERT(frame_test_count < 64);
		frame_test_controls[frame_test_count].operation = operations[i];
		frame_test_controls[frame_test_count].owned = true;
		out[i] = &frame_test_controls[frame_test_count++];
	}
	return true;
}
void
cluster_undo_record_release_savepoint_control(ClusterUndoSavepointControl *control)
{
	if (control == NULL || !control->owned)
		return;
	control->owned = false;
	frame_test_releases++;
}
bool
cluster_undo_record_savepoint_control_owned(const ClusterUndoSavepointControl *control)
{
	return control != NULL && control->owned && !frame_test_stale
		   && cluster_savepoint_operation_matches_frame(&control->operation);
}
void
cluster_undo_record_release_savepoint_controls_at_xact_end(void)
{
	for (unsigned i = 0; i < frame_test_count; i++)
		cluster_undo_record_release_savepoint_control(&frame_test_controls[i]);
}
static void
frame_test_fixture(void)
{
	fresh_transaction();
	memset(frame_test_controls, 0, sizeof(frame_test_controls));
	frame_test_calls = frame_test_count = frame_test_releases = 0;
	frame_test_fail = frame_test_stale = false;
}

UT_TEST(test_frame_reserve_top_and_empty_scopes_need_no_storage)
{
	frame_test_fixture();
	UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
	savepoint("empty");
	BeginInternalSubTransaction(NULL);
	RollbackAndReleaseCurrentSubTransaction();
	ReleaseSavepoint("empty");
	CommitTransactionCommand();
	UT_ASSERT_EQ(frame_test_calls + frame_test_count + frame_test_releases, 0);
	UT_ASSERT_EQ(xid_assignments, 0);
}

UT_TEST(test_frame_reserve_prepares_all_new_ancestors_once)
{
	frame_test_fixture();
	savepoint("a");
	BeginInternalSubTransaction(NULL);
	BeginInternalSubTransaction(NULL);
	UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
	UT_ASSERT_EQ(frame_test_calls, 1);
	UT_ASSERT_EQ(frame_test_count, 3);
	UT_ASSERT_EQ(xid_assignments, 4);
	UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
	UT_ASSERT_EQ(frame_test_calls, 1);
	BeginInternalSubTransaction(NULL);
	UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
	UT_ASSERT_EQ(frame_test_calls, 2);
	UT_ASSERT_EQ(frame_test_count, 4);
	UT_ASSERT_EQ(last_binding_xid, 700);
}

UT_TEST(test_frame_reserve_release_keeps_ancestor_ownership)
{
	frame_test_fixture();
	savepoint("same");
	savepoint("same");
	BeginInternalSubTransaction(NULL);
	UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
	ReleaseSavepoint("same");
	CommitTransactionCommand();
	UT_ASSERT_EQ(frame_test_releases, 2);
	UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
	UT_ASSERT_EQ(frame_test_calls, 1);
	UT_ASSERT_EQ(frame_test_count, 3);
	RollbackAndReleaseCurrentSubTransaction();
	UT_ASSERT_EQ(frame_test_releases, 3);
}

UT_TEST(test_frame_reserve_failed_batch_publishes_no_handles)
{
	frame_test_fixture();
	savepoint("a");
	BeginInternalSubTransaction(NULL);
	frame_test_fail = true;
	UT_ASSERT(!cluster_savepoint_reserve_current_frames(100));
	UT_ASSERT_EQ(frame_test_count, 0);
	frame_test_fail = false;
	UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
	UT_ASSERT_EQ(frame_test_calls, 2);
	UT_ASSERT_EQ(frame_test_count, 2);
	ReleaseCurrentSubTransaction();
	UT_ASSERT_EQ(frame_test_releases, 1);
	UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
	UT_ASSERT_EQ(frame_test_calls, 2);
}

UT_TEST(test_frame_reserve_allocation_failure_precedes_storage)
{
	for (unsigned leg = 1; leg <= 2; leg++) {
		frame_test_fixture();
		savepoint("a");
		BeginInternalSubTransaction(NULL);
		fail_allocation = leg;
		expect_error = true;
		if (setjmp(error_jump) == 0) {
			(void)cluster_savepoint_reserve_current_frames(100);
			UT_ASSERT(false);
		}
		expect_error = false;
		UT_ASSERT_EQ(frame_test_calls, 0);
		UT_ASSERT_EQ(xid_assignments, 0);
		/* The real abort retains this first scratch array in TopTransactionContext
		 * until bulk teardown; the malloc fixture has no context deletion. */
		if (leg == 2)
			pfree(last_allocation);
		UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
		UT_ASSERT_EQ(frame_test_count, 2);
	}
}

UT_TEST(test_frame_reserve_old_control_identity_cannot_authorize_new_effect)
{
	frame_test_fixture();
	savepoint("a");
	UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
	frame_test_stale = true;
	UT_ASSERT(!cluster_savepoint_reserve_current_frames(100));
	UT_ASSERT_EQ(frame_test_calls, 1);
	UT_ASSERT_EQ(frame_test_releases, 0);
	frame_test_stale = false;
	UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
	UT_ASSERT_EQ(frame_test_count, 1);
}

UT_TEST(test_frame_xact_callback_retires_controls_before_metadata)
{
	ClusterSavepointFrame *frames;

	frame_test_fixture();
	savepoint("a");
	BeginInternalSubTransaction(NULL);
	UT_ASSERT(cluster_savepoint_reserve_current_frames(100));
	UT_ASSERT_EQ(frame_test_count, 2);
	frames = savepoint_frames;
	end_callback(XACT_EVENT_ABORT, end_callback_arg);
	UT_ASSERT_EQ(frame_test_releases, 2);
	UT_ASSERT(savepoint_frames == NULL);
	end_callback(XACT_EVENT_ABORT, end_callback_arg);
	UT_ASSERT_EQ(frame_test_releases, 2);
	/* Simulate native TopTransactionContext teardown after its callback. */
	while (frames != NULL) {
		ClusterSavepointFrame *parent = frames->parent;
		pfree(frames);
		frames = parent;
	}
}
