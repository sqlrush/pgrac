/* Cancel through original IO/receipt terminators without hiding BM_IO_ERROR.
 * Final-pin eviction and full shutdown remain separate qualification gates.
 * Author: SqlRush <sqlrush@gmail.com> */
static void ns_cancel_io(void), ns_cancel_aborted(void);
#include "test_cluster_native_stop_cancel.inc"
#include "test_cluster_native_stop_census.h"
#if defined(NATIVE_STOP_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "stop-census assertion-OFF requires an assertion-disabled configuration"
#endif
static unsigned ns_io_cuts, ns_abort_cuts;
static void ns_cancel_io(void)
{
	ns_io_cuts++;
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & BM_IO_IN_PROGRESS, BM_IO_IN_PROGRESS);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(ns_pcm("cancel-before-IO-abort"), CLUSTER_NORMAL_STOP_PENDING);
	ns_buffer_pending("cancel-before-IO-abort");
}
static void ns_cancel_aborted(void)
{
	ClusterBufferStopObservation observed; const char *reason;
	ns_abort_cuts++;
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_IO_ERROR | BM_IO_IN_PROGRESS), BM_IO_ERROR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(ns_pcm("quiet-before-master-consumption"), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(ns_buffer(&observed, &reason), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(strcmp(reason, "BUFMGR_SHAPE_INVALID") == 0);
	UT_ASSERT_EQ(observed.state & BM_IO_ERROR, BM_IO_ERROR);
	printf("# stop-census cancel-after-abort buffer=%s IO_ERROR=1 original-IO-owner=0\n", reason);
}
static bool ns_cancel_parent(int fd, int order)
{
	NM_REQUIRE(cm_parent(fd, order));
	UT_ASSERT_EQ(ns_io_cuts, 1); UT_ASSERT_EQ(ns_abort_cuts, 1);
	/* The exact quiet receipt has reached the real master and its physical
	 * callback. Neither that fact nor this poll clears the failed buffer. */
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
	(void)ns_pcm("cancel-original-receipt-retired");
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & BM_IO_ERROR, BM_IO_ERROR);
	return true;
}
static void ns_cancel_run(unsigned cut, bool late)
{
	cm_cut = cut; cm_ack_before_callback = late; ns_io_cuts = ns_abort_cuts = 0;
	nm_run_roles(0, nm_master, ns_cancel_parent);
}
UT_TEST(test_stop_cancel_before_read_keeps_IO_and_quiet_owners) { ns_cancel_run(0, false); }
UT_TEST(test_stop_cancel_after_read_keeps_IO_and_quiet_owners) { ns_cancel_run(2, false); }
UT_TEST(test_stop_cancel_before_read_waits_late_physical_callback) { ns_cancel_run(0, true); }
UT_TEST(test_stop_cancel_after_read_waits_late_physical_callback) { ns_cancel_run(2, true); }
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(4);
	UT_RUN(test_stop_cancel_before_read_keeps_IO_and_quiet_owners);
	UT_RUN(test_stop_cancel_after_read_keeps_IO_and_quiet_owners);
	UT_RUN(test_stop_cancel_before_read_waits_late_physical_callback);
	UT_RUN(test_stop_cancel_after_read_waits_late_physical_callback);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
