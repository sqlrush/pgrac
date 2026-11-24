/* Original remote cancellation must not reopen the holder's settled read.
 * Requester ERROR/exit, M consumption and H's physical ACK are real bodies;
 * this checks H's PCM projection, not requester failed-IO cleanup or shutdown.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_native_stop_source_cancel.inc"
#include "test_cluster_native_stop_census.h"

static bool source_cancel_holder(int master, int requester, int mode)
{
	NM_REQUIRE(nt_holder(master, requester, mode));
	UT_ASSERT_EQ(ns_pcm("source-cancel-retired"), CLUSTER_NORMAL_STOP_READY);
	return true;
}

UT_TEST(test_stop_source_after_remote_cancel)
{
	nt_holder_role = source_cancel_holder;
	rc_run(1, 0, false);
}
UT_TEST(test_stop_source_after_remote_installed_ERROR)
{
	nt_holder_role = source_cancel_holder;
	rc_run(3, 0, true);
}
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(2);
	UT_RUN(test_stop_source_after_remote_cancel);
	UT_RUN(test_stop_source_after_remote_installed_ERROR);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
