/* PGRAC: actual publisher under the EXEC_BACKEND pre-attach startup order.
 * Author: SqlRush <sqlrush@gmail.com> */
#define EXEC_BACKEND 1
#define main content_trace_fixture_main
#include "test_cluster_content_trace.c"
#undef main

UT_TEST(exec_backend_unbound_start_cannot_authenticate_reused_identity)
{
	ContentTraceShared *mapping;
	ClusterContentTraceWalkStats stats;
	ContentTraceCapture capture = {.tag = NULL, .cut = &cut};
	reset(); hold(0, LW_EXCLUSIVE);
	cluster_content_trace_lock_changed(&buf(0)->content_lock);
	mapping = content_trace;
	content_trace = NULL; /* Native InitProcess precedes diagnostic attachment. */
	cluster_content_trace_process_start(0);
	content_trace = mapping;
	memset(&cut, 0, sizeof(cut));
	cluster_content_trace_walk(content_trace_capture_owner, &capture, &stats);
	UT_ASSERT(!stats.available); UT_ASSERT_EQ(cut.nowners, 0);
}

int main(void)
{
	UT_PLAN(1);
	UT_RUN(exec_backend_unbound_start_cannot_authenticate_reused_identity);
	UT_DONE(); free(content_trace); return ut_failed_count ? 1 : 0;
}
