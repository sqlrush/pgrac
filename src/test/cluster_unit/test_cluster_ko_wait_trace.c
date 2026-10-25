/* Actual KO CV producer and ERROR retirement, native boundary fixtures.
 * Author: SqlRush <sqlrush@gmail.com> */
#define main ko_stop_fixture_main
#include "test_cluster_ko_stop.c"
#undef main

int main(void)
{
	UT_PLAN(1);
	UT_RUN(test_admission_wait_uses_event_wake_with_bounded_fallback);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
