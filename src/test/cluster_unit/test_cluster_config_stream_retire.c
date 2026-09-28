/*-------------------------------------------------------------------------
 *
 * test_cluster_config_stream_retire.c
 *    Execute actual native retirement before configuration report invalidation.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_config_stream_retire.c
 * NOTES
 *    Uses the real transport translation unit and an explicit report callback
 *    boundary. Initial private stream state is a fixture, not a live socket.
 *    Actual TCP/FIFO tests remain separate; this test cannot replace them.
 *-------------------------------------------------------------------------
 */
int stream_retire_fixture_main(void);
#define main stream_retire_fixture_main
#include "test_cluster_ic_tier1_partial.c"
#undef main

static void
retire_fixture(void)
{
	peer_fds_lazy_init();
	MyProcPid = getpid();
	tier1_stream_owner = getpid();
	tier1_stream_next = 123;
	tier1_stream_exhausted = false;
	tier1_stream_serial[UT_PEER_ID] = 123;
	tier1_my_plane = CLUSTER_IC_PLANE_CONTROL;
	tier1_my_data_channel = 0;
	tier1_my_n_workers = 2;
	Tier1Shmem = NULL;
	ut_config_retire_calls = 0;
}

UT_TEST(close_retires_report_before_stream)
{
	retire_fixture();
	cluster_ic_tier1_close_peer(UT_PEER_ID, NULL);
	UT_ASSERT_EQ(ut_config_retire_calls, 1);
	UT_ASSERT_EQ(ut_config_retire_peer, UT_PEER_ID);
	UT_ASSERT_EQ(ut_config_retire_serial, 123);
	UT_ASSERT_EQ(tier1_stream_serial[UT_PEER_ID], 0);
}
UT_TEST(rebind_retires_report_before_replacement)
{
	retire_fixture();
	tier1_stream_bind(UT_PEER_ID);
	UT_ASSERT_EQ(ut_config_retire_calls, 1);
	UT_ASSERT_EQ(ut_config_retire_serial, 123);
	UT_ASSERT_EQ(tier1_stream_serial[UT_PEER_ID], 124);
}
UT_TEST(plane_channel_and_shutdown_retire_before_reset)
{
	for (unsigned kind = 0; kind < 3; ++kind) {
		retire_fixture();
		if (kind == 0)
			cluster_ic_tier1_set_my_plane(CLUSTER_IC_PLANE_DATA);
		else if (kind == 1)
			cluster_ic_tier1_set_my_data_channel(1, 2);
		else
			tier1_tier_shutdown();
		UT_ASSERT_EQ(ut_config_retire_calls, 1);
		UT_ASSERT_EQ(ut_config_retire_peer, -1);
		UT_ASSERT_EQ(ut_config_retire_serial, 123);
		UT_ASSERT_EQ(tier1_stream_serial[UT_PEER_ID], 0);
	}
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(close_retires_report_before_stream);
	UT_RUN(rebind_retires_report_before_replacement);
	UT_RUN(plane_channel_and_shutdown_retire_before_reset);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
