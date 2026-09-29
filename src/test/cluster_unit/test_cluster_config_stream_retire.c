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

static unsigned activity_sends, activity_receives;
static bool activity_socket_empty;
static uint64 activity_dispatch_before;
static Size activity_length;
static uint8 activity_type;

static void
activity_sending(int32 peer, const void *bytes, Size length)
{
	char available;
	ClusterICEnvelope env;
	UT_ASSERT_EQ(peer, UT_PEER_ID);
	UT_ASSERT(length >= sizeof(env));
	memcpy(&env, bytes, sizeof(env));
	activity_type = env.msg_type;
	activity_length = length;
	activity_sends++;
	activity_socket_empty = recv(ut_rx_fd, &available, 1, MSG_PEEK | MSG_DONTWAIT) < 0
							&& (errno == EAGAIN || errno == EWOULDBLOCK);
}

static void
activity_received(int32 peer, const ClusterICEnvelope *env, Size length)
{
	UT_ASSERT_EQ(peer, UT_PEER_ID);
	activity_receives++;
	activity_type = env->msg_type;
	activity_length = length;
	activity_dispatch_before = ut_dispatch_count;
}

static void
activity_pair(void)
{
	retire_fixture();
	test_connect_registers_peer_fd();
	(void)ut_drain_all_and_sweep(UT_PEER_ID, ut_rx_fd, ut_acc, sizeof(ut_acc));
	activity_sends = activity_receives = 0;
	activity_socket_empty = false;
	activity_dispatch_before = PG_UINT64_MAX;
	ut_config_send_hook = activity_sending;
	ut_config_receive_hook = activity_received;
}

static void
activity_close(void)
{
	ut_config_send_hook = NULL;
	ut_config_receive_hook = NULL;
	cluster_ic_tier1_close_peer(UT_PEER_ID, NULL);
	close(ut_rx_fd);
	ut_rx_fd = ut_tx_fd = -1;
}

UT_TEST(new_send_retires_before_any_wire_byte)
{
	ClusterICEnvelope env = { 0 };
	activity_pair();
	env.msg_type = PGRAC_IC_MSG_GES_REQUEST;
	UT_ASSERT_EQ(tier1_send_bytes(UT_PEER_ID, &env, sizeof(env)), CLUSTER_IC_SEND_DONE);
	UT_ASSERT_EQ(activity_sends, 1);
	UT_ASSERT(activity_socket_empty);
	UT_ASSERT_EQ(activity_type, PGRAC_IC_MSG_GES_REQUEST);
	UT_ASSERT_EQ(activity_length, sizeof(env));
	UT_ASSERT_EQ(ut_drain_and_collect(ut_rx_fd, ut_acc, sizeof(ut_acc), sizeof(env)), sizeof(env));
	UT_ASSERT(memcmp(ut_acc, &env, sizeof(env)) == 0);
	activity_close();
}

UT_TEST(backpressure_and_fifo_keep_bytes_after_notification)
{
	ClusterICEnvelope env = { 0 };
	long junk;
	activity_pair();
	env.msg_type = PGRAC_IC_CHUNK_MSG_TYPE;
	junk = ut_fill_until_eagain(ut_tx_fd);
	UT_ASSERT_EQ(tier1_send_bytes(UT_PEER_ID, &env, sizeof(env)), CLUSTER_IC_SEND_WOULD_BLOCK);
	UT_ASSERT_EQ(activity_sends, 1);
	UT_ASSERT_EQ(tier1_send_bytes(UT_PEER_ID, &env, sizeof(env)), CLUSTER_IC_SEND_WOULD_BLOCK);
	UT_ASSERT_EQ(activity_sends, 2);
	UT_ASSERT_EQ(activity_type, PGRAC_IC_CHUNK_MSG_TYPE);
	UT_ASSERT_EQ(tier1_outbound_fifo_frames[UT_PEER_ID], 1);
	UT_ASSERT_EQ(ut_drain_and_collect(ut_rx_fd, ut_acc, sizeof(ut_acc), junk + 2 * sizeof(env)),
				 junk + 2 * sizeof(env));
	UT_ASSERT(memcmp(ut_acc + junk, &env, sizeof(env)) == 0);
	UT_ASSERT(memcmp(ut_acc + junk + sizeof(env), &env, sizeof(env)) == 0);
	/* Draining an already owned tail is not a new admission. */
	UT_ASSERT_EQ(activity_sends, 2);
	activity_close();
}

UT_TEST(receive_retires_before_handler_not_at_partial_header)
{
	ClusterICEnvelope env = { 0 };
	uint64 dispatch_before;
	char payload[5] = "body";
	activity_pair();
	dispatch_before = ut_dispatch_count;
	env.msg_type = PGRAC_IC_CHUNK_MSG_TYPE;
	env.source_node_id = UT_PEER_ID;
	env.dest_node_id = cluster_node_id;
	env.payload_length = sizeof(payload);
	ut_send_receive_slice(&env, 7);
	UT_ASSERT_EQ(activity_receives, 0);
	ut_send_receive_slice((char *)&env + 7, sizeof(env) - 7);
	UT_ASSERT_EQ(activity_receives, 0);
	ut_send_receive_slice(payload, sizeof(payload));
	UT_ASSERT_EQ(activity_receives, 1);
	UT_ASSERT_EQ(activity_dispatch_before, dispatch_before);
	UT_ASSERT_EQ(ut_dispatch_count, dispatch_before + 1);
	UT_ASSERT_EQ(activity_type, PGRAC_IC_CHUNK_MSG_TYPE);
	UT_ASSERT_EQ(activity_length, sizeof(payload));
	activity_close();
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(close_retires_report_before_stream);
	UT_RUN(rebind_retires_report_before_replacement);
	UT_RUN(plane_channel_and_shutdown_retire_before_reset);
	UT_RUN(new_send_retires_before_any_wire_byte);
	UT_RUN(backpressure_and_fifo_keep_bytes_after_notification);
	UT_RUN(receive_retires_before_handler_not_at_partial_header);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
