/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual transport-composition body. The independently tested Tier1/chunk/RDMA
 * owner observations are boundary inputs here, not new transport proof. */
#include "postgres.h"
#include "../../backend/cluster/cluster_ic_mux.c"
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
int cluster_node_id = 0;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
int cluster_interconnect_rdma_fallback = CLUSTER_IC_RDMA_FALLBACK_AUTO;
static ClusterNormalStopPollResult tcp_result, chunk_result, rdma_result;
static bool rdma_active;
static unsigned tcp_calls, chunk_calls, rdma_calls;
static void
forbidden_init(void)
{
	abort();
}
const ClusterICOps ClusterICOps_Tier1
	= { .tier_init = forbidden_init, .tier_shutdown = forbidden_init };
const ClusterICOps ClusterICOps_Tier2
	= { .tier_init = forbidden_init, .tier_shutdown = forbidden_init };
const ClusterICOps ClusterICOps_Tier3
	= { .tier_init = forbidden_init, .tier_shutdown = forbidden_init };
const ClusterICOps *ClusterICOps_Active;

bool
errstart(int level, const char *domain)
{
	return false;
}
bool
errstart_cold(int level, const char *domain)
{
	return false;
}
void
errfinish(const char *file, int line, const char *func)
{
	abort();
}
int
errcode(int code)
{
	return 0;
}
int
errmsg(const char *format, ...)
{
	return 0;
}
int
errdetail(const char *format, ...)
{
	return 0;
}
int
errhint(const char *format, ...)
{
	return 0;
}
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 peer)
{
	return NULL;
}
bool
cluster_ic_rdma_runtime_available(const char **reason)
{
	abort();
}
void
cluster_ic_rdma_stats_note_fallback(int32 peer, const char *reason)
{}
void
cluster_ic_rdma_stats_note_transport(int32 peer, ClusterICPeerTransport transport,
									 ClusterICRdmaPeerState state)
{}
void
cluster_ic_rdma_stats_note_send(int32 peer, uint64 bytes, bool rdma)
{}
void
cluster_ic_rdma_stats_note_recv(int32 peer, uint64 bytes, bool rdma)
{}
void
cluster_ic_rdma_stats_note_error(int32 peer, const char *code, const char *reason)
{}
bool
cluster_ic_rdma_drain_recv(int32 *peer, void *buf, size_t cap, size_t *len)
{
	abort();
}
bool
cluster_ic_rdma_peek_sender(int32 *peer)
{
	abort();
}
ClusterNormalStopPollResult
cluster_ic_tier1_normal_stop_poll(int *peer, const char **reason)
{
	tcp_calls++;
	*peer = 1;
	*reason = "TCP_FIXTURE";
	return tcp_result;
}
ClusterNormalStopPollResult
cluster_ic_chunk_normal_stop_poll(int *peer, uint32 *seq, const char **reason)
{
	chunk_calls++;
	*peer = 2;
	*seq = 9;
	*reason = "CHUNK_FIXTURE";
	return chunk_result;
}
ClusterNormalStopPollResult
cluster_ic_rdma_normal_stop_poll(bool *active, int *peer, const char **reason)
{
	rdma_calls++;
	*active = rdma_active;
	*peer = 3;
	*reason = "RDMA_FIXTURE";
	return rdma_result;
}
static ClusterNormalStopPollResult
poll_transport(void)
{
	const char *domain, *reason;
	int peer;
	uint32 seq;
	return cluster_ic_normal_stop_poll(&domain, &peer, &seq, &reason);
}
static void
reset_test(void)
{
	mux_mark_all_tcp();
	ClusterICOps_Active = &ClusterICOps_Tier1;
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	rdma_active = false;
	tcp_result = chunk_result = rdma_result = CLUSTER_NORMAL_STOP_READY;
	tcp_calls = chunk_calls = rdma_calls = 0;
}
UT_TEST(test_supported_actual_runtime_and_all_owner_polls)
{
	reset_test();
	UT_ASSERT_EQ(poll_transport(), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(tcp_calls, 1);
	UT_ASSERT_EQ(chunk_calls, 1);
	UT_ASSERT_EQ(rdma_calls, 1);
	ClusterICOps_Active = NULL;
	UT_ASSERT_EQ(poll_transport(), CLUSTER_NORMAL_STOP_INVALID);
	ClusterICOps_Active = &ClusterICOps_Tier1;
	cluster_interconnect_tier = CLUSTER_IC_TIER_MOCK;
	UT_ASSERT_EQ(poll_transport(), CLUSTER_NORMAL_STOP_INVALID);
}
UT_TEST(test_later_invalid_cannot_hide_behind_tcp_pending)
{
	const char *domain, *reason;
	int peer;
	uint32 seq;
	reset_test();
	tcp_result = CLUSTER_NORMAL_STOP_PENDING;
	chunk_result = CLUSTER_NORMAL_STOP_INVALID;
	UT_ASSERT_EQ(cluster_ic_normal_stop_poll(&domain, &peer, &seq, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(domain, "chunk");
	UT_ASSERT_EQ(peer, 2);
	UT_ASSERT_EQ(seq, 9);
	UT_ASSERT_EQ(rdma_calls, 1);
	chunk_result = CLUSTER_NORMAL_STOP_READY;
	UT_ASSERT_EQ(poll_transport(), CLUSTER_NORMAL_STOP_PENDING);
}
UT_TEST(test_mux_fallback_does_not_hide_retained_rdma)
{
	reset_test();
	ClusterICOps_Active = &ClusterICOps_Mux;
	cluster_interconnect_tier = CLUSTER_IC_TIER_2;
	UT_ASSERT_EQ(poll_transport(), CLUSTER_NORMAL_STOP_READY); /* Actual TCP fallback allowed. */
	rdma_active = true;
	rdma_result = CLUSTER_NORMAL_STOP_PENDING;
	UT_ASSERT_EQ(poll_transport(), CLUSTER_NORMAL_STOP_PENDING); /* Even all-peer TCP. */
	rdma_result = CLUSTER_NORMAL_STOP_READY;
	cluster_ic_mux_set_peer_transport(1, CLUSTER_IC_PEER_TRANSPORT_RDMA,
									  CLUSTER_IC_RDMA_PEER_CONNECTED);
	UT_ASSERT_EQ(poll_transport(), CLUSTER_NORMAL_STOP_READY);
	rdma_active = false;
	UT_ASSERT_EQ(poll_transport(), CLUSTER_NORMAL_STOP_INVALID);
}
UT_TEST(test_unknown_transport_or_unexpected_active_provider)
{
	reset_test();
	rdma_active = true;
	UT_ASSERT_EQ(poll_transport(), CLUSTER_NORMAL_STOP_INVALID);
	rdma_active = false;
	cluster_ic_mux_set_peer_transport(127, (ClusterICPeerTransport)99, CLUSTER_IC_RDMA_PEER_ERROR);
	UT_ASSERT_EQ(poll_transport(), CLUSTER_NORMAL_STOP_INVALID);
}
int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_supported_actual_runtime_and_all_owner_polls);
	UT_RUN(test_later_invalid_cannot_hide_behind_tcp_pending);
	UT_RUN(test_mux_fallback_does_not_hide_retained_rdma);
	UT_RUN(test_unknown_transport_or_unexpected_active_provider);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
