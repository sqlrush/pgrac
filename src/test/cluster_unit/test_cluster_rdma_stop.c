/* Author: SqlRush <sqlrush@gmail.com> */
/* Verbatim production private types, queue/release bodies and stop observer.
 * No verbs/CM implementation, hardware, CQ completion, or real pin is faked as
 * proven: callback release invocation and device readiness are boundaries. */
#include "postgres.h"
#include "miscadmin.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_ic_rdma.h"
#include "utils/memutils.h"

/* Only select the extracted observer's real compile-time branch. No provider
 * source or headers are replaced, no production test branch is introduced. */
#ifndef PGRAC_TEST_RDMA_DISABLED
#define HAVE_LIBIBVERBS 1
#define HAVE_LIBRDMACM 1
#define HAVE_RDMA_RDMA_CMA_H 1
#else
#undef HAVE_LIBIBVERBS
#undef HAVE_LIBRDMACM
#undef HAVE_RDMA_RDMA_CMA_H
#endif
#include "test_cluster_rdma_stop.inc"
#undef printf
#undef fprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool IsUnderPostmaster = true;
AuxProcType MyAuxProcType = LmsProcess;
MemoryContext TopMemoryContext, CurrentMemoryContext;
static ClusterICRdmaProvider provider;
static int releases;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d %s\n", file, line, condition);
	abort();
}
void *
palloc0(Size size)
{
	return calloc(1, size);
}
void *
palloc(Size size)
{
	return malloc(size);
}
void
pfree(void *ptr)
{
	free(ptr);
}
static void
release_callback(void *arg)
{
	(*(int *)arg)++;
}
static ClusterNormalStopPollResult
poll_transport(bool *active)
{
	int peer;
	const char *reason;
	return cluster_ic_rdma_normal_stop_poll(active, &peer, &reason);
}
static void
reset_test(void)
{
	Assert(RdmaInboundHead == NULL && RdmaInboundTail == NULL);
	memset(RdmaPeers, 0, sizeof(RdmaPeers));
	RdmaProvider = NULL;
	RdmaCtxOpen = false;
	IsUnderPostmaster = true;
	MyAuxProcType = LmsProcess;
	releases = 0;
}
UT_TEST(test_inactive_provider_is_explicit_not_fabricated)
{
	bool active = true;
	reset_test();
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(!active);
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_INVALID);
	MyAuxProcType = LmsProcess;
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_INVALID);
}
UT_TEST(test_residual_inbound_is_not_inactive_success)
{
	bool active;
	char bytes[4] = { 1, 2, 3, 4 }, out[4];
	int32 sender;
	size_t count;
	reset_test();
	rdma_inbound_enqueue(1, bytes, sizeof(bytes));
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(!active);
	UT_ASSERT(rdma_inbound_read(&sender, out, sizeof(out), &count));
	UT_ASSERT_EQ(count, sizeof(bytes));
	UT_ASSERT_EQ(memcmp(bytes, out, sizeof(bytes)), 0);
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_READY);
}
#ifndef PGRAC_TEST_RDMA_DISABLED
UT_TEST(test_all_six_private_responsibilities_hold_completion)
{
	bool active;
	ClusterICRdmaPeer *peer;
	reset_test();
	RdmaProvider = &provider;
	RdmaCtxOpen = true; /* Device readiness, not hardware proof. */
	peer = &RdmaPeers[1];
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(active);
	peer->send_busy = true;
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_PENDING);
	peer->send_busy = false; /* CQ callback boundary. */
	peer->queued_buf = (uint8 *)"frame";
	peer->queued_buf_len = peer->queued_len = 5;
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_PENDING);
	peer->queued_len = 0;
	peer->block_reply_send_busy = true;
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_PENDING);
	peer->block_reply_send_busy = false;
	UT_ASSERT(rdma_peer_add_pending_release(peer, release_callback, &releases));
	UT_ASSERT(rdma_peer_add_block_reply_pending_release(peer, release_callback, &releases));
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(releases, 0); /* Poll cannot release the raw pin. */
	rdma_peer_release_pending_send(peer);
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_PENDING);
	rdma_peer_release_block_reply_pending_send(peer);
	UT_ASSERT_EQ(releases, 2);
	peer->block_scratch_borrowed = true;
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_PENDING);
	rdma_peer_release_block_scratch(peer);
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_partial_inbound_real_read_and_later_invalid_peer)
{
	bool active;
	char bytes[4] = { 1, 2, 3, 4 }, out[4];
	int32 sender;
	size_t count;
	reset_test();
	RdmaProvider = &provider;
	RdmaCtxOpen = true;
	rdma_inbound_enqueue(1, bytes, sizeof(bytes));
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_PENDING);
	RdmaPeers[127].pending_release_count = 9;
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_INVALID);
	RdmaPeers[127].pending_release_count = 0;
	UT_ASSERT(rdma_inbound_read(&sender, out, 1, &count));
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(rdma_inbound_read(&sender, out + 1, 3, &count));
	UT_ASSERT_EQ(count, 3);
	UT_ASSERT_EQ(memcmp(bytes, out, 4), 0);
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_callback_residue_and_dead_provider_are_invalid)
{
	bool active;
	reset_test();
	RdmaProvider = &provider;
	RdmaCtxOpen = true;
	RdmaPeers[2].pending_release_cb[7] = release_callback;
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_INVALID);
	RdmaPeers[2].pending_release_cb[7] = NULL;
	RdmaPeers[2].queued_len = 1;
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_INVALID);
	RdmaPeers[2].queued_len = 0;
	UT_ASSERT(rdma_peer_add_pending_release(&RdmaPeers[2], release_callback, &releases));
	RdmaCtxOpen = false;
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(releases, 0);
	rdma_peer_release_pending_send(&RdmaPeers[2]);
	UT_ASSERT_EQ(poll_transport(&active), CLUSTER_NORMAL_STOP_READY);
}
#endif
int
main(void)
{
#ifdef PGRAC_TEST_RDMA_DISABLED
	UT_PLAN(2);
	/* The shared extraction intentionally also contains enabled-only bodies. */
	(void)rdma_peer_release_pending_send;
	(void)rdma_peer_release_block_reply_pending_send;
	(void)rdma_peer_add_pending_release;
	(void)rdma_peer_add_block_reply_pending_release;
	(void)rdma_peer_release_block_scratch;
	(void)provider;
	(void)release_callback;
#else
	UT_PLAN(5);
#endif
	UT_RUN(test_inactive_provider_is_explicit_not_fabricated);
	UT_RUN(test_residual_inbound_is_not_inactive_success);
#ifndef PGRAC_TEST_RDMA_DISABLED
	UT_RUN(test_all_six_private_responsibilities_hold_completion);
	UT_RUN(test_partial_inbound_real_read_and_later_invalid_peer);
	UT_RUN(test_callback_residue_and_dead_provider_are_invalid);
#endif
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
