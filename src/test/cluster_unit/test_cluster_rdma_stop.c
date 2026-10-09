/* Author: SqlRush <sqlrush@gmail.com> */
/* Verbatim production private types, queue/release bodies and stop observer.
 * No verbs/CM implementation, hardware, CQ completion, or real pin is faked as
 * proven: callback release invocation and device readiness are boundaries. */
#include "postgres.h"
#include "miscadmin.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_xnode_profile.h"
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
static void rdma_peer_fail_or_fallback(int32 peer, const char *reason);
struct ClusterICRdmaPeer;
static bool rdma_post_peer_recv(struct ClusterICRdmaPeer *peer);
static const char *RdmaUnavailableReason;
#include "test_cluster_rdma_stop.inc"
#undef printf
#undef fprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_xnode_profile_enabled;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl;
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
int cluster_node_id = 0;
static ClusterICDispatchResult dispatch_result;
static unsigned dispatch_calls;
static unsigned peer_failures;
static unsigned dispatch_value;
static int pending_peer = -1;
static bool close_on_dispatch;
static unsigned receive_posts[CLUSTER_MAX_NODES];

static bool
rdma_post_peer_recv(ClusterICRdmaPeer *peer)
{
	receive_posts[peer->peer_id]++;
	return true;
}

void
cluster_ic_rdma_stats_note_recv(int32 peer, uint64 bytes, bool rdma)
{}

ClusterICEnvelopeVerifyResult
cluster_ic_envelope_verify(const ClusterICEnvelope *env, const void *payload, uint32 payload_len,
						   uint32 self, int32 peer)
{
	return CLUSTER_IC_ENVELOPE_OK;
}

ClusterICDispatchResult
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *payload, int32 peer)
{
	ClusterICDispatchResult result = dispatch_result;

	if (pending_peer >= 0 && peer != pending_peer)
		result = CLUSTER_IC_DISPATCH_DONE;
	if (result == CLUSTER_IC_DISPATCH_DONE) {
		dispatch_calls++;
		dispatch_value = *(const uint8 *)payload;
		if (close_on_dispatch) {
			RdmaPeers[peer].connected = false;
			RdmaPeers[peer].id = NULL;
		}
	}
	return result;
}

ClusterICDispatchResult
cluster_ic_dispatch_envelope_profiled(const ClusterICEnvelope *env, const void *payload, int32 peer,
									  ClusterXpScope *scope)
{
	ClusterICDispatchResult result = cluster_ic_dispatch_envelope(env, payload, peer);

	if (result != CLUSTER_IC_DISPATCH_PENDING)
		cluster_xp_profile_end(scope);
	return result;
}

void
cluster_ic_rdma_stats_note_error(int32 peer, const char *sqlstate, const char *reason)
{}

static void
rdma_peer_fail_or_fallback(int32 peer, const char *reason)
{
	peer_failures++;
	rdma_inbound_drop_peer(peer);
}

UT_TEST(test_pending_dispatch_keeps_exact_queue_head_without_completion_event)
{
	struct {
		ClusterICEnvelope env;
		uint8 payload[4];
	} frame = { 0 };
	ClusterICRdmaInboundFrame *original;
	ClusterXnodeProfileShared profile = { 0 };
	instr_time arrival;

	ClusterXnodeProfileCtl = &profile;
	cluster_xnode_profile_enabled = true;
	frame.env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_REQUEST;

	frame.env.payload_length = sizeof(frame.payload);
	frame.payload[0] = 71;
	rdma_inbound_enqueue(1, &frame, sizeof(frame));
	original = RdmaInboundHead;
	UT_ASSERT(original->queue_scope.active);
	arrival = original->queue_scope.start;
	dispatch_result = CLUSTER_IC_DISPATCH_PENDING;
	rdma_dispatch_pending_frames();
	UT_ASSERT_EQ(dispatch_calls, 0);
	UT_ASSERT_EQ(peer_failures, 0);
	UT_ASSERT(RdmaInboundHead == original && RdmaInboundTail == original);
	UT_ASSERT_EQ(original->consumed, 0);
	UT_ASSERT(original->queue_scope.active);
	UT_ASSERT(memcmp(&arrival, &original->queue_scope.start, sizeof(arrival)) == 0);
	UT_ASSERT_EQ(memcmp(original->data, &frame, sizeof(frame)), 0);
	/* No new CQ event: the original loop's next pass retries the same head. */
	dispatch_result = CLUSTER_IC_DISPATCH_DONE;
	rdma_dispatch_pending_frames();
	UT_ASSERT_EQ(dispatch_calls, 1);
	UT_ASSERT_EQ(dispatch_value, 71);
	UT_ASSERT(RdmaInboundHead == NULL && RdmaInboundTail == NULL);
	rdma_dispatch_pending_frames();
	UT_ASSERT_EQ(dispatch_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&profile.bucket[CLXP_LMS_RDMA_QUEUE_BLOCK].n_events), 1);
	cluster_xnode_profile_enabled = false;
	ClusterXnodeProfileCtl = NULL;
	/* A genuine peer rejection still closes/purges its queued frames. */
	rdma_inbound_enqueue(1, &frame, sizeof(frame));
	rdma_inbound_enqueue(1, &frame, sizeof(frame));
	dispatch_result = CLUSTER_IC_DISPATCH_REJECTED;
	rdma_dispatch_pending_frames();
	UT_ASSERT_EQ(peer_failures, 1);
	UT_ASSERT(RdmaInboundHead == NULL && RdmaInboundTail == NULL);
}

UT_TEST(test_pending_holds_receive_credit_but_not_other_peers)
{
	struct {
		ClusterICEnvelope env;
		uint8 payload[4];
	} frame = { 0 };
	ClusterICRdmaInboundFrame *original;
	unsigned calls = dispatch_calls;
	int i;

	frame.env.payload_length = sizeof(frame.payload);
	frame.payload[0] = 72;
	memset(receive_posts, 0, sizeof(receive_posts));
	for (i = 1; i <= 2; i++) {
		RdmaPeers[i].peer_id = i;
		RdmaPeers[i].connected = true;
		RdmaPeers[i].id = (struct rdma_cm_id *)&RdmaPeers[i];
		RdmaPeers[i].recv_buf = (uint8 *)&frame;
		RdmaPeers[i].recv_buf_len = sizeof(frame);
		rdma_process_recv_completion(&RdmaPeers[i], sizeof(frame));
	}
	original = RdmaInboundHead;
	pending_peer = 1;
	dispatch_result = CLUSTER_IC_DISPATCH_PENDING;
	rdma_dispatch_pending_frames();
	UT_ASSERT_EQ(receive_posts[1], 0);
	UT_ASSERT_EQ(receive_posts[2], 1);
	UT_ASSERT_EQ(dispatch_calls, calls + 1);
	UT_ASSERT(RdmaInboundHead == original && RdmaInboundTail == original);
	UT_ASSERT_EQ(memcmp(original->data, &frame, sizeof(frame)), 0);
	rdma_dispatch_pending_frames();
	UT_ASSERT_EQ(receive_posts[1], 0);
	UT_ASSERT_EQ(dispatch_calls, calls + 1);
	dispatch_result = CLUSTER_IC_DISPATCH_DONE;
	rdma_dispatch_pending_frames();
	UT_ASSERT_EQ(receive_posts[1], 1);
	UT_ASSERT_EQ(dispatch_calls, calls + 2);
	UT_ASSERT(RdmaInboundHead == NULL && RdmaInboundTail == NULL);
	pending_peer = -1;
	/* A handler-triggered disconnect cannot grant a fresh receive credit. */
	rdma_process_recv_completion(&RdmaPeers[1], sizeof(frame));
	close_on_dispatch = true;
	rdma_dispatch_pending_frames();
	close_on_dispatch = false;
	UT_ASSERT_EQ(receive_posts[1], 1);
	UT_ASSERT(RdmaInboundHead == NULL && RdmaInboundTail == NULL);
}

UT_TEST(test_completion_before_established_preserves_receive_credit)
{
	struct {
		ClusterICEnvelope env;
		uint8 payload[4];
	} frame = { 0 };
	unsigned calls = dispatch_calls;

	frame.env.payload_length = sizeof(frame.payload);
	RdmaPeers[1].id = (struct rdma_cm_id *)&RdmaPeers[1];
	RdmaPeers[1].connected = false;
	RdmaPeers[1].recv_buf = (uint8 *)&frame;
	RdmaPeers[1].recv_buf_len = sizeof(frame);
	receive_posts[1] = 0;
	/* prepare_peer already posts the first receive, before ESTABLISHED. */
	rdma_process_recv_completion(&RdmaPeers[1], sizeof(frame));
	dispatch_result = CLUSTER_IC_DISPATCH_DONE;
	rdma_dispatch_pending_frames();
	UT_ASSERT_EQ(receive_posts[1], 1);
	UT_ASSERT_EQ(dispatch_calls, calls + 1);
	RdmaPeers[1].connected = true; /* Later CM event must not post again. */
	rdma_dispatch_pending_frames();
	UT_ASSERT_EQ(receive_posts[1], 1);
	UT_ASSERT_EQ(dispatch_calls, calls + 1);
	UT_ASSERT(RdmaInboundHead == NULL && RdmaInboundTail == NULL);
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
	UT_PLAN(5);
	/* The shared extraction intentionally also contains enabled-only bodies. */
	(void)rdma_peer_release_pending_send;
	(void)rdma_peer_release_block_reply_pending_send;
	(void)rdma_peer_add_pending_release;
	(void)rdma_peer_add_block_reply_pending_release;
	(void)rdma_peer_release_block_scratch;
	(void)provider;
	(void)release_callback;
#else
	UT_PLAN(8);
#endif
	UT_RUN(test_inactive_provider_is_explicit_not_fabricated);
	UT_RUN(test_residual_inbound_is_not_inactive_success);
#ifndef PGRAC_TEST_RDMA_DISABLED
	UT_RUN(test_all_six_private_responsibilities_hold_completion);
	UT_RUN(test_partial_inbound_real_read_and_later_invalid_peer);
	UT_RUN(test_callback_residue_and_dead_provider_are_invalid);
#endif
	UT_RUN(test_pending_dispatch_keeps_exact_queue_head_without_completion_event);
	UT_RUN(test_pending_holds_receive_credit_but_not_other_peers);
	UT_RUN(test_completion_before_established_preserves_receive_credit);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
