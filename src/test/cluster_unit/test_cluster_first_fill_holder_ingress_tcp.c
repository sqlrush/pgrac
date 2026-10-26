/*
 * Production holder ingress over the real tier1 TCP receive loop.
 * Authentication, unrelated active producers and the SEAL endpoint are
 * controlled boundaries. The whole holder/SEAL handler is exercised separately
 * by test_cluster_first_fill_holder_ingress. No native admission is implied.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#define FIRST_FILL_TCP_FIXTURE_ONLY
#include "test_cluster_first_fill_ingress_tcp.c"
#undef FIRST_FILL_TCP_FIXTURE_ONLY
#include "cluster/cluster_pcm_read_source.h"

static bool
gcs_first_fill_holder_seal_pending(const GcsBlockFirstFillForwardPayload *forward pg_attribute_unused(),
	uint64 session pg_attribute_unused())
{
	return false; /* Quiet receipts are covered by the actual endpoint fixture. */
}

#define cluster_lms_workers 2
#define gcs_block_pcm_x_authenticated_session ingress_auth
#define cluster_gcs_lookup_master(tag) UT_PEER_ID
#define cluster_membership_get_state(node) CLUSTER_MEMBER_MEMBER
#define cluster_epoch_get_current() ut_epoch
#define MemoryContextAllocZero(context, bytes) calloc(1, (bytes))
#define pfree free
#define cluster_block_io_trace_note(...) ((void)0)
#undef ereport
#define ereport(level, rest) do { if ((level) >= ERROR) pg_re_throw(); } while (0)
#include "test_cluster_first_fill_holder_ingress_tcp.inc"
#undef cluster_block_io_trace_note
#undef pfree
#undef MemoryContextAllocZero
#undef cluster_epoch_get_current
#undef cluster_membership_get_state
#undef cluster_gcs_lookup_master
#undef gcs_block_pcm_x_authenticated_session
#undef cluster_lms_workers

static unsigned holder_received, holder_sealed, holder_following;

static GcsBlockFirstFillForwardPayload
holder_tcp_forward(unsigned i)
{
	GcsBlockFirstFillForwardPayload forward = {0};
	forward.forward.tag.blockNum = 400 + i;
	forward.forward.request_id = gcs_reqid_requester(1, 0, i + 1);
	forward.forward.epoch = ut_epoch;
	forward.forward.original_requester_node = 1;
	forward.forward.requester_backend_id = 1;
	forward.forward.master_node = UT_PEER_ID;
	forward.forward.transition_id = PCM_TRANS_N_TO_S;
	forward.forward.reserved_0[0] = forward.forward.reserved_0[3] = 1;
	forward.forward.reserved_0[6] = GCS_BLOCK_FORWARD_KIND_FIRST_FILL_S;
	forward.binding_generation = 7; forward.service_generation = 1;
	UT_ASSERT(GcsBlockFirstFillForwardPayloadValid(&forward, sizeof(forward)));
	return forward;
}

static ClusterICDispatchResult
holder_tcp_dispatch(const ClusterICEnvelope *env, const void *payload, int32 peer)
{
	uint64 session;
	UT_ASSERT_EQ(peer, UT_PEER_ID);
	UT_ASSERT_EQ(tier1_my_plane, CLUSTER_IC_PLANE_DATA);
	UT_ASSERT_EQ(cluster_ic_tier1_my_data_channel(), 0);
	UT_ASSERT(ingress_auth(peer, env->epoch, &session));
	if (env->msg_type == PGRAC_IC_MSG_GCS_BLOCK_FORWARD) {
		bool created;
		UT_ASSERT_EQ(env->payload_length, sizeof(GcsBlockFirstFillForwardPayload));
		UT_ASSERT(gcs_first_fill_holder_reserve(payload, session, &created) == NULL);
		if (!gcs_first_fill_holder_ingress_retain(env, payload, session))
			return CLUSTER_IC_DISPATCH_PENDING;
		holder_received++;
	} else if (env->msg_type == PGRAC_IC_MSG_GCS_BLOCK_DONE) {
		const GcsBlockDonePayload *done = payload;
		UT_ASSERT_EQ(env->payload_length, sizeof(*done));
		if (done->reserved_0[0] == GCS_BLOCK_DONE_PENDING_S_SEAL) {
			ClusterPcmReadGrant service;
			GcsBlockFirstFillForwardPayload forward = {0};
			UT_ASSERT_EQ(done->sender_node, peer);
			UT_ASSERT_EQ(done->epoch, ut_epoch);
			UT_ASSERT(GcsBlockDonePayloadPendingSService(done, &service));
			UT_ASSERT_EQ(service.holder_node, cluster_node_id);
			forward.forward.tag = service.key.tag;
			forward.forward.request_id = service.key.request_id;
			forward.forward.epoch = service.key.epoch;
			forward.forward.original_requester_node = service.key.requester_node;
			forward.forward.requester_backend_id = service.key.backend_id;
			forward.forward.master_node = peer;
			forward.forward.transition_id = PCM_TRANS_N_TO_S;
			forward.forward.reserved_0[0] = forward.forward.reserved_0[3] = service.holder_was_x;
			forward.forward.reserved_0[6] = GCS_BLOCK_FORWARD_KIND_FIRST_FILL_S;
			forward.binding_generation = service.binding_generation;
			forward.service_generation = service.service_generation;
			/* Bridge only the already-validated SEAL endpoint boundary. */
			gcs_first_fill_holder_ingress_cancel(&forward, session);
			holder_sealed++;
		} else {
			UT_ASSERT_EQ(done->request_id, 0);
			holder_following++;
		}
	} else
		UT_ASSERT(false);
	return CLUSTER_IC_DISPATCH_DONE;
}

static void
holder_tcp_drain(unsigned forwards, unsigned seals, unsigned following)
{
	for (int i = 0; i < 8 && (holder_received < forwards || holder_sealed < seals
		|| holder_following < following); i++) {
		fd_set fds;
		struct timeval tv = {1, 0};
		FD_ZERO(&fds); FD_SET(ut_tx_fd, &fds);
		UT_ASSERT_EQ(select(ut_tx_fd + 1, &fds, NULL, NULL, &tv), 1);
		UT_ASSERT(cluster_ic_tier1_recv_heartbeat_drain(UT_PEER_ID, ut_tx_fd));
	}
	UT_ASSERT_EQ(holder_received, forwards);
	UT_ASSERT_EQ(holder_sealed, seals);
	UT_ASSERT_EQ(holder_following, following);
	UT_ASSERT_EQ(tier1_recv_buf_len[UT_PEER_ID], 0);
}

UT_TEST(full_holder_forward_does_not_hide_same_peer_seal_or_following_completion)
{
	GcsBlockDonePayload done = {0}, completion = {0};
	GcsBlockFirstFillForwardPayload original = holder_tcp_forward(0);
	ClusterPcmReadGrant service;
	const char *reason;
	ingress_authenticated = true; ingress_session = 19;
	ut_dispatch_hook = holder_tcp_dispatch;
	/* Controlled full active-producer lot; no native producer is invented. */
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++) {
		bool created;
		GcsBlockFirstFillForwardPayload other = holder_tcp_forward(1000 + i);
		UT_ASSERT(gcs_first_fill_holder_reserve(&other, ingress_session, &created) != NULL);
		UT_ASSERT(created);
	}
	for (unsigned i = 0; i < 8; i++) {
		GcsBlockFirstFillForwardPayload forward = holder_tcp_forward(i);
		ingress_send(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &forward, sizeof(forward));
	}
	holder_tcp_drain(8, 0, 0);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 8);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_credit[UT_PEER_ID], 8);
	UT_ASSERT_EQ(memcmp(&gcs_first_fill_holder_ingress_head->forward, &original, sizeof(original)), 0);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_head->master_session, 19);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_head->envelope.msg_type, PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_head->envelope.source_node_id, UT_PEER_ID);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(GcsBlockFirstFillForwardPayloadGrant(&original, cluster_node_id, &service));
	done.tag = service.key.tag; done.request_id = service.key.request_id;
	done.epoch = ut_epoch; done.sender_node = UT_PEER_ID;
	done.requester_backend_id = service.key.backend_id; done.transition_id = PCM_TRANS_N_TO_S;
	done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_SEAL;
	done.reserved_0[1] = service.key.requester_node;
	GcsBlockDonePayloadSetPendingSService(&done, &service);
	ingress_send(PGRAC_IC_MSG_GCS_BLOCK_DONE, &done, sizeof(done));
	ingress_send(PGRAC_IC_MSG_GCS_BLOCK_DONE, &completion, sizeof(completion));
	holder_tcp_drain(8, 1, 1);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 7);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_credit[UT_PEER_ID], 7);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_head->forward.forward.request_id,
		holder_tcp_forward(1).forward.request_id);
	/* Neither following frame needed to free or overwrite an unrelated H slot. */
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++)
		UT_ASSERT(gcs_first_fill_holder_parks[i].in_use);
	while (gcs_first_fill_holder_ingress_head != NULL)
		gcs_first_fill_holder_ingress_forget(gcs_first_fill_holder_ingress_head);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_poll(&reason), CLUSTER_NORMAL_STOP_READY);
	cluster_ic_tier1_close_peer(UT_PEER_ID, NULL);
	close(ut_rx_fd); ut_rx_fd = -1;
}

int
main(void)
{
	MyProcPid = getpid();
	setvbuf(stdout, NULL, _IOLBF, 0);
	MyAuxProcType = LmsProcess;
	cluster_ic_tier1_set_my_data_channel(0, 2);
	UT_PLAN(2);
	UT_RUN(ingress_connects_actual_data_channel);
	UT_RUN(full_holder_forward_does_not_hide_same_peer_seal_or_following_completion);
	UT_DONE();
	return ut_failed_count != 0;
}
