/*
 * Production master ingress ownership over the real tier1 TCP receive loop.
 * Native admission/cancellation and envelope authentication are controlled
 * boundaries here. test_cluster_pcm_read_grant exercises real PCM admission;
 * this fixture proves that retaining the original request does not retain the
 * TCP head ahead of the cleanup that makes admission possible.
 * Author: SqlRush <sqlrush@gmail.com>
 */
int first_fill_tcp_base_main(void);
#define main first_fill_tcp_base_main
#include "test_cluster_ic_tier1_partial.c"
#undef main

#include "cluster/cluster_cssd.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_gcs_reqid.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_pcm_lock.h"

int MaxBackends = 32;
static bool ingress_capacity, cleanup_capacity, ingress_deferred;
static bool ingress_authenticated = true;
static uint64 ingress_session = 19;
static unsigned ingress_received, ingress_admitted, ingress_cancelled, ingress_cleanup;
static GcsBlockRequestPayload ingress_original[8];
static GcsBlockRequestPayload ingress_installed[8];
static bool ingress_native_absent;
static ClusterICSendResult ingress_return_send;
static unsigned ingress_return_notices;
static GcsBlockDonePayload ingress_return_notice;

static ClusterICSendResult
gcs_block_send_envelope_or_loopback(uint8 type, int32 dest, const void *payload, uint32 bytes)
{
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GCS_BLOCK_DONE);
	UT_ASSERT_EQ(dest, UT_PEER_ID);
	UT_ASSERT_EQ(bytes, sizeof(ingress_return_notice));
	ingress_return_notice = *(const GcsBlockDonePayload *)payload;
	ingress_return_notices++;
	return ingress_return_send;
}

static void
gcs_first_fill_master_trace(const GcsBlockRequestPayload *req pg_attribute_unused(),
	const char *phase pg_attribute_unused(), int result pg_attribute_unused())
{
}

static bool
ingress_auth(int node, uint64 epoch, uint64 *session)
{
	UT_ASSERT_EQ(node, UT_PEER_ID);
	*session = ingress_session;
	return ingress_authenticated && epoch == ut_epoch;
}

static bool
gcs_block_first_fill_try_ingress(const GcsBlockRequestPayload *req,
	const GcsBlockDedupKey *key, int worker)
{
	UT_ASSERT_EQ(worker, 0);
	UT_ASSERT_EQ(key->request_id, req->request_id);
	if (!ingress_capacity)
		return false;
	UT_ASSERT(ingress_admitted < lengthof(ingress_installed));
	ingress_installed[ingress_admitted++] = *req;
	return true;
}

static bool
gcs_first_fill_master_cancel_owned(const ClusterPcmReadGrantKey *key, int worker,
	uint32 original_lifetime_ms)
{
	UT_ASSERT_EQ(worker, 0);
	UT_ASSERT_EQ(key->requester_node, UT_PEER_ID);
	UT_ASSERT_EQ(original_lifetime_ms, 5000);
	if (!cleanup_capacity)
		return false;
	ingress_cancelled++;
	return true;
}

#define GCS_FIRST_FILL_PARK_MAX 64
#define cluster_lms_workers 2
#define gcs_block_pcm_x_authenticated_session ingress_auth
#define cluster_gcs_lookup_master(tag) cluster_node_id
#define cluster_membership_get_state(node) CLUSTER_MEMBER_MEMBER
#define cluster_normal_stop_requested() false
#define cluster_authority_readiness_managed() false
#define cluster_serving_ready_check(pending, failed) true
#define cluster_qvotec_in_quorum() true
#define cluster_membership_is_member(node) true
#define cluster_grd_join_remaster_active_for_shard(tag) false
#define cluster_grd_block_view_rebuilt(tag) true
#define cluster_gcs_block_phase_for_tag(tag) GCS_BLOCK_NORMAL
#define MemoryContextAllocZero(context, size) calloc(1, (size))
#define cluster_ic_dispatch_defer(env) (ingress_deferred = true)
#define cluster_pcm_lock_read_grant_absent(key) ingress_native_absent
#define cluster_block_io_trace_pending_s(key, phase, state, holder, binding, result) ((void)0)
#define cluster_ic_suppress_gcs_done_cap false
#define cluster_sf_peer_supports_gcs_done(node) true
#include "test_cluster_first_fill_ingress.inc"
#undef cluster_sf_peer_supports_gcs_done
#undef cluster_ic_suppress_gcs_done_cap
#undef cluster_block_io_trace_pending_s
#undef cluster_pcm_lock_read_grant_absent
#undef cluster_ic_dispatch_defer
#undef MemoryContextAllocZero
#undef cluster_gcs_block_phase_for_tag
#undef cluster_grd_block_view_rebuilt
#undef cluster_grd_join_remaster_active_for_shard
#undef cluster_membership_is_member
#undef cluster_qvotec_in_quorum
#undef cluster_authority_readiness_managed
#undef cluster_serving_ready_check
#undef cluster_normal_stop_requested
#undef cluster_membership_get_state
#undef cluster_gcs_lookup_master
#undef gcs_block_pcm_x_authenticated_session
#undef cluster_lms_workers

static ClusterICDispatchResult
ingress_dispatch(const ClusterICEnvelope *env, const void *payload, int32 peer)
{
	UT_ASSERT_EQ(peer, UT_PEER_ID);
	UT_ASSERT_EQ(tier1_my_plane, CLUSTER_IC_PLANE_DATA);
	UT_ASSERT_EQ(cluster_ic_tier1_my_data_channel(), 0);
	ingress_deferred = false;
	if (env->msg_type == PGRAC_IC_MSG_GCS_BLOCK_REQUEST) {
		const GcsBlockRequestPayload *req = payload;
		GcsBlockDedupKey key = {0};
		UT_ASSERT_EQ(env->payload_length, sizeof(*req));
		key.origin_node_id = req->sender_node;
		key.requester_backend_id = req->requester_backend_id;
		key.request_id = req->request_id; key.cluster_epoch = req->epoch;
		gcs_block_first_fill_ingress(env, req, &key, 0);
		ingress_received++;
	} else if (env->msg_type == PGRAC_IC_MSG_GCS_BLOCK_DONE) {
		const GcsBlockDonePayload *done = payload;
		UT_ASSERT_EQ(env->payload_length, sizeof(*done));
		if (done->reserved_0[0] == GCS_BLOCK_DONE_PENDING_S_RETURN) {
			ClusterPcmReadGrant service;
			uint64 session;
			if (env->source_node_id != (uint32)done->sender_node
				|| done->epoch != ut_epoch || !GcsBlockDonePayloadPendingSService(done, &service)
				|| !ingress_auth(done->sender_node, done->epoch, &session)
				|| !gcs_first_fill_return_receipt_retain(&service, session))
				ingress_deferred = true;
		} else if (done->request_id != 0) {
			ClusterPcmReadGrantKey key = {0};
			key.tag = done->tag; key.request_id = done->request_id; key.epoch = done->epoch;
			key.requester_node = done->sender_node; key.backend_id = done->requester_backend_id;
			(void)gcs_first_fill_master_cancel(&key, 0);
		} else {
			/* Controlled completion of an earlier native owner. The TCP
			 * parser and local retained-request lifecycle remain production. */
			ingress_capacity = cleanup_capacity = true;
			gcs_first_fill_ingress_tick(0);
		}
		ingress_cleanup++;
	} else
		UT_ASSERT(false);
	return ingress_deferred ? CLUSTER_IC_DISPATCH_PENDING : CLUSTER_IC_DISPATCH_DONE;
}

static void
ingress_reset(void)
{
	while (gcs_first_fill_ingress_head != NULL)
		gcs_first_fill_ingress_forget(gcs_first_fill_ingress_head);
	memset(gcs_first_fill_return_receipts, 0, sizeof(gcs_first_fill_return_receipts));
	gcs_first_fill_return_receipt_count = 0;
	ingress_native_absent = true;
	ingress_return_send = CLUSTER_IC_SEND_NOT_ADMITTED;
	ingress_return_notices = 0;
	ingress_capacity = cleanup_capacity = false;
	ingress_authenticated = true;
	ingress_session = 19;
	ingress_received = ingress_admitted = ingress_cancelled = ingress_cleanup = 0;
	memset(ingress_installed, 0, sizeof(ingress_installed));
	ut_dispatch_hook = ingress_dispatch;
	for (unsigned i = 0; i < lengthof(ingress_original); i++) {
		GcsBlockRequestPayload *req = &ingress_original[i];
		memset(req, 0, sizeof(*req));
		req->tag.blockNum = 300 + i;
		req->sender_node = UT_PEER_ID; req->requester_backend_id = 1;
		req->request_id = gcs_reqid_requester(UT_PEER_ID, 0, i + 1);
		req->epoch = ut_epoch; req->transition_id = PCM_TRANS_N_TO_S;
		GcsBlockRequestPayloadSetReadMiss(req, true);
		GcsBlockRequestPayloadSetLifetimeHintMs(req, 5000);
	}
}

static void
ingress_send(uint8 type, const void *payload, Size bytes)
{
	ClusterICEnvelope env = {0};
	char wire[PGRAC_IC_ENVELOPE_BYTES + sizeof(GcsBlockDonePayload) + sizeof(GcsBlockRequestPayload)];
	env.msg_type = type; env.source_node_id = UT_PEER_ID;
	env.dest_node_id = cluster_node_id; env.epoch = ut_epoch;
	env.payload_length = bytes;
	UT_ASSERT_EQ(sizeof(env), PGRAC_IC_ENVELOPE_BYTES);
	UT_ASSERT(sizeof(env) + bytes <= sizeof(wire));
	memcpy(wire, &env, sizeof(env));
	memcpy(wire + sizeof(env), payload, bytes);
	UT_ASSERT_EQ(send(ut_rx_fd, wire, sizeof(env) + bytes, 0), sizeof(env) + bytes);
}

static void
ingress_drain(unsigned requests, unsigned cleanup)
{
	for (int i = 0; i < 8 && (ingress_received < requests || ingress_cleanup < cleanup); i++) {
		fd_set fds;
		struct timeval tv = {1, 0};
		FD_ZERO(&fds); FD_SET(ut_tx_fd, &fds);
		UT_ASSERT_EQ(select(ut_tx_fd + 1, &fds, NULL, NULL, &tv), 1);
		UT_ASSERT(cluster_ic_tier1_recv_heartbeat_drain(UT_PEER_ID, ut_tx_fd));
	}
	UT_ASSERT_EQ(ingress_received, requests);
	UT_ASSERT_EQ(ingress_cleanup, cleanup);
	UT_ASSERT_EQ(tier1_recv_buf_len[UT_PEER_ID], 0);
}

UT_TEST(full_backend_credit_does_not_hide_following_same_peer_completion)
{
	GcsBlockDonePayload done = {0};
	const char *reason;
	ingress_reset();
	for (unsigned i = 0; i < lengthof(ingress_original); i++)
		ingress_send(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, &ingress_original[i], sizeof(ingress_original[i]));
	ingress_drain(8, 0);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 8);
	UT_ASSERT_EQ(ingress_admitted, 0);
	UT_ASSERT_EQ(gcs_first_fill_ingress_poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	/* All previous payload storage can be overwritten by the next frame. */
	ingress_send(PGRAC_IC_MSG_GCS_BLOCK_DONE, &done, sizeof(done));
	ingress_drain(8, 1);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 0);
	UT_ASSERT_EQ(ingress_admitted, 8);
	UT_ASSERT_EQ(memcmp(ingress_installed, ingress_original, sizeof(ingress_original)), 0);
	UT_ASSERT_EQ(gcs_first_fill_ingress_poll(&reason), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(ingress_connects_actual_data_channel)
{
	int port;
	int listener;
	struct sockaddr_in address;
	socklen_t bytes = sizeof(address);
	fd_set fds;
	struct timeval tv = {1, 0};
	cluster_ic_tier1_shmem_register();
	UT_ASSERT(ut_captured_region != NULL);
	ut_captured_region->init_fn();
	listener = ut_open_listener(&port);
	snprintf(ut_peer_info.data_addr, sizeof(ut_peer_info.data_addr), "127.0.0.1:%d", port);
	ut_peer_info.node_id = UT_PEER_ID;
	ut_peer_declared = true;
	if (!cluster_ic_tier1_connect_one(UT_PEER_ID, &ut_tx_fd)) {
		UT_ASSERT(false);
		close(listener);
		return;
	}
	FD_ZERO(&fds); FD_SET(listener, &fds);
	UT_ASSERT_EQ(select(listener + 1, &fds, NULL, NULL, &tv), 1);
	if (!FD_ISSET(listener, &fds)) {
		close(listener);
		return;
	}
	ut_rx_fd = accept(listener, (struct sockaddr *)&address, &bytes);
	UT_ASSERT(ut_rx_fd >= 0);
	UT_ASSERT(cluster_ic_tier1_finish_connect(UT_PEER_ID, ut_tx_fd));
	UT_ASSERT_EQ(cluster_ic_tier1_hello_send_remaining(UT_PEER_ID), 0);
	close(listener);
}

UT_TEST(following_cancel_seals_original_until_exact_cleanup_takes_over)
{
	GcsBlockDonePayload done = {0};
	ingress_reset();
	ingress_send(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, &ingress_original[0], sizeof(ingress_original[0]));
	done.request_id = ingress_original[0].request_id; done.epoch = ut_epoch;
	done.tag = ingress_original[0].tag; done.sender_node = UT_PEER_ID;
	done.requester_backend_id = 1;
	ingress_send(PGRAC_IC_MSG_GCS_BLOCK_DONE, &done, sizeof(done));
	ingress_drain(1, 1);
	UT_ASSERT(gcs_first_fill_ingress_head->cancelling);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 1);
	UT_ASSERT_EQ(ingress_admitted + ingress_cancelled, 0);
	memset(&done, 0, sizeof(done));
	ingress_send(PGRAC_IC_MSG_GCS_BLOCK_DONE, &done, sizeof(done));
	ingress_drain(1, 2);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 0);
	UT_ASSERT_EQ(ingress_cancelled, 1);
	UT_ASSERT_EQ(ingress_admitted, 0);
}

UT_TEST(orphan_return_leaves_following_same_peer_completion_readable)
{
	ClusterPcmReadGrant service = {0}, echoed;
	GcsBlockDonePayload returned = {0}, completion = {0};
	const char *reason;
	ingress_reset();
	service.key.tag.blockNum = 99;
	service.key.request_id = gcs_reqid_requester(1, 0, 99);
	service.key.epoch = ut_epoch; service.key.requester_node = 1; service.key.backend_id = 1;
	service.holder_node = UT_PEER_ID; service.binding_generation = 7; service.service_generation = 1;
	returned.request_id = service.key.request_id; returned.epoch = ut_epoch;
	returned.tag = service.key.tag; returned.sender_node = UT_PEER_ID; returned.requester_backend_id = 1;
	returned.transition_id = PCM_TRANS_N_TO_S;
	returned.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_RETURN;
	returned.reserved_0[1] = service.key.requester_node;
	GcsBlockDonePayloadSetPendingSService(&returned, &service);
	ingress_send(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, &ingress_original[0], sizeof(ingress_original[0]));
	ingress_send(PGRAC_IC_MSG_GCS_BLOCK_DONE, &returned, sizeof(returned));
	ingress_send(PGRAC_IC_MSG_GCS_BLOCK_DONE, &completion, sizeof(completion));
	ingress_drain(1, 2);
	UT_ASSERT_EQ(ingress_admitted, 1); /* Completion behind RETURN released capacity. */
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 1);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	gcs_first_fill_return_receipt_tick(0);
	UT_ASSERT_EQ(ingress_return_notices, 1);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 1);
	UT_ASSERT(GcsBlockDonePayloadPendingSService(&ingress_return_notice, &echoed));
	UT_ASSERT(gcs_first_fill_return_same_service(&service, &echoed));
	ingress_return_send = CLUSTER_IC_SEND_WOULD_BLOCK;
	ut_now += 10000;
	gcs_first_fill_return_receipt_tick(0);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_count, 0);
	gcs_first_fill_return_receipt_tick(0);
	UT_ASSERT_EQ(ingress_return_notices, 2);
	UT_ASSERT_EQ(gcs_first_fill_return_receipt_poll(&reason), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(socket_close_or_unknown_session_does_not_erase_master_responsibility)
{
	ingress_reset();
	ingress_send(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, &ingress_original[0], sizeof(ingress_original[0]));
	ingress_drain(1, 0);
	cluster_ic_tier1_close_peer(UT_PEER_ID, NULL);
	close(ut_rx_fd); ut_rx_fd = -1;
	ingress_authenticated = false;
	gcs_first_fill_ingress_tick(0);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 1);
	ingress_authenticated = true;
	ingress_session++;
	gcs_first_fill_ingress_tick(0);
	UT_ASSERT_EQ(gcs_first_fill_ingress_count, 0);
	UT_ASSERT_EQ(ingress_admitted + ingress_cancelled, 0);
}

#ifndef FIRST_FILL_TCP_FIXTURE_ONLY
int
main(void)
{
	MyProcPid = getpid();
	setvbuf(stdout, NULL, _IOLBF, 0);
	MyAuxProcType = LmsProcess;
	cluster_ic_tier1_set_my_data_channel(0, 2);
	UT_PLAN(5);
	UT_RUN(ingress_connects_actual_data_channel);
	UT_RUN(full_backend_credit_does_not_hide_following_same_peer_completion);
	UT_RUN(following_cancel_seals_original_until_exact_cleanup_takes_over);
	UT_RUN(orphan_return_leaves_following_same_peer_completion_readable);
	UT_RUN(socket_close_or_unknown_session_does_not_erase_master_responsibility);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
#endif
