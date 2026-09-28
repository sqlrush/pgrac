/*-------------------------------------------------------------------------
 *
 * test_cluster_config_prefix_transport.c
 *    Real native TCP/FIFO/framing and prefix exchange, on both planes.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_config_prefix_transport.c
 * NOTES
 *    Native tier1, envelope CRC/parser and prefix owner are production code.
 *    HELLO construction, shared capabilities, randomness, epoch/SCN and the
 *    peer application are explicit fixtures. This is not distributed APPLY.
 *-------------------------------------------------------------------------
 */
#define PGRAC_PREFIX_TRANSPORT_EMBEDDED
int prefix_tcp_fixture_main(void);
#define main prefix_tcp_fixture_main
#include "test_cluster_ic_tier1_partial.c"
#undef main

#include "cluster/cluster_config_prefix.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_touched_peers.h"
#include "../../backend/cluster/cluster_ic_mux.c"

bool cluster_enabled = true, cluster_shared_config = true;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
int cluster_interconnect_rdma_fallback = CLUSTER_IC_RDMA_FALLBACK_AUTO;
const ClusterICOps *ClusterICOps_Active = &ClusterICOps_Tier1;
BackendType MyBackendType = B_LMON;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static ClusterConfigPrefixExchange native_exchange;
static ClusterConfigMembersKey native_key;
static uint8 native_episode[16];
static uint64 native_random_counter;
static unsigned prefix_sends, prefix_received, ordinary_received;
static ClusterICSendResult last_send_result;
static uint8 prefix_type;
static unsigned rdma_sends;

static ClusterICSendResult
rdma_send_boundary(int32 peer, const void *bytes pg_attribute_unused(), size_t len)
{
	UT_ASSERT_EQ(peer, UT_PEER_ID);
	UT_ASSERT_EQ(len, sizeof(ClusterICEnvelope) + CLUSTER_CONFIG_PREFIX_BYTES);
	rdma_sends++;
	return CLUSTER_IC_SEND_DONE;
}
const ClusterICOps ClusterICOps_Tier2 = { .send_bytes = rdma_send_boundary };
const ClusterICOps ClusterICOps_Tier3 = { .send_bytes = rdma_send_boundary };
void
cluster_ic_rdma_stats_note_fallback(int32 peer pg_attribute_unused(),
									const char *why pg_attribute_unused())
{}
void
cluster_ic_rdma_stats_note_transport(int32 peer pg_attribute_unused(),
									 ClusterICPeerTransport transport pg_attribute_unused(),
									 ClusterICRdmaPeerState state pg_attribute_unused())
{}
void
cluster_ic_rdma_stats_note_send(int32 peer pg_attribute_unused(),
								uint64 bytes pg_attribute_unused(), bool rdma pg_attribute_unused())
{}
void
cluster_ic_rdma_stats_note_recv(int32 peer pg_attribute_unused(),
								uint64 bytes pg_attribute_unused(), bool rdma pg_attribute_unused())
{}
void
cluster_ic_rdma_stats_note_error(int32 peer pg_attribute_unused(),
								 const char *state pg_attribute_unused(),
								 const char *why pg_attribute_unused())
{}
bool
cluster_ic_rdma_runtime_available(const char **reason)
{
	*reason = "test RDMA boundary";
	return false;
}
bool
cluster_ic_rdma_pending_outbound(int32 peer pg_attribute_unused())
{
	return false;
}
bool
cluster_ic_rdma_drain_recv(int32 *peer pg_attribute_unused(), void *bytes pg_attribute_unused(),
						   size_t len pg_attribute_unused(), size_t *received)
{
	*received = 0;
	return true;
}
bool
cluster_ic_rdma_peek_sender(int32 *peer pg_attribute_unused())
{
	return false;
}
ClusterNormalStopPollResult
cluster_ic_rdma_normal_stop_poll(bool *active, int *peer, const char **reason)
{
	*active = false;
	*peer = -1;
	*reason = "test RDMA boundary";
	return CLUSTER_NORMAL_STOP_READY;
}
ClusterNormalStopPollResult
cluster_ic_chunk_normal_stop_poll(int *peer, uint32 *sequence, const char **reason)
{
	*peer = -1;
	*sequence = 0;
	*reason = "test chunk boundary";
	return CLUSTER_NORMAL_STOP_READY;
}

void
pg_re_throw(void)
{
	if (!PG_exception_stack)
		abort();
	siglongjmp(*PG_exception_stack, 1);
}
bool
pg_strong_random(void *out, size_t len)
{
	memset(out, 0, len);
	UT_ASSERT_EQ(len, 16);
	++native_random_counter;
	memcpy(out, &native_random_counter, sizeof(native_random_counter));
	return true;
}
SCN
cluster_scn_current(void)
{
	return 100;
}
void
cluster_scn_observe(SCN scn pg_attribute_unused())
{}
bool
cluster_epoch_observe_remote(uint64 epoch)
{
	ut_epoch = epoch;
	return true;
}
bool
cluster_touched_peers_stamp(int32 peer pg_attribute_unused(),
							ClusterTouchKind kind pg_attribute_unused())
{
	return true;
}
bool
cluster_sf_peer_capability_word_sample(int32 peer, uint32 bits, uint32 *word, uint32 *generation)
{
	UT_ASSERT_EQ(peer, UT_PEER_ID);
	UT_ASSERT_EQ(bits, PGRAC_IC_HELLO_CAP_CONFIG_PREFIX_V1);
	*word = bits;
	*generation = 10;
	return true;
}
bool
cluster_sf_peer_capability_generation_matches(int32 peer, uint32 bits, uint32 generation)
{
	UT_ASSERT_EQ(peer, UT_PEER_ID);
	UT_ASSERT_EQ(bits, PGRAC_IC_HELLO_CAP_CONFIG_PREFIX_V1);
	return generation == 10;
}

/* Only registry selection is a boundary. Actual codec builds one complete
 * frame; actual tier1 determines queue ownership and writes the TCP stream. */
static ClusterICSendResult
prefix_send_tcp(uint8 type, int32 dest, const void *payload, uint32 len)
{
	uint8 frame[sizeof(ClusterICEnvelope) + CLUSTER_CONFIG_PREFIX_BYTES];
	ClusterICEnvelope env;
	UT_ASSERT_EQ(type, prefix_type);
	UT_ASSERT_EQ(dest, UT_PEER_ID);
	UT_ASSERT_EQ(len, CLUSTER_CONFIG_PREFIX_BYTES);
	UT_ASSERT(cluster_ic_envelope_build(&env, type, 0, dest, payload, len));
	memcpy(frame, &env, sizeof(env));
	memcpy(frame + sizeof(env), payload, len);
	prefix_sends++;
	last_send_result = ClusterICOps_Active->send_bytes(dest, frame, sizeof(frame));
	return last_send_result;
}

/* Real parser/CRC already ran. Ordinary delivery is a counting boundary;
 * prefix delivery executes the actual production ingress. */
bool
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *payload, int32 peer)
{
	UT_ASSERT_EQ(peer, UT_PEER_ID);
	if (env->msg_type == prefix_type) {
		if (cluster_config_prefix_ingress(&native_exchange, env, payload))
			prefix_received++;
	} else {
		UT_ASSERT_EQ(env->msg_type, PGRAC_IC_MSG_HEARTBEAT);
		ordinary_received++;
	}
	return true;
}

static void
prefix_setup(ClusterICPlane plane)
{
	cluster_node_id = 0;
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	ClusterICOps_Active = &ClusterICOps_Tier1;
	MyProcPid = getpid();
	MyBackendType = plane == CLUSTER_IC_PLANE_CONTROL ? B_LMON : B_LMS;
	MyAuxProcType = plane == CLUSTER_IC_PLANE_CONTROL ? LmonProcess : LmsProcess;
	cluster_ic_tier1_set_my_plane(plane);
	memset(&native_exchange, 0, sizeof(native_exchange));
	memset(&native_key, 0, sizeof(native_key));
	memset(native_episode, 5, sizeof(native_episode));
	native_key.ref.identity.system_identifier = 11;
	native_key.ref.identity.database_incarnation = 12;
	native_key.ref.identity.generation = 13;
	native_key.ref.identity.storage_uuid[0] = 14;
	native_key.ref.identity.authority_uuid[0] = 15;
	native_key.ref.identity.configured[0] = (1 << UT_PEER_ID) | 1;
	native_key.required[0] = native_key.ref.identity.configured[0];
	native_key.epoch = ut_epoch;
	native_key.ref.sha256[0] = 16;
	native_key.members_sha256[0] = 17;
	prefix_sends = prefix_received = ordinary_received = 0;
	prefix_type = plane == CLUSTER_IC_PLANE_CONTROL ? PGRAC_IC_MSG_CONFIG_PREFIX_CONTROL
													: PGRAC_IC_MSG_CONFIG_PREFIX_DATA;
	ut_send_envelope_hook = prefix_send_tcp;
	ut_peer_declared = true;
	ut_peer_info.node_id = UT_PEER_ID;
	ut_reconnect_peer();
	UT_ASSERT(cluster_config_prefix_begin(&native_exchange, &native_key, native_episode, UT_PEER_ID,
										  31, 32));
}
static void
prefix_teardown(void)
{
	cluster_ic_tier1_close_peer(UT_PEER_ID, NULL);
	close(ut_rx_fd);
	ut_rx_fd = -1;
	UT_ASSERT(!cluster_config_prefix_complete(&native_exchange));
}
static ClusterConfigPrefixMessage
peer_message(bool ack)
{
	ClusterConfigPrefixMessage m = native_exchange.mark;
	m.sender = UT_PEER_ID;
	m.receiver = 0;
	m.sender_incarnation = 32;
	m.receiver_incarnation = 31;
	m.verb = ack ? CLUSTER_CONFIG_PREFIX_ACK : CLUSTER_CONFIG_PREFIX_MARK;
	if (!ack)
		memset(m.challenge, 91, sizeof(m.challenge));
	return m;
}
static Size
peer_frame(const ClusterConfigPrefixMessage *m, uint8 *frame)
{
	ClusterICEnvelope env;
	uint8 bytes[CLUSTER_CONFIG_PREFIX_BYTES];
	UT_ASSERT(cluster_config_prefix_encode(m, bytes));
	UT_ASSERT(cluster_ic_envelope_build(&env, prefix_type, UT_PEER_ID, 0, bytes, sizeof(bytes)));
	memcpy(frame, &env, sizeof(env));
	memcpy(frame + sizeof(env), bytes, sizeof(bytes));
	return sizeof(env) + sizeof(bytes);
}
static void
peer_deliver(const ClusterConfigPrefixMessage *m)
{
	uint8 frame[sizeof(ClusterICEnvelope) + CLUSTER_CONFIG_PREFIX_BYTES];
	Size length = peer_frame(m, frame);
	ut_send_receive_slice(frame, length);
}

static void
prefix_fifo_case(ClusterICPlane plane)
{
	uint8 earlier[8192], frame[sizeof(ClusterICEnvelope) + CLUSTER_CONFIG_PREFIX_BYTES];
	ClusterICEnvelope env;
	ClusterConfigPrefixMessage m, decoded;
	long received;
	prefix_setup(plane);
	(void)ut_fill_until_eagain(ut_tx_fd);
	/* Remove the saturation junk without advancing the actual private FIFO. */
	ut_allow_socket_progress(ut_tx_fd, ut_rx_fd);
	memset(earlier, 'E', sizeof(earlier));
	UT_ASSERT_EQ(ClusterICOps_Tier1.send_bytes(UT_PEER_ID, earlier, sizeof(earlier)),
				 CLUSTER_IC_SEND_WOULD_BLOCK);
	cluster_config_prefix_poll(&native_exchange);
	UT_ASSERT_EQ(last_send_result, CLUSTER_IC_SEND_WOULD_BLOCK);
	UT_ASSERT(native_exchange.mark_admitted && !cluster_config_prefix_complete(&native_exchange));
	cluster_config_prefix_poll(&native_exchange);
	UT_ASSERT_EQ(prefix_sends, 1); /* admitted tail must not be sent again */
	received
		= ut_drain_and_collect(ut_rx_fd, ut_acc, sizeof(ut_acc), sizeof(earlier) + sizeof(frame));
	UT_ASSERT_EQ(received, sizeof(earlier) + sizeof(frame));
	UT_ASSERT(memcmp(ut_acc, earlier, sizeof(earlier)) == 0);
	memcpy(&env, ut_acc + sizeof(earlier), sizeof(env));
	UT_ASSERT_EQ(cluster_ic_envelope_verify(&env, ut_acc + sizeof(earlier) + sizeof(env),
											CLUSTER_CONFIG_PREFIX_BYTES, UT_PEER_ID, 0),
				 CLUSTER_IC_ENVELOPE_OK);
	UT_ASSERT(cluster_config_prefix_decode(ut_acc + sizeof(earlier) + sizeof(env),
										   CLUSTER_CONFIG_PREFIX_BYTES, &decoded));
	UT_ASSERT_EQ(decoded.verb, CLUSTER_CONFIG_PREFIX_MARK);
	UT_ASSERT(!cluster_config_prefix_complete(&native_exchange)); /* TCP empty is insufficient */
	m = peer_message(true);
	peer_deliver(&m);
	UT_ASSERT(native_exchange.mark_acknowledged
			  && !cluster_config_prefix_complete(&native_exchange));
	m = peer_message(false);
	peer_deliver(&m);
	UT_ASSERT(native_exchange.peer_mark_seen && !native_exchange.peer_ack_admitted);
	UT_ASSERT_EQ(prefix_sends, 1); /* receive cannot send */
	cluster_config_prefix_poll(&native_exchange);
	UT_ASSERT_EQ(prefix_sends, 2);
	UT_ASSERT(cluster_config_prefix_complete(&native_exchange));
	UT_ASSERT_EQ(ut_drain_and_collect(ut_rx_fd, ut_acc, sizeof(ut_acc), sizeof(frame)),
				 sizeof(frame));
	UT_ASSERT(
		cluster_config_prefix_decode(ut_acc + sizeof(env), CLUSTER_CONFIG_PREFIX_BYTES, &decoded));
	UT_ASSERT_EQ(decoded.verb, CLUSTER_CONFIG_PREFIX_ACK);
	UT_ASSERT(memcmp(decoded.challenge, m.challenge, 16) == 0);
	prefix_teardown();
}
UT_TEST(control_prefix_obeys_real_fifo_and_receipt)
{
	prefix_fifo_case(CLUSTER_IC_PLANE_CONTROL);
}
UT_TEST(data_prefix_obeys_real_fifo_and_receipt)
{
	prefix_fifo_case(CLUSTER_IC_PLANE_DATA);
}
UT_TEST(earlier_receive_budget_and_partial_mark_cannot_be_skipped)
{
	ClusterICEnvelope ordinary[64];
	ClusterConfigPrefixMessage m;
	uint8 bytes[sizeof(ordinary) + sizeof(ClusterICEnvelope) + CLUSTER_CONFIG_PREFIX_BYTES];
	Size tail;
	fd_set rfds;
	struct timeval tv = { 5, 0 };
	prefix_setup(CLUSTER_IC_PLANE_CONTROL);
	for (unsigned i = 0; i < lengthof(ordinary); i++)
		UT_ASSERT(cluster_ic_envelope_build(&ordinary[i], PGRAC_IC_MSG_HEARTBEAT, UT_PEER_ID, 0,
											NULL, 0));
	m = peer_message(false);
	memcpy(bytes, ordinary, sizeof(ordinary));
	tail = peer_frame(&m, bytes + sizeof(ordinary));
	UT_ASSERT_EQ(send(ut_rx_fd, bytes, sizeof(bytes), 0), sizeof(bytes));
	FD_ZERO(&rfds);
	FD_SET(ut_tx_fd, &rfds);
	UT_ASSERT_EQ(select(ut_tx_fd + 1, &rfds, NULL, NULL, &tv), 1);
	UT_ASSERT(cluster_ic_tier1_recv_heartbeat_drain(UT_PEER_ID, ut_tx_fd));
	UT_ASSERT_EQ(ordinary_received, 64);
	UT_ASSERT_EQ(prefix_received, 0);
	UT_ASSERT(!native_exchange.peer_mark_seen && !cluster_config_prefix_complete(&native_exchange));
	UT_ASSERT(cluster_ic_tier1_recv_heartbeat_drain(UT_PEER_ID, ut_tx_fd));
	UT_ASSERT_EQ(prefix_received, 1);
	prefix_teardown();
	prefix_setup(CLUSTER_IC_PLANE_CONTROL);
	m = peer_message(false);
	tail = peer_frame(&m, bytes);
	ut_send_receive_slice(bytes, tail - 1);
	UT_ASSERT(!native_exchange.peer_mark_seen);
	UT_ASSERT(!cluster_config_prefix_complete(&native_exchange));
	ut_send_receive_slice(bytes + tail - 1, 1);
	UT_ASSERT(native_exchange.peer_mark_seen);
	prefix_teardown();
}
UT_TEST(reconnect_discards_old_prefix_and_rejects_old_ack)
{
	ClusterConfigPrefixMessage old_ack;
	ClusterICTier1Stream old_stream;
	prefix_setup(CLUSTER_IC_PLANE_DATA);
	old_stream = native_exchange.stream;
	old_ack = peer_message(true);
	cluster_config_prefix_poll(&native_exchange);
	(void)ut_drain_and_collect(ut_rx_fd, ut_acc, sizeof(ut_acc),
							   sizeof(ClusterICEnvelope) + CLUSTER_CONFIG_PREFIX_BYTES);
	cluster_ic_tier1_close_peer(UT_PEER_ID, NULL);
	close(ut_rx_fd);
	ut_reconnect_peer();
	UT_ASSERT(!cluster_config_prefix_complete(&native_exchange));
	UT_ASSERT(native_exchange.failed);
	UT_ASSERT(!cluster_ic_tier1_stream_current(&old_stream));
	cluster_config_prefix_clear(&native_exchange);
	UT_ASSERT(cluster_config_prefix_begin(&native_exchange, &native_key, native_episode, UT_PEER_ID,
										  31, 32));
	cluster_config_prefix_poll(&native_exchange);
	peer_deliver(&old_ack);
	UT_ASSERT(!native_exchange.mark_acknowledged
			  && !cluster_config_prefix_complete(&native_exchange));
	UT_ASSERT_EQ(prefix_received, 0);
	prefix_teardown();
}
UT_TEST(mux_rdma_cannot_certify_a_still_pending_tcp_prefix)
{
	char earlier[8192];
	prefix_setup(CLUSTER_IC_PLANE_CONTROL);
	(void)ut_fill_until_eagain(ut_tx_fd);
	ut_allow_socket_progress(ut_tx_fd, ut_rx_fd);
	memset(earlier, 'E', sizeof(earlier));
	UT_ASSERT_EQ(ClusterICOps_Tier1.send_bytes(UT_PEER_ID, earlier, sizeof(earlier)),
				 CLUSTER_IC_SEND_WOULD_BLOCK);
	/* Actual mux switches without closing, rebinding or draining TCP. RDMA
	 * hardware is the sole send boundary; the product mux chooses that leg. */
	ClusterICOps_Active = &ClusterICOps_Mux;
	cluster_interconnect_tier = CLUSTER_IC_TIER_2;
	cluster_ic_mux_set_peer_transport(UT_PEER_ID, CLUSTER_IC_PEER_TRANSPORT_RDMA,
									  CLUSTER_IC_RDMA_PEER_CONNECTED);
	rdma_sends = 0;
	UT_ASSERT(cluster_ic_tier1_stream_current(&native_exchange.stream));
	UT_ASSERT(cluster_ic_tier1_pending_outbound(UT_PEER_ID));
	cluster_config_prefix_poll(&native_exchange);
	UT_ASSERT(native_exchange.failed && !native_exchange.mark_admitted);
	UT_ASSERT_EQ(rdma_sends, 0);
	UT_ASSERT(cluster_ic_tier1_pending_outbound(UT_PEER_ID));
	cluster_config_prefix_clear(&native_exchange);
	UT_ASSERT(!cluster_config_prefix_begin(&native_exchange, &native_key, native_episode,
										   UT_PEER_ID, 31, 32));
	cluster_ic_mux_set_peer_transport(UT_PEER_ID, CLUSTER_IC_PEER_TRANSPORT_TCP,
									  CLUSTER_IC_RDMA_PEER_FALLBACK_TCP);
	/* Momentary TCP fallback still cannot prove the absence of RDMA debt. */
	UT_ASSERT(!cluster_config_prefix_begin(&native_exchange, &native_key, native_episode,
										   UT_PEER_ID, 31, 32));
	ut_release_backpressure(ut_tx_fd);
	prefix_teardown();
}
int
main(void)
{
	MyProcPid = getpid();
	cluster_ic_tier1_shmem_register();
	UT_ASSERT(ut_captured_region != NULL);
	ut_captured_region->init_fn();
	UT_PLAN(5);
	UT_RUN(control_prefix_obeys_real_fifo_and_receipt);
	UT_RUN(data_prefix_obeys_real_fifo_and_receipt);
	UT_RUN(earlier_receive_budget_and_partial_mark_cannot_be_skipped);
	UT_RUN(reconnect_discards_old_prefix_and_rejects_old_ack);
	UT_RUN(mux_rdma_cannot_certify_a_still_pending_tcp_prefix);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
