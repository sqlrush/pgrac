/* Execute the real requester table, registered landing and pure wire verifier.
 * Physical transport and admission scheduling reuse the existing route fixture. */
int route_fixture_main(int, char **);
#define main route_fixture_main
#define cluster_lms_outbound_enqueue_cap_bound route_fixture_enqueue
#include "test_cluster_r4_route_policy.c"
#undef cluster_lms_outbound_enqueue_cap_bound
#undef main
#include "cluster/cluster_mx_history_wire.h"
#include "utils/resowner.h"
#include "cluster/cluster_gcs_reqid.h"

BufferDescPadded *BufferDescriptors;
static ResourceReleaseCallback history_resource_release;
static void *history_resource_arg;
static ClusterMxHistoryRequestWire sent_history;
static int history_sends;
static int history_sent_worker;
static bool history_reentrant;
static void history_deliver(const ClusterMxHistoryRequestWire *, int);
bool cluster_lms_outbound_enqueue_cap_bound(int, uint8, uint32, const void *, uint16, uint32,
											uint32);

void
RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	history_resource_release = callback;
	history_resource_arg = arg;
}

bool
cluster_lms_outbound_enqueue_cap_bound(int worker, uint8 type, uint32 dest, const void *payload,
									   uint16 length, uint32 caps, uint32 generation)
{
	if (length == 424) {
		history_sent_worker = worker;
		history_sends++;
		memcpy(&sent_history, payload, 424);
		UT_ASSERT_EQ(type, PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
		UT_ASSERT_EQ(dest, 1);
		UT_ASSERT_EQ(generation, 43);
		if (history_reentrant)
			history_deliver(payload, 7);
		return route_seam.enqueue_ok;
	}
	return route_fixture_enqueue(worker, type, dest, payload, length, caps, generation);
}

static bool
history_zero(const void *p, Size n)
{
	const uint8 *b = p;
	for (Size i = 0; i < n; i++)
		if (b[i])
			return false;
	return true;
}
static uint32
history_crc(const void *p, Size n)
{
	pg_crc32c c;
	INIT_CRC32C(c);
	COMP_CRC32C(c, p, n);
	FIN_CRC32C(c);
	return c;
}
static ClusterMxHistoryRequest
history_input(void)
{
	ClusterMxHistoryRequest r = { 0 };
	r.locator.uba = uba_encode(257, 2, 3, 1);
	r.locator.xid = 100;
	r.locator.tt_wrap = 2;
	r.locator.itl_kind = ITL_FLAG_ACTIVE;
	r.locator.itl_slot_index = 1;
	r.forward.epoch = 9;
	r.forward.original_requester_node = 2;
	r.forward.requester_backend_id = 1;
	r.forward.master_node = 1;
	r.forward.transition_id = PCM_TRANS_N_TO_S;
	r.forward.tag = GcsBlockUndoFetchTagMake(257, 2);
	r.edge_kind = CMXH_ROOT;
	r.root_ordinal = 1;
	r.edge_itl_slot = 1;
	r.collector_id = 1;
	r.capture_generation = 2;
	r.backend_incarnation = 1000;
	r.requester_capability_generation = 12;
	r.origin_capability_generation = 43;
	r.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	r.admission_generation = 12;
	r.read_scn = 1000;
	r.read_epoch = 9;
	r.retained_floor = 900;
	r.snapshot_identity = 8;
	r.descriptor_hash = 9;
	r.multixact_id = 10;
	r.member_count = 256;
	r.target_locator = (RelFileLocator){ 1663, 42, 99 };
	r.target_fork = MAIN_FORKNUM;
	r.target_block = 17;
	r.target_offset = 2;
	r.root.xid = 100;
	r.root.wrap = 2;
	r.root.flags = ITL_FLAG_ACTIVE;
	r.root.undo_segment_head = r.locator.uba;
	r.root.write_scn = 100;
	r.edge = r.root;
	return r;
}
static void
history_reset(void)
{
	uint64 id;
	route_seam_reset();
	cluster_node_id = 2;
	MyBackendId = 1;
	MaxBackends = 1;
	MyStartTimestamp = 1000;
	history_sends = 0;
	history_reentrant = false;
	UT_ASSERT(cluster_gcs_block_test_r4_requester_arm(history_input().forward.tag, 9, 1, 1, &id));
	UT_ASSERT(cluster_gcs_block_test_release_r4_requester_slot());
	cluster_gcs_block_read_source_init_backend();
}
static ClusterMxHistoryRequestWire
history_prepare(void)
{
	ClusterMxHistoryRequest r = history_input();
	ClusterMxHistoryRequestWire w;
	UT_ASSERT(cluster_gcs_block_mx_history_prepare(&r, &w));
	return w;
}
static ClusterMxHistoryRequestWire
history_reset_prepare(void)
{
	history_reset();
	return history_prepare();
}
static ClusterMxHistoryReplyWire
history_reply(const ClusterMxHistoryRequestWire *w, int fill)
{
	ClusterMxHistoryReplyWire reply;
	ClusterMxHistoryPeer p = { 0 };
	ClusterMxHistoryPeerWire pw;
	uint8 data[8192], auth[16] = { 0 };
	memset(data, fill, sizeof(data));
	p.flags = CLUSTER_MX_HISTORY_ORDERED | CLUSTER_MX_HISTORY_HAS_GRANT;
	p.record_class = CMXH_OPERATION;
	p.outcome = CLUSTER_TX_IN_PROGRESS;
	p.effect = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	p.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	p.canonical = (ClusterTTSlotPhysicalLocator){ .segment_id = 257,
												  .xid = 100,
												  .slot_offset = 3,
												  .wrap = 2 };
	p.canonical_generation = 12;
	p.top_fxid = FullTransactionIdFromEpochAndXid(2, 100);
	p.operation_fxid = FullTransactionIdFromEpochAndXid(2, 103);
	p.operation_sequence = 4;
	p.record = uba_encode(257, 2, 3, 1);
	p.data_crc = history_crc(data, 8192);
	p.data_lsn = 120;
	p.formation_epoch = 9;
	p.admission_generation = 12;
	p.ctrc_grant = 13;
	ClusterR4WireWriteU64(auth, 12);
	ClusterR4WireWriteU64(auth + 8, 1001);
	UT_ASSERT(cluster_mx_history_peer_encode(&p, &pw));
	UT_ASSERT(cluster_mx_history_reply_encode(w, 1, 130, data, auth, &pw, &reply));
	return reply;
}
static void
history_deliver(const ClusterMxHistoryRequestWire *w, int fill)
{
	ClusterMxHistoryReplyWire reply = history_reply(w, fill);
	ClusterICEnvelope env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, 1, 2, 8816);
	cluster_gcs_handle_block_reply_envelope(&env, &reply);
}
UT_TEST(history_publication_precedes_reentrant_reply)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryPeerResult result;
	history_reentrant = true;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &result), CMXH_COMPLETE);
	UT_ASSERT_EQ(result.admitted, 1);
	UT_ASSERT_EQ(result.consumed, 0);
	UT_ASSERT_EQ(result.data[0], 7);
	UT_ASSERT_EQ(result.data[8191], 7);
	UT_ASSERT(memcmp(&w, &result.request, 424) == 0);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 0);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &result), CMXH_UNKNOWN);
	UT_ASSERT(history_zero(&result, sizeof(result)));
}
UT_TEST(history_queue_full_preserves_exact_attempt)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryPeerResult r;
	route_seam.enqueue_ok = false;
	UT_ASSERT(!cluster_gcs_block_mx_history_submit(&w));
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_PENDING);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 1);
	route_seam.enqueue_ok = true;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	UT_ASSERT_EQ(history_sends, 2);
	UT_ASSERT(memcmp(&w, &sent_history, 424) == 0);
	cluster_gcs_block_mx_history_cancel(&w);
}
UT_TEST(history_eight_slots_and_request_id_nonreuse)
{
	ClusterMxHistoryRequestWire w[8], n;
	ClusterMxHistoryRequest in = history_input();
	history_reset();
	for (int i = 0; i < 8; i++)
		w[i] = history_prepare();
	UT_ASSERT(!cluster_gcs_block_mx_history_prepare(&in, &n));
	UT_ASSERT(history_zero(&n, sizeof(n)));
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 8);
	for (int i = 0; i < 8; i++)
		cluster_gcs_block_mx_history_cancel(&w[i]);
	n = history_prepare();
	UT_ASSERT(ClusterR4WireReadU64(n.bytes) > ClusterR4WireReadU64(w[7].bytes));
	cluster_gcs_block_mx_history_cancel(&n);
}
UT_TEST(history_duplicate_exact_and_conflict_poison)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryPeerResult r;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	history_deliver(&w, 3);
	history_deliver(&w, 3);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_COMPLETE);
	w = history_prepare();
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	history_deliver(&w, 3);
	history_deliver(&w, 4);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_UNKNOWN);
	UT_ASSERT(history_zero(&r, sizeof(r)));
}
UT_TEST(history_cancel_late_reply_and_reused_slot)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare(), n;
	ClusterMxHistoryPeerResult r;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	cluster_gcs_block_mx_history_cancel(&w);
	UT_ASSERT_EQ(history_sends, 2);
	UT_ASSERT(cluster_mx_history_request_same(&w, &sent_history, true));
	UT_ASSERT(ClusterR4WireReadU16(sent_history.bytes + 102) & CLUSTER_MX_HISTORY_CANCEL);
	cluster_gcs_block_mx_history_cancel(&w);
	UT_ASSERT_EQ(history_sends, 2);
	n = history_prepare();
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&n));
	history_deliver(&w, 9);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&n, &r), CMXH_PENDING);
	history_deliver(&n, 5);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&n, &r), CMXH_COMPLETE);
	UT_ASSERT_EQ(r.data[0], 5);
}
UT_TEST(history_wrong_echo_source_and_crc_cannot_publish)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare(), wrong = w;
	ClusterMxHistoryPeerResult r;
	ClusterMxHistoryReplyWire reply = history_reply(&w, 6);
	ClusterICEnvelope env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, 3, 2, 8816);
	ClusterMxHistoryRequest decoded;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	cluster_gcs_handle_block_reply_envelope(&env, &reply);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_PENDING);
	UT_ASSERT(cluster_mx_history_request_decode(&w, 424, 2, 1, 9, &decoded));
	decoded.capture_generation++;
	UT_ASSERT(cluster_mx_history_request_encode(&decoded, &wrong));
	history_deliver(&wrong, 8);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_PENDING);
	env.source_node_id = 1;
	reply.bytes[500] ^= 1;
	cluster_gcs_handle_block_reply_envelope(&env, &reply);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_UNKNOWN);
	UT_ASSERT(history_zero(&r, sizeof(r)));
}
UT_TEST(history_negative_and_epoch_drift_requalify)
{
	ClusterMxHistoryRequestWire w;
	ClusterMxHistoryPeerResult r;
	for (int status = 25; status <= 26; status++) {
		ClusterMxHistoryRequestWire w = history_reset_prepare();
		ClusterMxHistoryPeerResult r;
		uint8 reply[48] = { 0 };
		ClusterICEnvelope env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, 1, 2, 48);
		memcpy(reply, w.bytes, 8);
		ClusterR4WireWriteU64(reply + 16, 9);
		ClusterR4WireWriteU32(reply + 28, 1);
		ClusterR4WireWriteU32(reply + 32, 1);
		reply[36] = PCM_TRANS_N_TO_S;
		reply[37] = status;
		ClusterR4WireWriteU32(reply + 38, (uint32)GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
		UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
		cluster_gcs_handle_block_reply_envelope(&env, reply);
		UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r),
					 status == 25 ? CMXH_REQUALIFY : CMXH_UNKNOWN);
		UT_ASSERT(history_zero(&r, sizeof(r)));
	}
	w = history_reset_prepare();
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	history_deliver(&w, 1);
	route_test_epoch = 10;
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_REQUALIFY);
	UT_ASSERT(history_zero(&r, sizeof(r)));
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 0);
}
UT_TEST(history_incarnation_and_admission_binding)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryPeerResult r;
	MyStartTimestamp = 1001;
	UT_ASSERT(!cluster_gcs_block_mx_history_submit(&w));
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_UNKNOWN);
	MyStartTimestamp = 1000;
	route_seam.recheck_ok = false;
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_REQUALIFY);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 0);
}
UT_TEST(history_resource_abort_cancels_and_closes_landing)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryPeerResult r;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	UT_ASSERT(history_resource_release != NULL);
	if (history_resource_release)
		history_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, history_resource_arg);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 0);
	history_deliver(&w, 2);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_UNKNOWN);
	UT_ASSERT(history_zero(&r, sizeof(r)));
}
UT_TEST(history_bad_scope_never_reserves)
{
	ClusterMxHistoryRequest r;
	ClusterMxHistoryRequestWire w;
	for (int fault = 0; fault < 9; fault++) {
		history_reset();
		r = history_input();
		switch (fault) {
		case 0:
			r.forward.request_id = 9;
			break;
		case 1:
			r.backend_incarnation++;
			break;
		case 2:
			r.forward.original_requester_node = 3;
			break;
		case 3:
			r.requester_capability_generation++;
			break;
		case 4:
			r.origin_capability_generation++;
			break;
		case 5:
			r.admission_generation++;
			break;
		case 6:
			route_seam.peer_open_ok = false;
			break;
		case 7:
			route_seam.capability_ok = false;
			break;
		case 8:
			route_seam.admission_result = CLUSTER_SEMANTIC_ADMISSION_CLOSED;
			break;
		}
		memset(&w, 0xaa, sizeof(w));
		UT_ASSERT(!cluster_gcs_block_mx_history_prepare(&r, &w));
		UT_ASSERT(history_zero(&w, sizeof(w)));
		UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 0);
		UT_ASSERT_EQ(history_sends, 0);
	}
}
UT_TEST(history_unsent_packet_and_cancel_backpressure)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryPeerResult r;
	history_deliver(&w, 4);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_PENDING);
	UT_ASSERT(history_zero(&r, sizeof(r)));
	cluster_gcs_block_mx_history_cancel(&w);
	UT_ASSERT_EQ(history_sends, 0);
	w = history_prepare();
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	route_seam.enqueue_ok = false;
	cluster_gcs_block_mx_history_cancel(&w);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 0);
	UT_ASSERT_EQ(history_sends, 2);
	history_deliver(&w, 4);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_UNKNOWN);
}
UT_TEST(history_request_id_exhaustion_does_not_wrap)
{
	ClusterMxHistoryRequest r;
	ClusterMxHistoryRequestWire w;
	uint64 id;
	history_reset();
	UT_ASSERT(cluster_gcs_block_test_r4_requester_arm(history_input().forward.tag, 9, 1,
													  GCS_REQID_REQUESTER_SEQ_MASK, &id));
	UT_ASSERT(cluster_gcs_block_test_release_r4_requester_slot());
	r = history_input();
	UT_ASSERT(!cluster_gcs_block_mx_history_prepare(&r, &w));
	UT_ASSERT(history_zero(&w, sizeof(w)));
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 0);
}
UT_TEST(history_domain_isolation_and_late_drop_count)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryReplyWire reply;
	ClusterICEnvelope env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, 1, 2, 8816);
	GcsBlockReplyHeader header;
	char data[8192];
	bool received = true;
	uint64 drops, id = ClusterR4WireReadU64(w.bytes);
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	cluster_gcs_block_mx_history_cancel(&w);
	reply = history_reply(&w, 2);
	UT_ASSERT(cluster_gcs_block_test_arm_legacy_reply_slot(id, 9, 1, PCM_TRANS_N_TO_S, 1));
	cluster_gcs_handle_block_reply_envelope(&env, &reply);
	UT_ASSERT(cluster_gcs_block_test_snapshot_r4_reply_slot(&header, data, &received, &drops));
	UT_ASSERT(!received);
	UT_ASSERT_EQ(drops, 1);
}
UT_TEST(history_subcommit_preserves_and_backend_exit_closes)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryPeerResult r;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	UT_ASSERT(history_resource_release != NULL);
	if (history_resource_release)
		history_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS, true, false, history_resource_arg);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_PENDING);
	UT_ASSERT(route_exit_cleanup != NULL);
	if (route_exit_cleanup)
		route_exit_cleanup(0, route_exit_cleanup_arg);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 0);
	history_deliver(&w, 3);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_UNKNOWN);
}

UT_TEST(history_data_stream_loss_requalifies_without_resending)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryPeerResult r;
	route_history_stream_generation = 0;
	UT_ASSERT(!cluster_gcs_block_mx_history_submit(&w));
	UT_ASSERT_EQ(history_sends, 0);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_PENDING);
	route_history_stream_generation = 101;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	route_history_stream_generation = 102;
	/* CONTROL/admission remain open. The exact DATA session was replaced. */
	history_deliver(&w, 8);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_REQUALIFY);
	UT_ASSERT(history_zero(&r, sizeof(r)));
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 0);
}
UT_TEST(history_data_send_failure_only_retires_exact_attempt)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare(), wrong = w;
	ClusterMxHistoryPeerResult r;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	cluster_gcs_block_mx_history_send_failed(&w, 424, 3);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_PENDING);
	wrong.bytes[110] ^= 1;
	cluster_gcs_block_mx_history_send_failed(&wrong, 424, 1);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_PENDING);
	cluster_gcs_block_mx_history_send_failed(&w, 424, 1);
	history_deliver(&w, 9);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &r), CMXH_REQUALIFY);
	UT_ASSERT(history_zero(&r, sizeof(r)));
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 0);
}

#ifndef HISTORY_BACKEND_TEST_MAIN
#define HISTORY_BACKEND_TEST_MAIN main
#endif
int
HISTORY_BACKEND_TEST_MAIN(void)
{
	UT_PLAN(16);
	UT_RUN(history_publication_precedes_reentrant_reply);
	UT_RUN(history_queue_full_preserves_exact_attempt);
	UT_RUN(history_eight_slots_and_request_id_nonreuse);
	UT_RUN(history_duplicate_exact_and_conflict_poison);
	UT_RUN(history_cancel_late_reply_and_reused_slot);
	UT_RUN(history_wrong_echo_source_and_crc_cannot_publish);
	UT_RUN(history_negative_and_epoch_drift_requalify);
	UT_RUN(history_incarnation_and_admission_binding);
	UT_RUN(history_resource_abort_cancels_and_closes_landing);
	UT_RUN(history_bad_scope_never_reserves);
	UT_RUN(history_unsent_packet_and_cancel_backpressure);
	UT_RUN(history_request_id_exhaustion_does_not_wrap);
	UT_RUN(history_domain_isolation_and_late_drop_count);
	UT_RUN(history_subcommit_preserves_and_backend_exit_closes);
	UT_RUN(history_data_stream_loss_requalifies_without_resending);
	UT_RUN(history_data_send_failure_only_retires_exact_attempt);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
