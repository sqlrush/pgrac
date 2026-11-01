/* Intentional, manually selected acceptance RED. Approved foreign-MX route
 * plus existing kind=2 payload, never a legacy V5/V6 request or new format id.
 * This test-only request container is not a product wire declaration. The
 * original inline holder must eventually dispatch it into qualified proofs;
 * current composition-only routing drops it before any CTRC touch.
 */
int k_proof_ask_component_main(void);
#define K_MX_NATIVE_CALLER_MAIN k_proof_ask_component_main
#define cluster_ic_send_envelope k_proof_original_send_envelope
#include "test_k_mx_g5_native_caller.c"
#undef cluster_ic_send_envelope
#include "port/pg_crc32c.h"

ClusterICSendResult cluster_ic_send_envelope(uint8 type, int32 destination,
	const void *payload, uint32 bytes);
static bool capture_proof_reply;
static uint32 captured_bytes;
static ClusterMxRangeResponse captured_reply;

ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 destination, const void *payload, uint32 bytes)
{
	if (!capture_proof_reply)
		return k_proof_original_send_envelope(type, destination, payload, bytes);
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GCS_BLOCK_REPLY);
	UT_ASSERT_EQ(destination, 1);
	UT_ASSERT(bytes <= sizeof(captured_reply));
	if (bytes > sizeof(captured_reply))
		return CLUSTER_IC_SEND_DONE;
	memcpy(&captured_reply, payload, bytes);
	captured_bytes = bytes;
	return CLUSTER_IC_SEND_DONE;
}

static void
seal_ask_request(ClusterMxRangeResponse *packet, uint32 body_bytes)
{
	pg_crc32c crc;
	packet->route.format_id = CLUSTER_FOREIGN_MX_FORMAT_ID_V1;
	packet->route.version = CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1;
	packet->route.kind = CLUSTER_MX_RANGE_REQUEST_KIND;
	packet->route.wire_bytes = sizeof(packet->route) + body_bytes;
	packet->route.checksum = 0;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, packet, packet->route.wire_bytes);
	FIN_CRC32C(crc);
	packet->route.checksum = (uint32)crc;
}

static void
expect_original_holder_proofs(uint32 total, uint32 first, bool corrupt)
{
	ClusterCurrentMxKey key = {0}; ClusterMxCompositionReceive *op = NULL;
	ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
	ClusterMxRangeResponse packet = {0}; ClusterICEnvelope env = {0};
	BufferTag route_tag;
	ClusterMxRangeProofAsk asks[7]; ClusterCurrentMxMemberDesc members[7];
	ClusterMxProofReply proof_reply; uint32 bytes = 0, count = 0, decoded_total = 0;
	unsigned described;
	chain_seed(total, 1); key.multixact_id = 1000; key.cluster_epoch = 7;
	UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot, &op, &stats));
	described = requests_sent;
	/* Earlier windows are provisional fixture data, solely to reach a genuine
 * uint32 tail ASK. No proof/CTRC authority is supplied by these literals. */
	for (uint32 next = 0; next < first;) {
		ClusterMxFullMemberProof entries[7]; ClusterMxProofReply r = proof_window(total, next, entries);
		UT_ASSERT(r.next <= first);
		UT_ASSERT_EQ(cluster_multixact_current_proofs_accept(&op, MXCOMP_READ_OK, &r), MXCOMP_PARTIAL);
		next = r.next;
	}
	c = proof_request_context();
	UT_ASSERT(cluster_multixact_current_proofs_request(&op, &c, packet.data.data, BLCKSZ, &bytes));
	UT_ASSERT(cluster_mx_range_wire_decode_proof_asks(packet.data.data, bytes, &c, asks, 7, &count, &decoded_total));
	UT_ASSERT_EQ(decoded_total, total); UT_ASSERT_EQ(asks[0].member_ordinal, first);
	if (first == 65534) UT_ASSERT_EQ(asks[2].member_ordinal, 65536);
	for (uint32 i = 0; i < count; i++) {
		memset(&members[i], 0, sizeof(members[i]));
		members[i].xid = asks[i].xid; members[i].member_status = asks[i].member_status;
	}
	packet.route.request_id = c.request_id; packet.route.epoch = c.epoch;
	packet.route.requester_node = 1; packet.route.requester_backend = 1;
	seal_ask_request(&packet, bytes);
	if (corrupt) packet.data.data[0] ^= 1; /* CRC must reject before any proof work. */
	env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD; env.source_node_id = 1;
	env.dest_node_id = 0; env.payload_length = packet.route.wire_bytes;
	route_tag = GcsBlockCurrentMxRouteTagMake(c.request_id, c.epoch, 1, 1);
	data_worker = cluster_lms_shard_for_tag(&route_tag, cluster_lms_workers);
	captured_bytes = 0; capture_proof_reply = true;
	cluster_node_id = 0; MyBackendId = InvalidBackendId;
	cluster_gcs_current_mx_range_serve_inline(&env, &packet);
	cluster_node_id = 1; MyBackendId = 1; capture_proof_reply = false;
	if (corrupt)
		UT_ASSERT_EQ(captured_bytes, 0);
	else {
		/* A metadata reply, UNKNOWN, prefix or no reply is not proof success. */
		UT_ASSERT(captured_bytes >= sizeof(ClusterMxRangeRoute));
		if (captured_bytes >= sizeof(ClusterMxRangeRoute)) {
			UT_ASSERT_EQ(cluster_mx_range_wire_decode_proofs(captured_reply.data.data,
				captured_bytes - sizeof(ClusterMxRangeRoute), &c, 0, total, first,
				members, count, &proof_reply), MXCOMP_READ_OK);
			UT_ASSERT_EQ(cluster_multixact_current_proofs_accept(&op, MXCOMP_READ_OK, &proof_reply),
				first + count == total ? MXCOMP_COMPLETE : MXCOMP_PARTIAL);
		}
	}
	UT_ASSERT_EQ(requests_sent, described); /* No hidden fallback/retry is permitted. */
	cluster_multixact_current_describe_finish(&op); chain_clean();
}

UT_TEST(holder_accepts_small_full_domain_asks) { expect_original_holder_proofs(2, 0, false); }
UT_TEST(holder_accepts_257_member_source_asks) { expect_original_holder_proofs(257, 0, false); }
UT_TEST(holder_accepts_global_ordinal_65536) { expect_original_holder_proofs(65537, 65534, false); }
UT_TEST(holder_corrupt_ask_never_yields_proofs) { expect_original_holder_proofs(2, 0, true); }

int main(void)
{
	TopMemoryContext = AllocSetContextCreate(CurrentMemoryContext, "proof holder RED", ALLOCSET_DEFAULT_SIZES);
	CurrentMemoryContext = TopMemoryContext;
	UT_PLAN(4);
	UT_RUN(holder_accepts_small_full_domain_asks);
	UT_RUN(holder_accepts_257_member_source_asks);
	UT_RUN(holder_accepts_global_ordinal_65536);
	UT_RUN(holder_corrupt_ask_never_yields_proofs);
	MemoryContextDelete(TopMemoryContext); UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
