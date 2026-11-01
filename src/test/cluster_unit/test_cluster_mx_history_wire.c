/* Exact history codec contract, without a synthetic authority provider. */
#include "postgres.h"
#include "cluster/cluster_mx_history_wire.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_semantic_activation.h"
#include "port/pg_crc32c.h"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

/* A product assertion is a fail-stop fixture error, never a passing case. */
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "unexpected assertion %s at %s:%d\n", condition, file, line);
	abort();
}

static bool
zero(const void *p, Size n)
{
	const uint8 *b = p;
	for (Size i = 0; i < n; i++)
		if (b[i])
			return false;
	return true;
}
static uint32
crc(const void *p, Size n)
{
	pg_crc32c c;
	INIT_CRC32C(c);
	COMP_CRC32C(c, p, n);
	FIN_CRC32C(c);
	return c;
}
static ClusterMxHistoryRequest
request(void)
{
	ClusterMxHistoryRequest r = { 0 };
	r.locator.uba = uba_encode(1, 2, 3, 1);
	r.locator.xid = 100;
	r.locator.tt_wrap = 2;
	r.locator.itl_kind = ITL_FLAG_ACTIVE;
	r.locator.itl_slot_index = 1;
	r.forward.request_id = 42;
	r.forward.epoch = 9;
	r.forward.original_requester_node = 1;
	r.forward.requester_backend_id = 7;
	r.forward.master_node = 0;
	r.forward.transition_id = PCM_TRANS_N_TO_S;
	r.forward.tag = GcsBlockUndoFetchTagMake(1, 2);
	r.edge_kind = CMXH_ROOT;
	r.root_ordinal = 1;
	r.edge_itl_slot = 1;
	r.collector_id = 1;
	r.capture_generation = 2;
	r.backend_incarnation = 3;
	r.requester_capability_generation = 4;
	r.origin_capability_generation = 5;
	r.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	r.admission_generation = 6;
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
static ClusterMxHistoryPeer
peer(void)
{
	ClusterMxHistoryPeer p = { 0 };
	p.flags = CLUSTER_MX_HISTORY_ORDERED | CLUSTER_MX_HISTORY_HAS_GRANT;
	p.record_class = CMXH_OPERATION;
	p.outcome = CLUSTER_TX_IN_PROGRESS;
	p.effect = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	p.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	p.canonical = (ClusterTTSlotPhysicalLocator){ .segment_id = 1,
												  .xid = 100,
												  .slot_offset = 3,
												  .wrap = 2 };
	p.canonical_generation = 12;
	p.top_fxid = FullTransactionIdFromEpochAndXid(2, 100);
	p.operation_fxid = FullTransactionIdFromEpochAndXid(2, 103);
	p.operation_sequence = 4;
	p.record = uba_encode(1, 2, 3, 1);
	p.data_generation = 0;
	p.data_lsn = 120;
	p.formation_epoch = 9;
	p.admission_generation = 6;
	p.ctrc_grant = 13;
	return p;
}

UT_TEST(request_roundtrip_literal_offsets)
{
	ClusterMxHistoryRequest r = request(), d;
	ClusterMxHistoryRequestWire w;
	UT_ASSERT(cluster_mx_history_request_encode(&r, &w));
	UT_ASSERT_EQ(ClusterR4WireReadU16(w.bytes + 96), 1);
	UT_ASSERT_EQ(ClusterR4WireReadU16(w.bytes + 98), 328);
	UT_ASSERT_EQ(ClusterR4WireReadU16(w.bytes + 196), 256);
	UT_ASSERT_EQ(ClusterR4WireReadU32(w.bytes + 416), crc(w.bytes, 416));
	UT_ASSERT_EQ(w.bytes[65], 5);
	UT_ASSERT(cluster_mx_history_request_decode(&w, sizeof(w), 1, 0, 9, &d));
	UT_ASSERT_EQ(d.forward.request_id, 42);
	UT_ASSERT_EQ(d.root.undo_segment_head.raw[0], r.root.undo_segment_head.raw[0]);
	UT_ASSERT_EQ(d.descriptor_hash, r.descriptor_hash);
	UT_ASSERT_EQ(d.target_offset, 2);
}
UT_TEST(request_semantic_failures_clear_output)
{
	ClusterMxHistoryRequest r = request();
	ClusterMxHistoryRequestWire w;
	r.member_count = 257;
	memset(&w, 0xaa, sizeof(w));
	UT_ASSERT(!cluster_mx_history_request_encode(&r, &w));
	UT_ASSERT(zero(&w, sizeof(w)));
	r = request();
	r.flags = 4;
	UT_ASSERT(!cluster_mx_history_request_encode(&r, &w));
	r = request();
	memset(&r.root.undo_segment_head, 0, sizeof(UBA));
	UT_ASSERT(!cluster_mx_history_request_encode(&r, &w));
	r = request();
	r.read_epoch = 10;
	UT_ASSERT(!cluster_mx_history_request_encode(&r, &w));
	r = request();
	r.retained_floor = 1001;
	UT_ASSERT(!cluster_mx_history_request_encode(&r, &w));
}
UT_TEST(request_wire_rejects_mutations_and_transport_mismatch)
{
	ClusterMxHistoryRequest r = request(), d;
	ClusterMxHistoryRequestWire w, b;
	UT_ASSERT(cluster_mx_history_request_encode(&r, &w));
	for (unsigned i = 0; i < sizeof(w); i++) {
		b = w;
		b.bytes[i] ^= 0x80;
		memset(&d, 0xaa, sizeof(d));
		UT_ASSERT(!cluster_mx_history_request_decode(&b, sizeof(b), 1, 0, 9, &d));
		UT_ASSERT(zero(&d, sizeof(d)));
	}
	UT_ASSERT(!cluster_mx_history_request_decode(&w, 423, 1, 0, 9, &d));
	UT_ASSERT(!cluster_mx_history_request_decode(&w, 425, 1, 0, 9, &d));
	UT_ASSERT(!cluster_mx_history_request_decode(&w, 424, 2, 0, 9, &d));
	UT_ASSERT(!cluster_mx_history_request_decode(&w, 424, 1, 1, 9, &d));
	UT_ASSERT(!cluster_mx_history_request_decode(&w, 424, 1, 0, 10, &d));
}
UT_TEST(request_cancel_is_exact_and_idempotent)
{
	ClusterMxHistoryRequest r = request(), d;
	ClusterMxHistoryRequestWire w, c, c2;
	UT_ASSERT(cluster_mx_history_request_encode(&r, &w));
	UT_ASSERT(cluster_mx_history_request_cancel(&w, &c));
	UT_ASSERT(cluster_mx_history_request_decode(&c, 424, 1, 0, 9, &d));
	UT_ASSERT_EQ(d.flags, CLUSTER_MX_HISTORY_CANCEL);
	UT_ASSERT(cluster_mx_history_request_same(&w, &c, true));
	UT_ASSERT(!cluster_mx_history_request_same(&w, &c, false));
	UT_ASSERT(cluster_mx_history_request_cancel(&c, &c2));
	UT_ASSERT(memcmp(&c, &c2, sizeof(c)) == 0);
	r.capture_generation++;
	UT_ASSERT(cluster_mx_history_request_encode(&r, &c2));
	UT_ASSERT(!cluster_mx_history_request_same(&w, &c2, true));
}
UT_TEST(global_and_prior_edges_keep_exact_owner_constraints)
{
	ClusterMxHistoryRequest r = request(), d;
	ClusterMxHistoryRequestWire w;
	r.edge_kind = CMXH_GLOBAL_PREV;
	r.edge_itl_slot = 255;
	r.locator.itl_kind = 0;
	r.locator.itl_slot_index = 255;
	memset(&r.edge, 0, sizeof(r.edge));
	r.parent = uba_encode(1, 3, 3, 0);
	r.parent_record_crc = 91;
	UT_ASSERT(!cluster_mx_history_request_encode(&r, &w));
	r.flags = CLUSTER_MX_HISTORY_EXPECT_OWNER | CLUSTER_MX_HISTORY_EXPECT_DATA_GENERATION;
	r.expected_owner = peer().canonical;
	r.expected_owner_generation = 12;
	r.expected_top_fxid = peer().top_fxid;
	UT_ASSERT(cluster_mx_history_request_encode(&r, &w));
	UT_ASSERT(cluster_mx_history_request_decode(&w, 424, 1, 0, 9, &d));
	UT_ASSERT_EQ(d.expected_data_generation, 0);
	UT_ASSERT_EQ(d.expected_owner_generation, 12);
	r.expected_owner.segment_id += CLUSTER_UNDO_SEGS_PER_INSTANCE;
	UT_ASSERT(!cluster_mx_history_request_encode(&r, &w));
	r.expected_owner.segment_id = 1;
	r.expected_owner.slot_offset = 4;
	UT_ASSERT(!cluster_mx_history_request_encode(&r, &w));
	r.expected_owner.slot_offset = 3;
	r.locator.tt_wrap = TT_WRAP_INVALID;
	UT_ASSERT(!cluster_mx_history_request_encode(&r, &w));
	r.locator.tt_wrap = 2;
	r.edge_kind = CMXH_PAGE_PRIOR;
	r.edge = r.root;
	r.locator.itl_kind = ITL_FLAG_ACTIVE;
	r.locator.itl_slot_index = r.edge_itl_slot = 1;
	UT_ASSERT(cluster_mx_history_request_encode(&r, &w));
	r.parent = r.locator.uba;
	UT_ASSERT(!cluster_mx_history_request_encode(&r, &w));
}
UT_TEST(peer_roundtrip_and_invalid_dispositions)
{
	ClusterMxHistoryPeer p = peer(), d;
	ClusterMxHistoryPeerWire w;
	UT_ASSERT(cluster_mx_history_peer_encode(&p, &w));
	UT_ASSERT_EQ(ClusterR4WireReadU32(w.bytes), UINT32_C(0x3158484d));
	UT_ASSERT(cluster_mx_history_peer_decode(&w, &d));
	UT_ASSERT_EQ(U64FromFullTransactionId(d.operation_fxid),
				 U64FromFullTransactionId(p.operation_fxid));
	p.flags = 0;
	UT_ASSERT(!cluster_mx_history_peer_encode(&p, &w));
	UT_ASSERT(zero(&w, sizeof(w)));
	p = peer();
	p.effect = 7;
	UT_ASSERT(!cluster_mx_history_peer_encode(&p, &w));
	p = peer();
	p.outcome = CLUSTER_TX_UNKNOWN;
	UT_ASSERT(!cluster_mx_history_peer_encode(&p, &w));
	p = peer();
	p.canonical_generation = UINT32_MAX;
	UT_ASSERT(!cluster_mx_history_peer_encode(&p, &w));
	p = peer();
	p.record_class = CMXH_V1_TOP;
	UT_ASSERT(!cluster_mx_history_peer_encode(&p, &w));
	p.operation_fxid = p.top_fxid;
	p.operation_sequence = 0;
	UT_ASSERT(cluster_mx_history_peer_encode(&p, &w));
	p = peer();
	p.record_class = CMXH_ROLLBACK_CONTROL;
	p.effect = 0;
	p.flags = 1;
	p.ctrc_grant = 0;
	UT_ASSERT(cluster_mx_history_peer_encode(&p, &w));
}
UT_TEST(reply_echo_data_and_proof_are_one_checked_body)
{
	ClusterMxHistoryRequest r = request();
	ClusterMxHistoryRequestWire w;
	ClusterMxHistoryPeer p = peer();
	ClusterMxHistoryPeerWire pw;
	ClusterMxHistoryReplyWire rw;
	ClusterMxHistoryPeerResult out;
	uint8 data[8192] = { 0 }, auth[16] = { 0 }, status = 0;
	data[8191] = 91;
	p.data_crc = crc(data, sizeof(data));
	ClusterR4WireWriteU64(auth, 12);
	ClusterR4WireWriteU64(auth + 8, 1001);
	UT_ASSERT(cluster_mx_history_request_encode(&r, &w));
	UT_ASSERT(cluster_mx_history_peer_encode(&p, &pw));
	UT_ASSERT(cluster_mx_history_reply_encode(&w, 0, 130, data, auth, &pw, &rw));
	UT_ASSERT_EQ(ClusterR4WireReadU32(rw.bytes + 24), crc(rw.bytes + 48, 8768));
	UT_ASSERT(cluster_mx_history_reply_decode(&rw, 8816, 0, &w, &status, &out));
	UT_ASSERT_EQ(status, 31);
	UT_ASSERT_EQ(out.data[8191], 91);
	UT_ASSERT_EQ(out.admitted, 0);
	UT_ASSERT_EQ(out.consumed, 0);
	UT_ASSERT(memcmp(&out.request, &w, sizeof(w)) == 0);
}
UT_TEST(reply_corrupt_fields_never_publish_partial_output)
{
	ClusterMxHistoryRequest r = request();
	ClusterMxHistoryRequestWire w;
	ClusterMxHistoryPeer p = peer();
	ClusterMxHistoryPeerWire pw;
	ClusterMxHistoryReplyWire rw, b;
	ClusterMxHistoryPeerResult out;
	uint8 data[8192] = { 0 }, auth[16] = { 0 }, status = 0;
	p.data_crc = crc(data, sizeof(data));
	ClusterR4WireWriteU64(auth, 12);
	ClusterR4WireWriteU64(auth + 8, 1001);
	UT_ASSERT(cluster_mx_history_request_encode(&r, &w));
	UT_ASSERT(cluster_mx_history_peer_encode(&p, &pw));
	UT_ASSERT(cluster_mx_history_reply_encode(&w, 0, 130, data, auth, &pw, &rw));
	for (unsigned i = 0; i < sizeof(rw); i++) {
		/* Header LSN is authenticated by the outer transport, not the body
		 * CRC or a cross-node LSN comparison. A different nonzero LSN has
		 * no contradictory expected scalar at this codec boundary. */
		if (i >= 8 && i < 16)
			continue;
		b = rw;
		b.bytes[i] ^= 0x80;
		memset(&out, 0xaa, sizeof(out));
		status = 99;
		UT_ASSERT(!cluster_mx_history_reply_decode(&b, sizeof(b), 0, &w, &status, &out));
		UT_ASSERT(zero(&out, sizeof(out)));
		UT_ASSERT_EQ(status, 0);
	}
	r.capture_generation++;
	UT_ASSERT(cluster_mx_history_request_encode(&r, &w));
	UT_ASSERT(!cluster_mx_history_reply_decode(&rw, 8816, 0, &w, &status, &out));
}
UT_TEST(negative_reply_has_no_page_or_proof)
{
	ClusterMxHistoryRequest r = request();
	ClusterMxHistoryRequestWire w;
	GcsBlockReplyHeader h = { 0 };
	ClusterMxHistoryPeerResult out;
	uint8 status = 0;
	UT_ASSERT(cluster_mx_history_request_encode(&r, &w));
	h.request_id = 42;
	h.epoch = 9;
	h.sender_node = 0;
	h.requester_backend_id = 7;
	h.transition_id = PCM_TRANS_N_TO_S;
	h.status = GCS_BLOCK_REPLY_R4_DENIED;
	GcsBlockReplyHeaderSetForwardingMasterNode(&h, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	UT_ASSERT(cluster_mx_history_reply_decode(&h, 48, 0, &w, &status, &out));
	UT_ASSERT_EQ(status, GCS_BLOCK_REPLY_R4_DENIED);
	UT_ASSERT(zero(&out, sizeof(out)));
	h.status = GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED;
	UT_ASSERT(cluster_mx_history_reply_decode(&h, 48, 0, &w, &status, &out));
	h.page_lsn = 1;
	UT_ASSERT(!cluster_mx_history_reply_decode(&h, 48, 0, &w, &status, &out));
}
UT_TEST(recomputed_crc_does_not_hide_wrong_slot_or_unsupported_context)
{
	ClusterMxHistoryRequest r = request(), d;
	ClusterMxHistoryRequestWire w, b;
	ClusterMxHistoryPeer p = peer();
	ClusterMxHistoryPeerWire pw;
	ClusterMxHistoryReplyWire rw;
	ClusterMxHistoryPeerResult out;
	uint8 data[8192] = { 0 }, auth[16] = { 0 }, status = 0;
	const unsigned offsets[] = { 64, 65, 66, 96, 98, 198, 222, 420 };
	UT_ASSERT(cluster_mx_history_request_encode(&r, &w));
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		b = w;
		b.bytes[offsets[i]] ^= 0x40;
		ClusterR4WireWriteU32(b.bytes + 416, crc(b.bytes, 416));
		UT_ASSERT(!cluster_mx_history_request_decode(&b, 424, 1, 0, 9, &d));
	}
	p.data_crc = crc(data, sizeof(data));
	ClusterR4WireWriteU64(auth, 12);
	ClusterR4WireWriteU64(auth + 8, 1001);
	UT_ASSERT(cluster_mx_history_peer_encode(&p, &pw));
	UT_ASSERT(cluster_mx_history_reply_encode(&w, 0, 130, data, auth, &pw, &rw));
	ClusterR4WireWriteU16(rw.bytes + 8680 + 24, 4);
	ClusterR4WireWriteU32(rw.bytes + 24, crc(rw.bytes + 48, 8768));
	UT_ASSERT(!cluster_mx_history_reply_decode(&rw, 8816, 0, &w, &status, &out));
	UT_ASSERT(zero(&out, sizeof(out)));
	ClusterR4WireWriteU16(rw.bytes + 8680 + 24, 3);
	ClusterR4WireWriteU32(rw.bytes + 8680 + 16, 1 + CLUSTER_UNDO_SEGS_PER_INSTANCE);
	ClusterR4WireWriteU32(rw.bytes + 24, crc(rw.bytes + 48, 8768));
	UT_ASSERT(!cluster_mx_history_reply_decode(&rw, 8816, 0, &w, &status, &out));
	UT_ASSERT(zero(&out, sizeof(out)));
}
UT_TEST(unaligned_frames_and_cancelled_request_never_gain_admission)
{
	ClusterMxHistoryRequest r = request(), d;
	ClusterMxHistoryRequestWire w, c;
	ClusterMxHistoryPeer p = peer();
	ClusterMxHistoryPeerWire pw;
	ClusterMxHistoryReplyWire rw;
	ClusterMxHistoryPeerResult out;
	uint8 raw[8817], data[8192] = { 0 }, auth[16] = { 0 }, status = 0;
	p.data_crc = crc(data, sizeof(data));
	ClusterR4WireWriteU64(auth, 12);
	ClusterR4WireWriteU64(auth + 8, 1001);
	UT_ASSERT(cluster_mx_history_request_encode(&r, &w));
	memcpy(raw + 1, &w, sizeof(w));
	UT_ASSERT(cluster_mx_history_request_decode(raw + 1, 424, 1, 0, 9, &d));
	UT_ASSERT(cluster_mx_history_peer_encode(&p, &pw));
	UT_ASSERT(cluster_mx_history_reply_encode(&w, 0, 130, data, auth, &pw, &rw));
	memcpy(raw + 1, &rw, sizeof(rw));
	UT_ASSERT(cluster_mx_history_reply_decode(raw + 1, 8816, 0, &w, &status, &out));
	UT_ASSERT_EQ(out.admitted, 0);
	UT_ASSERT(cluster_mx_history_request_cancel(&w, &c));
	UT_ASSERT(!cluster_mx_history_reply_decode(raw + 1, 8816, 0, &c, &status, &out));
	UT_ASSERT(zero(&out, sizeof(out)));
}
int
main(void)
{
	UT_PLAN(11);
	UT_RUN(request_roundtrip_literal_offsets);
	UT_RUN(request_semantic_failures_clear_output);
	UT_RUN(request_wire_rejects_mutations_and_transport_mismatch);
	UT_RUN(request_cancel_is_exact_and_idempotent);
	UT_RUN(global_and_prior_edges_keep_exact_owner_constraints);
	UT_RUN(peer_roundtrip_and_invalid_dispositions);
	UT_RUN(reply_echo_data_and_proof_are_one_checked_body);
	UT_RUN(reply_corrupt_fields_never_publish_partial_output);
	UT_RUN(negative_reply_has_no_page_or_proof);
	UT_RUN(recomputed_crc_does_not_hide_wrong_slot_or_unsupported_context);
	UT_RUN(unaligned_frames_and_cancelled_request_never_gain_admission);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
