/* Codec-only migration regressions. Literal old frames must not depend on
 * the new encoder, and no frame in this test confers origin authority. */
int history_wire_fixture_main(void);
#define main history_wire_fixture_main
#include "test_cluster_mx_history_wire.c"
#undef main

enum {
	OLD_PEER_BYTES = 136,
	OLD_REPLY_BYTES = 8816,
	OLD_BODY_BYTES = 8768,
	ENVELOPE_PEER_BYTES = 280,
	ENVELOPE_REPLY_BYTES = 8960,
	ENVELOPE_BODY_BYTES = 8912
};

/* Serialize the old prefix literally, not by calling a current encoder. */
static void
old_peer_bytes(uint8 *bytes, const ClusterMxHistoryPeer *p, bool creator)
{
	memset(bytes, 0, OLD_PEER_BYTES);
	ClusterR4WireWriteU32(bytes, UINT32_C(0x3158484d));
	ClusterR4WireWriteU16(bytes + 4, creator ? 2 : 1);
	ClusterR4WireWriteU16(bytes + 6, OLD_PEER_BYTES);
	ClusterR4WireWriteU32(bytes + 8, p->flags);
	bytes[12] = p->record_class;
	bytes[13] = p->outcome;
	bytes[14] = p->effect;
	bytes[15] = p->proof_kind;
	ClusterR4WireWriteU32(bytes + 16, p->canonical.segment_id);
	ClusterR4WireWriteU32(bytes + 20, p->canonical.xid);
	ClusterR4WireWriteU16(bytes + 24, p->canonical.slot_offset);
	ClusterR4WireWriteU16(bytes + 26, p->canonical.wrap);
	ClusterR4WireWriteU32(bytes + 28, p->canonical_generation);
	ClusterR4WireWriteU64(bytes + 32, U64FromFullTransactionId(p->top_fxid));
	ClusterR4WireWriteU64(bytes + 40, U64FromFullTransactionId(p->operation_fxid));
	ClusterR4WireWriteU64(bytes + 48, p->operation_sequence);
	ClusterR4WireWriteU64(bytes + 56, p->record.raw[0]);
	ClusterR4WireWriteU64(bytes + 64, p->record.raw[1]);
	ClusterR4WireWriteU64(bytes + 72, p->previous.raw[0]);
	ClusterR4WireWriteU64(bytes + 80, p->previous.raw[1]);
	ClusterR4WireWriteU32(bytes + 88, p->data_generation);
	ClusterR4WireWriteU32(bytes + 92, p->data_crc);
	ClusterR4WireWriteU64(bytes + 96, p->data_lsn);
	ClusterR4WireWriteU64(bytes + 104, p->commit_scn);
	ClusterR4WireWriteU64(bytes + 112, p->formation_epoch);
	ClusterR4WireWriteU64(bytes + 120, p->admission_generation);
	ClusterR4WireWriteU32(bytes + 128, p->ctrc_grant);
}

static void
reject_old_peer(bool creator)
{
	/* Full backing storage also makes this safe for the future fixed-size
	 * decoder; the declared old length/version are what must be rejected. */
	uint8 bytes[ENVELOPE_PEER_BYTES] = { 0 };
	ClusterMxHistoryPeer p = creator ? creator_peer() : peer(), decoded;
	bool accepted;

	old_peer_bytes(bytes, &p, creator);
	memset(&decoded, 0xaa, sizeof(decoded));
	accepted = cluster_mx_history_peer_decode((const ClusterMxHistoryPeerWire *)bytes, &decoded);
	UT_ASSERT(!accepted);
	UT_ASSERT(zero(&decoded, sizeof(decoded)));
}

static void
reject_old_reply(bool creator)
{
	ClusterMxHistoryRequest r = creator ? creator_request() : request();
	ClusterMxHistoryRequestWire expected;
	ClusterMxHistoryPeer p = creator ? creator_peer() : peer();
	ClusterMxHistoryPeerResult result;
	uint8 bytes[OLD_REPLY_BYTES] = { 0 };
	uint8 status = 0xaa;
	bool encoded, accepted;

	encoded = cluster_mx_history_request_encode(&r, &expected);
	UT_ASSERT(encoded);
	if (!encoded)
		return;
	ClusterR4WireWriteU64(bytes, r.forward.request_id);
	ClusterR4WireWriteU64(bytes + 8, 130);
	ClusterR4WireWriteU64(bytes + 16, r.forward.epoch);
	ClusterR4WireWriteU32(bytes + 28, r.forward.master_node);
	ClusterR4WireWriteU32(bytes + 32, r.forward.requester_backend_id);
	bytes[36] = PCM_TRANS_N_TO_S;
	bytes[37] = CLUSTER_MX_HISTORY_REPLY_STATUS;
	ClusterR4WireWriteU32(bytes + 38, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	memcpy(bytes + 48, &expected, sizeof(expected));
	ClusterR4WireWriteU64(bytes + 8664, p.canonical_generation);
	ClusterR4WireWriteU64(bytes + 8672, 1001);
	p.data_crc = crc(bytes + 472, 8192);
	old_peer_bytes(bytes + 8680, &p, creator);
	ClusterR4WireWriteU32(bytes + 24, crc(bytes + 48, OLD_BODY_BYTES));
	memset(&result, 0xaa, sizeof(result));
	accepted = cluster_mx_history_reply_decode(bytes, sizeof(bytes),
		r.forward.master_node, &expected, &status, &result);
	UT_ASSERT(!accepted);
	UT_ASSERT_EQ(status, 0);
	UT_ASSERT(zero(&result, sizeof(result)));
}

UT_TEST(history_envelope_carriers_have_complete_size_and_distinct_versions)
{
	UT_ASSERT_EQ(CLUSTER_MX_HISTORY_VERSION, 3);
	UT_ASSERT_EQ(CLUSTER_FOREIGN_CREATOR_HISTORY_VERSION, 4);
	UT_ASSERT_EQ(sizeof(ClusterMxHistoryRequestWire), 424);
	UT_ASSERT_EQ(sizeof(ClusterMxHistoryPeerWire), ENVELOPE_PEER_BYTES);
	UT_ASSERT_EQ(CLUSTER_MX_HISTORY_PEER_BYTES, ENVELOPE_PEER_BYTES);
	UT_ASSERT_EQ(sizeof(ClusterMxHistoryReplyWire), ENVELOPE_REPLY_BYTES);
	UT_ASSERT_EQ(CLUSTER_MX_HISTORY_REPLY_BYTES, ENVELOPE_REPLY_BYTES);
	UT_ASSERT_EQ(CLUSTER_MX_HISTORY_REPLY_BODY_BYTES, ENVELOPE_BODY_BYTES);
	UT_ASSERT_EQ(sizeof(ClusterMxHistoryPeerResult), 8920);
	UT_ASSERT_EQ(sizeof(ClusterForeignCreatorResult), 8920);
	UT_ASSERT_EQ(offsetof(ClusterForeignCreatorResult, peer), 8632);
}

UT_TEST(active_live_grant_without_origin_envelope_cannot_encode)
{
	ClusterMxHistoryPeer p = peer();
	ClusterMxHistoryPeerWire wire;

	/* peer() is zero-initialized, including any appended native envelope. */
	memset(&wire, 0xaa, sizeof(wire));
	UT_ASSERT(!cluster_mx_history_peer_encode(&p, &wire));
	UT_ASSERT(zero(&wire, sizeof(wire)));
}

UT_TEST(legacy_history_136B_peer_is_rejected) { reject_old_peer(false); }
UT_TEST(legacy_creator_136B_peer_is_rejected) { reject_old_peer(true); }
UT_TEST(legacy_history_8816B_reply_is_rejected) { reject_old_reply(false); }
UT_TEST(legacy_creator_8816B_reply_is_rejected) { reject_old_reply(true); }

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(history_envelope_carriers_have_complete_size_and_distinct_versions);
	UT_RUN(active_live_grant_without_origin_envelope_cannot_encode);
	UT_RUN(legacy_history_136B_peer_is_rejected);
	UT_RUN(legacy_creator_136B_peer_is_rejected);
	UT_RUN(legacy_history_8816B_reply_is_rejected);
	UT_RUN(legacy_creator_8816B_reply_is_rejected);
	UT_RUN(negative_reply_has_no_page_or_proof);
	UT_DONE();
	return ut_failed_count != 0;
}
