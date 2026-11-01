/* Range-codec contract tests. No product codec or admission stub is provided. */
int k_mx_g5_range_original_main(void);
#define main k_mx_g5_range_original_main
#include "test_cluster_multixact_current.c"
#undef main

#include "cluster/cluster_foreign_mx_format.h"
#include "cluster/cluster_multixact_range_wire.h"

/* Controlled source metadata, never native storage/H or caller authority. */
static const unsigned char k_range_source[64] = {19, 31, 47, 61};

static ClusterMxRangeWireContext
k_range_context(void)
{
	ClusterMxRangeWireContext ctx;

	memset(&ctx, 0, sizeof(ctx));
	ctx.request_id = 501;
	ctx.epoch = 9;
	ctx.key = test_mxkey();
	ctx.format_id = CLUSTER_FOREIGN_MX_FORMAT_ID_V1;
	ctx.version = CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1;
	/* Unit inputs stand in for observed loaded-control/CONTROL fields. */
	ctx.loaded_catalog = 202611010;
	ctx.peer_capabilities = CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES;
	ctx.peer_generation = 42;
	ctx.requested_cap = 256;
	ctx.source = k_range_source;
	ctx.source_bytes = sizeof(k_range_source);
	return ctx;
}

static ClusterMxCompositionReply
k_range_reply(uint32 total, uint32 first, uint32 count,
			  ClusterCurrentMxMemberDesc *members)
{
	ClusterMxCompositionReply r;
	uint32 i;

	memset(&r, 0, sizeof(r));
	r.source = k_range_source;
	r.source_bytes = sizeof(k_range_source);
	r.total = total;
	r.first = first;
	r.count = count;
	r.next = first + count;
	r.cap = 256;
	r.last = r.next == total;
	r.members = members;
	r.member_bytes = count * sizeof(*members);
	for (i = 0; i < count; i++)
		test_member(&members[i], 100 + first + i, MultiXactStatusForShare);
	return r;
}

static uint32
k_encode_range(const ClusterMxRangeWireContext *ctx,
			   const ClusterMxCompositionReply *reply, unsigned char *wire)
{
	uint32 bytes = 0;

	memset(wire, 0xa5, BLCKSZ);
	UT_ASSERT(cluster_mx_range_wire_encode_reply(ctx, MXCOMP_READ_OK, reply,
		wire, BLCKSZ, &bytes));
	UT_ASSERT(bytes > 0 && bytes <= BLCKSZ);
	return bytes;
}

static void
k_expect_bad_range(const void *wire, uint32 bytes,
				   const ClusterMxRangeWireContext *ctx)
{
	ClusterMxCompositionReply out, empty;

	memset(&out, 0xa5, sizeof(out));
	memset(&empty, 0, sizeof(empty));
	UT_ASSERT_EQ(cluster_mx_range_wire_decode_reply(wire, bytes, ctx, &out),
		MXCOMP_READ_UNKNOWN);
	UT_ASSERT_EQ(memcmp(&out, &empty, sizeof(out)), 0);
}

static void
k_roundtrip_range(uint32 total, uint32 first, uint32 count)
{
	ClusterMxRangeWireContext ctx = k_range_context();
	ClusterCurrentMxMemberDesc members[256];
	ClusterMxCompositionReply r = k_range_reply(total, first, count, members), out;
	unsigned char wire[BLCKSZ];
	uint32 bytes = k_encode_range(&ctx, &r, wire);

	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_mx_range_wire_decode_reply(wire, bytes, &ctx, &out),
		MXCOMP_READ_OK);
	UT_ASSERT_EQ(out.total, total);
	UT_ASSERT_EQ(out.first, first);
	UT_ASSERT_EQ(out.count, count);
	UT_ASSERT_EQ(out.next, first + count);
	UT_ASSERT_EQ(out.last, first + count == total);
	UT_ASSERT_EQ(out.source_bytes, 64);
	UT_ASSERT_EQ(out.cap, 256);
	UT_ASSERT_EQ(out.frame_bytes, bytes);
	UT_ASSERT_EQ(out.member_bytes, (Size)count * sizeof(*members));
	UT_ASSERT_NOT_NULL(out.source);
	UT_ASSERT_NOT_NULL(out.members);
	if (out.source != NULL)
		UT_ASSERT_EQ(memcmp(out.source, k_range_source, 64), 0);
	if (out.members != NULL)
		UT_ASSERT_EQ(memcmp(out.members, members, (Size)count * sizeof(*members)), 0);
}

UT_TEST(k_range_accepts_2_256_and_both_257_pieces)
{
	k_roundtrip_range(2, 0, 2);
	k_roundtrip_range(256, 0, 256);
	k_roundtrip_range(257, 0, 256);
	k_roundtrip_range(257, 256, 1);
}

UT_TEST(k_range_preserves_32_bit_positions)
{
	k_roundtrip_range(65537, 65536, 1);
	k_roundtrip_range(INT32_MAX, INT32_MAX - 1, 1);
}

UT_TEST(k_range_requires_exact_physical_and_declared_lengths)
{
	ClusterMxRangeWireContext ctx = k_range_context();
	ClusterCurrentMxMemberDesc members[2];
	ClusterMxCompositionReply r = k_range_reply(2, 0, 2, members);
	unsigned char wire[BLCKSZ];
	ClusterMxRangeWireHeader h;
	uint32 bytes = k_encode_range(&ctx, &r, wire);

	k_expect_bad_range(wire, 0, &ctx);
	k_expect_bad_range(wire, bytes - 1, &ctx);
	k_expect_bad_range(wire, bytes + 1, &ctx);
	k_expect_bad_range(wire, UINT32_MAX, &ctx);
	memcpy(&h, wire, sizeof(h));
	h.wire_length--;
	memcpy(wire, &h, sizeof(h));
	k_expect_bad_range(wire, bytes, &ctx);
}

UT_TEST(k_range_binds_allocated_identity_and_exact_lineage)
{
	ClusterMxRangeWireContext ctx = k_range_context();
	ClusterCurrentMxMemberDesc members[2];
	ClusterMxCompositionReply r = k_range_reply(2, 0, 2, members);
	unsigned char wire[BLCKSZ];
	int fault;

	for (fault = 0; fault < 9; fault++) {
		ClusterMxRangeWireHeader h;
		uint32 bytes = k_encode_range(&ctx, &r, wire);

		memcpy(&h, wire, sizeof(h));
		switch (fault) {
		case 0: h.format_id ^= 1; break;
		case 1: h.version ^= 1; break;
		case 2: h.request_id++; break;
		case 3: h.epoch++; break;
		case 4: h.key.origin_node_id++; break;
		case 5: h.key.multixact_id++; break;
		case 6: h.key.cluster_epoch++; break;
		case 7: h.peer_generation++; break;
		case 8: h.source_bytes--; break;
		}
		memcpy(wire, &h, sizeof(h));
		k_expect_bad_range(wire, bytes, &ctx);
	}
}

UT_TEST(k_range_compares_every_source_byte)
{
	ClusterMxRangeWireContext ctx = k_range_context();
	ClusterCurrentMxMemberDesc members[2];
	ClusterMxCompositionReply r = k_range_reply(2, 0, 2, members);
	unsigned char wire[BLCKSZ], changed[64];
	uint32 bytes = k_encode_range(&ctx, &r, wire);
	Size i;

	for (i = 0; i < sizeof(changed); i++) {
		memcpy(changed, k_range_source, sizeof(changed));
		changed[i] ^= 1;
		ctx.source = changed;
		k_expect_bad_range(wire, bytes, &ctx);
	}
}

UT_TEST(k_range_rejects_unqualified_peer_metadata)
{
	ClusterMxRangeWireContext good = k_range_context();
	ClusterCurrentMxMemberDesc members[2];
	ClusterMxCompositionReply r = k_range_reply(2, 0, 2, members);
	unsigned char wire[BLCKSZ];
	uint32 bytes = k_encode_range(&good, &r, wire);
	int fault;

	for (fault = 0; fault < 9; fault++) {
		ClusterMxRangeWireContext bad = good;

		switch (fault) {
		case 0: bad.peer_generation = 0; break;
		case 1: bad.peer_generation = UINT32_MAX; break;
		case 2: bad.loaded_catalog = 202611009; break;
		case 3: bad.peer_capabilities &= ~PGRAC_IC_HELLO_CAP_MULTIXACT_FULL_V1; break;
		case 4: bad.peer_capabilities &= ~PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1; break;
		case 5: bad.peer_capabilities &= ~PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1; break;
		case 6: bad.requested_cap = 1; break;
		case 7: bad.requested_cap = 0; break;
		case 8: bad.requested_cap = 257; break;
		}
		k_expect_bad_range(wire, bytes, &bad);
	}
}

UT_TEST(k_new_and_legacy_describe_domains_are_separate)
{
	ClusterMxRangeWireContext ctx = k_range_context();
	ClusterCurrentMxMemberDesc members[2], old_out[2];
	ClusterMxCompositionReply r = k_range_reply(2, 0, 2, members);
	ClusterCurrentMxDescribeReplyPage old;
	unsigned char wire[BLCKSZ];
	uint16 count;
	uint32 total;

	(void)k_encode_range(&ctx, &r, wire);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(wire,
		BLCKSZ, 2, 9, 501, &ctx.key, old_out, 2, &count, &total), CMX_DESC_UNKNOWN);
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT_EQ(total, 0);
	memset(&old, 0, sizeof(old));
	old.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	old.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	old.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	old.header.result = CMX_DESC_OK;
	old.header.source_node_id = 2;
	old.header.request_id = 501;
	old.header.mxkey = ctx.key;
	old.header.total_count = old.header.entry_count = 2;
	old.header.wire_length = 80;
	memcpy(old.members, members, sizeof(members));
	old.header.descriptor_hash = cluster_multixact_current_descriptor_hash(
		&ctx.key, old.members, 2);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(&old,
		sizeof(old), 2, 9, 501, &ctx.key, old_out, 2, &count, &total), CMX_DESC_OK);
	k_expect_bad_range(&old, sizeof(old), &ctx);
}

UT_TEST(k_range_rejects_overflow_and_inconsistent_tail)
{
	ClusterMxRangeWireContext ctx = k_range_context();
	ClusterCurrentMxMemberDesc members[2];
	ClusterMxCompositionReply good = k_range_reply(2, 0, 2, members);
	unsigned char wire[BLCKSZ];
	int fault;

	for (fault = 0; fault < 5; fault++) {
		ClusterMxCompositionReply bad = good;
		uint32 bytes = 99;

		switch (fault) {
		case 0: bad.first = UINT32_MAX; bad.next = 1; break;
		case 1: bad.total = UINT32_MAX; break;
		case 2: bad.last = false; break;
		case 3: bad.next = 1; break;
		case 4: bad.member_bytes--; break;
		}
		UT_ASSERT(!cluster_mx_range_wire_encode_reply(&ctx, MXCOMP_READ_OK,
			&bad, wire, BLCKSZ, &bytes));
		UT_ASSERT_EQ(bytes, 0);
	}
}

UT_TEST(k_range_non_ok_never_exports_members)
{
	ClusterMxRangeWireContext ctx = k_range_context();
	unsigned char wire[BLCKSZ];
	int status;

	for (status = MXCOMP_READ_RETRY; status <= MXCOMP_READ_UNKNOWN; status++) {
		ClusterMxCompositionReply out, empty;
		uint32 bytes = 0;

		UT_ASSERT(cluster_mx_range_wire_encode_reply(&ctx, status, NULL,
			wire, BLCKSZ, &bytes));
		memset(&out, 0xa5, sizeof(out));
		memset(&empty, 0, sizeof(empty));
		UT_ASSERT_EQ(cluster_mx_range_wire_decode_reply(wire, bytes, &ctx, &out), status);
		UT_ASSERT_EQ(memcmp(&out, &empty, sizeof(out)), 0);
	}
}

UT_TEST(k_proof_asks_roundtrip_32_bit_global_ordinal)
{
	ClusterMxRangeWireContext ctx = k_range_context();
	ClusterCurrentMxMemberDesc members[1];
	ClusterMxRangeProofAsk out[7];
	unsigned char wire[BLCKSZ];
	uint32 bytes = 0, count = 99, total = 99;

	test_member(&members[0], 100, MultiXactStatusForShare);
	UT_ASSERT(cluster_mx_range_wire_encode_proof_asks(&ctx, 65537, 65536,
		members, 1, wire, BLCKSZ, &bytes));
	UT_ASSERT(cluster_mx_range_wire_decode_proof_asks(wire, bytes, &ctx,
		out, 7, &count, &total));
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(total, 65537);
	UT_ASSERT_EQ(out[0].member_ordinal, 65536);
	UT_ASSERT_EQ(out[0].xid, 100);
	UT_ASSERT_EQ(out[0].member_status, MultiXactStatusForShare);
}

UT_TEST(k_proof_frame_bound_is_separate_from_total)
{
	ClusterMxRangeWireContext ctx = k_range_context();
	ClusterCurrentMxMemberDesc members[8];
	ClusterMxRangeProofAsk out[7];
	unsigned char wire[BLCKSZ];
	uint32 bytes = 0, count = 99, total = 99;
	uint32 i;

	for (i = 0; i < 8; i++)
		test_member(&members[i], 100 + i, MultiXactStatusForShare);
	UT_ASSERT(cluster_mx_range_wire_encode_proof_asks(&ctx, 65537, 65530,
		members, 7, wire, BLCKSZ, &bytes));
	UT_ASSERT(cluster_mx_range_wire_decode_proof_asks(wire, bytes, &ctx,
		out, 7, &count, &total));
	UT_ASSERT_EQ(count, 7);
	UT_ASSERT_EQ(total, 65537);
	UT_ASSERT_EQ(out[6].member_ordinal, 65536);
	bytes = 99;
	UT_ASSERT(!cluster_mx_range_wire_encode_proof_asks(&ctx, 65537, 65529,
		members, 8, wire, BLCKSZ, &bytes));
	UT_ASSERT_EQ(bytes, 0);
	bytes = 99;
	UT_ASSERT(!cluster_mx_range_wire_encode_proof_asks(&ctx, INT32_MAX,
		UINT32_MAX, members, 2, wire, BLCKSZ, &bytes));
	UT_ASSERT_EQ(bytes, 0);
}

UT_TEST(k_range_and_proof_payload_kinds_are_separate)
{
	ClusterMxRangeWireContext ctx = k_range_context();
	ClusterCurrentMxMemberDesc members[2];
	ClusterMxCompositionReply r = k_range_reply(2, 0, 2, members);
	ClusterMxRangeProofAsk out[7], empty[7];
	unsigned char wire[BLCKSZ];
	uint32 bytes = 0, count = 99, total = 99;

	UT_ASSERT(cluster_mx_range_wire_encode_proof_asks(&ctx, 2, 0,
		members, 2, wire, BLCKSZ, &bytes));
	k_expect_bad_range(wire, bytes, &ctx);
	bytes = k_encode_range(&ctx, &r, wire);
	memset(out, 0xa5, sizeof(out));
	memset(empty, 0, sizeof(empty));
	UT_ASSERT(!cluster_mx_range_wire_decode_proof_asks(wire, bytes, &ctx,
		out, 7, &count, &total));
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT_EQ(total, 0);
	UT_ASSERT_EQ(memcmp(out, empty, sizeof(out)), 0);
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(k_range_accepts_2_256_and_both_257_pieces);
	UT_RUN(k_range_preserves_32_bit_positions);
	UT_RUN(k_range_requires_exact_physical_and_declared_lengths);
	UT_RUN(k_range_binds_allocated_identity_and_exact_lineage);
	UT_RUN(k_range_compares_every_source_byte);
	UT_RUN(k_range_rejects_unqualified_peer_metadata);
	UT_RUN(k_new_and_legacy_describe_domains_are_separate);
	UT_RUN(k_range_rejects_overflow_and_inconsistent_tail);
	UT_RUN(k_range_non_ok_never_exports_members);
	UT_RUN(k_proof_asks_roundtrip_32_bit_global_ordinal);
	UT_RUN(k_proof_frame_bound_is_separate_from_total);
	UT_RUN(k_range_and_proof_payload_kinds_are_separate);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
