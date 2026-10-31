/*-------------------------------------------------------------------------
 * test_cluster_mx_operation_wire.c
 *    Real dormant codecs, with the existing MX fixture's runtime leaves.
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
int operation_unused_mx_main(void);
#define main operation_unused_mx_main
#include "test_cluster_multixact_current.c"
#undef main
#include "cluster/cluster_multixact_operation_wire.h"

typedef struct OperationWireFixture {
	ClusterCurrentMxKey key;
	ClusterCurrentMxMemberDesc members[8];
	ClusterSavepointOperationKey operations[8];
	uint16 origins[8];
	ClusterCurrentMxOperationRequestPlan plans[8];
	uint16 count;
} OperationWireFixture;

static bool
ow_zero(const void *data, Size size)
{
	const uint8 *bytes = data;
	for (Size i = 0; i < size; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

static uint64
ow_value(const void *bytes, unsigned offset, unsigned size)
{
	uint64 value = 0;
	memcpy(&value, (const char *)bytes + offset, size);
	return value;
}

static void
ow_init(OperationWireFixture *f, unsigned n)
{
	memset(f, 0, sizeof(*f));
	f->key = test_mxkey();
	for (unsigned i = 0; i < n; i++) {
		uint16 origin;
		test_member(&f->members[i], 100 + i * 3, MultiXactStatusForShare);
		origin = f->origins[i] = cluster_xid_origin_slot(f->members[i].xid);
		f->operations[i] = (ClusterSavepointOperationKey){
			.locator = { .uba = uba_encode(origin * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1, 2, 2, 0),
						 .xid = 97,
						 .tt_wrap = 3,
						 .itl_kind = ITL_FLAG_ACTIVE,
						 .itl_slot_index = 0 },
			.target
			= { .locator = { 1663, 42, 123 }, .forknum = MAIN_FORKNUM, .blockno = 17, .offnum = 7 },
			.top_fxid = FullTransactionIdFromEpochAndXid(2, 97),
			.operation_fxid = FullTransactionIdFromEpochAndXid(2, f->members[i].xid)
		};
	}
}

static bool
ow_build(OperationWireFixture *f, unsigned n, const ClusterCurrentUpdaterChallenge *challenge)
{
	return cluster_multixact_operation_wire_build_requests(
			   &f->key, f->members, f->origins, f->operations, n,
			   cluster_multixact_current_descriptor_hash(&f->key, f->members, n), challenge, 801, 9,
			   3, 7, f->plans, lengthof(f->plans), &f->count)
		   == CMX_RESOLVE_OK;
}

static ClusterCurrentMemberOperationProof
ow_proof(const OperationWireFixture *f, unsigned index, uint8 state, uint8 effect)
{
	ClusterCurrentMemberOperationProof p = { 0 };
	p.base.member_xid = f->members[index].xid;
	p.base.member_ordinal = index;
	p.base.member_status = f->members[index].member_status;
	p.base.state = state;
	p.base.commit_scn = state == CCM_COMMITTED ? scn_encode(0, 900) : InvalidScn;
	p.format_version = 2;
	if (!ow_zero(&f->operations[index], sizeof(f->operations[index]))) {
		p.flags = 1;
		p.effect_state = effect;
		p.operation_uba = f->operations[index].locator.uba;
		p.top_fxid = f->operations[index].top_fxid;
		p.operation_fxid = f->operations[index].operation_fxid;
		p.base.key = (ClusterCurrentMemberProofKey){
			.origin_node_id = f->origins[index],
			.undo_segment_id = f->origins[index] * CLUSTER_UNDO_SEGS_PER_INSTANCE + 3,
			.tt_slot_id = 3,
			.cluster_epoch = 9,
			.local_xid = 97,
			.segment_generation = 7,
			.slot_wrap = 3,
			.binding_version = 1
		};
	} else if (state == CCM_ACTIVE) {
		p.base.key = (ClusterCurrentMemberProofKey){
			.origin_node_id = f->origins[index],
			.undo_segment_id = f->origins[index] * CLUSTER_UNDO_SEGS_PER_INSTANCE + 3,
			.tt_slot_id = 3,
			.cluster_epoch = 9,
			.local_xid = p.base.member_xid,
			.segment_generation = 7,
			.slot_wrap = 3,
			.binding_version = 1
		};
	}
	if (state == CCM_ACTIVE)
		ClusterCurrentMemberProofSetCtrcGrant(&p.base, 47);
	return p;
}

static bool
ow_page(OperationWireFixture *f, ClusterCurrentMemberOperationProof *p,
		ClusterCurrentMxOperationReplyPageV6 *page)
{
	return cluster_multixact_operation_wire_encode_reply(
		&f->plans[0].request, f->origins[0], 9, CMX_RESOLVE_OK, p,
		f->plans[0].request.prefix.entry_count, NULL, 313, page);
}

static void
ow_bad_reply(const OperationWireFixture *f, const void *page, uint32 length)
{
	ClusterCurrentMemberOperationProof out[7];
	ClusterCurrentUpdaterProof updater;
	ClusterMxResolveResult result = CMX_RESOLVE_OK;
	uint16 count = 7;
	uint32 cap = 313;
	memset(out, 0xa5, sizeof(out));
	memset(&updater, 0xa5, sizeof(updater));
	UT_ASSERT(!cluster_multixact_operation_wire_validate_reply(page, length, f->origins[0], 9,
															   &f->plans[0].request, &result, out,
															   7, &count, &updater, &cap));
	UT_ASSERT_EQ(result, CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT_EQ(cap, 0);
	UT_ASSERT_EQ(updater.verdict, CUCP_UNKNOWN);
	for (unsigned i = 0; i < 7; i++) {
		ClusterCurrentMemberOperationProof unknown
			= { .base = { .state = CCM_UNKNOWN }, .format_version = 2 };
		UT_ASSERT_EQ(memcmp(&out[i], &unknown, sizeof(unknown)), 0);
	}
}

UT_TEST(request_exact_literal_layout)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationAsk ask;
	const uint8 *b = f.plans[0].request.body;
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(f.count, 1);
	UT_ASSERT_EQ(sizeof(f.plans[0].request), 768);
	UT_ASSERT_EQ(ow_value(&f.plans[0].request, 64, 4), UINT32_C(0x5047434d));
	UT_ASSERT_EQ(ow_value(&f.plans[0].request, 68, 2), 6);
	UT_ASSERT_EQ(ow_value(b, 0, 4), 100);
	UT_ASSERT_EQ(ow_value(b, 4, 2), 0);
	UT_ASSERT_EQ(b[6], MultiXactStatusForShare);
	UT_ASSERT_EQ(b[7], 1);
	UT_ASSERT_EQ(ow_value(b, 24, 4), 97);
	UT_ASSERT_EQ(memcmp(b + 8, b + 32, 16), 0);
	UT_ASSERT_EQ(ow_value(b, 48, 4), 1663);
	UT_ASSERT_EQ(ow_value(b, 52, 4), 42);
	UT_ASSERT_EQ(ow_value(b, 56, 4), 123);
	UT_ASSERT_EQ(ow_value(b, 60, 4), MAIN_FORKNUM);
	UT_ASSERT_EQ(ow_value(b, 64, 4), 17);
	UT_ASSERT_EQ(ow_value(b, 68, 2), 7);
	UT_ASSERT_EQ(ow_value(b, 72, 8), UINT64_C(0x200000061));
	UT_ASSERT_EQ(ow_value(b, 80, 8), UINT64_C(0x200000064));
	UT_ASSERT_EQ(ow_value(b, 88, 2), 1);
	UT_ASSERT_EQ(ow_value(b, 90, 2), 96);
	UT_ASSERT(ow_zero(b + 92, 4));
	UT_ASSERT(ow_zero(b + 96, 576 + 24));
	UT_ASSERT(cluster_multixact_operation_wire_get_ask(&f.plans[0].request, 0, &ask));
	UT_ASSERT(ask.exact);
	UT_ASSERT_EQ(memcmp(&ask.operation, &f.operations[0], sizeof(ask.operation)), 0);
}

UT_TEST(request_top_only_has_zero_context)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationAsk ask;
	ow_init(&f, 1);
	memset(f.operations, 0, sizeof(f.operations));
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(ow_zero(f.plans[0].request.body + 7, 89));
	UT_ASSERT(cluster_multixact_operation_wire_get_ask(&f.plans[0].request, 0, &ask));
	UT_ASSERT(!ask.exact);
	UT_ASSERT(ow_zero(&ask.operation, sizeof(ask.operation)));
}

UT_TEST(updater_request_carries_challenge_and_exact_entry)
{
	OperationWireFixture f;
	ClusterCurrentUpdaterChallenge challenge;
	ClusterCurrentMxOperationForwardV6 decoded;
	ClusterCurrentMxOperationAsk ask;
	ow_init(&f, 1);
	f.members[0].member_status = MultiXactStatusNoKeyUpdate;
	test_updater_challenge(&challenge, f.origins[0], 100, 20);
	if (!ow_build(&f, 1, &challenge)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(f.count, 1);
	UT_ASSERT_EQ(f.plans[0].request.prefix.body_kind, 2);
	UT_ASSERT_EQ(ow_value(f.plans[0].request.body, 56, 4), 100);
	UT_ASSERT_EQ(f.plans[0].request.body[63], 1);
	UT_ASSERT(ow_zero(f.plans[0].request.body + 152, 520 + 24));
	UT_ASSERT(cluster_multixact_operation_wire_validate_forward(&f.plans[0].request, 768, 3,
																f.origins[0], 9, &decoded));
	UT_ASSERT(cluster_multixact_operation_wire_get_ask(&decoded, 0, &ask));
	UT_ASSERT_EQ(ask.member.xid, 100);
	decoded.body[56] ^= 1;
	UT_ASSERT(!cluster_multixact_operation_wire_validate_forward(&decoded, 768, 3, f.origins[0], 9,
																 &f.plans[0].request));
}

UT_TEST(batch_256_preserves_grouping_order_and_seven_limit)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[256];
	uint16 origins[256], count;
	bool seen[256] = { false };
	ClusterCurrentMxOperationRequestPlan plans[256];
	for (unsigned i = 0; i < 256; i++) {
		test_member(&members[i], 100 + i, MultiXactStatusForShare);
		origins[i] = cluster_xid_origin_slot(members[i].xid);
	}
	UT_ASSERT_EQ(cluster_multixact_operation_wire_build_requests(
					 &key, members, origins, NULL, 256,
					 cluster_multixact_current_descriptor_hash(&key, members, 256), NULL, 801, 9, 3,
					 7, plans, 256, &count),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(count, 39);
	for (unsigned i = 0; i < count; i++) {
		ClusterCurrentMxOperationForwardV6 decoded;
		UT_ASSERT_EQ(plans[i].request.prefix.chunk_ordinal, i);
		UT_ASSERT_EQ(plans[i].request.prefix.chunk_count_minus_one, 38);
		UT_ASSERT(cluster_multixact_operation_wire_validate_forward(
			&plans[i].request, 768, 3, plans[i].destination_node_id, 9, &decoded));
		UT_ASSERT(decoded.prefix.entry_count <= 7);
		for (unsigned j = 0; j < decoded.prefix.entry_count; j++) {
			ClusterCurrentMxOperationAsk ask;
			UT_ASSERT(cluster_multixact_operation_wire_get_ask(&decoded, j, &ask));
			UT_ASSERT(!seen[ask.member.member_ordinal]);
			seen[ask.member.member_ordinal] = true;
			UT_ASSERT_EQ(ask.member.xid, members[ask.member.member_ordinal].xid);
		}
	}
	for (unsigned i = 0; i < 256; i++)
		UT_ASSERT(seen[i]);
}

UT_TEST(planner_failures_clear_all_plans)
{
	OperationWireFixture f;
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	f.origins[0]++;
	UT_ASSERT(!ow_build(&f, 1, NULL));
	UT_ASSERT_EQ(f.count, 0);
	UT_ASSERT(ow_zero(f.plans, sizeof(f.plans)));
	UT_ASSERT_EQ(cluster_multixact_operation_wire_build_requests(
					 NULL, NULL, NULL, NULL, 257, 0, NULL, 1, 9, 3, 7, f.plans, 8, &f.count),
				 CMX_RESOLVE_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(f.count, 0);
}

UT_TEST(incomplete_operation_never_downgrades)
{
	OperationWireFixture f;
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	for (unsigned c = 0; c < 10; c++) {
		ow_init(&f, 1);
		switch (c) {
		case 0:
			f.operations[0].operation_fxid = f.operations[0].top_fxid;
			break;
		case 1:
			f.operations[0].locator.xid++;
			break;
		case 2:
			f.operations[0].locator.tt_wrap = TT_WRAP_INVALID;
			break;
		case 3:
			f.operations[0].target._pad = 1;
			break;
		case 4:
			f.operations[0].target.offnum = 0;
			break;
		case 5:
			f.operations[0].target.forknum = FSM_FORKNUM;
			break;
		case 6:
			f.operations[0].locator.uba = uba_encode(1, 2, 2, 0);
			break;
		case 7:
			f.operations[0].top_fxid = FullTransactionIdFromU64(UINT64_MAX);
			break;
		case 8:
			f.operations[0].locator.itl_slot_index = 255;
			break;
		case 9:
			f.operations[0].locator.uba = uba_encode(1281, 0, 2, 0);
			break;
		}
		UT_ASSERT(!ow_build(&f, 1, NULL));
		UT_ASSERT_EQ(f.count, 0);
		UT_ASSERT(ow_zero(f.plans, sizeof(f.plans)));
	}
}

UT_TEST(forward_unaligned_exact_length_and_old_version_rejected)
{
	OperationWireFixture f;
	uint8 unaligned[769];
	ClusterCurrentMxOperationForwardV6 decoded;
	ClusterCurrentMxProofForwardV2 old;
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	memcpy(unaligned + 1, &f.plans[0].request, 768);
	UT_ASSERT(
		cluster_multixact_operation_wire_validate_forward(unaligned + 1, 768, 3, 5, 9, &decoded));
	UT_ASSERT(
		!cluster_multixact_operation_wire_validate_forward(unaligned + 1, 767, 3, 5, 9, &decoded));
	UT_ASSERT(ow_zero(&decoded, sizeof(decoded)));
	UT_ASSERT(
		!cluster_multixact_current_wire_validate_proof_forward(unaligned + 1, 768, 3, 5, 9, &old));
	f.plans[0].request.version = 5;
	UT_ASSERT(!cluster_multixact_operation_wire_validate_forward(&f.plans[0].request, 768, 3, 5, 9,
																 &decoded));
}

UT_TEST(forward_context_and_padding_tampering_rejected)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationForwardV6 decoded;
	const unsigned offsets[] = { 7, 8, 24, 31, 32, 60, 70, 72, 80, 88, 90, 92, 96, 672 };
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		ClusterCurrentMxOperationForwardV6 bad = f.plans[0].request;
		((uint8 *)&bad)[72 + offsets[i]] ^= 0x80;
		UT_ASSERT(!cluster_multixact_operation_wire_validate_forward(&bad, 768, 3, 5, 9, &decoded));
		UT_ASSERT(ow_zero(&decoded, sizeof(decoded)));
	}
}

UT_TEST(forward_envelope_and_chunk_bindings_rejected)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationForwardV6 decoded;
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	for (unsigned c = 0; c < 9; c++) {
		ClusterCurrentMxOperationForwardV6 bad = f.plans[0].request;
		switch (c) {
		case 0:
			bad.prefix.epoch++;
			break;
		case 1:
			bad.prefix.original_requester_node++;
			break;
		case 2:
			bad.prefix.requester_backend_id = 0;
			break;
		case 3:
			bad.prefix.chunk_ordinal = 1;
			break;
		case 4:
			bad.prefix.reserved_a[0] = 1;
			break;
		case 5:
			bad.prefix.body_kind = 3;
			break;
		case 6:
			bad.reserved16 = 1;
			break;
		case 7:
			bad.prefix.flags = 1;
			break;
		case 8:
			bad.magic = 0;
			break;
		}
		UT_ASSERT(!cluster_multixact_operation_wire_validate_forward(&bad, 768, 3, 5, 9, &decoded));
	}
}

UT_TEST(duplicate_ordinal_or_raw_member_rejected)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationForwardV6 bad, decoded;
	ow_init(&f, 2);
	if (!ow_build(&f, 2, NULL)) {
		UT_ASSERT(false);
		return;
	}
	bad = f.plans[0].request;
	memcpy(bad.body + 96 + 4, bad.body + 4, 2);
	UT_ASSERT(!cluster_multixact_operation_wire_validate_forward(&bad, 768, 3, 5, 9, &decoded));
	bad = f.plans[0].request;
	memcpy(bad.body + 96, bad.body, 4);
	UT_ASSERT(!cluster_multixact_operation_wire_validate_forward(&bad, 768, 3, 5, 9, &decoded));
}

UT_TEST(reply_exact_literal_layout)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationReplyPageV6 page;
	ClusterCurrentMemberOperationProof p;
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	p = ow_proof(&f, 0, CCM_ACTIVE, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	UT_ASSERT(ow_page(&f, &p, &page));
	UT_ASSERT_EQ(page.header.version, 6);
	UT_ASSERT_EQ(page.header.wire_length, 152);
	UT_ASSERT_EQ(page.header.requester_capability_generation, 313);
	UT_ASSERT_EQ(ow_value(page.body, 12, 4), 97);
	UT_ASSERT_EQ(ow_value(page.body, 32, 4), 100);
	UT_ASSERT_EQ(ow_value(page.body, 40, 4), 47);
	UT_ASSERT(ow_zero(page.body + 44, 4));
	UT_ASSERT_EQ(memcmp(page.body + 48, &p.operation_uba, 16), 0);
	UT_ASSERT_EQ(ow_value(page.body, 64, 8), UINT64_C(0x200000061));
	UT_ASSERT_EQ(ow_value(page.body, 72, 8), UINT64_C(0x200000064));
	UT_ASSERT_EQ(page.body[80], CLUSTER_SAVEPOINT_EFFECT_LIVE);
	UT_ASSERT_EQ(page.body[81], 2);
	UT_ASSERT_EQ(ow_value(page.body, 82, 2), 1);
	UT_ASSERT(ow_zero(page.body + 84, 4));
	UT_ASSERT(ow_zero((char *)&page + 152, 8040));
}

UT_TEST(top_state_and_operation_disposition_round_trip_independently)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationReplyPageV6 page;
	ClusterCurrentMemberOperationProof p, out;
	ClusterCurrentUpdaterProof updater;
	ClusterMxResolveResult result;
	uint16 count;
	uint32 cap;
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	for (uint8 state = CCM_ACTIVE; state <= CCM_ABORTED; state++)
		for (uint8 effect = CLUSTER_SAVEPOINT_EFFECT_LIVE;
			 effect <= CLUSTER_SAVEPOINT_EFFECT_DISCARDED; effect++) {
			p = ow_proof(&f, 0, state, effect);
			UT_ASSERT(ow_page(&f, &p, &page));
			UT_ASSERT(cluster_multixact_operation_wire_validate_reply(
				&page, 8192, 5, 9, &f.plans[0].request, &result, &out, 1, &count, &updater, &cap));
			UT_ASSERT_EQ(result, CMX_RESOLVE_OK);
			UT_ASSERT_EQ(count, 1);
			UT_ASSERT_EQ(cap, 313);
			UT_ASSERT_EQ(memcmp(&p, &out, sizeof(p)), 0);
		}
}

UT_TEST(top_only_format_two_round_trip)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationReplyPageV6 page;
	ClusterCurrentMemberOperationProof p, out;
	ClusterCurrentUpdaterProof updater;
	ClusterMxResolveResult result;
	uint16 count;
	uint32 cap;
	ow_init(&f, 1);
	memset(f.operations, 0, sizeof(f.operations));
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	for (uint8 state = CCM_ACTIVE; state <= CCM_ABORTED; state++) {
		p = ow_proof(&f, 0, state, 0);
		UT_ASSERT(ow_page(&f, &p, &page));
		UT_ASSERT(cluster_multixact_operation_wire_validate_reply(
			&page, 8192, 5, 9, &f.plans[0].request, &result, &out, 1, &count, &updater, &cap));
		UT_ASSERT_EQ(out.flags, 0);
		UT_ASSERT(ow_zero((char *)&out + 48, 33));
	}
}

UT_TEST(updater_reply_echo_and_member_proof)
{
	OperationWireFixture f;
	ClusterCurrentUpdaterChallenge challenge;
	ClusterCurrentUpdaterProof updater = { 0 }, decoded;
	ClusterCurrentMxOperationReplyPageV6 page;
	ClusterCurrentMemberOperationProof p, out;
	ClusterMxResolveResult result;
	uint16 count;
	uint32 cap;
	ow_init(&f, 1);
	f.members[0].member_status = MultiXactStatusNoKeyUpdate;
	test_updater_challenge(&challenge, 5, 100, 20);
	if (!ow_build(&f, 1, &challenge)) {
		UT_ASSERT(false);
		return;
	}
	p = ow_proof(&f, 0, CCM_COMMITTED, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	updater.mxkey = f.key;
	updater.candidate_next_xmin_alias = challenge.candidate_next_xmin_alias;
	updater.candidate_next_xmin_locator = challenge.candidate_next_xmin_locator;
	updater.updater_xid = 100;
	updater.candidate_next_xmin_locator.tt_wrap = 3;
	updater.verdict = CUCP_MATCH;
	UT_ASSERT(cluster_multixact_operation_wire_encode_reply(
		&f.plans[0].request, 5, 9, CMX_RESOLVE_OK, &p, 1, &updater, 313, &page));
	UT_ASSERT_EQ(page.header.wire_length, 224);
	UT_ASSERT_EQ(ow_value(page.body, 88, 2), 2);
	UT_ASSERT(cluster_multixact_operation_wire_validate_reply(
		&page, 8192, 5, 9, &f.plans[0].request, &result, &out, 1, &count, &decoded, &cap));
	UT_ASSERT_EQ(decoded.updater_xid, 100);
	page.body[88 + 64] ^= 1;
	ow_bad_reply(&f, &page, 8192);
}

UT_TEST(reply_header_bindings_and_typed_result_rejected)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationReplyPageV6 good, bad;
	ClusterCurrentMemberOperationProof p;
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	p = ow_proof(&f, 0, CCM_ACTIVE, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	UT_ASSERT(ow_page(&f, &p, &good));
	for (unsigned c = 0; c < 11; c++) {
		bad = good;
		switch (c) {
		case 0:
			bad.header.version = 5;
			break;
		case 1:
			bad.header.source_node_id++;
			break;
		case 2:
			bad.header.request_id++;
			break;
		case 3:
			bad.header.mxkey.cluster_epoch++;
			break;
		case 4:
			bad.header.descriptor_hash++;
			break;
		case 5:
			bad.header.total_count++;
			break;
		case 6:
			bad.header.chunk_ordinal++;
			break;
		case 7:
			bad.header.requester_capability_generation = 0;
			break;
		case 8:
			bad.header.result = CMX_RESOLVE_TIMEOUT;
			break;
		case 9:
			bad.header.reserved16 = 1;
			break;
		case 10:
			bad.header.wire_length = 112;
			break;
		}
		ow_bad_reply(&f, &bad, 8192);
	}
}

UT_TEST(reply_operation_identity_and_generation_rejected)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationReplyPageV6 good, bad;
	ClusterCurrentMemberOperationProof p;
	const unsigned offsets[] = { 0, 4, 8, 12, 20, 22, 32, 34, 36, 38, 48, 64, 72, 80, 81, 82, 84 };
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	p = ow_proof(&f, 0, CCM_ACTIVE, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	UT_ASSERT(ow_page(&f, &p, &good));
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		bad = good;
		((uint8 *)&bad)[64 + offsets[i]] ^= 0x80;
		ow_bad_reply(&f, &bad, 8192);
	}
	bad = good;
	memset(bad.body + 16, 0xff, 4);
	ow_bad_reply(&f, &bad, 8192);
}

UT_TEST(reply_padding_and_late_entry_failure_clear_whole_batch)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationReplyPageV6 good, bad;
	ClusterCurrentMemberOperationProof p[2];
	const unsigned offsets[] = { 44, 88 + 44, 88 + 80, 176, 616, 8127 };
	ow_init(&f, 2);
	if (!ow_build(&f, 2, NULL)) {
		UT_ASSERT(false);
		return;
	}
	for (unsigned i = 0; i < 2; i++)
		p[i] = ow_proof(&f, i, CCM_ACTIVE, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	UT_ASSERT(ow_page(&f, p, &good));
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		bad = good;
		((uint8 *)&bad)[64 + offsets[i]] ^= 0x80;
		ow_bad_reply(&f, &bad, 8192);
	}
}

UT_TEST(typed_negative_replies_are_valid_with_no_outputs)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationReplyPageV6 page;
	ClusterCurrentMemberOperationProof out;
	ClusterCurrentUpdaterProof updater;
	ClusterMxResolveResult result;
	uint16 count;
	uint32 cap;
	const ClusterMxResolveResult negatives[] = { CMX_RESOLVE_DENIED, CMX_RESOLVE_SUPPORTED_LIMIT,
												 CMX_RESOLVE_UNKNOWN, CMX_RESOLVE_RETRY };
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	for (unsigned i = 0; i < lengthof(negatives); i++) {
		UT_ASSERT(cluster_multixact_operation_wire_encode_reply(
			&f.plans[0].request, 5, 9, negatives[i], NULL, 0, NULL, 0, &page));
		UT_ASSERT_EQ(page.header.wire_length, 64);
		UT_ASSERT(ow_zero((char *)&page + 64, 8128));
		UT_ASSERT(cluster_multixact_operation_wire_validate_reply(
			&page, 8192, 5, 9, &f.plans[0].request, &result, &out, 1, &count, &updater, &cap));
		UT_ASSERT_EQ(result, negatives[i]);
		UT_ASSERT_EQ(count, 0);
		UT_ASSERT_EQ(cap, 0);
		page.reserved[0] = 1;
		ow_bad_reply(&f, &page, 8192);
	}
}

UT_TEST(encoder_invalid_proofs_clear_entire_output)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationReplyPageV6 page;
	ClusterCurrentMemberOperationProof p;
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	p = ow_proof(&f, 0, CCM_ACTIVE, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	UT_ASSERT(ow_page(&f, &p, &page));
	for (unsigned c = 0; c < 6; c++) {
		p = ow_proof(&f, 0, CCM_ACTIVE, CLUSTER_SAVEPOINT_EFFECT_LIVE);
		switch (c) {
		case 0:
			p.base.state = CCM_SELF;
			break;
		case 1:
			p.base.state = CCM_UNKNOWN;
			break;
		case 2:
			ClusterCurrentMemberProofSetCtrcGrant(&p.base, 0);
			break;
		case 3:
			p.base.commit_scn = scn_encode(0, 900);
			break;
		case 4:
			p.flags = 0;
			break;
		case 5:
			p.effect_state = 0;
			break;
		}
		memset(&page, 0xa5, sizeof(page));
		UT_ASSERT(!ow_page(&f, &p, &page));
		UT_ASSERT(ow_zero(&page, sizeof(page)));
	}
}

UT_TEST(top_only_active_cannot_smuggle_parent_key)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationReplyPageV6 page;
	ClusterCurrentMemberOperationProof p;
	ow_init(&f, 1);
	memset(f.operations, 0, sizeof(f.operations));
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	p = ow_proof(&f, 0, CCM_ACTIVE, 0);
	UT_ASSERT(ow_page(&f, &p, &page));
	p.base.key.local_xid = 97;
	UT_ASSERT(!ow_page(&f, &p, &page));
	UT_ASSERT(ow_zero(&page, sizeof(page)));
}

UT_TEST(reply_unaligned_length_capacity_and_old_family_separation)
{
	OperationWireFixture f;
	uint8 unaligned[8193];
	ClusterCurrentMxOperationReplyPageV6 page;
	ClusterCurrentMemberOperationProof p, out;
	ClusterCurrentMemberProof oldout;
	ClusterCurrentUpdaterProof updater;
	ClusterMxResolveResult result;
	ClusterCurrentMxProofRequestPlan oldplan[1];
	uint16 count;
	uint32 cap;
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	p = ow_proof(&f, 0, CCM_ACTIVE, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	UT_ASSERT(ow_page(&f, &p, &page));
	memcpy(unaligned + 1, &page, 8192);
	UT_ASSERT(cluster_multixact_operation_wire_validate_reply(
		unaligned + 1, 8192, 5, 9, &f.plans[0].request, &result, &out, 1, &count, &updater, &cap));
	ow_bad_reply(&f, unaligned + 1, 8191);
	UT_ASSERT(!cluster_multixact_operation_wire_validate_reply(
		&page, 8192, 5, 9, &f.plans[0].request, &result, &out, 0, &count, &updater, &cap));
	UT_ASSERT_EQ(count, 0);
	UT_ASSERT_EQ(cluster_multixact_current_wire_build_proof_requests(
					 &f.key, f.members, f.origins, 1,
					 cluster_multixact_current_descriptor_hash(&f.key, f.members, 1), NULL, 801, 9,
					 3, 7, oldplan, 1, &count),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, 8192, 5, 9, &oldplan[0].request, &oldout, 1, &count, &updater, &cap),
				 CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(CLUSTER_CURRENT_MX_WIRE_VERSION, 5);
}

UT_TEST(seven_proofs_round_trip_without_padding_leak)
{
	OperationWireFixture f;
	ClusterCurrentMemberOperationProof p[7], out[7];
	ClusterCurrentMxOperationReplyPageV6 page;
	ClusterCurrentUpdaterProof updater;
	ClusterMxResolveResult result;
	uint16 count;
	uint32 cap;
	ow_init(&f, 7);
	if (!ow_build(&f, 7, NULL)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(f.count, 1);
	for (unsigned i = 0; i < 7; i++) {
		p[i] = ow_proof(&f, i, CCM_ACTIVE,
						i % 2 ? CLUSTER_SAVEPOINT_EFFECT_DISCARDED : CLUSTER_SAVEPOINT_EFFECT_LIVE);
		memset((char *)&p[i] + 44, 0xa5, 4);
	}
	UT_ASSERT(ow_page(&f, p, &page));
	UT_ASSERT_EQ(page.header.wire_length, 680);
	UT_ASSERT(ow_zero(page.reserved, sizeof(page.reserved)));
	UT_ASSERT(cluster_multixact_operation_wire_validate_reply(
		&page, 8192, 5, 9, &f.plans[0].request, &result, out, 7, &count, &updater, &cap));
	UT_ASSERT_EQ(count, 7);
	for (unsigned i = 0; i < 7; i++) {
		UT_ASSERT(ow_zero(page.body + i * 88 + 44, 4));
		memset((char *)&p[i] + 44, 0, 4);
		UT_ASSERT_EQ(memcmp(&p[i], &out[i], sizeof(p[i])), 0);
	}
}

UT_TEST(top_only_extra_context_and_invalid_wrap_are_refused)
{
	OperationWireFixture f;
	ClusterCurrentMxOperationForwardV6 bad, out;
	ClusterCurrentMxOperationAsk ask;
	ow_init(&f, 1);
	if (!ow_build(&f, 1, NULL)) {
		UT_ASSERT(false);
		return;
	}
	bad = f.plans[0].request;
	memset(bad.body + 28, 0xff, 2);
	UT_ASSERT(!cluster_multixact_operation_wire_validate_forward(&bad, 768, 3, 5, 9, &out));
	bad = f.plans[0].request;
	bad.body[7] = 0;
	UT_ASSERT(!cluster_multixact_operation_wire_validate_forward(&bad, 768, 3, 5, 9, &out));
	memset(&ask, 0xa5, sizeof(ask));
	UT_ASSERT(!cluster_multixact_operation_wire_get_ask(&bad, 0, &ask));
	UT_ASSERT(ow_zero(&ask, sizeof(ask)));
}

int
main(void)
{
	UT_PLAN(23);
	UT_RUN(request_exact_literal_layout);
	UT_RUN(request_top_only_has_zero_context);
	UT_RUN(updater_request_carries_challenge_and_exact_entry);
	UT_RUN(batch_256_preserves_grouping_order_and_seven_limit);
	UT_RUN(planner_failures_clear_all_plans);
	UT_RUN(incomplete_operation_never_downgrades);
	UT_RUN(forward_unaligned_exact_length_and_old_version_rejected);
	UT_RUN(forward_context_and_padding_tampering_rejected);
	UT_RUN(forward_envelope_and_chunk_bindings_rejected);
	UT_RUN(duplicate_ordinal_or_raw_member_rejected);
	UT_RUN(reply_exact_literal_layout);
	UT_RUN(top_state_and_operation_disposition_round_trip_independently);
	UT_RUN(top_only_format_two_round_trip);
	UT_RUN(updater_reply_echo_and_member_proof);
	UT_RUN(reply_header_bindings_and_typed_result_rejected);
	UT_RUN(reply_operation_identity_and_generation_rejected);
	UT_RUN(reply_padding_and_late_entry_failure_clear_whole_batch);
	UT_RUN(typed_negative_replies_are_valid_with_no_outputs);
	UT_RUN(encoder_invalid_proofs_clear_entire_output);
	UT_RUN(top_only_active_cannot_smuggle_parent_key);
	UT_RUN(reply_unaligned_length_capacity_and_old_family_separation);
	UT_RUN(seven_proofs_round_trip_without_padding_leak);
	UT_RUN(top_only_extra_context_and_invalid_wrap_are_refused);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
