/* Strict legacy decoder boundaries. No range-wire or capability admission. */
int k_mx_g5_original_fixture_main(void);
#define main k_mx_g5_original_fixture_main
#include "test_cluster_multixact_current.c"
#undef main

static bool
k_all_zero(const void *data, Size bytes)
{
	const unsigned char *p = data;
	Size i;

	for (i = 0; i < bytes; i++)
		if (p[i] != 0)
			return false;
	return true;
}

static void
k_describe_page(ClusterCurrentMxDescribeReplyPage *page)
{
	ClusterCurrentMxKey key = test_mxkey();

	memset(page, 0, sizeof(*page));
	page->header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page->header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page->header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	page->header.result = CMX_DESC_OK;
	page->header.source_node_id = 2;
	page->header.request_id = 501;
	page->header.mxkey = key;
	page->header.total_count = page->header.entry_count = 2;
	page->header.wire_length = 80;
	test_member(&page->members[0], 100, MultiXactStatusForShare);
	test_member(&page->members[1], 101, MultiXactStatusNoKeyUpdate);
	page->header.descriptor_hash =
		cluster_multixact_current_descriptor_hash(&key, page->members, 2);
}

static void
k_check_describe(const ClusterCurrentMxDescribeReplyPage *page, uint32 bytes,
				 ClusterMxDescribeResult expected, uint32 expected_total)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc out[4];
	uint16 count = 99;
	uint32 total = 99;

	memset(out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
		page, bytes, 2, 9, 501, &key, out, lengthof(out), &count, &total), expected);
	UT_ASSERT_EQ(total, expected_total);
	if (expected == CMX_DESC_OK) {
		UT_ASSERT_EQ(count, 2);
		UT_ASSERT_EQ(out[0].xid, 100);
		UT_ASSERT_EQ(out[1].xid, 101);
		UT_ASSERT(k_all_zero(&out[2], 2 * sizeof(out[0])));
	} else {
		UT_ASSERT_EQ(count, 0);
		UT_ASSERT(k_all_zero(out, sizeof(out)));
	}
}

UT_TEST(k_describe_valid_control)
{
	ClusterCurrentMxDescribeReplyPage page;

	k_describe_page(&page);
	k_check_describe(&page, sizeof(page), CMX_DESC_OK, 2);
}

/* Removing exact version/magic checks must never admit a different domain. */
UT_TEST(k_describe_rejects_other_domains)
{
	static const uint16 versions[] = {0, 4, 5, CLUSTER_CURRENT_MX_WIRE_VERSION + 1, UINT16_MAX};
	ClusterCurrentMxDescribeReplyPage page;
	Size i;

	for (i = 0; i < lengthof(versions); i++) {
		k_describe_page(&page);
		page.header.version = versions[i];
		k_check_describe(&page, sizeof(page), CMX_DESC_UNKNOWN, 0);
	}
	k_describe_page(&page);
	page.header.magic ^= 1;
	k_check_describe(&page, sizeof(page), CMX_DESC_UNKNOWN, 0);
}

UT_TEST(k_describe_requires_exact_lengths)
{
	static const uint32 lengths[] = {0, 63, BLCKSZ - 1, BLCKSZ + 1, UINT32_MAX};
	static const uint16 declared[] = {0, 64, 79, 81, BLCKSZ};
	ClusterCurrentMxDescribeReplyPage page;
	Size i;

	k_describe_page(&page);
	for (i = 0; i < lengthof(lengths); i++)
		k_check_describe(&page, lengths[i], CMX_DESC_UNKNOWN, 0);
	for (i = 0; i < lengthof(declared); i++) {
		k_describe_page(&page);
		page.header.wire_length = declared[i];
		k_check_describe(&page, sizeof(page), CMX_DESC_UNKNOWN, 0);
	}
}

UT_TEST(k_describe_binds_complete_lineage)
{
	ClusterCurrentMxDescribeReplyPage page;
	int fault;

	for (fault = 0; fault < 9; fault++) {
		k_describe_page(&page);
		switch (fault) {
		case 0: page.header.source_node_id = 3; break;
		case 1: page.header.request_id++; break;
		case 2: page.header.mxkey.cluster_epoch++; break;
		case 3: page.header.mxkey.origin_node_id++; break;
		case 4: page.header.mxkey.multixact_id++; break;
		case 5: page.header.mxkey.reserved16 = 1; break;
		case 6: page.header.mxkey.reserved32 = 1; break;
		case 7: page.header.descriptor_hash++; break;
		case 8: page.members[1].xid++; break;
		}
		k_check_describe(&page, sizeof(page), CMX_DESC_UNKNOWN, 0);
	}
}

UT_TEST(k_describe_never_accepts_a_partial_total)
{
	ClusterCurrentMxDescribeReplyPage page;
	int fault;

	for (fault = 0; fault < 4; fault++) {
		k_describe_page(&page);
		switch (fault) {
		case 0: page.header.total_count = 257; break;
		case 1: page.header.total_count = 65538; break;
		case 2: page.header.chunk_ordinal = 1; break;
		case 3: page.header.chunk_count_minus_one = 1; break;
		}
		k_check_describe(&page, sizeof(page), CMX_DESC_UNKNOWN, 0);
	}
	memset(&page, 0, sizeof(page));
	page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	page.header.result = CMX_DESC_SUPPORTED_LIMIT;
	page.header.source_node_id = 2;
	page.header.request_id = 501;
	page.header.mxkey = test_mxkey();
	page.header.total_count = 257;
	page.header.wire_length = 64;
	k_check_describe(&page, sizeof(page), CMX_DESC_SUPPORTED_LIMIT, 257);
}

UT_TEST(k_describe_rejects_reserved_or_trailing_data)
{
	ClusterCurrentMxDescribeReplyPage page;
	int fault;

	for (fault = 0; fault < 5; fault++) {
		k_describe_page(&page);
		switch (fault) {
		case 0: page.header.flags = 1; break;
		case 1: page.header.reserved16 = 1; break;
		case 2: page.header.reserved32 = 1; break;
		case 3: page.members[2].xid = 100; break;
		case 4: page.reserved[sizeof(page.reserved) - 1] = 1; break;
		}
		k_check_describe(&page, sizeof(page), CMX_DESC_UNKNOWN, 0);
	}
}

UT_TEST(k_proof_forward_rejects_other_domains_and_narrowing)
{
	ClusterCurrentMxProofForwardV2 request, decoded;
	int fault;

	test_proof_request(&request, CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS, 2);
	UT_ASSERT(cluster_multixact_current_wire_validate_proof_forward(
		&request, sizeof(request), 3, 5, 9, &decoded));
	for (fault = 0; fault < 10; fault++) {
		test_proof_request(&request, CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS, 2);
		switch (fault) {
		case 0: request.trailer.version = 4; break;
		case 1: request.trailer.version = 5; break;
		case 2: request.trailer.magic ^= 1; break;
		case 3: request.prefix.total_count = 257; break;
		case 4: request.prefix.total_count = 65544; break;
		case 5: request.trailer.body.asks[1].member_ordinal = 8; break;
		case 6: request.prefix.reserved_b[0] = 1; break;
		case 7: request.trailer.reserved16 = 1; break;
		case 8: request.prefix.chunk_ordinal = 3; break;
		case 9: request.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION + 1; break;
		}
		memset(&decoded, 0xa5, sizeof(decoded));
		UT_ASSERT(!cluster_multixact_current_wire_validate_proof_forward(
			&request, sizeof(request), 3, 5, 9, &decoded));
		UT_ASSERT(k_all_zero(&decoded, sizeof(decoded)));
	}
}

static void
k_proof_page(ClusterCurrentMxProofForwardV2 *request,
			 ClusterCurrentMxProofReplyPage *page)
{
	int i;

	test_proof_request(request, CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS, 2);
	memset(page, 0, sizeof(*page));
	page->header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page->header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page->header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	page->header.result = CMX_RESOLVE_OK;
	page->header.source_node_id = 5;
	page->header.request_id = 700;
	page->header.mxkey = test_mxkey();
	page->header.descriptor_hash = UINT64CONST(0x8877665544332211);
	page->header.total_count = 8;
	page->header.entry_count = 2;
	page->header.chunk_count_minus_one = 2;
	page->header.requester_capability_generation = 41;
	page->header.wire_length = sizeof(page->header)
		+ 2 * sizeof(ClusterCurrentMemberProof);
	for (i = 0; i < 2; i++) {
		ClusterCurrentMxMemberDesc member;

		test_member(&member, 100 + 3 * i, MultiXactStatusForShare);
		test_proof(&page->body.proofs[i], &member, i, CCM_ACTIVE, 5, 20 + i);
	}
}

static void
k_check_proof(const ClusterCurrentMxProofForwardV2 *request,
			  const ClusterCurrentMxProofReplyPage *page, uint32 bytes,
			  ClusterMxResolveResult expected)
{
	ClusterCurrentMemberProof proofs[3];
	ClusterCurrentUpdaterProof updater;
	uint16 count = 99;
	uint32 generation = 99;
	int i;

	memset(proofs, 0xa5, sizeof(proofs));
	memset(&updater, 0xa5, sizeof(updater));
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
		page, bytes, 5, 9, request, proofs, lengthof(proofs), &count,
		&updater, &generation), expected);
	if (expected == CMX_RESOLVE_OK) {
		UT_ASSERT_EQ(count, 2);
		UT_ASSERT_EQ(generation, 41);
		UT_ASSERT_EQ(proofs[0].member_xid, 100);
		UT_ASSERT_EQ(proofs[1].member_xid, 103);
		UT_ASSERT_EQ(proofs[1].member_ordinal, 1);
		UT_ASSERT_EQ(proofs[0].state, CCM_ACTIVE);
		UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&proofs[0]), 7);
	} else {
		UT_ASSERT_EQ(count, 0);
		UT_ASSERT_EQ(generation, 0);
		for (i = 0; i < 3; i++) {
			ClusterCurrentMemberProof empty;

			memset(&empty, 0, sizeof(empty));
			empty.state = CCM_UNKNOWN;
			UT_ASSERT_EQ(memcmp(&proofs[i], &empty, sizeof(empty)), 0);
		}
	}
	UT_ASSERT_EQ(updater.verdict, CUCP_UNKNOWN);
}

UT_TEST(k_proof_reply_valid_control)
{
	ClusterCurrentMxProofForwardV2 request;
	ClusterCurrentMxProofReplyPage page;

	k_proof_page(&request, &page);
	k_check_proof(&request, &page, sizeof(page), CMX_RESOLVE_OK);
}

UT_TEST(k_proof_reply_binds_domain_lengths_and_lineage)
{
	static const uint32 lengths[] = {0, 63, BLCKSZ - 1, BLCKSZ + 1, UINT32_MAX};
	ClusterCurrentMxProofForwardV2 request;
	ClusterCurrentMxProofReplyPage page;
	Size i;
	int fault;

	k_proof_page(&request, &page);
	for (i = 0; i < lengthof(lengths); i++)
		k_check_proof(&request, &page, lengths[i], CMX_RESOLVE_UNKNOWN);
	for (fault = 0; fault < 14; fault++) {
		k_proof_page(&request, &page);
		switch (fault) {
		case 0: page.header.version = 4; break;
		case 1: page.header.version = 5; break;
		case 2: page.header.magic ^= 1; break;
		case 3: page.header.source_node_id++; break;
		case 4: page.header.request_id++; break;
		case 5: page.header.mxkey.cluster_epoch++; break;
		case 6: page.header.mxkey.multixact_id++; break;
		case 7: page.header.descriptor_hash++; break;
		case 8: page.header.total_count = 65544; break;
		case 9: page.header.chunk_ordinal++; break;
		case 10: page.header.chunk_count_minus_one++; break;
		case 11: page.header.wire_length--; break;
		case 12: page.header.wire_length++; break;
		case 13: page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION + 1; break;
		}
		k_check_proof(&request, &page, sizeof(page), CMX_RESOLVE_UNKNOWN);
	}
}

UT_TEST(k_proof_reply_scrubs_whole_batch_on_bad_second_proof)
{
	ClusterCurrentMxProofForwardV2 request;
	ClusterCurrentMxProofReplyPage page;
	int fault;

	for (fault = 0; fault < 7; fault++) {
		k_proof_page(&request, &page);
		switch (fault) {
		case 0: page.body.proofs[1].state = CCM_UNKNOWN; break;
		case 1: ClusterCurrentMemberProofSetCtrcGrant(&page.body.proofs[1], 0); break;
		case 2: page.body.proofs[1].key.cluster_epoch++; break;
		case 3: page.body.proofs[1].member_ordinal = 0; break;
		case 4: page.body.proofs[1].member_xid++; break;
		case 5: page.header.requester_capability_generation = 0; break;
		case 6: page.body.proofs[2].member_xid = 106; break;
		}
		k_check_proof(&request, &page, sizeof(page), CMX_RESOLVE_UNKNOWN);
	}
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(k_describe_valid_control);
	UT_RUN(k_describe_rejects_other_domains);
	UT_RUN(k_describe_requires_exact_lengths);
	UT_RUN(k_describe_binds_complete_lineage);
	UT_RUN(k_describe_never_accepts_a_partial_total);
	UT_RUN(k_describe_rejects_reserved_or_trailing_data);
	UT_RUN(k_proof_forward_rejects_other_domains_and_narrowing);
	UT_RUN(k_proof_reply_valid_control);
	UT_RUN(k_proof_reply_binds_domain_lengths_and_lineage);
	UT_RUN(k_proof_reply_scrubs_whole_batch_on_bad_second_proof);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
