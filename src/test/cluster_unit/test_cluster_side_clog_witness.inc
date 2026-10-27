/*-------------------------------------------------------------------------
 *
 * test_cluster_side_clog_witness.inc
 *    Original bound CLOG carrier ownership and exact correspondence.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_side_clog_witness.inc
 * NOTES
 *    Original SIDE plan/route/decoder execute; no native I/O or lifetime grant.
 *-------------------------------------------------------------------------
 */
static void
clog_witness_frame(FakeXactRecord *fake, unsigned kind)
{
	int page = 97;
	xl_clog_truncate truncate = {97, 3178496, 42};

	make_projection_record(fake, RM_CLOG_ID,
		kind == 0 ? CLOG_ZEROPAGE_FULL : kind == 1 ? CLOG_ZEROPAGE : CLOG_TRUNCATE,
		kind == 0 ? (const void *)&side_full_birth : kind == 1 ? (const void *)&page : (const void *)&truncate,
		kind == 0 ? sizeof(side_full_birth) : kind == 1 ? sizeof(page) : sizeof(truncate));
	fake->u.decoded.max_block_id = -1;
	fake->u.decoded.header.xl_xid = 120;
	fake->u.decoded.toplevel_xid = 100;
	fake->reader.cluster_expected_thread_id = 3;
	fake->reader.seg.ws_tli = 7;
	fake->u.decoded.header.xl_prev = 80;
	fake->u.decoded.header.xl_tot_len = SizeOfXLogRecord + 2 + fake->u.decoded.main_data_len;
}

static RfPageProofDetailV1
clog_witness_feed(RfSideOnlinePlanV1 *plan, FakeXactRecord *fake,
	const ClusterWalSourceRef *source, XLogRecPtr begin, XLogRecPtr end)
{
	uint8 uuid[16];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;

	memcpy(uuid, source->claim.identity.storage_uuid, sizeof(uuid));
	identity = make_identity(fake, uuid);
	set_identity_range(fake, &identity, begin, end);
	record = make_projection_record_plan(fake);
	return rf_side_online_plan_feed_record_v1(plan, &record, &identity);
}

static void
clog_witness_copy(FakeXactRecord *to, const FakeXactRecord *from)
{
	*to = *from;
	to->reader.record = &to->u.decoded;
	to->u.decoded.main_data = (char *)to->data;
}

UT_TEST(test_clog_witness_keeps_complete_native_bytes_after_reader_reuse)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *plan = terminal_witness_plan(&source, 0);
		FakeXactRecord fake, original;
		RfSideOnlineOperationV1 operation;

		UT_ASSERT_EQ(rf_side_online_plan_native_clog_count_v1(plan, &source), UINT32_MAX);
		clog_witness_frame(&fake, kind);
		UT_ASSERT_EQ(clog_witness_feed(plan, &fake, &source, 100, 512), 0);
		clog_witness_copy(&original, &fake);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), 0);
		memset(fake.data, 0x5a, sizeof(fake.data));
		memset(&fake.u.decoded.header, 0, sizeof(fake.u.decoded.header));
		UT_ASSERT_EQ(rf_side_online_plan_native_clog_count_v1(plan, &source), 1);
		UT_ASSERT(rf_side_online_plan_clog_matches_v1(plan, &source, &original.reader));
		UT_ASSERT_EQ(rf_side_online_plan_native_terminal_count_v1(plan, &source), 0);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
		UT_ASSERT_EQ(operation.owned_payload_length, kind == 0 ? 16 : 0);
		UT_ASSERT_EQ(operation.source_xid, 120);
		UT_ASSERT_EQ(operation.source_top_xid, 100);
		rf_side_online_plan_destroy_v1(&plan);
		UT_ASSERT(plan == NULL);
	}
}

UT_TEST(test_clog_witness_refuses_changed_full_header_payload_and_source)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *plan = terminal_witness_plan(&source, 0);
		FakeXactRecord original;

		clog_witness_frame(&original, kind);
		UT_ASSERT_EQ(clog_witness_feed(plan, &original, &source, 100, 512), 0);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), 0);
		for (unsigned fault = 0; fault < 23; fault++) {
			FakeXactRecord query;
			ClusterWalSourceRef other = source;
			clog_witness_copy(&query, &original);
			switch (fault) {
			case 0: query.data[0] ^= 1; break;
			case 1: query.u.decoded.header.xl_prev++; break;
			case 2: query.u.decoded.header.xl_tot_len++; break;
			case 3: query.u.decoded.header.xl_crc++; break;
			case 4: query.u.decoded.header.xl_xid++; break;
			case 5: query.u.decoded.header.xl_info ^= 0x20; break;
			case 6: query.u.decoded.header.xl_rmid = RM_XACT_ID; break;
			case 7: query.u.decoded.main_data_len--; break;
			case 8: query.u.decoded.main_data = NULL; break;
			case 9: query.u.decoded.toplevel_xid++; break;
			case 10: query.u.decoded.record_origin = 1; break;
			case 11: query.u.decoded.max_block_id = 0; break;
			case 12: query.u.decoded.has_page_version_edge = true; break;
			case 13: query.reader.cluster_record_crc_failed = true; break;
			case 14: query.reader.system_identifier++; break;
			case 15: query.reader.cluster_expected_thread_id++; break;
			case 16: query.reader.seg.ws_tli++; break;
			case 17: query.reader.ReadRecPtr++; break;
			case 18: query.reader.EndRecPtr++; break;
			case 19: other.claim.claim_sha256[31] ^= 1; break;
			case 20: other.claim.identity.origin_owner_incarnation++; break;
			case 21: other.claim.identity.root_lineage_seq++; break;
			case 22: query.data[XLogRecGetDataLen(&query.reader) - 1] ^= 1; break;
			}
			UT_ASSERT(!rf_side_online_plan_clog_matches_v1(plan, &other, &query.reader));
		}
		UT_ASSERT(rf_side_online_plan_clog_matches_v1(plan, &source, &original.reader));
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_clog_witness_requires_original_physical_recovery_binding)
{
	for (unsigned shape = 0; shape < 4; shape++) {
		ClusterWalSourceRef source, other;
		RfSideOnlinePlanV1 *plan = terminal_witness_plan_shape(&source, 0, shape);
		FakeXactRecord original;

		clog_witness_frame(&original, 0);
		UT_ASSERT_EQ(clog_witness_feed(plan, &original, &source, 100, 512), 0);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), 0);
		UT_ASSERT_EQ(rf_side_online_plan_native_clog_count_v1(plan, &source), shape == 3 ? UINT32_MAX : 1);
		UT_ASSERT_EQ(rf_side_online_plan_clog_matches_v1(plan, &source, &original.reader), shape != 3);
		other = source; other.claim.identity.origin_owner_incarnation++;
		other.claim.identity.root_lineage_seq++; other.claim.claim_sha256[31] ^= 1;
		if (shape == 2) { other.claim.identity.origin_thread_id = 2; other.claim.identity.origin_node_id = 1; }
		UT_ASSERT_EQ(rf_side_online_plan_native_clog_count_v1(plan, &other), UINT32_MAX);
		UT_ASSERT(!rf_side_online_plan_clog_matches_v1(plan, &other, &original.reader));
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_clog_witness_charges_original_budget_and_counts_complete_stream)
{
	Size used = 0;
	for (unsigned attempt = 0; attempt < 3; attempt++) {
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *plan = terminal_witness_plan(&source, attempt == 0 ? 0 : used - (attempt == 1));
		FakeXactRecord original;

		clog_witness_frame(&original, 0);
		UT_ASSERT_EQ(clog_witness_feed(plan, &original, &source, 100, 512),
			attempt == 1 ? RF_PAGE_PROOF_DETAIL_CAPACITY : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan) == 0, attempt != 1);
		UT_ASSERT_EQ(rf_side_online_plan_native_clog_count_v1(plan, &source), attempt == 1 ? UINT32_MAX : 1);
		if (attempt == 0) used = RF_SIDE_ONLINE_PLAN_MAX_BYTES - rf_side_online_plan_scratch_available_v1(plan);
		rf_side_online_plan_destroy_v1(&plan);
	}
	{
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *plan = terminal_witness_plan(&source, 0);
		FakeXactRecord original[3];
		for (unsigned kind = 0; kind < 3; kind++) {
			clog_witness_frame(&original[kind], kind);
			UT_ASSERT_EQ(clog_witness_feed(plan, &original[kind], &source,
				100 + kind * 100, kind == 2 ? 512 : 200 + kind * 100), 0);
		}
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), 0);
		UT_ASSERT_EQ(rf_side_online_plan_native_clog_count_v1(plan, &source), 3);
		for (unsigned kind = 0; kind < 3; kind++)
			UT_ASSERT(rf_side_online_plan_clog_matches_v1(plan, &source, &original[kind].reader));
		rf_side_online_plan_destroy_v1(&plan);
		plan = terminal_witness_plan(&source, 0);
		{
			FakeXactRecord terminal;
			RfPageOnlineRecordIdentityV1 identity;
			RfDetachedRecordPlanV1 record;
			cold_terminal_frame(&terminal, false, false, -1);
			identity = make_identity(&terminal, source.claim.identity.storage_uuid);
			set_identity_range(&terminal, &identity, 100, 512);
			record = make_record_plan(&terminal);
			UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity), 0);
		}
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), 0);
		UT_ASSERT_EQ(rf_side_online_plan_native_clog_count_v1(plan, &source), 0);
		UT_ASSERT(!rf_side_online_plan_clog_matches_v1(plan, &source, &original[0].reader));
		rf_side_online_plan_destroy_v1(&plan);
	}
}
