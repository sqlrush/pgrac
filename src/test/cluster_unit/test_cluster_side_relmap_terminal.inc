/*-------------------------------------------------------------------------
 *
 * test_cluster_side_relmap_terminal.inc
 *	  Original sealed native terminal witness tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_side_relmap_terminal.inc
 *
 * NOTES
 *	  These pgrac-original cases check carrier bytes and original plan
 *	  ownership without native era or completion qualification.
 *
 *-------------------------------------------------------------------------
 */
static RfSideOnlinePlanV1 *
terminal_witness_plan_shape(ClusterWalSourceRef *source, Size budget, unsigned shape)
{
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut = { 0 };
	RfSideOnlinePlanRequestV1 request = { 0 };
	ClusterWalSourceRef sources[2];
	XLogRecPtr redo = 100;

	memset(source, 0, sizeof(*source));
	source->claim.identity.system_identifier = UINT64_C(0x11223344);
	memset(source->claim.identity.storage_uuid, 0x44, 16);
	memset(source->claim.identity.authority_uuid, 0x55, 16);
	source->claim.identity.origin_thread_id = 3;
	source->claim.identity.origin_node_id = 2;
	source->claim.identity.origin_owner_incarnation = 19;
	source->claim.identity.thread_claim_created_at = 1;
	source->claim.identity.root_lineage_seq = 1;
	source->claim.database_incarnation = 42;
	source->claim.max_config_generation = 4;
	memset(source->claim.claim_sha256, 0x66, 32);
	source->timeline = 7;
	cut.failed_thread = 3;
	cut.origin_owner_incarnation = 19;
	cut.timeline_id = 7;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 512;
	request.system_identifier = source->claim.identity.system_identifier;
	memcpy(request.storage_uuid, source->claim.identity.storage_uuid, 16);
	request.physical_cuts = &cut;
	request.participant_count = 1;
	request.redo_starts = &redo;
	request.memory_budget = budget;
	request.purpose = shape == 3 ? RF_SIDE_PLAN_PI_CONTRIBUTIONS : RF_SIDE_PLAN_RECOVERY;
	if (shape == 3)
		request.redo_starts = NULL;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	sources[0] = *source;
	sources[1] = *source;
	sources[1].claim.identity.origin_owner_incarnation++;
	sources[1].claim.identity.root_lineage_seq++;
	sources[1].claim.claim_sha256[31] ^= 1;
	if (shape == 2) {
		sources[1].claim.identity.origin_node_id = 1;
		sources[1].claim.identity.origin_thread_id = 2;
	}
	if (shape != 3)
		UT_ASSERT(rf_side_online_plan_bind_sources_v1(plan, sources, shape == 0 ? 1 : 2));
	return plan;
}

static RfSideOnlinePlanV1 *
terminal_witness_plan(ClusterWalSourceRef *source, Size budget)
{
	return terminal_witness_plan_shape(source, budget, 0);
}

UT_TEST(test_native_terminal_witness_owns_exact_original_header_and_payload)
{
	for (unsigned aborting = 0; aborting < 2; aborting++) {
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *plan = terminal_witness_plan(&source, 0);
		FakeXactRecord fake, original;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlineOperationV1 operation;

		if (aborting) {
			undo_header_abort(&fake, 4);
			undo_header_feed(plan, &fake, 100);
		}
		cold_terminal_frame(&fake, aborting, false, -1);
		fake.reader.cluster_expected_thread_id = 3;
		fake.reader.seg.ws_tli = 7;
		fake.u.decoded.header.xl_tot_len = SizeOfXLogRecord + 2 + fake.u.decoded.main_data_len;
		identity = make_identity(&fake, source.claim.identity.storage_uuid);
		set_identity_range(&fake, &identity, aborting ? 200 : 100, 512);
		record = make_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		original = fake;
		original.reader.record = &original.u.decoded;
		original.u.decoded.main_data = (char *)original.data;
		UT_ASSERT_EQ(rf_side_online_plan_native_terminal_count_v1(plan, &source), UINT32_MAX);
		UT_ASSERT(!rf_side_online_plan_terminal_matches_v1(plan, &source, &original.reader));
		/* Borrowed decoder bytes are reused before sealing. */
		memset(fake.data, 0xa5, sizeof(fake.data));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_terminal_matches_v1(plan, &source, &original.reader));
		UT_ASSERT_EQ(rf_side_online_plan_native_terminal_count_v1(plan, &source), 1);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, aborting ? 1 : 0, &operation));
		UT_ASSERT_EQ(operation.owned_payload_length, 0);
		UT_ASSERT(operation.owned_payload == NULL);
		for (unsigned fault = 0; fault < 18; fault++) {
			FakeXactRecord query = original;
			ClusterWalSourceRef other = source;
			query.reader.record = &query.u.decoded;
			query.u.decoded.main_data = (char *)query.data;
			switch (fault) {
			case 0:
				query.data[0] ^= 1;
				break; /* Timestamp, not merely decoded TT fields. */
			case 1:
				query.u.decoded.header.xl_prev ^= 1;
				break;
			case 2:
				query.u.decoded.header.xl_crc ^= 1;
				break;
			case 3:
				query.u.decoded.header.xl_xid++;
				break;
			case 4:
				query.reader.cluster_expected_thread_id++;
				break;
			case 5:
				query.reader.seg.ws_tli++;
				break;
			case 6:
				query.reader.ReadRecPtr++;
				break;
			case 7:
				query.reader.EndRecPtr++;
				break;
			case 8:
				query.u.decoded.main_data_len--;
				break;
			case 9:
				query.reader.cluster_record_crc_failed = true;
				break;
			case 10:
				other.claim.claim_sha256[31] ^= 1;
				break;
			case 11:
				other.claim.identity.origin_owner_incarnation++;
				break;
			case 12:
				query.u.decoded.record_origin = 1;
				break;
			case 13:
				query.u.decoded.toplevel_xid = 802;
				break;
			case 14:
				query.u.decoded.max_block_id = 0;
				break;
			case 15:
				query.u.decoded.has_page_version_edge = true;
				break;
			case 16:
				query.u.decoded.lsn++;
				break;
			case 17:
				query.u.decoded.next_lsn++;
				break;
			}
			UT_ASSERT(!rf_side_online_plan_terminal_matches_v1(plan, &other, &query.reader));
			UT_ASSERT_EQ(rf_side_online_plan_native_terminal_count_v1(plan, &other),
						 fault == 10 || fault == 11 ? UINT32_MAX : 1);
		}
		UT_ASSERT(rf_side_online_plan_terminal_matches_v1(plan, &source, &original.reader));
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_native_terminal_witness_uses_original_bound_plan_budget)
{
	Size used = 0;
	for (unsigned attempt = 0; attempt < 3; attempt++) {
		ClusterWalSourceRef source;
		RfSideOnlinePlanV1 *plan
			= terminal_witness_plan(&source, attempt == 0 ? 0 : used - (attempt == 1));
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfPageProofDetailV1 detail;
		cold_terminal_frame(&fake, false, false, -1);
		identity = make_identity(&fake, source.claim.identity.storage_uuid);
		set_identity_range(&fake, &identity, 100, 512);
		record = make_record_plan(&fake);
		detail = rf_side_online_plan_feed_record_v1(plan, &record, &identity);
		UT_ASSERT_EQ(detail,
					 attempt == 1 ? RF_PAGE_PROOF_DETAIL_CAPACITY : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan) == RF_PAGE_PROOF_DETAIL_OK, attempt != 1);
		if (attempt == 0)
			used = RF_SIDE_ONLINE_PLAN_MAX_BYTES - rf_side_online_plan_scratch_available_v1(plan);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_native_terminal_witness_rejects_duplicate_physical_raw_terminal)
{
	ClusterWalSourceRef source;
	RfSideOnlinePlanV1 *plan = terminal_witness_plan(&source, 0);
	FakeXactRecord fake;
	for (unsigned i = 0; i < 2; i++) {
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		cold_terminal_frame(&fake, false, false, -1);
		fake.reader.cluster_expected_thread_id = 3;
		fake.reader.seg.ws_tli = 7;
		identity = make_identity(&fake, source.claim.identity.storage_uuid);
		set_identity_range(&fake, &identity, i == 0 ? 100 : 300, i == 0 ? 300 : 512);
		record = make_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	(void)rf_side_online_plan_seal_v1(plan);
	UT_ASSERT(!rf_side_online_plan_terminal_matches_v1(plan, &source, &fake.reader));
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_native_terminal_witness_rejects_unbound_plan)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(200);
	FakeXactRecord fake;
	ClusterWalSourceRef source = { 0 };

	cold_terminal_frame(&fake, false, false, -1);
	UT_ASSERT_EQ(space_commit_feed(plan, &fake, 100), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(!rf_side_online_plan_terminal_matches_v1(plan, &source, &fake.reader));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(!rf_side_online_plan_terminal_matches_v1(plan, &source, &fake.reader));
	rf_side_online_plan_destroy_v1(&plan);
}
