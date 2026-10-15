/* Live PI contribution planning shares the original decoders and boundaries.
 * Reuse the SIDE test environment without running its independent suite. */
int side_xact_fixture_main(void);
#define main side_xact_fixture_main
#include "test_cluster_side_xact.c"
#undef main
#include "pgstat.h"

/* Insert the native counted array before invalidations/SCN/TT/typed drops. */
static Size
pi_commit_add_stats(FakeXactRecord *fake, const xl_xact_stats_item *items, int count)
{
	uint32 xinfo;
	Size offset = MinSizeOfXactCommit + sizeof(xl_xact_xinfo);
	Size extra = sizeof(count) + count * sizeof(*items);

	memcpy(&xinfo, fake->data + MinSizeOfXactCommit, sizeof(xinfo));
	UT_ASSERT((xinfo & (XACT_XINFO_HAS_SUBXACTS | XACT_XINFO_HAS_DROPPED_STATS)) == 0);
	if ((xinfo & XACT_XINFO_HAS_DBINFO) != 0)
		offset += sizeof(xl_xact_dbinfo);
	if ((xinfo & XACT_XINFO_HAS_RELFILELOCATORS) != 0) {
		int nrels;
		memcpy(&nrels, fake->data + offset, sizeof(nrels));
		offset += sizeof(nrels) + nrels * sizeof(RelFileLocator);
	}
	UT_ASSERT(fake->u.decoded.main_data_len + extra <= sizeof(fake->data));
	memmove(fake->data + offset + extra, fake->data + offset,
			fake->u.decoded.main_data_len - offset);
	memcpy(fake->data + offset, &count, sizeof(count));
	memcpy(fake->data + offset + sizeof(count), items, count * sizeof(*items));
	xinfo |= XACT_XINFO_HAS_DROPPED_STATS;
	memcpy(fake->data + MinSizeOfXactCommit, &xinfo, sizeof(xinfo));
	fake->u.decoded.main_data_len += extra;
	return offset;
}

static void
pi_commit(FakeXactRecord *fake, uint32 flags, const SharedInvalidationMessage *messages, int count)
{
	xl_xact_dbinfo dbinfo = { 5, DEFAULTTABLESPACE_OID };
	uint32 xinfo;
	Size base = MinSizeOfXactCommit + sizeof(xl_xact_xinfo);
	Size extra = sizeof(dbinfo) + (count > 0 ? sizeof(count) + count * sizeof(*messages) : 0);
	uint8 *cursor;

	make_commit(fake, 801, 901, 123456, false);
	memcpy(&xinfo, fake->data + MinSizeOfXactCommit, sizeof(xinfo));
	xinfo |= flags | XACT_XINFO_HAS_DBINFO | (count > 0 ? XACT_XINFO_HAS_INVALS : 0);
	memmove(fake->data + base + extra, fake->data + base, fake->u.decoded.main_data_len - base);
	memcpy(fake->data + MinSizeOfXactCommit, &xinfo, sizeof(xinfo));
	cursor = fake->data + base;
	memcpy(cursor, &dbinfo, sizeof(dbinfo));
	cursor += sizeof(dbinfo);
	if (count > 0) {
		memcpy(cursor, &count, sizeof(count));
		memcpy(cursor + sizeof(count), messages, count * sizeof(*messages));
	}
	fake->u.decoded.main_data_len += extra;
}

static RfSideOnlinePlanV1 *
pi_side_plan(const RfPageOnlineRecordIdentityV1 *identity, bool inspect)
{
	RfSideOnlinePlanRequestV1 request = { 0 };
	RfContributorStreamCutV1 cut = { 0 };
	RfSideOnlinePlanV1 *plan = NULL;

	cut.failed_thread = identity->record.origin_thread;
	cut.timeline_id = identity->record.timeline_id;
	cut.origin_owner_incarnation = 1;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = identity->record.read_rec_ptr;
	cut.scan_end_exclusive = identity->record.end_rec_ptr;
	request.system_identifier = identity->record.system_identifier;
	memcpy(request.storage_uuid, identity->record.storage_uuid, 16);
	request.physical_cuts = &cut;
	request.participant_count = 1;
	request.purpose = inspect ? RF_SIDE_PLAN_PI_CONTRIBUTIONS : RF_SIDE_PLAN_RECOVERY;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static RfPageOnlineRecordIdentityV1
pi_identity(FakeXactRecord *fake)
{
	uint8 uuid[16];
	memset(uuid, 0x44, sizeof(uuid));
	/* These native SIDE fixtures contain no registered PAGE blocks. */
	fake->u.decoded.max_block_id = -1;
	return make_identity(fake, uuid);
}

UT_TEST(test_pi_commit_keeps_complete_flags_and_invalidation_payload)
{
	SharedInvalidationMessage messages[4] = { 0 };
	messages[0].cc.id = 0;
	messages[0].cc.dbId = 5;
	messages[0].cc.hashValue = 123;
	messages[1].sn.id = SHAREDINVALSNAPSHOT_ID;
	messages[1].sn.dbId = 5;
	messages[1].sn.relId = 2608;
	messages[2].rc.id = SHAREDINVALRELCACHE_ID;
	messages[2].rc.dbId = 5;
	messages[2].rc.relId = 16388;
	messages[3].cat.id = SHAREDINVALCATALOG_ID;
	messages[3].cat.dbId = 5;
	messages[3].cat.catId = 1259;
	for (int variant = 0; variant < 4; variant++) {
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlinePlanV1 *plan, *recovery;
		RfSideOnlineOperationV1 op;
		RfSideContributionOwnersV1 owners;
		RfPageProofDetailV1 detail;

		pi_commit(&fake, variant == 1 ? 0 : (XACT_COMPLETION_FORCE_SYNC_COMMIT
					| (variant == 3 ? XACT_XINFO_HAS_AE_LOCKS | XACT_COMPLETION_APPLY_FEEDBACK
					   | XACT_COMPLETION_UPDATE_RELCACHE_FILE : 0)),
				  messages, variant == 0 ? 0 : lengthof(messages));
		identity = pi_identity(&fake);
		record = make_record_plan(&fake);
		plan = pi_side_plan(&identity, true);
		detail = rf_side_online_plan_feed_record_v1(plan, &record, &identity);
		UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
		if (detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
			UT_ASSERT_EQ(op.owned_payload_length, fake.u.decoded.main_data_len);
			if (op.owned_payload_length == fake.u.decoded.main_data_len)
				UT_ASSERT(memcmp(op.owned_payload, fake.data, op.owned_payload_length) == 0);
			UT_ASSERT(rf_side_online_plan_contribution_owners_v1(plan, 0, &owners));
			UT_ASSERT((owners.owners & RF_SIDE_CONTRIBUTION_UNDO_HEADER) != 0);
			UT_ASSERT((owners.owners & RF_SIDE_CONTRIBUTION_TERMINAL) != 0);
			if (variant != 0) {
				UT_ASSERT_EQ(op.xact.invalidation_count, 4);
				UT_ASSERT_EQ(op.xact.invalidation_kinds, 15);
				UT_ASSERT((owners.owners & RF_SIDE_CONTRIBUTION_INVALIDATION) != 0);
			}
			UT_ASSERT_EQ(op.xact.xid, 801);
			UT_ASSERT_EQ(op.xact.terminal_scn, 901);
			UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&op.xact, op.owned_payload,
												   op.owned_payload_length), RF_SIDE_XACT_APPLY_BLOCKED);
		}
		recovery = pi_side_plan(&identity, false);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(recovery, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		rf_side_online_plan_destroy_v1(&recovery);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_pi_side_plan_rejects_execution_before_callbacks)
{
	FakeXactRecord fake;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;
	RfSideOnlinePlanV1 *plan;
	RfSideOnlineApplyOpsV1 ops = { 0 };
	ApplyCapture capture = { 0 };

	make_commit(&fake, 801, 901, 123456, false);
	identity = pi_identity(&fake);
	record = make_record_plan(&fake);
	plan = pi_side_plan(&identity, true);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	ops.arg = &capture;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	ops.preflight_xact = accept_preflight;
	ops.apply_xact = capture_apply;
	UT_ASSERT_NE(rf_side_online_plan_preflight_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_NE(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.begin_count, 0);
	UT_ASSERT_EQ(capture.count, 0);
	UT_ASSERT(!capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_pi_commit_unknown_or_damaged_effects_still_refuse)
{
	for (int variant = 0; variant < 7; variant++) {
		SharedInvalidationMessage msg = { 0 };
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlinePlanV1 *plan;
		uint32 word;

		msg.rc.id = SHAREDINVALRELCACHE_ID;
		msg.rc.dbId = 5;
		msg.rc.relId = 16388;
		if (variant == 0) msg.id = -99;
		pi_commit(&fake, XACT_COMPLETION_FORCE_SYNC_COMMIT, &msg, 1);
		if (variant == 1) fake.u.decoded.main_data_len--;
		if (variant == 2) fake.data[fake.u.decoded.main_data_len++] = 0;
		if (variant == 3) {
			memcpy(&word, fake.data + MinSizeOfXactCommit, sizeof(word));
			word |= UINT32_C(1) << 28;
			memcpy(fake.data + MinSizeOfXactCommit, &word, sizeof(word));
		}
		if (variant == 4) fake.u.decoded.header.xl_xid++;
		if (variant == 5) {
			word = UINT32_MAX;
			memcpy(fake.data + MinSizeOfXactCommit + sizeof(xl_xact_xinfo)
				   + sizeof(xl_xact_dbinfo), &word, sizeof(word));
		}
		identity = pi_identity(&fake);
		if (variant == 6) identity.record.origin_thread = 2;
		record = make_record_plan(&fake);
		plan = pi_side_plan(&identity, true);
		UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &record, &identity), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_NE(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_pi_native_create_requires_the_typed_identity_contribution)
{
	for (int missing = 0; missing < 2; missing++) {
		FakeXactRecord fake;
		xl_smgr_create native = { { DEFAULTTABLESPACE_OID, 5, 16384 }, MAIN_FORKNUM };
		ClusterSpaceReservationChange advance = space_advance_fixture();
		ClusterSpaceStructureChange create = { 0 };
		RfPageOnlineRecordIdentityV1 identity;
		RfSideOnlinePlanV1 *plan;
		RfSideSpaceTerminalV1 terminal;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

		make_projection_record(&fake, RM_SMGR_ID, XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE,
							   &native, sizeof(native));
		identity = pi_identity(&fake);
		identity.record.end_rec_ptr = missing ? 200 : 300;
		plan = pi_side_plan(&identity, true);
		UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_CREATE, &native, sizeof(native), 100, false),
					 RF_PAGE_PROOF_DETAIL_OK);
		create.identity.action = CLUSTER_SPACE_WAL_CREATE;
		create.identity.result = advance.result.identity;
		create.identity.result_token = 50;
		create.identity.nblocks = InvalidBlockNumber;
		create.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
		create.reservation.result.identity = create.identity.result;
		create.reservation.result_token = 50;
		if (!missing) {
			UT_ASSERT(cluster_space_structure_wal_encode(&create, wal, sizeof(wal)));
			UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_IDENTITY, wal, sizeof(wal), 200, false),
						 RF_PAGE_PROOF_DETAIL_OK);
		}
		UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
		if (missing)
			UT_ASSERT_NE(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		else {
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 2);
			UT_ASSERT(rf_side_online_plan_space_terminal_v1(plan, &create.identity.result.key, 0, &terminal));
			UT_ASSERT_EQ(terminal.operation, 1);
			UT_ASSERT_EQ(terminal.contribution.result_token[0], 50);
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_pi_ctrc_generation_zero_is_retained_without_apply_eligibility)
{
	FakeXactRecord fake;
	xl_undo_tt_slot_ctrc_release_v1 release = { 0 };
	uint8 wal[CLUSTER_UNDO_TT_CTRC_RELEASE_BYTES];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;
	RfSideOnlinePlanV1 *plan, *recovery;
	RfSideOnlineOperationV1 op;
	RfPageProofDetailV1 detail;

	release.segment_id = 513;
	release.segment_generation = 0;
	release.xid = 801;
	release.cluster_epoch = 1;
	release.root_id = release.root_generation = release.formation_epoch = 1;
	release.admission_record_generation = release.seal_generation = 1;
	release.touched_nodes_low = 4;
	release.slot_offset = 0;
	release.slot_wrap = 0;
	release.owner_instance = 3;
	release.terminal_status = TT_SLOT_COMMITTED;
	release.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
	release.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
	memset(release.ack_set_digest, 0x42, sizeof(release.ack_set_digest));
	UT_ASSERT(cluster_undo_tt_ctrc_release_encode(&release, wal));
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_CTRC_RELEASE, wal, sizeof(wal));
	identity = pi_identity(&fake);
	record = make_undo_record_plan(&fake);
	plan = pi_side_plan(&identity, true);
	detail = rf_side_online_plan_feed_record_v1(plan, &record, &identity);
	UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
	if (detail == RF_PAGE_PROOF_DETAIL_OK) {
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
		UT_ASSERT_EQ(op.undo.expected_generation, 0);
		UT_ASSERT_EQ(op.owned_payload_length, sizeof(wal));
		if (op.owned_payload_length == sizeof(wal))
			UT_ASSERT(memcmp(op.owned_payload, wal, sizeof(wal)) == 0);
	}
	recovery = pi_side_plan(&identity, false);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(recovery, &record, &identity),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	rf_side_online_plan_destroy_v1(&recovery);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_pi_source_cut_must_be_complete)
{
	FakeXactRecord fake;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;
	RfSideOnlinePlanV1 *plan;
	make_commit(&fake, 801, 901, 123456, false);
	identity = pi_identity(&fake);
	plan = pi_side_plan(&identity, true);
	set_identity_range(&fake, &identity, 100, 180);
	record = make_record_plan(&fake);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_pi_ctrc_other_fields_still_refuse)
{
	for (int variant = 0; variant < 10; variant++) {
		ClusterUndoDecoded d = { 0 };
		d.kind = CLUSTER_UNDO_KIND_TT_CTRC_RELEASE;
		d.opcode = XLOG_UNDO_TT_SLOT_CTRC_RELEASE;
		d.instance = 3;
		d.segment_id = 513;
		d.xid = 801;
		d.cluster_epoch = 1;
		d.root_id = d.root_generation = d.formation_epoch = 1;
		d.admission_record_generation = d.seal_generation = 1;
		d.touched_nodes_low = 4;
		d.terminal_status = TT_SLOT_COMMITTED;
		d.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
		d.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
		memset(d.ack_set_digest, 0x42, sizeof(d.ack_set_digest));
		UT_ASSERT(cluster_undo_inspect_pi(&d));
		UT_ASSERT(!cluster_undo_preflight(&d));
		switch (variant) {
		case 0: memset(d.ack_set_digest, 0, sizeof(d.ack_set_digest)); break;
		case 1: d.instance = 2; break;
		case 2: d.slot_offset = TT_SLOTS_PER_SEGMENT; break;
		case 3: d.wrap = TT_WRAP_INVALID; break;
		case 4: d.xid = InvalidTransactionId; break;
		case 5: d.root_id = 0; break;
		case 6: d.formation_epoch = 0; break;
		case 7: d.seal_generation = 0; break;
		case 8: d.flags = 0; break;
		case 9: d.expected_generation = UINT32_MAX; break;
		}
		UT_ASSERT(!cluster_undo_inspect_pi(&d));
		if (ut_current_failed) printf("# CTRC invalid field %d\n", variant);
	}
}

UT_TEST(test_pi_create_namespace_modifier_and_page_reuse_boundaries)
{
	for (int variant = 0; variant < 7; variant++) {
		FakeXactRecord fake;
		xl_smgr_create native = { { DEFAULTTABLESPACE_OID, 5, 16384 }, MAIN_FORKNUM };
		ClusterSpaceStructureChange create = { 0 };
		ClusterSpaceReservationChange advance = space_advance_fixture();
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlinePlanV1 *plan;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		uint8 info = XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE;
		RfPageProofDetailV1 fed;

		if (variant == 1) native.forkNum = VISIBILITYMAP_FORKNUM;
		if (variant == 2) native.rlocator.dbOid = InvalidOid;
		if (variant == 3) info |= XLR_CHECK_CONSISTENCY;
		make_projection_record(&fake, RM_SMGR_ID, info, &native, sizeof(native));
		identity = pi_identity(&fake);
		identity.record.end_rec_ptr = 300;
		plan = pi_side_plan(&identity, true);
		set_identity_range(&fake, &identity, 100, 200);
		record = make_projection_record_plan(&fake);
		fed = rf_side_online_plan_feed_record_v1(plan, &record, &identity);
		if (variant >= 1 && variant <= 3) {
			UT_ASSERT_NE(fed, RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_NE(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		} else {
			RfPageStableEdgeInputV1 edge = { 0 };
			RfContributorVectorV1 contributors = { 0 };
			RfPageOnlineTargetViewV1 target = { 0 };
			UT_ASSERT_EQ(fed, RF_PAGE_PROOF_DETAIL_OK);
			create.identity.action = CLUSTER_SPACE_WAL_CREATE;
			create.identity.result = advance.result.identity;
			create.identity.result_token = 50;
			create.identity.nblocks = InvalidBlockNumber;
			create.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
			create.reservation.result.identity = create.identity.result;
			create.reservation.result_token = 50;
			UT_ASSERT(cluster_space_structure_wal_encode(&create, wal, sizeof(wal)));
			UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_IDENTITY, wal, sizeof(wal), 200, false),
						 RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
			target.page_identity.system_identifier = identity.record.system_identifier;
			memcpy(target.page_identity.storage_uuid, identity.record.storage_uuid, 16);
			target.page_identity.locator = native.rlocator;
			target.page_identity.forknum = MAIN_FORKNUM;
			target.contributors = &contributors;
			contributors.edges = &edge;
			contributors.edge_count = 1;
			edge.page_identity = target.page_identity;
			edge.record_identity.read_rec_ptr = 300;
			edge.edge.before_kind = RF_PAGE_STATE_PRESENT;
			memcpy(edge.edge.result_incarnation, create.identity.result.incarnation, 16);
			memcpy(edge.edge.before.segment_incarnation, create.identity.result.incarnation, 16);
			if (variant == 4) edge.edge.result_incarnation[0]++;
			if (variant == 5) edge.record_identity.read_rec_ptr = 150;
			if (variant == 6) edge.edge.before.segment_incarnation[0]++;
			UT_ASSERT_EQ(rf_side_online_plan_page_lifecycle_v1(plan, &target), variant == 0);
			/* Another physical locator is independent, even if its relNumber matches. */
			target.page_identity.locator.dbOid++;
			UT_ASSERT(rf_side_online_plan_page_lifecycle_v1(plan, &target));
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_pi_commit_keeps_all_drop_locators_and_exact_namespace)
{
	for (int mismatch = 0; mismatch < 2; mismatch++) {
		FakeXactRecord fake;
		ClusterSpaceStructureChange drops[2] = { space_drop_fixture(16384), space_drop_fixture(16390) };
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlinePlanV1 *plan;
		RfSideOnlineOperationV1 op;
		if (mismatch) {
			drops[1].identity.result.key.storage_uuid[0]++;
			drops[1].identity.expected.key = drops[1].identity.result.key;
			drops[1].reservation.before.identity.key = drops[1].identity.result.key;
			drops[1].reservation.result.identity.key = drops[1].identity.result.key;
		}
		make_space_commit(&fake, drops, 2);
		identity = pi_identity(&fake);
		record = make_record_plan(&fake);
		plan = pi_side_plan(&identity, true);
		if (mismatch)
			UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &record, &identity), RF_PAGE_PROOF_DETAIL_OK);
		else {
			UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(rf_side_online_plan_space_contribution_count_v1(plan, 0), 2);
			UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
			UT_ASSERT_EQ(op.owned_payload_length, fake.u.decoded.main_data_len);
			UT_ASSERT(memcmp(op.owned_payload, fake.data, op.owned_payload_length) == 0);
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_pi_ko_commit_marker_preserves_owner_without_execution)
{
	for (int variant = 0; variant < 5; variant++) {
		SharedInvalidationMessage msg = { 0 };
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlinePlanV1 *plan, *recovery;
		RfSideOnlineOperationV1 op;
		RfSideContributionOwnersV1 owners;
		RfPageProofDetailV1 detail;

		msg.ko.id = SHAREDINVALKO_COMMIT_ID;
		msg.ko.version = SHAREDINVALKO_COMMIT_VERSION;
		msg.ko.batch_lo = 17;
		if (variant == 1) msg.ko.version++;
		if (variant == 2) msg.ko.flags = 1;
		if (variant == 3) msg.ko.reserved = 1;
		if (variant == 4) msg.ko.batch_lo = 0;
		pi_commit(&fake, XACT_COMPLETION_FORCE_SYNC_COMMIT, &msg, 1);
		identity = pi_identity(&fake);
		record = make_record_plan(&fake);
		plan = pi_side_plan(&identity, true);
		detail = rf_side_online_plan_feed_record_v1(plan, &record, &identity);
		if (variant != 0)
			UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		else {
			UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
			if (detail == RF_PAGE_PROOF_DETAIL_OK) {
				UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
				UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
				UT_ASSERT_EQ(op.xact.invalidation_count, 1);
				UT_ASSERT_EQ(op.owned_payload_length, fake.u.decoded.main_data_len);
				UT_ASSERT(memcmp(op.owned_payload, fake.data, op.owned_payload_length) == 0);
				UT_ASSERT(rf_side_online_plan_contribution_owners_v1(plan, 0, &owners));
				UT_ASSERT((owners.owners & RF_SIDE_CONTRIBUTION_INVALIDATION) != 0);
				UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&op.xact, op.owned_payload,
													   op.owned_payload_length), RF_SIDE_XACT_APPLY_BLOCKED);
			}
		}
		recovery = pi_side_plan(&identity, false);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(recovery, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		rf_side_online_plan_destroy_v1(&recovery);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_pi_relation_stats_keeps_native_owner_without_execution)
{
	for (int variant = 0; variant < 3; variant++) {
		/* Logical relation OIDs need not name any physical locator in the WAL. */
		xl_xact_stats_item items[2] = {
			{ PGSTAT_KIND_RELATION, 5, 76543 }, { PGSTAT_KIND_RELATION, InvalidOid, 1260 }
		};
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlinePlanV1 *plan, *recovery;
		RfSideOnlineOperationV1 op;
		RfSideContributionOwnersV1 owners;
		RfPageProofDetailV1 detail;
		RfSideOnlineApplyOpsV1 ops = { 0 };
		ApplyCapture capture = { 0 };

		if (variant == 2) {
			make_commit(&fake, 801, 901, 123456, false);
			items[0].dboid = InvalidOid;
		} else
			pi_commit(&fake, variant == 1 ? XACT_COMPLETION_FORCE_SYNC_COMMIT : 0, NULL, 0);
		pi_commit_add_stats(&fake, items, lengthof(items));
		identity = pi_identity(&fake);
		record = make_record_plan(&fake);
		plan = pi_side_plan(&identity, true);
		detail = rf_side_online_plan_feed_record_v1(plan, &record, &identity);
		UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
		if (detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
			UT_ASSERT_EQ(op.owned_payload_length, fake.u.decoded.main_data_len);
			UT_ASSERT(memcmp(op.owned_payload, fake.data, op.owned_payload_length) == 0);
			UT_ASSERT(rf_side_online_plan_contribution_owners_v1(plan, 0, &owners));
			UT_ASSERT_EQ(owners.owners, RF_SIDE_CONTRIBUTION_UNDO_HEADER
						 | RF_SIDE_CONTRIBUTION_TERMINAL | RF_SIDE_CONTRIBUTION_COMPLETION);
			UT_ASSERT_EQ(rf_side_online_plan_space_contribution_count_v1(plan, 0), 0);
			UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&op.xact, op.owned_payload,
												   op.owned_payload_length), RF_SIDE_XACT_APPLY_BLOCKED);
			ops.arg = &capture;
			ops.begin_protected_set = capture_begin;
			ops.end_protected_set = capture_end;
			ops.preflight_xact = accept_preflight;
			ops.apply_xact = capture_apply;
			UT_ASSERT_NE(rf_side_online_plan_preflight_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_NE(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(capture.begin_count, 0);
			UT_ASSERT_EQ(capture.count, 0);
			UT_ASSERT(!capture.end_complete);
		}
		recovery = pi_side_plan(&identity, false);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(recovery, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		rf_side_online_plan_destroy_v1(&recovery);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_pi_relation_stats_checks_every_item_and_bounded_scope)
{
	for (int variant = 0; variant < 10; variant++) {
		xl_xact_stats_item items[2] = {
			{ PGSTAT_KIND_RELATION, 5, 76543 }, { PGSTAT_KIND_RELATION, 5, 76544 }
		};
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlinePlanV1 *plan;
		Size offset;
		int count;
		Oid database = InvalidOid;

		if (variant == 0) items[1].kind = PGSTAT_KIND_DATABASE;
		if (variant == 1) items[1].kind = -1;
		if (variant == 2) items[1].objoid = InvalidOid;
		if (variant == 3) items[1].dboid = 6;
		if (variant == 4)
			make_commit(&fake, 801, 901, 123456, false);
		else
			pi_commit(&fake, 0, NULL, 0);
		if (variant == 5)
			memcpy(fake.data + MinSizeOfXactCommit + sizeof(xl_xact_xinfo),
				   &database, sizeof(database));
		offset = pi_commit_add_stats(&fake, items, lengthof(items));
		if (variant == 6 || variant == 7) {
			count = variant == 6 ? -1 : INT_MAX;
			memcpy(fake.data + offset, &count, sizeof(count));
		}
		if (variant == 8)
			fake.u.decoded.main_data_len = offset + sizeof(count) + sizeof(items) - 1;
		if (variant == 9)
			fake.data[fake.u.decoded.main_data_len++] = 0;
		identity = pi_identity(&fake);
		record = make_record_plan(&fake);
		plan = pi_side_plan(&identity, true);
		UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &record, &identity), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_NE(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_pi_relation_stats_preserves_other_commit_effect_checks)
{
	for (int variant = 0; variant < 5; variant++) {
		xl_xact_stats_item item = { PGSTAT_KIND_RELATION, 5, 76543 };
		SharedInvalidationMessage msg = { 0 };
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlinePlanV1 *plan;
		RfPageProofDetailV1 detail;

		msg.ko.id = SHAREDINVALKO_COMMIT_ID;
		msg.ko.version = SHAREDINVALKO_COMMIT_VERSION;
		msg.ko.batch_lo = 17;
		if (variant == 1) msg.ko.version++;
		if (variant == 2) msg.id = -99;
		pi_commit(&fake, XACT_COMPLETION_FORCE_SYNC_COMMIT | XACT_XINFO_HAS_AE_LOCKS, &msg, 1);
		pi_commit_add_stats(&fake, &item, 1);
		if (variant == 3) fake.u.decoded.header.xl_xid++;
		identity = pi_identity(&fake);
		if (variant == 4) identity.record.origin_thread = 2;
		record = make_record_plan(&fake);
		plan = pi_side_plan(&identity, true);
		detail = rf_side_online_plan_feed_record_v1(plan, &record, &identity);
		if (variant != 0)
			UT_ASSERT_NE(detail, RF_PAGE_PROOF_DETAIL_OK);
		else {
			RfSideOnlineOperationV1 op;
			RfSideContributionOwnersV1 owners;
			UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
			if (detail == RF_PAGE_PROOF_DETAIL_OK) {
				UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
				UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
				UT_ASSERT_EQ(op.xact.invalidation_count, 1);
				UT_ASSERT_EQ(op.owned_payload_length, fake.u.decoded.main_data_len);
				UT_ASSERT(memcmp(op.owned_payload, fake.data, op.owned_payload_length) == 0);
				UT_ASSERT(rf_side_online_plan_contribution_owners_v1(plan, 0, &owners));
				UT_ASSERT((owners.owners & RF_SIDE_CONTRIBUTION_INVALIDATION) != 0);
				UT_ASSERT((owners.owners & RF_SIDE_CONTRIBUTION_COMPLETION) != 0);
			}
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_pi_commit_keeps_complete_flags_and_invalidation_payload);
	UT_RUN(test_pi_side_plan_rejects_execution_before_callbacks);
	UT_RUN(test_pi_commit_unknown_or_damaged_effects_still_refuse);
	UT_RUN(test_pi_native_create_requires_the_typed_identity_contribution);
	UT_RUN(test_pi_ctrc_generation_zero_is_retained_without_apply_eligibility);
	UT_RUN(test_pi_source_cut_must_be_complete);
	UT_RUN(test_pi_ctrc_other_fields_still_refuse);
	UT_RUN(test_pi_create_namespace_modifier_and_page_reuse_boundaries);
	UT_RUN(test_pi_commit_keeps_all_drop_locators_and_exact_namespace);
	UT_RUN(test_pi_ko_commit_marker_preserves_owner_without_execution);
	UT_RUN(test_pi_relation_stats_keeps_native_owner_without_execution);
	UT_RUN(test_pi_relation_stats_checks_every_item_and_bounded_scope);
	UT_RUN(test_pi_relation_stats_preserves_other_commit_effect_checks);
	UT_DONE();
	return ut_failed_count != 0;
}
