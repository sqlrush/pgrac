/*-------------------------------------------------------------------------
 *
 * test_cluster_side_clog_full.inc
 *    Original immutable FULL CLOG input and cold consumer ordering.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_side_clog_full.inc
 * NOTES
 *    Original plan/owner execute; native storage leaves are controlled here.
 *    Native CLOG/SLRU files are tested in test_cluster_clog_recovery.
 *-------------------------------------------------------------------------
 */

/* Keep the expanded contract buildable against the unchanged parent header. */
extern bool cluster_clog_native_recovery_full_record_apply_v1(
	int origin, const ClusterSideProjectionOperationV1 *operation,
	const uint8 *payload, uint32 length);
extern bool cluster_clog_native_recovery_full_record_verify_v1(
	int origin, const ClusterSideProjectionOperationV1 *operation,
	const uint8 *payload, uint32 length);

static const xl_clog_zeropage_full side_full_birth = {
	97, 0, (UINT64_C(7) << 32) | UINT64_C(3178496)
};

bool
cluster_clog_native_recovery_full_record_apply_v1(
	int origin, const ClusterSideProjectionOperationV1 *operation,
	const uint8 *payload, uint32 length)
{
#ifdef TEST_CLUSTER_SIDE_NATIVE_CLOG_OWNER
	if (side_native_clog_test) {
		cold_clog_applies++;
		return test_side_native_clog_apply(origin, operation, payload, length);
	}
#endif
	UT_ASSERT_EQ(length, sizeof(side_full_birth));
	UT_ASSERT(payload != NULL && memcmp(payload, &side_full_birth, length) == 0);
	UT_ASSERT_EQ(operation->normalized_info, CLOG_ZEROPAGE_FULL);
	return cluster_clog_native_recovery_record_apply_v1(origin, operation);
}

bool
cluster_clog_native_recovery_full_record_verify_v1(
	int origin, const ClusterSideProjectionOperationV1 *operation,
	const uint8 *payload, uint32 length)
{
#ifdef TEST_CLUSTER_SIDE_NATIVE_CLOG_OWNER
	if (side_native_clog_test) {
		cold_clog_verifies++;
		return test_side_native_clog_verify(origin, operation, payload, length);
	}
#endif
	UT_ASSERT_EQ(length, sizeof(side_full_birth));
	UT_ASSERT(payload != NULL && memcmp(payload, &side_full_birth, length) == 0);
	UT_ASSERT_EQ(operation->normalized_info, CLOG_ZEROPAGE_FULL);
	return cluster_clog_native_recovery_record_verify_v1(origin, operation);
}

static RfPageProofDetailV1
side_full_feed(RfSideOnlinePlanV1 *plan, FakeXactRecord *fake)
{
	uint8 uuid[16];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;

	memset(uuid, 0x44, sizeof(uuid));
	identity = make_identity(fake, uuid);
	set_identity_range(fake, &identity, 100, 200);
	record = make_projection_record_plan(fake);
	return rf_side_online_plan_feed_record_v1(plan, &record, &identity);
}

UT_TEST(test_full_clog_plan_owns_all_original_birth_bytes)
{
	const uint64 epochs[] = { 0, 1, 7, UINT32_MAX };
	const uint32 pages[] = { 0, 97, 131071 };

	for (unsigned e = 0; e < lengthof(epochs); e++)
		for (unsigned p = 0; p < lengthof(pages); p++) {
			RfSideOnlinePlanV1 *plan = space_online_plan(200);
			FakeXactRecord fake;
			RfSideOnlineOperationV1 operation;
			xl_clog_zeropage_full birth = {
				pages[p], 0, (epochs[e] << 32) | (p == 0 ? 3 : pages[p] * UINT64_C(32768))
			};
			bool captured;

			make_projection_record(&fake, RM_CLOG_ID, CLOG_ZEROPAGE_FULL, &birth, sizeof(birth));
			fake.u.decoded.max_block_id = -1;
			fake.u.decoded.header.xl_xid = e % 2 ? 120 : 0;
			fake.u.decoded.toplevel_xid = p == 1 ? 100 : 0;
			UT_ASSERT(rf_side_online_projection_record_shape_v1(&fake.reader, true));
			captured = side_full_feed(plan, &fake) == RF_PAGE_PROOF_DETAIL_OK;
			UT_ASSERT(captured);
			if (captured) {
				UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
				memset(fake.data, 0x5a, sizeof(birth));
				UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
				UT_ASSERT_EQ(operation.projection.normalized_info, CLOG_ZEROPAGE_FULL);
				UT_ASSERT_EQ(operation.projection.page_number, pages[p]);
				UT_ASSERT_EQ(operation.source_xid, e % 2 ? 120 : 0);
				UT_ASSERT_EQ(operation.source_top_xid, p == 1 ? 100 : 0);
				UT_ASSERT_EQ(operation.owned_payload_length, sizeof(birth));
				UT_ASSERT(operation.owned_payload != NULL);
				if (operation.owned_payload != NULL)
					UT_ASSERT(memcmp(operation.owned_payload, &birth, sizeof(birth)) == 0);
			}
			rf_side_online_plan_destroy_v1(&plan);
		}
}

UT_TEST(test_full_clog_shape_refuses_incomplete_or_unconsumed_frame)
{
	for (unsigned fault = 0; fault < 15; fault++) {
		FakeXactRecord fake;
		xl_clog_zeropage_full birth = side_full_birth;

		if (fault == 3) birth.reserved_zero = 1;
		if (fault == 4) birth.pageno = -1;
		if (fault == 5) birth.first_full_xid = UINT64_C(7) << 32;
		if (fault == 6) birth.first_full_xid = (UINT64_C(7) << 32) | 1;
		if (fault == 7) birth.first_full_xid = (UINT64_C(7) << 32) | 2;
		if (fault == 8) birth.first_full_xid++;
		if (fault == 9) birth.pageno++;
		make_projection_record(&fake, RM_CLOG_ID, CLOG_ZEROPAGE_FULL, &birth, sizeof(birth));
		fake.u.decoded.max_block_id = -1;
		if (fault == 0) fake.u.decoded.main_data_len = 0;
		if (fault == 1) fake.u.decoded.main_data_len--;
		if (fault == 2) fake.u.decoded.main_data_len++;
		if (fault == 10) fake.u.decoded.has_page_version_edge = true;
		if (fault == 11) fake.u.decoded.main_data = NULL;
		if (fault == 12) fake.u.decoded.record_origin = 1;
		if (fault == 13) fake.u.decoded.max_block_id = 0;
		if (fault == 14) fake.u.decoded.header.xl_info |= XLR_SPECIAL_REL_UPDATE;
		UT_ASSERT(!rf_side_online_projection_record_shape_v1(&fake.reader, true));
	}
}

UT_TEST(test_full_clog_cold_owner_keeps_native_postread_before_projection)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		FakeXactRecord fake;
		RfSideOnlineProductionOwnerV1 owner;
		ClusterRecoverySerialGuard guard = { .mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED };
		ClusterThreadRecoveryAuthorityV1 authority
			= { .duty = &canonical_duty, .serial_guard = &guard };
		volatile bool caught = false;
		RfPageProofDetailV1 result = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		bool captured;

		make_projection_record(&fake, RM_CLOG_ID, CLOG_ZEROPAGE_FULL,
							   &side_full_birth, sizeof(side_full_birth));
		fake.u.decoded.max_block_id = -1;
		captured = side_full_feed(plan, &fake) == RF_PAGE_PROOF_DETAIL_OK;
		UT_ASSERT(captured);
		if (!captured) {
			rf_side_online_plan_destroy_v1(&plan);
			continue;
		}
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		if (fault == 6) guard.mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
		cold_clog_test = cold_clog_record_test = control_scope_test = true;
		control_scope_denied = false;
		control_scope_fault = 0;
		cold_clog_record_online = false;
		cold_clog_denied = fault == 4;
		cold_clog_apply_fail = fault == 1;
		cold_clog_verify_fail = fault == 2;
		cold_clog_stale_after_verify = fault == 3;
		cold_clog_throw = fault == 5;
		cold_clog_applies = cold_clog_verifies = cold_clog_record_projections = 0;
		UT_ASSERT(rf_side_online_production_owner_init_v1(
			&owner, &authority, canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		PG_TRY();
		{
			result = rf_side_online_production_apply_v1(plan, &owner);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 5);
		if (!caught)
			UT_ASSERT_EQ(result, fault == 0 ? RF_PAGE_PROOF_DETAIL_OK
										  : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(cold_clog_applies, fault == 4 || fault == 6 ? 0 : 1);
		UT_ASSERT_EQ(cold_clog_verifies, fault == 0 || fault == 2 || fault == 3 ? 1 : 0);
		UT_ASSERT_EQ(cold_clog_record_projections, fault == 0 ? 1 : 0);
		UT_ASSERT_EQ(owner.protected_set_complete, fault == 0);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0
				  && !owner.protected_set_active);
		cold_clog_record_test = cold_clog_record_online = cold_clog_test = cold_clog_denied = false;
		control_scope_test = false;
		cold_clog_apply_fail = cold_clog_verify_fail = cold_clog_throw
			= cold_clog_stale_after_verify = false;
		rf_side_online_plan_destroy_v1(&plan);
	}
}
