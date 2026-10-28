/*-------------------------------------------------------------------------
 * test_cluster_cold_native_clog_consumed.inc
 *    Original Startup source checks around its native CLOG callback.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_cold_native_clog_consumed.inc
 * NOTES
 *    Real Startup gates; the SIDE witness and native redo are controlled.
 *    Consumption is not native durability, lifetime or completion.
 *-------------------------------------------------------------------------
 */
static unsigned native_source_calls, native_source_checks, native_source_fault;
static bool native_source_attempted;
static ClusterColdTypedV1 *native_source_typed;
static int native_source_saved_node;

extern bool side_plan_clog_native_replay(const RfSideOnlinePlanV1 *plan,
	const ClusterWalSourceRef *source, XLogReaderState *reader,
	void (*redo)(XLogReaderState *, void *), bool (*current)(void *), void *arg);

/* The complete real SIDE wrapper is tested by test_cluster_side_xact.
 * This fixture isolates the original Startup/source checks at its callbacks. */
bool
side_plan_clog_native_replay(const RfSideOnlinePlanV1 *plan,
	const ClusterWalSourceRef *source, XLogReaderState *reader,
	void (*redo)(XLogReaderState *, void *), bool (*current)(void *), void *arg)
{
	if (native_source_attempted || plan == NULL || source == NULL || reader == NULL
		|| redo == NULL || current == NULL)
		return false;
	UT_ASSERT(plan == &side_fixture[0]);
	UT_ASSERT(memcmp(source, &native_source_typed->sources[0], sizeof(*source)) == 0);
	native_source_attempted = true;
	native_source_checks++;
	if (!current(arg))
		return false;
	redo(reader, arg);
	native_source_checks++;
	return current(arg);
}

static void
native_source_redo(XLogReaderState *reader, void *arg)
{
	UT_ASSERT(arg == &native_source_calls);
	UT_ASSERT_EQ(XLogRecGetRmid(reader), RM_CLOG_ID);
	native_source_calls++;
	switch (native_source_fault) {
	case 1: tokens[3].file_txn_seq++; break;
	case 2: restart_ref.claim.claim_sha256[0]++; break;
	case 3: side_fixture[0].redo++; break;
	case 4: siglongjmp(*PG_exception_stack, 1); break;
	case 5: native_source_typed->sources[0].claim.claim_sha256[0]++; break;
	case 6: native_source_typed->participants[0].native_redo++; break;
	}
}

static ClusterColdTypedV1 *
native_source_fixture(XLogReaderState *reader, DecodedXLogRecord *decoded)
{
	ClusterColdRecordV1 record;
	ClusterColdTypedV1 *typed = control_fixture(&record);

	memset(reader, 0, sizeof(*reader));
	memset(decoded, 0, sizeof(*decoded));
	reader->record = decoded;
	reader->ReadRecPtr = 0x1000;
	reader->EndRecPtr = 0x1100;
	decoded->header.xl_rmid = RM_CLOG_ID;
	native_source_calls = native_source_checks = native_source_fault = 0;
	native_source_attempted = false;
	native_source_typed = typed;
	native_source_saved_node = cluster_node_id;
	cluster_node_id = 0;
	/* Like the original own-C0 fixture, use the complete captured restart
	 * claim, including its database incarnation, for execution revalidation. */
	restart_ref = typed->sources[0];
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	return typed;
}

static bool
native_source_consume(ClusterColdTypedV1 *typed, XLogReaderState *reader)
{
	return cluster_cold_typed_clog_native_v1(typed,
		(ClusterRecoveryFencePlan *)&fixture_context, reader, native_source_redo,
		&native_source_calls);
}

static void
native_source_done(ClusterColdTypedV1 **typed)
{
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(typed);
	native_source_typed = NULL;
	cluster_node_id = native_source_saved_node;
	UT_ASSERT_EQ(contexts_alive, 0);
}

UT_TEST(test_native_clog_source_wraps_original_founder_callback)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	ClusterColdTypedV1 *typed = native_source_fixture(&reader, &decoded);

	UT_ASSERT(native_source_consume(typed, &reader));
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(native_source_calls, 1);
	UT_ASSERT_EQ(native_source_checks, 2);
	UT_ASSERT(!native_source_consume(typed, &reader));
	UT_ASSERT_EQ(native_source_calls, 1);
	UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
	native_source_done(&typed);
}

UT_TEST(test_native_clog_source_refuses_changed_inputs_before_callback)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		ClusterColdTypedV1 *typed = native_source_fixture(&reader, &decoded);

		switch (fault) {
		case 0: restart_ref_present = false; break;
		case 1: typed->sources[0].claim.claim_sha256[0]++; break;
		case 2: tokens[3].file_txn_seq++; break;
		case 3: typed->side_plans[0] = NULL; break;
		case 4: typed->side_capture_detail[0] = RF_PAGE_PROOF_DETAIL_SOURCE_GAP; break;
		case 5: typed->own_participant = 1; break;
		case 6: cluster_cold_replay_window_leave_v1(); break;
		case 7: side_fixture[0].cut.scan_end_exclusive++; break;
		case 8: decoded.header.xl_rmid = RM_XLOG_ID; break;
		}
		UT_ASSERT(!native_source_consume(typed, &reader));
		UT_ASSERT_EQ(native_source_calls, 0);
		UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
		/* Restore the fixture's source bytes without granting a retry. */
		restart_ref_present = true;
		typed->sources[0] = restart_ref;
		typed->own_participant = 0;
		typed->side_plans[0] = &side_fixture[0];
		typed->side_capture_detail[0] = 0;
		UT_ASSERT(!native_source_consume(typed, &reader));
		UT_ASSERT_EQ(native_source_calls, 0);
		native_source_done(&typed);
	}
}

UT_TEST(test_native_clog_source_late_failure_and_error_stay_refused)
{
	for (unsigned fault = 1; fault <= 6; fault++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		ClusterColdTypedV1 *typed = native_source_fixture(&reader, &decoded);
		volatile bool accepted = false;
		volatile bool caught = false;

		native_source_fault = fault;
		PG_TRY();
		{
			accepted = native_source_consume(typed, &reader);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(!accepted);
		UT_ASSERT_EQ(caught, fault == 4);
		UT_ASSERT_EQ(native_source_calls, 1);
		UT_ASSERT_NE(typed->refusal, CLUSTER_COLD_OK);
		native_source_fault = 0;
		UT_ASSERT(!native_source_consume(typed, &reader));
		UT_ASSERT_EQ(native_source_calls, 1);
		native_source_done(&typed);
	}
}
