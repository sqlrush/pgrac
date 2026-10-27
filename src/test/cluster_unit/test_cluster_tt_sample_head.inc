/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_sample_head.inc
 *    Exercise exact canonical head sampling before live rollback publication.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_tt_sample_head.inc
 *
 * NOTES
 *    PGRAC-original tests use the linked TT owner with explicit current,
 *    storage and admission boundaries; no shared server or recovery runs.
 *
 *-------------------------------------------------------------------------
 */

static bool
sample_head_call(TestLiveHeadCall *call, UBA *out, bool *result)
{
	volatile bool caught = false;

	*result = false;
	PG_TRY();
	{
#ifdef CLUSTER_TT_SAMPLE_OPERATION_HEAD_PRESENT
		*result = cluster_tt_slot_durable_sample_operation_head(
			(const xl_undo_tt_slot_set_head_operation *)&call->record, &call->admission, out);
#else
		(void)call;
		if (out != NULL)
			*out = (UBA){ { 0, 0 } };
#endif
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}

static void
sample_head_no_effects(void)
{
	live_head_no_publication();
	UT_ASSERT_EQ(g_writer_open_calls, 0);
	UT_ASSERT_EQ(g_pin_downgrade_calls, 0);
	UT_ASSERT_EQ(g_lock_content_calls, 0);
}

UT_TEST(test_sample_head_returns_exact_current_source_and_target)
{
	UBA heads[] = { (UBA){ { 0, 0 } }, uba_encode(1, 8, 7, 3), uba_encode(2, 1, 7, 0) };

	for (unsigned side = 0; side < 2; side++)
		for (unsigned i = 0; i < lengthof(heads); i++) {
			TestLiveHeadCall call = live_head_setup(side != 0);
			PGAlignedBlock before;
			UBA sampled;
			bool result;

			live_head_set_previous(&call, heads[i]);
			memcpy(before.data, g_canned_block, BLCKSZ);
			UT_ASSERT(!sample_head_call(&call, &sampled, &result));
			UT_ASSERT(result);
			UT_ASSERT(cluster_undo_record_uba_equal(sampled, heads[i]));
			UT_ASSERT_EQ(memcmp(before.data, g_canned_block, BLCKSZ), 0);
			UT_ASSERT_EQ(memcmp(before.data, g_current_resident, BLCKSZ), 0);
			UT_ASSERT_EQ(g_current_target_acquire_calls, side);
			UT_ASSERT_EQ(g_current_acquire_calls, 1);
			UT_ASSERT_EQ(g_current_cancel_calls, 1);
			UT_ASSERT_EQ(g_current_unpin_calls, 1);
			sample_head_no_effects();
		}
}

UT_TEST(test_sample_head_is_only_an_exact_compare_input)
{
	TestLiveHeadCall call = live_head_setup(false);
	UBA sampled;
	bool result;

	live_head_set_previous(&call, uba_encode(1, 8, 7, 3));
	UT_ASSERT(!sample_head_call(&call, &sampled, &result));
	UT_ASSERT(result);
	live_head_set_previous(&call, uba_encode(1, 4, 7, 1));
	call.previous = sampled;
	UT_ASSERT(live_head_call(&call, false, false));
	UT_ASSERT_EQ(last_ereport_elevel, ERROR);
	live_head_no_publication();
}

UT_TEST(test_sample_head_rejects_incomplete_identity_before_current)
{
	for (unsigned i = 0; i < 10; i++) {
		TestLiveHeadCall call = live_head_setup(false);
		UBA sampled = call.record.head.first_undo_block;
		bool result;

		if (i == 0)
			call.record.operation.canonical_segment_generation = UINT32_MAX;
		else if (i == 1)
			call.record.operation.canonical_top_fxid = 0;
		else if (i == 2)
			call.record.operation.operation_fxid = 0;
		else if (i == 3)
			call.record.operation.operation_sequence = 0;
		else if (i == 4)
			call.record.head._pad[0] = 0;
		else if (i == 5)
			call.admission.entered = false;
		else if (i == 6)
			call.admission.side = (ClusterSemanticAdmissionSide)99;
		else if (i == 7)
			MyProc = NULL;
		else if (i == 8)
			cluster_shared_config = false;
		else
			CritSectionCount = 1;
		UT_ASSERT(sample_head_call(&call, &sampled, &result));
		UT_ASSERT(!result && UBA_is_invalid(sampled));
		UT_ASSERT_EQ(g_current_acquire_calls, 0);
		CritSectionCount = 0;
		sample_head_no_effects();
	}
}

UT_TEST(test_sample_head_rejects_unknown_current_or_canonical_bytes)
{
	for (unsigned i = 0; i < 19; i++) {
		TestLiveHeadCall call = live_head_setup(false);
		UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;
		UBA sampled = call.record.head.first_undo_block;
		bool result;

		/* A changed, valid head is an observation, not an identity error. */
		if (i == 10)
			continue;
		if (i < 15)
			live_head_bad_authority(i);
		else {
			if (i == 15)
				disk->tt_slots[7].wrap++;
			else if (i == 16)
				disk->tt_slots[7].first_undo_block = uba_encode(1, 1, 8, 0);
			else if (i == 17)
				disk->tt_slots[7].first_undo_block = uba_encode(1, 0, 7, 0);
			else
				disk->tt_slots[7].first_undo_block
					= uba_encode(CLUSTER_UNDO_SEGS_PER_INSTANCE + 1, 1, 7, 0);
			UT_ASSERT(cluster_undo_segment_header_seal(disk));
			memcpy(g_current_resident, disk, BLCKSZ);
		}
		UT_ASSERT(sample_head_call(&call, &sampled, &result));
		UT_ASSERT(!result && UBA_is_invalid(sampled));
		UT_ASSERT_EQ(last_ereport_elevel, ERROR);
		sample_head_no_effects();
	}
}

UT_TEST(test_sample_head_rechecks_actor_root_and_current_before_return)
{
	for (unsigned i = 0; i < 3; i++) {
		TestLiveHeadCall call = live_head_setup(false);
		UBA sampled = call.record.head.first_undo_block;
		bool result;

		if (i == 0)
			g_modifier_recheck_hook = live_head_revoke_current;
		else if (i == 1)
			g_current_root_drift = true;
		else
			g_before_current_acquire_hook = live_head_revoke_vxid;
		UT_ASSERT(sample_head_call(&call, &sampled, &result));
		UT_ASSERT(!result && UBA_is_invalid(sampled));
		sample_head_no_effects();
	}
}

UT_TEST(test_sample_head_keeps_original_wait_and_request_snapshot)
{
	TestLiveHeadCall call = live_head_setup(false);
	UBA sampled;
	bool result;

	live_head_input = &call;
	g_before_current_acquire_hook = live_head_change_caller_request;
	g_current_acquire_pending = 3;
	UT_ASSERT(!sample_head_call(&call, &sampled, &result));
	UT_ASSERT(result && UBA_is_invalid(sampled));
	UT_ASSERT_EQ(g_current_wait_calls, 2);
	sample_head_no_effects();
	call = live_head_setup(false);
	g_current_acquire_pending = 3;
	g_process_interrupt_error = true;
	InterruptPending = true;
	UT_ASSERT(sample_head_call(&call, &sampled, &result));
	UT_ASSERT(!result && UBA_is_invalid(sampled));
	InterruptPending = false;
	sample_head_no_effects();
	reset_current_write_mock();
	g_live_head_test_mode = false;
	g_live_head_flush_hook = NULL;
}
