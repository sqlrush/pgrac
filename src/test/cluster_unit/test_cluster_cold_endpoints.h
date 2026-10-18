/* Real Startup orchestration and ordered counter fold; storage and original
 * execution ownership remain the Startup boundary fixture. */
static ClusterColdXidProgressV1 endpoint_progress[CLUSTER_COLD_MAX_PARTICIPANTS];
static ClusterColdReplayResultV1 endpoint_result;

static ClusterColdTypedV1 *
endpoint_fixture(bool history)
{
	ClusterColdTypedV1 *typed;

	fixture();
	native_checkpoint[1].checkpoint.nextXid = FullTransactionIdFromU64(UINT64_C(0x200000100));
	native_checkpoint[2].checkpoint.nextXid = FullTransactionIdFromU64(UINT64_C(0x1fffffff0));
	native_checkpoint[3].checkpoint.nextXid = FullTransactionIdFromU64(UINT64_C(0x300000005));
	if (history) {
		history_generation(2, 9, 0x40, 0x80);
		history_generation(2, 10, 0x80, 0xc0);
		history_generation(3, 8, 0x40, 0x90);
	}
	typed = prepare(0x800);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	cluster_cold_replay_window_enter_v1();
	UT_ASSERT(
		cluster_cold_typed_sync_sources_v1(typed, (ClusterRecoveryFencePlan *)&fixture_context));
	memset(endpoint_progress, 0, sizeof(endpoint_progress));
	memset(&endpoint_result, 0, sizeof(endpoint_result));
	endpoint_result.steps_done = cluster_cold_plan_step_count_v1(typed->plan);
	endpoint_result.space_relations_finished
		= cluster_cold_plan_space_relation_count_v1(typed->plan);
	for (uint32 p = 0; p < typed->participant_count; p++) {
		ClusterControlRootSnapshot root;
		ClusterWalRecoveryCheckpointV1 checkpoint;
		ClusterColdParticipantV1 source = typed->participants[p];
		ClusterColdXidProgressV1 *xid
			= p < typed->replay_count ? &endpoint_progress[p] : &typed->history_xid[p];
		ClusterColdRecordV1 record = { 0 };
		ClusterColdXidInputV1 input = { .complete = true };
		uint64 next;

		UT_ASSERT(cluster_cold_typed_checkpoint_v1(typed, p, &root, &checkpoint));
		source.native_redo = checkpoint.checkpoint.redo;
		UT_ASSERT(cluster_cold_xid_begin_v1(
			xid, &source, U64FromFullTransactionId(checkpoint.checkpoint.nextXid), 1));
		record.read_rec_ptr = root.tail_last_record_lsn;
		record.end_rec_ptr = root.validated_tail_lsn_exclusive;
		record.record_crc = root.tail_last_record_crc32c;
		if (p == typed->replay_count)
			input.checkpoint_next_xid = UINT64_C(0x900000009);
		UT_ASSERT(cluster_cold_xid_record_v1(xid, &source, &record, &input));
		UT_ASSERT(cluster_cold_xid_candidate_v1(xid, &source, &next));
		if (p < typed->replay_count) {
			UT_ASSERT_EQ(cluster_cold_plan_replay_record_count_v1(typed->plan, p), 1);
			typed->xid_next_at_drain[p] = next;
			endpoint_result.last_read[p] = record.read_rec_ptr;
			endpoint_result.last_end[p] = record.end_rec_ptr;
			endpoint_result.last_crc[p] = record.record_crc;
		}
	}
	endpoint_result.own_read = endpoint_result.last_read[0];
	endpoint_result.own_end = endpoint_result.last_end[0];
	history_checkpoint_reads = 0;
	return typed;
}

static bool
endpoint_finish(ClusterColdTypedV1 *typed)
{
	return cluster_cold_typed_xid_endpoints_v1(typed, (ClusterRecoveryFencePlan *)&fixture_context,
											   &endpoint_result, endpoint_progress);
}

static void
endpoint_destroy(ClusterColdTypedV1 **typed)
{
	cluster_cold_replay_window_leave_v1();
	cluster_cold_typed_destroy_v1(typed);
}

UT_TEST(test_endpoints_maximum_uses_complete_full_width_inputs)
{
	ClusterColdTypedV1 *typed = endpoint_fixture(false);
	UT_ASSERT(endpoint_finish(typed));
	UT_ASSERT_EQ(typed->xid_next_after_redo, UINT64_C(0x300000005));
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT(!typed->source_drained[0] && !typed->source_drained[1]);
	endpoint_destroy(&typed);
}

UT_TEST(test_endpoints_include_every_history_generation_original_checkpoint_cut)
{
	ClusterColdTypedV1 *typed = endpoint_fixture(true);
	UT_ASSERT(endpoint_finish(typed));
	UT_ASSERT_EQ(typed->xid_next_after_redo, UINT64_C(0x900000009));
	UT_ASSERT_EQ(history_checkpoint_reads, 12); /* before/after endpoints and each TT read */
	UT_ASSERT_EQ(endpoint_progress[3].next_xid, 0);
	UT_ASSERT_EQ(typed->xid_next_at_drain[3], 0);
	UT_ASSERT_EQ(source_sync_calls, 3);
	UT_ASSERT_EQ(history_sync_calls, 3);
	endpoint_destroy(&typed);
}

UT_TEST(test_endpoints_reject_partial_or_conflicting_replay_counters)
{
	for (int fault = 0; fault < 11; fault++) {
		ClusterColdTypedV1 *typed = endpoint_fixture(false);
		ClusterColdXidProgressV1 *xid = &endpoint_progress[1];
		switch (fault) {
		case 0:
			xid->begun = false;
			break;
		case 1:
			xid->failed = true;
			break;
		case 2:
			xid->records = 0;
			break;
		case 3:
			xid->expected_records++;
			break;
		case 4:
			xid->source.owner_incarnation++;
			break;
		case 5:
			xid->source.native_redo++;
			break;
		case 6:
			xid->last_read++;
			break;
		case 7:
			xid->last_end--;
			break;
		case 8:
			xid->next_xid = 0;
			break;
		case 9:
			typed->xid_next_at_drain[1]++;
			break;
		case 10:
			typed->xid_next_at_drain[1] = 0;
			break;
		}
		typed->xid_next_after_redo = 99;
		UT_ASSERT(!endpoint_finish(typed));
		UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
		UT_ASSERT_EQ(typed->xid_next_at_drain[0], 0);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT(!endpoint_finish(typed));
		endpoint_destroy(&typed);
	}
}

UT_TEST(test_endpoints_reject_missing_or_tail_only_history_inputs)
{
	for (int fault = 0; fault < 10; fault++) {
		ClusterColdTypedV1 *typed = endpoint_fixture(true);
		ClusterColdXidProgressV1 *xid = &typed->history_xid[3];
		switch (fault) {
		case 0:
			xid->begun = false;
			break;
		case 1:
			xid->failed = true;
			break;
		case 2:
			xid->source = typed->participants[3];
			break;
		case 3:
			xid->source.owner_incarnation++;
			break;
		case 4:
			xid->last_read++;
			break;
		case 5:
			xid->records--;
			break;
		case 6:
			xid->expected_records++;
			break;
		case 7:
			typed->xid_next_at_drain[3] = xid->next_xid;
			break;
		case 8:
			endpoint_result.last_read[3] = xid->last_read;
			break;
		case 9:
			xid->next_xid = 1;
			break;
		}
		UT_ASSERT(!endpoint_finish(typed));
		UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
		UT_ASSERT_EQ(typed->history_xid[3].next_xid, 0);
		UT_ASSERT(!typed->census_root_captured);
		endpoint_destroy(&typed);
	}
}

UT_TEST(test_endpoints_require_whole_sequencer_completion_and_exact_tails)
{
	for (int fault = 0; fault < 9; fault++) {
		ClusterColdTypedV1 *typed = endpoint_fixture(false);
		switch (fault) {
		case 0:
			endpoint_result.detail = CLUSTER_COLD_REPLAY_CUT_DIFFERS;
			break;
		case 1:
			endpoint_result.steps_done--;
			break;
		case 2:
			endpoint_result.space_relations_finished++;
			break;
		case 3:
			endpoint_result.last_read[1]++;
			break;
		case 4:
			endpoint_result.last_end[1]--;
			break;
		case 5:
			endpoint_result.last_crc[1]++;
			break;
		case 6:
			endpoint_result.own_read++;
			break;
		case 7:
			endpoint_result.own_end++;
			break;
		case 8:
			typed->participants[0].native_redo++;
			break;
		}
		UT_ASSERT(!endpoint_finish(typed));
		UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
		endpoint_destroy(&typed);
	}
}

UT_TEST(test_endpoints_require_current_original_owner_and_all_sources)
{
	for (int fault = 0; fault < 7; fault++) {
		ClusterColdTypedV1 *typed = endpoint_fixture(true);
		switch (fault) {
		case 0:
			fence_current = false;
			break;
		case 1:
			census_token.file_txn_seq++;
			break;
		case 2:
			native_checkpoint[1].checkpoint.oldestXid++;
			break;
		case 3:
			typed->sources[3].claim.claim_sha256[31]++;
			break;
		case 4:
			typed->own_participant = 1;
			break;
		case 5:
			cluster_cold_replay_window_leave_v1();
			break;
		case 6:
			typed->participant_count--;
			break;
		}
		UT_ASSERT(!endpoint_finish(typed));
		UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
		UT_ASSERT_EQ(typed->history_xid[3].next_xid, 0);
		endpoint_destroy(&typed);
	}
}

UT_TEST(test_endpoints_recheck_original_sources_before_publishing_maximum)
{
	ClusterColdTypedV1 *typed = endpoint_fixture(true);
	history_checkpoint_fault = 3;
	UT_ASSERT(!endpoint_finish(typed));
	UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
	endpoint_destroy(&typed);
}

UT_TEST(test_endpoints_error_discards_group_and_preserves_prior_refusal)
{
	ClusterColdTypedV1 *typed = endpoint_fixture(true);
	volatile bool caught = false;

	typed->xid_next_after_redo = 99;
	history_checkpoint_fault = 2;
	PG_TRY();
	{
		(void)endpoint_finish(typed);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
	UT_ASSERT_EQ(typed->xid_next_at_drain[0], 0);
	UT_ASSERT_EQ(typed->history_xid[3].next_xid, 0);
	typed->refusal = CLUSTER_COLD_SPACE_REFUSED;
	UT_ASSERT(!endpoint_finish(typed));
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SPACE_REFUSED);
	endpoint_destroy(&typed);
}

UT_TEST(test_endpoints_new_sync_invalidates_previous_aggregate)
{
	ClusterColdTypedV1 *typed = endpoint_fixture(false);
	UT_ASSERT(endpoint_finish(typed));
	UT_ASSERT(tt_after_captured);
	UT_ASSERT(
		cluster_cold_typed_sync_sources_v1(typed, (ClusterRecoveryFencePlan *)&fixture_context));
	UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
	UT_ASSERT(!tt_after_captured);
	UT_ASSERT_EQ(tt_after_resets, 1);
	UT_ASSERT(!endpoint_finish(typed));
	UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
	endpoint_destroy(&typed);
}

UT_TEST(test_endpoints_capture_and_recheck_post_redo_tt_before_publication)
{
	ClusterColdTypedV1 *typed = endpoint_fixture(true);

	UT_ASSERT_EQ(tt_after_captures, 0);
	UT_ASSERT(endpoint_finish(typed));
	UT_ASSERT_EQ(tt_after_captures, 1);
	UT_ASSERT_EQ(tt_after_rechecks, 1);
	UT_ASSERT_EQ(tt_after_participants, typed->participant_count);
	UT_ASSERT(tt_after_captured && tt_base_captured);
	UT_ASSERT_EQ(typed->xid_next_after_redo, UINT64_C(0x900000009));
	endpoint_destroy(&typed);
	UT_ASSERT_EQ(tt_after_resets, 1);
}

UT_TEST(test_endpoints_incomplete_replay_never_captures_post_redo_tt)
{
	ClusterColdTypedV1 *typed = endpoint_fixture(false);

	endpoint_result.steps_done--;
	UT_ASSERT(!endpoint_finish(typed));
	UT_ASSERT_EQ(tt_after_captures, 0);
	UT_ASSERT_EQ(tt_after_rechecks, 0);
	endpoint_destroy(&typed);
}

UT_TEST(test_endpoints_post_redo_tt_refusal_discards_all_attempt_inputs)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterColdTypedV1 *typed = endpoint_fixture(true);

		if (fault == 0)
			tt_after_capture_result = CLUSTER_COLD_SOURCE_GAP;
		if (fault == 1)
			tt_after_capture_result = CLUSTER_COLD_CAPACITY;
		if (fault == 2)
			tt_after_capture_result = CLUSTER_COLD_OOM;
		if (fault == 3)
			tt_after_recheck_result = CLUSTER_COLD_SOURCE_GAP;
		UT_ASSERT(!endpoint_finish(typed));
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
		UT_ASSERT_EQ(typed->xid_next_at_drain[0], 0);
		UT_ASSERT_EQ(typed->history_xid[3].next_xid, 0);
		UT_ASSERT(!tt_after_captured && !typed->census_root_captured);
		UT_ASSERT(tt_base_captured);
		UT_ASSERT(!endpoint_finish(typed));
		UT_ASSERT_EQ(tt_after_captures, 1);
		endpoint_destroy(&typed);
	}
}

UT_TEST(test_endpoints_post_redo_tt_rechecks_sources_and_cleans_error)
{
	for (int fault = 1; fault <= 4; fault++) {
		ClusterColdTypedV1 *typed = endpoint_fixture(true);
		volatile bool caught = false, ready = false;

		tt_after_fault = fault;
		PG_TRY();
		{
			ready = endpoint_finish(typed);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault >= 3);
		UT_ASSERT(!ready);
		UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
		UT_ASSERT_EQ(typed->history_xid[3].next_xid, 0);
		UT_ASSERT(!tt_after_captured && !typed->census_root_captured);
		UT_ASSERT_EQ(tt_after_resets, 1);
		endpoint_destroy(&typed);
	}
}

UT_TEST(test_endpoints_prior_refusal_resets_previous_post_redo_snapshot)
{
	ClusterColdTypedV1 *typed = endpoint_fixture(false);

	UT_ASSERT(endpoint_finish(typed));
	typed->refusal = CLUSTER_COLD_SPACE_REFUSED;
	UT_ASSERT(!endpoint_finish(typed));
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_SPACE_REFUSED);
	UT_ASSERT_EQ(typed->xid_next_after_redo, 0);
	UT_ASSERT(!tt_after_captured);
	endpoint_destroy(&typed);
}
