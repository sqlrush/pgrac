/* Actual Start/work/terminal consumers; the completed native fence owner
 * remains the existing controlled leaf in this fixture. */
static bool
ctrc_source_request_empty(const ClusterRecoverySerialRequest *request)
{
	ClusterRecoverySerialRequest empty;
	memset(&empty, 0, sizeof(empty));
	return memcmp(request, &empty, sizeof(empty)) == 0;
}

UT_TEST(test_ctrc_source_request_keeps_original_source_and_provisional_bindings)
{
	ClusterRecoverySerialRequest request;
	ClusterRecoveryTxResultValueV1 tx;
	ClusterRecoveryCtrcResultValueV1 ctrc;
	unsigned allocated, inserts, writes, scopes, steps;
	observation_two_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	tx = window_region.checkpoint_owner.tx_result;
	ctrc = window_region.checkpoint_owner.ctrc_result;
	allocated = allocations; inserts = wal_inserts; writes = stamp_writes;
	scopes = observation_scope_begins; steps = observation_steps;
	for (uint32 i = 0; i < 2; i++) {
		ClusterColdTtSlotInputV1 original = native_ctrc_work.entries[i];
		request = ClusterColdRecoveryCtrcSerialRequest(info.cluster_tx_start,
			&native_ctrc_work, i);
		UT_ASSERT_EQ(request.mode, CLUSTER_RECOVERY_SERIAL_TX_CLOSURE);
		UT_ASSERT(memcmp(&request.duty, &typed.sources[0].claim.identity,
			sizeof(request.duty)) == 0);
		UT_ASSERT_EQ(request.duty.origin_thread_id, 2);
		UT_ASSERT_EQ(request.duty.origin_owner_incarnation, 29);
		UT_ASSERT(request.duty.origin_owner_incarnation
			!= observed_writer.ref.claim.identity.origin_owner_incarnation);
		UT_ASSERT(request.formation == (const ClusterFormationWitnessV1 *)&tx_fence_object);
		UT_ASSERT(request.fence_need_set == (const PgracExternalFenceNeedSetV1 *)&tx_fence_object);
		UT_ASSERT(request.fence_admission_set
			== (const PgracExternalFenceAdmissionSetV1 *)&tx_fence_object);
		UT_ASSERT(request.tx_closure_scope == NULL);
		UT_ASSERT_EQ(request.acquire_timeout_ms, 1000);
		UT_ASSERT(memcmp(&original, &native_ctrc_work.entries[i], sizeof(original)) == 0);
	}
	UT_ASSERT_EQ(allocations, allocated);
	UT_ASSERT_EQ(observation_scope_begins, scopes);
	UT_ASSERT_EQ(observation_steps, steps);
	UT_ASSERT_EQ(wal_inserts, inserts); UT_ASSERT_EQ(stamp_writes, writes);
	UT_ASSERT_EQ(tx_ir_acquires, tx_ir_releases);
	UT_ASSERT(memcmp(&tx, &window_region.checkpoint_owner.tx_result, sizeof(tx)) == 0);
	UT_ASSERT(memcmp(&ctrc, &window_region.checkpoint_owner.ctrc_result, sizeof(ctrc)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
}

UT_TEST(test_ctrc_source_request_accepts_existing_terminals_without_a_c0_subject)
{
	for (unsigned committed = 0; committed < 2; committed++) {
		ClusterRecoverySerialRequest request;
		ClusterColdTtSlotInputV1 original = {0};
		reset();
		typed.replay_count = 1;
		typed.participants[0].thread_id = 2;
		typed.participants[0].owner_incarnation = 29;
		typed.sources[0].claim = observed_writer.ref.claim;
		typed.sources[0].claim.identity.origin_thread_id = 2;
		typed.sources[0].claim.identity.origin_owner_incarnation = 29;
		retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		ctrc_census.subject_count = ctrc_census.unresolved_count = 1;
		UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		UT_ASSERT_EQ(census.count, 0);
		original.key.owner_instance = 2; original.key.segment_id = 257;
		original.key.segment_generation = 0; original.key.slot_offset = 1;
		original.key.xid = 110; original.key.slot_wrap = 4;
		original.slot.xid = 110; original.slot.wrap = 4;
		original.slot.status = committed ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED;
		original.slot.commit_scn = committed ? 61 : InvalidScn;
		original.slot.first_undo_block = uba_encode(257, 3, 2, 16);
		native_ctrc_work.entries[0] = original;
		observation_physical[0].owner_instance = 2; observation_physical[0].segment_id = 257;
		observation_physical[0].segment_generation = 0; observation_physical[0].slot_offset = 1;
		observation_physical[0].slot = original.slot; observation_physical_count = 1;
		serving_actor(); UT_ASSERT(cluster_recovery_tx_result_accept_v1()); startup_actor();
		UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT(!ClusterColdRecoveryTxSerialRequest(info.cluster_tx_start, NULL, &request));
		request = ClusterColdRecoveryCtrcSerialRequest(info.cluster_tx_start, &native_ctrc_work, 0);
		UT_ASSERT_EQ(request.mode, CLUSTER_RECOVERY_SERIAL_TX_CLOSURE);
		UT_ASSERT_EQ(request.duty.origin_owner_incarnation, 29);
		UT_ASSERT(memcmp(&original, &native_ctrc_work.entries[0], sizeof(original)) == 0);
		UT_ASSERT_EQ(wal_inserts + stamp_writes + tx_ir_acquires, 0);
	}
}

UT_TEST(test_ctrc_source_request_requires_original_complete_observation_and_source)
{
	for (unsigned fault = 0; fault < 17; fault++) {
		ClusterRecoverySerialRequest request;
		const ClusterColdCtrcWorkV1 *work = &native_ctrc_work;
		uint32 index = 0;
		observation_ready();
		if (fault != 0)
			UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		switch (fault) {
		case 0: break;
		case 1: window_region.checkpoint_owner.tx_accepted_generation = 0; break;
		case 2: typed.replay_count = 0; break;
		case 3: typed.replay_count = typed.participant_count + 1; break;
		case 4: typed.participant_count = CLUSTER_COLD_MAX_PARTICIPANTS + 1; break;
		case 5: typed.sources[0].claim.identity.origin_owner_incarnation++; break;
		case 6: typed.sources[0].claim.identity.origin_thread_id++; break;
		case 7:
			typed.replay_count = 2; typed.participants[1] = typed.participants[0];
			typed.sources[1] = typed.sources[0]; break;
		case 8: work = (const ClusterColdCtrcWorkV1 *)&info; break;
		case 9: index = native_ctrc_work.counts.subject_count; break;
		case 10: cluster_recovery_cold_start->ctrc_physical->entries[0].slot.xid++; break;
		case 11: typed.census_root.image_sha256[0]++; break;
		case 12: tx_fence_current = false; break;
		case 13: tx_fence_subject_current = false; break;
		case 14: observed_writer.epoch++; break;
		case 15: native_ctrc_work.counts.unresolved_count++; break;
		case 16: typed.participants[0].owner_incarnation = 0;
			typed.sources[0].claim.identity.origin_owner_incarnation = 0; break;
		}
		request = ClusterColdRecoveryCtrcSerialRequest(info.cluster_tx_start, work, index);
		UT_ASSERT(ctrc_source_request_empty(&request));
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
	}
}

UT_TEST(test_ctrc_source_request_refuses_history_only_and_uncovered_sources)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRecoverySerialRequest request;
		observation_ready();
		UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		if (fault == 0) {
			typed.participants[1] = typed.participants[0]; typed.sources[1] = typed.sources[0];
			typed.participants[0].thread_id = 4;
			typed.sources[0].claim.identity.origin_thread_id = 4;
		} else if (fault == 1) {
			typed.participants[0].thread_id = 4;
			typed.sources[0].claim.identity.origin_thread_id = 4;
		} else tx_fence_subject_current = false;
		request = ClusterColdRecoveryCtrcSerialRequest(info.cluster_tx_start, &native_ctrc_work, 0);
		UT_ASSERT(ctrc_source_request_empty(&request));
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	}
}

static unsigned ctrc_request_late_fault;
static void
ctrc_source_request_late_change(void)
{
	ctrc_request_step = NULL;
	switch (ctrc_request_late_fault) {
	case 0: native_ctrc_work.entries[1].slot.first_undo_block.raw[0]++; break;
	case 1: typed.sources[0].timeline++; break;
	case 2: typed.participants[0].native_redo++; break;
	case 3: typed.replay_count = 2; break;
	case 4: typed.participant_count++; break;
	case 5: window_region.checkpoint_owner.tx_result.result_generation++; break;
	case 6: owner_boot++; break;
	case 7: typed.census_root.image_sha256[0]++; break;
	case 8: cluster_recovery_cold_start->ctrc_physical->entries[0].slot.first_undo_block.raw[0]++; break;
	case 9: tx_fence_current = false; break;
	case 10: tx_fence_subject_current = false; break;
	case 11: pg_re_throw(); break;
	}
}

UT_TEST(test_ctrc_source_request_withholds_private_result_after_late_drift)
{
	for (ctrc_request_late_fault = 0; ctrc_request_late_fault < 11; ctrc_request_late_fault++) {
		ClusterRecoverySerialRequest request;
		ClusterRecoveryCtrcResultValueV1 before;
		observation_two_ready();
		UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		before = window_region.checkpoint_owner.ctrc_result;
		ctrc_request_step = ctrc_source_request_late_change;
		request = ClusterColdRecoveryCtrcSerialRequest(info.cluster_tx_start, &native_ctrc_work, 0);
		UT_ASSERT(ctrc_source_request_empty(&request));
		UT_ASSERT(memcmp(&before, &window_region.checkpoint_owner.ctrc_result, sizeof(before)) == 0);
		UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
		if (ctrc_request_late_fault == 6) owner_boot--;
	}
}

UT_TEST(test_ctrc_source_request_propagates_error_without_publishing_or_retiring)
{
	volatile bool caught = false, returned = false;
	ClusterRecoveryCtrcResultValueV1 before;
	observation_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	before = window_region.checkpoint_owner.ctrc_result;
	ctrc_request_late_fault = 11; ctrc_request_step = ctrc_source_request_late_change;
	PG_TRY();
	{
		ClusterRecoverySerialRequest request = ClusterColdRecoveryCtrcSerialRequest(
			info.cluster_tx_start, &native_ctrc_work, 0);
		(void)request; returned = true;
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught && !returned);
	UT_ASSERT(observation_cursor_live && observation_scope_live);
	UT_ASSERT(memcmp(&before, &window_region.checkpoint_owner.ctrc_result, sizeof(before)) == 0);
	UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(!observation_cursor_live && !observation_scope_live);
}

UT_TEST(test_ctrc_source_request_rejects_stale_owner_and_critical_section)
{
	const struct ClusterRecoveryStartV1 *stale;
	ClusterRecoverySerialRequest request;
	observation_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	CritSectionCount = 1;
	request = ClusterColdRecoveryCtrcSerialRequest(info.cluster_tx_start, &native_ctrc_work, 0);
	UT_ASSERT(ctrc_source_request_empty(&request)); CritSectionCount = 0;
	stale = info.cluster_tx_start;
	ClusterReleaseColdRecoveryInputs(&info);
	request = ClusterColdRecoveryCtrcSerialRequest(stale, &native_ctrc_work, 0);
	UT_ASSERT(ctrc_source_request_empty(&request));
	request = ClusterColdRecoveryCtrcSerialRequest(NULL, NULL, UINT32_MAX);
	UT_ASSERT(ctrc_source_request_empty(&request));
}
