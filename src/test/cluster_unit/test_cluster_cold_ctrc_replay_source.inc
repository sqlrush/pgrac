/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_ctrc_replay_source.inc
 *     Keep required recovery source bindings independent of TT occupancy.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_ctrc_replay_source.inc
 *
 * NOTES
 *     Start/work/TX/terminal code is original. Completed-fence and physical
 *     scope services retain the existing controlled fixture boundaries.
 *
 *-------------------------------------------------------------------------
 */
static void
ctrc_replay_request_fixture(void)
{
	observation_two_ready();
	typed.replay_count = 2;
	typed.participants[0].timeline = typed.sources[0].timeline = 1;
	typed.participants[1] = typed.participants[0];
	typed.participants[1].thread_id = 3; typed.participants[1].owner_incarnation = 37;
	typed.sources[1] = typed.sources[0];
	typed.sources[1].claim.identity.origin_thread_id = 3;
	typed.sources[1].claim.identity.origin_owner_incarnation = 37;
	typed.sources[1].claim.identity.origin_node_id = 2;
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
}

UT_TEST(test_ctrc_replay_request_covers_original_source_without_a_tt_subject)
{
	ClusterRecoverySerialRequest request;
	ClusterColdTtSlotInputV1 originals[2];
	unsigned begins, ir, inserts;
	ctrc_replay_request_fixture();
	memcpy(originals, native_ctrc_work.entries, sizeof(originals));
	begins = observation_scope_begins; ir = tx_ir_acquires; inserts = wal_inserts;
	UT_ASSERT_EQ(native_ctrc_work.entries[0].key.owner_instance, 2);
	UT_ASSERT_EQ(native_ctrc_work.entries[1].key.owner_instance, 2);
	request = ClusterColdRecoveryCtrcReplayRequest(info.cluster_tx_start, &native_ctrc_work, 1);
	UT_ASSERT_EQ(request.mode, CLUSTER_RECOVERY_SERIAL_TX_CLOSURE);
	UT_ASSERT_EQ(request.duty.origin_thread_id, 3);
	UT_ASSERT_EQ(request.duty.origin_owner_incarnation, 37);
	UT_ASSERT(memcmp(&request.duty, &typed.sources[1].claim.identity, sizeof(request.duty)) == 0);
	UT_ASSERT(request.tx_closure_scope == NULL);
	UT_ASSERT(request.formation == (const ClusterFormationWitnessV1 *)&tx_fence_object);
	UT_ASSERT_EQ(request.acquire_timeout_ms, 1000);
	UT_ASSERT_EQ(request.release_timeout_ms, 1000);
	UT_ASSERT(memcmp(originals, native_ctrc_work.entries, sizeof(originals)) == 0);
	UT_ASSERT_EQ(observation_scope_begins, begins); UT_ASSERT_EQ(tx_ir_acquires, ir);
	UT_ASSERT_EQ(wal_inserts, inserts);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
}

UT_TEST(test_ctrc_replay_request_allows_empty_work_only_after_complete_observation)
{
	ClusterRecoverySerialRequest request;
	ClusterWalInputsV1 *scope;
	reset();
	typed.replay_count = 1;
	typed.participants[0].thread_id = 2; typed.participants[0].owner_incarnation = 29;
	typed.participants[0].timeline = typed.sources[0].timeline = 1;
	typed.sources[0].claim = observed_writer.ref.claim;
	typed.sources[0].claim.identity.origin_thread_id = 2;
	typed.sources[0].claim.identity.origin_owner_incarnation = 29;
	retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	serving_actor(); UT_ASSERT(cluster_recovery_tx_result_accept_v1()); startup_actor();
	request = ClusterColdRecoveryCtrcReplayRequest(info.cluster_tx_start, &native_ctrc_work, 0);
	UT_ASSERT(ctrc_source_request_empty(&request));
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT_EQ(native_ctrc_work.counts.subject_count, 0);
	request = ClusterColdRecoveryCtrcReplayRequest(info.cluster_tx_start, &native_ctrc_work, 0);
	UT_ASSERT_EQ(request.mode, CLUSTER_RECOVERY_SERIAL_TX_CLOSURE);
	UT_ASSERT_EQ(request.duty.origin_thread_id, 2);
	scope = ClusterColdRecoveryCtrcScope(info.cluster_tx_start, &native_ctrc_work);
	request.tx_closure_scope = scope;
	UT_ASSERT(ClusterColdRecoveryCtrcReplayRequestCurrent(info.cluster_tx_start,
		&native_ctrc_work, 0, scope, &request));
	UT_ASSERT_EQ(wal_inserts + stamp_writes + tx_ir_acquires, 0);
}

UT_TEST(test_ctrc_replay_request_refuses_nonreplay_ambiguous_and_changed_originals)
{
	for (unsigned fault = 0; fault < 13; fault++) {
		ClusterRecoverySerialRequest request;
		uint32 index = 1;
		const ClusterColdCtrcWorkV1 *work = &native_ctrc_work;
		ctrc_replay_request_fixture();
		switch (fault) {
		case 0: index = 2; break;
		case 1: typed.replay_count = 1; break;
		case 2: typed.replay_count = 0; break;
		case 3: typed.replay_count = typed.participant_count + 1; break;
		case 4: typed.participant_count = CLUSTER_COLD_MAX_PARTICIPANTS + 1; break;
		case 5: typed.participants[1] = typed.participants[0]; typed.sources[1] = typed.sources[0]; break;
		case 6: typed.sources[1].claim.identity.origin_owner_incarnation++; break;
		case 7: typed.sources[1].timeline++; break;
		case 8: typed.participants[1].owner_incarnation = 0; break;
		case 9: tx_fence_subject_current = false; break;
		case 10: work = (const ClusterColdCtrcWorkV1 *)&info; break;
		case 11: typed.participants[1].timeline = typed.sources[1].timeline = 0; break;
		case 12: window_region.checkpoint_owner.tx_accepted_generation = 0; break;
		}
		request = ClusterColdRecoveryCtrcReplayRequest(info.cluster_tx_start, work, index);
		UT_ASSERT(ctrc_source_request_empty(&request));
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	}
}

UT_TEST(test_ctrc_replay_current_checks_all_terminals_without_io_or_cleanup)
{
	ClusterRecoverySerialRequest request;
	ClusterWalInputsV1 *scope;
	unsigned manifests, steps, released, allocated;
	ctrc_replay_request_fixture();
	request = ClusterColdRecoveryCtrcReplayRequest(info.cluster_tx_start, &native_ctrc_work, 1);
	scope = ClusterColdRecoveryCtrcScope(info.cluster_tx_start, &native_ctrc_work);
	request.tx_closure_scope = scope;
	manifests = observation_manifests; steps = observation_steps;
	released = observation_scope_releases; allocated = allocations;
	UT_ASSERT(ClusterColdRecoveryCtrcReplayRequestCurrent(info.cluster_tx_start,
		&native_ctrc_work, 1, scope, &request));
	/* The target source has no TT. A different source's last physical TT
	 * still cannot be omitted from the original completed input proof. */
	cluster_recovery_cold_start->ctrc_physical->entries[1].slot.xid++;
	UT_ASSERT(!ClusterColdRecoveryCtrcReplayRequestCurrent(info.cluster_tx_start,
		&native_ctrc_work, 1, scope, &request));
	UT_ASSERT_EQ(observation_manifests, manifests); UT_ASSERT_EQ(observation_steps, steps);
	UT_ASSERT_EQ(observation_scope_releases, released); UT_ASSERT_EQ(allocations, allocated);
	UT_ASSERT(observation_cursor_live && observation_scope_live);
}

UT_TEST(test_ctrc_replay_current_rejects_other_source_and_changed_request_bindings)
{
	ClusterRecoverySerialRequest request, changed;
	ClusterWalInputsV1 *scope;
	ctrc_replay_request_fixture();
	request = ClusterColdRecoveryCtrcReplayRequest(info.cluster_tx_start, &native_ctrc_work, 1);
	scope = ClusterColdRecoveryCtrcScope(info.cluster_tx_start, &native_ctrc_work);
	request.tx_closure_scope = scope;
	for (unsigned fault = 0; fault < 11; fault++) {
		changed = request;
		switch (fault) {
		case 0: changed.mode = CLUSTER_RECOVERY_SERIAL_ONLINE; break;
		case 1: changed.duty.origin_owner_incarnation++; break;
		case 2: changed.expected_root_token.origin_thread_id++; break;
		case 3: changed.pending.generation++; break;
		case 4: changed.formation = NULL; break;
		case 5: changed.fence_need_set = NULL; break;
		case 6: changed.fence_admission_set = NULL; break;
		case 7: changed.tx_closure_scope = NULL; break;
		case 8: changed.acquire_timeout_ms++; break;
		case 9: changed.release_timeout_ms++; break;
		case 10: changed.duty = typed.sources[0].claim.identity; break;
		}
		UT_ASSERT(!ClusterColdRecoveryCtrcReplayRequestCurrent(info.cluster_tx_start,
			&native_ctrc_work, 1, scope, &changed));
	}
	UT_ASSERT(!ClusterColdRecoveryCtrcReplayRequestCurrent(info.cluster_tx_start,
		&native_ctrc_work, 0, scope, &request));
	UT_ASSERT(!ClusterColdRecoveryCtrcReplayRequestCurrent(info.cluster_tx_start,
		&native_ctrc_work, 1, (ClusterWalInputsV1 *)&info, &request));
}

static void
ctrc_replay_request_late_change(void)
{
	/* This fixture starts with two replay origins. The old subject fixture's
	 * count fault changed one to two; here two must actually change to one. */
	if (ctrc_request_late_fault == 3) {
		ctrc_request_step = NULL;
		typed.replay_count = 1;
	} else
		ctrc_source_request_late_change();
}

UT_TEST(test_ctrc_replay_request_withholds_result_after_original_late_drift)
{
	for (unsigned local = 0; local < 2; local++)
		for (ctrc_request_late_fault = 0; ctrc_request_late_fault < 11; ctrc_request_late_fault++) {
			ClusterRecoverySerialRequest request;
			ClusterWalInputsV1 *scope;
			unsigned released;
			ctrc_replay_request_fixture();
			request = ClusterColdRecoveryCtrcReplayRequest(info.cluster_tx_start, &native_ctrc_work, 0);
			scope = ClusterColdRecoveryCtrcScope(info.cluster_tx_start, &native_ctrc_work);
			released = observation_scope_releases;
			ctrc_request_step = ctrc_replay_request_late_change;
			if (local) {
				request.tx_closure_scope = scope;
				UT_ASSERT(!ClusterColdRecoveryCtrcReplayRequestCurrent(info.cluster_tx_start,
					&native_ctrc_work, 0, scope, &request));
				UT_ASSERT(observation_cursor_live && observation_scope_live);
				UT_ASSERT_EQ(observation_scope_releases, released);
			} else {
				request = ClusterColdRecoveryCtrcReplayRequest(info.cluster_tx_start, &native_ctrc_work, 0);
				UT_ASSERT(ctrc_source_request_empty(&request));
			}
			UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
			if (ctrc_request_late_fault == 6) owner_boot--;
		}
}

UT_TEST(test_ctrc_replay_request_error_never_publishes_or_retires_original_owner)
{
	volatile bool caught = false, returned = false;
	ctrc_replay_request_fixture();
	ctrc_request_late_fault = 11; ctrc_request_step = ctrc_source_request_late_change;
	PG_TRY();
	{
		ClusterRecoverySerialRequest request = ClusterColdRecoveryCtrcReplayRequest(
			info.cluster_tx_start, &native_ctrc_work, 1);
		(void)request; returned = true;
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught && !returned);
	UT_ASSERT(observation_cursor_live && observation_scope_live);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, true, NULL);
	UT_ASSERT(!observation_cursor_live && !observation_scope_live);
}

UT_TEST(test_ctrc_replay_request_rejects_stale_start_and_critical_section)
{
	const struct ClusterRecoveryStartV1 *stale;
	ClusterRecoverySerialRequest request;
	ctrc_replay_request_fixture();
	CritSectionCount = 1;
	request = ClusterColdRecoveryCtrcReplayRequest(info.cluster_tx_start, &native_ctrc_work, 0);
	UT_ASSERT(ctrc_source_request_empty(&request)); CritSectionCount = 0;
	stale = info.cluster_tx_start;
	ClusterReleaseColdRecoveryInputs(&info);
	request = ClusterColdRecoveryCtrcReplayRequest(stale, &native_ctrc_work, 0);
	UT_ASSERT(ctrc_source_request_empty(&request));
	request = ClusterColdRecoveryCtrcReplayRequest(NULL, NULL, UINT32_MAX);
	UT_ASSERT(ctrc_source_request_empty(&request));
	UT_ASSERT(!ClusterColdRecoveryCtrcReplayRequestCurrent(NULL, NULL, UINT32_MAX, NULL, &request));
}
