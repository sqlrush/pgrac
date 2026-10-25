/* Catch omission of the real canonical terminal consumer from the actual
 * Startup census/handoff. C0, accepted results and terminal matching are real;
 * physical input and scheduler/error boundaries remain the existing leaves. */
UT_TEST(test_ctrc_observation_requires_every_original_c0_terminal)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterRecoveryTxResultValueV1 tx;
		ClusterRecoveryCtrcResultValueV1 ctrc;
		ClusterColdTtSlotInputV1 original;
		observation_ready();
		switch (fault) {
		case 0: observation_physical[0].slot.status = TT_SLOT_ACTIVE; break;
		case 1:
			observation_physical[0].slot.status = TT_SLOT_COMMITTED;
			observation_physical[0].slot.commit_scn = 51; break;
		case 2: observation_physical[0].slot.flags |= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN; break;
		case 3: observation_physical[0].slot.commit_scn = 71; break;
		case 4: observation_physical[0].slot.first_undo_block.raw[0]++; break;
		case 5: census.entries[0].proof.qualified_base = false; break;
		case 6: xid_ready = false; break;
		case 7: census.entries[0].proof.terminal_match = false; break;
		case 8: effects_ready = false; break;
		case 9:
			census.entries[1] = census.entries[0]; census.count = census.proven_count = 2;
			window_region.checkpoint_owner.tx_result.subject_count = 2;
			window_region.checkpoint_owner.tx_result.processed_count = 2;
			window_region.checkpoint_owner.tx_result.completed_count = 2;
			break;
		}
		original = native_ctrc_work.entries[0];
		tx = window_region.checkpoint_owner.tx_result;
		ctrc = window_region.checkpoint_owner.ctrc_result;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT_EQ(observation_steps, 3); /* Full original census preceded qualification. */
		UT_ASSERT(strstr(ClusterColdRecoveryCtrcObservationRefusal(info.cluster_tx_start),
			"stage=physical_terminal result=0") != NULL);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT(ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work) == NULL);
		UT_ASSERT(memcmp(&original, &native_ctrc_work.entries[0], sizeof(original)) == 0);
		UT_ASSERT(memcmp(&tx, &window_region.checkpoint_owner.tx_result, sizeof(tx)) == 0);
		UT_ASSERT(memcmp(&ctrc, &window_region.checkpoint_owner.ctrc_result, sizeof(ctrc)) == 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		UT_ASSERT_EQ(wal_inserts, 1); UT_ASSERT_EQ(stamp_writes, 1);
	}
}

static void
ctrc_terminal_handoff_must_not_poll(void)
{
	/* Bound the parent regression: without qualification it reaches this wait. */
	test_fatal();
}

UT_TEST(test_startup_refuses_unusable_ctrc_terminal_before_independent_wait)
{
	unsigned destroyed;
	observation_ready(); destroyed = input_destroys;
	observation_physical[0].slot.flags |= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	ctrc_wait_step = ctrc_terminal_handoff_must_not_poll;
	fatal_armed = true;
	if (setjmp(fatal_jump) == 0) {
		actual_startup_finish_tx(&info);
		UT_ASSERT(false);
	}
	fatal_armed = false;
	UT_ASSERT_EQ(tx_waits, 0);
	UT_ASSERT_EQ(input_destroys, destroyed);
	UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
	UT_ASSERT_EQ(observation_census_destroys, 1);
	UT_ASSERT_EQ(observation_scope_releases, 1);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
}

UT_TEST(test_ctrc_observation_qualifies_the_last_subject_without_rewriting_inputs)
{
	ClusterColdTtSlotInputV1 originals[2];
	ClusterRecoveryTxResultValueV1 tx;
	ClusterRecoveryCtrcResultValueV1 ctrc;
	observation_two_ready();
	memcpy(originals, native_ctrc_work.entries, sizeof(originals));
	tx = window_region.checkpoint_owner.tx_result;
	ctrc = window_region.checkpoint_owner.ctrc_result;
	/* Reverse physical order: this is the second original subject, generation0. */
	observation_physical[0].slot.commit_scn = 98;
	UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT_EQ(observation_steps, 5);
	UT_ASSERT(strstr(ClusterColdRecoveryCtrcObservationRefusal(info.cluster_tx_start),
		"stage=physical_terminal result=1") != NULL);
	UT_ASSERT(memcmp(originals, native_ctrc_work.entries, sizeof(originals)) == 0);
	UT_ASSERT(memcmp(&tx, &window_region.checkpoint_owner.tx_result, sizeof(tx)) == 0);
	UT_ASSERT(memcmp(&ctrc, &window_region.checkpoint_owner.ctrc_result, sizeof(ctrc)) == 0);
	UT_ASSERT(!observation_cursor_live && !observation_scope_live);
}

UT_TEST(test_ctrc_borrowed_physical_view_rechecks_original_terminal_proofs)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		unsigned steps, begins;
		observation_ready();
		UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT(ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work) != NULL);
		steps = observation_steps; begins = observation_scope_begins;
		switch (fault) {
		case 0: census.entries[0].proof.qualified_base = false; break;
		case 1: census.entries[0].proof.sealed_redo = false; break;
		case 2: census.entries[0].proof.ordinary_effects = false; break;
		case 3: census.entries[0].proof.terminal_match = false; break;
		case 4: census.entries[0].proof.old_writer_isolated = false; break;
		case 5: effects_ready = false; break;
		case 6: xid_ready = false; break;
		}
		UT_ASSERT(ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work) == NULL);
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT_EQ(observation_steps, steps); UT_ASSERT_EQ(observation_scope_begins, begins);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_result.state, CLUSTER_RECOVERY_CTRC_RESULT_PENDING);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	}
}

UT_TEST(test_ctrc_terminal_recheck_preserves_exact_existing_terminals)
{
	for (unsigned committed = 0; committed < 2; committed++) {
		ClusterColdTtSlotInputV1 original = {0};
		const ClusterColdCtrcPhysicalV1 *view;
		unsigned allocations_before;
		reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		ctrc_census.subject_count = ctrc_census.unresolved_count = 1;
		UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
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
		allocations_before = allocations;
		view = ClusterColdRecoveryCtrcPhysical(info.cluster_tx_start, &native_ctrc_work);
		UT_ASSERT(view != NULL && view->count == 1);
		UT_ASSERT(memcmp(&view->entries[0], &observation_physical[0], sizeof(view->entries[0])) == 0);
		UT_ASSERT(memcmp(&original, &native_ctrc_work.entries[0], sizeof(original)) == 0);
		UT_ASSERT_EQ(allocations, allocations_before);
		UT_ASSERT_EQ(wal_inserts + stamp_writes, 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	}
}

static unsigned terminal_late_fault;
static void
ctrc_terminal_late_change(void)
{
	if (terminal_late_fault == 0) pg_re_throw();
	if (terminal_late_fault == 1) native_ctrc_work.entries[0].slot.first_undo_block.raw[0]++;
	if (terminal_late_fault == 2) window_region.checkpoint_owner.tx_result.result_generation++;
	if (terminal_late_fault == 3) owner_boot++;
}

UT_TEST(test_ctrc_terminal_recheck_cleans_original_children_on_error_and_late_drift)
{
	for (terminal_late_fault = 0; terminal_late_fault < 4; terminal_late_fault++) {
		volatile bool caught = false, observed = false;
		observation_ready();
		terminal_xid_step = ctrc_terminal_late_change;
		PG_TRY();
		{
			observed = ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		terminal_xid_step = NULL;
		UT_ASSERT(!observed);
		UT_ASSERT_EQ(caught, terminal_late_fault == 0);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT(strstr(observation_events, "DR") != NULL);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		if (terminal_late_fault == 3) owner_boot--;
	}
}
