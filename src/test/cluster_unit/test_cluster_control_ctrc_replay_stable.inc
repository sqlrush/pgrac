/*-------------------------------------------------------------------------
 *
 * test_cluster_control_ctrc_replay_stable.inc
 *     Bind a page graph to every original native replay-set owner.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_control_ctrc_replay_stable.inc
 *
 * NOTES
 *     Original ROOT/claim/WAL scopes and native set code execute. Start,
 *     distributed GES and decoded graph edges are controlled test inputs.
 *
 *-------------------------------------------------------------------------
 */
typedef struct CtrcReplayStableFixture {
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCtrcReplaySetSourceV1 owners[2];
	RfPagePinnedSourceV1 source;
	RfContributorStreamCutV1 cuts[2];
	RfPageStableEdgeInputV1 edges[2];
	RfContributorVectorV1 vector;
	RfPageStableGraphRequestV1 graph;
} CtrcReplayStableFixture;

static void
ctrc_replay_stable_fixture(CtrcReplayStableFixture *f)
{
	RfPageIdentityV1 identity = {0};
	RfPageVersionV1 version = {0};

	memset(f, 0, sizeof(*f));
	ctrc_set_fixture(3, true, &f->inputs);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(f->inputs), 0);
	/* Original indexes are reversed; the unchanged graph orders cuts by
	 * thread, while each native view retains its own original index. */
	for (unsigned i = 0; i < 2; i++)
		f->owners[i] = cluster_wal_inputs_ctrc_replays_source_v1(f->inputs, 1 - i);
	identity.system_identifier = f->owners[0].selected.source.claim.identity.system_identifier;
	memcpy(identity.storage_uuid, f->owners[0].selected.source.claim.identity.storage_uuid, 16);
	identity.locator = (RelFileLocator){1663, 5, 16392}; identity.blockno = 7;
	memset(version.segment_incarnation, 0x79, 16); version.mutation_token = 41;
	f->source.page_identity = identity; f->source.source_version = version;
	f->source.binding_cookie = f->source.current_binding_cookie = 19;
	f->source.identity_verified = f->source.integrity_verified = true;
	for (unsigned i = 0; i < 2; i++) {
		const ClusterWalInputV1 *selected = &f->owners[i].selected;
		RfContributorStreamCutV1 *cut = &f->cuts[i];
		RfPageStableEdgeInputV1 *edge = &f->edges[i];
		cut->failed_thread = selected->source.claim.identity.origin_thread_id;
		cut->origin_owner_incarnation = selected->source.claim.identity.origin_owner_incarnation;
		cut->timeline_id = selected->source.timeline; cut->flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cut->scan_begin_inclusive = selected->checkpoint.checkpoint_lower_lsn;
		cut->scan_end_exclusive = selected->checkpoint.validated_tail_lsn_exclusive;
		cut->contributor_count = cut->component_count = 1;
		edge->page_identity = identity; edge->participant_index = i;
		edge->edge.page_class = RF_PAGE_CLASS_ORDINARY;
		edge->edge.before_kind = edge->edge.result_kind = RF_PAGE_STATE_PRESENT;
		edge->edge.before = version; edge->edge.before.mutation_token = 41 + i;
		memcpy(edge->edge.result_incarnation, version.segment_incarnation, 16);
		edge->edge.edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
		edge->result_token = 42 + i;
		edge->record_identity.system_identifier = identity.system_identifier;
		memcpy(edge->record_identity.storage_uuid, identity.storage_uuid, 16);
		edge->record_identity.origin_thread = cut->failed_thread;
		edge->record_identity.timeline_id = cut->timeline_id;
		edge->record_identity.read_rec_ptr = cut->scan_begin_inclusive;
		edge->record_identity.end_rec_ptr = cut->scan_end_exclusive;
		edge->record_identity.record_crc = 17 + i; edge->record_identity.rmid = RM_GENERIC_ID;
		edge->component_count = 1; memset(edge->anchor_digest, 0x39 + i, 32);
		edge->record_complete = edge->opcode_supported = true;
		edge->side_complete = edge->image_integrity_ok = true;
	}
	f->vector.system_identifier = identity.system_identifier;
	memcpy(f->vector.storage_uuid, identity.storage_uuid, 16);
	f->vector.participant_count = f->vector.edge_count = 2;
	f->vector.cuts = f->cuts; f->vector.edges = f->edges;
	f->graph.page_identity = identity; f->graph.expected_result = version;
	f->graph.expected_result.mutation_token = 43;
	f->graph.source = &f->source; f->graph.contributors = &f->vector;
	f->graph.participant_count = 2;
}

static RfPageStableCtrcBuildV1
ctrc_replay_stable_build(CtrcReplayStableFixture *f)
{
	return rf_page_stable_base_proof_build_ctrc_replays_v1(&f->graph, f->owners, 2, 1024 * 1024);
}

static bool
ctrc_replay_stable_matches(CtrcReplayStableFixture *f, RfPageStableBaseProofV1 *proof)
{
	return rf_page_stable_base_proof_matches_ctrc_replays_v1(proof, &f->graph.page_identity,
		&f->graph.expected_result, f->owners, 2, &f->source, &f->vector);
}

UT_TEST(test_ctrc_replay_stable_two_actual_origins_without_tt)
{
	CtrcReplayStableFixture f; RfPageStableCtrcBuildV1 built; unsigned cf;
	ctrc_replay_stable_fixture(&f); cf = test_cf_lock_calls;
	built = ctrc_replay_stable_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK); UT_ASSERT(built.proof != NULL);
	UT_ASSERT(test_cf_lock_calls > cf); cf = test_cf_lock_calls;
	UT_ASSERT(ctrc_replay_stable_matches(&f, built.proof));
	UT_ASSERT_EQ(test_cf_lock_calls, cf); UT_ASSERT_EQ(test_tx_ir_grants, 2);
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT_EQ(ctrc_inputs_work.counts.subject_count, 0);
	UT_ASSERT(rf_page_stable_base_proof_covers_version_v1(built.proof,
		&f.graph.page_identity, &f.source.source_version));
	UT_ASSERT(!rf_page_stable_base_proof_matches_ctrc_v1(built.proof,
		&f.graph.page_identity, &f.graph.expected_result, NULL, 2, &f.source, &f.vector));
	UT_ASSERT(!rf_page_stable_base_proof_matches_v1(built.proof, &f.graph.page_identity,
		&f.graph.expected_result, NULL, NULL, NULL, NULL, NULL, NULL, &f.source, &f.vector, 2));
	UT_ASSERT(!rf_page_stable_base_proof_matches_sources_v1(built.proof,
		&f.graph.page_identity, &f.graph.expected_result, NULL, 2, &f.source, &f.vector));
	rf_page_stable_base_proof_destroy_v1(&built.proof);
	UT_ASSERT(inputs_recovery_held && ctrc_set_held == 2 && test_tx_ir_releases == 0);
	ctrc_set_finish(&f.inputs);
}

UT_TEST(test_ctrc_replay_stable_refuses_omitted_or_duplicated_set_origin)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		CtrcReplayStableFixture f; RfPageStableCtrcBuildV1 built; unsigned cf;
		ctrc_replay_stable_fixture(&f); cf = test_cf_lock_calls;
		if (fault == 0) {
			f.vector.participant_count = f.graph.participant_count = f.vector.edge_count = 1;
			f.graph.expected_result.mutation_token = 42;
			built = rf_page_stable_base_proof_build_ctrc_replays_v1(&f.graph, f.owners, 1, 1024 * 1024);
		} else {
			if (fault == 1) f.owners[1] = f.owners[0];
			else f.owners[1].source_index = 2;
			built = ctrc_replay_stable_build(&f);
		}
		UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK && built.proof == NULL);
		UT_ASSERT_EQ(test_cf_lock_calls, cf); UT_ASSERT_EQ(ctrc_set_held, 2);
		ctrc_set_finish(&f.inputs);
	}
}

UT_TEST(test_ctrc_replay_stable_refuses_changed_native_source_or_uncovered_cut)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		CtrcReplayStableFixture f; RfPageStableCtrcBuildV1 built;
		ctrc_replay_stable_fixture(&f);
		switch (fault) {
		case 0: f.owners[1].ir_request_id++; break;
		case 1: f.owners[1].scope = (ClusterWalInputsV1 *)(uintptr_t)1; break;
		case 2: f.owners[1].work_generation++; break;
		case 3: f.owners[1].input_owner.generation++; break;
		case 4: f.cuts[1].origin_owner_incarnation++; break;
		case 5: f.cuts[1].timeline_id++; break;
		case 6: f.cuts[1].scan_begin_inclusive--; break;
		case 7: f.cuts[1].scan_end_exclusive++; break;
		case 8: f.graph.page_identity.storage_uuid[0] ^= 1; break;
		}
		built = ctrc_replay_stable_build(&f);
		UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK && built.proof == NULL);
		UT_ASSERT(inputs_recovery_held && ctrc_set_held == 2); ctrc_set_finish(&f.inputs);
	}
}

UT_TEST(test_ctrc_replay_stable_preserves_budget_and_full_graph_refusals)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		CtrcReplayStableFixture f; RfPageStableCtrcBuildV1 built; unsigned cf;
		ctrc_replay_stable_fixture(&f); cf = test_cf_lock_calls;
		if (fault == 0) {
			built = rf_page_stable_base_proof_build_ctrc_replays_v1(&f.graph, f.owners, 2, 0);
			UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_CAPACITY); UT_ASSERT_EQ(test_cf_lock_calls, cf);
		} else {
			if (fault == 1) f.edges[1].side_complete = false;
			if (fault == 2) f.edges[1].edge.before.mutation_token = 999;
			if (fault == 3) f.edges[1].record_identity.origin_thread = f.cuts[0].failed_thread;
			if (fault == 4) f.source.binding_cookie++;
			built = ctrc_replay_stable_build(&f);
			UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK);
		}
		UT_ASSERT(built.proof == NULL && inputs_recovery_held && ctrc_set_held == 2);
		ctrc_set_finish(&f.inputs);
	}
}

UT_TEST(test_ctrc_replay_stable_freezes_every_owner_and_graph_byte)
{
	CtrcReplayStableFixture f; RfPageStableCtrcBuildV1 built;
	void *buffers[5]; Size sizes[5]; unsigned cf;
	ctrc_replay_stable_fixture(&f); built = ctrc_replay_stable_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK); cf = test_cf_lock_calls;
	buffers[0] = f.owners; sizes[0] = sizeof(f.owners);
	buffers[1] = &f.source; sizes[1] = sizeof(f.source);
	buffers[2] = f.cuts; sizes[2] = sizeof(f.cuts);
	buffers[3] = f.edges; sizes[3] = sizeof(f.edges);
	buffers[4] = &f.vector; sizes[4] = sizeof(f.vector);
	for (unsigned b = 0; b < lengthof(buffers); b++) {
		for (Size i = 0; i < sizes[b]; i++) {
			uint8 *byte = (uint8 *)buffers[b] + i;
			*byte ^= 1; UT_ASSERT(!ctrc_replay_stable_matches(&f, built.proof)); *byte ^= 1;
		}
	}
	UT_ASSERT(ctrc_replay_stable_matches(&f, built.proof)); UT_ASSERT_EQ(test_cf_lock_calls, cf);
	rf_page_stable_base_proof_destroy_v1(&built.proof); ctrc_set_finish(&f.inputs);
}

UT_TEST(test_ctrc_replay_stable_cf_wait_and_lost_holder_keep_all_debt)
{
	CtrcReplayStableFixture f; RfPageStableCtrcBuildV1 built;
	ctrc_replay_stable_fixture(&f); test_cf_grant = false;
	built = ctrc_replay_stable_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_WOULD_BLOCK);
	UT_ASSERT(built.proof == NULL && inputs_recovery_held && ctrc_set_held == 2);
	test_cf_grant = true; built = ctrc_replay_stable_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK);
	test_input_current = false; UT_ASSERT(!ctrc_replay_stable_matches(&f, built.proof));
	UT_ASSERT(inputs_recovery_held && ctrc_set_held == 2);
	rf_page_stable_base_proof_destroy_v1(&built.proof); ctrc_set_finish(&f.inputs);
	ctrc_replay_stable_fixture(&f); v2_checkpoint_root_race(); built = ctrc_replay_stable_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_ROOT_STALE);
	UT_ASSERT(built.proof == NULL && inputs_recovery_held && ctrc_set_held == 2);
	ctrc_set_finish(&f.inputs);
}

static CtrcReplayStableFixture *ctrc_replay_stable_io_fixture;
static unsigned ctrc_replay_stable_io_fault;
static void
ctrc_replay_stable_during_root(void)
{
	CtrcReplayStableFixture *f = ctrc_replay_stable_io_fixture;
	switch (ctrc_replay_stable_io_fault) {
	case 0: v2_checkpoint_root_race(); break;
	case 1: inputs_pin_current = false; break;
	case 2: ctrc_inputs_work.generation++; break;
	case 3: f->edges[1].edge.before.mutation_token++; break;
	case 4: f->owners[1].ir_request_id++; break;
	case 5: tx_inputs_typed.participants[1].reserved_zero = 1; break;
	case 6: pg_re_throw(); break;
	}
}

UT_TEST(test_ctrc_replay_stable_late_change_or_error_never_publishes)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		CtrcReplayStableFixture f; RfPageStableCtrcBuildV1 built = {0}; volatile bool caught = false;
		ctrc_replay_stable_fixture(&f); ctrc_replay_stable_io_fixture = &f;
		ctrc_replay_stable_io_fault = fault; test_stop_share_hook = ctrc_replay_stable_during_root;
		test_stop_share_call = test_cf_lock_calls + 1;
		PG_TRY(); { built = ctrc_replay_stable_build(&f); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY(); test_stop_share_hook = NULL; ctrc_replay_stable_io_fixture = NULL;
		UT_ASSERT_EQ((bool)caught, fault == 6);
		if (!caught) UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK && built.proof == NULL);
		UT_ASSERT(inputs_recovery_held && ctrc_set_held == 2); ctrc_set_finish(&f.inputs);
	}
}

UT_TEST(test_ctrc_replay_stable_retired_scope_cannot_revive_private_proof)
{
	CtrcReplayStableFixture f; RfPageStableCtrcBuildV1 built;
	ctrc_replay_stable_fixture(&f); built = ctrc_replay_stable_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_release_v1(f.inputs), 0);
	UT_ASSERT(!ctrc_replay_stable_matches(&f, built.proof));
	ctrc_set_finish(&f.inputs);
	UT_ASSERT(!ctrc_replay_stable_matches(&f, built.proof));
	rf_page_stable_base_proof_destroy_v1(&built.proof); UT_ASSERT(built.proof == NULL);
}
