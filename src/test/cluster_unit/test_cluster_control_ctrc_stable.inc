/*-------------------------------------------------------------------------
 *
 * test_cluster_control_ctrc_stable.inc
 *     Bind stable-base graph input to an original native CTRC subject.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_control_ctrc_stable.inc
 *
 * NOTES
 *     Original ROOT/claim/WAL input and native scope code are real. Graph
 *     edges and existing Start/GES leaves are controlled fixture inputs.
 *
 *-------------------------------------------------------------------------
 */
typedef struct CtrcStableFixture {
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCtrcSubjectAuthorityV1 owner;
	RfPagePinnedSourceV1 source;
	RfContributorStreamCutV1 cuts[2];
	RfPageStableEdgeInputV1 edges[2];
	RfContributorVectorV1 vector;
	RfPageStableGraphRequestV1 graph;
} CtrcStableFixture;

static void
ctrc_stable_fixture(CtrcStableFixture *f)
{
	ClusterRecoverySerialRequest request;
	const ClusterWalInputV1 *selected;
	RfPageIdentityV1 identity = {0};
	RfPageVersionV1 before = {0}, result = {0};

	memset(f, 0, sizeof(*f));
	ctrc_ir_fixture(true, &f->inputs); request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(f->inputs, 0, &request), 0);
	f->owner = cluster_wal_inputs_ctrc_subject_v1(f->inputs, 0);
	UT_ASSERT(cluster_wal_inputs_ctrc_subject_current_v1(&f->owner));
	selected = &f->owner.source.selected;
	identity.system_identifier = selected->source.claim.identity.system_identifier;
	memcpy(identity.storage_uuid, selected->source.claim.identity.storage_uuid, 16);
	identity.locator = (RelFileLocator){1663, 5, 16392};
	identity.blockno = 7;
	memset(before.segment_incarnation, 0x79, 16); before.mutation_token = 41;
	result = before; result.mutation_token++;
	f->source.page_identity = identity;
	f->source.source_version = before;
	f->source.binding_cookie = f->source.current_binding_cookie = 19;
	f->source.identity_verified = f->source.integrity_verified = true;
	f->cuts[0].failed_thread = selected->source.claim.identity.origin_thread_id;
	f->cuts[0].origin_owner_incarnation = selected->source.claim.identity.origin_owner_incarnation;
	f->cuts[0].timeline_id = selected->source.timeline;
	f->cuts[0].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	f->cuts[0].scan_begin_inclusive = selected->checkpoint.checkpoint_lower_lsn;
	f->cuts[0].scan_end_exclusive = selected->checkpoint.validated_tail_lsn_exclusive;
	f->cuts[0].contributor_count = f->cuts[0].component_count = 1;
	f->edges[0].page_identity = identity;
	f->edges[0].edge.page_class = RF_PAGE_CLASS_ORDINARY;
	f->edges[0].edge.before_kind = f->edges[0].edge.result_kind = RF_PAGE_STATE_PRESENT;
	f->edges[0].edge.before = before;
	memcpy(f->edges[0].edge.result_incarnation, result.segment_incarnation, 16);
	f->edges[0].edge.edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
	f->edges[0].result_token = result.mutation_token;
	f->edges[0].record_identity.system_identifier = identity.system_identifier;
	memcpy(f->edges[0].record_identity.storage_uuid, identity.storage_uuid, 16);
	f->edges[0].record_identity.origin_thread = f->cuts[0].failed_thread;
	f->edges[0].record_identity.timeline_id = f->cuts[0].timeline_id;
	f->edges[0].record_identity.read_rec_ptr = f->cuts[0].scan_begin_inclusive;
	f->edges[0].record_identity.end_rec_ptr = f->cuts[0].scan_end_exclusive;
	f->edges[0].record_identity.record_crc = 17;
	f->edges[0].record_identity.rmid = RM_GENERIC_ID;
	f->edges[0].component_count = 1;
	memset(f->edges[0].anchor_digest, 0x39, 32);
	f->edges[0].record_complete = f->edges[0].opcode_supported = true;
	f->edges[0].side_complete = f->edges[0].image_integrity_ok = true;
	f->vector.system_identifier = identity.system_identifier;
	memcpy(f->vector.storage_uuid, identity.storage_uuid, 16);
	f->vector.participant_count = f->vector.edge_count = 1;
	f->vector.cuts = f->cuts; f->vector.edges = f->edges;
	f->graph.page_identity = identity; f->graph.expected_result = result;
	f->graph.source = &f->source; f->graph.contributors = &f->vector;
	f->graph.participant_count = 1;
	/* Caller-supplied root/fence/retention booleans supply no authority. */
}

static RfPageStableCtrcBuildV1
ctrc_stable_build(CtrcStableFixture *f)
{
	return rf_page_stable_base_proof_build_ctrc_v1(&f->graph, &f->owner, 1, 1024 * 1024);
}

static bool
ctrc_stable_matches(CtrcStableFixture *f, RfPageStableBaseProofV1 *proof)
{
	return rf_page_stable_base_proof_matches_ctrc_v1(proof, &f->graph.page_identity,
		&f->graph.expected_result, &f->owner, 1, &f->source, &f->vector);
}

static void
ctrc_stable_finish(CtrcStableFixture *f)
{
	cluster_wal_inputs_release_v1(&f->inputs); test_inputs_startup_cf = false;
}

UT_TEST(test_ctrc_stable_uses_original_native_scope_and_unchanged_selector)
{
	CtrcStableFixture f;
	RfPageStableCtrcBuildV1 built;
	unsigned cf, grants;
	ctrc_stable_fixture(&f);
	cf = test_cf_lock_calls; grants = test_tx_ir_grants;
	built = ctrc_stable_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK); UT_ASSERT(built.proof != NULL);
	UT_ASSERT(test_cf_lock_calls > cf); cf = test_cf_lock_calls;
	UT_ASSERT(ctrc_stable_matches(&f, built.proof));
	UT_ASSERT_EQ(test_cf_lock_calls, cf); UT_ASSERT_EQ(test_tx_ir_grants, grants);
	UT_ASSERT(rf_page_stable_base_proof_covers_version_v1(built.proof,
		&f.graph.page_identity, &f.source.source_version));
	UT_ASSERT(!rf_page_stable_base_proof_matches_v1(built.proof, &f.graph.page_identity,
		&f.graph.expected_result, NULL, NULL, NULL, NULL, NULL, NULL,
		&f.source, &f.vector, 1));
	UT_ASSERT(!rf_page_stable_base_proof_matches_sources_v1(built.proof,
		&f.graph.page_identity, &f.graph.expected_result, NULL, 1, &f.source, &f.vector));
	rf_page_stable_base_proof_destroy_v1(&built.proof);
	UT_ASSERT(built.proof == NULL && inputs_pin_held && test_tx_ir_held);
	UT_ASSERT_EQ(test_tx_ir_releases, 0); ctrc_stable_finish(&f);
}

UT_TEST(test_ctrc_stable_refuses_forged_owner_and_uncovered_source_cut)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		CtrcStableFixture f;
		RfPageStableCtrcBuildV1 built;
		ctrc_stable_fixture(&f);
		switch (fault) {
		case 0: f.owner.ir_request_id++; break;
		case 1: f.owner.subject_index++; break;
		case 2: f.owner.source.retention.scope = (ClusterWalInputsV1 *)(uintptr_t)1; break;
		case 3: f.owner.source.selected.checkpoint.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED; break;
		case 4: f.cuts[0].origin_owner_incarnation++; break;
		case 5: f.cuts[0].timeline_id++; break;
		case 6: f.cuts[0].scan_begin_inclusive--; break;
		case 7: f.graph.page_identity.storage_uuid[0] ^= 1; break;
		}
		built = ctrc_stable_build(&f);
		UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK && built.proof == NULL);
		UT_ASSERT(inputs_pin_held && test_tx_ir_held); ctrc_stable_finish(&f);
	}
}

UT_TEST(test_ctrc_stable_checks_every_contributor_instead_of_first_owner)
{
	CtrcStableFixture f;
	ClusterRecoveryCtrcSubjectAuthorityV1 owners[2];
	RfPageStableCtrcBuildV1 built;
	ctrc_stable_fixture(&f);
	owners[0] = owners[1] = f.owner;
	owners[1].subject_index = 1;
	f.cuts[1] = f.cuts[0]; f.cuts[1].failed_thread++;
	f.cuts[1].flags = RF_CONTRIBUTOR_CUT_KNOWN_MASK;
	f.cuts[1].scan_end_exclusive = f.cuts[1].scan_begin_inclusive;
	f.cuts[1].contributor_count = f.cuts[1].component_count = 0;
	f.vector.participant_count = f.graph.participant_count = 2;
	built = rf_page_stable_base_proof_build_ctrc_v1(&f.graph, owners, 2, 1024 * 1024);
	UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK && built.proof == NULL);
	UT_ASSERT(cluster_wal_inputs_ctrc_subject_current_v1(&owners[0]));
	UT_ASSERT(!cluster_wal_inputs_ctrc_subject_current_v1(&owners[1]));
	ctrc_stable_finish(&f);
}

UT_TEST(test_ctrc_stable_preserves_graph_rejection_and_budget_before_root_io)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CtrcStableFixture f;
		RfPageStableCtrcBuildV1 built;
		unsigned cf;
		ctrc_stable_fixture(&f); cf = test_cf_lock_calls;
		if (fault == 0) {
			built = rf_page_stable_base_proof_build_ctrc_v1(&f.graph, &f.owner, 1, 0);
			UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_CAPACITY);
			UT_ASSERT_EQ(test_cf_lock_calls, cf);
		} else {
			if (fault == 1) f.edges[0].side_complete = false;
			if (fault == 2) f.edges[0].edge.edge_flags = 0;
			if (fault == 3) f.source.binding_cookie++;
			built = ctrc_stable_build(&f);
			UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK);
		}
		UT_ASSERT(built.proof == NULL && inputs_pin_held && test_tx_ir_held);
		ctrc_stable_finish(&f);
	}
}

UT_TEST(test_ctrc_stable_freezes_full_graph_and_source_bytes)
{
	CtrcStableFixture f;
	RfPageStableCtrcBuildV1 built;
	ctrc_stable_fixture(&f); built = ctrc_stable_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK);
	f.edges[0].edge.before.mutation_token++;
	UT_ASSERT(!ctrc_stable_matches(&f, built.proof));
	UT_ASSERT(rf_page_stable_base_proof_covers_version_v1(built.proof,
		&f.graph.page_identity, &f.source.source_version));
	f.edges[0].edge.before.mutation_token--;
	UT_ASSERT(ctrc_stable_matches(&f, built.proof));
	f.cuts[0].component_count++;
	UT_ASSERT(!ctrc_stable_matches(&f, built.proof)); f.cuts[0].component_count--;
	f.source.current_binding_cookie++;
	UT_ASSERT(!ctrc_stable_matches(&f, built.proof)); f.source.current_binding_cookie--;
	f.vector.cuts = (const RfContributorStreamCutV1 *)(uintptr_t)1;
	UT_ASSERT(!ctrc_stable_matches(&f, built.proof)); f.vector.cuts = f.cuts;
	UT_ASSERT(ctrc_stable_matches(&f, built.proof));
	rf_page_stable_base_proof_destroy_v1(&built.proof); ctrc_stable_finish(&f);
}

UT_TEST(test_ctrc_stable_new_native_grant_cannot_revive_old_proof)
{
	CtrcStableFixture f;
	RfPageStableCtrcBuildV1 built, next;
	ClusterRecoverySerialRequest request;
	ctrc_stable_fixture(&f); built = ctrc_stable_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK);
	ctrc_subject_reacquire_guard = f.owner.source.retention.serial;
	ctrc_subject_reacquire_id = f.owner.ir_request_id;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_release_v1(f.inputs, 0), 0);
	UT_ASSERT(!ctrc_stable_matches(&f, built.proof));
	test_tx_ir_after_grant = ctrc_subject_fresh_native_request;
	request = ctrc_ir_provisional();
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_acquire_v1(f.inputs, 0, &request), 0);
	test_tx_ir_after_grant = NULL;
	f.owner = cluster_wal_inputs_ctrc_subject_v1(f.inputs, 0);
	UT_ASSERT(!ctrc_stable_matches(&f, built.proof));
	next = ctrc_stable_build(&f); UT_ASSERT_EQ(next.detail, RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(ctrc_stable_matches(&f, next.proof));
	rf_page_stable_base_proof_destroy_v1(&built.proof);
	rf_page_stable_base_proof_destroy_v1(&next.proof); ctrc_stable_finish(&f);
}

UT_TEST(test_ctrc_stable_cf_wait_and_changed_root_keep_original_owners)
{
	CtrcStableFixture f;
	RfPageStableCtrcBuildV1 built;
	ctrc_stable_fixture(&f); test_cf_grant = false;
	built = ctrc_stable_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_WOULD_BLOCK);
	UT_ASSERT(built.proof == NULL && inputs_pin_held && test_tx_ir_held);
	test_cf_grant = true;
	UT_ASSERT(cluster_wal_inputs_ctrc_subject_current_v1(&f.owner));
	built = ctrc_stable_build(&f); UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK);
	rf_page_stable_base_proof_destroy_v1(&built.proof);
	v2_checkpoint_root_race(); built = ctrc_stable_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_ROOT_STALE);
	UT_ASSERT(built.proof == NULL && inputs_pin_held && test_tx_ir_held);
	ctrc_stable_finish(&f);
}

/* Inject at the original CF acquisition boundary. Whole-ROOT revalidation
 * selects metadata under CF and does not reread physical claim files. */
static unsigned ctrc_stable_io_fault;
static CtrcStableFixture *ctrc_stable_io_fixture;
static void
ctrc_stable_during_root_io(void)
{
	switch (ctrc_stable_io_fault) {
	case 1: v2_checkpoint_root_race(); break;
	case 2: inputs_pin_current = false; break;
	case 3: pg_re_throw(); break;
	case 4: ctrc_ir_start_current = false; break;
	case 5: ctrc_stable_io_fixture->edges[0].edge.before.mutation_token++; break;
	case 6: ctrc_stable_io_fixture->owner.work_generation++; break;
	}
}

UT_TEST(test_ctrc_stable_late_scope_loss_and_error_never_publish_proof)
{
	for (unsigned fault = 1; fault <= 6; fault++) {
		CtrcStableFixture f;
		volatile bool caught = false;
		RfPageStableCtrcBuildV1 built = {0};
		ctrc_stable_fixture(&f);
		ctrc_stable_io_fault = fault; ctrc_stable_io_fixture = &f;
		test_stop_share_hook = ctrc_stable_during_root_io;
		test_stop_share_call = test_cf_lock_calls + 1;
		PG_TRY(); { built = ctrc_stable_build(&f); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT_EQ((bool)caught, fault == 3);
		if (!caught) UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK && built.proof == NULL);
		UT_ASSERT(inputs_pin_held && test_tx_ir_held);
		test_stop_share_hook = NULL; ctrc_stable_io_fixture = NULL;
		ctrc_stable_finish(&f);
	}
}
