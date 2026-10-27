/*-------------------------------------------------------------------------
 * test_cluster_control_ctrc_generations.inc
 *	 Bind the original PAGE proof to every selected retained generation.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Original ROOT/history/claim/anchor/WAL, replay-set and PAGE code execute.
 * Start/GES, shared lock scheduling and decoded graph edges are fixtures.
 *-------------------------------------------------------------------------
 */
typedef struct CtrcGenerationFixture {
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCtrcReplaySetSourceV1 owners[3];
	RfPagePinnedSourceV1 source;
	RfContributorStreamCutV1 cuts[3];
	RfPageStableEdgeInputV1 edges[3];
	RfContributorVectorV1 vector;
	RfPageStableGraphRequestV1 graph;
} CtrcGenerationFixture;

static void
ctrc_generation_fixture(CtrcGenerationFixture *f)
{
	uint8 bytes[66048], uuid[16] = {0x87};
	ClusterPhase1FullStopPlan stop; ClusterWalHistoryImage history;
	ClusterWalHistoryStage stage; ControlRootImage root;
	ClusterWalInputsV1 *original = NULL;
	RfPageVersionV1 version = {0};
	memset(f, 0, sizeof(*f));
	v2_close_history_fixture_sequenced(bytes, &stop, &history, &stage, 2, true);
	root_fixture_version3(bytes);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	/* This fixture selects a closed current generation; OPEN coverage below
	 * is exercised only after its original native census fixes the cut. */
	root.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	root.records[0].lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE;
	tx_ir_complete(&history.records[0].snapshot);
	test_actual_cf = ExclusiveLock;
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 890003, uuid, &stage), 0);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
	root.refs[0].history_generation = stage.generation;
	memcpy(root.refs[0].history_sha256, stage.sha256, 32);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
	test_actual_cf = test_cf_mode = NoLock;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0); v2_write_roots(root.bytes);
	cluster_shared_config = true; MyBackendType = B_BG_WORKER;
	inputs_pin_held = inputs_pin_refuse = inputs_pin_race = inputs_pin_throw = false;
	UT_ASSERT_EQ(cluster_wal_inputs_begin_v1(v2_storage, TEST_SYSID, &original), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_count_v1(original), 3);
	memset(&tx_inputs_typed, 0, sizeof(tx_inputs_typed));
	tx_inputs_typed.system_identifier = TEST_SYSID; tx_inputs_typed.census_root_captured = true;
	UT_ASSERT_EQ(cluster_wal_inputs_root_token_v1(original, &tx_inputs_typed.census_root), 0);
	for (uint32 i = 0; i < 3; i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(original, i);
		if (!item->current && item->checkpoint.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE) {
			ClusterColdParticipantV1 *cut = &tx_inputs_typed.participants[0];
			tx_inputs_typed.participant_count = tx_inputs_typed.replay_count = 1;
			tx_inputs_typed.sources[0] = item->source;
			cut->thread_id = item->source.claim.identity.origin_thread_id;
			cut->owner_incarnation = item->source.claim.identity.origin_owner_incarnation;
			cut->timeline = item->source.timeline;
			cut->physical_lower = item->checkpoint.checkpoint_lower_lsn;
			cut->native_redo = item->native_redo;
			cut->tail_end = item->checkpoint.validated_tail_lsn_exclusive;
		}
	}
	cluster_wal_inputs_release_v1(&original); tx_inputs_activate(); ctrc_inputs_activate_work();
	ctrc_inputs_work.counts.subject_count = ctrc_inputs_work.counts.unresolved_count = 0;
	UT_ASSERT_EQ(ctrc_inputs_begin(&f->inputs), 0);
	ctrc_ir_original_scope = f->inputs; ctrc_ir_start_current = true;
	ctrc_set_held = ctrc_set_calls = ctrc_set_release_calls = 0;
	ctrc_set_block_thread = ctrc_set_fail_release = 0; ctrc_set_after_grant = NULL;
	ctrc_set_preflight_retire = 0; ctrc_replay_current_override = ctrc_set_original_current;
	test_ctrc_set_acquire_hook = ctrc_set_native_acquire;
	test_ctrc_set_release_hook = ctrc_set_native_release;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(f->inputs), 0);
	/* Enumerate original inputs, then sort by full cut identity. Neither
	 * the replay index nor the census order substitutes for an input index. */
	for (uint32 i = 0; i < 3; i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(f->inputs, i);
		uint32 at = i;
		RfContributorStreamCutV1 cut = {0};
		ClusterRecoveryCtrcReplaySetSourceV1 owner = cluster_wal_inputs_ctrc_generation_v1(f->inputs, i);
		cut.failed_thread = item->source.claim.identity.origin_thread_id;
		cut.origin_owner_incarnation = item->source.claim.identity.origin_owner_incarnation;
		cut.timeline_id = item->source.timeline; cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cut.scan_begin_inclusive = item->checkpoint.checkpoint_lower_lsn;
		cut.scan_end_exclusive = item->checkpoint.validated_tail_lsn_exclusive;
		cut.contributor_count = cut.component_count = 1;
		while (at > 0 && rf_contributor_cut_precedes_v1(&cut, &f->cuts[at - 1])) {
			f->cuts[at] = f->cuts[at - 1]; f->owners[at] = f->owners[at - 1]; at--;
		}
		f->cuts[at] = cut; f->owners[at] = owner;
	}
	f->source.page_identity.system_identifier = TEST_SYSID;
	memcpy(f->source.page_identity.storage_uuid, v2_storage, 16);
	f->source.page_identity.locator = (RelFileLocator){1663, 5, 16392};
	f->source.page_identity.blockno = 7;
	memset(version.segment_incarnation, 0x79, 16); version.mutation_token = 41;
	f->source.source_version = version;
	f->source.binding_cookie = f->source.current_binding_cookie = 19;
	f->source.identity_verified = f->source.integrity_verified = true;
	for (uint32 i = 0; i < 3; i++) {
		RfPageStableEdgeInputV1 *edge = &f->edges[i];
		edge->page_identity = f->source.page_identity; edge->participant_index = i;
		edge->edge.page_class = RF_PAGE_CLASS_ORDINARY;
		edge->edge.before_kind = edge->edge.result_kind = RF_PAGE_STATE_PRESENT;
		edge->edge.before = version; edge->edge.before.mutation_token = 41 + i;
		memcpy(edge->edge.result_incarnation, version.segment_incarnation, 16);
		edge->edge.edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
		edge->result_token = 42 + i;
		edge->record_identity.system_identifier = TEST_SYSID;
		memcpy(edge->record_identity.storage_uuid, v2_storage, 16);
		edge->record_identity.origin_thread = f->cuts[i].failed_thread;
		edge->record_identity.timeline_id = f->cuts[i].timeline_id;
		edge->record_identity.read_rec_ptr = f->cuts[i].scan_begin_inclusive;
		edge->record_identity.end_rec_ptr = f->cuts[i].scan_end_exclusive;
		edge->record_identity.record_crc = 17 + i; edge->record_identity.rmid = RM_GENERIC_ID;
		edge->component_count = 1; memset(edge->anchor_digest, 0x39 + i, 32);
		edge->record_complete = edge->opcode_supported = true;
		edge->side_complete = edge->image_integrity_ok = true;
	}
	f->vector.cuts = f->cuts; f->vector.edges = f->edges;
	f->vector.participant_count = f->vector.edge_count = 3;
	f->vector.system_identifier = TEST_SYSID;
	memcpy(f->vector.storage_uuid, v2_storage, 16);
	f->graph.page_identity = f->source.page_identity;
	f->graph.expected_result = version; f->graph.expected_result.mutation_token = 44;
	f->graph.source = &f->source; f->graph.contributors = &f->vector; f->graph.participant_count = 3;
}

static RfPageStableCtrcBuildV1
ctrc_generation_build(CtrcGenerationFixture *f)
{
	return rf_page_stable_base_proof_build_ctrc_replays_v1(&f->graph, f->owners, 3, 1024 * 1024);
}

static bool
ctrc_generation_matches(CtrcGenerationFixture *f, const RfPageStableBaseProofV1 *proof)
{
	return rf_page_stable_base_proof_matches_ctrc_replays_v1(proof,
		&f->graph.page_identity, &f->graph.expected_result, f->owners, 3, &f->source, &f->vector);
}

UT_TEST(test_ctrc_generations_bind_all_history_and_current_to_one_original_ir)
{
	CtrcGenerationFixture f; RfPageStableCtrcBuildV1 built;
	ClusterRecoveryCtrcReplaySetSourceV1 original; unsigned current = 0, history = 0;
	ctrc_generation_fixture(&f);
	original = cluster_wal_inputs_ctrc_replays_source_v1(f.inputs, 0);
	UT_ASSERT_EQ(original.replay_count, 1); UT_ASSERT_EQ(original.generation_count, 0);
	for (unsigned i = 0; i < 3; i++) {
		UT_ASSERT(cluster_wal_inputs_ctrc_replays_source_current_v1(&f.owners[i]));
		UT_ASSERT_EQ(f.owners[i].generation_count, 3);
		UT_ASSERT_EQ(f.owners[i].ir_request_id, original.ir_request_id);
		UT_ASSERT(memcmp(&f.owners[i].selected, &original.selected, sizeof(original.selected)) == 0);
		if (f.owners[i].generation.current) current++; else history++;
	}
	UT_ASSERT_EQ(current, 1); UT_ASSERT_EQ(history, 2); UT_ASSERT_EQ(ctrc_set_held, 1);
	UT_ASSERT_EQ(f.owners[0].generation_index + f.owners[1].generation_index
		+ f.owners[2].generation_index, 3);
	built = ctrc_generation_build(&f); UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(ctrc_generation_matches(&f, built.proof));
	rf_page_stable_base_proof_destroy_v1(&built.proof); ctrc_set_finish(&f.inputs);
}

UT_TEST(test_ctrc_generations_reject_omitted_duplicate_or_mixed_generation)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		CtrcGenerationFixture f; RfPageStableCtrcBuildV1 built;
		ctrc_generation_fixture(&f);
		if (fault == 0) {
			f.graph.participant_count = f.vector.participant_count = 2; f.vector.edge_count = 2;
			built = rf_page_stable_base_proof_build_ctrc_replays_v1(&f.graph, f.owners, 2, 1024 * 1024);
		} else {
			f.owners[2] = fault == 1 ? f.owners[0] : cluster_wal_inputs_ctrc_replays_source_v1(f.inputs, 0);
			built = ctrc_generation_build(&f);
		}
		UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK && built.proof == NULL);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 1); ctrc_set_finish(&f.inputs);
	}
}

UT_TEST(test_ctrc_generations_freeze_full_input_and_reject_changed_cut)
{
	CtrcGenerationFixture f; RfPageStableCtrcBuildV1 built;
	ctrc_generation_fixture(&f); built = ctrc_generation_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK);
	for (Size i = 0; i < sizeof(f.owners); i++) {
		uint8 *byte = (uint8 *)f.owners + i;
		*byte ^= 1; UT_ASSERT(!ctrc_generation_matches(&f, built.proof)); *byte ^= 1;
	}
	UT_ASSERT(ctrc_generation_matches(&f, built.proof));
	f.cuts[1].scan_begin_inclusive++;
	UT_ASSERT(!ctrc_generation_matches(&f, built.proof));
	rf_page_stable_base_proof_destroy_v1(&built.proof);
	built = ctrc_generation_build(&f); UT_ASSERT(built.proof == NULL);
	ctrc_set_finish(&f.inputs);
}

UT_TEST(test_ctrc_generations_promote_original_page_without_new_ir)
{
	CtrcGenerationFixture f; RfPageStableCtrcBuildV1 built;
	RfPageAuthorityTargetV1 target = {0}; RfPageCtrcAuthorityBuildV1 page;
	RfPageCtrcReferenceV1 reference; unsigned calls;
	ctrc_generation_fixture(&f); rf_page_guard_shmem_init_v1();
	built = ctrc_generation_build(&f); UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK);
	target.page_identity = f.graph.page_identity; target.expected_result = f.graph.expected_result;
	target.stable_base = built.proof; target.source = &f.source; target.contributors = &f.vector;
	calls = ctrc_set_calls;
	page = rf_page_authority_ctrc_replays_preflight_v1(&target, 1, f.owners, 3, 1024 * 1024);
	UT_ASSERT_EQ(page.verdict, RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(page.authority), 0);
	for (unsigned i = 0; i < 3; i++) {
		reference = rf_page_authority_ctrc_replays_reference_nowait_v1(page.authority,
			&f.owners[i], &target.page_identity, &target.expected_result);
		UT_ASSERT(reference.ready);
	}
	UT_ASSERT_EQ(ctrc_set_calls, calls); UT_ASSERT_EQ(ctrc_set_held, 1);
	inputs_pin_current = false;
	UT_ASSERT(!rf_page_authority_ctrc_replays_covers_nowait_v1(page.authority,
		&target.page_identity, &target.expected_result));
	rf_page_authority_ctrc_replays_destroy_v1(&page.authority);
	rf_page_stable_base_proof_destroy_v1(&built.proof);
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 1); ctrc_set_finish(&f.inputs);
}

UT_TEST(test_ctrc_generations_cf_wait_and_retirement_keep_original_debt)
{
	CtrcGenerationFixture f; RfPageStableCtrcBuildV1 built; unsigned cf;
	ctrc_generation_fixture(&f); cf = test_cf_lock_calls;
	built = rf_page_stable_base_proof_build_ctrc_replays_v1(&f.graph, f.owners, 3, 0);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_CAPACITY); UT_ASSERT_EQ(test_cf_lock_calls, cf);
	test_cf_grant = false; built = ctrc_generation_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_WOULD_BLOCK); UT_ASSERT(built.proof == NULL);
	test_cf_grant = true; built = ctrc_generation_build(&f);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK);
	ctrc_set_finish(&f.inputs);
	UT_ASSERT(!ctrc_generation_matches(&f, built.proof));
	for (unsigned i = 0; i < 3; i++)
		UT_ASSERT(!cluster_wal_inputs_ctrc_replays_source_current_v1(&f.owners[i]));
	rf_page_stable_base_proof_destroy_v1(&built.proof);
}

static void
ctrc_generation_set_activate(ClusterWalInputsV1 *inputs)
{
	ctrc_ir_original_scope = inputs; ctrc_ir_start_current = true;
	ctrc_set_held = ctrc_set_calls = ctrc_set_release_calls = 0;
	ctrc_set_block_thread = ctrc_set_fail_release = 0; ctrc_set_after_grant = NULL;
	ctrc_set_preflight_retire = 0; ctrc_replay_current_override = ctrc_set_original_current;
	test_ctrc_set_acquire_hook = ctrc_set_native_acquire;
	test_ctrc_set_release_hook = ctrc_set_native_release;
}

UT_TEST(test_ctrc_generations_local_open_requires_original_fixed_native_cut)
{
	ControlRootImage root; ControlFileData common; ClusterControlRootFileToken token;
	ClusterWalInputsV1 *inputs = NULL; CtrcLocalCensus scan = {0};
	ClusterRecoveryCtrcReplaySetSourceV1 source, fixed;
	uint32 local = UINT32_MAX; uint64 records; unsigned flushes;
	ctrc_local_fixture(false);
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID,
		&root, &common, &token), 0);
	for (unsigned node = 0; node <= 3; node += 3) {
		ClusterWalHistoryImage history; ClusterWalHistoryStage stage; uint8 uuid[16] = {0x6a};
		UT_ASSERT_EQ(cluster_wal_history_read_locked(&root, node, &history), 0);
		UT_ASSERT_EQ(history.count, 1); tx_ir_complete(&history.records[0].snapshot);
		test_actual_cf = test_cf_mode = ExclusiveLock; uuid[1] = node;
		UT_ASSERT_EQ(cluster_wal_history_prepare(&root, node, &history,
			root.refs[node].history_generation + 11000, uuid, &stage), 0);
		UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
		root.refs[node].history_generation = stage.generation;
		memcpy(root.refs[node].history_sha256, stage.sha256, 32);
		UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
	}
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0); v2_write_roots(root.bytes);
	test_actual_cf = test_cf_mode = NoLock;
	test_insert = test_flush = ctrc_local_append(ctrc_local_checkpoint, ctrc_local_checkpoint_end);
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0); ctrc_generation_set_activate(inputs);
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	for (uint32 i = 0; i < cluster_wal_inputs_count_v1(inputs); i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(inputs, i);
		if (item->current && item->source.claim.identity.origin_node_id == 0) local = i;
	}
	UT_ASSERT(local != UINT32_MAX);
	source = cluster_wal_inputs_ctrc_generation_v1(inputs, local);
	UT_ASSERT(source.scope == NULL); flushes = ctrc_local_flushes;
	UT_ASSERT_EQ(ctrc_local_scan(inputs, &scan, &records), 0);
	fixed = cluster_wal_inputs_ctrc_generation_v1(inputs, local);
	UT_ASSERT(cluster_wal_inputs_ctrc_replays_source_current_v1(&fixed));
	UT_ASSERT_EQ(fixed.generation_end, scan.local_end);
	UT_ASSERT_EQ(fixed.generation_count, 4);
	test_insert = ctrc_local_append(test_insert - 40, test_insert);
	source = cluster_wal_inputs_ctrc_generation_v1(inputs, local);
	UT_ASSERT(memcmp(&fixed, &source, sizeof(source)) == 0);
	UT_ASSERT_EQ(ctrc_local_flushes, flushes);
	UT_ASSERT(cluster_wal_inputs_ctrc_replays_source_covers_window_v1(&source,
		source.generation.source.claim.identity.origin_thread_id,
		source.generation_begin, source.generation_end));
	inputs_native_writer.epoch++;
	UT_ASSERT(!cluster_wal_inputs_ctrc_replays_source_current_v1(&fixed));
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 2); ctrc_set_finish(&inputs);
	test_checkpoint_mode = false;
}

UT_TEST(test_ctrc_generations_initializer_terminal_never_bypasses_original_ir)
{
	ClusterRecoverySerialGuard serial; ClusterThreadRecLaunchEligibility eligibility;
	ClusterWalStartupImage op = pending_worker_fixture(&serial, &eligibility);
	ClusterWalInputsV1 *original = NULL, *inputs = NULL;
	ControlRootImage root; ControlFileData common; ClusterControlRootFileToken token;
	ClusterRecoveryCtrcReplaySetSourceV1 source; uint32 terminal = UINT32_MAX;
	UT_ASSERT_EQ(op.claim.identity.origin_thread_id, 1);
	test_worker_pin_held = false; test_history_sync_count = 0; test_history_fail_sync = UINT_MAX;
	UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), CLUSTER_THREADREC_DONE);
	test_history_fail_sync = 0; MyBackendType = B_BG_WORKER;
	test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID,
		&root, &common, &token), 0);
	memset(&root.startup[3], 0, sizeof(root.startup[3]));
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0); v2_write_roots(root.bytes);
	test_actual_cf = test_cf_mode = NoLock;
	UT_ASSERT_EQ(inputs_begin_for_role(false, &original), 0);
	memset(&tx_inputs_typed, 0, sizeof(tx_inputs_typed));
	tx_inputs_typed.system_identifier = TEST_SYSID; tx_inputs_typed.census_root_captured = true;
	UT_ASSERT_EQ(cluster_wal_inputs_root_token_v1(original, &tx_inputs_typed.census_root), 0);
	for (uint32 i = 0; i < cluster_wal_inputs_count_v1(original); i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(original, i);
		ClusterColdParticipantV1 *cut; uint32 p;
		if (item->kind != CLUSTER_WAL_INPUT_CHECKPOINT) { terminal = i; continue; }
		p = tx_inputs_typed.participant_count++;
		tx_inputs_typed.sources[p] = item->source; cut = &tx_inputs_typed.participants[p];
		cut->thread_id = item->source.claim.identity.origin_thread_id;
		cut->owner_incarnation = item->source.claim.identity.origin_owner_incarnation;
		cut->timeline = item->source.timeline;
		cut->physical_lower = item->checkpoint.checkpoint_lower_lsn;
		cut->native_redo = item->native_redo;
		cut->tail_end = item->checkpoint.validated_tail_lsn_exclusive;
	}
	tx_inputs_typed.replay_count = tx_inputs_typed.participant_count;
	UT_ASSERT(terminal != UINT32_MAX);
	cluster_wal_inputs_release_v1(&original); tx_inputs_activate(); ctrc_inputs_activate_work();
	ctrc_inputs_work.counts.subject_count = ctrc_inputs_work.counts.unresolved_count = 0;
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0); ctrc_generation_set_activate(inputs);
	/* The authentic empty terminal is retained input, not a COMPLETE replay
	 * owner. Its uncompleted predecessor cannot grant a set or PAGE view. */
	{
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(inputs, terminal);
		UT_ASSERT(item != NULL);
		if (item != NULL) {
			UT_ASSERT_EQ(item->kind, CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL);
			UT_ASSERT_EQ(item->terminal.tail.records, 0);
		}
	}
	UT_ASSERT(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs) != 0);
	source = cluster_wal_inputs_ctrc_generation_v1(inputs, terminal);
	UT_ASSERT(v2_zero(&source, sizeof(source)));
	UT_ASSERT(inputs_pin_held && ctrc_set_held == 0); ctrc_set_finish(&inputs);
}
