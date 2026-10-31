/* Actual original PAGE projection into the original canonical graph consumer.
 * PAGE/forest/authority leaves remain the existing controlled fixtures.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static bool
page_contribution_empty(const RfPageCtrcOwnerGraphV1 *out)
{
	const ClusterRecoveryCTRCGraphV2 zero = {0};
	return memcmp(&out->contribution, &zero, sizeof(zero)) == 0;
}

static bool
page_contribution_hash(const RfPageCtrcOwnerGraphV1 *out, uint8 digest[32])
{
	/* Only a valid controlled payload shape for the real graph consumer.
	 * It is never installed, accepted or used as an owner completion grant. */
	ClusterRecoveryCTRCReleaseV2 r = {.version = 2, .length = 320, .flags = 1,
		.root_id = 1, .root_generation = 2, .formation_epoch = 3, .recovery_serial = 4,
		.database_incarnation = 2, .writer_boot = 6, .covered_history = 7, .highest_fpi_end = 8,
		.cohort_bitmap = 5, .origin_node_id = 1, .writer_thread = 3, .target_owner = 2,
		.slot_offset = 5, .wrap = 9, .terminal_outcome = TT_SLOT_COMMITTED,
		.segment_id = 257, .segment_generation = 0, .xid = 0x400102};
	r.before = (TTSlot){.xid = r.xid, .wrap = r.wrap, .status = TT_SLOT_COMMITTED,
		.commit_scn = 97, .first_undo_block = {.raw = {UINT64_C(0x300000102), UINT64_C(0x1000005)}}};
	r.after = r.before; r.after.flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
	memset(r.input_sha256, 1, sizeof(r.input_sha256));
	return cluster_recovery_ctrc_graph_hash_v2(&r, 91, &out->contribution, digest);
}

UT_TEST(test_start_page_contribution_reaches_original_graph_consumer_with_whole_evidence)
{
	uint8 digest[2][32] = {{0}};
	for (unsigned change = 0; change < 2; change++) {
		ClusterRecoveryCensusReferencesV1 *refs = projected_graph_ready(true, false, false);
		ClusterRecoveryCensusReferenceV1 fixed[6]; PGAlignedBlock image;
		RfPageCtrcOwnerGraphV1 out;
		startup_image_expected.data[SizeOfPageHeaderData + 73] = 57 + change;
		memcpy(fixed, refs->entries, sizeof(fixed)); image = startup_image_expected;
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			const ClusterRecoveryCTRCGraphV2 *g = &out.contribution;
			UT_ASSERT_EQ(out.count, 6); UT_ASSERT_EQ(out.unresolved_count, 4);
			UT_ASSERT_EQ(g->node_count, 4); UT_ASSERT_EQ(g->edge_count, 2); UT_ASSERT_EQ(g->root_count, 2);
			UT_ASSERT(g->nodes != NULL && g->edges != NULL && g->roots != NULL);
			if (g->node_count == 4 && g->edge_count == 2 && g->root_count == 2
				&& g->nodes != NULL && g->edges != NULL && g->roots != NULL) {
				for (uint32 j = 0; j < 2; j++) {
					const RfPageCtrcOwnerGraphEntryV1 *entry = &out.entries[j == 0 ? 0 : 4];
					const ClusterRecoveryCTRCGraphNodeV2 *ref = &g->nodes[2 * j], *owner = ref + 1;
					UT_ASSERT_EQ(ref->kind, 1); UT_ASSERT_EQ(owner->kind, 2);
					UT_ASSERT(ref->key.data == entry->reference_key && owner->key.data == entry->key);
					UT_ASSERT_EQ(ref->key.length, entry->reference_key_length);
					UT_ASSERT_EQ(owner->key.length, entry->key_length);
					UT_ASSERT(ref->evidence.data == entry->reference_evidence && owner->evidence.data == entry->evidence);
					UT_ASSERT_EQ(ref->evidence.length, entry->reference_evidence_length);
					UT_ASSERT_EQ(owner->evidence.length, entry->evidence_length);
					UT_ASSERT_EQ(g->edges[j].kind, 4); UT_ASSERT_EQ(g->edges[j].from, 2 * j);
					UT_ASSERT_EQ(g->edges[j].to, 2 * j + 1); UT_ASSERT_EQ(g->roots[j], 2 * j);
				}
			}
			UT_ASSERT(page_contribution_hash(&out, digest[change]));
			owner_scope_free(&out);
		}
		UT_ASSERT(memcmp(fixed, refs->entries, sizeof(fixed)) == 0);
		UT_ASSERT(memcmp(&image, &startup_image_expected, sizeof(image)) == 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
		owner_scope_cleanup();
	}
	UT_ASSERT(memcmp(digest[0], digest[1], 32) != 0);
}

UT_TEST(test_start_page_contribution_keeps_unresolved_references_and_observation_optout)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterRecoveryCensusReferencesV1 *refs = projected_graph_ready(true, false, false);
		RfPageCtrcOwnerGraphV1 out;
		if (mode < 2) ClusterPageGetItlSlots((Page)startup_image_expected.data)[0].write_scn++;
		if (mode == 1) ClusterPageGetItlSlots((Page)startup_image_expected.data)[7].write_scn++;
		out = mode == 2 ? ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024)
			: ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.count, refs->count);
			for (uint32 i = 0; i < out.count; i++) UT_ASSERT_EQ(out.entries[i].reference_index, i);
			UT_ASSERT(out.related.references == refs && refs->entries[1].version == 17);
			if (mode == 0) {
				UT_ASSERT_EQ(out.contribution.node_count, 2); UT_ASSERT_EQ(out.contribution.edge_count, 1);
				UT_ASSERT_EQ(out.contribution.root_count, 1); UT_ASSERT_EQ(out.unresolved_count, 5);
				UT_ASSERT(reference_graph_unresolved(&out.entries[0]));
			} else {
				UT_ASSERT(page_contribution_empty(&out));
				UT_ASSERT_EQ(out.unresolved_count, mode == 1 ? 6 : 0);
			}
			owner_scope_free(&out);
		}
		UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
		owner_scope_cleanup();
	}
}

UT_TEST(test_start_page_contribution_deduplicates_roots_without_dropping_original_entries)
{
	uint8 digest[2][32] = {{0}};
	for (unsigned duplicate = 0; duplicate < 2; duplicate++) {
		ClusterRecoveryCensusReferencesV1 *refs = projected_graph_ready(true, false, false);
		RfPageCtrcOwnerGraphV1 out;
		if (duplicate) refs->entries[5] = refs->entries[0];
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.count, 6); UT_ASSERT_EQ(out.unresolved_count, duplicate ? 3 : 4);
			UT_ASSERT_EQ(out.contribution.node_count, duplicate ? 6 : 4);
			UT_ASSERT_EQ(out.contribution.edge_count, duplicate ? 3 : 2);
			UT_ASSERT_EQ(out.contribution.root_count, 2);
			UT_ASSERT(page_contribution_hash(&out, digest[duplicate]));
			if (duplicate) {
				UT_ASSERT_EQ(out.entries[5].reference_index, 5);
				UT_ASSERT_EQ(out.entries[5].reference_edge_kind, 4);
			}
			owner_scope_free(&out);
		}
		owner_scope_cleanup();
	}
	UT_ASSERT(memcmp(digest[0], digest[1], 32) == 0);
}

UT_TEST(test_start_page_contribution_wait_error_and_late_drift_publish_no_prefix)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		RfPageCtrcOwnerGraphV1 out = {0}; volatile bool caught = false;
		unsigned a, f;
		projected_graph_ready(true, false, false);
		if (fault < 2) owner_inventory_observe(fault == 0 ? 3 : 2);
		if (fault == 2) startup_image_observer = projected_graph_late;
		if (fault == 3) { owner_scope_late_fault = 1; startup_image_observer = owner_scope_change; }
		a = allocations; f = frees;
		PG_TRY(); { out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 1);
		UT_ASSERT(owner_scope_empty(&out) && page_contribution_empty(&out));
		UT_ASSERT_EQ(out.unresolved_count, 0); UT_ASSERT_EQ(allocations - a, frees - f);
		UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0 && observation_scope_live);
		owner_scope_cleanup();
	}
}

UT_TEST(test_start_page_contribution_whole_budget_and_oom_precede_first_page)
{
	Size low = 0, high = 1024 * 1024;
	while (low < high) {
		Size budget = low + (high - low) / 2;
		RfPageCtrcOwnerGraphV1 out;
		projected_graph_ready(true, false, false);
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, budget);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) { high = budget; owner_scope_free(&out); }
		else { UT_ASSERT(owner_scope_empty(&out) && page_contribution_empty(&out)); low = budget + 1; }
		owner_scope_cleanup();
	}
	UT_ASSERT(low > 0);
	for (unsigned fault = 0; fault < 2; fault++) {
		RfPageCtrcOwnerGraphV1 out;
		unsigned a, f;
		projected_graph_ready(true, false, false);
		if (fault == 1) allocation_ok = false;
		a = allocations; f = frees;
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, fault == 0 ? low - 1 : high);
		allocation_ok = true;
		UT_ASSERT_EQ(out.detail, fault == 0 ? RF_PAGE_PROOF_DETAIL_CAPACITY : RF_PAGE_PROOF_DETAIL_OOM);
		UT_ASSERT(owner_scope_empty(&out) && page_contribution_empty(&out));
		UT_ASSERT_EQ(out.unresolved_count, 0); UT_ASSERT_EQ(startup_image_calls, 0);
		UT_ASSERT_EQ(allocations - a, frees - f);
		UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
		owner_scope_cleanup();
	}
}
