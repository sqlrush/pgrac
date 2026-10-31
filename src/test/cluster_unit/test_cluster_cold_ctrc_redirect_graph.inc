/* Current native HOT redirects retain the original canonical tuple dependency.
 * Transport/authority are controlled leaves; native PAGE predicates execute.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static PGAlignedBlock redirect_graph_images[2];
static unsigned redirect_graph_fault;
static ClusterRecoveryCensusReferencesV1 *redirect_graph_references;

static RfPageProofDetailV1
redirect_graph_image(uint32 index, RfPageProofDetailV1 detail)
{
	if (detail != RF_PAGE_PROOF_DETAIL_OK) return detail;
	if (index >= 2) return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	startup_image_expected = redirect_graph_images[index];
	if (index == 1 && redirect_graph_fault != 0) {
		unsigned fault = redirect_graph_fault; redirect_graph_fault = 0;
		if (fault == 1) tuple_graph_bytes[0][1][64] ^= 1;
		if (fault == 2) ItemIdSetRedirect(&redirect_graph_references->entries[8].entry.item, 1);
		if (fault == 3) return RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
		if (fault == 4) pg_re_throw();
		if (fault == 5) owner_boot++;
	}
	return detail;
}

static ClusterRecoveryCensusReferencesV1 *
redirect_graph_ready(bool committed)
{
	ClusterRecoveryCensusReferencesV1 *refs = hot_graph_ready(committed);
	for (unsigned p = 0; p < 2; p++) {
		ClusterRecoveryHeapEntry *entry = &refs->entries[p * 10 + 8].entry;
		ItemId item = PageGetItemId((Page)hot_graph_images[p].data, 1);
		ItemIdSetRedirect(item, 2);
		entry->item = *item; entry->tuple_bytes = NULL; entry->tuple_length = 0;
		redirect_graph_images[p] = hot_graph_images[p];
		UT_ASSERT(test_startup_native_ctrc_heap(redirect_graph_images[p].data));
		UT_ASSERT(test_startup_native_ctrc_hot(redirect_graph_images[p].data, 23 + p));
	}
	redirect_graph_references = refs; redirect_graph_fault = 0;
	startup_image_calls = 0; startup_image_observer = redirect_graph_image;
	return refs;
}

static void
redirect_graph_cleanup(void)
{
	redirect_graph_references = NULL; redirect_graph_fault = 0;
	hot_graph_cleanup();
}

static unsigned
redirect_graph_links(const RfPageCtrcOwnerGraphV1 *out)
{
	unsigned found = 0;
	for (size_t i = 0; i < out->contribution.edge_count; i++) {
		const ClusterRecoveryCTRCGraphEdgeV2 *e = &out->contribution.edges[i];
		const ClusterRecoveryCTRCGraphNodeV2 *a = &out->contribution.nodes[e->from];
		const ClusterRecoveryCTRCGraphNodeV2 *b = &out->contribution.nodes[e->to];
		if (e->kind != 2 || a->kind != 1 || b->kind != 1
			|| cluster_undo_tt_ctrc_get_u64_le(a->key.data + a->key.length - 8) != 1
			|| cluster_undo_tt_ctrc_get_u64_le(b->key.data + b->key.length - 8) != 2) continue;
		UT_ASSERT_EQ(a->key.length, b->key.length);
		UT_ASSERT(memcmp(a->key.data, b->key.data, a->key.length - 16) == 0);
		UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(a->key.data + a->key.length - 16),
			SizeOfPageHeaderData);
		UT_ASSERT_EQ(a->evidence.length, b->evidence.length);
		UT_ASSERT(memcmp(a->evidence.data, b->evidence.data, a->evidence.length) == 0);
		found++;
	}
	return found;
}

UT_TEST(test_start_redirect_graph_keeps_actual_line_pointer_and_full_target_identity)
{
	for (unsigned committed = 0; committed < 2; committed++) {
		ClusterRecoveryCensusReferencesV1 *refs = redirect_graph_ready(committed != 0);
		ClusterRecoveryCensusReferenceV1 saved[20]; RfPageCtrcOwnerGraphV1 out; uint8 digest[32];
		memcpy(saved, refs->entries, sizeof(saved));
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.count, 20); UT_ASSERT_EQ(out.unresolved_count, 12);
			UT_ASSERT_EQ(out.contribution.node_count, 12); UT_ASSERT_EQ(out.contribution.edge_count, 8);
			UT_ASSERT_EQ(out.contribution.root_count, 8); UT_ASSERT_EQ(redirect_graph_links(&out), 2);
			for (unsigned p = 0; p < 2; p++) {
				UT_ASSERT_EQ(out.entries[p * 10 + 8].reference_edge_kind, 2);
				UT_ASSERT_EQ(out.entries[p * 10 + 8].reference_field_count, 1);
			}
			UT_ASSERT(page_contribution_hash(&out, digest)); owner_scope_free(&out);
		}
		UT_ASSERT(memcmp(saved, refs->entries, sizeof(saved)) == 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		redirect_graph_cleanup();
	}
	{
		/* A supplied redirect must not borrow an ordinary actual tuple's
		 * identity when the actual PAGE has no HOT or redirect topology. */
		ClusterRecoveryCensusReferencesV1 *refs = coverage_ready(false);
		RfPageCtrcOwnerGraphV1 out;
		ItemIdSetRedirect(&refs->entries[9].entry.item, 1);
		refs->entries[9].entry.tuple_bytes = NULL; refs->entries[9].entry.tuple_length = 0;
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.entries[9].reference_field_count, 0);
			UT_ASSERT_EQ(out.entries[9].reference_edge_kind, 0);
			owner_scope_free(&out);
		} else UT_ASSERT(owner_scope_empty(&out) && page_contribution_empty(&out));
		coverage_cleanup();
	}
}

UT_TEST(test_start_redirect_graph_coverage_resolves_topology_without_erasing_free_slots)
{
	RfPageCtrcReferenceCoverageV1 out;
	redirect_graph_ready(true);
	out = ClusterColdRecoveryCtrcPageReferenceCoverage(info.cluster_tx_start, 1024 * 1024);
	UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
	if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
		UT_ASSERT_EQ(out.count, 20); UT_ASSERT_EQ(out.no_reference_count, 12);
		UT_ASSERT_EQ(out.unresolved_count, 0); UT_ASSERT_EQ(out.uncovered_page_count, 0);
		UT_ASSERT_EQ(out.classes[8], RF_PAGE_CTRC_REFERENCE_GRAPH);
		UT_ASSERT_EQ(out.classes[18], RF_PAGE_CTRC_REFERENCE_GRAPH);
		UT_ASSERT_EQ(redirect_graph_links(&out.graph), 2); coverage_free(&out);
	}
	redirect_graph_cleanup();
}

UT_TEST(test_start_redirect_graph_missing_history_duplicate_and_changed_identity_stay_open)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterRecoveryCensusReferencesV1 *refs = redirect_graph_ready(true);
		RfPageCtrcOwnerGraphV1 out;
		if (fault == 0) refs->entries[8].version++;
		if (fault == 1) refs->entries[9].version++;
		if (fault == 2) refs->entries[9].page_index++;
		if (fault == 3) refs->entries[8].entry.index = 2;
		if (fault == 4) refs->entries[0].transaction.transaction.slot_wrap++;
		if (fault == 5) refs->entries[6].entry.itl.wrap++;
		if (fault == 6) tuple_graph_bytes[0][1][64] ^= 1;
		if (fault == 7) ItemIdSetRedirect(&refs->entries[8].entry.item, 1);
		if (fault == 8) ItemIdSetRedirect(&refs->entries[8].entry.item, 3);
		if (fault == 9) {
			HeapTupleHeaderData h;
			HeapTupleHeader actual = (HeapTupleHeader)PageGetItem((Page)redirect_graph_images[0].data,
				PageGetItemId((Page)redirect_graph_images[0].data, 2));
			memcpy(&h, refs->entries[9].entry.tuple_bytes, SizeofHeapTupleHeader);
			HeapTupleHeaderSetXminFrozen(&h); h.t_itl_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;
			memcpy((char *)refs->entries[9].entry.tuple_bytes, &h, SizeofHeapTupleHeader);
			memcpy(actual, &h, SizeofHeapTupleHeader);
			UT_ASSERT(test_startup_native_ctrc_heap(redirect_graph_images[0].data));
			UT_ASSERT(test_startup_native_ctrc_hot(redirect_graph_images[0].data, 23));
		}
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.entries[8].reference_field_count, 0);
			UT_ASSERT(redirect_graph_links(&out) <= 1);
			UT_ASSERT(out.unresolved_count != 12); owner_scope_free(&out);
		} else UT_ASSERT(owner_scope_empty(&out) && page_contribution_empty(&out));
		redirect_graph_cleanup();
	}
}

UT_TEST(test_start_redirect_graph_late_change_wait_error_keep_whole_output_and_original_debt)
{
	for (unsigned fault = 1; fault <= 5; fault++) {
		RfPageCtrcOwnerGraphV1 out = {0}; volatile bool caught = false; unsigned a, f;
		redirect_graph_ready(true); redirect_graph_fault = fault; a = allocations; f = frees;
		PG_TRY(); { out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 4); UT_ASSERT(owner_scope_empty(&out) && page_contribution_empty(&out));
		if (!owner_scope_empty(&out)) owner_scope_free(&out);
		UT_ASSERT_EQ(allocations - a, frees - f); redirect_graph_cleanup();
	}
}

UT_TEST(test_start_redirect_graph_observation_and_pre_page_capacity_keep_original_contract)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterRecoveryCensusReferencesV1 *refs = redirect_graph_ready(true); RfPageCtrcOwnerGraphV1 out;
		if (mode == 2) allocation_ok = false;
		if (mode == 3) refs->count = UINT32_MAX;
		out = mode == 0 ? ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024)
			: ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, mode == 1 ? 1 : 1024 * 1024);
		if (mode == 3) refs->count = 20;
		allocation_ok = true;
		if (mode == 0) {
			UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK); UT_ASSERT(page_contribution_empty(&out));
			UT_ASSERT_EQ(out.entries[8].reference_field_count, 0); owner_scope_free(&out);
		} else { UT_ASSERT(owner_scope_empty(&out)); UT_ASSERT_EQ(startup_image_calls, 0); }
		redirect_graph_cleanup();
	}
}
