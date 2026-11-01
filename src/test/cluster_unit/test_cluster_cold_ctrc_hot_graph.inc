/* Complete current HOT chains borrow original native predicates and PAGE
 * owners. PAGE transport/forest/authority remain controlled fixture leaves.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static PGAlignedBlock hot_graph_images[2];
static unsigned hot_graph_late_fault;

static RfPageProofDetailV1
hot_graph_image(uint32 index, RfPageProofDetailV1 detail)
{
	if (detail != RF_PAGE_PROOF_DETAIL_OK) return detail;
	if (index >= 2) return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	startup_image_expected = hot_graph_images[index];
	if (index == 1 && hot_graph_late_fault != 0) {
		unsigned fault = hot_graph_late_fault; hot_graph_late_fault = 0;
		if (fault == 1) tuple_graph_bytes[0][1][64] ^= 1;
		if (fault == 2) return RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
		if (fault == 3) pg_re_throw();
		if (fault == 4) owner_boot++;
	}
	return detail;
}

static ClusterRecoveryCensusReferencesV1 *
hot_graph_ready(bool committed)
{
	ClusterRecoveryCensusReferencesV1 *refs = tuple_graph_ready(committed);
	ClusterColdTtSlotInputV1 terminal;
	PGAlignedBlock before = startup_image_expected;
	UT_ASSERT(cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
		&native_ctrc_work, 0, &observation_physical[0], &terminal));
	for (unsigned p = 0; p < 2; p++) {
		Page page = (Page)before.data;
		for (unsigned s = 0; s < 8; s++) ClusterPageGetItlSlots(page)[s] = refs->entries[p * 10 + s].entry.itl;
		for (unsigned item = 0; item < 2; item++) {
			ClusterRecoveryHeapEntry *entry = &refs->entries[p * 10 + 8 + item].entry;
			HeapTupleHeader tuple = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, item + 1));
			HeapTupleHeaderSetXmin(tuple, terminal.key.xid);
			HeapTupleHeaderSetXmax(tuple, item == 0 ? terminal.key.xid : InvalidTransactionId);
			tuple->t_itl_slot_idx = 0;
			tuple->t_infomask = item == 0 ? 0 : HEAP_XMAX_INVALID | HEAP_UPDATED;
			tuple->t_infomask2 = item == 0 ? HEAP_HOT_UPDATED : HEAP_ONLY_TUPLE;
			ItemPointerSet(&tuple->t_ctid, 23 + p, 2);
			memcpy((char *)entry->tuple_bytes, tuple, entry->tuple_length);
		}
		UT_ASSERT_EQ(test_startup_native_ctrc_project(&terminal, 1, page, hot_graph_images[p].data),
			CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN);
		UT_ASSERT(test_startup_native_ctrc_heap(hot_graph_images[p].data));
		UT_ASSERT(test_startup_native_ctrc_hot(hot_graph_images[p].data, 23 + p));
	}
	hot_graph_late_fault = 0; startup_image_calls = 0; startup_image_observer = hot_graph_image;
	return refs;
}

static void
hot_graph_cleanup(void)
{
	hot_graph_late_fault = 0; startup_image_observer = NULL; tuple_graph_cleanup();
}

static unsigned
hot_graph_links(const RfPageCtrcOwnerGraphV1 *out)
{
	unsigned count = 0;
	for (Size i = 0; i < out->contribution.edge_count; i++) {
		const ClusterRecoveryCTRCGraphEdgeV2 *edge = &out->contribution.edges[i];
		const ClusterRecoveryCTRCGraphNodeV2 *a = &out->contribution.nodes[edge->from];
		const ClusterRecoveryCTRCGraphNodeV2 *b = &out->contribution.nodes[edge->to];
		if (edge->kind == 2 && a->kind == 1 && b->kind == 1
			&& cluster_undo_tt_ctrc_get_u64_le(a->key.data + a->key.length - 8) == 1
			&& cluster_undo_tt_ctrc_get_u64_le(b->key.data + b->key.length - 8) == 2) {
			UT_ASSERT(memcmp(a->key.data, b->key.data, 34) == 0);
			UT_ASSERT_EQ(a->key.length, b->key.length);
			UT_ASSERT(memcmp(a->key.data + 34, b->key.data + 34, a->key.length - 50) == 0);
			count++;
		}
	}
	return count;
}

UT_TEST(test_start_hot_graph_binds_complete_native_current_chains)
{
	for (unsigned committed = 0; committed < 2; committed++) {
		ClusterRecoveryCensusReferencesV1 *refs = hot_graph_ready(committed != 0);
		ClusterRecoveryCensusReferenceV1 fixed[20]; char payloads[sizeof(tuple_graph_bytes)];
		RfPageCtrcOwnerGraphV1 out; uint8 digest[32];
		memcpy(fixed, refs->entries, sizeof(fixed)); memcpy(payloads, tuple_graph_bytes, sizeof(payloads));
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.count, 20); UT_ASSERT_EQ(out.unresolved_count, 12);
			UT_ASSERT_EQ(out.contribution.node_count, 14); UT_ASSERT_EQ(out.contribution.edge_count, 12);
			UT_ASSERT_EQ(hot_graph_links(&out), 2);
			for (unsigned p = 0; p < 2; p++) {
				UT_ASSERT_EQ(out.entries[p * 10 + 8].reference_field_count, 2);
				UT_ASSERT_EQ(out.entries[p * 10 + 9].reference_field_count, 1);
			}
			UT_ASSERT(page_contribution_hash(&out, digest)); owner_scope_free(&out);
		}
		UT_ASSERT(memcmp(fixed, refs->entries, sizeof(fixed)) == 0);
		UT_ASSERT(memcmp(payloads, tuple_graph_bytes, sizeof(payloads)) == 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		hot_graph_cleanup();
	}
}

UT_TEST(test_start_hot_graph_missing_or_changed_member_keeps_entire_chain)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterRecoveryCensusReferencesV1 *refs = hot_graph_ready(true);
		ClusterRecoveryHeapEntry *item = &refs->entries[9].entry;
		char ordinary[65];
		HeapTupleHeaderData h; RfPageCtrcOwnerGraphV1 out;
		memcpy(&h, item->tuple_bytes, SizeofHeapTupleHeader);
		if (fault == 0) refs->entries[9].version++;
		if (fault == 1) refs->entries[9].entry.index = 1;
		if (fault == 2) tuple_graph_bytes[0][1][64] ^= 1;
		if (fault == 3) h.t_infomask2 &= ~HEAP_ONLY_TUPLE;
		if (fault == 4) h.t_hoff++;
		if (fault == 5) HeapTupleHeaderSetXmin(&h, native_ctrc_work.entries[0].key.xid + 1);
		if (fault == 6) refs->entries[7].entry.itl.wrap++;
		if (fault == 7) {
			/* Both HOT members disappear from the current view, but an
			 * ordinary tuple remains on that actual HOT page. */
			Page page = (Page)hot_graph_images[0].data;
			PageHeader header = (PageHeader)page;
			HeapTupleHeader tuple;
			refs->entries[8].version = refs->entries[9].version = 1;
			header->pd_lower += sizeof(ItemIdData); header->pd_upper -= 64;
			ItemIdSetNormal(PageGetItemId(page, 3), header->pd_upper, 64);
			tuple = (HeapTupleHeader)(page + header->pd_upper);
			memset(tuple, 0, 64); tuple->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
			tuple->t_infomask = HEAP_XMAX_INVALID; tuple->t_itl_slot_idx = 0;
			HeapTupleHeaderSetXmin(tuple, native_ctrc_work.entries[0].key.xid);
			ItemPointerSet(&tuple->t_ctid, 23, 3);
			refs->entries[20] = refs->entries[8]; refs->entries[20].version = 0;
			refs->entries[20].entry.index = 3; refs->entries[20].entry.item = *PageGetItemId(page, 3);
			refs->entries[20].entry.tuple_bytes = ordinary + 1;
			memcpy(ordinary + 1, tuple, 64); refs->count = 21;
			UT_ASSERT(test_startup_native_ctrc_hot(page, 23));
		}
		if (fault >= 3 && fault <= 5) memcpy((char *)item->tuple_bytes, &h, SizeofHeapTupleHeader);
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.entries[8].reference_field_count, 0);
			UT_ASSERT_EQ(out.entries[9].reference_field_count, 0);
			UT_ASSERT_EQ(out.entries[18].reference_field_count, 2);
			UT_ASSERT_EQ(out.entries[19].reference_field_count, 1);
			if (fault == 7) UT_ASSERT_EQ(out.entries[20].reference_field_count, 0);
			UT_ASSERT_EQ(hot_graph_links(&out), 1); owner_scope_free(&out);
		} else UT_ASSERT(owner_scope_empty(&out) && page_contribution_empty(&out));
		hot_graph_cleanup();
	}
}

UT_TEST(test_start_hot_graph_late_change_wait_error_preserves_whole_debt)
{
	for (unsigned fault = 1; fault <= 4; fault++) {
		RfPageCtrcOwnerGraphV1 out = {0}; volatile bool caught = false; unsigned a, f;
		hot_graph_ready(true); hot_graph_late_fault = fault; a = allocations; f = frees;
		PG_TRY(); { out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 3); UT_ASSERT(owner_scope_empty(&out) && page_contribution_empty(&out));
		if (!owner_scope_empty(&out)) owner_scope_free(&out);
		UT_ASSERT_EQ(allocations - a, frees - f); hot_graph_cleanup();
	}
}

UT_TEST(test_start_hot_graph_budget_observation_and_history_keep_original_gates)
{
	for (unsigned mode = 0; mode < 5; mode++) {
		ClusterRecoveryCensusReferencesV1 *refs = hot_graph_ready(true); RfPageCtrcOwnerGraphV1 out;
		if (mode == 1) refs->entries[9].version = 1;
		if (mode == 3) allocation_ok = false;
		if (mode == 4) refs->count = UINT32_MAX;
		out = mode == 0 ? ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024)
			: ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, mode == 2 ? 1 : 1024 * 1024);
		if (mode == 4) refs->count = 20;
		allocation_ok = true;
		if (mode <= 1) {
			UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
			if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
				if (mode == 0) UT_ASSERT(page_contribution_empty(&out));
				else { UT_ASSERT_EQ(out.entries[8].reference_field_count, 0); UT_ASSERT_EQ(hot_graph_links(&out), 1); }
				owner_scope_free(&out);
			}
		} else { UT_ASSERT(owner_scope_empty(&out)); UT_ASSERT_EQ(startup_image_calls, 0); }
		hot_graph_cleanup();
	}
}
