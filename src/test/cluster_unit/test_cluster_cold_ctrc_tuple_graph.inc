/* Current tuple fields use the complete original native tuple/page predicates.
 * PAGE transport/forest/authority remain controlled, not graph admission.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static char tuple_graph_bytes[2][2][65];
static unsigned tuple_graph_late_fault;

static ClusterRecoveryCensusReferencesV1 *
tuple_graph_ready(bool committed)
{
	ClusterRecoveryCensusReferencesV1 *refs = projected_graph_ready(committed, false, false);
	ClusterRecoveryCensusReferenceV1 base = refs->entries[0];
	ClusterColdTtSlotInputV1 subject;
	PGAlignedBlock before;
	Page page = (Page)before.data;
	PageHeader h = (PageHeader)page;

	UT_ASSERT(cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
		&native_ctrc_work, 0, &observation_physical[0], &subject));
	memset(&before, 0, sizeof(before));
	h->pd_flags = PD_HAS_ITL; h->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	h->pd_lower = SizeOfPageHeaderData + 2 * sizeof(ItemIdData);
	h->pd_upper = h->pd_special - 128;
	PageSetPageSizeAndVersion(page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	ClusterPageGetItlSlots(page)[0] = base.entry.itl;
	ClusterPageGetItlSlots(page)[7] = base.entry.itl;
	ClusterPageGetItlSlots(page)[7].flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	ClusterPageGetItlSlots(page)[7].wrap++;
	for (unsigned item = 0; item < 2; item++) {
		HeapTupleHeader tuple = (HeapTupleHeader)(page + h->pd_upper + item * 64);
		ItemIdSetNormal(PageGetItemId(page, item + 1), h->pd_upper + item * 64, 64);
		tuple->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
		HeapTupleHeaderSetXmin(tuple, item == 0 ? base.entry.itl.xid : FrozenTransactionId);
		HeapTupleHeaderSetXmax(tuple, base.entry.itl.xid);
		tuple->t_itl_slot_idx = item == 0 ? 0 : CLUSTER_ITL_SLOT_UNALLOCATED;
		if (item == 1) tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
		memset((char *)tuple + tuple->t_hoff, 73 + item, 64 - tuple->t_hoff);
	}
	memset(refs->entries, 0, 20 * sizeof(refs->entries[0])); refs->count = 20;
	for (uint32 p = 0; p < 2; p++) {
		for (uint32 slot = 0; slot < 8; slot++) {
			ClusterRecoveryCensusReferenceV1 *r = &refs->entries[p * 10 + slot];
			r->page_index = p; r->entry.kind = CLUSTER_RECOVERY_HEAP_ITL; r->entry.index = slot;
			r->entry.itl = ClusterPageGetItlSlots(page)[slot];
			if (slot == 0 || slot == 7) r->transaction = base.transaction;
		}
		for (uint32 item = 0; item < 2; item++) {
			ClusterRecoveryCensusReferenceV1 *r = &refs->entries[p * 10 + 8 + item];
			r->page_index = p; r->entry.kind = CLUSTER_RECOVERY_HEAP_ITEM; r->entry.index = item + 1;
			r->entry.item = *PageGetItemId(page, item + 1); r->entry.tuple_length = 64;
			/* Intentionally packed/unaligned original census payload. */
			r->entry.tuple_bytes = tuple_graph_bytes[p][item] + 1;
			memcpy((char *)r->entry.tuple_bytes, page + ItemIdGetOffset(&r->entry.item), 64);
		}
	}
	UT_ASSERT_EQ(test_startup_native_ctrc_project(&subject, 1, page, startup_image_expected.data),
		CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN);
	startup_native_images = reference_tuple_native = true;
	return refs;
}

static void
tuple_graph_cleanup(void)
{
	startup_native_images = reference_tuple_native = false;
	reference_tuple_after_native = NULL;
	UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0 && observation_scope_live);
	owner_scope_cleanup();
}

UT_TEST(test_start_tuple_graph_uses_complete_native_tuple_and_original_itl_identity)
{
	for (unsigned committed = 0; committed < 2; committed++) {
		ClusterRecoveryCensusReferencesV1 *refs = tuple_graph_ready(committed);
		ClusterRecoveryCensusReferenceV1 saved[20]; PGAlignedBlock image = startup_image_expected;
		char payloads[sizeof(tuple_graph_bytes)]; uint8 digest[32];
		RfPageCtrcOwnerGraphV1 out;
		memcpy(saved, refs->entries, sizeof(saved)); memcpy(payloads, tuple_graph_bytes, sizeof(payloads));
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			const ClusterRecoveryCTRCGraphV2 *g = &out.contribution;
			UT_ASSERT_EQ(out.count, 20); UT_ASSERT_EQ(out.unresolved_count, 12);
			UT_ASSERT_EQ(g->node_count, 14); UT_ASSERT_EQ(g->edge_count, 10); UT_ASSERT_EQ(g->root_count, 10);
			for (uint32 p = 0; p < 2; p++) {
				UT_ASSERT_EQ(out.entries[p * 10].reference_field_count, 1);
				UT_ASSERT_EQ(out.entries[p * 10 + 7].reference_field_count, 1);
				UT_ASSERT_EQ(out.entries[p * 10 + 8].reference_field_count, 2);
				UT_ASSERT_EQ(out.entries[p * 10 + 9].reference_field_count, 1);
			}
			if (g->node_count == 14 && g->edge_count == 10) {
				for (size_t e = 4; e < g->edge_count; e++) {
					const ClusterRecoveryCTRCGraphEdgeV2 *edge = &g->edges[e];
					const ClusterRecoveryCTRCGraphNodeV2 *field = &g->nodes[edge->from];
					const ClusterRecoveryCTRCGraphNodeV2 *itl = &g->nodes[edge->to];
					Size id = cluster_undo_tt_ctrc_get_u64_le(field->key.data + 36);
					unsigned item = (e - 4) % 3 < 2 ? 1 : 2;
					Size relative = (e - 4) % 3 == 0 ? offsetof(HeapTupleHeaderData, t_choice.t_heap.t_xmin)
						: offsetof(HeapTupleHeaderData, t_choice.t_heap.t_xmax);
					UT_ASSERT_EQ(edge->kind, 2); UT_ASSERT_EQ(field->kind, 1); UT_ASSERT_EQ(itl->kind, 1);
					UT_ASSERT(memcmp(field->key.data, itl->key.data, 34) == 0);
					UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(field->key.data + 44 + id),
						ItemIdGetOffset(PageGetItemId((Page)image.data, item)) + relative);
					UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(field->key.data + 52 + id), item);
					UT_ASSERT(memcmp(field->evidence.data + field->evidence.length - BLCKSZ, image.data, BLCKSZ) == 0);
				}
			}
			UT_ASSERT(page_contribution_hash(&out, digest)); owner_scope_free(&out);
		}
		UT_ASSERT(memcmp(saved, refs->entries, sizeof(saved)) == 0);
		UT_ASSERT(memcmp(payloads, tuple_graph_bytes, sizeof(payloads)) == 0);
		UT_ASSERT(memcmp(&image, &startup_image_expected, sizeof(image)) == 0);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		tuple_graph_cleanup();
	}
}

UT_TEST(test_start_tuple_graph_missing_ambiguous_and_unproved_itls_keep_whole_item)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterRecoveryCensusReferencesV1 *refs = tuple_graph_ready(true);
		RfPageCtrcOwnerGraphV1 out;
		if (fault == 0) refs->entries[6].version++;
		if (fault == 1) refs->entries[6].page_index = 1;
		if (fault == 2) refs->entries[6].entry.index = 5;
		if (fault == 3) { refs->entries[6] = refs->entries[0]; refs->entries[6].entry.index = 6; }
		if (fault == 4) refs->entries[0].transaction.transaction.segment_generation++;
		if (fault == 5) refs->entries[7].entry.itl.wrap++;
		if (fault == 6) refs->entries[0].transaction.transaction.slot_wrap++;
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.entries[8].reference_field_count, 0);
			UT_ASSERT_EQ(out.entries[9].reference_field_count, 0);
			/* Moving the slot into page 1 also makes that page's set duplicate. */
			UT_ASSERT_EQ(out.entries[18].reference_field_count, fault == 1 ? 0 : 2);
			UT_ASSERT_EQ(out.entries[19].reference_field_count, fault == 1 ? 0 : 1); owner_scope_free(&out);
		} else UT_ASSERT(owner_scope_empty(&out));
		tuple_graph_cleanup();
	}
}

UT_TEST(test_start_tuple_graph_rejects_changed_bytes_layout_and_unsupported_fields)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterRecoveryCensusReferencesV1 *refs = tuple_graph_ready(true);
		ClusterRecoveryHeapEntry *item = &refs->entries[8].entry;
		HeapTupleHeaderData h; RfPageCtrcOwnerGraphV1 out;
		memcpy(&h, item->tuple_bytes, SizeofHeapTupleHeader);
		if (fault == 0) tuple_graph_bytes[0][0][64] ^= 1;
		if (fault == 1) item->item.lp_off -= MAXALIGN(8);
		if (fault == 2) item->tuple_length--;
		if (fault == 3) h.t_infomask |= HEAP_XMAX_IS_MULTI;
		if (fault == 4) h.t_infomask2 |= HEAP_HOT_UPDATED;
		if (fault == 5) h.t_itl_slot_idx = 7;
		if (fault == 6) h.t_hoff++;
		if (fault >= 3) memcpy((char *)item->tuple_bytes, &h, SizeofHeapTupleHeader);
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.entries[8].reference_field_count, 0);
			UT_ASSERT_EQ(out.entries[18].reference_field_count, 2); owner_scope_free(&out);
		} else UT_ASSERT(owner_scope_empty(&out));
		tuple_graph_cleanup();
	}
}

static RfPageProofDetailV1
tuple_graph_late(uint32 index, RfPageProofDetailV1 detail)
{
	if (index == 1 && detail == RF_PAGE_PROOF_DETAIL_OK) {
		ClusterRecoveryCensusReferencesV1 *refs = (void *)observation_references_storage[0].bytes;
		if (tuple_graph_late_fault == 0) tuple_graph_bytes[0][0][64] ^= 1;
		if (tuple_graph_late_fault == 1) refs->entries[8].entry.tuple_bytes = tuple_graph_bytes[1][0] + 1;
		if (tuple_graph_late_fault == 2) return projected_graph_late(index, detail);
	}
	return detail;
}

static void
tuple_graph_after_native(void)
{
	/* Drift after the PAGE snapshot, while the real tuple predicate returns. */
	reference_tuple_after_native = NULL;
	tuple_graph_bytes[0][0][64] ^= 1;
}

UT_TEST(test_start_tuple_graph_late_payload_and_wait_error_publish_no_prefix)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		RfPageCtrcOwnerGraphV1 out = {0}; volatile bool caught = false; unsigned a, f;
		tuple_graph_ready(true);
		if (fault < 3) { tuple_graph_late_fault = fault; startup_image_observer = tuple_graph_late; }
		else if (fault < 5) owner_inventory_observe(fault == 3 ? 3 : 2);
		else reference_tuple_after_native = tuple_graph_after_native;
		a = allocations; f = frees;
		PG_TRY(); { out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 4); UT_ASSERT(owner_scope_empty(&out) && page_contribution_empty(&out));
		if (!owner_scope_empty(&out)) owner_scope_free(&out);
		UT_ASSERT_EQ(allocations - a, frees - f); tuple_graph_cleanup();
	}
}

UT_TEST(test_start_tuple_graph_whole_tuple_budget_precedes_page)
{
	Size low = 0, high = 1024 * 1024;
	while (low < high) {
		Size budget = low + (high - low) / 2; RfPageCtrcOwnerGraphV1 out;
		tuple_graph_ready(true);
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, budget);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) { high = budget; owner_scope_free(&out); }
		else { UT_ASSERT(owner_scope_empty(&out)); low = budget + 1; }
		tuple_graph_cleanup();
	}
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRecoveryCensusReferencesV1 *refs = tuple_graph_ready(true); RfPageCtrcOwnerGraphV1 out;
		if (fault == 1) refs->entries[8].entry.tuple_length = BLCKSZ + 1;
		if (fault == 2) allocation_ok = false;
		if (fault == 3) refs->count = UINT32_MAX;
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, fault == 0 ? low - 1 : high);
		if (fault == 3) refs->count = 20;
		allocation_ok = true;
		UT_ASSERT(out.detail != RF_PAGE_PROOF_DETAIL_OK && owner_scope_empty(&out));
		UT_ASSERT_EQ(startup_image_calls, 0); tuple_graph_cleanup();
	}
}

UT_TEST(test_start_tuple_graph_observation_and_history_remain_unresolved)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		ClusterRecoveryCensusReferencesV1 *refs = tuple_graph_ready(true); RfPageCtrcOwnerGraphV1 out;
		if (mode == 1) refs->entries[8].version = 17;
		out = mode == 0 ? ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024)
			: ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.entries[8].reference_field_count, 0);
			if (mode == 0) {
				UT_ASSERT(page_contribution_empty(&out));
				for (uint32 i = 0; i < out.count; i++) UT_ASSERT_EQ(out.entries[i].reference_field_count, 0);
			} else UT_ASSERT_EQ(out.entries[18].reference_field_count, 2);
			UT_ASSERT_EQ(out.count, 20); owner_scope_free(&out);
		}
		tuple_graph_cleanup();
	}
}
