/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_reference_coverage.inc
 *    Original current PAGE reference coverage and native empty fields.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_cold_ctrc_reference_coverage.inc
 * NOTES
 *    Native page/projector predicates execute; transport/authority remain
 *    controlled fixture leaves, not full graph or completion authority.
 *-------------------------------------------------------------------------
 */
static PGAlignedBlock coverage_images[2];
static unsigned coverage_late_fault;
static ClusterRecoveryCensusReferencesV1 *coverage_references;

static bool
coverage_first_transaction(const ClusterRecoveryCensusReferenceV1 *r)
{
	return r->entry.kind == CLUSTER_RECOVERY_HEAP_ITL && r->entry.itl.flags != ITL_FLAG_FREE;
}

static RfPageProofDetailV1
coverage_image(uint32 index, RfPageProofDetailV1 detail)
{
	if (detail != RF_PAGE_PROOF_DETAIL_OK) return detail;
	if (index >= 2) return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	startup_image_expected = coverage_images[index];
	if (index == 1 && coverage_late_fault != 0) {
		unsigned fault = coverage_late_fault; coverage_late_fault = 0;
		if (fault == 1) tuple_graph_bytes[0][0][64] ^= 1;
		if (fault == 2) return RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
		if (fault == 3) pg_re_throw();
		if (fault == 4) owner_boot++;
		if (fault == 5) coverage_references->entries[1].version++;
	}
	return detail;
}

static ClusterRecoveryCensusReferencesV1 *
coverage_ready(bool empty)
{
	ClusterRecoveryCensusReferencesV1 *refs = tuple_graph_ready(true);
	ClusterColdTtSlotInputV1 subject;
	PGAlignedBlock before = startup_image_expected;
	Page page = (Page)before.data;

	UT_ASSERT(cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
		&native_ctrc_work, 0, &observation_physical[0], &subject));
	for (unsigned p = 0; p < 2; p++) {
		for (unsigned s = 0; s < 8; s++) {
			ClusterRecoveryCensusReferenceV1 *r = &refs->entries[p * 10 + s];
			if (empty) {
				memset(&r->entry.itl, 0, sizeof(r->entry.itl)); r->entry.itl.wrap = 17 + s;
				memset(&r->transaction, 0, sizeof(r->transaction));
			}
			ClusterPageGetItlSlots(page)[s] = r->entry.itl;
		}
		for (unsigned i = 0; i < 2; i++) {
			ClusterRecoveryHeapEntry *entry = &refs->entries[p * 10 + 8 + i].entry;
			ItemId item = PageGetItemId(page, i + 1);
			*item = entry->item;
			memcpy(page + ItemIdGetOffset(item), entry->tuple_bytes, entry->tuple_length);
			if (empty && i == 0) {
				HeapTupleHeader tuple = (HeapTupleHeader)PageGetItem(page, item);
				HeapTupleHeaderSetXmin(tuple, FrozenTransactionId);
				HeapTupleHeaderSetXmax(tuple, InvalidTransactionId);
				tuple->t_infomask = HEAP_XMAX_INVALID;
				tuple->t_itl_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;
				memcpy((char *)entry->tuple_bytes, tuple, entry->tuple_length);
			}
			if (empty && i == 1) {
				if (p == 0) ItemIdSetUnused(item); else ItemIdSetDead(item);
				entry->item = *item; entry->tuple_bytes = NULL; entry->tuple_length = 0;
			}
		}
		{
			ClusterCtrcItlCleanoutApplyResult result = test_startup_native_ctrc_project(&subject,
				1, page, coverage_images[p].data);
			UT_ASSERT(result == CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN
				|| result == CLUSTER_CTRC_ITL_CLEANOUT_ALREADY_TERMINAL);
			UT_ASSERT(test_startup_native_ctrc_heap(coverage_images[p].data));
		}
	}
	coverage_references = refs;
	start_ref_first_transaction = coverage_first_transaction;
	coverage_late_fault = 0; startup_image_calls = 0; startup_image_observer = coverage_image;
	return refs;
}

static void
coverage_free(RfPageCtrcReferenceCoverageV1 *out)
{
	if (out->classes != NULL) pfree(out->classes);
	owner_scope_free(&out->graph);
}

static void
coverage_cleanup(void)
{
	coverage_references = NULL;
	start_ref_first_transaction = NULL;
	coverage_late_fault = 0; startup_image_observer = NULL; tuple_graph_cleanup();
}

UT_TEST(test_start_reference_coverage_classifies_free_without_changing_original_graph)
{
	RfPageCtrcReferenceCoverageV1 out;
	RfPageCtrcOwnerGraphV1 old;
	coverage_ready(false);
	out = ClusterColdRecoveryCtrcPageReferenceCoverage(info.cluster_tx_start, 1024 * 1024);
	UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
	if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
		UT_ASSERT_EQ(out.count, 20); UT_ASSERT_EQ(out.no_reference_count, 12);
		UT_ASSERT_EQ(out.unresolved_count, 0); UT_ASSERT_EQ(out.uncovered_page_count, 0);
		UT_ASSERT_EQ(out.graph.unresolved_count, 12);
		for (unsigned i = 0; i < 20; i++)
			UT_ASSERT_EQ(out.classes[i], i % 10 > 0 && i % 10 < 7
				? RF_PAGE_CTRC_REFERENCE_NO_CURRENT_TX : RF_PAGE_CTRC_REFERENCE_GRAPH);
		coverage_free(&out);
	}
	old = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
	UT_ASSERT_EQ(old.detail, RF_PAGE_PROOF_DETAIL_OK); UT_ASSERT_EQ(old.unresolved_count, 12);
	owner_scope_free(&old); coverage_cleanup();
}

UT_TEST(test_start_reference_coverage_frozen_empty_dead_and_free_wrap_use_whole_page)
{
	ClusterRecoveryCensusReferencesV1 *refs = coverage_ready(true);
	ClusterRecoveryCensusReferenceV1 fixed[20]; RfPageCtrcReferenceCoverageV1 out;
	memcpy(fixed, refs->entries, sizeof(fixed));
	out = ClusterColdRecoveryCtrcPageReferenceCoverage(info.cluster_tx_start, 1024 * 1024);
	UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
	if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
		UT_ASSERT_EQ(out.count, 20); UT_ASSERT_EQ(out.no_reference_count, 20);
		UT_ASSERT_EQ(out.unresolved_count, 0); UT_ASSERT_EQ(out.uncovered_page_count, 0);
		UT_ASSERT(page_contribution_empty(&out.graph));
		for (unsigned i = 0; i < 20; i++) UT_ASSERT_EQ(out.classes[i], RF_PAGE_CTRC_REFERENCE_NO_CURRENT_TX);
		coverage_free(&out);
	}
	UT_ASSERT(memcmp(fixed, refs->entries, sizeof(fixed)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0); coverage_cleanup();
}

UT_TEST(test_start_reference_coverage_missing_duplicate_history_and_changed_members_remain)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterRecoveryCensusReferencesV1 *refs = coverage_ready(false);
		RfPageCtrcReferenceCoverageV1 out;
		if (fault == 0) refs->entries[6].version = 1;
		if (fault == 1) refs->entries[6].entry.index = 5;
		if (fault == 2) refs->entries[6].entry.itl.wrap++;
		if (fault == 3) tuple_graph_bytes[0][0][64] ^= 1;
		if (fault == 4) refs->entries[9].version = 1;
		if (fault == 5) {
			Page page = (Page)coverage_images[0].data;
			((PageHeader)page)->pd_lower += sizeof(ItemIdData); ItemIdSetUnused(PageGetItemId(page, 3));
		}
		if (fault == 6) refs->count = 0;
		if (fault == 7) refs->entries[6].entry.itl.xid = 81;
		out = ClusterColdRecoveryCtrcPageReferenceCoverage(info.cluster_tx_start, 1024 * 1024);
		if (fault == 6) refs->count = 20;
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT(out.uncovered_page_count != 0);
			for (unsigned i = 0; i < 10 && i < out.count; i++)
				UT_ASSERT(out.classes[i] != RF_PAGE_CTRC_REFERENCE_NO_CURRENT_TX);
			coverage_free(&out);
		} else UT_ASSERT(out.classes == NULL && owner_scope_empty(&out.graph));
		coverage_cleanup();
	}
}

UT_TEST(test_start_reference_coverage_does_not_erase_redirect_history_or_tuple_duties)
{
	for (unsigned mode = 0; mode < 6; mode++) {
		ClusterRecoveryCensusReferencesV1 *refs = coverage_ready(true);
		ClusterRecoveryHeapEntry *entry = &refs->entries[8].entry;
		HeapTupleHeaderData h; RfPageCtrcReferenceCoverageV1 out;
		memcpy(&h, entry->tuple_bytes, SizeofHeapTupleHeader);
		if (mode == 0) refs->entries[8].version = 1;
		if (mode == 1) h.t_infomask &= ~HEAP_XMAX_INVALID;
		if (mode == 2) { h.t_infomask = HEAP_XMAX_IS_MULTI; HeapTupleHeaderSetXmax(&h, 52); }
		if (mode == 3) { HeapTupleHeaderSetXmin(&h, 81); h.t_itl_slot_idx = 0; }
		if (mode == 4) h.t_infomask2 |= HEAP_HOT_UPDATED;
		if (mode == 5) {
			ItemIdSetRedirect(&refs->entries[9].entry.item, 1);
			*PageGetItemId((Page)coverage_images[0].data, 2) = refs->entries[9].entry.item;
		}
		if (mode > 0 && mode < 5) memcpy((char *)entry->tuple_bytes, &h, SizeofHeapTupleHeader);
		out = ClusterColdRecoveryCtrcPageReferenceCoverage(info.cluster_tx_start, 1024 * 1024);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT(out.classes[mode == 5 ? 9 : 8] != RF_PAGE_CTRC_REFERENCE_NO_CURRENT_TX);
			UT_ASSERT(out.unresolved_count != 0 || out.uncovered_page_count != 0); coverage_free(&out);
		} else UT_ASSERT(out.classes == NULL && owner_scope_empty(&out.graph));
		coverage_cleanup();
	}
}

UT_TEST(test_start_reference_coverage_late_change_wait_error_and_whole_budget_keep_debt)
{
	for (unsigned mode = 1; mode <= 8; mode++) {
		RfPageCtrcReferenceCoverageV1 out = {0}; volatile bool caught = false; unsigned a, f;
		ClusterRecoveryCensusReferencesV1 *refs = coverage_ready(false);
		if (mode <= 5) coverage_late_fault = mode;
		if (mode == 7) allocation_ok = false;
		if (mode == 8) refs->count = UINT32_MAX;
		a = allocations; f = frees;
		PG_TRY(); { out = ClusterColdRecoveryCtrcPageReferenceCoverage(info.cluster_tx_start, mode == 6 ? 1 : 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		if (mode == 8) refs->count = 20;
		allocation_ok = true;
		UT_ASSERT_EQ(caught, mode == 3); UT_ASSERT(out.classes == NULL && owner_scope_empty(&out.graph));
		if (out.classes != NULL) coverage_free(&out);
		UT_ASSERT_EQ(allocations - a, frees - f);
		if (mode >= 6) UT_ASSERT_EQ(startup_image_calls, 0);
		coverage_cleanup();
	}
}
