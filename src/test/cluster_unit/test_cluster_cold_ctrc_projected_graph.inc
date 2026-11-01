/* Original Start/PAGE owner and canonical terminal factories are real.
 * The pure native projector below is extracted byte-exact from its product;
 * PAGE I/O/forest/authority remain the existing controlled fixture leaves.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_itl.h"
#include "test_cluster_ctrc_projector.inc"

static bool projected_graph_commit;

static void
projected_graph_terminal(void)
{
	if (!projected_graph_commit) return;
	/* The original native loader is a controlled leaf. Select a preexisting
	 * terminal before the original Start captures its complete observation. */
	native_ctrc_work.entries[0].slot.status = TT_SLOT_COMMITTED;
	native_ctrc_work.entries[0].slot.commit_scn = 97;
	observation_physical[0].slot = native_ctrc_work.entries[0].slot;
}

static ClusterRecoveryCensusReferencesV1 *
projected_graph_ready(bool committed, bool locker, bool already)
{
	ClusterRecoveryCensusReferencesV1 *refs;
	ClusterColdTtSlotInputV1 subject;
	Page page;

	projected_graph_commit = committed;
	owner_scope_before_startup = projected_graph_terminal;
	refs = reference_graph_ready(already);
	owner_scope_before_startup = NULL;
	UT_ASSERT(cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
		&native_ctrc_work, 0, &observation_physical[0], &subject));
	page = (Page)startup_image_expected.data;
	for (uint32 i = 0; i < 2; i++) {
		ClusterRecoveryCensusReferenceV1 *ref = &refs->entries[i == 0 ? 0 : 4];
		ref->transaction.transaction = refs->entries[0].transaction.transaction;
		ref->entry.itl.flags = locker ? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_ACTIVE;
		ref->entry.itl.commit_scn = InvalidScn;
		if (already) {
			ref->entry.itl.flags = committed
				? (locker ? ITL_FLAG_LOCK_ONLY_COMMITTED : ITL_FLAG_COMMITTED)
				: (locker ? ITL_FLAG_LOCK_ONLY_ABORTED : ITL_FLAG_ABORTED);
			ref->entry.itl.commit_scn = subject.slot.commit_scn;
		}
		ClusterPageGetItlSlots(page)[ref->entry.index] = ref->entry.itl;
		UT_ASSERT_EQ(cluster_ctrc_recovery_itl_cleanout_image_v1(&subject,
			ref->entry.index, ref->entry.itl.wrap, locker ? 2 : 1, page, page),
			already ? CLUSTER_CTRC_ITL_CLEANOUT_ALREADY_TERMINAL : CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN);
	}
	return refs;
}

UT_TEST(test_start_projected_graph_binds_real_data_and_lock_terminal_projection)
{
	for (unsigned committed = 0; committed < 2; committed++)
		for (unsigned locker = 0; locker < 2; locker++)
			for (unsigned already = 0; already < 2; already++) {
				ClusterRecoveryCensusReferencesV1 *refs = projected_graph_ready(committed, locker, already);
				ClusterRecoveryCensusReferenceV1 fixed[6]; PGAlignedBlock image;
				RfPageCtrcOwnerGraphV1 out;
				memcpy(fixed, refs->entries, sizeof(fixed)); image = startup_image_expected;
				out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
				UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
				if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
					UT_ASSERT_EQ(out.count, 6);
					for (uint32 i = 0; i < out.count; i++) {
						const RfPageCtrcOwnerGraphEntryV1 *e = &out.entries[i];
						if (i != 0 && i != 4) { UT_ASSERT_EQ(e->reference_edge_kind, 0); continue; }
						UT_ASSERT_EQ(e->reference_edge_kind, 4);
						UT_ASSERT(e->reference_key != NULL && e->reference_evidence != NULL);
						if (e->reference_key != NULL) {
							const RfPageCtrcOwnerEvidenceV1 *proof = &out.related.owners.entries[e->owner_index];
							UT_ASSERT(memcmp(e->reference_key, e->key + 10, 34) == 0);
							UT_ASSERT(e->reference_evidence == proof->evidence);
							UT_ASSERT(memcmp(e->reference_evidence + e->reference_evidence_length - BLCKSZ,
								image.data, BLCKSZ) == 0);
							UT_ASSERT_EQ(proof->completed.owner.rule, already ? 6 : 1);
						}
					}
					owner_scope_free(&out);
				}
				UT_ASSERT(memcmp(fixed, refs->entries, sizeof(fixed)) == 0);
				UT_ASSERT(memcmp(&image, &startup_image_expected, sizeof(image)) == 0);
				UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
				UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
				owner_scope_cleanup();
			}
	/* NEEDS_CLEANOUT keeps the actual committed SCN. Do not treat that
	 * original state as an arbitrary changed/unknown current field. */
	{
		ClusterRecoveryCensusReferencesV1 *refs = projected_graph_ready(true, false, false);
		RfPageCtrcOwnerGraphV1 out;
		refs->entries[0].entry.itl.flags = ITL_FLAG_NEEDS_CLEANOUT;
		refs->entries[0].entry.itl.commit_scn = 97;
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.entries[0].reference_edge_kind, 4); owner_scope_free(&out);
		}
		owner_scope_cleanup();
	}
	/* Execute the original plain-lock tuple cleanup too. Terminal ITL bytes
	 * alone cannot substitute for the projector's required tuple changes. */
	for (unsigned pending_tuple = 0; pending_tuple < 2; pending_tuple++) {
		ClusterRecoveryCensusReferencesV1 *refs = projected_graph_ready(true, true, false);
		Page page = (Page)startup_image_expected.data;
		PageHeader h = (PageHeader)page;
		HeapTupleHeader tuple;
		RfPageCtrcOwnerGraphV1 out;
		refs->entries[4].version = 17;
		memset(&ClusterPageGetItlSlots(page)[7], 0, sizeof(ClusterItlSlotData));
		h->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
		h->pd_upper = h->pd_special - MAXALIGN(SizeofHeapTupleHeader);
		ItemIdSetNormal(PageGetItemId(page, 1), h->pd_upper, SizeofHeapTupleHeader);
		tuple = (HeapTupleHeader)(page + h->pd_upper); memset(tuple, 0, SizeofHeapTupleHeader);
		HeapTupleHeaderSetXmin(tuple, FrozenTransactionId);
		HeapTupleHeaderSetXmax(tuple, refs->entries[0].entry.itl.xid);
		tuple->t_infomask = HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_LOCK_ONLY
			| (pending_tuple ? 0 : HEAP_XMAX_INVALID);
		tuple->t_hoff = SizeofHeapTupleHeader;
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.entries[0].reference_edge_kind, pending_tuple ? 0 : 4);
			UT_ASSERT_EQ(out.entries[4].reference_edge_kind, 0); owner_scope_free(&out);
		}
		owner_scope_cleanup();
	}
}

UT_TEST(test_start_projected_graph_rejects_changed_terminal_or_unrelated_itl_bytes)
{
	for (Size byte = 0; byte < sizeof(ClusterItlSlotData); byte++) {
		ClusterRecoveryCensusReferencesV1 *refs = projected_graph_ready(true, false, false);
		RfPageCtrcOwnerGraphV1 out;
		((uint8 *)&ClusterPageGetItlSlots((Page)startup_image_expected.data)[refs->entries[0].entry.index])[byte] ^= 1;
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.entries[0].reference_edge_kind, 0);
			UT_ASSERT(reference_graph_unresolved(&out.entries[0]));
			UT_ASSERT_EQ(out.entries[4].reference_edge_kind, 4);
			owner_scope_free(&out);
		}
		owner_scope_cleanup();
	}
}

UT_TEST(test_start_projected_graph_requires_full_canonical_identity_and_keeps_other_duties)
{
	for (unsigned field = 0; field < 9; field++) {
		ClusterRecoveryCensusReferencesV1 *refs = projected_graph_ready(true, false, false);
		ClusterRecoveryTxnKey *t = &refs->entries[0].transaction.transaction;
		RfPageCtrcOwnerGraphV1 out;
		if (field == 0) t->system_identifier++;
		if (field == 1) t->database_incarnation++;
		if (field == 2) t->origin_node_id++;
		if (field == 3) t->segment_id++;
		if (field == 4) t->segment_generation++;
		if (field == 5) t->slot_offset++;
		if (field == 6) t->slot_wrap++;
		if (field == 7) t->xid++;
		if (field == 8) refs->entries[0].entry.itl.flags = ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI;
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT_EQ(out.entries[0].reference_edge_kind, 0);
			UT_ASSERT(reference_graph_unresolved(&out.entries[0]));
			UT_ASSERT_EQ(out.entries[4].reference_edge_kind, 4);
			UT_ASSERT_EQ(out.count, refs->count);
			for (uint32 i = 1; i < out.count; i++)
				if (i != 4) UT_ASSERT_EQ(out.entries[i].reference_edge_kind, 0);
			owner_scope_free(&out);
		} else UT_ASSERT(owner_scope_empty(&out));
		UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
		owner_scope_cleanup();
	}
}

UT_TEST(test_start_projected_graph_ordinary_mode_never_claims_discharge)
{
	for (unsigned already = 0; already < 2; already++) {
		RfPageCtrcOwnerGraphV1 out;
		projected_graph_ready(true, false, already);
		out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			for (uint32 i = 0; i < out.count; i++) UT_ASSERT_EQ(out.entries[i].reference_edge_kind, 0);
			UT_ASSERT_EQ(out.entries[0].reference_key != NULL, already);
			owner_scope_free(&out);
		}
		owner_scope_cleanup();
	}
}

static RfPageProofDetailV1
projected_graph_late(uint32 index, RfPageProofDetailV1 detail)
{
	if (index == 1 && detail == RF_PAGE_PROOF_DETAIL_OK) {
		window_region.checkpoint_owner.tx_result.result_generation++;
		window_region.checkpoint_owner.tx_accepted_generation = window_region.checkpoint_owner.tx_result.result_generation;
	}
	return detail;
}

UT_TEST(test_start_projected_graph_late_terminal_wait_error_preserve_whole_output_and_debt)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		RfPageCtrcOwnerGraphV1 out = {0}; volatile bool caught = false;
		unsigned a, f;
		projected_graph_ready(true, false, false);
		if (fault < 2) owner_inventory_observe(fault == 0 ? 3 : 2);
		else startup_image_observer = projected_graph_late;
		a = allocations; f = frees;
		PG_TRY(); { out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 1); UT_ASSERT(owner_scope_empty(&out));
		UT_ASSERT_EQ(allocations - a, frees - f);
		UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0 && observation_scope_live);
		owner_scope_cleanup();
	}
}

UT_TEST(test_start_projected_graph_reserves_whole_terminal_vector_before_page)
{
	Size low = 0, high = 1024 * 1024;
	/* Find the existing complete observation's actual budget boundary. The
	 * projection's simultaneous terminal/scratch peak must not borrow it. */
	while (low < high) {
		Size budget = low + (high - low) / 2;
		RfPageCtrcOwnerGraphV1 out;
		projected_graph_ready(true, false, false);
		out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, budget);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) { high = budget; owner_scope_free(&out); }
		else { UT_ASSERT(owner_scope_empty(&out)); low = budget + 1; }
		owner_scope_cleanup();
	}
	{
		RfPageCtrcOwnerGraphV1 out;
		projected_graph_ready(true, false, false);
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, low);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_CAPACITY);
		UT_ASSERT(owner_scope_empty(&out)); UT_ASSERT_EQ(startup_image_calls, 0);
		UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
		owner_scope_cleanup();
	}
	for (unsigned fault = 0; fault < 2; fault++) {
		RfPageCtrcOwnerGraphV1 out;
		projected_graph_ready(true, false, false);
		if (fault == 1) allocation_ok = false;
		out = ClusterColdRecoveryCtrcPageProjectedGraph(info.cluster_tx_start, fault == 0 ? 1 : 1024 * 1024);
		allocation_ok = true;
		UT_ASSERT(owner_scope_empty(&out)); UT_ASSERT_EQ(startup_image_calls, 0);
		UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0);
		owner_scope_cleanup();
	}
}
