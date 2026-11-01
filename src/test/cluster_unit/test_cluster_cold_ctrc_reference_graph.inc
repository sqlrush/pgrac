/* Exact current ITL fields from the original owner after-image. PAGE I/O and
 * forest membership are existing controlled leaves; no graph completion.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterRecoveryCensusReferencesV1 *
reference_graph_ready(bool unchanged)
{
	ClusterRecoveryCensusReferencesV1 *refs = owner_scope_ready(unchanged);
	Page page = (Page)startup_image_expected.data;
	PageHeader h = (PageHeader)page;
	memset(page, 0, BLCKSZ);
	h->pd_flags = PD_HAS_ITL;
	h->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	h->pd_lower = SizeOfPageHeaderData; h->pd_upper = h->pd_special;
	PageSetPageSizeAndVersion(page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	for (uint32 i = 0; i < 2; i++) {
		ClusterRecoveryCensusReferenceV1 *ref = &refs->entries[i == 0 ? 0 : 4];
		/* Controlled original forest members share raw xid, not full identity. */
		if (i == 1) ref->transaction.transaction.segment_generation++;
		ref->entry.index = i == 0 ? 0 : CLUSTER_ITL_INITRANS_DEFAULT - 1;
		ref->entry.itl.wrap = 7 + i;
		ref->entry.itl.undo_segment_head = uba_encode(ref->transaction.transaction.segment_id,
			3, ref->transaction.transaction.slot_offset, 16 + i);
		ref->entry.itl.commit_scn = 93 + i;
		ref->entry.itl.write_scn = 101 + i;
		ref->entry.itl.first_change_lsn = 700 + i;
		ClusterPageGetItlSlots(page)[ref->entry.index] = ref->entry.itl;
	}
	/* Unrelated bytes must stay in the complete DATA witness. */
	page[SizeOfPageHeaderData + 73] = 57;
	return refs;
}

static bool
reference_graph_unresolved(const RfPageCtrcOwnerGraphEntryV1 *e)
{
	return e->reference_key == NULL && e->reference_evidence == NULL
		&& e->reference_key_length == 0 && e->reference_evidence_length == 0;
}

UT_TEST(test_start_reference_graph_uses_actual_itl_offset_full_identity_and_same_owner_bytes)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		ClusterRecoveryCensusReferencesV1 *refs = reference_graph_ready(unchanged != 0);
		RfPageCtrcOwnerGraphV1 out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail != RF_PAGE_PROOF_DETAIL_OK) { owner_scope_cleanup(); continue; }
		UT_ASSERT_EQ(out.count, refs->count);
		for (uint32 i = 0; i < out.count; i++) {
			const RfPageCtrcOwnerGraphEntryV1 *e = &out.entries[i];
			if (i != 0 && i != 4) { UT_ASSERT(reference_graph_unresolved(e)); continue; }
			UT_ASSERT(e->reference_key != NULL && e->reference_evidence != NULL);
			if (e->reference_key != NULL && e->reference_evidence != NULL) {
				const RfPageCtrcOwnerEvidenceV1 *owner = &out.related.owners.entries[e->owner_index];
				const uint8 *id = owner->evidence + 26;
				Size id_length = cluster_undo_tt_ctrc_get_u64_le(owner->evidence + 18) - 16;
				const uint8 *key = e->reference_key;
				UT_ASSERT_EQ(e->reference_key_length, 60 + id_length);
				UT_ASSERT(memcmp(key, e->key + 10, 34) == 0);
				UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u16_le(key + 34), 1);
				UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(key + 36), id_length);
				UT_ASSERT(memcmp(key + 44, id, id_length) == 0);
				UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(key + 44 + id_length),
					((PageHeader)startup_image_expected.data)->pd_special
					+ refs->entries[i].entry.index * sizeof(ClusterItlSlotData) + offsetof(ClusterItlSlotData, xid));
				UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(key + 52 + id_length), refs->entries[i].entry.index);
				UT_ASSERT_EQ(e->reference_evidence_length, owner->evidence_length);
				UT_ASSERT(e->reference_evidence == owner->evidence);
				UT_ASSERT(memcmp(e->reference_evidence + e->reference_evidence_length - BLCKSZ,
					startup_image_expected.data, BLCKSZ) == 0);
			}
		}
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		owner_scope_free(&out); owner_scope_cleanup();
	}
}

UT_TEST(test_start_reference_graph_does_not_borrow_equal_xid_or_another_physical_slot)
{
	for (unsigned field = 0; field < sizeof(ClusterItlSlotData); field++) {
		ClusterRecoveryCensusReferencesV1 *refs = reference_graph_ready(false);
		ClusterItlSlotData *actual = &ClusterPageGetItlSlots((Page)startup_image_expected.data)[0];
		RfPageCtrcOwnerGraphV1 out;
		/* Same xid is insufficient; include every native field and both UBA words. */
		((uint8 *)actual)[field] ^= 1;
		/* A byte-identical copy elsewhere cannot supply field zero's reference. */
		ClusterPageGetItlSlots((Page)startup_image_expected.data)[1] = refs->entries[0].entry.itl;
		out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT(out.entries[0].key != NULL && reference_graph_unresolved(&out.entries[0]));
			UT_ASSERT(out.entries[4].reference_key != NULL);
			UT_ASSERT_EQ(out.count, 6); owner_scope_free(&out);
		}
		owner_scope_cleanup();
	}
}

UT_TEST(test_start_reference_graph_preserves_history_free_tuple_and_invalid_layout_as_unresolved)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterRecoveryCensusReferencesV1 *refs = reference_graph_ready(false);
		RfPageCtrcOwnerGraphV1 out;
		PageHeader h = (PageHeader)startup_image_expected.data;
		if (fault == 0) refs->entries[0].entry.index = CLUSTER_ITL_INITRANS_DEFAULT;
		if (fault == 1) h->pd_special = BLCKSZ;
		if (fault == 2) h->pd_special++;
		if (fault == 3) h->pd_flags &= ~PD_HAS_ITL;
		if (fault == 4) h->pd_lower = h->pd_upper + 1;
		if (fault == 5) PageSetPageSizeAndVersion((Page)h, BLCKSZ, 0);
		if (fault == 6) h->pd_flags |= PD_SPACE_METADATA;
		out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			UT_ASSERT(reference_graph_unresolved(&out.entries[0]));
			UT_ASSERT(reference_graph_unresolved(&out.entries[1]));
			UT_ASSERT(reference_graph_unresolved(&out.entries[2]));
			UT_ASSERT(reference_graph_unresolved(&out.entries[3]));
			UT_ASSERT(reference_graph_unresolved(&out.entries[5]));
			UT_ASSERT(out.related.references == refs && refs->entries[1].version == 17);
			owner_scope_free(&out);
		}
		owner_scope_cleanup();
	}
}

UT_TEST(test_start_reference_graph_retains_whole_page_bytes_and_distinct_field_keys)
{
	uint8 key[2][MAXPGPATH + 106]; Size lengths[2] = {0};
	for (unsigned change = 0; change < 2; change++) {
		RfPageCtrcOwnerGraphV1 out;
		reference_graph_ready(false);
		startup_image_expected.data[SizeOfPageHeaderData + 73] = 57 + change;
		out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
			const RfPageCtrcOwnerGraphEntryV1 *e = &out.entries[0];
			UT_ASSERT(e->reference_key != NULL && out.entries[4].reference_key != NULL);
			if (e->reference_key != NULL && out.entries[4].reference_key != NULL) {
				UT_ASSERT(e->reference_key_length < sizeof(key[change]));
				lengths[change] = e->reference_key_length;
				memcpy(key[change], e->reference_key, lengths[change]);
				UT_ASSERT(memcmp(e->reference_key, out.entries[4].reference_key, lengths[change]) != 0);
				UT_ASSERT_EQ(e->reference_evidence[e->reference_evidence_length - BLCKSZ + SizeOfPageHeaderData + 73], 57 + change);
			}
			owner_scope_free(&out);
		}
		owner_scope_cleanup();
	}
	UT_ASSERT(lengths[0] != 0 && lengths[0] == lengths[1]);
	UT_ASSERT(memcmp(key[0], key[1], lengths[0]) == 0);
}

UT_TEST(test_start_reference_graph_late_refusal_wait_error_and_budget_publish_no_prefix)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		RfPageCtrcOwnerGraphV1 out = {0}; volatile bool caught = false;
		unsigned a, f;
		reference_graph_ready(false);
		if (fault < 2) owner_inventory_observe(fault == 0 ? 3 : 2);
		if (fault == 2) { owner_scope_late_fault = 1; startup_image_observer = owner_scope_change; }
		if (fault == 4) allocation_ok = false;
		a = allocations; f = frees;
		PG_TRY(); { out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start,
			fault == 3 ? 1 : 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		allocation_ok = true;
		UT_ASSERT_EQ(caught, fault == 1);
		UT_ASSERT(owner_scope_empty(&out)); UT_ASSERT_EQ(allocations - a, frees - f);
		UT_ASSERT(pending_storage_mask == 3 && pending_storage_closes == 0 && observation_scope_live);
		if (fault >= 3) UT_ASSERT_EQ(startup_image_calls, 0);
		owner_scope_cleanup();
	}
}
