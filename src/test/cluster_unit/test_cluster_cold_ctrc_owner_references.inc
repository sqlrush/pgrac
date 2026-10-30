/* Complete original reference -> actual PAGE-owner adjacency only.
 * Native PAGE/forest transport remains the original controlled fixture leaf.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned owner_reference_fault;

static ClusterRecoveryCensusReferencesV1 *
owner_references_ready(bool unchanged, uint32 owners)
{
	ClusterRecoveryCensusReferencesV1 *refs;
	ClusterRecoveryTxnKey key;
	final_projection_ready(unchanged); final_projection_check = false;
	prefix_count = owners;
	retirement_run_startup(); UT_ASSERT(retirement_service_reached);
	startup_image_calls = 0; memset(&startup_image_expected, 93, sizeof(startup_image_expected));
	start_ref_fault = start_ref_calls = owner_reference_fault = 0; start_ref_complete = true;
	refs = (ClusterRecoveryCensusReferencesV1 *)observation_references_storage[0].bytes;
	/* Controlled complete original census view; no graph/terminal qualification. */
	memset(refs->entries, 0, 6 * sizeof(refs->entries[0])); refs->count = 6;
	key = (ClusterRecoveryTxnKey){.system_identifier = 91, .database_incarnation = 2,
		.origin_node_id = native_ctrc_work.entries[0].key.owner_instance - 1,
		.segment_id = native_ctrc_work.entries[0].key.segment_id,
		.segment_generation = native_ctrc_work.entries[0].key.segment_generation,
		.slot_offset = native_ctrc_work.entries[0].key.slot_offset,
		.slot_wrap = native_ctrc_work.entries[0].key.slot_wrap, .xid = native_ctrc_work.entries[0].key.xid};
	for (uint32 i = 0; i < refs->count; i++) {
		ClusterRecoveryCensusReferenceV1 *r = &refs->entries[i];
		r->page_index = i == 2 || i == 3 || i == 4 ? 0 : 1;
		r->version = i == 1 || i == 5 ? 17 : 0;
		r->entry.kind = i == 3 || i == 5 ? CLUSTER_RECOVERY_HEAP_ITEM : CLUSTER_RECOVERY_HEAP_ITL;
		if (r->entry.kind == CLUSTER_RECOVERY_HEAP_ITL) {
			r->entry.itl.flags = i == 2 ? ITL_FLAG_FREE : ITL_FLAG_ACTIVE;
			if (i != 2) {
				r->entry.itl.xid = key.xid;
				r->transaction.transaction = key;
				r->transaction.record = &start_tx_record_bytes;
				r->transaction.record_segment_generation = 19;
			}
		}
	}
	return refs;
}

static RfPageCtrcOwnerReferencesV1
owner_references_run(void)
{
	return ClusterColdRecoveryCtrcPageOwnerReferences(info.cluster_tx_start, 1024 * 1024);
}

static bool
owner_references_empty(const RfPageCtrcOwnerReferencesV1 *out)
{
	return out->count == 0 && out->entries == NULL && out->references == NULL
		&& out->owners.count == 0 && out->owners.entries == NULL && out->owners.detail == 0;
}

static void
owner_references_free(RfPageCtrcOwnerReferencesV1 *out)
{
	pfree(out->entries); pfree(out->owners.entries);
}

static void
owner_references_cleanup(void)
{
	start_ref_complete = false; start_ref_fault = 0; startup_image_observer = NULL;
	retirement_local_cleanup();
}

UT_TEST(test_start_page_owner_references_keep_full_original_order_and_distinct_duties)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		ClusterRecoveryCensusReferencesV1 *refs = owner_references_ready(unchanged != 0, 2);
		ClusterRecoveryCensusReferenceV1 fixed[6];
		RfPageCtrcOwnerReferencesV1 out;
		unsigned allocated = allocations, freed = frees;
		memcpy(fixed, refs->entries, sizeof(fixed)); out = owner_references_run();
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail != RF_PAGE_PROOF_DETAIL_OK) {
			fprintf(stderr, "owner-reference detail=%u refs=%u images=%u refusal=%s\n", out.detail, start_ref_calls, startup_image_calls, ClusterColdRecoveryCtrcObservationRefusal(info.cluster_tx_start));
			owner_references_cleanup(); continue;
		}
		UT_ASSERT_EQ(out.owners.count, 2); UT_ASSERT_EQ(out.count, 6); UT_ASSERT(out.references == refs);
		UT_ASSERT(memcmp(fixed, refs->entries, sizeof(fixed)) == 0);
		for (uint32 i = 0; i < out.count; i++) {
			UT_ASSERT_EQ(out.entries[i].reference_index, i);
			UT_ASSERT_EQ(out.entries[i].owner_index, i == 0 ? 1 : i == 4 ? 0 : UINT32_MAX);
		}
		UT_ASSERT_EQ(out.owners.entries[0].completed.owner.rule, unchanged ? 6 : 1);
		UT_ASSERT_EQ(startup_image_calls, 2); UT_ASSERT(start_ref_calls >= 2 * (refs->count + 1));
		UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		owner_references_free(&out); UT_ASSERT_EQ(allocations - allocated, frees - freed);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live); owner_references_cleanup();
	}
}

UT_TEST(test_start_page_owner_references_retain_unmatched_and_complete_empty_views)
{
	for (unsigned empty = 0; empty < 2; empty++) {
		ClusterRecoveryCensusReferencesV1 *refs = owner_references_ready(false, empty ? 2 : 1);
		RfPageCtrcOwnerReferencesV1 out;
		if (empty) refs->count = 0;
		out = owner_references_run(); UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail != RF_PAGE_PROOF_DETAIL_OK) { owner_references_cleanup(); continue; }
		UT_ASSERT_EQ(out.count, refs->count); UT_ASSERT(out.references == refs);
		if (!empty) {
			UT_ASSERT_EQ(out.entries[0].owner_index, UINT32_MAX);
			UT_ASSERT_EQ(out.entries[4].owner_index, 0);
		}
		UT_ASSERT_EQ(out.owners.count, empty ? 2 : 1);
		owner_references_free(&out); owner_references_cleanup();
	}
}

UT_TEST(test_start_page_owner_references_preflight_full_member_mapping_and_shape)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRecoveryCensusReferencesV1 *refs = owner_references_ready(false, 2);
		RfPageCtrcOwnerReferencesV1 out;
		if (fault == 0) refs->entries[5].page_index = observation_pages.count;
		if (fault == 1) observation_pages.entries[1] = observation_pages.entries[0];
		if (fault == 2) observation_pages.entries[1].heap = false;
		if (fault == 3) refs->entries[5].entry.kind = 99;
		if (fault == 4) prefix_targets[1].expected_result.segment_incarnation[15]++;
		out = owner_references_run();
		UT_ASSERT(out.detail != RF_PAGE_PROOF_DETAIL_OK && owner_references_empty(&out));
		UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(info.cluster_tx_start->revoked);
		owner_references_cleanup();
	}
}

static RfPageProofDetailV1
owner_references_change(uint32 index, RfPageProofDetailV1 detail)
{
	ClusterRecoveryCensusReferencesV1 *refs = (ClusterRecoveryCensusReferencesV1 *)observation_references_storage[0].bytes;
	ClusterRecoveryTxnKey *key = &refs->entries[0].transaction.transaction;
	if (index != 1 || detail != RF_PAGE_PROOF_DETAIL_OK) return detail;
	switch (owner_reference_fault) {
		case 0: key->system_identifier++; break;
		case 1: key->database_incarnation++; break;
		case 2: key->origin_node_id++; break;
		case 3: key->segment_id++; break;
		case 4: key->segment_generation++; break;
		case 5: key->slot_offset++; break;
		case 6: key->slot_wrap++; break;
		case 7: key->xid++; break;
		case 8: refs->entries[0].version++; break;
		case 9: refs->entries[0].page_index = 0; break;
		case 10: refs->entries[0].entry.itl.flags = ITL_FLAG_FREE; break;
		case 11: observation_pages.entries[0].reservation.identity.key.database_incarnation++; break;
		case 12: owner_round_change_first(); break;
		case 13: refs->count--; break;
	}
	return detail;
}

UT_TEST(test_start_page_owner_references_reject_late_full_identity_or_set_drift)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		RfPageCtrcOwnerReferencesV1 out;
		unsigned allocated, freed;
		owner_references_ready(false, 2); owner_reference_fault = fault;
		startup_image_observer = owner_references_change;
		allocated = allocations; freed = frees; out = owner_references_run();
		UT_ASSERT(out.detail != RF_PAGE_PROOF_DETAIL_OK && owner_references_empty(&out));
		UT_ASSERT(info.cluster_tx_start->revoked && pending_storage_mask == 3);
		UT_ASSERT_EQ(allocations - allocated, frees - freed); owner_references_cleanup();
	}
}

UT_TEST(test_start_page_owner_references_wait_and_error_publish_no_prefix_or_release)
{
	for (unsigned error = 0; error < 2; error++) {
		RfPageCtrcOwnerReferencesV1 out;
		unsigned allocated, freed;
		volatile bool caught = false;
		owner_references_ready(false, 2); owner_inventory_observe(error ? 2 : 3);
		allocated = allocations; freed = frees;
		PG_TRY(); { out = owner_references_run(); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, error != 0);
		UT_ASSERT_EQ(allocations - allocated, frees - freed);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live); UT_ASSERT_EQ(pending_storage_closes, 0);
		if (!error) {
			UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_WOULD_BLOCK); UT_ASSERT(owner_references_empty(&out));
			UT_ASSERT(!info.cluster_tx_start->revoked);
			startup_image_observer = NULL; ctrc_driver_waiting = false;
			out = owner_references_run(); UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
			owner_references_free(&out); UT_ASSERT_EQ(prefix_steps, 2);
		} else UT_ASSERT(info.cluster_tx_start->revoked);
		owner_references_cleanup();
	}
}

UT_TEST(test_start_page_owner_references_whole_budget_and_oom_precede_page)
{
	RfPageCtrcOwnerReferencesV1 out;
	owner_references_ready(false, 2);
	out = ClusterColdRecoveryCtrcPageOwnerReferences(info.cluster_tx_start, 1);
	UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_CAPACITY); UT_ASSERT(owner_references_empty(&out));
	UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(!info.cluster_tx_start->revoked);
	allocation_ok = false; out = owner_references_run(); allocation_ok = true;
	UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OOM); UT_ASSERT(owner_references_empty(&out));
	UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(!info.cluster_tx_start->revoked);
	out = ClusterColdRecoveryCtrcPageOwnerReferences(NULL, 1024 * 1024);
	UT_ASSERT(owner_references_empty(&out)); UT_ASSERT_EQ(start_ref_calls, 0);
	owner_references_cleanup();
}
