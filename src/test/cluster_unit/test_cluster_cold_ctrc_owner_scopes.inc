/* Original Start/PAGE owner scope. Census/decoded WAL and PAGE I/O remain
 * controlled leaves; no graph admission or discharge is supplied here.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_PAGE_CTRC_OWNER_GRAPH_V1
typedef struct RfPageCtrcOwnerGraphEntryV1 {
	uint32 reference_index, owner_index;
	Size key_length, evidence_length;
	const uint8 *key, *evidence;
	Size reference_key_length, reference_evidence_length;
	const uint8 *reference_key, *reference_evidence;
	uint16 reference_edge_kind;
} RfPageCtrcOwnerGraphEntryV1;
typedef struct RfPageCtrcOwnerGraphV1 {
	RfPageProofDetailV1 detail;
	RfPageCtrcOwnerReferencesV1 related;
	uint32 count;
	RfPageCtrcOwnerGraphEntryV1 *entries;
} RfPageCtrcOwnerGraphV1;
extern RfPageCtrcOwnerGraphV1 ClusterColdRecoveryCtrcPageOwnerGraph(
	const struct ClusterRecoveryStartV1 *, Size);
#endif

static union {
	uint64 align;
	uint8 bytes[offsetof(DecodedXLogRecord, blocks) + sizeof(DecodedBkpBlock)];
} owner_scope_records[2];
static unsigned owner_scope_fault, owner_scope_late_fault;
static RfContributorStreamCutV1 owner_scope_cuts[3];
static bool owner_scope_three;
static RfPageStableEdgeInputV1 owner_scope_duplicate[2];

static void
owner_scope_wal(uint32 index, ClusterRecoveryPageWalV1 *out)
{
	uint32 source = owner_scope_three ? 2 : 0;
	out->source_index = source;
	out->source = observation_wal_sources[source].source;
	out->cut = observation_wal_sources[source].cut;
	if (owner_scope_fault == 1) out->source.claim.claim_sha256[31] ^= 1;
	if (owner_scope_fault == 2) out->cut.flags ^= 1;
	if (owner_scope_fault == 3) out->ordinal = 0;
	if (owner_scope_fault == 4 && index == 1) out->reader = &observation_page_wal_readers[0];
}

static ClusterRecoveryCensusReferencesV1 *
owner_scope_ready(bool unchanged)
{
	ClusterRecoveryCensusReferencesV1 *refs = owner_references_ready(unchanged, 2);
	owner_scope_fault = owner_scope_late_fault = 0; owner_graph_wal_view = owner_scope_wal;
	memset(owner_scope_records, 0, sizeof(owner_scope_records));
	for (uint32 i = 0; i < observation_wal_source_count; i++)
		owner_scope_cuts[i] = observation_wal_sources[i].cut;
	for (uint32 i = 0; i < 2; i++) {
		DecodedXLogRecord *r = (DecodedXLogRecord *)owner_scope_records[i].bytes;
		RfPageStableEdgeInputV1 *edge = &prefix_edges[i];
		prefix_contributors[i].system_identifier = 91;
		memcpy(prefix_contributors[i].storage_uuid, observation_wal_sources[0].source.claim.identity.storage_uuid, 16);
		prefix_contributors[i].participant_count = observation_wal_source_count;
		prefix_contributors[i].cuts = owner_scope_cuts;
		*r = observation_page_wal_records[i]; r->max_block_id = 0;
		r->header.xl_rmid = RM_XLOG_ID; r->header.xl_info = XLOG_FPI;
		r->header.xl_xid = refs->entries[0].transaction.transaction.xid;
		r->page_version_edge.entry_count = 1;
		r->page_version_edge.result_token = edge->result_token;
		edge->edge.block_id = 0; edge->edge.page_class = RF_PAGE_CLASS_ORDINARY;
		edge->edge.component_ordinal = 7 + i;
		r->page_version_edge.entries[0] = edge->edge;
		r->blocks[0].in_use = true; r->blocks[0].component_ordinal = 7 + i;
		r->blocks[0].rlocator = edge->page_identity.locator;
		r->blocks[0].forknum = MAIN_FORKNUM; r->blocks[0].blkno = 23 + i;
		observation_page_wal_readers[i].record = r;
		memcpy(observation_page_wal_raw[i], &r->header, SizeOfXLogRecord);
		edge->record_identity.system_identifier = 91;
		memcpy(edge->record_identity.storage_uuid, observation_wal_sources[0].source.claim.identity.storage_uuid, 16);
		edge->record_identity.read_rec_ptr = r->lsn; edge->record_identity.end_rec_ptr = r->next_lsn;
		edge->record_identity.record_crc = r->header.xl_crc;
		edge->record_identity.rmid = r->header.xl_rmid; edge->record_identity.info = r->header.xl_info;
		edge->record_complete = edge->opcode_supported = edge->side_complete = edge->image_integrity_ok = true;
		edge->component_count = 1;
	}
	return refs;
}

static void
owner_scope_cleanup(void)
{
	owner_graph_wal_view = NULL; owner_scope_fault = 0; owner_scope_three = false;
	owner_scope_before_startup = NULL;
	owner_references_cleanup();
}

static void
owner_scope_free(RfPageCtrcOwnerGraphV1 *out)
{
	pfree(out->entries); owner_references_free(&out->related);
}

static bool
owner_scope_empty(const RfPageCtrcOwnerGraphV1 *out)
{
	return out->count == 0 && out->entries == NULL && owner_references_empty(&out->related);
}

UT_TEST(test_start_page_owner_scopes_bind_full_transaction_effects_and_completion)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		ClusterRecoveryCensusReferencesV1 *refs = owner_scope_ready(unchanged != 0);
		RfPageCtrcOwnerGraphV1 out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
		if (out.detail != RF_PAGE_PROOF_DETAIL_OK) { owner_scope_cleanup(); continue; }
		UT_ASSERT_EQ(out.count, refs->count); UT_ASSERT_EQ(out.related.owners.count, 2);
		for (uint32 i = 0; i < out.count; i++) {
			const RfPageCtrcOwnerGraphEntryV1 *e = &out.entries[i];
			uint32 owner = i == 0 ? 1 : i == 4 ? 0 : UINT32_MAX;
			UT_ASSERT_EQ(e->reference_index, i); UT_ASSERT_EQ(e->owner_index, owner);
			if (owner == UINT32_MAX) { UT_ASSERT(e->key == NULL && e->evidence == NULL && e->key_length == 0); continue; }
			UT_ASSERT_EQ(e->key_length, 126); UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u16_le(e->key), 10);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(e->key + 10), 91);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u32_le(e->key + 40), refs->entries[i].transaction.transaction.xid);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(e->key + 44), 1);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u32_le(e->key + 52), 0);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(e->key + 56), 1);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(e->key + 68), observation_page_wal_readers[owner].ReadRecPtr);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u16_le(e->key + 120), RF_PAGE_CLASS_ORDINARY);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u32_le(e->key + 122), 7 + owner);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(e->evidence), 2);
			UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u16_le(e->evidence + 16), 2);
			{
				const RfPageCtrcOwnerEvidenceV1 *proof = &out.related.owners.entries[owner];
				const uint8 *atom = e->evidence + proof->evidence_length + 8;
				UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u16_le(atom), 3);
				UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u64_le(atom + 2), e->key_length);
				UT_ASSERT(memcmp(atom + 10, e->key, e->key_length) == 0);
				UT_ASSERT(memcmp(atom + 18 + e->key_length, proof->completed.completion, proof->completed.length) == 0);
			}
		}
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
		owner_scope_free(&out); owner_scope_cleanup();
	}
}

UT_TEST(test_start_page_owner_scopes_preserve_unmatched_and_other_transaction_duties)
{
	ClusterRecoveryCensusReferencesV1 *refs = owner_scope_ready(false);
	ClusterRecoveryTxnKey *other = &refs->entries[4].transaction.transaction;
	RfPageCtrcOwnerGraphV1 out;
	/* Full keys remain per-reference; the PAGE does not invent a shared T. */
	((DecodedXLogRecord *)owner_scope_records[0].bytes)->header.xl_xid++;
	memcpy(observation_page_wal_raw[0], &observation_page_wal_readers[0].record->header, SizeOfXLogRecord);
	out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024);
	UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
	if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
		UT_ASSERT_EQ(out.count, 6); UT_ASSERT_EQ(out.entries[1].owner_index, UINT32_MAX);
		UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u16_le(out.entries[4].key + 38), other->slot_wrap);
		UT_ASSERT(out.related.references == refs && refs->entries[1].version == 17);
		owner_scope_free(&out);
	}
	owner_scope_cleanup();
	refs = owner_scope_ready(false); refs->count = 0;
	out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024);
	UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
	if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
		UT_ASSERT_EQ(out.count, 0); UT_ASSERT_EQ(out.related.owners.count, 2);
		owner_scope_free(&out);
	}
	owner_scope_cleanup();
}

UT_TEST(test_start_page_owner_scopes_refuse_incomplete_source_record_or_component)
{
	for (unsigned fault = 0; fault < 11; fault++) {
		RfPageCtrcOwnerGraphV1 out;
		owner_scope_ready(false); owner_scope_fault = fault + 1;
		if (fault == 4) prefix_edges[0].edge.component_ordinal++;
		if (fault == 5) prefix_edges[0].component_count++;
		if (fault == 6) prefix_contributors[1].edge_count = 0;
		if (fault == 7) prefix_edges[0].record_complete = false;
		if (fault == 8) prefix_edges[1].participant_index = 1;
		if (fault == 9) prefix_edges[1].record_identity.record_crc++;
		if (fault == 10) {
			owner_scope_duplicate[0] = owner_scope_duplicate[1] = prefix_edges[0];
			prefix_contributors[0].edges = owner_scope_duplicate; prefix_contributors[0].edge_count = 2;
		}
		out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT(out.detail != RF_PAGE_PROOF_DETAIL_OK && owner_scope_empty(&out));
		UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(pending_storage_mask == 3);
		UT_ASSERT(info.cluster_tx_start->revoked); owner_scope_cleanup();
	}
}

static RfPageProofDetailV1
owner_scope_change(uint32 index, RfPageProofDetailV1 detail)
{
	if (index != 1 || detail != RF_PAGE_PROOF_DETAIL_OK) return detail;
	if (owner_scope_late_fault == 0) prefix_edges[0].edge.component_ordinal++;
	if (owner_scope_late_fault == 1) observation_page_wal_raw[0][SizeOfXLogRecord] ^= 1;
	if (owner_scope_late_fault == 2) prefix_edges[0].record_identity.end_rec_ptr++;
	if (owner_scope_late_fault == 3) prefix_contributors[0].edge_count = 0;
	if (owner_scope_late_fault == 4) observation_wal_sources[0].transcript_sha256[31]++;
	return detail;
}

UT_TEST(test_start_page_owner_scopes_refuse_late_input_changes_without_rebasing)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		RfPageCtrcOwnerGraphV1 out;
		unsigned a, f;
		owner_scope_ready(false); owner_scope_late_fault = fault; startup_image_observer = owner_scope_change;
		a = allocations; f = frees;
		out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024);
		UT_ASSERT(out.detail != RF_PAGE_PROOF_DETAIL_OK && owner_scope_empty(&out));
		UT_ASSERT_EQ(allocations - a, frees - f); UT_ASSERT(pending_storage_mask == 3);
		owner_scope_cleanup();
	}
}

UT_TEST(test_start_page_owner_scopes_wait_error_keep_original_debt_and_whole_output)
{
	for (unsigned error = 0; error < 2; error++) {
		RfPageCtrcOwnerGraphV1 out;
		volatile bool caught = false;
		unsigned a, f;
		owner_scope_ready(false); owner_inventory_observe(error ? 2 : 3);
		a = allocations; f = frees;
		PG_TRY(); { out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT_EQ(caught, error != 0); UT_ASSERT_EQ(allocations - a, frees - f);
		if (!error) UT_ASSERT(out.detail == RF_PAGE_PROOF_DETAIL_WOULD_BLOCK && owner_scope_empty(&out));
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live && pending_storage_closes == 0);
		owner_scope_cleanup();
	}
}

UT_TEST(test_start_page_owner_scopes_capacity_and_oom_precede_page)
{
	RfPageCtrcOwnerGraphV1 out;
	owner_scope_ready(false);
	out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1);
	UT_ASSERT(out.detail == RF_PAGE_PROOF_DETAIL_CAPACITY && owner_scope_empty(&out));
	allocation_ok = false;
	out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024);
	allocation_ok = true;
	UT_ASSERT(out.detail == RF_PAGE_PROOF_DETAIL_OOM && owner_scope_empty(&out));
	UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(!info.cluster_tx_start->revoked);
	owner_scope_cleanup();
}

static void
owner_scope_prepare_three(void)
{
	page_source_input_count = observation_wal_source_count = observation_manifest.source_count = 3;
	for (uint32 i = 1; i < 3; i++) {
		observation_wal_sources[i] = observation_wal_sources[0];
		observation_wal_sources[i].source.claim.identity.origin_owner_incarnation = 29 + i;
		observation_wal_sources[i].source.claim.claim_sha256[31] = i;
		observation_wal_sources[i].source_index = i;
		observation_wal_sources[i].cut.origin_owner_incarnation = 29 + i;
	}
	prefix_source_ordinal = 2;
	for (uint32 i = 0; i < 2; i++) prefix_edges[i].participant_index = 2;
}

UT_TEST(test_start_page_owner_scopes_use_original_root_order_not_physical_order)
{
	RfPageCtrcOwnerGraphV1 out;
	owner_scope_three = true; owner_scope_before_startup = owner_scope_prepare_three;
	owner_scope_ready(false);
	out = ClusterColdRecoveryCtrcPageOwnerGraph(info.cluster_tx_start, 1024 * 1024);
	UT_ASSERT_EQ(out.detail, RF_PAGE_PROOF_DETAIL_OK);
	if (out.detail == RF_PAGE_PROOF_DETAIL_OK) {
		UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_sources[2].generation_index, 1);
		UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u32_le(out.entries[0].key + 52), 1);
		UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u32_le(out.entries[0].key + 64), 1);
		owner_scope_free(&out);
	}
	owner_scope_cleanup();
}
