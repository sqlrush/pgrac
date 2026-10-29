/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_reference_terminals.inc
 *     Complete original reference identities at the final Startup boundary.
 *     Census/record transport is controlled; original Start/C0 checks run.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static ClusterRecoveryUndoRecord reference_terminal_records[2];

static ClusterRecoveryCensusReferencesV1 *
reference_terminals_ready(void)
{
	ClusterRecoveryCensusReferencesV1 *references;
	const ClusterColdTtSlotKeyV1 *key;

	final_projection_ready(false); final_projection_check = false;
	/* Page one has no PAGE-WAL target in this fixture. Its historical
	 * reference must still bind to the original canonical terminal. */
	prefix_count = 1;
	references = (ClusterRecoveryCensusReferencesV1 *)observation_references_storage[0].bytes;
	references->count = 2;
	key = &native_ctrc_work.entries[0].key;
	memset(reference_terminal_records, 0, sizeof(reference_terminal_records));
	for (unsigned i = 0; i < 2; i++) {
		ClusterRecoveryCensusReferenceV1 *reference = &references->entries[i];
		ClusterRecoveryTxnKey *transaction = &reference->transaction.transaction;
		const ClusterSpaceIdentityKey *space = &observation_pages.entries[1].reservation.identity.key;

		reference->page_index = 1; reference->version = i == 0 ? 0 : 19;
		reference->entry.kind = CLUSTER_RECOVERY_HEAP_ITL;
		reference->entry.index = i;
		reference->entry.itl.flags = ITL_FLAG_ACTIVE;
		reference->entry.itl.xid = key->xid;
		reference->entry.itl.undo_segment_head = uba_encode(key->segment_id, 1, key->slot_offset, 0);
		transaction->system_identifier = space->system_identifier;
		transaction->database_incarnation = space->database_incarnation;
		/* Original graph NodeId is zero-based; native undo owner is one-based. */
		transaction->origin_node_id = key->owner_instance - 1;
		transaction->segment_id = key->segment_id;
		transaction->segment_generation = key->segment_generation;
		transaction->slot_offset = key->slot_offset;
		transaction->slot_wrap = key->slot_wrap;
		transaction->xid = key->xid;
		reference_terminal_records[i].address = reference->entry.itl.undo_segment_head;
		reference->transaction.record = &reference_terminal_records[i];
	}
	return references;
}

UT_TEST(test_startup_reference_terminals_include_untouched_and_inverse_versions)
{
	ClusterRecoveryCensusReferencesV1 *references = reference_terminals_ready();
	ClusterRecoveryCensusReferenceV1 original[2];

	memcpy(original, references->entries, sizeof(original));
	retirement_run_startup();
	UT_ASSERT(retirement_service_reached);
	UT_ASSERT_EQ(prefix_steps, 1);
	UT_ASSERT(memcmp(original, references->entries, sizeof(original)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	retirement_local_cleanup();
}

UT_TEST(test_startup_reference_terminals_reject_full_identity_drift_not_raw_xid_match)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterRecoveryCensusReferencesV1 *references = reference_terminals_ready();
		ClusterRecoveryTxnKey *key = &references->entries[1].transaction.transaction;

		switch (fault) {
			case 0: key->system_identifier++; break;
			case 1: key->database_incarnation++; break;
			case 2: key->origin_node_id++; break;
			case 3: key->segment_id++; break;
			case 4: key->segment_generation++; break;
			case 5: key->slot_offset++; break;
			case 6: key->slot_wrap++; break;
			case 7: key->xid++; break;
		}
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT(observation_scope_live && pending_storage_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_reference_terminals_require_original_record_and_physical_page)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRecoveryCensusReferencesV1 *references = reference_terminals_ready();
		ClusterRecoveryCensusReferenceV1 *last = &references->entries[1];

		if (fault == 0) last->transaction.record = NULL;
		if (fault == 1) reference_terminal_records[1].address.raw[0]++;
		if (fault == 2) last->page_index = observation_pages.count;
		if (fault == 3) observation_pages.entries[1].heap = false;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT(observation_scope_live && pending_storage_live);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_reference_terminals_repeat_rechecks_last_historical_reference)
{
	ClusterRecoveryCensusReferencesV1 *references = reference_terminals_ready();
	unsigned steps, opens;

	retirement_run_startup(); UT_ASSERT(retirement_service_reached);
	steps = prefix_steps; opens = pending_storage_created;
	references->entries[1].transaction.transaction.segment_generation++;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(prefix_steps, steps); UT_ASSERT_EQ(pending_storage_created, opens);
	UT_ASSERT(pending_storage_live && observation_scope_live);
	retirement_local_cleanup();
}

static void
reference_terminals_empty_probe(void)
{
	retirement_service_reached = true;
	pg_re_throw();
}

UT_TEST(test_startup_reference_terminals_do_not_skip_zero_page_wal_graph)
{
	(void)reference_terminals_ready();
	prefix_count = 0; ctrc_wait_step = reference_terminals_empty_probe;
	reference_terminal_records[1].address.raw[0]++;
	retirement_run_startup();
	UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(prefix_pages, 0); UT_ASSERT_EQ(prefix_steps, 0);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	retirement_local_cleanup();
}

UT_TEST(test_startup_reference_terminals_keep_free_and_tuple_candidates_unqualified)
{
	ClusterRecoveryCensusReferencesV1 *references = reference_terminals_ready();

	memset(&references->entries[0].entry.itl, 0, sizeof(ClusterItlSlotData));
	references->entries[0].transaction.record = NULL;
	references->entries[1].entry.kind = CLUSTER_RECOVERY_HEAP_ITEM;
	references->entries[1].transaction.record = NULL;
	/* A saved item group now supplies its complete original eight-slot view,
	 * even when the only item and every slot are unused. */
	for (unsigned slot = 0; slot < CLUSTER_ITL_INITRANS_DEFAULT; slot++) {
		ClusterRecoveryCensusReferenceV1 *value = &references->entries[2 + slot];

		memset(value, 0, sizeof(*value));
		value->page_index = references->entries[1].page_index;
		value->version = references->entries[1].version;
		value->entry.kind = CLUSTER_RECOVERY_HEAP_ITL; value->entry.index = slot;
	}
	references->count += CLUSTER_ITL_INITRANS_DEFAULT;
	retirement_run_startup();
	UT_ASSERT(retirement_service_reached);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	retirement_local_cleanup();
}
