/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_cross_page.inc
 *     Original Startup joins current cross-page tuple transaction identities.
 *     Census/physical TT, PAGE and native tuple/HOT leaves are controlled.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static ClusterRecoveryCensusReferencesV1 *
cross_page_ready(void)
{
	ClusterRecoveryCensusReferencesV1 *references = reference_tuples_ready();
	HeapTupleHeaderData tuple;

	/* Two complete current page views; the second has no PAGE-WAL target. */
	for (unsigned i = 0; i < references->count; i++) {
		references->entries[i].page_index = i / 9;
		references->entries[i].version = 0;
	}
	memcpy(&tuple, reference_tuple_bytes[0], SizeofHeapTupleHeader);
	tuple.t_infomask &= ~HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmax(&tuple, native_ctrc_work.entries[0].key.xid);
	tuple.t_itl_slot_idx = 0;
	ItemPointerSet(&tuple.t_ctid, observation_pages.entries[1].blockno, 1);
	memcpy(reference_tuple_bytes[0], &tuple, SizeofHeapTupleHeader);
	return references;
}

static void
cross_page_second_identity(ClusterRecoveryCensusReferencesV1 *references, unsigned field)
{
	ClusterColdTtSlotInputV1 *second = &native_ctrc_work.entries[1];
	ClusterRecoveryCensusReferenceV1 *reference = &references->entries[9];
	ClusterRecoveryTxnKey *key = &reference->transaction.transaction;

	/* A distinct original terminal subject may have the same raw xid. The
	 * existing canonical factory must accept both before testing the join. */
	*second = native_ctrc_work.entries[0];
	second->key.slot_offset++;
	if (field == 0) second->key.segment_generation++;
	if (field == 1) second->key.slot_wrap++;
	if (field == 2) second->key.slot_offset++;
	second->slot.wrap = second->key.slot_wrap;
	second->slot.status = TT_SLOT_COMMITTED; second->slot.commit_scn = 97;
	native_ctrc_work.counts.subject_count = 2;
	native_ctrc_work.counts.unresolved_count = 2;
	key->segment_generation = second->key.segment_generation;
	key->slot_wrap = second->key.slot_wrap; key->slot_offset = second->key.slot_offset;
	reference->entry.itl.undo_segment_head = uba_encode(second->key.segment_id, 1,
		second->key.slot_offset, 0);
	reference_terminal_records[1].address = reference->entry.itl.undo_segment_head;
	observation_physical[1] = observation_physical[0];
	observation_physical[1].segment_generation = second->key.segment_generation;
	observation_physical[1].slot_offset = second->key.slot_offset;
	observation_physical[1].slot = second->slot;
	observation_physical_count = 2;
}

static bool
cross_page_drain(void)
{
	/* Two controlled native terminal subjects do not supply a second C0
	 * service result. Exercise the original Start boundary independently of
	 * that still-unqualified later service collector. */
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
	UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
	return ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 2 * 1024 * 1024);
}

UT_TEST(test_startup_cross_page_pairs_use_complete_current_untouched_view)
{
	ClusterRecoveryCensusReferencesV1 *references = cross_page_ready();
	ClusterRecoveryCensusReferenceV1 original[18];

	memcpy(original, references->entries, sizeof(original));
	retirement_run_startup();
	UT_ASSERT(retirement_service_reached);
	UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(reference_tuple_calls, 2);
	UT_ASSERT(memcmp(original, references->entries, sizeof(original)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	retirement_local_cleanup();
}

UT_TEST(test_startup_cross_page_raw_xid_cannot_borrow_another_canonical_terminal)
{
	for (unsigned field = 0; field < 3; field++) {
		ClusterRecoveryCensusReferencesV1 *references = cross_page_ready();

		cross_page_second_identity(references, field);
		UT_ASSERT(!cross_page_drain() && info.cluster_tx_start->revoked);
		UT_ASSERT(pending_storage_live && observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_cross_page_matching_pair_cannot_be_heap_only)
{
	HeapTupleHeaderData tuple;

	(void)cross_page_ready();
	memcpy(&tuple, reference_tuple_bytes[1], SizeofHeapTupleHeader);
	tuple.t_infomask2 |= HEAP_ONLY_TUPLE;
	tuple.t_infomask |= HEAP_UPDATED;
	memcpy(reference_tuple_bytes[1], &tuple, SizeofHeapTupleHeader);
	UT_ASSERT(!cross_page_drain() && info.cluster_tx_start->revoked);
	UT_ASSERT(pending_storage_live && observation_scope_live);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	retirement_local_cleanup();
}

UT_TEST(test_startup_cross_page_does_not_join_stale_or_inverse_or_other_space)
{
	for (unsigned kind = 0; kind < 5; kind++) {
		ClusterRecoveryCensusReferencesV1 *references = cross_page_ready();
		HeapTupleHeaderData tuple;

		cross_page_second_identity(references, 0);
		if (kind == 0) {
			memcpy(&tuple, reference_tuple_bytes[0], SizeofHeapTupleHeader);
			ItemPointerSet(&tuple.t_ctid, observation_pages.entries[1].blockno, 2);
			memcpy(reference_tuple_bytes[0], &tuple, SizeofHeapTupleHeader);
		}
		if (kind == 1)
			for (unsigned i = 9; i < 18; i++) references->entries[i].version = 19;
		if (kind == 2) observation_pages.entries[1].reservation.identity.incarnation[15]++;
		if (kind == 3) observation_pages.entries[1].reservation.identity.key.locator.relNumber++;
		if (kind == 4) {
			memcpy(&tuple, reference_tuple_bytes[0], SizeofHeapTupleHeader);
			tuple.t_infomask |= HEAP_XMAX_INVALID;
			memcpy(reference_tuple_bytes[0], &tuple, SizeofHeapTupleHeader);
		}
		UT_ASSERT(cross_page_drain());
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_cross_page_repeat_rechecks_without_new_page_or_wal)
{
	ClusterRecoveryCensusReferencesV1 *references = cross_page_ready();
	HeapTupleHeaderData tuple;
	unsigned steps, opens;

	retirement_run_startup(); UT_ASSERT(retirement_service_reached);
	steps = prefix_steps; opens = pending_storage_created;
	memcpy(&tuple, reference_tuple_bytes[1], SizeofHeapTupleHeader);
	tuple.t_infomask2 |= HEAP_ONLY_TUPLE; tuple.t_infomask |= HEAP_UPDATED;
	memcpy(reference_tuple_bytes[1], &tuple, SizeofHeapTupleHeader);
	UT_ASSERT_EQ(references->count, 18);
	UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(prefix_steps, steps); UT_ASSERT_EQ(pending_storage_created, opens);
	UT_ASSERT(pending_storage_live && observation_scope_live);
	retirement_local_cleanup();
}

UT_TEST(test_startup_cross_page_zero_page_wal_still_checks_pair_identity)
{
	ClusterRecoveryCensusReferencesV1 *references = cross_page_ready();

	cross_page_second_identity(references, 0);
	prefix_count = 0; ctrc_wait_step = reference_terminals_empty_probe;
	UT_ASSERT(!cross_page_drain() && info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(prefix_steps, 0); UT_ASSERT(observation_scope_live && observation_cursor_live);
	retirement_local_cleanup();
}
