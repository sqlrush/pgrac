/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_plain_lock_refs.inc
 *     Original complete Startup view retains plain lockers as distinct roots.
 *     Native tuple/HOT, physical TT/PAGE and census transport leaves are controlled.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static ClusterRecoveryCensusReferencesV1 *
plain_lock_startup_ready(void)
{
	ClusterRecoveryCensusReferencesV1 *references = cross_page_ready();
	HeapTupleHeaderData tuple;

	cross_page_second_identity(references, 0);
	references->entries[0].entry.itl.flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	memcpy(&tuple, reference_tuple_bytes[0], SizeofHeapTupleHeader);
	HeapTupleHeaderSetXminFrozen(&tuple);
	tuple.t_infomask |= HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	tuple.t_itl_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;
	memcpy(reference_tuple_bytes[0], &tuple, SizeofHeapTupleHeader);
	return references;
}

UT_TEST(test_startup_plain_lock_refs_keep_distinct_terminals_without_cross_page_update)
{
	ClusterRecoveryCensusReferencesV1 *references = plain_lock_startup_ready();
	ClusterRecoveryCensusReferenceV1 original[18];

	memcpy(original, references->entries, sizeof(original));
	UT_ASSERT(cross_page_drain());
	UT_ASSERT_EQ(reference_tuple_calls, 2);
	UT_ASSERT(memcmp(original, references->entries, sizeof(original)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	retirement_local_cleanup();
}

UT_TEST(test_startup_plain_lock_refs_still_require_the_complete_canonical_terminal)
{
	for (unsigned field = 0; field < 3; field++) {
		ClusterRecoveryCensusReferencesV1 *references = plain_lock_startup_ready();
		ClusterRecoveryTxnKey *key = &references->entries[0].transaction.transaction;

		if (field == 0) key->origin_node_id++;
		if (field == 1) key->slot_wrap++;
		if (field == 2) key->segment_generation++;
		UT_ASSERT(!cross_page_drain() && info.cluster_tx_start->revoked);
		UT_ASSERT(pending_storage_live && observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_plain_lock_refs_preserve_multi_refusal_and_inverse_version)
{
	for (unsigned variant = 0; variant < 2; variant++) {
		ClusterRecoveryCensusReferencesV1 *references = plain_lock_startup_ready();
		HeapTupleHeaderData tuple;

		if (variant == 0) {
			memcpy(&tuple, reference_tuple_bytes[0], SizeofHeapTupleHeader);
			tuple.t_infomask |= HEAP_XMAX_IS_MULTI;
			memcpy(reference_tuple_bytes[0], &tuple, SizeofHeapTupleHeader);
			UT_ASSERT(!cross_page_drain() && info.cluster_tx_start->revoked);
		} else {
			for (unsigned i = 0; i < 9; i++) references->entries[i].version = 19;
			UT_ASSERT(cross_page_drain());
			UT_ASSERT_EQ(reference_tuple_calls, 2);
		}
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_plain_lock_refs_repeat_rechecks_update_polarity_and_keeps_debt)
{
	ClusterRecoveryCensusReferencesV1 *references = plain_lock_startup_ready();
	HeapTupleHeaderData tuple;
	unsigned steps, opens;

	UT_ASSERT(cross_page_drain());
	steps = prefix_steps; opens = pending_storage_created;
	references->entries[0].entry.itl.flags = ITL_FLAG_ACTIVE;
	memcpy(&tuple, reference_tuple_bytes[0], SizeofHeapTupleHeader);
	tuple.t_infomask &= ~(HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK);
	tuple.t_itl_slot_idx = 0;
	memcpy(reference_tuple_bytes[0], &tuple, SizeofHeapTupleHeader);
	UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
	UT_ASSERT(info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(prefix_steps, steps); UT_ASSERT_EQ(pending_storage_created, opens);
	UT_ASSERT(pending_storage_live && observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	retirement_local_cleanup();
}
