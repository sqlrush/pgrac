/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_update_predecessors.inc
 *     Complete current tuple references cannot merge update predecessors.
 *     Census/PAGE transport and native tuple/HOT leaves are controlled.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
static char update_predecessor_bytes[64];

static ClusterRecoveryCensusReferencesV1 *
update_predecessors_ready(bool local)
{
	ClusterRecoveryCensusReferencesV1 *references = cross_page_ready();
	ClusterRecoveryCensusReferenceV1 *extra = &references->entries[18];

	/* The original two complete page views retain both canonical DATA
	 * identities. Add a distinct stored tuple, never a duplicate item key. */
	*extra = references->entries[8];
	extra->page_index = local ? 1 : 0;
	extra->entry.index = 2;
	ItemIdSetNormal(&extra->entry.item, 1088, sizeof(update_predecessor_bytes));
	memcpy(update_predecessor_bytes, reference_tuple_bytes[0], sizeof(update_predecessor_bytes));
	extra->entry.tuple_bytes = update_predecessor_bytes;
	references->count = 19;
	return references;
}

static void
update_predecessors_nonedge(unsigned kind)
{
	HeapTupleHeaderData tuple;

	memcpy(&tuple, update_predecessor_bytes, SizeofHeapTupleHeader);
	if (kind == 0) tuple.t_infomask |= HEAP_XMAX_INVALID;
	if (kind == 1) HeapTupleHeaderSetXmax(&tuple, HeapTupleHeaderGetRawXmax(&tuple) + 1);
	if (kind == 2) ItemPointerSet(&tuple.t_ctid, observation_pages.entries[0].blockno, 2);
	if (kind == 3) ItemPointerSet(&tuple.t_ctid, observation_pages.entries[1].blockno, 2);
	memcpy(update_predecessor_bytes, &tuple, SizeofHeapTupleHeader);
}

UT_TEST(test_startup_update_predecessors_reject_two_cross_page_sources)
{
	ClusterRecoveryCensusReferencesV1 *references = update_predecessors_ready(false);
	ClusterRecoveryCensusReferenceV1 saved[19];

	memcpy(saved, references->entries, sizeof(saved));
	UT_ASSERT(!cross_page_drain() && info.cluster_tx_start->revoked);
	UT_ASSERT(memcmp(saved, references->entries, sizeof(saved)) == 0);
	UT_ASSERT(pending_storage_live && observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	retirement_local_cleanup();
}

UT_TEST(test_startup_update_predecessors_join_local_and_cross_page_without_page_wal)
{
	for (unsigned empty = 0; empty < 2; empty++) {
		(void)update_predecessors_ready(true);
		if (empty) { prefix_count = 0; ctrc_wait_step = reference_terminals_empty_probe; }
		UT_ASSERT(!cross_page_drain() && info.cluster_tx_start->revoked);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		UT_ASSERT_EQ(prefix_steps, empty ? 0 : 1);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_update_predecessors_keep_native_nonedge_and_inverse_meaning)
{
	for (unsigned kind = 0; kind < 5; kind++) {
		ClusterRecoveryCensusReferencesV1 *references = update_predecessors_ready(false);

		if (kind < 4) update_predecessors_nonedge(kind);
		else
			for (unsigned i = 0; i < references->count; i++)
				if (references->entries[i].page_index == 0) references->entries[i].version = 19;
		UT_ASSERT(cross_page_drain());
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		UT_ASSERT(observation_scope_live && observation_cursor_live);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_update_predecessors_repeat_refuses_without_replacing_custody)
{
	unsigned steps, opens;
	HeapTupleHeaderData tuple;

	(void)update_predecessors_ready(false); update_predecessors_nonedge(0);
	UT_ASSERT(cross_page_drain());
	steps = prefix_steps; opens = pending_storage_created;
	memcpy(&tuple, update_predecessor_bytes, SizeofHeapTupleHeader);
	tuple.t_infomask &= ~HEAP_XMAX_INVALID;
	memcpy(update_predecessor_bytes, &tuple, SizeofHeapTupleHeader);
	UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
	UT_ASSERT(info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(prefix_steps, steps); UT_ASSERT_EQ(pending_storage_created, opens);
	UT_ASSERT(pending_storage_live && observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(pending_storage_closes, 0);
	retirement_local_cleanup();
}
