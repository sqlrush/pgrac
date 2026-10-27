/*-------------------------------------------------------------------------
 * test_cluster_tt_slot_shared_retained.inc
 *      Shared TT capacity requires canonical release even without peers.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *      src/test/cluster_unit/test_cluster_tt_slot_shared_retained.inc
 *-------------------------------------------------------------------------
 */
static void
shared_terminal_seed(bool aborted, bool fill)
{
	uint16 slot;
	int i;

	reset_allocator();
	cluster_shared_config = true;
	mock_retention_horizon = 1000;
	slot = cluster_tt_slot_alloc(1, 100);
	UT_ASSERT_EQ(slot, 0);
	if (aborted)
		cluster_tt_slot_mark_aborted(1, slot, 100, 0);
	else
		cluster_tt_slot_mark_committed(1, slot, 100, 0, 10);
	if (fill) {
		for (i = 1; i < TT_SLOTS_PER_SEGMENT; i++)
			UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 2000 + i), i);
	}
}

UT_TEST(test_shared_solo_terminal_is_not_direct_allocator_capacity)
{
	int aborted, gate, exact;

	for (aborted = 0; aborted < 2; aborted++) {
		for (gate = 0; gate < 2; gate++) {
			for (exact = 0; exact < 2; exact++) {
				ClusterTTSlotCurrentOwner before, after;
				bool retained = false, drift = false;
				uint16 result, wrap = TT_WRAP_INVALID;

				shared_terminal_seed(aborted != 0, true);
				cluster_undo_retention_horizon_enabled = gate != 0;
				UT_ASSERT(cluster_tt_slot_current_owner_by_xid(0, 100, &before));
				if (exact)
					result = cluster_tt_slot_alloc_current_exact(0, 1, 99999, &retained, &drift,
																 &wrap);
				else
					result = cluster_tt_slot_alloc_ext(1, 99999, &retained);
				UT_ASSERT_EQ(result, INVALID_TT_SLOT_OFFSET);
				UT_ASSERT(retained);
				UT_ASSERT(!drift);
				UT_ASSERT_EQ(wrap, TT_WRAP_INVALID);
				UT_ASSERT(cluster_tt_slot_current_owner_by_xid(0, 100, &after));
				UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
				UT_ASSERT_EQ(cluster_tt_slot_get_wrap(1, 0), 0);
				UT_ASSERT_EQ(mock_ctrc_sample_calls, 0);
				UT_ASSERT_EQ(mock_lwlock_depth, 0);
			}
		}
	}
}

UT_TEST(test_shared_solo_gc_refusal_preserves_original_owner)
{
	int aborted;

	for (aborted = 0; aborted < 2; aborted++) {
		ClusterTTSlotCurrentOwner before, after;
		ClusterUndoCleanerPassStats stats = { 0 };

		shared_terminal_seed(aborted != 0, false);
		UT_ASSERT(cluster_tt_slot_current_owner_by_xid(0, 100, &before));
		UT_ASSERT(cluster_tt_slot_gc_current_pass(20, 7, &stats));
		UT_ASSERT_EQ(stats.shmem_tt_slots_gcd, 0);
		UT_ASSERT_EQ(mock_ctrc_sample_calls, 1);
		UT_ASSERT_EQ(mock_ctrc_sample_lock_depth, 0);
		UT_ASSERT(cluster_tt_slot_current_owner_by_xid(0, 100, &after));
		UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
		UT_ASSERT_EQ(cluster_tt_slot_get_wrap(1, 0), 0);
		UT_ASSERT_EQ(mock_lwlock_depth, 0);
	}
}

UT_TEST(test_shared_solo_gc_reopens_only_with_original_release_sample)
{
	int aborted;

	for (aborted = 0; aborted < 2; aborted++) {
		ClusterTTSlotCurrentOwner owner;
		ClusterUndoCleanerPassStats stats = { 0 };

		shared_terminal_seed(aborted != 0, false);
		mock_ctrc_release_proven = true;
		/* The original canonical floor is inclusive; ABORTED has no SCN. */
		UT_ASSERT(cluster_tt_slot_gc_current_pass(aborted ? InvalidScn : 10, 7, &stats));
		UT_ASSERT_EQ(stats.shmem_tt_slots_gcd, 1);
		UT_ASSERT_EQ(mock_ctrc_sample_calls, 1);
		UT_ASSERT_EQ(mock_ctrc_sample_lock_depth, 0);
		UT_ASSERT_EQ(cluster_tt_slot_get_wrap(1, 0), 1);
		UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 99999), 0);
		UT_ASSERT(cluster_tt_slot_current_owner_by_xid(0, 99999, &owner));
		UT_ASSERT_EQ(owner.status, CTS_ACTIVE);
		UT_ASSERT_EQ(owner.wrap, 1);
		UT_ASSERT_EQ(mock_lwlock_depth, 0);
	}
}

UT_TEST(test_shared_solo_gc_rejects_binding_aba_after_sample)
{
	int aborted;

	for (aborted = 0; aborted < 2; aborted++) {
		ClusterTTSlotCurrentOwner before, after;
		ClusterUndoCleanerPassStats stats = { 0 };

		shared_terminal_seed(aborted != 0, false);
		UT_ASSERT(cluster_tt_slot_current_owner_by_xid(0, 100, &before));
		mock_ctrc_release_proven = mock_ctrc_rebind_during_sample = true;
		UT_ASSERT(cluster_tt_slot_gc_current_pass(20, 7, &stats));
		UT_ASSERT_EQ(mock_ctrc_sample_calls, 1);
		UT_ASSERT_EQ(mock_ctrc_sample_lock_depth, 0);
		UT_ASSERT_EQ(stats.shmem_tt_slots_gcd, 0);
		UT_ASSERT(cluster_tt_slot_current_owner_by_xid(0, 100, &after));
		UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
		UT_ASSERT_EQ(cluster_tt_slot_get_wrap(1, 0), 0);
		UT_ASSERT_EQ(mock_lwlock_depth, 0);
	}
}

UT_TEST(test_shared_solo_free_and_own_active_need_no_release_sample)
{
	bool retained = false, drift = false;
	uint16 wrap;

	reset_allocator();
	cluster_shared_config = true;
	UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 100), 0);
	UT_ASSERT_EQ(cluster_tt_slot_alloc_current_exact(0, 1, 100, &retained, &drift, &wrap), 0);
	UT_ASSERT_EQ(wrap, 0);
	UT_ASSERT(!retained && !drift);
	UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 101), 1);
	UT_ASSERT_EQ(mock_ctrc_sample_calls, 0);
	UT_ASSERT_EQ(mock_lwlock_depth, 0);
	reset_allocator();
}
