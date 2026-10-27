/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_episode_birth_halfspace.inc
 *    FULL page birth and original native counter half-space boundaries.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_recovery_episode_birth_halfspace.inc
 *
 * NOTES
 *    Real ROOT/WAL/counter/SIDE inputs; native CLOG and owner leaves remain
 *    controlled. These boundaries establish no native lifetime authority.
 *-------------------------------------------------------------------------
 */
static uint64 episode_birth_halfspace_tail_next;

static WalTestRecord
episode_birth_halfspace_tail(const ClusterWalSourceRef *source, WalTestRecord previous)
{
	CheckPoint checkpoint = {0};
	WalTestRecord next;

	checkpoint.nextXid = FullTransactionIdFromU64(episode_birth_halfspace_tail_next);
	checkpoint.ThisTimeLineID = checkpoint.PrevTimeLineID = source->timeline;
	checkpoint.redo = previous.record_start;
	checkpoint.fullPageWrites = true;
	next = inventory_write_record(source, previous.exclusive_end, previous.record_start,
		RM_XLOG_ID, XLOG_CHECKPOINT_ONLINE, &checkpoint, sizeof(checkpoint));
	return episode_xid_frame(source, next, InvalidTransactionId, InvalidTransactionId);
}

static void
episode_birth_halfspace_observe(uint64 distance, bool refuse)
{
	const uint64 first = (UINT64_C(3) << 32) | UINT64_C(4194304);

	for (unsigned abort = 0; abort < 2; abort++) {
		ClusterWalInputsV1 *inputs = NULL;
		ClusterRecoveryRelmapHistory *history = NULL;
		ClusterRelmapV2Image pending;
		ClusterRecoveryRelmapFoldSummary summary;
		ClusterRecoveryRelmapPendingNativeV1 native, zero = {0};
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRelmapClogBaseHandleV1 *held;
		unsigned census, reads, acquires;
		bool observed;

		episode_birth_halfspace_tail_next = first + distance;
		episode_terminal_tail_hook = episode_birth_halfspace_tail;
		episode_clog_side_begin(0, abort, &inputs, &history, &pending);
		episode_terminal_tail_hook = NULL;
		if (history == NULL) return;
		UT_ASSERT_EQ(episode_pending_drain(history, &pending, &summary), CLUSTER_RECOVERY_RELMAP_FOLD_DRAINED);
		/* The existing queried-XID input remains separate and succeeds even
		 * where the complete page-birth interval must now be refused. */
		UT_ASSERT(cluster_recovery_relmap_history_pending_native_v1(history, &pending, &native, &diag));
		UT_ASSERT_EQ(native.birth.inputs.clog[native.birth.birth_index].full_birth.first_full_xid, first);
		UT_ASSERT_EQ(native.birth.inputs.tail_next, episode_birth_halfspace_tail_next);
		UT_ASSERT(native.birth.inputs.tail_next - pending.pending_owner.top_full_xid < UINT64_C(0x80000000));
		held = history->native_base;
		census = inventory_census_calls; reads = episode_base_calls; acquires = ctrc_set_calls;
		observed = cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, &native, &diag);
		UT_ASSERT_EQ(observed, !refuse);
		UT_ASSERT_EQ(inventory_census_calls, census + 1); UT_ASSERT_EQ(episode_base_calls, reads + 2);
		if (observed == !refuse) {
			if (refuse) {
				UT_ASSERT_EQ(memcmp(&native, &zero, sizeof(native)), 0);
				UT_ASSERT(history->failed && relmap_failed != 0);
				census = inventory_census_calls; reads = episode_base_calls;
				UT_ASSERT(!cluster_recovery_relmap_history_pending_native_side_v1(history, &pending, &native, &diag));
				UT_ASSERT_EQ(inventory_census_calls, census); UT_ASSERT_EQ(episode_base_calls, reads);
			} else {
				UT_ASSERT_EQ(native.birth.inputs.tail_next, episode_birth_halfspace_tail_next);
				UT_ASSERT_EQ(native.terminal.base.native.status,
					abort ? TRANSACTION_STATUS_ABORTED : TRANSACTION_STATUS_COMMITTED);
				UT_ASSERT(!history->failed && relmap_failed == 0);
			}
		}
		UT_ASSERT(history->inventory->relmap_xid == NULL && history->inventory->relmap_terminal == NULL);
		UT_ASSERT(history->native_base == held && episode_base_live == 1);
		UT_ASSERT_EQ(ctrc_set_calls, acquires);
		UT_ASSERT_EQ(episode_base_releases | ctrc_set_release_calls, 0);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		episode_terminal_done(&history, &inputs);
	}
}

UT_TEST(test_episode_birth_halfspace_accepts_last_unambiguous_whole_page_position)
{
	episode_birth_halfspace_observe(UINT64_C(0x7fffffff), false);
}

UT_TEST(test_episode_birth_halfspace_refuses_exact_whole_page_half_cycle)
{
	episode_birth_halfspace_observe(UINT64_C(0x80000000), true);
}

UT_TEST(test_episode_birth_halfspace_refuses_later_tail_with_legal_query_distance)
{
	/* The query is 832 XIDs beyond the FULL page's first position. */
	episode_birth_halfspace_observe(UINT64_C(0x8000033f), true);
}
