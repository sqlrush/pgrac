/* Same original Startup progress owns returned private FD cleanup.
 * Actual Start/F4 execute; raw storage/PAGE/WAL/GES remain controlled leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/storage/cluster_shared_fs_page.h"
static bool pending_storage_live, pending_storage_invalid, pending_storage_missing;
static bool pending_storage_check_close, pending_storage_cleanup_seen;
static unsigned pending_storage_created, pending_storage_checks, pending_storage_closes;
static unsigned pending_storage_token, pending_storage_token_second, pending_storage_mask;
static Size pending_storage_bytes;
static void pending_readback_reset(void);

static void
pending_storage_fixture_reset(void)
{
	UT_ASSERT(!pending_storage_live);
	pending_storage_invalid = pending_storage_missing = false;
	pending_storage_check_close = pending_storage_cleanup_seen = false;
	pending_storage_created = pending_storage_checks = pending_storage_closes = pending_storage_mask = 0;
	pending_storage_bytes = 8192;
	pending_readback_reset();
}

Size
cluster_shared_fs_page_bytes_v1(void)
{
	return pending_storage_bytes;
}

RfPageCtrcHeldStepV1
rf_page_recovery_ctrc_replays_itl_graph_hold_smgr_v1(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space, BlockNumber block,
	const RfPageVersionV1 *before, const char canonical[BLCKSZ],
	const ClusterRecoveryUndoEntry *physical, Size count, Size budget)
{
	RfPageCtrcHeldStepV1 held = {0};
	held.result = rf_page_recovery_ctrc_replays_itl_graph_sync_smgr_v1(start, work, source,
		authority, space, block, before, canonical, physical, count, budget);
	/* The older controlled leaf populated only its F4 descriptor. Mirror the
	 * native returned frame's complete identity/edge for the real continuation. */
	if (held.result.inserted.window.key.window_seq != 0) {
		ClusterRecoveryStampFpiV1 envelope;
		memcpy(&envelope, held.result.inserted.window.payload, sizeof(envelope));
		held.result.inserted.image.identity = *space;
		held.result.inserted.image.owner = envelope.image.owner;
		held.result.inserted.image.fork_number = envelope.image.fork_number;
		held.result.inserted.image.block_number = block;
		held.result.inserted.image.result_token = envelope.image.result_token;
		held.result.inserted.image.version = envelope.image.version;
	}
	if (held.result.phase == RF_PAGE_CTRC_STEP_FPI_DURABLE && !pending_storage_missing) {
		UT_ASSERT(pending_storage_created < 2);
		pending_storage_mask |= 1U << pending_storage_created;
		held.storage = pending_storage_created == 0 ? (void *)&pending_storage_token
			: (void *)&pending_storage_token_second;
		pending_storage_live = true; pending_storage_created++;
	}
	return held;
}

bool
cluster_shared_fs_page_current_v1(ClusterSharedFsPageV1 *storage)
{
	pending_storage_checks++;
	return !pending_storage_invalid
		&& ((storage == (void *)&pending_storage_token && (pending_storage_mask & 1) != 0)
			|| (storage == (void *)&pending_storage_token_second && (pending_storage_mask & 2) != 0));
}

bool
cluster_shared_fs_page_close_v1(ClusterSharedFsPageV1 *storage)
{
	unsigned mask;

	if (storage == NULL) return true;
	mask = storage == (void *)&pending_storage_token ? 1
		: storage == (void *)&pending_storage_token_second ? 2 : 0;
	UT_ASSERT(mask != 0 && (pending_storage_mask & mask) != 0);
	if (pending_storage_check_close) {
		UT_ASSERT(cluster_recovery_cold_start->ctrc_page_progress == NULL);
		UT_ASSERT(page_graph_live && observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(page_graph_destroys + observation_census_destroys + observation_scope_releases, 0);
	}
	pending_storage_mask &= ~mask; pending_storage_live = pending_storage_mask != 0;
	pending_storage_closes++;
	return !pending_storage_invalid;
}

static void
pending_storage_cleanup_probe(void)
{
	pending_storage_cleanup_seen = pending_storage_live && pending_storage_created == 1
		&& cluster_recovery_cold_start->ctrc_page_progress->pending;
}

static void
pending_storage_abandon(void)
{
	pending_storage_check_close = true;
	cluster_recovery_cold_observation_close(info.cluster_tx_start);
	UT_ASSERT(!pending_storage_live); UT_ASSERT_EQ(pending_storage_closes, 1);
	UT_ASSERT_EQ(page_graph_destroys, 1); UT_ASSERT_EQ(observation_census_destroys, 1);
	UT_ASSERT_EQ(observation_scope_releases, 1);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	UT_ASSERT(cluster_thread_recovery_replay_slot(prefix_window.key.target_thread)->stamp.failed);
}

UT_TEST(test_startup_pending_storage_is_saved_before_page_cleanup)
{
	prefix_ready(); prefix_destroy_callback = pending_storage_cleanup_probe;
	UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	UT_ASSERT(pending_storage_cleanup_seen && pending_storage_live);
	UT_ASSERT_EQ(pending_storage_created, 1); UT_ASSERT_EQ(pending_storage_closes, 0);
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(prefix_steps, 1); pending_storage_abandon();
}

UT_TEST(test_startup_pending_storage_loss_refuses_without_reopening)
{
	prefix_ready(); UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	pending_storage_invalid = true;
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(pending_storage_created, 1);
	pending_storage_abandon();
}

UT_TEST(test_startup_pending_storage_late_owner_loss_retains_cleanup)
{
	returned_fpi_ready(false, false, true, false);
	UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	UT_ASSERT(pending_storage_live); UT_ASSERT_EQ(pending_storage_closes, 0);
	pending_storage_abandon();
}

UT_TEST(test_startup_pending_storage_page_cleanup_error_keeps_handle)
{
	volatile bool caught = false;
	returned_fpi_ready(false, false, false, true);
	PG_TRY(); { (void)ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && pending_storage_live); UT_ASSERT_EQ(pending_storage_closes, 0);
	pending_storage_abandon();
}

UT_TEST(test_startup_pending_storage_requires_handle_for_durable_result)
{
	prefix_ready(); pending_storage_missing = true;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(cluster_recovery_cold_start->ctrc_page_progress->pending);
	UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(pending_storage_created, 0);
	UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
}

UT_TEST(test_startup_pending_storage_noop_and_wait_do_not_leak)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		prefix_ready(); prefix_unchanged = true; prefix_wait = mode == 1;
		UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
		UT_ASSERT(!pending_storage_live); UT_ASSERT_EQ(pending_storage_created + pending_storage_closes, 0);
		UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_progress->next_target, 2);
	}
}

UT_TEST(test_startup_pending_storage_checks_original_guards_before_metadata)
{
	unsigned checks;
	prefix_ready(); UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
	checks = pending_storage_checks;
	owner_boot++;
	UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT_EQ(pending_storage_checks, checks);
	owner_boot--;
	UT_ASSERT(ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT(pending_storage_checks > checks);
	pending_storage_abandon();
}
