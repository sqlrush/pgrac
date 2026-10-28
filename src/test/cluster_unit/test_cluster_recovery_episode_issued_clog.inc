/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_issued_clog.inc
 *     Actual issued FullXid and captured FULL CLOG bytes in one original episode.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_issued_clog.inc
 * NOTES
 *     Test-only composition; no native era, durability or completion grant.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_recovery_episode_allocator.h"
static TestEpisodeIssuedClogV1 episode_issued_clog;

static WalTestRecord
episode_issued_clog_prefix(const ClusterWalSourceRef *source, WalTestRecord previous)
{
	const XidNativeClogState *native = &episode_issued_clog.native;
	UT_ASSERT_EQ(source->claim.identity.origin_node_id, 0);
	previous = inventory_write_record(source, previous.exclusive_end, previous.record_start,
		RM_CLOG_ID, native->info, &native->bytes, native->length);
	return episode_xid_frame(source, previous, InvalidTransactionId, InvalidTransactionId);
}

UT_TEST(test_episode_issued_clog_bytes_reach_original_strict_reader)
{
	TestEpisodeAllocatorProbeV1 probe;
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryRelmapHistory *history = NULL;
	ClusterRelmapV2Image pending;
	ClusterRecoveryRelmapPendingNativePagesV1 out;
	ClusterRecoveryNativeInventoryDiag diag;
	const ClusterRecoveryRelmapClogEventV1 *birth;
	DecodedXLogRecord decoded = {0};
	XLogReaderState reader = {0};
	unsigned census;
	bool produced;
	int fds[3] = {-1, -1, -1};

	produced = test_episode_allocator_produce(true, false, &episode_issued_clog, &probe);
	UT_ASSERT(produced);
	if (!produced) return;
	UT_ASSERT_EQ(episode_issued_clog.first_issued, UINT64_C(0x300400000));
	UT_ASSERT_EQ(episode_issued_clog.queried_full_xid, UINT64_C(0x300400340));
	UT_ASSERT_EQ(episode_issued_clog.next_full_xid, UINT64_C(0x3004006a1));
	UT_ASSERT_EQ(probe.allocations, 107);
	UT_ASSERT_EQ(probe.published_xid, (uint32)(episode_issued_clog.next_full_xid - 1));
	UT_ASSERT(!probe.allocator_lock_held && !probe.native.xid_held && !probe.native.slru_held);
	UT_ASSERT_EQ(episode_issued_clog.native.bytes.first_full_xid, episode_issued_clog.first_issued);
	UT_ASSERT_EQ(episode_issued_clog.native.bytes.pageno, 128);
	UT_ASSERT_EQ(episode_issued_clog.native.length, sizeof(xl_clog_zeropage_full));
	UT_ASSERT_EQ(episode_issued_clog.native.info, CLOG_ZEROPAGE_FULL);
	UT_ASSERT_EQ(episode_issued_clog.native.inserts, 1);

	episode_native_pages_own_prefix = episode_issued_clog_prefix;
	episode_local_bounds_begin(episode_issued_clog.next_full_xid, 4195000, &inputs, &history, &pending);
	episode_native_pages_own_prefix = NULL;
	UT_ASSERT_EQ(pending.pending_owner.top_full_xid, episode_issued_clog.queried_full_xid);
	/* Replay the producer's complete captured bytes through the original rmgr.
	 * Native status is a fixture outcome, not XACT/TT terminal authority. */
	decoded.main_data = (char *)&episode_issued_clog.native.bytes;
	decoded.main_data_len = episode_issued_clog.native.length;
	decoded.max_block_id = -1;
	decoded.header.xl_info = episode_issued_clog.native.info;
	decoded.header.xl_rmid = RM_CLOG_ID;
	reader.record = &decoded;
	test_episode_native_clog_redo(&reader);
	test_episode_native_clog_set_committed((uint32)episode_issued_clog.queried_full_xid);
	test_episode_native_clog_write_all();
	census = inventory_census_calls;
	UT_ASSERT(cluster_recovery_relmap_history_pending_local_bounds_pages_v1(
		history, &pending, &out, &diag));
	UT_ASSERT_EQ(inventory_census_calls, census + 1);
	UT_ASSERT_EQ(episode_native_clog_calls, 6);
	UT_ASSERT_EQ(out.page_count, 3);
	UT_ASSERT_EQ(out.inputs.query.terminal.base.native.queried_full_xid,
		episode_issued_clog.queried_full_xid);
	birth = &out.inputs.query.birth.inputs.clog[out.inputs.query.birth.birth_index];
	UT_ASSERT_EQ(memcmp(&birth->full_birth, &episode_issued_clog.native.bytes,
		sizeof(birth->full_birth)), 0);
	episode_native_bounds_capture(history, fds, episode_issued_clog.next_full_xid, 4195000);
	UT_ASSERT(!history->failed && relmap_failed == 0 && inputs_pin_held && ctrc_set_held == 2);
	episode_local_bounds_end(&history, &inputs);
	for (unsigned i = 0; i < 3; ++i) UT_ASSERT_EQ(fcntl(fds[i], F_GETFD), -1);
}

UT_TEST(test_episode_issued_clog_raw_route_cannot_supply_full_record)
{
	TestEpisodeIssuedClogV1 out, zero = {0};
	TestEpisodeAllocatorProbeV1 probe;
	unsigned census = inventory_census_calls;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!test_episode_allocator_produce(false, false, &out, &probe));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT_EQ(probe.allocations, 107);
	UT_ASSERT_EQ(probe.next_full_xid, UINT64_C(0x3004006a1));
	UT_ASSERT_EQ(probe.native.calls | probe.native.begins | probe.native.inserts, 0);
	UT_ASSERT(!probe.allocator_lock_held && !probe.native.xid_held && !probe.native.slru_held);
	UT_ASSERT_EQ(inventory_census_calls, census);
}

UT_TEST(test_episode_issued_clog_wal_error_never_publishes_candidate)
{
	TestEpisodeIssuedClogV1 out, zero = {0};
	TestEpisodeAllocatorProbeV1 probe;
	unsigned census = inventory_census_calls;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!test_episode_allocator_produce(true, true, &out, &probe));
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT_EQ(probe.allocations, 0);
	UT_ASSERT_EQ(probe.next_full_xid, UINT64_C(0x3003ffffe));
	UT_ASSERT_EQ(probe.published_xid, InvalidTransactionId);
	UT_ASSERT_EQ(probe.native.inserts, 1);
	UT_ASSERT_EQ(probe.native.bytes.first_full_xid, UINT64_C(0x300400000));
	UT_ASSERT(probe.allocator_lock_held && probe.native.xid_held && probe.native.slru_held);
	UT_ASSERT_EQ(inventory_census_calls, census);
}
