/*-------------------------------------------------------------------------
 *
 * test_cluster_adg.c
 *	  Unit tests for spec-6.4 ADG pure correctness primitives.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_adg.c
 *
 * NOTES
 *	  This is a pgrac-original unit test for spec-6.4 ADG primitives.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <string.h>

#include "access/rmgr.h"
#include "cluster/cluster_adg.h"
#include "cluster/cluster_adg_xlog.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# unexpected Assert: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = scn_local(a);
	uint64 lb = scn_local(b);

	if (la < lb)
		return -1;
	if (la > lb)
		return 1;
	return 0;
}

int
scn_total_cmp(SCN a, SCN b)
{
	int c = scn_time_cmp(a, b);
	NodeId na;
	NodeId nb;

	if (c != 0)
		return c;
	na = scn_node_id(a);
	nb = scn_node_id(b);
	if (na < nb)
		return -1;
	if (na > nb)
		return 1;
	return 0;
}

int
scn_recovery_cmp(SCN a, XLogRecPtr a_lsn, NodeId a_node, SCN b, XLogRecPtr b_lsn, NodeId b_node)
{
	int c = scn_time_cmp(a, b);

	if (c != 0)
		return c;
	if (a_lsn < b_lsn)
		return -1;
	if (a_lsn > b_lsn)
		return 1;
	if (a_node < b_node)
		return -1;
	if (a_node > b_node)
		return 1;
	return 0;
}

SCN
cluster_scn_time_predecessor(SCN scn)
{
	NodeId node = scn_node_id(scn);
	uint64 local = scn_local(scn);

	if (!SCN_NODE_ID_VALID(node) || local == 0)
		return InvalidScn;
	return scn_encode(node, local - 1);
}

static SCN
S(int node, uint64 local)
{
	return scn_encode((NodeId)node, local);
}

UT_TEST(test_tracker_init_bounds)
{
	ClusterAdgScnTracker tracker;

	UT_ASSERT(!cluster_adg_scn_tracker_init(NULL, 1));
	UT_ASSERT(!cluster_adg_scn_tracker_init(&tracker, 0));
	UT_ASSERT_EQ((int)tracker.thread_count, 0);
	UT_ASSERT(!cluster_adg_scn_tracker_init(&tracker, CLUSTER_ADG_MAX_THREADS + 1));
	UT_ASSERT_EQ((int)tracker.thread_count, 0);
	UT_ASSERT(cluster_adg_scn_tracker_init(&tracker, 2));
	UT_ASSERT_EQ((int)tracker.thread_count, 2);
	UT_ASSERT_EQ(cluster_adg_scn_tracker_consistent_scn(&tracker), InvalidScn);
}

UT_TEST(test_barrier_min_publishes_after_all_threads)
{
	ClusterAdgScnTracker tracker;

	UT_ASSERT(cluster_adg_scn_tracker_init(&tracker, 2));
	UT_ASSERT(cluster_adg_mark_received(&tracker, 1, 100, 10));
	UT_ASSERT(cluster_adg_mark_received(&tracker, 2, 100, 10));
	UT_ASSERT(cluster_adg_apply_thread_barrier(&tracker, 1, 80, S(1, 50), 20));
	UT_ASSERT_EQ(cluster_adg_scn_tracker_consistent_scn(&tracker), InvalidScn);
	UT_ASSERT(cluster_adg_apply_thread_barrier(&tracker, 2, 70, S(2, 40), 20));
	UT_ASSERT_EQ(scn_time_cmp(cluster_adg_scn_tracker_consistent_scn(&tracker), S(2, 40)), 0);
	UT_ASSERT_EQ((int)tracker.publish_count, 1);
}

UT_TEST(test_receive_watermark_does_not_publish_consistent_scn)
{
	ClusterAdgScnTracker tracker;

	UT_ASSERT(cluster_adg_scn_tracker_init(&tracker, 2));
	UT_ASSERT(cluster_adg_mark_received(&tracker, 1, 1000, 10));
	UT_ASSERT(cluster_adg_mark_received(&tracker, 2, 1000, 10));
	UT_ASSERT(cluster_adg_mark_applied(&tracker, 1, 900, S(1, 900), 20));
	UT_ASSERT(cluster_adg_mark_applied(&tracker, 2, 900, S(2, 900), 20));
	UT_ASSERT_EQ(cluster_adg_scn_tracker_consistent_scn(&tracker), InvalidScn);
	UT_ASSERT(cluster_adg_apply_thread_barrier(&tracker, 1, 910, S(1, 100), 21));
	UT_ASSERT_EQ(cluster_adg_scn_tracker_consistent_scn(&tracker), InvalidScn);
	UT_ASSERT(cluster_adg_apply_thread_barrier(&tracker, 2, 910, S(2, 90), 21));
	UT_ASSERT_EQ(scn_time_cmp(cluster_adg_scn_tracker_consistent_scn(&tracker), S(2, 90)), 0);
}

UT_TEST(test_apply_cannot_overtake_receive)
{
	ClusterAdgScnTracker tracker;

	UT_ASSERT(cluster_adg_scn_tracker_init(&tracker, 1));
	UT_ASSERT(cluster_adg_mark_received(&tracker, 1, 100, 10));
	UT_ASSERT(!cluster_adg_mark_applied(&tracker, 1, 101, S(1, 10), 11));
	UT_ASSERT(!cluster_adg_apply_thread_barrier(&tracker, 1, 101, S(1, 10), 11));
	UT_ASSERT_EQ(cluster_adg_scn_tracker_consistent_scn(&tracker), InvalidScn);
}

UT_TEST(test_barrier_retreat_is_rejected)
{
	ClusterAdgScnTracker tracker;

	UT_ASSERT(cluster_adg_scn_tracker_init(&tracker, 1));
	UT_ASSERT(cluster_adg_mark_received(&tracker, 1, 100, 10));
	UT_ASSERT(cluster_adg_apply_thread_barrier(&tracker, 1, 80, S(1, 50), 20));
	UT_ASSERT_EQ(scn_time_cmp(cluster_adg_scn_tracker_consistent_scn(&tracker), S(1, 50)), 0);
	UT_ASSERT(!cluster_adg_apply_thread_barrier(&tracker, 1, 81, S(1, 49), 21));
	UT_ASSERT_EQ(scn_time_cmp(cluster_adg_scn_tracker_consistent_scn(&tracker), S(1, 50)), 0);
}

UT_TEST(test_thread_bounds_are_release_checked)
{
	ClusterAdgScnTracker tracker;

	UT_ASSERT(cluster_adg_scn_tracker_init(&tracker, 1));
	UT_ASSERT(!cluster_adg_mark_received(&tracker, 0, 1, 1));
	UT_ASSERT(!cluster_adg_mark_received(&tracker, 2, 1, 1));
	UT_ASSERT_EQ(cluster_adg_thread_apply_lag_ms(&tracker, 2, 100), -1);
}

UT_TEST(test_apply_lag_floor)
{
	ClusterAdgScnTracker tracker;

	UT_ASSERT(cluster_adg_scn_tracker_init(&tracker, 1));
	UT_ASSERT(cluster_adg_mark_applied(&tracker, 1, 10, S(1, 10), 100));
	UT_ASSERT_EQ((int)cluster_adg_thread_apply_lag_ms(&tracker, 1, 90), 0);
	UT_ASSERT_EQ((int)cluster_adg_thread_apply_lag_ms(&tracker, 1, 150), 50);
}

UT_TEST(test_pending_no_pending_uses_current_scn)
{
	ClusterAdgPendingCommitRegistry registry;

	cluster_adg_pending_init(&registry);
	UT_ASSERT_EQ(cluster_adg_thread_safe_scn(&registry, S(1, 100)), S(1, 100));
}

UT_TEST(test_pending_min_predecessor_and_abort_cleanup)
{
	ClusterAdgPendingCommitRegistry registry;

	cluster_adg_pending_init(&registry);
	UT_ASSERT(cluster_adg_pending_register(&registry, S(3, 120)));
	UT_ASSERT(cluster_adg_pending_register(&registry, S(2, 80)));
	UT_ASSERT_EQ(scn_time_cmp(cluster_adg_pending_min_scn(&registry), S(2, 80)), 0);
	UT_ASSERT_EQ(scn_time_cmp(cluster_adg_thread_safe_scn(&registry, S(1, 200)), S(2, 79)), 0);
	UT_ASSERT(cluster_adg_pending_clear(&registry, S(2, 80)));
	UT_ASSERT_EQ(scn_time_cmp(cluster_adg_pending_min_scn(&registry), S(3, 120)), 0);
	UT_ASSERT(cluster_adg_pending_clear(&registry, S(3, 120)));
	UT_ASSERT_EQ(cluster_adg_pending_min_scn(&registry), InvalidScn);
}

UT_TEST(test_pending_overflow_fails_closed)
{
	ClusterAdgPendingCommitRegistry registry;
	int i;

	cluster_adg_pending_init(&registry);
	UT_ASSERT(!cluster_adg_pending_register(&registry, InvalidScn));
	for (i = 0; i < CLUSTER_ADG_PENDING_MAX; i++)
		UT_ASSERT(cluster_adg_pending_register(&registry, S(1, (uint64)i + 1)));
	UT_ASSERT(!cluster_adg_pending_register(&registry, S(1, 1000)));
	UT_ASSERT(registry.overflowed);
}

UT_TEST(test_apply_master_lease_decisions)
{
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_check(5, 5, true, true, 10, 11),
				 (int)CLUSTER_ADG_LEASE_VALID);
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_check(5, 5, false, true, 10, 11),
				 (int)CLUSTER_ADG_LEASE_NOT_ATTACHED);
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_check(5, 5, true, false, 10, 11),
				 (int)CLUSTER_ADG_LEASE_NO_QUORUM);
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_check(4, 5, true, true, 10, 11),
				 (int)CLUSTER_ADG_LEASE_TERM_STALE);
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_check(5, 5, true, true, 11, 11),
				 (int)CLUSTER_ADG_LEASE_EXPIRED);
}

UT_TEST(test_apply_master_next_term_overflow)
{
	UT_ASSERT_EQ(cluster_adg_apply_master_next_term(41), (uint64)42);
	UT_ASSERT_EQ(cluster_adg_apply_master_next_term(UINT64_MAX), (uint64)0);
}

UT_TEST(test_apply_master_lease_marker_pack_unpack)
{
	ClusterAdgApplyMasterLease lease;
	ClusterAdgApplyMasterLease out;
	uint8 slot[CLUSTER_ADG_APPLY_LEASE_SLOT_BYTES];
	uint32 i;

	memset(slot, 0xA5, sizeof(slot));
	cluster_adg_apply_master_lease_init(&lease, 7, 3, 1000, 11);
	UT_ASSERT(cluster_adg_apply_master_lease_valid(&lease));
	UT_ASSERT(cluster_adg_apply_master_lease_pack(slot, &lease));
	for (i = sizeof(ClusterAdgApplyMasterLease); i < sizeof(slot); i++)
		UT_ASSERT_EQ((int)slot[i], 0);
	UT_ASSERT(cluster_adg_apply_master_lease_unpack(slot, &out));
	UT_ASSERT_EQ(out.magic, CLUSTER_ADG_APPLY_LEASE_MAGIC);
	UT_ASSERT_EQ(out.version, CLUSTER_ADG_APPLY_LEASE_VERSION);
	UT_ASSERT_EQ(out.term, (uint64)7);
	UT_ASSERT_EQ(out.owner_node_id, 3);
	UT_ASSERT_EQ(out.lease_expires_at_ms, (int64)1000);
	UT_ASSERT_EQ(out.generation, (uint64)11);

	slot[offsetof(ClusterAdgApplyMasterLease, generation)] ^= 0x01;
	UT_ASSERT(!cluster_adg_apply_master_lease_unpack(slot, &out));
}

UT_TEST(test_apply_master_lease_marker_rejects_invalid_fields)
{
	ClusterAdgApplyMasterLease lease;

	cluster_adg_apply_master_lease_init(&lease, 0, 3, 1000, 11);
	UT_ASSERT(!cluster_adg_apply_master_lease_valid(&lease));

	cluster_adg_apply_master_lease_init(&lease, 7, -1, 1000, 11);
	UT_ASSERT(!cluster_adg_apply_master_lease_valid(&lease));

	cluster_adg_apply_master_lease_init(&lease, 7, SCN_MAX_VALID_NODE_ID + 1, 1000, 11);
	UT_ASSERT(!cluster_adg_apply_master_lease_valid(&lease));

	cluster_adg_apply_master_lease_init(&lease, 7, 3, 1000, 11);
	lease.version++;
	UT_ASSERT(!cluster_adg_apply_master_lease_valid(&lease));
}

UT_TEST(test_streaming_merge_selects_scn_lsn_node_order)
{
	ClusterAdgMergeInput inputs[3];
	uint16 index = 99;

	memset(inputs, 0, sizeof(inputs));
	inputs[0].available = true;
	inputs[0].scn = S(1, 10);
	inputs[0].lsn = 50;
	inputs[0].node = 1;
	inputs[1].available = true;
	inputs[1].scn = S(2, 10);
	inputs[1].lsn = 40;
	inputs[1].node = 2;
	inputs[2].available = true;
	inputs[2].scn = S(0, 9);
	inputs[2].lsn = 99;
	inputs[2].node = 0;
	UT_ASSERT(cluster_adg_streaming_merge_select(inputs, 3, &index));
	UT_ASSERT_EQ((int)index, 2);

	inputs[2].available = false;
	UT_ASSERT(cluster_adg_streaming_merge_select(inputs, 3, &index));
	UT_ASSERT_EQ((int)index, 1);

	inputs[1].lsn = 50;
	inputs[1].node = 0;
	UT_ASSERT(cluster_adg_streaming_merge_select(inputs, 3, &index));
	UT_ASSERT_EQ((int)index, 1);
}

UT_TEST(test_streaming_merge_ignores_unavailable_or_invalid)
{
	ClusterAdgMergeInput inputs[2];
	uint16 index = 99;

	memset(inputs, 0, sizeof(inputs));
	inputs[0].available = true;
	inputs[0].scn = InvalidScn;
	inputs[1].available = false;
	inputs[1].scn = S(1, 10);
	UT_ASSERT(!cluster_adg_streaming_merge_select(inputs, 2, &index));
}

UT_TEST(test_read_only_decision_matrix)
{
	UT_ASSERT_EQ((int)cluster_adg_read_only_decide(true, true, S(1, 90), S(2, 100), 5, 10),
				 (int)CLUSTER_ADG_READ_ALLOW);
	UT_ASSERT_EQ((int)cluster_adg_read_only_decide(true, true, S(1, 110), S(2, 100), 5, 10),
				 (int)CLUSTER_ADG_READ_WAIT);
	UT_ASSERT_EQ((int)cluster_adg_read_only_decide(true, true, S(1, 90), S(2, 100), 11, 10),
				 (int)CLUSTER_ADG_READ_LAG_EXCESSIVE);
	UT_ASSERT_EQ((int)cluster_adg_read_only_decide(false, true, S(1, 90), S(2, 100), 5, 10),
				 (int)CLUSTER_ADG_READ_UNRESOLVABLE);
	UT_ASSERT_EQ((int)cluster_adg_read_only_decide(true, true, S(1, 90), InvalidScn, 5, 10),
				 (int)CLUSTER_ADG_READ_UNRESOLVABLE);
}

UT_TEST(test_overlay_resolve_on_commit_prepared)
{
	ClusterTTStatus out_status = CLUSTER_TT_STATUS_UNKNOWN;
	SCN out_scn = InvalidScn;

	UT_ASSERT(cluster_adg_overlay_resolve_on_commit_prepared(CLUSTER_TT_STATUS_IN_PROGRESS,
															 S(3, 77), &out_status, &out_scn));
	UT_ASSERT_EQ((int)out_status, (int)CLUSTER_TT_STATUS_COMMITTED);
	UT_ASSERT_EQ(scn_time_cmp(out_scn, S(3, 77)), 0);
	UT_ASSERT(!cluster_adg_overlay_resolve_on_commit_prepared(CLUSTER_TT_STATUS_ABORTED, S(3, 77),
															  &out_status, &out_scn));
	UT_ASSERT(!cluster_adg_overlay_resolve_on_commit_prepared(CLUSTER_TT_STATUS_IN_PROGRESS,
															  InvalidScn, &out_status, &out_scn));
}

UT_TEST(test_thread_barrier_wal_abi)
{
	xl_cluster_adg_thread_barrier rec;

	memset(&rec, 0, sizeof(rec));
	rec.thread_id = 4;
	rec.thread_safe_scn = S(3, 99);

	UT_ASSERT_EQ((int)sizeof(xl_cluster_adg_thread_barrier), 16);
	UT_ASSERT_EQ((int)offsetof(xl_cluster_adg_thread_barrier, thread_safe_scn), 8);
	UT_ASSERT_EQ((int)(XLOG_CLUSTER_ADG_THREAD_BARRIER & XLR_INFO_MASK), 0);
	UT_ASSERT_EQ((int)RM_CLUSTER_ADG_ID, (int)(RM_CLUSTER_RAW_LAYOUT_ID + 1));
	UT_ASSERT_EQ((uint64)rec.thread_safe_scn, (uint64)S(3, 99));
}

UT_TEST(test_standby_reply_trailer_validation)
{
	UT_ASSERT_EQ((int)CLUSTER_ADG_REPLY_TRAILER_BYTES, 24);
	UT_ASSERT(cluster_adg_reply_trailer_valid(CLUSTER_ADG_REPLY_MAGIC, CLUSTER_ADG_REPLY_VERSION,
											  XLP_THREAD_ID_LEGACY, InvalidScn, 0));
	UT_ASSERT(cluster_adg_reply_trailer_valid(CLUSTER_ADG_REPLY_MAGIC, CLUSTER_ADG_REPLY_VERSION,
											  CLUSTER_WAL_THREAD_MAX, S(2, 500), 7));
	UT_ASSERT(!cluster_adg_reply_trailer_valid(CLUSTER_ADG_REPLY_MAGIC + 1,
											   CLUSTER_ADG_REPLY_VERSION, XLP_THREAD_ID_LEGACY,
											   InvalidScn, 0));
	UT_ASSERT(!cluster_adg_reply_trailer_valid(CLUSTER_ADG_REPLY_MAGIC,
											   CLUSTER_ADG_REPLY_VERSION + 1, XLP_THREAD_ID_LEGACY,
											   InvalidScn, 0));
	UT_ASSERT(!cluster_adg_reply_trailer_valid(CLUSTER_ADG_REPLY_MAGIC, CLUSTER_ADG_REPLY_VERSION,
											   CLUSTER_WAL_THREAD_MAX + 1, S(2, 500), 7));
}

int
main(void)
{
	UT_PLAN(20);

	UT_RUN(test_tracker_init_bounds);
	UT_RUN(test_barrier_min_publishes_after_all_threads);
	UT_RUN(test_receive_watermark_does_not_publish_consistent_scn);
	UT_RUN(test_apply_cannot_overtake_receive);
	UT_RUN(test_barrier_retreat_is_rejected);
	UT_RUN(test_thread_bounds_are_release_checked);
	UT_RUN(test_apply_lag_floor);
	UT_RUN(test_pending_no_pending_uses_current_scn);
	UT_RUN(test_pending_min_predecessor_and_abort_cleanup);
	UT_RUN(test_pending_overflow_fails_closed);
	UT_RUN(test_apply_master_lease_decisions);
	UT_RUN(test_apply_master_next_term_overflow);
	UT_RUN(test_apply_master_lease_marker_pack_unpack);
	UT_RUN(test_apply_master_lease_marker_rejects_invalid_fields);
	UT_RUN(test_streaming_merge_selects_scn_lsn_node_order);
	UT_RUN(test_streaming_merge_ignores_unavailable_or_invalid);
	UT_RUN(test_read_only_decision_matrix);
	UT_RUN(test_overlay_resolve_on_commit_prepared);
	UT_RUN(test_thread_barrier_wal_abi);
	UT_RUN(test_standby_reply_trailer_validation);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
