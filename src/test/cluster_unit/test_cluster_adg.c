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
#include "cluster/cluster_mrp.h"

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

UT_TEST(test_non_monotone_record_scn_does_not_block_apply_or_publish)
{
	ClusterAdgScnTracker tracker;

	UT_ASSERT(cluster_adg_scn_tracker_init(&tracker, 1));
	UT_ASSERT(cluster_adg_mark_received(&tracker, 1, 300, 10));
	UT_ASSERT(cluster_adg_mark_applied(&tracker, 1, 100, S(1, 200), 20));
	UT_ASSERT(cluster_adg_mark_applied(&tracker, 1, 200, S(1, 150), 21));
	UT_ASSERT_EQ(scn_time_cmp(tracker.threads[0].last_apply_scn, S(1, 200)), 0);
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

UT_TEST(test_barrier_not_record_scn_advances_read_floor)
{
	ClusterAdgScnTracker tracker;

	UT_ASSERT(cluster_adg_scn_tracker_init(&tracker, 2));
	UT_ASSERT(cluster_adg_mark_received(&tracker, 1, 1000, 10));
	UT_ASSERT(cluster_adg_mark_received(&tracker, 2, 1000, 10));
	UT_ASSERT(cluster_adg_mark_applied(&tracker, 1, 900, S(1, 900), 20));
	UT_ASSERT(cluster_adg_mark_applied(&tracker, 2, 900, S(2, 10), 20));
	UT_ASSERT_EQ(cluster_adg_scn_tracker_consistent_scn(&tracker), InvalidScn);
	UT_ASSERT(cluster_adg_apply_thread_barrier(&tracker, 1, 950, S(1, 80), 21));
	UT_ASSERT(cluster_adg_apply_thread_barrier(&tracker, 2, 950, S(2, 70), 21));
	UT_ASSERT_EQ(scn_time_cmp(cluster_adg_scn_tracker_consistent_scn(&tracker), S(2, 70)), 0);
}

UT_TEST(test_thread_bounds_are_release_checked)
{
	ClusterAdgScnTracker tracker;

	UT_ASSERT(cluster_adg_scn_tracker_init(&tracker, 1));
	UT_ASSERT(!cluster_adg_mark_received(&tracker, 0, 1, 1));
	UT_ASSERT(!cluster_adg_mark_received(&tracker, 2, 1, 1));
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
	UT_ASSERT_EQ(out.lease_epoch, (uint64)1);
	UT_ASSERT_EQ(out.owner_incarnation, (uint64)1);

	slot[offsetof(ClusterAdgApplyMasterLease, generation)] ^= 0x01;
	UT_ASSERT(!cluster_adg_apply_master_lease_unpack(slot, &out));
}

UT_TEST(test_apply_master_lease_slot_zero_fills_tail)
{
	ClusterAdgApplyMasterLease lease;
	uint8 slot[CLUSTER_ADG_APPLY_LEASE_SLOT_BYTES];

	memset(slot, 0x5A, sizeof(slot));
	cluster_adg_apply_master_lease_init(&lease, 8, 4, 2000, 12);
	UT_ASSERT(cluster_adg_apply_master_lease_pack(slot, &lease));
	for (uint32 i = sizeof(ClusterAdgApplyMasterLease); i < sizeof(slot); i++)
		UT_ASSERT_EQ((int)slot[i], 0);
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

	cluster_adg_apply_master_lease_init_full(&lease, 7, 3, 1000, 11, 0, 1);
	UT_ASSERT(!cluster_adg_apply_master_lease_valid(&lease));

	cluster_adg_apply_master_lease_init_full(&lease, 7, 3, 1000, 11, 1, 0);
	UT_ASSERT(!cluster_adg_apply_master_lease_valid(&lease));
}

UT_TEST(test_apply_master_lease_quorum_selects_single_winner)
{
	ClusterAdgApplyMasterLease leases[3];
	bool valid[3] = { true, true, true };
	ClusterAdgApplyMasterLeaseQuorum result;

	cluster_adg_apply_master_lease_init(&leases[0], 7, 1, 1000, 11);
	cluster_adg_apply_master_lease_init(&leases[1], 7, 1, 1200, 11);
	cluster_adg_apply_master_lease_init(&leases[2], 7, 2, 1300, 11);

	UT_ASSERT(cluster_adg_apply_master_lease_quorum(leases, valid, 3, 2, &result));
	UT_ASSERT(result.attached);
	UT_ASSERT_EQ(result.count, 2);
	UT_ASSERT_EQ(result.durable_term, (uint64)7);
	UT_ASSERT_EQ(result.owner_node_id, 1);
	UT_ASSERT_EQ(result.lease_expires_at_ms, (int64)1200);
	UT_ASSERT_EQ(result.generation, (uint64)11);

	cluster_adg_apply_master_lease_init(&leases[0], 8, 2, 2000, 12);
	cluster_adg_apply_master_lease_init(&leases[1], 7, 1, 1200, 11);
	cluster_adg_apply_master_lease_init(&leases[2], 8, 2, 2100, 12);
	UT_ASSERT(cluster_adg_apply_master_lease_quorum(leases, valid, 3, 2, &result));
	UT_ASSERT(result.attached);
	UT_ASSERT_EQ(result.count, 2);
	UT_ASSERT_EQ(result.durable_term, (uint64)8);
	UT_ASSERT_EQ(result.owner_node_id, 2);
	UT_ASSERT_EQ(result.lease_expires_at_ms, (int64)2100);
	UT_ASSERT_EQ(result.generation, (uint64)12);
}

UT_TEST(test_apply_master_lease_quorum_rejects_split_without_majority)
{
	ClusterAdgApplyMasterLease leases[3];
	bool valid[3] = { true, true, true };
	ClusterAdgApplyMasterLeaseQuorum result;

	cluster_adg_apply_master_lease_init(&leases[0], 7, 1, 1000, 11);
	cluster_adg_apply_master_lease_init(&leases[1], 7, 2, 1000, 11);
	cluster_adg_apply_master_lease_init(&leases[2], 7, 3, 1000, 11);

	UT_ASSERT(cluster_adg_apply_master_lease_quorum(leases, valid, 3, 2, &result));
	UT_ASSERT(!result.attached);

	valid[2] = false;
	UT_ASSERT(cluster_adg_apply_master_lease_quorum(leases, valid, 3, 2, &result));
	UT_ASSERT(!result.attached);
	UT_ASSERT(!cluster_adg_apply_master_lease_quorum(leases, valid, 3, 4, &result));
}

UT_TEST(test_apply_master_lease_cas_verdict)
{
	ClusterAdgApplyMasterLeaseQuorum current;
	ClusterAdgApplyMasterLease desired;
	int64 now_ms = 1000;

	memset(&current, 0, sizeof(current));
	current.owner_node_id = -1;

	cluster_adg_apply_master_lease_init(&desired, 1, 1, 2000, 1);
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_cas_verdict(&current, &desired, now_ms),
				 (int)CLUSTER_ADG_APPLY_LEASE_CAS_TAKE_EMPTY);

	cluster_adg_apply_master_lease_init(&desired, 2, 1, 2000, 1);
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_cas_verdict(&current, &desired, now_ms),
				 (int)CLUSTER_ADG_APPLY_LEASE_CAS_STALE);

	current.attached = true;
	current.durable_term = 7;
	current.owner_node_id = 2;
	current.lease_expires_at_ms = 3000;
	current.generation = 11;
	current.lease_epoch = 1;
	current.owner_incarnation = 1;

	cluster_adg_apply_master_lease_init(&desired, 7, 2, 4000, 12);
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_cas_verdict(&current, &desired, now_ms),
				 (int)CLUSTER_ADG_APPLY_LEASE_CAS_RENEW);

	cluster_adg_apply_master_lease_init_full(&desired, 7, 2, 4000, 12, 1, 2);
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_cas_verdict(&current, &desired, now_ms),
				 (int)CLUSTER_ADG_APPLY_LEASE_CAS_STALE);

	cluster_adg_apply_master_lease_init_full(&desired, 7, 2, 4000, 12, 2, 1);
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_cas_verdict(&current, &desired, now_ms),
				 (int)CLUSTER_ADG_APPLY_LEASE_CAS_STALE);

	cluster_adg_apply_master_lease_init(&desired, 7, 3, 4000, 12);
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_cas_verdict(&current, &desired, now_ms),
				 (int)CLUSTER_ADG_APPLY_LEASE_CAS_STALE);

	now_ms = 3500;
	cluster_adg_apply_master_lease_init(&desired, 8, 3, 5000, 12);
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_cas_verdict(&current, &desired, now_ms),
				 (int)CLUSTER_ADG_APPLY_LEASE_CAS_TAKE_EXPIRED);

	cluster_adg_apply_master_lease_init(&desired, 7, 3, 5000, 12);
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_cas_verdict(&current, &desired, now_ms),
				 (int)CLUSTER_ADG_APPLY_LEASE_CAS_STALE);

	cluster_adg_apply_master_lease_init(&desired, 8, 3, 3000, 12);
	UT_ASSERT_EQ((int)cluster_adg_apply_master_lease_cas_verdict(&current, &desired, now_ms),
				 (int)CLUSTER_ADG_APPLY_LEASE_CAS_INVALID);
}

UT_TEST(test_apply_master_candidate_uses_lowest_fresh_live_node)
{
	uint8 alive[16];

	memset(alive, 0, sizeof(alive));
	UT_ASSERT_EQ(cluster_adg_apply_master_candidate_node(alive, sizeof(alive)), -1);
	UT_ASSERT_EQ(cluster_adg_apply_master_candidate_node(NULL, sizeof(alive)), -1);
	UT_ASSERT_EQ(cluster_adg_apply_master_candidate_node(alive, 0), -1);

	alive[0] = (uint8)((1u << 5) | (1u << 2));
	alive[1] = (uint8)(1u << 1);
	UT_ASSERT_EQ(cluster_adg_apply_master_candidate_node(alive, sizeof(alive)), 2);

	alive[0] = 0;
	UT_ASSERT_EQ(cluster_adg_apply_master_candidate_node(alive, sizeof(alive)), 9);
	UT_ASSERT_EQ(cluster_adg_apply_master_candidate_node(alive, 1), -1);
	UT_ASSERT(cluster_adg_apply_master_candidate_allows_owner(alive, sizeof(alive), 9));
	UT_ASSERT(!cluster_adg_apply_master_candidate_allows_owner(alive, sizeof(alive), 10));
	UT_ASSERT(!cluster_adg_apply_master_candidate_allows_owner(alive, 1, 9));
}

UT_TEST(test_apply_master_token_apply_gate)
{
	UT_ASSERT(
		cluster_adg_apply_master_token_allows_apply(2, 1, 7, 11, 5, 99, 2000, 2, 5, 99, 7, 1000));
	UT_ASSERT(
		cluster_adg_apply_master_token_allows_apply(2, 1, 7, 11, 5, 99, 2000, 2, 5, 0, 0, 1000));

	UT_ASSERT(
		!cluster_adg_apply_master_token_allows_apply(3, 1, 7, 11, 5, 99, 2000, 2, 5, 99, 7, 1000));
	UT_ASSERT(
		!cluster_adg_apply_master_token_allows_apply(2, 0, 7, 11, 5, 99, 2000, 2, 5, 99, 7, 1000));
	UT_ASSERT(
		!cluster_adg_apply_master_token_allows_apply(2, 1, 7, 11, 4, 99, 2000, 2, 5, 99, 7, 1000));
	UT_ASSERT(
		!cluster_adg_apply_master_token_allows_apply(2, 1, 7, 11, 5, 98, 2000, 2, 5, 99, 7, 1000));
	UT_ASSERT(
		!cluster_adg_apply_master_token_allows_apply(2, 1, 7, 11, 5, 99, 2000, 2, 5, 99, 6, 1000));
	UT_ASSERT(
		!cluster_adg_apply_master_token_allows_apply(2, 1, 7, 11, 5, 99, 1000, 2, 5, 99, 7, 1000));
	UT_ASSERT(
		!cluster_adg_apply_master_token_allows_apply(2, 1, 0, 11, 5, 99, 2000, 2, 5, 99, 0, 1000));
	UT_ASSERT(
		!cluster_adg_apply_master_token_allows_apply(2, 1, 7, 0, 5, 99, 2000, 2, 5, 99, 7, 1000));
	UT_ASSERT(
		!cluster_adg_apply_master_token_allows_apply(2, 1, 7, 11, 5, 0, 2000, 2, 5, 0, 0, 1000));
	UT_ASSERT(
		!cluster_adg_apply_master_token_allows_apply(2, 1, 7, 11, 5, 99, 2000, -1, 5, 0, 0, 1000));
}

UT_TEST(test_read_only_decision_matrix)
{
	UT_ASSERT_EQ((int)cluster_adg_read_only_decide(true, true, true, S(2, 100), 5, 10),
				 (int)CLUSTER_ADG_READ_ALLOW);
	UT_ASSERT_EQ((int)cluster_adg_read_only_decide(true, true, true, S(2, 100), 11, 10),
				 (int)CLUSTER_ADG_READ_LAG_EXCESSIVE);
	UT_ASSERT_EQ((int)cluster_adg_read_only_decide(false, true, true, S(2, 100), 5, 10),
				 (int)CLUSTER_ADG_READ_UNRESOLVABLE);
	UT_ASSERT_EQ((int)cluster_adg_read_only_decide(true, true, false, S(2, 100), 5, 10),
				 (int)CLUSTER_ADG_READ_UNRESOLVABLE);
	UT_ASSERT_EQ((int)cluster_adg_read_only_decide(true, true, true, InvalidScn, 5, 10),
				 (int)CLUSTER_ADG_READ_UNRESOLVABLE);
}

UT_TEST(test_thread_barrier_wal_abi)
{
	xl_cluster_adg_thread_barrier rec;

	memset(&rec, 0, sizeof(rec));
	rec.thread_id = 4;
	rec.primary_thread_count = 2;
	rec.thread_safe_scn = S(3, 99);

	UT_ASSERT_EQ((int)sizeof(xl_cluster_adg_thread_barrier), 16);
	UT_ASSERT_EQ((int)offsetof(xl_cluster_adg_thread_barrier, primary_thread_count), 2);
	UT_ASSERT_EQ((int)offsetof(xl_cluster_adg_thread_barrier, thread_safe_scn), 8);
	UT_ASSERT_EQ((int)(XLOG_CLUSTER_ADG_THREAD_BARRIER & XLR_INFO_MASK), 0);
	UT_ASSERT_EQ((int)RM_CLUSTER_ADG_ID, (int)(RM_CLUSTER_RAW_LAYOUT_ID + 1));
	UT_ASSERT_EQ((int)rec.primary_thread_count, 2);
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

UT_TEST(test_mrp_shmem_tracks_term_validity_and_drain)
{
	UT_ASSERT_EQ((int)offsetof(ClusterMrpSharedState, apply_master_term_valid)
					 > (int)offsetof(ClusterMrpSharedState, apply_master_node_id),
				 1);
	UT_ASSERT_EQ((int)offsetof(ClusterMrpSharedState, apply_master_term_valid_until_ms)
					 > (int)offsetof(ClusterMrpSharedState, apply_master_generation),
				 1);
	UT_ASSERT_EQ((int)offsetof(ClusterMrpSharedState, apply_master_generation)
					 > (int)offsetof(ClusterMrpSharedState, apply_master_term),
				 1);
	UT_ASSERT_EQ((int)offsetof(ClusterMrpSharedState, apply_master_lost_at_ms)
					 > (int)offsetof(ClusterMrpSharedState, apply_master_term_valid_until_ms),
				 1);
	UT_ASSERT_EQ((int)offsetof(ClusterMrpSharedState, pending_apply_lease_seq)
					 > (int)offsetof(ClusterMrpSharedState, apply_lease_request_seq),
				 1);
	UT_ASSERT_EQ((int)offsetof(ClusterMrpSharedState, apply_lease_completion_seq)
					 > (int)offsetof(ClusterMrpSharedState, pending_apply_lease_seq),
				 1);
	UT_ASSERT_EQ((int)offsetof(ClusterMrpSharedState, primary_thread_count)
					 > (int)offsetof(ClusterMrpSharedState, stopped_at_us),
				 1);
	UT_ASSERT_EQ((int)offsetof(ClusterMrpSharedState, standby_consistent_scn)
					 > (int)offsetof(ClusterMrpSharedState, apply_lsn),
				 1);
}

int
main(void)
{
	UT_PLAN(21);

	UT_RUN(test_tracker_init_bounds);
	UT_RUN(test_barrier_min_publishes_after_all_threads);
	UT_RUN(test_receive_watermark_does_not_publish_consistent_scn);
	UT_RUN(test_apply_cannot_overtake_receive);
	UT_RUN(test_non_monotone_record_scn_does_not_block_apply_or_publish);
	UT_RUN(test_barrier_retreat_is_rejected);
	UT_RUN(test_barrier_not_record_scn_advances_read_floor);
	UT_RUN(test_thread_bounds_are_release_checked);
	UT_RUN(test_apply_master_next_term_overflow);
	UT_RUN(test_apply_master_lease_marker_pack_unpack);
	UT_RUN(test_apply_master_lease_slot_zero_fills_tail);
	UT_RUN(test_apply_master_lease_marker_rejects_invalid_fields);
	UT_RUN(test_apply_master_lease_quorum_selects_single_winner);
	UT_RUN(test_apply_master_lease_quorum_rejects_split_without_majority);
	UT_RUN(test_apply_master_lease_cas_verdict);
	UT_RUN(test_apply_master_candidate_uses_lowest_fresh_live_node);
	UT_RUN(test_apply_master_token_apply_gate);
	UT_RUN(test_read_only_decision_matrix);
	UT_RUN(test_thread_barrier_wal_abi);
	UT_RUN(test_standby_reply_trailer_validation);
	UT_RUN(test_mrp_shmem_tracks_term_validity_and_drain);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
