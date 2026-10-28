/*-------------------------------------------------------------------------
 * test_cluster_clog_relmap_horizon.inc
 *     Original native CLOG truncation input on held-page observations.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_relmap_horizon.inc
 * NOTES
 *     Native file reads execute; VariableCache, locks, episode and fault
 *     delivery are controlled. Raw horizons do not certify an era/lifetime.
 *-------------------------------------------------------------------------
 */
static void
relmap_horizon_already_failed(ClusterRelmapClogBaseV1 *base,
	ClusterRelmapClogBaseHandleV1 **held)
{
	unsigned io = reads + syncs + dirsyncs;
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), base, held));
	UT_ASSERT_EQ(base->queried_full_xid, 0);
	UT_ASSERT_EQ(reads + syncs + dirsyncs, io);
	UT_ASSERT_EQ(transient_fds, 1);
	cluster_clog_relmap_base_release_v1(held); fixture_idle();
}

UT_TEST(relmap_own_raw_truncation_horizon_is_not_an_era)
{
	const TransactionId raw[] = {InvalidTransactionId, FirstNormalTransactionId, UINT32_MAX};
	for (unsigned i = 0; i < lengthof(raw); i++) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		fixture_variable_cache.oldestClogXid = raw[i];
		UT_ASSERT(relmap_mutation_open(0, i, &base, &held));
		if (held == NULL) return;
		UT_ASSERT(base.truncation_horizon_observed);
		UT_ASSERT_EQ(base.oldest_clog_xid, raw[i]);
		UT_ASSERT_EQ(base.queried_full_xid, relmap_base_query());
		UT_ASSERT_EQ(base.status, i);
		UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held));
		UT_ASSERT_EQ(base.oldest_clog_xid, raw[i]);
		cluster_clog_relmap_base_release_v1(&held); fixture_idle();
	}
	fixture_variable_cache.oldestClogXid = InvalidTransactionId;
}

UT_TEST(relmap_foreign_horizon_never_borrows_local_variable_cache)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	unsigned io;
	fixture_variable_cache.oldestClogXid = 17;
	UT_ASSERT(relmap_mutation_open(1, TRANSACTION_STATUS_COMMITTED, &base, &held));
	if (held == NULL) return;
	UT_ASSERT(!base.truncation_horizon_observed);
	UT_ASSERT_EQ(base.oldest_clog_xid, InvalidTransactionId);
	ShmemVariableCache = NULL;
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	UT_ASSERT(!base.truncation_horizon_observed);
	UT_ASSERT_EQ(base.oldest_clog_xid, InvalidTransactionId);
	ShmemVariableCache = &fixture_variable_cache;
	cluster_clog_relmap_base_release_v1(&held); fixture_idle();
	relmap_base_reset(0); fixture_zero(0);
	ShmemVariableCache = NULL; io = reads + syncs + dirsyncs;
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	UT_ASSERT(held == NULL); UT_ASSERT(authority);
	UT_ASSERT_EQ(reads + syncs + dirsyncs, io);
	ShmemVariableCache = &fixture_variable_cache;
	cluster_clog_relmap_base_release_v1(&held);
	fixture_variable_cache.oldestClogXid = InvalidTransactionId;
	fixture_idle();
}

UT_TEST(relmap_own_horizon_change_keeps_original_fd_and_failure)
{
	ClusterRelmapClogBaseV1 base, before;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	uint64 generation, after;
	fixture_variable_cache.oldestClogXid = 3;
	UT_ASSERT(relmap_mutation_open(0, TRANSACTION_STATUS_COMMITTED, &base, &held));
	if (held == NULL) return;
	before = base;
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &generation));
	LWLockAcquire(XactTruncationLock, LW_EXCLUSIVE);
	fixture_variable_cache.oldestClogXid = 17;
	LWLockRelease(XactTruncationLock);
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	UT_ASSERT_EQ(base.queried_full_xid, 0);
	UT_ASSERT_EQ(transient_fds, 1); UT_ASSERT(!authority);
	UT_ASSERT_EQ(held->base.oldest_clog_xid, 3);
	UT_ASSERT_EQ(memcmp(held->base.image, before.image, BLCKSZ), 0);
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &after));
	UT_ASSERT_EQ(after, generation);
	fixture_variable_cache.oldestClogXid = 3;
	relmap_horizon_already_failed(&base, &held);
	fixture_variable_cache.oldestClogXid = InvalidTransactionId;
}

static void
relmap_horizon_fault_during_sync(void)
{
	/* Deliberate provider fault under the held lock, not a legal concurrent writer. */
	UT_ASSERT(LWLockHeldByMe(XactTruncationLock));
	fixture_variable_cache.oldestClogXid++;
	fixture_native_sync_hook = NULL;
}

UT_TEST(relmap_own_horizon_drift_during_native_read_refuses)
{
	for (int existing = 0; existing < 2; existing++) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		relmap_base_reset(0); fixture_zero(0);
		fixture_variable_cache.oldestClogXid = 3;
		if (existing) {
			UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
				relmap_base_query(), &base, &held));
			if (held == NULL) return;
		}
		fixture_native_sync_hook = relmap_horizon_fault_during_sync;
		UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held));
		fixture_native_sync_hook = NULL;
		UT_ASSERT_EQ(base.queried_full_xid, 0); UT_ASSERT(!authority);
		UT_ASSERT_EQ(transient_fds, existing);
		if (existing) {
			UT_ASSERT_EQ(held->base.oldest_clog_xid, 3);
			fixture_variable_cache.oldestClogXid = 3;
			relmap_horizon_already_failed(&base, &held);
		} else {
			UT_ASSERT(held == NULL);
			cluster_clog_relmap_base_release_v1(&held); fixture_idle();
		}
	}
	fixture_variable_cache.oldestClogXid = InvalidTransactionId;
}
