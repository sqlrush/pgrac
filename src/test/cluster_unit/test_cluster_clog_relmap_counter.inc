/*-------------------------------------------------------------------------
 * test_cluster_clog_relmap_counter.inc
 *     Original native next FullXid input on held CLOG observations.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_relmap_counter.inc
 * NOTES
 *     The original getter and native file reads execute; VariableCache,
 *     locks and episode authority remain controlled. No era/lifetime grant.
 *-------------------------------------------------------------------------
 */
static void
relmap_counter_done(void)
{
	relmap_base_recheck_hook = NULL;
	ShmemVariableCache = &fixture_variable_cache;
	fixture_variable_cache.nextXid = FullTransactionIdFromU64(0);
	fixture_origin = c0_origin = 1;
	authority = InRecovery = true;
	c0_authority = relmap_base_interrupts = false;
	fixture_idle();
}

UT_TEST(relmap_own_actual_next_counter_is_only_an_observation)
{
	const uint64 next[] = {0, (UINT64CONST(7) << 32) | 101,
		(UINT64CONST(8) << 32) | FirstNormalTransactionId, UINT64_MAX};
	for (unsigned i = 0; i < lengthof(next); i++) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		fixture_variable_cache.nextXid = FullTransactionIdFromU64(next[i]);
		UT_ASSERT(relmap_mutation_open(0, i, &base, &held));
		if (held == NULL) { relmap_counter_done(); return; }
		UT_ASSERT(base.native_next_observed);
		UT_ASSERT_EQ(base.next_full_xid, next[i]);
		UT_ASSERT_EQ(base.queried_full_xid, relmap_base_query());
		UT_ASSERT_EQ(base.status, i);
		UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held));
		UT_ASSERT_EQ(base.next_full_xid, next[i]);
		UT_ASSERT_EQ(U64FromFullTransactionId(fixture_variable_cache.nextXid), next[i]);
		cluster_clog_relmap_base_release_v1(&held);
	}
	relmap_counter_done();
}

UT_TEST(relmap_foreign_counter_never_borrows_local_next)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	fixture_variable_cache.nextXid = FullTransactionIdFromU64(UINT64_MAX);
	UT_ASSERT(relmap_mutation_open(1, TRANSACTION_STATUS_COMMITTED, &base, &held));
	if (held == NULL) { relmap_counter_done(); return; }
	UT_ASSERT(!base.native_next_observed); UT_ASSERT_EQ(base.next_full_xid, 0);
	ShmemVariableCache = NULL;
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	UT_ASSERT(!base.native_next_observed); UT_ASSERT_EQ(base.next_full_xid, 0);
	ShmemVariableCache = &fixture_variable_cache;
	fixture_variable_cache.nextXid = FullTransactionIdFromU64(3);
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	UT_ASSERT(!base.native_next_observed); UT_ASSERT_EQ(base.next_full_xid, 0);
	cluster_clog_relmap_base_release_v1(&held);
	relmap_counter_done();
}

UT_TEST(relmap_own_counter_drift_keeps_original_baseline_and_fd)
{
	ClusterRelmapClogBaseV1 base, before;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	uint64 generation, after, next = (UINT64CONST(7) << 32) | 101;
	bool ok;
	fixture_variable_cache.nextXid = FullTransactionIdFromU64(next);
	UT_ASSERT(relmap_mutation_open(0, TRANSACTION_STATUS_COMMITTED, &base, &held));
	if (held == NULL) { relmap_counter_done(); return; }
	before = base;
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &generation));
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	fixture_variable_cache.nextXid = FullTransactionIdFromU64(next + 1);
	LWLockRelease(XidGenLock);
	ok = cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held);
	UT_ASSERT(!ok);
	if (!ok) {
		ClusterRelmapClogBaseV1 empty = {0};
		UT_ASSERT_EQ(memcmp(&base, &empty, sizeof(base)), 0);
		UT_ASSERT(!authority); UT_ASSERT_EQ(transient_fds, 1);
		UT_ASSERT_EQ(memcmp(&held->base, &before, sizeof(before)), 0);
		UT_ASSERT(clog_relmap_mutation_snapshot(0, &after));
		UT_ASSERT_EQ(after, generation);
		fixture_variable_cache.nextXid = FullTransactionIdFromU64(next);
		relmap_horizon_already_failed(&base, &held);
	} else
		cluster_clog_relmap_base_release_v1(&held);
	relmap_counter_done();
}

static unsigned relmap_counter_change_at;

static void
relmap_counter_change_during_root(void)
{
	if (relmap_base_rechecks == relmap_counter_change_at) {
		/* The original ROOT callback has already checked every native lock. */
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		fixture_variable_cache.nextXid = FullTransactionIdFromU64(
			U64FromFullTransactionId(fixture_variable_cache.nextXid) + 1);
		LWLockRelease(XidGenLock);
		relmap_base_recheck_hook = NULL;
	}
}

UT_TEST(relmap_counter_change_during_first_or_final_root_refuses)
{
	for (int existing = 0; existing < 2; existing++) {
		for (unsigned phase = 1; phase <= 2; phase++) {
			ClusterRelmapClogBaseV1 base, before = {0}, empty = {0};
			ClusterRelmapClogBaseHandleV1 *held = NULL;
			uint64 next = (UINT64CONST(7) << 32) | 101;
			bool ok;
			relmap_base_reset(0); fixture_zero(0);
			fixture_variable_cache.nextXid = FullTransactionIdFromU64(next);
			if (existing) {
				UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
					relmap_base_query(), &base, &held));
				if (held == NULL) { relmap_counter_done(); return; }
				before = base;
			}
			relmap_counter_change_at = relmap_base_rechecks + phase;
			relmap_base_recheck_hook = relmap_counter_change_during_root;
			ok = cluster_clog_relmap_base_read_v1(&relmap_base_source,
				relmap_base_query(), &base, &held);
			relmap_base_recheck_hook = NULL;
			UT_ASSERT(!ok);
			if (!ok) {
				UT_ASSERT_EQ(memcmp(&base, &empty, sizeof(base)), 0);
				UT_ASSERT(!authority); UT_ASSERT_EQ(transient_fds, existing);
				if (existing) {
					UT_ASSERT_EQ(memcmp(&held->base, &before, sizeof(before)), 0);
					fixture_variable_cache.nextXid = FullTransactionIdFromU64(next);
					relmap_horizon_already_failed(&base, &held);
				} else
					UT_ASSERT(held == NULL);
			}
			cluster_clog_relmap_base_release_v1(&held);
		}
	}
	relmap_counter_done();
}
