/*-------------------------------------------------------------------------
 * test_cluster_clog_relmap_native_counter.inc
 *     Original recovery counter advancement and held CLOG observations.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_relmap_native_counter.inc
 * NOTES
 *     Native counter producer/getter and file reads execute. Locks,
 *     VariableCache storage and episode authority remain controlled.
 *-------------------------------------------------------------------------
 */
static void
relmap_native_counter_done(void)
{
	IsUnderPostmaster = false;
	relmap_counter_done();
}

UT_TEST(relmap_native_advance_refuses_old_counter_without_page_mutation)
{
	const struct { uint64 start; TransactionId xid; uint64 next; } cases[] = {
		{UINT64_C(0x700000065), 101, UINT64_C(0x700000066)},
		{UINT64_C(0x7fffffffe), UINT32_MAX, UINT64_C(0x800000003)},
		{UINT64_C(0x800000003), 3, UINT64_C(0x800000004)}
	};
	for (unsigned i = 0; i < lengthof(cases); i++) {
		ClusterRelmapClogBaseV1 base, before, empty = {0};
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		uint64 generation, after;
		unsigned io;
		bool ok;
		fixture_variable_cache.nextXid = FullTransactionIdFromU64(cases[i].start);
		UT_ASSERT(relmap_mutation_open(0, TRANSACTION_STATUS_COMMITTED, &base, &held));
		if (held == NULL) { relmap_native_counter_done(); return; }
		before = base; io = writes + reads + syncs + dirsyncs;
		UT_ASSERT(clog_relmap_mutation_snapshot(0, &generation));
		IsUnderPostmaster = true;
		UT_ASSERT(AmStartupProcess());
		fixture_native_advance_next_full_xid(cases[i].xid);
		UT_ASSERT_EQ(U64FromFullTransactionId(ReadNextFullTransactionId()), cases[i].next);
		UT_ASSERT_EQ(writes + reads + syncs + dirsyncs, io);
		UT_ASSERT(clog_relmap_mutation_snapshot(0, &after));
		UT_ASSERT_EQ(after, generation);
		ok = cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held);
		UT_ASSERT(!ok);
		if (!ok) {
			UT_ASSERT_EQ(memcmp(&base, &empty, sizeof(base)), 0);
			UT_ASSERT_EQ(memcmp(&held->base, &before, sizeof(before)), 0);
			UT_ASSERT_EQ(transient_fds, 1); UT_ASSERT(!authority);
			relmap_horizon_already_failed(&base, &held);
		} else
			cluster_clog_relmap_base_release_v1(&held);
	}
	relmap_native_counter_done();
}

UT_TEST(relmap_native_advance_noop_and_foreign_keep_original_observation)
{
	const struct { int origin; uint64 start; TransactionId xid; uint64 next; } cases[] = {
		{0, UINT64_C(0x70000006a), 105, UINT64_C(0x70000006a)},
		{0, UINT64_C(0x800000003), UINT32_MAX, UINT64_C(0x800000003)},
		{1, UINT64_C(0x700000065), 102, UINT64_C(0x700000067)}
	};
	for (unsigned i = 0; i < lengthof(cases); i++) {
		ClusterRelmapClogBaseV1 base, before;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		unsigned io;
		fixture_variable_cache.nextXid = FullTransactionIdFromU64(cases[i].start);
		UT_ASSERT(relmap_mutation_open(cases[i].origin, TRANSACTION_STATUS_ABORTED,
			&base, &held));
		if (held == NULL) { relmap_native_counter_done(); return; }
		before = base; io = writes + reads + syncs + dirsyncs;
		IsUnderPostmaster = true;
		UT_ASSERT(AmStartupProcess());
		fixture_native_advance_next_full_xid(cases[i].xid);
		UT_ASSERT_EQ(U64FromFullTransactionId(ReadNextFullTransactionId()), cases[i].next);
		UT_ASSERT_EQ(writes + reads + syncs + dirsyncs, io);
		UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held));
		UT_ASSERT_EQ(memcmp(&base, &before, sizeof(base)), 0);
		UT_ASSERT(authority); UT_ASSERT_EQ(transient_fds, 1);
		cluster_clog_relmap_base_release_v1(&held);
	}
	relmap_native_counter_done();
}

static unsigned relmap_native_counter_change_at;

static void
relmap_native_counter_during_root(void)
{
	if (relmap_base_rechecks == relmap_native_counter_change_at) {
		UT_ASSERT(AmStartupProcess());
		fixture_native_advance_next_full_xid(101);
		relmap_base_recheck_hook = NULL;
	}
}

UT_TEST(relmap_native_advance_during_root_keeps_failure_and_custody)
{
	for (int existing = 0; existing < 2; existing++) {
		for (unsigned phase = 1; phase <= 2; phase++) {
			ClusterRelmapClogBaseV1 base, before = {0}, empty = {0};
			ClusterRelmapClogBaseHandleV1 *held = NULL;
			bool ok;
			relmap_base_reset(0); fixture_zero(0);
			fixture_variable_cache.nextXid = FullTransactionIdFromU64(UINT64_C(0x700000065));
			IsUnderPostmaster = true;
			if (existing) {
				UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
					relmap_base_query(), &base, &held));
				if (held == NULL) { relmap_native_counter_done(); return; }
				before = base;
			}
			relmap_native_counter_change_at = relmap_base_rechecks + phase;
			relmap_base_recheck_hook = relmap_native_counter_during_root;
			ok = cluster_clog_relmap_base_read_v1(&relmap_base_source,
				relmap_base_query(), &base, &held);
			relmap_base_recheck_hook = NULL;
			UT_ASSERT_EQ(U64FromFullTransactionId(ReadNextFullTransactionId()),
				UINT64_C(0x700000066));
			UT_ASSERT(!ok);
			if (!ok) {
				UT_ASSERT_EQ(memcmp(&base, &empty, sizeof(base)), 0);
				UT_ASSERT(!authority); UT_ASSERT_EQ(transient_fds, existing);
				if (existing) {
					UT_ASSERT_EQ(memcmp(&held->base, &before, sizeof(before)), 0);
					relmap_horizon_already_failed(&base, &held);
				} else
					UT_ASSERT(held == NULL);
			}
			cluster_clog_relmap_base_release_v1(&held);
		}
	}
	relmap_native_counter_done();
}
