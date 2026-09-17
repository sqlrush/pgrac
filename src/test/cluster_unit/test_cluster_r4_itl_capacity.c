/*-------------------------------------------------------------------------
 * test_cluster_r4_itl_capacity.c
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Real heap capacity consumer + real transaction resolver + real admission.
 * Only the origin response, buffer owner and actual transaction wait are
 * fixture boundaries. The terminal-only publication filter is not mocked.
 *-------------------------------------------------------------------------
 */
#define main itl_unused_lock_order_main
#define cluster_tx_locator_from_itl itl_fixture_locator
#define cluster_tx_locator_from_itl_terminal_census itl_fixture_census_locator
#define cluster_tx_resolve_exact itl_fixture_resolve
#define cluster_tx_resolve_exact_admitted itl_fixture_resolve_admitted
#define cluster_tx_resolve_terminal_census_retained_admitted itl_fixture_retained
#define cluster_tx_resolve_terminal_census_batch_preflight itl_fixture_preflight
#define cluster_tx_resolve_reason_name itl_fixture_reason_name
#include "test_cluster_r4_lock_order.c"
#undef cluster_tx_resolve_reason_name
#undef cluster_tx_resolve_terminal_census_batch_preflight
#undef cluster_tx_resolve_terminal_census_retained_admitted
#undef cluster_tx_resolve_exact_admitted
#undef cluster_tx_resolve_exact
#undef cluster_tx_locator_from_itl_terminal_census
#undef cluster_tx_locator_from_itl
#undef main

#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_r4_observe.h"

static unsigned origin_calls;
static unsigned active_publications;
static int origin_fault;

void
cluster_runtime_visibility_ensure_exit_hooks(void)
{}

void
cluster_r4_observe(ClusterR4Event event, ClusterTxResolveReason reason,
				   ClusterCrBuildReason build_reason)
{
	if (event == CLUSTER_R4_EVENT_TX_IN_PROGRESS && reason == CLUSTER_TX_RESOLVE_NONE)
		active_publications++;
}

static ClusterTxOutcome
itl_origin(const ClusterTxLocator *locator, uint64 epoch, ClusterTxResolution *out,
		   ClusterTxResolveReason *reason)
{
	origin_calls++;
	UT_ASSERT(!ut_hot_content_lock_held);
	if (ut_itl_pair_active) {
		UT_ASSERT(!ut_itl_pair_content_lock_held[0]);
		UT_ASSERT(!ut_itl_pair_content_lock_held[1]);
	}
	memset(out, 0, sizeof(*out));
	out->locator_echo = *locator;
	out->locator_echo.tt_wrap = 42;
	out->top_xid = locator->xid;
	out->outcome = CLUSTER_TX_IN_PROGRESS;
	out->proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	out->authority.origin_epoch = epoch;
	out->authority.tt_generation = 77;
	out->authority.authority_scn = 9002;
	*reason = CLUSTER_TX_RESOLVE_NONE;
	if (origin_fault == 1) {
		out->outcome = CLUSTER_TX_UNKNOWN;
		*reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	} else if (origin_fault == 2)
		out->locator_echo.xid++;
	else if (origin_fault == 3) {
		out->outcome = CLUSTER_TX_PREPARED;
		out->proof_kind = CLUSTER_TX_PROOF_ORIGIN_TWOPHASE;
	} else if (origin_fault == 4)
		pg_atomic_write_u64(&ut_itl_census_semantic.record_generation, 74);
	else if (origin_fault == 6)
		out->outcome = CLUSTER_TX_ABORTED;
	return out->outcome;
}

ClusterTxOutcome
cluster_runtime_visibility_resolve_exact_origin_admitted(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason)
{
	UT_ASSERT(mode == CLUSTER_TX_RESOLVE_VISIBILITY || mode == CLUSTER_TX_RESOLVE_TERMINAL_CENSUS);
	UT_ASSERT(admission->entered);
	return itl_origin(locator, admission->formation_epoch, out, reason);
}

ClusterTxOutcome
cluster_runtime_visibility_resolve_exact_origin(const ClusterTxLocator *locator,
												ClusterTxResolveMode mode, uint64 epoch,
												ClusterTxResolution *out,
												ClusterTxResolveReason *reason)
{
	return itl_origin(locator, epoch, out, reason);
}

ClusterTxOutcome
cluster_runtime_visibility_resolve_terminal_census_retained_exact(
	const ClusterTxLocator *locator, SCN retained_scn,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason)
{
	abort(); /* No precommit/retained terminal is supplied by this probe. */
}

UT_TEST(test_exact_active_owner_reaches_capacity_wait_through_real_resolver)
{
	for (int scenario = 0; scenario < 12; scenario++) {
		int pair = scenario % 2;
		int kind = (scenario / 2) % 3;
		UtR4HotProductFixture fixture;
		HeapHotSearchResult hot;
		uint64 deadline = 0;
		const char *reason = NULL;
		ClusterTxwResult actual;
		PGAlignedBlock before;

		ut_itl_census_begin(&fixture, &hot, kind == 1);
		origin_fault = 0;
		if (kind == 2)
			for (int i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
				ClusterItlSlotData *slot = &ClusterPageGetItlSlots((Page)fixture.live_page)[i];
				slot->flags = ITL_FLAG_NEEDS_CLEANOUT;
				slot->commit_scn = 9001;
			}
		if (scenario >= 6) {
			ut_cluster_conf.node_count = 4;
			cluster_r4_activation_test_current_epoch = 0;
			pg_atomic_write_u64(&ut_itl_census_semantic.formation_epoch, 0);
		}
		memcpy(before.data, fixture.live_page, BLCKSZ);
		/* Capacity must use ordinary admission before consuming a live
		 * blocker; a scoped terminal-only admission cannot grant that right. */
		pg_atomic_write_u64(&ut_itl_census_semantic.active_bits,
							CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
		origin_calls = active_publications = 0;
		if (pair) {
			ut_itl_pair_active = true;
			ut_itl_pair_content_lock_held[0] = true;
			ut_itl_pair_content_lock_held[1] = true;
			ut_hot_content_lock_held = false;
		}
		actual = cluster_heap_test_itl_wait_capacity(1, pair ? 2 : 1, 1, 9900, &deadline, &reason);
		printf("# pair=%d result=%d origin=%u active=%u wait=%d reason=%s\n", pair, actual,
			   origin_calls, active_publications, ut_itl_wait_calls, reason);
		UT_ASSERT_EQ(origin_calls, 8);
		UT_ASSERT_EQ(actual, CLUSTER_TXW_RESOLVED);
		UT_ASSERT_EQ(active_publications, 8);
		UT_ASSERT_EQ(ut_itl_wait_calls, 1);
		UT_ASSERT_EQ(ut_itl_wait_locator.xid, 1200);
		UT_ASSERT_EQ(ut_itl_wait_locator.tt_wrap, 42);
		UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
		UT_ASSERT_EQ(memcmp(before.data, fixture.live_page, BLCKSZ), 0);
		if (!pair && !ut_hot_content_lock_held)
			LockBuffer(1, BUFFER_LOCK_EXCLUSIVE); /* fixture teardown only */
		ut_itl_census_end();
	}
}

UT_TEST(test_capacity_keeps_unknown_identity_prepared_and_admission_refusals)
{
	for (int fault = 1; fault <= 5; fault++) {
		UtR4HotProductFixture fixture;
		HeapHotSearchResult hot;
		PGAlignedBlock before;
		uint64 deadline = 0;
		const char *reason = NULL;

		ut_itl_census_begin(&fixture, &hot, false);
		origin_fault = fault;
		origin_calls = active_publications = 0;
		if (fault != 5)
			pg_atomic_write_u64(&ut_itl_census_semantic.active_bits,
								CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
		memcpy(before.data, fixture.live_page, BLCKSZ);
		UT_ASSERT_EQ(cluster_heap_test_itl_wait_capacity(1, 1, 1, 9900, &deadline, &reason),
					 CLUSTER_TXW_UNPROVABLE);
		UT_ASSERT_EQ(ut_itl_wait_calls, 0);
		UT_ASSERT_EQ(deadline, 0);
		UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
		UT_ASSERT_EQ(memcmp(before.data, fixture.live_page, BLCKSZ), 0);
		if (!ut_hot_content_lock_held)
			LockBuffer(1, BUFFER_LOCK_EXCLUSIVE);
		ut_itl_census_end();
	}
}

/* The origin producer's TT-reuse proof is tested in r4_tx_outcome. Here its
 * exact terminal response traverses the real resolver and real heap census;
 * all eight old lock-only slots must be terminalized, not left active. */
UT_TEST(test_origin_abort_releases_eight_lock_only_slots_through_real_census)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult hot;
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &hot, true);
	origin_fault = 6;
	origin_calls = active_publications = 0;
	UT_ASSERT_EQ(cluster_heap_test_itl_capacity_outcome(UT_HOT_BUFFER, 9900, true),
				 CLUSTER_HEAP_ITL_CAPACITY_READY);
	slots = ClusterPageGetItlSlots((Page)fixture.live_page);
	for (int i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		UT_ASSERT_EQ(slots[i].flags, ITL_FLAG_LOCK_ONLY_ABORTED);
		UT_ASSERT_EQ(slots[i].commit_scn, InvalidScn);
	}
	UT_ASSERT_EQ(origin_calls, 8);
	UT_ASSERT_EQ(active_publications, 0);
	UT_ASSERT_EQ(ut_itl_wait_calls, 0);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	ut_itl_census_end();
}

UT_TEST(test_lock_capacity_does_not_mistake_self_data_slot_for_lock_capacity)
{
	for (int self_lock_slot = 0; self_lock_slot < 2; self_lock_slot++) {
		UtR4HotProductFixture fixture;
		HeapHotSearchResult hot;
		PGAlignedBlock before;
		uint64 deadline = 0;
		const char *reason = NULL;

		ut_itl_census_begin(&fixture, &hot, true);
		origin_fault = 0;
		origin_calls = active_publications = 0;
		ClusterPageGetItlSlots((Page)fixture.live_page)[0].flags
			= self_lock_slot ? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_ACTIVE;
		pg_atomic_write_u64(&ut_itl_census_semantic.active_bits,
							CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
		memcpy(before.data, fixture.live_page, BLCKSZ);
		UT_ASSERT_EQ(cluster_heap_test_itl_wait_lock_capacity(1, 1200, &deadline, &reason),
					 self_lock_slot ? CLUSTER_TXW_RETRY : CLUSTER_TXW_RESOLVED);
		UT_ASSERT_EQ(ut_itl_wait_calls, self_lock_slot ? 0 : 1);
		if (!self_lock_slot)
			UT_ASSERT_EQ(ut_itl_wait_locator.xid, 1201);
		UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
		UT_ASSERT_EQ(memcmp(before.data, fixture.live_page, BLCKSZ), 0);
		UT_ASSERT(!ut_hot_content_lock_held);
		LockBuffer(1, BUFFER_LOCK_EXCLUSIVE); /* fixture teardown only */
		ut_itl_census_end();
	}
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_exact_active_owner_reaches_capacity_wait_through_real_resolver);
	UT_RUN(test_capacity_keeps_unknown_identity_prepared_and_admission_refusals);
	UT_RUN(test_origin_abort_releases_eight_lock_only_slots_through_real_census);
	UT_RUN(test_lock_capacity_does_not_mistake_self_data_slot_for_lock_capacity);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
