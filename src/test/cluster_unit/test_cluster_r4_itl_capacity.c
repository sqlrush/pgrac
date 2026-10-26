/*-------------------------------------------------------------------------
 * test_cluster_r4_itl_capacity.c
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Real heap capacity consumer, allocator predicate, resolver and admission.
 * Origin replies, startup materialization facts, buffer ownership and TX
 * wakeups are fixture boundaries. Publication and history protection are real.
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
#define cluster_itl_has_allocatable_slot itl_fixture_has_allocatable_slot
#define cluster_itl_alloc_or_reuse_slot itl_fixture_alloc_or_reuse_slot
#define cluster_itl_alloc_or_reuse_lock_slot itl_fixture_alloc_or_reuse_lock_slot
#define cluster_itl_find_data_slot_index_by_xid itl_fixture_find_data_slot_index_by_xid
#define cluster_itl_get_tt_ref itl_fixture_get_tt_ref
#define cluster_itl_find_lock_slot_index_by_xmax itl_fixture_find_lock_slot_index_by_xmax
#define cluster_itl_find_multixact_origin_by_xmax itl_fixture_find_multixact_origin_by_xmax
#define cluster_itl_find_lock_tt_ref_by_xmax itl_fixture_find_lock_tt_ref_by_xmax
#define cluster_bufmgr_block_write_permitted itl_fixture_block_write_permitted
#include "test_cluster_r4_lock_order.c"
#undef cluster_bufmgr_block_write_permitted
#undef cluster_itl_find_lock_tt_ref_by_xmax
#undef cluster_itl_find_multixact_origin_by_xmax
#undef cluster_itl_find_lock_slot_index_by_xmax
#undef cluster_itl_get_tt_ref
#undef cluster_itl_find_data_slot_index_by_xid
#undef cluster_itl_alloc_or_reuse_lock_slot
#undef cluster_itl_alloc_or_reuse_slot
#undef cluster_itl_has_allocatable_slot
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
#include "cluster/cluster_recovery_merge.h"

static unsigned origin_calls;
static unsigned active_publications;
static int origin_fault;

/* Materialization is a startup fact, constant throughout this process. */
bool
cluster_merged_any_remote_materialized(void)
{
	return true;
}

bool
cluster_merged_instance_is_materialized(NodeId origin)
{
	return origin == 1;
}

bool cluster_bufmgr_block_write_permitted(Buffer buffer);
bool
cluster_bufmgr_block_write_permitted(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, (Buffer)1);
	UT_ASSERT(ut_itl_pair_active ? ut_itl_pair_content_lock_held[0] : ut_hot_content_lock_held);
	return ut_update_write_permitted;
}

/* Nonshared hint publication in this fixture retains its native semantics. */
ClusterSpaceHintResult
cluster_space_hint_begin(Buffer buffer pg_attribute_unused(),
						 RfPageProducerBatchV1 *batch pg_attribute_unused())
{
	UT_ASSERT(!cluster_shared_config);
	return CLUSTER_SPACE_HINT_NATIVE;
}
void
cluster_space_hint_finish(Buffer buffer pg_attribute_unused(), bool standard pg_attribute_unused(),
						  const RfPageProducerBatchV1 *batch pg_attribute_unused())
{
	UT_ASSERT(false); /* No qualified shared hint producer exists in this suite. */
}

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
	else if (origin_fault >= 7 && origin_fault <= 9) {
		if (locator->xid == 1206) {
			out->outcome = CLUSTER_TX_UNKNOWN;
			*reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
		} else if (origin_fault >= 8 && locator->xid == 1203) {
			out->outcome = CLUSTER_TX_COMMITTED;
			out->commit_scn = 9001;
		}
		if (origin_fault == 9 && locator->xid == 1207)
			pg_atomic_write_u64(&ut_itl_census_semantic.record_generation, 74);
	}
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

UT_TEST(test_unknown_slot_does_not_veto_independent_proven_capacity_progress)
{
	for (int fault = 7; fault <= 9; fault++) {
		UtR4HotProductFixture fixture;
		HeapHotSearchResult hot;
		PGAlignedBlock before;
		uint64 deadline = 0;
		const char *reason = NULL;

		ut_itl_census_begin(&fixture, &hot, false);
		origin_fault = fault;
		origin_calls = active_publications = 0;
		pg_atomic_write_u64(&ut_itl_census_semantic.active_bits,
							CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
		memcpy(before.data, fixture.live_page, BLCKSZ);
		UT_ASSERT_EQ(cluster_heap_test_itl_wait_capacity(1, 1, 1, 9900, &deadline, &reason),
					 fault == 7	  ? CLUSTER_TXW_RESOLVED
					 : fault == 8 ? CLUSTER_TXW_RETRY
								  : CLUSTER_TXW_UNPROVABLE);
		UT_ASSERT_EQ(ut_itl_wait_calls, fault == 7 ? 1 : 0);
		if (fault == 7)
			UT_ASSERT_EQ(ut_itl_wait_locator.xid, 1200);
		UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
		UT_ASSERT_EQ(memcmp(before.data, fixture.live_page, BLCKSZ), 0);
		UT_ASSERT(!ut_hot_content_lock_held);
		LockBuffer(1, BUFFER_LOCK_EXCLUSIVE);
		ut_itl_census_end();
	}
}

UT_TEST(test_partial_terminal_apply_leaves_unknown_and_active_slots_unchanged)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult hot;
	ClusterItlSlotData before[CLUSTER_ITL_INITRANS_DEFAULT];
	ClusterItlSlotData *slots;

	ut_itl_census_begin(&fixture, &hot, true);
	origin_fault = 8;
	memcpy(before, ClusterPageGetItlSlots((Page)fixture.live_page), sizeof(before));
	(void)cluster_heap_test_itl_capacity_outcome(UT_HOT_BUFFER, 9900, true);
	slots = ClusterPageGetItlSlots((Page)fixture.live_page);
	for (int i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		if (i == 3) {
			UT_ASSERT_EQ(slots[i].flags, ITL_FLAG_LOCK_ONLY_COMMITTED);
			UT_ASSERT_EQ(slots[i].commit_scn, 9001);
		} else
			UT_ASSERT_EQ(memcmp(&before[i], &slots[i], sizeof(before[i])), 0);
	}
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	ut_itl_census_end();
}

/* A completed foreign DATA slot still anchors materialized history. Repeating
 * the original capture/apply/wait sequence must not rediscover it as a fresh
 * terminal or make it allocatable. The other exact ACTIVE owner remains a
 * legal blocker despite slot 6 being UNKNOWN. */
UT_TEST(test_applied_protected_terminal_rechecks_wait_without_repeated_retry)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult hot;
	ClusterItlSlotData before[CLUSTER_ITL_INITRANS_DEFAULT];
	ClusterItlSlotData *slots;
	uint64 deadline = 0;
	uint64 saved_deadline = 0;
	const char *reason = NULL;

	ut_itl_census_begin(&fixture, &hot, false);
	origin_fault = 8;
	origin_calls = active_publications = 0;
	pg_atomic_write_u64(&ut_itl_census_semantic.active_bits,
						CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	slots = ClusterPageGetItlSlots((Page)fixture.live_page);
	slots[3].undo_segment_head = uba_encode(257, 4, 3, 0);
	memcpy(before, slots, sizeof(before));
	UT_ASSERT_EQ(cluster_heap_test_itl_wait_capacity(1, 1, 1, 9900, &deadline, &reason),
				 CLUSTER_TXW_RETRY);
	UT_ASSERT_EQ(deadline, 0);
	UT_ASSERT_EQ(ut_itl_wait_calls, 0);
	UT_ASSERT_EQ(memcmp(before, slots, sizeof(before)), 0);
	LockBuffer(1, BUFFER_LOCK_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_heap_test_itl_capacity_outcome(1, 9900, false),
				 CLUSTER_HEAP_ITL_CAPACITY_EXHAUSTED_OR_REFUSED);
	UT_ASSERT_EQ(slots[3].flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ(slots[3].commit_scn, 9001);
	UT_ASSERT_EQ(slots[3].wrap, before[3].wrap);
	UT_ASSERT_EQ(memcmp(&slots[3].undo_segment_head, &before[3].undo_segment_head, sizeof(UBA)), 0);
	for (int i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
		if (i != 3)
			UT_ASSERT_EQ(memcmp(&slots[i], &before[i], sizeof(before[i])), 0);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	memcpy(before, slots, sizeof(before));
	for (int round = 0; round < 3; round++) {
		/* Run the caller's allocation/terminal census again, then its
		 * capacity selector on the exact unchanged protected page. */
		UT_ASSERT_EQ(cluster_heap_test_itl_capacity_outcome(1, 9900, false),
					 CLUSTER_HEAP_ITL_CAPACITY_EXHAUSTED_OR_REFUSED);
		UT_ASSERT_EQ(cluster_heap_test_itl_wait_capacity(1, 1, 1, 9900, &deadline, &reason),
					 CLUSTER_TXW_RESOLVED);
		UT_ASSERT_STR_EQ(reason, "ITL_FRESH_REQUALIFY");
		UT_ASSERT_EQ(ut_itl_wait_locator.itl_slot_index, 0);
		UT_ASSERT_EQ(ut_itl_wait_locator.xid, 1200);
		UT_ASSERT_EQ(ut_itl_wait_locator.tt_wrap, 42);
		UT_ASSERT_EQ(memcmp(before, slots, sizeof(before)), 0);
		UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
		UT_ASSERT(!ut_hot_content_lock_held);
		UT_ASSERT(!ut_itl_recycle_guard_active);
		if (round == 0) {
			UT_ASSERT(deadline > 0);
			saved_deadline = deadline;
		} else
			UT_ASSERT_EQ(deadline, saved_deadline);
		LockBuffer(1, BUFFER_LOCK_EXCLUSIVE);
	}
	UT_ASSERT_EQ(ut_itl_wait_calls, 3);
	ut_itl_census_end();
}

UT_TEST(test_all_applied_protected_terminals_refuse_without_retry_or_wait)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult hot;
	PGAlignedBlock before;
	ClusterItlSlotData *slots;
	uint64 deadline = 0;
	const char *reason = NULL;

	ut_itl_census_begin(&fixture, &hot, false);
	origin_fault = 6;
	origin_calls = active_publications = 0;
	pg_atomic_write_u64(&ut_itl_census_semantic.active_bits,
						CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	slots = ClusterPageGetItlSlots((Page)fixture.live_page);
	for (int i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
		slots[i].undo_segment_head = uba_encode(257, i + 1, i, 0);
	UT_ASSERT_EQ(cluster_heap_test_itl_capacity_outcome(1, 9900, false),
				 CLUSTER_HEAP_ITL_CAPACITY_EXHAUSTED_OR_REFUSED);
	for (int i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		UT_ASSERT_EQ(slots[i].flags, ITL_FLAG_ABORTED);
		UT_ASSERT_EQ(slots[i].commit_scn, InvalidScn);
	}
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
	memcpy(before.data, fixture.live_page, BLCKSZ);
	for (int round = 0; round < 3; round++) {
		UT_ASSERT_EQ(cluster_heap_test_itl_capacity_outcome(1, 9900, false),
					 CLUSTER_HEAP_ITL_CAPACITY_EXHAUSTED_OR_REFUSED);
		UT_ASSERT_EQ(cluster_heap_test_itl_wait_capacity(1, 1, 1, 9900, &deadline, &reason),
					 CLUSTER_TXW_UNPROVABLE);
		UT_ASSERT_STR_EQ(reason, "ITL_PROTECTED_HISTORY_CAPACITY");
		UT_ASSERT_EQ(deadline, 0);
		UT_ASSERT_EQ(ut_itl_wait_calls, 0);
		UT_ASSERT_EQ(memcmp(before.data, fixture.live_page, BLCKSZ), 0);
		UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 1);
		UT_ASSERT(!ut_hot_content_lock_held);
		UT_ASSERT(!ut_itl_recycle_guard_active);
		LockBuffer(1, BUFFER_LOCK_EXCLUSIVE);
	}
	UT_ASSERT_EQ(origin_calls, 8);
	ut_itl_census_end();
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

UT_TEST(test_deadlock_retains_its_typed_cause_without_relabeling_as_authority)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult hot;
	PGAlignedBlock before;
	uint64 deadline = 0;
	const char *reason = NULL;

	ut_itl_census_begin(&fixture, &hot, true);
	origin_fault = 0;
	ut_itl_wait_result = CLUSTER_TXW_DEADLOCK;
	pg_atomic_write_u64(&ut_itl_census_semantic.active_bits,
						CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	memcpy(before.data, fixture.live_page, BLCKSZ);
	ut_itl_capacity_log[0] = '\0';
	ut_capture_miss_log = true;
	UT_ASSERT_EQ(cluster_heap_test_itl_wait_capacity(1, 1, 1, 9900, &deadline, &reason),
				 CLUSTER_TXW_DEADLOCK);
	ut_capture_miss_log = false;
	UT_ASSERT_STR_EQ(reason, "ITL_WAIT_DEADLOCK");
	UT_ASSERT(strstr(ut_itl_capacity_log, "waiter_xid=9900 blocker_xid=1200") != NULL);
	UT_ASSERT(strstr(ut_itl_capacity_log, "stage=WAIT_TERMINAL") != NULL);
	UT_ASSERT(strstr(ut_itl_capacity_log, "slot=7 xid=1207") != NULL);
	UT_ASSERT_EQ(ut_itl_wait_calls, 1);
	UT_ASSERT_EQ(ut_itl_census_dirty_hint_calls, 0);
	UT_ASSERT_EQ(memcmp(before.data, fixture.live_page, BLCKSZ), 0);
	UT_ASSERT(!ut_hot_content_lock_held);
	LockBuffer(1, BUFFER_LOCK_EXCLUSIVE);
	ut_itl_census_end();
}

UT_TEST(test_confirmed_deadlock_wins_over_simultaneous_capacity_deadline)
{
	UtR4HotProductFixture fixture;
	HeapHotSearchResult hot;
	uint64 deadline = 0;
	const char *reason = NULL;
	int saved_budget = cluster_ges_request_timeout_ms;

	ut_itl_census_begin(&fixture, &hot, true);
	origin_fault = 0;
	ut_itl_wait_result = CLUSTER_TXW_DEADLOCK;
	ut_itl_wait_past_budget = true;
	cluster_ges_request_timeout_ms = 1;
	pg_atomic_write_u64(&ut_itl_census_semantic.active_bits,
						CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(cluster_heap_test_itl_wait_capacity(1, 1, 1, 9900, &deadline, &reason),
				 CLUSTER_TXW_DEADLOCK);
	UT_ASSERT_STR_EQ(reason, "ITL_WAIT_DEADLOCK");
	UT_ASSERT_EQ(ut_itl_wait_calls, 1);
	cluster_ges_request_timeout_ms = saved_budget;
	LockBuffer(1, BUFFER_LOCK_EXCLUSIVE);
	ut_itl_census_end();
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_applied_protected_terminal_rechecks_wait_without_repeated_retry);
	UT_RUN(test_all_applied_protected_terminals_refuse_without_retry_or_wait);
	UT_RUN(test_partial_terminal_apply_leaves_unknown_and_active_slots_unchanged);
	UT_RUN(test_unknown_slot_does_not_veto_independent_proven_capacity_progress);
	UT_RUN(test_confirmed_deadlock_wins_over_simultaneous_capacity_deadline);
	UT_RUN(test_deadlock_retains_its_typed_cause_without_relabeling_as_authority);
	UT_RUN(test_exact_active_owner_reaches_capacity_wait_through_real_resolver);
	UT_RUN(test_capacity_keeps_unknown_identity_prepared_and_admission_refusals);
	UT_RUN(test_origin_abort_releases_eight_lock_only_slots_through_real_census);
	UT_RUN(test_lock_capacity_does_not_mistake_self_data_slot_for_lock_capacity);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
