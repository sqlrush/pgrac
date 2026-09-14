/* Real allocator + backend-local ACTIVE consumer + linked durable producer.
 * Only admission, locks, retention and physical IO/WAL dependencies are doubles.
 * Hooks choose exact interleavings; this is not a live scheduling replay.
 * Copyright (c) 2026, pgrac contributors.
 */
int durable_fixture_main(int argc, char **argv);
#define TT_DURABLE_REAL_ALLOCATOR_FIXTURE
#define main durable_fixture_main
#include "test_cluster_tt_durable.c"
#undef main
#include "../../backend/cluster/cluster_tt_slot.c"
#include "../../backend/cluster/cluster_tt_local.c"

static ClusterTTSlotShmem allocator_storage;
static int lock_depth;
static int modifier_enters;
static int modifier_leaves;
static int rollovers_on_enter;
static bool cold_fixture;
static bool cold_capacity_refused;
static uint32 cold_successor;
static int cold_selection_calls;
static TTSlot cold_old_slot;
bool cluster_enabled = true;
bool cluster_undo_retention_horizon_enabled = false;
int cluster_undo_segments_max_per_instance = 256;
ClusterConf *ClusterConfShmem = NULL;
MemoryContext CurrentMemoryContext = (MemoryContext)1;
MemoryContext TopTransactionContext = (MemoryContext)1;

void *(palloc0)(Size size)
{
	return calloc(1, size);
}

void *
MemoryContextAllocZero(MemoryContext context pg_attribute_unused(), Size size)
{
	return calloc(1, size);
}

void *
MemoryContextAllocZeroAligned(MemoryContext context, Size size)
{
	return MemoryContextAllocZero(context, size);
}

void *
repalloc(void *pointer, Size size)
{
	return realloc(pointer, size);
}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	lock_depth++;
	return false;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	UT_ASSERT(lock_depth > 0);
	lock_depth--;
}

int
s_lock(volatile slock_t *lock pg_attribute_unused(), const char *file pg_attribute_unused(),
	   int line pg_attribute_unused(), const char *func pg_attribute_unused())
{
	UT_ASSERT(false);
	return 0;
}

SCN
cluster_undo_retention_horizon(void)
{
	return InvalidScn;
}

bool
cluster_tt_slot_recyclable(uint8 status pg_attribute_unused(), SCN commit_scn pg_attribute_unused(),
						   SCN horizon pg_attribute_unused())
{
	return false; /* This fixture allocates only fresh FREE slots. */
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

ClusterJoinGateVerdict
cluster_reconfig_self_join_gate_verdict(void)
{
	return CLUSTER_JOIN_GATE_ALLOW;
}

static void
select_physical_fixture(uint32 segment)
{
	UndoSegmentHeaderData *disk = (UndoSegmentHeaderData *)g_canned_block;

	g_current_segment = g_canned_block_segment = segment;
	memset(g_canned_block, 0, BLCKSZ);
	disk->segment_id = segment;
	disk->owner_instance = 1;
	disk->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	disk->wrap_count = 4;
	if (cold_fixture && segment == 1)
		disk->tt_slots[0] = cold_old_slot;
	memcpy(g_current_resident, g_canned_block, BLCKSZ);
}

static void
roll_current_once(void)
{
	uint32 old = cluster_tt_slot_current_segment(0);
	bool had_active = false;

	g_epoch_hook = NULL;
	g_before_current_acquire_hook = NULL;
	g_after_bind_emit_hook = NULL;
	g_ctrc_overlap_hook = NULL;
	UT_ASSERT_EQ(lock_depth, 0);
	cluster_tt_slot_rollover(0, old + 1, &had_active);
	UT_ASSERT(had_active);
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_modifier_enter(bool writable, ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT(writable);
	modifier_enters++;
	if (rollovers_on_enter > 0) {
		rollovers_on_enter--;
		roll_current_once();
	}
	select_physical_fixture(cluster_tt_slot_current_segment(0));
	*token = target_modifier_token();
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT(token->entered);
	modifier_leaves++;
	token->entered = false;
}

uint32
cluster_undo_active_segment_for_node_or_create(int node pg_attribute_unused())
{
	UT_ASSERT(cold_fixture);
	return 1; /* Existing base file is not necessarily an empty TT. */
}

uint32
cluster_undo_tt_rollover_locked(int node, uint32 old, bool *hard_cap)
{
	UT_ASSERT(cold_fixture);
	UT_ASSERT_EQ(node, 0);
	UT_ASSERT_EQ(old, 0);
	UT_ASSERT_EQ(lock_depth, 0);
	cold_selection_calls++;
	*hard_cap = cold_capacity_refused;
	if (cold_capacity_refused)
		return 0;
	/* Only physical selection/current publication is doubled. The actual
	 * allocator, local consumer and durable ACTIVE producer run below. */
	cluster_tt_slot_rollover(node, cold_successor, NULL);
	return cold_successor;
}

bool
cluster_undo_cleaner_wait_for_capacity(uint32 segment pg_attribute_unused(),
									   ClusterCtrcTxnKeyV1 *key pg_attribute_unused())
{
	UT_ASSERT(cold_fixture && cold_capacity_refused);
	return false;
}

TimestampTz
cluster_undo_cleaner_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_undo_cleaner_main_loop_iters(void)
{
	return 0;
}
UndoCleanerStatus
cluster_undo_cleaner_status(void)
{
	return 0;
}
const char *
cluster_undo_cleaner_status_to_string(UndoCleanerStatus state pg_attribute_unused())
{
	return "unused";
}
SCN
cluster_scn_current(void)
{
	return 1;
}
uint64
cluster_undo_segment_reuse_count(void)
{
	return 0;
}
uint64
cluster_undo_segment_switch_count(void)
{
	return 0;
}
uint64
cluster_undo_tt_rollover_fail_hard_cap_count(void)
{
	return 0;
}
uint64
cluster_undo_tt_rollover_fail_extend_count(void)
{
	return 0;
}
uint64
cluster_undo_segment_hard_cap_fail_count(void)
{
	return 0;
}

static void
reset_active_owner(void)
{
	void *old_bindings = cluster_tt_local_bindings;

	cluster_tt_local_reset_binding();
	free(old_bindings);
	memset(&allocator_storage, 0, sizeof(allocator_storage));
	ClusterTTSlotShm = &allocator_storage;
	lock_depth = modifier_enters = modifier_leaves = rollovers_on_enter = 0;
	cold_fixture = cold_capacity_refused = false;
	cold_selection_calls = 0;
	cold_successor = 2;
	memset(&cold_old_slot, 0, sizeof(cold_old_slot));
	cluster_tt_slot_rollover(0, 1, NULL);
	reset_current_write_mock();
	select_physical_fixture(1);
}

static void
require_published_on(uint32 segment)
{
	ClusterCanonicalTxnBinding result;
	volatile bool caught = false;
	volatile bool published = false;

	memset(&result, 0, sizeof(result));
	PG_TRY();
	{
		published = cluster_tt_local_prepare_canonical_active(100, &result);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(!caught);
	UT_ASSERT(published);
	if (caught || !published)
		return;
	UT_ASSERT_EQ(result.segment_id, segment);
	UT_ASSERT_EQ(result.xid, 100);
	UT_ASSERT_EQ(result.segment_generation, 4);
	UT_ASSERT_EQ(result.publish_state, CLUSTER_CANONICAL_TXN_PUBLISHED);
	UT_ASSERT_EQ(g_bind_emit_calls, 1);
	UT_ASSERT_EQ(g_last_bind.segment_id, segment);
	UT_ASSERT_EQ(g_last_bind.xid, 100);
	UT_ASSERT_EQ(g_ctrc_open_calls, 1);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	UT_ASSERT_EQ(g_current_pin_calls, g_current_unpin_calls);
	UT_ASSERT_EQ(g_current_acquire_calls, g_current_cancel_calls);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT(cluster_tt_local_get_published_binding(100, &result));
}

UT_TEST(test_real_allocator_normal_local_publication)
{
	reset_active_owner();
	require_published_on(1);
}

UT_TEST(test_cold_allocator_preserves_occupied_durable_base)
{
	reset_active_owner();
	memset(&allocator_storage, 0, sizeof(allocator_storage));
	cold_fixture = true;
	cold_old_slot.xid = 4195185;
	cold_old_slot.status = TT_SLOT_COMMITTED;
	cold_old_slot.flags = 1;
	cold_old_slot.commit_scn = ((uint64)1 << 56) | 14274853;
	require_published_on(2);
	UT_ASSERT_EQ(cold_selection_calls, 1);
	UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 2);
}

UT_TEST(test_cold_fresh_database_uses_same_publication_path)
{
	reset_active_owner();
	memset(&allocator_storage, 0, sizeof(allocator_storage));
	cold_fixture = true;
	cold_successor = 1;
	require_published_on(1);
	UT_ASSERT_EQ(cold_selection_calls, 1);
}

UT_TEST(test_cold_full_pool_refuses_before_binding_or_bind_wal)
{
	ClusterCanonicalTxnBinding result;
	volatile bool caught = false;

	reset_active_owner();
	memset(&allocator_storage, 0, sizeof(allocator_storage));
	cold_fixture = cold_capacity_refused = true;
	PG_TRY();
	{
		(void)cluster_tt_local_prepare_canonical_active(100, &result);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cold_selection_calls, 1);
	UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
}

UT_TEST(test_real_allocator_rolls_after_reserve_before_observation)
{
	reset_active_owner();
	g_epoch_hook = roll_current_once;
	require_published_on(2);
}

UT_TEST(test_real_allocator_rolls_during_modifier_admission)
{
	reset_active_owner();
	rollovers_on_enter = 1;
	require_published_on(2);
}

UT_TEST(test_real_allocator_rolls_while_current_is_acquired)
{
	reset_active_owner();
	g_before_current_acquire_hook = roll_current_once;
	require_published_on(2);
}

UT_TEST(test_real_allocator_rolls_after_bind_keeps_original_identity)
{
	reset_active_owner();
	g_after_bind_emit_hook = roll_current_once;
	require_published_on(1);
	UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 2);
}

UT_TEST(test_real_allocator_rolls_during_released_ctrc_wait)
{
	reset_active_owner();
	g_ctrc_reserve_retry_countdown = 1;
	g_ctrc_overlap_hook = roll_current_once;
	require_published_on(2);
	UT_ASSERT_EQ(g_current_acquire_calls, 3);
	UT_ASSERT_EQ(g_current_cancel_calls, 3);
	UT_ASSERT_EQ(g_ctrc_reserve_calls, 2);
}

static void
corrupt_same_current_owner(void)
{
	g_before_current_acquire_hook = NULL;
	allocator_storage.per_node[0].slots[0].xid = 999;
}

UT_TEST(test_real_same_segment_identity_conflict_does_not_reallocate)
{
	ClusterCanonicalTxnBinding result;
	volatile bool caught = false;

	reset_active_owner();
	g_before_current_acquire_hook = corrupt_same_current_owner;
	PG_TRY();
	{
		(void)cluster_tt_local_prepare_canonical_active(100, &result);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(g_ctrc_reserve_calls, 0);
	UT_ASSERT_EQ(g_current_pin_calls, g_current_unpin_calls);
	UT_ASSERT_EQ(g_current_acquire_calls, g_current_cancel_calls);
	UT_ASSERT_EQ(cluster_tt_local_bindings[0].publish_state, CLUSTER_CANONICAL_TXN_FAILED);
	UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 1);
}

UT_TEST(test_reselection_preserves_other_local_published_binding)
{
	ClusterCanonicalTxnBinding other;
	ClusterCanonicalTxnBinding result;
	ClusterTTLocalBinding saved;
	volatile bool caught = false;

	reset_active_owner();
	UT_ASSERT(cluster_tt_local_prepare_canonical_active(101, &other));
	saved = cluster_tt_local_bindings[0];
	rollovers_on_enter = 3;
	PG_TRY();
	{
		UT_ASSERT(cluster_tt_local_prepare_canonical_active(100, &result));
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(!caught);
	if (caught)
		return;
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 2);
	UT_ASSERT_EQ(memcmp(&saved, &cluster_tt_local_bindings[0], sizeof(saved)), 0);
	UT_ASSERT_EQ(result.segment_id, 4);
	UT_ASSERT_EQ(result.xid, 100);
	UT_ASSERT_EQ(g_bind_emit_calls, 2);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	UT_ASSERT(cluster_tt_local_get_published_binding(101, &result));
	UT_ASSERT_EQ(memcmp(&other, &result, sizeof(other)), 0);
}

static void
roll_and_request_cancel(void)
{
	roll_current_once();
	InterruptPending = true;
	g_process_interrupt_error = true;
}

UT_TEST(test_cancel_after_unpublished_rollover_leaves_no_failed_binding)
{
	ClusterCanonicalTxnBinding result;
	volatile bool caught = false;

	reset_active_owner();
	g_before_current_acquire_hook = roll_and_request_cancel;
	PG_TRY();
	{
		(void)cluster_tt_local_prepare_canonical_active(100, &result);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(g_ctrc_reserve_calls, 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	UT_ASSERT_EQ(g_current_pin_calls, g_current_unpin_calls);
	UT_ASSERT_EQ(g_current_acquire_calls, g_current_cancel_calls);
}

UT_TEST(test_real_allocator_repeated_prebind_rollover_does_not_grow_bindings)
{
	reset_active_owner();
	rollovers_on_enter = 4;
	require_published_on(5);
}

UT_TEST(test_real_local_postbind_error_stays_failed_without_reallocation)
{
	ClusterCanonicalTxnBinding result;
	volatile bool caught = false;

	reset_active_owner();
	g_write_hdr_ok = false;
	PG_TRY();
	{
		(void)cluster_tt_local_prepare_canonical_active(100, &result);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(g_bind_emit_calls, 1);
	UT_ASSERT_EQ(g_ctrc_block_post_bind_calls, 1);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 1);
	UT_ASSERT_EQ(cluster_tt_local_bindings[0].publish_state, CLUSTER_CANONICAL_TXN_FAILED);
	UT_ASSERT_EQ(cluster_tt_local_bindings[0].segment_id, 1);
	UT_ASSERT_EQ(modifier_enters, 1);
	UT_ASSERT_EQ(modifier_leaves, 1);
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_cold_allocator_preserves_occupied_durable_base);
	UT_RUN(test_cold_fresh_database_uses_same_publication_path);
	UT_RUN(test_cold_full_pool_refuses_before_binding_or_bind_wal);
	UT_RUN(test_real_allocator_normal_local_publication);
	UT_RUN(test_real_allocator_rolls_after_reserve_before_observation);
	UT_RUN(test_real_allocator_rolls_during_modifier_admission);
	UT_RUN(test_real_allocator_rolls_while_current_is_acquired);
	UT_RUN(test_real_allocator_rolls_after_bind_keeps_original_identity);
	UT_RUN(test_real_allocator_rolls_during_released_ctrc_wait);
	UT_RUN(test_real_same_segment_identity_conflict_does_not_reallocate);
	UT_RUN(test_reselection_preserves_other_local_published_binding);
	UT_RUN(test_cancel_after_unpublished_rollover_leaves_no_failed_binding);
	UT_RUN(test_real_allocator_repeated_prebind_rollover_does_not_grow_bindings);
	UT_RUN(test_real_local_postbind_error_stays_failed_without_reallocation);
	UT_DONE();
	return ut_failed_count != 0;
}
