/* Author: SqlRush <sqlrush@gmail.com> */
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
#include "cluster/cluster_xid_stripe_boot.h"
#include "cluster/cluster_undo_cleaner.h"
#include "storage/lock.h"
#include "utils/wait_event.h"
#include "../../backend/cluster/cluster_tt_slot.c"
#include "../../backend/cluster/cluster_tt_local.c"

/* The parent has no discovery API. Its unavailable fallback cannot prove GC. */
#ifndef CLUSTER_TT_SLOT_CURRENT_ORIGIN_SNAPSHOT_V1
static bool
cluster_tt_slot_current_origin_snapshot(uint32 segment pg_attribute_unused(),
										uint16 offset pg_attribute_unused(),
										ClusterCtrcTxnKeyV1 *key, ClusterTTSlotCurrentOwner *owner,
										uint64 *binding)
{
	if (key != NULL)
		memset(key, 0, sizeof(*key));
	if (owner != NULL)
		memset(owner, 0, sizeof(*owner));
	if (binding != NULL)
		*binding = 0;
	return false;
}
#endif

/* Ordinary TT tests have no relmap producer; the composition supplies it. */
#ifndef TT_ACTIVE_OWNER_EXTERNAL_RELMAP_HOOK
bool
cluster_relmap_retention_runtime_after_active_v2(TransactionId xid pg_attribute_unused())
{
	return true;
}
#endif

static ClusterTTSlotShmem allocator_storage;
static UndoCleanerSharedState active_capacity_stats_storage;
static UndoCleanerSharedState *undo_cleaner_state;
static unsigned slot_stats_lock_acquires;
static int lock_depth;
static int modifier_enters;
static int modifier_leaves;
static bool modifier_refused;
static int rollovers_on_enter;
static bool first_recheck_pending;
static bool cold_fixture;
static bool cold_capacity_refused;
static uint32 cold_successor;
static int cold_selection_calls;
static TTSlot cold_old_slot;
static ClusterNormalStartState active_owner_start_state = CLUSTER_NORMAL_START_TARGET_READY;
static bool active_owner_recovered_open = true;
static bool fail_next_memory_allocation;
static bool capacity_test;
static bool capacity_cancel;
static bool capacity_other_admission;
static bool capacity_lease_available;
static bool capacity_lease_current;
static unsigned capacity_waits;
static unsigned capacity_rechecks;
static unsigned capacity_supply_after;
static unsigned capacity_admission_change;
static unsigned capacity_lease_captures;
static uint32 capacity_selected_segment;
static bool capacity_rollover_fill_once;
static bool capacity_passive_rollover;
static bool capacity_selection_unknown;
static bool capacity_prior_reservation;
static bool slot_wait_test, slot_wait_context_safe, slot_wait_in_native;
static unsigned slot_wait_lookups, slot_wait_calls, slot_wait_begins, slot_wait_ends;
static unsigned slot_wait_cancels;
static unsigned slot_wait_drift, slot_wait_owner_kind;
static int slot_wait_error;
static bool slot_wait_commit, slot_wait_terminal_capacity;
static uint32 slot_wait_event;
uint32 *my_wait_event_info = &slot_wait_event;
static void fill_selected_segment(ClusterTTSlotAllocStatus status);
bool cluster_shared_catalog;
bool cluster_xid_striping;
static VariableCacheData capacity_variables;
VariableCache ShmemVariableCache = &capacity_variables;
static LWLockPadded capacity_locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = capacity_locks;
bool cluster_enabled = true;
BackendType MyBackendType = B_BACKEND;
bool cluster_undo_retention_horizon_enabled = false;
int cluster_undo_segments_max_per_instance = 256;
ClusterConf *ClusterConfShmem = NULL;
MemoryContext CurrentMemoryContext = (MemoryContext)1;
MemoryContext TopTransactionContext = (MemoryContext)1;

ClusterNormalStartState
cluster_semantic_normal_start_state(void)
{
	return active_owner_start_state;
}

bool
cluster_semantic_activation_recovered_open_current(void)
{
	return active_owner_recovered_open;
}

static void
check_memory_allocation(void)
{
	if (fail_next_memory_allocation) {
		fail_next_memory_allocation = false;
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("injected allocation failure")));
	}
}

void *
MemoryContextAllocZero(MemoryContext context pg_attribute_unused(), Size size)
{
	check_memory_allocation();
	return calloc(1, size);
}

void *(palloc0)(Size size)
{
	return MemoryContextAllocZero(CurrentMemoryContext, size);
}

void *
MemoryContextAllocZeroAligned(MemoryContext context, Size size)
{
	return MemoryContextAllocZero(context, size);
}

void *
repalloc(void *pointer, Size size)
{
	check_memory_allocation();
	return realloc(pointer, size);
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode pg_attribute_unused())
{
	if (undo_cleaner_state != NULL && lock == &undo_cleaner_state->lwlock)
		slot_stats_lock_acquires++;
	lock_depth++;
	return false;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	UT_ASSERT(lock_depth > 0);
	lock_depth--;
}

/* Scheduling remains a boundary; initialization and both metric ends are real. */
static void *
active_capacity_stats_shmem_init(const char *name, Size size, bool *found)
{
	UT_ASSERT_STR_EQ(name, "pgrac cluster undo cleaner");
	UT_ASSERT_EQ(size, sizeof(active_capacity_stats_storage));
	*found = false;
	return &active_capacity_stats_storage;
}

static void
active_capacity_stats_lock_init(LWLock *lock, int tranche)
{
	UT_ASSERT(lock == &active_capacity_stats_storage.lwlock);
	UT_ASSERT_EQ(tranche, LWTRANCHE_CLUSTER_UNDO_CLEANER);
}

static void
active_capacity_stats_cv_init(ConditionVariable *cv)
{
	UT_ASSERT(cv == &active_capacity_stats_storage.capacity_cv);
}

#define ShmemInitStruct active_capacity_stats_shmem_init
#define LWLockInitialize active_capacity_stats_lock_init
#define ConditionVariableInit active_capacity_stats_cv_init
#include "test_cluster_active_capacity_stats.inc"
#undef ShmemInitStruct
#undef LWLockInitialize
#undef ConditionVariableInit

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
	cluster_undo_segment_make_header_bytes(segment, 1, g_canned_block);
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
	if (modifier_refused) {
		memset(token, 0, sizeof(*token));
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	}
	first_recheck_pending = true;
	*token = target_modifier_token();
	if (capacity_test && modifier_enters > 1) {
		if (capacity_admission_change == 1)
			token->feature_bit++;
		else if (capacity_admission_change == 2)
			token->record_generation++;
		else if (capacity_admission_change == 3)
			token->formation_epoch++;
		else if (capacity_admission_change == 4)
			token->side = CLUSTER_SEMANTIC_SOURCE_SIDE;
	}
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

/* The allocator now runs under admission. Keep the interleaving at the
 * first post-allocation check, before the actual durable publisher executes. */
static void
active_owner_first_recheck(void)
{
	/* Capacity reentry checks admission before it owns any slot. Keep this
	 * physical-publication interleaving at its original post-allocation seam. */
	if (!first_recheck_pending || cluster_tt_local_binding_count == 0)
		return;
	first_recheck_pending = false;
	if (rollovers_on_enter > 0) {
		rollovers_on_enter--;
		roll_current_once();
	}
	select_physical_fixture(cluster_tt_slot_current_segment(0));
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
cluster_undo_tt_rollover_locked(int node, uint32 old, bool *hard_cap, bool *no_side_effects)
{
	UT_ASSERT(cold_fixture);
	UT_ASSERT_EQ(node, 0);
	UT_ASSERT_EQ(old, capacity_selected_segment);
	UT_ASSERT_EQ(lock_depth, 0);
	cold_selection_calls++;
	*hard_cap = cold_capacity_refused;
	/* The original producer's exact output is tested separately. Unknown
	 * leaves the caller's pessimistic initial value untouched. */
	if (!capacity_selection_unknown)
		*no_side_effects
			= cold_capacity_refused || (capacity_rollover_fill_once && capacity_passive_rollover);
	if (cold_capacity_refused)
		return 0;
	/* Only physical selection/current publication is doubled. The actual
	 * allocator, local consumer and durable ACTIVE producer run below. */
	cluster_tt_slot_rollover(node, cold_successor, NULL);
	if (capacity_rollover_fill_once) {
		capacity_rollover_fill_once = false;
		capacity_selected_segment = cold_successor++;
		fill_selected_segment(CTS_COMMITTED);
		cold_capacity_refused = true;
	}
	return cluster_tt_slot_current_segment(node);
}

/* Native scheduling and the separately exercised safe-context predicate are
 * leaf seams. Allocation, owner snapshot, selection and publication are real. */
bool cluster_undo_cleaner_capacity_context_safe(void);
void cluster_undo_cleaner_tx_slot_wait_begin(void);
void cluster_undo_cleaner_tx_slot_wait_end(bool cancelled);

bool
cluster_undo_cleaner_capacity_context_safe(void)
{
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	return slot_wait_context_safe && !capacity_other_admission;
}

void
cluster_undo_cleaner_tx_slot_wait_begin(void)
{
	UT_ASSERT(!slot_wait_in_native);
	slot_wait_in_native = true;
	slot_wait_begins++;
}

void
cluster_undo_cleaner_tx_slot_wait_end(bool cancelled)
{
	UT_ASSERT(slot_wait_in_native);
	slot_wait_in_native = false;
	slot_wait_ends++;
	slot_wait_cancels += cancelled;
}

void
pfree(void *pointer)
{
	free(pointer);
}

int
geterrcode(void)
{
	return last_ereport_errcode;
}

VirtualTransactionId *
GetLockConflicts(const LOCKTAG *tag, LOCKMODE mode, int *count)
{
	VirtualTransactionId *owners = calloc(3, sizeof(*owners));

	UT_ASSERT(slot_wait_test);
	UT_ASSERT_EQ(tag->locktag_type, LOCKTAG_TRANSACTION);
	UT_ASSERT_EQ(tag->locktag_field1, 200);
	UT_ASSERT_EQ(mode, ShareLock);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	slot_wait_lookups++;
	*count = slot_wait_owner_kind == 1 ? 0 : slot_wait_owner_kind == 2 ? 2 : 1;
	owners[0].backendId = slot_wait_owner_kind == 3 ? InvalidBackendId : 9;
	owners[0].localTransactionId = 19;
	owners[1].backendId = 10;
	owners[1].localTransactionId = 20;
	if (slot_wait_owner_kind == 4)
		slot_wait_context_safe = false;
	if (slot_wait_drift != 0) {
		unsigned drift = slot_wait_drift;

		slot_wait_drift = 0;
		if (drift == 1) {
			cluster_tt_slot_rollover(0, 2, NULL);
			cluster_tt_slot_rollover(0, 1, NULL);
			/* Restore byte-identical slots: only the real binding generation
			 * distinguishes this allocator lifetime from the first snapshot. */
			for (unsigned i = 0; i < 48; i++) {
				bool retained, current_drift;
				uint16 wrap;

				UT_ASSERT_EQ(cluster_tt_slot_alloc_current_exact(0, 1, 200 + i, &retained,
																 &current_drift, &wrap),
							 i);
			}
		} else
			cluster_tt_slot_mark_aborted(1, 1, 201, allocator_storage.per_node[0].slots[1].wrap);
	}
	return owners;
}

bool
VirtualXactLock(VirtualTransactionId owner, bool wait)
{
	const ClusterTTSlotAllocEntry *entry = &allocator_storage.per_node[0].slots[0];

	UT_ASSERT(slot_wait_test && slot_wait_in_native);
	UT_ASSERT_EQ(owner.backendId, 9);
	UT_ASSERT_EQ(owner.localTransactionId, 19);
	UT_ASSERT(wait);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(g_ctrc_reserve_calls, 0);
	UT_ASSERT_EQ(g_current_acquire_calls, 0);
	slot_wait_calls++;
	pgstat_report_wait_start(PG_WAIT_LOCK | LOCKTAG_VIRTUALTRANSACTION);
	if (slot_wait_error != 0)
		ereport(ERROR, (errcode(slot_wait_error), errmsg("native transaction wait ended")));
	if (capacity_admission_change == 5)
		modifier_refused = true;
	if (capacity_admission_change == 6)
		capacity_lease_current = false;
	if (slot_wait_commit)
		cluster_tt_slot_mark_committed(1, 0, 200, entry->wrap, (SCN)91);
	else
		cluster_tt_slot_mark_aborted(1, 0, 200, entry->wrap);
	return true;
}

bool
cluster_undo_cleaner_wait_for_capacity(uint32 segment, ClusterCtrcTxnKeyV1 *key)
{
	if (slot_wait_test && slot_wait_terminal_capacity) {
		UT_ASSERT_EQ(segment, 1);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		UT_ASSERT_EQ(cold_selection_calls, 0);
		UT_ASSERT_EQ(g_bind_emit_calls, 0);
		UT_ASSERT_EQ(lock_depth, 0);
		UT_ASSERT_EQ(modifier_enters, modifier_leaves);
		capacity_waits++;
		/* Terminal/release/handoff are the existing cleaner boundary here;
		 * the real allocator must still observe and claim FREE. */
		cluster_tt_slot_free(1, 0);
		return true;
	}
	UT_ASSERT(cold_fixture && cold_capacity_refused);
	if (capacity_test) {
		capacity_waits++;
		UT_ASSERT_EQ(segment, capacity_selected_segment);
		UT_ASSERT(key != NULL);
		UT_ASSERT_EQ(lock_depth, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		UT_ASSERT_EQ(g_bind_emit_calls, 0);
		UT_ASSERT_EQ(g_ctrc_reserve_calls, 0);
		UT_ASSERT_EQ(g_current_acquire_calls, capacity_prior_reservation ? 1 : 0);
		/* The real safe-context predicate refuses any held admission. */
		if (modifier_enters != modifier_leaves || capacity_other_admission)
			return false;
		if (capacity_cancel)
			ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("capacity wait cancelled")));
		if (capacity_admission_change == 5)
			modifier_refused = true;
		if (capacity_admission_change == 6)
			capacity_lease_current = false;
		if (capacity_waits >= capacity_supply_after)
			cold_capacity_refused = false;
		return true; /* Existing timed recheck; does not itself grant capacity. */
	}
	return false;
}

bool
cluster_xid_stripe_sample_future(FullTransactionId reference, FullTransactionId *future,
								 ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(capacity_test && cluster_shared_catalog && cluster_xid_striping);
	UT_ASSERT_EQ(lock_depth, 1);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves + 1);
	UT_ASSERT(FullTransactionIdEquals(reference, ShmemVariableCache->nextXid));
	capacity_lease_captures++;
	memset(fence, 0, sizeof(*fence));
	fence->activation_generation = 7;
	*future = reference;
	return capacity_lease_available;
}

bool
cluster_xid_stripe_sample_fence_current(const ClusterXidStripeSampleFence *fence)
{
	UT_ASSERT(capacity_test && fence->activation_generation == 7);
	UT_ASSERT_EQ(lock_depth, 0);
	capacity_rechecks++;
	return capacity_lease_current;
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

uint64
cluster_undo_tt_rollover_fail_activate_count(void)
{
	return 0;
}

static void
reset_active_owner(void)
{
	void *old_bindings = cluster_tt_local_bindings;

	cluster_undo_cleaner_shmem_init();
	slot_stats_lock_acquires = 0;
	cluster_tt_local_reset_binding();
	free(old_bindings);
	memset(&allocator_storage, 0, sizeof(allocator_storage));
	ClusterTTSlotShm = &allocator_storage;
	lock_depth = modifier_enters = modifier_leaves = rollovers_on_enter = 0;
	slot_wait_test = slot_wait_in_native = slot_wait_commit = slot_wait_terminal_capacity = false;
	slot_wait_context_safe = true;
	slot_wait_lookups = slot_wait_calls = slot_wait_begins = slot_wait_ends = 0;
	slot_wait_cancels = 0;
	slot_wait_drift = slot_wait_owner_kind = 0;
	slot_wait_error = 0;
	slot_wait_event = WAIT_EVENT_CLIENT_READ;
	modifier_refused = false;
	cluster_shared_config = false;
	MyBackendType = B_BACKEND;
	active_owner_start_state = CLUSTER_NORMAL_START_TARGET_READY;
	active_owner_recovered_open = true;
	first_recheck_pending = false;
	cold_fixture = cold_capacity_refused = false;
	cold_selection_calls = 0;
	fail_next_memory_allocation = false;
	capacity_test = capacity_cancel = capacity_other_admission = false;
	capacity_lease_available = capacity_lease_current = true;
	capacity_waits = capacity_rechecks = capacity_lease_captures = 0;
	capacity_supply_after = 1;
	capacity_admission_change = 0;
	capacity_selected_segment = 0;
	capacity_rollover_fill_once = capacity_passive_rollover = capacity_selection_unknown
		= capacity_prior_reservation = false;
	cluster_shared_catalog = cluster_xid_striping = false;
	ShmemVariableCache->nextXid = FullTransactionIdFromU64(300);
	cold_successor = 2;
	memset(&cold_old_slot, 0, sizeof(cold_old_slot));
	cluster_tt_slot_rollover(0, 1, NULL);
	reset_current_write_mock();
	g_modifier_recheck_hook = active_owner_first_recheck;
	select_physical_fixture(1);
}

UT_TEST(test_recovered_open_gate_precedes_canonical_reservation)
{
	ClusterCanonicalTxnBinding result;

	reset_active_owner();
	cluster_shared_config = true;
	active_owner_start_state = CLUSTER_NORMAL_START_EXISTING_OTHER;
	active_owner_recovered_open = false;
	memset(&result, 0, sizeof(result));
	UT_ASSERT(!cluster_tt_local_prepare_canonical_active(100, &result));
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_EQ(modifier_enters, 0);

	cluster_shared_config = false;
	active_owner_start_state = CLUSTER_NORMAL_START_TARGET_READY;
	active_owner_recovered_open = true;
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

UT_TEST(test_fresh_open_reserves_active_without_recovered_proof)
{
	reset_active_owner();
	cluster_shared_config = true;
	active_owner_start_state = CLUSTER_NORMAL_START_TARGET_READY;
	active_owner_recovered_open = false;
	require_published_on(1);
	reset_active_owner();
}

UT_TEST(test_binding_memory_error_precedes_allocator_and_cold_selection)
{
	for (int cold = 0; cold < 2; cold++) {
		ClusterCanonicalTxnBinding result;
		ClusterTTSlotShmem before;
		volatile bool caught = false;

		reset_active_owner();
		if (cold) {
			memset(&allocator_storage, 0, sizeof(allocator_storage));
			cold_fixture = true;
		}
		before = allocator_storage;
		fail_next_memory_allocation = true;
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
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_OUT_OF_MEMORY);
		UT_ASSERT(!fail_next_memory_allocation);
		UT_ASSERT_EQ(memcmp(&allocator_storage, &before, sizeof(before)), 0);
		UT_ASSERT_NULL(cluster_tt_local_bindings);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		UT_ASSERT_EQ(cluster_tt_local_binding_capacity, 0);
		UT_ASSERT(!cluster_tt_local_has_binding(100));
		UT_ASSERT(!cluster_tt_local_preabort_durable_finish(100));
		UT_ASSERT_EQ(cold_selection_calls, 0);
		UT_ASSERT_EQ(g_bind_emit_calls, 0);
		UT_ASSERT_EQ(g_ctrc_reserve_calls, 0);
		UT_ASSERT_EQ(g_current_acquire_calls, 0);
		UT_ASSERT_EQ(modifier_enters, 1);
		UT_ASSERT_EQ(modifier_leaves, 1);
		UT_ASSERT_EQ(lock_depth, 0);
	}
}

UT_TEST(test_binding_growth_error_preserves_existing_reservations)
{
	ClusterCanonicalTxnBinding result;
	ClusterTTLocalBinding saved[8];
	ClusterTTLocalBinding *before_bindings;
	ClusterTTSlotShmem before;
	volatile bool caught = false;

	reset_active_owner();
	/* Seed the existing table through the original private reservation path;
	 * no durable publication or new transaction authority is simulated. */
	for (int i = 0; i < 8; i++) {
		uint32 segment;
		uint16 slot;
		uint32 slot_id;
		bool preflight_safe = true;

		UT_ASSERT(cluster_tt_local_reserve_binding(101 + i, &segment, &slot, &slot_id,
												   &preflight_safe, false));
	}
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 8);
	UT_ASSERT_EQ(cluster_tt_local_binding_capacity, 8);
	before_bindings = cluster_tt_local_bindings;
	memcpy(saved, before_bindings, sizeof(saved));
	before = allocator_storage;
	fail_next_memory_allocation = true;
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
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_OUT_OF_MEMORY);
	UT_ASSERT(!fail_next_memory_allocation);
	UT_ASSERT_EQ(memcmp(&allocator_storage, &before, sizeof(before)), 0);
	UT_ASSERT(cluster_tt_local_bindings == before_bindings);
	UT_ASSERT_EQ(memcmp(cluster_tt_local_bindings, saved, sizeof(saved)), 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 8);
	UT_ASSERT_EQ(cluster_tt_local_binding_capacity, 8);
	UT_ASSERT(!cluster_tt_local_has_binding(100));
	UT_ASSERT(!cluster_tt_local_preabort_durable_finish(100));
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(g_ctrc_reserve_calls, 0);
	UT_ASSERT_EQ(g_current_acquire_calls, 0);
	UT_ASSERT_EQ(modifier_enters, 1);
	UT_ASSERT_EQ(modifier_leaves, 1);
	UT_ASSERT_EQ(lock_depth, 0);
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

static void
start_capacity_test(void)
{
	reset_active_owner();
	memset(&allocator_storage, 0, sizeof(allocator_storage));
	cold_fixture = cold_capacity_refused = capacity_test = true;
}

static void
require_capacity_error(int expected_code)
{
	ClusterCanonicalTxnBinding result;
	ClusterCanonicalTxnBinding zero = { 0 };
	volatile bool caught = false;

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
	UT_ASSERT_EQ(last_ereport_errcode, expected_code);
	UT_ASSERT_EQ(memcmp(&result, &zero, sizeof(result)), 0);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(g_ctrc_reserve_calls, 0);
	UT_ASSERT_EQ(g_current_acquire_calls, 0);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves + (modifier_refused ? 1 : 0));
	UT_ASSERT_EQ(lock_depth, 0);
}

UT_TEST(test_capacity_wait_releases_only_own_modifier_before_retry)
{
	start_capacity_test();
	require_published_on(2);
	UT_ASSERT_EQ(capacity_waits, 1);
	UT_ASSERT_EQ(cold_selection_calls, 2);
	UT_ASSERT_EQ(modifier_enters, 2);
	UT_ASSERT_EQ(capacity_lease_captures, 0);
}

UT_TEST(test_capacity_timed_rechecks_do_not_fail_or_reserve_early)
{
	start_capacity_test();
	capacity_supply_after = 4;
	require_published_on(2);
	UT_ASSERT_EQ(capacity_waits, 4);
	UT_ASSERT_EQ(cold_selection_calls, 5);
	UT_ASSERT_EQ(modifier_enters, 5);
}

UT_TEST(test_capacity_wait_preserves_other_admission_refusal)
{
	start_capacity_test();
	capacity_other_admission = true;
	require_capacity_error(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT(capacity_other_admission);
	UT_ASSERT_EQ(capacity_waits, 1);
	UT_ASSERT_EQ(modifier_enters, 1);
}

UT_TEST(test_capacity_cancel_has_no_publication_or_double_leave)
{
	start_capacity_test();
	capacity_cancel = true;
	require_capacity_error(ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(capacity_waits, 1);
	UT_ASSERT_EQ(modifier_enters, 1);
}

UT_TEST(test_capacity_reentry_rejects_changed_admission_before_allocation)
{
	for (unsigned change = 1; change <= 5; change++) {
		start_capacity_test();
		capacity_admission_change = change;
		require_capacity_error(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS);
		UT_ASSERT_EQ(capacity_waits, 1);
		UT_ASSERT_EQ(cold_selection_calls, 1);
		UT_ASSERT_EQ(modifier_enters, 2);
	}
}

UT_TEST(test_capacity_wait_captures_and_rechecks_original_lease)
{
	start_capacity_test();
	cluster_shared_catalog = cluster_xid_striping = true;
	require_published_on(2);
	UT_ASSERT_EQ(capacity_waits, 1);
	UT_ASSERT_EQ(capacity_lease_captures, 1);
	UT_ASSERT(capacity_rechecks > 0);
}

UT_TEST(test_capacity_unknown_lease_refuses_before_sleep)
{
	start_capacity_test();
	cluster_shared_catalog = cluster_xid_striping = true;
	capacity_lease_available = false;
	require_capacity_error(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	UT_ASSERT_EQ(capacity_waits, 0);
	UT_ASSERT_EQ(capacity_lease_captures, 1);
}

UT_TEST(test_capacity_changed_lease_refuses_before_allocation)
{
	start_capacity_test();
	cluster_shared_catalog = cluster_xid_striping = true;
	capacity_admission_change = 6;
	require_capacity_error(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS);
	UT_ASSERT_EQ(capacity_waits, 1);
	UT_ASSERT_EQ(cold_selection_calls, 1);
	UT_ASSERT_EQ(modifier_enters, 2);
}

static void
fill_selected_segment(ClusterTTSlotAllocStatus status)
{
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		ClusterTTSlotAllocEntry *entry = &allocator_storage.per_node[0].slots[i];

		entry->status = status;
		entry->xid = 200 + i;
		entry->commit_scn = status == CTS_COMMITTED ? (SCN)91 : InvalidScn;
	}
}

UT_TEST(test_retained_selected_segment_rechecks_allocator_after_capacity_wait)
{
	reset_active_owner();
	cold_fixture = cold_capacity_refused = capacity_test = true;
	capacity_selected_segment = 1;
	cluster_undo_retention_horizon_enabled = true;
	fill_selected_segment(CTS_COMMITTED);
	require_published_on(2);
	UT_ASSERT_EQ(capacity_waits, 1);
	UT_ASSERT_EQ(cold_selection_calls, 2);
	UT_ASSERT_EQ(modifier_enters, 2);
	cluster_undo_retention_horizon_enabled = false;
}

/* A later hard-cap selection cannot erase an earlier rollover's effects in
 * this same reserve call. The actual allocator consumes the intervening
 * CURRENT change; only the physical rollover producer is a boundary input. */
UT_TEST(test_capacity_after_successful_rollover_keeps_error_boundary)
{
	for (int cold = 0; cold < 2; cold++) {
		start_capacity_test();
		if (!cold) {
			cluster_tt_slot_rollover(0, 1, NULL);
			capacity_selected_segment = 1;
			fill_selected_segment(CTS_COMMITTED);
		}
		cluster_undo_retention_horizon_enabled = true;
		cold_capacity_refused = false;
		capacity_rollover_fill_once = true;
		require_capacity_error(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
		UT_ASSERT_EQ(capacity_waits, 0);
		UT_ASSERT_EQ(cold_selection_calls, 2);
		UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 2);
		UT_ASSERT_EQ(modifier_enters, 1);
		cluster_undo_retention_horizon_enabled = false;
	}
}

UT_TEST(test_capacity_unknown_rollover_effects_do_not_enter_wait)
{
	start_capacity_test();
	capacity_selection_unknown = capacity_other_admission = true;
	require_capacity_error(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT_EQ(capacity_waits, 0);
	UT_ASSERT_EQ(cold_selection_calls, 1);
}

UT_TEST(test_capacity_passive_winner_adoption_preserves_safe_wait)
{
	start_capacity_test();
	cluster_undo_retention_horizon_enabled = true;
	cold_capacity_refused = false;
	capacity_rollover_fill_once = capacity_passive_rollover = true;
	require_published_on(3);
	UT_ASSERT_EQ(capacity_waits, 1);
	UT_ASSERT_EQ(cold_selection_calls, 3);
	UT_ASSERT_EQ(modifier_enters, 2);
	cluster_undo_retention_horizon_enabled = false;
}

static void
roll_away_to_retained_capacity(void)
{
	roll_current_once();
	capacity_selected_segment = cluster_tt_slot_current_segment(0);
	fill_selected_segment(CTS_COMMITTED);
}

/* An unpublished durable-producer retry has already owned a local slot.
 * Forgetting that reservation must not reset the whole prepare's preflight. */
UT_TEST(test_capacity_after_unpublished_reservation_does_not_reenter_preflight)
{
	ClusterCanonicalTxnBinding result;
	ClusterCanonicalTxnBinding zero = { 0 };
	volatile bool caught = false;

	reset_active_owner();
	cold_fixture = cold_capacity_refused = capacity_test = true;
	capacity_prior_reservation = capacity_other_admission = true;
	cluster_undo_retention_horizon_enabled = true;
	g_before_current_acquire_hook = roll_away_to_retained_capacity;
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
	UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT_EQ(memcmp(&result, &zero, sizeof(result)), 0);
	UT_ASSERT_EQ(capacity_waits, 0);
	UT_ASSERT_EQ(cold_selection_calls, 1);
	UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 2);
	UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(g_ctrc_reserve_calls, 0);
	UT_ASSERT_EQ(g_current_acquire_calls, 1);
	UT_ASSERT_EQ(g_current_pin_calls, g_current_unpin_calls);
	UT_ASSERT_EQ(g_current_acquire_calls, g_current_cancel_calls);
	UT_ASSERT_EQ(modifier_enters, 2);
	UT_ASSERT_EQ(modifier_enters, modifier_leaves);
	UT_ASSERT_EQ(lock_depth, 0);
	cluster_undo_retention_horizon_enabled = false;
}

UT_TEST(test_modifier_refusal_precedes_allocation_and_leaves_abort_without_effects)
{
	for (int cold = 0; cold < 2; cold++) {
		ClusterCanonicalTxnBinding result;
		ClusterTTSlotShmem before;
		volatile bool caught = false;
		volatile bool abort_failed = false;

		reset_active_owner();
		if (cold) {
			memset(&allocator_storage, 0, sizeof(allocator_storage));
			cold_fixture = true;
		}
		before = allocator_storage;
		modifier_refused = true;
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
		UT_ASSERT_EQ(last_ereport_errcode, ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS);
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		UT_ASSERT_EQ(memcmp(&allocator_storage, &before, sizeof(before)), 0);
		UT_ASSERT_EQ(cold_selection_calls, 0);
		UT_ASSERT_EQ(g_bind_emit_calls, 0);
		UT_ASSERT_EQ(g_ctrc_reserve_calls, 0);
		UT_ASSERT_EQ(g_current_acquire_calls, 0);
		UT_ASSERT_EQ(modifier_enters, 1);
		UT_ASSERT_EQ(modifier_leaves, 0);
		PG_TRY();
		{
			UT_ASSERT(!cluster_tt_local_preabort_durable_finish(100));
		}
		PG_CATCH();
		{
			abort_failed = true;
		}
		PG_END_TRY();
		UT_ASSERT(!abort_failed);
	}
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

/* Exercise the real finish consumer with an already-published binding. The
 * terminal receipt is supplied by this fixture; durable commit/abort admission
 * is covered separately. Never refresh the binding's identity from the slot. */
static void
check_terminal_binding_wrap(bool committed)
{
	for (unsigned drift = 0; drift < 6; drift++) {
		ClusterTTLocalBinding *binding;
		ClusterTTSlotAllocEntry *entry;
		int wakeups;

		reset_active_owner();
		require_published_on(1);
		binding = cluster_tt_local_bindings;
		entry = &allocator_storage.per_node[0].slots[binding->slot_offset];
		binding->terminal_state = committed ? CLUSTER_TT_LOCAL_TERMINAL_COMMIT_STAGED
											: CLUSTER_TT_LOCAL_TERMINAL_ABORT_DURABLE;
		if (drift == 1)
			entry->wrap++;
		else if (drift == 2)
			binding->wrap = TT_WRAP_INVALID;
		else if (drift == 3)
			entry->xid++;
		else if (drift == 4)
			allocator_storage.per_node[0].segment_id = 2;
		else if (drift == 5) {
			binding->wrap = TT_WRAP_INVALID;
			entry->wrap = TT_WRAP_INVALID;
		}
		wakeups = g_undo_cleaner_wakeup_calls;
		cluster_tt_local_finish_bindings(committed, committed ? (SCN)91 : InvalidScn);
		UT_ASSERT_EQ(entry->status,
					 drift == 0 ? (committed ? CTS_COMMITTED : CTS_ABORTED) : CTS_ACTIVE);
		UT_ASSERT_EQ(entry->commit_scn, committed && drift == 0 ? (SCN)91 : InvalidScn);
		UT_ASSERT_EQ(g_undo_cleaner_wakeup_calls, wakeups + (drift == 0));
		UT_ASSERT_EQ(entry->xid, drift == 3 ? 101 : 100);
		UT_ASSERT_EQ(entry->wrap, drift == 5 ? TT_WRAP_INVALID : (drift == 1 ? 1 : 0));
		UT_ASSERT_EQ(cluster_tt_local_binding_count, 0);
		UT_ASSERT(cluster_tt_local_bindings == NULL);
		UT_ASSERT_EQ(lock_depth, 0);
		free(binding);
	}
}

UT_TEST(test_real_local_commit_keeps_bound_wrap)
{
	check_terminal_binding_wrap(true);
}

UT_TEST(test_real_local_abort_keeps_bound_wrap)
{
	check_terminal_binding_wrap(false);
}

UT_TEST(test_original_identity_survives_origin_retirement_and_terminal)
{
	for (int committed = 0; committed < 2; committed++) {
		ClusterCtrcTxnKeyV1 key;
		FullTransactionId full = InvalidFullTransactionId;

		reset_active_owner();
		require_published_on(1);
		key = g_ctrc_original_reservation.key;
		UT_ASSERT(cluster_tt_slot_origin_identity_read(&key, &full));
		UT_ASSERT(FullTransactionIdEquals(full, g_ctrc_reserved_original_full));
		/* Original source retirement must not erase the slot-owned handoff. */
		g_ctrc_original_available = false;
		if (committed)
			cluster_tt_slot_mark_committed(key.segment_id, key.slot_offset, key.xid,
									   key.slot_wrap, (SCN)91);
		else
			cluster_tt_slot_mark_aborted(key.segment_id, key.slot_offset, key.xid, key.slot_wrap);
		UT_ASSERT(cluster_tt_slot_origin_identity_read(&key, &full));
		UT_ASSERT(FullTransactionIdEquals(full, g_ctrc_reserved_original_full));
		UT_ASSERT_EQ(lock_depth, 0);
	}
}

UT_TEST(test_original_identity_exact_key_and_slot_lifetime)
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcTxnKeyV1 changed;
	FullTransactionId full;

	reset_active_owner();
	require_published_on(1);
	key = g_ctrc_original_reservation.key;
	UT_ASSERT(cluster_tt_slot_origin_identity_read(&key, &full));
	UT_ASSERT(cluster_tt_slot_origin_identity_retain(&g_ctrc_original_reservation));
	changed = key;
	changed.root_generation++;
	full = g_ctrc_reserved_original_full;
	UT_ASSERT(!cluster_tt_slot_origin_identity_read(&changed, &full));
	UT_ASSERT(!FullTransactionIdIsValid(full));
	changed = key;
	changed.slot_wrap++;
	UT_ASSERT(!cluster_tt_slot_origin_identity_read(&changed, &full));
	cluster_tt_slot_free(key.segment_id, key.slot_offset);
	UT_ASSERT(!cluster_tt_slot_origin_identity_read(&key, &full));
	UT_ASSERT_EQ(cluster_tt_slot_alloc(key.segment_id, key.xid), key.slot_offset);
	/* Same raw identity after FREE is not the former published reservation. */
	UT_ASSERT(!cluster_tt_slot_origin_identity_read(&key, &full));
	cluster_tt_slot_rollover(0, 2, NULL);
	cluster_tt_slot_rollover(0, 1, NULL);
	UT_ASSERT(!cluster_tt_slot_origin_identity_read(&key, &full));
	UT_ASSERT_EQ(lock_depth, 0);
}

UT_TEST(test_missing_origin_identity_never_changes_active_success)
{
	FullTransactionId full;

	reset_active_owner();
	g_ctrc_original_available = false;
	require_published_on(1);
	full = g_ctrc_reserved_original_full;
	UT_ASSERT(!cluster_tt_slot_origin_identity_read(&g_ctrc_original_reservation.key, &full));
	UT_ASSERT(!FullTransactionIdIsValid(full));
	g_ctrc_original_available = true;
	g_ctrc_original_reservation.kind = CTRC_ORIGIN_RESERVATION_EXISTING_OPEN;
	UT_ASSERT(!cluster_tt_slot_origin_identity_retain(&g_ctrc_original_reservation));
	UT_ASSERT(!cluster_tt_slot_origin_identity_read(&g_ctrc_original_reservation.key, &full));
	UT_ASSERT_EQ(lock_depth, 0);
}
/* This catches the original immediate 54000 branch, using 48 allocations
 * rather than supplying a precomputed NEED_ACTIVE verdict to the caller. */
static void
start_full_active_test(void)
{
	reset_active_owner();
	slot_wait_test = capacity_test = true;
	for (unsigned i = 0; i < 48; i++) {
		bool retained = true, drift = true;
		uint16 wrap;

		UT_ASSERT_EQ(cluster_tt_slot_alloc_current_exact(0, 1, 200 + i, &retained, &drift, &wrap),
					 i);
		UT_ASSERT(!retained && !drift);
	}
}

UT_TEST(test_49th_active_transaction_waits_in_selected_segment_without_rollover)
{
	for (unsigned commit = 0; commit < 2; commit++) {
		start_full_active_test();
		slot_wait_commit = commit != 0;
		require_published_on(1);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		UT_ASSERT_EQ(slot_wait_begins, 1);
		UT_ASSERT_EQ(slot_wait_ends, 1);
		UT_ASSERT(!slot_wait_in_native);
		UT_ASSERT_EQ(slot_wait_event, WAIT_EVENT_CLIENT_READ);
		UT_ASSERT_EQ(cold_selection_calls, 0);
		UT_ASSERT_EQ(capacity_waits, 0);
		for (unsigned i = 1; i < 48; i++) {
			UT_ASSERT_EQ(allocator_storage.per_node[0].slots[i].status, CTS_ACTIVE);
			UT_ASSERT_EQ(allocator_storage.per_node[0].slots[i].xid, 200 + i);
		}
	}
}

UT_TEST(test_active_native_errors_propagate_without_publishing_or_double_leave)
{
	static const int errors[]
		= { ERRCODE_QUERY_CANCELED, ERRCODE_LOCK_NOT_AVAILABLE, ERRCODE_T_R_DEADLOCK_DETECTED,
			ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS };

	for (unsigned i = 0; i < lengthof(errors); i++) {
		start_full_active_test();
		slot_wait_error = errors[i];
		require_capacity_error(errors[i]);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		UT_ASSERT_EQ(slot_wait_begins, 1);
		UT_ASSERT_EQ(slot_wait_ends, 1);
		UT_ASSERT(!slot_wait_in_native);
		UT_ASSERT_EQ(slot_wait_cancels, errors[i] == ERRCODE_QUERY_CANCELED);
		UT_ASSERT_EQ(slot_wait_event, WAIT_EVENT_CLIENT_READ);
		UT_ASSERT_EQ(cold_selection_calls, 0);
	}
}

UT_TEST(test_active_wait_rechecks_exact_admission_and_lease_before_reservation)
{
	for (unsigned change = 1; change <= 6; change++) {
		start_full_active_test();
		capacity_admission_change = change;
		cluster_shared_catalog = cluster_xid_striping = true;
		require_capacity_error(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		UT_ASSERT_EQ(capacity_lease_captures, 1);
		UT_ASSERT_EQ(cold_selection_calls, 0);
	}
}

UT_TEST(test_active_wait_refuses_unknown_prepared_or_changed_unsafe_context)
{
	for (unsigned refusal = 0; refusal < 5; refusal++) {
		start_full_active_test();
		slot_wait_context_safe = refusal != 0;
		slot_wait_owner_kind = refusal;
		require_capacity_error(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
		UT_ASSERT_EQ(slot_wait_calls, 0);
		UT_ASSERT_EQ(slot_wait_begins, 0);
		UT_ASSERT_EQ(cold_selection_calls, 0);
	}
}

static void
assert_active_refusal_stats(uint64 context, uint64 proof)
{
	ClusterUndoCapacityStats stats;

	/* An unsafe-context refusal must not acquire a diagnostic LWLock. */
	UT_ASSERT_EQ(slot_stats_lock_acquires, 0);
	UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.origin_node_id, cluster_node_id);
	UT_ASSERT_EQ(stats.refused_context_count, context);
	UT_ASSERT_EQ(stats.refused_proof_count, proof);
	UT_ASSERT_EQ(stats.active_full_wait_count, 0);
	UT_ASSERT_EQ(stats.pool_wait_count, 0);
	UT_ASSERT_EQ(stats.wakeup_recheck_count, 0);
	UT_ASSERT_EQ(stats.cancel_count, 0);
}

UT_TEST(test_active_refusal_stats_count_initial_unsafe_context)
{
	start_full_active_test();
	slot_wait_context_safe = false;
	require_capacity_error(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT_EQ(slot_wait_lookups + slot_wait_calls + cold_selection_calls, 0);
	assert_active_refusal_stats(1, 0);
}

UT_TEST(test_active_refusal_stats_count_only_unproven_exact_owners)
{
	for (unsigned kind = 1; kind <= 3; kind++) {
		start_full_active_test();
		slot_wait_owner_kind = kind;
		require_capacity_error(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
		UT_ASSERT_EQ(slot_wait_lookups, 1);
		UT_ASSERT_EQ(slot_wait_calls + cold_selection_calls, 0);
		assert_active_refusal_stats(0, 1);
	}
}

UT_TEST(test_active_refusal_stats_count_final_context_without_proof_refusal)
{
	start_full_active_test();
	slot_wait_owner_kind = 4;
	require_capacity_error(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
	UT_ASSERT_EQ(slot_wait_lookups, 1);
	UT_ASSERT_EQ(slot_wait_calls + cold_selection_calls, 0);
	assert_active_refusal_stats(1, 0);
}

UT_TEST(test_active_refusal_stats_do_not_count_success_or_snapshot_recheck)
{
	for (unsigned drift = 0; drift < 3; drift++) {
		start_full_active_test();
		slot_wait_drift = drift;
		require_published_on(1);
		UT_ASSERT_EQ(cold_selection_calls, 0);
		assert_active_refusal_stats(0, 0);
	}
}

UT_TEST(test_active_snapshot_change_reenters_allocator_without_native_wait)
{
	for (unsigned drift = 1; drift < 3; drift++) {
		start_full_active_test();
		slot_wait_drift = drift;
		require_published_on(1);
		UT_ASSERT_EQ(slot_wait_lookups, drift == 1 ? 2 : 1);
		UT_ASSERT_EQ(slot_wait_calls, drift == 1 ? 1 : 0);
		UT_ASSERT_EQ(cold_selection_calls, 0);
	}
}

UT_TEST(test_active_wait_terminal_cleanup_uses_original_capacity_not_rollover)
{
	start_full_active_test();
	cluster_undo_retention_horizon_enabled = true;
	slot_wait_commit = slot_wait_terminal_capacity = true;
	require_published_on(1);
	UT_ASSERT_EQ(slot_wait_calls, 1);
	UT_ASSERT_EQ(capacity_waits, 1);
	UT_ASSERT_EQ(cold_selection_calls, 0);
	cluster_undo_retention_horizon_enabled = false;
}

UT_TEST(test_active_wait_unknown_slot_identity_never_acquires_native_wait)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterTTSlotAllocPerSegment *segment;

		start_full_active_test();
		segment = &allocator_storage.per_node[0];
		switch (fault) {
		case 0:
			segment->binding_generation = 0;
			break;
		case 1:
			segment->binding_generation = UINT64_MAX;
			break;
		case 2:
			segment->slots[47].xid = FrozenTransactionId;
			break;
		case 3:
			segment->slots[47].xid = 200;
			break;
		case 4:
			segment->slots[47].wrap = TT_WRAP_INVALID;
			break;
		case 5:
			segment->slots[47].commit_scn = (SCN)91;
			break;
		case 6:
			segment->slots[47].status = UINT8_MAX;
			break;
		}
		require_capacity_error(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
		UT_ASSERT_EQ(slot_wait_lookups, 0);
		UT_ASSERT_EQ(slot_wait_calls, 0);
		UT_ASSERT_EQ(cold_selection_calls, 0);
	}
}

UT_TEST(test_allocator_snapshot_is_complete_readonly_and_unavailable_leaves_output)
{
	ClusterTTSlotCurrentSnapshot snapshot, before;
	ClusterTTSlotShmem *saved;

	start_full_active_test();
	UT_ASSERT(cluster_tt_slot_current_snapshot(0, &snapshot));
	UT_ASSERT_EQ(snapshot.segment_id, 1);
	UT_ASSERT_EQ(snapshot.binding_generation, 1);
	for (unsigned i = 0; i < 48; i++) {
		UT_ASSERT_EQ(snapshot.slots[i].segment_id, 1);
		UT_ASSERT_EQ(snapshot.slots[i].slot_offset, i);
		UT_ASSERT_EQ(snapshot.slots[i].xid, 200 + i);
		UT_ASSERT_EQ(snapshot.slots[i].status, CTS_ACTIVE);
	}
	before = snapshot;
	saved = ClusterTTSlotShm;
	ClusterTTSlotShm = NULL;
	UT_ASSERT(!cluster_tt_slot_current_snapshot(0, &snapshot));
	ClusterTTSlotShm = saved;
	UT_ASSERT(!cluster_tt_slot_current_snapshot(-1, &snapshot));
	UT_ASSERT(!cluster_tt_slot_current_snapshot(128, &snapshot));
	UT_ASSERT(!cluster_tt_slot_current_snapshot(0, NULL));
	UT_ASSERT_EQ(memcmp(&before, &snapshot, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 1);
	UT_ASSERT_EQ(g_bind_emit_calls, 0);
	UT_ASSERT_EQ(lock_depth, 0);
}

static void
prepare_gc_origin_snapshot(bool committed, unsigned prefix, ClusterCtrcTxnKeyV1 *key)
{
	reset_active_owner();
	for (unsigned i = 0; i < prefix; i++)
		UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 1000 + i), i);
	require_published_on(1);
	*key = g_ctrc_original_reservation.key;
	UT_ASSERT_EQ(key->slot_offset, prefix);
	g_ctrc_original_available = false;
	if (committed)
		cluster_tt_slot_mark_committed(1, key->slot_offset, key->xid, key->slot_wrap, 91);
	else
		cluster_tt_slot_mark_aborted(1, key->slot_offset, key->xid, key->slot_wrap);
}

UT_TEST(test_current_origin_snapshot_uses_original_terminal_slot_provenance)
{
	for (unsigned variant = 0; variant < 4; variant++) {
		ClusterCtrcTxnKeyV1 key, observed;
		ClusterTTSlotCurrentOwner owner;
		ClusterTTSlotAllocPerSegment before;
		uint64 binding = 0;
		bool committed = (variant & 1) != 0;

		prepare_gc_origin_snapshot(committed, variant < 2 ? 0 : 47, &key);
		before = allocator_storage.per_node[0];
		UT_ASSERT(cluster_tt_slot_current_origin_snapshot(1, key.slot_offset, &observed, &owner,
														  &binding));
		UT_ASSERT_EQ(memcmp(&observed, &key, sizeof(key)), 0);
		UT_ASSERT_EQ(binding, before.binding_generation);
		UT_ASSERT_EQ(owner.segment_id, 1);
		UT_ASSERT_EQ(owner.slot_offset, key.slot_offset);
		UT_ASSERT_EQ(owner.xid, key.xid);
		UT_ASSERT_EQ(owner.wrap, key.slot_wrap);
		UT_ASSERT_EQ(owner.status, committed ? CTS_COMMITTED : CTS_ABORTED);
		UT_ASSERT_EQ(owner.commit_scn, committed ? 91 : InvalidScn);
		UT_ASSERT_EQ(memcmp(&before, &allocator_storage.per_node[0], sizeof(before)), 0);
		UT_ASSERT_EQ(lock_depth, 0);
		UT_ASSERT_EQ(g_bind_emit_calls, 1);
	}
}

UT_TEST(test_current_origin_snapshot_rejects_missing_or_malformed_original_identity)
{
	for (unsigned fault = 0; fault < 22; fault++) {
		ClusterCtrcTxnKeyV1 key, observed;
		ClusterTTSlotCurrentOwner owner, zero = { 0 };
		ClusterTTSlotAllocPerSegment *segment;
		ClusterTTSlotOriginIdentity *identity;
		uint64 binding = 7;

		prepare_gc_origin_snapshot(true, 0, &key);
		segment = &allocator_storage.per_node[0];
		identity = &segment->origins[0];
		switch (fault) {
		case 0:
			segment->slots[0].status = CTS_ACTIVE;
			segment->slots[0].commit_scn = 0;
			break;
		case 1:
			segment->slots[0].status = CTS_FREE;
			break;
		case 2:
			segment->slots[0].status = UINT8_MAX;
			break;
		case 3:
			segment->slots[0].commit_scn = InvalidScn;
			break;
		case 4:
			segment->slots[0].status = CTS_ABORTED;
			break;
		case 5:
			segment->slots[0].wrap = TT_WRAP_MAX;
			identity->key.slot_wrap = TT_WRAP_MAX;
			break;
		case 6:
			segment->binding_generation = 0;
			break;
		case 7:
			segment->binding_generation = UINT64_MAX;
			break;
		case 8:
			identity->binding_generation++;
			break;
		case 9:
			identity->key.xid++;
			break;
		case 10:
			identity->key.slot_wrap++;
			break;
		case 11:
			identity->key.segment_id = 2;
			break;
		case 12:
			identity->key.slot_offset = 1;
			break;
		case 13:
			identity->key.origin_node_id = 1;
			break;
		case 14:
			identity->key.format_version = 0;
			break;
		case 15:
			identity->original_full_xid = InvalidFullTransactionId;
			break;
		case 16:
			identity->original_full_xid = FullTransactionIdFromU64(UINT64_MAX);
			break;
		case 17:
			identity->original_full_xid = FullTransactionIdFromU64(101);
			break;
		case 18:
			segment->slots[0]._pad = 1;
			break;
		case 19:
			memset(identity, 0, sizeof(*identity));
			break;
		case 20:
			identity->key.segment_generation = 0;
			break;
		case 21:
			segment->slots[0].wrap = TT_WRAP_INVALID;
			break;
		}
		memset(&observed, 0xa5, sizeof(observed));
		memset(&owner, 0xa5, sizeof(owner));
		UT_ASSERT(!cluster_tt_slot_current_origin_snapshot(1, 0, &observed, &owner, &binding));
		UT_ASSERT_EQ(memcmp(&observed, &(ClusterCtrcTxnKeyV1){ 0 }, sizeof(observed)), 0);
		UT_ASSERT_EQ(memcmp(&owner, &zero, sizeof(owner)), 0);
		UT_ASSERT_EQ(binding, 0);
		UT_ASSERT_EQ(lock_depth, 0);
	}
}

UT_TEST(test_current_origin_snapshot_never_initializes_or_outlives_original_slot)
{
	ClusterCtrcTxnKeyV1 key, observed;
	ClusterTTSlotCurrentOwner owner;
	ClusterTTSlotShmem *saved;
	uint64 binding;

	prepare_gc_origin_snapshot(true, 0, &key);
	UT_ASSERT(cluster_tt_slot_current_origin_snapshot(1, 0, &observed, &owner, &binding));
	UT_ASSERT(!cluster_tt_slot_current_origin_snapshot(0, 0, &observed, &owner, &binding));
	UT_ASSERT(!cluster_tt_slot_current_origin_snapshot(257, 0, &observed, &owner, &binding));
	UT_ASSERT(!cluster_tt_slot_current_origin_snapshot(2, 0, &observed, &owner, &binding));
	UT_ASSERT(!cluster_tt_slot_current_origin_snapshot(1, 48, &observed, &owner, &binding));
	UT_ASSERT(!cluster_tt_slot_current_origin_snapshot(1, 0, NULL, &owner, &binding));
	UT_ASSERT(!cluster_tt_slot_current_origin_snapshot(1, 0, &observed, NULL, &binding));
	UT_ASSERT(!cluster_tt_slot_current_origin_snapshot(1, 0, &observed, &owner, NULL));
	saved = ClusterTTSlotShm;
	ClusterTTSlotShm = NULL;
	UT_ASSERT(!cluster_tt_slot_current_origin_snapshot(1, 0, &observed, &owner, &binding));
	ClusterTTSlotShm = saved;
	cluster_tt_slot_free(1, 0);
	UT_ASSERT(!cluster_tt_slot_current_origin_snapshot(1, 0, &observed, &owner, &binding));
	UT_ASSERT_EQ(cluster_tt_slot_alloc(1, key.xid), 0);
	cluster_tt_slot_mark_committed(1, 0, key.xid, key.slot_wrap, 91);
	UT_ASSERT(!cluster_tt_slot_current_origin_snapshot(1, 0, &observed, &owner, &binding));
	cluster_tt_slot_rollover(0, 2, NULL);
	cluster_tt_slot_rollover(0, 1, NULL);
	UT_ASSERT(!cluster_tt_slot_current_origin_snapshot(1, 0, &observed, &owner, &binding));
	UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 1);
	UT_ASSERT_EQ(lock_depth, 0);
}

/* An absent parent producer reads zero, so the same expectations fail RED. */
static uint64
native_wakeup_rechecks(const ClusterUndoCapacityStats *stats pg_attribute_unused())
{
#ifdef CLUSTER_UNDO_CAPACITY_NATIVE_RECHECK_STATS_V1
	return stats->native_wakeup_recheck_count;
#else
	return 0;
#endif
}

static void
note_native_wakeup_recheck(void)
{
#ifdef CLUSTER_UNDO_CAPACITY_NATIVE_RECHECK_STATS_V1
	cluster_undo_cleaner_note_tx_slot_recheck();
#endif
}

UT_TEST(test_native_wakeup_recheck_counts_normal_owner_commit_and_abort_once)
{
	for (unsigned commit = 0; commit < 2; commit++) {
		ClusterUndoCapacityStats stats;

		start_full_active_test();
		slot_wait_commit = commit != 0;
		require_published_on(1);
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		UT_ASSERT_EQ(native_wakeup_rechecks(&stats), 1);
		UT_ASSERT_EQ(stats.wakeup_recheck_count, 0);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		UT_ASSERT_EQ(slot_wait_begins, 1);
		UT_ASSERT_EQ(slot_wait_ends, 1);
		UT_ASSERT(!slot_wait_in_native);
		UT_ASSERT_EQ(slot_wait_event, WAIT_EVENT_CLIENT_READ);
		UT_ASSERT_EQ(cold_selection_calls + capacity_waits + lock_depth, 0);
		require_published_on(1);
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		UT_ASSERT_EQ(native_wakeup_rechecks(&stats), 1);
		UT_ASSERT_EQ(slot_wait_calls, 1);
	}
}

UT_TEST(test_native_wakeup_recheck_excludes_errors_drift_and_refusals)
{
	static const int errors[]
		= { ERRCODE_QUERY_CANCELED, ERRCODE_LOCK_NOT_AVAILABLE, ERRCODE_T_R_DEADLOCK_DETECTED,
			ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS, ERRCODE_IO_ERROR };
	ClusterUndoCapacityStats stats;

	for (unsigned i = 0; i < lengthof(errors); i++) {
		start_full_active_test();
		slot_wait_error = errors[i];
		require_capacity_error(errors[i]);
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		UT_ASSERT_EQ(native_wakeup_rechecks(&stats), 0);
		UT_ASSERT_EQ(slot_wait_calls, 1);
		UT_ASSERT_EQ(slot_wait_begins, 1);
		UT_ASSERT_EQ(slot_wait_ends, 1);
		UT_ASSERT(!slot_wait_in_native);
		UT_ASSERT_EQ(slot_wait_event, WAIT_EVENT_CLIENT_READ);
		UT_ASSERT_EQ(g_bind_emit_calls + cold_selection_calls + capacity_waits + lock_depth, 0);
	}
	for (unsigned drift = 1; drift < 3; drift++) {
		start_full_active_test();
		slot_wait_drift = drift;
		UT_ASSERT(cluster_tt_local_wait_for_active_slot(1, 100));
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		UT_ASSERT_EQ(native_wakeup_rechecks(&stats), 0);
		UT_ASSERT_EQ(slot_wait_calls + slot_wait_begins + slot_wait_ends + g_bind_emit_calls, 0);
		/* A generation-only drift still needs one later native handoff. */
		require_published_on(1);
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		UT_ASSERT_EQ(native_wakeup_rechecks(&stats), drift == 1 ? 1 : 0);
		UT_ASSERT_EQ(slot_wait_calls, drift == 1 ? 1 : 0);
	}
	for (unsigned refusal = 0; refusal < 5; refusal++) {
		start_full_active_test();
		slot_wait_context_safe = refusal != 0;
		slot_wait_owner_kind = refusal;
		require_capacity_error(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		UT_ASSERT_EQ(native_wakeup_rechecks(&stats), 0);
		UT_ASSERT_EQ(slot_wait_calls + slot_wait_begins + slot_wait_ends + g_bind_emit_calls, 0);
	}
}

UT_TEST(test_native_wakeup_counter_is_initialized_unsigned_unavailable_and_lock_free)
{
	ClusterUndoCapacityStats stats, before;
	UndoCleanerSharedState *saved;
	unsigned lock_acquires;

	reset_active_owner();
	UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
	UT_ASSERT_EQ(native_wakeup_rechecks(&stats), 0);
	lock_acquires = slot_stats_lock_acquires;
	note_native_wakeup_recheck();
	note_native_wakeup_recheck();
	UT_ASSERT_EQ(slot_stats_lock_acquires, lock_acquires);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
	UT_ASSERT_EQ(native_wakeup_rechecks(&stats), 2);
	UT_ASSERT_EQ(stats.wakeup_recheck_count, 0);
#ifdef CLUSTER_UNDO_CAPACITY_NATIVE_RECHECK_STATS_V1
	pg_atomic_write_u64(&undo_cleaner_state->active_full_wakeup_recheck_count,
						UINT64_C(9223372036854775809));
	UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
	UT_ASSERT_EQ(native_wakeup_rechecks(&stats), UINT64_C(9223372036854775809));
#endif
	before = stats;
	saved = undo_cleaner_state;
	undo_cleaner_state = NULL;
	note_native_wakeup_recheck();
	UT_ASSERT(!cluster_undo_cleaner_capacity_stats_snapshot(&stats));
	UT_ASSERT_EQ(memcmp(&stats, &before, sizeof(stats)), 0);
	undo_cleaner_state = saved;
	UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
	UT_ASSERT_EQ(native_wakeup_rechecks(&stats), native_wakeup_rechecks(&before));
	UT_ASSERT(!cluster_undo_cleaner_capacity_stats_snapshot(NULL));
	reset_active_owner();
	UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
	UT_ASSERT_EQ(native_wakeup_rechecks(&stats), 0);
	printf("# capacity layout shared=%zu snapshot=%zu atomic=%zu\n",
		   sizeof(UndoCleanerSharedState), sizeof(ClusterUndoCapacityStats), sizeof(pg_atomic_uint64));
}

#ifdef TT_ACTIVE_OWNER_FIXTURE_MAIN
int TT_ACTIVE_OWNER_FIXTURE_MAIN(void);
#define main TT_ACTIVE_OWNER_FIXTURE_MAIN
#endif

int
main(void)
{
	UT_PLAN(55);
	UT_RUN(test_current_origin_snapshot_uses_original_terminal_slot_provenance);
	UT_RUN(test_current_origin_snapshot_rejects_missing_or_malformed_original_identity);
	UT_RUN(test_current_origin_snapshot_never_initializes_or_outlives_original_slot);
	UT_RUN(test_active_wait_unknown_slot_identity_never_acquires_native_wait);
	UT_RUN(test_allocator_snapshot_is_complete_readonly_and_unavailable_leaves_output);
	UT_RUN(test_49th_active_transaction_waits_in_selected_segment_without_rollover);
	UT_RUN(test_active_native_errors_propagate_without_publishing_or_double_leave);
	UT_RUN(test_active_wait_rechecks_exact_admission_and_lease_before_reservation);
	UT_RUN(test_active_wait_refuses_unknown_prepared_or_changed_unsafe_context);
	UT_RUN(test_active_refusal_stats_count_initial_unsafe_context);
	UT_RUN(test_active_refusal_stats_count_only_unproven_exact_owners);
	UT_RUN(test_active_refusal_stats_count_final_context_without_proof_refusal);
	UT_RUN(test_active_refusal_stats_do_not_count_success_or_snapshot_recheck);
	UT_RUN(test_active_snapshot_change_reenters_allocator_without_native_wait);
	UT_RUN(test_active_wait_terminal_cleanup_uses_original_capacity_not_rollover);
	UT_RUN(test_recovered_open_gate_precedes_canonical_reservation);
	UT_RUN(test_binding_memory_error_precedes_allocator_and_cold_selection);
	UT_RUN(test_binding_growth_error_preserves_existing_reservations);
	UT_RUN(test_modifier_refusal_precedes_allocation_and_leaves_abort_without_effects);
	UT_RUN(test_cold_allocator_preserves_occupied_durable_base);
	UT_RUN(test_cold_fresh_database_uses_same_publication_path);
	UT_RUN(test_cold_full_pool_refuses_before_binding_or_bind_wal);
	UT_RUN(test_real_allocator_normal_local_publication);
	UT_RUN(test_fresh_open_reserves_active_without_recovered_proof);
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
	UT_RUN(test_real_local_commit_keeps_bound_wrap);
	UT_RUN(test_real_local_abort_keeps_bound_wrap);
	UT_RUN(test_original_identity_survives_origin_retirement_and_terminal);
	UT_RUN(test_original_identity_exact_key_and_slot_lifetime);
	UT_RUN(test_missing_origin_identity_never_changes_active_success);
	UT_RUN(test_capacity_wait_releases_only_own_modifier_before_retry);
	UT_RUN(test_capacity_timed_rechecks_do_not_fail_or_reserve_early);
	UT_RUN(test_capacity_wait_preserves_other_admission_refusal);
	UT_RUN(test_capacity_cancel_has_no_publication_or_double_leave);
	UT_RUN(test_capacity_reentry_rejects_changed_admission_before_allocation);
	UT_RUN(test_capacity_wait_captures_and_rechecks_original_lease);
	UT_RUN(test_capacity_unknown_lease_refuses_before_sleep);
	UT_RUN(test_capacity_changed_lease_refuses_before_allocation);
	UT_RUN(test_retained_selected_segment_rechecks_allocator_after_capacity_wait);
	UT_RUN(test_capacity_after_successful_rollover_keeps_error_boundary);
	UT_RUN(test_capacity_unknown_rollover_effects_do_not_enter_wait);
	UT_RUN(test_capacity_passive_winner_adoption_preserves_safe_wait);
	UT_RUN(test_capacity_after_unpublished_reservation_does_not_reenter_preflight);
	UT_RUN(test_native_wakeup_recheck_counts_normal_owner_commit_and_abort_once);
	UT_RUN(test_native_wakeup_recheck_excludes_errors_drift_and_refusals);
	UT_RUN(test_native_wakeup_counter_is_initialized_unsigned_unavailable_and_lock_free);
	UT_DONE();
	return ut_failed_count != 0;
}
#ifdef TT_ACTIVE_OWNER_FIXTURE_MAIN
#undef main
#endif
