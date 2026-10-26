/*-------------------------------------------------------------------------
 * test_cluster_tt_rollover_preflight.c
 *    Whole-invocation effect proof from the original rollover producer.
 *
 * The real record producer and TT binding publication run below. Segment
 * selection, physical lifecycle and current I/O are controlled boundaries;
 * these tests do not certify a live storage or scheduling interleaving.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef TT_ROLLOVER_SOURCE_PATH
#define TT_ROLLOVER_SOURCE_PATH "../../backend/cluster/cluster_undo_record.c"
#endif
#include TT_ROLLOVER_SOURCE_PATH
#include "../../backend/cluster/cluster_tt_slot.c"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static ClusterUndoRecordShared record_storage;
static ClusterTTSlotShmem slot_storage;
static LWLock *held[4];
static unsigned held_count, selections, provisions, reuses, activations, publications;
static bool select_candidate, select_hard_cap, needs_provision, needs_reuse;
static bool candidate_taken, activation_ready, publication_ready, publication_current;
static bool throw_provision;
static uint8 segment_state;
static const ClusterUndoBlock0LiveOwnerPublication *last_publication;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode pg_attribute_unused())
{
	UT_ASSERT(held_count < lengthof(held));
	if (held_count < lengthof(held))
		held[held_count++] = lock;
	return false;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(held_count > 0 && held[held_count - 1] == lock);
	if (held_count > 0)
		held_count--;
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode pg_attribute_unused())
{
	for (unsigned i = 0; i < held_count; i++)
		if (held[i] == lock)
			return true;
	return false;
}

bool
cluster_undo_segment_extend_or_create(uint8 owner, ClusterUndoSegmentExtendPlan *plan)
{
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT(LWLockHeldByMeInMode(&record_storage.lifecycle_lock.lock, LW_EXCLUSIVE));
	memset(plan, 0, sizeof(*plan));
	selections++;
	if (!select_candidate || selections > 1) {
		plan->at_hard_cap = select_hard_cap;
		return false;
	}
	plan->segment_id = 2;
	plan->generation = 1;
	plan->needs_provision = needs_provision;
	plan->needs_reuse = needs_reuse;
	return true;
}

static void
require_physical_boundary(uint32 segment, uint8 owner)
{
	UT_ASSERT_EQ(segment, 2);
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT(LWLockHeldByMeInMode(&record_storage.cursor_lock.lock, LW_EXCLUSIVE));
	UT_ASSERT(!LWLockHeldByMeInMode(&record_storage.lifecycle_lock.lock, LW_EXCLUSIVE));
}

void
cluster_undo_segment_allocate(uint32 segment, uint8 owner)
{
	require_physical_boundary(segment, owner);
	provisions++;
	if (throw_provision) {
		UT_ASSERT_NOT_NULL(PG_exception_stack);
		siglongjmp(*PG_exception_stack, 1);
	}
	segment_state = candidate_taken ? SEGMENT_ACTIVE : SEGMENT_ALLOCATED;
}

uint32
cluster_undo_segment_reuse_in_place(uint32 segment, uint8 owner, uint32 generation)
{
	require_physical_boundary(segment, owner);
	UT_ASSERT_EQ(generation, 1);
	reuses++;
	segment_state = candidate_taken ? SEGMENT_ACTIVE : SEGMENT_ALLOCATED;
	return segment;
}

uint8
cluster_undo_segment_read_state(uint32 segment, uint8 owner)
{
	UT_ASSERT_EQ(segment, 2);
	UT_ASSERT_EQ(owner, 1);
	return segment_state;
}

uint32
cluster_undo_segment_generation(uint32 segment, uint8 owner)
{
	UT_ASSERT_EQ(segment, 2);
	UT_ASSERT_EQ(owner, 1);
	return 1;
}

ClusterUndoPoolObservationResult
cluster_undo_segment_observe_pool(uint8 owner, ClusterUndoPoolObservation *out)
{
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT(LWLockHeldByMeInMode(&record_storage.lifecycle_lock.lock, LW_EXCLUSIVE));
	memset(out, 0, sizeof(*out));
	out->allocated_count = 2;
	return CLUSTER_UNDO_POOL_OBS_OK;
}

bool
cluster_undo_segment_mark_active(uint32 segment, uint8 owner)
{
	require_physical_boundary(segment, owner);
	activations++;
	if (activation_ready)
		segment_state = SEGMENT_ACTIVE;
	return activation_ready;
}

bool
cluster_undo_segment_mark_committed(uint32 segment pg_attribute_unused(),
									uint8 owner pg_attribute_unused())
{
	/* These cases roll away the fixed first segment, which stays protected. */
	UT_ASSERT(false);
	return false;
}

ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_ensure_resident_exact(
	const ClusterUndoBlock0LogicalKey *key, int timeout,
	ClusterUndoBlock0LiveOwnerPublication *publication)
{
	require_physical_boundary(key->segment_id, key->owner_instance);
	UT_ASSERT_EQ(timeout, 10000);
	UT_ASSERT_NOT_NULL(publication);
	publications++;
	last_publication = publication;
	return publication_ready ? CLUSTER_UNDO_BLOCK0_OK : CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
}

bool
cluster_undo_block0_current_live_owner_publication_recheck(
	const ClusterUndoBlock0LiveOwnerPublication *publication)
{
	UT_ASSERT(publication == last_publication);
	UT_ASSERT(LWLockHeldByMeInMode(&record_storage.lifecycle_lock.lock, LW_EXCLUSIVE));
	return publication_current;
}

void
cluster_undo_cleaner_wakeup(void)
{}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

static void
reset_rollover(void)
{
	memset(&record_storage, 0, sizeof(record_storage));
	memset(&slot_storage, 0, sizeof(slot_storage));
	UndoRecordShared = &record_storage;
	ClusterTTSlotShm = &slot_storage;
	held_count = selections = provisions = reuses = activations = publications = 0;
	select_candidate = needs_provision = needs_reuse = candidate_taken = throw_provision = false;
	select_hard_cap = activation_ready = publication_ready = publication_current = true;
	segment_state = SEGMENT_ALLOCATED;
	last_publication = NULL;
	cluster_tt_slot_rollover(0, 1, NULL);
}

UT_TEST(test_pure_selection_failure_proves_no_effects)
{
	for (int capped = 0; capped < 2; capped++) {
		bool hard_cap = !capped;
		bool no_effects = false;

		reset_rollover();
		select_hard_cap = capped != 0;
		UT_ASSERT_EQ(cluster_undo_tt_rollover_locked(0, 1, &hard_cap, &no_effects), 0);
		UT_ASSERT_EQ(hard_cap, capped != 0);
		UT_ASSERT(no_effects);
		UT_ASSERT_EQ(selections, 1);
		UT_ASSERT_EQ(activations, 0);
		UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 1);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_passive_winner_adoption_has_no_local_effects)
{
	bool hard_cap = true;
	bool no_effects = false;

	reset_rollover();
	cluster_tt_slot_rollover(0, 2, NULL);
	UT_ASSERT_EQ(cluster_undo_tt_rollover_locked(0, 1, &hard_cap, &no_effects), 2);
	UT_ASSERT(!hard_cap);
	UT_ASSERT(no_effects);
	UT_ASSERT_EQ(selections, 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_reselection_after_provision_or_reuse_never_regains_no_effects)
{
	for (int provision = 0; provision < 2; provision++) {
		bool hard_cap = false;
		bool no_effects = true;

		reset_rollover();
		select_candidate = candidate_taken = true;
		needs_provision = provision != 0;
		needs_reuse = provision == 0;
		UT_ASSERT_EQ(cluster_undo_tt_rollover_locked(0, 1, &hard_cap, &no_effects), 0);
		UT_ASSERT(hard_cap);
		UT_ASSERT(!no_effects);
		UT_ASSERT_EQ(selections, 2);
		UT_ASSERT_EQ(provisions, provision != 0);
		UT_ASSERT_EQ(reuses, provision == 0);
		UT_ASSERT_EQ(activations, 0);
		UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 1);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_activation_or_publication_refusal_keeps_effects_unknown)
{
	for (unsigned refusal = 0; refusal < 3; refusal++) {
		bool hard_cap = true;
		bool no_effects = true;

		reset_rollover();
		select_candidate = true;
		activation_ready = refusal != 0;
		publication_ready = refusal != 1;
		publication_current = refusal != 2;
		UT_ASSERT_EQ(cluster_undo_tt_rollover_locked(0, 1, &hard_cap, &no_effects), 0);
		UT_ASSERT(!hard_cap);
		UT_ASSERT(!no_effects);
		UT_ASSERT_EQ(activations, 1);
		UT_ASSERT_EQ(publications, refusal != 0);
		UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 1);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_real_current_publication_cannot_return_to_preflight)
{
	bool hard_cap = true;
	bool no_effects = true;

	reset_rollover();
	select_candidate = true;
	UT_ASSERT_EQ(cluster_undo_tt_rollover_locked(0, 1, &hard_cap, &no_effects), 2);
	UT_ASSERT(!hard_cap);
	UT_ASSERT(!no_effects);
	UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 2);
	UT_ASSERT_EQ(cluster_undo_tt_retention_rollover_count(), 1);
	UT_ASSERT_EQ(publications, 1);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_error_and_unavailable_owner_leave_proof_false)
{
	bool hard_cap = true;
	bool no_effects = true;
	static bool error_no_effects;
	volatile bool caught = false;

	reset_rollover();
	UndoRecordShared = NULL;
	UT_ASSERT_EQ(cluster_undo_tt_rollover_locked(0, 1, &hard_cap, &no_effects), 0);
	UT_ASSERT(!hard_cap);
	UT_ASSERT(!no_effects);
	reset_rollover();
	select_candidate = needs_provision = throw_provision = true;
	error_no_effects = true;
	PG_TRY();
	{
		(void)cluster_undo_tt_rollover_locked(0, 1, &hard_cap, &error_no_effects);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(!error_no_effects);
	UT_ASSERT_EQ(provisions, 1);
	/* The original ERROR owner releases remaining LWLocks; the producer
	 * neither releases them after longjmp nor certifies an effect-free retry. */
	UT_ASSERT_EQ(held_count, 1);
	held_count = 0;
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_pure_selection_failure_proves_no_effects);
	UT_RUN(test_passive_winner_adoption_has_no_local_effects);
	UT_RUN(test_reselection_after_provision_or_reuse_never_regains_no_effects);
	UT_RUN(test_activation_or_publication_refusal_keeps_effects_unknown);
	UT_RUN(test_real_current_publication_cannot_return_to_preflight);
	UT_RUN(test_error_and_unavailable_owner_leave_proof_false);
	UT_DONE();
	return ut_failed_count != 0;
}
