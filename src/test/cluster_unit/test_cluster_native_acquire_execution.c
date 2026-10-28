/*-------------------------------------------------------------------------
 *
 * test_cluster_native_acquire_execution.c
 *    Exercise original executing ownership and real native BufferIO records.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_native_acquire_execution.c
 *
 * NOTES
 *    This pgrac-original test uses actual native/PCM/codec/ResourceOwner
 *    implementations. Allocation and process/provider leaves are controlled.
 *    An executing capability is not installed bytes or native completion.
 *
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_acquire_stage_fixture.inc"
#include "utils/resowner_private.h"
#include "common/hashfn.h"
#include "test_cluster_acquire_resowner_types.inc"
ResourceOwner CurrentResourceOwner;
static void *
execution_alloc(Size size)
{
	void *p = calloc(1, size);
	UT_ASSERT(p != NULL);
	return p;
}
#define MemoryContextAlloc(context, size) execution_alloc(size)
#define MemoryContextAllocZero(context, size) execution_alloc(size)
#define pfree(pointer) free(pointer)
#include "test_cluster_acquire_resowner.inc"
#undef pfree
#undef MemoryContextAllocZero
#undef MemoryContextAlloc
/* The controlled PG lock leaf inspects the same held-lock fixture stack. */
bool
LWLockHeldByMe(LWLock *lock)
{
	return LWLockHeldByMeInMode(lock, LW_SHARED) || LWLockHeldByMeInMode(lock, LW_EXCLUSIVE);
}
#define LockBufHdr stage_header_lock
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#include "test_cluster_native_acquire_execution.inc"
#undef GetPrivateRefCount
#undef LockBufHdr

static ResourceXDeliveryClaim execution_claim;
static ResourceOwner original_io_owner;
static bool
execution_start(EvidenceFixture *f, int profile, int native_profile)
{
	if (!stage_start(f, profile, native_profile) || stage_enter() != RESOURCE_X_APPLY_APPLIED)
		return false;
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "native-execution");
	original_io_owner = CurrentResourceOwner;
	if (prepared_episode.path == RESOURCE_X_ACQUIRE_FIRST_FILL) {
		ResourceOwnerEnlargeBufferIOs(CurrentResourceOwner);
		ResourceOwnerRememberBufferIO(CurrentResourceOwner, 1);
	}
	memset(&execution_claim, 0xA5, sizeof(execution_claim));
	memset(&input_grant, 0xA5, sizeof(input_grant));
	memset(&input_image, 0xA5, sizeof(input_image));
	memset(&input_join, 0xA5, sizeof(input_join));
	request_rechecks = 0;
	request_hook = NULL;
	return true;
}
static ResourceXApplyResult
execution_enter(void)
{
	return cluster_bufmgr_pcm_own_acquire_execute_begin_exact(
		&request_buf, &request_base, &prepared_episode, &request_admission, &prepared_claim,
		&input_grant, &input_image, &input_join, &execution_claim);
}

UT_TEST(test_execution_io_membership_preserves_array_hash_holes_and_duplicates)
{
	ResourceOwner owner = ResourceOwnerCreate(NULL, "io-membership");
	UT_ASSERT(!ResourceOwnerHasBufferIO(NULL, 1));
	UT_ASSERT(!ResourceOwnerHasBufferIO(owner, InvalidBuffer));
	for (int n = 1; n <= 90; n++) {
		ResourceOwnerEnlargeBufferIOs(owner);
		ResourceOwnerRememberBufferIO(owner, n);
		if (n <= 5) {
			ResourceArray before = owner->bufferioarr;
			UT_ASSERT(ResourceOwnerHasBufferIO(owner, n));
			UT_ASSERT(memcmp(&before, &owner->bufferioarr, sizeof(before)) == 0);
		}
	}
	UT_ASSERT(owner->bufferioarr.capacity > 64);
	for (int n = 1; n <= 90; n += 2)
		ResourceOwnerForgetBufferIO(owner, n);
	for (int n = 1; n <= 90; n++) {
		ResourceArray before = owner->bufferioarr;
		Datum items[256];
		UT_ASSERT(before.capacity <= lengthof(items));
		memcpy(items, before.itemsarr, before.capacity * sizeof(Datum));
		UT_ASSERT_EQ(ResourceOwnerHasBufferIO(owner, n), n % 2 == 0);
		UT_ASSERT(memcmp(items, before.itemsarr, before.capacity * sizeof(Datum)) == 0);
		UT_ASSERT(memcmp(&before, &owner->bufferioarr, sizeof(before)) == 0);
	}
	ResourceOwnerEnlargeBufferIOs(owner);
	ResourceOwnerRememberBufferIO(owner, 2);
	ResourceOwnerForgetBufferIO(owner, 2);
	UT_ASSERT(ResourceOwnerHasBufferIO(owner, 2));
	ResourceOwnerForgetBufferIO(owner, 2);
	UT_ASSERT(!ResourceOwnerHasBufferIO(owner, 2));
}

UT_TEST(test_execution_io_membership_is_exact_owner_not_parent_or_buffer_bit)
{
	EvidenceFixture f;
	ResourceOwner parent;
	UT_ASSERT(execution_start(&f, 0, -1));
	parent = CurrentResourceOwner;
	CurrentResourceOwner = ResourceOwnerCreate(parent, "unrelated-child");
	UT_ASSERT(ResourceOwnerHasBufferIO(parent, 1));
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
	UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_ACQUIRE_PREPARED);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	CurrentResourceOwner = parent;
	ResourceOwnerForgetBufferIO(parent, 1);
	UT_ASSERT(pg_atomic_read_u32(&request_buf.state) & BM_IO_IN_PROGRESS);
	UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_BAD_STATE);
}

UT_TEST(test_execution_all_sources_and_native_modes_close_preparation_without_C)
{
	for (int profile = 0; profile < 10; profile++) {
		EvidenceFixture f;
		ClusterPcmOwnEntry own;
		uint32 state;
		UT_ASSERT(execution_start(&f, profile < 8 ? profile : 2, profile < 8 ? -1 : profile - 7));
		own = request_own;
		state = pg_atomic_read_u32(&request_buf.state);
		UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(execution_claim.purpose, RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING);
		UT_ASSERT_EQ(execution_claim.executor_sequence, 1);
		UT_ASSERT_EQ(stage_leave(&prepared_claim), RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_delivery_claim_end_exact(&execution_claim),
					 RESOURCE_X_APPLY_STALE);
		UT_ASSERT_EQ(input_grant.common.authority_generation, 10001);
		UT_ASSERT(input_join.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
		UT_ASSERT((input_join.flags & RESOURCE_X_REQUESTER_JOIN_READY) == 0);
		UT_ASSERT_EQ(input_join.t_install_us, 0);
		UT_ASSERT_EQ(request_shared.rings[1].count, 1);
		request_native_unchanged(&own, state);
	}
}

UT_TEST(test_execution_duplicate_never_reopens_or_rebinds_native)
{
	EvidenceFixture f;
	ResourceXDeliveryClaim saved;
	ClusterPcmOwnEntry own;
	uint32 state;
	UT_ASSERT(execution_start(&f, 1, 1));
	UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_APPLIED);
	saved = execution_claim;
	own = request_own;
	state = pg_atomic_read_u32(&request_buf.state);
	UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(memcmp(&saved, &execution_claim, sizeof(saved)) == 0);
	request_native_unchanged(&own, state);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_stage_begin_exact(
					 &execution_claim, &prepared_episode, &saved),
				 RESOURCE_X_APPLY_INVALID);
}

UT_TEST(test_execution_stale_prepared_capability_preserves_outputs_and_native)
{
	for (int field = 0; field < 6; field++) {
		EvidenceFixture f;
		ResourceXDeliveryClaim canary;
		ClusterPcmOwnEntry own;
		uint32 state;
		UT_ASSERT(execution_start(&f, 1, 1));
		if (field == 0)
			prepared_claim.executor_sequence++;
		if (field == 1)
			prepared_claim.observation.r4_record_generation++;
		if (field == 2)
			prepared_claim.observation.entry_binding_generation++;
		if (field == 3)
			prepared_claim.reserved[0] = 1;
		if (field == 4)
			prepared_episode.backend_generation++;
		if (field == 5)
			pg_atomic_write_u64(&request_own.delivery_attempt, 0);
		canary = execution_claim;
		own = request_own;
		state = pg_atomic_read_u32(&request_buf.state);
		UT_ASSERT(execution_enter() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&canary, &execution_claim, sizeof(canary)) == 0);
		request_native_unchanged(&own, state);
		UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
					 RESOURCE_X_DELIVERY_ACQUIRE_PREPARED);
	}
}

static void
execution_lose_io(void)
{
	ResourceOwnerForgetBufferIO(CurrentResourceOwner, 1);
}
UT_TEST(test_execution_lost_original_io_after_transition_retains_shared_and_native_debt)
{
	EvidenceFixture f;
	ResourceXDeliveryClaim canary;
	ResourceXDecodedFrame grant, image;
	ResourceXRequesterJoinSnapshot join;
	UT_ASSERT(execution_start(&f, 0, -1));
	canary = execution_claim;
	grant = input_grant;
	image = input_image;
	join = input_join;
	request_hook = execution_lose_io;
	UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(&canary, &execution_claim, sizeof(canary)) == 0);
	UT_ASSERT(memcmp(&grant, &input_grant, sizeof(grant)) == 0);
	UT_ASSERT(memcmp(&image, &input_image, sizeof(image)) == 0);
	UT_ASSERT(memcmp(&join, &input_join, sizeof(join)) == 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_own.flags), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(stage_leave(&prepared_claim), RESOURCE_X_APPLY_STALE);
}

static void
execution_error(void)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	pg_re_throw();
}
UT_TEST(test_execution_error_after_transition_never_reverts_or_aborts_original_io)
{
	EvidenceFixture f;
	volatile bool caught = false;
	UT_ASSERT(execution_start(&f, 0, -1));
	request_hook = execution_error;
	PG_TRY();
	{
		(void)execution_enter();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING);
	UT_ASSERT(pg_atomic_read_u32(&request_buf.state) & BM_IO_IN_PROGRESS);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
}

static void
execution_cancel(void)
{
	request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor = 1;
}
UT_TEST(test_execution_final_materials_recheck_observes_cancel_before_export)
{
	EvidenceFixture f;
	UT_ASSERT(execution_start(&f, 4, -1));
	request_hook = execution_cancel;
	UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
}

UT_TEST(test_execution_cancel_before_transition_keeps_prepared_quiet_cleanup)
{
	EvidenceFixture f;
	UT_ASSERT(execution_start(&f, 1, 1));
	execution_cancel();
	UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_BAD_STATE);
	UT_ASSERT_EQ(stage_leave(&prepared_claim), RESOURCE_X_APPLY_APPLIED);
}

static void
execution_change_native(void)
{
	pg_atomic_write_u64(&request_own.generation, 49);
}
UT_TEST(test_execution_native_turnover_after_transition_is_not_permission)
{
	EvidenceFixture f;
	UT_ASSERT(execution_start(&f, 1, 1));
	request_hook = execution_change_native;
	UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING);
	UT_ASSERT_EQ(pg_atomic_read_u64(&request_own.generation), 49);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
}

UT_TEST(test_execution_material_getter_rejects_old_phase_and_preserves_full_E)
{
	EvidenceFixture f;
	ResourceXDecodedFrame grant, image;
	ResourceXRequesterJoinSnapshot join;
	UT_ASSERT(execution_start(&f, 4, -1));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_execute_frames_exact(
					 &prepared_claim, &prepared_episode, &grant, &image, &join),
				 RESOURCE_X_APPLY_INVALID);
	UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_execute_frames_exact(
					 &execution_claim, &prepared_episode, &grant, &image, &join),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(grant.body.acquire_decision.episode.backend_generation, 71);
	UT_ASSERT(join.flags & RESOURCE_X_REQUESTER_JOIN_EVIDENCE_COMPLETE);
}

static void
execution_change_owner(void)
{
	CurrentResourceOwner = ResourceOwnerCreate(original_io_owner, "child-without-IO");
}
UT_TEST(test_execution_current_owner_change_after_transition_keeps_old_io_registration)
{
	EvidenceFixture f;
	ResourceXDeliveryClaim canary;
	UT_ASSERT(execution_start(&f, 0, -1));
	canary = execution_claim;
	/* Same thread changes the current owner at the controlled provider boundary. */
	request_hook = execution_change_owner;
	UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(memcmp(&canary, &execution_claim, sizeof(canary)) == 0);
}

UT_TEST(test_execution_entry_handoff_refuses_nested_content_owner)
{
	EvidenceFixture f;
	UT_ASSERT(execution_start(&f, 1, 1));
	LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
	UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_BAD_STATE);
	LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
				 RESOURCE_X_DELIVERY_ACQUIRE_PREPARED);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 1);
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_execution_io_membership_preserves_array_hash_holes_and_duplicates);
	UT_RUN(test_execution_io_membership_is_exact_owner_not_parent_or_buffer_bit);
	UT_RUN(test_execution_all_sources_and_native_modes_close_preparation_without_C);
	UT_RUN(test_execution_duplicate_never_reopens_or_rebinds_native);
	UT_RUN(test_execution_stale_prepared_capability_preserves_outputs_and_native);
	UT_RUN(test_execution_lost_original_io_after_transition_retains_shared_and_native_debt);
	UT_RUN(test_execution_error_after_transition_never_reverts_or_aborts_original_io);
	UT_RUN(test_execution_final_materials_recheck_observes_cancel_before_export);
	UT_RUN(test_execution_cancel_before_transition_keeps_prepared_quiet_cleanup);
	UT_RUN(test_execution_native_turnover_after_transition_is_not_permission);
	UT_RUN(test_execution_material_getter_rejects_old_phase_and_preserves_full_E);
	UT_RUN(test_execution_current_owner_change_after_transition_keeps_old_io_registration);
	UT_RUN(test_execution_entry_handoff_refuses_nested_content_owner);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
