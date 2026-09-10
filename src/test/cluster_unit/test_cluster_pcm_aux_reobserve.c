/*-------------------------------------------------------------------------
 * test_cluster_pcm_aux_reobserve.c
 *
 * Execute production auxiliary pin-handoff and observation decisions.
 * The source fragments are generated verbatim; buffer/pin/lock edges are
 * controlled fixtures, not a multi-process transport replay.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_semantic_activation.h"
#include "storage/bufmgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
int NBuffers = 2;
int NLocBuffer = 0;
bool cluster_shared_catalog = false;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

static BufferDesc fixture_buffers[2];
static int fixture_refs[2];
static bool fixture_content[2];
static int fixture_releases;
static int fixture_old_access;
static sigjmp_buf fixture_error;
static int fixture_lock_outcome;
static int fixture_errors;
static int fixture_core_calls;

static void
fixture_failure(void)
{
	fixture_errors++;
	siglongjmp(fixture_error, 1);
}

static void
fixture_lock_internal(Buffer buffer, int mode, bool *barrier,
					  const ClusterBufferBarrierSiteId *site, bool *replaced, bool *transient,
					  ResourceXAuxiliaryAcquireContext *context)
{
	UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
	UT_ASSERT(site == NULL && context != NULL);
	UT_ASSERT_EQ(fixture_refs[buffer - 1], 1);
	*barrier = *replaced = *transient = false;
	if (fixture_lock_outcome == 1) {
		fixture_refs[buffer - 1] = 0;
		fixture_buffers[buffer - 1].tag.relNumber = 999;
		*replaced = true;
		context->active = context->reobserve = true;
	} else if (fixture_lock_outcome == 2)
		*barrier = true;
	else if (fixture_lock_outcome == 3)
		fixture_failure();
	else
		fixture_content[buffer - 1] = true;
}

static ResourceXApplyResult
fixture_acquire_internal(BufferDesc *buf, const BufferTag *tag, uint64 r4, uint64 direct_generation,
						 uint64 direct_token, bool join, uint64 *deadline,
						 ResourceXAuxiliaryAcquireContext *context, ResourceXAcquisitionRef *out)
{
	UT_ASSERT(buf != NULL);
	UT_ASSERT(BufferTagsEqual(&context->resource, tag));
	UT_ASSERT_EQ(r4, context->r4_record_generation);
	UT_ASSERT_EQ(direct_generation, 0);
	UT_ASSERT_EQ(direct_token, 0);
	UT_ASSERT(!join && deadline == &context->absolute_deadline_us);
	fixture_core_calls++;
	memset(out, 0, sizeof(*out));
	if (context->caller.failed_attempt != 0)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (fixture_core_calls == 1) {
		*deadline = 9000;
		context->caller.joined_request.assertion_sequence = 77;
	} else {
		UT_ASSERT_EQ(*deadline, 9000);
		UT_ASSERT_EQ(context->caller.joined_request.assertion_sequence, 77);
	}
	context->reobserve = true;
	return RESOURCE_X_APPLY_NOT_FOUND;
}

static int32
GetPrivateRefCount(Buffer buffer)
{
	return fixture_refs[buffer - 1];
}

static void
fixture_release(Buffer buffer)
{
	UT_ASSERT(buffer > 0 && buffer <= 2);
	UT_ASSERT(fixture_refs[buffer - 1] > 0);
	fixture_refs[buffer - 1]--;
	fixture_releases++;
}

static bool
fixture_recent(RelFileLocator locator, ForkNumber fork, BlockNumber block, Buffer buffer)
{
	BufferTag expected;
	BufferDesc *buf = &fixture_buffers[buffer - 1];

	InitBufferTag(&expected, &locator, fork, block);
	if (!BufferTagsEqual(&expected, &buf->tag))
		return false;
	fixture_refs[buffer - 1]++;
	return true;
}

static bool
fixture_current(BufferDesc *buf, uint32 state)
{
	if (buf == &fixture_buffers[0] && buf->tag.relNumber == 999)
		fixture_old_access++;
	return (state & BM_VALID) != 0 && buf->pcm_state == (uint8)PCM_STATE_X;
}

#define LWLockHeldByMe(lock)                                                                       \
	((lock) == BufferDescriptorGetContentLock(&fixture_buffers[0]) ? fixture_content[0]            \
																   : fixture_content[1])
#define LockBufHdr(buf) (pg_atomic_read_u32(&(buf)->state))
#define UnlockBufHdr(buf, state) ((void)(buf), (void)(state))
#define ReleaseBuffer fixture_release
#define ReadRecentBuffer fixture_recent
#define cluster_bufmgr_pcm_current_image_locked fixture_current
#define GetBufferDescriptor(id) (&fixture_buffers[(id)])
#define cluster_pcm_is_active() true
#define cluster_bufmgr_resource_x_writer_report_failure(...) fixture_failure()
#define LockBufferInternal fixture_lock_internal

#include "test_cluster_pcm_aux_handoff.inc"
#define gcs_block_resource_x_target_own_observation_result fixture_production_own_guard
#include "test_cluster_pcm_aux_observation.inc"
#include "test_cluster_pcm_aux_reobserve_owner.inc"
#define gcs_block_resource_x_target_acquire_internal fixture_acquire_internal
#include "test_cluster_pcm_aux_reobserve_context.inc"

static ResourceXApplyResult
fixture_open_gate(ResourceXGateSnapshot *gate)
{
	memset(gate, 0, sizeof(*gate));
	gate->phase = RESOURCE_X_GATE_OPEN;
	gate->formation = 2;
	return RESOURCE_X_APPLY_APPLIED;
}

/* The acquired-authority branches are not part of this fixture: reaching
 * them after NOT_FOUND must fail, never manufacture a grant for the test. */
#define cluster_gcs_resource_x_target_acquire_until_exact(...)                                     \
	(fixture_failure(), RESOURCE_X_APPLY_INVALID)
#define cluster_pcm_lock_resource_x_gate_snapshot fixture_open_gate
#define cluster_bufmgr_pcm_own_snapshot(buf, out)                                                  \
	((void)(buf), (void)(out), fixture_failure(), CLUSTER_PCM_OWN_STALE)
#define cluster_resource_x_writer_path_snapshot(generation)                                        \
	(*(generation) = 6, RESOURCE_X_WRITER_TARGET)
#define cluster_gcs_resource_x_target_context_recheck_exact(context) ((void)(context), false)
#define cluster_bufmgr_resource_x_fail_closed_exact(gate) ((void)(gate), fixture_failure())
#undef ereport
#define ereport(...) fixture_failure()
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() fixture_failure()
#include "test_cluster_pcm_aux_prepare.inc"

static BufferTag
fixture_reset(ForkNumber fork)
{
	BufferTag tag = { 0 };

	tag.spcOid = 1663;
	tag.dbOid = 5;
	tag.relNumber = 16393;
	tag.forkNum = fork;
	memset(fixture_buffers, 0, sizeof(fixture_buffers));
	memset(fixture_refs, 0, sizeof(fixture_refs));
	memset(fixture_content, 0, sizeof(fixture_content));
	for (int i = 0; i < 2; i++) {
		fixture_buffers[i].buf_id = i;
		fixture_buffers[i].tag = tag;
		fixture_buffers[i].pcm_state = (uint8)PCM_STATE_X;
		pg_atomic_init_u32(&fixture_buffers[i].state, BM_VALID | BM_TAG_VALID);
	}
	fixture_refs[0] = 1;
	fixture_releases = fixture_old_access = 0;
	fixture_lock_outcome = fixture_errors = fixture_core_calls = 0;
	memset(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	return tag;
}

UT_TEST(handoff_releases_every_private_pin_before_the_wait)
{
	BufferTag tag = fixture_reset(VISIBILITYMAP_FORKNUM);
	ClusterBufmgrPcmAuxPinHandoff handoff;

	fixture_refs[0] = 3;
	UT_ASSERT(cluster_bufmgr_pcm_aux_pin_handoff_begin(&fixture_buffers[0], &tag, &handoff));
	UT_ASSERT_EQ(fixture_refs[0], 0);
	UT_ASSERT_EQ(fixture_releases, 3);
	UT_ASSERT_EQ(handoff.released_refs, 3);
	UT_ASSERT(handoff.active);
	UT_ASSERT(cluster_bufmgr_pcm_aux_pin_handoff_finish_exact(&fixture_buffers[0], &handoff));
	UT_ASSERT_EQ(fixture_refs[0], 3);
	UT_ASSERT(!handoff.active);
}

UT_TEST(retagged_old_slot_is_never_repinned_or_read)
{
	BufferTag tag = fixture_reset(VISIBILITYMAP_FORKNUM);
	ClusterBufmgrPcmAuxPinHandoff handoff;

	UT_ASSERT(cluster_bufmgr_pcm_aux_pin_handoff_begin(&fixture_buffers[0], &tag, &handoff));
	fixture_buffers[0].tag.relNumber = 999;
	UT_ASSERT(!cluster_bufmgr_pcm_aux_pin_handoff_finish_exact(&fixture_buffers[0], &handoff));
	UT_ASSERT_EQ(fixture_refs[0], 0);
	UT_ASSERT_EQ(fixture_refs[1], 0);
	UT_ASSERT_EQ(fixture_old_access, 0);
	UT_ASSERT(BufferTagsEqual(&fixture_buffers[1].tag, &tag));
}

UT_TEST(unowned_vm_observation_replacement_is_reobserve_not_stale)
{
	BufferTag tag = fixture_reset(VISIBILITYMAP_FORKNUM);
	ClusterPcmOwnSnapshot own = { 0 };

	own.tag = tag;
	own.tag.relNumber = 999;
	own.generation = 8622;
	UT_ASSERT_EQ(fixture_production_own_guard(&own, &tag, true), RESOURCE_X_APPLY_NOT_FOUND);
}

UT_TEST(pinned_or_retained_identity_contradiction_is_still_stale)
{
	BufferTag tag = fixture_reset(VISIBILITYMAP_FORKNUM);
	ClusterPcmOwnSnapshot own = { 0 };

	own.tag = tag;
	own.tag.relNumber = 999;
	own.generation = 8622;
	UT_ASSERT_EQ(fixture_production_own_guard(&own, &tag, false), RESOURCE_X_APPLY_STALE);
	own.tag = tag;
	own.generation = UINT64_MAX;
	UT_ASSERT_EQ(fixture_production_own_guard(&own, &tag, true), RESOURCE_X_APPLY_STALE);
	own.generation = 8622;
	UT_ASSERT_EQ(fixture_production_own_guard(&own, &tag, true), RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(real_unowned_gate_requires_empty_pin_and_handoff_not_retained_writer)
{
	ClusterPcmXWriterLedgerEntry *entry;

	(void)fixture_reset(FSM_FORKNUM);
	UT_ASSERT(!ClusterBufferAuxiliaryObservationUnowned(1));
	entry = cluster_bufmgr_pcm_x_writer_free_entry();
	UT_ASSERT_NOT_NULL(entry);
	entry->buffer_id = 0;
	entry->content_lock = BufferDescriptorGetContentLock(&fixture_buffers[0]);
	entry->phase = PCM_X_WRITER_LEDGER_HANDOFF;
	UT_ASSERT(!ClusterBufferAuxiliaryObservationUnowned(1));
	fixture_refs[0] = 0;
	UT_ASSERT(ClusterBufferAuxiliaryObservationUnowned(1));
	fixture_content[0] = true;
	UT_ASSERT(!ClusterBufferAuxiliaryObservationUnowned(1));
	fixture_content[0] = false;
	entry->phase = PCM_X_WRITER_LEDGER_ACQUIRING;
	UT_ASSERT(!ClusterBufferAuxiliaryObservationUnowned(1));
	cluster_bufmgr_pcm_x_writer_clear(entry);
	UT_ASSERT(!ClusterBufferAuxiliaryObservationUnowned(1));
}

UT_TEST(actual_handle_owner_invalidates_replaced_pin_and_keeps_other_pin_on_retry)
{
	ResourceXAuxiliaryAcquireContext context = { 0 };
	Buffer buffer = 1;

	(void)fixture_reset(VISIBILITYMAP_FORKNUM);
	fixture_lock_outcome = 1;
	UT_ASSERT(!ClusterLockBufferExclusiveAuxiliaryAware(&buffer, &context));
	UT_ASSERT_EQ(buffer, InvalidBuffer);
	UT_ASSERT_EQ(fixture_refs[0], 0);
	UT_ASSERT_EQ(fixture_releases, 0); /* handoff already released it */
	UT_ASSERT(context.active && context.reobserve);
	(void)fixture_reset(VISIBILITYMAP_FORKNUM);
	buffer = 1;
	fixture_lock_outcome = 2;
	UT_ASSERT(!ClusterLockBufferExclusiveAuxiliaryAware(&buffer, &context));
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(fixture_refs[0], 1);
	fixture_lock_outcome = 0;
	UT_ASSERT(ClusterLockBufferExclusiveAuxiliaryAware(&buffer, &context));
	UT_ASSERT(!context.active);
	UT_ASSERT(fixture_content[0]);
}

UT_TEST(actual_handle_owner_refuses_unowned_aliases_before_releasing_any_pin)
{
	ResourceXAuxiliaryAcquireContext context = { 0 };
	Buffer buffer = 1;

	(void)fixture_reset(VISIBILITYMAP_FORKNUM);
	fixture_refs[0] = 2;
	if (sigsetjmp(fixture_error, 1) == 0) {
		(void)ClusterLockBufferExclusiveAuxiliaryAware(&buffer, &context);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(fixture_errors, 1);
	UT_ASSERT_EQ(fixture_refs[0], 2);
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(fixture_releases, 0);
}

UT_TEST(real_reobserve_entry_preserves_failure_history_namespace_and_deadline)
{
	BufferTag tag = fixture_reset(VISIBILITYMAP_FORKNUM);
	ResourceXAuxiliaryAcquireContext context = { 0 };
	ResourceXAcquisitionRef ref;

	UT_ASSERT_EQ(cluster_gcs_resource_x_target_acquire_reobserve_exact(&fixture_buffers[0], &tag, 6,
																	   &context, &ref),
				 RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(context.absolute_deadline_us, 9000);
	UT_ASSERT_EQ(context.caller.joined_request.assertion_sequence, 77);
	UT_ASSERT_EQ(ref.acquisition_generation, 0);
	UT_ASSERT_EQ(cluster_gcs_resource_x_target_acquire_reobserve_exact(&fixture_buffers[1], &tag, 6,
																	   &context, &ref),
				 RESOURCE_X_APPLY_NOT_FOUND);
	UT_ASSERT_EQ(context.absolute_deadline_us, 9000);
	UT_ASSERT_EQ(fixture_core_calls, 2);
	UT_ASSERT_EQ(cluster_gcs_resource_x_target_acquire_reobserve_exact(&fixture_buffers[1], &tag, 7,
																	   &context, &ref),
				 RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(fixture_core_calls, 2);
	UT_ASSERT(!context.reobserve);
	context.caller.failed_attempt = 77;
	UT_ASSERT_EQ(cluster_gcs_resource_x_target_acquire_reobserve_exact(&fixture_buffers[1], &tag, 6,
																	   &context, &ref),
				 RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(context.caller.failed_attempt, 77);
	UT_ASSERT_EQ(ref.acquisition_generation, 0);
	UT_ASSERT(!context.reobserve);
}

UT_TEST(actual_prepare_preserves_fsm_exclusion_and_reobserves_vm_mapping_loss)
{
	for (int fork = FSM_FORKNUM; fork <= VISIBILITYMAP_FORKNUM; fork++) {
		BufferTag tag = fixture_reset((ForkNumber)fork);
		ClusterBufmgrPcmAuxPinHandoff handoff;
		ResourceXAuxiliaryAcquireContext context = { 0 };
		BufferDesc untouched;
		uint64 deadline = 0;
		bool barrier = false, transient = false;

		UT_ASSERT(cluster_bufmgr_pcm_aux_pin_handoff_begin(&fixture_buffers[0], &tag, &handoff));
		memset(&fixture_buffers[0].tag, 0, sizeof(BufferTag));
		memcpy(&untouched, &fixture_buffers[0], sizeof(untouched));
		UT_ASSERT(cluster_bufmgr_pcm_x_writer_prepare_target(&fixture_buffers[0], &tag,
															 PCM_LOCK_MODE_X, 6, &deadline,
															 &barrier, &transient, &context)
				  == NULL);
		if (fork == FSM_FORKNUM) {
			/* The real tracking predicate excludes advisory FSM ownership. */
			UT_ASSERT(!context.reobserve);
			UT_ASSERT_EQ(fixture_core_calls, 0);
			UT_ASSERT_EQ(deadline, 0);
		} else {
			UT_ASSERT(context.reobserve);
			UT_ASSERT_EQ(fixture_core_calls, 1);
			UT_ASSERT_EQ(deadline, 9000);
		}
		UT_ASSERT(!barrier && !transient);
		UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&fixture_buffers[0]) == NULL);
		UT_ASSERT_EQ(fixture_refs[0], 0);
		UT_ASSERT_EQ(fixture_refs[1], 0);
		UT_ASSERT_EQ(memcmp(&untouched, &fixture_buffers[0], sizeof(untouched)), 0);
	}
}

UT_TEST(actual_prepare_does_not_turn_failed_history_or_namespace_into_retry)
{
	for (int failure = 0; failure < 2; failure++) {
		BufferTag tag = fixture_reset(VISIBILITYMAP_FORKNUM);
		ClusterBufmgrPcmAuxPinHandoff handoff;
		ResourceXAuxiliaryAcquireContext context = { 0 };
		uint64 deadline = 9000;
		bool barrier = false, transient = false;

		context.active = context.reobserve = true;
		context.resource = tag;
		context.r4_record_generation = failure == 0 ? 7 : 6;
		context.caller.failed_attempt = failure == 0 ? 0 : 77;
		context.absolute_deadline_us = deadline;
		UT_ASSERT(cluster_bufmgr_pcm_aux_pin_handoff_begin(&fixture_buffers[0], &tag, &handoff));
		if (sigsetjmp(fixture_error, 1) == 0) {
			(void)cluster_bufmgr_pcm_x_writer_prepare_target(&fixture_buffers[0], &tag,
															 PCM_LOCK_MODE_X, 6, &deadline,
															 &barrier, &transient, &context);
			UT_ASSERT(false);
		}
		UT_ASSERT_EQ(fixture_errors, 1);
		UT_ASSERT(!context.reobserve && !barrier && !transient);
		UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&fixture_buffers[0]) == NULL);
		UT_ASSERT_EQ(fixture_refs[0], 0);
	}
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(handoff_releases_every_private_pin_before_the_wait);
	UT_RUN(retagged_old_slot_is_never_repinned_or_read);
	UT_RUN(unowned_vm_observation_replacement_is_reobserve_not_stale);
	UT_RUN(pinned_or_retained_identity_contradiction_is_still_stale);
	UT_RUN(real_unowned_gate_requires_empty_pin_and_handoff_not_retained_writer);
	UT_RUN(actual_handle_owner_invalidates_replaced_pin_and_keeps_other_pin_on_retry);
	UT_RUN(actual_handle_owner_refuses_unowned_aliases_before_releasing_any_pin);
	UT_RUN(real_reobserve_entry_preserves_failure_history_namespace_and_deadline);
	UT_RUN(actual_prepare_preserves_fsm_exclusion_and_reobserves_vm_mapping_loss);
	UT_RUN(actual_prepare_does_not_turn_failed_history_or_namespace_into_retry);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
