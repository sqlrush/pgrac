/*-------------------------------------------------------------------------
 *
 * test_cluster_pcm_aux_consumer.c
 *    Exercise the actual auxiliary-page initialization consumer.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_pcm_aux_consumer.c
 *
 * NOTES
 *    The arm, gate and upper consumer bodies are generated verbatim from
 *    bufmgr.c. Only process, transport and physical-buffer dependencies are
 *    fixtures. This is not a transport or multi-process runtime proof.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_pcm_direct_init.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_semantic_activation.h"
#include "storage/bufmgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static BufferDesc fixture_buf;
static ClusterPcmDirectInitSnapshot fixture_state;
static sigjmp_buf fixture_error;
static bool fixture_remote;
static bool fixture_context_valid;
static bool fixture_content_held;
static int fixture_mutation;
static int fixture_ordinary;
static int fixture_track;
static int fixture_activate;
static int fixture_fuse;
static int fixture_acquires;
static int fixture_reservations;
static uint64 fixture_payload;
static int fixture_context_pending, fixture_waits, fixture_post_t3_snapshots;
static bool fixture_change_on_wait, fixture_cancel_on_wait;
static void (*fixture_wait_check)(void);

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

typedef struct ClusterBufmgrPcmAuxPinHandoff {
	BufferTag tag;
	Buffer buffer;
	int32 released_refs;
	bool active;
} ClusterBufmgrPcmAuxPinHandoff;

void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}

void
pg_re_throw(void)
{
	abort();
}

static void
fixture_snapshot(BufferDesc *buf, uint32 state, bool is_new, ClusterPcmDirectInitSnapshot *out)
{
	UT_ASSERT(buf == &fixture_buf);
	if (fixture_acquires > 0)
		fixture_post_t3_snapshots++;
	*out = fixture_state;
	out->buf_state = state;
	out->page_is_new = is_new;
}

static ClusterPcmOwnResult
fixture_reserve(int id, uint64 generation, uint32 flags, uint64 *token)
{
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT_EQ(generation, 11);
	UT_ASSERT_EQ(flags, PCM_OWN_FLAG_GRANT_PENDING);
	fixture_reservations++;
	fixture_state.flags = flags;
	*token = ++fixture_state.reservation_token;
	return CLUSTER_PCM_OWN_OK;
}

static bool
fixture_pin_begin(BufferDesc *buf, const BufferTag *tag, ClusterBufmgrPcmAuxPinHandoff *handoff)
{
	UT_ASSERT(buf == &fixture_buf);
	UT_ASSERT(BufferTagsEqual(tag, &fixture_state.tag));
	UT_ASSERT_EQ(fixture_state.private_refcount, 1);
	handoff->active = true;
	fixture_state.private_refcount = 0;
	return true;
}

static bool
fixture_pin_finish(BufferDesc *buf, ClusterBufmgrPcmAuxPinHandoff *handoff)
{
	UT_ASSERT(buf == &fixture_buf);
	UT_ASSERT(handoff->active);
	handoff->active = false;
	fixture_state.private_refcount = 1;
	return true;
}

static ResourceXWriterPath
fixture_writer_path(uint64 *generation)
{
	*generation = fixture_acquires && fixture_mutation == 10 ? 7 : 6;
	return RESOURCE_X_WRITER_TARGET;
}

static ResourceXApplyResult
fixture_acquire(BufferDesc *buf, const BufferTag *tag, uint64 r4_generation, uint64 generation,
				uint64 token, ResourceXAcquisitionRef *ref)
{
	UT_ASSERT(buf == &fixture_buf);
	UT_ASSERT_EQ(fixture_state.private_refcount, 0);
	UT_ASSERT_EQ(r4_generation, 6);
	UT_ASSERT_EQ(generation, 11);
	UT_ASSERT_EQ(token, 5);
	fixture_acquires++;
	memset(ref, 0, sizeof(*ref));
	ref->formation = 2;
	ref->assertion.resource = *tag;
	fixture_state.generation = 12;
	fixture_state.flags = 0;
	fixture_state.buffer_type = (uint8)BUF_TYPE_XCUR;
	fixture_state.pcm_state = (uint8)PCM_STATE_X;
	fixture_state.page_is_new = !fixture_remote;
	if (fixture_remote)
		fixture_payload = UINT64CONST(0x123456789abcdef0);
	switch (fixture_mutation) {
	case 1:
		fixture_state.tag.blockNum++;
		break;
	case 2:
		fixture_state.generation++;
		break;
	case 3:
		fixture_state.reservation_token++;
		break;
	case 4:
		fixture_state.writer_activation_token = 5;
		break;
	case 5:
		fixture_state.resource_x_activation_generation = 1;
		break;
	case 6:
		fixture_state.flags = PCM_OWN_FLAG_GRANT_PENDING;
		break;
	case 7:
		fixture_state.buf_state |= BM_IO_ERROR;
		break;
	case 8:
		fixture_state.buffer_type = (uint8)BUF_TYPE_CURRENT;
		break;
	case 9:
		fixture_state.pcm_state = (uint8)PCM_STATE_N;
		break;
	default:
		break;
	}
	return RESOURCE_X_APPLY_APPLIED;
}

static bool
fixture_gate(ResourceXGateSnapshot *gate)
{
	memset(gate, 0, sizeof(*gate));
	gate->phase = RESOURCE_X_GATE_OPEN;
	gate->formation = 2;
	return true;
}

static bool
fixture_context(const ResourceXWriterUseContext *context)
{
	UT_ASSERT_EQ(context->ref.formation, 2);
	UT_ASSERT_EQ(context->r4_record_generation, 6);
	UT_ASSERT_EQ(context->writer_activation_token, 0);
	UT_ASSERT_EQ(context->resource_x_activation_generation, 0);
	if (fixture_context_pending > 0) {
		fixture_context_pending--;
		return false;
	}
	return fixture_context_valid;
}

static ResourceXApplyResult
pg_attribute_unused() fixture_context_result(const ResourceXWriterUseContext *context)
{
	bool pending = fixture_context_pending > 0;

	if (fixture_context(context))
		return RESOURCE_X_APPLY_APPLIED;
	return pending ? RESOURCE_X_APPLY_BAD_STATE : RESOURCE_X_APPLY_STALE;
}

static bool
pg_attribute_unused()
	fixture_observation_wait(LWLock *content, int32 buffer, uint32 wait_index, bool *barrier)
{
	UT_ASSERT(content == BufferDescriptorGetContentLock(&fixture_buf));
	UT_ASSERT_EQ(buffer, 0);
	UT_ASSERT_EQ(wait_index, 0);
	UT_ASSERT(barrier == NULL);
	UT_ASSERT(!fixture_content_held);
	UT_ASSERT_EQ(fixture_state.private_refcount, 1);
	UT_ASSERT_EQ(fixture_track, 0);
	if (fixture_wait_check != NULL)
		fixture_wait_check();
	fixture_waits++;
	if (fixture_change_on_wait)
		fixture_state.generation++;
	if (fixture_cancel_on_wait)
		siglongjmp(fixture_error, 1);
	return true;
}

static void
fixture_ordinary_lock(Buffer buffer, int mode, bool *barrier, void *deadline, bool *replaced,
					  bool *transient, ResourceXAuxiliaryAcquireContext *context)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
	UT_ASSERT(barrier == NULL && deadline == NULL && transient == NULL);
	UT_ASSERT(context == NULL);
	UT_ASSERT(!fixture_content_held);
	UT_ASSERT_EQ(fixture_track, 0);
	*replaced = false;
	fixture_ordinary++;
	fixture_content_held = true;
}

static bool
fixture_lock(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&fixture_buf));
	UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	UT_ASSERT(!fixture_content_held);
	fixture_content_held = true;
	return true;
}

static void
fixture_failure(void)
{
	siglongjmp(fixture_error, 1);
}

#define cluster_pcm_is_active() true
#define cluster_bufmgr_should_pcm_track(buf) ((buf) == &fixture_buf)
#define cluster_bufmgr_pcm_direct_init_snapshot_locked fixture_snapshot
#define LockBufHdr(buf) (fixture_state.buf_state)
#define UnlockBufHdr(buf, state) ((void)(buf), (void)(state))
#undef PageIsNew
#define PageIsNew(page) (fixture_state.page_is_new)
#define BufHdrGetBlock(buf) ((Block)NULL)
#define cluster_pcm_own_reservation_begin_exact fixture_reserve
#define cluster_bufmgr_pcm_aux_pin_handoff_begin fixture_pin_begin
#define cluster_bufmgr_pcm_aux_pin_handoff_finish_exact fixture_pin_finish
#define cluster_resource_x_writer_path_snapshot fixture_writer_path
#define cluster_gcs_resource_x_target_direct_init_acquire_exact fixture_acquire
#define cluster_pcm_lock_resource_x_gate_snapshot fixture_gate
#define cluster_gcs_resource_x_target_context_recheck_exact fixture_context
#define cluster_gcs_resource_x_target_context_recheck_result_exact fixture_context_result
#define cluster_bufmgr_resource_x_wait_retry fixture_observation_wait
#define cluster_bufmgr_pcm_direct_init_report_failure(...) fixture_failure()
#define cluster_bufmgr_resource_x_writer_report_failure(...) fixture_failure()
#define cluster_pcm_own_abort_grant_after_error(...) fixture_failure()
#define cluster_bufmgr_resource_x_fail_closed_current() (fixture_fuse++)
#define cluster_bufmgr_resource_x_fail_closed_exact(gate) ((void)(gate), fixture_fuse++)
#define cluster_bufmgr_pcm_join_aux_direct_init_exact(...) (fixture_failure(), false)
#define cluster_bufmgr_pcm_x_writer_track_target_direct_init(buf, context)                         \
	((void)(buf), (void)(context), fixture_track++)
#define cluster_bufmgr_pcm_x_writer_activate_target_direct_init(buf)                               \
	((void)(buf), fixture_activate++)
#define BufferIsPinned(buffer) ((buffer) == 1 && fixture_state.private_refcount > 0)
#define GetBufferDescriptor(id) (&fixture_buf)
#define LockBufferInternal fixture_ordinary_lock
#define LockBuffer(buffer, mode) fixture_failure()
#define LWLockAcquire fixture_lock
#undef elog
#define elog(level, ...) UT_ASSERT_EQ(level, DEBUG1)

#include "test_cluster_pcm_aux_consumer.inc"

/* The upper-consumer cases above keep their ledger dependencies controlled.
 * These additional cases execute the real ledger find/bind/activate bodies. */
static ClusterPcmOwnResult
fixture_own_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	UT_ASSERT(buf == &fixture_buf);
	memset(out, 0, sizeof(*out));
	out->tag = fixture_state.tag;
	out->generation = fixture_state.generation;
	out->pcm_state = fixture_state.pcm_state;
	out->flags = fixture_state.flags;
	out->writer_activation_token = fixture_state.writer_activation_token;
	out->resource_x_activation_generation = fixture_state.resource_x_activation_generation;
	return CLUSTER_PCM_OWN_OK;
}

static void
pg_attribute_unused() fixture_unlock(LWLock *lock)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&fixture_buf));
	UT_ASSERT(fixture_content_held);
	fixture_content_held = false;
}

#undef cluster_bufmgr_pcm_x_writer_track_target_direct_init
#undef cluster_bufmgr_pcm_x_writer_activate_target_direct_init
#define cluster_bufmgr_pcm_own_snapshot fixture_own_snapshot
#define cluster_node_id 0
#define LWLockHeldByMe(lock) ((void)(lock), fixture_content_held)
#define LWLockRelease fixture_unlock
#define cluster_pcm_lock_resource_x_trace_ref(...) (fixture_activate++)
#undef ereport
#define ereport(...) fixture_failure()
#include "test_cluster_pcm_aux_ledger.inc"

static void
fixture_reset(ClusterPcmDirectInitKind kind, bool remote)
{
	memset(&fixture_buf, 0, sizeof(fixture_buf));
	memset(&fixture_state, 0, sizeof(fixture_state));
	fixture_state.tag.spcOid = 1663;
	fixture_state.tag.dbOid = 5;
	fixture_state.tag.relNumber = 16386;
	fixture_state.tag.forkNum
		= kind == CLUSTER_PCM_DIRECT_INIT_VM ? VISIBILITYMAP_FORKNUM : FSM_FORKNUM;
	fixture_buf.tag = fixture_state.tag;
	fixture_state.generation = 11;
	fixture_state.reservation_token = 4;
	fixture_state.buf_state = BM_TAG_VALID | BM_VALID | 1;
	fixture_state.private_refcount = 1;
	fixture_state.buffer_type = (uint8)BUF_TYPE_CURRENT;
	fixture_state.pcm_state = (uint8)PCM_STATE_N;
	fixture_state.page_is_new = true;
	fixture_remote = remote;
	fixture_context_valid = true;
	fixture_content_held = false;
	fixture_mutation = fixture_ordinary = fixture_track = fixture_activate = 0;
	fixture_fuse = fixture_acquires = fixture_reservations = 0;
	fixture_payload = 0;
	fixture_context_pending = fixture_waits = fixture_post_t3_snapshots = 0;
	fixture_change_on_wait = fixture_cancel_on_wait = false;
	fixture_wait_check = NULL;
}

UT_TEST(test_initialized_remote_aux_uses_ordinary_upper_consumer)
{
	for (int kind = CLUSTER_PCM_DIRECT_INIT_VM; kind <= CLUSTER_PCM_DIRECT_INIT_FSM; kind++) {
		fixture_reset((ClusterPcmDirectInitKind)kind, true);
		if (sigsetjmp(fixture_error, 0) != 0) {
			UT_ASSERT(!"initialized remote image rejected by upper consumer");
			continue;
		}
		UT_ASSERT_EQ(LockBufferForAuxiliaryPageInit(1, (ClusterPcmDirectInitKind)kind), 1);
		UT_ASSERT_EQ(fixture_ordinary, 1);
		UT_ASSERT_EQ(fixture_track, 0);
		UT_ASSERT_EQ(fixture_activate, 0);
		UT_ASSERT_EQ(fixture_fuse, 0);
		UT_ASSERT_EQ(fixture_acquires, 1);
		UT_ASSERT_EQ(fixture_reservations, 1);
		UT_ASSERT_EQ(fixture_state.private_refcount, 1);
		UT_ASSERT(fixture_content_held);
		UT_ASSERT_EQ(fixture_payload, UINT64CONST(0x123456789abcdef0));
	}
}

UT_TEST(test_known_new_aux_keeps_initialization_proof)
{
	fixture_reset(CLUSTER_PCM_DIRECT_INIT_VM, false);
	if (sigsetjmp(fixture_error, 0) != 0) {
		UT_ASSERT(!"valid known-new consumer rejected");
		return;
	}
	UT_ASSERT_EQ(LockBufferForAuxiliaryPageInit(1, CLUSTER_PCM_DIRECT_INIT_VM), 1);
	UT_ASSERT_EQ(fixture_ordinary, 0);
	UT_ASSERT_EQ(fixture_track, 1);
	UT_ASSERT_EQ(fixture_activate, 1);
	UT_ASSERT_EQ(fixture_fuse, 0);
	UT_ASSERT(fixture_content_held);
	UT_ASSERT_EQ(fixture_state.private_refcount, 1);
}

UT_TEST(test_initialized_remote_aux_still_rejects_stale_context)
{
	fixture_reset(CLUSTER_PCM_DIRECT_INIT_VM, true);
	fixture_context_valid = false;
	if (sigsetjmp(fixture_error, 0) == 0) {
		(void)LockBufferForAuxiliaryPageInit(1, CLUSTER_PCM_DIRECT_INIT_VM);
		UT_ASSERT(!"stale terminal context admitted");
	}
	UT_ASSERT_EQ(fixture_ordinary, 0);
	UT_ASSERT_EQ(fixture_track, 0);
	UT_ASSERT_EQ(fixture_fuse, 1);
	UT_ASSERT(!fixture_content_held);
}

UT_TEST(test_initialized_remote_aux_rejects_each_changed_identity)
{
	for (int mutation = 1; mutation <= 10; mutation++) {
		fixture_reset(CLUSTER_PCM_DIRECT_INIT_VM, true);
		fixture_mutation = mutation;
		if (sigsetjmp(fixture_error, 0) == 0) {
			(void)LockBufferForAuxiliaryPageInit(1, CLUSTER_PCM_DIRECT_INIT_VM);
			UT_ASSERT(!"changed ownership admitted as initialized remote image");
		}
		UT_ASSERT_EQ(fixture_ordinary, 0);
		UT_ASSERT_EQ(fixture_track, 0);
		UT_ASSERT_EQ(fixture_fuse, 1);
		UT_ASSERT(!fixture_content_held);
	}
}

UT_TEST(test_actual_aux_post_t3_wait_resamples_proof_without_reinitializing_image)
{
	for (int remote = 0; remote <= 1; remote++) {
		fixture_reset(CLUSTER_PCM_DIRECT_INIT_VM, remote != 0);
		fixture_context_pending = 3;
		if (sigsetjmp(fixture_error, 0) != 0) {
			UT_ASSERT(!"temporary post-T3 observation was made a hard failure");
			continue;
		}
		UT_ASSERT_EQ(LockBufferForAuxiliaryPageInit(1, CLUSTER_PCM_DIRECT_INIT_VM), 1);
		UT_ASSERT_EQ(fixture_waits, 3);
		/* Installed remote bytes re-enter the original CACHED_X arm once. */
		UT_ASSERT_EQ(fixture_post_t3_snapshots, remote ? 5 : 4);
		UT_ASSERT_EQ(fixture_fuse, 0);
		UT_ASSERT_EQ(fixture_acquires, 1);
		UT_ASSERT_EQ(fixture_reservations, 1);
		UT_ASSERT_EQ(fixture_ordinary, remote ? 1 : 0);
		UT_ASSERT_EQ(fixture_track, remote ? 0 : 1);
		UT_ASSERT_EQ(fixture_activate, remote ? 0 : 1);
		UT_ASSERT_EQ(fixture_payload, remote ? UINT64CONST(0x123456789abcdef0) : 0);
	}
}

UT_TEST(test_actual_aux_post_t3_wait_rejects_later_drift_and_cancellation)
{
	for (int cancel = 0; cancel <= 1; cancel++) {
		fixture_reset(CLUSTER_PCM_DIRECT_INIT_VM, false);
		fixture_context_pending = 1;
		fixture_change_on_wait = cancel == 0;
		fixture_cancel_on_wait = cancel == 1;
		if (sigsetjmp(fixture_error, 0) == 0) {
			(void)LockBufferForAuxiliaryPageInit(1, CLUSTER_PCM_DIRECT_INIT_VM);
			UT_ASSERT(!"post-wait stale proof or caller cancellation admitted");
		}
		UT_ASSERT_EQ(fixture_waits, 1);
		UT_ASSERT_EQ(fixture_post_t3_snapshots, cancel ? 1 : 2);
		UT_ASSERT_EQ(fixture_fuse, cancel ? 0 : 1);
		UT_ASSERT_EQ(fixture_track, 0);
		UT_ASSERT_EQ(fixture_activate, 0);
		UT_ASSERT_EQ(fixture_ordinary, 0);
		UT_ASSERT(!fixture_content_held);
	}
}

static ResourceXWriterUseContext fixture_ledger_context;
static bool fixture_expect_bound;

static void
fixture_check_pending_ledger(void)
{
	ClusterPcmXWriterLedgerEntry *entry = cluster_bufmgr_pcm_x_writer_find(&fixture_buf);

	if (!fixture_expect_bound) {
		UT_ASSERT(entry == NULL);
		return;
	}
	UT_ASSERT_NOT_NULL(entry);
	if (entry != NULL) {
		UT_ASSERT_EQ(entry->phase, PCM_X_WRITER_LEDGER_ACQUIRING);
		UT_ASSERT_EQ(
			memcmp(&entry->authority, &fixture_ledger_context, sizeof(fixture_ledger_context)), 0);
		UT_ASSERT_EQ(entry->granted.generation, 12);
	}
	UT_ASSERT_EQ(fixture_activate, 0);
}

static void
fixture_ledger_reset(void)
{
	fixture_reset(CLUSTER_PCM_DIRECT_INIT_VM, false);
	memset(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	memset(&fixture_ledger_context, 0, sizeof(fixture_ledger_context));
	fixture_state.generation = 12;
	fixture_state.pcm_state = (uint8)PCM_STATE_X;
	fixture_ledger_context.ref.assertion.resource = fixture_buf.tag;
	fixture_ledger_context.ref.assertion.requester_node = 0;
	fixture_ledger_context.ref.formation = 2;
	fixture_ledger_context.ref.acquisition_generation = 41;
	fixture_ledger_context.r4_record_generation = 6;
	fixture_ledger_context.buffer_ownership_generation = 12;
	fixture_wait_check = fixture_check_pending_ledger;
	fixture_expect_bound = false;
}

UT_TEST(test_actual_direct_init_ledger_waits_before_binding_and_activation)
{
	fixture_ledger_reset();
	fixture_context_pending = 3;
	if (sigsetjmp(fixture_error, 0) != 0) {
		UT_ASSERT(!"direct-init observation gap became a ledger/activation failure");
		return;
	}
	cluster_bufmgr_pcm_x_writer_track_target_direct_init(&fixture_buf, &fixture_ledger_context);
	UT_ASSERT_EQ(fixture_waits, 3);
	UT_ASSERT_EQ(fixture_fuse, 0);
	fixture_expect_bound = true;
	fixture_context_pending = 3;
	fixture_content_held = true;
	cluster_bufmgr_pcm_x_writer_activate_target_direct_init(&fixture_buf);
	UT_ASSERT_EQ(fixture_waits, 6);
	UT_ASSERT_EQ(fixture_fuse, 0);
	UT_ASSERT_EQ(fixture_activate, 1);
	UT_ASSERT(fixture_content_held);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_x_writer_find(&fixture_buf)->phase, PCM_X_WRITER_LEDGER_ACTIVE);
	UT_ASSERT_EQ(fixture_state.private_refcount, 1);
	UT_ASSERT_EQ(fixture_payload, 0);
}

UT_TEST(test_actual_direct_init_ledger_wait_rechecks_physical_drift_and_cancel)
{
	for (int activate = 0; activate <= 1; activate++) {
		for (int cancel = 0; cancel <= 1; cancel++) {
			fixture_ledger_reset();
			if (activate) {
				cluster_bufmgr_pcm_x_writer_track_target_direct_init(&fixture_buf,
																	 &fixture_ledger_context);
				fixture_expect_bound = true;
				fixture_content_held = true;
			}
			fixture_context_pending = 1;
			fixture_change_on_wait = cancel == 0;
			fixture_cancel_on_wait = cancel == 1;
			if (sigsetjmp(fixture_error, 0) == 0) {
				if (activate)
					cluster_bufmgr_pcm_x_writer_activate_target_direct_init(&fixture_buf);
				else
					cluster_bufmgr_pcm_x_writer_track_target_direct_init(&fixture_buf,
																		 &fixture_ledger_context);
				UT_ASSERT(!"ledger admitted drift/cancel after pending observation");
			}
			UT_ASSERT_EQ(fixture_waits, 1);
			UT_ASSERT_EQ(fixture_fuse, cancel ? 0 : 1);
			UT_ASSERT_EQ(fixture_activate, 0);
			UT_ASSERT_EQ(fixture_state.private_refcount, 1);
			UT_ASSERT_EQ(fixture_payload, 0);
			UT_ASSERT(fixture_content_held == (activate && !cancel));
			if (activate)
				UT_ASSERT_EQ(cluster_bufmgr_pcm_x_writer_find(&fixture_buf)->phase,
							 PCM_X_WRITER_LEDGER_ACQUIRING);
			else
				UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&fixture_buf) == NULL);
		}
	}
}

UT_TEST(test_actual_ordinary_preuse_still_requalifies_without_waiting_under_lock)
{
	ClusterPcmXWriterLedgerEntry *entry;

	fixture_ledger_reset();
	cluster_bufmgr_pcm_x_writer_track_target_direct_init(&fixture_buf, &fixture_ledger_context);
	entry = cluster_bufmgr_pcm_x_writer_find(&fixture_buf);
	fixture_context_pending = 1;
	fixture_content_held = true;
	if (sigsetjmp(fixture_error, 0) != 0) {
		UT_ASSERT(!"ordinary pre-use failed instead of requalifying");
		return;
	}
	UT_ASSERT(!cluster_bufmgr_pcm_x_writer_activate(entry, true));
	UT_ASSERT(cluster_bufmgr_pcm_x_writer_find(&fixture_buf) == NULL);
	UT_ASSERT_EQ(fixture_waits, 0);
	UT_ASSERT_EQ(fixture_fuse, 0);
	UT_ASSERT(fixture_content_held); /* The existing caller releases this lock. */
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_initialized_remote_aux_uses_ordinary_upper_consumer);
	UT_RUN(test_known_new_aux_keeps_initialization_proof);
	UT_RUN(test_initialized_remote_aux_still_rejects_stale_context);
	UT_RUN(test_initialized_remote_aux_rejects_each_changed_identity);
	UT_RUN(test_actual_aux_post_t3_wait_resamples_proof_without_reinitializing_image);
	UT_RUN(test_actual_aux_post_t3_wait_rejects_later_drift_and_cancellation);
	UT_RUN(test_actual_direct_init_ledger_waits_before_binding_and_activation);
	UT_RUN(test_actual_direct_init_ledger_wait_rechecks_physical_drift_and_cancel);
	UT_RUN(test_actual_ordinary_preuse_still_requalifies_without_waiting_under_lock);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
