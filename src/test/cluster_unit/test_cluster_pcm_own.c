/*-------------------------------------------------------------------------
 *
 * test_cluster_pcm_own.c
 *	  C1 ownership-reservation and D5a buffer-reuse contract tests.
 *
 * This binary links the production cluster_pcm_own object.  Buffer-manager
 * behavior that cannot be linked standalone is covered in two paired ways:
 * the real reusable decision helpers are exercised here, and the production
 * bufmgr source is checked to prove both eviction paths call those helpers
 * before dropping header authority and use the saved-tag release API.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_semantic_activation.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "utils/memdebug.h"

#include "unit_test.h"

/* Generated from the production bufmgr function bodies, never a test rewrite. */
#include "test_cluster_pcm_snapshot_owner.inc"
#include "test_cluster_pcm_checksum_owner.inc"

#include <errno.h>
#include <limits.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

UT_DEFINE_GLOBALS();

int NBuffers = 4;

static union {
	uint64 align;
	char bytes[4096];
} fake_shmem;
static bool fake_found;

/* Compile the real source-copy and retained-finish owners.  Only their
 * process/physical-I/O dependencies are single-descriptor fixtures; the
 * ownership sidecar, image predicates, commit, PG_TRY and returned result
 * remain production code.  Disk I/O is not part of this unit's proof. */
static BufferDesc *transition_buf;
static PGIOAlignedBlock transition_page;
static LWLock transition_mapping_lock;
static bool transition_mapping_held;
static bool transition_content_held;
static bool transition_content_busy;
static int transition_pin_count;
static int transition_base_pins;
static int transition_flush_count;
static bool transition_flush_error;
static bool transition_flush_leaves_dirty;
static bool transition_copy_active;
static bool transition_wal_error;
static int transition_wal_calls;
static int transition_wal_changes;
#ifdef USE_CLUSTER_UNIT
void (*cluster_gcs_block_test_xlog_flush_hook)(uint64 page_lsn) = NULL;
int (*cluster_gcs_block_test_lsn_drift_hook)(void) = NULL;
#endif
static bool cluster_pcm_x_finish_retain_flush_active;
static bool cluster_pcm_x_finish_retain_flush_io_active;
static bool cluster_pcm_x_finish_retain_flush_error_context_pushed;
static ErrorContextCallback *cluster_pcm_x_finish_retain_flush_error_context_previous;
static ResourceXDeliveryTarget route_target;
static uint64 route_direct_generation, route_direct_token;
static int route_observations, route_binds;
static int route_drift;
static int n_predecessor_header_step;
static int read_reclaim_drift;
static bool read_reclaim_error;
static ResourceXApplyResult route_bind_result = RESOURCE_X_APPLY_APPLIED;

/* Only process-local ledger presence is a fixture. The exact production
 * refusal proof, snapshots and reservation abort below are not rewritten. */
typedef struct ClusterPcmXWriterLedgerEntry ClusterPcmXWriterLedgerEntry;
static bool barrier_local_writer;

/* Explicit entry observations; the dispatch, both physical lookup functions,
 * header predicates and actual ownership hold below remain production C. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_delivery_dispatch_observe_exact(
	const ResourceXDecodedFrame *dispatch, int32 master_node,
	ResourceXInstallClaimJoinObservation *out, ResourceXDeliveryTarget *target,
	uint64 *direct_generation_out, uint64 *direct_token_out)
{
	memset(out, 0, sizeof(*out));
	out->request = dispatch->common;
	out->entry_binding_generation = 4;
	out->r4_record_generation = 77;
	out->master_node = master_node;
	out->master_ingress_connection_generation = 61;
	*target = route_target;
	*direct_generation_out = route_direct_generation;
	*direct_token_out = route_direct_token;
	route_observations++;
	if (route_observations > 1) {
		if (route_drift == 1)
			out->entry_binding_generation++;
		else if (route_drift == 2)
			(*direct_token_out)++;
		else if (route_drift == 3)
			target->buffer_id_plus_one = 1;
		else if (route_drift == 4)
			out->request.sender_connection_generation++;
		else if (route_drift == 5)
			out->r4_record_generation++;
	}
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_delivery_bind_target_exact(
	const ResourceXInstallClaimJoinObservation *observation, int buffer_id,
	const ClusterPcmOwnSnapshot *before, const ClusterPcmOwnSnapshot *after)
{
	UT_ASSERT_EQ(buffer_id, transition_buf->buf_id);
	UT_ASSERT_EQ(observation->request.assertion_sequence, UINT64_C(41));
	UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(before, after));
	route_binds++;
	return route_bind_result;
}

sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

static uint32
transition_lock_header(BufferDesc *buf)
{
	uint32 state = pg_atomic_read_u32(&buf->state);

	if (read_reclaim_error && transition_content_held
		&& transition_pin_count > transition_base_pins) {
		read_reclaim_error = false;
		pg_re_throw();
	}

	/* A concurrent physical I/O interval changes no ownership identity.
	 * The third sample returns to the exact original physical projection. */
	if (n_predecessor_header_step != 0) {
		if (n_predecessor_header_step <= 2)
			state |= BM_IO_IN_PROGRESS;
		else
			state &= ~BM_IO_IN_PROGRESS;
		n_predecessor_header_step++;
	}
	UT_ASSERT((state & BM_LOCKED) == 0);
	pg_atomic_write_u32(&buf->state, state | BM_LOCKED);
	return state | BM_LOCKED;
}

static bool
transition_lock_acquire(LWLock *lock, LWLockMode mode)
{
	if (lock == &transition_mapping_lock) {
		UT_ASSERT(mode == LW_SHARED
				  || (mode == LW_EXCLUSIVE
					  && cluster_pcm_x_revoke_finish_mode(&transition_buf->tag, 0)
							 == CLUSTER_PCM_X_REVOKE_FINISH_DROP));
		UT_ASSERT(!transition_mapping_held);
		transition_mapping_held = true;
	} else {
		UT_ASSERT(lock == BufferDescriptorGetContentLock(transition_buf));
		UT_ASSERT_EQ(mode, transition_copy_active ? LW_SHARED : LW_EXCLUSIVE);
		if (transition_content_busy)
			return false;
		UT_ASSERT(!transition_content_held);
		transition_content_held = true;
		if (read_reclaim_drift == 1)
			pg_atomic_fetch_add_u64(&ClusterPcmOwnArray[transition_buf->buf_id].generation, 1);
		else if (read_reclaim_drift == 2)
			pg_atomic_fetch_or_u32(&transition_buf->state, BM_IO_IN_PROGRESS);
		read_reclaim_drift = 0;
	}
	return true;
}

static void
transition_lock_release(LWLock *lock)
{
	if (lock == &transition_mapping_lock) {
		UT_ASSERT(transition_mapping_held);
		transition_mapping_held = false;
	} else {
		UT_ASSERT(lock == BufferDescriptorGetContentLock(transition_buf));
		UT_ASSERT(transition_content_held);
		transition_content_held = false;
	}
}

static void
transition_pin_locked(BufferDesc *buf, uint32 state)
{
	UT_ASSERT(transition_mapping_held);
	UT_ASSERT_EQ(transition_pin_count, transition_base_pins);
	transition_pin_count++;
	UnlockBufHdr(buf, state + BUF_REFCOUNT_ONE);
}

static void
transition_unpin(BufferDesc *buf)
{
	if (transition_base_pins == 1 && transition_pin_count == 1)
		transition_base_pins = 0;
	UT_ASSERT_EQ(transition_pin_count, transition_base_pins + 1);
	UT_ASSERT(!transition_content_held);
	transition_pin_count--;
	pg_atomic_fetch_sub_u32(&buf->state, BUF_REFCOUNT_ONE);
}

static void
transition_flush(BufferDesc *buf)
{
	uint32 state = pg_atomic_read_u32(&buf->state);

	UT_ASSERT(transition_content_held);
	UT_ASSERT_EQ(transition_pin_count, transition_base_pins + 1);
	UT_ASSERT(transition_copy_active || cluster_pcm_x_finish_retain_flush_active);
	UT_ASSERT((state & BM_LOCKED) == 0);
	transition_flush_count++;
	if (transition_flush_error) {
		cluster_pcm_x_finish_retain_flush_io_active = true;
		pg_atomic_write_u32(&buf->state, state | BM_IO_IN_PROGRESS);
		pg_re_throw();
	}
	if (!transition_flush_leaves_dirty)
		pg_atomic_write_u32(&buf->state,
							state & ~(BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED));
}

static void
transition_abort_io(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, BufferDescriptorGetBuffer(transition_buf));
	pg_atomic_fetch_and_u32(&transition_buf->state, ~BM_IO_IN_PROGRESS);
	pg_atomic_fetch_or_u32(&transition_buf->state, BM_IO_ERROR);
}

/* Simulate only a concurrent page writer during the real owner's unlocked
 * WAL interval. Sampling, current-image validation and cleanup stay real. */
static void
transition_wal_flush(XLogRecPtr lsn)
{
	Page page = (Page)transition_page.data;

	UT_ASSERT(transition_copy_active);
	UT_ASSERT(!transition_content_held && !transition_mapping_held);
	UT_ASSERT_EQ(transition_pin_count, 1);
	UT_ASSERT_EQ(lsn, PageGetLSN(page));
	transition_wal_calls++;
	if (transition_wal_error)
		pg_re_throw();
	if (transition_wal_changes > 0) {
		transition_wal_changes--;
		PageSetLSNPreserveOrigin(page, lsn + UINT64_C(8));
		pg_atomic_fetch_or_u32(&transition_buf->state, BM_DIRTY);
	}
}

/* The real DROP owner has already checked and committed N. Only the mapping
 * table/free-list tail is a single-descriptor fixture; it must see zero pins. */
static void
transition_drop_tail(BufferDesc *buf, uint32 state)
{
	UT_ASSERT(transition_mapping_held);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(state), 0);
	UT_ASSERT_EQ(buf->pcm_state, PCM_STATE_N);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(buf->buf_id), 0);
	UnlockBufHdr(buf, state & ~(BM_TAG_VALID | BM_VALID));
	transition_lock_release(&transition_mapping_lock);
}

#define LockBufHdr transition_lock_header
#define BufHdrGetBlock(buf) ((Block)transition_page.data)
#define BufTableHashCode(tag) (0U)
#define BufMappingPartitionLock(hash) ((void)(hash), &transition_mapping_lock)
#define BufTableLookup(lookup_tag, hash)                                                           \
	(BufferTagsEqual((lookup_tag), &transition_buf->tag) ? transition_buf->buf_id : -1)
#define GetBufferDescriptor(id) ((void)(id), transition_buf)
#define LWLockAcquire transition_lock_acquire
#define LWLockConditionalAcquire transition_lock_acquire
#define LWLockRelease transition_lock_release
#define LWLockHeldByMe(lock) ((void)(lock), transition_content_held)
#define LWLockHeldByMeInMode(lock, mode)                                                           \
	((void)(lock), transition_content_held && (mode) == LW_EXCLUSIVE)
#define cluster_bufmgr_pin_for_gcs_locked transition_pin_locked
#define cluster_bufmgr_unpin_for_gcs transition_unpin
#define FlushBuffer(buf, rel, object, context) transition_flush(buf)
#define XLogFlush(lsn) transition_wal_flush(lsn)
#define cluster_gcs_clamp_ship_flush_lsn(lsn) (lsn)
#define AbortBufferIO transition_abort_io
#define InvalidateBufferCommitTailLocked(buf, tag, hash, lock, state, pcm, io)                     \
	transition_drop_tail(buf, state)
/* Injection selector/ERROR logging retain their existing t/406 coverage. */
#ifdef ENABLE_INJECTION
#define TRANSITION_RESTORE_INJECTION
#undef ENABLE_INJECTION
#endif
#undef elog
#define elog(...) ((void)0)
#undef HOLD_INTERRUPTS
#define HOLD_INTERRUPTS() ((void)0)
#include "test_cluster_pcm_transition_owner.inc"
#define cluster_bufmgr_pcm_x_writer_find(buf)                                                      \
	(barrier_local_writer ? (ClusterPcmXWriterLedgerEntry *)(buf) : NULL)
#include "test_cluster_pcm_barrier_proof.inc"
#undef cluster_bufmgr_pcm_x_writer_find
#include "test_cluster_pcm_delivery_route_owner.inc"
#undef HOLD_INTERRUPTS
#undef elog
#ifdef TRANSITION_RESTORE_INJECTION
#define ENABLE_INJECTION 1
#undef TRANSITION_RESTORE_INJECTION
#endif
#undef InvalidateBufferCommitTailLocked
#undef AbortBufferIO
#undef FlushBuffer
#undef cluster_gcs_clamp_ship_flush_lsn
#undef XLogFlush
#undef cluster_bufmgr_unpin_for_gcs
#undef cluster_bufmgr_pin_for_gcs_locked
#undef LWLockHeldByMeInMode
#undef LWLockHeldByMe
#undef LWLockRelease
#undef LWLockConditionalAcquire
#undef LWLockAcquire
#undef BufTableLookup
#undef GetBufferDescriptor
#undef BufMappingPartitionLock
#undef BufTableHashCode
#undef BufHdrGetBlock
#undef LockBufHdr

/* Complete production post-retention finish/PG_TRY/defer/publish consumer.
 * Physical owners are real; entry/publish effects below check call ordering.
 * Their exact shared-state transitions run separately in the PCM unit. */
static int source_finish_fuses;
static int source_finish_owner_releases;
static int source_finish_defer_calls;
static ResourceXApplyResult source_finish_defer_result = RESOURCE_X_APPLY_APPLIED;
static ResourceXApplyResult source_finish_pair_result = RESOURCE_X_APPLY_NOT_FOUND;
static int source_finish_publishes;
static ErrorData source_finish_error = { .message = "fixture flush failure" };

static ResourceXApplyResult
source_finish_publish(void)
{
	UT_ASSERT_EQ(transition_buf->pcm_state, PCM_STATE_N);
	UT_ASSERT_EQ(transition_pin_count, 0);
	UT_ASSERT(!transition_mapping_held && !transition_content_held);
	source_finish_publishes++;
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_source_finish_defer_exact(const ResourceXDecodedFrame *block,
													  int32 master_node,
													  const ClusterPcmOwnSnapshot *revoking,
													  const ResourceXLocalOwnerHandle *owner,
													  int buffer_id)
{
	UT_ASSERT(!transition_mapping_held && !transition_content_held);
	UT_ASSERT_EQ(transition_pin_count, 1);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&transition_buf->state)), 1);
	UT_ASSERT_EQ(master_node, 0);
	UT_ASSERT_EQ(buffer_id, transition_buf->buf_id);
	UT_ASSERT(BufferTagsEqual(&block->common.logical_assertion.resource, &revoking->tag));
	UT_ASSERT_EQ(owner->buffer_ownership_generation, revoking->generation);
	source_finish_defer_calls++;
	return source_finish_defer_result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_pair_publish_needed_exact(
	const ResourceXAssertion *assertion pg_attribute_unused(),
	uint64 sequence pg_attribute_unused(), int32 master_node pg_attribute_unused(),
	uint64 session pg_attribute_unused())
{
	/* A selected-S pair is not provided by this X-owner fixture. */
	return source_finish_pair_result;
}

static bool
source_finish_release(ResourceXLocalOwnerHandle *owner pg_attribute_unused(), bool *held)
{
	if (held == NULL || !*held)
		return true;
	source_finish_owner_releases++;
	*held = false;
	return true;
}

#undef ereport
#define ereport(...) ((void)0)
#define CurrentMemoryContext ((MemoryContext)0)
#define MemoryContextSwitchTo(context) ((void)(context))
#define CopyErrorData() (&source_finish_error)
#define FreeErrorData(error) ((void)(error))
#define FlushErrorState() ((void)0)
#define cluster_pcm_lock_resource_x_holder_status_exact(assertion, output)                         \
	((void)(assertion), memset(output, 0, sizeof(*(output))), RESOURCE_X_APPLY_NOT_FOUND)
#define cluster_pcm_lock_resource_x_holder_image_exact(assertion, output)                          \
	((void)(assertion), memset(output, 0, sizeof(*(output))), RESOURCE_X_APPLY_NOT_FOUND)
#define cluster_pcm_lock_resource_x_terminal_x_revoke_finish_drop_exact(...)                       \
	RESOURCE_X_APPLY_INVALID
#define cluster_pcm_lock_resource_x_holder_pair_publish_exact(...) source_finish_publish()
#define gcs_block_resource_x_fail_closed_current() (source_finish_fuses++)
#define gcs_block_resource_x_terminal_owner_release source_finish_release
#include "test_cluster_pcm_source_finish_consumer.inc"
#undef gcs_block_resource_x_terminal_owner_release
#undef gcs_block_resource_x_fail_closed_current
#undef cluster_pcm_lock_resource_x_holder_pair_publish_exact
#undef cluster_pcm_lock_resource_x_terminal_x_revoke_finish_drop_exact
#undef cluster_pcm_lock_resource_x_holder_image_exact
#undef cluster_pcm_lock_resource_x_holder_status_exact
#undef FlushErrorState
#undef FreeErrorData
#undef CopyErrorData
#undef MemoryContextSwitchTo
#undef CurrentMemoryContext
#undef ereport

static ResourceXApplyResult
source_finish_consume(ClusterPcmOwnHeldXRevoke *input, bool held_source)
{
	ResourceXLocalOwnerHandle target_revoke_owner = { 0 };
	ResourceXDecodedFrame frame = { 0 };

	frame.body.image_envelope.source_carrier_generation = input->revoking.generation + 1;
	frame.common.logical_assertion.resource = input->revoking.tag;
	PageSetLSNPreserveOrigin((Page)frame.body.image_envelope.page_bytes, UINT64_C(0x12340));
	target_revoke_owner.buffer_ownership_generation = input->revoking.generation;
	return gcs_block_resource_x_source_finish_owned(
		&frame, 0, 77, transition_buf, input->revoking, &frame, *input, target_revoke_owner,
		held_source, held_source, held_source, false, RESOURCE_X_APPLY_APPLIED);
}

/* Actual background callback with controlled membership/session observations.
 * Its winning continuation runs the real physical/pin/finish consumer above;
 * only the entry claim transfer is provided by this single-owner fixture. */
static PcmXSessionAuthResult source_tick_sample;
static ClusterPcmOwnHeldXRevoke *source_tick_held;
static int source_tick_runs, source_tick_leaves, source_tick_notifications;
static ResourceXApplyResult source_tick_peer_result = RESOURCE_X_APPLY_APPLIED;

static ClusterSemanticAdmissionResult
source_tick_enter(uint64 feature, ClusterSemanticAdmissionSide side,
				  ClusterSemanticAdmissionToken *admission)
{
	admission->entered = true;
	admission->feature_bit = feature;
	admission->side = side;
	admission->record_generation = 77;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

static PcmXSessionAuthResult
source_tick_snapshot(const BufferTag *tag, ResourceXGateSnapshot *gate, int32 *master,
					 uint64 *session)
{
	UT_ASSERT(BufferTagsEqual(tag, &source_tick_held->revoking.tag));
	memset(gate, 0, sizeof(*gate));
	gate->formation = 17;
	*master = 0;
	*session = source_tick_sample == PCM_X_SESSION_AUTH_OK ? 31 : 0;
	return source_tick_sample;
}

static ResourceXApplyResult
source_tick_run(const ResourceXAcquisitionRef *ref, const ClusterSemanticAdmissionToken *admission,
				int32 master, uint64 session)
{
	UT_ASSERT(BufferTagsEqual(&ref->assertion.resource, &source_tick_held->revoking.tag));
	UT_ASSERT_EQ(master, 0);
	UT_ASSERT_EQ(session, UINT64_C(31));
	UT_ASSERT_EQ(admission->record_generation, UINT64_C(77));
	source_tick_runs++;
	return source_finish_consume(source_tick_held, true);
}

#define MyBackendType B_LMS
#define cluster_semantic_activation_enter source_tick_enter
#define cluster_semantic_activation_recheck(token) ((token)->entered)
#define cluster_semantic_activation_leave(token) ((token)->entered = false, source_tick_leaves++)
#define gcs_block_resource_x_gate_session_snapshot(tag, gate, master, session)                     \
	(source_tick_snapshot(tag, gate, master, session) == PCM_X_SESSION_AUTH_OK)
#define gcs_block_resource_x_gate_session_snapshot_result source_tick_snapshot
#define gcs_block_pcm_x_resource_x_peer_ready_exact(node, generation)                              \
	((void)(node), *(generation) = 61, true)
#define gcs_block_resource_x_target_peer_matches_exact(token, node, generation)                    \
	((token)->entered && (node) == 0 && (generation) == 61                                         \
	 && source_tick_peer_result == RESOURCE_X_APPLY_APPLIED)
#define gcs_block_resource_x_target_peer_result_exact(token, node, generation)                     \
	((token)->entered && (node) == 0 && (generation) == 61 ? source_tick_peer_result               \
														   : RESOURCE_X_APPLY_STALE)
#define gcs_block_resource_x_source_finish_run_exact source_tick_run
#define gcs_block_resource_x_stage_ready_tag(tag) ((void)(tag), source_tick_notifications++)
#include "test_cluster_pcm_source_finish_tick.inc"
#undef gcs_block_resource_x_stage_ready_tag
#undef gcs_block_resource_x_source_finish_run_exact
#undef gcs_block_resource_x_target_peer_matches_exact
#undef gcs_block_resource_x_target_peer_result_exact
#undef gcs_block_pcm_x_resource_x_peer_ready_exact
#undef gcs_block_resource_x_gate_session_snapshot_result
#undef gcs_block_resource_x_gate_session_snapshot
#undef cluster_semantic_activation_leave
#undef cluster_semantic_activation_recheck
#undef cluster_semantic_activation_enter
#undef MyBackendType

/* Entry/clock/logging dependencies for the actual ordinary pre-assert
 * consumer. Physical snapshots, candidate validation, coherent B-E-B and
 * the rejection/retry block are compiled verbatim from production. */
static ResourceXTargetInstallFollowState preassert_entry_state;
static int preassert_fuses;
static int preassert_failure_records;
static int preassert_iterations;
static uint64 preassert_now_us;
static ClusterPcmOwnEntry *preassert_wait_entry;
static int preassert_sleeps;
static bool preassert_observation_unowned;
static int preassert_entry_captures;
static int preassert_entry_foreign_tag;

static ResourceXApplyResult
gcs_block_resource_x_target_own_observation_result(const ClusterPcmOwnSnapshot *own,
												   const BufferTag *resource, bool unowned);

static void
preassert_wait_schedule(long usec)
{
	UT_ASSERT(usec > 0);
	preassert_sleeps++;
	if (preassert_wait_entry != NULL) {
		pg_atomic_fetch_add_u64(&preassert_wait_entry->generation, 2);
		pg_atomic_write_u32(&preassert_wait_entry->flags, 0);
		preassert_wait_entry = NULL;
	}
}

static void
gcs_block_resource_x_fail_closed_current(void)
{
	preassert_fuses++;
}

ResourceXTargetInstallFollowState
cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
	const ResourceXAssertion *assertion, int32 master_node, uint64 formation, uint64 session,
	uint64 r4_generation, uint32 requester_connection, uint32 master_connection, uint64 retry_slice,
	uint64 deadline, const ClusterPcmOwnSnapshot *observed, ResourceXTargetInstallContinuation *out)
{
	preassert_entry_captures++;
	if (!BufferTagsEqual(&assertion->resource, &observed->tag))
		preassert_entry_foreign_tag++;
	UT_ASSERT_EQ(master_node, 3);
	UT_ASSERT_EQ(formation, 2);
	UT_ASSERT_EQ(session, UINT64_C(842236411871794));
	UT_ASSERT_EQ(r4_generation, 6);
	UT_ASSERT_EQ(requester_connection, 8);
	UT_ASSERT_EQ(master_connection, 8);
	UT_ASSERT_EQ(retry_slice, 10000);
	UT_ASSERT_EQ(deadline, UINT64_C(234641943560));
	memset(out, 0, sizeof(*out));
	return preassert_entry_state;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
	const ResourceXAssertion *assertion, int32 master_node, uint64 formation, uint64 session,
	uint64 r4_generation, uint32 requester_connection, uint32 master_connection, uint64 retry_slice,
	ResourceXBootstrapRoundFailureSnapshot *out)
{
	(void)assertion;
	(void)master_node;
	(void)formation;
	(void)session;
	(void)r4_generation;
	(void)requester_connection;
	(void)master_connection;
	(void)retry_slice;
	memset(out, 0, sizeof(*out));
	return RESOURCE_X_APPLY_NOT_FOUND;
}

static bool creation_fixture_unowned;
static bool creation_fixture_change;
static int creation_fixture_calls;
static ResourceXApplyResult creation_fixture_result;

bool
ClusterBufferDirectInitObservationUnowned(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	return creation_fixture_unowned;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_aux_creation_reobserve_exact(
	const ResourceXAssertion *assertion, int32 master_node, uint64 formation, uint64 master_session,
	uint64 r4_generation, uint32 requester_connection, uint32 master_connection,
	const ResourceXCallerWitness *caller, uint64 pending_generation, uint64 reservation_token,
	const ClusterPcmOwnSnapshot *observed)
{
	UT_ASSERT_NOT_NULL(assertion);
	UT_ASSERT_NOT_NULL(caller);
	UT_ASSERT_NOT_NULL(observed);
	UT_ASSERT_EQ(master_node, 3);
	UT_ASSERT_EQ(formation, 2);
	UT_ASSERT_EQ(master_session, 31);
	UT_ASSERT_EQ(r4_generation, 6);
	UT_ASSERT_EQ(requester_connection, 8);
	UT_ASSERT_EQ(master_connection, 8);
	UT_ASSERT_EQ(pending_generation, 0);
	UT_ASSERT_EQ(reservation_token, 1);
	creation_fixture_calls++;
	if (creation_fixture_change)
		pg_atomic_fetch_add_u64(&ClusterPcmOwnArray[0].generation, 1);
	return creation_fixture_result;
}

#include "test_cluster_pcm_preassert_owner.inc"

static void
gcs_block_resource_x_first_failure_record(const ResourceXFirstFailureEvidence *failure)
{
	UT_ASSERT(failure->result != RESOURCE_X_APPLY_APPLIED);
	preassert_failure_records++;
}

static ResourceXApplyResult
preassert_consume(BufferDesc *buf, const ClusterPcmOwnSnapshot *initial)
{
	ClusterPcmOwnSnapshot own = *initial;
	ClusterPcmOwnSnapshot failure_live;
	ClusterPcmOwnResult n_candidate_result;
	ResourceXApplyResult result = RESOURCE_X_APPLY_APPLIED;
	ResourceXApplyResult target_install_observation_result;
	ResourceXApplyResult failure_snapshot_result;
	ResourceXTargetInstallContinuation target_install_follow;
	ResourceXTargetInstallFollowState target_install_follow_state;
	ResourceXBootstrapRoundFailureSnapshot failure_round;
	ResourceXFirstFailureEvidence first_failure;
	ResourceXAssertion assertion;
	struct {
		uint64 record_generation;
	} admission = { 6 };
	struct {
		uint64 formation;
	} gate = { 2 };
	uint64 master_session = UINT64_C(842236411871794);
	uint64 absolute_deadline_us = UINT64_C(234641943560);
	uint64 retry_slice_us = 10000;
	uint64 diagnostic_request_sequence = 11994;
	uint64 now_us;
	uint64 remaining_us;
	long timeout_ms;
	int cluster_gcs_block_retransmit_initial_backoff_ms = 10;
	uint32 requester_sender_connection_generation = 8;
	uint32 master_ingress_connection_generation = 8;
	int32 master_node = 3;
	bool first_failure_recorded = false;
	bool diagnostic_deadline_expired = false;
	bool target_retained_release_inflight = false;
	bool direct_init = false;
	bool join_only = false;
	ResourceXAuxiliaryAcquireContext auxiliary_context;
	ResourceXAuxiliaryAcquireContext *aux_context
		= preassert_observation_unowned ? &auxiliary_context : NULL;
	const char *diagnostic_stage = "own-snapshot";

	memset(&assertion, 0, sizeof(assertion));
	assertion.resource = initial->tag;
	assertion.requester_node = 0;
	preassert_iterations = 0;
	for (;;) {
		preassert_iterations++;
		if (preassert_iterations > 3) {
			UT_ASSERT(false);
			return RESOURCE_X_APPLY_BAD_STATE;
		}
		/* The real enclosing loop rechecks caller history and clock before
		 * a new B. This fixture proves only the selected consumer's decision;
		 * real history/terminal authority remain in test_cluster_pcm_lock. */
		if (preassert_iterations > 1) {
			if (preassert_now_us == 0 || preassert_now_us == UINT64_MAX)
				return RESOURCE_X_APPLY_INVALID;
			UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(buf, &own), CLUSTER_PCM_OWN_OK);
		}
		/* The existing outer observer is real production code too.  A later
		 * tag/readiness loss must return to this owner without reaching an
		 * entry lookup for the wrong physical observation. */
		if (preassert_observation_unowned) {
			result = gcs_block_resource_x_target_own_observation_result(&own, &assertion.resource,
																		true);
			if (result != RESOURCE_X_APPLY_APPLIED)
				return result;
		}
		result = RESOURCE_X_APPLY_APPLIED;
#define gcs_block_pcm_x_monotonic_us() preassert_now_us
#define pg_usleep(us) preassert_wait_schedule(us)
#define ClusterBufferAuxiliaryObservationUnowned(buffer)                                           \
	((void)(buffer), preassert_observation_unowned)
/* This verbatim consumer fixture sinks only the non-authoritative logger;
 * the real age/reason state machine is exercised in test_cluster_pcm_lock. */
#define gcs_block_resource_x_requester_wait_note(context, reason) ((void)(reason))
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() ((void)0)
#include "test_cluster_pcm_pending_consumer.inc"
		if (own.pcm_state == (uint8)PCM_STATE_N) {
#include "test_cluster_pcm_preassert_consumer.inc"
		}
#undef CHECK_FOR_INTERRUPTS
#undef gcs_block_resource_x_requester_wait_note
#undef pg_usleep
#undef ClusterBufferAuxiliaryObservationUnowned
#undef gcs_block_pcm_x_monotonic_us
		break;
	}
	(void)diagnostic_stage;
	(void)first_failure_recorded;
	(void)diagnostic_deadline_expired;
	return result;
}

static char *read_bufmgr_source(void);
static void assert_ordered_in_function(const char *source, const char *function_start,
									   const char *function_end, const char *const *needles,
									   int needle_count);
static void assert_source_range_contains(const char *start, const char *end, const char *needle);

bool
cluster_pcm_lock_resource_x_holder_pair_retained_fence_exact(const BufferTag *tag, int32 master,
															 uint64 session, uint64 formation,
															 uint64 generation)
{
	UT_ASSERT_EQ(tag->forkNum, VISIBILITYMAP_FORKNUM);
	UT_ASSERT_EQ(master, 1);
	UT_ASSERT_EQ(session, UINT64_C(842379567870890));
	UT_ASSERT_EQ(formation, 2);
	UT_ASSERT_EQ(generation, 48);
	return true;
}

/* The entire production N/predecessor interlock, with only the entry answer
 * and scheduled physical I/O as dependencies. No hand-written decision model. */
static ResourceXApplyResult
n_predecessor_consume(BufferDesc *buf, const ClusterPcmOwnSnapshot *initial)
{
	ClusterPcmOwnSnapshot own = *initial;
	ClusterPcmOwnResult own_result;
	ResourceXApplyResult result = RESOURCE_X_APPLY_APPLIED;
	ResourceXAssertion assertion = { 0 };
	struct {
		uint64 formation;
	} gate = { 2 };
	int32 master_node = 1;
	uint64 master_session = UINT64_C(842379567870890);
	bool target_retained_release_inflight = false;
	bool target_retained_release_post_mutation = false;
	ClusterPcmOwnResult diagnostic_n_predecessor_result = CLUSTER_PCM_OWN_INVALID;
	bool diagnostic_n_predecessor_pair = false;
	const char *diagnostic_stage = "own-snapshot";
	int iteration;

	assertion.resource = initial->tag;
	for (iteration = 0; iteration < 4; iteration++) {
		if (iteration != 0)
			UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(buf, &own), CLUSTER_PCM_OWN_OK);
#define gcs_block_resource_x_requester_wait_note(context, reason) ((void)(reason))
#define gcs_block_resource_x_observation_pause() ((void)0)
#include "test_cluster_pcm_n_predecessor_consumer.inc"
#undef gcs_block_resource_x_observation_pause
#undef gcs_block_resource_x_requester_wait_note
		break;
	}
	UT_ASSERT(iteration < 4);
	(void)target_retained_release_post_mutation;
	(void)diagnostic_n_predecessor_result;
	(void)diagnostic_n_predecessor_pair;
	(void)diagnostic_stage;
	return result;
}

static void
n_predecessor_fixture(BufferDesc *buf, ClusterPcmOwnEntry *entry, uint8 image)
{
	memset(buf, 0, sizeof(*buf));
	memset(entry, 0, sizeof(*entry));
	buf->tag.spcOid = 1663;
	buf->tag.dbOid = 5;
	buf->tag.relNumber = 16393;
	buf->tag.forkNum = VISIBILITYMAP_FORKNUM;
	buf->buffer_type = image;
	buf->pcm_state = PCM_STATE_N;
	pg_atomic_init_u32(&buf->state, BM_TAG_VALID | BM_VALID);
	pg_atomic_init_u64(&entry->generation, 48);
	pg_atomic_init_u64(&entry->reservation_token, 48);
	ClusterPcmOwnArray = entry;
	n_predecessor_header_step = 0;
}

UT_TEST(test_real_n_predecessor_does_not_fence_a_lost_physical_observation)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before;
	int image;

	for (image = 0; image < 2; image++) {
		n_predecessor_fixture(&buf, &entry, image == 0 ? BUF_TYPE_CURRENT : BUF_TYPE_PI);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &before), CLUSTER_PCM_OWN_OK);
		preassert_fuses = 0;
		n_predecessor_header_step = 1;
		UT_ASSERT_EQ(n_predecessor_consume(&buf, &before), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(preassert_fuses, 0);
		n_predecessor_header_step = 0;
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 48);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), 48);
		UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf.state)), 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_n_predecessor_classifies_one_physical_image_without_mutation)
{
	static const struct {
		uint8 type;
		uint32 flags;
		uint32 bits;
		ClusterPcmOwnResult want;
		bool retained;
	} cases[] = { { BUF_TYPE_CURRENT, 0, 0, CLUSTER_PCM_OWN_OK, false },
				  { BUF_TYPE_PI, 0, 0, CLUSTER_PCM_OWN_OK, true },
				  { BUF_TYPE_PI, PCM_OWN_FLAG_REVOKING, 0, CLUSTER_PCM_OWN_OK, true },
				  { BUF_TYPE_CURRENT, PCM_OWN_FLAG_REVOKING, 0, CLUSTER_PCM_OWN_CORRUPT, false },
				  { BUF_TYPE_CURRENT, 0, BM_IO_IN_PROGRESS, CLUSTER_PCM_OWN_BUSY, false },
				  { BUF_TYPE_PI, 0, BM_IO_IN_PROGRESS, CLUSTER_PCM_OWN_BUSY, false },
				  { BUF_TYPE_CURRENT, 0, BM_DIRTY, CLUSTER_PCM_OWN_BUSY, false },
				  { BUF_TYPE_CURRENT, 0, BM_CHECKPOINT_NEEDED, CLUSTER_PCM_OWN_BUSY, false },
				  { BUF_TYPE_PI, 0, BM_DIRTY, CLUSTER_PCM_OWN_CORRUPT, false },
				  { BUF_TYPE_PI, 0, BM_JUST_DIRTIED, CLUSTER_PCM_OWN_CORRUPT, false },
				  { BUF_TYPE_PI, 0, BM_CHECKPOINT_NEEDED, CLUSTER_PCM_OWN_CORRUPT, false },
				  { BUF_TYPE_CURRENT, 0, BM_IO_ERROR | BM_IO_IN_PROGRESS, CLUSTER_PCM_OWN_CORRUPT,
					false },
				  { BUF_TYPE_PI, 0, BM_IO_ERROR, CLUSTER_PCM_OWN_CORRUPT, false },
				  { 255, 0, 0, CLUSTER_PCM_OWN_CORRUPT, false } };
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before, after;
	bool retained;
	size_t i;

	for (i = 0; i < lengthof(cases); i++) {
		n_predecessor_fixture(&buf, &entry, cases[i].type);
		pg_atomic_write_u32(&entry.flags, cases[i].flags);
		pg_atomic_fetch_or_u32(&buf.state, cases[i].bits);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &before), CLUSTER_PCM_OWN_OK);
		retained = !cases[i].retained;
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_n_predecessor_observe_exact(&buf, &before, &retained),
					 cases[i].want);
		UT_ASSERT_EQ(retained, cases[i].retained);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &after), CLUSTER_PCM_OWN_OK);
		UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&before, &after));
		UT_ASSERT_EQ(pg_atomic_read_u32(&buf.state), BM_TAG_VALID | BM_VALID | cases[i].bits);
		/* The old authority predicate does not inherit BUSY/STALE retry. */
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_n_retained_release_inflight_exact(&buf, &before),
					 cases[i].want == CLUSTER_PCM_OWN_OK && cases[i].retained);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_n_predecessor_clears_output_for_every_changed_snapshot_byte)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before, changed;
	ClusterPcmOwnResult result;
	bool retained;
	size_t i;

	n_predecessor_fixture(&buf, &entry, BUF_TYPE_PI);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &before), CLUSTER_PCM_OWN_OK);
	for (i = 0; i < sizeof(before); i++) {
		changed = before;
		((unsigned char *)&changed)[i] ^= 1;
		retained = true;
		result = cluster_bufmgr_pcm_own_n_predecessor_observe_exact(&buf, &changed, &retained);
		UT_ASSERT(result == CLUSTER_PCM_OWN_STALE || result == CLUSTER_PCM_OWN_INVALID);
		UT_ASSERT(!retained);
	}
	retained = true;
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_n_predecessor_observe_exact(NULL, &before, &retained),
				 CLUSTER_PCM_OWN_INVALID);
	UT_ASSERT(!retained);
	retained = true;
	ClusterPcmOwnArray = NULL;
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_n_predecessor_observe_exact(&buf, &before, &retained),
				 CLUSTER_PCM_OWN_NOT_READY);
	UT_ASSERT(!retained);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_n_predecessor_consumer_keeps_stable_image_contradictions_closed)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before;
	int image;

	for (image = 0; image < 2; image++) {
		n_predecessor_fixture(&buf, &entry, image == 0 ? BUF_TYPE_CURRENT : BUF_TYPE_PI);
		pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &before), CLUSTER_PCM_OWN_OK);
		preassert_fuses = 0;
		UT_ASSERT_EQ(n_predecessor_consume(&buf, &before), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(preassert_fuses, 1);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 48);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), 48);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_pcm_own_snapshot_equality_is_whole_object_exact)
{
	ClusterPcmOwnSnapshot base;
	ClusterPcmOwnSnapshot changed;
	ClusterPcmOwnSnapshot same;
	size_t i;

	memset(&base, 0, sizeof(base));
	base.tag.spcOid = 11;
	base.tag.dbOid = 22;
	base.tag.relNumber = 33;
	base.tag.forkNum = MAIN_FORKNUM;
	base.tag.blockNum = 44;
	base.generation = 55;
	base.reservation_token = 66;
	base.writer_activation_token = 77;
	base.resource_x_activation_generation = 88;
	base.flags = PCM_OWN_FLAG_GRANT_PENDING;
	base.pcm_state = (uint8)PCM_STATE_N;
	same = base;

	UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&base, &same));
	UT_ASSERT(!cluster_pcm_own_snapshot_equal_exact(NULL, &same));
	UT_ASSERT(!cluster_pcm_own_snapshot_equal_exact(&base, NULL));
	UT_ASSERT(!cluster_pcm_own_snapshot_equal_exact(NULL, NULL));

	/* The production constructor zeros the entire fixed-size object before
	 * assignment.  Flipping each byte therefore proves that every field,
	 * BufferTag byte, padding byte and reserved byte participates. */
	for (i = 0; i < sizeof(base); i++) {
		changed = base;
		((unsigned char *)&changed)[i] ^= UINT8_C(1);
		UT_ASSERT(!cluster_pcm_own_snapshot_equal_exact(&base, &changed));
	}
}

static void
snapshot_owner_fixture(BufferDesc *buf, ClusterPcmOwnEntry *entry)
{
	memset(buf, 0, sizeof(*buf));
	memset(entry, 0, sizeof(*entry));
	buf->tag.spcOid = 1663;
	buf->tag.dbOid = 5;
	buf->tag.relNumber = 16386;
	buf->tag.forkNum = VISIBILITYMAP_FORKNUM;
	buf->pcm_state = (uint8)PCM_STATE_N;
	buf->buffer_type = (uint8)BUF_TYPE_CURRENT;
	pg_atomic_init_u32(&buf->state, BM_LOCKED | BM_TAG_VALID | BM_VALID);
	pg_atomic_init_u64(&entry->generation, 17);
	pg_atomic_init_u64(&entry->reservation_token, 21);
	pg_atomic_init_u64(&entry->writer_activation_token, 23);
	pg_atomic_init_u64(&entry->resource_x_activation_generation, 29);
	pg_atomic_init_u32(&entry->flags, PCM_OWN_FLAG_GRANT_PENDING);
}

UT_TEST(test_aux_creation_disposition_uses_real_beb_and_excludes_retained_context)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before;
	ResourceXAssertion assertion;
	ResourceXCallerWitness caller = { 0 };

	for (unsigned mode = 0; mode < 5; mode++) {
		snapshot_owner_fixture(&buf, &entry);
		ClusterPcmOwnArray = &entry;
		cluster_pcm_own_snapshot_locked(&buf, &before);
		UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
		memset(&assertion, 0, sizeof(assertion));
		assertion.resource = buf.tag;
		creation_fixture_unowned = mode != 0;
		creation_fixture_change = mode == 2 || mode == 4;
		creation_fixture_result = mode >= 3 ? RESOURCE_X_APPLY_STALE : RESOURCE_X_APPLY_APPLIED;
		creation_fixture_calls = 0;
		UT_ASSERT_EQ(gcs_block_resource_x_aux_creation_reobserve_coherent(
						 &buf, &assertion, 3, 2, 31, 6, 8, 8, &caller, 0, 1, &before),
					 mode == 0	 ? RESOURCE_X_APPLY_NOT_FOUND
					 : mode == 1 ? RESOURCE_X_APPLY_APPLIED
					 : mode == 3 ? RESOURCE_X_APPLY_STALE
								 : RESOURCE_X_APPLY_DUPLICATE);
		UT_ASSERT_EQ(creation_fixture_calls, mode == 0 ? 0 : 1);
	}
	creation_fixture_unowned = creation_fixture_change = false;
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_barrier_refusal_ignores_another_callers_pending)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry stable;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot base = { 0 };
	ClusterPcmXWriterLedgerEntry *writer = NULL;
	ClusterBufmgrBarrierUnwindContext context = { 0 };

	snapshot_owner_fixture(&buf, &entry);
	ClusterPcmOwnArray = &entry;
	UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
	context.buf = &buf;
	context.pending_base = &base;
	context.writer = &writer;
	transition_content_held = barrier_local_writer = false;
	memcpy(&stable, &entry, sizeof(entry));
	UT_ASSERT(cluster_bufmgr_barrier_prove_empty(&context));
	UT_ASSERT_EQ(memcmp(&stable, &entry, sizeof(entry)), 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_barrier_refusal_abort_then_successor_is_not_own_residue)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry stable;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot base;
	ClusterPcmXWriterLedgerEntry *writer = NULL;
	ClusterBufmgrBarrierUnwindContext context = { 0 };
	uint64 successor_token = 0;
	uint64 committed_generation = 0;
	uint32 state;
	int conversion;

	snapshot_owner_fixture(&buf, &entry);
	ClusterPcmOwnArray = &entry;
	pg_atomic_write_u64(&entry.writer_activation_token, 0);
	pg_atomic_write_u64(&entry.resource_x_activation_generation, 0);
	pg_atomic_write_u32(&entry.flags, 0);
	cluster_pcm_own_snapshot_locked(&buf, &base);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(buf.buf_id, base.generation,
														 PCM_OWN_FLAG_GRANT_PENDING,
														 &context.pending_token),
				 CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
	context.buf = &buf;
	context.pending_base = &base;
	context.writer = &writer;
	transition_content_held = barrier_local_writer = false;
	UT_ASSERT(!cluster_bufmgr_barrier_prove_empty(&context));
	UT_ASSERT_EQ(cluster_pcm_own_abort_grant_reservation(&buf, &base, context.pending_token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT(cluster_bufmgr_barrier_prove_empty(&context));
	/* A different backend reserves immediately after our exact abort,
	 * before the common proof: real sidecar owner, not a Boolean model. */
	state = transition_lock_header(&buf);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(
					 buf.buf_id, base.generation, PCM_OWN_FLAG_GRANT_PENDING, &successor_token),
				 CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(&buf, state);
	UT_ASSERT(successor_token > context.pending_token);
	memcpy(&stable, &entry, sizeof(entry));
	UT_ASSERT(cluster_bufmgr_barrier_prove_empty(&context));
	UT_ASSERT_EQ(memcmp(&stable, &entry, sizeof(entry)), 0);
	/* Several later committed lifecycles are not a one-generation release
	 * exception. Drive real token/commit transitions, preserving every new
	 * reservation when the old caller observes its own empty responsibility. */
	for (conversion = 0; conversion < 3; conversion++) {
		state = transition_lock_header(&buf);
		UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(buf.buf_id,
														cluster_pcm_own_gen_get(buf.buf_id),
														successor_token, &committed_generation),
					 CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(buf.buf_id, committed_generation,
															 PCM_OWN_FLAG_GRANT_PENDING,
															 &successor_token),
					 CLUSTER_PCM_OWN_OK);
		UnlockBufHdr(&buf, state);
		memcpy(&stable, &entry, sizeof(entry));
		UT_ASSERT(cluster_bufmgr_barrier_prove_empty(&context));
		UT_ASSERT_EQ(memcmp(&stable, &entry, sizeof(entry)), 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_barrier_refusal_rejects_own_lock_and_ledger)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry stable;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterBufmgrBarrierUnwindContext context = { 0 };

	snapshot_owner_fixture(&buf, &entry);
	ClusterPcmOwnArray = &entry;
	UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
	context.buf = &buf;
	memcpy(&stable, &entry, sizeof(entry));
	transition_content_held = true;
	barrier_local_writer = false;
	UT_ASSERT(!cluster_bufmgr_barrier_prove_empty(&context));
	UT_ASSERT_EQ(strcmp(context.proof_reason, "REFUSAL_TARGET_LOCK_HELD"), 0);
	transition_content_held = false;
	barrier_local_writer = true;
	UT_ASSERT(!cluster_bufmgr_barrier_prove_empty(&context));
	UT_ASSERT_EQ(strcmp(context.proof_reason, "REFUSAL_LOCAL_WRITER_REMAINS"), 0);
	barrier_local_writer = false;
	UT_ASSERT_EQ(memcmp(&stable, &entry, sizeof(entry)), 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_barrier_refusal_preserves_identity_failures)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	int variant;

	for (variant = 0; variant < 15; variant++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		ClusterPcmOwnEntry stable;
		ClusterPcmOwnSnapshot base;
		ClusterBufmgrBarrierUnwindContext context = { 0 };

		snapshot_owner_fixture(&buf, &entry);
		ClusterPcmOwnArray = &entry;
		pg_atomic_write_u64(&entry.writer_activation_token, 0);
		pg_atomic_write_u64(&entry.resource_x_activation_generation, 0);
		pg_atomic_write_u32(&entry.flags, 0);
		cluster_pcm_own_snapshot_locked(&buf, &base);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(buf.buf_id, base.generation,
															 PCM_OWN_FLAG_GRANT_PENDING,
															 &context.pending_token),
					 CLUSTER_PCM_OWN_OK);
		UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
		context.buf = &buf;
		context.pending_base = &base;
		transition_content_held = barrier_local_writer = false;
		switch (variant) {
		case 0: /* Our exact un-aborted GRANT_PENDING. */
			break;
		case 1: /* The same still-live token cannot be relabeled away. */
			pg_atomic_write_u32(&entry.flags, PCM_OWN_FLAG_REVOKING);
			break;
		case 2:
			pg_atomic_write_u32(&entry.flags, PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);
			break;
		case 3:
			pg_atomic_write_u64(&entry.reservation_token, context.pending_token - 1);
			break;
		case 4:
			pg_atomic_write_u64(&entry.generation, UINT64_MAX);
			break;
		case 5:
			pg_atomic_write_u64(&entry.generation, base.generation - 1);
			break;
		case 6:
			pg_atomic_write_u64(&entry.reservation_token, UINT64_MAX);
			break;
		case 7:
			buf.tag.blockNum++;
			break;
		case 8:
			context.pending_base = NULL;
			break;
		case 9:
			context.pending_token = UINT64_MAX;
			break;
		case 10:
			base.flags = PCM_OWN_FLAG_GRANT_PENDING;
			break;
		case 11:
			base.reservation_token--;
			break;
		case 12:
			ClusterPcmOwnArray = NULL;
			break;
		case 13:
			base.pcm_state = (uint8)PCM_STATE_X;
			break;
		case 14:
			base.generation = UINT64_MAX;
			break;
		}
		memcpy(&stable, &entry, sizeof(entry));
		UT_ASSERT(!cluster_bufmgr_barrier_prove_empty(&context));
		UT_ASSERT_EQ(strcmp(context.proof_reason, variant < 2
													  ? "REFUSAL_EXACT_PENDING_REMAINS"
													  : "REFUSAL_PENDING_IDENTITY_UNPROVEN"),
					 0);
		UT_ASSERT_EQ(memcmp(&stable, &entry, sizeof(entry)), 0);
		UT_ASSERT(!transition_content_held);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_preassert_discards_completed_conversion_observation)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry stable;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before;
	PGIOAlignedBlock unchanged;

	snapshot_owner_fixture(&buf, &entry);
	ClusterPcmOwnArray = &entry;
	buf.tag.relNumber = 16429;
	buf.tag.forkNum = MAIN_FORKNUM;
	buf.tag.blockNum = 6717;
	buf.buffer_type = BUF_TYPE_PI;
	pg_atomic_write_u64(&entry.generation, 12);
	pg_atomic_write_u64(&entry.reservation_token, 8);
	pg_atomic_write_u64(&entry.writer_activation_token, 0);
	pg_atomic_write_u64(&entry.resource_x_activation_generation, 0);
	pg_atomic_write_u32(&entry.flags, 0);
	cluster_pcm_own_snapshot_locked(&buf, &before);
	UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
	/* Deterministic schedule: the descriptor has completed another current
	 * conversion after the caller's saved observation, before its real
	 * header-locked N assertion check. No replacement success predicate. */
	pg_atomic_write_u64(&entry.generation, 14);
	pg_atomic_write_u64(&entry.reservation_token, 10);
	memcpy(&stable, &entry, sizeof(entry));
	memcpy(unchanged.data, transition_page.data, BLCKSZ);
	preassert_entry_state = RESOURCE_X_TARGET_INSTALL_STALE;
	preassert_now_us = UINT64_C(234638955873);
	preassert_fuses = preassert_failure_records = 0;
	UT_ASSERT_EQ(preassert_consume(&buf, &before), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(preassert_iterations, 2);
	UT_ASSERT_EQ(preassert_failure_records, 0);
	UT_ASSERT_EQ(preassert_fuses, 0);
	UT_ASSERT_EQ(memcmp(&stable, &entry, sizeof(entry)), 0);
	UT_ASSERT_EQ(memcmp(unchanged.data, transition_page.data, BLCKSZ), 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_preassert_requalifies_late_unowned_auxiliary_retag)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	int axis;

	for (axis = 0; axis < 5; axis++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		ClusterPcmOwnEntry stable;
		ClusterPcmOwnSnapshot before;
		PGIOAlignedBlock unchanged;

		snapshot_owner_fixture(&buf, &entry);
		ClusterPcmOwnArray = &entry;
		buf.tag.relNumber = 16393;
		buf.tag.forkNum = VISIBILITYMAP_FORKNUM;
		buf.tag.blockNum = 0;
		buf.buffer_type = BUF_TYPE_CURRENT;
		pg_atomic_write_u64(&entry.generation, 11072);
		pg_atomic_write_u64(&entry.reservation_token, 11085);
		pg_atomic_write_u64(&entry.writer_activation_token, 0);
		pg_atomic_write_u64(&entry.resource_x_activation_generation, 0);
		pg_atomic_write_u32(&entry.flags, 0);
		cluster_pcm_own_snapshot_locked(&buf, &before);
		UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
		/* B0 passed the real entry observer.  The deliberately unpinned
		 * descriptor is reused before the header-locked candidate check. */
		pg_atomic_write_u64(&entry.generation, 11074);
		pg_atomic_write_u64(&entry.reservation_token, 11087);
		switch (axis) {
		case 0:
			buf.tag.spcOid++;
			break;
		case 1:
			buf.tag.dbOid++;
			break;
		case 2:
			buf.tag.relNumber++;
			break;
		case 3:
			buf.tag.forkNum = MAIN_FORKNUM;
			break;
		case 4:
			buf.tag.blockNum++;
			break;
		}
		memcpy(&stable, &entry, sizeof(entry));
		memcpy(unchanged.data, transition_page.data, BLCKSZ);
		preassert_entry_state = RESOURCE_X_TARGET_INSTALL_STALE;
		preassert_now_us = UINT64_C(234638955873);
		preassert_observation_unowned = true;
		preassert_entry_captures = preassert_entry_foreign_tag = 0;
		preassert_fuses = preassert_failure_records = 0;
		UT_ASSERT_EQ(preassert_consume(&buf, &before), RESOURCE_X_APPLY_NOT_FOUND);
		UT_ASSERT_EQ(preassert_iterations, 2);
		UT_ASSERT_EQ(preassert_entry_captures, 0);
		UT_ASSERT_EQ(preassert_entry_foreign_tag, 0);
		UT_ASSERT_EQ(preassert_failure_records, 0);
		UT_ASSERT_EQ(preassert_fuses, 0);
		UT_ASSERT_EQ(memcmp(&stable, &entry, sizeof(entry)), 0);
		UT_ASSERT_EQ(memcmp(unchanged.data, transition_page.data, BLCKSZ), 0);
	}
	preassert_observation_unowned = false;
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_preassert_requalifies_late_unowned_auxiliary_readiness)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	int reading;

	for (reading = 0; reading < 2; reading++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		ClusterPcmOwnSnapshot before;
		PGIOAlignedBlock unchanged;

		snapshot_owner_fixture(&buf, &entry);
		ClusterPcmOwnArray = &entry;
		buf.tag.forkNum = VISIBILITYMAP_FORKNUM;
		buf.buffer_type = BUF_TYPE_CURRENT;
		pg_atomic_write_u64(&entry.generation, 18);
		pg_atomic_write_u64(&entry.reservation_token, 21);
		pg_atomic_write_u64(&entry.writer_activation_token, 0);
		pg_atomic_write_u64(&entry.resource_x_activation_generation, 0);
		pg_atomic_write_u32(&entry.flags, 0);
		cluster_pcm_own_snapshot_locked(&buf, &before);
		UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
		pg_atomic_fetch_and_u32(&buf.state, ~BM_VALID);
		if (reading)
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_IN_PROGRESS);
		memcpy(unchanged.data, transition_page.data, BLCKSZ);
		preassert_entry_state = RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
		preassert_now_us = UINT64_C(234638955873);
		preassert_observation_unowned = true;
		preassert_entry_captures = preassert_entry_foreign_tag = 0;
		preassert_fuses = preassert_failure_records = 0;
		UT_ASSERT_EQ(preassert_consume(&buf, &before), RESOURCE_X_APPLY_NOT_FOUND);
		UT_ASSERT_EQ(preassert_iterations, 2);
		UT_ASSERT_EQ(preassert_entry_captures, 0);
		UT_ASSERT_EQ(preassert_failure_records, 0);
		UT_ASSERT_EQ(preassert_fuses, 0);
		UT_ASSERT_EQ(memcmp(unchanged.data, transition_page.data, BLCKSZ), 0);
	}
	preassert_observation_unowned = false;
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_capture_domain_preserves_owned_and_stable_refusals)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	int variant;

	for (variant = 0; variant < 14; variant++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		ClusterPcmOwnSnapshot before;
		ResourceXAssertion assertion;
		ResourceXTargetInstallContinuation continuation;
		ResourceXTargetInstallContinuation empty;
		ResourceXTargetInstallFollowState state;
		ResourceXApplyResult result;
		PGIOAlignedBlock unchanged;

		snapshot_owner_fixture(&buf, &entry);
		ClusterPcmOwnArray = &entry;
		buf.tag.forkNum = VISIBILITYMAP_FORKNUM;
		buf.buffer_type = BUF_TYPE_CURRENT;
		pg_atomic_write_u64(&entry.generation, 18);
		pg_atomic_write_u64(&entry.reservation_token, 21);
		pg_atomic_write_u64(&entry.writer_activation_token, 0);
		pg_atomic_write_u64(&entry.resource_x_activation_generation, 0);
		pg_atomic_write_u32(&entry.flags, 0);
		memset(&assertion, 0, sizeof(assertion));
		assertion.resource = buf.tag;
		if (variant >= 3 && variant <= 11)
			pg_atomic_fetch_and_u32(&buf.state, ~BM_VALID);
		switch (variant) {
		case 0:
			buf.tag.blockNum++;
			break; /* Owned descriptor. */
		case 1:
			buf.tag.blockNum++; /* TORN never becomes retag retry. */
								/* FALLTHROUGH */
		case 2:
			pg_atomic_write_u64(&entry.generation, UINT64_MAX);
			break;
		case 3:
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR);
			break;
		case 4:
			pg_atomic_fetch_or_u32(&buf.state, BM_DIRTY);
			break;
		case 5:
			buf.buffer_type = BUF_TYPE_PI;
			break;
		case 6:
			pg_atomic_write_u64(&entry.writer_activation_token, 1);
			break;
		case 7:
			pg_atomic_write_u32(&entry.flags, PCM_OWN_FLAG_REVOKING);
			break;
		case 8:
			pg_atomic_write_u32(&entry.flags, PCM_OWN_FLAG_GRANT_PENDING);
			break;
		case 9:
			pg_atomic_write_u64(&entry.resource_x_activation_generation, 1);
			break;
		case 10:
			buf.tag.forkNum = MAIN_FORKNUM;
			assertion.resource = buf.tag;
			break;
		case 11:
			buf.tag.forkNum = INIT_FORKNUM;
			assertion.resource = buf.tag;
			break;
		}
		cluster_pcm_own_snapshot_locked(&buf, &before);
		UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
		preassert_entry_state = variant == 12 ? RESOURCE_X_TARGET_INSTALL_INVALID
											  : RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
		preassert_entry_captures = preassert_entry_foreign_tag = 0;
		memset(&continuation, 0xa5, sizeof(continuation));
		memset(&empty, 0, sizeof(empty));
		memcpy(unchanged.data, transition_page.data, BLCKSZ);
		result = gcs_block_resource_x_target_install_capture_coherent(
			&buf, &assertion, 3, 2, UINT64_C(842236411871794), 6, 8, 8, 10000,
			UINT64_C(234641943560), &before, variant != 0, &state, &continuation);
		UT_ASSERT_EQ(result, variant < 3 ? RESOURCE_X_APPLY_STALE : RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(state,
					 variant < 3 ? RESOURCE_X_TARGET_INSTALL_INVALID : preassert_entry_state);
		UT_ASSERT_EQ(preassert_entry_captures, variant < 3 ? 0 : 1);
		UT_ASSERT_EQ(preassert_entry_foreign_tag, 0);
		UT_ASSERT_EQ(memcmp(&continuation, &empty, sizeof(empty)), 0);
		UT_ASSERT_EQ(memcmp(unchanged.data, transition_page.data, BLCKSZ), 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_pending_recapture_keeps_original_observation)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry stable;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before;
	PGIOAlignedBlock unchanged;

	snapshot_owner_fixture(&buf, &entry);
	ClusterPcmOwnArray = &entry;
	buf.tag.relNumber = 16429;
	buf.tag.forkNum = MAIN_FORKNUM;
	buf.tag.blockNum = 6645;
	buf.buffer_type = BUF_TYPE_PI;
	pg_atomic_write_u64(&entry.generation, 8);
	pg_atomic_write_u64(&entry.reservation_token, 5);
	pg_atomic_write_u64(&entry.writer_activation_token, 0);
	pg_atomic_write_u64(&entry.resource_x_activation_generation, 0);
	pg_atomic_write_u32(&entry.flags, PCM_OWN_FLAG_GRANT_PENDING);
	cluster_pcm_own_snapshot_locked(&buf, &before);
	UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
	/* The real pending consumer must not replace B0 with a newer B1,
	 * validate B1-E-B2, then still feed pending B0 to the clean-N check.
	 * This is a deterministic consumer counterexample, not a live replay. */
	pg_atomic_write_u64(&entry.generation, 10);
	pg_atomic_write_u32(&entry.flags, 0);
	memcpy(&stable, &entry, sizeof(entry));
	memcpy(unchanged.data, transition_page.data, BLCKSZ);
	preassert_entry_state = RESOURCE_X_TARGET_INSTALL_STALE;
	preassert_now_us = UINT64_C(234638955873);
	preassert_fuses = preassert_failure_records = 0;
	UT_ASSERT_EQ(preassert_consume(&buf, &before), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(preassert_iterations, 2);
	UT_ASSERT_EQ(preassert_failure_records, 0);
	UT_ASSERT_EQ(preassert_fuses, 0);
	UT_ASSERT_EQ(memcmp(&stable, &entry, sizeof(entry)), 0);
	UT_ASSERT_EQ(memcmp(unchanged.data, transition_page.data, BLCKSZ), 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_pending_observation_rechecks_successor_and_preserves_refusals)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	int variant;

	for (variant = 0; variant < 13; variant++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		ClusterPcmOwnSnapshot before;
		PGIOAlignedBlock unchanged;
		ResourceXApplyResult expected = RESOURCE_X_APPLY_APPLIED;
		int iterations = 2;

		snapshot_owner_fixture(&buf, &entry);
		ClusterPcmOwnArray = &entry;
		buf.buffer_type = BUF_TYPE_PI;
		pg_atomic_write_u64(&entry.generation, 8);
		pg_atomic_write_u64(&entry.reservation_token, variant == 3 ? 0 : 5);
		pg_atomic_write_u64(&entry.writer_activation_token, variant == 4 ? 1 : 0);
		pg_atomic_write_u64(&entry.resource_x_activation_generation, 0);
		pg_atomic_write_u32(&entry.flags, PCM_OWN_FLAG_GRANT_PENDING);
		cluster_pcm_own_snapshot_locked(&buf, &before);
		UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
		preassert_entry_state = RESOURCE_X_TARGET_INSTALL_STALE;
		preassert_now_us = UINT64_C(234638955873);
		preassert_wait_entry = NULL;
		preassert_sleeps = preassert_fuses = preassert_failure_records = 0;
		if (variant < 2 || variant >= 8) {
			pg_atomic_write_u64(&entry.generation, 10);
			pg_atomic_write_u32(&entry.flags, 0);
		}
		switch (variant) {
		case 0: /* Token changes without a generation change. */
			pg_atomic_write_u64(&entry.generation, 8);
			pg_atomic_write_u64(&entry.reservation_token, 6);
			break;
		case 1: /* More than one complete intervening conversion. */
			pg_atomic_write_u64(&entry.generation, 24);
			pg_atomic_write_u64(&entry.reservation_token, 17);
			break;
		case 2: /* Same pending observation is wait-only; owner later clears it. */
			preassert_wait_entry = &entry;
			break;
		case 3:
		case 4:
			expected = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			iterations = 1;
			break;
		case 5:
			preassert_entry_state = RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
			expected = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			iterations = 1;
			break;
		case 6:
			preassert_entry_state = RESOURCE_X_TARGET_INSTALL_INVALID;
			expected = RESOURCE_X_APPLY_INVALID;
			iterations = 1;
			break;
		case 7:
			preassert_now_us = UINT64_C(234641943560);
			preassert_wait_entry = &entry; /* The real owner releases after a wait. */
			break;
		case 8:
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR);
			expected = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			break;
		case 9:
			pg_atomic_fetch_and_u32(&buf.state, ~BM_VALID);
			expected = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			break;
		case 10:
			pg_atomic_fetch_or_u32(&buf.state, BM_DIRTY);
			expected = RESOURCE_X_APPLY_BAD_STATE;
			break;
		case 11:
			buf.pcm_state = PCM_STATE_X;
			buf.buffer_type = BUF_TYPE_XCUR;
			pg_atomic_write_u64(&entry.generation, 9);
			break;
		case 12:
			buf.pcm_state = PCM_STATE_S;
			buf.buffer_type = BUF_TYPE_CURRENT;
			pg_atomic_write_u64(&entry.generation, 9);
			break;
		}
		memcpy(unchanged.data, transition_page.data, BLCKSZ);
		UT_ASSERT_EQ(preassert_consume(&buf, &before), expected);
		UT_ASSERT_EQ(preassert_iterations, iterations);
		UT_ASSERT_EQ(preassert_sleeps, variant == 2 || variant == 7 ? 1 : 0);
		UT_ASSERT_EQ(memcmp(unchanged.data, transition_page.data, BLCKSZ), 0);
		preassert_wait_entry = NULL;
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_preassert_resample_is_not_an_identity_or_deadline_exception)
{
	ClusterPcmOwnSnapshot before;
	ClusterPcmOwnSnapshot live;
	ClusterPcmOwnSnapshot changed;

	memset(&before, 0, sizeof(before));
	before.tag.spcOid = 1663;
	before.tag.dbOid = 5;
	before.tag.relNumber = 16429;
	before.tag.blockNum = 6717;
	before.pcm_state = (uint8)PCM_STATE_N;
	before.generation = 12;
	before.reservation_token = 8;
	live = before;
	live.generation = 14;
	live.reservation_token = 10;
#define RESAMPLE(b, l, r, f, n, d)                                                                 \
	cluster_gcs_resource_x_target_preassert_resample_exact(b, r, l, f, n, d)
#define ELIGIBLE(b, l)                                                                             \
	RESAMPLE(b, l, CLUSTER_PCM_OWN_STALE, RESOURCE_X_TARGET_INSTALL_STALE, 100, 200)
	UT_ASSERT(ELIGIBLE(&before, &live));
	UT_ASSERT(!ELIGIBLE(NULL, &live));
	UT_ASSERT(!ELIGIBLE(&before, NULL));
	UT_ASSERT(!ELIGIBLE(&before, &before));
#define REJECT_LIVE(field, value)                                                                  \
	do {                                                                                           \
		changed = live;                                                                            \
		changed.field = (value);                                                                   \
		UT_ASSERT(!ELIGIBLE(&before, &changed));                                                   \
	} while (0)
	REJECT_LIVE(tag.blockNum, 6718);
	REJECT_LIVE(generation, 11);
	REJECT_LIVE(generation, UINT64_MAX);
	REJECT_LIVE(reservation_token, 7);
	REJECT_LIVE(reservation_token, UINT64_MAX);
	REJECT_LIVE(pcm_state, UINT8_MAX);
#undef REJECT_LIVE
#define REJECT_BEFORE(field, value)                                                                \
	do {                                                                                           \
		changed = before;                                                                          \
		changed.field = (value);                                                                   \
		UT_ASSERT(!ELIGIBLE(&changed, &live));                                                     \
	} while (0)
	REJECT_BEFORE(pcm_state, (uint8)PCM_STATE_X);
	REJECT_BEFORE(flags, PCM_OWN_FLAG_GRANT_PENDING);
	REJECT_BEFORE(writer_activation_token, 1);
	REJECT_BEFORE(resource_x_activation_generation, 1);
	REJECT_BEFORE(generation, UINT64_MAX);
	REJECT_BEFORE(reservation_token, UINT64_MAX);
#undef REJECT_BEFORE
	UT_ASSERT(
		!RESAMPLE(&before, &live, CLUSTER_PCM_OWN_OK, RESOURCE_X_TARGET_INSTALL_STALE, 100, 200));
	UT_ASSERT(!RESAMPLE(&before, &live, CLUSTER_PCM_OWN_STALE,
						RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED, 100, 200));
	UT_ASSERT(!RESAMPLE(&before, &live, CLUSTER_PCM_OWN_STALE, RESOURCE_X_TARGET_INSTALL_INVALID,
						100, 200));
	UT_ASSERT(
		RESAMPLE(&before, &live, CLUSTER_PCM_OWN_STALE, RESOURCE_X_TARGET_INSTALL_STALE, 200, 200));
	UT_ASSERT(
		!RESAMPLE(&before, &live, CLUSTER_PCM_OWN_STALE, RESOURCE_X_TARGET_INSTALL_STALE, 0, 200));
	UT_ASSERT(!RESAMPLE(&before, &live, CLUSTER_PCM_OWN_STALE, RESOURCE_X_TARGET_INSTALL_STALE,
						UINT64_MAX, 200));
	UT_ASSERT(
		!RESAMPLE(&before, &live, CLUSTER_PCM_OWN_STALE, RESOURCE_X_TARGET_INSTALL_STALE, 100, 0));
	UT_ASSERT(!RESAMPLE(&before, &live, CLUSTER_PCM_OWN_STALE, RESOURCE_X_TARGET_INSTALL_STALE, 100,
						UINT64_MAX));
	/* Recognizing that a B changed to S/X grants no authority. The original
	 * enclosing loop, not this helper, must check its newly observed mode. */
	live.pcm_state = (uint8)PCM_STATE_S;
	UT_ASSERT(ELIGIBLE(&before, &live));
	live.pcm_state = (uint8)PCM_STATE_X;
	UT_ASSERT(ELIGIBLE(&before, &live));
#undef ELIGIBLE
#undef RESAMPLE
}

UT_TEST(test_real_preassert_rechecks_changed_image_and_preserves_failures)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	int variant;

	for (variant = 0; variant < 13; variant++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		ClusterPcmOwnEntry stable;
		ClusterPcmOwnSnapshot before;
		PGIOAlignedBlock unchanged;
		ResourceXApplyResult expected = RESOURCE_X_APPLY_STALE;
		int iterations = 1;
		int fuses = 0;

		snapshot_owner_fixture(&buf, &entry);
		ClusterPcmOwnArray = &entry;
		buf.buffer_type = BUF_TYPE_PI;
		pg_atomic_write_u64(&entry.generation, 12);
		pg_atomic_write_u64(&entry.reservation_token, 8);
		pg_atomic_write_u64(&entry.writer_activation_token, 0);
		pg_atomic_write_u64(&entry.resource_x_activation_generation, 0);
		pg_atomic_write_u32(&entry.flags, 0);
		cluster_pcm_own_snapshot_locked(&buf, &before);
		UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
		pg_atomic_write_u64(&entry.generation, 14);
		pg_atomic_write_u64(&entry.reservation_token, 10);
		preassert_entry_state = RESOURCE_X_TARGET_INSTALL_STALE;
		preassert_now_us = UINT64_C(234638955873);
		preassert_fuses = preassert_failure_records = 0;
		switch (variant) {
		case 0:
			preassert_entry_state = RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
			break;
		case 1:
			preassert_entry_state = RESOURCE_X_TARGET_INSTALL_INVALID;
			break;
		case 2:
			preassert_now_us = UINT64_C(234641943560);
			expected = RESOURCE_X_APPLY_APPLIED;
			iterations = 2;
			break;
		case 3:
			pg_atomic_write_u64(&entry.generation, 11);
			break;
		case 4:
			pg_atomic_write_u64(&entry.reservation_token, 7);
			break;
		case 5:
			pg_atomic_write_u64(&entry.generation, UINT64_MAX);
			break;
		case 6:
			pg_atomic_write_u64(&entry.reservation_token, UINT64_MAX);
			break;
		case 7:
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR);
			expected = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			iterations = 2;
			fuses = 1;
			break;
		case 8:
			pg_atomic_fetch_and_u32(&buf.state, ~BM_VALID);
			expected = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			iterations = 2;
			fuses = 1;
			break;
		case 9:
			pg_atomic_fetch_or_u32(&buf.state, BM_DIRTY);
			expected = RESOURCE_X_APPLY_BAD_STATE;
			iterations = 2;
			break;
		case 10:
			buf.buffer_type = BUF_TYPE_XCUR;
			expected = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			iterations = 2;
			fuses = 1;
			break;
		case 11:
			pg_atomic_write_u64(&entry.generation, 12);
			expected = RESOURCE_X_APPLY_APPLIED;
			iterations = 2;
			break;
		case 12:
			pg_atomic_write_u64(&entry.generation, 38);
			pg_atomic_write_u64(&entry.reservation_token, 30);
			expected = RESOURCE_X_APPLY_APPLIED;
			iterations = 2;
			break;
		}
		memcpy(&stable, &entry, sizeof(entry));
		memcpy(unchanged.data, transition_page.data, BLCKSZ);
		UT_ASSERT_EQ(preassert_consume(&buf, &before), expected);
		UT_ASSERT_EQ(preassert_iterations, iterations);
		/* TORN is now refused at the shared domain guard before the late
		 * failure recorder.  Its STALE result and zero mutation remain required. */
		UT_ASSERT_EQ(preassert_failure_records,
					 expected == RESOURCE_X_APPLY_APPLIED || variant == 5 ? 0 : 1);
		UT_ASSERT_EQ(preassert_fuses, fuses);
		UT_ASSERT_EQ(memcmp(&stable, &entry, sizeof(entry)), 0);
		UT_ASSERT_EQ(memcmp(unchanged.data, transition_page.data, BLCKSZ), 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_bufmgr_snapshot_matches_rejects_writer_token_only_drift)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before;
	ClusterPcmOwnSnapshot after;

	snapshot_owner_fixture(&buf, &entry);
	ClusterPcmOwnArray = &entry;
	cluster_pcm_own_snapshot_locked(&buf, &before);
	UT_ASSERT(cluster_pcm_own_fence_matches_locked(&buf, &before));
	pg_atomic_write_u64(&entry.writer_activation_token, 24);
	cluster_pcm_own_snapshot_locked(&buf, &after);
	UT_ASSERT(!cluster_pcm_own_fence_matches_locked(&buf, &before));
	UT_ASSERT_EQ(cluster_pcm_x_target_install_follow_adjudicate_exact(
					 &before, &after, RESOURCE_X_TARGET_INSTALL_TERMINAL, NULL, NULL),
				 RESOURCE_X_TARGET_INSTALL_RESAMPLE);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_bufmgr_snapshot_captures_image_type)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before;
	ClusterPcmOwnSnapshot after;

	snapshot_owner_fixture(&buf, &entry);
	ClusterPcmOwnArray = &entry;
	cluster_pcm_own_snapshot_locked(&buf, &before);
	buf.buffer_type = (uint8)BUF_TYPE_PI;
	cluster_pcm_own_snapshot_locked(&buf, &after);
	UT_ASSERT(!cluster_pcm_own_snapshot_equal_exact(&before, &after));
	UT_ASSERT_EQ(cluster_pcm_x_target_install_follow_adjudicate_exact(
					 &before, &after, RESOURCE_X_TARGET_INSTALL_TERMINAL, NULL, NULL),
				 RESOURCE_X_TARGET_INSTALL_RESAMPLE);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_bufmgr_snapshot_captures_each_semantic_buffer_bit)
{
	const uint32 bits[] = { BM_TAG_VALID,		  BM_VALID,			 BM_DIRTY,	 BM_JUST_DIRTIED,
							BM_CHECKPOINT_NEEDED, BM_IO_IN_PROGRESS, BM_IO_ERROR };
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before;
	ClusterPcmOwnSnapshot after;
	size_t i;

	snapshot_owner_fixture(&buf, &entry);
	ClusterPcmOwnArray = &entry;
	cluster_pcm_own_snapshot_locked(&buf, &before);
	for (i = 0; i < lengthof(bits); i++) {
		pg_atomic_write_u32(&buf.state, (BM_LOCKED | BM_TAG_VALID | BM_VALID) ^ bits[i]);
		cluster_pcm_own_snapshot_locked(&buf, &after);
		UT_ASSERT(!cluster_pcm_own_snapshot_equal_exact(&before, &after));
		UT_ASSERT_EQ(cluster_pcm_x_target_install_follow_adjudicate_exact(
						 &before, &after, RESOURCE_X_TARGET_INSTALL_TERMINAL, NULL, NULL),
					 RESOURCE_X_TARGET_INSTALL_RESAMPLE);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_bufmgr_owned_fence_survives_flush_but_observation_resamples)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot revoking;
	ClusterPcmOwnSnapshot clean;

	snapshot_owner_fixture(&buf, &entry);
	ClusterPcmOwnArray = &entry;
	buf.pcm_state = (uint8)PCM_STATE_X;
	pg_atomic_write_u32(&entry.flags, PCM_OWN_FLAG_REVOKING);
	pg_atomic_write_u64(&entry.writer_activation_token, 0);
	pg_atomic_write_u64(&entry.resource_x_activation_generation, 0);
	pg_atomic_write_u32(&buf.state, BM_LOCKED | BM_TAG_VALID | BM_VALID | BM_DIRTY | BM_JUST_DIRTIED
										| BM_CHECKPOINT_NEEDED);
	cluster_pcm_own_snapshot_locked(&buf, &revoking);

	/* FlushBuffer may clean the source without changing its ownership fence.
	 * The later finish must still accept that fence, while an observation
	 * bracket spanning this I/O must never publish its old terminal result. */
	pg_atomic_write_u32(&buf.state, BM_LOCKED | BM_TAG_VALID | BM_VALID);
	cluster_pcm_own_snapshot_locked(&buf, &clean);
	UT_ASSERT(cluster_pcm_own_fence_matches_locked(&buf, &revoking));
	UT_ASSERT(!cluster_pcm_own_snapshot_equal_exact(&revoking, &clean));
	UT_ASSERT_EQ(cluster_pcm_x_target_install_follow_adjudicate_exact(
					 &revoking, &clean, RESOURCE_X_TARGET_INSTALL_TERMINAL, NULL, NULL),
				 RESOURCE_X_TARGET_INSTALL_RESAMPLE);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_bufmgr_fence_rejects_every_non_image_byte_drift)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot live;
	ClusterPcmOwnSnapshot changed;
	size_t i;

	snapshot_owner_fixture(&buf, &entry);
	ClusterPcmOwnArray = &entry;
	cluster_pcm_own_snapshot_locked(&buf, &live);
	for (i = 0; i < sizeof(live); i++) {
		bool image_byte
			= (i >= offsetof(ClusterPcmOwnSnapshot, semantic_buf_state)
			   && i < offsetof(ClusterPcmOwnSnapshot, semantic_buf_state) + sizeof(uint32))
			  || i == offsetof(ClusterPcmOwnSnapshot, buffer_type);

		memcpy(&changed, &live, sizeof(changed));
		((unsigned char *)&changed)[i] ^= UINT8_C(1);
		UT_ASSERT_EQ(cluster_pcm_own_fence_matches_locked(&buf, &changed), image_byte);
		UT_ASSERT(!cluster_pcm_own_snapshot_equal_exact(&live, &changed));
	}
	UT_ASSERT(!cluster_pcm_own_fence_matches_locked(&buf, NULL));
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_bufmgr_snapshot_ignores_refcount_usage_and_pin_waiter)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before;
	ClusterPcmOwnSnapshot after;

	snapshot_owner_fixture(&buf, &entry);
	ClusterPcmOwnArray = &entry;
	cluster_pcm_own_snapshot_locked(&buf, &before);
	pg_atomic_write_u32(&buf.state, BM_LOCKED | BM_TAG_VALID | BM_VALID | BUF_REFCOUNT_ONE
										| BUF_USAGECOUNT_ONE | BM_PIN_COUNT_WAITER);
	memset(&after, 0xff, sizeof(after));
	cluster_pcm_own_snapshot_locked(&buf, &after);
	UT_ASSERT(cluster_pcm_own_fence_matches_locked(&buf, &before));
	UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&before, &after));
	UT_ASSERT_EQ(before.semantic_buf_state, BM_TAG_VALID | BM_VALID);
	UT_ASSERT_EQ(before.buffer_type, BUF_TYPE_CURRENT);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_snapshot_post_state_uses_unpublished_locked_state)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before;
	ClusterPcmOwnSnapshot after;
	const uint32 old_state
		= BM_LOCKED | BM_TAG_VALID | BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED;
	const uint32 post_state = BM_LOCKED | BM_TAG_VALID | BM_VALID | BUF_REFCOUNT_ONE
							  | BUF_USAGECOUNT_ONE | BM_PIN_COUNT_WAITER;

	snapshot_owner_fixture(&buf, &entry);
	ClusterPcmOwnArray = &entry;
	pg_atomic_write_u32(&buf.state, old_state);
	cluster_pcm_own_snapshot_locked(&buf, &before);
	memset(&after, 0xff, sizeof(after));
	cluster_pcm_own_snapshot_post_state_locked(&buf, post_state, &after);
	UT_ASSERT_EQ(after.semantic_buf_state, BM_TAG_VALID | BM_VALID);
	UT_ASSERT_EQ(after._reserved[0], 0);
	UT_ASSERT_EQ(after._reserved[1], 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&buf.state), old_state);
	UT_ASSERT(!cluster_pcm_own_snapshot_equal_exact(&before, &after));
	UT_ASSERT(cluster_pcm_own_fence_equal_exact(&before, &after));
	UT_ASSERT_EQ(before.semantic_buf_state,
				 BM_TAG_VALID | BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED);
	ClusterPcmOwnArray = saved;
}

static void
transition_fixture(BufferDesc *buf, ClusterPcmOwnEntry *entry, ClusterPcmOwnSnapshot *revoking,
				   bool dirty)
{
	snapshot_owner_fixture(buf, entry);
	ClusterPcmOwnArray = entry;
	buf->tag.forkNum = MAIN_FORKNUM;
	buf->pcm_state = (uint8)PCM_STATE_S;
	buf->buffer_type = (uint8)BUF_TYPE_SCUR;
	pg_atomic_write_u32(&entry->flags, PCM_OWN_FLAG_REVOKING);
	pg_atomic_write_u64(&entry->writer_activation_token, 0);
	pg_atomic_write_u64(&entry->resource_x_activation_generation, 0);
	pg_atomic_write_u32(&buf->state,
						BM_LOCKED | BM_TAG_VALID | BM_VALID
							| (dirty ? BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED : 0));
	cluster_pcm_own_snapshot_locked(buf, revoking);
	UnlockBufHdr(buf, pg_atomic_read_u32(&buf->state));
	memset(transition_page.data, 0, BLCKSZ);
	PageSetLSNPreserveOrigin((Page)transition_page.data, UINT64_C(0x12340));
	((PageHeader)transition_page.data)->pd_block_scn = 123;
	transition_buf = buf;
	transition_mapping_held = false;
	transition_content_held = false;
	transition_pin_count = 0;
	transition_base_pins = 0;
	transition_content_busy = false;
	transition_flush_count = 0;
	transition_flush_error = false;
	transition_flush_leaves_dirty = false;
	transition_copy_active = false;
	transition_wal_error = false;
	transition_wal_calls = 0;
	transition_wal_changes = 0;
	cluster_pcm_x_finish_retain_flush_active = false;
	read_reclaim_drift = 0;
	read_reclaim_error = false;
	route_bind_result = RESOURCE_X_APPLY_APPLIED;
	cluster_pcm_x_finish_retain_flush_io_active = false;
}

/* Actual eviction driver and clock-sweep loop.  Only mapping, PG pin/resource
 * bookkeeping and the GCS publish dependency are controlled here.  The local
 * X->N transition and ownership sidecar are production code. */
static int eviction_publishes, eviction_sleeps, eviction_frees, eviction_fuses;
static int eviction_reuse_observed, eviction_private_pins, eviction_scenario;
static bool eviction_mapping_deleted, eviction_pin_reserved;
static sigjmp_buf eviction_clock_error;

#define LockBufHdr transition_lock_header
#define GetBufferDescriptor(id) ((void)(id), transition_buf)
#define ClockSweepTick() 0
#define AddBufferToRing(strategy, buf) ((void)(strategy), (void)(buf))
#define elog(...) siglongjmp(eviction_clock_error, 1)
static BufferDesc *
eviction_clock_sweep(void *strategy, uint32 *buf_state)
{
	BufferDesc *buf;
	uint32 local_buf_state;
	int trycounter;

#include "test_cluster_pcm_clock_sweep.inc"
}
#undef elog
#undef AddBufferToRing
#undef ClockSweepTick
#undef GetBufferDescriptor
#undef LockBufHdr

static bool
eviction_clock_can_reuse(void)
{
	uint32 state;
	BufferDesc *chosen;

	if (sigsetjmp(eviction_clock_error, 1) != 0)
		return false;
	chosen = eviction_clock_sweep(NULL, &state);
	UT_ASSERT(chosen == transition_buf);
	UnlockBufHdr(chosen, state);
	return true;
}

static bool
eviction_mapping_acquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &transition_mapping_lock && mode == LW_EXCLUSIVE);
	UT_ASSERT(!transition_mapping_held);
	transition_mapping_held = true;
	return true;
}

static void
eviction_pin_locked(BufferDesc *buf)
{
	uint32 state = pg_atomic_read_u32(&buf->state);

	UT_ASSERT(eviction_pin_reserved && transition_mapping_held);
	UT_ASSERT_EQ(eviction_private_pins, 0);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(state), 0);
	eviction_private_pins++;
	UnlockBufHdr(buf, state + BUF_REFCOUNT_ONE);
}

static void
eviction_unpin(BufferDesc *buf)
{
	UT_ASSERT_EQ(eviction_private_pins, 1);
	UT_ASSERT(!transition_mapping_held && !transition_content_held);
	UT_ASSERT(eviction_publishes == 4 || eviction_fuses > 0);
	eviction_private_pins--;
	pg_atomic_fetch_sub_u32(&buf->state, BUF_REFCOUNT_ONE);
}

static void
eviction_reserve_pin(void)
{
	UT_ASSERT(!transition_mapping_held);
	UT_ASSERT((pg_atomic_read_u32(&transition_buf->state) & BM_LOCKED) == 0);
	eviction_pin_reserved = true;
}

static ResourceXApplyResult
eviction_prepare(const BufferTag *tag, const ClusterPcmOwnSnapshot *revoking, uint64 r4_generation,
				 uint64 token, ResourceXTargetEvictionPlan *plan)
{
	UT_ASSERT(!transition_mapping_held && !transition_content_held);
	UT_ASSERT_EQ(revoking->flags, PCM_OWN_FLAG_REVOKING);
	plan->tag = *tag;
	plan->cached_ownership_generation = revoking->generation;
	plan->r4_record_generation = r4_generation;
	plan->owner.buffer_ownership_generation = revoking->generation;
	plan->owner.reservation_token = token;
	plan->prepared = true;
	return RESOURCE_X_APPLY_APPLIED;
}

static ResourceXApplyResult
eviction_publish(ResourceXTargetEvictionPlan *plan, bool *retry_pending_out)
{
	*retry_pending_out = false;
	UT_ASSERT(!transition_mapping_held && !transition_content_held);
	UT_ASSERT(plan->local_n_committed && plan->prepared);
	UT_ASSERT(eviction_mapping_deleted);
	UT_ASSERT_EQ(transition_buf->pcm_state, PCM_STATE_N);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(transition_buf->buf_id),
				 plan->cached_ownership_generation + 1);
	if (eviction_clock_can_reuse())
		eviction_reuse_observed++;
	eviction_publishes++;
	if (eviction_scenario == 1)
		return RESOURCE_X_APPLY_BAD_STATE; /* Not a known pending cause. */
	if (eviction_scenario == 3)
		return RESOURCE_X_APPLY_STALE;
	if (eviction_publishes <= 3) {
		*retry_pending_out = true;
		return RESOURCE_X_APPLY_BAD_STATE;
	}
	plan->prepared = false;
	return RESOURCE_X_APPLY_APPLIED;
}

static bool
eviction_wait(LWLock *content_lock, int32 buffer_id, uint32 wait_index, bool *barrier)
{
	UT_ASSERT(content_lock == BufferDescriptorGetContentLock(transition_buf));
	UT_ASSERT_EQ(buffer_id, transition_buf->buf_id);
	UT_ASSERT(!transition_mapping_held && !transition_content_held);
	UT_ASSERT_EQ(eviction_frees, 0);
	UT_ASSERT(eviction_private_pins > 0);
	eviction_sleeps++;
	if (eviction_scenario == 2)
		pg_re_throw();
	return true;
}

static void
eviction_free(BufferDesc *buf)
{
	UT_ASSERT(buf == transition_buf);
	UT_ASSERT_EQ(eviction_private_pins, 0);
	UT_ASSERT_EQ(eviction_publishes, 4);
	eviction_frees++;
}

#define LockBufHdr transition_lock_header
#define LWLockAcquire eviction_mapping_acquire
#define LWLockRelease transition_lock_release
#define BufTableDelete(tag, hash) ((void)(tag), (void)(hash), eviction_mapping_deleted = true)
#define StrategyFreeBuffer eviction_free
#define PinBuffer_Locked eviction_pin_locked
#define UnpinBuffer eviction_unpin
#define ReservePrivateRefCountEntry eviction_reserve_pin
#define ResourceOwnerEnlargeBuffers(owner) eviction_reserve_pin()
#define cluster_gcs_resource_x_target_evict_prepare_exact eviction_prepare
#define cluster_gcs_resource_x_target_evict_publish_exact eviction_publish
#define cluster_gcs_resource_x_target_evict_abort_exact(plan) RESOURCE_X_APPLY_APPLIED
#define cluster_bufmgr_resource_x_fail_closed_current() (eviction_fuses++)
#define cluster_bufmgr_resource_x_writer_report_failure(...) pg_re_throw()
#define cluster_pcm_own_report_bump_failure(...) pg_re_throw()
#define cluster_bufmgr_resource_x_wait_retry eviction_wait
#define elog(...) ((void)0)
#include "test_cluster_pcm_eviction_owner.inc"
#undef elog
#undef cluster_bufmgr_resource_x_wait_retry
#undef cluster_pcm_own_report_bump_failure
#undef cluster_bufmgr_resource_x_writer_report_failure
#undef cluster_bufmgr_resource_x_fail_closed_current
#undef cluster_gcs_resource_x_target_evict_abort_exact
#undef cluster_gcs_resource_x_target_evict_publish_exact
#undef cluster_gcs_resource_x_target_evict_prepare_exact
#undef ResourceOwnerEnlargeBuffers
#undef ReservePrivateRefCountEntry
#undef UnpinBuffer
#undef PinBuffer_Locked
#undef StrategyFreeBuffer
#undef BufTableDelete
#undef LWLockRelease
#undef LWLockAcquire
#undef LockBufHdr

UT_TEST(test_real_eviction_pending_excludes_clock_sweep_and_keeps_one_owner)
{
	int initial_pins;
	int leg;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;

	for (initial_pins = 0; initial_pins <= 1; initial_pins++) {
		for (leg = 0; leg < 4; leg++) {
			BufferDesc buf;
			ClusterPcmOwnEntry entry;
			ClusterPcmOwnSnapshot base;
			BufferTag tag;
			uint32 state;
			volatile bool completed = false;

			transition_fixture(&buf, &entry, &base, false);
			buf.pcm_state = PCM_STATE_X;
			buf.buffer_type = BUF_TYPE_XCUR;
			pg_atomic_write_u32(&entry.flags, 0);
			pg_atomic_write_u32(&buf.state,
								BM_TAG_VALID | BM_VALID | (initial_pins ? BUF_REFCOUNT_ONE : 0));
			eviction_private_pins = initial_pins;
			eviction_publishes = eviction_sleeps = eviction_frees = eviction_fuses = 0;
			eviction_reuse_observed = 0;
			eviction_scenario = leg;
			eviction_mapping_deleted = eviction_pin_reserved = false;
			tag = buf.tag;
			eviction_mapping_acquire(&transition_mapping_lock, LW_EXCLUSIVE);
			state = transition_lock_header(&buf);
			cluster_pcm_own_snapshot_locked(&buf, &base);
			PG_TRY();
			{
				completed = cluster_bufmgr_resource_x_target_evict_locked(
					&buf, &tag, 0, &transition_mapping_lock, state, &base, 77, initial_pins,
					initial_pins == 0);
			}
			PG_CATCH();
			{
				completed = false;
			}
			PG_END_TRY();
			UT_ASSERT(completed == (leg == 0));
			UT_ASSERT_EQ(eviction_reuse_observed, 0);
			UT_ASSERT_EQ(eviction_publishes, leg == 0 ? 4 : 1);
			UT_ASSERT_EQ(eviction_sleeps, leg == 0 ? 3 : leg == 2 ? 1 : 0);
			UT_ASSERT_EQ(eviction_fuses, leg == 0 ? 0 : 1);
			UT_ASSERT_EQ(eviction_frees, initial_pins == 0 && leg == 0 ? 1 : 0);
			UT_ASSERT_EQ(eviction_private_pins, initial_pins);
			UT_ASSERT(!transition_mapping_held && !transition_content_held);
		}
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_gcs_wal_recheck_yields_without_exporting_an_image)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnSnapshot ignored;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	PGIOAlignedBlock output;
	PGIOAlignedBlock sentinel;
	GcsBlockReplyStatus statuses[3];
	ClusterBufmgrGcsCopyRefusal refusal;
	XLogRecPtr copied_lsn;
	uint64 generation;
	int attempt;

	transition_fixture(&buf, &entry, &ignored, false);
	transition_copy_active = true;
	buf.pcm_state = PCM_STATE_X;
	buf.buffer_type = BUF_TYPE_XCUR;
	pg_atomic_write_u32(&entry.flags, 0);
	generation = pg_atomic_read_u64(&entry.generation);
	memset(sentinel.data, 0xa5, BLCKSZ);
	for (attempt = 0; attempt < 3; attempt++) {
		memcpy(output.data, sentinel.data, BLCKSZ);
		copied_lsn = UINT64_C(0xdead);
		transition_wal_changes = 2;
		UT_ASSERT(!cluster_bufmgr_copy_block_for_gcs(buf.tag, &copied_lsn, output.data, &refusal));
		statuses[attempt] = GcsBlockMasterDirectCopyRefusalStatus(refusal);
		UT_ASSERT_EQ(copied_lsn, UINT64_C(0xdead));
		UT_ASSERT_EQ(memcmp(output.data, sentinel.data, BLCKSZ), 0);
		UT_ASSERT_EQ(transition_wal_calls, 2 * (attempt + 1));
		UT_ASSERT_EQ(transition_pin_count, 0);
		UT_ASSERT(!transition_content_held && !transition_mapping_held);
	}

	/* The next actual attempt must earn an image, not merely report success
	 * after the prior retry. The pending dirty image is physically flushed. */
	UT_ASSERT(cluster_bufmgr_copy_block_for_gcs(buf.tag, &copied_lsn, output.data, &refusal));
	UT_ASSERT_EQ(memcmp(output.data, transition_page.data, BLCKSZ), 0);
	UT_ASSERT_EQ(copied_lsn, UINT64_C(0x12370));
	UT_ASSERT_EQ(refusal, CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE);
	UT_ASSERT_EQ(transition_wal_calls, 7);
	UT_ASSERT_EQ(transition_flush_count, 1);
	UT_ASSERT_EQ(transition_pin_count, 0);
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf.state)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entry.generation), generation);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry.flags), 0);
	UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_X);
	UT_ASSERT(!transition_content_held && !transition_mapping_held);
	ClusterPcmOwnArray = saved;
	for (attempt = 0; attempt < 3; attempt++)
		UT_ASSERT_EQ(statuses[attempt], GCS_BLOCK_REPLY_DENIED_PENDING_X);
}

UT_TEST(test_real_gcs_wal_recheck_preserves_invalid_image_and_io_refusals)
{
	int scenario;

	for (scenario = 0; scenario < 4; scenario++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		ClusterPcmOwnSnapshot ignored;
		ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
		ClusterBufmgrGcsCopyRefusal refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
		PGIOAlignedBlock output;
		BufferTag tag;
		XLogRecPtr copied_lsn = UINT64_C(0xdead);
		volatile bool threw = false;
		volatile bool copied = false;

		transition_fixture(&buf, &entry, &ignored, false);
		transition_copy_active = true;
		buf.pcm_state = PCM_STATE_X;
		buf.buffer_type = BUF_TYPE_XCUR;
		pg_atomic_write_u32(&entry.flags, 0);
		tag = buf.tag;
		if (scenario == 0)
			tag.blockNum++;
		else if (scenario == 1)
			buf.pcm_state = PCM_STATE_N;
		else if (scenario == 2)
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR);
		else
			transition_wal_error = true;
		memset(output.data, 0xa5, BLCKSZ);
		PG_TRY();
		{
			copied = cluster_bufmgr_copy_block_for_gcs(tag, &copied_lsn, output.data, &refusal);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		ClusterPcmOwnArray = saved;
		UT_ASSERT(!copied);
		UT_ASSERT_EQ(threw, scenario == 3);
		UT_ASSERT_EQ(copied_lsn, UINT64_C(0xdead));
		UT_ASSERT_EQ(transition_pin_count, 0);
		UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf.state)), 0);
		UT_ASSERT(!transition_content_held && !transition_mapping_held);
		if (scenario != 3)
			UT_ASSERT_EQ(GcsBlockMasterDirectCopyRefusalStatus(refusal),
						 GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER);
	}
	/* The separate exclusive-copy drift retains its hard classification. */
	UT_ASSERT_EQ(
		GcsBlockMasterDirectCopyRefusalStatus(CLUSTER_BUFMGR_GCS_COPY_REFUSAL_HC89_LSN_DRIFT),
		GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER);
}

static void
delivery_route_fixture(BufferDesc *buf, ClusterPcmOwnEntry *entry, ResourceXDecodedFrame *dispatch,
					   ForkNumber fork, bool direct, bool io)
{
	ClusterPcmOwnSnapshot ignored;

	transition_fixture(buf, entry, &ignored, false);
	buf->tag.forkNum = fork;
	buf->pcm_state = PCM_STATE_N;
	buf->buffer_type = BUF_TYPE_CURRENT;
	pg_atomic_write_u64(&entry->generation, 0);
	pg_atomic_write_u64(&entry->reservation_token, direct ? 1 : 0);
	pg_atomic_write_u32(&entry->flags, direct ? PCM_OWN_FLAG_GRANT_PENDING : 0);
	pg_atomic_write_u32(&buf->state,
						BM_TAG_VALID | (io ? BM_IO_IN_PROGRESS : BM_VALID) | BUF_REFCOUNT_ONE);
	memset(transition_page.data, 0, BLCKSZ);
	memset(dispatch, 0, sizeof(*dispatch));
	dispatch->kind = RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP;
	dispatch->common.logical_assertion.resource = buf->tag;
	dispatch->common.logical_assertion.requester_node = 0;
	dispatch->common.resource_formation = 17;
	dispatch->common.master_session_incarnation = 31;
	dispatch->common.assertion_sequence = 41;
	dispatch->common.flags = RESOURCE_X_COMMON_FLAG_REMOTE_ADMISSION;
	memset(&route_target, 0, sizeof(route_target));
	route_direct_generation = 0;
	route_direct_token = direct ? 1 : 0;
	route_observations = route_binds = route_drift = 0;
}

UT_TEST(test_actual_delivery_arm_preserves_foreground_io_initializer)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ResourceXDecodedFrame dispatch;
	ClusterPcmOwnSnapshot actual;
	int buffer_id;

	for (int fork = MAIN_FORKNUM; fork <= INIT_FORKNUM; fork++) {
		for (int phase = 0; phase < 2; phase++) {
			delivery_route_fixture(&buf, &entry, &dispatch, (ForkNumber)fork, true, true);
			if (phase) {
				dispatch.kind = RESOURCE_X_WIRE_ASSERT_X;
				dispatch.common.flags = 0;
			}
			/* Same real descriptor; no fabricated positive buffer lookup. */
			UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot_by_tag(&buf.tag, &buffer_id, &actual),
						 CLUSTER_PCM_OWN_STALE);
			UT_ASSERT_EQ(cluster_bufmgr_pcm_own_direct_init_snapshot_by_tag_exact(
							 &buf.tag, 0, 1, &buffer_id, &actual),
						 CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(gcs_block_resource_x_delivery_arm_exact(1, &dispatch),
						 RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(route_binds, 0);
			UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(buf.buf_id), UINT64_C(0));
			UT_ASSERT_EQ(pg_atomic_read_u32(&buf.state),
						 BM_TAG_VALID | BM_IO_IN_PROGRESS | BUF_REFCOUNT_ONE);
			UT_ASSERT_EQ(cluster_pcm_own_flags_get(buf.buf_id), PCM_OWN_FLAG_GRANT_PENDING);
			UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(buf.buf_id), UINT64_C(1));
		}
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_actual_delivery_arm_keeps_valid_target_and_existing_hold_guards)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ResourceXDecodedFrame dispatch;

	for (int fork = MAIN_FORKNUM; fork <= VISIBILITYMAP_FORKNUM; fork++) {
		delivery_route_fixture(&buf, &entry, &dispatch, (ForkNumber)fork, fork != MAIN_FORKNUM,
							   false);
		UT_ASSERT_EQ(gcs_block_resource_x_delivery_arm_exact(1, &dispatch),
					 RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(route_binds, 1);
		UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(buf.buf_id), UINT64_C(41));
		route_target.buffer_id_plus_one = (uint32)buf.buf_id + 1;
		route_target.generation = 0;
		UT_ASSERT_EQ(gcs_block_resource_x_delivery_arm_exact(1, &dispatch),
					 RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(route_binds, 1);
		pg_atomic_write_u64(&entry.delivery_attempt, 42);
		UT_ASSERT_EQ(gcs_block_resource_x_delivery_arm_exact(1, &dispatch), RESOURCE_X_APPLY_STALE);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_actual_delivery_arm_rejects_unproved_initializer_without_mutation)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry, entry_before;
	ResourceXDecodedFrame dispatch;
	PGAlignedBlock bytes_before;
	uint32 state_before;

	for (int fault = 0; fault < 17; fault++) {
		delivery_route_fixture(&buf, &entry, &dispatch, MAIN_FORKNUM, true, true);
		switch (fault) {
		case 0:
			route_direct_token++;
			break;
		case 1:
			route_direct_generation++;
			break;
		case 2:
			dispatch.common.logical_assertion.resource.blockNum++;
			break;
		case 3:
			pg_atomic_fetch_or_u32(&buf.state, BM_DIRTY);
			break;
		case 4:
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR);
			break;
		case 5:
			pg_atomic_fetch_sub_u32(&buf.state, BUF_REFCOUNT_ONE);
			break;
		case 6:
			((PageHeader)transition_page.data)->pd_upper = BLCKSZ;
			break;
		case 7:
			buf.buffer_type = BUF_TYPE_PI;
			break;
		case 8:
			pg_atomic_fetch_or_u32(&buf.state, BM_VALID);
			break;
		case 9:
			pg_atomic_write_u32(&entry.flags, 0);
			break;
		case 10:
			pg_atomic_write_u64(&entry.writer_activation_token, 1);
			break;
		case 11:
			route_direct_token = 0;
			break; /* No actual bound claim. */
		default:
			route_drift = fault - 11;
			break;
		}
		memcpy(&entry_before, &entry, sizeof(entry));
		memcpy(bytes_before.data, transition_page.data, BLCKSZ);
		state_before = pg_atomic_read_u32(&buf.state);
		UT_ASSERT(gcs_block_resource_x_delivery_arm_exact(1, &dispatch)
				  != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(route_binds, 0);
		UT_ASSERT_EQ(memcmp(&entry, &entry_before, sizeof(entry)), 0);
		UT_ASSERT_EQ(memcmp(transition_page.data, bytes_before.data, BLCKSZ), 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&buf.state), state_before);
	}
	ClusterPcmOwnArray = saved;
}

/* A reader and a direct initializer share GRANT_PENDING's physical shape,
 * but only the latter can delegate its token to its own delivery round. */
static uint64
delivery_reader_fixture(BufferDesc *buf, ClusterPcmOwnEntry *entry, ResourceXDecodedFrame *dispatch,
						ClusterPcmOwnSnapshot *base, ForkNumber fork, bool assertion)
{
	uint64 token = 0;
	uint32 state;

	delivery_route_fixture(buf, entry, dispatch, fork, false, false);
	if (assertion) {
		dispatch->kind = RESOURCE_X_WIRE_ASSERT_X;
		dispatch->common.flags = 0;
	}
	pg_atomic_write_u64(&entry->generation, 6);
	pg_atomic_write_u64(&entry->reservation_token, 2);
	((PageHeader)transition_page.data)->pd_upper = BLCKSZ;
	state = transition_lock_header(buf);
	cluster_pcm_own_snapshot_locked(buf, base);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(buf->buf_id, base->generation,
														 PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(buf, state);
	UT_ASSERT_EQ(token, UINT64_C(3));
	return token;
}

UT_TEST(test_delivery_dispatch_waits_for_ordinary_read_image_owner)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;

	for (int fork = MAIN_FORKNUM; fork <= VISIBILITYMAP_FORKNUM; fork++) {
		for (int assertion = 0; assertion < 2; assertion++) {
			BufferDesc buf;
			ClusterPcmOwnEntry entry, before;
			ResourceXDecodedFrame dispatch;
			ClusterPcmOwnSnapshot base;
			PGAlignedBlock bytes;
			uint64 token, generation = 0;
			uint32 state;

			token = delivery_reader_fixture(&buf, &entry, &dispatch, &base, (ForkNumber)fork,
											assertion != 0);
			memcpy(&before, &entry, sizeof(before));
			memcpy(bytes.data, transition_page.data, BLCKSZ);
			state = pg_atomic_read_u32(&buf.state);
			/* The real dispatch must defer before transport/target binding.
			 * It must not make the subsequent real reader commit return BUSY. */
			UT_ASSERT_EQ(gcs_block_resource_x_delivery_arm_exact(1, &dispatch),
						 RESOURCE_X_APPLY_BAD_STATE);
			UT_ASSERT_EQ(route_binds, 0);
			UT_ASSERT_EQ(memcmp(&entry, &before, sizeof(entry)), 0);
			UT_ASSERT_EQ(pg_atomic_read_u32(&buf.state), state);
			transition_content_held = true;
			UT_ASSERT_EQ(cluster_pcm_own_publish_read_image_exact(&buf, &base, token, &generation),
						 CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_READ_IMAGE);
			UT_ASSERT_EQ(generation, UINT64_C(7));
			UT_ASSERT_EQ(gcs_block_resource_x_delivery_arm_exact(1, &dispatch),
						 RESOURCE_X_APPLY_BAD_STATE);
			UT_ASSERT_EQ(route_binds, 0);
			UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(buf.buf_id), UINT64_C(0));
			UT_ASSERT_EQ(cluster_pcm_own_release_read_image_exact(&buf, &generation),
						 CLUSTER_PCM_OWN_OK);
			transition_content_held = false;
			UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_N);
			UT_ASSERT_EQ(generation, UINT64_C(8));
			/* Re-enter the same staging function after the real read bracket
			 * releases. No new grant or artificial successful abort is seeded. */
			UT_ASSERT_EQ(gcs_block_resource_x_delivery_arm_exact(1, &dispatch),
						 RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(route_binds, 1);
			UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(buf.buf_id), UINT64_C(41));
			UT_ASSERT_EQ(memcmp(transition_page.data, bytes.data, BLCKSZ), 0);
			UT_ASSERT_EQ(pg_atomic_read_u32(&buf.state), state);
		}
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_assert_dispatch_reclaims_only_abandoned_read_image)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;

	for (int fork = MAIN_FORKNUM; fork <= VISIBILITYMAP_FORKNUM; fork++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry, before;
		ResourceXDecodedFrame dispatch;
		ClusterPcmOwnSnapshot base;
		PGAlignedBlock bytes;
		uint64 token, generation = 0;
		uint32 state;

		token = delivery_reader_fixture(&buf, &entry, &dispatch, &base, (ForkNumber)fork, true);
		transition_content_held = true;
		UT_ASSERT_EQ(cluster_pcm_own_publish_read_image_exact(&buf, &base, token, &generation),
					 CLUSTER_PCM_OWN_OK);
		memcpy(&before, &entry, sizeof(before));
		memcpy(bytes.data, transition_page.data, BLCKSZ);
		state = pg_atomic_read_u32(&buf.state);
		/* A live read bracket is not permission to retire or bind anything. */
		UT_ASSERT_EQ(gcs_block_resource_x_delivery_arm_exact(1, &dispatch),
					 RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT_EQ(memcmp(&entry, &before, sizeof(entry)), 0);
		UT_ASSERT_EQ(route_binds, 0);
		/* AbortTransactionBody uses LWLockReleaseAll, not the ordinary
		 * LockBuffer UNLOCK that clears READ_IMAGE before releasing content.
		 * No new SHARE waiter exists to reclaim this leftover marker. */
		transition_content_held = false;
		UT_ASSERT_EQ(gcs_block_resource_x_delivery_arm_exact(1, &dispatch),
					 RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_N);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(buf.buf_id), generation + 1);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(buf.buf_id), token);
		UT_ASSERT_EQ(route_binds, 1);
		UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(buf.buf_id), UINT64_C(41));
		route_target.buffer_id_plus_one = (uint32)buf.buf_id + 1;
		route_target.generation = generation + 1;
		UT_ASSERT_EQ(gcs_block_resource_x_delivery_arm_exact(1, &dispatch),
					 RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(route_binds, 1);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(buf.buf_id), generation + 1);
		UT_ASSERT_EQ(memcmp(transition_page.data, bytes.data, BLCKSZ), 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&buf.state), state);
		UT_ASSERT_EQ(transition_pin_count, 0);
		UT_ASSERT(!transition_mapping_held && !transition_content_held);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_abandoned_read_image_adapter_preserves_all_refusal_boundaries)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;

	for (int fault = 0; fault < 20; fault++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry, before;
		ResourceXDecodedFrame dispatch;
		ClusterPcmOwnSnapshot base, published, cleared, zero = { 0 };
		PGAlignedBlock bytes;
		ClusterPcmOwnResult expected = CLUSTER_PCM_OWN_STALE;
		uint64 token, generation = 0;
		uint32 state;
		int expected_id = 0;

		token = delivery_reader_fixture(&buf, &entry, &dispatch, &base, MAIN_FORKNUM, true);
		transition_content_held = true;
		UT_ASSERT_EQ(cluster_pcm_own_publish_read_image_exact(&buf, &base, token, &generation),
					 CLUSTER_PCM_OWN_OK);
		transition_content_held = false;
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &published), CLUSTER_PCM_OWN_OK);
		switch (fault) {
		case 0:
			transition_content_busy = true;
			expected = CLUSTER_PCM_OWN_BUSY;
			break;
		case 1:
			published.tag.blockNum++;
			break;
		case 2:
			expected_id++;
			break;
		case 3:
			published.generation++;
			break;
		case 4:
			published.reservation_token++;
			break;
		case 5:
			pg_atomic_write_u32(&entry.flags, PCM_OWN_FLAG_GRANT_PENDING);
			break;
		case 6:
			published.flags = PCM_OWN_FLAG_REVOKING;
			expected = CLUSTER_PCM_OWN_CORRUPT;
			break;
		case 7:
			published.writer_activation_token = 1;
			expected = CLUSTER_PCM_OWN_CORRUPT;
			break;
		case 8:
			published.resource_x_activation_generation = 1;
			expected = CLUSTER_PCM_OWN_CORRUPT;
			break;
		case 9:
			pg_atomic_write_u64(&entry.delivery_attempt, 41);
			expected = CLUSTER_PCM_OWN_BUSY;
			break;
		case 10:
			published.semantic_buf_state |= BM_IO_ERROR;
			expected = CLUSTER_PCM_OWN_CORRUPT;
			break;
		case 11:
			published.semantic_buf_state |= BM_IO_IN_PROGRESS;
			expected = CLUSTER_PCM_OWN_BUSY;
			break;
		case 12:
			published.generation = UINT64_MAX;
			expected = CLUSTER_PCM_OWN_EXHAUSTED;
			break;
		case 13:
			published.reservation_token = UINT64_MAX;
			expected = CLUSTER_PCM_OWN_EXHAUSTED;
			break;
		case 14:
			published.buffer_type = (uint8)BUF_TYPE_PI;
			expected = CLUSTER_PCM_OWN_CORRUPT;
			break;
		case 15:
			published.generation = 0;
			expected = CLUSTER_PCM_OWN_CORRUPT;
			break;
		case 16:
			published.reservation_token = 0;
			expected = CLUSTER_PCM_OWN_CORRUPT;
			break;
		case 17:
			ClusterPcmOwnArray = NULL;
			expected = CLUSTER_PCM_OWN_NOT_READY;
			break;
		case 18:
			read_reclaim_drift = 1;
			break;
		case 19:
			read_reclaim_drift = 2;
			break;
		}
		memcpy(&before, &entry, sizeof(before));
		memcpy(bytes.data, transition_page.data, BLCKSZ);
		state = pg_atomic_read_u32(&buf.state);
		memset(&cleared, 0xa5, sizeof(cleared));
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_reclaim_read_image_for_delivery_exact(
						 expected_id, &published, &cleared),
					 expected);
		ClusterPcmOwnArray = &entry;
		if (fault == 18)
			pg_atomic_fetch_add_u64(&before.generation, 1); /* Only the injected other owner. */
		if (fault == 19)
			state |= BM_IO_IN_PROGRESS;
		UT_ASSERT_EQ(memcmp(&entry, &before, sizeof(entry)), 0);
		UT_ASSERT_EQ(memcmp(&cleared, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(memcmp(transition_page.data, bytes.data, BLCKSZ), 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&buf.state), state);
		UT_ASSERT_EQ(transition_pin_count, 0);
		UT_ASSERT(!transition_mapping_held && !transition_content_held);
		UT_ASSERT_EQ(route_binds, 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_abandoned_read_image_cleanup_balances_exception_and_does_not_grant)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ResourceXDecodedFrame dispatch;
	ClusterPcmOwnSnapshot base, published, cleared;
	PGAlignedBlock bytes;
	uint64 token, generation = 0;
	uint32 state;
	volatile bool caught = false;

	token = delivery_reader_fixture(&buf, &entry, &dispatch, &base, MAIN_FORKNUM, true);
	transition_content_held = true;
	UT_ASSERT_EQ(cluster_pcm_own_publish_read_image_exact(&buf, &base, token, &generation),
				 CLUSTER_PCM_OWN_OK);
	transition_content_held = false;
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &published), CLUSTER_PCM_OWN_OK);
	memcpy(bytes.data, transition_page.data, BLCKSZ);
	state = pg_atomic_read_u32(&buf.state);
	read_reclaim_error = true;
	PG_TRY();
	{
		(void)cluster_bufmgr_pcm_own_reclaim_read_image_for_delivery_exact(0, &published, &cleared);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(transition_pin_count, 0);
	UT_ASSERT(!transition_mapping_held && !transition_content_held);
	UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_READ_IMAGE);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), generation);
	UT_ASSERT_EQ(pg_atomic_read_u32(&buf.state), state);
	/* The marker may be reclaimed, but a real entry refusal still prevents
	 * dispatch. Its existing delivery hold must not be silently discarded. */
	route_bind_result = RESOURCE_X_APPLY_STALE;
	UT_ASSERT_EQ(gcs_block_resource_x_delivery_arm_exact(1, &dispatch), RESOURCE_X_APPLY_STALE);
	UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_N);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), generation + 1);
	UT_ASSERT_EQ(cluster_pcm_own_writer_activation_token_get(0), UINT64_C(0));
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_generation_get(0), UINT64_C(0));
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), UINT64_C(41));
	UT_ASSERT_EQ(memcmp(transition_page.data, bytes.data, BLCKSZ), 0);
	UT_ASSERT_EQ(transition_pin_count, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&buf.state), state);
	UT_ASSERT(!transition_mapping_held && !transition_content_held);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_delivery_dispatch_preserves_ordinary_read_abort_then_arms)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;

	for (int fork = MAIN_FORKNUM; fork <= VISIBILITYMAP_FORKNUM; fork++) {
		for (int assertion = 0; assertion < 2; assertion++) {
			BufferDesc buf;
			ClusterPcmOwnEntry entry, before;
			ResourceXDecodedFrame dispatch;
			ClusterPcmOwnSnapshot base;
			uint64 token;

			token = delivery_reader_fixture(&buf, &entry, &dispatch, &base, (ForkNumber)fork,
											assertion != 0);
			memcpy(&before, &entry, sizeof(before));
			UT_ASSERT_EQ(gcs_block_resource_x_delivery_arm_exact(1, &dispatch),
						 RESOURCE_X_APPLY_BAD_STATE);
			UT_ASSERT_EQ(route_binds, 0);
			UT_ASSERT_EQ(memcmp(&entry, &before, sizeof(entry)), 0);
			UT_ASSERT_EQ(cluster_pcm_own_abort_grant_reservation(&buf, &base, token),
						 CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(cluster_pcm_own_flags_get(buf.buf_id), 0);
			UT_ASSERT_EQ(cluster_pcm_own_gen_get(buf.buf_id), base.generation);
			UT_ASSERT_EQ(gcs_block_resource_x_delivery_arm_exact(1, &dispatch),
						 RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(route_binds, 1);
			UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(buf.buf_id), UINT64_C(41));
		}
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_known_new_sidecar_rejects_installed_remote_image)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot ignored;
	ResourceXAcquisitionRef ref;
	ResourceXBufferInstallProof installed;
	ResourceXBufferActivationProof activated;
	int initialized;

	for (initialized = 0; initialized < 2; initialized++) {
		transition_fixture(&buf, &entry, &ignored, false);
		buf.tag.forkNum = VISIBILITYMAP_FORKNUM;
		buf.pcm_state = (uint8)PCM_STATE_X;
		buf.buffer_type = (uint8)BUF_TYPE_XCUR;
		pg_atomic_write_u64(&entry.generation, 1);
		pg_atomic_write_u64(&entry.reservation_token, 1);
		pg_atomic_write_u64(&entry.writer_activation_token, 1);
		pg_atomic_write_u32(&entry.flags, 0);
		memset(transition_page.data, 0, BLCKSZ);
		if (initialized) {
			((PageHeader)transition_page.data)->pd_lower = SizeOfPageHeaderData;
			((PageHeader)transition_page.data)->pd_upper = BLCKSZ;
			((PageHeader)transition_page.data)->pd_special = BLCKSZ;
		}
		memset(&ref, 0, sizeof(ref));
		ref.assertion.resource = buf.tag;
		ref.assertion.requester_node = 3;
		ref.formation = 2;
		ref.acquisition_generation = 1;
		UT_ASSERT_EQ(PageIsNew((Page)transition_page.data), !initialized);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_direct_init_sidecar_by_tag_exact(&ref, 1, 1, false,
																			 &installed, NULL),
					 initialized ? RESOURCE_X_BUFFER_STALE : RESOURCE_X_BUFFER_T2_INSTALLED);
		if (initialized) {
			/* A remote carrier has replaced the known-new image.  This
			 * narrow helper must not turn that into known-new authority. */
			UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_generation_get(0), 0);
			UT_ASSERT_EQ(cluster_pcm_own_writer_activation_token_get(0), 1);
			UT_ASSERT_EQ(installed.ownership_generation, 0);
		} else {
			UT_ASSERT_EQ(installed.ownership_generation, 1);
			UT_ASSERT_EQ(installed.resource_x_activation_generation, 1);
			UT_ASSERT_EQ(cluster_bufmgr_pcm_own_direct_init_sidecar_by_tag_exact(&ref, 1, 1, true,
																				 NULL, &activated),
						 RESOURCE_X_BUFFER_T2_INSTALLED);
			UT_ASSERT_EQ(activated.writer_activation_token, 0);
		}
		UT_ASSERT_EQ(transition_pin_count, 0);
		UT_ASSERT(!transition_mapping_held && !transition_content_held);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_remote_image_t2_t3_use_image_proof_not_page_is_new)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot ignored;
	ResourceXAcquisitionRef ref;
	ResourceXCurrentImage image;
	ResourceXBufferInstallProof installed;
	ResourceXBufferActivationProof activated;
	PGAlignedBlock carrier;
	PGAlignedBlock before;
	int scenario;
	const ResourceXBufferActivationResult expected[]
		= { RESOURCE_X_BUFFER_T2_INSTALLED, RESOURCE_X_BUFFER_CORRUPT, RESOURCE_X_BUFFER_CORRUPT,
			RESOURCE_X_BUFFER_CORRUPT,		RESOURCE_X_BUFFER_ABSENT,  RESOURCE_X_BUFFER_STALE,
			RESOURCE_X_BUFFER_STALE,		RESOURCE_X_BUFFER_CORRUPT };

	for (scenario = 0; scenario < lengthof(expected); scenario++) {
		transition_fixture(&buf, &entry, &ignored, false);
		buf.tag.forkNum = VISIBILITYMAP_FORKNUM;
		buf.pcm_state = (uint8)PCM_STATE_X;
		buf.buffer_type = (uint8)BUF_TYPE_XCUR;
		pg_atomic_write_u64(&entry.generation, 1);
		pg_atomic_write_u64(&entry.reservation_token, 1);
		pg_atomic_write_u64(&entry.writer_activation_token, 1);
		pg_atomic_write_u32(&entry.flags, 0);
		((PageHeader)transition_page.data)->pd_lower = SizeOfPageHeaderData;
		((PageHeader)transition_page.data)->pd_upper = BLCKSZ;
		((PageHeader)transition_page.data)->pd_special = BLCKSZ;
		memcpy(carrier.data, transition_page.data, BLCKSZ);
		memcpy(before.data, transition_page.data, BLCKSZ);
		memset(&ref, 0, sizeof(ref));
		ref.assertion.resource = buf.tag;
		ref.assertion.requester_node = 3;
		ref.formation = 2;
		ref.acquisition_generation = 1;
		memset(&image, 0, sizeof(image));
		image.page_bytes = carrier.data;
		image.image_length = BLCKSZ;
		image.page_lsn = PageGetLSN((Page)carrier.data);
		image.page_scn = ((PageHeader)carrier.data)->pd_block_scn;
		image.page_checksum = cluster_gcs_block_compute_checksum(carrier.data);
		if (scenario == 1)
			image.page_checksum++;
		else if (scenario == 2)
			image.page_lsn++;
		else if (scenario == 3)
			image.page_scn++;
		else if (scenario == 4)
			ref.assertion.resource.relNumber++;
		else if (scenario == 5)
			pg_atomic_write_u64(&entry.writer_activation_token, 2);
		else if (scenario == 6)
			pg_atomic_write_u64(&entry.resource_x_activation_generation, 2);
		else if (scenario == 7)
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR);
		UT_ASSERT(!PageIsNew((Page)transition_page.data));
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_activate_x_by_tag(&ref, &image, &installed),
					 expected[scenario]);
		UT_ASSERT_EQ(memcmp(before.data, transition_page.data, BLCKSZ), 0);
		if (scenario == 0) {
			UT_ASSERT_EQ(installed.ownership_generation, 1);
			UT_ASSERT_EQ(installed.writer_activation_token, 1);
			UT_ASSERT_EQ(installed.resource_x_activation_generation, 1);
			UT_ASSERT_EQ(cluster_bufmgr_pcm_own_activate_x_by_tag(&ref, &image, &installed),
						 RESOURCE_X_BUFFER_ALREADY_INSTALLED);
			UT_ASSERT_EQ(
				cluster_bufmgr_pcm_own_writer_activation_clear_by_tag_exact(&ref, &activated),
				RESOURCE_X_BUFFER_T2_INSTALLED);
			UT_ASSERT_EQ(activated.ownership_generation, 1);
			UT_ASSERT_EQ(activated.writer_activation_token, 0);
			UT_ASSERT_EQ(activated.resource_x_activation_generation, 0);
		} else
			UT_ASSERT_EQ(installed.ownership_generation, 0);
		UT_ASSERT_EQ(transition_pin_count, 0);
		UT_ASSERT(!transition_mapping_held && !transition_content_held);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_installed_claim_shape_is_exact_and_not_a_new_base)
{
	ClusterPcmOwnSnapshot observed;
	ClusterPcmOwnSnapshot changed;
	ResourceXAcquisitionRef ref;
	ResourceXAcquisitionRef wrong_ref;
	int scenario;

	memset(&observed, 0, sizeof(observed));
	observed.tag.spcOid = 1663;
	observed.tag.dbOid = 5;
	observed.tag.relNumber = 16386;
	observed.tag.forkNum = VISIBILITYMAP_FORKNUM;
	observed.pcm_state = (uint8)PCM_STATE_X;
	observed.generation = 1;
	observed.reservation_token = 1;
	observed.writer_activation_token = 1;
	memset(&ref, 0, sizeof(ref));
	ref.assertion.resource = observed.tag;
	ref.assertion.requester_node = 3;
	ref.formation = 2;
	ref.acquisition_generation = 1;
	UT_ASSERT(cluster_pcm_x_resource_x_claim_installed_exact(&ref, &observed, 0, 1));
	observed.resource_x_activation_generation = 1;
	UT_ASSERT(cluster_pcm_x_resource_x_claim_installed_exact(&ref, &observed, 0, 1));
	UT_ASSERT(!cluster_pcm_x_resource_x_claim_installed_exact(&ref, &observed, 1, 1));
	UT_ASSERT(!cluster_pcm_x_resource_x_claim_installed_exact(&ref, &observed, UINT64_MAX, 1));
	UT_ASSERT(!cluster_pcm_x_resource_x_claim_installed_exact(&ref, &observed, 0, 0));
	UT_ASSERT(!cluster_pcm_x_resource_x_claim_installed_exact(&ref, &observed, 0, UINT64_MAX));
	UT_ASSERT(!cluster_pcm_x_resource_x_claim_installed_exact(NULL, &observed, 0, 1));
	UT_ASSERT(!cluster_pcm_x_resource_x_claim_installed_exact(&ref, NULL, 0, 1));
	for (scenario = 0; scenario < 8; scenario++) {
		changed = observed;
		wrong_ref = ref;
		if (scenario == 0)
			changed.tag.blockNum++;
		else if (scenario == 1)
			changed.generation++;
		else if (scenario == 2)
			changed.reservation_token++;
		else if (scenario == 3)
			changed.writer_activation_token++;
		else if (scenario == 4)
			changed.resource_x_activation_generation++;
		else if (scenario == 5)
			changed.flags = PCM_OWN_FLAG_GRANT_PENDING;
		else if (scenario == 6)
			changed.pcm_state = (uint8)PCM_STATE_N;
		else
			wrong_ref.acquisition_generation++;
		UT_ASSERT(!cluster_pcm_x_resource_x_claim_installed_exact(&wrong_ref, &changed, 0, 1));
	}
}

UT_TEST(test_real_source_copy_then_finish_preserves_owned_fence)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot revoking;
	ClusterPcmOwnSnapshot copied;
	ClusterPcmOwnSnapshot retained;
	PGIOAlignedBlock carrier;
	XLogRecPtr lsn = InvalidXLogRecPtr;
	uint64 scn = 0;

	transition_fixture(&buf, &entry, &revoking, true);
	memset(carrier.data, 0, BLCKSZ);
	/* Copy/materialize has flushed the image since REVOKING was captured. */
	pg_atomic_write_u32(&buf.state, BM_TAG_VALID | BM_VALID);
	transition_content_held = true;
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_copy_source_image_exact(&buf, &revoking,
																(Page)transition_page.data, false,
																&copied, carrier.data, &lsn, &scn),
				 CLUSTER_PCM_OWN_OK);
	transition_content_held = false;
	UT_ASSERT_EQ(lsn, UINT64_C(0x12340));
	UT_ASSERT_EQ(scn, 123);
	UT_ASSERT_EQ(memcmp(carrier.data, transition_page.data, BLCKSZ), 0);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_finish_revoke_retain(&buf, &revoking, UINT64_C(0x12340),
															 &retained, NULL),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(retained.generation, 18);
	UT_ASSERT_EQ(retained.pcm_state, PCM_STATE_N);
	UT_ASSERT_EQ(retained.buffer_type, BUF_TYPE_PI);
	UT_ASSERT_EQ(retained.semantic_buf_state, BM_TAG_VALID | BM_VALID);
	UT_ASSERT_EQ(transition_pin_count, 0);
	UT_ASSERT(!transition_mapping_held && !transition_content_held);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_finish_flush_uses_current_image_and_rejects_failures)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot revoking;
	ClusterPcmOwnSnapshot retained;
	int scenario;
	const ClusterPcmOwnResult want[]
		= { CLUSTER_PCM_OWN_OK,		 CLUSTER_PCM_OWN_STALE,	  CLUSTER_PCM_OWN_STALE,
			CLUSTER_PCM_OWN_CORRUPT, CLUSTER_PCM_OWN_CORRUPT, CLUSTER_PCM_OWN_BUSY,
			CLUSTER_PCM_OWN_BUSY };

	for (scenario = 0; scenario < lengthof(want); scenario++) {
		XLogRecPtr lsn = UINT64_C(0x12340);

		transition_fixture(&buf, &entry, &revoking, true);
		if (scenario == 1)
			lsn++;
		else if (scenario == 2)
			pg_atomic_write_u64(&entry.writer_activation_token, 1);
		else if (scenario == 3)
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR);
		else if (scenario == 4)
			buf.buffer_type = (uint8)BUF_TYPE_PI;
		else if (scenario == 5)
			transition_flush_leaves_dirty = true;
		else if (scenario == 6)
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_IN_PROGRESS);
		UT_ASSERT_EQ(
			cluster_bufmgr_pcm_own_finish_revoke_retain(&buf, &revoking, lsn, &retained, NULL),
			want[scenario]);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), scenario == 0 ? 18 : 17);
		UT_ASSERT_EQ(buf.pcm_state, scenario == 0 ? PCM_STATE_N : PCM_STATE_S);
		UT_ASSERT_EQ(transition_pin_count, 0);
		UT_ASSERT(!transition_mapping_held && !transition_content_held);
		UT_ASSERT(!cluster_pcm_x_finish_retain_flush_active);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_finish_failed_flush_rethrows_without_losing_fence)
{
	static BufferDesc buf;
	static ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot revoking;
	ClusterPcmOwnSnapshot retained;
	volatile bool caught = false;

	transition_fixture(&buf, &entry, &revoking, true);
	transition_flush_error = true;
	PG_TRY();
	{
		(void)cluster_bufmgr_pcm_own_finish_revoke_retain(&buf, &revoking, UINT64_C(0x12340),
														  &retained, NULL);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 17);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(transition_pin_count, 0);
	UT_ASSERT(!transition_mapping_held && !transition_content_held);
	UT_ASSERT(!cluster_pcm_x_finish_retain_flush_io_active);
	UT_ASSERT(!cluster_pcm_x_finish_retain_flush_active);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_source_finish_busy_is_owned_wait_not_global_failure)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot revoking;
	ClusterPcmOwnHeldXRevoke held;
	PGIOAlignedBlock before;
	int scenario;

	for (scenario = 0; scenario < 6; scenario++) {
		transition_fixture(&buf, &entry, &revoking, false);
		buf.pcm_state = (uint8)PCM_STATE_X;
		buf.buffer_type = (uint8)BUF_TYPE_XCUR;
		(void)transition_lock_header(&buf);
		cluster_pcm_own_snapshot_locked(&buf, &revoking);
		UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
		memset(&held, 0, sizeof(held));
		held.revoking = revoking;
		held.buffer_id = buf.buf_id;
		held.flags = CLUSTER_PCM_OWN_HELD_X_REVOKE_KNOWN_MASK;
		transition_pin_count = 1;
		transition_base_pins = 1;
		pg_atomic_fetch_add_u32(&buf.state, BUF_REFCOUNT_ONE);
		memcpy(before.data, transition_page.data, BLCKSZ);
		source_finish_fuses = 0;
		source_finish_owner_releases = 0;
		source_finish_defer_calls = 0;
		source_finish_defer_result
			= scenario == 2 ? RESOURCE_X_APPLY_STALE : RESOURCE_X_APPLY_APPLIED;
		if (scenario == 0 || scenario == 2)
			transition_content_busy = true;
		else if (scenario == 1 || scenario == 3)
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_IN_PROGRESS);
		if (scenario == 3)
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR);
		if (scenario == 4)
			pg_atomic_fetch_add_u64(&entry.reservation_token, 1);
		if (scenario == 5) {
			PageSetLSNPreserveOrigin((Page)transition_page.data, UINT64_C(0x12341));
			memcpy(before.data, transition_page.data, BLCKSZ);
		}
		UT_ASSERT_EQ(source_finish_consume(&held, true),
					 scenario < 2 ? RESOURCE_X_APPLY_BAD_STATE : RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(source_finish_fuses, scenario < 2 ? 0 : 1);
		UT_ASSERT_EQ(source_finish_owner_releases, scenario < 2 ? 0 : 1);
		UT_ASSERT_EQ(source_finish_defer_calls, scenario < 3 ? 1 : 0);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), revoking.generation);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
		UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_X);
		UT_ASSERT_EQ(memcmp(before.data, transition_page.data, BLCKSZ), 0);
		UT_ASSERT_EQ(transition_pin_count, scenario < 2 ? 1 : 0);
		UT_ASSERT(!transition_mapping_held && !transition_content_held);
	}
	transition_content_busy = false;
	source_finish_defer_result = RESOURCE_X_APPLY_APPLIED;
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_selected_s_finish_wait_keeps_hard_refusals)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnHeldXRevoke input;
	PGIOAlignedBlock before;

	for (int scenario = 0; scenario < 6; scenario++) {
		memset(&input, 0, sizeof(input));
		transition_fixture(&buf, &entry, &input.revoking, scenario == 4);
		memcpy(before.data, transition_page.data, BLCKSZ);
		source_finish_pair_result
			= scenario == 5 ? RESOURCE_X_APPLY_STALE : RESOURCE_X_APPLY_APPLIED;
		source_finish_fuses = source_finish_owner_releases = source_finish_defer_calls = 0;
		transition_content_busy = scenario == 0 || scenario == 5;
		if (scenario == 1 || scenario == 2)
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_IN_PROGRESS);
		if (scenario == 2)
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR);
		if (scenario == 3)
			pg_atomic_fetch_add_u64(&entry.reservation_token, 1);
		transition_flush_leaves_dirty = scenario == 4;
		UT_ASSERT_EQ(source_finish_consume(&input, false),
					 scenario < 2 ? RESOURCE_X_APPLY_BAD_STATE : RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(source_finish_fuses, scenario < 2 ? 0 : 1);
		UT_ASSERT_EQ(source_finish_defer_calls, 0); /* Never mint an X pin owner. */
		UT_ASSERT_EQ(transition_pin_count, 0);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), input.revoking.generation);
		UT_ASSERT_EQ(memcmp(before.data, transition_page.data, BLCKSZ), 0);
		UT_ASSERT(!transition_content_held && !transition_mapping_held);
	}
	source_finish_pair_result = RESOURCE_X_APPLY_NOT_FOUND;
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_aux_selected_s_waits_for_preexisting_pins_without_own_pin)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnHeldXRevoke input;
	ClusterPcmOwnSnapshot finished;
	PGIOAlignedBlock before;

	for (int scenario = 0; scenario < 3; scenario++) {
		memset(&input, 0, sizeof(input));
		transition_fixture(&buf, &entry, &input.revoking, false);
		buf.tag.forkNum = VISIBILITYMAP_FORKNUM;
		input.revoking.tag = buf.tag;
		pg_atomic_fetch_add_u32(&buf.state, BUF_REFCOUNT_ONE); /* An older foreground pin. */
		if (scenario == 1)
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_IN_PROGRESS);
		if (scenario == 2)
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR | BM_IO_IN_PROGRESS);
		memcpy(before.data, transition_page.data, BLCKSZ);
		source_finish_pair_result = RESOURCE_X_APPLY_APPLIED;
		source_finish_fuses = source_finish_owner_releases = source_finish_defer_calls = 0;
		UT_ASSERT_EQ(source_finish_consume(&input, false),
					 scenario < 2 ? RESOURCE_X_APPLY_BAD_STATE : RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(source_finish_fuses, scenario < 2 ? 0 : 1);
		UT_ASSERT_EQ(source_finish_defer_calls, 0);
		UT_ASSERT_EQ(transition_pin_count, 0);
		UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf.state)), 1);
		UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), input.revoking.generation);
		UT_ASSERT_EQ(memcmp(before.data, transition_page.data, BLCKSZ), 0);
		if (scenario < 2) {
			pg_atomic_fetch_and_u32(&buf.state, ~BM_IO_IN_PROGRESS);
			pg_atomic_fetch_sub_u32(&buf.state, BUF_REFCOUNT_ONE);
			UT_ASSERT_EQ(cluster_bufmgr_pcm_own_finish_revoke_retain(
							 &buf, &input.revoking, UINT64_C(0x12340), &finished, NULL),
						 CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(finished.generation, input.revoking.generation + 1);
			UT_ASSERT_EQ(finished.pcm_state, PCM_STATE_N);
			UT_ASSERT_EQ(transition_pin_count, 0);
			UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf.state)), 0);
		}
		UT_ASSERT(!transition_mapping_held && !transition_content_held);
	}
	source_finish_pair_result = RESOURCE_X_APPLY_NOT_FOUND;
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_source_finish_publishes_only_after_busy_clears_and_preserves_flush_error)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnHeldXRevoke held;
	PGIOAlignedBlock before;

	for (int scenario = 0; scenario < 3; scenario++) {
		memset(&held, 0, sizeof(held));
		transition_fixture(&buf, &entry, &held.revoking, scenario == 2);
		buf.pcm_state = PCM_STATE_X;
		buf.buffer_type = BUF_TYPE_XCUR;
		held.revoking.pcm_state = PCM_STATE_X;
		held.revoking.buffer_type = BUF_TYPE_XCUR;
		held.buffer_id = 0;
		held.flags = CLUSTER_PCM_OWN_HELD_X_REVOKE_KNOWN_MASK;
		transition_base_pins = transition_pin_count = 1;
		pg_atomic_fetch_add_u32(&buf.state, BUF_REFCOUNT_ONE);
		memcpy(before.data, transition_page.data, BLCKSZ);
		source_finish_publishes = source_finish_defer_calls = source_finish_fuses
			= source_finish_owner_releases = 0;
		transition_content_busy = scenario == 0;
		if (scenario == 1)
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_IN_PROGRESS);
		transition_flush_error = scenario == 2;
		UT_ASSERT_EQ(source_finish_consume(&held, true),
					 scenario < 2 ? RESOURCE_X_APPLY_BAD_STATE : RESOURCE_X_APPLY_RECOVERY_BLOCKED);
		UT_ASSERT_EQ(source_finish_publishes, 0);
		if (scenario < 2) {
			UT_ASSERT_EQ(source_finish_fuses, 0);
			UT_ASSERT_EQ(transition_pin_count, 1);
			transition_content_busy = false;
			pg_atomic_fetch_and_u32(&buf.state, ~BM_IO_IN_PROGRESS);
			/* The logical owner claim is independently tested. This invokes
			 * the same production completion used by its winning callback. */
			UT_ASSERT_EQ(source_finish_consume(&held, true), RESOURCE_X_APPLY_APPLIED);
			UT_ASSERT_EQ(source_finish_publishes, 1);
			UT_ASSERT_EQ(source_finish_fuses, 0);
			UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_N);
			UT_ASSERT_EQ(buf.buffer_type, BUF_TYPE_PI);
			UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), held.revoking.generation + 1);
		} else {
			UT_ASSERT(source_finish_fuses > 0);
			UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_X);
			UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), held.revoking.generation);
			UT_ASSERT((pg_atomic_read_u32(&buf.state) & BM_IO_ERROR) != 0);
		}
		UT_ASSERT_EQ(source_finish_owner_releases, 1);
		UT_ASSERT_EQ(transition_pin_count, 0);
		UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf.state)), 0);
		UT_ASSERT_EQ(memcmp(before.data, transition_page.data, BLCKSZ), 0);
		UT_ASSERT(!transition_mapping_held && !transition_content_held);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_source_pin_is_continuous_across_busy_and_owner_adoption)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot revoking;
	ClusterPcmOwnSnapshot retained;
	ClusterPcmOwnHeldXRevoke held;
	ResourceXLocalOwnerHandle owner;
	ClusterPcmOwnFinishRefusal refusal;
	PGIOAlignedBlock before;
	int scenario;
	int retry;

	for (scenario = 0; scenario < 4; scenario++) {
		transition_fixture(&buf, &entry, &revoking, false);
		buf.pcm_state = (uint8)PCM_STATE_X;
		buf.buffer_type = (uint8)BUF_TYPE_XCUR;
		transition_base_pins = transition_pin_count = 1;
		pg_atomic_fetch_add_u32(&buf.state, BUF_REFCOUNT_ONE);
		(void)transition_lock_header(&buf);
		cluster_pcm_own_snapshot_locked(&buf, &revoking);
		UnlockBufHdr(&buf, pg_atomic_read_u32(&buf.state));
		memcpy(before.data, transition_page.data, BLCKSZ);
		memset(&owner, 0, sizeof(owner));
		owner.ref.assertion.resource = revoking.tag;
		owner.buffer_ownership_generation = revoking.generation;
		owner.reservation_token = revoking.reservation_token - 1;
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_adopt_held_x_revoke(0, &owner, &held),
					 CLUSTER_PCM_OWN_OK);
		for (retry = 0; retry < 4; retry++) {
			transition_content_busy = scenario == 0;
			if (scenario != 0)
				pg_atomic_fetch_or_u32(&buf.state, BM_IO_IN_PROGRESS);
			UT_ASSERT_EQ(cluster_bufmgr_pcm_own_finish_held_x_revoke_retain(
							 &held, UINT64_C(0x12340), &retained, &refusal),
						 CLUSTER_PCM_OWN_BUSY);
			UT_ASSERT_EQ(refusal.reason, scenario == 0
											 ? CLUSTER_PCM_OWN_FINISH_REFUSAL_CONTENT_LOCK
											 : CLUSTER_PCM_OWN_FINISH_REFUSAL_IO_IN_PROGRESS);
			UT_ASSERT_EQ(cluster_bufmgr_pcm_own_validate_held_x_revoke(&held), CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(transition_pin_count, 1);
			UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf.state)), 1);
			memset(&held, 0,
				   sizeof(held)); /* The separately tested exact owner transfers responsibility. */
			UT_ASSERT_EQ(cluster_bufmgr_pcm_own_adopt_held_x_revoke(0, &owner, &held),
						 CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(transition_pin_count, 1);
		}
		transition_content_busy = false;
		pg_atomic_fetch_and_u32(&buf.state, ~BM_IO_IN_PROGRESS);
		if (scenario == 2)
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR | BM_IO_IN_PROGRESS);
		else if (scenario == 3)
			pg_atomic_fetch_add_u64(&entry.generation, 1);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_finish_held_x_revoke_retain(&held, UINT64_C(0x12340),
																		&retained, &refusal),
					 scenario < 2	 ? CLUSTER_PCM_OWN_OK
					 : scenario == 2 ? CLUSTER_PCM_OWN_CORRUPT
									 : CLUSTER_PCM_OWN_STALE);
		if (scenario >= 2) {
			UT_ASSERT_EQ(transition_pin_count, 1);
			(void)cluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed(&held);
		}
		UT_ASSERT_EQ(transition_pin_count, 0);
		UT_ASSERT_EQ(held.flags, 0);
		UT_ASSERT_EQ(memcmp(before.data, transition_page.data, BLCKSZ), 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_source_tick_observation_gap_keeps_same_pin_until_completion)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnHeldXRevoke held;
	ResourceXAcquisitionRef ref = { 0 };
	PGIOAlignedBlock before;
	int sample_kind;

	for (sample_kind = PCM_X_SESSION_AUTH_CONNECTION_NOT_READY;
		 sample_kind <= PCM_X_SESSION_AUTH_CONNECTION_TORN + 1; sample_kind++) {
		memset(&held, 0, sizeof(held));
		transition_fixture(&buf, &entry, &held.revoking, false);
		buf.pcm_state = PCM_STATE_X;
		buf.buffer_type = BUF_TYPE_XCUR;
		held.revoking.pcm_state = PCM_STATE_X;
		held.revoking.buffer_type = BUF_TYPE_XCUR;
		held.buffer_id = 0;
		held.flags = CLUSTER_PCM_OWN_HELD_X_REVOKE_KNOWN_MASK;
		transition_base_pins = transition_pin_count = 1;
		pg_atomic_fetch_add_u32(&buf.state, BUF_REFCOUNT_ONE);
		memcpy(before.data, transition_page.data, BLCKSZ);
		source_finish_publishes = source_finish_defer_calls = source_finish_fuses
			= source_finish_owner_releases = 0;
		transition_content_busy = true;
		UT_ASSERT_EQ(source_finish_consume(&held, true), RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT_EQ(source_finish_defer_calls, 1);
		transition_content_busy = false;
		source_tick_held = &held;
		source_tick_sample = sample_kind > PCM_X_SESSION_AUTH_CONNECTION_TORN
								 ? PCM_X_SESSION_AUTH_OK
								 : (PcmXSessionAuthResult)sample_kind;
		source_tick_peer_result = sample_kind > PCM_X_SESSION_AUTH_CONNECTION_TORN
									  ? RESOURCE_X_APPLY_BAD_STATE
									  : RESOURCE_X_APPLY_APPLIED;
		source_tick_runs = source_tick_leaves = source_tick_notifications = 0;
		ref.assertion.resource = held.revoking.tag;
		ref.formation = 17;
		ref.acquisition_generation = 41;
		UT_ASSERT_EQ(cluster_gcs_block_resource_x_source_finish_tick(&ref),
					 RESOURCE_X_APPLY_BAD_STATE);
		UT_ASSERT_EQ(source_tick_runs, 0);
		UT_ASSERT_EQ(source_tick_leaves, 1);
		UT_ASSERT_EQ(source_tick_notifications, 0);
		UT_ASSERT_EQ(transition_pin_count, 1);
		UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf.state)), 1);
		UT_ASSERT_EQ(source_finish_owner_releases, 0);
		UT_ASSERT_EQ(source_finish_publishes, 0);
		UT_ASSERT_EQ(memcmp(before.data, transition_page.data, BLCKSZ), 0);
		UT_ASSERT(!transition_mapping_held && !transition_content_held);
		source_tick_sample = PCM_X_SESSION_AUTH_OK;
		source_tick_peer_result = RESOURCE_X_APPLY_APPLIED;
		UT_ASSERT_EQ(cluster_gcs_block_resource_x_source_finish_tick(&ref),
					 RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(source_tick_runs, 1);
		UT_ASSERT_EQ(source_tick_notifications, 1);
		UT_ASSERT_EQ(source_finish_publishes, 1);
		UT_ASSERT_EQ(source_finish_owner_releases, 1);
		UT_ASSERT_EQ(source_finish_fuses, 0);
		UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_N);
		UT_ASSERT_EQ(buf.buffer_type, BUF_TYPE_PI);
		UT_ASSERT_EQ(transition_pin_count, 0);
		UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf.state)), 0);
		UT_ASSERT_EQ(memcmp(before.data, transition_page.data, BLCKSZ), 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_source_copy_returns_post_replacement_image_type)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot revoking;
	ClusterPcmOwnSnapshot copied;
	PGIOAlignedBlock carrier;
	PGIOAlignedBlock source;
	XLogRecPtr lsn;
	uint64 scn;

	transition_fixture(&buf, &entry, &revoking, false);
	buf.pcm_state = (uint8)PCM_STATE_N;
	buf.buffer_type = (uint8)BUF_TYPE_PI;
	cluster_pcm_own_snapshot_locked(&buf, &revoking);
	memcpy(source.data, transition_page.data, BLCKSZ);
	transition_content_held = true;
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_copy_source_image_exact(
					 &buf, &revoking, (Page)source.data, true, &copied, carrier.data, &lsn, &scn),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(copied.buffer_type, BUF_TYPE_CURRENT);
	UT_ASSERT_EQ(copied.semantic_buf_state, BM_TAG_VALID | BM_VALID);
	UT_ASSERT_EQ(copied.generation, 17);
	UT_ASSERT_EQ(buf.buffer_type, BUF_TYPE_CURRENT);
	transition_content_held = false;
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_snapshot_classifiers_use_the_captured_physical_inputs)
{
	const char *owners[] = { "\ncluster_bufmgr_pcm_own_n_assertion_candidate_exact(",
							 "\ncluster_bufmgr_pcm_own_n_retained_release_inflight_exact(",
							 "\ncluster_bufmgr_pcm_own_n_storage_candidate_exact(",
							 "\ncluster_bufmgr_pcm_own_n_direct_init_candidate_exact(",
							 "\ncluster_bufmgr_pcm_own_s_holder_candidate_exact(" };
	const char *forbidden[] = { "buf->pcm_state", "buf->buffer_type", "(buf_state & BM_",
								"cluster_pcm_own_fence_matches_locked(" };
	char *source = read_bufmgr_source();
	size_t i;
	size_t j;

	for (i = 0; i < lengthof(owners); i++) {
		const char *start = strstr(source, owners[i]);
		const char *end = start != NULL ? strstr(start, "\n}") : NULL;
		const char *capture
			= start != NULL ? strstr(start, "cluster_pcm_own_snapshot_locked(buf, &live)") : NULL;
		const char *compare
			= start != NULL ? strstr(start, "cluster_pcm_own_snapshot_equal_exact(&live,") : NULL;

		UT_ASSERT(start != NULL && end != NULL);
		if (start == NULL || end == NULL)
			continue;
		UT_ASSERT(capture != NULL && capture < end);
		UT_ASSERT(compare != NULL && compare < end);
		for (j = 0; j < lengthof(forbidden); j++) {
			const char *found = strstr(start, forbidden[j]);

			UT_ASSERT(found == NULL || found >= end);
		}
	}
	free(source);
}

static bool
pipe_read_byte(int fd)
{
	char byte;
	ssize_t nread;

	do {
		nread = read(fd, &byte, 1);
	} while (nread < 0 && errno == EINTR);
	return nread == 1;
}

static bool
pipe_write_byte(int fd)
{
	const char byte = 'x';
	ssize_t nwritten;

	do {
		nwritten = write(fd, &byte, 1);
	} while (nwritten < 0 && errno == EINTR);
	return nwritten == 1;
}

typedef struct ParallelStableCoverRace {
	ClusterPcmOwnEntry entries[4];
	pg_atomic_uint32 descriptor_state;
	ClusterPcmOwnResult begin_result;
	ClusterPcmOwnResult commit_result;
	uint64 token;
	uint64 committed_generation;
} ParallelStableCoverRace;

static void
parallel_stable_cover_race_init(ParallelStableCoverRace *race)
{
	int i;

	memset(race, 0, sizeof(*race));
	for (i = 0; i < lengthof(race->entries); i++) {
		pg_atomic_init_u64(&race->entries[i].generation, 0);
		pg_atomic_init_u64(&race->entries[i].reservation_token, 0);
		pg_atomic_init_u64(&race->entries[i].writer_activation_token, 0);
		pg_atomic_init_u64(&race->entries[i].resource_x_activation_generation, 0);
		pg_atomic_init_u32(&race->entries[i].flags, 0);
	}
	pg_atomic_init_u32(&race->descriptor_state, (uint32)PCM_STATE_N);
}

static void
parallel_stable_cover_child(ParallelStableCoverRace *race, int start_fd, int done_fd)
{
	if (!pipe_read_byte(start_fd))
		_exit(10);
	race->begin_result
		= cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &race->token);
	if (race->begin_result == CLUSTER_PCM_OWN_OK)
		race->commit_result
			= cluster_pcm_own_grant_commit_exact(0, 0, race->token, &race->committed_generation);
	if (race->commit_result == CLUSTER_PCM_OWN_OK)
		pg_atomic_write_u32(&race->descriptor_state, (uint32)PCM_STATE_S);
	if (!pipe_write_byte(done_fd))
		_exit(11);
	close(start_fd);
	close(done_fd);
	_exit(0);
}

static ParallelStableCoverRace *
run_parallel_stable_cover_race(void)
{
	ParallelStableCoverRace *race;
	int start_pipe[2] = { -1, -1 };
	int done_pipe[2] = { -1, -1 };
	int status;
	int rc;
	pid_t child;

	race = mmap(NULL, sizeof(*race), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
	UT_ASSERT(race != MAP_FAILED);
	if (race == MAP_FAILED)
		return NULL;
	parallel_stable_cover_race_init(race);
	ClusterPcmOwnArray = race->entries;

	rc = pipe(start_pipe);
	UT_ASSERT_EQ(rc, 0);
	if (rc != 0)
		goto fail;
	rc = pipe(done_pipe);
	UT_ASSERT_EQ(rc, 0);
	if (rc != 0)
		goto fail;

	UT_ASSERT_EQ(pg_atomic_read_u32(&race->descriptor_state), (uint32)PCM_STATE_N);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child < 0)
		goto fail;
	if (child == 0) {
		close(start_pipe[1]);
		close(done_pipe[0]);
		parallel_stable_cover_child(race, start_pipe[0], done_pipe[1]);
	}
	close(start_pipe[0]);
	close(done_pipe[1]);
	UT_ASSERT(pipe_write_byte(start_pipe[1]));
	close(start_pipe[1]);
	UT_ASSERT(pipe_read_byte(done_pipe[0]));
	close(done_pipe[0]);
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
	return race;

fail:
	if (start_pipe[0] >= 0)
		close(start_pipe[0]);
	if (start_pipe[1] >= 0)
		close(start_pipe[1]);
	if (done_pipe[0] >= 0)
		close(done_pipe[0]);
	if (done_pipe[1] >= 0)
		close(done_pipe[1]);
	ClusterPcmOwnArray = NULL;
	munmap(race, sizeof(*race));
	return NULL;
}

/* Linux ARM's real CRC dispatcher logs its CPU choice. Keep DEBUG quiet,
 * but never turn its hardware/software disagreement ERROR into success. */
bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	return elevel >= ERROR;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	abort();
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	UT_ASSERT(size <= sizeof(fake_shmem.bytes));
	*foundPtr = fake_found;
	fake_found = true;
	return fake_shmem.bytes;
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

static void
reset_fixture(void)
{
	memset(&fake_shmem, 0xA5, sizeof(fake_shmem));
	fake_found = false;
	ClusterPcmOwnArray = NULL;
	cluster_pcm_own_shmem_init();
}

static void
assert_entry(uint64 generation, uint64 token, uint32 flags)
{
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[0].generation), generation);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[0].reservation_token), token);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterPcmOwnArray[0].flags), flags);
}

static void
assert_writer_activation(uint64 token)
{
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[0].writer_activation_token), token);
}

static void
assert_resource_x_activation(uint64 generation)
{
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[0].resource_x_activation_generation),
				 generation);
}

UT_TEST(test_shmem_initializes_complete_entry)
{
	int i;

	reset_fixture();
	UT_ASSERT_EQ(cluster_pcm_own_shmem_size(), (Size)NBuffers * sizeof(ClusterPcmOwnEntry));
	UT_ASSERT_EQ(sizeof(ClusterPcmOwnEntry), 48);
	UT_ASSERT_EQ(offsetof(ClusterPcmOwnEntry, resource_x_activation_generation), 24);
	for (i = 0; i < NBuffers; i++) {
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[i].generation), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[i].reservation_token), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[i].writer_activation_token), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[i].resource_x_activation_generation),
					 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterPcmOwnArray[i].flags), 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[i].delivery_attempt), 0);
	}
}

UT_TEST(test_delivery_hold_blocks_idle_target_retag_after_caller_exit)
{
	ClusterPcmOwnEntry before;
	uint64 generation = 99;

	reset_fixture();
	/* No PG caller pin or grant reservation remains.  The instance still
	 * owes delivery into this descriptor.  Test the actual ordinary reuse
	 * primitive, not an emulation of the new delivery API. */
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].delivery_attempt, 41);
	memcpy(&before, &ClusterPcmOwnArray[0], sizeof(before));
	UT_ASSERT(!cluster_pcm_own_gen_bump_checked(0, &generation));
	UT_ASSERT_EQ(generation, 0);
	UT_ASSERT_EQ(memcmp(&before, &ClusterPcmOwnArray[0], sizeof(before)), 0);
}

UT_TEST(test_delivery_hold_exact_t2_preserves_residency_until_real_t3)
{
	uint64 token = 0;
	uint64 committed = 0;

	reset_fixture();
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_begin_exact(0, 0, 41), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_begin_exact(0, 0, 41), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_begin_exact(0, 0, 42), CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_begin_exact(0, 1, 41), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_release_exact(0, 0, 42), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_release_exact(0, 1, 41), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 41);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_grant_begin_exact(0, 0, 42, &token),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_grant_begin_exact(0, 0, 41, &token), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_release_exact(0, 0, 41), CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(cluster_pcm_own_writer_grant_commit_exact(0, 0, token, &committed),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 1);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 41);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_bind_exact(0, 1, token, 51),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_release_exact(0, 1, 41), CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_clear_exact(0, 1, token, 51),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_writer_activation_token_get(0), 0);
	UT_ASSERT(!cluster_pcm_own_gen_bump_checked(0, NULL));
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_release_exact(0, 0, 41), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_release_exact(0, 1, 41), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_release_exact(0, 1, 41), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT(cluster_pcm_own_gen_bump_checked(0, &committed));
	UT_ASSERT_EQ(committed, 2);
}

UT_TEST(test_delivery_hold_prevents_failed_direct_init_owner_from_aborting_delivery)
{
	uint64 token = 0;
	uint64 committed = 0;
	ClusterPcmOwnEntry before;

	reset_fixture();
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	/* Seed only the new residency field.  The production abort owner must
	 * not discard a token handed to instance-owned delivery after ERROR. */
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].delivery_attempt, 41);
	memcpy(&before, &ClusterPcmOwnArray[0], sizeof(before));
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 0, token, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(memcmp(&before, &ClusterPcmOwnArray[0], sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, token, &committed), CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(committed, 0);
	UT_ASSERT_EQ(memcmp(&before, &ClusterPcmOwnArray[0], sizeof(before)), 0);
}

UT_TEST(test_delivery_hold_rejects_unrelated_s_or_x_reservation)
{
	uint64 token = 99;
	ClusterPcmOwnEntry before;

	reset_fixture();
	UT_ASSERT_EQ(cluster_pcm_own_delivery_hold_begin_exact(0, 0, 41), CLUSTER_PCM_OWN_OK);
	memcpy(&before, &ClusterPcmOwnArray[0], sizeof(before));
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(token, 0);
	UT_ASSERT_EQ(memcmp(&before, &ClusterPcmOwnArray[0], sizeof(before)), 0);
}

UT_TEST(test_real_delivery_residency_owns_exact_heap_vm_fsm_descriptor)
{
	int fork;
	int pending;

	for (fork = MAIN_FORKNUM; fork <= VISIBILITYMAP_FORKNUM; fork++) {
		for (pending = 0; pending <= 1; pending++) {
			BufferDesc buf;
			ClusterPcmOwnEntry entry;
			ClusterPcmOwnSnapshot before;
			ClusterPcmOwnSnapshot after;
			ClusterPcmOwnSnapshot wrong;
			uint64 token = 21;
			uint64 generation = 0;
			uint32 flags = 0;
			ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;

			transition_fixture(&buf, &entry, &before, false);
			buf.tag.forkNum = fork;
			buf.pcm_state = PCM_STATE_N;
			buf.buffer_type = BUF_TYPE_CURRENT;
			pg_atomic_write_u32(&entry.flags, pending ? PCM_OWN_FLAG_GRANT_PENDING : 0);
			cluster_pcm_own_snapshot_locked(&buf, &before);
			wrong = before;
			wrong.tag.blockNum++;
			UT_ASSERT_EQ(cluster_bufmgr_pcm_own_delivery_hold_begin_exact(&buf, &wrong, 41),
						 CLUSTER_PCM_OWN_STALE);
			UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
			UT_ASSERT_EQ(cluster_bufmgr_pcm_own_delivery_hold_begin_exact(&buf, &before, 41),
						 CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(cluster_bufmgr_pcm_own_delivery_hold_begin_exact(&buf, &before, 41),
						 CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(
				cluster_bufmgr_pcm_own_delivery_snapshot_exact(&buf, &before.tag, 42, &after),
				CLUSTER_PCM_OWN_STALE);
			UT_ASSERT_EQ(
				cluster_bufmgr_pcm_own_delivery_snapshot_exact(&buf, &before.tag, 41, &after),
				CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
			/* Actual BufferDesc eviction owner, with zero foreground pins. */
			UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&buf.state)), 0);
			UT_ASSERT_EQ(cluster_pcm_own_eviction_commit_locked(&buf, &before, &generation, &flags),
						 CLUSTER_PCM_OWN_BUSY);
			UT_ASSERT(BufferTagsEqual(&buf.tag, &before.tag));
			UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_N);
			if (pending)
				UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 17, token,
																	 PCM_OWN_FLAG_GRANT_PENDING),
							 CLUSTER_PCM_OWN_BUSY);
			else
				UT_ASSERT_EQ(
					cluster_bufmgr_pcm_own_delivery_begin_x_reservation(&buf, &before, 41, &token),
					CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(cluster_pcm_own_writer_grant_commit_exact(0, 17, token, &generation),
						 CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(generation, 18);
			buf.pcm_state = PCM_STATE_X;
			buf.buffer_type = BUF_TYPE_XCUR;
			UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_bind_exact(0, 18, token, 41),
						 CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_clear_exact(0, 18, token, 41),
						 CLUSTER_PCM_OWN_OK);
			cluster_pcm_own_snapshot_locked(&buf, &after);
			wrong = after;
			wrong.generation--;
			UT_ASSERT_EQ(
				cluster_bufmgr_pcm_own_delivery_hold_release_exact(&buf, &wrong, 41, token),
				CLUSTER_PCM_OWN_STALE);
			UT_ASSERT_EQ(
				cluster_bufmgr_pcm_own_delivery_hold_release_exact(&buf, &after, 42, token),
				CLUSTER_PCM_OWN_STALE);
			UT_ASSERT_EQ(
				cluster_bufmgr_pcm_own_delivery_hold_release_exact(&buf, &after, 41, token - 1),
				CLUSTER_PCM_OWN_STALE); /* No first-release monotone exception. */
			UT_ASSERT_EQ(
				cluster_bufmgr_pcm_own_delivery_hold_release_exact(&buf, &after, 41, token),
				CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
			/* A completed T3 may be replayed after physical release but before
			 * the entry owner cleared its delivery debt. Only the same exact
			 * terminal incarnation is a release no-op. */
			UT_ASSERT_EQ(
				cluster_bufmgr_pcm_own_delivery_hold_release_exact(&buf, &after, 41, token),
				CLUSTER_PCM_OWN_OK);
			UT_ASSERT_EQ(
				cluster_bufmgr_pcm_own_delivery_hold_release_exact(&buf, &wrong, 41, token),
				CLUSTER_PCM_OWN_STALE);
			/* Actual post-release reservation + refused-plan abort. No
			 * generation/bytes change, but the token is intentionally monotone. */
			{
				uint64 cancelled_token = 0;
				ClusterPcmOwnSnapshot cancelled;

				UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(
								 0, after.generation, PCM_OWN_FLAG_REVOKING, &cancelled_token),
							 CLUSTER_PCM_OWN_OK);
				UT_ASSERT_EQ(cancelled_token, after.reservation_token + 1);
				cluster_pcm_own_snapshot_locked(&buf, &cancelled);
				UT_ASSERT_EQ(
					cluster_bufmgr_pcm_own_delivery_hold_release_exact(&buf, &cancelled, 41, token),
					CLUSTER_PCM_OWN_INVALID);
				UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(
								 0, after.generation, cancelled_token, PCM_OWN_FLAG_REVOKING),
							 CLUSTER_PCM_OWN_OK);
				cluster_pcm_own_snapshot_locked(&buf, &cancelled);
				UT_ASSERT_EQ(cancelled.generation, after.generation);
				UT_ASSERT_EQ(cancelled.pcm_state, PCM_STATE_X);
				UT_ASSERT_EQ(cancelled.flags, 0);
				UT_ASSERT_EQ(cancelled.reservation_token, cancelled_token);
				UT_ASSERT_EQ(
					cluster_bufmgr_pcm_own_delivery_hold_release_exact(&buf, &cancelled, 41, token),
					CLUSTER_PCM_OWN_OK);
				UT_ASSERT_EQ(cluster_bufmgr_pcm_own_delivery_hold_release_exact(
								 &buf, &cancelled, 41, cancelled_token + 1),
							 CLUSTER_PCM_OWN_STALE);
				UT_ASSERT_EQ(cluster_bufmgr_pcm_own_delivery_hold_release_exact(&buf, &cancelled,
																				41, UINT64_MAX),
							 CLUSTER_PCM_OWN_INVALID);
			}
			UT_ASSERT_EQ(transition_pin_count, 0);
			UT_ASSERT(!transition_mapping_held && !transition_content_held);
			ClusterPcmOwnArray = saved;
		}
	}
}

UT_TEST(test_resource_x_activation_binding_is_exact_and_legacy_closed)
{
	uint64 committed_generation = 0;
	uint64 writer_token = 0;

	reset_fixture();
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &writer_token),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(
		cluster_pcm_own_writer_grant_commit_exact(0, 0, writer_token, &committed_generation),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed_generation, 1);
	assert_writer_activation(writer_token);
	assert_resource_x_activation(0);

	UT_ASSERT_EQ(
		cluster_pcm_own_resource_x_activation_bind_exact(0, committed_generation, writer_token, 0),
		CLUSTER_PCM_OWN_INVALID);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_bind_exact(0, committed_generation + 1,
																  writer_token, 41),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_bind_exact(0, committed_generation,
																  writer_token + 1, 41),
				 CLUSTER_PCM_OWN_STALE);
	assert_resource_x_activation(0);

	UT_ASSERT_EQ(
		cluster_pcm_own_resource_x_activation_bind_exact(0, committed_generation, writer_token, 41),
		CLUSTER_PCM_OWN_OK);
	assert_writer_activation(writer_token);
	assert_resource_x_activation(41);
	UT_ASSERT_EQ(
		cluster_pcm_own_resource_x_activation_bind_exact(0, committed_generation, writer_token, 41),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(
		cluster_pcm_own_resource_x_activation_bind_exact(0, committed_generation, writer_token, 42),
		CLUSTER_PCM_OWN_STALE);
	assert_resource_x_activation(41);

	/* A generic legacy clear must not open a target Resource-X fence, and
	 * descriptor reuse cannot erase either live activation field. */
	UT_ASSERT_EQ(
		cluster_pcm_own_writer_activation_clear_exact(0, committed_generation, writer_token),
		CLUSTER_PCM_OWN_STALE);
	UT_ASSERT(!cluster_pcm_own_gen_bump_checked(0, NULL));
	assert_writer_activation(writer_token);
	assert_resource_x_activation(41);

	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_clear_exact(0, committed_generation,
																   writer_token, 42),
				 CLUSTER_PCM_OWN_STALE);
	assert_writer_activation(writer_token);
	assert_resource_x_activation(41);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_activation_clear_exact(0, committed_generation,
																   writer_token, 41),
				 CLUSTER_PCM_OWN_OK);
	assert_resource_x_activation(0);
	assert_writer_activation(0);
}

UT_TEST(test_resource_x_reconfig_neutralize_is_generation_exact_and_nonblocking)
{
	ClusterPcmOwnSnapshot live;
	BufferTag tag;
	uint64 committed_generation = 0;
	uint64 neutral_generation = 0;
	uint64 writer_token = 0;
	char *source;
	const char *neutralize;
	const char *neutralize_end;

	reset_fixture();
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &writer_token),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(
		cluster_pcm_own_writer_grant_commit_exact(0, 0, writer_token, &committed_generation),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(
		cluster_pcm_own_resource_x_activation_bind_exact(0, committed_generation, writer_token, 41),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_neutralize_exact(0, committed_generation, writer_token,
															 42, &neutral_generation),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(neutral_generation, 0);
	assert_writer_activation(writer_token);
	assert_resource_x_activation(41);
	UT_ASSERT_EQ(cluster_pcm_own_resource_x_neutralize_exact(0, committed_generation, writer_token,
															 41, &neutral_generation),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(neutral_generation, committed_generation + 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterPcmOwnArray[0].generation), neutral_generation);
	assert_writer_activation(0);
	assert_resource_x_activation(0);

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 1;
	tag.relNumber = 100;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = 72;
	memset(&live, 0, sizeof(live));
	live.tag = tag;
	live.generation = committed_generation;
	live.reservation_token = writer_token;
	live.writer_activation_token = writer_token;
	live.resource_x_activation_generation = 41;
	live.pcm_state = (uint8)PCM_STATE_X;
	UT_ASSERT(cluster_pcm_x_resource_x_r8_snapshot_exact(&tag, 17, 41, &live));
	UT_ASSERT(!cluster_pcm_x_resource_x_r8_snapshot_exact(&tag, 0, 41, &live));
	UT_ASSERT(!cluster_pcm_x_resource_x_r8_snapshot_exact(&tag, 17, 42, &live));

	source = read_bufmgr_source();
	neutralize = strstr(source, "\ncluster_bufmgr_resource_x_neutralize_exact(");
	neutralize_end = neutralize != NULL ? strstr(neutralize, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(neutralize);
	UT_ASSERT_NOT_NULL(neutralize_end);
	if (neutralize != NULL && neutralize_end != NULL) {
		const char *quarantine = strstr(neutralize, "buf->pcm_state = (uint8)PCM_STATE_N");
		const char *raw_clear = strstr(neutralize, "cluster_pcm_own_resource_x_neutralize_exact(");

		UT_ASSERT_NOT_NULL(strstr(source, "PGRAC_PCM_X_FENCE_TERMINAL_OWNER(R8_NEUTRALIZE, tag,"));
		UT_ASSERT_NOT_NULL(strstr(neutralize, "BufTableLookup"));
		UT_ASSERT_NOT_NULL(strstr(neutralize, "cluster_pcm_x_resource_x_r8_snapshot_exact("));
		UT_ASSERT_NOT_NULL(strstr(neutralize, "cluster_pcm_own_resource_x_neutralize_exact("));
		UT_ASSERT_NOT_NULL(strstr(neutralize, "buf->pcm_state = (uint8)PCM_STATE_N"));
		UT_ASSERT_NOT_NULL(strstr(neutralize, "buf->buffer_type = (uint8)BUF_TYPE_PI"));
		UT_ASSERT_NOT_NULL(raw_clear);
		UT_ASSERT(quarantine != NULL && quarantine < raw_clear && raw_clear < neutralize_end);
		{
			const char *content_lock = strstr(neutralize, "BufferDescriptorGetContentLock");

			UT_ASSERT(content_lock == NULL || content_lock >= neutralize_end);
		}
	}
	free(source);
}

UT_TEST(test_writer_activation_fence_blocks_revoke_until_exact_clear)
{
	uint64 committed_generation = 0;
	uint64 revoke_token = UINT64_MAX;
	uint64 writer_token = 0;

	reset_fixture();
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &writer_token),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(
		cluster_pcm_own_writer_grant_commit_exact(0, 0, writer_token, &committed_generation),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed_generation, 1);
	assert_entry(1, writer_token, 0);
	assert_writer_activation(writer_token);

	/* The committed X and its not-yet-activated writer are one shared
	 * linearization.  A downgrade cannot reserve the same tuple. */
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 1, PCM_OWN_FLAG_REVOKING, &revoke_token),
		CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(revoke_token, 0);
	assert_entry(1, writer_token, 0);
	assert_writer_activation(writer_token);
	UT_ASSERT(!cluster_pcm_own_gen_bump_checked(0, NULL));

	pg_atomic_write_u64(&ClusterPcmOwnArray[0].writer_activation_token, writer_token + 1);
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 1, PCM_OWN_FLAG_REVOKING, &revoke_token),
		CLUSTER_PCM_OWN_CORRUPT);
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].writer_activation_token, writer_token);

	/* A delayed or wrong clear is an exact no-op. */
	UT_ASSERT_EQ(cluster_pcm_own_writer_activation_clear_exact(0, 0, writer_token),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_writer_activation_clear_exact(0, 1, writer_token + 1),
				 CLUSTER_PCM_OWN_STALE);
	assert_writer_activation(writer_token);

	UT_ASSERT_EQ(cluster_pcm_own_writer_activation_clear_exact(0, 1, writer_token),
				 CLUSTER_PCM_OWN_OK);
	assert_writer_activation(0);
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 1, PCM_OWN_FLAG_REVOKING, &revoke_token),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(revoke_token, writer_token + 1);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 1, revoke_token, PCM_OWN_FLAG_REVOKING),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT(cluster_pcm_own_gen_bump_checked(0, &committed_generation));
	UT_ASSERT_EQ(committed_generation, 2);
	assert_writer_activation(0);
}

UT_TEST(test_begin_abort_is_exact_and_monotonic)
{
	uint64 token = UINT64_MAX;

	reset_fixture();
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(token, 1);
	assert_entry(0, 1, PCM_OWN_FLAG_GRANT_PENDING);

	/* A second begin cannot overwrite or advance the live token. */
	token = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, 1, PCM_OWN_FLAG_GRANT_PENDING);

	/* Old/wrong cleanup is a strict no-op. */
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 0, 2, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 1, 1, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_STALE);
	assert_entry(0, 1, PCM_OWN_FLAG_GRANT_PENDING);

	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 0, 1, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_OK);
	assert_entry(0, 1, 0);

	/* A delayed duplicate abort cannot clear the next reservation. */
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(token, 2);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 0, 1, PCM_OWN_FLAG_GRANT_PENDING),
				 CLUSTER_PCM_OWN_STALE);
	assert_entry(0, 2, PCM_OWN_FLAG_GRANT_PENDING);
}

UT_TEST(test_invalid_live_flag_shapes_are_corrupt_not_busy)
{
	static const char *const classifier_contract[]
		= { "cluster_pcm_own_reservation_token_get", "cluster_pcm_own_flags_get",
			"cluster_pcm_own_classify_live_flags", "live_result != CLUSTER_PCM_OWN_OK",
			"return live_result" };
	char *source;
	uint64 token = UINT64_MAX;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 7);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags,
						PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, 7, PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 7);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, (uint32)0x4);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, 7, (uint32)0x4);

	/* Even a recognized singleton flag is corrupt without a published token. */
	reset_fixture();
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, 0, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(0, 0), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(0, 7), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(PCM_OWN_FLAG_GRANT_PENDING, 7),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(PCM_OWN_FLAG_REVOKING, 7),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(
		cluster_pcm_own_classify_live_flags(PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING, 7),
		CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_own_classify_live_flags(PCM_OWN_FLAG_GRANT_PENDING, 0),
				 CLUSTER_PCM_OWN_CORRUPT);

	source = read_bufmgr_source();
	assert_ordered_in_function(source, "\ncluster_pcm_own_bump_failure(", "\nstatic ",
							   classifier_contract, lengthof(classifier_contract));
	free(source);
}

UT_TEST(test_remote_s_holder_pending_grant_is_retryable_busy)
{
	ClusterPcmOwnSnapshot snapshot;

	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.pcm_state = (uint8)PCM_STATE_N;
	snapshot.flags = PCM_OWN_FLAG_GRANT_PENDING;
	snapshot.reservation_token = 1;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_pending_grant_result(&snapshot),
				 CLUSTER_PCM_OWN_BUSY);

	/* The narrow retry classification must not hide malformed reservation
	 * evidence or an already active writer/Resource-X fence. */
	snapshot.reservation_token = 0;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_pending_grant_result(&snapshot),
				 CLUSTER_PCM_OWN_CORRUPT);
	snapshot.reservation_token = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_pending_grant_result(&snapshot),
				 CLUSTER_PCM_OWN_CORRUPT);
	snapshot.reservation_token = 1;
	snapshot.generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_pending_grant_result(&snapshot),
				 CLUSTER_PCM_OWN_CORRUPT);
	snapshot.generation = 0;
	snapshot.writer_activation_token = 1;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_pending_grant_result(&snapshot),
				 CLUSTER_PCM_OWN_CORRUPT);
	snapshot.writer_activation_token = 0;
	snapshot.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_pending_grant_result(&snapshot),
				 CLUSTER_PCM_OWN_CORRUPT);

	/* Clean S remains owned by the existing exact candidate path. */
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.pcm_state = (uint8)PCM_STATE_S;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_pending_grant_result(&snapshot),
				 CLUSTER_PCM_OWN_INVALID);
}

UT_TEST(test_real_remote_s_candidate_waits_for_exact_local_reservation)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before, after;
	uint32 flags[] = { PCM_OWN_FLAG_GRANT_PENDING, PCM_OWN_FLAG_REVOKING };
	uint64 token;
	uint32 state;

	for (int i = 0; i < lengthof(flags); i++) {
		n_predecessor_fixture(&buf, &entry, BUF_TYPE_SCUR);
		buf.pcm_state = PCM_STATE_S;
		buf.tag.forkNum = MAIN_FORKNUM;
		buf.tag.blockNum = 12516;
		state = transition_lock_header(&buf);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 48, flags[i], &token),
					 CLUSTER_PCM_OWN_OK);
		UnlockBufHdr(&buf, state);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &before), CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_s_holder_candidate_exact(&buf, &before),
					 CLUSTER_PCM_OWN_BUSY);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &after), CLUSTER_PCM_OWN_OK);
		UT_ASSERT(cluster_pcm_own_snapshot_equal_exact(&before, &after));
		UT_ASSERT((pg_atomic_read_u32(&buf.state) & BM_LOCKED) == 0);
		state = transition_lock_header(&buf);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, 48, token, flags[i]),
					 CLUSTER_PCM_OWN_OK);
		UnlockBufHdr(&buf, state);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &after), CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_s_holder_candidate_exact(&buf, &after),
					 CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_s_holder_candidate_exact(&buf, &before),
					 CLUSTER_PCM_OWN_STALE);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_real_remote_s_candidate_preserves_corruption_and_observation_guards)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot before, after;

	for (int leg = 0; leg < 16; leg++) {
		n_predecessor_fixture(&buf, &entry, BUF_TYPE_SCUR);
		buf.pcm_state = PCM_STATE_S;
		pg_atomic_write_u32(&entry.flags, PCM_OWN_FLAG_GRANT_PENDING);
		switch (leg) {
		case 0:
			pg_atomic_write_u32(&entry.flags, 3);
			break;
		case 1:
			pg_atomic_write_u32(&entry.flags, 4);
			break;
		case 2:
			pg_atomic_write_u64(&entry.reservation_token, 0);
			break;
		case 3:
			pg_atomic_write_u64(&entry.reservation_token, UINT64_MAX);
			break;
		case 4:
			pg_atomic_write_u64(&entry.generation, UINT64_MAX);
			break;
		case 5:
			pg_atomic_write_u64(&entry.writer_activation_token, 48);
			break;
		case 6:
			pg_atomic_write_u64(&entry.resource_x_activation_generation, 1);
			break;
		case 7:
			buf.buffer_type = BUF_TYPE_PI;
			break;
		case 8:
			pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR);
			break;
		case 9:
			pg_atomic_fetch_and_u32(&buf.state, ~BM_VALID);
			break;
		case 12:
		case 13:
		case 14:
		case 15:
			pg_atomic_write_u32(&entry.flags, 0);
			pg_atomic_fetch_or_u32(&buf.state, leg == 12   ? BM_IO_IN_PROGRESS
											   : leg == 13 ? BM_DIRTY
											   : leg == 14 ? BM_JUST_DIRTIED
														   : BM_CHECKPOINT_NEEDED);
			break;
		default:
			break;
		}
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &before), CLUSTER_PCM_OWN_OK);
		if (leg == 10)
			before.tag.blockNum++;
		if (leg == 11)
			before.generation++;
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_s_holder_candidate_exact(&buf, &before),
					 leg >= 12	 ? CLUSTER_PCM_OWN_BUSY
					 : leg >= 10 ? CLUSTER_PCM_OWN_STALE
								 : CLUSTER_PCM_OWN_CORRUPT);
		UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &after), CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(after.flags, pg_atomic_read_u32(&entry.flags));
		UT_ASSERT_EQ(after.reservation_token, pg_atomic_read_u64(&entry.reservation_token));
		UT_ASSERT((pg_atomic_read_u32(&buf.state) & BM_LOCKED) == 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_remote_s_holder_stable_n_replay_requires_exact_idle_tuple)
{
	ClusterPcmOwnSnapshot snapshot;

	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.pcm_state = (uint8)PCM_STATE_N;
	snapshot.generation = 2;
	snapshot.reservation_token = 2;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_stable_n_result(&snapshot), CLUSTER_PCM_OWN_OK);

	/* Stable-N idempotence is narrower than the general N assertion shape:
	 * it requires the finite monotonic token left by a committed revoke and
	 * rejects every live or residual ownership axis instead of manufacturing
	 * replay identity. */
	snapshot.generation = 0;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_stable_n_result(&snapshot), CLUSTER_PCM_OWN_STALE);
	snapshot.generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_stable_n_result(&snapshot), CLUSTER_PCM_OWN_CORRUPT);
	snapshot.generation = 2;
	snapshot.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_stable_n_result(&snapshot), CLUSTER_PCM_OWN_BUSY);
	snapshot.flags = 0;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_stable_n_result(&snapshot), CLUSTER_PCM_OWN_OK);
	snapshot.reservation_token = 0;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_stable_n_result(&snapshot), CLUSTER_PCM_OWN_STALE);
	snapshot.reservation_token = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_stable_n_result(&snapshot), CLUSTER_PCM_OWN_CORRUPT);
	snapshot.reservation_token = 2;
	snapshot.writer_activation_token = 1;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_stable_n_result(&snapshot), CLUSTER_PCM_OWN_CORRUPT);
	snapshot.writer_activation_token = 0;
	snapshot.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_stable_n_result(&snapshot), CLUSTER_PCM_OWN_CORRUPT);
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.pcm_state = (uint8)PCM_STATE_S;
	snapshot.generation = 2;
	UT_ASSERT_EQ(cluster_pcm_x_remote_s_holder_stable_n_result(&snapshot), CLUSTER_PCM_OWN_INVALID);
}

UT_TEST(test_grant_commit_is_exact_and_bumps_once)
{
	uint64 token;
	uint64 committed = UINT64_MAX;

	reset_fixture();
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);

	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, token + 1, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(0, token, PCM_OWN_FLAG_GRANT_PENDING);

	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, token, &committed), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 1);
	assert_entry(1, token, 0);

	committed = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, token, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(1, token, 0);

	/* A competing well-formed lifecycle is BUSY; a malformed tuple is
	 * corruption.  Neither may be flattened into a retryable stale result. */
	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 9);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, 9, &committed), CLUSTER_PCM_OWN_BUSY);
	assert_entry(0, 9, PCM_OWN_FLAG_REVOKING);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags,
						PCM_OWN_FLAG_REVOKING | PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 0, 9, &committed), CLUSTER_PCM_OWN_CORRUPT);
	assert_entry(0, 9, PCM_OWN_FLAG_REVOKING | PCM_OWN_FLAG_GRANT_PENDING);
}

UT_TEST(test_s_revoke_handoff_reuses_exact_token_and_bumps_once)
{
	uint64 committed = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_REVOKING, &token),
				 CLUSTER_PCM_OWN_OK);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	/* Stale identities cannot steal or rewrite the source revoke lifecycle. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 8, token), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token + 1),
				 CLUSTER_PCM_OWN_STALE);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	/* Handoff changes only the role of the same source lifecycle. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token), CLUSTER_PCM_OWN_OK);
	assert_entry(7, token, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token), CLUSTER_PCM_OWN_OK);
	assert_entry(7, token, PCM_OWN_FLAG_GRANT_PENDING);

	/* Malformed live tuples are corruption, never a stale/duplicate handoff. */
	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, token);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags,
						PCM_OWN_FLAG_REVOKING | PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token),
				 CLUSTER_PCM_OWN_CORRUPT);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING | PCM_OWN_FLAG_GRANT_PENDING);
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 0);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token),
				 CLUSTER_PCM_OWN_CORRUPT);
	assert_entry(7, 0, PCM_OWN_FLAG_REVOKING);

	/* Restore the exact handed-off tuple before its sole generation bump. */
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, token);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 7, token, &committed), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 8);
	assert_entry(8, token, 0);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token), CLUSTER_PCM_OWN_STALE);
}

UT_TEST(test_revoke_handoff_kinds_cover_n_s_x_with_one_lifecycle)
{
	ClusterPcmOwnSnapshot base;
	ClusterPcmOwnSnapshot live;
	uint64 committed = UINT64_MAX;
	uint64 token;
	uint8 states[] = { (uint8)PCM_STATE_N, (uint8)PCM_STATE_S, (uint8)PCM_STATE_X };
	ClusterPcmXGrantReservationKind expected_kinds[]
		= { CLUSTER_PCM_X_GRANT_RESERVATION_N_REVOKE_HANDOFF,
			CLUSTER_PCM_X_GRANT_RESERVATION_S_REVOKE_HANDOFF,
			CLUSTER_PCM_X_GRANT_RESERVATION_X_REVOKE_HANDOFF };
	int i;

	/* The added handoff arms must not broaden or shadow the ordinary new-token
	 * N reservation. */
	memset(&base, 0, sizeof(base));
	base.generation = 7;
	base.reservation_token = 4;
	base.pcm_state = (uint8)PCM_STATE_N;
	live = base;
	live.reservation_token = 5;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, 5),
				 CLUSTER_PCM_X_GRANT_RESERVATION_N_NEW);
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, 4),
				 CLUSTER_PCM_X_GRANT_RESERVATION_INVALID);

	for (i = 0; i < lengthof(states); i++) {
		reset_fixture();
		pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_REVOKING, &token),
					 CLUSTER_PCM_OWN_OK);

		memset(&base, 0, sizeof(base));
		base.generation = 7;
		base.reservation_token = token;
		base.flags = PCM_OWN_FLAG_REVOKING;
		base.pcm_state = states[i];
		live = base;
		live.flags = PCM_OWN_FLAG_GRANT_PENDING;

		UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, token), expected_kinds[i]);
		UT_ASSERT_EQ(cluster_pcm_own_revoke_to_grant_handoff_exact(0, 7, token),
					 CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, 7, token, &committed),
					 CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(committed, 8);
		assert_entry(8, token, 0);
	}
}

/*
 * Protocol pin for the t/400 fast-fail finish family (2026-07-20, loop9 and
 * loop10b DETAIL): a fresh-token GRANT_PENDING reservation taken from an
 * S/flags=0 base ("S_NEW" -- a stale-cover fallback wrongly entering the
 * legacy master acquire after an in-window X->S downgrade) must NEVER
 * become a legal finish shape.  Writer conversions are ordered by the
 * convert queue's FIFO/WFG; legalizing this shape at the finish would let
 * that fallback bypass the arbitration entirely (the original S3 unordered
 * multi-writer defect).  The fallback must re-enter the queue instead;
 * this classifier keeps refusing the bypass.
 */
UT_TEST(test_s_new_fresh_token_finish_shape_stays_invalid)
{
	ClusterPcmOwnSnapshot base;
	ClusterPcmOwnSnapshot live;

	/* The exact loop9/loop10b production tuple stays refused. */
	memset(&base, 0, sizeof(base));
	base.generation = 10;
	base.reservation_token = 5;
	base.pcm_state = (uint8)PCM_STATE_S;
	live = base;
	live.reservation_token = 6;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, 6),
				 CLUSTER_PCM_X_GRANT_RESERVATION_INVALID);

	/* A fresh-token X base is refused the same way: a live X cover never
	 * re-acquires through this path. */
	base.pcm_state = (uint8)PCM_STATE_X;
	live = base;
	live.reservation_token = 6;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, 6),
				 CLUSTER_PCM_X_GRANT_RESERVATION_INVALID);
}

UT_TEST(test_parallel_s_cover_is_rechecked_before_new_reservation)
{
	ParallelStableCoverRace *race;
	ClusterPcmOwnSnapshot base;
	ClusterPcmOwnSnapshot live;
	uint64 fresh_token = 0;

	/* The requester performs the optimistic probe before the peer publishes
	 * the compatible S grant for the same buffer. */
	race = run_parallel_stable_cover_race();
	if (race == NULL)
		return;
	UT_ASSERT_EQ(race->begin_result, CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(race->commit_result, CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(race->token, 1);
	UT_ASSERT_EQ(race->committed_generation, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&race->descriptor_state), (uint32)PCM_STATE_S);
	assert_entry(1, 1, 0);

	/* This is the exact entrance-race failure shape.  If bufmgr does not
	 * consume the stable cover under header authority, the raw begin can mint
	 * token 2 and strict finish correctly rejects the resulting S_NEW tuple. */
	memset(&base, 0, sizeof(base));
	base.generation = 1;
	base.reservation_token = 1;
	base.pcm_state = (uint8)PCM_STATE_S;
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, 1, PCM_OWN_FLAG_GRANT_PENDING, &fresh_token),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(fresh_token, 2);
	assert_entry(1, 2, PCM_OWN_FLAG_GRANT_PENDING);
	live = base;
	live.reservation_token = fresh_token;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, fresh_token),
				 CLUSTER_PCM_X_GRANT_RESERVATION_INVALID);

	/* Exercise the production cover predicate directly.  S readers may accept
	 * a stable successor generation; X writers remain generation-exact. */
	UT_ASSERT(cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_S, 1, 1,
														  (uint8)PCM_STATE_S, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts(
		(uint8)PCM_LOCK_MODE_S, 1, 1, (uint8)PCM_STATE_S, PCM_OWN_FLAG_GRANT_PENDING, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_X, 1, 1,
														   (uint8)PCM_STATE_S, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_X, 1, 2,
														   (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_S, 1, 2,
														  (uint8)PCM_STATE_S, 0, 0, 0));

	ClusterPcmOwnArray = NULL;
	UT_ASSERT_EQ(munmap(race, sizeof(*race)), 0);
}

UT_TEST(test_share_cover_reverify_accepts_stable_successor_grant)
{
	/* Once content authority is held, a stable current S/X successor is the
	 * exact node-level grant for a read.  Generation drift alone must not open
	 * a fresh legacy reservation from S (the forbidden S_NEW shape). */
	UT_ASSERT(cluster_pcm_x_cached_cover_reverify_accepts(
		(uint8)PCM_LOCK_MODE_S, UINT64_C(13), UINT64_C(14), (uint8)PCM_STATE_S, 0, 0, 0));
	UT_ASSERT(cluster_pcm_x_cached_cover_reverify_accepts(
		(uint8)PCM_LOCK_MODE_S, UINT64_C(13), UINT64_C(14), (uint8)PCM_STATE_X, 0, 0, 0));

	/* A writer keeps the stricter generation-exact gate and must re-enter the
	 * convert queue after any ownership round.  A non-covering or live
	 * lifecycle remains closed for both modes. */
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts(
		(uint8)PCM_LOCK_MODE_X, UINT64_C(13), UINT64_C(14), (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(cluster_pcm_x_cached_cover_reverify_accepts(
		(uint8)PCM_LOCK_MODE_X, UINT64_C(14), UINT64_C(14), (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts(
		(uint8)PCM_LOCK_MODE_X, UINT64_C(14), UINT64_C(14), (uint8)PCM_STATE_S, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_S, UINT64_C(14),
														   UINT64_C(14), (uint8)PCM_STATE_S,
														   PCM_OWN_FLAG_GRANT_PENDING, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_S, UINT64_C(14),
														   UINT64_C(14), (uint8)PCM_STATE_S,
														   PCM_OWN_FLAG_REVOKING, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts(
		(uint8)PCM_LOCK_MODE_N, UINT64_C(14), UINT64_C(14), (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_X, UINT64_C(14),
														   UINT64_C(14), (uint8)PCM_STATE_X, 0,
														   UINT64_C(91), 0));
	UT_ASSERT(!cluster_pcm_x_cached_cover_reverify_accepts((uint8)PCM_LOCK_MODE_X, UINT64_C(14),
														   UINT64_C(14), (uint8)PCM_STATE_X, 0,
														   UINT64_C(91), UINT64_C(22)));
}

UT_TEST(test_revoke_commit_is_exact_and_classifies_live_races)
{
	uint64 committed = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_REVOKING, &token),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(token, 1);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	/* A delayed/wrong lifecycle must not clear the current revoke. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 7, token + 1, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 7, token, &committed), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 8);
	assert_entry(8, token, 0);

	/* A duplicate cannot bump the ownership generation twice. */
	committed = UINT64_MAX;
	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 7, token, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(8, token, 0);

	/* A different well-formed lifecycle is contention, not corruption. */
	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 9);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 0, 9, &committed), CLUSTER_PCM_OWN_BUSY);
	assert_entry(0, 9, PCM_OWN_FLAG_GRANT_PENDING);

	/* Malformed live metadata is corruption, not ordinary contention. */
	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 9);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags,
						PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, 0, 9, &committed), CLUSTER_PCM_OWN_CORRUPT);
	assert_entry(0, 9, PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING);
}

UT_TEST(test_revoke_retain_commit_keeps_exact_token_until_release)
{
	ClusterPcmOwnEvictionCapture capture;
	uint64 committed = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_REVOKING, &token),
				 CLUSTER_PCM_OWN_OK);

	/* The retained commit bumps ownership exactly once but deliberately keeps
	 * the same live token: descriptor reuse remains fail-closed until the
	 * matching DRAIN/RELEASE_IMAGE arrives. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_commit_exact(0, 7, token + 1, &committed),
				 CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(7, token, PCM_OWN_FLAG_REVOKING);

	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_commit_exact(0, 7, token, &committed),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(committed, 8);
	assert_entry(8, token, PCM_OWN_FLAG_REVOKING);

	memset(&capture, 0, sizeof(capture));
	capture.generation = committed;
	capture.reservation_token = token;
	capture.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(!cluster_pcm_own_eviction_reuse_allowed(&capture));

	/* A stale DRAIN from either the pre-commit generation or a prior token is
	 * a strict no-op and cannot unpin a newer retained round. */
	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_release_exact(0, 7, token), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_release_exact(0, 8, token + 1),
				 CLUSTER_PCM_OWN_STALE);
	assert_entry(8, token, PCM_OWN_FLAG_REVOKING);

	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_release_exact(0, 8, token), CLUSTER_PCM_OWN_OK);
	assert_entry(8, token, 0);
	UT_ASSERT_EQ(cluster_pcm_own_revoke_retain_release_exact(0, 8, token), CLUSTER_PCM_OWN_STALE);
}

UT_TEST(test_revoke_commit_exhaustion_is_side_effect_free)
{
	uint64 committed = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, UINT64_MAX);
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, 17);
	pg_atomic_write_u32(&ClusterPcmOwnArray[0].flags, PCM_OWN_FLAG_REVOKING);
	token = 17;

	UT_ASSERT_EQ(cluster_pcm_own_revoke_commit_exact(0, UINT64_MAX, token, &committed),
				 CLUSTER_PCM_OWN_EXHAUSTED);
	UT_ASSERT_EQ(committed, 0);
	assert_entry(UINT64_MAX, token, PCM_OWN_FLAG_REVOKING);
}

UT_TEST(test_token_and_generation_never_wrap)
{
	uint64 token = UINT64_MAX;
	uint64 last_token;
	uint64 generation = 0;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].reservation_token, UINT64_MAX);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 0, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_EXHAUSTED);
	UT_ASSERT_EQ(token, 0);
	assert_entry(0, UINT64_MAX, 0);

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, UINT64_MAX - 1);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, UINT64_MAX - 1,
														 PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	last_token = token;
	UT_ASSERT_EQ(cluster_pcm_own_grant_commit_exact(0, UINT64_MAX - 1, token, &generation),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(generation, UINT64_MAX);
	assert_entry(UINT64_MAX, token, 0);

	/* MAX is terminal: begin must fail before token/flag side effects. */
	token = UINT64_MAX;
	UT_ASSERT_EQ(
		cluster_pcm_own_reservation_begin_exact(0, UINT64_MAX, PCM_OWN_FLAG_GRANT_PENDING, &token),
		CLUSTER_PCM_OWN_EXHAUSTED);
	UT_ASSERT_EQ(token, 0);
	assert_entry(UINT64_MAX, last_token, 0);
	UT_ASSERT(!cluster_pcm_own_gen_bump_checked(0, &generation));
	UT_ASSERT_EQ(generation, UINT64_MAX);
	assert_entry(UINT64_MAX, last_token, 0);
}

UT_TEST(test_ordinary_generation_bump_rejects_live_reservation)
{
	uint64 generation = UINT64_MAX;
	uint64 token;

	reset_fixture();
	pg_atomic_write_u64(&ClusterPcmOwnArray[0].generation, 7);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, 7, PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);

	/* Only the token-exact finish/revoke lifecycle may advance generation
	 * while a transient ownership flag is live.  The ordinary transition
	 * helper must be a no-op so it cannot bypass the reservation token. */
	UT_ASSERT(!cluster_pcm_own_gen_bump_checked(0, &generation));
	UT_ASSERT_EQ(generation, 7);
	assert_entry(7, token, PCM_OWN_FLAG_GRANT_PENDING);

	/* The same helper remains valid after exact cleanup makes the entry idle. */
	reset_fixture();
	UT_ASSERT(cluster_pcm_own_gen_bump_checked(0, &generation));
	UT_ASSERT_EQ(generation, 1);
	assert_entry(1, 0, 0);
}

UT_TEST(test_eviction_rejects_live_reservation_and_exhaustion)
{
	ClusterPcmOwnEvictionCapture capture;

	memset(&capture, 0, sizeof(capture));
	capture.generation = 9;
	UT_ASSERT(cluster_pcm_own_eviction_reuse_allowed(&capture));

	capture.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT(!cluster_pcm_own_eviction_reuse_allowed(&capture));
	capture.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(!cluster_pcm_own_eviction_reuse_allowed(&capture));
	capture.flags = 0;
	capture.reservation_token = 3;
	/* The single token is monotonic and remains nonzero while idle; flags are
	 * the only active-lifecycle marker. */
	UT_ASSERT(cluster_pcm_own_eviction_reuse_allowed(&capture));
	capture.generation = UINT64_MAX;
	UT_ASSERT(!cluster_pcm_own_eviction_reuse_allowed(&capture));
}

UT_TEST(test_target_eviction_bufferdesc_lifecycle_is_reversible_before_local_commit)
{
	static const char *const begin_contract[]
		= { "base->pcm_state != (uint8) PCM_STATE_X",
			"cluster_pcm_own_fence_matches_locked(buf, base)",
			"cluster_pcm_own_reservation_begin_exact", "PCM_OWN_FLAG_REVOKING",
			"cluster_pcm_own_snapshot_locked(buf, revoking_out)" };
	static const char *const finish_contract[]
		= { "expected_revoking->pcm_state != (uint8) PCM_STATE_X",
			"expected_revoking->flags != PCM_OWN_FLAG_REVOKING",
			"cluster_pcm_own_fence_matches_locked(buf, expected_revoking)",
			"cluster_pcm_own_revoke_commit_exact",
			"buf->pcm_state = (uint8) PCM_STATE_N",
			"cluster_pcm_own_snapshot_locked(buf, committed_n_out)" };
	static const char *const abort_contract[]
		= { "cluster_pcm_own_fence_matches_locked(buf, expected_revoking)",
			"cluster_pcm_own_reservation_abort_exact", "PCM_OWN_FLAG_REVOKING",
			"cluster_pcm_own_snapshot_locked(buf, restored_x_out)" };
	char *source = read_bufmgr_source();

	/* The TARGET cached-X release proof must be frozen while the descriptor is
	 * still X.  These helpers define the reversible BufferDesc half: begin
	 * publishes an exact REVOKING token without changing mode or tag, finish is
	 * the sole X->N generation bump, and abort restores the same X residency.
	 * Stale identity is rejected before any sidecar mutation. */
	assert_ordered_in_function(
		source, "\ncluster_pcm_own_eviction_begin_locked(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_eviction_finish_locked(", begin_contract,
		lengthof(begin_contract));
	assert_ordered_in_function(
		source, "\ncluster_pcm_own_eviction_finish_locked(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_eviction_abort_locked(", finish_contract,
		lengthof(finish_contract));
	assert_ordered_in_function(
		source, "\ncluster_pcm_own_eviction_abort_locked(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_eviction_commit_locked(", abort_contract,
		lengthof(abort_contract));
	free(source);
}

static char *
read_bufmgr_source(void)
{
	FILE *file;
	long length;
	char *source;

	file = fopen(BUFMGR_SOURCE_PATH, "rb");
	UT_ASSERT_NOT_NULL(file);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	length = ftell(file);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	source = malloc((size_t)length + 1);
	UT_ASSERT_NOT_NULL(source);
	UT_ASSERT_EQ(fread(source, 1, (size_t)length, file), (size_t)length);
	source[length] = '\0';
	fclose(file);
	return source;
}

static void
assert_commit_before_tail(const char *source, const char *function_start, const char *function_end)
{
	const char *begin = strstr(source, function_start);
	const char *end;
	const char *commit;
	const char *tail;

	UT_ASSERT_NOT_NULL(begin);
	if (begin == NULL)
		return;
	end = strstr(begin + strlen(function_start), function_end);
	UT_ASSERT_NOT_NULL(end);
	if (end == NULL)
		return;
	commit = strstr(begin, "cluster_pcm_own_eviction_commit_locked");
	tail = strstr(begin, "InvalidateBufferCommitTailLocked");
	UT_ASSERT_NOT_NULL(commit);
	UT_ASSERT_NOT_NULL(tail);
	if (commit == NULL || tail == NULL)
		return;
	UT_ASSERT(commit < tail);
	UT_ASSERT(commit < end);
	UT_ASSERT(tail < end);
}

static int
count_occurrences(const char *source, const char *needle)
{
	int count = 0;
	size_t needle_length = strlen(needle);

	while ((source = strstr(source, needle)) != NULL) {
		count++;
		source += needle_length;
	}
	return count;
}

static void
assert_ordered_in_function(const char *source, const char *function_start, const char *function_end,
						   const char *const *needles, int needle_count)
{
	const char *cursor = strstr(source, function_start);
	const char *end;
	int i;

	UT_ASSERT_NOT_NULL(cursor);
	if (cursor == NULL)
		return;
	end = strstr(cursor + strlen(function_start), function_end);
	UT_ASSERT_NOT_NULL(end);
	if (end == NULL)
		return;

	for (i = 0; i < needle_count; i++) {
		cursor = strstr(cursor, needles[i]);
		if (cursor == NULL) {
			printf("# Missing ordered source token: %s\n", needles[i]);
			UT_ASSERT_NOT_NULL(cursor);
			return;
		}
		UT_ASSERT(cursor < end);
		if (cursor >= end)
			return;
		cursor += strlen(needles[i]);
	}
}

static void
assert_source_range_contains(const char *start, const char *end, const char *needle)
{
	const char *found;

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	if (start == NULL || end == NULL)
		return;
	found = strstr(start, needle);
	UT_ASSERT_NOT_NULL(found);
	if (found != NULL)
		UT_ASSERT(found < end);
}

UT_TEST(test_bufmgr_d5a_commitlocked_uses_locked_commit_and_saved_tag_release)
{
	static const char *const commit_contract[]
		= { "ClusterPcmOwnEvictionCapture eviction_capture",
			"cluster_pcm_own_eviction_capture_locked", "cluster_pcm_own_eviction_commit_locked",
			"eviction_result != CLUSTER_PCM_OWN_OK", "InvalidateBufferCommitTailLocked" };
	static const char *const tail_contract[]
		= { "ClearBufferTag", "UnlockBufHdr", "cluster_pcm_lock_release_saved_tag_for_eviction" };
	char *source = read_bufmgr_source();

	/* Descriptor reuse is a single header-authority commit.  A live token,
	 * exhausted generation, or tuple mismatch must leave the old tag resident
	 * and return fail-closed; only an exact successful commit may clear the tag
	 * and later release the master holder by the saved immutable tag. */
	assert_commit_before_tail(source, "\nInvalidateBufferCommitLocked(",
							  "\n/*\n * InvalidateBufferCommitTailLocked");
	assert_ordered_in_function(source, "\nInvalidateBufferCommitLocked(",
							   "\n/*\n * InvalidateBufferCommitTailLocked", commit_contract,
							   lengthof(commit_contract));
	assert_ordered_in_function(source, "\nInvalidateBufferCommitTailLocked(",
							   "\n/*\n * InvalidateBufferTry", tail_contract,
							   lengthof(tail_contract));
	UT_ASSERT_NOT_NULL(strstr(source, "static bool\nInvalidateBufferCommitLocked"));
	UT_ASSERT_NOT_NULL(strstr(source, "cluster_pcm_lock_release_saved_tag_for_eviction"));
	UT_ASSERT_NULL(strstr(source, "buf->tag = *oldTag"));
	free(source);
}

UT_TEST(test_bufmgr_abort_cleanup_is_never_silent)
{
	static const char *const normal_cleanup[]
		= { "cluster_pcm_own_abort_grant_reservation", "CLUSTER_PCM_OWN_OK", "ereport(ERROR" };
	static const char *const error_cleanup[]
		= { "cluster_pcm_own_abort_grant_reservation", "CLUSTER_PCM_OWN_OK", "elog(LOG" };
	char *source = read_bufmgr_source();

	/* Every normal false/READ_IMAGE exit must prove exact cleanup or ERROR.
	 * During PG_CATCH, preserve the original error but emit LOG evidence when
	 * exact cleanup did not converge.  No call may discard the result. */
	UT_ASSERT_NULL(strstr(source, "(void) cluster_pcm_own_abort_grant_reservation"));
	assert_ordered_in_function(source, "\ncluster_pcm_own_abort_grant_or_error(",
							   "\nstatic void\ncluster_pcm_own_abort_grant_after_error(",
							   normal_cleanup, lengthof(normal_cleanup));
	assert_ordered_in_function(
		source, "\ncluster_pcm_own_abort_grant_after_error(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_abort_grant_after_master_rollback(",
		error_cleanup, lengthof(error_cleanup));
	free(source);
}

UT_TEST(test_bufmgr_finish_failure_rolls_back_acquired_master_grant)
{
	static const char *const rollback_contract[]
		= { "cluster_pcm_own_finish_grant_reservation",
			"PG_TRY",
			"cluster_pcm_lock_release_buffer_for_eviction",
			"PG_CATCH",
			"elog(LOG",
			"PG_RE_THROW",
			"cluster_pcm_own_abort_grant_after_master_rollback",
			"ereport(ERROR" };
	char *source = read_bufmgr_source();

	/* Definition plus the remaining S-side LockBuffer caller.  The source-
	 * removed X path is Resource-X native and has no legacy direct-lock caller;
	 * the surviving grant path still cannot leak a master holder when local
	 * finish rejects the token/tuple. */
	UT_ASSERT(count_occurrences(source, "cluster_pcm_own_finish_grant_or_rollback(") >= 2);
	assert_ordered_in_function(source, "\ncluster_pcm_own_finish_grant_or_rollback(", "\nstatic ",
							   rollback_contract, lengthof(rollback_contract));
	free(source);
}

UT_TEST(test_bufmgr_s_base_rollback_normalizes_to_n_under_header_authority)
{
	static const char *const s_to_n_contract[] = { "LockBufHdr",
												   "base->pcm_state != (uint8)PCM_STATE_N",
												   "base->pcm_state != (uint8)PCM_STATE_S",
												   "base->pcm_state == (uint8)PCM_STATE_S",
												   "base->generation == UINT64_MAX",
												   "cluster_pcm_own_reservation_abort_exact",
												   "base->pcm_state == (uint8)PCM_STATE_S",
												   "cluster_pcm_own_gen_bump_checked",
												   "buf->pcm_state = (uint8)PCM_STATE_N",
												   "UnlockBufHdr" };
	char *source = read_bufmgr_source();

	/* A legacy S-base acquire that reached master X cannot restore S after a
	 * failed local finish: the master rollback is X->N.  The local half must
	 * therefore exact-abort the live token and commit S->N plus one checked
	 * generation bump under a single header-lock hold.  N-base skips the bump
	 * and remains N.  All prechecks precede abort, so rejection leaves the live
	 * flag as fail-closed evidence rather than advertising successful cleanup. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_abort_grant_after_master_rollback(",
							   "\nstatic void\ncluster_pcm_own_finish_grant_or_rollback(",
							   s_to_n_contract, lengthof(s_to_n_contract));
	free(source);
}

UT_TEST(test_bufmgr_generation_bump_failure_is_classified_under_header_lock)
{
	static const char *const diagnostic_contract[] = { "cluster_pcm_own_reservation_token_get",
													   "cluster_pcm_own_flags_get",
													   "cluster_pcm_own_classify_live_flags",
													   "live_result != CLUSTER_PCM_OWN_OK",
													   "generation == UINT64_MAX",
													   "CLUSTER_PCM_OWN_EXHAUSTED" };
	static const char *const transition_contract[]
		= { "LockBufHdr", "cluster_pcm_own_bump_locked", "UnlockBufHdr",
			"cluster_pcm_own_report_bump_failure" };
	char *source = read_bufmgr_source();

	/* A checked bump can reject either a live exact lifecycle or terminal MAX.
	 * Both observations must be made while header authority is still held and
	 * must not be collapsed into the misleading "exhausted" diagnosis. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_bump_failure(", "\nstatic ",
							   diagnostic_contract, lengthof(diagnostic_contract));
	assert_ordered_in_function(source, "\ncluster_pcm_own_transition(", "\n/*", transition_contract,
							   lengthof(transition_contract));
	UT_ASSERT(count_occurrences(source, "cluster_pcm_own_bump_failure(") >= 2);
	UT_ASSERT_NOT_NULL(strstr(source, "active reservation"));
	UT_ASSERT_NOT_NULL(strstr(source, "generation exhausted"));
	UT_ASSERT_NOT_NULL(strstr(source, "LockBuffer unlock READ_IMAGE"));
	UT_ASSERT_NOT_NULL(strstr(source, "LockBuffer S read-image publish"));
	UT_ASSERT_NOT_NULL(strstr(source, "LockBuffer SHARE local-cache-off mirror"));
	free(source);
}

UT_TEST(test_read_image_lifecycle_is_exact_monotonic_and_header_atomic)
{
	typedef ClusterPcmOwnResult (*PublishReadImageFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
													  uint64, uint64 *);
	typedef ClusterPcmOwnResult (*ReleaseReadImageFn)(BufferDesc *, uint64 *);
	typedef ClusterPcmOwnResult (*ReclaimReadImageFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
													  uint64 *);
	static const char *const publish_contract[]
		= { "*published_generation_out = 0",
			"LWLockHeldByMe(BufferDescriptorGetContentLock(buf))",
			"LockBufHdr(buf)",
			"cluster_pcm_own_snapshot_locked(buf, &live)",
			"BufferTagsEqual(&live.tag, &base->tag)",
			"live.generation != base->generation",
			"live.reservation_token != reservation_token",
			"live.writer_activation_token != 0",
			"live.resource_x_activation_generation != 0",
			"live.flags != PCM_OWN_FLAG_GRANT_PENDING",
			"BM_VALID",
			"BM_IO_IN_PROGRESS",
			"BUF_TYPE_CURRENT",
			"cluster_pcm_own_grant_commit_exact",
			"buf->pcm_state = (uint8) PCM_STATE_READ_IMAGE",
			"UnlockBufHdr(buf, buf_state)" };
	static const char *const clear_contract[]
		= { "*cleared_generation_out = 0",
			"LockBufHdr(buf)",
			"cluster_pcm_own_snapshot_locked(buf, &live)",
			"cluster_pcm_own_snapshot_equal_exact(&live, published)",
			"live.pcm_state != (uint8) PCM_STATE_READ_IMAGE",
			"live.flags != 0",
			"live.writer_activation_token != 0",
			"live.resource_x_activation_generation != 0",
			"cluster_pcm_own_bump_locked",
			"buf->pcm_state = (uint8) PCM_STATE_N",
			"UnlockBufHdr(buf, buf_state)" };
	static const char *const begin_contract[]
		= { "*wait_reason = CLUSTER_PCM_GRANT_WAIT_NONE",
			"cluster_pcm_own_snapshot_locked(buf, out_base)",
			"cluster_pcm_x_read_image_begin_disposition(", "result == CLUSTER_PCM_OWN_INVALID",
			"cluster_pcm_own_reservation_begin_exact" };
	char *source = read_bufmgr_source();

	UT_ASSERT_EQ(CLUSTER_PCM_GRANT_WAIT_NONE, 0);
	UT_ASSERT_EQ(CLUSTER_PCM_GRANT_WAIT_LIVE_RESERVATION, 1);
	UT_ASSERT_EQ(CLUSTER_PCM_GRANT_WAIT_READ_IMAGE_BRACKET, 2);
	UT_ASSERT_EQ(CLUSTER_PCM_GRANT_WAIT_RESOURCE_X_BARRIER, 3);
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_pcm_own_publish_read_image_exact),
										   PublishReadImageFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_pcm_own_release_read_image_exact),
										   ReleaseReadImageFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_pcm_own_reclaim_read_image_exact),
										   ReclaimReadImageFn));

	assert_ordered_in_function(
		source, "\ncluster_pcm_own_publish_read_image_exact(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_clear_read_image_exact(", publish_contract,
		lengthof(publish_contract));
	assert_ordered_in_function(source, "\ncluster_pcm_own_clear_read_image_exact(",
							   "\nClusterPcmOwnResult\ncluster_pcm_own_release_read_image_exact(",
							   clear_contract, lengthof(clear_contract));
	assert_ordered_in_function(source, "\ncluster_pcm_own_begin_grant_reservation(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_begin_x_reservation(",
							   begin_contract, lengthof(begin_contract));
	free(source);
}

UT_TEST(test_share_read_image_publication_has_no_clean_n_observation_gap)
{
	char *source = read_bufmgr_source();
	const char *lockbuffer = strstr(source, "\nLockBufferInternal(Buffer buffer, int mode");
	const char *lockbuffer_end
		= lockbuffer != NULL ? strstr(lockbuffer, "\nvoid\nLockBuffer(Buffer buffer, int mode)")
							 : NULL;
	const char *non_durable
		= lockbuffer != NULL ? strstr(lockbuffer, "else if (pcm_pending_set)") : NULL;
	const char *branch_end = non_durable != NULL ? strstr(non_durable, "\n\t\t\t\tbreak;") : NULL;
	const char *publish = non_durable != NULL
							  ? strstr(non_durable, "cluster_pcm_own_publish_read_image_exact(")
							  : NULL;
	const char *pending_clear = publish != NULL ? strstr(publish, "pcm_pending_set = false") : NULL;
	const char *old_abort
		= non_durable != NULL ? strstr(non_durable, "cluster_pcm_own_abort_grant_or_error(") : NULL;
	const char *old_transition
		= non_durable != NULL ? strstr(non_durable, "cluster_pcm_own_transition(") : NULL;

	UT_ASSERT_NOT_NULL(lockbuffer);
	UT_ASSERT_NOT_NULL(lockbuffer_end);
	UT_ASSERT_NOT_NULL(non_durable);
	UT_ASSERT_NOT_NULL(branch_end);
	UT_ASSERT_NOT_NULL(publish);
	UT_ASSERT_NOT_NULL(pending_clear);
	if (publish != NULL && branch_end != NULL)
		UT_ASSERT(publish < branch_end);
	if (pending_clear != NULL && branch_end != NULL)
		UT_ASSERT(pending_clear < branch_end);
	if (old_abort != NULL && branch_end != NULL)
		UT_ASSERT(old_abort >= branch_end);
	if (old_transition != NULL && branch_end != NULL)
		UT_ASSERT(old_transition >= branch_end);
	if (lockbuffer_end != NULL)
		UT_ASSERT(branch_end == NULL || branch_end < lockbuffer_end);
	free(source);
}

UT_TEST(test_read_image_normal_unlock_clears_exactly_before_content_release)
{
	char *source = read_bufmgr_source();
	const char *lockbuffer = strstr(source, "\nLockBufferInternal(Buffer buffer, int mode");
	const char *unlock
		= lockbuffer != NULL ? strstr(lockbuffer, "if (mode == BUFFER_LOCK_UNLOCK)") : NULL;
	const char *unlock_end
		= unlock != NULL ? strstr(unlock, "\n\telse if (cluster_pcm_is_active()") : NULL;
	const char *release_exact
		= unlock != NULL ? strstr(unlock, "cluster_pcm_own_release_read_image_exact(") : NULL;
	const char *content_release
		= unlock != NULL ? strstr(unlock, "LWLockRelease(BufferDescriptorGetContentLock(buf))")
						 : NULL;
	const char *report_failure = release_exact != NULL
									 ? strstr(release_exact, "cluster_pcm_own_report_bump_failure(")
									 : NULL;
	const char *old_generic
		= unlock != NULL ? strstr(unlock, "cluster_pcm_own_transition(buf, (uint8) PCM_STATE_N")
						 : NULL;

	UT_ASSERT_NOT_NULL(unlock);
	UT_ASSERT_NOT_NULL(unlock_end);
	UT_ASSERT_NOT_NULL(release_exact);
	UT_ASSERT_NOT_NULL(content_release);
	UT_ASSERT_NOT_NULL(report_failure);
	if (release_exact != NULL && content_release != NULL)
		UT_ASSERT(release_exact < content_release);
	if (content_release != NULL && report_failure != NULL)
		UT_ASSERT(content_release < report_failure);
	if (old_generic != NULL && unlock_end != NULL)
		UT_ASSERT(old_generic >= unlock_end);
	free(source);
}

UT_TEST(test_read_image_abandoned_reclaim_wait_is_content_x_and_successor_exact)
{
	static const char *const reclaim_wait_contract[]
		= { "wait_reason == CLUSTER_PCM_GRANT_WAIT_READ_IMAGE_BRACKET",
			"abandoned = *base_out",
			"LWLockAcquireOrWait(",
			"LW_EXCLUSIVE",
			"PG_TRY()",
			"cluster_pcm_own_reclaim_read_image_exact(",
			"PG_FINALLY()",
			"LWLockHeldByMe(content_lock)",
			"LWLockRelease(content_lock)",
			"reclaim_result == CLUSTER_PCM_OWN_OK",
			"reclaim_result == CLUSTER_PCM_OWN_STALE",
			"continue" };
	char *source = read_bufmgr_source();

	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_begin_grant_reservation_wait(",
							   "\n\ntypedef enum ClusterBufmgrPcmRetryRearmResult",
							   reclaim_wait_contract, lengthof(reclaim_wait_contract));
	free(source);
}

UT_TEST(test_pending_x_denied_retry_drops_removed_queue_gap)
{
	static const char *const retry_contract[]
		= { "cluster_pcm_own_abort_grant_reservation", "cluster_bufmgr_resource_x_wait_retry",
			"cluster_bufmgr_pcm_begin_grant_reservation_wait" };
	char *source = read_bufmgr_source();
	const char *rearm = strstr(source, "\ncluster_bufmgr_pcm_retry_denied_rearm(");
	const char *rearm_end = rearm != NULL ? strstr(rearm, "\n}\n\nstatic ") : NULL;
	const char *old_delay
		= rearm != NULL ? strstr(rearm, "cluster_bufmgr_pcm_pending_x_retry_delay_ms") : NULL;
	const char *old_lmon = rearm != NULL ? strstr(rearm, "cluster_lmon_main_loop_interval") : NULL;
	const char *old_backoff
		= rearm != NULL ? strstr(rearm, "cluster_gcs_block_starvation_backoff_ms") : NULL;
	const char *blocking_sleep = rearm != NULL ? strstr(rearm, "pg_usleep") : NULL;

	/* R11 source removal deleted the PCM-X queue/LMON INVALIDATE pump whose
	 * two-interval gap used to drive this rearm.  Keep the exact abort and the
	 * existing short, gate-checked scheduling yield before a fresh reservation,
	 * but never inherit the removed queue's 2s..120s exponential wait. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_retry_denied_rearm(", "\nstatic ",
							   retry_contract, lengthof(retry_contract));
	UT_ASSERT_NOT_NULL(rearm);
	UT_ASSERT_NOT_NULL(rearm_end);
	if (rearm_end != NULL) {
		UT_ASSERT(old_delay == NULL || old_delay >= rearm_end);
		UT_ASSERT(old_lmon == NULL || old_lmon >= rearm_end);
		UT_ASSERT(old_backoff == NULL || old_backoff >= rearm_end);
		UT_ASSERT(blocking_sleep == NULL || blocking_sleep >= rearm_end);
	}
	UT_ASSERT_NULL(strstr(source, "\ncluster_bufmgr_pcm_pending_x_retry_delay_ms("));
	free(source);
}

UT_TEST(test_resource_x_s_barrier_aborts_one_racing_reservation_then_parks)
{
	char *source = read_bufmgr_source();
	const char *begin_wait
		= source != NULL ? strstr(source, "\ncluster_bufmgr_pcm_begin_grant_reservation_wait(")
						 : NULL;
	const char *begin_wait_end
		= begin_wait != NULL
			  ? strstr(begin_wait, "\n}\n\ntypedef enum ClusterBufmgrPcmRetryRearmResult")
			  : NULL;
	const char *reservation_begin
		= begin_wait != NULL ? strstr(begin_wait, "cluster_pcm_own_begin_grant_reservation(buf")
							 : NULL;
	const char *resource_x_barrier
		= reservation_begin != NULL
			  ? strstr(reservation_begin,
					   "cluster_gcs_block_resource_x_local_s_barrier_active(buf->tag)")
			  : NULL;
	const char *abort = resource_x_barrier != NULL
							? strstr(resource_x_barrier, "cluster_pcm_own_abort_grant_reservation(")
							: NULL;
	const char *park = abort != NULL ? strstr(abort, "\n\t\t\tdo\n") : NULL;
	const char *yield = park != NULL ? strstr(park, "cluster_bufmgr_resource_x_wait_retry(") : NULL;
	const char *barrier_recheck
		= yield != NULL
			  ? strstr(yield, "while (cluster_gcs_block_resource_x_local_s_barrier_active(")
			  : NULL;

	/* The header-locked begin must run first so a terminal cached X can cover
	 * the read.  If a requester-local round raced that begin, abort the one
	 * exact reversible reservation and remain parked behind the same barrier;
	 * do not mint one token per retry while T1 is waiting for clean N. */
	UT_ASSERT_NOT_NULL(begin_wait);
	UT_ASSERT_NOT_NULL(begin_wait_end);
	UT_ASSERT_NOT_NULL(reservation_begin);
	UT_ASSERT_NOT_NULL(resource_x_barrier);
	UT_ASSERT_NOT_NULL(abort);
	UT_ASSERT_NOT_NULL(park);
	UT_ASSERT_NOT_NULL(yield);
	UT_ASSERT_NOT_NULL(barrier_recheck);
	if (begin_wait_end != NULL && reservation_begin != NULL && resource_x_barrier != NULL
		&& abort != NULL && park != NULL && yield != NULL && barrier_recheck != NULL)
		UT_ASSERT(reservation_begin < resource_x_barrier && resource_x_barrier < abort
				  && abort < park && park < yield && yield < barrier_recheck
				  && barrier_recheck < begin_wait_end);
	free(source);
}

UT_TEST(test_bufmgr_finish_rejects_invalid_state_and_initializes_acquire_result)
{
	static const char *const finish_gate[]
		= { "new_pcm_state != (uint8)PCM_STATE_S", "new_pcm_state != (uint8)PCM_STATE_X",
			"return CLUSTER_PCM_OWN_INVALID", "LockBufHdr" };
	char *source = read_bufmgr_source();

	/* The only durable grant mirrors are S and X.  Validate that before any
	 * header/sidecar mutation, and never let PG_TRY leave an indeterminate
	 * acquire result for its catch/finalize paths. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_finish_grant_reservation(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_x_commit(",
							   finish_gate, lengthof(finish_gate));
	UT_ASSERT_NOT_NULL(strstr(source, "bool pcm_acquired = false"));
	free(source);
}

UT_TEST(test_bufmgr_finish_and_abort_gate_on_exact_base_state)
{
	static const char *const finish_contract[]
		= { "LockBufHdr", "cluster_pcm_own_snapshot_locked", "cluster_pcm_x_grant_reservation_kind",
			"cluster_pcm_own_grant_commit_exact" };
	static const char *const abort_contract[]
		= { "LockBufHdr", "BufferTagsEqual", "buf->pcm_state != base->pcm_state",
			"cluster_pcm_own_reservation_abort_exact" };
	char *source = read_bufmgr_source();

	/* Tag/gen/token/flag identity is insufficient: a concurrent ownership
	 * transition that changed only the descriptor mirror must make both exact
	 * finish and abort return STALE before touching generation or flags. */
	assert_ordered_in_function(source, "\ncluster_pcm_own_finish_grant_reservation(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_x_commit(",
							   finish_contract, lengthof(finish_contract));
	assert_ordered_in_function(source, "\ncluster_pcm_own_abort_grant_reservation(",
							   "\nstatic void\ncluster_pcm_own_abort_grant_or_error(",
							   abort_contract, lengthof(abort_contract));
	free(source);
}

UT_TEST(test_retained_release_retag_respects_pin_contract)
{
	BufferDesc buf;
	uint32 buf_state;
	uint8 type_before;
	uint32 state_before;

	/* Unpinned: the released retained image is dropped -- !BM_VALID plus a
	 * BUF_TYPE_CURRENT retag makes the next ordinary read reload the current
	 * page bytes through the buffer-IO protocol. */
	memset(&buf, 0, sizeof(buf));
	buf.buffer_type = (uint8)BUF_TYPE_PI;
	buf_state = BM_VALID | BM_TAG_VALID;
	UT_ASSERT(cluster_pcm_x_retained_release_retag(&buf.buffer_type, &buf_state));
	UT_ASSERT_EQ(buf_state & BM_VALID, 0);
	UT_ASSERT_EQ(buf.buffer_type, (uint8)BUF_TYPE_CURRENT);

	/* Pinned: a pre-existing PG pin freezes the page image -- the bytes may
	 * neither vanish (!BM_VALID under a pin breaks the pin contract and
	 * re-arms the legacy begin-over-invalid-base S_NEW mint) nor be reloaded
	 * in place (an image swap under the pin holder).  The release must keep
	 * the established PI+BM_VALID never-write/never-serve N mirror byte-exact
	 * (the passive-pin invalidate release shape): the next S acquire installs
	 * over it via an exact GRANT_PENDING and republishes CURRENT, an X
	 * convert rides the convert queue, and eviction retags after the last
	 * pin drains. */
	memset(&buf, 0, sizeof(buf));
	buf.buffer_type = (uint8)BUF_TYPE_PI;
	buf_state = (BM_VALID | BM_TAG_VALID) + BUF_REFCOUNT_ONE * 2;
	type_before = buf.buffer_type;
	state_before = buf_state;
	UT_ASSERT(!cluster_pcm_x_retained_release_retag(&buf.buffer_type, &buf_state));
	UT_ASSERT_EQ(buf.buffer_type, type_before);
	UT_ASSERT_EQ(buf_state, state_before);

	/* One pin behaves like many. */
	buf.buffer_type = (uint8)BUF_TYPE_PI;
	buf_state = (BM_VALID | BM_TAG_VALID) + BUF_REFCOUNT_ONE;
	UT_ASSERT(!cluster_pcm_x_retained_release_retag(&buf.buffer_type, &buf_state));
	UT_ASSERT(buf_state & BM_VALID);
	UT_ASSERT_EQ(buf.buffer_type, (uint8)BUF_TYPE_PI);

	/* The kept mirror is republished CURRENT only by a byte-currency proof
	 * inside the exact open legacy grant lifecycle (a shipped-image install,
	 * a storage refresh, or an SCN PASS proof): pcm N + live GRANT_PENDING +
	 * nonzero token + BM_VALID + PI.  Anything else must stay frozen so the
	 * finish valid-image gate keeps refusing an unproven stale cover. */
	UT_ASSERT(cluster_pcm_x_grant_pending_republish_shape(
		(uint8)PCM_STATE_N, PCM_OWN_FLAG_GRANT_PENDING, 7, true, (uint8)BUF_TYPE_PI));
	UT_ASSERT(!cluster_pcm_x_grant_pending_republish_shape(
		(uint8)PCM_STATE_S, PCM_OWN_FLAG_GRANT_PENDING, 7, true, (uint8)BUF_TYPE_PI));
	UT_ASSERT(!cluster_pcm_x_grant_pending_republish_shape((uint8)PCM_STATE_N, 0, 7, true,
														   (uint8)BUF_TYPE_PI));
	UT_ASSERT(!cluster_pcm_x_grant_pending_republish_shape(
		(uint8)PCM_STATE_N, PCM_OWN_FLAG_REVOKING, 7, true, (uint8)BUF_TYPE_PI));
	UT_ASSERT(!cluster_pcm_x_grant_pending_republish_shape(
		(uint8)PCM_STATE_N, PCM_OWN_FLAG_GRANT_PENDING, 0, true, (uint8)BUF_TYPE_PI));
	UT_ASSERT(!cluster_pcm_x_grant_pending_republish_shape(
		(uint8)PCM_STATE_N, PCM_OWN_FLAG_GRANT_PENDING, 7, false, (uint8)BUF_TYPE_PI));
	UT_ASSERT(!cluster_pcm_x_grant_pending_republish_shape(
		(uint8)PCM_STATE_N, PCM_OWN_FLAG_GRANT_PENDING, 7, true, (uint8)BUF_TYPE_CURRENT));
}

UT_TEST(test_passive_retained_pi_is_an_n_assertion_candidate_only)
{
	/* A DRAINed retained image kept under a pre-existing pin is a legal
	 * passive N requester shape.  It may originate ASSERT_X without a local
	 * image proof, but dirty/IO/malformed variants remain closed. */
	UT_ASSERT_EQ(
		cluster_pcm_x_n_assertion_shape((uint8)PCM_STATE_N, (uint8)BUF_TYPE_CURRENT, BM_VALID),
		CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_x_n_assertion_shape((uint8)PCM_STATE_N, (uint8)BUF_TYPE_PI, BM_VALID),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_x_n_assertion_shape((uint8)PCM_STATE_N, (uint8)BUF_TYPE_PI,
												 BM_VALID | BM_DIRTY),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_n_assertion_shape((uint8)PCM_STATE_N, (uint8)BUF_TYPE_PI,
												 BM_VALID | BM_IO_IN_PROGRESS),
				 CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ(cluster_pcm_x_n_assertion_shape((uint8)PCM_STATE_N, (uint8)BUF_TYPE_PI, 0),
				 CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(
		cluster_pcm_x_n_assertion_shape((uint8)PCM_STATE_N, (uint8)BUF_TYPE_XCUR, BM_VALID),
		CLUSTER_PCM_OWN_CORRUPT);
	UT_ASSERT_EQ(cluster_pcm_x_n_assertion_shape((uint8)PCM_STATE_S, (uint8)BUF_TYPE_PI, BM_VALID),
				 CLUSTER_PCM_OWN_STALE);
}

UT_TEST(test_retained_release_and_finish_never_cover_invalid_bytes)
{
	static const char *const release_retag_contract[]
		= { "cluster_pcm_own_revoke_retain_release_exact", "cluster_pcm_x_retained_release_retag",
			"UnlockBufHdr" };
	static const char *const finish_valid_gate[]
		= { "cluster_pcm_x_grant_reservation_kind", "BUF_TYPE_PI",
			"(buf_state & (BM_VALID | BM_IO_IN_PROGRESS)) == 0", "CLUSTER_PCM_OWN_CORRUPT",
			"cluster_pcm_own_grant_commit_exact" };
	char *source = read_bufmgr_source();

	/* The retained release must route its descriptor retag through the shared
	 * pin-aware decision helper under the same header-lock hold that released
	 * the exact write-fence token.  And a grant finish must never commit a
	 * durable S/X mirror over a page image that is not current (!BM_VALID or
	 * a PI mirror): that silent cover of stale bytes is how the pinned
	 * descriptor was previously stamped S over an invalid base, re-arming the
	 * refused legacy S_NEW convert (deterministic client ERROR) -- and, on
	 * the PI arm, a Rule 8.A stale read.  The one legal !BM_VALID commit
	 * shape is the direct-init window (EXTEND/READ_MISS), whose gate still
	 * owns BM_IO_IN_PROGRESS between StartBufferIO and
	 * TerminateBufferIO(BM_VALID).  Reloading the bytes in place under a
	 * foreign pin is equally forbidden. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_release_retained_image(",
							   "\ncluster_bufmgr_pcm_own_self_handoff_probe(",
							   release_retag_contract, lengthof(release_retag_contract));
	assert_ordered_in_function(source, "\ncluster_pcm_own_finish_grant_reservation(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_x_commit(",
							   finish_valid_gate, lengthof(finish_valid_gate));
	UT_ASSERT_NULL(strstr(source, "cluster_bufmgr_pcm_reload_invalid_pinned"));
	free(source);
}

UT_TEST(test_legacy_byte_proof_republishes_kept_pi_mirror)
{
	static const char *const republish_contract[]
		= { "cluster_pcm_x_grant_pending_republish_shape", "BUF_TYPE_CURRENT", "UnlockBufHdr" };
	char *bufmgr_source = read_bufmgr_source();

	/* A kept-pinned PI mirror regains CURRENT only where its bytes were just
	 * proven current inside the still-open legacy grant lifecycle (the
	 * gcs_block install / storage-fallback call sites are pinned in
	 * test_cluster_gcs_block).  The helper flips only the exact republish
	 * shape under header authority. */
	assert_ordered_in_function(
		bufmgr_source, "\ncluster_bufmgr_pcm_own_republish_grant_pending_image(",
		"\nstatic ClusterPcmOwnResult", republish_contract, lengthof(republish_contract));
	free(bufmgr_source);
}

UT_TEST(test_d5a_release_error_keeps_descriptor_out_of_freelist)
{
	static const char *const fail_closed_contract[]
		= { "BufTableDelete", "LWLockRelease(oldPartitionLock)",
			"PG_TRY",		  "cluster_pcm_lock_release_saved_tag_for_eviction",
			"PG_CATCH",		  "elog(LOG",
			"PG_RE_THROW",	  "StrategyFreeBuffer" };
	char *source = read_bufmgr_source();

	/* A remote release may throw only after the old mapping is gone.  Emit
	 * module evidence and rethrow before StrategyFreeBuffer, leaving the
	 * descriptor unmapped and non-reusable rather than losing a master holder
	 * through descriptor reuse. */
	assert_ordered_in_function(source, "\nInvalidateBufferCommitLocked(",
							   "\n/*\n * InvalidateBufferTry", fail_closed_contract,
							   lengthof(fail_closed_contract));
	free(source);
}

UT_TEST(test_resource_x_target_cached_x_eviction_uses_native_exact_release)
{
	static const char *const lifecycle_contract[]
		= { "cluster_pcm_own_eviction_begin_locked(",
			"UnlockBufHdr",
			"LWLockRelease(partition_lock)",
			"cluster_gcs_resource_x_target_evict_prepare_exact(",
			"LWLockAcquire(partition_lock, LW_EXCLUSIVE)",
			"LockBufHdr(buf)",
			"cluster_pcm_own_eviction_finish_locked(",
			"ClearBufferTag(&buf->tag)",
			"UnlockBufHdr(buf, buf_state)",
			"BufTableDelete(tag, hash)",
			"LWLockRelease(partition_lock)",
			"plan.local_n_committed = true",
			"cluster_gcs_resource_x_target_evict_publish_exact(",
			"StrategyFreeBuffer" };
	static const char *const abort_contract[]
		= { "cluster_gcs_resource_x_target_evict_abort_exact(&plan)",
			"cluster_pcm_own_eviction_abort_locked(",
			"cluster_bufmgr_resource_x_fail_closed_current()" };
	static const char *const commit_entry_contract[]
		= { "cluster_pcm_own_eviction_capture_locked", "cluster_resource_x_writer_path_snapshot(",
			"current_writer_path == RESOURCE_X_WRITER_TARGET",
			"eviction_capture.pcm_state == (uint8)PCM_STATE_X",
			"return cluster_bufmgr_resource_x_target_evict_locked(" };
	char *source = read_bufmgr_source();
	const char *helper;
	const char *helper_end;

	/* TARGET cached-X eviction is one shared four-phase lifecycle.  The helper
	 * fences X first, drops every buffer lock before PREPARE, commits the exact
	 * token only after reacquiring the same mapping/header, and publishes the
	 * frozen plan before an explicit invalidation returns it to the freelist. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_resource_x_target_evict_locked(",
							   "\n/*\n * InvalidateBufferCommitLocked", lifecycle_contract,
							   lengthof(lifecycle_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_resource_x_target_evict_locked(",
							   "\n/*\n * InvalidateBufferCommitLocked", abort_contract,
							   lengthof(abort_contract));
	assert_ordered_in_function(source, "\nInvalidateBufferCommitLocked(",
							   "\n/*\n * InvalidateBufferCommitTailLocked", commit_entry_contract,
							   lengthof(commit_entry_contract));
	helper = strstr(source, "\ncluster_bufmgr_resource_x_target_evict_locked(");
	helper_end = helper == NULL ? NULL : strstr(helper, "\n/*\n * InvalidateBufferCommitLocked");
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	if (helper != NULL && helper_end != NULL) {
		const char *late_commit = strstr(helper, "cluster_pcm_own_eviction_commit_locked(");

		UT_ASSERT(late_commit == NULL || late_commit >= helper_end);
	}
	UT_ASSERT_NULL(strstr(source, "cluster_gcs_resource_x_target_evict_release_exact("));
	free(source);
}

UT_TEST(test_resource_x_target_clock_sweep_eviction_uses_native_exact_release)
{
	static const char *const victim_contract[]
		= { "ClusterPcmOwnEvictionCapture eviction_capture",
			"cluster_pcm_own_eviction_capture_locked",
			"cluster_resource_x_writer_path_snapshot(",
			"current_writer_path == RESOURCE_X_WRITER_TARGET",
			"eviction_capture.pcm_state == (uint8)PCM_STATE_X",
			"return cluster_bufmgr_resource_x_target_evict_locked(" };
	char *source = read_bufmgr_source();
	const char *victim = strstr(source, "\nInvalidateVictimBuffer(");
	const char *victim_end
		= victim != NULL ? strstr(victim, "\nstatic Buffer\nGetVictimBuffer(") : NULL;

	/* Clock-sweep reuse joins the same pre-fenced helper while retaining its
	 * caller pin.  It does not duplicate the protocol or rebuild kind-4 after
	 * the descriptor has become N. */
	assert_ordered_in_function(source, "\nInvalidateVictimBuffer(",
							   "\nstatic Buffer\nGetVictimBuffer(", victim_contract,
							   lengthof(victim_contract));
	UT_ASSERT_NOT_NULL(victim);
	UT_ASSERT_NOT_NULL(victim_end);
	if (victim != NULL && victim_end != NULL) {
		const char *restore = strstr(victim, "buf_hdr->tag = tag");

		UT_ASSERT(restore == NULL || restore >= victim_end);
	}
	free(source);
}

UT_TEST(test_queue_begin_requires_normalized_n_snapshot)
{
	static const char *const normalized_n_gate[]
		= { "expected->pcm_state != (uint8)PCM_STATE_N", "return CLUSTER_PCM_OWN_STALE" };
	char *source = read_bufmgr_source();

	/* Ordinary queued acquisition must use a fresh normalized N snapshot.
	 * Sole-requester S conversion has a separate exact handoff API that reuses
	 * REVOKING and therefore still cannot enter this new-token path. */
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_begin_x_reservation(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_finish_grant_reservation(",
		normalized_n_gate, lengthof(normalized_n_gate));
	free(source);
}

UT_TEST(test_queue_contract_exposes_prepare_only_begin_api)
{
	typedef ClusterPcmOwnResult (*BeginFn)(BufferDesc *, const ClusterPcmOwnSnapshot *, uint64 *);
	typedef ClusterPcmOwnResult (*HandoffFn)(BufferDesc *, const ClusterPcmOwnSnapshot *, uint64 *);
	typedef ClusterPcmOwnResult (*ReleaseSFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
											  ClusterPcmOwnSnapshot *);
	typedef ClusterPcmOwnResult (*FinishFn)(BufferDesc *, const ClusterPcmOwnSnapshot *, uint64,
											uint64 *);
	typedef ClusterPcmOwnResult (*AbortFn)(BufferDesc *, const ClusterPcmOwnSnapshot *, uint64);

	/* The queue owns timing, but not the reservation lifecycle: JOIN/WAIT must
	 * never call this begin API.  ACTIVE_TRANSFER/PREPARE stores the returned
	 * token and all later finish/abort operations are exact. */
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_begin_x_reservation),
										   BeginFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_handoff_s_revoke_to_x_reservation), HandoffFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation), HandoffFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_finish_s_release_to_n), ReleaseSFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_finish_x_commit),
										   FinishFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_abort_x_reservation),
										   AbortFn));
}

UT_TEST(test_queue_contract_exposes_opaque_retained_revoke_api)
{
	typedef ClusterPcmOwnResult (*BeginRevokeFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
												 ClusterPcmOwnSnapshot *);
	typedef ClusterPcmOwnResult (*BeginHeldXRevokeFn)(
		const BufferTag *, const ClusterPcmOwnSnapshot *, ClusterPcmOwnHeldXRevoke *);
	typedef ClusterPcmOwnResult (*AbortHeldXRevokeFn)(ClusterPcmOwnHeldXRevoke *);
	typedef ClusterPcmOwnResult (*TryDrainHeldXRevokeFn)(const ClusterPcmOwnHeldXRevoke *);
	typedef ClusterPcmOwnResult (*TryDrainDropXRevokeFn)(BufferDesc *,
														 const ClusterPcmOwnSnapshot *);
	typedef ClusterPcmOwnResult (*FinishHeldXRevokeFn)(ClusterPcmOwnHeldXRevoke *, XLogRecPtr,
													   ClusterPcmOwnSnapshot *,
													   ClusterPcmOwnFinishRefusal *);
	typedef ClusterPcmOwnResult (*AbandonHeldXRevokeFn)(ClusterPcmOwnHeldXRevoke *);
	typedef ClusterPcmOwnResult (*PrepareNSourceFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
													ClusterPcmOwnSnapshot *, char *, XLogRecPtr *,
													uint64 *);
	typedef ClusterPcmOwnResult (*PrepareSSourceFn)(
		BufferDesc *, const ClusterPcmOwnSnapshot *, SCN, ClusterPcmOwnSnapshot *, char *,
		XLogRecPtr *, uint64 *, ClusterPcmOwnSourcePrepareRefusal *);
	typedef ClusterPcmOwnResult (*AbortRevokeFn)(BufferDesc *, const ClusterPcmOwnSnapshot *);
	typedef ClusterPcmOwnResult (*FinishRetainFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
												  XLogRecPtr, ClusterPcmOwnSnapshot *,
												  ClusterPcmOwnFinishRefusal *);
	typedef ClusterPcmOwnResult (*ReleaseRetainedFn)(const BufferTag *, uint64);
	typedef bool (*ContentWriteFn)(BufferDesc *);

	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_begin_x_revoke),
										   BeginRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_begin_x_revoke_held_by_tag), BeginHeldXRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_abort_held_x_revoke),
										   AbortHeldXRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_try_drain_held_x_revoke), TryDrainHeldXRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_try_drain_drop_x_revoke), TryDrainDropXRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_finish_held_x_revoke_retain), FinishHeldXRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed),
		AbandonHeldXRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_abort_x_revoke),
										   AbortRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_begin_s_revoke),
										   BeginRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_prepare_n_source_image), PrepareNSourceFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_prepare_s_source_image), PrepareSSourceFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_abort_n_revoke),
										   AbortRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_abort_s_revoke),
										   AbortRevokeFn));
	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_finish_revoke_retain),
										   FinishRetainFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_release_retained_image), ReleaseRetainedFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_x_content_write_permitted), ContentWriteFn));
}

UT_TEST(test_queue_n_source_refresh_is_exact_and_publishes_only_complete_image)
{
	static const char *const prepare_contract[]
		= { "PGIOAlignedBlock scratch",
			"expected_n->pcm_state != (uint8)PCM_STATE_N",
			"ReservePrivateRefCountEntry",
			"ResourceOwnerEnlargeBuffers(CurrentResourceOwner)",
			"cluster_pcm_own_fence_matches_locked",
			"BM_VALID",
			"BM_IO_ERROR",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"PinBuffer_Locked",
			"LWLockConditionalAcquire(content_lock, LW_SHARED)",
			"FlushBuffer",
			"UnpinBuffer",
			"BM_IO_IN_PROGRESS",
			"cluster_pcm_own_reservation_begin_exact",
			"PCM_OWN_FLAG_REVOKING",
			"smgrread",
			"PageIsVerifiedExtended",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
			"cluster_bufmgr_pcm_own_copy_source_image_exact(" };
	static const char *const copy_contract[]
		= { "cluster_pcm_own_fence_matches_locked",
			"PCM_OWN_FLAG_REVOKING",
			"BM_VALID",
			"BM_IO_ERROR",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_IN_PROGRESS",
			"memcpy((char *)BufHdrGetBlock(buf), source_page, BLCKSZ)",
			"buf->buffer_type = (uint8)BUF_TYPE_CURRENT",
			"memcpy(block_data, source_page, BLCKSZ)",
			"PageGetLSN(source_page)",
			"pd_block_scn",
			"cluster_pcm_own_snapshot_locked(buf, out_revoking)" };
	char *source = read_bufmgr_source();

	/* READY may be built only after one verified storage scratch has replaced
	 * the exact fenced N descriptor and supplied all image evidence.  A dirty
	 * pre-grant N page is legal (extension / redo dirt): the contract now pins
	 * the flush-then-BUSY convergence (reserve/pin under the header lock,
	 * WAL-first FlushBuffer, unpin) ahead of the reservation, with BM_IO_ERROR
	 * judged before the dirty branch on both passes. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_prepare_n_source_image(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_begin_s_revoke(",
							   prepare_contract, lengthof(prepare_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_copy_source_image_exact(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_prepare_n_source_image(", copy_contract,
		lengthof(copy_contract));
	free(source);
}

UT_TEST(test_queue_s_source_dirty_flush_makes_progress_and_reports_exact_refusal)
{
	static const char *const prepare_contract[]
		= { "ReservePrivateRefCountEntry",
			"ResourceOwnerEnlargeBuffers(CurrentResourceOwner)",
			"BM_IO_ERROR",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"PinBuffer_Locked",
			"LWLockConditionalAcquire(content_lock, LW_SHARED)",
			"CLUSTER_PCM_OWN_SOURCE_PREPARE_REFUSAL_CONTENT_LOCK",
			"FlushBuffer",
			"CLUSTER_PCM_OWN_SOURCE_PREPARE_REFUSAL_DIRTY_FLUSHED",
			"UnpinBuffer",
			"return CLUSTER_PCM_OWN_BUSY",
			"BM_IO_IN_PROGRESS",
			"CLUSTER_PCM_OWN_SOURCE_PREPARE_REFUSAL_IO_IN_PROGRESS",
			"cluster_bufmgr_pcm_own_begin_s_revoke",
			"smgrread",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)" };
	char *source = read_bufmgr_source();

	/* A clean checkpoint can be dirtied again by a SELECT's commit hint before
	 * the S->X self handoff.  That is progress work, not a permanent refusal:
	 * pin and flush it before opening REVOKING, then retry.  Keep I/O and both
	 * content-lock refusals separately diagnosed so a native stall cannot be
	 * collapsed back into an opaque materialize-begin BUSY. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_prepare_s_source_image(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_abort_s_revoke(",
							   prepare_contract, lengthof(prepare_contract));
	free(source);
}

UT_TEST(test_revoke_finish_mode_rejects_pinned_vm_fsm_and_retains_main)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.forkNum = MAIN_FORKNUM;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 0), CLUSTER_PCM_X_REVOKE_FINISH_RETAIN);
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 7), CLUSTER_PCM_X_REVOKE_FINISH_RETAIN);
	tag.forkNum = INIT_FORKNUM;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 3), CLUSTER_PCM_X_REVOKE_FINISH_RETAIN);
	tag.forkNum = FSM_FORKNUM;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 0), CLUSTER_PCM_X_REVOKE_FINISH_DROP);
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 1), CLUSTER_PCM_X_REVOKE_FINISH_BUSY);
	tag.forkNum = VISIBILITYMAP_FORKNUM;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 0), CLUSTER_PCM_X_REVOKE_FINISH_DROP);
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, UINT32_MAX),
				 CLUSTER_PCM_X_REVOKE_FINISH_BUSY);
	tag.forkNum = InvalidForkNumber;
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(&tag, 0), CLUSTER_PCM_X_REVOKE_FINISH_INVALID);
	UT_ASSERT_EQ(cluster_pcm_x_revoke_finish_mode(NULL, 0), CLUSTER_PCM_X_REVOKE_FINISH_INVALID);
}

UT_TEST(test_aux_passive_pin_admission_closes_on_exact_revoking_fence)
{
	char *source;
	const char *lookup_pin;
	const char *lookup_pin_end;
	const char *buffer_alloc;
	const char *buffer_alloc_end;
	const char *recent;
	const char *recent_end;
	const char *extend;
	const char *extend_end;
	static const char *const locked_gate_contract[]
		= { "cluster_bufmgr_pcm_aux_pin_admission_locked(", "PinBuffer_Locked" };

	/* REVOKING is the already-frozen admission fence.  It blocks only a new
	 * passive VM/FSM pin in the active tracked domain; inactive, untracked,
	 * non-auxiliary, and non-REVOKING shapes retain PostgreSQL pin semantics. */
	UT_ASSERT(!cluster_pcm_x_aux_pin_admission_allowed(true, true, VISIBILITYMAP_FORKNUM,
													   PCM_OWN_FLAG_REVOKING));
	UT_ASSERT(
		!cluster_pcm_x_aux_pin_admission_allowed(true, true, FSM_FORKNUM, PCM_OWN_FLAG_REVOKING));
	UT_ASSERT(cluster_pcm_x_aux_pin_admission_allowed(false, true, VISIBILITYMAP_FORKNUM,
													  PCM_OWN_FLAG_REVOKING));
	UT_ASSERT(cluster_pcm_x_aux_pin_admission_allowed(true, false, VISIBILITYMAP_FORKNUM,
													  PCM_OWN_FLAG_REVOKING));
	UT_ASSERT(
		cluster_pcm_x_aux_pin_admission_allowed(true, true, MAIN_FORKNUM, PCM_OWN_FLAG_REVOKING));
	UT_ASSERT(cluster_pcm_x_aux_pin_admission_allowed(true, true, VISIBILITYMAP_FORKNUM, 0));
	UT_ASSERT(cluster_pcm_x_aux_pin_admission_allowed(true, true, VISIBILITYMAP_FORKNUM,
													  PCM_OWN_FLAG_GRANT_PENDING));

	source = read_bufmgr_source();
	lookup_pin = strstr(source, "\nPinBufferForLookup(");
	lookup_pin_end = lookup_pin != NULL ? strstr(lookup_pin, "\nstatic bool\nPinBuffer(") : NULL;
	buffer_alloc = strstr(source, "\nBufferAlloc(");
	buffer_alloc_end
		= buffer_alloc != NULL ? strstr(buffer_alloc, "\n/*\n * InvalidateBuffer") : NULL;
	recent = strstr(source, "\nReadRecentBuffer(");
	recent_end = recent != NULL ? strstr(recent, "\n/*\n * ReadBuffer") : NULL;
	extend = strstr(source, "\nstatic BlockNumber\nExtendBufferedRelShared(");
	extend_end = extend != NULL ? strstr(extend, "\n/*\n * MarkBufferDirty") : NULL;
	UT_ASSERT_NOT_NULL(lookup_pin);
	UT_ASSERT_NOT_NULL(lookup_pin_end);
	UT_ASSERT_NOT_NULL(buffer_alloc);
	UT_ASSERT_NOT_NULL(buffer_alloc_end);
	UT_ASSERT_NOT_NULL(recent);
	UT_ASSERT_NOT_NULL(recent_end);
	UT_ASSERT_NOT_NULL(extend);
	UT_ASSERT_NOT_NULL(extend_end);
	if (lookup_pin != NULL && lookup_pin_end != NULL) {
		static const char *const atomic_contract[] = { "GetPrivateRefCountEntry",
													   "LockBufHdr",
													   "cluster_pcm_x_aux_pin_admission_allowed(",
													   "cluster_pcm_own_flags_get",
													   "PIN_BUFFER_LOOKUP_RETRY",
													   "BUF_REFCOUNT_ONE",
													   "UnlockBufHdr",
													   "NewPrivateRefCountEntry",
													   "ResourceOwnerRememberBuffer" };

		assert_ordered_in_function(source, "\nPinBufferForLookup(", "\nstatic bool\nPinBuffer(",
								   atomic_contract, lengthof(atomic_contract));
	}
	if (buffer_alloc != NULL && buffer_alloc_end != NULL) {
		UT_ASSERT_NOT_NULL(strstr(buffer_alloc, "PinBufferForLookup("));
		UT_ASSERT_NOT_NULL(strstr(buffer_alloc, "cluster_bufmgr_resource_x_wait_retry("));
		UT_ASSERT_NOT_NULL(strstr(buffer_alloc, "goto retry_lookup;"));
	}
	if (recent != NULL && recent_end != NULL) {
		const char *admission = strstr(recent, "cluster_pcm_x_aux_pin_admission_allowed(");
		const char *pin = admission != NULL ? strstr(admission, "PinBuffer_Locked(bufHdr)") : NULL;

		UT_ASSERT_NOT_NULL(admission);
		UT_ASSERT_NOT_NULL(pin);
		if (admission != NULL && pin != NULL)
			UT_ASSERT(admission < pin && pin < recent_end);
	}
	if (extend != NULL && extend_end != NULL) {
		const char *admission = strstr(extend, "PinBufferForLookup(existing_hdr, strategy)");
		const char *legacy = strstr(extend, "PinBuffer(existing_hdr, strategy)");
		const char *retry = strstr(extend, "goto retry_extend_collision;");
		const char *native_wait = strstr(extend, "cluster_bufmgr_resource_x_wait_retry(");
		const char *new_client_error
			= strstr(extend, "cannot reuse revoking auxiliary cluster PCM image");

		UT_ASSERT_NOT_NULL(admission);
		UT_ASSERT(admission == NULL || admission < extend_end);
		UT_ASSERT(legacy == NULL || legacy >= extend_end);
		UT_ASSERT_NOT_NULL(retry);
		UT_ASSERT(retry == NULL || retry < extend_end);
		UT_ASSERT_NOT_NULL(native_wait);
		UT_ASSERT(native_wait == NULL || native_wait < extend_end);
		UT_ASSERT(new_client_error == NULL || new_client_error >= extend_end);
	}
	assert_ordered_in_function(source, "\nstatic Buffer\nGetVictimBuffer(",
							   "\n/*\n * Limit the number of pins", locked_gate_contract,
							   lengthof(locked_gate_contract));
	assert_ordered_in_function(source, "\nSyncOneBuffer(", "\n/*\n *\t\tAtEOXact_Buffers",
							   locked_gate_contract, lengthof(locked_gate_contract));
	assert_ordered_in_function(
		source, "\nFlushRelationBuffers(",
		"\n/* ---------------------------------------------------------------------\n "
		"*\t\tFlushRelationsAllBuffers",
		locked_gate_contract, lengthof(locked_gate_contract));
	assert_ordered_in_function(
		source, "\nFlushRelationsAllBuffers(",
		"\n/* ---------------------------------------------------------------------\n "
		"*\t\tRelationCopyStorageUsingBuffer",
		locked_gate_contract, lengthof(locked_gate_contract));
	assert_ordered_in_function(source, "\nFlushDatabaseBuffers(", "\n/*\n * Flush a previously",
							   locked_gate_contract, lengthof(locked_gate_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_flush_and_release_x_for_leave(",
							   "\n#endif", locked_gate_contract, lengthof(locked_gate_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_lock_resident_for_stamp(",
							   "\nvoid\ncluster_bufmgr_unlock_resident_stamp(",
							   (const char *const[]){ "PinBufferForLookup(" }, 1);
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_lock_resident_for_exact_itl_stamp(",
		"\n/* ========================================================================\n"
		" * PGRAC MODIFICATIONS by SqlRush — spec-6.12a",
		(const char *const[]){ "PinBufferForLookup(" }, 1);
	free(source);
}

UT_TEST(test_pcm_tracking_excludes_only_fsm_for_user_and_shared_catalog_relations)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.relNumber = FirstNormalObjectId;
	tag.forkNum = MAIN_FORKNUM;
	UT_ASSERT(cluster_pcm_x_buffer_tag_tracked(&tag, false));
	tag.forkNum = INIT_FORKNUM;
	UT_ASSERT(cluster_pcm_x_buffer_tag_tracked(&tag, false));
	tag.forkNum = VISIBILITYMAP_FORKNUM;
	UT_ASSERT(cluster_pcm_x_buffer_tag_tracked(&tag, false));
	tag.forkNum = FSM_FORKNUM;
	UT_ASSERT(!cluster_pcm_x_buffer_tag_tracked(&tag, false));

	/* shared_catalog widens the relation-number domain, but must not put its
	 * advisory FSM fork back into the PCM/PCM-X authority domain. */
	tag.relNumber = FirstNormalObjectId - 1;
	tag.forkNum = MAIN_FORKNUM;
	UT_ASSERT(!cluster_pcm_x_buffer_tag_tracked(&tag, false));
	UT_ASSERT(cluster_pcm_x_buffer_tag_tracked(&tag, true));
	tag.forkNum = INIT_FORKNUM;
	UT_ASSERT(cluster_pcm_x_buffer_tag_tracked(&tag, true));
	tag.forkNum = VISIBILITYMAP_FORKNUM;
	UT_ASSERT(cluster_pcm_x_buffer_tag_tracked(&tag, true));
	tag.forkNum = FSM_FORKNUM;
	UT_ASSERT(!cluster_pcm_x_buffer_tag_tracked(&tag, true));
	UT_ASSERT(!cluster_pcm_x_buffer_tag_tracked(NULL, true));
}

UT_TEST(test_pcm_tracking_uses_one_tag_gate_for_acquire_direct_init_and_eviction)
{
	char *source = read_bufmgr_source();
	const char *should_track;
	const char *should_track_end;
	const char *invalidate_commit;
	const char *invalidate_tail;
	const char *victim;
	const char *victim_end;
	const char *direct_init;
	const char *direct_init_end;
	const char *direct_gate;
	const char *arm;
	const char *consume;
	const char *content_lock;

	/* A saved tag must make exactly the same fork decision as a live
	 * BufferDesc.  A relnumber-only eviction gate would leak an FSM release
	 * back into a domain from which acquire was excluded. */
	UT_ASSERT_NULL(strstr(source, "cluster_bufmgr_reln_pcm_tracked"));
	should_track = strstr(source, "\ncluster_bufmgr_should_pcm_track(");
	should_track_end = should_track != NULL ? strstr(should_track, "\n}") : NULL;
	invalidate_commit = strstr(source, "\nInvalidateBufferCommitLocked(");
	invalidate_tail = strstr(source, "\nInvalidateBufferCommitTailLocked(");
	victim = strstr(source, "\nInvalidateVictimBuffer(");
	victim_end = strstr(source, "\nstatic Buffer\nGetVictimBuffer(");
	UT_ASSERT_NOT_NULL(should_track);
	UT_ASSERT_NOT_NULL(should_track_end);
	UT_ASSERT_NOT_NULL(invalidate_commit);
	UT_ASSERT_NOT_NULL(invalidate_tail);
	UT_ASSERT_NOT_NULL(victim);
	UT_ASSERT_NOT_NULL(victim_end);
	if (should_track != NULL && should_track_end != NULL)
		assert_source_range_contains(should_track, should_track_end,
									 "cluster_pcm_x_buffer_tag_tracked");
	if (invalidate_commit != NULL && invalidate_tail != NULL)
		assert_source_range_contains(invalidate_commit, invalidate_tail,
									 "cluster_pcm_x_buffer_tag_tracked");
	if (victim != NULL && victim_end != NULL)
		assert_source_range_contains(victim, victim_end, "cluster_pcm_x_buffer_tag_tracked");

	/* The dedicated VM/FSM initialization wrapper retains provenance.  Its
	 * common tag gate decides whether PCM proof is armed: FSM falls through to
	 * the local content lock, while VM still arms and consumes the exact proof. */
	direct_init = strstr(source, "\nLockBufferForAuxiliaryPageInit(");
	direct_init_end = strstr(source, "\nBuffer\nLockBufferForVisibilityMapPageInit(");
	UT_ASSERT_NOT_NULL(direct_init);
	UT_ASSERT_NOT_NULL(direct_init_end);
	direct_gate
		= direct_init != NULL ? strstr(direct_init, "cluster_bufmgr_should_pcm_track(buf)") : NULL;
	arm = direct_init != NULL ? strstr(direct_init, "cluster_bufmgr_pcm_arm_direct_init") : NULL;
	consume
		= direct_init != NULL ? strstr(direct_init, "cluster_bufmgr_pcm_gate_direct_init") : NULL;
	content_lock = direct_init != NULL
					   ? strstr(direct_init,
								"LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE)")
					   : NULL;
	UT_ASSERT_NOT_NULL(direct_gate);
	UT_ASSERT_NOT_NULL(arm);
	UT_ASSERT_NOT_NULL(consume);
	UT_ASSERT_NOT_NULL(content_lock);
	if (direct_gate != NULL && arm != NULL && consume != NULL && content_lock != NULL)
		UT_ASSERT(direct_gate < arm && arm < consume && consume < content_lock
				  && content_lock < direct_init_end);

	free(source);
}

UT_TEST(test_queue_revoke_retains_main_but_drops_unpinned_vm_fsm)
{
	static const char *const begin_contract[] = { "expected_s->pcm_state != (uint8) PCM_STATE_S",
												  "LockBufHdr",
												  "cluster_pcm_own_gen_get",
												  "cluster_bufmgr_pcm_current_image_locked",
												  "cluster_pcm_own_classify_live_flags",
												  "cluster_pcm_own_reservation_begin_exact",
												  "PCM_OWN_FLAG_REVOKING",
												  "cluster_pcm_own_snapshot_locked",
												  "UnlockBufHdr" };
	static const char *const abort_contract[]
		= { "expected_revoking->pcm_state != (uint8) PCM_STATE_S",
			"LockBufHdr",
			"cluster_pcm_own_classify_live_flags",
			"cluster_pcm_own_reservation_abort_exact",
			"PCM_OWN_FLAG_REVOKING",
			"UnlockBufHdr" };
	static const char *const finish_contract[]
		= { "BufTableHashCode",
			"LWLockAcquire(partition_lock, LW_SHARED)",
			"BufTableLookup",
			"LockBufHdr",
			"cluster_pcm_own_fence_matches_locked",
			"cluster_bufmgr_pcm_current_image_locked",
			"BM_IO_IN_PROGRESS",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockRelease(partition_lock)",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
			"PG_TRY();",
			"cluster_pcm_own_fence_matches_locked",
			"PageGetLSN",
			"FlushBuffer(buf, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL)",
			"LockBufHdr",
			"cluster_pcm_own_fence_matches_locked",
			"cluster_bufmgr_pcm_current_image_locked",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"result = CLUSTER_PCM_OWN_BUSY",
			"PageGetLSN",
			"cluster_pcm_own_revoke_retain_commit_exact",
			"buf->pcm_state = (uint8)PCM_STATE_N",
			"buf->buffer_type = (uint8)BUF_TYPE_PI",
			"BM_DIRTY | BM_JUST_DIRTIED",
			"BM_CHECKPOINT_NEEDED | BM_IO_ERROR",
			"cluster_pcm_own_snapshot_post_state_locked(buf, buf_state, out_retained)",
			"LWLockRelease(content_lock)",
			"cluster_bufmgr_unpin_for_gcs" };
	static const char *const drop_contract[] = { "BufMappingPartitionLock",
												 "LWLockAcquire(partition_lock, LW_EXCLUSIVE)",
												 "BufTableLookup",
												 "LockBufHdr",
												 "BUF_STATE_GET_REFCOUNT",
												 "cluster_pcm_own_flags_get",
												 "BM_IO_IN_PROGRESS",
												 "CLUSTER_PCM_X_REVOKE_FINISH_BUSY",
												 "CLUSTER_PCM_OWN_FINISH_REFUSAL_VM_FSM_PINNED",
												 "CLUSTER_PCM_OWN_FINISH_REFUSAL_IO_IN_PROGRESS",
												 "CLUSTER_PCM_OWN_FINISH_REFUSAL_LIVE_FLAGS",
												 "PageGetLSN",
												 "cluster_pcm_own_revoke_commit_exact",
												 "buf->pcm_state = (uint8)PCM_STATE_N",
												 "cluster_pcm_own_snapshot_locked",
												 "InvalidateBufferCommitTailLocked" };
	static const char *const held_begin_contract[]
		= { "cluster_pcm_x_revoke_finish_mode(tag, 0)",
			"LWLockAcquire(partition_lock, LW_SHARED)",
			"BufTableLookup",
			"LockBufHdr",
			"cluster_pcm_own_fence_matches_locked",
			"cluster_bufmgr_pcm_current_image_locked",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockRelease(partition_lock)",
			"cluster_bufmgr_pcm_own_begin_x_revoke(",
			"cluster_bufmgr_unpin_for_gcs(buf)",
			"held_out->revoking = revoking",
			"held_out->flags = CLUSTER_PCM_OWN_HELD_X_REVOKE_KNOWN_MASK" };
	static const char *const held_abort_contract[]
		= { "cluster_bufmgr_pcm_own_abort_x_revoke(", "if (result != CLUSTER_PCM_OWN_OK)",
			"cluster_bufmgr_unpin_for_gcs(buf)", "memset(held, 0, sizeof(*held))" };
	static const char *const held_drain_contract[]
		= { "LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
			"LockBufHdr(buf)",
			"cluster_pcm_own_fence_matches_locked",
			"cluster_bufmgr_pcm_current_image_locked",
			"BM_IO_IN_PROGRESS",
			"BM_IO_ERROR",
			"UnlockBufHdr(buf, buf_state)",
			"LWLockRelease(content_lock)" };
	static const char *const held_finish_contract[]
		= { "cluster_bufmgr_pcm_own_finish_revoke_retain(", "if (result != CLUSTER_PCM_OWN_OK)",
			"cluster_bufmgr_unpin_for_gcs(buf)", "memset(held, 0, sizeof(*held))" };
	static const char *const held_abandon_contract[] = { "LockBufHdr",
														 "cluster_pcm_own_fence_matches_locked",
														 "cluster_bufmgr_pcm_current_image_locked",
														 "UnlockBufHdr",
														 "cluster_bufmgr_unpin_for_gcs(buf)",
														 "memset(held, 0, sizeof(*held))" };
	char *source = read_bufmgr_source();
	const char *begin_s;
	const char *abort_s;
	const char *begin_x;
	const char *abort_x;
	const char *drop_helper;
	const char *held_drain;
	const char *held_drain_end;
	const char *finish;
	const char *finish_end;

	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_begin_s_revoke(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_abort_s_revoke(",
							   begin_contract, lengthof(begin_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_abort_s_revoke(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_begin_x_revoke(",
							   abort_contract, lengthof(abort_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_finish_revoke_retain(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_release_retained_image(", finish_contract,
		lengthof(finish_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_finish_revoke_drop_unpinned(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_revoke_retain(", drop_contract,
		lengthof(drop_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_begin_x_revoke_held_by_tag(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_abort_held_x_revoke(",
							   held_begin_contract, lengthof(held_begin_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_abort_held_x_revoke(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_held_x_revoke_retain(",
		held_abort_contract, lengthof(held_abort_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_try_drain_held_x_revoke(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_held_x_revoke_retain(",
		held_drain_contract, lengthof(held_drain_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_finish_held_x_revoke_retain(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed(",
		held_finish_contract, lengthof(held_finish_contract));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_abandon_held_x_revoke_after_fail_closed(",
		"\nstatic ClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_revoke_drop_unpinned(",
		held_abandon_contract, lengthof(held_abandon_contract));

	/* Main/init passive PG pins are not PCM holders, so their retained commit
	 * must not recreate the S3 pin ring.  VM/FSM take the separate exact-drop
	 * arm above, where a foreign pin returns BUSY before any ownership commit. */
	begin_s = strstr(source, "\ncluster_bufmgr_pcm_own_begin_s_revoke(");
	abort_s = strstr(source, "\ncluster_bufmgr_pcm_own_abort_s_revoke(");
	begin_x = strstr(source, "\ncluster_bufmgr_pcm_own_begin_x_revoke(");
	abort_x = strstr(source, "\ncluster_bufmgr_pcm_own_abort_x_revoke(");
	drop_helper = strstr(source, "\ncluster_bufmgr_pcm_own_finish_revoke_drop_unpinned(");
	finish = strstr(source, "\ncluster_bufmgr_pcm_own_finish_revoke_retain(");
	finish_end = strstr(source, "\ncluster_bufmgr_pcm_own_release_retained_image(");
	held_drain = strstr(source, "\ncluster_bufmgr_pcm_own_try_drain_held_x_revoke(");
	held_drain_end
		= held_drain != NULL
			  ? strstr(held_drain,
					   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_held_x_revoke_retain(")
			  : NULL;
	UT_ASSERT_NOT_NULL(begin_s);
	UT_ASSERT_NOT_NULL(abort_s);
	UT_ASSERT_NOT_NULL(begin_x);
	UT_ASSERT_NOT_NULL(abort_x);
	UT_ASSERT_NOT_NULL(drop_helper);
	UT_ASSERT_NOT_NULL(finish);
	UT_ASSERT_NOT_NULL(finish_end);
	UT_ASSERT_NOT_NULL(held_drain);
	UT_ASSERT_NOT_NULL(held_drain_end);
	if (held_drain != NULL && held_drain_end != NULL) {
		const char *forbidden;

		forbidden = strstr(held_drain, "LWLockAcquire(content_lock");
		UT_ASSERT(forbidden == NULL || forbidden >= held_drain_end);
		forbidden = strstr(held_drain, "LWLockAcquireOrWait(");
		UT_ASSERT(forbidden == NULL || forbidden >= held_drain_end);
		forbidden = strstr(held_drain, "WaitLatch(");
		UT_ASSERT(forbidden == NULL || forbidden >= held_drain_end);
		forbidden = strstr(held_drain, "pg_usleep(");
		UT_ASSERT(forbidden == NULL || forbidden >= held_drain_end);
		forbidden = strstr(held_drain, "FlushBuffer(");
		UT_ASSERT(forbidden == NULL || forbidden >= held_drain_end);
		forbidden = strstr(held_drain, "cluster_pcm_own_revoke_retain_commit_exact(");
		UT_ASSERT(forbidden == NULL || forbidden >= held_drain_end);
	}
	/* buffer_type is a monotone hint: every exact source lifecycle must
	 * accept a yielded S+XCUR through the centralized current-image gate. */
	assert_source_range_contains(begin_s, abort_s, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(abort_s, begin_x, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(begin_x, abort_x, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(abort_x, drop_helper, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(drop_helper, finish, "cluster_bufmgr_pcm_current_image_locked");
	assert_source_range_contains(finish, finish_end, "cluster_bufmgr_pcm_current_image_locked");
	if (begin_s != NULL && begin_x != NULL)
		UT_ASSERT(strstr(begin_s, "BUF_STATE_GET_REFCOUNT") == NULL
				  || strstr(begin_s, "BUF_STATE_GET_REFCOUNT") >= begin_x);
	if (begin_x != NULL && abort_x != NULL)
		UT_ASSERT(strstr(begin_x, "BUF_STATE_GET_REFCOUNT") == NULL
				  || strstr(begin_x, "BUF_STATE_GET_REFCOUNT") >= abort_x);
	if (finish != NULL && finish_end != NULL) {
		const char *refcount = strstr(finish, "BUF_STATE_GET_REFCOUNT");
		const char *drop = strstr(finish, "InvalidateBuffer");
		const char *legacy_pi = strstr(finish, "cluster_bufmgr_convert_to_pi_locked");
		const char *mapping = strstr(finish, "partition_lock");
		const char *conditional
			= strstr(finish, "LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)");

		UT_ASSERT(refcount == NULL || refcount >= finish_end);
		UT_ASSERT(drop == NULL || drop >= finish_end);
		UT_ASSERT(legacy_pi == NULL || legacy_pi >= finish_end);
		UT_ASSERT_NOT_NULL(mapping);
		UT_ASSERT_NOT_NULL(conditional);
		if (mapping != NULL && conditional != NULL)
			UT_ASSERT(mapping < conditional);
	}
	free(source);
}

UT_TEST(test_retained_image_release_and_writeback_gates_are_exact)
{
	static const char *const content_write_contract[]
		= { "LockBufHdr",
			"cluster_pcm_own_flags_get",
			"PCM_OWN_FLAG_REVOKING",
			"cluster_bufmgr_pcm_x_retained_image_locked",
			"PCM_OWN_FLAG_GRANT_PENDING",
			"UnlockBufHdr" };
	static const char *const release_contract[]
		= { "source_generation + 1",
			"BufTableHashCode",
			"LWLockAcquire(partition_lock, LW_SHARED)",
			"BufTableLookup",
			"LockBufHdr",
			"PCM_OWN_FLAG_REVOKING",
			"LWLockRelease(partition_lock)",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
			"LockBufHdr",
			"BufferTagsEqual",
			"BM_VALID",
			"BUF_TYPE_PI",
			"PCM_STATE_N",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED | BM_IO_ERROR",
			"PCM_OWN_FLAG_REVOKING",
			"cluster_pcm_own_revoke_retain_release_exact" };
	char *source = read_bufmgr_source();
	const char *victim;
	const char *sync;
	const char *flush;
	const char *dirty;
	const char *hint;
	const char *lockbuffer;
	const char *conditional;
	const char *resident_stamp;
	const char *storage_refresh;

	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_release_retained_image(",
		"\n/* ========================================================================\n * PGRAC "
		"MODIFICATIONS by SqlRush — spec-6.12h D-h2",
		release_contract, lengthof(release_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_content_write_permitted(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_snapshot_by_tag(",
							   content_write_contract, lengthof(content_write_contract));

	/* The content-lock ordering is the race proof: an already-started flush
	 * owns SHARE and finishes before retain; after retain owns EXCLUSIVE, all
	 * later Sync/Flush/dirty paths see the immutable retained shape. */
	victim = strstr(source, "\nInvalidateVictimBuffer(");
	sync = strstr(source, "\nSyncOneBuffer(");
	flush = strstr(source, "\nFlushBuffer(");
	dirty = strstr(source, "\nMarkBufferDirty(Buffer buffer)");
	hint = strstr(source, "\nMarkBufferDirtyHint(Buffer buffer, bool buffer_std)");
	lockbuffer = strstr(source, "\nLockBufferInternal(Buffer buffer, int mode");
	conditional = strstr(source, "\nConditionalLockBuffer(Buffer buffer)");
	resident_stamp = strstr(source, "\ncluster_bufmgr_lock_resident_for_stamp(");
	storage_refresh = strstr(source, "\ncluster_bufmgr_refresh_block_from_storage_for_gcs(");
	UT_ASSERT_NOT_NULL(victim);
	UT_ASSERT_NOT_NULL(sync);
	UT_ASSERT_NOT_NULL(flush);
	UT_ASSERT_NOT_NULL(dirty);
	UT_ASSERT_NOT_NULL(hint);
	UT_ASSERT_NOT_NULL(lockbuffer);
	UT_ASSERT_NOT_NULL(conditional);
	UT_ASSERT_NOT_NULL(resident_stamp);
	UT_ASSERT_NOT_NULL(storage_refresh);
	if (victim != NULL)
		UT_ASSERT(strstr(victim, "cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked")
				  < strstr(victim, "ClearBufferTag"));
	if (sync != NULL) {
		const char *first_gate
			= strstr(sync, "cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked");
		const char *content_share
			= strstr(sync, "LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED)");
		const char *second_gate
			= first_gate != NULL
				  ? strstr(first_gate + 1,
						   "cluster_bufmgr_pcm_x_retained_image_reuse_blocked_locked")
				  : NULL;

		UT_ASSERT_NOT_NULL(first_gate);
		UT_ASSERT_NOT_NULL(content_share);
		UT_ASSERT_NOT_NULL(second_gate);
		if (first_gate != NULL)
			UT_ASSERT(first_gate < strstr(sync, "result |= BUF_REUSABLE"));
		if (content_share != NULL && second_gate != NULL)
			UT_ASSERT(content_share < second_gate);
	}
	if (flush != NULL)
		UT_ASSERT(strstr(flush, "cluster_bufmgr_pcm_x_retained_image_locked")
				  < strstr(flush, "StartBufferIO(buf, false)"));
	if (dirty != NULL)
		UT_ASSERT(strstr(dirty, "cluster_bufmgr_pcm_x_retained_image_locked")
				  < strstr(dirty, "buf_state |= BM_DIRTY"));
	if (hint != NULL) {
		const char *tracked = strstr(hint, "cluster_bufmgr_should_pcm_track(bufHdr)");
		const char *gate = strstr(hint, "cluster_pcm_x_content_holder_mutation_allowed(");
		const char *refuse = gate != NULL ? strstr(gate, "return;") : NULL;
		const char *dirty_flags = strstr(hint, "BM_DIRTY | BM_JUST_DIRTIED");

		/* Hint dirt is optional.  A kept pinned N/PI mirror may have its
		 * in-memory hint byte touched, but without live S/X current authority it
		 * must not regain writeback eligibility and block a later storage refresh. */
		UT_ASSERT_NOT_NULL(tracked);
		UT_ASSERT_NOT_NULL(gate);
		UT_ASSERT_NOT_NULL(refuse);
		UT_ASSERT_NOT_NULL(dirty_flags);
		if (tracked != NULL && gate != NULL && refuse != NULL && dirty_flags != NULL)
			UT_ASSERT(gate < tracked && tracked < refuse && refuse < dirty_flags);
	}
	if (lockbuffer != NULL) {
		const char *reserve
			= strstr(lockbuffer, "cluster_bufmgr_pcm_begin_grant_reservation_wait(");
		const char *content
			= strstr(lockbuffer, "LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_SHARED)");
		const char *w1_reverify
			= strstr(lockbuffer, "cluster_pcm_x_cached_cover_reverify_accepts(");

		UT_ASSERT_NOT_NULL(reserve);
		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(w1_reverify);
		if (reserve != NULL && content != NULL)
			UT_ASSERT(reserve < content);
	}
	if (conditional != NULL) {
		const char *content
			= strstr(conditional, "LWLockConditionalAcquire(BufferDescriptorGetContentLock(buf)");
		const char *ownership = strstr(conditional, "cluster_pcm_x_conditional_lock_allowed(");
		const char *release
			= strstr(conditional, "LWLockRelease(BufferDescriptorGetContentLock(buf))");

		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(ownership);
		UT_ASSERT_NOT_NULL(release);
		if (content != NULL && ownership != NULL && release != NULL)
			UT_ASSERT(content < ownership && ownership < release);
	}
	if (resident_stamp != NULL) {
		const char *content = strstr(
			resident_stamp, "LWLockAcquire(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE)");
		const char *gate = strstr(resident_stamp, "cluster_bufmgr_pcm_x_retained_image_locked");

		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(gate);
		if (content != NULL && gate != NULL)
			UT_ASSERT(content < gate);
	}
	if (storage_refresh != NULL) {
		const char *content = strstr(storage_refresh, "LWLockAcquire(content_lock, LW_EXCLUSIVE)");
		const char *gate = strstr(storage_refresh, "cluster_bufmgr_pcm_x_content_write_permitted");
		const char *copy = strstr(storage_refresh, "memcpy((char *) BufHdrGetBlock(buf)");

		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(gate);
		UT_ASSERT_NOT_NULL(copy);
		if (content != NULL && gate != NULL && copy != NULL)
			UT_ASSERT(content < gate && gate < copy);
	}
	free(source);
}

UT_TEST(test_retained_drain_retags_invalid_only_after_exact_token_release)
{
	static const char *const drain_contract[]
		= { "cluster_pcm_own_revoke_retain_release_exact", "result == CLUSTER_PCM_OWN_OK",
			"cluster_pcm_x_retained_release_retag", "&buf->buffer_type", "UnlockBufHdr" };
	char *source = read_bufmgr_source();
	const char *release;
	const char *release_end;

	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_release_retained_image(",
		"\n/* ========================================================================\n * PGRAC "
		"MODIFICATIONS by SqlRush — spec-6.12h D-h2",
		drain_contract, lengthof(drain_contract));

	release = strstr(source, "\ncluster_bufmgr_pcm_own_release_retained_image(");
	release_end = release != NULL ? strstr(release + 1, "\n/* "
														"=========================================="
														"==============================\n * PGRAC ")
								  : NULL;
	UT_ASSERT_NOT_NULL(release);
	UT_ASSERT_NOT_NULL(release_end);
	if (release != NULL && release_end != NULL)
		UT_ASSERT(strstr(release, "InvalidateBufferCommitTailLocked") == NULL
				  || strstr(release, "InvalidateBufferCommitTailLocked") >= release_end);

	free(source);
}

UT_TEST(test_source_settlement_releases_fence_without_discarding_pi)
{
	static const char *const preserve_contract[]
		= { "source_generation == 0",		"cluster_pcm_own_revoke_retain_release_exact",
			"result == CLUSTER_PCM_OWN_OK", "buf->buffer_type != (uint8) BUF_TYPE_PI",
			"(buf_state & BM_VALID) == 0",	"UnlockBufHdr" };
	char *source = read_bufmgr_source();
	const char *preserve;
	const char *preserve_end;

	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_release_retained_fence_preserve_pi(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_release_retained_image(", preserve_contract,
		lengthof(preserve_contract));
	preserve = strstr(source, "\ncluster_bufmgr_pcm_own_release_retained_fence_preserve_pi(");
	preserve_end
		= preserve != NULL
			  ? strstr(preserve + 1,
					   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_release_retained_image(")
			  : NULL;
	UT_ASSERT_NOT_NULL(preserve);
	UT_ASSERT_NOT_NULL(preserve_end);
	if (preserve != NULL && preserve_end != NULL) {
		const char *retag = strstr(preserve, "cluster_pcm_x_retained_release_retag");

		UT_ASSERT(retag == NULL || retag >= preserve_end);
	}
	free(source);
}

UT_TEST(test_source_settlement_post_release_n_pi_remains_exactly_observable)
{
	static const char *const observe_contract[]
		= { "expected_n->flags != PCM_OWN_FLAG_REVOKING",
			"expected_n->flags != 0",
			"cluster_pcm_own_snapshot_equal_exact(&live, expected_n)",
			"live.flags == PCM_OWN_FLAG_REVOKING || live.flags == 0",
			"live.pcm_state == (uint8)PCM_STATE_N",
			"live.buffer_type == (uint8)BUF_TYPE_PI",
			"(live.semantic_buf_state & BM_VALID) != 0" };
	char *source = read_bufmgr_source();

	/* SourceSettlement clears the retained token before its entry-lock commit.
	 * The exact undrained pair is checked by the caller; this BufferDesc half
	 * must recognize both sides of that bounded physical-release window. */
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_n_retained_release_inflight_exact(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_n_storage_candidate_exact(",
		observe_contract, lengthof(observe_contract));
	free(source);
}

UT_TEST(test_queue_s_release_finish_is_header_exact_and_returns_fresh_n)
{
	static const char *const release_contract[] = { "expected_s->pcm_state != (uint8) PCM_STATE_S",
													"expected_s->flags != 0",
													"LockBufHdr",
													"cluster_pcm_own_fence_matches_locked",
													"cluster_pcm_own_bump_locked",
													"buf->pcm_state = (uint8) PCM_STATE_N",
													"cluster_pcm_own_snapshot_locked",
													"UnlockBufHdr" };
	char *source = read_bufmgr_source();

	/* The caller proves the exact remote RELEASE application ACK before this
	 * adapter.  The adapter then normalizes only the matching S tuple and
	 * returns the fresh N generation that PREPARE must reserve. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_finish_s_release_to_n(",
							   "\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_begin_x_reservation(",
							   release_contract, lengthof(release_contract));
	free(source);
}

UT_TEST(test_resource_x_remote_s_finish_requires_content_and_exact_revoke)
{
	typedef ClusterPcmOwnResult (*RemoteSFinishFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
												   ClusterPcmOwnSnapshot *);
	static const char *const finish_contract[]
		= { "LWLockHeldByMe(BufferDescriptorGetContentLock(buf))",
			"expected_revoking->pcm_state != (uint8)PCM_STATE_S",
			"expected_revoking->flags != PCM_OWN_FLAG_REVOKING",
			"expected_revoking->reservation_token == 0",
			"LockBufHdr(buf)",
			"cluster_pcm_own_fence_matches_locked(buf, expected_revoking)",
			"cluster_bufmgr_pcm_current_image_locked(buf, buf_state)",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"cluster_pcm_own_revoke_commit_exact(",
			"buf->pcm_state = (uint8)PCM_STATE_N",
			"buf->buffer_type = (uint8)BUF_TYPE_CURRENT",
			"cluster_pcm_own_snapshot_locked(buf, out_n_snapshot)",
			"UnlockBufHdr(buf, buf_state)" };
	char *source = read_bufmgr_source();

	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_finish_remote_s_block_to_n), RemoteSFinishFn));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_finish_remote_s_block_to_n(",
							   "\n/*\n * Release a passively pinned MAIN/INIT S mirror",
							   finish_contract, lengthof(finish_contract));
	free(source);
}

UT_TEST(test_r11_lockbuffer_writer_selector_is_single_ingress_choice_and_exclusive)
{
	static const char *const selector_contract[]
		= { "pcm_writer_path = cluster_resource_x_writer_path_snapshot(",
			"if (pcm_writer_path != RESOURCE_X_WRITER_TARGET)",
			"cluster_bufmgr_resource_x_writer_report_failure(",
			"R11 target-only writer selector",
			"for (;;)",
			"cluster_bufmgr_pcm_x_writer_prepare_target(",
			"pcm_writer_r4_generation",
			"&pcm_x_absolute_deadline_us",
			"LWLockAcquire(",
			"cluster_bufmgr_pcm_x_writer_activate(",
			"pcm_x_writer, true",
			"LWLockRelease(",
			"cluster_lockbuffer_barrier_refusal" };
	char *source = read_bufmgr_source();

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	/* One LockBuffer ingress selection, two ordinary TARGET post-T3/content-lock
	 * revalidations, two known-new direct-init proof revalidations, and the
	 * direct-init ledger bind's exact post-T3 revalidation, plus one cached-X
	 * explicit-invalidation and clock-sweep eviction snapshots under descriptor
	 * authority.  None may resnapshot into
	 * the other implementation. */
	UT_ASSERT_EQ(count_occurrences(source, "cluster_resource_x_writer_path_snapshot("), 8);
	if (strstr(source, "cluster_resource_x_writer_path_snapshot(") == NULL) {
		free(source);
		return;
	}
	assert_ordered_in_function(source, "\nLockBufferInternal(Buffer buffer, int mode",
							   "\nvoid\nLockBuffer(", selector_contract,
							   lengthof(selector_contract));
	/* The selected target wrapper receives the exact sampled R4 generation. */
	UT_ASSERT_NOT_NULL(strstr(source, "&pcm_x_absolute_deadline_us"));
	free(source);
}

UT_TEST(test_queue_holder_snapshot_by_tag_is_mapping_and_header_exact)
{
	typedef ClusterPcmOwnResult (*SnapshotByTagFn)(const BufferTag *, int *,
												   ClusterPcmOwnSnapshot *);
	static const char *const snapshot_contract[]
		= { "BufTableHashCode", "LWLockAcquire(partition_lock, LW_SHARED)",
			"BufTableLookup",	"GetBufferDescriptor",
			"LockBufHdr",		"BufferTagsEqual",
			"BM_VALID",			"cluster_pcm_own_snapshot_locked",
			"UnlockBufHdr",		"LWLockRelease(partition_lock)" };
	char *source = read_bufmgr_source();

	UT_ASSERT(__builtin_types_compatible_p(__typeof__(&cluster_bufmgr_pcm_own_snapshot_by_tag),
										   SnapshotByTagFn));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_snapshot_by_tag(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_finish_s_release_to_n(", snapshot_contract,
		lengthof(snapshot_contract));
	free(source);
}

UT_TEST(test_queue_passive_pinned_s_release_serializes_bytes_and_ownership)
{
	typedef ClusterPcmOwnResult (*PassiveReleaseFn)(const BufferTag *, XLogRecPtr *, uint64 *);
	static const char *const release_contract[]
		= { "BufTableHashCode",
			"LWLockAcquire(partition_lock, LW_SHARED)",
			"BufTableLookup",
			"LockBufHdr",
			"cluster_pcm_x_revoke_finish_mode(tag, shared_refcount)",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockRelease(partition_lock)",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
			"cluster_pcm_own_fence_matches_locked",
			"PageGetLSN",
			"FlushBuffer(buf, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL)",
			"LockBufHdr",
			"cluster_pcm_own_fence_matches_locked",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"result = CLUSTER_PCM_OWN_BUSY",
			"cluster_pcm_own_bump_locked",
			"buf->pcm_state = (uint8) PCM_STATE_N",
			"buf->buffer_type = (uint8) BUF_TYPE_PI",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"BM_IO_ERROR",
			"cluster_bufmgr_unpin_for_gcs" };
	char *source = read_bufmgr_source();

	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_release_pinned_s_for_gcs), PassiveReleaseFn));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_release_pinned_s_for_gcs(",
		"\nClusterPcmOwnResult\ncluster_bufmgr_pcm_own_publish_installed_x_image(",
		release_contract, lengthof(release_contract));
	free(source);
}

UT_TEST(test_current_image_shape_accepts_monotone_xcur_after_x_to_s_yield)
{
	UT_ASSERT(cluster_pcm_x_current_image_shape((uint8)PCM_STATE_S, (uint8)BUF_TYPE_SCUR, true));
	UT_ASSERT(cluster_pcm_x_current_image_shape((uint8)PCM_STATE_S, (uint8)BUF_TYPE_XCUR, true));
	UT_ASSERT(!cluster_pcm_x_current_image_shape((uint8)PCM_STATE_X, (uint8)BUF_TYPE_SCUR, true));
	UT_ASSERT(cluster_pcm_x_current_image_shape((uint8)PCM_STATE_X, (uint8)BUF_TYPE_XCUR, true));
	UT_ASSERT(!cluster_pcm_x_current_image_shape((uint8)PCM_STATE_N, (uint8)BUF_TYPE_PI, true));
	UT_ASSERT(!cluster_pcm_x_current_image_shape((uint8)PCM_STATE_S, (uint8)BUF_TYPE_PI, true));
	UT_ASSERT(!cluster_pcm_x_current_image_shape((uint8)PCM_STATE_S, (uint8)BUF_TYPE_XCUR, false));
}

UT_TEST(test_conditional_lock_preserves_native_off_and_enforces_tracked_x)
{
	UT_ASSERT(
		cluster_pcm_x_conditional_lock_allowed(false, true, false, (uint8)PCM_STATE_N, 0, 0, 0));
	UT_ASSERT(
		cluster_pcm_x_conditional_lock_allowed(true, false, false, (uint8)PCM_STATE_N, 0, 0, 0));
	UT_ASSERT(
		!cluster_pcm_x_conditional_lock_allowed(true, true, false, (uint8)PCM_STATE_N, 0, 0, 0));
	UT_ASSERT(
		!cluster_pcm_x_conditional_lock_allowed(true, true, false, (uint8)PCM_STATE_S, 0, 0, 0));
	UT_ASSERT(
		cluster_pcm_x_conditional_lock_allowed(true, true, false, (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(
		!cluster_pcm_x_conditional_lock_allowed(true, true, false, (uint8)PCM_STATE_X, 0, 7, 0));
	UT_ASSERT(
		!cluster_pcm_x_conditional_lock_allowed(true, true, false, (uint8)PCM_STATE_X, 0, 0, 41));
	UT_ASSERT(
		!cluster_pcm_x_conditional_lock_allowed(false, false, true, (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(!cluster_pcm_x_conditional_lock_allowed(false, false, false, (uint8)PCM_STATE_X,
													  PCM_OWN_FLAG_GRANT_PENDING, 0, 0));
}

UT_TEST(test_resource_x_ordinary_mutation_gate_dominates_dirty_hint_and_flush)
{
	static const char *const dirty_contract[]
		= { "LockBufHdr", "cluster_pcm_x_content_holder_mutation_allowed(", "UnlockBufHdr",
			"pg_atomic_read_u32(&bufHdr->state)" };
	static const char *const hint_contract[]
		= { "LockBufHdr", "cluster_pcm_x_content_holder_mutation_allowed(", "UnlockBufHdr",
			"XLogHintBitIsNeeded()" };
	static const char *const flush_contract[]
		= { "LockBufHdr", "cluster_pcm_x_flush_fence_consistent(", "UnlockBufHdr",
			"StartBufferIO(buf, false)" };
	char *source;

	UT_ASSERT(
		cluster_pcm_x_ordinary_mutation_allowed(false, true, false, (uint8)PCM_STATE_N, 0, 0, 0));
	UT_ASSERT(
		cluster_pcm_x_ordinary_mutation_allowed(true, false, false, (uint8)PCM_STATE_N, 0, 0, 0));
	UT_ASSERT(
		cluster_pcm_x_ordinary_mutation_allowed(true, true, false, (uint8)PCM_STATE_X, 0, 0, 0));
	UT_ASSERT(
		!cluster_pcm_x_ordinary_mutation_allowed(true, true, false, (uint8)PCM_STATE_X, 0, 12, 0));
	UT_ASSERT(
		!cluster_pcm_x_ordinary_mutation_allowed(true, true, false, (uint8)PCM_STATE_X, 0, 0, 41));
	UT_ASSERT(
		!cluster_pcm_x_ordinary_mutation_allowed(true, true, false, (uint8)PCM_STATE_S, 0, 0, 0));
	UT_ASSERT(cluster_pcm_x_flush_fence_consistent(false, 12, 41));
	UT_ASSERT(!cluster_pcm_x_flush_fence_consistent(true, 12, 0));
	UT_ASSERT(cluster_pcm_x_flush_fence_consistent(true, 0, 0));

	source = read_bufmgr_source();
	assert_ordered_in_function(source, "\nMarkBufferDirty(", "\n/*\n * ReleaseAndReadBuffer",
							   dirty_contract, lengthof(dirty_contract));
	assert_ordered_in_function(source, "\nMarkBufferDirtyHint(",
							   "\n/*\n * Release buffer content locks", hint_contract,
							   lengthof(hint_contract));
	assert_ordered_in_function(source, "\nFlushBuffer(", "\n/*\n * RelationGetNumberOfBlocksInFork",
							   flush_contract, lengthof(flush_contract));
	free(source);
}

UT_TEST(test_preexisting_content_holder_can_finish_before_pre_retention_drain)
{
	static const char *const dirty_contract[]
		= { "LockBufHdr", "cluster_pcm_x_content_holder_mutation_allowed(", "UnlockBufHdr",
			"pg_atomic_read_u32(&bufHdr->state)" };
	static const char *const hint_contract[]
		= { "LockBufHdr", "cluster_pcm_x_content_holder_mutation_allowed(", "UnlockBufHdr",
			"XLogHintBitIsNeeded()" };
	static const char *const itl_write_contract[]
		= { "LWLockHeldByMeInMode(", "LockBufHdr", "cluster_pcm_x_content_holder_mutation_allowed(",
			"PCM_OWN_FLAG_REVOKING", "UnlockBufHdr" };
	char *source;
	ClusterPcmOwnSnapshot captured;
	ClusterPcmOwnSnapshot live;

	/* REVOKING still closes every new ordinary entrance.  A foreground that
	 * already owns BufferContent predates the nonblocking drain, however, and
	 * must be allowed to finish so that the drain observes BUSY, aborts the
	 * exact revoke, and returns to the event loop. */
	UT_ASSERT(!cluster_pcm_x_ordinary_mutation_allowed(true, true, false, (uint8)PCM_STATE_X,
													   PCM_OWN_FLAG_REVOKING, 0, 0));
	UT_ASSERT(cluster_pcm_x_content_holder_mutation_allowed(true, true, false, (uint8)PCM_STATE_X,
															PCM_OWN_FLAG_REVOKING, 0, 0));
	UT_ASSERT(!cluster_pcm_x_content_holder_mutation_allowed(true, true, true, (uint8)PCM_STATE_X,
															 PCM_OWN_FLAG_REVOKING, 0, 0));
	UT_ASSERT(!cluster_pcm_x_content_holder_mutation_allowed(true, true, false, (uint8)PCM_STATE_S,
															 PCM_OWN_FLAG_REVOKING, 0, 0));
	UT_ASSERT(!cluster_pcm_x_content_holder_mutation_allowed(true, true, false, (uint8)PCM_STATE_X,
															 PCM_OWN_FLAG_REVOKING, 12, 0));
	UT_ASSERT(!cluster_pcm_x_content_holder_mutation_allowed(true, true, false, (uint8)PCM_STATE_X,
															 PCM_OWN_FLAG_REVOKING, 0, 41));

	/* A27: the same pre-existing content-X bracket can observe the exact
	 * nonblocking drain before or after its reversible REVOKING publication.
	 * Token/flag churn is not a new current-block authority; every immutable
	 * axis and every non-revoke lifecycle shape remains strict. */
	memset(&captured, 0, sizeof(captured));
	captured.tag.spcOid = 1;
	captured.tag.dbOid = 2;
	captured.tag.relNumber = 319;
	captured.tag.forkNum = MAIN_FORKNUM;
	captured.tag.blockNum = 7;
	captured.generation = UINT64_C(83);
	captured.reservation_token = UINT64_C(140);
	captured.pcm_state = (uint8)PCM_STATE_X;
	live = captured;
	live.reservation_token++;
	live.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(cluster_pcm_x_content_holder_dml_authority_equivalent(&captured, &live));
	UT_ASSERT(cluster_pcm_x_content_holder_dml_authority_equivalent(&live, &captured));
	live.flags = 0;
	UT_ASSERT(cluster_pcm_x_content_holder_dml_authority_equivalent(&captured, &live));
	live = captured;
	live.generation++;
	UT_ASSERT(!cluster_pcm_x_content_holder_dml_authority_equivalent(&captured, &live));
	live = captured;
	live.tag.blockNum++;
	UT_ASSERT(!cluster_pcm_x_content_holder_dml_authority_equivalent(&captured, &live));
	live = captured;
	live.pcm_state = (uint8)PCM_STATE_S;
	UT_ASSERT(!cluster_pcm_x_content_holder_dml_authority_equivalent(&captured, &live));
	live = captured;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT(!cluster_pcm_x_content_holder_dml_authority_equivalent(&captured, &live));
	live = captured;
	live.flags = PCM_OWN_FLAG_REVOKING;
	live.reservation_token = 0;
	UT_ASSERT(!cluster_pcm_x_content_holder_dml_authority_equivalent(&captured, &live));
	live = captured;
	live.resource_x_activation_generation = UINT64_C(41);
	UT_ASSERT(!cluster_pcm_x_content_holder_dml_authority_equivalent(&captured, &live));

	source = read_bufmgr_source();
	assert_ordered_in_function(source, "\nMarkBufferDirty(", "\n/*\n * ReleaseAndReadBuffer",
							   dirty_contract, lengthof(dirty_contract));
	assert_ordered_in_function(source, "\nMarkBufferDirtyHint(",
							   "\n/*\n * Release buffer content locks", hint_contract,
							   lengthof(hint_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_block_write_permitted(", "\n#endif",
							   itl_write_contract, lengthof(itl_write_contract));
	free(source);
}

UT_TEST(test_resource_x_t2_t3_buffer_owner_is_generation_exact_and_ordered)
{
	static const char *const t2_contract[]
		= { "BufTableLookup",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
			"cluster_gcs_block_compute_checksum(image->page_bytes)",
			"memcpy(page, image->page_bytes, BLCKSZ)",
			"PageSetLSN",
			"cluster_pcm_own_resource_x_activation_bind_exact(",
			"cluster_pcm_own_snapshot_locked",
			"LWLockRelease(content_lock)",
			"cluster_bufmgr_unpin_for_gcs" };
	static const char *const t3_contract[]
		= { "BufTableLookup",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockConditionalAcquire(content_lock, LW_EXCLUSIVE)",
			"cluster_pcm_x_resource_x_t3_snapshot_exact",
			"cluster_pcm_own_resource_x_activation_clear_exact(",
			"cluster_pcm_own_snapshot_locked",
			"LWLockRelease(content_lock)",
			"cluster_bufmgr_unpin_for_gcs" };
	ResourceXAcquisitionRef ref;
	ClusterPcmOwnSnapshot live;
	char *source;

	memset(&ref, 0, sizeof(ref));
	ref.assertion.resource.spcOid = 1663;
	ref.assertion.resource.dbOid = 1;
	ref.assertion.resource.relNumber = 100;
	ref.assertion.resource.forkNum = MAIN_FORKNUM;
	ref.assertion.resource.blockNum = 71;
	ref.assertion.requester_node = 2;
	ref.formation = 17;
	ref.acquisition_generation = 41;
	memset(&live, 0, sizeof(live));
	live.tag = ref.assertion.resource;
	live.generation = 9;
	live.reservation_token = 12;
	live.writer_activation_token = 12;
	live.pcm_state = (uint8)PCM_STATE_X;
	UT_ASSERT_EQ(sizeof(ResourceXCurrentImage), 32);
	UT_ASSERT(cluster_pcm_x_resource_x_t2_snapshot_exact(&ref, &live));
	live.resource_x_activation_generation = ref.acquisition_generation;
	UT_ASSERT(cluster_pcm_x_resource_x_t2_snapshot_exact(&ref, &live));
	UT_ASSERT(cluster_pcm_x_resource_x_t3_snapshot_exact(&ref, &live));
	live.resource_x_activation_generation++;
	UT_ASSERT(!cluster_pcm_x_resource_x_t2_snapshot_exact(&ref, &live));
	UT_ASSERT(!cluster_pcm_x_resource_x_t3_snapshot_exact(&ref, &live));
	live.resource_x_activation_generation = ref.acquisition_generation;
	live.writer_activation_token = 0;
	UT_ASSERT(!cluster_pcm_x_resource_x_t2_snapshot_exact(&ref, &live));
	UT_ASSERT(!cluster_pcm_x_resource_x_t3_snapshot_exact(&ref, &live));

	source = read_bufmgr_source();
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_activate_x_by_tag(",
							   "\n/* T3 removes the Resource-X generation", t2_contract,
							   lengthof(t2_contract));
	assert_ordered_in_function(source,
							   "\ncluster_bufmgr_pcm_own_writer_activation_clear_by_tag_exact(",
							   "\n/* R8 reconfiguration owner", t3_contract, lengthof(t3_contract));
	free(source);
}

UT_TEST(test_queue_passive_n_mirror_is_never_gcs_ship_authority)
{
	static const char *const probe_contract[]
		= { "LockBufHdr", "cluster_bufmgr_pcm_current_image_locked", "UnlockBufHdr" };
	static const char *const copy_contract[]
		= { "LockBufHdr",
			"cluster_bufmgr_pcm_current_image_locked",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockConditionalAcquire(content_lock, LW_SHARED)",
			"cluster_bufmgr_pcm_current_image_locked",
			"FlushBuffer(buf, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL)",
			"BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED",
			"memcpy(dst, page, BLCKSZ)" };
	static const char *const live_sge_contract[]
		= { "LockBufHdr",
			"cluster_bufmgr_pcm_current_image_locked",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockConditionalAcquire(content_lock, LW_SHARED)",
			"cluster_bufmgr_pcm_current_image_locked",
			"*out_page_addr = page" };
	static const char *const smart_contract[]
		= { "LockBufHdr",
			"cluster_bufmgr_pcm_current_image_locked",
			"cluster_bufmgr_pin_for_gcs_locked",
			"LWLockConditionalAcquire(content_lock, LW_SHARED)",
			"cluster_bufmgr_pcm_current_image_locked",
			"memcpy(dst, page, BLCKSZ)" };
	char *source = read_bufmgr_source();

	assert_ordered_in_function(source, "\ncluster_bufmgr_probe_block_for_gcs(",
							   "\n/*\n * Read the shared-storage version", probe_contract,
							   lengthof(probe_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_copy_block_for_gcs(",
							   "\n/*\n * Borrow a live shared_buffers page", copy_contract,
							   lengthof(copy_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_borrow_block_for_gcs_live_sge(",
							   "\nvoid\ncluster_bufmgr_release_block_for_gcs_live_sge(",
							   live_sge_contract, lengthof(live_sge_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_copy_block_for_gcs_smart_fusion(",
							   "\n/*\n * cluster_bufmgr_redeclare_scan_chunk", smart_contract,
							   lengthof(smart_contract));
	free(source);
}

UT_TEST(test_gcs_ship_copy_reports_exact_nonblocking_refusal_stage)
{
	static const char *const refusal_contract[]
		= { "CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NOT_RESIDENT",
			"CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CURRENT_INVALID",
			"CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_FIRST",
			"CLUSTER_BUFMGR_GCS_COPY_REFUSAL_CONTENT_LOCK_SECOND",
			"CLUSTER_BUFMGR_GCS_COPY_REFUSAL_WAL_RECHECK_CHANGED" };
	char *source = read_bufmgr_source();
	const char *copy
		= source != NULL ? strstr(source, "\ncluster_bufmgr_copy_block_for_gcs(") : NULL;
	const char *copy_end
		= copy != NULL ? strstr(copy, "\n/*\n * Borrow a live shared_buffers page") : NULL;
	int i;

	/* P0-21 observation contract: every nonblocking false return that can
	 * become holder-side DENIED_MASTER_NOT_HOLDER identifies the precise
	 * residency/current-image/content-lock/WAL-recheck refusal stage. */
	UT_ASSERT_NOT_NULL(copy);
	UT_ASSERT_NOT_NULL(copy_end);
	if (copy != NULL && copy_end != NULL) {
		for (i = 0; i < lengthof(refusal_contract); i++) {
			const char *hit = strstr(copy, refusal_contract[i]);

			UT_ASSERT_NOT_NULL(hit);
			if (hit != NULL)
				UT_ASSERT(hit < copy_end);
		}
	}
	free(source);
}

UT_TEST(test_queue_installed_image_publication_is_exact_and_content_locked)
{
	typedef ClusterPcmOwnResult (*PublishImageFn)(BufferDesc *, const ClusterPcmOwnSnapshot *,
												  uint64);
	static const char *const publish_contract[]
		= { "LWLockHeldByMe(BufferDescriptorGetContentLock(buf))",
			"LockBufHdr",
			"BufferTagsEqual",
			"cluster_pcm_own_gen_get",
			"cluster_pcm_own_reservation_token_get",
			"PCM_OWN_FLAG_GRANT_PENDING",
			"buf->pcm_state != (uint8) PCM_STATE_N",
			"buf_state |= BM_VALID",
			"UnlockBufHdr" };
	char *source = read_bufmgr_source();

	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_bufmgr_pcm_own_publish_installed_x_image), PublishImageFn));
	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_publish_installed_x_image(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_begin_grant_reservation(", publish_contract,
		lengthof(publish_contract));
	free(source);
}

UT_TEST(test_queue_self_source_handoff_is_single_lifecycle_and_readonly_drain)
{
	static const char *const handoff_contract[] = { "LWLockHeldByMe(content_lock)",
													"LockBufHdr",
													"cluster_pcm_own_classify_live_flags",
													"cluster_bufmgr_pcm_current_image_locked",
													"cluster_pcm_own_revoke_to_grant_handoff_exact",
													"UnlockBufHdr" };
	static const char *const drain_proof_contract[] = { "BufMappingPartitionLock",
														"BufTableLookup",
														"CLUSTER_PCM_OWN_OK",
														"LockBufHdr",
														"cluster_pcm_own_classify_live_flags",
														"UnlockBufHdr",
														"CLUSTER_PCM_OWN_CORRUPT" };
	char *source = read_bufmgr_source();
	const char *handoff;
	const char *handoff_end;
	const char *forbidden;

	assert_ordered_in_function(
		source, "\ncluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(",
		"\nstatic ClusterPcmOwnResult\ncluster_pcm_own_finish_grant_reservation(", handoff_contract,
		lengthof(handoff_contract));
	handoff = strstr(source, "\ncluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(");
	handoff_end
		= handoff != NULL
			  ? strstr(handoff,
					   "\nstatic ClusterPcmOwnResult\ncluster_pcm_own_finish_grant_reservation(")
			  : NULL;
	UT_ASSERT_NOT_NULL(handoff);
	UT_ASSERT_NOT_NULL(handoff_end);
	if (handoff != NULL && handoff_end != NULL) {
		forbidden = strstr(handoff, "cluster_pcm_own_reservation_begin_exact(");
		UT_ASSERT(forbidden == NULL || forbidden >= handoff_end);
		forbidden = strstr(handoff, "cluster_pcm_own_reservation_abort_exact(");
		UT_ASSERT(forbidden == NULL || forbidden >= handoff_end);
	}
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_self_handoff_probe(",
							   "\n/* ==========", drain_proof_contract,
							   lengthof(drain_proof_contract));
	free(source);
}

UT_TEST(test_pcm_x_retain_flush_error_injection_is_exact_and_pre_write)
{
	static const char *const finish_flush_contract[]
		= { "bool finish_fault_armed",
			"cluster_injection_is_armed(\"cluster-pcm-x-retain-flush-error\")",
			"forced_test_flush = finish_fault_armed &&",
			"cluster_pcm_x_retain_flush_error_target_matches(",
			"expected_revoking->tag.spcOid",
			"expected_revoking->tag.dbOid",
			"expected_revoking->tag.relNumber",
			"(int) expected_revoking->tag.forkNum",
			"expected_revoking->tag.blockNum",
			"log_non_target_finish = finish_fault_armed && !forced_test_flush",
			"if (forced_test_flush)",
			"buf_state |= BM_DIRTY | BM_JUST_DIRTIED",
			"needs_flush =",
			"cluster_pcm_x_finish_retain_flush_active = true",
			"cluster_pcm_x_finish_retain_flush_fault_active = forced_test_flush",
			"FlushBuffer(buf, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL)",
			"cluster_pcm_x_finish_retain_flush_fault_active = false",
			"cluster_pcm_x_finish_retain_flush_active = false",
			"cluster PCM-X retained-image finish FlushBuffer succeeded" };
	static const char *const flush_error_contract[]
		= { "if (!StartBufferIO(buf, false))",
			"cluster_pcm_x_finish_retain_flush_io_active = true",
			"cluster_pcm_x_finish_retain_flush_active",
			"cluster_pcm_x_finish_retain_flush_fault_active",
			"cluster_pcm_own_flags_get(buf->buf_id) == PCM_OWN_FLAG_REVOKING",
			"CLUSTER_INJECTION_POINT(\"cluster-pcm-x-retain-flush-error\")",
			"cluster_injection_should_skip(\"cluster-pcm-x-retain-flush-error\")",
			"errmsg(\"injected PCM-X retained-image FlushBuffer failure\")",
			"smgrwrite(",
			"TerminateBufferIO(buf, true, 0)",
			"cluster_pcm_x_finish_retain_flush_io_active = false" };
	static const char *const catch_contract[]
		= { "PG_CATCH();",
			"cluster_pcm_x_finish_retain_flush_fault_active = false",
			"cluster_pcm_x_finish_retain_flush_active = false",
			"if (cluster_pcm_x_finish_retain_flush_error_context_pushed)",
			"error_context_stack = cluster_pcm_x_finish_retain_flush_error_context_previous",
			"if (content_locked && LWLockHeldByMe(content_lock))",
			"HOLD_INTERRUPTS();",
			"LWLockRelease(content_lock)",
			"if (cluster_pcm_x_finish_retain_flush_io_active)",
			"cluster_pcm_x_finish_retain_flush_io_active = false",
			"AbortBufferIO(BufferDescriptorGetBuffer(buf))",
			"cluster_bufmgr_unpin_for_gcs(buf)",
			"PG_RE_THROW();" };
	static const char *const non_target_contract[]
		= { "PG_END_TRY();",
			"if (caller_pinned)",
			"cluster_bufmgr_unpin_for_gcs(buf)",
			"if (result == CLUSTER_PCM_OWN_OK && log_non_target_finish)",
			"cluster PCM-X retained-image finish fault skipped non-target",
			"actual=%u/%u/%u/%d/%u target=\\\"%s\\\"",
			"cluster_pcm_x_retain_flush_error_target" };
	char *source = read_bufmgr_source();
	const char *finish;
	const char *catch;
	const char *rethrow;
	const char *resume;

	/* The point is armed only at the finish-revoke-retain seam.  An exact
	 * configured BufferTag makes an otherwise already-flushed copy dirty
	 * without changing bytes, so the caller-pin/content-EXCLUSIVE FlushBuffer
	 * leg is deterministic.  An armed non-target records evidence only after
	 * successful revalidation, unlock, and unpin; it neither dirties nor enters
	 * the dispatch scope.  The generic flush path dispatches the point only
	 * while that exact matching call is active and its ownership token is still
	 * REVOKING.  ERROR clears both dynamic flags before cleanup.  It also clears
	 * process interrupt holdoff count before longjmp, so the cleanup must
	 * restore FlushBuffer's stack-local error callback, release content
	 * authority with a replacement hold, abort the exact ResourceOwner-tracked
	 * BufferIO while its raw pin is still live, and only then unpin.  Otherwise
	 * an aux worker that absorbs the ERROR reaches commit-style resource-owner
	 * cleanup and PANICs with "lost track of buffer IO".  LWLockRelease consumes
	 * the replacement hold itself; a second RESUME would underflow in cassert
	 * builds and an unconditional HOLD would leak on the no-lock path. */
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_finish_revoke_retain(",
							   "\ncluster_bufmgr_pcm_own_release_retained_image(",
							   finish_flush_contract, lengthof(finish_flush_contract));
	assert_ordered_in_function(source, "\nFlushBuffer(", "\n/*\n * RelationGetNumberOfBlocksInFork",
							   flush_error_contract, lengthof(flush_error_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_finish_revoke_retain(",
							   "\ncluster_bufmgr_pcm_own_release_retained_image(", catch_contract,
							   lengthof(catch_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_own_finish_revoke_retain(",
							   "\ncluster_bufmgr_pcm_own_release_retained_image(",
							   non_target_contract, lengthof(non_target_contract));
	finish = strstr(source, "\ncluster_bufmgr_pcm_own_finish_revoke_retain(");
	UT_ASSERT_NOT_NULL(finish);
	catch = strstr(finish, "PG_CATCH();");
	UT_ASSERT_NOT_NULL(catch);
	rethrow = strstr(catch, "PG_RE_THROW();");
	UT_ASSERT_NOT_NULL(rethrow);
	resume = strstr(catch, "RESUME_INTERRUPTS();");
	UT_ASSERT(resume == NULL || resume > rethrow);
	free(source);
}

UT_TEST(test_resource_x_preuse_drift_reprobes_only_current_valid_tuple)
{
	ClusterPcmOwnSnapshot live;

	memset(&live, 0, sizeof(live));
	live.generation = UINT64_C(83);
	live.reservation_token = UINT64_C(140);
	live.pcm_state = (uint8)PCM_STATE_N;
	UT_ASSERT(cluster_pcm_x_target_preuse_drift_retryable(&live, true, true, true));

	/* A successor may be settled already or may still own the exact local
	 * reservation.  Re-probing grants nothing; the ordinary target acquire
	 * must classify the current round again under the original deadline. */
	live.pcm_state = (uint8)PCM_STATE_S;
	UT_ASSERT(cluster_pcm_x_target_preuse_drift_retryable(&live, true, true, true));
	live.pcm_state = (uint8)PCM_STATE_X;
	live.flags = PCM_OWN_FLAG_REVOKING;
	live.reservation_token = UINT64_C(141);
	UT_ASSERT(cluster_pcm_x_target_preuse_drift_retryable(&live, true, true, true));
	live.pcm_state = (uint8)PCM_STATE_N;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT(cluster_pcm_x_target_preuse_drift_retryable(&live, true, true, true));

	/* Formation/R4/tag drift and malformed ownership tuples never enter the
	 * local retry.  They retain the existing fail-closed classification. */
	UT_ASSERT(!cluster_pcm_x_target_preuse_drift_retryable(&live, false, true, true));
	UT_ASSERT(!cluster_pcm_x_target_preuse_drift_retryable(&live, true, false, true));
	UT_ASSERT(!cluster_pcm_x_target_preuse_drift_retryable(&live, true, true, false));
	live.flags = PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(!cluster_pcm_x_target_preuse_drift_retryable(&live, true, true, true));
	live.flags = PCM_OWN_FLAG_REVOKING;
	live.reservation_token = 0;
	UT_ASSERT(!cluster_pcm_x_target_preuse_drift_retryable(&live, true, true, true));
	live.flags = 0;
	live.reservation_token = UINT64_C(140);
	live.generation = 0;
	UT_ASSERT(!cluster_pcm_x_target_preuse_drift_retryable(&live, true, true, true));
	live.generation = UINT64_MAX;
	UT_ASSERT(!cluster_pcm_x_target_preuse_drift_retryable(&live, true, true, true));
	live.generation = UINT64_C(83);
	live.reservation_token = UINT64_MAX;
	UT_ASSERT(!cluster_pcm_x_target_preuse_drift_retryable(&live, true, true, true));
	live.reservation_token = UINT64_C(140);
	live.pcm_state = UINT8_C(255);
	UT_ASSERT(!cluster_pcm_x_target_preuse_drift_retryable(&live, true, true, true));
}

UT_TEST(test_resource_x_target_writer_context_is_post_t3_and_local_cleanup_only)
{
	static const char *const target_contract[]
		= { "entry->phase = PCM_X_WRITER_LEDGER_HANDOFF",
			"cluster_gcs_resource_x_target_acquire_until_exact(",
			"absolute_deadline_us",
			"context.ref = terminal_ref",
			"context.r4_record_generation = r4_generation",
			"context.buffer_ownership_generation = granted.generation",
			"context.writer_activation_token = 0",
			"context.resource_x_activation_generation = 0",
			"cluster_gcs_resource_x_target_context_recheck_exact(",
			"&context",
			"cluster_pcm_x_target_preuse_drift_retryable(",
			"continue;",
			"entry->authority = context",
			"entry->phase = PCM_X_WRITER_LEDGER_ACQUIRING" };
	static const char *const activation_contract[]
		= { "!LWLockHeldByMe(entry->content_lock)",
			"cluster_bufmgr_pcm_own_snapshot(buf, &live)",
			"cluster_gcs_resource_x_target_context_recheck_result_exact(",
			"if (allow_reprobe",
			"cluster_pcm_x_target_preuse_drift_retryable(",
			"cluster_bufmgr_pcm_x_writer_clear(entry)",
			"return false;",
			"entry->phase = PCM_X_WRITER_LEDGER_ACTIVE",
			"return true;" };
	static const char *const recycle_contract[]
		= { "cluster_gcs_resource_x_target_itl_recycle_begin_exact(",
			"entry->phase = PCM_X_WRITER_LEDGER_RECYCLING" };
	static const char *const recycle_relock_failure_contract[]
		= { "result = cluster_gcs_resource_x_target_itl_recycle_finish_exact(",
			"if (result != RESOURCE_X_APPLY_APPLIED)", "LWLockRelease(content_lock)",
			"cluster_bufmgr_itl_recycle_guard_cancel(buffer)", "return false;" };
	static const char *const direct_init_ledger_contract[]
		= { "cluster_pcm_lock_resource_x_gate_snapshot(&gate)",
			"current_path = cluster_resource_x_writer_path_snapshot(",
			"current_r4_generation != context->r4_record_generation",
			"granted.generation != context->buffer_ownership_generation",
			"granted.writer_activation_token != 0",
			"granted.resource_x_activation_generation != 0",
			"cluster_gcs_resource_x_target_context_recheck_result_exact(context)",
			"entry->authority = *context",
			"entry->phase = PCM_X_WRITER_LEDGER_ACQUIRING" };
	static const char *const cleanup_forbidden[]
		= { "cluster_pcm_lock_release(", "cluster_pcm_lock_resource_x_release_x_exact(",
			"RESOURCE_X_WIRE_RELEASE_X" };
	char *source = read_bufmgr_source();
	const char *ledger;
	const char *ledger_end;
	size_t i;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	UT_ASSERT_EQ(sizeof(ResourceXWriterUseContext), 72);
	UT_ASSERT_NOT_NULL(strstr(source, "ResourceXWriterUseContext authority"));
	UT_ASSERT_NOT_NULL(strstr(source, "CLUSTER_BUFMGR_ITL_RECYCLE_RETRY_REQUALIFY"));
	ledger = strstr(source, "typedef struct ClusterPcmXWriterLedgerEntry {");
	ledger_end = ledger != NULL ? strstr(ledger, "} ClusterPcmXWriterLedgerEntry;") : NULL;
	UT_ASSERT_NOT_NULL(ledger);
	UT_ASSERT_NOT_NULL(ledger_end);
	if (ledger != NULL && ledger_end != NULL) {
		const char *writer_path = strstr(ledger, "ResourceXWriterPath writer_path");

		UT_ASSERT(writer_path == NULL || writer_path >= ledger_end);
	}
	UT_ASSERT_NULL(strstr(source, "ResourceXWriterUseContext target"));
	UT_ASSERT_NULL(strstr(source, "RESOURCE_X_WRITER_SOURCE"));
	UT_ASSERT_NULL(strstr(source, "PcmXLocalWriterClaim"));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_prepare_target(",
							   "\nstatic bool\ncluster_bufmgr_pcm_x_writer_activate(",
							   target_contract, lengthof(target_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_activate(",
							   "\n/* Direct-init obtains its exact TARGET grant",
							   activation_contract, lengthof(activation_contract));
	UT_ASSERT_NOT_NULL(strstr(source, "cluster_pcm_direct_init_target_commit_validate("));
	UT_ASSERT_NOT_NULL(strstr(source, "cluster_bufmgr_pcm_x_writer_track_target_direct_init("));
	assert_ordered_in_function(source, "\ncluster_bufmgr_pcm_x_writer_track_target_direct_init(",
							   "\nstatic bool\ncluster_bufmgr_pcm_x_writer_activate(",
							   direct_init_ledger_contract, lengthof(direct_init_ledger_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_itl_recycle_guard_arm(",
							   "\nvoid\ncluster_bufmgr_itl_recycle_guard_unlock(", recycle_contract,
							   lengthof(recycle_contract));
	assert_ordered_in_function(source, "\ncluster_bufmgr_itl_recycle_guard_relock(",
							   "\nvoid\ncluster_bufmgr_itl_recycle_guard_cancel(",
							   recycle_relock_failure_contract,
							   lengthof(recycle_relock_failure_contract));
	for (i = 0; i < lengthof(cleanup_forbidden); i++)
		UT_ASSERT_NULL(strstr(source, cleanup_forbidden[i]));
	free(source);
}

int
main(void)
{
	UT_PLAN(129);
	UT_RUN(test_aux_creation_disposition_uses_real_beb_and_excludes_retained_context);
	UT_RUN(test_real_barrier_refusal_ignores_another_callers_pending);
	UT_RUN(test_real_barrier_refusal_abort_then_successor_is_not_own_residue);
	UT_RUN(test_real_barrier_refusal_rejects_own_lock_and_ledger);
	UT_RUN(test_real_barrier_refusal_preserves_identity_failures);
	UT_RUN(test_real_preassert_discards_completed_conversion_observation);
	UT_RUN(test_real_preassert_requalifies_late_unowned_auxiliary_retag);
	UT_RUN(test_real_preassert_requalifies_late_unowned_auxiliary_readiness);
	UT_RUN(test_real_capture_domain_preserves_owned_and_stable_refusals);
	UT_RUN(test_real_pending_recapture_keeps_original_observation);
	UT_RUN(test_real_pending_observation_rechecks_successor_and_preserves_refusals);
	UT_RUN(test_preassert_resample_is_not_an_identity_or_deadline_exception);
	UT_RUN(test_real_preassert_rechecks_changed_image_and_preserves_failures);
	UT_RUN(test_pcm_own_snapshot_equality_is_whole_object_exact);
	UT_RUN(test_bufmgr_snapshot_matches_rejects_writer_token_only_drift);
	UT_RUN(test_bufmgr_snapshot_captures_image_type);
	UT_RUN(test_bufmgr_snapshot_captures_each_semantic_buffer_bit);
	UT_RUN(test_bufmgr_owned_fence_survives_flush_but_observation_resamples);
	UT_RUN(test_bufmgr_fence_rejects_every_non_image_byte_drift);
	UT_RUN(test_bufmgr_snapshot_ignores_refcount_usage_and_pin_waiter);
	UT_RUN(test_snapshot_post_state_uses_unpublished_locked_state);
	UT_RUN(test_actual_delivery_arm_preserves_foreground_io_initializer);
	UT_RUN(test_actual_delivery_arm_keeps_valid_target_and_existing_hold_guards);
	UT_RUN(test_actual_delivery_arm_rejects_unproved_initializer_without_mutation);
	UT_RUN(test_delivery_dispatch_waits_for_ordinary_read_image_owner);
	UT_RUN(test_assert_dispatch_reclaims_only_abandoned_read_image);
	UT_RUN(test_abandoned_read_image_adapter_preserves_all_refusal_boundaries);
	UT_RUN(test_abandoned_read_image_cleanup_balances_exception_and_does_not_grant);
	UT_RUN(test_delivery_dispatch_preserves_ordinary_read_abort_then_arms);
	UT_RUN(test_real_known_new_sidecar_rejects_installed_remote_image);
	UT_RUN(test_real_remote_image_t2_t3_use_image_proof_not_page_is_new);
	UT_RUN(test_installed_claim_shape_is_exact_and_not_a_new_base);
	UT_RUN(test_real_source_copy_then_finish_preserves_owned_fence);
	UT_RUN(test_real_finish_flush_uses_current_image_and_rejects_failures);
	UT_RUN(test_real_finish_failed_flush_rethrows_without_losing_fence);
	UT_RUN(test_real_source_finish_busy_is_owned_wait_not_global_failure);
	UT_RUN(test_real_source_pin_is_continuous_across_busy_and_owner_adoption);
	UT_RUN(test_real_source_tick_observation_gap_keeps_same_pin_until_completion);
	UT_RUN(test_real_selected_s_finish_wait_keeps_hard_refusals);
	UT_RUN(test_real_aux_selected_s_waits_for_preexisting_pins_without_own_pin);
	UT_RUN(test_real_source_finish_publishes_only_after_busy_clears_and_preserves_flush_error);
	UT_RUN(test_real_source_copy_returns_post_replacement_image_type);
	UT_RUN(test_snapshot_classifiers_use_the_captured_physical_inputs);
	UT_RUN(test_shmem_initializes_complete_entry);
	UT_RUN(test_delivery_hold_blocks_idle_target_retag_after_caller_exit);
	UT_RUN(test_delivery_hold_exact_t2_preserves_residency_until_real_t3);
	UT_RUN(test_delivery_hold_prevents_failed_direct_init_owner_from_aborting_delivery);
	UT_RUN(test_delivery_hold_rejects_unrelated_s_or_x_reservation);
	UT_RUN(test_real_delivery_residency_owns_exact_heap_vm_fsm_descriptor);
	UT_RUN(test_resource_x_activation_binding_is_exact_and_legacy_closed);
	UT_RUN(test_resource_x_reconfig_neutralize_is_generation_exact_and_nonblocking);
	UT_RUN(test_writer_activation_fence_blocks_revoke_until_exact_clear);
	UT_RUN(test_begin_abort_is_exact_and_monotonic);
	UT_RUN(test_invalid_live_flag_shapes_are_corrupt_not_busy);
	UT_RUN(test_remote_s_holder_pending_grant_is_retryable_busy);
	UT_RUN(test_real_remote_s_candidate_waits_for_exact_local_reservation);
	UT_RUN(test_real_remote_s_candidate_preserves_corruption_and_observation_guards);
	UT_RUN(test_remote_s_holder_stable_n_replay_requires_exact_idle_tuple);
	UT_RUN(test_grant_commit_is_exact_and_bumps_once);
	UT_RUN(test_s_revoke_handoff_reuses_exact_token_and_bumps_once);
	UT_RUN(test_revoke_handoff_kinds_cover_n_s_x_with_one_lifecycle);
	UT_RUN(test_s_new_fresh_token_finish_shape_stays_invalid);
	UT_RUN(test_parallel_s_cover_is_rechecked_before_new_reservation);
	UT_RUN(test_share_cover_reverify_accepts_stable_successor_grant);
	UT_RUN(test_retained_release_retag_respects_pin_contract);
	UT_RUN(test_passive_retained_pi_is_an_n_assertion_candidate_only);
	UT_RUN(test_retained_release_and_finish_never_cover_invalid_bytes);
	UT_RUN(test_legacy_byte_proof_republishes_kept_pi_mirror);
	UT_RUN(test_revoke_commit_is_exact_and_classifies_live_races);
	UT_RUN(test_revoke_retain_commit_keeps_exact_token_until_release);
	UT_RUN(test_revoke_commit_exhaustion_is_side_effect_free);
	UT_RUN(test_token_and_generation_never_wrap);
	UT_RUN(test_ordinary_generation_bump_rejects_live_reservation);
	UT_RUN(test_eviction_rejects_live_reservation_and_exhaustion);
	UT_RUN(test_target_eviction_bufferdesc_lifecycle_is_reversible_before_local_commit);
	UT_RUN(test_bufmgr_d5a_commitlocked_uses_locked_commit_and_saved_tag_release);
	UT_RUN(test_bufmgr_abort_cleanup_is_never_silent);
	UT_RUN(test_bufmgr_finish_failure_rolls_back_acquired_master_grant);
	UT_RUN(test_bufmgr_s_base_rollback_normalizes_to_n_under_header_authority);
	UT_RUN(test_bufmgr_generation_bump_failure_is_classified_under_header_lock);
	UT_RUN(test_read_image_lifecycle_is_exact_monotonic_and_header_atomic);
	UT_RUN(test_share_read_image_publication_has_no_clean_n_observation_gap);
	UT_RUN(test_read_image_normal_unlock_clears_exactly_before_content_release);
	UT_RUN(test_read_image_abandoned_reclaim_wait_is_content_x_and_successor_exact);
	UT_RUN(test_pending_x_denied_retry_drops_removed_queue_gap);
	UT_RUN(test_resource_x_s_barrier_aborts_one_racing_reservation_then_parks);
	UT_RUN(test_bufmgr_finish_rejects_invalid_state_and_initializes_acquire_result);
	UT_RUN(test_bufmgr_finish_and_abort_gate_on_exact_base_state);
	UT_RUN(test_d5a_release_error_keeps_descriptor_out_of_freelist);
	UT_RUN(test_resource_x_target_cached_x_eviction_uses_native_exact_release);
	UT_RUN(test_resource_x_target_clock_sweep_eviction_uses_native_exact_release);
	UT_RUN(test_queue_begin_requires_normalized_n_snapshot);
	UT_RUN(test_queue_contract_exposes_prepare_only_begin_api);
	UT_RUN(test_queue_contract_exposes_opaque_retained_revoke_api);
	UT_RUN(test_queue_n_source_refresh_is_exact_and_publishes_only_complete_image);
	UT_RUN(test_queue_s_source_dirty_flush_makes_progress_and_reports_exact_refusal);
	UT_RUN(test_revoke_finish_mode_rejects_pinned_vm_fsm_and_retains_main);
	UT_RUN(test_aux_passive_pin_admission_closes_on_exact_revoking_fence);
	UT_RUN(test_pcm_tracking_excludes_only_fsm_for_user_and_shared_catalog_relations);
	UT_RUN(test_pcm_tracking_uses_one_tag_gate_for_acquire_direct_init_and_eviction);
	UT_RUN(test_queue_revoke_retains_main_but_drops_unpinned_vm_fsm);
	UT_RUN(test_retained_image_release_and_writeback_gates_are_exact);
	UT_RUN(test_retained_drain_retags_invalid_only_after_exact_token_release);
	UT_RUN(test_source_settlement_releases_fence_without_discarding_pi);
	UT_RUN(test_source_settlement_post_release_n_pi_remains_exactly_observable);
	UT_RUN(test_queue_s_release_finish_is_header_exact_and_returns_fresh_n);
	UT_RUN(test_resource_x_remote_s_finish_requires_content_and_exact_revoke);
	UT_RUN(test_r11_lockbuffer_writer_selector_is_single_ingress_choice_and_exclusive);
	UT_RUN(test_queue_holder_snapshot_by_tag_is_mapping_and_header_exact);
	UT_RUN(test_queue_passive_pinned_s_release_serializes_bytes_and_ownership);
	UT_RUN(test_current_image_shape_accepts_monotone_xcur_after_x_to_s_yield);
	UT_RUN(test_conditional_lock_preserves_native_off_and_enforces_tracked_x);
	UT_RUN(test_resource_x_ordinary_mutation_gate_dominates_dirty_hint_and_flush);
	UT_RUN(test_preexisting_content_holder_can_finish_before_pre_retention_drain);
	UT_RUN(test_resource_x_t2_t3_buffer_owner_is_generation_exact_and_ordered);
	UT_RUN(test_queue_installed_image_publication_is_exact_and_content_locked);
	UT_RUN(test_queue_self_source_handoff_is_single_lifecycle_and_readonly_drain);
	UT_RUN(test_queue_passive_n_mirror_is_never_gcs_ship_authority);
	UT_RUN(test_gcs_ship_copy_reports_exact_nonblocking_refusal_stage);
	UT_RUN(test_pcm_x_retain_flush_error_injection_is_exact_and_pre_write);
	UT_RUN(test_resource_x_preuse_drift_reprobes_only_current_valid_tuple);
	UT_RUN(test_resource_x_target_writer_context_is_post_t3_and_local_cleanup_only);
	UT_RUN(test_real_gcs_wal_recheck_yields_without_exporting_an_image);
	UT_RUN(test_real_gcs_wal_recheck_preserves_invalid_image_and_io_refusals);
	UT_RUN(test_real_eviction_pending_excludes_clock_sweep_and_keeps_one_owner);
	UT_RUN(test_real_n_predecessor_does_not_fence_a_lost_physical_observation);
	UT_RUN(test_real_n_predecessor_classifies_one_physical_image_without_mutation);
	UT_RUN(test_real_n_predecessor_clears_output_for_every_changed_snapshot_byte);
	UT_RUN(test_real_n_predecessor_consumer_keeps_stable_image_contradictions_closed);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
