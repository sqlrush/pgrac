/* Exact failed-first-fill retirement uses the native reservation owner, the
 * whole invalidate handler and real retained transport-continuation bodies.
 * Author: SqlRush <sqlrush@gmail.com>
 */
int first_fill_retire_own_fixture_main(void);
#define main first_fill_retire_own_fixture_main
#include "test_cluster_pcm_own.c"
#undef main
#ifndef FIRST_FILL_RETIRE_MAIN
#define FIRST_FILL_RETIRE_MAIN main
#endif
#include "cluster/cluster_gcs_reqid.h"
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_block_io_trace.h"
#include "cluster/cluster_membership.h"
#include "storage/condition_variable.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/wait_event.h"

#define MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND CLUSTER_GCS_BLOCK_MAX_OUTSTANDING_PER_BACKEND
#include "test_cluster_first_fill_retire_types.inc"
static void gcs_block_clear_slot_locked(ClusterGcsBlockOutstandingSlot *slot);
static ClusterGcsBlockBackendBlock retire_blocks[1];
static ClusterGcsBlockBackendBlock *gcs_block_backend_blocks = retire_blocks;
static struct {
	pg_atomic_uint64 invalidate_busy_sent_count;
	pg_atomic_uint64 done_sent_count, done_enqueue_drop_count;
	pg_atomic_uint64 stale_reply_drop_count, starvation_denied_pending_x_count;
} retire_shared;
static int retire_backend = 1, retire_pid = 71, retire_route = 2;
static TimestampTz retire_start = 700, retire_now = 10;
static uint64 retire_epoch = 1, retire_peer_session = 17;
static unsigned retire_sends, retire_admitted, retire_tag_reads, retire_wakes, retire_sleeps;
static unsigned retire_receipts, retire_trace_published, retire_trace_consumed;
static ClusterICSendResult retire_result;
static bool retire_confirm_admit;
static unsigned retire_confirm_sends, retire_confirm_handed, retire_worker_wakes;
static GcsBlockDonePayload retire_confirm;
static ClusterMembershipState retire_master_state = CLUSTER_MEMBER_MEMBER;
static GcsBlockInvalidateAckPayload retire_ack;
static bool retire_session = true, retire_cancel;
static void (*retire_on_wait)(void);
static ClusterPcmReadSource retire_wait_source;
bool cluster_trace_gcs_capacity;
static unsigned retire_trace_cuts, retire_trace_scans, retire_lock_depth;
#ifdef CLUSTER_CONTENT_TRACE_GCS_SLOTS
static ClusterContentTraceGcsCapacity retire_trace_cut;
void
cluster_content_trace_gcs_capacity(const ClusterContentTraceGcsCapacity *value)
{
	UT_ASSERT_EQ(retire_lock_depth, 0);
	retire_trace_cut = *value; retire_trace_cuts++;
}
void
cluster_content_trace_gcs_scan(int worker, int workers, uint64 begin, uint64 end,
	uint64 cap, int64 sampled_at)
{
	UT_ASSERT_EQ(retire_lock_depth, 0);
	UT_ASSERT_EQ(worker, 0); UT_ASSERT_EQ(workers, 1);
	UT_ASSERT_EQ(end - begin, 8); UT_ASSERT_EQ(cap, 8);
	UT_ASSERT_EQ(sampled_at, retire_now);
	retire_trace_scans++;
}
#endif
#define ClusterGcsBlock (&retire_shared)
#define MaxBackends 1
#define MyBackendId retire_backend
#define MyProcPid retire_pid
#define MyStartTimestamp retire_start
#define cluster_node_id 3
#define cluster_lms_workers 1
#define cluster_epoch_get_current() retire_epoch
#define cluster_lms_shard_for_tag(tag, workers) 0
#define cluster_gcs_lookup_master(tag) retire_route
#define cluster_membership_get_state(node) retire_master_state
#define cluster_lms_wakeup(worker) (++retire_worker_wakes)
#define cluster_ic_suppress_gcs_done_cap false
#define cluster_sf_peer_supports_gcs_done(node) true
#define cluster_injection_should_skip(name) false
#define GetCurrentTimestamp() retire_now
#define LWLockAcquire(lock, mode) (++retire_lock_depth)
#define LWLockRelease(lock) (--retire_lock_depth)
#define LockBufHdr transition_lock_header
#define ConditionVariablePrepareToSleep(cv) ((void)0)
#define ConditionVariableCancelSleep() ((void)0)
#define ConditionVariableSignal(cv) (++retire_wakes)
#define ConditionVariableBroadcast(cv) (++retire_wakes)
#define ResetLatch(latch) ((void)0)
#define cluster_gcs_block_note_send_outcome(family, result) ((void)(result))
#define cluster_block_io_trace_source(source, phase) ((void)0)
#define cluster_pcm_note_invalidate_parked_grant_pending() ((void)0)
#define cluster_sf_peer_supports_gcs_inval_busy(master) true
#define gcs_block_observe_grant_pending_invalidate(inv, woke) ((void)0)
#define gcs_block_wake_local_pending_s_request(inv) false
#define cluster_bufmgr_pending_s_aborted(tag) false
#define cluster_bufmgr_block_is_pi(tag) false
#define cluster_pcm_lock_apply_gcs_transition(tag, trans, node) false
#define cluster_bufmgr_invalidate_block_for_gcs(tag, mode, lsn, scn) CLUSTER_BUFMGR_GCS_DROP_PINNED
#define elog(...) ((void)0)
#define ereport(...) pg_re_throw()
static void
retire_check(void)
{
	if (retire_cancel) pg_re_throw();
}
#define cluster_pcm_lock_check_shutdown_wait retire_check
static int
retire_wait(void)
{
	retire_sleeps++;
	retire_now += 10000;
	if (retire_on_wait != NULL) retire_on_wait();
	return WL_TIMEOUT;
}
#define WaitLatch(...) retire_wait()
#define ConditionVariableTimedSleep(...) retire_wait()
static bool
gcs_block_pcm_x_authenticated_session(int master, uint64 epoch, uint64 *session)
{
	*session = retire_peer_session;
	return retire_session && master == 2 && epoch == retire_epoch;
}
static ClusterICSendResult
retire_send(uint8 type, int dest, const void *payload, uint32 len)
{
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK);
	UT_ASSERT_EQ(dest, 2);
	UT_ASSERT_EQ(len, sizeof(retire_ack));
	retire_ack = *(const GcsBlockInvalidateAckPayload *)payload;
	retire_sends++;
	if (retire_result == CLUSTER_IC_SEND_DONE || retire_result == CLUSTER_IC_SEND_WOULD_BLOCK)
		retire_admitted++;
	return retire_result;
}
#define gcs_block_send_envelope_or_loopback retire_send
#define cluster_ic_send_envelope retire_send
static bool
retire_enqueue(uint8 type, int dest, const void *payload, uint32 len)
{
	if (type != PGRAC_IC_MSG_GCS_BLOCK_DONE)
		return retire_send(type, dest, payload, len) == CLUSTER_IC_SEND_DONE;
	UT_ASSERT_EQ(dest, 2);
	UT_ASSERT_EQ(len, sizeof(retire_confirm));
	retire_confirm = *(const GcsBlockDonePayload *)payload;
	if (retire_confirm.reserved_0[0] != GCS_BLOCK_DONE_PENDING_S_CONFIRM)
		return true; /* Existing cancellation NACK boundary. */
	retire_confirm_sends++;
	if (retire_confirm_admit) retire_confirm_handed++;
	return retire_confirm_admit;
}
#define cluster_grd_outbound_enqueue_backend_msg retire_enqueue
static PcmLockMode
retire_tag_state(BufferTag tag pg_attribute_unused())
{
	retire_tag_reads++;
	return PCM_LOCK_MODE_N;
}
#define cluster_bufmgr_block_pcm_state retire_tag_state
#define cluster_bufmgr_block_grant_pending(tag) true
static void
retire_trace(const ClusterPcmReadSource *source pg_attribute_unused(), const char *event,
	int backend, int buf, int pid, uint64 incarnation, unsigned count)
{
	UT_ASSERT_EQ(backend, 1);
	UT_ASSERT_EQ(buf, 0);
	UT_ASSERT_EQ(pid, 71);
	UT_ASSERT_EQ(incarnation, 700);
	retire_receipts = count;
	if (strcmp(event, "QUIET_PUBLISHED") == 0) retire_trace_published++;
	else if (strcmp(event, "MASTER_ACK_CONSUMED") == 0) retire_trace_consumed++;
	else UT_ASSERT(false);
}
#define cluster_block_io_trace_receipt retire_trace
static PGPROC retire_proc;
static PGPROC *retire_backend_proc(int backend pg_attribute_unused())
{
	retire_proc.pid = retire_pid;
	return &retire_proc;
}
#define BackendIdGetProc retire_backend_proc
static const ClusterICEnvelope *retire_deferred_envelope;
static void retire_defer(const ClusterICEnvelope *env) { retire_deferred_envelope = env; }
#define cluster_ic_dispatch_defer retire_defer
#include "test_cluster_first_fill_retire.inc"
#undef cluster_ic_dispatch_defer

static void
retire_reset(BufferDesc *buf, ClusterPcmOwnEntry *entry, bool fresh)
{
	n_predecessor_fixture(buf, entry, BUF_TYPE_CURRENT);
	if (fresh) {
		pg_atomic_write_u64(&entry->generation, 0);
		pg_atomic_write_u64(&entry->reservation_token, 0);
	}
	pg_atomic_write_u32(&buf->state, BM_TAG_VALID | BM_IO_IN_PROGRESS | BUF_REFCOUNT_ONE);
	memset(retire_blocks, 0, sizeof(retire_blocks));
	memset(gcs_read_source_attempts, 0, sizeof(gcs_read_source_attempts));
	gcs_read_source_exit_registered = true;
	retire_blocks[0].next_request_id = 1;
	retire_backend = 1; retire_pid = 71; retire_start = 700; retire_route = 2;
	retire_now = 10; retire_epoch = 1; retire_peer_session = 17;
	memset(&retire_ack, 0, sizeof(retire_ack));
	retire_sends = retire_admitted = retire_tag_reads = retire_wakes = retire_sleeps = 0;
	retire_receipts = retire_trace_published = retire_trace_consumed = 0;
	retire_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	retire_session = true; retire_cancel = false; retire_on_wait = NULL;
	retire_confirm_admit = false;
	retire_confirm_sends = retire_confirm_handed = retire_worker_wakes = 0;
	cluster_trace_gcs_capacity = cluster_trace_tx_identity = false;
	retire_trace_cuts = retire_trace_scans = retire_lock_depth = 0;
	retire_master_state = CLUSTER_MEMBER_MEMBER;
	memset(&retire_confirm, 0, sizeof(retire_confirm));
	memset(&retire_shared, 0, sizeof(retire_shared));
	transition_buf = buf;
}
static ClusterPcmReadSource
retire_arm(BufferDesc *buf, bool admitted)
{
	ClusterPcmReadSource source = {0};
	ClusterGcsBlockOutstandingSlot *slot;
	source.tag = buf->tag;
	source.base_generation = cluster_pcm_own_gen_get(0);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, source.base_generation,
		PCM_OWN_FLAG_GRANT_PENDING, &source.reservation_token), CLUSTER_PCM_OWN_OK);
	slot = gcs_block_reserve_slot(buf->tag, PCM_TRANS_N_TO_S, 2, &source.request_id, true);
	source.request_epoch = slot->request_epoch = 1;
	source.master_node = 2;
	source.pending_s = true;
	source.kind = CLUSTER_PCM_SOURCE_FAIL;
	UT_ASSERT(gcs_read_source_arm(buf, &source));
	slot->first_fill.request_admitted = admitted;
	return source;
}
static void
retire_abort(BufferDesc *buf, ClusterPcmReadSource *source)
{
	UT_ASSERT(cluster_gcs_block_read_source_quiesce(buf, source));
	source->quiescent = true;
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, source->base_generation,
		source->reservation_token, PCM_OWN_FLAG_GRANT_PENDING), CLUSTER_PCM_OWN_OK);
	cluster_gcs_block_read_source_complete(buf, source, false);
}
static GcsBlockInvalidatePayload
retire_invalidate(const ClusterPcmReadSource *source)
{
	GcsBlockInvalidatePayload inv = {0};
	ClusterPcmReadGrantKey key = gcs_read_source_key(source);
	inv.request_id = source->request_id;
	inv.epoch = 1; inv.master_node = 2; inv.tag = source->tag;
	inv.invalidating_for_x_node = 3;
	GcsBlockInvalidatePayloadSetPendingS(&inv, &key);
	return inv;
}
static GcsBlockDonePayload
retire_notice(const ClusterPcmReadSource *source)
{
	GcsBlockDonePayload done = {0};
	done.request_id = source->request_id; done.epoch = source->request_epoch;
	done.sender_node = 2; done.requester_backend_id = 1; done.tag = source->tag;
	done.transition_id = PCM_TRANS_N_TO_S;
	done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_RETIRED; done.reserved_0[1] = 3;
	return done;
}
static bool
retire_ack_handed(const ClusterPcmReadSource *source)
{
	for (int i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++) {
		const ClusterGcsBlockOutstandingSlot *slot = &retire_blocks[0].slots[i];
		if (slot->in_use && slot->request_id == source->request_id
			&& slot->request_epoch == source->request_epoch
			&& BufferTagsEqual(&slot->tag, &source->tag))
			return slot->first_fill.ack_handed;
	}
	return false;
}
UT_TEST(test_old_quiet_a_never_reads_or_revokes_successor_b)
{
	BufferDesc buf; ClusterPcmOwnEntry entry;
	ClusterPcmReadSource a, b;
	GcsBlockInvalidatePayload inv;
	retire_reset(&buf, &entry, true);
	a = retire_arm(&buf, true); retire_abort(&buf, &a);
	UT_ASSERT_EQ(a.base_generation, 0);
	b = retire_arm(&buf, true);
	inv = retire_invalidate(&a);
	UT_ASSERT(gcs_block_invalidate_execute(&inv));
	UT_ASSERT_EQ(retire_tag_reads, 0);
	UT_ASSERT_EQ(retire_ack.ack_status, 2);
	UT_ASSERT_EQ(retire_ack.request_id, a.request_id);
	UT_ASSERT(!retire_blocks[0].slots[1].first_fill_revoked);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), b.reservation_token);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT(retire_blocks[0].slots[0].first_fill.quiet);
}
UT_TEST(test_ack_refusal_keeps_frame_and_receipt_until_real_takeover)
{
	for (int which = 0; which < 2; which++) {
		BufferDesc buf; ClusterPcmOwnEntry entry;
		ClusterPcmReadSource a; GcsBlockInvalidatePayload inv;
		GcsBlockInvalidateAckPayload first;
		retire_reset(&buf, &entry, false);
		a = retire_arm(&buf, true); retire_abort(&buf, &a); inv = retire_invalidate(&a);
		UT_ASSERT(gcs_block_invalidate_execute(&inv)); first = retire_ack;
		UT_ASSERT_EQ(retire_admitted, 0);
		UT_ASSERT(!retire_ack_handed(&a));
		retire_result = CLUSTER_IC_SEND_HARD_ERROR;
		retire_now += 10000; gcs_first_fill_receipt_tick(0);
		UT_ASSERT_EQ(memcmp(&first, &retire_ack, sizeof(first)), 0);
		UT_ASSERT_EQ(retire_admitted, 0);
		retire_result = which ? CLUSTER_IC_SEND_WOULD_BLOCK : CLUSTER_IC_SEND_DONE;
		retire_now += 10000; gcs_first_fill_receipt_tick(0);
		UT_ASSERT_EQ(retire_admitted, 1);
		UT_ASSERT(retire_ack_handed(&a));
		UT_ASSERT_EQ(retire_receipts, 1);
		UT_ASSERT(gcs_block_invalidate_execute(&inv));
		retire_now += 10000; gcs_first_fill_receipt_tick(0);
		UT_ASSERT_EQ(retire_sends, 3);
	}
}
static bool
retire_consume(const GcsBlockDonePayload *done)
{
	ClusterICEnvelope env = {0};
	uint64 session;
	env.source_node_id = (uint32)done->sender_node; env.dest_node_id = cluster_node_id;
	env.epoch = retire_epoch; env.payload_length = sizeof(*done);
	if (!gcs_block_pending_s_carrier_current(&env, done->tag, done->sender_node,
		-1, &session))
		return false;
	return gcs_first_fill_receipt_consumed(done, session);
}

UT_TEST(test_master_consumption_retires_count_and_only_the_exact_old_receipt)
{
	BufferDesc buf; ClusterPcmOwnEntry entry;
	ClusterPcmReadSource a, b; GcsBlockInvalidatePayload inv; GcsBlockDonePayload done;
	retire_reset(&buf, &entry, true);
	a = retire_arm(&buf, true); retire_abort(&buf, &a);
	retire_result = CLUSTER_IC_SEND_DONE; inv = retire_invalidate(&a);
	UT_ASSERT(gcs_block_invalidate_execute(&inv));
	UT_ASSERT(retire_ack_handed(&a));
	b = retire_arm(&buf, true); done = retire_notice(&a);
	done.request_id = b.request_id; UT_ASSERT(!retire_consume(&done));
	done = retire_notice(&a); done.epoch++; UT_ASSERT(!retire_consume(&done));
	done = retire_notice(&a); retire_session = false; UT_ASSERT(!retire_consume(&done));
	retire_session = true; retire_peer_session++;
	UT_ASSERT(!retire_consume(&done));
	retire_peer_session--;
	UT_ASSERT_EQ(retire_receipts, 1);
	UT_ASSERT(retire_consume(&done));
	UT_ASSERT_EQ(retire_receipts, 0);
	UT_ASSERT_EQ(retire_trace_published, 1); UT_ASSERT_EQ(retire_trace_consumed, 1);
	UT_ASSERT(!retire_blocks[0].slots[0].in_use);
	UT_ASSERT(retire_blocks[0].slots[1].in_use);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), b.reservation_token);
	UT_ASSERT(!retire_consume(&done));
}
UT_TEST(test_native_retirement_must_be_proven_before_quiet_publication)
{
	for (int bad = 0; bad < 6; bad++) {
		BufferDesc buf; ClusterPcmOwnEntry entry;
		ClusterPcmReadSource a; GcsBlockInvalidatePayload inv;
		retire_reset(&buf, &entry, false); a = retire_arm(&buf, true);
		UT_ASSERT(cluster_gcs_block_read_source_quiesce(&buf, &a)); a.quiescent = true;
		if (bad != 0) UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, a.base_generation,
			a.reservation_token, PCM_OWN_FLAG_GRANT_PENDING), CLUSTER_PCM_OWN_OK);
		if (bad == 1) pg_atomic_fetch_or_u32(&buf.state, BM_DIRTY);
		if (bad == 2) a.reservation_token++;
		if (bad == 3) retire_start++;
		if (bad == 4) pg_atomic_fetch_and_u32(&buf.state, ~BM_IO_IN_PROGRESS);
		if (bad == 5) { buf.pcm_state = PCM_STATE_S; pg_atomic_fetch_or_u32(&buf.state, BM_VALID); }
		cluster_gcs_block_read_source_complete(&buf, &a, false);
		UT_ASSERT(!retire_blocks[0].slots[0].first_fill.quiet);
		UT_ASSERT(a.pending_s); inv = retire_invalidate(&a);
		UT_ASSERT(gcs_block_invalidate_execute(&inv));
		UT_ASSERT_EQ(retire_ack.ack_status, GCS_BLOCK_INVALIDATE_ACK_STATUS_RETRYABLE_BUSY);
		UT_ASSERT_EQ(retire_trace_published, 0);
	}
}
UT_TEST(test_quiet_receipt_age_cannot_handoff_ack_or_retire)
{
	BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
	GcsBlockInvalidatePayload inv;
	retire_reset(&buf, &entry, true); a = retire_arm(&buf, true); retire_abort(&buf, &a);
	for (int tick = 0; tick < 3; tick++) {
		retire_now += 100000000; gcs_first_fill_receipt_tick(0);
		UT_ASSERT(retire_blocks[0].slots[0].in_use);
		UT_ASSERT_EQ(retire_receipts, 1); UT_ASSERT_EQ(retire_admitted, 0);
	}
	inv = retire_invalidate(&a); retire_result = CLUSTER_IC_SEND_DONE;
	UT_ASSERT(gcs_block_invalidate_execute(&inv));
	UT_ASSERT_EQ(retire_admitted, 1); UT_ASSERT(retire_ack_handed(&a));
	UT_ASSERT(retire_blocks[0].slots[0].first_fill.quiet);
}
UT_TEST(test_caller_cancel_cannot_discard_its_shared_quiet_receipt)
{
	BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
	GcsBlockInvalidatePayload inv; GcsBlockDonePayload done;
	retire_reset(&buf, &entry, false); a = retire_arm(&buf, true); retire_abort(&buf, &a);
	retire_cancel = true;
	gcs_block_release_slot(&retire_blocks[0].slots[0]);
	UT_ASSERT(retire_blocks[0].slots[0].first_fill.quiet);
	retire_result = CLUSTER_IC_SEND_DONE; inv = retire_invalidate(&a);
	UT_ASSERT(gcs_block_invalidate_execute(&inv));
	UT_ASSERT(retire_ack_handed(&a));
	done = retire_notice(&a); UT_ASSERT(retire_consume(&done));
	UT_ASSERT(!retire_blocks[0].slots[0].in_use);
	UT_ASSERT_EQ(retire_receipts, 0);
}
static void
retire_free_after_two(void)
{
	if (retire_sleeps == 2) {
		GcsBlockDonePayload done = retire_notice(&retire_wait_source);
		UT_ASSERT(retire_consume(&done));
	}
}
UT_TEST(test_full_original_container_waits_without_eviction_or_transaction_error)
{
	BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
	uint64 id; ClusterGcsBlockOutstandingSlot *next;
	retire_reset(&buf, &entry, true);
	for (int i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++) {
		GcsBlockInvalidatePayload inv;
		a = retire_arm(&buf, true); retire_abort(&buf, &a);
		inv = retire_invalidate(&a); retire_result = CLUSTER_IC_SEND_DONE;
		UT_ASSERT(gcs_block_invalidate_execute(&inv));
		UT_ASSERT(retire_ack_handed(&a));
	}
	UT_ASSERT_EQ(retire_receipts, MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND);
	retire_wait_source = a; retire_on_wait = retire_free_after_two;
	next = gcs_block_reserve_slot(buf.tag, PCM_TRANS_N_TO_S, 2, &id, true);
	UT_ASSERT_EQ(retire_sleeps, 2);
	UT_ASSERT_EQ(retire_receipts, MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND - 1);
	UT_ASSERT(next == &retire_blocks[0].slots[MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND - 1]);
	UT_ASSERT(id != a.request_id);
	UT_ASSERT(retire_blocks[0].slots[0].first_fill.quiet);
}
UT_TEST(test_never_admitted_cancel_needs_no_remote_receipt)
{
	BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
	retire_reset(&buf, &entry, false); a = retire_arm(&buf, false); retire_abort(&buf, &a);
	UT_ASSERT(!a.pending_s); UT_ASSERT(!retire_blocks[0].slots[0].in_use);
	UT_ASSERT_EQ(retire_trace_published, 0); UT_ASSERT_EQ(retire_sends, 0);
}

UT_TEST(test_capacity_observer_copies_original_full_source_keys_without_holding_lock)
{
#ifdef CLUSTER_CONTENT_TRACE_GCS_SLOTS
	BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
	uint64 id;
	retire_reset(&buf, &entry, true);
	for (int i = 0; i < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; i++) {
		GcsBlockInvalidatePayload inv;
		a = retire_arm(&buf, true); retire_abort(&buf, &a);
		inv = retire_invalidate(&a); retire_result = CLUSTER_IC_SEND_DONE;
		UT_ASSERT(gcs_block_invalidate_execute(&inv));
		UT_ASSERT(retire_ack_handed(&a));
	}
	cluster_trace_gcs_capacity = cluster_trace_tx_identity = true;
	retire_wait_source = a; retire_on_wait = retire_free_after_two;
	(void)gcs_block_reserve_slot(buf.tag, PCM_TRANS_N_TO_S, 2, &id, true);
	UT_ASSERT_EQ(retire_trace_cuts, 2); UT_ASSERT_EQ(retire_sleeps, 2);
	UT_ASSERT_EQ(retire_trace_cut.nslots, 8); UT_ASSERT_EQ(retire_trace_cut.max_backends, 1);
	for (unsigned i = 0; i < retire_trace_cut.nslots; i++) {
		const ClusterContentTraceGcsSlot *s = &retire_trace_cut.slots[i];
		UT_ASSERT(s->in_use && s->armed && s->quiet && s->ack_handed);
		UT_ASSERT_EQ(s->owner_pid, 71); UT_ASSERT_EQ(s->backend_incarnation, 700);
		UT_ASSERT_EQ(s->request_id, s->source.request_id);
		UT_ASSERT_EQ(s->request_epoch, s->source.request_epoch);
		UT_ASSERT_EQ(s->expected_master_node, s->source.master_node);
		UT_ASSERT(BufferTagsEqual(&s->tag, &s->source.tag));
	}
	/* Original native ACK freed the last slot. The exported cut is owned. */
	UT_ASSERT(retire_trace_cut.slots[7].quiet);
	UT_ASSERT(!retire_blocks[0].slots[7].first_fill.quiet);
	gcs_first_fill_receipt_tick(0);
	UT_ASSERT_EQ(retire_trace_scans, 1);
	cluster_trace_gcs_capacity = false;
	gcs_first_fill_receipt_tick(0);
	UT_ASSERT_EQ(retire_trace_scans, 1);
#else
	UT_ASSERT(false); /* Parent has no observation at the native capacity site. */
#endif
}
UT_TEST(test_exit_preserves_quiet_debt_and_reused_backend_cannot_take_it)
{
	BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
	GcsBlockInvalidatePayload inv; GcsBlockDonePayload done;
	retire_reset(&buf, &entry, true); a = retire_arm(&buf, true); retire_abort(&buf, &a);
	retire_start++;
	gcs_block_release_slot(&retire_blocks[0].slots[0]);
	UT_ASSERT_EQ(retire_blocks[0].slots[0].first_fill.backend_incarnation, 700);
	retire_start--;
	gcs_block_release_slot(&retire_blocks[0].slots[0]);
	UT_ASSERT(retire_blocks[0].slots[0].first_fill.quiet);
	retire_result = CLUSTER_IC_SEND_DONE; inv = retire_invalidate(&a);
	UT_ASSERT(gcs_block_invalidate_execute(&inv)); done = retire_notice(&a);
	UT_ASSERT(retire_consume(&done));
	UT_ASSERT_EQ(retire_receipts, 0); UT_ASSERT(!retire_blocks[0].slots[0].in_use);
}
UT_TEST(test_master_consumption_releases_capacity_without_a_backend_waiter)
{
	BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
	GcsBlockInvalidatePayload inv; GcsBlockDonePayload done;
	retire_reset(&buf, &entry, true); a = retire_arm(&buf, true); retire_abort(&buf, &a);
	retire_result = CLUSTER_IC_SEND_WOULD_BLOCK; inv = retire_invalidate(&a);
	UT_ASSERT(gcs_block_invalidate_execute(&inv)); done = retire_notice(&a);
	UT_ASSERT(retire_consume(&done));
	UT_ASSERT_EQ(retire_receipts, 0); UT_ASSERT(!retire_blocks[0].slots[0].in_use);
	UT_ASSERT_EQ(retire_sleeps, 0); UT_ASSERT(retire_wakes > 0);
}
UT_TEST(test_unknown_invalidate_still_observes_native_io_protection)
{
	BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
	GcsBlockInvalidatePayload inv;
	retire_reset(&buf, &entry, false); a = retire_arm(&buf, true);
	inv = retire_invalidate(&a); inv.request_id++;
	UT_ASSERT(gcs_block_invalidate_execute(&inv));
	UT_ASSERT_EQ(retire_ack.ack_status, GCS_BLOCK_INVALIDATE_ACK_STATUS_RETRYABLE_BUSY);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT(!retire_blocks[0].slots[0].first_fill.quiet);
	UT_ASSERT(!retire_blocks[0].slots[0].first_fill_revoked);
}

UT_TEST(test_admitted_original_master_notice_survives_route_change)
{
	BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
	GcsBlockInvalidatePayload inv; GcsBlockDonePayload done;
	retire_reset(&buf, &entry, true); a = retire_arm(&buf, true); retire_abort(&buf, &a);
	retire_result = CLUSTER_IC_SEND_DONE; inv = retire_invalidate(&a);
	UT_ASSERT(gcs_block_invalidate_execute(&inv));
	UT_ASSERT(retire_ack_handed(&a));
	retire_route = 1; done = retire_notice(&a);
	UT_ASSERT(retire_consume(&done));
	UT_ASSERT_EQ(retire_receipts, 0);
	UT_ASSERT(!retire_blocks[0].slots[0].in_use);
}

UT_TEST(test_deferred_original_notice_keeps_receipt_then_retires_without_successor_state)
{
	BufferDesc buf; ClusterPcmOwnEntry entry;
	ClusterPcmReadSource a, b;
	GcsBlockInvalidatePayload inv;
	GcsBlockDonePayload done;
	ClusterICEnvelope env = {0};
	uint64 session;
	retire_reset(&buf, &entry, true); a = retire_arm(&buf, true); retire_abort(&buf, &a);
	retire_result = CLUSTER_IC_SEND_DONE; inv = retire_invalidate(&a);
	UT_ASSERT(gcs_block_invalidate_execute(&inv));
	b = retire_arm(&buf, true); done = retire_notice(&a);
	env.source_node_id = (uint32)done.sender_node; env.dest_node_id = cluster_node_id;
	env.epoch = retire_epoch; env.payload_length = sizeof(done);
	for (int unknown = 0; unknown < 2; unknown++) {
		retire_session = unknown != 0; retire_peer_session = unknown ? 0 : 17;
		retire_deferred_envelope = NULL;
		UT_ASSERT(!gcs_block_pending_s_carrier_current(&env, done.tag, done.sender_node, -1, &session));
		UT_ASSERT(retire_deferred_envelope == &env);
		UT_ASSERT_EQ(retire_receipts, 1);
		UT_ASSERT(retire_blocks[0].slots[0].in_use && retire_blocks[0].slots[1].in_use);
		UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), b.reservation_token);
	}
	retire_session = true; retire_peer_session = 17;
	retire_route = 1; retire_epoch++; env.epoch = retire_epoch;
	UT_ASSERT(gcs_block_pending_s_carrier_current(&env, done.tag, done.sender_node, -1, &session));
	UT_ASSERT(!gcs_first_fill_receipt_consumed(&done, 0));
	UT_ASSERT(!gcs_first_fill_receipt_consumed(&done, session + 1));
	UT_ASSERT_EQ(retire_receipts, 1);
	UT_ASSERT(gcs_first_fill_receipt_consumed(&done, session));
	UT_ASSERT_EQ(retire_receipts, 0);
	UT_ASSERT(!retire_blocks[0].slots[0].in_use && retire_blocks[0].slots[1].in_use);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), b.reservation_token);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(retire_tag_reads, 0);
}

/* The installer boundary supplies a completed native S tuple. Actual
 * source completion must verify it before publishing a confirmation owner. */
static void
retire_prepare_install(BufferDesc *buf, ClusterPcmOwnEntry *entry, ClusterPcmReadSource *source)
{
	UT_ASSERT(cluster_gcs_block_read_source_quiesce(buf, source));
	source->quiescent = source->durable = true;
	source->kind = CLUSTER_PCM_SOURCE_IMAGE_INSTALLED;
	pg_atomic_write_u64(&entry->generation, source->base_generation + 1);
	pg_atomic_write_u32(&entry->flags, 0);
	buf->pcm_state = PCM_STATE_S;
	pg_atomic_write_u32(&buf->state, BM_TAG_VALID | BM_VALID | BUF_REFCOUNT_ONE);
}

UT_TEST(test_installed_confirm_refusal_keeps_original_owner_until_real_handoff)
{
	BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
	GcsBlockDonePayload first;
	unsigned wakes;
	retire_reset(&buf, &entry, true); a = retire_arm(&buf, true);
	retire_prepare_install(&buf, &entry, &a);
	cluster_gcs_block_read_source_complete(&buf, &a, true);
	UT_ASSERT(retire_blocks[0].slots[0].in_use);
	if (!retire_blocks[0].slots[0].in_use) return;
	UT_ASSERT(!a.pending_s && !retire_blocks[0].slots[0].first_fill.quiet);
	UT_ASSERT_EQ(retire_confirm_sends, 0); /* Backend only publishes; LMS owns offers. */
	UT_ASSERT(retire_worker_wakes > 0);
	gcs_first_fill_receipt_tick(0); first = retire_confirm;
	UT_ASSERT_EQ(retire_confirm_sends, 1); UT_ASSERT_EQ(retire_confirm_handed, 0);
	UT_ASSERT_EQ(first.reserved_0[0], GCS_BLOCK_DONE_PENDING_S_CONFIRM);
	UT_ASSERT_EQ(first.sender_node, 3); UT_ASSERT_EQ(first.reserved_0[1], 3);
	UT_ASSERT_EQ(first.requester_backend_id, 1); UT_ASSERT_EQ(first.request_id, a.request_id);
	UT_ASSERT_EQ(first.epoch, 1); UT_ASSERT(BufferTagsEqual(&first.tag, &a.tag));
	for (unsigned j = 2; j < sizeof(first.reserved_0); j++) UT_ASSERT_EQ(first.reserved_0[j], 0);
	for (int offer = 0; offer < 3; offer++) {
		retire_now += 10000; gcs_first_fill_receipt_tick(0);
		UT_ASSERT(retire_blocks[0].slots[0].in_use);
		UT_ASSERT_EQ(memcmp(&first, &retire_confirm, sizeof(first)), 0);
	}
	retire_confirm_admit = true; wakes = retire_wakes;
	retire_now += 10000; gcs_first_fill_receipt_tick(0);
	UT_ASSERT(!retire_blocks[0].slots[0].in_use); UT_ASSERT(retire_wakes > wakes);
	UT_ASSERT_EQ(retire_confirm_handed, 1);
	retire_now += 10000; gcs_first_fill_receipt_tick(0);
	UT_ASSERT_EQ(retire_confirm_sends, 5); UT_ASSERT_EQ(retire_confirm_handed, 1);
}

UT_TEST(test_installed_confirm_survives_exit_and_never_reads_successor_buffer)
{
	BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
	retire_reset(&buf, &entry, true); a = retire_arm(&buf, true);
	retire_prepare_install(&buf, &entry, &a);
	cluster_gcs_block_read_source_complete(&buf, &a, true);
	UT_ASSERT(retire_blocks[0].slots[0].in_use);
	if (!retire_blocks[0].slots[0].in_use) return;
	for (int n = 0; n < GCS_READ_SOURCE_LEDGER_MAX; n++)
		UT_ASSERT(!gcs_read_source_attempts[n].used);
	retire_pid++; retire_start++;
	gcs_block_release_slot(&retire_blocks[0].slots[0]);
	UT_ASSERT(retire_blocks[0].slots[0].in_use);
	buf.tag.relNumber++; buf.pcm_state = PCM_STATE_N;
	pg_atomic_write_u64(&entry.generation, 97);
	pg_atomic_write_u32(&entry.flags, PCM_OWN_FLAG_GRANT_PENDING);
	pg_atomic_write_u32(&buf.state, BM_TAG_VALID | BM_IO_IN_PROGRESS | BUF_REFCOUNT_ONE);
	retire_confirm_admit = true; gcs_first_fill_receipt_tick(0);
	UT_ASSERT(!retire_blocks[0].slots[0].in_use);
	UT_ASSERT_EQ(retire_confirm.request_id, a.request_id);
	UT_ASSERT(BufferTagsEqual(&retire_confirm.tag, &a.tag));
	UT_ASSERT_EQ(retire_tag_reads, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 97);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_N);
}

UT_TEST(test_installed_confirm_waits_for_unknown_session_but_retires_on_proved_reconfiguration)
{
	for (int why = 0; why < 6; why++) {
		BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
		retire_reset(&buf, &entry, true); a = retire_arm(&buf, true);
		retire_prepare_install(&buf, &entry, &a);
		cluster_gcs_block_read_source_complete(&buf, &a, true);
		UT_ASSERT(retire_blocks[0].slots[0].in_use);
		if (!retire_blocks[0].slots[0].in_use) continue;
		retire_session = false; retire_confirm_admit = true; retire_now += 300000000;
		gcs_first_fill_receipt_tick(0);
		UT_ASSERT(retire_blocks[0].slots[0].in_use); UT_ASSERT_EQ(retire_confirm_sends, 0);
		retire_session = true;
		if (why == 1) retire_epoch++;
		if (why == 2) retire_route = 1;
		if (why == 3) retire_peer_session++;
		if (why == 4) retire_master_state = CLUSTER_MEMBER_DEAD;
		if (why == 5) retire_master_state = CLUSTER_MEMBER_REMOVED;
		retire_now += 10000; gcs_first_fill_receipt_tick(0);
		UT_ASSERT(!retire_blocks[0].slots[0].in_use);
		UT_ASSERT_EQ(retire_confirm_sends, why == 0 ? 1 : 0);
		UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_S);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	}
}

UT_TEST(test_installed_confirm_requires_exact_native_publication_proof)
{
	for (int bad = 0; bad < 6; bad++) {
		BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
		retire_reset(&buf, &entry, true); a = retire_arm(&buf, true);
		retire_prepare_install(&buf, &entry, &a);
		if (bad == 0) a.reservation_token++;
		if (bad == 1) a.base_generation++;
		if (bad == 2) a.tag.relNumber++;
		if (bad == 3) pg_atomic_write_u32(&entry.flags, PCM_OWN_FLAG_GRANT_PENDING);
		if (bad == 4) pg_atomic_fetch_or_u32(&buf.state, BM_IO_ERROR);
		if (bad == 5) a.durable = false;
		cluster_gcs_block_read_source_complete(&buf, &a, true);
		retire_confirm_admit = true; gcs_first_fill_receipt_tick(0);
		UT_ASSERT(a.pending_s); UT_ASSERT(retire_blocks[0].slots[0].in_use);
		UT_ASSERT_EQ(retire_confirm_sends, 0); UT_ASSERT_EQ(retire_worker_wakes, 0);
	}
}

UT_TEST(test_published_install_revoke_uses_resident_cleanup_and_closed_delivery)
{
	BufferDesc buf; ClusterPcmOwnEntry entry; ClusterPcmReadSource a;
	GcsBlockInvalidatePayload inv;
	retire_reset(&buf, &entry, true); a = retire_arm(&buf, true);
	retire_prepare_install(&buf, &entry, &a);
	cluster_gcs_block_read_source_complete(&buf, &a, true);
	UT_ASSERT(retire_blocks[0].slots[0].in_use);
	if (!retire_blocks[0].slots[0].in_use) return;
	inv = retire_invalidate(&a);
	UT_ASSERT_EQ(gcs_first_fill_receipt_revoke(&inv), 0);
	UT_ASSERT(gcs_block_pending_s_delivery_closed(&inv));
	UT_ASSERT(!retire_blocks[0].slots[0].first_fill.quiet);
	UT_ASSERT(!retire_blocks[0].slots[0].first_fill_revoked);
	UT_ASSERT_EQ(retire_sends, 0); UT_ASSERT_EQ(retire_tag_reads, 0);
}

UT_TEST(test_repeated_installed_confirmations_return_original_bounded_slots)
{
	BufferDesc buf; ClusterPcmOwnEntry entry;
	retire_reset(&buf, &entry, true); retire_confirm_admit = true;
	for (int episode = 0; episode < 2 * MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND + 1; episode++) {
		ClusterPcmReadSource a;
		buf.pcm_state = PCM_STATE_N;
		pg_atomic_write_u32(&buf.state, BM_TAG_VALID | BM_IO_IN_PROGRESS | BUF_REFCOUNT_ONE);
		a = retire_arm(&buf, true); retire_prepare_install(&buf, &entry, &a);
		cluster_gcs_block_read_source_complete(&buf, &a, true);
		UT_ASSERT(retire_blocks[0].slots[0].in_use);
		retire_now += 10000; gcs_first_fill_receipt_tick(0);
		for (int slot = 0; slot < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; slot++)
			UT_ASSERT(!retire_blocks[0].slots[slot].in_use);
		UT_ASSERT_EQ(retire_confirm_handed, episode + 1);
	}
}

int
FIRST_FILL_RETIRE_MAIN(void)
{
	UT_PLAN(20);
	UT_RUN(test_deferred_original_notice_keeps_receipt_then_retires_without_successor_state);
	UT_RUN(test_capacity_observer_copies_original_full_source_keys_without_holding_lock);
	UT_RUN(test_installed_confirm_refusal_keeps_original_owner_until_real_handoff);
	UT_RUN(test_installed_confirm_survives_exit_and_never_reads_successor_buffer);
	UT_RUN(test_installed_confirm_waits_for_unknown_session_but_retires_on_proved_reconfiguration);
	UT_RUN(test_installed_confirm_requires_exact_native_publication_proof);
	UT_RUN(test_published_install_revoke_uses_resident_cleanup_and_closed_delivery);
	UT_RUN(test_repeated_installed_confirmations_return_original_bounded_slots);
	UT_RUN(test_old_quiet_a_never_reads_or_revokes_successor_b);
	UT_RUN(test_ack_refusal_keeps_frame_and_receipt_until_real_takeover);
	UT_RUN(test_master_consumption_retires_count_and_only_the_exact_old_receipt);
	UT_RUN(test_native_retirement_must_be_proven_before_quiet_publication);
	UT_RUN(test_quiet_receipt_age_cannot_handoff_ack_or_retire);
	UT_RUN(test_caller_cancel_cannot_discard_its_shared_quiet_receipt);
	UT_RUN(test_full_original_container_waits_without_eviction_or_transaction_error);
	UT_RUN(test_never_admitted_cancel_needs_no_remote_receipt);
	UT_RUN(test_exit_preserves_quiet_debt_and_reused_backend_cannot_take_it);
	UT_RUN(test_master_consumption_releases_capacity_without_a_backend_waiter);
	UT_RUN(test_unknown_invalidate_still_observes_native_io_protection);
	UT_RUN(test_admitted_original_master_notice_survives_route_change);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
