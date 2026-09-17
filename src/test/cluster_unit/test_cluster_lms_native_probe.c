/* Author: SqlRush <sqlrush@gmail.com> */
/* Execute the production collector; only process, native-lock and wire edges
 * are controlled. Deferred wire replies keep exactly eight real probes live. */
#include "postgres.h"
#include LMS_NATIVE_PROBE_SOURCE_PATH
#include GES_DEDUP_SOURCE_PATH
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

int cluster_node_id = 0;
int cluster_lms_native_lock_probe_max_inflight = 8;
int cluster_lms_native_lock_probe_retry_interval_ms = 500;
int cluster_lms_native_lock_probe_retry_budget = 60;
int cluster_ges_request_timeout_ms = 60000;
int cluster_ges_dedup_max_entries = 256;
int MyProcPid = 100;
bool IsUnderPostmaster = true;
BackendType MyBackendType = B_LMS;
Latch *MyLatch;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
volatile sig_atomic_t InterruptPending;

void
SetLatch(Latch *latch)
{
	(void)latch;
}
void
pg_re_throw(void)
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

static ClusterLmsSharedState state;
static ClusterNodeInfo peer;
static TimestampTz now_us;
static unsigned sends, grants, rejects, releases, wakes, sleeps, cv_cancels, peak_active;
static unsigned sleep_mode;
static bool nested_completion;
static uint64 epoch;
static uint64 wire_ids[256];
static ClusterGrdHolderId holders[128];
static int n_holders;
static ClusterGesDedupShared dedup_state;
static ClusterGesDedupEntry dedup_rows[256];
static bool dedup_used[256];
static LWLock dedup_lock;
static LWLock *held_locks[16];
static unsigned held_count;
static bool stop_poll_in_completion;
static bool stop_new_work_allowed = true;
static unsigned stop_new_work_calls;

void
hash_seq_init(HASH_SEQ_STATUS *scan, HTAB *table)
{
	UT_ASSERT(table == cluster_ges_dedup_htab);
	scan->hashp = table;
	scan->curBucket = 0;
}

void *
hash_seq_search(HASH_SEQ_STATUS *scan)
{
	while (scan->curBucket < lengthof(dedup_rows)) {
		uint32 row = scan->curBucket++;
		if (dedup_used[row])
			return &dedup_rows[row];
	}
	return NULL;
}

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	UT_ASSERT(!modifies_data);
	stop_new_work_calls++;
	return stop_new_work_allowed;
}

/* Only the shmem hash storage is controlled. Registration, exact reply
 * publication and completed-only removal execute the production module. */
void *
hash_search(HTAB *table, const void *key, HASHACTION action, bool *found)
{
	int free_row = -1;

	UT_ASSERT(table == cluster_ges_dedup_htab);
	for (int i = 0; i < lengthof(dedup_rows); i++) {
		if (!dedup_used[i]) {
			if (free_row < 0)
				free_row = i;
			continue;
		}
		if (memcmp(&dedup_rows[i].key, key, sizeof(ClusterGesDedupKey)) == 0) {
			if (found)
				*found = true;
			if (action == HASH_REMOVE)
				dedup_used[i] = false;
			return &dedup_rows[i];
		}
	}
	if (found)
		*found = false;
	if (action != HASH_ENTER_NULL || free_row < 0)
		return NULL;
	dedup_used[free_row] = true;
	memset(&dedup_rows[free_row], 0, sizeof(dedup_rows[free_row]));
	memcpy(&dedup_rows[free_row].key, key, sizeof(ClusterGesDedupKey));
	return &dedup_rows[free_row];
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

TimestampTz
GetCurrentTimestamp(void)
{
	return now_us;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (held_count >= lengthof(held_locks) || LWLockHeldByMe(lock))
		abort();
	UT_ASSERT(mode == LW_SHARED || mode == LW_EXCLUSIVE);
	held_locks[held_count++] = lock;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	if (held_count == 0 || held_locks[held_count - 1] != lock)
		abort();
	held_count--;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	for (unsigned i = 0; i < held_count; i++)
		if (held_locks[i] == lock)
			return true;
	return false;
}
void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	for (unsigned i = 0; i < held_count; i++)
		callback(held_locks[i], LW_EXCLUSIVE, context);
}
void
ConditionVariableBroadcast(ConditionVariable *cv)
{
	(void)cv;
	wakes++;
}
uint64
cluster_epoch_get_current(void)
{
	return epoch;
}
void
ProcessInterrupts(void)
{
	InterruptPending = 0;
	pg_re_throw();
}
void
ConditionVariablePrepareToSleep(ConditionVariable *cv)
{
	(void)cv;
}
bool
ConditionVariableCancelSleep(void)
{
	cv_cancels++;
	return true;
}
bool
ConditionVariableTimedSleep(ConditionVariable *cv, long timeout, uint32 wait_event)
{
	(void)cv;
	UT_ASSERT_EQ(wait_event, WAIT_EVENT_CLUSTER_NATIVE_PROBE_REPLY_WAIT);
	now_us += timeout * 1000;
	sleeps++;
	if (sleep_mode == 1) {
		/* Real receive/completion/driver, not a fabricated result. */
		for (unsigned i = 0; i < sends; i++) {
			cluster_lms_native_probe_recv_reply(wire_ids[i], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
			cluster_lms_native_probe_retry_tick();
		}
	} else if (sleep_mode == 2)
		InterruptPending = 1;
	return true;
}
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node)
{
	return node == 1 ? &peer : NULL;
}
ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 node)
{
	(void)node;
	return CLUSTER_CSSD_PEER_ALIVE;
}
ClusterNativeLockProbeReply
cluster_native_lock_probe_local(const LOCKTAG *tag, LOCKMODE mode,
								const ClusterGrdHolderId *requester)
{
	(void)tag;
	(void)mode;
	(void)requester;
	return CLUSTER_NATIVE_LOCK_PROBE_CLEAR;
}
void
cluster_grd_resid_decode(const ClusterResId *id, LOCKTAG *tag)
{
	(void)id;
	SET_LOCKTAG_RELATION(*tag, 5, 16384);
}
void
cluster_grd_outbound_enqueue_lms_native_probe(uint32 dest, const void *data, uint16 len)
{
	const GesNativeLockProbePayload *p = data;
	UT_ASSERT_EQ(dest, 1);
	UT_ASSERT_EQ(len, sizeof(*p));
	{
		unsigned active = 0;
		bool found = false;
		for (int i = 0; i < 64; i++) {
			ClusterLmsNativeLockProbeSlot *slot = &state.native_probe_slots[i];
			uint64 phase = pg_atomic_read_u64(&slot->in_use);
			if (phase == PROBE_ACTIVE || phase == PROBE_DISPATCHING || phase == PROBE_COMPLETING)
				active++;
			if (slot->probe_id == p->probe_id && phase == PROBE_DISPATCHING) {
				found = true;
				UT_ASSERT_EQ(slot->requester.procno, p->requester_procno);
				UT_ASSERT_EQ(slot->resid.type, LOCKTAG_RELATION);
				UT_ASSERT_EQ(slot->lockmode, ShareLock);
			}
		}
		UT_ASSERT(found);
		UT_ASSERT(active <= 8);
		peak_active = Max(peak_active, active);
	}
	UT_ASSERT(sends < lengthof(wire_ids));
	if (sends >= lengthof(wire_ids))
		abort();
	wire_ids[sends++] = p->probe_id;
}
void
cluster_grd_outbound_enqueue_lmon_reply(uint32 dest, const void *data, uint16 len)
{
	const GesReplyPayload *p = data;
	if (stop_poll_in_completion) {
		int slot;
		const char *reason;

		UT_ASSERT_EQ(cluster_lms_native_probe_normal_stop_poll(&slot, &reason),
					 CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_STR_EQ(reason, "NATIVE_PROBE_OWNED");
	}
	(void)dest;
	UT_ASSERT_EQ(len, sizeof(*p));
	if (p->opcode == GES_REPLY_OPCODE_GRANT)
		grants++;
	else
		rejects++;
	if (nested_completion) {
		nested_completion = false;
		cluster_lms_native_probe_recv_reply(wire_ids[0], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
		cluster_lms_native_probe_retry_tick();
	}
}
ClusterGrdEntryResult
cluster_grd_release_holder_by_id(const ClusterResId *id, const ClusterGrdHolderId *holder)
{
	(void)id;
	(void)holder;
	releases++;
	return CLUSTER_GRD_ENTRY_OK;
}
bool
cluster_grd_holder_mode_by_id(const ClusterResId *id, const ClusterGrdHolderId *holder,
							  LOCKMODE *mode)
{
	(void)id;
	for (int i = 0; i < n_holders; i++)
		if (memcmp(holder, &holders[i], sizeof(*holder)) == 0) {
			*mode = ShareLock;
			return true;
		}
	return false;
}
ClusterGrdConvertResult
cluster_grd_convert_grant_by_backend(const ClusterResId *id, int32 node, uint32 procno,
									 uint64 epoch, LOCKMODE oldmode, LOCKMODE newmode,
									 uint64 request, int32 source, uint64 gen)
{
	(void)id;
	(void)node;
	(void)procno;
	(void)epoch;
	(void)oldmode;
	(void)newmode;
	(void)request;
	(void)source;
	(void)gen;
	return CLUSTER_GRD_CONVERT_GRANTED_INPLACE;
}
static void
reset(void)
{
	UT_ASSERT_EQ(held_count, 0);
	stop_new_work_allowed = true;
	stop_new_work_calls = 0;
	stop_poll_in_completion = false;
	memset(&state, 0, sizeof(state));
	cluster_lms_state = &state;
	pg_atomic_init_u64(&state.native_probe_next_id, 1);
	pg_atomic_init_u64(&state.lms_restart_generation, 7);
	now_us = 1000000;
	sends = grants = rejects = releases = 0;
	n_holders = 0;
	wakes = sleeps = cv_cancels = peak_active = 0;
	sleep_mode = 0;
	nested_completion = false;
	epoch = 9;
	InterruptPending = 0;
	memset(wire_ids, 0, sizeof(wire_ids));
	memset(&dedup_state, 0, sizeof(dedup_state));
	memset(dedup_rows, 0, sizeof(dedup_rows));
	memset(dedup_used, 0, sizeof(dedup_used));
	cluster_ges_dedup_shared = &dedup_state;
	cluster_ges_dedup_htab = (HTAB *)dedup_rows;
	cluster_ges_dedup_lock = &dedup_lock;
}
static bool
submit_exact(int procno, uint64 request_id, uint32 opcode)
{
	ClusterResId id;
	ClusterGrdHolderId holder;
	memset(&id, 0, sizeof(id));
	id.type = LOCKTAG_RELATION;
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 1;
	holder.procno = procno;
	holder.cluster_epoch = 9;
	holder.request_id = request_id;
	holders[n_holders++] = holder;
	return cluster_lms_native_probe_schedule_grant(
		&id, ShareLock, &holder, 1, opcode, (UINT64_C(9) << 32) | 7,
		opcode == GES_REQ_OPCODE_CONVERT ? AccessShareLock : NoLock);
}
static bool
submit_op(int procno, uint32 opcode)
{
	return submit_exact(procno, procno + 1000, opcode);
}
static bool
submit(int procno)
{
	return submit_op(procno, GES_REQ_OPCODE_REQUEST);
}
static unsigned
occupied(void)
{
	unsigned n = 0;
	for (int i = 0; i < 64; i++)
		if (pg_atomic_read_u64(&state.native_probe_slots[i].in_use))
			n++;
	return n;
}
static bool
local_wait(int ms)
{
	ClusterResId id;
	ClusterGrdHolderId holder;
	memset(&id, 0, sizeof(id));
	id.type = LOCKTAG_RELATION;
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 0;
	holder.procno = 99;
	holder.cluster_epoch = 9;
	holder.request_id = 9000;
	return cluster_lms_native_probe_wait_clear(&id, ShareLock, &holder, ms);
}
UT_TEST(eight_active_ninth_must_wait_then_use_real_peer_clear)
{
	reset();
	for (int i = 0; i < 8; i++)
		UT_ASSERT(submit(i));
	UT_ASSERT_EQ(sends, 8);
	UT_ASSERT_EQ(grants, 0);
	UT_ASSERT(submit(8));
	UT_ASSERT_EQ(sends, 8);
	UT_ASSERT_EQ(grants, 0);
	UT_ASSERT_EQ(rejects, 0);
	cluster_lms_native_probe_recv_reply(wire_ids[0], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	cluster_lms_native_probe_retry_tick();
	UT_ASSERT_EQ(sends, 9);
	UT_ASSERT_EQ(grants, 1);
	UT_ASSERT_EQ(rejects, 0);
	cluster_lms_native_probe_recv_reply(wire_ids[8], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT_EQ(grants, 2);
	UT_ASSERT_EQ(releases, 0);
}
UT_TEST(sixteen_requests_never_exceed_eight_active_and_all_finish)
{
	reset();
	for (int i = 0; i < 16; i++)
		UT_ASSERT(submit(i));
	UT_ASSERT_EQ(occupied(), 16);
	UT_ASSERT_EQ(sends, 8);
	for (unsigned i = 0; i < sends; i++) {
		cluster_lms_native_probe_recv_reply(wire_ids[i], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
		cluster_lms_native_probe_retry_tick();
	}
	UT_ASSERT_EQ(grants, 16);
	UT_ASSERT_EQ(sends, 16);
	UT_ASSERT_EQ(peak_active, 8);
	UT_ASSERT_EQ(occupied(), 0);
	UT_ASSERT_EQ(rejects, 0);
	UT_ASSERT(wakes > 0);
}
UT_TEST(oldest_pending_is_promoted_first)
{
	reset();
	for (int i = 0; i < 12; i++)
		UT_ASSERT(submit(i));
	cluster_lms_native_probe_recv_reply(wire_ids[7], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	cluster_lms_native_probe_retry_tick();
	UT_ASSERT_EQ(wire_ids[8], 9);
	cluster_lms_native_probe_recv_reply(wire_ids[6], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	cluster_lms_native_probe_retry_tick();
	UT_ASSERT_EQ(wire_ids[9], 10);
}
UT_TEST(canceled_queued_request_cannot_grant_or_pin_capacity)
{
	reset();
	for (int i = 0; i < 9; i++)
		UT_ASSERT(submit(i));
	holders[8].request_id = 0;
	cluster_lms_native_probe_retry_tick();
	UT_ASSERT_EQ(occupied(), 8);
	UT_ASSERT_EQ(sends, 8);
	UT_ASSERT_EQ(grants, 0);
	cluster_lms_native_probe_recv_reply(9, 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT_EQ(grants, 0);
	UT_ASSERT_EQ(rejects, 0);
	UT_ASSERT_EQ(releases, 0);
}
UT_TEST(active_cancel_and_old_reply_do_not_attach_to_reused_slot)
{
	uint64 old_id, new_id;
	reset();
	UT_ASSERT(submit(0));
	old_id = wire_ids[0];
	holders[0].request_id = 0;
	cluster_lms_native_probe_recv_reply(old_id, 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT_EQ(occupied(), 0);
	UT_ASSERT_EQ(grants, 0);
	UT_ASSERT(submit(1));
	new_id = wire_ids[1];
	UT_ASSERT(old_id != new_id);
	cluster_lms_native_probe_recv_reply(old_id, 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT_EQ(grants, 0);
	cluster_lms_native_probe_recv_reply(new_id, 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT_EQ(grants, 1);
	UT_ASSERT_EQ(occupied(), 0);
}
UT_TEST(completion_owner_blocks_reentrant_duplicate_and_retry)
{
	reset();
	UT_ASSERT(submit(0));
	nested_completion = true;
	cluster_lms_native_probe_recv_reply(wire_ids[0], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT_EQ(grants, 1);
	UT_ASSERT_EQ(releases, 0);
	UT_ASSERT_EQ(occupied(), 0);
}
UT_TEST(nowait_and_convert_do_not_gain_capacity_wait)
{
	reset();
	for (int i = 0; i < 8; i++)
		UT_ASSERT(submit(i));
	UT_ASSERT(!submit_op(8, GES_REQ_OPCODE_REQUEST_NOWAIT));
	UT_ASSERT(!submit_op(9, GES_REQ_OPCODE_CONVERT));
	UT_ASSERT_EQ(occupied(), 8);
	UT_ASSERT_EQ(sends, 8);
}
UT_TEST(real_descriptor_exhaustion_retains_bounded_refusal)
{
	reset();
	for (int i = 0; i < 64; i++)
		UT_ASSERT(submit(i));
	UT_ASSERT(!submit(64));
	UT_ASSERT_EQ(occupied(), 64);
	UT_ASSERT_EQ(sends, 8);
}
UT_TEST(local_capacity_wait_observes_real_clear_and_releases_descriptor)
{
	reset();
	for (int i = 0; i < 8; i++)
		UT_ASSERT(submit(i));
	sleep_mode = 1;
	UT_ASSERT(local_wait(1000));
	UT_ASSERT(sleeps > 0);
	UT_ASSERT_EQ(cv_cancels, 1);
	UT_ASSERT_EQ(grants, 8);
	UT_ASSERT_EQ(occupied(), 0);
	UT_ASSERT_EQ(rejects, 0);
}
UT_TEST(local_cancel_releases_only_its_queued_descriptor)
{
	volatile bool caught = false;
	reset();
	for (int i = 0; i < 8; i++)
		UT_ASSERT(submit(i));
	sleep_mode = 2;
	PG_TRY();
	{
		(void)local_wait(1000);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cv_cancels, 1);
	UT_ASSERT_EQ(occupied(), 8);
	UT_ASSERT_EQ(grants, 0);
	UT_ASSERT_EQ(rejects, 0);
	UT_ASSERT_EQ(releases, 0);
}
UT_TEST(local_actual_deadline_does_not_restart_during_capacity_wait)
{
	reset();
	for (int i = 0; i < 8; i++)
		UT_ASSERT(submit(i));
	UT_ASSERT(!local_wait(200));
	UT_ASSERT_EQ(now_us, 1200000);
	UT_ASSERT_EQ(occupied(), 8);
	UT_ASSERT_EQ(cv_cancels, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.native_probe_timeout_count), 1);
}
UT_TEST(local_full_descriptor_wait_can_resume_without_expansion)
{
	reset();
	for (int i = 0; i < 64; i++)
		UT_ASSERT(submit(i));
	sleep_mode = 1;
	UT_ASSERT(local_wait(2000));
	UT_ASSERT_EQ(grants, 64);
	UT_ASSERT_EQ(occupied(), 0);
	UT_ASSERT_EQ(sends, 65);
	UT_ASSERT_EQ(peak_active, 8);
}
UT_TEST(foreign_same_procno_is_not_local_backend_cleanup)
{
	reset();
	UT_ASSERT(submit(99));
	cluster_lms_native_probe_cleanup_on_backend_exit(99);
	UT_ASSERT_EQ(occupied(), 1);
	cluster_lms_native_probe_recv_reply(wire_ids[0], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT_EQ(grants, 1);
}
UT_TEST(epoch_change_retires_pending_and_active_without_grant)
{
	reset();
	for (int i = 0; i < 9; i++)
		UT_ASSERT(submit(i));
	epoch = 10;
	cluster_lms_native_probe_retry_tick();
	UT_ASSERT_EQ(grants, 0);
	UT_ASSERT_EQ(occupied(), 0);
	UT_ASSERT_EQ(rejects, 9);
	UT_ASSERT_EQ(releases, 9);
}
UT_TEST(real_conflict_stays_ungranted_until_a_fresh_probe_clear)
{
	reset();
	UT_ASSERT(submit(0));
	cluster_lms_native_probe_recv_reply(wire_ids[0], 1, CLUSTER_NATIVE_LOCK_PROBE_WAITER_CONFLICT);
	UT_ASSERT_EQ(grants, 0);
	UT_ASSERT_EQ(occupied(), 1);
	now_us += 500000;
	cluster_lms_native_probe_retry_tick();
	UT_ASSERT_EQ(sends, 2);
	UT_ASSERT_EQ(grants, 0);
	cluster_lms_native_probe_recv_reply(wire_ids[1], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT_EQ(grants, 1);
	UT_ASSERT_EQ(occupied(), 0);
}
UT_TEST(convert_retry_expiry_preserves_old_mode_holder)
{
	reset();
	UT_ASSERT(submit_op(0, GES_REQ_OPCODE_CONVERT));
	for (int i = 0; i < 61; i++) {
		now_us += 500000;
		cluster_lms_native_probe_retry_tick();
	}
	UT_ASSERT_EQ(rejects, 1);
	UT_ASSERT_EQ(releases, 0);
	UT_ASSERT_EQ(grants, 0);
	UT_ASSERT_EQ(occupied(), 0);
}
UT_TEST(unexpected_peer_and_duplicate_reply_are_not_extra_evidence)
{
	reset();
	UT_ASSERT(submit(0));
	cluster_lms_native_probe_recv_reply(wire_ids[0], 2, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT_EQ(grants, 0);
	cluster_lms_native_probe_recv_reply(wire_ids[0], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	cluster_lms_native_probe_recv_reply(wire_ids[0], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT_EQ(grants, 1);
	UT_ASSERT_EQ(occupied(), 0);
}

static ClusterGesDedupKey
receipt_key(uint32 procno, uint64 request_id)
{
	/* Hand-derived ingress identity: origin1, REQUEST, epoch9/master7. */
	ClusterGesDedupKey key
		= { 1, GES_REQ_OPCODE_REQUEST, request_id, 9, (UINT64_C(9) << 32) | 7, procno, 0 };
	return key;
}

static ClusterGesDedupLookupStatus
lookup_receipt(const ClusterGesDedupKey *key, GesReplyPayload *reply)
{
	uint16 size = 0;
	return cluster_ges_dedup_lookup_or_register(key, (uint8 *)reply, sizeof(*reply), &size);
}

UT_TEST(native_clear_completes_the_registered_nonzero_procno_receipt)
{
	ClusterGesDedupKey key = receipt_key(37, 1037);
	GesReplyPayload reply = { 0 };
	reset();
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_MISS_REGISTERED);
	UT_ASSERT(submit(37));
	cluster_lms_native_probe_recv_reply(wire_ids[0], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT_EQ(grants, 1);
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ(reply.holder_procno, 37);
	UT_ASSERT_EQ(reply.opcode, GES_REPLY_OPCODE_GRANT);
	UT_ASSERT_EQ(occupied(), 0);
}

UT_TEST(native_timeout_completes_the_exact_reject_receipt)
{
	ClusterGesDedupKey key = receipt_key(37, 1037);
	GesReplyPayload reply = { 0 };
	reset();
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_MISS_REGISTERED);
	UT_ASSERT(submit(37));
	for (int i = 0; i < 61; i++) {
		now_us += 500000;
		cluster_lms_native_probe_retry_tick();
	}
	UT_ASSERT_EQ(rejects, 1);
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ(reply.holder_procno, 37);
	UT_ASSERT_EQ(reply.reject_reason, GES_REJECT_REASON_TIMEOUT);
	UT_ASSERT_EQ(grants, 0);
}

UT_TEST(native_completion_cannot_overwrite_another_backend_receipt)
{
	ClusterGesDedupKey key = receipt_key(37, 1037);
	ClusterGesDedupKey decoy = receipt_key(0, 1037);
	GesReplyPayload reply = { 0 };
	reset();
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ(lookup_receipt(&decoy, &reply), CLUSTER_GES_DEDUP_MISS_REGISTERED);
	UT_ASSERT(submit(37));
	cluster_lms_native_probe_recv_reply(wire_ids[0], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT_EQ(lookup_receipt(&decoy, &reply), CLUSTER_GES_DEDUP_IN_FLIGHT_DUPLICATE);
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ(reply.holder_procno, 37);
	UT_ASSERT_EQ(cluster_ges_dedup_entry_count(), 2);
}

UT_TEST(only_completed_exact_receipts_can_be_retired)
{
	ClusterGesDedupKey key = receipt_key(37, 1037);
	ClusterGesDedupKey other = receipt_key(38, 1037);
	GesReplyPayload reply = { 0 };
	reset();
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_MISS_REGISTERED);
	UT_ASSERT(!cluster_ges_dedup_remove_completed(&key));
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_IN_FLIGHT_DUPLICATE);
	UT_ASSERT(submit(37));
	cluster_lms_native_probe_recv_reply(wire_ids[0], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT(!cluster_ges_dedup_remove_completed(&other));
	UT_ASSERT(cluster_ges_dedup_remove_completed(&key));
	UT_ASSERT_EQ(cluster_ges_dedup_entry_count(), 0);
	UT_ASSERT(!cluster_ges_dedup_remove_completed(&key));
}

UT_TEST(native_grant_release_cycles_do_not_fill_the_receipt_table)
{
	GesReplyPayload reply = { 0 };
	reset();
	for (int i = 0; i < 257; i++) {
		ClusterGesDedupKey key = receipt_key(37, 1037 + i);
		UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_MISS_REGISTERED);
		/* Only the external holder/wire fixtures roll over, not the actual
		 * collector or dedup state. Each old request has really completed. */
		n_holders = 0;
		sends = 0;
		UT_ASSERT(submit_exact(37, 1037 + i, GES_REQ_OPCODE_REQUEST));
		cluster_lms_native_probe_recv_reply(wire_ids[0], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
		(void)cluster_ges_dedup_remove_completed(&key);
	}
	UT_ASSERT_EQ(grants, 257);
	UT_ASSERT_EQ(occupied(), 0);
	UT_ASSERT_EQ(cluster_ges_dedup_entry_count(), 0);
	UT_ASSERT_EQ(cluster_ges_dedup_full_reject_count(), 0);
}
UT_TEST(normal_stop_probe_owner_and_original_slot_locks)
{
	const char *reason;
	int slot;

	cluster_lms_state = NULL;
	UT_ASSERT_EQ(cluster_lms_native_probe_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(reason, "NATIVE_PROBE_UNINITIALIZED");
	reset();
	UT_ASSERT_EQ(cluster_lms_native_probe_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_READY);
	IsUnderPostmaster = false;
	UT_ASSERT_EQ(cluster_lms_native_probe_normal_stop_poll(NULL, NULL),
				 CLUSTER_NORMAL_STOP_INVALID);
	IsUnderPostmaster = true;
	LWLockAcquire(&state.native_probe_slots[63].lock.lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_lms_native_probe_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(reason, "NATIVE_PROBE_LOCK_HELD");
	LWLockRelease(&state.native_probe_slots[63].lock.lock);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(normal_stop_probe_all_descriptors_and_invalid_priority)
{
	const char *reason;
	int slot;
	ClusterLmsNativeLockProbeSlot *last;

	reset();
	UT_ASSERT(submit(0));
	last = &state.native_probe_slots[63];
	for (uint64 phase = PROBE_ACTIVE; phase <= PROBE_COMPLETING; phase++) {
		pg_atomic_write_u64(&last->in_use, phase);
		last->probe_id = 64;
		UT_ASSERT_EQ(cluster_lms_native_probe_normal_stop_poll(&slot, &reason),
					 CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_EQ(pg_atomic_read_u64(&last->in_use), phase);
	}
	pg_atomic_write_u64(&last->in_use, 99);
	UT_ASSERT_EQ(cluster_lms_native_probe_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(slot, 63);
	UT_ASSERT_STR_EQ(reason, "NATIVE_PROBE_STATE");
	UT_ASSERT_EQ(grants, 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(normal_stop_probe_original_finish_not_final_ready)
{
	const char *reason;
	int slot;

	reset();
	for (int i = 0; i < 16; i++)
		UT_ASSERT(submit(i));
	UT_ASSERT_EQ(cluster_lms_native_probe_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	stop_poll_in_completion = true;
	stop_new_work_allowed = false;
	stop_new_work_calls = 0;
	for (unsigned i = 0; i < sends; i++) {
		cluster_lms_native_probe_recv_reply(wire_ids[i], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
		cluster_lms_native_probe_retry_tick();
	}
	stop_poll_in_completion = false;
	UT_ASSERT_EQ(grants, 16);
	UT_ASSERT_EQ(stop_new_work_calls, 0);
	UT_ASSERT_EQ(occupied(), 0);
	UT_ASSERT_EQ(cluster_lms_native_probe_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(state.native_probe_slots[0].probe_id != 0); /* retained identity is not debt */
	stop_new_work_allowed = true;
	UT_ASSERT(submit(20));
	UT_ASSERT_EQ(cluster_lms_native_probe_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
}

UT_TEST(normal_stop_probe_free_residual_and_reply_shape)
{
	const char *reason;
	int slot;
	ClusterLmsNativeLockProbeSlot *s;

	reset();
	s = &state.native_probe_slots[63];
	s->final_ready = true;
	UT_ASSERT_EQ(cluster_lms_native_probe_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(reason, "NATIVE_PROBE_FREE_RESIDUAL");
	UT_ASSERT(s->final_ready);
	s->final_ready = false;
	UT_ASSERT(submit(0));
	s = &state.native_probe_slots[0];
	s->received_replies_bitmap |= 0x80000000U;
	UT_ASSERT_EQ(cluster_lms_native_probe_normal_stop_poll(&slot, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(reason, "NATIVE_PROBE_IDENTITY");
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(normal_stop_probe_new_service_context_not_frontend_wait)
{
	ClusterLmsSharedState before;

	reset();
	memcpy(&before, &state, sizeof(before));
	stop_new_work_allowed = false;
	UT_ASSERT(!submit(0));
	UT_ASSERT_EQ(stop_new_work_calls, 1);
	UT_ASSERT_EQ(occupied(), 0);
	UT_ASSERT_EQ(sends, 0);
	UT_ASSERT_EQ(grants, 0);
	UT_ASSERT_EQ(memcmp(&state, &before, sizeof(state)), 0);
	UT_ASSERT_EQ(held_count, 0);

	/* Smart-stop may be awaiting an already running frontend. The service
	 * seal must not be inserted into its common descriptor allocator. */
	reset();
	stop_new_work_allowed = false;
	MyBackendType = B_BACKEND;
	sleep_mode = 1;
	UT_ASSERT(local_wait(60000));
	MyBackendType = B_LMS;
	UT_ASSERT_EQ(stop_new_work_calls, 0);
	UT_ASSERT_EQ(occupied(), 0);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(normal_stop_ges_new_receipt_not_replay_or_completion)
{
	ClusterGesDedupKey key = receipt_key(37, 1037);
	GesReplyPayload reply = { 0 };

	reset();
	stop_new_work_allowed = false;
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_FULL);
	UT_ASSERT_EQ(stop_new_work_calls, 1);
	UT_ASSERT_EQ(cluster_ges_dedup_entry_count(), 0);
	stop_new_work_allowed = true;
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_MISS_REGISTERED);
	stop_new_work_allowed = false;
	stop_new_work_calls = 0;
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_IN_FLIGHT_DUPLICATE);
	cluster_ges_dedup_record_reply(&key, (uint8 *)&reply, sizeof(reply));
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_CACHED_REPLY);
	UT_ASSERT(cluster_ges_dedup_remove_completed(&key));
	UT_ASSERT_EQ(stop_new_work_calls, 0);
}

UT_TEST(normal_stop_ges_original_completion_retains_cache)
{
	ClusterGesDedupKey key = receipt_key(37, 1037);
	GesReplyPayload reply = { 0 };
	const char *domain, *reason;
	uint64 request;

	reset();
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(cluster_ges_dedup_normal_stop_poll(&domain, &request, &reason),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ(cluster_ges_dedup_normal_stop_poll(&domain, &request, &reason),
				 CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(request, key.request_id);
	UT_ASSERT_STR_EQ(reason, "GES_DEDUP_IN_FLIGHT");
	UT_ASSERT(submit(37));
	cluster_lms_native_probe_recv_reply(wire_ids[0], 1, CLUSTER_NATIVE_LOCK_PROBE_CLEAR);
	UT_ASSERT_EQ(cluster_ges_dedup_normal_stop_poll(&domain, &request, &reason),
				 CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(cluster_ges_dedup_entry_count(), 1);
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ(reply.holder_procno, 37);
	UT_ASSERT_EQ(reply.opcode, GES_REPLY_OPCODE_GRANT);
	UT_ASSERT_EQ(held_count, 0);
	MyBackendType = B_LMS;
}

UT_TEST(normal_stop_ges_invalid_overrides_pending_and_wrong_observer)
{
	ClusterGesDedupKey key = receipt_key(37, 1037);
	GesReplyPayload reply = { 0 };
	const char *domain, *reason;
	uint64 request;

	reset();
	MyBackendType = B_LMON;
	UT_ASSERT_EQ(lookup_receipt(&key, &reply), CLUSTER_GES_DEDUP_MISS_REGISTERED);
	dedup_used[255] = true;
	dedup_rows[255].key = receipt_key(99, 1099);
	dedup_rows[255].status = CLUSTER_GES_DEDUP_CACHED_REPLY;
	dedup_rows[255].cached_reply_len = 53;
	UT_ASSERT_EQ(cluster_ges_dedup_normal_stop_poll(&domain, &request, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_EQ(request, 1099);
	UT_ASSERT_STR_EQ(reason, "GES_DEDUP_REPLY_SHAPE");
	UT_ASSERT_EQ(dedup_rows[255].cached_reply_len, 53);
	reset();
	LWLockAcquire(&dedup_lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_ges_dedup_normal_stop_poll(NULL, NULL, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(reason, "GES_DEDUP_CALLER_LOCK");
	LWLockRelease(&dedup_lock);
	MyBackendType = B_BACKEND;
	UT_ASSERT_EQ(cluster_ges_dedup_normal_stop_poll(NULL, NULL, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(reason, "GES_DEDUP_OBSERVER_ROLE");
	MyBackendType = B_LMON;
	cluster_ges_dedup_htab = NULL;
	UT_ASSERT_EQ(cluster_ges_dedup_normal_stop_poll(NULL, NULL, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(reason, "GES_DEDUP_UNINITIALIZED");
	MyBackendType = B_LMS;
}

int
main(void)
{
	printf("1..30\n");
	UT_RUN(normal_stop_probe_owner_and_original_slot_locks);
	UT_RUN(normal_stop_probe_all_descriptors_and_invalid_priority);
	UT_RUN(normal_stop_probe_original_finish_not_final_ready);
	UT_RUN(normal_stop_probe_free_residual_and_reply_shape);
	UT_RUN(normal_stop_probe_new_service_context_not_frontend_wait);
	UT_RUN(normal_stop_ges_new_receipt_not_replay_or_completion);
	UT_RUN(normal_stop_ges_original_completion_retains_cache);
	UT_RUN(normal_stop_ges_invalid_overrides_pending_and_wrong_observer);
	UT_RUN(eight_active_ninth_must_wait_then_use_real_peer_clear);
	UT_RUN(sixteen_requests_never_exceed_eight_active_and_all_finish);
	UT_RUN(oldest_pending_is_promoted_first);
	UT_RUN(canceled_queued_request_cannot_grant_or_pin_capacity);
	UT_RUN(active_cancel_and_old_reply_do_not_attach_to_reused_slot);
	UT_RUN(completion_owner_blocks_reentrant_duplicate_and_retry);
	UT_RUN(nowait_and_convert_do_not_gain_capacity_wait);
	UT_RUN(real_descriptor_exhaustion_retains_bounded_refusal);
	UT_RUN(local_capacity_wait_observes_real_clear_and_releases_descriptor);
	UT_RUN(local_cancel_releases_only_its_queued_descriptor);
	UT_RUN(local_actual_deadline_does_not_restart_during_capacity_wait);
	UT_RUN(local_full_descriptor_wait_can_resume_without_expansion);
	UT_RUN(foreign_same_procno_is_not_local_backend_cleanup);
	UT_RUN(epoch_change_retires_pending_and_active_without_grant);
	UT_RUN(real_conflict_stays_ungranted_until_a_fresh_probe_clear);
	UT_RUN(convert_retry_expiry_preserves_old_mode_holder);
	UT_RUN(unexpected_peer_and_duplicate_reply_are_not_extra_evidence);
	UT_RUN(native_clear_completes_the_registered_nonzero_procno_receipt);
	UT_RUN(native_timeout_completes_the_exact_reject_receipt);
	UT_RUN(native_completion_cannot_overwrite_another_backend_receipt);
	UT_RUN(only_completed_exact_receipts_can_be_retired);
	UT_RUN(native_grant_release_cycles_do_not_fill_the_receipt_table);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
