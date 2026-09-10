/* Execute the production collector; only process, native-lock and wire edges
 * are controlled. Deferred wire replies keep exactly eight real probes live. */
#include "postgres.h"
#include LMS_NATIVE_PROBE_SOURCE_PATH
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

int cluster_node_id = 0;
int cluster_lms_native_lock_probe_max_inflight = 8;
int cluster_lms_native_lock_probe_retry_interval_ms = 500;
int cluster_lms_native_lock_probe_retry_budget = 60;
int cluster_ges_request_timeout_ms = 60000;
int MyProcPid = 100;
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
	(void)lock;
	(void)mode;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	(void)lock;
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
cluster_ges_dedup_record_reply(const ClusterGesDedupKey *key, const uint8 *reply, uint16 len)
{
	(void)key;
	(void)reply;
	(void)len;
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
	wire_ids[sends++] = p->probe_id;
}
void
cluster_grd_outbound_enqueue_lmon_reply(uint32 dest, const void *data, uint16 len)
{
	const GesReplyPayload *p = data;
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
}
static bool
submit_op(int procno, uint32 opcode)
{
	ClusterResId id;
	ClusterGrdHolderId holder;
	memset(&id, 0, sizeof(id));
	id.type = LOCKTAG_RELATION;
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 1;
	holder.procno = procno;
	holder.cluster_epoch = 9;
	holder.request_id = procno + 1000;
	holders[n_holders++] = holder;
	return cluster_lms_native_probe_schedule_grant(
		&id, ShareLock, &holder, 1, opcode, (UINT64_C(9) << 32) | 7,
		opcode == GES_REQ_OPCODE_CONVERT ? AccessShareLock : NoLock);
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
int
main(void)
{
	printf("1..17\n");
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
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
