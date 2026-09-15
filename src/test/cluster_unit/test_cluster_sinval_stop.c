/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual SI producer/consumer and ACK lifecycle. PG shmem, hash storage,
 * locks, wakeups and transport are fixtures, not remote completion proof. */
#include "postgres.h"
#include "../../backend/cluster/cluster_sinval.c"
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
ProcessingMode Mode = NormalProcessing;
bool cluster_enabled = true;
int cluster_node_id = 0;
int cluster_sinval_broadcast_max_queue_size = 64;
int cluster_sinval_broadcast_batch_size = 64;
int cluster_sinval_ack_wait_slots = 64;
int cluster_injection_armed_count = 0;
int MyProcPid = 0;
BackendType MyBackendType = B_LMON;
PGPROC *MyProc;
PROC_HDR *ProcGlobal;
static PROC_HDR proc_header;
static PGPROC procs[4];
static void *allocations[4];
static int allocation_count;
static LWLock *held_lock;
static ClusterSinvalAckWaitEntry hash_entries[64];
static bool hash_used[64];
static char hash_identity;
static int wake_count, apply_count, reset_count, send_count;
static bool gate_admit = true;
static unsigned gate_calls;
static ClusterNodeInfo known_peer;

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	Assert(!modifies_data && held_lock == NULL);
	gate_calls++;
	return gate_admit;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d %s\n", file, line, condition);
	abort();
}
Size
add_size(Size a, Size b)
{
	Assert(a <= SIZE_MAX - b);
	return a + b;
}
Size
mul_size(Size a, Size b)
{
	Assert(b == 0 || a <= SIZE_MAX / b);
	return a * b;
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	Assert(allocation_count < 4);
	*found = false;
	allocations[allocation_count] = calloc(1, size);
	Assert(allocations[allocation_count] != NULL);
	return allocations[allocation_count++];
}
HTAB *
ShmemInitHash(const char *name, long init_size, long max_size, HASHCTL *info, int flags)
{
	Assert(max_size == 64 && info->entrysize == sizeof(hash_entries[0]));
	Assert(info->keysize == sizeof(uint64));
	memset(hash_entries, 0, sizeof(hash_entries));
	memset(hash_used, 0, sizeof(hash_used));
	return (HTAB *)&hash_identity;
}
void *
hash_search(HTAB *table, const void *key, HASHACTION action, bool *found)
{
	int free_slot = -1;
	Assert(table == (HTAB *)&hash_identity && held_lock == ClusterSinvalAckWaitLock);
	*found = false;
	for (int i = 0; i < 64; i++) {
		if (!hash_used[i]) {
			if (free_slot < 0)
				free_slot = i;
			continue;
		}
		if (hash_entries[i].batch_id == *(const uint64 *)key) {
			*found = true;
			if (action == HASH_REMOVE)
				hash_used[i] = false;
			return &hash_entries[i];
		}
	}
	if ((action == HASH_ENTER || action == HASH_ENTER_NULL) && free_slot >= 0) {
		hash_used[free_slot] = true;
		memset(&hash_entries[free_slot], 0, sizeof(hash_entries[free_slot]));
		hash_entries[free_slot].batch_id = *(const uint64 *)key;
		return &hash_entries[free_slot];
	}
	return NULL;
}
void
hash_seq_init(HASH_SEQ_STATUS *scan, HTAB *table)
{
	Assert(held_lock == ClusterSinvalAckWaitLock);
	memset(scan, 0, sizeof(*scan));
	scan->hashp = table;
}
void *
hash_seq_search(HASH_SEQ_STATUS *scan)
{
	Assert(held_lock == ClusterSinvalAckWaitLock);
	while (scan->curBucket < 64) {
		uint32 i = scan->curBucket++;
		if (hash_used[i])
			return &hash_entries[i];
	}
	return NULL;
}
void
hash_seq_term(HASH_SEQ_STATUS *scan)
{
	Assert(held_lock == ClusterSinvalAckWaitLock);
}
void
LWLockInitialize(LWLock *lock, int tranche)
{
	memset(lock, 0, sizeof(*lock));
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	Assert(held_lock == NULL);
	held_lock = lock;
	return true;
}
bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	return LWLockAcquire(lock, mode);
}
void
LWLockRelease(LWLock *lock)
{
	Assert(held_lock == lock);
	held_lock = NULL;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	return held_lock == lock;
}
void
cluster_lmon_duty_mark_dirty(ClusterLmonDuty duty)
{
	Assert(held_lock == NULL);
}
void
cluster_lmon_wakeup(void)
{
	Assert(held_lock == NULL);
}
void
SetLatch(Latch *latch)
{
	Assert(held_lock == NULL);
	wake_count++;
}
TimestampTz
GetCurrentTimestamp(void)
{
	return 1000000;
}
uint64
cluster_epoch_get_current(void)
{
	return 1;
}
bool
cluster_injection_should_skip(const char *name)
{
	return false;
}
void
cluster_injection_run(const char *name)
{
	abort();
}
void
SendSharedInvalidMessages(const SharedInvalidationMessage *msgs, int n)
{
	Assert(held_lock == NULL && n > 0);
	apply_count += n;
}
void
SIResetAll(void)
{
	Assert(held_lock == NULL);
	reset_count++;
}
void *
palloc0(Size size)
{
	return calloc(1, size);
}
void
pfree(void *ptr)
{
	free(ptr);
}
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node)
{
	return node == 1 ? &known_peer : NULL;
}
bool
cluster_touched_peers_stamp(int32 node, ClusterTouchKind kind)
{
	return true;
}
void
cluster_ic_send_envelope_fanout(uint8 type, const void *payload, uint32 len,
								ClusterICFanoutResult *results)
{
	Assert(held_lock == NULL);
	for (int i = 0; i < CLUSTER_MAX_NODES; i++)
		results[i] = CLUSTER_IC_FANOUT_PEER_DOWN;
	results[1] = CLUSTER_IC_FANOUT_DONE;
	send_count++;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 dest, const void *payload, uint32 len)
{
	Assert(held_lock == NULL);
	send_count++;
	return CLUSTER_IC_SEND_DONE;
}
bool
errstart(int level, const char *domain)
{
	return false;
}
bool
errstart_cold(int level, const char *domain)
{
	return false;
}
void
errfinish(const char *file, int line, const char *func)
{
	abort();
}
int
errcode(int code)
{
	return 0;
}
int
errmsg(const char *format, ...)
{
	return 0;
}
int
errhint(const char *format, ...)
{
	return 0;
}

static ClusterNormalStopPollResult
poll_all(void)
{
	const char *domain, *reason;
	uint64 key;
	return cluster_sinval_normal_stop_poll(&domain, &key, &reason);
}
static void
reset_test(void)
{
	Assert(held_lock == NULL);
	for (int i = 0; i < allocation_count; i++)
		free(allocations[i]);
	allocation_count = 0;
	cluster_sinval_outbound_shmem_init();
	cluster_sinval_inbound_shmem_init();
	cluster_sinval_ack_wait_shmem_init();
	cluster_sinval_ack_outbound_shmem_init();
	memset(&proc_header, 0, sizeof(proc_header));
	memset(procs, 0, sizeof(procs));
	proc_header.allProcs = procs;
	proc_header.allProcCount = lengthof(procs);
	ProcGlobal = &proc_header;
	MyProc = &procs[0];
	wake_count = apply_count = reset_count = send_count = 0;
	gate_admit = true;
	gate_calls = 0;
}

UT_TEST(test_required_state_and_nonzero_empty_cursors)
{
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_INVALID);
	reset_test();
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_READY);
	pg_atomic_write_u32(&ClusterSinvalInbound->head, 61);
	pg_atomic_write_u32(&ClusterSinvalInbound->tail, 61);
	pg_atomic_write_u64(&ClusterSinval->next_batch_id, 900);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_actual_inbound_outbound_and_ack_handoffs)
{
	SharedInvalidationMessage msg = { 0 };
	reset_test();
	UT_ASSERT(cluster_sinval_enqueue_batch(&msg, 1));
	UT_ASSERT(cluster_sinval_inbound_try_enqueue(8, &msg, 1, 1));
	cluster_sinval_ack_outbound_enqueue(8, 1, SINVAL_ACK_RESET_PENDING);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
	cluster_sinval_drain_outbound_and_broadcast();
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
	cluster_sinval_drain_inbound_and_apply();
	UT_ASSERT_EQ(apply_count, 1);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
	cluster_sinval_drain_ack_outbound_and_send();
	UT_ASSERT_EQ(send_count, 2);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_READY);
	/* Transport fixture accepted; this READY does not sign remote apply. */
}
UT_TEST(test_all_acknowledged_still_needs_original_remove)
{
	uint64 id;
	reset_test();
	id = cluster_sinval_ack_wait_alloc_batch_id();
	UT_ASSERT(cluster_sinval_ack_wait_begin(id, 6, 9000000));
	cluster_sinval_ack_wait_record(id, 1);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
	cluster_sinval_ack_wait_record(id, 2);
	UT_ASSERT(cluster_sinval_ack_wait_is_complete(id));
	UT_ASSERT_EQ(wake_count, 1);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
	cluster_sinval_ack_wait_remove(id);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_READY);
}
UT_TEST(test_reset_flags_need_original_consumers)
{
	reset_test();
	pg_atomic_write_u32(&ClusterSinval->inbound_overflow_reset_pending, 1);
	cluster_sinval_request_reset_all_broadcast();
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
	cluster_sinval_apply_inbound_overflow_reset_if_pending();
	UT_ASSERT_EQ(reset_count, 1);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
	cluster_sinval_broadcast_reset_all();
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_READY);
	pg_atomic_write_u32(&ClusterSinval->inbound_overflow_reset_pending, 2);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_INVALID);
}
UT_TEST(test_later_malformed_ack_overrides_pending)
{
	SharedInvalidationMessage msg = { 0 };
	const char *domain, *reason;
	uint64 key;
	reset_test();
	UT_ASSERT(cluster_sinval_enqueue_batch(&msg, 1));
	UT_ASSERT(cluster_sinval_ack_wait_begin(77, 2, 9000000));
	hash_entries[0].ack_received_mask = 4;
	UT_ASSERT_EQ(cluster_sinval_normal_stop_poll(&domain, &key, &reason),
				 CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(domain, "ack_wait");
	UT_ASSERT_EQ(key, 77);
	hash_entries[0].ack_received_mask = 0;
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
}
UT_TEST(test_geometry_live_identity_and_original_lock)
{
	SharedInvalidationMessage msg = { 0 };
	reset_test();
	ClusterSinvalInbound->capacity++;
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_INVALID);
	ClusterSinvalInbound->capacity--;
	UT_ASSERT(cluster_sinval_inbound_try_enqueue(9, &msg, 1, 1));
	ClusterSinvalInbound->slots[0].nmsgs = 0;
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_INVALID);
	ClusterSinvalInbound->slots[0].nmsgs = 1;
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
	LWLockAcquire(ClusterSinvalAckWaitLock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_INVALID);
	LWLockRelease(ClusterSinvalAckWaitLock);
	cluster_sinval_drain_inbound_and_apply();
	cluster_sinval_ack_outbound_enqueue(3, 1, 9);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_new_inbound_and_reset_seal_after_validation_before_publication)
{
	struct {
		SinvalBroadcastHeader hdr;
		SharedInvalidationMessage msg;
	} frame = { 0 };
	ClusterICEnvelope env = { 0 };
	reset_test();
	gate_admit = false;
	frame.hdr.batch_id = 90;
	frame.hdr.epoch = 1;
	frame.hdr.source_node = 1;
	frame.hdr.nmsgs = 1;
	frame.hdr.flags = SINVAL_REQUIRES_ACK;
	env.source_node_id = 1;
	/* Original length validation must run before new admission. */
	cluster_sinval_handle_envelope(&env, &frame);
	UT_ASSERT_EQ(gate_calls, 0);
	env.payload_length = sizeof(frame);
	cluster_sinval_handle_envelope(&env, &frame);
	UT_ASSERT_EQ(gate_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterSinvalInbound->tail), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterSinvalAckOutbound->tail), 0);
	frame.hdr.nmsgs = 0;
	frame.hdr.flags = SINVAL_RESET_ALL_BROADCAST;
	env.payload_length = sizeof(frame.hdr);
	cluster_sinval_handle_envelope(&env, &frame);
	UT_ASSERT_EQ(gate_calls, 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterSinval->inbound_overflow_reset_pending), 0);
	reset_test();
	frame.hdr.nmsgs = 1;
	frame.hdr.flags = SINVAL_REQUIRES_ACK;
	env.payload_length = sizeof(frame);
	cluster_sinval_handle_envelope(&env, &frame);
	UT_ASSERT_EQ(gate_calls, 1);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
	cluster_sinval_drain_inbound_and_apply();
	cluster_sinval_drain_ack_outbound_and_send();
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_existing_exact_ack_completion_bypasses_new_work_seal)
{
	ClusterICEnvelope env = { 0 };
	SinvalAckHeader ack = { 0 };
	reset_test();
	UT_ASSERT(cluster_sinval_ack_wait_begin(88, 2, 9000000));
	gate_admit = false;
	env.source_node_id = 1;
	env.payload_length = sizeof(ack);
	ack.batch_id = 88;
	ack.epoch = 1;
	ack.acker_node = 1;
	ack.status = SINVAL_ACK_DONE;
	cluster_sinval_handle_ack_envelope(&env, &ack);
	UT_ASSERT_EQ(gate_calls, 0);
	UT_ASSERT_EQ(wake_count, 1);
	UT_ASSERT(cluster_sinval_ack_wait_is_complete(88));
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
	cluster_sinval_ack_wait_remove(88);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_READY);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_required_state_and_nonzero_empty_cursors);
	UT_RUN(test_actual_inbound_outbound_and_ack_handoffs);
	UT_RUN(test_all_acknowledged_still_needs_original_remove);
	UT_RUN(test_reset_flags_need_original_consumers);
	UT_RUN(test_later_malformed_ack_overrides_pending);
	UT_RUN(test_geometry_live_identity_and_original_lock);
	UT_RUN(test_new_inbound_and_reset_seal_after_validation_before_publication);
	UT_RUN(test_existing_exact_ack_completion_bypasses_new_work_seal);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
