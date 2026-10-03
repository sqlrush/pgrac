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
bool cluster_shared_catalog = false;
int cluster_sinval_ack_mode = CLUSTER_SINVAL_ACK_MODE_PEER_ENQUEUED;
int cluster_sinval_ack_timeout_ms = 5000;
int cluster_node_id = 0;
int cluster_sinval_broadcast_max_queue_size = 64;
int cluster_sinval_broadcast_batch_size = 64;
int cluster_sinval_ack_wait_slots = 64;
int cluster_injection_armed_count = 0;
int MyProcPid = 0;
volatile uint32 CritSectionCount = 0;
BackendType MyBackendType = B_LMON;
PGPROC *MyProc;
Latch *MyLatch;
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
static int initfile_invalidations;
static bool initfile_locked;
static uint8 sent_type;
static union {
	uint64 align;
	char bytes[2048];
} sent_storage;
#define sent_payload sent_storage.bytes
static uint32 sent_length;
static TimestampTz clock_us = 1000000;
static bool retry_transport;
static unsigned wait_count, publication_sends;
static bool gate_admit = true;
static unsigned gate_calls;
static ClusterNodeInfo known_peer;
static ClusterR4MembershipSnapshot current_membership;
static uint64 current_epoch = 1;
static bool membership_valid = true, prebump_pending;
static bool reset_during_apply;
static uint64 concurrent_reset_ticket;
static bool initfile_should_fail;
static sigjmp_buf reset_failure_boundary;

int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	/* Contention in this single-thread fixture is a recursive-lock bug. */
	abort();
}

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
	return clock_us;
}
void
ResetLatch(Latch *latch)
{}
int
WaitLatch(Latch *latch, int events, long timeout, uint32 event)
{
	wait_count++;
	Assert(wait_count < 10);
	clock_us += 6000000;
	cluster_sinval_apply_inbound_overflow_reset_if_pending();
	cluster_sinval_drain_outbound_and_broadcast();
	return WL_TIMEOUT;
}
ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 node)
{
	return node == 1 ? CLUSTER_CSSD_PEER_ALIVE : CLUSTER_CSSD_PEER_DEAD;
}
uint64
cluster_epoch_get_current(void)
{
	return current_epoch;
}
bool
cluster_reconfig_lmon_snapshot_r4_membership(ClusterR4MembershipSnapshot *out)
{
	*out = current_membership;
	return membership_valid;
}
bool
cluster_reconfig_has_pending_prebump_stage(void)
{
	return prebump_pending;
}
uint64
cluster_qvotec_get_self_incarnation(void)
{
	return 100;
}
ClusterMembershipState
cluster_membership_get_state(int32 node)
{
	return CLUSTER_MEMBER_MEMBER;
}
uint64
cluster_membership_get_last_admitted_incarnation(int32 node)
{
	return node == 0 ? 100 : 200;
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
RelationCacheInitFilePreInvalidateAll(void)
{
	Assert(!initfile_locked && held_lock == NULL);
	if (initfile_should_fail)
		siglongjmp(reset_failure_boundary, 1);
	initfile_locked = true;
	initfile_invalidations++;
}
void
RelationCacheInitFilePostInvalidate(void)
{
	Assert(initfile_locked && held_lock == NULL);
	initfile_locked = false;
}
void
SIResetAll(void)
{
	Assert(held_lock == NULL);
	reset_count++;
	if (reset_during_apply) {
		reset_during_apply = false;
		concurrent_reset_ticket = cluster_sinval_request_reset();
	}
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
	return node == 1 || node == 127 ? &known_peer : NULL;
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
	Assert(len <= sizeof(sent_payload));
	sent_type = type;
	sent_length = len;
	memcpy(sent_payload, payload, len);
	if (retry_transport && type == PGRAC_IC_MSG_SINVAL && ++publication_sends > 1) {
		SinvalPublicationAckHeader ack = { 0 };
		const SinvalPublicationHeader *pub = payload;
		ClusterICEnvelope env = { 0 };

		ack.base.batch_id = pub->base.batch_id;
		ack.base.epoch = pub->base.epoch;
		ack.base.acker_node = dest;
		ack.base.flags = SINVAL_PUBLICATION;
		ack.origin_incarnation = pub->origin_incarnation;
		ack.target_incarnation = pub->target_incarnation;
		env.source_node_id = dest;
		env.payload_length = sizeof(ack);
		cluster_sinval_handle_ack_envelope(&env, &ack);
	}
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
	MyLatch = &MyProc->procLatch;
	clock_us = 1000000;
	initfile_invalidations = 0;
	initfile_locked = false;
	wait_count = publication_sends = 0;
	retry_transport = false;
	cluster_shared_catalog = false;
	cluster_sinval_clear_commit();
	wake_count = apply_count = reset_count = send_count = 0;
	gate_admit = true;
	gate_calls = 0;
	current_epoch = 1;
	membership_valid = true;
	prebump_pending = false;
	reset_during_apply = false;
	concurrent_reset_ticket = 0;
	initfile_should_fail = false;
	memset(&current_membership, 0, sizeof(current_membership));
	current_membership.formation_epoch = 1;
	current_membership.admitted_members_lo = 3;
	current_membership.local_self_boot_incarnation = 100;
	current_membership.admitted_incarnation[0] = 100;
	current_membership.admitted_incarnation[1] = 200;
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
	cluster_sinval_ack_outbound_enqueue(8, 1, SINVAL_ACK_RESET_PENDING, 1);
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
	cluster_sinval_ack_outbound_enqueue(3, 1, 9, 1);
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

UT_TEST(test_receive_ack_follows_local_si_install)
{
	struct {
		SinvalBroadcastHeader hdr;
		SharedInvalidationMessage msg;
	} frame = { 0 };
	ClusterICEnvelope env = { 0 };
	reset_test();
	frame.hdr.batch_id = 90;
	frame.hdr.epoch = 1;
	frame.hdr.source_node = 1;
	frame.hdr.nmsgs = 1;
	frame.hdr.flags = SINVAL_REQUIRES_ACK;
	env.source_node_id = 1;
	env.payload_length = sizeof(frame);
	cluster_sinval_handle_envelope(&env, &frame);
	UT_ASSERT_EQ(apply_count, 0);
	cluster_sinval_drain_ack_outbound_and_send();
	UT_ASSERT_EQ(send_count, 0);
	cluster_sinval_drain_inbound_and_apply();
	UT_ASSERT_EQ(apply_count, 1);
	cluster_sinval_drain_ack_outbound_and_send();
	UT_ASSERT_EQ(send_count, 1);
}

UT_TEST(test_pending_reset_is_not_remote_completion)
{
	SinvalAckHeader ack = { 0 };
	ClusterICEnvelope env = { 0 };
	reset_test();
	UT_ASSERT(cluster_sinval_ack_wait_begin(88, 2, 9000000));
	env.source_node_id = 1;
	env.payload_length = sizeof(ack);
	ack.batch_id = 88;
	ack.epoch = 1;
	ack.acker_node = 1;
	ack.status = SINVAL_ACK_RESET_PENDING;
	cluster_sinval_handle_ack_envelope(&env, &ack);
	UT_ASSERT(!cluster_sinval_ack_wait_is_complete(88));
	UT_ASSERT_EQ(wake_count, 0);
}

UT_TEST(test_future_ack_is_not_remote_completion)
{
	SinvalAckHeader ack = { 0 };
	ClusterICEnvelope env = { 0 };
	reset_test();
	UT_ASSERT(cluster_sinval_ack_wait_begin(88, 2, 9000000));
	env.source_node_id = 1;
	env.payload_length = sizeof(ack);
	ack.batch_id = 88;
	ack.epoch = 2;
	ack.acker_node = 1;
	ack.status = SINVAL_ACK_DONE;
	cluster_sinval_handle_ack_envelope(&env, &ack);
	UT_ASSERT(!cluster_sinval_ack_wait_is_complete(88));
}

UT_TEST(test_publication_receive_binds_both_incarnations)
{
	struct {
		SinvalBroadcastHeader hdr;
		uint64 origin_incarnation;
		uint64 target_incarnation;
		SharedInvalidationMessage msg;
	} frame = { 0 };
	ClusterICEnvelope env = { 0 };
	reset_test();
	frame.hdr.batch_id = 90;
	frame.hdr.epoch = 1;
	frame.hdr.source_node = 1;
	frame.hdr.nmsgs = 1;
	frame.hdr.flags = SINVAL_REQUIRES_ACK | SINVAL_PUBLICATION;
	frame.origin_incarnation = 201;
	frame.target_incarnation = 100;
	env.source_node_id = 1;
	env.payload_length = sizeof(frame);
	cluster_sinval_handle_envelope(&env, &frame);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterSinvalInbound->tail), 0);
	frame.origin_incarnation = 200;
	frame.target_incarnation = 101;
	cluster_sinval_handle_envelope(&env, &frame);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterSinvalInbound->tail), 0);
	frame.target_incarnation = 100;
	cluster_sinval_handle_envelope(&env, &frame);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterSinvalInbound->tail), 1);
	UT_ASSERT_EQ(send_count, 0);
	cluster_sinval_drain_inbound_and_apply();
	UT_ASSERT_EQ(apply_count, 1);
	cluster_sinval_drain_ack_outbound_and_send();
	UT_ASSERT_EQ(send_count, 1);
	UT_ASSERT_EQ(sent_type, PGRAC_IC_MSG_SINVAL_ACK);
	UT_ASSERT_EQ(sent_length, 40);
	UT_ASSERT_EQ(initfile_invalidations, 1);
	UT_ASSERT(!initfile_locked);
	UT_ASSERT_EQ(((SinvalAckHeader *)sent_payload)->batch_id, 90);
	UT_ASSERT_EQ(((SinvalAckHeader *)sent_payload)->flags, SINVAL_PUBLICATION);
	UT_ASSERT_EQ(*(uint64 *)(sent_payload + 24), 200);
	UT_ASSERT_EQ(*(uint64 *)(sent_payload + 32), 100);
}

UT_TEST(test_publication_retries_past_old_timeout)
{
	SharedInvalidationMessage msg = { 0 };
	reset_test();
	cluster_shared_catalog = true;
	retry_transport = true;
	UT_ASSERT_EQ(cluster_sinval_enqueue_and_wait_ack(&msg, 1), CLUSTER_SINVAL_ACK_DONE);
	UT_ASSERT(publication_sends >= 2);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_READY);
}

static void
begin_publication(uint64 id)
{
	ClusterR4MembershipSnapshot cohort;
	SharedInvalidationMessage msg = { 0 };
	UT_ASSERT(sinval_read_membership(&cohort));
	UT_ASSERT(sinval_publication_begin(id, &cohort, &msg, 1));
}

UT_TEST(test_legacy_helpers_cannot_fulfill_or_remove_publication)
{
	reset_test();
	begin_publication(91);
	UT_ASSERT(!cluster_sinval_ack_wait_is_complete(91));
	cluster_sinval_ack_wait_record(91, 1);
	UT_ASSERT(!cluster_sinval_ack_wait_is_complete(91));
	cluster_sinval_ack_wait_remove(91);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
}

UT_TEST(test_duplicate_legacy_begin_preserves_publication)
{
	reset_test();
	begin_publication(92);
	UT_ASSERT(!cluster_sinval_ack_wait_begin(92, 2, 9000000));
	UT_ASSERT(hash_entries[0].publication);
	UT_ASSERT_EQ(hash_entries[0].nmsgs, 1);
}

UT_TEST(test_publication_stop_checks_payload_and_cohort)
{
	reset_test();
	begin_publication(93);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
	hash_entries[0].nmsgs = 0;
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_INVALID);
	hash_entries[0].nmsgs = 1;
	hash_entries[0].received[1] = 1;
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_INVALID);
	hash_entries[0].received[1] = 0;
	hash_entries[0].target_incarnation[1] = 0;
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_INVALID);
}

UT_TEST(test_reconfig_cannot_retire_publication)
{
	reset_test();
	begin_publication(94);
	cluster_sinval_reset_all_on_reconfig();
	UT_ASSERT(!hash_entries[0].completion_signaled);
	UT_ASSERT_EQ(hash_entries[0].received[0], 0);
	UT_ASSERT_EQ(wake_count, 0);
	cluster_sinval_apply_inbound_overflow_reset_if_pending();
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
}

UT_TEST(test_epoch_change_retries_survivors_with_new_identity)
{
	SinvalPublicationAckHeader ack = { 0 };
	ClusterICEnvelope env = { 0 };

	reset_test();
	cluster_shared_catalog = true;
	current_membership.admitted_members_hi = UINT64_C(1) << 63;
	current_membership.admitted_incarnation[127] = 999;
	begin_publication(95);
	current_epoch = current_membership.formation_epoch = 2;
	current_membership.admitted_members_lo = 1; /* Node 1 has left. */
	current_membership.admitted_incarnation[1] = 0;
	cluster_sinval_reset_all_on_reconfig();
	cluster_sinval_drain_outbound_and_broadcast();
	UT_ASSERT_EQ(hash_entries[0].epoch, 1); /* Pending RESET is not proof. */
	cluster_sinval_apply_inbound_overflow_reset_if_pending();
	clock_us += 200000;
	cluster_sinval_drain_outbound_and_broadcast();
	UT_ASSERT_EQ(hash_entries[0].epoch, 2);
	UT_ASSERT_EQ(hash_entries[0].targets[0], 0);
	UT_ASSERT_EQ(hash_entries[0].targets[1], UINT64_C(1) << 63);
	UT_ASSERT_EQ(hash_entries[0].received[1], 0);
	UT_ASSERT_EQ(((SinvalPublicationHeader *)sent_payload)->base.epoch, 2);
	UT_ASSERT_EQ(((SinvalPublicationHeader *)sent_payload)->target_incarnation, 999);
	UT_ASSERT_EQ(reset_count, 1);
	UT_ASSERT_EQ(initfile_invalidations, 1);
	ack.base.batch_id = 95;
	ack.base.epoch = 1;
	ack.base.acker_node = 127;
	ack.base.flags = SINVAL_PUBLICATION;
	ack.origin_incarnation = 100;
	ack.target_incarnation = 999;
	env.source_node_id = 127;
	env.payload_length = sizeof(ack);
	cluster_sinval_handle_ack_envelope(&env, &ack);
	UT_ASSERT(!sinval_publication_complete(&hash_entries[0]));
	ack.base.epoch = 2;
	ack.target_incarnation = 998;
	cluster_sinval_handle_ack_envelope(&env, &ack);
	UT_ASSERT(!sinval_publication_complete(&hash_entries[0]));
	ack.target_incarnation = 999;
	cluster_sinval_handle_ack_envelope(&env, &ack);
	UT_ASSERT(sinval_publication_complete(&hash_entries[0]));
	sinval_publication_wait(95);
	UT_ASSERT(!hash_used[0]);
}

UT_TEST(test_join_and_delayed_membership_change_retarget_incomplete_batch)
{
	reset_test();
	cluster_shared_catalog = true;
	begin_publication(97);
	current_epoch = current_membership.formation_epoch = 2;
	current_membership.admitted_members_hi = UINT64_C(1) << 63;
	current_membership.admitted_incarnation[127] = 999;
	cluster_sinval_reset_all_on_reconfig();
	cluster_sinval_apply_inbound_overflow_reset_if_pending();
	cluster_sinval_drain_outbound_and_broadcast();
	UT_ASSERT_EQ(hash_entries[0].targets[0], 2);
	UT_ASSERT_EQ(hash_entries[0].targets[1], UINT64_C(1) << 63);
	UT_ASSERT_EQ(hash_entries[0].epoch, 2);
	/* The same epoch can precede the final local membership projection. */
	current_membership.admitted_members_lo = 1;
	current_membership.admitted_incarnation[1] = 0;
	clock_us += 200000;
	cluster_sinval_drain_outbound_and_broadcast();
	UT_ASSERT_EQ(hash_entries[0].targets[0], 0);
	UT_ASSERT_EQ(hash_entries[0].targets[1], UINT64_C(1) << 63);
	UT_ASSERT_EQ(hash_entries[0].nmsgs, 1);
}

UT_TEST(test_new_receiver_epoch_needs_full_reset_before_publication_ack)
{
	struct {
		SinvalPublicationHeader hdr;
		SharedInvalidationMessage msg;
	} frame = { 0 };
	ClusterICEnvelope env = { 0 };

	reset_test();
	cluster_shared_catalog = true;
	frame.hdr.base.batch_id = 98;
	frame.hdr.base.epoch = 1;
	frame.hdr.base.source_node = 1;
	frame.hdr.base.nmsgs = 1;
	frame.hdr.base.flags = SINVAL_REQUIRES_ACK | SINVAL_PUBLICATION;
	frame.hdr.origin_incarnation = 200;
	frame.hdr.target_incarnation = 100;
	env.source_node_id = 1;
	env.payload_length = sizeof(frame);
	cluster_sinval_handle_envelope(&env, &frame);
	cluster_sinval_drain_inbound_and_apply();
	cluster_sinval_drain_ack_outbound_and_send();
	UT_ASSERT_EQ(apply_count, 0);
	UT_ASSERT_EQ(send_count, 0);
	cluster_sinval_apply_inbound_overflow_reset_if_pending();
	UT_ASSERT_EQ(reset_count, 1);
	cluster_sinval_handle_envelope(&env, &frame); /* Retained sender retries. */
	cluster_sinval_drain_inbound_and_apply();
	cluster_sinval_drain_ack_outbound_and_send();
	UT_ASSERT_EQ(apply_count, 1);
	UT_ASSERT_EQ(send_count, 1);
	UT_ASSERT_EQ(initfile_invalidations, 2);
}

UT_TEST(test_last_peer_departure_finishes_only_after_valid_cohort_and_reset)
{
	reset_test();
	cluster_shared_catalog = true;
	begin_publication(96);
	current_epoch = current_membership.formation_epoch = 2;
	current_membership.admitted_members_lo = 1;
	current_membership.admitted_incarnation[1] = 0;
	cluster_sinval_reset_all_on_reconfig();
	cluster_sinval_apply_inbound_overflow_reset_if_pending();
	for (int phase = 0; phase < 2; phase++) {
		membership_valid = phase != 0;
		prebump_pending = phase == 1;
		clock_us += 200000;
		cluster_sinval_drain_outbound_and_broadcast();
		UT_ASSERT(!sinval_publication_complete(&hash_entries[0]));
		UT_ASSERT_EQ(hash_entries[0].epoch, 1);
	}
	prebump_pending = false;
	clock_us += 200000;
	cluster_sinval_drain_outbound_and_broadcast();
	UT_ASSERT(sinval_publication_complete(&hash_entries[0]));
	sinval_publication_wait(96);
	UT_ASSERT(!hash_used[0]);
}

UT_TEST(test_reset_ticket_does_not_cover_later_request_or_new_epoch)
{
	uint64 first;

	reset_test();
	cluster_shared_catalog = true;
	first = cluster_sinval_request_reset();
	UT_ASSERT(first != 0);
	UT_ASSERT(!cluster_sinval_reset_is_complete(first));
	reset_during_apply = true;
	cluster_sinval_apply_inbound_overflow_reset_if_pending();
	UT_ASSERT(cluster_sinval_reset_is_complete(first));
	UT_ASSERT(concurrent_reset_ticket > first);
	UT_ASSERT(!cluster_sinval_reset_is_complete(concurrent_reset_ticket));
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
	cluster_sinval_apply_inbound_overflow_reset_if_pending();
	UT_ASSERT(cluster_sinval_reset_is_complete(concurrent_reset_ticket));
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT(!cluster_sinval_reconfig_reset_ready(1));
	cluster_sinval_apply_inbound_overflow_reset_if_pending();
	UT_ASSERT(cluster_sinval_reconfig_reset_ready(1));
	current_epoch = 2;
	UT_ASSERT(!cluster_sinval_reconfig_reset_ready(1));
	UT_ASSERT(!cluster_sinval_reconfig_reset_ready(2));
	cluster_sinval_apply_inbound_overflow_reset_if_pending();
	UT_ASSERT(cluster_sinval_reconfig_reset_ready(2));
	UT_ASSERT_EQ(reset_count, 4);
	UT_ASSERT_EQ(initfile_invalidations, 4);
}

UT_TEST(test_overflow_reset_install_error_retains_responsibility)
{
	reset_test();
	pg_atomic_write_u32(&ClusterSinval->inbound_overflow_reset_pending, 1);
	initfile_should_fail = true;
	if (sigsetjmp(reset_failure_boundary, 1) == 0) {
		cluster_sinval_apply_inbound_overflow_reset_if_pending();
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(reset_count, 0);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_PENDING);
	initfile_should_fail = false;
	cluster_sinval_apply_inbound_overflow_reset_if_pending();
	UT_ASSERT_EQ(reset_count, 1);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_READY);
}

UT_TEST(test_high_node_exact_ack_and_full_table_retention)
{
	ClusterR4MembershipSnapshot cohort;
	SharedInvalidationMessage msg = { 0 };
	SinvalPublicationAckHeader ack = { 0 };
	ClusterICEnvelope env = { 0 };
	reset_test();
	UT_ASSERT(sinval_read_membership(&cohort));
	cohort.admitted_members_lo = 1;
	cohort.admitted_members_hi = UINT64_C(1) << 63;
	cohort.admitted_incarnation[127] = 999;
	for (int i = 0; i < 64; i++)
		UT_ASSERT(sinval_publication_begin(100 + i, &cohort, &msg, 1));
	UT_ASSERT(!sinval_publication_begin(200, &cohort, &msg, 1));
	ack.base.batch_id = 100;
	ack.base.epoch = 1;
	ack.base.acker_node = 127;
	ack.base.flags = SINVAL_PUBLICATION;
	ack.origin_incarnation = 100;
	ack.target_incarnation = 998;
	env.source_node_id = 127;
	env.payload_length = sizeof(ack);
	cluster_sinval_handle_ack_envelope(&env, &ack);
	UT_ASSERT_EQ(hash_entries[0].received[1], 0);
	ack.target_incarnation = 999;
	cluster_sinval_handle_ack_envelope(&env, &ack);
	UT_ASSERT_EQ(hash_entries[0].received[1], UINT64_C(1) << 63);
	UT_ASSERT_EQ(wake_count, 1);
	UT_ASSERT(cluster_sinval_ack_wait_is_complete(100));
	cluster_sinval_ack_wait_remove(100);
	UT_ASSERT(sinval_publication_begin(200, &cohort, &msg, 1));
}

UT_TEST(test_publication_rejects_invalid_native_message_ids)
{
	struct {
		SinvalPublicationHeader hdr;
		SharedInvalidationMessage msg;
	} frame = { 0 };
	ClusterICEnvelope env = { 0 };
	reset_test();
	frame.hdr.base.batch_id = 300;
	frame.hdr.base.epoch = 1;
	frame.hdr.base.source_node = 1;
	frame.hdr.base.nmsgs = 1;
	frame.hdr.base.flags = SINVAL_REQUIRES_ACK | SINVAL_PUBLICATION;
	frame.hdr.origin_incarnation = 200;
	frame.hdr.target_incarnation = 100;
	env.source_node_id = 1;
	env.payload_length = sizeof(frame);
	frame.msg.id = INT8_MAX;
	cluster_sinval_handle_envelope(&env, &frame);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterSinvalInbound->tail), 0);
	frame.msg.id = INT8_MIN;
	cluster_sinval_handle_envelope(&env, &frame);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterSinvalInbound->tail), 0);
}

UT_TEST(test_publication_overflow_needs_retry_and_duplicates_are_safe)
{
	struct {
		SinvalPublicationHeader hdr;
		SharedInvalidationMessage msg;
	} frame = { 0 };
	ClusterICEnvelope env = { 0 };
	reset_test();
	for (int i = 0; i < 63; i++)
		UT_ASSERT(cluster_sinval_inbound_try_enqueue(i + 1, &frame.msg, 1, 1));
	frame.hdr.base.batch_id = 300;
	frame.hdr.base.epoch = 1;
	frame.hdr.base.source_node = 1;
	frame.hdr.base.nmsgs = 1;
	frame.hdr.base.flags = SINVAL_REQUIRES_ACK | SINVAL_PUBLICATION;
	frame.hdr.origin_incarnation = 200;
	frame.hdr.target_incarnation = 100;
	env.source_node_id = 1;
	env.payload_length = sizeof(frame);
	cluster_sinval_handle_envelope(&env, &frame);
	cluster_sinval_apply_inbound_overflow_reset_if_pending();
	UT_ASSERT_EQ(reset_count, 1);
	cluster_sinval_drain_ack_outbound_and_send();
	UT_ASSERT_EQ(send_count, 0);
	cluster_sinval_drain_inbound_and_apply();
	UT_ASSERT_EQ(apply_count, 63);
	cluster_sinval_handle_envelope(&env, &frame);
	cluster_sinval_handle_envelope(&env, &frame);
	cluster_sinval_drain_inbound_and_apply();
	cluster_sinval_drain_ack_outbound_and_send();
	UT_ASSERT_EQ(apply_count, 65);
	UT_ASSERT_EQ(send_count, 2);
	UT_ASSERT_EQ(poll_all(), CLUSTER_NORMAL_STOP_READY);
}

int
main(void)
{
	UT_PLAN(26);
	UT_RUN(test_required_state_and_nonzero_empty_cursors);
	UT_RUN(test_actual_inbound_outbound_and_ack_handoffs);
	UT_RUN(test_all_acknowledged_still_needs_original_remove);
	UT_RUN(test_reset_flags_need_original_consumers);
	UT_RUN(test_later_malformed_ack_overrides_pending);
	UT_RUN(test_geometry_live_identity_and_original_lock);
	UT_RUN(test_new_inbound_and_reset_seal_after_validation_before_publication);
	UT_RUN(test_existing_exact_ack_completion_bypasses_new_work_seal);
	UT_RUN(test_receive_ack_follows_local_si_install);
	UT_RUN(test_pending_reset_is_not_remote_completion);
	UT_RUN(test_future_ack_is_not_remote_completion);
	UT_RUN(test_publication_receive_binds_both_incarnations);
	UT_RUN(test_publication_retries_past_old_timeout);
	UT_RUN(test_legacy_helpers_cannot_fulfill_or_remove_publication);
	UT_RUN(test_duplicate_legacy_begin_preserves_publication);
	UT_RUN(test_publication_stop_checks_payload_and_cohort);
	UT_RUN(test_reconfig_cannot_retire_publication);
	UT_RUN(test_epoch_change_retries_survivors_with_new_identity);
	UT_RUN(test_join_and_delayed_membership_change_retarget_incomplete_batch);
	UT_RUN(test_new_receiver_epoch_needs_full_reset_before_publication_ack);
	UT_RUN(test_last_peer_departure_finishes_only_after_valid_cohort_and_reset);
	UT_RUN(test_reset_ticket_does_not_cover_later_request_or_new_epoch);
	UT_RUN(test_overflow_reset_install_error_retains_responsibility);
	UT_RUN(test_high_node_exact_ack_and_full_table_retention);
	UT_RUN(test_publication_rejects_invalid_native_message_ids);
	UT_RUN(test_publication_overflow_needs_retry_and_duplicates_are_safe);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
