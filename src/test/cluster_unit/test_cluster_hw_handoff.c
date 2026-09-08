/*-------------------------------------------------------------------------
 *
 * test_cluster_hw_handoff.c
 *    Exercise actual sender, reply correlation, requester promotion and
 *    master cleanup against separately owned GRD address spaces.
 *
 * Backend allocation, formation, clock and transport are fixtures. Grant,
 * promotion, exact release and successor drain execute production C.
 * No database or live network is started by this test.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_hw_handoff.c
 *-------------------------------------------------------------------------
 */
#define main retained_grd_main
#define ShmemInitStruct retained_grd_shmem
#define ShmemInitHash retained_grd_hash
#define hash_get_num_entries retained_grd_hash_count
#define cluster_grd_outbound_enqueue_backend_request retained_grd_enqueue
#define cluster_grd_outbound_enqueue_cleanup_release retained_grd_cleanup_enqueue
#define cluster_grd_redeclare_all_registered retained_grd_redeclare
#define cluster_lms_get_shard_master_generation retained_grd_generation
#define errfinish retained_grd_errfinish
#define ProcGlobal retained_grd_proc_global
#include "test_cluster_grd.c"
#undef main
#undef ShmemInitStruct
#undef ShmemInitHash
#undef hash_get_num_entries
#undef cluster_grd_outbound_enqueue_backend_request
#undef cluster_grd_outbound_enqueue_cleanup_release
#undef cluster_grd_redeclare_all_registered
#undef cluster_lms_get_shard_master_generation
#undef errfinish
#undef ProcGlobal

#include <sys/wait.h>
#include <unistd.h>
#include "access/xact.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_ges_dedup.h"
#include "cluster/cluster_ges_reply_wait.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_lmd_wait_state.h"
#include "cluster/cluster_native_lock_probe.h"
#include "cluster/cluster_touched_peers.h"
#include "cluster/cluster_advisory.h"
#include "cluster/cluster_xnode_profile.h"
#include "storage/proc.h"

PROC_HDR *ProcGlobal;

/* Fixture checks must execute in both assertion and production builds. */
#define HW_CHECK(condition)                                                                        \
	do {                                                                                           \
		if (!(condition)) {                                                                        \
			fprintf(stderr, "fixture check failed: %s at %s:%d\n", #condition, __FILE__,           \
					__LINE__);                                                                     \
			abort();                                                                               \
		}                                                                                          \
	} while (0)

typedef enum HandoffFault {
	HW_NORMAL,
	HW_CANCEL_RESERVATION,
	HW_PRE_EPOCH,
	HW_PRE_ROUTE,
	HW_POST_EPOCH,
	HW_POST_ROUTE,
	HW_S4_ERROR_READY,
	HW_S4_ERROR_PENDING,
	HW_S5_ERROR_PRE,
	HW_S5_ERROR_POST,
	HW_NON_GRANT_NONE,
	HW_REJECT,
	HW_TIMEOUT_PENDING,
	HW_DEADLOCK_PENDING,
	HW_RETRANSMIT_REFUSED,
	HW_FIRST_ENQUEUE_REFUSED,
	HW_IDENTITY_MISMATCH
} HandoffFault;

static HandoffFault fault;
static bool guard_armed;
static int guard_reads;
static int cancel_sent;
static PGPROC requester_proc;
static GesReplyPayload actual_reply;
static ClusterICEnvelope actual_envelope;

void
errfinish(const char *file, int line, const char *function)
{
	(void)file;
	(void)line;
	(void)function;
	if (ut_current_elevel >= ERROR)
		pg_re_throw();
}

uint64
cluster_lms_get_shard_master_generation(void)
{
	uint64 generation = retained_grd_generation();
	if (guard_armed) {
		guard_reads++;
		if (guard_reads == 1 && fault == HW_POST_EPOCH)
			ut_mock_epoch++;
		if (guard_reads == 1 && fault == HW_POST_ROUTE)
			mock_lms_shard_master_generation++;
		if ((guard_reads == 1 && fault == HW_S5_ERROR_PRE)
			|| (guard_reads == 2 && fault == HW_S5_ERROR_POST))
			ereport(ERROR, (errmsg("injected guard failure")));
	}
	return generation;
}

static union {
	uint64 align;
	char bytes[4096];
} ges_memory, reply_memory;
static GesReplyWaitEntry reply_slots[8];
static bool reply_used[8];
static int reply_hash_token;
static int cleanup_sent;
static int request_sent;
static int invalid_replies_rejected;
static int master_to_requester[2], requester_to_master[2];
static pid_t master_child = -1;
static GesRequestPayload master_request;
static GesReplyPayload master_reply;
static bool master_work_pending;
static int master_reply_count;

typedef struct MasterPacket {
	GesReplyPayload reply;
	int held_mode;
	int successor_mode;
	int replies;
	int master;
	uint64 generation;
} MasterPacket;

void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	if (strcmp(name, "pgrac cluster ges") == 0
		|| strcmp(name, "pgrac cluster ges reply wait") == 0) {
		void *memory = strstr(name, "reply wait") ? reply_memory.bytes : ges_memory.bytes;
		HW_CHECK(size <= sizeof(ges_memory.bytes));
		memset(memory, 0, size);
		*found = false;
		return memory;
	}
	return retained_grd_shmem(name, size, found);
}

HTAB *
ShmemInitHash(const char *name, long initial, long maximum, HASHCTL *ctl, int flags)
{
	if (strcmp(name, "pgrac cluster ges reply wait htab") == 0) {
		HW_CHECK(ctl->keysize == sizeof(GesReplyWaitKey));
		HW_CHECK(ctl->entrysize == sizeof(GesReplyWaitEntry));
		memset(reply_slots, 0, sizeof(reply_slots));
		memset(reply_used, 0, sizeof(reply_used));
		return (HTAB *)&reply_hash_token;
	}
	return retained_grd_hash(name, initial, maximum, ctl, flags);
}

long
hash_get_num_entries(HTAB *hash)
{
	int i, n = 0;
	if (hash != (HTAB *)&reply_hash_token)
		return retained_grd_hash_count(hash);
	for (i = 0; i < 8; i++)
		n += reply_used[i];
	return n;
}

void *
hash_search(HTAB *hash, const void *key, HASHACTION action, bool *found)
{
	int i;
	HW_CHECK(hash == (HTAB *)&reply_hash_token);
	for (i = 0; i < 8; i++) {
		if (reply_used[i] && memcmp(&reply_slots[i].key, key, sizeof(GesReplyWaitKey)) == 0) {
			if (found)
				*found = true;
			if (action == HASH_REMOVE)
				reply_used[i] = false;
			return &reply_slots[i];
		}
	}
	if (found)
		*found = false;
	if (action == HASH_FIND || action == HASH_REMOVE)
		return NULL;
	HW_CHECK(action == HASH_ENTER || action == HASH_ENTER_NULL);
	for (i = 0; i < 8; i++)
		if (!reply_used[i]) {
			reply_used[i] = true;
			memset(&reply_slots[i], 0, sizeof(reply_slots[i]));
			memcpy(&reply_slots[i].key, key, sizeof(GesReplyWaitKey));
			return &reply_slots[i];
		}
	abort(); /* Capacity/eviction is not substituted with success. */
}

void
LWLockInitialize(LWLock *lock, int tranche)
{
	memset(lock, 0, sizeof(*lock));
}
void
ConditionVariableInit(ConditionVariable *cv)
{
	memset(cv, 0, sizeof(*cv));
}
void
ConditionVariablePrepareToSleep(ConditionVariable *cv)
{
	(void)cv;
}
bool
ConditionVariableCancelSleep(void)
{
	return false;
}
void
ConditionVariableBroadcast(ConditionVariable *cv)
{
	(void)cv;
}
bool
ConditionVariableTimedSleep(ConditionVariable *cv, long ms, uint32 event)
{
	(void)cv;
	(void)event;
	ut_mock_now += ms * 1000;
	return true;
}

int cluster_ges_reply_wait_max_entries = 8;
int cluster_ges_request_timeout_ms = 5000;
int cluster_ges_convert_timeout_ms = 5000;
int cluster_ges_retransmit_max_attempts = 3;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;
AuxProcType MyAuxProcType = NotAnAuxProcess;
TransactionId
GetTopTransactionIdIfAny(void)
{
	return InvalidTransactionId;
}
void
cluster_lmd_cancel_wait_edge(void)
{}
bool
cluster_cancel_token_consume(void)
{
	return fault == HW_DEADLOCK_PENDING;
}
bool
cluster_extend_liveness_is_sole_native(void)
{
	return false;
}
void
cluster_lms_inc_priority_starvation_observed(void)
{}

/* Formation is an explicit fixture precondition, not tested by this probe.
 * Recovery, native-probe, convert, cancellation, and cache-eviction paths
 * are not driven here; an unexpected entry must fail instead of grant. */
bool
cluster_authority_readiness_managed(void)
{
	return true;
}
bool
cluster_serving_ready_is_current(void)
{
	return true;
}
bool
cluster_recovery_transport_is_current(void)
{
	abort();
}
bool
cluster_recovery_authority_is_current(void)
{
	abort();
}
bool
cluster_recovery_authority_resid_mode_allowed(const ClusterResId *r, LOCKMODE m)
{
	(void)r;
	(void)m;
	abort();
}
bool
cluster_recovery_authority_request_allowed(const ClusterResId *r, LOCKMODE m, bool startup)
{
	(void)r;
	(void)m;
	(void)startup;
	abort();
}
bool
cluster_ges_dedup_remove_completed(const ClusterGesDedupKey *key)
{
	HW_CHECK(key->request_id == 201);
	return true; /* Dedup storage is a fixture, not the grant authority. */
}
void
cluster_ges_dedup_record_reply(const ClusterGesDedupKey *key, const uint8 *reply, uint16 length)
{
	HW_CHECK(key->request_id == 201 || key->request_id == 203);
	HW_CHECK(length == sizeof(GesReplyPayload));
	HW_CHECK(
		((const GesReplyPayload *)reply)->opcode == GES_REPLY_OPCODE_GRANT
		|| ((fault == HW_REJECT || fault == HW_NON_GRANT_NONE)
			&& ((const GesReplyPayload *)reply)->opcode == GES_REPLY_OPCODE_REJECT
			&& ((const GesReplyPayload *)reply)->reject_reason == GES_REJECT_REASON_SHARD_FROZEN));
}
bool
cluster_lms_native_probe_required(const ClusterResId *r, LOCKMODE mode)
{
	/* Actual predicate at cluster_lms.c:1658 excludes this HW resid. */
	HW_CHECK(r && r->type == CLUSTER_HW_RESID_TYPE && mode == ExclusiveLock);
	return false;
}
bool
cluster_lms_native_probe_schedule_grant(const ClusterResId *r, LOCKMODE mode,
										const ClusterGrdHolderId *holder, int32 source,
										uint32 opcode, uint64 generation, LOCKMODE old)
{
	(void)r;
	(void)mode;
	(void)holder;
	(void)source;
	(void)opcode;
	(void)generation;
	(void)old;
	abort();
}
bool
cluster_lms_native_probe_wait_clear(const ClusterResId *r, LOCKMODE mode,
									const ClusterGrdHolderId *holder, int ms)
{
	(void)r;
	(void)mode;
	(void)holder;
	(void)ms;
	abort();
}
bool
cluster_touched_peers_stamp(int32 node, ClusterTouchKind kind)
{
	(void)node;
	(void)kind;
	return true;
}
volatile sig_atomic_t InterruptPending = false;
volatile uint32 InterruptHoldoffCount = 0;
void
ProcessInterrupts(void)
{
	abort();
}
bool
cluster_grd_work_queue_enqueue(uint32 source, const void *payload, uint16 length)
{
	(void)source;
	(void)payload;
	(void)length;
	abort();
}
void
cluster_grd_outbound_enqueue_lmd_cancel(uint32 destination, const void *payload, uint16 length)
{
	const GesCancelWaitPayload *cancel = payload;
	HW_CHECK(destination == 3 && length == sizeof(*cancel));
	HW_CHECK(cancel->opcode == GES_REQ_OPCODE_CANCEL_WAIT
			 && cancel->kind == GES_CANCEL_WAIT_KIND_REQUEST);
	HW_CHECK(cancel->waiter_node_id == 1 && cancel->waiter_procno == 21
			 && cancel->waiter_request_id == 201);
	HW_CHECK(cancel->waiter_cluster_epoch == 1 && cancel->wait_seq == master_request.wait_seq);
	HW_CHECK(memcmp(cancel->resid, master_request.resid, sizeof(cancel->resid)) == 0);
	cancel_sent++;
}
void
cluster_advisory_counter_inc(ClusterAdvisoryCounter which)
{
	(void)which;
	abort();
}

static void
transfer_exact(int fd, void *bytes, size_t length, bool writing)
{
	char *p = bytes;
	while (length) {
		ssize_t n = writing ? write(fd, p, length) : read(fd, p, length);
		if (n <= 0)
			_exit(92);
		p += n;
		length -= n;
	}
}

bool
cluster_grd_work_queue_dequeue(ClusterGrdWorkItem *out)
{
	if (!master_work_pending)
		return false;
	memset(out, 0, sizeof(*out));
	out->source_node_id = master_request.holder_node_id;
	out->payload_len = sizeof(master_request);
	memcpy(out->payload, &master_request, sizeof(master_request));
	master_work_pending = false;
	return true;
}

void
cluster_grd_outbound_enqueue_lmon_reply(uint32 destination, const void *payload, uint16 length)
{
	HW_CHECK(destination == master_request.holder_node_id || destination == 2);
	HW_CHECK(length == sizeof(master_reply));
	memcpy(&master_reply, payload, length);
	master_reply_count++;
}

void
cluster_grd_outbound_enqueue_cleanup_release(uint32 destination, const void *payload, uint16 length)
{
	char command = 'R';
	HW_CHECK(master_child > 0 && destination == 3 && length == sizeof(master_request));
	HW_CHECK(((const GesRequestPayload *)payload)->opcode == GES_REQ_OPCODE_RELEASE);
	HW_CHECK(((const GesRequestPayload *)payload)->holder_request_id_lo == 201);
	HW_CHECK(memcmp(((const GesRequestPayload *)payload)->resid, master_request.resid,
					sizeof(master_request.resid))
			 == 0);
	HW_CHECK(((const GesRequestPayload *)payload)->wait_seq == master_request.wait_seq
			 || ((const GesRequestPayload *)payload)->wait_seq == 0); /* Orphan reply echo. */
	cleanup_sent++;
	transfer_exact(requester_to_master[1], &command, 1, true);
	transfer_exact(requester_to_master[1], (void *)payload, length, true);
}

static void
run_master(uint32 destination)
{
	const int32 nodes[] = { 0, 1, 2, 3 };
	ClusterResId resid;
	ClusterGrdHolderId holder, successor;
	LOCKMODE mode = NoLock;
	MasterPacket packet;
	char command;
	grd_lifecycle_reset(4);
	set_mock_declared(4, nodes);
	cluster_grd_master_map_init();
	cluster_node_id = destination;
	ut_mock_epoch = 1;
	ut_qvotec_quorum = true;
	mock_lms_shard_master_generation = 9;
	MyProc = NULL;
	memcpy(&resid, master_request.resid, sizeof(resid));
	memset(&holder, 0, sizeof(holder));
	holder.node_id = master_request.holder_node_id;
	holder.procno = master_request.holder_procno;
	holder.cluster_epoch = master_request.holder_cluster_epoch_lo;
	holder.request_id = master_request.holder_request_id_lo;
	HW_CHECK(cluster_grd_lookup_master(&resid) == (int32)destination);
	if (fault == HW_REJECT || fault == HW_NON_GRANT_NONE)
		cluster_grd_shard_set_phase(cluster_grd_shard_for_resource(&resid), GRD_SHARD_FROZEN);
	master_work_pending = true;
	master_reply_count = 0;
	HW_CHECK(cluster_ges_lmon_drain_work_queue() == 1);
	memset(&packet, 0, sizeof(packet));
	packet.reply = master_reply;
	packet.replies = master_reply_count;
	packet.master = cluster_grd_lookup_master_gen(&resid, &packet.generation);
	if (cluster_grd_holder_mode_by_id(&resid, &holder, &mode))
		packet.held_mode = mode;
	transfer_exact(master_to_requester[1], &packet, sizeof(packet), true);
	/* One eligible queued successor must advance on exact cleanup RELEASE. */
	successor = grd_lifecycle_holder(2, 23, 203);
	successor.cluster_epoch = 1;
	if (packet.held_mode == ExclusiveLock) {
		ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
		int nconflicts = 0;
		HW_CHECK(cluster_grd_entry_enqueue_or_grant(&resid, &successor, 2, 203, 9,
													GES_REQ_OPCODE_REQUEST, ExclusiveLock,
													conflicts, &nconflicts)
				 == CLUSTER_GRD_ENQUEUED_WAITER);
		HW_CHECK(nconflicts == 1);
	}
	for (;;) {
		transfer_exact(requester_to_master[0], &command, 1, false);
		if (command == 'R') {
			transfer_exact(requester_to_master[0], &master_request, sizeof(master_request), false);
			HW_CHECK(master_request.opcode == GES_REQ_OPCODE_RELEASE);
			master_work_pending = true;
			HW_CHECK(cluster_ges_lmon_drain_work_queue() == 1);
		} else if (command == 'Q') {
			mode = NoLock;
			packet.held_mode
				= cluster_grd_holder_mode_by_id(&resid, &holder, &mode) ? mode : NoLock;
			mode = NoLock;
			packet.successor_mode
				= cluster_grd_holder_mode_by_id(&resid, &successor, &mode) ? mode : NoLock;
			transfer_exact(master_to_requester[1], &packet, sizeof(packet), true);
			_exit(0);
		} else
			_exit(93);
	}
}

bool
cluster_grd_outbound_enqueue_backend_request(uint32 destination, const void *payload, uint16 length)
{
	MasterPacket packet;
	ClusterICEnvelope env;
	GesReplyWaitKey key;
	GesReplyWaitEntry *entry;
	GesReplyPayload wrong;
	HW_CHECK(length == sizeof(master_request));
	if (fault == HW_FIRST_ENQUEUE_REFUSED)
		return false;
	if (request_sent > 0) {
		HW_CHECK(memcmp(&master_request, payload, length) == 0);
		request_sent++;
		return fault != HW_RETRANSMIT_REFUSED;
	}
	memcpy(&master_request, payload, length);
	HW_CHECK(master_request.opcode == GES_REQ_OPCODE_REQUEST);
	request_sent++;
	HW_CHECK(pipe(master_to_requester) == 0 && pipe(requester_to_master) == 0);
	master_child = fork();
	HW_CHECK(master_child >= 0);
	if (master_child == 0) {
		close(master_to_requester[0]);
		close(requester_to_master[1]);
		alarm(10);
		run_master(destination);
	}
	close(master_to_requester[1]);
	close(requester_to_master[0]);
	transfer_exact(master_to_requester[0], &packet, sizeof(packet), false);
	HW_CHECK(packet.replies == 1);
	HW_CHECK(packet.master == (int32)destination && packet.generation == 9);
	if (fault == HW_REJECT || fault == HW_NON_GRANT_NONE) {
		HW_CHECK(packet.held_mode == NoLock && packet.reply.opcode == GES_REPLY_OPCODE_REJECT);
		HW_CHECK(packet.reply.reject_reason == GES_REJECT_REASON_SHARD_FROZEN);
	} else
		HW_CHECK(packet.held_mode == ExclusiveLock && packet.reply.opcode == GES_REPLY_OPCODE_GRANT
				 && packet.reply.reject_reason == GES_REJECT_REASON_NONE);
	memset(&env, 0, sizeof(env));
	env.source_node_id = destination;
	env.epoch = 1;
	memset(&key, 0, sizeof(key));
	key.request_id = master_request.holder_request_id_lo;
	key.source_node_id = cluster_node_id;
	key.dest_node_id = destination;
	key.request_opcode = GES_REQ_OPCODE_REQUEST;
	key.cluster_epoch = 1;
	entry = cluster_ges_reply_wait_lookup(&key);
	HW_CHECK(entry && !entry->ready);
	wrong = packet.reply;
	wrong.holder_request_id_lo++;
	cluster_ges_reply_handler(&env, &wrong);
	HW_CHECK(!entry->ready);
	invalid_replies_rejected++;
	env.source_node_id = 0;
	cluster_ges_reply_handler(&env, &packet.reply);
	HW_CHECK(!entry->ready);
	invalid_replies_rejected++;
	env.source_node_id = destination;
	env.epoch = 2;
	wrong = packet.reply;
	wrong.holder_cluster_epoch_lo = 2;
	cluster_ges_reply_handler(&env, &wrong);
	HW_CHECK(!entry->ready);
	invalid_replies_rejected++;
	env.epoch = 1;
	wrong = packet.reply;
	wrong.reply_for_opcode = GES_REQ_OPCODE_RELEASE;
	cluster_ges_reply_handler(&env, &wrong);
	HW_CHECK(!entry->ready);
	invalid_replies_rejected++;
	wrong = packet.reply;
	wrong.holder_node_id = 0;
	cluster_ges_reply_handler(&env, &wrong);
	HW_CHECK(!entry->ready);
	invalid_replies_rejected++;
	actual_envelope = env;
	actual_reply = packet.reply;
	/* Force cleanup to retain the original sequence after caller state moves. */
	pg_atomic_write_u64(&MyProc->cluster_lmd_wait.wait_seq, 777);
	if (fault == HW_S4_ERROR_PENDING)
		ereport(ERROR, (errmsg("injected enqueue failure after accepted REQUEST")));
	if (fault == HW_TIMEOUT_PENDING || fault == HW_DEADLOCK_PENDING
		|| fault == HW_RETRANSMIT_REFUSED)
		return true;
	if (fault == HW_NON_GRANT_NONE)
		packet.reply.reject_reason = GES_REJECT_REASON_NONE;
	cluster_ges_reply_handler(&env, &packet.reply);
	HW_CHECK(entry->ready);
	if (fault == HW_S4_ERROR_READY)
		ereport(ERROR, (errmsg("injected enqueue failure after delivered GRANT")));
	if (fault == HW_CANCEL_RESERVATION) {
		ClusterResId resid;
		ClusterGrdHolderId holder;
		memcpy(&resid, master_request.resid, sizeof(resid));
		memset(&holder, 0, sizeof(holder));
		holder.node_id = master_request.holder_node_id;
		holder.procno = master_request.holder_procno;
		holder.cluster_epoch = master_request.holder_cluster_epoch_lo;
		holder.request_id = master_request.holder_request_id_lo;
		HW_CHECK(cluster_grd_cancel_reservation_by_id(&resid, &holder) == CLUSTER_GRD_ENTRY_OK);
	}
	return true;
}

static void
setup_case(ClusterLockAcquireRequest *req, bool sibling)
{
	const int32 nodes[] = { 0, 1, 2, 3 };
	ClusterGrdHolderId other;
	uint64 generation;
	bool fast = false;
	guard_armed = false;
	guard_reads = 0;
	grd_lifecycle_reset(4);
	set_mock_declared(4, nodes);
	cluster_grd_master_map_init();
	cluster_node_id = 1;
	ut_mock_epoch = 1;
	ut_qvotec_quorum = true;
	mock_lms_shard_master_generation = 9;
	cluster_ges_shmem_init();
	cluster_ges_reply_wait_shmem_init();
	memset(&requester_proc, 0, sizeof(requester_proc));
	MyProc = &requester_proc;
	cluster_lmd_wait_state_init(&MyProc->cluster_lmd_wait);
	pg_atomic_write_u64(&MyProc->cluster_lmd_wait.wait_seq, 77);
	memset(req, 0, sizeof(*req));
	req->resid = (ClusterResId){ .field1 = 5,
								 .field2 = 16429,
								 .field3 = 1663,
								 .type = CLUSTER_HW_RESID_TYPE,
								 .lockmethodid = DEFAULT_LOCKMETHOD };
	HW_CHECK(cluster_grd_lookup_master(&req->resid) == 3);
	req->op = CLUSTER_LOCK_OP_REQUEST;
	req->lockmode = ExclusiveLock;
	req->timeout_ms = 5000;
	req->holder = grd_lifecycle_holder(1, 21, 201);
	req->holder.cluster_epoch = 1;
	req->request_id = req->holder.request_id;
	other = grd_lifecycle_holder(1, 22, 202);
	other.cluster_epoch = 1;
	UT_ASSERT_EQ(cluster_grd_try_reserve(&req->resid, &req->holder, ExclusiveLock, 1, &fast,
										 &req->master_gen_snapshot),
				 CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT(!fast);
	if (sibling)
		UT_ASSERT_EQ(
			cluster_grd_try_reserve(&req->resid, &other, ExclusiveLock, 1, NULL, &generation),
			CLUSTER_GRD_ENTRY_OK);
	cleanup_sent = request_sent = invalid_replies_rejected = cancel_sent = 0;
	memset(&actual_reply, 0, sizeof(actual_reply));
	memset(&actual_envelope, 0, sizeof(actual_envelope));
	master_child = -1;
}

static void
run_case(bool sibling, HandoffFault selected)
{
	static ClusterLockAcquireRequest req; /* Survives the injected PG longjmp. */
	ClusterGrdHolderId original, other;
	ClusterResId original_resid;
	MasterPacket post;
	volatile ClusterLockAcquireResult s4 = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	volatile ClusterLockAcquireResult s5 = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	volatile bool caught = false;
	LOCKMODE mode = NoLock;
	sigjmp_buf *saved_exception_stack = PG_exception_stack;
	bool s4_error = selected == HW_S4_ERROR_READY || selected == HW_S4_ERROR_PENDING;
	bool s5_error = selected == HW_S5_ERROR_PRE || selected == HW_S5_ERROR_POST;
	bool pending = selected == HW_S4_ERROR_PENDING || selected == HW_TIMEOUT_PENDING
				   || selected == HW_DEADLOCK_PENDING || selected == HW_RETRANSMIT_REFUSED;
	bool no_master_grant = selected == HW_REJECT || selected == HW_NON_GRANT_NONE
						   || selected == HW_FIRST_ENQUEUE_REFUSED;

	fault = selected;
	setup_case(&req, sibling);
	original = req.holder;
	original_resid = req.resid;
	other = grd_lifecycle_holder(1, 22, 202);
	other.cluster_epoch = 1;
	PG_TRY();
	{
		s4 = cluster_lock_acquire_s4_remote_request_wait(&req);
		if (s4 == CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK) {
			if (selected == HW_PRE_EPOCH)
				ut_mock_epoch++;
			if (selected == HW_PRE_ROUTE)
				mock_lms_shard_master_generation++;
			if (selected == HW_IDENTITY_MISMATCH)
				req.holder = other;
			guard_armed = true;
			s5 = cluster_lock_acquire_s5_promote(&req);
			guard_armed = false;
		} else
			(void)cluster_lock_acquire_s7_cleanup(&req);
	}
	PG_CATCH();
	{
		caught = true;
		guard_armed = false;
	}
	PG_END_TRY();
	UT_ASSERT(PG_exception_stack == saved_exception_stack);
	UT_ASSERT_EQ(caught, s4_error || s5_error);
	UT_ASSERT_EQ(MyProc->cluster_lmd_wait.active, 0);
	if (selected == HW_NORMAL) {
		UT_ASSERT_EQ(s4, CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK);
		UT_ASSERT_EQ(s5, CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
		UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	} else
		UT_ASSERT_NE(s5, CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	(void)cluster_lock_acquire_s7_cleanup(&req);
	(void)cluster_lock_acquire_s7_cleanup(&req);
	if (pending) {
		GesReplyWaitEntry *entry = cluster_ges_reply_wait_lookup(&req.hw_grant.key);
		UT_ASSERT(entry != NULL && entry->abandoned);
		UT_ASSERT_EQ(cancel_sent, 1);
		UT_ASSERT_EQ(cleanup_sent, 0);
		/* The delayed actual GRANT must trigger the existing orphan release. */
		cluster_ges_reply_handler(&actual_envelope, &actual_reply);
	}
	/* Duplicate GRANT cannot resurrect a departed caller or revoke success. */
	if (!no_master_grant)
		cluster_ges_reply_handler(&actual_envelope, &actual_reply);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
	mode = NoLock;
	UT_ASSERT_EQ(cluster_grd_holder_mode_by_id(&original_resid, &original, &mode),
				 selected == HW_NORMAL);
	if (selected == HW_NORMAL)
		UT_ASSERT_EQ(mode, ExclusiveLock);
	if (sibling)
		UT_ASSERT_EQ(cluster_grd_cancel_reservation_by_id(&original_resid, &other),
					 CLUSTER_GRD_ENTRY_OK);
	if (master_child > 0) {
		int status;
		char command = 'Q';
		transfer_exact(requester_to_master[1], &command, 1, true);
		transfer_exact(master_to_requester[0], &post, sizeof(post), false);
		close(requester_to_master[1]);
		close(master_to_requester[0]);
		HW_CHECK(waitpid(master_child, &status, 0) == master_child && WIFEXITED(status)
				 && WEXITSTATUS(status) == 0);
		master_child = -1;
		UT_ASSERT_EQ(invalid_replies_rejected, 5);
		UT_ASSERT_EQ(post.held_mode, selected == HW_NORMAL ? ExclusiveLock : NoLock);
		UT_ASSERT_EQ(post.successor_mode,
					 !no_master_grant && selected != HW_NORMAL ? ExclusiveLock : NoLock);
	}
	UT_ASSERT_EQ(cleanup_sent, !no_master_grant && selected != HW_NORMAL ? 1 : 0);
	if (selected == HW_NORMAL) {
		/* By-value transfer preserves normal-release ownership. */
		ClusterLockAcquireRequest copied = req;
		UT_ASSERT(copied.hw_grant.consumed && !copied.hw_grant.cleanup_pending);
	}
	MyProc = NULL;
}

UT_TEST(no_sibling_control)
{
	run_case(false, HW_NORMAL);
}
UT_TEST(real_grant_sibling_promotes)
{
	run_case(true, HW_NORMAL);
}
UT_TEST(post_grant_failure_retains_release_owner)
{
	run_case(true, HW_CANCEL_RESERVATION);
}
UT_TEST(epoch_before_promotion)
{
	run_case(true, HW_PRE_EPOCH);
}
UT_TEST(route_before_promotion)
{
	run_case(true, HW_PRE_ROUTE);
}
UT_TEST(epoch_after_promotion)
{
	run_case(true, HW_POST_EPOCH);
}
UT_TEST(route_after_promotion)
{
	run_case(true, HW_POST_ROUTE);
}
UT_TEST(s4_error_after_grant)
{
	run_case(true, HW_S4_ERROR_READY);
}
UT_TEST(s4_error_before_grant)
{
	run_case(true, HW_S4_ERROR_PENDING);
}
UT_TEST(s5_error_before_promotion)
{
	run_case(true, HW_S5_ERROR_PRE);
}
UT_TEST(s5_error_after_promotion)
{
	run_case(true, HW_S5_ERROR_POST);
}
UT_TEST(reject_none_is_not_grant)
{
	run_case(false, HW_NON_GRANT_NONE);
}
UT_TEST(explicit_reject_is_not_grant)
{
	run_case(false, HW_REJECT);
}
UT_TEST(timeout_then_late_grant)
{
	run_case(true, HW_TIMEOUT_PENDING);
}
UT_TEST(deadlock_then_late_grant)
{
	run_case(true, HW_DEADLOCK_PENDING);
}
UT_TEST(retransmit_refused_then_late_grant)
{
	run_case(true, HW_RETRANSMIT_REFUSED);
}
UT_TEST(first_enqueue_refused)
{
	run_case(true, HW_FIRST_ENQUEUE_REFUSED);
}
UT_TEST(identity_mismatch_does_not_release_sibling)
{
	run_case(true, HW_IDENTITY_MISMATCH);
}

UT_TEST(local_optimistic_generation_fence)
{
	ClusterLockAcquireRequest req;
	fault = HW_NORMAL;
	setup_case(&req, true);
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ(request_sent, 0);
	UT_ASSERT_EQ(cleanup_sent, 0);
	MyProc = NULL;
}

UT_TEST(local_master_uses_existing_holder)
{
	ClusterLockAcquireRequest req;
	uint64 snapshot;
	fault = HW_NORMAL;
	setup_case(&req, false);
	UT_ASSERT_EQ(cluster_grd_cancel_reservation_by_id(&req.resid, &req.holder),
				 CLUSTER_GRD_ENTRY_OK);
	cluster_node_id = 3;
	req.holder.node_id = 3;
	UT_ASSERT_EQ(
		cluster_grd_try_reserve(&req.resid, &req.holder, ExclusiveLock, 3, NULL, &snapshot),
		CLUSTER_GRD_ENTRY_OK);
	req.master_gen_snapshot = snapshot;
	UT_ASSERT_EQ(cluster_lock_acquire_s4_remote_request_wait(&req),
				 CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK);
	UT_ASSERT_EQ(req.hw_grant.key.request_id, 0);
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT_EQ(request_sent, 0);
	UT_ASSERT_EQ(cleanup_sent, 0);
	UT_ASSERT_EQ(cluster_lock_acquire_s6_release(&req), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	MyProc = NULL;
}

int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	alarm(30); /* Standalone fixture owner, not a database deadline. */
	UT_PLAN(20);
	UT_RUN(no_sibling_control);
	UT_RUN(real_grant_sibling_promotes);
	UT_RUN(post_grant_failure_retains_release_owner);
	UT_RUN(epoch_before_promotion);
	UT_RUN(route_before_promotion);
	UT_RUN(epoch_after_promotion);
	UT_RUN(route_after_promotion);
	UT_RUN(s4_error_after_grant);
	UT_RUN(s4_error_before_grant);
	UT_RUN(s5_error_before_promotion);
	UT_RUN(s5_error_after_promotion);
	UT_RUN(reject_none_is_not_grant);
	UT_RUN(explicit_reject_is_not_grant);
	UT_RUN(timeout_then_late_grant);
	UT_RUN(deadlock_then_late_grant);
	UT_RUN(retransmit_refused_then_late_grant);
	UT_RUN(first_enqueue_refused);
	UT_RUN(identity_mismatch_does_not_release_sibling);
	UT_RUN(local_optimistic_generation_fence);
	UT_RUN(local_master_uses_existing_holder);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
