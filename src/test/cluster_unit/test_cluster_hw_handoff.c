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
/* This composition links the real control registry on every platform. */
#ifndef PGRAC_REAL_CONTROL_CENSUS
#define PGRAC_REAL_CONTROL_CENSUS
#endif
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
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_ges_dedup.h"
#include "cluster/cluster_ges_reply_wait.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_lock_owner.h"
#include "cluster/cluster_lmd_wait_state.h"
#include "cluster/cluster_native_lock_probe.h"
#include "cluster/cluster_touched_peers.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_advisory.h"
#include "cluster/cluster_xnode_profile.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "storage/latch.h"

#ifndef PGRAC_HW_HANDOFF_EMBEDDED
/* The dedicated control service suites execute these boundaries. */
Latch *MyLatch;
void
ResetLatch(Latch *latch pg_attribute_unused())
{}
int
WaitLatch(Latch *latch pg_attribute_unused(), int events pg_attribute_unused(),
		  long timeout pg_attribute_unused(), uint32 event pg_attribute_unused())
{
	abort();
}
void
before_shmem_exit(pg_on_exit_callback callback pg_attribute_unused(),
				  Datum arg pg_attribute_unused())
{
	abort();
}
bool
cluster_recovery_transport_components_current(void)
{
	return false;
}
bool
cluster_ges_dedup_retire_control_request(uint32 node pg_attribute_unused(),
										 uint32 procno pg_attribute_unused(),
										 uint64 epoch pg_attribute_unused(),
										 uint64 request pg_attribute_unused())
{
	abort();
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type pg_attribute_unused(), int32 dest pg_attribute_unused(),
						 const void *payload pg_attribute_unused(),
						 uint32 len pg_attribute_unused())
{
	abort();
}
#endif

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
static bool relation_case;
static bool relation_nowait_case;
static bool cf_case;
static LOCKMODE cf_mode = ShareLock;
static bool cooperative_case;
static bool queued_cut_case;
typedef enum ReleaseFault {
	RELEASE_NORMAL,
	RELEASE_REJECT_NONE,
	RELEASE_POST_EPOCH,
	RELEASE_POST_MASTER,
	RELEASE_ORIGIN_RESTART,
	RELEASE_ERROR_BEFORE_ACK,
	RELEASE_ERROR_AFTER_ACK,
	RELEASE_ENQUEUE_REFUSED,
	RELEASE_DROP_ACK,
	RELEASE_ENTRY_ROUTE
} ReleaseFault;
static bool release_case;
static ReleaseFault release_fault;
static bool release_route_changed;
static int cooperative_sleeps;
static int cooperative_drift;
static bool guard_armed;
static int guard_reads;
static int cancel_sent;
static PGPROC requester_proc;
static PROC_HDR hw_local_proc_header;
static PGPROC hw_local_procs[23];
static const ClusterLockAcquireRequest *hw_release_on_wait;
static bool hw_error_after_wait_release;
static bool hw_local_case;
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

void
FlushErrorState(void)
{
	ut_current_elevel = 0;
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
static GesReplyPayload master_release_reply;
static bool master_work_pending;
static uint64 master_queued_generation;
static int master_reply_count;
static int local_native_probes;
static GesRequestPayload local_cleanup_release;
bool cluster_local_fast_path_enabled = true;

void cluster_grd_outbound_enqueue_lmon_reply(uint32 destination, const void *payload,
											 uint16 length);

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
#ifdef PGRAC_REAL_CONTROL_CENSUS
	if (strcmp(name, "pgrac control requests") == 0) {
		static void *memory;

		if (memory == NULL)
			memory = calloc(1, size);
		HW_CHECK(memory != NULL);
		memset(memory, 0, size);
		*found = false;
		return memory;
	}
#endif
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
	cooperative_sleeps++;
	if (hw_release_on_wait != NULL) {
		const ClusterLockAcquireRequest *owner = hw_release_on_wait;
		int procno = MyProc->pgprocno;
		hw_release_on_wait = NULL;
		MyProc->pgprocno = owner->holder.procno;
		HW_CHECK(cluster_lock_acquire_s6_release(owner) == CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
		MyProc->pgprocno = procno;
		if (hw_error_after_wait_release)
			ereport(ERROR, (errmsg("injected cancellation after local HW grant")));
		return false;
	}
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
	if (release_case && release_fault == RELEASE_ENTRY_ROUTE && !release_route_changed) {
		ClusterResId resid;
		ClusterGrdShared *shared;
		bool found;

		memcpy(&resid, master_request.resid, sizeof(resid));
		shared = retained_grd_shmem("pgrac cluster grd", sizeof(*shared), &found);
		HW_CHECK(found);
		pg_atomic_write_u32(&shared->master[cluster_grd_shard_for_resource(&resid)], 1);
		release_route_changed = true;
	}
	return !cooperative_case || cf_case;
}
ClusterAuthorityReadiness
cluster_authority_readiness_get(void)
{
	return CLUSTER_AUTHORITY_SERVING_READY;
}
bool
cluster_recovery_transport_is_current(void)
{
	if (cooperative_case)
		return true; /* Exact accepted recovery cut is a formation fixture. */
	abort();
}
bool
cluster_recovery_authority_is_current(void)
{
	if (cooperative_case)
		return false; /* Reconstruction does not reopen ordinary acquisitions. */
	abort();
}
bool
cluster_configuration_read_transport_is_current(const ClusterResId *resid pg_attribute_unused(),
												LOCKMODE mode pg_attribute_unused())
{
	return false; /* An HW reconstruction is never the initial configuration mount. */
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
	if (cooperative_case && cf_case)
		return cluster_grd_control_recovery_ready(r, m);
	(void)r;
	(void)m;
	(void)startup;
	abort();
}
bool
cluster_ges_dedup_remove_completed(const ClusterGesDedupKey *key)
{
	HW_CHECK(key->request_id == 201 || (hw_local_case && key->request_id == 202));
	return true; /* Dedup storage is a fixture, not the grant authority. */
}
void
cluster_ges_dedup_record_reply(const ClusterGesDedupKey *key, const uint8 *reply, uint16 length)
{
	HW_CHECK(key->request_id == 201 || key->request_id == 203);
	HW_CHECK(length == sizeof(GesReplyPayload));
	HW_CHECK(
		((const GesReplyPayload *)reply)->opcode == GES_REPLY_OPCODE_GRANT
		|| (queued_cut_case && ((const GesReplyPayload *)reply)->opcode == GES_REPLY_OPCODE_REJECT)
		|| (cooperative_case && key->opcode == GES_REQ_OPCODE_REQUEST
			&& ((const GesReplyPayload *)reply)->opcode == GES_REPLY_OPCODE_REJECT
			&& ((const GesReplyPayload *)reply)->reject_reason == GES_REJECT_REASON_WORK_QUEUE_FULL)
		|| (cooperative_case && key->opcode == GES_REQ_OPCODE_REDECLARE
			&& ((const GesReplyPayload *)reply)->opcode == GES_REPLY_OPCODE_REJECT
			&& (((const GesReplyPayload *)reply)->reject_reason == GES_REJECT_REASON_EPOCH_MISMATCH
				|| ((const GesReplyPayload *)reply)->reject_reason
					   == GES_REJECT_REASON_MASTER_DEAD_NATIVE))
		|| ((fault == HW_REJECT || fault == HW_NON_GRANT_NONE)
			&& ((const GesReplyPayload *)reply)->opcode == GES_REPLY_OPCODE_REJECT
			&& ((const GesReplyPayload *)reply)->reject_reason == GES_REJECT_REASON_SHARD_FROZEN));
}
bool
cluster_lms_native_probe_required(const ClusterResId *r, LOCKMODE mode)
{
	/* Mirror the production predicate; PG-native locks are explicitly empty
	 * in this offline fixture, but a relation ShareLock still needs a probe. */
	HW_CHECK(r != NULL);
	return mode >= ShareUpdateExclusiveLock
		   && (r->type == LOCKTAG_RELATION || r->type == LOCKTAG_OBJECT);
}
bool
cluster_lms_native_probe_schedule_grant(const ClusterResId *r, LOCKMODE mode,
										const ClusterGrdHolderId *holder, int32 source,
										uint32 opcode, uint64 generation,
										uint64 receiver_generation, LOCKMODE old)
{
	GesReplyPayload reply;
	LOCKMODE actual = NoLock;

	/* Adjacent native-probe seam: no PG-native locks exist in either child.
	 * The authoritative grant itself must already exist in real GRD. */
	HW_CHECK(relation_case && r->type == LOCKTAG_RELATION && old == NoLock);
	HW_CHECK(cluster_grd_holder_mode_by_id(r, holder, &actual) && actual == mode);
	HW_CHECK(generation == 9);
	HW_CHECK(receiver_generation == master_queued_generation);
	memset(&reply, 0, sizeof(reply));
	reply.opcode = GES_REPLY_OPCODE_GRANT;
	reply.reply_for_opcode = opcode;
	reply.holder_node_id = holder->node_id;
	reply.holder_procno = holder->procno;
	reply.holder_cluster_epoch_lo = (uint32)holder->cluster_epoch;
	reply.holder_cluster_epoch_hi = (uint32)(holder->cluster_epoch >> 32);
	reply.holder_request_id_lo = (uint32)holder->request_id;
	reply.holder_request_id_hi = (uint32)(holder->request_id >> 32);
	memcpy(reply.resid, r, sizeof(*r));
	cluster_grd_outbound_enqueue_lmon_reply(source, &reply, sizeof(reply));
	return true;
}
bool
cluster_lms_native_probe_wait_clear(const ClusterResId *r, LOCKMODE mode,
									const ClusterGrdHolderId *holder, int ms)
{
	HW_CHECK(relation_case && r->type == LOCKTAG_RELATION && mode == ShareLock);
	HW_CHECK(holder->node_id == (uint32)cluster_node_id && ms == 0);
	local_native_probes++;
	return true; /* Explicitly empty PG-native tables; no GRD verdict is stubbed. */
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

static void
stage_master_work(void)
{
	master_queued_generation = cluster_lms_get_shard_master_generation();
	master_work_pending = true;
}

/* Keep the singleton's canonical bytes. Formation is already a fixture;
 * choose node3 as its master in both independently owned GRD address spaces. */
static void
fixture_cf_master(const ClusterResId *resid)
{
	bool found;
	ClusterGrdShared *shared;

	if (!cf_case)
		return;
	shared = retained_grd_shmem("pgrac cluster grd", sizeof(*shared), &found);
	HW_CHECK(found && resid->type == CLUSTER_CF_RESID_TYPE);
	pg_atomic_write_u32(&shared->master[cluster_grd_shard_for_resource(resid)], 3);
}

bool
cluster_grd_work_queue_dequeue(ClusterGrdWorkItem *out)
{
	if (!master_work_pending)
		return false;
	memset(out, 0, sizeof(*out));
	out->routing_generation = master_queued_generation;
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
	if (((const GesReplyPayload *)payload)->reply_for_opcode == GES_REQ_OPCODE_RELEASE)
		memcpy(&master_release_reply, payload, length);
	master_reply_count++;
}

void
cluster_grd_outbound_enqueue_cleanup_release(uint32 destination, const void *payload, uint16 length)
{
	char command = 'R';
	if ((relation_case || cf_case || hw_local_case) && master_child < 0 && destination == (uint32)cluster_node_id) {
		HW_CHECK(length == sizeof(local_cleanup_release));
		HW_CHECK(((const GesRequestPayload *)payload)->opcode == GES_REQ_OPCODE_RELEASE);
		HW_CHECK(((const GesRequestPayload *)payload)->holder_request_id_lo == 201
			|| (hw_local_case && ((const GesRequestPayload *)payload)->holder_request_id_lo == 202));
		local_cleanup_release = *(const GesRequestPayload *)payload;
		cleanup_sent++;
		return; /* Keep it owned until the test drives the real local drain. */
	}
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
	fixture_cf_master(&resid);
	memset(&holder, 0, sizeof(holder));
	holder.node_id = master_request.holder_node_id;
	holder.procno = master_request.holder_procno;
	holder.cluster_epoch = master_request.holder_cluster_epoch_lo;
	holder.request_id = master_request.holder_request_id_lo;
	HW_CHECK(cluster_grd_lookup_master(&resid) == (int32)destination);
	if (fault == HW_REJECT || fault == HW_NON_GRANT_NONE)
		cluster_grd_shard_set_phase(cluster_grd_shard_for_resource(&resid), GRD_SHARD_FROZEN);
	stage_master_work();
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
	if (packet.held_mode != NoLock) {
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
		if (command == 'R' || command == 'A') {
			transfer_exact(requester_to_master[0], &master_request, sizeof(master_request), false);
			HW_CHECK(master_request.opcode == GES_REQ_OPCODE_RELEASE);
			master_reply_count = 0;
			stage_master_work();
			HW_CHECK(cluster_ges_lmon_drain_work_queue() == 1);
			if (command == 'A') {
				packet.reply = master_release_reply;
				packet.replies = master_reply_count;
				transfer_exact(master_to_requester[1], &packet, sizeof(packet), true);
			}
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
	if (release_case) {
		char command = 'A';
		ClusterResId resid;
		bool found;
		ClusterGrdShared *shared;

		HW_CHECK(master_child > 0 && destination == 3);
		HW_CHECK(((const GesRequestPayload *)payload)->opcode == GES_REQ_OPCODE_RELEASE);
		if (release_fault == RELEASE_ENQUEUE_REFUSED)
			return false;
		transfer_exact(requester_to_master[1], &command, 1, true);
		transfer_exact(requester_to_master[1], (void *)payload, length, true);
		transfer_exact(master_to_requester[0], &packet, sizeof(packet), false);
		HW_CHECK(packet.replies > 0 && packet.reply.opcode == GES_REPLY_OPCODE_GRANT);
		HW_CHECK(packet.reply.reply_for_opcode == GES_REQ_OPCODE_RELEASE);
		if (release_fault == RELEASE_ERROR_BEFORE_ACK)
			ereport(ERROR, (errmsg("release accepted before lost caller")));
		if (release_fault == RELEASE_DROP_ACK)
			return true;
		memset(&env, 0, sizeof(env));
		env.source_node_id = destination;
		env.epoch = 1;
		if (release_fault == RELEASE_REJECT_NONE)
			packet.reply.opcode = GES_REPLY_OPCODE_REJECT;
		cluster_ges_reply_handler(&env, &packet.reply);
		if (release_fault == RELEASE_ERROR_AFTER_ACK)
			ereport(ERROR, (errmsg("release reply before caller unwind")));
		if (release_fault == RELEASE_POST_EPOCH)
			ut_mock_epoch++;
		if (release_fault == RELEASE_ORIGIN_RESTART)
			mock_lms_shard_master_generation++;
		if (release_fault == RELEASE_POST_MASTER) {
			memcpy(&resid, ((const GesRequestPayload *)payload)->resid, sizeof(resid));
			shared = retained_grd_shmem("pgrac cluster grd", sizeof(*shared), &found);
			HW_CHECK(found);
			pg_atomic_write_u32(&shared->master[cluster_grd_shard_for_resource(&resid)], 1);
		}
		return true;
	}
	if (cooperative_case) {
		if (cf_case
			&& ((const GesRequestPayload *)payload)->opcode == GES_REQ_OPCODE_REDECLARE_DONE)
			return retained_grd_enqueue(destination, payload, length);
		if (cf_case && ((const GesRequestPayload *)payload)->opcode == GES_REQ_OPCODE_BAST) {
			HW_CHECK(destination == 2);
			return true; /* Conflict holder notification, not a grant. */
		}
		HW_CHECK(destination == 3);
		HW_CHECK(((const GesRequestPayload *)payload)->opcode
				 == (cf_case ? GES_REQ_OPCODE_REQUEST : GES_REQ_OPCODE_REDECLARE));
		if (request_sent > 0)
			HW_CHECK(memcmp(&master_request, payload, length) == 0);
		memcpy(&master_request, payload, length);
		request_sent++;
		return true; /* Only queue: the owner must yield before this work runs. */
	}
	if (fault == HW_FIRST_ENQUEUE_REFUSED)
		return false;
	if (request_sent > 0) {
		HW_CHECK(memcmp(&master_request, payload, length) == 0);
		request_sent++;
		return fault != HW_RETRANSMIT_REFUSED;
	}
	memcpy(&master_request, payload, length);
	HW_CHECK(master_request.opcode
			 == (relation_nowait_case ? GES_REQ_OPCODE_REQUEST_NOWAIT : GES_REQ_OPCODE_REQUEST));
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
		HW_CHECK(packet.held_mode
					 == (cf_case ? cf_mode : (relation_case ? ShareLock : ExclusiveLock))
				 && packet.reply.opcode == GES_REPLY_OPCODE_GRANT
				 && packet.reply.reject_reason == GES_REJECT_REASON_NONE);
	memset(&env, 0, sizeof(env));
	env.source_node_id = destination;
	env.epoch = 1;
	memset(&key, 0, sizeof(key));
	key.request_id = master_request.holder_request_id_lo;
	key.source_node_id = cluster_node_id;
	key.dest_node_id = destination;
	key.request_opcode = master_request.opcode;
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
	hw_local_case = false;
	hw_error_after_wait_release = false;
	hw_release_on_wait = NULL;
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
	if (relation_case) {
		req->resid.type = LOCKTAG_RELATION;
		/* Fixed deterministic search for a resource mastered by node3. */
		while (cluster_grd_lookup_master(&req->resid) != 3)
			req->resid.field2++;
		req->locktag.locktag_type = LOCKTAG_RELATION;
	}
	if (cf_case) {
		memset(&req->resid, 0, sizeof(req->resid));
		req->resid.type = CLUSTER_CF_RESID_TYPE;
		req->resid.lockmethodid = DEFAULT_LOCKMETHOD;
		fixture_cf_master(&req->resid);
	}
	HW_CHECK(cluster_grd_lookup_master(&req->resid) == 3);
	req->op = CLUSTER_LOCK_OP_REQUEST;
	req->dontwait = relation_nowait_case;
	req->lockmode = cf_case ? cf_mode : (relation_case ? ShareLock : ExclusiveLock);
	req->timeout_ms = 5000;
	req->holder = grd_lifecycle_holder(1, 21, 201);
	req->holder.cluster_epoch = 1;
	req->request_id = req->holder.request_id;
	other = grd_lifecycle_holder(1, 22, 202);
	other.cluster_epoch = 1;
	UT_ASSERT_EQ(cluster_grd_try_reserve(&req->resid, &req->holder, req->lockmode, 1, &fast,
										 &req->master_gen_snapshot),
				 CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT(!fast);
	if (sibling)
		UT_ASSERT_EQ(
			cluster_grd_try_reserve(&req->resid, &other, req->lockmode, 1, NULL, &generation),
			CLUSTER_GRD_ENTRY_OK);
	cleanup_sent = request_sent = invalid_replies_rejected = cancel_sent = 0;
	local_native_probes = 0;
	memset(&local_cleanup_release, 0, sizeof(local_cleanup_release));
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
		UT_ASSERT_EQ(mode, cf_case ? cf_mode : (relation_case ? ShareLock : ExclusiveLock));
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
		UT_ASSERT_EQ(post.held_mode,
					 selected == HW_NORMAL
						 ? (cf_case ? cf_mode : (relation_case ? ShareLock : ExclusiveLock))
						 : NoLock);
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
UT_TEST(relation_share_grant_survives_compatible_sibling)
{
	relation_case = true;
	run_case(true, HW_NORMAL);
	relation_case = false;
}
UT_TEST(relation_remote_failure_ownership_matrix)
{
	int selected;

	relation_case = true;
	for (selected = HW_CANCEL_RESERVATION; selected <= HW_IDENTITY_MISMATCH; selected++)
		run_case(true, (HandoffFault)selected);
	relation_case = false;
}
UT_TEST(relation_remote_nowait_grant_and_cleanup)
{
	const HandoffFault cases[]
		= { HW_NORMAL,		   HW_CANCEL_RESERVATION, HW_PRE_EPOCH, HW_IDENTITY_MISMATCH,
			HW_S4_ERROR_READY, HW_NON_GRANT_NONE,	  HW_REJECT };
	unsigned int i;

	relation_case = relation_nowait_case = true;
	for (i = 0; i < sizeof(cases) / sizeof(cases[0]); i++)
		run_case(true, cases[i]);
	relation_case = relation_nowait_case = false;
}

static void
run_local_relation_case(bool abandon, bool nowait_conflict)
{
	ClusterLockAcquireRequest req;
	ClusterLockOwner owner = { 0 };
	ClusterGrdHolderId successor;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nconflicts = 0;
	LOCKMODE mode = NoLock;

	relation_case = !cf_case;
	fault = HW_NORMAL;
	setup_case(&req, false);
	UT_ASSERT_EQ(cluster_grd_cancel_reservation_by_id(&req.resid, &req.holder),
				 CLUSTER_GRD_ENTRY_OK);
	cluster_node_id = 3;
	MyProc->pgprocno = 21;
	req.dontwait = nowait_conflict;
	UT_ASSERT_EQ(cluster_lock_acquire_s3_partition_reservation(&req),
				 CLUSTER_LOCK_ACQUIRE_OK_GRANTED); /* Dispatch S4, not the old shortcut. */
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&req.resid, &req.holder, &mode));
	successor = grd_lifecycle_holder(2, 23, 203);
	if (nowait_conflict)
		UT_ASSERT_EQ(cluster_grd_entry_enqueue_or_grant(&req.resid, &successor, 2, 203, 9,
														GES_REQ_OPCODE_REQUEST, ExclusiveLock,
														conflicts, &nconflicts),
					 CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ(cluster_lock_acquire_s4_remote_request_wait(&req),
				 nowait_conflict ? CLUSTER_LOCK_ACQUIRE_NOT_AVAIL
								 : CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK);
	UT_ASSERT_EQ(local_native_probes, cf_case ? 0 : 1);
	UT_ASSERT_EQ(request_sent, 0);
	if (nowait_conflict) {
		(void)cluster_lock_acquire_s7_cleanup(&req);
		UT_ASSERT_EQ(cleanup_sent, 0);
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&req.resid, &req.holder, &mode));
		UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&req.resid, &successor),
					 GES_REJECT_REASON_NONE);
	} else if (abandon) {
		UT_ASSERT_EQ(cluster_grd_entry_enqueue_or_grant(&req.resid, &successor, 2, 203, 9,
														GES_REQ_OPCODE_REQUEST, ExclusiveLock,
														conflicts, &nconflicts),
					 CLUSTER_GRD_ENQUEUED_WAITER);
		(void)cluster_lock_acquire_s7_cleanup(&req);
		(void)cluster_lock_acquire_s7_cleanup(&req);
		UT_ASSERT_EQ(cleanup_sent, 1);
		UT_ASSERT(cluster_grd_holder_mode_by_id(&req.resid, &req.holder, &mode));
		UT_ASSERT_EQ(mode, cf_case ? cf_mode : ShareLock); /* Still owned until its drain. */
		UT_ASSERT_EQ(local_cleanup_release.holder_node_id, req.holder.node_id);
		master_request = local_cleanup_release;
		stage_master_work();
		UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&req.resid, &req.holder, &mode));
		UT_ASSERT(cluster_grd_holder_mode_by_id(&req.resid, &successor, &mode));
		UT_ASSERT_EQ(mode, ExclusiveLock);
		UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&req.resid, &successor),
					 GES_REJECT_REASON_NONE);
	} else {
		if (cf_case) {
			uint64 enumerated = 0;

			owner.request = req;
			UT_ASSERT(cluster_lock_owner_install(&owner));
			UT_ASSERT(cluster_lock_owner_is_usable(&owner));
			UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
			UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
			UT_ASSERT_EQ(enumerated, 1);
			req = owner.request;
		} else
			UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
		UT_ASSERT(req.hw_grant.consumed && !req.hw_grant.cleanup_pending);
		UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
		(void)cluster_lock_acquire_s7_cleanup(&req);
		UT_ASSERT_EQ(cleanup_sent, 0);
		if (cf_case) {
			UT_ASSERT(cluster_lock_owner_release(&owner));
			UT_ASSERT(!cluster_lock_owner_is_usable(&owner));
			UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
		} else
			UT_ASSERT_EQ(cluster_lock_acquire_s6_release(&req), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	}
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
	MyProc = NULL;
	relation_case = false;
}

UT_TEST(relation_local_authoritative_grant)
{
	run_local_relation_case(false, false);
}
UT_TEST(relation_local_backout_drains_successor)
{
	run_local_relation_case(true, false);
}
UT_TEST(relation_local_nowait_keeps_conflict_semantics)
{
	run_local_relation_case(false, true);
}

/* A scalar CF reply used to lose its original master/key on every S4/S5
 * failure. These cases execute both actual GRDs, including successor drain. */
UT_TEST(cf_remote_grant_and_exact_cleanup)
{
	int mode;
	int selected;

	cf_case = true;
	for (mode = 0; mode < 2; mode++) {
		cf_mode = mode == 0 ? ShareLock : ExclusiveLock;
		for (selected = HW_NORMAL; selected <= HW_IDENTITY_MISMATCH; selected++)
			run_case(true, (HandoffFault)selected);
	}
	cf_case = false;
}

UT_TEST(cf_local_grant_and_owned_backout)
{
	int mode;

	cf_case = true;
	for (mode = 0; mode < 2; mode++) {
		cf_mode = mode == 0 ? ShareLock : ExclusiveLock;
		run_local_relation_case(false, false);
		run_local_relation_case(true, false);
	}
	cf_case = false;
}

/* Break caught: the real RELEASE sender used to report success when a second
 * routing lookup became unknown/local, without touching the actual holder.
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(release_sender_unknown_master_cannot_confirm)
{
	ClusterLockAcquireRequest req;
	ClusterGrdShared *shared;
	bool found;

	cf_case = true;
	cf_mode = ShareLock;
	fault = HW_NORMAL;
	setup_case(&req, false);
	shared = retained_grd_shmem("pgrac cluster grd", sizeof(*shared), &found);
	HW_CHECK(found);
	pg_atomic_write_u32(&shared->master[cluster_grd_shard_for_resource(&req.resid)], UINT32_MAX);
	UT_ASSERT_NE(cluster_ges_send_release_and_wait(&req.resid, &req.holder, req.request_id,
												   req.timeout_ms, 0),
				 GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(request_sent, 0);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
	cf_case = false;
	MyProc = NULL;
}

UT_TEST(release_sender_local_route_really_drains_holder)
{
	ClusterLockAcquireRequest req;
	ClusterGrdHolderId successor;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	LOCKMODE mode = NoLock;
	int nconflicts = 0;

	cf_case = true;
	cf_mode = ShareLock;
	fault = HW_NORMAL;
	setup_case(&req, false);
	UT_ASSERT_EQ(cluster_grd_cancel_reservation_by_id(&req.resid, &req.holder),
				 CLUSTER_GRD_ENTRY_OK);
	cluster_node_id = 3;
	req.holder.node_id = 3;
	UT_ASSERT_EQ(cluster_grd_entry_rebind_or_insert_holder(&req.resid, &req.holder, 3, ShareLock),
				 CLUSTER_GRD_ENTRY_OK);
	successor = grd_lifecycle_holder(2, 23, 203);
	successor.cluster_epoch = 1;
	UT_ASSERT_EQ(cluster_grd_entry_enqueue_or_grant(&req.resid, &successor, 2, 203, 9,
													GES_REQ_OPCODE_REQUEST, ExclusiveLock,
													conflicts, &nconflicts),
				 CLUSTER_GRD_ENQUEUED_WAITER);
	/* The transport fixture records the actual successor GRANT. */
	memset(&master_request, 0, sizeof(master_request));
	master_request.holder_node_id = 3;
	UT_ASSERT_EQ(cluster_ges_send_release_and_wait(&req.resid, &req.holder, req.request_id,
												   req.timeout_ms, 0),
				 GES_REJECT_REASON_NONE);
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&req.resid, &req.holder, &mode));
	UT_ASSERT(cluster_grd_holder_mode_by_id(&req.resid, &successor, &mode));
	UT_ASSERT_EQ(mode, ExclusiveLock);
	UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&req.resid, &successor),
				 GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);
	UT_ASSERT_EQ(request_sent, 0);
	cf_case = false;
	MyProc = NULL;
}

/* The original diagnostic RED proved RELEASE alone is not whole-request
 * termination. Exercise the new retirement primitive after the real ordinary
 * sender; the old RELEASE contract itself remains holder-only.
 * Real sender and GRD run locally; the stable CF owner/service composition
 * is exercised separately, not replaced by this primitive test.
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(control_retirement_clears_the_same_queued_request)
{
	ClusterLockAcquireRequest req;
	ClusterGrdHolderId blocker;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	ClusterGrdEntryResult remaining;
	uint32 result;
	int nconflicts = 0;

	cf_case = true;
	cf_mode = ShareLock;
	fault = HW_NORMAL;
	setup_case(&req, false);
	HW_CHECK(cluster_grd_cancel_reservation_by_id(&req.resid, &req.holder) == CLUSTER_GRD_ENTRY_OK);
	cluster_node_id = 3;
	req.holder.node_id = 3;
	blocker = grd_lifecycle_holder(2, 23, 203);
	blocker.cluster_epoch = 1;
	HW_CHECK(cluster_grd_entry_rebind_or_insert_holder(&req.resid, &blocker, 2, ExclusiveLock)
			 == CLUSTER_GRD_ENTRY_OK);
	HW_CHECK(cluster_grd_entry_enqueue_or_grant(&req.resid, &req.holder, 3, req.request_id, 9,
												GES_REQ_OPCODE_REQUEST, req.lockmode, conflicts,
												&nconflicts)
			 == CLUSTER_GRD_ENQUEUED_WAITER);

	result = cluster_ges_send_release_and_wait(&req.resid, &req.holder, req.request_id,
											   req.timeout_ms, 0);
	UT_ASSERT_EQ(result, GES_REJECT_REASON_NONE);
	UT_ASSERT(cluster_grd_retire_request_and_drain(&req.resid, &req.holder, 0, NoLock, NULL, 0)
			  >= 0);
	/* Exact dequeue both observes and safely tears down the test's remaining
	 * waiter before the assertion; no queue state leaks into another test. */
	remaining = cluster_grd_cancel_waiter_by_id(&req.resid, &req.holder);
	HW_CHECK(cluster_ges_release_and_drain_local(&req.resid, &blocker) == GES_REJECT_REASON_NONE);
	cf_case = false;
	MyProc = NULL;
	printf("# abandoned CF release reason=%u remaining_waiter=%d\n", result, remaining);
	UT_ASSERT_EQ(remaining, CLUSTER_GRD_ENTRY_NOT_FOUND);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);
}

/* Actual remote master removal/ACK and reply-table ownership. A successful
 * removal raced by a new cut is not confirmation for the old caller; retry
 * can confirm absence only after restoring this fixture's exact original cut. */
UT_TEST(release_sender_confirms_exact_verdict_and_unwinds_waiter)
{
	int mode_index;
	int selected;

	cf_case = true;
	for (mode_index = 0; mode_index < 2; mode_index++) {
		cf_mode = mode_index == 0 ? ShareLock : ExclusiveLock;
		for (selected = RELEASE_NORMAL; selected <= RELEASE_ENTRY_ROUTE; selected++) {
			ClusterLockAcquireRequest req;
			MasterPacket post;
			volatile uint32 result = GES_REJECT_REASON_TIMEOUT;
			volatile bool caught = false;
			bool error
				= selected == RELEASE_ERROR_BEFORE_ACK || selected == RELEASE_ERROR_AFTER_ACK;
			bool success = selected == RELEASE_NORMAL || selected == RELEASE_ORIGIN_RESTART;
			int status;
			char command = 'Q';

			fault = HW_NORMAL;
			setup_case(&req, false);
			UT_ASSERT_EQ(cluster_lock_acquire_s4_remote_request_wait(&req),
						 CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK);
			UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
			release_case = true;
			release_fault = (ReleaseFault)selected;
			release_route_changed = false;
			printf("# release mode=%d fault=%d\n", cf_mode, selected);
			PG_TRY();
			{
				result = cluster_lock_acquire_s6_release(&req);
			}
			PG_CATCH();
			{
				caught = true;
				FlushErrorState();
			}
			PG_END_TRY();
			UT_ASSERT_EQ(caught, error);
			if (!caught)
				UT_ASSERT_EQ(result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED, success);
			UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
			ut_mock_epoch = 1;
			mock_lms_shard_master_generation = 9;
			fixture_cf_master(&req.resid);
			release_fault = RELEASE_NORMAL;
			UT_ASSERT_EQ(cluster_lock_acquire_s6_release(&req), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
			UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
			transfer_exact(requester_to_master[1], &command, 1, true);
			transfer_exact(master_to_requester[0], &post, sizeof(post), false);
			close(requester_to_master[1]);
			close(master_to_requester[0]);
			HW_CHECK(waitpid(master_child, &status, 0) == master_child && WIFEXITED(status)
					 && WEXITSTATUS(status) == 0);
			master_child = -1;
			UT_ASSERT_EQ(post.held_mode, NoLock);
			UT_ASSERT_EQ(post.successor_mode, ExclusiveLock);
			release_case = false;
			MyProc = NULL;
		}
	}
	cf_case = false;
}

UT_TEST(release_sender_keeps_foreign_waiter_and_rejects_bad_identity)
{
	ClusterLockAcquireRequest req;
	GesReplyWaitKey key = { 0 };
	GesReplyWaitEntry *entry;
	int invalid;

	cf_case = true;
	cf_mode = ShareLock;
	fault = HW_NORMAL;
	for (invalid = 0; invalid < 4; invalid++) {
		setup_case(&req, false);
		if (invalid == 0)
			req.request_id = 0;
		else if (invalid == 1)
			req.request_id++;
		else if (invalid == 2)
			req.holder.node_id++;
		else
			req.holder.cluster_epoch++;
		UT_ASSERT_NE(cluster_ges_send_release_and_wait(&req.resid, &req.holder, req.request_id,
													   req.timeout_ms, 0),
					 GES_REJECT_REASON_NONE);
		UT_ASSERT_EQ(request_sent, 0);
		UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
	}
	setup_case(&req, false);
	key.request_id = req.request_id;
	key.source_node_id = 1;
	key.dest_node_id = 3;
	key.request_opcode = GES_REQ_OPCODE_RELEASE;
	key.cluster_epoch = 1;
	entry = cluster_ges_reply_wait_insert(&key, 0);
	UT_ASSERT(entry != NULL);
	UT_ASSERT_NE(cluster_ges_send_release_and_wait(&req.resid, &req.holder, req.request_id,
												   req.timeout_ms, 0),
				 GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(request_sent, 0);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 1);
	UT_ASSERT(cluster_ges_reply_wait_lookup(&key) == entry && !entry->ready);
	cluster_ges_reply_wait_delete(&key);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
	cf_case = false;
	MyProc = NULL;
}

/* No malformed singleton, mismatched caller, or already-owned carrier may
 * dispatch a request or overwrite the responsibility for an earlier one. */
UT_TEST(cf_request_refuses_invalid_or_owned_provenance)
{
	int invalid;

	cf_case = true;
	cf_mode = ShareLock;
	fault = HW_NORMAL;
	for (invalid = 0; invalid < 21; invalid++) {
		ClusterLockAcquireRequest req, original;
		ClusterGesHwGrant before;

		setup_case(&req, false);
		original = req;
		switch (invalid) {
		case 0:
			req.resid.type = CLUSTER_HW_RESID_TYPE;
			break;
		case 1:
			req.resid.field1 = 1;
			break;
		case 2:
			req.resid.field2 = 1;
			break;
		case 3:
			req.resid.field3 = 1;
			break;
		case 4:
			req.resid.field4 = 1;
			break;
		case 5:
			req.resid.lockmethodid = USER_LOCKMETHOD;
			break;
		case 6:
			req.lockmode = NoLock;
			break;
		case 7:
			req.lockmode = RowExclusiveLock;
			break;
		case 8:
			req.request_id = 0;
			break;
		case 9:
			req.holder.node_id = 2;
			break;
		case 10:
			req.holder.request_id++;
			break;
		case 11:
			req.holder.cluster_epoch++;
			break;
		case 12:
			req.hw_grant.cleanup_pending = true;
			break;
		case 13:
			req.hw_grant.grant_observed = true;
			break;
		case 14:
			req.hw_grant.local_promoted = true;
			break;
		case 15:
			req.hw_grant.consumed = true;
			break;
		case 16:
			req.hw_grant.key.request_id = 123;
			break;
		case 17:
			req.lockmode = AccessExclusiveLock;
			break;
		default:
			break; /* The final three cases pass a null argument. */
		}
		before = req.hw_grant;
		UT_ASSERT_EQ(cluster_ges_send_cf_request_and_wait(
						 invalid == 18 ? NULL : &req.resid, req.lockmode,
						 invalid == 19 ? NULL : &req.holder, req.request_id, req.timeout_ms, 0,
						 invalid == 20 ? NULL : &req.hw_grant),
					 GES_REJECT_REASON_EPOCH_MISMATCH);
		UT_ASSERT_EQ(memcmp(&before, &req.hw_grant, sizeof(before)), 0);
		UT_ASSERT_EQ(request_sent, 0);
		UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
		(void)cluster_lock_acquire_s7_cleanup(&original);
		UT_ASSERT_EQ(cluster_grd_entry_count(), 0);
		MyProc = NULL;
	}
	cf_case = false;
}

UT_TEST(cf_reservation_without_observed_grant_is_not_authority)
{
	ClusterLockAcquireRequest req;
	LOCKMODE mode = NoLock;
	int requested;

	cf_case = true;
	for (requested = 0; requested < 2; requested++) {
		cf_mode = requested == 0 ? ShareLock : ExclusiveLock;
		setup_case(&req, false);
		UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&req.resid, &req.holder, &mode));
		UT_ASSERT_EQ(cluster_grd_entry_count(), 0);
		UT_ASSERT_EQ(request_sent, 0);
		UT_ASSERT_EQ(cleanup_sent, 0);
		MyProc = NULL;
	}
	cf_case = false;
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

static void
hw_local_competitors(ClusterLockAcquireRequest *a, ClusterLockAcquireRequest *b)
{
	fault = HW_NORMAL;
	setup_case(a, false);
	UT_ASSERT_EQ(cluster_grd_cancel_reservation_by_id(&a->resid, &a->holder),
				 CLUSTER_GRD_ENTRY_OK);
	cluster_node_id = 3;
	MyProc->pgprocno = 21;
	a->holder.node_id = 3;
	memset(&hw_local_proc_header, 0, sizeof(hw_local_proc_header));
	memset(hw_local_procs, 0, sizeof(hw_local_procs));
	hw_local_proc_header.allProcCount = lengthof(hw_local_procs);
	hw_local_proc_header.allProcs = hw_local_procs;
	hw_local_procs[21].pid = getpid();
	hw_local_procs[21].backendId = 1;
	ProcGlobal = &hw_local_proc_header;
	hw_local_case = true;
	hw_release_on_wait = NULL;
	cooperative_sleeps = 0;
	*b = *a;
	b->holder = grd_lifecycle_holder(3, 22, 202);
	b->holder.cluster_epoch = 1;
	b->request_id = 202;
}

static void
hw_dispatch_reserved(ClusterLockAcquireRequest *req, ClusterLockAcquireResult s3)
{
	MyProc->pgprocno = req->holder.procno;
	/* Execute exactly the wrapper's branch; do not force S4 in the test. */
	if (s3 == CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
		UT_ASSERT_EQ(cluster_lock_acquire_s4_remote_request_wait(req),
					 CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK);
	else
		UT_ASSERT_EQ(s3, CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK);
}

UT_TEST(hw_local_two_reservations_survive_exact_grants)
{
	ClusterLockAcquireRequest a, b;
	ClusterLockAcquireResult a3, b3;
	LOCKMODE mode = NoLock;
	hw_local_competitors(&a, &b);
	a3 = cluster_lock_acquire_s3_partition_reservation(&a);
	MyProc->pgprocno = 22;
	b3 = cluster_lock_acquire_s3_partition_reservation(&b);
	hw_dispatch_reserved(&a, a3);
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&a), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	hw_release_on_wait = &a;
	hw_dispatch_reserved(&b, b3);
	UT_ASSERT_EQ(cooperative_sleeps, 1);
	if (hw_release_on_wait != NULL) { /* Original RED's cleanup, not a grant. */
		hw_release_on_wait = NULL;
		UT_ASSERT_EQ(cluster_lock_acquire_s6_release(&a), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	}
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&b), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT(cluster_grd_holder_mode_by_id(&b.resid, &b.holder, &mode));
	UT_ASSERT_EQ(mode, ExclusiveLock);
	UT_ASSERT_EQ(cluster_lock_acquire_s6_release(&b), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);
	UT_ASSERT_EQ(request_sent, 0);
	UT_ASSERT_EQ(ut_mock_epoch, 1);
	MyProc = NULL;
}

UT_TEST(hw_local_release_does_not_invalidate_another_reserved_request)
{
	ClusterLockAcquireRequest a, b;
	ClusterLockAcquireResult a3, b3;
	hw_local_competitors(&a, &b);
	a3 = cluster_lock_acquire_s3_partition_reservation(&a);
	hw_dispatch_reserved(&a, a3);
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&a), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	MyProc->pgprocno = 22;
	b3 = cluster_lock_acquire_s3_partition_reservation(&b);
	/* A can finish native extension and release while B has only reserved
	 * HW, before its authoritative grant and promotion. */
	MyProc->pgprocno = 21;
	UT_ASSERT_EQ(cluster_lock_acquire_s6_release(&a), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	MyProc->pgprocno = 22;
	hw_dispatch_reserved(&b, b3);
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&b), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT_EQ(cluster_lock_acquire_s6_release(&b), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);
	UT_ASSERT_EQ(request_sent, 0);
	MyProc = NULL;
}

UT_TEST(hw_local_cancel_after_grant_keeps_exact_cleanup_owner)
{
	static ClusterLockAcquireRequest a, b; /* survive the ERROR longjmp */
	volatile bool caught = false;
	LOCKMODE mode = NoLock;
	hw_local_competitors(&a, &b);
	hw_dispatch_reserved(&a, cluster_lock_acquire_s3_partition_reservation(&a));
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&a), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	MyProc->pgprocno = 22;
	UT_ASSERT_EQ(cluster_lock_acquire_s3_partition_reservation(&b), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	hw_release_on_wait = &a;
	hw_error_after_wait_release = true;
	PG_TRY();
	{
		(void)cluster_lock_acquire_s4_remote_request_wait(&b);
	}
	PG_CATCH();
	{
		caught = true;
		FlushErrorState();
	}
	PG_END_TRY();
	hw_error_after_wait_release = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
	UT_ASSERT_EQ(cleanup_sent, 1);
	if (cleanup_sent == 1) {
		UT_ASSERT_EQ(local_cleanup_release.holder_request_id_lo, b.request_id);
		UT_ASSERT_EQ(local_cleanup_release.holder_procno, b.holder.procno);
		UT_ASSERT_EQ(memcmp(local_cleanup_release.resid, &b.resid, sizeof(b.resid)), 0);
		master_request = local_cleanup_release;
		stage_master_work();
		UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	}
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&b.resid, &b.holder, &mode));
	UT_ASSERT_EQ(cluster_grd_entry_count(), 0);
	MyProc = NULL;
}

UT_TEST(hw_local_grant_rejects_epoch_change_before_promotion)
{
	ClusterLockAcquireRequest a, b;
	hw_local_competitors(&a, &b);
	hw_dispatch_reserved(&a, cluster_lock_acquire_s3_partition_reservation(&a));
	ut_mock_epoch++;
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&a), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ(cleanup_sent, 1);
	UT_ASSERT(!a.hw_grant.consumed);
	/* The exact original cleanup is owned, never a release of a new epoch. */
	if (cleanup_sent == 1) {
		UT_ASSERT_EQ(local_cleanup_release.holder_cluster_epoch_lo, 1);
		UT_ASSERT_EQ(local_cleanup_release.holder_request_id_lo, a.request_id);
	}
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
	UT_ASSERT_EQ(req.hw_grant.key.request_id, req.request_id);
	UT_ASSERT(req.hw_grant.grant_observed);
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT_EQ(request_sent, 0);
	UT_ASSERT_EQ(cleanup_sent, 0);
	UT_ASSERT_EQ(cluster_lock_acquire_s6_release(&req), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	MyProc = NULL;
}

static int
relation_native_error_callback(void *argument, ClusterLockAcquireRequest *pending)
{
	*pending = *(ClusterLockAcquireRequest *)argument;
	HW_CHECK(cluster_lock_acquire_s4_remote_request_wait(pending)
			 == CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK);
	/* The PG-native/WAL preparation seam raises a real PG ERROR after real
	 * S4.  No LOCALLOCK has consumed the retained grant at this point. */
	ereport(ERROR, (errmsg("injected native preparation error after real relation GRANT")));
	return 0;
}

UT_TEST(relation_native_error_has_full_interval_cleanup_owner)
{
	ClusterLockAcquireRequest req;
	ClusterGrdHolderId successor;
	ClusterGrdConflictHolder conflicts[PGRAC_GRD_MAX_HOLDERS_PUBLIC];
	int nconflicts = 0;
	volatile bool caught = false;
	LOCKMODE mode = NoLock;

	relation_case = true;
	fault = HW_NORMAL;
	setup_case(&req, false);
	UT_ASSERT_EQ(cluster_grd_cancel_reservation_by_id(&req.resid, &req.holder),
				 CLUSTER_GRD_ENTRY_OK);
	cluster_node_id = 3;
	MyProc->pgprocno = 21;
	UT_ASSERT_EQ(cluster_lock_acquire_s3_partition_reservation(&req),
				 CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	PG_TRY();
	{
		/* Explicit standalone RED control for the old unprotected interval;
		 * never enabled by a product binary or normal qualification. */
		if (getenv("PGRAC_A93_TEST_UNPROTECTED_NATIVE") != NULL) {
			ClusterLockAcquireRequest unprotected;
			(void)relation_native_error_callback(&req, &unprotected);
		} else
			(void)cluster_lock_acquire_guarded_native(relation_native_error_callback, &req);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cleanup_sent, 1);
	UT_ASSERT(cluster_grd_holder_mode_by_id(&req.resid, &req.holder, &mode));
	UT_ASSERT_EQ(mode, ShareLock);
	successor = grd_lifecycle_holder(2, 23, 203);
	UT_ASSERT_EQ(cluster_grd_entry_enqueue_or_grant(&req.resid, &successor, 2, 203, 9,
													GES_REQ_OPCODE_REQUEST, ExclusiveLock,
													conflicts, &nconflicts),
				 CLUSTER_GRD_ENQUEUED_WAITER);
	if (cleanup_sent == 1) {
		master_request = local_cleanup_release;
		stage_master_work();
		UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&req.resid, &req.holder, &mode));
		UT_ASSERT(cluster_grd_holder_mode_by_id(&req.resid, &successor, &mode));
		UT_ASSERT_EQ(mode, ExclusiveLock);
		UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&req.resid, &successor),
					 GES_REJECT_REASON_NONE);
		UT_ASSERT_EQ(cluster_grd_entry_count(), 0);
	}
	MyProc = NULL;
	relation_case = false;
}

/* Existing blocking exchange is the RED control: the same service owner
 * cannot drain this work while blocked in its own request. Transport, hash
 * storage and formation are fixtures; rebind, drain and reply handling are not. */
static void
setup_cooperative(ClusterLockAcquireRequest *req)
{
	fault = HW_NORMAL;
	relation_case = relation_nowait_case = false;
	setup_case(req, false);
	HW_CHECK(cluster_grd_cancel_reservation_by_id(&req->resid, &req->holder)
			 == CLUSTER_GRD_ENTRY_OK);
	cooperative_case = true;
	cooperative_sleeps = 0;
	cooperative_drift = 0;
	ut_mock_epoch = req->holder.cluster_epoch = 2;
}

static void
run_redeclare_master(const ClusterLockAcquireRequest *req, int output)
{
	const int32 nodes[] = { 0, 1, 2, 3 };
	ClusterGrdHolderId old = req->holder, ordinary;
	LOCKMODE held = NoLock;
	GesReplyPayload granted;
	MasterPacket packet = { 0 };
	uint32 shard;

	grd_lifecycle_reset(4);
	set_mock_declared(4, nodes);
	cluster_grd_master_map_init();
	cluster_node_id = 3;
	ut_mock_epoch = 2;
	ut_qvotec_quorum = true;
	mock_lms_shard_master_generation = 9;
	MyProc = NULL;
	old.cluster_epoch = 1;
	old.request_id = 200;
	HW_CHECK(cluster_grd_entry_rebind_or_insert_holder(&req->resid, &old, 1, ExclusiveLock)
			 == CLUSTER_GRD_ENTRY_OK);
	shard = cluster_grd_shard_for_resource(&req->resid);
	cluster_grd_shard_set_phase(shard, GRD_SHARD_REBUILDING);
	/* Same-wire replay must rebind idempotently, without opening the shard. */
	for (int i = 0; i < 2; i++) {
		stage_master_work();
		HW_CHECK(cluster_ges_lmon_drain_work_queue() == 1);
		HW_CHECK(master_reply.opcode == GES_REPLY_OPCODE_GRANT);
		HW_CHECK(master_reply.reply_for_opcode == GES_REQ_OPCODE_REDECLARE);
		HW_CHECK(cluster_grd_holder_mode_by_id(&req->resid, &req->holder, &held));
		HW_CHECK(held == ExclusiveLock);
		HW_CHECK(!cluster_grd_holder_mode_by_id(&req->resid, &old, &held));
		HW_CHECK(cluster_grd_shard_phase(shard) == GRD_SHARD_REBUILDING);
	}
	granted = master_reply;
	if (cooperative_drift != 0) {
		ClusterGrdRecoveryCounters before, after;

		cluster_grd_recovery_counters_snapshot(&before);
		stage_master_work();
		if (cooperative_drift == 1)
			ut_mock_epoch = 3;
		else if (cooperative_drift == 2)
			mock_lms_shard_master_generation = 10;
		else
			cluster_node_id = 0; /* Queue handed to a process no longer master. */
		HW_CHECK(cluster_ges_lmon_drain_work_queue() == 1);
		packet.reply = master_reply;
		cluster_grd_recovery_counters_snapshot(&after);
		packet.replies = (int)(after.holders_rebound - before.holders_rebound);
		packet.held_mode
			= cluster_grd_holder_mode_by_id(&req->resid, &req->holder, &held) ? held : NoLock;
		transfer_exact(output, &packet, sizeof(packet), true);
		_exit(0);
	}
	master_request.opcode = GES_REQ_OPCODE_REQUEST;
	master_request.holder_node_id = 2;
	master_request.holder_procno = 23;
	master_request.holder_request_id_lo = 203;
	stage_master_work();
	HW_CHECK(cluster_ges_lmon_drain_work_queue() == 1);
	HW_CHECK(master_reply.opcode == GES_REPLY_OPCODE_REJECT);
	HW_CHECK(master_reply.reject_reason == GES_REJECT_REASON_WORK_QUEUE_FULL);
	ordinary = grd_lifecycle_holder(2, 23, 203);
	ordinary.cluster_epoch = 2;
	HW_CHECK(!cluster_grd_holder_mode_by_id(&req->resid, &ordinary, &held));
	packet.reply = granted;
	transfer_exact(output, &packet, sizeof(packet), true);
	_exit(0);
}

static GesReplyPayload
drive_redeclare_master(const ClusterLockAcquireRequest *req, MasterPacket *observation)
{
	int channel[2], status;
	pid_t child;
	MasterPacket packet;

	HW_CHECK(pipe(channel) == 0);
	child = fork();
	HW_CHECK(child >= 0);
	if (child == 0) {
		close(channel[0]);
		alarm(10);
		run_redeclare_master(req, channel[1]);
	}
	close(channel[1]);
	transfer_exact(channel[0], &packet, sizeof(packet), false);
	close(channel[0]);
	HW_CHECK(waitpid(child, &status, 0) == child);
	HW_CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
	if (observation != NULL)
		*observation = packet;
	return packet.reply;
}

UT_TEST(cooperative_redeclare_yields_then_consumes_real_grant)
{
	ClusterLockAcquireRequest req;
	ClusterGesRedeclareAttempt attempt = { 0 };
	ClusterICEnvelope env = { 0 };
	GesReplyPayload reply;
	ClusterGesRedeclareResult result;

	setup_cooperative(&req);
	if (getenv("PGRAC_PRE2_TEST_BLOCKING_REDECLARE") != NULL) {
		uint32 reason = cluster_ges_send_redeclare_and_wait(&req.resid, req.lockmode, &req.holder,
															req.holder.request_id);
		result = reason == 0 ? CLUSTER_GES_REDECLARE_CONFIRMED : CLUSTER_GES_REDECLARE_REJECTED;
	} else
		result = cluster_ges_redeclare_poll(&attempt, &req.resid, req.lockmode, &req.holder);
	UT_ASSERT_EQ(result, CLUSTER_GES_REDECLARE_PENDING);
	UT_ASSERT_EQ(cooperative_sleeps, 0);
	if (result != CLUSTER_GES_REDECLARE_PENDING) {
		cooperative_case = false;
		return;
	}
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 1);
	UT_ASSERT_EQ(request_sent, 1);
	ut_mock_now += 100000;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &req.resid, req.lockmode, &req.holder),
				 CLUSTER_GES_REDECLARE_PENDING);
	UT_ASSERT_EQ(request_sent, 2);
	reply = drive_redeclare_master(&req, NULL);
	env.source_node_id = 3;
	env.epoch = 2;
	cluster_ges_reply_handler(&env, &reply);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &req.resid, req.lockmode, &req.holder),
				 CLUSTER_GES_REDECLARE_CONFIRMED);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
	UT_ASSERT_EQ(cooperative_sleeps, 0);
	UT_ASSERT_EQ(request_sent, 2);
	cooperative_case = false;
}

UT_TEST(cooperative_redeclare_late_cut_cannot_publish_ack)
{
	ClusterLockAcquireRequest req;
	ClusterGesRedeclareAttempt attempt = { 0 };
	ClusterICEnvelope env = { 0 };
	GesReplyPayload reply;

	setup_cooperative(&req);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &req.resid, req.lockmode, &req.holder),
				 CLUSTER_GES_REDECLARE_PENDING);
	reply = drive_redeclare_master(&req, NULL);
	ut_mock_epoch = 3;
	env.source_node_id = 3;
	env.epoch = 2;
	cluster_ges_reply_handler(&env, &reply);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &req.resid, req.lockmode, &req.holder),
				 CLUSTER_GES_REDECLARE_CUT_CHANGED);
	UT_ASSERT(!attempt.confirmed);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 1);
	UT_ASSERT_EQ(cooperative_sleeps, 0);
	cooperative_case = false;
}

UT_TEST(cooperative_remote_exchange_survives_origin_counter_change)
{
	ClusterLockAcquireRequest req;
	ClusterGesRedeclareAttempt attempt = { 0 };
	ClusterICEnvelope env = { 0 };
	GesReplyPayload reply;

	setup_cooperative(&req);
	mock_lms_shard_master_generation = 47;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &req.resid, req.lockmode, &req.holder),
				 CLUSTER_GES_REDECLARE_PENDING);
	UT_ASSERT_EQ(attempt.master_generation, 47);
	/* The real remote master uses its independent receiver9 cut. */
	reply = drive_redeclare_master(&req, NULL);
	mock_lms_shard_master_generation = 48;
	env.source_node_id = 3;
	env.epoch = 2;
	cluster_ges_reply_handler(&env, &reply);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &req.resid, req.lockmode, &req.holder),
				 CLUSTER_GES_REDECLARE_CONFIRMED);
	UT_ASSERT_EQ(attempt.key.request_id, 201);
	UT_ASSERT_EQ(attempt.master_generation, 47);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
	UT_ASSERT_EQ(cooperative_sleeps, 0);
	UT_ASSERT_EQ(request_sent, 1);
	cooperative_case = false;
}

UT_TEST(cooperative_redeclare_malformed_verdict_cannot_restart)
{
	ClusterLockAcquireRequest req;
	ClusterGesRedeclareAttempt attempt = { 0 };
	ClusterICEnvelope env = { 0 };
	GesReplyPayload reply;

	setup_cooperative(&req);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &req.resid, req.lockmode, &req.holder),
				 CLUSTER_GES_REDECLARE_PENDING);
	reply = drive_redeclare_master(&req, NULL);
	/* The real wire handler permits this shape; it is not a usable verdict.
	 * Keep the potentially installed holder's identity, without retrying an
	 * internally contradictory result into a later apparent success. */
	reply.opcode = GES_REPLY_OPCODE_REJECT;
	reply.reject_reason = GES_REJECT_REASON_NONE;
	env.source_node_id = 3;
	env.epoch = 2;
	cluster_ges_reply_handler(&env, &reply);
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &req.resid, req.lockmode, &req.holder),
				 CLUSTER_GES_REDECLARE_INVALID);
	ut_mock_now += 200000;
	UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &req.resid, req.lockmode, &req.holder),
				 CLUSTER_GES_REDECLARE_INVALID);
	UT_ASSERT(attempt.sent && !attempt.confirmed);
	UT_ASSERT_EQ(attempt.key.request_id, 201);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
	UT_ASSERT_EQ(request_sent, 1);
	UT_ASSERT_EQ(cooperative_sleeps, 0);
	cooperative_case = false;
}

UT_TEST(cooperative_redeclare_stale_queue_cannot_rebind_current_holder)
{
	for (int drift = 1; drift <= 3; drift++) {
		ClusterLockAcquireRequest req;
		ClusterGesRedeclareAttempt attempt = { 0 };
		MasterPacket post;
		GesReplyPayload reply;

		setup_cooperative(&req);
		UT_ASSERT_EQ(cluster_ges_redeclare_poll(&attempt, &req.resid, req.lockmode, &req.holder),
					 CLUSTER_GES_REDECLARE_PENDING);
		cooperative_drift = drift;
		reply = drive_redeclare_master(&req, &post);
		UT_ASSERT_EQ(reply.opcode, GES_REPLY_OPCODE_REJECT);
		UT_ASSERT_EQ(reply.reject_reason, drift == 3 ? GES_REJECT_REASON_MASTER_DEAD_NATIVE
													 : GES_REJECT_REASON_EPOCH_MISMATCH);
		UT_ASSERT_EQ(post.held_mode, ExclusiveLock);
		UT_ASSERT_EQ(post.replies, 0);
		cooperative_case = false;
	}
}

/* A queue item was validated before its epoch/map changed. Exercise the
 * actual mutation owner, not a mocked refusal. Each reset has an empty
 * requester reservation and an independently chosen retained master hold. */
static void
queued_cut_prepare(uint32 opcode, bool conditional_convert, int drift,
				   ClusterLockAcquireRequest *req, ClusterGrdHolderId *retained)
{
	fault = HW_NORMAL;
	relation_case = relation_nowait_case = cooperative_case = false;
	setup_case(req, false);
	HW_CHECK(cluster_grd_cancel_reservation_by_id(&req->resid, &req->holder)
			 == CLUSTER_GRD_ENTRY_OK);
	cluster_node_id = 3;
	ut_mock_epoch = drift == 1 ? 1 : 2;
	if (conditional_convert) {
		req->resid.type = CLUSTER_WAL_RETENTION_RESID_TYPE;
		while (cluster_grd_lookup_master(&req->resid) != 3)
			req->resid.field1++;
	}
	memset(&master_request, 0, sizeof(master_request));
	master_request.opcode = opcode;
	master_request.lockmode = ExclusiveLock;
	master_request.holder_node_id = 1;
	master_request.holder_procno = 21;
	master_request.holder_cluster_epoch_lo = drift == 1 ? 1 : 2;
	master_request.holder_request_id_lo = 201;
	master_request.shard_master_generation_lo = 47; /* Independent origin token. */
	memcpy(master_request.resid, &req->resid, sizeof(req->resid));
	*retained = grd_lifecycle_holder(1, 21, 201);
	retained->cluster_epoch = master_request.holder_cluster_epoch_lo;
	if (opcode == GES_REQ_OPCODE_CONVERT || conditional_convert) {
		retained->request_id = 200;
		master_request.current_mode = ShareLock;
		master_request.wait_seq = 200;
	} else if (opcode == GES_REQ_OPCODE_CONVERT_ROLLBACK) {
		retained->request_id = 203;
		master_request.current_mode = ShareLock;
		master_request.wait_seq = 203;
	}
	if ((opcode != GES_REQ_OPCODE_REQUEST && opcode != GES_REQ_OPCODE_REQUEST_NOWAIT)
		|| conditional_convert) {
		LOCKMODE mode
			= opcode == GES_REQ_OPCODE_CONVERT || conditional_convert ? ShareLock : ExclusiveLock;
		HW_CHECK(cluster_grd_entry_rebind_or_insert_holder(&req->resid, retained, 1, mode)
				 == CLUSTER_GRD_ENTRY_OK);
	}
	stage_master_work();
	if (drift == 1)
		ut_mock_epoch = 2;
	else if (drift == 2)
		mock_lms_shard_master_generation = 10;
	else if (drift == 3)
		cluster_node_id = 0;
	master_reply_count = 0;
	memset(&master_reply, 0, sizeof(master_reply));
}

UT_TEST(queued_mutations_revalidate_the_complete_cut_before_touching_grd)
{
	const uint32 opcodes[] = { GES_REQ_OPCODE_REQUEST,		 GES_REQ_OPCODE_REQUEST_NOWAIT,
							   GES_REQ_OPCODE_CONVERT,		 GES_REQ_OPCODE_CONVERT_ROLLBACK,
							   GES_REQ_OPCODE_RELEASE,		 GES_REQ_OPCODE_REDECLARE,
							   GES_REQ_OPCODE_REQUEST_NOWAIT };

	queued_cut_case = true;
	for (int op = 0; op < lengthof(opcodes); op++) {
		for (int drift = 1; drift <= 3; drift++) {
			ClusterLockAcquireRequest req;
			ClusterGrdHolderId retained;
			LOCKMODE mode = NoLock;
			bool has_hold;

			queued_cut_prepare(opcodes[op], op == 6, drift, &req, &retained);
			printf("# queued opcode=%u conditional_convert=%d drift=%d\n", opcodes[op], op == 6,
				   drift);
			UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
			UT_ASSERT_EQ(master_reply_count, 1);
			UT_ASSERT_EQ(master_reply.opcode, GES_REPLY_OPCODE_REJECT);
			UT_ASSERT_EQ(master_reply.reply_for_opcode, opcodes[op]);
			UT_ASSERT_EQ(master_reply.reject_reason, drift == 3
														 ? GES_REJECT_REASON_MASTER_DEAD_NATIVE
														 : GES_REJECT_REASON_EPOCH_MISMATCH);
			has_hold = cluster_grd_holder_mode_by_id(&req.resid, &retained, &mode);
			UT_ASSERT_EQ(has_hold, op >= 2);
			if (has_hold)
				UT_ASSERT_EQ(mode, op == 2 || op == 6 ? ShareLock : ExclusiveLock);
			if (op < 2)
				UT_ASSERT_EQ(cluster_grd_entry_count(), 0);
		}
	}
	queued_cut_case = false;
	MyProc = NULL;
}

/* Sender LMS restart counts are not receiver routing cuts. A valid incoming
 * request must not fail because two healthy instances have different counts.
 * Author: SqlRush <sqlrush@gmail.com> */
UT_TEST(queued_origin_generation_is_not_receiver_cut)
{
	ClusterLockAcquireRequest req;
	ClusterGrdHolderId retained;
	LOCKMODE mode = NoLock;

	queued_cut_case = true;
	queued_cut_prepare(GES_REQ_OPCODE_REQUEST, false, 0, &req, &retained);
	master_request.shard_master_generation_lo = 47;
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(master_reply.opcode, GES_REPLY_OPCODE_GRANT);
	UT_ASSERT(cluster_grd_holder_mode_by_id(&req.resid, &retained, &mode));
	UT_ASSERT_EQ(mode, ExclusiveLock);
	queued_cut_case = false;
	MyProc = NULL;
}

#ifndef PGRAC_HW_HANDOFF_EMBEDDED
int
main(void)
{
	MyBackendType = B_BACKEND; /* Definition belongs to the embedded GRD fixture. */
	setvbuf(stdout, NULL, _IONBF, 0);
	alarm(30); /* Standalone fixture owner, not a database deadline. */
	UT_PLAN(47);
	UT_RUN(no_sibling_control);
	UT_RUN(real_grant_sibling_promotes);
	UT_RUN(relation_share_grant_survives_compatible_sibling);
	UT_RUN(relation_remote_failure_ownership_matrix);
	UT_RUN(relation_remote_nowait_grant_and_cleanup);
	UT_RUN(relation_local_authoritative_grant);
	UT_RUN(relation_local_backout_drains_successor);
	UT_RUN(relation_local_nowait_keeps_conflict_semantics);
	UT_RUN(cf_remote_grant_and_exact_cleanup);
	UT_RUN(cf_local_grant_and_owned_backout);
	UT_RUN(release_sender_unknown_master_cannot_confirm);
	UT_RUN(release_sender_local_route_really_drains_holder);
	UT_RUN(control_retirement_clears_the_same_queued_request);
	UT_RUN(release_sender_confirms_exact_verdict_and_unwinds_waiter);
	UT_RUN(release_sender_keeps_foreign_waiter_and_rejects_bad_identity);
	UT_RUN(cf_request_refuses_invalid_or_owned_provenance);
	UT_RUN(cf_reservation_without_observed_grant_is_not_authority);
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
	UT_RUN(hw_local_cancel_after_grant_keeps_exact_cleanup_owner);
	UT_RUN(hw_local_grant_rejects_epoch_change_before_promotion);
	UT_RUN(hw_local_two_reservations_survive_exact_grants);
	UT_RUN(hw_local_release_does_not_invalidate_another_reserved_request);
	UT_RUN(relation_native_error_has_full_interval_cleanup_owner);
	UT_RUN(cooperative_redeclare_yields_then_consumes_real_grant);
	UT_RUN(cooperative_redeclare_late_cut_cannot_publish_ack);
	UT_RUN(cooperative_remote_exchange_survives_origin_counter_change);
	UT_RUN(cooperative_redeclare_malformed_verdict_cannot_restart);
	UT_RUN(cooperative_redeclare_stale_queue_cannot_rebind_current_holder);
	UT_RUN(queued_mutations_revalidate_the_complete_cut_before_touching_grd);
	UT_RUN(queued_origin_generation_is_not_receiver_cut);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
#endif
