/*-------------------------------------------------------------------------
 *
 * cluster_ko_lock.c
 *	  KO (object-reuse flush) backend: shmem counters + the SPSC peer inbound
 *	  ring + KO(X) GES serialise lock + the apply-after-drop flush fanout/ACK
 *	  barrier + the SI-Broadcaster-aux peer drain (spec-5.7 §3.5/§3.6 / D6/D7).
 *	  The pure resid encoder lives in cluster_ko.c (standalone-linkable for the
 *	  unit test).
 *
 *	  Two sides of the barrier:
 *
 *	    Dropping node (cluster_ko_flush_and_wait_ack): hooked PRE-COMMIT at the
 *	    DDL path (RelationDropStorage / RelationTruncate) while the relation's
 *	    cross-node AccessExclusiveLock is held.  Acquires KO(X) on the
 *	    relfilenode, fanouts PGRAC_IC_MSG_KO_FLUSH to every alive peer (one msg
 *	    per peer via the GRD outbound ring; LMON sends), and waits on the reused
 *	    2.39 ack_wait correlation infra (KO-B2) until every peer has ACK'd DONE.
 *	    Any peer not ACKing in time -> ereport(ERROR 53RAA) (8.A fail-closed).
 *
 *	    Peer node: the IC inbound handler (LMON) nonblocking-enqueues the request
 *	    into a lock-free SPSC ring (HC133-style) and wakes the SI Broadcaster aux;
 *	    the aux (cluster_ko_drain_inbound_and_apply) does the heavy work off the
 *	    heartbeat path -- FlushRelationsAllBuffers (dirty -> shared storage, so a
 *	    rolled-back DROP loses no data) then DropRelationsAllBuffers (invalidate,
 *	    so no stale writeback) -- and ONLY THEN enqueues a KO_FLUSH_ACK (DONE).
 *	    This apply-after-drop ordering is the KO correctness gate (KO-M6): the
 *	    enqueuer never proceeds to the physical op until every peer's buffers are
 *	    gone.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ko_lock.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D6/D7, §3.5/§3.6)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h" /* RecoveryInProgress */
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_extend_gate.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_ges_holder.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hw_lease.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_pi_writeback.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "common/cryptohash.h"
#include "cluster/cluster_sinval.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/backendid.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "datatype/timestamp.h"
#include "utils/timestamp.h"
#include "utils/memutils.h"
#include "utils/inval.h"
#include "utils/resowner.h"
#include "utils/rel.h"
#include "utils/dsa.h"
#include "utils/wait_event.h"

/* PG core doesn't define USECS_PER_MSEC;  define locally (mirror cluster_sinval.c). */
#ifndef USECS_PER_MSEC
#define USECS_PER_MSEC INT64CONST(1000)
#endif

/* ============================================================
 * Shmem region: six observability counters + the SPSC peer inbound ring.
 *
 *   The inbound ring is single-producer (the LMON IC handler, which owns the
 *   tier1 fds, is the only KO_FLUSH dispatcher) / single-consumer (the SI
 *   Broadcaster aux is the only drainer), so it is lock-free: the producer
 *   publishes a slot then advances the tail with a write barrier, and the
 *   consumer reads the slot after loading the tail with a read barrier.
 * ============================================================ */

#define CLUSTER_KO_INBOUND_CAPACITY 64
#define CLUSTER_KO_SHARED_CAPACITY CLUSTER_KO_SHARED_CONTEXT_LIMIT_V2
#define CLUSTER_KO_SHARED_NODE_LIMIT 16

typedef struct ClusterKoSharedContext {
	bool used;
	bool v3;
	bool complete;
	bool structure_owned;
	bool structure_drop_pending;
	bool structure_drop_failed; /* Original executor requires retained-WAL recovery. */
	uint16 structure_peers_accepted;
	uint16 request_sent_peers;
	uint16 request_uncertain_peers;
	uint32 structure_scan;
	uint64 structure_scan_freeze;
	int32 pid;
	int32 drop_executor_pid;
	uint64 serial;
	ClusterKoSharedMessageV2 request;
	ClusterKoSharedMessageV3 request_v3;
	uint64 peer_boots[CLUSTER_KO_SHARED_NODE_LIMIT];
	/* The original background owner must clear an installed obligation.
	 * Completed native barriers wait in their preallocated transaction owner
	 * until the structural result can enter this bounded shared region. */
	ClusterPageWalBindingV1 terminal;
	uint8 structure[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
} ClusterKoSharedContext;

typedef struct ClusterKoInboundSlot {
	bool shared;
	bool v3;
	ClusterKoSharedMessageV2 qualified;
	ClusterKoSharedMessageV3 qualified_v3;
	ClusterICControlIngress ingress;
	uint64 peer_route_generation;
	uint32 db_oid;
	uint32 rel_number;
	uint32 spc_oid;
	int32 source_node;
	uint64 batch_id;
	uint64 epoch;
} ClusterKoInboundSlot;

typedef struct ClusterKoPeerFenceV3 {
	ClusterKoPeerFenceStateV3 state;
	uint64 serial;
	uint64 peer_route_generation;
	ClusterKoSharedMessageV3 request;
	uint8 encoded[CLUSTER_KO_SHARED_V3_BYTES];
	ClusterGesHolderProbe holder_probe;
	TimestampTz holder_sent_at;
} ClusterKoPeerFenceV3;

typedef struct ClusterKoCleanerWorkV3 {
	bool used;
	RelFileLocator locator;
	ClusterCtrcCleanerReadScopeV3 scope;
} ClusterKoCleanerWorkV3;

typedef struct ClusterKoShared {
	pg_atomic_uint64 flush_count;		 /* barriers initiated (enqueuer) */
	pg_atomic_uint64 ack_received_count; /* peer DONE ACKs recorded (enqueuer) */
	pg_atomic_uint64 failclosed_count;	 /* 53RAA fail-closed (enqueuer) */
	pg_atomic_uint64 native_count;		 /* no-op: single-node / no peer / private */
	pg_atomic_uint64 lockfail_count;	 /* KO(X) acquire failed (best-effort; barrier ran) */
	pg_atomic_uint64 peer_apply_count;	 /* flush+drop applied + ACK'd (peer) */
	pg_atomic_uint64 inbound_full_count; /* KO inbound ring full (peer -> no ACK) */
	pg_atomic_uint32 inbound_head;		 /* consumer (SI Broadcaster aux) */
	pg_atomic_uint32 inbound_tail;		 /* producer (LMON IC handler) */
	ClusterKoInboundSlot inbound[CLUSTER_KO_INBOUND_CAPACITY];
	/* Only this cold DDL family grows; the GES hot ring stays 80 bytes. */
	slock_t shared_lock;
	uint32 send_head, send_count;
	ClusterKoSharedMessageV2 send[CLUSTER_KO_SHARED_CAPACITY];
	uint32 send_v3_head, send_v3_count;
	ClusterKoSharedMessageV3 send_v3[CLUSTER_KO_SHARED_CAPACITY];
	uint64 send_v3_route_generation[CLUSTER_KO_SHARED_CAPACITY];
	uint64 context_serial;
	ClusterKoSharedContext contexts[CLUSTER_KO_SHARED_CAPACITY];
	/* Peer operations are not owned by an origin backend ResourceOwner.
	 * Passive READY/terminal identities survive its context cleanup. */
	uint64 peer_fence_serial;
	pg_atomic_uint64 peer_admission_revision;
	uint64 holder_probe_nonce;
	uint32 holder_probe_cursor;
	ClusterKoPeerFenceV3 peer_fences[CLUSTER_KO_SHARED_CAPACITY];
	ClusterKoCleanerWorkV3 cleaner_work[CLUSTER_CTRC_CLEANER_WORKERS];
	/* Preallocated native continuations belong to this same KO owner.
	 * One finite ready batch is drained before adopting newly committed work. */
	int native_dsa_tranche;
	dsa_pointer native_incoming;
	dsa_pointer native_ready;
	dsa_pointer native_failed; /* Retained evidence; never re-enters execution. */
	bool native_recovery_required;
	uint64 native_waiting;
} ClusterKoShared;

static ClusterKoShared *ko_state = NULL;
static bool ko_exit_registered;
static dsa_area *ko_native_area;

typedef struct KoAxBinding KoAxBinding;
static bool ko_capture_relation_ax_binding(Relation rel, KoAxBinding *out);

typedef struct ClusterKoNativeContinuation {
	dsa_pointer next;
	bool buffers_released;
	bool recovery_required;
	ClusterKoSharedContext context;
} ClusterKoNativeContinuation;

struct ClusterKoCompletionV2 {
	ResourceOwner owner;
	int32 pid;
	unsigned slot;
	uint64 serial;
	bool native_transaction;
	bool native_pending;
	bool space_observed;
	bool truncate_observed;
	bool drop_observed;
	bool postcommit;
	bool native_handed_off;
	bool native_buffers_released;
	dsa_pointer native_unready;
	/* Native DDL keeps its verified barrier in the original ResourceOwner,
	 * releasing the transport slot before the next relation's barrier. */
	bool barrier_private;
	bool barrier_v3;
	dsa_pointer native_allocation;
	ClusterKoNativeContinuation *native_reserved;
	ClusterKoSharedMessageV2 barrier_request;
	ClusterKoSharedMessageV3 barrier_request_v3;
	uint64 barrier_boots[CLUSTER_KO_SHARED_NODE_LIMIT];
	ClusterPageWalBindingV1 terminal;
	uint8 structure[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	struct ClusterKoCompletionV2 *next;
};

static ClusterKoCompletionV2 *ko_completions;
static bool ko_resource_registered;
static void ko_shared_backend_exit(int code, Datum arg);

/* Process-local storage state belongs to the original active KO context.
 * It survives a checkpointer ERROR, but can never migrate to another process. */
struct ClusterKoDropWorkV2 {
	ResourceOwner owner;
	int32 pid;
	uint32 slot;
	Size storage_bytes;
	bool abandoned;
	ClusterKoSharedContext context;
};
static ClusterKoDropWorkV2 *ko_drop_work[CLUSTER_KO_SHARED_CAPACITY];
static uint32 ko_drop_scan;
static uint64 ko_native_report_serial;
static uint64 ko_drop_report_serial[CLUSTER_KO_SHARED_CAPACITY];

ClusterNormalStopPollResult
cluster_ko_normal_stop_poll(uint32 *slot_out, const char **reason_out)
{
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_INVALID;
	const char *reason = "UNINITIALIZED";
	uint32 slot = 0, head, tail;

	if (ko_state == NULL)
		goto done;
	/* Only the sole consumer can inspect published slots without racing
	 * another consumer advancing head and allowing producer reuse. The
	 * checkpointer consumes this actor's sealed idle publication instead. */
	if (!IsUnderPostmaster || !AmSinvalBcastProcess()) {
		reason = "NOT_CONSUMER";
		goto done;
	}
	head = pg_atomic_read_u32(&ko_state->inbound_head);
	tail = pg_atomic_read_u32(&ko_state->inbound_tail);
	pg_read_barrier();
	if (head >= CLUSTER_KO_INBOUND_CAPACITY || tail >= CLUSTER_KO_INBOUND_CAPACITY) {
		reason = "RING_GEOMETRY";
		goto done;
	}
	result = CLUSTER_NORMAL_STOP_READY;
	reason = "EMPTY";
	for (uint32 i = head; i != tail; i = (i + 1) % CLUSTER_KO_INBOUND_CAPACITY) {
		const ClusterKoInboundSlot *item = &ko_state->inbound[i];
		if (item->batch_id == 0 || (item->shared && item->epoch == 0) || item->source_node < 0
			|| item->source_node >= CLUSTER_MAX_NODES || item->spc_oid == InvalidOid
			|| item->rel_number == InvalidRelFileNumber) {
			result = CLUSTER_NORMAL_STOP_INVALID;
			reason = "INVALID_REQUEST";
			slot = i;
			break;
		}
		if (result == CLUSTER_NORMAL_STOP_READY) {
			result = CLUSTER_NORMAL_STOP_PENDING;
			reason = "REQUEST_OWNED";
			slot = i;
		}
	}
done:
	if (slot_out != NULL)
		*slot_out = slot;
	if (reason_out != NULL)
		*reason_out = reason;
	return result;
}

static Size
ko_native_area_size(void)
{
	return MAXALIGN(Max(dsa_minimum_size(), 64 * 1024));
}

static void *
ko_native_area_memory(void)
{
	return (char *)ko_state + MAXALIGN(sizeof(ClusterKoShared));
}

Size
cluster_ko_shmem_size(void)
{
	return add_size(MAXALIGN(sizeof(ClusterKoShared)), ko_native_area_size());
}

void
cluster_ko_shmem_init(void)
{
	bool found;

	ko_state
		= (ClusterKoShared *)ShmemInitStruct("pgrac cluster ko", cluster_ko_shmem_size(), &found);
	if (!IsUnderPostmaster) {
		dsa_area *area;

		ko_state->native_dsa_tranche = LWLockNewTrancheId();
		LWLockRegisterTranche(ko_state->native_dsa_tranche, "ClusterKoNativeOwner");
		area = dsa_create_in_place(ko_native_area_memory(), ko_native_area_size(),
								   ko_state->native_dsa_tranche, NULL);
		dsa_pin(area);
		dsa_detach(area);
		ko_state->native_incoming = ko_state->native_ready = ko_state->native_failed
			= InvalidDsaPointer;
		ko_state->native_recovery_required = false;
		ko_state->native_waiting = 0;
		pg_atomic_init_u64(&ko_state->flush_count, 0);
		pg_atomic_init_u64(&ko_state->ack_received_count, 0);
		pg_atomic_init_u64(&ko_state->failclosed_count, 0);
		pg_atomic_init_u64(&ko_state->native_count, 0);
		pg_atomic_init_u64(&ko_state->lockfail_count, 0);
		pg_atomic_init_u64(&ko_state->peer_apply_count, 0);
		pg_atomic_init_u64(&ko_state->inbound_full_count, 0);
		pg_atomic_init_u32(&ko_state->inbound_head, 0);
		pg_atomic_init_u32(&ko_state->inbound_tail, 0);
		SpinLockInit(&ko_state->shared_lock);
		ko_state->send_head = ko_state->send_count = 0;
		ko_state->send_v3_head = ko_state->send_v3_count = 0;
		ko_state->context_serial = 0;
		memset(ko_state->contexts, 0, sizeof(ko_state->contexts));
		ko_state->peer_fence_serial = 0;
		pg_atomic_init_u64(&ko_state->peer_admission_revision, 1);
		ko_state->holder_probe_nonce = 0;
		ko_state->holder_probe_cursor = 0;
		memset(ko_state->peer_fences, 0, sizeof(ko_state->peer_fences));
	}
}

static const ClusterShmemRegion cluster_ko_region = {
	.name = "pgrac cluster ko",
	.size_fn = cluster_ko_shmem_size,
	.init_fn = cluster_ko_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "spec-5.7 KO object-reuse flush",
	.reserved_flags = 0,
};

void
cluster_ko_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ko_region);
}

#define KO_BUMP(field)                                                                             \
	do {                                                                                           \
		if (ko_state != NULL)                                                                      \
			pg_atomic_fetch_add_u64(&ko_state->field, 1);                                          \
	} while (0)

uint64
cluster_ko_flush_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->flush_count) : 0;
}
uint64
cluster_ko_ack_received_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->ack_received_count) : 0;
}
uint64
cluster_ko_failclosed_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->failclosed_count) : 0;
}
uint64
cluster_ko_native_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->native_count) : 0;
}
uint64
cluster_ko_lockfail_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->lockfail_count) : 0;
}
uint64
cluster_ko_peer_apply_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->peer_apply_count) : 0;
}
uint64
cluster_ko_inbound_full_count(void)
{
	return ko_state != NULL ? pg_atomic_read_u64(&ko_state->inbound_full_count) : 0;
}


/* ============================================================
 * KO(X) GES serialise lock over the spec-5.3 substrate (mirror dl_lock).
 *
 *   KO(X) serialises concurrent DROP/TRUNCATE of the SAME relfilenode and makes
 *   holder-crash recovery reconfig-driven (KO-M3).  The buffer-safety guarantee
 *   comes from the flush-ACK barrier, not from this lock; KO(X) is the coarse
 *   serialiser the spec lists alongside it (§3.5 KO-M1).
 * ============================================================ */

typedef struct KoLock {
	bool held;
	bool coordinated;
	ClusterLockAcquireRequest req;
} KoLock;

typedef enum KoAcquireOutcome {
	KO_ACQUIRE_GRANTED = 0,
	KO_ACQUIRE_NATIVE,
	KO_ACQUIRE_FAILED,
} KoAcquireOutcome;

static KoAcquireOutcome
ko_lock(const ClusterResId *resid, KoLock *lk)
{
	ClusterLockAcquireRequest req;
	ClusterLockAcquireResult r;

	Assert(resid != NULL && lk != NULL);
	memset(lk, 0, sizeof(*lk));

	memset(&req, 0, sizeof(req));
	req.resid = *resid;
	req.lockmode = ExclusiveLock;
	req.op = CLUSTER_LOCK_OP_REQUEST; /* KO never converts */
	req.current_mode = NoLock;
	req.lockmethod_id = DEFAULT_LOCKMETHOD;
	req.dontwait = false;
	req.sessionLock = false;
	req.caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	req.timeout_ms = cluster_ges_request_timeout_ms;
	/* A blocked KO(X) waiter is "awaiting a GES grant" -- reuse the GES reply wait
	 * event (per spec-5.7 Q10, only HW/KO get dedicated events, and KO's dedicated
	 * ClusterObjectFlushWait covers the peer-ACK wait, not the KO(X) acquire). */
	req.wait_event = WAIT_EVENT_CLUSTER_GES_REPLY_WAIT;

	r = cluster_lock_acquire_seven_step(&req);

	switch (r) {
	case CLUSTER_LOCK_ACQUIRE_OK_NATIVE:
		/* Cluster/LMS layer inactive for this resid (e.g. a new-in-txn private
		 * relfilenode no peer can see) -- no peer holds its buffers, so the flush
		 * barrier is vacuous.  Proceed without coordination (held=false). */
		return KO_ACQUIRE_NATIVE;

	case CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK:
		/* Modern wire success: promote to register the GRD holder for cross-node
		 * conflict.  KO has no PG-native heavyweight lock to take in between. */
		if (cluster_lock_acquire_s5_promote(&req) != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			return KO_ACQUIRE_FAILED;
		lk->held = true;
		lk->coordinated = true;
		lk->req = req;
		return KO_ACQUIRE_GRANTED;

	case CLUSTER_LOCK_ACQUIRE_OK_GRANTED:
	case CLUSTER_LOCK_ACQUIRE_OK_CONVERTED:
		/* Legacy / stub S4 path: seven_step already ran S5 and returned a promoted
		 * holder.  Do NOT promote a second time; just record it. */
		lk->held = true;
		lk->coordinated = true;
		lk->req = req;
		return KO_ACQUIRE_GRANTED;

	default:
		/* NOT_AVAIL / timeout / LMS-unavailable / deadlock / internal. */
		return KO_ACQUIRE_FAILED;
	}
}

static void
ko_unlock(KoLock *lk)
{
	if (lk == NULL || !lk->held)
		return;
	if (lk->coordinated)
		(void)cluster_lock_acquire_s6_release(&lk->req);
	lk->held = false;
	lk->coordinated = false;
}


/* The membership digest is a cross-node canonical projection, never a hash of
 * process-local observer fields or C padding. Capture and generation checks
 * belong to the original reconfig owner. No page or entry lock is held here.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
ko_cut_u64(uint8 **cursor, uint64 value)
{
	for (unsigned i = 0; i < 8; i++)
		*(*cursor)++ = (uint8)(value >> (8 * i));
}

static bool
ko_shared_members(ClusterKoSharedMessageV2 *message, uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT],
				  uint64 *generation)
{
	static const char domain[] = "PGRAC-KO-MEMBERS-V2";
	ClusterFormationSnapshotV1 snapshot;
	ClusterKoSharedMessageV2 result = *message;
	uint64 sampled[CLUSTER_KO_SHARED_NODE_LIMIT] = { 0 };
	uint64 before = cluster_membership_cut_generation();
	uint8 preimage[sizeof(domain) - 1 + 16 + CLUSTER_MAX_NODES * 9 + 64];
	uint8 *p = preimage;
	pg_cryptohash_ctx *hash;
	bool ok;

	StaticAssertStmt(CLUSTER_MAX_NODES == CLUSTER_KO_SHARED_MEMBER_BYTES * 8,
					 "KO must cover the complete member table");
	if (before == 0 || cluster_node_id < 0 || cluster_node_id >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| !cluster_reconfig_capture_formation_snapshot_v1((uint16)(cluster_node_id + 1), &snapshot)
		|| !cluster_membership_cut_generation_current(before) || snapshot.local_epoch == 0
		|| snapshot.local_epoch == UINT64_MAX || snapshot.prebump_sync_active != 0
		|| snapshot.self_join_failed || snapshot.reserved[0] != 0 || snapshot.reserved[1] != 0)
		return false;
	result.epoch = snapshot.local_epoch;
	memset(result.members, 0, sizeof(result.members));
	memcpy(p, domain, sizeof(domain) - 1);
	p += sizeof(domain) - 1;
	ko_cut_u64(&p, snapshot.local_epoch);
	ko_cut_u64(&p, snapshot.startup_formation_generation);
	for (int n = 0; n < CLUSTER_MAX_NODES; n++) {
		uint8 state = snapshot.membership.membership_state[n];
		uint64 boot = snapshot.membership.last_admitted_incarnation[n];
		uint8 bit = 1u << (n % 8);

		if (state > CLUSTER_MEMBER_REMOVED || state == CLUSTER_MEMBER_JOINING
			|| snapshot.pending_join_bitmap[n / 8] != 0)
			return false;
		if (state == CLUSTER_MEMBER_MEMBER) {
			if (n >= CLUSTER_KO_SHARED_NODE_LIMIT || boot == 0 || boot == UINT64_MAX
				|| ((snapshot.excluded_bitmap[n / 8] | snapshot.removed_bitmap[n / 8]
					 | snapshot.clean_departed_bitmap[n / 8])
					& bit)
					   != 0)
				return false;
			result.members[n / 8] |= bit;
			sampled[n] = boot;
		}
		*p++ = state;
		ko_cut_u64(&p, boot);
	}
	memcpy(p, snapshot.pending_join_bitmap, 16);
	p += 16;
	memcpy(p, snapshot.clean_departed_bitmap, 16);
	p += 16;
	memcpy(p, snapshot.removed_bitmap, 16);
	p += 16;
	memcpy(p, snapshot.excluded_bitmap, 16);
	p += 16;
	Assert(p == preimage + sizeof(preimage));
	hash = pg_cryptohash_create(PG_SHA256);
	if (hash == NULL)
		return false;
	ok = pg_cryptohash_init(hash) >= 0
		 && pg_cryptohash_update(hash, preimage, sizeof(preimage)) >= 0
		 && pg_cryptohash_final(hash, result.member_digest, sizeof(result.member_digest)) >= 0;
	pg_cryptohash_free(hash);
	if (!ok || !cluster_membership_cut_generation_current(before)
		|| snapshot.local_epoch != cluster_epoch_get_current())
		return false;
	*message = result;
	memcpy(boots, sampled, sizeof(sampled));
	*generation = before;
	return true;
}

bool
cluster_ko_shared_cut_current_v2(const ClusterKoSharedMessageV2 *request)
{
	ClusterKoSharedMessageV2 current;
	ClusterWalSourceRef before, after;
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], generation;
	uint32 capability[2], connection[2];
	int endpoints[2];
	uint8 expected[CLUSTER_KO_SHARED_V2_BYTES], observed[sizeof(expected)];

	if (request == NULL || !cluster_enabled || !cluster_shared_config || RecoveryInProgress()
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| request->origin_node >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| request->peer_node >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| request->verb != CLUSTER_KO_SHARED_REQUEST
		|| !cluster_ko_shared_encode_v2(request, expected, sizeof(expected))
		|| !cluster_wal_thread_current_v2_ref(&before)
		|| before.claim.identity.origin_node_id != cluster_node_id
		|| before.claim.identity.system_identifier != request->key.system_identifier
		|| before.claim.database_incarnation != request->key.database_incarnation
		|| memcmp(before.claim.identity.storage_uuid, request->key.storage_uuid, 16) != 0)
		return false;
	current = *request;
	if (!ko_shared_members(&current, boots, &generation) || boots[cluster_node_id] == 0
		|| boots[cluster_node_id] != before.claim.identity.origin_owner_incarnation)
		return false;
	current.origin_boot = boots[request->origin_node];
	current.peer_boot = boots[request->peer_node];
	if (!cluster_ko_shared_encode_v2(&current, observed, sizeof(observed))
		|| memcmp(expected, observed, sizeof(expected)) != 0)
		return false;
	endpoints[0] = request->origin_node;
	endpoints[1] = request->peer_node;
	for (int i = 0; i < 2; i++) {
		if (endpoints[i] == cluster_node_id)
			continue;
		if (!cluster_sf_peer_capability_word_sample(endpoints[i], PGRAC_IC_HELLO_CAP_KO_SHARED_V2,
													&capability[i], &connection[i])
			|| connection[i] == 0 || (capability[i] & PGRAC_IC_HELLO_CAP_KO_SHARED_V2) == 0)
			return false;
	}
	/* A decoded relation result is still untrusted. In particular, one
	 * endpoint's new CONTROL connection cannot inherit the old cut. */
	for (int i = 0; i < 2; i++) {
		uint32 word, sampled;
		if (endpoints[i] == cluster_node_id)
			continue;
		if (!cluster_sf_peer_capability_word_sample(endpoints[i], PGRAC_IC_HELLO_CAP_KO_SHARED_V2,
													&word, &sampled)
			|| word != capability[i] || sampled != connection[i])
			return false;
	}
	return cluster_wal_thread_current_v2_ref(&after) && memcmp(&before, &after, sizeof(before)) == 0
		   && cluster_membership_cut_generation_current(generation);
}

static bool
ko_shared_control_current(const ClusterKoSharedMessageV2 *message)
{
	ClusterKoSharedMessageV2 current = *message;
	ClusterWalSourceRef ref;
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], generation;
	uint32 capabilities, capability_generation;
	uint8 expected[CLUSTER_KO_SHARED_V2_BYTES], observed[CLUSTER_KO_SHARED_V2_BYTES];
	int other;

	if (message->origin_node >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| message->peer_node >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| (cluster_node_id != message->origin_node && cluster_node_id != message->peer_node)
		|| !cluster_ko_shared_encode_v2(message, expected, sizeof(expected))
		|| !cluster_wal_thread_current_v2_ref(&ref)
		|| ref.claim.identity.origin_node_id != cluster_node_id
		|| ref.claim.identity.system_identifier != message->key.system_identifier
		|| ref.claim.database_incarnation != message->key.database_incarnation
		|| memcmp(ref.claim.identity.storage_uuid, message->key.storage_uuid, 16) != 0
		|| !ko_shared_members(&current, boots, &generation)
		|| boots[cluster_node_id] != ref.claim.identity.origin_owner_incarnation)
		return false;
	current.origin_boot = boots[message->origin_node];
	current.peer_boot = boots[message->peer_node];
	other = cluster_node_id == message->origin_node ? message->peer_node : message->origin_node;
	return cluster_sf_peer_capability_word_sample(other, PGRAC_IC_HELLO_CAP_KO_SHARED_V2,
												  &capabilities, &capability_generation)
		   && capability_generation != 0 && cluster_membership_cut_generation_current(generation)
		   && cluster_ko_shared_encode_v2(&current, observed, sizeof(observed))
		   && memcmp(expected, observed, sizeof(expected)) == 0;
}

/* R2: v3 retains the exact v2 formation/WAL cut, but binds the peer check to
 * the independently negotiated object-drain capability and the immutable AX
 * suffix. No maintenance identity read is part of this control predicate. */
static bool
ko_shared_v3_control_at_generation(const ClusterKoSharedMessageV3 *message, uint64 local_generation)
{
	const uint32 required_capabilities
		= PGRAC_IC_HELLO_CAP_KO_OBJECT_DRAIN_V3 | PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1;
	ClusterKoSharedMessageV3 current;
	ClusterWalSourceRef ref;
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], generation;
	uint32 capabilities, capability_generation;
	uint8 expected[CLUSTER_KO_SHARED_V3_BYTES], observed[sizeof(expected)];
	int32 current_master, other;

	if (message == NULL || local_generation == 0 || local_generation == UINT64_MAX
		|| cluster_interconnect_tier != CLUSTER_IC_TIER_1)
		return false;
	current = *message;
	/* The AX wire token belongs to the origin. Each consumer retains and
	 * checks its own local route cut without rewriting that provenance. */
	current_master = cluster_grd_lookup_master_gen(&message->ax_resid, &generation);
	if (current_master < 0 || current_master != message->ax_master_node
		|| generation != local_generation)
		return false;
	if (message->base.origin_node >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| message->base.peer_node >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| (cluster_node_id != message->base.origin_node
			&& cluster_node_id != message->base.peer_node)
		|| !cluster_ko_shared_encode_v3(message, expected, sizeof(expected))
		|| !cluster_wal_thread_current_v2_ref(&ref)
		|| ref.claim.identity.origin_node_id != cluster_node_id
		|| ref.claim.identity.system_identifier != message->base.key.system_identifier
		|| ref.claim.database_incarnation != message->base.key.database_incarnation
		|| memcmp(ref.claim.identity.storage_uuid, message->base.key.storage_uuid, 16) != 0
		|| !ko_shared_members(&current.base, boots, &generation)
		|| boots[cluster_node_id] != ref.claim.identity.origin_owner_incarnation)
		return false;
	current.base.origin_boot = boots[message->base.origin_node];
	current.base.peer_boot = boots[message->base.peer_node];
	other = cluster_node_id == message->base.origin_node ? message->base.peer_node
																		 : message->base.origin_node;
	if (!cluster_sf_peer_capability_word_sample(other, required_capabilities, &capabilities,
												&capability_generation)
		|| capability_generation == 0
		|| (capabilities & required_capabilities) != required_capabilities
		|| !cluster_membership_cut_generation_current(generation)
		|| !cluster_ko_shared_encode_v3(&current, observed, sizeof(observed)))
		return false;
	return memcmp(expected, observed, sizeof(expected)) == 0
		   && cluster_grd_lookup_master_gen(&message->ax_resid, &generation) == current_master
		   && generation == local_generation;
}

static bool
ko_shared_v3_control_current(const ClusterKoSharedMessageV3 *message)
{
	return message != NULL
		   && ko_shared_v3_control_at_generation(message, message->ax_master_generation);
}

/* All operation-state transitions use the existing short KO lock. No
 * storage, membership, lock-master or transport operation runs under it. */
static bool
ko_peer_fence_key_matches(const ClusterKoPeerFenceV3 *entry, int32 source,
						  uint64 boot, uint64 batch)
{
	return entry->state != CLUSTER_KO_PEER_FENCE_EMPTY
		&& entry->request.base.origin_node == source
		&& entry->request.base.origin_boot == boot
		&& entry->request.base.batch_id == batch;
}

/* Recheck both at admission and at the original physical consumer. A
 * REQUEST already in the other inbound ring can outlive its COMMIT marker. */
static bool
ko_peer_request_ended_or_conflicts_v3(const ClusterKoSharedMessageV3 *request)
{
	uint8 encoded[CLUSTER_KO_SHARED_V3_BYTES];
	bool refused = false;

	if (!cluster_ko_shared_encode_v3(request, encoded, sizeof(encoded)))
		return true;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];

		if (!ko_peer_fence_key_matches(entry, request->base.origin_node,
									 request->base.origin_boot, request->base.batch_id))
			continue;
		refused = entry->state == CLUSTER_KO_PEER_FENCE_COMMITTED
			|| memcmp(entry->encoded, encoded, sizeof(encoded)) != 0;
		break;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return refused;
}

bool
cluster_ko_peer_admission_begin_v3(RelFileLocator locator, ClusterKoPeerAdmissionV3 *out)
{
	uint64 revision;
	bool allowed = true;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (ko_state == NULL || out == NULL || !cluster_shared_config
		|| cluster_interconnect_tier != CLUSTER_IC_TIER_1)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	if (revision == 0 || revision == UINT64_MAX)
		allowed = false;
	for (unsigned i = 0; allowed && i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];
		/* READY/COMMITTED do not reopen admission. Only a separately proven
		 * lifecycle retirement may remove this old-locator exclusion. */
		if (entry->state != CLUSTER_KO_PEER_FENCE_EMPTY
			&& RelFileLocatorEquals(entry->request.base.key.locator, locator))
			allowed = false;
	}
	if (allowed) {
		out->locator = locator;
		out->revision = revision;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return allowed;
}

bool
cluster_ko_peer_admission_current_v3(const ClusterKoPeerAdmissionV3 *admission,
									 RelFileLocator locator)
{
	/* Lock-free revalidation under a producer's header/entry lock avoids
	 * reversing the object-lock order. The caller publishes its reference
	 * before releasing that original lock; the later drain must observe it. */
	return ko_state != NULL && admission != NULL && cluster_shared_config
		   && cluster_interconnect_tier == CLUSTER_IC_TIER_1 && admission->cleaner.serial == 0
		   && admission->revision != 0 && admission->revision != UINT64_MAX
		   && RelFileLocatorEquals(admission->locator, locator)
		   && pg_atomic_read_u64(&ko_state->peer_admission_revision) == admission->revision;
}

static bool
ko_cleaner_scope_equal(const ClusterCtrcCleanerReadScopeV3 *a,
					   const ClusterCtrcCleanerReadScopeV3 *b)
{
	return a->serial == b->serial && a->pid == b->pid && a->worker == b->worker
		   && a->system_identifier == b->system_identifier
		   && a->formation_epoch == b->formation_epoch
		   && a->boot_incarnation == b->boot_incarnation;
}

/* Under the original object lock. A passive/terminal fence never grants
 * fresh cleanup work, even if the caller presents an old receipt. */
static bool
ko_cleaner_fences_allow_locked(RelFileLocator locator, const ClusterCtrcCleanerReadScopeV3 *scope,
							   bool *fenced)
{
	if (fenced != NULL)
		*fenced = false;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];
		if (entry->state == CLUSTER_KO_PEER_FENCE_EMPTY
			|| !RelFileLocatorEquals(entry->request.base.key.locator, locator))
			continue;
		if (fenced != NULL)
			*fenced = true;
		if (entry->state != CLUSTER_KO_PEER_FENCE_ACTIVE
			|| entry->request.base.key.system_identifier != scope->system_identifier
			|| entry->request.base.epoch != scope->formation_epoch
			|| entry->request.base.peer_boot != scope->boot_incarnation)
			return false;
	}
	return true;
}

static bool
ko_cleaner_pending_locked(RelFileLocator locator)
{
	for (unsigned i = 0; i < CLUSTER_CTRC_CLEANER_WORKERS; i++)
		if (ko_state->cleaner_work[i].used
			&& RelFileLocatorEquals(ko_state->cleaner_work[i].locator, locator))
			return true;
	return false;
}

bool
cluster_ko_cleaner_enter_v3(RelFileLocator locator, const ClusterCtrcCleanerReadScopeV3 *scope)
{
	ClusterKoCleanerWorkV3 *work;
	bool allowed = false, fenced = false;
	uint64 revision;

	/* Private and non-v3 configurations retain the original cleaner. */
	if (!cluster_enabled || !cluster_shared_config
		|| cluster_interconnect_tier != CLUSTER_IC_TIER_1)
		return true;
	if (ko_state == NULL || scope == NULL || scope->serial == 0 || scope->pid <= 0
		|| scope->worker >= CLUSTER_CTRC_CLEANER_WORKERS
		|| !cluster_ctrc_cleaner_read_scope_current_v3(scope, locator, SPACE_FORKNUM, 0))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	work = &ko_state->cleaner_work[scope->worker];
	/* No takeover by PID reuse, duplicate begin, or a replacement worker.
	 * An unexpected exit retains this exact obligation for original recovery. */
	revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	if (!work->used && revision != 0 && revision < UINT64_MAX - 1
		&& ko_cleaner_fences_allow_locked(locator, scope, &fenced)) {
		work->locator = locator;
		work->scope = *scope;
		work->used = true;
		/* A scan started before this finite producer must be retried. */
		if (fenced)
			(void)pg_atomic_fetch_add_u64(&ko_state->peer_admission_revision, 1);
		allowed = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return allowed;
}

void
cluster_ko_cleaner_leave_v3(RelFileLocator locator, const ClusterCtrcCleanerReadScopeV3 *scope)
{
	ClusterKoCleanerWorkV3 *work;
	bool wake = false, fenced = false;

	if (ko_state == NULL || scope == NULL || scope->serial == 0 || scope->pid != MyProcPid
		|| scope->worker >= CLUSTER_CTRC_CLEANER_WORKERS || MyBackendType != B_UNDO_CLEANER
		|| ClusterUndoCleanerWorkerIdForType(MyAuxProcType) != scope->worker)
		return;
	SpinLockAcquire(&ko_state->shared_lock);
	work = &ko_state->cleaner_work[scope->worker];
	if (work->used && RelFileLocatorEquals(work->locator, locator)
		&& ko_cleaner_scope_equal(&work->scope, scope)) {
		(void)ko_cleaner_fences_allow_locked(locator, scope, &fenced);
		if (fenced) {
			uint64 revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
			/* Saturation refuses further admission/READY; never wrap to an
			 * old scan generation. Ending the attempt cannot clear receipts. */
			pg_atomic_write_u64(&ko_state->peer_admission_revision,
								revision == 0 || revision == UINT64_MAX ? UINT64_MAX
																		: revision + 1);
		}
		memset(work, 0, sizeof(*work));
		wake = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	if (wake)
		cluster_sinval_set_proc_latch();
}

bool
cluster_ko_peer_read_begin_v3(RelFileLocator locator, ForkNumber fork, BlockNumber block,
							  ClusterKoPeerAdmissionV3 *out)
{
	ClusterCtrcCleanerReadScopeV3 scope;
	ClusterKoCleanerWorkV3 *work;
	uint64 revision;
	bool allowed = false;

	if (cluster_ko_peer_admission_begin_v3(locator, out))
		return true;
	if (out == NULL || ko_state == NULL || !cluster_shared_config
		|| cluster_interconnect_tier != CLUSTER_IC_TIER_1
		|| !cluster_ctrc_cleaner_read_scope_snapshot_v3(locator, fork, block, &scope)
		|| scope.worker >= CLUSTER_CTRC_CLEANER_WORKERS)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	work = &ko_state->cleaner_work[scope.worker];
	revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	if (revision != 0 && revision != UINT64_MAX && work->used
		&& RelFileLocatorEquals(work->locator, locator)
		&& ko_cleaner_scope_equal(&work->scope, &scope)
		&& ko_cleaner_fences_allow_locked(locator, &scope, NULL)) {
		out->locator = locator;
		out->revision = revision;
		out->fork = fork;
		out->block = block;
		out->cleaner = scope;
		allowed = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return allowed;
}

bool
cluster_ko_peer_read_current_v3(const ClusterKoPeerAdmissionV3 *admission, RelFileLocator locator,
								ForkNumber fork, BlockNumber block)
{
	if (admission == NULL || admission->cleaner.serial == 0)
		return cluster_ko_peer_admission_current_v3(admission, locator);
	/* The registered attempt prevents READY while this process-local scope
	 * is live. No object or CTRC lock is acquired under a buffer header. */
	return ko_state != NULL && cluster_shared_config
		   && cluster_interconnect_tier == CLUSTER_IC_TIER_1 && admission->revision != 0
		   && admission->revision != UINT64_MAX && admission->fork == fork
		   && admission->block == block && RelFileLocatorEquals(admission->locator, locator)
		   && cluster_ctrc_cleaner_read_scope_current_v3(&admission->cleaner, locator, fork, block)
		   && pg_atomic_read_u64(&ko_state->peer_admission_revision) == admission->revision;
}

bool
cluster_ko_peer_fence_begin_v3(const ClusterKoSharedMessageV3 *request,
							 ClusterKoPeerFenceHandleV3 *handle,
							 ClusterKoPeerFenceStateV3 *state)
{
	uint8 encoded[CLUSTER_KO_SHARED_V3_BYTES];
	uint64 peer_route_generation;
	int empty = -1;
	bool ok = false;

	if (ko_state == NULL || request == NULL || handle == NULL || state == NULL
		|| !cluster_shared_config || request->base.verb != CLUSTER_KO_SHARED_REQUEST
		|| (!AmLmonProcess() && !AmSinvalBcastProcess())
		|| request->base.peer_node != cluster_node_id
		|| cluster_grd_lookup_master_gen(&request->ax_resid, &peer_route_generation)
			   != request->ax_master_node
		|| !ko_shared_v3_control_at_generation(request, peer_route_generation)
		|| !cluster_ko_shared_encode_v3(request, encoded, sizeof(encoded)))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];

		if (entry->state == CLUSTER_KO_PEER_FENCE_EMPTY && empty < 0)
			empty = i;
		if (!ko_peer_fence_key_matches(entry, request->base.origin_node,
									 request->base.origin_boot, request->base.batch_id))
			continue;
		/* Same operation key with different target/AX/cut is a conflict,
		 * including a new epoch stamped onto an old operation identity. */
		if (entry->peer_route_generation == peer_route_generation
			&& memcmp(entry->encoded, encoded, sizeof(encoded)) == 0) {
			handle->slot = i;
			handle->serial = entry->serial;
			*state = entry->state;
			ok = true;
		}
		goto done;
	}
	if (empty >= 0 && ko_state->peer_fence_serial != UINT64_MAX
		&& pg_atomic_read_u64(&ko_state->peer_admission_revision) != 0
		&& pg_atomic_read_u64(&ko_state->peer_admission_revision) < UINT64_MAX - 1) {
		ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[empty];

		memset(entry, 0, sizeof(*entry));
		entry->request = *request;
		entry->peer_route_generation = peer_route_generation;
		memcpy(entry->encoded, encoded, sizeof(encoded));
		entry->serial = ++ko_state->peer_fence_serial;
		entry->state = CLUSTER_KO_PEER_FENCE_ACTIVE;
		/* Invalidate samples taken before this publication. No buffer or
		 * PCM lock is acquired while holding the KO lock. */
		(void)pg_atomic_fetch_add_u64(&ko_state->peer_admission_revision, 1);
		handle->slot = empty;
		handle->serial = entry->serial;
		*state = entry->state;
		ok = true;
	}
 done:
	SpinLockRelease(&ko_state->shared_lock);
	return ok;
}

/* Query state shares the exact peer slot, but cannot advance its drain or
 * terminal state. The LMON sends and the SI owner consumes the observation. */
static bool
ko_holder_probe_same_query(const ClusterGesHolderProbe *a, const ClusterGesHolderProbe *b)
{
	return a->nonce != 0 && a->nonce == b->nonce
		   && memcmp(&a->resid, &b->resid, sizeof(a->resid)) == 0
		   && memcmp(&a->holder, &b->holder, sizeof(a->holder)) == 0
		   && a->collector_generation == b->collector_generation
		   && a->collector_boot == b->collector_boot && a->master_boot == b->master_boot;
}

static bool
ko_peer_holder_cut(const ClusterKoSharedMessageV3 *request, uint64 peer_route_generation,
				   uint64 *master_boot)
{
	ClusterKoSharedMessageV2 current = request->base;
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], generation;

	if (!ko_shared_v3_control_at_generation(request, peer_route_generation)
		|| !ko_shared_members(&current, boots, &generation) || request->ax_master_node < 0
		|| request->ax_master_node >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| !cluster_membership_cut_generation_current(generation))
		return false;
	*master_boot = boots[request->ax_master_node];
	return *master_boot != 0 && boots[cluster_node_id] == request->base.peer_boot;
}

bool
cluster_ko_peer_holder_begin_v3(const ClusterKoPeerFenceHandleV3 *handle)
{
	ClusterKoSharedMessageV3 request;
	ClusterKoPeerFenceV3 *entry;
	ClusterGesHolderProbe probe;
	uint64 peer_route_generation;
	bool ok = false;

	if (ko_state == NULL || !AmSinvalBcastProcess() || handle == NULL
		|| handle->slot >= CLUSTER_KO_SHARED_CAPACITY || handle->serial == 0)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	entry = &ko_state->peer_fences[handle->slot];
	if (entry->serial != handle->serial || entry->state == CLUSTER_KO_PEER_FENCE_EMPTY) {
		SpinLockRelease(&ko_state->shared_lock);
		return false;
	}
	request = entry->request;
	peer_route_generation = entry->peer_route_generation;
	SpinLockRelease(&ko_state->shared_lock);
	memset(&probe, 0, sizeof(probe));
	if (!ko_peer_holder_cut(&request, peer_route_generation, &probe.master_boot))
		return false;
	probe.resid = request.ax_resid;
	probe.holder = request.ax_holder;
	probe.collector_generation = peer_route_generation;
	probe.collector_boot = request.base.peer_boot;
	SpinLockAcquire(&ko_state->shared_lock);
	entry = &ko_state->peer_fences[handle->slot];
	if (entry->serial == handle->serial && entry->state != CLUSTER_KO_PEER_FENCE_EMPTY
		&& ko_state->holder_probe_nonce < UINT64_MAX - 1) {
		probe.nonce = ++ko_state->holder_probe_nonce;
		entry->holder_probe = probe;
		entry->holder_sent_at = 0;
		ok = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	if (ok)
		cluster_lmon_wakeup();
	return ok;
}

void
cluster_ko_peer_holder_observed(int32 master, const ClusterGesHolderProbe *probe)
{
	bool wake = false;

	if (ko_state == NULL || !AmLmonProcess() || probe == NULL || !probe->reply
		|| (probe->observation == CLUSTER_GRD_HOLDER_PRESENT
				? probe->mode < AccessShareLock || probe->mode > AccessExclusiveLock
				: (probe->observation != CLUSTER_GRD_HOLDER_ABSENT
				   && probe->observation != CLUSTER_GRD_HOLDER_UNKNOWN)
					  || probe->mode != NoLock))
		return;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];

		if (entry->state != CLUSTER_KO_PEER_FENCE_EMPTY && entry->request.ax_master_node == master
			&& !entry->holder_probe.reply
			&& ko_holder_probe_same_query(&entry->holder_probe, probe)) {
			entry->holder_probe = *probe;
			wake = true;
			break;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
	if (wake)
		cluster_sinval_set_proc_latch();
}

bool
cluster_ko_peer_holder_result_v3(const ClusterKoPeerFenceHandleV3 *handle,
								 ClusterGrdHolderObservation *observation, LOCKMODE *mode)
{
	ClusterKoPeerFenceV3 saved;
	uint64 master_boot;
	bool current;

	if (observation != NULL)
		*observation = CLUSTER_GRD_HOLDER_UNKNOWN;
	if (mode != NULL)
		*mode = NoLock;
	if (ko_state == NULL || !AmSinvalBcastProcess() || handle == NULL
		|| handle->slot >= CLUSTER_KO_SHARED_CAPACITY || handle->serial == 0 || observation == NULL
		|| mode == NULL)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	saved = ko_state->peer_fences[handle->slot];
	SpinLockRelease(&ko_state->shared_lock);
	if (saved.serial != handle->serial || saved.state == CLUSTER_KO_PEER_FENCE_EMPTY
		|| !saved.holder_probe.reply
		|| !ko_peer_holder_cut(&saved.request, saved.peer_route_generation, &master_boot)
		|| master_boot != saved.holder_probe.master_boot)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	current = ko_state->peer_fences[handle->slot].serial == handle->serial
			  && ko_state->peer_fences[handle->slot].state != CLUSTER_KO_PEER_FENCE_EMPTY
			  && ko_holder_probe_same_query(&ko_state->peer_fences[handle->slot].holder_probe,
											&saved.holder_probe);
	SpinLockRelease(&ko_state->shared_lock);
	if (!current)
		return false;
	*observation = saved.holder_probe.observation;
	*mode = saved.holder_probe.mode;
	return true;
}

static void
ko_peer_holder_lmon_tick(void)
{
	TimestampTz now = GetCurrentTimestamp();
	unsigned sent = 0;

	for (unsigned scanned = 0; scanned < CLUSTER_KO_SHARED_CAPACITY && sent < 8; scanned++) {
		ClusterGesHolderProbe probe;
		ClusterKoSharedMessageV3 request;
		ClusterKoPeerFenceV3 *entry;
		uint64 master_boot;
		bool pending;

		SpinLockAcquire(&ko_state->shared_lock);
		entry = &ko_state->peer_fences[ko_state->holder_probe_cursor];
		ko_state->holder_probe_cursor
			= (ko_state->holder_probe_cursor + 1) % CLUSTER_KO_SHARED_CAPACITY;
		pending = entry->state != CLUSTER_KO_PEER_FENCE_EMPTY && entry->holder_probe.nonce != 0
				  && !entry->holder_probe.reply
				  && (entry->holder_sent_at == 0 || now < entry->holder_sent_at
					  || now - entry->holder_sent_at >= INT64CONST(100000));
		if (pending) {
			probe = entry->holder_probe;
			request = entry->request;
			entry->holder_sent_at = now;
		}
		SpinLockRelease(&ko_state->shared_lock);
		if (!pending)
			continue;
		if (!cluster_normal_stop_service_new_work(false))
			return;
		sent++;
		if (ko_peer_holder_cut(&request, probe.collector_generation, &master_boot)
			&& master_boot == probe.master_boot)
			(void)cluster_ges_holder_probe_send(request.ax_master_node, &probe);
	}
}

/* Called by the original drain owner only after its complete READY
 * conjunction. This state transition itself performs no drain or ACK. */
bool
cluster_ko_peer_fence_ready_v3(const ClusterKoSharedMessageV3 *request,
							 const ClusterKoPeerFenceHandleV3 *handle)
{
	uint8 encoded[CLUSTER_KO_SHARED_V3_BYTES];
	ClusterKoPeerFenceV3 *entry;
	uint64 peer_route_generation;
	bool ok = false;

	if (ko_state == NULL || request == NULL || handle == NULL || !AmSinvalBcastProcess()
		|| handle->slot >= CLUSTER_KO_SHARED_CAPACITY || handle->serial == 0
		|| !cluster_ko_shared_encode_v3(request, encoded, sizeof(encoded)))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	peer_route_generation = ko_state->peer_fences[handle->slot].peer_route_generation;
	SpinLockRelease(&ko_state->shared_lock);
	if (!ko_shared_v3_control_at_generation(request, peer_route_generation))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	entry = &ko_state->peer_fences[handle->slot];
	if (entry->serial == handle->serial && entry->peer_route_generation == peer_route_generation
		&& (entry->state == CLUSTER_KO_PEER_FENCE_ACTIVE
			|| entry->state == CLUSTER_KO_PEER_FENCE_READY)
		&& memcmp(entry->encoded, encoded, sizeof(encoded)) == 0
		&& !ko_cleaner_pending_locked(request->base.key.locator)
		&& pg_atomic_read_u64(&ko_state->peer_admission_revision) != 0
		&& pg_atomic_read_u64(&ko_state->peer_admission_revision) != UINT64_MAX) {
		entry->state = CLUSTER_KO_PEER_FENCE_READY;
		ok = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return ok;
}

bool
cluster_ko_peer_commit_v3(int32 source_node, uint64 epoch, uint64 origin_boot,
						 uint64 peer_boot, uint64 batch_id)
{
	bool ok = true;

	if (ko_state == NULL || !AmSinvalBcastProcess() || !cluster_shared_config
		|| source_node < 0 || source_node >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| source_node == cluster_node_id || epoch == 0 || origin_boot == 0
		|| epoch != cluster_epoch_get_current()
		|| peer_boot == 0 || batch_id == 0 || batch_id == UINT64_MAX)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];

		if (!ko_peer_fence_key_matches(entry, source_node, origin_boot, batch_id))
			continue;
		/* The immutable operation key is source/boot/batch. Publication
		 * retry may authenticate that same committed payload in a later
		 * epoch for surviving boots; it must never renew the old READY. */
		ok = entry->request.base.epoch <= epoch
			&& entry->request.base.peer_node == cluster_node_id
			&& entry->request.base.peer_boot == peer_boot
			&& (entry->state == CLUSTER_KO_PEER_FENCE_READY
				|| entry->state == CLUSTER_KO_PEER_FENCE_COMMITTED);
		if (ok)
			entry->state = CLUSTER_KO_PEER_FENCE_COMMITTED;
		break;
	}
	/* No new allocation, no PI/native completion, and no terminal GC here.
	 * Missing keys (e.g. duplicate publication after proven reclamation)
	 * cannot affect any other operation or manufacture a READY. */
	SpinLockRelease(&ko_state->shared_lock);
	return ok;
}

static bool
ko_shared_space_matches(const ClusterKoSharedMessageV2 *message,
						const ClusterSpaceIdentity *identity)
{
	return identity->state == CLUSTER_SPACE_IDENTITY_LIVE
		   && identity->key.system_identifier == message->key.system_identifier
		   && identity->key.database_incarnation == message->key.database_incarnation
		   && RelFileLocatorEquals(identity->key.locator, message->key.locator)
		   && memcmp(identity->key.storage_uuid, message->key.storage_uuid, 16) == 0
		   && memcmp(identity->incarnation, message->incarnation, 16) == 0;
}

static bool
ko_shared_space_current(const ClusterKoSharedMessageV2 *message)
{
	ClusterSpaceIdentity identity;

	return cluster_space_relation_read_identity(message->key.locator, &identity)
		   && ko_shared_space_matches(message, &identity);
}

/* The SI peer owns auxiliary maintenance work, not a backend-indexed S slot. */
static bool
ko_shared_peer_space_current(const ClusterKoSharedMessageV2 *message)
{
	ClusterSpaceIdentity identity;

	if (message == NULL || !cluster_shared_config
		|| message->verb != CLUSTER_KO_SHARED_REQUEST || message->peer_node != cluster_node_id
		|| MyBackendType != B_SINVAL_BCAST || CurrentResourceOwner == NULL || CritSectionCount != 0)
		return false;
	return cluster_space_relation_read_maintenance_identity(message->key.locator, &identity)
		   && ko_shared_space_matches(message, &identity);
}

bool
cluster_ko_shared_peer_projection_v2(const ClusterKoSharedMessageV2 *request, int32 peer,
									 ClusterKoSharedMessageV2 *out)
{
	ClusterKoSharedMessageV2 current, projected;
	ClusterWalSourceRef writer_before, writer_after;
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], generation, sampled_generation;
	uint32 capabilities, capability_generation;
	uint8 original[CLUSTER_KO_SHARED_V2_BYTES], observed[sizeof(original)];

	if (request == NULL || out == NULL || peer < 0 || peer >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| peer == cluster_node_id || request->verb != CLUSTER_KO_SHARED_REQUEST || !cluster_enabled
		|| !cluster_shared_config || RecoveryInProgress()
		|| (MyBackendType != B_BG_WRITER && MyBackendType != B_CHECKPOINTER)
		|| CurrentResourceOwner == NULL || CritSectionCount != 0)
		return false;
	generation = cluster_membership_cut_generation();
	if (generation == 0 || !cluster_wal_thread_current_v2_ref(&writer_before)
		|| !ko_shared_control_current(request))
		return false;
	current = *request;
	if (!ko_shared_members(&current, boots, &sampled_generation) || sampled_generation != generation
		|| boots[peer] == 0 || !cluster_ko_shared_encode_v2(request, original, sizeof(original))
		|| !cluster_ko_shared_encode_v2(&current, observed, sizeof(observed))
		|| memcmp(original, observed, sizeof(original)) != 0
		|| !cluster_sf_peer_capability_word_sample(peer, PGRAC_IC_HELLO_CAP_KO_SHARED_V2,
												   &capabilities, &capability_generation)
		|| capability_generation == 0 || (capabilities & PGRAC_IC_HELLO_CAP_KO_SHARED_V2) == 0)
		return false;
	projected = *request;
	/* An origin recipient verifies the same original request that the
	 * master received. It must never receive a fabricated self request. */
	if (peer != request->origin_node) {
		projected.peer_node = peer;
		projected.peer_boot = boots[peer];
	}
	if (!cluster_ko_shared_encode_v2(&projected, observed, sizeof(observed))
		|| !cluster_wal_thread_current_v2_ref(&writer_after)
		|| memcmp(&writer_before, &writer_after, sizeof(writer_before)) != 0
		|| !cluster_membership_cut_generation_current(generation))
		return false;
	*out = projected;
	return true;
}

/* The origin's local scope also covers a one-member cohort, for which no
 * remote message can be encoded. A completed barrier remains tied to the old
 * segment; publishing the structural successor does not rewrite that fact. */
static bool
ko_shared_origin_current(const ClusterKoSharedContext *context)
{
	const ClusterKoSharedMessageV2 *m;
	ClusterKoSharedMessageV2 current;
	ClusterWalSourceRef ref;
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], generation;

	if (context == NULL)
		return false;
	m = context->v3 ? &context->request_v3.base : &context->request;
	current = *m;
	if (context->v3 && !ko_shared_v3_control_current(&context->request_v3))
		return false;
	return cluster_enabled && cluster_shared_config && !RecoveryInProgress()
		   && cluster_node_id == m->origin_node && cluster_wal_thread_current_v2_ref(&ref)
		   && ref.claim.identity.origin_node_id == m->origin_node
		   && ref.claim.identity.origin_owner_incarnation == m->origin_boot
		   && ref.claim.identity.system_identifier == m->key.system_identifier
		   && ref.claim.database_incarnation == m->key.database_incarnation
		   && memcmp(ref.claim.identity.storage_uuid, m->key.storage_uuid, 16) == 0
		   && ko_shared_members(&current, boots, &generation)
		   && boots[cluster_node_id] == m->origin_boot && current.epoch == m->epoch
		   && memcmp(current.members, m->members, sizeof(m->members)) == 0
		   && memcmp(current.member_digest, m->member_digest, sizeof(m->member_digest)) == 0
		   && memcmp(boots, context->peer_boots, sizeof(boots)) == 0
		   && cluster_membership_cut_generation_current(generation);
}

/* Test pointer membership before dereferencing an opaque caller handle. */
static ClusterKoCompletionV2 **
ko_completion_link(const ClusterKoCompletionV2 *completion)
{
	ClusterKoCompletionV2 **link = &ko_completions;
	while (*link != NULL && *link != completion)
		link = &(*link)->next;
	return link;
}

/* PG's standard DSA lifetime, like pgstat's in-place area: mapping is local
 * to the backend, but a committed allocation outlives that backend. Never
 * attach, allocate, free or map a segment while holding the KO spinlock.
 * Author: SqlRush <sqlrush@gmail.com> */
static dsa_area *
ko_native_attach(void)
{
	if (ko_native_area == NULL) {
		MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);

		/* The checkpointer may attach without ever initiating a barrier. */
		if (!ko_exit_registered) {
			before_shmem_exit(ko_shared_backend_exit, (Datum)0);
			ko_exit_registered = true;
		}
		LWLockRegisterTranche(ko_state->native_dsa_tranche, "ClusterKoNativeOwner");
		ko_native_area = dsa_attach_in_place(ko_native_area_memory(), NULL);
		dsa_pin_mapping(ko_native_area);
		MemoryContextSwitchTo(oldcontext);
	}
	return ko_native_area;
}

static bool
ko_native_reserve(ClusterKoCompletionV2 *completion)
{
	dsa_area *area = ko_native_attach();

	Assert(!DsaPointerIsValid(completion->native_allocation));
	completion->native_allocation = dsa_allocate_extended(area, sizeof(ClusterKoNativeContinuation),
														  DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM);
	if (!DsaPointerIsValid(completion->native_allocation))
		return false;
	completion->native_reserved = dsa_get_address(area, completion->native_allocation);
	return true;
}

static void
ko_native_release_reservation(ClusterKoCompletionV2 *completion)
{
	if (DsaPointerIsValid(completion->native_allocation)) {
		dsa_free(ko_native_area, completion->native_allocation);
		completion->native_allocation = InvalidDsaPointer;
		completion->native_reserved = NULL;
	}
}

/* Only the original producer can abandon its still-unready continuation.
 * Published work is never cancelled by a transaction or ResourceOwner exit. */
static void
ko_native_abandon_unready(ClusterKoCompletionV2 *completion)
{
	ClusterKoNativeContinuation *work;
	if (!DsaPointerIsValid(completion->native_unready))
		return;
	work = dsa_get_address(ko_native_area, completion->native_unready);
	SpinLockAcquire(&ko_state->shared_lock);
	if (work->context.pid == completion->pid && work->context.serial == completion->serial
		&& !work->buffers_released) {
		work->recovery_required = true;
		ko_state->native_recovery_required = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	completion->native_unready = InvalidDsaPointer;
}

static void
ko_completion_cancel(ClusterKoCompletionV2 *completion)
{
	ko_native_abandon_unready(completion);
	ko_native_release_reservation(completion);
	if (ko_state != NULL && completion->slot < CLUSTER_KO_SHARED_CAPACITY) {
		ClusterKoSharedContext *entry;
		SpinLockAcquire(&ko_state->shared_lock);
		entry = &ko_state->contexts[completion->slot];
		if (entry->used && !entry->structure_owned && entry->pid == completion->pid
			&& entry->serial == completion->serial)
			memset(entry, 0, sizeof(*entry));
		SpinLockRelease(&ko_state->shared_lock);
	}
}

/* Native DDL may execute in a portal but finish at transaction commit. */
static bool
ko_native_transaction_current(void)
{
	if (CurTransactionResourceOwner == NULL)
		return false;
	for (ResourceOwner owner = CurrentResourceOwner; owner != NULL;
		 owner = ResourceOwnerGetParent(owner))
		if (owner == CurTransactionResourceOwner)
			return true;
	return false;
}

static bool
ko_completion_owner_current(const ClusterKoCompletionV2 *completion)
{
	if (completion->native_transaction)
		return completion->owner == CurTransactionResourceOwner && ko_native_transaction_current();
	return CurrentResourceOwner != NULL && completion->owner == CurrentResourceOwner;
}

void
cluster_ko_shared_release_v2(ClusterKoCompletionV2 **completion)
{
	ClusterKoCompletionV2 **link;
	if (completion == NULL || *completion == NULL)
		return;
	link = ko_completion_link(*completion);
	if (*link == NULL) {
		*completion = NULL; /* Its ResourceOwner already disposed of it. */
		return;
	}
	if ((*link)->pid != MyProcPid || !ko_completion_owner_current(*link))
		return;
	ko_completion_cancel(*link);
	*link = (*link)->next;
	pfree(*completion);
	*completion = NULL;
}

static void
ko_shared_xact_event(XactEvent event, void *arg pg_attribute_unused())
{
	/* ResourceOwnerRelease(true, true) is also used by PREPARE. Only the
	 * real native COMMIT event enters the pending-delete lifetime. */
	for (ClusterKoCompletionV2 *completion = ko_completions; completion != NULL;
		 completion = completion->next)
		if (completion->pid == MyProcPid && completion->native_transaction
			&& !completion->native_handed_off)
			completion->postcommit = event == XACT_EVENT_COMMIT && completion->space_observed
									 && !completion->native_pending
									 && completion->owner == TopTransactionResourceOwner
									 && CurrentResourceOwner == TopTransactionResourceOwner
									 && CurTransactionResourceOwner == TopTransactionResourceOwner;
}

static void
ko_shared_resource_release(ResourceReleasePhase phase, bool commit, bool top,
						   void *arg pg_attribute_unused())
{
	ClusterKoCompletionV2 **link = &ko_completions;
	ResourceOwner parent;
	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;
	parent = commit && !top ? ResourceOwnerGetParent(CurrentResourceOwner) : NULL;
	while (*link != NULL) {
		ClusterKoCompletionV2 *completion = *link;
		if (completion->pid == MyProcPid && completion->owner == CurrentResourceOwner) {
			/* A successful subtransaction does not finish its DDL. Keep the
			 * same barrier with the parent, just as native transaction locks
			 * survive; subabort still cancels it. */
			if (parent != NULL) {
				completion->owner = parent;
				link = &completion->next;
				continue;
			}
			/* Native pending deletes run after all ResourceOwner phases.
			 * Retain the same committed owner's deletion suppression even
			 * when its shared continuation requires recovery. An incomplete
			 * observation must not re-enable unqualified physical deletion. */
			if (commit && top && completion->native_transaction && completion->postcommit
				&& (completion->space_observed || completion->native_handed_off)
				&& !completion->native_pending && completion->owner == TopTransactionResourceOwner
				&& CurTransactionResourceOwner == TopTransactionResourceOwner) {
				link = &completion->next;
				continue;
			}
			ko_completion_cancel(completion);
			*link = completion->next;
			pfree(completion);
		} else
			link = &completion->next;
	}
}

static bool
ko_completion_snapshot(const ClusterKoCompletionV2 *completion, ClusterKoSharedContext *out)
{
	bool valid;
	const ClusterKoCompletionV2 *owned = *ko_completion_link(completion);
	if (owned == NULL || owned->native_handed_off || owned->pid != MyProcPid
		|| !ko_completion_owner_current(owned) || CurrentResourceOwner == NULL
		|| CritSectionCount != 0 || ko_state == NULL)
		return false;
	if (owned->barrier_private) {
		if (!owned->native_transaction || owned->slot != CLUSTER_KO_SHARED_CAPACITY
			|| owned->serial == 0)
			return false;
		memset(out, 0, sizeof(*out));
		out->used = out->complete = true;
		out->pid = owned->pid;
		out->serial = owned->serial;
		out->v3 = owned->barrier_v3;
		out->request = owned->barrier_v3 ? owned->barrier_request_v3.base
											: owned->barrier_request;
		if (owned->barrier_v3)
			out->request_v3 = owned->barrier_request_v3;
		memcpy(out->peer_boots, owned->barrier_boots, sizeof(out->peer_boots));
		return ko_shared_origin_current(out);
	}
	if (owned->slot >= CLUSTER_KO_SHARED_CAPACITY)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	*out = ko_state->contexts[owned->slot];
	valid = out->used && out->complete && !out->structure_owned && out->pid == owned->pid
			&& out->serial == owned->serial;
	SpinLockRelease(&ko_state->shared_lock);
	return valid && ko_shared_origin_current(out);
}

/* No public value can create this proof. Move only this process's exact
 * completed barrier, with its original full cut, before recycling the slot.
 * Backend/transaction cleanup still owns the preallocated opaque handle. */
static bool
ko_native_barrier_to_owner(ClusterKoCompletionV2 *completion)
{
	ClusterKoSharedContext before;
	bool moved = false;
	if (completion == NULL || !completion->native_transaction || completion->barrier_private
		|| !ko_completion_snapshot(completion, &before))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	if (memcmp(&ko_state->contexts[completion->slot], &before, sizeof(before)) == 0) {
		completion->barrier_v3 = before.v3;
		completion->barrier_request = before.request;
		if (before.v3)
			completion->barrier_request_v3 = before.request_v3;
		memcpy(completion->barrier_boots, before.peer_boots, sizeof(before.peer_boots));
		completion->barrier_private = true;
		memset(&ko_state->contexts[completion->slot], 0, sizeof(before));
		completion->slot = CLUSTER_KO_SHARED_CAPACITY;
		moved = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return moved;
}

bool
cluster_ko_shared_covers_v2(const ClusterKoCompletionV2 *completion,
							const ClusterSpaceIdentityKey *key, const uint8 incarnation[16])
{
	ClusterKoSharedContext context;
	return key != NULL && incarnation != NULL && ko_completion_snapshot(completion, &context)
		   && context.request.key.system_identifier == key->system_identifier
		   && context.request.key.database_incarnation == key->database_incarnation
		   && memcmp(context.request.key.storage_uuid, key->storage_uuid, 16) == 0
		   && RelFileLocatorEquals(context.request.key.locator, key->locator)
		   && memcmp(context.request.incarnation, incarnation, 16) == 0;
}

bool
cluster_ko_shared_observe_space_v2(ClusterKoCompletionV2 *completion,
								   const struct ClusterPageWalBindingV1 *terminal, const void *wal,
								   Size wal_length)
{
	ClusterKoCompletionV2 *owned = *ko_completion_link(completion);
	ClusterSpaceStructureChange change;
	ClusterWalSourceRef current;
	const ClusterSpaceIdentityKey *key;
	if (owned == NULL || !owned->native_transaction || owned->native_pending
		|| owned->space_observed || !cluster_page_wal_binding_shape_v1(terminal)
		|| terminal->flags != CLUSTER_PAGE_WAL_NATIVE_FLUSHED
		|| terminal->identity.forknum != SPACE_FORKNUM || terminal->identity.blockno != 0
		|| !cluster_space_structure_wal_decode(wal, wal_length, &change)
		|| !cluster_wal_thread_current_v2_ref(&current)
		|| memcmp(&current, &terminal->source, sizeof(current)) != 0)
		return false;
	key = &change.identity.result.key;
	if (key->system_identifier != terminal->identity.system_identifier
		|| key->database_incarnation != terminal->source.claim.database_incarnation
		|| memcmp(key->storage_uuid, terminal->identity.storage_uuid, 16) != 0
		|| !RelFileLocatorEquals(key->locator, terminal->identity.locator)
		|| change.identity.result_token != terminal->version.mutation_token
		|| memcmp(change.identity.result.incarnation, terminal->version.segment_incarnation, 16)
			   != 0)
		return false;
	if (change.identity.action == CLUSTER_SPACE_WAL_TRUNCATE) {
		if (terminal->rmid != RM_SMGR_ID
			|| (terminal->info & ~XLR_INFO_MASK) != XLOG_SMGR_SPACE_IDENTITY)
			return false;
	} else if (change.identity.action == CLUSTER_SPACE_WAL_TOMBSTONE) {
		if (terminal->rmid != RM_XACT_ID || (terminal->info & XLOG_XACT_OPMASK) != XLOG_XACT_COMMIT
			|| (terminal->info & XLOG_XACT_HAS_INFO) == 0)
			return false;
	} else
		return false;
	if (!cluster_ko_shared_covers_v2(owned, &change.identity.expected.key,
									 change.identity.expected.incarnation))
		return false;
	owned->terminal = *terminal;
	memcpy(owned->structure, wal, sizeof(owned->structure));
	owned->space_observed = true;
	return true;
}

bool
cluster_ko_shared_space_observation_v2(const ClusterKoCompletionV2 *completion,
									   struct ClusterPageWalBindingV1 *terminal, void *wal,
									   Size wal_length)
{
	const ClusterKoCompletionV2 *owned = *ko_completion_link(completion);
	ClusterKoSharedContext context;
	ClusterWalSourceRef current;
	if (owned == NULL || !owned->space_observed || terminal == NULL || wal == NULL
		|| wal_length != sizeof(owned->structure) || !ko_completion_snapshot(owned, &context)
		|| !cluster_wal_thread_current_v2_ref(&current)
		|| memcmp(&current, &owned->terminal.source, sizeof(current)) != 0)
		return false;
	*terminal = owned->terminal;
	memcpy(wal, owned->structure, sizeof(owned->structure));
	return true;
}

bool
cluster_ko_shared_observe_truncate_v2(ClusterKoCompletionV2 *completion)
{
	ClusterKoCompletionV2 *owned = *ko_completion_link(completion);
	ClusterPageWalBindingV1 terminal;
	ClusterSpaceStructureChange change;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	if (owned == NULL || owned->truncate_observed || owned->postcommit
		|| !cluster_ko_shared_space_observation_v2(owned, &terminal, wal, sizeof(wal))
		|| !cluster_space_structure_wal_decode(wal, sizeof(wal), &change)
		|| change.identity.action != CLUSTER_SPACE_WAL_TRUNCATE)
		return false;
	/* Only the original native finish calls this after its physical sync
	 * and exact SPACE readback. The saved KO/WAL scope cannot be replaced
	 * by a wire flag or by DROP's earlier SPACE-only observation. */
	owned->truncate_observed = true;
	return true;
}

bool
cluster_ko_shared_truncate_observation_v2(const ClusterKoCompletionV2 *completion,
										  struct ClusterPageWalBindingV1 *terminal, void *wal,
										  Size wal_length)
{
	const ClusterKoCompletionV2 *owned = *ko_completion_link(completion);

	return owned != NULL && owned->truncate_observed
		   && cluster_ko_shared_space_observation_v2(owned, terminal, wal, wal_length);
}

bool
cluster_ko_shared_observe_drop_v2(ClusterKoCompletionV2 *completion)
{
	ClusterKoCompletionV2 *owned = *ko_completion_link(completion), *borrowed = NULL;
	ClusterPageWalBindingV1 terminal;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	if (owned == NULL || owned->drop_observed || !owned->postcommit
		|| !cluster_ko_shared_space_observation_v2(owned, &terminal, wal, sizeof(wal))
		|| !cluster_ko_shared_pending_drop_v2(terminal.identity.locator, &borrowed)
		|| borrowed != owned)
		return false;
	/* The original storage owner reports its physical result only after the
	 * exact COMMIT-DROP's durable namespace operation. Re-borrowing the same
	 * unique top-transaction handle also rejects ambiguous retained barriers.
	 * No raw physical-result flag or replacement owner can create this fact. */
	owned->drop_observed = true;
	return true;
}

static bool
ko_structure_effect_observation(const ClusterKoCompletionV2 *completion,
								ClusterPageWalBindingV1 *terminal, void *wal, Size wal_length)
{
	const ClusterKoCompletionV2 *owned = *ko_completion_link(completion);

	return owned != NULL && (owned->truncate_observed || owned->drop_observed)
		   && cluster_ko_shared_space_observation_v2(owned, terminal, wal, wal_length);
}

bool
cluster_ko_shared_structure_offer_v2(const ClusterKoCompletionV2 *completion, int32 peer,
									 struct ClusterPiWritebackFactV2 *out)
{
	const ClusterKoCompletionV2 *owned = *ko_completion_link(completion);
	ClusterPiWritebackFactV2 value = { 0 };
	ClusterPiStructuralFactV2 *s = &value.proof.structural;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	if (out == NULL || owned == NULL || !owned->postcommit
		|| !ko_structure_effect_observation(owned, &s->terminal.binding, wal, sizeof(wal))
		|| !cluster_ko_shared_read_v2(owned, peer, &s->ko)
		|| !cluster_space_structure_wal_decode(wal, sizeof(wal), &s->change))
		return false;
	value.kind = CLUSTER_PI_WRITEBACK_STRUCTURE_OFFER_V2;
	s->durability_flags
		= CLUSTER_PI_STRUCTURAL_WAL_FLUSHED | CLUSTER_PI_STRUCTURAL_SPACE_SYNC_READBACK
		  | CLUSTER_PI_STRUCTURAL_KO_ALL_ACKED | CLUSTER_PI_STRUCTURAL_EFFECT_DURABLE;
	if (s->change.identity.action == CLUSTER_SPACE_WAL_TRUNCATE)
		s->durability_flags |= CLUSTER_PI_STRUCTURAL_BASE_DURABLE;
	/* This value has no page/master cut. It can only transfer the completed
	 * relation result to the original peer's background owner; it cannot
	 * acknowledge a page or extend this transaction's local ownership. */
	*out = value;
	return true;
}

bool
cluster_ko_shared_structure_handoff_v2(ClusterKoCompletionV2 **completion)
{
	ClusterKoCompletionV2 **link, *owned;
	ClusterKoSharedContext before;
	ClusterPageWalBindingV1 terminal;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	bool transferred = false;
	ClusterKoSharedContext *entry = NULL;

	if (completion == NULL || *completion == NULL)
		return false;
	link = ko_completion_link(*completion);
	owned = *link;
	if (owned == NULL || !owned->postcommit
		|| !ko_structure_effect_observation(owned, &terminal, wal, sizeof(wal))
		|| !ko_completion_snapshot(owned, &before))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	if (owned->barrier_private) {
		/* Full leaves the original proof and handle untouched. The original
		 * producer must keep servicing this handoff before ending its owner;
		 * no allocation or remote wait occurs under this lock. */
		for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
			if (!ko_state->contexts[i].used && ko_state->context_serial != UINT64_MAX) {
				entry = &ko_state->contexts[i];
				*entry = before;
				entry->serial = ++ko_state->context_serial;
				break;
			}
	} else if (memcmp(&ko_state->contexts[owned->slot], &before, sizeof(before)) == 0)
		entry = &ko_state->contexts[owned->slot];
	if (entry != NULL) {
		entry->terminal = terminal;
		memcpy(entry->structure, wal, sizeof(wal));
		entry->structure_owned = true;
		transferred = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	if (!transferred)
		return false;
	/* From this point even an exit before local cleanup must preserve the
	 * original shared obligation. There is no caller-supplied cancel token. */
	*link = owned->next;
	ko_native_release_reservation(owned);
	pfree(owned);
	*completion = NULL;
	return true;
}

/* Retain the original committed proof even if its cut has since gone stale.
 * Retention is not authority to execute: the background consumer must recheck
 * the entire original scope before doing anything. Preallocation makes this
 * postcommit transfer independent of active-slot capacity and allocator I/O. */
static bool
ko_native_publish(ClusterKoCompletionV2 *owned, bool from_commit_tail)
{
	ClusterKoNativeContinuation *work;
	ClusterSpaceStructureChange change;
	bool qualified;

	if (ko_state == NULL
		|| (MyBackendType != B_BACKEND && MyBackendType != B_AUTOVAC_WORKER
			&& MyBackendType != B_BG_WORKER)
		|| CritSectionCount != 0 || CurrentResourceOwner == NULL
		|| TopTransactionResourceOwner == NULL
		|| (!from_commit_tail && CurrentResourceOwner != TopTransactionResourceOwner)
		|| CurTransactionResourceOwner != TopTransactionResourceOwner)
		return false;
	if (owned == NULL || owned->pid != MyProcPid || owned->owner != TopTransactionResourceOwner
		|| !owned->native_transaction || !owned->barrier_private || owned->native_pending
		|| owned->native_handed_off || !owned->postcommit || owned->serial == 0
		|| owned->native_reserved == NULL || !DsaPointerIsValid(owned->native_allocation))
		return false;
	qualified
		= owned->space_observed
		  && cluster_space_structure_wal_decode(owned->structure, sizeof(owned->structure), &change)
		  && (change.identity.action == CLUSTER_SPACE_WAL_TOMBSTONE
			  || (change.identity.action == CLUSTER_SPACE_WAL_TRUNCATE
				  && owned->truncate_observed));
	if (!from_commit_tail
		&& (!qualified
			|| (change.identity.action == CLUSTER_SPACE_WAL_TOMBSTONE && !owned->drop_observed
				&& !owned->native_buffers_released)))
		return false;
	work = owned->native_reserved;
	memset(&work->context, 0, sizeof(work->context));
	work->context.used = work->context.complete = true;
	work->context.structure_owned = qualified;
	work->recovery_required = !qualified;
	work->buffers_released = qualified
							 && (change.identity.action == CLUSTER_SPACE_WAL_TRUNCATE
								 || owned->drop_observed || owned->native_buffers_released);
	work->context.structure_drop_pending = qualified
										   && change.identity.action == CLUSTER_SPACE_WAL_TOMBSTONE
										   && !owned->drop_observed;
	work->context.pid = owned->pid;
	work->context.serial = owned->serial;
	work->context.v3 = owned->barrier_v3;
	work->context.request = owned->barrier_v3 ? owned->barrier_request_v3.base
											: owned->barrier_request;
	if (owned->barrier_v3)
		work->context.request_v3 = owned->barrier_request_v3;
	memcpy(work->context.peer_boots, owned->barrier_boots, sizeof(owned->barrier_boots));
	work->context.terminal = owned->terminal;
	memcpy(work->context.structure, owned->structure, sizeof(owned->structure));
	if (!work->buffers_released)
		owned->native_unready = owned->native_allocation;
	SpinLockAcquire(&ko_state->shared_lock);
	if (work->recovery_required)
		ko_state->native_recovery_required = true;
	work->next = ko_state->native_incoming;
	ko_state->native_incoming = owned->native_allocation;
	ko_state->native_waiting++;
	SpinLockRelease(&ko_state->shared_lock);
	owned->native_allocation = InvalidDsaPointer;
	owned->native_reserved = NULL;
	owned->native_handed_off = true;
	return true;
}

bool
cluster_ko_shared_native_handoff_v2(ClusterKoCompletionV2 **completion)
{
	ClusterKoCompletionV2 **link, *owned;

	if (completion == NULL || *completion == NULL)
		return false;
	link = ko_completion_link(*completion);
	owned = *link;
	if (!ko_native_publish(owned, false))
		return false;
	*link = owned->next;
	pfree(owned);
	*completion = NULL;
	return true;
}

/* Called only by the native irreversible COMMIT tail, before any callback
 * may fail. Keep the original local handle until pending-delete cleanup;
 * its shared continuation no longer belongs to transaction cleanup. */
void
cluster_ko_shared_native_commit_v2(void)
{
	if (!cluster_enabled || !cluster_shared_config
		|| (cluster_ic_local_capability_word() & PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2) == 0)
		return;
	for (ClusterKoCompletionV2 *owned = ko_completions; owned != NULL; owned = owned->next) {
		if (!owned->native_transaction || owned->native_pending || owned->native_handed_off)
			continue;
		/* A claimed SPACE change must have saved its actual record and
		 * preallocated continuation before this irreversible point. */
		owned->postcommit = true;
		if (!ko_native_publish(owned, true))
			elog(PANIC, "committed shared structural change lost its original completion owner");
	}
}

/* Called at the native all-buffers-retired boundary, before storage cleanup.
 * The still-unready allocation cannot be freed by the checkpointer. Marking
 * it ready and surrendering the producer pointer share the original lock. */
void
cluster_ko_shared_native_drop_buffers_released_v2(RelFileLocator locator)
{
	if (ko_state == NULL || TopTransactionResourceOwner == NULL
		|| CurTransactionResourceOwner != TopTransactionResourceOwner || CritSectionCount != 0)
		return;
	for (ClusterKoCompletionV2 *owned = ko_completions; owned != NULL; owned = owned->next) {
		ClusterSpaceStructureChange change;
		ClusterKoNativeContinuation *work;

		if (owned->pid != MyProcPid || owned->owner != TopTransactionResourceOwner
			|| !owned->native_transaction || owned->native_pending || !owned->postcommit
			|| !owned->space_observed
			|| !RelFileLocatorEquals(owned->terminal.identity.locator, locator)
			|| !cluster_space_structure_wal_decode(owned->structure, sizeof(owned->structure),
												   &change)
			|| change.identity.action != CLUSTER_SPACE_WAL_TOMBSTONE)
			continue;
		owned->native_buffers_released = true;
		if (!DsaPointerIsValid(owned->native_unready))
			continue;
		work = dsa_get_address(ko_native_area, owned->native_unready);
		SpinLockAcquire(&ko_state->shared_lock);
		if (work->context.pid == owned->pid && work->context.serial == owned->serial
			&& memcmp(&work->context.terminal, &owned->terminal, sizeof(owned->terminal)) == 0
			&& memcmp(work->context.structure, owned->structure, sizeof(owned->structure)) == 0) {
			owned->native_unready = InvalidDsaPointer;
			work->buffers_released = true;
		}
		SpinLockRelease(&ko_state->shared_lock);
	}
}

/* Retained work suppresses the old backend pathname operation, even after
 * the live cut changes. This observation grants no physical I/O or receipt. */
bool
cluster_ko_shared_native_drop_deferred_v2(RelFileLocator locator)
{
	for (ClusterKoCompletionV2 *owned = ko_completions; owned != NULL; owned = owned->next) {
		ClusterSpaceStructureChange change;

		if (owned->pid == MyProcPid && owned->native_handed_off
			&& RelFileLocatorEquals((owned->barrier_v3 ? owned->barrier_request_v3.base.key.locator
															 : owned->barrier_request.key.locator), locator)
			&& (!owned->space_observed
				|| (cluster_space_structure_wal_decode(owned->structure, sizeof(owned->structure),
													   &change)
					&& change.identity.action == CLUSTER_SPACE_WAL_TOMBSTONE)))
			return true;
	}
	return false;
}

static void
ko_native_deferred(const ClusterKoSharedContext *context, uint64 *reported, const char *stage)
{
	if (!context->used || context->serial == 0
		|| (cluster_ic_local_capability_word() & PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2) == 0)
		return;
	KO_BUMP(failclosed_count);
	if (*reported != context->serial) {
		*reported = context->serial;
		elog(LOG,
			 "PGRAC_FAMILY=KO action=STRUCTURE_DEFERRED stage=%s "
			 "resource=%u/%u/%u serial=" UINT64_FORMAT " epoch=" UINT64_FORMAT,
			 stage, context->request.key.locator.spcOid, context->request.key.locator.dbOid,
			 context->request.key.locator.relNumber, context->serial, context->request.epoch);
	}
}

/* One original checkpointer step. New arrivals never preempt the finite
 * ready batch. A full active region, stale proof or lost cut retains all
 * bytes. Moving a DROP without effect cannot expose a structural receipt. */
ClusterKoStructurePollV2
cluster_ko_shared_native_promote_v2(void)
{
	dsa_pointer selected;
	ClusterKoNativeContinuation *work;
	ClusterKoSharedContext before;
	ClusterWalSourceRef current;
	int slot = -1;
	bool moved = false;
	bool failed, buffers_released;

	if (ko_state == NULL || MyBackendType != B_CHECKPOINTER || CurrentResourceOwner == NULL
		|| CritSectionCount != 0 || !cluster_enabled || !cluster_shared_config)
		return CLUSTER_KO_STRUCTURE_INVALID;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (!ko_state->contexts[i].used) {
			slot = i;
			break;
		}
	if (slot >= 0 && !DsaPointerIsValid(ko_state->native_ready)) {
		ko_state->native_ready = ko_state->native_incoming;
		ko_state->native_incoming = InvalidDsaPointer;
	}
	selected = ko_state->native_ready;
	SpinLockRelease(&ko_state->shared_lock);
	if (slot < 0)
		return CLUSTER_KO_STRUCTURE_PENDING;
	if (!DsaPointerIsValid(selected))
		return CLUSTER_KO_STRUCTURE_RELEASED;
	work = dsa_get_address(ko_native_attach(), selected);
	SpinLockAcquire(&ko_state->shared_lock);
	before = work->context;
	failed = work->recovery_required;
	buffers_released = work->buffers_released;
	SpinLockRelease(&ko_state->shared_lock);
	if (failed || !ko_shared_origin_current(&before) || !cluster_wal_thread_current_v2_ref(&current)
		|| memcmp(&current, &before.terminal.source, sizeof(current)) != 0) {
		SpinLockAcquire(&ko_state->shared_lock);
		if (ko_state->native_ready == selected && ko_state->native_waiting != 0) {
			ko_state->native_ready = work->next;
			work->next = ko_state->native_failed;
			ko_state->native_failed = selected;
			work->recovery_required = true;
			ko_state->native_recovery_required = true;
			ko_state->native_waiting--;
		}
		SpinLockRelease(&ko_state->shared_lock);
		ko_native_deferred(&before, &ko_native_report_serial,
						   failed ? "PRODUCER_INCOMPLETE" : "PROMOTION_CUT");
		return CLUSTER_KO_STRUCTURE_INVALID;
	}
	if (!buffers_released) {
		/* Let the rest of this finite batch advance while the original
		 * producer completes its local buffer cleanup. No storage may run. */
		SpinLockAcquire(&ko_state->shared_lock);
		if (ko_state->native_ready == selected) {
			ko_state->native_ready = work->next;
			work->next = ko_state->native_incoming;
			ko_state->native_incoming = selected;
		}
		SpinLockRelease(&ko_state->shared_lock);
		return CLUSTER_KO_STRUCTURE_PENDING;
	}
	SpinLockAcquire(&ko_state->shared_lock);
	if (ko_state->native_ready == selected && !ko_state->contexts[slot].used
		&& ko_state->context_serial != UINT64_MAX && ko_state->native_waiting != 0
		&& memcmp(&work->context, &before, sizeof(before)) == 0) {
		ko_state->contexts[slot] = before;
		ko_state->contexts[slot].serial = ++ko_state->context_serial;
		ko_state->native_ready = work->next;
		ko_state->native_waiting--;
		moved = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	if (!moved)
		return CLUSTER_KO_STRUCTURE_PENDING;
	dsa_free(ko_native_area, selected);
	return CLUSTER_KO_STRUCTURE_PROGRESS;
}

bool
cluster_ko_shared_native_poll_v2(void)
{
	bool progress, completed = false;

	if (ko_state == NULL || !cluster_enabled || !cluster_shared_config
		|| MyBackendType != B_CHECKPOINTER || CurrentResourceOwner == NULL || CritSectionCount != 0
		|| (cluster_ic_local_capability_word() & PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2) == 0)
		return false;
	progress = cluster_ko_shared_native_promote_v2() == CLUSTER_KO_STRUCTURE_PROGRESS;
	/* No WAL input, content/entry lock or remote wait spans physical work.
	 * A failing item still advances this cursor; wrapping waits another tick. */
	if (cluster_smgr_drop_work_poll(&ko_drop_scan, &completed)) {
		if (!completed) {
			ClusterKoSharedContext context;
			uint32 slot = ko_drop_scan - 1;

			Assert(ko_drop_scan > 0 && ko_drop_scan <= CLUSTER_KO_SHARED_CAPACITY);
			SpinLockAcquire(&ko_state->shared_lock);
			context = ko_state->contexts[slot];
			SpinLockRelease(&ko_state->shared_lock);
			ko_native_deferred(&context, &ko_drop_report_serial[slot], "DROP_WORK");
		}
		progress |= completed;
	} else
		ko_drop_scan = 0;
	return progress;
}

static bool
ko_drop_context_current(const ClusterKoSharedContext *context)
{
	ClusterSpaceStructureChange change;
	ClusterWalSourceRef current;

	return context->used && context->complete && context->structure_owned
		   && context->structure_drop_pending && !context->structure_drop_failed
		   && context->serial != 0 && context->request.origin_node == cluster_node_id
		   && cluster_space_structure_wal_decode(context->structure, sizeof(context->structure),
												 &change)
		   && change.identity.action == CLUSTER_SPACE_WAL_TOMBSTONE
		   && cluster_page_wal_binding_shape_v1(&context->terminal)
		   && context->terminal.flags == CLUSTER_PAGE_WAL_NATIVE_FLUSHED
		   && ko_shared_origin_current(context) && cluster_wal_thread_current_v2_ref(&current)
		   && memcmp(&current, &context->terminal.source, sizeof(current)) == 0;
}

static bool
ko_drop_actor_current(void)
{
	return ko_state != NULL && cluster_enabled && cluster_shared_config
		   && MyBackendType == B_CHECKPOINTER && MyProcPid > 0 && CurrentResourceOwner != NULL
		   && CritSectionCount == 0;
}

static bool
ko_drop_work_owned(const ClusterKoDropWorkV2 *work)
{
	uint32 slot;

	if (work == NULL || MyBackendType != B_CHECKPOINTER || MyProcPid <= 0
		|| CurrentResourceOwner == NULL || CritSectionCount != 0)
		return false;
	/* Authenticate pointer membership before dereferencing caller memory. */
	for (slot = 0; slot < CLUSTER_KO_SHARED_CAPACITY; slot++)
		if (ko_drop_work[slot] == work)
			break;
	return slot < CLUSTER_KO_SHARED_CAPACITY && work->slot == slot && work->pid == MyProcPid
		   && work->owner == CurrentResourceOwner;
}

bool
cluster_ko_shared_drop_work_revalidate_v2(const ClusterKoDropWorkV2 *work)
{
	ClusterKoSharedContext current;
	bool exact;
	uint32 slot;

	if (!ko_drop_actor_current() || !ko_drop_work_owned(work) || work->abandoned)
		return false;
	slot = work->slot;
	SpinLockAcquire(&ko_state->shared_lock);
	current = ko_state->contexts[slot];
	SpinLockRelease(&ko_state->shared_lock);
	if (current.drop_executor_pid != MyProcPid
		|| memcmp(&current, &work->context, sizeof(current)) != 0
		|| !ko_drop_context_current(&current))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	exact = memcmp(&current, &ko_state->contexts[slot], sizeof(current)) == 0;
	SpinLockRelease(&ko_state->shared_lock);
	return exact;
}

void *
cluster_ko_shared_drop_work_abandon_v2(ClusterKoDropWorkV2 *work, Size storage_bytes)
{
	if (!ko_drop_work_owned(work) || work->storage_bytes != storage_bytes)
		return NULL;
	/* Keep the shared obligation and original executor stamp; neither a
	 * restored cut nor another process may resume. Only this exact original
	 * context may receive the negative normal-stop diagnostic. A replaced
	 * slot belongs to a different work, even though its old fd needs cleanup. */
	work->abandoned = true;
	if (ko_state != NULL) {
		SpinLockAcquire(&ko_state->shared_lock);
		if (memcmp(&ko_state->contexts[work->slot], &work->context, sizeof(work->context)) == 0)
			ko_state->contexts[work->slot].structure_drop_failed = true;
		SpinLockRelease(&ko_state->shared_lock);
	}
	return (char *)work + MAXALIGN(sizeof(*work));
}

bool
cluster_ko_shared_drop_work_abandon_next_v2(uint32 *cursor, Size storage_bytes,
											ClusterKoDropWorkV2 **out)
{
	if (cursor == NULL || *cursor >= CLUSTER_KO_SHARED_CAPACITY || out == NULL || *out != NULL
		|| storage_bytes == 0)
		return false;
	for (uint32 slot = *cursor; slot < CLUSTER_KO_SHARED_CAPACITY; slot++) {
		ClusterKoDropWorkV2 *work = ko_drop_work[slot];
		ClusterKoSharedContext current;
		uint64 epoch, boot;

		if (!ko_drop_work_owned(work) || work->storage_bytes != storage_bytes)
			continue;
		if (!work->abandoned) {
			if (ko_state == NULL)
				continue;
			SpinLockAcquire(&ko_state->shared_lock);
			current = ko_state->contexts[slot];
			SpinLockRelease(&ko_state->shared_lock);
			epoch = cluster_epoch_get_current();
			boot = cluster_qvotec_get_self_incarnation();
			/* Require a positive replacement, not a transient admission or
			 * unavailable-witness result from the normal revalidation. */
			if (memcmp(&current, &work->context, sizeof(current)) == 0
				&& (epoch == 0 || epoch == work->context.request.epoch)
				&& (boot == 0 || boot == work->context.request.origin_boot))
				continue;
		}
		if (cluster_ko_shared_drop_work_abandon_v2(work, storage_bytes) == NULL)
			continue;
		*out = work;
		*cursor = slot + 1;
		return true;
	}
	return false;
}

bool
cluster_ko_shared_drop_work_begin_v2(uint32 *cursor, Size storage_bytes, ClusterKoDropWorkV2 **out)
{
	if (!ko_drop_actor_current() || cursor == NULL || *cursor >= CLUSTER_KO_SHARED_CAPACITY
		|| out == NULL || *out != NULL || storage_bytes == 0
		|| storage_bytes > MaxAllocSize - MAXALIGN(sizeof(ClusterKoDropWorkV2)))
		return false;
	for (uint32 slot = *cursor; slot < CLUSTER_KO_SHARED_CAPACITY; slot++) {
		ClusterKoDropWorkV2 *work = ko_drop_work[slot];
		ClusterKoSharedContext before;
		bool claimed = false;

		if (work != NULL) {
			if (work->storage_bytes != storage_bytes
				|| !cluster_ko_shared_drop_work_revalidate_v2(work)) {
				ko_native_deferred(&work->context, &ko_drop_report_serial[slot],
								   work->abandoned ? "DROP_RECOVERY_REQUIRED" : "DROP_CUT");
				continue;
			}
		} else {
			SpinLockAcquire(&ko_state->shared_lock);
			before = ko_state->contexts[slot];
			SpinLockRelease(&ko_state->shared_lock);
			if (before.drop_executor_pid != 0 || !ko_drop_context_current(&before)) {
				if (before.structure_drop_pending)
					ko_native_deferred(&before, &ko_drop_report_serial[slot], "DROP_OWNER");
				continue;
			}
			if (!ko_exit_registered) {
				before_shmem_exit(ko_shared_backend_exit, (Datum)0);
				ko_exit_registered = true;
			}
			work = MemoryContextAllocZero(TopMemoryContext,
										  add_size(MAXALIGN(sizeof(*work)), storage_bytes));
			work->owner = CurrentResourceOwner;
			work->pid = MyProcPid;
			work->slot = slot;
			work->storage_bytes = storage_bytes;
			work->context = before;
			work->context.drop_executor_pid = MyProcPid;
			SpinLockAcquire(&ko_state->shared_lock);
			if (memcmp(&before, &ko_state->contexts[slot], sizeof(before)) == 0) {
				ko_state->contexts[slot].drop_executor_pid = MyProcPid;
				ko_drop_work[slot] = work;
				claimed = true;
			}
			SpinLockRelease(&ko_state->shared_lock);
			if (!claimed) {
				pfree(work);
				continue;
			}
		}
		*out = work;
		*cursor = slot + 1;
		return true;
	}
	return false;
}

void *
cluster_ko_shared_drop_work_state_v2(const ClusterKoDropWorkV2 *work, Size storage_bytes)
{
	if (!cluster_ko_shared_drop_work_revalidate_v2(work) || work->storage_bytes != storage_bytes)
		return NULL;
	return (char *)work + MAXALIGN(sizeof(*work));
}

bool
cluster_ko_shared_drop_work_read_v2(const ClusterKoDropWorkV2 *work,
									struct ClusterPageWalBindingV1 *terminal, void *wal,
									Size wal_length)
{
	if (terminal == NULL || wal == NULL || wal_length != CLUSTER_SPACE_STRUCTURE_WAL_BYTES
		|| !cluster_ko_shared_drop_work_revalidate_v2(work))
		return false;
	*terminal = work->context.terminal;
	memcpy(wal, work->context.structure, wal_length);
	return true;
}

bool
cluster_ko_shared_drop_work_finish_v2(ClusterKoDropWorkV2 **work)
{
	ClusterKoDropWorkV2 *owned;
	bool finished = false;

	if (work == NULL || !cluster_ko_shared_drop_work_revalidate_v2(*work))
		return false;
	owned = *work;
	SpinLockAcquire(&ko_state->shared_lock);
	if (memcmp(&ko_state->contexts[owned->slot], &owned->context, sizeof(owned->context)) == 0) {
		ko_state->contexts[owned->slot].structure_drop_pending = false;
		ko_state->contexts[owned->slot].drop_executor_pid = 0;
		ko_drop_work[owned->slot] = NULL;
		finished = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	if (!finished)
		return false;
	pfree(owned);
	*work = NULL;
	return true;
}

static bool
ko_shared_import_current(const ClusterKoSharedContext *context)
{
	ClusterKoSharedMessageV2 current;
	const ClusterWalSourceRef *source;
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], generation;
	bool cut_current;

	if (context == NULL)
		return false;
	current = context->request;
	source = &context->terminal.source;
	if (context->v3) {
		if (!ko_shared_v3_control_current(&context->request_v3))
			return false;
		/* v3 has its own negotiated capability and AX suffix. Do not fall
		 * through to the legacy v2 cut predicate, which would re-admit the
		 * old capability family on the peer import path. */
		cut_current = true;
	} else
		cut_current = cluster_ko_shared_cut_current_v2(&current);

	return cluster_node_id == current.peer_node && cluster_node_id != current.origin_node
		   && source->claim.identity.origin_node_id == current.origin_node
		   && source->claim.identity.origin_owner_incarnation == current.origin_boot
		   && cut_current
		   && ko_shared_members(&current, boots, &generation)
		   && memcmp(boots, context->peer_boots, sizeof(boots)) == 0
		   && cluster_membership_cut_generation_current(generation);
}

bool
cluster_ko_shared_structure_accept_v2(const ClusterPiWritebackNoticeV1 *notice, uint32 index)
{
	ClusterPiWritebackFactV2 fact;
	ClusterKoSharedContext incoming = { 0 };
	uint64 revision, generation;
	bool accepted = false;
	int free_slot = -1;

	if (ko_state == NULL || MyBackendType != B_BG_WRITER || CurrentResourceOwner == NULL
		|| CritSectionCount != 0 || MyProcPid <= 0
		|| !cluster_pi_writeback_structure_offer_read_v2(notice, index, &revision, &fact)
		|| revision == 0 || fact.kind != CLUSTER_PI_WRITEBACK_STRUCTURE_OFFER_V2)
		return false;
	incoming.request = fact.proof.structural.ko;
	incoming.terminal = fact.proof.structural.terminal.binding;
	if (incoming.request.peer_node != cluster_node_id
		|| incoming.request.origin_node == cluster_node_id
		|| !cluster_space_structure_wal_encode(&fact.proof.structural.change, incoming.structure,
											   sizeof(incoming.structure))
		|| !ko_shared_members(&incoming.request, incoming.peer_boots, &generation)
		|| memcmp(&incoming.request, &fact.proof.structural.ko, sizeof(incoming.request)) != 0
		|| !ko_shared_import_current(&incoming)
		|| !cluster_membership_cut_generation_current(generation))
		return false;
	incoming.used = incoming.complete = incoming.structure_owned = true;
	incoming.pid = MyProcPid;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoSharedContext *entry = &ko_state->contexts[i];
		if (!entry->used) {
			if (free_slot < 0)
				free_slot = i;
			continue;
		}
		if (entry->request.origin_node != incoming.request.origin_node
			|| entry->request.origin_boot != incoming.request.origin_boot
			|| entry->request.epoch != incoming.request.epoch
			|| entry->request.batch_id != incoming.request.batch_id)
			continue;
		accepted = entry->complete && entry->structure_owned && entry->serial != 0
				   && memcmp(&entry->request, &incoming.request, sizeof(entry->request)) == 0
				   && memcmp(entry->peer_boots, incoming.peer_boots, sizeof(entry->peer_boots)) == 0
				   && memcmp(&entry->terminal, &incoming.terminal, sizeof(entry->terminal)) == 0
				   && memcmp(entry->structure, incoming.structure, sizeof(entry->structure)) == 0;
		goto done;
	}
	if (free_slot >= 0 && ko_state->context_serial != UINT64_MAX) {
		incoming.serial = ++ko_state->context_serial;
		ko_state->contexts[free_slot] = incoming;
		accepted = true;
	}
done:
	SpinLockRelease(&ko_state->shared_lock);
	return accepted;
}

static bool
ko_shared_structure_snapshot(uint32 slot, ClusterKoSharedContext *out)
{
	ClusterKoSharedContext context;
	ClusterWalSourceRef current;

	if (ko_state == NULL || slot >= CLUSTER_KO_SHARED_CAPACITY
		|| (MyBackendType != B_BG_WRITER && MyBackendType != B_CHECKPOINTER)
		|| CurrentResourceOwner == NULL || CritSectionCount != 0)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	context = ko_state->contexts[slot];
	SpinLockRelease(&ko_state->shared_lock);
	if (!context.used || !context.complete || !context.structure_owned
		|| context.structure_drop_pending || context.serial == 0
		|| (context.request.origin_node == cluster_node_id
				? (!ko_shared_origin_current(&context)
				   || !cluster_wal_thread_current_v2_ref(&current)
				   || memcmp(&current, &context.terminal.source, sizeof(current)) != 0)
				: !ko_shared_import_current(&context)))
		return false;
	*out = context;
	return true;
}

bool
cluster_ko_shared_structure_observation_v2(uint32 slot, uint64 serial,
										   struct ClusterPageWalBindingV1 *terminal, void *wal,
										   Size wal_length)
{
	ClusterKoSharedContext context;
	if (serial == 0 || terminal == NULL || wal == NULL
		|| wal_length != CLUSTER_SPACE_STRUCTURE_WAL_BYTES
		|| !ko_shared_structure_snapshot(slot, &context) || context.serial != serial)
		return false;
	*terminal = context.terminal;
	memcpy(wal, context.structure, sizeof(context.structure));
	return true;
}

bool
cluster_ko_shared_structure_next_v2(uint32 *cursor, uint64 *serial,
									ClusterPageWalBindingV1 *terminal)
{
	if (cursor == NULL || serial == NULL || terminal == NULL
		|| *cursor >= CLUSTER_KO_SHARED_CAPACITY)
		return false;
	for (uint32 i = *cursor; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoSharedContext context;
		if (!ko_shared_structure_snapshot(i, &context))
			continue;
		*cursor = i + 1;
		*serial = context.serial;
		*terminal = context.terminal;
		return true;
	}
	return false;
}

bool
cluster_ko_shared_structure_peer_v2(uint32 slot, uint64 serial, int32 peer,
									ClusterKoSharedMessageV2 *out)
{
	ClusterKoSharedContext context;
	return out != NULL && serial != 0 && ko_shared_structure_snapshot(slot, &context)
		   && context.serial == serial
		   && cluster_ko_shared_peer_projection_v2(&context.request, peer, out);
}

ClusterKoStructurePollV2
cluster_ko_shared_structure_finish_local_v2(uint32 slot, uint64 serial)
{
	ClusterKoSharedContext before, current;
	ResourceXGateSnapshot gate, after;
	ClusterPcmPiRelationScanV2 scanned;
	ClusterKoStructurePollV2 result = CLUSTER_KO_STRUCTURE_INVALID;
	uint32 cursor;
	uint16 expected = 0;
	bool reset;

	if (serial == 0 || MyBackendType != B_CHECKPOINTER
		|| !ko_shared_structure_snapshot(slot, &before) || before.serial != serial
		|| !cluster_pcm_lock_resource_x_gate_snapshot(&gate) || gate.phase != RESOURCE_X_GATE_OPEN
		|| gate.formation != before.request.epoch)
		return result;
	if (before.request.origin_node == cluster_node_id) {
		for (unsigned peer = 0; peer < CLUSTER_KO_SHARED_NODE_LIMIT; peer++)
			if (peer != cluster_node_id && before.peer_boots[peer] != 0)
				expected |= (uint16)(1u << peer);
		if ((before.structure_peers_accepted & ~expected) != 0)
			return result;
		if (before.structure_peers_accepted != expected)
			return CLUSTER_KO_STRUCTURE_PENDING;
	}
	cursor = before.structure_scan;
	reset = cursor != 0 && before.structure_scan_freeze != gate.freeze_generation;
	if (reset) {
		cursor = 0;
		scanned = CLUSTER_PCM_PI_RELATION_MORE;
	} else {
		/* No KO lock, CF pin or remote wait spans the directory observer.
		 * The original barrier has ended all old buffer producers. */
		scanned = cluster_pcm_lock_pi_relation_scan_v2(before.request.key.locator,
													   before.request.epoch, 128, &cursor);
	}
	if (scanned == CLUSTER_PCM_PI_RELATION_INVALID || (!reset && cursor < before.structure_scan)
		|| !ko_shared_structure_snapshot(slot, &current)
		|| memcmp(&before, &current, sizeof(before)) != 0
		|| !cluster_pcm_lock_resource_x_gate_snapshot(&after)
		|| memcmp(&gate, &after, sizeof(gate)) != 0)
		return result;
	SpinLockAcquire(&ko_state->shared_lock);
	if (memcmp(&ko_state->contexts[slot], &before, sizeof(before)) == 0) {
		if (scanned == CLUSTER_PCM_PI_RELATION_EMPTY) {
			/* Every remaining peer has its own original owner. This removes
			 * only completed local work, never a PI, file or retained WAL. */
			memset(&ko_state->contexts[slot], 0, sizeof(before));
			result = CLUSTER_KO_STRUCTURE_RELEASED;
		} else {
			ko_state->contexts[slot].structure_scan = cursor;
			ko_state->contexts[slot].structure_scan_freeze = gate.freeze_generation;
			result = scanned == CLUSTER_PCM_PI_RELATION_PENDING ? CLUSTER_KO_STRUCTURE_PENDING
																: CLUSTER_KO_STRUCTURE_PROGRESS;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
	return result;
}

bool
cluster_ko_shared_structure_offer_next_v2(uint32 *cursor, int32 peer, uint64 *serial,
										  struct ClusterPiWritebackFactV2 *out)
{
	if (cursor == NULL || serial == NULL || out == NULL || *cursor >= CLUSTER_KO_SHARED_CAPACITY
		|| peer < 0 || peer >= CLUSTER_KO_SHARED_NODE_LIMIT || peer == cluster_node_id)
		return false;
	for (uint32 i = *cursor; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoSharedContext context;
		ClusterPiWritebackFactV2 value = { 0 };
		ClusterPiStructuralFactV2 *s = &value.proof.structural;

		if (!ko_shared_structure_snapshot(i, &context)
			|| context.request.origin_node != cluster_node_id || context.peer_boots[peer] == 0
			|| (context.structure_peers_accepted & (1u << peer)) != 0)
			continue;
		s->ko = context.request;
		s->ko.peer_node = peer;
		s->ko.peer_boot = context.peer_boots[peer];
		if (!ko_shared_control_current(&s->ko)
			|| !cluster_space_structure_wal_decode(context.structure, sizeof(context.structure),
												   &s->change)
			|| (s->change.identity.action != CLUSTER_SPACE_WAL_TRUNCATE
				&& s->change.identity.action != CLUSTER_SPACE_WAL_TOMBSTONE))
			continue;
		value.kind = CLUSTER_PI_WRITEBACK_STRUCTURE_OFFER_V2;
		s->terminal.binding = context.terminal;
		s->durability_flags
			= CLUSTER_PI_STRUCTURAL_WAL_FLUSHED | CLUSTER_PI_STRUCTURAL_SPACE_SYNC_READBACK
			  | CLUSTER_PI_STRUCTURAL_KO_ALL_ACKED | CLUSTER_PI_STRUCTURAL_EFFECT_DURABLE;
		if (s->change.identity.action == CLUSTER_SPACE_WAL_TRUNCATE)
			s->durability_flags |= CLUSTER_PI_STRUCTURAL_BASE_DURABLE;
		*out = value;
		*serial = context.serial;
		*cursor = i + 1;
		return true;
	}
	return false;
}

/* A transport ACK only says that the original remote background owner now
 * retains this relation result. Keep the local KO and all page obligations. */
bool
cluster_ko_shared_structure_offer_complete_v2(uint32 slot, uint64 serial,
											  const ClusterPiWritebackJobV1 *job)
{
	ClusterKoSharedContext before;
	ClusterWalWriterToken peer;
	int32 node;
	bool accepted = false;

	if (serial == 0 || !ko_shared_structure_snapshot(slot, &before) || before.serial != serial
		|| before.request.origin_node != cluster_node_id
		|| !cluster_pi_writeback_structure_offer_ack_v2(job, slot, serial, &peer))
		return false;
	node = peer.ref.claim.identity.origin_node_id;
	if (node < 0 || node >= CLUSTER_KO_SHARED_NODE_LIMIT || node == cluster_node_id
		|| peer.epoch != before.request.epoch || peer.startup_first_lsn != 0
		|| peer.ref.claim.identity.origin_owner_incarnation != before.peer_boots[node]
		|| before.peer_boots[node] == 0)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	if (memcmp(&ko_state->contexts[slot], &before, sizeof(before)) == 0) {
		ko_state->contexts[slot].structure_peers_accepted |= (uint16)(1u << node);
		accepted = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return accepted;
}

bool
cluster_ko_shared_pending_drop_v2(RelFileLocator locator, ClusterKoCompletionV2 **out)
{
	ClusterKoCompletionV2 *candidate = NULL;
	if (out == NULL || *out != NULL || CurrentResourceOwner == NULL
		|| CurrentResourceOwner != TopTransactionResourceOwner
		|| CurTransactionResourceOwner != TopTransactionResourceOwner)
		return false;
	for (ClusterKoCompletionV2 *completion = ko_completions; completion != NULL;
		 completion = completion->next) {
		ClusterPageWalBindingV1 terminal;
		ClusterSpaceStructureChange change;
		uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		if (!completion->native_transaction || !completion->postcommit
			|| !cluster_ko_shared_space_observation_v2(completion, &terminal, wal, sizeof(wal))
			|| !RelFileLocatorEquals(terminal.identity.locator, locator)
			|| !cluster_space_structure_wal_decode(wal, sizeof(wal), &change)
			|| change.identity.action != CLUSTER_SPACE_WAL_TOMBSTONE)
			continue;
		if (candidate != NULL)
			return false;
		candidate = completion;
	}
	if (candidate == NULL)
		return false;
	*out = candidate;
	return true;
}

/* A pending-delete return does not certify physical effects. Published native
 * continuations are independent of these local handles; cancel releases only
 * a reservation that was never handed off. Do not require a still-live cut. */
void
cluster_ko_shared_postcommit_cleanup_v2(void)
{
	ClusterKoCompletionV2 **link = &ko_completions;
	if (TopTransactionResourceOwner == NULL
		|| CurTransactionResourceOwner != TopTransactionResourceOwner)
		return;
	while (*link != NULL) {
		ClusterKoCompletionV2 *completion = *link;
		if (completion->pid == MyProcPid && completion->owner == TopTransactionResourceOwner
			&& completion->native_transaction && completion->postcommit) {
			ko_completion_cancel(completion);
			*link = completion->next;
			pfree(completion);
		} else
			link = &completion->next;
	}
}

/* Only the original native wrapper offers a completion for SPACE to take.
 * Keep the ResourceOwner throughout; taking it does not complete the DDL or
 * convey COMMIT/durability. Ambiguous repeated barriers are not authority. */
bool
cluster_ko_shared_claim_v2(const ClusterSpaceIdentityKey *key, const uint8 incarnation[16],
						   ClusterKoCompletionV2 **out)
{
	ClusterKoCompletionV2 *candidate = NULL;
	if (out == NULL || *out != NULL || key == NULL || incarnation == NULL)
		return false;
	for (ClusterKoCompletionV2 *completion = ko_completions; completion != NULL;
		 completion = completion->next) {
		if (!completion->native_pending
			|| !cluster_ko_shared_covers_v2(completion, key, incarnation))
			continue;
		if (candidate != NULL)
			return false;
		candidate = completion;
	}
	if (candidate == NULL || !cluster_ko_shared_covers_v2(candidate, key, incarnation))
		return false;
	candidate->native_pending = false;
	*out = candidate;
	return true;
}

bool
cluster_ko_shared_read_v2(const ClusterKoCompletionV2 *completion, int32 peer,
						  ClusterKoSharedMessageV2 *out)
{
	ClusterKoSharedContext context;
	ClusterKoSharedMessageV2 request;
	if (out == NULL || peer < 0 || peer >= CLUSTER_KO_SHARED_NODE_LIMIT || peer == cluster_node_id
		|| !ko_completion_snapshot(completion, &context) || context.peer_boots[peer] == 0)
		return false;
	request = context.request;
	request.peer_node = peer;
	request.peer_boot = context.peer_boots[peer];
	if (!ko_shared_control_current(&request))
		return false;
	*out = request;
	return true;
}

static bool
ko_shared_enqueue(const ClusterKoSharedMessageV2 *message)
{
	bool accepted = false;
	uint8 bytes[CLUSTER_KO_SHARED_V2_BYTES];

	if (ko_state == NULL || !cluster_ko_shared_encode_v2(message, bytes, sizeof(bytes)))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	if (ko_state->send_count < CLUSTER_KO_SHARED_CAPACITY) {
		ko_state->send[(ko_state->send_head + ko_state->send_count) % CLUSTER_KO_SHARED_CAPACITY]
			= *message;
		ko_state->send_count++;
		accepted = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	if (accepted)
		cluster_lmon_wakeup();
	return accepted;
}

static bool
ko_shared_enqueue_v3(const ClusterKoSharedMessageV3 *message, uint64 local_generation)
{
	bool accepted = false;
	uint8 bytes[CLUSTER_KO_SHARED_V3_BYTES];

	if (ko_state == NULL || local_generation == 0 || local_generation == UINT64_MAX
		|| !cluster_ko_shared_encode_v3(message, bytes, sizeof(bytes)))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	if (ko_state->send_v3_count < CLUSTER_KO_SHARED_CAPACITY) {
		uint32 tail
			= (ko_state->send_v3_head + ko_state->send_v3_count) % CLUSTER_KO_SHARED_CAPACITY;
		ko_state->send_v3[tail] = *message;
		ko_state->send_v3_route_generation[tail] = local_generation;
		ko_state->send_v3_count++;
		accepted = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	if (accepted)
		cluster_lmon_wakeup();
	return accepted;
}

/* A queued request whose original backend has unwound must not be newly
 * dispatched. Already accepted frames remain subject to peer cut/identity
 * checks; clearing this context never manufactures a completion ACK. */
static bool
ko_shared_request_owned(const ClusterKoSharedMessageV2 *message)
{
	ClusterKoSharedMessageV2 ack = *message;
	bool owned = false;

	if (message->verb != CLUSTER_KO_SHARED_REQUEST)
		return true;
	ack.verb = CLUSTER_KO_SHARED_ACK;
	ack.status = CLUSTER_KO_SHARED_DONE;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoSharedContext *entry = &ko_state->contexts[i];
		ClusterKoSharedMessageV2 expected;
		if (!entry->used || entry->v3 || entry->complete || entry->request.batch_id != message->batch_id
			|| message->peer_node >= CLUSTER_KO_SHARED_NODE_LIMIT)
			continue;
		expected = entry->request;
		expected.peer_node = message->peer_node;
		expected.peer_boot = entry->peer_boots[message->peer_node];
		owned = cluster_ko_shared_ack_matches_v2(&expected, &ack);
		break;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return owned;
}

static bool
ko_shared_v3_request_owned(const ClusterKoSharedMessageV3 *message)
{
	ClusterKoSharedMessageV3 ack = *message;
	bool owned = false;

	if (message->base.verb != CLUSTER_KO_SHARED_REQUEST)
		return true;
	ack.base.verb = CLUSTER_KO_SHARED_ACK;
	ack.base.status = CLUSTER_KO_SHARED_DONE;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoSharedContext *entry = &ko_state->contexts[i];
		ClusterKoSharedMessageV3 expected;
		if (!entry->used || !entry->v3 || entry->complete
			|| entry->request_v3.base.batch_id != message->base.batch_id
			|| message->base.peer_node >= CLUSTER_KO_SHARED_NODE_LIMIT
			|| ((entry->request_sent_peers | entry->request_uncertain_peers)
				& (1u << message->base.peer_node))
				   != 0)
			continue;
		expected = entry->request_v3;
		expected.base.peer_node = message->base.peer_node;
		expected.base.peer_boot = entry->peer_boots[message->base.peer_node];
		owned = cluster_ko_shared_ack_matches_v3(&expected, &ack);
		break;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return owned;
}

/* DONE and WOULD_BLOCK transfer the exact frame to transport. An ambiguous
 * hard error must not retry it on another carrier. Neither result is READY. */
typedef enum KoSendOwnership {
	KO_SEND_CALLER_OWNED,
	KO_SEND_TRANSPORT_OWNED,
	KO_SEND_UNKNOWN
} KoSendOwnership;

static void
ko_shared_v3_record_ownership(const ClusterKoSharedMessageV3 *message, KoSendOwnership ownership)
{
	ClusterKoSharedMessageV3 ack = *message;

	if (message->base.verb != CLUSTER_KO_SHARED_REQUEST
		|| message->base.peer_node >= CLUSTER_KO_SHARED_NODE_LIMIT)
		return;
	ack.base.verb = CLUSTER_KO_SHARED_ACK;
	ack.base.status = CLUSTER_KO_SHARED_DONE;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoSharedContext *entry = &ko_state->contexts[i];
		ClusterKoSharedMessageV3 expected;

		if (!entry->used || !entry->v3 || entry->complete
			|| entry->request_v3.base.batch_id != message->base.batch_id)
			continue;
		expected = entry->request_v3;
		expected.base.peer_node = message->base.peer_node;
		expected.base.peer_boot = entry->peer_boots[message->base.peer_node];
		if (cluster_ko_shared_ack_matches_v3(&expected, &ack)) {
			uint16 peer = (uint16)(1u << message->base.peer_node);
			if (ownership == KO_SEND_TRANSPORT_OWNED) {
				entry->request_sent_peers |= peer;
				entry->request_uncertain_peers &= ~peer;
			} else if (ownership == KO_SEND_CALLER_OWNED)
				entry->request_uncertain_peers &= ~peer;
			else
				entry->request_uncertain_peers |= peer;
		}
		break;
	}
	SpinLockRelease(&ko_state->shared_lock);
}

static void
ko_shared_backend_exit(int code, Datum arg)
{
	uint64 batches[CLUSTER_KO_SHARED_CAPACITY];
	unsigned count = 0;

	if (ko_state == NULL)
		return;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (ko_state->contexts[i].used && !ko_state->contexts[i].structure_owned
			&& ko_state->contexts[i].pid == MyProcPid) {
			if (!ko_state->contexts[i].complete)
				batches[count++] = ko_state->contexts[i].request.batch_id;
			memset(&ko_state->contexts[i], 0, sizeof(ko_state->contexts[i]));
		}
	SpinLockRelease(&ko_state->shared_lock);
	for (unsigned i = 0; i < count; i++)
		cluster_sinval_ack_wait_remove(batches[i]);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (ko_drop_work[i] != NULL && ko_drop_work[i]->pid == MyProcPid) {
			/* Do not clear the shared executor or effect state: any partial
			 * physical operation belongs to the original recovery obligation. */
			pfree(ko_drop_work[i]);
			ko_drop_work[i] = NULL;
		}
	/* Exit owns all this process's ResourceOwners, not just its current one. */
	while (ko_completions != NULL) {
		ClusterKoCompletionV2 *completion = ko_completions;
		ko_completions = completion->next;
		ko_native_abandon_unready(completion);
		ko_native_release_reservation(completion);
		pfree(completion);
	}
	if (ko_native_area != NULL) {
		dsa_detach(ko_native_area);
		dsa_release_in_place(ko_native_area_memory());
		ko_native_area = NULL;
	}
}

void
cluster_ko_lmon_tick_v2(void)
{
	uint32 attempts;

	if (ko_state == NULL || !cluster_shared_config || !AmLmonProcess())
		return;
	ko_peer_holder_lmon_tick();
	for (unsigned n = 0; n < CLUSTER_KO_SHARED_CAPACITY; n++) {
		ClusterKoSharedMessageV2 message;
		uint8 bytes[CLUSTER_KO_SHARED_V2_BYTES];
		bool pending;

		SpinLockAcquire(&ko_state->shared_lock);
		pending = ko_state->send_count != 0;
		if (pending) {
			message = ko_state->send[ko_state->send_head];
			ko_state->send_head = (ko_state->send_head + 1) % CLUSTER_KO_SHARED_CAPACITY;
			ko_state->send_count--;
		}
		SpinLockRelease(&ko_state->shared_lock);
		if (!pending)
			break;
		/* The IC owner retains accepted WOULD_BLOCK sends. Other failures
		 * cannot produce an ACK; the original barrier remains unfulfilled. */
		if (ko_shared_request_owned(&message) && ko_shared_control_current(&message)
			&& cluster_ko_shared_encode_v2(&message, bytes, sizeof(bytes)))
			(void)cluster_ic_send_envelope(
				message.verb == CLUSTER_KO_SHARED_REQUEST ? PGRAC_IC_MSG_KO_FLUSH
														  : PGRAC_IC_MSG_KO_FLUSH_ACK,
				message.verb == CLUSTER_KO_SHARED_REQUEST ? message.peer_node : message.origin_node,
				bytes, sizeof(bytes));
	}
	SpinLockAcquire(&ko_state->shared_lock);
	attempts = Min(ko_state->send_v3_count, CLUSTER_KO_SHARED_CAPACITY);
	SpinLockRelease(&ko_state->shared_lock);
	for (unsigned n = 0; n < attempts; n++) {
		ClusterKoSharedMessageV3 message;
		uint64 local_generation;
		uint8 bytes[CLUSTER_KO_SHARED_V3_BYTES];
		ClusterICSendResult result = CLUSTER_IC_SEND_HARD_ERROR;
		bool pending;

		SpinLockAcquire(&ko_state->shared_lock);
		pending = ko_state->send_v3_count != 0;
		if (pending) {
			message = ko_state->send_v3[ko_state->send_v3_head];
			local_generation = ko_state->send_v3_route_generation[ko_state->send_v3_head];
		}
		SpinLockRelease(&ko_state->shared_lock);
		if (!pending)
			return;
		if (ko_shared_v3_request_owned(&message)
			&& ko_shared_v3_control_at_generation(&message, local_generation)
			&& cluster_ko_shared_encode_v3(&message, bytes, sizeof(bytes))) {
			/* ERROR may leave transport acceptance unknown. Arm the original
			 * operation before entering transport; only its explicit return
			 * can prove refusal or admission. A later tick cannot resend it. */
			ko_shared_v3_record_ownership(&message, KO_SEND_UNKNOWN);
			result = cluster_ic_send_envelope(
				message.base.verb == CLUSTER_KO_SHARED_REQUEST ? PGRAC_IC_MSG_KO_FLUSH
															   : PGRAC_IC_MSG_KO_FLUSH_ACK,
				message.base.verb == CLUSTER_KO_SHARED_REQUEST ? message.base.peer_node
															   : message.base.origin_node,
				bytes, sizeof(bytes));
			ko_shared_v3_record_ownership(&message, result == CLUSTER_IC_SEND_NOT_ADMITTED
														? KO_SEND_CALLER_OWNED
													: (result == CLUSTER_IC_SEND_DONE
													   || result == CLUSTER_IC_SEND_WOULD_BLOCK)
														? KO_SEND_TRANSPORT_OWNED
														: KO_SEND_UNKNOWN);
		}
		/* The original queue keeps the slot across the send. Only a refused
		 * frame rotates to its tail; process at most the captured count so a
		 * blocked peer does not busy-spin or prevent other peers' progress. */
		SpinLockAcquire(&ko_state->shared_lock);
		ko_state->send_v3_head = (ko_state->send_v3_head + 1) % CLUSTER_KO_SHARED_CAPACITY;
		ko_state->send_v3_count--;
		if (result == CLUSTER_IC_SEND_NOT_ADMITTED) {
			uint32 tail
				= (ko_state->send_v3_head + ko_state->send_v3_count) % CLUSTER_KO_SHARED_CAPACITY;
			ko_state->send_v3[tail] = message;
			ko_state->send_v3_route_generation[tail] = local_generation;
			ko_state->send_v3_count++;
		}
		SpinLockRelease(&ko_state->shared_lock);
	}
}

ClusterNormalStopPollResult
cluster_ko_shared_normal_stop_poll_v2(const char **reason)
{
	ClusterNormalStopPollResult result = CLUSTER_NORMAL_STOP_READY;
	const char *why = "KO_SHARED_EMPTY";

	if (ko_state == NULL) {
		if (reason != NULL)
			*reason = "KO_SHARED_UNINITIALIZED";
		return CLUSTER_NORMAL_STOP_INVALID;
	}
	SpinLockAcquire(&ko_state->shared_lock);
	if (ko_state->send_count > CLUSTER_KO_SHARED_CAPACITY
		|| ko_state->send_head >= CLUSTER_KO_SHARED_CAPACITY
		|| ko_state->send_v3_count > CLUSTER_KO_SHARED_CAPACITY
		|| ko_state->send_v3_head >= CLUSTER_KO_SHARED_CAPACITY) {
		result = CLUSTER_NORMAL_STOP_INVALID;
		why = "KO_SHARED_QUEUE_INVALID";
	} else if (ko_state->native_recovery_required || DsaPointerIsValid(ko_state->native_failed)) {
		result = CLUSTER_NORMAL_STOP_INVALID;
		why = "KO_SHARED_COMMITTED_RECOVERY_REQUIRED";
	} else if (ko_state->native_waiting != 0 || DsaPointerIsValid(ko_state->native_incoming)
			   || DsaPointerIsValid(ko_state->native_ready)) {
		result = CLUSTER_NORMAL_STOP_PENDING;
		why = "KO_SHARED_COMMITTED_OWNED";
	} else if (ko_state->send_count != 0 || ko_state->send_v3_count != 0) {
		result = CLUSTER_NORMAL_STOP_PENDING;
		why = "KO_SHARED_SEND_PENDING";
	}
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		if (ko_state->contexts[i].used && ko_state->contexts[i].structure_drop_failed
			&& result != CLUSTER_NORMAL_STOP_INVALID) {
			/* A permanent failure cannot be hidden behind other work that
			 * is still allowed to wait. LMON owns the existing failed stop. */
			result = CLUSTER_NORMAL_STOP_INVALID;
			why = "KO_SHARED_DROP_RECOVERY_REQUIRED";
		} else if (ko_state->contexts[i].used && result == CLUSTER_NORMAL_STOP_READY) {
			result = CLUSTER_NORMAL_STOP_PENDING;
			why = ko_state->contexts[i].structure_owned ? "KO_SHARED_STRUCTURE_OWNED"
				  : ko_state->contexts[i].complete		? "KO_SHARED_COMPLETION_OWNED"
														: "KO_SHARED_BARRIER_PENDING";
		}
	}
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoPeerFenceStateV3 state = ko_state->peer_fences[i].state;

		if (state < CLUSTER_KO_PEER_FENCE_EMPTY || state > CLUSTER_KO_PEER_FENCE_COMMITTED) {
			result = CLUSTER_NORMAL_STOP_INVALID;
			why = "KO_PEER_FENCE_INVALID";
		} else if (state == CLUSTER_KO_PEER_FENCE_ACTIVE && result == CLUSTER_NORMAL_STOP_READY) {
			result = CLUSTER_NORMAL_STOP_PENDING;
			why = "KO_PEER_DRAIN_OWNED";
		}
	}
	for (unsigned i = 0; i < CLUSTER_CTRC_CLEANER_WORKERS; i++) {
		if (ko_state->cleaner_work[i].used && result == CLUSTER_NORMAL_STOP_READY) {
			result = CLUSTER_NORMAL_STOP_PENDING;
			why = "KO_CLEANER_ATTEMPT_OWNED";
		}
	}

	SpinLockRelease(&ko_state->shared_lock);
	if (reason != NULL)
		*reason = why;
	return result;
}

static bool
ko_shared_prepare_request(RelFileLocator locator, ClusterKoSharedMessageV2 *request,
						  uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], uint32 *mask)
{
	ClusterSpaceIdentity identity;
	ClusterWalSourceRef ref;
	uint64 generation;

	if (ko_state == NULL || !cluster_wal_thread_current_v2_ref(&ref)
		|| ref.claim.identity.origin_node_id != cluster_node_id
		|| !cluster_space_relation_read_identity(locator, &identity)
		|| identity.state != CLUSTER_SPACE_IDENTITY_LIVE
		|| !ko_shared_members(request, boots, &generation) || boots[cluster_node_id] == 0
		|| boots[cluster_node_id] != ref.claim.identity.origin_owner_incarnation
		|| identity.key.system_identifier != ref.claim.identity.system_identifier
		|| identity.key.database_incarnation != ref.claim.database_incarnation
		|| memcmp(identity.key.storage_uuid, ref.claim.identity.storage_uuid, 16) != 0)
		return false;
	request->verb = CLUSTER_KO_SHARED_REQUEST;
	request->batch_id = cluster_sinval_ack_wait_alloc_batch_id();
	if (request->batch_id == 0 || request->batch_id == UINT64_MAX)
		return false;
	request->origin_node = cluster_node_id;
	request->origin_boot = boots[cluster_node_id];
	request->key = identity.key;
	memcpy(request->incarnation, identity.incarnation, 16);
	*mask = 0;
	for (int n = 0; n < CLUSTER_KO_SHARED_NODE_LIMIT; n++) {
		if (n == cluster_node_id || boots[n] == 0)
			continue;
		request->peer_node = n;
		request->peer_boot = boots[n];
		if (!ko_shared_control_current(request))
			return false;
		*mask |= 1u << n;
	}
	return cluster_membership_cut_generation_current(generation);
}

struct KoAxBinding {
	ClusterResId resid;
	ClusterGrdHolderId holder;
	uint64 request_id;
	uint64 master_generation;
	int32 master_node;
};

static bool
ko_capture_relation_ax_binding(Relation rel, KoAxBinding *out)
{
	LOCKTAG locktag;
	ClusterResId expected;

	if (rel == NULL || out == NULL || !OidIsValid(RelationGetRelid(rel)))
		return false;
	MemSet(&locktag, 0, sizeof(locktag));
	SET_LOCKTAG_RELATION(locktag, MyDatabaseId, RelationGetRelid(rel));
	cluster_grd_resid_encode(&locktag, &expected);
	MemSet(out, 0, sizeof(*out));
	if (!cluster_lock_current_ax_binding(&locktag, &out->holder, &out->request_id,
																			 &out->master_node,
																			 &out->master_generation))
		return false;
	out->resid = expected;
	return out->holder.node_id == (uint32)cluster_node_id
		   && out->holder.request_id == out->request_id && out->master_node >= 0
		   && out->master_generation != 0;
}

/* The origin backend owns the LOCALLOCK that produced a v3 request. Keep
 * this check outside ko_shared_v3_control_current(): that predicate also runs
 * in LMON and on peers, where there is no backend-local LOCALLOCK to inspect. */
static bool
ko_shared_v3_local_binding_current(const ClusterKoSharedMessageV3 *request)
{
	LOCKTAG locktag;
	ClusterGrdHolderId holder;
	uint64 request_id, master_generation;
	int32 master_node;

	if (request == NULL || request->base.origin_node != cluster_node_id)
		return false;
	cluster_grd_resid_decode(&request->ax_resid, &locktag);
	return cluster_lock_current_ax_binding(&locktag, &holder, &request_id, &master_node,
																					&master_generation)
			&& memcmp(&holder, &request->ax_holder, sizeof(holder)) == 0
			&& request_id == request->ax_request_id && master_node == request->ax_master_node
			&& master_generation == request->ax_master_generation;
}

static bool
ko_shared_prepare_request_v3(RelFileLocator locator, uint16 operation,
							 const KoAxBinding *binding, ClusterKoSharedMessageV3 *request,
							 uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], uint32 *mask)
{
	ClusterSpaceIdentity identity;
	ClusterWalSourceRef ref;
	uint64 generation;

	if (request == NULL || binding == NULL || mask == NULL || ko_state == NULL
		|| operation < CLUSTER_KO_SHARED_V3_OPERATION_DROP
		|| operation > CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD
		|| binding->resid.type != LOCKTAG_RELATION
		|| binding->resid.lockmethodid != DEFAULT_LOCKMETHOD
		|| binding->holder.node_id != (uint32)cluster_node_id
		|| binding->holder.cluster_epoch == 0
		|| binding->holder.request_id == 0 || binding->holder.request_id != binding->request_id
		|| binding->master_node < 0 || binding->master_generation == 0)
		return false;
	if (!cluster_wal_thread_current_v2_ref(&ref)
		|| ref.claim.identity.origin_node_id != cluster_node_id
		|| !cluster_space_relation_read_identity(locator, &identity)
		|| identity.state != CLUSTER_SPACE_IDENTITY_LIVE
		|| !ko_shared_members(&request->base, boots, &generation)
		|| boots[cluster_node_id] == 0
		|| boots[cluster_node_id] != ref.claim.identity.origin_owner_incarnation
		|| identity.key.system_identifier != ref.claim.identity.system_identifier
		|| identity.key.database_incarnation != ref.claim.database_incarnation
		|| memcmp(identity.key.storage_uuid, ref.claim.identity.storage_uuid, 16) != 0
		|| identity.operation == 0 || identity.operation == UINT64_MAX)
		return false;
	request->base.verb = CLUSTER_KO_SHARED_REQUEST;
	request->base.status = CLUSTER_KO_SHARED_REQUEST_STATUS;
	request->base.batch_id = cluster_sinval_ack_wait_alloc_batch_id();
	if (request->base.batch_id == 0 || request->base.batch_id == UINT64_MAX)
		return false;
	request->base.origin_node = cluster_node_id;
	request->base.origin_boot = boots[cluster_node_id];
	request->base.key = identity.key;
	memcpy(request->base.incarnation, identity.incarnation, sizeof(request->base.incarnation));
	request->ax_resid = binding->resid;
	request->ax_holder = binding->holder;
	request->ax_request_id = binding->request_id;
	request->ax_master_generation = binding->master_generation;
	request->ax_master_node = binding->master_node;
	request->ax_mode = AccessExclusiveLock;
	request->operation = operation;
	request->full_fork_mask = CLUSTER_KO_SHARED_V3_FULL_FORK_MASK;
	request->space_mutation_token = identity.operation;
	*mask = 0;
	for (int n = 0; n < CLUSTER_KO_SHARED_NODE_LIMIT; n++) {
		if (n == cluster_node_id || boots[n] == 0)
			continue;
		request->base.peer_node = n;
		request->base.peer_boot = boots[n];
		if (!ko_shared_v3_control_current(request))
			return false;
		*mask |= 1u << n;
	}
	return cluster_membership_cut_generation_current(generation);
}

static int
ko_shared_reserve(const ClusterKoSharedMessageV2 *request,
				  const uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT],
				  ClusterKoCompletionV2 *completion)
{
	int context = -1;
	if (!ko_exit_registered) {
		before_shmem_exit(ko_shared_backend_exit, (Datum)0);
		ko_exit_registered = true;
	}
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoSharedContext *entry = &ko_state->contexts[i];
		if (!entry->used && ko_state->context_serial != UINT64_MAX) {
			entry->used = true;
			entry->v3 = false;
			entry->complete = false;
			entry->pid = MyProcPid;
			entry->serial = ++ko_state->context_serial;
			entry->request = *request;
			memcpy(entry->peer_boots, boots, sizeof(entry->peer_boots));
			context = i;
			if (completion != NULL) {
				completion->slot = i;
				completion->serial = entry->serial;
			}
			break;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
	return context;
}

static int
ko_shared_reserve_v3(const ClusterKoSharedMessageV3 *request,
					 const uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT],
					 ClusterKoCompletionV2 *completion)
{
	int context = -1;

	if (!ko_exit_registered) {
		before_shmem_exit(ko_shared_backend_exit, (Datum)0);
		ko_exit_registered = true;
	}
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoSharedContext *entry = &ko_state->contexts[i];
		if (!entry->used && ko_state->context_serial != UINT64_MAX) {
			entry->used = true;
			entry->v3 = true;
			entry->complete = false;
			entry->request_sent_peers = entry->request_uncertain_peers = 0;
			entry->pid = MyProcPid;
			entry->serial = ++ko_state->context_serial;
			entry->request = request->base;
			entry->request_v3 = *request;
			memcpy(entry->peer_boots, boots, sizeof(entry->peer_boots));
			context = i;
			if (completion != NULL) {
				completion->slot = i;
				completion->serial = entry->serial;
			}
			break;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
	return context;
}

static bool
ko_shared_wait_for_acks(ClusterKoSharedMessageV2 *request, uint32 mask,
						const uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], TimestampTz deadline)
{
	ResetLatch(MyLatch);
	KO_BUMP(flush_count);
	for (int n = 0; n < CLUSTER_KO_SHARED_NODE_LIMIT; n++) {
		if ((mask & (1u << n)) == 0)
			continue;
		request->peer_node = n;
		request->peer_boot = boots[n];
		if (!ko_shared_control_current(request) || !ko_shared_enqueue(request))
			return false;
	}
	while (ko_shared_control_current(request)) {
		TimestampTz now;
		int events;
		CHECK_FOR_INTERRUPTS();
		if (cluster_sinval_ack_wait_is_complete(request->batch_id))
			return ko_shared_control_current(request) && ko_shared_space_current(request);
		now = GetCurrentTimestamp();
		if (now >= deadline)
			break;
		events = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						   (deadline - now + USECS_PER_MSEC - 1) / USECS_PER_MSEC,
						   WAIT_EVENT_CLUSTER_OBJECT_FLUSH_WAIT);
		ResetLatch(MyLatch);
		if (events & WL_POSTMASTER_DEATH)
			break;
	}
	return false;
}

static bool
ko_run_shared_barrier_impl(RelFileLocator locator, ClusterKoCompletionV2 *completion)
{
	ClusterKoSharedMessageV2 request = { 0 };
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT];
	uint64 batch_id;
	uint32 mask;
	int context;
	volatile bool begun = false, ok = false;
	TimestampTz deadline;

	if (!ko_shared_prepare_request(locator, &request, boots, &mask))
		return false;
	if (mask == 0 && completion == NULL) {
		KO_BUMP(native_count);
		return true;
	}
	context = ko_shared_reserve(&request, boots, completion);
	if (context < 0)
		return false;
	/* request's peer fields change inside TRY. Keep the original wait key
	 * separately so ERROR cleanup never reads a longjmp-clobbered object. */
	batch_id = request.batch_id;
	deadline = GetCurrentTimestamp() + (int64)cluster_sinval_ack_timeout_ms * USECS_PER_MSEC;
	PG_TRY();
	{
		if (mask == 0) {
			ClusterKoSharedContext scope = { 0 };
			scope.request = request;
			memcpy(scope.peer_boots, boots, sizeof(boots));
			ok = ko_shared_origin_current(&scope) && ko_shared_space_current(&request);
			if (ok)
				KO_BUMP(native_count);
		} else
			begun = cluster_sinval_ack_wait_begin(request.batch_id, mask, deadline);
		if (begun)
			ok = ko_shared_wait_for_acks(&request, mask, boots, deadline);
	}
	PG_FINALLY();
	{
		if (begun)
			cluster_sinval_ack_wait_remove(batch_id);
		SpinLockAcquire(&ko_state->shared_lock);
		if (ok && completion != NULL)
			ko_state->contexts[context].complete = true;
		else
			memset(&ko_state->contexts[context], 0, sizeof(ko_state->contexts[context]));
		SpinLockRelease(&ko_state->shared_lock);
	}
	PG_END_TRY();
	return ok;
}

static bool
ko_shared_v3_space_matches(const ClusterKoSharedMessageV3 *message,
							const ClusterSpaceIdentity *identity)
{
	return message != NULL && identity != NULL
			   && identity->state == CLUSTER_SPACE_IDENTITY_LIVE
			   && identity->key.system_identifier == message->base.key.system_identifier
			   && identity->key.database_incarnation == message->base.key.database_incarnation
			   && RelFileLocatorEquals(identity->key.locator, message->base.key.locator)
			   && memcmp(identity->key.storage_uuid, message->base.key.storage_uuid, 16) == 0
			   && memcmp(identity->incarnation, message->base.incarnation,
																 sizeof(identity->incarnation)) == 0
			   && identity->operation == message->space_mutation_token;
}

static bool
ko_shared_v3_space_current(const ClusterKoSharedMessageV3 *message)
{
	ClusterSpaceIdentity identity;

	return message != NULL && cluster_space_relation_read_identity(message->base.key.locator,
														 &identity)
			   && ko_shared_v3_space_matches(message, &identity);
}

/* SI auxiliary peers must use the maintenance/current owner path. A regular
 * relation reader is backend-indexed and cannot prove the peer's SPACE token. */
static bool
ko_shared_v3_peer_space_current(const ClusterKoSharedMessageV3 *message)
{
	ClusterSpaceIdentity identity;

	if (message == NULL || !cluster_shared_config
		|| message->base.verb != CLUSTER_KO_SHARED_REQUEST
		|| message->base.peer_node != cluster_node_id || MyBackendType != B_SINVAL_BCAST
		|| CurrentResourceOwner == NULL || CritSectionCount != 0)
		return false;
	return cluster_space_relation_read_maintenance_identity(message->base.key.locator, &identity)
		   && ko_shared_v3_space_matches(message, &identity);
}

static bool
ko_shared_wait_for_acks_v3(ClusterKoSharedMessageV3 *request, uint32 mask,
											 const uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT],
											 TimestampTz deadline)
{
	ResetLatch(MyLatch);
	KO_BUMP(flush_count);
	for (int n = 0; n < CLUSTER_KO_SHARED_NODE_LIMIT; n++) {
		if ((mask & (1u << n)) == 0)
			continue;
		request->base.peer_node = n;
		request->base.peer_boot = boots[n];
		if (!ko_shared_v3_control_current(request)
			|| !ko_shared_enqueue_v3(request, request->ax_master_generation))
			return false;
	}
	while (ko_shared_v3_control_current(request)) {
		TimestampTz now;
		int events;
		CHECK_FOR_INTERRUPTS();
		if (cluster_sinval_ack_wait_is_complete(request->base.batch_id))
			return ko_shared_v3_control_current(request) && ko_shared_v3_space_current(request);
		now = GetCurrentTimestamp();
		if (now >= deadline)
			break;
		events = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
								   (deadline - now + USECS_PER_MSEC - 1) / USECS_PER_MSEC,
								   WAIT_EVENT_CLUSTER_OBJECT_FLUSH_WAIT);
		ResetLatch(MyLatch);
		if (events & WL_POSTMASTER_DEATH)
			break;
	}
	return false;
}

static bool
ko_run_shared_barrier_impl_v3(RelFileLocator locator, uint16 operation,
							  const KoAxBinding *binding, ClusterKoCompletionV2 *completion)
{
	ClusterKoSharedMessageV3 request = { 0 };
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT];
	uint64 batch_id;
	uint32 mask;
	int context;
	volatile bool begun = false, ok = false;
	TimestampTz deadline;

	if (!ko_shared_prepare_request_v3(locator, operation, binding, &request, boots, &mask))
		return false;
	if (!ko_shared_v3_local_binding_current(&request))
		return false;
	if (mask == 0 && completion == NULL) {
		KO_BUMP(native_count);
		return true;
	}
	context = ko_shared_reserve_v3(&request, boots, completion);
	if (context < 0)
		return false;
	batch_id = request.base.batch_id;
	deadline = GetCurrentTimestamp() + (int64)cluster_sinval_ack_timeout_ms * USECS_PER_MSEC;
	PG_TRY();
	{
		/* Reserve transaction-owned publication before any peer REQUEST can
		 * escape. Abort discards it through the original invalidation owner;
		 * registration failure unwinds only this still-unsent reservation. */
		if (mask != 0)
			CacheInvalidateKoCommit(batch_id);
		if (mask == 0) {
			ClusterKoSharedContext scope = { 0 };
			scope.v3 = true;
			scope.request = request.base;
			scope.request_v3 = request;
			memcpy(scope.peer_boots, boots, sizeof(boots));
			ok = ko_shared_origin_current(&scope) && ko_shared_v3_space_current(&request);
			if (ok)
				KO_BUMP(native_count);
		} else
			begun = cluster_sinval_ack_wait_begin(request.base.batch_id, mask, deadline);
		if (begun)
			ok = ko_shared_wait_for_acks_v3(&request, mask, boots, deadline);
	}
	PG_FINALLY();
	{
		if (begun)
			cluster_sinval_ack_wait_remove(batch_id);
		SpinLockAcquire(&ko_state->shared_lock);
		if (ok && completion != NULL)
			ko_state->contexts[context].complete = true;
		else
			memset(&ko_state->contexts[context], 0, sizeof(ko_state->contexts[context]));
		SpinLockRelease(&ko_state->shared_lock);
	}
	PG_END_TRY();
	return ok;
}

static bool
ko_run_shared_barrier(RelFileLocator locator)
{
	return ko_run_shared_barrier_impl(locator, NULL);
}


/* ============================================================
 * Enqueuer side: the apply-after-drop flush fanout + ACK barrier.
 * ============================================================ */

/*
 * ko_run_barrier -- fanout KO_FLUSH to every alive peer and wait (reusing the
 * 2.39 ack_wait correlation infra, KO-B2) until each has ACK'd DONE.  Returns
 * true iff every alive peer ACK'd in time; false on ack_wait-table-full,
 * outbound-queue-full, or timeout (all map to 53RAA in the caller).  Never
 * consults cluster.sinval_ack_mode (KO-M6: ack_mode=none must not turn the
 * barrier into fire-and-forget).
 */
static bool
ko_run_barrier(RelFileLocator rloc, uint32 alive_mask)
{
	uint64 batch_id;
	TimestampTz deadline_us;
	KoFlushHeader hdr;
	int peer;
	bool ok = false;

	batch_id = cluster_sinval_ack_wait_alloc_batch_id();
	deadline_us = GetCurrentTimestamp() + (int64)cluster_sinval_ack_timeout_ms * USECS_PER_MSEC;
	if (!cluster_sinval_ack_wait_begin(batch_id, alive_mask, deadline_us))
		return false; /* ack_wait table full */

	/* Reset the latch before publishing so we never miss an early ACK. */
	ResetLatch(MyLatch);

	memset(&hdr, 0, sizeof(hdr));
	hdr.batch_id = batch_id;
	hdr.epoch = cluster_epoch_get_current();
	hdr.db_oid = (uint32)rloc.dbOid;
	hdr.rel_number = (uint32)rloc.relNumber;
	hdr.spc_oid = (uint32)rloc.spcOid;
	hdr.source_node = cluster_node_id;

	KO_BUMP(flush_count);
	for (peer = 0; peer < CLUSTER_MAX_NODES && peer < sizeof(alive_mask) * CHAR_BIT; peer++) {
		if ((alive_mask & (1u << peer)) == 0)
			continue;
		if (!cluster_grd_outbound_enqueue_backend_msg(PGRAC_IC_MSG_KO_FLUSH, (uint32)peer, &hdr,
													  (uint16)sizeof(hdr))) {
			/* Could not enqueue -> this peer would never get the flush.  Fail closed
			 * rather than proceed with an un-flushed peer (8.A). */
			cluster_sinval_ack_wait_remove(batch_id);
			return false;
		}
	}
	cluster_lmon_wakeup(); /* wake LMON to drain the outbound ring */

	for (;;) {
		TimestampTz now_us;
		long timeout_ms;
		int rc;

		/* Legacy non-shared INITIAL uses epoch zero. Equality still binds
		 * the whole barrier; shared requests use the nonzero v2 cut above. */
		if ((cluster_shared_config && hdr.epoch == 0) || hdr.epoch != cluster_epoch_get_current())
			break;
		if (cluster_sinval_ack_wait_is_complete(batch_id)) {
			/* The ACK lookup can overlap a reconfiguration. */
			ok = hdr.epoch == cluster_epoch_get_current();
			break;
		}
		now_us = GetCurrentTimestamp();
		if (now_us >= deadline_us)
			break; /* timeout -> fail closed */
		timeout_ms = (deadline_us - now_us + USECS_PER_MSEC - 1) / USECS_PER_MSEC;
		if (timeout_ms <= 0)
			timeout_ms = 1;
		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, timeout_ms,
					   WAIT_EVENT_CLUSTER_OBJECT_FLUSH_WAIT);
		ResetLatch(MyLatch);
		if (rc & WL_POSTMASTER_DEATH)
			break;
	}

	cluster_sinval_ack_wait_remove(batch_id);
	return ok;
}

static void
ko_flush_and_wait_ack(RelFileLocator rloc, char relpersistence, ClusterKoCompletionV2 *completion,
					  uint16 operation, const KoAxBinding *binding)
{
	ClusterResId resid;
	KoLock lk;
	KoAcquireOutcome out;
	bool ok = false;

	/*
	 * Gates.  KO only matters for a cross-node-visible relation in a live,
	 * multi-node cluster.  A temp relation is private (no peer caches it); a
	 * single node / recovery / disabled flush has no remote buffers to drop.
	 */
	if (!cluster_enabled)
		return;
	if (relpersistence == RELPERSISTENCE_TEMP)
		return;
	if (cluster_node_id < 0 || RecoveryInProgress())
		return;
	/* The caller holds exclusive lifecycle authority. A parked range from
	 * before a truncate/reset must not survive until this file regrows.
	 * Discard even when there are no peers; aborted DDL only orphans it. */
	cluster_hw_lease_discard(rloc, MAIN_FORKNUM);
	/*
	 * MXA-K14 / §12.8: the AccessExclusiveLock held by the DDL caller
	 * prevents a new reference publication while this bounded journal census
	 * runs.  Refuse before KO, buffer invalidation or physical removal unless
	 * every local receipt for the relfilenode is already terminal.
	 */
	if (!cluster_ctrc_relation_removal_ready_shared((uint32)rloc.spcOid, (uint32)rloc.dbOid,
													(uint32)rloc.relNumber)) {
		KO_BUMP(failclosed_count);
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE),
				 errmsg("could not drain terminal-reference receipts for relfilenode %u/%u/%u "
						"before reuse",
						(unsigned)rloc.spcOid, (unsigned)rloc.dbOid, (unsigned)rloc.relNumber),
				 errhint("Wait for canonical terminal-reference cleanout and retry.")));
	}
	if (!cluster_object_reuse_flush_enabled && !cluster_shared_config)
		return;
	/*
	 * Engage from runtime liveness, not the static configured node count
	 * (spec-5.7 §3.1d).  wait_for_lms = false: the flush barrier rides on sinval
	 * (not LMS) and has its own alive-peer gate below, so KO never blocks a
	 * DROP/TRUNCATE on LMS warming up.  With no alive peer (NATIVE) there are no
	 * remote buffers to flush -> skip.  COORDINATE / FAIL_CLOSED both fall through
	 * to the barrier, which self-gates on the peer-ACK requirement.
	 */
	if (!cluster_shared_config
		&& cluster_extend_liveness_engage(false) == CLUSTER_EXTEND_ENGAGE_NATIVE)
		return;

	/*
	 * KO(X) is BEST-EFFORT serialisation, not the correctness gate.  The
	 * flush-ACK barrier below is the 8.A gate; KO(X) only serialises concurrent
	 * DROP/TRUNCATE of the SAME relfilenode, which the relation's cross-node
	 * AccessExclusiveLock (spec-5.3 TM) already does.  So a KO(X) acquire that
	 * cannot be granted (e.g. the GES is not provisioned with enough entries)
	 * must NOT abort a DROP the barrier itself would permit -- we record it and
	 * proceed to the barrier, which self-gates via the peer-ACK requirement
	 * (a genuinely unhealthy cluster fails closed there instead).
	 */
	cluster_ko_resid_encode(rloc, &resid);
	out = ko_lock(&resid, &lk);
	if (out == KO_ACQUIRE_FAILED)
		KO_BUMP(lockfail_count);

	/* Run the barrier; release KO(X) (if held) on every exit path. */
	PG_TRY();
	{
		uint32 alive_mask = 0;

		if (cluster_shared_config) {
			if (binding != NULL)
				ok = ko_run_shared_barrier_impl_v3(rloc, operation, binding, completion);
			else
				ok = completion != NULL ? ko_run_shared_barrier_impl(rloc, completion)
									: ko_run_shared_barrier(rloc);
		}
		else {
			alive_mask = cluster_sinval_compute_alive_peer_mask();
			if (alive_mask == 0) {
				/* No alive peers -> nobody else can hold these buffers. */
				KO_BUMP(native_count);
				ok = true;
			} else
				ok = ko_run_barrier(rloc, alive_mask);
		}
	}
	PG_FINALLY();
	{
		if (out == KO_ACQUIRE_GRANTED)
			ko_unlock(&lk);
	}
	PG_END_TRY();

	if (!ok) {
		KO_BUMP(failclosed_count);
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE),
				 errmsg("could not confirm every peer dropped buffers for relfilenode %u/%u/%u "
						"before reuse",
						(unsigned)rloc.spcOid, (unsigned)rloc.dbOid, (unsigned)rloc.relNumber),
				 errhint("A peer did not acknowledge the cross-node buffer flush within "
						 "cluster.sinval_ack_timeout_ms; check cluster health and retry.")));
	}
}

void
cluster_ko_flush_and_wait_ack(RelFileLocator rloc, char relpersistence)
{
	/* Preserve a live shared DDL's exact original barrier until its SPACE
	 * owner takes it, or the transaction owner cancels it. Private/native
	 * recovery calls retain their original no-op/legacy behavior. */
	if (cluster_enabled && cluster_shared_config && relpersistence != RELPERSISTENCE_TEMP
		&& !RecoveryInProgress() && cluster_node_id >= 0) {
		ClusterKoCompletionV2 *completion = NULL;
		if (!ko_native_transaction_current()
			|| !cluster_ko_shared_begin_v2(rloc, relpersistence, &completion))
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE),
							errmsg("could not retain the original shared object flush owner")));
		completion->owner = CurTransactionResourceOwner;
		completion->native_transaction = true;
		if (!ko_native_barrier_to_owner(completion))
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE),
							errmsg("could not preserve the completed shared object flush proof")));
		/* Allocation and mapping finish before the caller can lock or modify
		 * SPACE. A later committed handoff cannot fail just because 64 other
		 * relation results are still being processed. */
		if (!ko_native_reserve(completion))
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("could not reserve the shared structural completion owner")));
		completion->native_pending = true;
		return;
	}
	ko_flush_and_wait_ack(rloc, relpersistence, NULL, 0, NULL);
}

bool
cluster_ko_shared_begin_v2(RelFileLocator rloc, char relpersistence, ClusterKoCompletionV2 **out)
{
	ClusterKoCompletionV2 *completion;
	if (out == NULL || *out != NULL || !cluster_enabled || !cluster_shared_config
		|| relpersistence == RELPERSISTENCE_TEMP || RecoveryInProgress() || cluster_node_id < 0
		|| CurrentResourceOwner == NULL || TopTransactionContext == NULL || CritSectionCount != 0
		|| MyProcPid <= 0)
		return false;
	if (!ko_resource_registered) {
		RegisterResourceReleaseCallback(ko_shared_resource_release, NULL);
		RegisterXactCallback(ko_shared_xact_event, NULL);
		ko_resource_registered = true;
	}
	completion = MemoryContextAllocZero(TopTransactionContext, sizeof(*completion));
	completion->owner = CurrentResourceOwner;
	completion->pid = MyProcPid;
	completion->slot = CLUSTER_KO_SHARED_CAPACITY;
	completion->next = ko_completions;
	ko_completions = completion;
	PG_TRY();
	{
		ko_flush_and_wait_ack(rloc, relpersistence, completion, 0, NULL);
	}
	PG_CATCH();
	{
		cluster_ko_shared_release_v2(&completion);
		PG_RE_THROW();
	}
	PG_END_TRY();
	*out = completion;
	return true;
}

static bool
ko_shared_begin_v3(RelFileLocator rloc, char relpersistence, uint16 operation,
					const KoAxBinding *binding, ClusterKoCompletionV2 **out)
{
	ClusterKoCompletionV2 *completion;

	if (out == NULL || *out != NULL || binding == NULL || !cluster_enabled
		|| !cluster_shared_config || relpersistence == RELPERSISTENCE_TEMP
		|| RecoveryInProgress() || cluster_node_id < 0 || CurrentResourceOwner == NULL
		|| TopTransactionContext == NULL || CritSectionCount != 0 || MyProcPid <= 0)
		return false;
	if (!ko_resource_registered) {
		RegisterResourceReleaseCallback(ko_shared_resource_release, NULL);
		RegisterXactCallback(ko_shared_xact_event, NULL);
		ko_resource_registered = true;
	}
	completion = MemoryContextAllocZero(TopTransactionContext, sizeof(*completion));
	completion->owner = CurrentResourceOwner;
	completion->pid = MyProcPid;
	completion->slot = CLUSTER_KO_SHARED_CAPACITY;
	completion->next = ko_completions;
	ko_completions = completion;
	PG_TRY();
	{
		ko_flush_and_wait_ack(rloc, relpersistence, completion, operation, binding);
	}
	PG_CATCH();
	{
		cluster_ko_shared_release_v2(&completion);
		PG_RE_THROW();
	}
	PG_END_TRY();
	*out = completion;
	return true;
}

void
cluster_ko_flush_and_wait_ack_relation(Relation rel, char relpersistence, uint16 operation)
{
	KoAxBinding binding;

	if (rel == NULL || !cluster_enabled || !cluster_shared_config
		|| relpersistence == RELPERSISTENCE_TEMP || RecoveryInProgress() || cluster_node_id < 0) {
		if (rel != NULL)
			cluster_ko_flush_and_wait_ack(rel->rd_locator, relpersistence);
		return;
	}
	if (!ko_capture_relation_ax_binding(rel, &binding))
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE),
				 errmsg("could not prove the relation AccessExclusiveLock owner for KO v3"),
				 errhint("Retry after the relation grant and current formation are stable.")));
	if (ko_native_transaction_current()) {
		ClusterKoCompletionV2 *completion = NULL;

		if (!ko_shared_begin_v3(rel->rd_locator, relpersistence, operation, &binding, &completion))
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE),
					 errmsg("could not retain the KO v3 completion owner")));
		completion->owner = CurTransactionResourceOwner;
		completion->native_transaction = true;
		completion->barrier_v3 = true;
		if (!ko_native_barrier_to_owner(completion))
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE),
					 errmsg("could not preserve the KO v3 completion proof")));
		if (!ko_native_reserve(completion))
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("could not reserve the KO v3 completion owner")));
		completion->native_pending = true;
		return;
	}
	ko_flush_and_wait_ack(rel->rd_locator, relpersistence, NULL, operation, &binding);
}


/* ============================================================
 * IC msg type registration + inbound handlers.
 * ============================================================ */

void
cluster_ko_register_ic_msg_types(void)
{
	ClusterICMsgTypeInfo info;

	memset(&info, 0, sizeof(info));
	info.msg_type = PGRAC_IC_MSG_KO_FLUSH;
	info.name = "ko_flush";
	info.allowed_producer_mask = CLUSTER_IC_PRODUCER_KO_FLUSH;
	info.broadcast_ok = false;
	info.handler = cluster_ko_flush_request_handler;
	cluster_ic_register_msg_type(&info);

	memset(&info, 0, sizeof(info));
	info.msg_type = PGRAC_IC_MSG_KO_FLUSH_ACK;
	info.name = "ko_flush_ack";
	info.allowed_producer_mask = CLUSTER_IC_PRODUCER_KO_FLUSH_ACK;
	info.broadcast_ok = false;
	info.handler = cluster_ko_flush_ack_handler;
	cluster_ic_register_msg_type(&info);
}

/*
 * cluster_ko_flush_request_handler -- peer-side IC inbound handler (runs in
 * LMON).  Validates the request, nonblocking-enqueues it into the SPSC inbound
 * ring, and wakes the SI Broadcaster aux to do the heavy flush+drop.  HC133-style
 * constraint: never blocks, never touches buffers here (that is the aux's job).
 * If the ring is full it drops the request (no ACK), so the enqueuer times out
 * and fails closed -- the request is never silently treated as fulfilled.
 */
void
cluster_ko_flush_request_handler(const ClusterICEnvelope *env, const void *payload)
{
	const KoFlushHeader *hdr = (const KoFlushHeader *)payload;
	KoFlushHeader legacy = { 0 };
	ClusterKoSharedMessageV2 qualified = { 0 };
	ClusterKoSharedMessageV3 qualified_v3 = { 0 };
	ClusterICControlIngress ingress = { 0 };
	uint64 peer_route_generation = 0;
	uint32 head, tail, next;
	bool shared = cluster_shared_config;
	bool v3 = false;

	if (ko_state == NULL || env == NULL || payload == NULL)
		return;
	if (shared) {
		if (env->payload_length == CLUSTER_KO_SHARED_V3_BYTES) {
			if (!cluster_ko_shared_decode_v3(payload, env->payload_length, &qualified_v3)
				|| qualified_v3.base.verb != CLUSTER_KO_SHARED_REQUEST
				|| qualified_v3.base.peer_node != cluster_node_id
				|| qualified_v3.base.origin_node != env->source_node_id || !AmLmonProcess()
				|| MyBackendType != B_LMON || !cluster_ic_dispatch_control_ingress(env, &ingress)
				|| ingress.carrier != CLUSTER_IC_CONTROL_TCP || ingress.serial == 0
				|| ingress.serial == UINT64_MAX || ingress.owner_pid != MyProcPid
				|| ingress.owner_pid <= 0 || ingress.epoch != qualified_v3.base.epoch
				|| ingress.source_node != qualified_v3.base.origin_node
				|| cluster_grd_lookup_master_gen(&qualified_v3.ax_resid, &peer_route_generation)
					   != qualified_v3.ax_master_node
				|| !ko_shared_v3_control_at_generation(&qualified_v3, peer_route_generation))
				return;
			v3 = true;
			legacy.batch_id = qualified_v3.base.batch_id;
			legacy.epoch = qualified_v3.base.epoch;
			legacy.source_node = qualified_v3.base.origin_node;
			legacy.spc_oid = qualified_v3.base.key.locator.spcOid;
			legacy.db_oid = qualified_v3.base.key.locator.dbOid;
			legacy.rel_number = qualified_v3.base.key.locator.relNumber;
		} else {
			if (!cluster_ko_shared_decode_v2(payload, env->payload_length, &qualified)
				|| qualified.verb != CLUSTER_KO_SHARED_REQUEST || qualified.peer_node != cluster_node_id
				|| qualified.origin_node != env->source_node_id
				|| !ko_shared_control_current(&qualified))
				return;
			legacy.batch_id = qualified.batch_id;
			legacy.epoch = qualified.epoch;
			legacy.source_node = qualified.origin_node;
			legacy.spc_oid = qualified.key.locator.spcOid;
			legacy.db_oid = qualified.key.locator.dbOid;
			legacy.rel_number = qualified.key.locator.relNumber;
		}
		hdr = &legacy;
	} else if (env->payload_length != (uint32)sizeof(KoFlushHeader))
		return;
	if ((shared && hdr->epoch == 0) || hdr->epoch != cluster_epoch_get_current())
		return; /* No request from another configuration can drain here. */
	if (hdr->source_node < 0 || hdr->source_node >= CLUSTER_MAX_NODES
		|| env->source_node_id != (uint32)hdr->source_node
		|| cluster_conf_lookup_node(hdr->source_node) == NULL)
		return;

	/* A new object flush/drop cannot cross the normal-stop modifier seal.
	 * Existing ACK completion still follows its original exact wait entry. */
	if (!cluster_normal_stop_service_new_work(true))
		return;
	if (v3 && ko_peer_request_ended_or_conflicts_v3(&qualified_v3))
		return;
	/* SPSC enqueue (LMON is the only producer). */
	tail = pg_atomic_read_u32(&ko_state->inbound_tail);
	head = pg_atomic_read_u32(&ko_state->inbound_head);
	next = (tail + 1) % CLUSTER_KO_INBOUND_CAPACITY;
	if (next == head) {
		/* Ring full -> drop (no ACK -> enqueuer fails closed 53RAA). */
		KO_BUMP(inbound_full_count);
		return;
	}
	/* REQUEST retransmit is idempotent while the original frame is still
	 * resident. Do not create a second physical attempt for the same PKO3
	 * identity; the first slot remains the sole owner of the eventual ACK or
	 * fail-closed timeout. A post-consumption retry still follows the existing
	 * physical owner and must not be turned into synthetic success here. */
	if (v3) {
		uint32 scan = head;

		while (scan != tail) {
			const ClusterKoInboundSlot *queued = &ko_state->inbound[scan];

			if (queued->shared && queued->v3
				&& memcmp(&queued->qualified_v3, &qualified_v3, sizeof(qualified_v3)) == 0)
				return;
			scan = (scan + 1) % CLUSTER_KO_INBOUND_CAPACITY;
		}
	}
	ko_state->inbound[tail].shared = shared;
	ko_state->inbound[tail].v3 = v3;
	ko_state->inbound[tail].qualified = qualified;
	ko_state->inbound[tail].qualified_v3 = qualified_v3;
	ko_state->inbound[tail].ingress = ingress;
	ko_state->inbound[tail].peer_route_generation = peer_route_generation;
	ko_state->inbound[tail].db_oid = hdr->db_oid;
	ko_state->inbound[tail].rel_number = hdr->rel_number;
	ko_state->inbound[tail].spc_oid = hdr->spc_oid;
	ko_state->inbound[tail].source_node = hdr->source_node;
	ko_state->inbound[tail].batch_id = hdr->batch_id;
	ko_state->inbound[tail].epoch = hdr->epoch;
	pg_write_barrier(); /* publish the slot before advancing the tail */
	pg_atomic_write_u32(&ko_state->inbound_tail, next);

	cluster_sinval_set_proc_latch(); /* wake the SI Broadcaster aux to drain */
}

/*
 * cluster_ko_flush_ack_handler -- enqueuer-side IC inbound handler (runs in
 * LMON).  Records a peer's apply-after-drop ACK against the ack_wait entry.
 * KO-M6: ONLY status DONE counts as fulfilled -- a FAILED ACK (or no ACK at all)
 * leaves the enqueuer to time out and fail closed; there is no RESET_PENDING
 * shortcut (SIResetAll does not drop buffer-pool dirty pages).
 */
void
cluster_ko_flush_ack_handler(const ClusterICEnvelope *env, const void *payload)
{
	const KoFlushAckHeader *hdr = (const KoFlushAckHeader *)payload;
	ClusterKoSharedMessageV2 ack_v2;
	bool matched_v2;

	if (ko_state == NULL || env == NULL || payload == NULL)
		return;
	if (cluster_shared_config) {
		if (env->payload_length == CLUSTER_KO_SHARED_V3_BYTES) {
			ClusterKoSharedMessageV3 ack;
			bool matched = false;

			if (!cluster_ko_shared_decode_v3(payload, env->payload_length, &ack)
				|| ack.base.verb != CLUSTER_KO_SHARED_ACK
				|| ack.base.status != CLUSTER_KO_SHARED_DONE
				|| ack.base.origin_node != cluster_node_id
				|| ack.base.peer_node != env->source_node_id
				|| !ko_shared_v3_control_current(&ack))
				return;
			SpinLockAcquire(&ko_state->shared_lock);
			for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
				const ClusterKoSharedContext *entry = &ko_state->contexts[i];
				ClusterKoSharedMessageV3 expected;
				if (!entry->used || !entry->v3 || entry->complete
					|| entry->request_v3.base.batch_id != ack.base.batch_id
					|| (entry->request_sent_peers & (1u << ack.base.peer_node)) == 0
					|| (entry->request_uncertain_peers & (1u << ack.base.peer_node)) != 0)
					continue;
				expected = entry->request_v3;
				expected.base.peer_node = ack.base.peer_node;
				expected.base.peer_boot = entry->peer_boots[ack.base.peer_node];
				matched = cluster_ko_shared_ack_matches_v3(&expected, &ack);
				break;
			}
			SpinLockRelease(&ko_state->shared_lock);
			if (matched) {
				cluster_sinval_ack_wait_record(ack.base.batch_id, ack.base.peer_node);
				KO_BUMP(ack_received_count);
			}
			return;
		}
		matched_v2 = false;

		if (!cluster_ko_shared_decode_v2(payload, env->payload_length, &ack_v2)
			|| ack_v2.verb != CLUSTER_KO_SHARED_ACK || ack_v2.status != CLUSTER_KO_SHARED_DONE
			|| ack_v2.origin_node != cluster_node_id || ack_v2.peer_node != env->source_node_id
			|| !ko_shared_control_current(&ack_v2))
			return;
		SpinLockAcquire(&ko_state->shared_lock);
		for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
			const ClusterKoSharedContext *entry = &ko_state->contexts[i];
			ClusterKoSharedMessageV2 expected;
			if (!entry->used || entry->v3 || entry->complete
				|| entry->request.batch_id != ack_v2.batch_id)
				continue;
			expected = entry->request;
			expected.peer_node = ack_v2.peer_node;
			expected.peer_boot = entry->peer_boots[ack_v2.peer_node];
			matched_v2 = cluster_ko_shared_ack_matches_v2(&expected, &ack_v2);
			break;
		}
		SpinLockRelease(&ko_state->shared_lock);
		if (matched_v2) {
			cluster_sinval_ack_wait_record(ack_v2.batch_id, ack_v2.peer_node);
			KO_BUMP(ack_received_count);
		}
		return;
	}
	if (env->payload_length != (uint32)sizeof(KoFlushAckHeader))
		return;
	if (hdr->flags != 0)
		return;
	if (hdr->acker_node < 0 || hdr->acker_node >= CLUSTER_MAX_NODES
		|| env->source_node_id != (uint32)hdr->acker_node
		|| cluster_conf_lookup_node(hdr->acker_node) == NULL)
		return;
	/* This is the legacy non-shared ACK branch; INITIAL is a real epoch.
	 * Shared v2 ACKs retain their separate nonzero exact-cut validation. */
	if (hdr->epoch != cluster_epoch_get_current())
		return;
	if (hdr->status != (uint16)KO_FLUSH_ACK_DONE)
		return; /* KO-M6: only an apply-after-drop DONE fulfils the barrier */

	cluster_sinval_ack_wait_record(hdr->batch_id, hdr->acker_node);
	KO_BUMP(ack_received_count);
}


/* ============================================================
 * Peer side: SI Broadcaster aux drain -- flush + drop, THEN ACK.
 * ============================================================ */

void
cluster_ko_drain_inbound_and_apply(void)
{
	uint32 head, tail;

	if (ko_state == NULL)
		return;

	head = pg_atomic_read_u32(&ko_state->inbound_head);
	tail = pg_atomic_read_u32(&ko_state->inbound_tail);

	while (head != tail) {
		ClusterKoInboundSlot slot;
		RelFileLocator rloc;
		SMgrRelation smgr;
		KoFlushAckHeader ack;

		pg_read_barrier();
		slot = ko_state->inbound[head]; /* copy before publishing consumer progress */
		if (slot.shared && slot.v3
			&& ko_peer_request_ended_or_conflicts_v3(&slot.qualified_v3)) {
			head = (head + 1) % CLUSTER_KO_INBOUND_CAPACITY;
			pg_atomic_write_u32(&ko_state->inbound_head, head);
			goto next;
		}
		/* R2 v3 keeps an accepted request at the ring head while its bounded
		 * terminal-reference proof is still pending. This is the existing ring's
		 * REQUEST -> READY continuation: later requests are not acknowledged out
		 * of order, and no identity read or synthetic ACK is introduced. */
		if (slot.shared && slot.v3 && slot.epoch == cluster_epoch_get_current()
			&& slot.shared == cluster_shared_config
			&& ko_shared_v3_control_at_generation(&slot.qualified_v3, slot.peer_route_generation)
			&& !cluster_ctrc_relation_removal_ready_shared(
				(uint32)slot.spc_oid, (uint32)slot.db_oid, (uint32)slot.rel_number))
			return;
		/*
		 * Advance the head BEFORE applying: a failed flush (longjmp to the aux
		 * error handler) must not re-process the same request forever -- the
		 * dropped request leaves the enqueuer to time out (53RAA), which is the
		 * correct fail-closed behavior rather than an infinite retry loop.
		 */
		head = (head + 1) % CLUSTER_KO_INBOUND_CAPACITY;
		pg_atomic_write_u32(&ko_state->inbound_head, head);

		rloc.spcOid = (Oid)slot.spc_oid;
		rloc.dbOid = (Oid)slot.db_oid;
		rloc.relNumber = (RelFileNumber)slot.rel_number;
		if ((slot.shared && slot.epoch == 0) || slot.epoch != cluster_epoch_get_current()
			|| slot.shared != cluster_shared_config
			|| (slot.shared
				&& (slot.v3 ? (!ko_shared_v3_control_at_generation(&slot.qualified_v3,
																   slot.peer_route_generation)
							   || !ko_shared_v3_peer_space_current(&slot.qualified_v3))
							: (!ko_shared_control_current(&slot.qualified)
							   || !ko_shared_peer_space_current(&slot.qualified)))))
			goto next;
		/* A peer may publish DONE only after its own bounded CTRC journal is
		 * drained.  No ACK makes the enqueuer fail closed without extending
		 * the existing KO wire. */
		if (!cluster_ctrc_relation_removal_ready_shared((uint32)rloc.spcOid, (uint32)rloc.dbOid,
														(uint32)rloc.relNumber)) {
			goto next;
		}
		smgr = smgropen(rloc, InvalidBackendId);
		if (slot.epoch != cluster_epoch_get_current()
			|| (slot.shared
				&& (slot.v3 ? (!ko_shared_v3_control_at_generation(&slot.qualified_v3,
																   slot.peer_route_generation)
							   || !ko_shared_v3_peer_space_current(&slot.qualified_v3))
							: !ko_shared_control_current(&slot.qualified))))
			goto next;

		/*
		 * Flush THEN invalidate (the abort-safe order): write any dirty buffers to
		 * shared storage so a rolled-back DROP loses no data, then drop ALL of the
		 * relfilenode's buffers so no later writeback can scribble into the file or
		 * a reused relfilenode.
		 */
		FlushRelationsAllBuffers(&smgr, 1);
		if (slot.epoch != cluster_epoch_get_current()
			|| (slot.shared
				&& (slot.v3 ? (!ko_shared_v3_control_at_generation(&slot.qualified_v3,
																   slot.peer_route_generation)
							   || !ko_shared_v3_peer_space_current(&slot.qualified_v3))
							: !ko_shared_control_current(&slot.qualified))))
			goto next;
		if (cluster_shared_config && cluster_smgr_which_for(rloc, InvalidBackendId) == 1) {
			/* Buffer writeout alone does not make shared DATA durable.  An
			 * I/O error must retain the buffers and leave the origin unacked. */
			for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
				if (smgrexists(smgr, fork))
					smgrimmedsync(smgr, fork);
				if (slot.epoch != cluster_epoch_get_current()
					|| (slot.shared
						&& (slot.v3 ? (!ko_shared_v3_control_at_generation(
										   &slot.qualified_v3, slot.peer_route_generation)
									   || !ko_shared_v3_peer_space_current(&slot.qualified_v3))
									: !ko_shared_control_current(&slot.qualified))))
					goto next;
			}
		}
		/* Check SPACE before invalidation; rereading it afterwards would
		 * recreate precisely the buffer the barrier has just discarded. */
		if (slot.shared
			&& (slot.v3 ? !ko_shared_v3_peer_space_current(&slot.qualified_v3)
						: !ko_shared_peer_space_current(&slot.qualified)))
			goto next;
		DropRelationsAllBuffers(&smgr, 1);
		cluster_hw_lease_discard(rloc, MAIN_FORKNUM);
		KO_BUMP(peer_apply_count);

		/*
		 * Fault injection (t/297 L2): when armed, the peer has applied the drop
		 * but deliberately does NOT ACK, so the dropping node's barrier times out
		 * and fails closed (53RAA).  Proves the apply-after-drop fail-closed path
		 * deterministically without having to kill a node.
		 */
		CLUSTER_INJECTION_POINT("cluster-ko-peer-skip-ack");
		if (cluster_injection_should_skip("cluster-ko-peer-skip-ack")
			|| slot.epoch != cluster_epoch_get_current()
			|| (slot.shared
				&& (slot.v3 ? !ko_shared_v3_control_at_generation(&slot.qualified_v3,
																  slot.peer_route_generation)
							: !ko_shared_control_current(&slot.qualified))))
			goto next;

		if (slot.shared) {
			if (slot.v3) {
				slot.qualified_v3.base.verb = CLUSTER_KO_SHARED_ACK;
				slot.qualified_v3.base.status = CLUSTER_KO_SHARED_DONE;
				(void)ko_shared_enqueue_v3(&slot.qualified_v3, slot.peer_route_generation);
			} else {
				slot.qualified.verb = CLUSTER_KO_SHARED_ACK;
				slot.qualified.status = CLUSTER_KO_SHARED_DONE;
				(void)ko_shared_enqueue(&slot.qualified);
			}
			goto next;
		}

		/* apply-after-drop: ACK only now that the buffers are really gone. */
		memset(&ack, 0, sizeof(ack));
		ack.batch_id = slot.batch_id;
		ack.epoch = slot.epoch;
		ack.acker_node = cluster_node_id;
		ack.status = (uint16)KO_FLUSH_ACK_DONE;
		ack.flags = 0;
		(void)cluster_grd_outbound_enqueue_backend_msg(
			PGRAC_IC_MSG_KO_FLUSH_ACK, (uint32)slot.source_node, &ack, (uint16)sizeof(ack));
		cluster_lmon_wakeup(); /* wake LMON to send the ACK */

	next:
		head = pg_atomic_read_u32(&ko_state->inbound_head);
		tail = pg_atomic_read_u32(&ko_state->inbound_tail);
	}
}
