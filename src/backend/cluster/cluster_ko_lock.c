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
#include "utils/resowner.h"
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
#define CLUSTER_KO_SHARED_CAPACITY 64
#define CLUSTER_KO_SHARED_NODE_LIMIT 16

typedef struct ClusterKoSharedContext {
	bool used;
	bool complete;
	bool structure_owned;
	int32 pid;
	uint64 serial;
	ClusterKoSharedMessageV2 request;
	uint64 peer_boots[CLUSTER_KO_SHARED_NODE_LIMIT];
	/* Reserved before the DDL starts; never allocate a second post-commit
	 * queue slot. The original background owner must clear this obligation. */
	ClusterPageWalBindingV1 terminal;
	uint8 structure[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
} ClusterKoSharedContext;

typedef struct ClusterKoInboundSlot {
	bool shared;
	ClusterKoSharedMessageV2 qualified;
	uint32 db_oid;
	uint32 rel_number;
	uint32 spc_oid;
	int32 source_node;
	uint64 batch_id;
	uint64 epoch;
} ClusterKoInboundSlot;

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
	uint64 context_serial;
	ClusterKoSharedContext contexts[CLUSTER_KO_SHARED_CAPACITY];
} ClusterKoShared;

static ClusterKoShared *ko_state = NULL;
static bool ko_exit_registered;

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
	ClusterPageWalBindingV1 terminal;
	uint8 structure[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	struct ClusterKoCompletionV2 *next;
};

static ClusterKoCompletionV2 *ko_completions;
static bool ko_resource_registered;

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
		if (item->batch_id == 0 || item->epoch == 0 || item->source_node < 0
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

Size
cluster_ko_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterKoShared));
}

void
cluster_ko_shmem_init(void)
{
	bool found;

	ko_state = (ClusterKoShared *)ShmemInitStruct("pgrac cluster ko",
												  MAXALIGN(sizeof(ClusterKoShared)), &found);
	if (!IsUnderPostmaster) {
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
		ko_state->context_serial = 0;
		memset(ko_state->contexts, 0, sizeof(ko_state->contexts));
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
ko_shared_members(ClusterKoSharedMessageV2 *message,
				  uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], uint64 *generation)
{
	static const char domain[] = "PGRAC-KO-MEMBERS-V2";
	ClusterFormationSnapshotV1 snapshot;
	ClusterKoSharedMessageV2 result = *message;
	uint64 sampled[CLUSTER_KO_SHARED_NODE_LIMIT] = {0};
	uint64 before = cluster_membership_cut_generation();
	uint8 preimage[sizeof(domain) - 1 + 16 + CLUSTER_MAX_NODES * 9 + 64];
	uint8 *p = preimage;
	pg_cryptohash_ctx *hash;
	bool ok;

	StaticAssertStmt(CLUSTER_MAX_NODES == CLUSTER_KO_SHARED_MEMBER_BYTES * 8,
		"KO must cover the complete member table");
	if (before == 0 || cluster_node_id < 0 || cluster_node_id >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| !cluster_reconfig_capture_formation_snapshot_v1((uint16)(cluster_node_id + 1), &snapshot)
		|| !cluster_membership_cut_generation_current(before)
		|| snapshot.local_epoch == 0 || snapshot.local_epoch == UINT64_MAX
		|| snapshot.prebump_sync_active != 0 || snapshot.self_join_failed
		|| snapshot.reserved[0] != 0 || snapshot.reserved[1] != 0)
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
					 | snapshot.clean_departed_bitmap[n / 8]) & bit) != 0)
				return false;
			result.members[n / 8] |= bit;
			sampled[n] = boot;
		}
		*p++ = state;
		ko_cut_u64(&p, boot);
	}
	memcpy(p, snapshot.pending_join_bitmap, 16); p += 16;
	memcpy(p, snapshot.clean_departed_bitmap, 16); p += 16;
	memcpy(p, snapshot.removed_bitmap, 16); p += 16;
	memcpy(p, snapshot.excluded_bitmap, 16); p += 16;
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
	if (!ko_shared_members(&current, boots, &generation)
		|| boots[cluster_node_id] == 0
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
				&capability[i], &connection[i]) || connection[i] == 0
			|| (capability[i] & PGRAC_IC_HELLO_CAP_KO_SHARED_V2) == 0)
			return false;
	}
	/* A decoded relation result is still untrusted. In particular, one
	 * endpoint's new CONTROL connection cannot inherit the old cut. */
	for (int i = 0; i < 2; i++) {
		uint32 word, sampled;
		if (endpoints[i] == cluster_node_id)
			continue;
		if (!cluster_sf_peer_capability_word_sample(endpoints[i], PGRAC_IC_HELLO_CAP_KO_SHARED_V2,
				&word, &sampled) || word != capability[i] || sampled != connection[i])
			return false;
	}
	return cluster_wal_thread_current_v2_ref(&after)
		&& memcmp(&before, &after, sizeof(before)) == 0
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
		&& capability_generation != 0
		&& cluster_membership_cut_generation_current(generation)
		&& cluster_ko_shared_encode_v2(&current, observed, sizeof(observed))
		&& memcmp(expected, observed, sizeof(expected)) == 0;
}

static bool
ko_shared_space_current(const ClusterKoSharedMessageV2 *message)
{
	ClusterSpaceIdentity identity;

	return cluster_space_relation_read_identity(message->key.locator, &identity)
		&& identity.state == CLUSTER_SPACE_IDENTITY_LIVE
		&& identity.key.system_identifier == message->key.system_identifier
		&& identity.key.database_incarnation == message->key.database_incarnation
		&& RelFileLocatorEquals(identity.key.locator, message->key.locator)
		&& memcmp(identity.key.storage_uuid, message->key.storage_uuid, 16) == 0
		&& memcmp(identity.incarnation, message->incarnation, 16) == 0;
}

bool
cluster_ko_shared_peer_projection_v2(const ClusterKoSharedMessageV2 *request,
	int32 peer, ClusterKoSharedMessageV2 *out)
{
	ClusterKoSharedMessageV2 current, projected;
	ClusterWalSourceRef writer_before, writer_after;
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], generation, sampled_generation;
	uint32 capabilities, capability_generation;
	uint8 original[CLUSTER_KO_SHARED_V2_BYTES], observed[sizeof(original)];

	if (request == NULL || out == NULL || peer < 0 || peer >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| peer == cluster_node_id || request->verb != CLUSTER_KO_SHARED_REQUEST
		|| !cluster_enabled || !cluster_shared_config || RecoveryInProgress()
		|| (MyBackendType != B_BG_WRITER && MyBackendType != B_CHECKPOINTER)
		|| CurrentResourceOwner == NULL || CritSectionCount != 0)
		return false;
	generation = cluster_membership_cut_generation();
	if (generation == 0 || !cluster_wal_thread_current_v2_ref(&writer_before)
		|| !ko_shared_control_current(request))
		return false;
	current = *request;
	if (!ko_shared_members(&current, boots, &sampled_generation)
		|| sampled_generation != generation || boots[peer] == 0
		|| !cluster_ko_shared_encode_v2(request, original, sizeof(original))
		|| !cluster_ko_shared_encode_v2(&current, observed, sizeof(observed))
		|| memcmp(original, observed, sizeof(original)) != 0
		|| !cluster_sf_peer_capability_word_sample(peer, PGRAC_IC_HELLO_CAP_KO_SHARED_V2,
			&capabilities, &capability_generation)
		|| capability_generation == 0
		|| (capabilities & PGRAC_IC_HELLO_CAP_KO_SHARED_V2) == 0)
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
	const ClusterKoSharedMessageV2 *m = &context->request;
	ClusterKoSharedMessageV2 current = *m;
	ClusterWalSourceRef ref;
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], generation;

	return cluster_enabled && cluster_shared_config && !RecoveryInProgress()
		&& cluster_node_id == m->origin_node
		&& cluster_wal_thread_current_v2_ref(&ref)
		&& ref.claim.identity.origin_node_id == m->origin_node
		&& ref.claim.identity.origin_owner_incarnation == m->origin_boot
		&& ref.claim.identity.system_identifier == m->key.system_identifier
		&& ref.claim.database_incarnation == m->key.database_incarnation
		&& memcmp(ref.claim.identity.storage_uuid, m->key.storage_uuid, 16) == 0
		&& ko_shared_members(&current, boots, &generation)
		&& boots[cluster_node_id] == m->origin_boot
		&& current.epoch == m->epoch
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

static void
ko_completion_cancel(ClusterKoCompletionV2 *completion)
{
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
		return completion->owner == CurTransactionResourceOwner
			&& ko_native_transaction_current();
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
		if (completion->pid == MyProcPid && completion->native_transaction)
			completion->postcommit = event == XACT_EVENT_COMMIT && completion->space_observed
				&& !completion->native_pending && completion->owner == TopTransactionResourceOwner
				&& CurrentResourceOwner == TopTransactionResourceOwner
				&& CurTransactionResourceOwner == TopTransactionResourceOwner;
}

static void
ko_shared_resource_release(ResourceReleasePhase phase, bool commit,
	bool top, void *arg pg_attribute_unused())
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
			 * Keep only a prepared structural observation with this same
			 * top transaction until its explicit postcommit tail cleanup. */
			if (commit && top && completion->native_transaction && completion->postcommit
				&& completion->space_observed
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
	if (owned == NULL || owned->pid != MyProcPid || !ko_completion_owner_current(owned)
		|| CurrentResourceOwner == NULL || CritSectionCount != 0 || ko_state == NULL
		|| owned->slot >= CLUSTER_KO_SHARED_CAPACITY)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	*out = ko_state->contexts[owned->slot];
	valid = out->used && out->complete && !out->structure_owned
		&& out->pid == owned->pid && out->serial == owned->serial;
	SpinLockRelease(&ko_state->shared_lock);
	return valid && ko_shared_origin_current(out);
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
	const struct ClusterPageWalBindingV1 *terminal, const void *wal, Size wal_length)
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
		|| memcmp(change.identity.result.incarnation, terminal->version.segment_incarnation, 16) != 0)
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
	struct ClusterPageWalBindingV1 *terminal, void *wal, Size wal_length)
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
	struct ClusterPageWalBindingV1 *terminal, void *wal, Size wal_length)
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
cluster_ko_shared_structure_offer_v2(const ClusterKoCompletionV2 *completion,
	int32 peer, struct ClusterPiWritebackFactV2 *out)
{
	const ClusterKoCompletionV2 *owned = *ko_completion_link(completion);
	ClusterPiWritebackFactV2 value = {0};
	ClusterPiStructuralFactV2 *s = &value.proof.structural;
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	if (out == NULL || owned == NULL || !owned->postcommit
		|| !ko_structure_effect_observation(owned, &s->terminal.binding,
			wal, sizeof(wal))
		|| !cluster_ko_shared_read_v2(owned, peer, &s->ko)
		|| !cluster_space_structure_wal_decode(wal, sizeof(wal), &s->change))
		return false;
	value.kind = CLUSTER_PI_WRITEBACK_STRUCTURE_OFFER_V2;
	s->durability_flags = CLUSTER_PI_STRUCTURAL_WAL_FLUSHED
		| CLUSTER_PI_STRUCTURAL_SPACE_SYNC_READBACK | CLUSTER_PI_STRUCTURAL_KO_ALL_ACKED
		| CLUSTER_PI_STRUCTURAL_EFFECT_DURABLE;
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

	if (completion == NULL || *completion == NULL)
		return false;
	link = ko_completion_link(*completion);
	owned = *link;
	if (owned == NULL || !owned->postcommit
		|| !ko_structure_effect_observation(owned, &terminal, wal, sizeof(wal))
		|| !ko_completion_snapshot(owned, &before))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	if (memcmp(&ko_state->contexts[owned->slot], &before, sizeof(before)) == 0) {
		ClusterKoSharedContext *entry = &ko_state->contexts[owned->slot];
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
	pfree(owned);
	*completion = NULL;
	return true;
}

static bool
ko_shared_import_current(const ClusterKoSharedContext *context)
{
	ClusterKoSharedMessageV2 current = context->request;
	const ClusterWalSourceRef *source = &context->terminal.source;
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], generation;

	return cluster_node_id == current.peer_node && cluster_node_id != current.origin_node
		   && source->claim.identity.origin_node_id == current.origin_node
		   && source->claim.identity.origin_owner_incarnation == current.origin_boot
		   && cluster_ko_shared_cut_current_v2(&current)
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
	if (!context.used || !context.complete || !context.structure_owned || context.serial == 0
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
	struct ClusterPageWalBindingV1 *terminal, void *wal, Size wal_length)
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
cluster_ko_shared_structure_peer_v2(uint32 slot, uint64 serial, int32 peer,
									ClusterKoSharedMessageV2 *out)
{
	ClusterKoSharedContext context;
	return out != NULL && serial != 0 && ko_shared_structure_snapshot(slot, &context)
		   && context.serial == serial
		   && cluster_ko_shared_peer_projection_v2(&context.request, peer, out);
}

bool
cluster_ko_shared_structure_offer_next_v2(uint32 *cursor, int32 peer, uint64 *serial,
	struct ClusterPiWritebackFactV2 *out)
{
	if (cursor == NULL || serial == NULL || out == NULL
		|| *cursor >= CLUSTER_KO_SHARED_CAPACITY || peer < 0
		|| peer >= CLUSTER_KO_SHARED_NODE_LIMIT || peer == cluster_node_id)
		return false;
	for (uint32 i = *cursor; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoSharedContext context;
		ClusterPiWritebackFactV2 value = {0};
		ClusterPiStructuralFactV2 *s = &value.proof.structural;

		if (!ko_shared_structure_snapshot(i, &context)
			|| context.request.origin_node != cluster_node_id || context.peer_boots[peer] == 0)
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
		s->durability_flags = CLUSTER_PI_STRUCTURAL_WAL_FLUSHED
			| CLUSTER_PI_STRUCTURAL_SPACE_SYNC_READBACK | CLUSTER_PI_STRUCTURAL_KO_ALL_ACKED
			| CLUSTER_PI_STRUCTURAL_EFFECT_DURABLE;
		if (s->change.identity.action == CLUSTER_SPACE_WAL_TRUNCATE)
			s->durability_flags |= CLUSTER_PI_STRUCTURAL_BASE_DURABLE;
		*out = value;
		*serial = context.serial;
		*cursor = i + 1;
		return true;
	}
	return false;
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

/* This is still the original backend/transaction owner. A pending-delete
 * return does not certify its physical effects, and no background ownership
 * is created here. Keep this cleanup independent of now-stale cluster scope. */
void
cluster_ko_shared_postcommit_cleanup_v2(void)
{
	ClusterKoCompletionV2 **link = &ko_completions;
	if (CurrentResourceOwner == NULL || CurrentResourceOwner != TopTransactionResourceOwner
		|| CurTransactionResourceOwner != TopTransactionResourceOwner)
		return;
	while (*link != NULL) {
		ClusterKoCompletionV2 *completion = *link;
		if (completion->pid == MyProcPid && completion->owner == CurrentResourceOwner
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
cluster_ko_shared_claim_v2(const ClusterSpaceIdentityKey *key,
	const uint8 incarnation[16], ClusterKoCompletionV2 **out)
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
	if (out == NULL || peer < 0 || peer >= CLUSTER_KO_SHARED_NODE_LIMIT
		|| peer == cluster_node_id || !ko_completion_snapshot(completion, &context)
		|| context.peer_boots[peer] == 0)
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
		if (!entry->used || entry->complete || entry->request.batch_id != message->batch_id
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
	/* Exit owns all this process's ResourceOwners, not just its current one. */
	while (ko_completions != NULL) {
		ClusterKoCompletionV2 *completion = ko_completions;
		ko_completions = completion->next;
		pfree(completion);
	}
}

void
cluster_ko_lmon_tick_v2(void)
{
	if (ko_state == NULL || !cluster_shared_config || !AmLmonProcess())
		return;
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
			return;
		/* The IC owner retains accepted WOULD_BLOCK sends. Other failures
		 * cannot produce an ACK; the original barrier remains unfulfilled. */
		if (ko_shared_request_owned(&message) && ko_shared_control_current(&message)
			&& cluster_ko_shared_encode_v2(&message, bytes, sizeof(bytes)))
			(void)cluster_ic_send_envelope(
				message.verb == CLUSTER_KO_SHARED_REQUEST ? PGRAC_IC_MSG_KO_FLUSH : PGRAC_IC_MSG_KO_FLUSH_ACK,
				message.verb == CLUSTER_KO_SHARED_REQUEST ? message.peer_node : message.origin_node,
				bytes, sizeof(bytes));
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
		|| ko_state->send_head >= CLUSTER_KO_SHARED_CAPACITY) {
		result = CLUSTER_NORMAL_STOP_INVALID;
		why = "KO_SHARED_QUEUE_INVALID";
	} else if (ko_state->send_count != 0) {
		result = CLUSTER_NORMAL_STOP_PENDING;
		why = "KO_SHARED_SEND_PENDING";
	}
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (ko_state->contexts[i].used && result == CLUSTER_NORMAL_STOP_READY) {
			result = CLUSTER_NORMAL_STOP_PENDING;
			why = ko_state->contexts[i].structure_owned ? "KO_SHARED_STRUCTURE_OWNED"
				: ko_state->contexts[i].complete ? "KO_SHARED_COMPLETION_OWNED"
				: "KO_SHARED_BARRIER_PENDING";
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
		|| !ko_shared_members(request, boots, &generation)
		|| boots[cluster_node_id] == 0
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

static int
ko_shared_reserve(const ClusterKoSharedMessageV2 *request,
	const uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], ClusterKoCompletionV2 *completion)
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
	ClusterKoSharedMessageV2 request = {0};
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
			ClusterKoSharedContext scope = {0};
			scope.request = request;
			memcpy(scope.peer_boots, boots, sizeof(boots));
			ok = ko_shared_origin_current(&scope) && ko_shared_space_current(&request);
			if (ok) KO_BUMP(native_count);
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

		if (hdr.epoch == 0 || hdr.epoch != cluster_epoch_get_current())
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
ko_flush_and_wait_ack(RelFileLocator rloc, char relpersistence, ClusterKoCompletionV2 *completion)
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

		if (cluster_shared_config)
			ok = completion != NULL ? ko_run_shared_barrier_impl(rloc, completion) : ko_run_shared_barrier(rloc);
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
		completion->native_pending = true;
		return;
	}
	ko_flush_and_wait_ack(rloc, relpersistence, NULL);
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
		ko_flush_and_wait_ack(rloc, relpersistence, completion);
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
	KoFlushHeader legacy = {0};
	ClusterKoSharedMessageV2 qualified = {0};
	uint32 head, tail, next;
	bool shared = cluster_shared_config;

	if (ko_state == NULL || env == NULL || payload == NULL)
		return;
	if (shared) {
		if (!cluster_ko_shared_decode_v2(payload, env->payload_length, &qualified)
			|| qualified.verb != CLUSTER_KO_SHARED_REQUEST
			|| qualified.peer_node != cluster_node_id
			|| qualified.origin_node != env->source_node_id
			|| !ko_shared_control_current(&qualified))
			return;
		legacy.batch_id = qualified.batch_id;
		legacy.epoch = qualified.epoch;
		legacy.source_node = qualified.origin_node;
		legacy.spc_oid = qualified.key.locator.spcOid;
		legacy.db_oid = qualified.key.locator.dbOid;
		legacy.rel_number = qualified.key.locator.relNumber;
		hdr = &legacy;
	} else if (env->payload_length != (uint32)sizeof(KoFlushHeader))
		return;
	if (hdr->epoch == 0 || hdr->epoch != cluster_epoch_get_current())
		return; /* No request from another configuration can drain here. */
	if (hdr->source_node < 0 || hdr->source_node >= CLUSTER_MAX_NODES
		|| env->source_node_id != (uint32)hdr->source_node
		|| cluster_conf_lookup_node(hdr->source_node) == NULL)
		return;

	/* A new object flush/drop cannot cross the normal-stop modifier seal.
	 * Existing ACK completion still follows its original exact wait entry. */
	if (!cluster_normal_stop_service_new_work(true))
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
	ko_state->inbound[tail].shared = shared;
	ko_state->inbound[tail].qualified = qualified;
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

	if (ko_state == NULL || env == NULL || payload == NULL)
		return;
	if (cluster_shared_config) {
		ClusterKoSharedMessageV2 ack;
		bool matched = false;

		if (!cluster_ko_shared_decode_v2(payload, env->payload_length, &ack)
			|| ack.verb != CLUSTER_KO_SHARED_ACK || ack.status != CLUSTER_KO_SHARED_DONE
			|| ack.origin_node != cluster_node_id || ack.peer_node != env->source_node_id
			|| !ko_shared_control_current(&ack))
			return;
		SpinLockAcquire(&ko_state->shared_lock);
		for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
			const ClusterKoSharedContext *entry = &ko_state->contexts[i];
			ClusterKoSharedMessageV2 expected;
			if (!entry->used || entry->complete || entry->request.batch_id != ack.batch_id)
				continue;
			expected = entry->request;
			expected.peer_node = ack.peer_node;
			expected.peer_boot = entry->peer_boots[ack.peer_node];
			matched = cluster_ko_shared_ack_matches_v2(&expected, &ack);
			break;
		}
		SpinLockRelease(&ko_state->shared_lock);
		if (matched) {
			cluster_sinval_ack_wait_record(ack.batch_id, ack.peer_node);
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
	if (hdr->epoch == 0 || hdr->epoch != cluster_epoch_get_current())
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
		if (slot.epoch == 0 || slot.epoch != cluster_epoch_get_current()
			|| slot.shared != cluster_shared_config
			|| (slot.shared && (!ko_shared_control_current(&slot.qualified)
				|| !ko_shared_space_current(&slot.qualified))))
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
			|| (slot.shared && !ko_shared_control_current(&slot.qualified)))
			goto next;

		/*
		 * Flush THEN invalidate (the abort-safe order): write any dirty buffers to
		 * shared storage so a rolled-back DROP loses no data, then drop ALL of the
		 * relfilenode's buffers so no later writeback can scribble into the file or
		 * a reused relfilenode.
		 */
		FlushRelationsAllBuffers(&smgr, 1);
		if (slot.epoch != cluster_epoch_get_current()
			|| (slot.shared && !ko_shared_control_current(&slot.qualified)))
			goto next;
		if (cluster_shared_config && cluster_smgr_which_for(rloc, InvalidBackendId) == 1) {
			/* Buffer writeout alone does not make shared DATA durable.  An
			 * I/O error must retain the buffers and leave the origin unacked. */
			for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
				if (smgrexists(smgr, fork))
					smgrimmedsync(smgr, fork);
				if (slot.epoch != cluster_epoch_get_current()
					|| (slot.shared && !ko_shared_control_current(&slot.qualified)))
					goto next;
			}
		}
		/* Check SPACE before invalidation; rereading it afterwards would
		 * recreate precisely the buffer the barrier has just discarded. */
		if (slot.shared && !ko_shared_space_current(&slot.qualified))
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
			|| (slot.shared && !ko_shared_control_current(&slot.qualified)))
			goto next;

		if (slot.shared) {
			slot.qualified.verb = CLUSTER_KO_SHARED_ACK;
			slot.qualified.status = CLUSTER_KO_SHARED_DONE;
			(void)ko_shared_enqueue(&slot.qualified);
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
