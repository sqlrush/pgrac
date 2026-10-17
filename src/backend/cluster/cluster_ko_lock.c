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
#include "storage/proc.h"
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
#define KO_DROP_IDENTITY_CAPACITY 1024
#define KO_DROP_IDENTITY_BUCKETS 128

typedef struct ClusterKoSharedContext {
	bool used;
	bool v3;
	bool committed_discard; /* Immutable PKO4 purpose, not completion. */
	bool complete;
	/* Durable COMMIT handed off this registration before its drain ran.
	 * Neither native execution nor AX release may consume it as complete. */
	bool committed_drain_pending;
	bool origin_checkpoint;
	bool origin_data_closed;
	bool structure_owned;
	bool structure_drop_pending;
	bool structure_drop_failed; /* Original executor requires retained-WAL recovery. */
	uint16 structure_peers_accepted;
	uint16 request_sent_peers;
	uint16 request_uncertain_peers;
	uint16 request_sending_peers;
	uint16 request_done_peers;
	uint16 request_failed_peers;
	bool origin_detached;
	bool ax_release_requested;
	bool ax_failed;
	int cancel_sqlerrcode;
	uint32 ax_lock_group;
	ClusterGesReleaseAttempt ax_release;
	uint32 structure_scan;
	uint64 structure_scan_freeze;
	int32 pid;
	int32 drop_executor_pid;
	uint64 serial;
	uint32 drop_identity;
	uint64 drop_identity_serial;
	/* Borrowed original committed carrier, valid only before I/O closing.
	 * Its allocation cannot be freed while a directory call is entered. */
	dsa_pointer pi_native_owner;
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
	bool committed_discard;
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

typedef enum KoPeerReplyState {
	KO_PEER_REPLY_NONE,
	KO_PEER_REPLY_QUEUED,
	KO_PEER_REPLY_SENDING,
	KO_PEER_REPLY_TRANSPORT_OWNED,
	KO_PEER_REPLY_UNKNOWN,
	KO_PEER_REPLY_REJECTED
} KoPeerReplyState;

typedef struct ClusterKoPeerFenceV3 {
	ClusterKoPeerFenceStateV3 state;
	bool committed_discard;
	bool physical_started;
	bool physical_applied;
	uint8 physical_phase;
	uint32 physical_cursor;
	uint64 physical_revision;
	ResourceXGateSnapshot physical_gate;
	bool ack_queued;
	bool failure_queued;
	uint8 reply_state;
	int failure_sqlerrcode;
	bool local_scan_complete;
	uint32 local_scan_cursor;
	uint64 local_scan_revision;
	uint64 local_writeback_revision;
	ResourceXGateSnapshot local_scan_gate;
	uint64 serial;
	uint64 peer_route_generation;
	ClusterKoSharedMessageV3 request;
	uint8 encoded[CLUSTER_KO_SHARED_V3_BYTES];
	ClusterGesHolderProbe holder_probe;
	TimestampTz holder_sent_at;
	uint32 drop_identity; /* One-based index; reserved before physical work. */
} ClusterKoPeerFenceV3;

typedef enum KoDropIdentityState {
	KO_DROP_IDENTITY_FREE,
	KO_DROP_IDENTITY_RESERVED,
	KO_DROP_IDENTITY_CLOSING,
	KO_DROP_IDENTITY_RETIRED
} KoDropIdentityState;

/* Exact negative identities outlive the reusable active operation slots.
 * They are instance-local and are never authority for recovery or DATA. */
typedef struct KoDropIdentity {
	KoDropIdentityState state;
	bool committed_discard;
	uint32 next;
	uint64 fence_serial;
	ClusterKoSharedMessageV3 request;
} KoDropIdentity;

typedef struct KoDirectoryIo {
	RelFileLocator locator;
	int32 pid;
	uint64 serial;
} KoDirectoryIo;

typedef struct ClusterKoCleanerWorkV3 {
	bool used;
	RelFileLocator locator;
	ClusterCtrcCleanerReadScopeV3 scope;
} ClusterKoCleanerWorkV3;

/* References to running native extension/creation calls, not an execution
 * queue. InvalidForkNumber names a constructor spanning MAIN and SPACE.
 * Physical work whose completion is unknown survives local ERROR. */
typedef struct ClusterKoExtendWorkV3 {
	bool used;
	bool failed;
	bool raw_copy;
	bool buffer_copy;
	int32 pid;
	uint64 serial;
	uint64 epoch;
	RelFileLocator locator;
	ForkNumber fork;
	ClusterWalSourceRef writer;
} ClusterKoExtendWorkV3;

typedef struct ClusterKoShared {
	pg_atomic_uint64 flush_count;		 /* barriers initiated (enqueuer) */
	pg_atomic_uint64 ack_received_count; /* peer DONE ACKs recorded (enqueuer) */
	pg_atomic_uint64 failclosed_count;	 /* 53RAA fail-closed (enqueuer) */
	pg_atomic_uint64 native_count;		 /* no-op: single-node / no peer / private */
	pg_atomic_uint64 lockfail_count;	 /* KO(X) acquire failed (best-effort; barrier ran) */
	pg_atomic_uint64 peer_apply_count;	 /* flush+drop applied + ACK'd (peer) */
	pg_atomic_uint64 inbound_full_count; /* KO inbound ring full (peer -> no ACK) */
	pg_atomic_uint64 origin_checkpoint_restart_count[CLUSTER_KO_ORIGIN_REASON_COUNT];
	pg_atomic_uint64 origin_checkpoint_wait_count[CLUSTER_KO_ORIGIN_REASON_COUNT];
	pg_atomic_uint64 origin_checkpoint_logged_mask;
	pg_atomic_uint64 origin_checkpoint_pending_logged_mask;
	ClusterKoOriginCheckpointPendingV1 origin_checkpoint_pending;
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
	bool send_committed_discard[CLUSTER_KO_SHARED_CAPACITY];
	uint64 context_serial;
	/* Even publication version; odd only during a bounded GRD cleanup. */
	pg_atomic_uint64 ax_publication_revision;
	uint32 ax_release_cursor;
	ClusterKoSharedContext contexts[CLUSTER_KO_SHARED_CAPACITY];
	/* Peer operations are not owned by an origin backend ResourceOwner.
	 * Passive READY/terminal identities survive its context cleanup. */
	uint64 peer_fence_serial;
	pg_atomic_uint64 peer_admission_revision;
	uint64 holder_probe_nonce;
	uint32 holder_probe_cursor;
	ClusterKoPeerFenceV3 peer_fences[CLUSTER_KO_SHARED_CAPACITY];
	uint32 drop_free;
	uint32 drop_buckets[KO_DROP_IDENTITY_BUCKETS];
	KoDropIdentity drop_identities[KO_DROP_IDENTITY_CAPACITY];
	uint64 directory_io_serial;
	KoDirectoryIo directory_io[CLUSTER_KO_SHARED_CAPACITY];
	ClusterKoCleanerWorkV3 cleaner_work[CLUSTER_CTRC_CLEANER_WORKERS];
	uint64 extend_serial;
	ClusterKoExtendWorkV3 extend_work[CLUSTER_KO_SHARED_CAPACITY];
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
static ClusterKoPeerFenceHandleV3 ko_peer_attempt;
static bool ko_peer_attempt_active;
static bool ko_shared_v3_peer_identity_current(const ClusterKoSharedMessageV3 *message);
static ClusterKoExtendWorkV3 ko_extend_local;
static uint32 ko_extend_slot;
static bool ko_extend_write_armed, ko_extend_materialized, ko_extend_aborting;
static ResourceOwner ko_extend_owner;
static ClusterKoExtendWorkV3 ko_create_local;
static uint32 ko_create_slot;
static bool ko_create_write_armed, ko_create_aborting;
static ResourceOwner ko_create_owner;
/* Private cursor of the original serial copy loop, not a work queue. */
static struct {
	uint64 serial;
	ForkNumber fork;
	BlockNumber nblocks;
	BlockNumber next;
	BlockNumber target;
} ko_buffer_copy;
static bool ko_exit_registered;
static dsa_area *ko_native_area;

typedef struct KoAxBinding KoAxBinding;
static bool ko_capture_relation_ax_binding(Relation rel, KoAxBinding *out);
static bool ko_shared_enqueue_v3(const ClusterKoSharedMessageV3 *message, uint64 local_generation);
static bool ko_shared_enqueue_object(const ClusterKoSharedMessageV3 *message, uint64 local_generation,
								 bool committed_discard);

typedef struct ClusterKoNativeContinuation {
	dsa_pointer next;
	/* The original transaction publication returned before committed drain.
	 * This records a milestone, never READY, SPACE or deletion authority. */
	bool publication_observed;
	bool buffers_released;
	/* The original owner has passed the pre-delete handoff. This is
	 * deliberately separate from buffers_released: DropRelationsAllBuffers
	 * must still provide the latter as a real post-invalidation fact. */
	bool predelete_ready;
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
	/* Old DATA only: the final SPACE write creates a separate obligation. */
	bool origin_data_closed;
	dsa_pointer native_unready;
	/* Native DDL keeps its verified barrier in the original ResourceOwner,
	 * releasing the transport slot before the next relation's barrier. */
	bool barrier_private;
	bool barrier_v3;
	/* Immutable protocol purpose, independent of physical completion. */
	bool barrier_committed_discard;
	uint16 barrier_ready_peers;
	uint32 drop_identity;
	uint64 drop_identity_serial;
	dsa_pointer native_allocation;
	ClusterKoNativeContinuation *native_reserved;
	ClusterKoSharedMessageV2 barrier_request;
	/* v3 captures these at registration; completion is checked separately. */
	ClusterKoSharedMessageV3 barrier_request_v3;
	uint64 barrier_boots[CLUSTER_KO_SHARED_NODE_LIMIT];
	ClusterPageWalBindingV1 terminal;
	uint8 structure[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	struct ClusterKoCompletionV2 *next;
};

static ClusterKoCompletionV2 *ko_completions;
static bool ko_resource_registered;
static void ko_shared_backend_exit(int code, Datum arg);
static void ko_shared_resource_release(ResourceReleasePhase phase, bool commit, bool top,
									   void *arg);
static void ko_shared_xact_event(XactEvent event, void *arg);
static bool ko_shared_v3_local_binding_current(const ClusterKoSharedMessageV3 *request);
static bool ko_shared_v3_space_matches(const ClusterKoSharedMessageV3 *message,
									   const ClusterSpaceIdentity *identity);
static bool ko_shared_registration_snapshot_v3(const ClusterKoCompletionV2 *completion,
												   ClusterKoSharedContext *out);
static dsa_area *ko_native_attach(void);
static bool ko_shared_ready_v3(const ClusterKoSharedContext *context);
static bool ko_origin_space_read_begin(RelFileLocator locator, ForkNumber fork,
	BlockNumber block, ClusterKoPeerAdmissionV3 *out);
static bool ko_origin_space_read_current(const ClusterKoPeerAdmissionV3 *sample,
	RelFileLocator locator, ForkNumber fork, BlockNumber block);

/* Process-local storage state belongs to the original active KO context.
 * It survives a checkpointer ERROR, but can never migrate to another process. */
struct ClusterKoDropWorkV2 {
	ResourceOwner owner;
	int32 pid;
	uint32 slot;
	Size storage_bytes;
	bool abandoned;
	/* Only a hint for this original CP work, never a new physical result. */
	bool local_scan_complete;
	uint32 local_scan_cursor;
	uint64 local_scan_revision;
	uint64 local_writeback_revision;
	ResourceXGateSnapshot local_scan_gate;
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
		for (int i = 0; i < CLUSTER_KO_ORIGIN_REASON_COUNT; i++) {
			pg_atomic_init_u64(&ko_state->origin_checkpoint_restart_count[i], 0);
			pg_atomic_init_u64(&ko_state->origin_checkpoint_wait_count[i], 0);
		}
		pg_atomic_init_u64(&ko_state->origin_checkpoint_logged_mask, 0);
		pg_atomic_init_u64(&ko_state->origin_checkpoint_pending_logged_mask, 0);
		memset(&ko_state->origin_checkpoint_pending, 0,
			   sizeof(ko_state->origin_checkpoint_pending));
		ko_state->origin_checkpoint_pending.registry_slot = UINT32_MAX;
		pg_atomic_init_u32(&ko_state->inbound_head, 0);
		pg_atomic_init_u32(&ko_state->inbound_tail, 0);
		SpinLockInit(&ko_state->shared_lock);
		ko_state->send_head = ko_state->send_count = 0;
		ko_state->send_v3_head = ko_state->send_v3_count = 0;
		ko_state->context_serial = 0;
		pg_atomic_init_u64(&ko_state->ax_publication_revision, 2);
		ko_state->ax_release_cursor = 0;
		memset(ko_state->contexts, 0, sizeof(ko_state->contexts));
		ko_state->peer_fence_serial = 0;
		pg_atomic_init_u64(&ko_state->peer_admission_revision, 1);
		ko_state->holder_probe_nonce = 0;
		ko_state->holder_probe_cursor = 0;
		memset(ko_state->peer_fences, 0, sizeof(ko_state->peer_fences));
		memset(ko_state->drop_buckets, 0, sizeof(ko_state->drop_buckets));
		memset(ko_state->drop_identities, 0, sizeof(ko_state->drop_identities));
		for (uint32 i = 0; i < KO_DROP_IDENTITY_CAPACITY; i++)
			ko_state->drop_identities[i].next = i + 1 < KO_DROP_IDENTITY_CAPACITY ? i + 2 : 0;
		ko_state->drop_free = 1;
		ko_state->directory_io_serial = 0;
		memset(ko_state->directory_io, 0, sizeof(ko_state->directory_io));
		memset(ko_state->cleaner_work, 0, sizeof(ko_state->cleaner_work));
		ko_state->extend_serial = 0;
		memset(ko_state->extend_work, 0, sizeof(ko_state->extend_work));
		memset(&ko_extend_local, 0, sizeof(ko_extend_local));
		ko_extend_aborting = false;
		ko_extend_owner = NULL;
		memset(&ko_create_local, 0, sizeof(ko_create_local));
		ko_create_write_armed = ko_create_aborting = false;
		ko_create_owner = NULL;
		memset(&ko_buffer_copy, 0, sizeof(ko_buffer_copy));
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

static const char *const ko_origin_checkpoint_reason_names[CLUSTER_KO_ORIGIN_REASON_COUNT] = {
	[CLUSTER_KO_ORIGIN_REASON_PROGRESS] = "progress",
	[CLUSTER_KO_ORIGIN_REASON_CUT_CURRENT] = "cut_current",
	[CLUSTER_KO_ORIGIN_REASON_PEER_REVISION] = "peer_revision",
	[CLUSTER_KO_ORIGIN_REASON_GATE_NOT_OPEN] = "gate_not_open",
	[CLUSTER_KO_ORIGIN_REASON_GATE_CHANGED] = "gate_changed",
	[CLUSTER_KO_ORIGIN_REASON_CTRC_NOT_READY] = "ctrc_not_ready",
	[CLUSTER_KO_ORIGIN_REASON_CLEANER_PENDING] = "cleaner_pending",
	[CLUSTER_KO_ORIGIN_REASON_EXTEND_PENDING] = "extend_pending",
	[CLUSTER_KO_ORIGIN_REASON_DRAIN_WAIT] = "drain_wait",
	[CLUSTER_KO_ORIGIN_REASON_DRAIN_MORE] = "drain_more",
	[CLUSTER_KO_ORIGIN_REASON_DRAIN_REDIRTIED] = "drain_redirtied",
	[CLUSTER_KO_ORIGIN_REASON_PI_SCAN_PENDING] = "pi_scan_pending",
	[CLUSTER_KO_ORIGIN_REASON_PI_SCAN_MORE] = "pi_scan_more",
	[CLUSTER_KO_ORIGIN_REASON_WB_NOT_CLOSED] = "wb_not_closed",
	[CLUSTER_KO_ORIGIN_REASON_WB_REVISION] = "wb_revision",
	[CLUSTER_KO_ORIGIN_REASON_CONTEXT_CHANGED] = "context_changed"
};

const char *
cluster_ko_origin_checkpoint_reason_name(ClusterKoOriginCheckpointReason reason)
{
	return reason < CLUSTER_KO_ORIGIN_REASON_COUNT
		? ko_origin_checkpoint_reason_names[reason]
		: "unknown";
}

static int
ko_origin_checkpoint_reason_from_name(const char *name)
{
	if (name == NULL)
		return -1;
	for (int i = 0; i < CLUSTER_KO_ORIGIN_REASON_COUNT; i++)
		if (strcmp(name, ko_origin_checkpoint_reason_names[i]) == 0)
			return i;
	return -1;
}

uint64
cluster_ko_origin_checkpoint_restart_count(const char *reason)
{
	int index = ko_origin_checkpoint_reason_from_name(reason);
	return ko_state != NULL && index >= 0
		? pg_atomic_read_u64(&ko_state->origin_checkpoint_restart_count[index])
		: 0;
}

uint64
cluster_ko_origin_checkpoint_wait_count(const char *reason)
{
	int index = ko_origin_checkpoint_reason_from_name(reason);
	return ko_state != NULL && index >= 0
		? pg_atomic_read_u64(&ko_state->origin_checkpoint_wait_count[index])
		: 0;
}

bool
cluster_ko_origin_checkpoint_pending_identity(ClusterKoOriginCheckpointPendingV1 *out)
{
	bool valid;

	if (out == NULL || ko_state == NULL)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	*out = ko_state->origin_checkpoint_pending;
	valid = out->valid;
	SpinLockRelease(&ko_state->shared_lock);
	return valid;
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

/* The purpose is fixed by the original operation, never inferred from a
 * matching locator or a later COMMIT marker. Each codec checks its own wire
 * version before decoding the common bounded identity. */
static bool
ko_shared_object_encode(const ClusterKoSharedMessageV3 *message, bool committed_discard,
						void *bytes, size_t length)
{
	ClusterKoSharedMessageV4 newer;

	if (!committed_discard)
		return cluster_ko_shared_encode_v3(message, bytes, length);
	if (message == NULL)
		return false;
	newer.object = *message;
	return cluster_ko_shared_encode_v4(&newer, bytes, length);
}

static bool
ko_shared_object_decode(const void *bytes, size_t length, ClusterKoSharedMessageV3 *message,
						bool *committed_discard)
{
	ClusterKoSharedMessageV4 newer;

	if (bytes == NULL || message == NULL || committed_discard == NULL
		|| length != CLUSTER_KO_SHARED_V4_BYTES)
		return false;
	if (memcmp(bytes, "PKO4", 4) == 0) {
		if (!cluster_ko_shared_decode_v4(bytes, length, &newer))
			return false;
		*message = newer.object;
		*committed_discard = true;
		return true;
	}
	*committed_discard = false;
	return cluster_ko_shared_decode_v3(bytes, length, message);
}

/* R2: v3 retains the exact v2 formation/WAL cut, but binds the peer check to
 * the independently negotiated object-drain capability and the immutable AX
 * suffix. No maintenance identity read is part of this control predicate. */
static bool
ko_shared_object_control_at_generation(const ClusterKoSharedMessageV3 *message,
									 uint64 local_generation, bool committed_discard)
{
	const uint32 required_capabilities
		= PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1
		  | (committed_discard ? PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1
							   : PGRAC_IC_HELLO_CAP_KO_OBJECT_DRAIN_V3);
	ClusterKoSharedMessageV3 current;
	ClusterWalSourceRef ref;
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], generation;
	uint32 capabilities, capability_generation;
	uint8 expected[CLUSTER_KO_SHARED_V3_BYTES], observed[sizeof(expected)];
	int32 current_master, other;

	/* A readable runtime source/cut or retained fence is not a recovery
	 * write grant. Recovery keeps its own IR/isolation/ROOT-qualified owner. */
	if (message == NULL || local_generation == 0 || local_generation == UINT64_MAX
		|| RecoveryInProgress() || cluster_interconnect_tier != CLUSTER_IC_TIER_1
		|| (committed_discard
			&& (cluster_ic_local_capability_word() & required_capabilities) != required_capabilities))
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
		|| !ko_shared_object_encode(message, committed_discard, expected, sizeof(expected))
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
		|| !ko_shared_object_encode(&current, committed_discard, observed, sizeof(observed)))
		return false;
	return memcmp(expected, observed, sizeof(expected)) == 0
		   && cluster_grd_lookup_master_gen(&message->ax_resid, &generation) == current_master
		   && generation == local_generation;
}

static bool
ko_shared_v3_control_at_generation(const ClusterKoSharedMessageV3 *message, uint64 local_generation)
{
	return ko_shared_object_control_at_generation(message, local_generation, false);
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

static uint32
ko_drop_bucket(RelFileLocator locator)
{
	uint32 hash = locator.spcOid * 2654435761U;
	hash ^= locator.dbOid * 2246822519U;
	hash ^= locator.relNumber * 3266489917U;
	return hash % KO_DROP_IDENTITY_BUCKETS;
}

static bool
ko_drop_closed_locked(RelFileLocator locator)
{
	uint32 link = ko_state->drop_buckets[ko_drop_bucket(locator)];

	for (unsigned n = 0; link != 0; n++) {
		const KoDropIdentity *identity;
		if (link > KO_DROP_IDENTITY_CAPACITY || n >= KO_DROP_IDENTITY_CAPACITY)
			return true;
		identity = &ko_state->drop_identities[link - 1];
		if (identity->state != KO_DROP_IDENTITY_RESERVED
			&& RelFileLocatorEquals(identity->request.base.key.locator, locator))
			return true;
		link = identity->next;
	}
	return false;
}

static bool
ko_directory_drop_at_least(RelFileLocator locator, uint8 minimum_state)
{
	bool committed = false;
	uint32 link;

	if (!cluster_enabled || !cluster_shared_config || cluster_interconnect_tier != CLUSTER_IC_TIER_1
		|| ko_state == NULL || locator.spcOid == 0 || locator.relNumber == 0
		|| RecoveryInProgress())
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	link = ko_state->drop_buckets[ko_drop_bucket(locator)];
	for (unsigned n = 0; link != 0; n++) {
		const KoDropIdentity *identity;

		/* Unlike the refusal predicate, unknown/corrupt state is never
		 * positive evidence that the original queued work may be canceled. */
		if (link > KO_DROP_IDENTITY_CAPACITY || n >= KO_DROP_IDENTITY_CAPACITY) {
			committed = false;
			break;
		}
		identity = &ko_state->drop_identities[link - 1];
		if (identity->state < KO_DROP_IDENTITY_RESERVED
			|| identity->state > KO_DROP_IDENTITY_RETIRED
			|| identity->fence_serial == 0
			|| (identity->request.operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP
				&& (!identity->committed_discard
					|| identity->request.operation != CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD))) {
			committed = false;
			break;
		}
		if (identity->state >= minimum_state
			&& RelFileLocatorEquals(identity->request.base.key.locator, locator))
			committed = true;
		link = identity->next;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return committed;
}

bool
cluster_ko_directory_drop_committed_v3(RelFileLocator locator)
{
	return ko_directory_drop_at_least(locator, KO_DROP_IDENTITY_CLOSING);
}

bool
cluster_ko_directory_drop_retired_v3(RelFileLocator locator)
{
	/* Pure directory retirement needs the original entered-call drain too.
	 * This immutable identity survives active fence/AX slot reclamation. */
	return ko_directory_drop_at_least(locator, KO_DROP_IDENTITY_RETIRED);
}

/* The original SI executor reserves before physical work; a native origin
 * reserves before REQUEST. COMMIT never allocates; exhaustion keeps the
 * original negative operation owner. */
static bool
ko_drop_reserve_object_locked(const ClusterKoSharedMessageV3 *request, uint64 serial,
							 uint32 *index, bool committed_discard)
{
	KoDropIdentity *identity;
	uint32 link, bucket;

	if (!committed_discard && request->operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP)
		return true;
	if (request->operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP
		&& request->operation != CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD)
		return false;
	if (*index != 0) {
		if (*index > KO_DROP_IDENTITY_CAPACITY)
			return false;
		identity = &ko_state->drop_identities[*index - 1];
		return identity->state == KO_DROP_IDENTITY_RESERVED
			&& identity->committed_discard == committed_discard
			&& identity->fence_serial == serial
			&& memcmp(&identity->request, request, sizeof(*request)) == 0;
	}
	link = ko_state->drop_free;
	if (link == 0 || link > KO_DROP_IDENTITY_CAPACITY
		|| ko_drop_closed_locked(request->base.key.locator))
		return false;
	identity = &ko_state->drop_identities[link - 1];
	if (identity->state != KO_DROP_IDENTITY_FREE)
		return false;
	ko_state->drop_free = identity->next;
	bucket = ko_drop_bucket(request->base.key.locator);
	memset(identity, 0, sizeof(*identity));
	identity->state = KO_DROP_IDENTITY_RESERVED;
	identity->committed_discard = committed_discard;
	identity->request = *request;
	identity->fence_serial = serial;
	identity->next = ko_state->drop_buckets[bucket];
	ko_state->drop_buckets[bucket] = link;
	*index = link;
	return true;
}

static bool
ko_drop_reserve_locked(const ClusterKoSharedMessageV3 *request, uint64 serial, uint32 *index)
{
	return ko_drop_reserve_object_locked(request, serial, index, false);
}

/* PKO4 itself is the authenticated post-COMMIT request. A catalog marker
 * can precede it on the independent SI ring, so never infer this purpose
 * from marker arrival or a matching locator. The negative identity remains
 * after the active slot is reused. No KO lock spans buffer or directory work.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static KoDropIdentity *
ko_peer_discard_identity_locked(const ClusterKoPeerFenceV3 *entry)
{
	KoDropIdentity *identity;

	if (!entry->committed_discard || entry->drop_identity == 0
		|| entry->drop_identity > KO_DROP_IDENTITY_CAPACITY)
		return NULL;
	identity = &ko_state->drop_identities[entry->drop_identity - 1];
	return identity->committed_discard && identity->fence_serial == entry->serial
		&& identity->state >= KO_DROP_IDENTITY_RESERVED
		&& identity->state <= KO_DROP_IDENTITY_RETIRED
		&& memcmp(&identity->request, &entry->request, sizeof(entry->request)) == 0
		? identity : NULL;
}

static bool
ko_drop_release_locked(const ClusterKoSharedMessageV3 *request, uint64 serial, uint32 index)
{
	KoDropIdentity *identity;
	uint32 *link;

	if (index == 0)
		return true;
	if (index > KO_DROP_IDENTITY_CAPACITY)
		return false;
	identity = &ko_state->drop_identities[index - 1];
	if (identity->fence_serial != serial
		|| memcmp(&identity->request, request, sizeof(*request)) != 0)
		return false;
	/* A committed DROP never reopens this old locator during this instance.
	 * The native MAIN placeholder prevents its reuse by a new object. */
	if (identity->state == KO_DROP_IDENTITY_RETIRED)
		return true;
	if (identity->state != KO_DROP_IDENTITY_RESERVED)
		return false;
	link = &ko_state->drop_buckets[ko_drop_bucket(identity->request.base.key.locator)];
	for (unsigned n = 0; *link != index; n++) {
		if (*link == 0 || *link > KO_DROP_IDENTITY_CAPACITY || n >= KO_DROP_IDENTITY_CAPACITY)
			return false;
		link = &ko_state->drop_identities[*link - 1].next;
	}
	*link = identity->next;
	memset(identity, 0, sizeof(*identity));
	identity->next = ko_state->drop_free;
	ko_state->drop_free = index;
	return true;
}

static bool
ko_directory_io_pending_locked(RelFileLocator locator)
{
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (ko_state->directory_io[i].serial != 0
			&& RelFileLocatorEquals(ko_state->directory_io[i].locator, locator))
			return true;
	return false;
}

/* Original native continuation reference; promotion changes the execution
 * slot serial, not this immutable reservation or full operation identity. */
static KoDropIdentity *
ko_origin_drop_identity_locked(const ClusterKoSharedContext *context)
{
	KoDropIdentity *identity;

	if (!context->v3 || context->request_v3.operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP
		|| context->drop_identity == 0 || context->drop_identity > KO_DROP_IDENTITY_CAPACITY
		|| context->drop_identity_serial == 0)
		return NULL;
	identity = &ko_state->drop_identities[context->drop_identity - 1];
	if (identity->state < KO_DROP_IDENTITY_RESERVED || identity->state > KO_DROP_IDENTITY_RETIRED
		|| identity->fence_serial != context->drop_identity_serial
		|| identity->committed_discard != context->committed_discard
		|| memcmp(&identity->request, &context->request_v3, sizeof(identity->request)) != 0)
		return NULL;
	return identity;
}

static bool
ko_origin_drop_io_closed(const ClusterKoSharedContext *context)
{
	KoDropIdentity *identity;
	bool closed = false;

	if (!context->v3 || context->request_v3.operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP)
		return true;
	SpinLockAcquire(&ko_state->shared_lock);
	identity = ko_origin_drop_identity_locked(context);
	if (identity != NULL && identity->state >= KO_DROP_IDENTITY_CLOSING
		&& !ko_directory_io_pending_locked(context->request.key.locator)) {
		identity->state = KO_DROP_IDENTITY_RETIRED;
		closed = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return closed;
}

bool
cluster_ko_directory_io_begin_v3(RelFileLocator locator, ClusterKoDirectoryIoV3 *out)
{
	bool entered = false;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!cluster_enabled || !cluster_shared_config || cluster_interconnect_tier != CLUSTER_IC_TIER_1)
		return true;
	if (ko_state == NULL || MyProcPid <= 0 || locator.spcOid == 0 || locator.relNumber == 0
		|| RecoveryInProgress())
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	if (!ko_drop_closed_locked(locator) && ko_state->directory_io_serial < UINT64_MAX) {
		for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
			KoDirectoryIo *call = &ko_state->directory_io[i];
			if (call->serial != 0)
				continue;
			call->locator = locator;
			call->pid = MyProcPid;
			call->serial = ++ko_state->directory_io_serial;
			out->slot = i;
			out->pid = call->pid;
			out->serial = call->serial;
			entered = true;
			break;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
	return entered;
}

bool
cluster_ko_directory_io_end_v3(const ClusterKoDirectoryIoV3 *scope)
{
	bool ended = false, wake = false;

	if (scope == NULL)
		return false;
	if (scope->serial == 0)
		return true;
	if (ko_state == NULL || scope->slot >= CLUSTER_KO_SHARED_CAPACITY || scope->pid != MyProcPid)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	{
		KoDirectoryIo *call = &ko_state->directory_io[scope->slot];
		if (call->serial == scope->serial && call->pid == scope->pid) {
			wake = ko_drop_closed_locked(call->locator);
			memset(call, 0, sizeof(*call));
			ended = true;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
	if (wake)
		cluster_sinval_set_proc_latch();
	return ended;
}


/* No external owner/route lookup under this lock. Both sampling and final
 * consumption select the same unique original operation and entered call. */
static bool
ko_pi_storage_identity_locked(const ClusterSpaceIdentityKey *key,
	const uint8 incarnation[16], const ClusterKoDirectoryIoV3 *call,
	ClusterKoPiStorageIdentityV3 *out)
{
	ClusterKoPiStorageIdentityV3 selected = {0};
	const KoDirectoryIo *entered;
	bool found = false;
	uint64 revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);

	if (call->slot >= CLUSTER_KO_SHARED_CAPACITY || call->serial == 0
		|| call->pid != MyProcPid || revision == 0 || revision == UINT64_MAX
		|| ko_drop_closed_locked(key->locator))
		return false;
	entered = &ko_state->directory_io[call->slot];
	if (entered->serial != call->serial || entered->pid != call->pid
		|| !RelFileLocatorEquals(entered->locator, key->locator))
		return false;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoSharedContext *origin = &ko_state->contexts[i];
		const ClusterKoPeerFenceV3 *peer = &ko_state->peer_fences[i];
		const ClusterKoSharedMessageV3 *request = NULL;
		bool local_origin = false;
		uint64 serial = 0, route = 0;
		ClusterKoPeerFenceStateV3 state = CLUSTER_KO_PEER_FENCE_EMPTY;

		if (origin->used && RelFileLocatorEquals(origin->request.key.locator, key->locator)) {
			KoDropIdentity *identity = ko_origin_drop_identity_locked(origin);
			bool native = DsaPointerIsValid(origin->pi_native_owner);

			if (!origin->v3 || origin->origin_detached || origin->ax_release_requested
				|| origin->ax_failed || origin->cancel_sqlerrcode != 0
				|| origin->request_failed_peers != 0 || origin->structure_owned
				|| (native ? (!origin->complete || origin->drop_identity != 0
							|| origin->drop_identity_serial != 0)
					: (identity == NULL || identity->state != KO_DROP_IDENTITY_RESERVED))
				|| origin->request_v3.base.origin_node != cluster_node_id
				|| memcmp(&origin->request, &origin->request_v3.base, sizeof(origin->request)) != 0)
				return false;
			request = &origin->request_v3;
			serial = origin->serial;
			route = request->ax_master_generation;
			local_origin = true;
			selected.native_owner = native ? (uint64)origin->pi_native_owner : 0;
		}
		if (peer->state != CLUSTER_KO_PEER_FENCE_EMPTY
			&& RelFileLocatorEquals(peer->request.base.key.locator, key->locator)) {
			uint8 encoded[CLUSTER_KO_SHARED_V3_BYTES];

			if (request != NULL
				|| (peer->state != CLUSTER_KO_PEER_FENCE_ACTIVE
					&& peer->state != CLUSTER_KO_PEER_FENCE_READY)
				|| peer->failure_sqlerrcode != 0 || peer->request.base.peer_node != cluster_node_id
				|| !cluster_ko_shared_encode_v3(&peer->request, encoded, sizeof(encoded))
				|| memcmp(encoded, peer->encoded, sizeof(encoded)) != 0)
				return false;
			request = &peer->request;
			serial = peer->serial;
			route = peer->peer_route_generation;
			state = peer->state;
		}
		if (request == NULL)
			continue;
		if (found || serial == 0 || route == 0 || route == UINT64_MAX
			|| request->base.verb != CLUSTER_KO_SHARED_REQUEST
			|| request->operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP
			|| request->base.key.system_identifier != key->system_identifier
			|| request->base.key.database_incarnation != key->database_incarnation
			|| memcmp(request->base.key.storage_uuid, key->storage_uuid, 16) != 0
			|| memcmp(request->base.incarnation, incarnation, 16) != 0)
			return false;
		selected.request = *request;
		selected.call = *call;
		selected.serial = serial;
		selected.route_generation = route;
		selected.admission_revision = revision;
		selected.slot = i;
		selected.state = state;
		selected.origin = local_origin;
		found = true;
	}
	if (!found)
		return false;
	*out = selected;
	return true;
}

static bool
ko_pi_storage_role(void)
{
	return cluster_enabled && cluster_shared_config && ko_state != NULL
		&& cluster_interconnect_tier == CLUSTER_IC_TIER_1 && !RecoveryInProgress()
		&& MyBackendType == B_CHECKPOINTER && MyProcPid > 0
		&& CurrentResourceOwner != NULL && CritSectionCount == 0;
}

static bool
ko_pi_storage_same_locked(const ClusterKoPiStorageIdentityV3 *sample)
{
	ClusterKoPiStorageIdentityV3 now;

	return ko_pi_storage_identity_locked(&sample->request.base.key,
										sample->request.base.incarnation, &sample->call, &now)
		&& now.serial == sample->serial && now.slot == sample->slot
		&& now.route_generation == sample->route_generation
		&& now.admission_revision == sample->admission_revision
		&& now.state == sample->state && now.origin == sample->origin
		&& now.native_owner == sample->native_owner
		&& memcmp(&now.request, &sample->request, sizeof(now.request)) == 0;
}

static bool
ko_pi_storage_native_current_locked(const ClusterKoPiStorageIdentityV3 *sample,
	const ClusterKoNativeContinuation *work)
{
	const ClusterKoSharedContext *origin;
	KoDropIdentity *identity;
	if (sample->native_owner == 0)
		return work == NULL;
	if (!sample->origin || sample->slot >= CLUSTER_KO_SHARED_CAPACITY || work == NULL)
		return false;
	origin = &ko_state->contexts[sample->slot];
	identity = ko_origin_drop_identity_locked(&work->context);
	return !work->recovery_required && !work->buffers_released
		&& work->context.used && work->context.complete && work->context.structure_owned
		&& work->context.structure_drop_pending && !work->context.structure_drop_failed
		&& work->context.pid == origin->pid && work->context.serial == sample->serial
		&& memcmp(&work->context.request_v3, &sample->request, sizeof(sample->request)) == 0
		&& memcmp(work->context.peer_boots, origin->peer_boots, sizeof(origin->peer_boots)) == 0
		&& ko_shared_ready_v3(&work->context)
		&& identity != NULL && identity->state == KO_DROP_IDENTITY_RESERVED;
}

bool
cluster_ko_pi_storage_identity_current_v3(const ClusterKoPiStorageIdentityV3 *sample)
{
	bool valid;
	const ClusterKoNativeContinuation *work = NULL;

	if (sample == NULL || !ko_pi_storage_role())
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	valid = ko_pi_storage_same_locked(sample);
	SpinLockRelease(&ko_state->shared_lock);
	if (!valid)
		return false;
	/* Mapping is outside the spinlock. This checkpointer is the original
	 * promoter; normal free requires all entered calls to drain, and an
	 * abandoned carrier remains in the original failed list. */
	if (sample->native_owner != 0)
		work = dsa_get_address(ko_native_attach(), (dsa_pointer)sample->native_owner);
	SpinLockAcquire(&ko_state->shared_lock);
	valid = ko_pi_storage_same_locked(sample) && ko_pi_storage_native_current_locked(sample, work);
	SpinLockRelease(&ko_state->shared_lock);
	if (!valid || !ko_shared_v3_control_at_generation(&sample->request, sample->route_generation))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	valid = ko_pi_storage_same_locked(sample) && ko_pi_storage_native_current_locked(sample, work);
	SpinLockRelease(&ko_state->shared_lock);
	return valid;
}

bool
cluster_ko_pi_storage_identity_v3(const ClusterSpaceIdentityKey *key,
	const uint8 incarnation[16], const ClusterKoDirectoryIoV3 *call,
	ClusterKoPiStorageIdentityV3 *out)
{
	ClusterKoPiStorageIdentityV3 sample;
	bool valid;

	if (key == NULL || incarnation == NULL || call == NULL || out == NULL
		|| !ko_pi_storage_role())
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	valid = ko_pi_storage_identity_locked(key, incarnation, call, &sample);
	SpinLockRelease(&ko_state->shared_lock);
	if (!valid || !cluster_ko_pi_storage_identity_current_v3(&sample))
		return false;
	*out = sample;
	return true;
}


/* Recheck both at admission and at the original physical consumer. A
 * REQUEST already in the other inbound ring can outlive its COMMIT marker. */
static bool
ko_peer_request_ended_or_conflicts(const ClusterKoSharedMessageV3 *request,
								 bool committed_discard)
{
	uint8 encoded[CLUSTER_KO_SHARED_V3_BYTES];
	bool refused = false;

	if (!ko_shared_object_encode(request, committed_discard, encoded, sizeof(encoded)))
		return true;
	SpinLockAcquire(&ko_state->shared_lock);
	refused = ko_drop_closed_locked(request->base.key.locator);
	for (unsigned i = 0; !refused && i < CLUSTER_KO_SHARED_CAPACITY; i++) {
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

static bool
ko_peer_request_ended_or_conflicts_v3(const ClusterKoSharedMessageV3 *request)
{
	return ko_peer_request_ended_or_conflicts(request, false);
}

/* The LMON handler publishes one exact physical owner before the SPSC slot is
 * visible.  A retry which arrives after the slot is consumed must not create a
 * second flush/drop attempt; the original SI owner is responsible for either
 * the READY ACK or the existing fail-closed timeout. */
static bool
ko_peer_fence_physical_owned(const ClusterKoSharedMessageV3 *request, bool committed_discard)
{
	uint8 encoded[CLUSTER_KO_SHARED_V3_BYTES];
	bool owned = false;

	if (ko_state == NULL || request == NULL
		|| !ko_shared_object_encode(request, committed_discard, encoded, sizeof(encoded)))
		return true;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];

		if (!ko_peer_fence_key_matches(entry, request->base.origin_node, request->base.origin_boot,
									   request->base.batch_id))
			continue;
		if (memcmp(entry->encoded, encoded, sizeof(encoded)) != 0) {
			owned = true;
			break;
		}
		owned = entry->physical_started || entry->physical_applied || entry->ack_queued
				|| entry->state != CLUSTER_KO_PEER_FENCE_ACTIVE;
		break;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return owned;
}

static bool
ko_peer_fence_mark_physical_started(const ClusterKoSharedMessageV3 *request,
									   ClusterKoPeerFenceHandleV3 *handle, bool committed_discard)
{
	uint8 encoded[CLUSTER_KO_SHARED_V3_BYTES];
	bool marked = false;

	if (ko_state == NULL || request == NULL
		|| !ko_shared_object_encode(request, committed_discard, encoded, sizeof(encoded)))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];

		if (entry->state == CLUSTER_KO_PEER_FENCE_ACTIVE && entry->failure_sqlerrcode == 0
			&& entry->request.base.origin_node == request->base.origin_node
			&& entry->request.base.origin_boot == request->base.origin_boot
			&& entry->request.base.batch_id == request->base.batch_id
			&& memcmp(entry->encoded, encoded, sizeof(encoded)) == 0) {
			entry->physical_started = true;
			if (handle != NULL) {
				handle->slot = i;
				handle->serial = entry->serial;
			}
			marked = true;
			break;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
	return marked;
}

static bool
ko_peer_fence_mark_physical_started_v3(const ClusterKoSharedMessageV3 *request,
									 ClusterKoPeerFenceHandleV3 *handle)
{
	return ko_peer_fence_mark_physical_started(request, handle, false);
}

static bool
ko_peer_fence_mark_physical_applied_v3(const ClusterKoSharedMessageV3 *request)
{
	uint8 encoded[CLUSTER_KO_SHARED_V3_BYTES];
	bool marked = false;
	ResourceXGateSnapshot gate;
	uint64 revision;

	if (ko_state == NULL || request == NULL
		|| !cluster_ko_shared_encode_v3(request, encoded, sizeof(encoded)))
		return false;
	revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	if (revision == 0 || revision == UINT64_MAX || !cluster_pcm_lock_resource_x_gate_snapshot(&gate)
		|| gate.phase != RESOURCE_X_GATE_OPEN)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];

		if (entry->state == CLUSTER_KO_PEER_FENCE_ACTIVE && entry->physical_started
			&& entry->failure_sqlerrcode == 0
			&& entry->request.base.origin_node == request->base.origin_node
			&& entry->request.base.origin_boot == request->base.origin_boot
			&& entry->request.base.batch_id == request->base.batch_id
			&& memcmp(entry->encoded, encoded, sizeof(encoded)) == 0) {
			entry->physical_applied = true;
			entry->physical_phase = 3;
			entry->physical_cursor = 0;
			entry->physical_revision = revision;
			entry->physical_gate = gate;
			entry->local_scan_cursor = 0;
			entry->local_scan_complete = false;
			entry->local_scan_revision = 0;
			entry->local_writeback_revision = 0;
			memset(&entry->local_scan_gate, 0, sizeof(entry->local_scan_gate));
			marked = true;
			break;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
	return marked;
}

void
cluster_ko_peer_apply_error_v3(int sqlerrcode)
{
	ClusterKoSharedMessageV3 request;
	bool recorded = false;

	if (ko_state == NULL || !AmSinvalBcastProcess() || !ko_peer_attempt_active
		|| ko_peer_attempt.slot >= CLUSTER_KO_SHARED_CAPACITY || sqlerrcode == 0)
		return;
	SpinLockAcquire(&ko_state->shared_lock);
	{
		ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[ko_peer_attempt.slot];
		if (entry->serial == ko_peer_attempt.serial && entry->state == CLUSTER_KO_PEER_FENCE_ACTIVE
			&& entry->physical_started && entry->failure_sqlerrcode == 0) {
			entry->failure_sqlerrcode = sqlerrcode;
			entry->local_scan_complete = false;
			request = entry->request;
			recorded = true;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
	ko_peer_attempt_active = false;
	if (recorded)
		ereport(LOG,
				(errcode(sqlerrcode),
				 errmsg("KO v3 peer operation failed; original work is retained"),
				 errdetail("origin=%u peer=%u epoch=" UINT64_FORMAT " batch=" UINT64_FORMAT
						   " locator=%u/%u/%u operation=%u sqlerrcode=%d",
						   request.base.origin_node, request.base.peer_node, request.base.epoch,
						   request.base.batch_id, request.base.key.locator.spcOid,
						   request.base.key.locator.dbOid, request.base.key.locator.relNumber,
						   request.operation, sqlerrcode)));
}

/* The origin participates locally, without manufacturing a self-peer frame.
 * Its accepted operation keeps this exclusion through cancel/AX handoff. */
static bool
ko_origin_fenced_locked(const ClusterKoSharedContext *entry, RelFileLocator locator)
{
	return entry->used && entry->v3 && entry->origin_checkpoint
		&& RelFileLocatorEquals(entry->request.key.locator, locator);
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
	if (revision == 0 || revision == UINT64_MAX || ko_drop_closed_locked(locator))
		allowed = false;
	for (unsigned i = 0; allowed && i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];
		/* READY/COMMITTED do not reopen admission. Only a separately proven
		 * lifecycle retirement may remove this old-locator exclusion. */
		if ((entry->state != CLUSTER_KO_PEER_FENCE_EMPTY
			 && RelFileLocatorEquals(entry->request.base.key.locator, locator))
			|| ko_origin_fenced_locked(&ko_state->contexts[i], locator))
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
		   && admission->extension_serial == 0 && admission->origin_serial == 0
		   && admission->revision != 0
		   && admission->revision != UINT64_MAX && RelFileLocatorEquals(admission->locator, locator)
		   && pg_atomic_read_u64(&ko_state->peer_admission_revision) == admission->revision;
}

bool
cluster_ko_peer_delivery_begin_v3(RelFileLocator locator, uint64 *revision)
{
	bool allowed = true;
	uint64 current;

	if (revision != NULL)
		*revision = 0;
	if (ko_state == NULL || revision == NULL || !cluster_shared_config
		|| cluster_interconnect_tier != CLUSTER_IC_TIER_1 || RecoveryInProgress()
		|| locator.spcOid == 0 || locator.relNumber == 0)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	current = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	if (current == 0 || current == UINT64_MAX || ko_drop_closed_locked(locator))
		allowed = false;
	for (unsigned i = 0; allowed && i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];

		if (entry->state != CLUSTER_KO_PEER_FENCE_EMPTY
			&& RelFileLocatorEquals(entry->request.base.key.locator, locator)
			&& entry->state != CLUSTER_KO_PEER_FENCE_ACTIVE)
			allowed = false;
	}
	if (allowed)
		*revision = current;
	SpinLockRelease(&ko_state->shared_lock);
	return allowed;
}

bool
cluster_ko_peer_delivery_current_v3(uint64 revision)
{
	/* Called under the original entry lock, before a retained target gains
	 * an executor. This sample cannot create a caller, round or target. */
	return ko_state != NULL && cluster_shared_config
		   && cluster_interconnect_tier == CLUSTER_IC_TIER_1 && !RecoveryInProgress()
		   && revision != 0 && revision != UINT64_MAX
		   && pg_atomic_read_u64(&ko_state->peer_admission_revision) == revision;
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
	if (ko_drop_closed_locked(locator))
		return false;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];
		const ClusterKoSharedContext *origin = &ko_state->contexts[i];
		if (ko_origin_fenced_locked(origin, locator)) {
			if (fenced != NULL)
				*fenced = true;
			if (origin->origin_data_closed || origin->origin_detached || origin->ax_failed
				|| origin->request.key.system_identifier != scope->system_identifier
				|| origin->request.epoch != scope->formation_epoch
				|| origin->request.origin_boot != scope->boot_incarnation)
				return false;
		}
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

static bool
ko_extend_local_exact(RelFileLocator locator, ForkNumber fork)
{
	return ko_extend_local.used && !ko_extend_aborting && ko_extend_local.serial != 0
		   && ko_extend_local.pid == MyProcPid && ko_extend_local.fork == fork
		   && RelFileLocatorEquals(ko_extend_local.locator, locator);
}

static bool
ko_copy_local_exact(RelFileLocator locator)
{
	return ko_create_local.used && ko_create_local.raw_copy && !ko_create_aborting
		   && ko_create_write_armed && ko_create_local.serial != 0
		   && ko_create_local.pid == MyProcPid && ko_create_owner == CurrentResourceOwner
		   && RelFileLocatorEquals(ko_create_local.locator, locator);
}

static bool
ko_buffer_copy_target_exact(RelFileLocator locator, ForkNumber fork, BlockNumber block)
{
	return ko_copy_local_exact(locator) && ko_create_local.buffer_copy
		   && ko_buffer_copy.serial == ko_create_local.serial && ko_buffer_copy.fork == fork
		   && block < ko_buffer_copy.nblocks && ko_buffer_copy.target == block;
}

static bool
ko_extend_fences_allow_locked(const ClusterKoExtendWorkV3 *work)
{
	if (ko_drop_closed_locked(work->locator))
		return false;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];
		const ClusterKoSharedMessageV2 *request = &entry->request.base;
		const ClusterKoSharedContext *origin = &ko_state->contexts[i];
		if (ko_origin_fenced_locked(origin, work->locator)) {
			const ClusterKoSharedMessageV2 *r = &origin->request;
			if (origin->origin_data_closed || origin->origin_detached || origin->ax_failed
				|| r->epoch != work->epoch
				|| r->origin_boot != work->writer.claim.identity.origin_owner_incarnation
				|| r->key.system_identifier != work->writer.claim.identity.system_identifier
				|| r->key.database_incarnation != work->writer.claim.database_incarnation
				|| memcmp(r->key.storage_uuid, work->writer.claim.identity.storage_uuid, 16) != 0)
				return false;
		}
		if (entry->state == CLUSTER_KO_PEER_FENCE_EMPTY
			|| !RelFileLocatorEquals(request->key.locator, work->locator))
			continue;
		if (entry->state != CLUSTER_KO_PEER_FENCE_ACTIVE || request->epoch != work->epoch
			|| request->peer_boot != work->writer.claim.identity.origin_owner_incarnation
			|| request->key.system_identifier != work->writer.claim.identity.system_identifier
			|| request->key.database_incarnation != work->writer.claim.database_incarnation
			|| memcmp(request->key.storage_uuid, work->writer.claim.identity.storage_uuid, 16) != 0)
			return false;
	}
	return true;
}

static bool
ko_extend_pending_locked(RelFileLocator locator)
{
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (ko_state->extend_work[i].used
			&& RelFileLocatorEquals(ko_state->extend_work[i].locator, locator))
			return true;
	return false;
}

/* The scope identifies the two SPACE targets of its original writer, or the
 * one selected destination of a buffered copy. A read sample cannot publish a
 * page or grant current; the current consumer retains the original PCM proof. */
static bool
ko_storage_read_begin(RelFileLocator locator, ForkNumber fork, BlockNumber block,
					  ClusterKoPeerAdmissionV3 *out)
{
	const ClusterKoExtendWorkV3 *work;
	uint32 slot;
	ClusterWalSourceRef writer;
	bool allowed = false;
	uint64 revision;
	bool space = fork == SPACE_FORKNUM && block <= 1;

	if (space && ko_extend_local_exact(locator, MAIN_FORKNUM)) {
		work = &ko_extend_local;
		slot = ko_extend_slot;
	} else if (ko_copy_local_exact(locator)
			   && (space || ko_buffer_copy_target_exact(locator, fork, block))) {
		if (!cluster_enabled || !cluster_shared_config || RecoveryInProgress()
			|| cluster_interconnect_tier != CLUSTER_IC_TIER_1)
			return false;
		work = &ko_create_local;
		slot = ko_create_slot;
	} else
		return false;
	if (slot >= CLUSTER_KO_SHARED_CAPACITY
		|| !cluster_wal_thread_current_v2_ref(&writer)
		|| memcmp(&writer, &work->writer, sizeof(writer)) != 0
		|| cluster_epoch_get_current() != work->epoch)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	if (revision != 0 && revision != UINT64_MAX
		&& memcmp(&ko_state->extend_work[slot], work, sizeof(*work)) == 0
		&& ko_extend_fences_allow_locked(work)) {
		memset(out, 0, sizeof(*out));
		out->locator = locator;
		out->fork = fork;
		out->block = block;
		out->revision = revision;
		out->extension_serial = work->serial;
		allowed = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return allowed;
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
		|| cluster_interconnect_tier != CLUSTER_IC_TIER_1)
		return false;
	if (ko_origin_space_read_begin(locator, fork, block, out)
		|| ko_storage_read_begin(locator, fork, block, out))
		return true;
	if (!cluster_ctrc_cleaner_read_scope_snapshot_v3(locator, fork, block, &scope)
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
	if (admission != NULL && admission->origin_serial != 0)
		return ko_origin_space_read_current(admission, locator, fork, block);
	if (admission != NULL && admission->extension_serial != 0)
		return ko_state != NULL && admission->cleaner.serial == 0
			   && ((admission->extension_serial == ko_extend_local.serial
					&& ko_extend_local_exact(locator, MAIN_FORKNUM) && fork == SPACE_FORKNUM
					&& block <= 1)
				   || (admission->extension_serial == ko_create_local.serial
					   && ko_copy_local_exact(locator)
					   && ((fork == SPACE_FORKNUM && block <= 1)
						   || ko_buffer_copy_target_exact(locator, fork, block))))
			   && admission->fork == fork && admission->block == block
			   && RelFileLocatorEquals(admission->locator, locator) && admission->revision != 0
			   && admission->revision != UINT64_MAX
			   && pg_atomic_read_u64(&ko_state->peer_admission_revision) == admission->revision;
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

/* The finite cleaner/extension owner already protects the exact target
 * through its original cleanup. Reuse that owner qualification, not a role
 * exemption. Acquiring current still requires every original PCM/GCS proof;
 * these samples confer only permission to enter that protocol. */
bool
cluster_ko_peer_current_begin_v3(RelFileLocator locator, ForkNumber fork, BlockNumber block,
								 ClusterKoCurrentAdmissionV3 *out)
{
	ClusterKoPeerAdmissionV3 sample;
	if (out == NULL || fork < MAIN_FORKNUM || fork > MAX_FORKNUM || block == InvalidBlockNumber
		|| !cluster_ko_peer_read_begin_v3(locator, fork, block, &sample))
		return false;
	sample.fork = fork;
	sample.block = block;
	out->target = sample;
	return true;
}

bool
cluster_ko_peer_current_valid_v3(const ClusterKoCurrentAdmissionV3 *admission,
								 RelFileLocator locator, ForkNumber fork, BlockNumber block)
{
	return admission != NULL && fork >= MAIN_FORKNUM && fork <= MAX_FORKNUM
		   && block != InvalidBlockNumber && admission->target.fork == fork
		   && admission->target.block == block
		   && cluster_ko_peer_read_current_v3(&admission->target, locator, fork, block);
}

bool
cluster_ko_extend_active_v3(void)
{
	return ko_extend_local.used;
}

static void
ko_storage_register_callbacks(void)
{
	if (!ko_resource_registered) {
		RegisterResourceReleaseCallback(ko_shared_resource_release, NULL);
		RegisterXactCallback(ko_shared_xact_event, NULL);
		ko_resource_registered = true;
	}
	if (!ko_exit_registered) {
		before_shmem_exit(ko_shared_backend_exit, (Datum)0);
		ko_exit_registered = true;
	}
}

static bool
ko_create_begin(RelFileLocator locator, bool raw_copy, bool buffer_copy, uint64 *serial)
{
	ClusterKoExtendWorkV3 work = { 0 };
	int empty = -1;
	unsigned available = 0, constructors = 0;
	uint64 revision;
	bool allowed = false;

	if (serial != NULL)
		*serial = 0;
	if (serial == NULL || ko_state == NULL || ko_create_local.used || ko_extend_local.used
		|| MyProcPid <= 0 || CurrentResourceOwner == NULL || !cluster_enabled
		|| !cluster_shared_config || cluster_interconnect_tier != CLUSTER_IC_TIER_1
		|| RecoveryInProgress() || locator.spcOid == 0 || locator.relNumber == 0
		|| !cluster_wal_thread_current_v2_ref(&work.writer)
		|| work.writer.claim.identity.origin_node_id != cluster_node_id
		|| work.writer.claim.identity.origin_owner_incarnation == 0
		|| work.writer.claim.identity.system_identifier == 0
		|| (work.epoch = cluster_epoch_get_current()) == 0)
		return false;
	ko_storage_register_callbacks();
	work.used = true;
	work.pid = MyProcPid;
	work.locator = locator;
	work.fork = InvalidForkNumber;
	work.raw_copy = raw_copy;
	work.buffer_copy = buffer_copy;
	SpinLockAcquire(&ko_state->shared_lock);
	revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	if (revision == 0 || revision == UINT64_MAX || ko_state->extend_serial == UINT64_MAX)
		goto done;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		if ((ko_state->peer_fences[i].state != CLUSTER_KO_PEER_FENCE_EMPTY
			 && RelFileLocatorEquals(ko_state->peer_fences[i].request.base.key.locator, locator))
			|| ko_origin_fenced_locked(&ko_state->contexts[i], locator))
			goto done;
		if (!ko_state->extend_work[i].used) {
			available++;
			if (empty < 0)
				empty = i;
		} else if (!ko_state->extend_work[i].failed
				   && ko_state->extend_work[i].fork == InvalidForkNumber)
			constructors++;
	}
	/* Every live constructor is promised one child slot. Its two SPACE
	 * extensions run serially. Do not fill the table with parents which
	 * can only release their own slots after acquiring a child slot. */
	if (empty >= 0 && available >= constructors + 2) {
		work.serial = ++ko_state->extend_serial;
		ko_state->extend_work[empty] = work;
		ko_create_local = work;
		ko_create_slot = empty;
		ko_create_write_armed = ko_create_aborting = false;
		ko_create_owner = CurrentResourceOwner;
		memset(&ko_buffer_copy, 0, sizeof(ko_buffer_copy));
		*serial = work.serial;
		allowed = true;
	}
done:
	SpinLockRelease(&ko_state->shared_lock);
	return allowed;
}

bool
cluster_ko_create_begin_v3(RelFileLocator locator, uint64 *serial)
{
	return ko_create_begin(locator, false, false, serial);
}

bool
cluster_ko_copy_begin_v3(RelFileLocator locator, uint64 *serial)
{
	return ko_create_begin(locator, true, false, serial);
}

bool
cluster_ko_build_begin_v3(RelFileLocator locator, uint64 *serial)
{
	return ko_create_begin(locator, true, false, serial);
}

bool
cluster_ko_buffer_copy_begin_v3(RelFileLocator locator, ForkNumber fork, BlockNumber nblocks,
								uint64 *serial)
{
	if (serial != NULL)
		*serial = 0;
	if ((fork != MAIN_FORKNUM && fork != FSM_FORKNUM && fork != VISIBILITYMAP_FORKNUM)
		|| nblocks == 0 || nblocks == InvalidBlockNumber
		|| !ko_create_begin(locator, true, true, serial))
		return false;
	ko_buffer_copy.serial = *serial;
	ko_buffer_copy.fork = fork;
	ko_buffer_copy.nblocks = nblocks;
	ko_buffer_copy.target = InvalidBlockNumber;
	return true;
}

bool
cluster_ko_buffer_copy_target_v3(RelFileLocator locator, uint64 serial, ForkNumber fork,
								 BlockNumber block, ClusterKoCurrentAdmissionV3 *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	/* Expire the prior target even when selecting its successor fails.
	 * Never revisit a selected block under the same serial (sample ABA). */
	if (serial != 0 && serial == ko_buffer_copy.serial)
		ko_buffer_copy.target = InvalidBlockNumber;
	if (out == NULL || serial == 0 || serial != ko_buffer_copy.serial
		|| serial != ko_create_local.serial || !ko_create_local.buffer_copy
		|| !ko_copy_local_exact(locator) || fork != ko_buffer_copy.fork
		|| block != ko_buffer_copy.next || block >= ko_buffer_copy.nblocks)
		return false;
	ko_buffer_copy.target = block;
	/* Force an owner-bound sample even before a fence has arrived. */
	if (!ko_storage_read_begin(locator, fork, block, &out->target)) {
		ko_buffer_copy.target = InvalidBlockNumber;
		return false;
	}
	ko_buffer_copy.next++;
	return true;
}

bool
cluster_ko_create_current_v3(RelFileLocator locator, uint64 *serial)
{
	ClusterWalSourceRef writer;
	uint64 revision;
	bool allowed;

	if (serial != NULL)
		*serial = 0;
	if (serial == NULL || ko_state == NULL || !ko_create_local.used || ko_create_aborting
		|| ko_create_local.pid != MyProcPid || CurrentResourceOwner != ko_create_owner
		|| !cluster_enabled || !cluster_shared_config || RecoveryInProgress()
		|| cluster_interconnect_tier != CLUSTER_IC_TIER_1
		|| ko_create_slot >= CLUSTER_KO_SHARED_CAPACITY
		|| !RelFileLocatorEquals(ko_create_local.locator, locator)
		|| cluster_epoch_get_current() != ko_create_local.epoch
		|| !cluster_wal_thread_current_v2_ref(&writer)
		|| memcmp(&writer, &ko_create_local.writer, sizeof(writer)) != 0)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	allowed = revision != 0 && revision != UINT64_MAX
			  && memcmp(&ko_state->extend_work[ko_create_slot], &ko_create_local,
						sizeof(ko_create_local)) == 0
			  && ko_extend_fences_allow_locked(&ko_create_local);
	if (allowed)
		*serial = ko_create_local.serial;
	SpinLockRelease(&ko_state->shared_lock);
	return allowed;
}

bool
cluster_ko_create_write_begin_v3(RelFileLocator locator, uint64 serial)
{
	uint64 current;
	if (serial == 0 || !cluster_ko_create_current_v3(locator, &current) || serial != current)
		return false;
	ko_create_write_armed = true;
	return true;
}

void
cluster_ko_create_abort_v3(uint64 serial)
{
	/* ERROR may still own buffer locks. Only expire local permission now;
	 * the original AFTER_LOCKS callback retains unknown physical work. */
	if (serial != 0 && ko_create_local.used && ko_create_local.serial == serial
		&& ko_create_local.pid == MyProcPid)
		ko_create_aborting = true;
}

bool
cluster_ko_create_end_v3(uint64 serial, bool complete)
{
	bool retired = false;
	uint64 current;

	if (ko_state == NULL || serial == 0 || !ko_create_local.used
		|| serial != ko_create_local.serial || ko_create_local.pid != MyProcPid
		|| ko_create_owner != CurrentResourceOwner || ko_create_slot >= CLUSTER_KO_SHARED_CAPACITY
		|| ko_extend_local.used)
		return false;
	if (complete && !cluster_ko_create_current_v3(ko_create_local.locator, &current))
		complete = false;
	SpinLockAcquire(&ko_state->shared_lock);
	if (memcmp(&ko_state->extend_work[ko_create_slot], &ko_create_local,
			   sizeof(ko_create_local)) == 0) {
		if (ko_create_write_armed && !complete)
			ko_state->extend_work[ko_create_slot].failed = true;
		else {
			memset(&ko_state->extend_work[ko_create_slot], 0, sizeof(ko_create_local));
			retired = true;
		}
		for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
			if ((ko_state->peer_fences[i].state != CLUSTER_KO_PEER_FENCE_EMPTY
				&& RelFileLocatorEquals(ko_state->peer_fences[i].request.base.key.locator,
										ko_create_local.locator))
				|| ko_origin_fenced_locked(&ko_state->contexts[i], ko_create_local.locator)) {
				uint64 revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
				pg_atomic_write_u64(&ko_state->peer_admission_revision,
									revision == 0 || revision == UINT64_MAX ? UINT64_MAX : revision + 1);
				break;
			}
		}
	}
	memset(&ko_create_local, 0, sizeof(ko_create_local));
	memset(&ko_buffer_copy, 0, sizeof(ko_buffer_copy));
	ko_create_owner = NULL;
	ko_create_aborting = false;
	SpinLockRelease(&ko_state->shared_lock);
	cluster_sinval_set_proc_latch();
	return retired;
}

bool
cluster_ko_extend_begin_v3(RelFileLocator locator, ForkNumber fork, uint64 *serial, bool *retry)
{
	ClusterKoExtendWorkV3 work;
	int empty = -1;
	unsigned available = 0, constructors = 0;
	bool allowed = false;
	uint64 revision;
	uint64 parent = 0;

	if (serial != NULL)
		*serial = 0;
	if (retry != NULL)
		*retry = false;
	if (serial == NULL || ko_state == NULL || ko_extend_local.used || MyProcPid <= 0
		|| CurrentResourceOwner == NULL || !cluster_enabled || !cluster_shared_config
		|| RecoveryInProgress() || cluster_interconnect_tier != CLUSTER_IC_TIER_1
		|| fork < MAIN_FORKNUM || fork > MAX_FORKNUM || locator.spcOid == 0
		|| locator.relNumber == 0)
		return false;
	if (ko_create_local.used
		&& (fork != SPACE_FORKNUM || !cluster_ko_create_current_v3(locator, &parent)))
		return false;
	memset(&work, 0, sizeof(work));
	if (!cluster_wal_thread_current_v2_ref(&work.writer)
		|| work.writer.claim.identity.origin_node_id != cluster_node_id
		|| work.writer.claim.identity.origin_owner_incarnation == 0
		|| work.writer.claim.identity.system_identifier == 0
		|| (work.epoch = cluster_epoch_get_current()) == 0)
		return false;
	if (parent != 0 && (work.epoch != ko_create_local.epoch
		|| memcmp(&work.writer, &ko_create_local.writer, sizeof(work.writer)) != 0))
		return false;
	/* Register the existing cleanup owner before publishing a reference.
	 * The callbacks can allocate; none runs under the object lock. */
	ko_storage_register_callbacks();
	work.used = true;
	work.pid = MyProcPid;
	work.locator = locator;
	work.fork = fork;
	SpinLockAcquire(&ko_state->shared_lock);
	revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	if (revision == 0 || revision == UINT64_MAX || ko_state->extend_serial == UINT64_MAX)
		goto done;
	if (parent != 0
		&& (memcmp(&ko_state->extend_work[ko_create_slot], &ko_create_local,
				   sizeof(ko_create_local)) != 0 || !ko_extend_fences_allow_locked(&ko_create_local)))
		goto done;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		if (parent == 0
			&& ((ko_state->peer_fences[i].state != CLUSTER_KO_PEER_FENCE_EMPTY
				 && RelFileLocatorEquals(ko_state->peer_fences[i].request.base.key.locator, locator))
				|| ko_origin_fenced_locked(&ko_state->contexts[i], locator))) {
			if (retry != NULL)
				*retry = true;
			goto done;
		}
		if (!ko_state->extend_work[i].used) {
			available++;
			if (empty < 0)
				empty = i;
		} else if (!ko_state->extend_work[i].failed
				   && ko_state->extend_work[i].fork == InvalidForkNumber)
			constructors++;
	}
	/* Ordinary extension cannot take capacity promised to a constructor.
	 * Exact child work consumes that promise and releases the slot through
	 * its unchanged materialization/ResourceOwner path. */
	if (empty >= 0 && (parent != 0 || available > constructors)) {
		work.serial = ++ko_state->extend_serial;
		ko_state->extend_work[empty] = work;
		ko_extend_local = work;
		ko_extend_slot = empty;
		ko_extend_write_armed = ko_extend_materialized = ko_extend_aborting = false;
		ko_extend_owner = CurrentResourceOwner;
		*serial = work.serial;
		allowed = true;
	} else if (retry != NULL)
		*retry = true;
done:
	SpinLockRelease(&ko_state->shared_lock);
	return allowed;
}

bool
cluster_ko_extend_write_begin_v3(RelFileLocator locator, ForkNumber fork)
{
	if (!ko_extend_local_exact(locator, fork) || ko_extend_materialized)
		return false;
	ko_extend_write_armed = true;
	return true;
}

bool
cluster_ko_extend_write_done_v3(RelFileLocator locator, ForkNumber fork)
{
	if (!ko_extend_local_exact(locator, fork) || !ko_extend_write_armed)
		return false;
	ko_extend_materialized = true;
	return true;
}

void
cluster_ko_extend_abort_v3(uint64 serial)
{
	/* ERROR can still hold target content/LW locks. Expire only the local
	 * read permission here; the original ResourceOwner's AFTER_LOCKS phase
	 * performs the shared reference transition. No lock, allocation or IO. */
	if (serial != 0 && ko_extend_local.used && ko_extend_local.serial == serial
		&& ko_extend_local.pid == MyProcPid)
		ko_extend_aborting = true;
}

bool
cluster_ko_extend_end_v3(uint64 serial)
{
	bool retired = false;

	if (ko_state == NULL || serial == 0 || serial != ko_extend_local.serial || !ko_extend_local.used
		|| ko_extend_local.pid != MyProcPid || CurrentResourceOwner != ko_extend_owner
		|| ko_extend_slot >= CLUSTER_KO_SHARED_CAPACITY)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	if (memcmp(&ko_state->extend_work[ko_extend_slot], &ko_extend_local, sizeof(ko_extend_local))
		== 0) {
		if (ko_extend_write_armed && !ko_extend_materialized)
			ko_state->extend_work[ko_extend_slot].failed = true;
		else {
			memset(&ko_state->extend_work[ko_extend_slot], 0, sizeof(ko_extend_local));
			retired = true;
		}
		/* Any scan which overlapped this finite owner must retry. */
		for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
			if ((ko_state->peer_fences[i].state != CLUSTER_KO_PEER_FENCE_EMPTY
				&& RelFileLocatorEquals(ko_state->peer_fences[i].request.base.key.locator,
										ko_extend_local.locator))
				|| ko_origin_fenced_locked(&ko_state->contexts[i], ko_extend_local.locator)) {
				uint64 revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
				pg_atomic_write_u64(&ko_state->peer_admission_revision,
									revision == 0 || revision == UINT64_MAX ? UINT64_MAX
																			: revision + 1);
				break;
			}
		}
	}
	memset(&ko_extend_local, 0, sizeof(ko_extend_local));
	ko_extend_aborting = false;
	ko_extend_owner = NULL;
	SpinLockRelease(&ko_state->shared_lock);
	cluster_sinval_set_proc_latch();
	return retired;
}

static bool
ko_peer_fence_begin(const ClusterKoSharedMessageV3 *request,
							 ClusterKoPeerFenceHandleV3 *handle,
							 ClusterKoPeerFenceStateV3 *state, bool committed_discard)
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
		|| !ko_shared_object_control_at_generation(request, peer_route_generation, committed_discard)
		|| !ko_shared_object_encode(request, committed_discard, encoded, sizeof(encoded)))
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
	if (ko_drop_closed_locked(request->base.key.locator))
		goto done;
	if (empty >= 0 && ko_state->peer_fence_serial != UINT64_MAX
		&& pg_atomic_read_u64(&ko_state->peer_admission_revision) != 0
		&& pg_atomic_read_u64(&ko_state->peer_admission_revision) < UINT64_MAX - 1) {
		ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[empty];

		memset(entry, 0, sizeof(*entry));
		entry->request = *request;
		entry->committed_discard = committed_discard;
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

bool
cluster_ko_peer_fence_begin_v3(const ClusterKoSharedMessageV3 *request,
							 ClusterKoPeerFenceHandleV3 *handle,
							 ClusterKoPeerFenceStateV3 *state)
{
	return ko_peer_fence_begin(request, handle, state, false);
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
	ClusterGesHolderProbe probe, previous;
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
	previous = entry->holder_probe;
	peer_route_generation = entry->peer_route_generation;
	SpinLockRelease(&ko_state->shared_lock);
	/* Only a completed observation may be replaced. An unanswered query
	 * remains owned by the original LMON retry, including after COMMIT. */
	if (previous.nonce != 0 && !previous.reply)
		return false;
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
		&& entry->peer_route_generation == peer_route_generation
		&& memcmp(&entry->request, &request, sizeof(request)) == 0
		&& memcmp(&entry->holder_probe, &previous, sizeof(previous)) == 0
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
static bool
ko_peer_fence_ready(const ClusterKoSharedMessageV3 *request,
					const ClusterKoPeerFenceHandleV3 *handle, bool committed_discard)
{
	uint8 encoded[CLUSTER_KO_SHARED_V3_BYTES];
	ClusterKoPeerFenceV3 *entry;
	ResourceXGateSnapshot gate;
	uint64 peer_route_generation;
	bool ok = false;

	if (ko_state == NULL || request == NULL || handle == NULL || !AmSinvalBcastProcess()
		|| handle->slot >= CLUSTER_KO_SHARED_CAPACITY || handle->serial == 0
		|| !ko_shared_object_encode(request, committed_discard, encoded, sizeof(encoded)))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	peer_route_generation = ko_state->peer_fences[handle->slot].peer_route_generation;
	SpinLockRelease(&ko_state->shared_lock);
	if (!ko_shared_object_control_at_generation(request, peer_route_generation, committed_discard))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	entry = &ko_state->peer_fences[handle->slot];
	if (entry->committed_discard == committed_discard && entry->serial == handle->serial && entry->peer_route_generation == peer_route_generation
		&& (entry->state == CLUSTER_KO_PEER_FENCE_ACTIVE
			|| entry->state == CLUSTER_KO_PEER_FENCE_READY)
		&& memcmp(entry->encoded, encoded, sizeof(encoded)) == 0 && entry->physical_applied
		&& entry->local_scan_complete && entry->failure_sqlerrcode == 0
		&& cluster_pi_writeback_local_revision_current_v3(entry->local_writeback_revision)
		&& entry->physical_revision == entry->local_scan_revision
		&& memcmp(&entry->physical_gate, &entry->local_scan_gate, sizeof(entry->physical_gate)) == 0
		&& !ko_cleaner_pending_locked(request->base.key.locator)
		&& !ko_extend_pending_locked(request->base.key.locator) && entry->local_scan_revision != 0
		&& entry->local_scan_revision != UINT64_MAX
		&& (entry->state == CLUSTER_KO_PEER_FENCE_READY
			|| entry->local_scan_revision < UINT64_MAX - 1)
		&& pg_atomic_read_u64(&ko_state->peer_admission_revision) == entry->local_scan_revision
		/* This is the original bounded atomic gate snapshot, not a PCM/CF
		 * lock acquisition. No directory lock is taken under the KO lock. */
		&& cluster_pcm_lock_resource_x_gate_snapshot(&gate)
		&& memcmp(&gate, &entry->local_scan_gate, sizeof(gate)) == 0
		&& gate.phase == RESOURCE_X_GATE_OPEN
		&& (committed_discard
			? (ko_peer_discard_identity_locked(entry) != NULL
			   && ko_peer_discard_identity_locked(entry)->state == KO_DROP_IDENTITY_RETIRED)
			: ko_drop_reserve_locked(&entry->request, entry->serial, &entry->drop_identity))) {
		if (entry->state == CLUSTER_KO_PEER_FENCE_ACTIVE) {
			uint64 sealed_revision = entry->local_scan_revision + 1;

			/* An ACTIVE delivery sample cannot outlive the READY seal.  The
			 * only change under this lock is sealing this already-proven cut;
			 * carry this fence's proof forward and invalidate all old samples.
			 * Other in-progress scans reobserve through their original owner. */
			pg_atomic_write_u64(&ko_state->peer_admission_revision, sealed_revision);
			entry->physical_revision = entry->local_scan_revision = sealed_revision;
		}
		entry->state = CLUSTER_KO_PEER_FENCE_READY;
		ok = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return ok;
}

bool
cluster_ko_peer_fence_ready_v3(const ClusterKoSharedMessageV3 *request,
							 const ClusterKoPeerFenceHandleV3 *handle)
{
	return ko_peer_fence_ready(request, handle, false);
}

/* Invalid/error attempts retire only the local scan hint, never the object
 * work or its original physical/PI obligations. */
static void
ko_peer_fence_scan_reset_v3(const ClusterKoPeerFenceHandleV3 *handle)
{
	ClusterKoPeerFenceV3 *entry;

	SpinLockAcquire(&ko_state->shared_lock);
	entry = &ko_state->peer_fences[handle->slot];
	if (entry->serial == handle->serial && entry->state == CLUSTER_KO_PEER_FENCE_ACTIVE) {
		entry->local_scan_cursor = 0;
		entry->local_scan_complete = false;
		entry->local_scan_revision = 0;
		entry->local_writeback_revision = 0;
		memset(&entry->local_scan_gate, 0, sizeof(entry->local_scan_gate));
	}
	SpinLockRelease(&ko_state->shared_lock);
}

/* A physical pass can span SI ticks. Only its cursor is restartable;
 * all buffer/current/PI responsibility remains with the native owners. */
static void
ko_peer_fence_physical_reset_locked(ClusterKoPeerFenceV3 *entry)
{
	entry->physical_applied = false;
	entry->physical_phase = 0;
	entry->physical_cursor = 0;
	entry->physical_revision = 0;
	memset(&entry->physical_gate, 0, sizeof(entry->physical_gate));
	entry->local_scan_complete = false;
	entry->local_scan_cursor = 0;
	entry->local_scan_revision = 0;
	entry->local_writeback_revision = 0;
	memset(&entry->local_scan_gate, 0, sizeof(entry->local_scan_gate));
}

static bool
ko_peer_physical_cut_current_v3(const ClusterKoPeerFenceV3 *before, uint64 revision,
								const ResourceXGateSnapshot *gate)
{
	ResourceXGateSnapshot after;

	return ko_shared_v3_control_at_generation(&before->request, before->peer_route_generation)
		   && ko_shared_v3_peer_identity_current(&before->request)
		   && pg_atomic_read_u64(&ko_state->peer_admission_revision) == revision
		   && cluster_pcm_lock_resource_x_gate_snapshot(&after)
		   && memcmp(gate, &after, sizeof(after)) == 0;
}

static bool
ko_peer_fence_physical_tick_v3(ClusterKoPeerFenceV3 *before,
							   const ClusterKoPeerFenceHandleV3 *handle)
{
	ClusterKoPeerFenceV3 *entry;
	ResourceXGateSnapshot gate;
	SMgrRelation smgr;
	uint64 revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	uint32 cursor = 0, budget = 128;
	uint8 phase = 0;
	bool applied = false;

	/* CONTROL authenticates the KO membership epoch. Resource-X cutover
	 * advances a separate formation; retain and recheck its complete gate. */
	if (revision == 0 || revision == UINT64_MAX || !cluster_pcm_lock_resource_x_gate_snapshot(&gate)
		|| gate.phase != RESOURCE_X_GATE_OPEN
		|| !ko_peer_physical_cut_current_v3(before, revision, &gate)
		|| !cluster_ctrc_relation_removal_ready_shared(before->request.base.key.locator.spcOid,
													   before->request.base.key.locator.dbOid,
													   before->request.base.key.locator.relNumber))
		return false;
	if (before->physical_revision == revision
		&& memcmp(&before->physical_gate, &gate, sizeof(gate)) == 0) {
		phase = before->physical_phase;
		cursor = before->physical_cursor;
	}
	SpinLockAcquire(&ko_state->shared_lock);
	entry = &ko_state->peer_fences[handle->slot];
	if (entry->serial != handle->serial || entry->state != CLUSTER_KO_PEER_FENCE_ACTIVE
		|| !entry->physical_started || entry->failure_sqlerrcode != 0
		|| memcmp(entry->encoded, before->encoded, sizeof(entry->encoded)) != 0
		|| ko_cleaner_pending_locked(before->request.base.key.locator)
		|| ko_extend_pending_locked(before->request.base.key.locator)) {
		SpinLockRelease(&ko_state->shared_lock);
		return false;
	}
	if (!ko_drop_reserve_locked(&entry->request, entry->serial, &entry->drop_identity)) {
		SpinLockRelease(&ko_state->shared_lock);
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("cannot reserve retired DROP identity"),
						errhint("Committed DROP identities remain reserved until instance restart. "
								"Resolve the retained KO operation before restarting and retrying.")));
	}
	/* ERROR cannot leave an old prefix reusable. No KO lock spans native I/O. */
	ko_peer_fence_physical_reset_locked(entry);
	SpinLockRelease(&ko_state->shared_lock);
	smgr = smgropen(before->request.base.key.locator, InvalidBackendId);
	while (phase < 3 && budget > 0) {
		if (phase == 1) {
			for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
				if (smgrexists(smgr, fork))
					smgrimmedsync(smgr, fork);
				if (!ko_peer_physical_cut_current_v3(before, revision, &gate))
					return false;
			}
			phase = 2;
		} else {
			uint32 start = cursor, remaining = budget;
			ClusterBufferDrainResult result
				= cluster_bufmgr_drain_relation_v3(smgr, phase == 0, &budget, &cursor);

			if (budget > remaining || cursor < start || cursor - start > remaining - budget
				|| !ko_peer_physical_cut_current_v3(before, revision, &gate))
				return false;
			if (result == CLUSTER_BUFFER_DRAIN_EMPTY) {
				phase++;
				cursor = 0;
			} else if (result == CLUSTER_BUFFER_DRAIN_REDIRTIED && phase == 2) {
				phase = 0;
				cursor = 0;
				break;
			} else if (result == CLUSTER_BUFFER_DRAIN_WAIT || result == CLUSTER_BUFFER_DRAIN_MORE)
				break;
			else
				return false;
		}
	}
	if (!ko_peer_physical_cut_current_v3(before, revision, &gate))
		return false;
	if (phase == 3) {
		cluster_hw_lease_discard(before->request.base.key.locator, MAIN_FORKNUM);
		KO_BUMP(peer_apply_count);
		CLUSTER_INJECTION_POINT("cluster-ko-peer-skip-ack");
		if (cluster_injection_should_skip("cluster-ko-peer-skip-ack"))
			return false;
	}
	SpinLockAcquire(&ko_state->shared_lock);
	entry = &ko_state->peer_fences[handle->slot];
	if (entry->serial == handle->serial && entry->state == CLUSTER_KO_PEER_FENCE_ACTIVE
		&& entry->failure_sqlerrcode == 0
		&& entry->peer_route_generation == before->peer_route_generation
		&& memcmp(entry->encoded, before->encoded, sizeof(entry->encoded)) == 0
		&& pg_atomic_read_u64(&ko_state->peer_admission_revision) == revision) {
		entry->physical_phase = phase;
		entry->physical_cursor = cursor;
		entry->physical_revision = revision;
		entry->physical_gate = gate;
		entry->physical_applied = applied = phase == 3;
		*before = *entry;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return applied;
}

static bool
ko_peer_discard_cut_current(const ClusterKoPeerFenceV3 *before, uint64 revision,
						   const ResourceXGateSnapshot *gate)
{
	ResourceXGateSnapshot after;

	return before->committed_discard
		&& ko_shared_object_control_at_generation(&before->request,
			before->peer_route_generation, true)
		&& pg_atomic_read_u64(&ko_state->peer_admission_revision) == revision
		&& cluster_pcm_lock_resource_x_gate_snapshot(&after)
		&& after.phase == RESOURCE_X_GATE_OPEN && memcmp(gate, &after, sizeof(after)) == 0;
}

/* Closing stops new entered calls immediately; their exact end and the
 * original finite owners must finish before this becomes I/O-retired.
 * This is neither buffer removal nor READY nor directory completion. */
static bool
ko_peer_discard_close_locked(ClusterKoPeerFenceV3 *entry)
{
	KoDropIdentity *identity;
	uint64 revision;

	if (entry->drop_identity == 0
		&& !ko_drop_reserve_object_locked(&entry->request, entry->serial,
			&entry->drop_identity, true))
		return false;
	identity = ko_peer_discard_identity_locked(entry);
	if (identity == NULL)
		return false;
	if (identity->state == KO_DROP_IDENTITY_RESERVED) {
		revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
		if (revision == 0 || revision >= UINT64_MAX - 1)
			return false;
		identity->state = KO_DROP_IDENTITY_CLOSING;
		pg_atomic_write_u64(&ko_state->peer_admission_revision, revision + 1);
		ko_peer_fence_physical_reset_locked(entry);
	}
	if (ko_directory_io_pending_locked(entry->request.base.key.locator)
		|| ko_cleaner_pending_locked(entry->request.base.key.locator)
		|| ko_extend_pending_locked(entry->request.base.key.locator))
		return false;
	identity->state = KO_DROP_IDENTITY_RETIRED;
	return true;
}

/* The existing SI owner advances at most 128 descriptors, without opening
 * storage or taking new current. Every error consumes only the scan hint;
 * committed negative identity and unresolved owners remain. */
static bool
ko_peer_fence_discard_tick_v4(ClusterKoPeerFenceV3 *before,
							 const ClusterKoPeerFenceHandleV3 *handle)
{
	ResourceXGateSnapshot gate, after;
	ClusterKoPeerFenceV3 *entry;
	ClusterBufferDrainResult result;
	uint64 revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	uint32 cursor = 0, budget = 128, start;
	bool applied = false;

	if (revision == 0 || revision >= UINT64_MAX - 1
		|| !cluster_pcm_lock_resource_x_gate_snapshot(&gate)
		|| gate.phase != RESOURCE_X_GATE_OPEN
		|| !ko_peer_discard_cut_current(before, revision, &gate))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	entry = &ko_state->peer_fences[handle->slot];
	if (entry->serial != handle->serial || !entry->committed_discard
		|| entry->state != CLUSTER_KO_PEER_FENCE_ACTIVE || !entry->physical_started
		|| entry->failure_sqlerrcode != 0
		|| entry->peer_route_generation != before->peer_route_generation
		|| memcmp(entry->encoded, before->encoded, sizeof(entry->encoded)) != 0
		|| pg_atomic_read_u64(&ko_state->peer_admission_revision) != revision
		|| !cluster_pcm_lock_resource_x_gate_snapshot(&after)
		|| memcmp(&gate, &after, sizeof(gate)) != 0
		|| !ko_peer_discard_close_locked(entry)) {
		SpinLockRelease(&ko_state->shared_lock);
		return false;
	}
	/* CLOSING itself invalidated old admission samples. Use its resulting
	 * revision, not the pre-close sample; do not invalidate our own prefix. */
	revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	if (entry->physical_revision == revision
		&& memcmp(&entry->physical_gate, &gate, sizeof(gate)) == 0)
		cursor = entry->physical_cursor;
	ko_peer_fence_physical_reset_locked(entry);
	SpinLockRelease(&ko_state->shared_lock);
	start = cursor;
	result = cluster_bufmgr_discard_relation_v4(before->request.base.key.locator, &budget, &cursor);
	if (budget > 128 || cursor < start || cursor - start > 128 - budget
		|| (result != CLUSTER_BUFFER_DRAIN_EMPTY && result != CLUSTER_BUFFER_DRAIN_MORE
			&& result != CLUSTER_BUFFER_DRAIN_WAIT)
		|| !ko_peer_discard_cut_current(before, revision, &gate))
		return false;
	if (result == CLUSTER_BUFFER_DRAIN_EMPTY)
		cluster_hw_lease_discard(before->request.base.key.locator, MAIN_FORKNUM);
	SpinLockAcquire(&ko_state->shared_lock);
	entry = &ko_state->peer_fences[handle->slot];
	if (entry->serial == handle->serial && entry->committed_discard
		&& entry->state == CLUSTER_KO_PEER_FENCE_ACTIVE && entry->failure_sqlerrcode == 0
		&& entry->peer_route_generation == before->peer_route_generation
		&& memcmp(entry->encoded, before->encoded, sizeof(entry->encoded)) == 0
		&& pg_atomic_read_u64(&ko_state->peer_admission_revision) == revision
		&& cluster_pcm_lock_resource_x_gate_snapshot(&after)
		&& memcmp(&gate, &after, sizeof(gate)) == 0) {
		entry->physical_cursor = cursor;
		entry->physical_revision = revision;
		entry->physical_gate = gate;
		entry->physical_applied = applied = result == CLUSTER_BUFFER_DRAIN_EMPTY;
		*before = *entry;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return applied;
}

static bool
ko_peer_fence_local_scan_v3(const ClusterKoPeerFenceV3 *before,
							const ClusterKoPeerFenceHandleV3 *handle)
{
	ResourceXGateSnapshot gate, after;
	ClusterPcmPiRelationScanV2 scan;
	ClusterKoPeerFenceV3 *entry;
	uint64 revision, writeback_revision = 0;
	uint32 cursor, start;
	bool complete = false;

	revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	if (revision == 0 || revision == UINT64_MAX || !cluster_pcm_lock_resource_x_gate_snapshot(&gate)
		|| gate.phase != RESOURCE_X_GATE_OPEN) {
		ko_peer_fence_scan_reset_v3(handle);
		return false;
	}
	start = before->local_scan_cursor;
	if (before->local_scan_revision != revision
		|| memcmp(&before->local_scan_gate, &gate, sizeof(gate)) != 0)
		start = 0;
	cursor = start;
	/* Consume the old hint before entering PCM. ERROR/cancel leaves it
	 * cleared without taking the KO lock from an unwinding directory stack.
	 * Only the fully revalidated return below publishes a new hint. */
	ko_peer_fence_scan_reset_v3(handle);
	scan = before->committed_discard
		? cluster_pcm_lock_ko_local_relation_discard_v4(before->request.base.key.locator,
			gate.formation, 128, &cursor)
		: cluster_pcm_lock_ko_local_relation_scan_v3(before->request.base.key.locator,
			gate.formation, 128, &cursor);
	if (scan == CLUSTER_PCM_PI_RELATION_EMPTY
		&& !cluster_pi_writeback_local_relation_closed_v3(
			before->request.base.key.locator, before->request.base.epoch, &writeback_revision))
		scan = CLUSTER_PCM_PI_RELATION_PENDING;
	if ((scan != CLUSTER_PCM_PI_RELATION_MORE && scan != CLUSTER_PCM_PI_RELATION_PENDING
		 && scan != CLUSTER_PCM_PI_RELATION_EMPTY)
		|| cursor < start || !cluster_pcm_lock_resource_x_gate_snapshot(&after)
		|| memcmp(&gate, &after, sizeof(gate)) != 0
		|| pg_atomic_read_u64(&ko_state->peer_admission_revision) != revision
		|| (scan == CLUSTER_PCM_PI_RELATION_EMPTY
			&& !cluster_pi_writeback_local_revision_current_v3(writeback_revision))) {
		ko_peer_fence_scan_reset_v3(handle);
		return false;
	}
	SpinLockAcquire(&ko_state->shared_lock);
	entry = &ko_state->peer_fences[handle->slot];
	if (entry->serial == handle->serial && entry->state == CLUSTER_KO_PEER_FENCE_ACTIVE
		&& entry->physical_applied && entry->peer_route_generation == before->peer_route_generation
		&& memcmp(entry->encoded, before->encoded, sizeof(entry->encoded)) == 0
		&& pg_atomic_read_u64(&ko_state->peer_admission_revision) == revision) {
		entry->local_scan_cursor = cursor;
		entry->local_scan_revision = revision;
		entry->local_writeback_revision = writeback_revision;
		entry->local_scan_gate = gate;
		entry->local_scan_complete = complete = scan == CLUSTER_PCM_PI_RELATION_EMPTY;
		/* Existing requests can finish installing after the previous physical
		 * pass. Keep them ACTIVE and resume the original flush/sync/drop pass;
		 * a local protocol observation is never a buffer invalidation. */
		if (scan == CLUSTER_PCM_PI_RELATION_PENDING)
			ko_peer_fence_physical_reset_locked(entry);
	}
	SpinLockRelease(&ko_state->shared_lock);
	return complete;
}

/* Continue the local physical READY proof from the SI owner.  The PCM
 * observer is deliberately called without the KO spinlock; its 128-entry
 * cursor is retained in the volatile peer-fence owner and never enters the
 * v3 wire or a persistent record.  Directory/master responsibility is not
 * consulted here and remains an R3 terminal concern. */
static void
ko_peer_fence_local_ready_tick_v3(void)
{
	if (ko_state == NULL || !AmSinvalBcastProcess() || !cluster_shared_config
		|| cluster_interconnect_tier != CLUSTER_IC_TIER_1)
		return;

	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoPeerFenceV3 before;
		ClusterKoSharedMessageV3 request;
		ClusterKoPeerFenceHandleV3 handle;
		uint64 route_generation;
		ClusterKoPeerFenceStateV3 state;
		bool physical_started, physical_applied, ack_queued, failed;

		SpinLockAcquire(&ko_state->shared_lock);
		state = ko_state->peer_fences[i].state;
		physical_started = ko_state->peer_fences[i].physical_started;
		physical_applied = ko_state->peer_fences[i].physical_applied;
		ack_queued = ko_state->peer_fences[i].ack_queued;
		failed = ko_state->peer_fences[i].failure_sqlerrcode != 0;
		if ((state != CLUSTER_KO_PEER_FENCE_ACTIVE && state != CLUSTER_KO_PEER_FENCE_READY)
			|| (failed ? ko_state->peer_fences[i].failure_queued
					   : (!physical_started || ack_queued))) {
			SpinLockRelease(&ko_state->shared_lock);
			continue;
		}
		before = ko_state->peer_fences[i];
		request = before.request;
		route_generation = before.peer_route_generation;
		handle.slot = i;
		handle.serial = ko_state->peer_fences[i].serial;
		SpinLockRelease(&ko_state->shared_lock);
		if (failed) {
			request.base.verb = CLUSTER_KO_SHARED_ACK;
			request.base.status = CLUSTER_KO_SHARED_FAILED;
			(void)ko_shared_enqueue_object(&request, route_generation, before.committed_discard);
			continue;
		}
		ko_peer_attempt = handle;
		ko_peer_attempt_active = true;

		if (state == CLUSTER_KO_PEER_FENCE_ACTIVE) {
			if (before.committed_discard) {
				if ((!physical_applied
					 || !ko_peer_discard_cut_current(&before, before.physical_revision,
						 &before.physical_gate))
					&& !ko_peer_fence_discard_tick_v4(&before, &handle))
					goto next_owner;
			} else if ((!physical_applied
				 || !ko_peer_physical_cut_current_v3(&before, before.physical_revision,
													 &before.physical_gate))
				&& !ko_peer_fence_physical_tick_v3(&before, &handle))
				goto next_owner;
			if (!ko_peer_fence_local_scan_v3(&before, &handle))
				goto next_owner;
			if (!ko_peer_fence_ready(&request, &handle, before.committed_discard))
				goto next_owner;
		}

		request.base.verb = CLUSTER_KO_SHARED_ACK;
		request.base.status = CLUSTER_KO_SHARED_DONE;
		if (!ko_shared_enqueue_object(&request, route_generation, before.committed_discard))
			goto next_owner;
	next_owner:
		ko_peer_attempt_active = false;
	}
	(void)cluster_ko_peer_fence_gc_tick_v3();
}

/* Compare only the immutable operation identity.  ACK frames change verb and
 * status, while a conflicting frame with the same source/boot/batch must
 * still keep the old terminal owner alive until the ring consumer rejects it. */
static bool
ko_peer_fence_message_key_equal_v3(const ClusterKoSharedMessageV3 *a,
								   const ClusterKoSharedMessageV3 *b)
{
	return a != NULL && b != NULL && a->base.origin_node == b->base.origin_node
		   && a->base.peer_node == b->base.peer_node && a->base.origin_boot == b->base.origin_boot
		   && a->base.peer_boot == b->base.peer_boot && a->base.batch_id == b->base.batch_id;
}

static bool
ko_peer_fence_has_queued_work_locked(const ClusterKoPeerFenceV3 *entry)
{
	uint32 head = pg_atomic_read_u32(&ko_state->inbound_head);
	uint32 tail = pg_atomic_read_u32(&ko_state->inbound_tail);

	for (uint32 cursor = head; cursor != tail;
		 cursor = (cursor + 1) % CLUSTER_KO_INBOUND_CAPACITY) {
		const ClusterKoInboundSlot *slot = &ko_state->inbound[cursor];

		if (slot->shared && slot->v3
			&& ko_peer_fence_message_key_equal_v3(&slot->qualified_v3, &entry->request))
			return true;
	}
	for (uint32 n = 0; n < ko_state->send_v3_count; n++) {
		uint32 cursor = (ko_state->send_v3_head + n) % CLUSTER_KO_SHARED_CAPACITY;

		if (ko_peer_fence_message_key_equal_v3(&ko_state->send_v3[cursor], &entry->request))
			return true;
	}
	return false;
}

static bool
ko_peer_fence_clear_locked(ClusterKoPeerFenceV3 *entry)
{
	uint64 revision;

	if (entry == NULL || entry->state == CLUSTER_KO_PEER_FENCE_EMPTY)
		return false;
	revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	if (revision == 0 || revision >= UINT64_MAX - 1)
		return false;
	if (!ko_drop_release_locked(&entry->request, entry->serial, entry->drop_identity))
		return false;
	memset(entry, 0, sizeof(*entry));
	(void)pg_atomic_fetch_add_u64(&ko_state->peer_admission_revision, 1);
	return true;
}

uint64
cluster_ko_peer_fence_request_sample_v3(void)
{
	uint64 limit = UINT64_MAX;

	if (ko_state == NULL || !cluster_shared_config
		|| cluster_interconnect_tier != CLUSTER_IC_TIER_1 || RecoveryInProgress())
		return limit;
	SpinLockAcquire(&ko_state->shared_lock);
	if (ko_state->peer_fence_serial < UINT64_MAX - 1)
		limit = ko_state->peer_fence_serial + 1;
	SpinLockRelease(&ko_state->shared_lock);
	return limit;
}

bool
cluster_ko_peer_fence_fresh_grant_v3(const ClusterLockAcquireRequest *request)
{
	bool retired = false;

	if (ko_state == NULL || !cluster_shared_config
		|| cluster_interconnect_tier != CLUSTER_IC_TIER_1 || request == NULL
		|| request->resid.type != LOCKTAG_RELATION || request->op != CLUSTER_LOCK_OP_REQUEST
		|| request->current_mode != NoLock || request->ko_fence_serial_limit <= 1
		|| request->ko_fence_serial_limit == UINT64_MAX || request->request_id == 0
		|| request->ko_fence_request_id != request->request_id
		|| !request->hw_grant.local_promoted
		|| !cluster_ges_relation_grant_is_current(&request->hw_grant, &request->resid,
			&request->holder, request->request_id, request->lockmode, request->dontwait))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoPeerFenceV3 saved, *entry;
		bool current;

		entry = &ko_state->peer_fences[i];
		if ((entry->state != CLUSTER_KO_PEER_FENCE_READY
			 && entry->state != CLUSTER_KO_PEER_FENCE_COMMITTED)
			|| entry->serial == 0 || entry->serial >= request->ko_fence_serial_limit
			|| memcmp(&entry->request.ax_resid, &request->resid, sizeof(request->resid)) != 0
			|| memcmp(&entry->request.ax_holder, &request->holder, sizeof(request->holder)) == 0
			|| entry->request.ax_master_node != request->hw_grant.master
			|| entry->peer_route_generation != request->hw_grant.master_generation
			|| !entry->physical_applied || !entry->ack_queued
			|| entry->reply_state != KO_PEER_REPLY_TRANSPORT_OWNED
			|| (entry->holder_probe.nonce != 0 && !entry->holder_probe.reply)
			|| ko_cleaner_pending_locked(entry->request.base.key.locator)
			|| ko_extend_pending_locked(entry->request.base.key.locator)
			|| ko_peer_fence_has_queued_work_locked(entry))
			continue;
		saved = *entry;
		SpinLockRelease(&ko_state->shared_lock);

		/* Metadata and GES proof reads remain outside the KO lock. The wire
		 * generation belongs to the old AX origin; this grant uses our saved
		 * local route cut. Neither counter is substituted for the other. */
		current = ko_shared_v3_control_at_generation(&saved.request, saved.peer_route_generation)
			&& cluster_ges_relation_grant_is_current(&request->hw_grant, &request->resid,
				&request->holder, request->request_id, request->lockmode, request->dontwait);
		SpinLockAcquire(&ko_state->shared_lock);
		entry = &ko_state->peer_fences[i];
		if (current && entry->serial == saved.serial && entry->state == saved.state
			&& entry->peer_route_generation == saved.peer_route_generation
			&& memcmp(entry->encoded, saved.encoded, sizeof(saved.encoded)) == 0
			&& memcmp(&entry->holder_probe, &saved.holder_probe, sizeof(saved.holder_probe)) == 0
			&& entry->physical_applied && entry->ack_queued
			&& entry->reply_state == KO_PEER_REPLY_TRANSPORT_OWNED
			&& !ko_cleaner_pending_locked(entry->request.base.key.locator)
			&& !ko_extend_pending_locked(entry->request.base.key.locator)
			&& !ko_peer_fence_has_queued_work_locked(entry)
			&& ko_peer_fence_clear_locked(entry))
			retired = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	if (retired)
		cluster_sinval_set_proc_latch();
	return retired;
}

bool
cluster_ko_peer_fence_gc_tick_v3(void)
{
	bool pressure = false;
	bool progress = false, retired = false;
	TimestampTz now;

	if (ko_state == NULL || !AmSinvalBcastProcess() || !cluster_shared_config
		|| cluster_interconnect_tier != CLUSTER_IC_TIER_1)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (ko_state->peer_fences[i].state == CLUSTER_KO_PEER_FENCE_EMPTY) {
			pressure = false;
			break;
		} else
			pressure = true;
	SpinLockRelease(&ko_state->shared_lock);
	if (!pressure)
		return false;
	now = GetCurrentTimestamp();

	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoPeerFenceHandleV3 handle;
		ClusterKoPeerFenceV3 saved, *entry;
		ClusterGrdHolderObservation observation;
		LOCKMODE mode;

		SpinLockAcquire(&ko_state->shared_lock);
		entry = &ko_state->peer_fences[i];
		saved = *entry;
		handle.slot = i;
		handle.serial = saved.serial;
		if ((saved.state != CLUSTER_KO_PEER_FENCE_READY
			 && saved.state != CLUSTER_KO_PEER_FENCE_COMMITTED)
			|| !saved.physical_applied || !saved.ack_queued
			|| saved.reply_state != KO_PEER_REPLY_TRANSPORT_OWNED
			|| ko_cleaner_pending_locked(saved.request.base.key.locator)
			|| ko_extend_pending_locked(saved.request.base.key.locator)
			|| ko_peer_fence_has_queued_work_locked(entry)) {
			SpinLockRelease(&ko_state->shared_lock);
			continue;
		}
		SpinLockRelease(&ko_state->shared_lock);

		if (saved.state == CLUSTER_KO_PEER_FENCE_COMMITTED || saved.committed_discard) {
			/* COMMIT supersedes the observation, not the query's owner.
			 * Wait for its exact reply before retiring the terminal record;
			 * PRESENT/UNKNOWN here finish only the query, never prove abort. */
			if (saved.holder_probe.nonce != 0 ? !saved.holder_probe.reply
												: saved.holder_probe.reply)
				continue;
		} else {
			if (saved.holder_probe.nonce == 0) {
				if (cluster_ko_peer_holder_begin_v3(&handle))
					progress = true;
				continue;
			}
			if (!cluster_ko_peer_holder_result_v3(&handle, &observation, &mode))
				continue;
			if (observation != CLUSTER_GRD_HOLDER_ABSENT) {
				/* Renew a completed PRESENT/UNKNOWN observation at the
				 * existing retry cadence. No local wake is needed while
				 * waiting for time, a reply, or the original holder to exit. */
				if ((saved.holder_sent_at == 0 || now < saved.holder_sent_at
					 || now - saved.holder_sent_at >= INT64CONST(100000))
					&& cluster_ko_peer_holder_begin_v3(&handle))
					progress = true;
				continue;
			}
		}

		SpinLockAcquire(&ko_state->shared_lock);
		entry = &ko_state->peer_fences[i];
		if (entry->serial == saved.serial && entry->state == saved.state
			&& entry->peer_route_generation == saved.peer_route_generation
			&& memcmp(entry->encoded, saved.encoded, sizeof(saved.encoded)) == 0
			&& memcmp(&entry->holder_probe, &saved.holder_probe, sizeof(saved.holder_probe)) == 0
			&& entry->physical_applied && entry->ack_queued
			&& entry->reply_state == KO_PEER_REPLY_TRANSPORT_OWNED
			&& !ko_cleaner_pending_locked(entry->request.base.key.locator)
			&& !ko_extend_pending_locked(entry->request.base.key.locator)
			&& !ko_peer_fence_has_queued_work_locked(entry)
			&& (!entry->committed_discard
				|| (ko_peer_discard_identity_locked(entry) != NULL
					&& ko_peer_discard_identity_locked(entry)->state == KO_DROP_IDENTITY_RETIRED))
			&& ko_peer_fence_clear_locked(entry))
			retired = progress = true;
		SpinLockRelease(&ko_state->shared_lock);
	}
	/* A reclaimed slot may unblock the inbound head. Query publication
	 * wakes LMON and an accepted reply wakes SI through their own owners. */
	if (retired)
		cluster_sinval_set_proc_latch();
	return progress;
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
		if (ok && entry->request.operation == CLUSTER_KO_SHARED_V3_OPERATION_DROP) {
			KoDropIdentity *identity;
			uint64 revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);

			if (entry->drop_identity == 0 || entry->drop_identity > KO_DROP_IDENTITY_CAPACITY) {
				ok = false;
				break;
			}
			identity = &ko_state->drop_identities[entry->drop_identity - 1];
			if (identity->fence_serial != entry->serial
				|| memcmp(&identity->request, &entry->request, sizeof(entry->request)) != 0
				|| identity->state < KO_DROP_IDENTITY_RESERVED
				|| identity->state > KO_DROP_IDENTITY_RETIRED) {
				ok = false;
				break;
			}
			if (identity->state == KO_DROP_IDENTITY_RESERVED) {
				if (revision == 0 || revision >= UINT64_MAX - 1) {
					ok = false;
					break;
				}
				identity->state = KO_DROP_IDENTITY_CLOSING;
				pg_atomic_write_u64(&ko_state->peer_admission_revision, revision + 1);
			}
			ok = !ko_directory_io_pending_locked(entry->request.base.key.locator);
			if (ok)
				identity->state = KO_DROP_IDENTITY_RETIRED;
		}
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

	if (context == NULL || (context->committed_discard && !context->v3))
		return false;
	m = context->v3 ? &context->request_v3.base : &context->request;
	current = *m;
	if (context->v3
		&& !ko_shared_object_control_at_generation(&context->request_v3,
			context->request_v3.ax_master_generation, context->committed_discard))
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
	if (ko_state != NULL && completion->slot < CLUSTER_KO_SHARED_CAPACITY) {
		ClusterKoSharedContext *entry;
		SpinLockAcquire(&ko_state->shared_lock);
		entry = &ko_state->contexts[completion->slot];
		if (entry->used && !entry->structure_owned && entry->pid == completion->pid
			&& entry->serial == completion->serial) {
			if (entry->v3)
				entry->origin_detached = true;
			else
				memset(entry, 0, sizeof(*entry));
		}
		SpinLockRelease(&ko_state->shared_lock);
	}
	ko_native_abandon_unready(completion);
	ko_native_release_reservation(completion);
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
	if (phase == RESOURCE_RELEASE_AFTER_LOCKS && ko_extend_local.used && ko_extend_aborting
		&& ko_extend_owner == CurrentResourceOwner && ko_extend_local.pid == MyProcPid)
		(void)cluster_ko_extend_end_v3(ko_extend_local.serial);
	if (phase == RESOURCE_RELEASE_AFTER_LOCKS && ko_create_local.used && ko_create_aborting
		&& ko_create_owner == CurrentResourceOwner && ko_create_local.pid == MyProcPid)
		(void)cluster_ko_create_end_v3(ko_create_local.serial, false);
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

/* Preserve the original per-peer results, never synthesize legacy structural
 * acceptance from READY. The caller separately validates the full live cut;
 * COMMIT may retain an old cut for recovery without granting execution.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool
ko_shared_ready_v3(const ClusterKoSharedContext *context)
{
	uint16 expected = 0;

	if (!context->used || !context->v3 || context->origin_detached || context->ax_failed
		|| context->request.origin_node != cluster_node_id
		|| memcmp(&context->request, &context->request_v3.base, sizeof(context->request)) != 0
		|| context->request_failed_peers != 0 || context->request_uncertain_peers != 0
		|| context->request_sending_peers != 0)
		return false;
	for (unsigned peer = 0; peer < CLUSTER_KO_SHARED_NODE_LIMIT; peer++)
		if (peer != cluster_node_id && context->peer_boots[peer] != 0)
			expected |= (uint16)(1u << peer);
	return context->request_sent_peers == expected && context->request_done_peers == expected;
}

static bool
ko_completion_snapshot(const ClusterKoCompletionV2 *completion, ClusterKoSharedContext *out)
{
	bool valid;
	const ClusterKoCompletionV2 *owned = *ko_completion_link(completion);
	if (owned == NULL || owned->native_handed_off || owned->pid != MyProcPid
		|| !ko_completion_owner_current(owned) || CurrentResourceOwner == NULL
		|| (owned->barrier_committed_discard && !owned->barrier_v3)
		|| CritSectionCount != 0 || ko_state == NULL)
		return false;
	if (owned->barrier_private) {
		if (!owned->native_transaction
			|| (!owned->barrier_v3 && owned->slot != CLUSTER_KO_SHARED_CAPACITY)
			|| (owned->barrier_v3
				&& owned->barrier_request_v3.operation == CLUSTER_KO_SHARED_V3_OPERATION_DROP
				&& !owned->origin_data_closed)
			|| owned->serial == 0)
			return false;
		memset(out, 0, sizeof(*out));
		out->used = out->complete = true;
		out->pid = owned->pid;
		out->serial = owned->serial;
		out->drop_identity = owned->drop_identity;
		out->drop_identity_serial = owned->drop_identity_serial;
		out->v3 = owned->barrier_v3;
		out->committed_discard = owned->barrier_committed_discard;
		out->origin_checkpoint = out->origin_data_closed = owned->origin_data_closed;
		out->request = owned->barrier_v3 ? owned->barrier_request_v3.base
											: owned->barrier_request;
		if (owned->barrier_v3) {
			out->request_v3 = owned->barrier_request_v3;
			out->request_sent_peers = out->request_done_peers = owned->barrier_ready_peers;
		}
		memcpy(out->peer_boots, owned->barrier_boots, sizeof(out->peer_boots));
		return (!out->v3 || ko_shared_ready_v3(out)) && ko_shared_origin_current(out);
	}
	if (owned->slot >= CLUSTER_KO_SHARED_CAPACITY)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	*out = ko_state->contexts[owned->slot];
	valid = out->used && out->complete && !out->structure_owned && out->pid == owned->pid
			&& out->serial == owned->serial;
	SpinLockRelease(&ko_state->shared_lock);
	return valid && (!out->v3 || ko_shared_ready_v3(out)) && ko_shared_origin_current(out);
}

/* No public value can create this proof. Move only this process's exact
 * completed barrier, with its original full cut. v3 keeps the origin slot
 * until the native AX release has actually finished.
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
		completion->barrier_committed_discard = before.committed_discard;
		completion->barrier_request = before.request;
		if (before.v3) {
			completion->barrier_request_v3 = before.request_v3;
			completion->barrier_ready_peers = before.request_done_peers;
			completion->drop_identity = before.drop_identity;
			completion->drop_identity_serial = before.drop_identity_serial;
		}
		memcpy(completion->barrier_boots, before.peer_boots, sizeof(before.peer_boots));
		completion->barrier_private = true;
		/* v3 retains its preallocated origin slot through native lock release.
		 * The private proof still serves only the committed storage owner. */
		if (!before.v3) {
			memset(&ko_state->contexts[completion->slot], 0, sizeof(before));
			completion->slot = CLUSTER_KO_SHARED_CAPACITY;
		}
		moved = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return moved;
}

/* Snapshot the same accepted origin operation before old DATA is closed.
 * No private caller flag supplies an AX grant, peer result or identity. */
static bool
ko_origin_checkpoint_snapshot(const ClusterKoCompletionV2 *owned, ClusterKoSharedContext *out)
{
	bool valid;
	if (owned == NULL || *ko_completion_link(owned) != owned || ko_state == NULL
		|| !owned->native_transaction || !owned->barrier_private || !owned->barrier_v3
		|| owned->barrier_request_v3.operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP
		|| owned->native_handed_off || owned->postcommit || owned->pid != MyProcPid
		|| !ko_completion_owner_current(owned) || CurrentResourceOwner == NULL
		|| CritSectionCount != 0 || owned->slot >= CLUSTER_KO_SHARED_CAPACITY)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	*out = ko_state->contexts[owned->slot];
	valid = out->used && out->v3 && out->complete && !out->structure_owned
		&& !out->origin_detached && !out->ax_release_requested && !out->ax_failed
		&& out->cancel_sqlerrcode == 0 && out->pid == owned->pid && out->serial == owned->serial
		&& out->drop_identity == owned->drop_identity
		&& out->drop_identity_serial == owned->drop_identity_serial
		&& memcmp(&out->request_v3, &owned->barrier_request_v3, sizeof(out->request_v3)) == 0
		&& memcmp(out->peer_boots, owned->barrier_boots, sizeof(out->peer_boots)) == 0;
	SpinLockRelease(&ko_state->shared_lock);
	return valid && ko_shared_ready_v3(out) && ko_shared_origin_current(out)
		&& ko_shared_v3_local_binding_current(&out->request_v3);
}

static bool
ko_origin_checkpoint_cut_current(const ClusterKoCompletionV2 *owned,
	const ClusterKoSharedContext *before, uint64 revision, const ResourceXGateSnapshot *gate)
{
	ClusterKoSharedContext after;
	ResourceXGateSnapshot current;
	return ko_origin_checkpoint_snapshot(owned, &after)
		&& memcmp(before, &after, sizeof(after)) == 0
		&& pg_atomic_read_u64(&ko_state->peer_admission_revision) == revision
		&& cluster_pcm_lock_resource_x_gate_snapshot(&current)
		&& memcmp(gate, &current, sizeof(current)) == 0;
}

/* The scanner supplies this value while holding the exact registry entry lock.
 * Keep the last value in the existing KO dump region and emit one complete
 * identity record for this reason.  This is observation only; no field below
 * is consumed as a retirement or DATA proof. */
static void
ko_origin_checkpoint_record_pending_identity(const ClusterPcmPiPendingIdentityV1 *pending)
{
	ClusterKoOriginCheckpointPendingV1 snapshot;
	uint64 bit, previous;

	if (ko_state == NULL || pending == NULL)
		return;
	snapshot = (ClusterKoOriginCheckpointPendingV1) {
		.registry_slot = pending->registry_slot,
		.locator = BufTagGetRelFileLocator(&pending->tag),
		.forknum = pending->tag.forkNum,
		.blocknum = pending->tag.blockNum,
		.binding_generation = pending->binding_generation,
		.local_pi_revision = pending->local_pi_revision,
		.pcm_state = pending->pcm_state,
		.current_holder_node = pending->current_holder_node,
		.master_holder_node = pending->master_holder_node,
		.master_node = pending->master_node,
		.pending_x_requester_node = pending->pending_x_requester_node,
		.s_holders_bitmap = pending->s_holders_bitmap,
		.pi_holders_bitmap = pending->pi_holders_bitmap,
		.pi_source_node_first = pending->pi_source_node_first,
		.pi_source_node_last = pending->pi_source_node_last,
		.pi_source_boot_first = pending->pi_source_boot_first,
		.pi_source_boot_last = pending->pi_source_boot_last,
		.pi_source_slot_first = pending->pi_source_slot_first,
		.pi_source_slot_last = pending->pi_source_slot_last,
		.pending_predicate_mask = pending->pending_predicate_mask,
		.first_token = pending->local_pi_first.token,
		.last_token = pending->local_pi_last.token,
		.valid = pending->valid
	};
	SpinLockAcquire(&ko_state->shared_lock);
	ko_state->origin_checkpoint_pending = snapshot;
	SpinLockRelease(&ko_state->shared_lock);

	bit = UINT64CONST(1) << CLUSTER_KO_ORIGIN_REASON_PI_SCAN_PENDING;
	previous = pg_atomic_fetch_or_u64(&ko_state->origin_checkpoint_pending_logged_mask, bit);
	if ((previous & bit) == 0) {
		ereport(LOG,
			(errmsg("native DROP origin checkpoint pi_scan_pending identity valid=%s slot=%u locator=%u/%u/%u fork=%u block=%u binding_generation=" UINT64_FORMAT " local_pi_revision=" UINT64_FORMAT " pcm_state=%u current_holder=%d master_holder=%d master_node=%d pending_x_requester=%d s_holders=%u pi_holders=%u predicate_mask=0x%08X pi_first_source_node=%d pi_first_source_boot=" UINT64_FORMAT " pi_first_source_slot=%u pi_last_source_node=%d pi_last_source_boot=" UINT64_FORMAT " pi_last_source_slot=%u pi_first_token=" UINT64_FORMAT " pi_last_token=" UINT64_FORMAT,
				snapshot.valid ? "true" : "false",
				snapshot.registry_slot, snapshot.locator.spcOid, snapshot.locator.dbOid,
				snapshot.locator.relNumber, snapshot.forknum, snapshot.blocknum,
				snapshot.binding_generation, snapshot.local_pi_revision, snapshot.pcm_state,
				snapshot.current_holder_node, snapshot.master_holder_node, snapshot.master_node,
				snapshot.pending_x_requester_node, snapshot.s_holders_bitmap,
				snapshot.pi_holders_bitmap, snapshot.pending_predicate_mask,
				snapshot.pi_source_node_first, snapshot.pi_source_boot_first,
				snapshot.pi_source_slot_first, snapshot.pi_source_node_last,
				snapshot.pi_source_boot_last, snapshot.pi_source_slot_last,
				 snapshot.first_token, snapshot.last_token)));
	}
}

/* Record one exact local predicate without making the observation an
 * authority.  Counters live in the existing KO dump region; the first log
 * per reason is guarded by a shared bit so concurrent origin backends do not
 * turn a diagnosis into a log storm. */
static void
ko_origin_checkpoint_record(ClusterKoOriginCheckpointReason reason, bool wait,
	RelFileLocator locator, uint8 phase, uint32 cursor, uint64 revision,
	uint64 saved_revision, uint64 wb_revision, const ResourceXGateSnapshot *gate)
{
	uint64 bit, previous;

	if (ko_state == NULL || reason >= CLUSTER_KO_ORIGIN_REASON_COUNT)
		return;
	if (wait)
		(void)pg_atomic_fetch_add_u64(&ko_state->origin_checkpoint_wait_count[reason], 1);
	else
		(void)pg_atomic_fetch_add_u64(&ko_state->origin_checkpoint_restart_count[reason], 1);
	bit = UINT64CONST(1) << reason;
	previous = pg_atomic_fetch_or_u64(&ko_state->origin_checkpoint_logged_mask, bit);
	if ((previous & bit) == 0) {
		uint32 gate_phase = gate != NULL ? gate->phase : UINT32_MAX;
		uint64 formation = gate != NULL ? gate->formation : 0;
		uint64 freeze_generation = gate != NULL ? gate->freeze_generation : 0;

		ereport(LOG,
			(errmsg("native DROP origin checkpoint %s reason=%s locator=%u/%u/%u phase=%u cursor=%u revision=" UINT64_FORMAT " saved_revision=" UINT64_FORMAT " wb_revision=" UINT64_FORMAT " gate_phase=%u gate_formation=" UINT64_FORMAT " gate_freeze=" UINT64_FORMAT,
				wait ? "wait" : "restart",
				cluster_ko_origin_checkpoint_reason_name(reason),
				locator.spcOid, locator.dbOid, locator.relNumber,
				phase, cursor, revision, saved_revision, wb_revision,
				gate_phase, formation, freeze_generation)));
	}
}

/* Same object checkpoint, local execution: no self REQUEST/READY and no
 * terminal or file-delete proof. Stack cursors cannot survive ERROR/cancel.
 * The original shared operation retains admission/AX responsibility. */
static void
ko_native_origin_checkpoint(ClusterKoCompletionV2 *owned)
{
	ClusterKoSharedContext before;
	ResourceXGateSnapshot saved_gate = {0};
	uint64 saved_revision = 0;
	uint32 cursor = 0;
	uint8 phase = 0;
	bool armed = false;
	ClusterKoOriginCheckpointReason wait_reason = CLUSTER_KO_ORIGIN_REASON_PROGRESS;
	RelFileLocator locator;

	if (!ko_origin_checkpoint_snapshot(owned, &before))
		goto invalid;
	locator = before.request.key.locator;
	SpinLockAcquire(&ko_state->shared_lock);
	{
		uint64 revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
		ClusterKoSharedContext *entry = &ko_state->contexts[owned->slot];
		if (revision != 0 && revision < UINT64_MAX - 1 && !entry->origin_checkpoint
			&& memcmp(entry, &before, sizeof(before)) == 0 && !ko_drop_closed_locked(locator)) {
			entry->origin_checkpoint = true;
			(void)pg_atomic_fetch_add_u64(&ko_state->peer_admission_revision, 1);
			armed = true;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
	if (!armed)
		goto invalid;

	for (;;) {
		ResourceXGateSnapshot gate;
		uint64 revision, wb_revision = 0;
		uint32 budget = 128;
		SMgrRelation smgr;
		bool gate_observed, ctrc_ready, cleaner_pending, extend_pending, closed = false;

		wait_reason = CLUSTER_KO_ORIGIN_REASON_PROGRESS;

		CHECK_FOR_INTERRUPTS();
		if (!ko_origin_checkpoint_snapshot(owned, &before) || !before.origin_checkpoint)
			goto invalid;
		revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
		gate_observed = cluster_pcm_lock_resource_x_gate_snapshot(&gate);
		ctrc_ready = cluster_ctrc_relation_removal_ready_shared(locator.spcOid, locator.dbOid,
			locator.relNumber);
		if (revision == 0 || revision == UINT64_MAX) {
			wait_reason = CLUSTER_KO_ORIGIN_REASON_PEER_REVISION;
			ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, gate_observed ? &gate : NULL);
			goto restart;
		}
		if (!gate_observed || gate.phase != RESOURCE_X_GATE_OPEN) {
			wait_reason = CLUSTER_KO_ORIGIN_REASON_GATE_NOT_OPEN;
			ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, gate_observed ? &gate : NULL);
			goto restart;
		}
		if (!ctrc_ready) {
			wait_reason = CLUSTER_KO_ORIGIN_REASON_CTRC_NOT_READY;
			ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, &gate);
			goto restart;
		}
		if (saved_revision != 0 && revision != saved_revision) {
			wait_reason = CLUSTER_KO_ORIGIN_REASON_PEER_REVISION;
			ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
				saved_revision, wb_revision, &gate);
		}
		if (saved_revision != 0 && memcmp(&saved_gate, &gate, sizeof(gate)) != 0) {
			wait_reason = CLUSTER_KO_ORIGIN_REASON_GATE_CHANGED;
			ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
				saved_revision, wb_revision, &gate);
		}
		if (revision != saved_revision || memcmp(&saved_gate, &gate, sizeof(gate)) != 0) {
			phase = 0;
			cursor = 0;
		}
		SpinLockAcquire(&ko_state->shared_lock);
		cleaner_pending = ko_cleaner_pending_locked(locator);
		extend_pending = ko_extend_pending_locked(locator);
		SpinLockRelease(&ko_state->shared_lock);
		if (cleaner_pending) {
			wait_reason = CLUSTER_KO_ORIGIN_REASON_CLEANER_PENDING;
			ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, &gate);
		}
		if (extend_pending) {
			wait_reason = CLUSTER_KO_ORIGIN_REASON_EXTEND_PENDING;
			ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, &gate);
		}
		if (cleaner_pending || extend_pending)
			goto restart;
		if (!ko_origin_checkpoint_cut_current(owned, &before, revision, &gate)) {
			wait_reason = CLUSTER_KO_ORIGIN_REASON_CUT_CURRENT;
			ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, &gate);
			goto restart;
		}
		smgr = smgropen(locator, InvalidBackendId);
		while (phase < 3 && budget > 0) {
			if (phase == 1) {
				for (ForkNumber fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
					if (smgrexists(smgr, fork))
						smgrimmedsync(smgr, fork);
					if (!ko_origin_checkpoint_cut_current(owned, &before, revision, &gate)) {
						wait_reason = CLUSTER_KO_ORIGIN_REASON_CUT_CURRENT;
						ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor,
							revision, saved_revision, wb_revision, &gate);
						goto restart;
					}
				}
				phase = 2;
			} else {
				uint32 start = cursor, remaining = budget;
				ClusterBufferDrainResult result
					= cluster_bufmgr_drain_relation_v3(smgr, phase == 0, &budget, &cursor);
				if (budget > remaining || cursor < start || cursor - start > remaining - budget)
					goto invalid;
				if (!ko_origin_checkpoint_cut_current(owned, &before, revision, &gate)) {
					wait_reason = CLUSTER_KO_ORIGIN_REASON_CUT_CURRENT;
					ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
						saved_revision, wb_revision, &gate);
					goto restart;
				}
				if (result == CLUSTER_BUFFER_DRAIN_EMPTY) {
					phase++;
					cursor = 0;
				} else if (result == CLUSTER_BUFFER_DRAIN_REDIRTIED && phase == 2) {
					wait_reason = CLUSTER_KO_ORIGIN_REASON_DRAIN_REDIRTIED;
					ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
						saved_revision, wb_revision, &gate);
					goto restart;
				} else if (result == CLUSTER_BUFFER_DRAIN_WAIT || result == CLUSTER_BUFFER_DRAIN_MORE) {
					wait_reason = result == CLUSTER_BUFFER_DRAIN_WAIT
						? CLUSTER_KO_ORIGIN_REASON_DRAIN_WAIT
						: CLUSTER_KO_ORIGIN_REASON_DRAIN_MORE;
					break;
				}
				else
					goto invalid;
			}
		}
		if (phase == 3) {
			ClusterPcmPiPendingIdentityV1 pending_identity;
			ClusterPcmPiRelationScanV2 scan;
			uint32 start = cursor;

			memset(&pending_identity, 0, sizeof(pending_identity));
			scan = cluster_pcm_lock_ko_local_relation_scan_v3_identity(locator, gate.formation,
				128, &cursor, &pending_identity);
			if (scan == CLUSTER_PCM_PI_RELATION_PENDING) {
				wait_reason = CLUSTER_KO_ORIGIN_REASON_PI_SCAN_PENDING;
				ko_origin_checkpoint_record_pending_identity(&pending_identity);
				ko_origin_checkpoint_record(wait_reason, true, locator, phase, cursor, revision,
					saved_revision, wb_revision, &gate);
				/* A pending local owner is the current bounded scan item.  Keep
				 * its cursor and the stable gate/revision while waiting; restarting
				 * at phase zero would repeatedly rescan every earlier registry slot
				 * and can starve this owner from ever reaching the final proof. */
				saved_gate = gate;
				saved_revision = revision;
				goto wait;
			}
			if (scan != CLUSTER_PCM_PI_RELATION_EMPTY
				&& (scan != CLUSTER_PCM_PI_RELATION_MORE || cursor <= start || cursor - start > 128))
				goto invalid;
			if (scan == CLUSTER_PCM_PI_RELATION_EMPTY) {
				if (!cluster_pi_writeback_local_relation_closed_v3(locator, before.request.epoch,
						&wb_revision)) {
					wait_reason = CLUSTER_KO_ORIGIN_REASON_WB_NOT_CLOSED;
					ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
						saved_revision, wb_revision, &gate);
					goto restart;
				}
				closed = true;
			} else
				wait_reason = CLUSTER_KO_ORIGIN_REASON_PI_SCAN_MORE;
		}
		if (!ko_origin_checkpoint_cut_current(owned, &before, revision, &gate)) {
			wait_reason = CLUSTER_KO_ORIGIN_REASON_CUT_CURRENT;
			ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
				saved_revision, wb_revision, &gate);
			goto restart;
		}
		if (closed) {
			ResourceXGateSnapshot final_gate_snapshot;
			bool final_ctrc_ready;
			bool final_wb_current;
			bool final_peer_revision = false;
			bool final_context = false;
			bool final_cleaner = false;
			bool final_extend = false;
			bool final_gate_current = false;

			final_ctrc_ready = cluster_ctrc_relation_removal_ready_shared(locator.spcOid,
				locator.dbOid, locator.relNumber);
			final_wb_current = cluster_pi_writeback_local_revision_current_v3(wb_revision);
			if (!final_ctrc_ready) {
				wait_reason = CLUSTER_KO_ORIGIN_REASON_CTRC_NOT_READY;
				ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, &gate);
			}
			if (!final_wb_current) {
				wait_reason = CLUSTER_KO_ORIGIN_REASON_WB_REVISION;
				ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, &gate);
			}
			if (!final_ctrc_ready || !final_wb_current)
				goto restart;
			SpinLockAcquire(&ko_state->shared_lock);
			final_peer_revision = revision < UINT64_MAX - 1
				&& pg_atomic_read_u64(&ko_state->peer_admission_revision) == revision;
			final_context = memcmp(&ko_state->contexts[owned->slot], &before, sizeof(before)) == 0;
			final_cleaner = !ko_cleaner_pending_locked(locator);
			final_extend = !ko_extend_pending_locked(locator);
			final_wb_current = cluster_pi_writeback_local_revision_current_v3(wb_revision);
			/* Original bounded atomic reader; no PCM/CF lock under KO. */
			final_gate_current = cluster_pcm_lock_resource_x_gate_snapshot(&final_gate_snapshot)
				&& memcmp(&final_gate_snapshot, &gate, sizeof(gate)) == 0;
			closed = final_peer_revision && final_context && final_cleaner && final_extend
				&& final_wb_current && final_gate_current;
			if (closed) {
				ko_state->contexts[owned->slot].origin_data_closed = true;
				owned->origin_data_closed = true;
				(void)pg_atomic_fetch_add_u64(&ko_state->peer_admission_revision, 1);
			}
			SpinLockRelease(&ko_state->shared_lock);
			if (closed)
				return;
			if (!final_peer_revision) {
				wait_reason = CLUSTER_KO_ORIGIN_REASON_PEER_REVISION;
				ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, &gate);
			}
			if (!final_context) {
				wait_reason = CLUSTER_KO_ORIGIN_REASON_CONTEXT_CHANGED;
				ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, &gate);
			}
			if (!final_cleaner) {
				wait_reason = CLUSTER_KO_ORIGIN_REASON_CLEANER_PENDING;
				ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, &gate);
			}
			if (!final_extend) {
				wait_reason = CLUSTER_KO_ORIGIN_REASON_EXTEND_PENDING;
				ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, &gate);
			}
			if (!final_wb_current) {
				wait_reason = CLUSTER_KO_ORIGIN_REASON_WB_REVISION;
				ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, &gate);
			}
			if (!final_gate_current) {
				wait_reason = CLUSTER_KO_ORIGIN_REASON_GATE_CHANGED;
				ko_origin_checkpoint_record(wait_reason, false, locator, phase, cursor, revision,
					saved_revision, wb_revision, &gate);
			}
			goto restart;
		}
		saved_gate = gate;
		saved_revision = revision;
		goto wait;
	restart:
		phase = 0;
		cursor = 0;
		saved_revision = 0;
	wait:
		ko_origin_checkpoint_record(wait_reason, true, locator, phase, cursor,
			revision, saved_revision, wb_revision,
			gate_observed ? &gate : NULL);
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 100L,
			WAIT_EVENT_CLUSTER_OBJECT_FLUSH_WAIT);
		ResetLatch(MyLatch);
	}

invalid:
	ereport(ERROR, (errcode(ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE),
		errmsg("could not prove the native DROP origin checkpoint"),
		errhint("Retry after the original object and formation owners have settled.")));
}

/* Local, immutable owner qualification only. Safe under a buffer header;
 * CONTROL/AX/shared slot validation belongs to the begin outside that lock. */
static bool
ko_origin_space_owner_current(const ClusterKoCompletionV2 *owned, RelFileLocator locator)
{
	return owned->native_transaction && owned->barrier_private && owned->barrier_v3
		&& owned->origin_data_closed && !owned->native_handed_off && !owned->postcommit
		&& owned->pid == MyProcPid && ko_completion_owner_current(owned)
		&& owned->barrier_request_v3.operation == CLUSTER_KO_SHARED_V3_OPERATION_DROP
		&& RelFileLocatorEquals(owned->barrier_request_v3.base.key.locator, locator);
}

static bool
ko_origin_space_read_begin(RelFileLocator locator, ForkNumber fork, BlockNumber block,
	ClusterKoPeerAdmissionV3 *out)
{
	ClusterKoCompletionV2 *candidate = NULL;
	ClusterKoSharedContext before;
	bool allowed;
	uint64 revision;

	if (fork != SPACE_FORKNUM || block > 1 || RecoveryInProgress())
		return false;
	for (ClusterKoCompletionV2 *owned = ko_completions; owned != NULL; owned = owned->next) {
		if (!ko_origin_space_owner_current(owned, locator))
			continue;
		if (candidate != NULL)
			return false;
		candidate = owned;
	}
	if (candidate == NULL || !ko_origin_checkpoint_snapshot(candidate, &before)
		|| !before.origin_checkpoint || !before.origin_data_closed)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	allowed = revision != 0 && revision != UINT64_MAX && !ko_drop_closed_locked(locator)
		&& memcmp(&ko_state->contexts[candidate->slot], &before, sizeof(before)) == 0;
	if (allowed) {
		memset(out, 0, sizeof(*out));
		out->locator = locator;
		out->fork = fork;
		out->block = block;
		out->revision = revision;
		out->origin_serial = candidate->serial;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return allowed;
}

static bool
ko_origin_space_read_current(const ClusterKoPeerAdmissionV3 *sample, RelFileLocator locator,
	ForkNumber fork, BlockNumber block)
{
	if (ko_state == NULL || !cluster_shared_config || cluster_interconnect_tier != CLUSTER_IC_TIER_1
		|| sample->origin_serial == 0 || sample->extension_serial != 0 || sample->cleaner.serial != 0
		|| fork != SPACE_FORKNUM || block > 1 || sample->fork != fork || sample->block != block
		|| !RelFileLocatorEquals(sample->locator, locator) || sample->revision == 0
		|| sample->revision == UINT64_MAX
		|| pg_atomic_read_u64(&ko_state->peer_admission_revision) != sample->revision)
		return false;
	for (const ClusterKoCompletionV2 *owned = ko_completions; owned != NULL; owned = owned->next)
		if (owned->serial == sample->origin_serial)
			return ko_origin_space_owner_current(owned, locator);
	return false;
}

static bool
ko_shared_context_covers(const ClusterKoSharedContext *context, const ClusterSpaceIdentityKey *key,
						 const uint8 incarnation[16])
{
	return key != NULL && incarnation != NULL
		   && context->request.key.system_identifier == key->system_identifier
		   && context->request.key.database_incarnation == key->database_incarnation
		   && memcmp(context->request.key.storage_uuid, key->storage_uuid, 16) == 0
		   && RelFileLocatorEquals(context->request.key.locator, key->locator)
		   && memcmp(context->request.incarnation, incarnation, 16) == 0;
}

bool
cluster_ko_shared_covers_v2(const ClusterKoCompletionV2 *completion,
							const ClusterSpaceIdentityKey *key, const uint8 incarnation[16])
{
	ClusterKoSharedContext context;
	return ko_completion_snapshot(completion, &context)
		   && ko_shared_context_covers(&context, key, incarnation);
}

/* This predicate is for the original native structural writer only. The
 * caller supplies the old identity while holding the SPACE content locks;
 * reusing a completed drain cannot authorize another operation or version.
 * LOCALLOCK is backend-local and read-only: no GES wait or extra page read.
 * Saved observations and post-commit owners do not acquire this permission. */
static bool
ko_shared_structure_current(const ClusterKoCompletionV2 *completion,
							const ClusterSpaceIdentity *identity, ClusterSpaceWalAction action)
{
	ClusterKoSharedContext context;
	uint16 operation;

	if (identity == NULL
		|| (action != CLUSTER_SPACE_WAL_TRUNCATE && action != CLUSTER_SPACE_WAL_TOMBSTONE)
		|| !ko_completion_snapshot(completion, &context)
		|| !ko_shared_context_covers(&context, &identity->key, identity->incarnation))
		return false;
	if (!context.v3)
		return true;
	operation = context.request_v3.operation;
	return ko_shared_v3_space_matches(&context.request_v3, identity)
		   && ((action == CLUSTER_SPACE_WAL_TRUNCATE
				&& operation == CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE)
			   || (action == CLUSTER_SPACE_WAL_TOMBSTONE
				   && (operation == CLUSTER_KO_SHARED_V3_OPERATION_DROP
					   || operation == CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD)))
		   && ko_shared_v3_local_binding_current(&context.request_v3);
}

/* Record preparation may consume an unsent registration. Physical publication
 * still requires the original completed barrier and local DROP closure. */
bool
cluster_ko_shared_drop_write_current(const ClusterKoCompletionV2 *completion,
									const ClusterSpaceIdentity *identity)
{
	const ClusterKoCompletionV2 *owned = *ko_completion_link(completion);

	return owned != NULL && owned->native_transaction && !owned->native_pending
		&& !owned->space_observed
		&& (!owned->barrier_v3
			|| owned->barrier_request_v3.operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP
			|| owned->origin_data_closed)
		&& ko_shared_structure_current(owned, identity, CLUSTER_SPACE_WAL_TOMBSTONE);
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
		/* DROP's final SPACE q is consumed only after the origin's own
		 * pre-COMMIT checkpoint has reached local Closed.  The shared
		 * snapshot below is a second identity/cut check; it must not be
		 * allowed to turn a stale owner-local state into a durable result. */
		if (owned->barrier_v3
			&& owned->barrier_request_v3.operation == CLUSTER_KO_SHARED_V3_OPERATION_DROP
			&& !owned->origin_data_closed)
			return false;
	} else
		return false;
	if (!ko_shared_structure_current(owned, &change.identity.expected, change.identity.action))
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

/* A native carrier keeps its original protocol version throughout handoff
 * and background execution. One version's capabilities cannot admit another. */
static bool
ko_native_capable(bool v3, bool committed_discard)
{
	uint32 capabilities = cluster_ic_local_capability_word();
	uint32 required = v3
		? PGRAC_IC_HELLO_CAP_KO_COMMIT_FENCE_V1
		  | (committed_discard ? PGRAC_IC_HELLO_CAP_KO_COMMITTED_DISCARD_V1
							   : PGRAC_IC_HELLO_CAP_KO_OBJECT_DRAIN_V3)
		: PGRAC_IC_HELLO_CAP_PI_STRUCTURAL_V2;

	return (!committed_discard || v3)
		&& (!v3 || cluster_interconnect_tier == CLUSTER_IC_TIER_1)
		&& (capabilities & required) == required;
}

/* Retain the original committed proof even if its cut has since gone stale.
 * Retention is not authority to execute: the background consumer must recheck
 * the entire original scope before doing anything. Preallocation makes this
 * postcommit transfer independent of active-slot capacity and allocator I/O. */
static bool
ko_native_publish_registration(ClusterKoCompletionV2 *owned)
{
	ClusterKoSharedContext *origin;
	ClusterKoNativeContinuation *work = owned->native_reserved;
	bool published = false;

	if (!owned->barrier_v3 || owned->space_observed || owned->truncate_observed
		|| owned->drop_observed || owned->origin_data_closed || owned->barrier_ready_peers != 0
		|| owned->slot >= CLUSTER_KO_SHARED_CAPACITY
		|| (owned->barrier_request_v3.operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP
			&& owned->barrier_request_v3.operation != CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD))
		return false;
	/* This is preservation after durable COMMIT, not permission to execute.
	 * Do not refresh the operation or demand a still-live remote proof here. */
	SpinLockAcquire(&ko_state->shared_lock);
	origin = &ko_state->contexts[owned->slot];
	if (origin->used && origin->v3 && !origin->complete && !origin->committed_drain_pending
		&& origin->committed_discard == owned->barrier_committed_discard
		&& !origin->origin_detached && !origin->ax_release_requested && !origin->ax_failed
		&& !origin->structure_owned && origin->cancel_sqlerrcode == 0
		&& origin->pid == owned->pid && origin->serial == owned->serial
		&& (origin->request_sent_peers | origin->request_sending_peers
			| origin->request_uncertain_peers | origin->request_done_peers
			| origin->request_failed_peers) == 0
		&& memcmp(&origin->request, &owned->barrier_request_v3.base,
				  sizeof(origin->request)) == 0
		&& memcmp(&origin->request_v3, &owned->barrier_request_v3,
				  sizeof(origin->request_v3)) == 0
		&& memcmp(origin->peer_boots, owned->barrier_boots, sizeof(origin->peer_boots)) == 0) {
		/* Keep the exact active operation for the original commit-window
		 * producer. The same preallocated carrier survives callback/exit
		 * failure without granting deletion or releasing the protecting AX. */
		origin->committed_drain_pending = true;
		work->context = *origin;
		work->buffers_released = work->predelete_ready = work->recovery_required = false;
		work->next = ko_state->native_incoming;
		ko_state->native_incoming = owned->native_allocation;
		ko_state->native_waiting++;
		owned->native_unready = owned->native_allocation;
		owned->native_allocation = InvalidDsaPointer;
		owned->native_reserved = NULL;
		owned->native_handed_off = published = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return published;
}

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
		|| !owned->native_transaction || owned->native_pending
		|| (owned->barrier_committed_discard && !owned->barrier_v3)
		|| owned->native_handed_off || !owned->postcommit || owned->serial == 0
		|| owned->native_reserved == NULL || !DsaPointerIsValid(owned->native_allocation))
		return false;
	if (!owned->barrier_private)
		return from_commit_tail && ko_native_publish_registration(owned);
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
	work->context.pid = owned->pid;
	work->context.serial = owned->serial;
	work->context.drop_identity = owned->drop_identity;
	work->context.drop_identity_serial = owned->drop_identity_serial;
	work->context.v3 = owned->barrier_v3;
	work->context.committed_discard = owned->barrier_committed_discard;
	work->context.origin_checkpoint = work->context.origin_data_closed = owned->origin_data_closed;
	work->context.request = owned->barrier_v3 ? owned->barrier_request_v3.base
											: owned->barrier_request;
	if (owned->barrier_v3) {
		work->context.request_v3 = owned->barrier_request_v3;
		work->context.request_sent_peers = work->context.request_done_peers
			= owned->barrier_ready_peers;
	}
	memcpy(work->context.peer_boots, owned->barrier_boots, sizeof(owned->barrier_boots));
	work->context.terminal = owned->terminal;
	memcpy(work->context.structure, owned->structure, sizeof(owned->structure));
	qualified = qualified && (!owned->barrier_v3 || (ko_shared_ready_v3(&work->context)
		&& (owned->barrier_request_v3.operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP
			|| owned->origin_data_closed)));
	work->context.structure_owned = qualified;
	work->recovery_required = !qualified;
	work->buffers_released = qualified
							 && (change.identity.action == CLUSTER_SPACE_WAL_TRUNCATE
								 || owned->drop_observed || owned->native_buffers_released);
	work->context.structure_drop_pending = qualified
										   && change.identity.action == CLUSTER_SPACE_WAL_TOMBSTONE
										   && !owned->drop_observed;
	if (!work->buffers_released)
		owned->native_unready = owned->native_allocation;
	SpinLockAcquire(&ko_state->shared_lock);
	if (work->context.v3
		&& work->context.request_v3.operation == CLUSTER_KO_SHARED_V3_OPERATION_DROP) {
		ClusterKoSharedContext *origin = owned->slot < CLUSTER_KO_SHARED_CAPACITY
			? &ko_state->contexts[owned->slot] : NULL;
		KoDropIdentity *identity = ko_origin_drop_identity_locked(&work->context);

		if (origin == NULL || !origin->used || origin->serial != owned->serial
			|| !origin->v3 || origin->committed_discard != owned->barrier_committed_discard
			|| origin->pid != owned->pid || origin->drop_identity != owned->drop_identity
			|| origin->drop_identity_serial != owned->drop_identity_serial
			|| identity == NULL || identity->state != KO_DROP_IDENTITY_RESERVED) {
			work->recovery_required = true;
			work->context.structure_owned = false;
		} else {
			/* Native work, including an incomplete observation, now owns this
			 * reservation. The original AX result cannot recycle it. */
			origin->drop_identity = 0;
			origin->drop_identity_serial = 0;
			/* Preserve the exact carrier across COMMIT-to-publication. This
			 * borrow creates no work and is never followed after I/O closing. */
			if (qualified)
				origin->pi_native_owner = owned->native_allocation;
		}
	}
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
	if (!cluster_enabled || !cluster_shared_config)
		return;
	for (ClusterKoCompletionV2 *owned = ko_completions; owned != NULL; owned = owned->next) {
		if (!owned->native_transaction || owned->native_pending || owned->native_handed_off
			|| (!ko_native_capable(owned->barrier_v3, owned->barrier_committed_discard)
				&& !(owned->barrier_v3 && !owned->barrier_private)))
			continue;
		/* The original claimed identity and allocation must survive this
		 * irreversible point even when its physical drain has not run. */
		owned->postcommit = true;
		if (!ko_native_publish(owned, true))
			elog(PANIC, "committed shared structural change lost its original completion owner");
	}
}

/* A registered operation reaches publication before its committed drain.
 * Keep that original phase separate from the completed SPACE consumer below.
 * The native allocation is still producer-owned; all mutable comparisons and
 * the milestone publication share the original KO lock. No live proof or I/O
 * can repair or refresh a committed operation here. */
static void
ko_native_registration_published(ClusterKoCompletionV2 *owned,
								ClusterKoNativeContinuation *work,
								const ClusterKoSharedContext *before)
{
	ClusterKoSharedContext *entry = NULL;
	bool qualified;

	SpinLockAcquire(&ko_state->shared_lock);
	if (owned->slot < CLUSTER_KO_SHARED_CAPACITY)
		entry = &ko_state->contexts[owned->slot];
	qualified = !owned->barrier_private && !owned->space_observed
		&& !owned->truncate_observed && !owned->drop_observed && !owned->origin_data_closed
		&& owned->barrier_ready_peers == 0 && !work->recovery_required
		&& !work->buffers_released && !work->predelete_ready
		&& before->used && before->v3 && before->committed_drain_pending
		&& before->committed_discard == owned->barrier_committed_discard
		&& !before->complete && !before->origin_detached && !before->ax_release_requested
		&& !before->ax_failed && !before->structure_owned && before->cancel_sqlerrcode == 0
		&& before->pid == owned->pid && before->serial == owned->serial
		&& (before->request_sent_peers | before->request_sending_peers
			| before->request_uncertain_peers | before->request_done_peers
			| before->request_failed_peers) == 0
		&& memcmp(&before->request, &owned->barrier_request_v3.base,
				  sizeof(before->request)) == 0
		&& memcmp(&before->request_v3, &owned->barrier_request_v3,
				  sizeof(before->request_v3)) == 0
		&& memcmp(before->peer_boots, owned->barrier_boots, sizeof(before->peer_boots)) == 0
		&& memcmp(before, &work->context, sizeof(*before)) == 0
		&& entry != NULL && memcmp(before, entry, sizeof(*before)) == 0;
	if (work->context.pid == owned->pid && work->context.serial == owned->serial
		&& !work->buffers_released) {
		if (qualified)
			work->publication_observed = true;
		else {
			work->recovery_required = true;
			ko_state->native_recovery_required = true;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
}

/* Only the successful native AtEOXact_Inval COMMIT boundary calls this,
 * after the original publication ACKs and before native lock release.
 * Completed barriers retain their original SPACE-before-close contract;
 * committed registrations retain publication separately until their drain.
 * No self-peer message or local SI cache callback may manufacture this fact.
 * Never wait for IO here. */
void
cluster_ko_shared_native_publication_v3(void)
{
	if (ko_state == NULL || !cluster_enabled || !cluster_shared_config)
		return;
	for (ClusterKoCompletionV2 *owned = ko_completions; owned != NULL; owned = owned->next) {
		ClusterKoNativeContinuation *work;
		ClusterKoSharedContext before;
		ClusterWalSourceRef writer;
		bool qualified;

		if (!owned->barrier_v3
			|| (owned->barrier_request_v3.operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP
				&& (owned->barrier_private || owned->barrier_request_v3.operation
					!= CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD))
			|| !owned->native_transaction)
			continue;
		if (!owned->native_handed_off || !owned->postcommit || owned->native_pending
			|| owned->pid != MyProcPid || owned->owner != TopTransactionResourceOwner
			|| CurrentResourceOwner == NULL || TopTransactionResourceOwner == NULL
			|| CurTransactionResourceOwner != TopTransactionResourceOwner || CritSectionCount != 0
			|| (MyBackendType != B_BACKEND && MyBackendType != B_AUTOVAC_WORKER
				&& MyBackendType != B_BG_WORKER)
			|| !DsaPointerIsValid(owned->native_unready))
			continue;
		/* The original buffer-release handoff has not surrendered this DSA
		 * pointer yet. No mapping/CONTROL work is performed under KO lock. */
		work = dsa_get_address(ko_native_area, owned->native_unready);
		SpinLockAcquire(&ko_state->shared_lock);
		before = work->context;
		qualified = !work->recovery_required && !work->buffers_released;
		SpinLockRelease(&ko_state->shared_lock);
		if (before.committed_drain_pending) {
			ko_native_registration_published(owned, work, &before);
			continue;
		}
		if (owned->barrier_request_v3.operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP)
			continue;
		qualified = qualified && owned->space_observed && before.used && before.complete
			&& before.v3 && before.structure_owned && before.structure_drop_pending
			&& before.committed_discard == owned->barrier_committed_discard
			&& before.pid == owned->pid && before.serial == owned->serial
			&& before.drop_identity == owned->drop_identity
			&& before.drop_identity_serial == owned->drop_identity_serial
			&& memcmp(&before.request_v3, &owned->barrier_request_v3,
					  sizeof(before.request_v3)) == 0
			&& memcmp(before.peer_boots, owned->barrier_boots, sizeof(before.peer_boots)) == 0
			&& memcmp(&before.terminal, &owned->terminal, sizeof(before.terminal)) == 0
			&& memcmp(before.structure, owned->structure, sizeof(before.structure)) == 0
			&& ko_shared_ready_v3(&before) && ko_shared_origin_current(&before)
			&& cluster_wal_thread_current_v2_ref(&writer)
			&& memcmp(&writer, &before.terminal.source, sizeof(writer)) == 0;
		SpinLockAcquire(&ko_state->shared_lock);
		if (work->context.pid == owned->pid && work->context.serial == owned->serial
			&& !work->buffers_released) {
			KoDropIdentity *identity = ko_origin_drop_identity_locked(&before);
			uint64 revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);

			if (qualified && memcmp(&before, &work->context, sizeof(before)) == 0
				&& identity != NULL && revision != 0 && revision < UINT64_MAX - 1) {
				if (identity->state == KO_DROP_IDENTITY_RESERVED) {
					identity->state = KO_DROP_IDENTITY_CLOSING;
					pg_atomic_write_u64(&ko_state->peer_admission_revision, revision + 1);
				}
				if (!ko_directory_io_pending_locked(before.request.key.locator))
					identity->state = KO_DROP_IDENTITY_RETIRED;
			} else {
				/* A completed publication does not repair an incomplete/stale
				 * local carrier. Preserve it for the original recovery owner. */
				work->recovery_required = true;
				ko_state->native_recovery_required = true;
			}
		}
		SpinLockRelease(&ko_state->shared_lock);
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
			&& work->predelete_ready
			&& work->context.committed_discard == owned->barrier_committed_discard
			&& memcmp(&work->context.terminal, &owned->terminal, sizeof(owned->terminal)) == 0
			&& memcmp(work->context.structure, owned->structure, sizeof(owned->structure)) == 0) {
			owned->native_unready = InvalidDsaPointer;
			work->buffers_released = true;
		}
		SpinLockRelease(&ko_state->shared_lock);
	}
}

/* Called immediately before the native smgr path enters
 * DropRelationsAllBuffers. This is a handoff, not an invalidation result:
 * the committed DATA/SPACE proof is retained in the continuation and the
 * exact origin identity is still fenced. Entered directory calls remain a
 * separate checkpointer predicate; the following buffer routine must set
 * buffers_released independently, and a callback without this phase is
 * ignored.
 */
bool
cluster_ko_shared_native_prepare_drop_v2(RelFileLocator locator)
{
	if (ko_state == NULL || TopTransactionResourceOwner == NULL
		|| CurTransactionResourceOwner != TopTransactionResourceOwner || CritSectionCount != 0)
		return false;
	for (ClusterKoCompletionV2 *owned = ko_completions; owned != NULL; owned = owned->next) {
		ClusterKoNativeContinuation *work;
		ClusterKoSharedContext before;
		ClusterSpaceStructureChange change;
		bool recovery_required;
		bool buffers_released;

		if (owned->pid != MyProcPid || owned->owner != TopTransactionResourceOwner
			|| !owned->native_transaction || !owned->native_handed_off || !owned->postcommit
			|| !owned->space_observed || !DsaPointerIsValid(owned->native_unready)
			|| !RelFileLocatorEquals(owned->terminal.identity.locator, locator)
			|| !cluster_space_structure_wal_decode(owned->structure, sizeof(owned->structure),
												   &change)
			|| change.identity.action != CLUSTER_SPACE_WAL_TOMBSTONE)
			continue;
		work = dsa_get_address(ko_native_area, owned->native_unready);
		SpinLockAcquire(&ko_state->shared_lock);
		before = work->context;
		recovery_required = work->recovery_required;
		buffers_released = work->buffers_released;
		SpinLockRelease(&ko_state->shared_lock);
		if (before.pid != owned->pid || before.serial != owned->serial
			|| before.committed_discard != owned->barrier_committed_discard
			|| before.drop_identity != owned->drop_identity
			|| before.drop_identity_serial != owned->drop_identity_serial
			|| !before.used || !before.complete || !before.v3 || !before.origin_data_closed
			|| !before.structure_owned || !before.structure_drop_pending
			|| recovery_required || buffers_released
			|| !ko_shared_origin_current(&before))
			continue;
		SpinLockAcquire(&ko_state->shared_lock);
		if (work->context.pid == before.pid && work->context.serial == before.serial
			&& memcmp(&work->context, &before, sizeof(before)) == 0
			&& !work->recovery_required && !work->buffers_released) {
			work->predelete_ready = true;
			SpinLockRelease(&ko_state->shared_lock);
			return true;
		}
		SpinLockRelease(&ko_state->shared_lock);
	}
	return false;
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
		|| !ko_native_capable(context->v3, context->committed_discard))
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
	bool failed, buffers_released, capable;

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
	capable = ko_native_capable(before.v3, before.committed_discard);
	if (capable && (failed || (!before.committed_drain_pending
		&& (!ko_shared_origin_current(&before)
			|| !cluster_wal_thread_current_v2_ref(&current)
			|| memcmp(&current, &before.terminal.source, sizeof(current)) != 0)))) {
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
	if (!capable || before.committed_drain_pending || !buffers_released
		|| !ko_origin_drop_io_closed(&before)) {
		/* Let the rest of this finite batch advance while the original
		 * mode is unavailable, the producer completes buffer cleanup or
		 * publication, or an entered directory call exits. No storage runs. */
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
		|| (!ko_native_capable(false, false) && !ko_native_capable(true, false)
			&& !ko_native_capable(true, true)))
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
		   && ko_native_capable(context->v3, context->committed_discard)
		   && (!context->v3 || ko_shared_ready_v3(context))
		   && context->structure_drop_pending && !context->structure_drop_failed
		   && context->serial != 0 && context->request.origin_node == cluster_node_id
		   && cluster_space_structure_wal_decode(context->structure, sizeof(context->structure),
												 &change)
		   && change.identity.action == CLUSTER_SPACE_WAL_TOMBSTONE
		   && cluster_page_wal_binding_shape_v1(&context->terminal)
		   && context->terminal.flags == CLUSTER_PAGE_WAL_NATIVE_FLUSHED
		   && ko_shared_origin_current(context) && cluster_wal_thread_current_v2_ref(&current)
		   && memcmp(&current, &context->terminal.source, sizeof(current)) == 0
		   && ko_origin_drop_io_closed(context);
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

static bool
ko_drop_work_context_current(const ClusterKoDropWorkV2 *work)
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

static bool
ko_drop_work_local_sample_current_locked(const ClusterKoDropWorkV2 *work)
{
	ResourceXGateSnapshot gate;

	if (work->local_scan_revision == 0 || work->local_scan_revision == UINT64_MAX
		|| !cluster_pcm_lock_resource_x_gate_snapshot(&gate)
		|| gate.phase != RESOURCE_X_GATE_OPEN
		|| memcmp(&gate, &work->local_scan_gate, sizeof(gate)) != 0)
		return false;
	return pg_atomic_read_u64(&ko_state->peer_admission_revision) == work->local_scan_revision
		&& !ko_cleaner_pending_locked(work->context.request.key.locator)
		&& !ko_extend_pending_locked(work->context.request.key.locator)
		&& (!work->local_scan_complete
			|| cluster_pi_writeback_local_revision_current_v3(work->local_writeback_revision));
}

static bool
ko_drop_work_local_sample_current(const ClusterKoDropWorkV2 *work)
{
	bool current;

	/* These existing gate/revision readers are bounded atomic observations;
	 * no PCM, CF, writeback lock or I/O is taken under the KO lock. */
	SpinLockAcquire(&ko_state->shared_lock);
	current = ko_drop_work_local_sample_current_locked(work);
	SpinLockRelease(&ko_state->shared_lock);
	return current;
}

/* The native producer already persisted the final SPACE and retired its
 * buffers. That alone does not retire local PI/protocol or ACK-send owners.
 * Resume their existing local-only census without waiting for remote master
 * directory completion. No KO lock spans PCM, and ERROR consumes the hint. */
static void
ko_drop_work_local_scan(ClusterKoDropWorkV2 *work)
{
	ResourceXGateSnapshot gate, after;
	ResourceXGateSnapshot previous_gate = work->local_scan_gate;
	ClusterPcmPiRelationScanV2 scan;
	uint64 revision, writeback_revision = 0;
	uint64 previous_revision = work->local_scan_revision;
	uint32 start = 0, cursor;
	bool same;

	if (!work->context.v3)
		return;
	same = ko_drop_work_local_sample_current(work);
	if (same && work->local_scan_complete)
		return;
	if (same)
		start = work->local_scan_cursor;
	work->local_scan_complete = false;
	work->local_scan_cursor = 0;
	work->local_scan_revision = 0;
	work->local_writeback_revision = 0;
	memset(&work->local_scan_gate, 0, sizeof(work->local_scan_gate));
	revision = pg_atomic_read_u64(&ko_state->peer_admission_revision);
	if (revision == 0 || revision == UINT64_MAX
		|| !cluster_pcm_lock_resource_x_gate_snapshot(&gate)
		|| gate.phase != RESOURCE_X_GATE_OPEN)
		return;
	/* The cut may change between validating the old prefix and sampling
	 * this attempt. Never relabel that prefix with the replacement cut. */
	if (revision != previous_revision || memcmp(&gate, &previous_gate, sizeof(gate)) != 0)
		start = 0;
	cursor = start;
	/* PCM consumes its Resource-X formation; the writeback transport below
	 * still consumes the independently authenticated membership epoch. */
	scan = cluster_pcm_lock_ko_local_relation_scan_v3(work->context.request.key.locator,
													  gate.formation, 128, &cursor);
	if ((scan != CLUSTER_PCM_PI_RELATION_MORE && scan != CLUSTER_PCM_PI_RELATION_EMPTY)
		|| cursor <= start || cursor - start > 128)
		return;
	if (scan == CLUSTER_PCM_PI_RELATION_EMPTY
		&& !cluster_pi_writeback_local_relation_closed_v3(work->context.request.key.locator,
															 work->context.request.epoch,
															 &writeback_revision))
		return;
	if (!ko_drop_work_context_current(work)
		|| !cluster_pcm_lock_resource_x_gate_snapshot(&after)
		|| memcmp(&gate, &after, sizeof(gate)) != 0)
		return;
	SpinLockAcquire(&ko_state->shared_lock);
	if (pg_atomic_read_u64(&ko_state->peer_admission_revision) == revision
		&& !ko_cleaner_pending_locked(work->context.request.key.locator)
		&& !ko_extend_pending_locked(work->context.request.key.locator)
		&& (scan != CLUSTER_PCM_PI_RELATION_EMPTY
			|| cluster_pi_writeback_local_revision_current_v3(writeback_revision))) {
		work->local_scan_cursor = cursor;
		work->local_scan_revision = revision;
		work->local_scan_gate = gate;
		work->local_writeback_revision = writeback_revision;
		work->local_scan_complete = scan == CLUSTER_PCM_PI_RELATION_EMPTY;
	}
	SpinLockRelease(&ko_state->shared_lock);
}

bool
cluster_ko_shared_drop_work_revalidate_v2(const ClusterKoDropWorkV2 *work)
{
	return ko_drop_work_context_current(work)
		&& (!work->context.v3
			|| (work->local_scan_complete && ko_drop_work_local_sample_current(work)));
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
				|| !ko_drop_work_context_current(work)) {
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
		/* One selected work and at most 128 slots per original poll. Pending
		 * local closure advances the outer cursor, but state/read/revalidate
		 * still refuse all provider effects. A later tick resumes this owner. */
		ko_drop_work_local_scan(work);
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
	bool retired_drop;
	bool finished = false;

	if (work == NULL || !cluster_ko_shared_drop_work_revalidate_v2(*work))
		return false;
	owned = *work;
	retired_drop = owned->context.v3
		&& owned->context.request_v3.operation == CLUSTER_KO_SHARED_V3_OPERATION_DROP;
	SpinLockAcquire(&ko_state->shared_lock);
	if (retired_drop) {
		KoDropIdentity *identity = ko_origin_drop_identity_locked(&owned->context);

		/* Consume the same local proof with the exact context. The physical
		 * caller has synced the namespace and closed every original FD. A
		 * changed proof retains that durable work for its original next tick. */
		if (identity == NULL || identity->state != KO_DROP_IDENTITY_RETIRED
			|| ko_directory_io_pending_locked(owned->context.request.key.locator)
			|| !owned->local_scan_complete || !ko_drop_work_local_sample_current_locked(owned))
			goto done;
	}
	if (memcmp(&ko_state->contexts[owned->slot], &owned->context, sizeof(owned->context)) == 0) {
		if (retired_drop) {
			/* Only this native file responsibility is terminal. Keep the
			 * immutable negative identity, separate AX owner and master debt;
			 * do not manufacture legacy structural acceptance or a receipt. */
			memset(&ko_state->contexts[owned->slot], 0, sizeof(owned->context));
		} else {
			ko_state->contexts[owned->slot].structure_drop_pending = false;
			ko_state->contexts[owned->slot].drop_executor_pid = 0;
		}
		ko_drop_work[owned->slot] = NULL;
		finished = true;
	}
done:
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
		|| (context.v3 && !ko_shared_ready_v3(&context))
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

/* Only the original native wrapper offers an operation for SPACE to take.
 * Keep the ResourceOwner throughout; taking it does not complete the DDL or
 * convey COMMIT/durability. Ambiguous repeated barriers are not authority. */
static bool
ko_shared_claim(const ClusterSpaceIdentityKey *key, const uint8 incarnation[16],
				const ClusterSpaceIdentity *identity, ClusterSpaceWalAction action,
				ClusterKoCompletionV2 **out)
{
	ClusterKoCompletionV2 *candidate = NULL;
	if (out == NULL || *out != NULL || key == NULL || incarnation == NULL)
		return false;
	for (ClusterKoCompletionV2 *completion = ko_completions; completion != NULL;
		 completion = completion->next) {
		ClusterKoSharedContext registered;

		if (!completion->native_pending)
			continue;
		if (!cluster_ko_shared_covers_v2(completion, key, incarnation)
			&& (identity == NULL
				|| !ko_shared_registration_snapshot_v3(completion, &registered)
				|| !ko_shared_context_covers(&registered, key, incarnation)))
			continue;
		if (candidate != NULL)
			return false;
		candidate = completion;
	}
	if (candidate == NULL)
		return false;
	if (identity != NULL) {
		if (!ko_shared_structure_current(candidate, identity, action)) {
			ClusterKoSharedContext registered;

			/* This claim prepares the existing TOMBSTONE record only. The
			 * completed-operation consumers remain separate and unchanged. */
			if (action != CLUSTER_SPACE_WAL_TOMBSTONE
				|| !ko_shared_registration_snapshot_v3(candidate, &registered)
				|| !ko_shared_v3_space_matches(&registered.request_v3, identity)
				|| (registered.request_v3.operation != CLUSTER_KO_SHARED_V3_OPERATION_DROP
					&& registered.request_v3.operation != CLUSTER_KO_SHARED_V3_OPERATION_REWRITE_OLD))
				return false;
		}
	} else if (candidate->barrier_v3 || !cluster_ko_shared_covers_v2(candidate, key, incarnation))
		return false;
	candidate->native_pending = false;
	*out = candidate;
	return true;
}

bool
cluster_ko_shared_claim_v2(const ClusterSpaceIdentityKey *key, const uint8 incarnation[16],
						   ClusterKoCompletionV2 **out)
{
	return ko_shared_claim(key, incarnation, NULL, 0, out);
}

bool
cluster_ko_shared_claim_structure(const ClusterSpaceIdentity *identity,
								  ClusterSpaceWalAction action, ClusterKoCompletionV2 **out)
{
	return identity != NULL
		   && ko_shared_claim(&identity->key, identity->incarnation, identity, action, out);
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

/* Caller holds shared_lock. Result polarity and the complete original
 * operation must agree; a queued frame cannot create a peer owner. */
static bool
ko_peer_reply_matches_locked(const ClusterKoPeerFenceV3 *entry,
							const ClusterKoSharedMessageV3 *message, uint64 local_generation,
							 bool committed_discard)
{
	if (entry->committed_discard != committed_discard || entry->serial == 0
		|| entry->peer_route_generation != local_generation
		|| !cluster_ko_shared_result_matches_v3(&entry->request, message))
		return false;
	if (message->base.status == CLUSTER_KO_SHARED_FAILED)
		return (entry->state == CLUSTER_KO_PEER_FENCE_ACTIVE
				|| entry->state == CLUSTER_KO_PEER_FENCE_READY)
			   && entry->failure_sqlerrcode != 0;
	return (entry->state == CLUSTER_KO_PEER_FENCE_READY
			|| entry->state == CLUSTER_KO_PEER_FENCE_COMMITTED)
		   && entry->physical_applied && entry->failure_sqlerrcode == 0;
}

static bool
ko_shared_enqueue_object(const ClusterKoSharedMessageV3 *message, uint64 local_generation,
							 bool committed_discard)
{
	bool accepted = false;
	uint8 bytes[CLUSTER_KO_SHARED_V3_BYTES];
	ClusterKoPeerFenceV3 *reply_owner = NULL;

	if (ko_state == NULL || local_generation == 0 || local_generation == UINT64_MAX
		|| !ko_shared_object_encode(message, committed_discard, bytes, sizeof(bytes)))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	if (message->base.verb == CLUSTER_KO_SHARED_ACK) {
		for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
			ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];

			if (entry->reply_state == KO_PEER_REPLY_NONE && !entry->ack_queued
				&& !entry->failure_queued
				&& ko_peer_reply_matches_locked(entry, message, local_generation, committed_discard)) {
				reply_owner = entry;
				break;
			}
		}
	}
	if (ko_state->send_v3_count < CLUSTER_KO_SHARED_CAPACITY
		&& (message->base.verb == CLUSTER_KO_SHARED_REQUEST || reply_owner != NULL)) {
		uint32 tail
			= (ko_state->send_v3_head + ko_state->send_v3_count) % CLUSTER_KO_SHARED_CAPACITY;
		ko_state->send_v3[tail] = *message;
		ko_state->send_v3_route_generation[tail] = local_generation;
		ko_state->send_committed_discard[tail] = committed_discard;
		ko_state->send_v3_count++;
		/* Publish ownership with the queue slot, before LMON can consume it. */
		if (reply_owner != NULL) {
			reply_owner->ack_queued = message->base.status == CLUSTER_KO_SHARED_DONE;
			reply_owner->failure_queued = message->base.status == CLUSTER_KO_SHARED_FAILED;
			reply_owner->reply_state = KO_PEER_REPLY_QUEUED;
		}
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
	return ko_shared_enqueue_object(message, local_generation, false);
}

static bool
ko_peer_reply_send_begin(const ClusterKoSharedMessageV3 *message, uint64 local_generation,
						 bool committed_discard,
						ClusterKoPeerFenceHandleV3 *handle)
{
	bool owned = false;

	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];

		if (entry->reply_state != KO_PEER_REPLY_QUEUED
			|| !(message->base.status == CLUSTER_KO_SHARED_DONE ? entry->ack_queued
																 : entry->failure_queued)
			|| !ko_peer_reply_matches_locked(entry, message, local_generation, committed_discard))
			continue;
		handle->slot = i;
		handle->serial = entry->serial;
		entry->reply_state = KO_PEER_REPLY_SENDING;
		owned = true;
		break;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return owned;
}

static void
ko_peer_reply_send_finish(const ClusterKoSharedMessageV3 *message, uint64 local_generation,
						 bool committed_discard,
						 const ClusterKoPeerFenceHandleV3 *handle, KoPeerReplyState state)
{
	ClusterKoPeerFenceV3 *entry;

	SpinLockAcquire(&ko_state->shared_lock);
	entry = &ko_state->peer_fences[handle->slot];
	if (entry->serial == handle->serial && entry->reply_state == KO_PEER_REPLY_SENDING
		&& ko_peer_reply_matches_locked(entry, message, local_generation, committed_discard))
		entry->reply_state = state;
	SpinLockRelease(&ko_state->shared_lock);
}

static ClusterICSendResult
ko_peer_reply_send(const ClusterKoSharedMessageV3 *message, uint64 local_generation,
							 bool committed_discard)
{
	ClusterKoPeerFenceHandleV3 handle;
	volatile ClusterICSendResult result = CLUSTER_IC_SEND_HARD_ERROR;
	uint8 bytes[CLUSTER_KO_SHARED_V3_BYTES];

	if (!ko_peer_reply_send_begin(message, local_generation, committed_discard, &handle))
		return result;
	PG_TRY();
	{
		if (ko_shared_object_control_at_generation(message, local_generation, committed_discard)
			&& ko_shared_object_encode(message, committed_discard, bytes, sizeof(bytes))) {
			result = cluster_ic_send_envelope(PGRAC_IC_MSG_KO_FLUSH_ACK,
											  message->base.origin_node, bytes, sizeof(bytes));
			ko_peer_reply_send_finish(message, local_generation, committed_discard, &handle,
				result == CLUSTER_IC_SEND_NOT_ADMITTED ? KO_PEER_REPLY_QUEUED
				: (result == CLUSTER_IC_SEND_DONE || result == CLUSTER_IC_SEND_WOULD_BLOCK)
					? KO_PEER_REPLY_TRANSPORT_OWNED : KO_PEER_REPLY_UNKNOWN);
		} else
			ko_peer_reply_send_finish(message, local_generation, committed_discard, &handle,
									 KO_PEER_REPLY_REJECTED);
	}
	PG_CATCH();
	{
		/* The original slot survives ERROR, but an uncertain send is not
		 * retried or reclaimed as a passive fence by a later owner tick. */
		ko_peer_reply_send_finish(message, local_generation, committed_discard, &handle,
								 KO_PEER_REPLY_UNKNOWN);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return result;
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
ko_shared_object_request_claim(const ClusterKoSharedMessageV3 *message, bool committed_discard)
{
	ClusterKoSharedMessageV3 ack = *message;
	bool owned = false;

	if (message->base.verb != CLUSTER_KO_SHARED_REQUEST)
		return true;
	ack.base.verb = CLUSTER_KO_SHARED_ACK;
	ack.base.status = CLUSTER_KO_SHARED_DONE;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoSharedContext *entry = &ko_state->contexts[i];
		ClusterKoSharedMessageV3 expected;
		if (!entry->used || !entry->v3 || entry->committed_discard != committed_discard
			|| entry->complete || entry->origin_detached
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
		if (owned) {
			entry->request_uncertain_peers |= (uint16)(1u << message->base.peer_node);
			entry->request_sending_peers |= (uint16)(1u << message->base.peer_node);
		}
		break;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return owned;
}

static bool
ko_shared_v3_request_claim(const ClusterKoSharedMessageV3 *message)
{
	return ko_shared_object_request_claim(message, false);
}

/* DONE and WOULD_BLOCK transfer the exact frame to transport. An ambiguous
 * hard error must not retry it on another carrier. Neither result is READY. */
typedef enum KoSendOwnership {
	KO_SEND_CALLER_OWNED,
	KO_SEND_TRANSPORT_OWNED,
	KO_SEND_UNKNOWN
} KoSendOwnership;

static void
ko_shared_object_record_ownership(const ClusterKoSharedMessageV3 *message, KoSendOwnership ownership,
								 bool committed_discard)
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

		if (!entry->used || !entry->v3 || entry->committed_discard != committed_discard
			|| entry->request_v3.base.batch_id != message->base.batch_id)
			continue;
		expected = entry->request_v3;
		expected.base.peer_node = message->base.peer_node;
		expected.base.peer_boot = entry->peer_boots[message->base.peer_node];
		if (cluster_ko_shared_ack_matches_v3(&expected, &ack)) {
			uint16 peer = (uint16)(1u << message->base.peer_node);
			entry->request_sending_peers &= ~peer;
			if (ownership == KO_SEND_TRANSPORT_OWNED) {
				entry->request_sent_peers |= peer;
				entry->request_uncertain_peers &= ~peer;
			} else if (ownership == KO_SEND_CALLER_OWNED)
				entry->request_uncertain_peers &= ~peer;
			else if (((entry->request_done_peers | entry->request_failed_peers) & peer) == 0)
				entry->request_uncertain_peers |= peer;
		}
		break;
	}
	SpinLockRelease(&ko_state->shared_lock);
}

static void
ko_shared_v3_record_ownership(const ClusterKoSharedMessageV3 *message, KoSendOwnership ownership)
{
	ko_shared_object_record_ownership(message, ownership, false);
}

static void
ko_shared_backend_exit(int code, Datum arg)
{
	uint64 batches[CLUSTER_KO_SHARED_CAPACITY];
	unsigned count = 0;

	if (ko_state == NULL)
		return;
	cluster_ko_extend_abort_v3(ko_extend_local.serial);
	cluster_ko_create_abort_v3(ko_create_local.serial);
	SpinLockAcquire(&ko_state->shared_lock);
	if (ko_create_local.used && ko_create_local.pid == MyProcPid
		&& ko_create_slot < CLUSTER_KO_SHARED_CAPACITY
		&& memcmp(&ko_state->extend_work[ko_create_slot], &ko_create_local,
				  sizeof(ko_create_local)) == 0)
		ko_state->extend_work[ko_create_slot].failed = true;
	if (ko_extend_local.used && ko_extend_local.pid == MyProcPid
		&& ko_extend_slot < CLUSTER_KO_SHARED_CAPACITY
		&& memcmp(&ko_state->extend_work[ko_extend_slot], &ko_extend_local, sizeof(ko_extend_local))
			   == 0)
		ko_state->extend_work[ko_extend_slot].failed = true;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (ko_state->contexts[i].used && !ko_state->contexts[i].structure_owned
			&& ko_state->contexts[i].pid == MyProcPid) {
			if (!ko_state->contexts[i].complete)
				batches[count++] = ko_state->contexts[i].request.batch_id;
			if (ko_state->contexts[i].v3) {
				ko_state->contexts[i].origin_detached = true;
				/* Backend exit is a native release intent for all its holds. */
				ko_state->contexts[i].ax_release_requested = true;
			} else
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

/* Origin cancellation remains in the original operation table. */
static bool
ko_origin_ax_equal(const ClusterKoSharedMessageV3 *a, const ClusterKoSharedMessageV3 *b)
{
	return memcmp(&a->ax_resid, &b->ax_resid, sizeof(a->ax_resid)) == 0
		   && memcmp(&a->ax_holder, &b->ax_holder, sizeof(a->ax_holder)) == 0
		   && a->ax_request_id == b->ax_request_id && a->ax_mode == b->ax_mode
		   && a->ax_master_node == b->ax_master_node
		   && a->ax_master_generation == b->ax_master_generation;
}

/* A surviving native weaker hold can keep the exact stronger AX while the
 * canceled operation drains. This is not a downgrade or a release intent. */
bool
cluster_ko_ax_retain_native_v3(const LOCKTAG *tag, const ClusterGrdHolderId *holder)
{
	ClusterResId resid;
	bool retained = false;
	if (ko_state == NULL || tag == NULL || holder == NULL || tag->locktag_type != LOCKTAG_RELATION)
		return false;
	cluster_grd_resid_encode(tag, &resid);
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoSharedContext *entry = &ko_state->contexts[i];
		if (entry->used && entry->v3 && !entry->structure_owned
			&& memcmp(&resid, &entry->request_v3.ax_resid, sizeof(resid)) == 0
			&& memcmp(holder, &entry->request_v3.ax_holder, sizeof(*holder)) == 0) {
			entry->origin_detached = true;
			retained = true;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
	return retained;
}

bool
cluster_ko_ax_release_handoff_v3(const LOCKTAG *tag, LOCKMODE mode,
								 const ClusterGrdHolderId *holder)
{
	ClusterResId resid;
	bool transferred = false;

	if (ko_state == NULL || tag == NULL || holder == NULL || tag->locktag_type != LOCKTAG_RELATION
		|| mode != AccessExclusiveLock)
		return false;
	cluster_grd_resid_encode(tag, &resid);
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoSharedContext *entry = &ko_state->contexts[i];
		if (!entry->used || !entry->v3 || entry->structure_owned
			|| memcmp(&resid, &entry->request_v3.ax_resid, sizeof(resid)) != 0
			|| memcmp(holder, &entry->request_v3.ax_holder, sizeof(*holder)) != 0)
			continue;
		entry->origin_detached = true;
		entry->ax_release_requested = true;
		transferred = true;
	}
	SpinLockRelease(&ko_state->shared_lock);
	return transferred;
}

bool
cluster_ko_ax_acquire_pending_v3(const ClusterResId *resid, uint32 procno, uint32 group)
{
	bool pending = false;

	if (ko_state == NULL || resid == NULL || resid->type != LOCKTAG_RELATION)
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoSharedContext *entry = &ko_state->contexts[i];
		if (entry->used && entry->v3 && !entry->structure_owned
			&& (entry->origin_detached || entry->ax_release_requested)
			&& entry->request_v3.ax_holder.node_id == (uint32)cluster_node_id
			&& memcmp(resid, &entry->request_v3.ax_resid, sizeof(*resid)) == 0
			&& (entry->request_v3.ax_holder.procno == procno
				|| (uint64)(group ? group : (uint64)procno + 1)
					   == (uint64)(entry->ax_lock_group
									   ? entry->ax_lock_group
									   : (uint64)entry->request_v3.ax_holder.procno + 1))) {
			pending = true;
			break;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
	return pending;
}

bool
cluster_ko_ax_native_wait_v3(const LOCKTAG *tag, bool dontwait)
{
	ClusterResId resid;
	ClusterGrdHolderId holder = { 0 };

	if (ko_state == NULL || tag == NULL || tag->locktag_type != LOCKTAG_RELATION || MyProc == NULL)
		return true;
	cluster_grd_resid_encode(tag, &resid);
	holder.node_id = cluster_node_id;
	holder.procno = MyProc->pgprocno;
	while (cluster_ko_ax_acquire_pending_v3(&resid, holder.procno,
											cluster_ges_current_lock_group(&holder))) {
		if (dontwait)
			return false;
		CHECK_FOR_INTERRUPTS();
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 100,
						WAIT_EVENT_CLUSTER_OBJECT_FLUSH_WAIT);
		ResetLatch(MyLatch);
	}
	return true;
}

/* Snapshot outside the GRD entry lock. The entry consumer claims this even
 * publication version without waiting; publication retries outside KO's lock.
 * No IO, allocation or ERROR-capable call spans the short claimed version. */
void
cluster_ko_ax_cleanup_sample_v3(const ClusterResId *resid, int dead_procno, int32 dead_node,
								ClusterKoAxCleanupV3 *sample)
{
	memset(sample, 0, sizeof(*sample));
	if (ko_state == NULL || resid == NULL || resid->type != LOCKTAG_RELATION)
		return;
	SpinLockAcquire(&ko_state->shared_lock);
	sample->revision = pg_atomic_read_u64(&ko_state->ax_publication_revision);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoSharedContext *entry = &ko_state->contexts[i];
		const ClusterGrdHolderId *holder = &entry->request_v3.ax_holder;
		if (!entry->used || !entry->v3 || entry->structure_owned
			|| holder->node_id != (uint32)cluster_node_id
			|| !((dead_procno >= 0 && holder->procno == (uint32)dead_procno)
				 || dead_node == cluster_node_id)
			|| memcmp(resid, &entry->request_v3.ax_resid, sizeof(*resid)) != 0)
			continue;
		entry->origin_detached = entry->ax_release_requested = true;
		sample->holders[sample->count++] = *holder;
	}
	SpinLockRelease(&ko_state->shared_lock);
}

bool
cluster_ko_ax_cleanup_claim_v3(const ClusterKoAxCleanupV3 *sample)
{
	uint64 expected = sample->revision;
	if (expected == 0)
		return true;
	return ko_state != NULL && (expected & 1) == 0 && expected < UINT64_MAX - 2
		   && pg_atomic_compare_exchange_u64(&ko_state->ax_publication_revision, &expected,
											 expected + 1);
}

bool
cluster_ko_ax_cleanup_retains_v3(const ClusterKoAxCleanupV3 *sample,
								 const ClusterGrdHolderId *holder)
{
	for (unsigned i = 0; i < sample->count; i++)
		if (memcmp(&sample->holders[i], holder, sizeof(*holder)) == 0)
			return true;
	return false;
}

void
cluster_ko_ax_cleanup_finish_v3(const ClusterKoAxCleanupV3 *sample)
{
	if (sample->revision != 0)
		pg_atomic_write_u64(&ko_state->ax_publication_revision, sample->revision + 2);
}

/* Requires shared_lock. A single S6 covers every operation of this exact AX. */
static bool
ko_origin_release_ready(unsigned slot)
{
	const ClusterKoSharedContext *selected = &ko_state->contexts[slot];
	if (!selected->used || !selected->v3 || selected->structure_owned || !selected->origin_detached
		|| !selected->ax_release_requested)
		return false;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoSharedContext *entry = &ko_state->contexts[i];
		if (!entry->used || !entry->v3 || entry->structure_owned
			|| !ko_origin_ax_equal(&selected->request_v3, &entry->request_v3))
			continue;
		if (i < slot || !entry->origin_detached || !entry->ax_release_requested || entry->ax_failed
			|| entry->committed_drain_pending
			|| entry->request_failed_peers != 0 || entry->request_sending_peers != 0
			|| entry->request_uncertain_peers != 0
			|| (entry->request_done_peers & entry->request_sent_peers) != entry->request_sent_peers)
			return false;
		for (unsigned n = 0; n < ko_state->send_v3_count; n++) {
			const ClusterKoSharedMessageV3 *message
				= &ko_state->send_v3[(ko_state->send_v3_head + n) % CLUSTER_KO_SHARED_CAPACITY];
			if (message->base.verb == CLUSTER_KO_SHARED_REQUEST
				&& message->base.batch_id == entry->request_v3.base.batch_id
				&& ko_origin_ax_equal(message, &entry->request_v3))
				return false;
		}
	}
	return true;
}

static void
ko_origin_release_tick(void)
{
	for (unsigned n = 0; n < 8; n++) {
		ClusterKoSharedContext before;
		volatile ClusterGesReleaseAttempt attempt;
		volatile ClusterGesRedeclareResult result = CLUSTER_GES_REDECLARE_PENDING;
		unsigned slot;
		bool ready;

		SpinLockAcquire(&ko_state->shared_lock);
		slot = ko_state->ax_release_cursor++ % CLUSTER_KO_SHARED_CAPACITY;
		ready = ko_origin_release_ready(slot);
		if (ready)
			before = ko_state->contexts[slot];
		SpinLockRelease(&ko_state->shared_lock);
		if (!ready)
			continue;
		attempt = before.ax_release;
		PG_TRY();
		{
			if (!ko_shared_v3_control_current(&before.request_v3))
				result = CLUSTER_GES_REDECLARE_CUT_CHANGED;
			else
				result = cluster_ges_release_poll(
					(ClusterGesReleaseAttempt *)&attempt, &before.request_v3.ax_resid,
					&before.request_v3.ax_holder, before.request_v3.ax_master_node,
					before.request_v3.ax_master_generation, before.ax_lock_group);
			if (result == CLUSTER_GES_REDECLARE_CONFIRMED
				&& !ko_shared_v3_control_current(&before.request_v3))
				result = CLUSTER_GES_REDECLARE_CUT_CHANGED;
		}
		PG_FINALLY();
		{
			SpinLockAcquire(&ko_state->shared_lock);
			if (ko_state->contexts[slot].used && ko_state->contexts[slot].serial == before.serial) {
				ko_state->contexts[slot].ax_release = attempt;
				if (result != CLUSTER_GES_REDECLARE_PENDING
					&& result != CLUSTER_GES_REDECLARE_CONFIRMED)
					ko_state->contexts[slot].ax_failed = true;
				if (result == CLUSTER_GES_REDECLARE_CONFIRMED && ko_origin_release_ready(slot)) {
					for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
						ClusterKoSharedContext *entry = &ko_state->contexts[i];
						if (entry->used && entry->v3 && !entry->structure_owned
							&& ko_origin_ax_equal(&before.request_v3, &entry->request_v3)) {
							if (ko_drop_release_locked(&entry->request_v3,
								entry->drop_identity_serial, entry->drop_identity))
								memset(entry, 0, sizeof(*entry));
							else
								entry->ax_failed = true;
						}
					}
				}
			}
			SpinLockRelease(&ko_state->shared_lock);
		}
		PG_END_TRY();
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
		bool committed_discard;
		uint8 bytes[CLUSTER_KO_SHARED_V3_BYTES];
		ClusterICSendResult result = CLUSTER_IC_SEND_HARD_ERROR;
		bool pending;

		SpinLockAcquire(&ko_state->shared_lock);
		pending = ko_state->send_v3_count != 0;
		if (pending) {
			message = ko_state->send_v3[ko_state->send_v3_head];
			local_generation = ko_state->send_v3_route_generation[ko_state->send_v3_head];
			committed_discard = ko_state->send_committed_discard[ko_state->send_v3_head];
		}
		SpinLockRelease(&ko_state->shared_lock);
		if (!pending)
			break;
		if (message.base.verb == CLUSTER_KO_SHARED_ACK)
			result = ko_peer_reply_send(&message, local_generation, committed_discard);
		else if (ko_shared_object_control_at_generation(&message, local_generation, committed_discard)
				 && ko_shared_object_encode(&message, committed_discard, bytes, sizeof(bytes))
				 && (committed_discard ? ko_shared_object_request_claim(&message, true)
									  : ko_shared_v3_request_claim(&message))) {
			volatile ClusterICSendResult sent = CLUSTER_IC_SEND_HARD_ERROR;
			PG_TRY();
			{
				sent = cluster_ic_send_envelope(PGRAC_IC_MSG_KO_FLUSH, message.base.peer_node,
												bytes, sizeof(bytes));
			}
			PG_FINALLY();
			{
				KoSendOwnership ownership
					= sent == CLUSTER_IC_SEND_NOT_ADMITTED ? KO_SEND_CALLER_OWNED
					  : (sent == CLUSTER_IC_SEND_DONE || sent == CLUSTER_IC_SEND_WOULD_BLOCK)
						  ? KO_SEND_TRANSPORT_OWNED : KO_SEND_UNKNOWN;

				if (committed_discard)
					ko_shared_object_record_ownership(&message, ownership, true);
				else
					ko_shared_v3_record_ownership(&message, ownership);
			}
			PG_END_TRY();
			result = sent;
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
			ko_state->send_committed_discard[tail] = committed_discard;
			ko_state->send_v3_count++;
		}
		SpinLockRelease(&ko_state->shared_lock);
	}
	ko_origin_release_tick();
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
		if (ko_state->contexts[i].used
			&& (ko_state->contexts[i].structure_drop_failed || ko_state->contexts[i].ax_failed
				|| ko_state->contexts[i].request_failed_peers != 0)
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
		} else if (ko_state->peer_fences[i].failure_sqlerrcode != 0) {
			result = CLUSTER_NORMAL_STOP_INVALID;
			why = "KO_PEER_APPLY_FAILED";
		} else if (ko_state->peer_fences[i].reply_state >= KO_PEER_REPLY_UNKNOWN) {
			result = CLUSTER_NORMAL_STOP_INVALID;
			why = "KO_PEER_REPLY_UNRESOLVED";
		} else if ((state == CLUSTER_KO_PEER_FENCE_ACTIVE
					|| ((state == CLUSTER_KO_PEER_FENCE_READY
						 || state == CLUSTER_KO_PEER_FENCE_COMMITTED)
						&& ko_state->peer_fences[i].physical_applied
						&& (!ko_state->peer_fences[i].ack_queued
							|| ko_state->peer_fences[i].reply_state
								   != KO_PEER_REPLY_TRANSPORT_OWNED)))
				   && result == CLUSTER_NORMAL_STOP_READY) {
			result = CLUSTER_NORMAL_STOP_PENDING;
			why = "KO_PEER_DRAIN_OWNED";
		}
	}
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		if (ko_state->directory_io[i].serial != 0 && result == CLUSTER_NORMAL_STOP_READY) {
			result = CLUSTER_NORMAL_STOP_PENDING;
			why = "KO_DIRECTORY_IO_ENTERED";
		}
	}
	for (unsigned i = 0; i < CLUSTER_CTRC_CLEANER_WORKERS; i++) {
		if (ko_state->cleaner_work[i].used && result == CLUSTER_NORMAL_STOP_READY) {
			result = CLUSTER_NORMAL_STOP_PENDING;
			why = "KO_CLEANER_ATTEMPT_OWNED";
		}
	}

	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		if (!ko_state->extend_work[i].used)
			continue;
		if (ko_state->extend_work[i].failed) {
			result = CLUSTER_NORMAL_STOP_INVALID;
			why = ko_state->extend_work[i].fork == InvalidForkNumber
				? "KO_CREATION_RECOVERY_REQUIRED" : "KO_EXTENSION_RECOVERY_REQUIRED";
		} else if (result == CLUSTER_NORMAL_STOP_READY) {
			result = CLUSTER_NORMAL_STOP_PENDING;
			why = ko_state->extend_work[i].fork == InvalidForkNumber
				? "KO_CREATION_OWNED" : "KO_EXTENSION_OWNED";
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
	uint64 revision;
	uint32 group = cluster_ges_current_lock_group(&request->ax_holder);

	if (!ko_exit_registered) {
		before_shmem_exit(ko_shared_backend_exit, (Datum)0);
		ko_exit_registered = true;
	}
	for (;;) {
		SpinLockAcquire(&ko_state->shared_lock);
		revision = pg_atomic_read_u64(&ko_state->ax_publication_revision);
		if ((revision & 1) == 0 && revision < UINT64_MAX - 2
			&& pg_atomic_compare_exchange_u64(&ko_state->ax_publication_revision, &revision,
											  revision + 1))
			break;
		SpinLockRelease(&ko_state->shared_lock);
		if (revision >= UINT64_MAX - 2)
			return -1;
		CHECK_FOR_INTERRUPTS();
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 10,
						WAIT_EVENT_CLUSTER_OBJECT_FLUSH_WAIT);
		ResetLatch(MyLatch);
	}
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		ClusterKoSharedContext *entry = &ko_state->contexts[i];
		if (!entry->used && ko_state->context_serial != UINT64_MAX) {
			memset(entry, 0, sizeof(*entry));
			entry->ax_lock_group = group;
			entry->used = true;
			entry->v3 = true;
			entry->complete = false;
			entry->request_sent_peers = entry->request_uncertain_peers = 0;
			entry->pid = MyProcPid;
			entry->serial = ++ko_state->context_serial;
			entry->request = request->base;
			entry->request_v3 = *request;
			memcpy(entry->peer_boots, boots, sizeof(entry->peer_boots));
			if (completion != NULL
				&& !ko_drop_reserve_locked(request, entry->serial, &entry->drop_identity)) {
				memset(entry, 0, sizeof(*entry));
				break;
			}
			if (entry->drop_identity != 0)
				entry->drop_identity_serial = entry->serial;
			context = i;
			if (completion != NULL) {
				completion->slot = i;
				completion->serial = entry->serial;
			}
			break;
		}
	}
	pg_atomic_write_u64(&ko_state->ax_publication_revision, revision + 2);
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

/* The origin captured the exact SPACE incarnation/token under its bound AX.
 * The authenticated ingress retained that complete identity in this fence.
 * Consuming it must not acquire current merely to observe SPACE: doing so can
 * create another old-incarnation carrier while this peer is draining it. */
static bool
ko_shared_v3_peer_identity_current(const ClusterKoSharedMessageV3 *message)
{
	uint8 encoded[CLUSTER_KO_SHARED_V3_BYTES];
	uint64 serial = 0, peer_route_generation = 0;
	unsigned slot = CLUSTER_KO_SHARED_CAPACITY;
	bool current = false;

	if (ko_state == NULL || message == NULL || !cluster_shared_config
		|| message->base.verb != CLUSTER_KO_SHARED_REQUEST
		|| message->base.peer_node != cluster_node_id || MyBackendType != B_SINVAL_BCAST
		|| CurrentResourceOwner == NULL || CritSectionCount != 0
		|| !cluster_ko_shared_encode_v3(message, encoded, sizeof(encoded)))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
		const ClusterKoPeerFenceV3 *entry = &ko_state->peer_fences[i];

		if (entry->state == CLUSTER_KO_PEER_FENCE_ACTIVE && entry->serial != 0
			&& memcmp(entry->encoded, encoded, sizeof(encoded)) == 0) {
			slot = i;
			serial = entry->serial;
			peer_route_generation = entry->peer_route_generation;
			break;
		}
	}
	SpinLockRelease(&ko_state->shared_lock);
	if (slot == CLUSTER_KO_SHARED_CAPACITY
		|| !ko_shared_v3_control_at_generation(message, peer_route_generation))
		return false;
	SpinLockAcquire(&ko_state->shared_lock);
	current = ko_state->peer_fences[slot].state == CLUSTER_KO_PEER_FENCE_ACTIVE
			  && ko_state->peer_fences[slot].serial == serial
			  && ko_state->peer_fences[slot].peer_route_generation == peer_route_generation
			  && memcmp(ko_state->peer_fences[slot].encoded, encoded, sizeof(encoded)) == 0;
	SpinLockRelease(&ko_state->shared_lock);
	return current;
}

static bool
ko_shared_wait_for_acks_v3(ClusterKoSharedMessageV3 *request, uint32 mask,
							 const uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT], uint32 context)
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
		int events;
		int32 failed_peer;
		CHECK_FOR_INTERRUPTS();
		if (cluster_sinval_ack_wait_read_failure(request->base.batch_id, &failed_peer)) {
			KO_BUMP(failclosed_count);
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE),
					 errmsg("KO v3 peer could not complete the object drain"),
					 errdetail("origin=%u epoch=" UINT64_FORMAT " batch=" UINT64_FORMAT
							   " peer=%d locator=%u/%u/%u operation=%u",
							   request->base.origin_node, request->base.epoch,
							   request->base.batch_id, failed_peer,
							   request->base.key.locator.spcOid, request->base.key.locator.dbOid,
							   request->base.key.locator.relNumber, request->operation),
					 errhint("Check the peer log for this operation's original error; "
							 "the object drain has not completed.")));
		}
		if (cluster_sinval_ack_wait_is_complete(request->base.batch_id)) {
			ClusterKoSharedContext current;

			SpinLockAcquire(&ko_state->shared_lock);
			current = ko_state->contexts[context];
			SpinLockRelease(&ko_state->shared_lock);
			/* A wait-table notification is not the operation's result set.
			 * The original sender may still be returning after an early ACK;
			 * wait for its reference instead of failing the native handoff. */
			if (current.pid == MyProcPid && current.request.batch_id == request->base.batch_id
				&& ko_shared_ready_v3(&current))
				return ko_shared_v3_control_current(request) && ko_shared_v3_space_current(request);
		}
		/* A timed wait only schedules the next identity/interrupt check.
		 * Missing READY is not evidence that the accepted drain failed;
		 * exact ACKs, peer failure and caller cancellation keep their original
		 * consumers. Latch notification can end this wait before the interval. */
		events = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
								   cluster_sinval_ack_timeout_ms,
								   WAIT_EVENT_CLUSTER_OBJECT_FLUSH_WAIT);
		ResetLatch(MyLatch);
		if (events & WL_POSTMASTER_DEATH)
			break;
	}
	return false;
}

/* Spec: 2026-10-16-pre2-p014-am05-shared-ddl-autocommit.md
 * Consume only the original, still-unsent registration. A later execution
 * must not allocate another batch or refresh the transaction's identity. */
static bool
ko_shared_registration_snapshot_v3(const ClusterKoCompletionV2 *completion,
								   ClusterKoSharedContext *out)
{
	const ClusterKoCompletionV2 *owned = *ko_completion_link(completion);
	const ClusterKoSharedMessageV3 *request;
	bool valid;

	if (owned == NULL || ko_state == NULL || out == NULL || !owned->native_transaction
		|| owned->pid != MyProcPid || !ko_completion_owner_current(owned)
		|| CritSectionCount != 0 || !owned->barrier_v3 || owned->barrier_private
		|| owned->native_handed_off || owned->space_observed
		|| owned->slot >= CLUSTER_KO_SHARED_CAPACITY || owned->serial == 0
		|| !DsaPointerIsValid(owned->native_allocation) || owned->native_reserved == NULL)
		return false;
	request = &owned->barrier_request_v3;
	SpinLockAcquire(&ko_state->shared_lock);
	*out = ko_state->contexts[owned->slot];
	valid = out->used && out->v3 && !out->complete && !out->origin_detached
		&& !out->ax_release_requested && !out->ax_failed && !out->structure_owned
		&& out->pid == owned->pid && out->serial == owned->serial
		&& (out->request_sent_peers | out->request_sending_peers | out->request_uncertain_peers
			| out->request_done_peers | out->request_failed_peers) == 0
		&& memcmp(&out->request, &request->base, sizeof(out->request)) == 0
		&& memcmp(&out->request_v3, request, sizeof(*request)) == 0
		&& memcmp(out->peer_boots, owned->barrier_boots, sizeof(out->peer_boots)) == 0;
	SpinLockRelease(&ko_state->shared_lock);
	return valid && ko_shared_origin_current(out) && ko_shared_v3_local_binding_current(request);
}

static bool
ko_shared_registered_v3(RelFileLocator locator, uint16 operation, const KoAxBinding *binding,
						const ClusterKoCompletionV2 *completion, ClusterKoSharedContext *out)
{
	const ClusterKoCompletionV2 *owned = *ko_completion_link(completion);
	const ClusterKoSharedMessageV3 *request;

	if (owned == NULL || binding == NULL)
		return false;
	request = &owned->barrier_request_v3;
	return RelFileLocatorEquals(locator, request->base.key.locator)
		&& operation == request->operation
		&& memcmp(&binding->resid, &request->ax_resid, sizeof(binding->resid)) == 0
		&& memcmp(&binding->holder, &request->ax_holder, sizeof(binding->holder)) == 0
		&& binding->request_id == request->ax_request_id
		&& binding->master_node == request->ax_master_node
		&& binding->master_generation == request->ax_master_generation
		&& ko_shared_registration_snapshot_v3(owned, out);
}

static bool
ko_run_shared_barrier_impl_v3(RelFileLocator locator, uint16 operation,
							  const KoAxBinding *binding, ClusterKoCompletionV2 *completion)
{
	ClusterKoSharedMessageV3 request = { 0 };
	ClusterKoSharedContext registered;
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT];
	uint64 batch_id;
	uint32 mask;
	int context;
	volatile bool begun = false, ok = false;

	if (completion != NULL) {
		if (!ko_shared_registered_v3(locator, operation, binding, completion, &registered))
			return false;
		request = registered.request_v3;
		memcpy(boots, registered.peer_boots, sizeof(boots));
		mask = 0;
		for (unsigned n = 0; n < CLUSTER_KO_SHARED_NODE_LIMIT; n++)
			if (n != cluster_node_id && boots[n] != 0)
				mask |= 1u << n;
		context = completion->slot;
	} else {
		if (!ko_shared_prepare_request_v3(locator, operation, binding, &request, boots, &mask)
			|| !ko_shared_v3_local_binding_current(&request))
			return false;
		if (mask == 0) {
			KO_BUMP(native_count);
			return true;
		}
		context = ko_shared_reserve_v3(&request, boots, NULL);
		if (context < 0)
			return false;
		SpinLockAcquire(&ko_state->shared_lock);
		registered = ko_state->contexts[context];
		SpinLockRelease(&ko_state->shared_lock);
	}
	batch_id = request.base.batch_id;
	PG_TRY();
	{
		/* Reserve transaction-owned publication before any peer REQUEST can
		 * escape. Abort discards it through the original invalidation owner;
		 * registration failure unwinds only this still-unsent reservation. */
		if (mask != 0 && completion == NULL)
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
			/* The original owner removes this entry; v3 has no protocol deadline. */
			begun = cluster_sinval_ack_wait_begin(request.base.batch_id, mask, 0);
		if (begun)
			ok = ko_shared_wait_for_acks_v3(&request, mask, boots, context);
	}
	PG_FINALLY();
	{
		ClusterKoSharedContext *current;

		SpinLockAcquire(&ko_state->shared_lock);
		current = &ko_state->contexts[context];
		if (current->used && current->v3 && current->pid == registered.pid
			&& current->serial == registered.serial && !current->structure_owned
			&& memcmp(&current->request_v3, &registered.request_v3,
					  sizeof(current->request_v3)) == 0
			&& memcmp(current->peer_boots, registered.peer_boots, sizeof(current->peer_boots)) == 0) {
			if (ok && completion != NULL)
				ok = ko_shared_ready_v3(current);
			if (ok && completion != NULL)
				current->complete = true;
			else if (!ok)
				current->origin_detached = true;
			else
				memset(current, 0, sizeof(*current));
		} else
			ok = false;
		SpinLockRelease(&ko_state->shared_lock);
		if (begun)
			cluster_sinval_ack_wait_remove(batch_id);
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
	if (binding != NULL && completion != NULL) {
		ClusterKoSharedContext registered;

		if (!ko_shared_registered_v3(rloc, operation, binding, completion, &registered))
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE),
					 errmsg("the registered KO v3 operation is no longer current"),
					 errhint("Retry the transaction after the relation grant and formation are stable.")));
		/* Execution consumes the unsent offer. The native wrapper reoffers
		 * only after its original barrier and local closure both succeed. */
		completion->native_pending = false;
	}
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
				 errhint(binding != NULL
						 ? "Check cluster and peer logs for an object operation or identity failure "
						   "before retrying."
						 : "A peer did not acknowledge the cross-node buffer flush within "
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

static void
ko_shared_registration_error_v3(ClusterKoCompletionV2 **completion, int sqlerrcode)
{
	ClusterKoCompletionV2 *owned = *completion;

	SpinLockAcquire(&ko_state->shared_lock);
	if (owned->slot < CLUSTER_KO_SHARED_CAPACITY
		&& ko_state->contexts[owned->slot].used
		&& ko_state->contexts[owned->slot].pid == owned->pid
		&& ko_state->contexts[owned->slot].serial == owned->serial)
		ko_state->contexts[owned->slot].cancel_sqlerrcode = sqlerrcode;
	SpinLockRelease(&ko_state->shared_lock);
	cluster_ko_shared_release_v2(completion);
}

/* Registration owns only the original allocation, identity and reversible
 * commit publication. It neither runs an object barrier nor proves READY. */
static bool
ko_shared_begin_v3(RelFileLocator rloc, char relpersistence, uint16 operation,
					const KoAxBinding *binding, ClusterKoCompletionV2 **out)
{
	ClusterKoCompletionV2 *completion;
	ClusterKoSharedMessageV3 request = { 0 };
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT];
	uint32 mask;

	if (out == NULL || *out != NULL || binding == NULL || !cluster_enabled
		|| !cluster_shared_config || ko_state == NULL || relpersistence == RELPERSISTENCE_TEMP
		|| RecoveryInProgress() || cluster_node_id < 0 || CurrentResourceOwner == NULL
		|| !ko_native_transaction_current() || TopTransactionContext == NULL
		|| CritSectionCount != 0 || MyProcPid <= 0)
		return false;
	if (!ko_resource_registered) {
		RegisterResourceReleaseCallback(ko_shared_resource_release, NULL);
		RegisterXactCallback(ko_shared_xact_event, NULL);
		ko_resource_registered = true;
	}
	completion = MemoryContextAllocZero(TopTransactionContext, sizeof(*completion));
	completion->owner = CurTransactionResourceOwner;
	completion->native_transaction = true;
	completion->native_pending = true;
	completion->pid = MyProcPid;
	completion->slot = CLUSTER_KO_SHARED_CAPACITY;
	completion->next = ko_completions;
	ko_completions = completion;
	PG_TRY();
	{
		/* Reserve the original native carrier before any object work or
		 * REQUEST can escape. Unpublished reservations use the same ERROR
		 * cleanup as the local handle; allocation is not a completion proof. */
		if (!ko_native_reserve(completion))
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("could not reserve the KO v3 completion owner")));
		if (!ko_shared_prepare_request_v3(rloc, operation, binding, &request, boots, &mask)
			|| !ko_shared_v3_local_binding_current(&request)
			|| ko_shared_reserve_v3(&request, boots, completion) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE),
					 errmsg("could not register the KO v3 operation"),
					 errhint("Retry after the relation identity, grant and formation are stable.")));
		completion->barrier_v3 = true;
		completion->barrier_request_v3 = request;
		memcpy(completion->barrier_boots, boots, sizeof(boots));
		if (mask != 0)
			CacheInvalidateKoCommit(request.base.batch_id);
	}
	PG_CATCH();
	{
		ko_shared_registration_error_v3(&completion, geterrcode());
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
		PG_TRY();
		{
			ko_flush_and_wait_ack(rel->rd_locator, relpersistence, completion, operation, &binding);
		}
		PG_CATCH();
		{
			ko_shared_registration_error_v3(&completion, geterrcode());
			PG_RE_THROW();
		}
		PG_END_TRY();
		completion->owner = CurTransactionResourceOwner;
		completion->native_transaction = true;
		completion->barrier_v3 = true;
		if (!ko_native_barrier_to_owner(completion))
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE),
					 errmsg("could not preserve the KO v3 completion proof")));
		if (operation == CLUSTER_KO_SHARED_V3_OPERATION_DROP)
			ko_native_origin_checkpoint(completion);
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
	bool v3 = false, committed_discard = false;

	if (ko_state == NULL || env == NULL || payload == NULL)
		return;
	if (shared) {
		if (env->payload_length == CLUSTER_KO_SHARED_V3_BYTES) {
			if (!ko_shared_object_decode(payload, env->payload_length, &qualified_v3,
									 &committed_discard)
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
				|| !ko_shared_object_control_at_generation(&qualified_v3, peer_route_generation,
													  committed_discard))
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
	if (v3 && (committed_discard ? ko_peer_request_ended_or_conflicts(&qualified_v3, true)
								  : ko_peer_request_ended_or_conflicts_v3(&qualified_v3)))
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
				&& queued->committed_discard == committed_discard
				&& memcmp(&queued->qualified_v3, &qualified_v3, sizeof(qualified_v3)) == 0)
				return;
			scan = (scan + 1) % CLUSTER_KO_INBOUND_CAPACITY;
		}
		/* Register the exact peer owner before publishing the physical work.
		 * A consumed request is never re-enqueued as a second flush attempt. */
		{
			ClusterKoPeerFenceHandleV3 handle;
			ClusterKoPeerFenceStateV3 state;

			if (!ko_peer_fence_begin(&qualified_v3, &handle, &state, committed_discard)
				|| (state != CLUSTER_KO_PEER_FENCE_ACTIVE && state != CLUSTER_KO_PEER_FENCE_READY)
				|| ko_peer_fence_physical_owned(&qualified_v3, committed_discard))
				return;
		}
	}
	ko_state->inbound[tail].shared = shared;
	ko_state->inbound[tail].v3 = v3;
	ko_state->inbound[tail].committed_discard = committed_discard;
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
 * KO-M6: ONLY status DONE counts as fulfilled. An exact v3 FAILED wakes the
 * original waiter with a negative result; legacy missing ACKs retain their
 * original timeout. There is no RESET_PENDING shortcut (SIResetAll does not
 * drop buffer-pool dirty pages).
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
			bool matched = false, committed_discard = false;

			if (!ko_shared_object_decode(payload, env->payload_length, &ack, &committed_discard)
				|| ack.base.verb != CLUSTER_KO_SHARED_ACK
				|| (ack.base.status != CLUSTER_KO_SHARED_DONE
					&& ack.base.status != CLUSTER_KO_SHARED_FAILED)
				|| ack.base.origin_node != cluster_node_id
				|| ack.base.peer_node != env->source_node_id
				|| !ko_shared_object_control_at_generation(&ack, ack.ax_master_generation,
														 committed_discard))
				return;
			SpinLockAcquire(&ko_state->shared_lock);
			for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++) {
				ClusterKoSharedContext *entry = &ko_state->contexts[i];
				ClusterKoSharedMessageV3 expected;
				if (!entry->used || !entry->v3
					|| entry->committed_discard != committed_discard
					|| entry->request_v3.base.batch_id != ack.base.batch_id
					|| ((entry->request_sent_peers | entry->request_uncertain_peers)
						& (1u << ack.base.peer_node))
						   == 0)
					continue;
				expected = entry->request_v3;
				expected.base.peer_node = ack.base.peer_node;
				expected.base.peer_boot = entry->peer_boots[ack.base.peer_node];
				matched = cluster_ko_shared_result_matches_v3(&expected, &ack);
				if (matched) {
					uint16 peer = (uint16)(1u << ack.base.peer_node);
					entry->request_sent_peers |= peer;
					entry->request_uncertain_peers &= ~peer;
					if (ack.base.status == CLUSTER_KO_SHARED_FAILED)
						entry->request_failed_peers |= peer;
					else
						entry->request_done_peers |= peer;
				}
				break;
			}
			SpinLockRelease(&ko_state->shared_lock);
			if (matched) {
				if (ack.base.status == CLUSTER_KO_SHARED_FAILED)
					cluster_sinval_ack_wait_record_failure(ack.base.batch_id, ack.base.peer_node);
				else {
					cluster_sinval_ack_wait_record(ack.base.batch_id, ack.base.peer_node);
					KO_BUMP(ack_received_count);
				}
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

	if (ko_state == NULL || ko_peer_attempt_active)
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
			&& ko_peer_request_ended_or_conflicts(&slot.qualified_v3, slot.committed_discard)) {
			head = (head + 1) % CLUSTER_KO_INBOUND_CAPACITY;
			pg_atomic_write_u32(&ko_state->inbound_head, head);
			goto next;
		}
		/* R2 v3 keeps an accepted request at the ring head while its bounded
		 * terminal-reference proof is still pending. This is the existing ring's
		 * REQUEST -> READY continuation: later requests are not acknowledged out
		 * of order, and no identity read or synthetic ACK is introduced. */
		if (slot.shared && slot.v3 && !slot.committed_discard
			&& slot.epoch == cluster_epoch_get_current()
			&& slot.shared == cluster_shared_config
			&& ko_shared_v3_control_at_generation(&slot.qualified_v3, slot.peer_route_generation)
			&& !cluster_ctrc_relation_removal_ready_shared(
				(uint32)slot.spc_oid, (uint32)slot.db_oid, (uint32)slot.rel_number)) {
			/* Preserve FIFO ownership of the blocked head, but do not starve
			 * already-applied fences behind it.  Their local READY/ACK
			 * continuation is independent of this head's CTRC proof. */
			ko_peer_fence_local_ready_tick_v3();
			return;
		}
		if (slot.shared && slot.v3) {
			if (!(slot.committed_discard
				  ? ko_peer_fence_mark_physical_started(&slot.qualified_v3, &ko_peer_attempt, true)
				  : ko_peer_fence_mark_physical_started_v3(&slot.qualified_v3, &ko_peer_attempt))) {
				head = (head + 1) % CLUSTER_KO_INBOUND_CAPACITY;
				pg_atomic_write_u32(&ko_state->inbound_head, head);
				goto next;
			}
			ko_peer_attempt_active = true;
		}
		/*
		 * Advance the head BEFORE applying. The exact v3 attempt is now owned
		 * by its shared fence; SI error cleanup records FAILED there without
		 * retrying a partially applied operation or losing its responsibility.
		 * Legacy requests retain the original no-ACK failure behavior.
		 */
		head = (head + 1) % CLUSTER_KO_INBOUND_CAPACITY;
		pg_atomic_write_u32(&ko_state->inbound_head, head);
		if (slot.shared && slot.v3)
			goto next;

		rloc.spcOid = (Oid)slot.spc_oid;
		rloc.dbOid = (Oid)slot.db_oid;
		rloc.relNumber = (RelFileNumber)slot.rel_number;
		if ((slot.shared && slot.epoch == 0) || slot.epoch != cluster_epoch_get_current()
			|| slot.shared != cluster_shared_config
			|| (slot.shared
				&& (slot.v3 ? (!ko_shared_v3_control_at_generation(&slot.qualified_v3,
																   slot.peer_route_generation)
							   || !ko_shared_v3_peer_identity_current(&slot.qualified_v3))
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
							   || !ko_shared_v3_peer_identity_current(&slot.qualified_v3))
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
							   || !ko_shared_v3_peer_identity_current(&slot.qualified_v3))
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
									   || !ko_shared_v3_peer_identity_current(&slot.qualified_v3))
									: !ko_shared_control_current(&slot.qualified))))
					goto next;
			}
		}
		/* v3 consumes the AX-bound identity without creating a SPACE carrier.
		 * The legacy reader stays before invalidation, never after it. */
		if (slot.shared
			&& (slot.v3 ? !ko_shared_v3_peer_identity_current(&slot.qualified_v3)
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
				/* Physical completion is retained by the fence owner.  The SI
				 * continuation performs the bounded local READY scan and only
				 * then queues the existing DONE ACK. */
				if (!ko_peer_fence_mark_physical_applied_v3(&slot.qualified_v3))
					goto next;
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
		ko_peer_attempt_active = false;
		head = pg_atomic_read_u32(&ko_state->inbound_head);
		tail = pg_atomic_read_u32(&ko_state->inbound_tail);
	}
	ko_peer_fence_local_ready_tick_v3();
}
