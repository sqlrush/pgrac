/*-------------------------------------------------------------------------
 *
 * cluster_pcm_lock.c
 *	  pgrac cluster PCM (Parallel Cache Management) lock state machine.
 *
 *	  spec-1.7 introduced the C API and shmem scaffolding.  spec-2.30
 *	  activates the local PCM 9-transition state machine, GrdEntry HTAB,
 *	  per-entry LWLockPadded protection, PI bitmap bookkeeping, and
 *	  transition counters.  Buffer manager / GCS wire callers are still
 *	  intentionally deferred to later Cache Fusion specs.
 *
 *	  The full GrdEntry struct definition lives in this file (private) per
 *	  the opaque-struct decision; callers use only the public helpers in
 *	  cluster_pcm_lock.h.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_pcm_lock.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-1.7-pcm-state-placeholder.md (frozen 2026-05-02 v1.1)
 *	  Design: docs/pcm-lock-protocol-design.md v1.0 §3-§5
 *	  AD-002 (PCM lock state machine N/S/X + PI orthogonal flag)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlogdefs.h"
#include "cluster/cluster_grd.h" /* PGRAC: spec-2.30 D1 — ClusterGrdHolderId 24B */
#include "cluster/cluster_guc.h" /* PGRAC: spec-2.30 D3 — cluster_node_id */
#include "cluster/cluster_gcs.h" /* PGRAC: spec-2.32 D5 — master lookup + send_transition_and_wait */
#include "cluster/cluster_gcs_block.h" /* PGRAC: spec-2.33 D7 — send_block_request_and_wait */
#include "cluster/cluster_inject.h"
#include "cluster/cluster_cssd.h" /* PGRAC: spec-4.7a D4 — peer liveness for other-holder check */
#include "cluster/cluster_lms.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"	   /* PGRAC: spec-2.30 D1 — pg_atomic_uint32/64 */
#include "portability/instr_time.h"
#include "storage/backendid.h" /* PGRAC: spec-6.14 D9 amend — no-backend-identity guard */
#include "storage/buf_internals.h"
#include "storage/condition_variable.h" /* PGRAC: spec-2.31 D1 — wait_cv */
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/hsearch.h"	 /* PGRAC: spec-2.30 D2 — HTAB API */
#include "pgstat.h"			 /* pgstat_report_wait_start/end */
#include "utils/timestamp.h" /* PGRAC: spec-2.30 D1 — TimestampTz */

/* Some focused lock-state binaries intentionally link cluster_pcm_lock.o
 * without the GCS transport object.  Permit that isolated link shape, but
 * never make the safety action optional: a complete backend provides the
 * strong symbol from cluster_gcs.c, while any remote-S execution without it
 * fails closed below. */
#if defined(__GNUC__) || defined(__clang__)
#pragma weak cluster_gcs_try_send_transition_and_wait
#define PCM_GCS_TRY_SENDER_LINKED() \
	(cluster_gcs_try_send_transition_and_wait != NULL)
#else
#define PCM_GCS_TRY_SENDER_LINKED() true
#endif

#define PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION UINT32_C(1)
#define PGRAC_RESOURCE_X_OUTBOUND_OWNER_SLOTS \
	(RESOURCE_X_PROTOCOL_NODE_LIMIT + 5)
#define PGRAC_RESOURCE_X_PROOF_DIGEST_OFFSET UINT64_C(1469598103934665603)
#define PGRAC_RESOURCE_X_PROOF_DIGEST_PRIME UINT64_C(1099511628211)
#define PGRAC_RESOURCE_X_NATIVE_INITIAL_FORMATION UINT64_C(1)

/* PGRAC adaptation identifiers.  The external cutover manifest binds this
 * semantic creator fingerprint to the exact product tree; neither byte array
 * is represented as an Oracle internal record. */
static const uint8 pcm_resource_x_completion_product_fingerprint[
	RESOURCE_X_COMPLETION_FINGERPRINT_BYTES] = {
	0x75, 0x31, 0x09, 0x2d, 0x1d, 0x8f, 0x64, 0x6a,
	0x1e, 0x35, 0x65, 0x3b, 0x55, 0xb0, 0xe4, 0x2e,
	0x4a, 0x18, 0xe7, 0x90, 0xd9, 0x78, 0x35, 0x36,
	0x19, 0x28, 0x07, 0x90, 0x6c, 0xf6, 0x60, 0x90
};
static const uint8 pcm_resource_x_send_c1_manifest_fingerprint[
	RESOURCE_X_COMPLETION_FINGERPRINT_BYTES] = {
	0xdf, 0x14, 0x38, 0x29, 0x7a, 0x73, 0xcc, 0xbd,
	0x19, 0x0c, 0xc6, 0x66, 0xaa, 0x97, 0x74, 0xb4,
	0x96, 0x52, 0x61, 0xc1, 0xfd, 0xa8, 0x34, 0xeb,
	0x41, 0x23, 0x27, 0xf7, 0x10, 0x6c, 0x20, 0x95
};


/*
 * GUC: cluster.pcm_grd_max_entries
 *
 *	spec-2.30 D5:  default -1 (sentinel for "auto → NBuffers");  0 = explicit
 *	disable (spec-1.7 stub behavior);  positive = explicit count (HC62
 *	fail-closed if < NBuffers).  Range [-1, 1048576].  PGC_POSTMASTER.
 */
int cluster_pcm_grd_max_entries = -1;


/*
 * PGRAC: spec-2.30 D5 + HC62 — resolve effective entry count from GUC value.
 *
 *	Returns:
 *	  0          — disabled (cluster_pcm_grd_max_entries == 0)
 *	  positive   — resolved count to use for HTAB / accessor / mutation
 *
 *	Fail-closed paths (ereport FATAL) raised only when fatal_on_misconfig
 *	is true (i.e. from init_fn after shmem reservation is fixed).  When
 *	called from shmem_size (fatal_on_misconfig=false), invalid configs
 *	return a plausible upper-bound to avoid under-reservation.
 */
static int
pcm_grd_effective_entries(bool fatal_on_misconfig)
{
	int guc = cluster_pcm_grd_max_entries;

	if (guc == 0)
		return 0; /* explicit disable */

	if (guc == -1) {
		/* auto: resolve to NBuffers with HC62 checks */
		if (NBuffers <= 0) {
			if (fatal_on_misconfig)
				ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("shared_buffers required for PCM activation"),
								errhint("Set shared_buffers > 0 or "
										"cluster.pcm_grd_max_entries=0 to disable.")));
			return 0;
		}
		if (NBuffers > 1048576) {
			if (fatal_on_misconfig)
				ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("PCM GRD requires more than 1048576 entries "
									   "(NBuffers=%d)",
									   NBuffers),
								errhint("Set cluster.pcm_grd_max_entries=0 to disable, "
										"or reduce shared_buffers.")));
			return 1048576;
		}
		return NBuffers;
	}

	/* explicit positive */
	if (NBuffers > 0 && guc < NBuffers) {
		if (fatal_on_misconfig)
			ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("PCM GRD entries (%d) must cover NBuffers (%d)", guc, NBuffers),
							errhint("Raise cluster.pcm_grd_max_entries to at least "
									"NBuffers, or set to 0 to disable.")));
		/* shmem_size path: return upper bound to avoid under-reservation */
		return NBuffers;
	}
	return guc;
}


/*
 * PGRAC: spec-2.30 D1 — file-private forward decl for ConvertQueue.
 *
 *	Convert queue node lifecycle / linked-list mutation is NOT in scope
 *	for spec-2.30 (本 spec: 状态机 + GrdEntry shmem layout 真激活;wire
 *	convert queue 推 spec-2.32 GCS req).  Forward decl 仅供 GrdEntry struct
 *	field type 引用;runtime 始终 NULL until future spec wires.
 */
typedef struct PcmConvertQueue PcmConvertQueue;


#define RESOURCE_X_BOOTSTRAP_ROUND_EMPTY UINT8_C(0)
#define RESOURCE_X_BOOTSTRAP_ROUND_REQUEST_DISPATCHED UINT8_C(1)
#define RESOURCE_X_BOOTSTRAP_ROUND_BASE_BOUND UINT8_C(2)
#define RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED UINT8_C(3)
#define RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED UINT8_C(4)
#define RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED UINT8_C(5)

/* PGRAC adaptation: one requester-local round per resource.  This is neither
 * a queue nor authority.  The immutable request bytes let same-node callers
 * fan in and let retry ownership move without allocating a new attempt. */
typedef struct ClusterPcmResourceXBootstrapRound {
	ResourceXDecodedCommon request;
	ResourceXDecodedCommon ack;
	ResourceXDecodedCommon assertion;
	ResourceXAcquisitionRef terminal_ref;
	uint64 highest_attempt_floor;
	uint64 resource_formation;
	uint64 master_session_incarnation;
	uint64 r4_record_generation;
	uint64 head_no_progress_deadline_us;
	uint64 head_change_generation;
	uint64 head_last_semantic_progress_us;
	uint64 head_no_progress_budget_us;
	uint64 last_dispatch_us;
	uint64 retry_slice_us;
	uint64 accepted_base;
	uint64 terminal_authority_generation;
	uint64 cached_ownership_generation;
	uint64 install_claim_pending_generation;
	uint64 install_claim_reservation_token;
	int32 current_master_node;
	uint32 requester_sender_connection_generation;
	uint32 master_ingress_connection_generation;
	uint8 phase;
	uint8 install_claim_source;
	uint8 head_failure_reason;
	uint8 reserved[1];
} ClusterPcmResourceXBootstrapRound;

StaticAssertDecl(sizeof(ClusterPcmResourceXBootstrapRound) == 464,
				 "Resource-X requester bootstrap round layout must remain 464 bytes");

#define RESOURCE_X_LOCAL_OWNER_EMPTY UINT8_C(0)
#define RESOURCE_X_LOCAL_OWNER_RECYCLING UINT8_C(1)
#define RESOURCE_X_LOCAL_OWNER_REVOKING UINT8_C(2)
#define RESOURCE_X_LOCAL_OWNER_HANDOFF UINT8_C(3)
#define RESOURCE_X_LOCAL_OWNER_EVICTING UINT8_C(4)

/* One bounded, entry-local successor priority.  The holder reference is the
 * retained terminal cover, while the successor fields identify the only
 * type-17 replay permitted to consume HANDOFF.  No transport connection or
 * process owner is retained here. */
typedef struct ClusterPcmResourceXLocalHandoff {
	ResourceXAssertion successor_assertion;
	ResourceXAcquisitionRef holder_ref;
	uint64 base_authority_generation;
	uint64 master_session_incarnation;
	uint64 assertion_sequence;
	uint64 r4_record_generation;
	uint64 buffer_ownership_generation;
	uint64 deadline_us;
	int32 master_node;
	uint32 ordered_lane;
	uint8 valid;
	uint8 reserved[7];
} ClusterPcmResourceXLocalHandoff;

StaticAssertDecl(sizeof(ClusterPcmResourceXLocalHandoff) == 128,
				 "Resource-X local handoff layout must remain 128 bytes");

/* Entry-local serialization only.  This record is not canonical authority,
 * is never serialized, and cannot make a terminal cover true.  It closes the
 * check-to-claim gap between an off-lock ITL recycler and the exact type-17
 * X-source revoke which consume the same retained local carrier. */
typedef struct ClusterPcmResourceXLocalOwner {
	ResourceXLocalOwnerHandle handle;
	ClusterPcmResourceXLocalHandoff handoff;
	uint64 highest_owner_generation;
	uint8 state;
	uint8 reserved[7];
} ClusterPcmResourceXLocalOwner;

StaticAssertDecl(sizeof(ClusterPcmResourceXLocalOwner) == 240,
				 "Resource-X local owner layout must remain 240 bytes");


/*
 * PGRAC: spec-2.30 D1 + spec-2.31 D1 — GrdEntry full struct definition (file-private).
 *
 *	Header keeps `typedef struct GrdEntry GrdEntry;` opaque (spec-1.7 Q3
 *	user-locked).  Callers/tests MUST go through accessor APIs; direct
 *	deref of `GrdEntry *` is forbidden.
 *
 *	Layout (spec-2.31 D1 v0.4 — size 实证 NEW; was 216B in spec-2.30):
 *	  [  0,  20) BufferTag       tag                        (PG-native; 20B)
 *	  [ 20,  24) pg_atomic_uint32 master_state              (PcmState N/S/X)
 *	  [ 24,  28) int32           x_holder_node              (-1 = no X holder)
 *	  [ 28,  32) pg_atomic_uint32 s_holders_bitmap          (per-node S bit)
 *	  [ 32,  36) pg_atomic_uint32 pi_holders_bitmap         (per-node PI bit)
 *	  [ 36,  38) uint16          s_holder_refcount_local    (spec-2.31 D1 v0.4: same-node S refs)
 *	  [ 38,  40) uint16          _pad1                      (4B align of next field)
 *	  [ 40,  48) PcmConvertQueue *convert_queue             (NULL until spec-2.32)
 *	  [ 48,  56) TimestampTz     last_transition_at         (GetCurrentTimestamp() on each transition)
 *	  [ 56,  64) pg_atomic_uint64 transition_count_local    (per-entry monotone)
 *	  [ 64,  88) ClusterGrdHolderId master_holder           (24B 4-tuple identity)
 *	  [ 88,  ??) ConditionVariable wait_cv                  (spec-2.31 D1 v0.4: incompatible state wait)
 *	  [  ?,  ??) LWLockPadded    entry_lock                 (PG_CACHE_LINE_SIZE=128B)
 *
 *	PGRAC: spec-2.30 §2.1 nominal 208B was based on BufferTag=16B
 *	assumption;  PG 16.13 实证 sizeof(BufferTag) == 20B (per
 *	test_cluster_buffer_desc.c:14 PIVOT B note + struct buftag in
 *	buf_internals.h:138).  spec-2.30 size 216B → flag Hardening v1.0.1 F1.
 *
 *	PGRAC: spec-2.31 D1 v0.4 — bufmgr-safe blocking/refcount API hardening
 *	adds `s_holder_refcount_local` (same-node S refs; spec-2.32 wire 配套
 *	时 extend 为 array per-node) + `wait_cv` (incompatible state wait via
 *	ConditionVariableSleep(wait_cv, WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT)).
 *	GrdEntry size 216 → 实证后定 (probable ~232-240B; StaticAssertDecl
 *	enforces actual value).  spec-2.30 §2.1 frozen 不改;Hardening v1.0.2
 *	forward-link appendix in ship-level closeout.
 *
 *	HC57 — transition mutation must hold entry_lock EXCLUSIVE;
 *	master_state read 路径 atomic uint32 read 无锁;atomic bitmap
 *	primitives 保留 (HC58) 为 future read-mostly fast path 预留 (本 spec
 *	所有 transition mutation 仍在 entry_lock 内 update).
 */
struct GrdEntry {
	BufferTag tag;							 /* 20B [  0,  20) */
	pg_atomic_uint32 master_state;			 /*  4B [ 20,  24) PcmState atomic */
	int32 x_holder_node;					 /*  4B [ 24,  28) -1 = no X holder */
	pg_atomic_uint32 s_holders_bitmap;		 /*  4B [ 28,  32) per-node S bit */
	pg_atomic_uint32 pi_holders_bitmap;		 /*  4B [ 32,  36) per-node PI bit */
	uint16 s_holder_refcount_local;			 /*  2B [ 36,  38) spec-2.31 D1: same-node S refs */
	uint16 _pad1;							 /*  2B [ 38,  40) 4B align */
	PcmConvertQueue *convert_queue;			 /*  8B [ 40,  48) NULL until spec-2.32 */
	TimestampTz last_transition_at;			 /*  8B [ 48,  56) */
	pg_atomic_uint64 transition_count_local; /*  8B [ 56,  64) per-entry monotone */
	ClusterGrdHolderId master_holder;		 /* 24B [ 64,  88) 4-tuple identity */
	/* PGRAC: spec-2.36 D5 HC117 NEW — S barrier reader starvation guard.
	 * pending_x_requester_node:	-1 = none; otherwise cluster_node_id
	 *								of the X requester whose request is in
	 *								flight (master-side broadcast pending).
	 *								Read by N→S handler to short-circuit
	 *								with DENIED_PENDING_X reply (HC117).
	 * pending_x_since_lsn:		legacy requests store a low-63-bit XLogCtl
	 *								LSN for observability; PCM-X queue heads
	 *								store high-bit|ticket_id as an exact cookie.
	 *								Decode the namespace before displaying it;
	 *								never use either form for timeout math.
	 * Cleared paths: (a) X grant install ack;  (b) reconfig epoch advance;
	 * (c) HC124 LMON node-dead sweep when requester crashes. */
	int32 pending_x_requester_node; /*  4B [ 88,  92) -1 = none */
	int32 _pad_pending_x;			/*  4B [ 92,  96) 8B align */
	uint64 pending_x_since_lsn;		/*  8B [ 96, 104) LSN or queue cookie */
	/* PGRAC: spec-2.37 D2 HC125+HC126 NEW — single max-historical PI watermark.
	 *
	 *	pi_watermark_lsn:	max(holder_last_page_lsn) observed across all
	 *						X→N / X→S downgrade events for this tag.
	 *						InvalidXLogRecPtr(0) = no historical PI.
	 *						Used by GCS block ship path to detect lost-write:
	 *						- master direct ship: produce_reply 校 page_lsn ≥
	 *						  pi_watermark_lsn, 失败 → DENIED_LOST_WRITE
	 *						- forward path: master stamps expected into
	 *						  GcsBlockForwardPayload.expected_pi_watermark_
	 *						  lsn_bytes[8], holder copy 后校 同 condition.
	 *
	 *	Path X MVP: 用 page_lsn (PG-native, replay-correct) 而非 pd_block_scn
	 *	(后者需 WAL record schema 扩展 + 每 AM redo 改写, defer 独立 spec).
	 *
	 *	Retire 路径 (HC130 — 永禁 epoch-tied):
	 *	  (a) tag lifecycle: drop/truncate/relfilenode change 走 smgr hook
	 *	  (b) durable-confirm: checkpointer/smgr sync-complete (defer 到
	 *	      spec-2.38/Stage3, 本 spec 仅立 helper + counter, callsite 不 wire)
	 *
	 *	Inserted between pending_x_since_lsn and wait_cv to keep entry_lock
	 *	must-stay-last (LWLockPadded PG_CACHE_LINE_SIZE alignment). */
	uint64 pi_watermark_lsn; /*  8B [104, 112) spec-2.37 D2 HC125+HC126 */
	/* PGRAC: spec-2.41 D2 (§2.8 Option A) NEW — dual watermark.
	 *
	 *	pi_watermark_scn:	max(observed pd_block_scn) for this tag — the
	 *						lost-write DETECTOR's cross-node version authority
	 *						(global Lamport SCN, AD-008; comparable across
	 *						per-node WAL streams, unlike page_lsn).  Read ONLY
	 *						via cluster_pcm_lock_pi_watermark_scn_query.
	 *	pi_watermark_lsn (above):  RETAINED — the spec-4.7 D5 redo-coverage
	 *						serve-gate's per-stream replay-position (LSN).  Read
	 *						ONLY via cluster_pcm_lock_pi_watermark_lsn_query.
	 *	The two are ORTHOGONAL (§2.8.1): detector never reads lsn, serve-gate
	 *	never reads scn.  Both monotone-max, both cleared together on retire. */
	SCN pi_watermark_scn;	   /*  8B [112, 120) spec-2.41 D2 §2.8 Option A */
	/* Stage 8 R9 Resource-X executor state.  The entry's immutable tag is
	 * the assertion resource; requester_node completes the D6 assertion. */
	int32 resource_x_requester_node;
	uint32 resource_x_progress_flags;
	uint64 resource_x_formation;
	uint64 resource_x_acquisition_generation;
	/* AuthorityGrant and requester-target generations are independent axes. */
	uint64 resource_x_requester_base_generation;
	uint64 resource_x_retired_acquisition_generation;
	uint64 resource_x_no_progress_generation;
	uint32 resource_x_no_progress_reason;
	uint32 resource_x_dispatch_phase;
	ClusterPcmResourceXBootstrapRound resource_x_bootstrap_round;
	ClusterPcmResourceXLocalOwner resource_x_local_owner;
	pg_atomic_uint32 lifecycle;
	pg_atomic_uint32 pin_count;
	pg_atomic_uint32 wait_refcount;
	pg_atomic_uint32 transport_refcount;
	uint64 binding_generation;
	uint32 registry_slot;
	uint32 lifecycle_pad;
	ConditionVariable wait_cv; /* spec-2.31 D1 v0.4 incompatible state wait */
	LWLockPadded entry_lock;   /*128B PG_CACHE_LINE_SIZE — must stay last */
};

/*
 * PGRAC: spec-2.31 D1 v0.4 F2 / spec-2.36 D5 — GrdEntry size bumps.
 *
 *	spec-2.30 baseline:	216B (no s_holder_refcount_local, no wait_cv).
 *	spec-2.31 D1 v0.4:	232B (added s_holder_refcount_local 2B + 2B
 *						align;  + ConditionVariable wait_cv 12-16B;
 *						+ LWLockPadded re-alignment).
 *	spec-2.36 D5 v0.3:	248B (added pending_x_requester_node 4B +
 *						_pad_pending_x 4B + pending_x_since_lsn 8B
 *						= +16B for HC117 S barrier;  inserted between
 *						master_holder and wait_cv).
 *
 *	`sizeof(ConditionVariable)` depends on platform alignment;  the
 *	assertion fires if the actual measured size diverges from the
 *	expected constant on this build platform, so silent layout drift
 *	(e.g. a future struct change in a dependency) cannot slip past CI.
 */
StaticAssertDecl(sizeof(struct GrdEntry) == 1056,
				 "Stage 8 D2 GrdEntry size must include pinned binding identity");


/*
 * PGRAC: spec-2.30 D2 — shmem header for module-wide atomic counters.
 *
 *	The 9 transition counters must be visible to every backend (not
 *	process-local) so dump_pcm / accessor SQL surface returns
 *	cluster-wide values, not per-process zero readings.  Lives in the
 *	'pgrac cluster pcm grd' shmem region as a header prefix before the
 *	GrdEntry array.
 *
 *	The embedded HTAB LWLock serializes dynahash lookups/inserts/iteration.
 *	Per-entry locks protect entry-local state after a stable pointer has been
 *	obtained; they do not make concurrent HASH_ENTER_NULL safe by themselves.
 */
/* D4 makes both bounded sidecar tables reusable without breaking their
 * open-addressing probe chains. */
#define CLUSTER_PCM_WM_PROV_SLOTS 8192

typedef enum PcmSidecarSlotState {
	PCM_SIDECAR_EMPTY = 0,
	PCM_SIDECAR_LIVE,
	PCM_SIDECAR_TOMBSTONE
} PcmSidecarSlotState;

typedef struct ClusterPcmWmProvSlot {
	uint8 state;  /* PcmSidecarSlotState */
	uint8 source; /* ClusterPcmWmSrc */
	uint16 reserved;
	int32 sender_node; /* -1 = local/unknown */
	BufferTag tag;	   /* 20B */
	int32 _pad2;
	uint64 binding_generation;
	SCN old_scn;	   /* watermark before the advancing feed */
	SCN new_scn;	   /* fed page_scn == watermark right after the feed */
	uint64 request_id; /* wire identity (0 = none) */
	uint64 epoch;	   /* wire epoch (0 = none) */
} ClusterPcmWmProvSlot;

StaticAssertDecl(sizeof(ClusterPcmWmProvSlot) == 72,
				 "D4 watermark provenance slot must bind object generation");

typedef enum PcmRegistrySlotState {
	PCM_REGISTRY_EMPTY = 0,
	PCM_REGISTRY_LIVE,
	PCM_REGISTRY_TOMBSTONE
} PcmRegistrySlotState;

/* Stable, non-authoritative index for bounded R8 traversal.  Resource-X
 * state stays solely in the keyed GrdEntry; this registry stores only the
 * immutable BufferTag needed to resolve one cursor slot without retaining a
 * raw hash pointer across calls. */
typedef struct ClusterPcmResourceXSlot {
	BufferTag tag;
	uint32 state;
	uint32 reserved;
	uint64 binding_generation;
	uint64 retired_authority_generation;
} ClusterPcmResourceXSlot;

StaticAssertDecl(sizeof(ClusterPcmResourceXSlot) == 48,
				 "D4 Resource-X registry slot must preserve tombstone authority floor");

typedef struct ClusterPcmResourceXMasterRequest {
	uint64 base_authority_generation;
	uint64 resource_formation;
	uint64 master_session_incarnation;
	uint64 assertion_sequence;
	uint64 enqueue_order;
	uint64 final_authority_generation;
	/* Master-local physical lineage frozen exactly once when this request
	 * becomes the canonical FIFO head.  It is freshness evidence only, never
	 * Resource-X authority and never a wire field. */
	uint64 head_transition_generation;
	uint64 source_carrier_generation;
	uint64 requester_target_generation;
	uint64 page_scn_lsn;
	uint64 holder_connection_generation;
	uint64 acting_formation;
	uint64 local_holder_authority_generation;
	uint64 requester_connection_generation;
	uint64 local_proof_generation;
	uint64 settlement_requester_connection_generation;
	uint64 dependencies[RESOURCE_X_DEPENDENCY_MAX];
	uint32 ordered_lane;
	uint32 sender_connection_generation;
	uint32 incompatible_holders_bitmap;
	uint32 blocked_holders_bitmap;
	uint32 source_proof_crc32c;
	uint32 page_checksum;
	uint32 dependency_vector_crc32c;
	uint8 source_fence[RESOURCE_X_SOURCE_FENCE_BYTES];
	int32 source_node;
	uint16 dependency_count;
	uint16 proof_flags;
	uint8 phase;
	uint8 proof_kind;
	uint8 source_disposition;
	uint8 reserved;
} ClusterPcmResourceXMasterRequest;

#define RESOURCE_X_SOURCE_SETTLEMENT_NONE UINT8_C(0)
#define RESOURCE_X_SOURCE_SETTLEMENT_PENDING UINT8_C(1)
#define RESOURCE_X_SOURCE_SETTLEMENT_ACKED UINT8_C(2)

/* One bounded latest-terminal replay record per resource.  This is retained
 * conversion history only: it is never a queue member, holder authority, PI
 * selector, or proof source. */
typedef struct ClusterPcmResourceXTerminalTombstone {
	ClusterPcmResourceXMasterRequest request;
	int32 requester_node;
	uint32 valid;
} ClusterPcmResourceXTerminalTombstone;

#define RESOURCE_X_BOOTSTRAP_RECEIPT_EMPTY UINT8_C(0)
#define RESOURCE_X_BOOTSTRAP_RECEIPT_RECEIVED UINT8_C(1)
#define RESOURCE_X_BOOTSTRAP_RECEIPT_CONSUMED_BY_ASSERT UINT8_C(2)
#define RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY UINT8_C(0)
#define RESOURCE_X_BOOTSTRAP_PRIORITY_NEXT_ADMISSION UINT8_C(1)

/* PGRAC adaptation: one bounded non-authority successor identity per
 * resource.  It retains neither an ACK nor a sampled authority base. */
typedef struct ClusterPcmResourceXBootstrapPriority {
	ResourceXDecodedCommon request;
	uint64 r4_record_generation;
	uint32 authenticated_ingress_connection_generation;
	uint32 master_sender_connection_generation;
	uint8 state;
	uint8 reserved[7];
} ClusterPcmResourceXBootstrapPriority;

/* PGRAC adaptation: fixed, non-authority master receipt for one requester.
 * highest_attempt_floor survives binding invalidation; every other byte is
 * cleared when the receipt becomes unjoinable. */
typedef struct ClusterPcmResourceXBootstrapReceipt {
	ResourceXDecodedCommon request;
	ResourceXDecodedCommon ack;
	uint64 highest_attempt_floor;
	uint64 sampled_base;
	uint64 r4_record_generation;
	uint32 authenticated_ingress_connection_generation;
	uint8 state;
	uint8 reserved[3];
} ClusterPcmResourceXBootstrapReceipt;

typedef struct ClusterPcmResourceXControlIntent {
	ResourceXIntentSlot slot;
	uint8 payload[RESOURCE_X_PROOF_V1_BYTES];
} ClusterPcmResourceXControlIntent;

typedef struct ClusterPcmResourceXSourceSettlement {
	ClusterPcmResourceXControlIntent intent;
	uint8 state;
	uint8 reserved[7];
} ClusterPcmResourceXSourceSettlement;

typedef struct ClusterPcmResourceXBlockIntent {
	ResourceXIntentSlot slot;
	uint8 payload[RESOURCE_X_CONTROL_V1_BYTES];
} ClusterPcmResourceXBlockIntent;

/* The retained records themselves carry the monotonic pair publication
 * witness.  PENDING bytes are transport-invisible; PUBLISHED proves both
 * existing intent slots were armed together before their independent DATA
 * drain progress began. */
#define RESOURCE_X_HOLDER_PAIR_PENDING UINT8_C(1)
#define RESOURCE_X_HOLDER_PAIR_PUBLISHED UINT8_C(2)

typedef struct ClusterPcmResourceXHolderStatus {
	ResourceXIntentBodyHandle body;
	uint64 logical_generation;
	uint64 authority_generation;
	uint64 resource_formation;
	uint32 destination_node;
	uint16 payload_bytes;
	uint8 kind;
	uint8 valid;
	uint8 payload[RESOURCE_X_PROOF_V1_BYTES];
} ClusterPcmResourceXHolderStatus;

typedef struct ClusterPcmResourceXHolderImage {
	ResourceXIntentBodyHandle body;
	uint64 logical_generation;
	uint64 authority_generation;
	uint64 resource_formation;
	uint32 destination_node;
	uint16 payload_bytes;
	uint8 kind;
	uint8 valid;
	uint8 payload[RESOURCE_X_IMAGE_V1_BYTES];
} ClusterPcmResourceXHolderImage;

typedef struct ClusterPcmResourceXRequesterJoin {
	uint64 t_image_us;
	uint64 t_grant_us;
	uint64 t_install_us;
	int32 grant_source_node;
	int32 image_source_node;
	uint16 grant_payload_bytes;
	uint16 image_payload_bytes;
	uint8 grant_valid;
	uint8 image_valid;
	uint8 proof_kind;
	uint8 install_succeeded;
	uint8 settled;
	uint8 requester_loss_seen;
	uint8 reserved[2];
	uint32 image_semantic_crc32c;
	uint32 crc_reserved;
	uint8 grant_payload[RESOURCE_X_PROOF_V1_BYTES];
	uint8 image_payload[RESOURCE_X_IMAGE_V1_BYTES];
} ClusterPcmResourceXRequesterJoin;

typedef struct ClusterPcmResourceXMasterState {
	uint64 binding_generation;
	uint64 authority_generation;
	uint64 next_enqueue_order;
	/* Local cleanup tombstone for the serial type-18/type-15 carrier pair.
	 * It is not grant authority: it only makes an authenticated, exact DRAIN
	 * replay idempotent after the physical N+PI carrier has been released. */
	uint64 holder_pair_drained_sequences[RESOURCE_X_PROTOCOL_NODE_LIMIT];
	uint64 holder_pair_drained_resource_formation;
	uint64 holder_pair_drained_master_session;
	int32 holder_pair_drained_master_node;
	uint32 holder_pair_drained_reserved;
	ClusterPcmResourceXBootstrapPriority bootstrap_priority;
	ClusterPcmResourceXBootstrapReceipt
		bootstrap_receipts[RESOURCE_X_PROTOCOL_NODE_LIMIT];
	ClusterPcmResourceXMasterRequest requests[RESOURCE_X_PROTOCOL_NODE_LIMIT];
	ClusterPcmResourceXTerminalTombstone terminal_tombstone;
	ClusterPcmResourceXBlockIntent block_intents[RESOURCE_X_PROTOCOL_NODE_LIMIT];
	ClusterPcmResourceXHolderStatus holder_status;
	ClusterPcmResourceXControlIntent holder_status_intent;
	ClusterPcmResourceXControlIntent grant_intent;
	ClusterPcmResourceXRequesterJoin requester_join;
	ClusterPcmResourceXControlIntent requester_settlement_intent;
	ClusterPcmResourceXSourceSettlement source_settlement;
	ClusterPcmResourceXHolderImage holder_image;
	ResourceXIntentSlot holder_image_intent;
} ClusterPcmResourceXMasterState;

StaticAssertDecl(sizeof(ClusterPcmResourceXMasterRequest) == 336,
				 "Resource-X master request layout must remain 336 bytes");
StaticAssertDecl(sizeof(ClusterPcmResourceXTerminalTombstone) == 344,
				 "Resource-X terminal tombstone layout must remain 344 bytes");
StaticAssertDecl(sizeof(ClusterPcmResourceXBootstrapPriority) == 120,
				 "Resource-X bootstrap priority layout must remain 120 bytes");
StaticAssertDecl(sizeof(ClusterPcmResourceXBootstrapReceipt) == 224,
				 "Resource-X bootstrap receipt layout must remain 224 bytes");
StaticAssertDecl(sizeof(ClusterPcmResourceXControlIntent) == 392,
				 "Resource-X control intent layout must remain 392 bytes");
StaticAssertDecl(sizeof(ClusterPcmResourceXSourceSettlement) == 400,
				 "Resource-X source settlement debt layout must remain 400 bytes");
StaticAssertDecl(sizeof(ClusterPcmResourceXBlockIntent) == 176,
				 "Resource-X block intent layout must remain 176 bytes");
StaticAssertDecl(sizeof(ClusterPcmResourceXHolderStatus) == 384,
				 "Resource-X holder status layout must remain 384 bytes");
StaticAssertDecl(sizeof(ClusterPcmResourceXHolderImage) == 8592,
				 "Resource-X holder image layout must remain 8592 bytes");
StaticAssertDecl(sizeof(ClusterPcmResourceXRequesterJoin) == 8888,
				 "Resource-X requester join layout must remain 8888 bytes");
StaticAssertDecl(sizeof(ClusterPcmResourceXMasterState) == 43840,
				 "D4 Resource-X master state must bind object generation");

typedef struct ClusterPcmShared {
	LWLockPadded htab_lock;
	pg_atomic_uint64 next_binding_generation;
	pg_atomic_uint64 resource_x_retired_authority_floor;
	pg_atomic_uint64 resource_x_bootstrap_next_attempt;
	pg_atomic_uint32 reclaim_requested;
	uint32 entry_lifecycle_pad;
	pg_atomic_uint64 reclaim_scan_cursor;
	pg_atomic_uint64 reclaim_attempt_count;
	pg_atomic_uint64 reclaim_success_count;
	pg_atomic_uint64 reclaim_reuse_count;
	pg_atomic_uint64 reclaim_capacity_retry_count;
	pg_atomic_uint64 reclaim_capacity_fail_count;
	pg_atomic_uint64 reclaim_peak_live_entries;
	pg_atomic_uint64 reclaim_tombstone_count;
	pg_atomic_uint64 reclaim_refused[PCM_RETIRE_REFUSAL_N];
	pg_atomic_uint32 resource_x_gate_phase;
	uint32 resource_x_gate_pad;
	pg_atomic_uint64 resource_x_gate_formation;
	pg_atomic_uint64 resource_x_freeze_generation;
	pg_atomic_uint64 resource_x_activation_inflight_count;
	pg_atomic_uint64 resource_x_reconfig_old_formation;
	pg_atomic_uint64 resource_x_reconfig_new_formation;
	pg_atomic_uint32 resource_x_reconfig_dead_requester_bitmap;
	uint32 resource_x_reconfig_dead_requester_pad;
	pg_atomic_uint64 resource_x_reconfig_next_state_index;
	pg_atomic_uint64 resource_x_reconfig_scan_capacity;
	pg_atomic_uint64 resource_x_reconfig_zero_proof_generation;
	pg_atomic_uint64 resource_x_reconfig_residual_count;
	pg_atomic_uint64 resource_x_semantic_mutation_sequence;
	pg_atomic_uint32 resource_x_reconfig_proof_pass;
	uint32 resource_x_reconfig_proof_pad;
	pg_atomic_uint64 resource_x_reconfig_proof_begin_sequence;
	pg_atomic_uint64 resource_x_reconfig_proof_begin_slot_count;
	pg_atomic_uint64 resource_x_reconfig_proof_digest;
	pg_atomic_uint64 resource_x_reconfig_proof_empty_slot_count;
	pg_atomic_uint64 resource_x_reconfig_proof_successor_slot_count;
	pg_atomic_uint64 resource_x_reconfig_proof_terminal_slot_count;
	pg_atomic_uint64 resource_x_reconfig_proof_retry_revisit_count;
	ResourceXZeroResidualProof resource_x_zero_residual_proof;
	ResourceXCleanCompletionProof resource_x_clean_completion_proof;
	pg_atomic_uint64 resource_x_reconfig_freeze_count;
	pg_atomic_uint64 resource_x_reconfig_slot_examined_count;
	pg_atomic_uint64 resource_x_reconfig_old_detached_count;
	pg_atomic_uint64 resource_x_reconfig_successor_count;
	pg_atomic_uint64 resource_x_reconfig_orphan_count;
	pg_atomic_uint64 resource_x_reconfig_sidecar_neutralized_count;
	pg_atomic_uint64 resource_x_reconfig_sidecar_stale_count;
	pg_atomic_uint64 resource_x_reconfig_retry_count;
	pg_atomic_uint64 resource_x_reconfig_blocked_count;
	/* Node-wide counters shared by foreground and asynchronous reply owners. */
	pg_atomic_uint64 block0_reply_wait_count[CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT]
											[CLUSTER_UNDO_BLOCK0_WAIT_METRIC_COUNT];
	pg_atomic_uint64 resource_x_reconfig_thaw_count;
	pg_atomic_uint64 resource_x_reconfig_reclaim_nonhead_count;
	pg_atomic_uint64 resource_x_reconfig_reclaim_head_count;
	pg_atomic_uint64 resource_x_reconfig_reclaim_orphan_count;
	pg_atomic_uint64 resource_x_reconfig_slot_count;
	pg_atomic_uint64 resource_x_intent_arm_generation;
	pg_atomic_uint64 resource_x_intent_scan_generation;
	pg_atomic_uint64 resource_x_intent_completed_generation;
	pg_atomic_uint64 resource_x_intent_next_state_index;
	pg_atomic_uint32 resource_x_intent_next_owner_index;
	pg_atomic_uint32 resource_x_intent_generation_exhausted;
	pg_atomic_uint64 resource_x_o1_remote_install_observed_count;
	pg_atomic_uint64 resource_x_o1_remote_grant_after_image_count;
	pg_atomic_uint64 resource_x_o1_remote_image_at_or_after_grant_count;
	pg_atomic_uint64 resource_x_o1_remote_episode_excluded_no_install;
	pg_atomic_uint64 resource_x_o1_remote_episode_excluded_missing_grant;
	pg_atomic_uint64 resource_x_o1_remote_episode_excluded_missing_image;
	pg_atomic_uint64 resource_x_o1_last_remote_t_image_us;
	pg_atomic_uint64 resource_x_o1_last_remote_t_grant_us;
	pg_atomic_uint64 resource_x_o1_last_remote_t_install_us;
	pg_atomic_uint64 trans_n_to_s_count;
	pg_atomic_uint64 trans_n_to_x_count;
	pg_atomic_uint64 trans_s_to_x_upgrade_count;
	pg_atomic_uint64 trans_x_to_s_downgrade_count;
	pg_atomic_uint64 trans_x_to_n_downgrade_count;
	pg_atomic_uint64 trans_x_to_n_release_count;
	pg_atomic_uint64 trans_s_to_n_invalidate_count;
	pg_atomic_uint64 trans_s_to_n_release_count;
	pg_atomic_uint64 trans_s_to_x_cleanout_count; /* HC60 永 0 in spec-2.30 */
	/* PGRAC: spec-6.14a D2 — local-master X-vs-remote-S arm: writer had no
	 * local S residency, no provable-current carrier -> fail-closed count. */
	pg_atomic_uint64 local_s_revoke_nonholder_failclosed_count;
	/* PGRAC: spec-6.14 D5 — aux-context eviction (KO flush drain) could not
	 * ride the GCS request wire; remote S release deferred, master keeps a
	 * phantom-holder bit until the next acquire / GRD reclaim. */
	pg_atomic_uint64 evict_release_deferred_aux_count;
	/* PGRAC ownership-generation wave: the cached-X writer re-verify.
	 * cover_stale_detected = a writer took the cached-cover fast path and, after
	 * taking the content lock, found the ownership generation changed / no longer
	 * covering / pending|revoking (a BAST X->S or any ownership round raced the
	 * content-lock window).  reverify_reacquire = of those, the ones that fell
	 * back to a real master re-acquire (the fix ACTION; detected without the
	 * action is the pre-fix bug surface). */
	pg_atomic_uint64 writer_cover_stale_detected_count;
	pg_atomic_uint64 writer_reverify_reacquire_count;
	pg_atomic_uint64 restore_aba_detected_count;
	pg_atomic_uint64 invalidate_parked_grant_pending_count;
	/* S3 forensics step 1b — per-tag SCN-watermark advance provenance table
	 * (see cluster_pcm_lock.h ClusterPcmWmProv).  Replaces the step-1a
	 * 256-slot ring: under S3 fan-out the ring recycled in milliseconds, so
	 * by the time a 53R93 emitted, the tag's advancing feed was overwritten
	 * and the query returned an unrelated late feed.  Open-addressed and
	 * generation-bound; one LIVE slot per binding holds the LAST feed that
	 * actually advanced the monotone-max watermark (non-advancing feeds
	 * never enter), while retire leaves a reusable TOMBSTONE so probe chains
	 * remain valid and a later binding inherits no provenance.
	 * Writers hold the tag's GRD entry_lock and take wm_prov_lock EXCLUSIVE
	 * around probe+store (entry_lock -> wm_prov_lock, strict leaf);  readers
	 * probe under SHARED.  When the table fills, further first-feeds are
	 * dropped and counted (wm_prov_insert_fail_count) so a query reports
	 * "absence inconclusive" instead of a false NONE. */
	LWLockPadded wm_prov_lock;
	pg_atomic_uint64 wm_prov_insert_fail_count;
	ClusterPcmWmProvSlot wm_prov[CLUSTER_PCM_WM_PROV_SLOTS];
} ClusterPcmShared;

StaticAssertDecl(sizeof(ClusterPcmShared) >= sizeof(LWLockPadded) + 72,
				 "spec-2.30 D2 ClusterPcmShared carries htab lock plus 9 counters");

/*
 * Module-level shmem pointers (set in init_fn).
 *
 *	ClusterPcm        — header(9 atomic counters)+ lock-free read by accessors
 *	cluster_pcm_htab  — HTAB keyed by BufferTag(20B);  HC59 lazy-alloc entries
 *	                    on first cluster_pcm_lock_acquire(tag, mode);  entries
 *	                    never freed until cluster shutdown.
 */
static ClusterPcmShared *ClusterPcm = NULL;
static HTAB *cluster_pcm_htab = NULL;
static ClusterPcmResourceXSlot *cluster_pcm_resource_x_slots = NULL;
static ClusterPcmResourceXMasterState *cluster_pcm_resource_x_master_states = NULL;
/*
 * Resolved (post-HC62) entry count used by HTAB cap + accessor + errmsg.
 *	Set in cluster_pcm_grd_init from pcm_grd_effective_entries(true) ;
 *	0 means disabled.  Reading the raw GUC `cluster_pcm_grd_max_entries`
 *	is fine for show / dump_pcm but NOT for sizing logic (which must use
 *	the resolved value post HC62 fail-closed checks).
 */
static int pcm_grd_effective = 0;


/* Forward decl — file-private HTAB directory helpers defined below init_fn. */
static bool pcm_entry_binding_generation_next(uint64 *generation_out);
static void pcm_entry_reclaim_request(void);
static void pcm_entry_retire_refused_mark(PcmRetireRefusal why);
/* S3 forensics step 1b — watermark-advance provenance recorder (defined with
 * the watermark helpers below; used by the inline writer sites above them).
 * Caller MUST hold the tag's GRD entry_lock and call ONLY on actual advance. */
static void pcm_wm_prov_record(BufferTag tag, uint64 binding_generation,
	SCN old_scn, SCN new_scn, ClusterPcmWmSrc source,
	int32 sender_node, uint64 request_id, uint64 epoch);
static bool pcm_wm_prov_validate_retire_exact_locked(const BufferTag *tag,
	uint64 binding_generation, ClusterPcmWmProvSlot **slot_out);
static void pcm_entry_lock_exclusive(struct GrdEntry *entry);
static ClusterPcmResourceXMasterState *pcm_resource_x_master_state_for_entry(
	const struct GrdEntry *entry);
static ClusterPcmResourceXMasterRequest *pcm_resource_x_master_head(
	ClusterPcmResourceXMasterState *state, int32 *requester_node_out);
static bool pcm_resource_x_s_barrier_active_locked(struct GrdEntry *entry);
static bool pcm_resource_x_bootstrap_receipt_valid(
	const ClusterPcmResourceXBootstrapReceipt *receipt);
static bool pcm_resource_x_local_owner_handle_valid(
	const ResourceXLocalOwnerHandle *handle);
static void pcm_resource_x_bootstrap_receipt_invalidate(
	ClusterPcmResourceXBootstrapReceipt *receipt);
static bool pcm_resource_x_bootstrap_priority_valid(
	const ClusterPcmResourceXBootstrapPriority *priority);
static void pcm_resource_x_bootstrap_priority_clear(
	ClusterPcmResourceXBootstrapPriority *priority);
static uint64 pcm_resource_x_source_fence_get_u64(const uint8 *bytes);
static ResourceXApplyResult
pcm_resource_x_holder_pair_precedes_local_bootstrap_locked(
	struct GrdEntry *entry, int32 current_master_node,
	uint64 current_master_session, uint64 current_formation,
	uint64 expected_carrier_generation);
static bool pcm_resource_x_s_predecessor_superseded_unbound_wait_locked(
	struct GrdEntry *entry, const ResourceXAssertion *assertion,
	int32 current_master_node, uint64 current_master_session,
	uint64 current_formation);
static bool pcm_resource_x_common_equal(
	const ResourceXDecodedCommon *left,
	const ResourceXDecodedCommon *right);
static void pcm_resource_x_bootstrap_round_clear_binding_locked(
	ClusterPcmResourceXBootstrapRound *round);
static bool pcm_resource_x_local_owner_valid_locked(
	const ClusterPcmResourceXLocalOwner *owner);
static bool pcm_resource_x_local_handoff_valid(
	const ClusterPcmResourceXLocalHandoff *handoff);
static bool pcm_resource_x_local_owner_priority_clear_locked(struct GrdEntry *entry);
static bool pcm_resource_x_local_owner_expire_locked(struct GrdEntry *entry, uint64 now_us);
static uint64 pcm_resource_x_monotonic_us(void);
static bool pcm_resource_x_head_changed_locked(struct GrdEntry *entry, bool semantic_progress,
											   uint64 now_us);
static bool pcm_resource_x_head_progress_ref_locked(struct GrdEntry *entry,
													const ResourceXAcquisitionRef *ref,
													uint64 now_us);
static void pcm_resource_x_head_fail_locked(struct GrdEntry *entry, uint8 reason);
static uint32 pcm_holder_bit(int holder_node_id);
static PcmState pcm_transition_target(PcmLockTransition trans);
static ResourceXReclaimResult pcm_resource_x_reclaim_requester_locked(
	struct GrdEntry *entry, ClusterPcmResourceXMasterState *state,
	int32 dead_node, uint64 dead_formation, ResourceXReclaimWitness *out,
	bool *broadcast_out);
static bool pcm_resource_x_arm_block_intents_locked(
	struct GrdEntry *entry, ClusterPcmResourceXMasterState *state,
	ClusterPcmResourceXMasterRequest *request, int32 requester_node);
static ResourceXApplyResult pcm_resource_x_build_authority_grant_locked(
	struct GrdEntry *entry,
	const ClusterPcmResourceXMasterRequest *request,
	int32 requester_node, uint64 final_authority_generation,
	ResourceXDecodedFrame *out);
static bool pcm_resource_x_redrive_grant_intent_locked(
	struct GrdEntry *entry, ClusterPcmResourceXMasterState *state,
	ClusterPcmResourceXMasterRequest *request, int32 requester_node);
typedef struct PcmResourceXRetirementWitness {
	bool had_join;
	bool already_settled;
	uint8 proof_kind;
	uint8 install_succeeded;
	uint8 requester_loss_seen;
	uint8 reserved[3];
	uint64 final_authority_generation;
	uint64 t_image_us;
	uint64 t_grant_us;
	uint64 t_install_us;
} PcmResourceXRetirementWitness;

static ResourceXApplyResult pcm_resource_x_requester_join_bindable_locked(
	struct GrdEntry *entry, const ResourceXAcquisitionRef *ref);
static bool pcm_resource_x_active_empty_locked(const struct GrdEntry *entry);
static bool pcm_resource_x_active_valid_locked(const struct GrdEntry *entry);
static bool pcm_resource_x_requester_join_empty_locked(
	const ClusterPcmResourceXRequesterJoin *join);
static bool pcm_resource_x_requester_join_decode_locked(
	const ClusterPcmResourceXRequesterJoin *join,
	ResourceXDecodedFrame *grant_out, ResourceXDecodedFrame *image_out);
static bool pcm_resource_x_requester_join_snapshot_locked(
	const ClusterPcmResourceXRequesterJoin *join,
	ResourceXRequesterJoinSnapshot *out);
static bool pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
	struct GrdEntry *entry,
	const ClusterPcmResourceXBootstrapRound *round,
	const ResourceXAcquisitionRef *ref,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint64 cached_ownership_generation);
static bool pcm_resource_x_requester_join_live_round_base_exact_locked(
	struct GrdEntry *entry, const ResourceXDecodedFrame *frame,
	int32 authenticated_source_node, uint64 requester_target_generation,
	uint64 now_us);
static ResourceXApplyResult pcm_resource_x_requester_retirement_prepare_locked(
	struct GrdEntry *entry, const ResourceXAcquisitionRef *ref,
	bool install_succeeded, bool requester_loss_seen,
	PcmResourceXRetirementWitness *witness_out);
static bool pcm_resource_x_requester_settlement_arm_locked(
	struct GrdEntry *entry, const ResourceXAcquisitionRef *ref,
	const PcmResourceXRetirementWitness *witness);
static void pcm_resource_x_requester_retirement_commit_locked(
	struct GrdEntry *entry, const ResourceXAcquisitionRef *ref,
	const PcmResourceXRetirementWitness *witness);


/* ============================================================
 * PGRAC: spec-2.30 D2 — transition validator + apply.
 *
 *	cluster_pcm_transition_legal(from, to, trans):  returns true iff
 *	  (from, to, trans) combination matches AD-002 9-transition map.
 *	  HC56 caller invokes before apply;  illegal combination MUST
 *	  ereport(ERROR, ERRCODE_DATA_CORRUPTED) at caller side.
 *
 *	cluster_pcm_transition_apply(entry, trans, holder_node_id):
 *	  caller MUST hold entry->entry_lock EXCLUSIVE (HC57 enforced via
 *	  Assert(LWLockHeldByMeInMode));  applies transition body (master_state
 *	  CAS + holder bitmap mutation);  bumps
 *	  per-entry transition_count_local + module-level transition counter.
 *	  Trans-9 fail-closed ereport (HC60).
 * ============================================================ */
bool
cluster_pcm_transition_legal(PcmState from, PcmState to, PcmLockTransition trans)
{
	/*
	 * Switch on trans, verify (from, to) matches AD-002 map.
	 *
	 *	1 N→S  / 2 N→X  / 3 S→X(upgrade)  / 4 X→S(downgrade)  / 5 X→N(downgrade)
	 *	6 X→N(release)  / 7 S→N(invalidate)  / 8 S→N(release)  / 9 S→X(cleanout)
	 */
	switch (trans) {
	case PCM_TRANS_N_TO_S:
		return from == PCM_STATE_N && to == PCM_STATE_S;
	case PCM_TRANS_N_TO_X:
		return from == PCM_STATE_N && to == PCM_STATE_X;
	case PCM_TRANS_S_TO_X_UPGRADE:
		return from == PCM_STATE_S && to == PCM_STATE_X;
	case PCM_TRANS_X_TO_S_DOWNGRADE:
		return from == PCM_STATE_X && to == PCM_STATE_S;
	case PCM_TRANS_X_TO_N_DOWNGRADE:
		return from == PCM_STATE_X && to == PCM_STATE_N;
	case PCM_TRANS_X_TO_N_RELEASE:
		return from == PCM_STATE_X && to == PCM_STATE_N;
	case PCM_TRANS_S_TO_N_INVALIDATE:
		return from == PCM_STATE_S && to == PCM_STATE_N;
	case PCM_TRANS_S_TO_N_RELEASE:
		return from == PCM_STATE_S && to == PCM_STATE_N;
	case PCM_TRANS_S_TO_X_CLEANOUT:
		/*
			 * HC60 reachable-from-validator:  validator accepts as legal entry
			 * transition to keep enum complete;  apply body fail-closed.
			 */
		return from == PCM_STATE_S && to == PCM_STATE_X;
	}
	return false; /* unknown trans value */
}

static PcmState
pcm_transition_target(PcmLockTransition trans)
{
	switch (trans) {
	case PCM_TRANS_N_TO_S:
	case PCM_TRANS_X_TO_S_DOWNGRADE:
		return PCM_STATE_S;
	case PCM_TRANS_N_TO_X:
	case PCM_TRANS_S_TO_X_UPGRADE:
	case PCM_TRANS_S_TO_X_CLEANOUT:
		return PCM_STATE_X;
	case PCM_TRANS_X_TO_N_DOWNGRADE:
	case PCM_TRANS_X_TO_N_RELEASE:
	case PCM_TRANS_S_TO_N_INVALIDATE:
	case PCM_TRANS_S_TO_N_RELEASE:
		return PCM_STATE_N;
	}
	return PCM_STATE_N;
}


/*
 * PGRAC: spec-2.35 D3 (HC110) — master_holder lifecycle helpers.
 *
 *	master_holder is a 24B ClusterGrdHolderId 4-tuple (cluster_grd.h:
 *	{node_id, procno, cluster_epoch, request_id}).  HC110 forbids direct
 *	int-style assignment (cf. user codereview P1-2).  spec-2.35 only
 *	requires the node_id field for forward routing; procno / cluster_
 *	epoch / request_id remain opaque context for future specs (spec-2.36
 *	S→X invalidation broadcast may populate them).
 *
 *	Sentinel: node_id == INVALID_PCM_MASTER_HOLDER_NODE marks "no holder
 *	known".  Caller must check via cluster_pcm_master_holder_is_valid()
 *	before consuming the node_id.
 */
#define INVALID_PCM_MASTER_HOLDER_NODE ((uint32)UINT32_MAX)

static inline void
pcm_master_holder_set_node(struct GrdEntry *entry, int32 node_id)
{
	Assert(node_id >= 0 && node_id < 32);
	if (entry->master_holder.node_id == (uint32)node_id)
		return; /* no-op; do not bump lifecycle counter */
	entry->master_holder.node_id = (uint32)node_id;
	/* procno / cluster_epoch / request_id intentionally remain zero on a
	 * fresh entry; node_id starts at the no-holder sentinel and is set here.
	 * HC110. */
	cluster_gcs_block_bump_master_holder_lifecycle();
}

static inline void
pcm_master_holder_set_exact(struct GrdEntry *entry, const ClusterGrdHolderId *holder)
{
	if (memcmp(&entry->master_holder, holder, sizeof(*holder)) == 0)
		return;
	entry->master_holder = *holder;
	cluster_gcs_block_bump_master_holder_lifecycle();
}

static inline void
pcm_master_holder_clear(struct GrdEntry *entry)
{
	if (entry->master_holder.node_id == INVALID_PCM_MASTER_HOLDER_NODE)
		return; /* already cleared; no lifecycle event */
	memset(&entry->master_holder, 0, sizeof(ClusterGrdHolderId));
	entry->master_holder.node_id = INVALID_PCM_MASTER_HOLDER_NODE;
	cluster_gcs_block_bump_master_holder_lifecycle();
}

static inline bool
pcm_master_holder_is_valid(const struct GrdEntry *entry)
{
	return entry->master_holder.node_id != INVALID_PCM_MASTER_HOLDER_NODE;
}

static void
pcm_authority_snapshot_locked(struct GrdEntry *entry, PcmAuthoritySnapshot *out)
{
	memset(out, 0, sizeof(*out));
	out->master_holder = entry->master_holder;
	out->transition_count = pg_atomic_read_u64(&entry->transition_count_local);
	out->pending_x_since_lsn = entry->pending_x_since_lsn;
	out->state = (PcmState)pg_atomic_read_u32(&entry->master_state);
	out->x_holder_node = entry->x_holder_node;
	out->s_holders_bitmap = pg_atomic_read_u32(&entry->s_holders_bitmap);
	out->pending_x_requester_node = entry->pending_x_requester_node;
}

static bool
pcm_authority_snapshot_equal(const PcmAuthoritySnapshot *left, const PcmAuthoritySnapshot *right)
{
	return memcmp(&left->master_holder, &right->master_holder, sizeof(left->master_holder)) == 0
		   && left->transition_count == right->transition_count
		   && left->pending_x_since_lsn == right->pending_x_since_lsn && left->state == right->state
		   && left->x_holder_node == right->x_holder_node
		   && left->s_holders_bitmap == right->s_holders_bitmap
		   && left->pending_x_requester_node == right->pending_x_requester_node;
}

static inline int32
pcm_lowest_set_bit_node(uint32 bitmap)
{
	uint32 i;

	if (bitmap == 0)
		return -1;
	for (i = 0; i < 32; i++)
		if (bitmap & ((uint32)1u << i))
			return (int32)i;
	return -1;
}

static int32
pcm_resource_x_select_shared_current_carrier_locked(
	struct GrdEntry *entry, uint32 candidate_bitmap)
{
	uint32 local_bit;

	Assert(entry != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	if (candidate_bitmap == 0 || cluster_node_id < 0
		|| cluster_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return -1;
	local_bit = UINT32_C(1) << (uint32)cluster_node_id;
	if ((candidate_bitmap & local_bit) != 0)
		return cluster_node_id;
	return pcm_lowest_set_bit_node(candidate_bitmap);
}

/*
 * Public extern wrapper:  master-side ship source decision (spec-2.35 D6)
 * needs to know the master_holder.node_id of a tag's GrdEntry.  Returns
 * -1 if no GRD entry exists or the slot is unset.  Caller invokes after
 * cluster_pcm_lock_query(tag) returns S to decide whether forward is
 * possible.
 */
int32
cluster_pcm_master_holder_node_by_tag(BufferTag tag)
{
	struct GrdEntry *entry;
	bool found;
	int32 node_id = -1;

	if (cluster_pcm_htab == NULL)
		return -1;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL && pcm_master_holder_is_valid(entry))
		node_id = (int32)entry->master_holder.node_id;
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return node_id;
}

/*
 * Return a single-entry-lock view of the PCM authority.  A missing entry is
 * represented by false; out is still initialized to the canonical empty
 * sentinels so diagnostic callers never observe uninitialized bytes.
 */
bool
cluster_pcm_lock_authority_snapshot(BufferTag tag, PcmAuthoritySnapshot *out)
{
	struct GrdEntry *entry;
	bool found;

	if (out == NULL)
		return false;

	memset(out, 0, sizeof(*out));
	out->state = PCM_STATE_N;
	out->x_holder_node = -1;
	out->pending_x_requester_node = -1;
	out->master_holder.node_id = INVALID_PCM_MASTER_HOLDER_NODE;

	if (ClusterPcm == NULL || cluster_pcm_htab == NULL)
		return false;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
		pcm_authority_snapshot_locked(entry, out);
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return found && entry != NULL;
}

/*
 * Return the complete R4 master-route proof in one resource-entry lock
 * window.  The three outputs deliberately remain different authority
 * domains: the LMS/formation generation is a master routing fence, the
 * authority snapshot contains this resource's transition count, and the
 * watermark is the minimum current-page SCN.  No holder-local buffer
 * generation is manufactured here.
 *
 * A found but unusable entry is still returned verbatim.  The route owner,
 * not this read-only accessor, maps N/recovering/malformed holder shapes to
 * the closed ClusterCrBuildReason domain.
 */
bool
cluster_pcm_lock_r4_route_snapshot(BufferTag tag, PcmAuthoritySnapshot *authority_out,
								   uint64 *master_authority_generation_out,
								   SCN *expected_page_scn_out)
{
	struct GrdEntry *entry;
	bool found;

	if (authority_out != NULL) {
		memset(authority_out, 0, sizeof(*authority_out));
		authority_out->state = PCM_STATE_N;
		authority_out->x_holder_node = -1;
		authority_out->pending_x_requester_node = -1;
		authority_out->master_holder.node_id = INVALID_PCM_MASTER_HOLDER_NODE;
	}
	if (master_authority_generation_out != NULL)
		*master_authority_generation_out = 0;
	if (expected_page_scn_out != NULL)
		*expected_page_scn_out = InvalidScn;

	if (authority_out == NULL || master_authority_generation_out == NULL
		|| expected_page_scn_out == NULL || ClusterPcm == NULL || cluster_pcm_htab == NULL)
		return false;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
		pcm_authority_snapshot_locked(entry, authority_out);
		*master_authority_generation_out = cluster_lms_get_shard_master_generation();
		*expected_page_scn_out = entry->pi_watermark_scn;
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return found && entry != NULL;
}

bool
cluster_pcm_lock_authority_matches(BufferTag tag, const PcmAuthoritySnapshot *expected)
{
	struct GrdEntry *entry;
	PcmAuthoritySnapshot current;
	bool found;
	bool matches = false;

	if (expected == NULL || ClusterPcm == NULL || cluster_pcm_htab == NULL)
		return false;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
		pcm_authority_snapshot_locked(entry, &current);
		matches = pcm_authority_snapshot_equal(&current, expected);
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	return matches;
}

static bool
pcm_resource_x_ref_valid(const ResourceXAcquisitionRef *ref)
{
	return ref != NULL && resource_x_assertion_valid(&ref->assertion) && ref->formation != 0
		   && ref->formation != UINT64_MAX && ref->acquisition_generation != 0
		   && ref->acquisition_generation != UINT64_MAX;
}

static bool
pcm_resource_x_inflight_increment(void)
{
	uint64 current;

	if (ClusterPcm == NULL)
		return false;
	current = pg_atomic_read_u64(&ClusterPcm->resource_x_activation_inflight_count);
	for (;;) {
		if (current == UINT64_MAX)
			return false;
		if (pg_atomic_compare_exchange_u64(&ClusterPcm->resource_x_activation_inflight_count,
										   &current, current + 1))
			return true;
	}
}

static bool
pcm_resource_x_inflight_decrement(void)
{
	uint64 current;

	if (ClusterPcm == NULL)
		return false;
	current = pg_atomic_read_u64(&ClusterPcm->resource_x_activation_inflight_count);
	for (;;) {
		if (current == 0)
			return false;
		if (pg_atomic_compare_exchange_u64(&ClusterPcm->resource_x_activation_inflight_count,
										   &current, current - 1))
			return true;
	}
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_gate_bind_formation_exact(uint64 formation)
{
	uint64 current;

	if (ClusterPcm == NULL || formation == 0 || formation == UINT64_MAX)
		return RESOURCE_X_APPLY_INVALID;
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_OPEN)
		return RESOURCE_X_APPLY_BAD_STATE;
	current = pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (current == formation)
		return RESOURCE_X_APPLY_DUPLICATE;
	if (pg_atomic_read_u64(&ClusterPcm->resource_x_activation_inflight_count)
			!= 0)
		return RESOURCE_X_APPLY_BAD_STATE;
	if (current != 0)
		return RESOURCE_X_APPLY_STALE;

	current = 0;
	if (pg_atomic_compare_exchange_u64(&ClusterPcm->resource_x_gate_formation, &current,
										formation))
		return RESOURCE_X_APPLY_APPLIED;
	return current == formation ? RESOURCE_X_APPLY_DUPLICATE : RESOURCE_X_APPLY_STALE;
}

/*
 * R11 source-removal cutover is the sole consumer allowed to observe the
 * pristine, not-yet-authoritative gate image.  The ordinary gate snapshot
 * deliberately rejects formation zero because no Resource-X operation may
 * use it as authority.  This narrower snapshot accepts zero only while every
 * mutable reconfiguration field is still at its initial value; it therefore
 * proves readiness to bind the current R4 formation, not permission to read
 * or write a resource.
 */
bool
cluster_pcm_lock_resource_x_cutover_gate_snapshot_exact(
	ResourceXGateSnapshot *snapshot_out)
{
	ResourceXGateSnapshot snapshot;
	uint32 dead_requester_bitmap;
	uint32 phase_after;
	uint64 activation_inflight;
	uint64 reconfig_new_formation;
	uint64 reconfig_old_formation;
	int attempt;

	if (snapshot_out == NULL)
		return false;
	memset(snapshot_out, 0, sizeof(*snapshot_out));
	if (ClusterPcm == NULL)
		return false;

	for (attempt = 0; attempt < 8; attempt++) {
		memset(&snapshot, 0, sizeof(snapshot));
		snapshot.phase
			= pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase);
		pg_read_barrier();
		snapshot.formation
			= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
		snapshot.freeze_generation
			= pg_atomic_read_u64(&ClusterPcm->resource_x_freeze_generation);
		activation_inflight = pg_atomic_read_u64(
			&ClusterPcm->resource_x_activation_inflight_count);
		reconfig_old_formation = pg_atomic_read_u64(
			&ClusterPcm->resource_x_reconfig_old_formation);
		reconfig_new_formation = pg_atomic_read_u64(
			&ClusterPcm->resource_x_reconfig_new_formation);
		dead_requester_bitmap = pg_atomic_read_u32(
			&ClusterPcm->resource_x_reconfig_dead_requester_bitmap);
		pg_read_barrier();
		phase_after
			= pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase);
		if (snapshot.phase != phase_after)
			continue;
		*snapshot_out = snapshot;
		return snapshot.phase == RESOURCE_X_GATE_OPEN
			&& snapshot.formation != UINT64_MAX
			&& snapshot.freeze_generation == 0
			&& activation_inflight == 0
			&& reconfig_old_formation == 0
			&& reconfig_new_formation == 0
			&& dead_requester_bitmap == 0;
	}
	return false;
}

bool
cluster_pcm_lock_resource_x_gate_snapshot(ResourceXGateSnapshot *snapshot_out)
{
	ResourceXGateSnapshot snapshot;
	uint32 phase_after;
	int attempt;

	if (snapshot_out == NULL)
		return false;
	memset(snapshot_out, 0, sizeof(*snapshot_out));
	if (ClusterPcm == NULL)
		return false;

	for (attempt = 0; attempt < 8; attempt++) {
		memset(&snapshot, 0, sizeof(snapshot));
		snapshot.phase
			= pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase);
		pg_read_barrier();
		snapshot.formation
			= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
		snapshot.freeze_generation
			= pg_atomic_read_u64(&ClusterPcm->resource_x_freeze_generation);
		pg_read_barrier();
		phase_after
			= pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase);
		if (snapshot.phase != phase_after)
			continue;
		*snapshot_out = snapshot;
		return snapshot.formation != 0
			&& snapshot.formation != UINT64_MAX
			&& snapshot.freeze_generation != UINT64_MAX
			&& (snapshot.phase == RESOURCE_X_GATE_OPEN
				|| snapshot.phase == RESOURCE_X_GATE_FROZEN
				|| snapshot.phase == RESOURCE_X_GATE_RECOVERY_BLOCKED);
	}
	return false;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_gate_fail_closed_exact(
	const ResourceXGateSnapshot *expected)
{
	uint32 phase;

	if (ClusterPcm == NULL || expected == NULL
		|| expected->reserved != 0
		|| expected->formation == 0
		|| expected->formation == UINT64_MAX
		|| expected->freeze_generation == UINT64_MAX
		|| expected->phase != RESOURCE_X_GATE_OPEN)
		return RESOURCE_X_APPLY_INVALID;
	if (pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation)
			!= expected->formation
		|| pg_atomic_read_u64(&ClusterPcm->resource_x_freeze_generation)
			!= expected->freeze_generation)
		return RESOURCE_X_APPLY_STALE;

	phase = pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase);
	for (;;) {
		if (phase == RESOURCE_X_GATE_RECOVERY_BLOCKED)
			return RESOURCE_X_APPLY_DUPLICATE;
		if (phase != RESOURCE_X_GATE_OPEN)
			return RESOURCE_X_APPLY_STALE;
		if (pg_atomic_compare_exchange_u32(
				&ClusterPcm->resource_x_gate_phase, &phase,
				RESOURCE_X_GATE_RECOVERY_BLOCKED)) {
			pg_atomic_fetch_add_u64(
				&ClusterPcm->resource_x_reconfig_blocked_count, 1);
			return RESOURCE_X_APPLY_APPLIED;
		}
	}
}

bool
cluster_pcm_lock_resource_x_gate_open_exact(uint64 formation)
{
	return formation != 0 && formation != UINT64_MAX
		&& ClusterPcm != NULL
		&& pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			   == RESOURCE_X_GATE_OPEN
		&& pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation)
			   == formation;
}

bool
cluster_pcm_lock_resource_x_executor_enter(const ResourceXAcquisitionRef *ref,
										  ResourceXActivationGateToken *out_gate)
{
	uint32 phase;
	uint64 formation;

	if (out_gate == NULL)
		return false;
	memset(out_gate, 0, sizeof(*out_gate));
	if (!pcm_resource_x_ref_valid(ref) || !pcm_resource_x_inflight_increment())
		return false;

	phase = pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase);
	formation = pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (phase != RESOURCE_X_GATE_OPEN || formation != ref->formation) {
		(void)pcm_resource_x_inflight_decrement();
		return false;
	}

	out_gate->formation = formation;
	out_gate->freeze_generation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_freeze_generation);
	out_gate->acquisition_generation = ref->acquisition_generation;
	out_gate->active = 1;
	return true;
}

void
cluster_pcm_lock_resource_x_executor_leave(ResourceXActivationGateToken *gate)
{
	if (gate == NULL || gate->active == 0)
		return;
	gate->active = 0;
	(void)pcm_resource_x_inflight_decrement();
}

uint64
cluster_pcm_lock_resource_x_activation_inflight_count(void)
{
	return ClusterPcm == NULL
			   ? 0
			   : pg_atomic_read_u64(&ClusterPcm->resource_x_activation_inflight_count);
}

static bool
pcm_resource_x_reconfig_token_exact(const ResourceXReconfigToken *token)
{
	return token != NULL && token->old_formation != 0 && token->old_formation != UINT64_MAX
		   && token->new_formation != 0 && token->new_formation != UINT64_MAX
		   && token->new_formation != token->old_formation && token->freeze_generation != 0
		   && token->freeze_generation != UINT64_MAX
		   && token->old_formation
				  == pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_old_formation)
		   && token->new_formation
				  == pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_new_formation)
		   && token->dead_requester_bitmap
				  == pg_atomic_read_u32(
					  &ClusterPcm->resource_x_reconfig_dead_requester_bitmap)
		   && token->freeze_generation
				  == pg_atomic_read_u64(&ClusterPcm->resource_x_freeze_generation);
}

static bool
pcm_resource_x_reconfig_pending_token_exact(
	const ResourceXReconfigToken *token)
{
	return token != NULL
		&& token->old_formation != 0
		&& token->old_formation != UINT64_MAX
		&& token->new_formation == 0
		&& token->freeze_generation != 0
		&& token->freeze_generation != UINT64_MAX
		&& token->reserved == 0
		&& token->old_formation
		   == pg_atomic_read_u64(
			   &ClusterPcm->resource_x_reconfig_old_formation)
		&& pg_atomic_read_u64(
			   &ClusterPcm->resource_x_reconfig_new_formation) == 0
		&& token->dead_requester_bitmap
		   == pg_atomic_read_u32(
			   &ClusterPcm->resource_x_reconfig_dead_requester_bitmap)
		&& token->freeze_generation
		   == pg_atomic_read_u64(&ClusterPcm->resource_x_freeze_generation);
}

static void
pcm_resource_x_reconfig_block(void)
{
	uint32 phase;

	if (ClusterPcm == NULL)
		return;
	phase = pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase);
	while (phase != RESOURCE_X_GATE_RECOVERY_BLOCKED) {
		if (pg_atomic_compare_exchange_u32(&ClusterPcm->resource_x_gate_phase, &phase,
										   RESOURCE_X_GATE_RECOVERY_BLOCKED)) {
			pg_atomic_fetch_add_u64(&ClusterPcm->resource_x_reconfig_blocked_count, 1);
			return;
		}
	}
}

static void
pcm_resource_x_semantic_mutation_mark(void)
{
	uint64 current;

	if (ClusterPcm == NULL)
		return;
	/* No proof exists while ordinary admission is OPEN.  Restrict the global
	 * sequence cache line to the frozen episode so the normal PCM hot path
	 * pays only this phase read and never contends on a proof counter. */
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_FROZEN)
		return;
	current = pg_atomic_read_u64(
		&ClusterPcm->resource_x_semantic_mutation_sequence);
	for (;;) {
		if (current == 0 || current == UINT64_MAX) {
			pcm_resource_x_reconfig_block();
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("Resource-X semantic mutation sequence exhausted")));
		}
		if (pg_atomic_compare_exchange_u64(
				&ClusterPcm->resource_x_semantic_mutation_sequence,
				&current, current + 1))
			return;
	}
}

/* Entry create/retire changes the logical registry even while the gate is
 * OPEN.  Keep that rare lifecycle traffic on the proof mutation sequence;
 * ordinary PCM transitions retain the frozen-only hot-path helper above. */
static bool
pcm_entry_lifecycle_mutation_mark(void)
{
	uint64 current;

	if (ClusterPcm == NULL)
		return false;
	current = pg_atomic_read_u64(
		&ClusterPcm->resource_x_semantic_mutation_sequence);
	for (;;) {
		if (current == 0 || current == UINT64_MAX) {
			pcm_resource_x_reconfig_block();
			return false;
		}
		if (pg_atomic_compare_exchange_u64(
				&ClusterPcm->resource_x_semantic_mutation_sequence,
				&current, current + 1))
			return true;
	}
}

static void
pcm_entry_reclaim_request(void)
{
	uint32 expected = 0;

	if (ClusterPcm == NULL)
		return;
	/* A new requested episode starts a complete registry traversal at zero.
	 * Concurrent producers may both store zero before one publishes the bit;
	 * neither can reset an already-active scan. */
	if (pg_atomic_read_u32(&ClusterPcm->reclaim_requested) == 0) {
		pg_atomic_write_u64(&ClusterPcm->reclaim_scan_cursor, 0);
		(void)pg_atomic_compare_exchange_u32(
			&ClusterPcm->reclaim_requested, &expected, 1);
	}
}

static void
pcm_entry_retire_refused_mark(PcmRetireRefusal why)
{
	if (ClusterPcm == NULL || why <= PCM_RETIRE_REFUSAL_NONE
		|| why >= PCM_RETIRE_REFUSAL_N)
		return;
	pg_atomic_fetch_add_u64(&ClusterPcm->reclaim_refused[why], 1);
}

static void
pcm_atomic_max_u64(pg_atomic_uint64 *value, uint64 candidate)
{
	uint64 current;

	Assert(value != NULL);
	current = pg_atomic_read_u64(value);
	while (current < candidate
		&& !pg_atomic_compare_exchange_u64(value, &current, candidate))
		;
}

static bool
pcm_entry_mark_quiescing_locked(struct GrdEntry *entry)
{
	uint32 lifecycle;

	Assert(entry != NULL);
	Assert(LWLockHeldByMeInMode(
		&entry->entry_lock.lock, LW_EXCLUSIVE));
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_OPEN
		|| pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation) == 0)
		return false;
	lifecycle = pg_atomic_read_u32(&entry->lifecycle);
	if (lifecycle == PCM_ENTRY_QUIESCING)
		return true;
	if (lifecycle != PCM_ENTRY_LIVE) {
		pcm_resource_x_reconfig_block();
		return false;
	}
	pg_atomic_write_u32(&entry->lifecycle, PCM_ENTRY_QUIESCING);
	return true;
}

static uint64
pcm_resource_x_proof_digest_u64(uint64 digest, uint64 value)
{
	int byte_index;

	for (byte_index = 0; byte_index < 8; byte_index++) {
		digest ^= (uint8)(value & UINT64_C(0xff));
		digest *= PGRAC_RESOURCE_X_PROOF_DIGEST_PRIME;
		value >>= 8;
	}
	return digest;
}

static uint64
pcm_resource_x_proof_digest_tag(uint64 digest, const BufferTag *tag)
{
	digest = pcm_resource_x_proof_digest_u64(digest, (uint64)tag->spcOid);
	digest = pcm_resource_x_proof_digest_u64(digest, (uint64)tag->dbOid);
	digest = pcm_resource_x_proof_digest_u64(digest, (uint64)tag->relNumber);
	digest = pcm_resource_x_proof_digest_u64(
		digest, (uint64)(uint32)tag->forkNum);
	return pcm_resource_x_proof_digest_u64(digest, (uint64)tag->blockNum);
}

static ResourceXApplyResult pcm_resource_x_holder_pair_decode_locked(
	const ClusterPcmResourceXMasterState *state,
	ResourceXDecodedFrame *status_out, ResourceXDecodedFrame *image_out);

/* A committed drain leaves the canonical pair bytes in place so an exact
 * duplicate settlement can be authenticated without touching BufferDesc
 * again.  Those bytes are terminal history only when the published pair,
 * empty send intents and drain tombstone all name the same episode. */
static bool
pcm_resource_x_holder_pair_drained_history_locked(
	struct GrdEntry *entry,
	const ClusterPcmResourceXMasterState *state,
	ResourceXDecodedFrame *status_out, ResourceXDecodedFrame *image_out)
{
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	int32 requester_node;

	Assert(entry != NULL);
	Assert(state != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		|| LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	if (state->holder_status.valid != RESOURCE_X_HOLDER_PAIR_PUBLISHED
		|| state->holder_image.valid != RESOURCE_X_HOLDER_PAIR_PUBLISHED
		|| state->holder_status_intent.slot.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY
		|| state->holder_image_intent.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY
		|| pcm_resource_x_holder_pair_decode_locked(
			state, &status, &image) != RESOURCE_X_APPLY_APPLIED)
		return false;
	requester_node = status.common.logical_assertion.requester_node;
	if (requester_node < 0
		|| requester_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| state->holder_pair_drained_sequences[requester_node]
			!= status.common.assertion_sequence
		|| state->holder_pair_drained_resource_formation
			!= status.common.resource_formation
		|| state->holder_pair_drained_master_session
			!= status.common.master_session_incarnation
		|| state->holder_pair_drained_master_node
			!= (int32)state->holder_status.destination_node
		|| state->holder_pair_drained_reserved != 0)
		return false;
	if (status_out != NULL)
		*status_out = status;
	if (image_out != NULL)
		*image_out = image;
	return true;
}

/* Decode the one independent SourceRetryDedupDebt without consulting a live
 * conversion request.  NONE is all-zero, PENDING owns one exact sendable
 * kind-10 slot, and ACKED retains only the canonical payload as a bounded
 * duplicate tombstone. */
static bool
pcm_resource_x_source_settlement_debt_decode_locked(
	struct GrdEntry *entry,
	const ClusterPcmResourceXMasterState *state,
	ResourceXDecodedFrame *settlement_out)
{
	static const ClusterPcmResourceXSourceSettlement empty_debt;
	static const ResourceXIntentSlot empty_slot;
	const ClusterPcmResourceXSourceSettlement *debt;
	const ResourceXIntentSlot *slot;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;

	Assert(entry != NULL);
	Assert(state != NULL);
	Assert(settlement_out != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		|| LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	memset(settlement_out, 0, sizeof(*settlement_out));
	debt = &state->source_settlement;
	if (debt->state == RESOURCE_X_SOURCE_SETTLEMENT_NONE)
		return memcmp(debt, &empty_debt, sizeof(*debt)) == 0;
	if ((debt->state != RESOURCE_X_SOURCE_SETTLEMENT_PENDING
			&& debt->state != RESOURCE_X_SOURCE_SETTLEMENT_ACKED)
		|| memcmp(debt->reserved, (uint8[7]){0},
			sizeof(debt->reserved)) != 0
		|| !cluster_resource_x_wire_decode(
			RESOURCE_X_MSG_BLOCK_TO_N, debt->intent.payload,
			RESOURCE_X_PROOF_V1_BYTES, settlement_out, &reject)
		|| settlement_out->kind
			!= RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
		|| !BufferTagsEqual(
			&settlement_out->common.logical_assertion.resource,
			&entry->tag))
		return false;

	slot = &debt->intent.slot;
	if (debt->state == RESOURCE_X_SOURCE_SETTLEMENT_ACKED)
		return memcmp(slot, &empty_slot, sizeof(*slot)) == 0;
	return (slot->state == RESOURCE_X_INTENT_SLOT_ARMED
			|| slot->state == RESOURCE_X_INTENT_SLOT_STAGED)
		&& slot->logical_generation
			== settlement_out->common.assertion_sequence
		&& slot->authority_generation
			== settlement_out->common.authority_generation
		&& slot->first_armed_us != 0
		&& slot->first_armed_us != UINT64_MAX
		&& slot->last_attempt_us != UINT64_MAX
		&& slot->destination_node
			== settlement_out->common.action_node
		&& slot->payload_bytes == RESOURCE_X_PROOF_V1_BYTES
		&& slot->kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
		&& resource_x_assertion_equal(
			&slot->body.assertion,
			&settlement_out->common.logical_assertion)
		&& slot->body.owner_generation == slot->logical_generation
		&& slot->body.owner_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		&& slot->body.owner_kind
			== RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE
		&& slot->body.owner_index == 0 && slot->body.reserved == 0;
}

/* Stage 8 8.15-PRE-CAP D3: the sole production retire eligibility table.
 * Caller holds the directory EXCLUSIVE lock and this exact entry lock.  The
 * helper is deliberately read-only; D4 owns sidecar clearing and HASH_REMOVE.
 */
static bool
pcm_entry_retire_eligible_locked(struct GrdEntry *entry,
	uint64 expected_generation, PcmRetireRefusal *why)
{
	static const ClusterPcmResourceXMasterRequest empty_request;
	static const ClusterPcmResourceXTerminalTombstone empty_tombstone;
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXSlot *registry_slot;
	ResourceXDecodedFrame source_settlement;
	uint32 lifecycle;
	uint64 gate_formation;
	bool drained = false;
	int node;

	Assert(entry != NULL);
	Assert(why != NULL);
	Assert(LWLockHeldByMeInMode(
		&ClusterPcm->htab_lock.lock, LW_EXCLUSIVE));
	Assert(LWLockHeldByMeInMode(
		&entry->entry_lock.lock, LW_EXCLUSIVE));
	*why = PCM_RETIRE_REFUSAL_NONE;

	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_OPEN) {
		*why = PCM_RETIRE_REFUSAL_GATE_NOT_OPEN;
		return false;
	}
	if (expected_generation == 0 || expected_generation == UINT64_MAX
		|| entry->binding_generation != expected_generation) {
		*why = PCM_RETIRE_REFUSAL_IDENTITY_MISMATCH;
		return false;
	}
	lifecycle = pg_atomic_read_u32(&entry->lifecycle);
	if (lifecycle != PCM_ENTRY_LIVE && lifecycle != PCM_ENTRY_QUIESCING) {
		*why = PCM_RETIRE_REFUSAL_LIFECYCLE_NOT_LIVE;
		return false;
	}
	if (pg_atomic_read_u32(&entry->wait_refcount) != 0) {
		*why = PCM_RETIRE_REFUSAL_WAITER_PRESENT;
		return false;
	}
	if (pg_atomic_read_u32(&entry->transport_refcount) != 0) {
		*why = PCM_RETIRE_REFUSAL_TRANSPORT_PRESENT;
		return false;
	}
	if (pg_atomic_read_u32(&entry->pin_count) != 0) {
		*why = PCM_RETIRE_REFUSAL_PINNED;
		return false;
	}
	if ((PcmState)pg_atomic_read_u32(&entry->master_state) != PCM_STATE_N) {
		*why = PCM_RETIRE_REFUSAL_PCM_MODE_NOT_N;
		return false;
	}
	if (entry->x_holder_node != -1
		|| pg_atomic_read_u32(&entry->s_holders_bitmap) != 0
		|| entry->s_holder_refcount_local != 0
		|| pcm_master_holder_is_valid(entry)) {
		*why = PCM_RETIRE_REFUSAL_HOLDER_PRESENT;
		return false;
	}
	if (pg_atomic_read_u32(&entry->pi_holders_bitmap) != 0) {
		*why = PCM_RETIRE_REFUSAL_PI_PRESENT;
		return false;
	}
	if (entry->pi_watermark_lsn != InvalidXLogRecPtr
		|| entry->pi_watermark_scn != InvalidScn) {
		*why = PCM_RETIRE_REFUSAL_WATERMARK_PRESENT;
		return false;
	}
	if (entry->pending_x_requester_node != -1
		|| entry->pending_x_since_lsn != 0
		|| entry->convert_queue != NULL) {
		*why = PCM_RETIRE_REFUSAL_CONVERT_PENDING;
		return false;
	}
	gate_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (entry->resource_x_formation != 0
		&& entry->resource_x_formation != gate_formation) {
		*why = PCM_RETIRE_REFUSAL_FORMATION_STALE;
		return false;
	}
	if (!pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner)) {
		*why = PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL;
		return false;
	}
	if (entry->registry_slot >= (uint32)pcm_grd_effective
		|| cluster_pcm_resource_x_slots == NULL
		|| cluster_pcm_resource_x_master_states == NULL) {
		*why = PCM_RETIRE_REFUSAL_IDENTITY_MISMATCH;
		return false;
	}
	registry_slot = &cluster_pcm_resource_x_slots[entry->registry_slot];
	if (registry_slot->state != PCM_REGISTRY_LIVE
		|| registry_slot->reserved != 0
		|| !BufferTagsEqual(&registry_slot->tag, &entry->tag)
		|| registry_slot->binding_generation != expected_generation
		|| registry_slot->retired_authority_generation != 0) {
		*why = PCM_RETIRE_REFUSAL_IDENTITY_MISMATCH;
		return false;
	}
	state = &cluster_pcm_resource_x_master_states[entry->registry_slot];
	if (state->binding_generation != expected_generation) {
		*why = PCM_RETIRE_REFUSAL_IDENTITY_MISMATCH;
		return false;
	}
	if (state->holder_status.valid != 0 || state->holder_image.valid != 0
		|| state->holder_status_intent.slot.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY
		|| state->holder_image_intent.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY) {
		*why = PCM_RETIRE_REFUSAL_RETAINED_PAIR_PRESENT;
		return false;
	}
	if (!pcm_resource_x_requester_join_empty_locked(
			&state->requester_join)
		|| state->requester_settlement_intent.slot.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY) {
		*why = PCM_RETIRE_REFUSAL_REQUESTER_NOT_TERMINAL;
		return false;
	}
	if (!pcm_resource_x_source_settlement_debt_decode_locked(
			entry, state, &source_settlement)) {
		*why = PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL;
		return false;
	}
	if (state->source_settlement.state
			== RESOURCE_X_SOURCE_SETTLEMENT_PENDING) {
		*why = PCM_RETIRE_REFUSAL_REQUESTER_NOT_TERMINAL;
		return false;
	}
	if (!pcm_resource_x_bootstrap_priority_valid(
			&state->bootstrap_priority)
		|| state->bootstrap_priority.state
			!= RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY
		|| state->grant_intent.slot.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY
		|| state->authority_generation == 0
		|| state->authority_generation == UINT64_MAX
		|| state->next_enqueue_order == 0
		|| state->next_enqueue_order == UINT64_MAX) {
		*why = PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL;
		return false;
	}
	for (node = 0; node < RESOURCE_X_PROTOCOL_NODE_LIMIT; node++) {
		const ClusterPcmResourceXBootstrapReceipt *receipt
			= &state->bootstrap_receipts[node];
		const ClusterPcmResourceXMasterRequest *request
			= &state->requests[node];

		if (!pcm_resource_x_bootstrap_receipt_valid(receipt)
			|| receipt->state != RESOURCE_X_BOOTSTRAP_RECEIPT_EMPTY
			|| state->block_intents[node].slot.state
				!= RESOURCE_X_INTENT_SLOT_EMPTY) {
			*why = PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL;
			return false;
		}
		if (request->phase == RESOURCE_X_MASTER_NONE) {
			if (memcmp(request, &empty_request, sizeof(*request)) != 0) {
				*why = PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL;
				return false;
			}
		} else if (request->phase != RESOURCE_X_MASTER_RELEASED
				   || request->resource_formation == 0
				   || request->resource_formation == UINT64_MAX
				   || request->assertion_sequence == 0
				   || request->assertion_sequence == UINT64_MAX) {
			*why = PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL;
			return false;
		}
		if (state->holder_pair_drained_sequences[node] != 0)
			drained = true;
	}
	if (state->terminal_tombstone.valid == 0) {
		if (memcmp(&state->terminal_tombstone, &empty_tombstone,
				sizeof(empty_tombstone)) != 0) {
			*why = PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL;
			return false;
		}
	} else if (state->terminal_tombstone.valid != 1
			   || state->terminal_tombstone.requester_node < 0
			   || state->terminal_tombstone.requester_node
				  >= RESOURCE_X_PROTOCOL_NODE_LIMIT
			   || state->terminal_tombstone.request.phase
				  != RESOURCE_X_MASTER_RELEASED
			   || state->terminal_tombstone.request.resource_formation == 0
			   || state->terminal_tombstone.request.resource_formation
				  == UINT64_MAX) {
		*why = PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL;
		return false;
	}
	if (drained) {
		if (state->holder_pair_drained_resource_formation == 0
			|| state->holder_pair_drained_resource_formation == UINT64_MAX
			|| state->holder_pair_drained_master_session == 0
			|| state->holder_pair_drained_master_session == UINT64_MAX
			|| state->holder_pair_drained_master_node < 0
			|| state->holder_pair_drained_master_node
				>= RESOURCE_X_PROTOCOL_NODE_LIMIT
			|| state->holder_pair_drained_reserved != 0) {
			*why = PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL;
			return false;
		}
	} else if (state->holder_pair_drained_resource_formation != 0
			   || state->holder_pair_drained_master_session != 0
			   || state->holder_pair_drained_master_node != 0
			   || state->holder_pair_drained_reserved != 0) {
		*why = PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL;
		return false;
	}
	if (entry->resource_x_bootstrap_round.highest_attempt_floor
			== UINT64_MAX) {
		*why = PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL;
		return false;
	}
	if (entry->resource_x_local_owner.state
			!= RESOURCE_X_LOCAL_OWNER_EMPTY
		|| entry->resource_x_bootstrap_round.phase
			!= RESOURCE_X_BOOTSTRAP_ROUND_EMPTY) {
		*why = PCM_RETIRE_REFUSAL_RESOURCE_X_ACTIVE;
		return false;
	}
	if (!pcm_resource_x_active_empty_locked(entry)) {
		*why = pcm_resource_x_active_valid_locked(entry)
			? PCM_RETIRE_REFUSAL_REQUESTER_NOT_TERMINAL
			: PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL;
		return false;
	}
	return true;
}

/* Read-only D3 wrapper used by focused validation and, in D4, by the exact
 * try-retire path.  It follows the frozen retire lock order and never waits
 * for entry_lock while holding the directory EXCLUSIVE lock. */
bool
pcm_entry_retire_classify_exact(const BufferTag *tag,
	uint64 expected_generation, PcmRetireRefusal *why)
{
	struct GrdEntry *entry;
	bool eligible;
	bool found;

	if (why != NULL)
		*why = PCM_RETIRE_REFUSAL_IDENTITY_MISMATCH;
	if (tag == NULL || why == NULL || ClusterPcm == NULL
		|| cluster_pcm_htab == NULL)
		return false;
	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_EXCLUSIVE);
	entry = (struct GrdEntry *)hash_search(
		cluster_pcm_htab, tag, HASH_FIND, &found);
	if (!found || entry == NULL
		|| !BufferTagsEqual(&entry->tag, tag)
		|| entry->binding_generation != expected_generation) {
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return false;
	}
	if (!LWLockConditionalAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE)) {
		*why = PCM_RETIRE_REFUSAL_ENTRY_LOCK_BUSY;
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return false;
	}
	eligible = pcm_entry_retire_eligible_locked(
		entry, expected_generation, why);
	LWLockRelease(&entry->entry_lock.lock);
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	return eligible;
}

bool
pcm_entry_try_retire_exact(const BufferTag *tag,
	uint64 binding_generation, PcmRetireReason reason)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXSlot *registry_slot;
	ClusterPcmWmProvSlot *provenance_slot = NULL;
	struct GrdEntry *entry;
	PcmRetireRefusal why;
	BufferTag remove_tag;
	uint64 authority_generation;
	uint64 live_slots;
	uint64 retired_authority_floor;
	bool found;
	bool removed;

	if (tag == NULL || binding_generation == 0
		|| binding_generation == UINT64_MAX
		|| reason < PCM_RETIRE_REASON_HOLDER_RELEASE
		|| reason > PCM_RETIRE_REASON_BOUNDED_RECLAIM
		|| ClusterPcm == NULL || cluster_pcm_htab == NULL)
		return false;
	pg_atomic_fetch_add_u64(&ClusterPcm->reclaim_attempt_count, 1);

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_EXCLUSIVE);
	entry = (struct GrdEntry *)hash_search(
		cluster_pcm_htab, tag, HASH_FIND, &found);
	if (!found || entry == NULL || !BufferTagsEqual(&entry->tag, tag)
		|| entry->binding_generation != binding_generation) {
		pcm_entry_retire_refused_mark(PCM_RETIRE_REFUSAL_IDENTITY_MISMATCH);
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return false;
	}
	if (pg_atomic_read_u32(&entry->pin_count) != 0) {
		pcm_entry_retire_refused_mark(PCM_RETIRE_REFUSAL_PINNED);
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return false;
	}
	if (!LWLockConditionalAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE)) {
		pcm_entry_retire_refused_mark(PCM_RETIRE_REFUSAL_ENTRY_LOCK_BUSY);
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return false;
	}
	if (!pcm_entry_retire_eligible_locked(entry, binding_generation, &why)) {
		pcm_entry_retire_refused_mark(why);
		LWLockRelease(&entry->entry_lock.lock);
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return false;
	}
	if (!LWLockConditionalAcquire(
			&ClusterPcm->wm_prov_lock.lock, LW_EXCLUSIVE)) {
		pcm_entry_retire_refused_mark(PCM_RETIRE_REFUSAL_ENTRY_LOCK_BUSY);
		LWLockRelease(&entry->entry_lock.lock);
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return false;
	}

	registry_slot = &cluster_pcm_resource_x_slots[entry->registry_slot];
	state = &cluster_pcm_resource_x_master_states[entry->registry_slot];
	authority_generation = state->authority_generation;
	retired_authority_floor = pg_atomic_read_u64(
		&ClusterPcm->resource_x_retired_authority_floor);
	live_slots = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_slot_count);
	if (registry_slot->state != PCM_REGISTRY_LIVE
		|| registry_slot->binding_generation != binding_generation
		|| registry_slot->retired_authority_generation != 0
		|| state->binding_generation != binding_generation
		|| authority_generation == 0
		|| authority_generation == UINT64_MAX
		|| retired_authority_floor == UINT64_MAX
		|| live_slots == 0) {
		pcm_entry_retire_refused_mark(PCM_RETIRE_REFUSAL_IDENTITY_MISMATCH);
		pcm_resource_x_reconfig_block();
		LWLockRelease(&ClusterPcm->wm_prov_lock.lock);
		LWLockRelease(&entry->entry_lock.lock);
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return false;
	}
	if (!pcm_wm_prov_validate_retire_exact_locked(
			tag, binding_generation, &provenance_slot)) {
		pcm_entry_retire_refused_mark(
			PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL);
		pcm_resource_x_reconfig_block();
		LWLockRelease(&ClusterPcm->wm_prov_lock.lock);
		LWLockRelease(&entry->entry_lock.lock);
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return false;
	}
	if (!pcm_entry_lifecycle_mutation_mark()) {
		pcm_entry_retire_refused_mark(
			PCM_RETIRE_REFUSAL_SIDECAR_NOT_TERMINAL);
		LWLockRelease(&ClusterPcm->wm_prov_lock.lock);
		LWLockRelease(&entry->entry_lock.lock);
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return false;
	}

	pg_atomic_write_u32(&entry->lifecycle, PCM_ENTRY_RETIRING);
	if (authority_generation > binding_generation
		&& authority_generation > retired_authority_floor)
		pg_atomic_write_u64(
			&ClusterPcm->resource_x_retired_authority_floor,
			authority_generation);
	if (provenance_slot != NULL) {
		memset(provenance_slot, 0, sizeof(*provenance_slot));
		provenance_slot->tag = *tag;
		provenance_slot->binding_generation = binding_generation;
		pg_write_barrier();
		provenance_slot->state = PCM_SIDECAR_TOMBSTONE;
	}
	memset(state, 0, sizeof(*state));
	registry_slot->retired_authority_generation = authority_generation;
	pg_write_barrier();
	registry_slot->state = PCM_REGISTRY_TOMBSTONE;
	pg_atomic_fetch_sub_u64(
		&ClusterPcm->resource_x_reconfig_slot_count, 1);
	pg_atomic_fetch_add_u64(&ClusterPcm->reclaim_tombstone_count, 1);
	remove_tag = *tag;

	LWLockRelease(&ClusterPcm->wm_prov_lock.lock);
	/* The directory EXCLUSIVE lock excludes all new lookups after RETIRING;
	 * release the embedded lock before dynahash invalidates its storage. */
	LWLockRelease(&entry->entry_lock.lock);
	(void)hash_search(
		cluster_pcm_htab, &remove_tag, HASH_REMOVE, &removed);
	if (!removed) {
		pcm_resource_x_reconfig_block();
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return false;
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	pg_atomic_fetch_add_u64(&ClusterPcm->reclaim_success_count, 1);
	return true;
}

bool
cluster_pcm_lock_reclaim_bounded(uint32 probe_budget, PcmReclaimBatch *out)
{
	uint64 capacity;
	uint64 cursor;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || probe_budget == 0
		|| probe_budget > PCM_GRD_LMON_RECLAIM_BUDGET
		|| ClusterPcm == NULL || cluster_pcm_htab == NULL
		|| cluster_pcm_resource_x_slots == NULL
		|| pcm_grd_effective <= 0)
		return false;
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_OPEN)
		return false;

	capacity = (uint64)pcm_grd_effective;
	cursor = pg_atomic_read_u64(&ClusterPcm->reclaim_scan_cursor);
	if (cursor >= capacity) {
		pcm_resource_x_reconfig_block();
		return false;
	}
	while (out->examined_count < probe_budget && cursor < capacity) {
		BufferTag tag;
		uint64 binding_generation = 0;
		uint32 state;

		if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
				!= RESOURCE_X_GATE_OPEN)
			break;
		LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
		state = cluster_pcm_resource_x_slots[cursor].state;
		if (state == PCM_REGISTRY_LIVE) {
			tag = cluster_pcm_resource_x_slots[cursor].tag;
			binding_generation
				= cluster_pcm_resource_x_slots[cursor].binding_generation;
			if (cluster_pcm_resource_x_slots[cursor].reserved != 0
				|| binding_generation == 0
				|| binding_generation == UINT64_MAX
				|| cluster_pcm_resource_x_slots[cursor]
					.retired_authority_generation != 0) {
				LWLockRelease(&ClusterPcm->htab_lock.lock);
				pcm_resource_x_reconfig_block();
				return false;
			}
		}
		else if (state != PCM_REGISTRY_EMPTY
				 && state != PCM_REGISTRY_TOMBSTONE) {
			LWLockRelease(&ClusterPcm->htab_lock.lock);
			pcm_resource_x_reconfig_block();
			return false;
		}
		LWLockRelease(&ClusterPcm->htab_lock.lock);

		cursor++;
		out->examined_count++;
		if (state != PCM_REGISTRY_LIVE)
			continue;
		out->attempted_count++;
		if (pcm_entry_try_retire_exact(&tag, binding_generation,
				PCM_RETIRE_REASON_BOUNDED_RECLAIM))
			out->retired_count++;
	}
	if (cursor == capacity) {
		cursor = 0;
		out->complete_wrap = 1;
		pg_atomic_write_u32(&ClusterPcm->reclaim_requested, 0);
	}
	pg_atomic_write_u64(&ClusterPcm->reclaim_scan_cursor, cursor);
	out->next_cursor = (uint32)cursor;
	return true;
}

void
cluster_pcm_lock_lmon_reclaim_tick(void)
{
	PcmReclaimBatch batch;
	uint64 live_slots;

	if (ClusterPcm == NULL || pcm_grd_effective <= 0
		|| pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_OPEN)
		return;
	live_slots = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_slot_count);
	if (live_slots * UINT64_C(100)
		>= (uint64)pcm_grd_effective
			* (uint64)PCM_GRD_RECLAIM_SOFT_PERCENT)
		pcm_entry_reclaim_request();
	if (pg_atomic_read_u32(&ClusterPcm->reclaim_requested) == 0)
		return;
	(void)cluster_pcm_lock_reclaim_bounded(
		PCM_GRD_LMON_RECLAIM_BUDGET, &batch);
}

static bool
pcm_resource_x_reconfig_proof_slot_locked(
	struct GrdEntry *entry,
	const ClusterPcmResourceXMasterState *state,
	const ResourceXReconfigToken *token, uint64 *digest_io,
	uint64 *successor_count_io, uint64 *terminal_count_io,
	bool *old_residual_out)
{
	static const ClusterPcmResourceXMasterRequest empty_request;
	static const ClusterPcmResourceXTerminalTombstone empty_tombstone;
	const ClusterPcmResourceXBootstrapPriority *priority;
	const ClusterPcmResourceXLocalOwner *local_owner;
	ResourceXRequesterJoinSnapshot join;
	ResourceXDecodedFrame source_settlement;
	uint64 digest;
	uint64 local_owner_formation;
	uint64 local_owner_generation;
	int requester_node;
	int holder_node;

	Assert(entry != NULL);
	Assert(state != NULL);
	Assert(token != NULL);
	Assert(digest_io != NULL);
	Assert(successor_count_io != NULL);
	Assert(terminal_count_io != NULL);
	Assert(old_residual_out != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED));
	*old_residual_out = false;
	digest = pcm_resource_x_proof_digest_tag(*digest_io, &entry->tag);

	if (!pcm_resource_x_active_empty_locked(entry)) {
		if (entry->resource_x_requester_node < 0
			|| entry->resource_x_requester_node
				   >= RESOURCE_X_PROTOCOL_NODE_LIMIT
			|| entry->resource_x_acquisition_generation == 0
			|| entry->resource_x_acquisition_generation == UINT64_MAX
			|| (entry->resource_x_progress_flags
				& ~RESOURCE_X_PROGRESS_KNOWN_MASK) != 0
			|| (entry->resource_x_formation != token->old_formation
				&& entry->resource_x_formation != token->new_formation))
			return false;
		if (entry->resource_x_formation == token->old_formation)
			*old_residual_out = true;
		else
			(*successor_count_io)++;
		digest = pcm_resource_x_proof_digest_u64(
			digest, entry->resource_x_formation);
		digest = pcm_resource_x_proof_digest_u64(
			digest, entry->resource_x_acquisition_generation);
		digest = pcm_resource_x_proof_digest_u64(
			digest, (uint64)entry->resource_x_progress_flags);
	}
	local_owner = &entry->resource_x_local_owner;
	if (!pcm_resource_x_local_owner_valid_locked(local_owner))
		return false;
	if (local_owner->state != RESOURCE_X_LOCAL_OWNER_EMPTY) {
		if (local_owner->state == RESOURCE_X_LOCAL_OWNER_HANDOFF) {
			if (!BufferTagsEqual(
					&local_owner->handoff.successor_assertion.resource,
					&entry->tag))
				return false;
			local_owner_formation = local_owner->handoff.holder_ref.formation;
			local_owner_generation = local_owner->highest_owner_generation;
		}
		else {
			if (!BufferTagsEqual(
					&local_owner->handle.ref.assertion.resource,
					&entry->tag))
				return false;
			local_owner_formation = local_owner->handle.ref.formation;
			local_owner_generation = local_owner->handle.owner_generation;
		}
		if (local_owner_formation != token->old_formation
			&& local_owner_formation != token->new_formation)
			return false;
		if (local_owner_formation == token->old_formation)
			*old_residual_out = true;
		else
			(*successor_count_io)++;
		digest = pcm_resource_x_proof_digest_u64(
			digest, local_owner_formation);
		digest = pcm_resource_x_proof_digest_u64(
			digest, local_owner_generation);
		digest = pcm_resource_x_proof_digest_u64(
			digest, (uint64)local_owner->state);
	}

	priority = &state->bootstrap_priority;
	if (!pcm_resource_x_bootstrap_priority_valid(priority))
		return false;
	digest = pcm_resource_x_proof_digest_u64(
		digest, (uint64)priority->state);
	if (priority->state == RESOURCE_X_BOOTSTRAP_PRIORITY_NEXT_ADMISSION) {
		if (!BufferTagsEqual(
				&priority->request.logical_assertion.resource, &entry->tag)
			|| (priority->request.resource_formation
					!= token->old_formation
				&& priority->request.resource_formation
					!= token->new_formation))
			return false;
		if (priority->request.resource_formation == token->old_formation)
			*old_residual_out = true;
		digest = pcm_resource_x_proof_digest_u64(
			digest, priority->request.resource_formation);
		digest = pcm_resource_x_proof_digest_u64(
			digest, priority->request.assertion_sequence);
		digest = pcm_resource_x_proof_digest_u64(
			digest, (uint64)(uint32)
				priority->request.logical_assertion.requester_node);
		digest = pcm_resource_x_proof_digest_u64(
			digest, priority->r4_record_generation);
		digest = pcm_resource_x_proof_digest_u64(
			digest,
			(uint64)priority->authenticated_ingress_connection_generation);
		digest = pcm_resource_x_proof_digest_u64(
			digest, (uint64)priority->master_sender_connection_generation);
		digest = pcm_resource_x_proof_digest_u64(
			digest, (uint64)priority->request.semantic_crc32c);
	}

	for (requester_node = 0;
		 requester_node < RESOURCE_X_PROTOCOL_NODE_LIMIT;
		 requester_node++) {
		const ClusterPcmResourceXBootstrapReceipt *receipt
			= &state->bootstrap_receipts[requester_node];
		const ClusterPcmResourceXMasterRequest *request
			= &state->requests[requester_node];

		if (!pcm_resource_x_bootstrap_receipt_valid(receipt))
			return false;
		digest = pcm_resource_x_proof_digest_u64(
			digest, receipt->highest_attempt_floor);
		if (receipt->state != RESOURCE_X_BOOTSTRAP_RECEIPT_EMPTY) {
			if (receipt->request.logical_assertion.requester_node
					!= requester_node
				|| (receipt->request.resource_formation
						!= token->old_formation
					&& receipt->request.resource_formation
							!= token->new_formation))
				return false;
			if (receipt->request.resource_formation
					== token->old_formation)
				*old_residual_out = true;
			digest = pcm_resource_x_proof_digest_u64(
				digest, receipt->request.resource_formation);
			digest = pcm_resource_x_proof_digest_u64(
				digest, receipt->request.assertion_sequence);
			digest = pcm_resource_x_proof_digest_u64(
				digest, (uint64)receipt->state);
		}

		if (request->phase == RESOURCE_X_MASTER_NONE) {
			if (memcmp(request, &empty_request, sizeof(*request)) != 0)
				return false;
			continue;
		}
		if (request->phase > RESOURCE_X_MASTER_RELEASED
			|| request->resource_formation == 0
			|| request->resource_formation == UINT64_MAX
			|| (request->resource_formation != token->old_formation
				&& request->resource_formation != token->new_formation))
			return false;
		if (request->resource_formation == token->old_formation) {
			if (request->phase != RESOURCE_X_MASTER_RELEASED)
				*old_residual_out = true;
			else
				(*terminal_count_io)++;
		}
		else
			(*successor_count_io)++;
		digest = pcm_resource_x_proof_digest_u64(
			digest, request->resource_formation);
		digest = pcm_resource_x_proof_digest_u64(
			digest, request->assertion_sequence);
		digest = pcm_resource_x_proof_digest_u64(
			digest, (uint64)request->phase);
	}
	for (holder_node = 0;
		 holder_node < RESOURCE_X_PROTOCOL_NODE_LIMIT; holder_node++) {
		const ResourceXIntentSlot *intent
			= &state->block_intents[holder_node].slot;
		const ClusterPcmResourceXMasterRequest *owner;
		int32 owner_node;

		if (intent->state == RESOURCE_X_INTENT_SLOT_EMPTY)
			continue;
		owner_node = intent->body.assertion.requester_node;
		if ((intent->state != RESOURCE_X_INTENT_SLOT_ARMED
			 && intent->state != RESOURCE_X_INTENT_SLOT_STAGED)
			|| intent->body.owner_kind
				   != RESOURCE_X_INTENT_OWNER_MASTER_BLOCK
			|| intent->body.owner_index != (uint8)holder_node
			|| owner_node < 0
			|| owner_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
			return false;
		owner = &state->requests[owner_node];
		if (owner->phase == RESOURCE_X_MASTER_NONE
			|| (owner->resource_formation != token->old_formation
				&& owner->resource_formation != token->new_formation))
			return false;
		if (owner->resource_formation == token->old_formation)
			*old_residual_out = true;
		else
			(*successor_count_io)++;
	}

	if (state->terminal_tombstone.valid == 0) {
		if (memcmp(&state->terminal_tombstone, &empty_tombstone,
				   sizeof(empty_tombstone)) != 0)
			return false;
	}
	else {
		const ClusterPcmResourceXTerminalTombstone *tombstone
			= &state->terminal_tombstone;

		if (tombstone->valid != 1 || tombstone->requester_node < 0
			|| tombstone->requester_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
			|| (tombstone->request.phase != RESOURCE_X_MASTER_SETTLED
				&& tombstone->request.phase != RESOURCE_X_MASTER_RELEASED)
			|| (tombstone->request.resource_formation != token->old_formation
				&& tombstone->request.resource_formation
					   != token->new_formation))
			return false;
		if (tombstone->request.resource_formation == token->old_formation
			&& tombstone->request.phase != RESOURCE_X_MASTER_RELEASED)
			*old_residual_out = true;
		else
			(*terminal_count_io)++;
		digest = pcm_resource_x_proof_digest_u64(
			digest, tombstone->request.resource_formation);
		digest = pcm_resource_x_proof_digest_u64(
			digest, tombstone->request.assertion_sequence);
		digest = pcm_resource_x_proof_digest_u64(
			digest, (uint64)tombstone->request.phase);
	}

	if (!pcm_resource_x_source_settlement_debt_decode_locked(
			entry, state, &source_settlement))
		return false;
	digest = pcm_resource_x_proof_digest_u64(
		digest, (uint64)state->source_settlement.state);
	if (state->source_settlement.state
			!= RESOURCE_X_SOURCE_SETTLEMENT_NONE) {
		if (source_settlement.common.resource_formation
				!= token->old_formation
			&& source_settlement.common.resource_formation
				!= token->new_formation)
			return false;
		if (state->source_settlement.state
				== RESOURCE_X_SOURCE_SETTLEMENT_PENDING) {
			if (source_settlement.common.resource_formation
					== token->old_formation)
				*old_residual_out = true;
			else
				(*successor_count_io)++;
		}
		else if (state->source_settlement.state
				== RESOURCE_X_SOURCE_SETTLEMENT_ACKED)
			(*terminal_count_io)++;
		digest = pcm_resource_x_proof_digest_u64(
			digest, source_settlement.common.resource_formation);
		digest = pcm_resource_x_proof_digest_u64(
			digest, source_settlement.common.assertion_sequence);
		digest = pcm_resource_x_proof_digest_u64(
			digest, source_settlement.common.authority_generation);
		digest = pcm_resource_x_proof_digest_u64(
			digest,
			source_settlement.body.blocked_to_n.source_carrier_generation);
	}

	if (state->holder_status.valid != 0) {
		if ((state->holder_status.valid != RESOURCE_X_HOLDER_PAIR_PENDING
			 && state->holder_status.valid
				!= RESOURCE_X_HOLDER_PAIR_PUBLISHED)
			|| (state->holder_status.resource_formation
				!= token->old_formation
				&& state->holder_status.resource_formation
					   != token->new_formation))
			return false;
		if (state->holder_status.resource_formation == token->old_formation)
			*old_residual_out = true;
		else
			(*successor_count_io)++;
	}
	if (state->holder_status_intent.slot.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY
		&& state->holder_status.valid == 0)
		return false;
	if (state->holder_image.valid != 0) {
		if ((state->holder_image.valid != RESOURCE_X_HOLDER_PAIR_PENDING
			 && state->holder_image.valid
				!= RESOURCE_X_HOLDER_PAIR_PUBLISHED)
			|| (state->holder_image.resource_formation
				!= token->old_formation
				&& state->holder_image.resource_formation
					   != token->new_formation))
			return false;
		if (state->holder_image.resource_formation == token->old_formation)
			*old_residual_out = true;
		else
			(*successor_count_io)++;
	}
	if (state->holder_image_intent.state != RESOURCE_X_INTENT_SLOT_EMPTY
		&& state->holder_image.valid == 0)
		return false;
	if (!pcm_resource_x_requester_join_empty_locked(
			&state->requester_join)) {
		if (!pcm_resource_x_requester_join_snapshot_locked(
				&state->requester_join, &join)
			|| (join.resource_formation != token->old_formation
				&& join.resource_formation != token->new_formation))
			return false;
		if (join.resource_formation == token->old_formation)
			*old_residual_out = true;
		else
			(*successor_count_io)++;
	}
	if (state->grant_intent.slot.state != RESOURCE_X_INTENT_SLOT_EMPTY
		|| state->requester_settlement_intent.slot.state
			   != RESOURCE_X_INTENT_SLOT_EMPTY) {
		const ResourceXIntentSlot *intents[2];
		int intent_index;

		intents[0] = &state->grant_intent.slot;
		intents[1] = &state->requester_settlement_intent.slot;
		for (intent_index = 0; intent_index < 2; intent_index++) {
			const ResourceXIntentSlot *intent = intents[intent_index];
			const ClusterPcmResourceXMasterRequest *owner;
			int32 owner_node;

			if (intent->state == RESOURCE_X_INTENT_SLOT_EMPTY)
				continue;
			owner_node = intent->body.assertion.requester_node;
			if ((intent->state != RESOURCE_X_INTENT_SLOT_ARMED
				 && intent->state != RESOURCE_X_INTENT_SLOT_STAGED)
				|| owner_node < 0
				|| owner_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
				return false;
			owner = &state->requests[owner_node];
			if (owner->phase == RESOURCE_X_MASTER_NONE) {
				if (state->terminal_tombstone.valid != 1
					|| state->terminal_tombstone.requester_node
						   != owner_node)
					return false;
				owner = &state->terminal_tombstone.request;
			}
			if (owner->resource_formation != token->old_formation
				&& owner->resource_formation != token->new_formation)
				return false;
			if (owner->resource_formation == token->old_formation)
				*old_residual_out = true;
			else
				(*successor_count_io)++;
		}
	}

	{
		bool drained = false;

		for (holder_node = 0;
			 holder_node < RESOURCE_X_PROTOCOL_NODE_LIMIT; holder_node++)
			if (state->holder_pair_drained_sequences[holder_node] != 0) {
				drained = true;
				(*terminal_count_io)++;
			}
		if (drained) {
			if (state->holder_pair_drained_resource_formation == 0
				|| state->holder_pair_drained_resource_formation == UINT64_MAX
				|| state->holder_pair_drained_master_session == 0
				|| state->holder_pair_drained_master_session == UINT64_MAX
				|| state->holder_pair_drained_master_node < 0
				|| state->holder_pair_drained_master_node
					   >= RESOURCE_X_PROTOCOL_NODE_LIMIT
				|| state->holder_pair_drained_reserved != 0)
				return false;
		}
		else if (state->holder_pair_drained_resource_formation != 0
				 || state->holder_pair_drained_master_session != 0
				 || state->holder_pair_drained_master_node != 0
				 || state->holder_pair_drained_reserved != 0)
			return false;
	}
	digest = pcm_resource_x_proof_digest_u64(
		digest, state->authority_generation);
	digest = pcm_resource_x_proof_digest_u64(
		digest, state->next_enqueue_order);
	*digest_io = digest;
	return true;
}

static bool
pcm_resource_x_zero_proof_phase_exact(
	const ResourceXReconfigToken *token,
	const ResourceXZeroResidualProof *proof, uint32 expected_phase)
{
	return token != NULL && proof != NULL
		&& memcmp(&proof->token, token, sizeof(*token)) == 0
		&& proof->proof_generation == token->freeze_generation
		&& proof->scan_begin_cursor == 0
		&& proof->scan_end_cursor == proof->scan_capacity
		&& proof->scan_capacity
			   == pg_atomic_read_u64(
				   &ClusterPcm->resource_x_reconfig_scan_capacity)
		&& proof->scan_begin_slot_count == proof->scan_end_slot_count
		&& proof->scan_end_slot_count
			   == pg_atomic_read_u64(
				   &ClusterPcm->resource_x_reconfig_slot_count)
		&& proof->final_mutation_sequence
			   == pg_atomic_read_u64(
				   &ClusterPcm->resource_x_semantic_mutation_sequence)
		&& proof->full_wrap_digest != 0 && proof->complete_wrap == 1
		&& proof->zero_residual == 1
		&& memcmp(proof->creator_product_fingerprint,
				  pcm_resource_x_completion_product_fingerprint,
				  RESOURCE_X_COMPLETION_FINGERPRINT_BYTES) == 0
		&& (expected_phase == RESOURCE_X_GATE_FROZEN
			|| expected_phase == RESOURCE_X_GATE_OPEN)
		&& pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			   == expected_phase
		&& (expected_phase != RESOURCE_X_GATE_OPEN
			|| pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation)
				   == token->new_formation)
		&& pcm_resource_x_reconfig_token_exact(token)
		&& pg_atomic_read_u64(
			   &ClusterPcm->resource_x_activation_inflight_count) == 0;
}

static bool
pcm_resource_x_zero_proof_live_exact(
	const ResourceXReconfigToken *token,
	const ResourceXZeroResidualProof *proof)
{
	return pcm_resource_x_zero_proof_phase_exact(
		token, proof, RESOURCE_X_GATE_FROZEN);
}

bool
cluster_resource_x_reconfig_freeze_pending_exact(uint64 old_formation,
										 uint32 dead_requester_bitmap,
										 ResourceXReconfigToken *out)
{
	ResourceXReconfigToken live;
	uint64 current_generation;
	uint64 next_generation;
	uint64 gate_formation;
	uint32 phase;
	uint32 expected_phase;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (ClusterPcm == NULL || cluster_pcm_htab == NULL || old_formation == 0
		|| old_formation == UINT64_MAX)
		return false;

	phase = pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase);
	live.old_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_old_formation);
	live.new_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_new_formation);
	live.freeze_generation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_freeze_generation);
	live.dead_requester_bitmap = pg_atomic_read_u32(
		&ClusterPcm->resource_x_reconfig_dead_requester_bitmap);
	live.reserved = 0;
	if (phase == RESOURCE_X_GATE_FROZEN && live.old_formation == old_formation
		&& live.dead_requester_bitmap == dead_requester_bitmap
		&& live.freeze_generation != 0 && live.freeze_generation != UINT64_MAX
		&& (live.new_formation == 0
			|| (live.new_formation != old_formation
				&& live.new_formation != UINT64_MAX))) {
		*out = live;
		return true;
	}
	if (phase != RESOURCE_X_GATE_OPEN) {
		if (phase == RESOURCE_X_GATE_FROZEN)
			pcm_resource_x_reconfig_block();
		return false;
	}

	gate_formation = pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (gate_formation != old_formation)
		return false;
	current_generation = pg_atomic_read_u64(&ClusterPcm->resource_x_freeze_generation);
	if (!cluster_resource_x_next_freeze_generation(current_generation, &next_generation)) {
		pcm_resource_x_reconfig_block();
		return false;
	}
	expected_phase = RESOURCE_X_GATE_OPEN;
	if (!pg_atomic_compare_exchange_u32(&ClusterPcm->resource_x_gate_phase, &expected_phase,
										 RESOURCE_X_GATE_FROZEN)) {
		pcm_resource_x_reconfig_block();
		return false;
	}

	pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_old_formation, old_formation);
	pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_new_formation, 0);
	pg_atomic_write_u32(&ClusterPcm->resource_x_reconfig_dead_requester_bitmap,
						dead_requester_bitmap);
	pg_atomic_write_u64(&ClusterPcm->resource_x_freeze_generation, next_generation);
	pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_next_state_index, 0);
	pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_scan_capacity,
						 (uint64)pcm_grd_effective);
	pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_zero_proof_generation, 0);
	pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_residual_count, 0);
	pg_atomic_write_u32(&ClusterPcm->resource_x_reconfig_proof_pass, 0);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_begin_sequence, 0);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_begin_slot_count, 0);
	pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_proof_digest, 0);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_empty_slot_count, 0);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_successor_slot_count, 0);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_terminal_slot_count, 0);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_retry_revisit_count, 0);
	memset(&ClusterPcm->resource_x_zero_residual_proof, 0,
		   sizeof(ClusterPcm->resource_x_zero_residual_proof));
	memset(&ClusterPcm->resource_x_clean_completion_proof, 0,
		   sizeof(ClusterPcm->resource_x_clean_completion_proof));
	pcm_resource_x_semantic_mutation_mark();
	pg_atomic_fetch_add_u64(&ClusterPcm->resource_x_reconfig_freeze_count, 1);
	out->old_formation = old_formation;
	out->new_formation = 0;
	out->freeze_generation = next_generation;
	out->dead_requester_bitmap = dead_requester_bitmap;
	return true;
}

bool
cluster_resource_x_reconfig_freeze_pending(uint64 old_formation,
										ResourceXReconfigToken *out)
{
	return cluster_resource_x_reconfig_freeze_pending_exact(old_formation, 0,
														 out);
}

bool
cluster_resource_x_reconfig_bind_new_formation_exact(ResourceXReconfigToken *token,
											 uint64 new_formation)
{
	uint64 expected_formation;

	if (ClusterPcm == NULL || token == NULL || token->old_formation == 0
		|| token->old_formation == UINT64_MAX || token->freeze_generation == 0
		|| token->freeze_generation == UINT64_MAX || new_formation == 0
		|| new_formation == UINT64_MAX || new_formation == token->old_formation
		|| pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase) != RESOURCE_X_GATE_FROZEN
		|| pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_old_formation)
			   != token->old_formation
		|| pg_atomic_read_u64(&ClusterPcm->resource_x_freeze_generation)
			   != token->freeze_generation
		|| pg_atomic_read_u32(&ClusterPcm->resource_x_reconfig_dead_requester_bitmap)
			   != token->dead_requester_bitmap)
		return false;

	expected_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_new_formation);
	if ((token->new_formation == 0 || token->new_formation == new_formation)
		&& expected_formation == new_formation) {
		token->new_formation = new_formation;
		return true;
	}
	if (token->new_formation != 0 || expected_formation != 0) {
		pcm_resource_x_reconfig_block();
		return false;
	}
	if (!pg_atomic_compare_exchange_u64(&ClusterPcm->resource_x_reconfig_new_formation,
										&expected_formation, new_formation)) {
		if (expected_formation == new_formation) {
			token->new_formation = new_formation;
			return true;
		}
		pcm_resource_x_reconfig_block();
		return false;
	}
	pcm_resource_x_semantic_mutation_mark();
	token->new_formation = new_formation;
	return true;
}

bool
cluster_resource_x_reconfig_freeze_exact(uint64 old_formation,
								  uint64 new_formation,
								  uint32 dead_requester_bitmap,
								  ResourceXReconfigToken *out)
{
	ResourceXReconfigToken live;
	uint32 phase;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (ClusterPcm == NULL || cluster_pcm_htab == NULL || old_formation == 0
		|| old_formation == UINT64_MAX || new_formation == 0 || new_formation == UINT64_MAX
		|| old_formation == new_formation)
		return false;

	phase = pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase);
	live.old_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_old_formation);
	live.new_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_new_formation);
	live.freeze_generation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_freeze_generation);
	live.dead_requester_bitmap = pg_atomic_read_u32(
		&ClusterPcm->resource_x_reconfig_dead_requester_bitmap);
	live.reserved = 0;
	if (phase == RESOURCE_X_GATE_FROZEN && live.old_formation == old_formation
		&& live.new_formation == new_formation && live.freeze_generation != 0
		&& live.freeze_generation != UINT64_MAX
		&& live.dead_requester_bitmap == dead_requester_bitmap) {
		*out = live;
		return true;
	}
	if (phase != RESOURCE_X_GATE_OPEN) {
		if (phase == RESOURCE_X_GATE_FROZEN)
			pcm_resource_x_reconfig_block();
		return false;
	}
	if (!cluster_resource_x_reconfig_freeze_pending_exact(
			old_formation, dead_requester_bitmap, out))
		return false;
	return cluster_resource_x_reconfig_bind_new_formation_exact(out, new_formation);
}

bool
cluster_resource_x_reconfig_freeze(uint64 old_formation, uint64 new_formation,
								   ResourceXReconfigToken *out)
{
	return cluster_resource_x_reconfig_freeze_exact(old_formation, new_formation,
												  0, out);
}

bool
cluster_resource_x_reconfig_cutover_begin_native_exact(
	ResourceXReconfigToken *out)
{
	ResourceXGateSnapshot gate;
	ResourceXApplyResult bind_result;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!cluster_pcm_lock_resource_x_gate_snapshot(&gate)) {
		if (!cluster_pcm_lock_resource_x_cutover_gate_snapshot_exact(&gate)
			|| gate.phase != RESOURCE_X_GATE_OPEN
			|| gate.formation != 0)
			return false;
		bind_result = cluster_pcm_lock_resource_x_gate_bind_formation_exact(
			PGRAC_RESOURCE_X_NATIVE_INITIAL_FORMATION);
		if (bind_result != RESOURCE_X_APPLY_APPLIED
			&& bind_result != RESOURCE_X_APPLY_DUPLICATE)
			return false;
		if (!cluster_pcm_lock_resource_x_gate_snapshot(&gate))
			return false;
	}
	if ((gate.phase != RESOURCE_X_GATE_OPEN
		 && gate.phase != RESOURCE_X_GATE_FROZEN)
		|| gate.formation == 0 || gate.formation == UINT64_MAX
		|| gate.reserved != 0)
		return false;
	return cluster_resource_x_reconfig_freeze_pending_exact(
		gate.formation, 0, out);
}

bool
cluster_resource_x_reconfig_cutover_bind_native_successor_exact(
	ResourceXReconfigToken *token)
{
	uint64 successor;

	if (token == NULL || token->old_formation == 0
		|| token->old_formation >= UINT64_MAX - 1) {
		pcm_resource_x_reconfig_block();
		return false;
	}
	successor = token->old_formation + 1;
	return cluster_resource_x_reconfig_bind_new_formation_exact(
		token, successor);
}

static ResourceXReconfigResult
pcm_resource_x_reconfig_proof_sweep(
	const ResourceXReconfigToken *token, uint32 probe_budget,
	ResourceXReconfigBatch *out, uint64 capacity, uint64 cursor)
{
	ResourceXZeroResidualProof proof;
	uint64 begin_sequence;
	uint64 begin_slot_count;
	uint64 digest;
	uint64 empty_count;
	uint64 successor_count;
	uint64 terminal_count;
	uint64 retry_count;
	uint32 probes = 0;

	begin_sequence = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_proof_begin_sequence);
	begin_slot_count = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_proof_begin_slot_count);
	digest = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_proof_digest);
	empty_count = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_proof_empty_slot_count);
	successor_count = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_proof_successor_slot_count);
	terminal_count = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_proof_terminal_slot_count);
	retry_count = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_proof_retry_revisit_count);
	if (begin_sequence == 0 || begin_sequence == UINT64_MAX
		|| digest == 0
		|| begin_sequence != pg_atomic_read_u64(
			&ClusterPcm->resource_x_semantic_mutation_sequence)
		|| begin_slot_count != pg_atomic_read_u64(
			&ClusterPcm->resource_x_reconfig_slot_count))
		goto retry_from_zero;

	while (probes < probe_budget && cursor < capacity) {
		ClusterPcmResourceXSlot *registry_slot
			= &cluster_pcm_resource_x_slots[cursor];

		digest = pcm_resource_x_proof_digest_u64(digest, cursor);
		if (registry_slot->state == PCM_REGISTRY_EMPTY
			|| registry_slot->state == PCM_REGISTRY_TOMBSTONE) {
			digest = pcm_resource_x_proof_digest_u64(digest, 0);
			empty_count++;
		}
		else if (registry_slot->state != PCM_REGISTRY_LIVE) {
			pcm_resource_x_reconfig_block();
			return RESOURCE_X_RECONFIG_CORRUPT;
		}
		else {
			ClusterPcmResourceXMasterState *state;
			PcmEntryRef entry_ref;
			PcmEntryAcquireResult acquire_result;
			struct GrdEntry *entry;
			bool old_residual = false;

			if (!pcm_entry_ref_acquire(&registry_slot->tag, false,
					&entry_ref, &acquire_result)) {
				pcm_resource_x_reconfig_block();
				return RESOURCE_X_RECONFIG_CORRUPT;
			}
			entry = entry_ref.entry;
			LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
			state = pcm_resource_x_master_state_for_entry(entry);
			if (state == NULL
				|| !BufferTagsEqual(&entry->tag, &registry_slot->tag)
				|| entry_ref.binding_generation
					!= registry_slot->binding_generation
				|| entry_ref.registry_slot != (uint32)cursor
				|| !pcm_resource_x_reconfig_proof_slot_locked(
					entry, state, token, &digest, &successor_count,
					&terminal_count, &old_residual)) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_entry_ref_release(&entry_ref);
				pcm_resource_x_reconfig_block();
				return RESOURCE_X_RECONFIG_CORRUPT;
			}
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			if (old_residual) {
				out->orphan_count = 1;
				out->residual_count = 1;
				out->next_state_index = cursor;
				pg_atomic_write_u64(
					&ClusterPcm->resource_x_reconfig_residual_count, 1);
				pg_atomic_write_u64(
					&ClusterPcm->resource_x_reconfig_next_state_index,
					cursor);
				pcm_resource_x_reconfig_block();
				return RESOURCE_X_RECONFIG_ORPHAN;
			}
		}
		cursor++;
		probes++;
		out->examined_count++;
		pg_atomic_fetch_add_u64(
			&ClusterPcm->resource_x_reconfig_slot_examined_count, 1);
	}

	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_digest, digest);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_empty_slot_count,
		empty_count);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_successor_slot_count,
		successor_count);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_terminal_slot_count,
		terminal_count);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_next_state_index, cursor);
	out->next_state_index = cursor;
	if (cursor < capacity)
		return RESOURCE_X_RECONFIG_MORE;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_EXCLUSIVE);
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_FROZEN
		|| !pcm_resource_x_reconfig_token_exact(token)
		|| pg_atomic_read_u64(
			   &ClusterPcm->resource_x_activation_inflight_count) != 0
		|| begin_sequence != pg_atomic_read_u64(
			   &ClusterPcm->resource_x_semantic_mutation_sequence)
		|| begin_slot_count != pg_atomic_read_u64(
			   &ClusterPcm->resource_x_reconfig_slot_count)
		|| capacity != pg_atomic_read_u64(
			   &ClusterPcm->resource_x_reconfig_scan_capacity)) {
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		goto retry_from_zero;
	}
	memset(&proof, 0, sizeof(proof));
	proof.token = *token;
	proof.proof_generation = token->freeze_generation;
	proof.scan_begin_cursor = 0;
	proof.scan_end_cursor = capacity;
	proof.scan_capacity = capacity;
	proof.scan_begin_slot_count = begin_slot_count;
	proof.scan_end_slot_count = begin_slot_count;
	proof.final_mutation_sequence = begin_sequence;
	proof.full_wrap_digest = digest;
	proof.empty_slot_count = empty_count;
	proof.successor_slot_count = successor_count;
	proof.terminal_slot_count = terminal_count;
	proof.retry_revisit_count = retry_count;
	memcpy(proof.creator_product_fingerprint,
		   pcm_resource_x_completion_product_fingerprint,
		   RESOURCE_X_COMPLETION_FINGERPRINT_BYTES);
	proof.complete_wrap = 1;
	proof.zero_residual = 1;
	ClusterPcm->resource_x_zero_residual_proof = proof;
	memset(&ClusterPcm->resource_x_clean_completion_proof, 0,
		   sizeof(ClusterPcm->resource_x_clean_completion_proof));
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_zero_proof_generation,
		token->freeze_generation);
	pg_atomic_write_u32(
		&ClusterPcm->resource_x_reconfig_proof_pass, 2);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_next_state_index, 0);
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	out->next_state_index = 0;
	out->complete_wrap = 1;
	out->zero_residual = 1;
	return RESOURCE_X_RECONFIG_DONE;

retry_from_zero:
	retry_count++;
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_retry_revisit_count,
		retry_count);
	pg_atomic_write_u32(
		&ClusterPcm->resource_x_reconfig_proof_pass, 0);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_next_state_index, 0);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_zero_proof_generation, 0);
	out->retry_count = 1;
	out->next_state_index = 0;
	pg_atomic_fetch_add_u64(
		&ClusterPcm->resource_x_reconfig_retry_count, 1);
	return RESOURCE_X_RECONFIG_RETRY;
}

static ResourceXReconfigResult
pcm_resource_x_reconfig_return_release(PcmEntryRef *entry_ref,
	ResourceXReconfigResult result)
{
	if (entry_ref != NULL && entry_ref->pinned)
		pcm_entry_ref_release(entry_ref);
	return result;
}

ResourceXReconfigResult
cluster_resource_x_reconfig_sweep(const ResourceXReconfigToken *token, uint32 probe_budget,
								  ResourceXReconfigBatch *out)
{
	ResourceXReconfigResult result = RESOURCE_X_RECONFIG_MORE;
	ClusterPcmResourceXSlot *slot;
	ClusterPcmResourceXMasterState *master_state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	BufferTag snapshot_tag;
	ResourceXSidecarNeutralizeResult sidecar_result;
	uint64 capacity;
	uint64 cursor;
	uint64 snapshot_acquisition_generation;
	uint64 snapshot_no_progress_generation;
	uint32 probes;
	uint32 progress_flags;
	uint32 snapshot_dispatch_phase;
	uint32 snapshot_no_progress_reason;
	uint32 snapshot_progress_flags;
	uint32 dead_requester_bitmap;
	uint64 formation;
	int32 dead_node;
	int32 snapshot_requester_node;
	ResourceXDecodedFrame join_grant;
	ResourceXDecodedFrame join_image;
	ResourceXDecodedFrame holder_pair_image;
	ResourceXDecodedFrame holder_pair_status;
	ResourceXDecodedFrame source_settlement;
	ResourceXRequesterJoinSnapshot join_snapshot;
	bool first_orphan = false;
	bool holder_pair_drained;
	bool holder_pair_old_unsettled;
	bool holder_pair_present;
	bool bootstrap_priority_invalidated;
	int32 holder_pair_node;
	bool bootstrap_receipt_invalidated;
	bool bootstrap_round_invalidated;
	bool local_priority_cleared;
	bool pending_token;
	int32 bootstrap_requester_node;

	if (out == NULL)
		return RESOURCE_X_RECONFIG_CORRUPT;
	memset(out, 0, sizeof(*out));
	memset(&entry_ref, 0, sizeof(entry_ref));
	pending_token = ClusterPcm != NULL
		&& pcm_resource_x_reconfig_pending_token_exact(token);
	if (ClusterPcm == NULL || cluster_pcm_htab == NULL || probe_budget == 0 || probe_budget > 4
		|| pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase) != RESOURCE_X_GATE_FROZEN
		|| (!pending_token && !pcm_resource_x_reconfig_token_exact(token))
		|| (pending_token
			&& pg_atomic_read_u32(
				&ClusterPcm->resource_x_reconfig_proof_pass) != 0))
		return pcm_resource_x_reconfig_return_release(
			&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
	if (pg_atomic_read_u64(&ClusterPcm->resource_x_activation_inflight_count) != 0)
		return pcm_resource_x_reconfig_return_release(
			&entry_ref, RESOURCE_X_RECONFIG_MORE);

	capacity = pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_scan_capacity);
	if (capacity > (uint64)pcm_grd_effective || cluster_pcm_resource_x_slots == NULL) {
		pcm_resource_x_reconfig_block();
		return pcm_resource_x_reconfig_return_release(
			&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
	}

	cursor = pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_next_state_index);
	if (cursor > capacity) {
		pcm_resource_x_reconfig_block();
		return pcm_resource_x_reconfig_return_release(
			&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
	}
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_reconfig_proof_pass) == 2) {
		bool proof_valid;

		LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
		proof_valid = pcm_resource_x_zero_proof_live_exact(
			token, &ClusterPcm->resource_x_zero_residual_proof);
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		if (!proof_valid)
			return pcm_resource_x_reconfig_return_release(
				&entry_ref, RESOURCE_X_RECONFIG_RETRY);
		out->complete_wrap = 1;
		out->zero_residual = 1;
		return RESOURCE_X_RECONFIG_DONE;
	}
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_reconfig_proof_pass) == 1)
		return pcm_resource_x_reconfig_proof_sweep(
			token, probe_budget, out, capacity, cursor);
	probes = 0;
	while (probes < probe_budget && cursor < capacity) {
		slot = &cluster_pcm_resource_x_slots[cursor];
		if (slot->state == PCM_REGISTRY_EMPTY
			|| slot->state == PCM_REGISTRY_TOMBSTONE) {
			cursor++;
			probes++;
			out->examined_count++;
			pg_atomic_fetch_add_u64(&ClusterPcm->resource_x_reconfig_slot_examined_count, 1);
			continue;
		}
		if (slot->state != PCM_REGISTRY_LIVE) {
			pcm_resource_x_reconfig_block();
			return pcm_resource_x_reconfig_return_release(
				&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
		}

		Assert(!entry_ref.pinned);
		if (!pcm_entry_ref_acquire(&slot->tag, false,
				&entry_ref, &acquire_result)) {
			pcm_resource_x_reconfig_block();
			return pcm_resource_x_reconfig_return_release(
				&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
		}
		entry = entry_ref.entry;
		pcm_entry_lock_exclusive(entry);
		if (!BufferTagsEqual(&entry->tag, &slot->tag)
			|| entry_ref.binding_generation != slot->binding_generation
			|| entry_ref.registry_slot != (uint32)cursor) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_resource_x_reconfig_block();
			return pcm_resource_x_reconfig_return_release(
				&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
		}
		formation = entry->resource_x_formation;
		progress_flags = entry->resource_x_progress_flags;
		dead_requester_bitmap = token->dead_requester_bitmap;
		master_state = pcm_resource_x_master_state_for_entry(entry);
		if (master_state == NULL) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_resource_x_reconfig_block();
			return pcm_resource_x_reconfig_return_release(
				&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
		}
		if (!pcm_resource_x_source_settlement_debt_decode_locked(
				entry, master_state, &source_settlement)
			|| (master_state->source_settlement.state
					!= RESOURCE_X_SOURCE_SETTLEMENT_NONE
				&& source_settlement.common.resource_formation
					!= token->old_formation
				&& (token->new_formation == 0
					|| source_settlement.common.resource_formation
						!= token->new_formation))) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_resource_x_reconfig_block();
			return pcm_resource_x_reconfig_return_release(
				&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
		}
		/* PENDING remains the sole retry owner for a possibly still-REVOKING
		 * source and therefore survives R8 as an old residual.  Only an exact
		 * kind-11 ACK makes the bounded history safe to discard. */
		if (master_state->source_settlement.state
				== RESOURCE_X_SOURCE_SETTLEMENT_ACKED
			&& source_settlement.common.resource_formation
				== token->old_formation)
			memset(&master_state->source_settlement, 0,
				sizeof(master_state->source_settlement));
		bootstrap_priority_invalidated = false;
		if (!pcm_resource_x_bootstrap_priority_valid(
				&master_state->bootstrap_priority)
			|| (master_state->bootstrap_priority.state
					!= RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY
				&& (!BufferTagsEqual(
						&master_state->bootstrap_priority.request
							.logical_assertion.resource,
						&entry->tag)
					|| (master_state->bootstrap_priority.request
							.resource_formation != token->old_formation
						&& master_state->bootstrap_priority.request
							.resource_formation != token->new_formation)))) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_resource_x_reconfig_block();
			return pcm_resource_x_reconfig_return_release(
				&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
		}
		if (master_state->bootstrap_priority.state
				== RESOURCE_X_BOOTSTRAP_PRIORITY_NEXT_ADMISSION
			&& master_state->bootstrap_priority.request.resource_formation
				== token->old_formation) {
			pcm_resource_x_bootstrap_priority_clear(
				&master_state->bootstrap_priority);
			bootstrap_priority_invalidated = true;
		}
		bootstrap_receipt_invalidated = false;
		for (bootstrap_requester_node = 0;
			 bootstrap_requester_node < RESOURCE_X_PROTOCOL_NODE_LIMIT;
			 bootstrap_requester_node++) {
			ClusterPcmResourceXBootstrapReceipt *receipt
				= &master_state->bootstrap_receipts[
					bootstrap_requester_node];

			if (!pcm_resource_x_bootstrap_receipt_valid(receipt)
				|| (receipt->state != RESOURCE_X_BOOTSTRAP_RECEIPT_EMPTY
					&& receipt->request.logical_assertion.requester_node
						!= bootstrap_requester_node)
				|| (receipt->state != RESOURCE_X_BOOTSTRAP_RECEIPT_EMPTY
					&& receipt->request.resource_formation
						!= token->old_formation
					&& receipt->request.resource_formation
						!= token->new_formation)) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_resource_x_reconfig_block();
				return pcm_resource_x_reconfig_return_release(
					&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
			}
			if (receipt->state != RESOURCE_X_BOOTSTRAP_RECEIPT_EMPTY
				&& receipt->request.resource_formation
					== token->old_formation) {
				pcm_resource_x_bootstrap_receipt_invalidate(receipt);
				bootstrap_receipt_invalidated = true;
			}
		}
		bootstrap_round_invalidated = false;
		if (!pcm_resource_x_local_owner_valid_locked(
				&entry->resource_x_local_owner)) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_resource_x_reconfig_block();
			return pcm_resource_x_reconfig_return_release(
				&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
		}
		local_priority_cleared
			= pcm_resource_x_local_owner_expire_locked(entry, pcm_resource_x_monotonic_us());
		if ((entry->resource_x_local_owner.state
				== RESOURCE_X_LOCAL_OWNER_RECYCLING
			|| entry->resource_x_local_owner.state
				== RESOURCE_X_LOCAL_OWNER_HANDOFF)
			&& pcm_resource_x_local_handoff_valid(
				&entry->resource_x_local_owner.handoff)
			&& entry->resource_x_local_owner.handoff.holder_ref.formation
				== token->old_formation)
			local_priority_cleared
				= pcm_resource_x_local_owner_priority_clear_locked(entry) || local_priority_cleared;
		if (local_priority_cleared)
			ConditionVariableBroadcast(&entry->wait_cv);
		if (entry->resource_x_local_owner.state
			!= RESOURCE_X_LOCAL_OWNER_EMPTY) {
			LWLockRelease(&entry->entry_lock.lock);
			return pcm_resource_x_reconfig_return_release(
				&entry_ref, RESOURCE_X_RECONFIG_MORE);
		}
		if (entry->resource_x_bootstrap_round.phase
			!= RESOURCE_X_BOOTSTRAP_ROUND_EMPTY) {
			ClusterPcmResourceXBootstrapRound *round
				= &entry->resource_x_bootstrap_round;

			if (round->phase > RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED
				|| !resource_x_assertion_valid(
					&round->request.logical_assertion)
				|| !BufferTagsEqual(
					&round->request.logical_assertion.resource, &entry->tag)
				|| round->request.logical_assertion.requester_node
					!= cluster_node_id
				|| round->request.assertion_sequence == 0
				|| round->request.assertion_sequence == UINT64_MAX
				|| round->request.assertion_sequence
					!= round->highest_attempt_floor
				|| round->resource_formation
					!= round->request.resource_formation
				|| (round->resource_formation != token->old_formation
					&& round->resource_formation != token->new_formation)) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_resource_x_reconfig_block();
				return pcm_resource_x_reconfig_return_release(
					&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
			}
			if (round->resource_formation == token->old_formation) {
				pcm_resource_x_bootstrap_round_clear_binding_locked(round);
				bootstrap_round_invalidated = true;
			}
		} else if (entry->resource_x_bootstrap_round.highest_attempt_floor
				   == UINT64_MAX) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_resource_x_reconfig_block();
			return pcm_resource_x_reconfig_return_release(
				&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
		}
		if (bootstrap_priority_invalidated
			|| bootstrap_receipt_invalidated
			|| bootstrap_round_invalidated)
			ConditionVariableBroadcast(&entry->wait_cv);
		if (!pcm_resource_x_requester_join_empty_locked(
				&master_state->requester_join)) {
			if (!pcm_resource_x_requester_join_decode_locked(
					&master_state->requester_join, &join_grant, &join_image)
				|| !pcm_resource_x_requester_join_snapshot_locked(
					&master_state->requester_join, &join_snapshot)) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_resource_x_reconfig_block();
				return pcm_resource_x_reconfig_return_release(
					&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
			}
			if (pcm_resource_x_active_empty_locked(entry)) {
				LWLockRelease(&entry->entry_lock.lock);
				cursor++;
				out->examined_count++;
				out->orphan_count = 1;
				out->residual_count = 1;
				out->next_state_index = cursor;
				pg_atomic_fetch_add_u64(
					&ClusterPcm->resource_x_reconfig_slot_examined_count, 1);
				pg_atomic_fetch_add_u64(
					&ClusterPcm->resource_x_reconfig_orphan_count, 1);
				pg_atomic_write_u64(
					&ClusterPcm->resource_x_reconfig_residual_count, 1);
				pg_atomic_write_u64(
					&ClusterPcm->resource_x_reconfig_next_state_index, cursor);
				pcm_resource_x_reconfig_block();
				return pcm_resource_x_reconfig_return_release(
					&entry_ref, RESOURCE_X_RECONFIG_ORPHAN);
			}
			if (!resource_x_assertion_equal(
					&join_snapshot.assertion,
					&(ResourceXAssertion){ .resource = entry->tag,
						.requester_node = entry->resource_x_requester_node })
				|| join_snapshot.resource_formation != entry->resource_x_formation
				|| join_snapshot.requester_target_generation
					   != entry->resource_x_acquisition_generation) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_resource_x_reconfig_block();
				return pcm_resource_x_reconfig_return_release(
					&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
			}
		}
		/* A proof-bearing image makes this the inseparable retained carrier
		 * pair.  Status-only noncarrier replies keep their existing independent
		 * R8 retirement rule. */
		holder_pair_present = master_state->holder_image.valid != 0;
		holder_pair_old_unsettled = false;
		if (holder_pair_present) {
			int32 pair_requester_node;
			bool pair_exactly_drained;

			if (pcm_resource_x_holder_pair_decode_locked(
					master_state, &holder_pair_status,
					&holder_pair_image) != RESOURCE_X_APPLY_APPLIED
				|| (holder_pair_status.common.resource_formation
						!= token->old_formation
					&& holder_pair_status.common.resource_formation
							!= token->new_formation)) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_resource_x_reconfig_block();
				return pcm_resource_x_reconfig_return_release(
					&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
			}
			pair_requester_node
				= holder_pair_status.common.logical_assertion.requester_node;
			pair_exactly_drained
				= master_state->holder_status.valid
					== RESOURCE_X_HOLDER_PAIR_PUBLISHED
				&& master_state->holder_image.valid
					== RESOURCE_X_HOLDER_PAIR_PUBLISHED
				&& master_state->holder_status_intent.slot.state
					== RESOURCE_X_INTENT_SLOT_EMPTY
				&& master_state->holder_image_intent.state
					== RESOURCE_X_INTENT_SLOT_EMPTY
				&& pair_requester_node >= 0
				&& pair_requester_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
				&& master_state->holder_pair_drained_sequences[
						pair_requester_node]
					== holder_pair_status.common.assertion_sequence
				&& master_state->holder_pair_drained_resource_formation
					== holder_pair_status.common.resource_formation
				&& master_state->holder_pair_drained_master_session
					== holder_pair_status.common.master_session_incarnation
				&& master_state->holder_pair_drained_master_node
					== (int32)master_state->holder_status.destination_node
				&& master_state->holder_pair_drained_reserved == 0;
			if (holder_pair_status.common.resource_formation
					== token->old_formation) {
				if (!pair_exactly_drained)
					holder_pair_old_unsettled = true;
				else {
					memset(&master_state->holder_status, 0,
						sizeof(master_state->holder_status));
					memset(&master_state->holder_status_intent, 0,
						sizeof(master_state->holder_status_intent));
					memset(&master_state->holder_image, 0,
						sizeof(master_state->holder_image));
					memset(&master_state->holder_image_intent, 0,
						sizeof(master_state->holder_image_intent));
				}
			}
		}
		else {
			if (master_state->holder_image_intent.state
					!= RESOURCE_X_INTENT_SLOT_EMPTY
				|| (master_state->holder_status.valid == 0
					&& master_state->holder_status_intent.slot.state
						!= RESOURCE_X_INTENT_SLOT_EMPTY)) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_resource_x_reconfig_block();
				return pcm_resource_x_reconfig_return_release(
					&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
			}
			if (master_state->holder_status.valid != 0) {
				if ((master_state->holder_status.valid
						 != RESOURCE_X_HOLDER_PAIR_PENDING
					 && master_state->holder_status.valid
						 != RESOURCE_X_HOLDER_PAIR_PUBLISHED)
					|| master_state->holder_status.resource_formation == 0
					|| (master_state->holder_status.resource_formation
							!= token->old_formation
						&& master_state->holder_status.resource_formation
								!= token->new_formation)) {
					LWLockRelease(&entry->entry_lock.lock);
					pcm_resource_x_reconfig_block();
					return pcm_resource_x_reconfig_return_release(
						&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
				}
				if (master_state->holder_status.resource_formation
						== token->old_formation) {
					memset(&master_state->holder_status, 0,
						sizeof(master_state->holder_status));
					memset(&master_state->holder_status_intent, 0,
						sizeof(master_state->holder_status_intent));
				}
			}
		}
		holder_pair_drained = false;
		for (holder_pair_node = 0;
			 holder_pair_node < RESOURCE_X_PROTOCOL_NODE_LIMIT;
			 holder_pair_node++)
			if (master_state->holder_pair_drained_sequences[holder_pair_node]
					!= 0) {
				holder_pair_drained = true;
				break;
			}
		if (holder_pair_drained) {
			if (master_state->holder_pair_drained_resource_formation == 0
				|| master_state->holder_pair_drained_master_session == 0
				|| master_state->holder_pair_drained_master_node < 0
				|| master_state->holder_pair_drained_master_node
					>= RESOURCE_X_PROTOCOL_NODE_LIMIT
				|| master_state->holder_pair_drained_reserved != 0
				|| (master_state->holder_pair_drained_resource_formation
						!= token->old_formation
					&& master_state->holder_pair_drained_resource_formation
							!= token->new_formation)) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_resource_x_reconfig_block();
				return pcm_resource_x_reconfig_return_release(
					&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
			}
			if (master_state->holder_pair_drained_resource_formation
					== token->old_formation
				&& !holder_pair_old_unsettled) {
				memset(master_state->holder_pair_drained_sequences, 0,
					   sizeof(master_state->holder_pair_drained_sequences));
				master_state->holder_pair_drained_resource_formation = 0;
				master_state->holder_pair_drained_master_session = 0;
				master_state->holder_pair_drained_master_node = 0;
				master_state->holder_pair_drained_reserved = 0;
			}
		} else if (master_state->holder_pair_drained_resource_formation != 0
				   || master_state->holder_pair_drained_master_session != 0
				   || master_state->holder_pair_drained_master_node != 0
				   || master_state->holder_pair_drained_reserved != 0) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_resource_x_reconfig_block();
			return pcm_resource_x_reconfig_return_release(
				&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
		}
		for (dead_node = 0; dead_node < RESOURCE_X_PROTOCOL_NODE_LIMIT;
			 dead_node++) {
			ResourceXReclaimResult reclaim_result;
			ResourceXReclaimWitness *witness;
			bool reclaim_broadcast = false;

			if ((dead_requester_bitmap & (UINT32_C(1) << (uint32)dead_node)) == 0)
				continue;
			if (out->reclaim_count >= RESOURCE_X_RECONFIG_RECLAIM_WITNESS_MAX) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_resource_x_reconfig_block();
				return pcm_resource_x_reconfig_return_release(
					&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
			}
			witness = &out->reclaim_witnesses[out->reclaim_count];
			reclaim_result = pcm_resource_x_reclaim_requester_locked(
				entry, master_state, dead_node, token->old_formation, witness,
				&reclaim_broadcast);
			if (reclaim_result == RESOURCE_X_RECLAIM_NONE)
				continue;
			out->reclaim_count++;
			switch (reclaim_result) {
				case RESOURCE_X_RECLAIM_NONHEAD:
					out->reclaim_nonhead_count++;
					out->old_detached_count++;
					pg_atomic_fetch_add_u64(
						&ClusterPcm->resource_x_reconfig_reclaim_nonhead_count, 1);
					pg_atomic_fetch_add_u64(
						&ClusterPcm->resource_x_reconfig_old_detached_count, 1);
					break;
				case RESOURCE_X_RECLAIM_HEAD_SUCCESSOR_STARTED:
					out->reclaim_head_count++;
					out->old_detached_count++;
					pg_atomic_fetch_add_u64(
						&ClusterPcm->resource_x_reconfig_reclaim_head_count, 1);
					pg_atomic_fetch_add_u64(
						&ClusterPcm->resource_x_reconfig_old_detached_count, 1);
					break;
				case RESOURCE_X_RECLAIM_ORPHAN_BLOCKED:
					out->reclaim_orphan_count++;
					pg_atomic_fetch_add_u64(
						&ClusterPcm->resource_x_reconfig_reclaim_orphan_count, 1);
					result = RESOURCE_X_RECONFIG_ORPHAN;
					first_orphan = true;
					break;
				case RESOURCE_X_RECLAIM_NONE:
				default:
					LWLockRelease(&entry->entry_lock.lock);
					pcm_resource_x_reconfig_block();
					return pcm_resource_x_reconfig_return_release(
						&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
			}
			if (reclaim_broadcast)
				ConditionVariableBroadcast(&entry->wait_cv);
		}
		if (formation == token->old_formation
			&& entry->resource_x_acquisition_generation != 0) {
			if (entry->resource_x_requester_node < 0
				|| (progress_flags & ~RESOURCE_X_PROGRESS_KNOWN_MASK) != 0) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_resource_x_reconfig_block();
				return pcm_resource_x_reconfig_return_release(
					&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
			}

			/* A T2/T3-gap sidecar belongs to the buffer owner.  Snapshot the
			 * complete Resource-X identity, drop this entry lock, and never
			 * carry the shared entry pointer across that authority boundary. */
			if (progress_flags
				== (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1
					| RESOURCE_X_PROGRESS_T2)) {
				snapshot_tag = entry->tag;
				snapshot_requester_node = entry->resource_x_requester_node;
				snapshot_acquisition_generation
					= entry->resource_x_acquisition_generation;
				snapshot_progress_flags = progress_flags;
				snapshot_no_progress_generation
					= entry->resource_x_no_progress_generation;
				snapshot_no_progress_reason = entry->resource_x_no_progress_reason;
				snapshot_dispatch_phase = entry->resource_x_dispatch_phase;
				LWLockRelease(&entry->entry_lock.lock);
				entry = NULL;

				sidecar_result = cluster_bufmgr_resource_x_neutralize_exact(
					&snapshot_tag, token->old_formation,
					snapshot_acquisition_generation);

				entry = entry_ref.entry;
				pcm_entry_lock_exclusive(entry);
				if (!BufferTagsEqual(&entry->tag, &snapshot_tag)
					|| entry->resource_x_requester_node != snapshot_requester_node
					|| entry->resource_x_formation != token->old_formation
					|| entry->resource_x_acquisition_generation
						   != snapshot_acquisition_generation
					|| entry->resource_x_progress_flags != snapshot_progress_flags
					|| entry->resource_x_no_progress_generation
						   != snapshot_no_progress_generation
					|| entry->resource_x_no_progress_reason != snapshot_no_progress_reason
					|| entry->resource_x_dispatch_phase != snapshot_dispatch_phase) {
					LWLockRelease(&entry->entry_lock.lock);
					out->retry_count = 1;
					out->next_state_index = cursor;
					pg_atomic_fetch_add_u64(&ClusterPcm->resource_x_reconfig_retry_count, 1);
					pg_atomic_fetch_add_u64(
						&ClusterPcm->resource_x_reconfig_proof_retry_revisit_count,
						1);
					pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_next_state_index,
									 cursor);
					return pcm_resource_x_reconfig_return_release(
						&entry_ref, RESOURCE_X_RECONFIG_RETRY);
				}

				switch (sidecar_result) {
					case RESOURCE_X_SIDECAR_NEUTRALIZED:
						out->sidecar_neutralized_count++;
						pg_atomic_fetch_add_u64(
							&ClusterPcm->resource_x_reconfig_sidecar_neutralized_count, 1);
						break;
					case RESOURCE_X_SIDECAR_ALREADY_CLEAR:
						break;
					case RESOURCE_X_SIDECAR_SUCCESSOR:
					case RESOURCE_X_SIDECAR_STALE:
						pg_atomic_fetch_add_u64(
							&ClusterPcm->resource_x_reconfig_sidecar_stale_count, 1);
						break;
					case RESOURCE_X_SIDECAR_BUSY:
						break;
					case RESOURCE_X_SIDECAR_CORRUPT:
					default:
						LWLockRelease(&entry->entry_lock.lock);
						pcm_resource_x_reconfig_block();
						return pcm_resource_x_reconfig_return_release(
							&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
				}
			}
			else if (progress_flags
					 != (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1)) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_resource_x_reconfig_block();
				return pcm_resource_x_reconfig_return_release(
					&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
			}

			if ((progress_flags & RESOURCE_X_PROGRESS_RECOVERY_BLOCKED) == 0) {
				entry->resource_x_progress_flags |= RESOURCE_X_PROGRESS_RECOVERY_BLOCKED;
				first_orphan = true;
			}
			result = RESOURCE_X_RECONFIG_ORPHAN;
		}
		else if (token->new_formation != 0
				 && formation == token->new_formation
				 && entry->resource_x_acquisition_generation != 0) {
			out->successor_count++;
			pg_atomic_fetch_add_u64(&ClusterPcm->resource_x_reconfig_successor_count, 1);
		}
		else if (formation != 0 || entry->resource_x_acquisition_generation != 0
				 || entry->resource_x_requester_node != -1 || progress_flags != 0) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_resource_x_reconfig_block();
			return pcm_resource_x_reconfig_return_release(
				&entry_ref, RESOURCE_X_RECONFIG_CORRUPT);
		}
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);

		cursor++;
		probes++;
		out->examined_count++;
		pg_atomic_fetch_add_u64(&ClusterPcm->resource_x_reconfig_slot_examined_count, 1);
		if (result == RESOURCE_X_RECONFIG_ORPHAN)
			break;
	}

	if (result == RESOURCE_X_RECONFIG_ORPHAN) {
		out->orphan_count = 1;
		out->residual_count = 1;
		pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_residual_count, 1);
		if (first_orphan)
			pg_atomic_fetch_add_u64(&ClusterPcm->resource_x_reconfig_orphan_count, 1);
		pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_next_state_index, cursor);
		out->next_state_index = cursor;
		pcm_resource_x_reconfig_block();
		return result;
	}

	if (cursor < capacity) {
		pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_next_state_index, cursor);
		out->next_state_index = cursor;
		return pcm_resource_x_reconfig_return_release(
			&entry_ref, RESOURCE_X_RECONFIG_MORE);
	}
	if (pending_token) {
		/* This is the required pre-bind active/join sweep, not an R8 zero
		 * proof.  Binding the externally current formation mutates the
		 * sequence, after which the ordinary two-pass proof sweep starts at
		 * cursor zero and publishes the immutable same-token proof. */
		pg_atomic_write_u64(
			&ClusterPcm->resource_x_reconfig_residual_count, 0);
		pg_atomic_write_u64(
			&ClusterPcm->resource_x_reconfig_next_state_index, 0);
		out->next_state_index = 0;
		out->complete_wrap = 1;
		out->zero_residual = 0;
		return RESOURCE_X_RECONFIG_DONE;
	}

	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_begin_sequence,
		pg_atomic_read_u64(
			&ClusterPcm->resource_x_semantic_mutation_sequence));
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_begin_slot_count,
		pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_slot_count));
	pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_proof_digest,
						 PGRAC_RESOURCE_X_PROOF_DIGEST_OFFSET);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_empty_slot_count, 0);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_successor_slot_count, 0);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_reconfig_proof_terminal_slot_count, 0);
	pg_atomic_write_u32(&ClusterPcm->resource_x_reconfig_proof_pass, 1);
	pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_zero_proof_generation,
						 0);
	pg_atomic_write_u64(&ClusterPcm->resource_x_reconfig_next_state_index, 0);
	out->complete_wrap = 1;
	out->zero_residual = 0;
	return pcm_resource_x_reconfig_return_release(
		&entry_ref, RESOURCE_X_RECONFIG_MORE);
}

bool
cluster_resource_x_reconfig_zero_proof_exact(
	const ResourceXReconfigToken *token, ResourceXZeroResidualProof *out)
{
	bool valid;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (ClusterPcm == NULL || token == NULL || out == NULL)
		return false;
	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	valid = pcm_resource_x_zero_proof_live_exact(
		token, &ClusterPcm->resource_x_zero_residual_proof);
	if (valid)
		*out = ClusterPcm->resource_x_zero_residual_proof;
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	return valid;
}

static bool
pcm_resource_x_terminal_request_identity_valid(
	const ClusterPcmResourceXMasterRequest *request,
	bool allow_settled)
{
	if (request == NULL
		|| (request->phase != RESOURCE_X_MASTER_RELEASED
			&& (!allow_settled
				|| request->phase != RESOURCE_X_MASTER_SETTLED))
		|| request->base_authority_generation == 0
		|| request->base_authority_generation == UINT64_MAX
		|| request->resource_formation == 0
		|| request->resource_formation == UINT64_MAX
		|| request->master_session_incarnation == 0
		|| request->master_session_incarnation == UINT64_MAX
		|| request->assertion_sequence == 0
		|| request->assertion_sequence == UINT64_MAX
		|| request->enqueue_order == 0
		|| request->enqueue_order == UINT64_MAX
		|| request->final_authority_generation
			<= request->base_authority_generation
		|| request->final_authority_generation == UINT64_MAX
		|| request->requester_target_generation == 0
		|| request->requester_target_generation == UINT64_MAX
		|| request->settlement_requester_connection_generation == 0
		|| request->settlement_requester_connection_generation == UINT64_MAX
		|| request->sender_connection_generation == 0
		|| request->sender_connection_generation == UINT32_MAX
		|| request->dependency_count > RESOURCE_X_DEPENDENCY_MAX
		|| request->proof_kind < RESOURCE_X_PROOF_KIND_MIN
		|| request->proof_kind > RESOURCE_X_PROOF_KIND_MAX
		|| request->reserved != 0
		|| (request->blocked_holders_bitmap
			& ~request->incompatible_holders_bitmap) != 0)
		return false;
	if (request->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER)
		return request->source_carrier_generation != 0
			&& request->source_carrier_generation != UINT64_MAX
			&& request->source_disposition
				== RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
	if (request->proof_kind == RESOURCE_X_PROOF_LOCAL_IMAGE)
		return request->source_disposition
			== RESOURCE_X_DISPOSITION_LOCAL_IMAGE;
	return request->source_disposition
		== RESOURCE_X_DISPOSITION_DURABLE_STORAGE;
}

/* One complete terminal-sidecar validity table serves both the frozen R10
 * clean proof and the read-only t/430 debt projection.  The latter may admit
 * exact stable cached residency, but never relaxes active/retained/invalid
 * state. */
static bool
pcm_resource_x_terminal_state_locked(
	struct GrdEntry *entry,
	const ClusterPcmResourceXMasterState *state,
	bool allow_stable_residency, bool allow_drained_pair_history,
	uint64 *terminal_request_count_io, uint64 *digest_io)
{
	static const ClusterPcmResourceXMasterRequest empty_request;
	static const ClusterPcmResourceXTerminalTombstone empty_tombstone;
	ClusterPcmResourceXBootstrapRound empty_round;
	ResourceXDecodedFrame drained_image;
	ResourceXDecodedFrame drained_status;
	ResourceXDecodedFrame source_settlement;
	uint64 digest;
	bool drained = false;
	bool drained_pair_history = false;
	int32 settled_terminal_node = -1;
	int node;

	Assert(entry != NULL);
	Assert(state != NULL);
	Assert(terminal_request_count_io != NULL);
	Assert(digest_io != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED));
	memset(&empty_round, 0, sizeof(empty_round));
	empty_round.highest_attempt_floor
		= entry->resource_x_bootstrap_round.highest_attempt_floor;
	if (entry->resource_x_bootstrap_round.phase
			== RESOURCE_X_BOOTSTRAP_ROUND_EMPTY) {
		if (empty_round.highest_attempt_floor == UINT64_MAX
			|| memcmp(&entry->resource_x_bootstrap_round, &empty_round,
				sizeof(empty_round)) != 0)
			return false;
	}
	else if (allow_stable_residency
			 && entry->resource_x_bootstrap_round.phase
				== RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
			 && pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
				entry, &entry->resource_x_bootstrap_round,
				&entry->resource_x_bootstrap_round.terminal_ref,
				entry->resource_x_bootstrap_round.master_session_incarnation,
				entry->resource_x_bootstrap_round.r4_record_generation,
				entry->resource_x_bootstrap_round.cached_ownership_generation))
		;
	else
		return false;
	if (state->holder_status.valid != 0 || state->holder_image.valid != 0
		|| state->holder_status_intent.slot.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY
		|| state->holder_image_intent.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY) {
		if (!allow_drained_pair_history
			|| !pcm_resource_x_holder_pair_drained_history_locked(
				entry, state, &drained_status, &drained_image))
			return false;
		drained_pair_history = true;
	}
	if (!pcm_resource_x_active_empty_locked(entry)
		|| !pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner)
		|| entry->resource_x_local_owner.state
			!= RESOURCE_X_LOCAL_OWNER_EMPTY
		|| state->grant_intent.slot.state != RESOURCE_X_INTENT_SLOT_EMPTY
		|| state->requester_settlement_intent.slot.state
			   != RESOURCE_X_INTENT_SLOT_EMPTY
		|| !pcm_resource_x_requester_join_empty_locked(
			&state->requester_join)
		|| !pcm_resource_x_bootstrap_priority_valid(
			&state->bootstrap_priority)
		|| state->bootstrap_priority.state
			!= RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY)
		return false;
	if (!pcm_resource_x_source_settlement_debt_decode_locked(
			entry, state, &source_settlement)
		|| state->source_settlement.state
			== RESOURCE_X_SOURCE_SETTLEMENT_PENDING)
		return false;

	digest = pcm_resource_x_proof_digest_tag(*digest_io, &entry->tag);
	digest = pcm_resource_x_proof_digest_u64(
		digest, entry->resource_x_bootstrap_round.highest_attempt_floor);
	digest = pcm_resource_x_proof_digest_u64(
		digest, entry->resource_x_local_owner.highest_owner_generation);
	digest = pcm_resource_x_proof_digest_u64(
		digest, (uint64)state->source_settlement.state);
	if (drained_pair_history) {
		(*terminal_request_count_io)++;
		digest = pcm_resource_x_proof_digest_u64(
			digest, drained_status.common.assertion_sequence);
		digest = pcm_resource_x_proof_digest_u64(
			digest, drained_status.common.semantic_crc32c);
		digest = pcm_resource_x_proof_digest_u64(
			digest, drained_image.common.semantic_crc32c);
		digest = pcm_resource_x_proof_digest_u64(
			digest,
			drained_image.body.image_envelope.source_carrier_generation);
	}
	if (state->source_settlement.state
			== RESOURCE_X_SOURCE_SETTLEMENT_ACKED) {
		(*terminal_request_count_io)++;
		digest = pcm_resource_x_proof_digest_u64(
			digest, source_settlement.common.resource_formation);
		digest = pcm_resource_x_proof_digest_u64(
			digest, source_settlement.common.assertion_sequence);
		digest = pcm_resource_x_proof_digest_u64(
			digest, source_settlement.common.authority_generation);
		digest = pcm_resource_x_proof_digest_u64(
			digest,
			source_settlement.body.blocked_to_n.source_carrier_generation);
	}
	for (node = 0; node < RESOURCE_X_PROTOCOL_NODE_LIMIT; node++) {
		const ClusterPcmResourceXBootstrapReceipt *receipt
			= &state->bootstrap_receipts[node];
		const ClusterPcmResourceXMasterRequest *request
			= &state->requests[node];

		if (!pcm_resource_x_bootstrap_receipt_valid(receipt)
			|| receipt->state != RESOURCE_X_BOOTSTRAP_RECEIPT_EMPTY)
			return false;
		digest = pcm_resource_x_proof_digest_u64(
			digest, receipt->highest_attempt_floor);
		if (state->block_intents[node].slot.state
				!= RESOURCE_X_INTENT_SLOT_EMPTY)
			return false;
		if (request->phase == RESOURCE_X_MASTER_NONE) {
			if (memcmp(request, &empty_request, sizeof(*request)) != 0)
				return false;
		}
		else if (request->phase == RESOURCE_X_MASTER_RELEASED
				 || (allow_stable_residency
					 && request->phase == RESOURCE_X_MASTER_SETTLED)) {
			if (!pcm_resource_x_terminal_request_identity_valid(
					request, allow_stable_residency))
				return false;
			if (request->phase == RESOURCE_X_MASTER_SETTLED) {
				if (settled_terminal_node != -1)
					return false;
				settled_terminal_node = node;
			}
			(*terminal_request_count_io)++;
			digest = pcm_resource_x_proof_digest_u64(
				digest, request->resource_formation);
			digest = pcm_resource_x_proof_digest_u64(
				digest, request->assertion_sequence);
		}
		else
			return false;
		if (state->holder_pair_drained_sequences[node] != 0) {
			drained = true;
			(*terminal_request_count_io)++;
			digest = pcm_resource_x_proof_digest_u64(
				digest,
				state->holder_pair_drained_sequences[node]);
		}
	}
	if (state->terminal_tombstone.valid == 0) {
		if (memcmp(&state->terminal_tombstone, &empty_tombstone,
				   sizeof(empty_tombstone)) != 0)
			return false;
	}
	else if (state->terminal_tombstone.valid != 1
			 || state->terminal_tombstone.requester_node < 0
			 || state->terminal_tombstone.requester_node
					>= RESOURCE_X_PROTOCOL_NODE_LIMIT
			 || !pcm_resource_x_terminal_request_identity_valid(
				&state->terminal_tombstone.request,
				allow_stable_residency)) {
		return false;
	}
	else {
		if (state->terminal_tombstone.request.phase
				== RESOURCE_X_MASTER_SETTLED) {
			if (!allow_stable_residency || settled_terminal_node != -1)
				return false;
			settled_terminal_node
				= state->terminal_tombstone.requester_node;
		}
		(*terminal_request_count_io)++;
		digest = pcm_resource_x_proof_digest_u64(
			digest,
			state->terminal_tombstone.request.assertion_sequence);
	}
	if (drained) {
		if (state->holder_pair_drained_resource_formation == 0
			|| state->holder_pair_drained_resource_formation == UINT64_MAX
			|| state->holder_pair_drained_master_session == 0
			|| state->holder_pair_drained_master_session == UINT64_MAX
			|| state->holder_pair_drained_master_node < 0
			|| state->holder_pair_drained_master_node
				   >= RESOURCE_X_PROTOCOL_NODE_LIMIT
			|| state->holder_pair_drained_reserved != 0)
			return false;
	}
	else if (state->holder_pair_drained_resource_formation != 0
			 || state->holder_pair_drained_master_session != 0
			 || state->holder_pair_drained_master_node != 0
			 || state->holder_pair_drained_reserved != 0)
		return false;
	if (settled_terminal_node != -1) {
		uint32 settled_bit
			= UINT32_C(1) << (uint32)settled_terminal_node;
		uint32 s_holders = pg_atomic_read_u32(&entry->s_holders_bitmap);
		uint32 pi_holders = pg_atomic_read_u32(&entry->pi_holders_bitmap);
		PcmState master_state
			= (PcmState)pg_atomic_read_u32(&entry->master_state);
		bool stable_cached;

		if (master_state == PCM_STATE_X)
			stable_cached = entry->x_holder_node == settled_terminal_node
				&& s_holders == 0;
		else if (master_state == PCM_STATE_S)
			stable_cached = entry->x_holder_node == -1
				&& (s_holders & settled_bit) != 0;
		else
			stable_cached = master_state == PCM_STATE_N
				&& entry->x_holder_node == -1 && s_holders == 0
				&& (pi_holders & settled_bit) != 0;
		if (!stable_cached || entry->pending_x_requester_node != -1
			|| entry->pending_x_since_lsn != 0)
			return false;
	}
	if (state->authority_generation == 0
		|| state->authority_generation == UINT64_MAX
		|| state->next_enqueue_order == 0
		|| state->next_enqueue_order == UINT64_MAX)
		return false;
	digest = pcm_resource_x_proof_digest_u64(
		digest, state->authority_generation);
	digest = pcm_resource_x_proof_digest_u64(
		digest, state->next_enqueue_order);
	*digest_io = digest;
	return true;
}

static bool
pcm_resource_x_clean_state_locked(
	struct GrdEntry *entry,
	const ClusterPcmResourceXMasterState *state,
	uint64 *terminal_request_count_io, uint64 *digest_io)
{
	return pcm_resource_x_terminal_state_locked(
		entry, state, false, false, terminal_request_count_io, digest_io);
}

static bool
pcm_resource_x_clean_proof_phase_exact(
	const ResourceXReconfigToken *token,
	const ResourceXZeroResidualProof *zero_proof,
	const ResourceXCleanCompletionProof *clean_proof, uint32 expected_phase)
{
	return token != NULL && zero_proof != NULL && clean_proof != NULL
		&& pcm_resource_x_zero_proof_phase_exact(
			token, zero_proof, expected_phase)
		&& memcmp(zero_proof, &ClusterPcm->resource_x_zero_residual_proof,
				  sizeof(*zero_proof)) == 0
		&& memcmp(&clean_proof->token, token, sizeof(*token)) == 0
		&& clean_proof->proof_generation == token->freeze_generation
		&& clean_proof->final_mutation_sequence
			   == zero_proof->final_mutation_sequence
		&& clean_proof->final_mutation_sequence
			   == pg_atomic_read_u64(
				   &ClusterPcm->resource_x_semantic_mutation_sequence)
		&& clean_proof->scan_capacity == zero_proof->scan_capacity
		&& clean_proof->logical_slot_count
			   == zero_proof->scan_end_slot_count
		&& clean_proof->logical_digest != 0
		&& clean_proof->transport_mutation_sequence != 0
		&& clean_proof->transport_mutation_sequence != UINT64_MAX
		&& clean_proof->transport_staged_count == 0
		&& clean_proof->logical_debt_zero == 1
		&& clean_proof->transport_debt_zero == 1
		&& memcmp(clean_proof->creator_product_fingerprint,
				  pcm_resource_x_completion_product_fingerprint,
				  RESOURCE_X_COMPLETION_FINGERPRINT_BYTES) == 0
		&& memcmp(clean_proof->send_c1_manifest_fingerprint,
				  pcm_resource_x_send_c1_manifest_fingerprint,
				  RESOURCE_X_COMPLETION_FINGERPRINT_BYTES) == 0;
}

static bool
pcm_resource_x_clean_proof_live_exact(
	const ResourceXReconfigToken *token,
	const ResourceXZeroResidualProof *zero_proof,
	const ResourceXCleanCompletionProof *clean_proof)
{
	return pcm_resource_x_clean_proof_phase_exact(
		token, zero_proof, clean_proof, RESOURCE_X_GATE_FROZEN);
}

bool
cluster_pcm_lock_resource_x_clean_completion_prove_exact(
	const ResourceXReconfigToken *token,
	const ResourceXZeroResidualProof *zero_proof,
	ResourceXCleanCompletionProof *out)
{
	ClusterLmsResourceXTransportSnapshot transport_begin;
	ClusterLmsResourceXTransportSnapshot transport_end;
	ClusterLmsResourceXTransportSnapshot transport_final;
	ResourceXCleanCompletionProof proof;
	uint64 begin_sequence;
	uint64 digest = PGRAC_RESOURCE_X_PROOF_DIGEST_OFFSET;
	uint64 slot_count;
	uint64 terminal_request_count = 0;
	uint64 cursor;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (ClusterPcm == NULL || cluster_pcm_resource_x_slots == NULL
		|| token == NULL || zero_proof == NULL || out == NULL)
		return false;
	begin_sequence = pg_atomic_read_u64(
		&ClusterPcm->resource_x_semantic_mutation_sequence);
	if (begin_sequence == 0 || begin_sequence == UINT64_MAX
		|| !pcm_resource_x_zero_proof_live_exact(token, zero_proof)
		|| memcmp(zero_proof, &ClusterPcm->resource_x_zero_residual_proof,
				  sizeof(*zero_proof)) != 0)
		return false;
	if (!cluster_lms_outbound_resource_x_transport_snapshot(&transport_begin)
		|| transport_begin.mutation_sequence == 0
		|| transport_begin.mutation_sequence == UINT64_MAX
		|| transport_begin.staged_count != 0)
		return false;
	slot_count = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_slot_count);
	for (cursor = 0; cursor < zero_proof->scan_capacity; cursor++) {
		ClusterPcmResourceXSlot *registry_slot
			= &cluster_pcm_resource_x_slots[cursor];

		digest = pcm_resource_x_proof_digest_u64(digest, cursor);
		if (registry_slot->state == PCM_REGISTRY_EMPTY
			|| registry_slot->state == PCM_REGISTRY_TOMBSTONE) {
			digest = pcm_resource_x_proof_digest_u64(digest, 0);
			continue;
		}
		else if (registry_slot->state != PCM_REGISTRY_LIVE)
			return false;
		else {
			ClusterPcmResourceXMasterState *state;
			PcmEntryRef entry_ref;
			PcmEntryAcquireResult acquire_result;
			struct GrdEntry *entry;

			if (!pcm_entry_ref_acquire(&registry_slot->tag, false,
					&entry_ref, &acquire_result))
				return false;
			entry = entry_ref.entry;
			LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
			state = pcm_resource_x_master_state_for_entry(entry);
			if (state == NULL
				|| !BufferTagsEqual(&entry->tag, &registry_slot->tag)
				|| entry_ref.binding_generation
					!= registry_slot->binding_generation
				|| entry_ref.registry_slot != (uint32)cursor
				|| !pcm_resource_x_clean_state_locked(
					entry, state, &terminal_request_count, &digest)) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_entry_ref_release(&entry_ref);
				return false;
			}
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
		}
	}
	if (!cluster_lms_outbound_resource_x_transport_snapshot(&transport_end)
		|| transport_end.staged_count != 0
		|| transport_end.mutation_sequence
			   != transport_begin.mutation_sequence
		|| begin_sequence != pg_atomic_read_u64(
			&ClusterPcm->resource_x_semantic_mutation_sequence)
		|| slot_count != pg_atomic_read_u64(
			&ClusterPcm->resource_x_reconfig_slot_count))
		return false;

	memset(&proof, 0, sizeof(proof));
	proof.token = *token;
	proof.proof_generation = token->freeze_generation;
	proof.final_mutation_sequence = begin_sequence;
	proof.scan_capacity = zero_proof->scan_capacity;
	proof.logical_slot_count = slot_count;
	proof.terminal_request_count = terminal_request_count;
	proof.logical_digest = digest;
	proof.transport_mutation_sequence
		= transport_begin.mutation_sequence;
	proof.transport_staged_count = 0;
	memcpy(proof.creator_product_fingerprint,
		   pcm_resource_x_completion_product_fingerprint,
		   RESOURCE_X_COMPLETION_FINGERPRINT_BYTES);
	memcpy(proof.send_c1_manifest_fingerprint,
		   pcm_resource_x_send_c1_manifest_fingerprint,
		   RESOURCE_X_COMPLETION_FINGERPRINT_BYTES);
	proof.logical_debt_zero = 1;
	proof.transport_debt_zero = 1;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_EXCLUSIVE);
	if (begin_sequence != pg_atomic_read_u64(
			&ClusterPcm->resource_x_semantic_mutation_sequence)
		|| slot_count != pg_atomic_read_u64(
			&ClusterPcm->resource_x_reconfig_slot_count)
		|| !pcm_resource_x_zero_proof_live_exact(token, zero_proof)
		|| memcmp(zero_proof, &ClusterPcm->resource_x_zero_residual_proof,
				  sizeof(*zero_proof)) != 0) {
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return false;
	}
	ClusterPcm->resource_x_clean_completion_proof = proof;
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	if (!cluster_lms_outbound_resource_x_transport_snapshot(&transport_final)
		|| transport_final.staged_count != 0
		|| transport_final.mutation_sequence
			   != proof.transport_mutation_sequence
		|| begin_sequence != pg_atomic_read_u64(
			&ClusterPcm->resource_x_semantic_mutation_sequence))
		return false;
	*out = proof;
	return true;
}

bool
cluster_pcm_lock_resource_x_clean_completion_proof_exact(
	const ResourceXReconfigToken *token,
	const ResourceXZeroResidualProof *zero_proof,
	ResourceXCleanCompletionProof *out)
{
	ClusterLmsResourceXTransportSnapshot transport_begin;
	ClusterLmsResourceXTransportSnapshot transport_end;
	ResourceXCleanCompletionProof proof;
	bool valid;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (ClusterPcm == NULL || token == NULL || zero_proof == NULL
		|| out == NULL)
		return false;
	if (!cluster_lms_outbound_resource_x_transport_snapshot(&transport_begin)
		|| transport_begin.staged_count != 0)
		return false;
	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	proof = ClusterPcm->resource_x_clean_completion_proof;
	valid = pcm_resource_x_clean_proof_live_exact(
		token, zero_proof, &proof);
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	valid = valid
		&& cluster_lms_outbound_resource_x_transport_snapshot(&transport_end)
		&& transport_end.staged_count == 0
		&& transport_begin.mutation_sequence
			   == proof.transport_mutation_sequence
		&& transport_end.mutation_sequence
			   == proof.transport_mutation_sequence;
	if (valid)
		*out = proof;
	return valid;
}

bool
cluster_pcm_lock_resource_x_cutover_proofs_exact(
	ResourceXReconfigToken *token_out,
	ResourceXZeroResidualProof *zero_proof_out,
	ResourceXCleanCompletionProof *clean_proof_out)
{
	ResourceXReconfigToken token;
	ResourceXZeroResidualProof zero_proof;
	ResourceXCleanCompletionProof clean_proof;

	if (token_out != NULL)
		memset(token_out, 0, sizeof(*token_out));
	if (zero_proof_out != NULL)
		memset(zero_proof_out, 0, sizeof(*zero_proof_out));
	if (clean_proof_out != NULL)
		memset(clean_proof_out, 0, sizeof(*clean_proof_out));
	if (ClusterPcm == NULL || token_out == NULL || zero_proof_out == NULL
		|| clean_proof_out == NULL)
		return false;

	/* Copy only the candidate token while holding the owner lock.  Both
	 * existing exact accessors below revalidate it against the live frozen
	 * gate, semantic mutation sequence, stored proof bytes, and transport
	 * sequence before any bytes escape this function. */
	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	token = ClusterPcm->resource_x_zero_residual_proof.token;
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	if (token.old_formation == 0 || token.old_formation == UINT64_MAX
		|| token.new_formation == 0 || token.new_formation == UINT64_MAX
		|| token.old_formation == token.new_formation
		|| token.freeze_generation == 0
		|| token.freeze_generation == UINT64_MAX || token.reserved != 0
		|| !cluster_resource_x_reconfig_zero_proof_exact(
			&token, &zero_proof)
		|| !cluster_pcm_lock_resource_x_clean_completion_proof_exact(
			&token, &zero_proof, &clean_proof))
		return false;

	*token_out = token;
	*zero_proof_out = zero_proof;
	*clean_proof_out = clean_proof;
	return true;
}

bool
cluster_pcm_lock_resource_x_cutover_thawed_proofs_exact(
	ResourceXReconfigToken *token_out,
	ResourceXZeroResidualProof *zero_proof_out,
	ResourceXCleanCompletionProof *clean_proof_out)
{
	ClusterLmsResourceXTransportSnapshot transport_begin;
	ClusterLmsResourceXTransportSnapshot transport_end;
	ResourceXReconfigToken token;
	ResourceXZeroResidualProof zero_proof;
	ResourceXCleanCompletionProof clean_proof;
	bool valid;

	if (token_out != NULL)
		memset(token_out, 0, sizeof(*token_out));
	if (zero_proof_out != NULL)
		memset(zero_proof_out, 0, sizeof(*zero_proof_out));
	if (clean_proof_out != NULL)
		memset(clean_proof_out, 0, sizeof(*clean_proof_out));
	if (ClusterPcm == NULL || token_out == NULL || zero_proof_out == NULL
		|| clean_proof_out == NULL
		|| !cluster_lms_outbound_resource_x_transport_snapshot(
			&transport_begin)
		|| transport_begin.staged_count != 0)
		return false;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	token = ClusterPcm->resource_x_zero_residual_proof.token;
	zero_proof = ClusterPcm->resource_x_zero_residual_proof;
	clean_proof = ClusterPcm->resource_x_clean_completion_proof;
	valid = token.old_formation != 0
		&& token.old_formation != UINT64_MAX
		&& token.new_formation != 0
		&& token.new_formation != UINT64_MAX
		&& token.old_formation != token.new_formation
		&& token.freeze_generation != 0
		&& token.freeze_generation != UINT64_MAX
		&& token.reserved == 0
		&& pcm_resource_x_clean_proof_phase_exact(
			&token, &zero_proof, &clean_proof, RESOURCE_X_GATE_OPEN);
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	valid = valid
		&& cluster_lms_outbound_resource_x_transport_snapshot(&transport_end)
		&& transport_end.staged_count == 0
		&& transport_begin.mutation_sequence
			   == clean_proof.transport_mutation_sequence
		&& transport_end.mutation_sequence
			   == clean_proof.transport_mutation_sequence;
	if (!valid)
		return false;

	*token_out = token;
	*zero_proof_out = zero_proof;
	*clean_proof_out = clean_proof;
	return true;
}

bool
cluster_pcm_lock_resource_x_cutover_current_proof_digest_exact(
	bool thawed, ResourceXReconfigToken *token_out, uint64 *digest_out)
{
	ResourceXCleanCompletionProof clean;
	ResourceXReconfigToken token;
	ResourceXZeroResidualProof zero;
	uint64 digest = PGRAC_RESOURCE_X_PROOF_DIGEST_OFFSET;

	if (token_out == NULL || digest_out == NULL)
		return false;
	memset(token_out, 0, sizeof(*token_out));
	*digest_out = 0;
	if (thawed) {
		if (!cluster_pcm_lock_resource_x_cutover_thawed_proofs_exact(
				&token, &zero, &clean))
			return false;
	}
	else if (!cluster_pcm_lock_resource_x_cutover_proofs_exact(
				 &token, &zero, &clean))
		return false;
	if (token.reserved != 0
		|| memcmp(&zero.token, &token, sizeof(token)) != 0
		|| memcmp(&clean.token, &token, sizeof(token)) != 0
		|| zero.proof_generation != token.freeze_generation
		|| clean.proof_generation != token.freeze_generation
		|| zero.complete_wrap != 1 || zero.zero_residual != 1
		|| clean.logical_debt_zero != 1
		|| clean.transport_debt_zero != 1
		|| clean.transport_staged_count != 0
		|| zero.final_mutation_sequence
		   != clean.final_mutation_sequence)
		return false;

	/* Fold only explicit, exact fields.  The result is a local prerequisite
	 * identity, never authority and never a wire value. */
	digest = pcm_resource_x_proof_digest_u64(
		digest, token.old_formation);
	digest = pcm_resource_x_proof_digest_u64(
		digest, token.new_formation);
	digest = pcm_resource_x_proof_digest_u64(
		digest, token.freeze_generation);
	digest = pcm_resource_x_proof_digest_u64(
		digest, (uint64)token.dead_requester_bitmap);
	digest = pcm_resource_x_proof_digest_u64(
		digest, zero.full_wrap_digest);
	digest = pcm_resource_x_proof_digest_u64(
		digest, zero.final_mutation_sequence);
	digest = pcm_resource_x_proof_digest_u64(
		digest, clean.logical_digest);
	digest = pcm_resource_x_proof_digest_u64(
		digest, clean.transport_mutation_sequence);
	if (digest == 0)
		digest = PGRAC_RESOURCE_X_PROOF_DIGEST_OFFSET;
	*token_out = token;
	*digest_out = digest;
	return true;
}

bool
cluster_resource_x_reconfig_thaw_exact(const ResourceXReconfigToken *token)
{
	uint32 expected_phase;

	if (ClusterPcm == NULL
		|| pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase) != RESOURCE_X_GATE_FROZEN
		|| !pcm_resource_x_reconfig_token_exact(token)
		|| pg_atomic_read_u64(&ClusterPcm->resource_x_activation_inflight_count) != 0
		|| pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_residual_count) != 0
		|| pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_zero_proof_generation)
			   != token->freeze_generation
		|| !pcm_resource_x_zero_proof_live_exact(
			token, &ClusterPcm->resource_x_zero_residual_proof))
		return false;

	pg_atomic_write_u64(&ClusterPcm->resource_x_gate_formation, token->new_formation);
	expected_phase = RESOURCE_X_GATE_FROZEN;
	if (!pg_atomic_compare_exchange_u32(&ClusterPcm->resource_x_gate_phase, &expected_phase,
										 RESOURCE_X_GATE_OPEN))
		return false;
	pg_atomic_fetch_add_u64(&ClusterPcm->resource_x_reconfig_thaw_count, 1);
	return true;
}

void
cluster_resource_x_reconfig_stats_snapshot(ResourceXReconfigStats *out)
{
	if (out == NULL)
		return;
	memset(out, 0, sizeof(*out));
	if (ClusterPcm == NULL)
		return;
	out->freeze_count = pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_freeze_count);
	out->slot_examined_count
		= pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_slot_examined_count);
	out->old_detached_count
		= pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_old_detached_count);
	out->successor_count = pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_successor_count);
	out->orphan_count = pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_orphan_count);
	out->sidecar_neutralized_count
		= pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_sidecar_neutralized_count);
	out->sidecar_stale_count
		= pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_sidecar_stale_count);
	out->retry_count = pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_retry_count);
	out->blocked_count = pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_blocked_count);
	out->thaw_count = pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_thaw_count);
	out->reclaim_nonhead_count = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_reclaim_nonhead_count);
	out->reclaim_head_count = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_reclaim_head_count);
	out->reclaim_orphan_count = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_reclaim_orphan_count);
}

void
cluster_pcm_lock_resource_x_o1_stats_snapshot(ResourceXO1Stats *out)
{
	if (out == NULL)
		return;
	memset(out, 0, sizeof(*out));
	if (ClusterPcm == NULL)
		return;
	out->remote_install_observed_count = pg_atomic_read_u64(
		&ClusterPcm->resource_x_o1_remote_install_observed_count);
	out->remote_grant_after_image_count = pg_atomic_read_u64(
		&ClusterPcm->resource_x_o1_remote_grant_after_image_count);
	out->remote_image_at_or_after_grant_count = pg_atomic_read_u64(
		&ClusterPcm->resource_x_o1_remote_image_at_or_after_grant_count);
	out->remote_episode_excluded_no_install = pg_atomic_read_u64(
		&ClusterPcm->resource_x_o1_remote_episode_excluded_no_install);
	out->remote_episode_excluded_missing_grant = pg_atomic_read_u64(
		&ClusterPcm->resource_x_o1_remote_episode_excluded_missing_grant);
	out->remote_episode_excluded_missing_image = pg_atomic_read_u64(
		&ClusterPcm->resource_x_o1_remote_episode_excluded_missing_image);
	out->last_remote_t_image_us = pg_atomic_read_u64(
		&ClusterPcm->resource_x_o1_last_remote_t_image_us);
	out->last_remote_t_grant_us = pg_atomic_read_u64(
		&ClusterPcm->resource_x_o1_last_remote_t_grant_us);
	out->last_remote_t_install_us = pg_atomic_read_u64(
		&ClusterPcm->resource_x_o1_last_remote_t_install_us);
}

static bool
pcm_resource_x_intent_body_valid(const ResourceXIntentBodyHandle *body)
{
	if (body == NULL || !resource_x_assertion_valid(&body->assertion)
		|| body->owner_generation == 0 || body->owner_generation == UINT64_MAX
		|| body->owner_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| body->owner_kind < RESOURCE_X_INTENT_OWNER_MASTER_BLOCK
		|| body->owner_kind > RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE
		|| body->reserved != 0)
		return false;
	if (body->owner_kind == RESOURCE_X_INTENT_OWNER_MASTER_BLOCK)
		return body->owner_index < RESOURCE_X_PROTOCOL_NODE_LIMIT;
	return body->owner_index == 0;
}

static bool
pcm_resource_x_intent_mark_dirty(void)
{
	uint64 generation;

	if (ClusterPcm == NULL)
		return false;
	generation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_intent_arm_generation);
	for (;;) {
		if (generation == UINT64_MAX) {
			pg_atomic_write_u32(
				&ClusterPcm->resource_x_intent_generation_exhausted, 1);
			return false;
		}
		if (pg_atomic_compare_exchange_u64(
				&ClusterPcm->resource_x_intent_arm_generation,
				&generation, generation + 1)) {
			cluster_lms_wakeup(0);
			return true;
		}
	}
}

static bool
pcm_resource_x_intent_payload_exact(ResourceXWireKind kind,
									uint16 payload_bytes)
{
	switch (kind) {
		case RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP:
			/* Bootstrap staging is non-authority and never uses the retained
			 * authority intent family. */
			return false;
		case RESOURCE_X_WIRE_ASSERT_X:
		case RESOURCE_X_WIRE_BLOCK_TO_N:
		case RESOURCE_X_WIRE_RELEASE_X:
			return payload_bytes == RESOURCE_X_CONTROL_V1_BYTES;
		case RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION:
		case RESOURCE_X_WIRE_INSTALL_SETTLEMENT:
			return payload_bytes == RESOURCE_X_SHORT_V1_BYTES;
		case RESOURCE_X_WIRE_BLOCKED_TO_N:
			return payload_bytes == RESOURCE_X_CONTROL_V1_BYTES
				|| payload_bytes == RESOURCE_X_PROOF_V1_BYTES;
		case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2:
		case RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2:
			return payload_bytes == RESOURCE_X_PROOF_V1_BYTES;
		case RESOURCE_X_WIRE_AUTHORITY_GRANT:
			return payload_bytes == RESOURCE_X_PROOF_V1_BYTES;
		case RESOURCE_X_WIRE_IMAGE_ENVELOPE:
			return payload_bytes == RESOURCE_X_IMAGE_V1_BYTES;
	}
	return false;
}

bool
cluster_pcm_lock_resource_x_intent_arm_exact(
	ResourceXIntentSlot *slot, const ResourceXIntentBodyHandle *body,
	uint64 logical_generation, uint64 authority_generation,
	uint64 now_us, uint32 destination_node, uint16 payload_bytes,
	ResourceXWireKind kind)
{
	if (slot == NULL || !pcm_resource_x_intent_body_valid(body)
		|| logical_generation == 0 || logical_generation == UINT64_MAX
		|| authority_generation == 0 || authority_generation == UINT64_MAX
		|| now_us == 0 || now_us == UINT64_MAX
		|| destination_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| !pcm_resource_x_intent_payload_exact(kind, payload_bytes))
		return false;
	if (slot->state != RESOURCE_X_INTENT_SLOT_EMPTY)
		return slot->logical_generation == logical_generation
			&& slot->authority_generation == authority_generation
			&& slot->destination_node == destination_node
			&& slot->payload_bytes == payload_bytes
			&& slot->kind == (uint8)kind
			&& memcmp(&slot->body, body, sizeof(*body)) == 0;

	memset(slot, 0, sizeof(*slot));
	slot->logical_generation = logical_generation;
	slot->authority_generation = authority_generation;
	slot->first_armed_us = now_us;
	slot->destination_node = destination_node;
	slot->payload_bytes = payload_bytes;
	slot->kind = (uint8)kind;
	slot->state = RESOURCE_X_INTENT_SLOT_ARMED;
	slot->body = *body;
	return true;
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_intent_not_admitted_exact(
	ResourceXIntentSlot *slot, const ResourceXIntentSlot *expected,
	uint64 now_us)
{
	if (slot == NULL || expected == NULL || now_us == 0
		|| now_us == UINT64_MAX
		|| slot->logical_generation != expected->logical_generation
		|| slot->authority_generation != expected->authority_generation
		|| slot->first_armed_us != expected->first_armed_us
		|| slot->destination_node != expected->destination_node
		|| slot->payload_bytes != expected->payload_bytes
		|| slot->kind != expected->kind
		|| memcmp(&slot->body, &expected->body, sizeof(slot->body)) != 0
		|| slot->state != RESOURCE_X_INTENT_SLOT_ARMED)
		return RESOURCE_X_INTENT_STALE;
	slot->last_attempt_us = now_us;
	return RESOURCE_X_INTENT_NOT_ADMITTED;
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_intent_stage_exact(
	ResourceXIntentSlot *slot, const ResourceXIntentSlot *expected,
	uint64 now_us)
{
	if (slot == NULL || expected == NULL || now_us == 0
		|| now_us == UINT64_MAX
		|| slot->logical_generation != expected->logical_generation
		|| slot->authority_generation != expected->authority_generation
		|| slot->first_armed_us != expected->first_armed_us
		|| slot->destination_node != expected->destination_node
		|| slot->payload_bytes != expected->payload_bytes
		|| slot->kind != expected->kind
		|| memcmp(&slot->body, &expected->body, sizeof(slot->body)) != 0)
		return RESOURCE_X_INTENT_STALE;
	if (slot->state == RESOURCE_X_INTENT_SLOT_STAGED)
		return RESOURCE_X_INTENT_STAGED;
	if (slot->state != RESOURCE_X_INTENT_SLOT_ARMED)
		return RESOURCE_X_INTENT_STALE;
	slot->last_attempt_us = now_us;
	slot->state = RESOURCE_X_INTENT_SLOT_STAGED;
	return RESOURCE_X_INTENT_STAGED;
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_intent_hard_rearm_exact(
	ResourceXIntentSlot *slot, const ResourceXIntentSlot *expected,
	uint64 now_us)
{
	if (slot == NULL || expected == NULL || now_us == 0
		|| now_us == UINT64_MAX
		|| slot->logical_generation != expected->logical_generation
		|| slot->authority_generation != expected->authority_generation
		|| slot->first_armed_us != expected->first_armed_us
		|| slot->destination_node != expected->destination_node
		|| slot->payload_bytes != expected->payload_bytes
		|| slot->kind != expected->kind
		|| memcmp(&slot->body, &expected->body, sizeof(slot->body)) != 0)
		return RESOURCE_X_INTENT_STALE;
	if (slot->state == RESOURCE_X_INTENT_SLOT_ARMED)
		return RESOURCE_X_INTENT_HARD_REARMED;
	if (slot->state != RESOURCE_X_INTENT_SLOT_STAGED)
		return RESOURCE_X_INTENT_STALE;
	slot->last_attempt_us = now_us;
	slot->state = RESOURCE_X_INTENT_SLOT_ARMED;
	return RESOURCE_X_INTENT_HARD_REARMED;
}

bool
cluster_pcm_lock_resource_x_intent_complete_exact(
	ResourceXIntentSlot *slot, const ResourceXIntentSlot *expected)
{
	if (slot == NULL || expected == NULL
		|| slot->logical_generation != expected->logical_generation
		|| slot->authority_generation != expected->authority_generation
		|| slot->first_armed_us != expected->first_armed_us
		|| slot->destination_node != expected->destination_node
		|| slot->payload_bytes != expected->payload_bytes
		|| slot->kind != expected->kind
		|| memcmp(&slot->body, &expected->body, sizeof(slot->body)) != 0
		|| slot->state != RESOURCE_X_INTENT_SLOT_STAGED)
		return false;
	memset(slot, 0, sizeof(*slot));
	return true;
}

static bool
pcm_resource_x_entry_exact(const struct GrdEntry *entry, const ResourceXAcquisitionRef *ref)
{
	return BufferTagsEqual(&entry->tag, &ref->assertion.resource)
		   && entry->resource_x_requester_node == ref->assertion.requester_node
		   && entry->resource_x_formation == ref->formation
		   && entry->resource_x_acquisition_generation == ref->acquisition_generation;
}

typedef enum PcmResourceXRefClass {
	PCM_RX_REF_RETIRED_OLD = 0,
	PCM_RX_REF_RETIRED_EXACT,
	PCM_RX_REF_FUTURE_EMPTY,
	PCM_RX_REF_ACTIVE_EXACT,
	PCM_RX_REF_ACTIVE_OTHER,
	PCM_RX_REF_CORRUPT
} PcmResourceXRefClass;

static bool
pcm_resource_x_active_empty_locked(const struct GrdEntry *entry)
{
	return entry->resource_x_requester_node == -1 && entry->resource_x_progress_flags == 0
		   && entry->resource_x_formation == 0
		   && entry->resource_x_acquisition_generation == 0
		   && entry->resource_x_no_progress_generation == 0
		   && entry->resource_x_no_progress_reason == RESOURCE_X_NO_PROGRESS_NONE
		   && entry->resource_x_dispatch_phase == 0;
}

static bool
pcm_resource_x_active_valid_locked(const struct GrdEntry *entry)
{
	uint32 active_flags;

	if (entry->resource_x_requester_node < 0
		|| entry->resource_x_requester_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| entry->resource_x_formation == 0
		|| entry->resource_x_formation == UINT64_MAX
		|| entry->resource_x_acquisition_generation == 0
		|| entry->resource_x_acquisition_generation == UINT64_MAX)
		return false;
	active_flags = entry->resource_x_progress_flags & ~RESOURCE_X_PROGRESS_RECOVERY_BLOCKED;
	if (active_flags != (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1)
		&& active_flags
			!= (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1 | RESOURCE_X_PROGRESS_T2))
		return false;
	if ((entry->resource_x_progress_flags & ~RESOURCE_X_PROGRESS_KNOWN_MASK) != 0
		|| (entry->resource_x_progress_flags & RESOURCE_X_PROGRESS_T3) != 0)
		return false;
	return (entry->resource_x_no_progress_generation == 0
			&& entry->resource_x_no_progress_reason == RESOURCE_X_NO_PROGRESS_NONE)
		   || (entry->resource_x_no_progress_generation
				   == entry->resource_x_acquisition_generation
			   && entry->resource_x_no_progress_reason > RESOURCE_X_NO_PROGRESS_NONE
			   && entry->resource_x_no_progress_reason
					  <= RESOURCE_X_NO_PROGRESS_BUFFER_CORRUPT);
}

static PcmResourceXRefClass
pcm_resource_x_ref_classify_locked(const struct GrdEntry *entry,
								 const ResourceXAcquisitionRef *ref)
{
	bool active_empty;

	if (entry->resource_x_requester_base_generation < 1
		|| entry->resource_x_requester_base_generation == UINT64_MAX
		|| entry->resource_x_retired_acquisition_generation == UINT64_MAX)
		return PCM_RX_REF_CORRUPT;
	active_empty = pcm_resource_x_active_empty_locked(entry);
	if (!active_empty) {
		if (!pcm_resource_x_active_valid_locked(entry))
			return PCM_RX_REF_CORRUPT;
		return pcm_resource_x_entry_exact(entry, ref)
			? PCM_RX_REF_ACTIVE_EXACT : PCM_RX_REF_ACTIVE_OTHER;
	}
	if (ref->acquisition_generation < entry->resource_x_retired_acquisition_generation)
		return PCM_RX_REF_RETIRED_OLD;
	if (entry->resource_x_retired_acquisition_generation != 0
		&& ref->acquisition_generation == entry->resource_x_retired_acquisition_generation)
		return PCM_RX_REF_RETIRED_EXACT;
	if (ref->acquisition_generation > entry->resource_x_retired_acquisition_generation)
		return PCM_RX_REF_FUTURE_EMPTY;
	return PCM_RX_REF_CORRUPT;
}

static void
pcm_resource_x_active_clear_locked(struct GrdEntry *entry)
{
	entry->resource_x_requester_node = -1;
	entry->resource_x_progress_flags = 0;
	entry->resource_x_formation = 0;
	entry->resource_x_acquisition_generation = 0;
	entry->resource_x_no_progress_generation = 0;
	entry->resource_x_no_progress_reason = RESOURCE_X_NO_PROGRESS_NONE;
	entry->resource_x_dispatch_phase = 0;
}

static void
pcm_resource_x_snapshot_locked(const struct GrdEntry *entry, ResourceXExecutorSnapshot *out)
{
	memset(out, 0, sizeof(*out));
	out->ref.assertion.resource = entry->tag;
	out->ref.assertion.requester_node = entry->resource_x_requester_node;
	out->ref.formation = entry->resource_x_formation;
	out->ref.acquisition_generation = entry->resource_x_acquisition_generation;
	out->progress_flags = entry->resource_x_progress_flags;
	out->no_progress_reason = entry->resource_x_no_progress_reason;
	out->no_progress_generation = entry->resource_x_no_progress_generation;
	out->requester_base_generation = entry->resource_x_requester_base_generation;
	out->retired_acquisition_generation = entry->resource_x_retired_acquisition_generation;
}

typedef struct PcmEntryWaitContext {
	PcmEntryRef ref;
	bool cv_prepared;
	bool wait_registered;
} PcmEntryWaitContext;

static bool
pcm_entry_ref_identity_exact(const PcmEntryRef *ref)
{
	struct GrdEntry *entry;

	if (ref == NULL || !ref->pinned || ref->entry == NULL
		|| ref->binding_generation == 0)
		return false;
	entry = ref->entry;
	return BufferTagsEqual(&entry->tag, &ref->tag)
		&& entry->binding_generation == ref->binding_generation
		&& entry->registry_slot == ref->registry_slot;
}

static void
pcm_entry_wait_ref_begin(PcmEntryWaitContext *context)
{
	struct GrdEntry *entry;
	uint32 refs;

	Assert(context != NULL && !context->wait_registered);
	if (!pcm_entry_ref_identity_exact(&context->ref)) {
		pcm_resource_x_reconfig_block();
		ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("PCM waiter entry identity changed before registration")));
	}
	entry = context->ref.entry;
	refs = pg_atomic_read_u32(&entry->wait_refcount);
	for (;;) {
		if (refs == UINT32_MAX) {
			pcm_resource_x_reconfig_block();
			ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("PCM entry wait reference count overflow")));
		}
		if (pg_atomic_compare_exchange_u32(
				&entry->wait_refcount, &refs, refs + 1))
			break;
	}
	context->wait_registered = true;
}

static void
pcm_entry_wait_ref_end(PcmEntryWaitContext *context)
{
	struct GrdEntry *entry;
	uint32 refs;

	Assert(context != NULL && context->wait_registered);
	if (!pcm_entry_ref_identity_exact(&context->ref)) {
		pcm_resource_x_reconfig_block();
		ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("PCM waiter entry identity changed before release")));
	}
	entry = context->ref.entry;
	refs = pg_atomic_read_u32(&entry->wait_refcount);
	for (;;) {
		if (refs == 0) {
			pcm_resource_x_reconfig_block();
			ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("PCM entry wait reference count underflow")));
		}
		if (pg_atomic_compare_exchange_u32(
				&entry->wait_refcount, &refs, refs - 1))
			break;
	}
	context->wait_registered = false;
}

static void
pcm_entry_wait_context_cancel_wait(PcmEntryWaitContext *context)
{
	if (context == NULL)
		return;
	if (context->cv_prepared) {
		ConditionVariableCancelSleep();
		context->cv_prepared = false;
	}
	if (context->wait_registered)
		pcm_entry_wait_ref_end(context);
}

static void
pcm_entry_wait_context_cleanup(PcmEntryWaitContext *context)
{
	if (context == NULL)
		return;
	pcm_entry_wait_context_cancel_wait(context);
	if (context->ref.pinned)
		pcm_entry_ref_release(&context->ref);
}

/* One backend can have at most one ConditionVariable sleep registration.
 * Preserve that registration across the executor probe/wait API boundary,
 * but retain only the exact pinned handle and logical Resource-X identity. */
static PcmEntryWaitContext pcm_resource_x_executor_wait_context;
static ResourceXAcquisitionRef pcm_resource_x_executor_wait_ref;

static bool
pcm_resource_x_executor_wait_ref_exact(const ResourceXAcquisitionRef *ref)
{
	return ref != NULL
		&& resource_x_assertion_equal(
			&pcm_resource_x_executor_wait_ref.assertion, &ref->assertion)
		&& pcm_resource_x_executor_wait_ref.formation == ref->formation
		&& pcm_resource_x_executor_wait_ref.acquisition_generation
			== ref->acquisition_generation;
}

static void
pcm_resource_x_executor_wait_cleanup(void)
{
	pcm_entry_wait_context_cleanup(
		&pcm_resource_x_executor_wait_context);
	memset(&pcm_resource_x_executor_wait_ref, 0,
		   sizeof(pcm_resource_x_executor_wait_ref));
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_t1_grant_exact(const ResourceXAcquisitionRef *ref)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	PcmResourceXRefClass ref_class;
	bool broadcast = false;

	if (!pcm_resource_x_ref_valid(ref))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&ref->assertion.resource, true,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	ref_class = pcm_resource_x_ref_classify_locked(entry, ref);
	switch (ref_class) {
	case PCM_RX_REF_RETIRED_OLD:
	case PCM_RX_REF_ACTIVE_OTHER:
		result = RESOURCE_X_APPLY_STALE;
		break;
	case PCM_RX_REF_RETIRED_EXACT:
		result = RESOURCE_X_APPLY_DUPLICATE;
		break;
	case PCM_RX_REF_ACTIVE_EXACT:
		if ((entry->resource_x_progress_flags & RESOURCE_X_PROGRESS_RECOVERY_BLOCKED) != 0)
			result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		else if ((entry->resource_x_progress_flags & RESOURCE_X_PROGRESS_T1) != 0)
			result = RESOURCE_X_APPLY_DUPLICATE;
		else
			result = RESOURCE_X_APPLY_BAD_STATE;
		break;
	case PCM_RX_REF_CORRUPT:
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		break;
	case PCM_RX_REF_FUTURE_EMPTY:
		result = pcm_resource_x_requester_join_bindable_locked(entry, ref);
		if (result == RESOURCE_X_APPLY_APPLIED) {
			entry->resource_x_requester_node = ref->assertion.requester_node;
			entry->resource_x_formation = ref->formation;
			entry->resource_x_acquisition_generation = ref->acquisition_generation;
			entry->resource_x_progress_flags
				= RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1;
			entry->resource_x_no_progress_generation = 0;
			entry->resource_x_no_progress_reason = RESOURCE_X_NO_PROGRESS_NONE;
			entry->resource_x_dispatch_phase = 0;
			if (!pcm_resource_x_head_progress_ref_locked(entry, ref, pcm_resource_x_monotonic_us()))
				result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			broadcast = true;
		}
		break;
	}

	LWLockRelease(&entry->entry_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXExecutorProbeResult
cluster_pcm_lock_resource_x_executor_probe_exact(const ResourceXAcquisitionRef *ref,
											 ResourceXExecutorSnapshot *out_snapshot)
{
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXExecutorProbeResult result;
	PcmResourceXRefClass ref_class;

	if (out_snapshot == NULL)
		return RESOURCE_X_EXECUTOR_CHANGED;
	memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (!pcm_resource_x_ref_valid(ref))
		return RESOURCE_X_EXECUTOR_CHANGED;
	pcm_resource_x_executor_wait_cleanup();
	if (!pcm_entry_ref_acquire(&ref->assertion.resource, false,
			&pcm_resource_x_executor_wait_context.ref, &acquire_result))
		return RESOURCE_X_EXECUTOR_CHANGED;
	entry = pcm_resource_x_executor_wait_context.ref.entry;

	/* Register before the predicate lock so every producer broadcast between
	 * registration and a possible sleep remains visible.  D2 carries an exact
	 * pinned handle, never a bare entry pointer, across the API boundary. */
	pcm_entry_wait_ref_begin(&pcm_resource_x_executor_wait_context);
	ConditionVariablePrepareToSleep(&entry->wait_cv);
	pcm_resource_x_executor_wait_context.cv_prepared = true;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	pcm_resource_x_snapshot_locked(entry, out_snapshot);
	ref_class = pcm_resource_x_ref_classify_locked(entry, ref);
	if (ref_class == PCM_RX_REF_RETIRED_EXACT) {
		out_snapshot->ref = *ref;
		out_snapshot->progress_flags
			= RESOURCE_X_PROGRESS_T1 | RESOURCE_X_PROGRESS_T2 | RESOURCE_X_PROGRESS_T3;
		result = RESOURCE_X_EXECUTOR_COMPLETE;
	}
	else if (ref_class == PCM_RX_REF_CORRUPT)
		result = RESOURCE_X_EXECUTOR_RECOVERY_BLOCKED;
	else if (ref_class != PCM_RX_REF_ACTIVE_EXACT)
		result = RESOURCE_X_EXECUTOR_CHANGED;
	else if ((entry->resource_x_progress_flags & RESOURCE_X_PROGRESS_RECOVERY_BLOCKED) != 0)
		result = RESOURCE_X_EXECUTOR_RECOVERY_BLOCKED;
	else if (entry->resource_x_no_progress_generation == ref->acquisition_generation
				 && entry->resource_x_no_progress_reason != RESOURCE_X_NO_PROGRESS_NONE)
		result = RESOURCE_X_EXECUTOR_BLOCKED;
	else
		result = RESOURCE_X_EXECUTOR_READY;
	LWLockRelease(&entry->entry_lock.lock);
	if (result == RESOURCE_X_EXECUTOR_BLOCKED)
		pcm_resource_x_executor_wait_ref = *ref;
	else
		pcm_resource_x_executor_wait_cleanup();
	return result;
}

/* Complete the wait armed by an exact BLOCKED probe.  The pinned
 * {tag,binding_generation} handle keeps this wait_cv alive; the complete
 * logical ref check prevents a successor acquisition from borrowing it. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_executor_wait_exact(const ResourceXAcquisitionRef *ref,
											long timeout_ms)
{
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	PcmResourceXRefClass ref_class;

	if (!pcm_resource_x_ref_valid(ref) || timeout_ms <= 0) {
		pcm_resource_x_executor_wait_cleanup();
		return RESOURCE_X_APPLY_INVALID;
	}
	if (!pcm_resource_x_executor_wait_context.ref.pinned
		|| !pcm_resource_x_executor_wait_context.wait_registered
		|| !pcm_resource_x_executor_wait_context.cv_prepared
		|| !pcm_resource_x_executor_wait_ref_exact(ref)) {
		pcm_resource_x_executor_wait_cleanup();
		return RESOURCE_X_APPLY_STALE;
	}
	entry = pcm_resource_x_executor_wait_context.ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	ref_class = pcm_resource_x_ref_classify_locked(entry, ref);
	if (ref_class == PCM_RX_REF_RETIRED_OLD || ref_class == PCM_RX_REF_ACTIVE_OTHER)
		result = RESOURCE_X_APPLY_STALE;
	else if (ref_class == PCM_RX_REF_RETIRED_EXACT)
		result = RESOURCE_X_APPLY_DUPLICATE;
	else if (ref_class == PCM_RX_REF_FUTURE_EMPTY)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (ref_class == PCM_RX_REF_CORRUPT
			 || (entry->resource_x_progress_flags & RESOURCE_X_PROGRESS_RECOVERY_BLOCKED) != 0)
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else
		result = RESOURCE_X_APPLY_APPLIED;
	LWLockRelease(&entry->entry_lock.lock);
	if (result != RESOURCE_X_APPLY_APPLIED) {
		pcm_resource_x_executor_wait_cleanup();
		return result;
	}

	PG_TRY();
	{
		(void)ConditionVariableTimedSleep(&entry->wait_cv, timeout_ms,
									 WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
		if (!pcm_entry_ref_identity_exact(
				&pcm_resource_x_executor_wait_context.ref)) {
			pcm_resource_x_reconfig_block();
			ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Resource-X executor waiter entry identity changed "
						"after wakeup")));
		}
	}
	PG_FINALLY();
	{
		pcm_resource_x_executor_wait_cleanup();
	}
	PG_END_TRY();
	return RESOURCE_X_APPLY_APPLIED;
}

/* One existing requester/master driver calls this only after a real wait or
 * scheduled tick.  Clearing the exact observation here (never in probe)
 * makes that producer change visible and prevents an immediate BUSY loop. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_executor_rearm_exact(const ResourceXAcquisitionRef *ref)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	PcmResourceXRefClass ref_class;
	bool broadcast = false;

	if (!pcm_resource_x_ref_valid(ref))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&ref->assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	ref_class = pcm_resource_x_ref_classify_locked(entry, ref);
	if (ref_class == PCM_RX_REF_RETIRED_OLD || ref_class == PCM_RX_REF_ACTIVE_OTHER)
		result = RESOURCE_X_APPLY_STALE;
	else if (ref_class == PCM_RX_REF_RETIRED_EXACT)
		result = RESOURCE_X_APPLY_DUPLICATE;
	else if (ref_class == PCM_RX_REF_FUTURE_EMPTY)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (ref_class == PCM_RX_REF_CORRUPT
			 || (entry->resource_x_progress_flags & RESOURCE_X_PROGRESS_RECOVERY_BLOCKED) != 0)
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (ref_class != PCM_RX_REF_ACTIVE_EXACT)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (entry->resource_x_no_progress_generation == 0
			 && entry->resource_x_no_progress_reason == RESOURCE_X_NO_PROGRESS_NONE)
		result = RESOURCE_X_APPLY_DUPLICATE;
	else if (entry->resource_x_no_progress_generation != ref->acquisition_generation
			 || entry->resource_x_no_progress_reason <= RESOURCE_X_NO_PROGRESS_NONE
			 || entry->resource_x_no_progress_reason > RESOURCE_X_NO_PROGRESS_BUFFER_CORRUPT)
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (entry->resource_x_dispatch_phase == UINT32_MAX)
	{
		entry->resource_x_progress_flags |= RESOURCE_X_PROGRESS_RECOVERY_BLOCKED;
		broadcast = true;
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	else
	{
		entry->resource_x_dispatch_phase++;
		entry->resource_x_no_progress_generation = 0;
		entry->resource_x_no_progress_reason = RESOURCE_X_NO_PROGRESS_NONE;
		broadcast = true;
		result = RESOURCE_X_APPLY_APPLIED;
	}
	/* Clearing an executor observation changes the predicate, not progress.
	 * It must wake followers without buying another head lease. */
	if (broadcast && !pcm_resource_x_head_changed_locked(entry, false, 0))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	LWLockRelease(&entry->entry_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_requester_apply_exact(const ResourceXAcquisitionRef *ref,
										  const ResourceXBufferInstallProof *proof)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	PcmResourceXRefClass ref_class;
	bool broadcast = false;
	bool same_bootstrap_ref;

	if (!pcm_resource_x_ref_valid(ref) || proof == NULL || proof->ownership_generation == 0
		|| proof->writer_activation_token == 0)
		return RESOURCE_X_APPLY_INVALID;
	if (proof->resource_x_activation_generation != ref->acquisition_generation)
		return RESOURCE_X_APPLY_STALE;
	if (!pcm_entry_ref_acquire(&ref->assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	same_bootstrap_ref
		= round->phase == RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED
		  && resource_x_assertion_equal(
			&round->request.logical_assertion, &ref->assertion)
		  && round->resource_formation == ref->formation
		  && round->request.assertion_sequence
			 == ref->acquisition_generation;
	ref_class = pcm_resource_x_ref_classify_locked(entry, ref);
	if (ref_class == PCM_RX_REF_RETIRED_OLD || ref_class == PCM_RX_REF_ACTIVE_OTHER)
		result = RESOURCE_X_APPLY_STALE;
	else if (ref_class == PCM_RX_REF_RETIRED_EXACT)
		result = RESOURCE_X_APPLY_DUPLICATE;
	else if (ref_class == PCM_RX_REF_FUTURE_EMPTY)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (ref_class == PCM_RX_REF_CORRUPT
			 || (entry->resource_x_progress_flags & RESOURCE_X_PROGRESS_RECOVERY_BLOCKED) != 0)
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if ((entry->resource_x_progress_flags & RESOURCE_X_PROGRESS_T2) != 0) {
		if (same_bootstrap_ref
			&& round->cached_ownership_generation != 0
			&& round->cached_ownership_generation
				!= proof->ownership_generation)
			result = RESOURCE_X_APPLY_STALE;
		else
			result = RESOURCE_X_APPLY_DUPLICATE;
	}
	else if ((entry->resource_x_progress_flags
			  & (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1))
			 != (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1))
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (same_bootstrap_ref
			 && round->cached_ownership_generation != 0
			 && round->cached_ownership_generation
				!= proof->ownership_generation)
		result = RESOURCE_X_APPLY_STALE;
	else {
		/* Freeze the exact BufferDesc generation while the T2 writer fence is
		 * still present.  A same-node follower can then recognize the short
		 * post-T3/pre-cover cross-lock interval without deriving authority from
		 * an arbitrary cached X descriptor. */
		if (same_bootstrap_ref)
			round->cached_ownership_generation
				= proof->ownership_generation;
		entry->resource_x_progress_flags |= RESOURCE_X_PROGRESS_T2;
		entry->resource_x_no_progress_generation = 0;
		entry->resource_x_no_progress_reason = RESOURCE_X_NO_PROGRESS_NONE;
		broadcast = true;
		/* This publication records the exact physical-X witness and T2 in
		 * one shared mutation.  Replayed T2 never renews the head lease. */
		result = pcm_resource_x_head_progress_ref_locked(entry, ref, pcm_resource_x_monotonic_us())
					 ? RESOURCE_X_APPLY_APPLIED
					 : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	LWLockRelease(&entry->entry_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_requester_activate_exact(const ResourceXAcquisitionRef *ref,
											 const ResourceXBufferActivationProof *proof)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	PcmResourceXRefClass ref_class;
	PcmResourceXRetirementWitness witness;
	bool broadcast = false;
	bool publish_terminal_cover = false;
	bool same_bootstrap_ref = false;

	if (!pcm_resource_x_ref_valid(ref) || proof == NULL || proof->ownership_generation == 0)
		return RESOURCE_X_APPLY_INVALID;
	if (proof->writer_activation_token != 0 || proof->resource_x_activation_generation != 0)
		return RESOURCE_X_APPLY_STALE;
	if (!pcm_entry_ref_acquire(&ref->assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	ref_class = pcm_resource_x_ref_classify_locked(entry, ref);
	if (ref_class == PCM_RX_REF_RETIRED_OLD || ref_class == PCM_RX_REF_ACTIVE_OTHER)
		result = RESOURCE_X_APPLY_STALE;
	else if (ref_class == PCM_RX_REF_RETIRED_EXACT)
		result = RESOURCE_X_APPLY_DUPLICATE;
	else if (ref_class == PCM_RX_REF_FUTURE_EMPTY)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (ref_class == PCM_RX_REF_CORRUPT
			 || (entry->resource_x_progress_flags & RESOURCE_X_PROGRESS_RECOVERY_BLOCKED) != 0)
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if ((entry->resource_x_progress_flags
			  & (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1
				 | RESOURCE_X_PROGRESS_T2))
			 != (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1
				 | RESOURCE_X_PROGRESS_T2)
			 || entry->resource_x_no_progress_generation != 0
			 || entry->resource_x_no_progress_reason != RESOURCE_X_NO_PROGRESS_NONE)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if ((result = pcm_resource_x_requester_retirement_prepare_locked(
				  entry, ref, true, false, &witness)) == RESOURCE_X_APPLY_APPLIED) {
		same_bootstrap_ref
			= witness.had_join
			  && resource_x_assertion_equal(
				&round->request.logical_assertion, &ref->assertion)
			  && round->resource_formation == ref->formation
			  && round->request.assertion_sequence
				 == ref->acquisition_generation;
		if (same_bootstrap_ref) {
			/* The requester round is the sole retained full-ref cover after
			 * T3.  Validate every fallible input before retirement, then publish
			 * the cover in the same entry-lock commit.  This removes the
			 * post-T3/pre-cover observation window without deriving authority
			 * from the retired floor or the local neutral GRD projection. */
			if (round->phase
					!= RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED
				|| round->highest_attempt_floor
					!= ref->acquisition_generation
				|| round->master_session_incarnation == 0
				|| round->master_session_incarnation == UINT64_MAX
				|| round->r4_record_generation == 0
				|| round->r4_record_generation == UINT64_MAX
				|| round->accepted_base == 0
				|| round->accepted_base >= UINT64_MAX - 1
				|| round->cached_ownership_generation
					!= proof->ownership_generation
				|| witness.already_settled
				|| witness.final_authority_generation
					<= round->accepted_base
				|| witness.final_authority_generation == UINT64_MAX
				|| !cluster_pcm_lock_resource_x_gate_open_exact(
					ref->formation))
				result = RESOURCE_X_APPLY_STALE;
			else
				publish_terminal_cover = true;
		}
		if (result == RESOURCE_X_APPLY_APPLIED
			&& !pcm_resource_x_requester_settlement_arm_locked(
				entry, ref, &witness))
			result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		else if (result == RESOURCE_X_APPLY_APPLIED) {
			pcm_resource_x_requester_retirement_commit_locked(
				entry, ref, &witness);
			if (publish_terminal_cover) {
				round->terminal_ref = *ref;
				round->terminal_authority_generation
					= witness.final_authority_generation;
				round->cached_ownership_generation
					= proof->ownership_generation;
				round->phase
					= RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED;
			}
			if (!pcm_resource_x_head_progress_ref_locked(entry, ref, pcm_resource_x_monotonic_us()))
				result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			broadcast = true;
		}
	}
	LWLockRelease(&entry->entry_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

void
cluster_pcm_lock_resource_x_publish_no_progress_exact(const ResourceXAcquisitionRef *ref,
												   ResourceXNoProgressReason reason)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	PcmResourceXRefClass ref_class;
	bool broadcast = false;

	if (!pcm_resource_x_ref_valid(ref) || reason <= RESOURCE_X_NO_PROGRESS_NONE
		|| reason > RESOURCE_X_NO_PROGRESS_BUFFER_CORRUPT)
		return;
	if (!pcm_entry_ref_acquire(&ref->assertion.resource, false,
			&entry_ref, &acquire_result))
		return;
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	ref_class = pcm_resource_x_ref_classify_locked(entry, ref);
	if (ref_class == PCM_RX_REF_ACTIVE_EXACT
		&& (entry->resource_x_progress_flags & RESOURCE_X_PROGRESS_T1) != 0
		&& (entry->resource_x_progress_flags
			& RESOURCE_X_PROGRESS_RECOVERY_BLOCKED)
			== 0
		&& (entry->resource_x_no_progress_generation != ref->acquisition_generation
			|| entry->resource_x_no_progress_reason != (uint32)reason)) {
		entry->resource_x_no_progress_generation = ref->acquisition_generation;
		entry->resource_x_no_progress_reason = (uint32)reason;
		(void)pcm_resource_x_head_changed_locked(entry, false, 0);
		broadcast = true;
	}
	LWLockRelease(&entry->entry_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
}

/* ============================================================
 * PGRAC: spec-2.36 D5 HC117 / HC124 — S barrier helpers.
 *
 *	Implementation contract:
 *	- set/clear mutate the field under entry_lock EXCLUSIVE because
 *	  the field is read+modified by N→S decision path and N→X grant
 *	  install ack path concurrently;  spec-2.30 entry_lock is also
 *	  taken on every state transition so the cost is bounded.
 *	- query takes htab_lock SHARED + reads pending_x_requester_node
 *	  with one atomic load equivalent (int32, naturally aligned);
 *	  callers are advisory readers that backoff retry on mismatch,
 *	  so a torn read would only delay the next attempt one round
 *	  trip (acceptable per HC117 backoff semantics).
 *	- clear_pending_x_for_node (HC124) scans all entries under
 *	  htab_lock SHARED;  per-entry mutation under entry_lock
 *	  EXCLUSIVE;  idempotent recheck inside the lock.
 * ============================================================ */
static PcmPendingXReserveResult
pcm_lock_try_reserve_pending_x_value(BufferTag tag, int32 requester_node, uint64 value)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	PcmPendingXReserveResult result = PCM_PENDING_X_RESERVE_OCCUPIED;

	if (ClusterPcm == NULL || cluster_pcm_htab == NULL || requester_node < 0
		|| requester_node >= 32)
		return PCM_PENDING_X_RESERVE_INVALID;
	/* Admission itself is the first authoritative touch for a cold tag.  Use
	 * the canonical lazy allocator so both legacy and queue writers can arm
	 * the barrier before any transition exists. */
	if (!pcm_entry_ref_acquire(
			&tag, true, &entry_ref, &acquire_result))
		return PCM_PENDING_X_RESERVE_NO_CAPACITY;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
	if (entry->pending_x_requester_node == -1) {
		entry->pending_x_requester_node = requester_node;
		entry->pending_x_since_lsn = value;
		result = PCM_PENDING_X_RESERVE_OK;
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

PcmPendingXReserveResult
cluster_pcm_lock_set_pending_x(BufferTag tag, int32 requester_node, uint64 current_lsn)
{
	if ((current_lsn & PCM_PENDING_X_QUEUE_KIND) != 0)
		return PCM_PENDING_X_RESERVE_INVALID;
	return pcm_lock_try_reserve_pending_x_value(tag, requester_node, current_lsn);
}

/*
 * Reserve the one GrdEntry pending-X barrier for an active PCM-X queue head.
 *
 * Legacy request paths still use cluster_pcm_lock_set_pending_x() for their
 * request-scoped round.  Queue arbitration cannot: a delayed drive of another
 * tag-head must never overwrite the requester that owns the live transfer.
 * A node-only match is not replay proof: a legacy request from the same node
 * may own that mark.  This primitive therefore succeeds only on the idle
 * transition; the queue engine separately persists the exact ticket claim.
 */
PcmPendingXReserveResult
cluster_pcm_lock_try_reserve_pending_x(BufferTag tag, int32 requester_node, uint64 ticket_id)
{
	uint64 value;

	if (!PcmPendingXQueueValue(ticket_id, &value))
		return PCM_PENDING_X_RESERVE_INVALID;
	return pcm_lock_try_reserve_pending_x_value(tag, requester_node, value);
}

bool
cluster_pcm_lock_queue_pending_x_exact(BufferTag tag, int32 requester_node, uint64 ticket_id)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	uint64 expected;
	bool exact = false;

	if (ClusterPcm == NULL || cluster_pcm_htab == NULL || requester_node < 0 || requester_node >= 32
		|| !PcmPendingXQueueValue(ticket_id, &expected))
		return false;
	if (!pcm_entry_ref_acquire(
			&tag, false, &entry_ref, &acquire_result))
		return false;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	exact = entry->pending_x_requester_node == requester_node
			&& entry->pending_x_since_lsn == expected;
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return exact;
}

bool
cluster_pcm_lock_clear_queue_pending_x_exact(BufferTag tag, int32 requester_node, uint64 ticket_id)
{
	struct GrdEntry *entry;
	uint64 expected;
	bool found;
	bool cleared = false;

	if (ClusterPcm == NULL || cluster_pcm_htab == NULL || requester_node < 0 || requester_node >= 32
		|| !PcmPendingXQueueValue(ticket_id, &expected))
		return false;
	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		if (entry->pending_x_requester_node == requester_node
			&& entry->pending_x_since_lsn == expected) {
			entry->pending_x_requester_node = -1;
			entry->pending_x_since_lsn = 0;
			cleared = true;
		}
		LWLockRelease(&entry->entry_lock.lock);
		if (cleared)
			ConditionVariableBroadcast(&entry->wait_cv);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	return cleared;
}

void
cluster_pcm_lock_clear_pending_x(BufferTag tag)
{
	struct GrdEntry *entry;
	bool found;
	bool cleared = false;

	if (cluster_pcm_htab == NULL)
		return;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		if (entry->pending_x_requester_node != -1
			&& (entry->pending_x_since_lsn & PCM_PENDING_X_QUEUE_KIND) == 0) {
			entry->pending_x_requester_node = -1;
			entry->pending_x_since_lsn = 0;
			cleared = true;
		}
		LWLockRelease(&entry->entry_lock.lock);
		if (cleared)
			ConditionVariableBroadcast(&entry->wait_cv);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
}

/*
 * cluster_pcm_lock_clear_pending_x_if -- identity-safe compare-and-clear
 * (GCS-race round-2 additional hardening).  Clears the pending-X mark ONLY
 * while it still names expected_requester.  Every request-scoped clear
 * must use this form: between a request's set and its clear the mark can
 * be legitimately cleared and RE-SET by a different requester (dead-node
 * sweep + a new upgrader, or a converged retry), and an unconditional
 * clear would wipe the newer writer's starvation guard -- readers then
 * slip N->S under a live X transfer.  Returns true when this call cleared
 * the mark.
 */
bool
cluster_pcm_lock_clear_pending_x_if(BufferTag tag, int32 expected_requester)
{
	struct GrdEntry *entry;
	bool found;
	bool cleared = false;

	if (cluster_pcm_htab == NULL)
		return false;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		if (entry->pending_x_requester_node == expected_requester
			&& (entry->pending_x_since_lsn & PCM_PENDING_X_QUEUE_KIND) == 0) {
			entry->pending_x_requester_node = -1;
			entry->pending_x_since_lsn = 0;
			cleared = true;
		}
		LWLockRelease(&entry->entry_lock.lock);
		if (cleared)
			ConditionVariableBroadcast(&entry->wait_cv);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	return cleared;
}

int32
cluster_pcm_lock_query_pending_x_requester(BufferTag tag)
{
	struct GrdEntry *entry;
	bool found;
	int32 requester = -1;

	if (cluster_pcm_htab == NULL)
		return -1;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL)
		requester = entry->pending_x_requester_node;
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return requester;
}

uint32
cluster_pcm_lock_query_s_holders_bitmap(BufferTag tag)
{
	struct GrdEntry *entry;
	bool found;
	uint32 bitmap = 0;

	if (cluster_pcm_htab == NULL)
		return 0;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL)
		bitmap = pg_atomic_read_u32(&entry->s_holders_bitmap);
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return bitmap;
}

/*
 * cluster_pcm_master_requester_is_holder — spec-4.7a D3.
 *
 *	Strict master-side coherence check:  does the GrdEntry for `tag`
 *	authoritatively record `node` as a holder whose existing grant already
 *	covers `trans`?  The GCS block master uses this to idempotently re-
 *	acknowledge a holder's re-request (GRANTED_STORAGE_FALLBACK, master state
 *	UNCHANGED) instead of replying DENIED_MASTER_NOT_HOLDER — which the sender
 *	would retransmit-loop into 53R90.  That loop is the spec-4.7a D0 bug:  a
 *	node releases its content_lock (buf->pcm_state → N) while the master still
 *	records it as the x_holder, so its next LockBuffer re-request diverges.
 *
 *	Returns true ONLY when the GrdEntry records `node`:
 *	  - x_holder_node == node                 → covers N→S and N→X (X ⊇ {S,X})
 *	  - (trans == N→S) && node ∈ s_holders_bitmap → covers N→S
 *	S→X_UPGRADE returns false: it is a real writer transition that MUST run
 *	the spec-2.36 invalidate-then-grant path (no self-regrant short-circuit,
 *	no double X — spec-4.7a v0.2 amend 2 / HG5).
 *	Missing entry / out-of-range node / any uncertainty → false → caller
 *	fails closed (Rule 8.A).
 */
bool
cluster_pcm_master_requester_is_holder(BufferTag tag, int32 node, PcmLockTransition trans)
{
	struct GrdEntry *entry;
	bool found;
	bool is_holder = false;

	if (node < 0 || node >= 32)
		return false;
	if (cluster_pcm_htab == NULL)
		return false;
	/* Only fresh-acquire transitions can be idempotent re-grants; upgrades
	 * (S→X) and releases must take their real paths. */
	if (trans != PCM_TRANS_N_TO_S && trans != PCM_TRANS_N_TO_X)
		return false;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		if (entry->x_holder_node == node)
			is_holder = true; /* X holder covers N→S and N→X */
		else if (trans == PCM_TRANS_N_TO_S
				 && (pg_atomic_read_u32(&entry->s_holders_bitmap) & (1u << (uint32)node)) != 0)
			is_holder = true; /* S holder covers N→S */
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return is_holder;
}

/*
 * cluster_pcm_master_other_live_holder_exists — spec-4.7a D4.
 *
 *	Does a node OTHER than `sender` currently hold `tag` in X or S, and is
 *	that node still LIVE (CSSD peer state != DEAD)?  The GCS block master
 *	calls this on an X request (N→X / S→X) to bounded-fail-closed BEFORE any
 *	state mutation when granting X would require invalidating / transferring
 *	the block from a live peer — the deferred writer-transfer path (spec-2.36
 *	completion / 4.7 / Stage 6, NOT implemented in 4.7a).  A DEAD holder is
 *	deliberately NOT counted: that is the dead-master / warm-recovery path
 *	(53R9K / spec-4.7).  Strict GrdEntry read; missing entry or no other live
 *	holder → false (Rule 8.A — the caller then proceeds to the normal grant
 *	path; it must never grant when this returns true).
 */
bool
cluster_pcm_master_other_live_holder_exists(BufferTag tag, int32 sender)
{
	struct GrdEntry *entry;
	bool found;
	int32 x_holder = -1;
	uint32 s_bitmap = 0;
	int n;

	if (cluster_pcm_htab == NULL)
		return false;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		x_holder = entry->x_holder_node;
		s_bitmap = pg_atomic_read_u32(&entry->s_holders_bitmap);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	if (!found)
		return false;

	/* Another node holds X. */
	if (x_holder >= 0 && x_holder != sender
		&& cluster_cssd_get_peer_state(x_holder) != CLUSTER_CSSD_PEER_DEAD)
		return true;

	/* Another node holds S (exclude the requester's own S bit). */
	if (sender >= 0 && sender < 32)
		s_bitmap &= ~((uint32)1u << sender);
	for (n = 0; n < 32; n++) {
		if ((s_bitmap & ((uint32)1u << n)) != 0
			&& cluster_cssd_get_peer_state(n) != CLUSTER_CSSD_PEER_DEAD)
			return true;
	}

	return false;
}

uint64
cluster_pcm_lock_clear_pending_x_for_node(int32 dead_node)
{
	HASH_SEQ_STATUS scan;
	struct GrdEntry *entry;
	uint64 cleared = 0;

	if (cluster_pcm_htab == NULL || dead_node < 0)
		return 0;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	hash_seq_init(&scan, cluster_pcm_htab);
	while ((entry = (struct GrdEntry *)hash_seq_search(&scan)) != NULL) {
		bool cleared_entry = false;

		/* Fast SHARED read first — most entries will not match. */
		if (entry->pending_x_requester_node != dead_node)
			continue;
		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		/* HC124 idempotent recheck under entry_lock: another path
		 * (X grant install ack / reconfig epoch advance) may have
		 * cleared the field between our SHARED read and the
		 * EXCLUSIVE acquire. */
		if (entry->pending_x_requester_node == dead_node) {
			entry->pending_x_requester_node = -1;
			entry->pending_x_since_lsn = 0;
			cleared++;
			cleared_entry = true;
		}
		LWLockRelease(&entry->entry_lock.lock);
		if (cleared_entry)
			ConditionVariableBroadcast(&entry->wait_cv);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return cleared;
}

uint64
cluster_pcm_lock_cleanup_on_node_dead(int32 dead_node)
{
	HASH_SEQ_STATUS scan;
	struct GrdEntry *entry;
	uint64 cleaned = 0;
	uint32 dead_bit;

	if (cluster_pcm_htab == NULL || dead_node < 0 || dead_node >= 32)
		return 0;

	dead_bit = (uint32)1u << (uint32)dead_node;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	hash_seq_init(&scan, cluster_pcm_htab);
	while ((entry = (struct GrdEntry *)hash_seq_search(&scan)) != NULL) {
		bool changed = false;
		bool broadcast_needed = false;
		bool master_holder_was_dead;
		PcmState before_state;
		PcmState after_state;
		uint32 s_bitmap;
		uint32 pi_bitmap;

		/* Cheap unlocked filter; rechecked under entry_lock below. */
		if (entry->x_holder_node != dead_node
			&& (pg_atomic_read_u32(&entry->s_holders_bitmap) & dead_bit) == 0
			&& (pg_atomic_read_u32(&entry->pi_holders_bitmap) & dead_bit) == 0
			&& (!pcm_master_holder_is_valid(entry)
				|| (int32)entry->master_holder.node_id != dead_node))
			continue;

		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		before_state = (PcmState)pg_atomic_read_u32(&entry->master_state);
		master_holder_was_dead
			= pcm_master_holder_is_valid(entry) && (int32)entry->master_holder.node_id == dead_node;

		if (entry->x_holder_node == dead_node) {
			entry->x_holder_node = -1;
			changed = true;
		}
		s_bitmap = pg_atomic_read_u32(&entry->s_holders_bitmap);
		if ((s_bitmap & dead_bit) != 0) {
			s_bitmap &= ~dead_bit;
			pg_atomic_write_u32(&entry->s_holders_bitmap, s_bitmap);
			changed = true;
		}
		pi_bitmap = pg_atomic_read_u32(&entry->pi_holders_bitmap);
		if ((pi_bitmap & dead_bit) != 0) {
			pg_atomic_write_u32(&entry->pi_holders_bitmap, pi_bitmap & ~dead_bit);
			changed = true;
		}

		if (entry->x_holder_node >= 0) {
			pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_X);
			if (master_holder_was_dead)
				pcm_master_holder_set_node(entry, entry->x_holder_node);
		} else if (s_bitmap != 0) {
			int32 next_holder = pcm_lowest_set_bit_node(s_bitmap);

			pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_S);
			if (master_holder_was_dead) {
				if (next_holder >= 0)
					pcm_master_holder_set_node(entry, next_holder);
				else
					pcm_master_holder_clear(entry);
			}
		} else {
			pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_N);
			if (pcm_master_holder_is_valid(entry))
				pcm_master_holder_clear(entry);
		}
		if (master_holder_was_dead)
			changed = true;
		after_state = (PcmState)pg_atomic_read_u32(&entry->master_state);
		if (changed && after_state != before_state)
			broadcast_needed = true;

		LWLockRelease(&entry->entry_lock.lock);

		if (broadcast_needed)
			ConditionVariableBroadcast(&entry->wait_cv);
		if (changed)
			cleaned++;
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return cleaned;
}


/* ========================================================================
 * PGRAC MODIFICATIONS by SqlRush — spec-5.13 D5 (clean-leave PCM release).
 *
 *   cluster_pcm_lock_clean_leave_release_all_self(leave_epoch) -> uint64
 *   — a leaving node clears its OWN holder records from the local PCM directory
 *   (entries this node masters): drop its X holdership, its S residency bit,
 *   and its PI residency bit on every entry.  Called AFTER the GCS flush +
 *   release-X seam has persisted every dirty X block to shared storage (CL-I5),
 *   so dropping the directory X record can never strand an unflushed current
 *   image.  The survivor side drops node_id == leaving from entries it masters
 *   as part of §3.2 step 2 (spec-5.13 S6).
 *
 *   Locking mirrors cluster_pcm_lock_clear_pending_x_for_node: htab_lock SHARED
 *   over the scan + per-entry entry_lock EXCLUSIVE for the mutation (HC57).  An
 *   entry left with no X holder and no S holders is demoted to master_state N.
 *   leave_epoch is logged only by the S5 driver — the release is unconditional
 *   once drain reaches this phase.  Returns the count of entries mutated.
 * ======================================================================== */
uint64
cluster_pcm_lock_clean_leave_release_all_self(uint64 leave_epoch)
{
	HASH_SEQ_STATUS scan;
	struct GrdEntry *entry;
	uint64 released = 0;
	uint32 self_bit;

	(void)leave_epoch; /* logged by the S5 driver; release is unconditional here */

	/* PCM holder bitmaps are 32-bit (one bit per node id 0..31). */
	if (cluster_pcm_htab == NULL || cluster_node_id < 0 || cluster_node_id >= 32)
		return 0;

	self_bit = (uint32)1u << (uint32)cluster_node_id;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	hash_seq_init(&scan, cluster_pcm_htab);
	while ((entry = (struct GrdEntry *)hash_seq_search(&scan)) != NULL) {
		bool changed = false;

		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);

		if (entry->x_holder_node == cluster_node_id) {
			entry->x_holder_node = -1;
			changed = true;
		}
		if ((pg_atomic_read_u32(&entry->s_holders_bitmap) & self_bit) != 0) {
			pg_atomic_fetch_and_u32(&entry->s_holders_bitmap, ~self_bit);
			changed = true;
		}
		if ((pg_atomic_read_u32(&entry->pi_holders_bitmap) & self_bit) != 0) {
			pg_atomic_fetch_and_u32(&entry->pi_holders_bitmap, ~self_bit);
			changed = true;
		}

		/* No X holder and no S holders left -> the block is unheld; demote the
		 * mastered state to N (shared storage becomes the sole authority). */
		if (entry->x_holder_node < 0 && pg_atomic_read_u32(&entry->s_holders_bitmap) == 0)
			pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_N);

		LWLockRelease(&entry->entry_lock.lock);

		if (changed)
			released++;
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return released;
}


/* ========================================================================
 * cluster_pcm_lock_clean_leave_verify_no_leftover(leaving_node) -> bool —
 * spec-5.13 D5 (CL-I2 proof).  Read-only scan of the local PCM directory:
 * returns true iff no entry still records `leaving_node` as the X holder or in
 * the S / PI holder bitmaps.  A leftover PCM record for a departed node is a
 * cross-node double-grant hazard (rule 8.A), so the clean-leave acceptance gate
 * asserts this is empty after the leaving node's release + the survivor's drop.
 * ======================================================================== */
bool
cluster_pcm_lock_clean_leave_verify_no_leftover(int32 leaving_node)
{
	HASH_SEQ_STATUS scan;
	struct GrdEntry *entry;
	uint32 leaving_bit;
	bool clean = true;

	/* PCM holder bitmaps are 32-bit (one bit per node id 0..31). */
	if (cluster_pcm_htab == NULL || leaving_node < 0 || leaving_node >= 32)
		return true;

	leaving_bit = (uint32)1u << (uint32)leaving_node;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	hash_seq_init(&scan, cluster_pcm_htab);
	while ((entry = (struct GrdEntry *)hash_seq_search(&scan)) != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
		if (entry->x_holder_node == leaving_node
			|| (pg_atomic_read_u32(&entry->s_holders_bitmap) & leaving_bit) != 0
			|| (pg_atomic_read_u32(&entry->pi_holders_bitmap) & leaving_bit) != 0)
			clean = false;
		LWLockRelease(&entry->entry_lock.lock);

		if (!clean) {
			hash_seq_term(&scan);
			break;
		}
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return clean;
}


/*
 * cluster_gcs_block_master_rebuild_from_redeclare -- spec-4.7 D2/D3.
 *
 *	Rebuild the minimal master block-resource view (holder / mode / PI
 *	watermark) from ONE survivor re-declare.  The GCS_BLOCK_REDECLARE handler
 *	calls this after validating checksum + episode epoch + sender identity.
 *	The block resource is RECOVERING while this runs (the spec-4.7 D1 acquire
 *	gate fail-closes requests 53R9L), so a transiently-partial view is never
 *	served;  unfreeze to NORMAL only happens after the rebuild AND the D5 redo
 *	boundary (P7).
 *
 *	D2: record the declared holder + monotone max PI watermark.  Multiple
 *	survivors re-declaring S merge their residency bits;  an X declare is
 *	authoritative for the block.  D3 adds the not-double-X invariant (two
 *	distinct nodes declaring X on the same block = protocol anomaly →
 *	fail-closed) and full reconciliation of a reused entry.
 */
bool
cluster_gcs_block_master_rebuild_from_redeclare(BufferTag tag, uint8 held_mode, XLogRecPtr page_lsn,
												SCN page_scn, int32 source_node,
												uint64 cluster_epoch)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	uint32 holder_bit;

	/* epoch already gated by the handler (L235/L236); recorded below as the
	 * re-declare feed's provenance epoch (S3 forensics step 1a). */

	if (cluster_pcm_htab == NULL)
		return false;
	if (source_node < 0 || source_node >= 32)
		return false;
	if (held_mode != (uint8)PCM_STATE_S && held_mode != (uint8)PCM_STATE_X)
		return false;

	if (!pcm_entry_ref_acquire(
			&tag, true, &entry_ref, &acquire_result))
		return false; /* HC59 cap fail-closed — leave unrebuilt;  survivor re-sends */
	entry = entry_ref.entry;

	holder_bit = pcm_holder_bit(source_node);

	pcm_entry_lock_exclusive(entry);

	if (held_mode == (uint8)PCM_STATE_X) {
		int32 cur_x = entry->x_holder_node;
		uint32 other_s = pg_atomic_read_u32(&entry->s_holders_bitmap) & ~holder_bit;

		/*
		 * spec-4.7 D3 not-double-X + X-vs-S contradiction (规则 8.A): another
		 * node already declared X (cur_x), OR another node already declared S
		 * (other_s), on this block this episode.  Pre-crash the PCM protocol
		 * guarantees a single X holder with NO concurrent S holders, so either
		 * is a protocol anomaly.  Fail-closed: do NOT apply (NEVER record two X
		 * holders nor X-over-a-live-S = never reconstruct a double grant);  the
		 * caller counts ambiguous_owner_failclosed and the block stays
		 * RECOVERING.  Same node re-declaring X is idempotent (cur_x ==
		 * source_node and only its own S bit → falls through and re-applies).
		 */
		if ((cur_x >= 0 && cur_x != source_node) || other_s != 0) {
			cluster_grd_inc_block_path_failclosed();
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return false;
		}
		/* X holder is authoritative for the block (first declarer, or the same
		 * node re-declaring). */
		pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_X);
		entry->x_holder_node = source_node;
		pg_atomic_write_u32(&entry->s_holders_bitmap, 0);
	} else if ((PcmState)pg_atomic_read_u32(&entry->master_state) == PCM_STATE_X) {
		/*
		 * spec-4.7 D3 S-vs-X contradiction (规则 8.A, code-review P1 fix): the
		 * block is already X-held by another view, and now a node declares S on
		 * it — pre-crash an X holder excludes all S holders, so this is a
		 * protocol anomaly.  Fail-closed: do NOT silently drop-and-succeed (the
		 * pre-fix bug returned true);  reject so the caller counts ambiguous and
		 * the block stays RECOVERING rather than serving an ambiguous owner.
		 */
		cluster_grd_inc_block_path_failclosed();
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return false;
	} else {
		/* S residency: merge the bit;  raise N→S. */
		pg_atomic_fetch_or_u32(&entry->s_holders_bitmap, holder_bit);
		if ((PcmState)pg_atomic_read_u32(&entry->master_state) == PCM_STATE_N)
			pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_S);
	}

	/* spec-2.41 D3 — advance BOTH watermarks from the survivor re-declare,
	 * monotone max.  page_lsn feeds the spec-4.7 D5 redo-coverage serve-gate's
	 * required_lsn (per-stream replay position);  page_scn feeds the lost-write
	 * detector's cross-node SCN watermark.  The REDECLARE wire now carries both
	 * (page_lsn@28 + page_scn@52, checksum-covered). */
	if ((uint64)page_lsn > entry->pi_watermark_lsn)
		entry->pi_watermark_lsn = (uint64)page_lsn;
	/* monotone-max by local_scn (scn_time_cmp order); a raw SCN compare would be
	 * node_id-dominated — see cluster_scn.h + gcs_block_lost_write_verdict. */
	if (SCN_VALID(page_scn)) {
		SCN wm_old = entry->pi_watermark_scn;

		if (scn_local(page_scn)
			> scn_local(entry->pi_watermark_scn)) { /* SCN_CMP_OK: scn_time_cmp via scn_local */
			entry->pi_watermark_scn = page_scn;
			/* step 1b: only an ACTUAL advance is recorded (per-tag table). */
			pcm_wm_prov_record(tag, entry->binding_generation, wm_old, page_scn,
							   CLUSTER_PCM_WM_SRC_REDECLARE, source_node, 0,
							   cluster_epoch);
		}
	}

	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return true; /* holder recorded */
}


/*
 * P0-26: exact local-master X-transfer commit.
 *
 * The remote holder drops its X before replying, but the local master keeps
 * the sampled remote-X authority until this barrier.  Requiring the complete
 * entry-lock snapshot prevents a late reply from overwriting a newer queue
 * winner, holder session, or generation.  STALE performs no mutation.
 */
PcmXTransferCommitResult
cluster_pcm_lock_master_take_x_after_transfer(BufferTag tag, const PcmAuthoritySnapshot *expected,
											  XLogRecPtr page_lsn, SCN page_scn, int32 holder_node,
											  uint32 requester_procno, uint64 request_id,
											  uint64 epoch)
{
	struct GrdEntry *entry;
	PcmAuthoritySnapshot current;
	ClusterGrdHolderId requester;
	bool found;

	if (expected == NULL || holder_node < 0 || holder_node >= 32 || request_id == 0)
		return PCM_X_TRANSFER_COMMIT_BAD_STATE;
	if (ClusterPcm == NULL || cluster_pcm_htab == NULL)
		return PCM_X_TRANSFER_COMMIT_NOT_FOUND;

	requester.node_id = (uint32)cluster_node_id;
	requester.procno = requester_procno;
	requester.cluster_epoch = epoch;
	requester.request_id = request_id;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return PCM_X_TRANSFER_COMMIT_NOT_FOUND;
	}

	LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
	pcm_authority_snapshot_locked(entry, &current);
	if (!pcm_authority_snapshot_equal(&current, expected)) {
		LWLockRelease(&entry->entry_lock.lock);
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return PCM_X_TRANSFER_COMMIT_STALE;
	}
	if (current.state != PCM_STATE_X || current.x_holder_node != holder_node
		|| current.s_holders_bitmap != 0 || current.pending_x_requester_node != -1
		|| current.master_holder.node_id != (uint32)holder_node) {
		LWLockRelease(&entry->entry_lock.lock);
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return PCM_X_TRANSFER_COMMIT_BAD_STATE;
	}

	pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_X);
	entry->x_holder_node = cluster_node_id;
	pg_atomic_write_u32(&entry->s_holders_bitmap, 0);
	entry->s_holder_refcount_local = 0;
	pcm_master_holder_set_exact(entry, &requester);
	if ((uint64)page_lsn > entry->pi_watermark_lsn)
		entry->pi_watermark_lsn = (uint64)page_lsn;
	if (SCN_VALID(page_scn)) {
		SCN wm_old = entry->pi_watermark_scn;

		if (scn_local(page_scn) > scn_local(entry->pi_watermark_scn)) {
			entry->pi_watermark_scn = page_scn;
			pcm_wm_prov_record(tag, entry->binding_generation, wm_old, page_scn,
							   CLUSTER_PCM_WM_SRC_TAKE_X, holder_node,
							   request_id, epoch);
		}
	}
	entry->last_transition_at = GetCurrentTimestamp();
	pg_atomic_fetch_add_u64(&entry->transition_count_local, 1);
	LWLockRelease(&entry->entry_lock.lock);
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	return PCM_X_TRANSFER_COMMIT_OK;
}

/*
 * PGRAC: spec-5.2 D11 path B — remote-master self-ship writer-transfer-revoke.
 *
 *	THIS node is BOTH the GCS master AND the X holder for the block, and a
 *	REMOTE requester wants X.  We ship our current image to the requester and
 *	revoke our own X (the caller dropped our local copy no-wire before calling
 *	this — Rule 8.A no-stale-flush), then record the REQUESTER as the new X
 *	holder on the authoritative master GRD entry: X held by `requester_node`,
 *	no S holders, master_holder follows, PI watermark advanced to the shipped
 *	page_lsn.  Single-phase (the requester installs the shipped image and takes
 *	X off the GRANTED reply with no post-install ACK), so we switch ownership
 *	here.  The previous holder (self) dropped its copy BEFORE this point, so
 *	there is never a two-X window — at most a brief no-holder window, which is
 *	safe.  Mirrors cluster_pcm_lock_master_take_x_after_transfer but the new X
 *	holder is the remote requester rather than self.
 */
void
cluster_pcm_lock_master_grant_x_to(BufferTag tag, int32 requester_node, XLogRecPtr page_lsn,
								   SCN page_scn, uint64 request_id, uint64 epoch)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;

	if (cluster_pcm_htab == NULL)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("PCM lock manager disabled (cluster.pcm_grd_max_entries=0)")));
	if (requester_node < 0 || requester_node >= 32)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_pcm_lock_master_grant_x_to: requester_node %d out of range",
							   requester_node)));

	if (!pcm_entry_ref_acquire(
			&tag, true, &entry_ref, &acquire_result))
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("cluster_pcm_lock_master_grant_x_to: PCM GRD HTAB FULL (cap=%d)",
							   pcm_grd_effective)));
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_X);
	entry->x_holder_node = requester_node;
	pg_atomic_write_u32(&entry->s_holders_bitmap, 0);
	pcm_master_holder_set_node(entry, requester_node);
	/* spec-2.41 D2 (§2.8.1) — local-page source advances BOTH watermarks (lsn
	 * for redo-coverage, scn for the detector) from the shipped page. */
	if ((uint64)page_lsn > entry->pi_watermark_lsn)
		entry->pi_watermark_lsn = (uint64)page_lsn;
	/* monotone-max by local_scn (scn_time_cmp order); a raw SCN compare would be
	 * node_id-dominated — see cluster_scn.h + gcs_block_lost_write_verdict. */
	if (SCN_VALID(page_scn)) {
		SCN wm_old = entry->pi_watermark_scn;

		if (scn_local(page_scn)
			> scn_local(entry->pi_watermark_scn)) { /* SCN_CMP_OK: scn_time_cmp via scn_local */
			entry->pi_watermark_scn = page_scn;
			/* step 1b: record the granted requester's wire identity on advance. */
			pcm_wm_prov_record(tag, entry->binding_generation, wm_old, page_scn,
							   CLUSTER_PCM_WM_SRC_GRANT_X, requester_node,
							   request_id, epoch);
		}
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
}


/* ============================================================
 * PGRAC: spec-2.37 D2/D7/D8/D9 HC125-HC130 — PI watermark helpers.
 *
 *	advance:	caller (GCS/invalidate handler) already obtained the
 *				downgrading holder's page_lsn via cluster_bufmgr_
 *				invalidate_block_for_gcs(..., &page_lsn) or equivalent
 *				and now records max-historical watermark on the master.
 *				Single field max — monotone advance, never regress.
 *				D7 caller-side advance keeps cluster_pcm_transition_
 *				apply IO-free (layering).
 *	query:		master direct ship + master-side forward path use this
 *				to populate GcsBlockForwardPayload.expected_pi_watermark_
 *				lsn_bytes[8] (D3) and master-direct DENIED_LOST_WRITE
 *				check (D4).
 *	retire_for_tag:		single-tag retire (test fixture / unit test).
 *	retire_for_relation_fork:	relation drop / relfilenode change —
 *				sweep all entries whose tag matches (db, relNumber,
 *				fork) range.
 *	retire_for_truncate_range:	relation truncate — sweep all entries
 *				whose tag.blockNum >= new_nblocks within (db, relNumber,
 *				fork).
 *	retire_if_durable:		HC130 part 2 — checkpointer/smgr sync-
 *				complete path only.  D9 helper立 but callsite defer:
 *				PG infrastructure does not currently expose a per-block
 *				durable-complete hook; spec-2.38/Stage3 may add it.
 *				For now this helper exists for unit test + future wire-
 *				up; production retire path is exclusively D8 lifecycle.
 * ============================================================ */

/*
 * PGRAC: spec-2.41 D2 §2.8.1 — LSN watermark (redo-coverage serve-gate ONLY).
 *	Renamed from the old unitless cluster_pcm_lock_pi_watermark_advance so no
 *	caller can advance "the watermark" without choosing the LSN unit.  The
 *	per-stream page_lsn fed here is consumed solely by the spec-4.7 D5
 *	serve-gate (cluster_gcs_block_redo_lsn_covered); the lost-write detector
 *	uses the SCN variant below.
 */
void
cluster_pcm_lock_pi_watermark_lsn_advance(BufferTag tag, XLogRecPtr page_lsn)
{
	struct GrdEntry *entry;
	bool found;

	if (cluster_pcm_htab == NULL || XLogRecPtrIsInvalid(page_lsn))
		return;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		/* Monotone advance — never regress.  HC126 single field max.
		 * D12 counter pi_watermark_advance_count is bumped by GCS-side
		 * caller (cluster_gcs_block.c) so the counter lives next to the
		 * GCS shared state, not PCM module state. */
		if ((uint64)page_lsn > entry->pi_watermark_lsn) {
			entry->pi_watermark_lsn = (uint64)page_lsn;
		}
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
}

/*
 * S3 forensics step 1b — per-tag SCN-watermark advance provenance table.
 *
 *	pcm_wm_prov_record stores the advancing feed into the exact binding's
 *	single LIVE slot (open addressing, linear probe; retire publishes a
 *	TOMBSTONE rather than EMPTY so probe chains stay valid).  The caller holds the tag's GRD
 *	entry_lock EXCLUSIVE — the advance and its record form one critical
 *	section, so the slot's new_scn always equals the watermark the feed
 *	produced (two racing advances cannot store out of order).  wm_prov_lock
 *	is a strict leaf below entry_lock (nothing is acquired under it).
 *	Table full + unknown tag: drop the record and count — the query then
 *	reports absence as inconclusive rather than a definite NONE.
 */
static uint32
pcm_wm_prov_hash(const BufferTag *tag)
{
	/* FNV-1a over the tag fields — file-private so the standalone unit
	 * binaries need no extra hash-library stub. */
	uint32 h = 0x811c9dc5;

#define PCM_WM_PROV_HASH_MIX(v)                                                                    \
	do {                                                                                           \
		h ^= (uint32)(v);                                                                          \
		h *= 0x01000193;                                                                           \
	} while (0)
	PCM_WM_PROV_HASH_MIX(tag->spcOid);
	PCM_WM_PROV_HASH_MIX(tag->dbOid);
	PCM_WM_PROV_HASH_MIX(tag->relNumber);
	PCM_WM_PROV_HASH_MIX(tag->forkNum);
	PCM_WM_PROV_HASH_MIX(tag->blockNum);
#undef PCM_WM_PROV_HASH_MIX
	return h;
}

static void
pcm_wm_prov_record(BufferTag tag, uint64 binding_generation,
	SCN old_scn, SCN new_scn, ClusterPcmWmSrc source,
	int32 sender_node, uint64 request_id, uint64 epoch)
{
	ClusterPcmWmProvSlot *first_tombstone = NULL;
	ClusterPcmWmProvSlot *target = NULL;
	uint32 start;
	uint32 probe;

	if (ClusterPcm == NULL || binding_generation == 0
		|| binding_generation == UINT64_MAX)
		return;
	start = pcm_wm_prov_hash(&tag) % (uint32)CLUSTER_PCM_WM_PROV_SLOTS;
	LWLockAcquire(&ClusterPcm->wm_prov_lock.lock, LW_EXCLUSIVE);
	for (probe = 0; probe < (uint32)CLUSTER_PCM_WM_PROV_SLOTS; probe++) {
		ClusterPcmWmProvSlot *slot
			= &ClusterPcm->wm_prov[(start + probe) % (uint32)CLUSTER_PCM_WM_PROV_SLOTS];

		if (slot->state == PCM_SIDECAR_TOMBSTONE) {
			if (first_tombstone == NULL)
				first_tombstone = slot;
			continue;
		}
		if (slot->state == PCM_SIDECAR_EMPTY) {
			target = first_tombstone != NULL ? first_tombstone : slot;
			break;
		}
		if (slot->state != PCM_SIDECAR_LIVE) {
			LWLockRelease(&ClusterPcm->wm_prov_lock.lock);
			pcm_resource_x_reconfig_block();
			return;
		}
		if (BufferTagsEqual(&slot->tag, &tag)
			&& slot->binding_generation == binding_generation) {
			target = slot;
			break;
		}
	}
	if (target == NULL)
		target = first_tombstone;
	if (target != NULL) {
		memset(target, 0, sizeof(*target));
		target->tag = tag;
		target->binding_generation = binding_generation;
		target->old_scn = old_scn;
		target->new_scn = new_scn;
		target->request_id = request_id;
		target->epoch = epoch;
		target->sender_node = sender_node;
		target->source = (uint8)source;
		target->state = PCM_SIDECAR_LIVE;
		LWLockRelease(&ClusterPcm->wm_prov_lock.lock);
		return;
	}
	/* table full and the binding has no reusable slot */
	pg_atomic_fetch_add_u64(&ClusterPcm->wm_prov_insert_fail_count, 1);
	LWLockRelease(&ClusterPcm->wm_prov_lock.lock);
}

/* Caller holds wm_prov_lock EXCLUSIVE.  This is deliberately read-only:
 * retire publishes RETIRING before clearing the returned exact slot. */
static bool
pcm_wm_prov_validate_retire_exact_locked(const BufferTag *tag,
	uint64 binding_generation, ClusterPcmWmProvSlot **slot_out)
{
	uint32 start;
	uint32 probe;

	Assert(tag != NULL);
	Assert(slot_out != NULL);
	Assert(binding_generation != 0 && binding_generation != UINT64_MAX);
	Assert(LWLockHeldByMeInMode(
		&ClusterPcm->wm_prov_lock.lock, LW_EXCLUSIVE));
	*slot_out = NULL;
	start = pcm_wm_prov_hash(tag) % (uint32)CLUSTER_PCM_WM_PROV_SLOTS;
	for (probe = 0; probe < (uint32)CLUSTER_PCM_WM_PROV_SLOTS; probe++) {
		ClusterPcmWmProvSlot *slot
			= &ClusterPcm->wm_prov[
				(start + probe) % (uint32)CLUSTER_PCM_WM_PROV_SLOTS];

		if (slot->state == PCM_SIDECAR_EMPTY)
			return true;
		if (slot->state == PCM_SIDECAR_TOMBSTONE)
			continue;
		if (slot->state != PCM_SIDECAR_LIVE)
			return false;
		if (!BufferTagsEqual(&slot->tag, tag))
			continue;
		if (slot->binding_generation != binding_generation)
			return false;
		*slot_out = slot;
		return true;
	}
	return true;
}

const char *
cluster_pcm_wm_src_text(ClusterPcmWmSrc src)
{
	switch (src) {
	case CLUSTER_PCM_WM_SRC_NONE:
		return "none";
	case CLUSTER_PCM_WM_SRC_REDECLARE:
		return "redeclare";
	case CLUSTER_PCM_WM_SRC_TAKE_X:
		return "take-x-after-transfer";
	case CLUSTER_PCM_WM_SRC_GRANT_X:
		return "grant-x-ship";
	case CLUSTER_PCM_WM_SRC_ACK_SLOTLESS:
		return "invalidate-ack-slotless";
	case CLUSTER_PCM_WM_SRC_ACK_SLOT:
		return "invalidate-ack-slot";
	}
	return "unknown";
}

/*
 * S3 forensics step 1b — query the tag's advance-provenance slot.
 *
 *	Returns true + the record of the LAST advancing feed for the tag.
 *	Returns false with out->table_full=false for a definite "no advance
 *	ever recorded", or out->table_full=true when the table has dropped
 *	inserts (the tag's advance may simply have gone unrecorded).
 */
bool
cluster_pcm_lock_pi_watermark_prov_query(BufferTag tag, ClusterPcmWmProv *out)
{
	PcmEntryAcquireResult acquire_result;
	PcmEntryRef entry_ref;
	uint64 binding_generation;
	uint32 start;
	uint32 probe;
	bool full_seen;

	if (ClusterPcm == NULL || out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	out->source = CLUSTER_PCM_WM_SRC_NONE;
	out->sender_node = -1;
	if (!pcm_entry_ref_acquire(&tag, false, &entry_ref, &acquire_result))
		return false;
	binding_generation = entry_ref.binding_generation;
	start = pcm_wm_prov_hash(&tag) % (uint32)CLUSTER_PCM_WM_PROV_SLOTS;
	LWLockAcquire(&ClusterPcm->wm_prov_lock.lock, LW_SHARED);
	for (probe = 0; probe < (uint32)CLUSTER_PCM_WM_PROV_SLOTS; probe++) {
		ClusterPcmWmProvSlot *slot
			= &ClusterPcm->wm_prov[(start + probe) % (uint32)CLUSTER_PCM_WM_PROV_SLOTS];

		if (slot->state == PCM_SIDECAR_EMPTY)
			break;
		if (slot->state == PCM_SIDECAR_TOMBSTONE)
			continue;
		if (slot->state != PCM_SIDECAR_LIVE
			|| !BufferTagsEqual(&slot->tag, &tag)
			|| slot->binding_generation != binding_generation)
			continue;
		out->source = (ClusterPcmWmSrc)slot->source;
		out->sender_node = slot->sender_node;
		out->request_id = slot->request_id;
		out->epoch = slot->epoch;
		out->old_scn = slot->old_scn;
		out->new_scn = slot->new_scn;
		LWLockRelease(&ClusterPcm->wm_prov_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return true;
	}
	full_seen = pg_atomic_read_u64(&ClusterPcm->wm_prov_insert_fail_count) > 0;
	LWLockRelease(&ClusterPcm->wm_prov_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	out->table_full = full_seen;
	return false;
}

/*
 * PGRAC: spec-2.41 D2 §2.8.1 — SCN watermark (lost-write detector ONLY).
 *	Monotone-max of the cross-node pd_block_scn observed for this tag; the
 *	detector compares a shipped page's pd_block_scn against this (§2.6).  Fed
 *	by the local-page sources today; the ack/redeclare wire sources feed it
 *	once D3 carries pd_block_scn on the wire.
 *	S3 forensics step 1b: every feed that ACTUALLY advances the max records
 *	its provenance (source enum + wire identity + old->new) into the per-tag
 *	table so a 53R93's expected_scn traces to the advance that produced it.
 */
void
cluster_pcm_lock_pi_watermark_scn_advance(BufferTag tag, SCN page_scn, ClusterPcmWmSrc source,
										  int32 sender_node, uint64 request_id, uint64 epoch)
{
	struct GrdEntry *entry;
	bool found;

	if (cluster_pcm_htab == NULL || !SCN_VALID(page_scn))
		return;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		SCN wm_old;

		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		wm_old = entry->pi_watermark_scn;
		/* monotone-max by local_scn (scn_time_cmp order); page_scn already
		 * SCN_VALID-checked above. */
		if (scn_local(page_scn)
			> scn_local(entry->pi_watermark_scn)) { /* SCN_CMP_OK: scn_time_cmp via scn_local */
			entry->pi_watermark_scn = page_scn;
			/* step 1b: record INSIDE the entry_lock critical section so the
			 * per-tag slot's new_scn always equals the watermark this feed
			 * produced;  non-advancing (late) feeds are never recorded. */
			pcm_wm_prov_record(tag, entry->binding_generation, wm_old, page_scn,
							   source, sender_node, request_id, epoch);
		}
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
}

XLogRecPtr
cluster_pcm_lock_pi_watermark_lsn_query(BufferTag tag)
{
	struct GrdEntry *entry;
	bool found;
	XLogRecPtr lsn = InvalidXLogRecPtr;

	if (cluster_pcm_htab == NULL)
		return InvalidXLogRecPtr;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL)
		lsn = (XLogRecPtr)entry->pi_watermark_lsn;
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return lsn;
}

SCN
cluster_pcm_lock_pi_watermark_scn_query(BufferTag tag)
{
	struct GrdEntry *entry;
	bool found;
	SCN scn = InvalidScn;

	if (cluster_pcm_htab == NULL)
		return InvalidScn;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL)
		scn = entry->pi_watermark_scn;
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return scn;
}

void
cluster_pcm_lock_pi_watermark_retire_for_tag(BufferTag tag)
{
	struct GrdEntry *entry;
	bool found;

	if (cluster_pcm_htab == NULL)
		return;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		entry->pi_watermark_lsn = InvalidXLogRecPtr;
		entry->pi_watermark_scn = InvalidScn; /* spec-2.41 D2/D6 — clear BOTH watermarks */
		pg_atomic_write_u32(&entry->pi_holders_bitmap, 0);
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
}

uint64
cluster_pcm_lock_pi_watermark_retire_for_relation_fork(Oid db_oid, RelFileNumber rel_number,
													   ForkNumber fork_num)
{
	HASH_SEQ_STATUS scan;
	struct GrdEntry *entry;
	uint64 cleared = 0;

	if (cluster_pcm_htab == NULL)
		return 0;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	hash_seq_init(&scan, cluster_pcm_htab);
	while ((entry = (struct GrdEntry *)hash_seq_search(&scan)) != NULL) {
		if (entry->tag.dbOid != db_oid || entry->tag.relNumber != rel_number
			|| entry->tag.forkNum != fork_num)
			continue;
		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		if (entry->pi_watermark_lsn != InvalidXLogRecPtr
			|| entry->pi_watermark_scn
				   != InvalidScn /* spec-2.41 D3 — SCN-only source must also clear */
			|| pg_atomic_read_u32(&entry->pi_holders_bitmap) != 0) {
			entry->pi_watermark_lsn = InvalidXLogRecPtr;
			entry->pi_watermark_scn = InvalidScn; /* spec-2.41 D2/D6 — clear BOTH watermarks */
			pg_atomic_write_u32(&entry->pi_holders_bitmap, 0);
			cleared++;
		}
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return cleared;
}

uint64
cluster_pcm_lock_pi_watermark_retire_for_truncate_range(Oid db_oid, RelFileNumber rel_number,
														ForkNumber fork_num,
														BlockNumber new_nblocks)
{
	HASH_SEQ_STATUS scan;
	struct GrdEntry *entry;
	uint64 cleared = 0;

	if (cluster_pcm_htab == NULL)
		return 0;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	hash_seq_init(&scan, cluster_pcm_htab);
	while ((entry = (struct GrdEntry *)hash_seq_search(&scan)) != NULL) {
		if (entry->tag.dbOid != db_oid || entry->tag.relNumber != rel_number
			|| entry->tag.forkNum != fork_num)
			continue;
		if (entry->tag.blockNum < new_nblocks)
			continue;
		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		if (entry->pi_watermark_lsn != InvalidXLogRecPtr
			|| entry->pi_watermark_scn
				   != InvalidScn /* spec-2.41 D3 — SCN-only source must also clear */
			|| pg_atomic_read_u32(&entry->pi_holders_bitmap) != 0) {
			entry->pi_watermark_lsn = InvalidXLogRecPtr;
			entry->pi_watermark_scn = InvalidScn; /* spec-2.41 D2/D6 — clear BOTH watermarks */
			pg_atomic_write_u32(&entry->pi_holders_bitmap, 0);
			cleared++;
		}
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return cleared;
}

bool
cluster_pcm_lock_pi_watermark_retire_if_durable(BufferTag tag, XLogRecPtr written_page_lsn)
{
	struct GrdEntry *entry;
	bool found;
	bool retired = false;

	if (cluster_pcm_htab == NULL || XLogRecPtrIsInvalid(written_page_lsn))
		return false;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		/* HC130 part 2: only retire if the durable copy at storage layer
		 * actually equals-or-exceeds the watermark.  Caller must guarantee
		 * fsync/sync_complete has happened — there is no per-block durable-
		 * complete hook in PG today, so production callsite is deferred to
		 * spec-2.38/Stage3.  This helper exists for unit test + future use. */
		if ((uint64)written_page_lsn >= entry->pi_watermark_lsn
			&& entry->pi_watermark_lsn != InvalidXLogRecPtr) {
			entry->pi_watermark_lsn = InvalidXLogRecPtr;
			entry->pi_watermark_scn = InvalidScn; /* spec-2.41 D2/D6 — clear BOTH watermarks */
			pg_atomic_write_u32(&entry->pi_holders_bitmap, 0);
			retired = true;
		}
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return retired;
}

/*
 * PGRAC: spec-6.12h D-h2 — record that `holder_node` kept a real Past Image
 * buffer (BUF_TYPE_PI, D-h1) for this block.  Called by the conversion sites
 * (locally on the master, or via the ACK-ride PI_KEPT/kept_pi wire reports)
 * so the discard protocol can later target the actual PI holders.  Advisory:
 * a missing entry is a no-op — an untracked PI only misses the discard
 * notify and lingers until buffer pressure / implicit-discard reread
 * (fail-safe by the §3.4b PI contract).
 */
void
cluster_pcm_lock_pi_holder_note(BufferTag tag, int32 holder_node)
{
	struct GrdEntry *entry;
	bool found;

	if (cluster_pcm_htab == NULL || holder_node < 0 || holder_node >= 32)
		return;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		pg_atomic_fetch_or_u32(&entry->pi_holders_bitmap, (uint32)1u << (uint32)holder_node);
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
}

/*
 * PGRAC: spec-6.12h D-h2 — the production durable-confirm retire (the
 * callsite HC130 deferred; see header block).  A node reported that it wrote
 * the block's CURRENT copy to shared storage and the write is durable
 * (checkpoint sync completed, Q25-A dual trigger).  If the written page's
 * pd_block_scn covers the SCN watermark (cluster_pcm_pi_discard_covered —
 * the only cross-node comparable unit under per-thread WAL), retire BOTH
 * watermarks, hand the pre-clear PI holder bitmap to the caller, and clear
 * it.  The caller owns notifying each holder (PI_DISCARD on the INVALIDATE
 * wire); a lost notify is fail-safe (the PI merely lingers).  Returns true
 * iff the watermarks were retired here.
 */
bool
cluster_pcm_lock_pi_discard_collect(BufferTag tag, SCN written_scn, uint32 *holders_out)
{
	struct GrdEntry *entry;
	uint64 retire_generation = 0;
	bool found;
	bool fast_retire = false;
	bool retired = false;

	if (holders_out != NULL)
		*holders_out = 0;
	if (cluster_pcm_htab == NULL)
		return false;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		if (cluster_pcm_pi_discard_covered(entry->pi_watermark_scn, written_scn)) {
			if (holders_out != NULL)
				*holders_out = pg_atomic_read_u32(&entry->pi_holders_bitmap);
			entry->pi_watermark_lsn = InvalidXLogRecPtr;
			entry->pi_watermark_scn = InvalidScn; /* spec-2.41 D2/D6 — clear BOTH watermarks */
			pg_atomic_write_u32(&entry->pi_holders_bitmap, 0);
			retired = true;
			if (pcm_entry_mark_quiescing_locked(entry)) {
				retire_generation = entry->binding_generation;
				fast_retire = true;
			}
		}
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	if (fast_retire)
		(void)pcm_entry_try_retire_exact(
			&tag, retire_generation, PCM_RETIRE_REASON_PI_DISCARDED);

	return retired;
}


static void
pcm_transition_apply_internal(struct GrdEntry *entry,
	PcmLockTransition trans, int holder_node_id,
	bool preserve_resource_x_terminal_cover)
{
	uint32 holder_bit;

	Assert(entry != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	Assert(holder_node_id >= 0 && holder_node_id < 32);

	holder_bit = (uint32)1u << (uint32)holder_node_id;
	switch (trans) {
	case PCM_TRANS_N_TO_S:
		pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_S);
		pg_atomic_fetch_or_u32(&entry->s_holders_bitmap, holder_bit);
		/* HC110: first S holder becomes master_holder (forward target). */
		pcm_master_holder_set_node(entry, holder_node_id);
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_n_to_s_count, 1);
		break;
	case PCM_TRANS_N_TO_X:
		pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_X);
		entry->x_holder_node = holder_node_id;
		/* HC110: X holder becomes master_holder. */
		pcm_master_holder_set_node(entry, holder_node_id);
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_n_to_x_count, 1);
		break;
	case PCM_TRANS_S_TO_X_UPGRADE:
		pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_X);
		pg_atomic_fetch_and_u32(&entry->s_holders_bitmap, ~holder_bit);
		entry->x_holder_node = holder_node_id;
		/* HC110: upgrading node becomes sole holder; spec-2.36 invalidates
		 * other S holders.  master_holder follows upgraded node. */
		pcm_master_holder_set_node(entry, holder_node_id);
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_s_to_x_upgrade_count, 1);
		break;
	case PCM_TRANS_X_TO_S_DOWNGRADE:
		pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_S);
		pg_atomic_fetch_or_u32(&entry->s_holders_bitmap, holder_bit);
		pg_atomic_fetch_or_u32(&entry->pi_holders_bitmap, holder_bit); /* HC58 PI set */
		entry->x_holder_node = -1;
		/* HC110: downgraded X→S node still holds the buffer cached. */
		pcm_master_holder_set_node(entry, holder_node_id);
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_x_to_s_downgrade_count, 1);
		break;
	case PCM_TRANS_X_TO_N_DOWNGRADE:
		pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_N);
		pg_atomic_fetch_or_u32(&entry->pi_holders_bitmap, holder_bit); /* HC58 PI set */
		entry->x_holder_node = -1;
		/* HC110: X holder fully released; clear master_holder. */
		pcm_master_holder_clear(entry);
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_x_to_n_downgrade_count, 1);
		break;
	case PCM_TRANS_X_TO_N_RELEASE:
		pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_N);
		entry->x_holder_node = -1;
		/* HC110: X holder released, no cache claim remains. */
		pcm_master_holder_clear(entry);
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_x_to_n_release_count, 1);
		break;
	case PCM_TRANS_S_TO_N_INVALIDATE:
		pg_atomic_fetch_and_u32(&entry->s_holders_bitmap, ~holder_bit);
		/* HC110: master_holder lifecycle on S release.
		 *   bitmap == 0:     no remaining holder, clear
		 *   master == holder being released: pick lowest remaining bit
		 *   else:            keep existing master_holder
		 */
		{
			uint32 bm_after = pg_atomic_read_u32(&entry->s_holders_bitmap);

			if (bm_after == 0) {
				pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_N);
				pcm_master_holder_clear(entry);
			} else if (pcm_master_holder_is_valid(entry)
					   && (int32)entry->master_holder.node_id == holder_node_id) {
				int32 next_holder = pcm_lowest_set_bit_node(bm_after);
				if (next_holder >= 0)
					pcm_master_holder_set_node(entry, next_holder);
				else
					pcm_master_holder_clear(entry);
			}
		}
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_s_to_n_invalidate_count, 1);
		break;
	case PCM_TRANS_S_TO_N_RELEASE:
		pg_atomic_fetch_and_u32(&entry->s_holders_bitmap, ~holder_bit);
		{
			uint32 bm_after = pg_atomic_read_u32(&entry->s_holders_bitmap);

			if (bm_after == 0) {
				pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_N);
				pcm_master_holder_clear(entry);
			} else if (pcm_master_holder_is_valid(entry)
					   && (int32)entry->master_holder.node_id == holder_node_id) {
				int32 next_holder = pcm_lowest_set_bit_node(bm_after);
				if (next_holder >= 0)
					pcm_master_holder_set_node(entry, next_holder);
				else
					pcm_master_holder_clear(entry);
			}
		}
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_s_to_n_release_count, 1);
		break;
	case PCM_TRANS_S_TO_X_CLEANOUT:
		/*
			 * HC60 apply-fail-closed:  Trans-9 ITL cleanout body wired in
			 * Stage 3 AD-006 第五轮 (~27000 LOC).  Counter intentionally
			 * NOT bumped (cluster_pcm_get_trans_s_to_x_cleanout_count() 永 0
			 * until Stage 3).
			 */
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("PCM transition S→X cleanout is not implemented in spec-2.30"),
						errhint("ITL cleanout (Trans-9) wires in Stage 3 AD-006 第五轮 "
								"(spec-2.36+);  do not invoke this transition.")));
		break;
	}
	if (holder_node_id == cluster_node_id
		&& (trans == PCM_TRANS_X_TO_S_DOWNGRADE
			|| trans == PCM_TRANS_X_TO_N_DOWNGRADE
			|| trans == PCM_TRANS_X_TO_N_RELEASE)
		&& entry->resource_x_bootstrap_round.phase
			!= RESOURCE_X_BOOTSTRAP_ROUND_EMPTY
		&& !(preserve_resource_x_terminal_cover
			&& trans == PCM_TRANS_X_TO_N_RELEASE
			&& entry->resource_x_bootstrap_round.phase
				== RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED)) {
		pcm_resource_x_bootstrap_round_clear_binding_locked(
			&entry->resource_x_bootstrap_round);
		ConditionVariableBroadcast(&entry->wait_cv);
	}

	entry->last_transition_at = GetCurrentTimestamp();
	pg_atomic_fetch_add_u64(&entry->transition_count_local, 1);
}

void
cluster_pcm_transition_apply(struct GrdEntry *entry,
	PcmLockTransition trans, int holder_node_id)
{
	pcm_transition_apply_internal(entry, trans, holder_node_id, false);
}

/* The legacy pending-X cookie, native Resource-X head, and S-holder bitmap are
 * all protected by entry_lock.
 * This is the final admission point for every local/remote S publication:
 * existing S residency may re-enter without changing authority, but a new bit
 * cannot cross any live writer barrier (including the same-node case). */
static bool
pcm_s_admission_allowed_locked(struct GrdEntry *entry, uint32 holder_bit)
{
	Assert(entry != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	return (pg_atomic_read_u32(&entry->s_holders_bitmap) & holder_bit) != 0
		   || entry->pending_x_requester_node == -1;
}

/*
 * Apply a GCS-requested PCM transition on the master side.
 *
 * Unlike the public local APIs, the exact helper distinguishes a raced
 * pending-X barrier from structural incompatibility so the block handler can
 * return the retryable denial without raising ERROR or leaking the caller's
 * reply wait.  The bool wrapper preserves existing callers' applied/not-
 * applied contract.
 */
PcmGcsTransitionApplyResult
cluster_pcm_lock_apply_gcs_transition_result(BufferTag tag, PcmLockTransition trans,
											 int holder_node_id)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	PcmState cur;
	PcmState target;
	PcmGcsTransitionApplyResult result = PCM_GCS_TRANSITION_INCOMPATIBLE;
	uint32 holder_bit;
	bool broadcast_needed = false;
	bool create;

	if (cluster_pcm_htab == NULL)
		return PCM_GCS_TRANSITION_INCOMPATIBLE;
	if (holder_node_id < 0 || holder_node_id >= 32)
		return PCM_GCS_TRANSITION_INCOMPATIBLE;
	if (trans < PCM_TRANS_N_TO_S || trans > PCM_TRANS_S_TO_X_CLEANOUT)
		return PCM_GCS_TRANSITION_INCOMPATIBLE;
	if (trans == PCM_TRANS_S_TO_X_CLEANOUT)
		return PCM_GCS_TRANSITION_INCOMPATIBLE;

	create = trans == PCM_TRANS_N_TO_S || trans == PCM_TRANS_N_TO_X;
	if (!pcm_entry_ref_acquire(&tag, create, &entry_ref, &acquire_result))
		return PCM_GCS_TRANSITION_INCOMPATIBLE;
	entry = entry_ref.entry;

	holder_bit = pcm_holder_bit(holder_node_id);
	target = pcm_transition_target(trans);

	pcm_entry_lock_exclusive(entry);
	if (trans == PCM_TRANS_N_TO_S
		&& (!pcm_s_admission_allowed_locked(entry, holder_bit)
			|| pcm_resource_x_s_barrier_active_locked(entry))) {
		result = PCM_GCS_TRANSITION_PENDING_X;
		goto unlock;
	}
	cur = (PcmState)pg_atomic_read_u32(&entry->master_state);

	/*
	 * GCS shared-read grant: a remote N->S acquire is compatible with an
	 * existing S state.  AD-002 names the transition from the requester's
	 * perspective (the requester has no copy yet); the master entry remains
	 * S and only gains another S holder bit.
	 */
	if (trans == PCM_TRANS_N_TO_S && cur == PCM_STATE_S) {
		pg_atomic_fetch_or_u32(&entry->s_holders_bitmap, holder_bit);
		result = PCM_GCS_TRANSITION_APPLIED;
		goto unlock;
	}

	/*
	 * The current PCM entry records S ownership as a per-node bitmap, not a
	 * per-node refcount.  Multiple shared acquires by the same node collapse
	 * into one bit, so remote S releases must be idempotent after that bit has
	 * already been cleared by an earlier release from the same node.
	 */
	if ((trans == PCM_TRANS_S_TO_N_RELEASE || trans == PCM_TRANS_S_TO_N_INVALIDATE)
		&& (cur == PCM_STATE_N
			|| (cur == PCM_STATE_S
				&& (pg_atomic_read_u32(&entry->s_holders_bitmap) & holder_bit) == 0))) {
		result = PCM_GCS_TRANSITION_APPLIED;
		goto unlock;
	}

	if (!cluster_pcm_transition_legal(cur, target, trans))
		goto unlock;

	switch (trans) {
	case PCM_TRANS_N_TO_S:
	case PCM_TRANS_N_TO_X:
		break;
	case PCM_TRANS_S_TO_X_UPGRADE:
		if ((pg_atomic_read_u32(&entry->s_holders_bitmap) & holder_bit) == 0
			|| (pg_atomic_read_u32(&entry->s_holders_bitmap) & ~holder_bit) != 0)
			goto unlock;
		break;
	case PCM_TRANS_X_TO_S_DOWNGRADE:
	case PCM_TRANS_X_TO_N_DOWNGRADE:
	case PCM_TRANS_X_TO_N_RELEASE:
		if (entry->x_holder_node != holder_node_id)
			goto unlock;
		break;
	case PCM_TRANS_S_TO_N_INVALIDATE:
	case PCM_TRANS_S_TO_N_RELEASE:
		if ((pg_atomic_read_u32(&entry->s_holders_bitmap) & holder_bit) == 0)
			goto unlock;
		break;
	case PCM_TRANS_S_TO_X_CLEANOUT:
		goto unlock;
	}

	cluster_pcm_transition_apply(entry, trans, holder_node_id);
	if ((PcmState)pg_atomic_read_u32(&entry->master_state) == PCM_STATE_N
		|| trans == PCM_TRANS_X_TO_S_DOWNGRADE)
		broadcast_needed = true;
	result = PCM_GCS_TRANSITION_APPLIED;

unlock:
	LWLockRelease(&entry->entry_lock.lock);

	if (broadcast_needed)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

bool
cluster_pcm_lock_apply_gcs_transition(BufferTag tag, PcmLockTransition trans, int holder_node_id)
{
	return cluster_pcm_lock_apply_gcs_transition_result(tag, trans, holder_node_id)
		   == PCM_GCS_TRANSITION_APPLIED;
}


static uint32
pcm_holder_bit(int holder_node_id)
{
	Assert(holder_node_id >= 0 && holder_node_id < 32);
	return (uint32)1u << (uint32)holder_node_id;
}


static void
pcm_entry_lock_exclusive(struct GrdEntry *entry)
{
	pgstat_report_wait_start(WAIT_EVENT_PCM_TRANSITION_APPLY);
	LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
	pgstat_report_wait_end();
	/* A conservative sequence barrier: every exclusive Resource/GRD entry
	 * episode invalidates an earlier R8/R10 proof, even if that episode later
	 * discovers an exact duplicate and performs no byte mutation. */
	pcm_resource_x_semantic_mutation_mark();
}


/* ============================================================
 * PGRAC: spec-2.30 D2 — 9 counter accessors (read-only observability).
 * ============================================================ */
uint64
cluster_pcm_get_trans_n_to_s_count(void)
{
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->trans_n_to_s_count) : 0;
}

uint64
cluster_pcm_get_trans_n_to_x_count(void)
{
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->trans_n_to_x_count) : 0;
}

uint64
cluster_pcm_get_trans_s_to_x_upgrade_count(void)
{
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->trans_s_to_x_upgrade_count) : 0;
}

uint64
cluster_pcm_get_trans_x_to_s_downgrade_count(void)
{
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->trans_x_to_s_downgrade_count) : 0;
}

/* PGRAC: spec-6.14a D2 — observability for the (b) fail-closed leg. */
uint64
cluster_pcm_get_local_s_revoke_nonholder_failclosed_count(void)
{
	return ClusterPcm != NULL
			   ? pg_atomic_read_u64(&ClusterPcm->local_s_revoke_nonholder_failclosed_count)
			   : 0;
}

/* PGRAC: spec-6.14 D5 — observability for the aux-deferred remote S release. */
uint64
cluster_pcm_get_evict_release_deferred_aux_count(void)
{
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->evict_release_deferred_aux_count)
							  : 0;
}

/* PGRAC ownership-generation wave: cached-X writer re-verify observability. */
void
cluster_pcm_note_writer_cover_stale_detected(void)
{
	if (ClusterPcm != NULL)
		pg_atomic_fetch_add_u64(&ClusterPcm->writer_cover_stale_detected_count, 1);
}

void
cluster_pcm_note_writer_reverify_reacquire(void)
{
	if (ClusterPcm != NULL)
		pg_atomic_fetch_add_u64(&ClusterPcm->writer_reverify_reacquire_count, 1);
}

uint64
cluster_pcm_get_writer_cover_stale_detected_count(void)
{
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->writer_cover_stale_detected_count)
							  : 0;
}

uint64
cluster_pcm_get_writer_reverify_reacquire_count(void)
{
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->writer_reverify_reacquire_count)
							  : 0;
}

/* PGRAC ownership-generation wave (W2): drop-restore ABA observability. */
void
cluster_pcm_note_restore_aba_detected(void)
{
	if (ClusterPcm != NULL)
		pg_atomic_fetch_add_u64(&ClusterPcm->restore_aba_detected_count, 1);
}

uint64
cluster_pcm_get_restore_aba_detected_count(void)
{
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->restore_aba_detected_count) : 0;
}

/*
 * PGRAC ownership-generation wave (W3): count invalidate directives parked
 * because a grant for the same tag was in flight (GRANT_PENDING) while the
 * local pcm_state still read N.  A non-zero delta proves the handler declined
 * to ack the in-flight grant away.
 */
void
cluster_pcm_note_invalidate_parked_grant_pending(void)
{
	if (ClusterPcm != NULL)
		pg_atomic_fetch_add_u64(&ClusterPcm->invalidate_parked_grant_pending_count, 1);
}

uint64
cluster_pcm_get_invalidate_parked_grant_pending_count(void)
{
	return ClusterPcm != NULL
			   ? pg_atomic_read_u64(&ClusterPcm->invalidate_parked_grant_pending_count)
			   : 0;
}

/* S3 forensics step 1b — advance-provenance table insert drops (table full
 * with no slot for the tag;  absence of a record is then inconclusive). */
uint64
cluster_pcm_get_wm_prov_insert_fail_count(void)
{
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->wm_prov_insert_fail_count) : 0;
}

uint64
cluster_pcm_get_trans_x_to_n_downgrade_count(void)
{
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->trans_x_to_n_downgrade_count) : 0;
}

uint64
cluster_pcm_get_trans_x_to_n_release_count(void)
{
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->trans_x_to_n_release_count) : 0;
}

uint64
cluster_pcm_get_trans_s_to_n_invalidate_count(void)
{
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->trans_s_to_n_invalidate_count) : 0;
}

uint64
cluster_pcm_get_trans_s_to_n_release_count(void)
{
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->trans_s_to_n_release_count) : 0;
}

uint64
cluster_pcm_get_trans_s_to_x_cleanout_count(void)
{
	/* HC60 永 0 until Stage 3 AD-006 第五轮 wires Trans-9 body. */
	return ClusterPcm != NULL ? pg_atomic_read_u64(&ClusterPcm->trans_s_to_x_cleanout_count) : 0;
}


/* ============================================================
 * PGRAC: spec-2.30 D2 (Step 3) — 4 mutation API真激活.
 *
 *	HC56 transition validator gate + HC57 LWLock EXCLUSIVE held + HC58
 *	bitmap mutation in lock + HC60 Trans-9 unreachable from acquire path.
 *
 *	disable-path:  cluster.pcm_grd_max_entries=0 → cluster_pcm_htab == NULL
 *	→ preserve spec-1.7 stub behavior (ereport ERRCODE_FEATURE_NOT_SUPPORTED).
 *
 *	HC56 illegal transition path:  validator returns false → ereport(ERROR,
 *	ERRCODE_DATA_CORRUPTED) — caller bug or GRD state corruption.
 *
 *	HC59 fail-closed cap path:  pcm_get_or_create_entry returns NULL when
 *	HTAB FULL → ereport(ERROR, ERRCODE_OUT_OF_MEMORY).
 * ============================================================ */

#define PCM_STUB_DISABLED_PATH                                                                     \
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),                                        \
					errmsg("PCM lock manager disabled (cluster.pcm_grd_max_entries=0)"),           \
					errhint("Set cluster.pcm_grd_max_entries to NBuffers and restart to "          \
							"activate the spec-2.30 PCM state machine.")))


static bool
pcm_lock_acquire_local_pinned(BufferTag tag, PcmLockMode mode,
	PcmAuthoritySnapshot *remote_x_out, PcmEntryWaitContext *wait_context)
{
	struct GrdEntry *entry;
	int holder_node;
	uint32 holder_bit;

	Assert(wait_context != NULL
		   && pcm_entry_ref_identity_exact(&wait_context->ref));
	entry = wait_context->ref.entry;
	holder_node = cluster_node_id;

	holder_bit = pcm_holder_bit(holder_node);

	/*
	 * PGRAC: spec-2.31 D1 v0.4 — bufmgr-safe blocking acquire loop.
	 *
	 *	Single-node multi-backend semantics:
	 *	  - S + N (no holders)                → state→S, refcount=1
	 *	  - S + S (this node already holds)   → refcount++ (no master change)
	 *	  - S + S (other node holds; n/a in single-node) → set bit + refcount=1
	 *	  - S + X                             → wait on wait_cv
	 *	  - X + N                             → state→X
	 *	  - X + S / X + X                     → wait on wait_cv
	 *
	 *	Wait path uses ConditionVariable with WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT
	 *	for DBA observability (pg_stat_activity.wait_event).  HC57 still holds
	 *	for transition mutation (only inside entry_lock EXCLUSIVE).
	 */
	for (;;) {
		PcmState cur;
		bool pending_x_blocks_new_s = false;

		pcm_entry_lock_exclusive(entry);

		cur = (PcmState)pg_atomic_read_u32(&entry->master_state);

		if (mode == PCM_LOCK_MODE_S) {
			pending_x_blocks_new_s = !pcm_s_admission_allowed_locked(entry, holder_bit);
			if (!pending_x_blocks_new_s && cur == PCM_STATE_N) {
				cluster_pcm_transition_apply(entry, PCM_TRANS_N_TO_S, holder_node);
				entry->s_holder_refcount_local = 1;
				LWLockRelease(&entry->entry_lock.lock);
				return true;
			}
			if (!pending_x_blocks_new_s && cur == PCM_STATE_S) {
				/* Same-node S re-acquire: bump refcount; or join from other-node-S. */
				if ((pg_atomic_read_u32(&entry->s_holders_bitmap) & holder_bit) != 0)
					entry->s_holder_refcount_local++;
				else {
					pg_atomic_fetch_or_u32(&entry->s_holders_bitmap, holder_bit);
					entry->s_holder_refcount_local = 1;
				}
				LWLockRelease(&entry->entry_lock.lock);
				return true;
			}
			/* cur == X → fall through to wait */
		} else /* mode == PCM_LOCK_MODE_X */
		{
			uint32 holders;

			if (cur == PCM_STATE_N) {
				cluster_pcm_transition_apply(entry, PCM_TRANS_N_TO_X, holder_node);
				LWLockRelease(&entry->entry_lock.lock);
				return true;
			}
			/*
			 * PGRAC: spec-6.12a — idempotent X re-acquire.  This NODE already
			 * holds X (e.g. a second local backend raced in after a quiescent
			 * X->S downgrade + local upgrade flipped buf->pcm_state, or the
			 * covering-mode cache was bypassed).  Node-level X is already
			 * ours; PG's buffer content lock serializes the local backends.
			 * Mirrors the spec-4.7a D3 master-handler idempotent re-ack.
			 * Without this branch the second backend would CV-wait forever
			 * (nothing ever broadcasts for a state that is already granted).
			 */
			if (cur == PCM_STATE_X && entry->x_holder_node == holder_node) {
				LWLockRelease(&entry->entry_lock.lock);
				return true;
			}

			holders = pg_atomic_read_u32(&entry->s_holders_bitmap);
			if (cur == PCM_STATE_S && (holders & holder_bit) != 0 && (holders & ~holder_bit) == 0) {
				/*
				 * spec-2.35 HC111/HC112: an S bit records cache residency,
				 * not a currently held shared content_lock.  A later local X
				 * acquire by the same node must upgrade the residency claim
				 * instead of waiting forever for its own preserved S bit.
				 * PG's content_lock is still acquired after this point and
				 * serializes against any in-process shared readers.
				 */
				cluster_pcm_transition_apply(entry, PCM_TRANS_S_TO_X_UPGRADE, holder_node);
				entry->s_holder_refcount_local = 0;
				LWLockRelease(&entry->entry_lock.lock);
				return true;
			}
			/* cur == S or X → fall through to wait */
		}

		/*
		 * PGRAC: spec-4.7a B (HG7 local-path completion) — bounded fail-closed
		 * for cross-node write contention on the LOCAL master path.  With
		 * hold-until-revoked (cluster_gcs_block_local_cache on), the
		 * incompatible holder reached here is a remote LIVE node that will NOT
		 * release on its own — the cross-node writer transfer / BAST that would
		 * revoke it is deferred (spec-2.36 / 4.7 / Stage 6).  Waiting on wait_cv
		 * would hang forever (this local master path emits no cross-node
		 * invalidate).  Fail closed with a bounded terminal instead — mirrors
		 * the D4 remote-dispatch gate so HG7's "no hang" covers BOTH the
		 * remote-request and local-master acquire paths.  Cache off (serialized-
		 * node merged-recovery / shared-data smoke tests) keeps the legitimate
		 * short wait (the holder releases on content-lock unlock).  Read the
		 * conflicting holder under the already-held entry_lock (no extra
		 * htab_lock → no lock-order inversion).
		 */
		if (cluster_gcs_block_local_cache && !pending_x_blocks_new_s) {
			int32 confl_x = entry->x_holder_node;
			uint32 confl_s = pg_atomic_read_u32(&entry->s_holders_bitmap) & ~holder_bit;
			bool remote_live = false;
			int n;

			if (confl_x >= 0 && confl_x != holder_node
				&& cluster_cssd_get_peer_state(confl_x) != CLUSTER_CSSD_PEER_DEAD)
				remote_live = true;
			for (n = 0; !remote_live && n < 32; n++)
				if ((confl_s & ((uint32)1u << n)) != 0
					&& cluster_cssd_get_peer_state(n) != CLUSTER_CSSD_PEER_DEAD)
					remote_live = true;

			if (remote_live) {
				/* P0-26: the optimistic buffer-aware precheck may race a queue
				 * handoff.  Capture the complete remote-X authority under the
				 * same entry lock instead of escaping through the tag-only
				 * legacy terminal; the caller owns the BufferDesc transfer. */
				if (cur == PCM_STATE_X && confl_x >= 0 && confl_x != holder_node
					&& remote_x_out != NULL) {
					pcm_authority_snapshot_locked(entry, remote_x_out);
					LWLockRelease(&entry->entry_lock.lock);
					return false;
				}

				/*
				 * PGRAC: spec-6.12a — LOCAL-master S->X upgrade.  When the
				 * conflict is ONLY remote S copies (the quiescent X->S
				 * downgrade parked them there) and this node is itself an S
				 * holder, revoke them: pending_x barrier + INVALIDATE via
				 * the backend outbound ring + ack-certified bit clearing +
				 * S_TO_X_UPGRADE (cluster_gcs_block_local_x_upgrade).  A
				 * remote X conflict stays on the pre-6.12a fail-closed
				 * (writer transfer is spec-2.36 / 4.7 territory).
				 *
				 * PGRAC: spec-6.14a D2 — the cluster_read_scache gate is
				 * removed: plain read-sharing (multiple nodes N->S on a
				 * never-written block, then one of them writes — the
				 * shared-catalog boot shape) creates the same S-vs-S
				 * conflict without any quiescent downgrade, and the arm's
				 * own preconditions already bound it.  Without the arm that
				 * shape fail-closed unconditionally.
				 */
				if (mode == PCM_LOCK_MODE_X && cur == PCM_STATE_S
					&& (confl_x < 0 || confl_x == holder_node)
					&& (pg_atomic_read_u32(&entry->s_holders_bitmap) & holder_bit) != 0) {
					LWLockRelease(&entry->entry_lock.lock);
					pcm_entry_wait_context_cancel_wait(wait_context);
					if (cluster_gcs_block_local_x_upgrade(tag)) {
						pcm_entry_lock_exclusive(entry);
						entry->s_holder_refcount_local = 0;
						LWLockRelease(&entry->entry_lock.lock);
						return true;
					}
					/* Invalidate did not complete — fail closed, retryable
					 * (Rule 8.A: never write past an unconfirmed invalidate). */
					ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
									errmsg("cluster_pcm: S->X upgrade invalidate did not complete"),
									errhint("Remote S holders did not all acknowledge in time; "
											"retry the statement.")));
				}

				/*
				 * PGRAC: spec-6.14a D2 (b) — X requested by a node with NO S
				 * residency while other live nodes cache the block in S.
				 * There is no provable-current local carrier to write on: a
				 * revoked dirty-S copy is dropped after only an XLogFlush
				 * (bufmgr invalidate contract), so shared storage may be
				 * stale post-revoke — writing on it would be a lost update.
				 * Fail closed (bounded, counted); holder-ship capture is a
				 * later spec.  Reaching here implies our own S bit is clear
				 * (the upgrade arm above handles the holder case).
				 */
				if (mode == PCM_LOCK_MODE_X && cur == PCM_STATE_S
					&& (confl_x < 0 || confl_x == holder_node)) {
					pg_atomic_fetch_add_u64(&ClusterPcm->local_s_revoke_nonholder_failclosed_count,
											1);
					LWLockRelease(&entry->entry_lock.lock);
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cluster_pcm: cross-node write needs local S residency "
										   "while other nodes cache this block"),
									errhint("This node holds no current copy (it never read the "
											"block), so revoking the remote shared copies would "
											"leave no provable-current image to write on.")));
				}

				LWLockRelease(&entry->entry_lock.lock);
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cluster_pcm: cross-node block write transfer not "
									   "supported in this stage"),
								errhint("Another live node holds this block; concurrent "
										"cross-node write (writer transfer) lands in "
										"spec-2.36 / 4.7. Set cluster.gcs_block_local_cache "
										"= off for serialized-node workloads.")));
			}
		}

		/* Incompatible state — wait on CV. */
		if (!wait_context->cv_prepared) {
			pcm_entry_wait_ref_begin(wait_context);
			ConditionVariablePrepareToSleep(&entry->wait_cv);
			wait_context->cv_prepared = true;
		}
		LWLockRelease(&entry->entry_lock.lock);
		ConditionVariableSleep(&entry->wait_cv, WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
		if (!pcm_entry_ref_identity_exact(&wait_context->ref)) {
			pcm_resource_x_reconfig_block();
			ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("PCM waiter entry identity changed after wakeup")));
		}
		/* loop and re-check master_state */
	}
}

static bool
pcm_lock_acquire_local(BufferTag tag, PcmLockMode mode,
	PcmAuthoritySnapshot *remote_x_out)
{
	PcmEntryWaitContext wait_context;
	PcmEntryAcquireResult acquire_result;
	bool acquired = false;

	CLUSTER_INJECTION_POINT("cluster-pcm-acquire-entry");

	if (cluster_pcm_htab == NULL)
		PCM_STUB_DISABLED_PATH;

	if (mode != PCM_LOCK_MODE_S && mode != PCM_LOCK_MODE_X)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("cluster_pcm_lock_acquire: invalid mode %d (must be S=1 or X=2)",
					(int)mode)));

	/* A tag-only cross-node caller still has no BufferDesc in which to
	 * install a shipped image.  D2 changes only entry lifetime, not routing. */
	if (cluster_gcs_lookup_master(tag) != cluster_node_id)
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cluster_pcm_lock_acquire: remote-master S/X requires "
					"BufferDesc-aware path"),
			 errhint("Use cluster_pcm_lock_acquire_buffer() instead; the "
					 "data plane needs a BufferDesc to install received "
					 "block bytes under content_lock EXCLUSIVE (HC84).")));

	if (cluster_node_id < 0 || cluster_node_id >= 32)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("cluster_pcm_lock_acquire: cluster_node_id=%d out of [0, 32) range",
					cluster_node_id)));

	memset(&wait_context, 0, sizeof(wait_context));
	if (!pcm_entry_ref_acquire(&tag, true, &wait_context.ref,
			&acquire_result))
		ereport(ERROR,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("cluster_pcm_lock_acquire: PCM GRD entry unavailable "
					"(cap=%d result=%d)",
					pcm_grd_effective, (int)acquire_result)));

	PG_TRY();
	{
		acquired = pcm_lock_acquire_local_pinned(
			tag, mode, remote_x_out, &wait_context);
	}
	PG_FINALLY();
	{
		pcm_entry_wait_context_cleanup(&wait_context);
	}
	PG_END_TRY();
	return acquired;
}

void
cluster_pcm_lock_acquire(BufferTag tag, PcmLockMode mode)
{
	/* A tag-only caller has no BufferDesc and therefore retains the
	 * historical fail-closed behavior for a remote-X conflict. */
	(void)pcm_lock_acquire_local(tag, mode, NULL);
}


/*
 * PGRAC: spec-5.2a D2 — clean-page X-transfer arm (backend-local, one-shot).
 *
 *	A single per-backend bool.  Set by cluster_pcm_clean_page_xfer_arm(true)
 *	right before a deliberately-clean cluster PCM X acquire (sequence refill,
 *	spec-5.2a D5).  cluster_pcm_lock_acquire_buffer consumes it exactly once
 *	(read-and-clear) so it can never bleed into a later heap access (inv ①/⑤).
 *	No shared memory, no locking — purely local to the requesting backend.
 */
static bool clean_page_xfer_armed = false;

void
cluster_pcm_clean_page_xfer_arm(bool armed)
{
	clean_page_xfer_armed = armed;
}

bool
cluster_pcm_clean_page_xfer_is_armed(void)
{
	return clean_page_xfer_armed;
}

bool
cluster_pcm_clean_page_xfer_consume(void)
{
	bool was_armed = clean_page_xfer_armed;

	clean_page_xfer_armed = false;
	return was_armed;
}


/*
 * PGRAC: spec-2.33 D7 — BufferDesc-aware PCM acquire.
 *
 *	Decision tree (§3.1):
 *	  master == self    → local fast path (same as cluster_pcm_lock_acquire)
 *	  master != self    → cluster_gcs_send_block_request_and_wait (HC79)
 *
 *	Required by bufmgr LockBuffer because the GCS data plane needs to
 *	install received block bytes into this buffer's content on GRANTED
 *	(HC84 PageSetLSN + memcpy under content_lock EXCLUSIVE).
 */
bool
cluster_pcm_lock_acquire_buffer(BufferDesc *buf, PcmLockMode mode, bool *out_retry_denied)
{
	BufferTag tag;
	int master_node;
	bool clean_eligible;

	Assert(out_retry_denied != NULL);
	if (out_retry_denied == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_pcm_lock_acquire_buffer: NULL retry result")));
	*out_retry_denied = false;
	if (buf == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_pcm_lock_acquire_buffer: NULL BufferDesc")));

	/*
	 * PGRAC: spec-5.2a D2 — consume the clean-page X-transfer arm exactly once
	 * here (read-and-clear), BEFORE any fail-closed path below, so the
	 * eligibility can never leak into a subsequent buffer access regardless of
	 * the path taken or an error thrown (inv ①/⑤).  Only an X acquire can be a
	 * clean-page transfer; an S read never arms, so consuming for S is a no-op
	 * (the flag is already false in practice, but consume() is unconditional to
	 * guarantee no leak).
	 */
	clean_eligible = cluster_pcm_clean_page_xfer_consume();

	if (cluster_pcm_htab == NULL)
		PCM_STUB_DISABLED_PATH;

	if (mode != PCM_LOCK_MODE_S && mode != PCM_LOCK_MODE_X)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_pcm_lock_acquire_buffer: invalid mode %d "
							   "(must be S=1 or X=2)",
							   (int)mode)));

	/*
	 * PGRAC: spec-6.14 D9 amend (INV-D9-R) — designed fail-closed boundary
	 * for processes without a backend identity (startup / aux:
	 * MyBackendId = InvalidBackendId).  The live GCS data plane keys its
	 * per-backend outstanding-slot table by MyBackendId and belongs to
	 * post-PM_RUN backends only; recovery-time access to cluster-coherent
	 * pages is lawful solely inside a recovery-ownership window (cold
	 * merged-replay window: PCM globally inactive; or the survivor's online
	 * thread-recovery engine, which bypasses the buffer manager).  Reaching
	 * this point from redo means the WAL tail holds records for PCM-tracked
	 * shared pages that no recovery-ownership regime covers -- refuse loudly
	 * instead of tripping the slot-table range check downstream
	 * (historically the accidental "MyBackendId=-1" internal FATAL,
	 * t/243 L4 scope note signature (a)).
	 */
	if (MyBackendId == InvalidBackendId)
		ereport(
			FATAL,
			(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
			 errmsg(
				 "crash recovery of a cluster-coherent page requires a recovery-ownership window"),
			 errdetail("A process without a backend identity attempted a live GCS block request "
					   "(page coherence negotiation) during recovery."),
			 errhint("Cold multi-node crash recovery must run under cluster.merged_recovery=on; "
					 "a node restarting while peers are alive must wait for the survivor's "
					 "online thread recovery to cover its WAL tail before restarting.")));

	tag = buf->tag;

	/*
	 * PGRAC: spec-4.7 D1 — RECOVERING gate.  A block whose GCS master is
	 * being recovered after reconfiguration (master DEAD; block-protocol
	 * state volatile and not yet rebuilt — D2/D3) must NOT be served from
	 * stale local state nor routed to the dead master.  Wait a bounded
	 * cluster.gcs_block_recovery_wait_ms for the rebuild, re-checking the
	 * phase, then fail-closed 53R9L (retryable).  This gate precedes the
	 * master==self / master!=self routing below, so it covers BOTH the
	 * local-master fast path and the remote-dispatch path (L240 — a
	 * fail-closed must be mirrored on every path reaching the same hazard).
	 */
	if (cluster_gcs_block_phase_for_tag(tag) == GCS_BLOCK_RECOVERING) {
		long waited_us = 0;
		const long step_us = 20000; /* 20 ms */
		long budget_us = (long)cluster_gcs_block_recovery_wait_ms * 1000L;

		while (cluster_gcs_block_phase_for_tag(tag) == GCS_BLOCK_RECOVERING) {
			if (waited_us >= budget_us)
				ereport(ERROR,
						(errcode(ERRCODE_CLUSTER_GCS_BLOCK_RESOURCE_RECOVERING),
						 errmsg("block-level cache protocol state is being rebuilt "
								"after reconfiguration"),
						 errhint("The block resource is recovering (survivor re-declare / "
								 "master rebuild after node failure); retry the transaction. "
								 "If the failed node stays down, restart it to run its "
								 "instance recovery, or enable online thread recovery in a "
								 "supported scope (cluster.online_thread_recovery).")));

			CHECK_FOR_INTERRUPTS();
			pgstat_report_wait_start(WAIT_EVENT_GCS_BLOCK_RECOVERING);
			pg_usleep(step_us);
			pgstat_report_wait_end();
			waited_us += step_us;
		}
	}

	master_node = cluster_gcs_lookup_master(tag);

	if (master_node != cluster_node_id) {
		PcmLockTransition trans = (mode == PCM_LOCK_MODE_S) ? PCM_TRANS_N_TO_S : PCM_TRANS_N_TO_X;

		/*
		 * HC79: data-plane block request.  Sender will install received
		 * bytes into buf under content_lock EXCLUSIVE on GRANTED, or keep
		 * the shared-storage page on GRANTED_STORAGE_FALLBACK (HC88).
		 */
		/* spec-5.2 D2: returns false for a one-shot READ_IMAGE so the bufmgr
		 * leaves buf->pcm_state == N (no durable ownership recorded).
		 * spec-5.2a D2/D3: carry clean-page eligibility (X only) so the remote
		 * master takes the dedicated clean-page X-transfer path rather than the
		 * conservative HG7 fail-closed DENY. */
		return cluster_gcs_send_block_request_and_wait(
			buf, trans, master_node, clean_eligible && mode == PCM_LOCK_MODE_X, out_retry_denied);
	}

	/*
	 * Local fast path:  reuse the existing tag-only implementation now that
	 * we've already established master == self (the inner master-lookup
	 * branch in cluster_pcm_lock_acquire would otherwise fail-closed under
	 * spec-2.33 D7 because it cannot reach the data plane without a
	 * BufferDesc — but with master == self that branch is never taken).
	 *
	 * spec-5.2 D2 (sub-case B): when master == self but the block is held in X
	 * by a REMOTE node, an N→S reader cannot be served by the tag-only acquire
	 * (no data plane).  Forward a read-image request to the holder and install
	 * the shipped current image for this read (non-durable — returns false so
	 * the caller leaves buf->pcm_state == N).  spec-6.12a ㉕: if the holder
	 * accepted the piggybacked downgrade request this returns true instead
	 * (durable S; caller mirrors pcm_state = S).
	 */
	if (mode == PCM_LOCK_MODE_S) {
		PcmAuthoritySnapshot authority;
		bool have_authority;
		bool remote_x_exact;
		bool s_barrier_active;

		/*
		 * R10/A' PGRAC adaptation: the local-master path does not enter the
		 * block-request dedup table, so it cannot observe that path's pending-X
		 * queue cookie during the pre-ASSERT interval.  An exact active PCM-X
		 * head or approved late-bind successor head is enough to defer this
		 * legacy S acquisition; it is not image or
		 * PI authority.  Re-entering through the existing reservation retry
		 * boundary forces a fresh BufferDesc/GRD generation sample.
		 */
		have_authority = cluster_pcm_lock_authority_snapshot(tag, &authority);
		remote_x_exact = have_authority && authority.state == PCM_STATE_X
			&& authority.x_holder_node >= 0
			&& authority.x_holder_node != cluster_node_id
			&& authority.s_holders_bitmap == 0
			&& authority.master_holder.node_id == (uint32)authority.x_holder_node;
		s_barrier_active = cluster_gcs_block_resource_x_local_s_barrier_active(tag);
		if (s_barrier_active && !remote_x_exact) {
			*out_retry_denied = true;
			return false;
		}

		if (remote_x_exact)
			return cluster_gcs_local_master_read_image_and_wait(
				buf, &authority, s_barrier_active, out_retry_denied);
	} else /* mode == PCM_LOCK_MODE_X */
	{
		/*
		 * PGRAC: spec-5.2 D11 — local-master writer-transfer (revoke).  master
		 * == self but a REMOTE node holds the block in X, and a local writer
		 * needs X.  The tag-only acquire below would bounded-fail-close (no
		 * cross-node writer transfer; spec-4.7a D4 / cluster_pcm_lock_acquire).
		 * Instead forward an X-transfer request to the holder: it ships its
		 * current image (carrying the uncommitted ITL row-lock the writer must
		 * wait on) and releases its X;  we install the bytes and take X durably.
		 * The heap AM then sees the remote row lock and enters the cross-node TX
		 * completion wait (spec-5.2 D4/D5).
		 */
		PcmAuthoritySnapshot authority;
		bool have_authority = cluster_pcm_lock_authority_snapshot(tag, &authority);
		PcmLockMode master_state = have_authority ? (PcmLockMode)authority.state : PCM_LOCK_MODE_N;

		if (have_authority && authority.state == PCM_STATE_X && authority.x_holder_node >= 0
			&& authority.x_holder_node != cluster_node_id && authority.s_holders_bitmap == 0
			&& authority.master_holder.node_id == (uint32)authority.x_holder_node)
			return cluster_gcs_local_master_x_transfer_and_wait(buf, &authority, clean_eligible,
																out_retry_denied);

		/*
		 * PGRAC: spec-4.6a BUG-C2 follow-through for shared_catalog DDL after
		 * fail-stop.  Local-master state=S with other live S holders but no local
		 * S bit used to fall through to the tag-only X acquire, which fail-closed
		 * as "no local S residency".  This entry point is buffer-aware: the
		 * caller has already read or initialized the BufferDesc before asking for
		 * X, so first register a local S residency, then reuse the existing
		 * local S->X invalidate/upgrade path.  If the invalidate cannot be proven,
		 * drop the temporary S claim and rethrow the same fail-closed error.
		 */
		if (master_state == PCM_LOCK_MODE_S && cluster_node_id >= 0 && cluster_node_id < 32) {
			uint32 self_bit = (uint32)1u << (uint32)cluster_node_id;

			if ((cluster_pcm_lock_query_s_holders_bitmap(tag) & self_bit) == 0) {
				PcmEntryRef entry_ref;
				PcmEntryAcquireResult acquire_result;
				struct GrdEntry *entry;
				PcmAuthoritySnapshot raced_remote_x;
				bool upgraded = false;

				/* The S bootstrap is still BufferDesc-aware: if a queue handoff
				 * replaces the sampled S authority with remote X before this entry
				 * lock, preserve that exact authority and use the normal X-transfer
				 * path instead of the tag-only legacy terminal. */
				if (!pcm_lock_acquire_local(tag, PCM_LOCK_MODE_S, &raced_remote_x))
					return cluster_gcs_local_master_x_transfer_and_wait(
						buf, &raced_remote_x, clean_eligible, out_retry_denied);
				/* Amendment v1.2 (R5): the upgrade waits on remote ACKs with
				 * CHECK_FOR_INTERRUPTS in the loop, so a cancel can THROW out
				 * of it — release the temporary S claim on that path too, not
				 * only on the false return. */
				PG_TRY();
				{
					upgraded = cluster_gcs_block_local_x_upgrade(tag);
				}
				PG_CATCH();
				{
					cluster_pcm_lock_release(tag);
					PG_RE_THROW();
				}
				PG_END_TRY();
				if (!upgraded) {
					cluster_pcm_lock_release(tag);
					ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
									errmsg("cluster_pcm: S->X upgrade invalidate did not complete"),
									errhint("Remote S holders did not all acknowledge in time; "
											"retry the statement.")));
				}

				if (pcm_entry_ref_acquire(
						&tag, false, &entry_ref, &acquire_result)) {
					entry = entry_ref.entry;
					pcm_entry_lock_exclusive(entry);
					/* Amendment v1.2 (R9): the completed S->X upgrade replaces
					 * ALL of this node's S declarations for the tag (the X
					 * grant subsumes them), so the local S refcount is
					 * intentionally hard-reset rather than decremented — a
					 * later release of the X does not owe any S releases. */
					entry->s_holder_refcount_local = 0;
					LWLockRelease(&entry->entry_lock.lock);
					pcm_entry_ref_release(&entry_ref);
				}
				/* PGRAC: GCS-race round-4c FUNC-1 (local-master flavour) —
				 * this node held NO local S bit, so the buffer bytes are a
				 * bare ReadBuffer pre-read; a remote X holder may have
				 * yield-flushed a newer version between that pre-read and
				 * this upgrade (the invalidate ACKs above just advanced the
				 * local authoritative watermark).  Prove the copy current
				 * or discard-and-re-read shared storage. */
				cluster_gcs_block_fallback_verify_refresh(
					buf, tag, cluster_pcm_lock_pi_watermark_scn_query(tag));
				return true;
			}
		}
	}

	{
		PcmAuthoritySnapshot raced_remote_x;

		if (!pcm_lock_acquire_local(tag, mode, &raced_remote_x)) {
			if (mode == PCM_LOCK_MODE_S)
				return cluster_gcs_local_master_read_image_and_wait(buf, &raced_remote_x,
					cluster_gcs_block_resource_x_local_s_barrier_active(tag),
					out_retry_denied);
			return cluster_gcs_local_master_x_transfer_and_wait(buf, &raced_remote_x,
																clean_eligible, out_retry_denied);
		}
	}
	/*
	 * PGRAC: GCS-race round-4c FUNC-1 (local-master flavour) — the tag-only
	 * grant ships no image, so the buffer keeps this backend's pre-read
	 * bytes.  The same pre-read-vs-yield-flush window as the remote storage
	 * fallback applies when a remote X holder yielded between our ReadBuffer
	 * and this grant: prove the local bytes current against the local
	 * authoritative pi_watermark_scn or discard-and-re-read shared storage.
	 * Watermark InvalidScn (never remotely written / untracked / extension
	 * block) is a SKIP — no cost beyond one GRD lookup.  Query AFTER the
	 * acquire: a mode=X acquire may have just collected invalidate ACKs,
	 * which advance the watermark with the dropped copies' page SCNs.
	 */
	cluster_gcs_block_fallback_verify_refresh(buf, tag,
											  cluster_pcm_lock_pi_watermark_scn_query(tag));
	return true;
}


void
cluster_pcm_lock_release(BufferTag tag)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	PcmState cur;
	BufferTag retire_tag;
	uint64 retire_generation = 0;
	int holder_node;
	uint32 holder_bit;
	bool broadcast_needed = false;
	bool fast_retire = false;

	CLUSTER_INJECTION_POINT("cluster-pcm-release-pre");

	if (cluster_pcm_htab == NULL)
		PCM_STUB_DISABLED_PATH;

	/*
	 * PGRAC: spec-2.32 D5 — HC78 release must symmetrize wire if acquire
	 * went through master.  master==self short-circuits to spec-2.31 local
	 * path (HC72).
	 */
	{
		int master_node = cluster_gcs_lookup_master(tag);

		if (master_node != cluster_node_id) {
			cluster_gcs_send_transition_and_wait(tag, PCM_TRANS_S_TO_N_RELEASE, master_node);
			return;
		}
	}

	holder_node = cluster_node_id;
	if (holder_node < 0 || holder_node >= 32)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_pcm_lock_release: cluster_node_id=%d out of [0, 32) range",
							   holder_node)));

	if (!pcm_entry_ref_acquire(
			&tag, false, &entry_ref, &acquire_result))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_pcm_lock_release: no PCM entry for BufferTag (released "
							   "without prior acquire?)")));
	entry = entry_ref.entry;

	holder_bit = pcm_holder_bit(holder_node);

	pcm_entry_lock_exclusive(entry);

	cur = (PcmState)pg_atomic_read_u32(&entry->master_state);

	/*
	 * PGRAC: spec-2.31 D1 v0.4 — refcount-aware release.
	 *
	 *	X → N release:  X holder unique per node; transition always to N;
	 *	                broadcast (X waiter and S waiter both eligible).
	 *	S release:      decrement same-node refcount;  if 0, call
	 *	                S_TO_N_RELEASE which clears this node's bit and
	 *	                transitions to N iff all node bits cleared.
	 *	                broadcast only when state truly went to N (X waiter
	 *	                wakes); same-node refcount-only paths skip broadcast.
	 */
	if (cur == PCM_STATE_X) {
		if (entry->x_holder_node != holder_node) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("cluster_pcm_lock_release: node %d cannot release X held by node %d",
							holder_node, entry->x_holder_node)));
		}
		cluster_pcm_transition_apply(entry, PCM_TRANS_X_TO_N_RELEASE, holder_node);
		broadcast_needed = true;
	} else if (cur == PCM_STATE_S) {
		if ((pg_atomic_read_u32(&entry->s_holders_bitmap) & holder_bit) == 0) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("cluster_pcm_lock_release: node %d is not an S holder", holder_node)));
		}
		/*
		 * PGRAC: spec-2.31 D1 v0.4 — refcount semantics under single-uint16
		 * design (F4 user decision: same-node only).  refcount tracks nested
		 * same-node acquires; cross-node simulations in unit tests may have
		 * overwritten refcount via "other-node S join" branch.  Be lenient:
		 * if refcount > 0, decrement; if refcount == 0 (either reached 0
		 * just now, or was already 0 due to cross-node simulation), clear
		 * this node's bit via transition_apply.
		 */
		if (entry->s_holder_refcount_local > 0)
			entry->s_holder_refcount_local--;
		if (entry->s_holder_refcount_local == 0) {
			cluster_pcm_transition_apply(entry, PCM_TRANS_S_TO_N_RELEASE, holder_node);
			if ((PcmState)pg_atomic_read_u32(&entry->master_state) == PCM_STATE_N)
				broadcast_needed = true;
		}
		/* else: refcount > 0 still; same-node S holder remains; no state change */
	} else {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
					errmsg("cluster_pcm_lock_release: nothing held (state=%d)", (int)cur)));
	}
	if ((PcmState)pg_atomic_read_u32(&entry->master_state) == PCM_STATE_N
		&& pcm_entry_mark_quiescing_locked(entry)) {
		retire_tag = entry_ref.tag;
		retire_generation = entry_ref.binding_generation;
		fast_retire = true;
	}

	LWLockRelease(&entry->entry_lock.lock);

	if (broadcast_needed)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	if (fast_retire)
		(void)pcm_entry_try_retire_exact(
			&retire_tag, retire_generation, PCM_RETIRE_REASON_HOLDER_RELEASE);
}


/*
 * PGRAC: spec-2.33 D7 hardening — BufferDesc/mode-aware release.
 * PGRAC: spec-2.35 D5 (HC111 + HC112) — renamed to
 *   cluster_pcm_lock_release_buffer_for_eviction.  Callers must invoke this
 *   only on real cache-residency loss (InvalidateBuffer / InvalidateVictim
 *   Buffer / DropRelations*Buffers / DropDatabaseBuffers + X content-lock
 *   unlock delegated through cluster_pcm_lock_unlock_content_buffer).  See
 *   cluster_pcm_lock.h banner for the bifurcation rationale.
 *
 * Remote-master release must mirror the mode acquired by
 * cluster_pcm_lock_acquire_buffer().  The tag-only API cannot distinguish an
 * S holder from an X holder when the authoritative entry lives on a remote
 * master, so it conservatively remains the tag-only/local API.  Bufmgr uses
 * this variant and passes the mode it acquired (or the BufferDesc mirror on
 * eviction) so X locks release with X→N rather than the S→N transition.
 */
void
cluster_pcm_lock_release_saved_tag_for_eviction(BufferTag tag, PcmLockMode mode)
{
	PcmEntryRef local_projection_ref;
	PcmEntryAcquireResult local_projection_result;
	BufferTag local_projection_tag;
	uint64 local_projection_generation = 0;
	int master_node;
	PcmLockTransition trans;

	if (cluster_pcm_htab == NULL)
		PCM_STUB_DISABLED_PATH;

	/*
	 * PGRAC: spec-5.2 §3.5 D11 — a deferred-writer read-image
	 * (PCM_STATE_READ_IMAGE) holds NO PCM lock, so there is nothing to
	 * release.  This can reach here when the transient marker leaks past a
	 * content-lock release on transaction abort (LWLockReleaseAll bypasses the
	 * LockBuffer(UNLOCK) clear hook) and the buffer is later evicted.  No-op
	 * for any non-{S,X} mode so eviction never tries to release an unheld lock
	 * -- on a LOCAL-master block whose remote peer holds X, the unconditional
	 * cluster_pcm_lock_release(tag) below would otherwise raise a spurious
	 * DATA_CORRUPTED ("cannot release X held by node M").  (The caller clears
	 * buf->pcm_state to N after this returns, so the leaked marker is cleaned
	 * up; a non-evicted leaked marker is harmless -- the next LockBuffer
	 * re-acquires because cluster_pcm_mode_covers treats it as no-lock.)
	 */
	if (mode != PCM_LOCK_MODE_S && mode != PCM_LOCK_MODE_X)
		return;

	master_node = cluster_gcs_lookup_master(tag);

	if (master_node != cluster_node_id) {
		/* A Resource-X requester may retain one neutral local entry while the
		 * canonical cached-S authority lives at the remote master.  Capture its
		 * exact pre-wire generation; after the master acknowledges final S
		 * eviction, the same D3 table may retire that old projection.  Sampling
		 * after the wait could mistake a concurrent rebind for this release. */
		if (mode == PCM_LOCK_MODE_S
			&& pcm_entry_ref_acquire(&tag, false, &local_projection_ref,
				&local_projection_result))
		{
			local_projection_tag = local_projection_ref.tag;
			local_projection_generation
				= local_projection_ref.binding_generation;
			pcm_entry_ref_release(&local_projection_ref);
		}

		/*
		 * PGRAC: spec-6.14 D5 — an aux-context eviction (the KO flush drain
		 * dropping a whole relfilenode on a peer, spec-5.7 D6) has no
		 * per-backend GCS request slot (MyBackendId is invalid), so it
		 * cannot ride the request/reply wire.  Defer the remote S release:
		 * the master keeps a stale S bit for this node -- the shipped
		 * phantom-holder shape, where a forwarded read finds no resident
		 * copy and falls back to shared storage, which IS current here (the
		 * KO drain flushes before it drops, and an S copy is never ahead of
		 * storage).  The bit self-heals on this node's next acquire of the
		 * block or on GRD entry reclaim.  Only S can legitimately reach
		 * this arm (X is single-holder and the DDL's cluster-wide AEL
		 * excludes a live remote writer before any KO drop), so a tracked X
		 * stays on the throwing wire path below.
		 */
		if (MyBackendId == InvalidBackendId && mode == PCM_LOCK_MODE_S) {
			pg_atomic_fetch_add_u64(&ClusterPcm->evict_release_deferred_aux_count, 1);
			elog(DEBUG1, "cluster_pcm: deferred remote S release for evicted buffer "
						 "(aux context); master keeps a phantom-holder bit");
			return;
		}

		if (mode == PCM_LOCK_MODE_S)
		{
			/*
			 * PGRAC adaptation: a remote X->S downgrade is deliberately
			 * fire-and-forget.  Cache replacement is the last local owner of
			 * that predecessor episode, so first give the current master one
			 * bounded opportunity to settle the exact X@self -> S transition.
			 *
			 * The ordinary S->N remains mandatory: it removes only the shared
			 * holder bit while preserving the predecessor's PI lineage.  If the
			 * earlier nowait X->S already arrived, the first request is rejected
			 * as a duplicate and S->N still closes the eviction.  Any ambiguous
			 * or incompatible predecessor remains fail closed at that unchanged
			 * terminal release.
			 */
			if (!PCM_GCS_TRY_SENDER_LINKED())
				ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("remote-S eviction lacks its GCS predecessor settlement owner")));
			(void) cluster_gcs_try_send_transition_and_wait(
				tag, PCM_TRANS_X_TO_S_DOWNGRADE, master_node);
			trans = PCM_TRANS_S_TO_N_RELEASE;
		}
		else if (mode == PCM_LOCK_MODE_X)
			trans = PCM_TRANS_X_TO_N_RELEASE;
		else
			return; /* nothing to release from the remote master */

		cluster_gcs_send_transition_and_wait(tag, trans, master_node);
		if (mode == PCM_LOCK_MODE_S && local_projection_generation != 0)
			(void)pcm_entry_try_retire_exact(
				&local_projection_tag, local_projection_generation,
				PCM_RETIRE_REASON_HOLDER_RELEASE);
		return;
	}

	/*
	 * Local master remains authoritative in the local GRD entry.  Reuse the
	 * existing tag-only release path so refcount and wakeup semantics stay in
	 * one place.
	 */
	cluster_pcm_lock_release(tag);
}

void
cluster_pcm_lock_release_buffer_for_eviction(BufferDesc *buf, PcmLockMode mode)
{
	if (buf == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_pcm_lock_release_buffer_for_eviction: NULL BufferDesc")));

	cluster_pcm_lock_release_saved_tag_for_eviction(buf->tag, mode);
}

/*
 * PGRAC: spec-2.35 D5 (HC111 + HC112) — content-lock unlock variant.
 *
 *	Called from bufmgr LockBuffer(BUFFER_LOCK_UNLOCK) when the in-process
 *	content_lock LWLock is dropped but the buffer is still resident in the
 *	shared buffer pool.  Per HC111, an SCUR cache residency bit must
 *	survive this event (so the master can still forward subsequent
 *	GCS_BLOCK_REQUEST to this node).  XCUR is single-holder so content-lock
 *	unlock genuinely releases (matches spec-2.31 D7 prior semantic for X).
 */
void
cluster_pcm_lock_unlock_content_buffer(BufferDesc *buf, PcmLockMode mode)
{
	if (buf == NULL)
		return;
	if (cluster_pcm_htab == NULL)
		return;

	/* HC111: S-holder bit = cache residency, NOT transient content-lock
	 * holding.  Content-lock unlock leaves the bit set so subsequent
	 * read traffic on other nodes can be forwarded here. */
	if (mode == PCM_LOCK_MODE_S)
		return;

	/* X is single-holder semantics; content-lock unlock = release X. */
	if (mode == PCM_LOCK_MODE_X) {
		cluster_pcm_lock_release_buffer_for_eviction(buf, mode);
		return;
	}

	/* mode == N: nothing to release */
}


void
cluster_pcm_lock_upgrade(BufferTag tag)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	PcmState cur;
	int holder_node;

	CLUSTER_INJECTION_POINT("cluster-pcm-convert-pre");

	if (cluster_pcm_htab == NULL)
		PCM_STUB_DISABLED_PATH;

	/* PGRAC: spec-2.32 D5 — HC78 upgrade symmetric wire when master remote. */
	{
		int master_node = cluster_gcs_lookup_master(tag);

		if (master_node != cluster_node_id) {
			cluster_gcs_send_transition_and_wait(tag, PCM_TRANS_S_TO_X_UPGRADE, master_node);
			return;
		}
	}

	holder_node = cluster_node_id;
	if (holder_node < 0 || holder_node >= 32)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_pcm_lock_upgrade: cluster_node_id=%d out of [0, 32) range",
							   holder_node)));

	if (!pcm_entry_ref_acquire(
			&tag, false, &entry_ref, &acquire_result))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_pcm_lock_upgrade: no PCM entry for BufferTag (must "
							   "acquire S first)")));
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);

	cur = (PcmState)pg_atomic_read_u32(&entry->master_state);
	if (cur != PCM_STATE_S) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cluster_pcm_lock_upgrade: state=%d (must be S to upgrade)", (int)cur)));
	}
	if ((pg_atomic_read_u32(&entry->s_holders_bitmap) & pcm_holder_bit(holder_node)) == 0) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cluster_pcm_lock_upgrade: node %d is not an S holder", holder_node)));
	}
	if ((pg_atomic_read_u32(&entry->s_holders_bitmap) & ~pcm_holder_bit(holder_node)) != 0) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						errmsg("cluster_pcm_lock_upgrade: other S holders still present")));
	}

	cluster_pcm_transition_apply(entry, PCM_TRANS_S_TO_X_UPGRADE, holder_node);

	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
}


void
cluster_pcm_lock_downgrade(BufferTag tag, PcmLockMode target_mode, bool keep_pi)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	PcmState cur;
	PcmLockTransition trans;
	int holder_node;

	CLUSTER_INJECTION_POINT("cluster-pcm-downgrade-pre");

	if (cluster_pcm_htab == NULL)
		PCM_STUB_DISABLED_PATH;

	if (!((target_mode == PCM_LOCK_MODE_S && keep_pi) || (target_mode == PCM_LOCK_MODE_N && keep_pi)
		  || (target_mode == PCM_LOCK_MODE_N && !keep_pi)))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_pcm_lock_downgrade: illegal target_mode=%d keep_pi=%d",
							   (int)target_mode, keep_pi)));

	/* PGRAC: spec-2.32 D5 — HC78 downgrade symmetric wire when master remote. */
	{
		int master_node = cluster_gcs_lookup_master(tag);

		if (master_node != cluster_node_id) {
			PcmLockTransition remote_trans;

			if (target_mode == PCM_LOCK_MODE_S && keep_pi)
				remote_trans = PCM_TRANS_X_TO_S_DOWNGRADE;
			else if (target_mode == PCM_LOCK_MODE_N && keep_pi)
				remote_trans = PCM_TRANS_X_TO_N_DOWNGRADE;
			else
				remote_trans = PCM_TRANS_X_TO_N_RELEASE;

			cluster_gcs_send_transition_and_wait(tag, remote_trans, master_node);
			return;
		}
	}

	holder_node = cluster_node_id;
	if (holder_node < 0 || holder_node >= 32)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_pcm_lock_downgrade: cluster_node_id=%d out of [0, 32) range",
						holder_node)));

	if (!pcm_entry_ref_acquire(
			&tag, false, &entry_ref, &acquire_result))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_pcm_lock_downgrade: no PCM entry for BufferTag (must "
							   "acquire X first)")));
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);

	cur = (PcmState)pg_atomic_read_u32(&entry->master_state);
	if (cur != PCM_STATE_X) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_pcm_lock_downgrade: state=%d (must be X to downgrade)",
							   (int)cur)));
	}
	if (entry->x_holder_node != holder_node) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cluster_pcm_lock_downgrade: node %d cannot downgrade X held by node %d",
						holder_node, entry->x_holder_node)));
	}

	/*
	 * Downgrade transitions:
	 *  X→S with PI    → trans 4 (PCM_TRANS_X_TO_S_DOWNGRADE)
	 *  X→N with PI    → trans 5 (PCM_TRANS_X_TO_N_DOWNGRADE)
	 *  X→N without PI → trans 6 (PCM_TRANS_X_TO_N_RELEASE)
	 *  X→S without PI is illegal (downgrade always leaves PI per AD-002)
	 */
	if (target_mode == PCM_LOCK_MODE_S && keep_pi)
		trans = PCM_TRANS_X_TO_S_DOWNGRADE;
	else if (target_mode == PCM_LOCK_MODE_N && keep_pi)
		trans = PCM_TRANS_X_TO_N_DOWNGRADE;
	else if (target_mode == PCM_LOCK_MODE_N && !keep_pi)
		trans = PCM_TRANS_X_TO_N_RELEASE;
	else
		pg_unreachable();

	if (!cluster_pcm_transition_legal(cur, (PcmState)target_mode, trans)) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_pcm_lock_downgrade: HC56 validator rejected transition")));
	}

	cluster_pcm_transition_apply(entry, trans, holder_node);

	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
}


/* ============================================================
 * Diagnostic / introspection helpers (always-callable).
 * ============================================================ */

PcmLockMode
cluster_pcm_lock_query(BufferTag tag)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	PcmState state;

	/*
	 * spec-2.30 D7 — real HTAB lookup.  No PCM lock + no entry → N.
	 *
	 *	disable-path (cluster_pcm_htab == NULL):  also returns N (consistent
	 *	with spec-1.7 stub behavior so callers expecting query to never
	 *	throw under disabled config still see N).
	 *
	 *	Lock-free read:  master_state is atomic uint32;  read without
	 *	entry_lock is safe (HC57 mutation always within entry_lock + atomic
	 *	store, so reader sees consistent value).
	 */
	if (cluster_pcm_htab == NULL)
		return PCM_LOCK_MODE_N;

	if (!pcm_entry_ref_acquire(
			&tag, false, &entry_ref, &acquire_result))
		return PCM_LOCK_MODE_N;
	entry = entry_ref.entry;

	state = (PcmState)pg_atomic_read_u32(&entry->master_state);
	pcm_entry_ref_release(&entry_ref);
	return (PcmLockMode)state;
}


int
cluster_pcm_grd_count(void)
{
	int count;

	/*
	 * spec-2.30 D7 — actual entry count from HTAB.
	 *
	 *	hash_get_num_entries returns the current number of entries in the
	 *	HTAB.  disable-path returns 0 (htab is NULL).
	 */
	if (cluster_pcm_htab == NULL)
		return 0;
	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	count = (int)hash_get_num_entries(cluster_pcm_htab);
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	return count;
}


int
cluster_pcm_grd_capacity(void)
{
	/* Read-only D1 capacity context.  This is the post-HC62 capacity that
	 * actually sized the HTAB, never the unresolved -1 GUC sentinel. */
	return pcm_grd_effective;
}


void
cluster_pcm_grd_lifecycle_stats_snapshot(PcmGrdLifecycleStats *out)
{
	uint64 next_binding_generation;
	int refusal;

	if (out == NULL)
		return;
	memset(out, 0, sizeof(*out));
	if (ClusterPcm == NULL || cluster_pcm_htab == NULL)
		return;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	out->live_entries = pg_atomic_read_u64(
		&ClusterPcm->resource_x_reconfig_slot_count);
	out->tombstone_slots = pg_atomic_read_u64(
		&ClusterPcm->reclaim_tombstone_count);
	next_binding_generation = pg_atomic_read_u64(
		&ClusterPcm->next_binding_generation);
	if (next_binding_generation > 1)
		out->binding_generation = next_binding_generation == UINT64_MAX
			? UINT64_MAX - 1 : next_binding_generation - 1;
	out->reclaim_attempt_count = pg_atomic_read_u64(
		&ClusterPcm->reclaim_attempt_count);
	out->reclaim_success_count = pg_atomic_read_u64(
		&ClusterPcm->reclaim_success_count);
	out->reclaim_reuse_count = pg_atomic_read_u64(
		&ClusterPcm->reclaim_reuse_count);
	out->capacity_retry_count = pg_atomic_read_u64(
		&ClusterPcm->reclaim_capacity_retry_count);
	out->capacity_fail_count = pg_atomic_read_u64(
		&ClusterPcm->reclaim_capacity_fail_count);
	out->peak_live_entries = pg_atomic_read_u64(
		&ClusterPcm->reclaim_peak_live_entries);
	for (refusal = 0; refusal < PCM_RETIRE_REFUSAL_N; refusal++)
		out->reclaim_refused[refusal] = pg_atomic_read_u64(
			&ClusterPcm->reclaim_refused[refusal]);
	LWLockRelease(&ClusterPcm->htab_lock.lock);
}


void
cluster_pcm_grd_protocol_debt_snapshot(PcmGrdProtocolDebtStats *out)
{
	HASH_SEQ_STATUS status;
	struct GrdEntry *entry;

	if (out == NULL)
		return;
	memset(out, 0, sizeof(*out));
	if (ClusterPcm == NULL || cluster_pcm_htab == NULL)
		return;

	/* Retirement/removal takes the directory lock EXCLUSIVE, so one SHARED
	 * scan keeps both the entry pointer and its registry sidecar stable. */
	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	hash_seq_init(&status, cluster_pcm_htab);
	while ((entry = (struct GrdEntry *)hash_seq_search(&status)) != NULL) {
		ClusterPcmResourceXMasterState *state = NULL;
		ClusterPcmResourceXBootstrapRound *round;
		ClusterPcmResourceXLocalOwner *owner;
		ResourceXDecodedFrame source_settlement;
		ResourceXDecodedFrame join_grant;
		ResourceXDecodedFrame join_image;
		bool active = false;
		bool drained_pair_history = false;
		bool retained = false;
		bool invalid = false;
		uint32 lifecycle;
		int node;

		LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
		out->wait_refcount += pg_atomic_read_u32(&entry->wait_refcount);
		out->transport_refcount += pg_atomic_read_u32(
			&entry->transport_refcount);
		lifecycle = pg_atomic_read_u32(&entry->lifecycle);
		if (lifecycle != PCM_ENTRY_LIVE && lifecycle != PCM_ENTRY_QUIESCING)
			invalid = true;

		owner = &entry->resource_x_local_owner;
		if (!pcm_resource_x_local_owner_valid_locked(owner))
			invalid = true;
		if (owner->state != RESOURCE_X_LOCAL_OWNER_EMPTY) {
			out->local_owner_entry_count++;
			if (owner->state == RESOURCE_X_LOCAL_OWNER_EVICTING)
				out->evicting_entry_count++;
		}

		if (!pcm_resource_x_active_empty_locked(entry)) {
			active = true;
			if (!pcm_resource_x_active_valid_locked(entry))
				invalid = true;
		}
		round = &entry->resource_x_bootstrap_round;
		if (round->phase > RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED)
			invalid = true;
		else if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_EMPTY
				 && round->phase
					!= RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED)
			active = true;

		if (entry->registry_slot >= (uint32)pcm_grd_effective
			|| cluster_pcm_resource_x_slots == NULL
			|| cluster_pcm_resource_x_master_states == NULL
			|| cluster_pcm_resource_x_slots[entry->registry_slot].state
				!= PCM_REGISTRY_LIVE
			|| !BufferTagsEqual(
				&cluster_pcm_resource_x_slots[entry->registry_slot].tag,
				&entry->tag)
			|| cluster_pcm_resource_x_slots[entry->registry_slot]
				.binding_generation != entry->binding_generation) {
			invalid = true;
		}
		else {
			state = &cluster_pcm_resource_x_master_states[
				entry->registry_slot];
			if (state->binding_generation != entry->binding_generation)
				invalid = true;
		}

		if (state != NULL) {
			drained_pair_history
				= pcm_resource_x_holder_pair_drained_history_locked(
					entry, state, NULL, NULL);
			if ((!drained_pair_history
					&& (state->holder_status.valid != 0
						|| state->holder_image.valid != 0
						|| state->holder_status_intent.slot.state
							!= RESOURCE_X_INTENT_SLOT_EMPTY
						|| state->holder_image_intent.state
							!= RESOURCE_X_INTENT_SLOT_EMPTY))
				|| state->grant_intent.slot.state
					!= RESOURCE_X_INTENT_SLOT_EMPTY
				|| state->requester_settlement_intent.slot.state
					!= RESOURCE_X_INTENT_SLOT_EMPTY)
				retained = true;
			if ((state->holder_status.valid > RESOURCE_X_HOLDER_PAIR_PUBLISHED)
				|| (state->holder_image.valid
					> RESOURCE_X_HOLDER_PAIR_PUBLISHED)
				|| state->holder_status_intent.slot.state
					> RESOURCE_X_INTENT_SLOT_STAGED
				|| state->holder_image_intent.state
					> RESOURCE_X_INTENT_SLOT_STAGED
				|| state->grant_intent.slot.state
					> RESOURCE_X_INTENT_SLOT_STAGED
				|| state->requester_settlement_intent.slot.state
					> RESOURCE_X_INTENT_SLOT_STAGED)
				invalid = true;

			if (!pcm_resource_x_requester_join_empty_locked(
					&state->requester_join)) {
				retained = true;
				if (!pcm_resource_x_requester_join_decode_locked(
						&state->requester_join, &join_grant, &join_image))
					invalid = true;
			}
			if (!pcm_resource_x_source_settlement_debt_decode_locked(
					entry, state, &source_settlement))
				invalid = true;
			else if (state->source_settlement.state
					== RESOURCE_X_SOURCE_SETTLEMENT_PENDING)
				retained = true;

			if (!pcm_resource_x_bootstrap_priority_valid(
					&state->bootstrap_priority))
				invalid = true;
			else if (state->bootstrap_priority.state
					!= RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY)
				retained = true;

			for (node = 0; node < RESOURCE_X_PROTOCOL_NODE_LIMIT; node++) {
				const ClusterPcmResourceXBootstrapReceipt *receipt
					= &state->bootstrap_receipts[node];
				const ClusterPcmResourceXMasterRequest *request
					= &state->requests[node];
				const ResourceXIntentSlot *block_intent
					= &state->block_intents[node].slot;

				if (!pcm_resource_x_bootstrap_receipt_valid(receipt))
					invalid = true;
				else if (receipt->state
						!= RESOURCE_X_BOOTSTRAP_RECEIPT_EMPTY)
					retained = true;
				if (block_intent->state != RESOURCE_X_INTENT_SLOT_EMPTY)
					retained = true;
				if (block_intent->state > RESOURCE_X_INTENT_SLOT_STAGED)
					invalid = true;

				if (request->phase > RESOURCE_X_MASTER_RELEASED)
					invalid = true;
				else if (request->phase != RESOURCE_X_MASTER_NONE
						 && request->phase != RESOURCE_X_MASTER_SETTLED
						 && request->phase != RESOURCE_X_MASTER_RELEASED)
					active = true;
			}

			/* Once all current debt categories are empty, consume the one
			 * complete terminal validity table.  This is what distinguishes
			 * exact stable residency from malformed terminal residue. */
			if (!active && !retained
				&& owner->state == RESOURCE_X_LOCAL_OWNER_EMPTY) {
				uint64 terminal_count = 0;
				uint64 digest = PGRAC_RESOURCE_X_PROOF_DIGEST_OFFSET;

				if (!pcm_resource_x_terminal_state_locked(
						entry, state, true, true,
						&terminal_count, &digest))
					invalid = true;
			}
		}

		if (retained) {
			out->retained_entry_count++;
		}
		if (active)
			out->active_resource_x_entry_count++;
		if (invalid)
			out->invalid_entry_count++;
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
}


void
cluster_pcm_grd_get_summary(int *n_count, int *s_count, int *x_count, int *pi_holders_total,
							int *convert_queue_active)
{
	HASH_SEQ_STATUS status;
	struct GrdEntry *entry;

	*n_count = 0;
	*s_count = 0;
	*x_count = 0;
	*pi_holders_total = 0;
	*convert_queue_active = 0;

	if (ClusterPcm == NULL || cluster_pcm_htab == NULL)
		return;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	hash_seq_init(&status, cluster_pcm_htab);
	while ((entry = (struct GrdEntry *)hash_seq_search(&status)) != NULL) {
		uint32 pi_bitmap = pg_atomic_read_u32(&entry->pi_holders_bitmap);
		PcmState state = (PcmState)pg_atomic_read_u32(&entry->master_state);

		switch (state) {
		case PCM_STATE_N:
			(*n_count)++;
			break;
		case PCM_STATE_S:
			(*s_count)++;
			break;
		case PCM_STATE_X:
			(*x_count)++;
			break;
		default:
			break;
		}
		while (pi_bitmap != 0) {
			*pi_holders_total += (int)(pi_bitmap & 1U);
			pi_bitmap >>= 1;
		}
		if (entry->convert_queue != NULL)
			(*convert_queue_active)++;
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
}


Size
cluster_pcm_grd_shmem_size(void)
{
	int eff;
	Size sz;

	/* shmem_size path: fatal_on_misconfig=false (HC62 FATALs from init_fn). */
	eff = pcm_grd_effective_entries(false);
	if (eff == 0)
		return 0;
	/*
	 * PGRAC: spec-2.30 D2 — header(ClusterPcmShared 72B aligned)+ HTAB
	 * estimated size.  hash_estimate_size returns size for eff slots
	 * given sizeof(struct GrdEntry) entry payload.
	 */
	sz = MAXALIGN(sizeof(ClusterPcmShared));
	sz = add_size(sz, mul_size((Size)eff, sizeof(ClusterPcmResourceXSlot)));
	sz = add_size(sz, mul_size((Size)eff,
							 sizeof(ClusterPcmResourceXMasterState)));
	sz = add_size(sz, hash_estimate_size((Size)eff, sizeof(struct GrdEntry)));
	return sz;
}


void
cluster_pcm_grd_init(void)
{
	bool found;
	HASHCTL info;
	Size header_size;
	int refusal;

	/*
	 * spec-2.30 D5 + HC62 — resolve effective entry count;  fatal_on_misconfig
	 * raises FATAL on invalid configs (NBuffers=0 / NBuffers>cap / GUC<NBuffers).
	 * Explicit `cluster.pcm_grd_max_entries=0` is the disable path:  ClusterPcm
	 * + cluster_pcm_htab stay NULL → 9 counter accessors return 0;  mutation
	 * API preserves spec-1.7 stub behavior (ereport ERRCODE_FEATURE_NOT_SUPPORTED).
	 */
	pcm_grd_effective = pcm_grd_effective_entries(true);
	if (pcm_grd_effective == 0)
		return;
	header_size = add_size(MAXALIGN(sizeof(ClusterPcmShared)),
						   mul_size((Size)pcm_grd_effective,
									sizeof(ClusterPcmResourceXSlot)));
	header_size = add_size(header_size,
						   mul_size((Size)pcm_grd_effective,
									sizeof(ClusterPcmResourceXMasterState)));

	pgstat_report_wait_start(WAIT_EVENT_PCM_GRD_INIT);
	ClusterPcm = (ClusterPcmShared *)ShmemInitStruct("pgrac cluster pcm grd hdr",
													 header_size, &found);
	cluster_pcm_resource_x_slots = (ClusterPcmResourceXSlot *)
		((char *)ClusterPcm + MAXALIGN(sizeof(ClusterPcmShared)));
	cluster_pcm_resource_x_master_states = (ClusterPcmResourceXMasterState *)
		((char *)cluster_pcm_resource_x_slots
		 + mul_size((Size)pcm_grd_effective,
					 sizeof(ClusterPcmResourceXSlot)));

	if (!found) {
		/*
		 * PGRAC: spec-2.30 D2 — header init (9 atomic uint64 counters
		 * zeroed).  Trans-9 (s_to_x_cleanout) counter starts 0 and stays 0
		 * by HC60 apply-fail-closed.
		 */
		memset(ClusterPcm, 0, header_size);
		for (int site = 0; site < CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT; site++)
			for (int metric = 0; metric < CLUSTER_UNDO_BLOCK0_WAIT_METRIC_COUNT; metric++)
				pg_atomic_init_u64(&ClusterPcm->block0_reply_wait_count[site][metric], 0);
		LWLockInitialize(&ClusterPcm->htab_lock.lock, LWTRANCHE_CLUSTER_PCM);
		pg_atomic_init_u64(&ClusterPcm->next_binding_generation, 1);
		pg_atomic_init_u64(
			&ClusterPcm->resource_x_retired_authority_floor, 0);
		pg_atomic_init_u64(
			&ClusterPcm->resource_x_bootstrap_next_attempt, 1);
		pg_atomic_init_u32(&ClusterPcm->reclaim_requested, 0);
		pg_atomic_init_u64(&ClusterPcm->reclaim_scan_cursor, 0);
		pg_atomic_init_u64(&ClusterPcm->reclaim_attempt_count, 0);
		pg_atomic_init_u64(&ClusterPcm->reclaim_success_count, 0);
		pg_atomic_init_u64(&ClusterPcm->reclaim_reuse_count, 0);
		pg_atomic_init_u64(&ClusterPcm->reclaim_capacity_retry_count, 0);
		pg_atomic_init_u64(&ClusterPcm->reclaim_capacity_fail_count, 0);
		pg_atomic_init_u64(&ClusterPcm->reclaim_peak_live_entries, 0);
		pg_atomic_init_u64(&ClusterPcm->reclaim_tombstone_count, 0);
		for (refusal = 0; refusal < PCM_RETIRE_REFUSAL_N; refusal++)
			pg_atomic_init_u64(
				&ClusterPcm->reclaim_refused[refusal], 0);
		pg_atomic_init_u32(&ClusterPcm->resource_x_gate_phase, RESOURCE_X_GATE_OPEN);
		pg_atomic_init_u64(&ClusterPcm->resource_x_gate_formation, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_freeze_generation, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_activation_inflight_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_old_formation, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_new_formation, 0);
		pg_atomic_init_u32(&ClusterPcm->resource_x_reconfig_dead_requester_bitmap, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_next_state_index, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_scan_capacity, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_zero_proof_generation, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_residual_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_semantic_mutation_sequence, 1);
		pg_atomic_init_u32(&ClusterPcm->resource_x_reconfig_proof_pass, 0);
		pg_atomic_init_u64(
			&ClusterPcm->resource_x_reconfig_proof_begin_sequence, 0);
		pg_atomic_init_u64(
			&ClusterPcm->resource_x_reconfig_proof_begin_slot_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_proof_digest, 0);
		pg_atomic_init_u64(
			&ClusterPcm->resource_x_reconfig_proof_empty_slot_count, 0);
		pg_atomic_init_u64(
			&ClusterPcm->resource_x_reconfig_proof_successor_slot_count, 0);
		pg_atomic_init_u64(
			&ClusterPcm->resource_x_reconfig_proof_terminal_slot_count, 0);
		pg_atomic_init_u64(
			&ClusterPcm->resource_x_reconfig_proof_retry_revisit_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_freeze_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_slot_examined_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_old_detached_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_successor_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_orphan_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_sidecar_neutralized_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_sidecar_stale_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_retry_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_blocked_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_thaw_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_reclaim_nonhead_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_reclaim_head_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_reclaim_orphan_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_reconfig_slot_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_intent_arm_generation, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_intent_scan_generation, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_intent_completed_generation, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_intent_next_state_index, 0);
		pg_atomic_init_u32(&ClusterPcm->resource_x_intent_next_owner_index, 0);
		pg_atomic_init_u32(&ClusterPcm->resource_x_intent_generation_exhausted, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_o1_remote_install_observed_count, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_o1_remote_grant_after_image_count, 0);
		pg_atomic_init_u64(
			&ClusterPcm->resource_x_o1_remote_image_at_or_after_grant_count, 0);
		pg_atomic_init_u64(
			&ClusterPcm->resource_x_o1_remote_episode_excluded_no_install, 0);
		pg_atomic_init_u64(
			&ClusterPcm->resource_x_o1_remote_episode_excluded_missing_grant, 0);
		pg_atomic_init_u64(
			&ClusterPcm->resource_x_o1_remote_episode_excluded_missing_image, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_o1_last_remote_t_image_us, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_o1_last_remote_t_grant_us, 0);
		pg_atomic_init_u64(&ClusterPcm->resource_x_o1_last_remote_t_install_us, 0);
		pg_atomic_init_u64(&ClusterPcm->trans_n_to_s_count, 0);
		pg_atomic_init_u64(&ClusterPcm->trans_n_to_x_count, 0);
		pg_atomic_init_u64(&ClusterPcm->trans_s_to_x_upgrade_count, 0);
		pg_atomic_init_u64(&ClusterPcm->trans_x_to_s_downgrade_count, 0);
		pg_atomic_init_u64(&ClusterPcm->trans_x_to_n_downgrade_count, 0);
		pg_atomic_init_u64(&ClusterPcm->trans_x_to_n_release_count, 0);
		pg_atomic_init_u64(&ClusterPcm->trans_s_to_n_invalidate_count, 0);
		pg_atomic_init_u64(&ClusterPcm->trans_s_to_n_release_count, 0);
		pg_atomic_init_u64(&ClusterPcm->trans_s_to_x_cleanout_count, 0);
		pg_atomic_init_u64(&ClusterPcm->local_s_revoke_nonholder_failclosed_count, 0);
		pg_atomic_init_u64(&ClusterPcm->evict_release_deferred_aux_count, 0);
		pg_atomic_init_u64(&ClusterPcm->writer_cover_stale_detected_count, 0);
		pg_atomic_init_u64(&ClusterPcm->writer_reverify_reacquire_count, 0);
		pg_atomic_init_u64(&ClusterPcm->restore_aba_detected_count, 0);
		pg_atomic_init_u64(&ClusterPcm->invalidate_parked_grant_pending_count, 0);
		/* S3 forensics step 1b — per-tag advance-provenance table (slots
		 * zeroed by the memset above; used=false marks free slots). */
		LWLockInitialize(&ClusterPcm->wm_prov_lock.lock, LWTRANCHE_CLUSTER_PCM);
		pg_atomic_init_u64(&ClusterPcm->wm_prov_insert_fail_count, 0);
	}

	/*
	 * PGRAC: spec-2.30 D2 — HTAB keyed by BufferTag (20B);  HASH_BLOBS
	 * with memcmp/hash_bytes_extended.  HC59 lazy alloc:  entries inserted
	 * on first cluster_pcm_lock_acquire(tag, mode) via HASH_ENTER_NULL +
	 * never freed until cluster shutdown.  Cap = max_entries (FULL → fail-
	 * closed ereport ERRCODE_OUT_OF_MEMORY at caller).
	 */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(BufferTag);
	info.entrysize = sizeof(struct GrdEntry);
	cluster_pcm_htab = ShmemInitHash("pgrac cluster pcm grd htab", (long)pcm_grd_effective,
									 (long)pcm_grd_effective, &info, HASH_ELEM | HASH_BLOBS);
	pgstat_report_wait_end();
}


/*
 * PGRAC: spec-2.30 D2 — HC59 lazy-alloc entry helper (file-private).
 *
 *	Looks up entry by BufferTag;  on miss, inserts new entry with all
 *	fields fresh (HC59 alloc on first acquire + LWLockInitialize entry_lock
 *	+ master_state = PCM_STATE_N + x_holder_node = -1 + bitmaps zeroed).
 *	Returns NULL when HTAB is at cap (HC59 fail-closed cap).
 */
static bool
pcm_entry_binding_generation_next(uint64 *generation_out)
{
	uint64 current;

	if (generation_out != NULL)
		*generation_out = 0;
	if (ClusterPcm == NULL || generation_out == NULL)
		return false;
	current = pg_atomic_read_u64(&ClusterPcm->next_binding_generation);
	for (;;) {
		if (current == 0 || current == UINT64_MAX)
			return false;
		if (pg_atomic_compare_exchange_u64(
				&ClusterPcm->next_binding_generation, &current, current + 1)) {
			*generation_out = current;
			return true;
		}
	}
}

/* A node only produces requester bootstrap rounds for its own requester_node.
 * Use one node-local sequence across resources so physical GRD retirement can
 * never reset the frozen {resource, requester_node} attempt namespace.  The
 * per-entry floor remains an exact lower bound for an already-live resource;
 * the shared allocator is a stronger monotonic namespace, not authority. */
static bool
pcm_resource_x_bootstrap_attempt_allocate(
	uint64 attempt_floor, uint64 *attempt_out)
{
	uint64 candidate;
	uint64 current;

	if (attempt_out != NULL)
		*attempt_out = 0;
	if (ClusterPcm == NULL || attempt_out == NULL
		|| attempt_floor == UINT64_MAX)
		return false;
	current = pg_atomic_read_u64(
		&ClusterPcm->resource_x_bootstrap_next_attempt);
	for (;;) {
		if (current == 0 || current == UINT64_MAX)
			return false;
		candidate = current;
		if (candidate <= attempt_floor)
			candidate = attempt_floor + 1;
		if (candidate == UINT64_MAX)
			return false;
		if (pg_atomic_compare_exchange_u64(
				&ClusterPcm->resource_x_bootstrap_next_attempt,
				&current, candidate + 1)) {
			*attempt_out = candidate;
			return true;
		}
	}
}

static bool
pcm_entry_pin_under_htab_lock(struct GrdEntry *entry,
	PcmEntryAcquireResult *result)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXSlot *slot;
	uint32 lifecycle;
	uint32 pins;

	Assert(entry != NULL);
	Assert(result != NULL);
	Assert(LWLockHeldByMeInMode(&ClusterPcm->htab_lock.lock, LW_SHARED)
		   || LWLockHeldByMeInMode(&ClusterPcm->htab_lock.lock,
								 LW_EXCLUSIVE));
	if (entry->registry_slot >= (uint32)pcm_grd_effective
		|| cluster_pcm_resource_x_slots == NULL
		|| cluster_pcm_resource_x_master_states == NULL) {
		pcm_resource_x_reconfig_block();
		*result = PCM_ENTRY_ACQUIRE_CORRUPT;
		return false;
	}
	slot = &cluster_pcm_resource_x_slots[entry->registry_slot];
	state = &cluster_pcm_resource_x_master_states[entry->registry_slot];
	if (slot->state != PCM_REGISTRY_LIVE || slot->reserved != 0
		|| !BufferTagsEqual(&slot->tag, &entry->tag)
		|| slot->binding_generation != entry->binding_generation
		|| slot->retired_authority_generation != 0
		|| state->binding_generation != entry->binding_generation) {
		pcm_resource_x_reconfig_block();
		*result = PCM_ENTRY_ACQUIRE_CORRUPT;
		return false;
	}
	lifecycle = pg_atomic_read_u32(&entry->lifecycle);
	if (lifecycle == PCM_ENTRY_RETIRING) {
		*result = PCM_ENTRY_ACQUIRE_RETRY_CURRENT;
		return false;
	}
	if (lifecycle == PCM_ENTRY_QUIESCING) {
		if (!pg_atomic_compare_exchange_u32(
				&entry->lifecycle, &lifecycle, PCM_ENTRY_LIVE)) {
			if (lifecycle == PCM_ENTRY_RETIRING) {
				*result = PCM_ENTRY_ACQUIRE_RETRY_CURRENT;
				return false;
			}
			if (lifecycle != PCM_ENTRY_LIVE) {
				pcm_resource_x_reconfig_block();
				*result = PCM_ENTRY_ACQUIRE_CORRUPT;
				return false;
			}
		}
	} else if (lifecycle != PCM_ENTRY_LIVE) {
		pcm_resource_x_reconfig_block();
		*result = PCM_ENTRY_ACQUIRE_CORRUPT;
		return false;
	}

	pins = pg_atomic_read_u32(&entry->pin_count);
	for (;;) {
		if (pins == UINT32_MAX) {
			pcm_resource_x_reconfig_block();
			*result = PCM_ENTRY_ACQUIRE_CORRUPT;
			return false;
		}
		if (pg_atomic_compare_exchange_u32(
				&entry->pin_count, &pins, pins + 1))
			break;
	}
	*result = PCM_ENTRY_ACQUIRE_OK;
	return true;
}

static struct GrdEntry *
pcm_get_or_create_entry_under_htab_lock(BufferTag tag,
	PcmEntryAcquireResult *result)
{
	ClusterPcmResourceXMasterState *master_state;
	ClusterPcmResourceXSlot *registry_slot;
	struct GrdEntry *entry;
	bool found;
	uint64 binding_generation;
	uint64 first_tombstone = UINT64_MAX;
	uint64 initial_authority_generation;
	uint64 live_slots;
	uint64 retired_authority_floor;
	uint64 retired_authority_generation;
	uint64 slot_index = 0;
	uint64 slot_probe;
	uint64 slot_seed;
	bool slot_found = false;
	bool slot_reused = false;

	Assert(result != NULL);
	Assert(LWLockHeldByMeInMode(&ClusterPcm->htab_lock.lock, LW_EXCLUSIVE));
	*result = PCM_ENTRY_ACQUIRE_CORRUPT;
	entry = (struct GrdEntry *)hash_search(
		cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		*result = PCM_ENTRY_ACQUIRE_OK;
		return entry;
	}
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_OPEN) {
		*result = PCM_ENTRY_ACQUIRE_RETRY_CURRENT;
		return NULL;
	}

	/* Open addressing must preserve collision chains across physical retire.
	 * Prefer the first tombstone encountered, but only after probing to EMPTY
	 * (or completing a full wrap) so an existing LIVE binding is never hidden. */
	slot_seed = (uint64)tag.spcOid;
	slot_seed = slot_seed * UINT64_C(1315423911) + (uint64)tag.dbOid;
	slot_seed = slot_seed * UINT64_C(1315423911) + (uint64)tag.relNumber;
	slot_seed = slot_seed * UINT64_C(1315423911) + (uint64)(uint32)tag.forkNum;
	slot_seed = slot_seed * UINT64_C(1315423911) + (uint64)tag.blockNum;
	for (slot_probe = 0; slot_probe < (uint64)pcm_grd_effective; slot_probe++) {
		slot_index = (slot_seed + slot_probe) % (uint64)pcm_grd_effective;
		registry_slot = &cluster_pcm_resource_x_slots[slot_index];
		if (registry_slot->state == PCM_REGISTRY_TOMBSTONE) {
			if (first_tombstone == UINT64_MAX)
				first_tombstone = slot_index;
			continue;
		}
		if (registry_slot->state == PCM_REGISTRY_EMPTY) {
			if (first_tombstone != UINT64_MAX)
				slot_index = first_tombstone;
			slot_found = true;
			break;
		}
		if (registry_slot->state != PCM_REGISTRY_LIVE
			|| registry_slot->reserved != 0
			|| registry_slot->binding_generation == 0
			|| registry_slot->binding_generation == UINT64_MAX
			|| registry_slot->retired_authority_generation != 0
			|| BufferTagsEqual(&registry_slot->tag, &tag)) {
			pcm_resource_x_reconfig_block();
			*result = PCM_ENTRY_ACQUIRE_CORRUPT;
			return NULL;
		}
	}
	if (!slot_found && first_tombstone != UINT64_MAX) {
		slot_index = first_tombstone;
		slot_found = true;
	}
	if (!slot_found) {
		*result = PCM_ENTRY_ACQUIRE_NO_CAPACITY;
		return NULL;
	}
	registry_slot = &cluster_pcm_resource_x_slots[slot_index];
	slot_reused = registry_slot->state == PCM_REGISTRY_TOMBSTONE;
	retired_authority_generation
		= slot_reused ? registry_slot->retired_authority_generation : 0;
	retired_authority_floor = pg_atomic_read_u64(
		&ClusterPcm->resource_x_retired_authority_floor);
	if (slot_reused
		&& pg_atomic_read_u64(&ClusterPcm->reclaim_tombstone_count) == 0) {
		pcm_resource_x_reconfig_block();
		*result = PCM_ENTRY_ACQUIRE_CORRUPT;
		return NULL;
	}
	if (!pcm_entry_binding_generation_next(&binding_generation)) {
		pcm_resource_x_reconfig_block();
		*result = PCM_ENTRY_ACQUIRE_CORRUPT;
		return NULL;
	}
	if (retired_authority_floor >= UINT64_MAX - 1) {
		pcm_resource_x_reconfig_block();
		*result = PCM_ENTRY_ACQUIRE_CORRUPT;
		return NULL;
	}
	initial_authority_generation = retired_authority_floor == 0
		? 1 : retired_authority_floor + 1;
	if (slot_reused) {
		if (retired_authority_generation == 0
			|| retired_authority_generation >= UINT64_MAX - 1) {
			pcm_resource_x_reconfig_block();
			*result = PCM_ENTRY_ACQUIRE_CORRUPT;
			return NULL;
		}
		if (initial_authority_generation
			< retired_authority_generation + 1)
			initial_authority_generation
				= retired_authority_generation + 1;
		if (initial_authority_generation < binding_generation)
			initial_authority_generation = binding_generation;
	}
	if (initial_authority_generation == 0
		|| initial_authority_generation == UINT64_MAX
		|| !pcm_entry_lifecycle_mutation_mark()) {
		pcm_resource_x_reconfig_block();
		*result = PCM_ENTRY_ACQUIRE_CORRUPT;
		return NULL;
	}
	entry = (struct GrdEntry *)hash_search(
		cluster_pcm_htab, &tag, HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		*result = PCM_ENTRY_ACQUIRE_NO_CAPACITY;
		return NULL;
	}
	if (!found) {
		BufferTag saved_tag = entry->tag;

		memset(entry, 0, sizeof(*entry));
		entry->tag = saved_tag;
		pg_atomic_init_u32(&entry->master_state, (uint32)PCM_STATE_N);
		entry->x_holder_node = -1;
		entry->master_holder.node_id = INVALID_PCM_MASTER_HOLDER_NODE;
		pg_atomic_init_u32(&entry->s_holders_bitmap, 0);
		pg_atomic_init_u32(&entry->pi_holders_bitmap, 0);
		/* A reused registry binding starts a new N/no-holder lifetime above
		 * the retired authority floor.  Seed the physical transition axis at
		 * the matching pre-transition baseline so its first N->X remains the
		 * direct predecessor of the new Resource-X authority generation.  A
		 * first-generation binding still starts at zero. */
		pg_atomic_init_u64(&entry->transition_count_local,
			initial_authority_generation - 1);
		entry->s_holder_refcount_local = 0;
		entry->pending_x_requester_node = -1;
		entry->pending_x_since_lsn = 0;
		entry->pi_watermark_lsn = InvalidXLogRecPtr;
		entry->pi_watermark_scn = InvalidScn;
		entry->resource_x_requester_node = -1;
		entry->resource_x_progress_flags = 0;
		entry->resource_x_formation = 0;
		entry->resource_x_acquisition_generation = 0;
		entry->resource_x_requester_base_generation
			= initial_authority_generation;
		entry->resource_x_retired_acquisition_generation = 0;
		entry->resource_x_no_progress_generation = 0;
		entry->resource_x_no_progress_reason = RESOURCE_X_NO_PROGRESS_NONE;
		entry->resource_x_dispatch_phase = 0;
		pg_atomic_init_u32(&entry->lifecycle, PCM_ENTRY_LIVE);
		pg_atomic_init_u32(&entry->pin_count, 0);
		pg_atomic_init_u32(&entry->wait_refcount, 0);
		pg_atomic_init_u32(&entry->transport_refcount, 0);
		entry->binding_generation = binding_generation;
		entry->registry_slot = (uint32)slot_index;
		master_state = &cluster_pcm_resource_x_master_states[slot_index];
		memset(master_state, 0, sizeof(*master_state));
		master_state->binding_generation = binding_generation;
		master_state->authority_generation = initial_authority_generation;
		master_state->next_enqueue_order = 1;
		registry_slot = &cluster_pcm_resource_x_slots[slot_index];
		registry_slot->tag = saved_tag;
		registry_slot->reserved = 0;
		registry_slot->binding_generation = binding_generation;
		registry_slot->retired_authority_generation = 0;
		if (slot_reused)
			pg_atomic_fetch_sub_u64(
				&ClusterPcm->reclaim_tombstone_count, 1);
		if (slot_reused)
			pg_atomic_fetch_add_u64(
				&ClusterPcm->reclaim_reuse_count, 1);
		pg_write_barrier();
		registry_slot->state = PCM_REGISTRY_LIVE;
		live_slots = pg_atomic_fetch_add_u64(
			&ClusterPcm->resource_x_reconfig_slot_count, 1) + 1;
		pcm_atomic_max_u64(
			&ClusterPcm->reclaim_peak_live_entries, live_slots);
		if (live_slots * UINT64_C(100)
			>= (uint64)pcm_grd_effective
				* (uint64)PCM_GRD_RECLAIM_SOFT_PERCENT)
			pcm_entry_reclaim_request();
		ConditionVariableInit(&entry->wait_cv);
		LWLockInitialize(&entry->entry_lock.lock, LWTRANCHE_CLUSTER_PCM);
	}
	*result = PCM_ENTRY_ACQUIRE_OK;
	return entry;
}

bool
pcm_entry_ref_acquire(const BufferTag *tag, bool create,
	PcmEntryRef *out, PcmEntryAcquireResult *result)
{
	PcmReclaimBatch reclaim_batch;
	struct GrdEntry *entry;
	bool found;
	bool capacity_retried = false;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (result != NULL)
		*result = PCM_ENTRY_ACQUIRE_CORRUPT;
	if (tag == NULL || out == NULL || result == NULL)
		return false;
	if (ClusterPcm == NULL || cluster_pcm_htab == NULL) {
		*result = PCM_ENTRY_ACQUIRE_NOT_READY;
		return false;
	}

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(
		cluster_pcm_htab, tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		if (!pcm_entry_pin_under_htab_lock(entry, result)) {
			LWLockRelease(&ClusterPcm->htab_lock.lock);
			return false;
		}
		out->entry = entry;
		out->tag = entry->tag;
		out->binding_generation = entry->binding_generation;
		out->registry_slot = entry->registry_slot;
		out->pinned = true;
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return true;
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	if (!create) {
		*result = PCM_ENTRY_ACQUIRE_NOT_FOUND;
		return false;
	}


retry_create:
	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_EXCLUSIVE);
	entry = pcm_get_or_create_entry_under_htab_lock(*tag, result);
	if (entry == NULL) {
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		if (*result == PCM_ENTRY_ACQUIRE_NO_CAPACITY
			&& !capacity_retried) {
			capacity_retried = true;
			pg_atomic_fetch_add_u64(
				&ClusterPcm->reclaim_capacity_retry_count, 1);
			(void)cluster_pcm_lock_reclaim_bounded(
				PCM_GRD_SYNC_RECLAIM_BUDGET, &reclaim_batch);
			goto retry_create;
		}
		if (*result == PCM_ENTRY_ACQUIRE_NO_CAPACITY
			&& capacity_retried)
			pg_atomic_fetch_add_u64(
				&ClusterPcm->reclaim_capacity_fail_count, 1);
		return false;
	}
	if (!pcm_entry_pin_under_htab_lock(entry, result)) {
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return false;
	}
	out->entry = entry;
	out->tag = entry->tag;
	out->binding_generation = entry->binding_generation;
	out->registry_slot = entry->registry_slot;
	out->pinned = true;
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	return true;
}

void
pcm_entry_ref_release(PcmEntryRef *ref)
{
	struct GrdEntry *entry;
	uint32 pins;
	uint32 next_pins;

	Assert(ref != NULL && ref->pinned && ref->entry != NULL);
	entry = ref->entry;
	if (!BufferTagsEqual(&entry->tag, &ref->tag)
		|| entry->binding_generation != ref->binding_generation
		|| entry->registry_slot != ref->registry_slot) {
		pcm_resource_x_reconfig_block();
		ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("PCM entry ref identity changed while pinned")));
	}
	pins = pg_atomic_read_u32(&entry->pin_count);
	for (;;) {
		if (pins == 0) {
			pcm_resource_x_reconfig_block();
			ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("PCM entry pin count underflow")));
		}
		next_pins = pins - 1;
		if (pg_atomic_compare_exchange_u32(
				&entry->pin_count, &pins, next_pins))
			break;
	}
	if (next_pins == 0
		&& pg_atomic_read_u32(&entry->lifecycle) == PCM_ENTRY_QUIESCING)
		pcm_entry_reclaim_request();
	memset(ref, 0, sizeof(*ref));
}

bool
pcm_entry_transport_ref_begin(const PcmEntryRef *entry_ref,
	PcmEntryTransportRef *transport_ref)
{
	struct GrdEntry *entry;
	uint32 refs;

	if (transport_ref != NULL)
		memset(transport_ref, 0, sizeof(*transport_ref));
	if (entry_ref == NULL || transport_ref == NULL
		|| !pcm_entry_ref_identity_exact(entry_ref))
		return false;
	entry = entry_ref->entry;
	refs = pg_atomic_read_u32(&entry->transport_refcount);
	for (;;) {
		if (refs == UINT32_MAX) {
			pcm_resource_x_reconfig_block();
			return false;
		}
		if (pg_atomic_compare_exchange_u32(
				&entry->transport_refcount, &refs, refs + 1))
			break;
	}
	transport_ref->entry = entry;
	transport_ref->tag = entry_ref->tag;
	transport_ref->binding_generation = entry_ref->binding_generation;
	transport_ref->registry_slot = entry_ref->registry_slot;
	transport_ref->held = true;
	return true;
}

void
pcm_entry_transport_ref_end(PcmEntryTransportRef *transport_ref)
{
	struct GrdEntry *entry;
	uint32 refs;

	Assert(transport_ref != NULL && transport_ref->held
		   && transport_ref->entry != NULL);
	entry = transport_ref->entry;
	if (!BufferTagsEqual(&entry->tag, &transport_ref->tag)
		|| entry->binding_generation != transport_ref->binding_generation
		|| entry->registry_slot != transport_ref->registry_slot) {
		pcm_resource_x_reconfig_block();
		ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("PCM transport entry identity changed while retained")));
	}
	refs = pg_atomic_read_u32(&entry->transport_refcount);
	for (;;) {
		if (refs == 0) {
			pcm_resource_x_reconfig_block();
			ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("PCM entry transport reference count underflow")));
		}
		if (pg_atomic_compare_exchange_u32(
				&entry->transport_refcount, &refs, refs - 1))
			break;
	}
	memset(transport_ref, 0, sizeof(*transport_ref));
}

static ClusterPcmResourceXMasterState *
pcm_resource_x_master_state_for_entry(const struct GrdEntry *entry)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXSlot *slot;

	/* D2 callers already hold an exact pinned entry handle (or the directory
	 * lock while scanning).  Resolve its immutable registry index directly.
	 * Re-probing by tag here would inspect unrelated slots without htab_lock
	 * and could mistake another resource's retire publication window for
	 * corruption of this pinned resource. */
	if (entry == NULL || cluster_pcm_resource_x_slots == NULL
		|| cluster_pcm_resource_x_master_states == NULL
		|| pcm_grd_effective <= 0
		|| entry->registry_slot >= (uint32)pcm_grd_effective)
		return NULL;
	slot = &cluster_pcm_resource_x_slots[entry->registry_slot];
	state = &cluster_pcm_resource_x_master_states[entry->registry_slot];
	if (slot->state != PCM_REGISTRY_LIVE || slot->reserved != 0
		|| !BufferTagsEqual(&slot->tag, &entry->tag)
		|| slot->binding_generation == 0
		|| slot->binding_generation == UINT64_MAX
		|| slot->binding_generation != entry->binding_generation
		|| slot->retired_authority_generation != 0
		|| state->binding_generation != entry->binding_generation)
		return NULL;
	return state;
}

static ClusterPcmResourceXMasterRequest *
pcm_resource_x_master_head(ClusterPcmResourceXMasterState *state,
							   int32 *requester_node_out)
{
	ClusterPcmResourceXMasterRequest *head = NULL;
	int32 requester_node;

	for (requester_node = 0; requester_node < RESOURCE_X_PROTOCOL_NODE_LIMIT;
		 requester_node++) {
		ClusterPcmResourceXMasterRequest *candidate
			= &state->requests[requester_node];

		if (candidate->phase == RESOURCE_X_MASTER_NONE
			|| candidate->phase == RESOURCE_X_MASTER_SETTLED
			|| candidate->phase == RESOURCE_X_MASTER_RELEASED)
			continue;
		if (head == NULL || candidate->enqueue_order < head->enqueue_order) {
			head = candidate;
			if (requester_node_out != NULL)
				*requester_node_out = requester_node;
		}
	}
	return head;
}

/*
 * Project the exact canonical Resource-X FIFO head into the existing
 * retry-only S admission barrier.  A kind-9 receipt is deliberately absent
 * from this classifier: only ASSERT has created a canonical master request.
 * The query carries no authority and performs no state change.
 *
 * Unknown or internally inconsistent active state is a barrier, never
 * permission to admit a new S holder.  Terminal request history is skipped by
 * pcm_resource_x_master_head(), so a settled/released resource returns false
 * and ordinary current-holder routing resumes.
 */
static bool
pcm_resource_x_s_barrier_active_locked(struct GrdEntry *entry)
{
	ClusterPcmResourceXMasterRequest *head;
	ClusterPcmResourceXMasterState *state;
	uint32 gate_phase;
	uint64 gate_formation;
	bool active = false;
	int32 head_node = -1;

	Assert(entry != NULL);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL || state->authority_generation == 0
		|| state->authority_generation == UINT64_MAX
		|| state->next_enqueue_order == 0
		|| state->next_enqueue_order == UINT64_MAX) {
		active = true;
		goto out;
	}
	head = pcm_resource_x_master_head(state, &head_node);
	if (head == NULL)
		goto out;

	gate_phase = pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase);
	gate_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (head_node < 0 || head_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| head->base_authority_generation == 0
		|| head->base_authority_generation == UINT64_MAX
		|| head->resource_formation == 0
		|| head->resource_formation == UINT64_MAX
		|| head->master_session_incarnation == 0
		|| head->master_session_incarnation == UINT64_MAX
		|| head->assertion_sequence == 0
		|| head->assertion_sequence == UINT64_MAX
		|| head->enqueue_order == 0
		|| head->enqueue_order == UINT64_MAX
		|| gate_phase != RESOURCE_X_GATE_OPEN
		|| gate_formation != head->resource_formation) {
		active = true;
		goto out;
	}

	switch (head->phase) {
		case RESOURCE_X_MASTER_QUEUED:
		case RESOURCE_X_MASTER_WAIT_BLOCKERS:
		case RESOURCE_X_MASTER_WAIT_PROOF:
		case RESOURCE_X_MASTER_GRANT_COMMITTED:
		case RESOURCE_X_MASTER_RECOVERY_BLOCKED:
			active = true;
			break;
		case RESOURCE_X_MASTER_NONE:
		case RESOURCE_X_MASTER_SETTLED:
		case RESOURCE_X_MASTER_RELEASED:
		default:
			/* NONE/terminal cannot be selected as head; reaching one here is
			 * therefore corruption and remains fail closed. */
			active = true;
			break;
	}

out:
	return active;
}

bool
cluster_pcm_lock_resource_x_s_barrier_active_exact(const BufferTag *tag)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	bool active;

	if (tag == NULL || ClusterPcm == NULL)
		return false;
	if (!pcm_entry_ref_acquire(
			tag, false, &entry_ref, &acquire_result))
		return false;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	active = pcm_resource_x_s_barrier_active_locked(entry);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return active;
}

bool
cluster_pcm_lock_resource_x_requester_s_barrier_active_exact(
	const BufferTag *tag)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	bool active;

	if (tag == NULL || ClusterPcm == NULL)
		return false;
	if (!pcm_entry_ref_acquire(tag, false, &entry_ref, &acquire_result))
		return false;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	switch (round->phase) {
		case RESOURCE_X_BOOTSTRAP_ROUND_EMPTY:
		case RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED:
			active = false;
			break;
		case RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED:
			/* A terminal phase opens S admission only when the retained round
			 * still proves the exact local cached-X cover.  Malformed terminal
			 * state remains a conservative retry barrier. */
			active
				= !pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
					entry, round, &round->terminal_ref,
					round->master_session_incarnation,
					round->r4_record_generation,
					round->cached_ownership_generation);
			break;
		case RESOURCE_X_BOOTSTRAP_ROUND_REQUEST_DISPATCHED:
		case RESOURCE_X_BOOTSTRAP_ROUND_BASE_BOUND:
		case RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED:
		default:
			active = true;
			break;
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return active;
}

static ClusterPcmResourceXMasterRequest *
pcm_resource_x_start_head_locked(struct GrdEntry *entry,
								 ClusterPcmResourceXMasterState *state,
								 int32 *requester_node_out)
{
	ClusterPcmResourceXMasterRequest *request;
	PcmState pcm_state;
	uint32 incompatible = 0;
	int32 requester_node = -1;

	Assert(entry != NULL);
	Assert(state != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	request = pcm_resource_x_master_head(state, &requester_node);
	if (request == NULL)
		return NULL;
	if (request->phase == RESOURCE_X_MASTER_QUEUED) {
		request->head_transition_generation
			= pg_atomic_read_u64(&entry->transition_count_local);
		if (request->head_transition_generation == UINT64_MAX) {
			request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
			goto done;
		}
		pcm_state = (PcmState)pg_atomic_read_u32(&entry->master_state);
		if (pcm_state == PCM_STATE_X && entry->x_holder_node >= 0
			&& entry->x_holder_node != requester_node)
			incompatible = UINT32_C(1) << (uint32)entry->x_holder_node;
		else if (pcm_state == PCM_STATE_S)
			incompatible = pg_atomic_read_u32(&entry->s_holders_bitmap)
				& ~(UINT32_C(1) << (uint32)requester_node);
		request->incompatible_holders_bitmap = incompatible;
		request->blocked_holders_bitmap = 0;
		request->phase = incompatible != 0 ? RESOURCE_X_MASTER_WAIT_BLOCKERS
										 : RESOURCE_X_MASTER_WAIT_PROOF;
		if (incompatible != 0
			&& !pcm_resource_x_arm_block_intents_locked(
				entry, state, request, requester_node))
			request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
	}
done:
	if (requester_node_out != NULL)
		*requester_node_out = requester_node;
	return request;
}

static void
pcm_resource_x_master_snapshot(const BufferTag *tag, int32 requester_node,
							   const ClusterPcmResourceXMasterState *state,
							   const ClusterPcmResourceXMasterRequest *request,
							   ResourceXMasterSnapshot *out)
{
	int32 head_node = -1;

	if (out == NULL)
		return;
	memset(out, 0, sizeof(*out));
	out->source_node = -1;
	if (state == NULL || request == NULL || request->phase == RESOURCE_X_MASTER_NONE)
		return;
	out->assertion.resource = *tag;
	out->assertion.requester_node = requester_node;
	out->base_authority_generation = request->base_authority_generation;
	out->resource_formation = request->resource_formation;
	out->master_session_incarnation = request->master_session_incarnation;
	out->assertion_sequence = request->assertion_sequence;
	out->final_authority_generation = request->final_authority_generation;
	out->source_carrier_generation = request->source_carrier_generation;
	out->requester_target_generation = request->requester_target_generation;
	out->incompatible_holders_bitmap = request->incompatible_holders_bitmap;
	out->blocked_holders_bitmap = request->blocked_holders_bitmap;
	out->source_node = request->source_node;
	out->phase = request->phase;
	out->proof_kind = request->proof_kind;
	out->source_disposition = request->source_disposition;
	(void)pcm_resource_x_master_head((ClusterPcmResourceXMasterState *)state,
									 &head_node);
	out->is_head = head_node == requester_node ? 1 : 0;
}

static bool
pcm_resource_x_terminal_tombstone_valid(
	const ClusterPcmResourceXTerminalTombstone *tombstone)
{
	return tombstone != NULL && tombstone->valid == 1
		&& tombstone->requester_node >= 0
		&& tombstone->requester_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		&& (tombstone->request.phase == RESOURCE_X_MASTER_SETTLED
			|| tombstone->request.phase == RESOURCE_X_MASTER_RELEASED);
}

static void
pcm_resource_x_terminal_tombstone_snapshot(
	const BufferTag *tag, const ClusterPcmResourceXMasterState *state,
	const ClusterPcmResourceXTerminalTombstone *tombstone,
	ResourceXMasterSnapshot *out)
{
	Assert(pcm_resource_x_terminal_tombstone_valid(tombstone));
	pcm_resource_x_master_snapshot(tag, tombstone->requester_node, state,
		&tombstone->request, out);
	if (out != NULL)
		out->is_head = 0;
}

static bool
pcm_resource_x_assert_common_matches_request(
	const ResourceXDecodedCommon *common,
	const ClusterPcmResourceXMasterRequest *request)
{
	return common != NULL && request != NULL
		&& common->base_authority_generation
			== request->base_authority_generation
		&& common->authority_generation
			== request->base_authority_generation
		&& common->resource_formation == request->resource_formation
		&& common->master_session_incarnation
			== request->master_session_incarnation
		&& common->assertion_sequence == request->assertion_sequence
		&& common->ordered_lane == request->ordered_lane
		&& common->sender_connection_generation
			== request->sender_connection_generation;
}

static bool
pcm_resource_x_assert_frame_valid(const ResourceXDecodedFrame *frame,
								  int32 authenticated_source_node)
{
	return frame != NULL && frame->kind == RESOURCE_X_WIRE_ASSERT_X
		&& resource_x_assertion_valid(&frame->common.logical_assertion)
		&& authenticated_source_node == frame->common.logical_assertion.requester_node
		&& frame->common.action_node == authenticated_source_node
		&& frame->common.base_authority_generation != 0
		&& frame->common.authority_generation
			   == frame->common.base_authority_generation
		&& frame->common.resource_formation != 0
		&& frame->common.master_session_incarnation != 0
		&& frame->common.assertion_sequence != 0
		&& frame->common.sender_connection_generation != 0
		&& frame->common.target_mode == (uint8)PCM_STATE_X
		&& frame->common.outcome == RESOURCE_X_OUTCOME_NONE;
}

static void
pcm_resource_x_common_copy(ResourceXDecodedCommon *destination,
						   const ResourceXDecodedCommon *source)
{
	Assert(destination != NULL);
	Assert(source != NULL);
	memset(destination, 0, sizeof(*destination));
	destination->logical_assertion = source->logical_assertion;
	destination->base_authority_generation
		= source->base_authority_generation;
	destination->resource_formation = source->resource_formation;
	destination->master_session_incarnation
		= source->master_session_incarnation;
	destination->assertion_sequence = source->assertion_sequence;
	destination->ordered_lane = source->ordered_lane;
	destination->action_node = source->action_node;
	destination->observed_mode = source->observed_mode;
	destination->target_mode = source->target_mode;
	destination->source_candidate = source->source_candidate;
	destination->retain_pi_if_dirty = source->retain_pi_if_dirty;
	destination->sender_connection_generation
		= source->sender_connection_generation;
	destination->outcome = source->outcome;
	destination->flags = source->flags;
	destination->authority_generation = source->authority_generation;
	destination->semantic_crc32c = source->semantic_crc32c;
}

static void
pcm_resource_x_bootstrap_round_dispatch_snapshot(
	const ClusterPcmResourceXBootstrapRound *round,
	ResourceXDecodedFrame *dispatch_out)
{
	Assert(round != NULL);
	Assert(dispatch_out != NULL);
	memset(dispatch_out, 0, sizeof(*dispatch_out));
	dispatch_out->kind = RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP;
	dispatch_out->payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	pcm_resource_x_common_copy(&dispatch_out->common, &round->request);
}

static void
pcm_resource_x_bootstrap_round_assertion_snapshot(
	const ClusterPcmResourceXBootstrapRound *round,
	ResourceXDecodedFrame *dispatch_out)
{
	Assert(round != NULL);
	Assert(dispatch_out != NULL);
	memset(dispatch_out, 0, sizeof(*dispatch_out));
	dispatch_out->kind = RESOURCE_X_WIRE_ASSERT_X;
	dispatch_out->payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	pcm_resource_x_common_copy(&dispatch_out->common, &round->assertion);
}

static bool
pcm_resource_x_bootstrap_round_ack_matches_request(
	const ResourceXDecodedFrame *ack,
	const ClusterPcmResourceXBootstrapRound *round)
{
	const ResourceXDecodedCommon *request;
	const ResourceXDecodedCommon *common;

	if (ack == NULL || round == NULL)
		return false;
	request = &round->request;
	common = &ack->common;
	return ack->kind == RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP
		&& ack->payload_bytes == RESOURCE_X_CONTROL_V1_BYTES
		&& !ack->blocked_has_remote_proof
		&& resource_x_assertion_equal(&common->logical_assertion,
			&request->logical_assertion)
		&& common->base_authority_generation != 0
		&& common->base_authority_generation != UINT64_MAX
		&& common->resource_formation == request->resource_formation
		&& common->master_session_incarnation
			== request->master_session_incarnation
		&& common->assertion_sequence == request->assertion_sequence
		&& common->ordered_lane == 0
		&& common->action_node
			== request->logical_assertion.requester_node
		&& common->observed_mode == (uint8)PCM_STATE_N
		&& common->target_mode == (uint8)PCM_STATE_X
		&& common->source_candidate == 0
		&& common->retain_pi_if_dirty == 0
		&& common->sender_connection_generation != 0
		&& common->sender_connection_generation != UINT32_MAX
		&& common->outcome == RESOURCE_X_OUTCOME_OK
		&& common->flags == 0
		&& common->authority_generation == 0;
}

static bool
pcm_resource_x_install_claim_valid_locked(const ClusterPcmResourceXBootstrapRound *round)
{
	if (round->install_claim_source == RESOURCE_X_INSTALL_CLAIM_NONE)
		return round->install_claim_pending_generation == 0
			   && round->install_claim_reservation_token == 0;
	return (round->install_claim_source == RESOURCE_X_INSTALL_CLAIM_ORDINARY_T1
			|| round->install_claim_source == RESOURCE_X_INSTALL_CLAIM_DIRECT_INIT)
		   && round->install_claim_pending_generation < UINT64_MAX - 1
		   && round->install_claim_reservation_token != 0
		   && round->install_claim_reservation_token != UINT64_MAX;
}

static bool
pcm_resource_x_install_claim_ref_exact_locked(const ClusterPcmResourceXBootstrapRound *round,
											  const ResourceXAcquisitionRef *ref)
{
	return resource_x_assertion_equal(&round->request.logical_assertion, &ref->assertion)
		   && round->resource_formation == ref->formation
		   && round->request.assertion_sequence == ref->acquisition_generation
		   && round->highest_attempt_floor == ref->acquisition_generation;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_install_claim_bind_t1_exact(const ResourceXAcquisitionRef *ref,
														const ClusterPcmOwnSnapshot *reserved)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ClusterPcmResourceXBootstrapRound *round;
	ResourceXApplyResult result;
	bool broadcast = false;

	if (!pcm_resource_x_ref_valid(ref) || reserved == NULL
		|| !BufferTagsEqual(&ref->assertion.resource, &reserved->tag)
		|| reserved->pcm_state != (uint8)PCM_STATE_N
		|| reserved->flags != PCM_OWN_FLAG_GRANT_PENDING || reserved->generation >= UINT64_MAX - 1
		|| reserved->reservation_token == 0 || reserved->reservation_token == UINT64_MAX
		|| reserved->writer_activation_token != 0
		|| reserved->resource_x_activation_generation != 0)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&ref->assertion.resource, false, &entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
	round = &entry->resource_x_bootstrap_round;
	if (!pcm_resource_x_install_claim_ref_exact_locked(round, ref))
		result = RESOURCE_X_APPLY_STALE;
	else if (!cluster_pcm_lock_resource_x_gate_open_exact(ref->formation)
			 || !pcm_resource_x_install_claim_valid_locked(round)
			 || !pcm_resource_x_local_owner_valid_locked(&entry->resource_x_local_owner))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED
			 || round->accepted_base == 0 || round->accepted_base == UINT64_MAX
			 || entry->resource_x_local_owner.state != RESOURCE_X_LOCAL_OWNER_EMPTY
			 || pcm_resource_x_ref_classify_locked(entry, ref) != PCM_RX_REF_ACTIVE_EXACT
			 || entry->resource_x_progress_flags
					!= (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1))
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (round->install_claim_source != RESOURCE_X_INSTALL_CLAIM_NONE) {
		if (round->install_claim_reservation_token == reserved->reservation_token
			&& round->install_claim_pending_generation == reserved->generation)
			result = RESOURCE_X_APPLY_DUPLICATE;
		else if (reserved->reservation_token < round->install_claim_reservation_token)
			result = RESOURCE_X_APPLY_STALE;
		else
			result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	} else {
		pcm_resource_x_semantic_mutation_mark();
		round->install_claim_pending_generation = reserved->generation;
		round->install_claim_reservation_token = reserved->reservation_token;
		round->install_claim_source = RESOURCE_X_INSTALL_CLAIM_ORDINARY_T1;
		broadcast = true;
		result = pcm_resource_x_head_changed_locked(entry, true, pcm_resource_x_monotonic_us())
					 ? RESOURCE_X_APPLY_APPLIED
					 : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	LWLockRelease(&entry->entry_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_install_claim_snapshot_exact(const ResourceXAcquisitionRef *ref,
														 uint8 *source_out,
														 uint64 *pending_generation_out,
														 uint64 *reservation_token_out)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	const ClusterPcmResourceXBootstrapRound *round;
	ResourceXApplyResult result;

	if (source_out != NULL)
		*source_out = RESOURCE_X_INSTALL_CLAIM_NONE;
	if (pending_generation_out != NULL)
		*pending_generation_out = 0;
	if (reservation_token_out != NULL)
		*reservation_token_out = 0;
	if (!pcm_resource_x_ref_valid(ref) || source_out == NULL || pending_generation_out == NULL
		|| reservation_token_out == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&ref->assertion.resource, false, &entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	if (!pcm_resource_x_install_claim_ref_exact_locked(round, ref))
		result = RESOURCE_X_APPLY_STALE;
	else if (!pcm_resource_x_install_claim_valid_locked(round))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED
			 && round->phase != RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (round->install_claim_source == RESOURCE_X_INSTALL_CLAIM_NONE)
		result = RESOURCE_X_APPLY_NOT_FOUND;
	else {
		*source_out = round->install_claim_source;
		*pending_generation_out = round->install_claim_pending_generation;
		*reservation_token_out = round->install_claim_reservation_token;
		result = RESOURCE_X_APPLY_APPLIED;
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

/* The held PcmEntryRef fences entry binding/reuse.  Continuations additionally
 * compare their saved binding_generation before reaching this comparator.
 * Scheduling budgets and physical installation observations are not part of
 * the node request key. */
static bool
pcm_resource_x_node_request_key_matches_locked(const ClusterPcmResourceXBootstrapRound *round,
											   const ResourceXAssertion *assertion,
											   int32 current_master_node, uint64 resource_formation,
											   uint64 master_session_incarnation,
											   uint64 r4_record_generation,
											   uint32 requester_sender_connection_generation,
											   uint32 master_ingress_connection_generation)
{
	return round != NULL && assertion != NULL
		   && resource_x_assertion_equal(&round->request.logical_assertion, assertion)
		   && round->current_master_node == current_master_node
		   && round->resource_formation == resource_formation
		   && round->master_session_incarnation == master_session_incarnation
		   && round->r4_record_generation == r4_record_generation
		   && round->requester_sender_connection_generation
				  == requester_sender_connection_generation
		   && round->master_ingress_connection_generation == master_ingress_connection_generation;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_install_claim_join_observe_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	ResourceXInstallClaimJoinObservation *out)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	const ClusterPcmResourceXBootstrapRound *round;
	ResourceXApplyResult result;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || !resource_x_assertion_valid(assertion)
		|| assertion->requester_node != cluster_node_id)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false, &entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY)
		result = RESOURCE_X_APPLY_NOT_FOUND;
	else if (!pcm_resource_x_node_request_key_matches_locked(
				 round, assertion, current_master_node, resource_formation,
				 master_session_incarnation, r4_record_generation,
				 requester_sender_connection_generation, master_ingress_connection_generation))
		result = RESOURCE_X_APPLY_STALE;
	else if (round->phase > RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (!pcm_resource_x_install_claim_valid_locked(round)
			 || round->request.assertion_sequence == 0
			 || round->request.assertion_sequence == UINT64_MAX
			 || round->request.assertion_sequence != round->highest_attempt_floor)
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else {
		out->request = round->request;
		out->entry_binding_generation = entry_ref.binding_generation;
		out->r4_record_generation = round->r4_record_generation;
		out->master_node = round->current_master_node;
		out->master_ingress_connection_generation = round->master_ingress_connection_generation;
		result = RESOURCE_X_APPLY_APPLIED;
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_install_claim_bind_direct_init_exact(
	const ResourceXInstallClaimJoinObservation *observation, const ClusterPcmOwnSnapshot *before,
	const ClusterPcmOwnSnapshot *after)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ClusterPcmResourceXBootstrapRound *round;
	const ResourceXDecodedCommon *request;
	ResourceXApplyResult result;
	bool broadcast = false;

	if (observation == NULL || before == NULL || after == NULL)
		return RESOURCE_X_APPLY_INVALID;
	request = &observation->request;
	if (!resource_x_assertion_valid(&request->logical_assertion)
		|| request->logical_assertion.requester_node != cluster_node_id
		|| observation->entry_binding_generation == 0
		|| observation->entry_binding_generation == UINT64_MAX || request->assertion_sequence == 0
		|| request->assertion_sequence == UINT64_MAX)
		return RESOURCE_X_APPLY_INVALID;
	if (!cluster_pcm_own_snapshot_equal_exact(before, after))
		return RESOURCE_X_APPLY_STALE;
	if (!BufferTagsEqual(&before->tag, &request->logical_assertion.resource)
		|| before->pcm_state != (uint8)PCM_STATE_N || before->flags != PCM_OWN_FLAG_GRANT_PENDING
		|| before->generation >= UINT64_MAX - 1 || before->reservation_token == 0
		|| before->reservation_token == UINT64_MAX || before->writer_activation_token != 0
		|| before->resource_x_activation_generation != 0)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&before->tag, false, &entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
	round = &entry->resource_x_bootstrap_round;
	if (entry_ref.binding_generation != observation->entry_binding_generation
		|| !pcm_resource_x_common_equal(&round->request, request)
		|| round->highest_attempt_floor != request->assertion_sequence
		|| !pcm_resource_x_node_request_key_matches_locked(
			round, &request->logical_assertion, observation->master_node,
			request->resource_formation, request->master_session_incarnation,
			observation->r4_record_generation, request->sender_connection_generation,
			observation->master_ingress_connection_generation))
		result = RESOURCE_X_APPLY_STALE;
	else if (!cluster_pcm_lock_resource_x_gate_open_exact(request->resource_formation)
			 || !pcm_resource_x_install_claim_valid_locked(round)
			 || !pcm_resource_x_local_owner_valid_locked(&entry->resource_x_local_owner))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (round->phase < RESOURCE_X_BOOTSTRAP_ROUND_REQUEST_DISPATCHED
			 || round->phase > RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (round->install_claim_source != RESOURCE_X_INSTALL_CLAIM_NONE) {
		if (round->install_claim_pending_generation == before->generation
			&& round->install_claim_reservation_token == before->reservation_token)
			result = RESOURCE_X_APPLY_DUPLICATE;
		else
			result = before->reservation_token < round->install_claim_reservation_token
						 ? RESOURCE_X_APPLY_STALE
						 : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	} else if (before->generation == 0 || !pcm_resource_x_active_empty_locked(entry)
			   || entry->resource_x_local_owner.state != RESOURCE_X_LOCAL_OWNER_EMPTY)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else {
		pcm_resource_x_semantic_mutation_mark();
		round->install_claim_pending_generation = before->generation;
		round->install_claim_reservation_token = before->reservation_token;
		round->install_claim_source = RESOURCE_X_INSTALL_CLAIM_DIRECT_INIT;
		broadcast = true;
		result = pcm_resource_x_head_changed_locked(entry, true, pcm_resource_x_monotonic_us())
					 ? RESOURCE_X_APPLY_APPLIED
					 : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	LWLockRelease(&entry->entry_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

/* A tokenless caller joins the node request without claiming any physical
 * installation.  A caller presenting a token must match the bound claim; it
 * cannot replace that claim, including when it is otherwise creator-capable. */
static bool
pcm_resource_x_request_observation_matches_locked(
	const ClusterPcmResourceXBootstrapRound *round, const ResourceXAssertion *assertion,
	int32 current_master_node, uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation, uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation, uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token)
{
	return pcm_resource_x_node_request_key_matches_locked(
			   round, assertion, current_master_node, resource_formation,
			   master_session_incarnation, r4_record_generation,
			   requester_sender_connection_generation, master_ingress_connection_generation)
		   && (direct_init_reservation_token == 0
				   ? direct_init_ownership_generation == 0
				   : (round->install_claim_pending_generation == direct_init_ownership_generation
					  && round->install_claim_reservation_token == direct_init_reservation_token));
}

/* These counters share the existing PCM diagnostic allocation.  Unavailable
 * shared memory is reported as unavailable, never as a zero-valued snapshot. */
void
cluster_undo_block0_reply_wait_metric_add(ClusterUndoBlock0ReplyWaitSite site,
										  ClusterUndoBlock0ReplyWaitMetric metric)
{
	if (ClusterPcm != NULL && (unsigned)site < CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT
		&& (unsigned)metric < CLUSTER_UNDO_BLOCK0_WAIT_METRIC_COUNT)
		pg_atomic_fetch_add_u64(&ClusterPcm->block0_reply_wait_count[site][metric], 1);
}

bool
cluster_undo_block0_reply_wait_stats_snapshot(ClusterUndoBlock0ReplyWaitStats *out)
{
	int site;
	int metric;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (ClusterPcm == NULL)
		return false;
	for (site = 0; site < CLUSTER_UNDO_BLOCK0_WAIT_SITE_COUNT; site++)
		for (metric = 0; metric < CLUSTER_UNDO_BLOCK0_WAIT_METRIC_COUNT; metric++)
			out->count[site][metric]
				= pg_atomic_read_u64(&ClusterPcm->block0_reply_wait_count[site][metric]);
	return true;
}

const char *
cluster_pcm_lock_resource_x_head_failure_name(uint8 reason)
{
	switch (reason) {
	case RESOURCE_X_HEAD_FAILURE_NONE:
		return "NONE";
	case RESOURCE_X_HEAD_NO_PROGRESS_EXPIRED:
		return "HEAD_NO_PROGRESS_EXPIRED";
	case RESOURCE_X_HEAD_PROGRESS_OVERFLOW:
		return "HEAD_PROGRESS_OVERFLOW";
	default:
		return "HEAD_FAILURE_MALFORMED";
	}
}

/* Only an exact owner calls this at a new predicate/progress edge.  Retry
 * pacing and follower observations never enter here.  Historical lease bytes
 * may remain in a terminal cover, but are never tested as terminal expiry. */
static bool
pcm_resource_x_head_changed_locked(struct GrdEntry *entry, bool semantic_progress, uint64 now_us)
{
	ClusterPcmResourceXBootstrapRound *round = &entry->resource_x_bootstrap_round;
	uint64 progress_us = Max(now_us, round->head_last_semantic_progress_us);
	bool preterminal;

	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY)
		return true;
	preterminal = round->phase >= RESOURCE_X_BOOTSTRAP_ROUND_REQUEST_DISPATCHED
				  && round->phase <= RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED;
	if (round->head_change_generation >= UINT64_MAX - 1
		|| (semantic_progress
			&& (now_us == 0 || now_us == UINT64_MAX
				|| (preterminal
					&& (round->head_no_progress_budget_us == 0
						|| round->head_no_progress_budget_us == UINT64_MAX
						|| progress_us >= UINT64_MAX - round->head_no_progress_budget_us))))) {
		round->phase = RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED;
		round->head_failure_reason = RESOURCE_X_HEAD_PROGRESS_OVERFLOW;
		ConditionVariableBroadcast(&entry->wait_cv);
		return false;
	}
	round->head_change_generation++;
	if (semantic_progress) {
		round->head_last_semantic_progress_us = progress_us;
		if (preterminal)
			round->head_no_progress_deadline_us = progress_us + round->head_no_progress_budget_us;
	}
	ConditionVariableBroadcast(&entry->wait_cv);
	return true;
}

static void
pcm_resource_x_head_fail_locked(struct GrdEntry *entry, uint8 reason)
{
	ClusterPcmResourceXBootstrapRound *round = &entry->resource_x_bootstrap_round;

	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED) {
		round->phase = RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED;
		round->head_failure_reason = reason;
		(void)pcm_resource_x_head_changed_locked(entry, false, 0);
	}
}

static bool
pcm_resource_x_head_progress_ref_locked(struct GrdEntry *entry, const ResourceXAcquisitionRef *ref,
										uint64 now_us)
{
	ClusterPcmResourceXBootstrapRound *round = &entry->resource_x_bootstrap_round;

	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY
		|| !pcm_resource_x_install_claim_ref_exact_locked(round, ref))
		return true;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED)
		return false;
	return pcm_resource_x_head_changed_locked(entry, true, now_us);
}

static void
pcm_resource_x_bootstrap_round_clear_binding_locked(
	ClusterPcmResourceXBootstrapRound *round)
{
	uint64 highest_attempt_floor;

	Assert(round != NULL);
	highest_attempt_floor = round->highest_attempt_floor;
	memset(round, 0, sizeof(*round));
	round->highest_attempt_floor = highest_attempt_floor;
}

/* An expired requester-local round is terminal for every caller that joined
 * its frozen R7 deadline, but it is not persistent authority and therefore
 * cannot poison this resource forever.  A later ordinary acquisition may
 * retire only a structurally complete expired binding, after all local
 * authority/debt carriers are empty, and then allocate a strictly higher
 * attempt from the retained floor.  The later caller contributes a new
 * deadline only to the new round; no field from the failed attempt is reused.
 * Direct-init has a separate creation reservation and is deliberately not
 * eligible for this ordinary-acquisition handoff. */
static bool
pcm_resource_x_bootstrap_failed_round_rearmable_locked(
	struct GrdEntry *entry, const ClusterPcmResourceXBootstrapRound *round,
	const ResourceXAssertion *assertion, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation,
	uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation,
	uint64 absolute_deadline_us, uint64 now_us, uint64 retry_slice_us,
	uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token)
{
	static const ResourceXAcquisitionRef empty_ref;
	static const ResourceXDecodedCommon empty_common;
	static const uint8 zero_reserved[1];
	ClusterPcmResourceXMasterState *master_state;
	ResourceXDecodedCommon expected_assertion;
	ResourceXDecodedFrame ack_frame;
	const ResourceXDecodedCommon *request;
	bool pre_ack;

	Assert(entry != NULL);
	Assert(round != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	master_state = pcm_resource_x_master_state_for_entry(entry);
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED
		|| round->head_failure_reason > RESOURCE_X_HEAD_NO_PROGRESS_EXPIRED
		|| direct_init_ownership_generation != 0 || direct_init_reservation_token != 0
		|| round->install_claim_pending_generation != 0
		|| round->install_claim_reservation_token != 0
		|| round->install_claim_source != RESOURCE_X_INSTALL_CLAIM_NONE
		|| round->head_no_progress_deadline_us == 0
		|| round->head_no_progress_deadline_us == UINT64_MAX
		|| now_us < round->head_no_progress_deadline_us || now_us >= absolute_deadline_us
		|| round->highest_attempt_floor == 0 || round->highest_attempt_floor == UINT64_MAX
		|| round->last_dispatch_us == 0
		|| round->last_dispatch_us >= round->head_no_progress_deadline_us
		|| round->terminal_authority_generation != 0 || round->cached_ownership_generation != 0
		|| memcmp(&round->terminal_ref, &empty_ref, sizeof(empty_ref)) != 0
		|| memcmp(round->reserved, zero_reserved, sizeof(zero_reserved)) != 0
		|| !pcm_resource_x_local_owner_valid_locked(&entry->resource_x_local_owner)
		|| entry->resource_x_local_owner.state != RESOURCE_X_LOCAL_OWNER_EMPTY
		|| !pcm_resource_x_active_empty_locked(entry) || master_state == NULL
		|| !pcm_resource_x_requester_join_empty_locked(&master_state->requester_join)
		|| !pcm_resource_x_request_observation_matches_locked(
			round, assertion, current_master_node, resource_formation, master_session_incarnation,
			r4_record_generation, requester_sender_connection_generation,
			master_ingress_connection_generation, 0, 0))
		return false;

	request = &round->request;
	if (!resource_x_assertion_valid(&request->logical_assertion)
		|| request->logical_assertion.requester_node != cluster_node_id
		|| !BufferTagsEqual(&request->logical_assertion.resource, &entry->tag)
		|| request->base_authority_generation != 0
		|| request->authority_generation != 0
		|| request->resource_formation != resource_formation
		|| request->master_session_incarnation
			!= master_session_incarnation
		|| request->assertion_sequence != round->highest_attempt_floor
		|| request->ordered_lane != 0
		|| request->action_node != cluster_node_id
		|| request->observed_mode != (uint8)PCM_STATE_N
		|| request->target_mode != (uint8)PCM_STATE_X
		|| request->source_candidate != 0
		|| request->retain_pi_if_dirty != 0
		|| request->sender_connection_generation
			!= requester_sender_connection_generation
		|| request->outcome != RESOURCE_X_OUTCOME_NONE
		|| request->flags != 0
		|| request->semantic_crc32c != 0)
		return false;

	pre_ack = round->accepted_base == 0;
	if (pre_ack)
		return memcmp(&round->ack, &empty_common,
				sizeof(empty_common)) == 0
			&& memcmp(&round->assertion, &empty_common,
				sizeof(empty_common)) == 0;
	if (round->accepted_base == UINT64_MAX)
		return false;
	memset(&ack_frame, 0, sizeof(ack_frame));
	ack_frame.kind = RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP;
	ack_frame.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	ack_frame.common = round->ack;
	if (!pcm_resource_x_bootstrap_round_ack_matches_request(
			&ack_frame, round)
		|| round->ack.base_authority_generation != round->accepted_base)
		return false;
	pcm_resource_x_common_copy(&expected_assertion, request);
	expected_assertion.base_authority_generation = round->accepted_base;
	expected_assertion.authority_generation = round->accepted_base;
	expected_assertion.outcome = RESOURCE_X_OUTCOME_NONE;
	expected_assertion.semantic_crc32c = 0;
	return pcm_resource_x_common_equal(
		&round->assertion, &expected_assertion);
}

static bool
pcm_resource_x_acquisition_ref_equal(
	const ResourceXAcquisitionRef *left,
	const ResourceXAcquisitionRef *right)
{
	return left != NULL && right != NULL
		&& resource_x_assertion_equal(&left->assertion, &right->assertion)
		&& left->formation == right->formation
		&& left->acquisition_generation == right->acquisition_generation;
}

static bool
pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
	struct GrdEntry *entry,
	const ClusterPcmResourceXBootstrapRound *round,
	const ResourceXAcquisitionRef *ref,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint64 cached_ownership_generation)
{
	Assert(entry != NULL);
	Assert(round != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		   || LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	return round->phase == RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
		&& pcm_resource_x_ref_valid(ref)
		&& pcm_resource_x_ref_valid(&round->terminal_ref)
		&& pcm_resource_x_acquisition_ref_equal(&round->terminal_ref, ref)
		&& resource_x_assertion_equal(
			&round->request.logical_assertion, &ref->assertion)
		&& round->resource_formation == ref->formation
		&& round->master_session_incarnation
			== master_session_incarnation
		&& round->r4_record_generation == r4_record_generation
		&& round->request.assertion_sequence
			== ref->acquisition_generation
		&& round->highest_attempt_floor == ref->acquisition_generation
		&& round->terminal_authority_generation > round->accepted_base
		&& round->terminal_authority_generation != UINT64_MAX
		&& round->cached_ownership_generation
			== cached_ownership_generation
		&& pcm_resource_x_active_empty_locked(entry)
		&& entry->resource_x_retired_acquisition_generation
			== ref->acquisition_generation;
}

static bool
pcm_resource_x_local_owner_handle_valid(
	const ResourceXLocalOwnerHandle *handle)
{
	return handle != NULL
		&& pcm_resource_x_ref_valid(&handle->ref)
		&& handle->master_session_incarnation != 0
		&& handle->master_session_incarnation != UINT64_MAX
		&& handle->r4_record_generation != 0
		&& handle->r4_record_generation != UINT64_MAX
		&& handle->buffer_ownership_generation != 0
		&& handle->buffer_ownership_generation != UINT64_MAX
		&& handle->reservation_token != 0
		&& handle->reservation_token != UINT64_MAX
		&& handle->owner_generation != 0
		&& handle->owner_generation != UINT64_MAX
		&& handle->absolute_deadline_us != 0
		&& handle->absolute_deadline_us != UINT64_MAX
		&& handle->owner_procno >= 0
		&& handle->reserved == 0;
}

static bool
pcm_resource_x_local_owner_handle_equal(
	const ResourceXLocalOwnerHandle *left,
	const ResourceXLocalOwnerHandle *right)
{
	return pcm_resource_x_local_owner_handle_valid(left)
		&& pcm_resource_x_local_owner_handle_valid(right)
		&& pcm_resource_x_acquisition_ref_equal(&left->ref, &right->ref)
		&& left->master_session_incarnation
			== right->master_session_incarnation
		&& left->r4_record_generation == right->r4_record_generation
		&& left->buffer_ownership_generation
			== right->buffer_ownership_generation
		&& left->reservation_token == right->reservation_token
		&& left->owner_generation == right->owner_generation
		&& left->absolute_deadline_us == right->absolute_deadline_us
		&& left->owner_procno == right->owner_procno;
}

static bool
pcm_resource_x_local_handoff_empty(
	const ClusterPcmResourceXLocalHandoff *handoff)
{
	static const ClusterPcmResourceXLocalHandoff empty_handoff;

	return handoff != NULL
		&& memcmp(handoff, &empty_handoff, sizeof(empty_handoff)) == 0;
}

static bool
pcm_resource_x_local_handoff_valid(
	const ClusterPcmResourceXLocalHandoff *handoff)
{
	static const uint8 zero_reserved[7];

	return handoff != NULL && handoff->valid == 1
		&& resource_x_assertion_valid(&handoff->successor_assertion)
		&& pcm_resource_x_ref_valid(&handoff->holder_ref)
		&& BufferTagsEqual(&handoff->successor_assertion.resource,
			&handoff->holder_ref.assertion.resource)
		&& !resource_x_assertion_equal(&handoff->successor_assertion,
			&handoff->holder_ref.assertion)
		&& handoff->base_authority_generation != 0
		&& handoff->base_authority_generation != UINT64_MAX
		&& handoff->master_session_incarnation != 0
		&& handoff->master_session_incarnation != UINT64_MAX
		&& handoff->assertion_sequence != 0
		&& handoff->assertion_sequence != UINT64_MAX
		&& handoff->r4_record_generation != 0
		&& handoff->r4_record_generation != UINT64_MAX
		&& handoff->buffer_ownership_generation != 0
		&& handoff->buffer_ownership_generation != UINT64_MAX
		&& handoff->deadline_us != 0
		&& handoff->deadline_us != UINT64_MAX
		&& handoff->master_node >= 0
		&& handoff->master_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		&& memcmp(handoff->reserved, zero_reserved,
			sizeof(zero_reserved)) == 0;
}

static bool
pcm_resource_x_local_handoff_matches_handle(
	const ClusterPcmResourceXLocalHandoff *handoff,
	const ResourceXLocalOwnerHandle *handle)
{
	return pcm_resource_x_local_handoff_valid(handoff)
		&& pcm_resource_x_local_owner_handle_valid(handle)
		&& pcm_resource_x_acquisition_ref_equal(
			&handoff->holder_ref, &handle->ref)
		&& handoff->master_session_incarnation
			== handle->master_session_incarnation
		&& handoff->r4_record_generation
			== handle->r4_record_generation
		&& handoff->buffer_ownership_generation
			== handle->buffer_ownership_generation;
}

static bool
pcm_resource_x_local_owner_valid_locked(
	const ClusterPcmResourceXLocalOwner *owner)
{
	static const ResourceXLocalOwnerHandle empty_handle;
	static const uint8 zero_reserved[7];
	bool handle_valid;
	bool handoff_empty;
	bool handoff_valid;

	Assert(owner != NULL);
	if (owner->highest_owner_generation == UINT64_MAX
		|| memcmp(owner->reserved, zero_reserved,
			sizeof(zero_reserved)) != 0)
		return false;
	handle_valid = pcm_resource_x_local_owner_handle_valid(&owner->handle);
	handoff_empty = pcm_resource_x_local_handoff_empty(&owner->handoff);
	handoff_valid = pcm_resource_x_local_handoff_valid(&owner->handoff);
	if (owner->state == RESOURCE_X_LOCAL_OWNER_EMPTY)
		return memcmp(&owner->handle, &empty_handle,
					  sizeof(empty_handle)) == 0 && handoff_empty;
	if (owner->state == RESOURCE_X_LOCAL_OWNER_HANDOFF)
		return memcmp(&owner->handle, &empty_handle,
					  sizeof(empty_handle)) == 0
			&& handoff_valid && owner->highest_owner_generation != 0;
	if ((owner->state != RESOURCE_X_LOCAL_OWNER_RECYCLING
			&& owner->state != RESOURCE_X_LOCAL_OWNER_REVOKING
			&& owner->state != RESOURCE_X_LOCAL_OWNER_EVICTING)
		|| !handle_valid
		|| owner->handle.owner_generation
			!= owner->highest_owner_generation)
		return false;
	if (owner->state == RESOURCE_X_LOCAL_OWNER_EVICTING)
		return handoff_empty;
	return handoff_empty
		|| pcm_resource_x_local_handoff_matches_handle(
			&owner->handoff, &owner->handle);
}

static void
pcm_resource_x_local_owner_clear_locked(struct GrdEntry *entry)
{
	ClusterPcmResourceXLocalOwner *owner = &entry->resource_x_local_owner;
	uint64 highest_owner_generation;
	bool changed = owner->state != RESOURCE_X_LOCAL_OWNER_EMPTY;

	Assert(owner != NULL);
	highest_owner_generation = owner->highest_owner_generation;
	memset(owner, 0, sizeof(*owner));
	owner->highest_owner_generation = highest_owner_generation;
	if (changed)
		(void)pcm_resource_x_head_changed_locked(entry, true, pcm_resource_x_monotonic_us());
}

static bool
pcm_resource_x_local_handoff_round_current_locked(
	const ClusterPcmResourceXLocalHandoff *handoff,
	const ClusterPcmResourceXBootstrapRound *round)
{
	return pcm_resource_x_local_handoff_valid(handoff)
		&& round != NULL
		&& round->phase == RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
		&& pcm_resource_x_acquisition_ref_equal(
			&handoff->holder_ref, &round->terminal_ref)
		&& handoff->base_authority_generation
			== round->terminal_authority_generation
		&& handoff->master_session_incarnation
			== round->master_session_incarnation
		&& handoff->r4_record_generation
			== round->r4_record_generation
		&& handoff->buffer_ownership_generation
			== round->cached_ownership_generation
		&& handoff->master_node == round->current_master_node;
}

static bool
pcm_resource_x_local_owner_round_exact_locked(
	struct GrdEntry *entry, const ClusterPcmResourceXBootstrapRound *round)
{
	const ClusterPcmResourceXLocalOwner *owner
		= &entry->resource_x_local_owner;

	Assert(entry != NULL);
	Assert(round != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		   || LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	if (!pcm_resource_x_local_owner_valid_locked(owner)
		|| owner->state == RESOURCE_X_LOCAL_OWNER_EMPTY)
		return false;
	if (owner->state == RESOURCE_X_LOCAL_OWNER_HANDOFF)
		return pcm_resource_x_local_handoff_round_current_locked(
			&owner->handoff, round);
	return pcm_resource_x_acquisition_ref_equal(
			&owner->handle.ref, &round->terminal_ref)
		&& owner->handle.master_session_incarnation
			== round->master_session_incarnation
		&& owner->handle.r4_record_generation
			== round->r4_record_generation
		&& owner->handle.buffer_ownership_generation
			== round->cached_ownership_generation;
}

static bool
pcm_resource_x_local_handoff_deadline(uint64 now_us, uint64 *deadline_out)
{
	uint64 budget_us = (uint64)Max(cluster_gcs_reply_timeout_ms, 1)
		* UINT64_C(1000);

	if (deadline_out != NULL)
		*deadline_out = 0;
	if (deadline_out == NULL || now_us == 0 || now_us == UINT64_MAX
		|| budget_us == 0 || now_us >= UINT64_MAX - budget_us)
		return false;
	*deadline_out = now_us + budget_us;
	return *deadline_out != UINT64_MAX;
}

static bool
pcm_resource_x_local_owner_priority_clear_locked(struct GrdEntry *entry)
{
	ClusterPcmResourceXLocalOwner *owner = &entry->resource_x_local_owner;

	Assert(owner != NULL);
	if (owner->state == RESOURCE_X_LOCAL_OWNER_RECYCLING
		&& pcm_resource_x_local_handoff_valid(&owner->handoff)) {
		memset(&owner->handoff, 0, sizeof(owner->handoff));
		(void)pcm_resource_x_head_changed_locked(entry, true, pcm_resource_x_monotonic_us());
		return true;
	}
	if (owner->state == RESOURCE_X_LOCAL_OWNER_HANDOFF
		&& pcm_resource_x_local_handoff_valid(&owner->handoff)) {
		pcm_resource_x_local_owner_clear_locked(entry);
		return true;
	}
	return false;
}

static bool
pcm_resource_x_local_owner_expire_locked(struct GrdEntry *entry, uint64 now_us)
{
	ClusterPcmResourceXLocalOwner *owner = &entry->resource_x_local_owner;

	Assert(owner != NULL);
	Assert(now_us != 0 && now_us != UINT64_MAX);
	return (owner->state == RESOURCE_X_LOCAL_OWNER_RECYCLING
			|| owner->state == RESOURCE_X_LOCAL_OWNER_HANDOFF)
		   && pcm_resource_x_local_handoff_valid(&owner->handoff)
		   && now_us >= owner->handoff.deadline_us
		   && pcm_resource_x_local_owner_priority_clear_locked(entry);
}

static bool
pcm_resource_x_local_handoff_successor_exact(
	const ClusterPcmResourceXLocalHandoff *handoff,
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation)
{
	return pcm_resource_x_local_handoff_valid(handoff)
		&& successor_block != NULL
		&& resource_x_assertion_equal(&handoff->successor_assertion,
			&successor_block->common.logical_assertion)
		&& handoff->base_authority_generation
			== successor_block->common.base_authority_generation
		&& handoff->holder_ref.formation
			== successor_block->common.resource_formation
		&& handoff->master_session_incarnation
			== successor_block->common.master_session_incarnation
		&& handoff->assertion_sequence
			== successor_block->common.assertion_sequence
		&& handoff->r4_record_generation == r4_record_generation
		&& handoff->buffer_ownership_generation
			== cached_ownership_generation
		&& handoff->master_node == authenticated_master_node
		&& handoff->ordered_lane == successor_block->common.ordered_lane;
}

static ResourceXApplyResult
pcm_resource_x_local_handoff_build_locked(
	ClusterPcmResourceXLocalHandoff *handoff,
	const ClusterPcmResourceXBootstrapRound *round,
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation, uint64 now_us)
{
	uint64 deadline_us;

	Assert(handoff != NULL);
	Assert(round != NULL);
	Assert(successor_block != NULL);
	if (!pcm_resource_x_local_handoff_empty(handoff)
		|| !pcm_resource_x_local_handoff_deadline(now_us, &deadline_us))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	/* This is the bounded local first-observation deadline.  It is neither
	 * copied from nor represented as the requester R7 deadline, and every
	 * later claim/yield preserves this exact value. */
	handoff->successor_assertion
		= successor_block->common.logical_assertion;
	handoff->holder_ref = round->terminal_ref;
	handoff->base_authority_generation
		= successor_block->common.base_authority_generation;
	handoff->master_session_incarnation
		= successor_block->common.master_session_incarnation;
	handoff->assertion_sequence
		= successor_block->common.assertion_sequence;
	handoff->r4_record_generation = r4_record_generation;
	handoff->buffer_ownership_generation
		= cached_ownership_generation;
	handoff->deadline_us = deadline_us;
	handoff->master_node = authenticated_master_node;
	handoff->ordered_lane = successor_block->common.ordered_lane;
	handoff->valid = 1;
	if (!pcm_resource_x_local_handoff_round_current_locked(
			handoff, round)) {
		memset(handoff, 0, sizeof(*handoff));
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	return RESOURCE_X_APPLY_APPLIED;
}

static ResourceXApplyResult
pcm_resource_x_local_handoff_freeze_locked(struct GrdEntry *entry,
										   const ClusterPcmResourceXBootstrapRound *round,
										   const ResourceXDecodedFrame *successor_block,
										   int32 authenticated_master_node,
										   uint64 r4_record_generation,
										   uint64 cached_ownership_generation, uint64 now_us)
{
	ClusterPcmResourceXLocalOwner *owner = &entry->resource_x_local_owner;
	ResourceXApplyResult result;

	Assert(owner != NULL);
	Assert(round != NULL);
	Assert(successor_block != NULL);
	if (owner->state != RESOURCE_X_LOCAL_OWNER_RECYCLING
		|| !pcm_resource_x_local_owner_handle_valid(&owner->handle)
		|| !pcm_resource_x_local_handoff_empty(&owner->handoff))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	result = pcm_resource_x_local_handoff_build_locked(
		&owner->handoff, round, successor_block,
		authenticated_master_node, r4_record_generation,
		cached_ownership_generation, now_us);
	if (result != RESOURCE_X_APPLY_APPLIED)
		return result;
	if (!pcm_resource_x_local_handoff_matches_handle(
			&owner->handoff, &owner->handle)) {
		memset(&owner->handoff, 0, sizeof(owner->handoff));
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	return pcm_resource_x_head_changed_locked(entry, true, now_us)
			   ? RESOURCE_X_APPLY_APPLIED
			   : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
}

static ResourceXApplyResult
pcm_resource_x_local_owner_claim_locked(
	struct GrdEntry *entry, uint8 state,
	const ResourceXAcquisitionRef *ref,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint64 cached_ownership_generation, uint64 reservation_token,
	int32 owner_procno, ResourceXLocalOwnerHandle *handle_out)
{
	ClusterPcmResourceXLocalOwner *owner;

	Assert(entry != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	if (handle_out != NULL)
		memset(handle_out, 0, sizeof(*handle_out));
	if (handle_out == NULL || !pcm_resource_x_ref_valid(ref)
		|| (state != RESOURCE_X_LOCAL_OWNER_RECYCLING && state != RESOURCE_X_LOCAL_OWNER_REVOKING
			&& state != RESOURCE_X_LOCAL_OWNER_EVICTING)
		|| master_session_incarnation == 0 || master_session_incarnation == UINT64_MAX
		|| r4_record_generation == 0 || r4_record_generation == UINT64_MAX
		|| cached_ownership_generation == 0 || cached_ownership_generation == UINT64_MAX
		|| reservation_token == 0 || reservation_token == UINT64_MAX
		|| entry->resource_x_bootstrap_round.head_no_progress_deadline_us == 0
		|| entry->resource_x_bootstrap_round.head_no_progress_deadline_us == UINT64_MAX
		|| owner_procno < 0)
		return RESOURCE_X_APPLY_INVALID;
	owner = &entry->resource_x_local_owner;
	if (!pcm_resource_x_local_owner_valid_locked(owner))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (owner->state != RESOURCE_X_LOCAL_OWNER_EMPTY)
		return RESOURCE_X_APPLY_BAD_STATE;
	if (owner->highest_owner_generation == UINT64_MAX - 1)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (!pcm_resource_x_head_changed_locked(entry, true, pcm_resource_x_monotonic_us()))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	owner->highest_owner_generation++;
	owner->handle.ref = *ref;
	owner->handle.master_session_incarnation
		= master_session_incarnation;
	owner->handle.r4_record_generation = r4_record_generation;
	owner->handle.buffer_ownership_generation
		= cached_ownership_generation;
	owner->handle.reservation_token = reservation_token;
	owner->handle.owner_generation = owner->highest_owner_generation;
	owner->handle.absolute_deadline_us
		= entry->resource_x_bootstrap_round.head_no_progress_deadline_us;
	owner->handle.owner_procno = owner_procno;
	owner->state = state;
	*handle_out = owner->handle;
	return RESOURCE_X_APPLY_APPLIED;
}

static ResourceXApplyResult
pcm_resource_x_local_owner_release_locked(
	struct GrdEntry *entry, uint8 expected_state,
	const ResourceXLocalOwnerHandle *handle)
{
	ClusterPcmResourceXLocalOwner *owner;

	Assert(entry != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	owner = &entry->resource_x_local_owner;
	if (!pcm_resource_x_local_owner_handle_valid(handle)
		|| (expected_state != RESOURCE_X_LOCAL_OWNER_RECYCLING
			&& expected_state != RESOURCE_X_LOCAL_OWNER_REVOKING
			&& expected_state != RESOURCE_X_LOCAL_OWNER_EVICTING))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_resource_x_local_owner_valid_locked(owner))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (owner->state == RESOURCE_X_LOCAL_OWNER_EMPTY)
		return RESOURCE_X_APPLY_NOT_FOUND;
	if (owner->state != expected_state
		|| !pcm_resource_x_local_owner_handle_equal(
			&owner->handle, handle))
		return RESOURCE_X_APPLY_STALE;
	pcm_resource_x_local_owner_clear_locked(entry);
	return RESOURCE_X_APPLY_APPLIED;
}

/* The direct-init BufferDesc sidecar and this Resource-X entry use separate
 * lock domains.  Once the exact post-clear X generation is visible, consume
 * the same round's retained T1/T2 (or exact retired) state under entry_lock
 * and install the terminal cover in the same transaction. */
static bool
pcm_resource_x_bootstrap_round_direct_init_handoff_locked(
	struct GrdEntry *entry, ClusterPcmResourceXBootstrapRound *round,
	uint64 cached_ownership_generation)
{
	ResourceXAcquisitionRef ref;
	PcmResourceXRefClass ref_class;
	PcmResourceXRetirementWitness witness;
	ResourceXApplyResult result;

	Assert(entry != NULL);
	Assert(round != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED
		|| round->install_claim_source != RESOURCE_X_INSTALL_CLAIM_DIRECT_INIT
		|| round->install_claim_reservation_token == 0
		|| round->install_claim_reservation_token == UINT64_MAX
		|| round->install_claim_pending_generation == UINT64_MAX || cached_ownership_generation == 0
		|| cached_ownership_generation == UINT64_MAX
		|| round->install_claim_pending_generation + 1 != cached_ownership_generation
		|| round->request.assertion_sequence == 0 || round->request.assertion_sequence == UINT64_MAX
		|| round->request.assertion_sequence != round->highest_attempt_floor
		|| round->accepted_base == 0 || round->accepted_base >= UINT64_MAX - 1)
		return false;

	memset(&ref, 0, sizeof(ref));
	ref.assertion = round->request.logical_assertion;
	ref.formation = round->resource_formation;
	ref.acquisition_generation = round->request.assertion_sequence;
	if (!pcm_resource_x_ref_valid(&ref))
		return false;
	ref_class = pcm_resource_x_ref_classify_locked(entry, &ref);
	if (ref_class == PCM_RX_REF_ACTIVE_EXACT) {
		if (entry->resource_x_progress_flags
				!= (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1
					| RESOURCE_X_PROGRESS_T2)
			|| entry->resource_x_no_progress_generation != 0
			|| entry->resource_x_no_progress_reason
				!= RESOURCE_X_NO_PROGRESS_NONE)
			return false;
		result = pcm_resource_x_requester_retirement_prepare_locked(
			entry, &ref, true, false, &witness);
		if (result != RESOURCE_X_APPLY_APPLIED
			|| !pcm_resource_x_requester_settlement_arm_locked(
				entry, &ref, &witness))
			return false;
		pcm_resource_x_requester_retirement_commit_locked(
			entry, &ref, &witness);
	}
	else if (ref_class != PCM_RX_REF_RETIRED_EXACT)
		return false;

	if (!pcm_resource_x_active_empty_locked(entry)
		|| entry->resource_x_retired_acquisition_generation
			!= ref.acquisition_generation)
		return false;
	round->terminal_ref = ref;
	round->terminal_authority_generation = round->accepted_base + 1;
	round->cached_ownership_generation = cached_ownership_generation;
	round->phase = RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED;
	if (!pcm_resource_x_head_changed_locked(entry, true, pcm_resource_x_monotonic_us()))
		return false;
	return pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
		entry, round, &ref, round->master_session_incarnation,
		round->r4_record_generation, cached_ownership_generation);
}

static ResourceXBootstrapRoundAction
pcm_resource_x_bootstrap_round_step_internal(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 absolute_deadline_us, uint64 head_no_progress_budget_us, uint64 now_us,
	uint64 retry_slice_us, uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token, bool allow_create, bool cached_local_x,
	uint64 cached_ownership_generation, ResourceXDecodedFrame *dispatch_out,
	ResourceXAcquisitionRef *terminal_ref_out)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXBootstrapRoundAction action;
	ResourceXApplyResult predecessor_result = RESOURCE_X_APPLY_NOT_FOUND;
	uint64 attempt_floor;
	uint64 attempt_sequence;
	uint64 diagnostic_absolute_deadline_us = 0;
	uint64 diagnostic_accepted_base = 0;
	uint64 diagnostic_attempt = 0;
	uint64 diagnostic_last_dispatch_us = 0;
	uint64 diagnostic_r4_record_generation = 0;
	uint64 diagnostic_resource_formation = 0;
	uint64 diagnostic_master_session_incarnation = 0;
	uint32 diagnostic_master_ingress_generation = 0;
	uint32 diagnostic_requester_sender_generation = 0;
	uint8 diagnostic_phase = UINT8_MAX;
	uint8 diagnostic_head_failure_reason = RESOURCE_X_HEAD_FAILURE_NONE;
	int32 diagnostic_master_node = -1;
	bool broadcast = false;
	bool diagnostic_identity_match = false;

	if (dispatch_out == NULL || terminal_ref_out == NULL)
		return RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	memset(dispatch_out, 0, sizeof(*dispatch_out));
	memset(terminal_ref_out, 0, sizeof(*terminal_ref_out));
	if (!resource_x_assertion_valid(assertion) || assertion->requester_node != cluster_node_id
		|| current_master_node < 0 || current_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| resource_formation == 0 || resource_formation == UINT64_MAX
		|| master_session_incarnation == 0 || master_session_incarnation == UINT64_MAX
		|| r4_record_generation == 0 || r4_record_generation == UINT64_MAX
		|| requester_sender_connection_generation == 0
		|| requester_sender_connection_generation == UINT32_MAX
		|| master_ingress_connection_generation == 0
		|| master_ingress_connection_generation == UINT32_MAX || absolute_deadline_us == 0
		|| absolute_deadline_us == UINT64_MAX || now_us == 0 || now_us == UINT64_MAX
		|| now_us >= absolute_deadline_us || retry_slice_us == 0 || retry_slice_us == UINT64_MAX
		|| (direct_init_reservation_token == 0 && direct_init_ownership_generation != 0)
		|| direct_init_ownership_generation >= UINT64_MAX - 1
		|| direct_init_reservation_token == UINT64_MAX
		|| (!cached_local_x && cached_ownership_generation != 0)
		|| (cached_local_x
			&& (cached_ownership_generation == 0 || cached_ownership_generation == UINT64_MAX))
		|| !cluster_pcm_lock_resource_x_gate_open_exact(resource_formation))
		return RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;

	if (!pcm_entry_ref_acquire(&assertion->resource, allow_create,
			&entry_ref, &acquire_result)) {
		if (allow_create
			&& acquire_result == PCM_ENTRY_ACQUIRE_NO_CAPACITY)
			return RESOURCE_X_BOOTSTRAP_ROUND_BACKPRESSURE;
		return RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	}
	entry = entry_ref.entry;

	/* Creator-capable callers are still followers of an existing head.  Check
	 * their key and optional claim before any shared semantic mutation. */
	pgstat_report_wait_start(WAIT_EVENT_PCM_TRANSITION_APPLY);
	LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
	pgstat_report_wait_end();
	round = &entry->resource_x_bootstrap_round;
	if ((!allow_create
		 && (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY
			 || round->phase == RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED))
		|| (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_EMPTY
			&& (!pcm_resource_x_install_claim_valid_locked(round)
				|| !pcm_resource_x_node_request_key_matches_locked(
					round, assertion, current_master_node, resource_formation,
					master_session_incarnation, r4_record_generation,
					requester_sender_connection_generation, master_ingress_connection_generation)
				|| (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
					&& direct_init_reservation_token != 0
					&& round->install_claim_source != RESOURCE_X_INSTALL_CLAIM_NONE
					&& (round->install_claim_pending_generation != direct_init_ownership_generation
						|| round->install_claim_reservation_token
							   != direct_init_reservation_token))))) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	}
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_EMPTY
		&& round->phase <= RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED
		&& direct_init_reservation_token != 0
		&& round->install_claim_source == RESOURCE_X_INSTALL_CLAIM_NONE) {
		/* Only the complete B-E-B binder or the T1 installer can publish the
		 * claim.  A follower's tuple alone cannot make it the physical owner. */
		if (now_us >= round->head_no_progress_deadline_us) {
			pcm_resource_x_semantic_mutation_mark();
			pcm_resource_x_head_fail_locked(entry, RESOURCE_X_HEAD_NO_PROGRESS_EXPIRED);
			action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
			goto round_step_done;
		}
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_BOOTSTRAP_ROUND_WAIT;
	}
	pcm_resource_x_semantic_mutation_mark();
	if (!pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner)) {
		if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_EMPTY)
			pcm_resource_x_head_fail_locked(entry, RESOURCE_X_HEAD_FAILURE_NONE);
		ConditionVariableBroadcast(&entry->wait_cv);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	}
	/* Expiry/cleanup belongs to the existing local owner and transport paths.
	 * A caller joining this head cannot clear another owner's handoff. */
	if (allow_create && !cached_local_x
		&& pcm_resource_x_bootstrap_failed_round_rearmable_locked(
			entry, round, assertion, current_master_node,
			resource_formation, master_session_incarnation,
			r4_record_generation,
			requester_sender_connection_generation,
			master_ingress_connection_generation,
			absolute_deadline_us, now_us, retry_slice_us,
			direct_init_ownership_generation,
			direct_init_reservation_token)) {
		pcm_resource_x_bootstrap_round_clear_binding_locked(round);
		broadcast = true;
	}
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED) {
		if (!pcm_resource_x_node_request_key_matches_locked(
				round, assertion, current_master_node, resource_formation,
				master_session_incarnation, r4_record_generation,
				requester_sender_connection_generation, master_ingress_connection_generation)) {
			action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
		} else if (!cached_local_x) {
			/* BufferDesc and the round are separate lock domains.  A caller may
			 * have sampled the pre-T3 descriptor immediately before T3 published
			 * this cover.  Re-probe instead of destroying exact terminal evidence. */
			action = RESOURCE_X_BOOTSTRAP_ROUND_WAIT;
		} else if (!pcm_resource_x_local_owner_valid_locked(&entry->resource_x_local_owner)) {
			pcm_resource_x_head_fail_locked(entry, RESOURCE_X_HEAD_FAILURE_NONE);
			broadcast = true;
			action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
		} else if (entry->resource_x_local_owner.state != RESOURCE_X_LOCAL_OWNER_EMPTY) {
			if (pcm_resource_x_local_owner_round_exact_locked(entry, round))
				action = RESOURCE_X_BOOTSTRAP_ROUND_WAIT;
			else {
				pcm_resource_x_head_fail_locked(entry, RESOURCE_X_HEAD_FAILURE_NONE);
				broadcast = true;
				action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
			}
		} else if (!pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
					   entry, round, &round->terminal_ref, master_session_incarnation,
					   r4_record_generation, cached_ownership_generation)) {
			/* This caller's physical observation cannot consume the cover.
			 * It is not authority to invalidate the node's retained cover. */
			action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
		} else {
			*terminal_ref_out = round->terminal_ref;
			action = RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL;
		}
	} else if (cached_local_x
			   && pcm_resource_x_request_observation_matches_locked(
				   round, assertion, current_master_node, resource_formation,
				   master_session_incarnation, r4_record_generation,
				   requester_sender_connection_generation, master_ingress_connection_generation,
				   direct_init_ownership_generation, direct_init_reservation_token)
			   && pcm_resource_x_bootstrap_round_direct_init_handoff_locked(
				   entry, round, cached_ownership_generation)) {
		*terminal_ref_out = round->terminal_ref;
		broadcast = true;
		action = RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL;
	} else if (cached_local_x) {
		action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	} else if (!cluster_pcm_lock_resource_x_gate_open_exact(resource_formation)) {
		if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_EMPTY
			&& round->phase != RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED) {
			pcm_resource_x_head_fail_locked(entry, RESOURCE_X_HEAD_FAILURE_NONE);
			broadcast = true;
		}
		action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	} else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY
			   && (predecessor_result = pcm_resource_x_holder_pair_precedes_local_bootstrap_locked(
					   entry, current_master_node, master_session_incarnation, resource_formation,
					   0))
					  == RESOURCE_X_APPLY_APPLIED) {
		/* The already-published holder pair belongs to an earlier canonical
		 * conversion.  Keep this requester round byte-empty until its exact
		 * SourceSettlement closes; no attempt, base, or master priority is
		 * created by this wait. */
		action = RESOURCE_X_BOOTSTRAP_ROUND_PREDECESSOR_WAIT;
	} else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY
			   && predecessor_result != RESOURCE_X_APPLY_NOT_FOUND) {
		action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	} else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY) {
		if (now_us >= absolute_deadline_us || head_no_progress_budget_us == 0
			|| head_no_progress_budget_us == UINT64_MAX
			|| now_us >= UINT64_MAX - head_no_progress_budget_us) {
			action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
		} else {
			attempt_floor = round->highest_attempt_floor;
			if (attempt_floor
				< entry->resource_x_retired_acquisition_generation)
				attempt_floor
					= entry->resource_x_retired_acquisition_generation;
			if (!pcm_resource_x_bootstrap_attempt_allocate(
					attempt_floor, &attempt_sequence)) {
				action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
			} else {
				memset(round, 0, sizeof(*round));
				round->highest_attempt_floor = attempt_sequence;
				round->resource_formation = resource_formation;
				round->master_session_incarnation
					= master_session_incarnation;
				round->r4_record_generation = r4_record_generation;
				round->head_no_progress_budget_us = head_no_progress_budget_us;
				round->last_dispatch_us = now_us;
				round->retry_slice_us = retry_slice_us;
				round->current_master_node = current_master_node;
				round->requester_sender_connection_generation
					= requester_sender_connection_generation;
				round->master_ingress_connection_generation
					= master_ingress_connection_generation;
				round->install_claim_pending_generation = direct_init_ownership_generation;
				round->install_claim_reservation_token = direct_init_reservation_token;
				round->install_claim_source = direct_init_reservation_token == 0
												  ? RESOURCE_X_INSTALL_CLAIM_NONE
												  : RESOURCE_X_INSTALL_CLAIM_DIRECT_INIT;
				round->request.logical_assertion = *assertion;
				round->request.resource_formation = resource_formation;
				round->request.master_session_incarnation
					= master_session_incarnation;
				round->request.assertion_sequence
					= round->highest_attempt_floor;
				round->request.action_node = assertion->requester_node;
				round->request.observed_mode = (uint8)PCM_STATE_N;
				round->request.target_mode = (uint8)PCM_STATE_X;
				round->request.sender_connection_generation
					= requester_sender_connection_generation;
				round->request.outcome = RESOURCE_X_OUTCOME_NONE;
				round->phase
					= RESOURCE_X_BOOTSTRAP_ROUND_REQUEST_DISPATCHED;
				(void)pcm_resource_x_head_changed_locked(entry, true, now_us);
				pcm_resource_x_bootstrap_round_dispatch_snapshot(
					round, dispatch_out);
				action = RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST;
			}
		}
	} else if (!pcm_resource_x_request_observation_matches_locked(
				   round, assertion, current_master_node, resource_formation,
				   master_session_incarnation, r4_record_generation,
				   requester_sender_connection_generation, master_ingress_connection_generation,
				   direct_init_ownership_generation, direct_init_reservation_token)) {
		action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	} else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED
			   || now_us >= round->head_no_progress_deadline_us) {
		if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED) {
			pcm_resource_x_head_fail_locked(entry, RESOURCE_X_HEAD_NO_PROGRESS_EXPIRED);
			broadcast = true;
		}
		action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	} else if (now_us < round->last_dispatch_us) {
		/* now_us is sampled before entry_lock.  An exact ACK can advance this
		 * same round and last_dispatch_us before the caller enters the critical
		 * section.  Preserve that newer progress and let the caller resample;
		 * the original absolute deadline and attempt remain unchanged. */
		action = RESOURCE_X_BOOTSTRAP_ROUND_WAIT;
	} else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_BASE_BOUND) {
		action = RESOURCE_X_BOOTSTRAP_ROUND_WAIT;
	} else if (now_us - round->last_dispatch_us < round->retry_slice_us) {
		action = RESOURCE_X_BOOTSTRAP_ROUND_WAIT;
	} else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_REQUEST_DISPATCHED) {
		round->last_dispatch_us = now_us;
		pcm_resource_x_bootstrap_round_dispatch_snapshot(round, dispatch_out);
		action = RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST;
	} else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED) {
		round->last_dispatch_us = now_us;
		pcm_resource_x_bootstrap_round_assertion_snapshot(
			round, dispatch_out);
		action = RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT;
	} else {
		action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	}

round_step_done:
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	if (action == RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED) {
		diagnostic_phase = round->phase;
		diagnostic_head_failure_reason = round->head_failure_reason;
		diagnostic_attempt = round->request.assertion_sequence;
		diagnostic_accepted_base = round->accepted_base;
		diagnostic_r4_record_generation = round->r4_record_generation;
		diagnostic_resource_formation = round->resource_formation;
		diagnostic_master_session_incarnation
			= round->master_session_incarnation;
		diagnostic_last_dispatch_us = round->last_dispatch_us;
		diagnostic_absolute_deadline_us = round->head_no_progress_deadline_us;
		diagnostic_master_node = round->current_master_node;
		diagnostic_requester_sender_generation
			= round->requester_sender_connection_generation;
		diagnostic_master_ingress_generation
			= round->master_ingress_connection_generation;
		diagnostic_identity_match = pcm_resource_x_request_observation_matches_locked(
			round, assertion, current_master_node, resource_formation, master_session_incarnation,
			r4_record_generation, requester_sender_connection_generation,
			master_ingress_connection_generation, direct_init_ownership_generation,
			direct_init_reservation_token);
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	if (action == RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED)
		ereport(
			LOG,
			(errmsg_internal("Resource-X bootstrap round diagnostic"),
			 errdetail(
				 "phase=%u identity_match=%s cached_local_x=%s "
				 "cached_generation=%llu caller_master=%d round_master=%d "
				 "caller_formation=%llu round_formation=%llu "
				 "caller_session=%llu round_session=%llu "
				 "caller_r4_generation=%llu round_r4_generation=%llu "
				 "attempt=%llu accepted_base=%llu "
				 "caller_requester_connection=%u round_requester_connection=%u "
				 "caller_master_connection=%u round_master_connection=%u "
				 "now=%llu last_dispatch=%llu caller_deadline=%llu round_deadline=%llu "
				 "head_failure_reason=%s",
				 (unsigned)diagnostic_phase, diagnostic_identity_match ? "true" : "false",
				 cached_local_x ? "true" : "false", (unsigned long long)cached_ownership_generation,
				 current_master_node, diagnostic_master_node,
				 (unsigned long long)resource_formation,
				 (unsigned long long)diagnostic_resource_formation,
				 (unsigned long long)master_session_incarnation,
				 (unsigned long long)diagnostic_master_session_incarnation,
				 (unsigned long long)r4_record_generation,
				 (unsigned long long)diagnostic_r4_record_generation,
				 (unsigned long long)diagnostic_attempt,
				 (unsigned long long)diagnostic_accepted_base,
				 requester_sender_connection_generation, diagnostic_requester_sender_generation,
				 master_ingress_connection_generation, diagnostic_master_ingress_generation,
				 (unsigned long long)now_us, (unsigned long long)diagnostic_last_dispatch_us,
				 (unsigned long long)absolute_deadline_us,
				 (unsigned long long)diagnostic_absolute_deadline_us,
				 cluster_pcm_lock_resource_x_head_failure_name(diagnostic_head_failure_reason))));
	return action;
}

ResourceXBootstrapRoundAction
cluster_pcm_lock_resource_x_bootstrap_round_step_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 absolute_deadline_us, uint64 head_no_progress_budget_us, uint64 now_us,
	uint64 retry_slice_us, bool cached_local_x, uint64 cached_ownership_generation,
	ResourceXDecodedFrame *dispatch_out, ResourceXAcquisitionRef *terminal_ref_out)
{
	return pcm_resource_x_bootstrap_round_step_internal(
		assertion, current_master_node, resource_formation, master_session_incarnation,
		r4_record_generation, requester_sender_connection_generation,
		master_ingress_connection_generation, absolute_deadline_us, head_no_progress_budget_us,
		now_us, retry_slice_us, 0, 0, true, cached_local_x, cached_ownership_generation,
		dispatch_out, terminal_ref_out);
}

ResourceXBootstrapRoundAction
cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 absolute_deadline_us, uint64 head_no_progress_budget_us, uint64 now_us,
	uint64 retry_slice_us, uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token, bool cached_local_x, uint64 cached_ownership_generation,
	ResourceXDecodedFrame *dispatch_out, ResourceXAcquisitionRef *terminal_ref_out)
{
	if (direct_init_reservation_token == 0)
		return RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	return pcm_resource_x_bootstrap_round_step_internal(
		assertion, current_master_node, resource_formation, master_session_incarnation,
		r4_record_generation, requester_sender_connection_generation,
		master_ingress_connection_generation, absolute_deadline_us, head_no_progress_budget_us,
		now_us, retry_slice_us, direct_init_ownership_generation, direct_init_reservation_token,
		true, cached_local_x, cached_ownership_generation, dispatch_out, terminal_ref_out);
}

/* Snapshot only the current node head's admission and progress lease.  This is a
 * read-only admission probe: an EMPTY/failed/expired or inexact round cannot
 * allocate an attempt, clear a binding, advance a phase, or mint a deadline.
 * The observed head lease is never a caller budget; terminal has no expiry. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_direct_init_join_budget_exact(
	const ResourceXAssertion *assertion,
	uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token,
	uint64 now_us,
	uint64 *r4_record_generation_out,
	uint64 *absolute_deadline_us_out)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;

	if (r4_record_generation_out != NULL)
		*r4_record_generation_out = 0;
	if (absolute_deadline_us_out != NULL)
		*absolute_deadline_us_out = 0;
	if (!resource_x_assertion_valid(assertion)
		|| assertion->requester_node != cluster_node_id
		|| direct_init_ownership_generation == UINT64_MAX
		|| direct_init_reservation_token == 0
		|| direct_init_reservation_token == UINT64_MAX
		|| now_us == 0 || now_us == UINT64_MAX
		|| r4_record_generation_out == NULL
		|| absolute_deadline_us_out == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY)
		result = RESOURCE_X_APPLY_NOT_FOUND;
	else if (!resource_x_assertion_equal(&round->request.logical_assertion, assertion)
			 || (round->install_claim_source != RESOURCE_X_INSTALL_CLAIM_NONE
				 && round->phase != RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
				 && (round->install_claim_pending_generation != direct_init_ownership_generation
					 || round->install_claim_reservation_token != direct_init_reservation_token)))
		result = RESOURCE_X_APPLY_STALE;
	else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED
			 || round->r4_record_generation == 0 || round->r4_record_generation == UINT64_MAX
			 || round->head_no_progress_deadline_us == 0
			 || round->head_no_progress_deadline_us == UINT64_MAX
			 || (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
				 && now_us >= round->head_no_progress_deadline_us))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_REQUEST_DISPATCHED
			 && round->phase != RESOURCE_X_BOOTSTRAP_ROUND_BASE_BOUND
			 && round->phase != RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED
			 && round->phase != RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else {
		*r4_record_generation_out = round->r4_record_generation;
		*absolute_deadline_us_out = round->head_no_progress_deadline_us;
		result = RESOURCE_X_APPLY_APPLIED;
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXBootstrapRoundAction
cluster_pcm_lock_resource_x_bootstrap_round_step_direct_init_join_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 absolute_deadline_us, uint64 head_no_progress_budget_us, uint64 now_us,
	uint64 retry_slice_us, uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token, bool cached_local_x, uint64 cached_ownership_generation,
	ResourceXDecodedFrame *dispatch_out, ResourceXAcquisitionRef *terminal_ref_out)
{
	if (direct_init_reservation_token == 0)
		return RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	return pcm_resource_x_bootstrap_round_step_internal(
		assertion, current_master_node, resource_formation, master_session_incarnation,
		r4_record_generation, requester_sender_connection_generation,
		master_ingress_connection_generation, absolute_deadline_us, head_no_progress_budget_us,
		now_us, retry_slice_us, direct_init_ownership_generation, direct_init_reservation_token,
		false, cached_local_x, cached_ownership_generation, dispatch_out, terminal_ref_out);
}

/* A dispatch-side authority recheck can fail while the requester-local kind-9
 * round remains pre-ACK/pre-ASSERT.  Its request may already have installed a
 * master RECEIVED receipt, but that receipt is explicitly non-authority and a
 * same-requester higher attempt may replace it before ASSERT consumption.
 * Clear only a byte-exact REQUEST_DISPATCHED binding so the caller recaptures
 * current authority under the same absolute deadline; retain the monotonic
 * attempt floor and reject every post-ACK or inexact shape. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_discard_pre_assert_authority_drift_exact(
	const ResourceXDecodedFrame *request, int32 expected_master_node,
	uint64 expected_r4_record_generation,
	uint32 expected_master_ingress_connection_generation,
	uint64 expected_retry_slice_us, uint64 expected_absolute_deadline_us,
	uint64 expected_direct_init_ownership_generation,
	uint64 expected_direct_init_reservation_token)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	bool broadcast = false;

	if (request == NULL
		|| request->kind != RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP
		|| request->payload_bytes != RESOURCE_X_CONTROL_V1_BYTES
		|| request->blocked_has_remote_proof
		|| !resource_x_assertion_valid(
			&request->common.logical_assertion)
		|| request->common.logical_assertion.requester_node
			!= cluster_node_id
		|| expected_master_node < 0
		|| expected_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| expected_r4_record_generation == 0
		|| expected_r4_record_generation == UINT64_MAX
		|| expected_master_ingress_connection_generation == 0
		|| expected_master_ingress_connection_generation == UINT32_MAX
		|| expected_retry_slice_us == 0
		|| expected_retry_slice_us == UINT64_MAX
		|| expected_absolute_deadline_us == 0
		|| expected_absolute_deadline_us == UINT64_MAX
		|| expected_direct_init_ownership_generation == UINT64_MAX
		|| expected_direct_init_reservation_token == UINT64_MAX
		|| (expected_direct_init_reservation_token == 0
			&& expected_direct_init_ownership_generation != 0))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(
			&request->common.logical_assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY)
		result = RESOURCE_X_APPLY_NOT_FOUND;
	else if (!pcm_resource_x_common_equal(&round->request, &request->common)
			 || !pcm_resource_x_request_observation_matches_locked(
				 round, &request->common.logical_assertion, expected_master_node,
				 request->common.resource_formation, request->common.master_session_incarnation,
				 expected_r4_record_generation, request->common.sender_connection_generation,
				 expected_master_ingress_connection_generation,
				 expected_direct_init_ownership_generation, expected_direct_init_reservation_token)
			 || round->request.assertion_sequence != round->highest_attempt_floor)
		result = RESOURCE_X_APPLY_STALE;
	else if (round->phase
			 != RESOURCE_X_BOOTSTRAP_ROUND_REQUEST_DISPATCHED)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (!pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (entry->resource_x_local_owner.state != RESOURCE_X_LOCAL_OWNER_EMPTY
			 || round->install_claim_source != RESOURCE_X_INSTALL_CLAIM_NONE
			 || !pcm_resource_x_active_empty_locked(entry))
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (round->accepted_base != 0
			 || round->terminal_authority_generation != 0
			 || round->cached_ownership_generation != 0
			 || round->ack.assertion_sequence != 0
			 || round->assertion.assertion_sequence != 0
			 || pcm_resource_x_ref_valid(&round->terminal_ref))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else {
		pcm_resource_x_bootstrap_round_clear_binding_locked(round);
		broadcast = true;
		result = RESOURCE_X_APPLY_APPLIED;
	}
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXBootstrapRoundAction
cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
	const ResourceXDecodedFrame *ack, int32 authenticated_master_node,
	uint32 authenticated_ingress_connection_generation,
	uint64 r4_record_generation, uint64 now_us,
	ResourceXDecodedFrame *assertion_out)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXBootstrapRoundAction action;
	bool broadcast = false;

	if (assertion_out == NULL)
		return RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	memset(assertion_out, 0, sizeof(*assertion_out));
	if (ack == NULL
		|| !resource_x_assertion_valid(&ack->common.logical_assertion)
		|| authenticated_master_node < 0
		|| authenticated_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| authenticated_ingress_connection_generation == 0
		|| authenticated_ingress_connection_generation == UINT32_MAX
		|| r4_record_generation == 0
		|| r4_record_generation == UINT64_MAX
		|| now_us == 0 || now_us == UINT64_MAX)
		return RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	if (!pcm_entry_ref_acquire(
			&ack->common.logical_assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY
		|| round->phase == RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED
		|| !pcm_resource_x_bootstrap_round_ack_matches_request(ack, round))
		action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	else if (authenticated_master_node != round->current_master_node
			 || authenticated_ingress_connection_generation
					!= round->master_ingress_connection_generation
			 || r4_record_generation != round->r4_record_generation
			 || !cluster_pcm_lock_resource_x_gate_open_exact(round->resource_formation)
			 || (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
				 && now_us >= round->head_no_progress_deadline_us)) {
		pcm_resource_x_head_fail_locked(entry, now_us >= round->head_no_progress_deadline_us
												   ? RESOURCE_X_HEAD_NO_PROGRESS_EXPIRED
												   : RESOURCE_X_HEAD_FAILURE_NONE);
		broadcast = true;
		action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	} else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_REQUEST_DISPATCHED) {
		pcm_resource_x_common_copy(&round->ack, &ack->common);
		round->accepted_base = ack->common.base_authority_generation;
		pcm_resource_x_common_copy(&round->assertion, &round->request);
		round->assertion.base_authority_generation = round->accepted_base;
		round->assertion.authority_generation = round->accepted_base;
		round->assertion.outcome = RESOURCE_X_OUTCOME_NONE;
		round->assertion.semantic_crc32c = 0;
		round->phase = RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED;
		round->last_dispatch_us = Max(round->last_dispatch_us, now_us);
		if (!pcm_resource_x_head_changed_locked(entry, true, now_us)) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
		}
		pcm_resource_x_bootstrap_round_assertion_snapshot(
			round, assertion_out);
		broadcast = true;
		action = RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT;
	} else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED
			   && pcm_resource_x_common_equal(&round->ack, &ack->common))
		action = RESOURCE_X_BOOTSTRAP_ROUND_WAIT;
	else
		action = RESOURCE_X_BOOTSTRAP_ROUND_FAIL_CLOSED;
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return action;
}

/* Complete one bounded follower wait on the same per-resource CV used by the
 * requester round.  Registration precedes the predicate lock, so an ACK/T3
 * broadcast cannot be lost between the exact recheck and the sleep. */
static ResourceXApplyResult
pcm_resource_x_bootstrap_round_wait_internal(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 retry_slice_us, uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token, uint64 caller_absolute_deadline_us, long timeout_ms)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryWaitContext wait_context;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	uint64 observed_head_change_generation;
	uint64 observed_attempt;
	uint64 effective_deadline_us;
	uint64 remaining_us;
	uint64 now_us;

	if (!resource_x_assertion_valid(assertion) || current_master_node < 0
		|| current_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT || resource_formation == 0
		|| resource_formation == UINT64_MAX || master_session_incarnation == 0
		|| master_session_incarnation == UINT64_MAX || r4_record_generation == 0
		|| r4_record_generation == UINT64_MAX || requester_sender_connection_generation == 0
		|| requester_sender_connection_generation == UINT32_MAX
		|| master_ingress_connection_generation == 0
		|| master_ingress_connection_generation == UINT32_MAX || retry_slice_us == 0
		|| retry_slice_us == UINT64_MAX
		|| (direct_init_reservation_token == 0 && direct_init_ownership_generation != 0)
		|| direct_init_ownership_generation == UINT64_MAX
		|| direct_init_reservation_token == UINT64_MAX || caller_absolute_deadline_us == 0
		|| caller_absolute_deadline_us == UINT64_MAX || timeout_ms <= 0)
		return RESOURCE_X_APPLY_INVALID;
	memset(&wait_context, 0, sizeof(wait_context));
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&wait_context.ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = wait_context.ref.entry;

	/* This stack-only observation owns no authority.  Pin the binding, sample
	 * its wait predicate, then enroll and compare before any timed sleep. */
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	observed_head_change_generation = entry->resource_x_bootstrap_round.head_change_generation;
	observed_attempt = entry->resource_x_bootstrap_round.request.assertion_sequence;
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_wait_ref_begin(&wait_context);
	ConditionVariablePrepareToSleep(&entry->wait_cv);
	wait_context.cv_prepared = true;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED) {
		/* The retained cover is node-level authority.  An ordinary caller has
		 * no creation-only direct-init token, so use the same relaxed terminal
		 * identity as step_exact while it re-probes the BufferDesc domain. */
		bool terminal_identity_matches = pcm_resource_x_node_request_key_matches_locked(
			round, assertion, current_master_node, resource_formation, master_session_incarnation,
			r4_record_generation, requester_sender_connection_generation,
			master_ingress_connection_generation);

		if (!terminal_identity_matches
			|| !pcm_resource_x_local_owner_valid_locked(
				&entry->resource_x_local_owner))
			result = RESOURCE_X_APPLY_STALE;
		else if (entry->resource_x_local_owner.state
				 != RESOURCE_X_LOCAL_OWNER_EMPTY)
			result = pcm_resource_x_local_owner_round_exact_locked(entry, round)
				? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_STALE;
		else
			result = RESOURCE_X_APPLY_DUPLICATE;
	}
	else if (direct_init_ownership_generation == 0
			 && direct_init_reservation_token == 0
			 && pcm_resource_x_s_predecessor_superseded_unbound_wait_locked(
				entry, assertion, current_master_node,
				master_session_incarnation, resource_formation)) {
		/* Retaining an exact remote-S predecessor may atomically cancel the
		 * non-authoritative REQUEST_DISPATCHED round after its caller selected
		 * WAIT but before this predicate recheck.  The old wait has then been
		 * superseded, not failed: return immediately so the caller re-enters
		 * step_exact under its original absolute deadline and observes
		 * PREDECESSOR_WAIT.  No pair, direct-init round, bound base or owner is
		 * admitted through this exception. */
		result = RESOURCE_X_APPLY_DUPLICATE;
	} else if (!pcm_resource_x_node_request_key_matches_locked(
				   round, assertion, current_master_node, resource_formation,
				   master_session_incarnation, r4_record_generation,
				   requester_sender_connection_generation, master_ingress_connection_generation)
			   || (round->install_claim_source != RESOURCE_X_INSTALL_CLAIM_NONE
				   && direct_init_reservation_token != 0
				   && (round->install_claim_pending_generation != direct_init_ownership_generation
					   || round->install_claim_reservation_token
							  != direct_init_reservation_token))) {
		result = RESOURCE_X_APPLY_STALE;
	} else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_REQUEST_DISPATCHED
			   || round->phase == RESOURCE_X_BOOTSTRAP_ROUND_BASE_BOUND
			   || round->phase == RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED)
		result = RESOURCE_X_APPLY_APPLIED;
	else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED)
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else
		result = RESOURCE_X_APPLY_BAD_STATE;
	now_us = pcm_resource_x_monotonic_us();
	if ((result == RESOURCE_X_APPLY_APPLIED || result == RESOURCE_X_APPLY_DUPLICATE)
		&& now_us >= caller_absolute_deadline_us)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (result == RESOURCE_X_APPLY_APPLIED
			 && (observed_head_change_generation != round->head_change_generation
				 || observed_attempt != round->request.assertion_sequence))
		result = RESOURCE_X_APPLY_DUPLICATE;
	effective_deadline_us = caller_absolute_deadline_us;
	if (round->phase >= RESOURCE_X_BOOTSTRAP_ROUND_REQUEST_DISPATCHED
		&& round->phase <= RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED)
		effective_deadline_us = Min(effective_deadline_us, round->head_no_progress_deadline_us);
	if (result == RESOURCE_X_APPLY_APPLIED && now_us >= effective_deadline_us)
		/* Only the exact drive step may expire the shared head. */
		result = RESOURCE_X_APPLY_DUPLICATE;
	LWLockRelease(&entry->entry_lock.lock);
	if (result != RESOURCE_X_APPLY_APPLIED) {
		pcm_entry_wait_context_cleanup(&wait_context);
		return result;
	}
	remaining_us = Min(effective_deadline_us - now_us, retry_slice_us);
	if (remaining_us < UINT64_C(1000)) {
		pcm_entry_wait_context_cleanup(&wait_context);
		return RESOURCE_X_APPLY_APPLIED;
	}
	timeout_ms = (long)Min((uint64)timeout_ms, remaining_us / UINT64_C(1000));

	PG_TRY();
	{
		(void)ConditionVariableTimedSleep(&entry->wait_cv, timeout_ms,
			WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
		if (!pcm_entry_ref_identity_exact(&wait_context.ref)) {
			pcm_resource_x_reconfig_block();
			ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Resource-X bootstrap waiter entry identity changed "
						"after wakeup")));
		}
	}
	PG_FINALLY();
	{
		pcm_entry_wait_context_cleanup(&wait_context);
	}
	PG_END_TRY();
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_wait_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 retry_slice_us, uint64 caller_absolute_deadline_us, long timeout_ms)
{
	return pcm_resource_x_bootstrap_round_wait_internal(
		assertion, current_master_node, resource_formation, master_session_incarnation,
		r4_record_generation, requester_sender_connection_generation,
		master_ingress_connection_generation, retry_slice_us, 0, 0, caller_absolute_deadline_us,
		timeout_ms);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_wait_direct_init_exact(
	const ResourceXAssertion *assertion, int32 current_master_node, uint64 resource_formation,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint32 requester_sender_connection_generation, uint32 master_ingress_connection_generation,
	uint64 retry_slice_us, uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token, uint64 caller_absolute_deadline_us, long timeout_ms)
{
	if (direct_init_reservation_token == 0)
		return RESOURCE_X_APPLY_INVALID;
	return pcm_resource_x_bootstrap_round_wait_internal(
		assertion, current_master_node, resource_formation, master_session_incarnation,
		r4_record_generation, requester_sender_connection_generation,
		master_ingress_connection_generation, retry_slice_us, direct_init_ownership_generation,
		direct_init_reservation_token, caller_absolute_deadline_us, timeout_ms);
}

/* A predecessor has its own exact settlement identity, not a requester head.
 * Pin the entry, capture that identity, enroll, then recheck both the existing
 * predicate and the captured bytes.  Wake/expiry only requests a fresh probe. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_predecessor_wait_exact(const BufferTag *tag, int32 current_master_node,
												   uint64 current_master_session,
												   uint64 current_formation,
												   uint64 expected_carrier_generation,
												   uint64 caller_absolute_deadline_us,
												   uint64 requested_sleep_slice_us)
{
	ClusterPcmResourceXHolderStatus observed_status;
	ClusterPcmResourceXHolderImage observed_image;
	ClusterPcmResourceXMasterState *master_state;
	PcmEntryWaitContext wait_context;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	uint64 now_us;
	uint64 remaining_us;
	long timeout_ms;

	if (tag == NULL || current_master_node < 0
		|| current_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT || current_master_session == 0
		|| current_master_session == UINT64_MAX || current_formation == 0
		|| current_formation == UINT64_MAX || expected_carrier_generation == UINT64_MAX
		|| caller_absolute_deadline_us == 0 || caller_absolute_deadline_us == UINT64_MAX
		|| requested_sleep_slice_us == 0 || requested_sleep_slice_us == UINT64_MAX)
		return RESOURCE_X_APPLY_INVALID;
	memset(&wait_context, 0, sizeof(wait_context));
	if (!pcm_entry_ref_acquire(tag, false, &wait_context.ref, &acquire_result))
		return RESOURCE_X_APPLY_DUPLICATE;
	entry = wait_context.ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	result = pcm_resource_x_holder_pair_precedes_local_bootstrap_locked(
		entry, current_master_node, current_master_session, current_formation,
		expected_carrier_generation);
	if (result == RESOURCE_X_APPLY_APPLIED) {
		master_state = pcm_resource_x_master_state_for_entry(entry);
		observed_status = master_state->holder_status;
		observed_image = master_state->holder_image;
	}
	LWLockRelease(&entry->entry_lock.lock);
	if (result != RESOURCE_X_APPLY_APPLIED) {
		pcm_entry_ref_release(&wait_context.ref);
		return result == RESOURCE_X_APPLY_NOT_FOUND ? RESOURCE_X_APPLY_DUPLICATE : result;
	}
	pcm_entry_wait_ref_begin(&wait_context);
	ConditionVariablePrepareToSleep(&entry->wait_cv);
	wait_context.cv_prepared = true;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	result = pcm_resource_x_holder_pair_precedes_local_bootstrap_locked(
		entry, current_master_node, current_master_session, current_formation,
		expected_carrier_generation);
	master_state = pcm_resource_x_master_state_for_entry(entry);
	if (result == RESOURCE_X_APPLY_NOT_FOUND)
		result = RESOURCE_X_APPLY_DUPLICATE;
	else if (result == RESOURCE_X_APPLY_APPLIED
			 && (memcmp(&observed_status, &master_state->holder_status, sizeof(observed_status))
					 != 0
				 || memcmp(&observed_image, &master_state->holder_image, sizeof(observed_image))
						!= 0))
		result = RESOURCE_X_APPLY_DUPLICATE;
	if (result == RESOURCE_X_APPLY_APPLIED && expected_carrier_generation == 0
		&& entry->resource_x_bootstrap_round.phase != RESOURCE_X_BOOTSTRAP_ROUND_EMPTY)
		result = RESOURCE_X_APPLY_DUPLICATE;
	LWLockRelease(&entry->entry_lock.lock);
	now_us = pcm_resource_x_monotonic_us();
	if ((result == RESOURCE_X_APPLY_APPLIED || result == RESOURCE_X_APPLY_DUPLICATE)
		&& now_us >= caller_absolute_deadline_us)
		result = RESOURCE_X_APPLY_BAD_STATE;
	if (result != RESOURCE_X_APPLY_APPLIED) {
		pcm_entry_wait_context_cleanup(&wait_context);
		return result;
	}
	remaining_us = Min(caller_absolute_deadline_us - now_us, requested_sleep_slice_us);
	if (remaining_us < UINT64_C(1000)) {
		pcm_entry_wait_context_cleanup(&wait_context);
		return RESOURCE_X_APPLY_APPLIED;
	}
	timeout_ms = (long)Min((uint64)LONG_MAX, remaining_us / UINT64_C(1000));
	PG_TRY();
	{
		(void)ConditionVariableTimedSleep(&entry->wait_cv, timeout_ms,
										  WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
		if (!pcm_entry_ref_identity_exact(&wait_context.ref)) {
			pcm_resource_x_reconfig_block();
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("Resource-X predecessor waiter entry identity changed after wakeup")));
		}
	}
	PG_FINALLY();
	{
		pcm_entry_wait_context_cleanup(&wait_context);
	}
	PG_END_TRY();
	return RESOURCE_X_APPLY_APPLIED;
}

/* The BufferDesc and requester round are separate lock domains.  A direct
 * initializer may therefore observe the exact post-commit X sidecar before
 * T3 clears its activation fields.  This predicate grants no authority: it
 * only proves that the observed sidecar is the in-flight product of this
 * exact ASSERT-dispatched direct-init round, so the requester may keep
 * waiting on the existing per-resource CV. */
bool
cluster_pcm_lock_resource_x_bootstrap_round_direct_init_inflight_exact(
	const ResourceXAssertion *assertion, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation,
	uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation,
	uint64 retry_slice_us,
	uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token,
	const ClusterPcmOwnSnapshot *observed)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	uint64 committed_generation;
	bool phase_matches = false;
	bool matches = false;

	if (!resource_x_assertion_valid(assertion)
		|| assertion->requester_node != cluster_node_id
		|| direct_init_ownership_generation == UINT64_MAX
		|| direct_init_reservation_token == 0
		|| direct_init_reservation_token == UINT64_MAX
		|| observed == NULL)
		return false;
	committed_generation = direct_init_ownership_generation + 1;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return false;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	if (pcm_resource_x_request_observation_matches_locked(
			round, assertion, current_master_node, resource_formation, master_session_incarnation,
			r4_record_generation, requester_sender_connection_generation,
			master_ingress_connection_generation, direct_init_ownership_generation,
			direct_init_reservation_token)) {
		phase_matches = round->phase
			== RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED;
		if (!phase_matches
			&& round->phase
				== RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED)
			phase_matches
				= pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
					entry, round, &round->terminal_ref,
					master_session_incarnation, r4_record_generation,
					committed_generation);
	}
	matches = phase_matches
		&& round->request.assertion_sequence != 0
		&& round->request.assertion_sequence != UINT64_MAX
		&& round->request.assertion_sequence == round->highest_attempt_floor
		&& BufferTagsEqual(&observed->tag, &assertion->resource)
		&& observed->generation == committed_generation
		&& observed->reservation_token == direct_init_reservation_token
		&& observed->pcm_state == (uint8)PCM_STATE_X
		&& observed->flags == 0
		&& observed->writer_activation_token
			== direct_init_reservation_token
		&& (observed->resource_x_activation_generation == 0
			|| observed->resource_x_activation_generation
				== round->request.assertion_sequence);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return matches;
}

static bool
pcm_resource_x_bootstrap_round_target_install_inflight_locked(
	struct GrdEntry *entry, ClusterPcmResourceXBootstrapRound *round,
	const ResourceXAssertion *assertion, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation,
	uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation,
	uint64 retry_slice_us,
	const ClusterPcmOwnSnapshot *observed)
{
	ResourceXAcquisitionRef ref;
	PcmResourceXRefClass ref_class;
	uint64 attempt;
	uint32 progress_flags;
	bool direct_init_generation_exact;
	bool direct_init_round;
	bool executor_retry_state_exact;
	bool identity_exact;
	bool n_installing;
	bool phase_exact;
	bool x_activation_cleared;
	bool x_activation_fenced;
	bool x_activation_fenced_phase;

	Assert(entry != NULL);
	Assert(round != NULL);
	if (!resource_x_assertion_valid(assertion)
		|| assertion->requester_node != cluster_node_id
		|| observed == NULL
		|| !BufferTagsEqual(&observed->tag, &assertion->resource)
		|| observed->generation == UINT64_MAX
		|| observed->reservation_token == 0
		|| observed->reservation_token == UINT64_MAX)
		return false;
	memset(&ref, 0, sizeof(ref));
	ref.assertion = round->request.logical_assertion;
	ref.formation = round->resource_formation;
	ref.acquisition_generation = round->request.assertion_sequence;
	attempt = round->request.assertion_sequence;
	progress_flags = entry->resource_x_progress_flags;
	direct_init_round = round->install_claim_reservation_token != 0
						&& round->install_claim_reservation_token != UINT64_MAX
						&& round->install_claim_pending_generation != UINT64_MAX;
	direct_init_generation_exact
		= direct_init_round && observed->reservation_token == round->install_claim_reservation_token
		  && ((observed->pcm_state == (uint8)PCM_STATE_N
			   && observed->generation == round->install_claim_pending_generation)
			  || (observed->pcm_state == (uint8)PCM_STATE_X
				  && round->install_claim_pending_generation != UINT64_MAX
				  && observed->generation == round->install_claim_pending_generation + 1));
	identity_exact
		= pcm_resource_x_node_request_key_matches_locked(
			  round, assertion, current_master_node, resource_formation, master_session_incarnation,
			  r4_record_generation, requester_sender_connection_generation,
			  master_ingress_connection_generation)
		  && (!direct_init_round || direct_init_generation_exact);
	n_installing = observed->pcm_state == (uint8)PCM_STATE_N
		&& observed->flags == PCM_OWN_FLAG_GRANT_PENDING
		&& observed->writer_activation_token == 0
		&& observed->resource_x_activation_generation == 0;
	x_activation_fenced = observed->generation != 0
		&& observed->pcm_state == (uint8)PCM_STATE_X
		&& observed->flags == 0
		&& observed->writer_activation_token == observed->reservation_token
		&& (observed->resource_x_activation_generation == 0
			|| observed->resource_x_activation_generation == attempt);
	x_activation_cleared = observed->generation != 0
		&& observed->pcm_state == (uint8)PCM_STATE_X
		&& observed->flags == 0
		&& observed->writer_activation_token == 0
		&& observed->resource_x_activation_generation == 0
		&& (round->cached_ownership_generation == observed->generation
			|| direct_init_generation_exact);
	x_activation_fenced_phase = x_activation_fenced
		&& ((observed->resource_x_activation_generation == 0
				&& (progress_flags
						== (RESOURCE_X_PROGRESS_BOUND
							| RESOURCE_X_PROGRESS_T1)
					|| progress_flags
						== (RESOURCE_X_PROGRESS_BOUND
							| RESOURCE_X_PROGRESS_T1
							| RESOURCE_X_PROGRESS_T2)))
			|| (observed->resource_x_activation_generation == attempt
				&& (progress_flags
						== (RESOURCE_X_PROGRESS_BOUND
							| RESOURCE_X_PROGRESS_T1)
					|| progress_flags
						== (RESOURCE_X_PROGRESS_BOUND
							| RESOURCE_X_PROGRESS_T1
							| RESOURCE_X_PROGRESS_T2))));
	phase_exact = (n_installing
				&& progress_flags
					== (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1))
		|| x_activation_fenced_phase
		|| (x_activation_cleared
			&& progress_flags
				== (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1
					| RESOURCE_X_PROGRESS_T2));
	/* dispatch_phase counts scheduled re-drive attempts; it is neither
	 * authority nor ownership-loss evidence.  An exact BUFFER_BUSY
	 * observation is likewise the executor's BLOCKED state.  Keep the local
	 * foreground follower parked across both shapes so the existing retained
	 * transport tick can rearm and make one more conditional buffer attempt. */
	executor_retry_state_exact
		= (entry->resource_x_no_progress_generation == 0
			&& entry->resource_x_no_progress_reason
				== RESOURCE_X_NO_PROGRESS_NONE)
		  || (entry->resource_x_no_progress_generation == attempt
			  && entry->resource_x_no_progress_reason
				 == RESOURCE_X_NO_PROGRESS_BUFFER_BUSY);
	ref_class = pcm_resource_x_ref_classify_locked(entry, &ref);
	return round->phase == RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED
		&& identity_exact
		&& round->request.assertion_sequence != 0
		&& round->request.assertion_sequence != UINT64_MAX
		&& round->request.assertion_sequence == round->highest_attempt_floor
		&& round->accepted_base != 0
		&& round->accepted_base != UINT64_MAX
		&& pcm_resource_x_ref_valid(&ref)
		&& (ref_class == PCM_RX_REF_ACTIVE_EXACT
			|| (direct_init_generation_exact && x_activation_cleared
				&& ref_class == PCM_RX_REF_RETIRED_EXACT))
		&& phase_exact
		&& executor_retry_state_exact;
}

/* BufferDesc and the requester ledger are separate lock domains.  The R9
 * executor publishes a short closed interval from N+GRANT_PENDING through
 * the post-commit X writer fence, image binding, and T2 until T3 clears that
 * fence.  An ordinary same-node caller may wait through those observations
 * only when the immutable bootstrap identity, sidecar, and active ledger
 * still name one exact attempt.  A direct-init creation identity is read only
 * from that same locked round and must be paired with its exact BufferDesc
 * generation/token; it is never exposed as authority to the follower. */
bool
cluster_pcm_lock_resource_x_bootstrap_round_target_install_inflight_exact(
	const ResourceXAssertion *assertion, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation,
	uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation,
	uint64 retry_slice_us,
	const ClusterPcmOwnSnapshot *observed)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	bool matches;

	if (!resource_x_assertion_valid(assertion) || observed == NULL)
		return false;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return false;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	matches = pcm_resource_x_bootstrap_round_target_install_inflight_locked(
		entry, round, assertion, current_master_node, resource_formation,
		master_session_incarnation, r4_record_generation,
		requester_sender_connection_generation,
		master_ingress_connection_generation, retry_slice_us, observed);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return matches;
}

static bool
pcm_resource_x_target_install_continuation_valid(
	const ResourceXTargetInstallContinuation *continuation,
	ResourceXAssertion *assertion_out)
{
	ResourceXAssertion assertion;

	if (assertion_out != NULL)
		memset(assertion_out, 0, sizeof(*assertion_out));
	if (continuation == NULL || !continuation->valid
		|| continuation->requester_node != cluster_node_id || continuation->master_node < 0
		|| continuation->master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| continuation->entry_binding_generation == 0
		|| continuation->entry_binding_generation == UINT64_MAX
		|| continuation->resource_formation == 0 || continuation->resource_formation == UINT64_MAX
		|| continuation->master_session_incarnation == 0
		|| continuation->master_session_incarnation == UINT64_MAX
		|| continuation->r4_record_generation == 0
		|| continuation->r4_record_generation == UINT64_MAX
		|| continuation->acquisition_generation == 0
		|| continuation->acquisition_generation == UINT64_MAX
		|| continuation->accepted_base_authority_generation == 0
		|| continuation->accepted_base_authority_generation == UINT64_MAX
		|| continuation->observed_head_change_generation == 0
		|| continuation->observed_head_change_generation == UINT64_MAX
		|| continuation->caller_absolute_deadline_us == 0
		|| continuation->caller_absolute_deadline_us == UINT64_MAX
		|| continuation->requested_sleep_slice_us == 0
		|| continuation->requested_sleep_slice_us == UINT64_MAX
		|| continuation->pending_ownership_generation == UINT64_MAX
		|| continuation->expected_x_ownership_generation == 0
		|| continuation->expected_x_ownership_generation == UINT64_MAX
		|| continuation->expected_x_ownership_generation
			   != continuation->pending_ownership_generation + 1
		|| continuation->reservation_token == 0 || continuation->reservation_token == UINT64_MAX
		|| continuation->observed_claim_pending_generation == UINT64_MAX
		|| continuation->observed_claim_reservation_token == UINT64_MAX
		|| (continuation->observed_claim_reservation_token == 0
			&& continuation->observed_claim_pending_generation != 0)
		|| (continuation->capture_flags & ~RESOURCE_X_TARGET_INSTALL_CAPTURE_KNOWN_MASK) != 0
		|| continuation->requester_sender_connection_generation == 0
		|| continuation->requester_sender_connection_generation == UINT32_MAX
		|| continuation->master_ingress_connection_generation == 0
		|| continuation->master_ingress_connection_generation == UINT32_MAX)
		return false;
	assertion.resource = continuation->resource;
	assertion.requester_node = continuation->requester_node;
	if (!resource_x_assertion_valid(&assertion))
		return false;
	if (assertion_out != NULL)
		*assertion_out = assertion;
	return true;
}

static bool
pcm_resource_x_target_install_round_identity_exact_locked(
	const ClusterPcmResourceXBootstrapRound *round,
	const ResourceXTargetInstallContinuation *continuation,
	const ResourceXAssertion *assertion)
{
	return pcm_resource_x_node_request_key_matches_locked(
			   round, assertion, continuation->master_node, continuation->resource_formation,
			   continuation->master_session_incarnation, continuation->r4_record_generation,
			   continuation->requester_sender_connection_generation,
			   continuation->master_ingress_connection_generation)
		   && round->request.assertion_sequence == continuation->acquisition_generation
		   && round->highest_attempt_floor == continuation->acquisition_generation
		   && round->accepted_base == continuation->accepted_base_authority_generation;
}

static bool
pcm_resource_x_target_install_claim_observation_exact_locked(
	const ClusterPcmResourceXBootstrapRound *round,
	const ResourceXTargetInstallContinuation *continuation)
{
	return pcm_resource_x_install_claim_valid_locked(round)
		   && continuation->observed_claim_pending_generation
				  == round->install_claim_pending_generation
		   && continuation->observed_claim_reservation_token
				  == round->install_claim_reservation_token
		   && ((continuation->capture_flags & RESOURCE_X_TARGET_INSTALL_CAPTURE_DIRECT_INIT) != 0)
				  == (round->install_claim_source == RESOURCE_X_INSTALL_CLAIM_DIRECT_INIT);
}

/* A BufferDesc snapshot is taken before the caller acquires entry_lock.  Once
 * the immutable continuation has been captured, terminal publication may win
 * between those two lock-domain samples.  Recognize only the exact legal
 * I0..I3 predecessor shapes here; this is a request to resample, never
 * terminal evidence and never writer permission. */
static bool
pcm_resource_x_target_install_preterminal_observation_exact(
	const ResourceXTargetInstallContinuation *continuation,
	const ClusterPcmOwnSnapshot *observed)
{
	bool n_installing;
	bool x_activation_cleared;
	bool x_activation_fenced;

	Assert(continuation != NULL);
	Assert(observed != NULL);
	if (!BufferTagsEqual(&observed->tag, &continuation->resource)
		|| observed->reservation_token != continuation->reservation_token)
		return false;
	n_installing
		= observed->pcm_state == (uint8)PCM_STATE_N
		  && observed->flags == PCM_OWN_FLAG_GRANT_PENDING
		  && observed->generation
			== continuation->pending_ownership_generation
		  && observed->writer_activation_token == 0
		  && observed->resource_x_activation_generation == 0;
	x_activation_fenced
		= observed->pcm_state == (uint8)PCM_STATE_X
		  && observed->flags == 0
		  && observed->generation
			== continuation->expected_x_ownership_generation
		  && observed->writer_activation_token
			== continuation->reservation_token
		  && (observed->resource_x_activation_generation == 0
			  || observed->resource_x_activation_generation
				== continuation->acquisition_generation);
	x_activation_cleared
		= observed->pcm_state == (uint8)PCM_STATE_X
		  && observed->flags == 0
		  && observed->generation
			== continuation->expected_x_ownership_generation
		  && observed->writer_activation_token == 0
		  && observed->resource_x_activation_generation == 0;
	return n_installing || x_activation_fenced || x_activation_cleared;
}

/* A28/A29: one terminal-X receipt may be superseded before its backend
 * publishes a writer ledger or takes content-X.  A nonblocking successor may
 * exact-abort and replay more than once; every begin consumes a monotonic
 * token while the X ownership generation stays fixed.  BufferDesc is sampled
 * before entry_lock, so accept the exact same-generation revoke/abort envelope
 * and its single X->N ownership transition for any strictly newer,
 * non-wrapping token.  This predicate only requests a fresh authority probe;
 * it never grants authority. */
static bool
pcm_resource_x_target_install_postpreuse_observation_exact(
	const ResourceXTargetInstallContinuation *continuation,
	const ClusterPcmOwnSnapshot *observed)
{
	bool x_revoke_or_abort;
	bool n_revoke_or_finish;

	Assert(continuation != NULL);
	Assert(observed != NULL);
	if (!BufferTagsEqual(&observed->tag, &continuation->resource)
		|| observed->reservation_token <= continuation->reservation_token
		|| observed->reservation_token == UINT64_MAX
		|| observed->writer_activation_token != 0
		|| observed->resource_x_activation_generation != 0)
		return false;
	x_revoke_or_abort
		= observed->pcm_state == (uint8)PCM_STATE_X
		  && observed->generation
			== continuation->expected_x_ownership_generation
		  && (observed->flags == 0
			  || observed->flags == PCM_OWN_FLAG_REVOKING);
	n_revoke_or_finish
		= continuation->expected_x_ownership_generation < UINT64_MAX - 1
		  && observed->pcm_state == (uint8)PCM_STATE_N
		  && observed->generation
			== continuation->expected_x_ownership_generation + 1
		  && (observed->flags == 0
			  || observed->flags == PCM_OWN_FLAG_REVOKING);
	return x_revoke_or_abort || n_revoke_or_finish;
}

static bool
pcm_resource_x_target_install_terminal_observation_exact(
	const ResourceXTargetInstallContinuation *continuation,
	const ClusterPcmOwnSnapshot *observed)
{
	Assert(continuation != NULL);
	Assert(observed != NULL);
	return BufferTagsEqual(&observed->tag, &continuation->resource)
		&& observed->pcm_state == (uint8)PCM_STATE_X
		&& observed->flags == 0
		&& observed->generation
			== continuation->expected_x_ownership_generation
		&& observed->reservation_token == continuation->reservation_token
		&& observed->writer_activation_token == 0
		&& observed->resource_x_activation_generation == 0;
}

/* A terminal receipt may outlive the old requester binding for only one
 * reason admitted here: its exact X carrier completed one ownership-generation
 * transfer after zero or more reversible token cycles and the old acquisition
 * floor is still retained.  A non-empty successor round must remain in the
 * same formation/session/transport domain.  Direct-init proof identities are
 * deliberately excluded. */
static bool
pcm_resource_x_target_install_superseded_round_exact_locked(
	struct GrdEntry *entry, const ClusterPcmResourceXBootstrapRound *round,
	const ResourceXTargetInstallContinuation *continuation,
	const ResourceXAssertion *assertion)
{
	Assert(entry != NULL);
	Assert(round != NULL);
	Assert(continuation != NULL);
	Assert(assertion != NULL);
	if ((continuation->capture_flags & RESOURCE_X_TARGET_INSTALL_CAPTURE_DIRECT_INIT) != 0
		|| entry->resource_x_retired_acquisition_generation != continuation->acquisition_generation
		|| round->highest_attempt_floor < continuation->acquisition_generation
		|| !pcm_resource_x_local_owner_valid_locked(&entry->resource_x_local_owner)
		|| entry->resource_x_local_owner.state != RESOURCE_X_LOCAL_OWNER_EMPTY
		|| !cluster_pcm_lock_resource_x_gate_open_exact(continuation->resource_formation))
		return false;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY)
		return true;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED)
		return false;
	return pcm_resource_x_node_request_key_matches_locked(
		round, assertion, continuation->master_node, continuation->resource_formation,
		continuation->master_session_incarnation, continuation->r4_record_generation,
		continuation->requester_sender_connection_generation,
		continuation->master_ingress_connection_generation);
}

static bool
pcm_resource_x_target_install_captured_live_revoke_exact(
	const ClusterPcmResourceXLocalOwner *owner,
	const ResourceXTargetInstallContinuation *continuation,
	const ClusterPcmOwnSnapshot *observed)
{
	Assert(owner != NULL);
	Assert(continuation != NULL);
	Assert(observed != NULL);
	return owner->state == RESOURCE_X_LOCAL_OWNER_REVOKING
		&& pcm_resource_x_local_owner_handle_valid(&owner->handle)
		&& owner->handle.reservation_token < UINT64_MAX - 1
		&& BufferTagsEqual(&observed->tag, &continuation->resource)
		&& observed->pcm_state == (uint8)PCM_STATE_X
		&& observed->flags == PCM_OWN_FLAG_REVOKING
		&& observed->generation
			== continuation->expected_x_ownership_generation
		&& observed->reservation_token == continuation->reservation_token
		&& observed->reservation_token == owner->handle.reservation_token + 1
		&& observed->writer_activation_token == 0
		&& observed->resource_x_activation_generation == 0;
}

static bool
pcm_resource_x_target_install_owner_observation_exact_locked(
	const ClusterPcmResourceXLocalOwner *owner,
	const ResourceXTargetInstallContinuation *continuation,
	const ClusterPcmOwnSnapshot *observed)
{
	Assert(owner != NULL);
	Assert(continuation != NULL);
	Assert(observed != NULL);
	return pcm_resource_x_target_install_terminal_observation_exact(
			continuation, observed)
		|| pcm_resource_x_target_install_preterminal_observation_exact(
			continuation, observed)
		|| pcm_resource_x_target_install_postpreuse_observation_exact(
			continuation, observed)
		|| pcm_resource_x_target_install_captured_live_revoke_exact(
			owner, continuation, observed);
}

/* A30: ordinary T1 names the requester attempt but carries no BufferDesc
 * generation/token.  An immediate successor N reservation can therefore be
 * captured as I0 before publication of the exact older terminal cover.  Once
 * that cover is visible, use it only to disprove the process-local receipt:
 * the immediate N successor may request canonical requalification, but can
 * never produce a terminal ref. */
static bool
pcm_resource_x_target_install_late_misbound_i0_exact_locked(
	struct GrdEntry *entry, const ClusterPcmResourceXBootstrapRound *round,
	const ResourceXTargetInstallContinuation *continuation,
	const ClusterPcmOwnSnapshot *observed,
	const ResourceXAcquisitionRef *expected_ref)
{
	const ClusterPcmResourceXLocalOwner *owner;
	uint64 terminal_generation;

	Assert(entry != NULL);
	Assert(round != NULL);
	Assert(continuation != NULL);
	Assert(observed != NULL);
	Assert(expected_ref != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		   || LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	terminal_generation = round->cached_ownership_generation;
	owner = &entry->resource_x_local_owner;
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
		|| continuation->observed_claim_pending_generation != 0
		|| continuation->observed_claim_reservation_token != 0 || terminal_generation == 0
		|| terminal_generation >= UINT64_MAX - 2
		|| continuation->pending_ownership_generation != terminal_generation + 1
		|| continuation->expected_x_ownership_generation != terminal_generation + 2
		|| continuation->reservation_token == 0 || continuation->reservation_token == UINT64_MAX
		|| !BufferTagsEqual(&observed->tag, &continuation->resource)
		|| observed->pcm_state != (uint8)PCM_STATE_N
		|| observed->generation != terminal_generation + 1
		|| (observed->flags != PCM_OWN_FLAG_GRANT_PENDING && observed->flags != 0
			&& observed->flags != PCM_OWN_FLAG_REVOKING)
		|| observed->reservation_token == 0 || observed->reservation_token == UINT64_MAX
		|| observed->reservation_token < continuation->reservation_token
		|| observed->writer_activation_token != 0 || observed->resource_x_activation_generation != 0
		|| !pcm_resource_x_local_owner_valid_locked(owner)
		|| (owner->state != RESOURCE_X_LOCAL_OWNER_EMPTY
			&& !pcm_resource_x_local_owner_round_exact_locked(entry, round)))
		return false;
	return pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
		entry, round, expected_ref,
		continuation->master_session_incarnation,
		continuation->r4_record_generation, terminal_generation);
}

/* A31: capture may sample the exact terminal X only after a source successor
 * has armed REVOKING and consumed its new token.  The continuation then stores
 * that revoke token.  Completing the same X(G)->N(G+1) conversion retains the
 * token, so equality is receipt invalidation only.  Require the authoritative
 * terminal cover and exact one-generation N envelope; this predicate returns
 * no ref and leaves the retained pair for the canonical caller path to prove. */
static bool
pcm_resource_x_target_install_same_token_n_successor_exact_locked(
	struct GrdEntry *entry, const ClusterPcmResourceXBootstrapRound *round,
	const ResourceXTargetInstallContinuation *continuation,
	const ClusterPcmOwnSnapshot *observed,
	const ResourceXAcquisitionRef *expected_ref)
{
	const ClusterPcmResourceXLocalOwner *owner;
	uint64 terminal_generation;

	Assert(entry != NULL);
	Assert(round != NULL);
	Assert(continuation != NULL);
	Assert(observed != NULL);
	Assert(expected_ref != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		   || LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	terminal_generation = continuation->expected_x_ownership_generation;
	owner = &entry->resource_x_local_owner;
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
		|| (continuation->capture_flags & RESOURCE_X_TARGET_INSTALL_CAPTURE_REVOKE_TOKEN) == 0
		|| (continuation->capture_flags & RESOURCE_X_TARGET_INSTALL_CAPTURE_DIRECT_INIT) != 0
		|| terminal_generation == 0 || terminal_generation >= UINT64_MAX - 1
		|| continuation->pending_ownership_generation != terminal_generation - 1
		|| round->cached_ownership_generation != terminal_generation
		|| !BufferTagsEqual(&observed->tag, &continuation->resource)
		|| observed->pcm_state != (uint8)PCM_STATE_N
		|| (observed->flags != 0 && observed->flags != PCM_OWN_FLAG_REVOKING)
		|| observed->generation != terminal_generation + 1 || observed->reservation_token == 0
		|| observed->reservation_token == UINT64_MAX
		|| observed->reservation_token != continuation->reservation_token
		|| observed->writer_activation_token != 0 || observed->resource_x_activation_generation != 0
		|| !pcm_resource_x_local_owner_valid_locked(owner)
		|| (owner->state != RESOURCE_X_LOCAL_OWNER_EMPTY
			&& !pcm_resource_x_local_owner_round_exact_locked(entry, round)))
		return false;
	return pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
		entry, round, expected_ref,
		continuation->master_session_incarnation,
		continuation->r4_record_generation, terminal_generation);
}

/* A32: two target-install receipts can retain the same generation/token tuple
 * after owner release.  Preserve the distinguishing capture-time fact only
 * when the entry-locked terminal cover, local revoke owner, and coherent
 * X+REVOKING sample all name the same ordinary round.  This is process-local
 * provenance and never terminal authority. */
static bool
pcm_resource_x_target_install_capture_revoke_provenance_exact_locked(
	struct GrdEntry *entry, const ClusterPcmResourceXBootstrapRound *round,
	const ResourceXTargetInstallContinuation *continuation,
	const ResourceXAssertion *assertion,
	const ClusterPcmOwnSnapshot *observed)
{
	ResourceXAcquisitionRef expected_ref;
	const ClusterPcmResourceXLocalOwner *owner;
	uint64 terminal_generation;

	Assert(entry != NULL);
	Assert(round != NULL);
	Assert(continuation != NULL);
	Assert(assertion != NULL);
	Assert(observed != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		   || LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	terminal_generation = continuation->expected_x_ownership_generation;
	owner = &entry->resource_x_local_owner;
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
		|| (continuation->capture_flags & RESOURCE_X_TARGET_INSTALL_CAPTURE_DIRECT_INIT) != 0
		|| terminal_generation == 0 || terminal_generation >= UINT64_MAX - 1
		|| continuation->pending_ownership_generation != terminal_generation - 1
		|| round->cached_ownership_generation != terminal_generation
		|| !pcm_resource_x_target_install_round_identity_exact_locked(round, continuation,
																	  assertion)
		|| !pcm_resource_x_local_owner_valid_locked(owner)
		|| owner->state != RESOURCE_X_LOCAL_OWNER_REVOKING
		|| !pcm_resource_x_local_owner_round_exact_locked(entry, round)
		|| !pcm_resource_x_target_install_captured_live_revoke_exact(owner, continuation, observed))
		return false;
	memset(&expected_ref, 0, sizeof(expected_ref));
	expected_ref.assertion = *assertion;
	expected_ref.formation = continuation->resource_formation;
	expected_ref.acquisition_generation
		= continuation->acquisition_generation;
	return pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
		entry, round, &expected_ref,
		continuation->master_session_incarnation,
		continuation->r4_record_generation, terminal_generation);
}

static ResourceXTargetInstallFollowState
pcm_resource_x_target_install_classify_locked(
	struct GrdEntry *entry, ClusterPcmResourceXBootstrapRound *round,
	const ResourceXTargetInstallContinuation *continuation,
	const ResourceXAssertion *assertion,
	const ClusterPcmOwnSnapshot *observed,
	ResourceXAcquisitionRef *terminal_ref_out)
{
	ResourceXAcquisitionRef expected_ref;
	ClusterPcmResourceXLocalOwner *owner;
	bool postpreuse_observation;
	bool round_identity_exact;
	bool terminal_cover_exact = false;

	Assert(entry != NULL);
	Assert(round != NULL);
	Assert(continuation != NULL);
	Assert(assertion != NULL);
	if (terminal_ref_out != NULL)
		memset(terminal_ref_out, 0, sizeof(*terminal_ref_out));
	if (observed == NULL)
		return RESOURCE_X_TARGET_INSTALL_INVALID;
	if (!BufferTagsEqual(&observed->tag, &continuation->resource))
		return RESOURCE_X_TARGET_INSTALL_STALE;
	owner = &entry->resource_x_local_owner;
	if (!pcm_resource_x_local_owner_valid_locked(owner))
		return RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
	postpreuse_observation
		= pcm_resource_x_target_install_postpreuse_observation_exact(
			continuation, observed);
	round_identity_exact
		= pcm_resource_x_target_install_round_identity_exact_locked(
			round, continuation, assertion);
	if (!round_identity_exact
		&& postpreuse_observation
		&& pcm_resource_x_target_install_superseded_round_exact_locked(
			entry, round, continuation, assertion))
		return RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED)
		return RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
	if (round->request.assertion_sequence != 0
		&& round->request.assertion_sequence
			!= continuation->acquisition_generation)
		return RESOURCE_X_TARGET_INSTALL_STALE;
	if (round->phase < RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED)
		return RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
	if (!round_identity_exact)
		return RESOURCE_X_TARGET_INSTALL_STALE;
	if (!pcm_resource_x_target_install_claim_observation_exact_locked(round, continuation)) {
		/* A follower captured while T1 had not published its physical claim
		 * must start a fresh B-E-B after the bind.  Its old sample cannot be
		 * promoted into the new claim or into terminal authority. */
		if (continuation->observed_claim_reservation_token == 0
			&& continuation->observed_claim_pending_generation == 0
			&& (continuation->capture_flags & RESOURCE_X_TARGET_INSTALL_CAPTURE_DIRECT_INIT) == 0
			&& pcm_resource_x_install_claim_valid_locked(round)
			&& round->install_claim_source != RESOURCE_X_INSTALL_CLAIM_NONE)
			return RESOURCE_X_TARGET_INSTALL_RESAMPLE;
		return RESOURCE_X_TARGET_INSTALL_STALE;
	}

	memset(&expected_ref, 0, sizeof(expected_ref));
	expected_ref.assertion = *assertion;
	expected_ref.formation = continuation->resource_formation;
	expected_ref.acquisition_generation
		= continuation->acquisition_generation;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED)
	{
		if (pcm_resource_x_target_install_late_misbound_i0_exact_locked(
				entry, round, continuation, observed, &expected_ref))
			return RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY;
		terminal_cover_exact
			= round->cached_ownership_generation
				== continuation->expected_x_ownership_generation
			  && pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
					entry, round, &expected_ref,
					continuation->master_session_incarnation,
					continuation->r4_record_generation,
					continuation->expected_x_ownership_generation);
		if (!terminal_cover_exact)
			return RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
		if (pcm_resource_x_target_install_same_token_n_successor_exact_locked(
				entry, round, continuation, observed, &expected_ref))
			return RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY;
		if (owner->state != RESOURCE_X_LOCAL_OWNER_EMPTY)
		{
			if (!pcm_resource_x_local_owner_round_exact_locked(entry, round)
				|| !pcm_resource_x_target_install_owner_observation_exact_locked(
					owner, continuation, observed))
				return RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
			return RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY;
		}
		if (pcm_resource_x_target_install_terminal_observation_exact(
				continuation, observed))
		{
			if (terminal_ref_out != NULL)
				*terminal_ref_out = expected_ref;
			return RESOURCE_X_TARGET_INSTALL_TERMINAL;
		}
		if (postpreuse_observation)
			return RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY;
		if (observed->pcm_state == (uint8)PCM_STATE_N
			&& observed->generation
				> continuation->expected_x_ownership_generation)
			return RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
		if (observed->reservation_token != continuation->reservation_token
			|| (observed->generation
				!= continuation->pending_ownership_generation
				&& observed->generation
					!= continuation->expected_x_ownership_generation))
			return RESOURCE_X_TARGET_INSTALL_STALE;
		return RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
	}
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED)
		return RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
	if (observed->reservation_token != continuation->reservation_token
		|| (observed->generation
			!= continuation->pending_ownership_generation
			&& observed->generation
				!= continuation->expected_x_ownership_generation))
		return RESOURCE_X_TARGET_INSTALL_STALE;
	if (pcm_resource_x_bootstrap_round_target_install_inflight_locked(
			entry, round, assertion, continuation->master_node, continuation->resource_formation,
			continuation->master_session_incarnation, continuation->r4_record_generation,
			continuation->requester_sender_connection_generation,
			continuation->master_ingress_connection_generation,
			continuation->requested_sleep_slice_us, observed))
		return RESOURCE_X_TARGET_INSTALL_INFLIGHT;
	return RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
}

ResourceXTargetInstallFollowState
cluster_pcm_lock_resource_x_bootstrap_round_target_install_capture_exact(
	const ResourceXAssertion *assertion, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation,
	uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation,
	uint64 retry_slice_us, uint64 caller_absolute_deadline_us,
	const ClusterPcmOwnSnapshot *observed,
	ResourceXTargetInstallContinuation *continuation_out)
{
	ResourceXTargetInstallContinuation continuation;
	ResourceXTargetInstallFollowState state;
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;

	if (continuation_out != NULL)
		memset(continuation_out, 0, sizeof(*continuation_out));
	if (continuation_out == NULL || !resource_x_assertion_valid(assertion)
		|| assertion->requester_node != cluster_node_id
		|| current_master_node < 0
		|| current_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| resource_formation == 0 || resource_formation == UINT64_MAX
		|| master_session_incarnation == 0
		|| master_session_incarnation == UINT64_MAX
		|| r4_record_generation == 0
		|| r4_record_generation == UINT64_MAX
		|| requester_sender_connection_generation == 0
		|| requester_sender_connection_generation == UINT32_MAX
		|| master_ingress_connection_generation == 0
		|| master_ingress_connection_generation == UINT32_MAX
		|| retry_slice_us == 0 || retry_slice_us == UINT64_MAX
		|| caller_absolute_deadline_us == 0
		|| caller_absolute_deadline_us == UINT64_MAX
		|| observed == NULL)
		return RESOURCE_X_TARGET_INSTALL_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_TARGET_INSTALL_STALE;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED)
		state = RESOURCE_X_TARGET_INSTALL_RECOVERY_BLOCKED;
	else if (!pcm_resource_x_request_observation_matches_locked(
				 round, assertion, current_master_node, resource_formation,
				 master_session_incarnation, r4_record_generation,
				 requester_sender_connection_generation, master_ingress_connection_generation,
				 round->install_claim_pending_generation, round->install_claim_reservation_token)
			 || round->request.assertion_sequence == 0
			 || round->request.assertion_sequence == UINT64_MAX || round->accepted_base == 0
			 || round->accepted_base == UINT64_MAX || round->head_no_progress_deadline_us == 0
			 || round->head_no_progress_deadline_us == UINT64_MAX)
		state = RESOURCE_X_TARGET_INSTALL_STALE;
	else if (!BufferTagsEqual(&observed->tag, &assertion->resource)
		|| observed->reservation_token == 0
		|| observed->reservation_token == UINT64_MAX
		|| observed->generation == UINT64_MAX)
		state = RESOURCE_X_TARGET_INSTALL_STALE;
	else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
		&& round->cached_ownership_generation != 0
		&& round->cached_ownership_generation < UINT64_MAX - 1
		&& observed->pcm_state == (uint8)PCM_STATE_N
		&& (observed->flags == PCM_OWN_FLAG_GRANT_PENDING
			|| observed->flags == 0
			|| observed->flags == PCM_OWN_FLAG_REVOKING)
		&& observed->generation == round->cached_ownership_generation + 1
		&& observed->writer_activation_token == 0
		&& observed->resource_x_activation_generation == 0)
	{
		/* This first sample already follows the round's cached terminal X.
		 * It cannot seed an install continuation: doing so would invent an
		 * expected X at G+2 and intercept the caller's local-pending or
		 * retained-release path.  STALE declines only this optional capture;
		 * the caller must validate the N carrier before fresh authority. */
		state = RESOURCE_X_TARGET_INSTALL_STALE;
	}
	else
	{
		memset(&continuation, 0, sizeof(continuation));
		continuation.resource = assertion->resource;
		continuation.requester_node = assertion->requester_node;
		continuation.master_node = current_master_node;
		continuation.entry_binding_generation
			= entry_ref.binding_generation;
		continuation.resource_formation = resource_formation;
		continuation.master_session_incarnation
			= master_session_incarnation;
		continuation.r4_record_generation = r4_record_generation;
		continuation.acquisition_generation
			= round->request.assertion_sequence;
		continuation.accepted_base_authority_generation
			= round->accepted_base;
		continuation.observed_head_change_generation = round->head_change_generation;
		continuation.caller_absolute_deadline_us
			= caller_absolute_deadline_us;
		continuation.requested_sleep_slice_us = retry_slice_us;
		continuation.pending_ownership_generation
			= observed->pcm_state == (uint8)PCM_STATE_N
				? observed->generation : observed->generation - 1;
		continuation.expected_x_ownership_generation
			= continuation.pending_ownership_generation + 1;
		continuation.reservation_token = observed->reservation_token;
		continuation.observed_claim_pending_generation = round->install_claim_pending_generation;
		continuation.observed_claim_reservation_token = round->install_claim_reservation_token;
		if (round->install_claim_source == RESOURCE_X_INSTALL_CLAIM_DIRECT_INIT)
			continuation.capture_flags |= RESOURCE_X_TARGET_INSTALL_CAPTURE_DIRECT_INIT;
		continuation.requester_sender_connection_generation
			= requester_sender_connection_generation;
		continuation.master_ingress_connection_generation
			= master_ingress_connection_generation;
		if (pcm_resource_x_target_install_capture_revoke_provenance_exact_locked(
				entry, round, &continuation, assertion, observed))
			continuation.capture_flags
				|= RESOURCE_X_TARGET_INSTALL_CAPTURE_REVOKE_TOKEN;
		continuation.valid = true;
		state = pcm_resource_x_target_install_classify_locked(
			entry, round, &continuation, assertion, observed, NULL);
		if (state == RESOURCE_X_TARGET_INSTALL_INFLIGHT
			|| state == RESOURCE_X_TARGET_INSTALL_TERMINAL
			|| state == RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY)
			*continuation_out = continuation;
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return state;
}

ResourceXTargetInstallFollowState
cluster_pcm_lock_resource_x_bootstrap_round_target_install_classify_exact(
	const ResourceXTargetInstallContinuation *continuation,
	const ClusterPcmOwnSnapshot *observed,
	ResourceXAcquisitionRef *terminal_ref_out)
{
	ResourceXAssertion assertion;
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXTargetInstallFollowState state;

	if (terminal_ref_out != NULL)
		memset(terminal_ref_out, 0, sizeof(*terminal_ref_out));
	if (!pcm_resource_x_target_install_continuation_valid(
			continuation, &assertion)
		|| observed == NULL)
		return RESOURCE_X_TARGET_INSTALL_INVALID;
	if (!pcm_entry_ref_acquire(&continuation->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_TARGET_INSTALL_STALE;
	if (entry_ref.binding_generation
		!= continuation->entry_binding_generation)
	{
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_TARGET_INSTALL_STALE;
	}
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	state = pcm_resource_x_target_install_classify_locked(
		entry, round, continuation, &assertion, observed,
		terminal_ref_out);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return state;
}

/* Recheck only the entry-owned portion of an exact retained continuation.
 * BufferDesc is deliberately absent: a waiter cannot pair a caller's old
 * physical snapshot with newer entry progress.  APPLIED means only that the
 * exact entry predicate remains waitable; it carries no authority. */
static ResourceXApplyResult
pcm_resource_x_target_install_wait_recheck_locked(
	struct GrdEntry *entry,
	ClusterPcmResourceXBootstrapRound *round,
	const ResourceXTargetInstallContinuation *continuation,
	const ResourceXAssertion *assertion,
	ResourceXTargetInstallFollowState expected_follow_state)
{
	static const ResourceXAcquisitionRef empty_ref;
	ResourceXAcquisitionRef expected_ref;
	PcmResourceXRefClass ref_class;
	uint32 progress_flags;

	Assert(entry != NULL);
	Assert(round != NULL);
	Assert(continuation != NULL);
	Assert(assertion != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		   || LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));

	if (!pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner)
		|| round->phase > RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED
		|| (entry->resource_x_progress_flags
			& ~RESOURCE_X_PROGRESS_KNOWN_MASK) != 0)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED
		|| (entry->resource_x_progress_flags
			& RESOURCE_X_PROGRESS_RECOVERY_BLOCKED) != 0)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;

	memset(&expected_ref, 0, sizeof(expected_ref));
	expected_ref.assertion = *assertion;
	expected_ref.formation = continuation->resource_formation;
	expected_ref.acquisition_generation
		= continuation->acquisition_generation;

	if (expected_follow_state == RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY)
	{
		/* Owner clear or a valid successor that won after the caller's
		 * coherent classification is progress.  Return immediately so the
		 * caller can obtain a fresh B-E-B observation. */
		if (entry->resource_x_local_owner.state
			== RESOURCE_X_LOCAL_OWNER_EMPTY)
			return RESOURCE_X_APPLY_DUPLICATE;
		if (!pcm_resource_x_target_install_round_identity_exact_locked(
				round, continuation, assertion))
			return RESOURCE_X_APPLY_STALE;
		if (round->phase
			!= RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
			|| round->cached_ownership_generation
				!= continuation->expected_x_ownership_generation
			|| !pcm_resource_x_local_owner_round_exact_locked(entry, round))
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		/* The old terminal cover may coexist with a newer active attempt after
		 * T1 wins between classification and this registered recheck.  Accept
		 * only a structurally valid, same-formation, strictly newer attempt;
		 * this is progress to re-probe, never authority for either attempt. */
		if (pcm_resource_x_active_valid_locked(entry)
			&& entry->resource_x_formation
				== continuation->resource_formation
			&& entry->resource_x_acquisition_generation
				> continuation->acquisition_generation
			&& entry->resource_x_retired_acquisition_generation
				== continuation->acquisition_generation
			&& pcm_resource_x_acquisition_ref_equal(
				&round->terminal_ref, &expected_ref)
			&& round->terminal_authority_generation
				> round->accepted_base
			&& round->terminal_authority_generation != UINT64_MAX)
			return RESOURCE_X_APPLY_DUPLICATE;
		if (!pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
				entry, round, &expected_ref,
				continuation->master_session_incarnation,
				continuation->r4_record_generation,
				continuation->expected_x_ownership_generation))
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		return RESOURCE_X_APPLY_APPLIED;
	}

	if (expected_follow_state != RESOURCE_X_TARGET_INSTALL_INFLIGHT)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_resource_x_target_install_round_identity_exact_locked(
			round, continuation, assertion))
		return RESOURCE_X_APPLY_STALE;
	if (!pcm_resource_x_target_install_claim_observation_exact_locked(round, continuation))
		return continuation->observed_claim_reservation_token == 0
					   && continuation->observed_claim_pending_generation == 0
					   && (continuation->capture_flags
						   & RESOURCE_X_TARGET_INSTALL_CAPTURE_DIRECT_INIT)
							  == 0
					   && pcm_resource_x_install_claim_valid_locked(round)
					   && round->install_claim_source != RESOURCE_X_INSTALL_CLAIM_NONE
				   ? RESOURCE_X_APPLY_DUPLICATE
				   : RESOURCE_X_APPLY_STALE;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED)
	{
		if (round->cached_ownership_generation
				!= continuation->expected_x_ownership_generation
			|| !pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
				entry, round, &expected_ref,
				continuation->master_session_incarnation,
				continuation->r4_record_generation,
				continuation->expected_x_ownership_generation))
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		return RESOURCE_X_APPLY_DUPLICATE;
	}
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED
		|| entry->resource_x_local_owner.state
			!= RESOURCE_X_LOCAL_OWNER_EMPTY
		|| round->terminal_authority_generation != 0
		|| memcmp(&round->terminal_ref, &empty_ref,
			sizeof(empty_ref)) != 0)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	ref_class = pcm_resource_x_ref_classify_locked(entry, &expected_ref);
	if (ref_class == PCM_RX_REF_ACTIVE_OTHER
		|| ref_class == PCM_RX_REF_RETIRED_OLD)
		return RESOURCE_X_APPLY_STALE;
	if (ref_class != PCM_RX_REF_ACTIVE_EXACT)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	progress_flags = entry->resource_x_progress_flags;
	if (progress_flags
		== (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1))
	{
		if (round->cached_ownership_generation != 0)
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	else if (progress_flags
			 == (RESOURCE_X_PROGRESS_BOUND | RESOURCE_X_PROGRESS_T1
				 | RESOURCE_X_PROGRESS_T2))
	{
		if (round->cached_ownership_generation
			!= continuation->expected_x_ownership_generation)
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	else
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (entry->resource_x_no_progress_generation == 0
		&& entry->resource_x_no_progress_reason
			== RESOURCE_X_NO_PROGRESS_NONE)
		return RESOURCE_X_APPLY_APPLIED;
	if (entry->resource_x_no_progress_generation
			== continuation->acquisition_generation
		&& entry->resource_x_no_progress_reason
			== RESOURCE_X_NO_PROGRESS_BUFFER_BUSY)
		return RESOURCE_X_APPLY_APPLIED;
	return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
}

/* Register before rechecking the exact retained install receipt and its
 * observed predicate version.  Read the current progress lease under the
 * entry lock, never from a follower's saved observation.  Terminal PREUSE
 * has only the caller's fixed deadline.  Every wake requires a fresh probe. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_target_install_wait_exact(
	const ResourceXTargetInstallContinuation *continuation,
	ResourceXTargetInstallFollowState expected_follow_state,
	long timeout_ms)
{
	ResourceXAssertion assertion;
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryWaitContext wait_context;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	uint64 deadline_ms;
	uint64 effective_deadline_us;
	uint64 now_us;
	uint64 remaining_us;
	long sleep_ms;

	if (!pcm_resource_x_target_install_continuation_valid(
			continuation, &assertion)
		|| (expected_follow_state != RESOURCE_X_TARGET_INSTALL_INFLIGHT
			&& expected_follow_state
				!= RESOURCE_X_TARGET_INSTALL_PREUSE_RETRY)
		|| timeout_ms <= 0)
		return RESOURCE_X_APPLY_INVALID;
	memset(&wait_context, 0, sizeof(wait_context));
	if (!pcm_entry_ref_acquire(&continuation->resource, false,
			&wait_context.ref, &acquire_result))
		return RESOURCE_X_APPLY_STALE;
	if (wait_context.ref.binding_generation
		!= continuation->entry_binding_generation)
	{
		pcm_entry_ref_release(&wait_context.ref);
		return RESOURCE_X_APPLY_STALE;
	}
	entry = wait_context.ref.entry;
	pcm_entry_wait_ref_begin(&wait_context);
	ConditionVariablePrepareToSleep(&entry->wait_cv);
	wait_context.cv_prepared = true;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	result = pcm_resource_x_target_install_wait_recheck_locked(
		entry, round, continuation, &assertion, expected_follow_state);
	now_us = pcm_resource_x_monotonic_us();
	if ((result == RESOURCE_X_APPLY_APPLIED || result == RESOURCE_X_APPLY_DUPLICATE)
		&& now_us >= continuation->caller_absolute_deadline_us)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (result == RESOURCE_X_APPLY_APPLIED
			 && continuation->observed_head_change_generation != round->head_change_generation)
		result = RESOURCE_X_APPLY_DUPLICATE;
	effective_deadline_us = continuation->caller_absolute_deadline_us;
	if (expected_follow_state == RESOURCE_X_TARGET_INSTALL_INFLIGHT)
		effective_deadline_us = Min(effective_deadline_us, round->head_no_progress_deadline_us);
	LWLockRelease(&entry->entry_lock.lock);
	if (result != RESOURCE_X_APPLY_APPLIED) {
		pcm_entry_wait_context_cleanup(&wait_context);
		return result;
	}
	now_us = pcm_resource_x_monotonic_us();
	if (now_us >= effective_deadline_us) {
		pcm_entry_wait_context_cleanup(&wait_context);
		/* The head may have progressed after unlocking.  A sampled lease
		 * expiry only asks the exact driver to recheck the current head;
		 * this follower may fail only on its own fixed deadline. */
		return now_us >= continuation->caller_absolute_deadline_us ? RESOURCE_X_APPLY_BAD_STATE
																   : RESOURCE_X_APPLY_DUPLICATE;
	}
	remaining_us = effective_deadline_us - now_us;
	remaining_us = Min(remaining_us, continuation->requested_sleep_slice_us);
	deadline_ms = remaining_us / UINT64_C(1000);
	if (deadline_ms == 0) {
		pcm_entry_wait_context_cleanup(&wait_context);
		return RESOURCE_X_APPLY_APPLIED;
	}
	sleep_ms = timeout_ms;
	if (deadline_ms < (uint64) sleep_ms)
		sleep_ms = (long) deadline_ms;

	PG_TRY();
	{
		(void)ConditionVariableTimedSleep(&entry->wait_cv, sleep_ms,
			WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
		if (!pcm_entry_ref_identity_exact(&wait_context.ref)) {
			pcm_resource_x_reconfig_block();
			ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Resource-X target-install waiter entry identity changed "
					"after wakeup")));
		}
	}
	PG_FINALLY();
	{
		pcm_entry_wait_context_cleanup(&wait_context);
	}
	PG_END_TRY();
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_failure_snapshot_exact(
	const ResourceXAssertion *assertion, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation,
	uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation, uint64 retry_slice_us,
	ResourceXBootstrapRoundFailureSnapshot *out)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXAcquisitionRef ref;
	struct GrdEntry *entry;
	ResourceXApplyResult result;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || !resource_x_assertion_valid(assertion)
		|| current_master_node < 0
		|| current_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| resource_formation == 0 || resource_formation == UINT64_MAX
		|| master_session_incarnation == 0
		|| master_session_incarnation == UINT64_MAX
		|| r4_record_generation == 0
		|| r4_record_generation == UINT64_MAX
		|| requester_sender_connection_generation == 0
		|| requester_sender_connection_generation == UINT32_MAX
		|| master_ingress_connection_generation == 0
		|| master_ingress_connection_generation == UINT32_MAX
		|| retry_slice_us == 0 || retry_slice_us == UINT64_MAX)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY)
		result = RESOURCE_X_APPLY_NOT_FOUND;
	else if (round->phase > RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED
		|| round->request.assertion_sequence == 0
		|| round->request.assertion_sequence == UINT64_MAX
		|| (entry->resource_x_progress_flags
			& ~RESOURCE_X_PROGRESS_KNOWN_MASK) != 0)
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (!pcm_resource_x_node_request_key_matches_locked(
				 round, assertion, current_master_node, resource_formation,
				 master_session_incarnation, r4_record_generation,
				 requester_sender_connection_generation, master_ingress_connection_generation))
		result = RESOURCE_X_APPLY_STALE;
	else {
		memset(&ref, 0, sizeof(ref));
		ref.assertion = round->request.logical_assertion;
		ref.formation = round->resource_formation;
		ref.acquisition_generation
			= round->request.assertion_sequence;
		out->ref = ref;
		out->base_authority_generation = round->accepted_base;
		out->authority_generation
			= round->terminal_authority_generation;
		out->buffer_ownership_generation
			= round->cached_ownership_generation;
		out->r4_record_generation = round->r4_record_generation;
		out->master_session_incarnation
			= round->master_session_incarnation;
		out->absolute_deadline_us = round->head_no_progress_deadline_us;
		out->head_change_generation = round->head_change_generation;
		out->head_last_semantic_progress_us = round->head_last_semantic_progress_us;
		out->head_no_progress_budget_us = round->head_no_progress_budget_us;
		out->head_failure_reason = round->head_failure_reason;
		out->requester_base_generation
			= entry->resource_x_requester_base_generation;
		out->retired_acquisition_generation
			= entry->resource_x_retired_acquisition_generation;
		out->progress_flags = entry->resource_x_progress_flags;
		out->round_phase = round->phase;
		out->terminal
			= pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
				entry, round, &ref, master_session_incarnation,
				r4_record_generation,
				round->cached_ownership_generation)
			? 1 : 0;
		result = RESOURCE_X_APPLY_APPLIED;
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

bool
cluster_pcm_lock_resource_x_bootstrap_round_direct_init_snapshot_exact(
	const ResourceXAcquisitionRef *ref,
	uint64 *direct_init_ownership_generation_out,
	uint64 *direct_init_reservation_token_out)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	bool matches;

	if (direct_init_ownership_generation_out != NULL)
		*direct_init_ownership_generation_out = 0;
	if (direct_init_reservation_token_out != NULL)
		*direct_init_reservation_token_out = 0;
	if (!pcm_resource_x_ref_valid(ref)
		|| direct_init_ownership_generation_out == NULL
		|| direct_init_reservation_token_out == NULL)
		return false;
	if (!pcm_entry_ref_acquire(&ref->assertion.resource, false,
			&entry_ref, &acquire_result))
		return false;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	matches = round->phase == RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED
			  && resource_x_assertion_equal(&round->request.logical_assertion, &ref->assertion)
			  && round->resource_formation == ref->formation
			  && round->request.assertion_sequence == ref->acquisition_generation
			  && round->highest_attempt_floor == ref->acquisition_generation
			  && round->accepted_base != 0 && round->accepted_base != UINT64_MAX
			  && round->install_claim_source == RESOURCE_X_INSTALL_CLAIM_DIRECT_INIT
			  && round->install_claim_reservation_token != 0
			  && round->install_claim_reservation_token != UINT64_MAX
			  && round->install_claim_pending_generation != UINT64_MAX;
	if (matches) {
		*direct_init_ownership_generation_out = round->install_claim_pending_generation;
		*direct_init_reservation_token_out = round->install_claim_reservation_token;
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return matches;
}

bool
cluster_pcm_lock_resource_x_bootstrap_round_direct_init_matches_exact(
	const ResourceXAcquisitionRef *ref,
	uint64 direct_init_ownership_generation,
	uint64 direct_init_reservation_token)
{
	uint64 bound_generation;
	uint64 bound_token;

	if (direct_init_ownership_generation == UINT64_MAX
		|| direct_init_reservation_token == 0
		|| direct_init_reservation_token == UINT64_MAX
		|| !cluster_pcm_lock_resource_x_bootstrap_round_direct_init_snapshot_exact(
			ref, &bound_generation, &bound_token))
		return false;
	return bound_generation == direct_init_ownership_generation
		&& bound_token == direct_init_reservation_token;
}

static bool
pcm_resource_x_terminal_x_lineage_args_valid(
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation)
{
	return successor_block != NULL
		&& successor_block->kind == RESOURCE_X_WIRE_BLOCK_TO_N
		&& successor_block->payload_bytes == RESOURCE_X_CONTROL_V1_BYTES
		&& !successor_block->blocked_has_remote_proof
		&& resource_x_assertion_valid(
			&successor_block->common.logical_assertion)
		&& authenticated_master_node >= 0
		&& authenticated_master_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		&& cluster_node_id >= 0
		&& cluster_node_id < RESOURCE_X_PROTOCOL_NODE_LIMIT
		&& successor_block->common.action_node == cluster_node_id
		&& successor_block->common.base_authority_generation != 0
		&& successor_block->common.base_authority_generation != UINT64_MAX
		&& successor_block->common.authority_generation
			== successor_block->common.base_authority_generation
		&& successor_block->common.resource_formation != 0
		&& successor_block->common.resource_formation != UINT64_MAX
		&& successor_block->common.master_session_incarnation != 0
		&& successor_block->common.master_session_incarnation != UINT64_MAX
		&& successor_block->common.assertion_sequence != 0
		&& successor_block->common.assertion_sequence != UINT64_MAX
		&& successor_block->common.sender_connection_generation != 0
		&& successor_block->common.observed_mode == (uint8)PCM_STATE_X
		&& successor_block->common.target_mode == (uint8)PCM_STATE_N
		&& successor_block->common.source_candidate == 1
		&& successor_block->common.retain_pi_if_dirty == 1
		&& successor_block->common.outcome == RESOURCE_X_OUTCOME_NONE
		&& successor_block->common.flags == 0
		&& r4_record_generation != 0
		&& r4_record_generation != UINT64_MAX
		&& cached_ownership_generation != 0
		&& cached_ownership_generation != UINT64_MAX;
}

static bool
pcm_resource_x_terminal_x_lineage_locked(
	struct GrdEntry *entry, ClusterPcmResourceXBootstrapRound *round,
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation, bool require_direct_init,
	ResourceXTerminalXLineage *lineage_out)
{
	uint64 final_authority_generation;
	bool direct_init_lineage;
	bool matches;

	Assert(entry != NULL);
	Assert(round != NULL);
	Assert(successor_block != NULL);
	Assert(lineage_out != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		   || LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	final_authority_generation = round->terminal_authority_generation;
	direct_init_lineage = round->install_claim_source == RESOURCE_X_INSTALL_CLAIM_DIRECT_INIT;
	matches
		= round->phase == RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
		  && resource_x_assertion_valid(&round->request.logical_assertion)
		  && round->request.logical_assertion.requester_node == cluster_node_id
		  && BufferTagsEqual(&round->request.logical_assertion.resource,
							 &successor_block->common.logical_assertion.resource)
		  && !resource_x_assertion_equal(&round->request.logical_assertion,
										 &successor_block->common.logical_assertion)
		  && pcm_resource_x_ref_valid(&round->terminal_ref)
		  && resource_x_assertion_equal(&round->terminal_ref.assertion,
										&round->request.logical_assertion)
		  && round->terminal_ref.formation == successor_block->common.resource_formation
		  && round->terminal_ref.acquisition_generation == round->request.assertion_sequence
		  && round->request.assertion_sequence != 0
		  && round->request.assertion_sequence != UINT64_MAX
		  && round->highest_attempt_floor == round->request.assertion_sequence
		  && round->resource_formation == successor_block->common.resource_formation
		  && round->master_session_incarnation == successor_block->common.master_session_incarnation
		  && round->r4_record_generation == r4_record_generation
		  && round->current_master_node == authenticated_master_node && round->accepted_base != 0
		  && round->accepted_base != UINT64_MAX && final_authority_generation > round->accepted_base
		  && final_authority_generation != UINT64_MAX
		  && successor_block->common.base_authority_generation == final_authority_generation
		  && (!require_direct_init || direct_init_lineage)
		  && pcm_resource_x_install_claim_valid_locked(round)
		  && (round->install_claim_source == RESOURCE_X_INSTALL_CLAIM_NONE
			  || round->install_claim_pending_generation + 1 == cached_ownership_generation)
		  && round->cached_ownership_generation == cached_ownership_generation
		  && entry->resource_x_retired_acquisition_generation
				 == round->terminal_ref.acquisition_generation;
	if (!matches)
		return false;
	lineage_out->holder_assertion = round->terminal_ref.assertion;
	lineage_out->holder_attempt
		= round->terminal_ref.acquisition_generation;
	lineage_out->resource_formation = round->resource_formation;
	lineage_out->master_session_incarnation
		= round->master_session_incarnation;
	lineage_out->accepted_base_authority_generation
		= round->accepted_base;
	lineage_out->final_authority_generation
		= final_authority_generation;
	lineage_out->direct_init_ownership_generation
		= direct_init_lineage ? round->install_claim_pending_generation : 0;
	lineage_out->cached_ownership_generation
		= round->cached_ownership_generation;
	lineage_out->r4_record_generation = round->r4_record_generation;
	lineage_out->successor_attempt
		= successor_block->common.assertion_sequence;
	lineage_out->master_node = round->current_master_node;
	lineage_out->successor_node
		= successor_block->common.logical_assertion.requester_node;
	return true;
}

/* Read-only lineage for the narrow tagless holder downgrade after L3.  The
 * retained requester round proves only that the current local X generation
 * is the exact product of a known-new direct-init Resource-X grant.  It does
 * not create holder authority; the authenticated type-17 and the master
 * entry still own the downgrade decision. */
bool
cluster_pcm_lock_resource_x_bootstrap_round_direct_init_terminal_holder_exact(
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation,
	ResourceXTerminalXLineage *lineage_out)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	bool matches = false;

	if (lineage_out != NULL)
		memset(lineage_out, 0, sizeof(*lineage_out));
	if (lineage_out == NULL
		|| !pcm_resource_x_terminal_x_lineage_args_valid(
			successor_block, authenticated_master_node,
			r4_record_generation, cached_ownership_generation))
		return false;
	if (!pcm_entry_ref_acquire(
			&successor_block->common.logical_assertion.resource, false,
			&entry_ref, &acquire_result))
		return false;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	matches = entry->resource_x_local_owner.state
			  == RESOURCE_X_LOCAL_OWNER_EMPTY
		&& pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner)
		&& pcm_resource_x_terminal_x_lineage_locked(
			entry, round, successor_block, authenticated_master_node,
			r4_record_generation, cached_ownership_generation, true,
			lineage_out);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return matches;
}

/* The same retained terminal cover also exists after an ordinary type-15/R9
 * install.  Its exact final grant generation, rather than a creation-only
 * direct-init token, joins the authenticated successor type-17 to the current
 * cached X generation.  This retained lineage is not a second authority. */
bool
cluster_pcm_lock_resource_x_bootstrap_round_terminal_holder_exact(
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation,
	ResourceXTerminalXLineage *lineage_out)
{
	static bool diagnostic_emitted = false;
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXAssertion holder_assertion;
	ResourceXAcquisitionRef terminal_ref;
	uint64 holder_attempt = 0;
	uint64 highest_attempt = 0;
	uint64 resource_formation = 0;
	uint64 master_session = 0;
	uint64 accepted_base = 0;
	uint64 terminal_authority = 0;
	uint64 round_cached_generation = 0;
	uint64 round_r4_generation = 0;
	uint64 retired_generation = 0;
	uint64 direct_init_generation = 0;
	uint64 direct_init_token = 0;
	uint32 local_owner_state = 0;
	int32 current_master = -1;
	uint8 round_phase = 0;
	bool local_owner_valid = false;
	bool terminal_ref_valid = false;
	bool same_tag = false;
	bool distinct_assertion = false;
	bool matches = false;

	if (lineage_out != NULL)
		memset(lineage_out, 0, sizeof(*lineage_out));
	if (lineage_out == NULL
		|| !pcm_resource_x_terminal_x_lineage_args_valid(
			successor_block, authenticated_master_node,
			r4_record_generation, cached_ownership_generation))
		return false;
	if (!pcm_entry_ref_acquire(
			&successor_block->common.logical_assertion.resource, false,
			&entry_ref, &acquire_result))
		return false;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	matches = pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner)
		&& pcm_resource_x_terminal_x_lineage_locked(
			entry, round, successor_block, authenticated_master_node,
			r4_record_generation, cached_ownership_generation, false,
			lineage_out);
	if (matches) {
		ClusterPcmResourceXLocalOwner *owner
			= &entry->resource_x_local_owner;

		matches = owner->state == RESOURCE_X_LOCAL_OWNER_EMPTY
			|| (owner->state == RESOURCE_X_LOCAL_OWNER_RECYCLING
				&& pcm_resource_x_local_owner_round_exact_locked(
					entry, round))
			|| (owner->state == RESOURCE_X_LOCAL_OWNER_HANDOFF
				&& pcm_resource_x_local_owner_round_exact_locked(
					entry, round)
				&& pcm_resource_x_local_handoff_successor_exact(
					&owner->handoff, successor_block,
					authenticated_master_node, r4_record_generation,
					cached_ownership_generation));
	}
	if (!matches && !diagnostic_emitted) {
		holder_assertion = round->request.logical_assertion;
		terminal_ref = round->terminal_ref;
		holder_attempt = round->request.assertion_sequence;
		highest_attempt = round->highest_attempt_floor;
		resource_formation = round->resource_formation;
		master_session = round->master_session_incarnation;
		accepted_base = round->accepted_base;
		terminal_authority = round->terminal_authority_generation;
		round_cached_generation = round->cached_ownership_generation;
		round_r4_generation = round->r4_record_generation;
		retired_generation
			= entry->resource_x_retired_acquisition_generation;
		direct_init_generation = round->install_claim_pending_generation;
		direct_init_token = round->install_claim_reservation_token;
		local_owner_state = entry->resource_x_local_owner.state;
		current_master = round->current_master_node;
		round_phase = round->phase;
		local_owner_valid = pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner);
		terminal_ref_valid = pcm_resource_x_ref_valid(&round->terminal_ref);
		same_tag = BufferTagsEqual(
			&round->request.logical_assertion.resource,
			&successor_block->common.logical_assertion.resource);
		distinct_assertion = !resource_x_assertion_equal(
			&round->request.logical_assertion,
			&successor_block->common.logical_assertion);
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	if (!matches && !diagnostic_emitted) {
		diagnostic_emitted = true;
		ereport(LOG,
				(errmsg_internal("Resource-X terminal-X lineage diagnostic"),
				 errdetail("phase=%u local_owner=%u local_owner_valid=%s "
						   "holder=%d successor=%d same_tag=%s distinct=%s "
						   "terminal_ref_valid=%s holder_attempt=%llu "
						   "terminal_attempt=%llu highest=%llu retired=%llu "
						   "round_formation=%llu successor_formation=%llu "
						   "round_session=%llu successor_session=%llu "
						   "round_master=%d authenticated_master=%d "
						   "round_r4=%llu caller_r4=%llu accepted_base=%llu "
						   "terminal_authority=%llu successor_base=%llu "
						   "round_cached=%llu caller_cached=%llu "
						   "direct_init_generation=%llu direct_init_token=%llu",
						   (unsigned)round_phase, (unsigned)local_owner_state,
						   local_owner_valid ? "true" : "false",
						   holder_assertion.requester_node,
						   successor_block->common.logical_assertion.requester_node,
						   same_tag ? "true" : "false",
						   distinct_assertion ? "true" : "false",
						   terminal_ref_valid ? "true" : "false",
						   (unsigned long long)holder_attempt,
						   (unsigned long long)terminal_ref.acquisition_generation,
						   (unsigned long long)highest_attempt,
						   (unsigned long long)retired_generation,
						   (unsigned long long)resource_formation,
						   (unsigned long long)successor_block->common.resource_formation,
						   (unsigned long long)master_session,
						   (unsigned long long)successor_block->common.master_session_incarnation,
						   current_master, authenticated_master_node,
						   (unsigned long long)round_r4_generation,
						   (unsigned long long)r4_record_generation,
						   (unsigned long long)accepted_base,
						   (unsigned long long)terminal_authority,
						   (unsigned long long)successor_block->common.base_authority_generation,
						   (unsigned long long)round_cached_generation,
						   (unsigned long long)cached_ownership_generation,
						   (unsigned long long)direct_init_generation,
						   (unsigned long long)direct_init_token)));
	}
	return matches;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_itl_recycle_begin_exact(
	const ResourceXAcquisitionRef *ref,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint64 cached_ownership_generation, uint64 reservation_token,
	int32 owner_procno, uint64 now_us pg_attribute_unused(),
	ResourceXLocalOwnerHandle *handle_out)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	bool broadcast = false;

	if (handle_out != NULL)
		memset(handle_out, 0, sizeof(*handle_out));
	if (!pcm_resource_x_ref_valid(ref) || handle_out == NULL
		|| master_session_incarnation == 0
		|| master_session_incarnation == UINT64_MAX
		|| r4_record_generation == 0
		|| r4_record_generation == UINT64_MAX
		|| cached_ownership_generation == 0
		|| cached_ownership_generation == UINT64_MAX
		|| reservation_token == 0 || reservation_token == UINT64_MAX
		|| owner_procno < 0 || now_us == 0 || now_us == UINT64_MAX)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&ref->assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	if (!pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else {
		broadcast = pcm_resource_x_local_owner_expire_locked(entry, now_us);
		if (!cluster_pcm_lock_resource_x_gate_open_exact(ref->formation)
		|| !pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
			entry, round, ref, master_session_incarnation,
			r4_record_generation, cached_ownership_generation))
			result = RESOURCE_X_APPLY_STALE;
		else
			result = pcm_resource_x_local_owner_claim_locked(
				entry, RESOURCE_X_LOCAL_OWNER_RECYCLING, ref,
				master_session_incarnation, r4_record_generation,
				cached_ownership_generation, reservation_token,
				owner_procno, handle_out);
	}
	if (broadcast || result == RESOURCE_X_APPLY_APPLIED)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_itl_recycle_finish_exact(
	const ResourceXLocalOwnerHandle *handle,
	uint64 now_us pg_attribute_unused())
{
	ClusterPcmResourceXBootstrapRound *round;
	ClusterPcmResourceXLocalOwner *owner;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	bool broadcast = false;

	if (!pcm_resource_x_local_owner_handle_valid(handle)
		|| now_us == 0 || now_us == UINT64_MAX)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&handle->ref.assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	owner = &entry->resource_x_local_owner;
	if (!pcm_resource_x_local_owner_valid_locked(
			owner))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (owner->state
			 != RESOURCE_X_LOCAL_OWNER_RECYCLING
		|| !pcm_resource_x_local_owner_handle_equal(
			&owner->handle, handle))
		result = RESOURCE_X_APPLY_STALE;
	else {
		broadcast = pcm_resource_x_local_owner_expire_locked(entry, now_us);
		if (!cluster_pcm_lock_resource_x_gate_open_exact(
			handle->ref.formation)
		|| !pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
			entry, round, &handle->ref,
			handle->master_session_incarnation,
			handle->r4_record_generation,
			handle->buffer_ownership_generation)) {
			broadcast = pcm_resource_x_local_owner_priority_clear_locked(entry) || broadcast;
			result = RESOURCE_X_APPLY_STALE;
		} else if (pcm_resource_x_local_handoff_valid(&owner->handoff)) {
			if (!pcm_resource_x_local_handoff_round_current_locked(
					&owner->handoff, round)) {
				broadcast = pcm_resource_x_local_owner_priority_clear_locked(entry) || broadcast;
				result = RESOURCE_X_APPLY_STALE;
			} else {
				memset(&owner->handle, 0, sizeof(owner->handle));
				owner->state = RESOURCE_X_LOCAL_OWNER_HANDOFF;
				result = pcm_resource_x_local_owner_valid_locked(owner)
								 && pcm_resource_x_head_changed_locked(entry, true, now_us)
							 ? RESOURCE_X_APPLY_APPLIED
							 : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			}
		} else
			result = pcm_resource_x_local_owner_release_locked(
				entry, RESOURCE_X_LOCAL_OWNER_RECYCLING, handle);
	}
	if (broadcast || result == RESOURCE_X_APPLY_APPLIED)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_itl_recycle_cancel_exact(
	const ResourceXLocalOwnerHandle *handle)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;

	if (!pcm_resource_x_local_owner_handle_valid(handle))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&handle->ref.assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	result = pcm_resource_x_local_owner_release_locked(
		entry, RESOURCE_X_LOCAL_OWNER_RECYCLING, handle);
	if (result == RESOURCE_X_APPLY_APPLIED)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_terminal_x_revoke_claim_exact(
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation, uint64 reservation_token,
	int32 owner_procno, uint64 now_us pg_attribute_unused(),
	ResourceXTerminalXLineage *lineage_out,
	ResourceXLocalOwnerHandle *handle_out)
{
	ClusterPcmResourceXBootstrapRound *round;
	ClusterPcmResourceXLocalHandoff retained_handoff;
	ClusterPcmResourceXLocalOwner *owner;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	bool broadcast = false;
	bool lineage_matches;

	if (lineage_out != NULL)
		memset(lineage_out, 0, sizeof(*lineage_out));
	if (handle_out != NULL)
		memset(handle_out, 0, sizeof(*handle_out));
	if (lineage_out == NULL || handle_out == NULL
		|| reservation_token == 0 || reservation_token == UINT64_MAX
		|| owner_procno < 0 || now_us == 0 || now_us == UINT64_MAX
		|| !pcm_resource_x_terminal_x_lineage_args_valid(
			successor_block, authenticated_master_node,
			r4_record_generation, cached_ownership_generation))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(
			&successor_block->common.logical_assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	owner = &entry->resource_x_local_owner;
	if (!pcm_resource_x_local_owner_valid_locked(
			owner))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (pcm_resource_x_local_owner_expire_locked(entry, now_us)) {
		broadcast = true;
		result = RESOURCE_X_APPLY_STALE;
	} else {
		lineage_matches = pcm_resource_x_terminal_x_lineage_locked(
			entry, round, successor_block, authenticated_master_node,
			r4_record_generation, cached_ownership_generation, false,
			lineage_out);
		if (owner->state == RESOURCE_X_LOCAL_OWNER_RECYCLING) {
			if (!lineage_matches) {
				broadcast = pcm_resource_x_local_owner_priority_clear_locked(entry);
				result = RESOURCE_X_APPLY_STALE;
			} else if (pcm_resource_x_local_handoff_empty(
					&owner->handoff)) {
				result = pcm_resource_x_local_handoff_freeze_locked(
					entry, round, successor_block, authenticated_master_node, r4_record_generation,
					cached_ownership_generation, now_us);
				if (result == RESOURCE_X_APPLY_APPLIED) {
					broadcast = true;
					result = RESOURCE_X_APPLY_BAD_STATE;
				}
			} else
				result = pcm_resource_x_local_handoff_successor_exact(
					&owner->handoff, successor_block,
					authenticated_master_node, r4_record_generation,
					cached_ownership_generation)
					? RESOURCE_X_APPLY_BAD_STATE
					: RESOURCE_X_APPLY_STALE;
		} else if (owner->state == RESOURCE_X_LOCAL_OWNER_HANDOFF) {
			if (!pcm_resource_x_local_handoff_valid(&owner->handoff)
				|| now_us >= owner->handoff.deadline_us
				|| !lineage_matches
				|| !pcm_resource_x_local_handoff_round_current_locked(
					&owner->handoff, round)) {
				broadcast = pcm_resource_x_local_owner_priority_clear_locked(entry);
				result = RESOURCE_X_APPLY_STALE;
			} else if (!pcm_resource_x_local_handoff_successor_exact(
					&owner->handoff, successor_block,
					authenticated_master_node, r4_record_generation,
					cached_ownership_generation))
				result = RESOURCE_X_APPLY_STALE;
			else {
				retained_handoff = owner->handoff;
				pcm_resource_x_local_owner_clear_locked(entry);
				result = pcm_resource_x_local_owner_claim_locked(
					entry, RESOURCE_X_LOCAL_OWNER_REVOKING,
					&retained_handoff.holder_ref,
					retained_handoff.master_session_incarnation,
					retained_handoff.r4_record_generation,
					retained_handoff.buffer_ownership_generation,
					reservation_token, owner_procno, handle_out);
				if (result == RESOURCE_X_APPLY_APPLIED) {
					owner->handoff = retained_handoff;
					if (!pcm_resource_x_local_owner_valid_locked(owner))
						result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
				}
				broadcast = result == RESOURCE_X_APPLY_APPLIED;
			}
		} else if (owner->state == RESOURCE_X_LOCAL_OWNER_REVOKING) {
			/* Only an exact replay of the successor already retained by this
			 * active owner is retryable.  A conflicting successor must not be
			 * allowed to hide behind the same generic BUSY disposition: reject it
			 * as STALE without changing owner, HANDOFF, or the local deadline. */
			if (!pcm_resource_x_local_handoff_valid(&owner->handoff)
				|| now_us >= owner->handoff.deadline_us
				|| !lineage_matches
				|| !pcm_resource_x_local_handoff_round_current_locked(
					&owner->handoff, round))
				result = RESOURCE_X_APPLY_STALE;
			else
				result = pcm_resource_x_local_handoff_successor_exact(
					&owner->handoff, successor_block,
					authenticated_master_node, r4_record_generation,
					cached_ownership_generation)
					? RESOURCE_X_APPLY_BAD_STATE
					: RESOURCE_X_APPLY_STALE;
		}
		else if (!lineage_matches)
			result = RESOURCE_X_APPLY_STALE;
		else {
			/* The first authenticated exact type-17 owns the same bounded
			 * successor priority whether it encountered RECYCLING or EMPTY.
			 * Build the HANDOFF identity before claiming, then publish both
			 * under this one entry-lock hold.  No process handle or authority
			 * survives if either exact step fails. */
			memset(&retained_handoff, 0, sizeof(retained_handoff));
			result = pcm_resource_x_local_handoff_build_locked(
				&retained_handoff, round, successor_block,
				authenticated_master_node, r4_record_generation,
				cached_ownership_generation, now_us);
			if (result == RESOURCE_X_APPLY_APPLIED)
				result = pcm_resource_x_local_owner_claim_locked(
					entry, RESOURCE_X_LOCAL_OWNER_REVOKING,
					&retained_handoff.holder_ref,
					retained_handoff.master_session_incarnation,
					retained_handoff.r4_record_generation,
					retained_handoff.buffer_ownership_generation,
					reservation_token, owner_procno, handle_out);
			if (result == RESOURCE_X_APPLY_APPLIED) {
				owner->handoff = retained_handoff;
				if (!pcm_resource_x_local_owner_valid_locked(owner)) {
					pcm_resource_x_local_owner_clear_locked(entry);
					memset(handle_out, 0, sizeof(*handle_out));
					result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
				}
			}
		}
	}
	if (broadcast || result == RESOURCE_X_APPLY_APPLIED)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

/* A retained type-17 callback cannot adopt the raw pin held by the callback
 * which first claimed REVOKING.  Observe that owner under the entry lock and
 * distinguish the byte-exact active replay from a conflicting successor.
 * This helper is deliberately read-only: it never expires, clears, claims, or
 * refreshes the HANDOFF owner or its original absolute deadline. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_terminal_x_revoke_replay_exact(
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation, uint64 now_us,
	ResourceXTerminalXLineage *lineage_out)
{
	ClusterPcmResourceXBootstrapRound *round;
	ClusterPcmResourceXLocalOwner *owner;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;

	if (lineage_out != NULL)
		memset(lineage_out, 0, sizeof(*lineage_out));
	if (lineage_out == NULL || now_us == 0 || now_us == UINT64_MAX
		|| !pcm_resource_x_terminal_x_lineage_args_valid(
			successor_block, authenticated_master_node,
			r4_record_generation, cached_ownership_generation))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(
			&successor_block->common.logical_assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	owner = &entry->resource_x_local_owner;
	if (!pcm_resource_x_local_owner_valid_locked(owner)
		|| owner->state != RESOURCE_X_LOCAL_OWNER_REVOKING
		|| !pcm_resource_x_local_handoff_valid(&owner->handoff))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (now_us >= owner->handoff.deadline_us
		|| !pcm_resource_x_local_handoff_round_current_locked(
			&owner->handoff, round)
		|| !pcm_resource_x_terminal_x_lineage_locked(
			entry, round, successor_block, authenticated_master_node,
			r4_record_generation, cached_ownership_generation, false,
			lineage_out))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (!pcm_resource_x_local_handoff_successor_exact(
			&owner->handoff, successor_block, authenticated_master_node,
			r4_record_generation, cached_ownership_generation))
		result = RESOURCE_X_APPLY_STALE;
	else
		result = RESOURCE_X_APPLY_BAD_STATE;
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

bool
cluster_pcm_lock_resource_x_terminal_x_revoke_revalidate_held_exact(
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	uint64 cached_ownership_generation,
	const ResourceXLocalOwnerHandle *handle,
	ResourceXTerminalXLineage *lineage_out)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	bool matches;

	if (lineage_out != NULL)
		memset(lineage_out, 0, sizeof(*lineage_out));
	if (lineage_out == NULL
		|| !pcm_resource_x_local_owner_handle_valid(handle)
		|| !pcm_resource_x_terminal_x_lineage_args_valid(
			successor_block, authenticated_master_node,
			r4_record_generation, cached_ownership_generation))
		return false;
	if (!pcm_entry_ref_acquire(
			&successor_block->common.logical_assertion.resource, false,
			&entry_ref, &acquire_result))
		return false;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	matches = pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner)
		&& entry->resource_x_local_owner.state
			== RESOURCE_X_LOCAL_OWNER_REVOKING
		&& pcm_resource_x_local_owner_handle_equal(
			&entry->resource_x_local_owner.handle, handle)
		&& pcm_resource_x_terminal_x_lineage_locked(
			entry, round, successor_block, authenticated_master_node,
			r4_record_generation, cached_ownership_generation, false,
			lineage_out);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return matches;
}

/* VM/FSM DROP removes the exact descriptor mapping and therefore has no
 * retained N+PI carrier on which SourceSettlement can later serialize cover
 * cleanup.  Consume the old cached-X cover only after the caller presents the
 * exact post-drop generation produced from this REVOKING owner.  The
 * transition grants no authority: it only closes the terminal binding and its
 * process-local owner in one entry-lock critical section. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_terminal_x_revoke_finish_drop_exact(
	const ResourceXDecodedFrame *successor_block,
	int32 authenticated_master_node, uint64 r4_record_generation,
	const ClusterPcmOwnSnapshot *revoking,
	const ClusterPcmOwnSnapshot *dropped,
	const ResourceXLocalOwnerHandle *handle)
{
	ClusterPcmResourceXBootstrapRound *round;
	ClusterPcmResourceXLocalOwner *owner;
	ResourceXTerminalXLineage lineage;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	bool broadcast = false;

	memset(&lineage, 0, sizeof(lineage));
	if (!pcm_resource_x_local_owner_handle_valid(handle)
		|| revoking == NULL || dropped == NULL
		|| !pcm_resource_x_terminal_x_lineage_args_valid(
			successor_block, authenticated_master_node,
			r4_record_generation, revoking->generation)
		|| revoking->pcm_state != (uint8)PCM_STATE_X
		|| revoking->flags != PCM_OWN_FLAG_REVOKING
		|| revoking->generation == 0
		|| revoking->generation == UINT64_MAX
		|| revoking->reservation_token == 0
		|| revoking->reservation_token == UINT64_MAX
		|| revoking->writer_activation_token != 0
		|| revoking->resource_x_activation_generation != 0
		|| dropped->pcm_state != (uint8)PCM_STATE_N
		|| dropped->flags != 0
		|| dropped->writer_activation_token != 0
		|| dropped->resource_x_activation_generation != 0)
		return RESOURCE_X_APPLY_INVALID;
	if (!BufferTagsEqual(&revoking->tag, &dropped->tag)
		|| !BufferTagsEqual(
			&revoking->tag,
			&successor_block->common.logical_assertion.resource)
		|| dropped->generation != revoking->generation + 1
		|| dropped->reservation_token != revoking->reservation_token
		|| handle->buffer_ownership_generation != revoking->generation
		|| revoking->reservation_token != handle->reservation_token + 1
		|| handle->r4_record_generation != r4_record_generation)
		return RESOURCE_X_APPLY_STALE;
	if (!pcm_entry_ref_acquire(&revoking->tag, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	owner = &entry->resource_x_local_owner;
	if (!pcm_resource_x_local_owner_valid_locked(owner))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (owner->state == RESOURCE_X_LOCAL_OWNER_EMPTY)
		result = RESOURCE_X_APPLY_NOT_FOUND;
	else if (owner->state != RESOURCE_X_LOCAL_OWNER_REVOKING
		|| !pcm_resource_x_local_owner_handle_equal(
			&owner->handle, handle)
		|| !pcm_resource_x_local_handoff_valid(&owner->handoff)
		|| !pcm_resource_x_local_handoff_successor_exact(
			&owner->handoff, successor_block,
			authenticated_master_node, r4_record_generation,
			revoking->generation)
		|| !pcm_resource_x_local_handoff_round_current_locked(
			&owner->handoff, round)
		|| !cluster_pcm_lock_resource_x_gate_open_exact(
			successor_block->common.resource_formation)
		|| !pcm_resource_x_terminal_x_lineage_locked(
			entry, round, successor_block, authenticated_master_node,
			r4_record_generation, revoking->generation, false, &lineage)
		|| !pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
			entry, round, &handle->ref,
			handle->master_session_incarnation,
			handle->r4_record_generation,
			handle->buffer_ownership_generation))
		result = RESOURCE_X_APPLY_STALE;
	else {
		pcm_resource_x_bootstrap_round_clear_binding_locked(round);
		pcm_resource_x_local_owner_clear_locked(entry);
		broadcast = true;
		result = RESOURCE_X_APPLY_APPLIED;
	}
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

/* Return a pre-semantic terminal-X claimant to its exact bounded priority
 * after the BufferDesc REVOKING token has been aborted.  A claim which did
 * not consume HANDOFF simply leaves the local-owner slot empty.  The first
 * observation deadline and owner-generation floor are never refreshed. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact(
	const ResourceXLocalOwnerHandle *handle, uint64 now_us)
{
	ClusterPcmResourceXBootstrapRound *round;
	ClusterPcmResourceXLocalOwner *owner;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	bool broadcast = false;

	if (!pcm_resource_x_local_owner_handle_valid(handle)
		|| now_us == 0 || now_us == UINT64_MAX)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&handle->ref.assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	owner = &entry->resource_x_local_owner;
	if (!pcm_resource_x_local_owner_valid_locked(owner))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (owner->state != RESOURCE_X_LOCAL_OWNER_REVOKING
		|| !pcm_resource_x_local_owner_handle_equal(
			&owner->handle, handle))
		result = RESOURCE_X_APPLY_STALE;
	else if (pcm_resource_x_local_handoff_valid(&owner->handoff)) {
		if (now_us >= owner->handoff.deadline_us
			|| !cluster_pcm_lock_resource_x_gate_open_exact(
				handle->ref.formation)
			|| !pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
				entry, round, &handle->ref,
				handle->master_session_incarnation,
				handle->r4_record_generation,
				handle->buffer_ownership_generation)
			|| !pcm_resource_x_local_handoff_round_current_locked(
				&owner->handoff, round)) {
			pcm_resource_x_local_owner_clear_locked(entry);
			broadcast = true;
			result = RESOURCE_X_APPLY_STALE;
		} else {
			memset(&owner->handle, 0, sizeof(owner->handle));
			owner->state = RESOURCE_X_LOCAL_OWNER_HANDOFF;
			result = pcm_resource_x_local_owner_valid_locked(owner)
							 && pcm_resource_x_head_changed_locked(entry, true, now_us)
						 ? RESOURCE_X_APPLY_APPLIED
						 : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			broadcast = result == RESOURCE_X_APPLY_APPLIED;
		}
	} else {
		pcm_resource_x_local_owner_clear_locked(entry);
		broadcast = true;
		result = RESOURCE_X_APPLY_APPLIED;
	}
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_terminal_x_revoke_release_exact(
	const ResourceXLocalOwnerHandle *handle)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;

	if (!pcm_resource_x_local_owner_handle_valid(handle))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&handle->ref.assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	result = pcm_resource_x_local_owner_release_locked(
		entry, RESOURCE_X_LOCAL_OWNER_REVOKING, handle);
	if (result == RESOURCE_X_APPLY_APPLIED)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_publish_terminal_exact(
	const ResourceXAcquisitionRef *ref, uint64 master_session_incarnation,
	uint64 r4_record_generation, uint64 cached_ownership_generation,
	uint64 terminal_authority_generation, uint64 now_us)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	bool broadcast = false;

	if (!pcm_resource_x_ref_valid(ref)
		|| master_session_incarnation == 0
		|| master_session_incarnation == UINT64_MAX
		|| r4_record_generation == 0
		|| r4_record_generation == UINT64_MAX
		|| cached_ownership_generation == 0
		|| cached_ownership_generation == UINT64_MAX
		|| terminal_authority_generation == 0
		|| terminal_authority_generation == UINT64_MAX
		|| now_us == 0 || now_us == UINT64_MAX)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&ref->assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	if (pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
			entry, round, ref, master_session_incarnation,
			r4_record_generation, cached_ownership_generation))
		result = round->terminal_authority_generation
			== terminal_authority_generation
			? RESOURCE_X_APPLY_DUPLICATE : RESOURCE_X_APPLY_STALE;
	else if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED
			 || !resource_x_assertion_equal(&round->request.logical_assertion, &ref->assertion)
			 || round->resource_formation != ref->formation
			 || round->master_session_incarnation != master_session_incarnation
			 || round->r4_record_generation != r4_record_generation
			 || round->request.assertion_sequence != ref->acquisition_generation
			 || round->highest_attempt_floor != ref->acquisition_generation
			 || round->accepted_base == 0 || round->accepted_base == UINT64_MAX
			 || terminal_authority_generation <= round->accepted_base
			 || now_us >= round->head_no_progress_deadline_us
			 || !cluster_pcm_lock_resource_x_gate_open_exact(ref->formation)
			 || !pcm_resource_x_active_empty_locked(entry)
			 || entry->resource_x_retired_acquisition_generation != ref->acquisition_generation)
		result = RESOURCE_X_APPLY_STALE;
	else {
		round->terminal_ref = *ref;
		round->terminal_authority_generation
			= terminal_authority_generation;
		round->cached_ownership_generation = cached_ownership_generation;
		round->phase = RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED;
		broadcast = true;
		result = pcm_resource_x_head_changed_locked(entry, true, now_us)
					 ? RESOURCE_X_APPLY_APPLIED
					 : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

bool
cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(
	const ResourceXAcquisitionRef *ref, uint64 master_session_incarnation,
	uint64 r4_record_generation, uint64 cached_ownership_generation)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	bool matches;

	if (!pcm_resource_x_ref_valid(ref)
		|| master_session_incarnation == 0
		|| master_session_incarnation == UINT64_MAX
		|| r4_record_generation == 0
		|| r4_record_generation == UINT64_MAX
		|| cached_ownership_generation == 0
		|| cached_ownership_generation == UINT64_MAX)
		return false;
	if (!pcm_entry_ref_acquire(&ref->assertion.resource, false,
			&entry_ref, &acquire_result))
		return false;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	matches = pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
		entry, round, ref, master_session_incarnation,
		r4_record_generation, cached_ownership_generation);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return matches;
}

static void
pcm_resource_x_target_evict_release_build_locked(
	const ClusterPcmResourceXBootstrapRound *round,
	uint32 sender_connection_generation, ResourceXDecodedFrame *release_out)
{
	Assert(round != NULL);
	Assert(release_out != NULL);
	Assert(sender_connection_generation != 0
		&& sender_connection_generation != UINT32_MAX);

	memset(release_out, 0, sizeof(*release_out));
	release_out->kind = RESOURCE_X_WIRE_RELEASE_X;
	release_out->payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	release_out->common.logical_assertion = round->terminal_ref.assertion;
	release_out->common.base_authority_generation = round->accepted_base;
	release_out->common.authority_generation
		= round->terminal_authority_generation;
	release_out->common.resource_formation = round->resource_formation;
	release_out->common.master_session_incarnation
		= round->master_session_incarnation;
	release_out->common.assertion_sequence
		= round->terminal_ref.acquisition_generation;
	release_out->common.ordered_lane = round->request.ordered_lane;
	release_out->common.action_node
		= round->terminal_ref.assertion.requester_node;
	release_out->common.observed_mode = (uint8)PCM_STATE_X;
	release_out->common.target_mode = (uint8)PCM_STATE_N;
	release_out->common.sender_connection_generation
		= sender_connection_generation;
	release_out->common.outcome = RESOURCE_X_OUTCOME_OK;
}

static bool
pcm_resource_x_target_evict_owner_exact_locked(
	const ClusterPcmResourceXLocalOwner *owner,
	const ClusterPcmResourceXBootstrapRound *round,
	uint64 master_session_incarnation, uint64 r4_record_generation,
	uint64 cached_ownership_generation, uint64 reservation_token,
	int32 owner_procno)
{
	return pcm_resource_x_local_owner_valid_locked(owner)
		   && owner->state == RESOURCE_X_LOCAL_OWNER_EVICTING
		   && pcm_resource_x_acquisition_ref_equal(&owner->handle.ref, &round->terminal_ref)
		   && owner->handle.master_session_incarnation == master_session_incarnation
		   && owner->handle.r4_record_generation == r4_record_generation
		   && owner->handle.buffer_ownership_generation == cached_ownership_generation
		   && owner->handle.reservation_token == reservation_token
		   && owner->handle.absolute_deadline_us == round->head_no_progress_deadline_us
		   && owner->handle.owner_procno == owner_procno;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_target_evict_prepare_exact(
	const BufferTag *tag, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation, uint64 cached_ownership_generation,
	uint64 reservation_token, uint32 sender_connection_generation,
	int32 owner_procno, ResourceXDecodedFrame *release_out,
	ResourceXLocalOwnerHandle *handle_out)
{
	ClusterPcmResourceXBootstrapRound *round;
	ClusterPcmResourceXLocalOwner *owner;
	ClusterPcmResourceXMasterRequest *successor = NULL;
	ClusterPcmResourceXMasterState *master_state = NULL;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result = RESOURCE_X_APPLY_STALE;
	bool broadcast = false;

	if (release_out != NULL)
		memset(release_out, 0, sizeof(*release_out));
	if (handle_out != NULL)
		memset(handle_out, 0, sizeof(*handle_out));
	if (tag == NULL || release_out == NULL || handle_out == NULL
		|| current_master_node < 0
		|| current_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| resource_formation == 0 || resource_formation == UINT64_MAX
		|| master_session_incarnation == 0
		|| master_session_incarnation == UINT64_MAX
		|| r4_record_generation == 0
		|| r4_record_generation == UINT64_MAX
		|| cached_ownership_generation == 0
		|| cached_ownership_generation == UINT64_MAX
		|| reservation_token == 0 || reservation_token == UINT64_MAX
		|| sender_connection_generation == 0
		|| sender_connection_generation == UINT32_MAX
		|| owner_procno < 0)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(tag, false, &entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	owner = &entry->resource_x_local_owner;
	if (current_master_node == cluster_node_id) {
		master_state = pcm_resource_x_master_state_for_entry(entry);
		if (master_state != NULL)
			successor = pcm_resource_x_master_head(master_state, NULL);
	}
	if (round->current_master_node != current_master_node
		|| round->resource_formation != resource_formation
		|| round->master_session_incarnation != master_session_incarnation
		|| round->r4_record_generation != r4_record_generation
		|| !cluster_pcm_lock_resource_x_gate_open_exact(resource_formation)
		|| !pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
			entry, round, &round->terminal_ref,
			master_session_incarnation, r4_record_generation,
			cached_ownership_generation))
		result = RESOURCE_X_APPLY_STALE;
	else if (!pcm_resource_x_local_owner_valid_locked(owner))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (current_master_node == cluster_node_id
			 && master_state == NULL)
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (successor != NULL)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (owner->state == RESOURCE_X_LOCAL_OWNER_EMPTY) {
		result = pcm_resource_x_local_owner_claim_locked(
			entry, RESOURCE_X_LOCAL_OWNER_EVICTING,
			&round->terminal_ref, master_session_incarnation,
			r4_record_generation, cached_ownership_generation,
			reservation_token, owner_procno, handle_out);
		if (result == RESOURCE_X_APPLY_APPLIED) {
			pcm_resource_x_target_evict_release_build_locked(
				round, sender_connection_generation, release_out);
			broadcast = true;
		}
	} else if (pcm_resource_x_target_evict_owner_exact_locked(
			owner, round, master_session_incarnation,
			r4_record_generation, cached_ownership_generation,
			reservation_token, owner_procno)) {
		pcm_resource_x_target_evict_release_build_locked(
			round, sender_connection_generation, release_out);
		*handle_out = owner->handle;
		result = RESOURCE_X_APPLY_DUPLICATE;
	} else
		result = RESOURCE_X_APPLY_BAD_STATE;
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_target_evict_abort_exact(
	const ResourceXLocalOwnerHandle *handle)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;

	if (!pcm_resource_x_local_owner_handle_valid(handle))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&handle->ref.assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	if (!pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
			entry, round, &handle->ref, handle->master_session_incarnation,
			handle->r4_record_generation, handle->buffer_ownership_generation)
		|| handle->absolute_deadline_us != round->head_no_progress_deadline_us)
		result = RESOURCE_X_APPLY_STALE;
	else
		result = pcm_resource_x_local_owner_release_locked(
			entry, RESOURCE_X_LOCAL_OWNER_EVICTING, handle);
	if (result == RESOURCE_X_APPLY_APPLIED)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

static bool
pcm_resource_x_target_evict_release_matches_locked(
	struct GrdEntry *entry,
	const ClusterPcmResourceXBootstrapRound *round,
	const ResourceXDecodedFrame *release, int32 current_master_node,
	uint64 r4_record_generation, uint64 cached_ownership_generation,
	const ResourceXLocalOwnerHandle *handle)
{
	const ResourceXDecodedCommon *common;

	Assert(entry != NULL);
	Assert(round != NULL);
	Assert(LWLockHeldByMeInMode(
		&entry->entry_lock.lock, LW_EXCLUSIVE));
	if (release == NULL || release->kind != RESOURCE_X_WIRE_RELEASE_X
		|| release->payload_bytes != RESOURCE_X_CONTROL_V1_BYTES
		|| release->blocked_has_remote_proof
		|| !pcm_resource_x_local_owner_handle_valid(handle)
		|| current_master_node < 0
		|| current_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| r4_record_generation == 0
		|| r4_record_generation == UINT64_MAX
		|| cached_ownership_generation == 0
		|| cached_ownership_generation == UINT64_MAX)
		return false;
	common = &release->common;
	return resource_x_assertion_valid(&common->logical_assertion)
		   && round->current_master_node == current_master_node
		   && round->r4_record_generation == r4_record_generation
		   && resource_x_assertion_equal(&common->logical_assertion, &round->terminal_ref.assertion)
		   && common->base_authority_generation == round->accepted_base
		   && common->authority_generation == round->terminal_authority_generation
		   && common->resource_formation == round->resource_formation
		   && common->master_session_incarnation == round->master_session_incarnation
		   && common->assertion_sequence == round->terminal_ref.acquisition_generation
		   && common->ordered_lane == round->request.ordered_lane
		   && common->action_node == round->terminal_ref.assertion.requester_node
		   && common->observed_mode == (uint8)PCM_STATE_X
		   && common->target_mode == (uint8)PCM_STATE_N && common->source_candidate == 0
		   && common->retain_pi_if_dirty == 0 && common->sender_connection_generation != 0
		   && common->sender_connection_generation != UINT32_MAX
		   && common->outcome == RESOURCE_X_OUTCOME_OK && common->flags == 0
		   && cluster_pcm_lock_resource_x_gate_open_exact(common->resource_formation)
		   && pcm_resource_x_local_owner_valid_locked(&entry->resource_x_local_owner)
		   && entry->resource_x_local_owner.state == RESOURCE_X_LOCAL_OWNER_EVICTING
		   && pcm_resource_x_local_owner_handle_equal(&entry->resource_x_local_owner.handle, handle)
		   && handle->absolute_deadline_us == round->head_no_progress_deadline_us
		   && pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
			   entry, round, &round->terminal_ref, common->master_session_incarnation,
			   r4_record_generation, cached_ownership_generation);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_target_evict_commit_exact(
	const ResourceXDecodedFrame *release, int32 current_master_node,
	uint64 r4_record_generation, uint64 cached_ownership_generation,
	const ResourceXLocalOwnerHandle *handle)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	BufferTag retire_tag;
	uint64 retire_generation;

	if (release == NULL || !pcm_resource_x_local_owner_handle_valid(handle))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(
			&release->common.logical_assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	if (!pcm_resource_x_target_evict_release_matches_locked(
			entry, round, release, current_master_node,
			r4_record_generation, cached_ownership_generation, handle)
		|| !pcm_entry_mark_quiescing_locked(entry)) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_STALE;
	}
	pcm_resource_x_bootstrap_round_clear_binding_locked(round);
	pcm_resource_x_local_owner_clear_locked(entry);
	retire_tag = entry_ref.tag;
	retire_generation = entry_ref.binding_generation;
	ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	(void)pcm_entry_try_retire_exact(&retire_tag, retire_generation,
		PCM_RETIRE_REASON_RESOURCE_X_SETTLED);
	return RESOURCE_X_APPLY_APPLIED;
}

/* A retained terminal cover is joinable only while the exact cached-X
 * BufferDesc generation remains current.  The buffer and resource entry are
 * separate lock domains, so a stable later non-X generation is sufficient
 * evidence that this cover lost its local half.  Clear only the binding;
 * highest_attempt_floor and the retired acquisition floor remain monotone. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_invalidate_ownership_loss_exact(
	const ResourceXAssertion *assertion, int32 current_master_node,
	uint64 resource_formation, uint64 master_session_incarnation,
	uint64 r4_record_generation,
	uint32 requester_sender_connection_generation,
	uint32 master_ingress_connection_generation, uint64 retry_slice_us,
	const ClusterPcmOwnSnapshot *observed)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	bool broadcast = false;

	if (!resource_x_assertion_valid(assertion)
		|| assertion->requester_node != cluster_node_id
		|| current_master_node < 0
		|| current_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| resource_formation == 0 || resource_formation == UINT64_MAX
		|| master_session_incarnation == 0
		|| master_session_incarnation == UINT64_MAX
		|| r4_record_generation == 0
		|| r4_record_generation == UINT64_MAX
		|| requester_sender_connection_generation == 0
		|| requester_sender_connection_generation == UINT32_MAX
		|| master_ingress_connection_generation == 0
		|| master_ingress_connection_generation == UINT32_MAX
		|| retry_slice_us == 0 || retry_slice_us == UINT64_MAX
		|| observed == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED)
		result = RESOURCE_X_APPLY_NOT_FOUND;
	else if (!pcm_resource_x_node_request_key_matches_locked(
				 round, assertion, current_master_node, resource_formation,
				 master_session_incarnation, r4_record_generation,
				 requester_sender_connection_generation, master_ingress_connection_generation)
			 || !pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
				 entry, round, &round->terminal_ref, master_session_incarnation,
				 r4_record_generation, round->cached_ownership_generation)
			 || !BufferTagsEqual(&observed->tag, &assertion->resource)
			 || (observed->pcm_state != (uint8)PCM_STATE_N
				 && observed->pcm_state != (uint8)PCM_STATE_S)
			 || observed->flags != 0 || observed->writer_activation_token != 0
			 || observed->resource_x_activation_generation != 0 || observed->generation == 0
			 || observed->generation == UINT64_MAX
			 || observed->generation <= round->cached_ownership_generation)
		result = RESOURCE_X_APPLY_STALE;
	else if (!pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (entry->resource_x_local_owner.state
			 == RESOURCE_X_LOCAL_OWNER_EVICTING)
		/* The exact plan owns both the later local-N observation and cover
		 * cleanup.  An observer must neither clear nor reinterpret it. */
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (entry->resource_x_local_owner.state
			 != RESOURCE_X_LOCAL_OWNER_EMPTY)
		result = RESOURCE_X_APPLY_STALE;
	else if (observed->pcm_state == (uint8)PCM_STATE_N)
		/* A terminal cached-X round paired with local N but no EVICTING owner
		 * has crossed the irreversible point without its completion evidence. */
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else {
		pcm_resource_x_bootstrap_round_clear_binding_locked(round);
		broadcast = true;
		result = RESOURCE_X_APPLY_APPLIED;
	}
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

/* An optional read-cache X->S downgrade must not physically replace the local
 * X half of an exact TERMINAL_X_CACHED cover.  This read-only preflight runs
 * while bufmgr holds a raw pin but no BufferContent lock, preserving the
 * Resource-X-before-BufferContent lock order.  APPLIED means the exact cover
 * is present and must be preserved (the caller ships one image instead);
 * NOT_FOUND means there is no terminal cover at this observation point.
 *
 * The post-commit note remains mandatory: a cover installed after this
 * observation is a cross-domain drift and continues to fail closed rather
 * than being silently cleared or treated as a successful downgrade. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_x_to_s_preflight_exact(
	const ClusterPcmOwnSnapshot *current)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;

	if (current == NULL
		|| current->pcm_state != (uint8)PCM_STATE_X
		|| current->flags != 0
		|| current->generation == 0
		|| current->generation == UINT64_MAX
		|| current->reservation_token == 0
		|| current->reservation_token == UINT64_MAX)
		return RESOURCE_X_APPLY_INVALID;
	if (current->writer_activation_token != 0
		|| current->resource_x_activation_generation != 0)
		return RESOURCE_X_APPLY_BAD_STATE;
	if (!pcm_entry_ref_acquire(&current->tag, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED)
		result = RESOURCE_X_APPLY_NOT_FOUND;
	else if (!pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (entry->resource_x_local_owner.state
			 != RESOURCE_X_LOCAL_OWNER_EMPTY)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (round->terminal_ref.assertion.requester_node != cluster_node_id
		|| round->cached_ownership_generation != current->generation
		|| !cluster_pcm_lock_resource_x_gate_open_exact(
			round->terminal_ref.formation)
		|| !pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
			entry, round, &round->terminal_ref,
			round->master_session_incarnation,
			round->r4_record_generation, current->generation))
		result = RESOURCE_X_APPLY_STALE;
	else
		result = RESOURCE_X_APPLY_APPLIED;
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

/* An exact X->S commit irreversibly ends a requester-local cached-X cover.
 * The BufferDesc raw pin prevents descriptor reuse while this entry-local
 * check runs, and generation monotonicity means the old X cover cannot become
 * current again even if a later writer promotes the stable S copy.  This
 * consumes no authority: it only clears the retained binding whose local X
 * half was just replaced by the exact S tuple. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_round_note_x_to_s_exact(
	const ClusterPcmOwnSnapshot *revoking,
	const ClusterPcmOwnSnapshot *shared)
{
	ClusterPcmResourceXBootstrapRound *round;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	bool broadcast = false;

	if (revoking == NULL || shared == NULL
		|| revoking->pcm_state != (uint8)PCM_STATE_X
		|| revoking->flags != PCM_OWN_FLAG_REVOKING
		|| revoking->generation == 0
		|| revoking->generation == UINT64_MAX
		|| revoking->reservation_token == 0
		|| revoking->reservation_token == UINT64_MAX
		|| revoking->writer_activation_token != 0
		|| revoking->resource_x_activation_generation != 0
		|| shared->pcm_state != (uint8)PCM_STATE_S
		|| shared->flags != 0
		|| shared->generation == 0
		|| shared->generation == UINT64_MAX
		|| shared->reservation_token == 0
		|| shared->reservation_token == UINT64_MAX
		|| shared->writer_activation_token != 0
		|| shared->resource_x_activation_generation != 0)
		return RESOURCE_X_APPLY_INVALID;
	if (!BufferTagsEqual(&revoking->tag, &shared->tag)
		|| shared->generation != revoking->generation + 1
		|| shared->reservation_token != revoking->reservation_token)
		return RESOURCE_X_APPLY_STALE;
	if (!pcm_entry_ref_acquire(&revoking->tag, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	round = &entry->resource_x_bootstrap_round;
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED)
		result = RESOURCE_X_APPLY_NOT_FOUND;
	else if (!pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner))
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if (entry->resource_x_local_owner.state
			 != RESOURCE_X_LOCAL_OWNER_EMPTY)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else if (round->terminal_ref.assertion.requester_node != cluster_node_id
		|| round->cached_ownership_generation != revoking->generation
		|| !cluster_pcm_lock_resource_x_gate_open_exact(
			round->terminal_ref.formation)
		|| !pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
			entry, round, &round->terminal_ref,
			round->master_session_incarnation,
			round->r4_record_generation, revoking->generation))
		result = RESOURCE_X_APPLY_STALE;
	else {
		pcm_resource_x_bootstrap_round_clear_binding_locked(round);
		broadcast = true;
		result = RESOURCE_X_APPLY_APPLIED;
	}
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

static bool
pcm_resource_x_common_equal(const ResourceXDecodedCommon *left,
							const ResourceXDecodedCommon *right)
{
	return left != NULL && right != NULL
		&& resource_x_assertion_equal(&left->logical_assertion,
			&right->logical_assertion)
		&& left->base_authority_generation
			== right->base_authority_generation
		&& left->resource_formation == right->resource_formation
		&& left->master_session_incarnation
			== right->master_session_incarnation
		&& left->assertion_sequence == right->assertion_sequence
		&& left->ordered_lane == right->ordered_lane
		&& left->action_node == right->action_node
		&& left->observed_mode == right->observed_mode
		&& left->target_mode == right->target_mode
		&& left->source_candidate == right->source_candidate
		&& left->retain_pi_if_dirty == right->retain_pi_if_dirty
		&& left->sender_connection_generation
			== right->sender_connection_generation
		&& left->outcome == right->outcome
		&& left->flags == right->flags
		&& left->authority_generation == right->authority_generation
		&& left->semantic_crc32c == right->semantic_crc32c;
}

static bool
pcm_resource_x_bootstrap_request_valid(
	const ResourceXDecodedFrame *request, int32 authenticated_source_node,
	uint32 authenticated_ingress_connection_generation,
	uint64 r4_record_generation, uint64 current_master_session_incarnation,
	uint32 master_sender_connection_generation)
{
	return request != NULL
		&& request->kind == RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP
		&& request->payload_bytes == RESOURCE_X_CONTROL_V1_BYTES
		&& !request->blocked_has_remote_proof
		&& resource_x_assertion_valid(
			&request->common.logical_assertion)
		&& authenticated_source_node
			== request->common.logical_assertion.requester_node
		&& authenticated_ingress_connection_generation != 0
		&& authenticated_ingress_connection_generation != UINT32_MAX
		&& request->common.sender_connection_generation != 0
		&& request->common.sender_connection_generation != UINT32_MAX
		&& r4_record_generation != 0
		&& r4_record_generation != UINT64_MAX
		&& current_master_session_incarnation != 0
		&& current_master_session_incarnation != UINT64_MAX
		&& current_master_session_incarnation
			== request->common.master_session_incarnation
		&& master_sender_connection_generation != 0
		&& master_sender_connection_generation != UINT32_MAX
		&& request->common.base_authority_generation == 0
		&& request->common.authority_generation == 0
		&& request->common.resource_formation != 0
		&& request->common.resource_formation != UINT64_MAX
		&& request->common.assertion_sequence != 0
		&& request->common.assertion_sequence != UINT64_MAX
		&& request->common.ordered_lane == 0
		&& request->common.action_node == authenticated_source_node
		&& request->common.observed_mode == (uint8)PCM_STATE_N
		&& request->common.target_mode == (uint8)PCM_STATE_X
		&& request->common.source_candidate == 0
		&& request->common.retain_pi_if_dirty == 0
		&& request->common.outcome == RESOURCE_X_OUTCOME_NONE
		&& request->common.flags == 0;
}

static bool
pcm_resource_x_bootstrap_priority_valid(
	const ClusterPcmResourceXBootstrapPriority *priority)
{
	static const ClusterPcmResourceXBootstrapPriority empty_priority;
	static const uint8 empty_reserved[7];
	const ResourceXDecodedCommon *request;

	if (priority == NULL
		|| memcmp(priority->reserved, empty_reserved,
			sizeof(empty_reserved)) != 0)
		return false;
	if (priority->state == RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY)
		return memcmp(priority, &empty_priority, sizeof(*priority)) == 0;
	if (priority->state
			!= RESOURCE_X_BOOTSTRAP_PRIORITY_NEXT_ADMISSION)
		return false;
	request = &priority->request;
	return resource_x_assertion_valid(&request->logical_assertion)
		&& request->base_authority_generation == 0
		&& request->authority_generation == 0
		&& request->resource_formation != 0
		&& request->resource_formation != UINT64_MAX
		&& request->master_session_incarnation != 0
		&& request->master_session_incarnation != UINT64_MAX
		&& request->assertion_sequence != 0
		&& request->assertion_sequence != UINT64_MAX
		&& request->ordered_lane == 0
		&& request->action_node
			== request->logical_assertion.requester_node
		&& request->observed_mode == (uint8)PCM_STATE_N
		&& request->target_mode == (uint8)PCM_STATE_X
		&& request->source_candidate == 0
		&& request->retain_pi_if_dirty == 0
		&& request->sender_connection_generation != 0
		&& request->sender_connection_generation != UINT32_MAX
		&& request->outcome == RESOURCE_X_OUTCOME_NONE
		&& request->flags == 0
		&& priority->r4_record_generation != 0
		&& priority->r4_record_generation != UINT64_MAX
		&& priority->authenticated_ingress_connection_generation != 0
		&& priority->authenticated_ingress_connection_generation
			!= UINT32_MAX
		&& priority->master_sender_connection_generation != 0
		&& priority->master_sender_connection_generation != UINT32_MAX;
}

static void
pcm_resource_x_bootstrap_priority_clear(
	ClusterPcmResourceXBootstrapPriority *priority)
{
	Assert(priority != NULL);
	memset(priority, 0, sizeof(*priority));
}

static void
pcm_resource_x_bootstrap_receipt_invalidate(
	ClusterPcmResourceXBootstrapReceipt *receipt)
{
	uint64 highest_attempt_floor;

	Assert(receipt != NULL);
	highest_attempt_floor = receipt->highest_attempt_floor;
	memset(receipt, 0, sizeof(*receipt));
	receipt->highest_attempt_floor = highest_attempt_floor;
}

static bool
pcm_resource_x_bootstrap_receipt_valid(
	const ClusterPcmResourceXBootstrapReceipt *receipt)
{
	static const ResourceXDecodedCommon empty_common;
	static const uint8 empty_reserved[3];
	const ResourceXDecodedCommon *request;
	const ResourceXDecodedCommon *ack;

	if (receipt == NULL
		|| memcmp(receipt->reserved, empty_reserved,
			sizeof(empty_reserved)) != 0)
		return false;
	if (receipt->state == RESOURCE_X_BOOTSTRAP_RECEIPT_EMPTY)
		return memcmp(&receipt->request, &empty_common,
				sizeof(empty_common)) == 0
			&& memcmp(&receipt->ack, &empty_common,
				sizeof(empty_common)) == 0
			&& receipt->sampled_base == 0
			&& receipt->r4_record_generation == 0
			&& receipt->authenticated_ingress_connection_generation == 0
			&& receipt->highest_attempt_floor != UINT64_MAX;
	if (receipt->state != RESOURCE_X_BOOTSTRAP_RECEIPT_RECEIVED
		&& receipt->state
			!= RESOURCE_X_BOOTSTRAP_RECEIPT_CONSUMED_BY_ASSERT)
		return false;
	request = &receipt->request;
	ack = &receipt->ack;
	return resource_x_assertion_valid(&request->logical_assertion)
		&& request->base_authority_generation == 0
		&& request->authority_generation == 0
		&& request->resource_formation != 0
		&& request->resource_formation != UINT64_MAX
		&& request->master_session_incarnation != 0
		&& request->master_session_incarnation != UINT64_MAX
		&& request->assertion_sequence != 0
		&& request->assertion_sequence != UINT64_MAX
		&& request->ordered_lane == 0
		&& request->action_node
			== request->logical_assertion.requester_node
		&& request->observed_mode == (uint8)PCM_STATE_N
		&& request->target_mode == (uint8)PCM_STATE_X
		&& request->source_candidate == 0
		&& request->retain_pi_if_dirty == 0
		&& request->sender_connection_generation != 0
		&& request->sender_connection_generation != UINT32_MAX
		&& request->outcome == RESOURCE_X_OUTCOME_NONE
		&& request->flags == 0
		&& resource_x_assertion_equal(&ack->logical_assertion,
			&request->logical_assertion)
		&& ack->base_authority_generation == receipt->sampled_base
		&& ack->base_authority_generation != 0
		&& ack->base_authority_generation != UINT64_MAX
		&& ack->resource_formation == request->resource_formation
		&& ack->master_session_incarnation
			== request->master_session_incarnation
		&& ack->assertion_sequence == request->assertion_sequence
		&& ack->ordered_lane == 0
		&& ack->action_node == request->logical_assertion.requester_node
		&& ack->observed_mode == (uint8)PCM_STATE_N
		&& ack->target_mode == (uint8)PCM_STATE_X
		&& ack->source_candidate == 0
		&& ack->retain_pi_if_dirty == 0
		&& ack->sender_connection_generation != 0
		&& ack->sender_connection_generation != UINT32_MAX
		&& ack->outcome == RESOURCE_X_OUTCOME_OK
		&& ack->flags == 0
		&& ack->authority_generation == 0
		&& receipt->highest_attempt_floor >= request->assertion_sequence
		&& receipt->highest_attempt_floor != UINT64_MAX
		&& receipt->r4_record_generation != 0
		&& receipt->r4_record_generation != UINT64_MAX
		&& receipt->authenticated_ingress_connection_generation != 0
		&& receipt->authenticated_ingress_connection_generation
			!= UINT32_MAX;
}

static void
pcm_resource_x_bootstrap_ack_snapshot(
	const ClusterPcmResourceXBootstrapReceipt *receipt,
	ResourceXDecodedFrame *ack_out)
{
	Assert(receipt != NULL);
	Assert(ack_out != NULL);
	memset(ack_out, 0, sizeof(*ack_out));
	ack_out->kind = RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP;
	ack_out->payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	pcm_resource_x_common_copy(&ack_out->common, &receipt->ack);
}

typedef enum PcmResourceXBootstrapDispatchState {
	PCM_RESOURCE_X_BOOTSTRAP_DISPATCH_CORRUPT = 0,
	PCM_RESOURCE_X_BOOTSTRAP_DISPATCH_SOURCE_INELIGIBLE,
	PCM_RESOURCE_X_BOOTSTRAP_DISPATCH_OCCUPIED,
	PCM_RESOURCE_X_BOOTSTRAP_DISPATCH_AVAILABLE
} PcmResourceXBootstrapDispatchState;

#define PCM_RESOURCE_X_DISPATCH_DIAGNOSTIC_INTERVAL_US UINT64_C(250000)

/* The ordinary occupied state is retry guidance, not authority.  It is
 * common while a same-resource predecessor settles, so recording it for
 * every candidate scan can starve the master worker.  Corrupt receipts are
 * still logged unconditionally below. */
static uint64 pcm_resource_x_dispatch_diagnostic_next_log_at = 0;

static bool
pcm_resource_x_dispatch_diagnostic_should_log(void)
{
	uint64 now_us = pcm_resource_x_monotonic_us();

	if (now_us < pcm_resource_x_dispatch_diagnostic_next_log_at)
		return false;
	pcm_resource_x_dispatch_diagnostic_next_log_at
		= now_us > UINT64_MAX - PCM_RESOURCE_X_DISPATCH_DIAGNOSTIC_INTERVAL_US
		? UINT64_MAX
		: now_us + PCM_RESOURCE_X_DISPATCH_DIAGNOSTIC_INTERVAL_US;
	return true;
}

/* A bootstrap ACK freezes the exact current authority base before ASSERT.
 * Until that receipt is either replaced by the same requester or consumed
 * and retired with its canonical request, issuing another requester an ACK
 * for the same base would create two independently valid ASSERT candidates.
 * Serialize that non-authority dispatch using the fixed receipt/request
 * state; the separate single NEXT_ADMISSION identity provides bounded
 * priority without retaining a base or ACK. */
static PcmResourceXBootstrapDispatchState
pcm_resource_x_bootstrap_dispatch_state_locked(
	struct GrdEntry *entry, const ClusterPcmResourceXMasterState *state,
	int32 requester_node)
{
	static const ClusterPcmResourceXMasterRequest empty_request;
	ResourceXDecodedFrame source_settlement;
	bool occupied = false;
	uint32 requester_bit;
	int32 candidate_node;

	Assert(entry != NULL);
	Assert(state != NULL);
	Assert(requester_node >= 0
		&& requester_node < RESOURCE_X_PROTOCOL_NODE_LIMIT);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	requester_bit = UINT32_C(1) << (uint32)requester_node;
	if (!pcm_resource_x_source_settlement_debt_decode_locked(
			entry, state, &source_settlement))
		return PCM_RESOURCE_X_BOOTSTRAP_DISPATCH_CORRUPT;
	/* INSTALL_SETTLEMENT is transport-reliable but has no semantic ACK.  Do
	 * not admit another conversion while the previous exact former-source
	 * release is still PENDING: a second REMOTE_CARRIER install could not arm
	 * its sole debt and would strand the canonical request in GRANT_COMMITTED.
	 * The bounded successor priority remains non-authoritative and retries the
	 * same request after the exact source ACK changes PENDING to ACKED. */
	if (state->source_settlement.state
			== RESOURCE_X_SOURCE_SETTLEMENT_PENDING) {
		/* The selected former source still owns the preceding physical carrier
		 * fence.  SourceSettlementV2 makes its local reacquire request
		 * ineligible until that exact debt settles, so a rejected kind-9 frame
		 * must not reserve NEXT_ADMISSION.  No base, receipt, or authority is
		 * sampled here. */
		if (source_settlement.common.action_node == requester_node)
			return PCM_RESOURCE_X_BOOTSTRAP_DISPATCH_SOURCE_INELIGIBLE;
		occupied = true;
	}

	for (candidate_node = 0;
		 candidate_node < RESOURCE_X_PROTOCOL_NODE_LIMIT;
		 candidate_node++) {
		const ClusterPcmResourceXBootstrapReceipt *receipt
			= &state->bootstrap_receipts[candidate_node];
		const ClusterPcmResourceXMasterRequest *request
			= &state->requests[candidate_node];

		if (!pcm_resource_x_bootstrap_receipt_valid(receipt)) {
			ereport(LOG,
					(errmsg_internal("Resource-X kind-9 dispatch blocker diagnostic"),
					 errdetail("requester=%d candidate=%d invalid_receipt=true "
							   "receipt_state=%u request_phase=%u",
							   requester_node, candidate_node,
							   (unsigned)receipt->state,
							   (unsigned)request->phase)));
			return PCM_RESOURCE_X_BOOTSTRAP_DISPATCH_CORRUPT;
		}
		/* An unresolved blocker of the active predecessor is about to lose its
		 * local carrier through the predecessor's exact type-17 transition.
		 * Its current pre-ASSERT round can therefore be canceled before terminal
		 * settlement and is not "otherwise admissible" for NEXT_ADMISSION.  Do
		 * not retain an identity that would orphan priority when that requester
		 * legitimately allocates the next monotone attempt after physical
		 * release.  Once its blocker bit is acknowledged, the local mutation is
		 * already complete and an independently current attempt may use the
		 * ordinary occupied-dispatch rule. */
		if (request->phase == RESOURCE_X_MASTER_WAIT_BLOCKERS
			&& (request->incompatible_holders_bitmap & requester_bit) != 0
			&& (request->blocked_holders_bitmap & requester_bit) == 0)
			return PCM_RESOURCE_X_BOOTSTRAP_DISPATCH_SOURCE_INELIGIBLE;
		if (receipt->state != RESOURCE_X_BOOTSTRAP_RECEIPT_EMPTY
			&& (candidate_node != requester_node
				|| receipt->state
					!= RESOURCE_X_BOOTSTRAP_RECEIPT_RECEIVED)) {
			if (pcm_resource_x_dispatch_diagnostic_should_log())
				ereport(LOG,
					(errmsg_internal("Resource-X kind-9 dispatch blocker diagnostic"),
					 errdetail("requester=%d candidate=%d invalid_receipt=false "
							   "receipt_state=%u request_phase=%u",
							   requester_node, candidate_node,
							   (unsigned)receipt->state,
							   (unsigned)request->phase)));
			occupied = true;
		}
		if (request->phase != RESOURCE_X_MASTER_NONE
			|| memcmp(request, &empty_request, sizeof(*request)) != 0) {
			if (pcm_resource_x_dispatch_diagnostic_should_log())
				ereport(LOG,
					(errmsg_internal("Resource-X kind-9 dispatch blocker diagnostic"),
					 errdetail("requester=%d candidate=%d invalid_receipt=false "
							   "receipt_state=%u request_phase=%u",
							   requester_node, candidate_node,
							   (unsigned)receipt->state,
							   (unsigned)request->phase)));
			occupied = true;
		}
	}
	return occupied ? PCM_RESOURCE_X_BOOTSTRAP_DISPATCH_OCCUPIED
		: PCM_RESOURCE_X_BOOTSTRAP_DISPATCH_AVAILABLE;
}

static bool
pcm_resource_x_bootstrapped_assert_matches_receipt(
	const ResourceXDecodedFrame *assertion,
	const ClusterPcmResourceXBootstrapReceipt *receipt,
	uint32 authenticated_ingress_connection_generation,
	uint64 r4_record_generation, uint64 current_master_session_incarnation,
	uint32 current_master_sender_connection_generation)
{
	return assertion != NULL && receipt != NULL
		&& receipt->state == RESOURCE_X_BOOTSTRAP_RECEIPT_RECEIVED
		&& assertion->payload_bytes == RESOURCE_X_CONTROL_V1_BYTES
		&& !assertion->blocked_has_remote_proof
		&& resource_x_assertion_equal(
			&assertion->common.logical_assertion,
			&receipt->request.logical_assertion)
		&& assertion->common.base_authority_generation
			== receipt->sampled_base
		&& assertion->common.authority_generation
			== receipt->sampled_base
		&& assertion->common.resource_formation
			== receipt->request.resource_formation
		&& assertion->common.master_session_incarnation
			== receipt->request.master_session_incarnation
		&& assertion->common.assertion_sequence
			== receipt->request.assertion_sequence
		&& assertion->common.ordered_lane == 0
		&& assertion->common.action_node
			== receipt->request.logical_assertion.requester_node
		&& assertion->common.observed_mode == (uint8)PCM_STATE_N
		&& assertion->common.target_mode == (uint8)PCM_STATE_X
		&& assertion->common.source_candidate == 0
		&& assertion->common.retain_pi_if_dirty == 0
		&& assertion->common.sender_connection_generation
			== receipt->request.sender_connection_generation
		&& authenticated_ingress_connection_generation
			== receipt->authenticated_ingress_connection_generation
		&& assertion->common.outcome == RESOURCE_X_OUTCOME_NONE
		&& assertion->common.flags == 0
		&& receipt->sampled_base != 0
		&& receipt->sampled_base != UINT64_MAX
		&& r4_record_generation == receipt->r4_record_generation
		&& current_master_session_incarnation
			== receipt->request.master_session_incarnation
		&& current_master_sender_connection_generation
			== receipt->ack.sender_connection_generation;
}

/* The initial module values are not an authority witness once legacy PCM has
 * already moved this resource.  The temporary production adapter may adopt
 * the exact selected-ticket base only while every Resource-X semantic owner
 * is still pristine. */
static bool
pcm_resource_x_master_state_pristine(
	const ClusterPcmResourceXMasterState *state)
{
	static const ClusterPcmResourceXMasterState pristine;
	const char *semantic_tail;
	Size semantic_tail_bytes;

	if (state == NULL || state->binding_generation == 0
		|| state->binding_generation == UINT64_MAX
		|| state->authority_generation != 1
		|| state->next_enqueue_order != 1)
		return false;
	semantic_tail = (const char *)&state->bootstrap_priority;
	semantic_tail_bytes = sizeof(*state)
		- offsetof(ClusterPcmResourceXMasterState, bootstrap_priority);
	return memcmp(semantic_tail, &pristine.bootstrap_priority,
		semantic_tail_bytes) == 0;
}

/* The promoted-successor adapter may advance an enqueue-time base only before
 * Resource-X has observed this exact assertion sequence.  Older SETTLED or
 * RELEASED requests are retained terminal history, not a live assertion; any
 * nonterminal request or an exact-sequence terminal replay closes the window. */
static bool
pcm_resource_x_master_state_preassert_rebindable(
	const ClusterPcmResourceXMasterState *state,
	const ResourceXAssertion *assertion, uint64 assertion_sequence)
{
	static const ClusterPcmResourceXMasterRequest empty_request;
	int32 requester_node;

	if (state == NULL || state->authority_generation == 0
		|| state->authority_generation == UINT64_MAX
		|| state->next_enqueue_order == 0
		|| state->next_enqueue_order == UINT64_MAX
		|| !resource_x_assertion_valid(assertion)
		|| assertion_sequence == 0 || assertion_sequence == UINT64_MAX)
		return false;
	for (requester_node = 0;
		 requester_node < RESOURCE_X_PROTOCOL_NODE_LIMIT;
		 requester_node++) {
		const ClusterPcmResourceXMasterRequest *request
			= &state->requests[requester_node];

		if (request->phase == RESOURCE_X_MASTER_NONE) {
			if (memcmp(request, &empty_request, sizeof(*request)) != 0)
				return false;
			continue;
		}
		if (request->phase != RESOURCE_X_MASTER_SETTLED
			&& request->phase != RESOURCE_X_MASTER_RELEASED)
			return false;
		if (request->base_authority_generation == 0
			|| request->base_authority_generation == UINT64_MAX
			|| request->resource_formation == 0
			|| request->resource_formation == UINT64_MAX
			|| request->master_session_incarnation == 0
			|| request->assertion_sequence == 0
			|| request->assertion_sequence == UINT64_MAX
			|| request->enqueue_order == 0
			|| request->final_authority_generation == 0
			|| request->final_authority_generation == UINT64_MAX
			|| request->assertion_sequence == assertion_sequence)
			return false;
	}
	return true;
}

static bool
pcm_resource_x_legacy_authority_shape_valid(
	const PcmAuthoritySnapshot *authority)
{
	if (authority == NULL || authority->transition_count == 0
		|| authority->transition_count == UINT64_MAX)
		return false;
	switch (authority->state) {
		case PCM_STATE_N:
			return authority->x_holder_node == -1
				&& authority->s_holders_bitmap == 0
				&& authority->master_holder.node_id
					== INVALID_PCM_MASTER_HOLDER_NODE;
		case PCM_STATE_S:
			return authority->x_holder_node == -1
				&& authority->s_holders_bitmap != 0
				&& authority->master_holder.node_id
					< RESOURCE_X_PROTOCOL_NODE_LIMIT
				&& (authority->s_holders_bitmap
					& (UINT32_C(1) << authority->master_holder.node_id)) != 0;
		case PCM_STATE_X:
			return authority->x_holder_node >= 0
				&& authority->x_holder_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
				&& authority->s_holders_bitmap == 0
				&& authority->master_holder.node_id
					== (uint32)authority->x_holder_node;
		default:
			return false;
	}
}

static bool
pcm_resource_x_legacy_authority_valid(
	const PcmAuthoritySnapshot *authority,
	uint64 base_authority_generation)
{
	return base_authority_generation != 0
		&& base_authority_generation != UINT64_MAX
		&& authority != NULL
		&& authority->transition_count == base_authority_generation
		&& authority->pending_x_requester_node == -1
		&& authority->pending_x_since_lsn == 0
		&& pcm_resource_x_legacy_authority_shape_valid(authority);
}

/* A promoted head may already own the process-local pending-X barrier when
 * its enqueue-time base is refreshed.  That barrier is admissible only when
 * the cookie names the same requester and immutable PCM-X ticket. */
static bool
pcm_resource_x_head_rebind_authority_valid(
	const ResourceXAssertion *assertion, uint64 ticket_id,
	const PcmAuthoritySnapshot *authority,
	uint64 base_authority_generation)
{
	uint64 expected_pending_x;

	if (assertion == NULL || authority == NULL
		|| base_authority_generation == 0
		|| base_authority_generation == UINT64_MAX
		|| authority->transition_count != base_authority_generation
		|| !pcm_resource_x_legacy_authority_shape_valid(authority)
		|| !PcmPendingXQueueValue(ticket_id, &expected_pending_x))
		return false;
	if (authority->pending_x_requester_node == -1)
		return authority->pending_x_since_lsn == 0;
	return authority->pending_x_requester_node == assertion->requester_node
		&& authority->pending_x_since_lsn == expected_pending_x;
}

static ResourceXApplyResult
pcm_resource_x_adapter_base_bind_exact(
	const ResourceXAssertion *assertion, uint64 formation,
	uint64 base_authority_generation,
	const PcmAuthoritySnapshot *legacy_authority,
	bool allow_preassert_rebind, uint64 assertion_sequence,
	uint64 ticket_id)
{
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	PcmAuthoritySnapshot current;
	struct GrdEntry *entry;
	ResourceXApplyResult result;

	if (!resource_x_assertion_valid(assertion) || formation == 0
		|| formation == UINT64_MAX
		|| !(allow_preassert_rebind
			? pcm_resource_x_head_rebind_authority_valid(
				assertion, ticket_id, legacy_authority,
				base_authority_generation)
			: pcm_resource_x_legacy_authority_valid(
				legacy_authority, base_authority_generation))
		|| ClusterPcm == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_OPEN
		|| pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation)
			!= formation)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	pcm_entry_lock_exclusive(entry);
	pcm_authority_snapshot_locked(entry, &current);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL || !pcm_authority_snapshot_equal(
			&current, legacy_authority))
		result = RESOURCE_X_APPLY_STALE;
	else if (allow_preassert_rebind
			 && !pcm_resource_x_master_state_preassert_rebindable(
				 state, assertion, assertion_sequence))
		result = RESOURCE_X_APPLY_STALE;
	else if (state->authority_generation == base_authority_generation)
		result = RESOURCE_X_APPLY_DUPLICATE;
	else if (!pcm_resource_x_active_empty_locked(entry)
			 || (!allow_preassert_rebind
				 && !pcm_resource_x_master_state_pristine(state)))
		result = RESOURCE_X_APPLY_STALE;
	else {
		state->authority_generation = base_authority_generation;
		result = RESOURCE_X_APPLY_APPLIED;
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_adapter_base_bind_exact(
	const ResourceXAssertion *assertion, uint64 formation,
	uint64 base_authority_generation,
	const PcmAuthoritySnapshot *legacy_authority)
{
	return pcm_resource_x_adapter_base_bind_exact(
		assertion, formation, base_authority_generation, legacy_authority,
		false, 0, 0);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_adapter_head_rebind_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	uint64 ticket_id, uint64 formation,
	uint64 base_authority_generation,
	const PcmAuthoritySnapshot *legacy_authority)
{
	return pcm_resource_x_adapter_base_bind_exact(
		assertion, formation, base_authority_generation, legacy_authority,
		true, assertion_sequence, ticket_id);
}

/* A non-head PCM-X ticket must receive its first admission ACK so its
 * ENQUEUE retry episode can close, but it cannot assert until ordinary FIFO
 * promotion.  If a predecessor has already made the Resource-X state
 * non-pristine, sample that state's canonical generation without mutating it.
 * The complete legacy snapshot remains an optimistic token; an exact queue
 * pending-X cookie is accepted only when it names the live Resource-X head. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_adapter_successor_base_exact(
	const ResourceXAssertion *assertion, uint64 formation,
	const PcmAuthoritySnapshot *legacy_authority,
	uint64 *base_authority_generation_out)
{
	ClusterPcmResourceXMasterRequest *head;
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	PcmAuthoritySnapshot current;
	struct GrdEntry *entry;
	int32 head_node = -1;
	ResourceXApplyResult result;

	if (base_authority_generation_out != NULL)
		*base_authority_generation_out = 0;
	if (!resource_x_assertion_valid(assertion) || formation == 0
		|| formation == UINT64_MAX || legacy_authority == NULL
		|| base_authority_generation_out == NULL
		|| !pcm_resource_x_legacy_authority_shape_valid(legacy_authority)
		|| (legacy_authority->pending_x_requester_node == -1
			&& legacy_authority->pending_x_since_lsn != 0)
		|| (legacy_authority->pending_x_requester_node != -1
			&& (legacy_authority->pending_x_requester_node < 0
				|| legacy_authority->pending_x_requester_node
					>= RESOURCE_X_PROTOCOL_NODE_LIMIT
				|| (legacy_authority->pending_x_since_lsn
					& PCM_PENDING_X_QUEUE_KIND) == 0))
		|| ClusterPcm == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_OPEN
		|| pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation)
			!= formation)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	pcm_authority_snapshot_locked(entry, &current);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (!pcm_authority_snapshot_equal(&current, legacy_authority))
		result = RESOURCE_X_APPLY_STALE;
	else if (state == NULL || state->authority_generation == 0
			 || state->authority_generation == UINT64_MAX)
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else {
		head = pcm_resource_x_master_head(state, &head_node);
		if ((current.pending_x_requester_node != -1
				&& (head == NULL
					|| current.pending_x_requester_node != head_node
					|| head_node == assertion->requester_node))
			|| (head != NULL && head_node == assertion->requester_node)
			|| (state->requests[assertion->requester_node].phase
					!= RESOURCE_X_MASTER_NONE
				&& state->requests[assertion->requester_node].phase
					!= RESOURCE_X_MASTER_SETTLED
				&& state->requests[assertion->requester_node].phase
					!= RESOURCE_X_MASTER_RELEASED))
			result = RESOURCE_X_APPLY_STALE;
		else {
			*base_authority_generation_out
				= state->authority_generation;
			result = RESOURCE_X_APPLY_APPLIED;
		}
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_bootstrap_request_exact(
	const ResourceXDecodedFrame *request, int32 authenticated_source_node,
	uint32 authenticated_ingress_connection_generation,
	uint64 r4_record_generation, uint64 current_master_session_incarnation,
	uint32 master_sender_connection_generation,
	ResourceXDecodedFrame *ack_out)
{
	ClusterPcmResourceXBootstrapPriority *priority;
	ClusterPcmResourceXBootstrapReceipt *receipt;
	ClusterPcmResourceXMasterRequest *master_request;
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	PcmResourceXBootstrapDispatchState dispatch_state;
	struct GrdEntry *entry;
	uint32 gate_phase;
	uint64 gate_formation;
	uint64 attempt_floor;
	int32 requester_node;
	bool priority_exact_wire;
	bool priority_same_attempt;
	ResourceXApplyResult result;

	if (ack_out != NULL)
		memset(ack_out, 0, sizeof(*ack_out));
	if (ack_out == NULL
		|| !pcm_resource_x_bootstrap_request_valid(
			request, authenticated_source_node,
			authenticated_ingress_connection_generation,
			r4_record_generation, current_master_session_incarnation,
			master_sender_connection_generation)
		|| ClusterPcm == NULL)
		return RESOURCE_X_APPLY_INVALID;
	gate_phase = pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase);
	gate_formation = pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (gate_phase != RESOURCE_X_GATE_OPEN
		|| gate_formation != request->common.resource_formation)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;

	if (!pcm_entry_ref_acquire(
			&request->common.logical_assertion.resource, true,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	requester_node = request->common.logical_assertion.requester_node;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL || state->authority_generation == 0
		|| state->authority_generation == UINT64_MAX
		|| state->next_enqueue_order == 0) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	priority = &state->bootstrap_priority;
	if (!pcm_resource_x_bootstrap_priority_valid(priority)
		|| (priority->state
				== RESOURCE_X_BOOTSTRAP_PRIORITY_NEXT_ADMISSION
			&& !BufferTagsEqual(
				&priority->request.logical_assertion.resource,
				&entry->tag))) {
		pcm_resource_x_bootstrap_priority_clear(priority);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	if (priority->state == RESOURCE_X_BOOTSTRAP_PRIORITY_NEXT_ADMISSION
		&& (priority->request.resource_formation != gate_formation
			|| priority->request.master_session_incarnation
				!= current_master_session_incarnation
			|| priority->r4_record_generation != r4_record_generation)) {
		pcm_resource_x_bootstrap_priority_clear(priority);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_STALE;
	}
	priority_same_attempt
		= priority->state == RESOURCE_X_BOOTSTRAP_PRIORITY_NEXT_ADMISSION
		&& resource_x_assertion_equal(
			&priority->request.logical_assertion,
			&request->common.logical_assertion)
		&& priority->request.resource_formation
			== request->common.resource_formation
		&& priority->request.master_session_incarnation
			== request->common.master_session_incarnation
		&& priority->request.assertion_sequence
			== request->common.assertion_sequence;
	priority_exact_wire = priority_same_attempt
		&& pcm_resource_x_common_equal(
			&priority->request, &request->common);
	if (priority_same_attempt
		&& (!priority_exact_wire
			|| priority->authenticated_ingress_connection_generation
				!= authenticated_ingress_connection_generation
			|| priority->master_sender_connection_generation
				!= master_sender_connection_generation)) {
		pcm_resource_x_bootstrap_priority_clear(priority);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_STALE;
	}
	receipt = &state->bootstrap_receipts[requester_node];
	master_request = &state->requests[requester_node];
	if (!pcm_resource_x_bootstrap_receipt_valid(receipt)) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	if (receipt->state == RESOURCE_X_BOOTSTRAP_RECEIPT_RECEIVED
		&& (receipt->sampled_base != state->authority_generation
			|| receipt->r4_record_generation != r4_record_generation
			|| receipt->request.resource_formation != gate_formation
			|| receipt->request.master_session_incarnation
				!= current_master_session_incarnation
			|| receipt->request.sender_connection_generation
				!= request->common.sender_connection_generation
			|| receipt->authenticated_ingress_connection_generation
				!= authenticated_ingress_connection_generation
			|| receipt->ack.sender_connection_generation
				!= master_sender_connection_generation))
		pcm_resource_x_bootstrap_receipt_invalidate(receipt);

	attempt_floor = receipt->highest_attempt_floor;
	if (pcm_resource_x_terminal_tombstone_valid(
			&state->terminal_tombstone)
		&& state->terminal_tombstone.requester_node == requester_node
		&& state->terminal_tombstone.request.assertion_sequence
			> attempt_floor)
		attempt_floor
			= state->terminal_tombstone.request.assertion_sequence;
	if (master_request->phase != RESOURCE_X_MASTER_NONE
		&& master_request->assertion_sequence > attempt_floor)
		attempt_floor = master_request->assertion_sequence;
	if (attempt_floor > receipt->highest_attempt_floor)
		receipt->highest_attempt_floor = attempt_floor;

	if (receipt->state == RESOURCE_X_BOOTSTRAP_RECEIPT_RECEIVED
		&& pcm_resource_x_common_equal(
			&request->common, &receipt->request)) {
		pcm_resource_x_bootstrap_ack_snapshot(receipt, ack_out);
		result = RESOURCE_X_APPLY_DUPLICATE;
	}
	else if (request->common.assertion_sequence <= attempt_floor
			 || receipt->state
				== RESOURCE_X_BOOTSTRAP_RECEIPT_CONSUMED_BY_ASSERT
			 || (master_request->phase != RESOURCE_X_MASTER_NONE
				 && master_request->phase != RESOURCE_X_MASTER_RELEASED)) {
		if (priority_exact_wire)
			pcm_resource_x_bootstrap_priority_clear(priority);
		result = RESOURCE_X_APPLY_STALE;
	}
	else {
		dispatch_state = pcm_resource_x_bootstrap_dispatch_state_locked(
			entry, state, requester_node);
		if (dispatch_state
				== PCM_RESOURCE_X_BOOTSTRAP_DISPATCH_CORRUPT) {
			pcm_resource_x_bootstrap_priority_clear(priority);
			result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
		else if (dispatch_state
				== PCM_RESOURCE_X_BOOTSTRAP_DISPATCH_SOURCE_INELIGIBLE)
			result = RESOURCE_X_APPLY_BAD_STATE;
		else if (dispatch_state
				== PCM_RESOURCE_X_BOOTSTRAP_DISPATCH_OCCUPIED) {
			if (priority->state == RESOURCE_X_BOOTSTRAP_PRIORITY_EMPTY) {
				pcm_resource_x_common_copy(
					&priority->request, &request->common);
				priority->r4_record_generation = r4_record_generation;
				priority->authenticated_ingress_connection_generation
					= authenticated_ingress_connection_generation;
				priority->master_sender_connection_generation
					= master_sender_connection_generation;
				priority->state
					= RESOURCE_X_BOOTSTRAP_PRIORITY_NEXT_ADMISSION;
			}
			result = RESOURCE_X_APPLY_BAD_STATE;
		}
		else if (priority->state
				== RESOURCE_X_BOOTSTRAP_PRIORITY_NEXT_ADMISSION
			 && !priority_exact_wire)
			result = RESOURCE_X_APPLY_BAD_STATE;
		else {
			pcm_resource_x_bootstrap_receipt_invalidate(receipt);
			pcm_resource_x_common_copy(
				&receipt->request, &request->common);
			pcm_resource_x_common_copy(&receipt->ack, &request->common);
			receipt->ack.base_authority_generation
				= state->authority_generation;
			receipt->ack.sender_connection_generation
				= master_sender_connection_generation;
			receipt->ack.outcome = RESOURCE_X_OUTCOME_OK;
			receipt->ack.semantic_crc32c = 0;
			receipt->highest_attempt_floor
				= request->common.assertion_sequence;
			receipt->sampled_base = state->authority_generation;
			receipt->r4_record_generation = r4_record_generation;
			receipt->authenticated_ingress_connection_generation
				= authenticated_ingress_connection_generation;
			receipt->state = RESOURCE_X_BOOTSTRAP_RECEIPT_RECEIVED;
			if (priority_exact_wire)
				pcm_resource_x_bootstrap_priority_clear(priority);
			pcm_resource_x_bootstrap_ack_snapshot(receipt, ack_out);
			result = RESOURCE_X_APPLY_APPLIED;
		}
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

bool
cluster_pcm_lock_resource_x_s_barrier_active(const BufferTag *tag)
{
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	int32 requester_node;
	bool barrier_active = false;

	if (tag == NULL)
		return true;
	if (!pcm_entry_ref_acquire(
			tag, false, &entry_ref, &acquire_result))
		return false;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL || state->authority_generation == 0
		|| state->authority_generation == UINT64_MAX
		|| state->next_enqueue_order == 0
		|| state->next_enqueue_order == UINT64_MAX) {
		barrier_active = true;
		goto out;
	}
	for (requester_node = 0;
		 requester_node < RESOURCE_X_PROTOCOL_NODE_LIMIT;
		 requester_node++) {
		const ClusterPcmResourceXBootstrapReceipt *receipt
			= &state->bootstrap_receipts[requester_node];
		const ClusterPcmResourceXMasterRequest *request
			= &state->requests[requester_node];

		if (!pcm_resource_x_bootstrap_receipt_valid(receipt)
			|| receipt->state != RESOURCE_X_BOOTSTRAP_RECEIPT_EMPTY
			|| (request->phase != RESOURCE_X_MASTER_NONE
				&& request->phase != RESOURCE_X_MASTER_SETTLED
				&& request->phase != RESOURCE_X_MASTER_RELEASED)) {
			barrier_active = true;
			break;
		}
	}
out:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return barrier_active;
}

static ResourceXApplyResult
pcm_resource_x_assert_exact_internal(
	const ResourceXDecodedFrame *assertion, int32 authenticated_source_node,
	bool require_bootstrap,
	uint32 authenticated_ingress_connection_generation,
	uint64 r4_record_generation, uint64 current_master_session_incarnation,
	uint32 current_master_sender_connection_generation,
	ResourceXMasterSnapshot *out)
{
	ClusterPcmResourceXBootstrapReceipt *receipt = NULL;
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXMasterRequest *request;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	uint32 gate_phase;
	uint64 gate_formation;
	uint32 incompatible = 0;
	PcmState pcm_state;
	int32 requester_node;

	if (out != NULL) {
		memset(out, 0, sizeof(*out));
		out->source_node = -1;
	}
	if (out == NULL || !pcm_resource_x_assert_frame_valid(assertion,
												 authenticated_source_node)
		|| (require_bootstrap
			&& (assertion->payload_bytes
					!= RESOURCE_X_CONTROL_V1_BYTES
				|| assertion->blocked_has_remote_proof
				|| assertion->common.base_authority_generation
					== UINT64_MAX
				|| assertion->common.resource_formation == UINT64_MAX
				|| assertion->common.assertion_sequence == UINT64_MAX
				|| assertion->common.ordered_lane != 0
				|| assertion->common.observed_mode != (uint8)PCM_STATE_N
				|| assertion->common.source_candidate != 0
				|| assertion->common.retain_pi_if_dirty != 0
				|| assertion->common.flags != 0
				|| authenticated_ingress_connection_generation == 0
				|| authenticated_ingress_connection_generation == UINT32_MAX
				|| assertion->common.sender_connection_generation
					== UINT32_MAX
				|| r4_record_generation == 0
				|| r4_record_generation == UINT64_MAX
				|| current_master_session_incarnation == 0
				|| current_master_session_incarnation == UINT64_MAX
				|| assertion->common.master_session_incarnation
					!= current_master_session_incarnation
				|| current_master_sender_connection_generation == 0
				|| current_master_sender_connection_generation
					== UINT32_MAX))
		|| ClusterPcm == NULL)
		return RESOURCE_X_APPLY_INVALID;
	gate_phase = pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase);
	gate_formation = pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (gate_phase != RESOURCE_X_GATE_OPEN
		|| gate_formation != assertion->common.resource_formation)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;

	if (!pcm_entry_ref_acquire(
			&assertion->common.logical_assertion.resource, true,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	requester_node = assertion->common.logical_assertion.requester_node;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL || state->authority_generation == 0
		|| state->next_enqueue_order == 0) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto unlock;
	}
	request = &state->requests[requester_node];
	if (pcm_resource_x_terminal_tombstone_valid(&state->terminal_tombstone)
		&& state->terminal_tombstone.requester_node == requester_node) {
		ClusterPcmResourceXMasterRequest *terminal
			= &state->terminal_tombstone.request;

		if (pcm_resource_x_assert_common_matches_request(
				&assertion->common, terminal)) {
			pcm_resource_x_terminal_tombstone_snapshot(
				&entry->tag, state, &state->terminal_tombstone, out);
			result = RESOURCE_X_APPLY_DUPLICATE;
			goto unlock;
		}
		if (assertion->common.assertion_sequence
				<= terminal->assertion_sequence) {
			pcm_resource_x_terminal_tombstone_snapshot(
				&entry->tag, state, &state->terminal_tombstone, out);
			result = RESOURCE_X_APPLY_STALE;
			goto unlock;
		}
	}
	if (request->phase != RESOURCE_X_MASTER_NONE) {
		bool same_attempt
			= pcm_resource_x_assert_common_matches_request(
				&assertion->common, request);

		if (same_attempt || request->phase != RESOURCE_X_MASTER_RELEASED) {
			if (same_attempt
				&& request->phase == RESOURCE_X_MASTER_WAIT_BLOCKERS
				&& !pcm_resource_x_arm_block_intents_locked(
					entry, state, request, requester_node))
				request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
			else if (same_attempt
					 && request->phase
						== RESOURCE_X_MASTER_GRANT_COMMITTED
					 && !pcm_resource_x_redrive_grant_intent_locked(
						 entry, state, request, requester_node))
				request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
			pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
										   request, out);
			result = request->phase == RESOURCE_X_MASTER_RECOVERY_BLOCKED
				? RESOURCE_X_APPLY_RECOVERY_BLOCKED
				: (same_attempt ? RESOURCE_X_APPLY_DUPLICATE
								: RESOURCE_X_APPLY_STALE);
			goto unlock;
		}
	}
	if (require_bootstrap) {
		receipt = &state->bootstrap_receipts[requester_node];
		if (!pcm_resource_x_bootstrap_receipt_valid(receipt)) {
			result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			goto unlock;
		}
		if (!pcm_resource_x_bootstrapped_assert_matches_receipt(
				assertion, receipt,
				authenticated_ingress_connection_generation,
				r4_record_generation, current_master_session_incarnation,
				current_master_sender_connection_generation)
			|| receipt->sampled_base != state->authority_generation) {
			if (receipt->state == RESOURCE_X_BOOTSTRAP_RECEIPT_RECEIVED
				&& (receipt->sampled_base != state->authority_generation
					|| receipt->r4_record_generation
						!= r4_record_generation
					|| receipt->request.master_session_incarnation
						!= current_master_session_incarnation
					|| receipt->authenticated_ingress_connection_generation
						!= authenticated_ingress_connection_generation
					|| receipt->ack.sender_connection_generation
						!= current_master_sender_connection_generation))
				pcm_resource_x_bootstrap_receipt_invalidate(receipt);
			result = RESOURCE_X_APPLY_STALE;
			goto unlock;
		}
	}
	if (assertion->common.base_authority_generation != state->authority_generation
		|| (entry->pending_x_requester_node != -1
			&& entry->pending_x_requester_node != requester_node)
		|| state->next_enqueue_order == UINT64_MAX) {
		result = assertion->common.base_authority_generation
				 != state->authority_generation
			? RESOURCE_X_APPLY_STALE : RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto unlock;
	}
	if (require_bootstrap)
		receipt->state = RESOURCE_X_BOOTSTRAP_RECEIPT_CONSUMED_BY_ASSERT;

	memset(request, 0, sizeof(*request));
	request->base_authority_generation = assertion->common.base_authority_generation;
	request->resource_formation = assertion->common.resource_formation;
	request->master_session_incarnation = assertion->common.master_session_incarnation;
	request->assertion_sequence = assertion->common.assertion_sequence;
	request->enqueue_order = state->next_enqueue_order++;
	request->ordered_lane = assertion->common.ordered_lane;
	request->sender_connection_generation
		= assertion->common.sender_connection_generation;
	request->source_node = -1;
	request->phase = RESOURCE_X_MASTER_QUEUED;

	{
		int32 head_node = -1;

		(void)pcm_resource_x_master_head(state, &head_node);
		if (head_node != requester_node)
			request->phase = RESOURCE_X_MASTER_QUEUED;
		else {
			request->head_transition_generation
				= pg_atomic_read_u64(&entry->transition_count_local);
			if (request->head_transition_generation == UINT64_MAX) {
				request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
				goto assertion_head_done;
			}
			pcm_state = (PcmState)pg_atomic_read_u32(&entry->master_state);
			if (pcm_state == PCM_STATE_X && entry->x_holder_node >= 0
				&& entry->x_holder_node != requester_node)
				incompatible = UINT32_C(1) << (uint32)entry->x_holder_node;
			else if (pcm_state == PCM_STATE_S)
				incompatible = pg_atomic_read_u32(&entry->s_holders_bitmap)
					& ~(UINT32_C(1) << (uint32)requester_node);
			request->incompatible_holders_bitmap = incompatible;
			request->phase = incompatible != 0 ? RESOURCE_X_MASTER_WAIT_BLOCKERS
										 : RESOURCE_X_MASTER_WAIT_PROOF;
			if (incompatible != 0
				&& !pcm_resource_x_arm_block_intents_locked(
					entry, state, request, requester_node))
				request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
		}
	assertion_head_done:
		;
	}
	pcm_resource_x_master_snapshot(&entry->tag, requester_node, state, request, out);
	result = request->phase == RESOURCE_X_MASTER_RECOVERY_BLOCKED
		? RESOURCE_X_APPLY_RECOVERY_BLOCKED : RESOURCE_X_APPLY_APPLIED;

unlock:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_assert_exact(const ResourceXDecodedFrame *assertion,
										 int32 authenticated_source_node,
										 ResourceXMasterSnapshot *out)
{
	return pcm_resource_x_assert_exact_internal(
		assertion, authenticated_source_node, false, 0, 0, 0, 0, out);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_assert_bootstrapped_exact(
	const ResourceXDecodedFrame *assertion, int32 authenticated_source_node,
	uint32 authenticated_ingress_connection_generation,
	uint64 r4_record_generation, uint64 current_master_session_incarnation,
	uint32 current_master_sender_connection_generation,
	ResourceXMasterSnapshot *out)
{
	return pcm_resource_x_assert_exact_internal(
		assertion, authenticated_source_node, true,
		authenticated_ingress_connection_generation,
		r4_record_generation, current_master_session_incarnation,
		current_master_sender_connection_generation, out);
}

static bool
pcm_resource_x_remote_proof_valid(const ResourceXDecodedFrame *frame)
{
	const ResourceXDecodedBlockedToN *proof = &frame->body.blocked_to_n;
	int dependency_index;

	if (!frame->blocked_has_remote_proof
		|| proof->source_carrier_generation == 0
		|| proof->requester_target_generation == 0
		|| proof->proof_kind != RESOURCE_X_PROOF_REMOTE_CARRIER
		|| proof->source_disposition
			   != RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE
		|| proof->proof_flags != 0
		|| proof->holder_connection_generation == 0
		|| proof->acting_formation != frame->common.resource_formation
		|| proof->dependency_count > RESOURCE_X_DEPENDENCY_MAX)
		return false;
	for (dependency_index = proof->dependency_count;
		 dependency_index < RESOURCE_X_DEPENDENCY_MAX; dependency_index++)
		if (proof->dependencies[dependency_index] != 0)
			return false;
	return true;
}

static uint64
pcm_resource_x_monotonic_us(void)
{
	instr_time now;

	INSTR_TIME_SET_CURRENT(now);
	return (uint64)INSTR_TIME_GET_MICROSEC(now);
}

static bool
pcm_resource_x_arm_block_intents_locked(
	struct GrdEntry *entry, ClusterPcmResourceXMasterState *state,
	ClusterPcmResourceXMasterRequest *request, int32 requester_node)
{
	ClusterPcmResourceXBlockIntent pending[RESOURCE_X_PROTOCOL_NODE_LIMIT];
	PcmState pcm_state;
	uint32 armed_bitmap = 0;
	uint32 incompatible;
	uint32 s_holders_bitmap;
	uint64 now_us;
	int32 holder_node;

	Assert(entry != NULL);
	Assert(state != NULL);
	Assert(request != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	incompatible = request->incompatible_holders_bitmap
		& ~request->blocked_holders_bitmap;
	if (incompatible == 0)
		return true;
	if (cluster_node_id < 0
		|| cluster_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return false;
	now_us = pcm_resource_x_monotonic_us();
	if (now_us == 0 || now_us == UINT64_MAX)
		return false;
	memset(pending, 0, sizeof(pending));
	pcm_state = (PcmState)pg_atomic_read_u32(&entry->master_state);
	s_holders_bitmap = pg_atomic_read_u32(&entry->s_holders_bitmap);
	if (pcm_state == PCM_STATE_S && request->source_node == -1
		&& (s_holders_bitmap
			& (UINT32_C(1) << (uint32)requester_node)) == 0) {
		uint32 candidates
			= request->incompatible_holders_bitmap & s_holders_bitmap;

		request->source_node
			= pcm_resource_x_select_shared_current_carrier_locked(
				entry, candidates);
		if (request->source_node < 0)
			return false;
	}
	for (holder_node = 0; holder_node < RESOURCE_X_PROTOCOL_NODE_LIMIT;
		 holder_node++) {
		ResourceXDecodedFrame block;
		ResourceXIntentBodyHandle body;
		ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
		uint32 holder_bit = UINT32_C(1) << (uint32)holder_node;
		uint16 payload_len = 0;
		bool is_selected_s_source;
		bool is_source;
		bool is_x_source;

		if ((incompatible & holder_bit) == 0)
			continue;
		is_x_source = pcm_state == PCM_STATE_X
			&& entry->x_holder_node == holder_node;
		if (!is_x_source
			&& (pcm_state != PCM_STATE_S
				|| (s_holders_bitmap & holder_bit) == 0))
			return false;
		is_selected_s_source = pcm_state == PCM_STATE_S
			&& request->source_node == holder_node;
		is_source = is_x_source || is_selected_s_source;
		memset(&block, 0, sizeof(block));
		block.kind = RESOURCE_X_WIRE_BLOCK_TO_N;
		block.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
		block.common.logical_assertion.resource = entry->tag;
		block.common.logical_assertion.requester_node = requester_node;
		block.common.base_authority_generation
			= request->base_authority_generation;
		block.common.resource_formation = request->resource_formation;
		block.common.master_session_incarnation
			= request->master_session_incarnation;
		block.common.assertion_sequence = request->assertion_sequence;
		block.common.ordered_lane = request->ordered_lane;
		block.common.action_node = holder_node;
		block.common.observed_mode
			= (uint8)(is_x_source ? PCM_STATE_X : PCM_STATE_S);
		block.common.target_mode = (uint8)PCM_STATE_N;
		block.common.source_candidate = is_source ? 1 : 0;
		block.common.retain_pi_if_dirty = is_source ? 1 : 0;
		/* This retained wire-shaped body is not a physical frame yet.
		 * sender freshness is rebound from the exact master-to-holder DATA
		 * session by LMS immediately before transport admission. */
		block.common.sender_connection_generation
			= PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
		block.common.outcome = RESOURCE_X_OUTCOME_NONE;
		block.common.authority_generation
			= request->base_authority_generation;
		if (!cluster_resource_x_wire_encode(
				RESOURCE_X_MSG_BLOCK_TO_N, &block,
				pending[holder_node].payload,
				sizeof(pending[holder_node].payload), &payload_len,
				&reject)
			|| payload_len != RESOURCE_X_CONTROL_V1_BYTES)
			return false;
		memset(&body, 0, sizeof(body));
		body.assertion = block.common.logical_assertion;
		body.owner_generation = request->assertion_sequence;
		body.owner_node = (uint32)cluster_node_id;
		body.owner_kind = RESOURCE_X_INTENT_OWNER_MASTER_BLOCK;
		body.owner_index = (uint8)holder_node;
		if (!cluster_pcm_lock_resource_x_intent_arm_exact(
				&pending[holder_node].slot, &body,
				request->assertion_sequence,
				request->base_authority_generation, now_us,
				(uint32)holder_node, payload_len,
				RESOURCE_X_WIRE_BLOCK_TO_N))
			return false;
		if (state->block_intents[holder_node].slot.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY) {
			ResourceXIntentSlot *existing
				= &state->block_intents[holder_node].slot;
			ResourceXIntentSlot *candidate
				= &pending[holder_node].slot;

			if ((existing->state != RESOURCE_X_INTENT_SLOT_ARMED
				 && existing->state != RESOURCE_X_INTENT_SLOT_STAGED)
				|| existing->logical_generation
					   != candidate->logical_generation
				|| existing->authority_generation
					   != candidate->authority_generation
				|| existing->destination_node
					   != candidate->destination_node
				|| existing->payload_bytes != candidate->payload_bytes
				|| existing->kind != candidate->kind
				|| memcmp(&existing->body, &candidate->body,
						  sizeof(existing->body)) != 0
				|| memcmp(state->block_intents[holder_node].payload,
						  pending[holder_node].payload,
						  candidate->payload_bytes) != 0)
				return false;
			continue;
		}
		armed_bitmap |= holder_bit;
	}
	for (holder_node = 0; holder_node < RESOURCE_X_PROTOCOL_NODE_LIMIT;
		 holder_node++) {
		uint32 holder_bit = UINT32_C(1) << (uint32)holder_node;

		if ((incompatible & holder_bit) == 0)
			continue;
		if ((armed_bitmap & holder_bit) != 0)
			state->block_intents[holder_node] = pending[holder_node];
	}
	if (armed_bitmap == 0)
		return true;
	if (!pcm_resource_x_intent_mark_dirty()) {
		for (holder_node = 0; holder_node < RESOURCE_X_PROTOCOL_NODE_LIMIT;
			 holder_node++)
			if ((armed_bitmap & (UINT32_C(1) << (uint32)holder_node)) != 0)
				memset(&state->block_intents[holder_node], 0,
					   sizeof(state->block_intents[holder_node]));
		return false;
	}
	return true;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_block_intent_snapshot_exact(
	const ResourceXAssertion *assertion, int32 holder_node,
	ResourceXIntentSlot *slot_out, void *payload_out,
	uint16 payload_capacity)
{
	ClusterPcmResourceXBlockIntent *intent;
	ClusterPcmResourceXMasterRequest *request;
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXApplyResult result;
	struct GrdEntry *entry;
	uint32 holder_bit;
	int32 requester_node;

	if (slot_out != NULL)
		memset(slot_out, 0, sizeof(*slot_out));
	if (!resource_x_assertion_valid(assertion)
		|| holder_node < 0
		|| holder_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| slot_out == NULL || payload_out == NULL
		|| payload_capacity < RESOURCE_X_CONTROL_V1_BYTES)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	requester_node = assertion->requester_node;
	holder_bit = UINT32_C(1) << (uint32)holder_node;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto block_intent_snapshot_done;
	}
	request = &state->requests[requester_node];
	intent = &state->block_intents[holder_node];
	if (request->phase == RESOURCE_X_MASTER_NONE
		|| intent->slot.state == RESOURCE_X_INTENT_SLOT_EMPTY) {
		result = RESOURCE_X_APPLY_NOT_FOUND;
		goto block_intent_snapshot_done;
	}
	if (!resource_x_assertion_equal(assertion, &intent->slot.body.assertion)) {
		result = RESOURCE_X_APPLY_STALE;
		goto block_intent_snapshot_done;
	}
	if ((request->incompatible_holders_bitmap & holder_bit) == 0
		|| intent->slot.state > RESOURCE_X_INTENT_SLOT_STAGED
		|| !pcm_resource_x_intent_body_valid(&intent->slot.body)
		|| intent->slot.logical_generation != request->assertion_sequence
		|| intent->slot.authority_generation
			   != request->base_authority_generation
		|| intent->slot.destination_node != (uint32)holder_node
		|| intent->slot.payload_bytes != RESOURCE_X_CONTROL_V1_BYTES
		|| intent->slot.kind != RESOURCE_X_WIRE_BLOCK_TO_N
		|| intent->slot.body.owner_generation
			   != request->assertion_sequence
		|| intent->slot.body.owner_kind
			   != RESOURCE_X_INTENT_OWNER_MASTER_BLOCK
		|| intent->slot.body.owner_index != (uint8)holder_node) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto block_intent_snapshot_done;
	}
	*slot_out = intent->slot;
	memcpy(payload_out, intent->payload, intent->slot.payload_bytes);
	result = RESOURCE_X_APPLY_APPLIED;

block_intent_snapshot_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

static bool
pcm_resource_x_holder_status_matches_block(
	const ClusterPcmResourceXHolderStatus *record,
	const ResourceXDecodedFrame *block, int32 authenticated_master_node)
{
	ResourceXDecodedFrame status;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;

	if (record == NULL || block == NULL || record->valid != 1
		|| record->logical_generation != block->common.assertion_sequence
		|| record->authority_generation
			   != block->common.base_authority_generation
		|| record->resource_formation != block->common.resource_formation
		|| record->destination_node != (uint32)authenticated_master_node
		|| record->kind != RESOURCE_X_WIRE_BLOCKED_TO_N
		|| !resource_x_assertion_equal(&record->body.assertion,
			&block->common.logical_assertion)
		|| !cluster_resource_x_wire_decode(
			RESOURCE_X_MSG_BLOCKED_TO_N, record->payload,
			record->payload_bytes, &status, &reject))
		return false;
	return status.common.base_authority_generation
			   == block->common.base_authority_generation
		&& status.common.resource_formation
			   == block->common.resource_formation
		&& status.common.master_session_incarnation
			   == block->common.master_session_incarnation
		&& status.common.assertion_sequence
			   == block->common.assertion_sequence
		&& status.common.ordered_lane == block->common.ordered_lane
		&& status.common.action_node == block->common.action_node
		&& status.common.observed_mode == block->common.observed_mode
		&& status.common.target_mode == block->common.target_mode
		&& status.common.source_candidate
			   == block->common.source_candidate
		&& status.common.retain_pi_if_dirty
			   == block->common.retain_pi_if_dirty
		&& status.common.authority_generation
			   == block->common.authority_generation
		&& status.common.outcome == RESOURCE_X_OUTCOME_OK;
}

static bool
pcm_resource_x_rearm_holder_status_locked(
	ClusterPcmResourceXMasterState *state,
	const ClusterPcmResourceXHolderStatus *record)
{
	ClusterPcmResourceXControlIntent *intent;
	uint64 now_us;

	Assert(state != NULL);
	Assert(record != NULL);
	intent = &state->holder_status_intent;
	if (intent->slot.state != RESOURCE_X_INTENT_SLOT_EMPTY)
		return intent->slot.logical_generation == record->logical_generation
			&& intent->slot.authority_generation
				   == record->authority_generation
			&& intent->slot.destination_node == record->destination_node
			&& intent->slot.payload_bytes == record->payload_bytes
			&& intent->slot.kind == record->kind
			&& memcmp(&intent->slot.body, &record->body,
					  sizeof(record->body)) == 0;
	memcpy(intent->payload, record->payload, record->payload_bytes);
	now_us = pcm_resource_x_monotonic_us();
	if (!cluster_pcm_lock_resource_x_intent_arm_exact(
			&intent->slot, &record->body, record->logical_generation,
			record->authority_generation, now_us, record->destination_node,
			record->payload_bytes, (ResourceXWireKind)record->kind)
		|| !pcm_resource_x_intent_mark_dirty()) {
		memset(intent, 0, sizeof(*intent));
		return false;
	}
	return true;
}

static bool
pcm_resource_x_rearm_holder_source_locked(
	ClusterPcmResourceXMasterState *state,
	const ClusterPcmResourceXHolderStatus *status_record,
	const ClusterPcmResourceXHolderImage *image_record)
{
	ResourceXIntentSlot *status_slot;
	ResourceXIntentSlot *image_slot;
	uint64 now_us;
	bool armed_image = false;
	bool armed_status = false;

	Assert(state != NULL);
	Assert(status_record != NULL);
	Assert(image_record != NULL);
	status_slot = &state->holder_status_intent.slot;
	image_slot = &state->holder_image_intent;
	if (status_slot->state != RESOURCE_X_INTENT_SLOT_EMPTY
		&& (status_slot->logical_generation
				!= status_record->logical_generation
			|| status_slot->authority_generation
					!= status_record->authority_generation
			|| status_slot->destination_node
					!= status_record->destination_node
			|| status_slot->payload_bytes != status_record->payload_bytes
			|| status_slot->kind != status_record->kind
			|| memcmp(&status_slot->body, &status_record->body,
					  sizeof(status_record->body)) != 0))
		return false;
	if (image_slot->state != RESOURCE_X_INTENT_SLOT_EMPTY
		&& (image_slot->logical_generation
				!= image_record->logical_generation
			|| image_slot->authority_generation
					!= image_record->authority_generation
			|| image_slot->destination_node
					!= image_record->destination_node
			|| image_slot->payload_bytes != image_record->payload_bytes
			|| image_slot->kind != image_record->kind
			|| memcmp(&image_slot->body, &image_record->body,
					  sizeof(image_record->body)) != 0))
		return false;
	if (status_slot->state != RESOURCE_X_INTENT_SLOT_EMPTY
		&& image_slot->state != RESOURCE_X_INTENT_SLOT_EMPTY)
		return true;
	now_us = pcm_resource_x_monotonic_us();
	if (now_us == 0 || now_us == UINT64_MAX)
		return false;
	if (status_slot->state == RESOURCE_X_INTENT_SLOT_EMPTY) {
		memcpy(state->holder_status_intent.payload,
			status_record->payload, status_record->payload_bytes);
		if (!cluster_pcm_lock_resource_x_intent_arm_exact(
				status_slot, &status_record->body,
				status_record->logical_generation,
				status_record->authority_generation, now_us,
				status_record->destination_node,
				status_record->payload_bytes,
				(ResourceXWireKind)status_record->kind))
			return false;
		armed_status = true;
	}
	if (image_slot->state == RESOURCE_X_INTENT_SLOT_EMPTY) {
		if (!cluster_pcm_lock_resource_x_intent_arm_exact(
				image_slot, &image_record->body,
				image_record->logical_generation,
				image_record->authority_generation, now_us,
				image_record->destination_node,
				image_record->payload_bytes,
				(ResourceXWireKind)image_record->kind)) {
			if (armed_status) {
				memset(status_slot, 0, sizeof(*status_slot));
				memset(state->holder_status_intent.payload, 0,
					   sizeof(state->holder_status_intent.payload));
			}
			return false;
		}
		armed_image = true;
	}
	if ((armed_status || armed_image)
		&& !pcm_resource_x_intent_mark_dirty()) {
		if (armed_status) {
			memset(status_slot, 0, sizeof(*status_slot));
			memset(state->holder_status_intent.payload, 0,
				   sizeof(state->holder_status_intent.payload));
		}
		if (armed_image)
			memset(image_slot, 0, sizeof(*image_slot));
		return false;
	}
	return true;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_block_to_n_exact(
	const ResourceXDecodedFrame *block, int32 authenticated_master_node)
{
	ClusterPcmResourceXControlIntent *intent;
	ClusterPcmResourceXHolderStatus *record;
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXDecodedFrame status;
	ResourceXIntentBodyHandle body;
	ResourceXApplyResult result;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	struct GrdEntry *entry;
	PcmState pcm_state;
	uint32 holder_bit;
	uint64 gate_formation;
	uint16 payload_len = 0;
	bool broadcast = false;

	if (block == NULL || block->kind != RESOURCE_X_WIRE_BLOCK_TO_N
		|| !resource_x_assertion_valid(&block->common.logical_assertion)
		|| authenticated_master_node < 0
		|| authenticated_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| cluster_node_id < 0
		|| cluster_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| block->common.action_node != cluster_node_id
		|| block->common.base_authority_generation == 0
		|| block->common.authority_generation
			   != block->common.base_authority_generation
		|| block->common.resource_formation == 0
		|| block->common.master_session_incarnation == 0
		|| block->common.assertion_sequence == 0
		|| block->common.sender_connection_generation == 0
		|| block->common.target_mode != (uint8)PCM_STATE_N
		|| block->common.outcome != RESOURCE_X_OUTCOME_NONE
		|| ClusterPcm == NULL
		|| cluster_gcs_lookup_master(block->common.logical_assertion.resource)
			   != authenticated_master_node)
		return RESOURCE_X_APPLY_INVALID;
	gate_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_OPEN
		|| gate_formation != block->common.resource_formation)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (!pcm_entry_ref_acquire(&block->common.logical_assertion.resource,
			false, &entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	holder_bit = UINT32_C(1) << (uint32)cluster_node_id;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto block_to_n_done;
	}
	record = &state->holder_status;
	intent = &state->holder_status_intent;
	if (record->valid != 0) {
		if (!pcm_resource_x_holder_status_matches_block(
				record, block, authenticated_master_node))
			result = RESOURCE_X_APPLY_STALE;
		else if (!pcm_resource_x_rearm_holder_status_locked(state, record))
			result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		else
			result = RESOURCE_X_APPLY_DUPLICATE;
		goto block_to_n_done;
	}
	if (intent->slot.state != RESOURCE_X_INTENT_SLOT_EMPTY) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto block_to_n_done;
	}
	pcm_state = (PcmState)pg_atomic_read_u32(&entry->master_state);
	if (state->authority_generation
			!= block->common.base_authority_generation) {
		result = RESOURCE_X_APPLY_STALE;
		goto block_to_n_done;
	}
	/* The current slice closes the proof-free S blocker.  An X source must
	 * first freeze its immutable proof and page carrier; until that owner is
	 * present it remains writable and this consumer fails closed. */
	if (pcm_state != PCM_STATE_S
		|| (pg_atomic_read_u32(&entry->s_holders_bitmap) & holder_bit) == 0
		|| block->common.observed_mode != (uint8)PCM_STATE_S
		|| block->common.source_candidate != 0
		|| block->common.retain_pi_if_dirty != 0) {
		result = RESOURCE_X_APPLY_BAD_STATE;
		goto block_to_n_done;
	}
	memset(&status, 0, sizeof(status));
	status.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	status.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	status.common = block->common;
	status.common.action_node = cluster_node_id;
	status.common.source_candidate = 0;
	status.common.retain_pi_if_dirty = 0;
	status.common.outcome = RESOURCE_X_OUTCOME_OK;
	if (!cluster_resource_x_wire_encode(
			RESOURCE_X_MSG_BLOCKED_TO_N, &status, record->payload,
			sizeof(record->payload), &payload_len, &reject)
		|| payload_len != RESOURCE_X_CONTROL_V1_BYTES) {
		memset(record, 0, sizeof(*record));
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto block_to_n_done;
	}
	memset(&body, 0, sizeof(body));
	body.assertion = block->common.logical_assertion;
	body.owner_generation = block->common.assertion_sequence;
	body.owner_node = (uint32)cluster_node_id;
	body.owner_kind = RESOURCE_X_INTENT_OWNER_HOLDER_STATUS;
	record->body = body;
	record->logical_generation = block->common.assertion_sequence;
	record->authority_generation
		= block->common.base_authority_generation;
	record->resource_formation = block->common.resource_formation;
	record->destination_node = (uint32)authenticated_master_node;
	record->payload_bytes = payload_len;
	record->kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	record->valid = 1;
	if (!pcm_resource_x_rearm_holder_status_locked(state, record)) {
		memset(record, 0, sizeof(*record));
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto block_to_n_done;
	}
	cluster_pcm_transition_apply(entry, PCM_TRANS_S_TO_N_INVALIDATE,
							 cluster_node_id);
	broadcast = true;
	result = RESOURCE_X_APPLY_APPLIED;

block_to_n_done:
	LWLockRelease(&entry->entry_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

static ResourceXApplyResult pcm_resource_x_holder_pair_decode_locked(
	const ClusterPcmResourceXMasterState *state,
	ResourceXDecodedFrame *status_out, ResourceXDecodedFrame *image_out);
static ResourceXApplyResult pcm_resource_x_holder_pair_drain_domain_locked(
	const BufferTag *tag, const ClusterPcmResourceXMasterState *state,
	int32 authenticated_master_node, uint64 authenticated_master_session,
	const ResourceXAssertion *assertion, uint64 assertion_sequence);

/* A prepared remote-S predecessor can become retained after this node has
 * emitted a non-authoritative kind-9 request but before its ACK binds a base.
 * Resolve that ordering only at the pair-retention linearization point: an
 * exact REQUEST_DISPATCHED round may be discarded while preserving its
 * monotone attempt floor.  Any bound/ASSERT/terminal round, local owner, or
 * identity drift remains fail closed; SourceSettlement therefore keeps its
 * frozen EMPTY-only cover table. */
static ResourceXApplyResult
pcm_resource_x_s_predecessor_cancel_unbound_round_locked(
	struct GrdEntry *entry, const ResourceXDecodedFrame *block,
	int32 authenticated_master_node, bool *broadcast)
{
	static const ResourceXAcquisitionRef empty_ref;
	static const ResourceXDecodedCommon empty_common;
	static const uint8 zero_reserved[1];
	ClusterPcmResourceXBootstrapRound *round;
	const ResourceXDecodedCommon *request;

	Assert(entry != NULL);
	Assert(block != NULL);
	Assert(broadcast != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	round = &entry->resource_x_bootstrap_round;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY)
		return RESOURCE_X_APPLY_APPLIED;
	/* Every other phase remains owned by SourceSettlement's existing total
	 * cover table.  Retention itself does not preempt or reinterpret it. */
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_REQUEST_DISPATCHED)
		return RESOURCE_X_APPLY_APPLIED;
	if (!pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner))
		return RESOURCE_X_APPLY_APPLIED;
	if (entry->resource_x_local_owner.state != RESOURCE_X_LOCAL_OWNER_EMPTY)
		return RESOURCE_X_APPLY_APPLIED;

	request = &round->request;
	if (!cluster_pcm_lock_resource_x_gate_open_exact(block->common.resource_formation)
		|| cluster_gcs_lookup_master(request->logical_assertion.resource)
			   != authenticated_master_node
		|| round->current_master_node != authenticated_master_node
		|| round->resource_formation != block->common.resource_formation
		|| round->master_session_incarnation != block->common.master_session_incarnation
		|| round->r4_record_generation == 0 || round->r4_record_generation == UINT64_MAX
		|| round->head_no_progress_deadline_us == 0
		|| round->head_no_progress_deadline_us == UINT64_MAX || round->last_dispatch_us == 0
		|| round->last_dispatch_us >= round->head_no_progress_deadline_us
		|| round->retry_slice_us == 0 || round->retry_slice_us == UINT64_MAX
		|| round->requester_sender_connection_generation == 0
		|| round->requester_sender_connection_generation == UINT32_MAX
		|| round->master_ingress_connection_generation == 0
		|| round->master_ingress_connection_generation == UINT32_MAX || round->accepted_base != 0
		|| round->terminal_authority_generation != 0 || round->cached_ownership_generation != 0
		|| round->install_claim_pending_generation != 0
		|| round->install_claim_reservation_token != 0
		|| memcmp(&round->ack, &empty_common, sizeof(empty_common)) != 0
		|| memcmp(&round->assertion, &empty_common, sizeof(empty_common)) != 0
		|| memcmp(&round->terminal_ref, &empty_ref, sizeof(empty_ref)) != 0
		|| memcmp(round->reserved, zero_reserved, sizeof(zero_reserved)) != 0
		|| !resource_x_assertion_valid(&request->logical_assertion)
		|| !BufferTagsEqual(&request->logical_assertion.resource, &entry->tag)
		|| request->logical_assertion.requester_node != cluster_node_id
		|| request->base_authority_generation != 0 || request->authority_generation != 0
		|| request->resource_formation != round->resource_formation
		|| request->master_session_incarnation != round->master_session_incarnation
		|| request->assertion_sequence == 0 || request->assertion_sequence == UINT64_MAX
		|| request->assertion_sequence != round->highest_attempt_floor || request->ordered_lane != 0
		|| request->action_node != cluster_node_id || request->observed_mode != (uint8)PCM_STATE_N
		|| request->target_mode != (uint8)PCM_STATE_X || request->source_candidate != 0
		|| request->retain_pi_if_dirty != 0
		|| request->sender_connection_generation != round->requester_sender_connection_generation
		|| request->outcome != RESOURCE_X_OUTCOME_NONE || request->flags != 0
		|| request->semantic_crc32c != 0)
		return RESOURCE_X_APPLY_APPLIED;

	pcm_resource_x_bootstrap_round_clear_binding_locked(round);
	*broadcast = true;
	return RESOURCE_X_APPLY_APPLIED;
}

static bool
pcm_resource_x_prepared_terminal_x_source_exact_locked(
	struct GrdEntry *entry, const ResourceXDecodedFrame *block,
	int32 authenticated_master_node,
	const ClusterPcmOwnSnapshot *prepared_x_source,
	const ResourceXLocalOwnerHandle *x_owner)
{
	ClusterPcmResourceXBootstrapRound *round;
	ClusterPcmResourceXLocalOwner *local_owner;
	ResourceXTerminalXLineage lineage;

	Assert(entry != NULL);
	Assert(block != NULL);
	Assert(prepared_x_source != NULL);
	Assert(x_owner != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	round = &entry->resource_x_bootstrap_round;
	local_owner = &entry->resource_x_local_owner;
	memset(&lineage, 0, sizeof(lineage));
	return pcm_resource_x_local_owner_valid_locked(local_owner)
		&& local_owner->state == RESOURCE_X_LOCAL_OWNER_REVOKING
		&& pcm_resource_x_local_owner_handle_equal(
			&local_owner->handle, x_owner)
		&& pcm_resource_x_local_handoff_valid(&local_owner->handoff)
		&& pcm_resource_x_local_handoff_matches_handle(
			&local_owner->handoff, x_owner)
		&& pcm_resource_x_local_handoff_successor_exact(
			&local_owner->handoff, block, authenticated_master_node,
			x_owner->r4_record_generation, prepared_x_source->generation)
		&& pcm_resource_x_local_handoff_round_current_locked(
			&local_owner->handoff, round)
		&& pcm_resource_x_terminal_x_lineage_locked(
			entry, round, block, authenticated_master_node,
			x_owner->r4_record_generation, prepared_x_source->generation,
			false, &lineage)
		&& pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
			entry, round, &x_owner->ref,
			x_owner->master_session_incarnation,
			x_owner->r4_record_generation,
			x_owner->buffer_ownership_generation);
}

static ResourceXApplyResult
pcm_resource_x_block_to_n_source_exact_internal(
	const ResourceXDecodedFrame *block, int32 authenticated_master_node,
	const ResourceXDecodedFrame *blocked_status,
	const ResourceXDecodedFrame *image_envelope,
	const ClusterPcmOwnSnapshot *prepared_s_source,
	XLogRecPtr prepared_page_lsn, uint64 prepared_page_scn,
	uint32 prepared_page_checksum,
	const ClusterPcmOwnSnapshot *prepared_x_source,
	const ResourceXLocalOwnerHandle *x_owner,
	ClusterPcmXRevokeFinishMode required_x_finish_mode)
{
	ClusterPcmResourceXHolderStatus status_record;
	ClusterPcmResourceXHolderImage image_record;
	ClusterPcmResourceXMasterState *state;
	ResourceXDecodedFrame canonical_image;
	ResourceXDecodedFrame canonical_status;
	ResourceXDecodedFrame decoded_image;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXIntentBodyHandle image_body;
	ResourceXIntentBodyHandle status_body;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	struct GrdEntry *entry;
	uint64 gate_formation;
	uint16 image_payload_bytes = 0;
	uint16 status_payload_bytes = 0;
	bool broadcast = false;
	bool created_retention_container = false;
	bool installed_remote_source;
	bool local_grd_source;
	bool prepared_remote_s_source = false;
	bool prepared_terminal_x_source_exact = false;
	bool prepared_x_source_exact = false;
	bool pristine_remote_source;
	bool shared_s_source;
	uint8 source_mode;

	if (block == NULL)
		return RESOURCE_X_APPLY_INVALID;
	source_mode = block->common.observed_mode;
	shared_s_source = source_mode == (uint8)PCM_STATE_S;
	if (blocked_status == NULL || image_envelope == NULL
		|| block->kind != RESOURCE_X_WIRE_BLOCK_TO_N
		|| blocked_status->kind != RESOURCE_X_WIRE_BLOCKED_TO_N
		|| !blocked_status->blocked_has_remote_proof
		|| image_envelope->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE
		|| !resource_x_assertion_valid(&block->common.logical_assertion)
		|| authenticated_master_node < 0
		|| authenticated_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| cluster_node_id < 0
		|| cluster_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| cluster_gcs_lookup_master(block->common.logical_assertion.resource)
			!= authenticated_master_node
		|| block->common.action_node != cluster_node_id
		|| (source_mode != (uint8)PCM_STATE_X
			&& source_mode != (uint8)PCM_STATE_S)
		|| block->common.target_mode != (uint8)PCM_STATE_N
		|| block->common.source_candidate != 1
		|| block->common.retain_pi_if_dirty != 1
		|| block->common.outcome != RESOURCE_X_OUTCOME_NONE
		|| block->common.base_authority_generation == UINT64_MAX
		|| blocked_status->common.action_node != cluster_node_id
		|| blocked_status->common.observed_mode != source_mode
		|| blocked_status->common.target_mode != (uint8)PCM_STATE_N
		|| blocked_status->common.flags
			!= RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED
		|| blocked_status->common.outcome != RESOURCE_X_OUTCOME_OK
		|| image_envelope->common.action_node
			!= block->common.logical_assertion.requester_node
		|| image_envelope->common.observed_mode != source_mode
		|| image_envelope->common.target_mode != (uint8)PCM_STATE_X
		|| image_envelope->common.outcome != RESOURCE_X_OUTCOME_OK
		|| image_envelope->common.semantic_crc32c == 0
		|| !resource_x_assertion_equal(&block->common.logical_assertion,
			&blocked_status->common.logical_assertion)
		|| !resource_x_assertion_equal(&block->common.logical_assertion,
			&image_envelope->common.logical_assertion))
		return RESOURCE_X_APPLY_INVALID;
	if (blocked_status->common.base_authority_generation
			!= block->common.base_authority_generation
		|| image_envelope->common.base_authority_generation
			!= block->common.base_authority_generation
		|| blocked_status->common.resource_formation
			!= block->common.resource_formation
		|| image_envelope->common.resource_formation
			!= block->common.resource_formation
		|| blocked_status->common.master_session_incarnation
			!= block->common.master_session_incarnation
		|| image_envelope->common.master_session_incarnation
			!= block->common.master_session_incarnation
		|| blocked_status->common.assertion_sequence
			!= block->common.assertion_sequence
		|| image_envelope->common.assertion_sequence
			!= block->common.assertion_sequence
		|| blocked_status->common.ordered_lane != block->common.ordered_lane
		|| image_envelope->common.ordered_lane != block->common.ordered_lane
		|| blocked_status->common.authority_generation
			!= block->common.base_authority_generation
		|| image_envelope->common.authority_generation
			!= block->common.base_authority_generation + 1
		|| image_envelope->body.image_envelope.conversion_base_generation
			!= block->common.base_authority_generation
		|| blocked_status->body.blocked_to_n.acting_formation
			!= block->common.resource_formation
		|| blocked_status->body.blocked_to_n.holder_connection_generation == 0
		|| blocked_status->body.blocked_to_n.source_carrier_generation
			!= image_envelope->body.image_envelope.source_carrier_generation
		|| blocked_status->body.blocked_to_n.requester_target_generation
			!= image_envelope->body.image_envelope.requester_target_generation
		|| blocked_status->body.blocked_to_n.page_scn_lsn
			!= image_envelope->body.image_envelope.page_scn_lsn
		|| blocked_status->body.blocked_to_n.dependency_count
			!= image_envelope->body.image_envelope.dependency_count
		|| memcmp(blocked_status->body.blocked_to_n.source_fence,
			image_envelope->body.image_envelope.source_fence,
			sizeof(blocked_status->body.blocked_to_n.source_fence)) != 0
		|| memcmp(blocked_status->body.blocked_to_n.dependencies,
			image_envelope->body.image_envelope.dependencies,
			sizeof(blocked_status->body.blocked_to_n.dependencies)) != 0
		|| blocked_status->body.blocked_to_n.source_proof_crc32c
			!= image_envelope->common.semantic_crc32c
		|| blocked_status->body.blocked_to_n.page_checksum
			!= image_envelope->body.image_envelope.page_checksum
		|| blocked_status->body.blocked_to_n.proof_kind
			!= RESOURCE_X_PROOF_REMOTE_CARRIER
		|| blocked_status->body.blocked_to_n.source_disposition
			!= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE)
		return RESOURCE_X_APPLY_STALE;
	if (shared_s_source) {
		if (prepared_s_source == NULL
			|| !BufferTagsEqual(&prepared_s_source->tag,
				&block->common.logical_assertion.resource)
			|| prepared_s_source->generation == 0
			|| prepared_s_source->generation == UINT64_MAX
			|| prepared_s_source->reservation_token == 0
			|| prepared_s_source->flags != PCM_OWN_FLAG_REVOKING
			|| prepared_s_source->pcm_state != (uint8)PCM_STATE_S
			|| prepared_s_source->generation + 1
				!= image_envelope->body.image_envelope.source_carrier_generation
			|| PageGetLSN((Page)image_envelope->body.image_envelope.page_bytes)
				!= prepared_page_lsn
			|| ((PageHeader)image_envelope->body.image_envelope.page_bytes)
					->pd_block_scn != prepared_page_scn
			|| image_envelope->body.image_envelope.page_scn_lsn
				!= prepared_page_scn
			|| image_envelope->body.image_envelope.page_checksum
				!= prepared_page_checksum
			|| image_envelope->body.image_envelope.source_fence[28]
				!= (uint8)PCM_STATE_S)
			return RESOURCE_X_APPLY_STALE;
		prepared_remote_s_source = true;
	} else if (prepared_s_source != NULL) {
		return RESOURCE_X_APPLY_INVALID;
	}
	if (prepared_x_source != NULL || x_owner != NULL
		|| required_x_finish_mode != CLUSTER_PCM_X_REVOKE_FINISH_INVALID) {
		if (prepared_x_source == NULL || x_owner == NULL
			|| shared_s_source)
			return RESOURCE_X_APPLY_INVALID;
		if ((required_x_finish_mode != CLUSTER_PCM_X_REVOKE_FINISH_RETAIN
				&& required_x_finish_mode != CLUSTER_PCM_X_REVOKE_FINISH_DROP)
			|| !pcm_resource_x_local_owner_handle_valid(x_owner)
			|| cluster_pcm_x_revoke_finish_mode(
				&block->common.logical_assertion.resource, 0)
				!= required_x_finish_mode
			|| !BufferTagsEqual(&prepared_x_source->tag,
				&block->common.logical_assertion.resource)
			|| prepared_x_source->pcm_state != (uint8)PCM_STATE_X
			|| prepared_x_source->flags != PCM_OWN_FLAG_REVOKING
			|| prepared_x_source->generation == 0
			|| prepared_x_source->generation == UINT64_MAX
			|| prepared_x_source->reservation_token == 0
			|| prepared_x_source->reservation_token == UINT64_MAX
			|| prepared_x_source->writer_activation_token != 0
			|| prepared_x_source->resource_x_activation_generation != 0
			|| x_owner->buffer_ownership_generation
				!= prepared_x_source->generation
			|| x_owner->reservation_token == UINT64_MAX
			|| x_owner->reservation_token + 1
				!= prepared_x_source->reservation_token
			|| !BufferTagsEqual(&x_owner->ref.assertion.resource,
				&block->common.logical_assertion.resource)
			|| x_owner->ref.formation
				!= block->common.resource_formation
			|| x_owner->master_session_incarnation
				!= block->common.master_session_incarnation
			|| pcm_resource_x_source_fence_get_u64(
				image_envelope->body.image_envelope.source_fence + 20)
				!= prepared_x_source->generation
			|| image_envelope->body.image_envelope.source_fence[28]
				!= (uint8)PCM_STATE_X
			|| image_envelope->body.image_envelope.source_fence[29] != 1
			|| image_envelope->body.image_envelope.source_carrier_generation
				!= prepared_x_source->generation + 1)
			return RESOURCE_X_APPLY_STALE;
		prepared_x_source_exact = true;
	}
	memset(&status_record, 0, sizeof(status_record));
	memset(&image_record, 0, sizeof(image_record));
	/* Retain one connection-neutral logical pair.  Physical DATA copies are
	 * rebound immediately before send, while the requester restores this
	 * canonical generation before comparing the proof-bound image CRC.  The
	 * derived CRC fields therefore move together without changing source,
	 * assertion, authority, or page bytes across a reconnect. */
	canonical_image = *image_envelope;
	canonical_image.common.sender_connection_generation
		= PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	if (!cluster_resource_x_wire_encode(
			RESOURCE_X_MSG_IMAGE_OR_GRANT, &canonical_image,
			image_record.payload, sizeof(image_record.payload),
			&image_payload_bytes, &reject)
		|| image_payload_bytes != RESOURCE_X_IMAGE_V1_BYTES
		|| !cluster_resource_x_wire_decode(
			RESOURCE_X_MSG_IMAGE_OR_GRANT, image_record.payload,
			image_payload_bytes, &decoded_image, &reject)
		|| decoded_image.kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE
		|| decoded_image.common.sender_connection_generation
			!= PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION
		|| decoded_image.common.semantic_crc32c == 0)
		return RESOURCE_X_APPLY_INVALID;
	reject = RESOURCE_X_WIRE_REJECT_NONE;
	canonical_status = *blocked_status;
	canonical_status.common.sender_connection_generation
		= PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	canonical_status.body.blocked_to_n.source_proof_crc32c
		= decoded_image.common.semantic_crc32c;
	if (!cluster_resource_x_wire_encode(
			RESOURCE_X_MSG_BLOCKED_TO_N, &canonical_status,
			status_record.payload, sizeof(status_record.payload),
			&status_payload_bytes, &reject)
		|| status_payload_bytes != RESOURCE_X_PROOF_V1_BYTES)
		return RESOURCE_X_APPLY_INVALID;
	memset(&status_body, 0, sizeof(status_body));
	status_body.assertion = block->common.logical_assertion;
	status_body.owner_generation = block->common.assertion_sequence;
	status_body.owner_node = (uint32)cluster_node_id;
	status_body.owner_kind = RESOURCE_X_INTENT_OWNER_HOLDER_STATUS;
	status_record.body = status_body;
	status_record.logical_generation = block->common.assertion_sequence;
	status_record.authority_generation
		= block->common.base_authority_generation;
	status_record.resource_formation = block->common.resource_formation;
	status_record.destination_node = (uint32)authenticated_master_node;
	status_record.payload_bytes = status_payload_bytes;
	status_record.kind = RESOURCE_X_WIRE_BLOCKED_TO_N;
	status_record.valid = RESOURCE_X_HOLDER_PAIR_PENDING;
	memset(&image_body, 0, sizeof(image_body));
	image_body.assertion = block->common.logical_assertion;
	image_body.owner_generation = block->common.assertion_sequence;
	image_body.owner_node = (uint32)cluster_node_id;
	image_body.owner_kind = RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE;
	image_record.body = image_body;
	image_record.logical_generation = block->common.assertion_sequence;
	image_record.authority_generation
		= image_envelope->common.authority_generation;
	image_record.resource_formation = block->common.resource_formation;
	image_record.destination_node
		= (uint32)block->common.logical_assertion.requester_node;
	image_record.payload_bytes = image_payload_bytes;
	image_record.kind = RESOURCE_X_WIRE_IMAGE_ENVELOPE;
	image_record.valid = RESOURCE_X_HOLDER_PAIR_PENDING;
	gate_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_OPEN
		|| gate_formation != block->common.resource_formation)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (!pcm_entry_ref_acquire(&block->common.logical_assertion.resource,
			false, &entry_ref, &acquire_result)) {
		if (acquire_result != PCM_ENTRY_ACQUIRE_NOT_FOUND)
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		/* The exact current-X carrier is a BufferDesc property on a node whose
		 * authoritative GRD lives at the authenticated remote master.  The GCS
		 * caller has already frozen and revalidated that BufferDesc lineage.
		 * Allocate only the existing keyed Resource-X container so the retained
		 * type-18/type-15 pair has a local owner; this does not create authority. */
		if (!pcm_entry_ref_acquire(&block->common.logical_assertion.resource,
				true, &entry_ref, &acquire_result))
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		created_retention_container = true;
	}
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	prepared_terminal_x_source_exact = prepared_x_source_exact
		&& authenticated_master_node != cluster_node_id
		&& entry->resource_x_requester_base_generation >= 1
		&& entry->resource_x_requester_base_generation
			<= block->common.base_authority_generation
		&& entry->resource_x_retired_acquisition_generation != 0
		&& pcm_resource_x_active_empty_locked(entry)
		&& (PcmState)pg_atomic_read_u32(&entry->master_state) == PCM_STATE_N
		&& entry->x_holder_node == -1
		&& pg_atomic_read_u32(&entry->s_holders_bitmap) == 0
		&& pcm_resource_x_prepared_terminal_x_source_exact_locked(
			entry, block, authenticated_master_node, prepared_x_source,
			x_owner);
	/* The prepared terminal-X predicate is the remote-master provenance
	 * class added for a holder whose local GRD is only an N mirror.  A
	 * self-master DROP is instead admitted by local_grd_source below; its
	 * exact REVOKING snapshot remains a physical-finish fence and must not
	 * replace or veto that canonical local GRD authority. */
	if (prepared_x_source_exact
		&& authenticated_master_node != cluster_node_id
		&& !prepared_terminal_x_source_exact) {
		ResourceXApplyResult result
			= pcm_resource_x_local_owner_valid_locked(
				&entry->resource_x_local_owner)
				? RESOURCE_X_APPLY_STALE
				: RESOURCE_X_APPLY_RECOVERY_BLOCKED;

		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return result;
	}
	if (state->holder_status.valid != 0 || state->holder_image.valid != 0) {
		ResourceXDecodedFrame old_image;
		ResourceXDecodedFrame old_status;
		ResourceXApplyResult result;
		bool exact_existing;
		bool exact_existing_drained;
		bool exact_drop_episode_restart = false;

		exact_existing = state->holder_status.valid
				== state->holder_image.valid
			&& (state->holder_status.valid
					== RESOURCE_X_HOLDER_PAIR_PENDING
				|| state->holder_status.valid
					== RESOURCE_X_HOLDER_PAIR_PUBLISHED)
			&& state->holder_status.payload_bytes
				   == status_record.payload_bytes
			&& state->holder_image.payload_bytes
				   == image_record.payload_bytes
			&& memcmp(state->holder_status.payload,
				status_record.payload, status_record.payload_bytes) == 0
			&& memcmp(state->holder_image.payload,
				image_record.payload, image_record.payload_bytes) == 0;
		if (exact_existing) {
			/* A drained pair is a cleanup tombstone.  Replaying its type-17
			 * cannot resurrect outbound payload ownership or physical PI. */
			exact_existing_drained = state->holder_pair_drained_sequences
						[status_record.body.assertion.requester_node]
					>= status_record.logical_generation
				&& state->holder_pair_drained_resource_formation
					   == status_record.resource_formation
				&& state->holder_pair_drained_master_session
					   == block->common.master_session_incarnation
				&& state->holder_pair_drained_master_node
					   == authenticated_master_node;
			if (!exact_existing_drained && prepared_remote_s_source) {
				result
					= pcm_resource_x_s_predecessor_cancel_unbound_round_locked(
						entry, block, authenticated_master_node, &broadcast);
				if (result != RESOURCE_X_APPLY_APPLIED) {
					LWLockRelease(&entry->entry_lock.lock);
					pcm_entry_ref_release(&entry_ref);
					return result;
				}
			}
			/* Retention is deliberately transport-invisible.  An exact replay
			 * cannot publish a pair before the caller proves the physical revoke
			 * terminal through holder_pair_publish_exact(). */
			result = RESOURCE_X_APPLY_DUPLICATE;
			LWLockRelease(&entry->entry_lock.lock);
			if (broadcast)
				ConditionVariableBroadcast(&entry->wait_cv);
			pcm_entry_ref_release(&entry_ref);
			return result;
		}

		/* One source owns one retained pair at a time.  A newer exact direct-X
		 * carrier may supersede it only after post-settlement DRAIN retired the
		 * old pair and both old outbound intents are empty.  Attempt ordering is
		 * comparable only within one {resource, requester} namespace; a different
		 * requester is ordered here by the exact drain and carrier-generation
		 * fences instead.  The monotone per-requester tombstone remains so a
		 * delayed old DRAIN still replays exactly. */
		result = pcm_resource_x_holder_pair_decode_locked(
			state, &old_status, &old_image);
		local_grd_source = state->authority_generation
				== block->common.base_authority_generation
			&& (PcmState)pg_atomic_read_u32(&entry->master_state)
				   == PCM_STATE_X
			&& entry->x_holder_node == cluster_node_id;
		installed_remote_source = authenticated_master_node != cluster_node_id
			&& entry->resource_x_requester_base_generation >= 1
			&& entry->resource_x_requester_base_generation
				   <= block->common.base_authority_generation
			&& entry->resource_x_retired_acquisition_generation != 0
			&& pcm_resource_x_active_empty_locked(entry)
			&& state->authority_generation
				   <= block->common.base_authority_generation
			&& (PcmState)pg_atomic_read_u32(&entry->master_state)
				   == PCM_STATE_N
			&& entry->x_holder_node == -1
			&& pg_atomic_read_u32(&entry->s_holders_bitmap) == 0;
		if (result == RESOURCE_X_APPLY_APPLIED
			&& prepared_terminal_x_source_exact
			&& required_x_finish_mode == CLUSTER_PCM_X_REVOKE_FINISH_DROP
			&& decoded_image.body.image_envelope.source_carrier_generation
				== old_image.body.image_envelope.source_carrier_generation
			&& resource_x_assertion_equal(
				&block->common.logical_assertion,
				&old_status.common.logical_assertion)
			&& status_record.logical_generation
				> state->holder_status.logical_generation)
			exact_drop_episode_restart = true;
		if (result != RESOURCE_X_APPLY_APPLIED
				|| (!local_grd_source && !installed_remote_source
					&& !prepared_remote_s_source
					&& !prepared_terminal_x_source_exact)
			|| state->holder_status_intent.slot.state
				   != RESOURCE_X_INTENT_SLOT_EMPTY
			|| state->holder_image_intent.state
				   != RESOURCE_X_INTENT_SLOT_EMPTY
			|| state->holder_pair_drained_sequences
					  [state->holder_status.body.assertion.requester_node]
				   < state->holder_status.logical_generation
			|| state->holder_pair_drained_resource_formation
				   != state->holder_status.resource_formation
			|| state->holder_pair_drained_master_session
				   != old_status.common.master_session_incarnation
			|| state->holder_pair_drained_master_node
				   != (int32)state->holder_status.destination_node
			|| block->common.resource_formation
				   != state->holder_pair_drained_resource_formation
			|| block->common.master_session_incarnation
				   != state->holder_pair_drained_master_session
			|| authenticated_master_node
				   != state->holder_pair_drained_master_node
			|| (resource_x_assertion_equal(
					&block->common.logical_assertion,
					&old_status.common.logical_assertion)
				&& status_record.logical_generation
				   <= state->holder_status.logical_generation)
			|| (decoded_image.body.image_envelope.source_carrier_generation
				   <= old_image.body.image_envelope.source_carrier_generation
				&& !exact_drop_episode_restart)) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return result == RESOURCE_X_APPLY_RECOVERY_BLOCKED
				? result : RESOURCE_X_APPLY_STALE;
		}
		if (prepared_remote_s_source) {
			result
				= pcm_resource_x_s_predecessor_cancel_unbound_round_locked(
					entry, block, authenticated_master_node, &broadcast);
			if (result != RESOURCE_X_APPLY_APPLIED) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_entry_ref_release(&entry_ref);
				return result;
			}
		}
		state->holder_status = status_record;
		state->holder_image = image_record;

		LWLockRelease(&entry->entry_lock.lock);
		if (broadcast)
			ConditionVariableBroadcast(&entry->wait_cv);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_APPLIED;
	}
	local_grd_source = state->authority_generation
			== block->common.base_authority_generation
		&& (PcmState)pg_atomic_read_u32(&entry->master_state) == PCM_STATE_X
		&& entry->x_holder_node == cluster_node_id;
	/* A remote Resource-X master is authoritative for the type-17 current-X
	 * selection.  The installed holder's local GRD entry is intentionally a
	 * non-authoritative N mirror; bind it instead to the exact final authority
	 * recorded by the completed requester install.  The caller has already
	 * frozen and encoded the matching BufferDesc X+REVOKING lineage. */
	installed_remote_source = authenticated_master_node != cluster_node_id
		&& entry->resource_x_requester_base_generation >= 1
		&& entry->resource_x_requester_base_generation
			<= block->common.base_authority_generation
		&& entry->resource_x_retired_acquisition_generation != 0
		&& pcm_resource_x_active_empty_locked(entry)
		&& state->authority_generation
			<= block->common.base_authority_generation
		&& (PcmState)pg_atomic_read_u32(&entry->master_state) == PCM_STATE_N
		&& entry->x_holder_node == -1
		&& pg_atomic_read_u32(&entry->s_holders_bitmap) == 0;
	/* PGRAC adaptation for the direct last-current carrier: only a container
	 * that was absent at this call and is still byte-for-byte pristine on all
	 * authority/lifecycle axes may retain the pair.  It remains N/no-holder;
	 * bitmap or historical PI provenance never enters this decision.  Base 1
	 * is the frozen clean-formation slice; later bases require installed-holder
	 * lineage above and fail closed here. */
	pristine_remote_source = !shared_s_source && created_retention_container
		&& authenticated_master_node != cluster_node_id
		&& block->common.base_authority_generation == UINT64_C(1)
		&& state->authority_generation == UINT64_C(1)
		&& state->next_enqueue_order == UINT64_C(1)
		&& entry->resource_x_requester_base_generation == UINT64_C(1)
		&& entry->resource_x_retired_acquisition_generation == 0
		&& pcm_resource_x_active_empty_locked(entry)
		&& pcm_resource_x_requester_join_empty_locked(&state->requester_join)
		&& state->grant_intent.slot.state == RESOURCE_X_INTENT_SLOT_EMPTY
		&& state->requester_settlement_intent.slot.state
			== RESOURCE_X_INTENT_SLOT_EMPTY
		&& (PcmState)pg_atomic_read_u32(&entry->master_state) == PCM_STATE_N
		&& entry->x_holder_node == -1
		&& pg_atomic_read_u32(&entry->s_holders_bitmap) == 0
		&& pg_atomic_read_u32(&entry->pi_holders_bitmap) == 0
		&& pg_atomic_read_u64(&entry->transition_count_local) == 0;
	if ((!local_grd_source && !installed_remote_source
			&& !pristine_remote_source && !prepared_remote_s_source
			&& !prepared_terminal_x_source_exact)
		|| state->holder_status_intent.slot.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY
		|| state->holder_image_intent.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_BAD_STATE;
	}
	if (prepared_remote_s_source) {
		ResourceXApplyResult result
			= pcm_resource_x_s_predecessor_cancel_unbound_round_locked(
				entry, block, authenticated_master_node, &broadcast);

		if (result != RESOURCE_X_APPLY_APPLIED) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return result;
		}
	}
	state->holder_status = status_record;
	state->holder_image = image_record;
	/* A self-master holder shares this GRD entry with the authority state.
	 * Its BufferDesc revoke supplies the local nonwritable fence; preserve
	 * master X until the retained BLOCKED_TO_N is authenticated and applied.
	 * A remote-master holder owns only its local mirror and may retire it now. */
	if (authenticated_master_node != cluster_node_id && local_grd_source)
		cluster_pcm_transition_apply(entry, PCM_TRANS_X_TO_N_DOWNGRADE,
								 cluster_node_id);
	broadcast = true;
	LWLockRelease(&entry->entry_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
return RESOURCE_X_APPLY_APPLIED;
}

/* Classify an exact replay without touching transport state.  APPLIED means
 * the retained pair is still PENDING and needs the physical finish+publish;
 * DUPLICATE means the pair was atomically published already (its two intent
 * slots may now be at different normal DATA-drain positions).  NOT_FOUND
 * means the only retained pair is an exact older attempt in the same domain;
 * the caller may build current frames, but the source replacement contract
 * still requires the predecessor's DRAIN tombstone before publishing them. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_pair_publish_needed_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	int32 authenticated_master_node, uint64 authenticated_master_session)
{
	ClusterPcmResourceXMasterState *state;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXApplyResult result;
	struct GrdEntry *entry;
	uint64 gate_formation;

	if (!resource_x_assertion_valid(assertion) || assertion_sequence == 0
		|| assertion_sequence == UINT64_MAX
		|| authenticated_master_node < 0
		|| authenticated_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0 || ClusterPcm == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto pair_needed_done;
	}
	result = pcm_resource_x_holder_pair_drain_domain_locked(
		&entry->tag, state, authenticated_master_node,
		authenticated_master_session, assertion, assertion_sequence);
	if (result == RESOURCE_X_APPLY_DUPLICATE
		|| result == RESOURCE_X_APPLY_RECOVERY_BLOCKED
		|| result == RESOURCE_X_APPLY_STALE)
		goto pair_needed_done;
	result = pcm_resource_x_holder_pair_decode_locked(state, &status, &image);
	if (result != RESOURCE_X_APPLY_APPLIED)
		goto pair_needed_done;
	gate_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (!resource_x_assertion_equal(
			assertion, &status.common.logical_assertion)
		|| status.common.resource_formation != gate_formation
		|| status.common.master_session_incarnation
			   != authenticated_master_session
		|| state->holder_status.destination_node
			   != (uint32)authenticated_master_node) {
		result = RESOURCE_X_APPLY_STALE;
		goto pair_needed_done;
	}
	if (status.common.assertion_sequence < assertion_sequence) {
		result = RESOURCE_X_APPLY_NOT_FOUND;
		goto pair_needed_done;
	}
	if (status.common.assertion_sequence != assertion_sequence) {
		result = RESOURCE_X_APPLY_STALE;
		goto pair_needed_done;
	}
	if (state->holder_status.valid == RESOURCE_X_HOLDER_PAIR_PUBLISHED)
		result = RESOURCE_X_APPLY_DUPLICATE;
	else if (state->holder_status.valid
			 == RESOURCE_X_HOLDER_PAIR_PENDING
		&& state->holder_status_intent.slot.state
			   == RESOURCE_X_INTENT_SLOT_EMPTY
		&& state->holder_image_intent.state
			   == RESOURCE_X_INTENT_SLOT_EMPTY)
		result = RESOURCE_X_APPLY_APPLIED;
	else
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;

pair_needed_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

/* Make one already-retained type-18/type-15 pair transport-visible only after
 * the holder caller has completed the exact BufferDesc revoke.  The retained
 * records are the PENDING owner; the two existing intent slots are the READY
 * projection.  No wire, authority, or holder registry is created here. */
ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_pair_publish_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	int32 authenticated_master_node, uint64 authenticated_master_session)
{
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	ResourceXApplyResult result;
	struct GrdEntry *entry;
	uint64 gate_formation;
	bool image_ready;
	bool status_ready;

	if (!resource_x_assertion_valid(assertion) || assertion_sequence == 0
		|| assertion_sequence == UINT64_MAX
		|| authenticated_master_node < 0
		|| authenticated_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0 || ClusterPcm == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto pair_publish_done;
	}
	result = pcm_resource_x_holder_pair_drain_domain_locked(
		&entry->tag, state, authenticated_master_node,
		authenticated_master_session, assertion, assertion_sequence);
	if (result == RESOURCE_X_APPLY_DUPLICATE
		|| result == RESOURCE_X_APPLY_RECOVERY_BLOCKED
		|| result == RESOURCE_X_APPLY_STALE)
		goto pair_publish_done;
	result = pcm_resource_x_holder_pair_decode_locked(state, &status, &image);
	if (result != RESOURCE_X_APPLY_APPLIED)
		goto pair_publish_done;
	gate_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (!resource_x_assertion_equal(
			assertion, &status.common.logical_assertion)
		|| status.common.assertion_sequence != assertion_sequence
		|| status.common.resource_formation != gate_formation
		|| status.common.master_session_incarnation
			   != authenticated_master_session
		|| state->holder_status.destination_node
			   != (uint32)authenticated_master_node) {
		result = RESOURCE_X_APPLY_STALE;
		goto pair_publish_done;
	}
	status_ready = state->holder_status_intent.slot.state
		!= RESOURCE_X_INTENT_SLOT_EMPTY;
	image_ready = state->holder_image_intent.state
		!= RESOURCE_X_INTENT_SLOT_EMPTY;
	/* A pair is one transport unit.  Once either exact half has completed,
	 * reconstructing only that missing half would create a new observable pair
	 * episode and could release the source fence on partial evidence. */
	if (status_ready != image_ready) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto pair_publish_done;
	}
	if (state->holder_status.valid == RESOURCE_X_HOLDER_PAIR_PUBLISHED) {
		if (!status_ready
			|| !pcm_resource_x_rearm_holder_source_locked(
				state, &state->holder_status, &state->holder_image))
			result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		else
			result = RESOURCE_X_APPLY_DUPLICATE;
		goto pair_publish_done;
	}
	if (state->holder_status.valid != RESOURCE_X_HOLDER_PAIR_PENDING
		|| status_ready) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto pair_publish_done;
	}
	if (!pcm_resource_x_rearm_holder_source_locked(
			state, &state->holder_status, &state->holder_image)) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto pair_publish_done;
	}
	state->holder_status.valid = RESOURCE_X_HOLDER_PAIR_PUBLISHED;
	state->holder_image.valid = RESOURCE_X_HOLDER_PAIR_PUBLISHED;
	result = RESOURCE_X_APPLY_APPLIED;

pair_publish_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

/* Classify one retained former-holder pair under the resource entry lock.
 * APPLIED means an exact, current, undrained predecessor exists and must
 * settle before a local kind-9 round may be created.  NOT_FOUND means there
 * is no live predecessor (including an exact drained tombstone).  Every
 * malformed or stale present pair stays fail closed.  A nonzero expected
 * carrier generation additionally binds the result to a sampled BufferDesc
 * carrier; zero is used only by the pre-round ordering check. */
static ResourceXApplyResult
pcm_resource_x_holder_pair_precedes_local_bootstrap_locked(
	struct GrdEntry *entry, int32 current_master_node,
	uint64 current_master_session, uint64 current_formation,
	uint64 expected_carrier_generation)
{
	ClusterPcmResourceXMasterState *state;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	ResourceXApplyResult domain;
	ResourceXApplyResult decoded;
	uint64 carrier_generation;
	uint64 source_generation;
	uint8 source_mode;

	Assert(entry != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		|| LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	decoded = pcm_resource_x_holder_pair_decode_locked(
		state, &status, &image);
	if (decoded != RESOURCE_X_APPLY_APPLIED)
		return decoded;
	domain = pcm_resource_x_holder_pair_drain_domain_locked(
		&entry->tag, state, current_master_node,
		current_master_session, &status.common.logical_assertion,
		status.common.assertion_sequence);
	if (domain == RESOURCE_X_APPLY_DUPLICATE)
		return RESOURCE_X_APPLY_NOT_FOUND;
	if (domain != RESOURCE_X_APPLY_NOT_FOUND)
		return domain;
	source_mode = status.common.observed_mode;
	source_generation = pcm_resource_x_source_fence_get_u64(
		image.body.image_envelope.source_fence + 20);
	carrier_generation
		= image.body.image_envelope.source_carrier_generation;
	if (state->holder_status.valid != state->holder_image.valid
		|| (state->holder_status.valid
				!= RESOURCE_X_HOLDER_PAIR_PENDING
			&& state->holder_status.valid
				!= RESOURCE_X_HOLDER_PAIR_PUBLISHED)
		|| state->holder_status.destination_node
			!= (uint32)current_master_node
		|| status.common.resource_formation != current_formation
		|| image.common.resource_formation != current_formation
		|| status.common.master_session_incarnation
			!= current_master_session
		|| image.common.master_session_incarnation
			!= current_master_session
		|| status.common.action_node != cluster_node_id
		|| status.common.target_mode != (uint8)PCM_STATE_N
		|| status.common.flags
			!= RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED
		|| (source_mode != (uint8)PCM_STATE_X
			&& source_mode != (uint8)PCM_STATE_S)
		|| image.common.observed_mode != source_mode
		|| status.body.blocked_to_n.source_fence[28] != source_mode
		|| image.body.image_envelope.source_fence[28] != source_mode
		|| source_generation == 0
		|| source_generation == UINT64_MAX
		|| carrier_generation == 0
		|| carrier_generation == UINT64_MAX
		|| carrier_generation != source_generation + 1
		|| (expected_carrier_generation != 0
			&& carrier_generation != expected_carrier_generation))
		return RESOURCE_X_APPLY_STALE;
	return RESOURCE_X_APPLY_APPLIED;
}

/* The S-predecessor retention linearization point is the only path allowed to
 * erase an ordinary unbound kind-9 round while leaving an exact current pair
 * ahead of it.  Recognize only that post-cancel shape.  This predicate grants
 * no authority and creates no replacement round; it merely prevents the old
 * waiter from turning a successful rollback into terminal STALE. */
static bool
pcm_resource_x_s_predecessor_superseded_unbound_wait_locked(
	struct GrdEntry *entry, const ResourceXAssertion *assertion,
	int32 current_master_node, uint64 current_master_session,
	uint64 current_formation)
{
	ClusterPcmResourceXBootstrapRound *round;
	ClusterPcmResourceXMasterState *state;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;

	Assert(entry != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		|| LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	if (assertion == NULL
		|| assertion->requester_node != cluster_node_id
		|| !BufferTagsEqual(&assertion->resource, &entry->tag))
		return false;
	round = &entry->resource_x_bootstrap_round;
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_EMPTY
		|| round->highest_attempt_floor == 0
		|| round->highest_attempt_floor == UINT64_MAX
		|| !pcm_resource_x_local_owner_valid_locked(
			&entry->resource_x_local_owner)
		|| entry->resource_x_local_owner.state
			!= RESOURCE_X_LOCAL_OWNER_EMPTY
		|| pcm_resource_x_holder_pair_precedes_local_bootstrap_locked(
			entry, current_master_node, current_master_session,
			current_formation, 0) != RESOURCE_X_APPLY_APPLIED)
		return false;
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL
		|| pcm_resource_x_holder_pair_decode_locked(
			state, &status, &image) != RESOURCE_X_APPLY_APPLIED)
		return false;
	return status.common.observed_mode == (uint8)PCM_STATE_S
		&& image.common.observed_mode == (uint8)PCM_STATE_S;
}

/* A former holder's local target acquire may observe the physical N+PI
 * carrier before SourceSettlementV2 releases its exact REVOKING token.  This
 * read-only predicate supplies no authority: it only binds that physical
 * generation to one current, PENDING/PUBLISHED and not-yet-drained retained
 * pair so the caller can wait locally before starting a new bootstrap round. */
static bool
pcm_resource_x_holder_pair_retained_fence_internal(
	const BufferTag *tag, int32 current_master_node,
	uint64 current_master_session, uint64 current_formation,
	uint64 retained_generation)
{
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	bool matches = false;

	if (tag == NULL
		|| current_master_node < 0
		|| current_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| current_master_session == 0
		|| current_master_session == UINT64_MAX
		|| current_formation == 0
		|| current_formation == UINT64_MAX
		|| retained_generation == 0
		|| retained_generation == UINT64_MAX
		|| cluster_node_id < 0
		|| cluster_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| ClusterPcm == NULL)
		return false;
	if (!pcm_entry_ref_acquire(tag, false, &entry_ref, &acquire_result))
		return false;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	matches = pcm_resource_x_holder_pair_precedes_local_bootstrap_locked(
		entry, current_master_node, current_master_session,
		current_formation, retained_generation) == RESOURCE_X_APPLY_APPLIED;

	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return matches;
}

bool
cluster_pcm_lock_resource_x_holder_pair_retained_fence_exact(
	const BufferTag *tag, int32 current_master_node,
	uint64 current_master_session, uint64 current_formation,
	uint64 retained_generation)
{
	return pcm_resource_x_holder_pair_retained_fence_internal(
		tag, current_master_node, current_master_session,
		current_formation, retained_generation);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_status_intent_snapshot_exact(
	const ResourceXAssertion *assertion, ResourceXIntentSlot *slot_out,
	void *payload_out, uint16 payload_capacity)
{
	ClusterPcmResourceXControlIntent *intent;
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXApplyResult result;
	struct GrdEntry *entry;

	if (slot_out != NULL)
		memset(slot_out, 0, sizeof(*slot_out));
	if (!resource_x_assertion_valid(assertion) || slot_out == NULL
		|| payload_out == NULL
		|| payload_capacity < RESOURCE_X_CONTROL_V1_BYTES)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto holder_status_intent_done;
	}
	intent = &state->holder_status_intent;
	if (intent->slot.state == RESOURCE_X_INTENT_SLOT_EMPTY) {
		result = RESOURCE_X_APPLY_NOT_FOUND;
		goto holder_status_intent_done;
	}
	if (!resource_x_assertion_equal(assertion,
								&intent->slot.body.assertion)) {
		result = RESOURCE_X_APPLY_STALE;
		goto holder_status_intent_done;
	}
	if (intent->slot.state > RESOURCE_X_INTENT_SLOT_STAGED
		|| !pcm_resource_x_intent_body_valid(&intent->slot.body)
		|| (intent->slot.payload_bytes == RESOURCE_X_PROOF_V1_BYTES
			&& state->holder_status.valid
				   != RESOURCE_X_HOLDER_PAIR_PUBLISHED)
		|| intent->slot.payload_bytes > payload_capacity
		|| (intent->slot.payload_bytes != RESOURCE_X_CONTROL_V1_BYTES
			&& intent->slot.payload_bytes != RESOURCE_X_PROOF_V1_BYTES)
		|| intent->slot.kind != RESOURCE_X_WIRE_BLOCKED_TO_N
		|| intent->slot.body.owner_kind
			   != RESOURCE_X_INTENT_OWNER_HOLDER_STATUS
		|| intent->slot.body.owner_node
			   >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| intent->slot.body.owner_index != 0) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto holder_status_intent_done;
	}
	*slot_out = intent->slot;
	memcpy(payload_out, intent->payload, intent->slot.payload_bytes);
	result = RESOURCE_X_APPLY_APPLIED;

holder_status_intent_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_block_to_n_source_exact(
	const ResourceXDecodedFrame *block, int32 authenticated_master_node,
	const ResourceXDecodedFrame *blocked_status,
	const ResourceXDecodedFrame *image_envelope)
{
	return pcm_resource_x_block_to_n_source_exact_internal(
		block, authenticated_master_node, blocked_status, image_envelope,
		NULL, InvalidXLogRecPtr, 0, 0, NULL, NULL,
		CLUSTER_PCM_X_REVOKE_FINISH_INVALID);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_block_to_n_drop_x_source_exact(
	const ResourceXDecodedFrame *block, int32 authenticated_master_node,
	const ResourceXDecodedFrame *blocked_status,
	const ResourceXDecodedFrame *image_envelope,
	const ClusterPcmOwnSnapshot *revoking,
	const ResourceXLocalOwnerHandle *owner)
{
	if (revoking == NULL || owner == NULL)
		return RESOURCE_X_APPLY_INVALID;
	return pcm_resource_x_block_to_n_source_exact_internal(
		block, authenticated_master_node, blocked_status, image_envelope,
		NULL, InvalidXLogRecPtr, 0, 0, revoking, owner,
		CLUSTER_PCM_X_REVOKE_FINISH_DROP);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_block_to_n_prepared_x_source_exact(
	const ResourceXDecodedFrame *block, int32 authenticated_master_node,
	const ResourceXDecodedFrame *blocked_status,
	const ResourceXDecodedFrame *image_envelope,
	const ClusterPcmOwnSnapshot *revoking,
	const ResourceXLocalOwnerHandle *owner)
{
	if (block == NULL || revoking == NULL || owner == NULL
		|| block->common.observed_mode != (uint8)PCM_STATE_X)
		return RESOURCE_X_APPLY_INVALID;
	return pcm_resource_x_block_to_n_source_exact_internal(
		block, authenticated_master_node, blocked_status, image_envelope,
		NULL, InvalidXLogRecPtr, 0, 0, revoking, owner,
		CLUSTER_PCM_X_REVOKE_FINISH_RETAIN);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_block_to_n_prepared_s_source_exact(
	const ResourceXDecodedFrame *block, int32 authenticated_master_node,
	const ResourceXDecodedFrame *blocked_status,
	const ResourceXDecodedFrame *image_envelope,
	const ClusterPcmOwnSnapshot *prepared_source,
	XLogRecPtr prepared_page_lsn, uint64 prepared_page_scn,
	uint32 prepared_page_checksum)
{
	if (block == NULL
		|| block->common.observed_mode != (uint8)PCM_STATE_S)
		return RESOURCE_X_APPLY_INVALID;
	return pcm_resource_x_block_to_n_source_exact_internal(
		block, authenticated_master_node, blocked_status, image_envelope,
		prepared_source, prepared_page_lsn, prepared_page_scn,
		prepared_page_checksum, NULL, NULL,
		CLUSTER_PCM_X_REVOKE_FINISH_INVALID);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_image_intent_snapshot_exact(
	const ResourceXAssertion *assertion, ResourceXIntentSlot *slot_out,
	void *payload_out, uint16 payload_capacity)
{
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXIntentSlot *intent;
	ResourceXApplyResult result;
	struct GrdEntry *entry;

	if (slot_out != NULL)
		memset(slot_out, 0, sizeof(*slot_out));
	if (!resource_x_assertion_valid(assertion) || slot_out == NULL
		|| payload_out == NULL
		|| payload_capacity < RESOURCE_X_IMAGE_V1_BYTES)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto holder_image_intent_done;
	}
	intent = &state->holder_image_intent;
	if (intent->state == RESOURCE_X_INTENT_SLOT_EMPTY) {
		result = RESOURCE_X_APPLY_NOT_FOUND;
		goto holder_image_intent_done;
	}
	if (!resource_x_assertion_equal(assertion, &intent->body.assertion)) {
		result = RESOURCE_X_APPLY_STALE;
		goto holder_image_intent_done;
	}
	if (intent->state > RESOURCE_X_INTENT_SLOT_STAGED
		|| !pcm_resource_x_intent_body_valid(&intent->body)
		|| intent->payload_bytes != RESOURCE_X_IMAGE_V1_BYTES
		|| intent->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE
		|| intent->body.owner_kind != RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE
		|| intent->body.owner_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| intent->body.owner_index != 0
		|| state->holder_image.valid
			   != RESOURCE_X_HOLDER_PAIR_PUBLISHED
		|| state->holder_image.payload_bytes != intent->payload_bytes
		|| state->holder_image.logical_generation
			!= intent->logical_generation
		|| state->holder_image.authority_generation
			!= intent->authority_generation) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto holder_image_intent_done;
	}
	*slot_out = *intent;
	memcpy(payload_out, state->holder_image.payload,
		   intent->payload_bytes);
	result = RESOURCE_X_APPLY_APPLIED;

holder_image_intent_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_status_exact(
	const ResourceXAssertion *assertion, ResourceXDecodedFrame *out)
{
	ClusterPcmResourceXHolderStatus *record;
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXApplyResult result;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	struct GrdEntry *entry;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (!resource_x_assertion_valid(assertion) || out == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto holder_status_done;
	}
	record = &state->holder_status;
	if (record->valid == 0) {
		result = RESOURCE_X_APPLY_NOT_FOUND;
		goto holder_status_done;
	}
	if (!resource_x_assertion_equal(assertion, &record->body.assertion)) {
		result = RESOURCE_X_APPLY_STALE;
		goto holder_status_done;
	}
	if ((record->valid != RESOURCE_X_HOLDER_PAIR_PENDING
		 && record->valid != RESOURCE_X_HOLDER_PAIR_PUBLISHED)
		|| record->body.owner_kind
			   != RESOURCE_X_INTENT_OWNER_HOLDER_STATUS
		|| record->body.owner_index != 0
		|| record->body.owner_generation != record->logical_generation
		|| record->resource_formation == 0
		|| record->payload_bytes < RESOURCE_X_CONTROL_V1_BYTES
		|| record->payload_bytes > RESOURCE_X_PROOF_V1_BYTES
		|| record->kind != RESOURCE_X_WIRE_BLOCKED_TO_N
		|| !cluster_resource_x_wire_decode(
			RESOURCE_X_MSG_BLOCKED_TO_N, record->payload,
			record->payload_bytes, out, &reject)) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto holder_status_done;
	}
	if (out->common.resource_formation != record->resource_formation) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto holder_status_done;
	}
	result = RESOURCE_X_APPLY_APPLIED;

holder_status_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_image_exact(
	const ResourceXAssertion *assertion, ResourceXDecodedFrame *out)
{
	ClusterPcmResourceXHolderImage *record;
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXApplyResult result;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	struct GrdEntry *entry;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (!resource_x_assertion_valid(assertion) || out == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto holder_image_done;
	}
	record = &state->holder_image;
	if (record->valid == 0) {
		result = RESOURCE_X_APPLY_NOT_FOUND;
		goto holder_image_done;
	}
	if (!resource_x_assertion_equal(assertion, &record->body.assertion)) {
		result = RESOURCE_X_APPLY_STALE;
		goto holder_image_done;
	}
	if ((record->valid != RESOURCE_X_HOLDER_PAIR_PENDING
		 && record->valid != RESOURCE_X_HOLDER_PAIR_PUBLISHED)
		|| record->payload_bytes != RESOURCE_X_IMAGE_V1_BYTES
		|| record->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE
		|| record->logical_generation == 0
		|| record->authority_generation == 0
		|| record->resource_formation == 0
		|| record->destination_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| record->body.owner_kind != RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE
		|| record->body.owner_index != 0
		|| record->body.owner_generation != record->logical_generation
		|| !cluster_resource_x_wire_decode(
			RESOURCE_X_MSG_IMAGE_OR_GRANT, record->payload,
			record->payload_bytes, out, &reject)
		|| out->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE
		|| !resource_x_assertion_equal(
			assertion, &out->common.logical_assertion)
		|| out->common.resource_formation != record->resource_formation) {
		memset(out, 0, sizeof(*out));
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto holder_image_done;
	}
	result = RESOURCE_X_APPLY_APPLIED;

holder_image_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

/* Decode the retained type-18/type-15 pair under the owning GrdEntry lock.
 * Neither record is independently sufficient: every lineage field must
 * agree before a DRAIN may touch the physical N+PI carrier. */
static ResourceXApplyResult
pcm_resource_x_holder_pair_decode_locked(
	const ClusterPcmResourceXMasterState *state,
	ResourceXDecodedFrame *status_out, ResourceXDecodedFrame *image_out)
{
	const ClusterPcmResourceXHolderStatus *status_record;
	const ClusterPcmResourceXHolderImage *image_record;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;

	Assert(state != NULL);
	Assert(status_out != NULL);
	Assert(image_out != NULL);
	memset(status_out, 0, sizeof(*status_out));
	memset(image_out, 0, sizeof(*image_out));
	status_record = &state->holder_status;
	image_record = &state->holder_image;
	if (status_record->valid == 0 && image_record->valid == 0)
		return RESOURCE_X_APPLY_NOT_FOUND;
	if (status_record->valid != image_record->valid
		|| (status_record->valid != RESOURCE_X_HOLDER_PAIR_PENDING
			&& status_record->valid != RESOURCE_X_HOLDER_PAIR_PUBLISHED)
		|| status_record->payload_bytes != RESOURCE_X_PROOF_V1_BYTES
		|| image_record->payload_bytes != RESOURCE_X_IMAGE_V1_BYTES
		|| status_record->kind != RESOURCE_X_WIRE_BLOCKED_TO_N
		|| image_record->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE
		|| status_record->logical_generation == 0
		|| status_record->logical_generation != image_record->logical_generation
		|| status_record->resource_formation == 0
		|| status_record->resource_formation != image_record->resource_formation
		|| status_record->destination_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| image_record->destination_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| status_record->body.owner_kind
			   != RESOURCE_X_INTENT_OWNER_HOLDER_STATUS
		|| image_record->body.owner_kind
			   != RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE
		|| status_record->body.owner_index != 0
		|| image_record->body.owner_index != 0
		|| status_record->body.owner_generation
			   != status_record->logical_generation
		|| image_record->body.owner_generation
			   != image_record->logical_generation
		|| status_record->body.owner_node != image_record->body.owner_node
		|| !resource_x_assertion_equal(&status_record->body.assertion,
			&image_record->body.assertion)
		|| !cluster_resource_x_wire_decode(
			RESOURCE_X_MSG_BLOCKED_TO_N, status_record->payload,
			status_record->payload_bytes, status_out, &reject))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	reject = RESOURCE_X_WIRE_REJECT_NONE;
	if (!cluster_resource_x_wire_decode(
			RESOURCE_X_MSG_IMAGE_OR_GRANT, image_record->payload,
			image_record->payload_bytes, image_out, &reject)
		|| status_out->kind != RESOURCE_X_WIRE_BLOCKED_TO_N
		|| !status_out->blocked_has_remote_proof
		|| image_out->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE
		|| !resource_x_assertion_equal(
			&status_record->body.assertion,
			&status_out->common.logical_assertion)
		|| !resource_x_assertion_equal(
			&status_record->body.assertion,
			&image_out->common.logical_assertion)
		|| status_out->common.assertion_sequence
			   != status_record->logical_generation
		|| image_out->common.assertion_sequence
			   != image_record->logical_generation
		|| status_out->common.resource_formation
			   != status_record->resource_formation
		|| image_out->common.resource_formation
			   != image_record->resource_formation
		|| status_out->common.master_session_incarnation
			   != image_out->common.master_session_incarnation
		|| status_out->common.base_authority_generation
			   != image_out->common.base_authority_generation
		|| status_out->common.authority_generation
			   != status_record->authority_generation
		|| image_out->common.authority_generation
			   != image_record->authority_generation
		|| status_out->common.authority_generation
			   != status_out->common.base_authority_generation
		|| image_out->common.authority_generation
			   != status_out->common.base_authority_generation + 1
		|| status_out->common.action_node
			   != (int32)status_record->body.owner_node
		|| image_out->common.action_node
			   != status_out->common.logical_assertion.requester_node
		/* The authenticated master and requester are distinct roles, not
		 * necessarily distinct nodes.  Their independent destination checks
		 * below remain exact when a local master is also the requester. */
		|| image_record->destination_node
			   != (uint32)status_out->common.logical_assertion.requester_node
		|| (status_out->common.observed_mode != (uint8)PCM_STATE_X
			&& status_out->common.observed_mode != (uint8)PCM_STATE_S)
		|| status_out->common.target_mode != (uint8)PCM_STATE_N
		|| status_out->common.outcome != RESOURCE_X_OUTCOME_OK
		|| status_out->common.flags
			   != RESOURCE_X_COMMON_FLAG_PI_ESTABLISHED
		|| image_out->common.observed_mode
			   != status_out->common.observed_mode
		|| image_out->common.target_mode != (uint8)PCM_STATE_X
		|| image_out->common.outcome != RESOURCE_X_OUTCOME_OK
		|| image_out->body.image_envelope.source_fence[28]
			   != status_out->common.observed_mode
		|| status_out->body.blocked_to_n.source_carrier_generation == 0
		|| status_out->body.blocked_to_n.source_carrier_generation
			   != image_out->body.image_envelope.source_carrier_generation
		|| status_out->body.blocked_to_n.requester_target_generation
			   != image_out->body.image_envelope.requester_target_generation
		|| status_out->body.blocked_to_n.page_scn_lsn
			   != image_out->body.image_envelope.page_scn_lsn
		|| status_out->body.blocked_to_n.dependency_count
			   != image_out->body.image_envelope.dependency_count
		|| memcmp(status_out->body.blocked_to_n.source_fence,
			image_out->body.image_envelope.source_fence,
			sizeof(status_out->body.blocked_to_n.source_fence)) != 0
		|| memcmp(status_out->body.blocked_to_n.dependencies,
			image_out->body.image_envelope.dependencies,
			sizeof(status_out->body.blocked_to_n.dependencies)) != 0
		|| status_out->body.blocked_to_n.source_proof_crc32c
			   != image_out->common.semantic_crc32c
		|| status_out->body.blocked_to_n.page_checksum
			   != image_out->body.image_envelope.page_checksum
		|| status_out->body.blocked_to_n.proof_kind
			   != RESOURCE_X_PROOF_REMOTE_CARRIER
		|| image_out->body.image_envelope.proof_kind
			   != RESOURCE_X_PROOF_REMOTE_CARRIER
		|| status_out->body.blocked_to_n.source_disposition
			   != RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE
		|| image_out->body.image_envelope.source_disposition
			   != RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	return RESOURCE_X_APPLY_APPLIED;
}

static ResourceXApplyResult
pcm_resource_x_holder_pair_drain_domain_locked(
	const BufferTag *tag, const ClusterPcmResourceXMasterState *state,
	int32 authenticated_master_node, uint64 authenticated_master_session,
	const ResourceXAssertion *assertion, uint64 assertion_sequence)
{
	uint64 gate_formation;

	Assert(tag != NULL);
	Assert(state != NULL);
	Assert(assertion != NULL);
	gate_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_OPEN
		|| cluster_gcs_lookup_master(*tag) != authenticated_master_node)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (state->holder_pair_drained_sequences[assertion->requester_node]
			>= assertion_sequence
		&& state->holder_pair_drained_sequences[assertion->requester_node]
			   != 0) {
		if (state->holder_pair_drained_resource_formation != gate_formation
			|| state->holder_pair_drained_master_session
				   != authenticated_master_session
			|| state->holder_pair_drained_master_node
				   != authenticated_master_node
			|| state->holder_pair_drained_reserved != 0)
			return RESOURCE_X_APPLY_STALE;
		return RESOURCE_X_APPLY_DUPLICATE;
	}
	return RESOURCE_X_APPLY_NOT_FOUND;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_pair_supersedes_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	int32 authenticated_master_node, uint64 authenticated_master_session)
{
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	ResourceXApplyResult result;
	struct GrdEntry *entry;
	uint64 gate_formation;

	if (!resource_x_assertion_valid(assertion) || assertion_sequence == 0
		|| assertion_sequence == UINT64_MAX
		|| authenticated_master_node < 0
		|| authenticated_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0 || ClusterPcm == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto pair_supersedes_done;
	}
	result = pcm_resource_x_holder_pair_decode_locked(state, &status, &image);
	if (result != RESOURCE_X_APPLY_APPLIED)
		goto pair_supersedes_done;
	gate_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_OPEN
		|| cluster_gcs_lookup_master(entry->tag)
			!= authenticated_master_node) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto pair_supersedes_done;
	}
	if (!resource_x_assertion_equal(
			assertion, &status.common.logical_assertion)
		|| status.common.assertion_sequence <= assertion_sequence
		|| status.common.resource_formation != gate_formation
		|| status.common.master_session_incarnation
			   != authenticated_master_session
		|| state->holder_status.destination_node
			   != (uint32)authenticated_master_node)
		result = RESOURCE_X_APPLY_STALE;
	else
		result = RESOURCE_X_APPLY_APPLIED;

pair_supersedes_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_pair_drain_prepare_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	int32 authenticated_master_node, uint64 authenticated_master_session,
	uint64 *source_generation_out)
{
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	ResourceXApplyResult result;
	struct GrdEntry *entry;
	uint64 gate_formation;

	if (source_generation_out != NULL)
		*source_generation_out = 0;
	if (!resource_x_assertion_valid(assertion) || assertion_sequence == 0
		|| assertion_sequence == UINT64_MAX
		|| authenticated_master_node < 0
		|| authenticated_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0 || source_generation_out == NULL
		|| ClusterPcm == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto pair_prepare_done;
	}
	result = pcm_resource_x_holder_pair_drain_domain_locked(
		&entry->tag, state, authenticated_master_node,
		authenticated_master_session, assertion, assertion_sequence);
	if (result == RESOURCE_X_APPLY_DUPLICATE
		|| result == RESOURCE_X_APPLY_RECOVERY_BLOCKED
		|| result == RESOURCE_X_APPLY_STALE)
		goto pair_prepare_done;
	result = pcm_resource_x_holder_pair_decode_locked(state, &status, &image);
	if (result != RESOURCE_X_APPLY_APPLIED)
		goto pair_prepare_done;
	if (!resource_x_assertion_equal(
			assertion, &status.common.logical_assertion)
		|| status.common.assertion_sequence != assertion_sequence) {
		result = RESOURCE_X_APPLY_NOT_FOUND;
		goto pair_prepare_done;
	}
	gate_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (status.common.resource_formation != gate_formation
		|| status.common.master_session_incarnation
			   != authenticated_master_session
		|| state->holder_status.destination_node
			   != (uint32)authenticated_master_node) {
		result = RESOURCE_X_APPLY_STALE;
		goto pair_prepare_done;
	}
	if (state->holder_status.valid != RESOURCE_X_HOLDER_PAIR_PUBLISHED
		|| state->holder_image.valid
			   != RESOURCE_X_HOLDER_PAIR_PUBLISHED) {
		result = RESOURCE_X_APPLY_BAD_STATE;
		goto pair_prepare_done;
	}
	if (state->holder_status_intent.slot.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY
		|| state->holder_image_intent.state
			   != RESOURCE_X_INTENT_SLOT_EMPTY) {
		result = RESOURCE_X_APPLY_BAD_STATE;
		goto pair_prepare_done;
	}
	*source_generation_out
		= image.body.image_envelope.source_carrier_generation - 1;
	result = RESOURCE_X_APPLY_APPLIED;

pair_prepare_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_holder_pair_drain_commit_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	int32 authenticated_master_node, uint64 authenticated_master_session,
	uint64 source_generation)
{
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	ResourceXApplyResult result;
	struct GrdEntry *entry;
	uint64 gate_formation;

	if (!resource_x_assertion_valid(assertion) || assertion_sequence == 0
		|| assertion_sequence == UINT64_MAX
		|| authenticated_master_node < 0
		|| authenticated_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| authenticated_master_session == 0 || source_generation == UINT64_MAX
		|| ClusterPcm == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto pair_commit_done;
	}
	result = pcm_resource_x_holder_pair_drain_domain_locked(
		&entry->tag, state, authenticated_master_node,
		authenticated_master_session, assertion, assertion_sequence);
	if (result == RESOURCE_X_APPLY_DUPLICATE
		|| result == RESOURCE_X_APPLY_RECOVERY_BLOCKED
		|| result == RESOURCE_X_APPLY_STALE)
		goto pair_commit_done;
	result = pcm_resource_x_holder_pair_decode_locked(state, &status, &image);
	if (result != RESOURCE_X_APPLY_APPLIED)
		goto pair_commit_done;
	gate_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (!resource_x_assertion_equal(
			assertion, &status.common.logical_assertion)
		|| status.common.assertion_sequence != assertion_sequence
		|| status.common.resource_formation != gate_formation
		|| status.common.master_session_incarnation
			   != authenticated_master_session
		|| state->holder_status.destination_node
			   != (uint32)authenticated_master_node
		|| image.body.image_envelope.source_carrier_generation
			   != source_generation + 1) {
		result = RESOURCE_X_APPLY_STALE;
		goto pair_commit_done;
	}
	if (state->holder_status.valid != RESOURCE_X_HOLDER_PAIR_PUBLISHED
		|| state->holder_image.valid
			   != RESOURCE_X_HOLDER_PAIR_PUBLISHED) {
		result = RESOURCE_X_APPLY_BAD_STATE;
		goto pair_commit_done;
	}
	if (state->holder_status_intent.slot.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY
		|| state->holder_image_intent.state
			   != RESOURCE_X_INTENT_SLOT_EMPTY) {
		result = RESOURCE_X_APPLY_BAD_STATE;
		goto pair_commit_done;
	}
	state->holder_pair_drained_sequences[assertion->requester_node]
		= assertion_sequence;
	state->holder_pair_drained_resource_formation = gate_formation;
	state->holder_pair_drained_master_session
		= authenticated_master_session;
	state->holder_pair_drained_master_node = authenticated_master_node;
	state->holder_pair_drained_reserved = 0;
	result = RESOURCE_X_APPLY_APPLIED;

pair_commit_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

static bool
pcm_resource_x_source_settlement_matches_pair(
	const ResourceXDecodedFrame *settlement,
	const ResourceXDecodedFrame *status,
	const ResourceXDecodedFrame *image)
{
	const ResourceXDecodedBlockedToN *left;
	const ResourceXDecodedBlockedToN *right;

	if (settlement == NULL || status == NULL || image == NULL
		|| settlement->kind != RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
		|| !settlement->blocked_has_remote_proof
		|| status->kind != RESOURCE_X_WIRE_BLOCKED_TO_N
		|| !status->blocked_has_remote_proof
		|| image->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE
		|| !resource_x_assertion_equal(
			&settlement->common.logical_assertion,
			&status->common.logical_assertion)
		|| !resource_x_assertion_equal(
			&settlement->common.logical_assertion,
			&image->common.logical_assertion)
		|| settlement->common.base_authority_generation
			!= status->common.base_authority_generation
		|| settlement->common.resource_formation
			!= status->common.resource_formation
		|| settlement->common.master_session_incarnation
			!= status->common.master_session_incarnation
		|| settlement->common.assertion_sequence
			!= status->common.assertion_sequence
		|| settlement->common.ordered_lane != 0
		|| status->common.ordered_lane != 0
		|| image->common.ordered_lane != 0
		|| settlement->common.action_node != status->common.action_node
		|| settlement->common.observed_mode != (uint8)PCM_STATE_N
		|| settlement->common.target_mode != (uint8)PCM_STATE_N
		|| settlement->common.source_candidate != 1
		|| settlement->common.retain_pi_if_dirty != 1
		|| settlement->common.sender_connection_generation == 0
		|| settlement->common.outcome != RESOURCE_X_OUTCOME_NONE
		|| settlement->common.flags != 0
		|| image->common.authority_generation == UINT64_MAX
		|| settlement->common.authority_generation == UINT64_MAX
		|| settlement->common.authority_generation
			<= image->common.authority_generation)
		return false;
	left = &settlement->body.blocked_to_n;
	right = &status->body.blocked_to_n;
	return left->source_carrier_generation
			   == right->source_carrier_generation
		&& left->requester_target_generation
			   == right->requester_target_generation
		&& left->page_scn_lsn == right->page_scn_lsn
		&& left->dependency_count == right->dependency_count
		&& left->source_proof_crc32c == right->source_proof_crc32c
		&& left->page_checksum == right->page_checksum
		&& left->source_disposition == right->source_disposition
		&& left->proof_kind == right->proof_kind
		&& left->proof_flags == right->proof_flags
		&& left->holder_connection_generation
			   == right->holder_connection_generation
		&& left->acting_formation == right->acting_formation
		&& memcmp(left->source_fence, right->source_fence,
			sizeof(left->source_fence)) == 0
		&& memcmp(left->dependencies, right->dependencies,
			sizeof(left->dependencies)) == 0;
}

static uint64
pcm_resource_x_source_fence_get_u64(const uint8 *bytes)
{
	uint64 value = 0;
	int i;

	Assert(bytes != NULL);
	for (i = 0; i < 8; i++)
		value = (value << 8) | bytes[i];
	return value;
}

static bool
pcm_resource_x_source_settlement_plan_valid(
	const ResourceXSourceSettlementPlan *plan)
{
	ResourceXAcquisitionRef empty_ref;

	if (plan == NULL || !plan->valid || plan->reserved != 0
		|| !resource_x_assertion_valid(&plan->assertion)
		|| plan->resource_formation == 0
		|| plan->resource_formation == UINT64_MAX
		|| plan->master_session_incarnation == 0
		|| plan->master_session_incarnation == UINT64_MAX
		|| plan->assertion_sequence == 0
		|| plan->assertion_sequence == UINT64_MAX
		|| plan->pair_base_authority_generation == 0
		|| plan->pair_base_authority_generation == UINT64_MAX
		|| plan->pair_image_authority_generation == 0
		|| plan->pair_image_authority_generation == UINT64_MAX
		|| plan->settlement_authority_generation == 0
		|| plan->settlement_authority_generation == UINT64_MAX
		|| plan->settlement_authority_generation
			<= plan->pair_image_authority_generation
		|| plan->source_generation == 0
		|| plan->source_generation == UINT64_MAX
		|| plan->carrier_generation == 0
		|| plan->carrier_generation == UINT64_MAX
		|| plan->carrier_generation != plan->source_generation + 1
		|| plan->status_semantic_crc32c == 0
		|| plan->image_semantic_crc32c == 0
		|| plan->authenticated_master_node < 0
		|| plan->authenticated_master_node
			>= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| (plan->source_mode != (uint8)PCM_STATE_X
			&& plan->source_mode != (uint8)PCM_STATE_S)
		|| (plan->cover_action
			!= RESOURCE_X_SETTLEMENT_COVER_NO_COVER
			&& plan->cover_action
				!= RESOURCE_X_SETTLEMENT_COVER_CLOSE_EXACT_X))
		return false;
	memset(&empty_ref, 0, sizeof(empty_ref));
	if (plan->cover_action == RESOURCE_X_SETTLEMENT_COVER_NO_COVER)
		return plan->terminal_cached_generation == 0
			&& plan->terminal_authority_generation == 0
			&& plan->terminal_r4_record_generation == 0
			&& memcmp(&plan->terminal_ref, &empty_ref,
				sizeof(empty_ref)) == 0;
	return plan->source_mode == (uint8)PCM_STATE_X
		&& pcm_resource_x_ref_valid(&plan->terminal_ref)
		&& plan->terminal_cached_generation == plan->source_generation
		&& plan->terminal_authority_generation != 0
		&& plan->terminal_authority_generation != UINT64_MAX
		&& plan->terminal_r4_record_generation != 0
		&& plan->terminal_r4_record_generation != UINT64_MAX;
}

static bool
pcm_resource_x_source_settlement_plan_equal(
	const ResourceXSourceSettlementPlan *left,
	const ResourceXSourceSettlementPlan *right)
{
	if (!pcm_resource_x_source_settlement_plan_valid(left)
		|| !pcm_resource_x_source_settlement_plan_valid(right)
		|| !resource_x_assertion_equal(&left->assertion, &right->assertion)
		|| left->resource_formation != right->resource_formation
		|| left->master_session_incarnation
			!= right->master_session_incarnation
		|| left->assertion_sequence != right->assertion_sequence
		|| left->pair_base_authority_generation
			!= right->pair_base_authority_generation
		|| left->pair_image_authority_generation
			!= right->pair_image_authority_generation
		|| left->settlement_authority_generation
			!= right->settlement_authority_generation
		|| left->source_generation != right->source_generation
		|| left->carrier_generation != right->carrier_generation
		|| left->status_semantic_crc32c != right->status_semantic_crc32c
		|| left->image_semantic_crc32c != right->image_semantic_crc32c
		|| left->authenticated_master_node
			!= right->authenticated_master_node
		|| left->source_mode != right->source_mode
		|| left->cover_action != right->cover_action)
		return false;
	if (left->cover_action == RESOURCE_X_SETTLEMENT_COVER_NO_COVER)
		return true;
	return pcm_resource_x_acquisition_ref_equal(
			&left->terminal_ref, &right->terminal_ref)
		&& left->terminal_cached_generation
			== right->terminal_cached_generation
		&& left->terminal_authority_generation
			== right->terminal_authority_generation
		&& left->terminal_r4_record_generation
			== right->terminal_r4_record_generation;
}

static void
pcm_resource_x_source_settlement_observe_round_locked(
	struct GrdEntry *entry,
	ResourceXSourceSettlementCommitObservation *observation)
{
	ClusterPcmResourceXBootstrapRound *round;
	ClusterPcmResourceXLocalOwner *owner;

	Assert(entry != NULL);
	Assert(observation != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		|| LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	round = &entry->resource_x_bootstrap_round;
	owner = &entry->resource_x_local_owner;
	observation->current_round_phase = round->phase;
	observation->current_owner_state = owner->state;
	observation->current_master_node = round->current_master_node;
	observation->current_master_session
		= round->master_session_incarnation;
	observation->current_terminal_authority_generation
		= round->terminal_authority_generation;
	observation->current_cached_ownership_generation
		= round->cached_ownership_generation;
	observation->current_terminal_cover_present
		= round->phase == RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED;
}

static ResourceXApplyResult
pcm_resource_x_source_settlement_plan_build_locked(
	struct GrdEntry *entry, ClusterPcmResourceXMasterState *state,
	const ResourceXDecodedFrame *settlement,
	int32 authenticated_master_node,
	const ResourceXDecodedFrame *status,
	const ResourceXDecodedFrame *image,
	ResourceXSourceSettlementPlan *plan_out)
{
	ClusterPcmResourceXBootstrapRound *round;
	ClusterPcmResourceXLocalOwner *owner;
	ResourceXSourceSettlementPlan candidate;
	uint64 carrier_generation;
	uint64 source_generation;
	uint8 source_mode;

	Assert(entry != NULL);
	Assert(state != NULL);
	Assert(settlement != NULL);
	Assert(status != NULL);
	Assert(image != NULL);
	Assert(plan_out != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		|| LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	memset(plan_out, 0, sizeof(*plan_out));
	memset(&candidate, 0, sizeof(candidate));
	if (!pcm_resource_x_source_settlement_matches_pair(
			settlement, status, image)
		|| state->holder_status.destination_node
			!= (uint32)authenticated_master_node)
		return RESOURCE_X_APPLY_STALE;
	if (state->holder_status.valid != RESOURCE_X_HOLDER_PAIR_PUBLISHED
		|| state->holder_image.valid != RESOURCE_X_HOLDER_PAIR_PUBLISHED
		|| state->holder_status_intent.slot.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY
		|| state->holder_image_intent.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY)
		return RESOURCE_X_APPLY_BAD_STATE;
	if (!cluster_pcm_lock_resource_x_gate_open_exact(
			settlement->common.resource_formation))
		return RESOURCE_X_APPLY_STALE;
	source_mode = status->common.observed_mode;
	source_generation = pcm_resource_x_source_fence_get_u64(
		image->body.image_envelope.source_fence + 20);
	carrier_generation
		= image->body.image_envelope.source_carrier_generation;
	if ((source_mode != (uint8)PCM_STATE_X
			&& source_mode != (uint8)PCM_STATE_S)
		|| image->common.observed_mode != source_mode
		|| status->body.blocked_to_n.source_fence[28] != source_mode
		|| image->body.image_envelope.source_fence[28] != source_mode
		|| status->body.blocked_to_n.source_fence[29] != 1
		|| image->body.image_envelope.source_fence[29] != 1)
		return RESOURCE_X_APPLY_INVALID;
	if (source_generation == 0 || source_generation == UINT64_MAX
		|| carrier_generation == 0 || carrier_generation == UINT64_MAX
		|| carrier_generation != source_generation + 1)
		return RESOURCE_X_APPLY_INVALID;
	if (status->common.semantic_crc32c == 0
		|| image->common.semantic_crc32c == 0)
		return RESOURCE_X_APPLY_INVALID;
	owner = &entry->resource_x_local_owner;
	if (!pcm_resource_x_local_owner_valid_locked(owner))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (owner->state != RESOURCE_X_LOCAL_OWNER_EMPTY)
		return RESOURCE_X_APPLY_BAD_STATE;

	candidate.assertion = settlement->common.logical_assertion;
	candidate.resource_formation
		= settlement->common.resource_formation;
	candidate.master_session_incarnation
		= settlement->common.master_session_incarnation;
	candidate.assertion_sequence
		= settlement->common.assertion_sequence;
	candidate.pair_base_authority_generation
		= status->common.base_authority_generation;
	candidate.pair_image_authority_generation
		= image->common.authority_generation;
	candidate.settlement_authority_generation
		= settlement->common.authority_generation;
	candidate.source_generation = source_generation;
	candidate.carrier_generation = carrier_generation;
	candidate.status_semantic_crc32c = status->common.semantic_crc32c;
	candidate.image_semantic_crc32c = image->common.semantic_crc32c;
	candidate.authenticated_master_node = authenticated_master_node;
	candidate.source_mode = source_mode;
	round = &entry->resource_x_bootstrap_round;
	if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_EMPTY)
		candidate.cover_action = RESOURCE_X_SETTLEMENT_COVER_NO_COVER;
	else if (round->phase
			 == RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED) {
		if (source_mode == (uint8)PCM_STATE_S)
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		if (settlement->common.action_node != cluster_node_id
			|| round->terminal_ref.assertion.requester_node
				!= cluster_node_id
			|| round->current_master_node != authenticated_master_node
			|| round->resource_formation
				!= settlement->common.resource_formation
			|| round->master_session_incarnation
				!= settlement->common.master_session_incarnation
			|| round->terminal_authority_generation
				!= settlement->common.base_authority_generation
			|| round->cached_ownership_generation != source_generation
			|| !pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
				entry, round, &round->terminal_ref,
				round->master_session_incarnation,
				round->r4_record_generation, source_generation))
			return RESOURCE_X_APPLY_STALE;
		candidate.terminal_ref = round->terminal_ref;
		candidate.terminal_cached_generation
			= round->cached_ownership_generation;
		candidate.terminal_authority_generation
			= round->terminal_authority_generation;
		candidate.terminal_r4_record_generation
			= round->r4_record_generation;
		candidate.cover_action
			= RESOURCE_X_SETTLEMENT_COVER_CLOSE_EXACT_X;
	}
	else if (round->phase == RESOURCE_X_BOOTSTRAP_ROUND_FAILED_CLOSED)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else
		return RESOURCE_X_APPLY_BAD_STATE;
	candidate.valid = true;
	if (!pcm_resource_x_source_settlement_plan_valid(&candidate))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	*plan_out = candidate;
	return RESOURCE_X_APPLY_APPLIED;
}

static ResourceXApplyResult
pcm_resource_x_source_settlement_prepare_internal(
	const ResourceXDecodedFrame *settlement, int32 authenticated_master_node,
	ResourceXSourceSettlementPlan *plan_out,
	ResourceXSourceSettlementCommitObservation *observation_out)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXBootstrapRound *round;
	ClusterPcmResourceXLocalOwner *owner;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	ResourceXApplyResult domain;
	ResourceXApplyResult result;
	struct GrdEntry *entry;
	uint64 gate_formation;

	if (plan_out != NULL)
		memset(plan_out, 0, sizeof(*plan_out));
	if (observation_out != NULL)
		memset(observation_out, 0, sizeof(*observation_out));
	if (settlement == NULL
		|| !resource_x_assertion_valid(
			&settlement->common.logical_assertion)
		|| authenticated_master_node < 0
		|| authenticated_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| plan_out == NULL || ClusterPcm == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&settlement->common.logical_assertion.resource,
			false, &entry_ref, &acquire_result)) {
		if (observation_out != NULL)
			observation_out->commit_stage
				= RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_ENTRY_LOOKUP;
		return RESOURCE_X_APPLY_NOT_FOUND;
	}
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	round = &entry->resource_x_bootstrap_round;
	owner = &entry->resource_x_local_owner;
	gate_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (observation_out != NULL) {
		pcm_resource_x_source_settlement_observe_round_locked(
			entry, observation_out);
		observation_out->current_resource_formation = gate_formation;
	}
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		if (observation_out != NULL)
			observation_out->commit_stage
				= RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_MASTER_STATE;
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto source_prepare_done;
	}
	if (observation_out != NULL) {
		observation_out->current_holder_status_valid
			= state->holder_status.valid;
		observation_out->current_holder_image_valid
			= state->holder_image.valid;
		observation_out->current_status_intent_state
			= state->holder_status_intent.slot.state;
		observation_out->current_image_intent_state
			= state->holder_image_intent.state;
		observation_out->current_pair_destination_node
			= state->holder_status.destination_node;
	}
	domain = pcm_resource_x_holder_pair_drain_domain_locked(
		&entry->tag, state, authenticated_master_node,
		settlement->common.master_session_incarnation,
		&settlement->common.logical_assertion,
		settlement->common.assertion_sequence);
	if (domain == RESOURCE_X_APPLY_RECOVERY_BLOCKED
		|| domain == RESOURCE_X_APPLY_STALE) {
		if (observation_out != NULL) {
			observation_out->commit_stage
				= RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_DRAIN_DOMAIN;
			if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
					!= RESOURCE_X_GATE_OPEN
				|| gate_formation
					!= settlement->common.resource_formation)
				observation_out->mismatch_mask
					|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_GATE_FORMATION;
			if (cluster_gcs_lookup_master(entry->tag)
					!= authenticated_master_node)
				observation_out->mismatch_mask
					|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_MASTER;
		}
		result = domain;
		goto source_prepare_done;
	}
	result = pcm_resource_x_holder_pair_decode_locked(state, &status, &image);
	if (result != RESOURCE_X_APPLY_APPLIED) {
		if (observation_out != NULL) {
			observation_out->commit_stage
				= RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_PAIR_DECODE;
			observation_out->mismatch_mask
				|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_PAIR_BYTES;
		}
		goto source_prepare_done;
	}
	if (observation_out != NULL) {
		observation_out->current_pair_resource_formation
			= image.common.resource_formation;
		observation_out->current_pair_master_session
			= image.common.master_session_incarnation;
		observation_out->current_pair_assertion_sequence
			= image.common.assertion_sequence;
		observation_out->current_pair_source_generation
			= pcm_resource_x_source_fence_get_u64(
				image.body.image_envelope.source_fence + 20);
		observation_out->current_pair_carrier_generation
			= image.body.image_envelope.source_carrier_generation;
		observation_out->current_pair_observed_mode
			= image.common.observed_mode;
	}
	if (domain == RESOURCE_X_APPLY_DUPLICATE) {
		if (observation_out != NULL)
			observation_out->commit_stage
				= RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_DRAIN_DOMAIN;
		result = RESOURCE_X_APPLY_DUPLICATE;
		goto source_prepare_done;
	}
	result = pcm_resource_x_source_settlement_plan_build_locked(
		entry, state, settlement, authenticated_master_node,
		&status, &image, plan_out);
	if (observation_out != NULL) {
		observation_out->current_cover_action = plan_out->cover_action;
		observation_out->commit_stage
			= round->phase == RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED
			? RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_TERMINAL_COVER
			: RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_PAIR_STATE;
		if (result != RESOURCE_X_APPLY_APPLIED) {
			if (!pcm_resource_x_local_owner_valid_locked(owner))
				observation_out->mismatch_mask
					|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_OWNER_INVALID;
			else if (owner->state != RESOURCE_X_LOCAL_OWNER_EMPTY)
				observation_out->mismatch_mask
					|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_OWNER_NONEMPTY;
			if (round->phase
					== RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED) {
				if (status.common.observed_mode == (uint8)PCM_STATE_S)
					observation_out->mismatch_mask
						|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_COVER;
				if (settlement->common.action_node != cluster_node_id)
					observation_out->mismatch_mask
						|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_ACTION;
				if (round->terminal_ref.assertion.requester_node
						!= cluster_node_id)
					observation_out->mismatch_mask
						|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_REQUESTER;
				if (round->current_master_node != authenticated_master_node)
					observation_out->mismatch_mask
						|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_MASTER;
				if (round->resource_formation
						!= settlement->common.resource_formation)
					observation_out->mismatch_mask
						|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_FORMATION;
				if (round->master_session_incarnation
						!= settlement->common.master_session_incarnation)
					observation_out->mismatch_mask
						|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_SESSION;
				if (round->terminal_authority_generation
						!= settlement->common.base_authority_generation)
					observation_out->mismatch_mask
						|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_AUTHORITY;
				if (round->cached_ownership_generation
						!= observation_out->current_pair_source_generation)
					observation_out->mismatch_mask
						|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_OWNERSHIP;
				if (status.common.observed_mode == (uint8)PCM_STATE_X
					&& !pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
						entry, round, &round->terminal_ref,
						round->master_session_incarnation,
						round->r4_record_generation,
						observation_out->current_pair_source_generation))
					observation_out->mismatch_mask
						|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_COVER;
			}
			if (observation_out->mismatch_mask == 0)
				observation_out->mismatch_mask
					|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_PAIR_BYTES;
		}
	}

source_prepare_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_source_settlement_prepare_exact(
	const ResourceXDecodedFrame *settlement, int32 authenticated_master_node,
	ResourceXSourceSettlementPlan *plan_out)
{
	return pcm_resource_x_source_settlement_prepare_internal(
		settlement, authenticated_master_node, plan_out, NULL);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_source_settlement_prepare_observed_exact(
	const ResourceXDecodedFrame *settlement, int32 authenticated_master_node,
	ResourceXSourceSettlementPlan *plan_out,
	ResourceXSourceSettlementCommitObservation *observation_out)
{
	if (observation_out == NULL)
		return RESOURCE_X_APPLY_INVALID;
	return pcm_resource_x_source_settlement_prepare_internal(
		settlement, authenticated_master_node, plan_out, observation_out);
}

/* Close only the exact terminal-X episode frozen by prepare.  NO_COVER is
 * handled by the plan rebuild/equality check and can never enter this helper;
 * every non-terminal phase here is therefore post-prepare drift, not absence. */
static ResourceXApplyResult
pcm_resource_x_source_settlement_close_terminal_cover_locked(
	struct GrdEntry *entry, const ResourceXSourceSettlementPlan *plan,
	bool *cleared_out,
	ResourceXSourceSettlementCommitObservation *observation)
{
	ClusterPcmResourceXBootstrapRound *round;
	ClusterPcmResourceXLocalOwner *owner;
	uint32 mismatch_mask = 0;

	Assert(entry != NULL);
	Assert(plan != NULL);
	Assert(cleared_out != NULL);
	Assert(observation != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	*cleared_out = false;
	round = &entry->resource_x_bootstrap_round;
	owner = &entry->resource_x_local_owner;
	observation->current_round_phase = round->phase;
	observation->current_owner_state = owner->state;
	observation->current_master_node = round->current_master_node;
	observation->current_master_session
		= round->master_session_incarnation;
	observation->current_terminal_authority_generation
		= round->terminal_authority_generation;
	observation->current_cached_ownership_generation
		= round->cached_ownership_generation;
	if (plan->cover_action
			!= RESOURCE_X_SETTLEMENT_COVER_CLOSE_EXACT_X
		|| plan->source_mode != (uint8)PCM_STATE_X)
		return RESOURCE_X_APPLY_INVALID;
	if (round->phase != RESOURCE_X_BOOTSTRAP_ROUND_TERMINAL_X_CACHED) {
		observation->mismatch_mask
			|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_COVER;
		return RESOURCE_X_APPLY_STALE;
	}
	if (!pcm_resource_x_local_owner_valid_locked(owner)) {
		observation->mismatch_mask
			|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_OWNER_INVALID;
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	if (owner->state != RESOURCE_X_LOCAL_OWNER_EMPTY) {
		observation->mismatch_mask
			|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_OWNER_NONEMPTY;
		return RESOURCE_X_APPLY_BAD_STATE;
	}
	if (!pcm_resource_x_acquisition_ref_equal(
			&round->terminal_ref, &plan->terminal_ref)
		|| round->terminal_ref.assertion.requester_node != cluster_node_id)
		mismatch_mask
			|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_REQUESTER;
	if (round->current_master_node != plan->authenticated_master_node)
		mismatch_mask
			|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_MASTER;
	if (round->resource_formation != plan->resource_formation)
		mismatch_mask
			|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_FORMATION;
	if (round->master_session_incarnation
		!= plan->master_session_incarnation)
		mismatch_mask
			|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_SESSION;
	if (round->terminal_authority_generation
		!= plan->terminal_authority_generation)
		mismatch_mask
			|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_AUTHORITY;
	if (round->cached_ownership_generation != plan->source_generation
		|| round->cached_ownership_generation
			!= plan->terminal_cached_generation)
		mismatch_mask
			|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_OWNERSHIP;
	if (round->r4_record_generation
		!= plan->terminal_r4_record_generation)
		mismatch_mask
			|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_COVER;
	if (mismatch_mask == 0
		&& !pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
			entry, round, &plan->terminal_ref,
			plan->master_session_incarnation,
			plan->terminal_r4_record_generation,
			plan->source_generation))
		mismatch_mask
			|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_COVER;
	observation->mismatch_mask |= mismatch_mask;
	if (mismatch_mask != 0)
		return RESOURCE_X_APPLY_STALE;
	pcm_resource_x_bootstrap_round_clear_binding_locked(round);
	*cleared_out = true;
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_source_settlement_commit_exact(
	const ResourceXDecodedFrame *settlement, int32 authenticated_master_node,
	const ResourceXSourceSettlementPlan *plan,
	ResourceXSourceSettlementCommitObservation *observation_out)
{
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame status;
	ResourceXSourceSettlementPlan current_plan;
	ResourceXApplyResult domain;
	ResourceXApplyResult result;
	struct GrdEntry *entry;
	uint64 gate_formation;
	bool settlement_committed = false;
	bool terminal_cover_cleared = false;

	if (observation_out != NULL)
		memset(observation_out, 0, sizeof(*observation_out));
	if (settlement == NULL
		|| !resource_x_assertion_valid(
			&settlement->common.logical_assertion)
		|| authenticated_master_node < 0
		|| authenticated_master_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| !pcm_resource_x_source_settlement_plan_valid(plan)
		|| authenticated_master_node != plan->authenticated_master_node
		|| !resource_x_assertion_equal(
			&settlement->common.logical_assertion, &plan->assertion)
		|| settlement->common.resource_formation
			!= plan->resource_formation
		|| settlement->common.master_session_incarnation
			!= plan->master_session_incarnation
		|| settlement->common.assertion_sequence
			!= plan->assertion_sequence
		|| ClusterPcm == NULL
		|| observation_out == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&settlement->common.logical_assertion.resource,
			false, &entry_ref, &acquire_result)) {
		observation_out->commit_stage
			= RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_ENTRY_LOOKUP;
		return RESOURCE_X_APPLY_NOT_FOUND;
	}
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	pcm_resource_x_source_settlement_observe_round_locked(
		entry, observation_out);
	observation_out->current_cover_action = plan->cover_action;
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		observation_out->commit_stage
			= RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_MASTER_STATE;
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto source_commit_done;
	}
	observation_out->current_holder_status_valid = state->holder_status.valid;
	observation_out->current_holder_image_valid = state->holder_image.valid;
	observation_out->current_status_intent_state
		= state->holder_status_intent.slot.state;
	observation_out->current_image_intent_state
		= state->holder_image_intent.state;
	observation_out->current_pair_destination_node
		= state->holder_status.destination_node;
	domain = pcm_resource_x_holder_pair_drain_domain_locked(
		&entry->tag, state, authenticated_master_node,
		settlement->common.master_session_incarnation,
		&settlement->common.logical_assertion,
		settlement->common.assertion_sequence);
	if (domain == RESOURCE_X_APPLY_DUPLICATE
		|| domain == RESOURCE_X_APPLY_RECOVERY_BLOCKED
		|| domain == RESOURCE_X_APPLY_STALE) {
		observation_out->commit_stage
			= RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_DRAIN_DOMAIN;
		result = domain;
		goto source_commit_done;
	}
	result = pcm_resource_x_holder_pair_decode_locked(state, &status, &image);
	if (result != RESOURCE_X_APPLY_APPLIED) {
		observation_out->commit_stage
			= RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_PAIR_DECODE;
		goto source_commit_done;
	}
	observation_out->current_pair_resource_formation
		= image.common.resource_formation;
	observation_out->current_pair_master_session
		= image.common.master_session_incarnation;
	observation_out->current_pair_assertion_sequence
		= image.common.assertion_sequence;
	observation_out->current_pair_source_generation
		= pcm_resource_x_source_fence_get_u64(
			image.body.image_envelope.source_fence + 20);
	observation_out->current_pair_carrier_generation
		= image.body.image_envelope.source_carrier_generation;
	observation_out->current_pair_observed_mode = image.common.observed_mode;
	gate_formation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	observation_out->current_resource_formation = gate_formation;
	memset(&current_plan, 0, sizeof(current_plan));
	result = pcm_resource_x_source_settlement_plan_build_locked(
		entry, state, settlement, authenticated_master_node,
		&status, &image, &current_plan);
	if (result != RESOURCE_X_APPLY_APPLIED) {
		observation_out->commit_stage
			= (entry->resource_x_bootstrap_round.phase
				!= RESOURCE_X_BOOTSTRAP_ROUND_EMPTY)
			? RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_TERMINAL_COVER
			: RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_PAIR_STATE;
		if (entry->resource_x_bootstrap_round.phase
				!= RESOURCE_X_BOOTSTRAP_ROUND_EMPTY)
			observation_out->mismatch_mask
				|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_COVER;
		goto source_commit_done;
	}
	observation_out->current_cover_action = current_plan.cover_action;
	if (!pcm_resource_x_source_settlement_plan_equal(plan, &current_plan)) {
		if (plan->source_generation != current_plan.source_generation
			|| plan->carrier_generation != current_plan.carrier_generation)
			observation_out->mismatch_mask
				|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_SOURCE_GENERATION;
		if (plan->cover_action != current_plan.cover_action)
			observation_out->mismatch_mask
				|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_COVER;
		if (plan->terminal_cached_generation
			!= current_plan.terminal_cached_generation)
			observation_out->mismatch_mask
				|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_OWNERSHIP;
		if (plan->terminal_authority_generation
			!= current_plan.terminal_authority_generation)
			observation_out->mismatch_mask
				|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_AUTHORITY;
		if (plan->master_session_incarnation
			!= current_plan.master_session_incarnation)
			observation_out->mismatch_mask
				|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_SESSION;
		if (observation_out->mismatch_mask == 0)
			observation_out->mismatch_mask
				|= RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_PAIR_BYTES;
		observation_out->commit_stage
			= (observation_out->mismatch_mask
				& (RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_COVER
					| RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_OWNERSHIP
					| RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_AUTHORITY
					| RESOURCE_X_SOURCE_SETTLEMENT_MISMATCH_TERMINAL_SESSION))
			? RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_TERMINAL_COVER
			: RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_PAIR_IDENTITY;
		result = RESOURCE_X_APPLY_STALE;
		goto source_commit_done;
	}
	observation_out->commit_stage
		= RESOURCE_X_SOURCE_SETTLEMENT_COMMIT_STAGE_TERMINAL_COVER;
	if (plan->cover_action
		== RESOURCE_X_SETTLEMENT_COVER_CLOSE_EXACT_X) {
		result = pcm_resource_x_source_settlement_close_terminal_cover_locked(
			entry, plan, &terminal_cover_cleared, observation_out);
		if (result != RESOURCE_X_APPLY_APPLIED)
			goto source_commit_done;
	}
	state->holder_pair_drained_sequences[
		settlement->common.logical_assertion.requester_node]
		= settlement->common.assertion_sequence;
	state->holder_pair_drained_resource_formation = gate_formation;
	state->holder_pair_drained_master_session
		= settlement->common.master_session_incarnation;
	state->holder_pair_drained_master_node = authenticated_master_node;
	state->holder_pair_drained_reserved = 0;
	settlement_committed = true;
	result = RESOURCE_X_APPLY_APPLIED;

source_commit_done:
	LWLockRelease(&entry->entry_lock.lock);
	if (terminal_cover_cleared || settlement_committed)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_source_settlement_ack_build_exact(
	const ResourceXDecodedFrame *settlement,
	uint32 sender_connection_generation, ResourceXDecodedFrame *ack_out)
{
	if (ack_out != NULL)
		memset(ack_out, 0, sizeof(*ack_out));
	if (settlement == NULL || ack_out == NULL
		|| settlement->kind != RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
		|| !settlement->blocked_has_remote_proof
		|| !resource_x_assertion_valid(
			&settlement->common.logical_assertion)
		|| settlement->common.ordered_lane != 0
		|| settlement->common.observed_mode != (uint8)PCM_STATE_N
		|| settlement->common.target_mode != (uint8)PCM_STATE_N
		|| settlement->common.source_candidate != 1
		|| settlement->common.retain_pi_if_dirty != 1
		|| settlement->common.outcome != RESOURCE_X_OUTCOME_NONE
		|| settlement->common.flags != 0
		|| sender_connection_generation == 0)
		return RESOURCE_X_APPLY_INVALID;
	*ack_out = *settlement;
	ack_out->kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2;
	ack_out->payload_bytes = RESOURCE_X_PROOF_V1_BYTES;
	ack_out->common.sender_connection_generation
		= sender_connection_generation;
	ack_out->common.outcome = RESOURCE_X_OUTCOME_OK;
	ack_out->common.semantic_crc32c = 0;
	return RESOURCE_X_APPLY_APPLIED;
}

/* Retained type-15 bytes use one canonical sender generation so connection
 * freshness remains an ingress property and exact duplicate comparison is
 * stable across a reconnect.  All other wire semantics, including the
 * semantic CRC over the canonical bytes, remain frozen. */
static bool
pcm_resource_x_requester_join_normalize(
	const ResourceXDecodedFrame *frame, uint8 *payload, uint16 capacity,
	uint16 *payload_bytes_out)
{
	ResourceXDecodedFrame canonical;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;

	if (payload_bytes_out != NULL)
		*payload_bytes_out = 0;
	if (frame == NULL || payload == NULL || payload_bytes_out == NULL
		|| (frame->kind != RESOURCE_X_WIRE_AUTHORITY_GRANT
			&& frame->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE))
		return false;
	canonical = *frame;
	canonical.common.sender_connection_generation
		= PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	return cluster_resource_x_wire_encode(
		RESOURCE_X_MSG_IMAGE_OR_GRANT, &canonical, payload, capacity,
		payload_bytes_out, &reject);
}

static bool
pcm_resource_x_requester_join_empty_locked(
	const ClusterPcmResourceXRequesterJoin *join)
{
	static const ClusterPcmResourceXRequesterJoin empty_join;

	Assert(join != NULL);
	return memcmp(join, &empty_join, sizeof(*join)) == 0;
}

static bool
pcm_resource_x_requester_join_decode_locked(
	const ClusterPcmResourceXRequesterJoin *join,
	ResourceXDecodedFrame *grant_out, ResourceXDecodedFrame *image_out)
{
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;

	Assert(join != NULL);
	if (grant_out != NULL)
		memset(grant_out, 0, sizeof(*grant_out));
	if (image_out != NULL)
		memset(image_out, 0, sizeof(*image_out));
	if (join->grant_valid != 0
		&& (join->grant_valid != 1 || grant_out == NULL
			|| join->grant_payload_bytes != RESOURCE_X_PROOF_V1_BYTES
			|| !cluster_resource_x_wire_decode(
				RESOURCE_X_MSG_IMAGE_OR_GRANT, join->grant_payload,
				join->grant_payload_bytes, grant_out, &reject)
			|| grant_out->kind != RESOURCE_X_WIRE_AUTHORITY_GRANT))
		return false;
	reject = RESOURCE_X_WIRE_REJECT_NONE;
	if (join->image_valid != 0
		&& (join->image_valid != 1 || image_out == NULL
			|| join->image_payload_bytes != RESOURCE_X_IMAGE_V1_BYTES
			|| !cluster_resource_x_wire_decode(
				RESOURCE_X_MSG_IMAGE_OR_GRANT, join->image_payload,
				join->image_payload_bytes, image_out, &reject)
			|| image_out->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE))
		return false;
	return true;
}

static bool
pcm_resource_x_requester_join_pair_matches(
	const ResourceXDecodedFrame *grant, const ResourceXDecodedFrame *image,
	uint32 image_semantic_crc32c)
{
	const ResourceXDecodedAuthorityGrant *grant_body;
	const ResourceXDecodedImageEnvelope *image_body;
	uint8 source_mode;

	if (grant == NULL || image == NULL
		|| grant->kind != RESOURCE_X_WIRE_AUTHORITY_GRANT
		|| image->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE)
		return false;
	grant_body = &grant->body.authority_grant;
	image_body = &image->body.image_envelope;
	source_mode = image_body->source_fence[28];
	return resource_x_assertion_equal(&grant->common.logical_assertion,
								  &image->common.logical_assertion)
		&& grant->common.base_authority_generation
			== image->common.base_authority_generation
		&& grant->common.resource_formation
			== image->common.resource_formation
		&& grant->common.master_session_incarnation
			== image->common.master_session_incarnation
		&& grant->common.assertion_sequence
			== image->common.assertion_sequence
		&& grant->common.ordered_lane == image->common.ordered_lane
		&& grant->common.action_node == image->common.action_node
		/* The retained carrier is the selected source's exact base+1
		 * source-mode->N+PI edge.  Other frozen blockers may consume further
		 * authority edges before the authenticated master publishes the final
		 * N->X grant, so the total join contract is base < image < grant rather
		 * than adjacency between image and grant. */
		&& (source_mode == (uint8)PCM_STATE_X
			|| source_mode == (uint8)PCM_STATE_S)
		&& image->common.observed_mode == source_mode
		&& image->common.authority_generation > 0
		&& image->common.authority_generation - 1
			== grant->common.base_authority_generation
		&& grant->common.authority_generation
			> image->common.authority_generation
		&& grant_body->final_authority_generation
			== grant->common.authority_generation
		&& image_body->conversion_base_generation
			== grant->common.base_authority_generation
		&& memcmp(grant_body->source_fence, image_body->source_fence,
				  sizeof(grant_body->source_fence)) == 0
		&& grant_body->source_carrier_generation
			== image_body->source_carrier_generation
		&& grant_body->requester_target_generation
			== image_body->requester_target_generation
		&& grant_body->page_scn_lsn == image_body->page_scn_lsn
		&& grant_body->dependency_count == image_body->dependency_count
		&& memcmp(grant_body->dependencies, image_body->dependencies,
				  sizeof(grant_body->dependencies)) == 0
		&& image_semantic_crc32c != 0
		&& grant_body->source_proof_crc32c == image_semantic_crc32c
		&& grant_body->page_checksum == image_body->page_checksum
		&& grant_body->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER
		&& image_body->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER
		&& grant_body->source_disposition
			== RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE
		&& image_body->source_disposition
			== RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
}

static bool
pcm_resource_x_requester_join_snapshot_locked(
	const ClusterPcmResourceXRequesterJoin *join,
	ResourceXRequesterJoinSnapshot *out)
{
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	const ResourceXDecodedFrame *identity;

	Assert(join != NULL);
	Assert(out != NULL);
	memset(out, 0, sizeof(*out));
	if (!pcm_resource_x_requester_join_decode_locked(join, &grant, &image))
		return false;
	if (join->grant_valid == 0 && join->image_valid == 0)
		return true;
	identity = join->grant_valid != 0 ? &grant : &image;
	out->assertion = identity->common.logical_assertion;
	out->base_authority_generation
		= identity->common.base_authority_generation;
	out->resource_formation = identity->common.resource_formation;
	out->master_session_incarnation
		= identity->common.master_session_incarnation;
	out->assertion_sequence = identity->common.assertion_sequence;
	out->final_authority_generation
		= identity->common.authority_generation;
	out->t_image_us = join->t_image_us;
	out->t_grant_us = join->t_grant_us;
	out->t_install_us = join->t_install_us;
	out->grant_source_node = join->grant_source_node;
	out->image_source_node = join->image_source_node;
	if (join->grant_valid != 0) {
		out->source_carrier_generation
			= grant.body.authority_grant.source_carrier_generation;
		out->requester_target_generation
			= grant.body.authority_grant.requester_target_generation;
		out->flags |= RESOURCE_X_REQUESTER_JOIN_HAS_GRANT;
	} else {
		out->source_carrier_generation
			= image.body.image_envelope.source_carrier_generation;
		out->requester_target_generation
			= image.body.image_envelope.requester_target_generation;
	}
	if (join->image_valid != 0)
		out->flags |= RESOURCE_X_REQUESTER_JOIN_HAS_IMAGE;
	if (join->grant_valid != 0
		&& (grant.body.authority_grant.proof_kind
				!= RESOURCE_X_PROOF_REMOTE_CARRIER
			|| (join->image_valid != 0
				&& pcm_resource_x_requester_join_pair_matches(
					&grant, &image, join->image_semantic_crc32c))))
		out->flags |= RESOURCE_X_REQUESTER_JOIN_READY;
	if (join->settled != 0)
		out->flags |= RESOURCE_X_REQUESTER_JOIN_TERMINAL;
	return true;
}

static ResourceXApplyResult
pcm_resource_x_requester_join_bindable_locked(
	struct GrdEntry *entry, const ResourceXAcquisitionRef *ref)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXRequesterJoin *join;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXRequesterJoinSnapshot snapshot;

	Assert(entry != NULL);
	Assert(ref != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	join = &state->requester_join;
	if (!pcm_resource_x_requester_join_decode_locked(join, &grant, &image))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (join->grant_valid == 0 && join->image_valid == 0)
		return RESOURCE_X_APPLY_APPLIED;
	if (!pcm_resource_x_requester_join_snapshot_locked(join, &snapshot))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	return resource_x_assertion_equal(&snapshot.assertion, &ref->assertion)
			   && snapshot.resource_formation == ref->formation
			   && snapshot.requester_target_generation
				  == ref->acquisition_generation
		? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_STALE;
}

static ResourceXApplyResult
pcm_resource_x_requester_retirement_prepare_locked(
	struct GrdEntry *entry, const ResourceXAcquisitionRef *ref,
	bool install_succeeded, bool requester_loss_seen,
	PcmResourceXRetirementWitness *witness_out)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXRequesterJoin *join;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXRequesterJoinSnapshot snapshot;
	uint64 now_us = 0;
	uint8 proof_kind;
	bool live_round_base_exact = false;

	Assert(entry != NULL);
	Assert(ref != NULL);
	Assert(witness_out != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	memset(witness_out, 0, sizeof(*witness_out));
	if (entry->resource_x_requester_base_generation < 1
		|| entry->resource_x_requester_base_generation == UINT64_MAX
		|| entry->resource_x_retired_acquisition_generation == UINT64_MAX
		|| ref->acquisition_generation
			<= entry->resource_x_retired_acquisition_generation)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL || ClusterPcm == NULL)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	join = &state->requester_join;
	if (!pcm_resource_x_requester_join_decode_locked(join, &grant, &image))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (join->grant_valid == 0 && join->image_valid == 0)
		return RESOURCE_X_APPLY_APPLIED;
	if (!pcm_resource_x_requester_join_snapshot_locked(join, &snapshot))
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if ((snapshot.flags & RESOURCE_X_REQUESTER_JOIN_READY) == 0
		|| join->grant_valid == 0
		|| !resource_x_assertion_equal(&snapshot.assertion, &ref->assertion)
		|| snapshot.resource_formation != ref->formation
		|| snapshot.requester_target_generation != ref->acquisition_generation)
		return RESOURCE_X_APPLY_BAD_STATE;
	if (snapshot.final_authority_generation
			<= entry->resource_x_requester_base_generation) {
		now_us = pcm_resource_x_monotonic_us();
		live_round_base_exact
			= pcm_resource_x_requester_join_live_round_base_exact_locked(
				entry, &grant, join->grant_source_node,
				ref->acquisition_generation, now_us);
	}
	if (snapshot.final_authority_generation < 2
		|| snapshot.final_authority_generation == UINT64_MAX
		|| (snapshot.final_authority_generation
				<= entry->resource_x_requester_base_generation
			&& !live_round_base_exact)
		|| grant.common.authority_generation
			!= grant.body.authority_grant.final_authority_generation)
		return RESOURCE_X_APPLY_STALE;
	proof_kind = grant.body.authority_grant.proof_kind;
	if (proof_kind < RESOURCE_X_PROOF_KIND_MIN
		|| proof_kind > RESOURCE_X_PROOF_KIND_MAX)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	if (join->settled != 0) {
		if (join->settled != 1 || join->proof_kind != proof_kind
			|| join->install_succeeded > 1 || join->requester_loss_seen > 1)
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		witness_out->already_settled = true;
		witness_out->install_succeeded = join->install_succeeded;
		witness_out->requester_loss_seen = join->requester_loss_seen;
		witness_out->t_install_us = join->t_install_us;
	} else {
		if (install_succeeded) {
			if (now_us == 0)
				now_us = pcm_resource_x_monotonic_us();
			if (now_us == 0 || now_us == UINT64_MAX)
				return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
		witness_out->install_succeeded = install_succeeded ? 1 : 0;
		witness_out->requester_loss_seen = requester_loss_seen ? 1 : 0;
		witness_out->t_install_us = now_us;
	}
	witness_out->had_join = true;
	witness_out->proof_kind = proof_kind;
	witness_out->final_authority_generation
		= snapshot.final_authority_generation;
	witness_out->t_image_us = join->t_image_us;
	witness_out->t_grant_us = join->t_grant_us;
	return RESOURCE_X_APPLY_APPLIED;
}

static void
pcm_resource_x_requester_settlement_clear(
	ClusterPcmResourceXControlIntent *intent)
{
	Assert(intent != NULL);
	memset(intent, 0, sizeof(*intent));
}

static bool
pcm_resource_x_requester_settlement_arm_locked(
	struct GrdEntry *entry, const ResourceXAcquisitionRef *ref,
	const PcmResourceXRetirementWitness *witness)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXRequesterJoin *join;
	ClusterPcmResourceXControlIntent *intent;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame settlement;
	ResourceXIntentBodyHandle body;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint8 payload[RESOURCE_X_SHORT_V1_BYTES];
	uint16 payload_bytes = 0;

	Assert(entry != NULL);
	Assert(ref != NULL);
	Assert(witness != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	if (!witness->had_join)
		return true;
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL || cluster_node_id < 0
		|| cluster_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return false;
	join = &state->requester_join;
	intent = &state->requester_settlement_intent;
	if (!pcm_resource_x_requester_join_decode_locked(join, &grant, &image)
		|| join->grant_valid == 0
		|| join->grant_source_node < 0
		|| join->grant_source_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| witness->already_settled || !witness->install_succeeded
		|| witness->requester_loss_seen
		|| witness->t_install_us == 0
		|| !resource_x_assertion_equal(
			&grant.common.logical_assertion, &ref->assertion)
		|| grant.common.resource_formation != ref->formation
		|| grant.common.assertion_sequence != ref->acquisition_generation
		|| grant.common.authority_generation
			!= witness->final_authority_generation
		|| grant.body.authority_grant.final_authority_generation
			!= witness->final_authority_generation)
		return false;

	memset(&settlement, 0, sizeof(settlement));
	settlement.kind = RESOURCE_X_WIRE_INSTALL_SETTLEMENT;
	settlement.payload_bytes = RESOURCE_X_SHORT_V1_BYTES;
	settlement.common.logical_assertion = grant.common.logical_assertion;
	settlement.common.base_authority_generation
		= grant.common.base_authority_generation;
	settlement.common.resource_formation = grant.common.resource_formation;
	settlement.common.master_session_incarnation
		= grant.common.master_session_incarnation;
	settlement.common.assertion_sequence = grant.common.assertion_sequence;
	settlement.common.ordered_lane = grant.common.ordered_lane;
	settlement.common.action_node = cluster_node_id;
	settlement.common.observed_mode = (uint8)PCM_STATE_N;
	settlement.common.target_mode = (uint8)PCM_STATE_X;
	settlement.common.sender_connection_generation
		= PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.authority_generation
		= witness->final_authority_generation;
	settlement.body.install_settlement.conversion_base_generation
		= grant.common.base_authority_generation;
	settlement.body.install_settlement.final_authority_generation
		= witness->final_authority_generation;
	settlement.body.install_settlement.requester_connection_generation
		= grant.body.authority_grant.requester_connection_generation;
	settlement.body.install_settlement.requester_target_generation
		= ref->acquisition_generation;
	settlement.body.install_settlement.page_scn_lsn
		= grant.body.authority_grant.page_scn_lsn;
	settlement.body.install_settlement.page_checksum
		= grant.body.authority_grant.page_checksum;
	settlement.body.install_settlement.source_proof_crc32c
		= grant.body.authority_grant.source_proof_crc32c;
	settlement.body.install_settlement.installed_mode = (uint8)PCM_STATE_X;
	settlement.body.install_settlement.requester_role
		= RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome
		= RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state
		= RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	if (!cluster_resource_x_wire_encode(
			RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &settlement, payload,
			sizeof(payload), &payload_bytes, &reject)
		|| payload_bytes != RESOURCE_X_SHORT_V1_BYTES)
		return false;

	memset(&body, 0, sizeof(body));
	body.assertion = ref->assertion;
	body.owner_generation = ref->acquisition_generation;
	body.owner_node = (uint32)cluster_node_id;
	body.owner_kind = RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT;
	if (intent->slot.state != RESOURCE_X_INTENT_SLOT_EMPTY)
		return intent->slot.logical_generation == ref->acquisition_generation
			&& intent->slot.authority_generation
				== witness->final_authority_generation
			&& intent->slot.destination_node
				== (uint32)join->grant_source_node
			&& intent->slot.payload_bytes == payload_bytes
			&& intent->slot.kind == RESOURCE_X_WIRE_INSTALL_SETTLEMENT
			&& memcmp(&intent->slot.body, &body, sizeof(body)) == 0
			&& memcmp(intent->payload, payload, payload_bytes) == 0;
	memcpy(intent->payload, payload, payload_bytes);
	if (!cluster_pcm_lock_resource_x_intent_arm_exact(
			&intent->slot, &body, ref->acquisition_generation,
			witness->final_authority_generation, witness->t_install_us,
			(uint32)join->grant_source_node, payload_bytes,
			RESOURCE_X_WIRE_INSTALL_SETTLEMENT)
		|| !pcm_resource_x_intent_mark_dirty()) {
		pcm_resource_x_requester_settlement_clear(intent);
		return false;
	}
	return true;
}

static void
pcm_resource_x_requester_retirement_commit_locked(
	struct GrdEntry *entry, const ResourceXAcquisitionRef *ref,
	const PcmResourceXRetirementWitness *witness)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXRequesterJoin *join;

	Assert(entry != NULL);
	Assert(ref != NULL);
	Assert(witness != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	state = pcm_resource_x_master_state_for_entry(entry);
	Assert(state != NULL);
	join = &state->requester_join;
	if (witness->had_join && !witness->already_settled) {
		join->t_install_us = witness->t_install_us;
		join->proof_kind = witness->proof_kind;
		join->install_succeeded = witness->install_succeeded;
		join->requester_loss_seen = witness->requester_loss_seen;
		join->settled = 1;
		if (witness->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER
			&& !witness->requester_loss_seen) {
			if (witness->t_image_us != 0)
				pg_atomic_write_u64(
					&ClusterPcm->resource_x_o1_last_remote_t_image_us,
					witness->t_image_us);
			if (witness->t_grant_us != 0)
				pg_atomic_write_u64(
					&ClusterPcm->resource_x_o1_last_remote_t_grant_us,
					witness->t_grant_us);
			if (witness->t_install_us != 0)
				pg_atomic_write_u64(
					&ClusterPcm->resource_x_o1_last_remote_t_install_us,
					witness->t_install_us);
			if (witness->install_succeeded && witness->t_image_us != 0
				&& witness->t_grant_us != 0 && witness->t_install_us != 0) {
				pg_atomic_fetch_add_u64(
					&ClusterPcm->resource_x_o1_remote_install_observed_count, 1);
				if (witness->t_grant_us > witness->t_image_us)
					pg_atomic_fetch_add_u64(
						&ClusterPcm->resource_x_o1_remote_grant_after_image_count, 1);
				else
					pg_atomic_fetch_add_u64(
						&ClusterPcm->resource_x_o1_remote_image_at_or_after_grant_count, 1);
			} else {
				pg_atomic_fetch_add_u64(
					&ClusterPcm->resource_x_o1_remote_episode_excluded_no_install, 1);
				if (witness->t_grant_us == 0)
					pg_atomic_fetch_add_u64(
						&ClusterPcm->resource_x_o1_remote_episode_excluded_missing_grant, 1);
				if (witness->t_image_us == 0)
					pg_atomic_fetch_add_u64(
						&ClusterPcm->resource_x_o1_remote_episode_excluded_missing_image, 1);
			}
		}
	}
	memset(join, 0, sizeof(*join));
	entry->resource_x_retired_acquisition_generation
		= ref->acquisition_generation;
	if (witness->had_join)
		entry->resource_x_requester_base_generation
			= witness->final_authority_generation;
	pcm_resource_x_active_clear_locked(entry);
}

/* A freshly reused local entry may carry a node-local lifecycle floor above
 * the current remote master's authority sequence.  That floor prevents
 * binding ABA; it is not a second Resource-X authority.  Before T1 (and for
 * an exact replay while T1 is active), the accepted kind-9 ACK is the only
 * base allowed to disambiguate the two axes.  Keep the exception bounded to
 * one live ASSERT-dispatched round and its original absolute deadline. */
static bool
pcm_resource_x_requester_join_live_round_base_exact_locked(
	struct GrdEntry *entry, const ResourceXDecodedFrame *frame,
	int32 authenticated_source_node, uint64 requester_target_generation,
	uint64 now_us)
{
	const ClusterPcmResourceXBootstrapRound *round;
	const ResourceXDecodedCommon *common;

	Assert(entry != NULL);
	Assert(frame != NULL);
	Assert(LWLockHeldByMeInMode(
		&entry->entry_lock.lock, LW_EXCLUSIVE));
	round = &entry->resource_x_bootstrap_round;
	common = &frame->common;
	return round->phase == RESOURCE_X_BOOTSTRAP_ROUND_ASSERT_DISPATCHED && now_us != 0
		   && now_us != UINT64_MAX && now_us >= round->last_dispatch_us
		   && now_us < round->head_no_progress_deadline_us && round->current_master_node >= 0
		   && round->current_master_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		   && (frame->kind != RESOURCE_X_WIRE_AUTHORITY_GRANT
			   || round->current_master_node == authenticated_source_node)
		   && resource_x_assertion_equal(&round->request.logical_assertion,
										 &common->logical_assertion)
		   && round->resource_formation == common->resource_formation
		   && round->master_session_incarnation == common->master_session_incarnation
		   && round->r4_record_generation != 0 && round->r4_record_generation != UINT64_MAX
		   && round->request.assertion_sequence == requester_target_generation
		   && round->highest_attempt_floor == requester_target_generation
		   && round->request.ordered_lane == common->ordered_lane && round->accepted_base != 0
		   && round->accepted_base != UINT64_MAX
		   && round->accepted_base == common->base_authority_generation
		   && common->authority_generation > round->accepted_base
		   && common->authority_generation != UINT64_MAX
		   && resource_x_assertion_equal(&round->ack.logical_assertion,
										 &round->request.logical_assertion)
		   && round->ack.base_authority_generation == round->accepted_base
		   && round->ack.authority_generation == 0
		   && round->ack.resource_formation == round->resource_formation
		   && round->ack.master_session_incarnation == round->master_session_incarnation
		   && round->ack.assertion_sequence == requester_target_generation
		   && round->ack.outcome == RESOURCE_X_OUTCOME_OK
		   && resource_x_assertion_equal(&round->assertion.logical_assertion,
										 &round->request.logical_assertion)
		   && round->assertion.base_authority_generation == round->accepted_base
		   && round->assertion.authority_generation == round->accepted_base
		   && round->assertion.resource_formation == round->resource_formation
		   && round->assertion.master_session_incarnation == round->master_session_incarnation
		   && round->assertion.assertion_sequence == requester_target_generation
		   && round->assertion.ordered_lane == common->ordered_lane
		   && round->assertion.outcome == RESOURCE_X_OUTCOME_NONE
		   && cluster_pcm_lock_resource_x_gate_open_exact(round->resource_formation);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_requester_join_exact(
	const ResourceXDecodedFrame *frame, int32 authenticated_source_node,
	ResourceXRequesterJoinSnapshot *out)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXRequesterJoin *join;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXDecodedFrame grant;
	ResourceXDecodedFrame image;
	ResourceXDecodedFrame normalized_frame;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	ResourceXAcquisitionRef ref;
	ResourceXApplyResult result;
	PcmResourceXRefClass ref_class;
	struct GrdEntry *entry;
	uint8 normalized[RESOURCE_X_IMAGE_V1_BYTES];
	uint16 normalized_bytes = 0;
	uint64 gate_formation;
	uint64 final_authority_generation;
	uint64 requester_target_generation;
	uint64 now_us = 0;
	bool live_round_base_exact = false;
	bool is_grant;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (frame == NULL || out == NULL
		|| (frame->kind != RESOURCE_X_WIRE_AUTHORITY_GRANT
			&& frame->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE)
		|| !resource_x_assertion_valid(&frame->common.logical_assertion)
		|| authenticated_source_node < 0
		|| authenticated_source_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| cluster_node_id < 0
		|| cluster_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| frame->common.logical_assertion.requester_node != cluster_node_id
		|| frame->common.action_node != cluster_node_id
		|| frame->common.resource_formation == 0
		|| frame->common.authority_generation == 0
		|| frame->common.sender_connection_generation == 0
		|| (frame->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE
			&& frame->common.semantic_crc32c == 0)
		|| ClusterPcm == NULL)
		return RESOURCE_X_APPLY_INVALID;
	is_grant = frame->kind == RESOURCE_X_WIRE_AUTHORITY_GRANT;
	requester_target_generation = is_grant
		? frame->body.authority_grant.requester_target_generation
		: frame->body.image_envelope.requester_target_generation;
	final_authority_generation = frame->common.authority_generation;
	if (requester_target_generation == 0
		|| requester_target_generation == UINT64_MAX
		|| frame->common.assertion_sequence != requester_target_generation
		|| final_authority_generation < 2
		|| final_authority_generation == UINT64_MAX
		|| (is_grant
			&& frame->body.authority_grant.final_authority_generation
				!= final_authority_generation))
		return RESOURCE_X_APPLY_INVALID;
	if ((is_grant
			&& cluster_gcs_lookup_master(frame->common.logical_assertion.resource)
				!= authenticated_source_node)
		|| (!is_grant && authenticated_source_node == cluster_node_id)
		|| !pcm_resource_x_requester_join_normalize(
			frame, normalized, sizeof(normalized), &normalized_bytes)
		|| (is_grant && normalized_bytes != RESOURCE_X_PROOF_V1_BYTES)
		|| (!is_grant && normalized_bytes != RESOURCE_X_IMAGE_V1_BYTES))
		return RESOURCE_X_APPLY_INVALID;
	if (!cluster_resource_x_wire_decode(
			RESOURCE_X_MSG_IMAGE_OR_GRANT, normalized, normalized_bytes,
			&normalized_frame, &reject)
		|| normalized_frame.kind != frame->kind
		|| normalized_frame.common.sender_connection_generation
			!= PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION
		|| (!is_grant
			&& normalized_frame.common.semantic_crc32c == 0))
		return RESOURCE_X_APPLY_INVALID;
	gate_formation = pg_atomic_read_u64(&ClusterPcm->resource_x_gate_formation);
	if (pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			!= RESOURCE_X_GATE_OPEN
		|| gate_formation != frame->common.resource_formation)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	/* A retained grant/image is allowed to arrive before T1 creates the
	 * requester active tuple.  Materialize only the bounded keyed container
	 * after the complete wire/gate/source validation above; this publishes no
	 * authority, and T1 remains the sole active-ref bind. */
	if (!pcm_entry_ref_acquire(&frame->common.logical_assertion.resource,
			true, &entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	ref.assertion = frame->common.logical_assertion;
	ref.formation = frame->common.resource_formation;
	ref.acquisition_generation = requester_target_generation;
	ref_class = pcm_resource_x_ref_classify_locked(entry, &ref);
	if (ref_class == PCM_RX_REF_CORRUPT) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto requester_join_done;
	}
	if (final_authority_generation
			<= entry->resource_x_requester_base_generation
		&& (ref_class == PCM_RX_REF_FUTURE_EMPTY
			|| ref_class == PCM_RX_REF_ACTIVE_EXACT)) {
		now_us = pcm_resource_x_monotonic_us();
		live_round_base_exact
			= pcm_resource_x_requester_join_live_round_base_exact_locked(
				entry, frame, authenticated_source_node,
				requester_target_generation, now_us);
	}
	if (ref_class == PCM_RX_REF_RETIRED_OLD
		|| ref_class == PCM_RX_REF_RETIRED_EXACT
		|| ref_class == PCM_RX_REF_ACTIVE_OTHER
		|| (final_authority_generation
				<= entry->resource_x_requester_base_generation
			&& !live_round_base_exact)) {
		result = RESOURCE_X_APPLY_STALE;
		goto requester_join_done;
	}
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto requester_join_done;
	}
	join = &state->requester_join;
	if (!pcm_resource_x_requester_join_decode_locked(join, &grant, &image)) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto requester_join_done;
	}
	if ((is_grant && join->grant_valid != 0)
		|| (!is_grant && join->image_valid != 0)) {
		const uint8 *retained = is_grant
			? join->grant_payload : join->image_payload;
		uint16 retained_bytes = is_grant
			? join->grant_payload_bytes : join->image_payload_bytes;
		result = retained_bytes == normalized_bytes
				&& memcmp(retained, normalized, normalized_bytes) == 0
			? RESOURCE_X_APPLY_DUPLICATE : RESOURCE_X_APPLY_STALE;

		if (!pcm_resource_x_requester_join_snapshot_locked(join, out))
			result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto requester_join_done;
	}
	if ((is_grant && join->image_valid != 0
			&& !pcm_resource_x_requester_join_pair_matches(
				frame, &image, join->image_semantic_crc32c))
		|| (!is_grant && join->grant_valid != 0
			&& !pcm_resource_x_requester_join_pair_matches(
				&grant, &normalized_frame,
				normalized_frame.common.semantic_crc32c))) {
		(void)pcm_resource_x_requester_join_snapshot_locked(join, out);
		result = RESOURCE_X_APPLY_STALE;
		goto requester_join_done;
	}
	if (now_us == 0)
		now_us = pcm_resource_x_monotonic_us();
	if (now_us == 0 || now_us == UINT64_MAX) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto requester_join_done;
	}
	if (is_grant) {
		memcpy(join->grant_payload, normalized, normalized_bytes);
		join->grant_payload_bytes = normalized_bytes;
		join->grant_source_node = authenticated_source_node;
		join->t_grant_us = now_us;
		join->grant_valid = 1;
	} else {
		memcpy(join->image_payload, normalized, normalized_bytes);
		join->image_payload_bytes = normalized_bytes;
		join->image_source_node = authenticated_source_node;
		join->t_image_us = now_us;
		join->image_semantic_crc32c
			= normalized_frame.common.semantic_crc32c;
		join->image_valid = 1;
	}
	if (!pcm_resource_x_requester_join_snapshot_locked(join, out)) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto requester_join_done;
	}
	result = RESOURCE_X_APPLY_APPLIED;

requester_join_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_requester_join_frames_exact(
	const ResourceXAssertion *assertion, ResourceXDecodedFrame *grant_out,
	ResourceXDecodedFrame *image_out, ResourceXRequesterJoinSnapshot *out)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXRequesterJoin *join;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXApplyResult result;
	struct GrdEntry *entry;

	if (grant_out != NULL)
		memset(grant_out, 0, sizeof(*grant_out));
	if (image_out != NULL)
		memset(image_out, 0, sizeof(*image_out));
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (!resource_x_assertion_valid(assertion) || grant_out == NULL
		|| image_out == NULL || out == NULL)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto requester_join_frames_done;
	}
	join = &state->requester_join;
	if (!pcm_resource_x_requester_join_decode_locked(join, grant_out,
			image_out)
		|| !pcm_resource_x_requester_join_snapshot_locked(join, out)) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto requester_join_frames_done;
	}
	if (join->image_valid != 0)
		image_out->common.semantic_crc32c = join->image_semantic_crc32c;
	if (join->grant_valid == 0 && join->image_valid == 0) {
		result = RESOURCE_X_APPLY_NOT_FOUND;
		goto requester_join_frames_done;
	}
	if (!resource_x_assertion_equal(assertion, &out->assertion)) {
		result = RESOURCE_X_APPLY_STALE;
		goto requester_join_frames_done;
	}
	if ((out->flags & RESOURCE_X_REQUESTER_JOIN_READY) == 0) {
		result = RESOURCE_X_APPLY_BAD_STATE;
		goto requester_join_frames_done;
	}
	result = RESOURCE_X_APPLY_APPLIED;

requester_join_frames_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

static ResourceXApplyResult
pcm_resource_x_build_authority_grant_locked(
	struct GrdEntry *entry, const ClusterPcmResourceXMasterRequest *request,
	int32 requester_node, uint64 final_authority_generation,
	ResourceXDecodedFrame *out)
{
	ResourceXDecodedAuthorityGrant *grant;
	uint64 requester_connection_generation;

	Assert(entry != NULL);
	Assert(request != NULL);
	Assert(out != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_SHARED)
		   || LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	memset(out, 0, sizeof(*out));
	requester_connection_generation
		= request->requester_connection_generation != 0
		? request->requester_connection_generation
		: (uint64)request->sender_connection_generation;
	if (final_authority_generation == 0
		|| request->requester_target_generation == 0
		|| requester_connection_generation == 0
		|| request->proof_kind < RESOURCE_X_PROOF_KIND_MIN
		|| request->proof_kind > RESOURCE_X_PROOF_KIND_MAX
		|| request->dependency_count > RESOURCE_X_DEPENDENCY_MAX
		|| (request->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER
			&& (request->source_carrier_generation == 0
				|| request->source_disposition
					   != RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE))
		|| (request->proof_kind == RESOURCE_X_PROOF_LOCAL_IMAGE
			&& request->source_disposition
				   != RESOURCE_X_DISPOSITION_LOCAL_IMAGE)
		|| (request->proof_kind == RESOURCE_X_PROOF_DURABLE_STORAGE
			&& request->source_disposition
				   != RESOURCE_X_DISPOSITION_DURABLE_STORAGE))
		return RESOURCE_X_APPLY_BAD_STATE;
	out->kind = RESOURCE_X_WIRE_AUTHORITY_GRANT;
	out->payload_bytes = RESOURCE_X_PROOF_V1_BYTES;
	out->common.logical_assertion.resource = entry->tag;
	out->common.logical_assertion.requester_node = requester_node;
	out->common.base_authority_generation = request->base_authority_generation;
	out->common.resource_formation = request->resource_formation;
	out->common.master_session_incarnation
		= request->master_session_incarnation;
	out->common.assertion_sequence = request->assertion_sequence;
	out->common.ordered_lane = request->ordered_lane;
	out->common.action_node = requester_node;
	out->common.observed_mode = (uint8)PCM_STATE_N;
	out->common.target_mode = (uint8)PCM_STATE_X;
	out->common.sender_connection_generation
		= request->sender_connection_generation;
	out->common.outcome = RESOURCE_X_OUTCOME_OK;
	if (request->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER)
		out->common.flags = RESOURCE_X_COMMON_FLAG_REMOTE_IMAGE_REQUIRED;
	out->common.authority_generation = final_authority_generation;
	grant = &out->body.authority_grant;
	memcpy(grant->source_fence, request->source_fence,
		   sizeof(grant->source_fence));
	grant->final_authority_generation = final_authority_generation;
	grant->source_carrier_generation = request->source_carrier_generation;
	grant->requester_target_generation = request->requester_target_generation;
	grant->page_scn_lsn = request->page_scn_lsn;
	grant->dependency_count = request->dependency_count;
	memcpy(grant->dependencies, request->dependencies,
		   sizeof(grant->dependencies));
	grant->source_proof_crc32c = request->source_proof_crc32c;
	grant->page_checksum = request->page_checksum;
	grant->proof_kind = request->proof_kind;
	grant->source_disposition = request->source_disposition;
	grant->requester_connection_generation
		= requester_connection_generation;
	return RESOURCE_X_APPLY_APPLIED;
}

/* A physical AUTHORITY_GRANT send completion is not requester T3.  While the
 * exact canonical request remains GRANT_COMMITTED, an ordinary R7 replay of
 * the same ASSERT must recreate the same frozen grant after the prior DATA
 * copy has completed.  This changes only the retained physical-send owner:
 * authority, request identity, attempt, formation and session remain fixed. */
static bool
pcm_resource_x_redrive_grant_intent_locked(
	struct GrdEntry *entry, ClusterPcmResourceXMasterState *state,
	ClusterPcmResourceXMasterRequest *request, int32 requester_node)
{
	ClusterPcmResourceXControlIntent pending;
	ClusterPcmResourceXControlIntent *intent;
	ResourceXDecodedFrame grant;
	ResourceXIntentBodyHandle body;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint64 now_us;
	uint16 payload_len = 0;

	Assert(entry != NULL);
	Assert(state != NULL);
	Assert(request != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	intent = &state->grant_intent;
	if (requester_node < 0
		|| requester_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| cluster_node_id < 0
		|| cluster_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| request->phase != RESOURCE_X_MASTER_GRANT_COMMITTED
		|| request->assertion_sequence == 0
		|| request->assertion_sequence == UINT64_MAX
		|| request->final_authority_generation == 0
		|| request->final_authority_generation == UINT64_MAX
		|| state->authority_generation
			!= request->final_authority_generation
		|| (PcmState)pg_atomic_read_u32(&entry->master_state)
			!= PCM_STATE_X
		|| entry->x_holder_node != requester_node
		|| pg_atomic_read_u32(&entry->s_holders_bitmap) != 0
		|| pcm_resource_x_build_authority_grant_locked(
			entry, request, requester_node,
			request->final_authority_generation, &grant)
			!= RESOURCE_X_APPLY_APPLIED)
		return false;

	memset(&pending, 0, sizeof(pending));
	if (!cluster_resource_x_wire_encode(
			RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant, pending.payload,
			sizeof(pending.payload), &payload_len, &reject)
		|| payload_len != RESOURCE_X_PROOF_V1_BYTES)
		return false;
	memset(&body, 0, sizeof(body));
	body.assertion = grant.common.logical_assertion;
	body.owner_generation = request->final_authority_generation;
	body.owner_node = (uint32)cluster_node_id;
	body.owner_kind = RESOURCE_X_INTENT_OWNER_MASTER_GRANT;

	if (intent->slot.state != RESOURCE_X_INTENT_SLOT_EMPTY)
		return (intent->slot.state == RESOURCE_X_INTENT_SLOT_ARMED
				|| intent->slot.state == RESOURCE_X_INTENT_SLOT_STAGED)
			&& intent->slot.logical_generation
				== request->assertion_sequence
			&& intent->slot.authority_generation
				== request->final_authority_generation
			&& intent->slot.destination_node == (uint32)requester_node
			&& intent->slot.payload_bytes == payload_len
			&& intent->slot.kind == RESOURCE_X_WIRE_AUTHORITY_GRANT
			&& memcmp(&intent->slot.body, &body, sizeof(body)) == 0
			&& memcmp(intent->payload, pending.payload, payload_len) == 0;

	now_us = pcm_resource_x_monotonic_us();
	if (now_us == 0 || now_us == UINT64_MAX
		|| !cluster_pcm_lock_resource_x_intent_arm_exact(
			&pending.slot, &body, request->assertion_sequence,
			request->final_authority_generation, now_us,
			(uint32)requester_node, payload_len,
			RESOURCE_X_WIRE_AUTHORITY_GRANT)
		|| !pcm_resource_x_intent_mark_dirty())
		return false;
	*intent = pending;
	return true;
}

static ResourceXApplyResult
pcm_resource_x_commit_grant_locked(struct GrdEntry *entry,
							  ClusterPcmResourceXMasterState *state,
							  ClusterPcmResourceXMasterRequest *request,
							  int32 requester_node)
{
	ResourceXDecodedFrame grant;
	ResourceXIntentBodyHandle body;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint64 final_authority_generation;
	uint64 current_transition_generation;
	uint64 expected_transition_generation;
	uint64 generation_frontier;
	uint64 now_us;
	uint32 remaining_blockers;
	uint32 blocker_count = 0;
	uint16 payload_len = 0;
	PcmLockTransition transition = PCM_TRANS_N_TO_X;
	PcmState current_state;
	uint32 requester_bit;
	uint32 s_holders;
	ResourceXApplyResult build_result;
	bool transition_needed = true;
	Assert(entry != NULL);
	Assert(state != NULL);
	Assert(request != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	current_state = (PcmState)pg_atomic_read_u32(&entry->master_state);
	s_holders = pg_atomic_read_u32(&entry->s_holders_bitmap);
	current_transition_generation
		= pg_atomic_read_u64(&entry->transition_count_local);
	expected_transition_generation = 0;
	if (request->proof_kind < RESOURCE_X_PROOF_KIND_MIN
		|| request->proof_kind > RESOURCE_X_PROOF_KIND_MAX)
		return RESOURCE_X_APPLY_BAD_STATE;
	requester_bit = UINT32_C(1) << (uint32)requester_node;
	if (current_state == PCM_STATE_N) {
		if (entry->x_holder_node != -1 || s_holders != 0) {
			request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
	} else if (current_state == PCM_STATE_S
			   && entry->x_holder_node == -1
			   && s_holders == requester_bit)
		transition = PCM_TRANS_S_TO_X_UPGRADE;
	else if (current_state == PCM_STATE_X
			 && entry->x_holder_node == requester_node
			 && s_holders == 0)
		transition_needed = false;
	else {
		request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	/* Resource-X authority and physical PCM transition generation are distinct
	 * monotone axes.  The request freezes the latter exactly once when it
	 * becomes FIFO head.  Each authenticated blocker consumes exactly one
	 * physical transition before this commit, so the head snapshot plus the
	 * exact blocked bitmap is the complete physical-lineage equation. */
	remaining_blockers = request->blocked_holders_bitmap;
	while (remaining_blockers != 0) {
		remaining_blockers &= remaining_blockers - 1;
		blocker_count++;
	}
	if (request->head_transition_generation == UINT64_MAX
		|| request->head_transition_generation
			> UINT64_MAX - blocker_count) {
		request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	expected_transition_generation
		= request->head_transition_generation + blocker_count;
	if (state->authority_generation == UINT64_MAX
		|| current_transition_generation == UINT64_MAX
		|| state->authority_generation
			   != request->base_authority_generation
		|| request->base_authority_generation
			   > UINT64_MAX - blocker_count
		|| current_transition_generation
			   != expected_transition_generation) {
		request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	/* Every authenticated blocker transition consumes one Resource-X
	 * authority edge before the successor N->X grant consumes the final edge.
	 * Derive that frontier from the frozen Resource-X base and exact blocker
	 * count after the legacy counter has passed the floor/ceiling check above.
	 * Taking Max(base, legacy) loses the pristine-N one-step origin offset when
	 * the two axes become equal after X->N. */
	generation_frontier
		= state->authority_generation + blocker_count;
	if (generation_frontier >= UINT64_MAX - 1) {
		request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	if (state->grant_intent.slot.state != RESOURCE_X_INTENT_SLOT_EMPTY
		|| cluster_node_id < 0
		|| cluster_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT) {
		request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	final_authority_generation = generation_frontier + 1;
	build_result = pcm_resource_x_build_authority_grant_locked(
		entry, request, requester_node, final_authority_generation, &grant);
	if (build_result != RESOURCE_X_APPLY_APPLIED
		|| !cluster_resource_x_wire_encode(
			RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant,
			state->grant_intent.payload, sizeof(state->grant_intent.payload),
			&payload_len, &reject)
		|| payload_len != RESOURCE_X_PROOF_V1_BYTES) {
		memset(&state->grant_intent, 0, sizeof(state->grant_intent));
		request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
		return build_result == RESOURCE_X_APPLY_APPLIED
			? RESOURCE_X_APPLY_RECOVERY_BLOCKED : build_result;
	}
	memset(&body, 0, sizeof(body));
	body.assertion = grant.common.logical_assertion;
	body.owner_generation = final_authority_generation;
	body.owner_node = (uint32)cluster_node_id;
	body.owner_kind = RESOURCE_X_INTENT_OWNER_MASTER_GRANT;
	now_us = pcm_resource_x_monotonic_us();
	if (!cluster_pcm_lock_resource_x_intent_arm_exact(
			&state->grant_intent.slot, &body, request->assertion_sequence,
			final_authority_generation, now_us, (uint32)requester_node,
			payload_len, RESOURCE_X_WIRE_AUTHORITY_GRANT)
		|| !pcm_resource_x_intent_mark_dirty()) {
		memset(&state->grant_intent, 0, sizeof(state->grant_intent));
		request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	if (transition_needed)
		cluster_pcm_transition_apply(entry, transition, requester_node);
	entry->pending_x_requester_node = -1;
	entry->pending_x_since_lsn = 0;
	state->authority_generation = final_authority_generation;
	request->final_authority_generation = final_authority_generation;
	request->phase = RESOURCE_X_MASTER_GRANT_COMMITTED;
	return RESOURCE_X_APPLY_APPLIED;
}

/* Apply one authenticated holder result while the exact master entry is
 * locked.  Keeping the holder transition, blocker bitmap, and final grant in
 * one state-producing edge gives type-18 ingress a single mutation boundary:
 * removing the edge leaves both the holder and queue unchanged. */
static ResourceXApplyResult
pcm_resource_x_apply_blocked_holder_locked(
	struct GrdEntry *entry, ClusterPcmResourceXMasterState *state,
	ClusterPcmResourceXMasterRequest *request, int32 requester_node,
	int32 authenticated_source_node, uint32 source_bit, bool is_x_source,
	bool *broadcast)
{
	static const ClusterPcmResourceXMasterRequest empty_request;
	ClusterPcmResourceXMasterRequest *source_request = NULL;
	bool release_source_request = false;

	Assert(entry != NULL);
	Assert(state != NULL);
	Assert(request != NULL);
	Assert(broadcast != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));

	*broadcast = false;
	if (is_x_source) {
		source_request = &state->requests[authenticated_source_node];
		if (source_request->phase == RESOURCE_X_MASTER_NONE) {
			/* Bootstrap legacy X has no prior Resource-X requester record. */
			if (memcmp(source_request, &empty_request,
					sizeof(*source_request)) != 0) {
				request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
				return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
			}
		} else if ((source_request->phase == RESOURCE_X_MASTER_SETTLED
					|| source_request->phase == RESOURCE_X_MASTER_RELEASED)
				   && source_request != request
				   && source_request->resource_formation
					  == request->resource_formation
				   && source_request->master_session_incarnation
					  == request->master_session_incarnation
				   && source_request->final_authority_generation
					  == request->base_authority_generation)
			release_source_request
				= source_request->phase == RESOURCE_X_MASTER_SETTLED;
		else {
			request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
		cluster_pcm_transition_apply(entry, PCM_TRANS_X_TO_N_DOWNGRADE,
									 authenticated_source_node);
		/* The authenticated type-18 is the exact physical terminal edge for
		 * the old current X requester.  Retire that direct record with the same
		 * entry lock; historical PI bits never participate in this decision. */
		if (release_source_request)
			source_request->phase = RESOURCE_X_MASTER_RELEASED;
	} else if ((pg_atomic_read_u32(&entry->s_holders_bitmap) & source_bit) != 0)
		cluster_pcm_transition_apply(entry, PCM_TRANS_S_TO_N_INVALIDATE,
								 authenticated_source_node);
	request->blocked_holders_bitmap |= source_bit;
	if (request->blocked_holders_bitmap == request->incompatible_holders_bitmap) {
		if (request->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER) {
			ResourceXApplyResult commit_result
				= pcm_resource_x_commit_grant_locked(entry, state, request,
											 requester_node);

			*broadcast = commit_result == RESOURCE_X_APPLY_APPLIED;
		} else
			request->phase = RESOURCE_X_MASTER_WAIT_PROOF;
	}
	return request->phase == RESOURCE_X_MASTER_RECOVERY_BLOCKED
		? RESOURCE_X_APPLY_RECOVERY_BLOCKED : RESOURCE_X_APPLY_APPLIED;
}

static bool
pcm_resource_x_post_grant_blocker_state_exact(
	const ClusterPcmResourceXMasterRequest *request, uint32 source_bit)
{
	Assert(request != NULL);
	Assert(request->phase == RESOURCE_X_MASTER_GRANT_COMMITTED);

	return request->blocked_holders_bitmap
			   == request->incompatible_holders_bitmap
		&& (request->blocked_holders_bitmap & source_bit) != 0;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_blocked_to_n_exact(const ResourceXDecodedFrame *blocked,
											int32 authenticated_source_node,
											ResourceXMasterSnapshot *out)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXMasterRequest *request;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	uint32 source_bit;
	int32 head_node = -1;
	int32 requester_node;
	bool direct_x_source;
	bool selected_s_source;
	bool broadcast = false;

	if (out != NULL) {
		memset(out, 0, sizeof(*out));
		out->source_node = -1;
	}
	if (out == NULL || blocked == NULL
		|| blocked->kind != RESOURCE_X_WIRE_BLOCKED_TO_N
		|| !resource_x_assertion_valid(&blocked->common.logical_assertion)
		|| authenticated_source_node < 0
		|| authenticated_source_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| blocked->common.action_node != authenticated_source_node
		|| blocked->common.outcome == RESOURCE_X_OUTCOME_NONE
		|| blocked->common.resource_formation == 0)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&blocked->common.logical_assertion.resource,
			false, &entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	requester_node = blocked->common.logical_assertion.requester_node;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	request = &state->requests[requester_node];
	(void)pcm_resource_x_master_head(state, &head_node);
	if (request->phase == RESOURCE_X_MASTER_NONE) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_NOT_FOUND;
	}
	if (head_node != requester_node
		|| blocked->common.base_authority_generation
			   != request->base_authority_generation
		|| blocked->common.resource_formation != request->resource_formation
		|| blocked->common.master_session_incarnation
			   != request->master_session_incarnation
		|| blocked->common.assertion_sequence != request->assertion_sequence
		|| blocked->common.authority_generation
			   != request->base_authority_generation) {
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_STALE;
	}
	source_bit = UINT32_C(1) << (uint32)authenticated_source_node;
	if ((request->incompatible_holders_bitmap & source_bit) == 0
		|| (request->phase != RESOURCE_X_MASTER_WAIT_BLOCKERS
			&& request->phase != RESOURCE_X_MASTER_GRANT_COMMITTED)) {
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_BAD_STATE;
	}
	if (request->phase == RESOURCE_X_MASTER_GRANT_COMMITTED
		&& !pcm_resource_x_post_grant_blocker_state_exact(request,
													 source_bit)) {
		request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	if ((request->blocked_holders_bitmap & source_bit) != 0) {
		ResourceXApplyResult result;

		if (!blocked->blocked_has_remote_proof)
			result = request->source_node != authenticated_source_node
				&& blocked->common.observed_mode == (uint8)PCM_STATE_S
				&& blocked->common.target_mode == (uint8)PCM_STATE_N
				? RESOURCE_X_APPLY_DUPLICATE : RESOURCE_X_APPLY_STALE;
		else
			result
				= request->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER
					&& pcm_resource_x_remote_proof_valid(blocked)
					&& request->source_node == authenticated_source_node
					&& (blocked->common.observed_mode == (uint8)PCM_STATE_X
						|| blocked->common.observed_mode == (uint8)PCM_STATE_S)
					&& request->source_carrier_generation
						   == blocked->body.blocked_to_n.source_carrier_generation
					&& request->requester_target_generation
						   == blocked->body.blocked_to_n.requester_target_generation
					&& request->page_scn_lsn
						   == blocked->body.blocked_to_n.page_scn_lsn
					&& request->dependency_count
						   == blocked->body.blocked_to_n.dependency_count
					&& request->proof_flags
						   == blocked->body.blocked_to_n.proof_flags
					&& request->source_proof_crc32c
						   == blocked->body.blocked_to_n.source_proof_crc32c
					&& request->page_checksum
						   == blocked->body.blocked_to_n.page_checksum
					&& request->holder_connection_generation
						   == blocked->body.blocked_to_n.holder_connection_generation
					&& request->acting_formation
						   == blocked->body.blocked_to_n.acting_formation
					&& memcmp(request->source_fence,
							  blocked->body.blocked_to_n.source_fence,
							  sizeof(request->source_fence)) == 0
					&& memcmp(request->dependencies,
							  blocked->body.blocked_to_n.dependencies,
							  sizeof(request->dependencies)) == 0
					? RESOURCE_X_APPLY_DUPLICATE : RESOURCE_X_APPLY_STALE;

		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return result;
	}
	direct_x_source
		= (PcmState)pg_atomic_read_u32(&entry->master_state) == PCM_STATE_X
		&& entry->x_holder_node == authenticated_source_node;
	selected_s_source
		= request->source_node == authenticated_source_node
		&& (PcmState)pg_atomic_read_u32(&entry->master_state) == PCM_STATE_S
		&& (pg_atomic_read_u32(&entry->s_holders_bitmap) & source_bit) != 0
		&& blocked->common.observed_mode == (uint8)PCM_STATE_S;
	if (blocked->blocked_has_remote_proof
		&& !direct_x_source && !selected_s_source) {
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_BAD_STATE;
	}
	if ((direct_x_source || selected_s_source)
		&& (!pcm_resource_x_remote_proof_valid(blocked)
			|| blocked->common.observed_mode
				!= (uint8)(direct_x_source ? PCM_STATE_X : PCM_STATE_S)
			|| blocked->common.target_mode != (uint8)PCM_STATE_N)) {
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_BAD_STATE;
	}
	if (!blocked->blocked_has_remote_proof
		&& (request->source_node == authenticated_source_node
			|| blocked->common.observed_mode != (uint8)PCM_STATE_S
			|| blocked->common.target_mode != (uint8)PCM_STATE_N)) {
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_BAD_STATE;
	}
	if (blocked->blocked_has_remote_proof) {
		if (!pcm_resource_x_remote_proof_valid(blocked)) {
			pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
										   request, out);
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return RESOURCE_X_APPLY_INVALID;
		}
		request->proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
		request->source_disposition
			= RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
		if (direct_x_source)
			request->source_node = authenticated_source_node;
		memcpy(request->source_fence,
			   blocked->body.blocked_to_n.source_fence,
			   sizeof(request->source_fence));
		request->source_carrier_generation
			= blocked->body.blocked_to_n.source_carrier_generation;
		request->requester_target_generation
			= blocked->body.blocked_to_n.requester_target_generation;
		request->page_scn_lsn = blocked->body.blocked_to_n.page_scn_lsn;
		request->dependency_count
			= blocked->body.blocked_to_n.dependency_count;
		memcpy(request->dependencies, blocked->body.blocked_to_n.dependencies,
			   sizeof(request->dependencies));
		request->source_proof_crc32c
			= blocked->body.blocked_to_n.source_proof_crc32c;
		request->page_checksum = blocked->body.blocked_to_n.page_checksum;
		request->holder_connection_generation
			= blocked->body.blocked_to_n.holder_connection_generation;
		request->acting_formation = blocked->body.blocked_to_n.acting_formation;
		request->proof_flags = blocked->body.blocked_to_n.proof_flags;
	}

	result = pcm_resource_x_apply_blocked_holder_locked(
		entry, state, request, requester_node, authenticated_source_node,
		source_bit, direct_x_source, &broadcast);
	pcm_resource_x_master_snapshot(&entry->tag, requester_node, state, request, out);
	LWLockRelease(&entry->entry_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

static bool
pcm_resource_x_common_matches_request(const ResourceXDecodedCommon *common,
									 const ClusterPcmResourceXMasterRequest *request)
{
	return common->base_authority_generation
			   == request->base_authority_generation
		&& common->authority_generation == request->base_authority_generation
		&& common->resource_formation == request->resource_formation
		&& common->master_session_incarnation
			   == request->master_session_incarnation
		&& common->assertion_sequence == request->assertion_sequence;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_local_proof_exact(const ResourceXDecodedFrame *local_proof,
											  int32 authenticated_source_node,
											  ResourceXMasterSnapshot *out)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXMasterRequest *request;
	const ResourceXDecodedLocalProof *proof;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	int32 head_node = -1;
	int32 requester_node;
	bool broadcast = false;

	if (out != NULL) {
		memset(out, 0, sizeof(*out));
		out->source_node = -1;
	}
	if (out == NULL || local_proof == NULL
		|| local_proof->kind != RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION
		|| !resource_x_assertion_valid(&local_proof->common.logical_assertion)
		|| authenticated_source_node
			   != local_proof->common.logical_assertion.requester_node
		|| local_proof->common.action_node != authenticated_source_node
		|| local_proof->common.outcome == RESOURCE_X_OUTCOME_NONE
		|| local_proof->common.target_mode != (uint8)PCM_STATE_X)
		return RESOURCE_X_APPLY_INVALID;
	proof = &local_proof->body.local_proof;
	if (proof->local_holder_authority_generation == 0
		|| proof->requester_target_generation == 0
		|| proof->dependency_count > RESOURCE_X_DEPENDENCY_MAX
		|| proof->requester_connection_generation == 0
		|| proof->local_proof_generation == 0)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&local_proof->common.logical_assertion.resource,
			false, &entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	requester_node = local_proof->common.logical_assertion.requester_node;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	request = &state->requests[requester_node];
	(void)pcm_resource_x_master_head(state, &head_node);
	if (request->phase == RESOURCE_X_MASTER_NONE) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_NOT_FOUND;
	}
	if (head_node != requester_node
		|| !pcm_resource_x_common_matches_request(&local_proof->common, request)) {
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_STALE;
	}
	if (request->phase == RESOURCE_X_MASTER_GRANT_COMMITTED) {
		result = request->proof_kind == RESOURCE_X_PROOF_LOCAL_IMAGE
				&& request->local_holder_authority_generation
					   == proof->local_holder_authority_generation
				&& request->requester_target_generation
					   == proof->requester_target_generation
				&& request->page_scn_lsn == proof->page_scn_lsn
				&& request->dependency_count == proof->dependency_count
				&& request->dependency_vector_crc32c
					   == proof->dependency_vector_crc32c
				&& request->page_checksum == proof->page_checksum
				&& request->source_proof_crc32c
					   == proof->local_image_proof_crc32c
				&& request->requester_connection_generation
					   == proof->requester_connection_generation
				&& request->local_proof_generation
					   == proof->local_proof_generation
				? RESOURCE_X_APPLY_DUPLICATE : RESOURCE_X_APPLY_STALE;
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return result;
	}
	if (request->phase != RESOURCE_X_MASTER_WAIT_PROOF
		|| request->incompatible_holders_bitmap
			   != request->blocked_holders_bitmap) {
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_BAD_STATE;
	}
	request->proof_kind = RESOURCE_X_PROOF_LOCAL_IMAGE;
	request->source_disposition = RESOURCE_X_DISPOSITION_LOCAL_IMAGE;
	request->source_node = requester_node;
	request->local_holder_authority_generation
		= proof->local_holder_authority_generation;
	request->requester_target_generation = proof->requester_target_generation;
	request->page_scn_lsn = proof->page_scn_lsn;
	request->dependency_count = proof->dependency_count;
	request->dependency_vector_crc32c = proof->dependency_vector_crc32c;
	request->page_checksum = proof->page_checksum;
	request->source_proof_crc32c = proof->local_image_proof_crc32c;
	request->requester_connection_generation
		= proof->requester_connection_generation;
	request->local_proof_generation = proof->local_proof_generation;
	result = pcm_resource_x_commit_grant_locked(entry, state, request,
												 requester_node);
	broadcast = result == RESOURCE_X_APPLY_APPLIED;
	pcm_resource_x_master_snapshot(&entry->tag, requester_node, state, request, out);
	LWLockRelease(&entry->entry_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_durable_proof_exact(const ResourceXDurableProof *durable_proof,
												ResourceXMasterSnapshot *out)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXMasterRequest *request;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	int32 head_node = -1;
	int32 requester_node;
	bool broadcast = false;

	if (out != NULL) {
		memset(out, 0, sizeof(*out));
		out->source_node = -1;
	}
	if (out == NULL || durable_proof == NULL
		|| !resource_x_assertion_valid(&durable_proof->assertion)
		|| durable_proof->base_authority_generation == 0
		|| durable_proof->resource_formation == 0
		|| durable_proof->master_session_incarnation == 0
		|| durable_proof->assertion_sequence == 0
		|| durable_proof->requester_target_generation == 0)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&durable_proof->assertion.resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	requester_node = durable_proof->assertion.requester_node;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	request = &state->requests[requester_node];
	(void)pcm_resource_x_master_head(state, &head_node);
	if (request->phase == RESOURCE_X_MASTER_NONE) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_NOT_FOUND;
	}
	if (head_node != requester_node
		|| request->base_authority_generation
			   != durable_proof->base_authority_generation
		|| request->resource_formation != durable_proof->resource_formation
		|| request->master_session_incarnation
			   != durable_proof->master_session_incarnation
		|| request->assertion_sequence != durable_proof->assertion_sequence) {
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_STALE;
	}
	if (request->phase == RESOURCE_X_MASTER_GRANT_COMMITTED) {
		result = request->proof_kind == RESOURCE_X_PROOF_DURABLE_STORAGE
				&& request->requester_target_generation
					   == durable_proof->requester_target_generation
				&& request->page_scn_lsn == durable_proof->page_scn_lsn
				&& request->page_checksum == durable_proof->page_checksum
				&& request->source_proof_crc32c
					   == durable_proof->source_proof_crc32c
				? RESOURCE_X_APPLY_DUPLICATE : RESOURCE_X_APPLY_STALE;
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return result;
	}
	if (request->phase != RESOURCE_X_MASTER_WAIT_PROOF
		|| request->incompatible_holders_bitmap
			   != request->blocked_holders_bitmap
		|| (PcmState)pg_atomic_read_u32(&entry->master_state) != PCM_STATE_N
		|| entry->x_holder_node != -1
		|| pg_atomic_read_u32(&entry->s_holders_bitmap) != 0
		|| pg_atomic_read_u32(&entry->pi_holders_bitmap) != 0) {
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_BAD_STATE;
	}
	request->proof_kind = RESOURCE_X_PROOF_DURABLE_STORAGE;
	request->source_disposition = RESOURCE_X_DISPOSITION_DURABLE_STORAGE;
	request->source_node = -1;
	request->requester_target_generation
		= durable_proof->requester_target_generation;
	request->page_scn_lsn = durable_proof->page_scn_lsn;
	request->page_checksum = durable_proof->page_checksum;
	request->source_proof_crc32c = durable_proof->source_proof_crc32c;
	result = pcm_resource_x_commit_grant_locked(entry, state, request,
												 requester_node);
	broadcast = result == RESOURCE_X_APPLY_APPLIED;
	pcm_resource_x_master_snapshot(&entry->tag, requester_node, state, request, out);
	LWLockRelease(&entry->entry_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_master_snapshot_exact(
	const ResourceXAssertion *assertion, ResourceXMasterSnapshot *out)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXMasterRequest *request;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	int32 requester_node;

	if (out != NULL) {
		memset(out, 0, sizeof(*out));
		out->source_node = -1;
	}
	if (out == NULL || !resource_x_assertion_valid(assertion))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	requester_node = assertion->requester_node;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	request = &state->requests[requester_node];
	if (request->phase == RESOURCE_X_MASTER_NONE) {
		if (pcm_resource_x_terminal_tombstone_valid(
				&state->terminal_tombstone)
			&& state->terminal_tombstone.requester_node == requester_node) {
			pcm_resource_x_terminal_tombstone_snapshot(
				&entry->tag, state, &state->terminal_tombstone, out);
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return RESOURCE_X_APPLY_APPLIED;
		}
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_NOT_FOUND;
	}
	pcm_resource_x_master_snapshot(&entry->tag, requester_node, state, request,
								   out);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return RESOURCE_X_APPLY_APPLIED;
}

/*
 * A granted holder remains the physical current-X carrier while a different
 * Resource-X requester is already present in the same resource FIFO.  Legacy
 * read-scache may still ship a one-shot read image, but it must not turn that
 * X into S before the successor's exact BLOCK_TO_N has frozen its type-18/
 * type-15 carrier pair.  This is a read-only classification over the existing
 * per-resource request array; it creates neither holder authority nor a new
 * registry.
 */
ResourceXApplyResult
cluster_pcm_lock_resource_x_current_x_successor_exact(
	const BufferTag *tag, int32 holder_node, bool *preserve_current_x_out)
{
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result = RESOURCE_X_APPLY_APPLIED;
	int32 requester_node;

	if (preserve_current_x_out != NULL)
		*preserve_current_x_out = false;
	if (tag == NULL || preserve_current_x_out == NULL
		|| holder_node < 0 || holder_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(tag, false, &entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;

	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL || state->authority_generation == 0
		|| state->authority_generation == UINT64_MAX)
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	else if ((PcmState)pg_atomic_read_u32(&entry->master_state)
			 != PCM_STATE_X
			 || entry->x_holder_node != holder_node)
		result = RESOURCE_X_APPLY_BAD_STATE;
	else {
		for (requester_node = 0;
			 requester_node < RESOURCE_X_PROTOCOL_NODE_LIMIT;
			 requester_node++) {
			ClusterPcmResourceXMasterRequest *request;

			if (requester_node == holder_node)
				continue;
			request = &state->requests[requester_node];
			if (request->phase == RESOURCE_X_MASTER_NONE
				|| request->phase == RESOURCE_X_MASTER_RELEASED)
				continue;
			if (request->phase == RESOURCE_X_MASTER_SETTLED
				|| request->phase == RESOURCE_X_MASTER_RECOVERY_BLOCKED
				|| request->phase > RESOURCE_X_MASTER_RELEASED
				|| request->base_authority_generation == 0
				|| request->base_authority_generation == UINT64_MAX
				|| request->resource_formation == 0
				|| request->master_session_incarnation == 0
				|| request->assertion_sequence == 0
				|| request->assertion_sequence == UINT64_MAX
				|| request->enqueue_order == 0
				|| request->enqueue_order == UINT64_MAX) {
				result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
				break;
			}
			*preserve_current_x_out = true;
			break;
		}
	}
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

static bool
pcm_resource_x_settled_snapshot_matches_request(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	const ResourceXMasterSnapshot *settled,
	const ClusterPcmResourceXMasterRequest *request, int32 requester_node)
{
	return resource_x_assertion_valid(assertion) && settled != NULL
		&& request != NULL
		&& resource_x_assertion_equal(assertion, &settled->assertion)
		&& assertion->requester_node == requester_node
		&& assertion_sequence != 0
		&& assertion_sequence != UINT64_MAX
		&& assertion_sequence == request->assertion_sequence
		&& settled->base_authority_generation
			== request->base_authority_generation
		&& settled->resource_formation == request->resource_formation
		&& settled->master_session_incarnation
			== request->master_session_incarnation
		&& settled->assertion_sequence == request->assertion_sequence
		&& settled->final_authority_generation
			== request->final_authority_generation
		&& settled->source_carrier_generation
			== request->source_carrier_generation
		&& settled->requester_target_generation
			== request->requester_target_generation
		&& settled->incompatible_holders_bitmap
			== request->incompatible_holders_bitmap
		&& settled->blocked_holders_bitmap
			== request->blocked_holders_bitmap
		&& settled->source_node == request->source_node
		&& settled->phase == RESOURCE_X_MASTER_SETTLED
		&& request->phase == RESOURCE_X_MASTER_SETTLED
		&& settled->proof_kind == request->proof_kind
		&& settled->source_disposition == request->source_disposition;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_settled_retire_exact(
	const ResourceXAssertion *assertion, uint64 assertion_sequence,
	const ResourceXMasterSnapshot *settled)
{
	ClusterPcmResourceXBootstrapReceipt *receipt;
	ClusterPcmResourceXTerminalTombstone *tombstone;
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXMasterRequest *request;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	int32 requester_node;

	if (!resource_x_assertion_valid(assertion) || settled == NULL
		|| assertion_sequence == 0 || assertion_sequence == UINT64_MAX)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	requester_node = assertion->requester_node;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	tombstone = &state->terminal_tombstone;
	receipt = &state->bootstrap_receipts[requester_node];
	request = &state->requests[requester_node];
	if (request->phase == RESOURCE_X_MASTER_NONE) {
		ResourceXApplyResult result
			= pcm_resource_x_terminal_tombstone_valid(tombstone)
				&& tombstone->requester_node == requester_node
				&& pcm_resource_x_settled_snapshot_matches_request(
					assertion, assertion_sequence, settled,
					&tombstone->request, requester_node)
				? RESOURCE_X_APPLY_DUPLICATE
				: RESOURCE_X_APPLY_STALE;

		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return result;
	}
	if (!pcm_resource_x_settled_snapshot_matches_request(
			assertion, assertion_sequence, settled, request, requester_node)) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_STALE;
	}
	if (!pcm_resource_x_bootstrap_receipt_valid(receipt)) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	tombstone->request = *request;
	tombstone->requester_node = requester_node;
	tombstone->valid = 1;
	if (assertion_sequence > receipt->highest_attempt_floor)
		receipt->highest_attempt_floor = assertion_sequence;
	pcm_resource_x_bootstrap_receipt_invalidate(receipt);
	memset(request, 0, sizeof(*request));
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return RESOURCE_X_APPLY_APPLIED;
}

static void
pcm_resource_x_reclaim_witness(const BufferTag *tag, int32 requester_node,
							   const ClusterPcmResourceXMasterRequest *request,
							   ResourceXReclaimWitness *out)
{
	Assert(tag != NULL);
	Assert(request != NULL);
	Assert(out != NULL);
	memset(out, 0, sizeof(*out));
	out->assertion.resource = *tag;
	out->assertion.requester_node = requester_node;
	out->base_authority_generation = request->base_authority_generation;
	out->resource_formation = request->resource_formation;
	out->master_session_incarnation = request->master_session_incarnation;
	out->assertion_sequence = request->assertion_sequence;
	out->enqueue_order = request->enqueue_order;
	out->final_authority_generation = request->final_authority_generation;
	out->successor_node = -1;
	out->previous_phase = request->phase;
	out->source_evidence_preserved
		= request->proof_kind != 0 || request->source_node >= 0
			|| request->source_carrier_generation != 0
			|| request->requester_target_generation != 0
			|| request->final_authority_generation != 0;
}

static ResourceXReclaimResult
pcm_resource_x_reclaim_requester_locked(
	struct GrdEntry *entry, ClusterPcmResourceXMasterState *state,
	int32 dead_node, uint64 dead_formation, ResourceXReclaimWitness *out,
	bool *broadcast_out)
{
	ClusterPcmResourceXMasterRequest *request;
	ClusterPcmResourceXMasterRequest *successor = NULL;
	ResourceXReclaimResult result = RESOURCE_X_RECLAIM_NONE;
	int32 head_node = -1;
	int32 successor_node = -1;

	Assert(entry != NULL);
	Assert(state != NULL);
	Assert(out != NULL);
	Assert(broadcast_out != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	*broadcast_out = false;
	memset(out, 0, sizeof(*out));
	out->successor_node = -1;
	out->result = RESOURCE_X_RECLAIM_NONE;
	if (dead_node < 0 || dead_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return RESOURCE_X_RECLAIM_NONE;
	request = &state->requests[dead_node];
	if (request->phase == RESOURCE_X_MASTER_NONE
		|| request->phase == RESOURCE_X_MASTER_RELEASED
		|| request->resource_formation != dead_formation)
		return RESOURCE_X_RECLAIM_NONE;
	(void)pcm_resource_x_master_head(state, &head_node);
	pcm_resource_x_reclaim_witness(&entry->tag, dead_node, request, out);
	out->was_head = head_node == dead_node ? 1 : 0;

	if (head_node != dead_node) {
		if (request->phase == RESOURCE_X_MASTER_QUEUED) {
			memset(request, 0, sizeof(*request));
			result = RESOURCE_X_RECLAIM_NONHEAD;
		}
		else {
			request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
			result = RESOURCE_X_RECLAIM_ORPHAN_BLOCKED;
		}
	}
	else if ((request->phase == RESOURCE_X_MASTER_QUEUED
			  || request->phase == RESOURCE_X_MASTER_WAIT_BLOCKERS
			  || request->phase == RESOURCE_X_MASTER_WAIT_PROOF)
			 && request->final_authority_generation == 0
			 && request->proof_kind == 0) {
		memset(request, 0, sizeof(*request));
		successor = pcm_resource_x_start_head_locked(entry, state,
										  &successor_node);
		result = RESOURCE_X_RECLAIM_HEAD_SUCCESSOR_STARTED;
		*broadcast_out = true;
		if (successor != NULL) {
			out->successor_node = successor_node;
			out->successor_phase = successor->phase;
			out->successor_assertion_sequence = successor->assertion_sequence;
			out->successor_enqueue_order = successor->enqueue_order;
		}
	}
	else {
		request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
		result = RESOURCE_X_RECLAIM_ORPHAN_BLOCKED;
	}
	out->result = (uint32)result;
	return result;
}

ResourceXReclaimResult
cluster_pcm_lock_resource_x_reclaim_requester_exact(
	const BufferTag *tag, int32 dead_node, uint64 dead_formation,
	ResourceXReclaimWitness *out)
{
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXReclaimResult result;
	struct GrdEntry *entry;
	bool broadcast = false;

	if (out != NULL) {
		memset(out, 0, sizeof(*out));
		out->successor_node = -1;
		out->result = RESOURCE_X_RECLAIM_NONE;
	}
	if (tag == NULL || out == NULL || dead_node < 0
		|| dead_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT || dead_formation == 0
		|| dead_formation == UINT64_MAX || ClusterPcm == NULL
		|| pg_atomic_read_u32(&ClusterPcm->resource_x_gate_phase)
			   != RESOURCE_X_GATE_FROZEN
		|| pg_atomic_read_u64(&ClusterPcm->resource_x_reconfig_old_formation)
			   != dead_formation
		|| (pg_atomic_read_u32(
				&ClusterPcm->resource_x_reconfig_dead_requester_bitmap)
			& (UINT32_C(1) << (uint32)dead_node)) == 0)
		return RESOURCE_X_RECLAIM_NONE;

	if (!pcm_entry_ref_acquire(tag, false, &entry_ref, &acquire_result))
		return RESOURCE_X_RECLAIM_NONE;
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_RECLAIM_NONE;
	}
	result = pcm_resource_x_reclaim_requester_locked(
		entry, state, dead_node, dead_formation, out, &broadcast);
	LWLockRelease(&entry->entry_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_authority_grant_exact(
	const ResourceXAssertion *assertion, ResourceXDecodedFrame *out)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXMasterRequest *request;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXApplyResult result;
	struct GrdEntry *entry;
	int32 requester_node;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || !resource_x_assertion_valid(assertion))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	requester_node = assertion->requester_node;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto authority_grant_done;
	}
	request = &state->requests[requester_node];
	if (request->phase == RESOURCE_X_MASTER_NONE) {
		result = RESOURCE_X_APPLY_NOT_FOUND;
		goto authority_grant_done;
	}
	if (request->phase != RESOURCE_X_MASTER_GRANT_COMMITTED) {
		result = RESOURCE_X_APPLY_BAD_STATE;
		goto authority_grant_done;
	}
	result = pcm_resource_x_build_authority_grant_locked(
		entry, request, requester_node, request->final_authority_generation, out);

authority_grant_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
	const ResourceXAssertion *assertion, ResourceXIntentSlot *slot_out,
	void *payload_out, uint16 payload_capacity)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXMasterRequest *request;
	ClusterPcmResourceXControlIntent *intent;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXApplyResult result;
	struct GrdEntry *entry;
	int32 requester_node;

	if (slot_out != NULL)
		memset(slot_out, 0, sizeof(*slot_out));
	if (!resource_x_assertion_valid(assertion) || slot_out == NULL
		|| payload_out == NULL || payload_capacity < RESOURCE_X_PROOF_V1_BYTES)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	requester_node = assertion->requester_node;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto grant_intent_snapshot_done;
	}
	request = &state->requests[requester_node];
	intent = &state->grant_intent;
	if (request->phase == RESOURCE_X_MASTER_NONE
		|| intent->slot.state == RESOURCE_X_INTENT_SLOT_EMPTY) {
		result = RESOURCE_X_APPLY_NOT_FOUND;
		goto grant_intent_snapshot_done;
	}
	if (!resource_x_assertion_equal(assertion, &intent->slot.body.assertion)) {
		result = RESOURCE_X_APPLY_STALE;
		goto grant_intent_snapshot_done;
	}
	if (request->phase != RESOURCE_X_MASTER_GRANT_COMMITTED
		|| intent->slot.state > RESOURCE_X_INTENT_SLOT_STAGED
		|| !pcm_resource_x_intent_body_valid(&intent->slot.body)
		|| intent->slot.logical_generation != request->assertion_sequence
		|| intent->slot.authority_generation
			   != request->final_authority_generation
		|| intent->slot.destination_node != (uint32)requester_node
		|| intent->slot.payload_bytes != RESOURCE_X_PROOF_V1_BYTES
		|| intent->slot.kind != RESOURCE_X_WIRE_AUTHORITY_GRANT
		|| intent->slot.body.owner_generation
			   != request->final_authority_generation
		|| intent->slot.body.owner_kind
			   != RESOURCE_X_INTENT_OWNER_MASTER_GRANT) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto grant_intent_snapshot_done;
	}
	*slot_out = intent->slot;
	memcpy(payload_out, intent->payload, intent->slot.payload_bytes);
	result = RESOURCE_X_APPLY_APPLIED;

grant_intent_snapshot_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

static ResourceXIntentSlot *
pcm_resource_x_grant_intent_lock_exact(
	const ResourceXIntentSlot *expected, PcmEntryRef *entry_ref_out)
{
	ClusterPcmResourceXMasterState *state;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;

	Assert(entry_ref_out != NULL);
	memset(entry_ref_out, 0, sizeof(*entry_ref_out));
	if (expected == NULL || !pcm_resource_x_intent_body_valid(&expected->body)
		|| expected->body.owner_kind
			   != RESOURCE_X_INTENT_OWNER_MASTER_GRANT
		|| expected->kind != RESOURCE_X_WIRE_AUTHORITY_GRANT
		|| expected->payload_bytes != RESOURCE_X_PROOF_V1_BYTES)
		return NULL;
	if (!pcm_entry_ref_acquire(&expected->body.assertion.resource, false,
			entry_ref_out, &acquire_result))
		return NULL;
	entry = entry_ref_out->entry;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(entry_ref_out);
		return NULL;
	}
	return &state->grant_intent.slot;
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_grant_intent_not_admitted_exact(
	const ResourceXIntentSlot *expected, uint64 now_us)
{
	PcmEntryRef entry_ref;
	ResourceXIntentSlot *slot;
	ResourceXIntentResult result;

	slot = pcm_resource_x_grant_intent_lock_exact(expected, &entry_ref);
	if (slot == NULL)
		return RESOURCE_X_INTENT_STALE;
	result = cluster_pcm_lock_resource_x_intent_not_admitted_exact(
		slot, expected, now_us);
	if (result == RESOURCE_X_INTENT_NOT_ADMITTED
		&& !pcm_resource_x_intent_mark_dirty())
		result = RESOURCE_X_INTENT_STALE;
	LWLockRelease(&entry_ref.entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_grant_intent_stage_exact(
	const ResourceXIntentSlot *expected, uint64 now_us)
{
	PcmEntryRef entry_ref;
	ResourceXIntentSlot *slot;
	ResourceXIntentResult result;

	slot = pcm_resource_x_grant_intent_lock_exact(expected, &entry_ref);
	if (slot == NULL)
		return RESOURCE_X_INTENT_STALE;
	result = cluster_pcm_lock_resource_x_intent_stage_exact(
		slot, expected, now_us);
	LWLockRelease(&entry_ref.entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_grant_intent_hard_rearm_exact(
	const ResourceXIntentSlot *expected, uint64 now_us)
{
	PcmEntryRef entry_ref;
	ResourceXIntentSlot *slot;
	ResourceXIntentResult result;

	slot = pcm_resource_x_grant_intent_lock_exact(expected, &entry_ref);
	if (slot == NULL)
		return RESOURCE_X_INTENT_STALE;
	result = cluster_pcm_lock_resource_x_intent_hard_rearm_exact(
		slot, expected, now_us);
	if (result == RESOURCE_X_INTENT_HARD_REARMED
		&& !pcm_resource_x_intent_mark_dirty())
		result = RESOURCE_X_INTENT_STALE;
	LWLockRelease(&entry_ref.entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

bool
cluster_pcm_lock_resource_x_grant_intent_complete_exact(
	const ResourceXIntentSlot *expected)
{
	PcmEntryRef entry_ref;
	ResourceXIntentSlot *slot;
	bool completed;

	slot = pcm_resource_x_grant_intent_lock_exact(expected, &entry_ref);
	if (slot == NULL)
		return false;
	completed = cluster_pcm_lock_resource_x_intent_complete_exact(slot, expected);
	LWLockRelease(&entry_ref.entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return completed;
}

static bool
pcm_resource_x_intent_identity_equal(const ResourceXIntentSlot *left,
									 const ResourceXIntentSlot *right)
{
	return left != NULL && right != NULL
		&& left->logical_generation == right->logical_generation
		&& left->authority_generation == right->authority_generation
		&& left->first_armed_us == right->first_armed_us
		&& left->destination_node == right->destination_node
		&& left->payload_bytes == right->payload_bytes
		&& left->kind == right->kind
		&& memcmp(&left->body, &right->body, sizeof(left->body)) == 0;
}

static ResourceXIntentSlot *
pcm_resource_x_outbound_intent_lock_exact(
	const ResourceXIntentSlot *expected, PcmEntryRef *entry_ref_out,
	uint8 **payload_out)
{
	ClusterPcmResourceXMasterState *state;
	PcmEntryAcquireResult acquire_result;
	ResourceXIntentSlot *slot = NULL;
	struct GrdEntry *entry;

	Assert(entry_ref_out != NULL);
	Assert(payload_out != NULL);
	memset(entry_ref_out, 0, sizeof(*entry_ref_out));
	*payload_out = NULL;
	if (expected == NULL
		|| !pcm_resource_x_intent_body_valid(&expected->body))
		return NULL;
	switch ((ResourceXIntentOwnerKind)expected->body.owner_kind) {
	case RESOURCE_X_INTENT_OWNER_MASTER_BLOCK:
		if (expected->kind != RESOURCE_X_WIRE_BLOCK_TO_N
			|| expected->payload_bytes != RESOURCE_X_CONTROL_V1_BYTES
			|| expected->destination_node != expected->body.owner_index)
			return NULL;
		break;
	case RESOURCE_X_INTENT_OWNER_HOLDER_STATUS:
		if (expected->kind != RESOURCE_X_WIRE_BLOCKED_TO_N
			|| (expected->payload_bytes != RESOURCE_X_CONTROL_V1_BYTES
				&& expected->payload_bytes != RESOURCE_X_PROOF_V1_BYTES))
			return NULL;
		break;
	case RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE:
		if (expected->kind != RESOURCE_X_WIRE_IMAGE_ENVELOPE
			|| expected->payload_bytes != RESOURCE_X_IMAGE_V1_BYTES)
			return NULL;
		break;
	case RESOURCE_X_INTENT_OWNER_MASTER_GRANT:
		if (expected->kind != RESOURCE_X_WIRE_AUTHORITY_GRANT
			|| expected->payload_bytes != RESOURCE_X_PROOF_V1_BYTES)
			return NULL;
		break;
	case RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT:
		if (expected->kind != RESOURCE_X_WIRE_INSTALL_SETTLEMENT
			|| expected->payload_bytes != RESOURCE_X_SHORT_V1_BYTES)
			return NULL;
		break;
	case RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE:
		if (expected->kind != RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
			|| expected->payload_bytes != RESOURCE_X_PROOF_V1_BYTES)
			return NULL;
		break;
	default:
		return NULL;
	}
	if (!pcm_entry_ref_acquire(&expected->body.assertion.resource, false,
			entry_ref_out, &acquire_result))
		return NULL;
	entry = entry_ref_out->entry;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(entry_ref_out);
		return NULL;
	}
	switch ((ResourceXIntentOwnerKind)expected->body.owner_kind) {
	case RESOURCE_X_INTENT_OWNER_MASTER_BLOCK:
		slot = &state->block_intents[expected->body.owner_index].slot;
		*payload_out
			= state->block_intents[expected->body.owner_index].payload;
		break;
	case RESOURCE_X_INTENT_OWNER_HOLDER_STATUS:
		slot = &state->holder_status_intent.slot;
		*payload_out = state->holder_status_intent.payload;
		break;
	case RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE:
		slot = &state->holder_image_intent;
		*payload_out = state->holder_image.payload;
		break;
	case RESOURCE_X_INTENT_OWNER_MASTER_GRANT:
		slot = &state->grant_intent.slot;
		*payload_out = state->grant_intent.payload;
		break;
	case RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT:
		slot = &state->requester_settlement_intent.slot;
		*payload_out = state->requester_settlement_intent.payload;
		break;
	case RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE:
		slot = &state->source_settlement.intent.slot;
		*payload_out = state->source_settlement.intent.payload;
		break;
	default:
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(entry_ref_out);
		return NULL;
	}
	return slot;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_outbound_intent_snapshot_exact(
	const ResourceXIntentSlot *expected, ResourceXIntentSlot *slot_out,
	void *payload_out, uint16 payload_capacity)
{
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	ResourceXDecodedFrame pair_image;
	ResourceXDecodedFrame pair_status;
	ResourceXIntentSlot *slot;
	struct GrdEntry *entry;
	uint8 *payload;

	if (slot_out != NULL)
		memset(slot_out, 0, sizeof(*slot_out));
	if (expected == NULL || slot_out == NULL || payload_out == NULL)
		return RESOURCE_X_APPLY_INVALID;
	slot = pcm_resource_x_outbound_intent_lock_exact(
		expected, &entry_ref, &payload);
	if (slot == NULL)
		return RESOURCE_X_APPLY_STALE;
	entry = entry_ref.entry;
	if (slot->state == RESOURCE_X_INTENT_SLOT_EMPTY) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_NOT_FOUND;
	}
	if (!pcm_resource_x_intent_identity_equal(slot, expected)
		|| slot->state > RESOURCE_X_INTENT_SLOT_STAGED
		|| payload_capacity < slot->payload_bytes) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_STALE;
	}
	if (expected->body.owner_kind
			== RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE) {
		state = pcm_resource_x_master_state_for_entry(entry);
		if (state == NULL
			|| state->holder_status.valid
				!= RESOURCE_X_HOLDER_PAIR_PUBLISHED
			|| state->holder_image.valid
				!= RESOURCE_X_HOLDER_PAIR_PUBLISHED
			|| pcm_resource_x_holder_pair_decode_locked(
				state, &pair_status, &pair_image)
				!= RESOURCE_X_APPLY_APPLIED) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
	}
	*slot_out = *slot;
	memcpy(payload_out, payload, slot->payload_bytes);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_outbound_intent_not_admitted_exact(
	const ResourceXIntentSlot *expected, uint64 now_us)
{
	PcmEntryRef entry_ref;
	ResourceXIntentSlot *slot;
	ResourceXIntentResult result;
	uint8 *payload;

	slot = pcm_resource_x_outbound_intent_lock_exact(
		expected, &entry_ref, &payload);
	if (slot == NULL)
		return RESOURCE_X_INTENT_STALE;
	result = cluster_pcm_lock_resource_x_intent_not_admitted_exact(
		slot, expected, now_us);
	if (result == RESOURCE_X_INTENT_NOT_ADMITTED
		&& !pcm_resource_x_intent_mark_dirty())
		result = RESOURCE_X_INTENT_STALE;
	LWLockRelease(&entry_ref.entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
	const ResourceXIntentSlot *expected, uint64 now_us)
{
	PcmEntryRef entry_ref;
	ResourceXIntentSlot *slot;
	ResourceXIntentResult result;
	uint8 *payload;

	slot = pcm_resource_x_outbound_intent_lock_exact(
		expected, &entry_ref, &payload);
	if (slot == NULL)
		return RESOURCE_X_INTENT_STALE;
	result = cluster_pcm_lock_resource_x_intent_stage_exact(
		slot, expected, now_us);
	LWLockRelease(&entry_ref.entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXIntentResult
cluster_pcm_lock_resource_x_outbound_intent_hard_rearm_exact(
	const ResourceXIntentSlot *expected, uint64 now_us)
{
	PcmEntryRef entry_ref;
	ResourceXIntentSlot *slot;
	ResourceXIntentResult result;
	uint8 *payload;

	slot = pcm_resource_x_outbound_intent_lock_exact(
		expected, &entry_ref, &payload);
	if (slot == NULL)
		return RESOURCE_X_INTENT_STALE;
	result = cluster_pcm_lock_resource_x_intent_hard_rearm_exact(
		slot, expected, now_us);
	if (result == RESOURCE_X_INTENT_HARD_REARMED
		&& !pcm_resource_x_intent_mark_dirty())
		result = RESOURCE_X_INTENT_STALE;
	LWLockRelease(&entry_ref.entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

bool
cluster_pcm_lock_resource_x_outbound_intent_complete_exact(
	const ResourceXIntentSlot *expected)
{
	PcmEntryRef entry_ref;
	ResourceXIntentSlot *slot;
	uint8 *payload;
	bool completed;

	slot = pcm_resource_x_outbound_intent_lock_exact(
		expected, &entry_ref, &payload);
	if (slot == NULL)
		return false;
	completed = cluster_pcm_lock_resource_x_intent_complete_exact(
		slot, expected);
	LWLockRelease(&entry_ref.entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return completed;
}

static void
pcm_resource_x_outbound_owner_at(
	ClusterPcmResourceXMasterState *state, uint32 owner_index,
	ResourceXIntentSlot **slot_out, uint8 **payload_out)
{
	Assert(state != NULL);
	Assert(slot_out != NULL);
	Assert(payload_out != NULL);
	*slot_out = NULL;
	*payload_out = NULL;
	if (owner_index < RESOURCE_X_PROTOCOL_NODE_LIMIT) {
		*slot_out = &state->block_intents[owner_index].slot;
		*payload_out = state->block_intents[owner_index].payload;
	} else if (owner_index == RESOURCE_X_PROTOCOL_NODE_LIMIT) {
		*slot_out = &state->holder_status_intent.slot;
		*payload_out = state->holder_status_intent.payload;
	} else if (owner_index == RESOURCE_X_PROTOCOL_NODE_LIMIT + 1) {
		*slot_out = &state->grant_intent.slot;
		*payload_out = state->grant_intent.payload;
	} else if (owner_index == RESOURCE_X_PROTOCOL_NODE_LIMIT + 2) {
		*slot_out = &state->holder_image_intent;
		*payload_out = state->holder_image.payload;
	} else if (owner_index == RESOURCE_X_PROTOCOL_NODE_LIMIT + 3) {
		*slot_out = &state->requester_settlement_intent.slot;
		*payload_out = state->requester_settlement_intent.payload;
	} else if (owner_index == RESOURCE_X_PROTOCOL_NODE_LIMIT + 4) {
		*slot_out = &state->source_settlement.intent.slot;
		*payload_out = state->source_settlement.intent.payload;
	}
}

static bool
pcm_resource_x_outbound_owner_valid(const ResourceXIntentSlot *slot,
									uint32 owner_index)
{
	if (slot == NULL || !pcm_resource_x_intent_body_valid(&slot->body))
		return false;
	if (owner_index < RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return slot->body.owner_kind
				   == RESOURCE_X_INTENT_OWNER_MASTER_BLOCK
			&& slot->body.owner_index == owner_index
			&& slot->body.owner_generation == slot->logical_generation
			&& slot->destination_node == owner_index
			&& slot->kind == RESOURCE_X_WIRE_BLOCK_TO_N
			&& slot->payload_bytes == RESOURCE_X_CONTROL_V1_BYTES;
	if (owner_index == RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return slot->body.owner_kind
				   == RESOURCE_X_INTENT_OWNER_HOLDER_STATUS
			&& slot->body.owner_index == 0
			&& slot->body.owner_generation == slot->logical_generation
			&& slot->kind == RESOURCE_X_WIRE_BLOCKED_TO_N
			&& (slot->payload_bytes == RESOURCE_X_CONTROL_V1_BYTES
				|| slot->payload_bytes == RESOURCE_X_PROOF_V1_BYTES);
	if (owner_index == RESOURCE_X_PROTOCOL_NODE_LIMIT + 1)
		return slot->body.owner_kind
				   == RESOURCE_X_INTENT_OWNER_MASTER_GRANT
			&& slot->body.owner_index == 0
			&& slot->body.owner_generation == slot->authority_generation
			&& slot->kind == RESOURCE_X_WIRE_AUTHORITY_GRANT
			&& slot->payload_bytes == RESOURCE_X_PROOF_V1_BYTES;
	if (owner_index == RESOURCE_X_PROTOCOL_NODE_LIMIT + 2)
		return slot->body.owner_kind
				   == RESOURCE_X_INTENT_OWNER_HOLDER_IMAGE
			&& slot->body.owner_index == 0
			&& slot->body.owner_generation == slot->logical_generation
			&& slot->kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE
			&& slot->payload_bytes == RESOURCE_X_IMAGE_V1_BYTES;
	if (owner_index == RESOURCE_X_PROTOCOL_NODE_LIMIT + 3)
		return slot->body.owner_kind
				   == RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT
			&& slot->body.owner_index == 0
			&& slot->body.owner_generation == slot->logical_generation
			&& slot->body.assertion.requester_node == cluster_node_id
			&& slot->kind == RESOURCE_X_WIRE_INSTALL_SETTLEMENT
			&& slot->payload_bytes == RESOURCE_X_SHORT_V1_BYTES;
	if (owner_index == RESOURCE_X_PROTOCOL_NODE_LIMIT + 4)
		return slot->body.owner_kind
				   == RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE
			&& slot->body.owner_index == 0
			&& slot->body.owner_generation == slot->logical_generation
			&& slot->kind == RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
			&& slot->payload_bytes == RESOURCE_X_PROOF_V1_BYTES;
	return false;
}

ResourceXIntentProbeResult
cluster_pcm_lock_resource_x_outbound_intent_probe_exact(
	uint32 probe_budget, ResourceXIntentSlot *slot_out, void *payload_out,
	uint16 payload_capacity, uint32 *examined_out)
{
	uint64 arm_generation;
	uint64 capacity;
	uint64 completed_generation;
	uint64 cursor;
	uint64 scan_generation;
	uint32 examined = 0;
	uint32 owner_cursor;

	if (slot_out != NULL)
		memset(slot_out, 0, sizeof(*slot_out));
	if (examined_out != NULL)
		*examined_out = 0;
	if (probe_budget == 0 || probe_budget > 4 || slot_out == NULL
		|| payload_out == NULL || examined_out == NULL
		|| payload_capacity < RESOURCE_X_PROOF_V1_BYTES
		|| ClusterPcm == NULL || cluster_pcm_resource_x_slots == NULL
		|| cluster_pcm_resource_x_master_states == NULL
		|| pcm_grd_effective <= 0
		|| pg_atomic_read_u32(
			&ClusterPcm->resource_x_intent_generation_exhausted) != 0)
		return RESOURCE_X_INTENT_PROBE_CORRUPT;
	capacity = (uint64)pcm_grd_effective;
	arm_generation
		= pg_atomic_read_u64(&ClusterPcm->resource_x_intent_arm_generation);
	cursor = pg_atomic_read_u64(
		&ClusterPcm->resource_x_intent_next_state_index);
	owner_cursor = pg_atomic_read_u32(
		&ClusterPcm->resource_x_intent_next_owner_index);
	scan_generation = pg_atomic_read_u64(
		&ClusterPcm->resource_x_intent_scan_generation);
	completed_generation = pg_atomic_read_u64(
		&ClusterPcm->resource_x_intent_completed_generation);
	if (cursor > capacity
		|| owner_cursor >= PGRAC_RESOURCE_X_OUTBOUND_OWNER_SLOTS)
		return RESOURCE_X_INTENT_PROBE_CORRUPT;
	if (cursor == 0 && owner_cursor == 0) {
		if (completed_generation == arm_generation)
			return RESOURCE_X_INTENT_PROBE_IDLE;
		scan_generation = arm_generation;
		pg_atomic_write_u64(
			&ClusterPcm->resource_x_intent_scan_generation,
			scan_generation);
	}
	while (cursor < capacity && examined < probe_budget) {
		ClusterPcmResourceXSlot *registry_slot
			= &cluster_pcm_resource_x_slots[cursor];
		ClusterPcmResourceXMasterState *state;
		PcmEntryRef entry_ref;
		PcmEntryAcquireResult acquire_result;
		struct GrdEntry *entry;
		uint32 owner_index;

		examined++;
		if (registry_slot->state == PCM_REGISTRY_EMPTY
			|| registry_slot->state == PCM_REGISTRY_TOMBSTONE) {
			cursor++;
			owner_cursor = 0;
			continue;
		}
		if (registry_slot->state != PCM_REGISTRY_LIVE) {
			*examined_out = examined;
			return RESOURCE_X_INTENT_PROBE_CORRUPT;
		}
		if (!pcm_entry_ref_acquire(&registry_slot->tag, false,
				&entry_ref, &acquire_result)) {
			*examined_out = examined;
			return RESOURCE_X_INTENT_PROBE_CORRUPT;
		}
		entry = entry_ref.entry;
		LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
		state = pcm_resource_x_master_state_for_entry(entry);
		if (state == NULL
			|| entry_ref.binding_generation
				!= registry_slot->binding_generation
			|| entry_ref.registry_slot != (uint32)cursor) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			*examined_out = examined;
			return RESOURCE_X_INTENT_PROBE_CORRUPT;
		}
		for (owner_index = owner_cursor;
			 owner_index < PGRAC_RESOURCE_X_OUTBOUND_OWNER_SLOTS;
			 owner_index++) {
			ResourceXIntentSlot *slot;
			uint8 *payload;

			pcm_resource_x_outbound_owner_at(
				state, owner_index, &slot, &payload);
			if (slot == NULL || payload == NULL) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_entry_ref_release(&entry_ref);
				*examined_out = examined;
				return RESOURCE_X_INTENT_PROBE_CORRUPT;
			}
			if (slot->state == RESOURCE_X_INTENT_SLOT_EMPTY
				|| slot->state == RESOURCE_X_INTENT_SLOT_STAGED)
				continue;
			if (slot->state != RESOURCE_X_INTENT_SLOT_ARMED
				|| !pcm_resource_x_outbound_owner_valid(
					slot, owner_index)
					|| slot->payload_bytes > payload_capacity) {
				LWLockRelease(&entry->entry_lock.lock);
				pcm_entry_ref_release(&entry_ref);
				*examined_out = examined;
				return RESOURCE_X_INTENT_PROBE_CORRUPT;
			}
			*slot_out = *slot;
			memcpy(payload_out, payload, slot->payload_bytes);
			owner_cursor = owner_index + 1;
			if (owner_cursor
				== PGRAC_RESOURCE_X_OUTBOUND_OWNER_SLOTS) {
				cursor++;
				owner_cursor = 0;
			}
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			pg_atomic_write_u64(
				&ClusterPcm->resource_x_intent_next_state_index,
				cursor);
			pg_atomic_write_u32(
				&ClusterPcm->resource_x_intent_next_owner_index,
				owner_cursor);
			*examined_out = examined;
			return RESOURCE_X_INTENT_PROBE_FOUND;
		}
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		cursor++;
		owner_cursor = 0;
	}
	*examined_out = examined;
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_intent_next_state_index, cursor);
	pg_atomic_write_u32(
		&ClusterPcm->resource_x_intent_next_owner_index, owner_cursor);
	if (cursor < capacity)
		return RESOURCE_X_INTENT_PROBE_MORE;
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_intent_next_state_index, 0);
	pg_atomic_write_u32(
		&ClusterPcm->resource_x_intent_next_owner_index, 0);
	pg_atomic_write_u64(
		&ClusterPcm->resource_x_intent_completed_generation,
		scan_generation);
	return RESOURCE_X_INTENT_PROBE_COMPLETE;
}

static bool
pcm_resource_x_final_common_matches_request(
	const ResourceXDecodedCommon *common,
	const ClusterPcmResourceXMasterRequest *request)
{
	return common->base_authority_generation
			   == request->base_authority_generation
		&& common->authority_generation == request->final_authority_generation
		&& common->resource_formation == request->resource_formation
		&& common->master_session_incarnation
			   == request->master_session_incarnation
		&& common->assertion_sequence == request->assertion_sequence
		&& common->ordered_lane == request->ordered_lane;
}

static bool
pcm_resource_x_install_settlement_valid(
	const ResourceXDecodedFrame *settlement, int32 authenticated_source_node)
{
	const ResourceXDecodedInstallSettlement *body;

	if (settlement == NULL
		|| settlement->kind != RESOURCE_X_WIRE_INSTALL_SETTLEMENT
		|| !resource_x_assertion_valid(&settlement->common.logical_assertion)
		|| authenticated_source_node
			   != settlement->common.logical_assertion.requester_node
		|| settlement->common.action_node != authenticated_source_node
		|| settlement->common.target_mode != (uint8)PCM_STATE_X
		|| settlement->common.source_candidate != 0
		|| settlement->common.retain_pi_if_dirty != 0
		|| settlement->common.sender_connection_generation == 0
		|| settlement->common.outcome != RESOURCE_X_OUTCOME_OK
		|| settlement->common.flags != 0)
		return false;
	body = &settlement->body.install_settlement;
	return body->conversion_base_generation
			   == settlement->common.base_authority_generation
		&& body->final_authority_generation
			   == settlement->common.authority_generation
		&& body->requester_connection_generation != 0
		&& body->requester_target_generation != 0
		&& body->installed_mode == (uint8)PCM_STATE_X
		&& body->requester_role == RESOURCE_X_REQUESTER_ROLE_ACQUIRER
		&& body->terminal_outcome == RESOURCE_X_OUTCOME_OK
		&& body->terminal_state == RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED
		&& body->settlement_flags == 0;
}

static bool
pcm_resource_x_install_settlement_matches_request(
	const ResourceXDecodedFrame *settlement,
	const ClusterPcmResourceXMasterRequest *request)
{
	const ResourceXDecodedInstallSettlement *body;

	if (settlement == NULL || request == NULL
		|| !pcm_resource_x_final_common_matches_request(
			&settlement->common, request))
		return false;
	body = &settlement->body.install_settlement;
	return body->conversion_base_generation
			== request->base_authority_generation
		&& body->final_authority_generation
			== request->final_authority_generation
		&& body->requester_connection_generation
			== request->settlement_requester_connection_generation
		&& body->requester_target_generation
			== request->requester_target_generation
		&& body->page_scn_lsn == request->page_scn_lsn
		&& body->page_checksum == request->page_checksum
		&& body->source_proof_crc32c == request->source_proof_crc32c;
}

static bool
pcm_resource_x_source_settlement_ack_matches_retained(
	const ResourceXDecodedFrame *ack,
	const ResourceXDecodedFrame *retained)
{
	const ResourceXDecodedBlockedToN *left;
	const ResourceXDecodedBlockedToN *right;

	if (ack == NULL || retained == NULL
		|| ack->kind != RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2
		|| retained->kind != RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
		|| !ack->blocked_has_remote_proof
		|| !retained->blocked_has_remote_proof
		|| !resource_x_assertion_equal(
			&ack->common.logical_assertion,
			&retained->common.logical_assertion)
		|| ack->common.base_authority_generation
			!= retained->common.base_authority_generation
		|| ack->common.authority_generation
			!= retained->common.authority_generation
		|| ack->common.resource_formation
			!= retained->common.resource_formation
		|| ack->common.master_session_incarnation
			!= retained->common.master_session_incarnation
		|| ack->common.assertion_sequence
			!= retained->common.assertion_sequence
		|| ack->common.ordered_lane != retained->common.ordered_lane
		|| ack->common.action_node != retained->common.action_node
		|| ack->common.observed_mode != retained->common.observed_mode
		|| ack->common.target_mode != retained->common.target_mode
		|| ack->common.source_candidate
			!= retained->common.source_candidate
		|| ack->common.retain_pi_if_dirty
			!= retained->common.retain_pi_if_dirty
		|| ack->common.sender_connection_generation == 0
		|| ack->common.outcome != RESOURCE_X_OUTCOME_OK
		|| ack->common.flags != retained->common.flags)
		return false;
	left = &ack->body.blocked_to_n;
	right = &retained->body.blocked_to_n;
	return left->source_carrier_generation
			   == right->source_carrier_generation
		&& left->requester_target_generation
			   == right->requester_target_generation
		&& left->page_scn_lsn == right->page_scn_lsn
		&& left->dependency_count == right->dependency_count
		&& left->source_proof_crc32c == right->source_proof_crc32c
		&& left->page_checksum == right->page_checksum
		&& left->source_disposition == right->source_disposition
		&& left->proof_kind == right->proof_kind
		&& left->proof_flags == right->proof_flags
		&& left->holder_connection_generation
			   == right->holder_connection_generation
		&& left->acting_formation == right->acting_formation
		&& memcmp(left->source_fence, right->source_fence,
			sizeof(left->source_fence)) == 0
		&& memcmp(left->dependencies, right->dependencies,
			sizeof(left->dependencies)) == 0;
}

static ResourceXApplyResult
pcm_resource_x_arm_source_settlement_locked(
	struct GrdEntry *entry, ClusterPcmResourceXMasterState *state,
	ClusterPcmResourceXMasterRequest *request, int32 requester_node)
{
	ResourceXDecodedBlockedToN *body;
	ResourceXDecodedFrame settlement;
	ResourceXIntentBodyHandle owner;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	uint64 now_us;
	uint16 payload_len = 0;

	Assert(entry != NULL);
	Assert(state != NULL);
	Assert(request != NULL);
	Assert(LWLockHeldByMeInMode(&entry->entry_lock.lock, LW_EXCLUSIVE));
	if (request->ordered_lane != 0
		|| request->proof_kind != RESOURCE_X_PROOF_REMOTE_CARRIER
		|| request->source_node < 0
		|| request->source_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| request->source_node == requester_node
		|| request->final_authority_generation
			<= request->base_authority_generation
		|| (state->source_settlement.state
			!= RESOURCE_X_SOURCE_SETTLEMENT_NONE
			&& state->source_settlement.state
				!= RESOURCE_X_SOURCE_SETTLEMENT_ACKED)
		|| state->source_settlement.intent.slot.state
			!= RESOURCE_X_INTENT_SLOT_EMPTY
		|| cluster_node_id < 0
		|| cluster_node_id >= RESOURCE_X_PROTOCOL_NODE_LIMIT)
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	memset(&settlement, 0, sizeof(settlement));
	settlement.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2;
	settlement.payload_bytes = RESOURCE_X_PROOF_V1_BYTES;
	settlement.blocked_has_remote_proof = true;
	settlement.common.logical_assertion.resource = entry->tag;
	settlement.common.logical_assertion.requester_node = requester_node;
	settlement.common.base_authority_generation
		= request->base_authority_generation;
	settlement.common.authority_generation
		= request->final_authority_generation;
	settlement.common.resource_formation = request->resource_formation;
	settlement.common.master_session_incarnation
		= request->master_session_incarnation;
	settlement.common.assertion_sequence = request->assertion_sequence;
	settlement.common.ordered_lane = 0;
	settlement.common.action_node = request->source_node;
	settlement.common.observed_mode = (uint8)PCM_STATE_N;
	settlement.common.target_mode = (uint8)PCM_STATE_N;
	settlement.common.source_candidate = 1;
	settlement.common.retain_pi_if_dirty = 1;
	settlement.common.sender_connection_generation
		= PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	settlement.common.outcome = RESOURCE_X_OUTCOME_NONE;
	body = &settlement.body.blocked_to_n;
	memcpy(body->source_fence, request->source_fence,
		sizeof(body->source_fence));
	body->source_carrier_generation = request->source_carrier_generation;
	body->requester_target_generation = request->requester_target_generation;
	body->page_scn_lsn = request->page_scn_lsn;
	body->dependency_count = request->dependency_count;
	memcpy(body->dependencies, request->dependencies,
		sizeof(body->dependencies));
	body->source_proof_crc32c = request->source_proof_crc32c;
	body->page_checksum = request->page_checksum;
	body->source_disposition = request->source_disposition;
	body->proof_kind = request->proof_kind;
	body->proof_flags = request->proof_flags;
	body->holder_connection_generation
		= request->holder_connection_generation;
	body->acting_formation = request->acting_formation;
	if (!cluster_resource_x_wire_encode(
			RESOURCE_X_MSG_BLOCK_TO_N, &settlement,
			state->source_settlement.intent.payload,
			sizeof(state->source_settlement.intent.payload),
			&payload_len, &reject)
		|| payload_len != RESOURCE_X_PROOF_V1_BYTES) {
		memset(&state->source_settlement, 0,
			sizeof(state->source_settlement));
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	memset(&owner, 0, sizeof(owner));
	owner.assertion = settlement.common.logical_assertion;
	owner.owner_generation = request->assertion_sequence;
	owner.owner_node = (uint32)cluster_node_id;
	owner.owner_kind = RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE;
	now_us = pcm_resource_x_monotonic_us();
	if (!cluster_pcm_lock_resource_x_intent_arm_exact(
			&state->source_settlement.intent.slot, &owner,
			request->assertion_sequence,
			request->final_authority_generation, now_us,
			(uint32)request->source_node, payload_len,
			RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2)
		|| !pcm_resource_x_intent_mark_dirty()) {
		memset(&state->source_settlement, 0,
			sizeof(state->source_settlement));
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	state->source_settlement.state = RESOURCE_X_SOURCE_SETTLEMENT_PENDING;
	return RESOURCE_X_APPLY_APPLIED;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_install_settlement_exact(
	const ResourceXDecodedFrame *settlement, int32 authenticated_source_node,
	ResourceXMasterSnapshot *out)
{
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXMasterRequest *request;
	const ResourceXDecodedInstallSettlement *body;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	ResourceXApplyResult result;
	PcmState current_state;
	uint32 s_holders;
	int32 requester_node;

	if (out != NULL) {
		memset(out, 0, sizeof(*out));
		out->source_node = -1;
	}
	if (out == NULL
		|| !pcm_resource_x_install_settlement_valid(settlement,
											 authenticated_source_node))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&settlement->common.logical_assertion.resource,
			false, &entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	requester_node = settlement->common.logical_assertion.requester_node;
	body = &settlement->body.install_settlement;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	request = &state->requests[requester_node];
	if (pcm_resource_x_terminal_tombstone_valid(&state->terminal_tombstone)
		&& state->terminal_tombstone.requester_node == requester_node
		&& pcm_resource_x_install_settlement_matches_request(
			settlement, &state->terminal_tombstone.request)) {
		pcm_resource_x_terminal_tombstone_snapshot(
			&entry->tag, state, &state->terminal_tombstone, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_DUPLICATE;
	}
	if (request->phase == RESOURCE_X_MASTER_NONE) {
		if (pcm_resource_x_terminal_tombstone_valid(
				&state->terminal_tombstone)
			&& state->terminal_tombstone.requester_node == requester_node) {
			pcm_resource_x_terminal_tombstone_snapshot(
				&entry->tag, state, &state->terminal_tombstone, out);
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return RESOURCE_X_APPLY_STALE;
		}
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_NOT_FOUND;
	}
	if (!pcm_resource_x_final_common_matches_request(&settlement->common,
											 request)
		|| body->conversion_base_generation
			   != request->base_authority_generation
		|| body->final_authority_generation
			   != request->final_authority_generation
		|| body->requester_target_generation
			   != request->requester_target_generation
		|| body->page_scn_lsn != request->page_scn_lsn
		|| body->page_checksum != request->page_checksum
		|| body->source_proof_crc32c != request->source_proof_crc32c) {
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_STALE;
	}
	if (request->phase == RESOURCE_X_MASTER_SETTLED) {
		result = request->settlement_requester_connection_generation
					 == body->requester_connection_generation
			? RESOURCE_X_APPLY_DUPLICATE : RESOURCE_X_APPLY_STALE;
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return result;
	}
	current_state = (PcmState)pg_atomic_read_u32(&entry->master_state);
	s_holders = pg_atomic_read_u32(&entry->s_holders_bitmap);
	if (request->phase != RESOURCE_X_MASTER_GRANT_COMMITTED
		|| entry->pending_x_requester_node != -1
		|| entry->pending_x_since_lsn != 0) {
		pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
									   request, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_BAD_STATE;
	}
	if (current_state != PCM_STATE_X
		|| entry->x_holder_node != requester_node || s_holders != 0)
		goto bad_state;
	if (request->ordered_lane == 0
		&& request->proof_kind == RESOURCE_X_PROOF_REMOTE_CARRIER) {
		result = pcm_resource_x_arm_source_settlement_locked(
			entry, state, request, requester_node);
		if (result != RESOURCE_X_APPLY_APPLIED) {
			pcm_resource_x_master_snapshot(&entry->tag, requester_node,
				state, request, out);
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return result;
		}
	}
	request->settlement_requester_connection_generation
		= body->requester_connection_generation;
	request->phase = RESOURCE_X_MASTER_SETTLED;
	(void)pcm_resource_x_start_head_locked(entry, state, NULL);
	pcm_resource_x_master_snapshot(&entry->tag, requester_node, state, request,
								   out);
	LWLockRelease(&entry->entry_lock.lock);
	ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	return RESOURCE_X_APPLY_APPLIED;

bad_state:
	pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
								   request, out);
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return RESOURCE_X_APPLY_BAD_STATE;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_source_settlement_intent_snapshot_exact(
	const ResourceXAssertion *assertion, ResourceXIntentSlot *slot_out,
	void *payload_out, uint16 payload_capacity)
{
	ClusterPcmResourceXSourceSettlement *debt;
	ClusterPcmResourceXControlIntent *intent;
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXApplyResult result;
	struct GrdEntry *entry;

	if (slot_out != NULL)
		memset(slot_out, 0, sizeof(*slot_out));
	if (!resource_x_assertion_valid(assertion) || slot_out == NULL
		|| payload_out == NULL
		|| payload_capacity < RESOURCE_X_PROOF_V1_BYTES)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&assertion->resource, false,
			&entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto source_settlement_intent_done;
	}
	debt = &state->source_settlement;
	intent = &debt->intent;
	if (debt->state == RESOURCE_X_SOURCE_SETTLEMENT_NONE
		|| debt->state == RESOURCE_X_SOURCE_SETTLEMENT_ACKED
		|| intent->slot.state == RESOURCE_X_INTENT_SLOT_EMPTY) {
		result = RESOURCE_X_APPLY_NOT_FOUND;
		goto source_settlement_intent_done;
	}
	if (!resource_x_assertion_equal(assertion,
			&intent->slot.body.assertion)) {
		result = RESOURCE_X_APPLY_STALE;
		goto source_settlement_intent_done;
	}
	if (debt->state != RESOURCE_X_SOURCE_SETTLEMENT_PENDING
		|| memcmp(debt->reserved, (uint8[7]){0},
			sizeof(debt->reserved)) != 0
		|| intent->slot.state > RESOURCE_X_INTENT_SLOT_STAGED
		|| !pcm_resource_x_intent_body_valid(&intent->slot.body)
		|| intent->slot.payload_bytes != RESOURCE_X_PROOF_V1_BYTES
		|| intent->slot.kind != RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
		|| intent->slot.body.owner_generation
			!= intent->slot.logical_generation
		|| intent->slot.body.owner_kind
			!= RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto source_settlement_intent_done;
	}
	*slot_out = intent->slot;
	memcpy(payload_out, intent->payload, intent->slot.payload_bytes);
	result = RESOURCE_X_APPLY_APPLIED;

source_settlement_intent_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_source_settlement_ack_exact(
	const ResourceXDecodedFrame *ack, int32 authenticated_source_node,
	ResourceXMasterSnapshot *out)
{
	ClusterPcmResourceXSourceSettlement *debt;
	ClusterPcmResourceXMasterState *state;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	ResourceXDecodedFrame retained;
	ResourceXApplyResult result;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_NONE;
	struct GrdEntry *entry;

	if (out != NULL) {
		memset(out, 0, sizeof(*out));
		out->source_node = -1;
	}
	if (ack == NULL || out == NULL
		|| ack->kind != RESOURCE_X_WIRE_SOURCE_SETTLEMENT_ACK_V2
		|| !resource_x_assertion_valid(&ack->common.logical_assertion)
		|| authenticated_source_node < 0
		|| authenticated_source_node >= RESOURCE_X_PROTOCOL_NODE_LIMIT
		|| ack->common.action_node != authenticated_source_node)
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&ack->common.logical_assertion.resource,
			false, &entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto source_settlement_ack_done;
	}
	debt = &state->source_settlement;
	if (debt->state == RESOURCE_X_SOURCE_SETTLEMENT_NONE) {
		result = RESOURCE_X_APPLY_NOT_FOUND;
		goto source_settlement_ack_done;
	}
	if ((debt->state != RESOURCE_X_SOURCE_SETTLEMENT_PENDING
			&& debt->state != RESOURCE_X_SOURCE_SETTLEMENT_ACKED)
		|| memcmp(debt->reserved, (uint8[7]){0},
			sizeof(debt->reserved)) != 0
		|| !cluster_resource_x_wire_decode(
			RESOURCE_X_MSG_BLOCK_TO_N, debt->intent.payload,
			RESOURCE_X_PROOF_V1_BYTES, &retained, &reject)) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto source_settlement_ack_done;
	}
	if (authenticated_source_node != retained.common.action_node
		|| !pcm_resource_x_source_settlement_ack_matches_retained(
			ack, &retained)) {
		result = RESOURCE_X_APPLY_STALE;
		goto source_settlement_ack_done;
	}
	if (debt->state == RESOURCE_X_SOURCE_SETTLEMENT_ACKED) {
		result = debt->intent.slot.state == RESOURCE_X_INTENT_SLOT_EMPTY
			? RESOURCE_X_APPLY_DUPLICATE
			: RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto source_settlement_ack_done;
	}
	if ((debt->intent.slot.state != RESOURCE_X_INTENT_SLOT_ARMED
			&& debt->intent.slot.state != RESOURCE_X_INTENT_SLOT_STAGED)
		|| debt->intent.slot.destination_node
			!= (uint32)authenticated_source_node
		|| debt->intent.slot.kind
			!= RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2
		|| debt->intent.slot.payload_bytes != RESOURCE_X_PROOF_V1_BYTES
		|| debt->intent.slot.body.owner_kind
			!= RESOURCE_X_INTENT_OWNER_HOLDER_RELEASE
		|| !resource_x_assertion_equal(
			&debt->intent.slot.body.assertion,
			&retained.common.logical_assertion)) {
		result = RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		goto source_settlement_ack_done;
	}
	memset(&debt->intent.slot, 0, sizeof(debt->intent.slot));
	debt->state = RESOURCE_X_SOURCE_SETTLEMENT_ACKED;
	result = RESOURCE_X_APPLY_APPLIED;

source_settlement_ack_done:
	LWLockRelease(&entry->entry_lock.lock);
	pcm_entry_ref_release(&entry_ref);
	return result;
}

static bool
pcm_resource_x_release_valid(const ResourceXDecodedFrame *release,
							 int32 authenticated_source_node)
{
	return release != NULL && release->kind == RESOURCE_X_WIRE_RELEASE_X
		&& resource_x_assertion_valid(&release->common.logical_assertion)
		&& authenticated_source_node
			   == release->common.logical_assertion.requester_node
		&& release->common.action_node == authenticated_source_node
		&& release->common.observed_mode == (uint8)PCM_STATE_X
		&& release->common.target_mode == (uint8)PCM_STATE_N
		&& release->common.source_candidate == 0
		&& release->common.retain_pi_if_dirty == 0
		&& release->common.sender_connection_generation != 0
		&& release->common.outcome == RESOURCE_X_OUTCOME_OK
		&& release->common.flags == 0;
}

static void
pcm_resource_x_release_snapshot(
	const BufferTag *tag, const ClusterPcmResourceXMasterState *state,
	const ClusterPcmResourceXMasterRequest *request,
	const ClusterPcmResourceXTerminalTombstone *tombstone,
	bool tombstone_request, int32 requester_node,
	ResourceXMasterSnapshot *out)
{
	if (tombstone_request)
		pcm_resource_x_terminal_tombstone_snapshot(
			tag, state, tombstone, out);
	else
		pcm_resource_x_master_snapshot(
			tag, requester_node, state, request, out);
}

ResourceXApplyResult
cluster_pcm_lock_resource_x_release_x_exact(
	const ResourceXDecodedFrame *release, int32 authenticated_source_node,
	ResourceXMasterSnapshot *out)
{
	BufferTag retire_tag;
	ClusterPcmResourceXMasterState *state;
	ClusterPcmResourceXMasterRequest *request;
	ClusterPcmResourceXMasterRequest *successor;
	ClusterPcmResourceXTerminalTombstone *tombstone;
	PcmEntryRef entry_ref;
	PcmEntryAcquireResult acquire_result;
	struct GrdEntry *entry;
	uint64 retire_generation = 0;
	uint64 legacy_generation;
	uint32 requester_bit;
	int32 requester_node;
	bool fast_retire = false;
	bool preserve_local_terminal_cover = false;
	bool released_without_successor = false;
	bool tombstone_request = false;

	if (out != NULL) {
		memset(out, 0, sizeof(*out));
		out->source_node = -1;
	}
	if (out == NULL
		|| !pcm_resource_x_release_valid(release, authenticated_source_node))
		return RESOURCE_X_APPLY_INVALID;
	if (!pcm_entry_ref_acquire(&release->common.logical_assertion.resource,
			false, &entry_ref, &acquire_result))
		return RESOURCE_X_APPLY_NOT_FOUND;
	entry = entry_ref.entry;
	requester_node = release->common.logical_assertion.requester_node;
	requester_bit = UINT32_C(1) << (uint32)requester_node;
	pcm_entry_lock_exclusive(entry);
	state = pcm_resource_x_master_state_for_entry(entry);
	if (state == NULL) {
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	}
	tombstone = &state->terminal_tombstone;
	request = &state->requests[requester_node];
	if (request->phase == RESOURCE_X_MASTER_NONE) {
		if (!pcm_resource_x_terminal_tombstone_valid(tombstone)
			|| tombstone->requester_node != requester_node) {
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return RESOURCE_X_APPLY_NOT_FOUND;
		}
		if (!pcm_resource_x_final_common_matches_request(
				&release->common, &tombstone->request)) {
			pcm_resource_x_terminal_tombstone_snapshot(
				&entry->tag, state, tombstone, out);
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return RESOURCE_X_APPLY_STALE;
		}
		request = &tombstone->request;
		tombstone_request = true;
	}
	else if (!pcm_resource_x_final_common_matches_request(
				&release->common, request)) {
		if (pcm_resource_x_terminal_tombstone_valid(tombstone)
			&& tombstone->requester_node == requester_node
			&& pcm_resource_x_final_common_matches_request(
				&release->common, &tombstone->request)) {
			request = &tombstone->request;
			tombstone_request = true;
		}
		else {
			pcm_resource_x_master_snapshot(&entry->tag, requester_node, state,
				request, out);
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return RESOURCE_X_APPLY_STALE;
		}
	}
	if (request->phase == RESOURCE_X_MASTER_RELEASED) {
		pcm_resource_x_release_snapshot(&entry->tag, state, request,
			tombstone, tombstone_request, requester_node, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_DUPLICATE;
	}
	/* Once GRD no longer names the tombstone requester as current X, an exact
	 * late RELEASE_X is terminal duplicate.  The tombstone does not recreate
	 * holder authority or select PI. */
	if (tombstone_request
		&& ((PcmState)pg_atomic_read_u32(&entry->master_state) != PCM_STATE_X
			|| entry->x_holder_node != requester_node)) {
		pcm_resource_x_terminal_tombstone_snapshot(
			&entry->tag, state, tombstone, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_DUPLICATE;
	}
	if (request->phase != RESOURCE_X_MASTER_SETTLED
		|| (PcmState)pg_atomic_read_u32(&entry->master_state) != PCM_STATE_X
		|| entry->x_holder_node != requester_node
		|| entry->pending_x_requester_node != -1
		|| (tombstone_request
			&& state->authority_generation
				!= request->final_authority_generation)) {
		pcm_resource_x_release_snapshot(&entry->tag, state, request,
			tombstone, tombstone_request, requester_node, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return RESOURCE_X_APPLY_BAD_STATE;
	}
	/* A colocated requester/master shares this entry with the requester
	 * terminal cover.  Validate that cover before selecting a successor:
	 * pcm_resource_x_start_head_locked() may change phase, bitmaps, and
	 * outbound intents, so detecting cover loss afterwards is too late. */
	if (requester_node == cluster_node_id) {
		preserve_local_terminal_cover
			= entry->resource_x_bootstrap_round.current_master_node == cluster_node_id
			  && entry->resource_x_bootstrap_round.accepted_base
					 == release->common.base_authority_generation
			  && entry->resource_x_bootstrap_round.terminal_authority_generation
					 == release->common.authority_generation
			  && entry->resource_x_bootstrap_round.request.ordered_lane
					 == release->common.ordered_lane
			  && resource_x_assertion_equal(
				  &entry->resource_x_bootstrap_round.terminal_ref.assertion,
				  &release->common.logical_assertion)
			  && pcm_resource_x_local_owner_valid_locked(&entry->resource_x_local_owner)
			  && entry->resource_x_local_owner.state == RESOURCE_X_LOCAL_OWNER_EVICTING
			  && pcm_resource_x_local_owner_round_exact_locked(entry,
															   &entry->resource_x_bootstrap_round)
			  && entry->resource_x_local_owner.handle.absolute_deadline_us
					 == entry->resource_x_bootstrap_round.head_no_progress_deadline_us
			  && pcm_resource_x_bootstrap_round_terminal_cover_exact_locked(
				  entry, &entry->resource_x_bootstrap_round,
				  &entry->resource_x_bootstrap_round.terminal_ref,
				  release->common.master_session_incarnation,
				  entry->resource_x_bootstrap_round.r4_record_generation,
				  entry->resource_x_bootstrap_round.cached_ownership_generation);
		if (!preserve_local_terminal_cover) {
			pcm_resource_x_release_snapshot(&entry->tag, state, request,
				tombstone, tombstone_request, requester_node, out);
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return RESOURCE_X_APPLY_STALE;
		}
		/* A FIFO successor already owns the current carrier handoff.  Cache
		 * eviction cannot repurpose the same local X or call start_head(), which
		 * would mutate blocker intents before the BufferDesc commit. */
		if (pcm_resource_x_master_head(state, NULL) != NULL) {
			pcm_resource_x_release_snapshot(&entry->tag, state, request,
				tombstone, tombstone_request, requester_node, out);
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return RESOURCE_X_APPLY_BAD_STATE;
		}
	}

	/* Preserve the terminal requester as the exact current carrier while a
	 * FIFO successor exists.  Starting the successor against the still-X GRD
	 * state arms its existing BLOCK_TO_N intent; the authenticated type-18
	 * response owns the later X->N+PI transition and grant.  Dropping to N
	 * here would erase the current source and leave only historical PI bits,
	 * which are not Resource-X authority. */
	successor = pcm_resource_x_start_head_locked(entry, state, NULL);
	if (successor == NULL) {
		/* No FIFO successor consumes the current X carrier, so RELEASE_X owns
		 * the physical X->N transition.  Keep the adapter's canonical base in
		 * the same entry-lock critical section: the preceding grant either
		 * advanced legacy authority with a physical transition (equal), or was
		 * an already-X logical grant (legacy trails by exactly one).  Any other
		 * relation is not current-holder lineage and must stop before release.
		 * This is the A' RETURN_MAINLINE lifecycle repair, not PI promotion or
		 * an expansion of the FIFO-successor late-bind contract. */
		legacy_generation
			= pg_atomic_read_u64(&entry->transition_count_local);
		if (state->authority_generation == 0
			|| state->authority_generation == UINT64_MAX
			|| legacy_generation == UINT64_MAX
			|| (legacy_generation != state->authority_generation
				&& legacy_generation + 1 != state->authority_generation)
			|| (legacy_generation == state->authority_generation
				&& legacy_generation == UINT64_MAX - 1)) {
			/* A terminal tombstone is immutable replay history.  A delayed
			 * RELEASE_X whose physical lineage has drifted still fails closed,
			 * but must not turn that terminal record back into live recovery
			 * state merely to report the refusal. */
			if (!tombstone_request)
				request->phase = RESOURCE_X_MASTER_RECOVERY_BLOCKED;
			pcm_resource_x_release_snapshot(&entry->tag, state, request,
				tombstone, tombstone_request, requester_node, out);
			LWLockRelease(&entry->entry_lock.lock);
			pcm_entry_ref_release(&entry_ref);
			return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
		}
		/* On a colocated requester/master, canonical kind-4 apply and the
		 * requester release commit share this entry.  The pre-mutation gate
		 * above proved the exact cover, and this transition must preserve it
		 * until the following requester commit clears the binding. */
		pcm_transition_apply_internal(entry, PCM_TRANS_X_TO_N_RELEASE,
			requester_node, preserve_local_terminal_cover);
		state->authority_generation = legacy_generation + 1;
		released_without_successor = true;
	} else if (successor->phase != RESOURCE_X_MASTER_WAIT_BLOCKERS
			 || (successor->incompatible_holders_bitmap & requester_bit) == 0
			 || (successor->blocked_holders_bitmap & requester_bit) != 0) {
		pcm_resource_x_release_snapshot(&entry->tag, state, request,
			tombstone, tombstone_request, requester_node, out);
		LWLockRelease(&entry->entry_lock.lock);
		pcm_entry_ref_release(&entry_ref);
		return successor->phase == RESOURCE_X_MASTER_RECOVERY_BLOCKED
			? RESOURCE_X_APPLY_RECOVERY_BLOCKED : RESOURCE_X_APPLY_BAD_STATE;
	}
	request->phase = RESOURCE_X_MASTER_RELEASED;
	pcm_resource_x_release_snapshot(&entry->tag, state, request,
		tombstone, tombstone_request, requester_node, out);
	if (tombstone_request && released_without_successor
		&& pcm_entry_mark_quiescing_locked(entry)) {
		retire_tag = entry_ref.tag;
		retire_generation = entry_ref.binding_generation;
		fast_retire = true;
	}
	LWLockRelease(&entry->entry_lock.lock);
	ConditionVariableBroadcast(&entry->wait_cv);
	pcm_entry_ref_release(&entry_ref);
	if (fast_retire)
		(void)pcm_entry_try_retire_exact(&retire_tag, retire_generation,
			PCM_RETIRE_REASON_RESOURCE_X_SETTLED);
	return RESOURCE_X_APPLY_APPLIED;
}


/* ============================================================
 * Module-level shmem registration.
 * ============================================================ */

static const ClusterShmemRegion cluster_pcm_grd_region = {
	.name = "pgrac cluster pcm grd",
	.size_fn = cluster_pcm_grd_shmem_size,
	.init_fn = cluster_pcm_grd_init,
	.lwlock_count = 0, /* per-entry LWLock initialized in init_fn */
	.owner_subsys = "cluster_pcm",
	.reserved_flags = 0,
};


void
cluster_pcm_lock_module_init(void)
{
	/*
	 * Register cluster_pcm_grd region with the spec-1.3 shmem registry.
	 *
	 * Idempotent (registry checks for duplicate names); safe to call
	 * from cluster_init_shmem_module() once per postmaster start.
	 */
	cluster_shmem_register_region(&cluster_pcm_grd_region);
}


#endif /* USE_PGRAC_CLUSTER */
