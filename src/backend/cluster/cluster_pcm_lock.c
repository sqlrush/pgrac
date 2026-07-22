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
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h"	   /* PGRAC: spec-2.30 D1 — pg_atomic_uint32/64 */
#include "storage/backendid.h" /* PGRAC: spec-6.14 D9 amend — no-backend-identity guard */
#include "storage/buf_internals.h"
#include "storage/condition_variable.h" /* PGRAC: spec-2.31 D1 — wait_cv */
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/hsearch.h"	 /* PGRAC: spec-2.30 D2 — HTAB API */
#include "pgstat.h"			 /* pgstat_report_wait_start/end */
#include "utils/timestamp.h" /* PGRAC: spec-2.30 D1 — TimestampTz */


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
StaticAssertDecl(sizeof(struct GrdEntry) == 264,
				 "spec-2.41 D2 §2.8 Option A GrdEntry size 256 → 264 (added pi_watermark_scn "
				 "8B dual watermark for the lost-write detector; pi_watermark_lsn retained for "
				 "the spec-4.7 D5 redo-coverage serve-gate);  spec-2.37 baseline was 256B (had "
				 "pi_watermark_lsn 8B for HC125/HC126);  field inserted after pi_watermark_lsn "
				 "and before wait_cv to keep entry_lock LWLockPadded must-stay-last invariant;  "
				 "amend this constant with Hardening appendix if a different platform produces a "
				 "different size");


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
/*
 * S3 forensics step 1b — one advance-provenance slot (64B).  Insert-once:
 * `used` is never cleared, so open-addressing probe chains stay valid for
 * the region's lifetime (mirrors the GRD HTAB's never-freed entries).
 */
#define CLUSTER_PCM_WM_PROV_SLOTS 8192

typedef struct ClusterPcmWmProvSlot {
	bool used;	  /* slot claimed (never cleared) */
	uint8 source; /* ClusterPcmWmSrc */
	int16 _pad1;
	int32 sender_node; /* -1 = local/unknown */
	BufferTag tag;	   /* 20B */
	int32 _pad2;
	SCN old_scn;	   /* watermark before the advancing feed */
	SCN new_scn;	   /* fed page_scn == watermark right after the feed */
	uint64 request_id; /* wire identity (0 = none) */
	uint64 epoch;	   /* wire epoch (0 = none) */
} ClusterPcmWmProvSlot;

typedef struct ClusterPcmShared {
	LWLockPadded htab_lock;
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
	 * and the query returned an unrelated late feed.  Open-addressed,
	 * insert-once-never-evict; one slot per tag holds the LAST feed that
	 * actually advanced the monotone-max watermark (non-advancing feeds
	 * never enter), so new_scn always explains the CURRENT watermark.
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
/*
 * Resolved (post-HC62) entry count used by HTAB cap + accessor + errmsg.
 *	Set in cluster_pcm_grd_init from pcm_grd_effective_entries(true) ;
 *	0 means disabled.  Reading the raw GUC `cluster_pcm_grd_max_entries`
 *	is fine for show / dump_pcm but NOT for sizing logic (which must use
 *	the resolved value post HC62 fail-closed checks).
 */
static int pcm_grd_effective = 0;


/* Forward decl — file-private HTAB lazy-alloc helper defined below init_fn. */
static struct GrdEntry *pcm_get_or_create_entry(BufferTag tag);
/* S3 forensics step 1b — watermark-advance provenance recorder (defined with
 * the watermark helpers below; used by the inline writer sites above them).
 * Caller MUST hold the tag's GRD entry_lock and call ONLY on actual advance. */
static void pcm_wm_prov_record(BufferTag tag, SCN old_scn, SCN new_scn, ClusterPcmWmSrc source,
							   int32 sender_node, uint64 request_id, uint64 epoch);
static struct GrdEntry *pcm_find_entry(BufferTag tag);
static void pcm_entry_lock_exclusive(struct GrdEntry *entry);
static uint32 pcm_holder_bit(int holder_node_id);
static PcmState pcm_transition_target(PcmLockTransition trans);


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

/*
 * Atomically install the queue winner as the sole X holder, but only if the
 * authority still exactly matches the snapshot captured when the transfer
 * began.  No engine lock is held here; callers prepare an immutable token,
 * perform this GRD barrier, then finalize their queue state.
 */
PcmXGrdHandoffResult
cluster_pcm_lock_queue_handoff_x_exact(const PcmXGrdHandoffToken *token)
{
	struct GrdEntry *entry;
	PcmAuthoritySnapshot current;
	ClusterGrdHolderId requester;
	PcmXGrdHandoffResult result = PCM_X_GRD_HANDOFF_BAD_STATE;
	bool found;
	bool broadcast = false;
	uint64 pending_x_value;
	uint32 source_bit;

	if (token == NULL || token->requester_node < 0 || token->requester_node >= 32
		|| token->source_node < 0 || token->source_node >= 32 || token->request_id == 0
		|| token->grant_generation == 0
		|| !cluster_pcm_x_image_id_decode(token->image_id, NULL, NULL))
		return PCM_X_GRD_HANDOFF_INVALID;
	if (!PcmPendingXQueueValue(token->ticket_id, &pending_x_value))
		return PCM_X_GRD_HANDOFF_INVALID;
	if (ClusterPcm == NULL || cluster_pcm_htab == NULL)
		return PCM_X_GRD_HANDOFF_NOT_FOUND;

	requester.node_id = (uint32)token->requester_node;
	requester.procno = token->requester_procno;
	requester.cluster_epoch = token->cluster_epoch;
	requester.request_id = token->request_id;
	source_bit = (uint32)1u << (uint32)token->source_node;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &token->tag, HASH_FIND, &found);
	if (!found || entry == NULL) {
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return PCM_X_GRD_HANDOFF_NOT_FOUND;
	}

	LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
	pcm_authority_snapshot_locked(entry, &current);

	/* A retry after the commit barrier is harmless only for the same full
	 * requester identity and an image no newer than the installed Lamport-SCN
	 * authority.  page_lsn is per-node A-record evidence, not a cross-node
	 * version comparator. */
	if (current.state == PCM_STATE_X && current.x_holder_node == token->requester_node
		&& current.s_holders_bitmap == 0 && current.pending_x_requester_node == -1
		&& memcmp(&current.master_holder, &requester, sizeof(requester)) == 0
		&& (!SCN_VALID(token->page_scn)
			|| scn_local(entry->pi_watermark_scn) >= scn_local(token->page_scn))) {
		result = PCM_X_GRD_HANDOFF_DUPLICATE;
		goto done;
	}

	if (!pcm_authority_snapshot_equal(&current, &token->authority)) {
		result = PCM_X_GRD_HANDOFF_STALE;
		goto done;
	}
	if (current.pending_x_requester_node != token->requester_node
		|| current.pending_x_since_lsn != pending_x_value)
		goto done;
	/* The authority snapshot deliberately excludes watermarks, so close the
	 * SCN window under the same entry lock as the pending-X cookie and GRD
	 * transition.  Cross-node version authority is Lamport page_scn only;
	 * page_lsn belongs to its source WAL stream and remains A-record evidence. */
	if (SCN_VALID(entry->pi_watermark_scn)
		&& (!SCN_VALID(token->page_scn)
			|| scn_local(token->page_scn) < scn_local(entry->pi_watermark_scn)))
		goto done;

	if (current.state == PCM_STATE_X) {
		if (current.x_holder_node != token->source_node || current.s_holders_bitmap != 0
			|| current.master_holder.node_id != (uint32)token->source_node)
			goto done;
	} else if (current.state == PCM_STATE_S) {
		if (current.x_holder_node != -1 || current.s_holders_bitmap != source_bit
			|| current.master_holder.node_id != (uint32)token->source_node)
			goto done;
	} else if (current.state == PCM_STATE_N) {
		/* A cold global-N round has no physical cache holder.  The queue names
		 * the requester as synthetic source so the existing image/A-record path
		 * remains mandatory and the final barrier stays ticket-exact. */
		if (token->source_node != token->requester_node || current.x_holder_node != -1
			|| current.s_holders_bitmap != 0 || pcm_master_holder_is_valid(entry))
			goto done;
	} else {
		goto done;
	}

	pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_X);
	entry->x_holder_node = token->requester_node;
	pg_atomic_write_u32(&entry->s_holders_bitmap, 0);
	entry->s_holder_refcount_local = 0;
	pcm_master_holder_set_exact(entry, &requester);
	entry->pending_x_requester_node = -1;
	entry->pending_x_since_lsn = 0;
	if (SCN_VALID(token->page_scn)
		&& scn_local(token->page_scn) > scn_local(entry->pi_watermark_scn)) {
		SCN old_scn = entry->pi_watermark_scn;

		entry->pi_watermark_scn = token->page_scn;
		pcm_wm_prov_record(token->tag, old_scn, token->page_scn, CLUSTER_PCM_WM_SRC_GRANT_X,
						   token->source_node, token->request_id, token->cluster_epoch);
	}
	entry->last_transition_at = GetCurrentTimestamp();
	pg_atomic_fetch_add_u64(&entry->transition_count_local, 1);
	broadcast = true;
	result = PCM_X_GRD_HANDOFF_OK;

done:
	LWLockRelease(&entry->entry_lock.lock);
	LWLockRelease(&ClusterPcm->htab_lock.lock);
	if (broadcast)
		ConditionVariableBroadcast(&entry->wait_cv);
	return result;
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
	struct GrdEntry *entry;
	bool found;
	PcmPendingXReserveResult result = PCM_PENDING_X_RESERVE_OCCUPIED;

	if (ClusterPcm == NULL || cluster_pcm_htab == NULL || requester_node < 0
		|| requester_node >= 32)
		return PCM_PENDING_X_RESERVE_INVALID;
	/* Admission itself is the first authoritative touch for a cold tag.  Use
	 * the canonical lazy allocator so both legacy and queue writers can arm
	 * the barrier before any transition exists. */
	if (pcm_get_or_create_entry(tag) == NULL)
		return PCM_PENDING_X_RESERVE_NO_CAPACITY;
	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_EXCLUSIVE);
		if (entry->pending_x_requester_node == -1) {
			entry->pending_x_requester_node = requester_node;
			entry->pending_x_since_lsn = value;
			result = PCM_PENDING_X_RESERVE_OK;
		}
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
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
	struct GrdEntry *entry;
	uint64 expected;
	bool found;
	bool exact = false;

	if (ClusterPcm == NULL || cluster_pcm_htab == NULL || requester_node < 0 || requester_node >= 32
		|| !PcmPendingXQueueValue(ticket_id, &expected))
		return false;
	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	if (found && entry != NULL) {
		LWLockAcquire(&entry->entry_lock.lock, LW_SHARED);
		exact = entry->pending_x_requester_node == requester_node
				&& entry->pending_x_since_lsn == expected;
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);
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

	entry = pcm_get_or_create_entry(tag);
	if (entry == NULL)
		return false; /* HC59 cap fail-closed — leave unrebuilt;  survivor re-sends */

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
			pcm_wm_prov_record(tag, wm_old, page_scn, CLUSTER_PCM_WM_SRC_REDECLARE, source_node, 0,
							   cluster_epoch);
		}
	}

	LWLockRelease(&entry->entry_lock.lock);
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
			pcm_wm_prov_record(tag, wm_old, page_scn, CLUSTER_PCM_WM_SRC_TAKE_X, holder_node,
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
	struct GrdEntry *entry;

	if (cluster_pcm_htab == NULL)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("PCM lock manager disabled (cluster.pcm_grd_max_entries=0)")));
	if (requester_node < 0 || requester_node >= 32)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_pcm_lock_master_grant_x_to: requester_node %d out of range",
							   requester_node)));

	entry = pcm_get_or_create_entry(tag);
	if (entry == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("cluster_pcm_lock_master_grant_x_to: PCM GRD HTAB FULL (cap=%d)",
							   pcm_grd_effective)));

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
			pcm_wm_prov_record(tag, wm_old, page_scn, CLUSTER_PCM_WM_SRC_GRANT_X, requester_node,
							   request_id, epoch);
		}
	}
	LWLockRelease(&entry->entry_lock.lock);
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
 *	pcm_wm_prov_record stores the advancing feed into the tag's single
 *	insert-once slot (open addressing, linear probe;  slots are never
 *	evicted so probe chains stay valid).  The caller holds the tag's GRD
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
pcm_wm_prov_record(BufferTag tag, SCN old_scn, SCN new_scn, ClusterPcmWmSrc source,
				   int32 sender_node, uint64 request_id, uint64 epoch)
{
	uint32 start;
	uint32 probe;

	if (ClusterPcm == NULL)
		return;
	start = pcm_wm_prov_hash(&tag) % (uint32)CLUSTER_PCM_WM_PROV_SLOTS;
	LWLockAcquire(&ClusterPcm->wm_prov_lock.lock, LW_EXCLUSIVE);
	for (probe = 0; probe < (uint32)CLUSTER_PCM_WM_PROV_SLOTS; probe++) {
		ClusterPcmWmProvSlot *slot
			= &ClusterPcm->wm_prov[(start + probe) % (uint32)CLUSTER_PCM_WM_PROV_SLOTS];

		if (slot->used && !BufferTagsEqual(&slot->tag, &tag))
			continue;
		/* a free slot (first advance for the tag) or the tag's own slot */
		slot->used = true;
		slot->tag = tag;
		slot->old_scn = old_scn;
		slot->new_scn = new_scn;
		slot->request_id = request_id;
		slot->epoch = epoch;
		slot->sender_node = sender_node;
		slot->source = (uint8)source;
		LWLockRelease(&ClusterPcm->wm_prov_lock.lock);
		return;
	}
	/* table full and the tag has no slot: the advance goes unrecorded */
	pg_atomic_fetch_add_u64(&ClusterPcm->wm_prov_insert_fail_count, 1);
	LWLockRelease(&ClusterPcm->wm_prov_lock.lock);
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
	uint32 start;
	uint32 probe;
	bool full_seen;

	if (ClusterPcm == NULL || out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	out->source = CLUSTER_PCM_WM_SRC_NONE;
	out->sender_node = -1;
	start = pcm_wm_prov_hash(&tag) % (uint32)CLUSTER_PCM_WM_PROV_SLOTS;
	LWLockAcquire(&ClusterPcm->wm_prov_lock.lock, LW_SHARED);
	for (probe = 0; probe < (uint32)CLUSTER_PCM_WM_PROV_SLOTS; probe++) {
		ClusterPcmWmProvSlot *slot
			= &ClusterPcm->wm_prov[(start + probe) % (uint32)CLUSTER_PCM_WM_PROV_SLOTS];

		if (!slot->used)
			break; /* insert-once: an unused slot terminates the probe chain */
		if (!BufferTagsEqual(&slot->tag, &tag))
			continue;
		out->source = (ClusterPcmWmSrc)slot->source;
		out->sender_node = slot->sender_node;
		out->request_id = slot->request_id;
		out->epoch = slot->epoch;
		out->old_scn = slot->old_scn;
		out->new_scn = slot->new_scn;
		LWLockRelease(&ClusterPcm->wm_prov_lock.lock);
		return true;
	}
	full_seen = pg_atomic_read_u64(&ClusterPcm->wm_prov_insert_fail_count) > 0;
	LWLockRelease(&ClusterPcm->wm_prov_lock.lock);
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
			pcm_wm_prov_record(tag, wm_old, page_scn, source, sender_node, request_id, epoch);
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
	bool found;
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
		}
		LWLockRelease(&entry->entry_lock.lock);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return retired;
}


void
cluster_pcm_transition_apply(struct GrdEntry *entry, PcmLockTransition trans, int holder_node_id)
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

	entry->last_transition_at = GetCurrentTimestamp();
	pg_atomic_fetch_add_u64(&entry->transition_count_local, 1);
}

/* The pending-X cookie and S-holder bitmap are both protected by entry_lock.
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
	struct GrdEntry *entry;
	PcmState cur;
	PcmState target;
	uint32 holder_bit;
	bool broadcast_needed = false;

	if (cluster_pcm_htab == NULL)
		return PCM_GCS_TRANSITION_INCOMPATIBLE;
	if (holder_node_id < 0 || holder_node_id >= 32)
		return PCM_GCS_TRANSITION_INCOMPATIBLE;
	if (trans < PCM_TRANS_N_TO_S || trans > PCM_TRANS_S_TO_X_CLEANOUT)
		return PCM_GCS_TRANSITION_INCOMPATIBLE;
	if (trans == PCM_TRANS_S_TO_X_CLEANOUT)
		return PCM_GCS_TRANSITION_INCOMPATIBLE;

	if (trans == PCM_TRANS_N_TO_S || trans == PCM_TRANS_N_TO_X)
		entry = pcm_get_or_create_entry(tag);
	else
		entry = pcm_find_entry(tag);
	if (entry == NULL)
		return PCM_GCS_TRANSITION_INCOMPATIBLE;

	holder_bit = pcm_holder_bit(holder_node_id);
	target = pcm_transition_target(trans);

	pcm_entry_lock_exclusive(entry);
	if (trans == PCM_TRANS_N_TO_S && !pcm_s_admission_allowed_locked(entry, holder_bit)) {
		LWLockRelease(&entry->entry_lock.lock);
		return PCM_GCS_TRANSITION_PENDING_X;
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
		LWLockRelease(&entry->entry_lock.lock);
		return PCM_GCS_TRANSITION_APPLIED;
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
		LWLockRelease(&entry->entry_lock.lock);
		return PCM_GCS_TRANSITION_APPLIED;
	}

	if (!cluster_pcm_transition_legal(cur, target, trans)) {
		LWLockRelease(&entry->entry_lock.lock);
		return PCM_GCS_TRANSITION_INCOMPATIBLE;
	}

	switch (trans) {
	case PCM_TRANS_N_TO_S:
	case PCM_TRANS_N_TO_X:
		break;
	case PCM_TRANS_S_TO_X_UPGRADE:
		if ((pg_atomic_read_u32(&entry->s_holders_bitmap) & holder_bit) == 0
			|| (pg_atomic_read_u32(&entry->s_holders_bitmap) & ~holder_bit) != 0) {
			LWLockRelease(&entry->entry_lock.lock);
			return PCM_GCS_TRANSITION_INCOMPATIBLE;
		}
		break;
	case PCM_TRANS_X_TO_S_DOWNGRADE:
	case PCM_TRANS_X_TO_N_DOWNGRADE:
	case PCM_TRANS_X_TO_N_RELEASE:
		if (entry->x_holder_node != holder_node_id) {
			LWLockRelease(&entry->entry_lock.lock);
			return PCM_GCS_TRANSITION_INCOMPATIBLE;
		}
		break;
	case PCM_TRANS_S_TO_N_INVALIDATE:
	case PCM_TRANS_S_TO_N_RELEASE:
		if ((pg_atomic_read_u32(&entry->s_holders_bitmap) & holder_bit) == 0) {
			LWLockRelease(&entry->entry_lock.lock);
			return PCM_GCS_TRANSITION_INCOMPATIBLE;
		}
		break;
	case PCM_TRANS_S_TO_X_CLEANOUT:
		LWLockRelease(&entry->entry_lock.lock);
		return PCM_GCS_TRANSITION_INCOMPATIBLE;
	}

	cluster_pcm_transition_apply(entry, trans, holder_node_id);
	if ((PcmState)pg_atomic_read_u32(&entry->master_state) == PCM_STATE_N
		|| trans == PCM_TRANS_X_TO_S_DOWNGRADE)
		broadcast_needed = true;
	LWLockRelease(&entry->entry_lock.lock);

	if (broadcast_needed)
		ConditionVariableBroadcast(&entry->wait_cv);
	return PCM_GCS_TRANSITION_APPLIED;
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
pcm_lock_acquire_local(BufferTag tag, PcmLockMode mode, PcmAuthoritySnapshot *remote_x_out)
{
	struct GrdEntry *entry;
	int holder_node;
	uint32 holder_bit;
	bool cv_prepared = false;

	CLUSTER_INJECTION_POINT("cluster-pcm-acquire-entry");

	if (cluster_pcm_htab == NULL)
		PCM_STUB_DISABLED_PATH;

	if (mode != PCM_LOCK_MODE_S && mode != PCM_LOCK_MODE_X)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_pcm_lock_acquire: invalid mode %d (must be S=1 or X=2)",
							   (int)mode)));

	/*
	 * PGRAC: spec-2.32 D5 / spec-2.33 D7 — master lookup branch.  HC72
	 * production self short-circuit is the hot path;  spec-2.33 enables the
	 * real deterministic-hash master lookup so remote-master is now a real
	 * (non-test) outcome in multi-node topologies.
	 *
	 * S/X with remote master needs a block-shipping data plane round-trip
	 * (HC79 GCS_BLOCK_REQUEST/REPLY) which requires a BufferDesc to install
	 * the received bytes into.  This tag-only entry point has no BufferDesc,
	 * so we fail closed with an errhint redirecting the caller to
	 * cluster_pcm_lock_acquire_buffer().  Unit tests / non-bufmgr callers
	 * that legitimately need tag-only semantics MUST stay on the master to
	 * keep working;  any cross-node usage MUST go through the buffer-aware
	 * variant.
	 */
	{
		int master_node = cluster_gcs_lookup_master(tag);

		if (master_node != cluster_node_id) {
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cluster_pcm_lock_acquire: remote-master S/X requires "
								   "BufferDesc-aware path"),
							errhint("Use cluster_pcm_lock_acquire_buffer() instead; the "
									"data plane needs a BufferDesc to install received "
									"block bytes under content_lock EXCLUSIVE (HC84).")));
		}
	}

	holder_node = cluster_node_id;
	if (holder_node < 0 || holder_node >= 32)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_pcm_lock_acquire: cluster_node_id=%d out of [0, 32) range",
							   holder_node)));

	entry = pcm_get_or_create_entry(tag);
	if (entry == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("cluster_pcm_lock_acquire: PCM GRD HTAB FULL (cap=%d)",
							   pcm_grd_effective)));

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
				if (cv_prepared)
					ConditionVariableCancelSleep();
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
				if (cv_prepared)
					ConditionVariableCancelSleep();
				return true;
			}
			/* cur == X → fall through to wait */
		} else /* mode == PCM_LOCK_MODE_X */
		{
			uint32 holders;

			if (cur == PCM_STATE_N) {
				cluster_pcm_transition_apply(entry, PCM_TRANS_N_TO_X, holder_node);
				LWLockRelease(&entry->entry_lock.lock);
				if (cv_prepared)
					ConditionVariableCancelSleep();
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
				if (cv_prepared)
					ConditionVariableCancelSleep();
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
				if (cv_prepared)
					ConditionVariableCancelSleep();
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
					if (cv_prepared)
						ConditionVariableCancelSleep();
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
					if (cv_prepared) {
						ConditionVariableCancelSleep();
						cv_prepared = false;
					}
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
					if (cv_prepared)
						ConditionVariableCancelSleep();
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("cluster_pcm: cross-node write needs local S residency "
										   "while other nodes cache this block"),
									errhint("This node holds no current copy (it never read the "
											"block), so revoking the remote shared copies would "
											"leave no provable-current image to write on.")));
				}

				LWLockRelease(&entry->entry_lock.lock);
				if (cv_prepared)
					ConditionVariableCancelSleep();
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
		if (!cv_prepared) {
			ConditionVariablePrepareToSleep(&entry->wait_cv);
			cv_prepared = true;
		}
		LWLockRelease(&entry->entry_lock.lock);
		ConditionVariableSleep(&entry->wait_cv, WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
		/* loop and re-check master_state */
	}
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
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
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
		bool have_authority = cluster_pcm_lock_authority_snapshot(tag, &authority);

		if (have_authority && authority.state == PCM_STATE_X
			&& authority.x_holder_node >= 0 && authority.x_holder_node != cluster_node_id
			&& authority.s_holders_bitmap == 0
			&& authority.master_holder.node_id == (uint32)authority.x_holder_node)
			return cluster_gcs_local_master_read_image_and_wait(buf, &authority,
														 out_retry_denied);
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

				entry = pcm_find_entry(tag);
				if (entry != NULL) {
					pcm_entry_lock_exclusive(entry);
					/* Amendment v1.2 (R9): the completed S->X upgrade replaces
					 * ALL of this node's S declarations for the tag (the X
					 * grant subsumes them), so the local S refcount is
					 * intentionally hard-reset rather than decremented — a
					 * later release of the X does not owe any S releases. */
					entry->s_holder_refcount_local = 0;
					LWLockRelease(&entry->entry_lock.lock);
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
	struct GrdEntry *entry;
	PcmState cur;
	int holder_node;
	uint32 holder_bit;
	bool broadcast_needed = false;

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

	entry = pcm_find_entry(tag);
	if (entry == NULL)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_pcm_lock_release: no PCM entry for BufferTag (released "
							   "without prior acquire?)")));

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
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_pcm_lock_release: nothing held (state=%d)", (int)cur)));
	}

	LWLockRelease(&entry->entry_lock.lock);

	if (broadcast_needed)
		ConditionVariableBroadcast(&entry->wait_cv);
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
			trans = PCM_TRANS_S_TO_N_RELEASE;
		else if (mode == PCM_LOCK_MODE_X)
			trans = PCM_TRANS_X_TO_N_RELEASE;
		else
			return; /* nothing to release from the remote master */

		cluster_gcs_send_transition_and_wait(tag, trans, master_node);
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

	entry = pcm_find_entry(tag);
	if (entry == NULL)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_pcm_lock_upgrade: no PCM entry for BufferTag (must "
							   "acquire S first)")));

	pcm_entry_lock_exclusive(entry);

	cur = (PcmState)pg_atomic_read_u32(&entry->master_state);
	if (cur != PCM_STATE_S) {
		LWLockRelease(&entry->entry_lock.lock);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cluster_pcm_lock_upgrade: state=%d (must be S to upgrade)", (int)cur)));
	}
	if ((pg_atomic_read_u32(&entry->s_holders_bitmap) & pcm_holder_bit(holder_node)) == 0) {
		LWLockRelease(&entry->entry_lock.lock);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cluster_pcm_lock_upgrade: node %d is not an S holder", holder_node)));
	}
	if ((pg_atomic_read_u32(&entry->s_holders_bitmap) & ~pcm_holder_bit(holder_node)) != 0) {
		LWLockRelease(&entry->entry_lock.lock);
		ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						errmsg("cluster_pcm_lock_upgrade: other S holders still present")));
	}

	cluster_pcm_transition_apply(entry, PCM_TRANS_S_TO_X_UPGRADE, holder_node);

	LWLockRelease(&entry->entry_lock.lock);
}


void
cluster_pcm_lock_downgrade(BufferTag tag, PcmLockMode target_mode, bool keep_pi)
{
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

	entry = pcm_find_entry(tag);
	if (entry == NULL)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_pcm_lock_downgrade: no PCM entry for BufferTag (must "
							   "acquire X first)")));

	pcm_entry_lock_exclusive(entry);

	cur = (PcmState)pg_atomic_read_u32(&entry->master_state);
	if (cur != PCM_STATE_X) {
		LWLockRelease(&entry->entry_lock.lock);
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_pcm_lock_downgrade: state=%d (must be X to downgrade)",
							   (int)cur)));
	}
	if (entry->x_holder_node != holder_node) {
		LWLockRelease(&entry->entry_lock.lock);
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
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster_pcm_lock_downgrade: HC56 validator rejected transition")));
	}

	cluster_pcm_transition_apply(entry, trans, holder_node);

	LWLockRelease(&entry->entry_lock.lock);
}


/* ============================================================
 * Diagnostic / introspection helpers (always-callable).
 * ============================================================ */

PcmLockMode
cluster_pcm_lock_query(BufferTag tag)
{
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

	entry = pcm_find_entry(tag);
	if (entry == NULL)
		return PCM_LOCK_MODE_N;

	state = (PcmState)pg_atomic_read_u32(&entry->master_state);
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
	sz = add_size(sz, hash_estimate_size((Size)eff, sizeof(struct GrdEntry)));
	return sz;
}


void
cluster_pcm_grd_init(void)
{
	bool found;
	HASHCTL info;

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

	pgstat_report_wait_start(WAIT_EVENT_PCM_GRD_INIT);
	ClusterPcm = (ClusterPcmShared *)ShmemInitStruct("pgrac cluster pcm grd hdr",
													 MAXALIGN(sizeof(ClusterPcmShared)), &found);

	if (!found) {
		/*
		 * PGRAC: spec-2.30 D2 — header init (9 atomic uint64 counters
		 * zeroed).  Trans-9 (s_to_x_cleanout) counter starts 0 and stays 0
		 * by HC60 apply-fail-closed.
		 */
		memset(ClusterPcm, 0, sizeof(*ClusterPcm));
		LWLockInitialize(&ClusterPcm->htab_lock.lock, LWTRANCHE_CLUSTER_PCM);
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
static struct GrdEntry *
pcm_get_or_create_entry(BufferTag tag)
{
	struct GrdEntry *entry;
	bool found;

	if (cluster_pcm_htab == NULL)
		return NULL;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_EXCLUSIVE);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		LWLockRelease(&ClusterPcm->htab_lock.lock);
		return NULL; /* HTAB FULL — caller fail-closed */
	}

	if (!found) {
		/*
		 * HC59 fresh entry init.  hash_search wrote tag into entry->tag
		 * (key field) already;  zero / init the rest.
		 */
		BufferTag saved_tag = entry->tag;
		memset(entry, 0, sizeof(*entry));
		entry->tag = saved_tag;
		pg_atomic_init_u32(&entry->master_state, (uint32)PCM_STATE_N);
		entry->x_holder_node = -1;
		entry->master_holder.node_id = INVALID_PCM_MASTER_HOLDER_NODE;
		pg_atomic_init_u32(&entry->s_holders_bitmap, 0);
		pg_atomic_init_u32(&entry->pi_holders_bitmap, 0);
		pg_atomic_init_u64(&entry->transition_count_local, 0);
		entry->s_holder_refcount_local = 0; /* PGRAC: spec-2.31 D1 v0.4 */
		/* PGRAC: spec-2.36 D5 HC117 — S barrier fields default to "none". */
		entry->pending_x_requester_node = -1;
		entry->pending_x_since_lsn = 0;
		/* PGRAC: spec-2.37 D2 HC125+HC126 — PI watermark single field default 0. */
		entry->pi_watermark_lsn = InvalidXLogRecPtr;
		entry->pi_watermark_scn = InvalidScn;	/* spec-2.41 D2 — SCN watermark default */
		ConditionVariableInit(&entry->wait_cv); /* PGRAC: spec-2.31 D1 v0.4 */
		LWLockInitialize(&entry->entry_lock.lock, LWTRANCHE_CLUSTER_PCM);
	}
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return entry;
}


static struct GrdEntry *
pcm_find_entry(BufferTag tag)
{
	struct GrdEntry *entry;
	bool found;

	if (ClusterPcm == NULL || cluster_pcm_htab == NULL)
		return NULL;

	LWLockAcquire(&ClusterPcm->htab_lock.lock, LW_SHARED);
	entry = (struct GrdEntry *)hash_search(cluster_pcm_htab, &tag, HASH_FIND, &found);
	LWLockRelease(&ClusterPcm->htab_lock.lock);

	return (found && entry != NULL) ? entry : NULL;
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
