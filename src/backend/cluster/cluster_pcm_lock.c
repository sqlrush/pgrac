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
#include "cluster/cluster_inject.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "port/atomics.h" /* PGRAC: spec-2.30 D1 — pg_atomic_uint32/64 */
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
	ConditionVariable wait_cv;				 /* spec-2.31 D1 v0.4 incompatible state wait */
	LWLockPadded entry_lock;				 /*128B PG_CACHE_LINE_SIZE — must stay last */
};

/*
 * PGRAC: spec-2.31 D1 v0.4 F2 — GrdEntry size bump from spec-2.30's 216B.
 *
 *	`sizeof(ConditionVariable) == sizeof(slock_t) + sizeof(proclist_head)`
 *	depends on platform alignment (slock_t typically 4B on macOS / Linux;
 *	proclist_head = 8B).  We assert the actual measured size matches an
 *	expected value to catch silent layout drift; if the platform produces
 *	a different size, the assertion fires and the constant must be amended
 *	(with spec Hardening appendix) before ship.
 */
StaticAssertDecl(sizeof(struct GrdEntry) == 232,
				 "spec-2.31 D1 v0.4 GrdEntry size 216 → 232 (added s_holder_refcount_local "
				 "2B replacing 4B _pad1 → 2B + 2B align; + ConditionVariable wait_cv 12-16B "
				 "+ LWLockPadded alignment); spec-2.30 was 216B; spec-2.30 Hardening v1.0.2 "
				 "forward-link appendix to be added at ship-level closeout");


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
static struct GrdEntry *pcm_find_entry(BufferTag tag);
static void pcm_entry_lock_exclusive(struct GrdEntry *entry);
static uint32 pcm_holder_bit(int holder_node_id);


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
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_n_to_s_count, 1);
		break;
	case PCM_TRANS_N_TO_X:
		pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_X);
		entry->x_holder_node = holder_node_id;
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_n_to_x_count, 1);
		break;
	case PCM_TRANS_S_TO_X_UPGRADE:
		pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_X);
		pg_atomic_fetch_and_u32(&entry->s_holders_bitmap, ~holder_bit);
		entry->x_holder_node = holder_node_id;
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_s_to_x_upgrade_count, 1);
		break;
	case PCM_TRANS_X_TO_S_DOWNGRADE:
		pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_S);
		pg_atomic_fetch_or_u32(&entry->s_holders_bitmap, holder_bit);
		pg_atomic_fetch_or_u32(&entry->pi_holders_bitmap, holder_bit); /* HC58 PI set */
		entry->x_holder_node = -1;
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_x_to_s_downgrade_count, 1);
		break;
	case PCM_TRANS_X_TO_N_DOWNGRADE:
		pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_N);
		pg_atomic_fetch_or_u32(&entry->pi_holders_bitmap, holder_bit); /* HC58 PI set */
		entry->x_holder_node = -1;
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_x_to_n_downgrade_count, 1);
		break;
	case PCM_TRANS_X_TO_N_RELEASE:
		pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_N);
		entry->x_holder_node = -1;
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_x_to_n_release_count, 1);
		break;
	case PCM_TRANS_S_TO_N_INVALIDATE:
		pg_atomic_fetch_and_u32(&entry->s_holders_bitmap, ~holder_bit);
		/* If no S holders remain after this clear, advance to N. */
		if (pg_atomic_read_u32(&entry->s_holders_bitmap) == 0)
			pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_N);
		pg_atomic_fetch_add_u64(&ClusterPcm->trans_s_to_n_invalidate_count, 1);
		break;
	case PCM_TRANS_S_TO_N_RELEASE:
		pg_atomic_fetch_and_u32(&entry->s_holders_bitmap, ~holder_bit);
		if (pg_atomic_read_u32(&entry->s_holders_bitmap) == 0)
			pg_atomic_write_u32(&entry->master_state, (uint32)PCM_STATE_N);
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


void
cluster_pcm_lock_acquire(BufferTag tag, PcmLockMode mode)
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

		pcm_entry_lock_exclusive(entry);

		cur = (PcmState)pg_atomic_read_u32(&entry->master_state);

		if (mode == PCM_LOCK_MODE_S) {
			if (cur == PCM_STATE_N) {
				cluster_pcm_transition_apply(entry, PCM_TRANS_N_TO_S, holder_node);
				entry->s_holder_refcount_local = 1;
				LWLockRelease(&entry->entry_lock.lock);
				if (cv_prepared)
					ConditionVariableCancelSleep();
				return;
			}
			if (cur == PCM_STATE_S) {
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
				return;
			}
			/* cur == X → fall through to wait */
		} else /* mode == PCM_LOCK_MODE_X */
		{
			if (cur == PCM_STATE_N) {
				cluster_pcm_transition_apply(entry, PCM_TRANS_N_TO_X, holder_node);
				LWLockRelease(&entry->entry_lock.lock);
				if (cv_prepared)
					ConditionVariableCancelSleep();
				return;
			}
			/* cur == S or X → fall through to wait */
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


void
cluster_pcm_lock_upgrade(BufferTag tag)
{
	struct GrdEntry *entry;
	PcmState cur;
	int holder_node;

	CLUSTER_INJECTION_POINT("cluster-pcm-convert-pre");

	if (cluster_pcm_htab == NULL)
		PCM_STUB_DISABLED_PATH;

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
	else {
		LWLockRelease(&entry->entry_lock.lock);
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_pcm_lock_downgrade: illegal target_mode=%d keep_pi=%d",
							   (int)target_mode, keep_pi)));
	}

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
		pg_atomic_init_u32(&entry->s_holders_bitmap, 0);
		pg_atomic_init_u32(&entry->pi_holders_bitmap, 0);
		pg_atomic_init_u64(&entry->transition_count_local, 0);
		entry->s_holder_refcount_local = 0;		/* PGRAC: spec-2.31 D1 v0.4 */
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
