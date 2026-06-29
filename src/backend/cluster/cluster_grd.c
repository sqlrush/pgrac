/*-------------------------------------------------------------------------
 *
 * cluster_grd.c
 *	  Global Resource Directory (GRD) routing substrate — spec-2.14.
 *
 *	  Implements ClusterResId 16-byte canonical wire encoding + 4096 shard
 *	  routing via hash_bytes_extended (PG-native) + declared-node-aware
 *	  static master map + observability accessors.
 *
 *	  See cluster_grd.h for the protocol contract, scope边界, performance
 *	  hook design (Stage 6 swap point), counter invariant.
 *	  See spec-2.14-grd-resource-identity-shard-routing.md (frozen v0.4)
 *	  for design rationale.
 *
 *	  AD-002 PCM vs GES 分工 + AD-011 不移植 LC/RC Lock.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_grd.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Substrate routing layer only;
 *	  entry holders/waiters/hash table lands in spec-2.15;  caller-side
 *	  integration in spec-2.15+; cross-node real send in spec-2.16.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_ges.h"	  /* GesRequestPayload / RELEASE cleanup payload */
#include "cluster/cluster_ges_mode.h" /* spec-5.1b D1: ges_modes_compatible (frozen matrix) */
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h" /* cluster_grd_outbound_enqueue_cleanup_release (D10) */
#include "cluster/cluster_hw_remaster.h" /* spec-5.7 D3 S5d — HW authority rebuild launch + gate */
#include "cluster/cluster_lmd.h"		 /* spec-2.24 D10 cleanup_*_count_inc */
#include "cluster/cluster_guc.h"		 /* cluster_node_id, cluster_grd_max_entries */
#include "cluster/cluster_lms.h"		 /* spec-4.6 D2 — Q3-C wire routing token */
#include "cluster/cluster_pcm_lock.h"	 /* spec-2.36 HC124 pending_x node-dead cleanup */
#include "cluster/cluster_gcs.h"		 /* spec-4.7 D2 — cluster_gcs_lookup_master */
#include "cluster/cluster_membership.h"	 /* spec-5.16 D1 — cluster_membership_is_member */
#include "cluster/cluster_gcs_block.h"	 /* spec-4.7 D2 — block re-declare scan + send */
#include "cluster/cluster_signal.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_cssd.h"			 /* spec-2.16 D8 newly-dead bitmap diff */
#include "cluster/cluster_epoch.h"			 /* spec-4.6 D1 — accepted epoch reads */
#include "cluster/cluster_reconfig.h"		 /* spec-4.6 D1 — reconfig event consume */
#include "cluster/cluster_thread_recovery.h" /* spec-4.11 D3 — unfreeze gate */
#include "storage/procsignal.h"				 /* spec-4.6 D3 — redeclare broadcast */
#include "storage/sinvaladt.h"				 /* spec-4.6 D3 — BackendIdGetProc */
#include "utils/timestamp.h"				 /* spec-4.6 D1 — barrier deadline */
#include "storage/proc.h"					 /* spec-2.17 D8 — MyProc->cluster_grd_bast_pending */
#include "common/hashfn.h"					 /* hash_bytes_extended (spec-2.29 同款) */
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/elog.h"
#include "utils/hsearch.h"


/* ============================================================
 * Shmem region state.
 * ============================================================ */

static ClusterGrdShared *cluster_grd_state = NULL;

/* spec-2.15 v0.3 P1.3:  Per-shard LWLock array (named tranche).  4096
 * LWLock managed by PG lwlock.c — cluster_grd_shmem_init only obtains
 * the array pointer; PG auto-initializes the lock objects. */
static LWLockPadded *cluster_grd_shard_locks = NULL;

/* spec-2.15:  HTAB for entry storage.  NULL when cluster.grd_max_entries
 * = 0 (skeleton mode → NOT_READY sentinel) or non-cluster builds. */
static HTAB *cluster_grd_entry_htab = NULL;

/* spec-2.15 v0.4 P1.1:  HTAB init size = Max(GUC, PGRAC_GRD_SHARD_COUNT)
 * — HASH_PARTITION=4096 forces dynahash nbuckets >= 4096 (nbuckets =
 * max(next_pow2(n), num_partitions)).  naive max_size=GUC=16 would let
 * ShmemInitHash severely under-estimate size → init FATAL.  Use
 * hash_estimate_size(hash_init_max_size, sizeof(ClusterGrdEntry)) for
 * real reservation.  Cached at shmem_size first-call for diagnostic
 * consistency (size_fn must stay pure per I15).
 */
static Size cluster_grd_entries_alloc_bytes = 0;


/* ============================================================
 * spec-2.15:  Private file-static entry struct body (P1.1 opaque body —
 *   header only declares opaque handle).  struct layout reserves
 *   holders/waiters/converts arrays for spec-2.16 mutator API;  本 spec
 *   仅初始化 zero,无 mutation 路径.
 *
 *   v0.3 scope 收紧 P1.3:  cap constants live here (private),不暴露;
 *   spec-2.16 mutator API public extern 时再 expose 或 keep private.
 * ============================================================ */

#define PGRAC_GRD_MAX_HOLDERS 16
#define PGRAC_GRD_MAX_WAITERS 16
#define PGRAC_GRD_MAX_CONVERTS 8

typedef struct ClusterGrdHolder {
	int32 node_id;
	uint32 procno;
	uint64 cluster_epoch;
	uint64 request_id;
	LOCKMODE mode;
} ClusterGrdHolder;

typedef struct ClusterGrdWaiter {
	/*
	 * spec-2.23 D6 / FU amend — full reply identity so the LMS
	 * release-and-pop path can route GES_REPLY GRANT back to the
	 * originating backend.  spec-2.21 ship version stored only
	 * (node_id, mode, wait_start);  the procno/request_id/request_opcode
	 * + source_node_id additions are NEW for spec-2.23.
	 */
	int32 node_id;					/* legacy: equal to source_node_id; kept for binary compat */
	int32 source_node_id;			/* node hosting the waiting backend */
	uint32 procno;					/* PG ProcNumber of the waiting backend */
	uint64 cluster_epoch;			/* epoch at waiter enqueue time */
	uint64 request_id;				/* per-backend monotonic id (for 5-tuple reply key) */
	TransactionId waiter_xid;		/* spec-5.8 D1c — waiter's xid for the WFG vertex */
	uint64 wait_seq;				/* spec-5.8 D1e — waiter's D1d wait-state seq */
	uint64 shard_master_generation; /* spec-2.27 dedup key carry */
	uint32 request_opcode;			/* GesRequestOpcode of the queued request */
	LOCKMODE mode;
	TimestampTz wait_start;
	/*
	 * spec-5.10 D1 — GES enqueue lock-starvation fairness state.  Master-local
	 * and shmem-only (NEVER on the wire);  fair_queue_seq is a per-entry
	 * monotonic ordering key (D4, minted at enqueue/barrier time, 0 until
	 * minted) and is NOT a wait identity / cancel token — the canonical wait
	 * identity (waiter_xid + wait_seq) above is spec-5.8-owned (Q12 / L5).
	 */
	uint64 fair_queue_seq; /* master-local monotonic order (D4) */
	uint32 skip_count;	   /* scan-on-grant jump count (D2) */
	bool boosted;		   /* head-of-line boosted once skip_count >= max_skips (D2) */
} ClusterGrdWaiter;

/*
 * spec-5.1b D2 — ClusterGrdConvert was promoted to cluster_grd.h (full
 * struct: locator (node,procno,current_mode) vs convert_request_id reply
 * key) so the convert state machine + drain helper are a public,
 * unit-testable surface.  The entry below embeds the converts[] array of
 * the header type.
 */

struct ClusterGrdEntry {
	ClusterResId resid; /* hash key (16B) */
	slock_t lock;		/* entry-level spinlock (Q11 + P1.3 minor) */
	int ngranted;
	ClusterGrdHolder holders[PGRAC_GRD_MAX_HOLDERS];
	int nwaiters;
	ClusterGrdWaiter waiters[PGRAC_GRD_MAX_WAITERS];
	int nconverts;
	ClusterGrdConvert converts[PGRAC_GRD_MAX_CONVERTS];
	uint64 last_modified_scn;
	uint32 state_flags; /* 预留 spec-2.16 grant pending/DRM in-flight */
	/*
	 * spec-2.21 D5 ABI extend:
	 *   generation:  bumped on every mutator under entry->lock; S5 promote
	 *     compares against S3 snapshot to detect race (P2.3 revalidate).
	 *   nreservations:  pending reservations count (S3 → S5 promote window).
	 *   reservations:  pending reservation slots (reuse holders[] LOCKMODE
	 *     semantic;identified by holder.request_id;not yet a real holder).
	 */
	uint64 generation;
	int nreservations;
	struct {
		ClusterGrdHolderId id;
		LOCKMODE mode;
	} reservations[PGRAC_GRD_MAX_HOLDERS];
	/*
	 * spec-5.10 D4 — master-local monotonic mint source for waiter/convert
	 * fair_queue_seq ordering (entry->lock protected; 0 means "nothing minted
	 * yet").  Minted ONLY when a request actually enqueues / is barriered
	 * (P1c) — never for a grant or a scan-on-grant pass.
	 */
	uint64 fair_queue_next;
};


/* ============================================================
 * spec-5.8 D1b — master-side wait-for-graph (WFG) edge authority.
 *
 *	The GRD master holds the authoritative holder/waiter state for every
 *	cluster resource, so it is the only place that can build a deadlock
 *	wait-for graph whose edges follow the CURRENT holders (spec-5.8 §3.1
 *	E1/E2).  A waiter is blocked by exactly the granted holders whose mode
 *	conflicts with its wanted mode; a backend never blocks on its own hold
 *	(same-backend self-exclusion, mirroring the enqueue conflict scan).  Each
 *	such (waiter, blocker) pair is one WFG edge in the LMD graph (multi-edge
 *	keyed on both 4-tuples per spec-5.8 D1a).
 *
 *	Lock discipline (CLAUDE.md 规则 16):  the GRD entry is guarded by a
 *	spinlock (entry->lock) while the LMD wait-for graph is LWLock-protected,
 *	and an LWLock must never be acquired under a spinlock.  So the edge set is
 *	rebuilt in two steps — grd_wfg_snapshot_locked() copies the holder /
 *	waiter / convert identities under entry->lock, then grd_wfg_resync_entry()
 *	applies them to the graph AFTER the spinlock is released.  This is the same
 *	collect-under-lock / act-after-unlock pattern the targeted BAST path
 *	already uses (conflict_holders_out).
 *
 *	D1b registers edges on the 4-tuple identity only (vertex xid is
 *	InvalidTransactionId); the waiter xid carry + persistence is spec-5.8 D1c.
 *	Edge registration is best-effort: a full WFG table must never fail a lock
 *	request (the finite request timeout still backstops).  No cancellation
 *	fires until the spec-5.8 D3 two-round gate + D5 wait-state revalidate are
 *	wired, so a transiently stale/leaked edge cannot mis-cancel a live
 *	transaction in this deliverable.
 * ============================================================ */

typedef struct GrdWfgHolderSnap {
	int32 node_id;
	uint32 procno;
	uint64 cluster_epoch;
	uint64 request_id;
	LOCKMODE mode;
} GrdWfgHolderSnap;

typedef struct GrdWfgWaiterSnap {
	int32 node_id;
	uint32 procno;
	uint64 cluster_epoch;
	uint64 request_id;		  /* REQUEST waiter id, or convert_request_id for a convert */
	TransactionId waiter_xid; /* spec-5.8 D1c — xid stamped on the WFG vertex */
	uint64 wait_seq;		  /* spec-5.8 D1e — wait_seq stamped on the WFG vertex */
	LOCKMODE mode;			  /* wanted mode (requested_mode for a convert) */
	/* spec-5.10 D5 — fairness state carried so refresh can RE-EMIT the
	 * FAIRNESS_BARRIER edge (R -> earlier boosted conflicting waiter) on every
	 * resync, mirroring the grant-time / serve-time derivation. */
	bool boosted;		   /* this waiter is boosted to head-of-line */
	uint64 fair_queue_seq; /* master-local order (barrier "earlier-than" test) */
} GrdWfgWaiterSnap;

typedef struct GrdWfgSnapshot {
	int n_holders;
	GrdWfgHolderSnap holders[PGRAC_GRD_MAX_HOLDERS];
	int n_waiters; /* queued REQUEST waiters + pending converts */
	GrdWfgWaiterSnap waiters[PGRAC_GRD_MAX_WAITERS + PGRAC_GRD_MAX_CONVERTS];
} GrdWfgSnapshot;

/* spec-5.10 D4 — wrap-safe fair_queue_seq order (defined with the other
 * starvation helpers below; forward-declared here because the spec-5.8 WFG
 * refresh re-emits the FAIRNESS_BARRIER edge and needs the "earlier-than"
 * test). */
static inline bool grd_fair_seq_precedes(uint64 a, uint64 b);

/* Build an LMD vertex from a 4-tuple identity + sort-metadata xid + wait_seq
 * (spec-5.8 D1c/D1e).  xid / wait_seq are sort/revalidate metadata, never part
 * of the identity; GES-holder blockers carry 0 (only waiters carry real
 * metadata, threaded from the GES request / convert at enqueue time). */
static void
grd_wfg_make_vertex(int32 node_id, uint32 procno, uint64 cluster_epoch, uint64 request_id,
					TransactionId xid, uint64 wait_seq, ClusterLmdVertex *out)
{
	memset(out, 0, sizeof(*out));
	out->node_id = node_id;
	out->procno = procno;
	out->cluster_epoch = cluster_epoch;
	out->request_id = request_id;
	out->xid = xid;
	out->local_start_ts_ms = 0;
	out->wait_seq = wait_seq;
}

/* Caller holds entry->lock.  Copy holders + queued waiters + pending converts
 * into *snap (identities + modes only — no LMD graph lock taken here). */
static void
grd_wfg_snapshot_locked(const ClusterGrdEntry *entry, GrdWfgSnapshot *snap)
{
	int i;

	snap->n_holders = 0;
	for (i = 0; i < entry->ngranted && snap->n_holders < PGRAC_GRD_MAX_HOLDERS; i++) {
		GrdWfgHolderSnap *h = &snap->holders[snap->n_holders++];

		h->node_id = entry->holders[i].node_id;
		h->procno = entry->holders[i].procno;
		h->cluster_epoch = entry->holders[i].cluster_epoch;
		h->request_id = entry->holders[i].request_id;
		h->mode = entry->holders[i].mode;
	}

	snap->n_waiters = 0;
	for (i = 0;
		 i < entry->nwaiters && snap->n_waiters < PGRAC_GRD_MAX_WAITERS + PGRAC_GRD_MAX_CONVERTS;
		 i++) {
		GrdWfgWaiterSnap *w = &snap->waiters[snap->n_waiters++];

		w->node_id = entry->waiters[i].node_id;
		w->procno = entry->waiters[i].procno;
		w->cluster_epoch = entry->waiters[i].cluster_epoch;
		w->request_id = entry->waiters[i].request_id;
		w->waiter_xid = entry->waiters[i].waiter_xid;
		w->wait_seq = entry->waiters[i].wait_seq;
		w->mode = entry->waiters[i].mode;
		w->boosted = entry->waiters[i].boosted;				  /* spec-5.10 D5 */
		w->fair_queue_seq = entry->waiters[i].fair_queue_seq; /* spec-5.10 D5 */
	}
	for (i = 0;
		 i < entry->nconverts && snap->n_waiters < PGRAC_GRD_MAX_WAITERS + PGRAC_GRD_MAX_CONVERTS;
		 i++) {
		GrdWfgWaiterSnap *w = &snap->waiters[snap->n_waiters++];

		/* A pending convert blocks on its requested (target) mode; its vertex
		 * id uses the convert's own reply key (convert_request_id). */
		w->node_id = entry->converts[i].node_id;
		w->procno = entry->converts[i].procno;
		w->cluster_epoch = entry->converts[i].cluster_epoch;
		w->request_id = entry->converts[i].convert_request_id;
		w->waiter_xid = entry->converts[i].waiter_xid;
		w->wait_seq = entry->converts[i].wait_seq;
		w->mode = entry->converts[i].requested_mode;
		w->boosted = entry->converts[i].boosted;			   /* spec-5.10 D5 */
		w->fair_queue_seq = entry->converts[i].fair_queue_seq; /* spec-5.10 D5 */
	}
}

/* Refresh one waiter's WFG edges against a holder snapshot (spec-5.8 D1b
 * refresh_waiter_edges primitive — runs AFTER entry->lock is released; the
 * snapshot is the holder set as of the locked instant).  Removes every prior
 * edge of the waiter, then submits one edge per current conflicting holder. */
static void
grd_wfg_refresh_waiter_edges(const GrdWfgSnapshot *snap, const GrdWfgWaiterSnap *w)
{
	ClusterLmdVertex waiter_v;
	int i;

	grd_wfg_make_vertex(w->node_id, w->procno, w->cluster_epoch, w->request_id, w->waiter_xid,
						w->wait_seq, &waiter_v);
	cluster_lmd_cancel_wait_edge_real(&waiter_v); /* clear stale edges first */

	for (i = 0; i < snap->n_holders; i++) {
		ClusterLmdVertex blocker_v;

		/* spec-5.1c D5 self-exclusion: a backend never blocks on its own hold. */
		if (snap->holders[i].node_id == w->node_id && snap->holders[i].procno == w->procno)
			continue;
		/* Only conflicting holders block the waiter (frozen 8x8 matrix). */
		if (ges_modes_compatible(snap->holders[i].mode, w->mode))
			continue;
		grd_wfg_make_vertex(snap->holders[i].node_id, snap->holders[i].procno,
							snap->holders[i].cluster_epoch, snap->holders[i].request_id,
							InvalidTransactionId, 0, &blocker_v);
		(void)cluster_lmd_submit_wait_edge_real(&waiter_v, &blocker_v, w->request_id);
	}

	/*
	 * spec-5.10 D5 — re-emit the FAIRNESS_BARRIER edge so it survives this
	 * resync.  A barriered request R is held behind the earliest boosted
	 * conflicting waiter W (the same derivation as the grant-time / serve-time
	 * barrier, run here on the snapshot).  W is a spec-5.8-owned canonical
	 * vertex built from the snapshot (P0#3 — never fabricated).  Without this
	 * re-emit the cancel above would drop the edge and a barrier deadlock would
	 * be missed (R2: edge-visible-before-block holds across every refresh).
	 */
	if (cluster_grd_starvation_protection_enabled()) {
		int best = -1;

		for (i = 0; i < snap->n_waiters; i++) {
			const GrdWfgWaiterSnap *cand = &snap->waiters[i];

			if (!cand->boosted)
				continue;
			if (cand->node_id == w->node_id && cand->procno == w->procno
				&& cand->cluster_epoch == w->cluster_epoch && cand->request_id == w->request_id)
				continue; /* a waiter never barriers itself */
			if (ges_modes_compatible(cand->mode, w->mode))
				continue; /* no conflict -> does not block R */
			if (!grd_fair_seq_precedes(cand->fair_queue_seq, w->fair_queue_seq))
				continue; /* W must be EARLIER than R */
			if (best < 0
				|| grd_fair_seq_precedes(cand->fair_queue_seq, snap->waiters[best].fair_queue_seq))
				best = i;
		}
		if (best >= 0) {
			ClusterLmdVertex blocker_v;

			grd_wfg_make_vertex(snap->waiters[best].node_id, snap->waiters[best].procno,
								snap->waiters[best].cluster_epoch, snap->waiters[best].request_id,
								snap->waiters[best].waiter_xid, snap->waiters[best].wait_seq,
								&blocker_v);
			(void)cluster_lmd_submit_wait_edge_real(&waiter_v, &blocker_v, w->request_id);
		}
	}
}

/* spec-5.8 D1b — re-sync the WFG edge set for one resource after a master-side
 * holder/waiter mutation.  `departed` lists waiters that LEFT the queue
 * (granted / cancelled) so their edges are removed even if the entry emptied
 * and was reclaimed; every still-queued waiter + pending convert is then
 * refreshed against the current holders.  Best-effort: the submit/cancel calls
 * self-gate (no-op) when the LMD graph is unavailable. */
static void
grd_wfg_resync_entry(const ClusterResId *resid, const ClusterGrdHolderId *departed, int n_departed)
{
	ClusterGrdEntry *entry = NULL;
	GrdWfgSnapshot snap;
	int i;

	/* (1) Remove edges of waiters that left the queue (identity-only; works
	 *	   even if the entry was reclaimed when it emptied). */
	for (i = 0; i < n_departed; i++) {
		ClusterLmdVertex v;

		grd_wfg_make_vertex((int32)departed[i].node_id, departed[i].procno,
							departed[i].cluster_epoch, departed[i].request_id, InvalidTransactionId,
							0, &v);
		cluster_lmd_cancel_wait_edge_real(&v);
	}

	/* (2) Snapshot the current entry state under entry->lock, then refresh
	 *	   every still-queued waiter / convert after releasing the spinlock. */
	if (cluster_grd_entry_lookup_or_create(resid, false, &entry) != CLUSTER_GRD_ENTRY_OK
		|| entry == NULL)
		return; /* entry gone (emptied) — nothing left to refresh */

	SpinLockAcquire(&entry->lock);
	grd_wfg_snapshot_locked(entry, &snap);
	SpinLockRelease(&entry->lock);
	cluster_grd_entry_release(entry);

	for (i = 0; i < snap.n_waiters; i++)
		grd_wfg_refresh_waiter_edges(&snap, &snap.waiters[i]);
}

/* Resync after a release/drain that granted `n` identities (now departed from
 * the wait queue). */
static void
grd_wfg_resync_after_grants(const ClusterResId *resid, const ClusterGrdGrantIdentity *granted,
							int n)
{
	ClusterGrdHolderId departed[PGRAC_GRD_MAX_WAITERS + PGRAC_GRD_MAX_CONVERTS];
	int i, nd = 0;

	for (i = 0; i < n && nd < (int)lengthof(departed); i++)
		departed[nd++] = granted[i].holder;
	grd_wfg_resync_entry(resid, departed, nd);
}

/* Resync after a release-and-pop that granted `n` REQUEST waiters. */
static void
grd_wfg_resync_after_pops(const ClusterResId *resid, const ClusterGrdWaiterIdentity *granted, int n)
{
	ClusterGrdHolderId departed[PGRAC_GRD_MAX_WAITERS];
	int i, nd = 0;

	for (i = 0; i < n && nd < (int)lengthof(departed); i++)
		departed[nd++] = granted[i].holder;
	grd_wfg_resync_entry(resid, departed, nd);
}


/* ============================================================
 * spec-2.15 v0.3 P1.1:  named tranche request hook.  Single-call
 *   contract — invoked once by cluster_request_shmem() inside the
 *   process_shmem_requests_in_progress window.  size_fn stays pure so
 *   diagnostic paths (cluster_shmem_get_total_bytes) can call it N
 *   times without triggering RequestNamedLWLockTranche (which is
 *   restricted to the request phase).
 * ============================================================ */

void
cluster_grd_request_lwlocks(void)
{
	RequestNamedLWLockTranche("ClusterGrdShard", PGRAC_GRD_SHARD_COUNT);
	/* spec-2.16 D4/D5:  outbound ring + work queue named tranches.
	 * Same process_shmem_requests_in_progress lifecycle window — co-
	 * located here so cluster_unit standalone tests piggyback on the
	 * existing cluster_grd_request_lwlocks stub (L104). */
	RequestNamedLWLockTranche("ClusterGrdOutbound", 1);
	RequestNamedLWLockTranche("ClusterGrdWorkQueue", 1);
}


/* ============================================================
 * Shmem region lifecycle.
 *
 *   spec-2.15 v0.4 P1.1:  entry HTAB allocation gated on
 *   cluster.grd_max_entries GUC.  GUC=0 → only ClusterGrdShared
 *   allocated (skeleton mode, lookup_or_create returns NOT_READY).
 *   GUC>0 → hash_init_max_size = Max(GUC, PGRAC_GRD_SHARD_COUNT) and
 *   ShmemInitHash uses that size; grd_allocated_bytes reflects the
 *   hash_estimate_size() pre-computation.
 * ============================================================ */

static Size
grd_entries_init_max_size(void)
{
	/* v0.4 P1.1:  HASH_PARTITION=4096 forces nbuckets >= 4096; raise
	 * the dynahash init max_size to match so the ShmemInitHash
	 * reservation is realistic. */
	if (cluster_grd_max_entries <= 0)
		return 0;
	return Max((Size)cluster_grd_max_entries, (Size)PGRAC_GRD_SHARD_COUNT);
}

static Size
grd_entries_estimate_bytes(void)
{
	Size init_max_size = grd_entries_init_max_size();

	if (init_max_size == 0)
		return 0;
	return hash_estimate_size(init_max_size, sizeof(ClusterGrdEntry));
}

Size
cluster_grd_shmem_size(void)
{
	/* size_fn MUST stay pure (idempotent) per I15 — cluster_shmem_get_
	 * total_bytes() calls this N times for diagnostics.  No side effect
	 * (no RequestNamedLWLockTranche, no global state mutation). */
	return add_size(sizeof(ClusterGrdShared), grd_entries_estimate_bytes());
}

void
cluster_grd_shmem_init(void)
{
	bool found;
	Size entry_alloc = grd_entries_estimate_bytes();

	cluster_grd_state = ShmemInitStruct("pgrac cluster grd", sizeof(ClusterGrdShared), &found);
	if (!found) {
		int i;

		/* spec-2.14 D3 init zero (Q9 all-atomic, no LWLock). */
		for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
			pg_atomic_init_u32(&cluster_grd_state->master[i], 0);
			/* spec-4.6 D2/D1: remaster generation + recovery phase. */
			pg_atomic_init_u32(&cluster_grd_state->master_generation[i], 0);
			pg_atomic_init_u32(&cluster_grd_state->shard_phase[i], (uint32)GRD_SHARD_NORMAL);
		}
		pg_atomic_init_u32(&cluster_grd_state->master_map_initialized, 0);
		pg_atomic_init_u64(&cluster_grd_state->resid_encode_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->shard_lookup_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->local_master_lookup_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->remote_master_lookup_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->master_map_refresh_count, 0);

		/* spec-2.15 v0.3 NEW counters.  entry_current_count is the
		 * current-size source for cap checks and grd_entry_count; the
		 * three lifetime counters are exposed as pg_cluster_state rows. */
		pg_atomic_init_u64(&cluster_grd_state->entry_current_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->entry_create_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->entry_lookup_hit_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->entry_full_count, 0);

		/* spec-2.16 D1:  4 cap counter + 5 nofail counter
		 * (skeleton-init;  mutator + nofail path 真激活在 Step 2-4). */
		pg_atomic_init_u64(&cluster_grd_state->holders_full_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->waiters_full_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->converts_full_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ngranted_promoted_count, 0);

		/* spec-5.1b D9 — convert state-machine counters. */
		pg_atomic_init_u64(&cluster_grd_state->convert_granted_inplace_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->convert_enqueued_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->convert_illegal_count, 0);

		/* spec-5.10 — lock-starvation fairness: protection on by default. */
		pg_atomic_init_u32(&cluster_grd_state->starvation_protection_enabled, 1);
		pg_atomic_init_u32(&cluster_grd_state->starvation_sweep_pending, 0);
		pg_atomic_init_u64(&cluster_grd_state->starvation_boost_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->starvation_barrier_enqueued_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->starvation_barrier_publish_fail_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->starvation_max_skip_observed, 0);

		pg_atomic_init_u64(&cluster_grd_state->ges_work_queue_full_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_cleanup_deferred_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_inbound_validation_fail_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->cleanup_skip_stale_cancel_count, 0);
		/* spec-2.25 D13 — RELATION + OBJECT cluster gate hit counter. */
		pg_atomic_init_u64(&cluster_grd_state->relation_object_cluster_path_count, 0);
		/* spec-2.26 D5 — TRANSACTION cluster gate hit counter. */
		pg_atomic_init_u64(&cluster_grd_state->transaction_cluster_path_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_reply_deferred_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_reply_dropped_count, 0);

		/* spec-2.17 D28b — generation init 从 1(0 reserved sentinel). */
		pg_atomic_init_u64(&cluster_grd_state->next_generation, 1);

		/* spec-2.17 D12 — 6 BAST counter init 0. */
		pg_atomic_init_u64(&cluster_grd_state->ges_bast_sent_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_bast_received_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_bast_ack_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_bast_retry_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_bast_reject_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_bast_stale_drop_count, 0);

		/* spec-2.17 D26c — 3 deadlock chunked counter init 0. */
		pg_atomic_init_u64(&cluster_grd_state->ges_deadlock_probe_drop_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_deadlock_probe_collision_drop_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->ges_deadlock_chunk_oo_buffer_overflow_count, 0);

		/* spec-4.6 D2/D5 — remaster epoch + 13 grd_recovery counters. */
		pg_atomic_init_u64(&cluster_grd_state->reconfig_remaster_epoch, 0);
		/* spec-4.6 D1 — recovery sequence cursor. */
		pg_atomic_init_u64(&cluster_grd_state->recovery_last_event_id, 0);
		pg_atomic_init_u32(&cluster_grd_state->recovery_state, (uint32)GRD_RECOVERY_IDLE);
		for (i = 0; i < (CLUSTER_MAX_NODES + 63) / 64; i++)
			pg_atomic_init_u64(&cluster_grd_state->recovery_dead_bitmap[i], 0);
		pg_atomic_init_u64(&cluster_grd_state->recovery_event_old_epoch, 0);
		pg_atomic_init_u64(&cluster_grd_state->recovery_redeclare_generation, 0);
		pg_atomic_init_u64(&cluster_grd_state->recovery_barrier_deadline, 0);
		/* spec-4.6 P0#3 cluster gate — per-node barrier-done epochs. */
		for (i = 0; i < CLUSTER_MAX_NODES; i++)
			pg_atomic_init_u64(&cluster_grd_state->recovery_done_epoch[i], 0);
		pg_atomic_init_u64(&cluster_grd_state->remaster_started_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->remaster_done_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->remaster_failed_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->shards_remastered_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->holders_redeclared_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->holders_rebound_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->waiters_requeued_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->converts_requeued_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->stale_request_drop_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->rebuild_timeout_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->block_path_failclosed_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->unaffected_holder_survived_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->stale_holder_swept_count, 0);

		/* spec-5.16 D2/D3b/D5 — online-join remaster fence + counters. */
		pg_atomic_init_u64(&cluster_grd_state->join_pcm_fence_epoch, 0);
		for (i = 0; i < CLUSTER_MAX_NODES; i++)
			pg_atomic_init_u64(&cluster_grd_state->join_pcm_fence_member_epoch[i], 0);
		pg_atomic_init_u32(&cluster_grd_state->recovery_direction, (uint32)GRD_REMASTER_DIR_NONE);
		pg_atomic_init_u64(&cluster_grd_state->join_remaster_started_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->join_remaster_done_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->join_shards_remastered_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->join_block_views_rebuilt_count, 0);
		pg_atomic_init_u64(&cluster_grd_state->join_block_recovering_failclosed_count, 0);
	}

	/* spec-2.15 v0.4 P1.1:  entry HTAB allocation gated on GUC.  GUC=0
	 * → htab stays NULL → lookup_or_create returns NOT_READY → entire
	 * shard partition LWLock path also unused → skip GetNamedLWLockTranche
	 * lookup entirely.  Bootstrap mode (initdb --boot) runs cluster_init_
	 * shmem without process_shmem_requests / cluster_grd_request_lwlocks,
	 * so the tranche is not registered there;  gating here keeps the
	 * skeleton-mode path FATAL-free. */
	cluster_grd_shard_locks = NULL;

	if (entry_alloc > 0) {
		HASHCTL info;
		Size init_max_size = grd_entries_init_max_size();

		/* spec-2.15 v0.3 P1.3 + I15:  obtain the named tranche array
		 * pointer (PG lwlock.c auto-initialized the 4096 LWLock;
		 * DO NOT call LWLockInitialize manually per I4 + I15).  Only
		 * reachable when cluster_grd_request_lwlocks() has run, i.e.
		 * full postmaster init under cluster.grd_max_entries > 0. */
		cluster_grd_shard_locks = GetNamedLWLockTranche("ClusterGrdShard");

		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(ClusterResId);
		info.entrysize = sizeof(ClusterGrdEntry);
		info.num_partitions = PGRAC_GRD_SHARD_COUNT;
		/* spec-2.15 v0.4 P1.1 I13:  HASHCTL.hash NOT set — single hash
		 * source 走 hash_search_with_hash_value(hashvalue) with
		 * cluster_grd_hash_resource() 32-bit projection.  Leaving
		 * info.hash NULL means dynahash uses tag_hash by default for
		 * HASH_BLOBS — but we always call hash_search_with_hash_value
		 * so the default never fires; defensive choice. */

		cluster_grd_entry_htab
			= ShmemInitHash("pgrac cluster grd entries", init_max_size, init_max_size, &info,
							HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
		cluster_grd_entries_alloc_bytes = entry_alloc;
	} else {
		cluster_grd_entry_htab = NULL;
		cluster_grd_entries_alloc_bytes = 0;
	}
}

static const ClusterShmemRegion cluster_grd_region = {
	.name = "pgrac cluster grd",
	.size_fn = cluster_grd_shmem_size,
	.init_fn = cluster_grd_shmem_init,
	.lwlock_count = 0, /* spec-2.14 Q9: lock-free (L106 inherit) */
	.owner_subsys = "cluster_grd",
	.reserved_flags = 0,
};

void
cluster_grd_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_grd_region);
}


/* ============================================================
 * Wire encoding / decoding (D2; Q4 user-correction).
 *
 *	Explicit field-by-field encode/decode — NOT memcpy(LOCKTAG).
 *	Wire ABI boundary;  LOCKTAG internal layout 与 ClusterResId 16B
 *	wire ABI 解耦.
 *
 *	v0.4 P1.1:  cluster_grd_resid_encode() must fetch_add resid_encode_count
 *	each call (observability;  was missing in v0.2/v0.3 spec body).
 * ============================================================ */

void
cluster_grd_resid_encode(const LOCKTAG *src, ClusterResId *dst)
{
	Assert(src != NULL);
	Assert(dst != NULL);

	if (src == NULL || dst == NULL)
		return;

	dst->field1 = src->locktag_field1;
	dst->field2 = src->locktag_field2;
	dst->field3 = src->locktag_field3;
	dst->field4 = src->locktag_field4;
	dst->type = src->locktag_type;
	dst->lockmethodid = src->locktag_lockmethodid;

	/*
	 * spec-2.26 D2 / HC40 — LOCKTAG_TRANSACTION origin wrapper.
	 *
	 *	PG SET_LOCKTAG_TRANSACTION leaves field2/3/4 zero (only field1
	 *	carries the local TransactionId).  Cluster wrapper overlays
	 *	field2 = cluster_node_id (origin_node_id) for cross-instance
	 *	GES routing while preserving field1 unchanged.  Other GES /
	 *	GRD layers (holder identity, envelope epoch validation) supply
	 *	the remaining stale-defence dimensions (HC43 / HC44).
	 *
	 *	HC47 caller contract: cluster_lock_should_globalize must have
	 *	rejected invalid cluster_node_id ranges before reaching this
	 *	encoder.  Assert in debug for defense in depth; production
	 *	leaves the (LOCKTAG-native) field2 = 0 unchanged on Assert miss
	 *	to avoid silently writing 0xFFFFFFFF to the wire (R11).
	 */
	if (src->locktag_type == LOCKTAG_TRANSACTION) {
		Assert(src->locktag_field2 == 0);
		Assert(src->locktag_field3 == 0);
		Assert(src->locktag_field4 == 0);
		/* Always clear the PG-native padding fields before overlaying the
		 * origin.  This keeps the encoder defensive even if a future caller
		 * bypasses the gate with a malformed TRANSACTION locktag. */
		dst->field2 = 0;
		dst->field3 = 0;
		dst->field4 = 0;
		if (cluster_node_id >= 0 && cluster_node_id < CLUSTER_MAX_NODES)
			dst->field2 = (uint32)cluster_node_id;
		/* else leave origin_node_id = 0 — caller gate is expected to have
		 * prevented us reaching here, but never write an out-of-range id. */
	}

	/* v0.4 P1.1:  increment observability counter on every encode. */
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->resid_encode_count, 1);
}

void
cluster_grd_resid_decode(const ClusterResId *src, LOCKTAG *dst)
{
	Assert(src != NULL);
	Assert(dst != NULL);

	if (src == NULL || dst == NULL)
		return;

	dst->locktag_field1 = src->field1;
	dst->locktag_field2 = src->field2;
	dst->locktag_field3 = src->field3;
	dst->locktag_field4 = src->field4;
	dst->locktag_type = src->type;
	dst->locktag_lockmethodid = src->lockmethodid;

	/*
	 * spec-2.26 D2 / HC40 — TRANSACTION reverse decode must NOT
	 * propagate origin_node_id back into PG-native LOCKTAG.field2.
	 * PG SET_LOCKTAG_TRANSACTION layout has field2 = 0 invariant;
	 * any downstream call comparing the decoded LOCKTAG against a
	 * freshly-built PG LOCKTAG would mis-match if we left field2 = N.
	 */
	if (src->type == LOCKTAG_TRANSACTION)
		dst->locktag_field2 = 0;
}


/* ============================================================
 * Cluster-aware lock type classifier (Q5 user-correction).
 *
 *	pgrac mapping function — NOT new LockTagType enum.  Returns true
 *	for the 4 cluster-coordinated lock classes; false for all others
 *	(PG-native lock manager handles them locally).
 * ============================================================ */

bool
cluster_grd_is_cluster_aware(const LOCKTAG *tag)
{
	Assert(tag != NULL);

	if (tag == NULL)
		return false;

	switch ((LockTagType)tag->locktag_type) {
	case LOCKTAG_RELATION:	  /* TM 表锁跨节点 (feature-024) */
	case LOCKTAG_TRANSACTION: /* TX 行锁等待图 (feature-023) */
	case LOCKTAG_OBJECT:	  /* catalog object 跨节点 */
	case LOCKTAG_ADVISORY:	  /* user lock 跨节点 (feature-078) */
		return true;
	default:
		return false; /* PAGE / TUPLE / RELATION_EXTEND /
									 * VIRTUALTRANSACTION / etc 本地 only */
	}
}


/* ============================================================
 * Performance hook API split (P1.2 v0.2; Stage 6 swap point).
 *
 *	cluster_grd_hash_resource is the ONLY function whose body Stage 6
 *	replaces (xxhash3 / RDMA-aware locality hash).  Hash input is
 *	14 bytes (P1.1 v0.2): field1-3 + type + lockmethodid;  skip ONLY
 *	field4 (tuple offset, co-locates same-page tuples in spec-2.16
 *	batched routing).
 *
 *	Counter invariant (v0.4 P1.2):
 *	  shard_lookup_count >= local_master_lookup_count +
 *	                        remote_master_lookup_count
 *	  shard_lookup() thin wrapper increments total only;
 *	  lookup_master() increments total + local-or-remote.
 * ============================================================ */

uint64
cluster_grd_hash_resource(const ClusterResId *resid)
{
	uint8 hash_input[14];

	Assert(resid != NULL);

	if (resid == NULL)
		return 0;

	/* Pack 14B input: field1-3 + type + lockmethodid.  Skip ONLY field4. */
	memcpy(&hash_input[0], &resid->field1, 4);
	memcpy(&hash_input[4], &resid->field2, 4);
	memcpy(&hash_input[8], &resid->field3, 4);
	hash_input[12] = resid->type;
	hash_input[13] = resid->lockmethodid; /* v0.2 P1.1: identity 必含 */

	return hash_bytes_extended(hash_input, sizeof(hash_input), 0);
}

uint32
cluster_grd_shard_for_hash(uint64 hash)
{
	return (uint32)(hash % PGRAC_GRD_SHARD_COUNT);
}

uint32
cluster_grd_shard_for_resource(const ClusterResId *resid)
{
	/* compose hash_resource + shard_for_hash;  no counter (pure). */
	return cluster_grd_shard_for_hash(cluster_grd_hash_resource(resid));
}

int32
cluster_grd_lookup_master(const ClusterResId *resid)
{
	uint32 shard_id;
	int32 master;

	Assert(cluster_grd_state != NULL);

	shard_id = cluster_grd_shard_for_resource(resid);
	pg_atomic_fetch_add_u64(&cluster_grd_state->shard_lookup_count, 1);

	if (pg_atomic_read_u32(&cluster_grd_state->master_map_initialized) == 0)
		return -1;

	master = (int32)pg_atomic_read_u32(&cluster_grd_state->master[shard_id]);
	if (master == cluster_node_id)
		pg_atomic_fetch_add_u64(&cluster_grd_state->local_master_lookup_count, 1);
	else
		pg_atomic_fetch_add_u64(&cluster_grd_state->remote_master_lookup_count, 1);

	return master;
}

uint32
cluster_grd_shard_lookup(const ClusterResId *resid)
{
	uint32 shard_id;

	Assert(cluster_grd_state != NULL);

	shard_id = cluster_grd_shard_for_resource(resid);
	/* Thin compat wrapper:  total counter only;  does NOT read master
	 * so local/remote counters NOT incremented (Counter invariant
	 * v0.4 P1.2:  shard_lookup_count >= local + remote). */
	pg_atomic_fetch_add_u64(&cluster_grd_state->shard_lookup_count, 1);
	return shard_id;
}


/* ============================================================
 * Master mapping (Q10 + Q11; declared-node-aware).
 *
 *	v0.4 P2.1 修正:  use existing cluster_conf_lookup_node() scan +
 *	cluster_conf_node_count() cross-check (NOT cluster_conf_get_declared_nodes
 *	which does NOT exist;  规则 23 linkdb SSOT).
 * ============================================================ */

int32
cluster_grd_shard_master(uint32 shard_id)
{
	Assert(cluster_grd_state != NULL);
	if (shard_id >= PGRAC_GRD_SHARD_COUNT)
		return -1;
	if (pg_atomic_read_u32(&cluster_grd_state->master_map_initialized) == 0)
		return -1;
	return (int32)pg_atomic_read_u32(&cluster_grd_state->master[shard_id]);
}

bool
cluster_grd_is_local_master(uint32 shard_id)
{
	int32 master = cluster_grd_shard_master(shard_id);

	return master >= 0 && master == cluster_node_id;
}

void
cluster_grd_master_map_init(void)
{
	int32 declared[CLUSTER_MAX_NODES];
	int declared_count = 0;
	int i;

	Assert(cluster_grd_state != NULL);

	/* Q10 + P2.1:  collect declared node_ids in scan order (= 升序)
	 * via existing cluster_conf_lookup_node().  Sparse node_id
	 * (e.g. pgrac.conf declares 0/2/5) yields declared = [0, 2, 5]. */
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (cluster_conf_lookup_node(i) != NULL)
			declared[declared_count++] = i;
	}
	if (declared_count <= 0) {
		ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("cluster_grd: no declared nodes in pgrac.conf"),
						errhint("Declare at least one [node.N] entry in pgrac.conf "
								"before initializing the GRD master map.")));
		return;
	}
	Assert(declared_count == cluster_conf_node_count());

	/* Distribute 4096 shards over declared nodes (round-robin in
	 * declared-list order, NOT modulo node_id directly). */
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
		int idx = i % declared_count;

		pg_atomic_write_u32(&cluster_grd_state->master[i], (uint32)declared[idx]);
	}
	pg_atomic_write_u32(&cluster_grd_state->master_map_initialized, 1);
	pg_atomic_fetch_add_u64(&cluster_grd_state->master_map_refresh_count, 1);
}

void
cluster_grd_master_map_refresh(void)
{
	/* Affinity/DRM placeholder (Stage 6) — ALIVE→ALIVE migration only.
	 * Failure-driven remaster is REAL since spec-4.6:  see
	 * cluster_grd_master_map_remaster() below.  Body stays a no-op
	 * except for the observability counter. */
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->master_map_refresh_count, 1);
}

/* spec-4.6 D2 — dead-bitmap bit test (CLUSTER_MAX_NODES bits as uint64
 * words;  word [node >> 6], bit (node & 63)). */
static inline bool
grd_dead_bitmap_test(const uint64 *dead_bitmap, int32 node_id)
{
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;
	return (dead_bitmap[node_id >> 6] >> (node_id & 63)) & 1;
}

/*
 * cluster_grd_master_map_remaster
 *
 *	spec-4.6 D2 — failure-driven remaster.  See cluster_grd.h for the
 *	full contract (deterministic survivor recompute from the accepted
 *	membership snapshot;  idempotent;  generation bumps only on real
 *	master movement).
 *
 *	Concurrency:  master[] writes are lock-free atomics;  callers are
 *	ordered by the spec-4.6 D1 P0-P7 barrier (P1 freeze precedes this,
 *	so backend lookups on affected shards are already fenced by the
 *	shard phase, and unaffected shards never change here).
 */
uint32
cluster_grd_master_map_remaster(const uint64 *dead_bitmap, uint64 reconfig_epoch)
{
	int32 survivors[CLUSTER_MAX_NODES];
	int survivor_count = 0;
	uint32 moved = 0;
	int i;

	Assert(cluster_grd_state != NULL);

	if (dead_bitmap == NULL)
		return 0;
	if (pg_atomic_read_u32(&cluster_grd_state->master_map_initialized) == 0)
		return 0; /* no map yet — nothing to remaster */

	/*
	 * Accepted membership snapshot = declared list (pgrac.conf, ascending
	 * scan order) minus the reconfig-accepted dead bits.  NEVER ad-hoc
	 * local peer_state (hard-gate #1:  every node must compute the same
	 * survivor list from the same snapshot).
	 */
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (!grd_dead_bitmap_test(dead_bitmap, i))
			survivors[survivor_count++] = i;
	}
	if (survivor_count <= 0) {
		/* Total declared death — fail-closed upstream (QVOTEC quorum
		 * loss freezes writes);  never reassign shards to nobody. */
		return 0;
	}

	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
		uint32 cur = pg_atomic_read_u32(&cluster_grd_state->master[i]);
		int32 target;

		if (!grd_dead_bitmap_test(dead_bitmap, (int32)cur))
			continue; /* master alive — failure-driven only, no affinity */

		target = survivors[i % survivor_count];
		pg_atomic_write_u32(&cluster_grd_state->master[i], (uint32)target);
		pg_atomic_fetch_add_u32(&cluster_grd_state->master_generation[i], 1);
		moved++;
	}

	if (moved > 0) {
		pg_atomic_write_u64(&cluster_grd_state->reconfig_remaster_epoch, reconfig_epoch);
		pg_atomic_fetch_add_u64(&cluster_grd_state->shards_remastered_count, moved);
		pg_atomic_fetch_add_u64(&cluster_grd_state->master_map_refresh_count, 1);
		ereport(DEBUG1, (errmsg_internal("cluster_grd_master_map_remaster: moved %u shards "
										 "to %d survivors (reconfig epoch " UINT64_FORMAT ")",
										 moved, survivor_count, reconfig_epoch)));
	}
	return moved;
}

/*
 * join_member_bitmap_test -- spec-5.16 D1 helper.
 *
 *	Test bit `node_id` in a CLUSTER_MAX_NODES-bit bitmap stored as uint8[16]
 *	(the reconfig dead/join bitmap shape, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES).
 */
static inline bool
join_member_bitmap_test(const uint8 *bm, int32 node_id)
{
	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;
	if ((node_id >> 3) >= CLUSTER_RECONFIG_DEAD_BITMAP_BYTES)
		return false;
	return (bm[node_id >> 3] & (uint8)(1u << (node_id & 7))) != 0;
}

/*
 * grd_build_membership_lists -- spec-5.16 D1/D2 helper.
 *
 *	Build the declared-node list (ascending scan order, same as master_map_init
 *	/ remaster) and its MEMBER sublist from an accepted membership snapshot
 *	(uint8[16]).  Both arrays are sized CLUSTER_MAX_NODES;  *declared_count /
 *	*member_count receive the populated lengths.
 */
static void
grd_build_membership_lists(const uint8 *active_member, int32 *declared, int *declared_count,
						   int32 *members, int *member_count)
{
	int i;

	*declared_count = 0;
	*member_count = 0;
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		declared[(*declared_count)++] = i;
		if (join_member_bitmap_test(active_member, i))
			members[(*member_count)++] = i;
	}
}

/*
 * grd_membership_desired_master -- spec-5.16 D1/D2 helper (SINGLE SOURCE).
 *
 *	The deterministic membership-desired master of shard `shard`:  its static
 *	home if the home is a member, else the deterministic survivor (the same
 *	survivor[i%n] formula the failure remaster uses → U2 equivalence).  Used by
 *	BOTH the WAIT_EPOCH freeze predicate AND the recompute apply, so freeze and
 *	move can never diverge (INV-R4 freeze-before-move).  Returns -1 if there is
 *	no member (defensive; caller must not freeze/move then).
 */
static int32
grd_membership_desired_master(const uint8 *active_member, const int32 *declared, int declared_count,
							  const int32 *members, int member_count, int shard)
{
	int32 home;

	if (declared_count <= 0 || member_count <= 0)
		return -1;
	home = declared[shard % declared_count];
	if (join_member_bitmap_test(active_member, home))
		return home;
	return members[shard % member_count];
}

/*
 * grd_build_active_member_bitmap -- spec-5.16 D2 helper.
 *
 *	Project the accepted MEMBER set into a uint8[16] bitmap from the 5.15
 *	membership SSOT (cluster_membership_is_member, INV-J8) — NEVER ad-hoc CSSD
 *	peer_state (INV-R3).  By the time a JOIN_COMMITTED episode reaches
 *	WAIT_EPOCH the joiner is already MEMBER on every node (5.15 D4 apply).
 */
static void
grd_build_active_member_bitmap(uint8 *out /* [CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] */)
{
	int i;

	memset(out, 0, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (cluster_membership_is_member(i))
			out[i >> 3] |= (uint8)(1u << (i & 7));
	}
}

/*
 * cluster_grd_master_map_recompute_for_membership -- spec-5.16 D1.
 *
 *	Membership-aware deterministic recompute (JOIN direction only).  See
 *	cluster_grd.h for the full contract.  Parallel to cluster_grd_master_map_
 *	remaster (Q3=A);  the shipped failure-driven function is NOT touched.  Join
 *	moves the joiner's home shards back from the survivor that held them in its
 *	absence;  a pure failure scenario (no home revival) is per-shard equivalent
 *	to the old function (U2), because the steady-state map is home-based.
 */
uint32
cluster_grd_master_map_recompute_for_membership(const uint8 *active_member, uint64 epoch)
{
	int32 declared[CLUSTER_MAX_NODES];
	int32 members[CLUSTER_MAX_NODES];
	int declared_count = 0;
	int member_count = 0;
	uint32 moved = 0;
	int i;

	Assert(cluster_grd_state != NULL);

	if (active_member == NULL)
		return 0;
	if (pg_atomic_read_u32(&cluster_grd_state->master_map_initialized) == 0)
		return 0; /* no map yet — nothing to recompute */

	grd_build_membership_lists(active_member, declared, &declared_count, members, &member_count);
	if (declared_count <= 0)
		return 0;
	if (member_count <= 0) {
		/*
		 * Total-membership-empty:  fail-closed precondition (never reassign a
		 * shard to nobody).  Unreachable in practice — self is always a member
		 * when this runs — so this is defensive only (INV-R1).
		 */
		return 0;
	}

	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
		int32 desired = grd_membership_desired_master(active_member, declared, declared_count,
													  members, member_count, i);
		uint32 cur;

		if (desired < 0)
			continue;
		cur = pg_atomic_read_u32(&cluster_grd_state->master[i]);
		if ((int32)cur != desired) {
			pg_atomic_write_u32(&cluster_grd_state->master[i], (uint32)desired);
			pg_atomic_fetch_add_u32(&cluster_grd_state->master_generation[i], 1);
			moved++;
		}
	}

	if (moved > 0) {
		pg_atomic_write_u64(&cluster_grd_state->reconfig_remaster_epoch, epoch);
		pg_atomic_fetch_add_u64(&cluster_grd_state->join_shards_remastered_count, moved);
		pg_atomic_fetch_add_u64(&cluster_grd_state->master_map_refresh_count, 1);
		ereport(DEBUG1,
				(errmsg_internal("cluster_grd_master_map_recompute_for_membership: moved %u "
								 "shards to membership (epoch " UINT64_FORMAT ")",
								 moved, epoch)));
	}
	return moved;
}

/*
 * cluster_grd_arm_join_pcm_fence -- spec-5.16 D3b (INV-R13).
 *
 *	Arm the joiner-home PCM block fence SYNCHRONOUSLY (NEVER from the LMON
 *	tick — the async tick is structurally later than the 5.15 write-gate open,
 *	so a fence armed there would leave a "MEMBER-writable but unfenced" window).
 *	Stamps each rejoining recipient's join_pcm_fence_member_epoch with THIS
 *	episode's epoch THEN raises join_pcm_fence_epoch (a write barrier between, so
 *	a reader that sees the raised epoch sees the recipient stamps).  Monotonic-
 *	max:  a later join re-arms higher;  re-arm of the same epoch is an idempotent
 *	no-op (INV-R12).
 *
 *	spec-5.16 Hardening v1.4 (Rule 8.A) — the recipient set is keyed PER NODE by
 *	the arming epoch, not an OR-accumulated bitmap.  Two arms race on the
 *	rejoining node: qvotec (note_self_admitted, {self}) and the LMON tick
 *	({evt.join_bitmap} for a multi-joiner episode).  Both stamp the SAME epoch on
 *	their respective nodes, so they union with no lost update and no under-fence.
 *	Crucially, a node armed in a COMPLETED prior episode keeps its lower stamp,
 *	which is < the current join_pcm_fence_epoch, so the recipient test (used by
 *	active_for_shard AND the re-declare barriers) excludes it from THIS episode
 *	automatically.  The previous v1.3 bitmap was never cleared, so a prior
 *	rejoiner — now a steady survivor that may hold X on the new joiner's home
 *	block — was wrongly skipped by the barrier -> premature fence lift -> cold-
 *	serve -> 8.A double-grant / false-visible (reviewer P1 #1).  Per-node epoch
 *	keying fixes both the union (no under-fence) and the staleness (no cross-
 *	episode under-wait) with no reset race.
 */
void
cluster_grd_arm_join_pcm_fence(const uint8 *rejoining_set)
{
	uint64 epoch;
	uint64 prev;
	int node;

	if (cluster_grd_state == NULL || rejoining_set == NULL)
		return;

	epoch = cluster_epoch_get_current();

	for (node = 0; node < CLUSTER_MAX_NODES; node++) {
		uint64 cur;

		if (node >= CLUSTER_RECONFIG_DEAD_BITMAP_BYTES * 8)
			break; /* rejoining_set bitmap is exhausted */
		if (((rejoining_set[node >> 3] >> (node & 7)) & 1) == 0)
			continue;
		/* monotonic-max per node — concurrent same-epoch arms union safely */
		cur = pg_atomic_read_u64(&cluster_grd_state->join_pcm_fence_member_epoch[node]);
		while (epoch > cur) {
			if (pg_atomic_compare_exchange_u64(
					&cluster_grd_state->join_pcm_fence_member_epoch[node], &cur, epoch))
				break;
		}
	}
	pg_write_barrier(); /* recipient stamps visible before the epoch is raised */

	prev = pg_atomic_read_u64(&cluster_grd_state->join_pcm_fence_epoch);
	while (epoch > prev) {
		if (pg_atomic_compare_exchange_u64(&cluster_grd_state->join_pcm_fence_epoch, &prev, epoch))
			break;
	}
	pg_atomic_write_u32(&cluster_grd_state->recovery_direction, (uint32)GRD_REMASTER_DIR_JOIN);
}

/*
 * cluster_grd_join_remaster_in_progress -- spec-5.16 D2.  recovery_direction
 * == JOIN (observability + recompute selection; does not change the FSM).
 */
bool
cluster_grd_join_remaster_in_progress(void)
{
	if (cluster_grd_state == NULL)
		return false;
	return pg_atomic_read_u32(&cluster_grd_state->recovery_direction)
		   == (uint32)GRD_REMASTER_DIR_JOIN;
}

/*
 * cluster_grd_join_remaster_active_for_shard -- spec-5.16 D3 (INV-R8).
 *
 *	True iff the block's STATIC PCM home (cluster_gcs_lookup_master_static) is a
 *	rejoining RECIPIENT of the CURRENT fence episode (member_epoch[home] ==
 *	join_pcm_fence_epoch).  Bound to online_join (the fence epoch is armed by
 *	note_self_admitted / LMON P0-accept), INDEPENDENT of any GRD master[]
 *	movement — so join_remaster_enabled=off still fences (r2 P1-①, P1-A
 *	closure).  false when the fence is not armed or the home is a steady member
 *	(incl. a prior rejoiner whose stamp is from a completed earlier episode).
 */
bool
cluster_grd_join_remaster_active_for_shard(BufferTag tag)
{
	uint64 fence_epoch;
	int home;

	if (cluster_grd_state == NULL)
		return false;
	fence_epoch = pg_atomic_read_u64(&cluster_grd_state->join_pcm_fence_epoch);
	if (fence_epoch == 0)
		return false; /* fence not armed */
	pg_read_barrier();

	home = cluster_gcs_lookup_master_static(tag);
	if (home < 0 || home >= CLUSTER_MAX_NODES)
		return false;
	return pg_atomic_read_u64(&cluster_grd_state->join_pcm_fence_member_epoch[home]) == fence_epoch;
}

/*
 * cluster_grd_block_view_rebuilt -- spec-5.16 D3 / Hardening v1.1.
 *
 *	True iff the joiner-home block view is rebuilt = EVERY declared member has
 *	announced its local re-declare barrier for the fence epoch (recovery_done_
 *	epoch >= join_pcm_fence_epoch).  This is the all_done barrier, NOT the
 *	joiner's own done-epoch:  the joiner announces its trivial local barrier
 *	BEFORE the survivors finish re-declaring their held joiner-home blocks to
 *	it, so gating on recovery_done_epoch[joiner] alone would lift the fence
 *	early → the joiner cold-serves a block a survivor still holds X on → 8.A
 *	double-grant (Hardening v1.1, user-approved 2026-06-28).  The tag is
 *	vestigial (the barrier is per-fence-epoch, not per-block) but kept so
 *	call sites read uniformly with active_for_shard.
 *
 *	The barrier set is the CURRENT members (cluster_membership_is_member), NOT
 *	all declared nodes:  a node dead from a prior failure never re-declares, so
 *	requiring it would wedge the fence forever;  and if the joiner itself
 *	re-dies mid-join it drops out of the member set, lifting this JOIN fence so
 *	the failure-remaster path's own gate (recovery_in_progress / materialized)
 *	takes over (§3.2).  The binding safety condition is "all survivors finished
 *	re-declaring" (their recovery_done_epoch >= fence_epoch);  the master-side
 *	gate on the joiner is the authoritative backstop (INV-R8/R14).
 */

/*
 * Test whether node_id is a rejoining RECIPIENT of the fence episode identified
 * by ref_epoch (member_epoch[node] == ref_epoch).  A stale stamp from a prior
 * episode (< ref_epoch) returns false, so a now-steady survivor is correctly
 * waited for by the re-declare barriers (Hardening v1.4, reviewer P1 #1).
 */
static inline bool
join_fence_is_recipient_for(int32 node_id, uint64 ref_epoch)
{
	if (cluster_grd_state == NULL || node_id < 0 || node_id >= CLUSTER_MAX_NODES)
		return false;
	if (ref_epoch == 0)
		return false;
	return pg_atomic_read_u64(&cluster_grd_state->join_pcm_fence_member_epoch[node_id])
		   == ref_epoch;
}

bool
cluster_grd_block_view_rebuilt(BufferTag tag)
{
	uint64 fence_epoch;
	int i;

	(void)tag;
	if (cluster_grd_state == NULL)
		return true;
	fence_epoch = pg_atomic_read_u64(&cluster_grd_state->join_pcm_fence_epoch);
	if (fence_epoch == 0)
		return true; /* nothing fenced */

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (!cluster_membership_is_member(i))
			continue; /* dead/joining node never re-declares; not a barrier member */
		/*
		 * The rejoining node(s) are the re-declare RECIPIENTS, not re-declarers:
		 * a fresh joiner holds no blocks to re-declare and never observes the
		 * JOIN_COMMITTED event as a reconfig episode (it is published coordinator-
		 * side only), so it never announces REDECLARE_DONE.  The binding safety
		 * condition is "every SURVIVOR finished re-declaring its held joiner-home
		 * blocks" — exclude only THIS episode's recipients so view_rebuilt
		 * converges on the survivors' done (Hardening v1.1 + D8 fix).  Keyed on
		 * fence_epoch so a prior rejoiner (now a steady survivor) is NOT skipped
		 * (Hardening v1.4, reviewer P1 #1: cross-episode under-wait -> 8.A).
		 */
		if (join_fence_is_recipient_for(i, fence_epoch))
			continue;
		if (pg_atomic_read_u64(&cluster_grd_state->recovery_done_epoch[i]) < fence_epoch)
			return false;
	}
	return true;
}

/*
 * cluster_grd_lookup_master_gen
 *
 *	spec-4.6 D2 — master lookup + wire routing token.  Q3-C:  the token
 *	is the EXISTING (accepted_epoch<<32)|lms_restart_gen verbatim;  the
 *	per-shard remaster generation never rides the wire.
 */
int32
cluster_grd_lookup_master_gen(const ClusterResId *resid, uint64 *out_routing_generation)
{
	if (out_routing_generation)
		*out_routing_generation = cluster_lms_get_shard_master_generation();
	return cluster_grd_lookup_master(resid);
}

uint32
cluster_grd_shard_master_generation(uint32 shard_id)
{
	Assert(cluster_grd_state != NULL);
	if (shard_id >= PGRAC_GRD_SHARD_COUNT)
		return 0;
	return pg_atomic_read_u32(&cluster_grd_state->master_generation[shard_id]);
}

/*
 * Shard recovery phase accessors — spec-4.6 D1/D4.
 *
 *	LMON is the only writer (set_phase);  backends read lock-free on
 *	the request path.  Unknown shard ids read as NORMAL (defensive:
 *	the caller-side resid→shard mapping already bounds the id).
 */
ClusterGrdShardPhase
cluster_grd_shard_phase(uint32 shard_id)
{
	if (cluster_grd_state == NULL || shard_id >= PGRAC_GRD_SHARD_COUNT)
		return GRD_SHARD_NORMAL;
	return (ClusterGrdShardPhase)pg_atomic_read_u32(&cluster_grd_state->shard_phase[shard_id]);
}

void
cluster_grd_shard_set_phase(uint32 shard_id, ClusterGrdShardPhase phase)
{
	Assert(cluster_grd_state != NULL);
	if (shard_id >= PGRAC_GRD_SHARD_COUNT)
		return;
	pg_atomic_write_u32(&cluster_grd_state->shard_phase[shard_id], (uint32)phase);
}


/* ============================================================
 * spec-4.6 D1 — GRD recovery sequence (P0-P7).
 *
 *	LMON tick driver, sequenced AFTER cluster_reconfig_lmon_tick in the
 *	LMON main loop:
 *	  P0 accept DEAD     reconfig event published (quorum/evict/fence)
 *	  P1 freeze affected shards whose CURRENT master is dead → FROZEN
 *	  P2 cleanup dead    dead_sweep (earlier in the tick, I47)
 *	  P3 scoped sweep    epoch-stale leftovers on affected shards only
 *	  P4 remaster        cluster_grd_master_map_remaster (D2)
 *	  P5 rebuild         REBUILDING + redeclare broadcast + ack barrier
 *	  P6 global sweep    post-barrier ONLY (P0#3)
 *	  P7 unfreeze        NORMAL
 *	Phase regressions are impossible by construction (single LMON
 *	writer + the recovery_state cursor);  the P5→P6 edge is the hard
 *	barrier:  running the global sweep before every live backend acked
 *	would re-create P0#2 (deleting live-but-not-yet-rebound holders).
 * ============================================================ */

#define GRD_SHARD_BITMAP_WORDS (PGRAC_GRD_SHARD_COUNT / 64)

static inline bool
grd_shard_bitmap_test(const uint64 *bm, uint32 shard)
{
	return (bm[shard >> 6] >> (shard & 63)) & 1;
}

static inline void
grd_shard_bitmap_set(uint64 *bm, uint32 shard)
{
	bm[shard >> 6] |= ((uint64)1 << (shard & 63));
}

uint64
cluster_grd_redeclare_generation(void)
{
	/* NULL-safe:  InitProcess seeds the PGPROC ack from this before the
	 * backend ever touches GRD state;  shmem-less contexts read 0. */
	if (cluster_grd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_grd_state->recovery_redeclare_generation);
}

uint64
cluster_grd_redeclare_episode_epoch(void)
{
	if (cluster_grd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_grd_state->recovery_episode_epoch);
}

/*
 * cluster_grd_recovery_in_progress -- spec-4.7 D7 (P0 code-review fix).
 *
 *	True while this node's reconfig recovery FSM is NOT idle, i.e. an episode
 *	is freezing / rebinding / awaiting the cluster REDECLARE_DONE barrier.
 *	The block-resource phase gate uses this to keep every dead-static-master
 *	block fail-closed (RECOVERING) for the whole episode:  until this node
 *	reaches IDLE it has not seen every survivor's REDECLARE_DONE (now gated on
 *	their block re-declare scans completing), so a held block may not yet have
 *	been re-declared to its recovery-aware master — serving it would risk an
 *	8.A double-grant.  Reaching IDLE implies all survivor scans completed.
 */
bool
cluster_grd_recovery_in_progress(void)
{
	if (cluster_grd_state == NULL)
		return false;
	return pg_atomic_read_u32(&cluster_grd_state->recovery_state) != (uint32)GRD_RECOVERY_IDLE;
}

/* spec-4.6 D4/D5 — recovery counter bump helpers for out-of-module
 * call sites (S4 stale mapping in cluster_lock_acquire.c;  GCS block
 * fail-closed guard in cluster_gcs_block.c). */
void
cluster_grd_inc_stale_request_drop(void)
{
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->stale_request_drop_count, 1);
}

void
cluster_grd_inc_block_path_failclosed(void)
{
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->block_path_failclosed_count, 1);
}

/* spec-5.16 D5 — bump the join-direction 53R9L fail-closed counter (both the
 * requester-side phase gate and the master-side envelope gate call this). */
void
cluster_grd_inc_join_block_failclosed(void)
{
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->join_block_recovering_failclosed_count, 1);
}

/* spec-4.6 D5 — bulk counter snapshot for the dump path. */
void
cluster_grd_recovery_counters_snapshot(ClusterGrdRecoveryCounters *out)
{
	memset(out, 0, sizeof(*out));
	if (cluster_grd_state == NULL)
		return;
	out->remaster_started = pg_atomic_read_u64(&cluster_grd_state->remaster_started_count);
	out->remaster_done = pg_atomic_read_u64(&cluster_grd_state->remaster_done_count);
	out->remaster_failed = pg_atomic_read_u64(&cluster_grd_state->remaster_failed_count);
	out->shards_remastered = pg_atomic_read_u64(&cluster_grd_state->shards_remastered_count);
	out->holders_redeclared = pg_atomic_read_u64(&cluster_grd_state->holders_redeclared_count);
	out->holders_rebound = pg_atomic_read_u64(&cluster_grd_state->holders_rebound_count);
	out->waiters_requeued = pg_atomic_read_u64(&cluster_grd_state->waiters_requeued_count);
	out->converts_requeued = pg_atomic_read_u64(&cluster_grd_state->converts_requeued_count);
	out->stale_request_drop = pg_atomic_read_u64(&cluster_grd_state->stale_request_drop_count);
	out->rebuild_timeout = pg_atomic_read_u64(&cluster_grd_state->rebuild_timeout_count);
	out->block_path_failclosed
		= pg_atomic_read_u64(&cluster_grd_state->block_path_failclosed_count);
	out->unaffected_holder_survived
		= pg_atomic_read_u64(&cluster_grd_state->unaffected_holder_survived_count);
	out->stale_holder_swept = pg_atomic_read_u64(&cluster_grd_state->stale_holder_swept_count);
	/* spec-5.16 D5 — join-direction remaster counters. */
	out->join_remaster_started
		= pg_atomic_read_u64(&cluster_grd_state->join_remaster_started_count);
	out->join_remaster_done = pg_atomic_read_u64(&cluster_grd_state->join_remaster_done_count);
	out->join_shards_remastered
		= pg_atomic_read_u64(&cluster_grd_state->join_shards_remastered_count);
	out->join_block_views_rebuilt
		= pg_atomic_read_u64(&cluster_grd_state->join_block_views_rebuilt_count);
	out->join_block_recovering_failclosed
		= pg_atomic_read_u64(&cluster_grd_state->join_block_recovering_failclosed_count);
}

/*
 * cluster_grd_cleanup_stale_epoch_scoped — spec-4.6 P0#2 (P3).
 *
 *	Pre-remaster hygiene:  remove epoch-stale holders/waiters ONLY on
 *	the affected (about-to-be-remastered) shards, so the new master
 *	does not inherit dirt from an earlier mastership era.  Unaffected
 *	shards are NOT touched here — their live holders are rebound in
 *	place by their owner backends (P5) and the global sweep waits for
 *	the ack barrier (P6).
 */
void
cluster_grd_cleanup_stale_epoch_scoped(uint64 current_epoch, const uint64 *affected_shards)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	int swept = 0;

	if (cluster_grd_entry_htab == NULL || affected_shards == NULL)
		return;

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		int i;

		if (!grd_shard_bitmap_test(affected_shards, cluster_grd_shard_for_resource(&entry->resid)))
			continue;

		SpinLockAcquire(&entry->lock);
		for (i = 0; i < entry->ngranted;) {
			if (entry->holders[i].cluster_epoch < current_epoch) {
				if (i < entry->ngranted - 1)
					entry->holders[i] = entry->holders[entry->ngranted - 1];
				memset(&entry->holders[entry->ngranted - 1], 0, sizeof(entry->holders[0]));
				entry->ngranted--;
				swept++;
				continue;
			}
			i++;
		}
		for (i = 0; i < entry->nwaiters;) {
			if (entry->waiters[i].cluster_epoch < current_epoch) {
				if (i < entry->nwaiters - 1)
					entry->waiters[i] = entry->waiters[entry->nwaiters - 1];
				memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(entry->waiters[0]));
				entry->nwaiters--;
				swept++;
				continue;
			}
			i++;
		}
		/* spec-5.1b §3 clause 14: convert queue entries are epoch-swept with
		 * holders/waiters (the convert carries its enqueue cluster_epoch).
		 * Latent in 5.1b (converts[] is production-empty until the spec-5.2
		 * producer lands), kept complete for that producer. */
		for (i = 0; i < entry->nconverts;) {
			if (entry->converts[i].cluster_epoch < current_epoch) {
				if (i < entry->nconverts - 1)
					entry->converts[i] = entry->converts[entry->nconverts - 1];
				memset(&entry->converts[entry->nconverts - 1], 0, sizeof(entry->converts[0]));
				entry->nconverts--;
				swept++;
				continue;
			}
			i++;
		}
		SpinLockRelease(&entry->lock);
	}

	if (swept > 0)
		ereport(DEBUG1, (errmsg_internal("cluster_grd_cleanup_stale_epoch_scoped(" UINT64_FORMAT
										 "): swept %d affected-shard slots",
										 current_epoch, swept)));
}

/*
 * cluster_grd_cleanup_stale_epoch_postbarrier — spec-4.6 P0#3 (P6).
 *
 *	Global stale sweep, legal ONLY after the redeclare ack barrier:
 *	every live backend has rebound all its registered grants to the
 *	current epoch, so any remaining old-epoch holder/waiter is provably
 *	unclaimed (its backend exited mid-window, or its release was
 *	epoch-rejected during the window) and MUST be removed — a leaked
 *	holder blocks the resource forever.  Converts carry no epoch and
 *	are not swept here (dead-node converts fall to dead_sweep;  a
 *	live-node stale convert self-resolves through its requester's
 *	timeout + retry).
 */
uint32
cluster_grd_cleanup_stale_epoch_postbarrier(uint64 current_epoch)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	uint32 swept = 0;
	uint32 waiters_dropped = 0;

	if (cluster_grd_entry_htab == NULL)
		return 0;

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		int i;

		SpinLockAcquire(&entry->lock);
		for (i = 0; i < entry->ngranted;) {
			if (entry->holders[i].cluster_epoch < current_epoch) {
				if (i < entry->ngranted - 1)
					entry->holders[i] = entry->holders[entry->ngranted - 1];
				memset(&entry->holders[entry->ngranted - 1], 0, sizeof(entry->holders[0]));
				entry->ngranted--;
				swept++;
				continue;
			}
			i++;
		}
		for (i = 0; i < entry->nwaiters;) {
			if (entry->waiters[i].cluster_epoch < current_epoch) {
				if (i < entry->nwaiters - 1)
					entry->waiters[i] = entry->waiters[entry->nwaiters - 1];
				memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(entry->waiters[0]));
				entry->nwaiters--;
				waiters_dropped++;
				continue;
			}
			i++;
		}
		SpinLockRelease(&entry->lock);
	}

	if (swept > 0)
		pg_atomic_fetch_add_u64(&cluster_grd_state->stale_holder_swept_count, swept);
	if (waiters_dropped > 0)
		pg_atomic_fetch_add_u64(&cluster_grd_state->waiters_requeued_count, waiters_dropped);
	if (swept > 0 || waiters_dropped > 0)
		ereport(DEBUG1,
				(errmsg_internal("cluster_grd_cleanup_stale_epoch_postbarrier(" UINT64_FORMAT
								 "): swept %u leaked holders + %u stale waiters",
								 current_epoch, swept, waiters_dropped)));
	return swept;
}

/*
 * Broadcast PROCSIG_CLUSTER_GRD_REDECLARE to every live backend.
 * Pattern mirrors cluster_reconfig_broadcast_local_procsig.
 */
static int
grd_recovery_broadcast_redeclare(void)
{
	int beid;
	int signaled = 0;
	pid_t self_pid = MyProcPid;

	for (beid = 1; beid <= MaxBackends; beid++) {
		PGPROC *proc = BackendIdGetProc((BackendId)beid);
		pid_t pid;

		if (proc == NULL)
			continue;
		pid = proc->pid;
		if (pid == 0 || pid == self_pid)
			continue;
		(void)SendProcSignal(pid, PROCSIG_CLUSTER_GRD_REDECLARE, (BackendId)beid);
		signaled++;
	}
	return signaled;
}

/*
 * Barrier check:  every live backend has acked the redeclare
 * generation.  Backends born after the broadcast were seeded with the
 * current generation at InitProcess (they hold no stale-epoch grants);
 * backends that exited simply drop out of the scan (their leaked
 * master-side state is exactly what P6 sweeps).
 */
static bool
grd_recovery_barrier_complete(uint64 gen, uint64 episode_epoch)
{
	int beid;
	pid_t self_pid = MyProcPid;

	for (beid = 1; beid <= MaxBackends; beid++) {
		PGPROC *proc = BackendIdGetProc((BackendId)beid);

		if (proc == NULL)
			continue;
		if (proc->pid == 0 || proc->pid == self_pid)
			continue;

		/*
		 * Scope filter:  a proc holding ZERO cluster_registered grants
		 * has nothing to rebind (any later acquire mints the current
		 * epoch), and sinval-registered processes that never run the
		 * generic ProcessInterrupts path (autovacuum launcher, logical
		 * replication launcher) would otherwise wedge the barrier
		 * forever.
		 */
		if (pg_atomic_read_u32(&proc->cluster_grd_registered_count) == 0)
			continue;
		if (pg_atomic_read_u64(&proc->cluster_grd_redeclare_acked) < gen)
			return false;
		/* P0-1:  the ack must be coherent with the LOCKED episode epoch.
		 * A proc that acked an earlier generation under the old epoch and
		 * then short-circuited (acked >= gen via a stale generation race)
		 * must not satisfy the barrier for a newer epoch. */
		if (pg_atomic_read_u64(&proc->cluster_grd_redeclare_acked_epoch) != episode_epoch)
			return false;
	}
	return true;
}

/*
 * spec-4.6 P0#3 cluster gate — announce "my local rebind barrier is
 * complete for `epoch`" to every declared peer (fire-and-forget;  the
 * WAIT_CLUSTER state re-announces each tick).  Standard
 * GesRequestPayload, zero wire-ABI change.
 */
static void
grd_recovery_broadcast_done(uint64 epoch)
{
	GesRequestPayload req;
	uint64 master_gen = cluster_lms_get_shard_master_generation();
	int i;

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_REDECLARE_DONE;
	req.holder_node_id = (uint32)cluster_node_id;
	req.holder_procno = 0;
	req.holder_cluster_epoch_lo = (uint32)(epoch & 0xffffffffu);
	req.holder_cluster_epoch_hi = (uint32)(epoch >> 32);
	req.shard_master_generation_lo = (uint32)(master_gen & 0xffffffffu);
	req.shard_master_generation_hi = (uint32)(master_gen >> 32);

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (i == cluster_node_id)
			continue;
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		/* Dead peers neither gate P6 nor drain their inbound — do not
		 * stuff per-tick announcements into the outbound ring for them. */
		if (cluster_cssd_get_peer_state(i) == CLUSTER_CSSD_PEER_DEAD)
			continue;
		(void)cluster_grd_outbound_enqueue_backend_request((uint32)i, &req, sizeof(req));
	}
}

/* REDECLARE_DONE receiver (cluster_ges.c inbound handler). */
void
cluster_grd_recovery_mark_peer_done(int32 node, uint64 epoch)
{
	uint64 prev;

	if (cluster_grd_state == NULL || node < 0 || node >= CLUSTER_MAX_NODES)
		return;
	/* Monotonic max:  late/duplicate announcements never regress. */
	prev = pg_atomic_read_u64(&cluster_grd_state->recovery_done_epoch[node]);
	while (epoch > prev) {
		if (pg_atomic_compare_exchange_u64(&cluster_grd_state->recovery_done_epoch[node], &prev,
										   epoch))
			break;
	}
}

/*
 * spec-4.6 P0-1 / P1-2 — abort the in-flight episode back to IDLE.
 *
 *	Called when a mid-episode epoch bump (P0-1) or a fresh reconfig event
 *	(P1-2:  a SECOND node died during recovery) invalidates the locked
 *	episode.  Affected shards STAY frozen (fail-closed; never opened
 *	half-rebuilt) and the master map keeps its already-remastered state;
 *	resetting recovery_last_event_id forces the IDLE branch to re-consume
 *	the (now-current) event next tick, which re-snapshots the epoch,
 *	bumps a fresh redeclare generation (forcing every backend to re-walk
 *	and rebind under the new epoch), and re-runs P1-P7.  Convergent:  the
 *	epoch eventually stops moving and an episode completes.
 */
static void
grd_recovery_abort_to_idle(void)
{
	pg_atomic_fetch_add_u64(&cluster_grd_state->remaster_failed_count, 1);
	pg_atomic_write_u64(&cluster_grd_state->recovery_last_event_id, 0);
	pg_atomic_write_u32(&cluster_grd_state->recovery_state, (uint32)GRD_RECOVERY_IDLE);
	ereport(DEBUG1,
			(errmsg_internal("cluster_grd_recovery: episode aborted (epoch moved / new event "
							 "mid-recovery); shards stay frozen, re-running under the new epoch")));
}


/*
 * spec-4.7 D2 — survivor block re-declare scan (Q6-A' worker-centric).
 *
 *	BufferDesc.pcm_state is shared per-node authoritative state (not the
 *	backend-private GES LocalLockHash), so the block re-declare is NOT a
 *	backend-cooperative protocol like the GES rebind — the LMON reconfig tick
 *	itself scans the shared buffer pool in bounded chunks and sends one
 *	GCS_BLOCK_REDECLARE per locally-held S/X buffer to the block's current
 *	(remastered) master.  The cursor is LMON-process-local (the LMON aux
 *	process is the sole tick driver — no shmem needed);  it re-arms to 0 each
 *	time a reconfig episode locks a new episode epoch and advances by
 *	GRD_BLOCK_REDECLARE_CHUNK per tick until it reaches NBuffers, so a large
 *	pool scan never blocks the heartbeat (§6 risk mitigation).  Epoch
 *	coherence (L235) is enforced by the WAIT_BARRIER/WAIT_CLUSTER epoch guards
 *	that abort the episode on a mid-episode bump.
 */
#define GRD_BLOCK_REDECLARE_CHUNK 256
static int grd_block_redeclare_cursor = 0;
static uint64 grd_block_redeclare_epoch = 0;
static bool grd_block_redeclare_done = false;

static void
grd_block_redeclare_cb(BufferTag tag, uint8 held_mode, XLogRecPtr page_lsn, SCN page_scn, void *arg)
{
	uint64 episode_epoch = *(const uint64 *)arg;
	int master = cluster_gcs_lookup_master(tag);

	cluster_gcs_block_send_redeclare(tag, held_mode, page_lsn, page_scn, episode_epoch, master);
}

/* Non-static (exposed via cluster_grd.h) so the unit test can drive the scan
 * cursor + assert grd_block_redeclare_scan_complete tracks NBuffers without
 * spinning up the full reconfig FSM. */
void
grd_block_redeclare_step(uint64 episode_epoch)
{
	int next;

	/* Re-arm to the start of the pool whenever a fresh episode locks a new
	 * epoch (the previous episode's partial scan is abandoned — the new epoch
	 * re-stamps every re-declare). */
	if (grd_block_redeclare_epoch != episode_epoch) {
		grd_block_redeclare_epoch = episode_epoch;
		grd_block_redeclare_cursor = 0;
		grd_block_redeclare_done = false;
	}

	if (grd_block_redeclare_done)
		return;

	/* Bounded chunk;  scan_chunk caps the cursor at NBuffers and returns the
	 * cursor unchanged once the whole pool has been scanned this episode. */
	next
		= cluster_bufmgr_redeclare_scan_chunk(grd_block_redeclare_cursor, GRD_BLOCK_REDECLARE_CHUNK,
											  grd_block_redeclare_cb, &episode_epoch);
	if (next == grd_block_redeclare_cursor)
		grd_block_redeclare_done = true; /* reached NBuffers — whole pool scanned */
	grd_block_redeclare_cursor = next;
}

/*
 * grd_block_redeclare_scan_complete -- spec-4.7 D2/D7 (P0 code-review fix).
 *
 *	True iff THIS survivor's block re-declare scan for `episode_epoch` has
 *	swept the whole buffer pool (every locally-held S/X block has been
 *	re-declared to its recovery-aware master).  This MUST be a precondition of
 *	announcing REDECLARE_DONE:  otherwise a held block whose buffer sits after
 *	the scan cursor is never re-declared, the episode reaches IDLE, the new
 *	master treats it as cold, and serves it from shared storage while the
 *	original holder still holds X → 8.A double-grant.
 */
bool
grd_block_redeclare_scan_complete(uint64 episode_epoch)
{
	return grd_block_redeclare_epoch == episode_epoch && grd_block_redeclare_done;
}

void
cluster_grd_recovery_lmon_tick(void)
{
	uint32 state;

	if (!cluster_enabled || cluster_grd_state == NULL)
		return;
	if (pg_atomic_read_u32(&cluster_grd_state->master_map_initialized) == 0)
		return;

	state = pg_atomic_read_u32(&cluster_grd_state->recovery_state);

	if (state == (uint32)GRD_RECOVERY_IDLE) {
		ReconfigEvent evt;
		uint64 ev_id;
		int b;

		cluster_reconfig_get_last_event(&evt);
		ev_id = evt.event_id;
		if (ev_id == 0 || ev_id == pg_atomic_read_u64(&cluster_grd_state->recovery_last_event_id)) {
			/*
			 * P1-3 (Fable review) — idle:  track the CURRENT epoch as the
			 * pre-reconfig baseline.  We must NOT trust evt.old_epoch as
			 * the WAIT_EPOCH gate baseline:  for a non-coordinator
			 * survivor it is a fresh read taken when the survivor-role
			 * event was published, and an IC piggyback can deliver the
			 * coordinator's bumped epoch BEFORE this node's own deadband
			 * fires, making old_epoch already == the post-bump epoch and
			 * wedging WAIT_EPOCH forever (cur <= old).  The last stable
			 * idle epoch is the reliable "before reconfig" value.
			 */
			pg_atomic_write_u64(&cluster_grd_state->recovery_event_old_epoch,
								cluster_epoch_get_current());
			return;
		}

		/* P0 accept:  a fresh reconfig event (quorum-accepted dead set).
		 * recovery_event_old_epoch already holds last idle tick's stable
		 * epoch (the genuine pre-reconfig baseline) — do NOT overwrite it
		 * with evt.old_epoch here. */
		pg_atomic_write_u64(&cluster_grd_state->recovery_last_event_id, ev_id);
		for (b = 0; b < (CLUSTER_MAX_NODES + 63) / 64; b++) {
			uint64 word = 0;
			int j;

			for (j = 0; j < 8; j++) {
				int byte_idx = b * 8 + j;

				if (byte_idx < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES)
					word |= ((uint64)evt.dead_bitmap[byte_idx]) << (8 * j);
			}
			pg_atomic_write_u64(&cluster_grd_state->recovery_dead_bitmap[b], word);
		}
		/*
		 * spec-5.16 D2 — record the remaster direction and, for JOIN, arm the
		 * joiner-home PCM block fence on THIS node.  The joiner already armed it
		 * synchronously before opening its write gate (note_self_admitted,
		 * INV-R13);  this covers survivors and is an idempotent monotonic-max
		 * re-arm on the joiner.  JOIN events carry the rejoiner set in
		 * join_bitmap (dead_bitmap is empty), so the failure-path freeze/remaster
		 * below is a structural no-op — only the block re-declare barrier runs,
		 * which rebuilds the joiner's PCM block view.
		 */
		if (evt.reconfig_kind == (uint8)RECONFIG_KIND_JOIN_COMMITTED) {
			pg_atomic_write_u32(&cluster_grd_state->recovery_direction,
								(uint32)GRD_REMASTER_DIR_JOIN);
			cluster_grd_arm_join_pcm_fence(evt.join_bitmap);
			pg_atomic_fetch_add_u64(&cluster_grd_state->join_remaster_started_count, 1);
		} else {
			pg_atomic_write_u32(&cluster_grd_state->recovery_direction,
								(uint32)GRD_REMASTER_DIR_FAIL);
			pg_atomic_fetch_add_u64(&cluster_grd_state->remaster_started_count, 1);
		}
		pg_atomic_write_u32(&cluster_grd_state->recovery_state, (uint32)GRD_RECOVERY_WAIT_EPOCH);
		state = (uint32)GRD_RECOVERY_WAIT_EPOCH;
		ereport(DEBUG1,
				(errmsg_internal(
					"cluster_grd_recovery: P0 accept event " UINT64_FORMAT
					" (pre-reconfig baseline epoch " UINT64_FORMAT ")",
					ev_id, pg_atomic_read_u64(&cluster_grd_state->recovery_event_old_epoch))));
		/* fall through — the coordinator already bumped the epoch this
		 * tick, so P1-P5 usually run immediately below. */
	}

	if (state == (uint32)GRD_RECOVERY_WAIT_EPOCH) {
		uint64 cur_epoch = cluster_epoch_get_current();
		uint64 old_epoch = pg_atomic_read_u64(&cluster_grd_state->recovery_event_old_epoch);
		uint64 dead[(CLUSTER_MAX_NODES + 63) / 64];
		uint64 affected[GRD_SHARD_BITMAP_WORDS];
		uint32 nfrozen = 0;
		uint32 moved;
		uint64 gen;
		TimestampTz deadline;
		int i;
		int signaled;

		/*
		 * Gate on the ACCEPTED epoch having advanced past the episode's
		 * old epoch:  the coordinator bumped it earlier this tick;  a
		 * non-coordinator survivor observes it via IC envelope piggyback
		 * a tick or two later.  Running the rebind before the local
		 * epoch advances would mint holders the new master rejects.
		 *
		 * spec-5.16 (P0, Rule 8.A) — direction-aware.  A FAIL survivor captures
		 * old_epoch BEFORE the coordinator bumps, so it must wait for a STRICT
		 * advance (cur > old) to prove it adopted the new master view.  A JOIN
		 * observer survivor instead adopts the coordinator's JOIN epoch bump (via the
		 * joiner_self_tick max-peer-epoch adoption, same LMON tick) BEFORE it readmits
		 * the rejoiner and publishes its own observer JOIN_COMMITTED event, so by the
		 * time its FSM accepts that event old_epoch == cur_epoch == the join epoch.
		 * Requiring a strict advance there wedges the survivor in WAIT_EPOCH forever:
		 * it never runs the re-declare barrier, never broadcasts REDECLARE_DONE, and
		 * the coordinator's all-members JOIN barrier (Hardening v1.1) hangs →
		 * join_remaster_done never advances.  For JOIN the survivor already holds the
		 * epoch it rebinds under (that IS the post-bump master view), so equality is
		 * sufficient; cur_epoch is monotonic so cur < old never occurs for JOIN.
		 */
		if (pg_atomic_read_u32(&cluster_grd_state->recovery_direction)
					== (uint32)GRD_REMASTER_DIR_JOIN
				? cur_epoch < old_epoch
				: cur_epoch <= old_epoch)
			return;

		for (i = 0; i < (CLUSTER_MAX_NODES + 63) / 64; i++)
			dead[i] = pg_atomic_read_u64(&cluster_grd_state->recovery_dead_bitmap[i]);

		/*
		 * P1 freeze affected + P3 scoped sweep + P4 remaster.  Direction-
		 * specific (spec-5.16 D2):
		 *   FAIL (4.6):  affected = shards whose CURRENT master is dead;
		 *                remaster dead → survivor.
		 *   JOIN (5.16): affected = shards the membership recompute will move
		 *                (joiner home shards a survivor held in its absence);
		 *                recompute survivor → joiner.  ONLY when join_remaster_
		 *                enabled — the PCM block fence (D3b) carries correctness
		 *                regardless, so off leaves GRD mastership on the survivor
		 *                (load imbalance only, INV-R12 / r2 P1-①).
		 * Both directions then run the cluster-wide block re-declare barrier
		 * (P5-P7 below);  for JOIN that rebuilds the joiner's PCM block view even
		 * when join_remaster_enabled is off.  The two scopes are independent:
		 * grd_moved_shards = `affected` here (GRD, GUC-gated); pcm_fenced_home_set
		 * lives in join_pcm_fence_member_epoch (PCM, online_join-gated, armed at P0).
		 */
		memset(affected, 0, sizeof(affected));
		if (pg_atomic_read_u32(&cluster_grd_state->recovery_direction)
			== (uint32)GRD_REMASTER_DIR_JOIN) {
			if (cluster_join_remaster_enabled) {
				uint8 active_member[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
				int32 declared[CLUSTER_MAX_NODES];
				int32 members[CLUSTER_MAX_NODES];
				int declared_count = 0;
				int member_count = 0;

				grd_build_active_member_bitmap(active_member);
				grd_build_membership_lists(active_member, declared, &declared_count, members,
										   &member_count);
				for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
					int32 desired = grd_membership_desired_master(
						active_member, declared, declared_count, members, member_count, i);

					if (desired >= 0
						&& (int32)pg_atomic_read_u32(&cluster_grd_state->master[i]) != desired) {
						cluster_grd_shard_set_phase((uint32)i, GRD_SHARD_FROZEN);
						grd_shard_bitmap_set(affected, (uint32)i);
						nfrozen++;
					}
				}
				cluster_grd_cleanup_stale_epoch_scoped(cur_epoch, affected);
				moved = cluster_grd_master_map_recompute_for_membership(active_member, cur_epoch);
			} else {
				/* JOIN with GRD rebalance off: no GRD freeze/move; the PCM
				 * fence (armed at P0) + the barrier below carry the rebuild. */
				cluster_grd_cleanup_stale_epoch_scoped(cur_epoch, affected); /* empty: no-op */
				moved = 0;
			}
		} else {
			for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
				uint32 m = pg_atomic_read_u32(&cluster_grd_state->master[i]);

				if (grd_dead_bitmap_test(dead, (int32)m)) {
					cluster_grd_shard_set_phase((uint32)i, GRD_SHARD_FROZEN);
					grd_shard_bitmap_set(affected, (uint32)i);
					nfrozen++;
				}
			}
			cluster_grd_cleanup_stale_epoch_scoped(cur_epoch, affected);
			moved = cluster_grd_master_map_remaster(dead, cur_epoch);
		}
		ereport(DEBUG1, (errmsg_internal("cluster_grd_recovery: P1 freeze %u affected shards "
										 "(%s); P4 moved %u",
										 nfrozen,
										 pg_atomic_read_u32(&cluster_grd_state->recovery_direction)
												 == (uint32)GRD_REMASTER_DIR_JOIN
											 ? "join"
											 : "fail",
										 moved)));

		/* Affected shards now have a live master but unrebuilt holder
		 * state:  REBUILDING (requests stay fenced until P7). */
		for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
			if (grd_shard_bitmap_test(affected, (uint32)i))
				cluster_grd_shard_set_phase((uint32)i, GRD_SHARD_REBUILDING);
		}

		/* P0-1:  LOCK the episode to this epoch.  Everything downstream
		 * (rebind ack, barrier, REDECLARE_DONE, P6 sweep) is coherent
		 * against this one value;  a mid-episode bump aborts back to IDLE
		 * (see the WAIT_BARRIER / WAIT_CLUSTER epoch-change guards). */
		pg_atomic_write_u64(&cluster_grd_state->recovery_episode_epoch, cur_epoch);

		/* P5 arm the cooperative-rebind barrier + broadcast.  The rebind
		 * is CLUSTER-WIDE (P0#3):  the epoch bump staled EVERY stored
		 * holder identity, not only those on remastered shards. */
		gen = pg_atomic_add_fetch_u64(&cluster_grd_state->recovery_redeclare_generation, 1);
		deadline
			= TimestampTzPlusMilliseconds(GetCurrentTimestamp(), cluster_grd_rebuild_timeout_ms);
		pg_atomic_write_u64(&cluster_grd_state->recovery_barrier_deadline, (uint64)deadline);
		signaled = grd_recovery_broadcast_redeclare();
		pg_atomic_write_u32(&cluster_grd_state->recovery_state, (uint32)GRD_RECOVERY_WAIT_BARRIER);
		ereport(DEBUG1, (errmsg_internal("cluster_grd_recovery: P5 redeclare gen " UINT64_FORMAT
										 " broadcast to %d backends",
										 gen, signaled)));
		return; /* barrier evaluated from the next tick on */
	}

	if (state == (uint32)GRD_RECOVERY_WAIT_BARRIER) {
		uint64 gen = pg_atomic_read_u64(&cluster_grd_state->recovery_redeclare_generation);
		uint64 episode_epoch = pg_atomic_read_u64(&cluster_grd_state->recovery_episode_epoch);

		/* P0-1 epoch-coherence guard:  a SECOND epoch bump landed
		 * mid-episode (e.g. a third node's heartbeat flap re-fired
		 * reconfig).  The holders rebound under episode_epoch are now
		 * stale;  abort to IDLE and re-consume the event under the new
		 * epoch (shards stay frozen the whole time). */
		if (cluster_epoch_get_current() != episode_epoch) {
			grd_recovery_abort_to_idle();
			return;
		}

		/* spec-4.7 D2 — advance the survivor block re-declare scan one chunk
		 * while the GES rebind barrier is still pending (worker-centric, runs
		 * in this tick;  epoch-coherent via the guard just above). */
		grd_block_redeclare_step(episode_epoch);

		/*
		 * spec-4.7 D2/D7 (P0 code-review fix) — REDECLARE_DONE may be announced
		 * ONLY after BOTH the GES rebind barrier AND this survivor's block
		 * re-declare scan are complete.  Without the scan-complete conjunct, a
		 * fast GES barrier would let the episode reach IDLE before a held block
		 * (buffer after the scan cursor) was re-declared → the new master
		 * serves it as cold while the original node still holds X → 8.A
		 * double-grant.  The scan advances one chunk per tick above, so this
		 * just defers the announce until the whole pool is swept.
		 */
		if (grd_recovery_barrier_complete(gen, episode_epoch)
			&& grd_block_redeclare_scan_complete(episode_epoch)) {
			/*
			 * Local rebind barrier complete:  announce to every survivor
			 * (REDECLARE_DONE) and record self for the LOCKED episode
			 * epoch.  P6 must NOT run yet — this master's HTAB holds
			 * grants owned by REMOTE backends, and sweeping before THEIR
			 * barriers complete would delete a live-but-not-yet-rebound
			 * holder (double grant).
			 */
			pg_atomic_write_u64(&cluster_grd_state->recovery_done_epoch[cluster_node_id],
								episode_epoch);
			grd_recovery_broadcast_done(episode_epoch);
			pg_atomic_write_u32(&cluster_grd_state->recovery_state,
								(uint32)GRD_RECOVERY_WAIT_CLUSTER);
			ereport(DEBUG1,
					(errmsg_internal("cluster_grd_recovery: local barrier done "
									 "(gen " UINT64_FORMAT " epoch " UINT64_FORMAT
									 "); announced REDECLARE_DONE, waiting for all survivors",
									 gen, episode_epoch)));
			return;
		}

		if (GetCurrentTimestamp()
			> (TimestampTz)pg_atomic_read_u64(&cluster_grd_state->recovery_barrier_deadline)) {
			TimestampTz deadline;

			/*
			 * Barrier deadline expired:  fail-closed.  Affected shards
			 * STAY frozen (a half-rebuilt shard is never opened — the
			 * 53R9I surface on the request path is the user-visible
			 * fail-closed), the global sweep does NOT run, and the
			 * redeclare is re-broadcast with a fresh deadline so a
			 * slow-but-alive backend converges on its next CFI.
			 * (ereport(FATAL) here would crash-loop the respawned LMON
			 * against the same stalled barrier;  WARNING + retry keeps
			 * the fail-closed posture without self-DoS.)
			 */
			pg_atomic_fetch_add_u64(&cluster_grd_state->rebuild_timeout_count, 1);
			pg_atomic_fetch_add_u64(&cluster_grd_state->remaster_failed_count, 1);
			ereport(WARNING,
					(errcode(ERRCODE_CLUSTER_GRD_SHARD_REMASTERING),
					 errmsg("cluster GRD holder-rebuild barrier timed out; affected shards "
							"stay frozen"),
					 errhint("A backend has not acked the cooperative rebind within "
							 "cluster.grd_rebuild_timeout_ms; re-broadcasting.")));
			deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
												   cluster_grd_rebuild_timeout_ms);
			pg_atomic_write_u64(&cluster_grd_state->recovery_barrier_deadline, (uint64)deadline);
			(void)grd_recovery_broadcast_redeclare();
		}
		return;
	}

	if (state == (uint32)GRD_RECOVERY_WAIT_CLUSTER) {
		uint64 episode_epoch = pg_atomic_read_u64(&cluster_grd_state->recovery_episode_epoch);
		uint64 dead[(CLUSTER_MAX_NODES + 63) / 64];
		bool all_done = true;
		int i;

		/* P0-1 epoch-coherence guard (same as WAIT_BARRIER):  a
		 * mid-episode bump invalidates every node's locked barrier, so
		 * abort and re-run under the new epoch rather than sweep with a
		 * fresher epoch than the holders were rebound under. */
		if (cluster_epoch_get_current() != episode_epoch) {
			grd_recovery_abort_to_idle();
			return;
		}

		/* spec-4.7 D2 — keep advancing the block re-declare scan after the GES
		 * rebind barrier completed, so a large pool is fully re-declared within
		 * the recovery window (no-op once the cursor reaches NBuffers). */
		grd_block_redeclare_step(episode_epoch);

		for (i = 0; i < (CLUSTER_MAX_NODES + 63) / 64; i++)
			dead[i] = pg_atomic_read_u64(&cluster_grd_state->recovery_dead_bitmap[i]);

		/*
		 * spec-4.11 D1 (3b-4b Part 3) — launch one per-episode online thread-
		 * recovery worker for each in-scope dead origin (idempotent per tick;
		 * a NO-OP out of scope, so the spec-4.7 path is unchanged).  The worker
		 * online-replays the dead thread's WAL data + visibility to shared
		 * storage and publishes the node-local materialization authority; the
		 * D3 gate below then holds the GES shards frozen until it lands.
		 */
		cluster_thread_recovery_launch_workers(dead, (CLUSTER_MAX_NODES + 63) / 64, episode_epoch);

		/*
		 * spec-5.7 D3 S5d (§3.1b R4/R9) — launch one per-episode HW authority
		 * rebuild worker for each dead origin whose shards this survivor adopted.
		 * Independent of online_thread_recovery (the HW authority is default-on):
		 * a NO-OP unless the HW authority is engaged (multi-node + shared storage),
		 * idempotent per tick.  Off the LMON tick because the dead master's
		 * HW_RESERVE WAL-tail scan can be large; the P7 gate below holds the
		 * adopted shards frozen until each rebuild marks them.
		 */
		cluster_hw_remaster_launch_workers(dead, (CLUSTER_MAX_NODES + 63) / 64, episode_epoch);

		/*
		 * P6 cluster gate (P0#3):  EVERY survivor (declared ∧ not dead in
		 * this episode) must have announced its local barrier for the
		 * LOCKED episode epoch.  No timeout:  fail-closed — affected
		 * shards stay frozen until the cluster converges;  the lagging
		 * node's own rebuild_timeout WARNING is the observability surface.
		 */
		{
			bool is_join = (pg_atomic_read_u32(&cluster_grd_state->recovery_direction)
							== (uint32)GRD_REMASTER_DIR_JOIN);

			for (i = 0; i < CLUSTER_MAX_NODES; i++) {
				if (cluster_conf_lookup_node(i) == NULL)
					continue;
				if (grd_dead_bitmap_test(dead, i))
					continue;
				/*
				 * spec-5.16 D8 — for a JOIN episode the rejoining node(s) are the
				 * re-declare RECIPIENTS: a fresh joiner holds nothing to re-declare
				 * and never observes the JOIN_COMMITTED event as its own reconfig
				 * episode (it is published coordinator-side only), so it never
				 * announces REDECLARE_DONE.  Waiting for it would wedge the survivor's
				 * barrier forever.  The survivors (everyone outside THIS episode's
				 * recipient set) ARE the re-declarers and must converge.  Keyed on
				 * episode_epoch so a prior rejoiner (now a steady survivor) is still
				 * waited for (Hardening v1.4, reviewer P1 #1: a stale cross-episode
				 * exclusion would skip a survivor holding X on the joiner's home
				 * block -> premature unfreeze -> 8.A double-grant).
				 */
				if (is_join && join_fence_is_recipient_for(i, episode_epoch))
					continue;
				if (pg_atomic_read_u64(&cluster_grd_state->recovery_done_epoch[i])
					< episode_epoch) {
					all_done = false;
					break;
				}
			}
		}

		/* Re-announce each tick until released:  REDECLARE_DONE is
		 * fire-and-forget and a lost packet must not wedge a peer. */
		grd_recovery_broadcast_done(episode_epoch);

		if (all_done) {
			uint32 swept;
			uint32 unfrozen = 0;

			/*
			 * spec-4.11 D3 (3b-3) — the re-declare barrier restored the GES/GCS
			 * block PROTOCOL state, but a dead origin's WAL DATA must be online-
			 * replayed to shared storage before any survivor serves it; otherwise
			 * P7 would unfreeze the shards (resuming GES grants) while the dead
			 * thread's committed changes are still missing -> a survivor could act
			 * on a stale resource (8.A).  Stay in WAIT_CLUSTER until every dead
			 * origin's thread recovery is materialized on THIS node.  No timeout:
			 * fail-closed (the per-block cluster_gcs_block_phase_for_tag gate
			 * keeps the same posture for individual blocks; this gate additionally
			 * holds the GES shards frozen).  Out of scope — online_thread_recovery
			 * off (the default) / no shared backend / >2-node — the gate is a
			 * no-op, so the spec-4.7 unfreeze path is unchanged (no regression).
			 * REDECLARE_DONE was re-announced just above, so peers stay converged
			 * while we wait for the replay (the live executor lands in 3b-4).
			 */
			if (cluster_thread_recovery_gate_unfreeze(dead, (CLUSTER_MAX_NODES + 63) / 64)) {
				ereport(DEBUG1,
						(errmsg_internal(
							"cluster_grd_recovery: re-declare barrier passed but a dead origin's "
							"thread recovery is not yet materialized; staying frozen "
							"(epoch " UINT64_FORMAT ")",
							episode_epoch)));
				return;
			}

			/*
			 * spec-5.7 D3 S5d (§3.1b R4/R9) — HW authority rebuild gate.  Hold the
			 * shards frozen until every adopted shard this node masters has its HWM
			 * rebuilt from the dead master's snapshot+tail (hw_rebuilt_generation ==
			 * master_generation).  Otherwise P7 would set the shard NORMAL while its
			 * HWM is unknown, and a first HW_ALLOC could auto-create at block 0 over
			 * an already-allocated range (R9, 8.A).  A NO-OP when the HW authority is
			 * inactive, so the spec-4.6/4.7 unfreeze path is unchanged.
			 */
			if (cluster_hw_remaster_gate_unfreeze()) {
				ereport(
					DEBUG1,
					(errmsg_internal(
						"cluster_grd_recovery: re-declare barrier passed but an adopted shard's "
						"HW authority is not yet rebuilt; staying frozen (epoch " UINT64_FORMAT ")",
						episode_epoch)));
				return;
			}

			/* P6 post-barrier global sweep (P0#3 + P0-1:  legal HERE and
			 * coherent against the LOCKED episode epoch — holders rebound
			 * under it survive, only genuinely older state is removed). */
			swept = cluster_grd_cleanup_stale_epoch_postbarrier(episode_epoch);

			/* P7 unfreeze. */
			for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
				if (pg_atomic_read_u32(&cluster_grd_state->shard_phase[i])
					!= (uint32)GRD_SHARD_NORMAL) {
					cluster_grd_shard_set_phase((uint32)i, GRD_SHARD_NORMAL);
					unfrozen++;
				}
			}
			/*
			 * spec-5.16 D2/D5 — JOIN episodes count separately;  reaching IDLE
			 * means every declared member announced REDECLARE_DONE for the
			 * episode epoch (>= fence_epoch), so cluster_grd_block_view_rebuilt
			 * now returns true and the joiner-home PCM fence lifts (Hardening
			 * v1.1 all-members barrier).  Reset the direction to NONE.
			 */
			if (pg_atomic_read_u32(&cluster_grd_state->recovery_direction)
				== (uint32)GRD_REMASTER_DIR_JOIN) {
				pg_atomic_fetch_add_u64(&cluster_grd_state->join_remaster_done_count, 1);
				pg_atomic_fetch_add_u64(&cluster_grd_state->join_block_views_rebuilt_count, 1);
			} else {
				pg_atomic_fetch_add_u64(&cluster_grd_state->remaster_done_count, 1);
			}
			pg_atomic_write_u32(&cluster_grd_state->recovery_direction,
								(uint32)GRD_REMASTER_DIR_NONE);
			pg_atomic_write_u32(&cluster_grd_state->recovery_state, (uint32)GRD_RECOVERY_IDLE);
			ereport(DEBUG1,
					(errmsg_internal("cluster_grd_recovery: cluster gate passed; P6 swept %u "
									 "leaked slots; P7 unfroze %u shards (epoch " UINT64_FORMAT ")",
									 swept, unfrozen, episode_epoch)));
		}
	}
}

/*
 * cluster_grd_entry_rebind_or_insert_holder — spec-4.6 D3 master side.
 *
 *	Match key = (node_id, procno, lockmode) + resid:  a backend holds at
 *	most one grant per (resid, mode) (LOCALLOCK uniqueness), so the
 *	stale epoch/request_id of the old identity carry no information the
 *	master could verify.  Match → overwrite identity in place
 *	(unaffected-shard rebind;  idempotent for retransmits);  no match →
 *	insert (remastered-shard rebuild;  the new master fills holders[]
 *	from re-declarations ONLY).  Defensive double-grant check:  an
 *	insert that CONFLICTS with another backend's live holder is refused
 *	(the pre-crash grant set was compatible by construction, so a
 *	conflict here means a protocol anomaly — fail closed, the sender
 *	stays un-acked and its shard frozen).
 */
ClusterGrdEntryResult
cluster_grd_entry_rebind_or_insert_holder(const ClusterResId *resid,
										  const ClusterGrdHolderId *new_holder,
										  int32 source_node_id, int lockmode)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult er;
	int i;

	Assert(resid != NULL && new_holder != NULL);

	er = cluster_grd_entry_lookup_or_create(resid, true, &entry);
	if (er != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return er;

	SpinLockAcquire(&entry->lock);

	/* In-place rebind:  same backend + same mode. */
	for (i = 0; i < entry->ngranted; i++) {
		if ((uint32)entry->holders[i].node_id == new_holder->node_id
			&& entry->holders[i].procno == new_holder->procno
			&& entry->holders[i].mode == (LOCKMODE)lockmode) {
			uint32 rb_shard = cluster_grd_shard_for_resource(resid);

			entry->holders[i].cluster_epoch = new_holder->cluster_epoch;
			entry->holders[i].request_id = new_holder->request_id;
			entry->generation++;
			SpinLockRelease(&entry->lock);
			pg_atomic_fetch_add_u64(&cluster_grd_state->holders_rebound_count, 1);
			/* L13 evidence:  an in-place rebind on a NORMAL-phase shard
			 * is a surviving holder on an UNAFFECTED shard — it was not
			 * deleted by the scoped sweep and stays operable. */
			if (cluster_grd_shard_phase(rb_shard) == GRD_SHARD_NORMAL)
				pg_atomic_fetch_add_u64(&cluster_grd_state->unaffected_holder_survived_count, 1);
			return CLUSTER_GRD_ENTRY_OK;
		}
	}

	/* Defensive double-grant refusal (see header comment). */
	for (i = 0; i < entry->ngranted; i++) {
		if (((uint32)entry->holders[i].node_id != new_holder->node_id
			 || entry->holders[i].procno != new_holder->procno)
			&& !ges_modes_compatible(entry->holders[i].mode,
									 (LOCKMODE)lockmode)) { /* spec-5.1b D1: frozen matrix */
			SpinLockRelease(&entry->lock);
			return CLUSTER_GRD_ENTRY_ERROR;
		}
	}

	if (entry->ngranted >= PGRAC_GRD_MAX_HOLDERS) {
		pg_atomic_fetch_add_u64(&cluster_grd_state->holders_full_count, 1);
		SpinLockRelease(&entry->lock);
		return CLUSTER_GRD_ENTRY_FULL;
	}

	entry->holders[entry->ngranted].node_id = (int32)new_holder->node_id;
	entry->holders[entry->ngranted].procno = new_holder->procno;
	entry->holders[entry->ngranted].cluster_epoch = new_holder->cluster_epoch;
	entry->holders[entry->ngranted].request_id = new_holder->request_id;
	entry->holders[entry->ngranted].mode = (LOCKMODE)lockmode;
	entry->ngranted++;
	entry->generation++;
	SpinLockRelease(&entry->lock);

	(void)source_node_id; /* identity already carried by new_holder */
	pg_atomic_fetch_add_u64(&cluster_grd_state->holders_redeclared_count, 1);
	return CLUSTER_GRD_ENTRY_OK;
}


/* ============================================================
 * Observability accessors (D6 dump_grd consumers; 7 accessors).
 *
 *	v0.4 P1.1 修正:  补 shard_lookup_count + resid_encode_count
 *	accessor (v0.2/v0.3 shmem 有 field 但漏 accessor extern).
 * ============================================================ */

uint32
cluster_grd_local_master_count(void)
{
	uint32 count = 0;
	int i;

	Assert(cluster_grd_state != NULL);
	for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
		if ((int32)pg_atomic_read_u32(&cluster_grd_state->master[i]) == cluster_node_id)
			count++;
	}
	return count;
}

uint32
cluster_grd_remote_master_count(void)
{
	Assert(cluster_grd_state != NULL);
	return PGRAC_GRD_SHARD_COUNT - cluster_grd_local_master_count();
}

uint64
cluster_grd_shard_lookup_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->shard_lookup_count);
}

uint64
cluster_grd_local_master_lookup_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->local_master_lookup_count);
}

uint64
cluster_grd_remote_master_lookup_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->remote_master_lookup_count);
}

uint64
cluster_grd_resid_encode_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->resid_encode_count);
}

uint64
cluster_grd_master_map_refresh_count_get(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->master_map_refresh_count);
}


/* cluster_get_grd_shards SRF body lives in cluster_grd_srf.c (mirror
 * spec-2.3 cluster_ic_msg_types_srf.c split pattern) so test_cluster_grd
 * standalone unit test可以 link cluster_grd.o without pulling in
 * InitMaterializedSRF / tuplestore_putvalues / etc PG runtime symbols. */


/* ============================================================
 * spec-2.15:  Entry table API — lookup/create + release.
 *
 *   I13 hash 单源:cluster_grd_hash_resource() 算 14B hash;shard_id =
 *   hash64 % 4096;HTAB bucket via hash_search_with_hash_value() with
 *   32-bit projection of same hash64.  绝不让 dynahash 自己 hash key.
 *
 *   I17 double-cap check:
 *     1. HASH_FIND existing entry first; existing entries must remain
 *        reusable even when the table is at soft cap.
 *     2. Soft cap reads entry_current_count atomically and applies only
 *        to new entries.
 *     3. HASH_ENTER_NULL → NULL remains the hard-cap/OOM defensive path.
 * ============================================================ */

ClusterGrdEntryResult
cluster_grd_entry_lookup_or_create(const ClusterResId *resid, bool create, ClusterGrdEntry **out)
{
	uint64 hash64;
	uint32 shard_id;
	uint32 hashvalue;
	bool found;
	ClusterGrdEntry *entry;

	Assert(resid != NULL);
	Assert(out != NULL);
	if (resid == NULL || out == NULL)
		return CLUSTER_GRD_ENTRY_ERROR;

	*out = NULL;

	/* Step 1: skeleton-mode fast path (cluster.grd_max_entries=0 → htab
	 * NULL → NOT_READY).  caller 必处理(spec-2.16 caller-side 真激活前
	 * 固定走此路径). */
	if (cluster_grd_entry_htab == NULL)
		return CLUSTER_GRD_ENTRY_NOT_READY;

	/* Step 2: I13 single hash source — shard_id 与 HTAB bucket 必同源.
	 * cluster_grd_hash_resource() returns 14B hash (skip field4); use
	 * % 4096 for shard_id and 32-bit projection for HTAB hashvalue. */
	hash64 = cluster_grd_hash_resource(resid);
	shard_id = (uint32)(hash64 % PGRAC_GRD_SHARD_COUNT);
	hashvalue = (uint32)hash64;

	/* Step 3: shard partition LWLock acquire (I5 + I6 — shard partition
	 * LWLock 必先于 entry slock_t). */
	LWLockAcquire(&cluster_grd_shard_locks[shard_id].lock, LW_EXCLUSIVE);

	/* Step 4: always look for an existing entry before any cap decision.
	 * Otherwise a table at soft cap would reject reusing an already-created
	 * resource and return FULL incorrectly. */
	entry
		= hash_search_with_hash_value(cluster_grd_entry_htab, resid, hashvalue, HASH_FIND, &found);
	if (entry != NULL) {
		pg_atomic_fetch_add_u64(&cluster_grd_state->entry_lookup_hit_count, 1);
		LWLockRelease(&cluster_grd_shard_locks[shard_id].lock);
		*out = entry;
		return CLUSTER_GRD_ENTRY_OK;
	}

	if (!create) {
		LWLockRelease(&cluster_grd_shard_locks[shard_id].lock);
		return CLUSTER_GRD_ENTRY_NOT_FOUND;
	}

	/* Step 5: new-entry soft cap.  Use our own atomic current count rather
	 * than hash_get_num_entries(); future remove will decrement this counter
	 * in cluster_grd_entry_release while holding the proper partition lock. */
	if (pg_atomic_read_u64(&cluster_grd_state->entry_current_count)
		>= (uint64)cluster_grd_max_entries) {
		LWLockRelease(&cluster_grd_shard_locks[shard_id].lock);
		pg_atomic_fetch_add_u64(&cluster_grd_state->entry_full_count, 1);
		ereport(LOG, (errmsg("cluster_grd: entry table soft cap reached "
							 "(cluster.grd_max_entries = %d)",
							 cluster_grd_max_entries)));
		return CLUSTER_GRD_ENTRY_FULL;
	}

	/* Step 6: HASH_ENTER_NULL only after existing lookup + soft cap.  NOT
	 * HASH_ENTER because the latter ereport(ERROR) cannot support the FULL
	 * sentinel. */
	entry = hash_search_with_hash_value(cluster_grd_entry_htab, resid, hashvalue, HASH_ENTER_NULL,
										&found);

	/* Step 7: sentinel 5 paths — FULL on HASH_ENTER_NULL OOM defensive
	 * bounce; OK otherwise. */
	if (entry == NULL) {
		LWLockRelease(&cluster_grd_shard_locks[shard_id].lock);
		/* HASH_ENTER_NULL returned NULL — shmem OOM defensive. */
		pg_atomic_fetch_add_u64(&cluster_grd_state->entry_full_count, 1);
		ereport(LOG, (errmsg("cluster_grd: HASH_ENTER_NULL returned NULL "
							 "(shmem OOM defensive bounce)")));
		return CLUSTER_GRD_ENTRY_FULL;
	}

	if (!found) {
		/* New entry — init slock + body zero. */
		SpinLockInit(&entry->lock);
		entry->ngranted = 0;
		entry->nwaiters = 0;
		entry->nconverts = 0;
		entry->last_modified_scn = 0;
		entry->state_flags = 0;
		/* holders / waiters / converts arrays left uninitialized;
		 * spec-2.16 mutator path initializes per-slot on add. */
		pg_atomic_fetch_add_u64(&cluster_grd_state->entry_current_count, 1);
		pg_atomic_fetch_add_u64(&cluster_grd_state->entry_create_count, 1);
	}
	pg_atomic_fetch_add_u64(&cluster_grd_state->entry_lookup_hit_count, 1);

	/* Step 8: release shard partition LWLock — caller holds entry handle. */
	LWLockRelease(&cluster_grd_shard_locks[shard_id].lock);

	*out = entry;
	return CLUSTER_GRD_ENTRY_OK;
}

void
cluster_grd_entry_release(ClusterGrdEntry *entry)
{
	/* spec-2.15 RESERVED no-op (v0.3 P1.3 contract unified — header doc
	 * + impl 一致).  本 spec 不保证任何 side effect:不 decrement
	 * refcount,不 remove entry,不改 holders/waiters/converts 状态.
	 *
	 * spec-2.16 caller-side 集成时真实装 logic (API signature 不变,body
	 * 加):decrement refcount + 若 ngranted == 0 && nwaiters == 0 &&
	 * nconverts == 0 → HASH_REMOVE + DRM reclaim path (Stage 6).
	 */
	(void)entry;
}


/* ============================================================
 * spec-2.15 v0.3:  6 observability accessor (P1.2 metric scope 收紧).
 *
 *   3 derived/internal (GUC value / entry_current_count / static
 *   allocated_bytes) + 3 public atomic lifetime counters
 *   (entry_create_count / entry_lookup_hit_count / entry_full_count)
 *   = 6 cleanly-observable metrics.
 *
 *   holder/waiter/convert counter 推 spec-2.16 配 mutator API.
 * ============================================================ */

int
cluster_grd_max_entries_get(void)
{
	return cluster_grd_max_entries;
}

int
cluster_grd_entry_count(void)
{
	if (cluster_grd_entry_htab == NULL)
		return 0;
	return (int)pg_atomic_read_u64(&cluster_grd_state->entry_current_count);
}

Size
cluster_grd_allocated_bytes(void)
{
	return cluster_grd_entries_alloc_bytes;
}

uint64
cluster_grd_entry_create_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->entry_create_count);
}

uint64
cluster_grd_entry_lookup_hit_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->entry_lookup_hit_count);
}

uint64
cluster_grd_entry_full_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->entry_full_count);
}


/* ============================================================
 * spec-2.15 D8:  SRF row visitor — hash_seq_search the entry HTAB
 *   and emit one row per entry under per-entry slock_t snapshot.
 *
 *   **spec-2.16 forward-link TODO (P2.4 + I14)**:
 *   Wrap hash_seq_search in full 4096-shard LW_SHARED acquire OR
 *   chunked snapshot to defend concurrent HASH_ENTER_NULL writers
 *   once caller-side LockAcquire integration lands.  本 spec 0
 *   caller → 0 row → 无并发问题 (本 walker safe).
 * ============================================================ */

void
cluster_grd_entries_walk(ClusterGrdEntryRowVisitor visitor, void *ctx)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;

	Assert(visitor != NULL);

	/* skeleton mode (cluster.grd_max_entries=0) → 0 row.  Mirrors the
	 * NOT_READY sentinel surface for SRF callers. */
	if (cluster_grd_entry_htab == NULL)
		return;

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		int32 fields[11];

		/* per-entry slock_t snapshot — short critical section (memcpy
		 * fixed-size struct fields).  spec-2.16 mutator writers also
		 * acquire entry->lock so snapshot is consistent. */
		SpinLockAcquire(&entry->lock);
		fields[0] = (int32)(cluster_grd_hash_resource(&entry->resid) % PGRAC_GRD_SHARD_COUNT);
		fields[1] = (int32)entry->resid.field1;
		fields[2] = (int32)entry->resid.field2;
		fields[3] = (int32)entry->resid.field3;
		fields[4] = (int32)entry->resid.field4;
		fields[5] = (int32)entry->resid.type;
		fields[6] = (int32)entry->resid.lockmethodid;
		fields[7] = entry->ngranted;
		fields[8] = entry->nwaiters;
		fields[9] = entry->nconverts;
		fields[10] = (int32)entry->state_flags;
		SpinLockRelease(&entry->lock);

		visitor(ctx, fields);
	}
}


/* ============================================================
 * spec-2.16 D2:  9 counter accessor + mutator stub + should_globalize
 *   stub + LOCKMODE compat stub + cleanup stub.
 *
 *   All mutator bodies are规则 8 ERRCODE_FEATURE_NOT_SUPPORTED stubs
 *   with errhint pointing to the activating Step (Step 4 D9).
 *   Skeleton phase guarantees Step 1 ship does not break cluster_unit
 *   or PG 219 regression.
 * ============================================================ */

uint64
cluster_grd_holders_full_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->holders_full_count);
}

uint64
cluster_grd_waiters_full_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->waiters_full_count);
}

uint64
cluster_grd_converts_full_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->converts_full_count);
}

uint64
cluster_grd_ngranted_promoted_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->ngranted_promoted_count);
}

uint64
cluster_grd_ges_work_queue_full_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->ges_work_queue_full_count);
}

uint64
cluster_grd_ges_cleanup_deferred_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->ges_cleanup_deferred_count);
}

uint64
cluster_grd_ges_inbound_validation_fail_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->ges_inbound_validation_fail_count);
}

uint64
cluster_grd_ges_reply_deferred_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->ges_reply_deferred_count);
}

uint64
cluster_grd_ges_reply_dropped_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->ges_reply_dropped_count);
}

void
cluster_grd_inc_ges_work_queue_full(void)
{
	Assert(cluster_grd_state != NULL);
	pg_atomic_fetch_add_u64(&cluster_grd_state->ges_work_queue_full_count, 1);
}

void
cluster_grd_inc_ges_cleanup_deferred(void)
{
	Assert(cluster_grd_state != NULL);
	pg_atomic_fetch_add_u64(&cluster_grd_state->ges_cleanup_deferred_count, 1);
}

void
cluster_grd_inc_ges_inbound_validation_fail(void)
{
	Assert(cluster_grd_state != NULL);
	pg_atomic_fetch_add_u64(&cluster_grd_state->ges_inbound_validation_fail_count, 1);
}

/* spec-2.24 D5 — cleanup_skip_stale_cancel(4-tuple match fail in LMD dispatch). */
void
cluster_grd_inc_cleanup_skip_stale_cancel(void)
{
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->cleanup_skip_stale_cancel_count, 1);
}

uint64
cluster_grd_cleanup_skip_stale_cancel_count(void)
{
	if (cluster_grd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_grd_state->cleanup_skip_stale_cancel_count);
}

/* spec-2.25 D13 — RELATION + OBJECT cluster gate hit counter accessor. */
void
cluster_grd_inc_relation_object_cluster_path(void)
{
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->relation_object_cluster_path_count, 1);
}

uint64
cluster_grd_relation_object_cluster_path_count(void)
{
	if (cluster_grd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_grd_state->relation_object_cluster_path_count);
}

/* spec-2.26 D5 — TRANSACTION cluster gate hit counter accessor. */
void
cluster_grd_inc_transaction_cluster_path(void)
{
	if (cluster_grd_state != NULL)
		pg_atomic_fetch_add_u64(&cluster_grd_state->transaction_cluster_path_count, 1);
}

uint64
cluster_grd_transaction_cluster_path_count(void)
{
	if (cluster_grd_state == NULL)
		return 0;
	return pg_atomic_read_u64(&cluster_grd_state->transaction_cluster_path_count);
}

void
cluster_grd_inc_ges_reply_deferred(void)
{
	Assert(cluster_grd_state != NULL);
	pg_atomic_fetch_add_u64(&cluster_grd_state->ges_reply_deferred_count, 1);
}

void
cluster_grd_inc_ges_reply_dropped(void)
{
	Assert(cluster_grd_state != NULL);
	pg_atomic_fetch_add_u64(&cluster_grd_state->ges_reply_dropped_count, 1);
}


/* ============================================================
 * should_globalize — D10 skeleton.
 *
 *   Step 1:  always return false (no LOCKTAG enters cluster path).
 *   Step 4 D10:  O(1) allowlist (RELATION / TRANSACTION / OBJECT /
 *   ADVISORY classes per cluster_grd_is_cluster_aware contract).
 * ============================================================ */

bool
cluster_grd_should_globalize(const LOCKTAG *tag)
{
	/* Step 4 D10:  O(1) allowlist anchored on cluster_grd_is_cluster_aware
	 * (4 LockTagType classes — RELATION / TRANSACTION / OBJECT / ADVISORY
	 * per spec-2.14).  No catalog lookup;  branch-only dispatch.
	 *
	 *   spec-2.16 v0.4 L1.9 contract:  O(1), no catalog SQL, no LWLock,
	 *   no allocation.  Fast path for non-cluster locks returns false
	 *   immediately (~3 instructions).
	 *
	 *   Future spec-2.17+ may extend allowlist via cached relpersistence
	 *   for RELATION class (heap_open + cache);  本 spec scope skip. */
	if (tag == NULL)
		return false;
	return cluster_grd_is_cluster_aware(tag);
}


/* ============================================================
 * LOCKMODE compat — D9 helper (Step 1 skeleton).
 *
 *   Step 4 D9:  wires to lmgr/lock.c LockMethodConflicts (NEW
 *   exposed symbol via PGRAC MODIFICATIONS in lock.c).  Skeleton
 *   returns true conservatively (any-mode conflicts any-mode) to
 *   keep safety contract — no false GRANT before Step 4 真激活.
 * ============================================================ */

bool
cluster_grd_lockmode_conflicts(int held pg_attribute_unused(), int wanted pg_attribute_unused())
{
	return true; /* skeleton — Step 4 D9 wires real LockMethodConflicts */
}


/* ============================================================
 * spec-2.21 D5 — minimal ADVISORY mutator real bodies.
 *
 *   MVP scope: LOCKTAG_ADVISORY only (per spec-2.21 Q3 v1.1).  Caller
 *   must hold the entry->lock spinlock (S3.1 / S5.1 / S6.1 sequence;
 *   shard partition LWLock is the outer lock per HC8 lock-order safe).
 *
 *   Compatibility / queueing logic is intentionally minimal — full
 *   conflict matrix + FIFO waiter promotion live in D8 LMS worker.
 *   These helpers expose the slot lifecycle (grant / release / reserve)
 *   that D8 + cluster_lock_acquire.c S3-S6 build on.
 * ============================================================ */

ClusterGrdEntryResult
cluster_grd_entry_grant_holder(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder, int mode)
{
	int slot;

	Assert(entry != NULL && holder != NULL);

	if (entry->ngranted >= PGRAC_GRD_MAX_HOLDERS) {
		cluster_grd_inc_ges_work_queue_full();
		return CLUSTER_GRD_ENTRY_FULL;
	}
	slot = entry->ngranted++;
	entry->holders[slot].node_id = (int32)holder->node_id;
	entry->holders[slot].procno = holder->procno;
	entry->holders[slot].cluster_epoch = holder->cluster_epoch;
	entry->holders[slot].request_id = holder->request_id;
	entry->holders[slot].mode = (LOCKMODE)mode;
	entry->generation++;
	return CLUSTER_GRD_ENTRY_OK;
}

ClusterGrdEntryResult
cluster_grd_entry_release_holder(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder)
{
	int i;

	Assert(entry != NULL && holder != NULL);

	for (i = 0; i < entry->ngranted; i++) {
		if ((uint32)entry->holders[i].node_id == holder->node_id
			&& entry->holders[i].procno == holder->procno
			&& entry->holders[i].cluster_epoch == holder->cluster_epoch
			&& entry->holders[i].request_id == holder->request_id) {
			/* compact down */
			if (i < entry->ngranted - 1)
				entry->holders[i] = entry->holders[entry->ngranted - 1];
			memset(&entry->holders[entry->ngranted - 1], 0, sizeof(ClusterGrdHolder));
			entry->ngranted--;
			entry->generation++;
			return CLUSTER_GRD_ENTRY_OK;
		}
	}
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

ClusterGrdEntryResult
cluster_grd_entry_add_waiter(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder, int mode)
{
	int slot;

	Assert(entry != NULL && holder != NULL);

	if (entry->nwaiters >= PGRAC_GRD_MAX_WAITERS) {
		cluster_grd_inc_ges_work_queue_full();
		return CLUSTER_GRD_ENTRY_FULL;
	}
	slot = entry->nwaiters++;
	entry->waiters[slot].node_id = (int32)holder->node_id;
	/*
	 * spec-2.23 D6 — populate full reply identity from the GES holder
	 * tuple.  source_node_id mirrors node_id (the node hosting the
	 * waiting backend).  request_opcode defaults to GES_REQ_OPCODE_
	 * REQUEST when caller is the legacy 2-arg add_waiter path; the
	 * cluster_grd_entry_enqueue_or_grant entry point overrides it
	 * via the extended ClusterGrdWaiter mutation directly.
	 */
	entry->waiters[slot].source_node_id = (int32)holder->node_id;
	entry->waiters[slot].procno = holder->procno;
	entry->waiters[slot].cluster_epoch = holder->cluster_epoch;
	entry->waiters[slot].request_id = holder->request_id;
	entry->waiters[slot].request_opcode = 1; /* GES_REQ_OPCODE_REQUEST default */
	entry->waiters[slot].mode = (LOCKMODE)mode;
	/* spec-2.21: 0 placeholder — real timestamp 推 spec-2.22 wait-edge maintenance.
	 * Standalone cluster_unit binaries don't link utils/timestamp.o; using a real
	 * GetCurrentTimestamp() call broke L41 link surface on macOS arm64. */
	entry->waiters[slot].wait_start = 0;
	entry->generation++;
	return CLUSTER_GRD_ENTRY_OK;
}

ClusterGrdEntryResult
cluster_grd_entry_promote_waiter(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder)
{
	int i;

	Assert(entry != NULL && holder != NULL);

	for (i = 0; i < entry->nwaiters; i++) {
		if ((uint32)entry->waiters[i].node_id == holder->node_id
			&& entry->waiters[i].procno == holder->procno
			&& entry->waiters[i].cluster_epoch == holder->cluster_epoch
			&& entry->waiters[i].request_id == holder->request_id) {
			LOCKMODE mode = entry->waiters[i].mode;

			if (i < entry->nwaiters - 1)
				entry->waiters[i] = entry->waiters[entry->nwaiters - 1];
			memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(ClusterGrdWaiter));
			entry->nwaiters--;
			entry->generation++;
			return cluster_grd_entry_grant_holder(entry, holder, mode);
		}
	}
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}


/* ============================================================
 * spec-2.23 D6 — GRD-owned grant / waiter-pop API (HC18 / HC19 / HC20).
 *
 *	Bundles conflict-matrix check + grant-or-enqueue + waiter-identity
 *	carry under a single critical section.  Cluster_lms.c dispatch uses
 *	these instead of the spec-2.21 lower-level mutators so that the
 *	ClusterGrdEntry body stays opaque (cluster_grd.h line 104
 *	forward-decl invariant).
 *
 *	Conflict judgement uses the frozen 8x8 compatibility matrix via
 *	ges_modes_compatible() (spec-5.1b D1).  Per the spec-5.1a contract
 *	ges_modes_compatible(held, wanted) == !DoLockModesConflict(wanted,
 *	held);  the startup self-check in cluster_ges_mode_backend.c guards
 *	that equivalence against PG's live conflict table.
 * ============================================================ */

/* ============================================================
 * spec-5.10 — GES enqueue lock-starvation fairness.
 *
 *	GES object-lock enqueue grants on "compatible-with-holder" alone, with no
 *	request-vs-waiter fairness:  a steady stream of compatible S requests can
 *	starve a queued X waiter (and a convert flood can starve a plain waiter).
 *	spec-5.10 adds bounded fairness at the GRD master — the single queue
 *	authority (AD-013) — entirely master-local: scan-on-grant charges every
 *	jumped earlier conflicting waiter one skip; at a bounded skip count the
 *	waiter is boosted to head-of-line; a later compatible jumper that conflicts
 *	with a boosted waiter is held behind it (D2 grant barrier).  All fairness
 *	state is shmem-only and never on the wire (Q12 / L5).
 * ============================================================ */

/*
 * spec-5.10 — read the cluster-global starvation-protection toggle.  Returns
 * true when bounded fairness is active.  Safe to call before shmem init (the
 * GES grant path always runs after it, but defensive NULL -> "on" keeps the
 * conservative default).
 */
bool
cluster_grd_starvation_protection_enabled(void)
{
	if (cluster_grd_state == NULL)
		return true;
	return pg_atomic_read_u32(&cluster_grd_state->starvation_protection_enabled) != 0;
}

/*
 * spec-5.10 D7 — flip the cluster-global starvation-protection toggle.  Only
 * flips the shared flag (so a new grant decision reads it immediately and falls
 * back to spec-5.1b);  clearing already-boosted waiters + retracting barrier
 * edges is the LMON-context sweep (D8), NOT done here (P1#1 — never touch the
 * shared GRD/LMD graph from a GUC assign-hook).
 */
void
cluster_grd_set_starvation_protection(bool enabled)
{
	if (cluster_grd_state == NULL)
		return;
	pg_atomic_write_u32(&cluster_grd_state->starvation_protection_enabled, enabled ? 1 : 0);
	/*
	 * spec-5.10 fix-forward (P1#1) — when protection is turned OFF, request the
	 * LMON-context sweep that clears already-boosted waiters + retracts the
	 * FAIRNESS_BARRIER edges so the disable is a clean escape rather than
	 * leaving stale boost/edge state to age out on the next unrelated drain.
	 * Only flip the request flag here (a single atomic write is assign-hook
	 * safe); the actual sweep runs in cluster_grd_lmon_tick_starvation_sweep.
	 */
	if (!enabled)
		pg_atomic_write_u32(&cluster_grd_state->starvation_sweep_pending, 1);
}

/*
 * spec-5.10 fix-forward — LMON-context runtime-off sweep.  Test-and-clear the
 * pending flag and, if protection is (still) off, clear every boosted waiter +
 * retract barrier edges across all GRD entries.  Returns the number of boosted
 * flags cleared (0 if no sweep was pending).  Called from the LMON tick.
 */
uint32
cluster_grd_lmon_tick_starvation_sweep(void)
{
	if (cluster_grd_state == NULL)
		return 0;
	if (pg_atomic_exchange_u32(&cluster_grd_state->starvation_sweep_pending, 0) == 0)
		return 0;
	/* A concurrent re-enable wins: nothing to sweep if protection came back on. */
	if (cluster_grd_starvation_protection_enabled())
		return 0;
	return cluster_grd_clear_all_boosted();
}

/* spec-5.10 D7 — starvation-fairness observability counter accessors. */
uint64
cluster_grd_starvation_boost_count(void)
{
	return cluster_grd_state ? pg_atomic_read_u64(&cluster_grd_state->starvation_boost_count) : 0;
}

uint64
cluster_grd_starvation_barrier_enqueued_count(void)
{
	return cluster_grd_state
			   ? pg_atomic_read_u64(&cluster_grd_state->starvation_barrier_enqueued_count)
			   : 0;
}

uint64
cluster_grd_starvation_barrier_publish_fail_count(void)
{
	return cluster_grd_state
			   ? pg_atomic_read_u64(&cluster_grd_state->starvation_barrier_publish_fail_count)
			   : 0;
}

uint64
cluster_grd_starvation_max_skip_observed(void)
{
	return cluster_grd_state ? pg_atomic_read_u64(&cluster_grd_state->starvation_max_skip_observed)
							 : 0;
}

/*
 * spec-5.10 D8 — clear boosted state across every GRD entry (runtime-off clean
 * escape).  Runs in the LMON cluster context (NOT a GUC assign-hook, P1#1):
 * under each entry->lock it clears every waiter / convert boosted flag, then —
 * after releasing the spinlock — refreshes that entry's WFG edges so the now-
 * dissolved FAIRNESS_BARRIER edges are retracted (the refresh re-derives the
 * barrier set, which is now empty).  Barriered waiters are no longer held back
 * and are served by the next release/drain (with protection off the serve-side
 * barrier is already a no-op).  Returns the number of boosted flags cleared.
 */
uint32
cluster_grd_clear_all_boosted(void)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	uint32 cleared = 0;

	if (cluster_grd_entry_htab == NULL)
		return 0;

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		GrdWfgSnapshot snap;
		bool changed = false;
		int i;

		SpinLockAcquire(&entry->lock);
		for (i = 0; i < entry->nwaiters; i++) {
			if (entry->waiters[i].boosted) {
				entry->waiters[i].boosted = false;
				cleared++;
				changed = true;
			}
		}
		for (i = 0; i < entry->nconverts; i++) {
			if (entry->converts[i].boosted) {
				entry->converts[i].boosted = false;
				changed = true;
			}
		}
		if (changed)
			entry->generation++;
		grd_wfg_snapshot_locked(entry, &snap);
		SpinLockRelease(&entry->lock);

		if (changed) {
			for (i = 0; i < snap.n_waiters; i++)
				grd_wfg_refresh_waiter_edges(&snap, &snap.waiters[i]);
		}
	}
	return cleared;
}

/*
 * spec-5.10 D8 — clear boosted state contributed by a dead / fenced / reconfig-
 * out node (LMON node-dead path).  A boosted waiter on a departed node must not
 * keep barriering live requests behind a vertex that no longer exists; clearing
 * its boost dissolves those barriers (the dead node's queue entries are removed
 * by the existing dead-node sweep).  Same lock discipline as
 * cluster_grd_clear_all_boosted.  Returns the number of boosted flags cleared.
 */
uint32
cluster_grd_clear_boosted_for_node(int32 dead_node)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	uint32 cleared = 0;

	if (cluster_grd_entry_htab == NULL)
		return 0;

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		GrdWfgSnapshot snap;
		bool changed = false;
		int i;

		SpinLockAcquire(&entry->lock);
		for (i = 0; i < entry->nwaiters; i++) {
			if (entry->waiters[i].boosted && entry->waiters[i].node_id == dead_node) {
				entry->waiters[i].boosted = false;
				cleared++;
				changed = true;
			}
		}
		for (i = 0; i < entry->nconverts; i++) {
			if (entry->converts[i].boosted && entry->converts[i].node_id == dead_node) {
				entry->converts[i].boosted = false;
				changed = true;
			}
		}
		if (changed)
			entry->generation++;
		grd_wfg_snapshot_locked(entry, &snap);
		SpinLockRelease(&entry->lock);

		if (changed) {
			for (i = 0; i < snap.n_waiters; i++)
				grd_wfg_refresh_waiter_edges(&snap, &snap.waiters[i]);
		}
	}
	return cleared;
}

/*
 * spec-5.10 D4 — mint the next master-local fair-queue order for a waiter /
 * convert that is about to enqueue.  Strictly monotonic per entry (the small
 * uint64 wrap is handled wrap-safely by grd_fair_seq_precedes).  Caller holds
 * entry->lock.  0 is reserved for "not yet minted".
 */
static uint64
grd_mint_fair_queue_seq(ClusterGrdEntry *entry)
{
	entry->fair_queue_next += 1;
	return entry->fair_queue_next;
}

/*
 * spec-5.10 D4 — wrap-safe "did a precede b" for fair_queue_seq ordering.
 * Mirrors PG's TransactionIdPrecedes / LSN-style modular compare so the rare
 * uint64 wrap never inverts the serve order.  0 (unminted) precedes everything.
 */
static inline bool
grd_fair_seq_precedes(uint64 a, uint64 b)
{
	return (int64)(a - b) < 0;
}

/*
 * spec-5.10 D2 — scan-on-grant skip accounting.  A holder-compatible request
 * that is about to be granted "jumps" every already-queued waiter whose wanted
 * mode conflicts with it (a compatible waiter is not delayed by the grant and
 * is not charged).  Every queued waiter is by construction earlier than this
 * brand-new request, so no explicit ordering test is needed here.  Counting
 * only — Rule 8.A:  this never blocks or alters the grant.  Caller holds
 * entry->lock.
 */
static void
grd_starvation_account_skip_on_grant(ClusterGrdEntry *entry, LOCKMODE granted_mode)
{
	int i;

	int max_skips = cluster_ges_starvation_max_skips;

	for (i = 0; i < entry->nwaiters; i++) {
		/* Compatible waiter -> the grant does not delay it -> no skip. */
		if (ges_modes_compatible(entry->waiters[i].mode, granted_mode))
			continue;
		if (entry->waiters[i].skip_count < UINT32_MAX)
			entry->waiters[i].skip_count++;
		/* Observability high-water (D7). */
		if (cluster_grd_state != NULL
			&& entry->waiters[i].skip_count
				   > pg_atomic_read_u64(&cluster_grd_state->starvation_max_skip_observed))
			pg_atomic_write_u64(&cluster_grd_state->starvation_max_skip_observed,
								entry->waiters[i].skip_count);
		/*
		 * Bounded fairness: once the waiter has been jumped max_skips times it
		 * is boosted to head-of-line (max_skips <= 0 disables boosting; the
		 * waiter still accrues skips for observability).
		 */
		if (max_skips > 0 && entry->waiters[i].skip_count >= (uint32)max_skips
			&& !entry->waiters[i].boosted) {
			entry->waiters[i].boosted = true;
			if (cluster_grd_state != NULL)
				pg_atomic_fetch_add_u64(&cluster_grd_state->starvation_boost_count, 1);
		}
	}
}

/*
 * spec-5.10 D2 — does a holder-compatible requester R conflict with any
 * CURRENT non-self holder?  Mirrors the conflict scan in enqueue_or_grant_impl
 * (frozen 8x8 matrix + spec-5.1c self-exclusion) but without populating the
 * BAST snapshot.  Used to revalidate a barrier candidate after the edge-publish
 * spinlock gap and on the failed-barrier fallback (never grant a request that
 * conflicts with a holder — Rule 8.A).  Caller holds entry->lock.
 */
static bool
grd_scan_holder_conflict(const ClusterGrdEntry *entry, uint32 node_id, uint32 procno, LOCKMODE mode)
{
	int i;

	for (i = 0; i < entry->ngranted; i++) {
		if (ges_modes_compatible(entry->holders[i].mode, mode))
			continue;
		if ((uint32)entry->holders[i].node_id == node_id && entry->holders[i].procno == procno)
			continue; /* spec-5.1c self-exclusion */
		return true;
	}
	return false;
}

/*
 * spec-5.10 D2/D5 — find the boosted queued waiter that a would-be-granted
 * request of `mode` must be held behind: the earliest (min fair_queue_seq)
 * boosted waiter whose wanted mode conflicts with `mode`.  exclude_seq > 0
 * restricts the search to waiters strictly EARLIER than that seq (used when the
 * requester is itself queued — serve-side / refresh); exclude_seq == 0 means
 * the requester is not yet queued (grant-time) so every boosted waiter counts.
 * Returns the waiter index, or -1 if no such waiter exists.  Caller holds
 * entry->lock.
 */
static int
grd_find_earliest_boosted_conflicting_waiter(const ClusterGrdEntry *entry, LOCKMODE mode,
											 uint64 exclude_seq)
{
	int i, best = -1;

	for (i = 0; i < entry->nwaiters; i++) {
		if (!entry->waiters[i].boosted)
			continue;
		if (ges_modes_compatible(entry->waiters[i].mode, mode))
			continue; /* no conflict -> does not block the requester */
		if (exclude_seq != 0
			&& !grd_fair_seq_precedes(entry->waiters[i].fair_queue_seq, exclude_seq))
			continue; /* not earlier than the requester */
		if (best < 0
			|| grd_fair_seq_precedes(entry->waiters[i].fair_queue_seq,
									 entry->waiters[best].fair_queue_seq))
			best = i;
	}
	return best;
}

/*
 * spec-5.10 D5 — build the spec-5.8-owned canonical WFG vertex for a queued
 * waiter so a FAIRNESS_BARRIER edge can point at it WITHOUT fabricating a
 * vertex (P0#3).  Reuses grd_wfg_make_vertex with the waiter's persisted
 * canonical wait identity (waiter_xid + wait_seq, spec-5.8 D1c/D1e).  Caller
 * holds entry->lock.
 */
static void
grd_capture_waiter_vertex(const ClusterGrdEntry *entry, int idx, ClusterLmdVertex *out)
{
	grd_wfg_make_vertex(entry->waiters[idx].node_id, entry->waiters[idx].procno,
						entry->waiters[idx].cluster_epoch, entry->waiters[idx].request_id,
						entry->waiters[idx].waiter_xid, entry->waiters[idx].wait_seq, out);
}

/*
 * spec-5.10 — is queued waiter `widx` barriered behind an earlier boosted
 * conflicting waiter?  Serve-side mirror of the grant-time barrier test: the
 * drain must not promote a waiter while a boosted waiter it conflicts with is
 * still ahead of it.  Caller holds entry->lock.
 */
static bool
grd_waiter_is_barriered(const ClusterGrdEntry *entry, int widx)
{
	if (!cluster_grd_starvation_protection_enabled())
		return false;
	return grd_find_earliest_boosted_conflicting_waiter(entry, entry->waiters[widx].mode,
														entry->waiters[widx].fair_queue_seq)
		   >= 0;
}

/*
 * spec-5.10 — fill one waiter slot with full reply identity + spec-5.8
 * canonical wait identity + fresh spec-5.10 fairness state (fair_queue_seq
 * minted here, P1c).  Shared by the plain enqueue path and the barrier enqueue
 * so the two can never drift.  Returns the slot index, or -1 if the waiter
 * queue is full (caller fails closed).  Caller holds entry->lock.
 */
static int
grd_enqueue_waiter_locked(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder,
						  int32 source_node_id, uint64 request_id, uint64 shard_master_generation,
						  uint32 request_opcode, LOCKMODE lockmode, ClusterGrdWaiterMeta meta)
{
	int slot;

	if (entry->nwaiters >= PGRAC_GRD_MAX_WAITERS)
		return -1;

	slot = entry->nwaiters++;
	entry->waiters[slot].node_id = (int32)holder->node_id;
	entry->waiters[slot].source_node_id = source_node_id;
	entry->waiters[slot].procno = holder->procno;
	entry->waiters[slot].cluster_epoch = holder->cluster_epoch;
	entry->waiters[slot].request_id = request_id;
	entry->waiters[slot].waiter_xid = meta.xid;	   /* spec-5.8 D1c */
	entry->waiters[slot].wait_seq = meta.wait_seq; /* spec-5.8 D1e */
	entry->waiters[slot].shard_master_generation = shard_master_generation;
	entry->waiters[slot].request_opcode = request_opcode;
	entry->waiters[slot].mode = lockmode;
	entry->waiters[slot].wait_start = 0;
	entry->waiters[slot].fair_queue_seq = grd_mint_fair_queue_seq(entry); /* spec-5.10 D4 */
	entry->waiters[slot].skip_count = 0;								  /* spec-5.10 D2 */
	entry->waiters[slot].boosted = false;								  /* spec-5.10 D2 */
	entry->generation++;
	return slot;
}

/*
 * spec-5.10 D6 — observability accessor: report a queued waiter's fairness
 * state (skip_count / boosted / fair_queue_seq) by its 4-tuple identity.
 * Returns true iff a matching queued REQUEST waiter exists.  Read-only;
 * acquires entry->lock for a consistent snapshot.  Used by the cluster_unit
 * suite and (D7) the dump/observability surface.
 */
bool
cluster_grd_entry_describe_waiter(const ClusterResId *resid, const ClusterGrdHolderId *id,
								  uint32 *out_skip_count, bool *out_boosted,
								  uint64 *out_fair_queue_seq)
{
	ClusterGrdEntry *entry = NULL;
	bool found = false;
	int i;

	if (resid == NULL || id == NULL)
		return false;
	if (cluster_grd_entry_lookup_or_create(resid, false, &entry) != CLUSTER_GRD_ENTRY_OK
		|| entry == NULL)
		return false;

	SpinLockAcquire(&entry->lock);
	for (i = 0; i < entry->nwaiters; i++) {
		if ((uint32)entry->waiters[i].node_id == id->node_id
			&& entry->waiters[i].procno == id->procno
			&& entry->waiters[i].cluster_epoch == id->cluster_epoch
			&& entry->waiters[i].request_id == id->request_id) {
			if (out_skip_count != NULL)
				*out_skip_count = entry->waiters[i].skip_count;
			if (out_boosted != NULL)
				*out_boosted = entry->waiters[i].boosted;
			if (out_fair_queue_seq != NULL)
				*out_fair_queue_seq = entry->waiters[i].fair_queue_seq;
			found = true;
			break;
		}
	}
	SpinLockRelease(&entry->lock);
	cluster_grd_entry_release(entry);
	return found;
}

/*
 * spec-5.5 D5 — shared implementation for the blocking (enqueue_or_grant) and
 * conditional (grant_conditional) grant paths.  Keeping ONE conflict scan +
 * self-exclusion + no-conflict grant body means the try-lock path can never
 * diverge from the blocking path's hard-won correctness (spec-5.1c D5 self-
 * conflict exclusion, L364).  The only difference is at the conflict branch:
 * conditional callers get CLUSTER_GRD_CONFLICT_NOWAIT (entry untouched) instead
 * of an enqueued waiter.
 */
static ClusterGrdGrantAction
cluster_grd_entry_enqueue_or_grant_impl(const ClusterResId *resid, const ClusterGrdHolderId *holder,
										int32 source_node_id, uint64 request_id,
										uint64 shard_master_generation, uint32 request_opcode,
										int lockmode,
										ClusterGrdConflictHolder *conflict_holders_out,
										int *n_conflict_out, ClusterGrdWaiterMeta meta,
										bool conditional)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult lookup_result;
	int n_conflict = 0;
	int slot;

	Assert(resid != NULL && holder != NULL);

	lookup_result = cluster_grd_entry_lookup_or_create(resid, true, &entry);
	if (lookup_result == CLUSTER_GRD_ENTRY_NOT_READY)
		return CLUSTER_GRD_NOT_READY;
	if (lookup_result != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return CLUSTER_GRD_NOT_READY;

	SpinLockAcquire(&entry->lock);

	/*
	 * (1) Conflict scan via PG exported DoLockModesConflict.  Snapshot
	 *	  conflicting holders into the caller-provided buffer so the LMS
	 *	  can later fan out a targeted BAST (HC18).
	 */
	for (int i = 0; i < entry->ngranted; i++) {
		/*
		 * spec-5.1b D1: frozen-matrix conflict check.  Contract (spec-5.1a
		 * §2.2): ges_modes_compatible(held, wanted) == !DoLockModesConflict(
		 * wanted, held);  matrix is symmetric (5.1a U3) so the canonical
		 * (held, wanted) argument order is behaviourally identical.
		 */
		if (ges_modes_compatible(entry->holders[i].mode, (LOCKMODE)lockmode))
			continue;
		/*
		 * spec-5.1c D5: same-backend self-conflict exclusion.  Under PG's
		 * additive lock model a backend holding this resid in one mode may
		 * request a second, conflicting mode (a different LOCALLOCK that
		 * re-enters the cluster gate, e.g. xact-advisory share then
		 * exclusive).  The requester is not a conflict against its own prior
		 * hold; without this the master would enqueue/BAST the request behind
		 * the requester's own holder slot -> cross-node self-deadlock (the
		 * holder waits on itself).  Identity is {node_id, procno}: the full
		 * 4-tuple would carry a fresh request_id for the additive re-acquire
		 * and never match.  (A real cross-node convert -- same backend, mode
		 * change of the SAME hold -- is the spec-5.2 path and self-excludes
		 * in cluster_grd_entry_request_convert.)
		 */
		if ((uint32)entry->holders[i].node_id == holder->node_id
			&& entry->holders[i].procno == holder->procno)
			continue;
		if (conflict_holders_out != NULL && n_conflict < PGRAC_GRD_MAX_HOLDERS) {
			conflict_holders_out[n_conflict].holder.node_id = entry->holders[i].node_id;
			conflict_holders_out[n_conflict].holder.procno = entry->holders[i].procno;
			conflict_holders_out[n_conflict].holder.cluster_epoch = entry->holders[i].cluster_epoch;
			conflict_holders_out[n_conflict].holder.request_id = entry->holders[i].request_id;
			conflict_holders_out[n_conflict].source_node_id = entry->holders[i].node_id;
			conflict_holders_out[n_conflict].held_mode = entry->holders[i].mode;
		}
		n_conflict++;
	}
	if (n_conflict_out != NULL)
		*n_conflict_out = n_conflict < PGRAC_GRD_MAX_HOLDERS ? n_conflict : PGRAC_GRD_MAX_HOLDERS;

	/*
	 * (2) No conflict → grant immediately and bump generation.
	 */
	if (n_conflict == 0) {
		if (entry->ngranted >= PGRAC_GRD_MAX_HOLDERS) {
			SpinLockRelease(&entry->lock);
			cluster_grd_entry_release(entry);
			cluster_grd_inc_ges_work_queue_full();
			return CLUSTER_GRD_WAIT_QUEUE_FULL;
		}
		/*
		 * spec-5.10 D2 — scan-on-grant.  This holder-compatible request is about
		 * to be granted and thereby "jumps" every already-queued waiter whose
		 * wanted mode conflicts with it;  charge each such earlier waiter one
		 * skip (counting only — the grant is never blocked here, Rule 8.A).
		 * Gated on the cluster-global toggle: when starvation protection is off
		 * the grant path is the unmodified spec-5.1b fast path (Q9 / P1#1).
		 */
		if (cluster_grd_starvation_protection_enabled()) {
			int wi;

			grd_starvation_account_skip_on_grant(entry, (LOCKMODE)lockmode);

			/*
			 * spec-5.10 D2/D5 — grant barrier.  If an EARLIER boosted waiter
			 * conflicts with this would-be-granted request, hold the request
			 * behind it (the FAIRNESS_BARRIER) instead of letting it jump.  A
			 * request that conflicts with a HOLDER never reaches here, so the
			 * conflict matrix is never bypassed (Rule 8.A — no false grant).
			 */
			wi = grd_find_earliest_boosted_conflicting_waiter(entry, (LOCKMODE)lockmode, 0);
			if (wi >= 0) {
				ClusterLmdVertex w_vertex, r_vertex;
				bool published;

				/* NOWAIT jumper cannot enqueue -> try-lock fails (P0#1-r3). */
				if (conditional) {
					SpinLockRelease(&entry->lock);
					cluster_grd_entry_release(entry);
					return CLUSTER_GRD_CONFLICT_NOWAIT;
				}

				/*
				 * Two-phase FAIRNESS_BARRIER edge publish (P0#2-r3 lock order):
				 * capture the requester's vertex + the boosted waiter's
				 * spec-5.8-owned canonical vertex (P0#3 — never fabricated)
				 * under entry->lock, release the spinlock (the LMD graph is
				 * LWLock-protected and must never be touched under a spinlock),
				 * publish R -> W, then re-acquire + revalidate.  The edge is
				 * visible BEFORE R blocks (edge-visible-before-block, R2); the
				 * pre-enqueue edge is tentative and only feeds the spec-5.8
				 * two-round confirm (P1#2).
				 */
				grd_capture_waiter_vertex(entry, wi, &w_vertex);
				grd_wfg_make_vertex((int32)holder->node_id, holder->procno, holder->cluster_epoch,
									request_id, meta.xid, meta.wait_seq, &r_vertex);
				SpinLockRelease(&entry->lock);

				published = cluster_lmd_submit_wait_edge_real(&r_vertex, &w_vertex, request_id);

				SpinLockAcquire(&entry->lock);
				/*
				 * Revalidate after the gap.  Block only if the edge published,
				 * the requester still does not conflict with any holder, and an
				 * earlier boosted conflicting waiter still exists.  Otherwise
				 * retract the tentative edge and fall back to the spec-5.1b
				 * decision (Rule 8.A — fail closed to the safe path).
				 */
				if (published
					&& !grd_scan_holder_conflict(entry, holder->node_id, holder->procno,
												 (LOCKMODE)lockmode)
					&& grd_find_earliest_boosted_conflicting_waiter(entry, (LOCKMODE)lockmode, 0)
						   >= 0) {
					if (grd_enqueue_waiter_locked(entry, holder, source_node_id, request_id,
												  shard_master_generation, request_opcode,
												  (LOCKMODE)lockmode, meta)
						< 0) {
						SpinLockRelease(&entry->lock);
						cluster_lmd_cancel_wait_edge_real(&r_vertex);
						cluster_grd_entry_release(entry);
						cluster_grd_inc_ges_work_queue_full();
						return CLUSTER_GRD_WAIT_QUEUE_FULL;
					}
					pg_atomic_fetch_add_u64(&cluster_grd_state->starvation_barrier_enqueued_count,
											1);
					SpinLockRelease(&entry->lock);
					cluster_grd_entry_release(entry);
					return CLUSTER_GRD_ENQUEUED_WAITER;
				}

				/* Barrier withdrawn (publish/revalidate failed): retract the
				 * tentative edge outside the spinlock, then re-decide against
				 * the CURRENT holders (never false-grant). */
				SpinLockRelease(&entry->lock);
				if (published)
					cluster_lmd_cancel_wait_edge_real(&r_vertex);
				else
					pg_atomic_fetch_add_u64(
						&cluster_grd_state->starvation_barrier_publish_fail_count, 1);
				SpinLockAcquire(&entry->lock);
				if (grd_scan_holder_conflict(entry, holder->node_id, holder->procno,
											 (LOCKMODE)lockmode)) {
					if (conditional) {
						SpinLockRelease(&entry->lock);
						cluster_grd_entry_release(entry);
						return CLUSTER_GRD_CONFLICT_NOWAIT;
					}
					if (grd_enqueue_waiter_locked(entry, holder, source_node_id, request_id,
												  shard_master_generation, request_opcode,
												  (LOCKMODE)lockmode, meta)
						< 0) {
						SpinLockRelease(&entry->lock);
						cluster_grd_entry_release(entry);
						cluster_grd_inc_ges_work_queue_full();
						return CLUSTER_GRD_WAIT_QUEUE_FULL;
					}
					SpinLockRelease(&entry->lock);
					cluster_grd_entry_release(entry);
					return CLUSTER_GRD_ENQUEUED_WAITER;
				}
				/* No conflict remains -> fall through and grant below. */
			}
		}
		/* Re-check the holder cap (the barrier path may have released the
		 * spinlock); never overflow holders[] (Rule 8.A). */
		if (entry->ngranted >= PGRAC_GRD_MAX_HOLDERS) {
			SpinLockRelease(&entry->lock);
			cluster_grd_entry_release(entry);
			cluster_grd_inc_ges_work_queue_full();
			return CLUSTER_GRD_WAIT_QUEUE_FULL;
		}
		slot = entry->ngranted++;
		entry->holders[slot].node_id = (int32)holder->node_id;
		entry->holders[slot].procno = holder->procno;
		entry->holders[slot].cluster_epoch = holder->cluster_epoch;
		entry->holders[slot].request_id = holder->request_id;
		entry->holders[slot].mode = (LOCKMODE)lockmode;
		entry->generation++;
		SpinLockRelease(&entry->lock);
		cluster_grd_entry_release(entry);
		return CLUSTER_GRD_GRANT_NOW;
	}

	/*
	 * spec-5.5 D5 — conditional (NOWAIT) path: a conflict means the try-lock
	 * fails immediately.  Do NOT enqueue a waiter and do NOT touch the entry;
	 * the caller rejects with LOCK_CONFLICT and sends no BAST (T5).  The
	 * conflict_holders_out snapshot was populated by step (1) above but the
	 * conditional caller does not consume it.
	 */
	if (conditional) {
		SpinLockRelease(&entry->lock);
		cluster_grd_entry_release(entry);
		return CLUSTER_GRD_CONFLICT_NOWAIT;
	}

	/*
	 * (3) Conflict → enqueue waiter with full reply identity (HC17/HC19).
	 *	  HC12 family 53R71 fail-closed when waiter slot exhausted.
	 */
	if (grd_enqueue_waiter_locked(entry, holder, source_node_id, request_id,
								  shard_master_generation, request_opcode, (LOCKMODE)lockmode, meta)
		< 0) {
		SpinLockRelease(&entry->lock);
		cluster_grd_entry_release(entry);
		cluster_grd_inc_ges_work_queue_full();
		return CLUSTER_GRD_WAIT_QUEUE_FULL;
	}

	SpinLockRelease(&entry->lock);
	cluster_grd_entry_release(entry);
	return CLUSTER_GRD_ENQUEUED_WAITER;
}

/*
 * spec-5.5 D5 — blocking grant-or-enqueue (the original public entry point).
 */
ClusterGrdGrantAction
cluster_grd_entry_enqueue_or_grant(const ClusterResId *resid, const ClusterGrdHolderId *holder,
								   int32 source_node_id, uint64 request_id,
								   uint64 shard_master_generation, uint32 request_opcode,
								   int lockmode, ClusterGrdConflictHolder *conflict_holders_out,
								   int *n_conflict_out)
{
	/* spec-5.8 D1c/D1e — plain entry forwards with a zero waiter meta. */
	ClusterGrdWaiterMeta meta = { InvalidTransactionId, 0 };

	return cluster_grd_entry_enqueue_or_grant_meta(resid, holder, source_node_id, request_id, meta,
												   shard_master_generation, request_opcode,
												   lockmode, conflict_holders_out, n_conflict_out);
}

/*
 * spec-5.5 D5 — conditional (NOWAIT) grant for try-locks:  grant if no conflict,
 * else CLUSTER_GRD_CONFLICT_NOWAIT (no waiter enqueued).
 */
ClusterGrdGrantAction
cluster_grd_entry_grant_conditional(const ClusterResId *resid, const ClusterGrdHolderId *holder,
									int32 source_node_id, uint64 request_id,
									uint64 shard_master_generation, uint32 request_opcode,
									int lockmode, ClusterGrdConflictHolder *conflict_holders_out,
									int *n_conflict_out)
{
	/* spec-5.8 D1c/D1e — plain entry forwards with a zero waiter meta. */
	ClusterGrdWaiterMeta meta = { InvalidTransactionId, 0 };

	return cluster_grd_entry_grant_conditional_meta(resid, holder, source_node_id, request_id, meta,
													shard_master_generation, request_opcode,
													lockmode, conflict_holders_out, n_conflict_out);
}

/*
 * spec-5.8 D1c/D1e — waiter-metadata variants.  Stamp the enqueued waiter's
 * xid + wait_seq onto its master-side WFG vertex (via enqueue_or_grant_impl)
 * for TX-edge resolution + D5 ABA revalidate; the resync then (re)builds the
 * WFG edges off the released lock.
 */
ClusterGrdGrantAction
cluster_grd_entry_enqueue_or_grant_meta(const ClusterResId *resid, const ClusterGrdHolderId *holder,
										int32 source_node_id, uint64 request_id,
										ClusterGrdWaiterMeta meta, uint64 shard_master_generation,
										uint32 request_opcode, int lockmode,
										ClusterGrdConflictHolder *conflict_holders_out,
										int *n_conflict_out)
{
	ClusterGrdGrantAction act = cluster_grd_entry_enqueue_or_grant_impl(
		resid, holder, source_node_id, request_id, shard_master_generation, request_opcode,
		lockmode, conflict_holders_out, n_conflict_out, meta, /* conditional */ false);

	/* spec-5.8 D1b — register/refresh master-side WFG edges (lock released). */
	grd_wfg_resync_entry(resid, NULL, 0);
	return act;
}

ClusterGrdGrantAction
cluster_grd_entry_grant_conditional_meta(const ClusterResId *resid,
										 const ClusterGrdHolderId *holder, int32 source_node_id,
										 uint64 request_id, ClusterGrdWaiterMeta meta,
										 uint64 shard_master_generation, uint32 request_opcode,
										 int lockmode,
										 ClusterGrdConflictHolder *conflict_holders_out,
										 int *n_conflict_out)
{
	ClusterGrdGrantAction act = cluster_grd_entry_enqueue_or_grant_impl(
		resid, holder, source_node_id, request_id, shard_master_generation, request_opcode,
		lockmode, conflict_holders_out, n_conflict_out, meta, /* conditional */ true);

	/* spec-5.8 D1b — a NOWAIT grant may add a holder; refresh queued waiters. */
	grd_wfg_resync_entry(resid, NULL, 0);
	return act;
}

int
cluster_grd_entry_release_and_pop_compatible_waiter(const ClusterResId *resid,
													const ClusterGrdHolderId *holder,
													ClusterGrdWaiterIdentity *granted_out,
													int max_out)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult lookup_result;
	int found_holder = -1;
	int popped = 0;

	Assert(resid != NULL && holder != NULL);
	Assert(granted_out != NULL && max_out > 0);

	lookup_result = cluster_grd_entry_lookup_or_create(resid, false, &entry);
	if (lookup_result != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return 0;

	SpinLockAcquire(&entry->lock);

	/* (1) Locate the holder slot by full 4-tuple match. */
	for (int i = 0; i < entry->ngranted; i++) {
		if ((uint32)entry->holders[i].node_id == holder->node_id
			&& entry->holders[i].procno == holder->procno
			&& entry->holders[i].cluster_epoch == holder->cluster_epoch
			&& entry->holders[i].request_id == holder->request_id) {
			found_holder = i;
			break;
		}
	}
	if (found_holder < 0) {
		SpinLockRelease(&entry->lock);
		cluster_grd_entry_release(entry);
		return 0;
	}

	/* Compact holders[] down (preserve relative order for the surviving slots). */
	if (found_holder < entry->ngranted - 1)
		entry->holders[found_holder] = entry->holders[entry->ngranted - 1];
	memset(&entry->holders[entry->ngranted - 1], 0, sizeof(ClusterGrdHolder));
	entry->ngranted--;
	entry->generation++;

	/*
	 * (2) Scan FIFO waiters; pop the first whose mode is compatible with
	 *	  every surviving holder.  Promote to holders[].
	 *
	 * spec-4.6 P0#3 window guard:  a waiter queued under an OLD epoch
	 * must never be promoted — its GRANT reply would echo the stale
	 * holder tuple and be rejected by the requester's inbound
	 * validation, leaving a zombie grant nobody owns.  Drop such
	 * waiters here (the requester self-retries under the current
	 * epoch);  count as stale_request_drop.
	 */
	while (popped < max_out && entry->nwaiters > 0) {
		int chosen = -1;
		uint64 cur_epoch = cluster_epoch_get_current();

		for (int w = 0; w < entry->nwaiters;) {
			if (entry->waiters[w].cluster_epoch < cur_epoch) {
				if (w < entry->nwaiters - 1)
					entry->waiters[w] = entry->waiters[entry->nwaiters - 1];
				memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(entry->waiters[0]));
				entry->nwaiters--;
				pg_atomic_fetch_add_u64(&cluster_grd_state->stale_request_drop_count, 1);
				continue;
			}
			w++;
		}

		for (int w = 0; w < entry->nwaiters; w++) {
			bool compatible = true;

			for (int h = 0; h < entry->ngranted; h++) {
				/* spec-5.1b D1: frozen-matrix conflict check. */
				if (!ges_modes_compatible(entry->holders[h].mode, entry->waiters[w].mode)) {
					compatible = false;
					break;
				}
			}
			if (compatible) {
				chosen = w;
				break;
			}
		}
		if (chosen < 0)
			break;

		/* Capture identity for the caller's GES_REPLY send. */
		granted_out[popped].holder.node_id = (uint32)entry->waiters[chosen].node_id;
		granted_out[popped].holder.procno = entry->waiters[chosen].procno;
		granted_out[popped].holder.cluster_epoch = entry->waiters[chosen].cluster_epoch;
		granted_out[popped].holder.request_id = entry->waiters[chosen].request_id;
		granted_out[popped].source_node_id = entry->waiters[chosen].source_node_id;
		granted_out[popped].request_id = entry->waiters[chosen].request_id;
		granted_out[popped].shard_master_generation
			= entry->waiters[chosen].shard_master_generation;
		granted_out[popped].request_opcode = entry->waiters[chosen].request_opcode;
		granted_out[popped].mode = entry->waiters[chosen].mode;

		/* Promote waiter to holder. */
		if (entry->ngranted < PGRAC_GRD_MAX_HOLDERS) {
			int hslot = entry->ngranted++;

			entry->holders[hslot].node_id = entry->waiters[chosen].node_id;
			entry->holders[hslot].procno = entry->waiters[chosen].procno;
			entry->holders[hslot].cluster_epoch = entry->waiters[chosen].cluster_epoch;
			entry->holders[hslot].request_id = entry->waiters[chosen].request_id;
			entry->holders[hslot].mode = entry->waiters[chosen].mode;
		}
		/* Compact waiters[]. */
		if (chosen < entry->nwaiters - 1)
			entry->waiters[chosen] = entry->waiters[entry->nwaiters - 1];
		memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(ClusterGrdWaiter));
		entry->nwaiters--;
		entry->generation++;
		popped++;
	}

	SpinLockRelease(&entry->lock);
	cluster_grd_entry_release(entry);
	/* spec-5.8 D1b — popped waiters left the queue; refresh the rest. */
	grd_wfg_resync_after_pops(resid, granted_out, popped);
	return popped;
}


/* ============================================================
 * spec-2.21 D5 — inspection + reservation helpers.
 * ============================================================ */

bool
cluster_grd_entry_has_remote_holder(ClusterGrdEntry *entry, int32 self_node_id)
{
	int i;

	Assert(entry != NULL);
	for (i = 0; i < entry->ngranted; i++)
		if (entry->holders[i].node_id != self_node_id)
			return true;
	return false;
}

bool
cluster_grd_entry_has_pending_waiter(ClusterGrdEntry *entry)
{
	Assert(entry != NULL);
	return entry->nwaiters > 0;
}

bool
cluster_grd_entry_has_pending_convert(ClusterGrdEntry *entry)
{
	Assert(entry != NULL);
	return entry->nconverts > 0;
}

uint64
cluster_grd_entry_generation(ClusterGrdEntry *entry)
{
	Assert(entry != NULL);
	return entry->generation;
}


/* ============================================================
 * spec-5.1b — GES lock-conversion (convert) state machine.
 *
 *	D4 partial-order decision + D5 convert-priority drain + anti-
 *	starvation + D6 fail-closed invariants.  All raw mutators (caller
 *	holds entry->lock, mirroring grant_holder).  LOGIC-only in spec-5.1b:
 *	the live cross-node producer (opcode-2) is an explicit REJECT (D3);
 *	the real backend convert trigger + wire/identity land in spec-5.2.
 *
 *	Holder identity for the convert locator is (node_id, procno,
 *	current_mode) — the REDECLARE convention — and the convert's own slot
 *	is self-excluded from the UPGRADE conflict scan, so a holder already
 *	in a self-conflicting mode (SUE/SRE/E/AE) can still upgrade.
 * ============================================================ */

/* Locate the holder being converted by the (node,procno,mode) locator. */
static int
grd_find_holder_slot(ClusterGrdEntry *entry, int32 node_id, uint32 procno, LOCKMODE mode)
{
	for (int i = 0; i < entry->ngranted; i++) {
		if (entry->holders[i].node_id == node_id && entry->holders[i].procno == procno
			&& entry->holders[i].mode == mode)
			return i;
	}
	return -1;
}

ClusterGrdConvertResult
cluster_grd_entry_request_convert(ClusterGrdEntry *entry, const ClusterGrdConvert *req,
								  bool *out_drain_hint)
{
	int hslot;
	ClusterGesConvertClass klass;

	Assert(entry != NULL && req != NULL);
	Assert(cluster_grd_state != NULL);

	if (out_drain_hint != NULL)
		*out_drain_hint = false;

	hslot = grd_find_holder_slot(entry, req->node_id, req->procno, req->current_mode);
	if (hslot < 0) {
		/*
		 * The requester claims to hold (node,procno,current_mode) but the
		 * GRD has no such holder — fail closed (53R74 mapping forward 5.2),
		 * never fabricate a grant.
		 */
		pg_atomic_fetch_add_u64(&cluster_grd_state->convert_illegal_count, 1);
		return CLUSTER_GRD_CONVERT_ILLEGAL;
	}

	klass = ges_mode_convert_class(entry->holders[hslot].mode, req->requested_mode);
	switch (klass) {
	case GES_CONVERT_SAME:
		/* idempotent no-op. */
		pg_atomic_fetch_add_u64(&cluster_grd_state->convert_granted_inplace_count, 1);
		return CLUSTER_GRD_CONVERT_GRANTED_INPLACE;

	case GES_CONVERT_DOWNGRADE:
		/* compat-set widens → always grantable in place; signal drain so
		 * the caller can re-evaluate blocked converts/waiters. */
		entry->holders[hslot].mode = req->requested_mode;
		entry->generation++;
		if (out_drain_hint != NULL)
			*out_drain_hint = true;
		pg_atomic_fetch_add_u64(&cluster_grd_state->convert_granted_inplace_count, 1);
		return CLUSTER_GRD_CONVERT_GRANTED_INPLACE;

	case GES_CONVERT_UPGRADE:
		/*
		 * requested_mode must be compatible with every OTHER holder
		 * (self-excluded by slot) before we may mutate in place; otherwise
		 * enqueue with strict priority over new waiters (anti-starvation).
		 */
		for (int i = 0; i < entry->ngranted; i++) {
			if (i == hslot)
				continue;
			if (!ges_modes_compatible(entry->holders[i].mode, req->requested_mode)) {
				if (entry->nconverts >= PGRAC_GRD_MAX_CONVERTS) {
					pg_atomic_fetch_add_u64(&cluster_grd_state->converts_full_count, 1);
					return CLUSTER_GRD_CONVERT_QUEUE_FULL;
				}
				entry->converts[entry->nconverts++] = *req;
				entry->generation++;
				pg_atomic_fetch_add_u64(&cluster_grd_state->convert_enqueued_count, 1);
				return CLUSTER_GRD_CONVERT_ENQUEUED;
			}
		}
		entry->holders[hslot].mode = req->requested_mode;
		/*
		 * PGRAC: spec-5.3 §3.1a release-ownership — rebind the holder slot's
		 * request_id to the convert's own reply key (R_new = convert_request_
		 * id).  The requester then makes its new (stronger-mode) LOCALLOCK the
		 * sole cluster owner and de-registers the old weaker LOCALLOCK locally,
		 * so the eventual release matches this slot by R_new exactly once
		 * (no holder leak / no early strong-lock release).
		 */
		entry->holders[hslot].request_id = req->convert_request_id;
		entry->generation++;
		pg_atomic_fetch_add_u64(&cluster_grd_state->convert_granted_inplace_count, 1);
		/*
		 * spec-5.10 D3 — convert-side scan-on-grant: granting this convert in
		 * place raised a holder to requested_mode, "jumping" every queued waiter
		 * that conflicts with the new mode;  charge each a skip (and boost at the
		 * threshold) so a convert flood cannot silently starve a plain waiter.
		 */
		if (cluster_grd_starvation_protection_enabled())
			grd_starvation_account_skip_on_grant(entry, req->requested_mode);
		return CLUSTER_GRD_CONVERT_GRANTED_INPLACE;

	case GES_CONVERT_LATERAL:
	default:
		/* incomparable modes are not a conversion (two distinct locks); a
		 * caller wanting both must take a fresh REQUEST. */
		pg_atomic_fetch_add_u64(&cluster_grd_state->convert_illegal_count, 1);
		return CLUSTER_GRD_CONVERT_ILLEGAL;
	}
}

/* Remove convert slot c (swap-with-last compaction). */
static void
grd_convert_remove(ClusterGrdEntry *entry, int c)
{
	if (c < entry->nconverts - 1)
		entry->converts[c] = entry->converts[entry->nconverts - 1];
	memset(&entry->converts[entry->nconverts - 1], 0, sizeof(ClusterGrdConvert));
	entry->nconverts--;
	entry->generation++;
}

int
cluster_grd_entry_drain_converts_then_waiters(ClusterGrdEntry *entry,
											  ClusterGrdGrantIdentity *granted_out, int max_out)
{
	int n = 0;
	bool served_waiter = false;

	Assert(entry != NULL && granted_out != NULL && max_out > 0);
	Assert(cluster_grd_state != NULL);

	/*
	 * spec-5.10 D3 — convert capping (narrowed, P1c-r2).  Before granting any
	 * convert in place, if a BOOSTED waiter is already compatible with every
	 * holder and is held back ONLY by the convert priority (its mode conflicts
	 * with a pending convert's target), serve it FIRST so a convert flood cannot
	 * starve it.  This consumes the drain's single waiter slot (Phase 2 below is
	 * then skipped), so the granted_out budget is unchanged.  A boosted waiter
	 * still blocked by a HOLDER is NOT un-capped (the convert is not paused,
	 * U13); convert partial-order correctness (5.1b) is untouched — the
	 * conflicting convert simply stays queued one more round.
	 */
	if (n < max_out && cluster_grd_starvation_protection_enabled() && entry->nconverts > 0) {
		int chosen = -1;

		for (int w = 0; w < entry->nwaiters; w++) {
			bool holder_ok = true;
			bool convert_blocked = false;

			if (!entry->waiters[w].boosted)
				continue;
			for (int h = 0; h < entry->ngranted; h++) {
				if (!ges_modes_compatible(entry->holders[h].mode, entry->waiters[w].mode)) {
					holder_ok = false;
					break;
				}
			}
			if (!holder_ok)
				continue; /* still blocked by a holder -> do NOT pause the convert */
			if (grd_waiter_is_barriered(entry, w))
				continue; /* held behind an earlier boosted waiter */
			for (int cc = 0; cc < entry->nconverts; cc++) {
				if (!ges_modes_compatible(entry->converts[cc].requested_mode,
										  entry->waiters[w].mode)) {
					convert_blocked = true;
					break;
				}
			}
			if (!convert_blocked)
				continue; /* not held by a convert -> Phase 2 serves it normally */
			if (chosen < 0
				|| grd_fair_seq_precedes(entry->waiters[w].fair_queue_seq,
										 entry->waiters[chosen].fair_queue_seq))
				chosen = w;
		}

		if (chosen >= 0) {
			int w = chosen;

			granted_out[n].holder.node_id = (uint32)entry->waiters[w].node_id;
			granted_out[n].holder.procno = entry->waiters[w].procno;
			granted_out[n].holder.cluster_epoch = entry->waiters[w].cluster_epoch;
			granted_out[n].holder.request_id = entry->waiters[w].request_id;
			granted_out[n].source_node_id = entry->waiters[w].source_node_id;
			granted_out[n].request_opcode = entry->waiters[w].request_opcode;
			granted_out[n].shard_master_generation = entry->waiters[w].shard_master_generation;
			granted_out[n].mode = entry->waiters[w].mode;
			n++;
			served_waiter = true;

			if (entry->ngranted < PGRAC_GRD_MAX_HOLDERS) {
				int hs = entry->ngranted++;

				entry->holders[hs].node_id = entry->waiters[w].node_id;
				entry->holders[hs].procno = entry->waiters[w].procno;
				entry->holders[hs].cluster_epoch = entry->waiters[w].cluster_epoch;
				entry->holders[hs].request_id = entry->waiters[w].request_id;
				entry->holders[hs].mode = entry->waiters[w].mode;
			}
			if (w < entry->nwaiters - 1)
				entry->waiters[w] = entry->waiters[entry->nwaiters - 1];
			memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(ClusterGrdWaiter));
			entry->nwaiters--;
			entry->generation++;
		}
	}

	/*
	 * Phase 1 — grant every pending convert now compatible with the other
	 * holders (self-excluded), in place.  Granting a convert changes a
	 * holder mode, which may unblock an earlier-skipped convert, so we
	 * restart the scan after each grant.  Each grant removes one queue
	 * entry, so the loop is bounded.
	 */
	for (int c = 0; c < entry->nconverts && n < max_out;) {
		ClusterGrdConvert *cv = &entry->converts[c];
		int hslot = grd_find_holder_slot(entry, cv->node_id, cv->procno, cv->current_mode);
		bool compatible = true;

		if (hslot < 0) {
			/* the holder being converted vanished (released / swept) — drop
			 * the orphaned convert rather than block the queue forever. */
			grd_convert_remove(entry, c);
			continue; /* re-check slot c (now the swapped-in entry) */
		}
		for (int i = 0; i < entry->ngranted; i++) {
			if (i == hslot)
				continue;
			if (!ges_modes_compatible(entry->holders[i].mode, cv->requested_mode)) {
				compatible = false;
				break;
			}
		}
		if (!compatible) {
			c++; /* still blocked — leave queued, try the next */
			continue;
		}

		entry->holders[hslot].mode = cv->requested_mode;
		/* PGRAC: spec-5.3 §3.1a — rebind the granted holder slot to the
		 * convert's reply key (R_new), mirroring the in-place UPGRADE path. */
		entry->holders[hslot].request_id = cv->convert_request_id;
		granted_out[n].holder.node_id = (uint32)cv->node_id;
		granted_out[n].holder.procno = cv->procno;
		granted_out[n].holder.cluster_epoch = cv->cluster_epoch;
		granted_out[n].holder.request_id = cv->convert_request_id;
		granted_out[n].source_node_id = cv->source_node_id;
		granted_out[n].request_opcode = GES_REQ_OPCODE_CONVERT;
		granted_out[n].shard_master_generation = cv->shard_master_generation;
		granted_out[n].mode = cv->requested_mode;
		n++;
		pg_atomic_fetch_add_u64(&cluster_grd_state->convert_granted_inplace_count, 1);
		/* spec-5.10 D3 — convert-side scan-on-grant: the in-place grant raised a
		 * holder to requested_mode, jumping every conflicting waiter. */
		if (cluster_grd_starvation_protection_enabled())
			grd_starvation_account_skip_on_grant(entry, cv->requested_mode);
		grd_convert_remove(entry, c);
		c = 0; /* a holder mode changed — re-scan from the front */
	}

	/*
	 * Phase 2 — pop ONE FIFO REQUEST waiter compatible with every holder
	 * AND every still-pending convert target (anti-starvation: never grant
	 * a waiter that would block a queued convert).
	 */
	if (n < max_out && !served_waiter) {
		int chosen = -1;

		/*
		 * spec-5.10 D4 (P1a) — choose the waiter to promote by MIN fair_queue_seq
		 * among those compatible with every holder + pending convert AND not held
		 * behind an earlier boosted conflicting waiter (serve-side barrier).
		 * Array order is unreliable after swap-remove, so an explicit min-seq scan
		 * replaces the spec-5.1b "first compatible" pick; with starvation
		 * protection off grd_waiter_is_barriered() is always false and the choice
		 * degrades to the lowest-seq compatible waiter.
		 */
		for (int w = 0; w < entry->nwaiters; w++) {
			bool ok = true;

			for (int h = 0; h < entry->ngranted; h++) {
				if (!ges_modes_compatible(entry->holders[h].mode, entry->waiters[w].mode)) {
					ok = false;
					break;
				}
			}
			for (int cc = 0; ok && cc < entry->nconverts; cc++) {
				if (!ges_modes_compatible(entry->converts[cc].requested_mode,
										  entry->waiters[w].mode)) {
					ok = false;
					break;
				}
			}
			if (!ok)
				continue;
			if (grd_waiter_is_barriered(entry, w))
				continue; /* spec-5.10 — held behind a boosted conflicting waiter */
			if (chosen < 0
				|| grd_fair_seq_precedes(entry->waiters[w].fair_queue_seq,
										 entry->waiters[chosen].fair_queue_seq))
				chosen = w;
		}

		if (chosen >= 0) {
			int w = chosen;

			granted_out[n].holder.node_id = (uint32)entry->waiters[w].node_id;
			granted_out[n].holder.procno = entry->waiters[w].procno;
			granted_out[n].holder.cluster_epoch = entry->waiters[w].cluster_epoch;
			granted_out[n].holder.request_id = entry->waiters[w].request_id;
			granted_out[n].source_node_id = entry->waiters[w].source_node_id;
			granted_out[n].request_opcode = entry->waiters[w].request_opcode;
			granted_out[n].shard_master_generation = entry->waiters[w].shard_master_generation;
			granted_out[n].mode = entry->waiters[w].mode;
			n++;

			if (entry->ngranted < PGRAC_GRD_MAX_HOLDERS) {
				int hs = entry->ngranted++;

				entry->holders[hs].node_id = entry->waiters[w].node_id;
				entry->holders[hs].procno = entry->waiters[w].procno;
				entry->holders[hs].cluster_epoch = entry->waiters[w].cluster_epoch;
				entry->holders[hs].request_id = entry->waiters[w].request_id;
				entry->holders[hs].mode = entry->waiters[w].mode;
			}
			if (w < entry->nwaiters - 1)
				entry->waiters[w] = entry->waiters[entry->nwaiters - 1];
			memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(ClusterGrdWaiter));
			entry->nwaiters--;
			entry->generation++;
		}
	}
	return n;
}

/*
 * cluster_grd_entry_bast_consume -- advance the pending convert queue then a
 * waiter after a BAST-induced holder release/downgrade (spec-5.1c D3; caller
 * holds entry->lock).
 *
 *	Thin named seam over cluster_grd_entry_drain_converts_then_waiters so the
 *	first live convert producer (spec-5.2) has a single BAST-side entry point
 *	to call.  Until spec-5.2 the convert queue is never populated in
 *	production (cross-node CONVERT is rejected FEATURE_NOT_SUPPORTED), so this
 *	is LOGIC exercised by cluster_unit only.  The live multi-grant release
 *	path keeps the spec-5.1b release_and_pop(granted[1]) signature unchanged.
 *
 *	released_holder identifies the holder that gave way (informational: the
 *	drain re-evaluates the full holder set against each pending convert /
 *	waiter; spec-5.2 may use it to scope the re-evaluation).  Returns the
 *	number of identities written to granted_out (<= max_out; buffer should
 *	hold PGRAC_GRD_MAX_CONVERTS_PUBLIC + 1).
 */
int
cluster_grd_entry_bast_consume(ClusterGrdEntry *entry, const ClusterGrdHolderId *released_holder,
							   ClusterGrdGrantIdentity *granted_out, int max_out)
{
	(void)released_holder; /* informational; consumed by spec-5.2 */

	if (entry == NULL || granted_out == NULL || max_out <= 0)
		return 0;
	return cluster_grd_entry_drain_converts_then_waiters(entry, granted_out, max_out);
}

bool
cluster_grd_entry_request_blocked_by_pending_convert(ClusterGrdEntry *entry, int wanted_mode)
{
	Assert(entry != NULL);
	for (int c = 0; c < entry->nconverts; c++) {
		if (!ges_modes_compatible(entry->converts[c].requested_mode, (LOCKMODE)wanted_mode))
			return true;
	}
	return false;
}

int
cluster_grd_entry_ngranted(ClusterGrdEntry *entry)
{
	Assert(entry != NULL);
	return entry->ngranted;
}

int
cluster_grd_entry_nconverts(ClusterGrdEntry *entry)
{
	Assert(entry != NULL);
	return entry->nconverts;
}

bool
cluster_grd_entry_holder_mode(ClusterGrdEntry *entry, int32 node_id, uint32 procno,
							  LOCKMODE *out_mode)
{
	Assert(entry != NULL);
	for (int i = 0; i < entry->ngranted; i++) {
		if (entry->holders[i].node_id == node_id && entry->holders[i].procno == procno) {
			if (out_mode != NULL)
				*out_mode = entry->holders[i].mode;
			return true;
		}
	}
	return false;
}

uint64
cluster_grd_convert_granted_inplace_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->convert_granted_inplace_count);
}

uint64
cluster_grd_convert_enqueued_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->convert_enqueued_count);
}

uint64
cluster_grd_convert_illegal_count(void)
{
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->convert_illegal_count);
}

uint64
cluster_grd_convert_queue_full_count(void)
{
	/* convert queue-full reuses the existing converts_full_count (D9). */
	Assert(cluster_grd_state != NULL);
	return pg_atomic_read_u64(&cluster_grd_state->converts_full_count);
}


/* ============================================================
 * spec-5.3 — master-side convert API (live consumer of the 5.1b state
 *	machine).  These wrap the raw entry mutators with the lookup +
 *	entry->lock dance, mirroring cluster_grd_entry_enqueue_or_grant, so the
 *	GES work-queue drain (REQUEST/CONVERT) and the release drain stay
 *	symmetric and the ClusterGrdEntry body stays opaque to cluster_ges.c.
 * ============================================================ */

/*
 * cluster_grd_convert_or_enqueue -- master-side opcode-2 CONVERT entry point.
 *
 *	Locates the OLD holder slot by (node_id, procno, current_mode) + resid
 *	(the REDECLARE locator) and runs the 5.1b partial-order convert decision.
 *	On GRANTED_INPLACE the slot mode is upgraded and its request_id rebound to
 *	convert_request_id (§3.1a).  On ENQUEUED the convert sits in the entry's
 *	convert queue (priority over new waiters) and the conflicting holders are
 *	snapshotted into conflict_holders_out so the caller can fan out an
 *	advisory BAST.  ILLEGAL (LATERAL / no such holder) is fail-closed (53R74).
 */
ClusterGrdConvertResult
cluster_grd_convert_or_enqueue(const ClusterResId *resid, int32 node_id, uint32 procno,
							   uint64 cluster_epoch, LOCKMODE current_mode, LOCKMODE requested_mode,
							   uint64 convert_request_id, int32 source_node_id,
							   uint64 shard_master_generation,
							   ClusterGrdConflictHolder *conflict_holders_out, int *n_conflict_out)
{
	/* spec-5.8 D1c/D1e — plain entry forwards with a zero waiter meta. */
	ClusterGrdWaiterMeta meta = { InvalidTransactionId, 0 };

	return cluster_grd_convert_or_enqueue_meta(
		resid, node_id, procno, cluster_epoch, current_mode, requested_mode, convert_request_id,
		source_node_id, shard_master_generation, meta, conflict_holders_out, n_conflict_out);
}

/*
 * spec-5.8 D1c/D1e — waiter-metadata variant.  Stamps the converter's xid +
 * wait_seq onto the pending convert (ClusterGrdConvert) so its master-side WFG
 * convert-waiter vertex carries the real metadata.
 */
ClusterGrdConvertResult
cluster_grd_convert_or_enqueue_meta(const ClusterResId *resid, int32 node_id, uint32 procno,
									uint64 cluster_epoch, LOCKMODE current_mode,
									LOCKMODE requested_mode, uint64 convert_request_id,
									int32 source_node_id, uint64 shard_master_generation,
									ClusterGrdWaiterMeta meta,
									ClusterGrdConflictHolder *conflict_holders_out,
									int *n_conflict_out)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult lookup_result;
	ClusterGrdConvert creq;
	ClusterGrdConvertResult result;
	bool drain_hint = false;

	Assert(resid != NULL);

	if (n_conflict_out != NULL)
		*n_conflict_out = 0;

	lookup_result = cluster_grd_entry_lookup_or_create(resid, true, &entry);
	if (lookup_result == CLUSTER_GRD_ENTRY_NOT_READY)
		return CLUSTER_GRD_CONVERT_NOT_READY;
	if (lookup_result != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return CLUSTER_GRD_CONVERT_NOT_READY;

	memset(&creq, 0, sizeof(creq));
	creq.node_id = node_id;
	creq.source_node_id = source_node_id;
	creq.procno = procno;
	creq.cluster_epoch = cluster_epoch;
	creq.current_mode = current_mode;
	creq.requested_mode = requested_mode;
	creq.convert_request_id = convert_request_id;
	creq.shard_master_generation = shard_master_generation;
	creq.request_opcode = GES_REQ_OPCODE_CONVERT;
	creq.waiter_xid = meta.xid;	   /* spec-5.8 D1c */
	creq.wait_seq = meta.wait_seq; /* spec-5.8 D1e */
	creq.wait_start = 0;

	SpinLockAcquire(&entry->lock);
	result = cluster_grd_entry_request_convert(entry, &creq, &drain_hint);

	/*
	 * Snapshot the conflicting holders under the entry lock so the caller can
	 * emit a targeted advisory BAST (HC18 mirror).  Only meaningful when the
	 * convert was enqueued (UPGRADE conflict).
	 */
	if (result == CLUSTER_GRD_CONVERT_ENQUEUED && conflict_holders_out != NULL) {
		int nc = 0;

		for (int i = 0; i < entry->ngranted && nc < PGRAC_GRD_MAX_HOLDERS; i++) {
			if ((uint32)entry->holders[i].node_id == (uint32)node_id
				&& entry->holders[i].procno == procno)
				continue; /* spec-5.1c D5 self-exclude */
			if (ges_modes_compatible(entry->holders[i].mode, requested_mode))
				continue;
			conflict_holders_out[nc].holder.node_id = entry->holders[i].node_id;
			conflict_holders_out[nc].holder.procno = entry->holders[i].procno;
			conflict_holders_out[nc].holder.cluster_epoch = entry->holders[i].cluster_epoch;
			conflict_holders_out[nc].holder.request_id = entry->holders[i].request_id;
			conflict_holders_out[nc].source_node_id = entry->holders[i].node_id;
			conflict_holders_out[nc].held_mode = entry->holders[i].mode;
			nc++;
		}
		if (n_conflict_out != NULL)
			*n_conflict_out = nc;
	}

	SpinLockRelease(&entry->lock);
	cluster_grd_entry_release(entry);
	/* spec-5.8 D1b — enqueued convert registers its edges; in-place grant
	 * changed a holder mode, so queued waiters are refreshed. */
	grd_wfg_resync_entry(resid, NULL, 0);
	return result;
}

/*
 * cluster_grd_convert_grant_by_backend -- run the convert decision locating the
 *	OLD slot by the precise (node_id, procno, current_mode) (spec-5.3 §3.5
 *	native-probe clear path).
 *
 *	Used by the LMS native-probe resolve path: when a convert needed a native-
 *	lock probe, the master did NOT pre-mutate the holder (it stayed at the old
 *	mode for the probe window — fail-safe / conservative).  On probe CLEAR this
 *	commits the convert.  A backend may hold multiple cluster modes on one
 *	resid, so the OLD slot is located by the precise REDECLARE locator
 *	(node, procno, current_mode) carried in the LMS probe slot, then the 5.1b
 *	partial-order decision runs (GRANTED_INPLACE mutates + rebinds; ENQUEUED if
 *	a cluster conflict appeared during the probe; ILLEGAL if no such holder).
 */
ClusterGrdConvertResult
cluster_grd_convert_grant_by_backend(const ClusterResId *resid, int32 node_id, uint32 procno,
									 uint64 cluster_epoch, LOCKMODE current_mode,
									 LOCKMODE requested_mode, uint64 convert_request_id,
									 int32 source_node_id, uint64 shard_master_generation)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult lookup_result;
	ClusterGrdConvert creq;
	ClusterGrdConvertResult result;
	bool drain_hint = false;
	int hslot;

	Assert(resid != NULL);

	lookup_result = cluster_grd_entry_lookup_or_create(resid, true, &entry);
	if (lookup_result != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return CLUSTER_GRD_CONVERT_NOT_READY;

	SpinLockAcquire(&entry->lock);

	/*
	 * Precise REDECLARE locator (review): a backend may hold several cluster
	 * modes on one resid, so match (node, procno, current_mode) exactly rather
	 * than the first (node, procno) holder — otherwise the upgrade could land
	 * on the wrong slot.
	 */
	hslot = grd_find_holder_slot(entry, node_id, procno, current_mode);
	if (hslot < 0) {
		SpinLockRelease(&entry->lock);
		cluster_grd_entry_release(entry);
		pg_atomic_fetch_add_u64(&cluster_grd_state->convert_illegal_count, 1);
		return CLUSTER_GRD_CONVERT_ILLEGAL;
	}

	memset(&creq, 0, sizeof(creq));
	creq.node_id = node_id;
	creq.source_node_id = source_node_id;
	creq.procno = procno;
	creq.cluster_epoch = cluster_epoch;
	creq.current_mode = current_mode;
	creq.requested_mode = requested_mode;
	creq.convert_request_id = convert_request_id;
	creq.shard_master_generation = shard_master_generation;
	creq.request_opcode = GES_REQ_OPCODE_CONVERT;
	creq.wait_start = 0;

	result = cluster_grd_entry_request_convert(entry, &creq, &drain_hint);
	SpinLockRelease(&entry->lock);
	cluster_grd_entry_release(entry);
	/* spec-5.8 D1b — native-probe convert grant changed a holder mode. */
	grd_wfg_resync_entry(resid, NULL, 0);
	return result;
}

/* spec-5.3 — forward decl for the post-release empty-entry reclaim (definition
 * lives further down with the D10 cleanup primitives). */
static bool cluster_grd_hashremove_if_still_empty(const ClusterResId *resid);

/*
 * cluster_grd_release_and_drain -- remove a holder then drain the convert
 *	queue and one waiter under a single entry lock (spec-5.3 D3 live release).
 *
 *	Replaces the spec-2.23 release_and_pop on the GES RELEASE path: after the
 *	releasing holder is removed, every pending convert now compatible with the
 *	surviving holders is granted in place (FIFO, priority over waiters), then a
 *	single FIFO REQUEST waiter is popped.  Stale-epoch converts and waiters are
 *	dropped first (spec-4.6 P0#3 window guard: a grant reply echoing a stale
 *	tuple would be rejected and leak a zombie grant).  Returns the number of
 *	granted identities (each tagged REQUEST or CONVERT) for the caller to route.
 */
int
cluster_grd_release_and_drain(const ClusterResId *resid, const ClusterGrdHolderId *holder,
							  ClusterGrdGrantIdentity *granted_out, int max_out)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult lookup_result;
	uint64 cur_epoch;
	int n;

	Assert(resid != NULL && holder != NULL);
	Assert(granted_out != NULL && max_out > 0);

	lookup_result = cluster_grd_entry_lookup_or_create(resid, false, &entry);
	if (lookup_result != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return 0;

	SpinLockAcquire(&entry->lock);

	/* (1) Remove the releasing holder by full 4-tuple match (if present). */
	for (int i = 0; i < entry->ngranted; i++) {
		if ((uint32)entry->holders[i].node_id == holder->node_id
			&& entry->holders[i].procno == holder->procno
			&& entry->holders[i].cluster_epoch == holder->cluster_epoch
			&& entry->holders[i].request_id == holder->request_id) {
			if (i < entry->ngranted - 1)
				entry->holders[i] = entry->holders[entry->ngranted - 1];
			memset(&entry->holders[entry->ngranted - 1], 0, sizeof(ClusterGrdHolder));
			entry->ngranted--;
			entry->generation++;
			break;
		}
	}

	/* (2) Drop stale-epoch converts and waiters before granting. */
	cur_epoch = cluster_epoch_get_current();
	for (int c = 0; c < entry->nconverts;) {
		if (entry->converts[c].cluster_epoch < cur_epoch) {
			grd_convert_remove(entry, c);
			pg_atomic_fetch_add_u64(&cluster_grd_state->stale_request_drop_count, 1);
			continue;
		}
		c++;
	}
	for (int w = 0; w < entry->nwaiters;) {
		if (entry->waiters[w].cluster_epoch < cur_epoch) {
			if (w < entry->nwaiters - 1)
				entry->waiters[w] = entry->waiters[entry->nwaiters - 1];
			memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(entry->waiters[0]));
			entry->nwaiters--;
			pg_atomic_fetch_add_u64(&cluster_grd_state->stale_request_drop_count, 1);
			continue;
		}
		w++;
	}

	/* (3) Grant compatible converts then one waiter (5.1b drain). */
	n = cluster_grd_entry_drain_converts_then_waiters(entry, granted_out, max_out);

	SpinLockRelease(&entry->lock);
	cluster_grd_entry_release(entry);

	/* spec-5.8 D1b — granted waiters/converts departed; holders changed.
	 * Runs BEFORE the spec-5.3 empty-entry reclaim below: resync re-looks-up
	 * the entry by resid to refresh WFG edges, so it must see the entry before
	 * the reclaim may HASH_REMOVE it. */
	grd_wfg_resync_after_grants(resid, granted_out, n);

	/*
	 * PGRAC: spec-5.3 — reclaim the entry if this release left it completely
	 * empty (no holders/waiters/converts/reservations).  cluster_grd_entry_release
	 * is a documented no-op and the D8 periodic sweep is not wired, so without
	 * this a long-lived backend never frees GRD HTAB slots for finished lock
	 * resources — most visibly LOCKTAG_TRANSACTION, where every write txn has a
	 * unique xid (unique entry) and the HTAB fills (53R71 FAIL_RESERVATION_FULL).
	 * The helper re-acquires the shard partition lock, re-looks up by resid, and
	 * HASH_REMOVEs at-most-once ONLY when all occupancy counters are 0 — so a
	 * concurrent S3 reservation (nreservations > 0) or a freshly granted waiter
	 * (ngranted > 0) is safe (no-op).  Done after the spinlock is dropped to keep
	 * the partition-lock-then-entry-spinlock order.
	 */
	(void)cluster_grd_hashremove_if_still_empty(resid);
	return n;
}

/*
 * cluster_grd_entry_rollback_convert -- restore a slot upgraded by a convert
 *	back to its pre-convert (old_mode, old_request_id) (spec-5.3 §3.1a T4;
 *	caller holds entry->lock; raw mutator).
 *
 *	This is the STRICT INVERSE of the convert grant, NOT a release: a plain
 *	cluster_grd_entry_release_holder would DELETE the holder, leaving the
 *	master with no record of the still-held weaker lock (a false-grant for the
 *	next requester).
 *
 *	The backout must handle BOTH stages of a convert (review P0-1):
 *	  (1) the convert is still QUEUED (ENQUEUED behind a conflicting holder and
 *	      not yet granted) -- the requester timed out before it was granted.  A
 *	      pending entry sits in entry->converts[] (a backend has at most one
 *	      pending convert per resource, so it is located by (node,procno)).  It
 *	      MUST be removed, else a later release drain would grant it to a
 *	      requester that is gone -> phantom strong-mode holder.
 *	  (2) the convert was already GRANTED in place -- the upgraded holder slot
 *	      (located by (node,procno,upgraded_mode)) is restored to (old_mode,
 *	      old_request_id).
 *
 *	Returns OK if a pending convert was dequeued OR an upgraded slot was
 *	restored;  NOT_FOUND otherwise (idempotent on a retransmit / already-rolled-
 *	back rollback).
 */
ClusterGrdEntryResult
cluster_grd_entry_rollback_convert(ClusterGrdEntry *entry, int32 node_id, uint32 procno,
								   LOCKMODE upgraded_mode, LOCKMODE old_mode, uint64 old_request_id,
								   uint64 convert_request_id)
{
	int hslot;

	Assert(entry != NULL);

	/*
	 * (1) Cancel a still-queued convert from this backend (P0-1).
	 *
	 * PGRAC: spec-5.9 Hardening v1.0.1 (P1#2) — when convert_request_id is
	 * provided (!= 0) match it too, so a LATE rollback cannot evict a re-issued
	 * convert that reused this (node, procno) slot under a fresh request_id
	 * (ABA-safe).  A zero id falls back to the spec-5.3 (node, procno) match for
	 * callers that do not carry the convert's own reply key.
	 */
	for (int c = 0; c < entry->nconverts; c++) {
		if (entry->converts[c].node_id == node_id && entry->converts[c].procno == procno
			&& (convert_request_id == 0
				|| entry->converts[c].convert_request_id == convert_request_id)) {
			grd_convert_remove(entry, c);
			return CLUSTER_GRD_ENTRY_OK;
		}
	}

	/* (2) Restore an already-granted in-place upgrade. */
	hslot = grd_find_holder_slot(entry, node_id, procno, upgraded_mode);
	if (hslot < 0)
		return CLUSTER_GRD_ENTRY_NOT_FOUND;
	/*
	 * PGRAC: spec-5.9 Hardening v1.0.1 (P1#2) — verify the granted slot is the
	 * very convert we are rolling back (its request_id == convert_request_id)
	 * before downgrading it, so a late rollback cannot demote a re-issued convert
	 * that re-upgraded to the same mode (ABA false-grant).  A zero id keeps the
	 * spec-5.3 mode-only match.
	 */
	if (convert_request_id != 0 && entry->holders[hslot].request_id != convert_request_id)
		return CLUSTER_GRD_ENTRY_NOT_FOUND;

	entry->holders[hslot].mode = old_mode;
	entry->holders[hslot].request_id = old_request_id;
	entry->generation++;
	return CLUSTER_GRD_ENTRY_OK;
}

/*
 * cluster_grd_rollback_convert -- master-side opcode-14 CONVERT_ROLLBACK entry
 *	point (lookup + entry->lock + restore).  Wraps
 *	cluster_grd_entry_rollback_convert for the GES work-queue drain.
 */
ClusterGrdEntryResult
cluster_grd_rollback_convert(const ClusterResId *resid, int32 node_id, uint32 procno,
							 LOCKMODE upgraded_mode, LOCKMODE old_mode, uint64 old_request_id,
							 uint64 convert_request_id)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult lookup_result;
	ClusterGrdEntryResult result;

	Assert(resid != NULL);

	lookup_result = cluster_grd_entry_lookup_or_create(resid, false, &entry);
	if (lookup_result != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return CLUSTER_GRD_ENTRY_NOT_FOUND;

	SpinLockAcquire(&entry->lock);
	result = cluster_grd_entry_rollback_convert(entry, node_id, procno, upgraded_mode, old_mode,
												old_request_id, convert_request_id);
	SpinLockRelease(&entry->lock);
	cluster_grd_entry_release(entry);
	/* spec-5.8 D1b — convert rolled back (holder mode restored / queued convert
	 * cancelled); refresh queued waiters against the current holders. */
	grd_wfg_resync_entry(resid, NULL, 0);
	return result;
}

ClusterGrdEntryResult
cluster_grd_reservation_create(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder, int mode)
{
	int slot;

	Assert(entry != NULL && holder != NULL);

	if (entry->nreservations >= PGRAC_GRD_MAX_HOLDERS)
		return CLUSTER_GRD_ENTRY_FULL;
	slot = entry->nreservations++;
	entry->reservations[slot].id = *holder;
	entry->reservations[slot].mode = (LOCKMODE)mode;
	entry->generation++;
	return CLUSTER_GRD_ENTRY_OK;
}

ClusterGrdEntryResult
cluster_grd_reservation_cancel(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder)
{
	int i;

	Assert(entry != NULL && holder != NULL);

	for (i = 0; i < entry->nreservations; i++) {
		if (entry->reservations[i].id.node_id == holder->node_id
			&& entry->reservations[i].id.request_id == holder->request_id) {
			if (i < entry->nreservations - 1)
				entry->reservations[i] = entry->reservations[entry->nreservations - 1];
			memset(&entry->reservations[entry->nreservations - 1], 0,
				   sizeof(entry->reservations[0]));
			entry->nreservations--;
			entry->generation++;
			return CLUSTER_GRD_ENTRY_OK;
		}
	}
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

ClusterGrdEntryResult
cluster_grd_reservation_promote(ClusterGrdEntry *entry, const ClusterGrdHolderId *holder)
{
	int i;

	Assert(entry != NULL && holder != NULL);

	for (i = 0; i < entry->nreservations; i++) {
		if (entry->reservations[i].id.node_id == holder->node_id
			&& entry->reservations[i].id.request_id == holder->request_id) {
			LOCKMODE mode = entry->reservations[i].mode;
			ClusterGrdEntryResult r;

			if (i < entry->nreservations - 1)
				entry->reservations[i] = entry->reservations[entry->nreservations - 1];
			memset(&entry->reservations[entry->nreservations - 1], 0,
				   sizeof(entry->reservations[0]));
			entry->nreservations--;
			r = cluster_grd_entry_grant_holder(entry, holder, (int)mode);
			/* generation already bumped by grant_holder */
			return r;
		}
	}
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}


/* ============================================================
 * CSSD DEAD / stale-epoch cleanup stubs — Step 4 D11 真激活.
 * ============================================================ */

/*
 * spec-2.16 Step 4 D11:  CSSD DEAD master sweep — traverses entry HTAB
 *   and per-entry filters holders[] / waiters[] / converts[] by
 *   node_id == dead_node_id (I48 — NO epoch filter).
 *
 *   Step 4 implementation:  uses cluster_grd_entry_htab via existing
 *   hash_seq_search pattern (mirror cluster_grd_entries_walk).  For
 *   each matching slot, decrement entry->ngranted / nwaiters / nconverts
 *   under entry->lock and zero the slot.  Idempotent re-entry safe.
 *
 *   Counters per cleanup invocation tracked via existing entry mutator
 *   counter family (spec-2.15 entry_current_count when ngranted hits 0).
 *   本 Step 0 真 mutator caller (spec-2.16 ships caller-side hooks
 *   stub only — full LockAcquireExtended 6-step integration in spec-
 *   2.17), so sweep is a no-op until cluster_unit Step 6 inject test
 *   exercises mutator + sweep round-trip.
 */
/* spec-2.24 D10 forward decl (definition later in same TU). */
extern int cluster_grd_entry_cleanup_guarded(ClusterGrdEntry *entry, int dead_procno,
											 int32 dead_node_id);

void
cluster_grd_cleanup_on_node_dead(int32 dead_node_id)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	int swept = 0;
	uint64 pending_x_cleared = 0;

	pending_x_cleared = cluster_pcm_lock_clear_pending_x_for_node(dead_node_id);
	if (pending_x_cleared > 0)
		ereport(DEBUG1, (errmsg_internal("cluster_grd_cleanup_on_node_dead(%d): "
										 "cleared " UINT64_FORMAT " PCM pending_x entries",
										 dead_node_id, pending_x_cleared)));

	if (cluster_grd_entry_htab == NULL) {
		ereport(DEBUG2, (errmsg_internal("cluster_grd_cleanup_on_node_dead(%d): "
										 "entry HTAB not allocated;  no-op",
										 dead_node_id)));
		return;
	}

	/*
	 * spec-2.24 D9 — converge via D10 cluster_grd_entry_cleanup_guarded
	 * (HC27 dual-path convergence;previously spec-2.16 had its own ad-hoc
	 * sweep loop here).  D10 enforces HC25-26 / I-cleanup-1..4 — HASH_REMOVE
	 * at-most-once + concurrent cleanup safety + RELEASE enqueue.
	 */
	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		swept += cluster_grd_entry_cleanup_guarded(entry, -1, dead_node_id);
	}

	if (swept > 0)
		ereport(DEBUG1, (errmsg_internal("cluster_grd_cleanup_on_node_dead(%d): "
										 "swept %d holder/waiter/convert slots via D10 primitive",
										 dead_node_id, swept)));
}

/*
 * cluster_grd_clean_leave_drain_self -- spec-5.13 D4 cooperative GES drain.
 *
 *	The planned, cooperative dual of the failure path: a LIVE node leaving on
 *	purpose proactively (1) remasters the shards it owns to survivors and (2)
 *	releases every GES grant/waiter/convert it holds.  Both reuse the proven
 *	failure-driven primitives, invoked proactively rather than on a CSSD DEAD
 *	edge: cluster_grd_master_map_remaster() recomputes the survivor master map
 *	with the leaving node removed, and cluster_grd_cleanup_on_node_dead()
 *	enqueues a RELEASE to each master (HC25-26 at-most-once) and clears the
 *	local holder/waiter/convert slots.  Run on the leaving node (RELEASE to
 *	masters + clear self); survivors run the same primitives from their own
 *	reconfig path.  After this + the existing REDECLARE all-survivor barrier no
 *	GRD state references the leaving node (CL-I2; see verify below).  Returns
 *	the number of master shards moved off the leaving node.
 */
uint32
cluster_grd_clean_leave_drain_self(int32 leaving_node, uint64 leave_epoch)
{
	uint64 leave_bitmap[(CLUSTER_MAX_NODES + 63) / 64];
	uint32 moved;

	if (leaving_node < 0 || leaving_node >= CLUSTER_MAX_NODES)
		return 0;

	/* 1. move the shards this node masters to survivors (deterministic recompute) */
	memset(leave_bitmap, 0, sizeof(leave_bitmap));
	leave_bitmap[leaving_node >> 6] |= (UINT64CONST(1) << (leaving_node & 63));
	moved = cluster_grd_master_map_remaster(leave_bitmap, leave_epoch);

	/* 2. release every GES grant/waiter/convert held by the leaving node */
	cluster_grd_cleanup_on_node_dead(leaving_node);

	return moved;
}

/*
 * cluster_grd_clean_leave_verify_no_leftover -- spec-5.13 D4 CL-I2 proof.
 *
 *	Read-only scan: returns true iff no GRD entry holds a holder / waiter /
 *	convert for the leaving node AND no shard is still mastered by it.  Used as
 *	the acceptance/assertion gate after drain + REDECLARE barrier (a leftover
 *	leaving holder would be a cross-node double-grant hazard, rule 8.A).
 */
bool
cluster_grd_clean_leave_verify_no_leftover(int32 leaving_node)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	uint32 i;

	if (leaving_node < 0 || leaving_node >= CLUSTER_MAX_NODES)
		return true;

	/* no shard may still be mastered by the leaving node */
	if (cluster_grd_state != NULL
		&& pg_atomic_read_u32(&cluster_grd_state->master_map_initialized) != 0) {
		for (i = 0; i < PGRAC_GRD_SHARD_COUNT; i++) {
			if ((int32)pg_atomic_read_u32(&cluster_grd_state->master[i]) == leaving_node)
				return false;
		}
	}

	if (cluster_grd_entry_htab == NULL)
		return true;

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		int k;

		for (k = 0; k < entry->ngranted; k++) {
			if (entry->holders[k].node_id == leaving_node) {
				hash_seq_term(&status);
				return false;
			}
		}
		for (k = 0; k < entry->nwaiters; k++) {
			if (entry->waiters[k].node_id == leaving_node) {
				hash_seq_term(&status);
				return false;
			}
		}
		for (k = 0; k < entry->nconverts; k++) {
			if (entry->converts[k].node_id == leaving_node) {
				hash_seq_term(&status);
				return false;
			}
		}
	}
	return true;
}

/*
 * spec-2.16 Step 4 D11:  stale-epoch sweep — independent rule per I48.
 *   Filters by holder.cluster_epoch < current_epoch.  Triggered post-
 *   reconfig epoch bump (LMON tick S2;  I47).
 *
 *   Filters real holders by cluster_epoch.  Reservation cleanup is owned
 *   by caller-side S7 because reservations are local in-flight state, not
 *   a cluster-visible grant.
 */
void
cluster_grd_cleanup_stale_epoch(uint64 current_epoch)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	int swept = 0;

	if (cluster_grd_entry_htab == NULL) {
		ereport(DEBUG2, (errmsg_internal("cluster_grd_cleanup_stale_epoch(%lu): "
										 "entry HTAB not allocated;  no-op",
										 (unsigned long)current_epoch)));
		return;
	}

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		int i;

		SpinLockAcquire(&entry->lock);
		for (i = 0; i < entry->ngranted;) {
			if (entry->holders[i].cluster_epoch < current_epoch) {
				if (i < entry->ngranted - 1)
					entry->holders[i] = entry->holders[entry->ngranted - 1];
				memset(&entry->holders[entry->ngranted - 1], 0, sizeof(entry->holders[0]));
				entry->ngranted--;
				swept++;
				continue;
			}
			i++;
		}
		SpinLockRelease(&entry->lock);
	}

	if (swept > 0)
		ereport(DEBUG1, (errmsg_internal("cluster_grd_cleanup_stale_epoch(%lu): "
										 "swept %d holder slots",
										 (unsigned long)current_epoch, swept)));
}


/* ============================================================
 * spec-2.16 D8:  LMON tick body GRD dead sweep — newly-dead bitmap
 *   diff per v0.5 P1.2 + I51.
 *
 *   static last_dead_bitmap + per-tick diff:
 *     - poll cluster_cssd_get_dead_generation();  unchanged → return
 *     - scan peer_state for all peers;  state==DEAD → set bit
 *     - newly_dead = current & ~last_dead_bitmap
 *     - for each newly-dead peer → cluster_grd_cleanup_on_node_dead(id)
 *     - last_dead_bitmap = current (commit AFTER sweep — crash-safe)
 *
 *   ALIVE / SUSPECTED不计;DEAD→ALIVE recovery 不重 sweep (bit drops
 *   from current_dead_bitmap, but already in last_dead_bitmap; on next
 *   transition ALIVE→DEAD bit re-enters newly_dead per AND-NOT logic).
 *
 *   Process-local static (per-postmaster).  LMON is singleton, so no
 *   shared-state contention.
 * ============================================================ */

static uint64 cluster_grd_last_dead_bitmap = 0;
static uint64 cluster_grd_last_dead_generation = 0;

void
cluster_grd_lmon_tick_dead_sweep(void)
{
	uint64 current_gen;
	uint64 current_dead_bitmap = 0;
	uint64 newly_dead;
	int peer_id;

	/* Postmaster-only tick (single LMON consumer).  No LWLock needed
	 * for static state. */
	current_gen = cluster_cssd_get_dead_generation();
	if (current_gen == cluster_grd_last_dead_generation)
		return;

	/* Scan peer states to build current_dead_bitmap.  Only DEAD counts;
	 * SUSPECTED is hysteresis-mid, not a sweep trigger. */
	for (peer_id = 0; peer_id < CLUSTER_MAX_NODES && peer_id < 64; peer_id++) {
		ClusterCssdPeerState s = cluster_cssd_get_peer_state(peer_id);
		if (s == CLUSTER_CSSD_PEER_DEAD)
			current_dead_bitmap |= ((uint64)1 << peer_id);
	}

	newly_dead = current_dead_bitmap & ~cluster_grd_last_dead_bitmap;
	for (peer_id = 0; peer_id < 64; peer_id++) {
		if (newly_dead & ((uint64)1 << peer_id)) {
			/*
			 * spec-5.10 D8 (fix-forward order) — clear the dead node's boosted
			 * flags BEFORE the cleanup removes its queue slots.  Run first, the
			 * sweep finds the dead boosted W still queued, clears it, and the
			 * WFG refresh re-derives every live waiter without that boost — so a
			 * live request held behind the dead W is un-barriered and its stale
			 * R->W edge is retracted.  If cleanup ran first the dead W would
			 * already be gone, the sweep would find nothing, and the live waiter
			 * would stay barriered behind a vertex that no longer exists until
			 * an unrelated resync.
			 */
			(void)cluster_grd_clear_boosted_for_node((int32)peer_id);
			cluster_grd_cleanup_on_node_dead((int32)peer_id);
		}
	}

	/* Commit AFTER sweep — crash-safe idempotent;  reboot reconstructs. */
	cluster_grd_last_dead_bitmap = current_dead_bitmap;
	cluster_grd_last_dead_generation = current_gen;
}


/* ============================================================
 * spec-2.17 D28b:  cluster_grd_alloc_generation helper.
 *
 *   Called from InitProcess() to allocate a per-backend monotonic
 *   generation number(uint64).  ABA-free via atomic fetch_add.
 *   0 reserved sentinel(0 = uninitialized).
 *
 *   Used by BAST/CANCEL stale signal validation:
 *     `MyProc->cluster_grd_generation == payload.target_generation`
 *     防 stale signal 误打到复用 procno 的新 backend.
 * ============================================================ */

uint64
cluster_grd_alloc_generation(void)
{
	/* Bootstrap-safe:  cluster_grd_state may be NULL in bootstrap mode
	 * (postmaster shmem not yet initialized).  Return 0 sentinel —
	 * caller is InitProcess() PGRAC hook which falls through gracefully. */
	if (cluster_grd_state == NULL)
		return 0;
	return pg_atomic_fetch_add_u64(&cluster_grd_state->next_generation, 1);
}


/* ============================================================
 * spec-2.17 D8 + D12:  BAST handler + 6 counter helpers.
 *
 *   D8 cluster_grd_bast_handler — ProcessInterrupts hook;backend 收到
 *   PROCSIG_CLUSTER_GES_BAST 后调.  **硬契约(I85 P1.8 v0.6)**:
 *   仅标 `MyProc->cluster_grd_bast_pending = true` flag;**0 主动 release**;
 *   naturally 等 canonical LockRelease/LockReleaseAll 自然路径 → LOCALLOCK
 *   refcount 0 → 7-step state machine release path 补发 GES_RELEASE.
 *
 *   spec-5.8 D8:  the PROCSIG_CLUSTER_GES_CANCEL pending flag
 *   (cluster_ges_cancel_pending) is NOT handled from ProcessInterrupts — it
 *   is owned by the caller-side lock-acquire check points (seven-step entry +
 *   the GES wait loops), which observe it after CHECK_FOR_INTERRUPTS and turn
 *   it into FAIL_DEADLOCK.  See cluster_grd_check_pending_interrupts.
 *
 *   D12 6 BAST nofail counter inc + read helpers.
 * ============================================================ */

/*
 * cluster_grd_bast_local_deliver_ok -- pure best-effort delivery guard for a
 * local BAST target (spec-5.1c D5).  Returns true when the master should send
 * PROCSIG_CLUSTER_GES_BAST to the resolved backend.
 *
 *	Guards: (1) procno in range; (2) the holder's recorded cluster_epoch is
 *	the current epoch (drops cross-epoch stale holders, mirroring the CANCEL
 *	path); (3) the resolved slot has a live backend (pid != 0 and a valid
 *	BackendId).  This is best-effort: it does NOT detect same-epoch procno
 *	reuse (an exited holder whose slot a new backend now occupies) -- that is
 *	acceptable because BAST only sets an advisory flag (no proactive release,
 *	I85), so a spurious flag has no correctness effect.  No shmem access, so
 *	it is unit-testable standalone (the caller resolves pid/BackendId via
 *	GetPGProcByNumber and SendProcSignal re-checks pss_pid == pid).
 */
bool
cluster_grd_bast_local_deliver_ok(uint32 procno, int proc_count, uint64 holder_epoch,
								  uint64 current_epoch, int target_pid, int target_backendid)
{
	if (proc_count <= 0 || procno >= (uint32)proc_count)
		return false; /* procno out of range */
	if (holder_epoch != current_epoch)
		return false; /* stale (cross-epoch) holder */
	if (target_pid == 0 || target_backendid <= 0)
		return false; /* no live backend (BackendId must be > 0:
					   * SendProcSignal indexes psh_slot[backendId-1], and PG
					   * assigns backendId only after startup -- a recycled slot
					   * mid-startup has pid set but backendId still 0). */
	return true;
}

void
cluster_grd_bast_handler(void)
{
	/* spec-2.17 I85 硬契约:仅标 flag;不主动 release / convert.
	 * naturally 等 LockRelease canonical 路径补发 GES_RELEASE. */
	if (MyProc != NULL)
		MyProc->cluster_grd_bast_pending = true;
	cluster_grd_inc_bast_received();
}

void
cluster_grd_check_pending_interrupts(void)
{
	if (cluster_ges_bast_pending) {
		cluster_ges_bast_pending = false;
		cluster_grd_bast_handler();
	}

	/*
	 * spec-5.8 D8 — deliberately do NOT consume cluster_ges_cancel_pending
	 * here.  The deadlock-victim cancel flag is owned by the caller-side
	 * lock-acquire check points: (a) cluster_lock_acquire_seven_step entry and
	 * (b) the GES reply/convert wait loops in cluster_ges.c.  This function
	 * runs from ProcessInterrupts on every CHECK_FOR_INTERRUPTS();  resetting
	 * the flag here would eat it before the wait loop's post-sleep re-check
	 * observes it, so the victim would sleep through its own cancellation and
	 * never break out (the deadlock would only "resolve" via the request
	 * timeout backstop).  The flag must survive CFI and reach the wait loop
	 * that turns it into FAIL_DEADLOCK -> 40P01.
	 */

	/* spec-4.6 D3 — cooperative holder rebind.  Clear-then-work (a new
	 * broadcast re-arms the flag);  the walker is no-throw and acks the
	 * barrier generation only on full success, so a partial pass simply
	 * leaves this backend un-acked until LMON's re-broadcast. */
	if (cluster_grd_redeclare_pending) {
		cluster_grd_redeclare_pending = false;
		cluster_grd_redeclare_all_registered();
	}
}

#define DEFINE_BAST_COUNTER(short_name, full_field)                                                \
	void cluster_grd_inc_##short_name(void)                                                        \
	{                                                                                              \
		if (cluster_grd_state != NULL)                                                             \
			pg_atomic_fetch_add_u64(&cluster_grd_state->ges_##full_field, 1);                      \
	}                                                                                              \
	uint64 cluster_grd_##full_field(void)                                                          \
	{                                                                                              \
		if (cluster_grd_state == NULL)                                                             \
			return 0;                                                                              \
		return pg_atomic_read_u64(&cluster_grd_state->ges_##full_field);                           \
	}

DEFINE_BAST_COUNTER(bast_sent, bast_sent_count)
DEFINE_BAST_COUNTER(bast_received, bast_received_count)
DEFINE_BAST_COUNTER(bast_ack, bast_ack_count)
DEFINE_BAST_COUNTER(bast_retry, bast_retry_count)
DEFINE_BAST_COUNTER(bast_reject, bast_reject_count)
DEFINE_BAST_COUNTER(bast_stale_drop, bast_stale_drop_count)


/* ============================================================
 * spec-2.17 Step 5/8:  deadlock detector skeleton.
 *
 *   Real activation:
 *     - LMON tick body invokes cluster_grd_deadlock_lmon_tick() each
 *       cluster.ges_deadlock_check_interval_ms(default 1000ms);
 *     - Tick body builds wait-for graph via vertex dictionary + edge
 *       chunk protocol(I82 collision-free);
 *     - On cycle detected:  victim selection via deterministic age-based
 *       4-tuple `(cluster_epoch, local_start_ts_ms DESC, node_id, xid)`
 *       (I69 P2.2);
 *     - Master enqueues GES_CANCEL_PENDING(opcode 7)or GES_RELEASE
 *       (opcode 3)to victim's outbound(I73-I74).
 *
 *   Step 5/8 skeleton:  function symbol + 3 counter only.  Real Tarjan
 *   SCC + vertex dict encode/decode + chunked reassembly buffer 推
 *   Hardening round(本 skeleton 已建立完整调用面 + counter 接口供
 *   Step 8 dump_grd + TAP test 钩入).
 * ============================================================ */

void
cluster_grd_deadlock_lmon_tick(void)
{
	/* Skeleton — Hardening round real Tarjan + vertex dict. */
}

DEFINE_BAST_COUNTER(deadlock_probe_drop, deadlock_probe_drop_count)
DEFINE_BAST_COUNTER(deadlock_probe_collision_drop, deadlock_probe_collision_drop_count)
DEFINE_BAST_COUNTER(deadlock_chunk_oo_buffer_overflow, deadlock_chunk_oo_buffer_overflow_count)


/* ============================================================
 * spec-2.17 Step 6:  cleanup_on_backend_exit(D21 skeleton).
 *
 *   Real activation:  on_proc_exit hook + ResourceOwner callback wire
 *   in Step 6;遍历 GRD entries 清单 backend procno 的 holders/waiters/
 *   converts(类 cleanup_on_node_dead pattern;但 backend-level not
 *   node-level)。
 *
 *   场景(I65 P1.1 — NOT BAST timeout):CANCEL / SIGTERM / on_proc_exit
 *   / backend self-abort.
 * ============================================================ */

/*
 * spec-2.24 D10 helper — HASH_REMOVE guarded by shard partition LWLock
 * + re-lookup + verify-still-empty.  Returns true if removed (we are
 * the at-most-once winner per HC26 I-cleanup-4);  false if another
 * cleanup path already removed the entry or the entry is no longer
 * empty (race lost — caller bumps skip counter).
 */
static bool
cluster_grd_hashremove_if_still_empty(const ClusterResId *resid)
{
	uint32 hashvalue;
	int shard_id;
	bool found = false;
	ClusterGrdEntry *entry;
	bool removed = false;

	if (cluster_grd_entry_htab == NULL)
		return false;

	hashvalue = cluster_grd_hash_resource(resid);
	shard_id = cluster_grd_shard_for_hash(hashvalue);

	LWLockAcquire(&cluster_grd_shard_locks[shard_id].lock, LW_EXCLUSIVE);

	entry
		= hash_search_with_hash_value(cluster_grd_entry_htab, resid, hashvalue, HASH_FIND, &found);
	if (entry != NULL) {
		SpinLockAcquire(&entry->lock);
		if (entry->ngranted == 0 && entry->nwaiters == 0 && entry->nconverts == 0
			&& entry->nreservations == 0) {
			SpinLockRelease(&entry->lock);
			(void)hash_search_with_hash_value(cluster_grd_entry_htab, resid, hashvalue, HASH_REMOVE,
											  &found);
			pg_atomic_fetch_sub_u64(&cluster_grd_state->entry_current_count, 1);
			removed = true;
		} else {
			SpinLockRelease(&entry->lock);
		}
	}

	LWLockRelease(&cluster_grd_shard_locks[shard_id].lock);
	return removed;
}

/*
 * spec-2.24 D10 — idempotent generation-guarded cleanup primitive.
 *
 *	HC25-26 / I-cleanup-1..4 enforcement single source of truth.  All 3
 *	cleanup paths converge here (HC27 dual-path convergence):
 *	  - D6 on_proc_exit fast path → cluster_grd_cleanup_on_backend_exit
 *	  - D8 LMD periodic safety net → cluster_lmd_periodic_cleanup_sweep
 *	  - D9 cssd dead-node bitmap → cluster_grd_cleanup_on_node_dead
 *
 *	Semantics (user §3 invariants):
 *	  I-cleanup-1 safe-to-call-multi:  re-entry NOT_FOUND no-op safe.
 *	  I-cleanup-2 only-one-owner:  entry->lock serializes mutation; if
 *	    a concurrent path already swept everything we look for, our
 *	    inner loop finds no matching slot and removed remains 0.
 *	  I-cleanup-3 NOT_FOUND no-op:  loop matches by 4-tuple of slot
 *	    contents; absent → just continue, no error.
 *	  I-cleanup-4 HASH_REMOVE at-most-once:  empty-entry HASH_REMOVE
 *	    guarded by shard partition LWLock re-lookup + verify-still-
 *	    empty + verify-generation-advanced;losing path increments
 *	    cleanup_skip_other_owner_count.
 *
 *	dead_procno:  if >= 0, match slot.procno == dead_procno (local
 *	  backend exit / SIGKILL).  Always combined with local node match
 *	  (slot.node_id == cluster_node_id) — must NOT compare local
 *	  ProcArray to remote holder procno per user codereview Change 4.
 *	dead_node_id:  if >= 0, match slot.node_id == dead_node_id (peer
 *	  node death from cssd dead-bitmap).
 *
 *	Returns number of slots removed across all 4 slot kinds.
 */
int
cluster_grd_entry_cleanup_guarded(ClusterGrdEntry *entry, int dead_procno, int32 dead_node_id)
{
	int removed = 0;
	bool became_empty = false;
	GesRequestPayload release_payloads[PGRAC_GRD_MAX_HOLDERS];
	int n_release = 0;
	ClusterResId entry_resid;

	Assert(entry != NULL);

	SpinLockAcquire(&entry->lock);

	/* HC26 I-cleanup-3 — each remove path matches by content; absent → continue. */
	for (int i = entry->ngranted - 1; i >= 0; i--) {
		bool match = false;

		if (dead_procno >= 0 && entry->holders[i].node_id == (int32)cluster_node_id
			&& entry->holders[i].procno == (uint32)dead_procno)
			match = true;
		if (dead_node_id >= 0 && entry->holders[i].node_id == dead_node_id)
			match = true;
		if (!match)
			continue;

		/* Stash a full GES_RELEASE payload for post-lock enqueue. */
		if (n_release < PGRAC_GRD_MAX_HOLDERS) {
			memset(&release_payloads[n_release], 0, sizeof(GesRequestPayload));
			release_payloads[n_release].opcode = GES_REQ_OPCODE_RELEASE;
			release_payloads[n_release].lockmode = (uint32)entry->holders[i].mode;
			release_payloads[n_release].holder_node_id = (uint32)entry->holders[i].node_id;
			release_payloads[n_release].holder_procno = entry->holders[i].procno;
			release_payloads[n_release].holder_cluster_epoch_lo
				= (uint32)(entry->holders[i].cluster_epoch & 0xffffffffu);
			release_payloads[n_release].holder_cluster_epoch_hi
				= (uint32)(entry->holders[i].cluster_epoch >> 32);
			release_payloads[n_release].holder_request_id_lo
				= (uint32)(entry->holders[i].request_id & 0xffffffffu);
			release_payloads[n_release].holder_request_id_hi
				= (uint32)(entry->holders[i].request_id >> 32);
			memcpy(release_payloads[n_release].resid, &entry->resid,
				   sizeof(release_payloads[n_release].resid));
			n_release++;
		}

		if (i < entry->ngranted - 1)
			entry->holders[i] = entry->holders[entry->ngranted - 1];
		memset(&entry->holders[entry->ngranted - 1], 0, sizeof(ClusterGrdHolder));
		entry->ngranted--;
		removed++;
	}
	for (int i = entry->nwaiters - 1; i >= 0; i--) {
		bool match = false;

		if (dead_procno >= 0 && entry->waiters[i].node_id == (int32)cluster_node_id
			&& entry->waiters[i].procno == (uint32)dead_procno)
			match = true;
		if (dead_node_id >= 0 && entry->waiters[i].node_id == dead_node_id)
			match = true;
		if (!match)
			continue;

		if (i < entry->nwaiters - 1)
			entry->waiters[i] = entry->waiters[entry->nwaiters - 1];
		memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(ClusterGrdWaiter));
		entry->nwaiters--;
		removed++;
	}
	for (int i = entry->nconverts - 1; i >= 0; i--) {
		bool match = false;

		/* spec-5.1b §3 clause 14: convert queue entries sweep with holders/
		 * waiters — match the local-backend-death case symmetrically (the
		 * convert locator carries (node_id, procno)).  Latent in 5.1b
		 * (converts[] is production-empty: opcode-2 is rejected, no live
		 * producer until spec-5.2), but keeps the sweep complete for the
		 * 5.2 producer. */
		if (dead_procno >= 0 && entry->converts[i].node_id == (int32)cluster_node_id
			&& entry->converts[i].procno == (uint32)dead_procno)
			match = true;
		if (dead_node_id >= 0 && entry->converts[i].node_id == dead_node_id)
			match = true;
		if (!match)
			continue;

		if (i < entry->nconverts - 1)
			entry->converts[i] = entry->converts[entry->nconverts - 1];
		memset(&entry->converts[entry->nconverts - 1], 0, sizeof(ClusterGrdConvert));
		entry->nconverts--;
		removed++;
	}
	for (int i = entry->nreservations - 1; i >= 0; i--) {
		bool match = false;

		if (dead_procno >= 0 && entry->reservations[i].id.node_id == (uint32)cluster_node_id
			&& entry->reservations[i].id.procno == (uint32)dead_procno)
			match = true;
		if (dead_node_id >= 0 && entry->reservations[i].id.node_id == (uint32)dead_node_id)
			match = true;
		if (!match)
			continue;

		if (i < entry->nreservations - 1)
			entry->reservations[i] = entry->reservations[entry->nreservations - 1];
		memset(&entry->reservations[entry->nreservations - 1], 0, sizeof(entry->reservations[0]));
		entry->nreservations--;
		removed++;
	}

	/* HC25 — bump generation if any mutation; serves as ABA marker for
	 * concurrent cleanup detection (other paths see new generation and
	 * recheck before HASH_REMOVE). */
	if (removed > 0)
		entry->generation++;

	became_empty = (entry->ngranted == 0 && entry->nwaiters == 0 && entry->nconverts == 0
					&& entry->nreservations == 0);

	memcpy(&entry_resid, &entry->resid, sizeof(entry_resid));

	SpinLockRelease(&entry->lock);

	/* Enqueue one full GES_RELEASE per removed real holder.  Route to the
	 * resource master, not to the holder node. */
	if (n_release > 0) {
		int32 master = cluster_grd_lookup_master(&entry_resid);

		if (master >= 0 && master != cluster_node_id) {
			for (int i = 0; i < n_release; i++)
				cluster_grd_outbound_enqueue_cleanup_release((uint32)master, &release_payloads[i],
															 sizeof(GesRequestPayload));
		}
	}

	/* HC26 I-cleanup-4 — HASH_REMOVE at-most-once per entry lifetime.
	 * Re-acquire shard partition LWLock + re-lookup resid + verify still
	 * empty; the losing path (race with another cleanup that already
	 * removed) increments skip counter. */
	if (became_empty) {
		bool removed_ok = cluster_grd_hashremove_if_still_empty(&entry_resid);
		if (!removed_ok)
			cluster_lmd_cleanup_skip_other_owner_count_inc(1);
	}

	return removed;
}

/*
 * Entry-by-procno sweep.  Iterates GRD HTAB; for each entry, invokes
 * D10 guarded primitive.  Returns total slots removed.
 */
static int
cluster_grd_entries_cleanup_by_procno_guarded(int procno)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	int total = 0;

	if (cluster_grd_entry_htab == NULL || procno < 0)
		return 0;

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		total += cluster_grd_entry_cleanup_guarded(entry, procno, -1);
	}
	return total;
}

void
cluster_grd_cleanup_on_backend_exit(int procno)
{
	int swept;

	if (procno < 0)
		return; /* I-cleanup-1 — re-entry safe */

	swept = cluster_grd_entries_cleanup_by_procno_guarded(procno);
	if (swept > 0)
		cluster_lmd_cleanup_on_backend_exit_count_inc((uint64)swept);
}

/*
 * spec-2.24 D7 — before_shmem_exit callback wrapper.
 *
 *	Registered from InitPostgres so every backend gets the hook on
 *	exit.  MyProcNumber is valid by InitPostgres time;  if -1 (auxiliary
 *	process pre-ProcSignalInit), I-cleanup-1 early return is safe.
 */
/*
 * spec-2.24 D8 helper — sweep local stale procnos.
 *
 *	HC28 chunked semantic:  iterate the GRD HTAB once, briefly snapshot
 *	ProcArray for active local pgprocno set, then for each entry invoke
 *	cluster_grd_entry_cleanup_guarded with the local stale procno if its
 *	holders[].node_id == cluster_node_id and procno not in the active
 *	set.  Per spec-2.24 §1.4 example 2 — must NOT compare local
 *	ProcArray to remote holder procno.
 *
 *	Returns total slots removed.
 */
int
cluster_grd_sweep_local_stale_procnos(void)
{
	HASH_SEQ_STATUS status;
	ClusterGrdEntry *entry;
	int total = 0;
	uint8 *alive;
	int n_alive_max = MaxBackends;

	if (cluster_grd_entry_htab == NULL)
		return 0;

	alive = (uint8 *)palloc0((Size)n_alive_max);

	/* Briefly snapshot ProcArray active pgprocno set. */
	LWLockAcquire(ProcArrayLock, LW_SHARED);
	{
		int n = ProcGlobal->allProcCount;
		for (int i = 0; i < n && i < n_alive_max; i++) {
			PGPROC *p = &ProcGlobal->allProcs[i];
			if (p->pid != 0)
				alive[i] = 1;
		}
	}
	LWLockRelease(ProcArrayLock);

	hash_seq_init(&status, cluster_grd_entry_htab);
	while ((entry = (ClusterGrdEntry *)hash_seq_search(&status)) != NULL) {
		uint32 stale_procno = (uint32)-1;
		uint32 i;

		SpinLockAcquire(&entry->lock);
		for (i = 0; i < (uint32)entry->ngranted; i++) {
			if (entry->holders[i].node_id != (int32)cluster_node_id)
				continue;
			if (entry->holders[i].procno < (uint32)n_alive_max
				&& alive[entry->holders[i].procno] == 0) {
				stale_procno = entry->holders[i].procno;
				break;
			}
		}
		SpinLockRelease(&entry->lock);

		if (stale_procno != (uint32)-1)
			total += cluster_grd_entry_cleanup_guarded(entry, (int)stale_procno, -1);
	}

	pfree(alive);
	return total;
}

void
cluster_grd_cleanup_on_backend_exit_callback(int code, Datum arg)
{
	(void)code;
	(void)arg;
	if (MyProc == NULL)
		return;
	cluster_grd_cleanup_on_backend_exit(MyProc->pgprocno);
}


/* ============================================================
 * spec-2.21 D5 high-level helpers — encapsulate entry slock + 5-check +
 * reservation/promote.  cluster_lock_acquire.c uses these so the entry
 * struct definition can stay private to this file.
 * ============================================================ */

ClusterGrdEntryResult
cluster_grd_try_reserve(const ClusterResId *resid, const ClusterGrdHolderId *holder, int mode,
						int32 self_node_id, bool *fast_path_out, uint64 *gen_snapshot_out)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult er;
	int32 master;
	bool fast_path;

	Assert(resid != NULL && holder != NULL);

	er = cluster_grd_entry_lookup_or_create(resid, true, &entry);
	if (er != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return er;

	master = cluster_grd_lookup_master(resid);

	SpinLockAcquire(&entry->lock);
	if (gen_snapshot_out)
		*gen_snapshot_out = entry->generation;

	fast_path = (master == self_node_id || master < 0)
				&& !cluster_grd_entry_has_remote_holder(entry, self_node_id)
				&& !cluster_grd_entry_has_pending_waiter(entry)
				&& !cluster_grd_entry_has_pending_convert(entry);

	er = cluster_grd_reservation_create(entry, holder, mode);
	SpinLockRelease(&entry->lock);

	if (fast_path_out)
		*fast_path_out = fast_path && (er == CLUSTER_GRD_ENTRY_OK);
	return er;
}

ClusterGrdEntryResult
cluster_grd_revalidate_and_promote(const ClusterResId *resid, const ClusterGrdHolderId *holder,
								   int32 self_node_id, uint64 gen_snapshot)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult er;
	bool revalidate_ok;

	Assert(resid != NULL && holder != NULL);

	er = cluster_grd_entry_lookup_or_create(resid, false, &entry);
	if (er != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return CLUSTER_GRD_ENTRY_NOT_FOUND;

	SpinLockAcquire(&entry->lock);

	/*
	 * spec-5.3 (L11) — local-master REQUEST path.  When this node masters the
	 * resource, the holder was already registered authoritatively by
	 * enqueue_or_grant / the release drain under the entry lock (cluster_ges.c
	 * local-master branch), so there is NO reservation to promote — the
	 * optimistic S3 reservation's gen_snapshot is stale (the entry mutated when
	 * the holder/waiter was added).  Detect that path by an exact identity match
	 * (node, procno, request_id) against the granted holders:  8.A-verify the
	 * holder is present, drop the unused reservation, and succeed.  Calling
	 * reservation_promote here would grant_holder a SECOND copy (blind add).
	 * The remote-master path never has its holder pre-registered at the
	 * requester GRD, so it falls through to the revalidate below unchanged.
	 */
	for (int i = 0; i < entry->ngranted; i++) {
		if ((uint32)entry->holders[i].node_id == holder->node_id
			&& entry->holders[i].procno == holder->procno
			&& entry->holders[i].cluster_epoch == holder->cluster_epoch
			&& entry->holders[i].request_id == holder->request_id) {
			(void)cluster_grd_reservation_cancel(entry, holder);
			SpinLockRelease(&entry->lock);
			cluster_grd_entry_release(entry);
			return CLUSTER_GRD_ENTRY_OK;
		}
	}

	/*
	 * P2.3 revalidate target = no incompatible state ascended after the
	 * S3 snapshot.  reservation_create() bumps generation exactly once
	 * after gen_snapshot; any later mutation means the caller's reservation
	 * is no longer the sole in-flight state we can safely promote.
	 */
	revalidate_ok = (entry->generation == gen_snapshot + 1)
					&& !cluster_grd_entry_has_remote_holder(entry, self_node_id)
					&& !cluster_grd_entry_has_pending_waiter(entry)
					&& !cluster_grd_entry_has_pending_convert(entry);
	if (!revalidate_ok) {
		(void)cluster_grd_reservation_cancel(entry, holder);
		SpinLockRelease(&entry->lock);
		return CLUSTER_GRD_ENTRY_NOT_FOUND;
	}
	er = cluster_grd_reservation_promote(entry, holder);
	SpinLockRelease(&entry->lock);
	return er;
}

ClusterGrdEntryResult
cluster_grd_release_holder_by_id(const ClusterResId *resid, const ClusterGrdHolderId *holder)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult er;

	Assert(resid != NULL && holder != NULL);

	er = cluster_grd_entry_lookup_or_create(resid, false, &entry);
	if (er != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return CLUSTER_GRD_ENTRY_NOT_FOUND;

	SpinLockAcquire(&entry->lock);
	er = cluster_grd_entry_release_holder(entry, holder);
	SpinLockRelease(&entry->lock);
	/* spec-5.8 D1b — holder removed; refresh queued waiters' edges. */
	grd_wfg_resync_entry(resid, NULL, 0);
	return er;
}

ClusterGrdEntryResult
cluster_grd_cancel_reservation_by_id(const ClusterResId *resid, const ClusterGrdHolderId *holder)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult er;

	Assert(resid != NULL && holder != NULL);

	er = cluster_grd_entry_lookup_or_create(resid, false, &entry);
	if (er != CLUSTER_GRD_ENTRY_OK || entry == NULL)
		return CLUSTER_GRD_ENTRY_NOT_FOUND;

	SpinLockAcquire(&entry->lock);
	er = cluster_grd_reservation_cancel(entry, holder);
	SpinLockRelease(&entry->lock);
	return er;
}

/*
 * cluster_grd_cancel_waiter_by_id -- remove a queued REQUEST waiter by its full
 *	identity (node, procno, request_id).  spec-5.3 L11:  a local-master REQUEST
 *	that blocked on a conflict and then timed out must drop its still-queued
 *	waiter, else a later release drain could grant it to a requester that is
 *	already gone (phantom strong-mode holder).  Returns OK if a waiter was
 *	removed;  NOT_FOUND if none matched (the drain already granted it -> the
 *	caller accepts the grant that raced the timeout).
 */
/*
 * spec-5.9 D4 — shared REQUEST-waiter dequeue core.  When match_wait_seq is
 * true the queued waiter's spec-5.8 wait_seq must ALSO equal wait_seq, so a
 * stale / retransmitted CANCEL_WAIT that names a since-reused identity (same
 * 4-tuple, fresh wait_seq) is rejected as NOT_FOUND rather than dequeuing the
 * new waiter (Rule 8.A P0#2 ABA guard).  Touches only waiters[] — never
 * holders[] (the spec-2.24 P0#2 distinction: a victim's holder lock is dropped
 * by its own abort LockReleaseAll, never by this waiter-dequeue path).
 *
 *	Edge resync: the WFG edge key is the waiter 4-tuple (wait_seq is NOT part of
 *	it), so on a wait_seq MISMATCH we must NOT resync — that would tear down the
 *	reused waiter's edges.  The 4-tuple-only path (match_wait_seq == false, used
 *	by a backend cancelling its OWN current waiter on abort) keeps the original
 *	always-resync belt-and-suspenders behavior.
 */
static ClusterGrdEntryResult
grd_cancel_waiter_impl(const ClusterResId *resid, const ClusterGrdHolderId *holder, uint64 wait_seq,
					   bool match_wait_seq)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult er = CLUSTER_GRD_ENTRY_NOT_FOUND;

	Assert(resid != NULL && holder != NULL);

	if (cluster_grd_entry_lookup_or_create(resid, false, &entry) != CLUSTER_GRD_ENTRY_OK
		|| entry == NULL)
		return CLUSTER_GRD_ENTRY_NOT_FOUND;

	SpinLockAcquire(&entry->lock);
	for (int i = 0; i < entry->nwaiters; i++) {
		if ((uint32)entry->waiters[i].node_id == holder->node_id
			&& entry->waiters[i].procno == holder->procno
			&& entry->waiters[i].cluster_epoch == holder->cluster_epoch
			&& entry->waiters[i].request_id == holder->request_id
			&& (!match_wait_seq || entry->waiters[i].wait_seq == wait_seq)) {
			if (i < entry->nwaiters - 1)
				entry->waiters[i] = entry->waiters[entry->nwaiters - 1];
			memset(&entry->waiters[entry->nwaiters - 1], 0, sizeof(ClusterGrdWaiter));
			entry->nwaiters--;
			entry->generation++;
			er = CLUSTER_GRD_ENTRY_OK;
			break;
		}
	}
	SpinLockRelease(&entry->lock);
	cluster_grd_entry_release(entry);
	/* spec-5.8 D1b — the cancelled waiter left the queue; remove its edges. */
	if (er == CLUSTER_GRD_ENTRY_OK || !match_wait_seq)
		grd_wfg_resync_entry(resid, holder, 1);
	return er;
}

ClusterGrdEntryResult
cluster_grd_cancel_waiter_by_id(const ClusterResId *resid, const ClusterGrdHolderId *holder)
{
	return grd_cancel_waiter_impl(resid, holder, 0, false);
}

/*
 * spec-5.9 D4 — wait_seq-exact REQUEST-waiter dequeue (the CANCEL_WAIT path).
 * NOT_FOUND when no waiter matches the 4-tuple AND wait_seq (caller bumps
 * cancel_wait_stale_rejected on a stale CANCEL_WAIT).
 */
ClusterGrdEntryResult
cluster_grd_cancel_waiter_by_id_seq(const ClusterResId *resid, const ClusterGrdHolderId *holder,
									uint64 wait_seq)
{
	return grd_cancel_waiter_impl(resid, holder, wait_seq, true);
}

/*
 * spec-5.9 D4 — wait_seq-exact CONVERT dequeue (new primitive; the convert
 * queue had no cancel-by-id before).  A convert is a waiter on its
 * requested_mode whose WFG vertex id is convert_request_id, so the caller
 * passes holder->request_id = convert_request_id.  Matches (node_id, procno,
 * cluster_epoch, convert_request_id, wait_seq) and removes from converts[] only
 * — never a granted holder.  NOT_FOUND on any mismatch (ABA-safe like the
 * waiter variant).
 */
ClusterGrdEntryResult
cluster_grd_cancel_convert_by_id(const ClusterResId *resid, const ClusterGrdHolderId *holder,
								 uint64 wait_seq)
{
	ClusterGrdEntry *entry = NULL;
	ClusterGrdEntryResult er = CLUSTER_GRD_ENTRY_NOT_FOUND;

	Assert(resid != NULL && holder != NULL);

	if (cluster_grd_entry_lookup_or_create(resid, false, &entry) != CLUSTER_GRD_ENTRY_OK
		|| entry == NULL)
		return CLUSTER_GRD_ENTRY_NOT_FOUND;

	SpinLockAcquire(&entry->lock);
	for (int i = 0; i < entry->nconverts; i++) {
		if ((uint32)entry->converts[i].node_id == holder->node_id
			&& entry->converts[i].procno == holder->procno
			&& entry->converts[i].cluster_epoch == holder->cluster_epoch
			&& entry->converts[i].convert_request_id == holder->request_id
			&& entry->converts[i].wait_seq == wait_seq) {
			grd_convert_remove(entry, i);
			er = CLUSTER_GRD_ENTRY_OK;
			break;
		}
	}
	SpinLockRelease(&entry->lock);
	cluster_grd_entry_release(entry);
	/* The departed convert's WFG vertex id is convert_request_id (== holder->
	 * request_id), so resync removes exactly its edges.  Skip on mismatch. */
	if (er == CLUSTER_GRD_ENTRY_OK)
		grd_wfg_resync_entry(resid, holder, 1);
	return er;
}
