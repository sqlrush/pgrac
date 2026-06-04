/*-------------------------------------------------------------------------
 *
 * cluster_tt_local.c
 *	  pgrac cluster Undo TT local install helper.
 *
 *	  spec-3.1 D5 (NEW).
 *
 *	  Builds a provisional ClusterTTStatusKey for a local transaction
 *	  and installs it into the in-memory TT status overlay (D2).
 *
 *	  Provisional `tt_slot_id` mint:
 *	    - `cluster_tt_local_slot_seq` is a pg_atomic_uint32 in shmem;
 *	      init = 1; wraparound to 1 (value 0 reserved as invalid).
 *	    - This counter is NOT compatible with future spec-3.4 real
 *	      undo-segment TT slot allocation.  spec-3.4 ship MUST first
 *	      cluster_tt_status_flush_all() to clear all provisional ids
 *	      before swapping to real TT slot allocator output.
 *
 *	  Caller: D6 xact.c commit/abort hook.  spec-3.1 v0.4 N7 requires
 *	  D6 to additionally re-`cluster_tt_status_lookup_exact` the just-
 *	  installed key in every build to prove the D5/D6 path is wired
 *	  (not dead helper); assert builds add an assertion on top of the
 *	  runtime counter evidence.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.1-cluster-xid-status-foundation.md
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tt_local.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/memutils.h"

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_durable.h" /* spec-3.11 D4 durable commit */
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_tt_slot.h"		 /* spec-3.4b D4 real binding */
#include "cluster/cluster_undo_record_api.h" /* spec-3.12 D2b cluster_undo_tt_rollover_locked */
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tt_status_hint.h"		/* spec-3.2 D4 wire emit append */
#include "cluster/storage/cluster_undo_alloc.h" /* cluster_undo_active_segment_for_node_or_create */

#ifdef USE_PGRAC_CLUSTER

typedef struct ClusterTTLocalShmem {
	pg_atomic_uint32 slot_seq; /* monotonic provisional tt_slot_id */
} ClusterTTLocalShmem;

static ClusterTTLocalShmem *ClusterTTLocalState = NULL;

/* ------------------------------------------------------------ */
/* shmem layout                                                 */
/* ------------------------------------------------------------ */

Size
cluster_tt_local_shmem_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return 0;
	return MAXALIGN(sizeof(ClusterTTLocalShmem));
}

void
cluster_tt_local_shmem_init(void)
{
	bool found;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return;

	ClusterTTLocalState = (ClusterTTLocalShmem *)ShmemInitStruct(
		"ClusterTTLocalState", MAXALIGN(sizeof(ClusterTTLocalShmem)), &found);
	if (!found) {
		/* Start at 1; value 0 reserved as invalid sentinel. */
		pg_atomic_init_u32(&ClusterTTLocalState->slot_seq, 1);
	}
}

static const ClusterShmemRegion cluster_tt_local_region = {
	.name = "pgrac cluster tt local seq",
	.size_fn = cluster_tt_local_shmem_size,
	.init_fn = cluster_tt_local_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_tt_local",
	.reserved_flags = 0,
};

void
cluster_tt_local_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_tt_local_region);
}

/* ------------------------------------------------------------ */
/* private helpers                                              */
/* ------------------------------------------------------------ */

/*
 * spec-3.1 D5 monotonic provisional `tt_slot_id` mint
 * (mint_provisional_tt_slot_id) was removed in spec-3.4b D4 / F7:
 * production install paths now route exclusively through the real
 * allocator binding below (cluster_tt_local_get_or_create_binding).
 * The pg_atomic_uint32 slot_seq counter still lives in shmem for
 * the peek introspection used by test_cluster_tt_status (D9 T17).
 */


/* ------------------------------------------------------------ */
/* spec-3.4b D4 — xact-local real TT binding (F11)              */
/* ------------------------------------------------------------ */

/*
 * Backend-local binding state.  spec-3.4b needed a single binding because
 * subxacts were still gated off.  spec-3.5 can have parent + child xids
 * live at the same time, so keep a small backend-local binding table and
 * free all entries at top-level xact end via cluster_tt_local_reset_binding().
 * Postmaster restart clears shmem TT slot state and any next backend starts
 * fresh.
 *
 * F11 key invariant: every ITL slot stamped by this xact (D5) AND the
 * commit / abort install_status call (this file) MUST produce the same
 * ClusterTTStatusKey bytes.  Achieved by routing both through
 * cluster_tt_local_get_or_create_binding() / build_local_key().
 */
typedef struct ClusterTTLocalBinding {
	TransactionId top_xid; /* InvalidTransactionId == no binding */
	uint32 segment_id;
	uint16 slot_offset;
	/* spec-3.12 D2b: wrap counter captured at bind time.  The slot is held
	 * ACTIVE for the whole xact (never recycled while ACTIVE), so wrap cannot
	 * change before commit -- capturing it here lets precommit stamp the durable
	 * TT slot without re-reading the shmem allocator, which may have rolled this
	 * segment away (cluster_tt_slot_get_wrap would then ERROR on the stale id). */
	uint16 wrap;
	uint32 cluster_epoch; /* snapshot at bind time */
} ClusterTTLocalBinding;

static ClusterTTLocalBinding *cluster_tt_local_bindings = NULL;
static uint32 cluster_tt_local_binding_count = 0;
static uint32 cluster_tt_local_binding_capacity = 0;

static int
cluster_tt_local_find_binding(TransactionId xid)
{
	uint32 i;

	for (i = 0; i < cluster_tt_local_binding_count; i++) {
		if (cluster_tt_local_bindings[i].top_xid == xid)
			return (int)i;
	}
	return -1;
}

static ClusterTTLocalBinding *
cluster_tt_local_append_binding(void)
{
	if (cluster_tt_local_bindings == NULL) {
		MemoryContext oldcxt;

		Assert(TopTransactionContext != NULL);
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		cluster_tt_local_binding_capacity = 8;
		cluster_tt_local_bindings = (ClusterTTLocalBinding *)palloc0(
			sizeof(ClusterTTLocalBinding) * cluster_tt_local_binding_capacity);
		MemoryContextSwitchTo(oldcxt);
	} else if (cluster_tt_local_binding_count == cluster_tt_local_binding_capacity) {
		cluster_tt_local_binding_capacity *= 2;
		cluster_tt_local_bindings = (ClusterTTLocalBinding *)repalloc(
			cluster_tt_local_bindings,
			sizeof(ClusterTTLocalBinding) * cluster_tt_local_binding_capacity);
	}

	memset(&cluster_tt_local_bindings[cluster_tt_local_binding_count], 0,
		   sizeof(ClusterTTLocalBinding));
	cluster_tt_local_bindings[cluster_tt_local_binding_count].top_xid = InvalidTransactionId;
	return &cluster_tt_local_bindings[cluster_tt_local_binding_count++];
}


/*
 * cluster_tt_local_get_or_create_binding
 *
 *	Public entry point for heap DML (D5).  On first call within an
 *	xact, allocates a real TT slot via cluster_tt_slot_alloc().  On
 *	subsequent calls within the same xact, returns the cached binding.
 *
 *	Returns true when binding is available (caller proceeds with UBA
 *	encode); returns false when cluster mode is disabled or xid is not
 *	a normal transaction id (caller falls back to PG-native silent
 *	per spec-3.4a A6).  F7: NO provisional fallback on allocator
 *	failure -- raises ERROR fail-closed.
 *
 *	Caller MUST NOT be inside a critical section: this function may
 *	take LWLocks (allocator) and call cluster_undo_segment_allocate
 *	(which emits WAL on first segment creation).
 */
bool
cluster_tt_local_get_or_create_binding(TransactionId top_xid, uint32 *out_segment_id,
									   uint16 *out_slot_offset, uint32 *out_tt_slot_id)
{
	if (!cluster_enabled || cluster_node_id < 0)
		return false;
	if (!TransactionIdIsNormal(top_xid))
		return false;

	{
		int idx = cluster_tt_local_find_binding(top_xid);

		if (idx >= 0) {
			const ClusterTTLocalBinding *b = &cluster_tt_local_bindings[idx];

			/* Idempotent reuse. */
			*out_segment_id = b->segment_id;
			*out_slot_offset = b->slot_offset;
			*out_tt_slot_id = cluster_tt_slot_offset_to_id(b->slot_offset);
			return true;
		}
	}

	{
		ClusterTTLocalBinding *b;
		uint32 seg;
		uint16 off;
		bool retained_pressure = false;

		/*
		 * spec-3.12 D2b: allocate on the node's CURRENT TT segment (which may
		 * have been rolled forward by an earlier retention rollover); only the
		 * first-ever bind falls back to the fixed spec-3.4b segment id (and
		 * lazily provisions its file).
		 */
		seg = cluster_tt_slot_current_segment(cluster_node_id);
		if (seg == 0)
			seg = cluster_undo_active_segment_for_node_or_create(cluster_node_id);

		off = cluster_tt_slot_alloc_ext(seg, top_xid, &retained_pressure);
		if (off == INVALID_TT_SLOT_OFFSET) {
			if (!retained_pressure)
				/* All 48 slots ACTIVE -- genuine in-flight concurrency limit. */
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("cluster TT slot allocator exhausted on segment %u (48 slots full)",
								seg),
						 errhint("All concurrent xacts on this node hold ACTIVE slots; retry after "
								 "shorter transactions commit or abort.")));

			/*
			 * spec-3.12 D2b / C3b: retained COMMITTED slots (a long reader holds
			 * the horizon) fill the active segment.  Roll over to a fresh undo
			 * segment + rebind the allocator instead of failing the writer.
			 */
			{
				bool at_hard_cap = false;
				uint32 new_seg
					= cluster_undo_tt_rollover_locked(cluster_node_id, seg, &at_hard_cap);

				if (new_seg == 0)
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("cluster TT slot allocator: retention rollover failed for "
									"segment %u (%s)",
									seg,
									at_hard_cap ? "undo segment pool hard cap reached"
												: "undo segment autoextend failed"),
							 errhint("A long-running reader is retaining committed undo; end it, "
									 "or raise cluster.undo_segments_max_per_instance.")));

				seg = new_seg;
				off = cluster_tt_slot_alloc(seg, top_xid);
				if (off == INVALID_TT_SLOT_OFFSET)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("cluster TT slot allocator: fresh rollover segment %u already "
									"full",
									seg)));
			}
		}

		b = cluster_tt_local_append_binding();
		b->top_xid = top_xid;
		b->segment_id = seg;
		b->slot_offset = off;
		b->wrap = cluster_tt_slot_get_wrap(seg, off); /* D2b: capture before any rollover */
		b->cluster_epoch = (uint32)cluster_epoch_get_current();

		*out_segment_id = seg;
		*out_slot_offset = off;
		*out_tt_slot_id = cluster_tt_slot_offset_to_id(off);
		return true;
	}
}

/*
 * cluster_tt_local_peek_binding
 *
 *	Read-only accessor: returns true + fills outs if a binding exists
 *	for `top_xid`, false otherwise.  Used by install_status to skip the
 *	overlay install when no DML ever ran (DDL-only xact path).
 */
bool
cluster_tt_local_peek_binding(TransactionId top_xid, uint32 *out_segment_id,
							  uint16 *out_slot_offset, uint32 *out_tt_slot_id,
							  uint32 *out_cluster_epoch, uint16 *out_wrap)
{
	int idx;
	const ClusterTTLocalBinding *b;

	idx = cluster_tt_local_find_binding(top_xid);
	if (idx < 0 || !TransactionIdIsValid(top_xid))
		return false;

	b = &cluster_tt_local_bindings[idx];
	*out_segment_id = b->segment_id;
	*out_slot_offset = b->slot_offset;
	*out_tt_slot_id = cluster_tt_slot_offset_to_id(b->slot_offset);
	*out_cluster_epoch = b->cluster_epoch;
	if (out_wrap != NULL) /* spec-3.12 D2b: wrap captured at bind time */
		*out_wrap = b->wrap;
	return true;
}

bool
cluster_tt_local_has_binding(TransactionId top_xid)
{
	return TransactionIdIsValid(top_xid) && cluster_tt_local_find_binding(top_xid) >= 0;
}

/*
 * cluster_tt_local_reset_binding
 *
 *	Clear the backend-local TT slot binding table.  Idempotent.  Does NOT
 *	transition the shmem allocator slots: spec-3.12 D2 moved the end-of-xact
 *	slot state machine (commit -> retain COMMITTED, abort -> ABORTED) into
 *	cluster_tt_local_finish_bindings(), which runs just before this.  The
 *	binding-table array itself lives in TopTransactionContext and is reclaimed
 *	when that context resets at xact end, so dropping the pointer is enough.
 */
void
cluster_tt_local_reset_binding(void)
{
	cluster_tt_local_bindings = NULL;
	cluster_tt_local_binding_count = 0;
	cluster_tt_local_binding_capacity = 0;
}

/*
 * cluster_tt_local_finish_bindings -- spec-3.12 D2.
 *
 *	Transition every backend-local binding's shmem TT slot at top-level xact
 *	end: commit retains the slot as COMMITTED + commit_scn (so the durable
 *	segment-header TT slot is not overwritten while a reader needs it); abort
 *	marks it ABORTED (immediately recyclable, C7).  Then clears the binding
 *	table.  Child/subxact bindings are transitioned here too; their overlay
 *	entries keep exact keys because local_xid is part of ClusterTTStatusKey.
 */
static void
cluster_tt_local_finish_bindings(bool committed, SCN commit_scn)
{
	uint32 i;

	for (i = 0; i < cluster_tt_local_binding_count; i++) {
		const ClusterTTLocalBinding *b = &cluster_tt_local_bindings[i];

		if (!TransactionIdIsValid(b->top_xid))
			continue;
		if (committed)
			cluster_tt_slot_mark_committed(b->segment_id, b->slot_offset, b->top_xid, commit_scn);
		else
			cluster_tt_slot_mark_aborted(b->segment_id, b->slot_offset, b->top_xid);
	}

	cluster_tt_local_reset_binding();
}

/*
 * build_local_key -- compose ClusterTTStatusKey for `xid`.
 *
 *	Returns true with the key populated when an xact-local binding
 *	exists for `xid` (F11 real allocator path); returns false otherwise
 *	and leaves `*out` zeroed.  spec-3.4b F7: production code paths MUST
 *	NOT take a provisional id when the binding is absent -- this would
 *	create a real/provisional key collision risk.  Callers should
 *	silently skip install when build_local_key returns false (the
 *	matching DML never stamped an ITL slot, so no overlay entry is
 *	needed).
 */
static bool
build_local_key(TransactionId xid, ClusterTTStatusKey *out)
{
	uint32 seg;
	uint16 off;
	uint32 tt_id;
	uint32 epoch;

	memset(out, 0, sizeof(*out));

	if (!cluster_tt_local_peek_binding(xid, &seg, &off, &tt_id, &epoch, NULL))
		return false;

	out->origin_node_id = (uint16)cluster_node_id;
	out->undo_segment_id = (uint16)seg;
	out->tt_slot_id = tt_id;
	out->cluster_epoch = epoch;
	out->local_xid = xid;
	/* _reserved + _reserved2 already zero from memset. */
	return true;
}

/*
 * install_status -- common path for commit / abort install + N7
 * self-consumer lookup.
 *
 * spec-3.3 D6: commit_scn flows in from the caller. For COMMITTED status
 * the caller (cluster_tt_local_record_commit) must pass a real SCN that
 * came from cluster_scn_advance_for_commit(); for ABORTED status the
 * caller passes InvalidScn (abort has no commit_scn).
 */
static void
install_status(TransactionId xid, ClusterTTStatus status, SCN commit_scn)
{
	ClusterTTStatusKey key;
	bool installed;

	if (!cluster_enabled || cluster_node_id < 0)
		return;
	if (!TransactionIdIsNormal(xid))
		return;

	/*
	 * spec-3.4b D4 + F7 + F11: only install when an xact-local binding
	 * exists.  No binding ⇒ this xact never stamped an ITL slot
	 * (DDL-only or read-only); no overlay entry is needed and the
	 * provisional fallback path is forbidden in production.
	 */
	if (!build_local_key(xid, &key))
		return;

	/*
	 * spec-3.3 D6 (L181 chain step 2): install in-memory overlay entry
	 * with the real commit_scn (or InvalidScn on abort). The overlay
	 * carries commit_scn to (a) self-consumer N7 lookup and (b) the wire
	 * emit path which will ship it to peers in V2 hints (D8).
	 */
	installed = cluster_tt_status_install_local(&key, status, commit_scn);
	if (!installed)
		return;

	/*
	 * spec-3.1 v0.4 N7 self-consumer:  re-lookup to prove the just-installed key
	 * is reachable + bump self_consumer_hit_count (D9 T8 + D10 L2).
	 *
	 * P0 perf hardening: this is now a DEBUG-ONLY invariant check.  In every
	 * commit it cost an extra HTAB lookup + LW_SHARED on the hot path; the
	 * production install path does not need to re-read its own write.  Assert
	 * builds still fail fast if the install cannot read its own key, and the
	 * self_consumer_hit_count counter is now a debug-build signal.
	 */
#ifdef USE_ASSERT_CHECKING
	{
		ClusterTTStatusResult res;
		bool hit = cluster_tt_status_lookup_exact(&key, &res);

		Assert(hit && res.authoritative && res.status == status);
		if (hit && res.authoritative && res.status == status)
			cluster_tt_status_bump_self_consumer_hit();
	}
#endif

	/*
	 * spec-3.2 D4 + spec-3.3 D6/D8 (L181 chain step 4): cross-node TT
	 * status hint wire emit. Uses the EXACT key just minted + installed
	 * (HC184 — no raw-xid rebuild); commit_scn flows through so peers
	 * see the real value via V2 wire. Fire-and-forget; emit_mode =
	 * disabled is a no-op inside the function. commit/abort 对称
	 * (install_status is called from both record_commit + record_abort;
	 * abort path passes InvalidScn).
	 */
	cluster_tt_status_hint_emit(&key, status, commit_scn);
}

/* ------------------------------------------------------------ */
/* public API                                                   */
/* ------------------------------------------------------------ */

void
cluster_tt_local_precommit_durable_finish(TransactionId xid, SCN commit_scn)
{
	uint32 segment_id;
	uint16 slot_offset;
	uint32 tt_slot_id;
	uint32 cluster_epoch;
	uint16 wrap;

	/*
	 * spec-3.11 D4 / C1: durably stamp commit_scn on this xact's TT slot in the
	 * undo segment header, emitting XLOG_UNDO_TT_SLOT_COMMIT BEFORE the commit
	 * record (caller is the xact.c pre-commit hook; the commit record's flush
	 * makes it durable -- no independent fsync, C10).  No binding = DDL-only /
	 * read-only xact that never allocated a slot -> nothing durable.  Runs
	 * before reset_binding() (post-commit in record_commit), so the binding is
	 * still present.  C1b: this only stamps commit_scn; whether the xact
	 * actually committed is still decided by the commit record / CLOG.
	 */
	if (!cluster_tt_local_peek_binding(xid, &segment_id, &slot_offset, &tt_slot_id, &cluster_epoch,
									   &wrap))
		return;

	/*
	 * spec-3.12 D2b: use the wrap captured at bind time, NOT a fresh
	 * cluster_tt_slot_get_wrap() -- a peer backend may have rolled this node's
	 * TT allocator to a new segment since this xact bound, which would make a
	 * fresh read of the (now stale) segment ERROR.  The slot is held ACTIVE for
	 * the whole xact, so its wrap cannot have changed.
	 */
	cluster_tt_slot_durable_commit(segment_id, slot_offset, xid, wrap, commit_scn);
}

void
cluster_tt_local_record_commit(TransactionId xid, SCN commit_scn)
{
	install_status(xid, CLUSTER_TT_STATUS_COMMITTED, commit_scn);
	/*
	 * spec-3.12 D2: retain the binding's TT slot as COMMITTED + commit_scn
	 * (replaces the spec-3.4b commit-time free).  Retention keeps the durable
	 * segment-header TT slot at this offset addressable -- so a reader whose
	 * read_scn is at/below commit_scn can still resolve it by-xid -- until the
	 * horizon advances past commit_scn.  The overlay entry's key references the
	 * same slot offset, but ClusterTTStatusKey includes local_xid, so a future
	 * xact that recycles the offset produces a different key.
	 */
	cluster_tt_local_finish_bindings(true /* committed */, commit_scn);
}

void
cluster_tt_local_record_abort(TransactionId xid)
{
	install_status(xid, CLUSTER_TT_STATUS_ABORTED, InvalidScn);
	/* spec-3.12 D2 / C7: aborted slots are immediately recyclable. */
	cluster_tt_local_finish_bindings(false /* aborted */, InvalidScn);
}

/*
 * cluster_tt_local_record_active (spec-3.4d D4 / F3 P0):
 *
 *	Install local CLUSTER_TT_STATUS_IN_PROGRESS(== "ACTIVE" in spec
 *	v0.2 §6.4 wording) + emit cross-node TT_STATUS_HINT for `xid`.
 *	Distinguishes from record_commit/abort:
 *	  - does NOT reset the xact-local TT binding (xact still running)
 *	  - commit_scn = InvalidScn (lock-only carries no commit ordering)
 *	  - idempotent (overlay install overwrites;  hint emit fire-and-forget)
 *
 *	Called from heap_lock_tuple / heap_lock_updated_tuple_rec AFTER
 *	the lock-only ITL slot is stamped (under buffer content lock) and
 *	AFTER xact-local binding exists (caller must have invoked
 *	cluster_tt_local_get_or_create_binding first).
 */
void
cluster_tt_local_record_active(TransactionId xid)
{
	install_status(xid, CLUSTER_TT_STATUS_IN_PROGRESS, InvalidScn);
	/* NOTE: NOT calling cluster_tt_local_reset_binding() — xact is still
	 * running and may stamp additional lock-only or data ITL slots. */
}

uint32
cluster_tt_local_slot_seq_peek(void)
{
	if (ClusterTTLocalState == NULL)
		return 0;
	return pg_atomic_read_u32(&ClusterTTLocalState->slot_seq);
}

#else /* !USE_PGRAC_CLUSTER */

Size
cluster_tt_local_shmem_size(void)
{
	return 0;
}

void
cluster_tt_local_shmem_init(void)
{}

void
cluster_tt_local_shmem_register(void)
{}

void
cluster_tt_local_record_commit(TransactionId xid, SCN commit_scn)
{
	(void)xid;
	(void)commit_scn;
}

void
cluster_tt_local_record_abort(TransactionId xid)
{
	(void)xid;
}

void
cluster_tt_local_record_active(TransactionId xid)
{
	(void)xid;
}

uint32
cluster_tt_local_slot_seq_peek(void)
{
	return 0;
}

bool
cluster_tt_local_get_or_create_binding(TransactionId top_xid, uint32 *out_segment_id,
									   uint16 *out_slot_offset, uint32 *out_tt_slot_id)
{
	(void)top_xid;
	(void)out_segment_id;
	(void)out_slot_offset;
	(void)out_tt_slot_id;
	return false;
}

bool
cluster_tt_local_peek_binding(TransactionId top_xid, uint32 *out_segment_id,
							  uint16 *out_slot_offset, uint32 *out_tt_slot_id,
							  uint32 *out_cluster_epoch, uint16 *out_wrap)
{
	(void)top_xid;
	(void)out_segment_id;
	(void)out_slot_offset;
	(void)out_tt_slot_id;
	(void)out_cluster_epoch;
	(void)out_wrap;
	return false;
}

bool
cluster_tt_local_has_binding(TransactionId top_xid)
{
	(void)top_xid;
	return false;
}

void
cluster_tt_local_reset_binding(void)
{}

#endif /* USE_PGRAC_CLUSTER */
