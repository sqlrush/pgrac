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

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_tt_slot.h"			/* spec-3.4b D4 real binding */
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tt_status_hint.h" /* spec-3.2 D4 wire emit append */
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
 * Backend-local binding state.  Lives in static module storage so it
 * survives across DML statements within the same xact but is wiped on
 * end-of-xact via cluster_tt_local_reset_binding().  Postmaster restart
 * clears shmem TT slot state and any next backend starts fresh.
 *
 * F11 key invariant: every ITL slot stamped by this xact (D5) AND the
 * commit / abort install_status call (this file) MUST produce the same
 * ClusterTTStatusKey bytes.  Achieved by routing both through
 * cluster_tt_local_get_or_create_binding() / build_local_key().
 */
typedef struct ClusterTTLocalBinding
{
	TransactionId top_xid;	/* InvalidTransactionId == no binding */
	uint32 segment_id;
	uint16 slot_offset;
	uint16 _pad;
	uint32 cluster_epoch;	/* snapshot at bind time */
} ClusterTTLocalBinding;

static ClusterTTLocalBinding cluster_tt_local_binding = {
	InvalidTransactionId, 0, 0, 0, 0
};


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
cluster_tt_local_get_or_create_binding(TransactionId top_xid,
									   uint32 *out_segment_id,
									   uint16 *out_slot_offset,
									   uint32 *out_tt_slot_id)
{
	uint32 seg;
	uint16 off;

	if (!cluster_enabled || cluster_node_id < 0)
		return false;
	if (!TransactionIdIsNormal(top_xid))
		return false;

	if (cluster_tt_local_binding.top_xid == top_xid)
	{
		/* Idempotent reuse. */
		*out_segment_id = cluster_tt_local_binding.segment_id;
		*out_slot_offset = cluster_tt_local_binding.slot_offset;
		*out_tt_slot_id = cluster_tt_slot_offset_to_id(cluster_tt_local_binding.slot_offset);
		return true;
	}

	if (TransactionIdIsValid(cluster_tt_local_binding.top_xid))
	{
		/*
		 * Stale binding from a prior xact that did not call reset
		 * (should not happen in production -- xact.c always calls
		 * record_commit/abort -> reset on commit/abort).  Best-effort
		 * cleanup: free the orphaned slot then re-allocate.
		 */
		elog(WARNING,
			 "cluster_tt_local: stale binding for xid %u while allocating for xid %u; "
			 "freeing orphan",
			 cluster_tt_local_binding.top_xid, top_xid);
		cluster_tt_slot_free(cluster_tt_local_binding.segment_id,
							 cluster_tt_local_binding.slot_offset);
		cluster_tt_local_binding.top_xid = InvalidTransactionId;
	}

	seg = cluster_undo_active_segment_for_node_or_create(cluster_node_id);
	off = cluster_tt_slot_alloc(seg, top_xid);
	if (off == INVALID_TT_SLOT_OFFSET)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cluster TT slot allocator exhausted on segment %u (48 slots full)",
						seg),
				 errhint("All concurrent xacts on this node hold ACTIVE slots; retry after "
						 "shorter transactions commit or abort.")));

	cluster_tt_local_binding.top_xid = top_xid;
	cluster_tt_local_binding.segment_id = seg;
	cluster_tt_local_binding.slot_offset = off;
	cluster_tt_local_binding.cluster_epoch = (uint32) cluster_epoch_get_current();

	*out_segment_id = seg;
	*out_slot_offset = off;
	*out_tt_slot_id = cluster_tt_slot_offset_to_id(off);
	return true;
}


/*
 * cluster_tt_local_peek_binding
 *
 *	Read-only accessor: returns true + fills outs if a binding exists
 *	for `top_xid`, false otherwise.  Used by install_status to skip the
 *	overlay install when no DML ever ran (DDL-only xact path).
 */
bool
cluster_tt_local_peek_binding(TransactionId top_xid,
							  uint32 *out_segment_id,
							  uint16 *out_slot_offset,
							  uint32 *out_tt_slot_id,
							  uint32 *out_cluster_epoch)
{
	if (cluster_tt_local_binding.top_xid != top_xid
		|| !TransactionIdIsValid(top_xid))
		return false;

	*out_segment_id = cluster_tt_local_binding.segment_id;
	*out_slot_offset = cluster_tt_local_binding.slot_offset;
	*out_tt_slot_id = cluster_tt_slot_offset_to_id(cluster_tt_local_binding.slot_offset);
	*out_cluster_epoch = cluster_tt_local_binding.cluster_epoch;
	return true;
}


/*
 * cluster_tt_local_reset_binding
 *
 *	Free the TT slot and clear the binding.  Idempotent.  Called from
 *	record_commit / record_abort after install_status fires, and as a
 *	safety net from XactCallback in case those paths are skipped.
 */
void
cluster_tt_local_reset_binding(void)
{
	if (!TransactionIdIsValid(cluster_tt_local_binding.top_xid))
		return;

	cluster_tt_slot_free(cluster_tt_local_binding.segment_id,
						 cluster_tt_local_binding.slot_offset);

	cluster_tt_local_binding.top_xid = InvalidTransactionId;
	cluster_tt_local_binding.segment_id = 0;
	cluster_tt_local_binding.slot_offset = 0;
	cluster_tt_local_binding.cluster_epoch = 0;
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

	if (!cluster_tt_local_peek_binding(xid, &seg, &off, &tt_id, &epoch))
		return false;

	out->origin_node_id = (uint16) cluster_node_id;
	out->undo_segment_id = (uint16) seg;
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
	ClusterTTStatusResult res;
	bool hit;

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
	cluster_tt_status_install_local(&key, status, commit_scn);

	/*
	 * spec-3.1 v0.4 N7 self-consumer:  immediately re-lookup to prove
	 * the just-installed key is reachable + bump the
	 * self_consumer_hit_count counter (D9 T8 + D10 L2 covers).  This runs
	 * in every build; assert builds additionally fail fast if the local
	 * install path cannot read its own key.
	 */
	hit = cluster_tt_status_lookup_exact(&key, &res);
	Assert(hit && res.authoritative && res.status == status);
	if (hit && res.authoritative && res.status == status)
		cluster_tt_status_bump_self_consumer_hit();

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
cluster_tt_local_record_commit(TransactionId xid, SCN commit_scn)
{
	install_status(xid, CLUSTER_TT_STATUS_COMMITTED, commit_scn);
	/*
	 * spec-3.4b D4 / F11: free the binding's TT slot once the overlay
	 * entry has been installed.  Subsequent xacts on this backend will
	 * allocate a fresh binding via get_or_create_binding().  The overlay
	 * entry's key references the just-freed slot offset, but the
	 * ClusterTTStatusKey includes local_xid, so a future xact that
	 * recycles the same slot offset will produce a different key.
	 */
	cluster_tt_local_reset_binding();
}

void
cluster_tt_local_record_abort(TransactionId xid)
{
	install_status(xid, CLUSTER_TT_STATUS_ABORTED, InvalidScn);
	cluster_tt_local_reset_binding();
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

uint32
cluster_tt_local_slot_seq_peek(void)
{
	return 0;
}

bool
cluster_tt_local_get_or_create_binding(TransactionId top_xid,
									   uint32 *out_segment_id,
									   uint16 *out_slot_offset,
									   uint32 *out_tt_slot_id)
{
	(void) top_xid;
	(void) out_segment_id;
	(void) out_slot_offset;
	(void) out_tt_slot_id;
	return false;
}

bool
cluster_tt_local_peek_binding(TransactionId top_xid,
							  uint32 *out_segment_id,
							  uint16 *out_slot_offset,
							  uint32 *out_tt_slot_id,
							  uint32 *out_cluster_epoch)
{
	(void) top_xid;
	(void) out_segment_id;
	(void) out_slot_offset;
	(void) out_tt_slot_id;
	(void) out_cluster_epoch;
	return false;
}

void
cluster_tt_local_reset_binding(void)
{}

#endif /* USE_PGRAC_CLUSTER */
