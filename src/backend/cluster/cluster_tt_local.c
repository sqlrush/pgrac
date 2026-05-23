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
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tt_status_hint.h" /* spec-3.2 D4 wire emit append */

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
 * mint_provisional_tt_slot_id -- monotonic counter; 0 reserved invalid,
 * wraparound -> 1 (spec-3.1 v0.4 N6).
 */
static uint32
mint_provisional_tt_slot_id(void)
{
	uint32 v;

	if (ClusterTTLocalState == NULL)
		return 0;

	v = pg_atomic_fetch_add_u32(&ClusterTTLocalState->slot_seq, 1);
	if (v == 0) {
		/* Just consumed value 0; mint next and skip. */
		v = pg_atomic_fetch_add_u32(&ClusterTTLocalState->slot_seq, 1);
		if (v == 0)
			v = 1;
	}
	return v;
}

/*
 * build_local_key -- compose a provisional ClusterTTStatusKey for the
 * given xid using local origin / current epoch / minted tt_slot_id.
 */
static void
build_local_key(TransactionId xid, ClusterTTStatusKey *out)
{
	memset(out, 0, sizeof(*out));
	out->origin_node_id = (uint16)cluster_node_id;
	out->undo_segment_id = 0;
	out->tt_slot_id = mint_provisional_tt_slot_id();
	out->cluster_epoch = (uint32)cluster_epoch_get_current();
	out->local_xid = xid;
	/* _reserved + _reserved2 already zero from memset. */
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

	build_local_key(xid, &key);

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
}

void
cluster_tt_local_record_abort(TransactionId xid)
{
	install_status(xid, CLUSTER_TT_STATUS_ABORTED, InvalidScn);
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

#endif /* USE_PGRAC_CLUSTER */
