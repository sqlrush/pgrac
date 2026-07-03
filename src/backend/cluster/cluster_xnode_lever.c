/*-------------------------------------------------------------------------
 *
 * cluster_xnode_lever.c
 *	  pgrac spec-6.12 cross-node performance lever runtime.
 *
 *	  See cluster_xnode_lever.h for the contract.  Two pieces live here:
 *
 *	  1. The "pgrac cluster xnode lever" shmem region: append-only
 *	     monotonic counters per 6.12 wave, dumped through the
 *	     cluster_dump_state 'xnode_lever' category.
 *
 *	  2. The wave-c backend-local terminal-outcome memo: a small
 *	     direct-mapped array keyed by the exact ClusterTTStatusKey and
 *	     stamped with the installing top-level transaction's lxid.  Only
 *	     TERMINAL outcomes enter (COMMITTED with valid SCN / ABORTED);
 *	     a probe hit replays an immutable fact the same transaction
 *	     already obtained from an authoritative TT lookup, so the memo
 *	     can never widen visibility beyond what the TT already answered
 *	     (rule 8.A).  Everything else -- IN_PROGRESS, SUBCOMMITTED,
 *	     CLEANED_OUT, UNKNOWN, non-authoritative -- always re-resolves.
 *
 *	  NOTES
 *	    - The memo is deliberately NOT in shared memory: no locks, no
 *	      cross-backend invalidation surface; correctness rests only on
 *	      terminal-outcome immutability + same-transaction scoping.
 *	    - Collisions overwrite (it is a memo, not an authority).
 *	    - With cluster.page_scn_shortcut off both entry points return
 *	      immediately; with cluster.xnode_profile also off the counters
 *	      never tick (byte-identical hot path).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_xnode_lever.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_xnode_lever.h"
#include "storage/proc.h"
#include "storage/shmem.h"

ClusterXnodeLeverShared *ClusterXnodeLeverCtl = NULL;

/* ---------------- shmem region ---------------- */

Size
cluster_xnode_lever_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterXnodeLeverShared));
}

void
cluster_xnode_lever_shmem_init(void)
{
	bool found;

	ClusterXnodeLeverCtl = (ClusterXnodeLeverShared *)ShmemInitStruct(
		"pgrac cluster xnode lever", cluster_xnode_lever_shmem_size(), &found);
	if (!found) {
		pg_atomic_init_u64(&ClusterXnodeLeverCtl->c_resolve_count, 0);
		pg_atomic_init_u64(&ClusterXnodeLeverCtl->c_tt_lookup_count, 0);
		pg_atomic_init_u64(&ClusterXnodeLeverCtl->c_memo_hit_count, 0);
		pg_atomic_init_u64(&ClusterXnodeLeverCtl->c_memo_install_count, 0);
		pg_atomic_init_u64(&ClusterXnodeLeverCtl->c_stamp_cached_seen_count, 0);
		pg_atomic_init_u64(&ClusterXnodeLeverCtl->c_stamp_contradicted_count, 0);
		pg_atomic_init_u64(&ClusterXnodeLeverCtl->a_downgrade_count, 0);
		pg_atomic_init_u64(&ClusterXnodeLeverCtl->a_downgrade_refused_count, 0);
		pg_atomic_init_u64(&ClusterXnodeLeverCtl->a_fwd_oneshot_count, 0);
		pg_atomic_init_u64(&ClusterXnodeLeverCtl->e1_drain_count, 0);
		pg_atomic_init_u64(&ClusterXnodeLeverCtl->e1_grant_count, 0);
		pg_atomic_init_u64(&ClusterXnodeLeverCtl->e1_invariant_violation_count, 0);
	}
}

static const ClusterShmemRegion cluster_xnode_lever_region = {
	.name = "pgrac cluster xnode lever",
	.size_fn = cluster_xnode_lever_shmem_size,
	.init_fn = cluster_xnode_lever_shmem_init,
	.lwlock_count = 0, /* atomics only */
	.owner_subsys = "cluster_xnode_lever",
	.reserved_flags = 0,
};

void
cluster_xnode_lever_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_xnode_lever_region);
}

/* ---------------- wave-c terminal-outcome memo ---------------- */

/* Direct-mapped, power of two; ~14kB of backend-local static storage. */
#define CLUSTER_VIS_MEMO_SLOTS 256

typedef struct ClusterVisMemoEntry {
	ClusterTTStatusKey key;
	LocalTransactionId lxid; /* installing top-level transaction */
	uint8 status;			 /* COMMITTED or ABORTED only */
	SCN commit_scn;
} ClusterVisMemoEntry;

static ClusterVisMemoEntry cluster_vis_memo[CLUSTER_VIS_MEMO_SLOTS];

static inline bool
vis_memo_active(void)
{
	return cluster_page_scn_shortcut && MyProc != NULL && LocalTransactionIdIsValid(MyProc->lxid);
}

static inline uint32
vis_memo_slot(const ClusterTTStatusKey *key)
{
	uint32 h;

	h = ((uint32)key->origin_node_id << 16) ^ ((uint32)key->undo_segment_id);
	h ^= key->tt_slot_id;
	h ^= key->cluster_epoch * 0x9E3779B9u;
	h ^= (uint32)key->local_xid * 0x85EBCA6Bu;
	return (h ^ (h >> 16)) & (CLUSTER_VIS_MEMO_SLOTS - 1);
}

static inline bool
vis_memo_status_terminal(uint8 status, SCN commit_scn)
{
	if (status == CLUSTER_TT_STATUS_COMMITTED)
		return SCN_VALID(commit_scn);
	return status == CLUSTER_TT_STATUS_ABORTED;
}

bool
cluster_vis_memo_probe(const ClusterTTStatusKey *key, uint8 *status_out, SCN *scn_out)
{
	ClusterVisMemoEntry *e;

	if (!vis_memo_active() || key == NULL)
		return false;

	e = &cluster_vis_memo[vis_memo_slot(key)];
	if (e->lxid != MyProc->lxid)
		return false; /* stale transaction's entry: never replayed */
	if (memcmp(&e->key, key, sizeof(ClusterTTStatusKey)) != 0)
		return false;

	Assert(vis_memo_status_terminal(e->status, e->commit_scn));
	*status_out = e->status;
	*scn_out = e->commit_scn;
	if (ClusterXnodeLeverCtl != NULL)
		pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->c_memo_hit_count, 1);
	return true;
}

void
cluster_vis_memo_install(const ClusterTTStatusKey *key, uint8 status, SCN commit_scn)
{
	ClusterVisMemoEntry *e;

	if (!vis_memo_active() || key == NULL)
		return;
	if (!vis_memo_status_terminal(status, commit_scn))
		return; /* 8.A: only immutable terminal facts enter */

	e = &cluster_vis_memo[vis_memo_slot(key)];
	e->key = *key;
	e->lxid = MyProc->lxid;
	e->status = status;
	e->commit_scn = (status == CLUSTER_TT_STATUS_COMMITTED) ? commit_scn : InvalidScn;
	if (ClusterXnodeLeverCtl != NULL)
		pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->c_memo_install_count, 1);
}

/* ---------------- D0 measure hooks ---------------- */

static inline bool
lever_c_counting(void)
{
	return (cluster_page_scn_shortcut || cluster_xnode_profile_enabled)
		   && ClusterXnodeLeverCtl != NULL;
}

void
cluster_lever_c_note_resolve(void)
{
	if (!lever_c_counting())
		return;
	pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->c_resolve_count, 1);
}

void
cluster_lever_c_note_tt_lookup(bool stamp_cached_present, bool stamp_contradicted)
{
	if (!lever_c_counting())
		return;
	pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->c_tt_lookup_count, 1);
	if (stamp_cached_present)
		pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->c_stamp_cached_seen_count, 1);
	if (stamp_contradicted)
		pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->c_stamp_contradicted_count, 1);
}

/* ---------------- wave-a measure hooks ---------------- */

static inline bool
lever_a_counting(void)
{
	return (cluster_read_scache || cluster_xnode_profile_enabled) && ClusterXnodeLeverCtl != NULL;
}

void
cluster_lever_a_note_downgrade(bool downgraded)
{
	if (!lever_a_counting())
		return;
	if (downgraded)
		pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->a_downgrade_count, 1);
	else
		pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->a_downgrade_refused_count, 1);
}

void
cluster_lever_a_note_fwd_oneshot(void)
{
	if (!lever_a_counting())
		return;
	pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->a_fwd_oneshot_count, 1);
}

/* spec-6.12a ㉕ — holder-side remote-downgrade outcome. */
void
cluster_lever_a_note_remote_downgrade(bool downgraded)
{
	if (!lever_a_counting())
		return;
	if (downgraded)
		pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->a_remote_downgrade_count, 1);
	else
		pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->a_remote_downgrade_refused_count, 1);
}

/* spec-6.12a ㉕ — requester-side registration denial (degrade to one-shot). */
void
cluster_lever_a_note_remote_ack_degraded(void)
{
	if (!lever_a_counting())
		return;
	pg_atomic_fetch_add_u64(&ClusterXnodeLeverCtl->a_remote_ack_degraded_count, 1);
}
