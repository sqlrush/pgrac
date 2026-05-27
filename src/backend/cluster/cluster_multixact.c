/*-------------------------------------------------------------------------
 *
 * cluster_multixact.c
 *	  pgrac MULTIXACT reader/member-resolution foundation — implementation.
 *
 *	  spec-3.6 D2 (NEW;Stage 3 第 10 sub-spec).
 *
 *	  See cluster_multixact.h for the public contract.  This file
 *	  implements:
 *	    - cluster_multixact_member_overlay HTAB (shmem-resident,
 *	      bounded by cluster.multixact_member_overlay_max_entries)
 *	    - install / lookup / purge_epoch
 *	    - cluster_multixact_resolve_visibility (truth table per
 *	      OBS-1 MVCC-accurate semantics)
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.6-multixact-reader-member-resolution.md (v0.3 FROZEN 2026-05-27)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_multixact.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "cluster/cluster_elog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_multixact.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_status.h"

#ifdef USE_PGRAC_CLUSTER

/*
 * Overlay HTAB entry.  Key is ClusterMultiXactKey (16B blob, HASH_BLOBS);
 * value carries member_count + generation_ts + fixed-size members[] array
 * (CLUSTER_MULTIXACT_MAX_MEMBERS = 256 per HC208/HC209 default cap).
 */
#define CLUSTER_MULTIXACT_MAX_MEMBERS 256

typedef struct ClusterMultiXactOverlayEntry {
	ClusterMultiXactKey key;
	uint16 member_count;
	uint16 _pad16;
	TimestampTz generation_ts;
	ClusterMultiXactMember members[CLUSTER_MULTIXACT_MAX_MEMBERS];
} ClusterMultiXactOverlayEntry;

typedef struct ClusterMultiXactShmem {
	pg_atomic_uint64 overlay_install_count;
	pg_atomic_uint64 overlay_lookup_hit_count;
	pg_atomic_uint64 overlay_miss_count;
	pg_atomic_uint64 overlay_overflow_count;
	pg_atomic_uint64 resolve_visibility_count;
} ClusterMultiXactShmem;

static HTAB *ClusterMultiXactHTAB = NULL;
static LWLock *ClusterMultiXactLock = NULL;
static ClusterMultiXactShmem *ClusterMultiXactState = NULL;

/* ------------------------------------------------------------ */
/* shmem layout                                                 */
/* ------------------------------------------------------------ */

Size
cluster_multixact_shmem_size(void)
{
	Size sz;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return 0;

	sz = MAXALIGN(sizeof(ClusterMultiXactShmem));
	sz = add_size(sz, MAXALIGN(sizeof(LWLockPadded)));
	sz = add_size(sz, hash_estimate_size(cluster_multixact_member_overlay_max_entries,
										 sizeof(ClusterMultiXactOverlayEntry)));
	return sz;
}

void
cluster_multixact_shmem_init(void)
{
	HASHCTL info;
	bool found;
	LWLockPadded *lockblock;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return;

	ClusterMultiXactState = (ClusterMultiXactShmem *)ShmemInitStruct(
		"ClusterMultiXactState", MAXALIGN(sizeof(ClusterMultiXactShmem)), &found);
	if (!found) {
		pg_atomic_init_u64(&ClusterMultiXactState->overlay_install_count, 0);
		pg_atomic_init_u64(&ClusterMultiXactState->overlay_lookup_hit_count, 0);
		pg_atomic_init_u64(&ClusterMultiXactState->overlay_miss_count, 0);
		pg_atomic_init_u64(&ClusterMultiXactState->overlay_overflow_count, 0);
		pg_atomic_init_u64(&ClusterMultiXactState->resolve_visibility_count, 0);
	}

	lockblock = (LWLockPadded *)ShmemInitStruct("ClusterMultiXactLock",
												MAXALIGN(sizeof(LWLockPadded)), &found);
	if (!found)
		LWLockInitialize(&lockblock->lock, LWTRANCHE_CLUSTER_TT_STATUS);
	ClusterMultiXactLock = &lockblock->lock;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ClusterMultiXactKey);
	info.entrysize = sizeof(ClusterMultiXactOverlayEntry);
	info.num_partitions = 1;
	ClusterMultiXactHTAB = ShmemInitHash(
		"ClusterMultiXactOverlay", cluster_multixact_member_overlay_max_entries,
		cluster_multixact_member_overlay_max_entries, &info, HASH_ELEM | HASH_BLOBS);
}

static const ClusterShmemRegion cluster_multixact_region = {
	.name = "pgrac cluster multixact overlay",
	.size_fn = cluster_multixact_shmem_size,
	.init_fn = cluster_multixact_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_multixact",
	.reserved_flags = 0,
};

void
cluster_multixact_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_multixact_region);
}

/* ------------------------------------------------------------ */
/* Public API                                                   */
/* ------------------------------------------------------------ */

bool
cluster_multixact_member_overlay_install(const ClusterMultiXactKey *key, uint16 member_count,
										 const ClusterMultiXactMember *members)
{
	ClusterMultiXactOverlayEntry *e;
	bool found;

	if (key == NULL || members == NULL || ClusterMultiXactHTAB == NULL)
		return false;

	if (member_count == 0 || member_count > cluster_multixact_member_overlay_max_members
		|| member_count > CLUSTER_MULTIXACT_MAX_MEMBERS) {
		if (ClusterMultiXactState != NULL)
			pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_overflow_count, 1);
		return false;
	}

	LWLockAcquire(ClusterMultiXactLock, LW_EXCLUSIVE);
	e = (ClusterMultiXactOverlayEntry *)hash_search(ClusterMultiXactHTAB, key, HASH_ENTER_NULL,
													&found);
	if (e == NULL) {
		LWLockRelease(ClusterMultiXactLock);
		pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_overflow_count, 1);
		ereport(WARNING, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						  errmsg("cluster multixact overlay full; install dropped"),
						  errhint("Raise cluster.multixact_member_overlay_max_entries.")));
		return false;
	}

	e->member_count = member_count;
	e->_pad16 = 0;
	e->generation_ts = GetCurrentTimestamp();
	memcpy(e->members, members, member_count * sizeof(ClusterMultiXactMember));

	LWLockRelease(ClusterMultiXactLock);
	pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_install_count, 1);
	return true;
}

bool
cluster_multixact_member_overlay_lookup(const ClusterMultiXactKey *key,
										ClusterMultiXactMemberOverlayResult *out,
										int max_members_buf)
{
	const ClusterMultiXactOverlayEntry *e;
	uint32 current_epoch;

	if (key == NULL || out == NULL || ClusterMultiXactHTAB == NULL)
		return false;

	/* fail-closed sentinel */
	out->authoritative = false;
	out->member_count = 0;
	out->_pad16 = 0;
	out->generation_ts = 0;

	current_epoch = (uint32)cluster_epoch_get_current();
	if (key->cluster_epoch != current_epoch) {
		pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_miss_count, 1);
		return false;
	}

	LWLockAcquire(ClusterMultiXactLock, LW_SHARED);
	e = (const ClusterMultiXactOverlayEntry *)hash_search(ClusterMultiXactHTAB, key, HASH_FIND,
														  NULL);
	if (e == NULL) {
		LWLockRelease(ClusterMultiXactLock);
		pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_miss_count, 1);
		return false;
	}

	if ((int)e->member_count > max_members_buf) {
		LWLockRelease(ClusterMultiXactLock);
		out->member_count = e->member_count; /* tell caller how big buf needed */
		pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_overflow_count, 1);
		return false;
	}

	out->authoritative = true;
	out->member_count = e->member_count;
	out->generation_ts = e->generation_ts;
	memcpy(out->members, e->members, e->member_count * sizeof(ClusterMultiXactMember));

	LWLockRelease(ClusterMultiXactLock);
	pg_atomic_fetch_add_u64(&ClusterMultiXactState->overlay_lookup_hit_count, 1);
	return true;
}

/*
 * cluster_multixact_resolve_visibility (D2 core helper)
 *
 *   Per OBS-1 truth table:
 *     lock-only (status 0-3) ANY xid state              -> VISIBLE
 *     Update/NoKeyUpdate (4-5) ABORTED                  -> VISIBLE
 *     Update/NoKeyUpdate (4-5) IN_PROGRESS authoritative-> VISIBLE
 *     Update/NoKeyUpdate (4-5) COMMITTED scn<=read_scn  -> INVISIBLE
 *     Update/NoKeyUpdate (4-5) COMMITTED scn>read_scn   -> VISIBLE
 *     UNKNOWN / TT miss / overlay miss                  -> UNKNOWN
 *
 *   Helper resolves each member's per-xid status via
 *   cluster_tt_status_lookup_exact + builds ClusterTTStatusKey from
 *   (member.origin_node_id, segment=0, slot=0, epoch, member.xid)
 *   matching the spec-3.1 D5 binding contract;  miss -> UNKNOWN per L199.
 */
ClusterVisibilityDecision
cluster_multixact_resolve_visibility(const ClusterMultiXactMemberOverlayResult *overlay,
									 const Snapshot snap)
{
	uint16 i;

	if (overlay == NULL || !overlay->authoritative || snap == NULL)
		return CLUSTER_VISIBILITY_UNKNOWN;

	if (ClusterMultiXactState != NULL)
		pg_atomic_fetch_add_u64(&ClusterMultiXactState->resolve_visibility_count, 1);

	for (i = 0; i < overlay->member_count; i++) {
		const ClusterMultiXactMember *m = &overlay->members[i];
		uint8 status = m->status;

		/*
		 * Lock-only members (FOR_KEY_SHARE / FOR_SHARE / FOR_NOKEYUPDATE /
		 * FOR_UPDATE) cannot hide tuple data regardless of commit/abort
		 * state -- they only lock the row.
		 */
		if (status <= 3) /* MultiXactStatusForKeyShare..ForUpdate */
			continue;

		/*
		 * Update / NoKeyUpdate members:  consult per-member TT status to
		 * decide visibility against snapshot.
		 */
		{
			ClusterTTStatusKey ttkey;
			ClusterTTStatusResult ttres;

			memset(&ttkey, 0, sizeof(ttkey));
			ttkey.origin_node_id = m->origin_node_id;
			ttkey.undo_segment_id = 0;
			ttkey.tt_slot_id = 0;
			ttkey.cluster_epoch = m->epoch;
			ttkey.local_xid = m->xid;

			if (!cluster_tt_status_lookup_exact(&ttkey, &ttres) || !ttres.authoritative)
				return CLUSTER_VISIBILITY_UNKNOWN;

			if (ttres.status == CLUSTER_TT_STATUS_ABORTED)
				continue; /* aborted updater does not hide tuple */
			if (ttres.status == CLUSTER_TT_STATUS_IN_PROGRESS)
				continue; /* in-progress update not yet visible: tuple still visible */
			if (ttres.status == CLUSTER_TT_STATUS_COMMITTED
				|| ttres.status == CLUSTER_TT_STATUS_CLEANED_OUT) {
				ClusterVisibilityDecision d
					= cluster_visibility_decide_by_scn(ttres.commit_scn, snap->read_scn);
				if (d == CLUSTER_VISIBILITY_INVISIBLE)
					return CLUSTER_VISIBILITY_INVISIBLE;
				if (d == CLUSTER_VISIBILITY_UNKNOWN)
					return CLUSTER_VISIBILITY_UNKNOWN;
				/* VISIBLE -> updater happened after snapshot;  continue */
				continue;
			}
			/* SUBCOMMITTED / UNKNOWN / other -> caller fail-closed */
			return CLUSTER_VISIBILITY_UNKNOWN;
		}
	}

	/* No updater member hid the tuple -> visible. */
	return CLUSTER_VISIBILITY_VISIBLE;
}

uint16
cluster_multixact_get_member_count(const ClusterMultiXactKey *key)
{
	const ClusterMultiXactOverlayEntry *e;
	uint16 count = 0;

	if (key == NULL || ClusterMultiXactHTAB == NULL)
		return 0;

	LWLockAcquire(ClusterMultiXactLock, LW_SHARED);
	e = (const ClusterMultiXactOverlayEntry *)hash_search(ClusterMultiXactHTAB, key, HASH_FIND,
														  NULL);
	if (e != NULL)
		count = e->member_count;
	LWLockRelease(ClusterMultiXactLock);
	return count;
}

void
cluster_multixact_purge_epoch(uint32 obsolete_epoch)
{
	HASH_SEQ_STATUS hseq;
	ClusterMultiXactOverlayEntry *e;

	if (ClusterMultiXactHTAB == NULL)
		return;

	LWLockAcquire(ClusterMultiXactLock, LW_EXCLUSIVE);
	hash_seq_init(&hseq, ClusterMultiXactHTAB);
	while ((e = (ClusterMultiXactOverlayEntry *)hash_seq_search(&hseq)) != NULL) {
		if (e->key.cluster_epoch < obsolete_epoch)
			hash_search(ClusterMultiXactHTAB, &e->key, HASH_REMOVE, NULL);
	}
	LWLockRelease(ClusterMultiXactLock);
}

/* ------------------------------------------------------------ */
/* Counter getters                                              */
/* ------------------------------------------------------------ */

#define CLUSTER_MULTIXACT_GETTER(name)                                                             \
	uint64 cluster_multixact_get_##name(void)                                                      \
	{                                                                                              \
		if (ClusterMultiXactState == NULL)                                                         \
			return 0;                                                                              \
		return pg_atomic_read_u64(&ClusterMultiXactState->name);                                   \
	}

CLUSTER_MULTIXACT_GETTER(overlay_install_count)
CLUSTER_MULTIXACT_GETTER(overlay_lookup_hit_count)
CLUSTER_MULTIXACT_GETTER(overlay_miss_count)
CLUSTER_MULTIXACT_GETTER(overlay_overflow_count)
CLUSTER_MULTIXACT_GETTER(resolve_visibility_count)

#else /* !USE_PGRAC_CLUSTER */

Size
cluster_multixact_shmem_size(void)
{
	return 0;
}
void
cluster_multixact_shmem_init(void)
{}
void
cluster_multixact_shmem_register(void)
{}

bool
cluster_multixact_member_overlay_install(const ClusterMultiXactKey *key, uint16 member_count,
										 const ClusterMultiXactMember *members)
{
	(void)key;
	(void)member_count;
	(void)members;
	return false;
}

bool
cluster_multixact_member_overlay_lookup(const ClusterMultiXactKey *key,
										ClusterMultiXactMemberOverlayResult *out,
										int max_members_buf)
{
	(void)key;
	(void)max_members_buf;
	if (out != NULL) {
		out->authoritative = false;
		out->member_count = 0;
		out->generation_ts = 0;
	}
	return false;
}

ClusterVisibilityDecision
cluster_multixact_resolve_visibility(const ClusterMultiXactMemberOverlayResult *overlay,
									 const Snapshot snap)
{
	(void)overlay;
	(void)snap;
	return CLUSTER_VISIBILITY_UNKNOWN;
}

uint16
cluster_multixact_get_member_count(const ClusterMultiXactKey *key)
{
	(void)key;
	return 0;
}

void
cluster_multixact_purge_epoch(uint32 obsolete_epoch)
{
	(void)obsolete_epoch;
}

#define CLUSTER_MULTIXACT_GETTER_STUB(name)                                                        \
	uint64 cluster_multixact_get_##name(void)                                                      \
	{                                                                                              \
		return 0;                                                                                  \
	}

CLUSTER_MULTIXACT_GETTER_STUB(overlay_install_count)
CLUSTER_MULTIXACT_GETTER_STUB(overlay_lookup_hit_count)
CLUSTER_MULTIXACT_GETTER_STUB(overlay_miss_count)
CLUSTER_MULTIXACT_GETTER_STUB(overlay_overflow_count)
CLUSTER_MULTIXACT_GETTER_STUB(resolve_visibility_count)

#endif /* USE_PGRAC_CLUSTER */
