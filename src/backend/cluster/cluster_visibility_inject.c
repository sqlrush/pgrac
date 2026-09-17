/*-------------------------------------------------------------------------
 *
 * cluster_visibility_inject.c
 *	  pgrac test-only visibility cluster path inject mechanism.
 *
 *	  spec-3.2 D5b (NEW;v0.3 N3 driver).
 *
 *	  ENABLE_INJECTION conditional:  production binary (no
 *	  --enable-injection-points configure flag) gets a stub body:
 *	  lookup helper returns false, no GUC is registered, and SQL UDFs
 *	  raise FEATURE_NOT_SUPPORTED.  See header file for full design
 *	  rationale.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.2-mvcc-cluster-path-tt-status-wire.md (v1.0 FROZEN 2026-05-22)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_visibility_inject.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_visibility_inject.h"
#include "fmgr.h"

#ifdef ENABLE_INJECTION

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_status.h"

#define CLUSTER_VISIBILITY_INJECT_CAPACITY 256

typedef struct ClusterVisibilityInjectEntry {
	TransactionId xid; /* HTAB key */
	ClusterUndoTTSlotRef ref;
	SCN commit_scn; /* spec-3.4c D9: stash for ref->cached_commit_scn on lookup */
} ClusterVisibilityInjectEntry;

static HTAB *ClusterVisibilityInjectHTAB = NULL;
static LWLock *ClusterVisibilityInjectLock = NULL;

/* ------------------------------------------------------------ */
/* shmem layout                                                 */
/* ------------------------------------------------------------ */

Size
cluster_visibility_inject_shmem_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return 0;
	return hash_estimate_size(CLUSTER_VISIBILITY_INJECT_CAPACITY,
							  sizeof(ClusterVisibilityInjectEntry))
		   + MAXALIGN(sizeof(LWLockPadded));
}

void
cluster_visibility_inject_shmem_init(void)
{
	HASHCTL info;
	LWLockPadded *lockblock;
	bool found;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(TransactionId);
	info.entrysize = sizeof(ClusterVisibilityInjectEntry);
	info.num_partitions = 1;
	ClusterVisibilityInjectHTAB
		= ShmemInitHash("ClusterVisibilityInject", CLUSTER_VISIBILITY_INJECT_CAPACITY,
						CLUSTER_VISIBILITY_INJECT_CAPACITY, &info, HASH_ELEM | HASH_BLOBS);

	lockblock = (LWLockPadded *)ShmemInitStruct("ClusterVisibilityInjectLock",
												MAXALIGN(sizeof(LWLockPadded)), &found);
	if (!found)
		LWLockInitialize(&lockblock->lock, LWTRANCHE_CLUSTER_TT_STATUS);
	ClusterVisibilityInjectLock = &lockblock->lock;
}

static const ClusterShmemRegion cluster_visibility_inject_region = {
	.name = "pgrac cluster visibility inject",
	.size_fn = cluster_visibility_inject_shmem_size,
	.init_fn = cluster_visibility_inject_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_visibility_inject",
	.reserved_flags = 0,
};

void
cluster_visibility_inject_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_visibility_inject_region);
}

/* ------------------------------------------------------------ */
/* lookup helper used by D5 fork                                */
/* ------------------------------------------------------------ */

bool
cluster_test_lookup_visibility_inject(TransactionId xid, ClusterUndoTTSlotRef *ref)
{
	const ClusterVisibilityInjectEntry *e;
	bool hit = false;

	if (!cluster_test_force_visibility_cluster_path || ClusterVisibilityInjectHTAB == NULL
		|| ref == NULL)
		return false;

	LWLockAcquire(ClusterVisibilityInjectLock, LW_SHARED);
	e = (ClusterVisibilityInjectEntry *)hash_search(ClusterVisibilityInjectHTAB, &xid, HASH_FIND,
													NULL);
	if (e != NULL) {
		*ref = e->ref;
		/*
		 * spec-3.4c D9: surface stashed commit_scn through the ref so the
		 * reader path's cached_commit_scn / has_cached_status invariants
		 * match the spec-3.4b D7 third branch (real UBA decode +
		 * cached_commit_scn populated).  Inject UDF (D7) already mirrors
		 * commit_scn into e->commit_scn and the install_local overlay; this
		 * copy keeps the ref-level cached fields consistent.
		 */
		ref->cached_commit_scn = e->commit_scn;
		ref->has_cached_status = SCN_VALID(e->commit_scn);
		hit = true;
	}
	LWLockRelease(ClusterVisibilityInjectLock);
	return hit;
}

/* ------------------------------------------------------------ */
/* SQL UDF (superuser only)                                     */
/* ------------------------------------------------------------ */

PG_FUNCTION_INFO_V1(cluster_test_inject_visibility_tt_ref);
PG_FUNCTION_INFO_V1(cluster_test_clear_visibility_injects);
/* PGRAC spec-3.5 D19 */
PG_FUNCTION_INFO_V1(cluster_test_inject_subtrans_subcommitted);

/*
 * cluster_test_inject_visibility_tt_ref (spec-3.4c D7 + A1 + F5):
 *
 *	  7-arg signature.  In addition to stashing the inject ref into the
 *	  per-xid HTAB, the UDF synchronously installs the corresponding
 *	  TT status overlay entry (status=COMMITTED, commit_scn=<arg>) and
 *	  immediately verifies via lookup_exact() so an overlay-full drop
 *	  cannot hide behind a "success" return.  Verification failure raises
 *	  ERRCODE_CONFIGURATION_LIMIT_EXCEEDED.
 *
 *	  Spec-3.4c A1 P0: the visibility hot path goes
 *	  ref -> ClusterTTStatusKey -> cluster_tt_status_lookup_exact() ->
 *	  result.commit_scn -> cluster_visibility_decide_by_scn().  Stashing
 *	  commit_scn only in the inject ref (no overlay install) leaves
 *	  lookup_exact returning miss -> 53R97 fail-closed.
 */
Datum
cluster_test_inject_visibility_tt_ref(PG_FUNCTION_ARGS)
{
	TransactionId xid;
	uint16 origin;
	uint16 segment;
	uint32 slot;
	uint32 epoch;
	SCN commit_scn;
	bool is_lock_only;
	bool installed;
	bool install_value;
	bool lookup_value;
	ClusterSemanticAdmissionResult install_admission;
	ClusterSemanticAdmissionResult lookup_admission;
	ClusterTTStatus install_status;
	ClusterTTStatusKey key;
	ClusterTTStatusResult res;
	ClusterTTStatusSourceRequest source_request;
	ClusterTTStatusSourceResult source_result;
	ClusterVisibilityInjectEntry *e;
	bool found;
	bool looked_up;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to use cluster_test_inject_visibility_tt_ref")));

	if (ClusterVisibilityInjectHTAB == NULL)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster visibility inject shmem not initialized")));

	xid = (TransactionId)PG_GETARG_UINT32(0);
	origin = (uint16)PG_GETARG_INT32(1);
	segment = (uint16)PG_GETARG_INT32(2);
	slot = (uint32)PG_GETARG_INT32(3);
	epoch = (uint32)PG_GETARG_INT32(4);
	commit_scn = (SCN)PG_GETARG_INT64(5);
	is_lock_only = PG_GETARG_BOOL(6);

	/*
	 * spec-3.4d D8/D9 + F3:  is_lock_only=true installs IN_PROGRESS status
	 * (== ACTIVE in spec v0.2 §6.4 wording) + commit_scn forced to InvalidScn.
	 * Used by t/209 + cluster_unit to verify wait_policy-aware fail-closed
	 * remote ACTIVE detection without a real cross-node lock fixture.
	 *
	 * is_lock_only=false preserves spec-3.4c COMMITTED + commit_scn behavior.
	 */
	if (is_lock_only) {
		install_status = CLUSTER_TT_STATUS_IN_PROGRESS;
		commit_scn = InvalidScn;
	} else {
		install_status = CLUSTER_TT_STATUS_COMMITTED;
	}

	/* 1. inject ref HTAB (existing surface + D9 commit_scn stash) */
	LWLockAcquire(ClusterVisibilityInjectLock, LW_EXCLUSIVE);
	e = (ClusterVisibilityInjectEntry *)hash_search(ClusterVisibilityInjectHTAB, &xid,
													HASH_ENTER_NULL, &found);
	if (e == NULL) {
		LWLockRelease(ClusterVisibilityInjectLock);
		ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						errmsg("cluster visibility inject table full")));
	}

	memset(&e->ref, 0, sizeof(e->ref));
	e->ref.origin_node_id = origin;
	e->ref.undo_segment_id = segment;
	e->ref.tt_slot_id = slot;
	e->ref.cluster_epoch = epoch;
	e->ref.local_xid = xid;
	e->ref.cached_commit_scn = commit_scn;
	e->ref.has_cached_status = SCN_VALID(commit_scn);
	e->commit_scn = commit_scn;
	LWLockRelease(ClusterVisibilityInjectLock);

	/*
	 * 2. spec-3.4c A1 P0 — synchronously install TT status overlay so
	 *    cluster_tt_status_lookup_exact() returns COMMITTED + commit_scn
	 *    on the visibility hot path.  Without this, lookup miss -> 53R97
	 *    fail-closed even though the inject "succeeded".
	 */
	memset(&key, 0, sizeof(key));
	key.origin_node_id = origin;
	key.undo_segment_id = segment;
	key.tt_slot_id = slot;
	key.cluster_epoch = epoch;
	key.local_xid = xid;

	memset(&source_request, 0, sizeof(source_request));
	source_request.key = &key;
	source_request.status = install_status;
	source_request.commit_scn = commit_scn;
	install_admission = cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_INSTALL_LOCAL,
														  &source_request, &source_result);
	install_value = source_result.bool_value;
	installed = install_admission == CLUSTER_SEMANTIC_ADMISSION_OK && install_value;

	/*
	 * 3. spec-3.4c F5 + spec-3.4d D9:  install_local() is best-effort and
	 *    returns false on overlay-full drop.  Verify immediately so a
	 *    silent drop cannot hide behind a successful PG_RETURN_BOOL(true).
	 *    spec-3.4d extends the check to verify is_lock_only path installs
	 *    IN_PROGRESS + InvalidScn commit_scn (not COMMITTED + valid
	 *    commit_scn).
	 */
	memset(&source_request, 0, sizeof(source_request));
	source_request.key = &key;
	lookup_admission = cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_LOOKUP, &source_request,
														 &source_result);
	lookup_value = source_result.bool_value;
	looked_up = lookup_admission == CLUSTER_SEMANTIC_ADMISSION_OK && lookup_value;
	res = source_result.lookup;
	if (!installed || !looked_up || res.status != install_status || res.commit_scn != commit_scn)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("cluster TT status overlay install verification failed"),
				 errdetail("install admission=%d result=%s; lookup admission=%d result=%s; "
						   "key epoch=%u current epoch=" UINT64_FORMAT "; "
						   "requested status=%d observed status=%d; "
						   "requested commit SCN=" UINT64_FORMAT
						   " observed commit SCN=" UINT64_FORMAT ".",
						   (int)install_admission, install_value ? "true" : "false",
						   (int)lookup_admission, lookup_value ? "true" : "false",
						   key.cluster_epoch, cluster_epoch_get_current(), (int)install_status,
						   (int)res.status, (uint64)commit_scn, (uint64)res.commit_scn),
				 errhint("Raise cluster.tt_status_overlay_max_entries or lower TTL.")));

	PG_RETURN_BOOL(true);
}

/*
 * cluster_test_clear_visibility_injects (spec-3.4c D7 + F4):
 *
 *	  Removes every inject HTAB entry AND, for each entry, calls
 *	  cluster_tt_status_delete_exact() to drop its overlay companion.
 *	  Per F4, must not fake-clear by writing ABORTED into the overlay --
 *	  use the real per-key delete API (D6).
 */
Datum
cluster_test_clear_visibility_injects(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS hseq;
	ClusterVisibilityInjectEntry *e;
	int removed = 0;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to clear cluster visibility injects")));

	if (ClusterVisibilityInjectHTAB == NULL)
		PG_RETURN_INT32(0);

	LWLockAcquire(ClusterVisibilityInjectLock, LW_EXCLUSIVE);
	hash_seq_init(&hseq, ClusterVisibilityInjectHTAB);
	while ((e = (ClusterVisibilityInjectEntry *)hash_seq_search(&hseq)) != NULL) {
		ClusterTTStatusKey key;
		ClusterTTStatusSourceRequest source_request;
		ClusterTTStatusSourceResult source_result;

		/* F4: build exact key + real delete_exact (NOT fake ABORT install). */
		memset(&key, 0, sizeof(key));
		key.origin_node_id = e->ref.origin_node_id;
		key.undo_segment_id = e->ref.undo_segment_id;
		key.tt_slot_id = e->ref.tt_slot_id;
		key.cluster_epoch = e->ref.cluster_epoch;
		key.local_xid = e->ref.local_xid;
		memset(&source_request, 0, sizeof(source_request));
		source_request.key = &key;
		(void)cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_DELETE_EXACT, &source_request,
												&source_result);

		hash_search(ClusterVisibilityInjectHTAB, &e->xid, HASH_REMOVE, NULL);
		removed++;
	}
	LWLockRelease(ClusterVisibilityInjectLock);

	PG_RETURN_INT32(removed);
}

/*
 * cluster_test_inject_subtrans_subcommitted (spec-3.5 D19 NEW):
 *
 *	  Test-only inject of a SUBCOMMITTED parent chain overlay entry.
 *	  Builds the child's ClusterTTStatusKey from (child_xid, origin,
 *	  segment, slot, epoch) and the parent's key from (parent_xid,
 *	  origin, segment+1, slot+1, epoch) — synthetic parent identity
 *	  distinct from the child but living in the same origin/epoch so
 *	  reader lazy follow can resolve.  Caller is responsible for the
 *	  parent's terminal status (COMMITTED/ABORTED/IN_PROGRESS) — typical
 *	  pattern:  call cluster_test_inject_visibility_tt_ref for the
 *	  parent first, then this UDF to register the child SUBCOMMITTED.
 *
 *	  Used by t/210 L9 SUBCOMMITTED chain inject smoke (when
 *	  ENABLE_INJECTION).  Production build returns FEATURE_NOT_SUPPORTED
 *	  via the #else stub below.
 */
Datum
cluster_test_inject_subtrans_subcommitted(PG_FUNCTION_ARGS)
{
	TransactionId child_xid;
	TransactionId parent_xid;
	uint16 origin;
	uint16 segment;
	uint32 slot;
	uint32 epoch;
	ClusterTTStatusKey child_key;
	ClusterTTStatusKey parent_key;
	ClusterTTStatusSourceRequest source_request;
	ClusterTTStatusSourceResult source_result;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to use "
							   "cluster_test_inject_subtrans_subcommitted")));

	child_xid = (TransactionId)PG_GETARG_UINT32(0);
	parent_xid = (TransactionId)PG_GETARG_UINT32(1);
	origin = (uint16)PG_GETARG_INT32(2);
	segment = (uint16)PG_GETARG_INT32(3);
	slot = (uint32)PG_GETARG_INT32(4);
	epoch = (uint32)PG_GETARG_INT32(5);

	memset(&child_key, 0, sizeof(child_key));
	child_key.origin_node_id = origin;
	child_key.undo_segment_id = segment;
	child_key.tt_slot_id = slot;
	child_key.cluster_epoch = epoch;
	child_key.local_xid = child_xid;

	memset(&parent_key, 0, sizeof(parent_key));
	parent_key.origin_node_id = origin;
	parent_key.undo_segment_id = (uint16)(segment + 1);
	parent_key.tt_slot_id = slot + 1;
	parent_key.cluster_epoch = epoch;
	parent_key.local_xid = parent_xid;

	memset(&source_request, 0, sizeof(source_request));
	source_request.key = &child_key;
	source_request.parent_key = &parent_key;
	if (cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_INSTALL_SUBCOMMITTED, &source_request,
										  &source_result)
			!= CLUSTER_SEMANTIC_ADMISSION_OK
		|| !source_result.bool_value)
		ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						errmsg("cluster_test_inject_subtrans_subcommitted: install failed"),
						errhint("Raise cluster.tt_status_overlay_max_entries or lower TTL.")));

	PG_RETURN_BOOL(true);
}

#else /* !ENABLE_INJECTION */

Size
cluster_visibility_inject_shmem_size(void)
{
	return 0;
}
void
cluster_visibility_inject_shmem_init(void)
{}
void
cluster_visibility_inject_shmem_register(void)
{}
bool
cluster_test_lookup_visibility_inject(TransactionId xid, ClusterUndoTTSlotRef *ref)
{
	(void)xid;
	(void)ref;
	return false;
}

PG_FUNCTION_INFO_V1(cluster_test_inject_visibility_tt_ref);
PG_FUNCTION_INFO_V1(cluster_test_clear_visibility_injects);
/* PGRAC spec-3.5 D19 */
PG_FUNCTION_INFO_V1(cluster_test_inject_subtrans_subcommitted);

Datum
cluster_test_inject_visibility_tt_ref(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster visibility inject support is not enabled"),
					errhint("Rebuild with --enable-injection-points to use "
							"cluster_test_inject_visibility_tt_ref().")));
	PG_RETURN_BOOL(false);
}

Datum
cluster_test_clear_visibility_injects(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster visibility inject support is not enabled"),
					errhint("Rebuild with --enable-injection-points to use "
							"cluster_test_clear_visibility_injects().")));
	PG_RETURN_INT32(0);
}

/* PGRAC spec-3.5 D19 stub for production build (--without ENABLE_INJECTION). */
Datum
cluster_test_inject_subtrans_subcommitted(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster visibility inject support is not enabled"),
					errhint("Rebuild with --enable-injection-points to use "
							"cluster_test_inject_subtrans_subcommitted().")));
	PG_RETURN_BOOL(false);
}

#endif /* ENABLE_INJECTION */
