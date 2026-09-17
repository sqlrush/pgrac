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

#include "access/xact.h" /* spec-3.18 D4.1: xl_xact_tt_commit fold delta */
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/timestamp.h" /* S3 forensics — cleaner liveness-tick age */

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_scn.h" /* S3 forensics — floor-lag vs current SCN in errdetail */
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_durable.h" /* spec-3.11 D4 durable commit */
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_tt_2pc.h"			 /* ClusterTT2PCBinding export (spec-3.15) */
#include "cluster/cluster_tt_slot.h"		 /* spec-3.4b D4 real binding */
#include "cluster/cluster_undo_cleaner.h"	 /* S3 forensics — cleaner stall state in errdetail */
#include "cluster/cluster_undo_record_api.h" /* spec-3.12 D2b cluster_undo_tt_rollover_locked */
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tt_status_hint.h"		/* spec-3.2 D4 wire emit append */
#include "cluster/cluster_uba.h"				/* P0-33 exact data-ref alias */
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
typedef enum ClusterTTLocalTerminalState {
	CLUSTER_TT_LOCAL_TERMINAL_NONE = 0,
	CLUSTER_TT_LOCAL_TERMINAL_COMMIT_STAGED,
	CLUSTER_TT_LOCAL_TERMINAL_ABORT_DURABLE
} ClusterTTLocalTerminalState;

typedef struct ClusterTTLocalBinding {
	TransactionId top_xid; /* InvalidTransactionId == no binding */
	uint32 segment_id;
	uint32 segment_generation;
	uint16 slot_offset;
	/* spec-3.12 D2b: wrap counter captured at bind time.  The slot is held
	 * ACTIVE for the whole xact (never recycled while ACTIVE), so wrap cannot
	 * change before commit -- capturing it here lets precommit stamp the durable
	 * TT slot without re-reading the shmem allocator, which may have rolled this
	 * segment away (cluster_tt_slot_get_wrap would then ERROR on the stale id). */
	uint16 wrap;
	uint8 publish_state;
	uint8 terminal_state;
	uint8 reserved8[2];
	XLogRecPtr active_lsn;
	uint32 cluster_epoch; /* snapshot at bind time */
	/* Real undo records use an independent record cursor.  Its segment may
	 * differ from segment_id after rollover, while the page ref carries that
	 * record segment as its exact-key segment. */
	uint16 *active_alias_segments;
	uint16 active_alias_count;
	uint16 active_alias_capacity;
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
 * cluster_tt_local_reserve_binding
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
static bool
cluster_tt_local_reserve_binding(TransactionId top_xid, uint32 *out_segment_id,
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

			if (b->publish_state == CLUSTER_CANONICAL_TXN_FAILED)
				ereport(ERROR,
						(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
						 errmsg("canonical ACTIVE publication previously failed for transaction %u",
								b->top_xid)));
			/* Idempotent reuse of the exact backend-local reservation. */
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
		uint16 wrap;
		ClusterCtrcTxnKeyV1 capacity_continuation = { 0 };

		for (;;) {
			bool retained_pressure = false;
			bool current_drift = false;

			/*
			 * spec-3.12 D2b: allocate on the node's CURRENT TT segment (which may
			 * have been rolled forward by an earlier retention rollover). Cold
			 * memory does not prove the fixed base segment is empty: use the
			 * same available-segment/current-publication owner for its first bind.
			 *
			 * C3b/Q11: rollover releases lifecycle_lock while publishing the live
			 * owner.  A concurrent winner can therefore advance and fill CURRENT
			 * before this backend resumes.  Always reread and reclassify CURRENT;
			 * never assume the segment returned by rollover is still fresh.
			 */
			seg = cluster_tt_slot_current_segment(cluster_node_id);
			if (seg == 0) {
				/* No local slot reservation exists until a proven fresh segment
				 * has been published below. Never reset old durable TT history. */
				off = INVALID_TT_SLOT_OFFSET;
				retained_pressure = true;
			} else
				off = cluster_tt_slot_alloc_current_exact(
					cluster_node_id, seg, top_xid, &retained_pressure, &current_drift, &wrap);
			if (current_drift)
				continue;
			if (off != INVALID_TT_SLOT_OFFSET)
				break;

			if (!retained_pressure)
				/* All 48 slots ACTIVE -- genuine in-flight concurrency limit. */
				ereport(
					ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("cluster TT slot allocator exhausted on segment %u (48 slots full)",
							seg),
					 errdetail("node=%d retention_horizon_scn=" UINT64_FORMAT
							   " current_scn=" UINT64_FORMAT " retain_skip=" UINT64_FORMAT
							   " retention_recycle=" UINT64_FORMAT ".",
							   cluster_node_id, cluster_tt_slot_retention_horizon_scn(),
							   (uint64)cluster_scn_current(), cluster_tt_slot_retain_skip_count(),
							   cluster_tt_slot_retention_recycle_count()),
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

				if (new_seg == 0) {
					/* S3 forensics step 1/1a — the errdetail separates "cleaner
					 * stopped reclaiming" (cleaner status/iters/liveness stale,
					 * recycle counters flat, horizon far behind current_scn)
					 * from "pool genuinely at capacity under live retention"
					 * before anyone reaches for the GUC.  The rollover-failure
					 * counters are TT-specific; record_extent_hard_cap_fail is
					 * the OTHER (record-extent CLAIM) path, labeled as such. */
					TimestampTz cleaner_tick = cluster_undo_cleaner_last_liveness_tick_at();
					long cleaner_tick_age_ms
						= cleaner_tick > 0 ? (long)((GetCurrentTimestamp() - cleaner_tick) / 1000)
										   : -1;

					if (at_hard_cap
						&& cluster_undo_cleaner_wait_for_capacity(seg, &capacity_continuation))
						continue;

					ereport(
						ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("cluster TT slot allocator: retention rollover failed for "
								"segment %u (%s)",
								seg,
								at_hard_cap ? "undo segment pool hard cap reached"
											: "undo segment autoextend failed"),
						 errdetail(
							 "node=%d retention_horizon_scn=" UINT64_FORMAT
							 " current_scn=" UINT64_FORMAT " retain_skip=" UINT64_FORMAT
							 " retention_recycle=" UINT64_FORMAT
							 " retention_off_recycle=" UINT64_FORMAT " wrap_retired=" UINT64_FORMAT
							 " segment_reuse=" UINT64_FORMAT " segment_switch=" UINT64_FORMAT
							 " tt_rollover_fail_hard_cap=" UINT64_FORMAT
							 " tt_rollover_fail_extend=" UINT64_FORMAT
							 " record_extent_hard_cap_fail=" UINT64_FORMAT
							 " segments_max_per_instance=%d cleaner_status=%s"
							 " cleaner_loop_iters=" INT64_FORMAT " cleaner_tick_age_ms=%ld.",
							 cluster_node_id, cluster_tt_slot_retention_horizon_scn(),
							 (uint64)cluster_scn_current(), cluster_tt_slot_retain_skip_count(),
							 cluster_tt_slot_retention_recycle_count(),
							 cluster_tt_slot_retention_off_recycle_count(),
							 cluster_tt_slot_wrap_retired_count(),
							 cluster_undo_segment_reuse_count(),
							 cluster_undo_segment_switch_count(),
							 cluster_undo_tt_rollover_fail_hard_cap_count(),
							 cluster_undo_tt_rollover_fail_extend_count(),
							 cluster_undo_segment_hard_cap_fail_count(),
							 cluster_undo_segments_max_per_instance,
							 cluster_undo_cleaner_status_to_string(cluster_undo_cleaner_status()),
							 cluster_undo_cleaner_main_loop_iters(), cleaner_tick_age_ms),
						 errhint("A long-running reader is retaining committed undo; end it, "
								 "or raise cluster.undo_segments_max_per_instance.")));
				}
			}
		}

		b = cluster_tt_local_append_binding();
		b->top_xid = top_xid;
		b->segment_id = seg;
		b->slot_offset = off;
		b->wrap = wrap;
		b->publish_state = CLUSTER_CANONICAL_TXN_RESERVED;
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
	if (idx < 0 || !TransactionIdIsValid(top_xid)
		|| cluster_tt_local_bindings[idx].publish_state != CLUSTER_CANONICAL_TXN_PUBLISHED)
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
 * cluster_tt_local_export_bindings -- spec-3.15 D1 (PREPARE serialize).
 *
 *	Copy every backend-local binding into the caller's 2PC binding
 *	array.  Returns the binding count; when the count exceeds `max`
 *	the copy stops at max and the SATURATED count (max) is returned --
 *	the AtPrepare shell passes max = CAP+1 so saturation reads as
 *	"over cap" and serialize() rejects it (no silent truncation).
 *	Read-only: PREPARE may still fail and abort needs the bindings.
 */
uint16
cluster_tt_local_export_bindings(ClusterTT2PCBinding *dst, uint16 max)
{
	uint16 n = 0;
	uint32 i;

	for (i = 0; i < cluster_tt_local_binding_count && n < max; i++) {
		const ClusterTTLocalBinding *b = &cluster_tt_local_bindings[i];

		if (!TransactionIdIsValid(b->top_xid))
			continue;
		dst[n].undo_segment_id = b->segment_id;
		dst[n].slot_offset = b->slot_offset;
		dst[n].wrap = b->wrap;
		dst[n].cluster_epoch = b->cluster_epoch;
		dst[n].xid = b->top_xid;
		n++;
	}
	return n;
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
		if (committed && b->publish_state == CLUSTER_CANONICAL_TXN_PUBLISHED
			&& b->terminal_state == CLUSTER_TT_LOCAL_TERMINAL_COMMIT_STAGED)
			cluster_tt_slot_mark_committed(b->segment_id, b->slot_offset, b->top_xid, commit_scn);
		else if (!committed
				 && ((b->publish_state == CLUSTER_CANONICAL_TXN_RESERVED
					  && b->terminal_state == CLUSTER_TT_LOCAL_TERMINAL_NONE)
					 || (b->publish_state == CLUSTER_CANONICAL_TXN_PUBLISHED
						 && b->terminal_state == CLUSTER_TT_LOCAL_TERMINAL_ABORT_DURABLE)))
			cluster_tt_slot_mark_aborted(b->segment_id, b->slot_offset, b->top_xid);
	}

	cluster_tt_local_reset_binding();
}

/* Compose either the canonical key or one page-ref segment alias. */
static bool
build_binding_key(const ClusterTTLocalBinding *binding, uint16 segment_id, ClusterTTStatusKey *out)
{
	memset(out, 0, sizeof(*out));
	if (binding == NULL || !TransactionIdIsValid(binding->top_xid) || segment_id == 0)
		return false;

	out->origin_node_id = (uint16)cluster_node_id;
	out->undo_segment_id = segment_id;
	out->tt_slot_id = cluster_tt_slot_offset_to_id(binding->slot_offset);
	out->cluster_epoch = binding->cluster_epoch;
	out->local_xid = binding->top_xid;
	return true;
}

static bool
build_local_key(TransactionId xid, ClusterTTStatusKey *out)
{
	int idx = cluster_tt_local_find_binding(xid);

	if (idx < 0) {
		memset(out, 0, sizeof(*out));
		return false;
	}
	return build_binding_key(&cluster_tt_local_bindings[idx],
							 (uint16)cluster_tt_local_bindings[idx].segment_id, out);
}

/* Install and emit one already-minted exact key. */
static void
install_key(const ClusterTTStatusKey *key, ClusterTTStatus status, SCN commit_scn)
{
	ClusterTTStatusSourceRequest source_request;
	ClusterTTStatusSourceResult source_result;
	ClusterTTStatusHintSourceRequest hint_request;
	bool installed;

	memset(&source_request, 0, sizeof(source_request));
	source_request.key = key;
	source_request.status = status;
	source_request.commit_scn = commit_scn;
	installed = cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_INSTALL_LOCAL, &source_request,
												  &source_result)
					== CLUSTER_SEMANTIC_ADMISSION_OK
				&& source_result.bool_value;

	if (!installed)
		return;

#ifdef USE_ASSERT_CHECKING
	{
		ClusterTTStatusResult res;
		ClusterTTStatusSourceRequest lookup_request;
		ClusterTTStatusSourceResult lookup_result;
		bool hit = false;
		bool epoch_stable = ((uint32)cluster_epoch_get_current() == key->cluster_epoch);

		if (epoch_stable) {
			memset(&lookup_request, 0, sizeof(lookup_request));
			lookup_request.key = key;
			hit = cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_LOOKUP, &lookup_request,
													&lookup_result)
					  == CLUSTER_SEMANTIC_ADMISSION_OK
				  && lookup_result.bool_value;
			res = lookup_result.lookup;
			epoch_stable = ((uint32)cluster_epoch_get_current() == key->cluster_epoch);
		}
		if (epoch_stable)
			Assert(hit && res.authoritative && res.status == status);
		if (hit && res.authoritative && res.status == status) {
			ClusterTTStatusSourceRequest bump_request;
			ClusterTTStatusSourceResult bump_result;

			memset(&bump_request, 0, sizeof(bump_request));
			(void)cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_BUMP_SELF_CONSUMER_HIT,
													&bump_request, &bump_result);
		}
	}
#endif

	memset(&hint_request, 0, sizeof(hint_request));
	hint_request.key = key;
	hint_request.status = status;
	hint_request.commit_scn = commit_scn;
	(void)cluster_tt_status_hint_source_dispatch(CLUSTER_TT_HINT_SOURCE_EMIT, &hint_request);
}

static void
install_binding_aliases(const ClusterTTLocalBinding *binding, ClusterTTStatus status,
						SCN commit_scn)
{
	uint16 i;

	for (i = 0; i < binding->active_alias_count; i++) {
		ClusterTTStatusKey key;

		if (build_binding_key(binding, binding->active_alias_segments[i], &key))
			install_key(&key, status, commit_scn);
	}
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
	const ClusterTTLocalBinding *binding;
	int idx;

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
	idx = cluster_tt_local_find_binding(xid);
	if (idx < 0 || !build_local_key(xid, &key))
		return;
	binding = &cluster_tt_local_bindings[idx];

	/*
	 * spec-3.3 D6 (L181 chain step 2): install in-memory overlay entry
	 * with the real commit_scn (or InvalidScn on abort). The overlay
	 * carries commit_scn to (a) self-consumer N7 lookup and (b) the wire
	 * emit path which will ship it to peers in V2 hints (D8).
	 */
	install_key(&key, status, commit_scn);
	install_binding_aliases(binding, status, commit_scn);
}

/* ------------------------------------------------------------ */
/* public API                                                   */
/* ------------------------------------------------------------ */

static bool
cluster_tt_local_writable_admission(void)
{
	ClusterJoinGateVerdict verdict = cluster_reconfig_self_join_gate_verdict();

	if (verdict == CLUSTER_JOIN_GATE_BLOCK_53R61)
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_JOIN_REJECTED_STALE),
				 errmsg("cannot commit a transaction: this node's cluster join was rejected"),
				 errhint("Restart this node so it presents a fresh cluster incarnation.")));
	return verdict == CLUSTER_JOIN_GATE_ALLOW;
}


static void
cluster_tt_local_modifier_recheck_or_error(const ClusterSemanticAdmissionToken *token)
{
	if (!cluster_semantic_activation_modifier_recheck(token, cluster_tt_local_writable_admission()))
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
				 errmsg("cannot commit a transaction: cluster reconfiguration in progress"),
				 errhint("The transaction was refused before its commit record; retry is safe.")));
}

static bool
cluster_tt_local_copy_published(const ClusterTTLocalBinding *binding,
								ClusterCanonicalTxnBinding *out)
{
	if (binding == NULL || out == NULL || binding->publish_state != CLUSTER_CANONICAL_TXN_PUBLISHED
		|| !TransactionIdIsNormal(binding->top_xid) || binding->segment_id == 0
		|| binding->segment_generation == UINT32_MAX || binding->slot_offset >= TT_SLOTS_PER_SEGMENT
		|| binding->wrap == TT_WRAP_INVALID || XLogRecPtrIsInvalid(binding->active_lsn)
		|| binding->terminal_state != CLUSTER_TT_LOCAL_TERMINAL_NONE || binding->reserved8[0] != 0
		|| binding->reserved8[1] != 0)
		return false;

	memset(out, 0, sizeof(*out));
	out->segment_id = binding->segment_id;
	out->segment_generation = binding->segment_generation;
	out->xid = binding->top_xid;
	out->slot_offset = binding->slot_offset;
	out->slot_wrap = binding->wrap;
	out->origin_instance = (uint8)(cluster_node_id + 1);
	out->publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED;
	out->active_lsn = binding->active_lsn;
	return true;
}

bool
cluster_tt_local_get_published_binding(TransactionId top_xid,
									   ClusterCanonicalTxnBinding *binding_out)
{
	int idx;

	if (binding_out == NULL)
		return false;
	memset(binding_out, 0, sizeof(*binding_out));
	if (!cluster_enabled || cluster_node_id < 0 || !TransactionIdIsNormal(top_xid))
		return false;
	idx = cluster_tt_local_find_binding(top_xid);
	return idx >= 0
		   && cluster_tt_local_copy_published(&cluster_tt_local_bindings[idx], binding_out);
}

/* Only called after the publisher's normal, zero-side-effect retry result.
 * The old allocator has rolled away, so do not free/abort an old physical slot
 * or touch another backend's CURRENT.  Remove just this local candidate. */
static void
cluster_tt_local_forget_unpublished_reservation(TransactionId top_xid)
{
	int idx = cluster_tt_local_find_binding(top_xid);
	ClusterTTLocalBinding *binding;

	if (idx < 0)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("unpublished canonical reservation disappeared")));
	binding = &cluster_tt_local_bindings[idx];
	if (binding->publish_state != CLUSTER_CANONICAL_TXN_RESERVED
		|| binding->terminal_state != CLUSTER_TT_LOCAL_TERMINAL_NONE
		|| !XLogRecPtrIsInvalid(binding->active_lsn) || binding->active_alias_segments != NULL
		|| binding->active_alias_count != 0 || binding->active_alias_capacity != 0)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cannot discard a published or unproved canonical reservation")));
	memmove(binding, binding + 1,
			(cluster_tt_local_binding_count - (uint32)idx - 1) * sizeof(*binding));
	cluster_tt_local_binding_count--;
	memset(&cluster_tt_local_bindings[cluster_tt_local_binding_count], 0, sizeof(*binding));
}

bool
cluster_tt_local_prepare_canonical_active(TransactionId top_xid,
										  ClusterCanonicalTxnBinding *binding_out)
{
	uint64 retry_count = 0;

	if (binding_out == NULL)
		return false;
	memset(binding_out, 0, sizeof(*binding_out));
	if (!cluster_enabled || cluster_node_id < 0 || !TransactionIdIsNormal(top_xid))
		return false;

	for (;;) {
		ClusterSemanticAdmissionToken modifier_token;
		ClusterSemanticAdmissionResult admission;
		ClusterTTSlotCurrentOwner expected_owner;
		ClusterTTLocalBinding *binding;
		XLogRecPtr active_lsn;
		uint32 segment_id;
		uint32 segment_generation = UINT32_MAX;
		uint32 tt_slot_id;
		uint16 slot_offset;
		TTSlot successor;
		TTSlot zero_slot;
		volatile bool retry_unpublished = false;
		int idx;

		CHECK_FOR_INTERRUPTS();
		idx = cluster_tt_local_find_binding(top_xid);
		if (idx >= 0) {
			binding = &cluster_tt_local_bindings[idx];
			if (cluster_tt_local_copy_published(binding, binding_out))
				return true;
			if (binding->publish_state != CLUSTER_CANONICAL_TXN_RESERVED)
				ereport(ERROR,
						(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
						 errmsg("canonical ACTIVE publication is not reusable for transaction %u",
								binding->top_xid)));
		}

		if (!cluster_tt_local_reserve_binding(top_xid, &segment_id, &slot_offset, &tt_slot_id))
			return false;
		(void)tt_slot_id;
		idx = cluster_tt_local_find_binding(top_xid);
		Assert(idx >= 0);
		binding = &cluster_tt_local_bindings[idx];
		if (binding->publish_state != CLUSTER_CANONICAL_TXN_RESERVED
			|| binding->terminal_state != CLUSTER_TT_LOCAL_TERMINAL_NONE
			|| !XLogRecPtrIsInvalid(binding->active_lsn) || binding->active_alias_segments != NULL
			|| binding->active_alias_count != 0 || binding->active_alias_capacity != 0)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("canonical ACTIVE reservation has prior publication effects")));

		/* The exact candidate was captured under the allocation lock.  Do not
		 * require an unlocked rescan of a CURRENT index which may have moved.
		 * The durable producer revalidates before and inside block-zero XCUR. */
		memset(&expected_owner, 0, sizeof(expected_owner));
		expected_owner.segment_id = segment_id;
		expected_owner.slot_offset = slot_offset;
		expected_owner.xid = top_xid;
		expected_owner.wrap = binding->wrap;
		expected_owner.status = CTS_ACTIVE;
		binding->publish_state = CLUSTER_CANONICAL_TXN_PUBLISHING;

		admission = cluster_semantic_activation_modifier_enter(
			cluster_tt_local_writable_admission(), &modifier_token);
		if (admission != CLUSTER_SEMANTIC_ADMISSION_OK) {
			binding->publish_state = CLUSTER_CANONICAL_TXN_FAILED;
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
					 errmsg("cannot publish canonical ACTIVE during cluster reconfiguration")));
		}

		PG_TRY();
		{
			cluster_tt_local_modifier_recheck_or_error(&modifier_token);
			active_lsn = cluster_tt_slot_durable_publish_active(&expected_owner, &modifier_token,
																&segment_generation, &successor);
			cluster_tt_local_modifier_recheck_or_error(&modifier_token);
			if (XLogRecPtrIsInvalid(active_lsn)) {
				memset(&zero_slot, 0, sizeof(zero_slot));
				if (segment_generation != UINT32_MAX
					|| memcmp(&successor, &zero_slot, sizeof(successor)) != 0)
					ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
									errmsg("canonical ACTIVE retry returned publication effects")));
				/* A normal return certifies that this invocation never reserved
				 * CTRC or started BIND.  ERROR paths never reach this transition. */
				binding->publish_state = CLUSTER_CANONICAL_TXN_RESERVED;
				retry_unpublished = true;
			} else {
				if (segment_generation == UINT32_MAX || successor.status != TT_SLOT_ACTIVE
					|| successor.xid != top_xid || successor.wrap != binding->wrap
					|| successor.flags != TT_FLAGS_RESERVED || SCN_VALID(successor.commit_scn)
					|| !UBA_is_invalid(successor.first_undo_block))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("canonical ACTIVE publisher returned an invalid successor")));

				binding->segment_generation = segment_generation;
				binding->active_lsn = active_lsn;
				binding->publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED;
				if (!cluster_tt_local_copy_published(binding, binding_out))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("canonical ACTIVE publication receipt is inconsistent")));
			}
		}
		PG_CATCH();
		{
			binding->publish_state = CLUSTER_CANONICAL_TXN_FAILED;
			cluster_semantic_activation_leave(&modifier_token);
			PG_RE_THROW();
		}
		PG_END_TRY();

		cluster_semantic_activation_leave(&modifier_token);
		if (!retry_unpublished)
			return true;
		cluster_tt_local_forget_unpublished_reservation(top_xid);
		retry_count++;
		if (retry_count == 1 || (retry_count & (retry_count - 1)) == 0)
			ereport(
				LOG,
				(errmsg("canonical ACTIVE unpublished allocation reobserved"),
				 errdetail(
					 "PGRAC_FAMILY=TT_ACTIVE PGRAC_REASON=ALLOCATOR_ROLLED_BEFORE_BIND "
					 "node=%d xid=%u old_segment=%u current_segment=%u retry_count=" UINT64_FORMAT,
					 cluster_node_id, top_xid, segment_id,
					 cluster_tt_slot_current_segment(cluster_node_id), retry_count)));
	}
}

/* Legacy shape retained for non-heap callers, but it is now strictly
 * read-only and can never reserve or publish while a caller holds page locks. */
bool
cluster_tt_local_get_or_create_binding(TransactionId top_xid, uint32 *out_segment_id,
									   uint16 *out_slot_offset, uint32 *out_tt_slot_id)
{
	ClusterCanonicalTxnBinding binding;

	if (!cluster_tt_local_get_published_binding(top_xid, &binding))
		return false;
	*out_segment_id = binding.segment_id;
	*out_slot_offset = binding.slot_offset;
	*out_tt_slot_id = cluster_tt_slot_offset_to_id(binding.slot_offset);
	return true;
}

bool
cluster_tt_local_precommit_durable_finish(TransactionId xid, SCN commit_scn,
										  xl_xact_tt_commit *out_fold)
{
	ClusterSemanticAdmissionToken modifier_token;
	ClusterSemanticAdmissionResult admission;
	ClusterCanonicalTxnBinding binding;
	uint32 segment_id;
	uint32 segment_generation;
	uint16 slot_offset;
	uint16 wrap;
	uint8 owner;
	TTSlot successor;
	int idx;

	/*
	 * spec-3.11 D4 / C1: durably stamp commit_scn on this xact's TT slot in the
	 * undo segment header BEFORE the commit record (caller is the xact.c
	 * pre-commit hook; the commit record's flush makes it durable -- no
	 * independent fsync, C10).  No binding = DDL-only / read-only xact that
	 * never allocated a slot -> nothing durable; return false so the caller
	 * leaves XACT_XINFO_HAS_TT_COMMIT clear (WAL byte-identical to vanilla).
	 * Runs before reset_binding() (post-commit in record_commit), so the
	 * binding is still present.  C1b: this only stamps commit_scn; whether the
	 * xact actually committed is still decided by the commit record / CLOG.
	 *
	 * spec-3.18 D4.1: instead of emitting a standalone 0x30, write the slot via
	 * cluster_tt_slot_durable_commit_writeonly() and hand the equivalent delta
	 * back to RecordTransactionCommit, which folds it into the commit record.
	 * One record now carries both the TT stamp and CLOG commit -> they become
	 * durable atomically (no stamped-but-uncommitted window).  2PC keeps the
	 * standalone 0x30 (cluster_tt_slot_durable_commit) -- not this path.
	 */
	if (!cluster_tt_local_get_published_binding(xid, &binding))
		return false;
	idx = cluster_tt_local_find_binding(xid);
	if (idx < 0)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("canonical ACTIVE binding disappeared before commit")));
	segment_id = binding.segment_id;
	segment_generation = binding.segment_generation;
	slot_offset = binding.slot_offset;
	wrap = binding.slot_wrap;

	admission = cluster_semantic_activation_modifier_enter(cluster_tt_local_writable_admission(),
														   &modifier_token);
	if (admission != CLUSTER_SEMANTIC_ADMISSION_OK)
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
				 errmsg("cannot commit a transaction: cluster reconfiguration in progress"),
				 errhint("The transaction was refused before its commit record; retry is safe.")));

	PG_TRY();
	{
		/*
		 * spec-3.12 D2b: use the wrap captured at bind time, NOT a fresh
		 * cluster_tt_slot_get_wrap() -- a peer backend may have rolled this node's
		 * TT allocator to a new segment since this xact bound, which would make a
		 * fresh read of the (now stale) segment ERROR.  The slot is held ACTIVE for
		 * the whole xact, so its wrap cannot have changed.
		 */
		cluster_tt_local_modifier_recheck_or_error(&modifier_token);
		owner = cluster_tt_slot_durable_commit_writeonly(segment_id, segment_generation,
														 slot_offset, xid, wrap, commit_scn,
														 &modifier_token, &successor);

		/*
		 * Build the fold delta (mirrors xl_undo_tt_slot_commit fields).  xid is the
		 * committing top-xid (== slot owner for a non-prepared xact); instance is
		 * the slot's owner instance, needed for path resolution at redo.
		 */
		out_fold->segment_id = segment_id;
		out_fold->segment_generation = segment_generation;
		out_fold->xid = successor.xid;
		out_fold->slot_offset = slot_offset;
		out_fold->wrap = successor.wrap;
		out_fold->instance = owner;
		out_fold->format_version = CLUSTER_XACT_TT_COMMIT_VERSION;
		out_fold->flags = 0;
		out_fold->reserved = 0;
		out_fold->commit_scn = successor.commit_scn;
		cluster_tt_local_bindings[idx].terminal_state = CLUSTER_TT_LOCAL_TERMINAL_COMMIT_STAGED;
	}
	PG_FINALLY();
	{
		cluster_semantic_activation_leave(&modifier_token);
	}
	PG_END_TRY();
	return true;
}

bool
cluster_tt_local_preabort_durable_finish(TransactionId xid)
{
	ClusterSemanticAdmissionToken modifier_token;
	ClusterSemanticAdmissionResult admission;
	ClusterTTLocalBinding *binding;
	TTSlot successor;
	XLogRecPtr abort_lsn;
	int idx;

	idx = cluster_tt_local_find_binding(xid);
	if (idx < 0)
		return false;
	binding = &cluster_tt_local_bindings[idx];
	if (binding->publish_state == CLUSTER_CANONICAL_TXN_RESERVED
		&& binding->terminal_state == CLUSTER_TT_LOCAL_TERMINAL_NONE)
		return false;
	if (binding->publish_state != CLUSTER_CANONICAL_TXN_PUBLISHED)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("canonical transaction publication is unproved during abort"),
						errdetail("xid=%u publication=%u terminal=%u", xid, binding->publish_state,
								  binding->terminal_state)));
	if (binding->terminal_state == CLUSTER_TT_LOCAL_TERMINAL_ABORT_DURABLE)
		return true;
	if (binding->terminal_state != CLUSTER_TT_LOCAL_TERMINAL_NONE)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("canonical transaction terminal state cannot be aborted"),
						errdetail("xid=%u publication=%u terminal=%u", xid, binding->publish_state,
								  binding->terminal_state)));

	admission = cluster_semantic_activation_modifier_enter(cluster_tt_local_writable_admission(),
														   &modifier_token);
	if (admission != CLUSTER_SEMANTIC_ADMISSION_OK)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS),
						errmsg("cannot make canonical transaction abort durable during cluster "
							   "reconfiguration")));
	PG_TRY();
	{
		cluster_tt_local_modifier_recheck_or_error(&modifier_token);
		abort_lsn = cluster_tt_slot_durable_abort_exact(
			binding->segment_id, binding->segment_generation, binding->slot_offset,
			binding->top_xid, binding->wrap, &modifier_token, &successor);
		cluster_tt_local_modifier_recheck_or_error(&modifier_token);
		if (XLogRecPtrIsInvalid(abort_lsn) || successor.status != TT_SLOT_ABORTED
			|| successor.xid != binding->top_xid || successor.wrap != binding->wrap
			|| successor.flags != TT_FLAGS_RESERVED || SCN_VALID(successor.commit_scn)
			|| !UBA_is_invalid(successor.first_undo_block))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("canonical abort publisher returned an invalid successor")));
		binding->terminal_state = CLUSTER_TT_LOCAL_TERMINAL_ABORT_DURABLE;
	}
	PG_FINALLY();
	{
		cluster_semantic_activation_leave(&modifier_token);
	}
	PG_END_TRY();
	return true;
}

void
cluster_tt_local_record_commit(TransactionId xid, SCN commit_scn)
{
	int idx = cluster_tt_local_find_binding(xid);

	if (idx >= 0
		&& cluster_tt_local_bindings[idx].terminal_state == CLUSTER_TT_LOCAL_TERMINAL_COMMIT_STAGED)
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
	int idx = cluster_tt_local_find_binding(xid);
	uint32 i;

	/* Validate the entire batch before emitting even its first terminal hint
	 * or changing an allocator slot.  A failed publisher may have written WAL;
	 * dropping its binding is not proof that abort completed. */
	for (i = 0; i < cluster_tt_local_binding_count; i++) {
		const ClusterTTLocalBinding *binding = &cluster_tt_local_bindings[i];

		if (!TransactionIdIsValid(binding->top_xid))
			continue;
		if ((binding->publish_state == CLUSTER_CANONICAL_TXN_RESERVED
			 && binding->terminal_state == CLUSTER_TT_LOCAL_TERMINAL_NONE)
			|| (binding->publish_state == CLUSTER_CANONICAL_TXN_PUBLISHED
				&& binding->terminal_state == CLUSTER_TT_LOCAL_TERMINAL_ABORT_DURABLE))
			continue;
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cannot release a canonical transaction without abort proof"),
						errdetail("xid=%u publication=%u terminal=%u", binding->top_xid,
								  binding->publish_state, binding->terminal_state)));
	}

	if (idx >= 0
		&& cluster_tt_local_bindings[idx].terminal_state == CLUSTER_TT_LOCAL_TERMINAL_ABORT_DURABLE)
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

void
cluster_tt_local_record_data_active(TransactionId xid, UBA uba)
{
	ClusterTTLocalBinding *binding;
	uint32 record_segment;
	uint32 block_no;
	uint16 slot_offset;
	uint16 row_offset;
	int idx;

	if (!cluster_enabled || cluster_node_id < 0 || !TransactionIdIsNormal(xid))
		return;
	idx = cluster_tt_local_find_binding(xid);
	if (idx < 0)
		return;
	binding = &cluster_tt_local_bindings[idx];

	/* The writer made this UBA from the same binding before entering CRIT.
	 * If that invariant is ever broken, publish no guessed alias. */
	if (!uba_decode(uba, &record_segment, &block_no, &slot_offset, &row_offset)
		|| uba_origin_node_id(uba) != (NodeId)cluster_node_id
		|| slot_offset != binding->slot_offset) {
		Assert(false);
		return;
	}
	(void)block_no;
	(void)row_offset;

	if (record_segment != binding->segment_id) {
		uint16 i;

		for (i = 0; i < binding->active_alias_count; i++)
			if (binding->active_alias_segments[i] == (uint16)record_segment)
				break;

		if (i == binding->active_alias_count) {
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(TopTransactionContext);
			if (binding->active_alias_segments == NULL) {
				binding->active_alias_capacity = 4;
				binding->active_alias_segments
					= (uint16 *)palloc(sizeof(uint16) * binding->active_alias_capacity);
			} else if (binding->active_alias_count == binding->active_alias_capacity) {
				binding->active_alias_capacity *= 2;
				binding->active_alias_segments
					= (uint16 *)repalloc(binding->active_alias_segments,
										 sizeof(uint16) * binding->active_alias_capacity);
			}
			MemoryContextSwitchTo(oldcxt);
			binding->active_alias_segments[binding->active_alias_count++] = (uint16)record_segment;
		}
	}

	/* install_status publishes canonical + every registered alias.  Commit and
	 * abort use the same path, closing each alias terminally. */
	install_status(xid, CLUSTER_TT_STATUS_IN_PROGRESS, InvalidScn);
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

bool
cluster_tt_local_preabort_durable_finish(TransactionId xid)
{
	(void)xid;
	return false;
}

void
cluster_tt_local_record_active(TransactionId xid)
{
	(void)xid;
}

void
cluster_tt_local_record_data_active(TransactionId xid, UBA uba)
{
	(void)xid;
	(void)uba;
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
cluster_tt_local_prepare_canonical_active(TransactionId top_xid,
										  ClusterCanonicalTxnBinding *binding_out)
{
	(void)top_xid;
	if (binding_out != NULL)
		memset(binding_out, 0, sizeof(*binding_out));
	return false;
}

bool
cluster_tt_local_get_published_binding(TransactionId top_xid,
									   ClusterCanonicalTxnBinding *binding_out)
{
	(void)top_xid;
	if (binding_out != NULL)
		memset(binding_out, 0, sizeof(*binding_out));
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
