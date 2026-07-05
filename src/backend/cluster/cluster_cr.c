/*-------------------------------------------------------------------------
 *
 * cluster_cr.c
 *	  pgrac own-instance Consistent Read (CR) block construction.
 *
 *	  Stage 3 第 13 sub-spec (spec-3.9).  Top-level CR machinery:
 *	    - backend-local 8 KB scratch slot + non-reentrant guard
 *	    - ClusterCRShared shmem region (9 atomic counters)
 *	    - 2-layer API: cluster_cr_lookup_or_construct (top, spec-3.10 cache
 *	      hook) / cluster_cr_construct_block (bottom, always constructs)
 *	    - chain walker driver (Step 3) + tuple remap + CR-image visibility
 *	      helper (Step 4.5)
 *
 *	  Inverse-apply helpers live in cluster_cr_apply.c (Step 4).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.9-own-instance-cr-block-construction.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/transam.h"	   /* spec-3.11 D6/C1b: TransactionIdDidCommit, TransactionIdIsNormal */
#include "access/xact.h"	   /* spec-3.19 D3: TransactionIdIsCurrentTransactionId */
#include "storage/procarray.h" /* spec-3.19 D3: TransactionIdIsInProgress */
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/relfilelocator.h" /* spec-3.20 D3.A: RelFileLocatorEquals (F8 block-scope) */
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_admit.h" /* spec-5.52 D2: insert-side admission gate */
#include "cluster/cluster_cr_apply.h"
#include "cluster/cluster_cr_cache.h"
#include "cluster/cluster_cr_coordinator_stat.h" /* spec-5.57 D2/D3: coordinator boundary */
#include "cluster/cluster_cr_pool.h"			 /* spec-5.51 D4: shared L2 CR pool */
#include "cluster/cluster_cr_tuple.h" /* spec-5.54: tuple-level / verdict-only fast path */
#include "cluster/cluster_conf.h"	  /* spec-3.24 D1: cluster_conf_has_peers */
#include "cluster/cluster_guc.h"	  /* cluster_cr_chain_walk_max_steps, cluster_node_id */
#include "cluster/cluster_inject.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_recovery_merge.h" /* PGRAC: spec-4.5a is_materialized */
#include "cluster/cluster_remote_xact.h" /* PGRAC: spec-4.5a G5 outcome */ /* spec-3.21: cluster_itl_get_tt_ref (xmax overlay key) */
#include "cluster/cluster_touched_peers.h" /* PGRAC: spec-5.14 D2 class 4 */
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_resolver_cache.h"		/* spec-5.55 D0: by-xid scan measure hook */
#include "cluster/cluster_tt_durable.h"			/* spec-3.11 D6: watermark by-xid resolve */
#include "cluster/cluster_tt_slot.h"			/* spec-3.22: retention_off_recycle_count */
#include "cluster/cluster_undo_retention.h"		/* spec-3.22: retention horizon proof */
#include "cluster/cluster_visibility_resolve.h" /* spec-3.21: cluster_vis_cr_xmax_verdict */
#include "cluster/cluster_uba.h"
#include "cluster/cluster_cr_server.h" /* spec-6.12b CR-server data plane */
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_xnode_profile.h" /* spec-5.59 D3: profiling probes */


/*
 * ClusterCRShared -- per-instance shmem counters (spec-3.9 §2.5).
 *
 *	9 atomic counters, no LWLock (region lwlock_count = 0): counters are
 *	bumped with pg_atomic_fetch_add_u64 from the constructing backend,
 *	read lock-free by dump_state / pg_cluster_state.
 */
typedef struct ClusterCRShared {
	pg_atomic_uint64 cr_construct_count;
	pg_atomic_uint64 cr_snapshot_too_old_count;
	pg_atomic_uint64 cr_cross_instance_unsupported_count;
	pg_atomic_uint64 cr_corruption_count;
	pg_atomic_uint64 cr_chain_walk_steps_sum;
	pg_atomic_uint64 cr_inverse_insert_count;
	pg_atomic_uint64 cr_inverse_update_count;
	pg_atomic_uint64 cr_inverse_delete_count;
	pg_atomic_uint64 cr_inverse_itl_count;
	/* spec-3.10 D5: CR block cache (4 counters, 9 -> 13 cr-category rows). */
	pg_atomic_uint64 cr_cache_hit_count;
	pg_atomic_uint64 cr_cache_miss_count;
	pg_atomic_uint64 cr_cache_evict_count;
	pg_atomic_uint64 cr_cache_install_count;
	/*
	 * spec-3.22 D3: xmax recycled-slot resolve outcome split (4 buckets) so the
	 * 53R9F drop is observable and the residual fail-closed is monitorable
	 * (feeds spec-3.23).  resolved = exact commit_scn compared; recycled_invisible
	 * = durable 0-match proven below horizon -> invisible; invalid_or_ambiguous =
	 * delayed-cleanout / wrap residue fail-closed; scan_unavail_or_no_proof =
	 * degraded scan or a recycled 0-match without a valid retention proof.
	 */
	pg_atomic_uint64 cr_xmax_resolved_count;
	pg_atomic_uint64 cr_xmax_recycled_invisible_count;
	pg_atomic_uint64 cr_xmax_invalid_or_ambiguous_count;
	pg_atomic_uint64 cr_xmax_scan_unavail_or_no_proof_count;
	/*
	 * spec-6.12b: CR-server data plane (6 counters).  requester side: how the
	 * remote fetch resolved (full page / partial + local continue / failed ->
	 * unchanged 53R9G).  server side (bumped in the LMS construction and the
	 * LMON submit path): requests parked, results shipped full/partial, and
	 * refusals (GUC off, no free slot, block not resident, interleaved homes,
	 * construction error -> all fail-closed to the requester).
	 */
	pg_atomic_uint64 cr_remote_full_count;
	pg_atomic_uint64 cr_remote_partial_count;
	pg_atomic_uint64 cr_remote_failed_count;
	pg_atomic_uint64 cr_server_full_count;
	pg_atomic_uint64 cr_server_partial_count;
	pg_atomic_uint64 cr_server_denied_count;
	/*
	 * spec-6.12i: undo-TT fetch data plane (5 counters).  requester side:
	 * wire fetches that returned block + co-sampled authority, cache hits
	 * (L2 CR pool bytes + same-epoch per-backend authority memo), and
	 * fail-closed misses (GUC off / bad UBA / non-header block / timeout /
	 * DENIED / checksum / trailer missing -> the caller keeps 53R97).
	 * server side (bumped in the LMS drain): TT header blocks served with a
	 * co-sampled authority triple, and refusals.
	 */
	pg_atomic_uint64 rtvis_undo_fetch_wire_count;
	pg_atomic_uint64 rtvis_undo_fetch_cache_hit_count;
	pg_atomic_uint64 rtvis_undo_fetch_failclosed_count;
	pg_atomic_uint64 cr_server_undo_served_count;
	pg_atomic_uint64 cr_server_undo_denied_count;
	/*
	 * spec-6.12i CP3: recycled-slot active-runtime RESOLUTION verdicts (a
	 * level above the fetch counters): terminal proofs returned vs
	 * fail-closed refusals (fetch failed / covers gate / 0-match /
	 * multi-match / non-terminal -> the caller keeps 53R97).
	 */
	pg_atomic_uint64 rtvis_resolve_committed_count;
	pg_atomic_uint64 rtvis_resolve_aborted_count;
	pg_atomic_uint64 rtvis_resolve_failclosed_count;
	/*
	 * spec-6.12i CP5 (D-i4) / spec-6.15 D4: origin-verdict leg (5 requester
	 * counters + 2 server counters).  requester side: verdict wire round
	 * trips, verdict-leg fail-closed refusals (wire / covers gate / page
	 * validation), exact terminal verdicts consumed, below-horizon bounds
	 * consumed (leg (e) admissible), and below-horizon bounds REFUSED by
	 * leg (e) (read_scn behind the shipped horizon — the clock-skew
	 * diagnostic; the observe makes the next snapshot admissible).
	 * server side (LMS drain): complete-scan verdicts served vs refused.
	 */
	pg_atomic_uint64 rtvis_verdict_wire_count;
	pg_atomic_uint64 rtvis_verdict_failclosed_count;
	pg_atomic_uint64 rtvis_verdict_exact_count;
	pg_atomic_uint64 rtvis_verdict_below_horizon_count;
	pg_atomic_uint64 rtvis_verdict_inadmissible_count;
	pg_atomic_uint64 cr_server_verdict_served_count;
	pg_atomic_uint64 cr_server_verdict_denied_count;
	/*
	 * spec-6.15 D4: a recycled remote ref whose tuple xid the stripe
	 * derivation could not attribute (striping off / below the activation
	 * floor) — the active-runtime resolution never even asks the wire and
	 * the caller keeps 53R97 (t/347 L2 asserts this stays 0 on a fully
	 * striped cluster).
	 */
	pg_atomic_uint64 rtvis_underivable_failclosed_count;
} ClusterCRShared;

static ClusterCRShared *CRShared = NULL;

/*
 * Backend-local CR scratch slot (spec-3.9 Q3 / I-cr-1).
 *
 *	Single 8 KB reusable page allocated once in TopMemoryContext on first
 *	construction.  cluster_cr_construct_block returns a pointer into this
 *	slot; the pointer is valid only until the next construction call.
 *
 *	cr_in_progress is the non-reentrant guard (I-lock-3): nested CR
 *	construction in the same backend would clobber the shared scratch, so
 *	we Assert against it and keep the flag balanced across ereport via
 *	PG_TRY/PG_CATCH.
 */
static char *cr_scratch = NULL;
static bool cr_in_progress = false;

/*
 * spec-5.54 D2: a SEPARATE backend-local scratch page for the tuple-level
 * verdict-only fast path, distinct from cr_scratch so the fast path never shares
 * (and never has to reset) the full-block construct slot or its cr_in_progress
 * guard (Q5 / U18).  The fast path does not call cluster_cr_construct_block, so
 * the two scratch slots are never live simultaneously.
 */
static char *cr_tuple_scratch = NULL;


/* ============================================================
 * Shmem region (L206 5-step)
 * ============================================================ */

Size
cluster_cr_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterCRShared));
}

void
cluster_cr_shmem_init(void)
{
	bool found;

	CRShared = ShmemInitStruct("ClusterCRShared", cluster_cr_shmem_size(), &found);

	if (!found) {
		pg_atomic_init_u64(&CRShared->cr_construct_count, 0);
		pg_atomic_init_u64(&CRShared->cr_snapshot_too_old_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cross_instance_unsupported_count, 0);
		pg_atomic_init_u64(&CRShared->cr_corruption_count, 0);
		pg_atomic_init_u64(&CRShared->cr_chain_walk_steps_sum, 0);
		pg_atomic_init_u64(&CRShared->cr_inverse_insert_count, 0);
		pg_atomic_init_u64(&CRShared->cr_inverse_update_count, 0);
		pg_atomic_init_u64(&CRShared->cr_inverse_delete_count, 0);
		pg_atomic_init_u64(&CRShared->cr_inverse_itl_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cache_hit_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cache_miss_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cache_evict_count, 0);
		pg_atomic_init_u64(&CRShared->cr_cache_install_count, 0);
		pg_atomic_init_u64(&CRShared->cr_remote_full_count, 0);
		pg_atomic_init_u64(&CRShared->cr_remote_partial_count, 0);
		pg_atomic_init_u64(&CRShared->cr_remote_failed_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_full_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_partial_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_denied_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_undo_fetch_wire_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_undo_fetch_cache_hit_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_undo_fetch_failclosed_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_undo_served_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_undo_denied_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_resolve_committed_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_resolve_aborted_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_resolve_failclosed_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_verdict_wire_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_verdict_failclosed_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_verdict_exact_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_verdict_below_horizon_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_verdict_inadmissible_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_verdict_served_count, 0);
		pg_atomic_init_u64(&CRShared->cr_server_verdict_denied_count, 0);
		pg_atomic_init_u64(&CRShared->rtvis_underivable_failclosed_count, 0);
		pg_atomic_init_u64(&CRShared->cr_xmax_resolved_count, 0);
		pg_atomic_init_u64(&CRShared->cr_xmax_recycled_invisible_count, 0);
		pg_atomic_init_u64(&CRShared->cr_xmax_invalid_or_ambiguous_count, 0);
		pg_atomic_init_u64(&CRShared->cr_xmax_scan_unavail_or_no_proof_count, 0);
	}
}

static const ClusterShmemRegion cluster_cr_region = {
	.name = "pgrac cluster cr counters",
	.size_fn = cluster_cr_shmem_size,
	.init_fn = cluster_cr_shmem_init,
	.lwlock_count = 0, /* atomic counters only; no LWLock */
	.owner_subsys = "cluster_cr",
	.reserved_flags = 0,
};

void
cluster_cr_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cr_region);
}


/* ============================================================
 * Counter accessors (spec-3.9 §2.1)
 * ============================================================ */

#define CR_COUNTER_ACCESSOR(fn, field)                                                             \
	uint64 fn(void)                                                                                \
	{                                                                                              \
		if (CRShared == NULL)                                                                      \
			return 0;                                                                              \
		return pg_atomic_read_u64(&CRShared->field);                                               \
	}

CR_COUNTER_ACCESSOR(cluster_cr_construct_count, cr_construct_count)
CR_COUNTER_ACCESSOR(cluster_cr_snapshot_too_old_count, cr_snapshot_too_old_count)
CR_COUNTER_ACCESSOR(cluster_cr_cross_instance_unsupported_count,
					cr_cross_instance_unsupported_count)

/* PGRAC: spec-6.12b — CR-server side counter bump (LMS/LMON contexts). */
void
cluster_cr_server_stat_bump(ClusterCrServerStat which)
{
	if (CRShared == NULL)
		return;
	switch (which) {
	case CLUSTER_CR_SERVER_STAT_FULL:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_full_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_PARTIAL:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_partial_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_DENIED:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_denied_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_UNDO_SERVED:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_undo_served_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_UNDO_DENIED:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_undo_denied_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_VERDICT_SERVED:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_verdict_served_count, 1);
		break;
	case CLUSTER_CR_SERVER_STAT_VERDICT_DENIED:
		pg_atomic_fetch_add_u64(&CRShared->cr_server_verdict_denied_count, 1);
		break;
	}
}

/* PGRAC: spec-6.12i — requester-side undo-TT fetch outcome bumps (backend
 * context, cluster_runtime_visibility.c). */
void
cluster_rtvis_undo_fetch_note_wire(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_undo_fetch_wire_count, 1);
}

void
cluster_rtvis_undo_fetch_note_cache_hit(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_undo_fetch_cache_hit_count, 1);
}

void
cluster_rtvis_undo_fetch_note_failclosed(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_undo_fetch_failclosed_count, 1);
}

/* PGRAC: spec-6.12i CP3 — resolution-verdict bumps (backend context). */
void
cluster_rtvis_resolve_note_committed(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_resolve_committed_count, 1);
}

void
cluster_rtvis_resolve_note_aborted(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_resolve_aborted_count, 1);
}

void
cluster_rtvis_resolve_note_failclosed(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_resolve_failclosed_count, 1);
}

/* PGRAC: spec-6.12i CP5 (D-i4) — origin-verdict leg bumps (backend context,
 * cluster_runtime_visibility.c). */
void
cluster_rtvis_verdict_note_wire(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_verdict_wire_count, 1);
}

void
cluster_rtvis_verdict_note_failclosed(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_verdict_failclosed_count, 1);
}

void
cluster_rtvis_verdict_note_exact(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_verdict_exact_count, 1);
}

void
cluster_rtvis_verdict_note_below_horizon(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_verdict_below_horizon_count, 1);
}

void
cluster_rtvis_verdict_note_inadmissible(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_verdict_inadmissible_count, 1);
}

/* PGRAC: spec-6.15 D4 — underivable-origin refusal (classify_ref). */
void
cluster_rtvis_note_underivable_failclosed(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->rtvis_underivable_failclosed_count, 1);
}
CR_COUNTER_ACCESSOR(cluster_cr_corruption_count, cr_corruption_count)
CR_COUNTER_ACCESSOR(cluster_cr_chain_walk_steps_sum, cr_chain_walk_steps_sum)
CR_COUNTER_ACCESSOR(cluster_cr_inverse_insert_count, cr_inverse_insert_count)
CR_COUNTER_ACCESSOR(cluster_cr_inverse_update_count, cr_inverse_update_count)
CR_COUNTER_ACCESSOR(cluster_cr_inverse_delete_count, cr_inverse_delete_count)
CR_COUNTER_ACCESSOR(cluster_cr_inverse_itl_count, cr_inverse_itl_count)
CR_COUNTER_ACCESSOR(cluster_cr_cache_hit_count, cr_cache_hit_count)
CR_COUNTER_ACCESSOR(cluster_cr_cache_miss_count, cr_cache_miss_count)
CR_COUNTER_ACCESSOR(cluster_cr_cache_evict_count, cr_cache_evict_count)
CR_COUNTER_ACCESSOR(cluster_cr_cache_install_count, cr_cache_install_count)

/* spec-6.12b: CR-server data plane (6 counters). */
CR_COUNTER_ACCESSOR(cluster_cr_remote_full_count, cr_remote_full_count)
CR_COUNTER_ACCESSOR(cluster_cr_remote_partial_count, cr_remote_partial_count)
CR_COUNTER_ACCESSOR(cluster_cr_remote_failed_count, cr_remote_failed_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_full_count, cr_server_full_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_partial_count, cr_server_partial_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_denied_count, cr_server_denied_count)
/* spec-6.12i: undo-TT fetch data plane (5 counters). */
CR_COUNTER_ACCESSOR(cluster_rtvis_undo_fetch_wire_count, rtvis_undo_fetch_wire_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_undo_fetch_cache_hit_count, rtvis_undo_fetch_cache_hit_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_undo_fetch_failclosed_count, rtvis_undo_fetch_failclosed_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_undo_served_count, cr_server_undo_served_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_undo_denied_count, cr_server_undo_denied_count)
/* spec-6.12i CP3: resolution-verdict counters. */
CR_COUNTER_ACCESSOR(cluster_rtvis_resolve_committed_count, rtvis_resolve_committed_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_resolve_aborted_count, rtvis_resolve_aborted_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_resolve_failclosed_count, rtvis_resolve_failclosed_count)
/* spec-6.12i CP5 (D-i4): origin-verdict leg counters. */
CR_COUNTER_ACCESSOR(cluster_rtvis_verdict_wire_count, rtvis_verdict_wire_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_verdict_failclosed_count, rtvis_verdict_failclosed_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_verdict_exact_count, rtvis_verdict_exact_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_verdict_below_horizon_count, rtvis_verdict_below_horizon_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_verdict_inadmissible_count, rtvis_verdict_inadmissible_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_verdict_served_count, cr_server_verdict_served_count)
CR_COUNTER_ACCESSOR(cluster_cr_server_verdict_denied_count, cr_server_verdict_denied_count)
CR_COUNTER_ACCESSOR(cluster_rtvis_underivable_failclosed_count, rtvis_underivable_failclosed_count)
/* spec-3.22 D3: xmax recycled-slot resolve outcome buckets. */
CR_COUNTER_ACCESSOR(cluster_cr_xmax_resolved_count, cr_xmax_resolved_count)
CR_COUNTER_ACCESSOR(cluster_cr_xmax_recycled_invisible_count, cr_xmax_recycled_invisible_count)
CR_COUNTER_ACCESSOR(cluster_cr_xmax_invalid_or_ambiguous_count, cr_xmax_invalid_or_ambiguous_count)
CR_COUNTER_ACCESSOR(cluster_cr_xmax_scan_unavail_or_no_proof_count,
					cr_xmax_scan_unavail_or_no_proof_count)


/* ============================================================
 * Scratch slot helpers
 * ============================================================ */

/*
 * Ensure the backend-local scratch page is allocated (once, in
 * TopMemoryContext so it survives the lifetime of the backend).
 */
static void
cr_scratch_ensure(void)
{
	if (cr_scratch == NULL)
		cr_scratch = MemoryContextAllocZero(TopMemoryContext, BLCKSZ);
}


/*
 * cr_coordinator_refuse_runtime_remote -- spec-5.57 D2: the single fail-closed
 *	refusal routine for a class③ (runtime-warm remote) CR origin.  Shared by the
 *	CR-side pre-check (the real boundary, on a real remote UBA) AND the W2 test
 *	injection (a synthetic class③), so the injection exercises the exact runtime
 *	behavior the production pre-check uses.
 *
 *	One class③ refusal is BOTH the boundary headline (cross_instance_cr_refused)
 *	and specifically the remote-undo-read leg (remote_undo_read_refused); both
 *	bump by construction.  The rare W1 header-mismatch belt bumps only
 *	cross_instance_cr_refused, giving the invariant
 *	cross_instance_cr_refused >= remote_undo_read_refused.
 *
 *	NON-DEGRADABLE (rule 8.A, §2.2): the ereport fires under ANY GUC value; the
 *	GUC only gates the advisory counters / probe / LOG-once.  This function never
 *	returns (it always ereport(ERROR)s).  The data plane is Stage 6 (#119).
 */
static void
pg_attribute_noreturn() cr_coordinator_refuse_runtime_remote(int origin_node)
{
	if (cluster_cross_instance_cr_coordinator != CR_COORD_MODE_OFF) {
		cluster_cr_coordinator_stat_bump(CR_COORD_CROSS_INSTANCE_CR_REFUSED);
		cluster_cr_coordinator_stat_bump(CR_COORD_REMOTE_UNDO_READ_REFUSED);
	}
	/* D0 measure-leg: count the class③ hit (behavior unchanged -- still fails). */
	if (cluster_cross_instance_cr_probe)
		cluster_cr_coordinator_stat_bump(CR_COORD_CROSS_INSTANCE_BOUNDARY_PROBE);
	/* forward mode: LOG-once that the data plane is Stage 6 (L213: once per
	 * backend so the hot path is never flooded). */
	if (cluster_cross_instance_cr_coordinator == CR_COORD_MODE_FORWARD) {
		static bool forward_logged = false;

		if (!forward_logged) {
			forward_logged = true;
			elog(LOG, "cluster.cross_instance_cr_coordinator=forward is a contract "
					  "placeholder: cross-instance CR/undo data plane lands in Stage 6 "
					  "(#119); reads stay fail-closed (Spec: spec-5.57)");
		}
	}
	ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED),
					errmsg("cluster CR cross-instance UBA encountered at the remote-undo-read "
						   "leg (origin_node_id=%d, local=%d)",
						   origin_node, cluster_node_id),
					errhint("Own-instance CR only unless the origin was materialized by merged "
							"recovery; the runtime cross-instance CR/undo data plane lands in "
							"Stage 6 (#119 undo-block Cache Fusion); see Spec: spec-5.57.")));
}


/* ============================================================
 * Test injection hooks (spec-3.9 Step 7; SKIP-style precondition)
 * ============================================================ */

/*
 * cr_check_error_injections -- if a CR error injection point is armed, raise
 *	the CR code's OWN precise SQLSTATE (NOT the framework's generic XX000).
 *	Called at the top of the chain walker so it fires deterministically
 *	regardless of the actual undo chain (spec-3.9 v0.4 F8/F10).
 */
static void
cr_check_error_injections(void)
{
	uint64 param = 0;

	if (cluster_cr_injection_armed("cr_snapshot_too_old", &param))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
						errmsg("snapshot too old: CR cannot reconstruct (injected; segment %u)",
							   (uint32)param),
						errhint("test injection cr_snapshot_too_old; disarm with "
								"cluster_inject_fault('cr_snapshot_too_old','none',0).")));

	if (cluster_cr_injection_armed("cr_cross_instance", &param))
		/* spec-5.57 D2/D3: synthetic class③ refusal -- drive the SAME fail-closed
		 * routine the production pre-check uses (53R9G + both coordinator counters
		 * + probe/forward), so the TAP injection legs exercise the real boundary
		 * behavior deterministically (param = synthetic origin_node_id). */
		cr_coordinator_refuse_runtime_remote((int)param);

	if (cluster_cr_injection_armed("cr_corruption", &param)) {
		const char *kind = (param == 1)	  ? "uba_decode"
						   : (param == 2) ? "crc"
						   : (param == 3) ? "unknown_record_type"
						   : (param == 4) ? "chain_break"
										  : "unknown";

		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster CR undo chain corruption (injected; kind=%s)", kind),
						errhint("test injection cr_corruption param 1..4.")));
	}
}


/* ============================================================
 * Chain walker driver (body lands in Step 3)
 * ============================================================ */

/*
 * cr_walk_chain -- walk one per-transaction undo chain from start_uba backward
 *	and inverse-apply every undo record newer than read_scn onto scratch_page.
 *	*steps is a running total across all candidate chains in one construction
 *	(spec-3.10); cluster_cr_construct_block_into calls this once per chain.
 *
 *	Stop conditions + chain terminal taxonomy (spec-3.9 §3.1 I-chain-1..4):
 *	  - I-chain-1  write_scn not later than read_scn: the unconditional
 *	               normal stop; this record + everything older is already in
 *	               the snapshot.
 *	  - I-chain-2  invalid prev_uba (chain end) is a legal base state ONLY
 *	               when no record was applied (empty chain) or the last
 *	               applied record was an INSERT (the row was created after
 *	               read_scn and inverse-INSERT made it LP_UNUSED).
 *	  - I-chain-3  reaching chain end while still newer than read_scn after an
 *	               UPDATE/DELETE/ITL record => the older base needed to reach
 *	               read_scn is unreachable (most likely retention recycled)
 *	               => 53R9F snapshot_too_old (fail-closed, NEVER
 *	               silent-success).
 *	  - missing record (reader returns 0) => 53R9F.
 *	  - cross-instance origin => 53R9G.
 *	  - malformed UBA / short record / unknown record_type / step cap
 *	    exceeded => data_corrupted.
 *
 *	The inverse-apply bodies live in cluster_cr_apply.c (Step 4); the
 *	helpers return bool and the caller MUST ereport on false (I-fail-4).
 *
 *	Caller already memcpy'd the buffer page into scratch_page and holds the
 *	non-reentrant guard; this function performs no locking (I-lock-2).
 */
static void
cr_walk_chain(char *scratch_page, UBA start_uba, SCN read_scn,
			  const ClusterCRCandidateChain *chains, int nchains, uint32 *steps, uint32 max_steps,
			  RelFileLocator cur_locator, ForkNumber cur_fork, BlockNumber cur_block)
{
	UBA uba = start_uba;
	PGAlignedBlock record_buf;
	ClusterXpScope xp_scope; /* PGRAC: spec-5.59 D3 profiling */

	/* PGRAC: spec-5.59 D3 profiling -- nested breakdown under
	 * CLXP_R_CR_CONSTRUCT; every ereport(ERROR) path below simply loses the
	 * sample (scope is a stack variable, no cleanup needed). */
	cluster_xp_begin(&xp_scope, CLXP_R_CR_CHAIN_WALK);

	while (!UBA_is_invalid(uba)) {
		UndoRecordHeader *hdr;
		size_t len;
		uint32 seg;
		uint32 blk;
		uint16 tt_off;
		uint16 row_off;

		if (++(*steps) > max_steps)
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR chain walk exceeded %u steps", max_steps),
							errhint("chain walk infinite loop suspected; raise "
									"cluster.cr_chain_walk_max_steps if a hot row legitimately "
									"has a longer in-snapshot undo chain.")));

		if (!uba_decode(uba, &seg, &blk, &tt_off, &row_off))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR encountered a malformed UBA in the undo chain")));

		/*
		 * spec-5.57 D2 (Q11-A): CR-side remote-UBA pre-check -- the read-path
		 * coordinator boundary, applied BEFORE the undo read.  Derive the
		 * segment owner from the UBA and classify it (§0.1).  A runtime-warm
		 * cross-instance origin (class③: not own, not merged-materialized) is
		 * the net-new boundary: fail closed HERE with the canonical 53R9G,
		 * rather than letting cluster_undo_get_record() return 0 and conflate it
		 * with own-instance retention-recycled undo (53R9F below).  This is the
		 * W3 hardening: the remote-undo-read leg now fails closed with the SAME
		 * errcode as the W1 walker wall (errcode consolidation, CR-9).  It is
		 * NON-DEGRADABLE -- the ereport fires under any GUC value; the GUC only
		 * gates the advisory counters (rule 8.A, §2.2).  Own-instance is the
		 * common OLTP path: classify returns OWN and we fall straight through.
		 * The data plane (real remote undo fetch) is Stage 6 (#119).
		 * See Spec: spec-5.57 §3.1 (W3) / §2.1 (three roles).
		 */
		{
			NodeId cr_origin = uba_origin_node_id(uba);
			ClusterCrCoordOriginClass cr_origin_class
				= cluster_cr_coordinator_classify_origin(cr_origin);

			if (cr_origin_class == CR_COORD_ORIGIN_RUNTIME_REMOTE) {
				/* class③: fail closed via the shared refusal routine (53R9G +
				 * both coordinator counters; non-degradable, §2.2). */
				cr_coordinator_refuse_runtime_remote((int)cr_origin);
			} else if (cr_origin_class == CR_COORD_ORIGIN_MATERIALIZED_REMOTE) {
				/* class②: merged-materialized remote, served from the local tree
				 * (already shipped, spec-4.5a D8).  Count the serve (advisory). */
				if (cluster_cross_instance_cr_coordinator != CR_COORD_MODE_OFF)
					cluster_cr_coordinator_stat_bump(CR_COORD_MATERIALIZED_REMOTE_SERVED);
			}
		}

		len = cluster_undo_get_record(uba, record_buf.data, sizeof(record_buf.data));
		if (len == 0)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
					 errmsg("snapshot too old: CR cannot read undo record at segment %u block %u "
							"(recycled or retention exceeded)",
							seg, blk),
					 errhint("Own-instance retention (cluster.undo_retention_horizon_enabled) "
							 "keeps undo for active readers; this fires only past the best-"
							 "effort capacity bound or when the reader's read_scn predates the "
							 "horizon.")));
		if (len < sizeof(UndoRecordHeader))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR read a short undo record (%zu < %zu bytes)", len,
								   sizeof(UndoRecordHeader))));

		hdr = (UndoRecordHeader *)record_buf.data;

		/* Own-instance, or a merged-materialized remote instance whose undo
		 * lives in the local pg_undo/instance_<origin> tree (spec-4.5a D8).
		 * Anything else stays fail-closed (Spec: spec-5.57 §3.1 W1).
		 *
		 * This is the W1 belt: the spec-5.57 D2 segment-derived pre-check above
		 * (on uba_origin_node_id) catches the common class③ case BEFORE the undo
		 * read, so this header-origin check now fires only on the rare segment-
		 * vs-header mismatch (D8 §2.5 cross-check) -- defense-in-depth, never a
		 * silent assumption.
		 *
		 * spec-5.56 C4 (reconfig contract, §3.3): this carve-out is ALSO the
		 * fail-closed boundary that keeps the THIRD origin class — runtime warm
		 * remote (not own, not merged-materialized) — OUT of the CR pool: it never
		 * constructs (ERROR) so it never caches.  The two pool-eligible classes are
		 * reconfig-INVARIANT and need NO membership/remaster invalidation: (①)
		 * own-instance pages/undo are unchanged by reconfig; (②) merged-materialized
		 * remote undo is durable in the local tree with a reconfig-invariant
		 * merge_recovered_lsn authority, and an origin rejoin's NEW writes are new
		 * versions => new key => MISS (already fenced by C1/key).  read_scn is a
		 * GLOBAL SCN (AD-008), not a membership epoch (INV-C2).  spec-5.57 freezes
		 * this class③ fail-closed as the read-path coordinator boundary (CR-9); the
		 * runtime-warm-remote data plane lands in Stage 6 (#119). */
		if (hdr->origin_node_id != (uint16)cluster_node_id
			&& !cluster_merged_instance_is_materialized((int)hdr->origin_node_id)) {
			if (cluster_cross_instance_cr_coordinator != CR_COORD_MODE_OFF)
				cluster_cr_coordinator_stat_bump(CR_COORD_CROSS_INSTANCE_CR_REFUSED);
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED),
					 errmsg("cluster CR cross-instance UBA encountered "
							"(origin_node_id=%u, local=%d)",
							hdr->origin_node_id, cluster_node_id),
					 errhint("Own-instance CR only unless the origin was materialized by "
							 "merged recovery; the runtime cross-instance CR/undo data plane "
							 "lands in Stage 6 (#119 undo-block Cache Fusion); see "
							 "Spec: spec-5.57.")));
		}

		/* I-chain-1: normal SCN stop. */
		if (scn_time_cmp(hdr->write_scn, read_scn) <= 0)
			break;

		/*
		 * spec-3.20 D3.B: every physical INSERT/UPDATE/DELETE/ITL undo record is
		 * written with a valid target relation (cluster_undo_record.c sets it
		 * from the heap relation's locator).  A missing/invalid target locator is
		 * undo corruption, NOT a skippable cross-block record -- fail closed
		 * rather than silently drop it (which would hide the corruption and could
		 * leave a post-read_scn version unrolled).
		 */
		if (!RelFileNumberIsValid(hdr->target_locator.relNumber))
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR undo record has an invalid target relation"),
							errhint("undo chain corruption suspected; retry the transaction.")));

		/*
		 * spec-3.20 D3.A (F8): block-scope filter.  The prev_uba chain is
		 * transaction-GLOBAL -- a single txn (e.g. a TPC-B statement updating
		 * pgbench_accounts + _tellers + _branches + inserting _history) chains
		 * undo records across multiple relations/forks/blocks.  We are
		 * reconstructing ONE block (cur_locator/cur_fork/cur_block), so a record
		 * targeting any other relation/fork/block must NOT be inverse-applied to
		 * this scratch page: doing so lands a foreign old image at target_offset
		 * (DIFFLEN -> fail-closed, or -- worse -- a same-length SILENT overwrite
		 * that returns a wrong CR image, the spec-3.20 D0 P0/8.A finding).
		 *
		 * Skip-apply but KEEP walking prev_uba: the global chain can hold a
		 * deeper record that DOES belong to this block (records between read_scn
		 * and this block's head write_scn for other blocks are interleaved).
		 * Stopping here would truncate this block's legitimate history.  The
		 * header already carries the full physical target (HC213).
		 */
		if (!RelFileLocatorEquals(hdr->target_locator, cur_locator) || hdr->target_fork != cur_fork
			|| hdr->target_block != cur_block) {
			uba = hdr->prev_uba;
			continue;
		}

		switch (hdr->record_type) {
		case UNDO_RECORD_INSERT: {
			const UndoInsertPayload *p
				= (const UndoInsertPayload *)(record_buf.data + sizeof(UndoRecordHeader));

			if (!cluster_cr_apply_insert_inverse(scratch_page, hdr, p))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR insert inverse-apply failed")));
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_inverse_insert_count, 1);
			break;
		}
		case UNDO_RECORD_UPDATE: {
			const UndoUpdatePayload *p
				= (const UndoUpdatePayload *)(record_buf.data + sizeof(UndoRecordHeader));
			/* payload offsets are PAYLOAD-relative (heapam.c sets them to
			 * sizeof(...Payload)); base them at the payload start, not the
			 * record start which includes the UndoRecordHeader. */
			const char *old_bytes = (const char *)p + p->old_tuple_offset;

			if (sizeof(UndoRecordHeader) + (size_t)p->old_tuple_offset + p->old_tuple_length > len)
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR update payload old-tuple bytes out of bounds")));
			if (!cluster_cr_apply_update_inverse(scratch_page, hdr, p, old_bytes,
												 p->old_tuple_length))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR update inverse-apply failed")));
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_inverse_update_count, 1);
			break;
		}
		case UNDO_RECORD_DELETE: {
			const UndoDeletePayload *p
				= (const UndoDeletePayload *)(record_buf.data + sizeof(UndoRecordHeader));
			/* payload-relative offset (see UPDATE branch). */
			const char *full_bytes = (const char *)p + p->full_tuple_offset;

			if (sizeof(UndoRecordHeader) + (size_t)p->full_tuple_offset + p->full_tuple_length
				> len)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("cluster CR delete payload full-tuple bytes out of bounds")));
			if (!cluster_cr_apply_delete_inverse(scratch_page, hdr, p, full_bytes,
												 p->full_tuple_length))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR delete inverse-apply failed")));
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_inverse_delete_count, 1);
			break;
		}
		case UNDO_RECORD_ITL: {
			const UndoItlPayload *p
				= (const UndoItlPayload *)(record_buf.data + sizeof(UndoRecordHeader));

			if (len < sizeof(UndoRecordHeader) + sizeof(UndoItlPayload))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR ITL undo record too short")));
			if (!cluster_cr_apply_itl_inverse(scratch_page, hdr, p))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR ITL inverse-apply failed")));
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_inverse_itl_count, 1);
			break;
		}
		default:
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
							errmsg("cluster CR encountered unknown undo record type %u "
								   "(version skew or corruption)",
								   hdr->record_type)));
		}

		/*
		 * Keep the transient CR image close to the read_scn shape.  A hot row can
		 * have many post-read_scn intermediate versions on the same page
		 * (pgbench_tellers is the small-table repro): if the walk re-adds every
		 * intermediate image and defers pruning to the very end, the scratch page
		 * can temporarily overflow or present a foreign NORMAL tuple at a later
		 * restore offset.  Any tuple whose xmin is one of the candidate xids did
		 * not exist at read_scn, so it is safe to remove immediately after each
		 * inverse step, then compact before the next older undo record.
		 */
		(void)cluster_cr_prune_post_snapshot_versions(scratch_page, chains, nchains);
		PageRepairFragmentation((Page)scratch_page);

		uba = hdr->prev_uba;
	}

	/*
	 * Clean chain-end is a LEGITIMATE terminal: the ITL undo_segment_head chain
	 * is PER-TRANSACTION (threads every undo record under that slot's xact, NOT
	 * per-row), so an invalid prev_uba simply means the xact's undo is
	 * exhausted and the tuples it touched are back at read_scn.  A genuine
	 * truncation (retention recycled a still-needed older record) instead
	 * surfaces as cluster_undo_get_record() == 0 -> 53R9F inside the loop.
	 * *steps is a running total across all candidate chains; the caller
	 * (cluster_cr_construct_block_into) accumulates cr_chain_walk_steps_sum
	 * once after every chain completes.
	 */

	/* PGRAC: spec-5.59 D3 profiling */
	cluster_xp_end(&xp_scope);
}


/*
 * cr_resolve_kept_tuples_durable -- spec-3.11 D6/C4/C5.
 *
 * Called only when the block's ITL recycle watermark exceeds read_scn: a
 * completed DATA ITL slot whose write_scn is newer than the snapshot was
 * recycled out of this block, so cluster_cr_collect_candidate_chains above may
 * not have captured that writer and a post-read_scn tuple version it created
 * could survive the candidate prune as a false-visible (spec-3.10 §v0.5 A).
 *
 * For every still-NORMAL tuple whose xmin is NOT one of the live candidate
 * xids, resolve xmin's commit_scn from the durable TT by xid (spec-3.11 D2):
 *
 *   - committed (CLOG-confirmed -- C1b) and commit_scn is later than read_scn:
 *     the evicted post-read_scn creator -> prune (LP_UNUSED).
 *   - committed and commit_scn is not later than read_scn: a legitimate
 *     pre-read_scn version -> keep.
 *   - not committed per CLOG (aborted / still in flight at this read): the
 *     creator's row was not visible at read_scn -> prune.
 *   - durable lookup miss / ambiguous: cannot prove either way -> fail closed
 *     (53R9F).  Never leave a possibly-post-read_scn version visible (规则 8.A).
 *
 * This retires the spec-3.10 blanket fail-closed for every kept tuple whose
 * durable slot is still resolvable; only a tuple whose own slot was already
 * recycled (lookup miss) still fails closed -- spec-3.12 retention shrinks that
 * window.  Own-instance only (the watermark is a local-block property; remote
 * origins resolve via Cache Fusion in Stage 4).
 *
 * Mutates dst_page (LP_UNUSED marks); the caller leaves them unrepaired (the CR
 * image is read-only and visibility scans skip LP_UNUSED, matching prune-AFTER).
 */
static void
cr_resolve_kept_tuples_durable(char *dst_page, SCN read_scn, const ClusterCRCandidateChain *chains,
							   int nchains)
{
	Page page = (Page)dst_page;
	OffsetNumber off;
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

	for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off)) {
		ItemId lp = PageGetItemId(page, off);
		HeapTupleHeader htup;
		TransactionId xmin;
		SCN durable_scn = InvalidScn;
		bool is_candidate = false;
		int c;

		if (!ItemIdIsNormal(lp))
			continue;

		htup = (HeapTupleHeader)PageGetItem(page, lp);
		xmin = HeapTupleHeaderGetRawXmin(htup);

		/* Frozen / bootstrap xids predate any cluster snapshot -> visible. */
		if (!TransactionIdIsNormal(xmin))
			continue;

		/* xmin already covered by the candidate prune/walk -> nothing to do. */
		for (c = 0; c < nchains; c++) {
			if (chains[c].xid == xmin) {
				is_candidate = true;
				break;
			}
		}
		if (is_candidate)
			continue;

		/*
		 * xmin fell outside the (possibly incomplete) candidate set.  Resolve it
		 * durably by xid; a miss / ambiguity is unresolvable -> fail closed.
		 */
		if (!cluster_tt_slot_durable_lookup_by_xid(xmin, &durable_scn))
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
							errmsg("cluster CR cannot reconstruct block: durable TT slot for "
								   "writer xid %u is unavailable after ITL slot reuse",
								   xmin),
							errhint("retry the transaction with a fresh snapshot")));

		/*
		 * C1b (规则 8.A): the durable slot is stamped at pre-commit, so a
		 * COMMITTED stamp alone does not prove the xact committed.  Confirm via
		 * CLOG: an xact that stamped then aborted (or is still in flight at this
		 * read) was not visible at read_scn -> prune.
		 */
		if (!TransactionIdDidCommit(xmin)) {
			ItemIdSetUnused(lp);
			continue;
		}

		/* Committed after the snapshot -> evicted post-read_scn creator -> prune. */
		if (scn_time_cmp(durable_scn, read_scn) > 0)
			ItemIdSetUnused(lp);
		/* else committed at/before read_scn -> legitimate pre-read_scn -> keep. */
	}
}


/* ============================================================
 * 2-layer public API
 * ============================================================ */

/*
 * cr_construct_from_copy -- construction core over an ALREADY-COPIED page
 *	(spec-6.12b refactor of the spec-3.9/3.10/3.11 body; the Buffer wrapper
 *	below memcpy's the live page first and keeps its exact pre-6.12b
 *	semantics).
 *
 *	Modes:
 *	  local  (server_mode=false)  the classic backend construction.  NEW:
 *	         when cluster.crossnode_cr_data_plane is on and the NEWEST
 *	         candidate chain's undo home is a class-(3) runtime-remote
 *	         origin (the spec-5.57 boundary that otherwise refuses 53R9G),
 *	         fetch the CR result from that origin's CR-server first: a FULL
 *	         result replaces dst outright; a PARTIAL result replaces dst and
 *	         the construction CONTINUES here on the shipped page (candidates
 *	         re-derive from its ITL state; a still-foreign chain hits the
 *	         class-(3) walk backstop -> 53R9G).  A failed fetch falls into
 *	         the unchanged cr_coordinator_refuse_runtime_remote (Rule 8.A).
 *	  server (server_mode=true)   the LMS-side construction: classify the
 *	         write_scn-DESC chain homes (cluster_cr_server_split_classify),
 *	         refuse an interleave (53R9G -> the LMS wrapper turns any error
 *	         into a DENIED reply), peel only the self-home DESC prefix and
 *	         report *out_partial so the requester knows to continue.
 *
 *	The caller owns cr_in_progress + PG_TRY taxonomy + the profiling scope;
 *	this helper throws on every failure path (I-fail-4 discipline).
 */
static void
cr_construct_from_copy(char *dst_page, SCN read_scn, RelFileLocator cur_locator,
					   ForkNumber cur_fork, BlockNumber cur_block, bool server_mode,
					   bool *out_partial)
{
	Page page;
	const ClusterItlSlotData *slots;
	ClusterCRCandidateChain chains[CLUSTER_ITL_INITRANS_DEFAULT];
	int nchains;
	int peel_n;
	uint32 steps = 0;
	uint32 max_steps = (uint32)cluster_cr_chain_walk_max_steps;
	bool watermark_exceeds = false;

	if (out_partial != NULL)
		*out_partial = false;

	/* I-lock-1/2/4: caller holds the content lock (or made a stable copy);
	 * we only read page bytes in dst -- no buffer lock, no WAL, no dirty.
	 * The wait event covers the undo I/O of the chain walks. */
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_CR_CONSTRUCT);

	/* deterministic wait-event window for TAP L8 (armed us sleep). */
	{
		uint64 delay_us = 0;

		if (cluster_cr_injection_armed("cr_construct_delay_us", &delay_us) && delay_us > 0)
			pg_usleep((long)delay_us);
	}

	/* spec-3.9 Step 7: injection-forced taxonomy fires FIRST (TAP L4/L5/L6),
	 * before any page inspection, so it is page-state-independent. */
	cr_check_error_injections();

	page = (Page)dst_page;
	if (!PageHasItl(page))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("cluster CR target page has no ITL special area")));

	/*
	 * spec-3.10 (S)v0.5 / spec-3.11 D6: slot-reuse gate.  If a completed DATA
	 * ITL slot whose write_scn is newer than this reader's snapshot was
	 * recycled out of this block (its undo-chain anchor overwritten), the
	 * per-page candidate set may be incomplete and a post-read_scn tuple
	 * version could survive the candidate prune as a false-visible.
	 */
	{
		SCN recycle_wm = ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn;

		watermark_exceeds = SCN_VALID(recycle_wm) && scn_time_cmp(recycle_wm, read_scn) > 0;
	}

	/* Snapshot candidate chains BEFORE mutation, then peel newest-first. */
	slots = ClusterPageGetItlSlots(page);
	nchains = cluster_cr_collect_candidate_chains(slots, read_scn, chains,
												  CLUSTER_ITL_INITRANS_DEFAULT);
	if (nchains > 1)
		qsort(chains, nchains, sizeof(chains[0]), cluster_cr_chain_cmp_by_write_scn_desc);
	peel_n = nchains;

	if (server_mode) {
		/*
		 * PGRAC: spec-6.12b -- CR-server split.  One chain = one transaction
		 * = one undo home, so classifying the DESC-ordered head origins fully
		 * decides the one-hop serve (see cluster_cr_server.h).  DENY throws
		 * the canonical cross-instance SQLSTATE; the LMS wrapper converts any
		 * throw into a DENIED reply (fail-closed at the requester, Rule 8.A).
		 */
		int32 origins[CLUSTER_ITL_INITRANS_DEFAULT];
		int prefix = 0;
		ClusterCrServerSplit split;

		for (int i = 0; i < nchains; i++)
			origins[i] = (int32)uba_origin_node_id(chains[i].undo_segment_head);
		split = cluster_cr_server_split_classify(origins, nchains, (int32)cluster_node_id, &prefix);
		if (split == CLUSTER_CR_SPLIT_DENY)
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED),
					 errmsg("cluster CR server cannot construct: candidate chain homes "
							"interleave across instances"),
					 errhint("The one-page one-hop CR protocol preserves the write_scn-DESC "
							 "peel order only for a self-home prefix; the requester keeps the "
							 "fail-closed refusal.")));
		peel_n = prefix;
		if (out_partial != NULL)
			*out_partial = (split == CLUSTER_CR_SPLIT_PARTIAL);
	} else if (cluster_crossnode_cr_data_plane && nchains > 0) {
		/*
		 * PGRAC: spec-6.12b -- requester-side remote-first.  Only when the
		 * NEWEST chain is class-(3) remote: the DESC peel must start there, so
		 * a local peel could never come first (a local-prefix mix would need a
		 * page round-trip mid-construction -- out of the one-hop protocol;
		 * those interleaves keep the in-walk class-(3) backstop -> 53R9G).
		 */
		NodeId head_origin = uba_origin_node_id(chains[0].undo_segment_head);

		if (cluster_cr_coordinator_classify_origin(head_origin) == CR_COORD_ORIGIN_RUNTIME_REMOTE) {
			bool partial = false;
			BufferTag tag;

			InitBufferTag(&tag, &cur_locator, cur_fork, cur_block);
			if (!cluster_gcs_block_cr_fetch_and_wait(tag, read_scn, (int32)head_origin, dst_page,
													 &partial)) {
				if (CRShared != NULL)
					pg_atomic_fetch_add_u64(&CRShared->cr_remote_failed_count, 1);
				/* Unchanged spec-5.57 refusal: 53R9G + coordinator counters. */
				cr_coordinator_refuse_runtime_remote((int)head_origin);
			}

			if (!partial) {
				/* FULL: dst holds the origin-finished CR page (it already ran
				 * prune + walk + durable resolve there). */
				if (CRShared != NULL) {
					pg_atomic_fetch_add_u64(&CRShared->cr_remote_full_count, 1);
					pg_atomic_fetch_add_u64(&CRShared->cr_construct_count, 1);
				}
				pgstat_report_wait_end();
				return;
			}

			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_remote_partial_count, 1);

			/* PARTIAL: continue on the shipped page -- re-derive everything
			 * from its content (the peeled prefix chains' ITL slots were
			 * inverse-restored, so they no longer collect as candidates). */
			page = (Page)dst_page;
			if (!PageHasItl(page))
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
								errmsg("cluster CR server shipped a page without an ITL "
									   "special area")));
			{
				SCN recycle_wm = ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn;

				watermark_exceeds = SCN_VALID(recycle_wm) && scn_time_cmp(recycle_wm, read_scn) > 0;
			}
			slots = ClusterPageGetItlSlots(page);
			nchains = cluster_cr_collect_candidate_chains(slots, read_scn, chains,
														  CLUSTER_ITL_INITRANS_DEFAULT);
			if (nchains > 1)
				qsort(chains, nchains, sizeof(chains[0]), cluster_cr_chain_cmp_by_write_scn_desc);
			peel_n = nchains;
		}
	}

	/*
	 * If the recycle watermark says the candidate set may be incomplete,
	 * resolve evicted post-read_scn tuple creators before walking undo.
	 * Otherwise a recycled-slot tuple can remain NORMAL at the target
	 * offset and block an older UPDATE/DELETE restore with a length/identity
	 * mismatch.  The post-walk call below remains as the final fail-closed
	 * guard for tuples materialized by the chain walk itself.
	 */
	if (watermark_exceeds) {
		if (cluster_tt_durable_lookup)
			cr_resolve_kept_tuples_durable(dst_page, read_scn, chains, nchains);
		else
			ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
							errmsg("cluster CR cannot reconstruct block: ITL slot reused "
								   "after snapshot"),
							errhint("retry the transaction with a fresh snapshot")));
	}

	/*
	 * spec-3.10 (S)v0.6: prune post-read_scn versions BOTH before and after
	 * the inverse-apply (v0.4 pruned only after).  prune-FIRST +
	 * PageRepairFragmentation frees the line pointer a later INSERT reused so
	 * the inverse-apply can re-add the old image at its read_scn offnum; the
	 * prune-AFTER strips intermediate versions the walk restored.  In the
	 * spec-6.12b server-PARTIAL mode the prune still uses the FULL candidate
	 * list (pruning is xmin matching, no undo I/O) while the walk below peels
	 * only the self-home prefix -- the requester's continue-run re-prunes
	 * idempotently with its own re-collected list.
	 */
	(void)cluster_cr_prune_post_snapshot_versions(dst_page, chains, nchains);
	PageRepairFragmentation((Page)dst_page);

	for (int i = 0; i < peel_n; i++)
		cr_walk_chain(dst_page, chains[i].undo_segment_head, read_scn, chains, nchains, &steps,
					  max_steps, cur_locator, cur_fork, cur_block);

	(void)cluster_cr_prune_post_snapshot_versions(dst_page, chains, nchains);

	/*
	 * spec-3.11 D6/C4: if a completed post-read_scn DATA slot was evicted
	 * (watermark > read_scn), the candidate set above may miss its writer.
	 * Resolve every kept tuple whose xmin is not a live candidate against the
	 * durable TT by xid, pruning evicted post-read_scn versions and failing
	 * closed only on an unresolvable (recycled) slot.
	 */
	if (watermark_exceeds)
		cr_resolve_kept_tuples_durable(dst_page, read_scn, chains, nchains);

	pgstat_report_wait_end();

	if (CRShared != NULL) {
		pg_atomic_fetch_add_u64(&CRShared->cr_construct_count, 1);
		pg_atomic_fetch_add_u64(&CRShared->cr_chain_walk_steps_sum, steps);
	}
}

/*
 * cluster_cr_construct_block_into -- full-block CR into a caller-provided
 *	BLCKSZ destination (the CR cache victim slot in spec-3.10, or the backend
 *	scratch via the public wrapper).  memcpy the live page, then inverse-apply
 *	EVERY candidate ITL chain (write_scn newer than read_scn) in write_scn-DESC
 *	order so the whole block is rolled back to read_scn (Oracle block-level
 *	CR).  Body lives in cr_construct_from_copy (spec-6.12b refactor); the
 *	local-mode semantics are unchanged.
 *
 *	I-lock-3 non-reentrant + I-fail-1 balanced guard (PG_TRY resets
 *	cr_in_progress and re-throws the precise SQLSTATE).  Returns dst_page.
 */
static const char *
cluster_cr_construct_block_into(Buffer buf, SCN read_scn, char *dst_page)
{
	/* Init silences cppcheck uninitvar across the PG_RE_THROW longjmp. */
	const char *result = NULL;
	ClusterXpScope xp_scope; /* PGRAC: spec-5.59 D3 profiling */

	Assert(BufferIsValid(buf));
	Assert(dst_page != NULL);
	Assert(!cr_in_progress); /* I-lock-3 non-reentrant */

	/* PGRAC: spec-5.59 D3 profiling -- time the whole CR construction at its
	 * single choke point (cache-fill and scratch paths both land here); an
	 * ereport(ERROR) re-thrown by the PG_CATCH below loses the sample,
	 * acceptable. */
	cluster_xp_begin(&xp_scope, CLXP_R_CR_CONSTRUCT);

	cr_in_progress = true;

	PG_TRY();
	{
		RelFileLocator cur_locator;
		ForkNumber cur_fork;
		BlockNumber cur_block;

		/* spec-3.20 D3.A (F8): the physical identity of the block we are
		 * reconstructing, so cr_walk_chain can drop undo records the same
		 * transaction wrote against OTHER relations/forks/blocks. */
		BufferGetTag(buf, &cur_locator, &cur_fork, &cur_block);

		memcpy(dst_page, BufferGetPage(buf), BLCKSZ);
		cr_construct_from_copy(dst_page, read_scn, cur_locator, cur_fork, cur_block,
							   false /* local mode */, NULL);
		result = dst_page;
	}
	PG_CATCH();
	{
		/*
		 * spec-3.9 Step 6: centralized error-taxonomy counter bump.  Every
		 * failure path ereports a precise SQLSTATE (53R9F / 53R9G /
		 * data_corrupted); geterrcode() bumps the matching counter once per
		 * failed construction.  Injection-forced failures flow through here too.
		 */
		if (CRShared != NULL) {
			int sqlerr = geterrcode();

			if (sqlerr == ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD)
				pg_atomic_fetch_add_u64(&CRShared->cr_snapshot_too_old_count, 1);
			else if (sqlerr == ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED)
				pg_atomic_fetch_add_u64(&CRShared->cr_cross_instance_unsupported_count, 1);
			else if (sqlerr == ERRCODE_DATA_CORRUPTED)
				pg_atomic_fetch_add_u64(&CRShared->cr_corruption_count, 1);
		}

		pgstat_report_wait_end();
		cr_in_progress = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	cr_in_progress = false;
	cluster_xp_end(&xp_scope); /* PGRAC: spec-5.59 D3 */
	return result;
}

/*
 * cluster_cr_construct_page_for_server -- spec-6.12b LMS-side entry: full CR
 *	construction over a STABLE COPY of the current page (cur_page; the LMS
 *	drain made it with the raw-pin/content-lock copy helper), peeling only
 *	the self-home write_scn-DESC prefix.  *out_partial reports whether a
 *	foreign suffix remains for the requester.  Throws on every failure
 *	(interleave, snapshot-too-old, corruption); the LMS drain wrapper
 *	converts any throw into a DENIED reply -- fail-closed at the requester,
 *	never an LMS crash (Rule 8.A).
 */
void
cluster_cr_construct_page_for_server(const char *cur_page, SCN read_scn, BufferTag tag,
									 char *dst_page, bool *out_partial)
{
	Assert(cur_page != NULL && dst_page != NULL);
	Assert(!cr_in_progress); /* LMS is single-threaded; guard stays honest */

	cr_in_progress = true;

	PG_TRY();
	{
		RelFileLocator cur_locator = BufTagGetRelFileLocator(&tag);

		memcpy(dst_page, cur_page, BLCKSZ);
		cr_construct_from_copy(dst_page, read_scn, cur_locator, tag.forkNum, tag.blockNum,
							   true /* server mode */, out_partial);
	}
	PG_CATCH();
	{
		/* Same taxonomy bumps as the local wrapper (spec-3.9 Step 6). */
		if (CRShared != NULL) {
			int sqlerr = geterrcode();

			if (sqlerr == ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD)
				pg_atomic_fetch_add_u64(&CRShared->cr_snapshot_too_old_count, 1);
			else if (sqlerr == ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED)
				pg_atomic_fetch_add_u64(&CRShared->cr_cross_instance_unsupported_count, 1);
			else if (sqlerr == ERRCODE_DATA_CORRUPTED)
				pg_atomic_fetch_add_u64(&CRShared->cr_corruption_count, 1);
		}

		pgstat_report_wait_end();
		cr_in_progress = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	cr_in_progress = false;
}

const char *
cluster_cr_construct_block(Buffer buf, SCN read_scn)
{
	/* Public fallback: construct into the backend-local scratch.  Used by the
	 * test SRF and as the cache-disabled path (spec-3.10 (S)3.1). */
	cr_scratch_ensure();
	return cluster_cr_construct_block_into(buf, read_scn, cr_scratch);
}

/*
 * cr_build_cache_key -- CR cache identity for `buf` at `read_scn`.  base_page_lsn
 * is the live page LSN (bumped by every WAL-logged physical change incl.
 * HOT-prune / VACUUM), the version guard that forces a miss after a relayout
 * (spec-3.10 §3.2).  memset zeroes padding so the key compares cleanly.
 */
static ClusterCRCacheKey
cr_build_cache_key(Buffer buf, SCN read_scn)
{
	ClusterCRCacheKey key;

	memset(&key, 0, sizeof(key));
	BufferGetTag(buf, &key.rlocator, &key.forknum, &key.blockno);
	key.read_scn = read_scn;
	key.base_page_lsn = PageGetLSN(BufferGetPage(buf));
	return key;
}

/*
 * cr_note_retention_if_advanced -- spec-5.56 D2 (C3): PURE OBSERVATION on a
 * cache hit.  Reads the EXISTING undo retention horizon (no pin, no extension)
 * and, if it has advanced PAST this served image's read_scn, bumps the
 * retention_horizon_advance_noted counter.  This does NOT affect serving: the
 * materialized image is a key pure function (AD-006 / INV-R1), so recycling the
 * source undo never rewrites the already-materialized bytes — the image stays the
 * correct as-of-read_scn page.  The counter only quantifies that the §3.2
 * retention-recycle scenario really occurs (it is naturally near-unreachable
 * because an active reader@X pins the horizon <= X, INV-R2; U5 forces it via the
 * retention-off / forced-advance leg, F0-13b).  Cheap: a bool guard first, then
 * one atomic read only when retention is on. */
static inline void
cr_note_retention_if_advanced(SCN read_scn)
{
	SCN horizon;

	if (!cluster_undo_retention_horizon_enabled)
		return; /* retention off: nothing to compare (no hot-path cost) */
	horizon = cluster_undo_retention_horizon();
	if (SCN_VALID(horizon) && scn_time_cmp(horizon, read_scn) > 0)
		cluster_cr_pool_note_retention_horizon_advance();
}

/*
 * spec-5.53 D3 — MISS-forcing rules (the eight categories; rule 8.A: any one
 * uncertain → MISS → reconstruct, never a stale hit).  Every category below maps
 * to a concrete mechanism already present on this path and an observable
 * counter; the set is exhaustive (no relfilenode lifecycle / page version /
 * snapshot / slot-reuse case can over-hit):
 *
 *   trigger                              mechanism                         counter
 *   1 DROP → different relid reuse       pool_epoch bump (D2b floor)       epoch_mismatch
 *   2 TRUNCATE (same relid, new          old relfilenode unlink → bump     epoch_mismatch
 *     relfilenode)
 *   3 VACUUM FULL / CLUSTER (same        swap → old unlink → bump          epoch_mismatch
 *     relid, swapped relfilenode)
 *   4 page rewrite (HOT-prune /          base_page_lsn in key → key differs base_lsn_mismatch
 *     VACUUM relayout)
 *   5 base_page_lsn churn (hint bit /    base_page_lsn in key → key differs base_lsn_mismatch
 *     neighbour-row change)
 *   6 different read_scn (new snapshot)  read_scn in key → key differs      key_mismatch
 *   7 slot reuse / hash collision        field-wise key_equal + slot gen    key/generation_mismatch
 *   8 belt source unavailable / uncertain fail-closed MISS (D0=RED: no belt; epoch_mismatch
 *                                        the epoch floor is the fence)
 *
 * INV-K2 (fail-closed): if any signal is unavailable / inconsistent / uncertain,
 * MISS and reconstruct (cluster_cr_construct_block_into, spec-3.9) — a CR cache
 * may ALWAYS miss safely; it must NEVER false-hit.
 *
 * spec-5.53 D4 — ABA / lifecycle combination invariant.  A served image must
 * pass ALL of these independent guards; any single one failing forces a MISS, so
 * no single mechanism is load-bearing alone (defence in depth):
 *   (a) field-wise key_equal               (canonical, never memcmp; F0-6)
 *   (b) pool_epoch == current              (D2b floor; L1 per-entry + L2 per-slot
 *                                           = "L1+L2 same fence", D2c)
 *   (c) slot.generation == handle.generation  (two-phase publish ABA; F0-21)
 *   (d) commit-time epoch recheck          (serve-but-don't-cache if epoch moved
 *                                           during construction; spec-5.51 §3.4)
 * (D0=RED → there is no catalog-incarnation belt; correctness rests on the
 * provably-complete bump completeness of (b), honestly NOT a self-validating
 * token — see cluster_cr_cache.h / spec-5.53 §3.2.)
 */
const char *
cluster_cr_lookup_or_construct(Buffer buf, SCN read_scn)
{
	/*
	 * spec-3.10 D3 + spec-5.51 D4: two-level CR cache.
	 *
	 *  L1 = backend-local cache (cluster_cr_cache.c).  L2 = per-instance shared
	 *  CR pool (cluster_cr_pool.c), behind L1, consulted on an L1 miss.  L2 is
	 *  disabled by default (cluster_cr_pool_current_epoch() == 0); when disabled
	 *  this degrades EXACTLY to the spec-3.10 L1-only path (zero behavior
	 *  change).
	 *
	 *  Lifecycle epoch gate (spec-5.51 §3.4/§3.5, all returning paths checked):
	 *   - entry: capture start_epoch; if it changed since this backend last saw
	 *     it, reset L1 (a DROP/TRUNCATE/sinval since the last call may have left
	 *     stale-epoch L1 entries).
	 *   - L1 hit: recheck the epoch before returning; if it advanced mid-lookup,
	 *     reset L1 and fall through to reconstruct (never return a stale L1 hit).
	 *   - construct/publish: two-phase, construction OUTSIDE the L2 lock; an
	 *     ereport aborts the L2 reservation (PG_CATCH).
	 *   - commit: recheck the epoch; if it advanced during construction, serve
	 *     the (correct, just-built) image to the caller but skip caching it in
	 *     L1 (serve-but-skip-cache) so no stale-epoch entry persists.
	 */
	ClusterCRCacheKey key = cr_build_cache_key(buf, read_scn);
	uint64 start_epoch;
	uint64 start_rel_gen = 0; /* spec-5.56 D4: per-relation gen captured for ①/②;
							   * 0 = gen table disabled / locator unregistered */
	uint64 install_gen = 0;	  /* spec-5.56 D4: gen to stamp the constructed image */
	bool skip_cache = false;  /* spec-5.56 D4: gen-table full => serve-but-skip-cache */
	const char *hit;
	char *slot;
	bool evicted = false;
	int miss_reason = CR_CACHE_MISS_NONE;

	start_epoch = cluster_cr_pool_current_epoch(); /* 0 when L2 disabled */
	/* spec-5.56 D4: capture the locator's current per-relation generation for the
	 * composite {pool_epoch, rel_gen} fence (P1-c).  0 when the gen table is
	 * disabled or the locator is unregistered (then the fence is epoch-only and a
	 * registered entry, which always has gen >= 1, simply MISSes). */
	if (cluster_cr_pool_rel_generation_enabled())
		(void)cluster_cr_pool_rel_generation(key.rlocator, &start_rel_gen);

	/*
	 * spec-5.53 D2c: per-entry L1 lifecycle-epoch fence ("L1+L2 same fence").
	 * cluster_cr_cache_lookup serves an entry only when its stamped epoch ==
	 * start_epoch; a stale-epoch exact-key match is reported and counted, never
	 * served (rule 8.A: a relfilenode reuse can never serve a stale image).  This
	 * replaces the spec-5.51 D4 coarse whole-L1 flush with a finer per-entry
	 * reject (stale entries clock-evict lazily), mirroring the L2 per-slot epoch
	 * gate.  spec-5.53 D5: the linear L1 scan also attributes a miss to a churned
	 * page version / diverged snapshot for the over-miss diagnostics.
	 */
	/* spec-5.56 ①: L1 lookup under the COMPOSITE fence (epoch + start_rel_gen). */
	hit = cluster_cr_cache_lookup_fenced(&key, start_epoch, start_rel_gen, &miss_reason);
	switch (miss_reason) {
	case CR_CACHE_MISS_EPOCH:
		cluster_cr_pool_note_l1_epoch_mismatch(); /* L1 lifecycle fence fired */
		break;
	case CR_CACHE_MISS_BASE_LSN:
		cluster_cr_pool_note_base_lsn_mismatch();
		break;
	case CR_CACHE_MISS_KEY:
		cluster_cr_pool_note_key_mismatch();
		break;
	default:
		break;
	}
	if (hit != NULL) {
		/* spec-5.51 §3.4 + spec-5.56 ②: recheck the COMPOSITE fence for the
		 * within-call race (a global epoch bump OR a per-relation gen bump between
		 * the lookup and here makes the {start_epoch, start_rel_gen}-stamped entry
		 * stale).  start_epoch == 0 means the pool is disabled (no fence). */
		bool stale = false;

		if (start_epoch != 0) {
			if (cluster_cr_pool_current_epoch() != start_epoch) {
				stale = true;
			} else if (cluster_cr_pool_rel_generation_enabled()) {
				uint64 cur_gen = 0;

				(void)cluster_cr_pool_rel_generation(key.rlocator, &cur_gen);
				if (cur_gen != start_rel_gen)
					stale = true;
			}
		}
		if (!stale) {
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_cache_hit_count, 1);
			cr_note_retention_if_advanced(read_scn); /* spec-5.56 D2: observe only */
			return hit;
		}
		/* lifecycle event advanced mid-lookup: drop this now-stale hit and rebuild
		 * at the new (epoch, rel_gen) (the entry keeps its old stamp, so it can
		 * never be re-served). */
		cluster_cr_pool_note_l1_epoch_mismatch();
		start_epoch = cluster_cr_pool_current_epoch();
		start_rel_gen = 0;
		if (cluster_cr_pool_rel_generation_enabled())
			(void)cluster_cr_pool_rel_generation(key.rlocator, &start_rel_gen);
	}
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_cache_miss_count, 1);

	slot = cluster_cr_cache_victim_slot(&key, start_epoch, &evicted);
	if (evicted && CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_cache_evict_count, 1);

	/* spec-5.51 D4: L2 probe (copy-out into the L1 victim slot).  On an L2 hit,
	 * the image is now in L1; commit + return.  spec-5.56 ③ + P1-fix: lookup_copy
	 * returns the per-relation gen the copied bytes are valid for, CAPTURED UNDER
	 * THE POOL LOCK at copy time.  Stamp the L1 entry with THAT matched gen — NOT a
	 * fresh re-read of the current gen.  A racing unlink between the L2 copy and
	 * this commit advances the locator's current gen; re-reading it would brand the
	 * stale gen-N bytes with gen N+1, and a later L1 lookup at N+1 would pass the
	 * composite fence and serve a stale-lifecycle page (8.A false-visible).
	 * Stamping with the matched gen is self-fencing: any unlink past it (gen
	 * advances) makes the L1 entry MISS, so the stale page is never served. */
	{
		uint64 hit_gen = 0;

		if (start_epoch != 0 && cluster_cr_pool_lookup_copy_gen(&key, slot, &hit_gen)) {
			cluster_cr_cache_commit_slot_gen(hit_gen);
			if (CRShared != NULL)
				pg_atomic_fetch_add_u64(&CRShared->cr_cache_install_count, 1);
			cr_note_retention_if_advanced(read_scn); /* spec-5.56 D2: observe only */
			return slot;
		}
	}

	/* L1+L2 miss: construct.  Two-phase L2 insert with construction outside the
	 * L2 lock; an ereport mid-construction must release the L2 reservation. */
	{
		ClusterCRPoolHandle ph;
		bool reserved = false;

		/*
		 * spec-5.56 D4 / INV-G3 (register-at-install): register the locator in the
		 * per-relation generation table BEFORE caching into L1 OR L2, so ANY cached
		 * entry's locator is tracked and a later unlink fences it (the global epoch
		 * does NOT bump for a tracked unlink in GO mode).  Covers BOTH the L2
		 * reserve below AND the bare-construct L1 commit (the admission-reject /
		 * L2-disabled path still commits to L1).  A register failure (gen table
		 * full) -> serve-but-skip-cache: construct and return the correct image
		 * WITHOUT caching it in L1 or L2 (so the untracked locator has no entry and
		 * its unlink is a safe no-op, P2-d′; rel_gen_table_overflow++ in register).
		 * Disabled -> install_gen stays 0 (epoch-only fence; spec-5.53 behavior).
		 */
		if (start_epoch != 0 && cluster_cr_pool_rel_generation_enabled()) {
			if (!cluster_cr_pool_register_locator(key.rlocator, &install_gen))
				skip_cache = true;
		}

		/*
		 * spec-5.52 D2: insert-side admission gate.  Only reserve+publish into
		 * L2 when the admission predicate admits this image; a reject bypasses
		 * the pool and falls through to the bare-construct else branch (F0-2),
		 * serving the SAME correct image (INV-A1).  admit_all (the default) ->
		 * always admit == spec-5.51 v1 (zero behavior change).  The predicate is
		 * advisory only and never affects the served image.
		 */
		if (!skip_cache && start_epoch != 0) {
			ClusterCRAdmitCtx actx = { .scan_kind = cluster_cr_admit_current_scan_kind() };

			if (cluster_cr_pool_admit(&key, &actx))
				reserved = cluster_cr_pool_reserve_gen(&key, install_gen, &ph);
			/* spec-5.52 D9: record the admission decision (advisory counter). */
			cluster_cr_admit_stat_bump(cluster_cr_admit_last_reason());
		}

		if (reserved) {
			PG_TRY();
			{
				(void)cluster_cr_construct_block_into(buf, read_scn, slot);
				cluster_cr_pool_publish(&ph, slot);
				cluster_cr_admit_note_published(&key); /* spec-5.52 D5: relcap */
				/* publish consumed the reservation;  the PG_CATCH only runs on
				 * an exception before this point, so no post-block abort check
				 * is needed (no dead `reserved = false` here). */
			}
			PG_CATCH();
			{
				cluster_cr_pool_abort(&ph);
				PG_RE_THROW();
			}
			PG_END_TRY();
		} else {
			/* L2 disabled / admission reject / skip-cache: bare construct
			 * (spec-3.10 path).  Still committed to L1 below unless skip_cache. */
			(void)cluster_cr_construct_block_into(buf, read_scn, slot);
		}
	}

	/* spec-5.56 D4: gen table full -> serve-but-skip-cache.  The freshly-built
	 * image is correct; just do NOT cache it (no L1 commit) so the untracked
	 * locator keeps zero entries (INV-G3) and its later unlink is a safe no-op. */
	if (skip_cache)
		return slot;

	/* spec-5.51 §3.4 + spec-5.56 ④: commit-time COMPOSITE recheck.  If a lifecycle
	 * event happened during construction — the global epoch advanced (coarse) OR
	 * this relation's per-relation generation advanced (a per-relation unlink in GO
	 * mode, compared against install_gen per P2-e′ so a first-registration is not
	 * falsely skipped) — serve the freshly-built (correct) image but do NOT commit
	 * it to L1 (the slot stays invalid -> not served later), so no stale entry
	 * persists. */
	if (start_epoch != 0) {
		bool gen_moved = false;

		if (cluster_cr_pool_rel_generation_enabled()) {
			uint64 cur_gen = 0;

			(void)cluster_cr_pool_rel_generation(key.rlocator, &cur_gen);
			gen_moved = (cur_gen != install_gen);
		}
		if (cluster_cr_pool_current_epoch() != start_epoch || gen_moved)
			return slot;
	}

	cluster_cr_cache_commit_slot_gen(install_gen);
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_cache_install_count, 1);
	return slot;
}


/* ============================================================
 * Tuple remap (CR scratch page → HeapTupleData wrapper)
 * ============================================================ */

bool
cluster_cr_remap_tuple(const char *cr_page, OffsetNumber off, HeapTupleData *out_htup)
{
	Page page = (Page)cr_page;
	ItemId itemid;

	if (cr_page == NULL || out_htup == NULL)
		return false;
	if (off < FirstOffsetNumber || off > PageGetMaxOffsetNumber(page))
		return false;

	itemid = PageGetItemId(page, off);
	if (!ItemIdIsNormal(itemid))
		return false; /* LP_UNUSED / LP_DEAD / LP_REDIRECT: CR-removed or not a tuple */

	out_htup->t_len = ItemIdGetLength(itemid);
	out_htup->t_data = (HeapTupleHeader)PageGetItem(page, itemid);
	out_htup->t_tableOid = InvalidOid;
	ItemPointerSetOffsetNumber(&out_htup->t_self, off);
	/* Block number of t_self is filled by the caller (it knows the buffer's
	 * block); CR scratch alone does not carry it. */
	return true;
}


/* ============================================================
 * Tuple-level cluster visibility helpers (spec-3.9 Step 4.5)
 *
 *   spec-3.9 §5 places cluster_visibility_decide_tuple in the generic
 *   cluster visibility helper layer and cluster_visibility_decide_cr_tuple
 *   in cluster_cr.c.  linkdb has no standalone generic cluster_visibility.c
 *   (only cluster_visibility_inject.c), so both helpers live here for now;
 *   FLAG FOR USER CODEREVIEW — if a dedicated cluster_visibility.c is
 *   wanted, decide_tuple moves there unchanged.
 *
 *   Both decide visibility WITHOUT PG-native ProcArray/CLOG (AD-012 例外 9
 *   / spec-3.9 I-fail-2/3).  They use SCN-based decisions and the tuple
 *   header committed/invalid bits.
 *
 *   VISIBILITY SEMANTICS (MVP — flag for codereview):
 *     - decide_tuple is for the 3-tier fast-path exits, where the block is
 *       already at/before read_scn so the physical tuple IS the read_scn
 *       version.  When the tuple's ITL slot carries a valid commit_scn it
 *       defers to cluster_visibility_decide_by_scn(commit_scn, read_scn);
 *       otherwise it uses the tuple's xmin-committed / xmax-invalid bits.
 *     - decide_cr_tuple is for a CR image already reconstructed to read_scn:
 *       post-read_scn changes are undone, so the row is VISIBLE iff its
 *       image xmin is committed/frozen and its xmax is invalid.  No buffer,
 *       no hint-bit writes (I-lock-4).
 * ============================================================ */

/*
 * Pure tuple-header visibility approximation (no syscall, no ProcArray).
 *   VISIBLE iff xmin is committed/frozen AND xmax is invalid.
 */
static ClusterVisibilityDecision
cr_decide_by_infomask(HeapTupleHeader htup)
{
	bool xmin_committed = (htup->t_infomask & HEAP_XMIN_COMMITTED) != 0;
	bool xmax_invalid = (htup->t_infomask & HEAP_XMAX_INVALID) != 0
						|| !TransactionIdIsValid(HeapTupleHeaderGetRawXmax(htup));

	if (xmin_committed && xmax_invalid)
		return CLUSTER_VISIBILITY_VISIBLE;
	return CLUSTER_VISIBILITY_INVISIBLE;
}

ClusterVisibilityDecision
cluster_visibility_decide_tuple(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	HeapTupleHeader tup = htup->t_data;

	/*
	 * If the tuple's ITL slot carries an authoritative commit_scn, decide by
	 * SCN against the snapshot read_scn (cluster semantics).  Else fall back
	 * to the tuple-header committed/invalid bits.  Never consult
	 * ProcArray/CLOG (I-fail-2).
	 */
	if (BufferIsValid(buffer)) {
		Page page = BufferGetPage(buffer);

		if (PageHasItl(page) && tup->t_itl_slot_idx != CLUSTER_ITL_SLOT_UNALLOCATED
			&& tup->t_itl_slot_idx < CLUSTER_ITL_INITRANS_DEFAULT) {
			const ClusterItlSlotData *slot = &ClusterPageGetItlSlots(page)[tup->t_itl_slot_idx];

			if (SCN_VALID(slot->commit_scn) && SCN_VALID(snapshot->read_scn))
				return cluster_visibility_decide_by_scn(slot->commit_scn, snapshot->read_scn);
		}
	}

	return cr_decide_by_infomask(tup);
}

/*
 * spec-3.22: the xmax recycled-slot resolve outcome.  The exact scratch/overlay
 * sources plus the durable by-xid resolve (ClusterTTDurableResolve) collapse here
 * into the four cases the gate acts on.  INVALID_OR_AMBIGUOUS and SCAN_UNAVAILABLE
 * both fail closed; they stay distinct only for the D3 counter buckets.
 */
typedef enum ClusterCrXmaxResolve {
	CLUSTER_CR_XMAX_RESOLVED_SCN,		  /* exact commit_scn -> compare to read_scn */
	CLUSTER_CR_XMAX_RECYCLED,			  /* durable 0-match -> invisible IFF proof holds */
	CLUSTER_CR_XMAX_INVALID_OR_AMBIGUOUS, /* delayed-cleanout / wrap residue -> fail closed */
	CLUSTER_CR_XMAX_SCAN_UNAVAILABLE	  /* degraded / unreadable scan -> fail closed */
} ClusterCrXmaxResolve;

/*
 * cluster_cr_retention_proof_valid -- spec-3.22 §2.2: prove that a durable
 * 0-match (the deleter's TT slot was recycled) implies the deleter committed
 * below the retention horizon, hence before this reader's snapshot, hence the
 * delete is visible at read_scn (the CR tuple is INVISIBLE).  Returns true only
 * when EVERY leg holds; otherwise a 0-match is just missing information and the
 * caller fails closed (规则 8.A -- never a guess, never Option A):
 *
 *	(a) own-instance node (a durable scan was actually possible);
 *	(b) the retention GUC is on right now;
 *	(c) no COMMITTED slot was recycled ungated this incarnation -- an off-window
 *	    could have recycled a committed-after slot, so a 0-match would not prove
 *	    below-horizon (spec-3.22 retention_off_recycle_count);
 *	(d) the current horizon is valid (cluster enabled -- a real lower bound);
 *	(e) the horizon is not newer than this reader's read_scn -- i.e. the reader is
 *	    protected by the horizon, so a slot recycled below the horizon committed
 *	    before this reader's snapshot.  A normal backend reader publishes its
 *	    read_scn, so the horizon (min over live readers) never exceeds it; this
 *	    leg fails closed only for an offline/stale read_scn (R8).
 */
static bool
cluster_cr_retention_proof_valid(SCN read_scn)
{
	SCN horizon = InvalidScn;

	if (!cluster_cr_retention_proof_origin_legs(&horizon))
		return false; /* (a)-(d) */

	/* (e) horizon must not be newer than read_scn (scn_time_cmp keeps the gate). */
	return scn_time_cmp(horizon, read_scn) <= 0;
}

/*
 * cluster_cr_retention_proof_origin_legs -- spec-6.12i CP5 (D-i4): the
 * ORIGIN-side legs (a)-(d) of the spec-3.22 retention proof, extracted so
 * the origin-verdict serve (cluster_cr_server.c) can evaluate them for a
 * cross-instance requester whose leg (e) — "horizon not newer than MY
 * read_scn" — can only be decided at the requester against the shipped
 * horizon.  Returns true with *out_horizon = the current horizon when every
 * origin leg holds:
 *
 *	(a) own-instance node (a durable scan was actually possible here);
 *	(b) the retention GUC is on right now;
 *	(c) no COMMITTED slot was ever recycled ungated this incarnation;
 *	(d) the current horizon is valid.
 *
 * ORDERING CONTRACT (soundness of the shipped bound): the caller must
 * evaluate these legs — in particular sample the horizon — AFTER its
 * complete by-xid scan finished.  The horizon is monotonic, so any slot
 * recycled before a scanned block was read had its commit_scn at or below
 * the recycle-time horizon, itself at or below this sample; sampling BEFORE
 * the scan would let a recycle land between sample and scan whose
 * commit_scn exceeds the shipped bound (false-visible direction, Rule 8.A).
 */
bool
cluster_cr_retention_proof_origin_legs(SCN *out_horizon)
{
	SCN horizon;

	if (out_horizon != NULL)
		*out_horizon = InvalidScn;

	if (cluster_node_id < 0)
		return false; /* (a) */
	if (!cluster_undo_retention_horizon_enabled)
		return false; /* (b) currently off */
	if (cluster_tt_slot_retention_off_recycle_count() != 0)
		return false; /* (c) an ungated recycle happened this incarnation */

	horizon = cluster_undo_retention_horizon();
	if (!SCN_VALID(horizon))
		return false; /* (d) cluster disabled / no horizon */

	if (out_horizon != NULL)
		*out_horizon = horizon;
	return true;
}

/* spec-3.22 D3: xmax resolve outcome counters (lock-free; bumped at the gate). */
static void
cluster_cr_count_xmax_resolved(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_xmax_resolved_count, 1);
}
static void
cluster_cr_count_xmax_recycled_invisible(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_xmax_recycled_invisible_count, 1);
}
static void
cluster_cr_count_xmax_invalid_or_ambiguous(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_xmax_invalid_or_ambiguous_count, 1);
}
static void
cluster_cr_count_xmax_scan_unavail_or_no_proof(void)
{
	if (CRShared != NULL)
		pg_atomic_fetch_add_u64(&CRShared->cr_xmax_scan_unavail_or_no_proof_count, 1);
}

/*
 * cluster_cr_resolved_scn_is_acceptable -- spec-4.8 D3 acceptance PREDICATE for a
 *	Source 3 by-xid RESOLVED match: the pure verdict, with NO observability side
 *	effect.  Extracted (spec-5.55 D6) so the fresh-scan path AND a shared-resolver-
 *	cache memo hit reach the SAME verdict (gate (3), verdict-equivalent by
 *	construction -- they cannot drift).
 *
 *	The durable scan runs WRAP_ANY (a recycled scratch ITL slot carries no
 *	binding wrap), so a single COMMITTED match cannot tell a genuine commit from a
 *	2^32-wrapped raw-xid collision.  When retention is unreliable AND the match is
 *	below the CURRENT horizon it is wrap-suspect -> NOT acceptable (规则 8.A: a
 *	wrong deleter scn would false-hide a live row).  The reliability proof is the
 *	EXACT sticky condition (horizon enabled AND no retention-off recycle this
 *	incarnation), not an abstraction -- losing the sticky leg would re-trust a
 *	retention-off-window recycle.  Returns true to ACCEPT (RESOLVED).
 */
static bool
cluster_cr_resolved_scn_is_acceptable(SCN scn)
{
	bool retention_reliable = cluster_undo_retention_horizon_enabled
							  && cluster_tt_slot_retention_off_recycle_count() == 0;

	return !cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, scn,
											 cluster_undo_retention_horizon(), retention_reliable);
}

/*
 * cluster_cr_accept_resolved_scn -- the AUTHORITATIVE acceptance gate: the pure
 *	predicate above PLUS the existing wrap_generation_disambiguated observability
 *	bump on a fail-closed verdict.  Used by the fresh-scan path and the TRUST-mode
 *	memo hit -- both AUTHORITATIVE resolutions, so each bumps the base counter
 *	exactly once.
 *
 *	The MEASURE-mode memo probe (spec-5.55 Hardening v1.1, finding #1) must NOT use
 *	this wrapper: it is a non-authoritative shadow measurement that still falls
 *	through to the authoritative scan, so bumping here would DOUBLE-count the base
 *	wrap_generation_disambiguated counter on a wrap-suspect hit -- it calls the
 *	pure predicate instead.  Returns true to ACCEPT (RESOLVED).
 *
 *	spec-6.12i CP5 (D-i4) exports this gate: the origin-verdict serve
 *	(cluster_cr_server.c) vets its own complete-scan RESOLVED_SCN match with
 *	the SAME wrap-suspect acceptance before shipping a COMMITTED_EXACT
 *	verdict cross-instance (an authoritative resolution here too, so the
 *	observability bump semantics are identical).
 */
bool
cluster_cr_accept_resolved_scn(SCN scn)
{
	if (cluster_cr_resolved_scn_is_acceptable(scn))
		return true;
	cluster_tt_recovery_count_wrap_generation_disambiguated();
	return false; /* suspect -> fail closed */
}

/*
 * cluster_cr_resolve_xmax_commit_scn -- resolve the EXACT commit_scn of a
 * committed own-instance deleter (cr_xmax) recorded on a CR image, for the
 * spec-3.21 xmax-side visibility decision.  Returns true + *out_scn on success.
 *
 *	Used only for the committed-at/before-read_scn branch: the committed-AFTER-
 *	read_scn case is decided VISIBLE in the caller via the live slot (tier-2 has
 *	already proved the live deleter wrote after read_scn, hence its commit is also
 *	after read_scn -- the sound direction of write_scn, NOT the P1-a inverse).  So
 *	this resolver is reached only when the LIVE slot was recycled to a newer writer.
 *
 *	Exact-key source order (spec-3.21 D2; never a CLOG/write_scn proxy -- P1-a):
 *	  1. the CR SCRATCH page ITL slot at itl_idx, IFF slot.xid == cr_xmax;
 *	  2. the BOC / spec-3.6 overlay by exact key (scratch ITL ref + local_xid);
 *	  3. the durable TT by exact xid -- survives an ITL slot recycle.
 *	A slot/xid mismatch or InvalidScn at every source -> false, and the caller
 *	fails closed (53R9F); rule 8.A: an unresolved deleter is NEVER treated as a
 *	committed-before-read_scn delete (which would false-hide a live row).
 */
static ClusterCrXmaxResolve
cluster_cr_resolve_xmax_commit_scn(const char *cr_page, uint8 itl_idx, TransactionId cr_xmax,
								   SCN *out_scn)
{
	Page page = (Page)cr_page; /* read-only ITL access on the scratch image */
	uint32 expected_wrap = CLUSTER_TT_WRAP_ANY;
	/* spec-5.55 D1: Source 3 RESOLVED also reports the matched durable slot
	 * identity, cached as a position hint by the shared resolver cache (D5). */
	uint16 resolved_seg = 0;
	uint16 resolved_slot = 0;
	uint16 resolved_wrap = 0;

	*out_scn = InvalidScn;

	/* Sources 1+2 require the scratch slot to still hold cr_xmax exactly. */
	if (itl_idx != CLUSTER_ITL_SLOT_UNALLOCATED && itl_idx < CLUSTER_ITL_INITRANS_DEFAULT
		&& PageHasItl(page)) {
		const ClusterItlSlotData *slot = &ClusterPageGetItlSlots(page)[itl_idx];

		if (slot->xid == cr_xmax) {
			ClusterUndoTTSlotRef ref;

			/*
			 * Source 1: the rolled-back scratch slot's own commit stamp.  NB: a
			 * lock-only ITL undo record for this same slot index, inverse-applied
			 * during the chain walk, can restore prev_commit_scn (= InvalidScn)
			 * over cr_xmax's later-stamped commit_scn while leaving slot.xid ==
			 * cr_xmax (cluster_cr_apply_itl_inverse restores commit_scn/flags/
			 * undo_segment_head but not xid).  In that case this and Source 2
			 * (whose ref came from the same rolled-back slot) both miss, and
			 * Source 3 (durable scan by exact xid) is the true authoritative
			 * fallback.  Never a wrong scn -- a stale InvalidScn just falls through.
			 */
			if (SCN_VALID(slot->commit_scn)) {
				*out_scn = slot->commit_scn;
				return CLUSTER_CR_XMAX_RESOLVED_SCN;
			}

			/* Source 2: BOC / overlay by the exact key (ref + local_xid). */
			if (cluster_itl_get_tt_ref(page, itl_idx, &ref)) {
				ClusterTTStatusKey key;
				ClusterTTStatusResult result;

				memset(&key, 0, sizeof(key));
				key.origin_node_id = ref.origin_node_id;
				key.undo_segment_id = ref.undo_segment_id;
				key.tt_slot_id = ref.tt_slot_id;
				key.cluster_epoch = ref.cluster_epoch;
				key.local_xid = cr_xmax;

				if (cluster_tt_status_lookup_exact(&key, &result) && result.authoritative
					&& (result.status == CLUSTER_TT_STATUS_COMMITTED
						|| result.status == CLUSTER_TT_STATUS_CLEANED_OUT)
					&& SCN_VALID(result.commit_scn)) {
					*out_scn = result.commit_scn;
					return CLUSTER_CR_XMAX_RESOLVED_SCN;
				}
			}
		}
	}

	/*
	 * spec-5.55 D6: shared resolver cache search-shortcut.  In TRUST mode a memo
	 * hit re-validates the single hint slot in O(1) (gate (1)+(2)+(4)) and runs the
	 * PHYSICALLY SAME acceptance gate the fresh scan runs (gate (3)), resolving
	 * WITHOUT the O(segments) scan below -- verdict-equivalent by construction.  In
	 * MEASURE/off mode the authoritative scan still runs (measure still records the
	 * would-hit counters).  origin == own node (Source 3 is own-instance, gate (5)).
	 */
	{
		SCN hint_scn = InvalidScn;

		if (cluster_resolver_cache_probe((uint16)cluster_node_id, cr_xmax, &hint_scn)) {
			if (cluster_resolver_cache_trust()) {
				/*
				 * TRUST: the memo hit IS the authoritative resolution (no fresh
				 * scan follows), so it runs the side-effecting acceptance gate --
				 * byte-identical to the fresh-scan path (gate (3)); its
				 * wrap_generation_disambiguated bump replaces the fresh-scan bump.
				 */
				bool accepted = cluster_cr_accept_resolved_scn(hint_scn);

				/*
				 * spec-5.55 Hardening v1.1 (finding #2): the memo-path fail-closed
				 * branch is a defensive 2^32-wrap guard the natural MVCC workload
				 * cannot reach (a resolved deleter is always recent -> scn >=
				 * horizon -> never suspect).  This injection forces it closed so a
				 * TAP test proves the branch routes (count_acceptance(false) ->
				 * INVALID_OR_AMBIGUOUS) and the 8.A invariant still holds.  It does
				 * NOT bump the base wrap_generation_disambiguated counter (no real
				 * wrap occurred).  Disarmed in production -> byte-identical.
				 */
				if (accepted
					&& cluster_cr_injection_armed("cluster-cr-resolver-memo-suspect", NULL))
					accepted = false;

				cluster_resolver_cache_count_acceptance(accepted);
				if (!accepted) {
					*out_scn = InvalidScn; /* same fail-closed as the fresh-scan branch */
					return CLUSTER_CR_XMAX_INVALID_OR_AMBIGUOUS;
				}
				*out_scn = hint_scn; /* gate (1): durable re-read, verdict-equivalent */
				return CLUSTER_CR_XMAX_RESOLVED_SCN;
			}

			/*
			 * MEASURE (spec-5.55 Hardening v1.1, finding #1): record the would-hit
			 * acceptance outcome with the PURE predicate, NOT the side-effecting
			 * gate.  This shadow probe falls through to the authoritative scan
			 * below, which bumps wrap_generation_disambiguated itself; using the
			 * wrapper here would double-count that base TT counter on a wrap-suspect
			 * hit.  The verdict is identical (the wrapper is predicate + side
			 * effect), so the resolver-cache acceptance counters stay accurate.
			 */
			cluster_resolver_cache_count_acceptance(
				cluster_cr_resolved_scn_is_acceptable(hint_scn));
			/* never trust the hint -- fall through to the authoritative scan. */
		}
	}

	/*
	 * Source 3: durable TT by exact xid (survives ITL slot recycle).  spec-3.22:
	 * consume the finer-grained resolve enum so a 0-match (RECYCLED_ZERO_MATCH ->
	 * provably below horizon, IF the gate's retention proof holds) is no longer
	 * conflated with a delayed-cleanout / wrap / unreadable miss (all fail closed).
	 */
	switch (cluster_tt_slot_durable_resolve_by_xid(cr_xmax, expected_wrap, out_scn, &resolved_seg,
												   &resolved_slot, &resolved_wrap)) {
	case CLUSTER_TT_DURABLE_RESOLVED_SCN:
		/*
		 * spec-4.8 D3 acceptance (now the shared cluster_cr_accept_resolved_scn
		 * helper, spec-5.55 D6, gate (3)): the durable scan ran WRAP_ANY so a
		 * single COMMITTED match below the horizon with unreliable retention is
		 * wrap-suspect -> fail closed (规则 8.A: a wrong deleter scn would false-
		 * hide a live row).  A memo hit above ran the SAME helper.
		 */
		if (!cluster_cr_accept_resolved_scn(*out_scn)) {
			*out_scn = InvalidScn;
			return CLUSTER_CR_XMAX_INVALID_OR_AMBIGUOUS;
		}
		/*
		 * spec-5.55 D5: cache the matched {seg,slot,wrap} position hint (own-
		 * instance gate (5) + COMMITTED gate (6) + acceptance-passed) so a peer
		 * backend can re-validate it in O(1) instead of re-scanning every segment.
		 * No-op unless the memo region is live.
		 */
		cluster_resolver_cache_install((uint16)cluster_node_id, cr_xmax, resolved_seg,
									   resolved_slot, resolved_wrap, *out_scn);
		return CLUSTER_CR_XMAX_RESOLVED_SCN; /* *out_scn set by the resolve */
	case CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH:
		*out_scn = InvalidScn;
		return CLUSTER_CR_XMAX_RECYCLED;
	case CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN:
	case CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP:
		*out_scn = InvalidScn;
		return CLUSTER_CR_XMAX_INVALID_OR_AMBIGUOUS;
	case CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE:
	default:
		*out_scn = InvalidScn;
		return CLUSTER_CR_XMAX_SCAN_UNAVAILABLE;
	}
}

ClusterVisibilityDecision
cluster_visibility_decide_cr_tuple(HeapTuple htup, Snapshot snapshot)
{
	HeapTupleHeader tup = htup->t_data;

	(void)snapshot; /* CR image already reconstructed to read_scn */

	/*
	 * The CR image tuple is the row as of read_scn: the chain walker has
	 * undone every change newer than read_scn.  Reasoning about
	 * visibility on the reconstructed image:
	 *
	 *   - A tuple that was INSERTED after read_scn was inverse-INSERTed to
	 *     LP_UNUSED, so it never reaches here (cluster_cr_remap_tuple returns
	 *     false -> the gate caller treats it invisible).  Therefore any tuple
	 *     present in the image existed at read_scn.
	 *   - A tuple DELETED after read_scn was inverse-DELETEd, restoring the
	 *     pre-delete image with xmax cleared -> visible.
	 *   - A tuple DELETED at/before read_scn keeps its xmax in the image
	 *     (the delete is not undone, SCN stop) -> invisible.
	 *
	 * So this image-level decision reduces to "is the reconstructed tuple
	 * still live (xmax invalid) as of read_scn".  We deliberately do NOT
	 * require the HEAP_XMIN_COMMITTED hint bit: it is a lazy optimization that
	 * is often unset in a captured undo image, and the chain walk already
	 * encodes the xmin-after-read_scn case via inverse-INSERT.
	 *
	 * The xmin-side (creation) check — a tuple still present in the image whose
	 * inserting xact committed AFTER read_scn (the UPDATE new-version and the
	 * H6 insert-then-late-commit cases) — is handled by the caller gate
	 * (cluster_cr_satisfies_mvcc, spec-3.9 Hardening L214) using the tuple's
	 * live ITL slot write_scn, because that needs the buffer/slot which this
	 * image-only helper deliberately does not take (I-lock-4 / I-fail-3).
	 */
	if (tup->t_infomask & HEAP_XMAX_INVALID)
		return CLUSTER_VISIBILITY_VISIBLE;
	if (!TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tup)))
		return CLUSTER_VISIBILITY_VISIBLE;
	return CLUSTER_VISIBILITY_INVISIBLE;
}


/* ============================================================
 * MVCC 3-tier short-circuit gate (spec-3.9 Step 5)
 * ============================================================ */

/*
 * spec-3.24 D1: no-peer + session-local CR-gate fast path.  The pure verdict
 * (cluster_cr_no_peer_fastpath_decide) is unit-tested as a full truth table;
 * the eligible() wrapper binds it to live GUC / topology / snapshot state and
 * yields to a forced-CR test override.  See cluster_cr.h for the soundness
 * contract (AD-012 例外 9 row #1).
 */
/* Backend-lifetime cache for cluster_merged_any_remote_materialized(). */
static bool fastpath_any_materialized_known = false;
static bool fastpath_any_materialized = false;

bool
cluster_cr_no_peer_fastpath_decide(bool gate_on, bool has_peers, bool session_local,
								   bool has_materialized_remote)
{
	return gate_on && !has_peers && session_local && !has_materialized_remote;
}

bool
cluster_cr_no_peer_fastpath_eligible(Snapshot snapshot)
{
	/*
	 * Fail-closed: only a CLUSTER-source snapshot can take this path.  The
	 * caller (HeapTupleSatisfiesMVCC) already gates on cluster_source, but a
	 * LOCAL / static snapshot carries cluster_snapshot_session_local as plain
	 * padding -- guard here too so no other caller can fast-path one.
	 */
	if (snapshot == NULL || snapshot->cluster_source != (uint8)SNAPSHOT_SOURCE_CLUSTER)
		return false;

#ifdef ENABLE_INJECTION
	/* A forced-CR test must still exercise the cluster path. */
	if (cluster_test_force_visibility_cluster_path)
		return false;
#endif

	/*
	 * Fail-closed topology check: the fast path requires the topology to be
	 * KNOWN single-node.  cluster_conf_has_peers() alone is fail-OPEN here:
	 * it returns false while ClusterConfShmem is NULL or not yet populated
	 * (conf not loaded), which would route a multi-node deployment through
	 * PG-native during that window (t/203/208/209 ClusterPair nodes caught
	 * exactly this).  cluster_conf_node_count() == 1 holds only after a
	 * successful load that declared exactly this node (single-node degraded
	 * fallback included); 0 (not loaded) and >1 (peers) are both ineligible.
	 */
	if (cluster_conf_node_count() != 1)
		return false;

	/*
	 * spec-4.5a G6 (8.A): merged recovery may have materialized a dead
	 * peer's data on this node.  Those tuples carry foreign xids that must
	 * resolve through the origin-qualified authority (durable TT /
	 * pg_xact_remote); the PG-native body would alias them into this
	 * node's own CLOG (AD-012 例外 9).  One registry scan per backend:
	 * materialization only changes during startup recovery (no live
	 * backends), so the cache cannot go stale in the unsafe direction.
	 */
	if (!fastpath_any_materialized_known) {
		fastpath_any_materialized = cluster_merged_any_remote_materialized();
		fastpath_any_materialized_known = true;
	}

	return cluster_cr_no_peer_fastpath_decide(
		cluster_cr_gate_no_peer_fastpath, cluster_conf_has_peers(),
		snapshot->cluster_snapshot_session_local != 0, fastpath_any_materialized);
}

/*
 * cluster_cr_verdict_on_image -- decide the per-tuple visibility verdict of the
 * queried offnum on a reconstructed read_scn CR image (spec-3.9/3.21/3.22 logic,
 * extracted in spec-5.54 D4 so the full-block gate AND the tuple-level fast path
 * run the IDENTICAL verdict on their respective images -> INV-T1 bit-equivalence
 * by construction).  `slot` is the LIVE ITL slot of the queried tuple (drives the
 * tier-2 + live-slot shortcuts), NOT a scratch slot.  Returns DECIDED + out_visible,
 * FAILCLOSED (materialized-remote INDOUBT only), or ereports the precise SQLSTATE
 * on an own-instance unresolvable deleter (I-fail-1).  No CR construct here (the
 * caller already built cr_page), so it is reentrancy-safe for the fast path.
 */
static ClusterCrVerdict cluster_cr_verdict_on_image(const char *cr_page, OffsetNumber offnum,
													const ClusterItlSlotData *slot,
													Snapshot snapshot, bool remote_materialized,
													int32 remote_origin, bool *out_visible);

ClusterCrVerdict
cluster_cr_satisfies_mvcc(HeapTuple htup, Snapshot snapshot, Buffer buffer, bool *out_visible)
{
	HeapTupleHeader tup = htup->t_data;
	Page page;
	PageHeader phdr;
	uint8 itl_idx;
	const ClusterItlSlotData *slot;
	const char *cr_page;
	bool remote_materialized = false;
	int32 remote_origin = -1;

	/*
	 * Master switch (default on after spec-3.9 Hardening v1.0.1).  Turning it
	 * off is a diagnostic escape hatch; normal cluster snapshots take the CR
	 * path when the page/ITL SCN gates prove the tuple was modified after
	 * read_scn.
	 */
	if (!cluster_cr_mvcc_gate)
		return CLUSTER_CR_NOT_APPLICABLE;

	if (!cluster_enabled || !BufferIsValid(buffer)
		|| snapshot->cluster_source != (uint8)SNAPSHOT_SOURCE_CLUSTER)
		return CLUSTER_CR_NOT_APPLICABLE;

	page = BufferGetPage(buffer);
	if (!PageHasItl(page))
		return CLUSTER_CR_NOT_APPLICABLE;

	itl_idx = tup->t_itl_slot_idx;
	if (itl_idx == CLUSTER_ITL_SLOT_UNALLOCATED || itl_idx >= CLUSTER_ITL_INITRANS_DEFAULT)
		return CLUSTER_CR_NOT_APPLICABLE;

	phdr = (PageHeader)page;

	/* Tier 1 (page gate): block already at/before snapshot -> not our case;
	 * the existing visibility path / PG-native body handles it. */
	if (!SCN_VALID(phdr->pd_block_scn) || !SCN_VALID(snapshot->read_scn))
		return CLUSTER_CR_NOT_APPLICABLE;
	if (scn_time_cmp(phdr->pd_block_scn, snapshot->read_scn) <= 0)
		return CLUSTER_CR_NOT_APPLICABLE;

	slot = &ClusterPageGetItlSlots(page)[itl_idx];

	/* Tier 2 (ITL gate): this tuple's own change is already in the snapshot. */
	if (!SCN_VALID(slot->write_scn))
		return CLUSTER_CR_NOT_APPLICABLE;
	if (scn_time_cmp(slot->write_scn, snapshot->read_scn) <= 0)
		return CLUSTER_CR_NOT_APPLICABLE;

	/* Tier 3 (spec-4.5a D8): own-instance, OR a remote instance whose undo
	 * this node MATERIALIZED during merged recovery (persistent
	 * merge_recovered_lsn authority).  A non-materialized remote origin
	 * keeps today's fall-through to the spec-3.2/3.3 remote-xid block;
	 * runtime warm cross-instance CR stays 4.6/4.7. */
	if (UBA_is_invalid(slot->undo_segment_head))
		return CLUSTER_CR_NOT_APPLICABLE;
	{
		int32 tuple_origin = (int32)uba_origin_node_id(slot->undo_segment_head);

		if (tuple_origin != cluster_node_id) {
			if (!cluster_merged_instance_is_materialized((int)tuple_origin))
				return CLUSTER_CR_NOT_APPLICABLE;
			remote_materialized = true;
			remote_origin = tuple_origin;
			/* spec-5.14 D2 class 4: this CR verdict consumes the remote origin's
			 * volatile xact outcome (via cluster_remote_outcome_durable_checked
			 * below) — stamp so a fail-stop of that origin aborts this tx. */
			cluster_touched_peers_stamp(remote_origin, CLUSTER_TOUCH_VISIBILITY);
		}
	}

	/*
	 * spec-3.19 D3: live-tuple xmin guard (fail-closed toward invisible).
	 *
	 * The CR image occupant of this offset can differ from the LIVE occupant
	 * after HOT line-pointer reuse + 8-slot ITL recycling, so the historical-
	 * image decision below must NOT be applied to a live tuple that is not
	 * itself a committed, fully-finished version.  Without this guard the gate
	 * reports the historical version's visibility for a live version whose own
	 * xmin HeapTupleSatisfiesUpdate then rejects -> TM_Invisible ("attempted to
	 * update invisible tuple", spec-3.19 D0/D1).
	 *
	 * The verdict must match what HeapTupleSatisfiesUpdate uses, NOT a CLOG-only
	 * test: the disagreement window is the COMMIT-IN-PROGRESS gap.  A committing
	 * xact sets its CLOG commit bit (TransactionIdDidCommit -> true) BEFORE it
	 * leaves the ProcArray (ProcArrayEndTransaction).  During that gap the native
	 * SatisfiesUpdate path still sees the writer via TransactionIdIsInProgress and
	 * returns TM_Invisible (D0 captured xmin{commit=1 inprog=1}).  So the live
	 * version is a valid visible update target only when its xmin is BOTH
	 * committed AND no longer in progress; otherwise (still in progress, in the
	 * commit gap, or aborted) it is visible to no snapshot -> invisible.
	 *
	 * Own-instance only (tier-3 above): the live xmin's CLOG + ProcArray state is
	 * local-authoritative.  Our own write is excluded (self-modification is
	 * handled by the native path).  The construct-time prune already drops
	 * post-read_scn *committed* versions; this guard closes the in-progress /
	 * commit-gap / aborted versions the prune cannot key off commit_scn.  The
	 * snapshot's correct older version is found by the normal chain walk.
	 */
	{
		TransactionId live_xmin = HeapTupleHeaderGetRawXmin(tup);

		/*
		 * spec-4.5a D8/G5: for a materialized-remote tuple the live xmin is
		 * the PEER's xid; the local ProcArray/CLOG would alias across
		 * instances (AD-012 例外 9), so resolve the creator's outcome from
		 * the per-origin authority (D10c) instead.  ABORTED creator ->
		 * invisible; COMMITTED -> the guard does not fire (a committed,
		 * finished creator is a valid version, fall through to construct);
		 * INDOUBT (B stamped its TT then crashed) -> fail closed (规则 8.A).
		 */
		if (remote_materialized) {
			SCN rscn;

			/*
			 * spec-4.5a G6 (P1 #2): wrap-check the creator's outcome against
			 * the origin's durable TT slots (by-xid scan, wrap-qualified) -- a
			 * bare (origin,xid) verdict could alias a same-xid wraparound
			 * overwrite of the outcome store.  Unprovable -> fail closed.
			 */
			switch (cluster_remote_outcome_durable_checked(remote_origin, live_xmin, &rscn)) {
			case CLUSTER_REMOTE_XACT_COMMITTED:
				break; /* finished creator -> construct CR image below */
			case CLUSTER_REMOTE_XACT_ABORTED:
				*out_visible = false;
				return CLUSTER_CR_DECIDED;
			case CLUSTER_REMOTE_XACT_INDOUBT:
			default:
				return CLUSTER_CR_FAILCLOSED;
			}
		} else if (TransactionIdIsNormal(live_xmin)
				   && !TransactionIdIsCurrentTransactionId(live_xmin)
				   && (TransactionIdIsInProgress(live_xmin)
					   || !TransactionIdDidCommit(live_xmin))) {
			*out_visible = false;
			return CLUSTER_CR_DECIDED;
		}
	}

	/*
	 * Gate fired: construct the read_scn block image (full-block CR, spec-3.10)
	 * and decide on the historical tuple.  cluster_cr_lookup_or_construct never
	 * returns NULL — it ereports the precise SQLSTATE on failure (I-fail-1).
	 * itl_idx is no longer passed (full-block rolls back every candidate chain);
	 * the queried tuple's live `slot` (already resolved above) still drives the
	 * tier-2 + xmin-side checks.
	 */
	/*
	 * spec-5.54 D3: tuple-level / verdict-only fast path (GUC, default off).  The
	 * 3-tier gate + live-xmin guard above have already fired; before materializing
	 * the whole read_scn block, try reconstructing ONLY the queried offnum from the
	 * single candidate chain and run the IDENTICAL verdict.  A DECIDED result is
	 * bit-equivalent to full-block (INV-T1); eligibility-false / NOT_APPLICABLE
	 * falls through to the authoritative full-block path below (fail-safe, INV-T2).
	 * Own-instance only: materialized-remote is excluded by eligibility, so the
	 * fast path is skipped for it and the full-block path keeps its 4.5a handling.
	 */
	if (cluster_cr_tuple_level_fastpath && !remote_materialized) {
		ClusterCRCandidateChain fp_chains[CLUSTER_ITL_INITRANS_DEFAULT];
		int fp_nchains;
		ClusterCRTupleOutcome fp_reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;

		fp_nchains
			= cluster_cr_collect_candidate_chains(ClusterPageGetItlSlots(page), snapshot->read_scn,
												  fp_chains, CLUSTER_ITL_INITRANS_DEFAULT);
		if (cluster_cr_tuple_eligible(buffer, htup, snapshot, slot, fp_nchains, &fp_reason)) {
			ClusterCrVerdict fp_v
				= cluster_cr_tuple_verdict(buffer, htup, snapshot, out_visible, &fp_reason);

			cluster_cr_tuple_stat_bump(fp_reason);
			if (fp_v != CLUSTER_CR_NOT_APPLICABLE)
				return fp_v; /* DECIDED -> bit-equivalent to full-block (INV-T1) */
		} else {
			cluster_cr_tuple_stat_bump(fp_reason);
		}
		/* not eligible / NOT_APPLICABLE -> fall through to authoritative full-block */
	}

	cr_page = cluster_cr_lookup_or_construct(buffer, snapshot->read_scn);
	return cluster_cr_verdict_on_image(cr_page, ItemPointerGetOffsetNumber(&htup->t_self), slot,
									   snapshot, remote_materialized, remote_origin, out_visible);
}

static ClusterCrVerdict
cluster_cr_verdict_on_image(const char *cr_page, OffsetNumber offnum,
							const ClusterItlSlotData *slot, Snapshot snapshot,
							bool remote_materialized, int32 remote_origin, bool *out_visible)
{
	HeapTupleData cr_htup;
	ClusterVisibilityDecision decision;

	if (!cluster_cr_remap_tuple(cr_page, offnum, &cr_htup)) {
		/* CR-removed (inverse-INSERT made it LP_UNUSED): the row did not
		 * exist at read_scn -> invisible. */
		*out_visible = false;
		return CLUSTER_CR_DECIDED;
	}

	/*
	 * spec-3.9 Hardening (L214): xmin-side (creation) visibility.
	 *
	 * spec-3.10 NOTE: this per-tuple check is now SUPERSEDED by the construct-
	 * time cluster_cr_prune_post_snapshot_versions() (full-block CR), which
	 * marks LP_UNUSED every post-read_scn-created tuple BEFORE remap — so any
	 * tuple reaching here has a pre-read_scn creator and cr_xmin != slot->xid,
	 * i.e. this branch never fires (verified across t/215 + t/216).  Kept as a
	 * zero-cost defense-in-depth backstop; removable in a future cleanup
	 * (spec-3.10 §v0.4 D-A).  L214 alone could NOT handle same-row-multi-update
	 * (a doubly-updated tuple's live slot is its latest modifier, not creator).
	 *
	 * cluster_visibility_decide_cr_tuple is xmax-only and assumes every tuple
	 * still present in the CR image existed at read_scn.  That holds for a
	 * fresh INSERT (inverse-INSERTed to LP_UNUSED -> remap false above) and a
	 * DELETE (inverse-DELETE clears xmax), but NOT for the NEW physical
	 * version produced by an UPDATE: the walker inverse-applies the update
	 * onto the OLD tuple's slot, leaving the new version present in the image
	 * with xmin = the updating xact.  If that xact committed after read_scn
	 * the new version did not exist at the snapshot and must be invisible, or
	 * the snapshot would see BOTH the old and the new value (caught by t/215
	 * L10).  This also closes the H6 "inserted-before-read_scn-but-committed-
	 * after" case.
	 *
	 * The tuple's own ITL slot is the writer that produced this version, and
	 * the tier-2 gate above already established slot.write_scn is newer than read_scn.  If
	 * that post-snapshot writer IS the image tuple's creator (slot.xid ==
	 * xmin) the row was CREATED after the snapshot -> invisible.  We key off
	 * write_scn, NOT commit_scn: under delayed cleanout a just-committed slot
	 * is still ACTIVE with commit_scn = InvalidScn, but write_scn is always
	 * stamped at write time, and commit_scn is not earlier than write_scn, so a
	 * write_scn newer than read_scn already implies the creator committed after
	 * the snapshot.  For
	 * a delete-marked old tuple slot.xid == xmax (the deleter), not xmin, so a
	 * genuine inverse-DELETE / inverse-UPDATE restore is left untouched.
	 */
	{
		TransactionId cr_xmin = HeapTupleHeaderGetRawXmin(cr_htup.t_data);

		if (TransactionIdIsValid(cr_xmin) && cr_xmin == slot->xid) {
			*out_visible = false;
			return CLUSTER_CR_DECIDED;
		}
	}

	decision = cluster_visibility_decide_cr_tuple(&cr_htup, snapshot);
	if (decision == CLUSTER_VISIBILITY_VISIBLE) {
		/* xmax invalid -> the row was never deleted -> visible. */
		*out_visible = true;
		return CLUSTER_CR_DECIDED;
	}

	/*
	 * spec-3.21 D1: the CR image carries a VALID xmax.  The pre-3.21 code
	 * returned invisible here ("any valid xmax -> deleted at read_scn"), but a
	 * valid xmax only proves SOME xact wrote a delete/lock mark -- not that the
	 * delete COMMITTED at/before read_scn.  When the deleter is lock-only,
	 * in-progress, aborted, or committed AFTER read_scn, the row was LIVE at the
	 * snapshot and must be VISIBLE.  Mis-hiding it produced silent hot-row
	 * UPDATE 0 / lost updates (D0.6: 538 in-progress false-invisibles).  Resolve
	 * the deleter's commit-state vs read_scn instead (own-instance, tier-3).
	 */
	{
		HeapTupleHeader cr_tup = cr_htup.t_data;
		uint16 cr_infomask = cr_tup->t_infomask;
		TransactionId cr_xmax;
		ClusterTTStatus xmax_status;
		ClusterVisibilityDecision scn_decision = CLUSTER_VISIBILITY_UNKNOWN;

		/*
		 * A lock-only xmax never deletes the tuple (spec-3.21 P1-c).
		 * HEAP_XMAX_IS_LOCKED_ONLY also catches HEAP_LOCKED_UPGRADED (the legacy
		 * multi-locker pattern has HEAP_XMAX_LOCK_ONLY set), so it is handled
		 * here -- and HeapTupleGetUpdateXid below (reached only on the MULTI
		 * branch) is thus never called on a lock-only multi (its internal
		 * Assert(!LOCK_ONLY) holds).
		 */
		if (HEAP_XMAX_IS_LOCKED_ONLY(cr_infomask)) {
			*out_visible = true;
			return CLUSTER_CR_DECIDED;
		}

		/* MultiXact: decode the UPDATE member; a lockers-only multi has no
		 * update xid -> visible.  Never treat the multi value as a plain xid. */
		if (cr_infomask & HEAP_XMAX_IS_MULTI)
			cr_xmax = HeapTupleGetUpdateXid(cr_tup);
		else
			cr_xmax = HeapTupleHeaderGetRawXmax(cr_tup);

		if (!TransactionIdIsValid(cr_xmax) || TransactionIdIsCurrentTransactionId(cr_xmax)) {
			/* lockers-only multi, or our own delete (native handles self). */
			*out_visible = true;
			return CLUSTER_CR_DECIDED;
		}

		/*
		 * spec-4.5a D8/G5: a materialized-remote deleter's commit-state comes
		 * from the per-origin authority (D10c), NOT the local raw-xid
		 * ProcArray/CLOG/durable-TT (AD-012 例外 9).  COMMITTED -> visible at
		 * read_scn IFF the commit SCN is after read_scn (delete committed
		 * after the snapshot -> row was live -> VISIBLE); ABORTED deleter ->
		 * the row was never deleted -> VISIBLE; INDOUBT -> fail closed.
		 */
		if (remote_materialized) {
			/*
			 * The per-origin authority gives the deleter's outcome; for a
			 * COMMITTED deleter the SCN comparison feeds the SAME verdict
			 * table the own-instance path uses (decide_by_scn(commit_scn,
			 * read_scn) VISIBLE => the delete is visible at read_scn =>
			 * tuple INVISIBLE), so the direction is the audited one.
			 *
			 * spec-4.5a G6 (P1 #2): wrap-check the deleter against the
			 * origin's durable TT slots (by-xid scan, wrap-qualified), so a
			 * same-xid wraparound overwrite of the outcome store cannot alias
			 * the deleter; unprovable -> fail closed (规则 8.A).
			 */
			SCN rscn;

			switch (cluster_remote_outcome_durable_checked(remote_origin, cr_xmax, &rscn)) {
			case CLUSTER_REMOTE_XACT_COMMITTED:
				xmax_status = CLUSTER_TT_STATUS_COMMITTED;
				scn_decision = cluster_visibility_decide_by_scn(rscn, snapshot->read_scn);
				break;
			case CLUSTER_REMOTE_XACT_ABORTED:
				xmax_status = CLUSTER_TT_STATUS_ABORTED;
				break;
			case CLUSTER_REMOTE_XACT_INDOUBT:
			default:
				return CLUSTER_CR_FAILCLOSED;
			}
		}
		/* Own-instance authoritative classification of the deleting xact. */
		else if (TransactionIdIsInProgress(cr_xmax))
			xmax_status = CLUSTER_TT_STATUS_IN_PROGRESS;
		else if (!TransactionIdDidCommit(cr_xmax))
			xmax_status = CLUSTER_TT_STATUS_ABORTED;
		else {
			/* Committed deleter: invisible IFF the delete is visible at read_scn. */
			SCN xmax_cscn;

			/*
			 * Live-slot shortcut: if the LIVE tuple's ITL slot still holds cr_xmax
			 * (the deleter), tier-2 above already proved the slot wrote after
			 * read_scn, so its commit (at or after the write) is also after
			 * read_scn -- the delete committed AFTER the snapshot -> the row was
			 * live at read_scn -> VISIBLE.  (Sound direction of write_scn; P1-a
			 * forbids only the inverse.)  This is the common RR-snapshot +
			 * concurrent-commit case (t/229 L6) whose deleter slot has not been
			 * recycled; its commit_scn need not be stamped yet.
			 */
			if (slot->xid == cr_xmax) {
				*out_visible = true;
				return CLUSTER_CR_DECIDED;
			}

			/*
			 * The live slot was recycled to a newer writer (slot->xid != cr_xmax):
			 * resolve cr_xmax's commit-state by exact xid (P1-a: no proxy).  A
			 * durable 0-match (RECYCLED) is provably below the retention horizon --
			 * hence before this snapshot, hence the delete is visible -> the CR
			 * tuple is INVISIBLE -- but only when the retention proof holds; all
			 * other resolves either compare an exact commit_scn or fail closed
			 * (53R9F).  spec-3.22 §2.2 / 规则 8.A: a 0-match without proof is missing
			 * information, never an Option-A guess.
			 */
			switch (cluster_cr_resolve_xmax_commit_scn(cr_page, cr_tup->t_itl_slot_idx, cr_xmax,
													   &xmax_cscn)) {
			case CLUSTER_CR_XMAX_RESOLVED_SCN:
				cluster_cr_count_xmax_resolved();
				xmax_status = CLUSTER_TT_STATUS_COMMITTED;
				scn_decision = cluster_visibility_decide_by_scn(xmax_cscn, snapshot->read_scn);
				break; /* exits this resolve switch; the verdict switch below decides */

			case CLUSTER_CR_XMAX_RECYCLED:
				if (cluster_cr_retention_proof_valid(snapshot->read_scn)) {
					cluster_cr_count_xmax_recycled_invisible();
					/* spec-4.8 D4 (task#84): surface the recycled-slot liveness
					 * relax under the tt_recovery category too.  The §3.5 proof
					 * chain's own-instance legs (a-e) ARE this 3.22 retention
					 * proof + D3's generation gate (condition 6); a 53R9F that
					 * this proof turns into a sound INVISIBLE is the relax.  Cross-node
					 * INDOUBT has no relax arm: recovered_through >= page LSN proves
					 * the deleter's WRITE is covered, not its (later) COMMIT LSN, so an
					 * INDOUBT could be an as-yet-unrecovered commit -> relaxing it would
					 * be false-visible (规则 8.A).  Q5 strict fallback applies; forward. */
					cluster_tt_recovery_count_recycled_liveness_relaxed();
					*out_visible = false; /* deleter proven below horizon -> invisible */
					return CLUSTER_CR_DECIDED;
				}
				cluster_cr_count_xmax_scan_unavail_or_no_proof();
				ereport(
					ERROR,
					(errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
					 errmsg("cluster CR cannot resolve commit_scn for recycled deleting xmax %u",
							cr_xmax),
					 errhint("the deleter's TT slot was recycled but the retention proof is "
							 "unavailable (retention off, invalid horizon, or a read_scn older "
							 "than the horizon); retry with a fresh snapshot.")));

			case CLUSTER_CR_XMAX_INVALID_OR_AMBIGUOUS:
				cluster_cr_count_xmax_invalid_or_ambiguous();
				ereport(ERROR, (errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
								errmsg("cluster CR cannot resolve commit_scn for deleting xmax %u",
									   cr_xmax),
								errhint("delayed cleanout (commit_scn not yet stamped) or xid-wrap "
										"residue; retry with a fresh snapshot.")));

			case CLUSTER_CR_XMAX_SCAN_UNAVAILABLE:
			default:
				cluster_cr_count_xmax_scan_unavail_or_no_proof();
				ereport(
					ERROR,
					(errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
					 errmsg("cluster CR durable scan unavailable for deleting xmax %u", cr_xmax),
					 errhint("degraded node or an unreadable undo segment prevented a complete "
							 "by-xid scan; retry with a fresh snapshot.")));
			}
		}

		switch (cluster_vis_cr_xmax_verdict(xmax_status, scn_decision)) {
		case CVV_VISIBLE:
			*out_visible = true;
			return CLUSTER_CR_DECIDED;
		case CVV_INVISIBLE:
			*out_visible = false;
			return CLUSTER_CR_DECIDED;
		case CVV_FAILCLOSED_UNKNOWN:
		default:
			/*
			 * cluster_vis_cr_xmax_verdict only ever returns VISIBLE / INVISIBLE /
			 * FAILCLOSED_UNKNOWN; the wait/conflict/gone verdicts (CVV_BEING_
			 * MODIFIED / CVV_GONE_* / CVV_FAILCLOSED_CONFLICT) belong to the
			 * SatisfiesUpdate/Dirty forks, not this snapshot-read gate.  Any of
			 * them reaching here would be a verdict-table bug -> fail closed.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_CR_SNAPSHOT_TOO_OLD),
					 errmsg("cluster CR xmax visibility unresolved for deleting xmax %u", cr_xmax),
					 errhint("deleter commit_scn could not be proven against read_scn; retry.")));
		}
	}

	pg_unreachable(); /* every verdict branch above returns or ereports */
}

/*
 * cluster_cr_tuple_eligible -- spec-5.54 D1 Buffer wrapper over the pure static
 * predicate.  Resolves the queried offnum + the tuple's undo origin from the live
 * buffer + slot, then defers to cluster_cr_tuple_eligible_page.  Precondition
 * (gate-enforced): the 3-tier gate already proved slot->undo_segment_head valid.
 */
bool
cluster_cr_tuple_eligible(Buffer buf, HeapTuple htup, Snapshot snapshot,
						  const ClusterItlSlotData *slot, int nchains,
						  ClusterCRTupleOutcome *reason)
{
	Page page = BufferGetPage(buf);
	OffsetNumber off = ItemPointerGetOffsetNumber(&htup->t_self);
	int32 tuple_origin = (int32)uba_origin_node_id(slot->undo_segment_head);

	return cluster_cr_tuple_eligible_page(page, off, snapshot->read_scn, nchains, tuple_origin,
										  cluster_node_id, reason);
}

/*
 * cluster_cr_tuple_verdict -- spec-5.54 D2/D4 single-chain target-offnum verdict.
 *
 * Reconstructs ONLY the queried offnum on the separate backend-local fast scratch
 * by walking the single candidate chain (TargetTupleCrState contract), then runs
 * the IDENTICAL cluster_cr_verdict_on_image the full-block gate runs -> a DECIDED
 * verdict is bit-equivalent to full-block (INV-T1).  Any walk uncertainty
 * (foreign identity / no space / malformed / missing undo / cliff / cross-instance)
 * returns NOT_APPLICABLE so the caller defers to the authoritative full-block path
 * (INV-T2/T3); the fast path never guesses and never fail-closes from its narrower
 * view (full-block re-walks the same single chain and produces the precise
 * SQLSTATE for a genuine terminal).
 *
 * I-lock invariants are inherited from the full-block path: the caller holds the
 * content lock, we only read live-page bytes + the undo chain into the scratch,
 * no buffer lock / no WAL / no dirty (spec-3.9 I-lock-1/2/4).
 */
ClusterCrVerdict
cluster_cr_tuple_verdict(Buffer buf, HeapTuple htup, Snapshot snapshot, bool *out_visible,
						 ClusterCRTupleOutcome *out_reason)
{
	Page live_page = BufferGetPage(buf);
	OffsetNumber offnum = ItemPointerGetOffsetNumber(&htup->t_self);
	ClusterCRCandidateChain chains[CLUSTER_ITL_INITRANS_DEFAULT];
	int nchains;
	TransactionId candidate_xid;
	const ClusterItlSlotData *live_slot;
	uint8 itl_idx;
	UBA uba;
	uint32 steps = 0;
	uint32 max_steps = (uint32)cluster_cr_chain_walk_max_steps;
	PGAlignedBlock record_buf;
	RelFileLocator cur_locator;
	ForkNumber cur_fork;
	BlockNumber cur_block;
	ClusterCRTupleOutcome reason = CR_TUPLE_OUTCOME_VERDICT;

	/*
	 * Re-collect the single candidate chain (cheap O(ITL)); eligibility already
	 * proved nchains==1 + own-instance + watermark, but re-derive the chain head +
	 * candidate xid here and fail-safe to full-block if anything changed.
	 */
	nchains
		= cluster_cr_collect_candidate_chains(ClusterPageGetItlSlots(live_page), snapshot->read_scn,
											  chains, CLUSTER_ITL_INITRANS_DEFAULT);
	if (nchains != 1) {
		if (out_reason != NULL)
			*out_reason = CR_TUPLE_OUTCOME_FALLBACK_MULTICHAIN;
		return CLUSTER_CR_NOT_APPLICABLE;
	}
	candidate_xid = chains[0].xid;

	/* The queried tuple's LIVE ITL slot drives cluster_cr_verdict_on_image's
	 * tier-2 / live-slot shortcuts (the same slot the gate resolved). */
	itl_idx = htup->t_data->t_itl_slot_idx;
	if (itl_idx == CLUSTER_ITL_SLOT_UNALLOCATED || itl_idx >= CLUSTER_ITL_INITRANS_DEFAULT) {
		if (out_reason != NULL)
			*out_reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
		return CLUSTER_CR_NOT_APPLICABLE;
	}
	live_slot = &ClusterPageGetItlSlots(live_page)[itl_idx];

	/* Materialize the target on the SEPARATE fast scratch (never cr_scratch). */
	if (cr_tuple_scratch == NULL)
		cr_tuple_scratch = MemoryContextAllocZero(TopMemoryContext, BLCKSZ);
	memcpy(cr_tuple_scratch, live_page, BLCKSZ);

	BufferGetTag(buf, &cur_locator, &cur_fork, &cur_block);

	/* prune-before (target only): mirror full-block prune-first restricted to the
	 * queried offnum (F0-25). */
	cluster_cr_tuple_prune_target(cr_tuple_scratch, offnum, candidate_xid);

	uba = chains[0].undo_segment_head;
	while (!UBA_is_invalid(uba)) {
		const UndoRecordHeader *hdr;
		size_t len;
		uint32 seg;
		uint32 blk;
		uint16 tt_off;
		uint16 row_off;

		if (++steps > max_steps) {
			reason = CR_TUPLE_OUTCOME_FALLBACK_CLIFF; /* full-block re-walk -> data_corrupted */
			goto fallback;
		}
		if (!uba_decode(uba, &seg, &blk, &tt_off, &row_off)) {
			reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
			goto fallback;
		}
		len = cluster_undo_get_record(uba, record_buf.data, sizeof(record_buf.data));
		if (len < sizeof(UndoRecordHeader)) {
			reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN; /* missing/short undo -> full-block */
			goto fallback;
		}
		hdr = (const UndoRecordHeader *)record_buf.data;

		if (hdr->origin_node_id != (uint16)cluster_node_id
			&& !cluster_merged_instance_is_materialized((int)hdr->origin_node_id)) {
			reason = CR_TUPLE_OUTCOME_FALLBACK_CROSS_BLOCK; /* cross-instance UBA -> full-block */
			goto fallback;
		}

		/* I-chain-1: normal SCN stop -> read_scn reached (clean chain end). */
		if (scn_time_cmp(hdr->write_scn, snapshot->read_scn) <= 0)
			break;

		if (!RelFileNumberIsValid(hdr->target_locator.relNumber)) {
			reason = CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN;
			goto fallback;
		}

		/* spec-3.20 D3.A (F8) block-scope filter: skip-apply records for other
		 * relations/forks/blocks but KEEP walking prev_uba. */
		if (!RelFileLocatorEquals(hdr->target_locator, cur_locator) || hdr->target_fork != cur_fork
			|| hdr->target_block != cur_block) {
			uba = hdr->prev_uba;
			continue;
		}

		if (cluster_cr_tuple_apply_record(cr_tuple_scratch, offnum, record_buf.data, len, &reason)
			== CR_TUPLE_APPLY_FALLBACK)
			goto fallback;

		/* per-step target prune (mirror full-block per-step whole-page prune). */
		cluster_cr_tuple_prune_target(cr_tuple_scratch, offnum, candidate_xid);
		uba = hdr->prev_uba;
	}

	/* prune-after (target only). */
	cluster_cr_tuple_prune_target(cr_tuple_scratch, offnum, candidate_xid);

	/*
	 * Run the IDENTICAL verdict the full-block gate runs.  Own-instance only (the
	 * fast path is never eligible for a materialized-remote tuple), so pass
	 * remote_materialized=false; an own-instance unresolvable deleter ereports the
	 * same SQLSTATE the full-block path would (authoritative).
	 */
	if (out_reason != NULL)
		*out_reason = CR_TUPLE_OUTCOME_VERDICT;
	return cluster_cr_verdict_on_image(cr_tuple_scratch, offnum, live_slot, snapshot,
									   /*remote_materialized*/ false, /*remote_origin*/ -1,
									   out_visible);

fallback:
	if (out_reason != NULL)
		*out_reason = reason;
	return CLUSTER_CR_NOT_APPLICABLE;
}
