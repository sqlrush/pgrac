/*-------------------------------------------------------------------------
 *
 * cluster_tt_durable_stat.c
 *	  pgrac durable Transaction Table (TT) slot observability (spec-3.11 D7/D8).
 *
 *	  Shmem counter region + wait-event bracket hooks for the durable TT slot
 *	  path implemented in cluster_tt_durable.c.  Kept in a separate translation
 *	  unit so the pure decision/I/O logic in cluster_tt_durable.c links into the
 *	  cluster_unit harness (mocked smgr) without dragging in shmem / wait-event
 *	  backend symbols: cluster_tt_durable.c calls the extern hooks below, which
 *	  the unit test stubs as no-ops (same pure-unit + e2e-IO split as the rest
 *	  of spec-3.9 / spec-3.10 / spec-3.11).
 *
 *	  Five atomic counters (no LWLock), surfaced under category='undo' by
 *	  cluster_debug.c dump_undo (D8): durable_commit / lookup_hit / lookup_miss
 *	  / by_xid_scan / redo_apply.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.11-durable-tt-slot.md (§D7/D8)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tt_durable_stat.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "port/atomics.h"
#include "storage/shmem.h"
#include "utils/wait_event.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_terminal_authority.h"
#include "cluster/cluster_tt_durable.h"

/*
 * ClusterTTDurableShared -- per-instance shmem counters (spec-3.11 D7).
 *
 *	5 atomic counters, no LWLock (region lwlock_count = 0): bumped with
 *	pg_atomic_fetch_add_u64 from the committing / reading / redo backend, read
 *	lock-free by dump_undo / pg_cluster_state.
 */
typedef struct ClusterTTDurableShared {
	pg_atomic_uint64 durable_commit_count;		/* cluster_tt_slot_durable_commit */
	pg_atomic_uint64 durable_lookup_hit_count;	/* seg/slot lookup hit (D5) */
	pg_atomic_uint64 durable_lookup_miss_count; /* seg/slot lookup miss (D5) */
	pg_atomic_uint64 by_xid_scan_count;			/* by-xid watermark scan (D6) */
	pg_atomic_uint64 redo_apply_count;			/* XLOG_UNDO_TT_SLOT_COMMIT APPLY */

	/* spec-4.8 tt_recovery counters (8), dump category 'tt_recovery'. */
	pg_atomic_uint64 active_resolved_aborted;		  /* D1 crash-left ACTIVE -> ABORTED */
	pg_atomic_uint64 remote_active_failclosed;		  /* D2 cross-node authority fail-closed */
	pg_atomic_uint64 wrap_generation_disambiguated;	  /* D3 (xid,gen) disambiguation */
	pg_atomic_uint64 recycled_liveness_relaxed;		  /* D4 53R9F proof-gated relax */
	pg_atomic_uint64 scn_highwater_recovered;		  /* D5 SCN high-watermark recovery */
	pg_atomic_uint64 recovery_verdict_failclosed;	  /* D2/D7 verdict fail-closed */
	pg_atomic_uint64 heap_tuples_physically_reverted; /* D7 physical revert */
	pg_atomic_uint64 undo_revert_failclosed;		  /* D7 revert safety-gate fail-closed */

	/* spec-6.2 Smart Fusion terminal-authority counters. */
	pg_atomic_uint64 terminal_authority_check;
	pg_atomic_uint64 terminal_authority_ok;
	pg_atomic_uint64 terminal_authority_failclosed;
	pg_atomic_uint64 terminal_authority_epoch_failclosed;
	pg_atomic_uint64 terminal_authority_ownership_failclosed;
	pg_atomic_uint64 terminal_authority_unknown_failclosed;
	pg_atomic_uint64 terminal_authority_nonterminal_failclosed;
	pg_atomic_uint64 terminal_authority_durable_failclosed;
	pg_atomic_uint64 terminal_authority_retention_failclosed;

	/* Calls and normal results of the any-state locator, not SCN queries. */
	pg_atomic_uint64 locator_fallback_count;
	pg_atomic_uint64 locator_found_count;
	pg_atomic_uint64 locator_missing_count;
	pg_atomic_uint64 locator_ambiguous_count;
	pg_atomic_uint64 locator_unavailable_count;

	pg_atomic_uint64 locator_family_fallback_count[CLUSTER_TT_LOCATOR_FAMILY_COUNT];
	pg_atomic_uint64 locator_family_found_count[CLUSTER_TT_LOCATOR_FAMILY_COUNT];
	pg_atomic_uint64 locator_family_missing_count[CLUSTER_TT_LOCATOR_FAMILY_COUNT];
	pg_atomic_uint64 locator_family_ambiguous_count[CLUSTER_TT_LOCATOR_FAMILY_COUNT];
	pg_atomic_uint64 locator_family_unavailable_count[CLUSTER_TT_LOCATOR_FAMILY_COUNT];
} ClusterTTDurableShared;

#ifdef USE_PGRAC_CLUSTER

static ClusterTTDurableShared *TTDurableShared = NULL;

/* ------------------------------------------------------------ */
/* shmem region                                                 */
/* ------------------------------------------------------------ */

Size
cluster_tt_durable_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterTTDurableShared));
}

void
cluster_tt_durable_shmem_init(void)
{
	bool found;
	unsigned family;

	TTDurableShared
		= ShmemInitStruct("ClusterTTDurableShared", cluster_tt_durable_shmem_size(), &found);

	if (!found) {
		pg_atomic_init_u64(&TTDurableShared->durable_commit_count, 0);
		pg_atomic_init_u64(&TTDurableShared->durable_lookup_hit_count, 0);
		pg_atomic_init_u64(&TTDurableShared->durable_lookup_miss_count, 0);
		pg_atomic_init_u64(&TTDurableShared->by_xid_scan_count, 0);
		pg_atomic_init_u64(&TTDurableShared->redo_apply_count, 0);
		pg_atomic_init_u64(&TTDurableShared->active_resolved_aborted, 0);
		pg_atomic_init_u64(&TTDurableShared->remote_active_failclosed, 0);
		pg_atomic_init_u64(&TTDurableShared->wrap_generation_disambiguated, 0);
		pg_atomic_init_u64(&TTDurableShared->recycled_liveness_relaxed, 0);
		pg_atomic_init_u64(&TTDurableShared->scn_highwater_recovered, 0);
		pg_atomic_init_u64(&TTDurableShared->recovery_verdict_failclosed, 0);
		pg_atomic_init_u64(&TTDurableShared->heap_tuples_physically_reverted, 0);
		pg_atomic_init_u64(&TTDurableShared->undo_revert_failclosed, 0);
		pg_atomic_init_u64(&TTDurableShared->terminal_authority_check, 0);
		pg_atomic_init_u64(&TTDurableShared->terminal_authority_ok, 0);
		pg_atomic_init_u64(&TTDurableShared->terminal_authority_failclosed, 0);
		pg_atomic_init_u64(&TTDurableShared->terminal_authority_epoch_failclosed, 0);
		pg_atomic_init_u64(&TTDurableShared->terminal_authority_ownership_failclosed, 0);
		pg_atomic_init_u64(&TTDurableShared->terminal_authority_unknown_failclosed, 0);
		pg_atomic_init_u64(&TTDurableShared->terminal_authority_nonterminal_failclosed, 0);
		pg_atomic_init_u64(&TTDurableShared->terminal_authority_durable_failclosed, 0);
		pg_atomic_init_u64(&TTDurableShared->terminal_authority_retention_failclosed, 0);
		pg_atomic_init_u64(&TTDurableShared->locator_fallback_count, 0);
		pg_atomic_init_u64(&TTDurableShared->locator_found_count, 0);
		pg_atomic_init_u64(&TTDurableShared->locator_missing_count, 0);
		pg_atomic_init_u64(&TTDurableShared->locator_ambiguous_count, 0);
		pg_atomic_init_u64(&TTDurableShared->locator_unavailable_count, 0);
		for (family = 0; family < CLUSTER_TT_LOCATOR_FAMILY_COUNT; family++) {
			pg_atomic_init_u64(&TTDurableShared->locator_family_fallback_count[family], 0);
			pg_atomic_init_u64(&TTDurableShared->locator_family_found_count[family], 0);
			pg_atomic_init_u64(&TTDurableShared->locator_family_missing_count[family], 0);
			pg_atomic_init_u64(&TTDurableShared->locator_family_ambiguous_count[family], 0);
			pg_atomic_init_u64(&TTDurableShared->locator_family_unavailable_count[family], 0);
		}
	}
}

static const ClusterShmemRegion cluster_tt_durable_region = {
	.name = "pgrac cluster durable tt counters",
	.size_fn = cluster_tt_durable_shmem_size,
	.init_fn = cluster_tt_durable_shmem_init,
	.lwlock_count = 0, /* atomic counters only; no LWLock */
	.owner_subsys = "cluster_tt_durable",
	.reserved_flags = 0,
};

void
cluster_tt_durable_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_tt_durable_region);
}

/* ------------------------------------------------------------ */
/* counter bump hooks (called from cluster_tt_durable.c / xlog)  */
/* ------------------------------------------------------------ */

void
cluster_tt_durable_count_commit(void)
{
	if (TTDurableShared != NULL)
		pg_atomic_fetch_add_u64(&TTDurableShared->durable_commit_count, 1);
}

void
cluster_tt_durable_count_lookup(bool hit)
{
	if (TTDurableShared == NULL)
		return;
	if (hit)
		pg_atomic_fetch_add_u64(&TTDurableShared->durable_lookup_hit_count, 1);
	else
		pg_atomic_fetch_add_u64(&TTDurableShared->durable_lookup_miss_count, 1);
}

void
cluster_tt_durable_count_by_xid_scan(void)
{
	if (TTDurableShared != NULL)
		pg_atomic_fetch_add_u64(&TTDurableShared->by_xid_scan_count, 1);
}

void
cluster_tt_durable_count_redo_apply(void)
{
	if (TTDurableShared != NULL)
		pg_atomic_fetch_add_u64(&TTDurableShared->redo_apply_count, 1);
}

void
cluster_tt_durable_count_locator_call(void)
{
	if (TTDurableShared != NULL)
		pg_atomic_fetch_add_u64(&TTDurableShared->locator_fallback_count, 1);
}

void
cluster_tt_durable_count_locator_result(ClusterTTDurableLocate result)
{
	pg_atomic_uint64 *counter;

	if (TTDurableShared == NULL)
		return;
	switch (result) {
	case CLUSTER_TT_DURABLE_LOCATE_FOUND:
		counter = &TTDurableShared->locator_found_count;
		break;
	case CLUSTER_TT_DURABLE_LOCATE_MISSING:
		counter = &TTDurableShared->locator_missing_count;
		break;
	case CLUSTER_TT_DURABLE_LOCATE_AMBIGUOUS:
		counter = &TTDurableShared->locator_ambiguous_count;
		break;
	case CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE:
		counter = &TTDurableShared->locator_unavailable_count;
		break;
	default:
		return;
	}
	pg_atomic_fetch_add_u64(counter, 1);
}

bool
cluster_tt_durable_locator_stats_snapshot(ClusterTTDurableLocatorStats *out)
{
	ClusterTTDurableLocatorStats snapshot;
	unsigned family;

	if (TTDurableShared == NULL || out == NULL || cluster_node_id < 0)
		return false;
	MemSet(&snapshot, 0, sizeof(snapshot));
	snapshot.origin_node_id = cluster_node_id;
	snapshot.fallback_count = pg_atomic_read_u64(&TTDurableShared->locator_fallback_count);
	snapshot.found_count = pg_atomic_read_u64(&TTDurableShared->locator_found_count);
	snapshot.missing_count = pg_atomic_read_u64(&TTDurableShared->locator_missing_count);
	snapshot.ambiguous_count = pg_atomic_read_u64(&TTDurableShared->locator_ambiguous_count);
	snapshot.unavailable_count = pg_atomic_read_u64(&TTDurableShared->locator_unavailable_count);
	for (family = 0; family < CLUSTER_TT_LOCATOR_FAMILY_COUNT; family++) {
		snapshot.families[family].fallback_count
			= pg_atomic_read_u64(&TTDurableShared->locator_family_fallback_count[family]);
		snapshot.families[family].found_count
			= pg_atomic_read_u64(&TTDurableShared->locator_family_found_count[family]);
		snapshot.families[family].missing_count
			= pg_atomic_read_u64(&TTDurableShared->locator_family_missing_count[family]);
		snapshot.families[family].ambiguous_count
			= pg_atomic_read_u64(&TTDurableShared->locator_family_ambiguous_count[family]);
		snapshot.families[family].unavailable_count
			= pg_atomic_read_u64(&TTDurableShared->locator_family_unavailable_count[family]);
	}
	*out = snapshot;
	return true;
}

ClusterTTDurableLocate
cluster_tt_slot_durable_locate_any_by_xid_origin_profiled(ClusterTTDurableLocatorFamily family,
														  int origin_node, TransactionId xid,
														  uint16 *out_seg, uint16 *out_slot,
														  uint16 *out_wrap, uint8 *out_status)
{
	bool profile = TTDurableShared != NULL && (unsigned)family < CLUSTER_TT_LOCATOR_FAMILY_COUNT;
	ClusterTTDurableLocate result;
	pg_atomic_uint64 *counter = NULL;

	if (profile)
		pg_atomic_fetch_add_u64(&TTDurableShared->locator_family_fallback_count[family], 1);
	result = cluster_tt_slot_durable_locate_any_by_xid_origin(origin_node, xid, out_seg, out_slot,
															  out_wrap, out_status);
	if (profile) {
		switch (result) {
		case CLUSTER_TT_DURABLE_LOCATE_FOUND:
			counter = &TTDurableShared->locator_family_found_count[family];
			break;
		case CLUSTER_TT_DURABLE_LOCATE_MISSING:
			counter = &TTDurableShared->locator_family_missing_count[family];
			break;
		case CLUSTER_TT_DURABLE_LOCATE_AMBIGUOUS:
			counter = &TTDurableShared->locator_family_ambiguous_count[family];
			break;
		case CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE:
			counter = &TTDurableShared->locator_family_unavailable_count[family];
			break;
		default:
			break;
		}
		if (counter != NULL)
			pg_atomic_fetch_add_u64(counter, 1);
	}
	return result;
}

/* ------------------------------------------------------------ */
/* wait-event bracket hooks (durable TT header I/O)             */
/* ------------------------------------------------------------ */

void
cluster_tt_durable_io_wait_start(void)
{
	pgstat_report_wait_start(WAIT_EVENT_UNDO_TT_DURABLE_IO);
}

void
cluster_tt_durable_io_wait_end(void)
{
	pgstat_report_wait_end();
}

/* ------------------------------------------------------------ */
/* counter accessors (lock-free; dump_undo / pg_cluster_state)  */
/* ------------------------------------------------------------ */

#define TT_DURABLE_ACCESSOR(fn, field)                                                             \
	uint64 fn(void)                                                                                \
	{                                                                                              \
		if (TTDurableShared == NULL)                                                               \
			return 0;                                                                              \
		return pg_atomic_read_u64(&TTDurableShared->field);                                        \
	}

TT_DURABLE_ACCESSOR(cluster_tt_durable_commit_count, durable_commit_count)
TT_DURABLE_ACCESSOR(cluster_tt_durable_lookup_hit_count, durable_lookup_hit_count)
TT_DURABLE_ACCESSOR(cluster_tt_durable_lookup_miss_count, durable_lookup_miss_count)
TT_DURABLE_ACCESSOR(cluster_tt_durable_by_xid_scan_count, by_xid_scan_count)
TT_DURABLE_ACCESSOR(cluster_tt_durable_redo_apply_count, redo_apply_count)

/* spec-4.8 tt_recovery counters: bump hooks + accessors. */
#define TT_RECOVERY_BUMP(fn, field)                                                                \
	void fn(void)                                                                                  \
	{                                                                                              \
		if (TTDurableShared != NULL)                                                               \
			pg_atomic_fetch_add_u64(&TTDurableShared->field, 1);                                   \
	}

TT_RECOVERY_BUMP(cluster_tt_recovery_count_active_resolved_aborted, active_resolved_aborted)
TT_RECOVERY_BUMP(cluster_tt_recovery_count_remote_active_failclosed, remote_active_failclosed)
TT_RECOVERY_BUMP(cluster_tt_recovery_count_wrap_generation_disambiguated,
				 wrap_generation_disambiguated)
TT_RECOVERY_BUMP(cluster_tt_recovery_count_recycled_liveness_relaxed, recycled_liveness_relaxed)
TT_RECOVERY_BUMP(cluster_tt_recovery_count_scn_highwater_recovered, scn_highwater_recovered)
TT_RECOVERY_BUMP(cluster_tt_recovery_count_recovery_verdict_failclosed, recovery_verdict_failclosed)
TT_RECOVERY_BUMP(cluster_tt_recovery_count_heap_tuples_physically_reverted,
				 heap_tuples_physically_reverted)
TT_RECOVERY_BUMP(cluster_tt_recovery_count_undo_revert_failclosed, undo_revert_failclosed)

TT_DURABLE_ACCESSOR(cluster_tt_recovery_active_resolved_aborted_count, active_resolved_aborted)
TT_DURABLE_ACCESSOR(cluster_tt_recovery_remote_active_failclosed_count, remote_active_failclosed)
TT_DURABLE_ACCESSOR(cluster_tt_recovery_wrap_generation_disambiguated_count,
					wrap_generation_disambiguated)
TT_DURABLE_ACCESSOR(cluster_tt_recovery_recycled_liveness_relaxed_count, recycled_liveness_relaxed)
TT_DURABLE_ACCESSOR(cluster_tt_recovery_scn_highwater_recovered_count, scn_highwater_recovered)
TT_DURABLE_ACCESSOR(cluster_tt_recovery_recovery_verdict_failclosed_count,
					recovery_verdict_failclosed)
TT_DURABLE_ACCESSOR(cluster_tt_recovery_heap_tuples_physically_reverted_count,
					heap_tuples_physically_reverted)
TT_DURABLE_ACCESSOR(cluster_tt_recovery_undo_revert_failclosed_count, undo_revert_failclosed)

void
cluster_terminal_authority_count_decision(ClusterTerminalAuthorityReason reason)
{
	if (TTDurableShared == NULL)
		return;

	pg_atomic_fetch_add_u64(&TTDurableShared->terminal_authority_check, 1);
	if (reason == CLUSTER_TERMINAL_AUTH_OK) {
		pg_atomic_fetch_add_u64(&TTDurableShared->terminal_authority_ok, 1);
		return;
	}

	pg_atomic_fetch_add_u64(&TTDurableShared->terminal_authority_failclosed, 1);
	switch (reason) {
	case CLUSTER_TERMINAL_AUTH_EPOCH_UNKNOWN:
	case CLUSTER_TERMINAL_AUTH_EPOCH_CHANGED:
		pg_atomic_fetch_add_u64(&TTDurableShared->terminal_authority_epoch_failclosed, 1);
		break;
	case CLUSTER_TERMINAL_AUTH_OWNER_UNKNOWN:
	case CLUSTER_TERMINAL_AUTH_OWNER_MISMATCH:
	case CLUSTER_TERMINAL_AUTH_INVALID_ORIGIN:
	case CLUSTER_TERMINAL_AUTH_INVALID_XID:
		pg_atomic_fetch_add_u64(&TTDurableShared->terminal_authority_ownership_failclosed, 1);
		break;
	case CLUSTER_TERMINAL_AUTH_TERMINAL_UNKNOWN:
	case CLUSTER_TERMINAL_AUTH_DISABLED:
	case CLUSTER_TERMINAL_AUTH_COMMIT_SCN_MISSING:
		pg_atomic_fetch_add_u64(&TTDurableShared->terminal_authority_unknown_failclosed, 1);
		break;
	case CLUSTER_TERMINAL_AUTH_NONTERMINAL:
		pg_atomic_fetch_add_u64(&TTDurableShared->terminal_authority_nonterminal_failclosed, 1);
		break;
	case CLUSTER_TERMINAL_AUTH_DURABLE_MISSING:
	case CLUSTER_TERMINAL_AUTH_DURABLE_MISMATCH:
		pg_atomic_fetch_add_u64(&TTDurableShared->terminal_authority_durable_failclosed, 1);
		break;
	case CLUSTER_TERMINAL_AUTH_RETENTION_UNPROVEN:
		pg_atomic_fetch_add_u64(&TTDurableShared->terminal_authority_retention_failclosed, 1);
		break;
	case CLUSTER_TERMINAL_AUTH_OK:
	case CLUSTER_TERMINAL_AUTH_REASON__COUNT:
		break;
	}
}

TT_DURABLE_ACCESSOR(cluster_terminal_authority_check_count, terminal_authority_check)
TT_DURABLE_ACCESSOR(cluster_terminal_authority_ok_count, terminal_authority_ok)
TT_DURABLE_ACCESSOR(cluster_terminal_authority_failclosed_count, terminal_authority_failclosed)
TT_DURABLE_ACCESSOR(cluster_terminal_authority_epoch_failclosed_count,
					terminal_authority_epoch_failclosed)
TT_DURABLE_ACCESSOR(cluster_terminal_authority_ownership_failclosed_count,
					terminal_authority_ownership_failclosed)
TT_DURABLE_ACCESSOR(cluster_terminal_authority_unknown_failclosed_count,
					terminal_authority_unknown_failclosed)
TT_DURABLE_ACCESSOR(cluster_terminal_authority_nonterminal_failclosed_count,
					terminal_authority_nonterminal_failclosed)
TT_DURABLE_ACCESSOR(cluster_terminal_authority_durable_failclosed_count,
					terminal_authority_durable_failclosed)
TT_DURABLE_ACCESSOR(cluster_terminal_authority_retention_failclosed_count,
					terminal_authority_retention_failclosed)

#else /* !USE_PGRAC_CLUSTER */

void
cluster_tt_durable_count_locator_call(void)
{}

void
cluster_tt_durable_count_locator_result(ClusterTTDurableLocate result)
{
	(void)result;
}

bool
cluster_tt_durable_locator_stats_snapshot(ClusterTTDurableLocatorStats *out)
{
	(void)out;
	return false;
}

ClusterTTDurableLocate
cluster_tt_slot_durable_locate_any_by_xid_origin_profiled(ClusterTTDurableLocatorFamily family,
														  int origin_node, TransactionId xid,
														  uint16 *out_seg, uint16 *out_slot,
														  uint16 *out_wrap, uint8 *out_status)
{
	(void)family;
	return cluster_tt_slot_durable_locate_any_by_xid_origin(origin_node, xid, out_seg, out_slot,
															out_wrap, out_status);
}

Size
cluster_tt_durable_shmem_size(void)
{
	return 0;
}

void
cluster_tt_durable_shmem_init(void)
{}

void
cluster_tt_durable_shmem_register(void)
{}

void
cluster_tt_durable_count_commit(void)
{}

void
cluster_tt_durable_count_lookup(bool hit)
{
	(void)hit;
}

void
cluster_tt_durable_count_by_xid_scan(void)
{}

void
cluster_tt_durable_count_redo_apply(void)
{}

void
cluster_tt_durable_io_wait_start(void)
{}

void
cluster_tt_durable_io_wait_end(void)
{}

uint64
cluster_tt_durable_commit_count(void)
{
	return 0;
}

uint64
cluster_tt_durable_lookup_hit_count(void)
{
	return 0;
}

uint64
cluster_tt_durable_lookup_miss_count(void)
{
	return 0;
}

uint64
cluster_tt_durable_by_xid_scan_count(void)
{
	return 0;
}

uint64
cluster_tt_durable_redo_apply_count(void)
{
	return 0;
}

/* spec-4.8 tt_recovery counters: no-op bumps + zero accessors. */
#define TT_RECOVERY_BUMP_NOOP(fn)                                                                  \
	void fn(void)                                                                                  \
	{}
#define TT_RECOVERY_ZERO(fn)                                                                       \
	uint64 fn(void)                                                                                \
	{                                                                                              \
		return 0;                                                                                  \
	}

TT_RECOVERY_BUMP_NOOP(cluster_tt_recovery_count_active_resolved_aborted)
TT_RECOVERY_BUMP_NOOP(cluster_tt_recovery_count_remote_active_failclosed)
TT_RECOVERY_BUMP_NOOP(cluster_tt_recovery_count_wrap_generation_disambiguated)
TT_RECOVERY_BUMP_NOOP(cluster_tt_recovery_count_recycled_liveness_relaxed)
TT_RECOVERY_BUMP_NOOP(cluster_tt_recovery_count_scn_highwater_recovered)
TT_RECOVERY_BUMP_NOOP(cluster_tt_recovery_count_recovery_verdict_failclosed)
TT_RECOVERY_BUMP_NOOP(cluster_tt_recovery_count_heap_tuples_physically_reverted)
TT_RECOVERY_BUMP_NOOP(cluster_tt_recovery_count_undo_revert_failclosed)

TT_RECOVERY_ZERO(cluster_tt_recovery_active_resolved_aborted_count)
TT_RECOVERY_ZERO(cluster_tt_recovery_remote_active_failclosed_count)
TT_RECOVERY_ZERO(cluster_tt_recovery_wrap_generation_disambiguated_count)
TT_RECOVERY_ZERO(cluster_tt_recovery_recycled_liveness_relaxed_count)
TT_RECOVERY_ZERO(cluster_tt_recovery_scn_highwater_recovered_count)
TT_RECOVERY_ZERO(cluster_tt_recovery_recovery_verdict_failclosed_count)
TT_RECOVERY_ZERO(cluster_tt_recovery_heap_tuples_physically_reverted_count)
TT_RECOVERY_ZERO(cluster_tt_recovery_undo_revert_failclosed_count)

void
cluster_terminal_authority_count_decision(ClusterTerminalAuthorityReason reason)
{
	(void)reason;
}

TT_RECOVERY_ZERO(cluster_terminal_authority_check_count)
TT_RECOVERY_ZERO(cluster_terminal_authority_ok_count)
TT_RECOVERY_ZERO(cluster_terminal_authority_failclosed_count)
TT_RECOVERY_ZERO(cluster_terminal_authority_epoch_failclosed_count)
TT_RECOVERY_ZERO(cluster_terminal_authority_ownership_failclosed_count)
TT_RECOVERY_ZERO(cluster_terminal_authority_unknown_failclosed_count)
TT_RECOVERY_ZERO(cluster_terminal_authority_nonterminal_failclosed_count)
TT_RECOVERY_ZERO(cluster_terminal_authority_durable_failclosed_count)
TT_RECOVERY_ZERO(cluster_terminal_authority_retention_failclosed_count)

#endif /* USE_PGRAC_CLUSTER */
