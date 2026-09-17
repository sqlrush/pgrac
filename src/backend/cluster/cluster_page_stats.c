/*-------------------------------------------------------------------------
 *
 * cluster_page_stats.c
 *	  RF-PAGE PGDEL-08 — recovery-only observability (implementation).
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-08 / §9.1 / §9.3 / §11.1 G2, G4′.
 *
 *	  One producer per counter (the PGDEL-09 wiring fires these at the
 *	  owning proof points); one consumer per counter (the
 *	  pg_stat_cluster_counters mirror + the §9.3 dump).  The G2
 *	  vocabulary table is the single source of semantic truth.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_page_stats.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/timestamp.h" /* GetCurrentTimestamp */
#include "cluster/cluster_page_stats.h"

void
cluster_page_stats_init(ClusterPageRecoveryStats *stats)
{
	int i;
	pg_atomic_uint64 *fields;
	int nfields;

	if (stats == NULL)
		return;
	fields = (pg_atomic_uint64 *)stats;
	nfields = sizeof(ClusterPageRecoveryStats) / sizeof(pg_atomic_uint64);
	for (i = 0; i < nfields; i++)
		pg_atomic_init_u64(&fields[i], 0);
}

void
cluster_page_stats_source_selected(ClusterPageRecoveryStats *stats, ClusterPageSourceKind kind)
{
	if (stats == NULL)
		return;
	switch (kind) {
	case CLUSTER_PAGE_SOURCE_CURRENT:
		pg_atomic_fetch_add_u64(&stats->source_selected_current, 1);
		break;
	case CLUSTER_PAGE_SOURCE_PI:
		pg_atomic_fetch_add_u64(&stats->source_selected_pi, 1);
		break;
	case CLUSTER_PAGE_SOURCE_STORAGE:
		pg_atomic_fetch_add_u64(&stats->source_selected_storage, 1);
		break;
	default:
		pg_atomic_fetch_add_u64(&stats->source_invalid, 1);
		break;
	}
}

void
cluster_page_stats_source_invalid(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_fetch_add_u64(&stats->source_invalid, 1);
}

void
cluster_page_stats_source_missing(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_fetch_add_u64(&stats->source_missing, 1);
}

void
cluster_page_stats_source_conflict(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_fetch_add_u64(&stats->source_conflict, 1);
}

void
cluster_page_stats_result_skip(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_fetch_add_u64(&stats->result_skip, 1);
}

void
cluster_page_stats_apply(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_fetch_add_u64(&stats->apply_count, 1);
}

void
cluster_page_stats_version_mismatch(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_fetch_add_u64(&stats->version_mismatch, 1);
}

void
cluster_page_stats_unknown_class_blocked(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_fetch_add_u64(&stats->unknown_class_blocked, 1);
}

void
cluster_page_stats_authority_stale(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_fetch_add_u64(&stats->authority_stale_rejected, 1);
}

void
cluster_page_stats_early_release(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_fetch_add_u64(&stats->resource_early_release, 1);
}

void
cluster_page_stats_retire_denied(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_fetch_add_u64(&stats->retire_denied, 1);
}

void
cluster_page_stats_d3_rebuild(ClusterPageRecoveryStats *stats, bool optimization_hit)
{
	if (stats == NULL)
		return;
	if (optimization_hit)
		pg_atomic_fetch_add_u64(&stats->d3_optimization_hit, 1);
	else
		pg_atomic_fetch_add_u64(&stats->d3_rebuild, 1);
}

void
cluster_page_stats_stable_base_unresolved(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_fetch_add_u64(&stats->stable_base_unresolved, 1);
}

void
cluster_page_stats_contributors(ClusterPageRecoveryStats *stats, uint64 records, uint64 threads,
								uint64 gaps)
{
	if (stats == NULL)
		return;
	pg_atomic_write_u64(&stats->contributor_records, records);
	pg_atomic_write_u64(&stats->contributor_threads, threads);
	pg_atomic_write_u64(&stats->contributor_gaps, gaps);
}

void
cluster_page_stats_pinned_bytes(ClusterPageRecoveryStats *stats, uint64 bytes)
{
	if (stats != NULL)
		pg_atomic_write_u64(&stats->retained_pinned_bytes, bytes);
}

void
cluster_page_stats_note_page_write(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_write_u64(&stats->last_page_write_ts, (uint64)GetCurrentTimestamp());
}

void
cluster_page_stats_note_durability(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_write_u64(&stats->last_durability_barrier_ts, (uint64)GetCurrentTimestamp());
}

void
cluster_page_stats_note_post_read(ClusterPageRecoveryStats *stats)
{
	if (stats != NULL)
		pg_atomic_write_u64(&stats->last_post_read_ts, (uint64)GetCurrentTimestamp());
}

/*
 * G2 vocabulary: one kind per name, matching the cluster_pgstat registry
 * entries.  The single table every consumer reads.
 */
typedef struct ClusterPageStatName {
	const char *name;
	ClusterPageStatKind kind;
} ClusterPageStatName;

static const ClusterPageStatName cluster_page_stat_names[] = {
	{ "cluster.page.source_selected_current", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.source_selected_pi", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.source_selected_storage", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.source_invalid", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.source_missing", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.source_conflict", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.result_skip", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.apply_count", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.version_mismatch", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.unknown_class_blocked", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.authority_stale_rejected", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.resource_early_release", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.retire_denied", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.d3_rebuild", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.d3_optimization_hit", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.stable_base_unresolved", CLUSTER_PAGE_STAT_EVENT },
	{ "cluster.page.contributor_records", CLUSTER_PAGE_STAT_GAUGE },
	{ "cluster.page.contributor_threads", CLUSTER_PAGE_STAT_GAUGE },
	{ "cluster.page.contributor_gaps", CLUSTER_PAGE_STAT_GAUGE },
	{ "cluster.page.retained_pinned_bytes", CLUSTER_PAGE_STAT_GAUGE },
	{ "cluster.page.last_page_write_ts", CLUSTER_PAGE_STAT_TIMESTAMP },
	{ "cluster.page.last_durability_barrier_ts", CLUSTER_PAGE_STAT_TIMESTAMP },
	{ "cluster.page.last_post_read_ts", CLUSTER_PAGE_STAT_TIMESTAMP },
};

bool
cluster_page_stats_describe(const char *name, ClusterPageStatKind *kind)
{
	int i;

	if (name == NULL)
		return false;
	for (i = 0; i < (int)lengthof(cluster_page_stat_names); i++) {
		if (strcmp(cluster_page_stat_names[i].name, name) == 0) {
			if (kind != NULL)
				*kind = cluster_page_stat_names[i].kind;
			return true;
		}
	}
	return false; /* unknown name: consumers fail closed */
}

static const char *
cluster_page_source_kind_name(ClusterPageSourceKind kind)
{
	switch (kind) {
	case CLUSTER_PAGE_SOURCE_CURRENT:
		return "CURRENT";
	case CLUSTER_PAGE_SOURCE_PI:
		return "PI";
	case CLUSTER_PAGE_SOURCE_STORAGE:
		return "STORAGE";
	}
	return "UNKNOWN";
}

int
cluster_page_attempt_dump(const ClusterPageAttemptDump *dump, char *buf, Size buflen)
{
	uint64 src_tok;
	uint64 term_tok;
	int written;

	if (dump == NULL)
		return 0;
	src_tok = cluster_page_version_valid(&dump->source_version) ? dump->source_version.token : 0;
	term_tok
		= cluster_page_version_valid(&dump->terminal_version) ? dump->terminal_version.token : 0;
	written = snprintf(
		buf, buflen,
		"page-attempt thread=%u gen=" UINT64_FORMAT
		" rel=%u/%u/%u fork=%d block=%u class=%d source=%s"
		" src_tok=" UINT64_FORMAT " term_tok=" UINT64_FORMAT " contributors=" UINT64_FORMAT
		" threads=0x" UINT64_FORMAT " applied=%d mismatch=%d durable=%d post_read=%d"
		" authority=%d released=%d stop=%s",
		(unsigned)dump->failed_origin_thread, dump->failure_generation,
		(unsigned)dump->identity.rlocator.spcOid, (unsigned)dump->identity.rlocator.dbOid,
		(unsigned)dump->identity.rlocator.relNumber, (int)dump->identity.forknum,
		(unsigned)dump->identity.blocknum, (int)dump->page_class,
		cluster_page_source_kind_name(dump->source_kind), src_tok, term_tok,
		dump->contributor_count, dump->contributor_thread_set, (int)dump->applied,
		(int)dump->version_mismatch, (int)dump->durability_ok, (int)dump->post_read_ok,
		(int)dump->authority_revalidated, (int)dump->released,
		dump->stop_reason != NULL ? dump->stop_reason : "none");
	return written;
}
