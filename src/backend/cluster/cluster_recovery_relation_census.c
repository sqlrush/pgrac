/*-------------------------------------------------------------------------
 * cluster_recovery_relation_census.c
 *   Whole-root relation and heap/history observations for cold recovery.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_recovery_relation_census.h"
#ifdef USE_PGRAC_CLUSTER
struct ClusterRecoveryRelationCensus {
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryReferenceGraph *graph;
	ClusterRecoveryRelationScan *relation;
	ClusterRecoveryPageCensus *history;
	ClusterRecoveryRelationPage page;
	const ClusterRecoveryRootEntry *root;
	uint8 root_digest[32];
	uint32 root_count, next_root, root_index;
	size_t relation_budget, history_budget;
	bool heap;
	unsigned stage;
	ClusterRecoveryRelationCensusResult status;
};

enum {
	RELATIONS_ROOT,
	RELATIONS_BEGIN_FORK,
	RELATIONS_READ_FORK,
	RELATIONS_BEGIN_HISTORY,
	RELATIONS_READ_HISTORY,
	RELATIONS_FINAL_ROOTS
};

static ClusterRecoveryRelationCensusResult
relations_revoke(ClusterRecoveryRootSet *roots, ClusterRecoveryReferenceGraph *graph,
				 ClusterRecoveryRelationCensusResult result)
{
	cluster_recovery_root_set_invalidate(roots);
	cluster_recovery_reference_graph_invalidate(graph);
	return result;
}

static ClusterRecoveryRelationCensusResult
relations_fail(ClusterRecoveryRelationCensus *census, ClusterRecoveryRelationCensusResult result)
{
	if (census != NULL) {
		census->status = result;
		return relations_revoke(census->roots, census->graph, result);
	}
	return result;
}

static ClusterRecoveryRelationCensusResult
relations_root_failure(ClusterRecoveryRelationCensus *census, ClusterRecoveryRootsResult result)
{
	ClusterRecoveryRelationCensusResult mapped;

	switch (result) {
	case CLUSTER_RECOVERY_ROOTS_IO:
		mapped = CLUSTER_RECOVERY_RELATIONS_IO;
		break;
	case CLUSTER_RECOVERY_ROOTS_CAPACITY:
		mapped = CLUSTER_RECOVERY_RELATIONS_CAPACITY;
		break;
	case CLUSTER_RECOVERY_ROOTS_CHANGED:
		mapped = CLUSTER_RECOVERY_RELATIONS_CHANGED;
		break;
	default:
		mapped = CLUSTER_RECOVERY_RELATIONS_INVALID;
		break;
	}
	return relations_fail(census, mapped);
}

static ClusterRecoveryRelationCensusResult
relations_fork_failure(ClusterRecoveryRelationCensus *census, ClusterRecoveryRelationResult result)
{
	ClusterRecoveryRelationCensusResult mapped;

	switch (result) {
	case CLUSTER_RECOVERY_RELATION_STRUCTURE_REQUIRED:
		mapped = CLUSTER_RECOVERY_RELATIONS_STRUCTURE_REQUIRED;
		break;
	case CLUSTER_RECOVERY_RELATION_IO:
		mapped = CLUSTER_RECOVERY_RELATIONS_IO;
		break;
	case CLUSTER_RECOVERY_RELATION_CHANGED:
		mapped = CLUSTER_RECOVERY_RELATIONS_CHANGED;
		break;
	case CLUSTER_RECOVERY_RELATION_CORRUPT:
		mapped = CLUSTER_RECOVERY_RELATIONS_CORRUPT;
		break;
	case CLUSTER_RECOVERY_RELATION_CAPACITY:
		mapped = CLUSTER_RECOVERY_RELATIONS_CAPACITY;
		break;
	default:
		mapped = CLUSTER_RECOVERY_RELATIONS_INVALID;
		break;
	}
	return relations_fail(census, mapped);
}

static ClusterRecoveryRelationCensusResult
relations_history_failure(ClusterRecoveryRelationCensus *census,
						  ClusterRecoveryPageCensusResult result)
{
	ClusterRecoveryRelationCensusResult mapped;

	switch (result) {
	case CLUSTER_RECOVERY_PAGE_BASE_REQUIRED:
		mapped = CLUSTER_RECOVERY_RELATIONS_BASE_REQUIRED;
		break;
	case CLUSTER_RECOVERY_PAGE_CHANGED:
		mapped = CLUSTER_RECOVERY_RELATIONS_CHANGED;
		break;
	case CLUSTER_RECOVERY_PAGE_CORRUPT:
	case CLUSTER_RECOVERY_PAGE_IDENTITY:
		mapped = CLUSTER_RECOVERY_RELATIONS_CORRUPT;
		break;
	case CLUSTER_RECOVERY_PAGE_CAPACITY:
		mapped = CLUSTER_RECOVERY_RELATIONS_CAPACITY;
		break;
	default:
		mapped = CLUSTER_RECOVERY_RELATIONS_INVALID;
		break;
	}
	return relations_fail(census, mapped);
}

ClusterRecoveryRelationCensusResult
cluster_recovery_relation_census_begin(ClusterRecoveryRootSet *roots,
									   ClusterRecoveryReferenceGraph *graph, size_t memory_budget,
									   ClusterRecoveryRelationCensus **out)
{
	ClusterRecoveryRelationCensus *census;
	uint8 digest[32];
	size_t remaining;

	if (out != NULL)
		*out = NULL;
	if (out == NULL || !cluster_recovery_root_set_digest(roots, digest)
		|| !cluster_recovery_reference_graph_observed(graph))
		return relations_revoke(roots, graph, CLUSTER_RECOVERY_RELATIONS_INVALID);
	/* The original sub-consumers enforce their own allocation bounds. This
	 * reservation includes this cursor and both concurrently live consumers. */
	if (memory_budget <= sizeof(*census))
		return relations_revoke(roots, graph, CLUSTER_RECOVERY_RELATIONS_CAPACITY);
	remaining = memory_budget - sizeof(*census);
	if (remaining / 2 < 8 * BLCKSZ)
		return relations_revoke(roots, graph, CLUSTER_RECOVERY_RELATIONS_CAPACITY);
	census = calloc(1, sizeof(*census));
	if (census == NULL)
		return relations_revoke(roots, graph, CLUSTER_RECOVERY_RELATIONS_CAPACITY);
	census->roots = roots;
	census->graph = graph;
	census->root_count = cluster_recovery_root_set_count(roots);
	memcpy(census->root_digest, digest, sizeof(digest));
	census->relation_budget = remaining / 2;
	census->history_budget = remaining - census->relation_budget;
	*out = census;
	return CLUSTER_RECOVERY_RELATIONS_MORE;
}

static ClusterRecoveryRelationCensusResult
relations_emit(ClusterRecoveryRelationCensus *census, ClusterRecoveryRelationCensusEntry *out,
			   ClusterRecoveryRelationCensusKind kind)
{
	out->kind = kind;
	out->root_index = census->root_index;
	out->root = census->root;
	if (kind == CLUSTER_RECOVERY_RELATIONS_PAGE || kind == CLUSTER_RECOVERY_RELATIONS_REFERENCE) {
		out->page = census->page;
		out->heap = census->heap;
	}
	return CLUSTER_RECOVERY_RELATIONS_ENTRY;
}

ClusterRecoveryRelationCensusResult
cluster_recovery_relation_census_next(ClusterRecoveryRelationCensus *census,
									  ClusterRecoveryRelationCensusEntry *out)
{
	uint8 digest[32];

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (census == NULL || out == NULL)
		return relations_fail(census, CLUSTER_RECOVERY_RELATIONS_INVALID);
	if (census->status > CLUSTER_RECOVERY_RELATIONS_DRAINED)
		return census->status;
	if (!cluster_recovery_reference_graph_observed(census->graph))
		return relations_fail(census, CLUSTER_RECOVERY_RELATIONS_CHANGED);
	if (census->stage != RELATIONS_FINAL_ROOTS
		|| census->status == CLUSTER_RECOVERY_RELATIONS_DRAINED) {
		if (!cluster_recovery_root_set_digest(census->roots, digest)
			|| memcmp(digest, census->root_digest, sizeof(digest)) != 0
			|| cluster_recovery_root_set_count(census->roots) != census->root_count)
			return relations_fail(census, CLUSTER_RECOVERY_RELATIONS_CHANGED);
	}
	if (census->status == CLUSTER_RECOVERY_RELATIONS_DRAINED)
		return census->status;
	switch (census->stage) {
	case RELATIONS_ROOT:
		if (census->next_root < census->root_count) {
			census->root_index = census->next_root++;
			census->root = cluster_recovery_root_set_entry(census->roots, census->root_index);
			if (census->root == NULL)
				return relations_fail(census, CLUSTER_RECOVERY_RELATIONS_CHANGED);
			if (census->root->file.kind == CLUSTER_RECOVERY_FILE_RELATION)
				census->stage = RELATIONS_BEGIN_FORK;
			return relations_emit(census, out, CLUSTER_RECOVERY_RELATIONS_ROOT);
		} else {
			ClusterRecoveryRootsResult result
				= cluster_recovery_root_set_recheck_begin(census->roots);
			if (result != CLUSTER_RECOVERY_ROOTS_MORE)
				return relations_root_failure(census, result);
			census->stage = RELATIONS_FINAL_ROOTS;
			census->root = NULL;
			return CLUSTER_RECOVERY_RELATIONS_MORE;
		}
	case RELATIONS_BEGIN_FORK: {
		ClusterRecoveryRelationResult result = cluster_recovery_relation_begin(
			census->roots, census->root_index, census->relation_budget, &census->relation);
		if (result != CLUSTER_RECOVERY_RELATION_MORE)
			return relations_fork_failure(census, result);
		census->stage = RELATIONS_READ_FORK;
		return CLUSTER_RECOVERY_RELATIONS_MORE;
	}
	case RELATIONS_READ_FORK: {
		ClusterRecoveryRelationResult result
			= cluster_recovery_relation_next(census->relation, &census->page);
		if (result == CLUSTER_RECOVERY_RELATION_MORE)
			return CLUSTER_RECOVERY_RELATIONS_MORE;
		if (result == CLUSTER_RECOVERY_RELATION_PAGE) {
			census->heap
				= census->page.forknum == MAIN_FORKNUM && PageHasItl((Page)census->page.page);
			if (census->heap)
				census->stage = RELATIONS_BEGIN_HISTORY;
			return relations_emit(census, out, CLUSTER_RECOVERY_RELATIONS_PAGE);
		}
		if (result != CLUSTER_RECOVERY_RELATION_DRAINED)
			return relations_fork_failure(census, result);
		cluster_recovery_relation_destroy(census->relation);
		census->relation = NULL;
		memset(&census->page, 0, sizeof(census->page));
		census->stage = RELATIONS_ROOT;
		return relations_emit(census, out, CLUSTER_RECOVERY_RELATIONS_FORK_END);
	}
	case RELATIONS_BEGIN_HISTORY: {
		ClusterRecoveryPageCensusResult result = cluster_recovery_page_census_begin(
			&census->page, census->graph, census->history_budget, &census->history);
		if (result != CLUSTER_RECOVERY_PAGE_MORE)
			return relations_history_failure(census, result);
		census->stage = RELATIONS_READ_HISTORY;
		return CLUSTER_RECOVERY_RELATIONS_MORE;
	}
	case RELATIONS_READ_HISTORY: {
		ClusterRecoveryPageReference reference;
		ClusterRecoveryPageCensusResult result
			= cluster_recovery_page_census_next(census->history, &reference);
		if (result == CLUSTER_RECOVERY_PAGE_MORE)
			return CLUSTER_RECOVERY_RELATIONS_MORE;
		if (result == CLUSTER_RECOVERY_PAGE_ENTRY) {
			out->reference = reference;
			return relations_emit(census, out, CLUSTER_RECOVERY_RELATIONS_REFERENCE);
		}
		if (result != CLUSTER_RECOVERY_PAGE_DRAINED)
			return relations_history_failure(census, result);
		cluster_recovery_page_census_destroy(census->history);
		census->history = NULL;
		census->stage = RELATIONS_READ_FORK;
		return CLUSTER_RECOVERY_RELATIONS_MORE;
	}
	case RELATIONS_FINAL_ROOTS: {
		ClusterRecoveryRootsEvent event;
		ClusterRecoveryRootsResult result = cluster_recovery_root_set_step(census->roots, &event);
		if (result < CLUSTER_RECOVERY_ROOTS_OBSERVED)
			return CLUSTER_RECOVERY_RELATIONS_MORE;
		if (result != CLUSTER_RECOVERY_ROOTS_OBSERVED)
			return relations_root_failure(census, result);
		if (!cluster_recovery_root_set_digest(census->roots, digest)
			|| memcmp(digest, census->root_digest, sizeof(digest)) != 0
			|| cluster_recovery_root_set_count(census->roots) != census->root_count)
			return relations_fail(census, CLUSTER_RECOVERY_RELATIONS_CHANGED);
		census->status = CLUSTER_RECOVERY_RELATIONS_DRAINED;
		return census->status;
	}
	default:
		return relations_fail(census, CLUSTER_RECOVERY_RELATIONS_INVALID);
	}
}

void
cluster_recovery_relation_census_destroy(ClusterRecoveryRelationCensus *census)
{
	if (census != NULL) {
		cluster_recovery_page_census_destroy(census->history);
		cluster_recovery_relation_destroy(census->relation);
		free(census);
	}
}
#endif
