/*-------------------------------------------------------------------------
 * cluster_recovery_relation_census.h
 *   Whole-root relation and heap/history observations for cold recovery.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_RELATION_CENSUS_H
#define CLUSTER_RECOVERY_RELATION_CENSUS_H

#include "cluster/cluster_recovery_page_census.h"

#ifdef USE_PGRAC_CLUSTER
typedef struct ClusterRecoveryRelationCensus ClusterRecoveryRelationCensus;

typedef enum ClusterRecoveryRelationCensusResult {
	CLUSTER_RECOVERY_RELATIONS_MORE = 0,
	CLUSTER_RECOVERY_RELATIONS_ENTRY,
	CLUSTER_RECOVERY_RELATIONS_DRAINED,
	CLUSTER_RECOVERY_RELATIONS_INVALID,
	CLUSTER_RECOVERY_RELATIONS_CHANGED,
	CLUSTER_RECOVERY_RELATIONS_IO,
	CLUSTER_RECOVERY_RELATIONS_STRUCTURE_REQUIRED,
	CLUSTER_RECOVERY_RELATIONS_BASE_REQUIRED,
	CLUSTER_RECOVERY_RELATIONS_CORRUPT,
	CLUSTER_RECOVERY_RELATIONS_CAPACITY
} ClusterRecoveryRelationCensusResult;

typedef enum ClusterRecoveryRelationCensusKind {
	CLUSTER_RECOVERY_RELATIONS_ROOT = 1,
	CLUSTER_RECOVERY_RELATIONS_PAGE,
	CLUSTER_RECOVERY_RELATIONS_REFERENCE,
	CLUSTER_RECOVERY_RELATIONS_FORK_END
} ClusterRecoveryRelationCensusKind;

typedef struct ClusterRecoveryRelationCensusEntry {
	ClusterRecoveryRelationCensusKind kind;
	uint32 root_index;
	const ClusterRecoveryRootEntry *root;
	/* Original physical page, not an inverse-applied historical version. */
	ClusterRecoveryRelationPage page;
	ClusterRecoveryPageReference reference;
	bool heap; /* original MAIN/ITL layout only, not semantic completeness */
} ClusterRecoveryRelationCensusEntry;

/* Borrow the OBSERVED complete ROOT/WAL union and observed transaction
 * forest, both of which must outlive this cursor. The original cold owner
 * retains isolation/cuts and supplies all TT/WAL roots before forest use.
 * memory_budget covers this cursor and its relation/page consumers, not
 * these borrowed owners or their later full physical re-observation.
 * Any begin refusal also invalidates both supplied owners. */
extern ClusterRecoveryRelationCensusResult
cluster_recovery_relation_census_begin(ClusterRecoveryRootSet *roots,
									   ClusterRecoveryReferenceGraph *graph, size_t memory_budget,
									   ClusterRecoveryRelationCensus **out);

/* Emit every root, including non-relations, and every physical relation
 * page. MAIN/ITL pages also emit their original heap/history references.
 * Other raw pages/roots remain explicit obligations for their own consumers.
 * Each call emits one item or invokes one original bounded reader/consumer.
 * FORK_END follows both SPACE rereads. After all forks, re-observe the whole
 * original root baseline. No dependent root reader may run concurrently
 * with this final pass. Entries are borrowed until next()/destroy(); they
 * remain provisional until the original complete cold input recheck.
 *
 * Failures revoke roots and forest, even after DRAINED. DRAINED is only this
 * traversal: native/MX/HOT/relmap/structural interpretation and full graph
 * sealing remain required. No terminal, apply, OPEN or GC permission. */
extern ClusterRecoveryRelationCensusResult
cluster_recovery_relation_census_next(ClusterRecoveryRelationCensus *census,
									  ClusterRecoveryRelationCensusEntry *out);
extern void cluster_recovery_relation_census_destroy(ClusterRecoveryRelationCensus *census);
#endif
#endif
