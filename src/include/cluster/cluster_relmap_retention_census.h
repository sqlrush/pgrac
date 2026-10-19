/* PGRAC: relmap pending census through the original cold ROOT reader.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RELMAP_RETENTION_CENSUS_H
#define CLUSTER_RELMAP_RETENTION_CENSUS_H

#include "cluster/cluster_recovery_root_set.h"
#include "cluster/cluster_relmap_retention_restore.h"

#ifdef USE_PGRAC_CLUSTER
typedef struct ClusterRelmapRetentionCensus ClusterRelmapRetentionCensus;

typedef enum ClusterRelmapCensusResult {
	CLUSTER_RELMAP_CENSUS_MORE = 0,
	CLUSTER_RELMAP_CENSUS_DRAINED,
	CLUSTER_RELMAP_CENSUS_INVALID,
	CLUSTER_RELMAP_CENSUS_UNAVAILABLE,
	CLUSTER_RELMAP_CENSUS_CHANGED,
	CLUSTER_RELMAP_CENSUS_CAPACITY
} ClusterRelmapCensusResult;

/* A copied committed image, never the pending slot or a terminal proof. */
typedef struct ClusterRelmapCensusCommitted {
	ClusterRelmapV2Namespace identity;
	uint64 generation;
	RelMapFile map;
	bool present;
} ClusterRelmapCensusCommitted;

/* Borrow the original isolated, OBSERVED ROOT set, not the configured path.
 * Every supplied original-origin directory must be exclusive, unpublished and
 * BUILDING, with native/SIDE floor/last-issued and the complete origin set
 * already qualified by the original cold owner. Copy the pointer array only;
 * keep roots and directories alive and exclusively owned until destroy.
 * The caller's memory budget covers this fixed scratch cursor.
 *
 * This authenticates neither those qualifications nor ROOT/isolation/retention.
 * Any refusal invalidates roots and all directories in a valid bounded input
 * array, preserving pins. An oversized array is never accessed. */
extern ClusterRelmapCensusResult cluster_relmap_retention_census_begin(
	ClusterRecoveryRootSet *roots, uint64 system_identifier, uint64 database_incarnation,
	const uint8 storage_uuid[16], ClusterRelmapRetentionDirectory *const directories[],
	size_t count, size_t memory_budget, ClusterRelmapRetentionCensus **out);

/* At most128 metadata candidates, one <=BLCKSZ original file read, or one
 * existing whole-ROOT recheck step. Require global/base and both exact v2
 * copies for global and every canonical physical base/db directory, including
 * namespaces with no WAL reference. Restore backup-only pending to its original
 * origin. Missing, degraded, conflicting or changed input refuses, never empty.
 *
 * DRAINED requires the original entire ROOT set to pass recheck unchanged.
 * Directories remain BUILDING: no terminal, COMPLETE publication, truncate,
 * CTRC release, allocation or OPEN permission. Destroy before the original
 * owner performs its remaining qualification/publication steps. */
extern ClusterRelmapCensusResult
cluster_relmap_retention_census_next(ClusterRelmapRetentionCensus *census);

/* Same cursor and bounded work as next. Clear output on every call, then set
 * present only on the MORE tick that finishes a namespace's complete legal
 * pair AND restores any pending obligation to its original-origin directory.
 * Copies the selected committed image and exact namespace/generation; no
 * configured-path reread, pending image, TT binding or release permission.
 * NULL output discards that tick's image, as the legacy next API does.
 *
 * Every emitted image remains provisional until this cursor returns DRAINED
 * after the whole ROOT recheck. On any refusal, the caller must discard ALL
 * images from this cursor, including earlier copies; output clearing cannot
 * revoke copies already held by the caller. DRAINED still leaves directories
 * BUILDING and the original cold owner's remaining qualifications unchanged.
 * Use this API throughout a scan when every namespace image is required. */
extern ClusterRelmapCensusResult cluster_relmap_retention_census_next_committed(
	ClusterRelmapRetentionCensus *census, ClusterRelmapCensusCommitted *out);

/* Interrupted scans invalidate the borrowed build and ROOT set, retaining
 * every restored pin. No directory, pin or ROOT ownership is freed here. */
extern void cluster_relmap_retention_census_destroy(ClusterRelmapRetentionCensus *census);
#endif
#endif
