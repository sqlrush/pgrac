/* PGRAC: original-origin relmap pending restore from both durable copies.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RELMAP_RETENTION_RESTORE_H
#define CLUSTER_RELMAP_RETENTION_RESTORE_H

#include "cluster/cluster_relmap_retention.h"

#define CLUSTER_RELMAP_RETENTION_MAX_ORIGINS 128

/* One namespace observation, NOT a complete cold census or release proof.
 * Startup exclusively owns all supplied, unpublished BUILDING directories;
 * no retention lock is held across this disk read. Native floor/last-issued
 * values and the complete original-origin set must already be qualified by
 * the original cold owner. Directories need not be ordered by origin.
 *
 * Both copies must be valid and the pending union complete. Restore the exact
 * pending into its original origin, including a backup-only obligation after
 * partial publish/discard. A clean pair leaves all directories BUILDING.
 * For a non-NULL array within the fixed bound, any refusal invalidates all
 * supplied directories without deleting pins. A failed build cannot be reused.
 * NULL/oversized arrays are invalid inputs; no element is accessed when count
 * exceeds the fixed bound.
 *
 * The caller must enumerate/recheck ALL selected namespaces under the original
 * cold isolation before publishing COMPLETE and enabling any native/TT/WAL
 * reclamation or new allocation. No live consumer is enabled by this API. */
extern ClusterRelmapRetentionResult cluster_relmap_retention_restore_namespace(
	const ClusterRelmapV2Namespace *expected,
	ClusterRelmapRetentionDirectory *const directories[], size_t count);

#endif
