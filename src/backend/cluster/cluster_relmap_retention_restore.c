/* PGRAC: original-origin relmap pending restore from both durable copies.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres.h"

#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_relmap_retention_restore.h"

static ClusterRelmapRetentionResult
restore_failed(ClusterRelmapRetentionDirectory *const directories[], size_t count,
	ClusterRelmapRetentionResult result)
{
	if (directories != NULL && count <= CLUSTER_RELMAP_RETENTION_MAX_ORIGINS)
		for (size_t i = 0; i < count; i++)
			cluster_relmap_retention_invalidate(directories[i]);
	return result;
}

ClusterRelmapRetentionResult
cluster_relmap_retention_restore_namespace(const ClusterRelmapV2Namespace *expected,
	ClusterRelmapRetentionDirectory *const directories[], size_t count)
{
	ClusterRelmapRetentionDirectory *origins[CLUSTER_RELMAP_RETENTION_MAX_ORIGINS] = { 0 };
	ClusterRelmapRetentionKey key = { 0 };
	ClusterRelmapRetentionResult result;
	ClusterRelmapV2View view;

	if (expected == NULL || expected->system_identifier == 0
		|| expected->database_incarnation == 0
		|| expected->shared != (expected->dbid == InvalidOid)
		|| directories == NULL || count == 0 || count > lengthof(origins))
		return restore_failed(directories, count, CLUSTER_RELMAP_RETENTION_INVALID);
	for (size_t i = 0; i < count; i++) {
		ClusterRelmapRetentionDirectory *directory = directories[i];

		if (directory == NULL || directory->state != CLUSTER_RELMAP_RETENTION_BUILDING
			|| directory->system_identifier != expected->system_identifier
			|| directory->database_incarnation != expected->database_incarnation
			|| directory->origin >= lengthof(origins) || origins[directory->origin] != NULL)
			return restore_failed(directories, count, CLUSTER_RELMAP_RETENTION_INVALID);
		origins[directory->origin] = directory;
	}
	/* These directories are exclusively owned, unpublished build storage.
	 * Never hold the runtime retention lock while reading authority files. */
	if (cluster_relmap_authority_v2_read(expected, &view) != CLUSTER_RELMAP_V2_PAIR_VALID
		|| !view.pending_complete)
		return restore_failed(directories, count, CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	if (!view.has_pending)
		return CLUSTER_RELMAP_RETENTION_OK;
	if (view.pending_owner.node >= lengthof(origins) || origins[view.pending_owner.node] == NULL)
		return restore_failed(directories, count, CLUSTER_RELMAP_RETENTION_INVALID);
	key.identity = *expected;
	key.generation = view.pending_generation;
	key.origin = view.pending_owner.node;
	key.top_full_xid = view.pending_owner.top_full_xid;
	key.membership_epoch = view.pending_owner.membership_epoch;
	key.admitted_incarnation = view.pending_owner.admitted_incarnation;
	key.boot_incarnation = view.pending_owner.boot_incarnation;
	result = cluster_relmap_retention_build_add(origins[key.origin], &key, view.pending_owner.wal_end);
	if (result != CLUSTER_RELMAP_RETENTION_OK)
		return restore_failed(directories, count, result);
	return CLUSTER_RELMAP_RETENTION_OK;
}
