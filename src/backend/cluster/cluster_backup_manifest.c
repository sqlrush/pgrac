/*-------------------------------------------------------------------------
 *
 * cluster_backup_manifest.c
 *	  Dependency-light helpers for cluster backup manifests and PITR cuts.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_backup_manifest.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-6.5-cluster-aware-backup-restore-pitr.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_backup.h"
#include "port/pg_crc32c.h"

static int
cluster_backup_scn_cmp(SCN a, SCN b)
{
#ifdef USE_PGRAC_CLUSTER
	return scn_time_cmp(a, b);
#else
	uint64 alocal = scn_local(a);
	uint64 blocal = scn_local(b);

	if (alocal < blocal)
		return -1;
	if (alocal > blocal)
		return 1;
	return 0;
#endif
}

void
cluster_backup_manifest_init(ClusterBackupManifest *manifest, const char *backup_id)
{
	if (manifest == NULL)
		return;

	memset(manifest, 0, sizeof(*manifest));
	manifest->magic = CLUSTER_BACKUP_MANIFEST_MAGIC;
	manifest->version = CLUSTER_BACKUP_MANIFEST_VERSION;
	if (backup_id != NULL)
		strlcpy(manifest->backup_id, backup_id, sizeof(manifest->backup_id));
}

bool
cluster_backup_manifest_set_thread(ClusterBackupManifest *manifest, int thread_index,
								   const ClusterBackupManifestThread *thread)
{
	if (manifest == NULL || thread == NULL)
		return false;
	if (thread_index < 0 || thread_index >= CLUSTER_MAX_NODES)
		return false;
	if (thread->thread_id == 0 || thread->thread_id > CLUSTER_MAX_NODES)
		return false;

	if (!manifest->threads[thread_index].present)
		manifest->thread_count++;
	manifest->threads[thread_index] = *thread;
	manifest->threads[thread_index].present = true;
	return true;
}

uint32
cluster_backup_manifest_compute_crc(const ClusterBackupManifest *manifest)
{
	ClusterBackupManifest copy;
	pg_crc32c crc;

	if (manifest == NULL)
		return 0;

	copy = *manifest;
	copy.manifest_crc = 0;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &copy, sizeof(copy));
	FIN_CRC32C(crc);
	return crc;
}

void
cluster_backup_manifest_seal(ClusterBackupManifest *manifest)
{
	if (manifest == NULL)
		return;
	manifest->manifest_crc = cluster_backup_manifest_compute_crc(manifest);
}

ClusterBackupManifestReason
cluster_backup_manifest_validate(const ClusterBackupManifest *manifest)
{
	int i;
	int present_count = 0;

	if (manifest == NULL)
		return CLUSTER_BACKUP_MANIFEST_NULL;
	if (manifest->magic != CLUSTER_BACKUP_MANIFEST_MAGIC)
		return CLUSTER_BACKUP_MANIFEST_BAD_MAGIC;
	if (manifest->version != CLUSTER_BACKUP_MANIFEST_VERSION)
		return CLUSTER_BACKUP_MANIFEST_BAD_VERSION;
	if (manifest->node_count == 0 || manifest->node_count > CLUSTER_MAX_NODES
		|| manifest->thread_count == 0 || manifest->thread_count > CLUSTER_MAX_NODES)
		return CLUSTER_BACKUP_MANIFEST_BAD_COUNTS;
	if (!manifest->control_included)
		return CLUSTER_BACKUP_MANIFEST_MISSING_CONTROL;
	if (!SCN_VALID(manifest->consistent_scn) || !SCN_VALID(manifest->scn_durable_peak)
		|| cluster_backup_scn_cmp(manifest->scn_durable_peak, manifest->consistent_scn) < 0)
		return CLUSTER_BACKUP_MANIFEST_BAD_SCN_PEAK;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		const ClusterBackupManifestThread *thread = &manifest->threads[i];

		if (!thread->present)
			continue;

		present_count++;
		if (thread->thread_id == 0 || thread->thread_id > CLUSTER_MAX_NODES)
			return CLUSTER_BACKUP_MANIFEST_MISSING_THREAD;
		if (thread->start_redo_lsn == InvalidXLogRecPtr
			|| thread->checkpoint_lsn == InvalidXLogRecPtr
			|| thread->stop_cut_lsn == InvalidXLogRecPtr
			|| thread->stop_cut_lsn < thread->start_redo_lsn)
			return CLUSTER_BACKUP_MANIFEST_BAD_LSN_RANGE;
		if (!thread->wal_included)
			return CLUSTER_BACKUP_MANIFEST_MISSING_WAL;
		if (!thread->undo_included)
			return CLUSTER_BACKUP_MANIFEST_MISSING_UNDO;
		if (!thread->tt_included)
			return CLUSTER_BACKUP_MANIFEST_MISSING_TT;
	}

	if (present_count != (int)manifest->thread_count)
		return CLUSTER_BACKUP_MANIFEST_MISSING_THREAD;
	if (manifest->manifest_crc != cluster_backup_manifest_compute_crc(manifest))
		return CLUSTER_BACKUP_MANIFEST_BAD_CRC;

	return CLUSTER_BACKUP_MANIFEST_OK;
}

const char *
cluster_backup_manifest_reason_name(ClusterBackupManifestReason reason)
{
	switch (reason) {
	case CLUSTER_BACKUP_MANIFEST_OK:
		return "ok";
	case CLUSTER_BACKUP_MANIFEST_NULL:
		return "null";
	case CLUSTER_BACKUP_MANIFEST_BAD_MAGIC:
		return "bad_magic";
	case CLUSTER_BACKUP_MANIFEST_BAD_VERSION:
		return "bad_version";
	case CLUSTER_BACKUP_MANIFEST_BAD_COUNTS:
		return "bad_counts";
	case CLUSTER_BACKUP_MANIFEST_MISSING_THREAD:
		return "missing_thread";
	case CLUSTER_BACKUP_MANIFEST_BAD_LSN_RANGE:
		return "bad_lsn_range";
	case CLUSTER_BACKUP_MANIFEST_MISSING_WAL:
		return "missing_wal";
	case CLUSTER_BACKUP_MANIFEST_MISSING_UNDO:
		return "missing_undo";
	case CLUSTER_BACKUP_MANIFEST_MISSING_TT:
		return "missing_tt";
	case CLUSTER_BACKUP_MANIFEST_MISSING_CONTROL:
		return "missing_control";
	case CLUSTER_BACKUP_MANIFEST_BAD_SCN_PEAK:
		return "bad_scn_peak";
	case CLUSTER_BACKUP_MANIFEST_BAD_CRC:
		return "bad_crc";
	}
	return "unknown";
}

ClusterRestorePointCutReason
cluster_restore_point_build(ClusterRestorePoint *out, const char *name, const SCN *thread_scn,
							const XLogRecPtr *thread_lsn, int max_threads,
							bool pending_commits_empty, bool commit_fence_held, uint32 incarnation)
{
	SCN max_scn = InvalidScn;
	int i;
	int nthreads = 0;

	if (!pending_commits_empty)
		return CLUSTER_RESTORE_POINT_CUT_PENDING_COMMITS;
	if (!commit_fence_held)
		return CLUSTER_RESTORE_POINT_CUT_NO_FENCE;
	if (out == NULL || thread_scn == NULL || thread_lsn == NULL || max_threads <= 0
		|| max_threads > CLUSTER_MAX_NODES)
		return CLUSTER_RESTORE_POINT_CUT_NO_THREADS;

	memset(out, 0, sizeof(*out));
	out->present = true;
	out->incarnation = incarnation;
	if (name != NULL)
		strlcpy(out->name, name, sizeof(out->name));

	for (i = 0; i < max_threads; i++) {
		if (!SCN_VALID(thread_scn[i]) && thread_lsn[i] == InvalidXLogRecPtr)
			continue;
		if (!SCN_VALID(thread_scn[i]) || thread_lsn[i] == InvalidXLogRecPtr)
			return CLUSTER_RESTORE_POINT_CUT_BAD_THREAD;

		out->cut_lsn[i] = thread_lsn[i];
		if (!SCN_VALID(max_scn) || cluster_backup_scn_cmp(thread_scn[i], max_scn) > 0)
			max_scn = thread_scn[i];
		nthreads++;
	}

	if (nthreads == 0)
		return CLUSTER_RESTORE_POINT_CUT_NO_THREADS;

	out->cut_scn = max_scn;
	out->thread_count = (uint32)nthreads;
	return CLUSTER_RESTORE_POINT_CUT_OK;
}

const char *
cluster_restore_point_cut_reason_name(ClusterRestorePointCutReason reason)
{
	switch (reason) {
	case CLUSTER_RESTORE_POINT_CUT_OK:
		return "ok";
	case CLUSTER_RESTORE_POINT_CUT_PENDING_COMMITS:
		return "pending_commits";
	case CLUSTER_RESTORE_POINT_CUT_NO_FENCE:
		return "no_fence";
	case CLUSTER_RESTORE_POINT_CUT_NO_THREADS:
		return "no_threads";
	case CLUSTER_RESTORE_POINT_CUT_BAD_THREAD:
		return "bad_thread";
	}
	return "unknown";
}

ClusterPitrTargetReason
cluster_pitr_resolve_scn(const ClusterRestorePoint *points, int npoints, SCN requested_scn,
						 SCN backup_consistent_scn, ClusterRestorePoint *out)
{
	const ClusterRestorePoint *best = NULL;
	int i;

	if (!SCN_VALID(requested_scn) || !SCN_VALID(backup_consistent_scn)
		|| cluster_backup_scn_cmp(requested_scn, backup_consistent_scn) < 0)
		return CLUSTER_PITR_TARGET_BEFORE_BACKUP;
	if (points == NULL || npoints <= 0)
		return CLUSTER_PITR_TARGET_NO_RESTORE_POINT;

	for (i = 0; i < npoints; i++) {
		const ClusterRestorePoint *point = &points[i];

		if (!point->present || !SCN_VALID(point->cut_scn))
			continue;
		if (cluster_backup_scn_cmp(point->cut_scn, backup_consistent_scn) < 0)
			continue;
		if (cluster_backup_scn_cmp(point->cut_scn, requested_scn) > 0)
			continue;
		if (point->thread_count == 0 || point->thread_count > CLUSTER_MAX_NODES)
			return CLUSTER_PITR_TARGET_MISSING_THREAD;
		if (best == NULL || cluster_backup_scn_cmp(point->cut_scn, best->cut_scn) > 0)
			best = point;
	}

	if (best == NULL)
		return CLUSTER_PITR_TARGET_NO_RESTORE_POINT;

	if (out != NULL)
		*out = *best;
	return CLUSTER_PITR_TARGET_OK;
}

const char *
cluster_pitr_target_reason_name(ClusterPitrTargetReason reason)
{
	switch (reason) {
	case CLUSTER_PITR_TARGET_OK:
		return "ok";
	case CLUSTER_PITR_TARGET_NO_RESTORE_POINT:
		return "no_restore_point";
	case CLUSTER_PITR_TARGET_BEFORE_BACKUP:
		return "before_backup";
	case CLUSTER_PITR_TARGET_MISSING_THREAD:
		return "missing_thread";
	case CLUSTER_PITR_TARGET_UNARCHIVED_WAL:
		return "unarchived_wal";
	}
	return "unknown";
}

ClusterRestoreCompatibilityReason
cluster_backup_manifest_compatible(const ClusterBackupManifest *manifest, uint32 current_catversion,
								   uint32 current_storage_id, uint32 expected_node_count)
{
	if (cluster_backup_manifest_validate(manifest) != CLUSTER_BACKUP_MANIFEST_OK)
		return CLUSTER_RESTORE_COMPAT_MANIFEST;
	if (manifest->catversion != current_catversion)
		return CLUSTER_RESTORE_COMPAT_CATVERSION;
	if (manifest->backend_storage_id != current_storage_id)
		return CLUSTER_RESTORE_COMPAT_STORAGE;
	if (manifest->node_count != expected_node_count)
		return CLUSTER_RESTORE_COMPAT_TOPOLOGY;
	return CLUSTER_RESTORE_COMPAT_OK;
}

const char *
cluster_restore_compat_reason_name(ClusterRestoreCompatibilityReason reason)
{
	switch (reason) {
	case CLUSTER_RESTORE_COMPAT_OK:
		return "ok";
	case CLUSTER_RESTORE_COMPAT_CATVERSION:
		return "catversion";
	case CLUSTER_RESTORE_COMPAT_STORAGE:
		return "storage";
	case CLUSTER_RESTORE_COMPAT_TOPOLOGY:
		return "topology";
	case CLUSTER_RESTORE_COMPAT_MANIFEST:
		return "manifest";
	}
	return "unknown";
}

void
cluster_backup_wire_request_compute_crc(ClusterBackupWireRequest *request)
{
	pg_crc32c crc;

	if (request == NULL)
		return;

	request->crc = 0;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, request, offsetof(ClusterBackupWireRequest, crc));
	FIN_CRC32C(crc);
	request->crc = crc;
}

bool
cluster_backup_wire_request_valid(const ClusterBackupWireRequest *request)
{
	ClusterBackupWireRequest copy;

	if (request == NULL)
		return false;
	if (request->magic != CLUSTER_BACKUP_IC_MAGIC || request->version != CLUSTER_BACKUP_IC_VERSION)
		return false;
	if (request->op == CLUSTER_BACKUP_WIRE_OP_NONE
		|| request->op > CLUSTER_BACKUP_WIRE_OP_RESTORE_POINT)
		return false;
	if (request->request_id == 0)
		return false;
	if (request->coordinator_node_id < 0 || request->coordinator_node_id >= CLUSTER_MAX_NODES)
		return false;
	if (request->backup_id[CLUSTER_BACKUP_ID_MAX - 1] != '\0')
		return false;
	if (request->restore_point_name[CLUSTER_RESTORE_POINT_NAME_MAX - 1] != '\0')
		return false;

	copy = *request;
	cluster_backup_wire_request_compute_crc(&copy);
	return copy.crc == request->crc;
}

void
cluster_backup_wire_ack_compute_crc(ClusterBackupWireAck *ack)
{
	pg_crc32c crc;

	if (ack == NULL)
		return;

	ack->crc = 0;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ack, offsetof(ClusterBackupWireAck, crc));
	FIN_CRC32C(crc);
	ack->crc = crc;
}

bool
cluster_backup_wire_ack_valid(const ClusterBackupWireAck *ack)
{
	ClusterBackupWireAck copy;

	if (ack == NULL)
		return false;
	if (ack->magic != CLUSTER_BACKUP_IC_MAGIC || ack->version != CLUSTER_BACKUP_IC_VERSION)
		return false;
	if (ack->op == CLUSTER_BACKUP_WIRE_OP_NONE || ack->op > CLUSTER_BACKUP_WIRE_OP_RESTORE_POINT)
		return false;
	if (ack->result > CLUSTER_BACKUP_WIRE_RESULT_EXECUTOR_ERROR)
		return false;
	if (ack->sender_node_id < 0 || ack->sender_node_id >= CLUSTER_MAX_NODES)
		return false;
	if (ack->request_id == 0)
		return false;
	if (ack->result == CLUSTER_BACKUP_WIRE_RESULT_OK) {
		if (ack->thread_id == 0 || ack->thread_id > CLUSTER_MAX_NODES)
			return false;
		if (ack->op == CLUSTER_BACKUP_WIRE_OP_START
			&& (ack->start_redo_lsn == InvalidXLogRecPtr
				|| ack->checkpoint_lsn == InvalidXLogRecPtr))
			return false;
		if ((ack->op == CLUSTER_BACKUP_WIRE_OP_STOP
			 || ack->op == CLUSTER_BACKUP_WIRE_OP_RESTORE_POINT)
			&& (ack->stop_cut_lsn == InvalidXLogRecPtr || !SCN_VALID(ack->cut_scn)))
			return false;
	}

	copy = *ack;
	cluster_backup_wire_ack_compute_crc(&copy);
	return copy.crc == ack->crc;
}

const char *
cluster_backup_wire_result_name(ClusterBackupWireResult result)
{
	switch (result) {
	case CLUSTER_BACKUP_WIRE_RESULT_OK:
		return "ok";
	case CLUSTER_BACKUP_WIRE_RESULT_BUSY:
		return "busy";
	case CLUSTER_BACKUP_WIRE_RESULT_BAD_REQUEST:
		return "bad_request";
	case CLUSTER_BACKUP_WIRE_RESULT_NOT_IN_BACKUP:
		return "not_in_backup";
	case CLUSTER_BACKUP_WIRE_RESULT_EXECUTOR_ERROR:
		return "executor_error";
	}
	return "unknown";
}
