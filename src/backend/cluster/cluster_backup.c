/*-------------------------------------------------------------------------
 *
 * cluster_backup.c
 *	  Cluster-aware backup / restore / PITR SQL surface and shmem state.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_backup.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linked in both build modes because
 *	  pg_proc.dat references the SQL symbols unconditionally.
 *	  Spec: spec-6.5-cluster-aware-backup-restore-pitr.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xlog.h"
#include "access/xlogbackup.h"
#include "catalog/catversion.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

#include "cluster/cluster_backup.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"

PG_FUNCTION_INFO_V1(pg_cluster_backup_start);
PG_FUNCTION_INFO_V1(pg_cluster_backup_stop);
PG_FUNCTION_INFO_V1(pg_cluster_create_restore_point);
PG_FUNCTION_INFO_V1(cluster_get_backup_state);
PG_FUNCTION_INFO_V1(cluster_get_backup_history);
PG_FUNCTION_INFO_V1(cluster_get_restore_points);
PG_FUNCTION_INFO_V1(cluster_get_pitr_status);

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_wal_thread.h"

typedef struct ClusterBackupSharedState {
	LWLockPadded lock;
	ClusterBackupStatus status;
	bool have_manifest;
	ClusterBackupManifest last_manifest;
	int restore_point_count;
	int restore_point_next;
	ClusterRestorePoint restore_points[CLUSTER_BACKUP_RESTORE_POINT_MAX];
} ClusterBackupSharedState;

static ClusterBackupSharedState *cluster_backup_state = NULL;
static BackupState *cluster_backup_session_state = NULL;
static StringInfo cluster_backup_tablespace_map = NULL;
static MemoryContext cluster_backup_context = NULL;

static const char *
cluster_pitr_action_name(int action)
{
	switch (action) {
	case CLUSTER_RECOVERY_TARGET_ACTION_PAUSE:
		return "pause";
	case CLUSTER_RECOVERY_TARGET_ACTION_PROMOTE:
		return "promote";
	case CLUSTER_RECOVERY_TARGET_ACTION_SHUTDOWN:
		return "shutdown";
	}
	return "unknown";
}

Size
cluster_backup_shmem_size(void)
{
	return sizeof(ClusterBackupSharedState);
}

void
cluster_backup_shmem_init(void)
{
	bool found;

	cluster_backup_state
		= ShmemInitStruct("pgrac cluster backup", cluster_backup_shmem_size(), &found);
	if (!found) {
		MemSet(cluster_backup_state, 0, sizeof(*cluster_backup_state));
		LWLockInitialize(&cluster_backup_state->lock.lock, LWTRANCHE_CLUSTER_BACKUP);
	}
}

static const ClusterShmemRegion cluster_backup_region = {
	.name = "pgrac cluster backup",
	.size_fn = cluster_backup_shmem_size,
	.init_fn = cluster_backup_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_backup",
	.reserved_flags = 0,
};

void
cluster_backup_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_backup_region);
}

static void
cluster_backup_error_if_unavailable(const char *op)
{
	if (!cluster_enabled)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("%s requires cluster.enabled", op)));
	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("%s requires a valid cluster.node_id", op)));
	if (cluster_backup_state == NULL)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cluster backup shared state is not initialized")));
	if (cluster_conf_has_peers())
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("%s is not available for multi-node clusters yet", op),
						errhint("Use a single-node cluster topology, or wait for the 6.5 "
								"coordinator/backup-set writer to land.")));
}

static SCN
cluster_backup_current_scn(void)
{
	SCN scn;

	scn = cluster_scn_current();
	if (!SCN_VALID(scn))
		scn = cluster_scn_advance();
	return scn;
}

static void
cluster_backup_update_start(const char *backup_id, const BackupState *state)
{
	ClusterBackupStatus status;

	MemSet(&status, 0, sizeof(status));
	status.in_progress = true;
	strlcpy(status.backup_id, backup_id, sizeof(status.backup_id));
	status.coordinator_node_id = cluster_node_id;
	status.start_redo_lsn = state->startpoint;
	status.checkpoint_lsn = state->checkpointloc;
	status.start_tli = state->starttli;
	status.started_at = GetCurrentTimestamp();

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	cluster_backup_state->status = status;
	LWLockRelease(&cluster_backup_state->lock.lock);
}

static void
cluster_backup_update_stop(const BackupState *state, const ClusterBackupManifest *manifest,
						   SCN cut_scn)
{
	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	cluster_backup_state->status.in_progress = false;
	cluster_backup_state->status.stop_cut_lsn = state->stoppoint;
	cluster_backup_state->status.consistent_scn = cut_scn;
	cluster_backup_state->status.manifest_crc = manifest->manifest_crc;
	cluster_backup_state->status.stopped_at = GetCurrentTimestamp();
	cluster_backup_state->last_manifest = *manifest;
	cluster_backup_state->have_manifest = true;
	LWLockRelease(&cluster_backup_state->lock.lock);
}

static void
cluster_backup_mark_native_stopped(const BackupState *state)
{
	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	cluster_backup_state->status.in_progress = false;
	if (state != NULL)
		cluster_backup_state->status.stop_cut_lsn = state->stoppoint;
	cluster_backup_state->status.stopped_at = GetCurrentTimestamp();
	LWLockRelease(&cluster_backup_state->lock.lock);
}

static void
cluster_backup_add_restore_point(const ClusterRestorePoint *point)
{
	int slot;

	if (point == NULL || !point->present)
		return;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_EXCLUSIVE);
	slot = cluster_backup_state->restore_point_next;
	cluster_backup_state->restore_points[slot] = *point;
	cluster_backup_state->restore_point_next
		= (cluster_backup_state->restore_point_next + 1) % CLUSTER_BACKUP_RESTORE_POINT_MAX;
	if (cluster_backup_state->restore_point_count < CLUSTER_BACKUP_RESTORE_POINT_MAX)
		cluster_backup_state->restore_point_count++;
	LWLockRelease(&cluster_backup_state->lock.lock);
}

void
cluster_backup_get_status(ClusterBackupStatus *out)
{
	if (out == NULL)
		return;
	MemSet(out, 0, sizeof(*out));
	if (cluster_backup_state == NULL)
		return;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_SHARED);
	*out = cluster_backup_state->status;
	LWLockRelease(&cluster_backup_state->lock.lock);
}

bool
cluster_backup_get_last_manifest(ClusterBackupManifest *out)
{
	bool have_manifest;

	if (out == NULL)
		return false;
	MemSet(out, 0, sizeof(*out));
	if (cluster_backup_state == NULL)
		return false;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_SHARED);
	have_manifest = cluster_backup_state->have_manifest;
	if (have_manifest)
		*out = cluster_backup_state->last_manifest;
	LWLockRelease(&cluster_backup_state->lock.lock);
	return have_manifest;
}

int
cluster_backup_get_restore_points(ClusterRestorePoint *out, int max_points)
{
	int count;
	int start;
	int i;

	if (out == NULL || max_points <= 0 || cluster_backup_state == NULL)
		return 0;

	LWLockAcquire(&cluster_backup_state->lock.lock, LW_SHARED);
	count = Min(cluster_backup_state->restore_point_count, max_points);
	start = cluster_backup_state->restore_point_next - cluster_backup_state->restore_point_count;
	if (start < 0)
		start += CLUSTER_BACKUP_RESTORE_POINT_MAX;
	for (i = 0; i < count; i++) {
		int slot = (start + i) % CLUSTER_BACKUP_RESTORE_POINT_MAX;

		out[i] = cluster_backup_state->restore_points[slot];
	}
	LWLockRelease(&cluster_backup_state->lock.lock);
	return count;
}

static void
cluster_backup_fill_local_manifest(ClusterBackupManifest *manifest, const BackupState *state,
								   SCN cut_scn)
{
	ClusterBackupManifestThread thread;
	uint16 thread_id = cluster_wal_thread_id();
	int thread_index;

	if (thread_id == XLP_THREAD_ID_LEGACY)
		thread_id = 1;
	thread_index = (int)thread_id - 1;

	cluster_backup_manifest_init(manifest, state->name);
	manifest->consistent_scn = cut_scn;
	manifest->scn_durable_peak = cut_scn;
	manifest->timeline = state->stoptli;
	manifest->catversion = CATALOG_VERSION_NO;
	manifest->incarnation = 0;
	manifest->backend_storage_id = (uint32)cluster_shared_storage_backend;
	manifest->node_count = 1;
	manifest->control_included = true;
	manifest->voting_included = false;

	MemSet(&thread, 0, sizeof(thread));
	thread.present = true;
	thread.wal_included = true;
	thread.undo_included = true;
	thread.tt_included = true;
	thread.thread_id = thread_id;
	thread.node_id = cluster_node_id;
	thread.start_redo_lsn = state->startpoint;
	thread.checkpoint_lsn = state->checkpointloc;
	thread.start_tli = state->starttli;
	thread.stop_cut_lsn = state->stoppoint;

	if (!cluster_backup_manifest_set_thread(manifest, thread_index, &thread))
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("could not add local WAL thread to cluster backup manifest")));

	if (cluster_backup_manifest_checksums != CLUSTER_BACKUP_MANIFEST_CHECKSUM_CRC32C)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("cluster backup manifests require crc32c checksums")));
	cluster_backup_manifest_seal(manifest);
	if (cluster_backup_manifest_validate(manifest) != CLUSTER_BACKUP_MANIFEST_OK)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_INCOMPLETE),
						errmsg("cluster backup manifest failed self-validation")));
}

static char *
cluster_backup_build_label(const BackupState *state, const ClusterBackupManifest *manifest,
						   SCN cut_scn)
{
	StringInfoData buf;
	char *native;

	native = build_backup_content((BackupState *)state, false);
	initStringInfo(&buf);
	appendStringInfoString(&buf, native);
	appendStringInfo(&buf, "CLUSTER_BACKUP_ID: %s\n", manifest->backup_id);
	appendStringInfo(&buf, "CLUSTER_CONSISTENT_SCN: " UINT64_FORMAT "\n", (uint64)cut_scn);
	appendStringInfo(&buf, "CLUSTER_MANIFEST_CRC32C: %u\n", manifest->manifest_crc);
	appendStringInfo(&buf, "CLUSTER_NODE_COUNT: %u\n", manifest->node_count);
	appendStringInfo(&buf, "CLUSTER_THREAD_COUNT: %u\n", manifest->thread_count);
	pfree(native);
	return buf.data;
}

Datum
pg_cluster_backup_start(PG_FUNCTION_ARGS)
{
#define PG_CLUSTER_BACKUP_START_COLS 4
	TupleDesc tupdesc;
	Datum values[PG_CLUSTER_BACKUP_START_COLS] = { 0 };
	bool nulls[PG_CLUSTER_BACKUP_START_COLS] = { 0 };
	text *backupid = PG_GETARG_TEXT_PP(0);
	bool fast = PG_GETARG_BOOL(1);
	char *backupidstr;
	SessionBackupState status;
	MemoryContext oldcontext;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to start a cluster backup")));
	cluster_backup_error_if_unavailable("pg_cluster_backup_start");

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	backupidstr = text_to_cstring(backupid);
	if (strlen(backupidstr) >= CLUSTER_BACKUP_ID_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cluster backup id is too long"),
				 errdetail("Maximum length is %d bytes.", CLUSTER_BACKUP_ID_MAX - 1)));

	status = get_backup_status();
	if (status == SESSION_BACKUP_RUNNING)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BACKUP_IN_PROGRESS),
						errmsg("a backup is already in progress in this session")));

	if (cluster_backup_context == NULL)
		cluster_backup_context = AllocSetContextCreate(TopMemoryContext, "cluster backup context",
													   ALLOCSET_START_SMALL_SIZES);
	else {
		cluster_backup_session_state = NULL;
		cluster_backup_tablespace_map = NULL;
		MemoryContextReset(cluster_backup_context);
	}

	oldcontext = MemoryContextSwitchTo(cluster_backup_context);
	cluster_backup_session_state = (BackupState *)palloc0(sizeof(BackupState));
	cluster_backup_tablespace_map = makeStringInfo();
	MemoryContextSwitchTo(oldcontext);

	register_persistent_abort_backup_handler();
	do_pg_backup_start(backupidstr, fast, NULL, cluster_backup_session_state,
					   cluster_backup_tablespace_map);
	cluster_backup_update_start(backupidstr, cluster_backup_session_state);

	values[0] = CStringGetTextDatum(backupidstr);
	values[1] = LSNGetDatum(cluster_backup_session_state->startpoint);
	values[2] = LSNGetDatum(cluster_backup_session_state->checkpointloc);
	values[3] = Int32GetDatum((int32)cluster_backup_session_state->starttli);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
pg_cluster_backup_stop(PG_FUNCTION_ARGS)
{
#define PG_CLUSTER_BACKUP_STOP_COLS 4
	TupleDesc tupdesc;
	Datum values[PG_CLUSTER_BACKUP_STOP_COLS] = { 0 };
	bool nulls[PG_CLUSTER_BACKUP_STOP_COLS] = { 0 };
	bool waitforarchive = PG_GETARG_BOOL(0);
	ClusterBackupManifest manifest;
	ClusterRestorePoint point;
	SCN cut_scn;
	char *backup_label;
	XLogRecPtr thread_lsn[CLUSTER_MAX_NODES];
	SCN thread_scn[CLUSTER_MAX_NODES];
	int thread_index;
	uint16 thread_id;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to stop a cluster backup")));
	cluster_backup_error_if_unavailable("pg_cluster_backup_stop");

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	if (get_backup_status() != SESSION_BACKUP_RUNNING)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cluster backup is not in progress"),
						errhint("Did you call pg_cluster_backup_start()?")));
	if (cluster_backup_session_state == NULL || cluster_backup_tablespace_map == NULL)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cluster backup session state is missing")));

	do_pg_backup_stop(cluster_backup_session_state, waitforarchive);
	cluster_backup_mark_native_stopped(cluster_backup_session_state);
	cut_scn = cluster_backup_current_scn();
	cluster_backup_fill_local_manifest(&manifest, cluster_backup_session_state, cut_scn);
	backup_label = cluster_backup_build_label(cluster_backup_session_state, &manifest, cut_scn);
	cluster_backup_update_stop(cluster_backup_session_state, &manifest, cut_scn);

	MemSet(thread_lsn, 0, sizeof(thread_lsn));
	MemSet(thread_scn, 0, sizeof(thread_scn));
	thread_id = cluster_wal_thread_id();
	if (thread_id == XLP_THREAD_ID_LEGACY)
		thread_id = 1;
	thread_index = (int)thread_id - 1;
	thread_lsn[thread_index] = cluster_backup_session_state->stoppoint;
	thread_scn[thread_index] = cut_scn;
	if (cluster_restore_point_build(&point, manifest.backup_id, thread_scn, thread_lsn,
									CLUSTER_MAX_NODES, true, true, manifest.incarnation)
		== CLUSTER_RESTORE_POINT_CUT_OK) {
		point.created_at = GetCurrentTimestamp();
		cluster_backup_add_restore_point(&point);
	}

	values[0] = Int64GetDatum((int64)cut_scn);
	values[1] = LSNGetDatum(cluster_backup_session_state->stoppoint);
	values[2] = Int64GetDatum((int64)manifest.manifest_crc);
	values[3] = CStringGetTextDatum(backup_label);

	pfree(backup_label);
	cluster_backup_session_state = NULL;
	cluster_backup_tablespace_map = NULL;
	MemoryContextDelete(cluster_backup_context);
	cluster_backup_context = NULL;

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
pg_cluster_create_restore_point(PG_FUNCTION_ARGS)
{
#define PG_CLUSTER_RESTORE_POINT_COLS 3
	TupleDesc tupdesc;
	Datum values[PG_CLUSTER_RESTORE_POINT_COLS] = { 0 };
	bool nulls[PG_CLUSTER_RESTORE_POINT_COLS] = { 0 };
	text *restore_name = PG_GETARG_TEXT_PP(0);
	char *restore_name_str;
	XLogRecPtr restorepoint;
	SCN cut_scn;
	SCN thread_scn[CLUSTER_MAX_NODES];
	XLogRecPtr thread_lsn[CLUSTER_MAX_NODES];
	uint16 thread_id;
	int thread_index;
	ClusterRestorePoint point;
	ClusterRestorePointCutReason reason;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to create a cluster restore point")));
	cluster_backup_error_if_unavailable("pg_cluster_create_restore_point");
	if (RecoveryInProgress())
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("recovery is in progress"),
						errhint("WAL control functions cannot be executed during recovery.")));
	if (!XLogIsNeeded())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL level not sufficient for creating a restore point"),
				 errhint("wal_level must be set to \"replica\" or \"logical\" at server start.")));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	restore_name_str = text_to_cstring(restore_name);
	if (strlen(restore_name_str) >= CLUSTER_RESTORE_POINT_NAME_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster restore point name is too long"),
				 errdetail("Maximum length is %d bytes.", CLUSTER_RESTORE_POINT_NAME_MAX - 1)));

	restorepoint = XLogRestorePoint(restore_name_str);
	cut_scn = cluster_backup_current_scn();

	MemSet(thread_scn, 0, sizeof(thread_scn));
	MemSet(thread_lsn, 0, sizeof(thread_lsn));
	thread_id = cluster_wal_thread_id();
	if (thread_id == XLP_THREAD_ID_LEGACY)
		thread_id = 1;
	thread_index = (int)thread_id - 1;
	thread_scn[thread_index] = cut_scn;
	thread_lsn[thread_index] = restorepoint;
	reason = cluster_restore_point_build(&point, restore_name_str, thread_scn, thread_lsn,
										 CLUSTER_MAX_NODES, true, true, 0);
	if (reason != CLUSTER_RESTORE_POINT_CUT_OK)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_RESTORE_POINT_DRAIN_TIMEOUT),
						errmsg("could not build cluster restore point cut: %s",
							   cluster_restore_point_cut_reason_name(reason))));
	point.created_at = GetCurrentTimestamp();
	cluster_backup_add_restore_point(&point);

	values[0] = CStringGetTextDatum(restore_name_str);
	values[1] = Int64GetDatum((int64)cut_scn);
	values[2] = LSNGetDatum(restorepoint);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
cluster_get_backup_state(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ClusterBackupStatus status;
	Datum values[14];
	bool nulls[14];

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	if (!cluster_enabled)
		return (Datum)0;

	cluster_backup_get_status(&status);
	MemSet(nulls, false, sizeof(nulls));
	values[0] = BoolGetDatum(status.in_progress);
	if (status.backup_id[0] == '\0')
		nulls[1] = true;
	else
		values[1] = CStringGetTextDatum(status.backup_id);
	values[2] = Int32GetDatum(status.coordinator_node_id);
	values[3] = LSNGetDatum(status.start_redo_lsn);
	values[4] = LSNGetDatum(status.checkpoint_lsn);
	values[5] = LSNGetDatum(status.stop_cut_lsn);
	values[6] = Int64GetDatum((int64)status.consistent_scn);
	values[7] = Int64GetDatum((int64)status.manifest_crc);
	if (status.started_at == 0)
		nulls[8] = true;
	else
		values[8] = TimestampTzGetDatum(status.started_at);
	if (status.stopped_at == 0)
		nulls[9] = true;
	else
		values[9] = TimestampTzGetDatum(status.stopped_at);
	values[10] = Int32GetDatum(cluster_backup_parallel_channels);
	values[11] = Int32GetDatum(cluster_backup_wal_retention);
	values[12] = BoolGetDatum(cluster_enable_pitr_restore_points);
	values[13] = Int32GetDatum(cluster_pitr_restore_point_interval_ms);

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum)0;
}

Datum
cluster_get_backup_history(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ClusterBackupManifest manifest;
	Datum values[9];
	bool nulls[9];

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	if (!cluster_enabled || !cluster_backup_get_last_manifest(&manifest))
		return (Datum)0;

	MemSet(nulls, false, sizeof(nulls));
	values[0] = CStringGetTextDatum(manifest.backup_id);
	values[1] = Int64GetDatum((int64)manifest.consistent_scn);
	values[2] = Int64GetDatum((int64)manifest.scn_durable_peak);
	values[3] = Int32GetDatum((int32)manifest.timeline);
	values[4] = Int64GetDatum((int64)manifest.catversion);
	values[5] = Int32GetDatum((int32)manifest.backend_storage_id);
	values[6] = Int32GetDatum((int32)manifest.node_count);
	values[7] = Int32GetDatum((int32)manifest.thread_count);
	values[8] = Int64GetDatum((int64)manifest.manifest_crc);

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum)0;
}

Datum
cluster_get_restore_points(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ClusterRestorePoint points[CLUSTER_BACKUP_RESTORE_POINT_MAX];
	int count;
	int i;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	if (!cluster_enabled)
		return (Datum)0;

	count = cluster_backup_get_restore_points(points, CLUSTER_BACKUP_RESTORE_POINT_MAX);
	for (i = 0; i < count; i++) {
		Datum values[5];
		bool nulls[5] = { false };

		values[0] = CStringGetTextDatum(points[i].name);
		values[1] = Int64GetDatum((int64)points[i].cut_scn);
		values[2] = Int32GetDatum((int32)points[i].thread_count);
		values[3] = Int32GetDatum((int32)points[i].incarnation);
		if (points[i].created_at == 0)
			nulls[4] = true;
		else
			values[4] = TimestampTzGetDatum(points[i].created_at);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	return (Datum)0;
}

Datum
cluster_get_pitr_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ClusterBackupManifest manifest;
	ClusterRestorePoint points[CLUSTER_BACKUP_RESTORE_POINT_MAX];
	ClusterRestorePoint chosen;
	ClusterPitrTargetReason reason;
	Datum values[6];
	bool nulls[6] = { false };
	int count;
	int i;
	const char *target_action = cluster_pitr_action_name(cluster_recovery_target_action);
	bool have_requested_scn = false;
	bool invalid_requested_scn = false;
	SCN requested_scn = InvalidScn;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	if (!cluster_enabled)
		return (Datum)0;

	if ((cluster_recovery_target_scn == NULL || cluster_recovery_target_scn[0] == '\0')
		&& (cluster_recovery_target_name == NULL || cluster_recovery_target_name[0] == '\0')
		&& cluster_recovery_target_cluster_time != NULL
		&& cluster_recovery_target_cluster_time[0] != '\0') {
		values[0] = CStringGetTextDatum("cluster_time");
		values[1] = CStringGetTextDatum(target_action);
		values[2] = BoolGetDatum(false);
		values[3] = CStringGetTextDatum("unsupported_target_type");
		nulls[4] = true;
		nulls[5] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		return (Datum)0;
	}

	if (cluster_recovery_target_scn != NULL && cluster_recovery_target_scn[0] != '\0') {
		int64 parsed = pg_strtoint64(cluster_recovery_target_scn);

		have_requested_scn = true;
		if (parsed > 0)
			requested_scn = (SCN)parsed;
		else
			invalid_requested_scn = true;
	}

	if (invalid_requested_scn) {
		values[0] = CStringGetTextDatum("scn");
		values[1] = CStringGetTextDatum(target_action);
		values[2] = BoolGetDatum(false);
		values[3] = CStringGetTextDatum("invalid_target");
		nulls[4] = true;
		nulls[5] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		return (Datum)0;
	}

	if (!have_requested_scn && cluster_recovery_target_name != NULL
		&& cluster_recovery_target_name[0] != '\0') {
		if (!cluster_backup_get_last_manifest(&manifest)) {
			values[0] = CStringGetTextDatum("name");
			values[1] = CStringGetTextDatum(target_action);
			values[2] = BoolGetDatum(false);
			values[3] = CStringGetTextDatum("manifest");
			nulls[4] = true;
			nulls[5] = true;
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
			return (Datum)0;
		}

		count = cluster_backup_get_restore_points(points, CLUSTER_BACKUP_RESTORE_POINT_MAX);
		for (i = 0; i < count; i++) {
			if (!points[i].present)
				continue;
			if (strcmp(points[i].name, cluster_recovery_target_name) == 0) {
				if (scn_time_cmp(points[i].cut_scn, manifest.consistent_scn) < 0) {
					values[0] = CStringGetTextDatum("name");
					values[1] = CStringGetTextDatum(target_action);
					values[2] = BoolGetDatum(false);
					values[3] = CStringGetTextDatum("before_backup");
					nulls[4] = true;
					nulls[5] = true;
					tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
					return (Datum)0;
				}
				if (points[i].thread_count == 0 || points[i].thread_count > CLUSTER_MAX_NODES) {
					values[0] = CStringGetTextDatum("name");
					values[1] = CStringGetTextDatum(target_action);
					values[2] = BoolGetDatum(false);
					values[3] = CStringGetTextDatum("missing_thread");
					nulls[4] = true;
					nulls[5] = true;
					tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
					return (Datum)0;
				}

				values[0] = CStringGetTextDatum("name");
				values[1] = CStringGetTextDatum(target_action);
				values[2] = BoolGetDatum(true);
				values[3] = CStringGetTextDatum("ok");
				values[4] = Int64GetDatum((int64)points[i].cut_scn);
				values[5] = CStringGetTextDatum(points[i].name);
				tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
				return (Datum)0;
			}
		}
		values[0] = CStringGetTextDatum("name");
		values[1] = CStringGetTextDatum(target_action);
		values[2] = BoolGetDatum(false);
		values[3] = CStringGetTextDatum("no_restore_point");
		nulls[4] = true;
		nulls[5] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		return (Datum)0;
	}

	if (!SCN_VALID(requested_scn)) {
		values[0] = CStringGetTextDatum("latest");
		values[1] = CStringGetTextDatum(target_action);
		values[2] = BoolGetDatum(true);
		values[3] = CStringGetTextDatum("ok");
		nulls[4] = true;
		nulls[5] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		return (Datum)0;
	}

	if (!cluster_backup_get_last_manifest(&manifest)) {
		values[0] = CStringGetTextDatum("scn");
		values[1] = CStringGetTextDatum(target_action);
		values[2] = BoolGetDatum(false);
		values[3] = CStringGetTextDatum("manifest");
		nulls[4] = true;
		nulls[5] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		return (Datum)0;
	}

	count = cluster_backup_get_restore_points(points, CLUSTER_BACKUP_RESTORE_POINT_MAX);
	reason
		= cluster_pitr_resolve_scn(points, count, requested_scn, manifest.consistent_scn, &chosen);
	values[0] = CStringGetTextDatum("scn");
	values[1] = CStringGetTextDatum(target_action);
	values[2] = BoolGetDatum(reason == CLUSTER_PITR_TARGET_OK);
	values[3] = CStringGetTextDatum(cluster_pitr_target_reason_name(reason));
	if (reason == CLUSTER_PITR_TARGET_OK) {
		values[4] = Int64GetDatum((int64)chosen.cut_scn);
		values[5] = CStringGetTextDatum(chosen.name);
	} else {
		nulls[4] = true;
		nulls[5] = true;
	}
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	return (Datum)0;
}

#else /* !USE_PGRAC_CLUSTER */

Size
cluster_backup_shmem_size(void)
{
	return 0;
}

void
cluster_backup_shmem_init(void)
{}

void
cluster_backup_shmem_register(void)
{}

void
cluster_backup_get_status(ClusterBackupStatus *out)
{
	if (out != NULL)
		MemSet(out, 0, sizeof(*out));
}

bool
cluster_backup_get_last_manifest(ClusterBackupManifest *out)
{
	if (out != NULL)
		MemSet(out, 0, sizeof(*out));
	return false;
}

int
cluster_backup_get_restore_points(ClusterRestorePoint *out, int max_points)
{
	return 0;
}

Datum
pg_cluster_backup_start(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_backup_start requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
pg_cluster_backup_stop(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_backup_stop requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
pg_cluster_create_restore_point(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_create_restore_point requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
cluster_get_backup_state(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_get_backup_state requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
cluster_get_backup_history(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_get_backup_history requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
cluster_get_restore_points(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_get_restore_points requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
cluster_get_pitr_status(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_get_pitr_status requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
