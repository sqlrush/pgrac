/*-------------------------------------------------------------------------
 *
 * cluster_backup.h
 *	  Cluster-aware backup / restore / PITR substrate.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_backup.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-6.5-cluster-aware-backup-restore-pitr.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_BACKUP_H
#define CLUSTER_BACKUP_H

#include "access/xlogdefs.h"
#include "c.h"
#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES */
#include "cluster/cluster_scn.h"
#include "datatype/timestamp.h"

#define CLUSTER_BACKUP_ID_MAX 64
#define CLUSTER_RESTORE_POINT_NAME_MAX 64
#define CLUSTER_BACKUP_MANIFEST_MAGIC 0x5047424BU /* "PGBK" */
#define CLUSTER_BACKUP_MANIFEST_VERSION 1
#define CLUSTER_BACKUP_RESTORE_POINT_MAX 16

typedef enum ClusterBackupManifestReason {
	CLUSTER_BACKUP_MANIFEST_OK = 0,
	CLUSTER_BACKUP_MANIFEST_NULL,
	CLUSTER_BACKUP_MANIFEST_BAD_MAGIC,
	CLUSTER_BACKUP_MANIFEST_BAD_VERSION,
	CLUSTER_BACKUP_MANIFEST_BAD_COUNTS,
	CLUSTER_BACKUP_MANIFEST_MISSING_THREAD,
	CLUSTER_BACKUP_MANIFEST_BAD_LSN_RANGE,
	CLUSTER_BACKUP_MANIFEST_MISSING_WAL,
	CLUSTER_BACKUP_MANIFEST_MISSING_UNDO,
	CLUSTER_BACKUP_MANIFEST_MISSING_TT,
	CLUSTER_BACKUP_MANIFEST_MISSING_CONTROL,
	CLUSTER_BACKUP_MANIFEST_BAD_SCN_PEAK,
	CLUSTER_BACKUP_MANIFEST_BAD_CRC
} ClusterBackupManifestReason;

typedef enum ClusterPitrTargetReason {
	CLUSTER_PITR_TARGET_OK = 0,
	CLUSTER_PITR_TARGET_NO_RESTORE_POINT,
	CLUSTER_PITR_TARGET_BEFORE_BACKUP,
	CLUSTER_PITR_TARGET_MISSING_THREAD,
	CLUSTER_PITR_TARGET_UNARCHIVED_WAL
} ClusterPitrTargetReason;

typedef enum ClusterRestoreCompatibilityReason {
	CLUSTER_RESTORE_COMPAT_OK = 0,
	CLUSTER_RESTORE_COMPAT_CATVERSION,
	CLUSTER_RESTORE_COMPAT_STORAGE,
	CLUSTER_RESTORE_COMPAT_TOPOLOGY,
	CLUSTER_RESTORE_COMPAT_MANIFEST
} ClusterRestoreCompatibilityReason;

typedef enum ClusterRestorePointCutReason {
	CLUSTER_RESTORE_POINT_CUT_OK = 0,
	CLUSTER_RESTORE_POINT_CUT_PENDING_COMMITS,
	CLUSTER_RESTORE_POINT_CUT_NO_FENCE,
	CLUSTER_RESTORE_POINT_CUT_NO_THREADS,
	CLUSTER_RESTORE_POINT_CUT_BAD_THREAD
} ClusterRestorePointCutReason;

typedef struct ClusterBackupManifestThread {
	bool present;
	bool wal_included;
	bool undo_included;
	bool tt_included;
	uint32 thread_id;
	int32 node_id;
	XLogRecPtr start_redo_lsn;
	XLogRecPtr checkpoint_lsn;
	TimeLineID start_tli;
	XLogRecPtr stop_cut_lsn;
} ClusterBackupManifestThread;

typedef struct ClusterBackupManifest {
	uint32 magic;
	uint32 version;
	char backup_id[CLUSTER_BACKUP_ID_MAX];
	SCN consistent_scn;
	SCN scn_durable_peak;
	TimeLineID timeline;
	uint32 catversion;
	uint32 incarnation;
	uint32 backend_storage_id;
	uint32 node_count;
	uint32 thread_count;
	bool control_included;
	bool voting_included;
	ClusterBackupManifestThread threads[CLUSTER_MAX_NODES];
	uint32 manifest_crc;
} ClusterBackupManifest;

typedef struct ClusterRestorePoint {
	bool present;
	char name[CLUSTER_RESTORE_POINT_NAME_MAX];
	SCN cut_scn;
	XLogRecPtr cut_lsn[CLUSTER_MAX_NODES];
	uint32 thread_count;
	uint32 incarnation;
	TimestampTz created_at;
} ClusterRestorePoint;

typedef struct ClusterBackupStatus {
	bool in_progress;
	char backup_id[CLUSTER_BACKUP_ID_MAX];
	int32 coordinator_node_id;
	XLogRecPtr start_redo_lsn;
	XLogRecPtr checkpoint_lsn;
	TimeLineID start_tli;
	XLogRecPtr stop_cut_lsn;
	SCN consistent_scn;
	uint32 manifest_crc;
	TimestampTz started_at;
	TimestampTz stopped_at;
} ClusterBackupStatus;

extern void cluster_backup_manifest_init(ClusterBackupManifest *manifest, const char *backup_id);
extern bool cluster_backup_manifest_set_thread(ClusterBackupManifest *manifest, int thread_index,
											   const ClusterBackupManifestThread *thread);
extern uint32 cluster_backup_manifest_compute_crc(const ClusterBackupManifest *manifest);
extern void cluster_backup_manifest_seal(ClusterBackupManifest *manifest);
extern ClusterBackupManifestReason
cluster_backup_manifest_validate(const ClusterBackupManifest *manifest);
extern const char *cluster_backup_manifest_reason_name(ClusterBackupManifestReason reason);

extern ClusterRestorePointCutReason
cluster_restore_point_build(ClusterRestorePoint *out, const char *name, const SCN *thread_scn,
							const XLogRecPtr *thread_lsn, int max_threads,
							bool pending_commits_empty, bool commit_fence_held, uint32 incarnation);
extern const char *cluster_restore_point_cut_reason_name(ClusterRestorePointCutReason reason);

extern ClusterPitrTargetReason cluster_pitr_resolve_scn(const ClusterRestorePoint *points,
														int npoints, SCN requested_scn,
														SCN backup_consistent_scn,
														ClusterRestorePoint *out);
extern const char *cluster_pitr_target_reason_name(ClusterPitrTargetReason reason);

extern ClusterRestoreCompatibilityReason
cluster_backup_manifest_compatible(const ClusterBackupManifest *manifest, uint32 current_catversion,
								   uint32 current_storage_id, uint32 expected_node_count);
extern const char *cluster_restore_compat_reason_name(ClusterRestoreCompatibilityReason reason);

#ifndef FRONTEND
extern Size cluster_backup_shmem_size(void);
extern void cluster_backup_shmem_init(void);
extern void cluster_backup_shmem_register(void);
extern void cluster_backup_get_status(ClusterBackupStatus *out);
extern bool cluster_backup_get_last_manifest(ClusterBackupManifest *out);
extern int cluster_backup_get_restore_points(ClusterRestorePoint *out, int max_points);
#endif

#endif /* CLUSTER_BACKUP_H */
