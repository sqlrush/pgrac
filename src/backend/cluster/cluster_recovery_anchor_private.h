/*-------------------------------------------------------------------------
 * cluster_recovery_anchor_private.h
 *    Backend-private, root-selected thread recovery view.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_ANCHOR_PRIVATE_H
#define CLUSTER_RECOVERY_ANCHOR_PRIVATE_H

#include "cluster/cluster_control_root.h"
#include "cluster/cluster_recovery_anchor.h"

/* PGRAC: decoded carriers, never written as native C structures. No admission
 * or publication authority is conveyed by constructing either carrier.
 */
typedef struct ClusterRecoveryAnchorV2 {
	ClusterControlRootIdentity identity;
	uint64 database_incarnation;
	uint64 config_generation;
	uint64 anchor_generation;
	uint8 claim_sha256[32];
	uint32 state;
	XLogRecPtr checkpoint;
	pg_time_t write_time;
	CheckPoint checkpoint_copy;
	XLogRecPtr unlogged_lsn;
	XLogRecPtr min_recovery_point;
	TimeLineID min_recovery_tli;
	XLogRecPtr backup_start;
	XLogRecPtr backup_end;
	bool backup_end_required;
	/* Historical requirements, not the current shared configuration. */
	bool wal_log_hints;
	bool track_commit_timestamp;
	uint32 wal_level;
	uint32 max_connections;
	uint32 max_worker_processes;
	uint32 max_wal_senders;
	uint32 max_prepared_xacts;
	uint32 max_locks_per_xact;
} ClusterRecoveryAnchorV2;

typedef struct ClusterRecoveryAnchorRefV2 {
	ClusterControlRootIdentity identity;
	uint64 database_incarnation;
	uint64 max_config_generation;
	uint64 anchor_generation;
	uint8 anchor_sha256[32];
	uint8 claim_sha256[32];
} ClusterRecoveryAnchorRefV2;

/* PGRAC: process-owned staged object, not persistent or wire authority.
 * Caller must keep CF-X from successful install through its root CAS.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct ClusterRecoveryAnchorStageV2 {
	ClusterRecoveryAnchorRefV2 ref;
	uint8 operation_uuid[16];
	uint64 object_dir_dev;
	uint64 object_dir_ino;
	uint64 staging_dir_dev;
	uint64 staging_dir_ino;
	uint64 file_dev;
	uint64 file_ino;
	uint32 owner_pid;
	uint32 state;
} ClusterRecoveryAnchorStageV2;

StaticAssertDecl(sizeof(ClusterRecoveryAnchorStageV2) == 240, "anchor staging descriptor size");

/* Exact-size codecs. All outputs clear on refusal; inputs/outputs must not
 * overlap. V1 paths do not call these APIs. Hash selects the whole object,
 * CRC classifies damage; neither is a substitute for the expected identity.
 */
extern ClusterControlRootResult
cluster_recovery_anchor_v2_encode(const ClusterRecoveryAnchorV2 *anchor,
								  uint8 bytes[CLUSTER_RECOVERY_ANCHOR_SIZE]);
extern ClusterControlRootResult
cluster_recovery_anchor_v2_decode(const uint8 *bytes, size_t len,
								  const ClusterRecoveryAnchorRefV2 *ref,
								  ClusterRecoveryAnchorV2 *out);
extern ClusterControlRootResult
cluster_recovery_anchor_v2_project(const uint8 *bytes, size_t len,
								   const ClusterRecoveryAnchorRefV2 *ref,
								   const ControlFileData *common, ControlFileData *out);

/* Compose current native state from the same exact decoded root record, not
 * an older checkpoint's state. In-place view clears on refusal; no admission
 * or recovery completion is implied. Raw historical projection stays separate.
 */
extern ClusterControlRootResult
cluster_recovery_anchor_v2_thread_state(const ClusterRecoveryAnchorRefV2 *ref,
										const ClusterControlRootSnapshot *record,
										ControlFileData *view);

/* Caller already holds clusterwide CF-S/X and has validated storage/root.
 * Read only the exact immutable path. Never create directories or select a
 * compatibility projection. No serving or recovery-completion decision.
 */
extern ClusterControlRootResult
cluster_recovery_anchor_v2_read_locked(const ClusterRecoveryAnchorRefV2 *ref,
									   const ControlFileData *common, ControlFileData *out);

extern ClusterControlRootResult
cluster_recovery_anchor_v2_prepare(const ClusterRecoveryAnchorV2 *anchor,
								   const uint8 operation_uuid[16],
								   ClusterRecoveryAnchorStageV2 *out);
extern ClusterControlRootResult
cluster_recovery_anchor_v2_install(ClusterRecoveryAnchorStageV2 *stage);
extern ClusterControlRootResult
cluster_recovery_anchor_v2_discard(ClusterRecoveryAnchorStageV2 *stage);

#endif /* CLUSTER_RECOVERY_ANCHOR_PRIVATE_H */
