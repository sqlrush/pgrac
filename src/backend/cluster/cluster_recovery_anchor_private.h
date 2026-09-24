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
} ClusterRecoveryAnchorV2;

typedef struct ClusterRecoveryAnchorRefV2 {
	ClusterControlRootIdentity identity;
	uint64 database_incarnation;
	uint64 max_config_generation;
	uint64 anchor_generation;
	uint8 anchor_sha256[32];
	uint8 claim_sha256[32];
} ClusterRecoveryAnchorRefV2;

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

/* Caller already holds clusterwide CF-S/X and has validated storage/root.
 * Read only the exact immutable path. Never create directories or select a
 * compatibility projection. No serving or recovery-completion decision.
 */
extern ClusterControlRootResult
cluster_recovery_anchor_v2_read_locked(const ClusterRecoveryAnchorRefV2 *ref,
									   const ControlFileData *common, ControlFileData *out);

#endif /* CLUSTER_RECOVERY_ANCHOR_PRIVATE_H */
