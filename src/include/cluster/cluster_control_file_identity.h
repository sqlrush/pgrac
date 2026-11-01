/* Original control-selected file identity; no writer or recovery admission. */
#ifndef CLUSTER_CONTROL_FILE_IDENTITY_H
#define CLUSTER_CONTROL_FILE_IDENTITY_H
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_recovery_file_identity.h"
/* Independent logical address. Birth/registration/branch come only from ROOT's
 * complete selected registry, never from the target file or an inode. */
typedef struct ClusterControlFileKeyV2 {
	ClusterRecoveryFileKindV2 kind;
	uint16 origin;
	Oid database_oid;
	Oid tablespace_oid;
	uint64 object_id;
} ClusterControlFileKeyV2;
/* Borrow the original clusterwide CF-S/X across selection AND target I/O.
 * expected is the caller's original exact configuration/cohort reference.
 * On success out is by value, not custody or checkpoint/lifetime qualification.
 * Refusal/ERROR preserves output; aliases refuse before I/O. No CF acquire,
 * wait, ROOT publication, fallback, file repair, or protection release.
 * Call again under that same CF after I/O and compare the complete identity. */
extern ClusterControlRootResult cluster_control_root_file_identity_read_locked_v4(
	const ClusterSharedConfigRef *expected, const ClusterControlFileKeyV2 *key,
	Size registry_byte_budget, ClusterRecoveryFileIdentityV2 *out);
#endif
