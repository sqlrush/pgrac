/* Original control-selected file identity; no writer or recovery admission. */
#ifndef CLUSTER_CONTROL_FILE_IDENTITY_H
#define CLUSTER_CONTROL_FILE_IDENTITY_H
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_recovery_file_identity.h"
#include "cluster/cluster_recovery_checkpoint_coverage.h"
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

/* Complete control-selected relmap inventory. A successful read proves an
 * explicit managed ROOTv4 registration, never a fallback from an unknown file.
 * Creation origin comes from that registration, not the current writer.
 * Caller retains the SAME original CF-S/X from read through target I/O and
 * recheck, before acquiring MappingLock. No CF acquisition/reentry occurs here.
 * Fresh *out must be NULL; budget bounds the complete registry, not 64 pins. */
typedef struct ClusterControlRelmapSelectionV2 ClusterControlRelmapSelectionV2;
/* Copied control observation for the checkpoint owner's local receipt across
 * its narrow CF intervals. A later publisher must read a NEW selection and
 * compare every field, then conjoin actual sync/source/cut receipts. The copy
 * retains no CF authority and cannot revive the old selection. */
typedef struct ClusterControlRelmapReferenceV2 {
	ClusterSharedConfigRef config;
	uint8 root_pair_sha256[32];
	uint8 registry_reference[128];
	uint64 logical_count;
	uint64 source_count;
} ClusterControlRelmapReferenceV2;
extern bool cluster_control_root_relmap_selection_reference_v4(ClusterControlRelmapSelectionV2 *,
	ClusterControlRelmapReferenceV2 *out);
extern ClusterControlRootResult
cluster_control_root_relmap_selection_read_locked_v4(const ClusterSharedConfigRef *expected,
													 Size registry_byte_budget,
													 ClusterControlRelmapSelectionV2 **out);
extern uint64 cluster_control_root_relmap_selection_count_v4(ClusterControlRelmapSelectionV2 *);
extern bool cluster_control_root_relmap_selection_at_v4(ClusterControlRelmapSelectionV2 *,
														uint64 index,
														ClusterRecoveryFileIdentityV2 *out);
extern bool cluster_control_root_relmap_selection_lookup_v4(ClusterControlRelmapSelectionV2 *,
															Oid database_oid, Oid tablespace_oid,
															uint64 object_id,
															ClusterRecoveryFileIdentityV2 *out);
extern uint64
cluster_control_root_relmap_selection_source_count_v4(ClusterControlRelmapSelectionV2 *);
extern bool
cluster_control_root_relmap_selection_source_at_v4(ClusterControlRelmapSelectionV2 *, uint64 index,
												   ClusterRecoveryCheckpointSourceV2 *out);
extern ClusterControlRootResult
cluster_control_root_relmap_selection_recheck_locked_v4(ClusterControlRelmapSelectionV2 *);
/* Match the COMPLETE logical relmap receipt key vector, in selection order,
 * then reread original ROOT/registry/common/config under the same CF. Missing,
 * extra, duplicate or changed keys poison this local selection. This matches
 * membership only: D's original synchronizer must prove each actual pair and
 * the checkpoint owner must conjoin ALL file families and original source/cut
 * receipts before the original ROOT publish. No boolean here grants coverage. */
extern ClusterControlRootResult cluster_control_root_relmap_selection_complete_locked_v4(
	ClusterControlRelmapSelectionV2 *, const ClusterRecoveryFileIdentityV2 *completed,
	uint64 count);
/* All returned values are copies. No pointer/view may outlive this local
 * custody. False/ERROR retains original CF/FD/pins/debt; destroy consumes only
 * local decoded storage and its own directory FD, before original owner exit.
 * Loss/reacquisition of CF invalidates the selection; it cannot be revived. */
extern void cluster_control_root_relmap_selection_destroy_v4(ClusterControlRelmapSelectionV2 **);
#endif
