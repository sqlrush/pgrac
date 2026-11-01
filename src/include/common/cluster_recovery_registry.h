/* Complete original-owner registration. Codecs and I/O confer no authority. */
#ifndef CLUSTER_RECOVERY_REGISTRY_H
#define CLUSTER_RECOVERY_REGISTRY_H
#include "cluster/cluster_recovery_checkpoint_coverage.h"
#include "cluster/cluster_recovery_format.h"

typedef struct ClusterRecoveryRegistryV2
{
	uint64 system_identifier;
	uint64 database_incarnation;
	uint8 storage_uuid[16];
	uint8 redo_branch_uuid[16];
	uint64 redo_branch_generation;
	ClusterRecoveryCheckpointCoverageV2 coverage;
} ClusterRecoveryRegistryV2;

/* Exact full vectors in original order, not self-authenticating file metadata.
 * No allocation escapes; false preserves all outputs. byte_budget bounds the
 * complete object, not an active-pin count. Digests are computed from the
 * canonical vectors; custody and caller digest fields are never serialized. */
extern bool cluster_recovery_registry_size_v2(const ClusterRecoveryRegistryV2 *, Size byte_budget,
											  Size *length);
extern bool cluster_recovery_registry_encode_v2(const ClusterRecoveryRegistryV2 *, uint8 *bytes,
												Size length, uint8 reference[128]);
extern bool cluster_recovery_registry_matches_v2(const uint8 *bytes, Size length,
												 const ClusterRecoveryRegistryV2 *,
												 const uint8 reference[128]);
extern bool cluster_recovery_registry_reference_valid_v2(const uint8 reference[128]);

/* Borrow global directory under the original creator/control owner's custody.
 * Immutable create never replaces/unlinks material, including on uncertainty.
 * No latest scan, activation, ROOT publication, or original protection release.
 * Caller holds/revalidates the complete original cohort across these calls.
 * All local descriptors close on every return; output changes only on success. */
extern bool cluster_recovery_registry_create_v2(int global_directory,
												const ClusterRecoveryRegistryV2 *, Size byte_budget,
												uint8 reference[128]);
extern bool cluster_recovery_registry_read_v2(int global_directory,
											  const ClusterRecoveryRegistryV2 *,
											  const uint8 reference[128], Size byte_budget);
/* Local decoded storage only. The complete canonical vectors are validated
 * before returning a view. A selected reference must come independently from
 * the original control owner; decoding or loading alone grants no authority.
 * budget bounds complete wire bytes, not the active-pin capacity. Fresh *out
 * must be NULL. Refusal preserves outputs; destroy invalidates before free. */
typedef struct ClusterRecoveryRegistryDecodedV2 ClusterRecoveryRegistryDecodedV2;
extern bool cluster_recovery_registry_decode_v2(const uint8 *bytes, Size length,
												const uint8 reference[128], Size byte_budget,
												ClusterRecoveryRegistryDecodedV2 **out);
extern bool cluster_recovery_registry_load_selected_v2(int global_directory,
													   const uint8 reference[128], Size byte_budget,
													   ClusterRecoveryRegistryDecodedV2 **out);
extern const ClusterRecoveryRegistryV2 *
cluster_recovery_registry_view_v2(const ClusterRecoveryRegistryDecodedV2 *);
extern void cluster_recovery_registry_destroy_v2(ClusterRecoveryRegistryDecodedV2 **);
#endif
