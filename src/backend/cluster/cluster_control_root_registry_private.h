/* ROOTv4 representation and selected-registry observation, not publication. */
#ifndef CLUSTER_CONTROL_ROOT_REGISTRY_PRIVATE_H
#define CLUSTER_CONTROL_ROOT_REGISTRY_PRIVATE_H
#include "cluster_control_root_private.h"
#include "common/cluster_recovery_registry.h"
/* Original owner supplies its unpublished candidate and complete registry.
 * The v3 codec validates all original common/record semantics; bytes are built
 * only as v4. This does not migrate a selected legacy ROOT or authorize one. */
extern bool cluster_control_root_registry_encode_v4(const ControlRootImage *,
													const ClusterRecoveryRegistryV2 *,
													const uint8 reference[128], uint8 *bytes,
													Size length);
extern bool cluster_control_root_registry_matches_v4(const uint8 *bytes, Size length,
													 const ClusterRecoveryRegistryV2 *);
/* Explicit physical read, while original owner holds CF-S/X and its storage /
 * cohort custody. Primary must equal the owner's complete captured candidate;
 * no fallback, latest scan, repair, native-control qualification or publication.
 * Revisit primary/backup after registry read. No output or authority escapes. */
extern bool cluster_control_root_registry_read_selected_v4(int global_directory,
														   const uint8 *expected, Size length,
														   const ClusterRecoveryRegistryV2 *,
														   Size byte_budget);
/* Complete primary+backup capture for the existing control reader. No legacy
 * bytes/token escape; metadata.bytes is empty and format_version stays 4.
 * Both fixed paths are reread before local descriptors close. */
extern bool cluster_control_root_registry_capture_v4(int global_directory,
													 const uint8 storage_uuid[16],
													 uint64 system_identifier, uint8 pair[133120],
													 ControlRootImage *metadata);
#endif
