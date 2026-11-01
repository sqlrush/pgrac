/* Identity reservations are distinct from synchronized file coverage. */
#ifndef CLUSTER_RECOVERY_RESERVATIONS_H
#define CLUSTER_RECOVERY_RESERVATIONS_H
#include "common/cluster_recovery_registry.h"
#include "cluster/cluster_recovery_registry_reservation_format.h"

#define CLUSTER_RECOVERY_FILE_RESERVED_V1 UINT32_C(1)
#define CLUSTER_RECOVERY_REGISTRY_HEADER_V3_BYTES 256
#define CLUSTER_RECOVERY_RESERVATION_V1_BYTES 256

/* No FD, pin, sync, page, digest or copy receipt belongs to this record.
 * The original control owner authenticates batch/owner/current before use. */
typedef struct ClusterRecoveryFileReservationV1 {
	ClusterRecoveryFileIdentityV2 expected;
	uint8 batch_uuid[16];
	ClusterControlRootIdentity creation_owner;
	uint64 owner_generation;
	uint32 state;
} ClusterRecoveryFileReservationV1;

typedef struct ClusterRecoveryRegistryV3 {
	ClusterRecoveryRegistryV2 available;
	uint32 catalog_version;
	uint64 reservation_count;
	const ClusterRecoveryFileReservationV1 *reservations;
} ClusterRecoveryRegistryV3;

/* Big-endian V3: header[256], exact original V2 coverage image, records[256].
 * Header: magic0, format8, version12, width16, catalog20, total24,
 * coverage-offset32/length40, reservations-offset48/count56,
 * record-version64/width68, body-CRC72/header-CRC76, reserved80..127,
 * exact nested V2 reference128..255.
 * Record: exact identity0..127, batch128..143, original root owner144..223,
 * owner-generation224, state232, version236, reserved240..255.
 * Reference: original generation/count offsets, V3 magic/ID/version and whole
 * SHA256; record-version108, reservation-count112, catalog120, zero124..127.
 * Full byte_budget includes both vectors, independent of active pin limits.
 * Ordinary consumers use only available; reservations confer no file rights.
 * All false returns preserve outputs, including alias and fresh-out refusal. */
extern bool cluster_recovery_registry_size_v3(const ClusterRecoveryRegistryV3 *, Size, Size *);
extern bool cluster_recovery_registry_encode_v3(const ClusterRecoveryRegistryV3 *, uint8 *, Size,
												uint8 reference[128]);
extern bool cluster_recovery_registry_reference_valid_v3(const uint8 reference[128],
													 uint32 actual_catalog_version);
extern bool cluster_recovery_registry_matches_v3(const uint8 *, Size,
												 const ClusterRecoveryRegistryV3 *,
												 const uint8 reference[128]);

typedef struct ClusterRecoveryRegistryDecodedV3 ClusterRecoveryRegistryDecodedV3;
extern bool cluster_recovery_registry_decode_v3(const uint8 *, Size, const uint8 reference[128],
											   uint32 actual_catalog_version, Size byte_budget,
											   ClusterRecoveryRegistryDecodedV3 **fresh);
extern const ClusterRecoveryRegistryV3 *
cluster_recovery_registry_view_v3(const ClusterRecoveryRegistryDecodedV3 *);
/* Invalidate and free local decoded vectors only; never retire material/debt. */
extern void cluster_recovery_registry_destroy_v3(ClusterRecoveryRegistryDecodedV3 **);

/* Immutable .v3 object under the original borrowed global-directory custody.
 * No replacement/unlink even on failure. Same descriptor/full readback and
 * named-file rechecks precede success; all local descriptors close on exit.
 * Creating/loading does not publish ROOT, change a state, or authorize I/O.
 * The original CF-X owner must separately revalidate/publish the selection. */
extern bool cluster_recovery_registry_create_v3(int global_directory,
											   const ClusterRecoveryRegistryV3 *, Size byte_budget,
											   uint8 reference[128]);
extern bool cluster_recovery_registry_load_selected_v3(int global_directory,
													  const uint8 reference[128], uint32 actual_catalog_version,
													  Size byte_budget, ClusterRecoveryRegistryDecodedV3 **fresh);
#endif
