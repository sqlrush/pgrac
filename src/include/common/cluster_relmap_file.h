/*-------------------------------------------------------------------------
 * cluster_relmap_file.h
 *    Control-bound relmap envelope and pair I/O observations.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/include/common/cluster_relmap_file.h
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_FILE_H
#define CLUSTER_RELMAP_FILE_H

#include "cluster/cluster_recovery_format.h"
#include "common/cluster_relmap_v2.h"

#define CLUSTER_RELMAP_FILE_FORMAT_VERSION_V2 CLUSTER_RECOVERY_FORMAT_VERSION_V2
#define CLUSTER_RELMAP_FILE_HEADER_SIZE_V2 128
#define CLUSTER_RELMAP_FILE_SIZE_V2 (CLUSTER_RELMAP_FILE_HEADER_SIZE_V2 + CLUSTER_RELMAP_V2_FILE_SIZE)

/* Complete observed bytes, not a checkpoint coverage grant. The control owner
 * must conjoin every registered namespace and the original source vector. */
typedef struct ClusterRelmapFilePairV2
{
	unsigned char bytes[2][CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRelmapV2View view;
} ClusterRelmapFilePairV2;

extern bool cluster_relmap_file_encode_v2(const ClusterRecoveryFileIdentityV2 *identity,
										const ClusterRelmapV2Image *image,
										void *output, size_t length);
extern bool cluster_relmap_file_decode_v2(const void *bytes, size_t length,
										const ClusterRecoveryFileIdentityV2 *expected,
										ClusterRelmapV2Image *image);

/* Parse saved file/WAL bytes without importing an expected control binding.
 * This exposes an observed identity only; it grants no registration, custody,
 * replay permission, terminal state or checkpoint coverage. Both outputs stay
 * unchanged on failure and must not overlap each other or the input. Runtime
 * owners must still use decode_v2 with their independently selected identity. */
extern bool cluster_relmap_file_observe_v2(const void *bytes, size_t length,
								ClusterRecoveryFileIdentityV2 *identity,
								ClusterRelmapV2Image *image);

/* The directory descriptor is borrowed from the original path owner; it is
 * never closed here. That owner serializes map updates and rechecks its path
 * and original registration before publishing any returned observation.
 * Failure leaves output untouched.
 * Checkpoint syncs both held files and directory, then rereads both whole files.
 * No helper repairs a missing, old-format or foreign copy. */
extern bool cluster_relmap_file_pair_read_v2(int directory,
								const ClusterRecoveryFileIdentityV2 *identity,
								ClusterRelmapFilePairV2 *output);
extern bool cluster_relmap_file_pair_checkpoint_v2(int directory,
								const ClusterRecoveryFileIdentityV2 *identity,
								ClusterRelmapFilePairV2 *output);
#endif
