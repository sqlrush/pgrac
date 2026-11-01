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
/* Fresh target only: encode a clean generation-one image before creating
 * either O_EXCL name. Sync both held files and directory, reread every byte,
 * and close before publishing output. Failure never unlinks/replaces a name;
 * the original creator retains every partial-file/registration obligation. */
extern bool cluster_relmap_file_pair_create_v2(int directory,
	const ClusterRecoveryFileIdentityV2 *identity, const RelMapFile *committed,
	ClusterRelmapFilePairV2 *output);
/* Creation redo's storage step only. The original replay owner independently
 * authenticates the complete creation record/batch and actual EndRecPtr,
 * retains target registration and file protection, and later confirms its
 * receipts. Both missing names may be created exclusively; a complete exact
 * generation-one pair is only synced/read back. Partial, foreign, changed or
 * later images refuse without replacement, repair or publication. No recovery,
 * transaction, AVAILABLE or coverage authority is granted by these bytes. */
extern bool cluster_relmap_file_pair_replay_create_v2(int directory,
	const ClusterRecoveryFileIdentityV2 *identity, const RelMapFile *committed,
	ClusterRelmapFilePairV2 *output);
#endif
