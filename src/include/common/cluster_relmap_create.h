/*-------------------------------------------------------------------------
 * cluster_relmap_create.h
 *    Independent managed relmap creation WAL and replay inputs.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/include/common/cluster_relmap_create.h
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_CREATE_H
#define CLUSTER_RELMAP_CREATE_H
#include "cluster/cluster_relmap_create_format.h"
#include "common/cluster_relmap_file.h"

#define CLUSTER_RELMAP_CREATE_HEADER_SIZE_V1 64
#define CLUSTER_RELMAP_CREATE_WAL_SIZE_V1 (64 + CLUSTER_RELMAP_FILE_SIZE_V2)

typedef struct ClusterRelmapCreateWalV1
{
	ClusterRecoveryFileIdentityV2 identity;
	uint8 batch_uuid[16];
	RelMapFile committed;
} ClusterRelmapCreateWalV1;

typedef struct ClusterRelmapCreateReplayV1
{
	ClusterRelmapCreateWalV1 record;
	XLogRecPtr actual_end;
} ClusterRelmapCreateReplayV1;

/* RMC1: big-endian magic/version/total/state at 0/4/8/12, batch at16,
 * image bytes/CRC/record CRC at32/36/40, reserved44..63 zero, complete
 * generation-one clean RFM2 at64. Record CRC covers all bytes with its own
 * four bytes zero. Local carrier fields/padding are never serialized.
 * This observation grants no transaction/AVAILABLE/episode permission.
 * All failures preserve output; overlapping outputs refuse before access. */
extern bool cluster_relmap_create_encode_v1(const ClusterRecoveryFileIdentityV2 *,
	const uint8 batch_uuid[16], const RelMapFile *, void *, Size);
extern bool cluster_relmap_create_decode_v1(const void *, Size, ClusterRelmapCreateWalV1 *);
/* Owner supplies independently selected complete creation identity/batch and
 * the ORIGINAL record's actual EndRecPtr (or actual successful insert end).
 * Merely joins checked bytes to that end: no guessed end, current-file fill,
 * pending update, I/O, recovery grant or control completion. Original episode,
 * RESERVED registration, file lifetime and final confirmation remain caller
 * obligations; neither this value nor a copied record supplies them. */
extern bool cluster_relmap_create_rebuild_v1(const void *, Size,
	const ClusterRecoveryFileIdentityV2 *, const uint8 batch_uuid[16],
	XLogRecPtr actual_end, ClusterRelmapCreateReplayV1 *);
#endif
