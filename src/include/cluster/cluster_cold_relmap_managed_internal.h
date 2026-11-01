/*-------------------------------------------------------------------------
 * cluster_cold_relmap_managed_internal.h
 *     Complete managed record comparisons for original cold participants.
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/include/cluster/cluster_cold_relmap_managed_internal.h
 * NOTES
 *     This is a pgrac-original internal header; helpers use cold_relmap_.
 *     Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_COLD_RELMAP_MANAGED_INTERNAL_H
#define CLUSTER_COLD_RELMAP_MANAGED_INTERNAL_H
#include "access/xlogreader.h"
#include "utils/relmapper.h"
#include "common/cluster_relmap_file.h"
#include "cluster/cluster_cold_relmap.h"

/* Canonical bytes include every identity and both complete images; padding
 * in by-value inputs is not a persistence or equality contract. */
static inline bool
cold_relmap_managed_image_matches(const ClusterColdRelmapInputV1 *input,
	const ClusterRecoveryFileIdentityV2 *identity, const ClusterRelmapV2Image *image)
{
	unsigned char expected[CLUSTER_RELMAP_FILE_SIZE_V2], observed[CLUSTER_RELMAP_FILE_SIZE_V2];
	return identity != NULL && image != NULL
		&& cluster_relmap_file_encode_v2(&input->file_identity, &input->staged, expected, sizeof(expected))
		&& cluster_relmap_file_encode_v2(identity, image, observed, sizeof(observed))
		&& memcmp(expected, observed, sizeof(expected)) == 0;
}

static inline bool
cold_relmap_managed_wal_matches(const ClusterColdRelmapInputV1 *input, const XLogReaderState *reader)
{
	unsigned char expected[CLUSTER_RELMAP_FILE_SIZE_V2], observed[CLUSTER_RELMAP_FILE_SIZE_V2];
	return reader != NULL && reader->record != NULL
		&& XLogRecGetInfo(reader) == XLOG_RELMAP_UPDATE_V2
		&& cluster_relmap_file_encode_v2(&input->file_identity, &input->staged, expected, sizeof(expected))
		&& cluster_relmap_wal_rebuild_pending_v2(XLogRecGetData(reader), XLogRecGetDataLen(reader),
			&input->file_identity, reader->EndRecPtr, observed, sizeof(observed))
		&& memcmp(expected, observed, sizeof(expected)) == 0;
}
#endif
