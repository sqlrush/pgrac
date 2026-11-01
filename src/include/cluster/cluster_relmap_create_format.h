/*-------------------------------------------------------------------------
 *
 * cluster_relmap_create_format.h
 *     Format registration for independent managed relmap creation WAL.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/include/cluster/cluster_relmap_create_format.h
 *
 * NOTES
 *     Registration only; no encoder, producer, redo or runtime activation.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_CREATE_FORMAT_H
#define CLUSTER_RELMAP_CREATE_FORMAT_H

#include "c.h"
#include "catalog/catversion.h"

#define CLUSTER_RELMAP_CREATE_FORMAT_ID_V1 UINT32_C(0x524D4331)
#define CLUSTER_RELMAP_CREATE_FORMAT_VERSION_V1 UINT32_C(1)
#define CLUSTER_RELMAP_CREATE_MIN_CATALOG_VERSION UINT32_C(202611010)
#define CLUSTER_RELMAP_CREATE_WAL_INFO_V1 0x20
#define CLUSTER_RELMAP_CREATE_RECORD_STATE_V1 UINT32_C(2)

/* The independent record carries the complete target identity, original
 * creation batch and complete generation-1 image. Its state belongs to that
 * record, not the RFM2 inner byte53 or a pending transaction owner. The clean
 * file image keeps its original zero state, owner and pending slot. This
 * registration does not define a record byte layout or make existing file
 * or UPDATE_V2 decoders accept creation records.
 *
 * The caller supplies its independently loaded control catalog. Matching
 * these fields grants neither creation custody nor replay permission. The
 * original owner must flush the actual insert end before first file creation;
 * redo must use that record's actual EndRecPtr and independent control binding.
 * No current-file inference, synthetic pending update or completion grant. */
static inline bool
cluster_relmap_create_format_registered_v1(uint32 format_id, uint32 format_version,
										 uint32 record_state, uint32 control_catalog_version)
{
	return format_id == CLUSTER_RELMAP_CREATE_FORMAT_ID_V1
		   && format_version == CLUSTER_RELMAP_CREATE_FORMAT_VERSION_V1
		   && record_state == CLUSTER_RELMAP_CREATE_RECORD_STATE_V1
		   && control_catalog_version == CATALOG_VERSION_NO
		   && control_catalog_version >= CLUSTER_RELMAP_CREATE_MIN_CATALOG_VERSION;
}

#endif /* CLUSTER_RELMAP_CREATE_FORMAT_H */
