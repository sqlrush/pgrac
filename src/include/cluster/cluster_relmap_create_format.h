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
 *     Explicit byte registration; no encoder, producer, redo or activation.
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

/* Independent header: unsigned integers are big-endian, UUID is raw bytes.
 * The payload is the complete original RFM2 envelope and inner image; their
 * existing encoding, identity and CRC rules are unchanged. Never serialize
 * ClusterControlNativeRelmapCreateV2 or any of its owner/private fields. */
#define CLUSTER_RELMAP_CREATE_HEADER_BYTES_V1 64
#define CLUSTER_RELMAP_CREATE_IMAGE_BYTES_V1 2304
#define CLUSTER_RELMAP_CREATE_RECORD_BYTES_V1 2368
#define CLUSTER_RELMAP_CREATE_FORMAT_ID_OFFSET_V1 0
#define CLUSTER_RELMAP_CREATE_FORMAT_VERSION_OFFSET_V1 4
#define CLUSTER_RELMAP_CREATE_STATE_OFFSET_V1 8
#define CLUSTER_RELMAP_CREATE_RECORD_BYTES_OFFSET_V1 12
#define CLUSTER_RELMAP_CREATE_HEADER_BYTES_OFFSET_V1 16
#define CLUSTER_RELMAP_CREATE_IMAGE_BYTES_OFFSET_V1 20
#define CLUSTER_RELMAP_CREATE_BATCH_UUID_OFFSET_V1 24
#define CLUSTER_RELMAP_CREATE_BATCH_UUID_BYTES_V1 16
#define CLUSTER_RELMAP_CREATE_RESERVED_ZERO_OFFSET_V1 40
#define CLUSTER_RELMAP_CREATE_RESERVED_ZERO_BYTES_V1 16
#define CLUSTER_RELMAP_CREATE_IMAGE_CRC32C_OFFSET_V1 56
#define CLUSTER_RELMAP_CREATE_HEADER_CRC32C_OFFSET_V1 60
#define CLUSTER_RELMAP_CREATE_IMAGE_OFFSET_V1 64

/* Image CRC32C covers all 2304 payload bytes [64,2368). Header CRC32C
 * covers [0,60), including the stored image CRC and zero reserved bytes.
 * Both use PostgreSQL INIT/COMP/FIN_CRC32C; results are stored big-endian.
 * Exact lengths, zero reserved bytes and a nonzero batch UUID are required.
 * The payload must be clean generation 1 with zero pending generation,
 * owner and slot. Its complete expected identity comes from RFM2, never
 * from an extra C struct or a current file. No actual WAL end is serialized
 * here: the original owner must independently validate the actual insert
 * end / redo EndRecPtr without inventing a pending transaction owner.
 * Pure codecs must reject overlapping input/output and output/output spans
 * before writing and leave every output unchanged on refusal. Parsing
 * proves no reservation, creation custody, AVAILABLE or completion grant. */
StaticAssertDecl(CLUSTER_RELMAP_CREATE_HEADER_CRC32C_OFFSET_V1 + 4
				 == CLUSTER_RELMAP_CREATE_HEADER_BYTES_V1, "RMC1 header extent");
StaticAssertDecl(CLUSTER_RELMAP_CREATE_IMAGE_OFFSET_V1
				 + CLUSTER_RELMAP_CREATE_IMAGE_BYTES_V1
				 == CLUSTER_RELMAP_CREATE_RECORD_BYTES_V1, "RMC1 record extent");

/* The independent record carries the complete target identity, original
 * creation batch and complete generation-1 image. Its state belongs to that
 * record, not the RFM2 inner byte53 or a pending transaction owner. The clean
 * file image keeps its original zero state, owner and pending slot. This
 * registration does not make existing file or UPDATE_V2 decoders accept
 * creation records.
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
