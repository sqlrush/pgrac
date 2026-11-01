/*-------------------------------------------------------------------------
 *
 * cluster_recovery_coverage_format.h
 *     Coverage registry format with the original creation batch UUID.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/include/cluster/cluster_recovery_coverage_format.h
 *
 * NOTES
 *     Byte registration only; no encoder, publication or recovery authority.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_COVERAGE_FORMAT_H
#define CLUSTER_RECOVERY_COVERAGE_FORMAT_H

#include "c.h"
#include "catalog/catversion.h"
#include "cluster/cluster_recovery_format.h"

#define CLUSTER_RECOVERY_COVERAGE_FORMAT_ID_V4 UINT32_C(0x43564634)
#define CLUSTER_RECOVERY_COVERAGE_FORMAT_VERSION_V4 UINT32_C(4)
#define CLUSTER_RECOVERY_COVERAGE_MIN_CATALOG_VERSION UINT32_C(202611010)
#define CLUSTER_RECOVERY_COVERAGE_HEADER_MAGIC_V4 "PRCREG4"
#define CLUSTER_RECOVERY_COVERAGE_ROOT_REFERENCE_MAGIC_V4 "PRCREF4"
#define CLUSTER_RECOVERY_COVERAGE_HEADER_BYTES_V4 CLUSTER_RECOVERY_REGISTRY_HEADER_V2_BYTES
#define CLUSTER_RECOVERY_COVERAGE_ROOT_REFERENCE_BYTES_V4 \
	CLUSTER_RECOVERY_REGISTRY_ROOT_REFERENCE_V2_BYTES
#define CLUSTER_RECOVERY_COVERAGE_SOURCE_BYTES_V4 CLUSTER_RECOVERY_CHECKPOINT_SOURCE_V2_BYTES
#define CLUSTER_RECOVERY_COVERAGE_INTERVAL_BYTES_V4 CLUSTER_RECOVERY_FILE_SOURCE_INTERVAL_V2_BYTES
#define CLUSTER_RECOVERY_CHECKPOINT_FILE_V4_PREFIX_BYTES CLUSTER_RECOVERY_CHECKPOINT_FILE_V2_BYTES
#define CLUSTER_RECOVERY_CHECKPOINT_FILE_V4_CREATION_BATCH_UUID_OFFSET 208
#define CLUSTER_RECOVERY_CHECKPOINT_FILE_V4_CREATION_BATCH_UUID_BYTES 16
#define CLUSTER_RECOVERY_CHECKPOINT_FILE_V4_BYTES 224

/* Both magic values occupy exactly eight bytes including their trailing NUL.
 * Header/reference fields keep the original V2 offsets and encodings, with
 * this family's exact magic/format/version. Each file retains its complete
 * original 208-byte prefix and appends the unchanged 16-byte batch UUID.
 * Sources and intervals retain their original bytes. Integer fields remain
 * big-endian; UUIDs/digests remain raw bytes. There is no native struct image
 * or per-file version field on disk.
 *
 * Exact magic/format/version selects the stride: old coverage V2 is 208,
 * this coverage V4 is 224. Never infer the stride from available bytes, read
 * V4 as V2, silently drop the UUID, or accept trailing/partial records.
 * Exact total length is 256 + sources*288 + files*224 + intervals*24, with
 * checked arithmetic and exact vector offsets/counts. The existing reserved
 * zero bytes, source SHA, file-plus-interval SHA, vector CRC, header CRC and
 * full-registry reference SHA/CRC retain their original scopes; the appended
 * UUID bytes are included in every enclosing file/vector/full-registry hash
 * or CRC. A reservation container keeps its own RSV1/V3 identity and record
 * V1 layout; its embedded coverage and reference explicitly select V4.
 *
 * BOOT/old records without batch evidence expose zero. Zero never authorizes
 * creation recovery. Completed copies of the same identity require the same
 * nonzero batch for that authorization; other coverage gates remain intact.
 * Parsing/registration grants no custody, AVAILABLE, COMPLETE or OPEN.
 */
StaticAssertDecl(sizeof(CLUSTER_RECOVERY_COVERAGE_HEADER_MAGIC_V4) == 8,
				 "coverage V4 magic extent");
StaticAssertDecl(sizeof(CLUSTER_RECOVERY_COVERAGE_ROOT_REFERENCE_MAGIC_V4) == 8,
				 "coverage V4 reference magic extent");
StaticAssertDecl(CLUSTER_RECOVERY_CHECKPOINT_FILE_V4_PREFIX_BYTES
				 == CLUSTER_RECOVERY_CHECKPOINT_FILE_V4_CREATION_BATCH_UUID_OFFSET,
				 "coverage V4 preserves the V2 prefix");
StaticAssertDecl(CLUSTER_RECOVERY_CHECKPOINT_FILE_V4_CREATION_BATCH_UUID_OFFSET
				 + CLUSTER_RECOVERY_CHECKPOINT_FILE_V4_CREATION_BATCH_UUID_BYTES
				 == CLUSTER_RECOVERY_CHECKPOINT_FILE_V4_BYTES, "coverage V4 file extent");

/* control_catalog_version is the original independently loaded control value,
 * never a field supplied by the registry being parsed. This metadata predicate
 * does not replace the original control, source or coverage checks. */
static inline bool
cluster_recovery_coverage_format_registered_v4(uint32 format_id, uint32 format_version,
											uint32 file_record_bytes, uint32 control_catalog_version)
{
	return format_id == CLUSTER_RECOVERY_COVERAGE_FORMAT_ID_V4
		   && format_version == CLUSTER_RECOVERY_COVERAGE_FORMAT_VERSION_V4
		   && file_record_bytes == CLUSTER_RECOVERY_CHECKPOINT_FILE_V4_BYTES
		   && control_catalog_version == CATALOG_VERSION_NO
		   && control_catalog_version >= CLUSTER_RECOVERY_COVERAGE_MIN_CATALOG_VERSION;
}

#endif /* CLUSTER_RECOVERY_COVERAGE_FORMAT_H */
