/*-------------------------------------------------------------------------
 * cluster_relmap_file.c
 *    Encode the complete registered identity around an unchanged relmap image.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/common/cluster_relmap_file.c
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif
#include "common/cluster_relmap_file.h"
#include "common/cluster_relmap_create.h"
#include "catalog/pg_tablespace_d.h"

/* RFM2 uses the shared RFI2 identity prefix and its assigned byte order. */
static void
file_put32(unsigned char *p, uint32 value)
{
	for (unsigned i = 0; i < 4; ++i)
		p[i] = (unsigned char) (value >> (24 - 8 * i));
}

static uint32
file_get32(const unsigned char *p)
{
	return ((uint32) p[0] << 24) | ((uint32) p[1] << 16)
		| ((uint32) p[2] << 8) | p[3];
}

static bool
file_overlap(const void *a, size_t alen, const void *b, size_t blen)
{
	uintptr_t x = (uintptr_t) a, y = (uintptr_t) b;
	return x <= y ? y - x < alen : x - y < blen;
}

static pg_crc32c
file_crc(const void *first, size_t first_length, const void *last, size_t last_length)
{
	pg_crc32c crc;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, first, first_length);
	if (last_length != 0)
		COMP_CRC32C(crc, last, last_length);
	FIN_CRC32C(crc);
	return crc;
}

bool
cluster_relmap_file_encode_v2(const ClusterRecoveryFileIdentityV2 *identity,
							const ClusterRelmapV2Image *image, void *output, size_t length)
{
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2] = {0};
	if (identity == NULL || identity->kind != CLUSTER_RECOVERY_FILE_RELMAP_V2
		|| !OidIsValid(identity->tablespace_oid)
		|| image == NULL || output == NULL || length != sizeof(bytes)
		|| file_overlap(output, length, identity, sizeof(*identity))
		|| file_overlap(output, length, image, sizeof(*image))
		|| image->identity.system_identifier != identity->system_identifier
		|| image->identity.database_incarnation != identity->database_incarnation
		|| image->identity.dbid != identity->database_oid
		|| image->identity.shared != (identity->database_oid == InvalidOid)
		|| !cluster_recovery_file_identity_encode_v2(identity, bytes, 128))
		return false;
	memcpy(bytes, "RFM2", 4);
	if (cluster_relmap_v2_encode(image, bytes + 128, CLUSTER_RELMAP_V2_FILE_SIZE)
		!= CLUSTER_RELMAP_V2_VALID)
		return false;
	file_put32(bytes + 112, CLUSTER_RELMAP_V2_FILE_SIZE);
	file_put32(bytes + 116, file_crc(bytes + 128, CLUSTER_RELMAP_V2_FILE_SIZE, NULL, 0));
	file_put32(bytes + 120, file_crc(bytes, 120, bytes + 128, CLUSTER_RELMAP_V2_FILE_SIZE));
	file_put32(bytes + 124, file_crc(bytes, 124, NULL, 0));
	memcpy(output, bytes, sizeof(bytes));
	return true;
}

bool
cluster_relmap_file_decode_v2(const void *input, size_t length,
							const ClusterRecoveryFileIdentityV2 *expected,
							ClusterRelmapV2Image *output)
{
	const unsigned char *bytes = input;
	unsigned char reference[CLUSTER_RECOVERY_IDENTITY_REFERENCE_V2_BYTES];
	ClusterRelmapV2Namespace ns;
	ClusterRelmapV2Image image;

	if (input == NULL || output == NULL || expected == NULL
		|| expected->kind != CLUSTER_RECOVERY_FILE_RELMAP_V2
		|| !OidIsValid(expected->tablespace_oid)
		|| length != CLUSTER_RELMAP_FILE_SIZE_V2
		|| file_overlap(output, sizeof(*output), input, length)
		|| file_overlap(output, sizeof(*output), expected, sizeof(*expected)))
		return false;
	if (memcmp(bytes, "RFM2", 4) != 0 || file_get32(bytes + 112) != CLUSTER_RELMAP_V2_FILE_SIZE
		|| file_get32(bytes + 124) != file_crc(bytes, 124, NULL, 0)
		|| file_get32(bytes + 120) != file_crc(bytes, 120, bytes + 128, CLUSTER_RELMAP_V2_FILE_SIZE)
		|| file_get32(bytes + 116) != file_crc(bytes + 128, CLUSTER_RELMAP_V2_FILE_SIZE, NULL, 0))
		return false;
	/* Reconstruct only private scratch; the saved envelope is never repaired. */
	memcpy(reference, bytes, sizeof(reference));
	memcpy(reference, "RFI2", 4);
	memset(reference + 112, 0, 12);
	file_put32(reference + 124, file_crc(reference, 124, NULL, 0));
	if (!cluster_recovery_file_identity_matches_v2(reference, sizeof(reference), expected))
		return false;
	ns = (ClusterRelmapV2Namespace) {expected->system_identifier,
		expected->database_incarnation, expected->database_oid, expected->database_oid == InvalidOid};
	if (cluster_relmap_v2_decode(bytes + 128, CLUSTER_RELMAP_V2_FILE_SIZE, &ns, &image)
		!= CLUSTER_RELMAP_V2_VALID)
		return false;
	*output = image;
	return true;
}

/* Independent CREATE WAL never borrows an UPDATE/pending owner. */
#define CREATE_OFFSET(name) CLUSTER_RELMAP_CREATE_##name##_OFFSET_V1

static bool
create_nonzero_batch(const uint8 batch[16])
{
	for (unsigned i = 0; i < 16; ++i)
		if (batch[i] != 0)
			return true;
	return false;
}

static bool
create_target(const ClusterRecoveryFileIdentityV2 *id)
{
	return id != NULL && id->kind == CLUSTER_RECOVERY_FILE_RELMAP_V2
		&& OidIsValid(id->database_oid) && OidIsValid(id->tablespace_oid)
		&& id->tablespace_oid != GLOBALTABLESPACE_OID && id->object_id == 0;
}

bool
cluster_relmap_create_encode_v1(const ClusterRecoveryFileIdentityV2 *id,
	const uint8 batch[16], const RelMapFile *map, void *out, Size length)
{
	unsigned char bytes[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1] = {0};
	ClusterRelmapV2Image image = {0};
	if (id == NULL || batch == NULL || map == NULL || out == NULL
		|| length != sizeof(bytes)
		|| file_overlap(out, length, id, sizeof(*id))
		|| file_overlap(out, length, batch, 16)
		|| file_overlap(out, length, map, sizeof(*map))
		|| !create_target(id) || !create_nonzero_batch(batch))
		return false;
	image.identity = (ClusterRelmapV2Namespace){id->system_identifier,
		id->database_incarnation, id->database_oid, false};
	image.committed_generation = 1;
	image.committed = *map;
	if (!cluster_relmap_file_encode_v2(id, &image, bytes + CREATE_OFFSET(IMAGE), CLUSTER_RELMAP_FILE_SIZE_V2))
		return false;
	file_put32(bytes + CREATE_OFFSET(FORMAT_ID), CLUSTER_RELMAP_CREATE_FORMAT_ID_V1);
	file_put32(bytes + CREATE_OFFSET(FORMAT_VERSION), CLUSTER_RELMAP_CREATE_FORMAT_VERSION_V1);
	file_put32(bytes + CREATE_OFFSET(STATE), CLUSTER_RELMAP_CREATE_RECORD_STATE_V1);
	file_put32(bytes + CREATE_OFFSET(RECORD_BYTES), sizeof(bytes));
	file_put32(bytes + CREATE_OFFSET(HEADER_BYTES), CLUSTER_RELMAP_CREATE_HEADER_BYTES_V1);
	file_put32(bytes + CREATE_OFFSET(IMAGE_BYTES), CLUSTER_RELMAP_FILE_SIZE_V2);
	memcpy(bytes + CREATE_OFFSET(BATCH_UUID), batch, CLUSTER_RELMAP_CREATE_BATCH_UUID_BYTES_V1);
	file_put32(bytes + CREATE_OFFSET(IMAGE_CRC32C),
		file_crc(bytes + CREATE_OFFSET(IMAGE), CLUSTER_RELMAP_FILE_SIZE_V2, NULL, 0));
	file_put32(bytes + CREATE_OFFSET(HEADER_CRC32C),
		file_crc(bytes, CREATE_OFFSET(HEADER_CRC32C), NULL, 0));
	memcpy(out, bytes, sizeof(bytes));
	return true;
}

bool
cluster_relmap_create_decode_v1(const void *input, Size length, ClusterRelmapCreateWalV1 *out)
{
	const unsigned char *bytes = input;
	unsigned char reference[CLUSTER_RECOVERY_IDENTITY_REFERENCE_V2_BYTES];
	ClusterRelmapCreateWalV1 record = {0};
	ClusterRelmapV2Image image;
	if (input == NULL || out == NULL || length != CLUSTER_RELMAP_CREATE_WAL_SIZE_V1
		|| file_overlap(out, sizeof(*out), input, length))
		return false;
	if (!cluster_relmap_create_format_registered_v1(file_get32(bytes + CREATE_OFFSET(FORMAT_ID)),
			file_get32(bytes + CREATE_OFFSET(FORMAT_VERSION)), file_get32(bytes + CREATE_OFFSET(STATE)), CATALOG_VERSION_NO)
		|| file_get32(bytes + CREATE_OFFSET(RECORD_BYTES)) != length
		|| file_get32(bytes + CREATE_OFFSET(HEADER_BYTES)) != CLUSTER_RELMAP_CREATE_HEADER_BYTES_V1
		|| file_get32(bytes + CREATE_OFFSET(IMAGE_BYTES)) != CLUSTER_RELMAP_FILE_SIZE_V2
		|| !create_nonzero_batch(bytes + CREATE_OFFSET(BATCH_UUID))
		|| file_get32(bytes + CREATE_OFFSET(HEADER_CRC32C))
			!= file_crc(bytes, CREATE_OFFSET(HEADER_CRC32C), NULL, 0)
		|| file_get32(bytes + CREATE_OFFSET(IMAGE_CRC32C)) != file_crc(bytes + CREATE_OFFSET(IMAGE), CLUSTER_RELMAP_FILE_SIZE_V2, NULL, 0))
		return false;
	for (unsigned i = CREATE_OFFSET(RESERVED_ZERO);
		 i < CREATE_OFFSET(RESERVED_ZERO) + CLUSTER_RELMAP_CREATE_RESERVED_ZERO_BYTES_V1; ++i)
		if (bytes[i] != 0)
			return false;
	/* Decode only scratch: the complete original RFM2, including its envelope
	 * CRC and all identity fields, is still independently checked below. */
	memcpy(reference, bytes + CREATE_OFFSET(IMAGE), sizeof(reference));
	memcpy(reference, "RFI2", 4);
	memset(reference + 112, 0, 12);
	file_put32(reference + 124, file_crc(reference, 124, NULL, 0));
	if (!cluster_recovery_file_identity_decode_v2(reference, sizeof(reference), &record.identity)
		|| !create_target(&record.identity)
		|| !cluster_relmap_file_decode_v2(bytes + CREATE_OFFSET(IMAGE), CLUSTER_RELMAP_FILE_SIZE_V2,
			&record.identity, &image)
		|| image.committed_generation != 1 || image.pending_generation != 0)
		return false;
	memcpy(record.batch_uuid, bytes + CREATE_OFFSET(BATCH_UUID), CLUSTER_RELMAP_CREATE_BATCH_UUID_BYTES_V1);
	record.committed = image.committed;
	*out = record;
	return true;
}

bool
cluster_relmap_create_rebuild_v1(const void *input, Size length,
	const ClusterRecoveryFileIdentityV2 *expected, const uint8 batch[16],
	XLogRecPtr actual_end, ClusterRelmapCreateReplayV1 *out)
{
	ClusterRelmapCreateReplayV1 replay = {0};
	uint8 reference[CLUSTER_RECOVERY_IDENTITY_REFERENCE_V2_BYTES];
	if (input == NULL || expected == NULL || batch == NULL || out == NULL
		|| length != CLUSTER_RELMAP_CREATE_WAL_SIZE_V1
		|| file_overlap(out, sizeof(*out), input, length)
		|| file_overlap(out, sizeof(*out), expected, sizeof(*expected))
		|| file_overlap(out, sizeof(*out), batch, 16)
		|| XLogRecPtrIsInvalid(actual_end) || actual_end == UINT64_MAX
		|| !cluster_relmap_create_decode_v1(input, length, &replay.record)
		|| memcmp(replay.record.batch_uuid, batch, 16)
		|| !cluster_recovery_file_identity_encode_v2(&replay.record.identity, reference, sizeof(reference))
		|| !cluster_recovery_file_identity_matches_v2(reference, sizeof(reference), expected))
		return false;
	replay.actual_end = actual_end;
	*out = replay;
	return true;
}
