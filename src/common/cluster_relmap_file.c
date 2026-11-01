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

static bool
file_encode(const ClusterRecoveryFileIdentityV2 *identity,
			const ClusterRelmapV2Image *image, void *output, size_t length, bool intent)
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
	if ((intent ? cluster_relmap_v2_encode_intent(image, bytes + 128, CLUSTER_RELMAP_V2_FILE_SIZE)
			 : cluster_relmap_v2_encode(image, bytes + 128, CLUSTER_RELMAP_V2_FILE_SIZE))
		!= CLUSTER_RELMAP_V2_VALID)
		return false;
	file_put32(bytes + 112, CLUSTER_RELMAP_V2_FILE_SIZE);
	file_put32(bytes + 116, file_crc(bytes + 128, CLUSTER_RELMAP_V2_FILE_SIZE, NULL, 0));
	file_put32(bytes + 120, file_crc(bytes, 120, bytes + 128, CLUSTER_RELMAP_V2_FILE_SIZE));
	file_put32(bytes + 124, file_crc(bytes, 124, NULL, 0));
	memcpy(output, bytes, sizeof(bytes));
	return true;
}

static bool
file_observe(const void *input, size_t length, ClusterRecoveryFileIdentityV2 *identity_out,
			 ClusterRelmapV2Image *output, bool intent)
{
	const unsigned char *bytes = input;
	unsigned char reference[CLUSTER_RECOVERY_IDENTITY_REFERENCE_V2_BYTES];
	ClusterRelmapV2Namespace ns;
	ClusterRecoveryFileIdentityV2 observed;
	ClusterRelmapV2Image image;

	if (input == NULL || output == NULL || identity_out == NULL
		|| length != CLUSTER_RELMAP_FILE_SIZE_V2
		|| file_overlap(output, sizeof(*output), input, length)
		|| file_overlap(identity_out, sizeof(*identity_out), input, length)
		|| file_overlap(output, sizeof(*output), identity_out, sizeof(*identity_out)))
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
	if (!cluster_recovery_file_identity_decode_v2(reference, sizeof(reference), &observed)
		|| observed.kind != CLUSTER_RECOVERY_FILE_RELMAP_V2
		|| !OidIsValid(observed.tablespace_oid))
		return false;
	ns = (ClusterRelmapV2Namespace) {observed.system_identifier,
		observed.database_incarnation, observed.database_oid, observed.database_oid == InvalidOid};
	if ((intent ? cluster_relmap_v2_decode_intent(bytes + 128, CLUSTER_RELMAP_V2_FILE_SIZE, &ns, &image)
			 : cluster_relmap_v2_decode(bytes + 128, CLUSTER_RELMAP_V2_FILE_SIZE, &ns, &image))
		!= CLUSTER_RELMAP_V2_VALID)
		return false;
	*identity_out = observed;
	*output = image;
	return true;
}

bool
cluster_relmap_file_encode_v2(const ClusterRecoveryFileIdentityV2 *identity,
							const ClusterRelmapV2Image *image, void *output, size_t length)
{
	return file_encode(identity, image, output, length, false);
}

bool
cluster_relmap_file_observe_v2(const void *input, size_t length,
							 ClusterRecoveryFileIdentityV2 *identity_out,
							 ClusterRelmapV2Image *output)
{
	return file_observe(input, length, identity_out, output, false);
}

bool
cluster_relmap_wal_encode_intent_v2(const ClusterRecoveryFileIdentityV2 *identity,
								  const ClusterRelmapV2Image *image, void *output, size_t length)
{
	return file_encode(identity, image, output, length, true);
}

bool
cluster_relmap_wal_observe_intent_v2(const void *input, size_t length,
								   ClusterRecoveryFileIdentityV2 *identity_out,
								   ClusterRelmapV2Image *output)
{
	return file_observe(input, length, identity_out, output, true);
}

bool
cluster_relmap_wal_rebuild_pending_v2(const void *input, size_t length,
									const ClusterRecoveryFileIdentityV2 *expected,
									uint64 actual_end, void *output, size_t output_length)
{
	ClusterRecoveryFileIdentityV2 observed;
	ClusterRelmapV2Image image;
	unsigned char reference[CLUSTER_RECOVERY_IDENTITY_REFERENCE_V2_BYTES];

	if (expected == NULL || output == NULL || actual_end == 0
		|| output_length != CLUSTER_RELMAP_FILE_SIZE_V2
		|| file_overlap(output, output_length, input, length)
		|| file_overlap(output, output_length, expected, sizeof(*expected))
		|| !cluster_relmap_wal_observe_intent_v2(input, length, &observed, &image)
		|| !cluster_recovery_file_identity_encode_v2(&observed, reference, sizeof(reference))
		|| !cluster_recovery_file_identity_matches_v2(reference, sizeof(reference), expected))
		return false;
	image.pending_owner.wal_end = actual_end;
	return cluster_relmap_file_encode_v2(expected, &image, output, output_length);
}

bool
cluster_relmap_file_decode_v2(const void *input, size_t length,
							const ClusterRecoveryFileIdentityV2 *expected,
							ClusterRelmapV2Image *output)
{
	ClusterRecoveryFileIdentityV2 observed;
	ClusterRelmapV2Image image;
	unsigned char reference[CLUSTER_RECOVERY_IDENTITY_REFERENCE_V2_BYTES];

	if (input == NULL || output == NULL || expected == NULL
		|| file_overlap(output, sizeof(*output), input, length)
		|| file_overlap(output, sizeof(*output), expected, sizeof(*expected))
		|| !cluster_relmap_file_observe_v2(input, length, &observed, &image)
		|| !cluster_recovery_file_identity_encode_v2(&observed, reference, sizeof(reference))
		|| !cluster_recovery_file_identity_matches_v2(reference, sizeof(reference), expected))
		return false;
	*output = image;
	return true;
}
