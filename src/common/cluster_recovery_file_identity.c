/* PGRAC: shared explicit file identity codec; not a control authority. */
#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif

#include "cluster/cluster_recovery_format.h"
#include "port/pg_crc32c.h"

#define ID_OFFSET(name) CLUSTER_RECOVERY_IDENTITY_REFERENCE_V2_##name##_OFFSET

static bool
identity_overlap(const void *left, Size left_size, const void *right, Size right_size)
{
	uintptr_t a = (uintptr_t) left;
	uintptr_t b = (uintptr_t) right;

	if (left == NULL || right == NULL || left_size == 0 || right_size == 0)
		return false;
	return a <= b ? b - a < left_size : a - b < right_size;
}

static bool
identity_zero(const uint8 *bytes, Size size)
{
	for (Size i = 0; i < size; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

static bool
identity_counter(uint64 value)
{
	return value != 0 && value != UINT64_MAX;
}

/* The common format bound is not a caller's node/segment/permission check. */
static bool
identity_valid(const ClusterRecoveryFileIdentityV2 *id)
{
	return id != NULL
		&& id->kind >= CLUSTER_RECOVERY_FILE_CLOG_V2
		&& id->kind <= CLUSTER_RECOVERY_FILE_DEBT_V2
		&& id->origin < 128
		&& id->format_id == CLUSTER_RECOVERY_FORMAT_ID_V2
		&& id->format_version == CLUSTER_RECOVERY_FORMAT_VERSION_V2
		&& identity_counter(id->system_identifier)
		&& identity_counter(id->database_incarnation)
		&& !identity_zero(id->storage_uuid, sizeof(id->storage_uuid))
		&& !identity_zero(id->redo_branch_uuid, sizeof(id->redo_branch_uuid))
		&& identity_counter(id->redo_branch_generation)
		&& id->redo_timeline != 0 && id->redo_timeline != UINT32_MAX
		&& identity_counter(id->creation_generation)
		&& identity_counter(id->registration_generation);
}

static void
identity_put(uint8 *bytes, Size offset, uint64 value, unsigned width)
{
	for (unsigned i = 0; i < width; i++)
		bytes[offset+i] = (uint8) (value >> (8*(width-i-1)));
}

static uint64
identity_get(const uint8 *bytes, Size offset, unsigned width)
{
	uint64 value = 0;

	for (unsigned i = 0; i < width; i++)
		value = (value << 8) | bytes[offset+i];
	return value;
}

static uint32
identity_crc(const uint8 *bytes)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, ID_OFFSET(IDENTITY_CRC32C));
	FIN_CRC32C(crc);
	return (uint32) crc;
}

bool
cluster_recovery_file_identity_encode_v2(const ClusterRecoveryFileIdentityV2 *id,
									   uint8 *out, Size length)
{
	uint8 bytes[CLUSTER_RECOVERY_IDENTITY_REFERENCE_V2_BYTES] = {0};

	if (out == NULL || length != sizeof(bytes)
		|| identity_overlap(id, sizeof(*id), out, length) || !identity_valid(id))
		return false;
	memcpy(bytes + ID_OFFSET(MAGIC), "RFI2", 4);
	identity_put(bytes, ID_OFFSET(REFERENCE_VERSION), 2, 2);
	identity_put(bytes, ID_OFFSET(REFERENCE_BYTES), sizeof(bytes), 2);
	identity_put(bytes, ID_OFFSET(KIND), id->kind, 2);
	identity_put(bytes, ID_OFFSET(ORIGIN), id->origin, 2);
	identity_put(bytes, ID_OFFSET(FORMAT_ID), id->format_id, 4);
	identity_put(bytes, ID_OFFSET(FORMAT_VERSION), id->format_version, 4);
	identity_put(bytes, ID_OFFSET(REDO_TIMELINE), id->redo_timeline, 4);
	identity_put(bytes, ID_OFFSET(SYSTEM_IDENTIFIER), id->system_identifier, 8);
	identity_put(bytes, ID_OFFSET(DATABASE_INCARNATION), id->database_incarnation, 8);
	memcpy(bytes + ID_OFFSET(STORAGE_UUID), id->storage_uuid, sizeof(id->storage_uuid));
	memcpy(bytes + ID_OFFSET(REDO_BRANCH_UUID), id->redo_branch_uuid, sizeof(id->redo_branch_uuid));
	identity_put(bytes, ID_OFFSET(REDO_BRANCH_GENERATION), id->redo_branch_generation, 8);
	identity_put(bytes, ID_OFFSET(DATABASE_OID), id->database_oid, 4);
	identity_put(bytes, ID_OFFSET(TABLESPACE_OID), id->tablespace_oid, 4);
	identity_put(bytes, ID_OFFSET(OBJECT_ID), id->object_id, 8);
	identity_put(bytes, ID_OFFSET(CREATION_GENERATION), id->creation_generation, 8);
	identity_put(bytes, ID_OFFSET(REGISTRATION_GENERATION), id->registration_generation, 8);
	identity_put(bytes, ID_OFFSET(IDENTITY_CRC32C), identity_crc(bytes), 4);
	memcpy(out, bytes, sizeof(bytes));
	return true;
}

bool
cluster_recovery_file_identity_decode_v2(const uint8 *bytes, Size length,
									   ClusterRecoveryFileIdentityV2 *out)
{
	ClusterRecoveryFileIdentityV2 id;

	if (bytes == NULL || out == NULL || length != CLUSTER_RECOVERY_IDENTITY_REFERENCE_V2_BYTES
		|| identity_overlap(bytes, length, out, sizeof(*out)))
		return false;
	if (memcmp(bytes + ID_OFFSET(MAGIC), "RFI2", 4) != 0
		|| identity_get(bytes, ID_OFFSET(REFERENCE_VERSION), 2) != 2
		|| identity_get(bytes, ID_OFFSET(REFERENCE_BYTES), 2) != length
		|| !identity_zero(bytes + ID_OFFSET(RESERVED_ZERO), 12)
		|| identity_get(bytes, ID_OFFSET(IDENTITY_CRC32C), 4) != identity_crc(bytes))
		return false;
	memset(&id, 0, sizeof(id));
	id.kind = (ClusterRecoveryFileKindV2) identity_get(bytes, ID_OFFSET(KIND), 2);
	id.origin = (uint16) identity_get(bytes, ID_OFFSET(ORIGIN), 2);
	id.format_id = (uint32) identity_get(bytes, ID_OFFSET(FORMAT_ID), 4);
	id.format_version = (uint32) identity_get(bytes, ID_OFFSET(FORMAT_VERSION), 4);
	id.redo_timeline = (TimeLineID) identity_get(bytes, ID_OFFSET(REDO_TIMELINE), 4);
	id.system_identifier = identity_get(bytes, ID_OFFSET(SYSTEM_IDENTIFIER), 8);
	id.database_incarnation = identity_get(bytes, ID_OFFSET(DATABASE_INCARNATION), 8);
	memcpy(id.storage_uuid, bytes + ID_OFFSET(STORAGE_UUID), sizeof(id.storage_uuid));
	memcpy(id.redo_branch_uuid, bytes + ID_OFFSET(REDO_BRANCH_UUID), sizeof(id.redo_branch_uuid));
	id.redo_branch_generation = identity_get(bytes, ID_OFFSET(REDO_BRANCH_GENERATION), 8);
	id.database_oid = (Oid) identity_get(bytes, ID_OFFSET(DATABASE_OID), 4);
	id.tablespace_oid = (Oid) identity_get(bytes, ID_OFFSET(TABLESPACE_OID), 4);
	id.object_id = identity_get(bytes, ID_OFFSET(OBJECT_ID), 8);
	id.creation_generation = identity_get(bytes, ID_OFFSET(CREATION_GENERATION), 8);
	id.registration_generation = identity_get(bytes, ID_OFFSET(REGISTRATION_GENERATION), 8);
	if (!identity_valid(&id))
		return false;
	*out = id;
	return true;
}

bool
cluster_recovery_file_identity_matches_v2(const uint8 *bytes, Size length,
										const ClusterRecoveryFileIdentityV2 *expected)
{
	uint8 canonical[CLUSTER_RECOVERY_IDENTITY_REFERENCE_V2_BYTES];

	if (bytes == NULL || expected == NULL || length != sizeof(canonical)
		|| identity_overlap(bytes, length, expected, sizeof(*expected)))
		return false;
	return cluster_recovery_file_identity_encode_v2(expected, canonical, sizeof(canonical))
		&& memcmp(bytes, canonical, sizeof(canonical)) == 0;
}
