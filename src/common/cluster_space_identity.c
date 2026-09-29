/*-------------------------------------------------------------------------
 *
 * cluster_space_identity.c
 *    Allocation-free SPACE identity codec and exact replay transitions.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/common/cluster_space_identity.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "cluster/cluster_space_identity.h"

#ifdef USE_PGRAC_CLUSTER
#include "catalog/pg_tablespace_d.h"
#include "port/pg_crc32c.h"
#include "storage/bufpage.h"

StaticAssertDecl(SizeOfPageHeaderData == 32, "SPACE identity requires the cluster page header");
StaticAssertDecl(BLCKSZ == 8192, "SPACE identity page size");
StaticAssertDecl((PD_SPACE_METADATA & (PD_LSN_ORIGIN_VALID | PD_LSN_ORIGIN_MASK)) == 0,
				 "SPACE page type overlaps page LSN origin");

static uint64
get_le(const uint8 *bytes, size_t width)
{
	uint64 value = 0;
	size_t i;

	for (i = 0; i < width; i++)
		value |= (uint64)bytes[i] << (8 * i);
	return value;
}

static void
put_le(uint8 *bytes, uint64 value, size_t width)
{
	size_t i;

	for (i = 0; i < width; i++)
		bytes[i] = (uint8)(value >> (8 * i));
}

static bool
all_zero(const uint8 *bytes, size_t length)
{
	size_t i;

	for (i = 0; i < length; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

static bool
key_valid(const ClusterSpaceIdentityKey *key)
{
	return key != NULL && key->system_identifier != 0 && key->database_incarnation != 0
		   && !all_zero(key->storage_uuid, 16) && key->locator.spcOid != InvalidOid
		   && key->locator.relNumber != InvalidRelFileNumber
		   && ((key->locator.spcOid == GLOBALTABLESPACE_OID) == (key->locator.dbOid == InvalidOid));
}

static bool
key_equal(const ClusterSpaceIdentityKey *left, const ClusterSpaceIdentityKey *right)
{
	return left->system_identifier == right->system_identifier
		   && left->database_incarnation == right->database_incarnation
		   && memcmp(left->storage_uuid, right->storage_uuid, 16) == 0
		   && RelFileLocatorEquals(left->locator, right->locator);
}

static bool
identity_valid(const ClusterSpaceIdentity *identity)
{
	return identity != NULL && key_valid(&identity->key) && !all_zero(identity->incarnation, 16)
		   && identity->sequence != 0 && identity->operation != 0
		   && (identity->state == CLUSTER_SPACE_IDENTITY_LIVE
			   || identity->state == CLUSTER_SPACE_IDENTITY_TOMBSTONED);
}

static bool
identity_equal(const ClusterSpaceIdentity *left, const ClusterSpaceIdentity *right)
{
	return key_equal(&left->key, &right->key)
		   && memcmp(left->incarnation, right->incarnation, 16) == 0
		   && left->sequence == right->sequence && left->operation == right->operation
		   && left->state == right->state;
}

static uint32
identity_crc(const uint8 *bytes)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 124);
	FIN_CRC32C(crc);
	return crc;
}

bool
cluster_space_identity_encode(const ClusterSpaceIdentity *identity, void *bytes, size_t length)
{
	uint8 encoded[CLUSTER_SPACE_IDENTITY_BYTES];

	if (bytes == NULL || length != sizeof(encoded) || !identity_valid(identity))
		return false;
	memset(encoded, 0, sizeof(encoded));
	put_le(encoded, CLUSTER_SPACE_IDENTITY_MAGIC, 4);
	put_le(encoded + 4, CLUSTER_SPACE_IDENTITY_FORMAT, 2);
	put_le(encoded + 6, sizeof(encoded), 2);
	put_le(encoded + 8, identity->key.system_identifier, 8);
	put_le(encoded + 16, identity->key.database_incarnation, 8);
	memcpy(encoded + 24, identity->key.storage_uuid, 16);
	put_le(encoded + 40, identity->key.locator.spcOid, 4);
	put_le(encoded + 44, identity->key.locator.dbOid, 4);
	put_le(encoded + 48, identity->key.locator.relNumber, 4);
	put_le(encoded + 52, CLUSTER_SPACE_IDENTITY_PROFILE, 4);
	memcpy(encoded + 56, identity->incarnation, 16);
	put_le(encoded + 72, identity->sequence, 8);
	put_le(encoded + 80, identity->operation, 8);
	put_le(encoded + 88, identity->state, 4);
	put_le(encoded + 124, identity_crc(encoded), 4);
	memcpy(bytes, encoded, sizeof(encoded));
	return true;
}

static bool
decode_payload(const uint8 *bytes, size_t length, ClusterSpaceIdentity *out)
{
	ClusterSpaceIdentity decoded;

	if (bytes == NULL || length != CLUSTER_SPACE_IDENTITY_BYTES
		|| get_le(bytes, 4) != CLUSTER_SPACE_IDENTITY_MAGIC
		|| get_le(bytes + 4, 2) != CLUSTER_SPACE_IDENTITY_FORMAT
		|| get_le(bytes + 6, 2) != CLUSTER_SPACE_IDENTITY_BYTES
		|| get_le(bytes + 52, 4) != CLUSTER_SPACE_IDENTITY_PROFILE || !all_zero(bytes + 92, 32)
		|| get_le(bytes + 124, 4) != identity_crc(bytes))
		return false;
	memset(&decoded, 0, sizeof(decoded));
	decoded.key.system_identifier = get_le(bytes + 8, 8);
	decoded.key.database_incarnation = get_le(bytes + 16, 8);
	memcpy(decoded.key.storage_uuid, bytes + 24, 16);
	decoded.key.locator.spcOid = (Oid)get_le(bytes + 40, 4);
	decoded.key.locator.dbOid = (Oid)get_le(bytes + 44, 4);
	decoded.key.locator.relNumber = (RelFileNumber)get_le(bytes + 48, 4);
	memcpy(decoded.incarnation, bytes + 56, 16);
	decoded.sequence = get_le(bytes + 72, 8);
	decoded.operation = get_le(bytes + 80, 8);
	decoded.state = (ClusterSpaceIdentityState)get_le(bytes + 88, 4);
	if (!identity_valid(&decoded))
		return false;
	*out = decoded;
	return true;
}

bool
cluster_space_identity_decode(const void *bytes, size_t length,
							  const ClusterSpaceIdentityKey *expected, ClusterSpaceIdentity *out)
{
	ClusterSpaceIdentity decoded;

	if (out == NULL || !key_valid(expected) || !decode_payload(bytes, length, &decoded)
		|| !key_equal(&decoded.key, expected))
		return false;
	*out = decoded;
	return true;
}

bool
cluster_space_identity_page_encode(const ClusterSpaceIdentity *identity, uint64 mutation_token,
								   void *page, size_t length)
{
	PGAlignedBlock encoded;
	PageHeader header = (PageHeader)encoded.data;

	if (page == NULL || length != BLCKSZ || mutation_token == 0)
		return false;
	memset(&encoded, 0, sizeof(encoded));
	if (!cluster_space_identity_encode(identity, encoded.data + SizeOfPageHeaderData,
									   CLUSTER_SPACE_IDENTITY_BYTES))
		return false;
	header->pd_flags = PD_SPACE_METADATA;
	header->pd_lower = SizeOfPageHeaderData + CLUSTER_SPACE_IDENTITY_BYTES;
	header->pd_upper = BLCKSZ;
	header->pd_special = BLCKSZ;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header->pd_block_scn = mutation_token;
	memcpy(page, encoded.data, BLCKSZ);
	return true;
}

static bool
decode_page(const void *page, size_t length, ClusterSpaceIdentity *out, uint64 *mutation_token)
{
	const uint8 *bytes = page;
	PageHeaderData header;
	const uint16 allowed
		= PD_SPACE_METADATA | PD_CLUSTER_FORCE_FPI | PD_LSN_ORIGIN_VALID | PD_LSN_ORIGIN_MASK;
	const size_t end = SizeOfPageHeaderData + CLUSTER_SPACE_IDENTITY_BYTES;

	if (page == NULL || length != BLCKSZ)
		return false;
	memset(&header, 0, sizeof(header));
	memcpy(&header, page, SizeOfPageHeaderData);
	if ((header.pd_flags & PD_SPACE_METADATA) == 0 || (header.pd_flags & ~allowed) != 0
		|| ((header.pd_flags & PD_LSN_ORIGIN_VALID) == 0
			&& (header.pd_flags & PD_LSN_ORIGIN_MASK) != 0)
		|| header.pd_lower != end || header.pd_upper != BLCKSZ || header.pd_special != BLCKSZ
		|| header.pd_pagesize_version != (BLCKSZ | PG_PAGE_LAYOUT_VERSION)
		|| header.pd_prune_xid != InvalidTransactionId || header.pd_block_scn == 0
		|| !all_zero(bytes + end, BLCKSZ - end)
		|| !decode_payload(bytes + SizeOfPageHeaderData, CLUSTER_SPACE_IDENTITY_BYTES, out))
		return false;
	*mutation_token = header.pd_block_scn;
	return true;
}

bool
cluster_space_identity_page_valid(const void *page, size_t length)
{
	ClusterSpaceIdentity decoded;
	uint64 token;

	return decode_page(page, length, &decoded, &token);
}

bool
cluster_space_identity_page_decode(const void *page, size_t length, ForkNumber forknum,
								   BlockNumber block, const ClusterSpaceIdentityKey *expected,
								   ClusterSpaceIdentity *out, uint64 *mutation_token)
{
	ClusterSpaceIdentity decoded;
	uint64 token;

	if (out == NULL || mutation_token == NULL || forknum != SPACE_FORKNUM || block != 0
		|| !key_valid(expected) || !decode_page(page, length, &decoded, &token)
		|| !key_equal(&decoded.key, expected))
		return false;
	*out = decoded;
	*mutation_token = token;
	return true;
}

ClusterSpaceIdentityTransition
cluster_space_identity_transition(const ClusterSpaceIdentity *current,
								  const ClusterSpaceIdentity *expected,
								  const ClusterSpaceIdentity *result)
{
	bool same_incarnation;

	if (!identity_valid(current) || !identity_valid(expected) || !identity_valid(result)
		|| !key_equal(&expected->key, &result->key)
		|| expected->state != CLUSTER_SPACE_IDENTITY_LIVE || expected->sequence == UINT64_MAX
		|| result->sequence != expected->sequence + 1 || result->operation == expected->operation)
		return CLUSTER_SPACE_IDENTITY_INVALID;
	same_incarnation = memcmp(expected->incarnation, result->incarnation, 16) == 0;
	if ((result->state == CLUSTER_SPACE_IDENTITY_LIVE && same_incarnation)
		|| (result->state == CLUSTER_SPACE_IDENTITY_TOMBSTONED && !same_incarnation))
		return CLUSTER_SPACE_IDENTITY_INVALID;
	if (identity_equal(current, result))
		return CLUSTER_SPACE_IDENTITY_ALREADY;
	if (identity_equal(current, expected))
		return CLUSTER_SPACE_IDENTITY_APPLY;
	return CLUSTER_SPACE_IDENTITY_MISMATCH;
}
#endif /* USE_PGRAC_CLUSTER */
