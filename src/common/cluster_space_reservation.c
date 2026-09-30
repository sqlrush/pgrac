/*-------------------------------------------------------------------------
 * cluster_space_reservation.c
 *    Sequential reservation representation and exact byte transitions.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "cluster/cluster_space_reservation.h"

#ifdef USE_PGRAC_CLUSTER
#include "port/pg_crc32c.h"
#include "storage/bufpage.h"

StaticAssertDecl(SizeOfPageHeaderData == 32, "SPACE reservation requires the cluster page header");
StaticAssertDecl(BLCKSZ == 8192, "SPACE reservation page size");

static uint64
get_le(const uint8 *bytes, size_t width)
{
	uint64 value = 0;

	for (size_t i = 0; i < width; i++)
		value |= (uint64) bytes[i] << (8 * i);
	return value;
}

static void
put_le(uint8 *bytes, uint64 value, size_t width)
{
	for (size_t i = 0; i < width; i++)
		bytes[i] = (uint8) (value >> (8 * i));
}

static bool
all_zero(const void *input, size_t length)
{
	const uint8 *bytes = input;

	for (size_t i = 0; i < length; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

static uint32
reservation_crc(const uint8 *bytes)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 156);
	FIN_CRC32C(crc);
	return crc;
}

bool
cluster_space_reservation_encode(const ClusterSpaceReservation *state, void *bytes, size_t length)
{
	uint8 encoded[CLUSTER_SPACE_RESERVATION_BYTES] = {0};

	if (state == NULL || bytes == NULL || length != sizeof(encoded)
		|| !cluster_space_identity_encode(&state->identity, encoded + 24,
										  CLUSTER_SPACE_IDENTITY_BYTES))
		return false;
	put_le(encoded, CLUSTER_SPACE_RESERVATION_MAGIC, 4);
	put_le(encoded + 4, CLUSTER_SPACE_RESERVATION_FORMAT, 2);
	put_le(encoded + 6, sizeof(encoded), 2);
	put_le(encoded + 8, CLUSTER_SPACE_RESERVATION_PROFILE, 4);
	put_le(encoded + 12, MAIN_FORKNUM, 4);
	put_le(encoded + 16, state->next_block, 4);
	put_le(encoded + 156, reservation_crc(encoded), 4);
	memcpy(bytes, encoded, sizeof(encoded));
	return true;
}

static bool
decode_payload(const uint8 *bytes, size_t length, const ClusterSpaceIdentityKey *expected,
			   ClusterSpaceReservation *out)
{
	ClusterSpaceReservation decoded = {0};
	ClusterSpaceIdentityKey embedded = {0};
	const uint8 *identity;

	if (bytes == NULL || length != CLUSTER_SPACE_RESERVATION_BYTES
		|| get_le(bytes, 4) != CLUSTER_SPACE_RESERVATION_MAGIC
		|| get_le(bytes + 4, 2) != CLUSTER_SPACE_RESERVATION_FORMAT
		|| get_le(bytes + 6, 2) != CLUSTER_SPACE_RESERVATION_BYTES
		|| get_le(bytes + 8, 4) != CLUSTER_SPACE_RESERVATION_PROFILE
		|| get_le(bytes + 12, 4) != MAIN_FORKNUM || !all_zero(bytes + 20, 4)
		|| !all_zero(bytes + 152, 4) || get_le(bytes + 156, 4) != reservation_crc(bytes))
		return false;
	identity = bytes + 24;
	if (expected == NULL) {
		/* Unbound parsing is only used by representation/WAL validation. These
		 * untrusted fields confer no namespace or mutation authority. */
		embedded.system_identifier = get_le(identity + 8, 8);
		embedded.database_incarnation = get_le(identity + 16, 8);
		memcpy(embedded.storage_uuid, identity + 24, 16);
		embedded.locator.spcOid = (Oid) get_le(identity + 40, 4);
		embedded.locator.dbOid = (Oid) get_le(identity + 44, 4);
		embedded.locator.relNumber = (RelFileNumber) get_le(identity + 48, 4);
		expected = &embedded;
	}
	if (!cluster_space_identity_decode(identity, CLUSTER_SPACE_IDENTITY_BYTES, expected,
									  &decoded.identity))
		return false;
	decoded.next_block = (BlockNumber) get_le(bytes + 16, 4);
	*out = decoded;
	return true;
}

bool
cluster_space_reservation_decode(const void *bytes, size_t length,
								 const ClusterSpaceIdentityKey *expected, ClusterSpaceReservation *out)
{
	return expected != NULL && out != NULL && decode_payload(bytes, length, expected, out);
}

bool
cluster_space_reservation_page_encode(const ClusterSpaceReservation *state, uint64 token,
									  void *page, size_t length)
{
	PGAlignedBlock encoded = {0};
	PageHeader header = (PageHeader) encoded.data;

	if (page == NULL || length != BLCKSZ || token == 0
		|| !cluster_space_reservation_encode(state, encoded.data + SizeOfPageHeaderData,
											CLUSTER_SPACE_RESERVATION_BYTES))
		return false;
	header->pd_flags = PD_SPACE_METADATA;
	header->pd_lower = SizeOfPageHeaderData + CLUSTER_SPACE_RESERVATION_BYTES;
	header->pd_upper = BLCKSZ;
	header->pd_special = BLCKSZ;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header->pd_block_scn = token;
	memcpy(page, encoded.data, BLCKSZ);
	return true;
}

static bool
decode_page(const void *page, size_t length, const ClusterSpaceIdentityKey *expected,
			ClusterSpaceReservation *out, uint64 *token)
{
	const uint8 *bytes = page;
	PageHeaderData header = {0};
	ClusterSpaceReservation decoded;
	const uint16 allowed
		= PD_SPACE_METADATA | PD_CLUSTER_FORCE_FPI | PD_LSN_ORIGIN_VALID | PD_LSN_ORIGIN_MASK;
	const size_t end = SizeOfPageHeaderData + CLUSTER_SPACE_RESERVATION_BYTES;

	if (page == NULL || length != BLCKSZ)
		return false;
	memcpy(&header, page, SizeOfPageHeaderData);
	if ((header.pd_flags & PD_SPACE_METADATA) == 0 || (header.pd_flags & ~allowed) != 0
		|| ((header.pd_flags & PD_LSN_ORIGIN_VALID) == 0
			&& (header.pd_flags & PD_LSN_ORIGIN_MASK) != 0)
		|| header.pd_lower != end || header.pd_upper != BLCKSZ || header.pd_special != BLCKSZ
		|| header.pd_pagesize_version != (BLCKSZ | PG_PAGE_LAYOUT_VERSION)
		|| header.pd_prune_xid != InvalidTransactionId || header.pd_block_scn == 0
		|| !all_zero(bytes + end, BLCKSZ - end)
		|| !decode_payload(bytes + SizeOfPageHeaderData, CLUSTER_SPACE_RESERVATION_BYTES,
						   expected, &decoded))
		return false;
	*out = decoded;
	*token = header.pd_block_scn;
	return true;
}

bool
cluster_space_reservation_page_valid(const void *page, size_t length)
{
	ClusterSpaceReservation decoded;
	uint64 token;

	return decode_page(page, length, NULL, &decoded, &token);
}

bool
cluster_space_reservation_page_decode(const void *page, size_t length, ForkNumber forknum,
									  BlockNumber block, const ClusterSpaceIdentityKey *expected,
									  ClusterSpaceReservation *out, uint64 *token)
{
	return expected != NULL && out != NULL && token != NULL && forknum == SPACE_FORKNUM
		&& block == CLUSTER_SPACE_RESERVATION_BLOCK
		&& decode_page(page, length, expected, out, token);
}

static bool
reservation_empty(const ClusterSpaceReservation *state)
{
	const ClusterSpaceIdentity *identity = &state->identity;

	return state->next_block == 0 && identity->key.system_identifier == 0
		&& identity->key.database_incarnation == 0 && all_zero(identity->key.storage_uuid, 16)
		&& identity->key.locator.spcOid == 0 && identity->key.locator.dbOid == 0
		&& identity->key.locator.relNumber == 0 && all_zero(identity->incarnation, 16)
		&& identity->sequence == 0 && identity->operation == 0 && identity->state == 0;
}

static bool
change_valid(const ClusterSpaceReservationChange *change)
{
	uint8 before[CLUSTER_SPACE_RESERVATION_BYTES];
	uint8 result[CLUSTER_SPACE_RESERVATION_BYTES];

	if (change == NULL || change->result_token == 0 || change->result_token == change->before_token
		|| !cluster_space_reservation_encode(&change->result, result, sizeof(result)))
		return false;
	if (change->action == CLUSTER_SPACE_RESERVATION_INIT)
		return change->before_token == 0 && reservation_empty(&change->before)
			&& change->first_block == 0 && change->granted == 0 && change->result.next_block == 0
			&& change->result.identity.state == CLUSTER_SPACE_IDENTITY_LIVE
			&& change->result.identity.sequence == 1;
	if (change->before_token == 0
		|| !cluster_space_reservation_encode(&change->before, before, sizeof(before)))
		return false;
	if (change->action == CLUSTER_SPACE_RESERVATION_ADVANCE)
		return change->before.identity.state == CLUSTER_SPACE_IDENTITY_LIVE
			&& memcmp(before + 24, result + 24, CLUSTER_SPACE_IDENTITY_BYTES) == 0
			&& change->first_block == change->before.next_block && change->granted != 0
			&& (uint64) change->first_block + change->granted == change->result.next_block;
	if (change->granted != 0
		|| cluster_space_identity_transition(&change->before.identity, &change->before.identity,
											 &change->result.identity) != CLUSTER_SPACE_IDENTITY_APPLY)
		return false;
	if (change->action == CLUSTER_SPACE_RESERVATION_RESET)
		return change->result.identity.state == CLUSTER_SPACE_IDENTITY_LIVE
			&& change->result.next_block <= change->before.next_block
			&& change->first_block == change->result.next_block;
	if (change->action == CLUSTER_SPACE_RESERVATION_TOMBSTONE)
		return change->result.identity.state == CLUSTER_SPACE_IDENTITY_TOMBSTONED
			&& change->result.next_block == change->before.next_block && change->first_block == 0;
	return false;
}

bool
cluster_space_reservation_wal_encode(const ClusterSpaceReservationChange *change,
									 void *bytes, size_t length)
{
	uint8 encoded[CLUSTER_SPACE_RESERVATION_WAL_BYTES] = {0};

	if (bytes == NULL || length != sizeof(encoded) || !change_valid(change))
		return false;
	put_le(encoded, CLUSTER_SPACE_RESERVATION_WAL_MAGIC, 4);
	put_le(encoded + 4, 1, 2);
	put_le(encoded + 6, sizeof(encoded), 2);
	put_le(encoded + 8, change->action, 4);
	put_le(encoded + 12, change->first_block, 4);
	put_le(encoded + 16, change->granted, 4);
	put_le(encoded + 24, change->before_token, 8);
	put_le(encoded + 32, change->result_token, 8);
	if (change->action != CLUSTER_SPACE_RESERVATION_INIT
		&& !cluster_space_reservation_encode(&change->before, encoded + 48,
											CLUSTER_SPACE_RESERVATION_BYTES))
		return false;
	if (!cluster_space_reservation_encode(&change->result, encoded + 208,
										 CLUSTER_SPACE_RESERVATION_BYTES))
		return false;
	memcpy(bytes, encoded, sizeof(encoded));
	return true;
}

bool
cluster_space_reservation_wal_decode(const void *bytes, size_t length,
									 ClusterSpaceReservationChange *out)
{
	const uint8 *encoded = bytes;
	ClusterSpaceReservationChange decoded = {0};

	if (bytes == NULL || out == NULL || length != CLUSTER_SPACE_RESERVATION_WAL_BYTES
		|| get_le(encoded, 4) != CLUSTER_SPACE_RESERVATION_WAL_MAGIC
		|| get_le(encoded + 4, 2) != 1 || get_le(encoded + 6, 2) != CLUSTER_SPACE_RESERVATION_WAL_BYTES
		|| !all_zero(encoded + 20, 4) || !all_zero(encoded + 40, 8))
		return false;
	decoded.action = (ClusterSpaceReservationAction) get_le(encoded + 8, 4);
	decoded.first_block = (BlockNumber) get_le(encoded + 12, 4);
	decoded.granted = (uint32) get_le(encoded + 16, 4);
	decoded.before_token = get_le(encoded + 24, 8);
	decoded.result_token = get_le(encoded + 32, 8);
	if (decoded.action == CLUSTER_SPACE_RESERVATION_INIT) {
		if (!all_zero(encoded + 48, CLUSTER_SPACE_RESERVATION_BYTES))
			return false;
	} else if (!decode_payload(encoded + 48, CLUSTER_SPACE_RESERVATION_BYTES, NULL, &decoded.before))
		return false;
	if (!decode_payload(encoded + 208, CLUSTER_SPACE_RESERVATION_BYTES, NULL, &decoded.result)
		|| !change_valid(&decoded))
		return false;
	*out = decoded;
	return true;
}

static bool
reservation_equal(const ClusterSpaceReservation *left, const ClusterSpaceReservation *right)
{
	uint8 left_bytes[CLUSTER_SPACE_RESERVATION_BYTES];
	uint8 right_bytes[CLUSTER_SPACE_RESERVATION_BYTES];

	return cluster_space_reservation_encode(left, left_bytes, sizeof(left_bytes))
		&& cluster_space_reservation_encode(right, right_bytes, sizeof(right_bytes))
		&& memcmp(left_bytes, right_bytes, sizeof(left_bytes)) == 0;
}

ClusterSpaceIdentityTransition
cluster_space_reservation_apply(const ClusterSpaceReservationChange *change,
								const ClusterSpaceIdentityKey *expected, void *page, size_t length)
{
	ClusterSpaceReservation current;
	ClusterSpaceIdentity checked;
	uint8 identity_bytes[CLUSTER_SPACE_IDENTITY_BYTES];
	uint64 token;

	if (page == NULL || length != BLCKSZ || expected == NULL || !change_valid(change))
		return CLUSTER_SPACE_IDENTITY_INVALID;
	checked = change->result.identity;
	checked.key = *expected;
	if (!cluster_space_identity_encode(&checked, identity_bytes, sizeof(identity_bytes)))
		return CLUSTER_SPACE_IDENTITY_INVALID;
	if (!cluster_space_identity_decode(identity_bytes, sizeof(identity_bytes),
									  &change->result.identity.key, &checked))
		return CLUSTER_SPACE_IDENTITY_MISMATCH;
	if (all_zero(page, BLCKSZ)) {
		if (change->action != CLUSTER_SPACE_RESERVATION_INIT)
			return CLUSTER_SPACE_IDENTITY_MISMATCH;
	} else {
		if (!decode_page(page, BLCKSZ, expected, &current, &token))
			return CLUSTER_SPACE_IDENTITY_INVALID;
		if (token == change->result_token && reservation_equal(&current, &change->result))
			return CLUSTER_SPACE_IDENTITY_ALREADY;
		if (change->action == CLUSTER_SPACE_RESERVATION_INIT || token != change->before_token
			|| !reservation_equal(&current, &change->before))
			return CLUSTER_SPACE_IDENTITY_MISMATCH;
	}
	if (!cluster_space_reservation_page_encode(&change->result, change->result_token, page, length))
		return CLUSTER_SPACE_IDENTITY_INVALID;
	return CLUSTER_SPACE_IDENTITY_APPLY;
}
#endif
