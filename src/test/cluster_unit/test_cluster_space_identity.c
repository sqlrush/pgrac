/*-------------------------------------------------------------------------
 *
 * test_cluster_space_identity.c
 *    Persistent SPACE identity bytes, namespace and replay transitions.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_space_identity.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_space_identity.h"
#include "port/pg_crc32c.h"
#include "storage/bufpage.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterSpaceIdentity
identity(void)
{
	ClusterSpaceIdentity id;

	memset(&id, 0, sizeof(id));
	id.key.system_identifier = UINT64_C(0x0807060504030201);
	id.key.database_incarnation = 9;
	memset(id.key.storage_uuid, 0xa7, 16);
	id.key.locator.spcOid = DEFAULTTABLESPACE_OID;
	id.key.locator.dbOid = 5;
	id.key.locator.relNumber = 16384;
	memset(id.incarnation, 0x31, 16);
	id.sequence = 1;
	id.operation = 123;
	id.state = CLUSTER_SPACE_IDENTITY_LIVE;
	return id;
}

static void
repair_crc(uint8 *bytes)
{
	pg_crc32c crc;
	int i;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 124);
	FIN_CRC32C(crc);
	for (i = 0; i < 4; i++)
		bytes[124 + i] = (uint8)(crc >> (8 * i));
}

UT_TEST(test_literal_encoding_and_unaligned_decode)
{
	ClusterSpaceIdentity id = identity();
	ClusterSpaceIdentity out;
	uint8 storage[129];
	uint8 *bytes = storage + 1;
	const uint8 prefix[] = { 'P', 'S', 'I', '1', 1, 0, 128, 0, 1, 2, 3, 4, 5, 6, 7, 8 };

	UT_ASSERT(cluster_space_identity_encode(&id, bytes, 128));
	UT_ASSERT(memcmp(bytes, prefix, sizeof(prefix)) == 0);
	UT_ASSERT_EQ(bytes[16], 9);
	UT_ASSERT_EQ(bytes[40], 0x7f); /* 1663, little endian. */
	UT_ASSERT_EQ(bytes[41], 6);
	UT_ASSERT_EQ(bytes[48], 0);
	UT_ASSERT_EQ(bytes[49], 0x40);
	UT_ASSERT_EQ(bytes[52], 1);
	UT_ASSERT_EQ(bytes[56], 0x31);
	UT_ASSERT_EQ(bytes[72], 1);
	UT_ASSERT_EQ(bytes[80], 123);
	UT_ASSERT_EQ(bytes[88], 1);
	UT_ASSERT(cluster_space_identity_decode(bytes, 128, &id.key, &out));
	UT_ASSERT_EQ(out.operation, 123);
	UT_ASSERT_EQ(out.sequence, 1);
	UT_ASSERT(memcmp(out.incarnation, id.incarnation, 16) == 0);
}

UT_TEST(test_every_payload_byte_tear_rejected_without_output)
{
	ClusterSpaceIdentity id = identity();
	ClusterSpaceIdentity out;
	ClusterSpaceIdentity sentinel;
	uint8 valid[128];
	uint8 broken[128];
	int i;

	UT_ASSERT(cluster_space_identity_encode(&id, valid, sizeof(valid)));
	memset(&sentinel, 0xa5, sizeof(sentinel));
	for (i = 0; i < 128; i++) {
		memcpy(broken, valid, 128);
		broken[i] ^= 1;
		out = sentinel;
		UT_ASSERT(!cluster_space_identity_decode(broken, 128, &id.key, &out));
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	}
}

UT_TEST(test_crc_valid_unknown_fields_rejected)
{
	ClusterSpaceIdentity id = identity();
	ClusterSpaceIdentity out;
	uint8 bytes[128];
	const size_t offsets[] = { 0, 4, 6, 52, 53, 88, 89, 92, 111, 123 };
	size_t i;

	for (i = 0; i < lengthof(offsets); i++) {
		UT_ASSERT(cluster_space_identity_encode(&id, bytes, 128));
		bytes[offsets[i]] = 0x7f;
		repair_crc(bytes);
		UT_ASSERT(!cluster_space_identity_decode(bytes, 128, &id.key, &out));
	}
}

UT_TEST(test_exact_namespace_and_all_uuid_bytes)
{
	ClusterSpaceIdentity id = identity();
	ClusterSpaceIdentity out;
	ClusterSpaceIdentityKey wrong;
	uint8 bytes[128];
	int field;

	UT_ASSERT(cluster_space_identity_encode(&id, bytes, 128));
	for (field = 0; field < 21; field++) {
		wrong = id.key;
		if (field == 0)
			wrong.system_identifier++;
		else if (field == 1)
			wrong.database_incarnation++;
		else if (field == 2)
			wrong.locator.spcOid++;
		else if (field == 3)
			wrong.locator.dbOid++;
		else if (field == 4)
			wrong.locator.relNumber++;
		else
			wrong.storage_uuid[field - 5] ^= 1;
		UT_ASSERT(!cluster_space_identity_decode(bytes, 128, &wrong, &out));
	}
	UT_ASSERT(!cluster_space_identity_decode(bytes, 128, NULL, &out));
}

UT_TEST(test_invalid_identity_and_sizes_do_not_write)
{
	ClusterSpaceIdentity id = identity();
	ClusterSpaceIdentity bad;
	uint8 bytes[129];
	uint8 before[129];
	int field;

	memset(before, 0x5a, sizeof(before));
	for (field = 0; field < 10; field++) {
		bad = id;
		switch (field) {
		case 0:
			bad.key.system_identifier = 0;
			break;
		case 1:
			bad.key.database_incarnation = 0;
			break;
		case 2:
			memset(bad.key.storage_uuid, 0, 16);
			break;
		case 3:
			bad.key.locator.spcOid = 0;
			break;
		case 4:
			bad.key.locator.dbOid = 0;
			break;
		case 5:
			bad.key.locator.relNumber = 0;
			break;
		case 6:
			memset(bad.incarnation, 0, 16);
			break;
		case 7:
			bad.sequence = 0;
			break;
		case 8:
			bad.operation = 0;
			break;
		case 9:
			bad.state = 0;
			break;
		}
		memcpy(bytes, before, sizeof(bytes));
		UT_ASSERT(!cluster_space_identity_encode(&bad, bytes, 128));
		UT_ASSERT(memcmp(bytes, before, sizeof(bytes)) == 0);
	}
	UT_ASSERT(!cluster_space_identity_encode(&id, bytes, 127));
	UT_ASSERT(!cluster_space_identity_encode(&id, bytes, 129));
	UT_ASSERT(!cluster_space_identity_encode(NULL, bytes, 128));
	UT_ASSERT(!cluster_space_identity_encode(&id, NULL, 128));
	UT_ASSERT(memcmp(bytes, before, sizeof(bytes)) == 0);
	id.key.locator.spcOid = GLOBALTABLESPACE_OID;
	UT_ASSERT(!cluster_space_identity_encode(&id, bytes, 128));
	id.key.locator.dbOid = 0;
	UT_ASSERT(cluster_space_identity_encode(&id, bytes, 128));
}

UT_TEST(test_page_roundtrip_and_origin_flags)
{
	ClusterSpaceIdentity id = identity();
	ClusterSpaceIdentity out;
	PGAlignedBlock page;
	PageHeader header = (PageHeader)page.data;
	uint64 token = 0;
	int node;

	UT_ASSERT(cluster_space_identity_page_encode(&id, 55, page.data, BLCKSZ));
	UT_ASSERT_EQ(header->pd_lower, 160);
	UT_ASSERT_EQ(header->pd_special, BLCKSZ);
	UT_ASSERT_EQ(header->pd_flags, 0x0800);
	for (node = 0; node < 16; node++) {
		header->pd_flags = 0x0800 | 0x0020 | 0x0040 | (node << 7);
		UT_ASSERT(cluster_space_identity_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 0, &id.key,
													 &out, &token));
		UT_ASSERT_EQ(token, 55);
		UT_ASSERT(memcmp(out.incarnation, id.incarnation, 16) == 0);
	}
}

UT_TEST(test_page_fork_block_header_and_tail_rejected)
{
	ClusterSpaceIdentity id = identity();
	ClusterSpaceIdentity out;
	ClusterSpaceIdentity sentinel;
	PGAlignedBlock page;
	PGAlignedBlock valid;
	PageHeader header = (PageHeader)page.data;
	const size_t damage[] = { 10, 12, 14, 16, 18, 20, 24, 32, 160, BLCKSZ - 1 };
	uint64 token = 987;
	size_t i;

	UT_ASSERT(cluster_space_identity_page_encode(&id, 55, valid.data, BLCKSZ));
	memset(&sentinel, 0xa5, sizeof(sentinel));
	for (i = 0; i < lengthof(damage); i++) {
		page = valid;
		page.data[damage[i]] ^= 0x40;
		if (damage[i] == 10)
			header->pd_flags |= PD_HAS_ITL;
		/* A changed opaque token is not independently a malformed page. */
		if (damage[i] == 24)
			header->pd_block_scn = 0;
		out = sentinel;
		UT_ASSERT(!cluster_space_identity_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 0, &id.key,
													  &out, &token));
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
		UT_ASSERT_EQ(token, 987);
	}
	page = valid;
	UT_ASSERT(!cluster_space_identity_page_decode(page.data, BLCKSZ, MAIN_FORKNUM, 0, &id.key, &out,
												  &token));
	UT_ASSERT(!cluster_space_identity_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 1, &id.key,
												  &out, &token));
	UT_ASSERT(!cluster_space_identity_page_valid(page.data, BLCKSZ - 1));
	header->pd_flags = 0x0040; /* Existing LSN origin is not SPACE. */
	UT_ASSERT(!cluster_space_identity_page_valid(page.data, BLCKSZ));
	page = valid;
	header->pd_flags |= PD_HAS_ITL;
	UT_ASSERT(!cluster_space_identity_page_valid(page.data, BLCKSZ));
	page = valid;
	header->pd_flags |= 0x0080; /* Origin bits without origin-valid. */
	UT_ASSERT(!cluster_space_identity_page_valid(page.data, BLCKSZ));
}

UT_TEST(test_exact_rotation_and_duplicate)
{
	ClusterSpaceIdentity old = identity();
	ClusterSpaceIdentity next = old;
	ClusterSpaceIdentity wrong;

	next.sequence = 2;
	next.operation = 124;
	next.incarnation[15] ^= 1;
	UT_ASSERT_EQ(cluster_space_identity_transition(&old, &old, &next),
				 CLUSTER_SPACE_IDENTITY_APPLY);
	UT_ASSERT_EQ(cluster_space_identity_transition(&next, &old, &next),
				 CLUSTER_SPACE_IDENTITY_ALREADY);
	wrong = old;
	wrong.incarnation[0] ^= 1;
	UT_ASSERT_EQ(cluster_space_identity_transition(&wrong, &old, &next),
				 CLUSTER_SPACE_IDENTITY_MISMATCH);
	wrong = next;
	wrong.key.locator.relNumber++;
	UT_ASSERT_EQ(cluster_space_identity_transition(&wrong, &old, &next),
				 CLUSTER_SPACE_IDENTITY_MISMATCH);
}

UT_TEST(test_tombstone_never_revives)
{
	ClusterSpaceIdentity old = identity();
	ClusterSpaceIdentity dead = old;
	ClusterSpaceIdentity next;

	dead.sequence++;
	dead.operation++;
	dead.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	UT_ASSERT_EQ(cluster_space_identity_transition(&old, &old, &dead),
				 CLUSTER_SPACE_IDENTITY_APPLY);
	next = dead;
	next.state = CLUSTER_SPACE_IDENTITY_LIVE;
	next.sequence++;
	next.operation++;
	next.incarnation[0]++;
	UT_ASSERT_EQ(cluster_space_identity_transition(&dead, &dead, &next),
				 CLUSTER_SPACE_IDENTITY_INVALID);
}

UT_TEST(test_invalid_transition_not_duplicate)
{
	ClusterSpaceIdentity old = identity();
	ClusterSpaceIdentity next;
	int field;

	for (field = 0; field < 6; field++) {
		next = old;
		next.sequence++;
		next.operation++;
		next.incarnation[0]++;
		switch (field) {
		case 0:
			next.sequence++;
			break;
		case 1:
			next.operation = old.operation;
			break;
		case 2:
			memcpy(next.incarnation, old.incarnation, 16);
			break;
		case 3:
			next.key.database_incarnation++;
			break;
		case 4:
			next.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
			break;
		case 5:
			old.sequence = UINT64_MAX;
			next.sequence = 1;
			break;
		}
		UT_ASSERT_EQ(cluster_space_identity_transition(&old, &old, &next),
					 CLUSTER_SPACE_IDENTITY_INVALID);
		UT_ASSERT_EQ(cluster_space_identity_transition(&next, &old, &next),
					 CLUSTER_SPACE_IDENTITY_INVALID);
	}
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_literal_encoding_and_unaligned_decode);
	UT_RUN(test_every_payload_byte_tear_rejected_without_output);
	UT_RUN(test_crc_valid_unknown_fields_rejected);
	UT_RUN(test_exact_namespace_and_all_uuid_bytes);
	UT_RUN(test_invalid_identity_and_sizes_do_not_write);
	UT_RUN(test_page_roundtrip_and_origin_flags);
	UT_RUN(test_page_fork_block_header_and_tail_rejected);
	UT_RUN(test_exact_rotation_and_duplicate);
	UT_RUN(test_tombstone_never_revives);
	UT_RUN(test_invalid_transition_not_duplicate);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
