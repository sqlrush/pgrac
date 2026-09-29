/*-------------------------------------------------------------------------
 *
 * test_cluster_space_wal.c
 *    Exact typed SPACE lifecycle records and atomic page transitions.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_space_wal.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_space_identity.h"
#include "storage/bufpage.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterSpaceWalChange
create_change(void)
{
	ClusterSpaceWalChange change;

	memset(&change, 0, sizeof(change));
	change.action = CLUSTER_SPACE_WAL_CREATE;
	change.nblocks = InvalidBlockNumber;
	change.result_token = 17;
	change.result.key.system_identifier = 1;
	change.result.key.database_incarnation = 2;
	change.result.key.storage_uuid[15] = 3;
	change.result.key.locator = (RelFileLocator){ DEFAULTTABLESPACE_OID, 5, 16384 };
	change.result.incarnation[15] = 4;
	change.result.sequence = 1;
	change.result.operation = 9;
	change.result.state = CLUSTER_SPACE_IDENTITY_LIVE;
	return change;
}

static ClusterSpaceWalChange
truncate_change(void)
{
	ClusterSpaceWalChange change = create_change();

	change.action = CLUSTER_SPACE_WAL_TRUNCATE;
	change.nblocks = 0;
	change.expected = change.result;
	change.before_token = change.result_token;
	change.result_token = 5; /* Token identity, never numeric ordering. */
	change.result.incarnation[15] = 6;
	change.result.sequence = 2;
	change.result.operation = 10;
	return change;
}

UT_TEST(test_literal_wal_layout_and_roundtrip)
{
	ClusterSpaceWalChange change = create_change(), decoded;
	uint8 bytes[CLUSTER_SPACE_WAL_BYTES + 1];
	static const uint8 header[]
		= { 0x50, 0x53, 0x57, 0x31, 1, 0, 0x20, 1, 1, 0, 0, 0, 0xff, 0xff, 0xff, 0xff };

	UT_ASSERT(cluster_space_wal_encode(&change, bytes + 1, 288));
	UT_ASSERT(memcmp(bytes + 1, header, sizeof(header)) == 0);
	for (int i = 16; i < 24; i++)
		UT_ASSERT_EQ(bytes[1 + i], 0);
	UT_ASSERT_EQ(bytes[25], 17);
	for (int i = 32; i < 160; i++)
		UT_ASSERT_EQ(bytes[1 + i], 0);
	UT_ASSERT(cluster_space_wal_decode(bytes + 1, 288, &decoded));
	UT_ASSERT_EQ(decoded.action, CLUSTER_SPACE_WAL_CREATE);
	UT_ASSERT_EQ(decoded.result.key.locator.relNumber, 16384);
	UT_ASSERT_EQ(decoded.result.incarnation[15], 4);
	UT_ASSERT_EQ(decoded.result_token, 17);
}

UT_TEST(test_bad_wal_never_changes_output)
{
	ClusterSpaceWalChange change = create_change(), out, saved;
	uint8 bytes[CLUSTER_SPACE_WAL_BYTES], bad[CLUSTER_SPACE_WAL_BYTES];
	const int offsets[] = { 0, 4, 6, 8, 12, 16, 32, 159, 160, 215, 287 };

	UT_ASSERT(cluster_space_wal_encode(&change, bytes, sizeof(bytes)));
	memset(&saved, 0x5a, sizeof(saved));
	for (int i = 0; i < lengthof(offsets); i++) {
		memcpy(bad, bytes, sizeof(bad));
		bad[offsets[i]] ^= 0x80;
		out = saved;
		UT_ASSERT(!cluster_space_wal_decode(bad, sizeof(bad), &out));
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	}
	out = saved;
	UT_ASSERT(!cluster_space_wal_decode(bytes, sizeof(bytes) - 1, &out));
	UT_ASSERT(!cluster_space_wal_decode(bytes, sizeof(bytes) + 1, &out));
	UT_ASSERT(!cluster_space_wal_decode(NULL, sizeof(bytes), &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
}

UT_TEST(test_create_requires_absence_or_exact_duplicate)
{
	ClusterSpaceWalChange change = create_change();
	PGAlignedBlock page, saved;
	ClusterSpaceIdentity readback;
	uint64 token;

	memset(&page, 0, sizeof(page));
	UT_ASSERT_EQ(cluster_space_wal_apply(&change, &change.result.key, page.data, BLCKSZ),
				 CLUSTER_SPACE_IDENTITY_APPLY);
	UT_ASSERT(cluster_space_identity_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 0,
												 &change.result.key, &readback, &token));
	UT_ASSERT_EQ(readback.operation, 9);
	UT_ASSERT_EQ(token, 17);
	saved = page;
	UT_ASSERT_EQ(cluster_space_wal_apply(&change, &change.result.key, page.data, BLCKSZ),
				 CLUSTER_SPACE_IDENTITY_ALREADY);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	change.result.incarnation[0] = 8;
	UT_ASSERT_EQ(cluster_space_wal_apply(&change, &change.result.key, page.data, BLCKSZ),
				 CLUSTER_SPACE_IDENTITY_MISMATCH);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
}

UT_TEST(test_exact_before_and_result_not_numeric_versions)
{
	ClusterSpaceWalChange change = truncate_change();
	PGAlignedBlock page, saved;

	UT_ASSERT(cluster_space_identity_page_encode(&change.expected, 17, page.data, BLCKSZ));
	UT_ASSERT_EQ(cluster_space_wal_apply(&change, &change.result.key, page.data, BLCKSZ),
				 CLUSTER_SPACE_IDENTITY_APPLY);
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 5);
	saved = page;
	UT_ASSERT_EQ(cluster_space_wal_apply(&change, &change.result.key, page.data, BLCKSZ),
				 CLUSTER_SPACE_IDENTITY_ALREADY);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	((PageHeader)page.data)->pd_block_scn = 6;
	saved = page;
	UT_ASSERT_EQ(cluster_space_wal_apply(&change, &change.result.key, page.data, BLCKSZ),
				 CLUSTER_SPACE_IDENTITY_MISMATCH);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	memset(&page, 0, sizeof(page));
	saved = page;
	UT_ASSERT_EQ(cluster_space_wal_apply(&change, &change.result.key, page.data, BLCKSZ),
				 CLUSTER_SPACE_IDENTITY_MISMATCH);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
}

UT_TEST(test_wrong_key_torn_and_wrong_token_never_mutate)
{
	ClusterSpaceWalChange change = truncate_change();
	ClusterSpaceIdentityKey wrong = change.result.key;
	PGAlignedBlock page, saved;

	UT_ASSERT(cluster_space_identity_page_encode(&change.expected, 17, page.data, BLCKSZ));
	saved = page;
	wrong.locator.relNumber++;
	UT_ASSERT_EQ(cluster_space_wal_apply(&change, &wrong, page.data, BLCKSZ),
				 CLUSTER_SPACE_IDENTITY_MISMATCH);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	change.before_token = 18;
	UT_ASSERT_EQ(cluster_space_wal_apply(&change, &change.result.key, page.data, BLCKSZ),
				 CLUSTER_SPACE_IDENTITY_MISMATCH);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	page.data[160] = 1;
	saved = page;
	UT_ASSERT_EQ(cluster_space_wal_apply(&change, &change.result.key, page.data, BLCKSZ),
				 CLUSTER_SPACE_IDENTITY_INVALID);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
}

UT_TEST(test_tombstone_and_invalid_action_shapes)
{
	ClusterSpaceWalChange change = truncate_change(), bad;
	PGAlignedBlock page;
	uint8 bytes[CLUSTER_SPACE_WAL_BYTES], saved[CLUSTER_SPACE_WAL_BYTES];

	change.action = CLUSTER_SPACE_WAL_TOMBSTONE;
	change.nblocks = InvalidBlockNumber;
	memcpy(change.result.incarnation, change.expected.incarnation, 16);
	change.result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	UT_ASSERT(cluster_space_wal_encode(&change, bytes, sizeof(bytes)));
	UT_ASSERT(cluster_space_identity_page_encode(&change.expected, 17, page.data, BLCKSZ));
	UT_ASSERT_EQ(cluster_space_wal_apply(&change, &change.result.key, page.data, BLCKSZ),
				 CLUSTER_SPACE_IDENTITY_APPLY);
	memset(saved, 0xa5, sizeof(saved));
	for (int i = 0; i < 7; i++) {
		bad = change;
		switch (i) {
		case 0:
			bad.action = 9;
			break;
		case 1:
			bad.nblocks = 0;
			break;
		case 2:
			bad.before_token = 0;
			break;
		case 3:
			bad.result_token = bad.before_token;
			break;
		case 4:
			bad.result.sequence = 1;
			break;
		case 5:
			bad.result.key.storage_uuid[0] = 1;
			break;
		case 6:
			bad.result.incarnation[0] = 1;
			break;
		}
		memcpy(bytes, saved, sizeof(bytes));
		UT_ASSERT(!cluster_space_wal_encode(&bad, bytes, sizeof(bytes)));
		UT_ASSERT(memcmp(bytes, saved, sizeof(bytes)) == 0);
	}
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_literal_wal_layout_and_roundtrip);
	UT_RUN(test_bad_wal_never_changes_output);
	UT_RUN(test_create_requires_absence_or_exact_duplicate);
	UT_RUN(test_exact_before_and_result_not_numeric_versions);
	UT_RUN(test_wrong_key_torn_and_wrong_token_never_mutate);
	UT_RUN(test_tombstone_and_invalid_action_shapes);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
