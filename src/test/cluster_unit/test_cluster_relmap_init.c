/*-------------------------------------------------------------------------
 *
 * test_cluster_relmap_init.c
 *    Fresh native-map validation and atomic image construction, frontend.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_init.c
 *
 * NOTES
 *    Synthetic fault cases complement real initdb input/consumer tests.
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include "cluster/cluster_relmap_authority.h"
#include "common/relmap.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static const Oid shared_oids[]
	= { 1262, 2964, 1213, 1260, 1261, 1214, 2396, 6000, 3592, 6243, 6100, 4177, 4178,
		2966, 2967, 4185, 4186, 4175, 4176, 2846, 2847, 4181, 4182, 4060, 4061, 6244,
		6245, 4183, 4184, 2671, 2672, 2965, 2697, 2698, 2676, 2677, 6303, 2694, 2695,
		6302, 1232, 1233, 2397, 6001, 6002, 3593, 6246, 6247, 6114, 6115 };
static const Oid local_oids[] = { 1259, 1249, 1255, 1247, 2836, 2837, 4171, 4172, 2690,
								  2691, 2703, 2704, 2658, 2659, 2662, 2663, 3455 };

static void
checksum(RelMapFile *map)
{
	INIT_CRC32C(map->crc);
	COMP_CRC32C(map->crc, map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map->crc);
}

static RelMapFile
native_map(bool shared)
{
	RelMapFile map = { 0 };
	const Oid *oids = shared ? shared_oids : local_oids;
	int n = shared ? lengthof(shared_oids) : lengthof(local_oids);

	map.magic = RELMAPPER_FILEMAGIC;
	map.num_mappings = n;
	for (int i = 0; i < n; ++i)
		map.mappings[i] = (RelMapping){ oids[i], oids[i] };
	checksum(&map);
	return map;
}

static bool
rejected_unchanged(const void *input, size_t len, bool shared, Oid dbid,
				   ClusterRelmapInitResult expected)
{
	char output[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE + 2] = { 0 };
	char before[sizeof(output)];
	ClusterRelmapInitResult result;

	output[0] = 31;
	output[sizeof(output) - 1] = 47;
	memcpy(before, output, sizeof(output));
	result = cluster_relmap_authority_init_image(shared, dbid, input, len, output + 1,
												 sizeof(output) - 2);
	return result == expected && memcmp(output, before, sizeof(output)) == 0;
}

UT_TEST(valid_initial_images)
{
	for (int shared = 0; shared <= 1; ++shared) {
		RelMapFile map = native_map(shared);
		ClusterRelmapAuthorityHeader h;
		pg_crc32c crc;
		char out[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE] = { 0 };

		UT_ASSERT_EQ(cluster_relmap_authority_init_image(shared, shared ? 0 : 5, &map, sizeof(map),
														 out, sizeof(out)),
					 CLUSTER_RELMAP_INIT_OK);
		memcpy(&h, out, sizeof(h));
		UT_ASSERT_EQ(h.magic, CLUSTER_RELMAP_AUTHORITY_MAGIC);
		UT_ASSERT_EQ(h.version, CLUSTER_RELMAP_AUTHORITY_VERSION);
		UT_ASSERT_EQ(h.committed_generation, 1);
		UT_ASSERT_EQ(h.pending_generation, 0);
		UT_ASSERT_EQ(h.shared_map, shared);
		UT_ASSERT_EQ(h.dbid, shared ? 0 : 5);
		UT_ASSERT_EQ(h.image_size, sizeof(map));
		UT_ASSERT_EQ(h.owner_node | h.owner_xid | h.owner_epoch | h.relmap_lsn, 0);
		UT_ASSERT_EQ(h.pad0 | h.pad1, 0);
		INIT_CRC32C(crc);
		COMP_CRC32C(crc, out, offsetof(ClusterRelmapAuthorityHeader, crc));
		FIN_CRC32C(crc);
		UT_ASSERT(EQ_CRC32C(crc, h.crc));
		UT_ASSERT_EQ(memcmp(out + CLUSTER_RELMAP_COMMITTED_OFFSET, &map, sizeof(map)), 0);
		UT_ASSERT_EQ(memcmp(out + CLUSTER_RELMAP_PENDING_OFFSET, &map, sizeof(map)), 0);
		for (size_t i = sizeof(map); i < CLUSTER_RELMAP_IMAGE_MAX; ++i) {
			UT_ASSERT_EQ(out[CLUSTER_RELMAP_COMMITTED_OFFSET + i], 0);
			UT_ASSERT_EQ(out[CLUSTER_RELMAP_PENDING_OFFSET + i], 0);
		}
	}
}

UT_TEST(exact_lengths)
{
	RelMapFile map = native_map(false);
	char larger[sizeof(map) + 1];
	char out[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE + 1] = { 0 };
	char before[sizeof(out)];

	memcpy(larger, &map, sizeof(map));
	for (size_t n = 0; n < sizeof(map); ++n)
		UT_ASSERT(rejected_unchanged(&map, n, false, 1, CLUSTER_RELMAP_INIT_INVALID_LENGTH));
	UT_ASSERT(
		rejected_unchanged(larger, sizeof(larger), false, 1, CLUSTER_RELMAP_INIT_INVALID_LENGTH));
	memcpy(before, out, sizeof(out));
	UT_ASSERT_EQ(cluster_relmap_authority_init_image(false, 1, &map, sizeof(map), out, sizeof(out)),
				 CLUSTER_RELMAP_INIT_INVALID_LENGTH);
	UT_ASSERT_EQ(
		cluster_relmap_authority_init_image(false, 1, &map, sizeof(map), out, sizeof(out) - 2),
		CLUSTER_RELMAP_INIT_INVALID_LENGTH);
	UT_ASSERT_EQ(memcmp(out, before, sizeof(out)), 0);
}

UT_TEST(bad_magic_crc_count)
{
	RelMapFile map = native_map(false);
	map.magic++;
	checksum(&map);
	UT_ASSERT(rejected_unchanged(&map, sizeof(map), false, 1, CLUSTER_RELMAP_INIT_INVALID_MAGIC));
	map = native_map(false);
	map.mappings[0].mapfilenumber++;
	UT_ASSERT(rejected_unchanged(&map, sizeof(map), false, 1, CLUSTER_RELMAP_INIT_INVALID_CRC));
	for (int n = -1; n <= MAX_MAPPINGS + 1; ++n) {
		if (n > 0 && n <= MAX_MAPPINGS)
			continue;
		map = native_map(false);
		map.num_mappings = n;
		checksum(&map);
		UT_ASSERT(
			rejected_unchanged(&map, sizeof(map), false, 1, CLUSTER_RELMAP_INIT_INVALID_COUNT));
	}
}

UT_TEST(namespace_mismatch)
{
	RelMapFile local = native_map(false);
	RelMapFile shared = native_map(true);
	UT_ASSERT(
		rejected_unchanged(&local, sizeof(local), true, 1, CLUSTER_RELMAP_INIT_NAMESPACE_MISMATCH));
	UT_ASSERT(rejected_unchanged(&local, sizeof(local), false, 0,
								 CLUSTER_RELMAP_INIT_NAMESPACE_MISMATCH));
	UT_ASSERT(
		rejected_unchanged(&local, sizeof(local), true, 0, CLUSTER_RELMAP_INIT_NAMESPACE_MISMATCH));
	UT_ASSERT(rejected_unchanged(&shared, sizeof(shared), false, 5,
								 CLUSTER_RELMAP_INIT_NAMESPACE_MISMATCH));
	local.mappings[0] = shared.mappings[0];
	checksum(&local);
	UT_ASSERT(rejected_unchanged(&local, sizeof(local), false, 5,
								 CLUSTER_RELMAP_INIT_NAMESPACE_MISMATCH));
}

UT_TEST(invalid_entries_and_duplicates)
{
	RelMapFile map;
	const Oid bad[] = { 0, 16384, PG_UINT32_MAX };
	for (size_t i = 0; i < lengthof(bad); ++i) {
		map = native_map(false);
		map.mappings[0].mapoid = bad[i];
		checksum(&map);
		UT_ASSERT(
			rejected_unchanged(&map, sizeof(map), false, 5, CLUSTER_RELMAP_INIT_INVALID_ENTRY));
		map = native_map(false);
		map.mappings[0].mapfilenumber = bad[i];
		checksum(&map);
		UT_ASSERT(
			rejected_unchanged(&map, sizeof(map), false, 5, CLUSTER_RELMAP_INIT_INVALID_ENTRY));
	}
	map = native_map(false);
	map.mappings[1].mapoid = map.mappings[0].mapoid;
	checksum(&map);
	UT_ASSERT(rejected_unchanged(&map, sizeof(map), false, 5, CLUSTER_RELMAP_INIT_DUPLICATE));
	map = native_map(false);
	map.mappings[1].mapfilenumber = map.mappings[0].mapfilenumber;
	checksum(&map);
	UT_ASSERT(rejected_unchanged(&map, sizeof(map), false, 5, CLUSTER_RELMAP_INIT_DUPLICATE));
	map = native_map(false);
	map.num_mappings--;
	checksum(&map);
	UT_ASSERT(rejected_unchanged(&map, sizeof(map), false, 5, CLUSTER_RELMAP_INIT_INVALID_COUNT));
}

UT_TEST(rewritten_map_and_reordering)
{
	RelMapFile map = native_map(false);
	RelMapping first = map.mappings[0];
	char out[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE] = { 0 };
	map.mappings[0].mapfilenumber = 15000;
	checksum(&map);
	UT_ASSERT(rejected_unchanged(&map, sizeof(map), false, 5, CLUSTER_RELMAP_INIT_INVALID_ENTRY));
	map.mappings[0] = map.mappings[1];
	map.mappings[1] = first;
	checksum(&map);
	UT_ASSERT_EQ(cluster_relmap_authority_init_image(false, 5, &map, sizeof(map), out, sizeof(out)),
				 CLUSTER_RELMAP_INIT_OK);
}

UT_TEST(existing_output_zero_overwrite)
{
	RelMapFile map = native_map(false);
	char out[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE] = { 0 };
	char before[sizeof(out)];
	UT_ASSERT_EQ(cluster_relmap_authority_init_image(false, 5, &map, sizeof(map), out, sizeof(out)),
				 CLUSTER_RELMAP_INIT_OK);
	memcpy(before, out, sizeof(out));
	UT_ASSERT_EQ(cluster_relmap_authority_init_image(false, 5, &map, sizeof(map), out, sizeof(out)),
				 CLUSTER_RELMAP_INIT_OUTPUT_NOT_EMPTY);
	UT_ASSERT_EQ(memcmp(out, before, sizeof(out)), 0);
	for (size_t i = 0; i < sizeof(out); ++i) {
		memset(out, 0, sizeof(out));
		out[i] = 1;
		memcpy(before, out, sizeof(out));
		UT_ASSERT_EQ(
			cluster_relmap_authority_init_image(false, 5, &map, sizeof(map), out, sizeof(out)),
			CLUSTER_RELMAP_INIT_OUTPUT_NOT_EMPTY);
		UT_ASSERT_EQ(memcmp(out, before, sizeof(out)), 0);
	}
}

UT_TEST(null_and_unaligned_buffers)
{
	RelMapFile map = native_map(false);
	char in[sizeof(map) + 1];
	char out[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE + 2] = { 0 };
	UT_ASSERT(
		rejected_unchanged(NULL, sizeof(map), false, 5, CLUSTER_RELMAP_INIT_INVALID_ARGUMENT));
	UT_ASSERT_EQ(cluster_relmap_authority_init_image(false, 5, &map, sizeof(map), NULL,
													 CLUSTER_RELMAP_AUTHORITY_FILE_SIZE),
				 CLUSTER_RELMAP_INIT_INVALID_ARGUMENT);
	memcpy(in + 1, &map, sizeof(map));
	out[0] = 35;
	out[sizeof(out) - 1] = 36;
	UT_ASSERT_EQ(cluster_relmap_authority_init_image(false, 5, in + 1, sizeof(map), out + 1,
													 sizeof(out) - 2),
				 CLUSTER_RELMAP_INIT_OK);
	UT_ASSERT_EQ(out[0], 35);
	UT_ASSERT_EQ(out[sizeof(out) - 1], 36);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(valid_initial_images);
	UT_RUN(exact_lengths);
	UT_RUN(bad_magic_crc_count);
	UT_RUN(namespace_mismatch);
	UT_RUN(invalid_entries_and_duplicates);
	UT_RUN(rewritten_map_and_reordering);
	UT_RUN(existing_output_zero_overwrite);
	UT_RUN(null_and_unaligned_buffers);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
