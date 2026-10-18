/*-------------------------------------------------------------------------
 *
 * test_cluster_relmap_v2.c
 *    Exercise the production relmap v2 codec with independent wire fixtures.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_v2.c
 *
 * NOTES
 *    Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "common/cluster_relmap_v2.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Literal offsets here intentionally do not share the encoder's layout code. */
static void
put_le(unsigned char *p, uint64 value, size_t count)
{
	for (size_t i = 0; i < count; i++)
		p[i] = (unsigned char)(value >> (8 * i));
}

static pg_crc32c
crc_bytes(const void *p, size_t count)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, p, count);
	FIN_CRC32C(crc);
	return crc;
}

static void
refresh_crc(unsigned char *wire)
{
	put_le(wire + 116, crc_bytes(wire + 128, 1024), 4);
	put_le(wire + 120, crc_bytes(wire + 1152, 1024), 4);
	put_le(wire + 124, crc_bytes(wire, 124), 4);
}

static RelMapFile
native_map(Oid oid, RelFileNumber file)
{
	RelMapFile map;

	memset(&map, 0, sizeof(map));
	map.magic = RELMAPPER_FILEMAGIC;
	map.num_mappings = 1;
	map.mappings[0].mapoid = oid;
	map.mappings[0].mapfilenumber = file;
	map.crc = crc_bytes(&map, offsetof(RelMapFile, crc));
	return map;
}

static const ClusterRelmapV2Namespace identity
	= { UINT64CONST(0x0123456789abcdef), UINT64CONST(0xfedcba9876543210), 42, false };

static void
fixture(unsigned char *wire, bool pending)
{
	RelMapFile committed = native_map(1259, 40000);
	RelMapFile next = native_map(1259, 40001);

	memset(wire, 0, 2176);
	put_le(wire, 0x0240dea9, 4);
	put_le(wire + 4, 2, 2);
	put_le(wire + 6, 128, 2);
	put_le(wire + 8, 2176, 4);
	put_le(wire + 12, sizeof(RelMapFile), 4);
	put_le(wire + 16, identity.system_identifier, 8);
	put_le(wire + 24, identity.database_incarnation, 8);
	put_le(wire + 32, 7, 8);
	put_le(wire + 48, 42, 4);
	memcpy(wire + 128, &committed, sizeof(committed));
	if (pending) {
		put_le(wire + 40, 8, 8);
		put_le(wire + 54, 3, 2);
		put_le(wire + 56, UINT64CONST(0x0000000212345678), 8);
		put_le(wire + 64, 91, 8);
		put_le(wire + 72, 101, 8);
		put_le(wire + 80, UINT64CONST(0x123456789), 8);
		put_le(wire + 88, 202, 8);
		memcpy(wire + 1152, &next, sizeof(next));
	}
	refresh_crc(wire);
}

UT_TEST(test_frozen_wire_decodes_without_native_header_cast)
{
	unsigned char wire[2176];
	ClusterRelmapV2Image image;

	fixture(wire, true);
	UT_ASSERT_EQ(cluster_relmap_v2_decode(wire, sizeof(wire), &identity, &image),
				 CLUSTER_RELMAP_V2_VALID);
	UT_ASSERT_EQ(image.committed_generation, 7);
	UT_ASSERT_EQ(image.pending_generation, 8);
	UT_ASSERT_EQ(image.pending_owner.top_full_xid, UINT64CONST(0x0000000212345678));
	UT_ASSERT_EQ(image.pending_owner.admitted_incarnation, 101);
	UT_ASSERT_EQ(image.pending_owner.boot_incarnation, 202);
	UT_ASSERT_EQ(image.committed.mappings[0].mapfilenumber, 40000);
	UT_ASSERT_EQ(image.pending.mappings[0].mapfilenumber, 40001);
}

static void
expect_reject(const unsigned char *wire, size_t length, ClusterRelmapV2Validity expected)
{
	ClusterRelmapV2Image image;
	ClusterRelmapV2Image zero;

	memset(&image, 0xa5, sizeof(image));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT_EQ(cluster_relmap_v2_decode(wire, length, &identity, &image), expected);
	UT_ASSERT(memcmp(&image, &zero, sizeof(image)) == 0);
}

static void
make_published(unsigned char *wire)
{
	fixture(wire, true);
	put_le(wire + 32, 8, 8);
	put_le(wire + 40, 0, 8);
	memset(wire + 54, 0, 42);
	memcpy(wire + 128, wire + 1152, 1024);
	memset(wire + 1152, 0, 1024);
	refresh_crc(wire);
}

UT_TEST(test_encoder_matches_independent_frozen_bytes)
{
	unsigned char wire[2176];
	unsigned char encoded[2176];
	ClusterRelmapV2Image image;

	for (int pending = 0; pending <= 1; pending++) {
		fixture(wire, pending != 0);
		UT_ASSERT_EQ(cluster_relmap_v2_decode(wire, sizeof(wire), &identity, &image),
					 CLUSTER_RELMAP_V2_VALID);
		memset(encoded, 0xa5, sizeof(encoded));
		UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, encoded, sizeof(encoded)),
					 CLUSTER_RELMAP_V2_VALID);
		UT_ASSERT(memcmp(encoded, wire, sizeof(wire)) == 0);
	}
}

UT_TEST(test_old_unknown_and_reserved_format_refused)
{
	static const struct {
		size_t offset;
		size_t width;
		uint64 value;
	} changes[] = { { 0, 4, 0x0140dea9 }, { 0, 4, 0x0340dea9 }, { 4, 2, 1 },	 { 4, 2, 3 },
					{ 6, 2, 120 },		  { 8, 4, 2175 },		{ 12, 4, 1025 }, { 12, 4, 523 },
					{ 52, 1, 2 },		  { 53, 1, 1 } };
	unsigned char wire[2176];

	for (size_t i = 0; i < lengthof(changes); i++) {
		fixture(wire, true);
		put_le(wire + changes[i].offset, changes[i].value, changes[i].width);
		refresh_crc(wire);
		expect_reject(wire, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_FORMAT);
	}
	for (size_t i = 96; i < 116; i++) {
		fixture(wire, true);
		wire[i] = 1;
		refresh_crc(wire);
		expect_reject(wire, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_FORMAT);
	}
}

UT_TEST(test_exact_length_required_and_unaligned_input_safe)
{
	unsigned char unaligned[2178];
	ClusterRelmapV2Image image;

	fixture(unaligned + 1, true);
	for (size_t length = 0; length < 2176; length++)
		expect_reject(unaligned + 1, length, CLUSTER_RELMAP_V2_INVALID_LENGTH);
	expect_reject(unaligned + 1, 2177, CLUSTER_RELMAP_V2_INVALID_LENGTH);
	UT_ASSERT_EQ(cluster_relmap_v2_decode(unaligned + 1, 2176, &identity, &image),
				 CLUSTER_RELMAP_V2_VALID);
}

UT_TEST(test_namespace_identity_bound_to_expected_root)
{
	static const size_t offsets[] = { 16, 24, 48, 52 };
	unsigned char wire[2176];
	ClusterRelmapV2Image image;
	ClusterRelmapV2Namespace shared = identity;

	for (size_t i = 0; i < lengthof(offsets); i++) {
		fixture(wire, true);
		wire[offsets[i]] ^= 1;
		refresh_crc(wire);
		expect_reject(wire, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_IDENTITY);
	}
	fixture(wire, false);
	shared.shared = true;
	shared.dbid = InvalidOid;
	wire[52] = 1;
	put_le(wire + 48, 0, 4);
	refresh_crc(wire);
	UT_ASSERT_EQ(cluster_relmap_v2_decode(wire, sizeof(wire), &shared, &image),
				 CLUSTER_RELMAP_V2_VALID);
	UT_ASSERT(image.identity.shared);
	UT_ASSERT_EQ(image.identity.dbid, InvalidOid);
}

UT_TEST(test_crc_covers_every_header_and_slot_byte)
{
	unsigned char original[2176];
	unsigned char wire[2176];
	ClusterRelmapV2Image image;

	UT_ASSERT_EQ(crc_bytes("123456789", 9), 0xe3069283);
	fixture(original, true);
	for (size_t i = 0; i < sizeof(wire); i++) {
		memcpy(wire, original, sizeof(wire));
		wire[i] ^= 1;
		UT_ASSERT(cluster_relmap_v2_decode(wire, sizeof(wire), &identity, &image)
				  != CLUSTER_RELMAP_V2_VALID);
	}
}

UT_TEST(test_native_images_checked_even_after_outer_crc_repaired)
{
	unsigned char wire[2176];
	RelMapFile map;

	for (size_t slot = 128; slot <= 1152; slot += 1024) {
		for (int fault = 0; fault < 6; fault++) {
			fixture(wire, true);
			memcpy(&map, wire + slot, sizeof(map));
			if (fault == 0)
				map.magic++;
			else if (fault == 1)
				map.num_mappings = -1;
			else if (fault == 2)
				map.num_mappings = MAX_MAPPINGS + 1;
			else if (fault == 3)
				map.mappings[0].mapoid = InvalidOid;
			else if (fault == 4)
				map.mappings[0].mapfilenumber = InvalidRelFileNumber;
			if (fault != 5)
				map.crc = crc_bytes(&map, offsetof(RelMapFile, crc));
			else
				map.crc ^= 1;
			memcpy(wire + slot, &map, sizeof(map));
			refresh_crc(wire);
			expect_reject(wire, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_IMAGE);
		}
		fixture(wire, true);
		wire[slot + sizeof(RelMapFile)] = 1;
		refresh_crc(wire);
		expect_reject(wire, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_IMAGE);
	}
}

UT_TEST(test_no_pending_requires_zero_owner_and_entire_slot)
{
	unsigned char wire[2176];
	ClusterRelmapV2Image image;

	fixture(wire, false);
	UT_ASSERT_EQ(cluster_relmap_v2_decode(wire, sizeof(wire), &identity, &image),
				 CLUSTER_RELMAP_V2_VALID);
	for (size_t i = 54; i < 96; i++) {
		fixture(wire, false);
		wire[i] = 1;
		refresh_crc(wire);
		expect_reject(wire, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_OWNER);
	}
	for (size_t i = 1152; i < 2176; i++) {
		fixture(wire, false);
		wire[i] = 1;
		refresh_crc(wire);
		expect_reject(wire, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_IMAGE);
	}
}

UT_TEST(test_boot_admitted_and_original_fullxid_independently_required)
{
	static const size_t required[] = { 56, 72, 80, 88 };
	unsigned char wire[2176];
	ClusterRelmapV2Image image;

	for (size_t i = 0; i < lengthof(required); i++) {
		fixture(wire, true);
		put_le(wire + required[i], 0, 8);
		refresh_crc(wire);
		expect_reject(wire, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_OWNER);
	}
	for (uint32 xid = 0; xid < 3; xid++) {
		fixture(wire, true);
		put_le(wire + 56, (UINT64CONST(2) << 32) | xid, 8);
		refresh_crc(wire);
		expect_reject(wire, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_OWNER);
	}
	fixture(wire, true);
	put_le(wire + 54, 128, 2);
	refresh_crc(wire);
	expect_reject(wire, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_OWNER);
	/* Equal numeric incarnations are legal, but both fields remain mandatory. */
	fixture(wire, true);
	put_le(wire + 88, 101, 8);
	put_le(wire + 54, 0, 2);
	put_le(wire + 64, 0, 8);
	refresh_crc(wire);
	UT_ASSERT_EQ(cluster_relmap_v2_decode(wire, sizeof(wire), &identity, &image),
				 CLUSTER_RELMAP_V2_VALID);
	UT_ASSERT_EQ(image.pending_owner.membership_epoch, 0);
}

UT_TEST(test_generation_zero_gap_and_overflow_refused)
{
	unsigned char wire[2176];
	ClusterRelmapV2Image image;

	fixture(wire, true);
	put_le(wire + 32, 0, 8);
	refresh_crc(wire);
	expect_reject(wire, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_GENERATION);
	fixture(wire, true);
	put_le(wire + 40, 9, 8);
	refresh_crc(wire);
	expect_reject(wire, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_GENERATION);
	fixture(wire, true);
	put_le(wire + 32, PG_UINT64_MAX, 8);
	refresh_crc(wire);
	expect_reject(wire, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_GENERATION);
	fixture(wire, false);
	put_le(wire + 32, PG_UINT64_MAX, 8);
	refresh_crc(wire);
	UT_ASSERT_EQ(cluster_relmap_v2_decode(wire, sizeof(wire), &identity, &image),
				 CLUSTER_RELMAP_V2_VALID);
}

UT_TEST(test_invalid_encode_and_arguments_publish_no_output)
{
	unsigned char wire[2176];
	unsigned char original[2176];
	ClusterRelmapV2Image image;
	ClusterRelmapV2Image zero;
	ClusterRelmapV2Namespace bad = identity;

	fixture(wire, true);
	UT_ASSERT_EQ(cluster_relmap_v2_decode(wire, sizeof(wire), &identity, &image),
				 CLUSTER_RELMAP_V2_VALID);
	image.pending_owner.boot_incarnation = 0;
	memset(original, 0xa5, sizeof(original));
	memcpy(wire, original, sizeof(wire));
	UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, wire, sizeof(wire)),
				 CLUSTER_RELMAP_V2_INVALID_OWNER);
	UT_ASSERT(memcmp(wire, original, sizeof(wire)) == 0);
	UT_ASSERT_EQ(cluster_relmap_v2_encode(NULL, wire, sizeof(wire)),
				 CLUSTER_RELMAP_V2_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, wire, sizeof(wire) - 1),
				 CLUSTER_RELMAP_V2_INVALID_LENGTH);
	bad.system_identifier = 0;
	UT_ASSERT_EQ(cluster_relmap_v2_decode(wire, sizeof(wire), &bad, &image),
				 CLUSTER_RELMAP_V2_INVALID_ARGUMENT);
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT(memcmp(&image, &zero, sizeof(image)) == 0);
	UT_ASSERT_EQ(cluster_relmap_v2_decode(wire, sizeof(wire), &identity, NULL),
				 CLUSTER_RELMAP_V2_INVALID_ARGUMENT);
	expect_reject(NULL, sizeof(wire), CLUSTER_RELMAP_V2_INVALID_LENGTH);
}

UT_TEST(test_identical_copies_only_expose_committed_as_read_value)
{
	unsigned char wire[2176];
	ClusterRelmapV2View view;

	for (int pending = 0; pending <= 1; pending++) {
		fixture(wire, pending != 0);
		UT_ASSERT_EQ(
			cluster_relmap_v2_read_pair(wire, sizeof(wire), wire, sizeof(wire), &identity, &view),
			CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(view.pending_complete);
		UT_ASSERT_EQ(view.has_pending, pending != 0);
		UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, 40000);
		UT_ASSERT(!view.used_backup);
	}
}

UT_TEST(test_stage_cut_retains_primary_pending_without_exposing_it)
{
	unsigned char primary[2176];
	unsigned char backup[2176];
	ClusterRelmapV2View view;

	fixture(primary, true);
	fixture(backup, false);
	UT_ASSERT_EQ(cluster_relmap_v2_read_pair(primary, sizeof(primary), backup, sizeof(backup),
											 &identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.pending_complete && view.has_pending);
	UT_ASSERT_EQ(view.committed_generation, 7);
	UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, 40000);
	UT_ASSERT_EQ(view.pending_generation, 8);
	UT_ASSERT_EQ(view.pending.mappings[0].mapfilenumber, 40001);
}

UT_TEST(test_publish_cut_preserves_backup_pending_obligation)
{
	unsigned char primary[2176];
	unsigned char backup[2176];
	ClusterRelmapV2View view;

	make_published(primary);
	fixture(backup, true);
	UT_ASSERT_EQ(cluster_relmap_v2_read_pair(primary, sizeof(primary), backup, sizeof(backup),
											 &identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.pending_complete && view.has_pending);
	UT_ASSERT_EQ(view.committed_generation, 8);
	UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, 40001);
	UT_ASSERT_EQ(view.pending_owner.boot_incarnation, 202);
	UT_ASSERT_EQ(view.pending_owner.admitted_incarnation, 101);
}

UT_TEST(test_discard_cut_does_not_erase_backup_pending)
{
	unsigned char primary[2176];
	unsigned char backup[2176];
	ClusterRelmapV2View view;

	fixture(primary, false);
	fixture(backup, true);
	UT_ASSERT_EQ(cluster_relmap_v2_read_pair(primary, sizeof(primary), backup, sizeof(backup),
											 &identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.pending_complete && view.has_pending);
	UT_ASSERT_EQ(view.committed_generation, 7);
	UT_ASSERT_EQ(view.pending_generation, 8);
}

UT_TEST(test_fallback_never_proves_complete_pending_coverage)
{
	unsigned char wire[2176];
	unsigned char damaged[2176];
	ClusterRelmapV2View view;

	fixture(wire, false);
	UT_ASSERT_EQ(cluster_relmap_v2_read_pair(NULL, 0, wire, sizeof(wire), &identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_DEGRADED);
	UT_ASSERT(view.used_backup && !view.pending_complete && !view.has_pending);
	UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, 40000);
	fixture(wire, true);
	UT_ASSERT_EQ(cluster_relmap_v2_read_pair(wire, sizeof(wire), NULL, 0, &identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_DEGRADED);
	UT_ASSERT(!view.used_backup && !view.pending_complete && view.has_pending);
	memcpy(damaged, wire, sizeof(wire));
	damaged[128] ^= 1;
	UT_ASSERT_EQ(
		cluster_relmap_v2_read_pair(damaged, sizeof(damaged), wire, sizeof(wire), &identity, &view),
		CLUSTER_RELMAP_V2_PAIR_DEGRADED);
	UT_ASSERT(view.used_backup && !view.pending_complete && view.has_pending);
	/* A torn identity is CRC damage; a checksummed foreign identity is refusal. */
	memcpy(damaged, wire, sizeof(wire));
	damaged[16] ^= 1;
	UT_ASSERT_EQ(
		cluster_relmap_v2_read_pair(damaged, sizeof(damaged), wire, sizeof(wire), &identity, &view),
		CLUSTER_RELMAP_V2_PAIR_DEGRADED);
}

UT_TEST(test_foreign_or_unknown_copy_refused_even_with_valid_other_copy)
{
	unsigned char wire[2176];
	unsigned char foreign[2176];
	ClusterRelmapV2View view;
	ClusterRelmapV2View zero;

	memset(&zero, 0, sizeof(zero));
	fixture(wire, true);
	for (int fault = 0; fault < 2; fault++) {
		memcpy(foreign, wire, sizeof(wire));
		if (fault == 0)
			foreign[16] ^= 1;
		else
			put_le(foreign, 0x0140dea9, 4);
		refresh_crc(foreign);
		UT_ASSERT_EQ(cluster_relmap_v2_read_pair(wire, sizeof(wire), foreign, sizeof(foreign),
												 &identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_INVALID);
		UT_ASSERT(memcmp(&view, &zero, sizeof(view)) == 0);
	}
	UT_ASSERT_EQ(cluster_relmap_v2_read_pair(NULL, 0, NULL, 0, &identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_INVALID);
	UT_ASSERT(memcmp(&view, &zero, sizeof(view)) == 0);
}

UT_TEST(test_same_generation_committed_conflict_rejected)
{
	unsigned char primary[2176];
	unsigned char backup[2176];
	RelMapFile changed = native_map(1259, 49999);
	ClusterRelmapV2View view;
	ClusterRelmapV2View zero;

	fixture(primary, true);
	fixture(backup, true);
	memcpy(backup + 128, &changed, sizeof(changed));
	refresh_crc(backup);
	UT_ASSERT_EQ(cluster_relmap_v2_read_pair(primary, sizeof(primary), backup, sizeof(backup),
											 &identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_CONFLICT);
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT(memcmp(&view, &zero, sizeof(view)) == 0);
}

UT_TEST(test_pending_conflicts_include_each_original_owner_field_and_image)
{
	static const struct {
		size_t offset;
		size_t width;
		uint64 value;
	} changes[] = { { 54, 2, 4 },	{ 56, 8, UINT64CONST(0x0000000312345678) }, { 64, 8, 92 },
					{ 72, 8, 102 }, { 80, 8, UINT64CONST(0x12345678a) },		{ 88, 8, 203 } };
	unsigned char primary[2176];
	unsigned char backup[2176];
	RelMapFile changed = native_map(1259, 49999);
	ClusterRelmapV2View view;

	fixture(primary, true);
	for (size_t i = 0; i < lengthof(changes); i++) {
		fixture(backup, true);
		put_le(backup + changes[i].offset, changes[i].value, changes[i].width);
		refresh_crc(backup);
		UT_ASSERT_EQ(cluster_relmap_v2_read_pair(primary, sizeof(primary), backup, sizeof(backup),
												 &identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_CONFLICT);
	}
	fixture(backup, true);
	memcpy(backup + 1152, &changed, sizeof(changed));
	refresh_crc(backup);
	UT_ASSERT_EQ(cluster_relmap_v2_read_pair(primary, sizeof(primary), backup, sizeof(backup),
											 &identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_CONFLICT);
}

UT_TEST(test_generation_regression_gap_and_unmatched_publish_conflict)
{
	unsigned char primary[2176];
	unsigned char backup[2176];
	ClusterRelmapV2View view;

	fixture(primary, true);
	make_published(backup);
	UT_ASSERT_EQ(cluster_relmap_v2_read_pair(primary, sizeof(primary), backup, sizeof(backup),
											 &identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_CONFLICT);
	make_published(primary);
	fixture(backup, false);
	UT_ASSERT_EQ(cluster_relmap_v2_read_pair(primary, sizeof(primary), backup, sizeof(backup),
											 &identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_CONFLICT);
	put_le(primary + 32, 9, 8);
	refresh_crc(primary);
	fixture(backup, true);
	UT_ASSERT_EQ(cluster_relmap_v2_read_pair(primary, sizeof(primary), backup, sizeof(backup),
											 &identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_CONFLICT);
}

int
main(void)
{
	UT_PLAN(20);
	UT_RUN(test_frozen_wire_decodes_without_native_header_cast);
	UT_RUN(test_encoder_matches_independent_frozen_bytes);
	UT_RUN(test_old_unknown_and_reserved_format_refused);
	UT_RUN(test_exact_length_required_and_unaligned_input_safe);
	UT_RUN(test_namespace_identity_bound_to_expected_root);
	UT_RUN(test_crc_covers_every_header_and_slot_byte);
	UT_RUN(test_native_images_checked_even_after_outer_crc_repaired);
	UT_RUN(test_no_pending_requires_zero_owner_and_entire_slot);
	UT_RUN(test_boot_admitted_and_original_fullxid_independently_required);
	UT_RUN(test_generation_zero_gap_and_overflow_refused);
	UT_RUN(test_invalid_encode_and_arguments_publish_no_output);
	UT_RUN(test_identical_copies_only_expose_committed_as_read_value);
	UT_RUN(test_stage_cut_retains_primary_pending_without_exposing_it);
	UT_RUN(test_publish_cut_preserves_backup_pending_obligation);
	UT_RUN(test_discard_cut_does_not_erase_backup_pending);
	UT_RUN(test_fallback_never_proves_complete_pending_coverage);
	UT_RUN(test_foreign_or_unknown_copy_refused_even_with_valid_other_copy);
	UT_RUN(test_same_generation_committed_conflict_rejected);
	UT_RUN(test_pending_conflicts_include_each_original_owner_field_and_image);
	UT_RUN(test_generation_regression_gap_and_unmatched_publish_conflict);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
