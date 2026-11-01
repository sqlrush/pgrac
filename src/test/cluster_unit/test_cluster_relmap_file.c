/*-------------------------------------------------------------------------
 * test_cluster_relmap_file.c
 *    Managed relmap file identity and real pair I/O contracts.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_file.c
 * NOTES
 *    Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "common/cluster_relmap_file.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterRecoveryFileIdentityV2 identity;
static ClusterRelmapV2Image image;
static char root[MAXPGPATH];
static int directory;
static unsigned syncs;
static unsigned fail_sync;
static bool corrupt_on_sync;
static unsigned closes, fail_close;
static RelMapFile *drift_map_on_sync;
static ClusterRecoveryFileIdentityV2 *drift_identity_on_sync;

static int
test_file_close(int fd)
{
	int rc = close(fd);
	++closes;
	if (closes == fail_close)
	{
		errno = EIO;
		return -1;
	}
	return rc;
}

static int
test_sync(int fd)
{
	++syncs;
	if (syncs == 3) {
		if (drift_map_on_sync) ++drift_map_on_sync->mappings[0].mapfilenumber;
		if (drift_identity_on_sync) ++drift_identity_on_sync->creation_generation;
	}
	if (corrupt_on_sync && syncs == 3)
	{
		int other = openat(directory, "pgrac_relmap_authority", O_WRONLY);
		char byte = 7;
		UT_ASSERT(other >= 0 && pwrite(other, &byte, 1, 140) == 1 && close(other) == 0);
	}
	if (syncs == fail_sync)
	{
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}
#define fsync test_sync
#define close test_file_close
#include "../../common/cluster_relmap_file_io.c"
#undef fsync
#undef close

static void
prepare(void)
{
	memset(&identity, 0, sizeof(identity));
	identity.system_identifier = 91;
	identity.database_incarnation = 27;
	identity.creation_generation = 103;
	identity.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	identity.origin = 0;
	identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	identity.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	identity.storage_uuid[0] = 1;
	identity.redo_branch_uuid[0] = 2;
	identity.redo_branch_generation = 3;
	identity.redo_timeline = 4;
	identity.tablespace_oid = 1664;
	identity.registration_generation = 19;
	memset(&image, 0, sizeof(image));
	image.identity = (ClusterRelmapV2Namespace) {91, 27, InvalidOid, true};
	image.committed_generation = 7;
	image.committed.magic = RELMAPPER_FILEMAGIC;
	image.committed.num_mappings = 1;
	image.committed.mappings[0] = (RelMapping) {1259, 9001};
	INIT_CRC32C(image.committed.crc);
	COMP_CRC32C(image.committed.crc, &image.committed, offsetof(RelMapFile, crc));
	FIN_CRC32C(image.committed.crc);
	strlcpy(root, "/tmp/pgrac-relmap-file-XXXXXX", sizeof(root));
	UT_ASSERT(mkdtemp(root) != NULL);
	directory = open(root, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
	UT_ASSERT(directory >= 0);
	syncs = fail_sync = closes = fail_close = 0;
	corrupt_on_sync = false;
	drift_map_on_sync = NULL;
	drift_identity_on_sync = NULL;
}

static void
cleanup(void)
{
	unlinkat(directory, "pgrac_relmap_authority", 0);
	unlinkat(directory, "pgrac_relmap_authority.bak", 0);
	UT_ASSERT(close(directory) == 0 && rmdir(root) == 0);
}

static void
create_pair(void)
{
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	image.committed_generation = 1;
	UT_ASSERT(cluster_relmap_file_encode_v2(&identity, &image, bytes, sizeof(bytes)));
	for (unsigned c = 0; c < 2; ++c)
	{
		int fd = openat(directory, c ? "pgrac_relmap_authority.bak" : "pgrac_relmap_authority",
			O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
		UT_ASSERT(fd >= 0 && write(fd, bytes, sizeof(bytes)) == sizeof(bytes)
			&& fsync(fd) == 0 && close(fd) == 0);
	}
}

UT_TEST(complete_envelope_preserves_inner_and_rejects_old_format)
{
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	unsigned char inner[CLUSTER_RELMAP_V2_FILE_SIZE];
	ClusterRelmapV2Image actual;
	prepare();
	UT_ASSERT(cluster_relmap_v2_encode(&image, inner, sizeof(inner)) == CLUSTER_RELMAP_V2_VALID);
	UT_ASSERT(cluster_relmap_file_encode_v2(&identity, &image, bytes, sizeof(bytes)));
	UT_ASSERT_EQ(sizeof(bytes), 2304);
	UT_ASSERT(memcmp(bytes + 128, inner, sizeof(inner)) == 0);
	UT_ASSERT(cluster_relmap_file_decode_v2(bytes, sizeof(bytes), &identity, &actual));
	UT_ASSERT_EQ(actual.committed_generation, 7);
	UT_ASSERT(!cluster_relmap_file_decode_v2(inner, sizeof(inner), &identity, &actual));
	cleanup();
}

UT_TEST(creation_identity_is_independent_of_map_generation)
{
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRecoveryFileIdentityV2 wrong;
	ClusterRelmapV2Image actual;
	prepare();
	UT_ASSERT(cluster_relmap_file_encode_v2(&identity, &image, bytes, sizeof(bytes)));
	wrong = identity;
	++wrong.creation_generation;
	UT_ASSERT(!cluster_relmap_file_decode_v2(bytes, sizeof(bytes), &wrong, &actual));
	image.committed_generation = 8;
	UT_ASSERT(cluster_relmap_file_encode_v2(&identity, &image, bytes, sizeof(bytes)));
	UT_ASSERT(cluster_relmap_file_decode_v2(bytes, sizeof(bytes), &identity, &actual));
	UT_ASSERT_EQ(actual.committed_generation, 8);
	cleanup();
}

UT_TEST(every_envelope_or_payload_byte_is_checked)
{
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRelmapV2Image actual;
	prepare();
	UT_ASSERT(cluster_relmap_file_encode_v2(&identity, &image, bytes, sizeof(bytes)));
	for (size_t i = 0; i < sizeof(bytes); ++i)
	{
		bytes[i] ^= 1;
		UT_ASSERT(!cluster_relmap_file_decode_v2(bytes, sizeof(bytes), &identity, &actual));
		bytes[i] ^= 1;
	}
	cleanup();
}

UT_TEST(real_pair_create_read_and_checkpoint_receipt)
{
	ClusterRelmapFilePairV2 pair;
	struct stat primary, backup;
	prepare();
	create_pair();
	UT_ASSERT(fstatat(directory, "pgrac_relmap_authority", &primary, AT_SYMLINK_NOFOLLOW) == 0);
	UT_ASSERT(fstatat(directory, "pgrac_relmap_authority.bak", &backup, AT_SYMLINK_NOFOLLOW) == 0);
	UT_ASSERT(primary.st_ino != backup.st_ino && primary.st_size == 2304 && backup.st_size == 2304);
	UT_ASSERT(cluster_relmap_file_pair_read_v2(directory, &identity, &pair));
	UT_ASSERT_EQ(pair.view.committed.mappings[0].mapfilenumber, 9001);
	syncs = 0;
	UT_ASSERT(cluster_relmap_file_pair_checkpoint_v2(directory, &identity, &pair));
	UT_ASSERT_EQ(syncs, 3);
	UT_ASSERT_EQ(pair.view.committed_generation, 1);

	cleanup();
}

UT_TEST(checkpoint_sync_and_late_corruption_never_publish_output)
{
	ClusterRelmapFilePairV2 pair, saved;
	prepare();
	create_pair();
	for (unsigned i = 1; i <= 3; ++i)
	{
		memset(&pair, 0xa5, sizeof(pair));
		saved = pair;
		syncs = 0;
		fail_sync = i;
		UT_ASSERT(!cluster_relmap_file_pair_checkpoint_v2(directory, &identity, &pair));
		UT_ASSERT(memcmp(&pair, &saved, sizeof(pair)) == 0);
	}
	fail_sync = 0;
	syncs = 0;
	corrupt_on_sync = true;
	UT_ASSERT(!cluster_relmap_file_pair_checkpoint_v2(directory, &identity, &pair));
	UT_ASSERT(memcmp(&pair, &saved, sizeof(pair)) == 0);
	cleanup();
}

UT_TEST(missing_short_alias_or_foreign_copy_refuses)
{
	ClusterRelmapFilePairV2 pair;
	int fd;
	prepare();
	create_pair();
	UT_ASSERT(unlinkat(directory, "pgrac_relmap_authority.bak", 0) == 0);
	UT_ASSERT(!cluster_relmap_file_pair_read_v2(directory, &identity, &pair));
	UT_ASSERT(symlinkat("pgrac_relmap_authority", directory, "pgrac_relmap_authority.bak") == 0);
	UT_ASSERT(!cluster_relmap_file_pair_read_v2(directory, &identity, &pair));
	cleanup();
	prepare();
	create_pair();
	fd = openat(directory, "pgrac_relmap_authority.bak", O_WRONLY);
	UT_ASSERT(fd >= 0 && ftruncate(fd, 2176) == 0 && close(fd) == 0);
	UT_ASSERT(!cluster_relmap_file_pair_read_v2(directory, &identity, &pair));
	cleanup();
}

UT_TEST(all_registration_fields_and_unknown_versions_refuse)
{
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRecoveryFileIdentityV2 changed;
	ClusterRelmapV2Image output, saved;
	prepare();
	UT_ASSERT(cluster_relmap_file_encode_v2(&identity, &image, bytes, sizeof(bytes)));
	memset(&output, 0xa5, sizeof(output));
	saved = output;
	for (unsigned field = 0; field < 16; ++field)
	{
		changed = identity;
		switch (field)
		{
			case 0: ++changed.format_id; break;
			case 1: ++changed.format_version; break;
			case 2: changed.kind = CLUSTER_RECOVERY_FILE_CLOG_V2; break;
			case 3: ++changed.origin; break;
			case 4: ++changed.system_identifier; break;
			case 5: ++changed.database_incarnation; break;
			case 6: changed.storage_uuid[15] ^= 1; break;
			case 7: changed.redo_branch_uuid[15] ^= 1; break;
			case 8: ++changed.redo_branch_generation; break;
			case 9: ++changed.redo_timeline; break;
			case 10: ++changed.database_oid; break;
			case 11: ++changed.tablespace_oid; break;
			case 12: ++changed.object_id; break;
			case 13: ++changed.creation_generation; break;
			case 14: ++changed.registration_generation; break;
			case 15: changed.creation_generation = 0; break;
		}
		UT_ASSERT(!cluster_relmap_file_decode_v2(bytes, sizeof(bytes), &changed, &output));
		UT_ASSERT(memcmp(&output, &saved, sizeof(output)) == 0);
	}
	cleanup();
}

UT_TEST(overlapping_outputs_and_late_file_close_never_mutate_identity)
{
	union {
		ClusterRelmapFilePairV2 pair;
		ClusterRelmapV2Image image;
		ClusterRecoveryFileIdentityV2 identity;
	} alias;
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRelmapFilePairV2 pair, saved;
	prepare();
	create_pair();
	UT_ASSERT(cluster_relmap_file_encode_v2(&identity, &image, bytes, sizeof(bytes)));
	memset(&alias, 0, sizeof(alias));
	alias.identity = identity;
	UT_ASSERT(!cluster_relmap_file_encode_v2(&alias.identity, &image, &alias, sizeof(bytes)));
	UT_ASSERT(!cluster_relmap_file_decode_v2(bytes, sizeof(bytes), &alias.identity, &alias.image));
	UT_ASSERT(!cluster_relmap_file_pair_read_v2(directory, &alias.identity, &alias.pair));
	UT_ASSERT_EQ(alias.identity.creation_generation, identity.creation_generation);
	memset(&pair, 0xa5, sizeof(pair));
	saved = pair;
	for (unsigned i = 1; i <= 2; ++i)
	{
		closes = 0;
		fail_close = i;
		UT_ASSERT(!cluster_relmap_file_pair_read_v2(directory, &identity, &pair));
		UT_ASSERT(memcmp(&pair, &saved, sizeof(pair)) == 0);
	}
	cleanup();
}

UT_TEST(assigned_format_matches_the_complete_big_endian_byte_table)
{
	static const unsigned char expected[112] = {
		[0] = 'R', [1] = 'F', [2] = 'M', [3] = '2',
		[5] = 2, [7] = 128, [9] = 3,
		[12] = 0x45, [13] = 0x31, [14] = 0x43, [15] = 0x32,
		[19] = 2, [23] = 4, [31] = 91, [39] = 27,
		[40] = 1, [56] = 2, [79] = 3, [86] = 6, [87] = 128,
		[103] = 103, [111] = 19
	};
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	pg_crc32c crc;
	prepare();
	UT_ASSERT(cluster_relmap_file_encode_v2(&identity, &image, bytes, sizeof(bytes)));
	UT_ASSERT(memcmp(bytes, expected, sizeof(expected)) == 0);
	UT_ASSERT(bytes[112] == 0 && bytes[113] == 0 && bytes[114] == 8 && bytes[115] == 128);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 120);
	COMP_CRC32C(crc, bytes + 128, CLUSTER_RELMAP_V2_FILE_SIZE);
	FIN_CRC32C(crc);
	for (unsigned i = 0; i < 4; ++i)
		UT_ASSERT_EQ(bytes[120 + i], (crc >> (24 - i * 8)) & 255);
	cleanup();
}

#include "test_cluster_relmap_file_replay_create.inc"
#include "test_cluster_relmap_file_create_wal.inc"

int
main(void)
{
	UT_PLAN(22);
	UT_RUN(assigned_format_matches_the_complete_big_endian_byte_table);
	UT_RUN(complete_envelope_preserves_inner_and_rejects_old_format);
	UT_RUN(creation_identity_is_independent_of_map_generation);
	UT_RUN(every_envelope_or_payload_byte_is_checked);
	UT_RUN(real_pair_create_read_and_checkpoint_receipt);
	UT_RUN(checkpoint_sync_and_late_corruption_never_publish_output);
	UT_RUN(missing_short_alias_or_foreign_copy_refuses);
	UT_RUN(all_registration_fields_and_unknown_versions_refuse);
	UT_RUN(overlapping_outputs_and_late_file_close_never_mutate_identity);
	UT_RUN(replay_missing_pair_creates_exact_generation_one);
	UT_RUN(replay_exact_pair_only_syncs_and_preserves_both_inodes);
	UT_RUN(replay_partial_pairs_keep_the_missing_name_and_original_output);
	UT_RUN(replay_changed_birth_map_generation_or_pending_never_rewrites);
	UT_RUN(replay_sync_late_corruption_and_close_failure_keep_output);
	UT_RUN(replay_alias_invalid_image_and_linked_names_refuse);
	UT_RUN(replay_late_input_drift_cannot_publish_saved_output);
	UT_RUN(creation_wal_roundtrip_preserves_full_identity_batch_and_generation_one);
	UT_RUN(creation_wal_every_byte_and_exact_length_are_checked);
	UT_RUN(creation_wal_rejects_valid_crc_but_wrong_state_birth_and_pending);
	UT_RUN(creation_rebuild_requires_independent_identity_batch_and_actual_end);
	UT_RUN(creation_wal_alias_and_invalid_input_do_not_change_any_output);
	UT_RUN(creation_wal_matches_frozen_header_and_rejects_old_layout);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
