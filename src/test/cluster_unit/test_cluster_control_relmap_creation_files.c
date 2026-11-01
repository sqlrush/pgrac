/* Original control reservation -> actual managed pair I/O -> control completion.
 * Creator/CF/source authority remains the inherited controlled fixture. */
#define TEST_CONTROL_CLOG_PREPARATION 1
#include "test_cluster_control_native_reservations.c"
#undef main
#include "storage/fd.h"

bool enableFsync = true;
static unsigned pair_syncs, pair_fail_sync, pair_closes, pair_fail_close;

static int
creation_pair_sync(int fd)
{
	if (++pair_syncs == pair_fail_sync) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}
static int
creation_pair_close(int fd)
{
	int rc = close(fd);
	if (++pair_closes == pair_fail_close) {
		errno = EIO;
		return -1;
	}
	return rc;
}
#define pg_fsync creation_pair_sync
#define close creation_pair_close
#include "../../common/cluster_relmap_file_io.c"
#undef pg_fsync
#undef close

static char target_path[MAXPGPATH];
static int target_dir = -1;
static ClusterControlNativeRelmapCreateV2 *target;
static ClusterRecoveryCheckpointFileSourceV2 receipt_sources[2];
static const char *const target_names[] = {
	"pgrac_relmap_authority", "pgrac_relmap_authority.bak"
};

static void
files_begin(void)
{
	table_reset();
	owner_begin();
	target = NULL;
	pair_syncs = pair_fail_sync = pair_closes = pair_fail_close = 0;
	UT_ASSERT_EQ(cluster_control_native_files_reserve_relmap_create_locked_v2(
					 2000, 1663, &target), CLUSTER_NATIVE_FILES_OK_V2);
	strlcpy(target_path, "/tmp/pgrac-relmap-create-XXXXXX", sizeof(target_path));
	UT_ASSERT(mkdtemp(target_path) != NULL);
	target_dir = open(target_path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
	UT_ASSERT(target_dir >= 0 && target != NULL);
	if (!target || target_dir < 0)
		abort();
}
static void
files_end(void)
{
	/* Local destruction does not cancel the persistent reservation or original
	 * creator duty. Only this disposable fixture removes its own files. */
	cluster_control_native_relmap_create_destroy_v2(&target);
	for (unsigned i = 0; i < 2; ++i)
		(void)unlinkat(target_dir, target_names[i], 0);
	UT_ASSERT(close(target_dir) == 0 && rmdir(target_path) == 0);
	target_dir = -1;
	batch_end();
}
static void
create_target_pair(const ClusterRecoveryFileIdentityV2 *expected)
{
	ClusterRelmapV2Image image = { 0 };
	uint8 bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	image.identity = (ClusterRelmapV2Namespace) {
		expected->system_identifier, expected->database_incarnation, expected->database_oid, false
	};
	image.committed_generation = 1;
	image.committed.magic = RELMAPPER_FILEMAGIC;
	image.committed.num_mappings = 1;
	image.committed.mappings[0] = (RelMapping) { 1259, 9001 };
	INIT_CRC32C(image.committed.crc);
	COMP_CRC32C(image.committed.crc, &image.committed, offsetof(RelMapFile, crc));
	FIN_CRC32C(image.committed.crc);
	UT_ASSERT(cluster_relmap_file_encode_v2(expected, &image, bytes, sizeof(bytes)));
	for (unsigned i = 0; i < 2; ++i) {
		int fd = openat(target_dir, target_names[i],
						O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
		UT_ASSERT(fd >= 0);
		if (fd < 0)
			abort();
		UT_ASSERT(write(fd, bytes, sizeof(bytes)) == sizeof(bytes));
		UT_ASSERT(close(fd) == 0);
	}
}
static bool
file_receipts(ClusterRecoveryCheckpointFileV2 out[2])
{
	ClusterRelmapFilePairV2 pair;
	ClusterRecoveryCheckpointFileV2 receipts[2] = { 0 };
	ClusterControlRelmapSelectionV2 *selection = NULL;
	bool ok = false;

	/* No receipt is synthesized if the original physical checkpoint refuses. */
	if (!cluster_relmap_file_pair_checkpoint_v2(target_dir, &target->expected, &pair)
		|| cluster_control_root_relmap_selection_read_locked_v4(
			&expected_config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, &selection) != 0)
		return false;
	if (cluster_control_root_relmap_selection_source_count_v4(selection) != 2)
		goto done;
	for (unsigned i = 0; i < 2; ++i) {
		ClusterRecoveryCheckpointSourceV2 source;
		if (!cluster_control_root_relmap_selection_source_at_v4(selection, i, &source))
			goto done;
		receipt_sources[i] = (ClusterRecoveryCheckpointFileSourceV2) {
			i, source.redo_start, source.replay_end
		};
	}
	for (unsigned i = 0; i < 2; ++i) {
		receipts[i].identity = target->expected;
		receipts[i].synced_bytes = sizeof(pair.bytes[i]);
		receipts[i].page_count = 1;
		receipts[i].copy_index = i;
		receipts[i].copy_count = 2;
		receipts[i].source_count = 2;
		receipts[i].sources = receipt_sources;
		fixture_sha(pair.bytes[i], sizeof(pair.bytes[i]), receipts[i].image_digest);
	}
	memcpy(out, receipts, sizeof(receipts));
	ok = true;
done:
	cluster_control_root_relmap_selection_destroy_v4(&selection);
	return ok;
}
static void
assert_reserved_files_retained(void)
{
	ClusterControlNativeFileBorrowV2 borrow = { 0 };
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 id;
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
					 CLUSTER_RECOVERY_FILE_RELMAP_V2, 2000, 1663, 0, &borrow),
				 CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2);
	UT_ASSERT_EQ(cluster_control_root_relmap_selection_read_locked_v4(
					 &expected_config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, &s), 0);
	if (s) {
		UT_ASSERT(cluster_control_root_reserved_file_locked_v4(
					  s, target->batch_uuid, &target->expected, &id));
		UT_ASSERT(identity_same(&id, &target->expected));
	}
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(fcntl(target_dir, F_GETFD) >= 0 && cf_held && batch_exclusive);
}

UT_TEST(real_pair_receipts_publish_then_ordinary_reader_uses_target_identity)
{
	ClusterRecoveryCheckpointFileV2 receipts[2];
	ClusterControlNativeFileBorrowV2 borrow = { 0 };
	ClusterRelmapFilePairV2 pair;
	uint8 digest[32];
	files_begin();
	UT_ASSERT_EQ(cluster_control_native_file_reserved_acquire_locked_v2(
					 &target->control.config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2,
					 target->startup_generation, target->batch_uuid, &target->expected, &borrow), 0);
	create_target_pair(&borrow.expected);
	UT_ASSERT(file_receipts(receipts));
	UT_ASSERT_EQ(pair_syncs, 3);
	UT_ASSERT(cluster_control_native_file_current_v2(&borrow));
	/* Same original CF-X/creator protection continues across table replacement. */
	UT_ASSERT(cluster_control_native_file_release_v2(&borrow));
	UT_ASSERT_EQ(cluster_control_native_files_complete_relmap_create_locked_v2(target, receipts, 2), 0);
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
					 CLUSTER_RECOVERY_FILE_RELMAP_V2, 2000, 1663, 0, &borrow), 0);
	UT_ASSERT(identity_same(&borrow.expected, &target->expected));
	UT_ASSERT(cluster_relmap_file_pair_read_v2(target_dir, &borrow.expected, &pair));
	UT_ASSERT_EQ(pair.view.committed.mappings[0].mapfilenumber, 9001);
	fixture_sha(pair.bytes[0], sizeof(pair.bytes[0]), digest);
	UT_ASSERT(memcmp(digest, receipts[0].image_digest, sizeof(digest)) == 0);
	UT_ASSERT_EQ(root_renames, 4);
	UT_ASSERT(cluster_control_native_file_release_v2(&borrow));
	files_end();
}
UT_TEST(each_sync_failure_keeps_receipt_output_and_persistent_creation_debt)
{
	for (unsigned phase = 1; phase <= 3; ++phase) {
		ClusterRecoveryCheckpointFileV2 receipts[2], before[2];
		files_begin();
		create_target_pair(&target->expected);
		memset(receipts, 0xa5, sizeof(receipts));
		memcpy(before, receipts, sizeof(before));
		pair_fail_sync = phase;
		UT_ASSERT(!file_receipts(receipts));
		UT_ASSERT(memcmp(before, receipts, sizeof(before)) == 0);
		UT_ASSERT_EQ(pair_syncs, phase);
		UT_ASSERT(!target->private_attempted && root_renames == 2);
		assert_reserved_files_retained();
		files_end();
	}
}
UT_TEST(missing_partial_corrupt_and_wrong_birth_never_produce_receipts)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		ClusterRecoveryCheckpointFileV2 receipts[2], before[2];
		ClusterRecoveryFileIdentityV2 expected;
		files_begin();
		expected = target->expected;
		if (fault == 3)
			expected.creation_generation++;
		create_target_pair(&expected);
		if (fault == 0)
			UT_ASSERT(unlinkat(target_dir, target_names[1], 0) == 0);
		else if (fault < 3) {
			int fd = openat(target_dir, target_names[1], O_WRONLY | O_NOFOLLOW);
			uint8 byte = 0xff;
			UT_ASSERT(fd >= 0);
			if (fault == 1)
				UT_ASSERT(ftruncate(fd, 128) == 0);
			else
				UT_ASSERT(pwrite(fd, &byte, 1, 140) == 1);
			UT_ASSERT(close(fd) == 0);
		}
		memset(receipts, 0xa5, sizeof(receipts));
		memcpy(before, receipts, sizeof(before));
		UT_ASSERT(!file_receipts(receipts));
		UT_ASSERT(memcmp(before, receipts, sizeof(before)) == 0);
		UT_ASSERT_EQ(root_renames, 2);
		assert_reserved_files_retained();
		files_end();
	}
}
UT_TEST(late_close_failure_cannot_turn_read_bytes_into_receipts)
{
	ClusterRecoveryCheckpointFileV2 receipts[2], before[2];
	files_begin();
	create_target_pair(&target->expected);
	memset(receipts, 0xa5, sizeof(receipts));
	memcpy(before, receipts, sizeof(before));
	pair_fail_close = 2;
	UT_ASSERT(!file_receipts(receipts));
	UT_ASSERT(memcmp(before, receipts, sizeof(before)) == 0);
	UT_ASSERT_EQ(pair_syncs, 3);
	UT_ASSERT_EQ(pair_closes, 2);
	assert_reserved_files_retained();
	files_end();
}
UT_TEST(durable_pair_cannot_replace_exact_selected_source_intervals)
{
	ClusterRecoveryCheckpointFileV2 receipts[2];
	ClusterRelmapFilePairV2 pair;
	files_begin();
	create_target_pair(&target->expected);
	UT_ASSERT(file_receipts(receipts));
	receipt_sources[1].replay_end++;
	UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(target, receipts, 2) != 0);
	UT_ASSERT(target->private_attempted && root_renames == 2);
	UT_ASSERT(cluster_relmap_file_pair_read_v2(target_dir, &target->expected, &pair));
	assert_reserved_files_retained();
	UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(target, receipts, 2) != 0);
	files_end();
}
UT_TEST(late_owner_loss_keeps_durable_files_and_refuses_publication)
{
	ClusterRecoveryCheckpointFileV2 receipts[2];
	ClusterRelmapFilePairV2 pair;
	files_begin();
	create_target_pair(&target->expected);
	UT_ASSERT(file_receipts(receipts));
	owner_ok = false;
	UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(target, receipts, 2) != 0);
	owner_ok = true;
	UT_ASSERT(target->private_attempted && root_renames == 2);
	UT_ASSERT(cluster_relmap_file_pair_read_v2(target_dir, &target->expected, &pair));
	assert_reserved_files_retained();
	files_end();
}
UT_TEST(publication_failure_keeps_real_pair_and_once_attempt)
{
	ClusterRecoveryCheckpointFileV2 receipts[2];
	ClusterRelmapFilePairV2 pair;
	unsigned after;
	files_begin();
	create_target_pair(&target->expected);
	UT_ASSERT(file_receipts(receipts));
	fail_rename = 3;
	UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(target, receipts, 2) != 0);
	UT_ASSERT(target->private_attempted);
	after = root_renames;
	UT_ASSERT(cluster_relmap_file_pair_read_v2(target_dir, &target->expected, &pair));
	assert_reserved_files_retained();
	UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(target, receipts, 2) != 0);
	UT_ASSERT_EQ(root_renames, after);
	files_end();
}
static void
error_after_published_pair(void)
{
	if (root_renames < 4)
		rename_hook = error_after_published_pair;
	else
		config_error = true;
}
UT_TEST(error_after_root_write_retains_real_files_and_no_retry)
{
	ClusterRecoveryCheckpointFileV2 receipts[2];
	ClusterControlNativeFileBorrowV2 borrow = { 0 };
	ClusterRelmapFilePairV2 pair;
	volatile bool caught = false;
	files_begin();
	create_target_pair(&target->expected);
	UT_ASSERT(file_receipts(receipts));
	rename_hook = error_after_published_pair;
	PG_TRY();
	{
		(void)cluster_control_native_files_complete_relmap_create_locked_v2(target, receipts, 2);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	config_error = false;
	rename_hook = NULL;
	UT_ASSERT(caught && target->private_attempted && root_renames == 4);
	UT_ASSERT(cluster_relmap_file_pair_read_v2(target_dir, &target->expected, &pair));
	UT_ASSERT(fcntl(target_dir, F_GETFD) >= 0 && cf_held && batch_exclusive);
	/* Durable ROOT may already name the pair; failed caller cannot publish its
	 * shared table or retry, and original creation responsibility remains. */
	UT_ASSERT_EQ(cluster_control_native_file_borrow_by_address_v2(
					 CLUSTER_RECOVERY_FILE_RELMAP_V2, 2000, 1663, 0, &borrow),
				 CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2);
	UT_ASSERT(cluster_control_native_files_complete_relmap_create_locked_v2(target, receipts, 2) != 0);
	UT_ASSERT_EQ(root_renames, 4);
	files_end();
}
int
main(int argc, char **argv)
{
	(void)argc;
	(void)argv;
	UT_RUN(real_pair_receipts_publish_then_ordinary_reader_uses_target_identity);
	UT_RUN(each_sync_failure_keeps_receipt_output_and_persistent_creation_debt);
	UT_RUN(missing_partial_corrupt_and_wrong_birth_never_produce_receipts);
	UT_RUN(late_close_failure_cannot_turn_read_bytes_into_receipts);
	UT_RUN(durable_pair_cannot_replace_exact_selected_source_intervals);
	UT_RUN(late_owner_loss_keeps_durable_files_and_refuses_publication);
	UT_RUN(publication_failure_keeps_real_pair_and_once_attempt);
	UT_RUN(error_after_root_write_retains_real_files_and_no_retry);
	UT_DONE();
	return ut_failed_count != 0;
}
