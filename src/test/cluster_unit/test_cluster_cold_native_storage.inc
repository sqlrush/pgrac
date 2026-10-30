/*-------------------------------------------------------------------------
 * test_cluster_cold_native_storage.inc
 *     Original Startup record owner through native FULL CLOG redo and finish.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_native_storage.inc
 * NOTES
 *     Source owner, native callback, CLOG/SLRU and files execute. The original
 *     fence/ROOT/SIDE capture boundaries remain fixtures, not native grants.
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "test_cluster_recovery_episode_clog.h"

static struct {
	char root[MAXPGPATH], cwd[MAXPGPATH];
	XLogReaderState reader;
	xl_clog_zeropage_full birth;
	xl_clog_truncate_full cut;
	ClusterColdTypedV1 *typed;
	TestEpisodeClogReplayStats stats;
	unsigned fault, calls;
	bool truncate;
	DecodedXLogRecord decoded;
} native_storage;

/* This program uses the real Startup owner, not the separate relmap-input
 * fixture. Reaching its alternate cold leaf is always a test failure. */
bool
cluster_wal_inputs_relmap_source_current_v1(const ClusterRecoveryRelmapSourceV1 *source)
{
	UT_ASSERT(false);
	return false;
}

static void
native_storage_path(char *path, const char *suffix)
{
	int length = snprintf(path, MAXPGPATH, "%s/%s", native_storage.root, suffix);
	UT_ASSERT(length > 0 && length < MAXPGPATH);
}

static void
native_storage_begin(bool truncate)
{
	char path[MAXPGPATH], target[MAXPGPATH];
	XLogReaderState *reader = &native_storage.reader;
	DecodedXLogRecord *decoded = &native_storage.decoded;

	memset(&native_storage, 0, sizeof(native_storage));
	native_storage.truncate = truncate;
	native_storage.typed = native_source_fixture(reader, decoded);
	UT_ASSERT(getcwd(native_storage.cwd, sizeof(native_storage.cwd)) != NULL);
	strlcpy(native_storage.root, "/tmp/pgrac-d-source-native-XXXXXX",
		sizeof(native_storage.root));
	UT_ASSERT(mkdtemp(native_storage.root) != NULL);
	/* Resolve the platform's temporary-directory alias before native I/O. */
	UT_ASSERT(realpath(native_storage.root, path) != NULL);
	strlcpy(native_storage.root, path, sizeof(native_storage.root));
	native_storage_path(path, "native_side"); UT_ASSERT_EQ(mkdir(path, 0700), 0);
	native_storage_path(path, "native_side/origin_0"); UT_ASSERT_EQ(mkdir(path, 0700), 0);
	native_storage_path(target, "native_side/origin_0/pg_xact"); UT_ASSERT_EQ(mkdir(target, 0700), 0);
	native_storage_path(path, "pg_xact"); UT_ASSERT_EQ(symlink(target, path), 0);
	UT_ASSERT_EQ(chdir(native_storage.root), 0);
	test_episode_native_clog_start(native_storage.root);
	test_episode_native_clog_record_guard(cluster_cold_typed_clog_native_record_current_v1);
	test_episode_native_clog_local_bounds(true, (UINT64_C(7) << 32) | UINT64_C(8388608),
		FirstNormalTransactionId);
	decoded->max_block_id = -1;
	decoded->header.xl_info = CLOG_ZEROPAGE_FULL;
	decoded->header.xl_scn = 43;
	decoded->main_data = (char *)&native_storage.birth;
	decoded->main_data_len = sizeof(native_storage.birth);
	native_storage.birth = (xl_clog_zeropage_full){128, 0,
		(UINT64_C(7) << 32) | UINT64_C(4194304)};
	if (truncate) {
		/* Setup only: create original old/retained native files and cache.
		 * Neither setup redo is claimed as source-owned consumption. */
		native_storage.birth = (xl_clog_zeropage_full){0, 0, (UINT64_C(7) << 32) | 3};
		test_episode_native_clog_redo(reader);
		native_storage.birth = (xl_clog_zeropage_full){128, 0,
			(UINT64_C(7) << 32) | UINT64_C(4194304)};
		test_episode_native_clog_redo(reader);
		native_storage.cut = (xl_clog_truncate_full){128, 4194321, 79, 0,
			(UINT64_C(7) << 32) | UINT64_C(4194321)};
		decoded->header.xl_info = CLOG_TRUNCATE_FULL;
		decoded->main_data = (char *)&native_storage.cut;
		decoded->main_data_len = sizeof(native_storage.cut);
	}
}

static void
native_storage_end(void)
{
	char path[MAXPGPATH];
	const char *files[] = {"native_side/origin_0/pg_xact/0000",
		"native_side/origin_0/pg_xact/0004", "pg_xact"};
	const char *dirs[] = {"native_side/origin_0/pg_xact", "native_side/origin_0", "native_side"};

	test_episode_native_clog_record_guard(NULL);
	test_episode_native_clog_local_bounds(false, 0, 0);
	test_episode_native_clog_stop();
	UT_ASSERT_EQ(chdir(native_storage.cwd), 0);
	for (unsigned i = 0; i < lengthof(files); ++i) {
		native_storage_path(path, files[i]);
		UT_ASSERT(unlink(path) == 0 || errno == ENOENT);
	}
	for (unsigned i = 0; i < lengthof(dirs); ++i) {
		native_storage_path(path, dirs[i]); UT_ASSERT_EQ(rmdir(path), 0);
	}
	UT_ASSERT_EQ(rmdir(native_storage.root), 0);
	native_source_done(&native_storage.typed);
}

static void
native_storage_redo(XLogReaderState *reader, void *arg)
{
	XLogReaderState copy = *reader;

	UT_ASSERT(arg == &native_storage.calls);
	++native_storage.calls;
	test_episode_native_clog_replay_record(native_storage.fault == 3 ? &copy : reader, 1,
		native_storage.fault <= 2 ? native_storage.fault : 0, &native_storage.stats);
	if (native_storage.fault == 4) ++tokens[3].file_txn_seq;
}

static bool
native_storage_consume(bool *caught)
{
	volatile bool ok = false, error = false;

	PG_TRY(); {
		ok = cluster_cold_typed_clog_native_v1(native_storage.typed,
			(ClusterRecoveryFencePlan *)&fixture_context, &native_storage.reader,
			native_storage_redo, &native_storage.calls);
	} PG_CATCH(); { error = true; } PG_END_TRY();
	*caught = error;
	UT_ASSERT(!cluster_cold_typed_clog_native_record_current_v1(0, &native_storage.reader));
	return ok;
}

static void
native_storage_bytes(void)
{
	char path[MAXPGPATH];
	PGAlignedBlock bytes;
	struct stat info;
	int fd;

	native_storage_path(path, "native_side/origin_0/pg_xact/0004");
	fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
	UT_ASSERT(fd >= 0);
	if (fd < 0) return;
	UT_ASSERT_EQ(fstat(fd, &info), 0);
	UT_ASSERT_EQ(info.st_size, BLCKSZ);
	UT_ASSERT_EQ(pread(fd, bytes.data, BLCKSZ, 0), BLCKSZ);
	for (unsigned i = 0; i < BLCKSZ; ++i) UT_ASSERT_EQ(bytes.data[i], 0);
	UT_ASSERT_EQ(close(fd), 0);
	if (native_storage.truncate) {
		native_storage_path(path, "native_side/origin_0/pg_xact/0000");
		UT_ASSERT(lstat(path, &info) != 0 && errno == ENOENT);
	}
}

UT_TEST(test_source_native_clog_finishes_original_records_without_cold_scope)
{
	for (unsigned truncate = 0; truncate < 2; ++truncate) {
		bool caught;

		native_storage_begin(truncate);
		UT_ASSERT(native_storage_consume(&caught) && !caught);
		UT_ASSERT_EQ(native_storage.calls, 1);
		UT_ASSERT_EQ(native_storage.stats.redos, 1);
		UT_ASSERT_EQ(native_storage.stats.touches, 1);
		UT_ASSERT(native_storage.stats.returned);
		UT_ASSERT(native_storage.stats.directory_syncs > 0);
		UT_ASSERT_EQ(native_storage.stats.file_syncs > 0, !truncate);
		UT_ASSERT_EQ(native_storage.typed->refusal, CLUSTER_COLD_OK);
		UT_ASSERT_EQ(native_source_checks, 2);
		native_storage_bytes();
		native_storage_end();
	}
}

UT_TEST(test_source_native_clog_rejects_unowned_and_substituted_reader)
{
	for (unsigned truncate = 0; truncate < 2; ++truncate) {
		bool caught;
		volatile bool unowned_error = false;

		native_storage_begin(truncate);
		/* The complete original callback cannot finish outside its owner. */
		PG_TRY(); {
			test_episode_native_clog_replay_record(&native_storage.reader, 1, 0, &native_storage.stats);
		} PG_CATCH(); { unowned_error = true; } PG_END_TRY();
		UT_ASSERT(unowned_error);
		UT_ASSERT(!native_storage.stats.returned);
		UT_ASSERT(!native_source_attempted);
		native_storage_end();

		native_storage_begin(truncate);
		native_storage.fault = 3;
		UT_ASSERT(!native_storage_consume(&caught) && caught);
		UT_ASSERT_EQ(native_storage.calls, 1);
		UT_ASSERT(!native_storage.stats.returned);
		UT_ASSERT_NE(native_storage.typed->refusal, CLUSTER_COLD_OK);
		UT_ASSERT(!native_storage_consume(&caught) && !caught);
		UT_ASSERT_EQ(native_storage.calls, 1);
		native_storage_end();
	}
}

UT_TEST(test_source_native_clog_sync_errors_preserve_attempt_and_bytes)
{
	for (unsigned fault = 1; fault <= 2; ++fault) {
		bool caught;

		native_storage_begin(false);
		native_storage.fault = fault;
		UT_ASSERT(!native_storage_consume(&caught) && caught);
		UT_ASSERT(native_source_attempted);
		UT_ASSERT(!native_storage.stats.returned);
		UT_ASSERT_EQ(native_storage.stats.redos, 1);
		UT_ASSERT_NE(native_storage.typed->refusal, CLUSTER_COLD_OK);
		native_storage_bytes();
		native_storage.fault = 0;
		UT_ASSERT(!native_storage_consume(&caught) && !caught);
		UT_ASSERT_EQ(native_storage.calls, 1);
		native_storage_end();
	}
}

UT_TEST(test_source_native_clog_late_source_change_cannot_retry)
{
	for (unsigned truncate = 0; truncate < 2; ++truncate) {
		bool caught;

		native_storage_begin(truncate);
		native_storage.fault = 4;
		UT_ASSERT(!native_storage_consume(&caught) && !caught);
		UT_ASSERT(native_storage.stats.returned);
		UT_ASSERT_EQ(native_storage.stats.redos, 1);
		UT_ASSERT_NE(native_storage.typed->refusal, CLUSTER_COLD_OK);
		native_storage_bytes();
		UT_ASSERT(!native_storage_consume(&caught) && !caught);
		UT_ASSERT_EQ(native_storage.calls, 1);
		native_storage_end();
	}
}
