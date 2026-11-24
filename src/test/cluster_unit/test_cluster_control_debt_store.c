/*-------------------------------------------------------------------------
 * test_cluster_control_debt_store.c
 *     Immutable PREPARED debt storage and exact-reference reconstruction.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
static unsigned writes, syncs, reads, fail_write, fail_sync, fail_read;
static unsigned mutate_sync;
static void mutate_page(void);
static ssize_t store_pwrite(int fd, const void *p, size_t n, off_t at)
{
	if (++writes == fail_write) { errno = EIO; return -1; }
	return pwrite(fd, p, n, at);
}
static ssize_t store_pread(int fd, void *p, size_t n, off_t at)
{
	ssize_t result;
	if (++reads == fail_read) { errno = EIO; return -1; }
	result = pread(fd, p, n, at);
#ifdef CONTROL_DEBT_STORE_TEST_READ_HOOK
	CONTROL_DEBT_STORE_TEST_READ_HOOK();
#endif
	return result;
}
static int store_sync(int fd)
{
	if (++syncs == fail_sync) { errno = EIO; return -1; }
	if (syncs == mutate_sync) mutate_page();
	return fsync(fd);
}
#define CONTROL_DEBT_STORE_PWRITE store_pwrite
#define CONTROL_DEBT_STORE_PREAD store_pread
#define CONTROL_DEBT_STORE_SYNC store_sync
#define DEBT_WIRE_TEST_MAIN debt_store_old_wire_main
#include "test_cluster_control_debt_wire.c"
static char store_path[MAXPGPATH];
static int store_global;
static ControlDebtWireImageV1 *image;
static void reset_faults(void)
{
	writes = syncs = reads = fail_write = fail_sync = fail_read = mutate_sync = 0;
}
static void store_begin(uint64 count)
{
	uint8 ref[128];
	reset_faults(); debt_fixture(count);
	strcpy(store_path, "/tmp/pgrac-debt-store-XXXXXX");
	if (!mkdtemp(store_path)) abort();
	store_global = open(store_path, O_RDONLY | O_DIRECTORY);
	if (store_global < 0 || !cluster_recovery_registry_create_v3(store_global,
		&debt_registry, DEBT_TEST_BUDGET, ref) || memcmp(ref, debt_original_ref, 128)
		|| !debt_encode(&image, 1)) abort();
}
static void store_end(void)
{
	/* Only this test's freshly created files are removed. Production failure
	 * and destruction must never remove an uncertain immutable object. */
	int dir = openat(store_global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
	DIR *entries = dir < 0 ? NULL : fdopendir(dir);
	struct dirent *entry;
	if (entries) {
		while ((entry = readdir(entries)))
			if (strcmp(entry->d_name, ".") && strcmp(entry->d_name, ".."))
				if (unlinkat(dir, entry->d_name, 0)) abort();
		closedir(entries);
	}
	if (unlinkat(store_global, "pgrac_recovery_registry", AT_REMOVEDIR)) abort();
	close(store_global); if (rmdir(store_path)) abort();
	control_debt_wire_destroy_v1(&image); debt_fixture_end();
}
static bool store_create(ControlDebtStoreAttemptV1 *attempt, uint8 ref[128])
{
	return control_debt_store_create_v1(store_global, image, debt_batches, 1,
		DEBT_TEST_BUDGET, attempt, ref);
}
static void mutate_page(void)
{
	uint8 hash[32], byte;
	char name[96];
	int dir, fd;
	debt_sha(image->pages, 33024, hash); debt_store_name(hash, false, name);
	dir = openat(store_global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
	fd = openat(dir, name, O_RDWR);
	if (fd < 0 || pread(fd, &byte, 1, 257) != 1) abort();
	byte ^= 1;
	if (pwrite(fd, &byte, 1, 257) != 1) abort();
	close(fd); close(dir);
}
UT_TEST(store_pages_reload_without_input_vectors)
{
	static const uint64 counts[] = {1, 64, 65, 128, 129, 130};
	for (unsigned i = 0; i < lengthof(counts); i++) {
		ControlDebtStoreAttemptV1 attempt = {0};
		ControlDebtWireImageV1 *loaded = NULL;
		uint8 ref[128];
		store_begin(counts[i]);
		UT_ASSERT(store_create(&attempt, ref));
		UT_ASSERT(attempt.attempted);
		UT_ASSERT(!memcmp(ref, image->reference, 128));
		memset(debt_entries, 0, sizeof(debt_entries));
		memset(debt_batches, 0, sizeof(debt_batches));
		UT_ASSERT(control_debt_store_load_v1(store_global, ref, DEBT_TEST_BUDGET, &loaded));
		if (loaded) {
			UT_ASSERT_EQ(loaded->page_bytes, image->page_bytes);
			UT_ASSERT(!memcmp(loaded->pages, image->pages, loaded->page_bytes));
			UT_ASSERT(!memcmp(loaded->registry, image->registry, loaded->registry_bytes));
		}
		control_debt_wire_destroy_v1(&loaded); store_end();
	}
}
UT_TEST(store_preflight_budget_alias_and_permissions_do_not_write)
{
	ControlDebtStoreAttemptV1 attempt = {0};
	uint8 ref[128];
	store_begin(65);
	UT_ASSERT(!control_debt_store_create_v1(store_global, image, debt_batches, 1,
		65536, &attempt, ref));
	UT_ASSERT(!control_debt_store_create_v1(store_global, image, debt_batches, 1,
		DEBT_TEST_BUDGET, &attempt, image->reference));
	UT_ASSERT(!attempt.attempted && writes == 0 && syncs == 0);
	UT_ASSERT(fchmod(store_global, 0777) == 0);
	UT_ASSERT(!store_create(&attempt, ref));
	UT_ASSERT(!attempt.attempted);
	UT_ASSERT(fchmod(store_global, 0700) == 0); store_end();
}
UT_TEST(store_each_write_sync_read_failure_latches_without_retry)
{
	unsigned totals[3];
	ControlDebtStoreAttemptV1 attempt = {0};
	uint8 ref[128];
	store_begin(65); UT_ASSERT(store_create(&attempt, ref));
	totals[0] = writes; totals[1] = syncs; totals[2] = reads; store_end();
	for (unsigned kind = 0; kind < 3; kind++)
		for (unsigned fail = 1; fail <= totals[kind]; fail++) {
			memset(&attempt, 0, sizeof(attempt)); memset(ref, 0x5a, 128); store_begin(65);
			if (!kind) fail_write = fail;
			else if (kind == 1) fail_sync = fail;
			else fail_read = fail;
			UT_ASSERT(!store_create(&attempt, ref));
			UT_ASSERT(attempt.attempted && ref[0] == 0x5a);
			reset_faults(); UT_ASSERT(!store_create(&attempt, ref));
			UT_ASSERT_EQ(writes, 0); store_end();
		}
}
UT_TEST(store_collision_never_overwrites_or_retries)
{
	ControlDebtStoreAttemptV1 first = {0}, next = {0};
	ControlDebtWireImageV1 *loaded = NULL;
	uint8 ref[128], again[128];
	store_begin(65); UT_ASSERT(store_create(&first, ref));
	reset_faults(); memset(again, 0x61, 128);
	UT_ASSERT(!store_create(&next, again));
	UT_ASSERT(next.attempted && again[0] == 0x61 && writes == 0);
	UT_ASSERT(control_debt_store_load_v1(store_global, ref, DEBT_TEST_BUDGET, &loaded));
	control_debt_wire_destroy_v1(&loaded); store_end();
}
UT_TEST(store_missing_corrupt_trailing_page_or_basis_refuses_whole_load)
{
	for (unsigned kind = 0; kind < 5; kind++) {
		ControlDebtStoreAttemptV1 attempt = {0};
		ControlDebtWireImageV1 *loaded = NULL;
		uint8 ref[128], hash[32], byte = 0;
		char name[96];
		int fd, dir;
		store_begin(65); UT_ASSERT(store_create(&attempt, ref));
		debt_sha(image->pages + 33024, 33024, hash); debt_store_name(hash, false, name);
		dir = openat(store_global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
		fd = openat(dir, name, O_RDWR); UT_ASSERT(fd >= 0);
		if (!kind) UT_ASSERT(ftruncate(fd, 17) == 0);
		if (kind == 1) {
			UT_ASSERT(pread(fd, &byte, 1, 257) == 1); byte ^= 1;
			UT_ASSERT(pwrite(fd, &byte, 1, 257) == 1);
		}
		if (kind == 2) UT_ASSERT(pwrite(fd, "x", 1, 33024) == 1);
		close(fd);
		if (kind == 3) UT_ASSERT(unlinkat(dir, name, 0) == 0);
		if (kind == 4) {
			snprintf(name, sizeof(name), "%016llx-%016llx-%016llx.v3",
				(unsigned long long)debt_get(debt_original_ref, 24, 8),
				(unsigned long long)debt_get(debt_original_ref, 32, 8),
				(unsigned long long)debt_get(debt_original_ref, 40, 8));
			UT_ASSERT(unlinkat(dir, name, 0) == 0);
		}
		close(dir);
		UT_ASSERT(!control_debt_store_load_v1(store_global, ref, DEBT_TEST_BUDGET, &loaded));
		UT_ASSERT(loaded == NULL); store_end();
	}
}
UT_TEST(store_selected_reference_only_and_alias_refuse)
{
	ControlDebtStoreAttemptV1 attempt = {0};
	ControlDebtWireImageV1 *loaded = NULL;
	uint8 ref[128];
	store_begin(65); UT_ASSERT(store_create(&attempt, ref));
	UT_ASSERT(!control_debt_store_load_v1(store_global, ref, 65536, &loaded));
	UT_ASSERT(!control_debt_store_load_v1(store_global, ref, DEBT_TEST_BUDGET,
		(ControlDebtWireImageV1 **)ref));
	ref[64] ^= 1; debt_put(ref, 124, debt_crc(ref, 124), 4);
	UT_ASSERT(!control_debt_store_load_v1(store_global, ref, DEBT_TEST_BUDGET, &loaded));
	UT_ASSERT(loaded == NULL); store_end();
}
UT_TEST(store_late_page_change_refuses_without_rewriting_input)
{
	ControlDebtStoreAttemptV1 attempt = {0};
	uint8 ref[128], before[32], after[32];
	store_begin(65);
	debt_sha(image->pages, image->page_bytes, before);
	/* Two pages + registry + directory + global: mutate after all creates,
	 * before the final full set readback. Neither input nor output may change. */
	mutate_sync = 5; memset(ref, 0x71, 128);
	UT_ASSERT(!store_create(&attempt, ref));
	debt_sha(image->pages, image->page_bytes, after);
	UT_ASSERT(attempt.attempted && ref[0] == 0x71 && !memcmp(before, after, 32));
	store_end();
}
UT_TEST(store_multiple_batches_keep_complete_basis_and_tt)
{
	ControlDebtStoreAttemptV1 attempt = {0};
	ControlDebtWireImageV1 *loaded = NULL;
	uint8 ref[128];
	store_begin(130); control_debt_wire_destroy_v1(&image);
	debt_batches[1] = debt_batches[0]; debt_batches[0].entry_count = 65;
	debt_batches[1].episode++; debt_batches[1].entries += 65; debt_batches[1].entry_count = 65;
	for (unsigned i = 65; i < 130; i++) debt_entries[i].entry.ordinal -= 65;
	debt_entries[0].pin.has_tt_binding = true;
	debt_entries[0].pin.binding = (ClusterCanonicalTxnBinding){.segment_id = 4,
		.segment_generation = 9, .xid = 3, .slot_offset = 2, .slot_wrap = 1,
		.origin_instance = 1, .publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED, .active_lsn = 130};
	UT_ASSERT(debt_encode(&image, 2));
	UT_ASSERT(control_debt_store_create_v1(store_global, image, debt_batches, 2,
		DEBT_TEST_BUDGET, &attempt, ref));
	memset(debt_batches, 0, sizeof(debt_batches));
	UT_ASSERT(control_debt_store_load_v1(store_global, ref, DEBT_TEST_BUDGET, &loaded));
	if (loaded) {
		UT_ASSERT_EQ(loaded->batch_count, 2);
		UT_ASSERT_EQ(loaded->page_bytes, 4 * 33024);
		UT_ASSERT_EQ(debt_get(loaded->pages + 256 + 352, 112, 8), 130);
	}
	control_debt_wire_destroy_v1(&loaded); store_end();
}
UT_TEST(store_symlink_hardlink_and_unsafe_modes_refuse)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		ControlDebtStoreAttemptV1 attempt = {0};
		ControlDebtWireImageV1 *loaded = NULL;
		uint8 ref[128], hash[32];
		char name[96];
		int dir;
		store_begin(65); UT_ASSERT(store_create(&attempt, ref));
		debt_sha(image->pages, 33024, hash); debt_store_name(hash, false, name);
		dir = openat(store_global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
		if (!kind) {
			UT_ASSERT(renameat(dir, name, dir, "retained-test-page") == 0);
			UT_ASSERT(symlinkat("retained-test-page", dir, name) == 0);
		} else if (kind == 1)
			UT_ASSERT(linkat(dir, name, dir, "retained-test-page", 0) == 0);
		else UT_ASSERT(fchmodat(dir, name, 0666, 0) == 0);
		close(dir);
		UT_ASSERT(!control_debt_store_load_v1(store_global, ref, DEBT_TEST_BUDGET, &loaded));
		UT_ASSERT(loaded == NULL); store_end();
	}
}
#ifndef DEBT_STORE_TEST_MAIN
#define DEBT_STORE_TEST_MAIN main
#endif
int DEBT_STORE_TEST_MAIN(void)
{
	UT_PLAN(9);
	UT_RUN(store_pages_reload_without_input_vectors);
	UT_RUN(store_preflight_budget_alias_and_permissions_do_not_write);
	UT_RUN(store_each_write_sync_read_failure_latches_without_retry);
	UT_RUN(store_collision_never_overwrites_or_retries);
	UT_RUN(store_missing_corrupt_trailing_page_or_basis_refuses_whole_load);
	UT_RUN(store_selected_reference_only_and_alias_refuse);
	UT_RUN(store_late_page_change_refuses_without_rewriting_input);
	UT_RUN(store_multiple_batches_keep_complete_basis_and_tt);
	UT_RUN(store_symlink_hardlink_and_unsafe_modes_refuse);
	UT_DONE(); return ut_failed_count != 0;
}
