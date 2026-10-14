/* Real filesystem root union, two-pass observation and full SHA-256.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#define main namespace_fixture_main
#include "test_cluster_recovery_namespace.c"
#undef main
#include "../../backend/cluster/cluster_recovery_root_set.c"

static const uint8 root_uuid[16] = { 9, 3, 7 };

static ClusterRecoveryRootSet *
begin_roots(int fd, size_t budget)
{
	ClusterRecoveryRootSet *roots = NULL;

	UT_ASSERT_EQ(cluster_recovery_root_set_begin(fd, 817, 91, root_uuid, budget, &roots),
				 CLUSTER_RECOVERY_ROOTS_MORE);
	return roots;
}

static void
root_file(int fd, const char *name, const void *data, size_t length)
{
	int file = openat(fd, name, O_CREAT | O_TRUNC | O_WRONLY, 0600);

	if (file < 0 || write(file, data, length) != (ssize_t)length || close(file) != 0)
		abort();
}

static ClusterRecoveryRootsResult
finish_roots(ClusterRecoveryRootSet *roots, size_t bytes[2])
{
	ClusterRecoveryRootsEvent event;
	ClusterRecoveryRootsResult result = CLUSTER_RECOVERY_ROOTS_MORE;

	for (unsigned i = 0; i < 10000; i++) {
		result = cluster_recovery_root_set_step(roots, &event);
		if (result >= CLUSTER_RECOVERY_ROOTS_OBSERVED)
			return result;
		if (result == CLUSTER_RECOVERY_ROOTS_BYTES) {
			UT_ASSERT(event.pass < 2 && event.length > 0 && event.length <= BLCKSZ);
			if (bytes != NULL && event.pass < 2)
				bytes[event.pass] += event.length;
		}
	}
	UT_ASSERT(false); /* a bounded fixture must terminate */
	return result;
}

static RfPageIdentityV1
target(RelFileNumber number, ForkNumber forknum, BlockNumber block)
{
	RfPageIdentityV1 result = { 0 };

	result.system_identifier = 817;
	memcpy(result.storage_uuid, root_uuid, 16);
	result.locator = (RelFileLocator){ DEFAULTTABLESPACE_OID, 5, number };
	result.forknum = forknum;
	result.blockno = block;
	return result;
}

UT_TEST(test_unreferenced_physical_files_and_missing_wal_targets_are_preserved)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots;
	RfPageIdentityV1 page = target(99999, MAIN_FORKNUM, 17);
	unsigned missing = 0, unknown = 0, untouched = 0;
	size_t bytes[2] = { 0, 0 };
	uint8 digest[32];

	UT_ASSERT(fd >= 0);
	if (fd < 0)
		return;
	UT_ASSERT_EQ(mkdirat(fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "base/5", 0700), 0);
	write_file(fd, "base/5/16385");
	write_file(fd, "unknown-owner");
	roots = begin_roots(fd, 1024 * 1024);
	if (roots == NULL) {
		remove_root(fd);
		return;
	}
	UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
	UT_ASSERT(cluster_recovery_root_set_entry(roots, 0) == NULL);
	UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &page), CLUSTER_RECOVERY_ROOTS_MORE);
	page.blockno = 3;
	UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &page), CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(finish_roots(roots, bytes), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(bytes[0], 8);
	UT_ASSERT_EQ(bytes[1], 8);
	UT_ASSERT_EQ(cluster_recovery_root_set_count(roots), 5);
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);

		UT_ASSERT(entry != NULL);
		if (entry == NULL)
			continue;
		if (strcmp(entry->file.path, "base/5/99999") == 0) {
			UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
			UT_ASSERT_EQ(entry->required_blocks, 18);
			missing++;
		}
		if (strcmp(entry->file.path, "unknown-owner") == 0) {
			UT_ASSERT_EQ(entry->file.kind, CLUSTER_RECOVERY_FILE_UNCLASSIFIED);
			unknown++;
		}
		if (strcmp(entry->file.path, "base/5/16385") == 0) {
			UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_PHYSICAL);
			untouched++;
		}
	}
	UT_ASSERT_EQ(missing, 1);
	UT_ASSERT_EQ(unknown, 1);
	UT_ASSERT_EQ(untouched, 1);
	UT_ASSERT(cluster_recovery_root_set_digest(roots, digest));
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_all_file_bytes_are_read_twice_in_bounded_chunks)
{
	int fd = new_root();
	uint8 data[2 * BLCKSZ + 13];
	ClusterRecoveryRootSet *roots;
	size_t bytes[2] = { 0, 0 };

	memset(data, 0x37, sizeof(data));
	root_file(fd, "large", data, sizeof(data));
	roots = begin_roots(fd, 1024 * 1024);
	if (roots == NULL) {
		remove_root(fd);
		return;
	}
	UT_ASSERT_EQ(finish_roots(roots, bytes), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(bytes[0], sizeof(data));
	UT_ASSERT_EQ(bytes[1], sizeof(data));
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_digest_is_path_ordered_and_bound_to_the_namespace)
{
	int fd = new_root();
	uint8 digest[3][32];
	ClusterRecoveryRootSet *roots;
	RfPageIdentityV1 a = target(20000, MAIN_FORKNUM, 1);
	RfPageIdentityV1 b = target(20001, SPACE_FORKNUM, 0);

	write_file(fd, "z");
	write_file(fd, "a");
	for (unsigned pass = 0; pass < 3; pass++) {
		ClusterRecoveryRootsResult result = cluster_recovery_root_set_begin(
			fd, 817, pass == 2 ? 92 : 91, root_uuid, 1024 * 1024, &roots);

		UT_ASSERT_EQ(result, CLUSTER_RECOVERY_ROOTS_MORE);
		if (roots == NULL) {
			remove_root(fd);
			return;
		}
		UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, pass == 0 ? &a : &b),
					 CLUSTER_RECOVERY_ROOTS_MORE);
		UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, pass == 0 ? &b : &a),
					 CLUSTER_RECOVERY_ROOTS_MORE);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(cluster_recovery_root_set_digest(roots, digest[pass]));
		cluster_recovery_root_set_destroy(roots);
	}
	UT_ASSERT_EQ(memcmp(digest[0], digest[1], 32), 0);
	UT_ASSERT(memcmp(digest[0], digest[2], 32) != 0);
	remove_root(fd);
}

UT_TEST(test_file_digest_uses_all_sha256_bits)
{
	static const uint8 expected[32]
		= { 0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40,
			0xde, 0x5d, 0xae, 0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17,
			0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad };
	int fd = new_root();
	ClusterRecoveryRootSet *roots;
	const ClusterRecoveryRootEntry *entry;

	root_file(fd, "abc", "abc", 3);
	roots = begin_roots(fd, 1024 * 1024);
	if (roots == NULL) {
		remove_root(fd);
		return;
	}
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	entry = cluster_recovery_root_set_entry(roots, 0);
	UT_ASSERT(entry != NULL);
	if (entry != NULL)
		UT_ASSERT_EQ(memcmp(entry->content_sha256, expected, 32), 0);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_equal_count_rename_or_rewrite_between_passes_is_not_stable)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots;
		ClusterRecoveryRootsEvent event;
		uint8 digest[32];

		write_file(fd, "before");
		roots = begin_roots(fd, 1024 * 1024);
		if (roots == NULL) {
			remove_root(fd);
			return;
		}
		for (unsigned i = 0; roots->pass == 0 && i < 100; i++)
			UT_ASSERT(cluster_recovery_root_set_step(roots, &event)
					  < CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(roots->pass, 1);
		if (mode == 0)
			UT_ASSERT_EQ(renameat(fd, "before", fd, "after"), 0);
		else
			root_file(fd, "before", "warm", 4);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_CHANGED);
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_short_read_and_io_failure_cannot_finish_a_root_set)
{
	for (unsigned mode = 1; mode <= 2; mode++) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots;
		uint8 digest[32];

		write_file(fd, "original");
		roots = begin_roots(fd, 1024 * 1024);
		if (roots == NULL) {
			remove_root(fd);
			return;
		}
		inject_pread_fault = mode;
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_IO);
		inject_pread_fault = 0;
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_capacity_and_late_targets_never_shrink_the_union)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots = NULL;
	ClusterRecoveryRootsEvent event;
	RfPageIdentityV1 page = target(1, MAIN_FORKNUM, 0);
	ClusterRecoveryRootsResult result = CLUSTER_RECOVERY_ROOTS_MORE;

	UT_ASSERT_EQ(cluster_recovery_root_set_begin(fd, 817, 91, root_uuid, 1, &roots),
				 CLUSTER_RECOVERY_ROOTS_CAPACITY);
	UT_ASSERT(roots == NULL);
	roots = begin_roots(fd, 320 * 1024);
	if (roots == NULL) {
		remove_root(fd);
		return;
	}
	for (unsigned i = 0; i < 1000 && result == CLUSTER_RECOVERY_ROOTS_MORE; i++) {
		page.locator.relNumber = i + 1;
		result = cluster_recovery_root_set_add_target(roots, &page);
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_ROOTS_CAPACITY);
	UT_ASSERT_EQ(cluster_recovery_root_set_step(roots, &event), CLUSTER_RECOVERY_ROOTS_CAPACITY);
	cluster_recovery_root_set_destroy(roots);
	roots = begin_roots(fd, 1024 * 1024);
	if (roots == NULL) {
		remove_root(fd);
		return;
	}
	UT_ASSERT_EQ(cluster_recovery_root_set_step(roots, &event), CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &page),
				 CLUSTER_RECOVERY_ROOTS_INVALID);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_malformed_and_foreign_wal_targets_refuse)
{
	int fd = new_root();

	for (unsigned mode = 0; mode < 5; mode++) {
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		RfPageIdentityV1 page = target(16385, MAIN_FORKNUM, 0);

		if (roots == NULL) {
			remove_root(fd);
			return;
		}
		switch (mode) {
		case 0:
			page.system_identifier++;
			break;
		case 1:
			page.storage_uuid[0]++;
			break;
		case 2:
			page.locator.spcOid = GLOBALTABLESPACE_OID;
			break;
		case 3:
			page.forknum = INIT_FORKNUM;
			break;
		case 4:
			page.blockno = InvalidBlockNumber;
			break;
		}
		UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &page),
					 CLUSTER_RECOVERY_ROOTS_INVALID);
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

UT_TEST(test_a_late_root_revokes_the_completed_candidate_digest)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
	RfPageIdentityV1 page = target(16385, MAIN_FORKNUM, 0);
	uint8 digest[32];

	if (roots == NULL) {
		remove_root(fd);
		return;
	}
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT(cluster_recovery_root_set_digest(roots, digest));
	UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &page),
				 CLUSTER_RECOVERY_ROOTS_INVALID);
	UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_wal_and_physical_forks_merge_without_losing_missing_blocks)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots;
	RfPageIdentityV1 page = target(1260, MAIN_FORKNUM, 9);
	const ClusterRecoveryRootEntry *entry;

	UT_ASSERT_EQ(mkdirat(fd, "global", 0700), 0);
	root_file(fd, "global/1260", "", 0); /* committed DROP placeholder is still an object */
	page.locator.spcOid = GLOBALTABLESPACE_OID;
	page.locator.dbOid = InvalidOid;
	roots = begin_roots(fd, 1024 * 1024);
	if (roots == NULL) {
		remove_root(fd);
		return;
	}
	UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &page), CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(cluster_recovery_root_set_count(roots), 2);
	entry = cluster_recovery_root_set_entry(roots, 1);
	UT_ASSERT(entry != NULL);
	if (entry != NULL) {
		UT_ASSERT_EQ(entry->origins,
					 CLUSTER_RECOVERY_ROOT_PHYSICAL | CLUSTER_RECOVERY_ROOT_FROM_WAL);
		UT_ASSERT_EQ(entry->file.size, 0);
		UT_ASSERT_EQ(entry->required_blocks, 10);
		UT_ASSERT_EQ(strcmp(entry->file.path, "global/1260"), 0);
	}
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_large_root_union_sorts_across_ticks_without_dropping_targets)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots = begin_roots(fd, 2 * 1024 * 1024);
	char previous[MAXPGPATH] = "";

	if (roots == NULL) {
		remove_root(fd);
		return;
	}
	for (unsigned i = 0; i < 300; i++) {
		RfPageIdentityV1 page = target(40000 - i * 13, MAIN_FORKNUM, i);

		UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &page),
					 CLUSTER_RECOVERY_ROOTS_MORE);
	}
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT_EQ(cluster_recovery_root_set_count(roots), 300);
	for (uint32 i = 0; i < 300; i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);

		UT_ASSERT(entry != NULL);
		if (entry != NULL) {
			UT_ASSERT(strcmp(previous, entry->file.path) < 0);
			UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL);
			strlcpy(previous, entry->file.path, sizeof(previous));
		}
	}
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_directory_buffers_share_the_root_memory_budget)
{
	int fd = new_root();
	int parent = dup(fd);
	ClusterRecoveryRootSet *roots;
	uint8 digest[32];

	/* Few entries, but many simultaneously live libc directory buffers. */
	for (unsigned depth = 0; depth < 36; depth++) {
		int child;

		UT_ASSERT_EQ(mkdirat(parent, "d", 0700), 0);
		child = openat(parent, "d", O_RDONLY | O_DIRECTORY);
		UT_ASSERT(child >= 0);
		close(parent);
		parent = child;
	}
	close(parent);
	roots = begin_roots(fd, 320 * 1024);
	if (roots != NULL) {
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_CAPACITY);
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

UT_TEST(test_directory_budget_is_returned_after_each_subtree)
{
	int fd = new_root();
	ClusterRecoveryRootSet *roots;
	char path[32];

	for (unsigned i = 0; i < 50; i++) {
		snprintf(path, sizeof(path), "d%u", i);
		UT_ASSERT_EQ(mkdirat(fd, path, 0700), 0);
		snprintf(path, sizeof(path), "d%u/child", i);
		UT_ASSERT_EQ(mkdirat(fd, path, 0700), 0);
	}
	roots = begin_roots(fd, 1024 * 1024);
	if (roots != NULL) {
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_root_set_count(roots), 100);
		cluster_recovery_root_set_destroy(roots);
	}
	remove_root(fd);
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_unreferenced_physical_files_and_missing_wal_targets_are_preserved);
	UT_RUN(test_all_file_bytes_are_read_twice_in_bounded_chunks);
	UT_RUN(test_digest_is_path_ordered_and_bound_to_the_namespace);
	UT_RUN(test_file_digest_uses_all_sha256_bits);
	UT_RUN(test_equal_count_rename_or_rewrite_between_passes_is_not_stable);
	UT_RUN(test_short_read_and_io_failure_cannot_finish_a_root_set);
	UT_RUN(test_capacity_and_late_targets_never_shrink_the_union);
	UT_RUN(test_malformed_and_foreign_wal_targets_refuse);
	UT_RUN(test_a_late_root_revokes_the_completed_candidate_digest);
	UT_RUN(test_wal_and_physical_forks_merge_without_losing_missing_blocks);
	UT_RUN(test_large_root_union_sorts_across_ticks_without_dropping_targets);
	UT_RUN(test_directory_buffers_share_the_root_memory_budget);
	UT_RUN(test_directory_budget_is_returned_after_each_subtree);
	UT_DONE();
	return ut_failed_count != 0;
}
