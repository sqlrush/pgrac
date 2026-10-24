/* Original held-fd observation after planned physical mutation.
 * Included only by the ROOT unit's main, not its shared census fixture.
 * Author: SqlRush <sqlrush@gmail.com> */
static const ClusterRecoveryRootEntry *
reobserved_entry(const ClusterRecoveryRootSet *roots, const char *path)
{
	for (uint32 i = 0; i < cluster_recovery_root_set_count(roots); i++) {
		const ClusterRecoveryRootEntry *entry = cluster_recovery_root_set_entry(roots, i);
		if (entry != NULL && strcmp(entry->file.path, path) == 0)
			return entry;
	}
	return NULL;
}

UT_TEST(test_reobserve_uses_original_fd_and_new_complete_physical_bytes)
{
	int fd = new_root();
	char data[BLCKSZ + 17] = { 7 };
	ClusterRecoveryRootSet *old, *fresh = NULL;
	ClusterRecoveryRootSet saved;
	ClusterRecoveryRootEntry entry;
	const ClusterRecoveryRootEntry *updated;
	uint8 before[32], after[32];
	size_t bytes[2] = { 0 };

	root_file(fd, "unreferenced", "cold", 4);
	old = begin_roots(fd, 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(old, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT(cluster_recovery_root_set_digest(old, before));
	cluster_recovery_root_set_invalidate(old);
	cluster_recovery_root_set_invalidate(old);
	saved = *old;
	entry = old->entries[0];
	root_file(fd, "replacement", data, sizeof(data));
	UT_ASSERT_EQ(renameat(fd, "replacement", fd, "unreferenced"), 0);
	root_file(fd, "new-owner", "new", 3);
	UT_ASSERT_EQ(cluster_recovery_root_set_reobserve_begin(old, 1024 * 1024, &fresh),
				 CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT(fresh != NULL && fresh != old);
	if (fresh != NULL) {
		UT_ASSERT(!cluster_recovery_root_set_digest(fresh, after));
		UT_ASSERT(cluster_recovery_root_set_entry(fresh, 0) == NULL);
		UT_ASSERT_EQ(finish_roots(fresh, bytes), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(bytes[0], sizeof(data) + 3);
		UT_ASSERT_EQ(bytes[1], sizeof(data) + 3);
		UT_ASSERT_EQ(cluster_recovery_root_set_count(fresh), 2);
		UT_ASSERT(cluster_recovery_root_set_storage_matches(fresh, 817, 91, root_uuid));
		UT_ASSERT(cluster_recovery_root_set_digest(fresh, after));
		UT_ASSERT(memcmp(before, after, 32) != 0);
		UT_ASSERT(reobserved_entry(fresh, "new-owner") != NULL);
		updated = reobserved_entry(fresh, "unreferenced");
		UT_ASSERT(updated != NULL);
		if (updated != NULL) {
			UT_ASSERT_EQ(updated->file.size, sizeof(data));
			UT_ASSERT(updated->file.observed.st_ino != entry.file.observed.st_ino);
		}
	}
	UT_ASSERT_EQ(memcmp(&saved, old, sizeof(saved)), 0);
	UT_ASSERT_EQ(memcmp(&entry, &old->entries[0], sizeof(entry)), 0);
	UT_ASSERT(!cluster_recovery_root_set_digest(old, after));
	UT_ASSERT(cluster_recovery_root_set_entry(old, 0) == NULL);
	cluster_recovery_root_set_destroy(fresh);
	cluster_recovery_root_set_destroy(old);
	remove_root(fd);
}

UT_TEST(test_reobserve_keeps_missing_and_beyond_eof_typed_wal_requirements)
{
	int fd = new_root();
	ClusterRecoveryRootSet *old, *fresh = NULL;
	RfPageIdentityV1 page = target(16385, VISIBILITYMAP_FORKNUM, 17);
	ClusterSpaceIdentityKey key = { .system_identifier = 817,
									.database_incarnation = 91,
									.locator = { DEFAULTTABLESPACE_OID, 5, 16385 } };
	const char *paths[] = { "base/5/16385", "base/5/16385_vm", "pg_undo/instance_0/seg_1.dat",
							"native_side/origin_7/pg_xact/0001" };
	const uint64 required[] = { 128, 18, 64, 4 };

	memcpy(key.storage_uuid, root_uuid, 16);
	UT_ASSERT_EQ(mkdirat(fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "base/5", 0700), 0);
	root_file(fd, paths[0], "x", 1);
	old = begin_roots(fd, 1024 * 1024);
	UT_ASSERT_EQ(cluster_recovery_root_set_add_target(old, &page), CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(cluster_recovery_root_set_add_fork(old, &key, MAIN_FORKNUM, 128),
				 CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(cluster_recovery_root_set_add_undo(old, 817, 91, root_uuid, 1, 1, 64),
				 CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(roots_add_slru_page(old, 7, CLUSTER_RECOVERY_NATIVE_FAMILY_XACT,
									 SLRU_PAGES_PER_SEGMENT + 3),
				 CLUSTER_RECOVERY_ROOTS_MORE);
	UT_ASSERT_EQ(finish_roots(old, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	cluster_recovery_root_set_invalidate(old);
	root_file(fd, paths[0], "new", 3);
	UT_ASSERT_EQ(cluster_recovery_root_set_reobserve_begin(old, 1024 * 1024, &fresh),
				 CLUSTER_RECOVERY_ROOTS_MORE);
	if (fresh != NULL) {
		UT_ASSERT_EQ(finish_roots(fresh, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT_EQ(cluster_recovery_root_set_count(fresh), cluster_recovery_root_set_count(old));
		for (unsigned i = 0; i < lengthof(paths); i++) {
			const ClusterRecoveryRootEntry *entry = reobserved_entry(fresh, paths[i]);
			UT_ASSERT(entry != NULL);
			if (entry == NULL)
				continue;
			UT_ASSERT_EQ(entry->required_blocks, required[i]);
			UT_ASSERT_EQ(entry->origins, CLUSTER_RECOVERY_ROOT_FROM_WAL
											 | (i == 0 ? CLUSTER_RECOVERY_ROOT_PHYSICAL : 0));
			UT_ASSERT_EQ(entry->file.size, i == 0 ? 3 : 0);
			if (i == 1)
				UT_ASSERT(entry->file.forknum == VISIBILITYMAP_FORKNUM
						  && entry->file.locator.relNumber == 16385);
			if (i == 2)
				UT_ASSERT(entry->file.undo_segment == 1 && entry->file.undo_owner_instance == 1);
			if (i == 3)
				UT_ASSERT(entry->file.native_origin_node == 7 && entry->file.native_segment == 1
						  && entry->file.native_family == CLUSTER_RECOVERY_NATIVE_FAMILY_XACT);
		}
	}
	cluster_recovery_root_set_destroy(fresh);
	cluster_recovery_root_set_destroy(old);
	remove_root(fd);
}

UT_TEST(test_reobserve_refuses_old_path_removal_type_change_io_and_late_drift)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		int fd = new_root();
		ClusterRecoveryRootSet *old, *fresh = NULL, *retry = NULL;
		uint8 digest[32];
		root_file(fd, "must-stay", "old", 3);
		old = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(old, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		cluster_recovery_root_set_invalidate(old);
		if (fault < 2) {
			UT_ASSERT_EQ(unlinkat(fd, "must-stay", 0), 0);
			if (fault == 1)
				UT_ASSERT_EQ(mkdirat(fd, "must-stay", 0700), 0);
		}
		UT_ASSERT_EQ(cluster_recovery_root_set_reobserve_begin(old, 1024 * 1024, &fresh),
					 CLUSTER_RECOVERY_ROOTS_MORE);
		if (fresh != NULL) {
			if (fault == 2)
				inject_pread_fault = 1;
			if (fault == 3) {
				ClusterRecoveryRootsEvent event;
				for (unsigned tick = 0; tick < 10000 && fresh->pass == 0; tick++)
					UT_ASSERT(cluster_recovery_root_set_step(fresh, &event)
							  < CLUSTER_RECOVERY_ROOTS_OBSERVED);
				UT_ASSERT_EQ(fresh->pass, 1);
				root_file(fd, "must-stay", "x", 1);
			}
			UT_ASSERT_EQ(finish_roots(fresh, NULL),
						 fault == 2 ? CLUSTER_RECOVERY_ROOTS_IO : CLUSTER_RECOVERY_ROOTS_CHANGED);
			inject_pread_fault = 0;
			UT_ASSERT(!cluster_recovery_root_set_digest(fresh, digest));
			cluster_recovery_root_set_invalidate(fresh);
			UT_ASSERT_EQ(cluster_recovery_root_set_reobserve_begin(fresh, 1024 * 1024, &retry),
						 CLUSTER_RECOVERY_ROOTS_INVALID);
			UT_ASSERT(retry == NULL);
		}
		UT_ASSERT(!cluster_recovery_root_set_digest(old, digest));
		cluster_recovery_root_set_destroy(fresh);
		cluster_recovery_root_set_destroy(old);
		remove_root(fd);
	}
}

UT_TEST(test_reobserve_requires_explicit_invalidation_of_complete_observation)
{
	ClusterRecoveryRootSet *absent = (ClusterRecoveryRootSet *)1;
	for (unsigned state = 0; state < 5; state++) {
		int fd = new_root();
		ClusterRecoveryRootSet *old, *fresh = (ClusterRecoveryRootSet *)1;
		ClusterRecoveryRootSet saved;
		root_file(fd, "original", "cold", 4);
		old = begin_roots(fd, 1024 * 1024);
		if (state == 1 || state == 3) {
			UT_ASSERT_EQ(finish_roots(old, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
			if (state == 3) {
				root_file(fd, "original", "changed", 7);
				UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(old),
							 CLUSTER_RECOVERY_ROOTS_MORE);
				UT_ASSERT_EQ(finish_roots(old, NULL), CLUSTER_RECOVERY_ROOTS_CHANGED);
			}
		}
		if (state == 4) {
			inject_pread_fault = 1;
			UT_ASSERT_EQ(finish_roots(old, NULL), CLUSTER_RECOVERY_ROOTS_IO);
			inject_pread_fault = 0;
		}
		if (state >= 2)
			cluster_recovery_root_set_invalidate(old);
		saved = *old;
		UT_ASSERT_EQ(cluster_recovery_root_set_reobserve_begin(old, 1024 * 1024, &fresh),
					 CLUSTER_RECOVERY_ROOTS_INVALID);
		UT_ASSERT(fresh == NULL);
		UT_ASSERT_EQ(memcmp(&saved, old, sizeof(saved)), 0);
		cluster_recovery_root_set_destroy(old);
		remove_root(fd);
	}
	UT_ASSERT_EQ(cluster_recovery_root_set_reobserve_begin(NULL, 1024 * 1024, &absent),
				 CLUSTER_RECOVERY_ROOTS_INVALID);
	UT_ASSERT(absent == NULL);
}

UT_TEST(test_reobserve_output_aliases_leave_old_storage_unchanged)
{
	int fd = new_root();
	ClusterRecoveryRootSet *old, saved;
	ClusterRecoveryRootEntry entry;
	void *aliases[6];
	root_file(fd, "original", "cold", 4);
	old = begin_roots(fd, 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(old, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	cluster_recovery_root_set_invalidate(old);
	saved = *old;
	entry = old->entries[0];
	aliases[0] = old;
	aliases[1] = old->entries;
	aliases[2] = old->buckets;
	aliases[3] = old->order;
	aliases[4] = old->merge;
	aliases[5] = old->verified;
	for (unsigned i = 0; i < lengthof(aliases); i++) {
		UT_ASSERT_EQ(cluster_recovery_root_set_reobserve_begin(
						 old, 1024 * 1024, (ClusterRecoveryRootSet **)aliases[i]),
					 CLUSTER_RECOVERY_ROOTS_INVALID);
		UT_ASSERT_EQ(memcmp(&saved, old, sizeof(saved)), 0);
		UT_ASSERT_EQ(memcmp(&entry, &old->entries[0], sizeof(entry)), 0);
	}
	UT_ASSERT_EQ(cluster_recovery_root_set_reobserve_begin(old, 1024 * 1024, NULL),
				 CLUSTER_RECOVERY_ROOTS_INVALID);
	cluster_recovery_root_set_destroy(old);
	remove_root(fd);
}

UT_TEST(test_reobserve_capacity_closed_and_reused_fd_preserve_old_set)
{
	int fd = new_root(), other = new_root(), borrowed = dup(fd);
	ClusterRecoveryRootSet *old, *fresh = NULL;
	ClusterRecoveryRootSet saved;
	size_t budgets[]
		= { 1, sizeof(*old) + 256 * 1024
				   + 2 * (sizeof(ClusterRecoveryRootEntry) + 6 * sizeof(uint32) + sizeof(bool)) };
	for (unsigned i = 0; i < 32; i++) {
		char path[32];
		snprintf(path, sizeof(path), "original-%u", i);
		root_file(fd, path, "cold", 4);
	}
	old = begin_roots(borrowed, 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(old, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	cluster_recovery_root_set_invalidate(old);
	saved = *old;
	for (unsigned i = 0; i < lengthof(budgets); i++) {
		UT_ASSERT_EQ(cluster_recovery_root_set_reobserve_begin(old, budgets[i], &fresh),
					 CLUSTER_RECOVERY_ROOTS_CAPACITY);
		UT_ASSERT(fresh == NULL);
	}
	UT_ASSERT_EQ(close(borrowed), 0);
	UT_ASSERT_EQ(cluster_recovery_root_set_reobserve_begin(old, 1024 * 1024, &fresh),
				 CLUSTER_RECOVERY_ROOTS_IO);
	UT_ASSERT(fresh == NULL);
	UT_ASSERT_EQ(dup2(other, borrowed), borrowed);
	UT_ASSERT_EQ(cluster_recovery_root_set_reobserve_begin(old, 1024 * 1024, &fresh),
				 CLUSTER_RECOVERY_ROOTS_CHANGED);
	UT_ASSERT(fresh == NULL);
	UT_ASSERT_EQ(memcmp(&saved, old, sizeof(saved)), 0);
	cluster_recovery_root_set_destroy(old);
	UT_ASSERT_EQ(fcntl(borrowed, F_GETFD), 0);
	close(borrowed);
	remove_root(other);
	remove_root(fd);
}
