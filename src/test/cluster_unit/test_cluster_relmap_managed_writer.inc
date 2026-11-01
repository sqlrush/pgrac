/*-------------------------------------------------------------------------
 * test_cluster_relmap_managed_writer.inc
 *    Original transaction entry chooses registered managed files.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_managed_writer.inc
 * NOTES
 *    Control/authority leaves are controlled; original producer/retention/WAL
 *    sequencing and transaction callbacks execute. No native qualification.
 *-------------------------------------------------------------------------
 */
volatile uint32 InterruptHoldoffCount;
int
pg_fsync(int fd)
{
	return fsync(fd);
}
void *
palloc0(Size n)
{
	void *p = calloc(1, n);
	if (!p)
		abort();
	return p;
}
void
pfree(void *p)
{
	free(p);
}
void
pg_re_throw(void)
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	siglongjmp(error_target, 1);
}
static void managed_setup_files(void);
static void managed_cleanup_files(void);
static bool managed_cf;
static unsigned managed_selects;
static unsigned managed_fault;
bool
cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	return managed_cf && mode == ShareLock;
}
bool
cluster_cf_lock(LOCKMODE mode)
{
	UT_ASSERT(mode == ShareLock && !local_held && authority_held);
	managed_cf = true;
	return true;
}
ClusterCfReleaseResult
cluster_cf_unlock_confirmed(LOCKMODE mode)
{
	UT_ASSERT(mode == ShareLock && managed_cf && !local_held);
	managed_cf = false;
	return managed_fault == 7 ? CLUSTER_CF_RELEASE_UNCONFIRMED : CLUSTER_CF_RELEASE_CONFIRMED;
}
ClusterControlRootResult
cluster_control_root_relmap_format_locked_v2(Size budget, ClusterControlRelmapFormatV2 *out)
{
	UT_ASSERT(managed_cf && !local_held && budget > 0);
	memset(out, 0, sizeof(*out));
	out->root_version = managed_writer_test ? 4 : 3;
	managed_selects++;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
UT_TEST(managed_writer_original_transaction_dispatches_registered_batch)
{
	(void)precommit_fixture(false, 3, false);
	managed_setup_files();
	managed_writer_test = true;
	managed_selects = 0;
	managed_cf = false;
	AtEOXact_RelationMap(true, false);
	UT_ASSERT(managed_selects > 0);
	UT_ASSERT(cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_arms[1].armed);
	UT_ASSERT_EQ(inserts, 2);
	UT_ASSERT(!managed_cf && !local_held);
	managed_cleanup_files();
	managed_writer_test = false;
}

static ClusterRecoveryFileIdentityV2 managed_files[2];
static ClusterRelmapV2Image managed_disk[2];
static unsigned managed_clears, managed_reads;
static bool managed_finishing;
static char managed_root[MAXPGPATH];
static void managed_setup_files(void);
ClusterControlRootResult
cluster_control_root_relmap_selection_read_locked_v4(const ClusterSharedConfigRef *config,
													 Size budget,
													 ClusterControlRelmapSelectionV2 **out)
{
	(void)config;
	(void)budget;
	UT_ASSERT(managed_cf && !local_held && *out == NULL);
	if (managed_fault == 1)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	*out = (ClusterControlRelmapSelectionV2 *)managed_files;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
bool
cluster_control_root_relmap_selection_lookup_v4(ClusterControlRelmapSelectionV2 *s, Oid dbid,
												Oid tsid, uint64 object,
												ClusterRecoveryFileIdentityV2 *out)
{
	unsigned slot = dbid ? 1 : 0;
	UT_ASSERT(s && managed_cf && !local_held && object == 0);
	if (managed_fault == 2 && slot == 1)
		return false;
	UT_ASSERT_EQ(tsid, slot ? MyDatabaseTableSpace : GLOBALTABLESPACE_OID);
	*out = managed_files[slot];
	if (managed_finishing && managed_fault == 5)
		out->registration_generation++;
	return true;
}
ClusterControlRootResult
cluster_control_root_relmap_selection_recheck_locked_v4(ClusterControlRelmapSelectionV2 *s)
{
	UT_ASSERT(s && managed_cf && !local_held);
	return managed_fault == 3 && inserts ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
										 : CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
void
cluster_control_root_relmap_selection_destroy_v4(ClusterControlRelmapSelectionV2 **s)
{
	UT_ASSERT(!local_held);
	*s = NULL;
}
bool
cluster_relmap_authority_managed_read_v2(const ClusterRecoveryFileIdentityV2 *id,
										 ClusterRelmapFilePairV2 *pair)
{
	unsigned slot = id->database_oid ? 1 : 0;
	UT_ASSERT(managed_cf && local_held && authority_held && !v2_retention_held);
	managed_reads++;
	if (memcmp(id, &managed_files[slot], sizeof(*id)))
		return false;
	if (!test_relmap_physical_managed_read_v2(id, pair))
		return false;
	managed_disk[slot].committed_generation = pair->view.committed_generation;
	managed_disk[slot].committed = pair->view.committed;
	managed_disk[slot].pending_generation = pair->view.pending_generation;
	managed_disk[slot].pending_owner = pair->view.pending_owner;
	managed_disk[slot].pending = pair->view.pending;
	terminal_images[slot] = managed_disk[slot];
	terminal_preparing_slot = slot;
	return true;
}
bool
cluster_relmap_authority_managed_write_pending_v2(
	const ClusterRecoveryFileIdentityV2 *id, uint64 generation, const ClusterRelmapV2Owner *owner,
	const RelMapFile *pending, ClusterRelmapRetentionDirectory *directory, LWLock *lock)
{
	unsigned slot = id->database_oid ? 1 : 0;
	bool ok;
	UT_ASSERT(managed_cf && local_held && !v2_retention_held && flushes == inserts);
	ok = test_relmap_physical_managed_write_pending_v2(id, generation, owner, pending, directory,
													   lock);
	if (!ok)
		return false;
	managed_disk[slot].pending_generation = generation;
	managed_disk[slot].pending_owner = *owner;
	managed_disk[slot].pending = *pending;
	if (managed_fault == 8 && slot == 1)
		ereport(ERROR, (errmsg("controlled managed native write error")));
	if (managed_fault == 4 && slot == 1)
		return false;
	return true;
}
static ClusterRelmapV2ClearResult
managed_clear(const ClusterRecoveryFileIdentityV2 *id, const ClusterRelmapV2Image *staged,
			  bool commit)
{
	unsigned slot = id->database_oid ? 1 : 0;
	UT_ASSERT(managed_cf && local_held && authority_held && managed_finishing);
	managed_clears++;
	if (managed_fault == 6 && slot == 1)
		return CLUSTER_RELMAP_V2_RETAINED_UNKNOWN;
	{
		ClusterRelmapV2ClearResult result
			= commit ? test_relmap_physical_managed_publish_v2(id, staged)
					 : test_relmap_physical_managed_discard_pending_v2(id, staged);
		terminal_trace[terminal_calls++] = slot;
		if (result == CLUSTER_RELMAP_V2_CLEARED_POSTREAD)
			terminal_disk_cleared[slot] = true;
		return result;
	}
}
ClusterRelmapV2ClearResult
cluster_relmap_authority_managed_publish_v2(const ClusterRecoveryFileIdentityV2 *id,
											const ClusterRelmapV2Image *staged)
{
	return managed_clear(id, staged, true);
}
ClusterRelmapV2ClearResult
cluster_relmap_authority_managed_discard_pending_v2(const ClusterRecoveryFileIdentityV2 *id,
													const ClusterRelmapV2Image *staged)
{
	return managed_clear(id, staged, false);
}
static void
managed_setup_files(void)
{
	memset(managed_files, 0, sizeof(managed_files));
	memset(managed_disk, 0, sizeof(managed_disk));
	for (unsigned slot = 0; slot < 2; slot++) {
		ClusterRecoveryFileIdentityV2 *id = &managed_files[slot];
		id->kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
		id->format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
		id->format_version = 2;
		id->system_identifier = GetSystemIdentifier();
		id->database_incarnation = writer_ref.claim.database_incarnation;
		memcpy(id->storage_uuid, writer_ref.claim.identity.storage_uuid, 16);
		id->redo_branch_uuid[0] = 99;
		id->redo_branch_generation = 1;
		id->redo_timeline = 1;
		id->database_oid = slot ? MyDatabaseId : InvalidOid;
		id->tablespace_oid = slot ? MyDatabaseTableSpace : GLOBALTABLESPACE_OID;
		id->origin = 0;
		id->creation_generation = 9;
		id->registration_generation = 4;
		managed_disk[slot].identity
			= (ClusterRelmapV2Namespace){ id->system_identifier, id->database_incarnation,
										  id->database_oid, !slot };
		managed_disk[slot].committed_generation = 7;
		managed_disk[slot].committed = committed;
	}
	{
		char path[MAXPGPATH];
		strlcpy(managed_root, "/tmp/pgrac_managed_writer_XXXXXX", sizeof(managed_root));
		if (!mkdtemp(managed_root))
			abort();
		snprintf(path, sizeof(path), "%s/global", managed_root);
		if (mkdir(path, 0700))
			abort();
		snprintf(path, sizeof(path), "%s/base", managed_root);
		if (mkdir(path, 0700))
			abort();
		snprintf(path, sizeof(path), "%s/base/%u", managed_root, MyDatabaseId);
		if (mkdir(path, 0700))
			abort();
		for (unsigned slot = 0; slot < 2; slot++) {
			uint8 bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
			int dir;
			if (slot)
				snprintf(path, sizeof(path), "%s/base/%u", managed_root, MyDatabaseId);
			else
				snprintf(path, sizeof(path), "%s/global", managed_root);
			dir = open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
			if (dir < 0
				|| !cluster_relmap_file_encode_v2(&managed_files[slot], &managed_disk[slot], bytes,
												  sizeof(bytes)))
				abort();
			for (unsigned copy = 0; copy < 2; copy++) {
				int fd = openat(dir, copy ? "pgrac_relmap_authority.bak" : "pgrac_relmap_authority",
								O_WRONLY | O_CREAT | O_EXCL, 0600);
				if (fd < 0 || write(fd, bytes, sizeof(bytes)) != sizeof(bytes) || fsync(fd)
					|| close(fd))
					abort();
			}
			if (fsync(dir) || close(dir))
				abort();
		}
		test_relmap_physical_reset(managed_root, NULL);
	}
	managed_fault = managed_reads = managed_clears = 0;
	managed_finishing = false;
}

static void
managed_cleanup_files(void)
{
	char path[MAXPGPATH];
	test_relmap_physical_reset(NULL, NULL);
	for (unsigned slot = 0; slot < 2; slot++) {
		int dir;
		if (slot)
			snprintf(path, sizeof(path), "%s/base/%u", managed_root, MyDatabaseId);
		else
			snprintf(path, sizeof(path), "%s/global", managed_root);
		dir = open(path, O_RDONLY | O_DIRECTORY);
		if (dir < 0)
			abort();
		unlinkat(dir, "pgrac_relmap_authority", 0);
		unlinkat(dir, "pgrac_relmap_authority.bak", 0);
		unlinkat(dir, "pgrac_relmap_authority.tmp", 0);
		unlinkat(dir, "pgrac_relmap_authority.bak.tmp", 0);
		if (close(dir) || rmdir(path))
			abort();
	}
	snprintf(path, sizeof(path), "%s/base", managed_root);
	if (rmdir(path) || rmdir(managed_root))
		abort();
	managed_writer_test = managed_cf = false;
	relmap_managed_writer = NULL;
}
static bool
managed_dispatch_error(void)
{
	volatile bool caught = false;
	error_expected = true;
	if (sigsetjmp(error_target, 1) == 0)
		AtEOXact_RelationMap(true, false);
	else
		caught = true;
	error_expected = false;
	return caught;
}
UT_TEST(managed_writer_commit_and_abort_finish_before_original_unlock)
{
	for (unsigned commit = 0; commit < 2; commit++) {
		ClusterRelmapRetentionDirectory *directory = precommit_fixture(false, 3, commit != 0);
		ClusterRelmapRetentionDirectory before;
		ClusterRelmapFilePairV2 pair;
		TestRelmapPhysicalStats stats;
		managed_writer_test = true;
		managed_cf = false;
		managed_setup_files();
		AtEOXact_RelationMap(true, false);
		before = *directory;
		managed_finishing = true;
		v2_terminal_context = commit != 0;
		if (commit)
			cluster_relmap_producer_postcommit_v2();
		else
			cluster_relmap_producer_abort_v2();
		UT_ASSERT(!authority_held && !managed_cf && !local_held);
		UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		for (unsigned slot = 0; slot < 2; slot++) {
			char path[MAXPGPATH];
			int dir;
			bool read_ok;
			if (slot)
				snprintf(path, sizeof(path), "%s/base/%u", managed_root, MyDatabaseId);
			else
				snprintf(path, sizeof(path), "%s/global", managed_root);
			dir = open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
			read_ok
				= dir >= 0 && cluster_relmap_file_pair_read_v2(dir, &managed_files[slot], &pair);
			UT_ASSERT(read_ok);
			if (dir >= 0)
				UT_ASSERT_EQ(close(dir), 0);
			if (!read_ok)
				continue;
			UT_ASSERT(pair.view.pending_complete && !pair.view.has_pending);
			UT_ASSERT_EQ(pair.view.committed_generation, commit ? 8 : 7);
			UT_ASSERT_EQ(pair.view.committed.mappings[0].mapfilenumber,
						 commit ? (slot ? 503 : 501) : 500);
		}
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
		stats = test_relmap_physical_stats();
		UT_ASSERT_EQ(stats.writes, 8);
		UT_ASSERT_EQ(stats.file_syncs, 8);
		UT_ASSERT_EQ(stats.directory_syncs, 8);
		UT_ASSERT_EQ(managed_clears, 2);
		managed_cleanup_files();
	}
}
UT_TEST(managed_writer_missing_registration_refuses_whole_batch_before_wal)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		RelMapFile queues[4];
		(void)precommit_fixture(false, 3, false);
		managed_writer_test = true;
		managed_setup_files();
		managed_fault = fault;
		precommit_save_queues(queues);
		UT_ASSERT(managed_dispatch_error());
		precommit_same_queues(queues);
		UT_ASSERT_EQ(inserts + flushes + managed_reads, 0);
		UT_ASSERT_EQ(test_relmap_physical_stats().writes, 0);
		UT_ASSERT(!managed_cf && !local_held && authority_held);
		managed_cleanup_files();
	}
}
UT_TEST(managed_writer_late_control_or_partial_write_keeps_whole_input_and_debt)
{
	for (unsigned fault = 3; fault <= 4; fault++) {
		RelMapFile queues[4];
		ClusterRelmapRetentionDirectory *directory = precommit_fixture(false, 3, false);
		ClusterRelmapRetentionDirectory before;
		TestRelmapPhysicalStats stats;
		managed_writer_test = true;
		managed_setup_files();
		managed_fault = fault;
		precommit_save_queues(queues);
		UT_ASSERT(managed_dispatch_error());
		precommit_same_queues(queues);
		UT_ASSERT(cluster_relmap_v2_prepare_unfinished);
		UT_ASSERT(!managed_cf && !local_held && authority_held);
		before = *directory;
		stats = test_relmap_physical_stats();
		UT_ASSERT(abort_panic());
		UT_ASSERT(postcommit_panic());
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
		UT_ASSERT_EQ(test_relmap_physical_stats().writes, stats.writes);
		UT_ASSERT_EQ(unlock_calls + managed_clears, 0);
		managed_cleanup_files();
	}
}
UT_TEST(managed_writer_terminal_rejects_replacement_and_never_retries_partial_clear)
{
	for (unsigned fault = 5; fault <= 6; fault++) {
		ClusterRelmapRetentionDirectory *directory = precommit_fixture(false, 3, false);
		ClusterRelmapRetentionDirectory before;
		TestRelmapPhysicalStats stats;
		managed_writer_test = true;
		managed_setup_files();
		AtEOXact_RelationMap(true, false);
		before = *directory;
		managed_fault = fault;
		managed_finishing = true;
		UT_ASSERT(abort_panic());
		UT_ASSERT(!managed_cf && !local_held && authority_held);
		UT_ASSERT_EQ(managed_clears, fault == 5 ? 0 : 2);
		stats = test_relmap_physical_stats();
		UT_ASSERT(abort_panic());
		UT_ASSERT(postcommit_panic());
		UT_ASSERT_EQ(test_relmap_physical_stats().writes, stats.writes);
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
		UT_ASSERT_EQ(unlock_calls, 0);
		managed_cleanup_files();
	}
}

UT_TEST(managed_writer_error_and_unconfirmed_release_keep_batch_without_lock_inversion)
{
	for (unsigned fault = 7; fault <= 8; fault++) {
		ClusterRelmapRetentionDirectory *directory = precommit_fixture(false, 3, false);
		ClusterRelmapRetentionDirectory before;
		RelMapFile queues[4];
		managed_writer_test = true;
		managed_setup_files();
		managed_fault = fault;
		precommit_save_queues(queues);
		UT_ASSERT(managed_dispatch_error());
		precommit_same_queues(queues);
		UT_ASSERT(!managed_cf && !local_held && authority_held);
		UT_ASSERT(cluster_relmap_v2_prepare_unfinished);
		before = *directory;
		UT_ASSERT(abort_panic());
		UT_ASSERT(postcommit_panic());
		UT_ASSERT(memcmp(before.pins, directory->pins, sizeof(before.pins)) == 0);
		UT_ASSERT_EQ(unlock_calls + managed_clears, 0);
		managed_cleanup_files();
	}
}
UT_TEST(managed_writer_borrows_original_control_without_reentry_or_release)
{
	(void)precommit_fixture(false, 3, false);
	managed_writer_test = true;
	managed_setup_files();
	managed_cf = true;
	AtEOXact_RelationMap(true, false);
	UT_ASSERT(managed_cf && !local_held && authority_held);
	managed_finishing = true;
	cluster_relmap_producer_abort_v2();
	UT_ASSERT(managed_cf && !local_held && !authority_held);
	UT_ASSERT_EQ(managed_clears, 2);
	managed_cleanup_files();
}

UT_TEST(managed_writer_exact_legacy_dispatch_keeps_original_native_format)
{
	terminal_test_reset();
	reset(false);
	v2_preserve_observer = NULL;
	precommit_reset_queues();
	managed_writer_test = managed_cf = false;
	managed_fault = managed_selects = 0;
	active_shared_updates = updates;
	AtEOXact_RelationMap(true, false);
	UT_ASSERT(managed_selects > 0);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(flushes, 1);
	UT_ASSERT_EQ(registered, 2);
	UT_ASSERT(cluster_relmap_publish_arms[0].armed);
	UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
	UT_ASSERT(!managed_cf && !local_held);
	UT_ASSERT_EQ(active_shared_updates.num_mappings, 0);
}
