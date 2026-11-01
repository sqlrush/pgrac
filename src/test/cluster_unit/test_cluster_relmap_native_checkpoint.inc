/*-------------------------------------------------------------------------
 * test_cluster_relmap_native_checkpoint.inc
 *    Original writer, cache and complete pair checkpoint in one component.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_native_checkpoint.inc
 * NOTES
 *    Control selection/CF/WAL leaves remain controlled. Original transaction,
 *    table, authority and common pair sync/readback execute; no ROOT publication
 *    or native lifetime qualification follows from this local family receipt.
 *-------------------------------------------------------------------------
 */
static unsigned checkpoint_syncs, checkpoint_file_syncs, checkpoint_dir_syncs;
static unsigned checkpoint_fail_at, checkpoint_error_at, checkpoint_drift_at;
static bool checkpoint_source_error;
static ClusterRecoveryCheckpointSourceV2 checkpoint_sources[2];

/* Only control selection and backend allocation leaves are supplied here.
 * Pair checkpoint/sync/decode/digest and both transaction exits are unchanged. */
void *
palloc(Size n)
{
	void *p = malloc(n);
	if (!p) abort();
	return p;
}

uint64
cluster_control_root_relmap_selection_count_v4(ClusterControlRelmapSelectionV2 *s)
{
	UT_ASSERT(s && managed_cf && !local_held && managed_checkpoint_test);
	return 2;
}

bool
cluster_control_root_relmap_selection_at_v4(ClusterControlRelmapSelectionV2 *s,
	uint64 index, ClusterRecoveryFileIdentityV2 *out)
{
	UT_ASSERT(s && managed_cf && !local_held && managed_checkpoint_test);
	if (index >= 2) return false;
	*out = managed_files[index];
	return true;
}

uint64
cluster_control_root_relmap_selection_source_count_v4(ClusterControlRelmapSelectionV2 *s)
{
	UT_ASSERT(s && managed_cf && !local_held && managed_checkpoint_test);
	return 2;
}

bool
cluster_control_root_relmap_selection_source_at_v4(ClusterControlRelmapSelectionV2 *s,
	uint64 index, ClusterRecoveryCheckpointSourceV2 *out)
{
	UT_ASSERT(s && managed_cf && !local_held && managed_checkpoint_test);
	if (checkpoint_source_error)
		ereport(ERROR, (errmsg("controlled checkpoint source ERROR")));
	if (index >= 2) return false;
	*out = checkpoint_sources[index];
	return true;
}

ClusterControlRootResult
cluster_control_root_relmap_selection_complete_locked_v4(ClusterControlRelmapSelectionV2 *s,
	const ClusterRecoveryFileIdentityV2 *keys, uint64 count)
{
	UT_ASSERT(s && managed_cf && !local_held && managed_checkpoint_test);
	if (count != 2 || memcmp(keys, managed_files, sizeof(managed_files)))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return cluster_control_root_relmap_selection_recheck_locked_v4(s);
}

static int
managed_checkpoint_sync(int fd)
{
	struct stat st;
	UT_ASSERT(local_held && managed_cf && !v2_retention_held && !CritSectionCount);
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	checkpoint_syncs++;
	if (S_ISDIR(st.st_mode)) checkpoint_dir_syncs++;
	else checkpoint_file_syncs++;
	if (checkpoint_syncs == checkpoint_error_at)
		ereport(ERROR, (errmsg("controlled checkpoint sync ERROR")));
	if (checkpoint_syncs == checkpoint_fail_at) {
		errno = EIO;
		return -1;
	}
	if (checkpoint_syncs == checkpoint_drift_at) managed_fault = 3;
	return fsync(fd);
}

static void
checkpoint_begin(bool publish)
{
	(void)precommit_fixture(false, 3, publish);
	managed_writer_test = true;
	managed_setup_files();
	AtEOXact_RelationMap(true, false);
	managed_finishing = true;
	managed_checkpoint_test = true;
	checkpoint_syncs = checkpoint_file_syncs = checkpoint_dir_syncs = 0;
	checkpoint_fail_at = checkpoint_error_at = checkpoint_drift_at = 0;
	checkpoint_source_error = false;
	memset(checkpoint_sources, 0, sizeof(checkpoint_sources));
	for (unsigned i = 0; i < 2; i++) {
		checkpoint_sources[i].source = writer_ref;
		checkpoint_sources[i].checkpoint_start = 100 + i * 100;
		checkpoint_sources[i].checkpoint_end = 150 + i * 100;
		checkpoint_sources[i].replay_end = 190 + i * 100;
	}
}

static void
checkpoint_finish(bool commit)
{
	managed_checkpoint_test = false;
	v2_terminal_context = commit;
	if (commit) cluster_relmap_producer_postcommit_v2();
	else cluster_relmap_producer_abort_v2();
	UT_ASSERT_EQ(borrowed_count(), 0);
	UT_ASSERT(!authority_held && !local_held && !managed_cf);
}

static void
checkpoint_cache(unsigned slot, RelFileNumber expected)
{
	RelMapFile map = {0};
	managed_cache_test = true;
	UT_ASSERT(!cache_error(&map, false, slot ? MyDatabaseId : 0,
		slot ? MyDatabaseTableSpace : GLOBALTABLESPACE_OID));
	managed_cache_test = false;
	UT_ASSERT_EQ(map.mappings[0].mapfilenumber, expected);
}

static bool
checkpoint_call(ClusterRelmapCheckpointReceiptV2 **out)
{
#ifdef TEST_NATIVE_CHECKPOINT_BARRIER_ONLY
	/* Sensitivity control only: the old barrier produces no complete receipt. */
	(void)out;
	CheckPointRelationMap();
	return false;
#else
	return CheckPointRelationMapControlV2(out);
#endif
}

static void
checkpoint_pair(const ClusterRelmapCheckpointReceiptV2 *r, unsigned slot,
	uint64 committed_generation, uint64 pending, RelFileNumber filenumber)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapFilePairV2 pair;
	ClusterRelmapV2Image image;
	bool read = cluster_relmap_checkpoint_file_at_v2(r, slot, &id, &pair);
	UT_ASSERT(read);
	if (!read) return;
	UT_ASSERT(memcmp(&id, &managed_files[slot], sizeof(id)) == 0);
	for (unsigned copy = 0; copy < 2; copy++) {
		UT_ASSERT(cluster_relmap_file_decode_v2(pair.bytes[copy], sizeof(pair.bytes[copy]), &id, &image));
		UT_ASSERT_EQ(image.committed_generation, committed_generation);
		UT_ASSERT_EQ(image.pending_generation, pending);
		UT_ASSERT_EQ(image.committed.mappings[0].mapfilenumber, filenumber);
		if (pending) UT_ASSERT_EQ(image.pending.mappings[0].mapfilenumber, slot ? 503 : 501);
	}
}

UT_TEST(native_checkpoint_pending_pair_and_old_cache)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	ClusterRelmapCheckpointSummaryV2 summary;
	ClusterRecoveryCheckpointSourceV2 source;
	TestRelmapPhysicalStats before, after;
	checkpoint_begin(false);
	before = test_relmap_physical_stats();
	UT_ASSERT(checkpoint_call(&r));
	if (!r) {
		checkpoint_finish(false);
		managed_cleanup_files();
		return;
	}
	UT_ASSERT(cluster_relmap_checkpoint_summary_v2(r, &summary));
	UT_ASSERT_EQ(summary.control.logical_count, 2);
	UT_ASSERT_EQ(summary.control.source_count, 2);
	for (unsigned slot = 0; slot < 2; slot++) {
		checkpoint_pair(r, slot, 7, 8, 500);
		checkpoint_cache(slot, 500);
		UT_ASSERT(cluster_relmap_checkpoint_source_at_v2(r, slot, &source));
		UT_ASSERT(memcmp(&source, &checkpoint_sources[slot], sizeof(source)) == 0);
	}
	after = test_relmap_physical_stats();
	UT_ASSERT_EQ(checkpoint_file_syncs, 4);
	UT_ASSERT_EQ(checkpoint_dir_syncs, 2);
	UT_ASSERT_EQ(after.writes, before.writes);
	UT_ASSERT_EQ(after.renames, before.renames);
	UT_ASSERT_EQ(borrowed_count(), 2);
	UT_ASSERT_EQ(inserts, 2);
	UT_ASSERT_EQ(flushes, 2);
	UT_ASSERT(authority_held && !local_held && !managed_cf);
	cluster_relmap_checkpoint_destroy_v2(&r);
	UT_ASSERT_EQ(borrowed_count(), 2);
	checkpoint_finish(false);
	managed_cleanup_files();
}

UT_TEST(native_checkpoint_commit_abort_and_fresh_receipt)
{
	for (unsigned commit = 0; commit < 2; commit++) {
		ClusterRelmapCheckpointReceiptV2 *before = NULL, *after = NULL;
		ClusterRelmapCheckpointSummaryV2 a, b;
		checkpoint_begin(commit != 0);
		UT_ASSERT(checkpoint_call(&before));
		if (!before) {
			checkpoint_finish(commit != 0);
			managed_cleanup_files();
			continue;
		}
		UT_ASSERT(cluster_relmap_checkpoint_summary_v2(before, &a));
		checkpoint_finish(commit != 0);
		managed_checkpoint_test = true;
		UT_ASSERT(checkpoint_call(&after));
		if (!after) {
			cluster_relmap_checkpoint_destroy_v2(&before);
			managed_checkpoint_test = false;
			managed_cleanup_files();
			continue;
		}
		UT_ASSERT(cluster_relmap_checkpoint_summary_v2(after, &b));
		UT_ASSERT(memcmp(a.files_sha256, b.files_sha256, sizeof(a.files_sha256)) != 0);
		for (unsigned slot = 0; slot < 2; slot++) {
			checkpoint_pair(before, slot, 7, 8, 500); /* immutable local copy */
			checkpoint_pair(after, slot, commit ? 8 : 7, 0, commit ? (slot ? 503 : 501) : 500);
			checkpoint_cache(slot, commit ? (slot ? 503 : 501) : 500);
		}
		UT_ASSERT_EQ(borrowed_count(), 0);
		cluster_relmap_checkpoint_destroy_v2(&before);
		cluster_relmap_checkpoint_destroy_v2(&after);
		managed_checkpoint_test = false;
		managed_cleanup_files();
	}
}

UT_TEST(native_checkpoint_partial_sync_and_error_keep_duties)
{
	for (unsigned error = 0; error < 2; error++)
		for (unsigned point = 2; point <= 5; point += 3) {
			ClusterRelmapCheckpointReceiptV2 *r = NULL;
			volatile bool caught = false, ok = false;
			checkpoint_begin(false);
			if (error) checkpoint_error_at = point;
			else checkpoint_fail_at = point;
			error_expected = true;
			PG_TRY();
			{
				ok = checkpoint_call(&r);
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			error_expected = false;
			UT_ASSERT(!ok && !r && caught == (error != 0));
			UT_ASSERT_EQ(checkpoint_syncs, point);
			UT_ASSERT_EQ(InterruptHoldoffCount, 0);
			UT_ASSERT(authority_held && !local_held && !managed_cf);
			UT_ASSERT_EQ(borrowed_count(), 2);
			checkpoint_cache(0, 500);
			checkpoint_cache(1, 500);
			checkpoint_finish(false);
			managed_cleanup_files();
		}
}

UT_TEST(native_checkpoint_late_control_and_release_refuse_whole_receipt)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterRelmapCheckpointReceiptV2 *r = NULL;
		checkpoint_begin(false);
		if (fault) managed_fault = 7;
		else checkpoint_drift_at = 6;
		UT_ASSERT(!checkpoint_call(&r));
		UT_ASSERT(!r && !managed_cf && !local_held && authority_held);
		UT_ASSERT_EQ(checkpoint_syncs, 6);
		UT_ASSERT_EQ(borrowed_count(), 2);
		managed_fault = 0;
		checkpoint_finish(false);
		managed_cleanup_files();
	}
}

UT_TEST(native_checkpoint_source_recheck_is_sticky)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterRelmapCheckpointReceiptV2 *r = NULL;
		ClusterControlRelmapSelectionV2 *s = NULL;
		ClusterRelmapCheckpointSummaryV2 out, before;
		ClusterSharedConfigRef config = {0};
		volatile bool caught = false, ok = false;
		unsigned syncs;
		checkpoint_begin(false);
		UT_ASSERT(checkpoint_call(&r));
		managed_cf = true;
		UT_ASSERT_EQ(cluster_control_root_relmap_selection_read_locked_v4(&config, MaxAllocSize, &s),
			CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(cluster_relmap_checkpoint_recheck_v2(r, s));
		syncs = checkpoint_syncs;
		if (fault == 0) checkpoint_sources[1].checkpoint.nextXid.value++;
		else if (fault == 1) managed_files[1].registration_generation++;
		else checkpoint_source_error = true;
		error_expected = true;
		PG_TRY();
		{
			ok = cluster_relmap_checkpoint_recheck_v2(r, s);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		error_expected = false;
		UT_ASSERT(!ok && caught == (fault == 2));
		if (fault == 0) checkpoint_sources[1].checkpoint.nextXid.value--;
		else if (fault == 1) managed_files[1].registration_generation--;
		checkpoint_source_error = false;
		UT_ASSERT(!cluster_relmap_checkpoint_recheck_v2(r, s));
		memset(&out, 0x5a, sizeof(out));
		before = out;
		UT_ASSERT(!cluster_relmap_checkpoint_summary_v2(r, &out));
		UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
		UT_ASSERT_EQ(checkpoint_syncs, syncs);
		cluster_control_root_relmap_selection_destroy_v4(&s);
		managed_cf = false;
		cluster_relmap_checkpoint_destroy_v2(&r);
		UT_ASSERT_EQ(borrowed_count(), 2);
		checkpoint_finish(false);
		managed_cleanup_files();
	}
}

UT_TEST(native_checkpoint_inherited_cf_and_preflight)
{
	ClusterRelmapCheckpointReceiptV2 *r = NULL;
	unsigned syncs;
	checkpoint_begin(false);
	managed_cf = true;
	UT_ASSERT(checkpoint_call(&r));
	UT_ASSERT(managed_cf && !local_held);
	syncs = checkpoint_syncs;
	UT_ASSERT(!checkpoint_call(&r)); /* nonfresh output left intact */
	UT_ASSERT_EQ(checkpoint_syncs, syncs);
	cluster_relmap_checkpoint_destroy_v2(&r);
	local_held = true;
	UT_ASSERT(!checkpoint_call(&r));
	UT_ASSERT(local_held && managed_cf && !r);
	local_held = false;
	CritSectionCount = 1;
	UT_ASSERT(!checkpoint_call(&r));
	CritSectionCount = 0;
	UT_ASSERT_EQ(checkpoint_syncs, syncs);
	enableFsync = false;
	UT_ASSERT(!checkpoint_call(&r));
	enableFsync = true;
	UT_ASSERT(!r && managed_cf && !local_held);
	UT_ASSERT_EQ(checkpoint_syncs, syncs);
	managed_cf = false;
	checkpoint_finish(false);
	managed_cleanup_files();
}
