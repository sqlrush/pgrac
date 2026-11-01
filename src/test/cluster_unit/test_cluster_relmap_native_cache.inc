/*-------------------------------------------------------------------------
 * test_cluster_relmap_native_cache.inc
 *    Selected-address cache reads under the original MappingLock.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_native_cache.inc
 * NOTES
 *    Reuses real table/pair and original cache helper; no CF inside a native
 *    family lock, creation-origin guessing or file-lifetime qualification.
 *-------------------------------------------------------------------------
 */
#include <sys/wait.h>
/* Legacy PG file leaves must never be reached by the managed cache. */
int errcode_for_file_access(void) { return 0; }
static void
cache_read(RelMapFile *out, bool held, Oid db, Oid ts)
{
#ifdef TEST_NATIVE_CACHE_BASELINE
	cluster_read_relmap_from_control(out, "global", db, ts, ERROR);
#else
	read_relmap_file(out, "not-an-identity", held, ERROR, db, ts);
#endif
}
static bool
cache_error(RelMapFile *out, bool held, Oid db, Oid ts)
{
	volatile bool caught = false;
	error_expected = true;
	if (sigsetjmp(error_target, 1) == 0)
		cache_read(out, held, db, ts);
	else
		caught = true;
	error_expected = false;
	return caught;
}
static void
cache_setup(void)
{
	(void)precommit_fixture(false, 3, false);
	managed_writer_test = true;
	managed_setup_files();
	managed_cache_test = true;
	managed_cache_fault = 0;
	InterruptHoldoffCount = 0; /* test isolation after an intentionally failing cleanup */
	authority_held = false;
}
static void
cache_cleanup(void)
{
	managed_cache_test = false;
	managed_cache_fault = 0;
	managed_cleanup_files();
}
static void
managed_cache_after_read(void)
{
	UT_ASSERT(local_held);
	UT_ASSERT_EQ(borrowed_count(), cluster_relmap_v2_arms[0].armed ? 3 : 1);
	if (managed_cache_fault == 2 || managed_cache_fault == 4)
		ereport(ERROR, (errmsg("controlled cache read ERROR")));
	if (managed_cache_fault == 3)
		MyProcPid++;
}
UT_TEST(native_cache_registered_address_under_original_mapping_lock)
{
	for (unsigned held = 0; held < 2; held++) {
		RelMapFile map = { 0 };
		unsigned control;
		cache_setup();
		control = managed_selects;
		for (unsigned slot = 0; slot < 2; slot++) {
			local_held = held != 0;
			UT_ASSERT(!cache_error(&map, held != 0, slot ? MyDatabaseId : 0,
								   slot ? MyDatabaseTableSpace : GLOBALTABLESPACE_OID));
			UT_ASSERT_EQ(map.mappings[0].mapfilenumber, 500);
			UT_ASSERT_EQ(local_held, held != 0);
			UT_ASSERT_EQ(borrowed_count(), 0);
			UT_ASSERT_EQ(managed_selects, control);
			UT_ASSERT_EQ(managed_files[slot].origin, 0);
			UT_ASSERT(cluster_node_id != 0); /* creation origin differs from writer */
			local_held = false;
		}
		cache_cleanup();
	}
}
UT_TEST(native_cache_uses_committed_image_while_writer_pending)
{
	RelMapFile map = { 0 };
	borrow_prepare();
	managed_cache_test = true;
	UT_ASSERT(!cache_error(&map, false, 0, GLOBALTABLESPACE_OID));
	UT_ASSERT_EQ(map.mappings[0].mapfilenumber, 500);
	UT_ASSERT_EQ(borrowed_count(), 2);
	managed_cache_test = false;
	cluster_relmap_producer_abort_v2();
	managed_cleanup_files();
}
UT_TEST(native_cache_unknown_missing_and_ambiguous_never_fall_back)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		RelMapFile out, before;
		cache_setup();
		memset(&out, 0xa7, sizeof(out));
		before = out;
		if (fault == 0) {
			memset(native_files, 0, sizeof(*native_files));
			SpinLockInit(&native_files->lock);
		} else if (fault == 1) {
			native_files->banks[native_files->active].info.mode = CLUSTER_NATIVE_FILES_CREATION_V2;
		} else if (fault == 3) {
			borrow_table_files[2] = managed_files[0];
			borrow_table_files[2].origin = 1;
			borrow_table_count = 3;
			UT_ASSERT_EQ(borrow_publish(), CLUSTER_NATIVE_FILES_OK_V2);
		}
		local_held = true;
		UT_ASSERT(cache_error(&out, true, fault == 2 ? 98765 : 0, GLOBALTABLESPACE_OID));
		UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
		UT_ASSERT_EQ(managed_reads, 0);
		UT_ASSERT_EQ(borrowed_count(), 0);
		UT_ASSERT(local_held && !managed_cf);
		local_held = false;
		cache_cleanup();
	}
}
UT_TEST(native_cache_wrong_disk_identity_preserves_output)
{
	RelMapFile out, before;
	cache_setup();
	memset(&out, 0x51, sizeof(out));
	before = out;
	/* Independent selected registration deliberately disagrees with real file. */
	memset(native_files, 0, sizeof(*native_files));
	SpinLockInit(&native_files->lock);
	borrow_table_files[0].creation_generation++;
	UT_ASSERT_EQ(borrow_publish(), CLUSTER_NATIVE_FILES_OK_V2);
	UT_ASSERT(cache_error(&out, false, 0, GLOBALTABLESPACE_OID));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	UT_ASSERT_EQ(borrowed_count(), 0);
	UT_ASSERT(!local_held && !managed_cf);
	cache_cleanup();
}
UT_TEST(native_cache_read_failure_and_error_release_only_local_duty)
{
	for (unsigned fault = 1; fault <= 4; fault++)
		for (unsigned held = 0; held < 2; held++) {
			RelMapFile out, before;
			if (fault == 3) continue;
			cache_setup();
			managed_cache_fault = fault;
			memset(&out, 0x63, sizeof(out));
			before = out;
			local_held = held != 0;
			UT_ASSERT(cache_error(&out, held != 0, 0, GLOBALTABLESPACE_OID));
			UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
			UT_ASSERT_EQ(local_held, held != 0);
			UT_ASSERT_EQ(borrowed_count(), 0);
			UT_ASSERT_EQ(InterruptHoldoffCount, 0);
			local_held = false;
			cache_cleanup();
		}
}
UT_TEST(native_cache_current_failure_keeps_borrow_and_refuses_reentry)
{
	pid_t child;
	int status;
	fflush(NULL);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		RelMapFile out, before;
		unsigned reads;
		cache_setup();
		memset(&out, 0x34, sizeof(out));
		before = out;
		managed_cache_fault = 3;
		UT_ASSERT(cache_error(&out, false, 0, GLOBALTABLESPACE_OID));
		UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
		UT_ASSERT_EQ(borrowed_count(), 1);
		UT_ASSERT(!local_held);
		MyProcPid--;
		managed_cache_fault = 0;
		reads = managed_reads;
		UT_ASSERT(cache_error(&out, false, 0, GLOBALTABLESPACE_OID));
		UT_ASSERT_EQ(managed_reads, reads);
		UT_ASSERT_EQ(borrowed_count(), 1);
		cache_cleanup();
		_exit(ut_current_failed ? 1 : 0);
	}
	if (child > 0) {
		UT_ASSERT_EQ(waitpid(child, &status, 0), child);
		UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
	}
}
UT_TEST(native_cache_refuses_inconsistent_caller_locks)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		RelMapFile out;
		cache_setup();
		if (fault == 0)
			local_held = true;
		if (fault == 2)
			v2_xidgen_held = true;
		UT_ASSERT(cache_error(&out, fault == 1, 0, GLOBALTABLESPACE_OID));
		UT_ASSERT_EQ(managed_reads, 0);
		UT_ASSERT_EQ(borrowed_count(), 0);
		UT_ASSERT_EQ(local_held, fault == 0);
		local_held = v2_xidgen_held = false;
		cache_cleanup();
	}
}
UT_TEST(native_cache_borrow_capacity_refuses_before_file_io)
{
	RelMapFile out, before;
	cache_setup();
	memset(&out, 0x41, sizeof(out));
	before = out;
	native_files->borrowed = UINT64_MAX;
	UT_ASSERT(cache_error(&out, false, 0, GLOBALTABLESPACE_OID));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	UT_ASSERT_EQ(managed_reads, 0);
	UT_ASSERT_EQ(borrowed_count(), UINT64_MAX);
	native_files->borrowed = 0;
	cache_cleanup();
}

/* Real FATAL does not run PG_CATCH. This leaf exits at that exact report
 * boundary, checking only cleanup completed before proc_exit would run. */
static void
managed_cache_fatal_observe(void)
{
	_exit(borrowed_count() == 0 && local_held == managed_cache_fatal_inherited
		  && !managed_cf && InterruptHoldoffCount == 0 ? 0 : 1);
}
UT_TEST(native_cache_fatal_refusal_retires_local_borrow_before_exit)
{
	for (unsigned held = 0; held < 2; held++) {
		pid_t child;
		int status;
		fflush(NULL);
		child = fork();
		UT_ASSERT(child >= 0);
		if (child == 0) {
			cache_setup();
			managed_cache_fatal_probe = true;
			managed_cache_fatal_inherited = held != 0;
			local_held = held != 0;
			managed_cache_fault = 1;
			load_relmap_file(true, held != 0);
			_exit(2);
		}
		if (child > 0) {
			UT_ASSERT_EQ(waitpid(child, &status, 0), child);
			UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
		}
	}
}
