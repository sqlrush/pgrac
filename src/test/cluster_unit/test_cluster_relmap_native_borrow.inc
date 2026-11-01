/*-------------------------------------------------------------------------
 * test_cluster_relmap_native_borrow.inc
 *    Real shared identity table across the original transaction writer.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_native_borrow.inc
 * NOTES
 *    CF/registration/WAL leaves remain controlled. Table, producer, pair I/O
 *    and terminal callbacks are original code; no native lifetime grant.
 *-------------------------------------------------------------------------
 */
#include "storage/shmem.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_shmem.h"
#include "common/pgrac_initdb_recovery.h"
static ClusterRecoveryFileIdentityV2 borrow_table_files[3];
static uint64 borrow_table_count;
static ClusterControlRelmapReferenceV2 borrow_table_reference;
static bool borrow_publishing;
const PgracInitdbRecoveryContext *
cluster_wal_thread_initdb_recovery_context(void)
{
	return NULL;
}
bool
cluster_control_root_relmap_selection_reference_v4(ClusterControlRelmapSelectionV2 *s,
												   ClusterControlRelmapReferenceV2 *out)
{
	UT_ASSERT(s && managed_cf && !local_held);
	if (borrow_publishing)
		*out = borrow_table_reference;
	else {
		memset(out, 0, sizeof(*out));
		out->logical_count = 2;
		if (managed_checkpoint_test) out->source_count = 2;
	}
	return true;
}
bool
cluster_control_root_native_files_copy_v4(ClusterControlRelmapSelectionV2 *s,
										  ClusterRecoveryFileIdentityV2 *out, uint64 capacity,
										  uint64 *count)
{
	UT_ASSERT(s && managed_cf && !local_held && borrow_publishing);
	if (capacity < borrow_table_count)
		return false;
	memcpy(out, borrow_table_files, borrow_table_count * sizeof(*out));
	*count = borrow_table_count;
	return true;
}
static void *borrow_region_bytes;
static const ClusterShmemRegion *borrow_region;
static void
borrow_region_register(const ClusterShmemRegion *region)
{
	borrow_region = region;
}
static void *
borrow_region_init(const char *name, Size size, bool *found)
{
	(void)name;
	*found = false;
	borrow_region_bytes = calloc(1, size);
	if (!borrow_region_bytes)
		abort();
	return borrow_region_bytes;
}
#define cluster_shmem_register_region borrow_region_register
#define ShmemInitStruct borrow_region_init
#include "../../backend/cluster/cluster_control_native_files.c"
#undef ShmemInitStruct
#undef cluster_shmem_register_region
static ClusterControlNativeResultV2
borrow_publish(void)
{
	ClusterSharedConfigRef config = { 0 };
	ClusterControlNativeResultV2 result;
	bool saved_cf = managed_cf;
	managed_cf = borrow_publishing = true;
	result = cluster_control_native_files_publish_locked_v2(
		&config, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2, 77);
	managed_cf = saved_cf;
	borrow_publishing = false;
	return result;
}
static void
managed_native_table_setup(void)
{
	/* Test isolation only: production never resets an outstanding borrow. */
	free(borrow_region_bytes);
	borrow_region_bytes = NULL;
	native_files = NULL;
	cluster_control_native_files_shmem_register();
	borrow_region->init_fn();
	memcpy(borrow_table_files, managed_files, sizeof(managed_files));
	borrow_table_count = 2;
	memset(&borrow_table_reference, 0, sizeof(borrow_table_reference));
	borrow_table_reference.logical_count = 2;
	UT_ASSERT_EQ(borrow_publish(), CLUSTER_NATIVE_FILES_OK_V2);
}
static uint64
borrowed_count(void)
{
	ClusterControlNativeTableInfoV2 info;
	UT_ASSERT(cluster_control_native_files_info_v2(&info));
	return info.borrowed;
}
static void
borrow_add_file(void)
{
	borrow_table_files[2] = managed_files[0];
	borrow_table_files[2].kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	borrow_table_files[2].database_oid = borrow_table_files[2].tablespace_oid = 0;
	borrow_table_files[2].object_id = 1;
	borrow_table_count = 3;
}
static void
borrow_prepare(void)
{
	(void)precommit_fixture(false, 3, false);
	managed_writer_test = true;
	managed_setup_files();
	AtEOXact_RelationMap(true, false);
	managed_finishing = true;
}
static void (*borrow_original_send)(const SharedInvalidationMessage *, int);
static bool borrow_send_error;
static void
borrow_send(const SharedInvalidationMessage *msg, int n)
{
	UT_ASSERT_EQ(borrowed_count(), 2);
	UT_ASSERT(authority_held && !local_held);
	if (borrow_send_error)
		ereport(ERROR, (errmsg("controlled notification failure")));
	if (borrow_original_send)
		borrow_original_send(msg, n);
}
UT_TEST(managed_native_borrow_holds_both_through_commit_and_abort)
{
	for (unsigned commit = 0; commit < 2; commit++) {
		borrow_prepare();
		UT_ASSERT_EQ(borrowed_count(), 2);
		borrow_original_send = v2_send_observer;
		borrow_send_error = false;
		v2_send_observer = borrow_send;
		v2_terminal_context = commit != 0;
		if (commit)
			cluster_relmap_producer_postcommit_v2();
		else
			cluster_relmap_producer_abort_v2();
		UT_ASSERT_EQ(borrowed_count(), 0);
		UT_ASSERT(!authority_held && !cluster_relmap_v2_arms[0].armed);
		managed_cleanup_files();
	}
}
UT_TEST(managed_native_borrow_unknown_missing_or_wrong_identity_refuses_before_effect)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		RelMapFile queues[4];
		(void)precommit_fixture(false, 3, false);
		managed_writer_test = true;
		managed_setup_files();
		/* Fresh table with unknown, missing namespace or wrong birth. */
		memset(native_files, 0, sizeof(*native_files));
		SpinLockInit(&native_files->lock);
		if (fault) {
			if (fault == 1)
				borrow_table_count = 1;
			else
				borrow_table_files[1].creation_generation++;
			UT_ASSERT_EQ(borrow_publish(), CLUSTER_NATIVE_FILES_OK_V2);
		}
		precommit_save_queues(queues);
		UT_ASSERT(managed_dispatch_error());
		precommit_same_queues(queues);
		UT_ASSERT_EQ(inserts + flushes + managed_reads, 0);
		UT_ASSERT_EQ(test_relmap_physical_stats().writes, 0);
		UT_ASSERT(!local_held && !managed_cf && authority_held);
		managed_cleanup_files();
	}
}
UT_TEST(managed_native_borrow_complete_control_reference_must_match)
{
	(void)precommit_fixture(false, 3, false);
	managed_writer_test = true;
	managed_setup_files();
	borrow_table_reference.root_pair_sha256[3] = 1;
	UT_ASSERT_EQ(borrow_publish(), CLUSTER_NATIVE_FILES_OK_V2);
	UT_ASSERT(managed_dispatch_error());
	UT_ASSERT_EQ(inserts + managed_reads, 0);
	managed_cleanup_files();
}
UT_TEST(managed_native_borrow_blocks_table_change_until_terminal)
{
	borrow_prepare();
	borrow_add_file();
	UT_ASSERT_EQ(borrow_publish(), CLUSTER_NATIVE_FILES_BUSY_V2);
	UT_ASSERT_EQ(borrowed_count(), 2);
	cluster_relmap_producer_abort_v2();
	UT_ASSERT_EQ(borrow_publish(), CLUSTER_NATIVE_FILES_OK_V2);
	UT_ASSERT_EQ(borrowed_count(), 0);
	managed_cleanup_files();
}
UT_TEST(managed_native_borrow_late_error_keeps_batch_and_prevents_retry)
{
	RelMapFile queues[4];
	TestRelmapPhysicalStats before;
	(void)precommit_fixture(false, 3, false);
	managed_writer_test = true;
	managed_setup_files();
	managed_fault = 8;
	precommit_save_queues(queues);
	UT_ASSERT(managed_dispatch_error());
	precommit_same_queues(queues);
	UT_ASSERT_EQ(borrowed_count(), 2);
	before = test_relmap_physical_stats();
	{
		unsigned selected = managed_selects;
		unsigned wal = inserts;
		managed_writer_test = false; /* a later format read must not revive raw */
		UT_ASSERT(managed_dispatch_error());
		UT_ASSERT_EQ(managed_selects, selected);
		UT_ASSERT_EQ(inserts, wal);
		managed_writer_test = true;
	}
	UT_ASSERT(abort_panic());
	UT_ASSERT(postcommit_panic());
	UT_ASSERT_EQ(borrowed_count(), 2);
	UT_ASSERT_EQ(test_relmap_physical_stats().writes, before.writes);
	UT_ASSERT(authority_held);
	managed_cleanup_files();
}
UT_TEST(managed_native_borrow_owner_change_refuses_before_terminal_io)
{
	TestRelmapPhysicalStats before;
	borrow_prepare();
	before = test_relmap_physical_stats();
	MyProcPid++;
	UT_ASSERT(abort_panic());
	UT_ASSERT_EQ(test_relmap_physical_stats().writes, before.writes);
	UT_ASSERT_EQ(borrowed_count(), 2);
	UT_ASSERT(authority_held);
	MyProcPid--;
	managed_cleanup_files();
}
UT_TEST(managed_native_borrow_notification_failure_keeps_original_duty)
{
	borrow_prepare();
	borrow_original_send = v2_send_observer;
	borrow_send_error = true;
	v2_send_observer = borrow_send;
	v2_terminal_context = true;
	UT_ASSERT(postcommit_panic());
	UT_ASSERT_EQ(borrowed_count(), 2);
	UT_ASSERT(authority_held && cluster_relmap_v2_arms[0].armed);
	borrow_send_error = false;
	managed_cleanup_files();
}
UT_TEST(managed_native_borrow_direct_finish_retires_only_local_borrow)
{
	borrow_prepare();
	UT_ASSERT_EQ(borrowed_count(), 2);
	UT_ASSERT(cluster_relmap_producer_finish_v2(false));
	UT_ASSERT_EQ(borrowed_count(), 0);
	UT_ASSERT(authority_held); /* original direct API does not unlock X */
	managed_cleanup_files();
}
