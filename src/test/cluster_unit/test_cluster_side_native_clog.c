/*-------------------------------------------------------------------------
 * test_cluster_side_native_clog.c
 *     Original native CLOG/SLRU for the sealed SIDE record executor.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_side_native_clog.c
 * NOTES
 *     Original native bodies execute; authority, locks and faults are fixtures.
 *     No generation lifetime or joint completion certification.
 *-------------------------------------------------------------------------
 */

#include "test_cluster_side_native_clog_namespace.h"
int d3_side_native_clog_main(void);
#define TEST_CLUSTER_EPISODE_CLOG
#define TEST_CLUSTER_SIDE_NATIVE_CLOG
#include "test_cluster_clog_recovery.c"
#include "test_cluster_side_native_clog.h"

/* Unused observation APIs get no authority from this write-only fixture. */
bool
cluster_wal_inputs_relmap_source_current_v1(const ClusterRecoveryRelmapSourceV1 *source)
{
	(void)source;
	return false;
}
void
cluster_wal_inputs_relmap_source_fail_v1(const ClusterRecoveryRelmapSourceV1 *source)
{
	(void)source;
}
bool
cluster_wal_inputs_relmap_source_revalidate_v1(const ClusterRecoveryRelmapSourceV1 *source)
{
	(void)source;
	return false;
}

static unsigned side_native_fault;

static void
side_native_unlocked(void)
{
	for (unsigned i = 0; i < fixture_lock_count; ++i)
		UT_ASSERT(!fixture_locks[i].held);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT_EQ(transient_fds, 0);
}

void
test_side_native_clog_start(void)
{
	const char *dirs[] = {"native_side", "native_side/origin_2", "native_side/origin_2/pg_xact"};
	char path[MAXPGPATH];
	UT_ASSERT_EQ(allocation_count, 0);
	UT_ASSERT(getcwd(fixture_cwd, sizeof(fixture_cwd)) != NULL);
	strlcpy(fixture_root, "/tmp/pgrac-side-native-clog.XXXXXX", sizeof(fixture_root));
	UT_ASSERT(mkdtemp(fixture_root) != NULL);
	for (unsigned i = 0; i < lengthof(dirs); ++i) {
		snprintf(path, sizeof(path), "%s/%s", fixture_root, dirs[i]);
		UT_ASSERT_EQ(mkdir(path, 0700), 0);
	}
	cluster_shared_data_dir = fixture_root;
	cluster_shared_config = enableFsync = InRecovery = true;
	cluster_node_id = fixture_origin = 2;
	MyAuxProcType = StartupProcess;
	MyBackendType = B_STARTUP;
	relmap_base_interrupts = true;
	InterruptHoldoffCount = CritSectionCount = 0;
	writes = syncs = dirsyncs = reads = 0;
	side_native_fault = 0;
	fail_sync = fail_dirsync = expect_error = false;
	memset(ClogRecoveryCtl, 0, sizeof(ClogRecoveryCtl));
	memset(XactCtl, 0, sizeof(*XactCtl));
	clog_recovery_shmem_init();
	MainLWLockArray = own_main_locks;
	LWLockInitialize(XactSLRULock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XactTruncationLock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XidGenLock, LWTRANCHE_XACT_BUFFER);
	SimpleLruInit(XactCtl, "Xact SIDE fixture", 4, CLOG_LSNS_PER_PAGE,
		XactSLRULock, "pg_xact", LWTRANCHE_XACT_BUFFER, SYNC_HANDLER_NONE);
	XactCtl->PagePrecedes = CLOGPagePrecedes;
	UT_ASSERT_EQ(chdir(fixture_root), 0);
	snprintf(path, sizeof(path), "%s/native_side/origin_2/pg_xact", fixture_root);
	UT_ASSERT_EQ(symlink(path, "pg_xact"), 0);
}

void
test_side_native_clog_fault(unsigned fault)
{
	side_native_fault = fault;
	fail_sync = fault == 1;
	fail_dirsync = expect_error = fault == 2;
}

void
test_side_native_clog_state(SideNativeClogState *out)
{
	char path[MAXPGPATH];
	PGAlignedBlock bytes;
	int fd;
	side_native_unlocked();
	memset(out, 0, sizeof(*out));
	out->writes = writes; out->syncs = syncs; out->dirsyncs = dirsyncs;
	fixture_path(97, path);
	fd = open(path, O_RDONLY);
	if (fd >= 0) {
		if (pread(fd, bytes.data, BLCKSZ, (off_t)(97 % SLRU_PAGES_PER_SEGMENT) * BLCKSZ) == BLCKSZ) {
			out->disk_zero = true;
			for (unsigned i = 0; i < BLCKSZ; ++i)
				if (bytes.data[i] != 0) out->disk_zero = false;
		}
		UT_ASSERT_EQ(close(fd), 0);
	}
}

bool
test_side_native_clog_apply(int origin, const ClusterSideProjectionOperationV1 *operation,
	const uint8 *payload, uint32 length)
{
	return cluster_clog_native_recovery_full_record_apply_v1(origin, operation, payload, length);
}

bool
test_side_native_clog_verify(int origin, const ClusterSideProjectionOperationV1 *operation,
	const uint8 *payload, uint32 length)
{
	if (side_native_fault == 3)
		fixture_bit(3178496 + 7, TRANSACTION_STATUS_COMMITTED);
	return cluster_clog_native_recovery_full_record_verify_v1(origin, operation, payload, length);
}

void
test_side_native_clog_stop(void)
{
	const char *parents[] = {"/native_side/origin_2/pg_xact", "/native_side/origin_2", "/native_side", ""};
	char path[MAXPGPATH];
	side_native_unlocked();
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[2].writers), 0);
	fixture_path(97, path);
	if (access(path, F_OK) == 0) UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(unlink("pg_xact"), 0);
	UT_ASSERT_EQ(chdir(fixture_cwd), 0);
	for (unsigned i = 0; i < lengthof(parents); ++i) {
		snprintf(path, sizeof(path), "%s%s", fixture_root, parents[i]);
		UT_ASSERT_EQ(rmdir(path), 0);
	}
	for (unsigned i = 0; i < allocation_count; ++i) free(allocations[i]);
	allocation_count = fixture_lock_count = 0;
	UT_ASSERT_EQ(munmap(fixture_shared_mutation, fixture_shared_mutation_size), 0);
	fixture_shared_mutation = NULL;
	fixture_shared_mutation_size = 0;
	memset(ClogRecoveryCtl, 0, sizeof(ClogRecoveryCtl));
	memset(XactCtl, 0, sizeof(*XactCtl));
	ClogRecoveryLocks = NULL;
	MainLWLockArray = NULL;
	cluster_shared_data_dir = NULL;
}
