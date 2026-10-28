/*-------------------------------------------------------------------------
 *
 * test_cluster_xid_native_clog.c
 *    Original native FULL CLOG and SLRU for retained allocation.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_xid_native_clog.c
 * NOTES
 *    Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *
 *-------------------------------------------------------------------------
 */
#include "test_cluster_xid_native_clog_namespace.h"
int d3_xid_native_clog_main(void);
#define TEST_CLUSTER_EPISODE_CLOG
#include "test_cluster_clog_recovery.c"
#include "test_cluster_xid_native_clog.h"

static unsigned xid_native_calls;

/* This producer fixture cannot qualify an unrelated recovery observation. */
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

static void
xid_native_idle(void)
{
	for (unsigned i = 0; i < fixture_lock_count; i++)
		UT_ASSERT(!fixture_locks[i].held);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT_EQ(transient_fds, 0);
}

void
test_xid_native_clog_start(int origin)
{
	char path[MAXPGPATH], target[MAXPGPATH];

	UT_ASSERT_EQ(allocation_count, 0);
	UT_ASSERT(origin >= 0 && origin < CLUSTER_MAX_NODES);
	UT_ASSERT(getcwd(fixture_cwd, sizeof(fixture_cwd)) != NULL);
	strlcpy(fixture_root, "/tmp/pgrac-xid-native-clog.XXXXXX", sizeof(fixture_root));
	UT_ASSERT(mkdtemp(fixture_root) != NULL);
	snprintf(path, sizeof(path), "%s/native_side", fixture_root);
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	snprintf(path, sizeof(path), "%s/native_side/origin_%d", fixture_root, origin);
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	snprintf(target, sizeof(target), "%s/native_side/origin_%d/pg_xact", fixture_root, origin);
	UT_ASSERT_EQ(mkdir(target, 0700), 0);
	UT_ASSERT_EQ(chdir(fixture_root), 0);
	UT_ASSERT_EQ(symlink(target, "pg_xact"), 0);
	cluster_shared_data_dir = fixture_root;
	cluster_shared_config = enableFsync = true;
	InRecovery = false;
	cluster_node_id = fixture_origin = origin;
	MyAuxProcType = NotAnAuxProcess;
	MyBackendType = B_BACKEND;
	relmap_base_interrupts = true;
	InterruptHoldoffCount = CritSectionCount = 0;
	writes = syncs = dirsyncs = reads = xid_native_calls = 0;
	memset(&birth_wal, 0, sizeof(birth_wal));
	birth_wal.mode = LW_EXCLUSIVE;
	memset(ClogRecoveryCtl, 0, sizeof(ClogRecoveryCtl));
	memset(XactCtl, 0, sizeof(*XactCtl));
	clog_recovery_shmem_init();
	MainLWLockArray = own_main_locks;
	LWLockInitialize(XactSLRULock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XactTruncationLock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XidGenLock, LWTRANCHE_XACT_BUFFER);
	SimpleLruInit(XactCtl, "Xact allocator fixture", 4, CLOG_LSNS_PER_PAGE,
		XactSLRULock, "pg_xact", LWTRANCHE_XACT_BUFFER, SYNC_HANDLER_NONE);
	XactCtl->PagePrecedes = CLOGPagePrecedes;
}

void
test_xid_native_clog_extend(FullTransactionId full)
{
	xid_native_idle();
	xid_native_calls++;
	/* The caller separately verifies its real allocator fixture lock. This
	 * namespaced copy mirrors that same native lock for the original CLOG body. */
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	ExtendCLOGFull(full);
	UT_ASSERT(LWLockHeldByMe(XidGenLock));
	UT_ASSERT(!LWLockHeldByMe(XactSLRULock));
	LWLockRelease(XidGenLock);
	xid_native_idle();
}

void
test_xid_native_clog_state(int page, TestXidNativeClogState *out)
{
	xid_native_idle();
	memset(out, 0, sizeof(*out));
	out->birth = birth_wal.bytes;
	out->length = birth_wal.length;
	out->info = birth_wal.info;
	out->calls = xid_native_calls;
	out->inserts = birth_wal.inserts;
	out->writes = writes;
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	for (int slot = 0; slot < XactCtl->shared->num_slots; slot++) {
		if (XactCtl->shared->page_status[slot] == SLRU_PAGE_VALID
			&& XactCtl->shared->page_number[slot] == page) {
			out->dirty = XactCtl->shared->page_dirty[slot];
			out->page_zero = true;
			for (unsigned i = 0; i < BLCKSZ; i++)
				if (XactCtl->shared->page_buffer[slot][i] != 0) out->page_zero = false;
		}
	}
	LWLockRelease(XactSLRULock);
	xid_native_idle();
}

bool
test_xid_native_clog_writeback(int page)
{
	char path[MAXPGPATH];
	PGAlignedBlock image;
	int fd;
	ssize_t nread;
	bool zero = true;

	xid_native_idle();
	CheckPointCLOG();
	xid_native_idle();
	fixture_path(page, path);
	fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	if (fd < 0) return false;
	nread = pread(fd, image.data, BLCKSZ,
		(off_t)(page % SLRU_PAGES_PER_SEGMENT) * BLCKSZ);
	UT_ASSERT_EQ(nread, BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
	if (nread != BLCKSZ) return false;
	for (unsigned i = 0; i < BLCKSZ; i++)
		if (image.data[i] != 0) zero = false;
	return zero && writes > 0;
}

void
test_xid_native_clog_stop(void)
{
	char path[MAXPGPATH], directory[MAXPGPATH];
	DIR *dir;
	struct dirent *entry;

	xid_native_idle();
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[fixture_origin].writers), 0);
	snprintf(directory, sizeof(directory), "%s/native_side/origin_%d/pg_xact", fixture_root, fixture_origin);
	dir = opendir(directory);
	UT_ASSERT(dir != NULL);
	while ((entry = readdir(dir)) != NULL) {
		if (entry->d_name[0] == '.') continue;
		snprintf(path, sizeof(path), "%s/%s", directory, entry->d_name);
		UT_ASSERT_EQ(unlink(path), 0);
	}
	UT_ASSERT_EQ(closedir(dir), 0);
	UT_ASSERT_EQ(unlink("pg_xact"), 0);
	UT_ASSERT_EQ(chdir(fixture_cwd), 0);
	UT_ASSERT_EQ(rmdir(directory), 0);
	snprintf(path, sizeof(path), "%s/native_side/origin_%d", fixture_root, fixture_origin);
	UT_ASSERT_EQ(rmdir(path), 0);
	snprintf(path, sizeof(path), "%s/native_side", fixture_root);
	UT_ASSERT_EQ(rmdir(path), 0);
	UT_ASSERT_EQ(rmdir(fixture_root), 0);
	for (unsigned i = 0; i < allocation_count; i++) free(allocations[i]);
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
