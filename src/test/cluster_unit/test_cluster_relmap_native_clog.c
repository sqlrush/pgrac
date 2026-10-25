/* Run the existing actual native CLOG/SLRU fixture beside the ROOT fixture.
 * Namespacing keeps their controlled PG shmem/lock/error leaves separate;
 * original-origin identities and real file bytes are never translated.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_relmap_native_clog_namespace.h"
int d3_clog_main(void);
#include "test_cluster_clog_recovery.c"
#include "test_cluster_relmap_native_clog.h"

static unsigned relmap_native_calls, relmap_native_fault, relmap_native_on_call;
static char relmap_native_path[MAXPGPATH];
static bool relmap_native_dirs[3];

TestRelmapNativeClogStats
test_relmap_native_clog_stats(void)
{
	TestRelmapNativeClogStats result
		= { relmap_native_calls, syncs, dirsyncs, transient_fds, ut_current_failed };
	return result;
}

static void
relmap_native_apply_fault(void)
{
	if (relmap_native_fault == 1)
		UT_ASSERT_EQ(unlink(relmap_native_path), 0);
	if (relmap_native_fault == 2) {
		int fd = open(relmap_native_path, O_RDWR);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(ftruncate(fd, BLCKSZ - 1), 0);
		UT_ASSERT_EQ(close(fd), 0);
	}
	if (relmap_native_fault == 3)
		fail_sync = true;
	if (relmap_native_fault == 4)
		directory_sync_error = EIO;
	if (relmap_native_fault == 5)
		fail_dirsync = expect_error = true;
	if (relmap_native_fault == 6) {
		int fd = open(relmap_native_path, O_RDWR);
		uint8 byte;
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(pread(fd, &byte, 1, BLCKSZ - 1), 1);
		byte ^= 1;
		UT_ASSERT_EQ(pwrite(fd, &byte, 1, BLCKSZ - 1), 1);
		UT_ASSERT_EQ(close(fd), 0);
	}
	if (relmap_native_fault == 7) {
		char replacement[MAXPGPATH];
		PGAlignedBlock image;
		struct stat before, after;
		int old, fresh;
		UT_ASSERT(snprintf(replacement, sizeof(replacement), "%s.replace", relmap_native_path)
				  < sizeof(replacement));
		old = open(relmap_native_path, O_RDONLY);
		UT_ASSERT(old >= 0 && fstat(old, &before) == 0);
		UT_ASSERT_EQ(before.st_size, BLCKSZ);
		UT_ASSERT_EQ(pread(old, image.data, BLCKSZ, 0), BLCKSZ);
		fresh = open(replacement, O_RDWR | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(fresh >= 0);
		UT_ASSERT_EQ(write(fresh, image.data, BLCKSZ), BLCKSZ);
		UT_ASSERT_EQ(fstat(fresh, &after), 0);
		UT_ASSERT(before.st_dev != after.st_dev || before.st_ino != after.st_ino);
		UT_ASSERT_EQ(rename(replacement, relmap_native_path), 0);
		UT_ASSERT_EQ(pread(fresh, image.data, BLCKSZ, 0), BLCKSZ);
		{
			PGAlignedBlock original;
			UT_ASSERT_EQ(pread(old, original.data, BLCKSZ, 0), BLCKSZ);
			UT_ASSERT_EQ(memcmp(original.data, image.data, BLCKSZ), 0);
		}
		UT_ASSERT_EQ(close(fresh), 0);
		UT_ASSERT_EQ(close(old), 0);
	}
}

void
test_relmap_native_clog_authorize(bool (*check)(int))
{
	fixture_cold_origin = check;
}

void
test_relmap_native_clog_fault(unsigned fault, unsigned on_call)
{
	relmap_native_fault = fault;
	relmap_native_on_call = on_call;
	if (on_call == 0)
		relmap_native_apply_fault();
}

void
test_relmap_native_clog_setup(const char *root, int origin, TransactionId xid, XidStatus status,
							  uint8 last_byte)
{
	char paths[3][MAXPGPATH];
	PGAlignedBlock image;
	int fd;
	strlcpy(fixture_root, root, sizeof(fixture_root));
	cluster_shared_data_dir = fixture_root;
	cluster_node_id = origin == 0 ? 1 : 0;
	fixture_origin = origin;
	base_fixture = authority = true;
	relmap_native_calls = relmap_native_fault = relmap_native_on_call = 0;
	syncs = dirsyncs = writes = ut_current_failed = 0;
	LWLockInitialize(XidGenLock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XactTruncationLock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XactSLRULock, LWTRANCHE_XACT_BUFFER);
	clog_recovery_shmem_init();
	snprintf(paths[0], MAXPGPATH, "%s/native_side", root);
	snprintf(paths[1], MAXPGPATH, "%s/native_side/origin_%d", root, origin);
	snprintf(paths[2], MAXPGPATH, "%s/native_side/origin_%d/pg_xact", root, origin);
	for (unsigned i = 0; i < lengthof(paths); i++) {
		relmap_native_dirs[i] = mkdir(paths[i], 0700) == 0;
		UT_ASSERT(relmap_native_dirs[i] || errno == EEXIST);
	}
	snprintf(relmap_native_path, sizeof(relmap_native_path), "%s/0000", paths[2]);
	memset(image.data, 0x42, BLCKSZ);
	image.data[TransactionIdToByte(xid)]
		&= ~(CLOG_XACT_BITMASK << (TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT));
	image.data[TransactionIdToByte(xid)] |= status
											<< (TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT);
	image.data[BLCKSZ - 1] = last_byte;
	fd = open(relmap_native_path, O_WRONLY | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, image.data, BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
}

bool
test_relmap_native_clog_read(int origin, TransactionId xid, ClusterClogNativeBaseV1 *out,
							 sigjmp_buf *outer_stack, ClusterClogNativeBaseHandleV1 **held)
{
	relmap_native_calls++;
	PG_exception_stack = outer_stack;
	if (relmap_native_calls == relmap_native_on_call)
		relmap_native_apply_fault();
	/* This is the namespaced original production body from clog.c. */
	if (held != NULL)
		return cluster_clog_native_recovery_base_read_held_v1(origin, xid, out, held);
	return cluster_clog_native_recovery_base_read_v1(origin, xid, out);
}

void
test_relmap_native_clog_release(ClusterClogNativeBaseHandleV1 **held)
{
	cluster_clog_native_recovery_base_release_v1(held);
}

TestRelmapNativeClogStats
test_relmap_native_clog_done(void)
{
	char paths[3][MAXPGPATH];
	TestRelmapNativeClogStats result;
	fixture_idle();
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(unlink(relmap_native_path) == 0 || errno == ENOENT);
	snprintf(paths[0], MAXPGPATH, "%s/native_side", fixture_root);
	snprintf(paths[1], MAXPGPATH, "%s/native_side/origin_%d", fixture_root, fixture_origin);
	snprintf(paths[2], MAXPGPATH, "%s/native_side/origin_%d/pg_xact", fixture_root, fixture_origin);
	for (int i = 2; i >= 0; i--)
		if (relmap_native_dirs[i])
			UT_ASSERT_EQ(rmdir(paths[i]), 0);
	result = test_relmap_native_clog_stats();
	for (unsigned i = 0; i < allocation_count; i++)
		free(allocations[i]);
	allocation_count = fixture_lock_count = 0;
	ClogRecoveryLocks = NULL;
	memset(ClogRecoveryCtl, 0, sizeof(ClogRecoveryCtl));
	fail_sync = fail_dirsync = expect_error = base_fixture = false;
	directory_sync_error = 0;
	fixture_cold_origin = NULL;
	PG_exception_stack = NULL;
	return result;
}
