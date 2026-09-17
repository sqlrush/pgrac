/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual HW shmem/read/apply/allocation/checkpoint with real temporary files.
 * Hash/lock and own-WAL inputs are fixtures, not native Startup qualification. */
int allocation_stop_fixture_main(void);
#define main allocation_stop_fixture_main
#include "test_cluster_allocation_stop.c"
#undef main
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "catalog/pg_control.h"
#include "cluster/cluster_semantic_activation.h"

/* This suite qualifies HW in isolation. Its actual Startup tail must leave
 * the separately tested normal-TT producer untouched in EXISTING_OTHER. */
ClusterNormalStartState
cluster_semantic_normal_start_state(void)
{
	return CLUSTER_NORMAL_START_EXISTING_OTHER;
}
bool
cluster_semantic_normal_start_finish(const char **failure)
{
	abort();
}

AuxProcType MyAuxProcType = StartupProcess;
static char cold_root[] = "/tmp/pgrac-hw-cold-XXXXXX";
static unsigned cold_reads, cold_writes;
static bool cold_cf_close_ok = true;
static unsigned cold_cf_closes;
bool cluster_cf_owner_eor_close(void);
bool
cluster_cf_owner_eor_close(void)
{
	cold_cf_closes++;
	return cold_cf_close_ok;
}
int
errdetail(const char *fmt, ...)
{
	return 0;
}

/* Linux arm64 libpgport's runtime CRC selection uses the internal variant. */
int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* These are verbatim real caller tails, not a copy of the expected calls.
 * Earlier WAL decoding / recovery decision and CF completion are fixtures. */
static void
actual_init_tail(bool wasShutdown, bool haveBackupLabel, bool InRecovery,
				 bool ArchiveRecoveryRequested, XLogRecPtr own_redo)
{
	XLogRecPtr abortedRecPtr, missingContrecPtr;
	bool haveTblspcMap = false, result1, result2, result3;
	bool *wasShutdown_ptr = &result1, *haveBackupLabel_ptr = &result2,
		 *haveTblspcMap_ptr = &result3;
	CheckPoint checkPoint = { 0 };
	ControlFileData peer_control = { 0 }, *ControlFile = &peer_control;
	checkPoint.redo = own_redo;
	peer_control.checkPointCopy.redo = own_redo + 4096;
#include "test_cluster_hw_init_tail.inc"
	(void)ControlFile;
	(void)checkPoint;
	(void)abortedRecPtr;
	(void)missingContrecPtr;
}
static void
actual_complete_tail(void)
{
#include "test_cluster_hw_complete_tail.inc"
}
static void
actual_checkpoint_call(XLogRecPtr redo)
{
	CheckPoint checkPoint = { 0 };
	checkPoint.redo = redo;
#include "test_cluster_hw_checkpoint_call.inc"
}

uint64
GetSystemIdentifier(void)
{
	return UINT64CONST(987654321);
}
uint64
cluster_epoch_get_current(void)
{
	return 7;
}
XLogRecPtr
GetXLogInsertRecPtr(void)
{
	return 16384;
}
void *
palloc(Size size)
{
	void *p = malloc(size);
	Assert(p != NULL);
	return p;
}
void
pfree(void *p)
{
	free(p);
}
int
OpenTransientFile(const char *path, int flags)
{
	if ((flags & O_ACCMODE) == O_RDONLY)
		cold_reads++;
	else
		cold_writes++;
	return open(path, flags, 0600);
}
int
CloseTransientFile(int fd)
{
	return close(fd);
}
int
MakePGDirectory(const char *path)
{
	return mkdir(path, 0700);
}
int
pg_fsync(int fd)
{
	return fsync(fd);
}
int
errcode_for_file_access(void)
{
	return 0;
}
int
durable_rename(const char *oldpath, const char *newpath, int elevel)
{
	char dir[MAXPGPATH];
	int fd, rc;
	if (rename(oldpath, newpath) != 0)
		return -1;
	strlcpy(dir, newpath, sizeof(dir));
	*strrchr(dir, '/') = '\0';
	fd = open(dir, O_RDONLY);
	if (fd < 0)
		return -1;
	rc = fsync(fd);
	if (close(fd) != 0)
		rc = -1;
	return rc;
}
long
hash_get_num_entries(HTAB *table)
{
	TestHash *h = (TestHash *)table;
	long n = 0;
	Assert(held_lock == h->lock);
	for (long i = 0; i < h->capacity; i++)
		n += h->used[i];
	return n;
}
static void
cold_setup(void)
{
	reset_fixture();
	cluster_shared_data_dir = cold_root;
	cluster_node_id = 7;
	MyAuxProcType = StartupProcess;
	MyBackendType = B_STARTUP;
	fixture_throws = true;
	cold_reads = cold_writes = 0;
}
static ClusterResId
cold_key(unsigned rel)
{
	ClusterResId key;
	cluster_hw_resid_encode((RelFileLocator){ 1663, 5, rel }, MAIN_FORKNUM, &key);
	return key;
}
static void
cold_snapshot(ClusterHwSnapshotKind kind, XLogRecPtr redo, uint64 sysid, unsigned n)
{
	ClusterHwSnapshotEntry entries[3];
	Assert(n <= lengthof(entries));
	for (unsigned i = 0; i < n; i++) {
		entries[i].resid = cold_key(20000 + i);
		entries[i].next_hwm = 100 + i * 100;
	}
	cluster_hw_snapshot_write(cluster_node_id, cluster_node_id, 0, redo, sysid, kind, entries, n);
}
static ClusterHwStatus
cold_advance(unsigned rel, BlockNumber seed, BlockNumber *first)
{
	ClusterResId key = cold_key(rel);
	BlockNumber next;
	uint32 granted;
	return cluster_hw_try_advance(&key, 1, seed, first, &granted, &next);
}
static bool
cold_checkpoint_rejected(XLogRecPtr redo)
{
	volatile bool caught = false;
	PG_TRY();
	{
		cluster_hw_snapshot_checkpoint_write(redo);
	}
	PG_CATCH();
	{
		caught = true;
		/* Native FATAL/PANIC does not resume this process. The test catches
		 * it only to inspect rejection, then releases its fixture locks. */
		while (held_lock != NULL)
			LWLockRelease(held_lock);
	}
	PG_END_TRY();
	return caught;
}

UT_TEST(unclassified_cannot_allocate_or_publish_empty_checkpoint)
{
	BlockNumber first = 999;
	cold_setup();
	UT_ASSERT_EQ(cluster_hw_cold_boot_mode(), CLUSTER_HW_BOOT_UNCLASSIFIED);
	UT_ASSERT_EQ(cluster_hw_cold_boot_state(), CLUSTER_HW_COLD);
	UT_ASSERT_EQ(cold_advance(20000, 90, &first), CLUSTER_HW_NOT_READY);
	UT_ASSERT_EQ(first, 999);
	UT_ASSERT(cold_checkpoint_rejected(4096));
	UT_ASSERT_EQ(cold_writes, 0);
}
UT_TEST(normal_load_precedes_ready_and_first_allocation_uses_durable_hwm)
{
	const char *reason = NULL;
	BlockNumber first = 999;
	cold_setup();
	cold_snapshot(CLUSTER_HW_SNAPSHOT_CHECKPOINT, 4096, GetSystemIdentifier(), 1);
	UT_ASSERT(cluster_hw_startup_prepare(false, true, 4096, &reason));
	UT_ASSERT_EQ(cluster_hw_cold_boot_mode(), CLUSTER_HW_BOOT_NORMAL_SELF);
	UT_ASSERT_EQ(cluster_hw_cold_boot_state(), CLUSTER_HW_REBUILT);
	UT_ASSERT_EQ(cold_advance(20000, 90, &first), CLUSTER_HW_NOT_READY);
	UT_ASSERT(cluster_hw_startup_complete(&reason));
	UT_ASSERT_EQ(cluster_hw_cold_boot_state(), CLUSTER_HW_READY);
	UT_ASSERT_EQ(cold_advance(20000, 90, &first), CLUSTER_HW_OK);
	UT_ASSERT_EQ(first, 100);
	UT_ASSERT_EQ(cold_advance(21000, 300, &first), CLUSTER_HW_OK);
	UT_ASSERT_EQ(first, 300);
	UT_ASSERT(!cluster_hw_startup_complete(&reason));
}
UT_TEST(normal_rebuilt_allows_closing_checkpoint_but_not_service)
{
	const char *reason = NULL;
	ClusterHwSnapshotHeader hdr = { 0 };
	ClusterHwSnapshotEntry entries[3] = { { 0 } };
	BlockNumber first = 999;
	cold_setup();
	cold_snapshot(CLUSTER_HW_SNAPSHOT_CHECKPOINT, 4096, GetSystemIdentifier(), 2);
	UT_ASSERT(cluster_hw_startup_prepare(false, true, 4096, &reason));
	cluster_hw_snapshot_checkpoint_write(8192);
	UT_ASSERT_EQ(cluster_hw_snapshot_normal_read(7, GetSystemIdentifier(), 8192, &hdr, entries, 3),
				 CLUSTER_HW_NORMAL_READ_VALID);
	UT_ASSERT_EQ(hdr.n_entries, 2);
	UT_ASSERT_EQ(entries[0].next_hwm, 100);
	UT_ASSERT_EQ(entries[1].next_hwm, 200);
	UT_ASSERT_EQ(cluster_hw_cold_boot_state(), CLUSTER_HW_REBUILT);
	UT_ASSERT_EQ(cold_advance(20000, 90, &first), CLUSTER_HW_NOT_READY);
}
UT_TEST(normal_read_failure_is_sticky_and_never_becomes_recovery)
{
	const char *reason = NULL;
	BlockNumber first;
	for (unsigned leg = 0; leg < 4; leg++) {
		cold_setup();
		if (leg == 0)
			cluster_node_id = 127;
		else
			cold_snapshot(leg == 1 ? CLUSTER_HW_SNAPSHOT_ADOPTION : CLUSTER_HW_SNAPSHOT_CHECKPOINT,
						  leg == 2 ? 8192 : 4096, GetSystemIdentifier() + (leg == 3), 1);
		UT_ASSERT(!cluster_hw_startup_prepare(false, true, 4096, &reason));
		UT_ASSERT(reason != NULL);
		UT_ASSERT_EQ(cluster_hw_cold_boot_mode(), CLUSTER_HW_BOOT_NORMAL_SELF);
		UT_ASSERT_EQ(cluster_hw_cold_boot_state(), CLUSTER_HW_FAILED);
		UT_ASSERT(!cluster_hw_startup_prepare(true, false, 4096, &reason));
		UT_ASSERT_EQ(cluster_hw_cold_boot_mode(), CLUSTER_HW_BOOT_NORMAL_SELF);
		UT_ASSERT(!cluster_hw_startup_complete(&reason));
		UT_ASSERT_EQ(cold_advance(20000, 90, &first), CLUSTER_HW_NOT_READY);
		UT_ASSERT(cold_checkpoint_rejected(8192));
	}
}
UT_TEST(seed_metadata_is_maintained_without_enabling_single_node_allocation)
{
	const char *reason = NULL;
	BlockNumber first;
	ClusterHwSnapshotHeader hdr;
	cold_setup();
	fixture_node_count = 1;
	UT_ASSERT(cluster_hw_metadata_configured());
	UT_ASSERT(!cluster_hw_authority_active());
	cold_snapshot(CLUSTER_HW_SNAPSHOT_CHECKPOINT, 4096, GetSystemIdentifier(), 0);
	UT_ASSERT(cluster_hw_startup_prepare(false, true, 4096, &reason));
	cluster_hw_snapshot_checkpoint_write(8192);
	UT_ASSERT_EQ(cluster_hw_snapshot_normal_read(7, GetSystemIdentifier(), 8192, &hdr, NULL, 0),
				 CLUSTER_HW_NORMAL_READ_VALID);
	UT_ASSERT(cluster_hw_startup_complete(&reason));
	UT_ASSERT_EQ(cold_advance(20000, 0, &first), CLUSTER_HW_NOT_READY);
}
UT_TEST(recovery_keeps_original_load_and_state_contract)
{
	const char *reason = NULL;
	BlockNumber first;
	unsigned reads;
	cold_setup();
	cold_snapshot(CLUSTER_HW_SNAPSHOT_ADOPTION, 123, GetSystemIdentifier(), 1);
	reads = cold_reads;
	UT_ASSERT(cluster_hw_startup_prepare(true, false, 4096, &reason));
	UT_ASSERT_EQ(cold_reads, reads);
	UT_ASSERT_EQ(cluster_hw_cold_boot_mode(), CLUSTER_HW_BOOT_EXISTING_RECOVERY);
	cluster_hw_snapshot_recovery_load();
	UT_ASSERT_EQ(cold_advance(20000, 90, &first), CLUSTER_HW_OK);
	UT_ASSERT_EQ(first, 100);
	UT_ASSERT(cluster_hw_startup_complete(&reason));
	UT_ASSERT_EQ(cluster_hw_cold_boot_state(), CLUSTER_HW_COLD);
	UT_ASSERT(!cluster_hw_startup_prepare(false, true, 4096, &reason));
	UT_ASSERT_EQ(cluster_hw_cold_boot_mode(), CLUSTER_HW_BOOT_EXISTING_RECOVERY);
	fixture_node_count = 1;
	reads = cold_writes;
	cluster_hw_snapshot_checkpoint_write(8192);
	UT_ASSERT_EQ(cold_writes, reads);
}
UT_TEST(normal_has_no_partial_success_when_shared_table_cannot_hold_the_file)
{
	const char *reason = NULL;
	BlockNumber first;
	cold_setup();
	cold_snapshot(CLUSTER_HW_SNAPSHOT_CHECKPOINT, 4096, GetSystemIdentifier(), 2);
	((TestHash *)hw_htab)->capacity = 1;
	UT_ASSERT(!cluster_hw_startup_prepare(false, true, 4096, &reason));
	UT_ASSERT_EQ(cluster_hw_cold_boot_state(), CLUSTER_HW_FAILED);
	UT_ASSERT(!cluster_hw_startup_complete(&reason));
	UT_ASSERT_EQ(cold_advance(20000, 90, &first), CLUSTER_HW_NOT_READY);
}
UT_TEST(startup_role_classification_and_no_metadata_are_distinct)
{
	const char *reason = NULL;
	cold_setup();
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT(!cluster_hw_startup_prepare(false, true, 4096, &reason));
	UT_ASSERT_EQ(cluster_hw_cold_boot_mode(), CLUSTER_HW_BOOT_UNCLASSIFIED);
	MyAuxProcType = StartupProcess;
	UT_ASSERT(!cluster_hw_startup_prepare(false, false, 4096, &reason));
	UT_ASSERT_EQ(cluster_hw_cold_boot_state(), CLUSTER_HW_FAILED);
	UT_ASSERT(!cluster_hw_startup_prepare(true, false, 4096, &reason));
	cold_setup();
	cluster_shared_data_dir = NULL;
	UT_ASSERT(cluster_hw_startup_prepare(false, true, 4096, &reason));
	UT_ASSERT_EQ(cluster_hw_cold_boot_mode(), CLUSTER_HW_BOOT_DISABLED);
	UT_ASSERT(cluster_hw_startup_complete(&reason));
	UT_ASSERT(!cold_checkpoint_rejected(8192));
	UT_ASSERT_EQ(cold_reads + cold_writes, 0);
	cold_setup();
	cluster_node_id = 128;
	UT_ASSERT(!cluster_hw_metadata_configured());
	UT_ASSERT(!cluster_hw_startup_prepare(false, true, 4096, &reason));
}
UT_TEST(actual_wal_tail_uses_own_decoded_checkpoint_and_recovery_decision)
{
	cold_setup();
	cold_snapshot(CLUSTER_HW_SNAPSHOT_CHECKPOINT, 4096, GetSystemIdentifier(), 1);
	actual_init_tail(true, false, false, false, 4096);
	UT_ASSERT_EQ(cluster_hw_cold_boot_mode(), CLUSTER_HW_BOOT_NORMAL_SELF);
	UT_ASSERT_EQ(cluster_hw_cold_boot_state(), CLUSTER_HW_REBUILT);
	cold_setup();
	actual_init_tail(true, true, true, false, 4096);
	UT_ASSERT_EQ(cluster_hw_cold_boot_mode(), CLUSTER_HW_BOOT_EXISTING_RECOVERY);
	UT_ASSERT_EQ(cold_reads, 0);
	UT_ASSERT_EQ(cluster_hw_cold_boot_state(), CLUSTER_HW_COLD);
}
UT_TEST(actual_startup_completion_is_after_successful_cf_close)
{
	const char *reason = NULL;
	volatile bool caught = false;
	cold_setup();
	cold_snapshot(CLUSTER_HW_SNAPSHOT_CHECKPOINT, 4096, GetSystemIdentifier(), 1);
	UT_ASSERT(cluster_hw_startup_prepare(false, true, 4096, &reason));
	cold_cf_closes = 0;
	cold_cf_close_ok = false;
	PG_TRY();
	{
		actual_complete_tail();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cold_cf_closes, 1);
	UT_ASSERT_EQ(cluster_hw_cold_boot_state(), CLUSTER_HW_REBUILT);
	cold_cf_close_ok = true;
	actual_complete_tail();
	UT_ASSERT_EQ(cold_cf_closes, 2);
	UT_ASSERT_EQ(cluster_hw_cold_boot_state(), CLUSTER_HW_READY);
}
UT_TEST(actual_checkpoint_call_maintains_native_seed_metadata)
{
	const char *reason = NULL;
	ClusterHwSnapshotHeader hdr = { 0 };
	cold_setup();
	fixture_node_count = 1;
	cold_snapshot(CLUSTER_HW_SNAPSHOT_CHECKPOINT, 4096, GetSystemIdentifier(), 0);
	UT_ASSERT(cluster_hw_startup_prepare(false, true, 4096, &reason));
	actual_checkpoint_call(8192);
	UT_ASSERT_EQ(cluster_hw_snapshot_normal_read(7, GetSystemIdentifier(), 8192, &hdr, NULL, 0),
				 CLUSTER_HW_NORMAL_READ_VALID);
}
UT_TEST(rootless_storage_keeps_existing_hw_serve_gate)
{
	const char *reason = NULL;
	BlockNumber first = 999;
	cold_setup();
	cluster_shared_data_dir = NULL;
	UT_ASSERT_EQ(cold_advance(22000, 90, &first), CLUSTER_HW_NOT_READY);
	UT_ASSERT_EQ(first, 999);
	UT_ASSERT(cluster_hw_startup_prepare(false, true, 4096, &reason));
	UT_ASSERT(cluster_hw_startup_complete(&reason));
	UT_ASSERT_EQ(cluster_hw_cold_boot_mode(), CLUSTER_HW_BOOT_DISABLED);
	UT_ASSERT_EQ(cold_advance(22000, 90, &first), CLUSTER_HW_OK);
	UT_ASSERT_EQ(first, 90);
	first = 999;
	fixture_hw_master_generation = 1;
	UT_ASSERT_EQ(cold_advance(22000, 90, &first), CLUSTER_HW_NOT_READY);
	UT_ASSERT_EQ(first, 999);
	fixture_hw_master_generation = 0;
	UT_ASSERT_EQ(cold_advance(22000, 90, &first), CLUSTER_HW_OK);
	UT_ASSERT_EQ(first, 91);
	UT_ASSERT_EQ(cold_reads + cold_writes, 0);
}
int
main(void)
{
	if (mkdtemp(cold_root) == NULL)
		return 2;
	UT_PLAN(12);
	UT_RUN(unclassified_cannot_allocate_or_publish_empty_checkpoint);
	UT_RUN(normal_load_precedes_ready_and_first_allocation_uses_durable_hwm);
	UT_RUN(normal_rebuilt_allows_closing_checkpoint_but_not_service);
	UT_RUN(normal_read_failure_is_sticky_and_never_becomes_recovery);
	UT_RUN(seed_metadata_is_maintained_without_enabling_single_node_allocation);
	UT_RUN(recovery_keeps_original_load_and_state_contract);
	UT_RUN(normal_has_no_partial_success_when_shared_table_cannot_hold_the_file);
	UT_RUN(startup_role_classification_and_no_metadata_are_distinct);
	UT_RUN(actual_wal_tail_uses_own_decoded_checkpoint_and_recovery_decision);
	UT_RUN(actual_startup_completion_is_after_successful_cf_close);
	UT_RUN(actual_checkpoint_call_maintains_native_seed_metadata);
	UT_RUN(rootless_storage_keeps_existing_hw_serve_gate);
	UT_DONE();
	/* Leave only our fresh disposable test directory; never clear a user root. */
	return ut_failed_count != 0;
}
