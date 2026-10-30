/* Original native observation and local CLOG/SLRU in one lock domain.
 * Reuse the unchanged CLOG fixture's namespacing and controlled PG leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_relmap_native_clog_namespace.h"
#define cluster_relmap_retention_truncate_clog_v2 d3_clog_cluster_relmap_retention_truncate_clog_v2
#undef ShmemVariableCache
#define ShmemVariableCache d3_publish_ShmemVariableCache
#define LWLockHeldByMeInMode d3_publish_birth_LWLockHeldByMeInMode
#define cluster_relmap_owner_terminal_matches_v2 d3_publish_owner_terminal_matches_v2
#define cluster_relmap_recovery_issued_publish d3_publish_issued_publish
#define cluster_relmap_recovery_native_bounds d3_publish_native_bounds
#define cluster_relmap_recovery_native_terminal_capture d3_publish_native_terminal_capture
#define cluster_relmap_recovery_native_terminal_sync d3_publish_native_terminal_sync
int d3_clog_main(void);
#define TEST_CLUSTER_CLOG_EXTERNAL_VARIABLE_CACHE
#include "test_cluster_clog_recovery.c"
#undef TEST_CLUSTER_CLOG_EXTERNAL_VARIABLE_CACHE
#undef LWLockHeldByMeInMode
#define LWLockHeldByMeInMode d3_publish_LWLockHeldByMeInMode
bool LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode);
#include "test_cluster_relmap_publish_clog.h"
#include "../../backend/cluster/cluster_relmap_recovery.c"

static VariableCacheData publish_variables;
VariableCache ShmemVariableCache = &publish_variables;
static bool publish_issuing;
static unsigned publish_calls, publish_owner_checks;
static TransactionId publish_xid;
static bool (*publish_owner_current)(const ClusterRelmapOwnerSampleV2 *);

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return publish_issuing && lock == XidGenLock && mode == LW_EXCLUSIVE && LWLockHeldByMe(lock);
}

bool
cluster_relmap_owner_terminal_matches_v2(const ClusterRelmapOwnerSampleV2 *sample)
{
	publish_owner_checks++;
	UT_ASSERT(!LWLockHeldByMe(XidGenLock) && !LWLockHeldByMe(XactTruncationLock)
		&& !LWLockHeldByMe(XactSLRULock));
	return publish_owner_current != NULL && publish_owner_current(sample);
}

TestRelmapPublishClogStats
test_relmap_publish_clog_stats(void)
{
	TestRelmapPublishClogStats result = { 0 };
	char path[MAXPGPATH];
	uint8 byte = 0;
	int fd;

	result.calls = publish_calls;
	result.owner_checks = publish_owner_checks;
	result.wal_flushes = native_wal_flushes;
	result.sync_requests = native_sync_requests;
	result.file_syncs = syncs;
	result.dir_syncs = dirsyncs;
	result.open_fds = transient_fds;
	result.holdoff = InterruptHoldoffCount;
	result.assertion_failed = ut_current_failed;
	for (unsigned i = 0; i < fixture_lock_count; i++)
		result.locks_held |= fixture_locks[i].held;
	snprintf(path, sizeof(path), "%s/pg_xact/0000", fixture_root);
	fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pread(fd, &byte, 1, TransactionIdToByte(publish_xid)), 1);
	UT_ASSERT_EQ(close(fd), 0);
	result.disk_status = (byte >> (TransactionIdToBIndex(publish_xid) * CLOG_BITS_PER_XACT))
		& CLOG_XACT_BITMASK;
	result.assertion_failed |= ut_current_failed;
	return result;
}

void
test_relmap_publish_clog_setup(const char *root, const ClusterRelmapOwnerSampleV2 *sample,
	XidStatus status, unsigned fault)
{
	int previous, slot;

	strlcpy(fixture_root, root, sizeof(fixture_root));
	ut_current_failed = 0;
	publish_xid = (TransactionId)sample->owner.top_full_xid;
	UT_ASSERT(TransactionIdToPage(publish_xid) == 0);
	native_fixture = true;
	cluster_shared_data_dir = fixture_root;
	cluster_node_id = sample->owner.node;
	InRecovery = false;
	MyBackendType = B_BACKEND;
	MyAuxProcType = NotAnAuxProcess;
	publish_calls = publish_owner_checks = 0;
	native_wal_flushes = native_sync_requests = 0;
	writes = reads = syncs = dirsyncs = 0;
	enableFsync = true;
	LWLockInitialize(XidGenLock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XactTruncationLock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XactSLRULock, LWTRANCHE_XACT_BUFFER);
	previous = open(".", O_RDONLY | O_DIRECTORY);
	UT_ASSERT(previous >= 0 && chdir(root) == 0);
	UT_ASSERT_EQ(mkdir("pg_xact", 0700), 0);
	XactCtl->PagePrecedes = CLOGPagePrecedes;
	SimpleLruInit(XactCtl, "Relmap native publication test", 2, CLOG_LSNS_PER_PAGE,
		XactSLRULock, "pg_xact", LWTRANCHE_XACT_BUFFER, SYNC_HANDLER_CLOG);
	native_input(TRANSACTION_STATUS_IN_PROGRESS);
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	slot = SimpleLruZeroPage(XactCtl, 0);
	XactCtl->shared->page_buffer[slot][TransactionIdToByte(publish_xid)]
		= status << (TransactionIdToBIndex(publish_xid) * CLOG_BITS_PER_XACT);
	XactCtl->shared->group_lsn[GetLSNIndex(slot, publish_xid)] = 1234;
	LWLockRelease(XactSLRULock);
	UT_ASSERT_EQ(fchdir(previous), 0);
	UT_ASSERT_EQ(close(previous), 0);
	memset(&publish_variables, 0, sizeof(publish_variables));
	publish_variables.oldestClogXid = publish_xid - 8;
	publish_variables.nextXid = FullTransactionIdFromU64(sample->owner.top_full_xid + 5);
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	publish_issuing = true;
	cluster_relmap_recovery_issued_publish(FullTransactionIdFromU64(sample->owner.top_full_xid + 4));
	publish_issuing = false;
	LWLockRelease(XidGenLock);
	if (fault == 1) fail_sync = true;
	if (fault == 2) directory_sync_error = EIO;
	if (fault == 3) fail_sync_close = true;
	if (fault == 4) native_short = true;
	if (fault == 5) directory_open_error = EIO;
	if (fault == 6) publish_variables.clusterIssuedState = 2;
	if (fault == 7) fail_open = expect_error = true;
}

bool
test_relmap_publish_clog_sync(const ClusterRelmapOwnerSampleV2 *sample,
	ClusterRelmapRecoveryNativeTerminal *out,
	bool (*owner_current)(const ClusterRelmapOwnerSampleV2 *), sigjmp_buf *outer_stack)
{
	volatile bool result = false;
	int previous = open(".", O_RDONLY | O_DIRECTORY);

	UT_ASSERT(previous >= 0 && chdir(fixture_root) == 0);
	publish_calls++;
	publish_owner_current = owner_current;
	PG_exception_stack = outer_stack;
	PG_TRY();
	{
		result = cluster_relmap_recovery_native_terminal_sync(sample, out);
	}
	PG_FINALLY();
	{
		UT_ASSERT_EQ(fchdir(previous), 0);
		UT_ASSERT_EQ(close(previous), 0);
	}
	PG_END_TRY();
	PG_exception_stack = NULL;
	return result;
}

TestRelmapPublishClogStats
test_relmap_publish_clog_done(void)
{
	char path[MAXPGPATH];
	TestRelmapPublishClogStats result;

	fixture_idle();
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	result = test_relmap_publish_clog_stats();
	snprintf(path, sizeof(path), "%s/pg_xact/0000", fixture_root);
	UT_ASSERT_EQ(unlink(path), 0);
	snprintf(path, sizeof(path), "%s/pg_xact", fixture_root);
	UT_ASSERT_EQ(rmdir(path), 0);
	for (unsigned i = 0; i < allocation_count; i++)
		free(allocations[i]);
	allocation_count = fixture_lock_count = 0;
	memset(XactCtl, 0, sizeof(*XactCtl));
	native_fixture = expect_error = false;
	PG_exception_stack = NULL;
	publish_owner_current = NULL;
	result.assertion_failed |= ut_current_failed;
	return result;
}
