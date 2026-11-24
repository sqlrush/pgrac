/* Whole original CLOG/SLRU over real framed temporary files. Child processes
 * keep an old cassert failure distinct from the required production ERROR. */
#define NATIVE_CLOG_IO_MAIN status_native_original_main
int status_native_original_main(void);
#ifdef NATIVE_STATUS_FIXTURE_SOURCE
#include NATIVE_STATUS_FIXTURE_SOURCE
#else
#include "test_cluster_clog_native_io.c"
#endif
#include <sys/wait.h>

int
errhint(const char *fmt, ...)
{
	(void) fmt;
	return 0;
}

static void
status_put(TransactionId xid, XidStatus status)
{
	unsigned shift = (xid % 4) * 2;
	unsigned byte = (xid % 32768) / 4;

	page.data[byte] = ((uint8) page.data[byte] & ~(3U << shift)) | (status << shift);
}

static XidStatus
status_get(TransactionId xid)
{
	return ((uint8) page.data[(xid % 32768) / 4] >> ((xid % 4) * 2)) & 3;
}

static void
status_reset(void)
{
	native_reset(); dirty[0] = false;
	memset(page.data, 0, BLCKSZ);
}

static bool
status_attempt(TransactionId xid, int count, TransactionId *children, XidStatus status,
			   int pageno, bool adopt)
{
	volatile bool caught = false;

	PG_TRY();
	{
		if (adopt) {
			control_held = false;
			ClusterClogAdoptNativeStatus(xid, status);
		} else
			TransactionIdSetPageStatusInternal(xid, count, children, status, 70, pageno);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	return caught;
}

static void
status_refusal(unsigned variant)
{
	TransactionId xid = 98304, children[2] = {98305, 98306};
	int count = 0;
	XidStatus status = TRANSACTION_STATUS_ABORTED;
	PGAlignedBlock before;
	ClusterClogPageIdentityV2 identity;
	ClusterClogFileBindingV2 file;
	XLogRecPtr before_lsn[2];

	status_reset();
	if (variant == 0 || variant == 5) status_put(xid, TRANSACTION_STATUS_COMMITTED);
	if (variant == 1) {
		status_put(xid, TRANSACTION_STATUS_ABORTED);
		status = TRANSACTION_STATUS_COMMITTED; count = 2;
	}
	if (variant == 2) { count = 2; status_put(children[1], TRANSACTION_STATUS_COMMITTED); }
	if (variant == 3) {
		count = 2; status = TRANSACTION_STATUS_COMMITTED;
		status_put(children[1], TRANSACTION_STATUS_ABORTED);
	}
	if (variant == 4) {
		count = 1; status = TRANSACTION_STATUS_COMMITTED;
		status_put(children[0], TRANSACTION_STATUS_COMMITTED);
	}
	if (variant == 6) { count = 1; children[0] += 32768; }
	if (variant == 7) { count = 1; children[0] = FrozenTransactionId; }
	if (variant == 8) count = -1;
	if (variant == 9) status = 4;
	if (variant == 10) xid = FrozenTransactionId;
	before = page; identity = identities[0]; file = cached_bindings[0];
	memcpy(before_lsn, lsns, sizeof(before_lsn));
	UT_ASSERT(status_attempt(xid, count, children, status, 3, variant == 5));
	UT_ASSERT(control_held && !io_held);
	UT_ASSERT_EQ(memcmp(page.data, before.data, BLCKSZ), 0);
	UT_ASSERT_EQ(memcmp(identities, &identity, sizeof(identity)), 0);
	UT_ASSERT_EQ(memcmp(cached_bindings, &file, sizeof(file)), 0);
	UT_ASSERT_EQ(memcmp(lsns, before_lsn, sizeof(before_lsn)), 0);
	UT_ASSERT(!dirty[0]);
	UT_ASSERT_EQ(native_borrows, 0);
	UT_ASSERT_EQ(inserts + writes + queues, 0);
	LWLockRelease(&control);
	UT_ASSERT_EQ(close(borrowed), 0);
}

/* A debug Assert/abort is a failing observation, not a substitute for ERROR.
 * Parent cleanup only removes this unit child's unpublished temporary file. */
static bool
status_refusal_child(unsigned variant)
{
	pid_t child;
	int result;

	fflush(NULL);
	child = fork();
	if (child < 0) abort();
	if (child == 0) {
		ut_current_failed = 0;
		status_refusal(variant);
		fflush(NULL);
		_exit(ut_current_failed != 0);
	}
	if (waitpid(child, &result, 0) != child) abort();
	if (unlink(filename) != 0 && errno != ENOENT) abort();
	return WIFEXITED(result) && WEXITSTATUS(result) == 0;
}

UT_TEST(native_terminal_conflicts_refuse_before_any_page_change)
{
	for (unsigned variant = 0; variant < 2; variant++)
		UT_ASSERT(status_refusal_child(variant));
}

UT_TEST(native_late_child_conflicts_preserve_all_bytes_and_lsn)
{
	for (unsigned variant = 2; variant < 5; variant++)
		UT_ASSERT(status_refusal_child(variant));
}

UT_TEST(native_adopt_uses_the_same_terminal_transition_guard)
{
	UT_ASSERT(status_refusal_child(5));
}

UT_TEST(native_bad_page_arguments_refuse_before_status_changes)
{
	for (unsigned variant = 6; variant < 11; variant++)
		UT_ASSERT(status_refusal_child(variant));
}

UT_TEST(native_original_legal_transitions_keep_dirty_and_group_lsn)
{
	for (unsigned current = 0; current < 4; current++)
		for (unsigned target = 1; target < 4; target++) {
			TransactionId xid = 98304;
			ClusterClogPageIdentityV2 identity;

			if (current != 0 && current != 3 && current != target) continue;
			status_reset(); identity = identities[0]; status_put(xid, current);
			UT_ASSERT(!status_attempt(target == 3 ? InvalidTransactionId : xid,
				target == 3 ? 1 : 0, &xid, target, 3, false));
			UT_ASSERT_EQ(status_get(xid), target);
			UT_ASSERT(dirty[0] && control_held && !io_held);
			UT_ASSERT_EQ(lsns[0], 70);
			UT_ASSERT_EQ(memcmp(identities, &identity, sizeof(identity)), 0);
			UT_ASSERT_EQ(native_acquires, native_releases);
			UT_ASSERT_EQ(inserts + writes + queues, 0);
			LWLockRelease(&control); close(borrowed); unlink(filename);
		}
}

UT_TEST(native_recovery_committed_subcommit_noop_remains_legal)
{
	TransactionId xid = 98304, children[2] = {98305, 98306};

	status_reset(); InRecovery = true;
	status_put(xid, TRANSACTION_STATUS_COMMITTED);
	status_put(children[0], TRANSACTION_STATUS_COMMITTED);
	status_put(children[1], TRANSACTION_STATUS_SUB_COMMITTED);
	UT_ASSERT(!status_attempt(xid, 2, children, TRANSACTION_STATUS_COMMITTED, 3, false));
	UT_ASSERT_EQ(status_get(xid), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(status_get(children[0]), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(status_get(children[1]), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT(dirty[0] && control_held && !io_held);
	UT_ASSERT_EQ(native_borrows + inserts + writes + queues, 0);
	LWLockRelease(&control); close(borrowed); unlink(filename);
}

UT_TEST(legacy_legal_status_path_remains_unmanaged)
{
	TransactionId xid = 98304;

	status_reset(); cluster_enabled = false; native_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
	memset(cached_bindings, 0, sizeof(cached_bindings));
	UT_ASSERT(!status_attempt(xid, 0, NULL, TRANSACTION_STATUS_COMMITTED, 3, false));
	UT_ASSERT_EQ(status_get(xid), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT(dirty[0] && control_held && !io_held);
	UT_ASSERT_EQ(native_acquires + inserts + writes + queues, 0);
	LWLockRelease(&control); close(borrowed); unlink(filename);
}

int
main(void)
{
	strlcpy(directory, "/tmp/pgrac-clog-status-XXXXXX", sizeof(directory));
	UT_ASSERT_NOT_NULL(mkdtemp(directory));
	snprintf(filename, sizeof(filename), "%s/0000", directory);
	UT_PLAN(7);
	UT_RUN(native_terminal_conflicts_refuse_before_any_page_change);
	UT_RUN(native_late_child_conflicts_preserve_all_bytes_and_lsn);
	UT_RUN(native_adopt_uses_the_same_terminal_transition_guard);
	UT_RUN(native_bad_page_arguments_refuse_before_status_changes);
	UT_RUN(native_original_legal_transitions_keep_dirty_and_group_lsn);
	UT_RUN(native_recovery_committed_subcommit_noop_remains_legal);
	UT_RUN(legacy_legal_status_path_remains_unmanaged);
	UT_ASSERT_EQ(rmdir(directory), 0);
	UT_DONE();
	return ut_failed_count != 0;
}
