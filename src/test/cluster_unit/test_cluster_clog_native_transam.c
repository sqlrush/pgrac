/* The real transaction-log cache, CLOG and SLRU over framed temporary files.
 * Selected control custody and scheduling use the original finite providers. */
#define NATIVE_CLOG_IO_MAIN transam_native_original_main
int transam_native_original_main(void);
#include "test_cluster_clog_native_io.c"

#ifdef NATIVE_TRANSAM_SOURCE
#include NATIVE_TRANSAM_SOURCE
#else
#include "../../backend/access/transam/transam.c"
#endif

static void
transam_reset(void)
{
	native_reset(); dirty[0] = false;
	UT_ASSERT(managed_read());
	control_held = false;
	cachedFetchXid = InvalidTransactionId;
	cachedFetchXidStatus = TRANSACTION_STATUS_IN_PROGRESS;
	cachedCommitLSN = InvalidXLogRecPtr;
}

static bool
transam_read_raises(bool commit_lsn)
{
	volatile bool caught = false;

	PG_TRY();
	{
		if (commit_lsn)
			(void) TransactionIdGetCommitLSN(98304);
		else
			(void) TransactionLogFetch(98304);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	return caught;
}

UT_TEST(managed_status_cannot_reuse_raw_xid_after_identity_loss)
{
	transam_reset();
	UT_ASSERT_EQ(TransactionLogFetch(98304), TRANSACTION_STATUS_COMMITTED);
	native_missing = true;
	UT_ASSERT(transam_read_raises(false));
	/* The original ERROR caller retains its family lock until abort cleanup. */
	UT_ASSERT(control_held && !io_held);
	if (control_held) LWLockRelease(&control);
	UT_ASSERT_EQ(native_borrows, 0);
	UT_ASSERT_EQ(inserts + writes + queues, 0);
	close(borrowed);
}

UT_TEST(managed_commit_lsn_cannot_reuse_raw_xid_after_identity_loss)
{
	transam_reset(); lsns[0] = 70;
	UT_ASSERT_EQ(TransactionLogFetch(98304), TRANSACTION_STATUS_COMMITTED);
	native_missing = true;
	UT_ASSERT(transam_read_raises(true));
	UT_ASSERT(control_held && !io_held);
	if (control_held) LWLockRelease(&control);
	UT_ASSERT_EQ(native_borrows, 0);
	UT_ASSERT_EQ(inserts + writes + queues, 0);
	close(borrowed);
}

UT_TEST(managed_cache_hits_recheck_original_binding_birth_and_current)
{
	for (unsigned lsn = 0; lsn < 2; lsn++)
		for (unsigned fault = 0; fault < 4; fault++) {
			transam_reset();
			UT_ASSERT_EQ(TransactionLogFetch(98304), TRANSACTION_STATUS_COMMITTED);
			if (fault == 0) native_current = false;
			if (fault == 1) cached_bindings[0].identity.creation_generation++;
			if (fault == 2) identities[0].identity.birth_observed = false;
			if (fault == 3) native_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
			UT_ASSERT(transam_read_raises(lsn != 0));
			UT_ASSERT(control_held && !io_held);
			if (control_held) LWLockRelease(&control);
			UT_ASSERT_EQ(native_borrows, 0);
			UT_ASSERT_EQ(native_acquires, native_releases);
			UT_ASSERT_EQ(inserts + writes + queues, 0);
			close(borrowed);
		}
}

UT_TEST(managed_creation_mode_rechecks_without_cluster_serving)
{
	for (unsigned lsn = 0; lsn < 2; lsn++) {
		transam_reset(); cluster_enabled = false;
		UT_ASSERT_EQ(TransactionLogFetch(98304), TRANSACTION_STATUS_COMMITTED);
		native_missing = true;
		UT_ASSERT(transam_read_raises(lsn != 0));
		UT_ASSERT_EQ(native_borrows, 0);
		UT_ASSERT_EQ(native_opens + inserts + writes, 0);
		close(borrowed);
	}
}

UT_TEST(managed_entry_retires_preexisting_legacy_cache)
{
	for (unsigned lsn = 0; lsn < 2; lsn++) {
		transam_reset();
		cachedFetchXid = 98304;
		cachedFetchXidStatus = TRANSACTION_STATUS_ABORTED;
		cachedCommitLSN = 999;
		if (lsn)
			UT_ASSERT_EQ(TransactionIdGetCommitLSN(98304), 0);
		else
			UT_ASSERT_EQ(TransactionLogFetch(98304), TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT_EQ(cachedFetchXid, InvalidTransactionId);
		UT_ASSERT_EQ(native_acquires, 1);
		UT_ASSERT_EQ(native_acquires, native_releases);
		UT_ASSERT(!control_held && !io_held);
		close(borrowed);
	}
}

UT_TEST(managed_permanent_xids_keep_original_semantics_without_file_io)
{
	transam_reset(); native_missing = true;
	UT_ASSERT_EQ(TransactionLogFetch(BootstrapTransactionId), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(TransactionLogFetch(FrozenTransactionId), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(TransactionIdGetCommitLSN(FrozenTransactionId), InvalidXLogRecPtr);
	UT_ASSERT_EQ(native_acquires + native_opens + inserts + writes, 0);
	UT_ASSERT(!control_held && !io_held);
	close(borrowed);
}

UT_TEST(legacy_terminal_status_and_commit_lsn_still_use_original_cache)
{
	transam_reset(); cluster_enabled = false;
	native_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
	/* This is an original raw resident page, without a managed binding. */
	memset(cached_bindings, 0, sizeof(cached_bindings)); lsns[0] = 70;
	UT_ASSERT_EQ(TransactionLogFetch(98304), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(cachedFetchXid, 98304);
	UT_ASSERT_EQ(cachedCommitLSN, 70);
	/* A repeat must not touch any shared cache or require an open file. */
	states[0] = SLRU_PAGE_EMPTY;
	UT_ASSERT_EQ(TransactionLogFetch(98304), TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT_EQ(TransactionIdGetCommitLSN(98304), 70);
	UT_ASSERT_EQ(native_acquires + native_opens + inserts + writes, 0);
	UT_ASSERT(!control_held && !io_held);
	close(borrowed);
}

int
main(void)
{
	strlcpy(directory, "/tmp/pgrac-clog-transam-XXXXXX", sizeof(directory));
	UT_ASSERT_NOT_NULL(mkdtemp(directory));
	snprintf(filename, sizeof(filename), "%s/0000", directory);
	UT_PLAN(7);
	UT_RUN(managed_status_cannot_reuse_raw_xid_after_identity_loss);
	UT_RUN(managed_commit_lsn_cannot_reuse_raw_xid_after_identity_loss);
	UT_RUN(managed_cache_hits_recheck_original_binding_birth_and_current);
	UT_RUN(managed_creation_mode_rechecks_without_cluster_serving);
	UT_RUN(managed_entry_retires_preexisting_legacy_cache);
	UT_RUN(managed_permanent_xids_keep_original_semantics_without_file_io);
	UT_RUN(legacy_terminal_status_and_commit_lsn_still_use_original_cache);
	UT_ASSERT_EQ(unlink(filename), 0);
	UT_ASSERT_EQ(rmdir(directory), 0);
	UT_DONE();
	return ut_failed_count != 0;
}
