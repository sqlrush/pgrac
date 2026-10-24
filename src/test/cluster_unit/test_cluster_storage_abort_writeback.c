/* Original CREATE/pending-delete producers with their physical I/O boundary.
 * Author: SqlRush <sqlrush@gmail.com>
 */
int abort_storage_fixture_main(void);
#define main abort_storage_fixture_main
#define errcode abort_storage_fixture_errcode
#include "test_cluster_space_storage.c"
#undef main
#undef errcode

static int abort_case_error;
int errcode(int code);
int
errcode(int code)
{
	if (!expecting_error)
		abort();
	abort_case_error = code;
	return 0;
}

static void
created_abort_case(int cause)
{
	volatile bool caught = false;
	RelationData rel = {0};
	FormData_pg_class form = {0};

	reset();
	abort_case_error = 0;
	native_owner = true;
	UT_ASSERT(RelationCreateStorage(locator, RELPERSISTENCE_PERMANENT, true) == &storage);
	UT_ASSERT_NOT_NULL(native_pending);
	UT_ASSERT(!create_scope_live);
	if (cause != 0) {
		expecting_error = true;
		PG_TRY();
		{
			if (cause == 1)
				ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));
			else {
				rel.rd_locator = locator;
				rel.rd_backend = InvalidBackendId;
				rel.rd_smgr = &storage;
				rel.rd_rel = &form;
				form.relpersistence = RELPERSISTENCE_PERMANENT;
				storage_gate_xact.blockState = TBLOCK_INPROGRESS;
				RelationCheckStorageDDLTransaction(&rel, true, "DROP TABLE");
			}
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(abort_case_error, cause == 1 ? ERRCODE_DIVISION_BY_ZERO
													 : ERRCODE_ACTIVE_SQL_TRANSACTION);
		expecting_error = false;
	}
	abort_writeback_watch = true;
	smgrDoPendingDeletes(false);
	UT_ASSERT_EQ(abort_writeback_calls, 1);
	UT_ASSERT_EQ(unlink_calls, 1);
	UT_ASSERT_EQ(wal_calls, 1); /* No COMMIT or tombstone was manufactured. */
}

UT_TEST(created_rollback_writes_before_original_unlink)
{
	created_abort_case(0);
}
UT_TEST(created_error_writes_before_original_unlink)
{
	created_abort_case(1);
}
UT_TEST(created_refusal_writes_before_original_unlink)
{
	created_abort_case(2);
}

static void
expect_retained_abort_error(void)
{
	volatile bool caught = false;
	RelFileLocator *deletes = NULL;

	expecting_error = true;
	PG_TRY();
	{
		smgrDoPendingDeletes(false);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(abort_case_error, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	UT_ASSERT_EQ(unlink_calls, 0);
	UT_ASSERT_EQ(smgrGetPendingDeletes(false, &deletes), 1);
	pfree(deletes);
	expecting_error = false;
}

UT_TEST(created_identity_or_namespace_refusal_retains_original_pending)
{
	for (int fault = 0; fault < 8; fault++) {
		ClusterSpaceCreateObservationV1 saved;
		ClusterWalSourceRef saved_ref;
		PGAlignedBlock saved_page;
		reset();
		native_owner = true;
		RelationCreateStorage(locator, RELPERSISTENCE_PERMANENT, true);
		saved = native_pending->shared_create;
		saved_ref = ref;
		saved_page = page;
		if (fault == 0) native_pending->shared_create.identity.incarnation[0] ^= 1;
		if (fault == 1) native_pending->shared_create.identity.key.locator.dbOid++;
		if (fault == 2) native_pending->shared_create.create_serial = 0;
		if (fault == 3) native_pending->shared_create.end_rec_ptr = InvalidXLogRecPtr;
		if (fault == 4) native_pending->shared_create.identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
		if (fault == 5) native_pending->shared_create.identity.sequence++;
		if (fault == 6) ref.claim.database_incarnation++;
		if (fault == 7) page.data[128] ^= 1;
		expect_retained_abort_error();
		UT_ASSERT_EQ(abort_writeback_calls, 0);
		/* Restore only controlled fixture inputs for original cleanup. */
		native_pending->shared_create = saved;
		ref = saved_ref;
		page = saved_page;
		smgrDoPendingDeletes(false);
		UT_ASSERT_EQ(unlink_calls, 1);
	}
}

UT_TEST(writer_refusal_keeps_the_whole_native_pending_list)
{
	RelationData rel = {0};
	FormData_pg_class form = {0};
	RelFileLocator *deletes = NULL;

	reset();
	native_owner = true;
	RelationCreateStorage(locator, RELPERSISTENCE_PERMANENT, true);
	rel.rd_locator = locator;
	rel.rd_backend = InvalidBackendId;
	rel.rd_smgr = &storage;
	rel.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	storage.smgr_owner = &rel.rd_smgr;
	RelationDropStorage(&rel); /* Separate native at-commit entry, ahead of abort. */
	abort_writeback_refused = true;
	expect_retained_abort_error();
	UT_ASSERT_EQ(abort_writeback_calls, 1);
	UT_ASSERT_EQ(smgrGetPendingDeletes(true, &deletes), 1);
	pfree(deletes);
	abort_writeback_refused = false;
	smgrDoPendingDeletes(false);
	UT_ASSERT_EQ(unlink_calls, 1);
}

UT_TEST(identity_drift_after_output_still_retains_original_pending)
{
	PGAlignedBlock before;
	reset();
	native_owner = true;
	RelationCreateStorage(locator, RELPERSISTENCE_PERMANENT, true);
	before = page;
	abort_writeback_drift = true;
	expect_retained_abort_error();
	UT_ASSERT_EQ(abort_writeback_calls, 1);
	abort_writeback_drift = false;
	page = before;
	smgrDoPendingDeletes(false);
	UT_ASSERT_EQ(unlink_calls, 1);
}

UT_TEST(committing_create_does_not_write_or_unlink_abort_storage)
{
	reset();
	native_owner = true;
	RelationCreateStorage(locator, RELPERSISTENCE_PERMANENT, true);
	smgrDoPendingDeletes(true);
	UT_ASSERT_EQ(abort_writeback_calls + unlink_calls, 0);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(created_rollback_writes_before_original_unlink);
	UT_RUN(created_error_writes_before_original_unlink);
	UT_RUN(created_refusal_writes_before_original_unlink);
	UT_RUN(created_identity_or_namespace_refusal_retains_original_pending);
	UT_RUN(writer_refusal_keeps_the_whole_native_pending_list);
	UT_RUN(identity_drift_after_output_still_retains_original_pending);
	UT_RUN(committing_create_does_not_write_or_unlink_abort_storage);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
