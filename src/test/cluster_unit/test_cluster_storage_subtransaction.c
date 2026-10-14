/*
 * Shared subtransaction DDL must refuse before the native storage producer.
 * Reuse the native I/O boundaries; catalog/storage.c is linked unchanged.
 * Author: SqlRush <sqlrush@gmail.com>
 */
int space_storage_fixture_main(void);
#define main space_storage_fixture_main
#define errcode space_fixture_errcode
#define errhint space_fixture_errhint
#include "test_cluster_space_storage.c"
#undef main
#undef errcode
#undef errhint

static int refusal_code;
static char refusal_hint[512];

int errcode(int code);
int errhint(const char *fmt,...) pg_attribute_printf(1, 2);

int
errcode(int code)
{
	refusal_code = code;
	return 0;
}

int
errhint(const char *fmt,...)
{
	va_list args;

	va_start(args, fmt);
	vsnprintf(refusal_hint, sizeof(refusal_hint), fmt, args);
	va_end(args);
	return 0;
}

static void
check_refusal(void)
{
	UT_ASSERT_EQ(reported_level, ERROR);
	UT_ASSERT_EQ(refusal_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	UT_ASSERT(strstr(refusal_hint, "top-level transaction") != NULL);
	UT_ASSERT(strstr(refusal_hint, "PL/pgSQL EXCEPTION") != NULL);
}

UT_TEST(test_child_create_refuses_before_storage_or_abort_owner)
{
	volatile bool caught = false;

	reset();
	native_owner = expecting_error = true;
	nest_level = 2;
	PG_TRY();
	{
		RelationCreateStorage(locator, RELPERSISTENCE_PERMANENT, true);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	check_refusal();
	UT_ASSERT_EQ(io_calls + main_create_calls + create_calls + wal_calls + dirty_calls, 0);
	UT_ASSERT_EQ(capture_calls + shared_identity_locks + maintenance_identity_locks, 0);
	UT_ASSERT(!delete_registered);
	/* Also exercise the original rollback cleanup, which must have no work. */
	AtSubAbort_smgr();
	UT_ASSERT_EQ(unlink_calls, 0);
	nest_level = 1;
	smgrDoPendingDeletes(true);
}

UT_TEST(test_child_drop_refuses_before_ko_or_pending_delete)
{
	RelationData rel = {0};
	FormData_pg_class form = {0};
	volatile bool caught = false;

	reset();
	native_owner = expecting_error = true;
	nest_level = 2;
	rel.rd_locator = locator;
	rel.rd_backend = InvalidBackendId;
	rel.rd_smgr = &storage;
	rel.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	storage.smgr_owner = &rel.rd_smgr;
	PG_TRY();
	{
		RelationDropStorage(&rel);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	check_refusal();
	UT_ASSERT_EQ(ko_calls + io_calls + wal_calls + dirty_calls, 0);
	UT_ASSERT(!delete_registered);
	AtSubAbort_smgr();
	nest_level = 1;
	smgrDoPendingDeletes(true);
	UT_ASSERT_EQ(unlink_calls, 0);
}

UT_TEST(test_child_truncate_refuses_before_flush_or_structure)
{
	Relation rel = native_truncate_relation();
	PGAlignedBlock saved[2];
	volatile bool caught = false;
	unsigned before_io = io_calls;

	memcpy(saved, pages, sizeof(saved));
	nest_level = 2;
	expecting_error = true;
	PG_TRY();
	{
		RelationTruncate(rel, 4);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	check_refusal();
	UT_ASSERT_EQ(ko_calls + relation_flushes + wal_calls + dirty_calls + truncate_calls, 0);
	UT_ASSERT_EQ(io_calls, before_io);
	UT_ASSERT(memcmp(saved, pages, sizeof(saved)) == 0);
	nest_level = 1;
	FreeFakeRelcacheEntry(rel);
}

UT_TEST(test_private_child_create_keeps_native_cleanup)
{
	locator.relNumber = 1249;
	reset();
	shared = false;
	native_owner = true;
	nest_level = 2;
	UT_ASSERT(RelationCreateStorage(locator, RELPERSISTENCE_PERMANENT, true) == &storage);
	UT_ASSERT_EQ(main_create_calls, 1);
	UT_ASSERT_EQ(create_calls + wal_calls + dirty_calls, 0);
	UT_ASSERT(delete_registered);
	AtSubAbort_smgr();
	UT_ASSERT_EQ(unlink_calls, 1);
	nest_level = 1;
	locator.relNumber = 16384;
}

UT_TEST(test_temp_and_disabled_storage_keep_native_eligibility)
{
	reset();
	nest_level = 3;
	RelationCheckStorageTransaction(locator, RELPERSISTENCE_TEMP, "creation");
	cluster_enabled = false;
	RelationCheckStorageTransaction(locator, RELPERSISTENCE_PERMANENT, "creation");
	cluster_enabled = true;
	UT_ASSERT_EQ(io_calls + wal_calls + ko_calls, 0);
	UT_ASSERT(!delete_registered);
}

UT_TEST(test_shared_catalog_and_internal_create_cannot_bypass_refusal)
{
	for (int variant = 0; variant < 3; variant++)
	{
		volatile bool caught = false;

		reset();
		nest_level = 3;
		native_owner = expecting_error = true;
		cluster_shared_catalog = variant == 0;
		if (cluster_shared_catalog)
			locator.relNumber = 1249;
		PG_TRY();
		{
			RelationCreateStorage(locator,
				variant == 2 ? RELPERSISTENCE_UNLOGGED : RELPERSISTENCE_PERMANENT,
				false);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		check_refusal();
		UT_ASSERT_EQ(io_calls + main_create_calls + wal_calls + capture_calls, 0);
		UT_ASSERT(!delete_registered);
		locator.relNumber = 16384;
		cluster_shared_catalog = false;
	}
}

int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(7);
	UT_RUN(test_child_create_refuses_before_storage_or_abort_owner);
	UT_RUN(test_child_drop_refuses_before_ko_or_pending_delete);
	UT_RUN(test_child_truncate_refuses_before_flush_or_structure);
	UT_RUN(test_private_child_create_keeps_native_cleanup);
	UT_RUN(test_temp_and_disabled_storage_keep_native_eligibility);
	UT_RUN(test_shared_catalog_and_internal_create_cannot_bypass_refusal);
	UT_RUN(test_native_create_registers_abort_cleanup_before_space);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
