/* PGRAC: native catalog cache boundary test, not installed by default.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/transam.h"
#include "catalog/catalog.h"
#include "catalog/pg_class.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/smgr.h"
#include "storage/sinvaladt.h"
#include "utils/builtins.h"
#include "utils/relcache.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "cluster/storage/cluster_smgr.h"
#endif

PG_FUNCTION_INFO_V1(test_pgrac_catalog_reset);
PG_FUNCTION_INFO_V1(test_pgrac_set_next_oid);
PG_FUNCTION_INFO_V1(test_pgrac_relfile_candidate);
PG_FUNCTION_INFO_V1(test_pgrac_shared_relfile_candidate);
PG_FUNCTION_INFO_V1(test_pgrac_shared_drop_replay);
PG_FUNCTION_INFO_V1(test_pgrac_shared_drop_database_guard);

/* These counter controls are only for disposable native test instances. */
static void
check_native_oid_test(void)
{
	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("OID candidate test requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled || cluster_shared_catalog || cluster_shared_config)
		ereport(ERROR, (errmsg("OID candidate test requires a native test instance")));
#else
	ereport(ERROR, (errmsg("cluster support required")));
#endif
}

Datum
test_pgrac_set_next_oid(PG_FUNCTION_ARGS)
{
	check_native_oid_test();
	LWLockAcquire(OidGenLock, LW_EXCLUSIVE);
	ShmemVariableCache->nextOid = PG_GETARG_OID(0);
	ShmemVariableCache->oidCount = 0;
	LWLockRelease(OidGenLock);
	PG_RETURN_VOID();
}

/* Reach the utility refusal from inside an already-started native command.
 * No quorum/admission qualification is supplied, and neither shared flag
 * survives this scope. The direct leg also exercises dropdb's own boundary. */
Datum
test_pgrac_shared_drop_database_guard(PG_FUNCTION_ARGS)
{
	check_native_oid_test();
#ifdef USE_PGRAC_CLUSTER
	SPI_connect();
	PG_TRY();
	{
		cluster_shared_config = PG_GETARG_BOOL(0);
		cluster_shared_catalog = !cluster_shared_config;
		if (PG_GETARG_BOOL(1))
			dropdb("retained_database", false, false);
		else
			SPI_execute("DROP DATABASE retained_database", false, 0);
	}
	PG_FINALLY();
	{
		cluster_shared_config = false;
		cluster_shared_catalog = false;
		SPI_finish();
	}
	PG_END_TRY();
#endif
	PG_RETURN_VOID();
}

Datum
test_pgrac_relfile_candidate(PG_FUNCTION_ARGS)
{
	check_native_oid_test();
	PG_RETURN_OID(GetNewRelFileNumber(InvalidOid, NULL, RELPERSISTENCE_PERMANENT));
}

Datum
test_pgrac_shared_relfile_candidate(PG_FUNCTION_ARGS)
{
	Oid result = InvalidOid;
#ifdef USE_PGRAC_CLUSTER
	bool saved_route = cluster_smgr_user_relations;
	int saved_backend = cluster_shared_storage_backend;
	char *saved_root = cluster_shared_data_dir;
	char *test_root;
	Oid cached_oid;
	SMgrRelation volatile cached = NULL;
#endif

	check_native_oid_test();
#ifdef USE_PGRAC_CLUSTER
	if (saved_backend != CLUSTER_SHARED_FS_BACKEND_STUB || saved_route)
		ereport(ERROR, (errmsg("OID file probe requires the unused stub storage backend")));
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("test arguments are required")));
	test_root = text_to_cstring(PG_GETARG_TEXT_PP(0));
	cached_oid = PG_GETARG_OID(1);
	/* Exercise storage routing only.  The native allocator remains active;
	 * the cached-create case selects only the shared-catalog rejection guard,
	 * with no catalog I/O, shared allocation, or authority activation.
	 * All state below is backend-local.  The cached-create probe operates on
	 * an existing fixture file, never a catalog relation. */
	PG_TRY();
	{
		cluster_shared_fs_shutdown();
		cluster_shared_storage_backend = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS;
		cluster_shared_data_dir = test_root;
		cluster_shared_fs_init();
		cluster_smgr_user_relations = true;
		if (OidIsValid(cached_oid))
		{
			RelFileLocator locator = {MyDatabaseTableSpace, MyDatabaseId, cached_oid};

			cached = smgropen(locator, InvalidBackendId);
			(void)smgrnblocks(cached, MAIN_FORKNUM);
			cluster_shared_catalog = true;
			smgrcreate(cached, MAIN_FORKNUM, false);
			result = cached_oid;
		}
		else
			result = GetNewRelFileNumber(InvalidOid, NULL, RELPERSISTENCE_PERMANENT);
	}
	PG_FINALLY();
	{
		if (cached != NULL)
			smgrclose(cached);
		cluster_shared_catalog = false;
		cluster_shared_fs_shutdown();
		cluster_smgr_user_relations = saved_route;
		cluster_shared_storage_backend = saved_backend;
		cluster_shared_data_dir = saved_root;
		cluster_shared_fs_init();
	}
	PG_END_TRY();
	pfree(test_root);
#endif
	PG_RETURN_OID(result);
}

Datum
test_pgrac_catalog_reset(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("catalog cache test requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	if (cluster_enabled)
		ereport(ERROR, (errmsg("catalog cache test requires a native test instance")));
	RelationCacheInitFilePreInvalidateAll();
	SIResetAll();
	RelationCacheInitFilePostInvalidate();
#else
	ereport(ERROR, (errmsg("cluster support required")));
#endif
	PG_RETURN_VOID();
}

/* Invoke the real shared storage DROP replay entry in a native fixture only.
 * Backend-local routing selects the PRE2 retention guard; no shared catalog
 * I/O, admission, recovery authority or window-closed proof is synthesized. */
Datum
test_pgrac_shared_drop_replay(PG_FUNCTION_ARGS)
{
	check_native_oid_test();
#ifdef USE_PGRAC_CLUSTER
	{
		bool saved_route = cluster_smgr_user_relations;
		int saved_backend = cluster_shared_storage_backend;
		char *saved_root = cluster_shared_data_dir;
		char *test_root = text_to_cstring(PG_GETARG_TEXT_PP(0));
		RelFileLocatorBackend locator
			= { { MyDatabaseTableSpace, MyDatabaseId, PG_GETARG_OID(1) }, InvalidBackendId };

		if (saved_backend != CLUSTER_SHARED_FS_BACKEND_STUB || saved_route)
			ereport(ERROR,
					(errmsg("DROP replay fixture requires the unused stub storage backend")));
		PG_TRY();
		{
			cluster_shared_fs_shutdown();
			cluster_shared_storage_backend = CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS;
			cluster_shared_data_dir = test_root;
			cluster_shared_fs_init();
			cluster_smgr_user_relations = true;
			cluster_shared_catalog = true;
			cluster_smgr_unlink(locator, InvalidForkNumber, true);
		}
		PG_FINALLY();
		{
			cluster_shared_catalog = false;
			cluster_shared_fs_shutdown();
			cluster_smgr_user_relations = saved_route;
			cluster_shared_storage_backend = saved_backend;
			cluster_shared_data_dir = saved_root;
			cluster_shared_fs_init();
		}
		PG_END_TRY();
		pfree(test_root);
	}
#endif
	PG_RETURN_VOID();
}
