/* Native smgr synchronization and elog boundary, test module only.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include "access/relation.h"
#include "catalog/pg_class.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/storage/cluster_shared_fs.h"
#endif

PG_FUNCTION_INFO_V1(test_pgrac_storage_sync);

Datum
test_pgrac_storage_sync(PG_FUNCTION_ARGS)
{
	Relation rel;
	SMgrRelation smgr;
	const char *route = "md";
	volatile int caught = 0;
	MemoryContext context = CurrentMemoryContext;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("storage synchronization test requires superuser")));
	rel = relation_open(PG_GETARG_OID(0), AccessShareLock);
	if (rel->rd_rel->relkind != RELKIND_RELATION ||
		rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR, (errmsg("storage synchronization test requires a permanent table")));
	smgr = RelationGetSmgr(rel);
#ifdef USE_PGRAC_CLUSTER
	if (smgr->smgr_which != 0)
	{
		const ClusterSharedFsOps *ops = cluster_shared_fs_get_active_ops();

		if (ops == NULL || ops->id != CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS)
			ereport(ERROR, (errmsg("storage synchronization test requires cluster_fs or md")));
		route = "cluster_fs";
	}
#endif
	if (PG_GETARG_BOOL(1))
	{
		PG_TRY();
		{
			/* The caller checkpoints before arming the syscall fault. Do not
			 * manufacture a provider handle or modify any admission input. */
			smgrimmedsync(smgr, MAIN_FORKNUM);
		}
		PG_CATCH();
		{
			ErrorData *error;

			MemoryContextSwitchTo(context);
			error = CopyErrorData();
			caught = error->sqlerrcode;
			FlushErrorState();
			FreeErrorData(error);
		}
		PG_END_TRY();
	}
	relation_close(rel, AccessShareLock);
	PG_RETURN_TEXT_P(cstring_to_text(psprintf("%s:%s", route,
		caught ? unpack_sql_state(caught) : "synced")));
}
