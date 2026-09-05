/*-------------------------------------------------------------------------
 *
 * test_pgrac_r4_native_tx.c
 *		Native transaction authority probes for PGRAC R4 tests.
 *
 * IDENTIFICATION
 *		src/test/modules/test_pgrac_r4_native_tx/test_pgrac_r4_native_tx.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/subtrans.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/table.h"
#include "utils/rel.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_hw.h"
#include "cluster/cluster_grd.h"
#include "storage/ipc.h"

static HwLock probe_hw_lock;
static bool probe_hw_callbacks_registered;

static void
probe_hw_xact_end(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_PRE_COMMIT || event == XACT_EVENT_ABORT
		|| event == XACT_EVENT_PRE_PREPARE)
		cluster_hw_unlock(&probe_hw_lock);
}

static void
probe_hw_backend_end(int code, Datum arg)
{
	cluster_hw_unlock(&probe_hw_lock);
}
#endif

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_pgrac_r4_two_phase_is_prepared);
PG_FUNCTION_INFO_V1(test_pgrac_r4_current_xid);
PG_FUNCTION_INFO_V1(test_pgrac_r4_subtrans_parent);
PG_FUNCTION_INFO_V1(test_pgrac_hw_lock);
PG_FUNCTION_INFO_V1(test_pgrac_hw_unlock);
PG_FUNCTION_INFO_V1(test_pgrac_hw_master);

/* Optional test module only. This does not allocate/extend/write relation data,
 * and is never installed into the canonical benchmark schema. Hold lifetime is
 * one explicit test transaction, with existing S6 cleanup on abort/backend exit. */
Datum
test_pgrac_hw_lock(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	Relation relation;
	ClusterResId resid;

	if (!superuser() || !IsTransactionBlock() || probe_hw_lock.held)
		ereport(ERROR,
				(errmsg("HW probe requires superuser, explicit transaction and no prior hold")));
	relation = table_open(PG_GETARG_OID(0), AccessShareLock);
	if (relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR, (errmsg("HW probe requires a permanent relation")));
	cluster_hw_resid_encode(relation->rd_locator, MAIN_FORKNUM, &resid);
	table_close(relation, NoLock);
	if (!probe_hw_callbacks_registered) {
		RegisterXactCallback(probe_hw_xact_end, NULL);
		before_shmem_exit(probe_hw_backend_end, (Datum)0);
		probe_hw_callbacks_registered = true;
	}
	if (!cluster_hw_lock(&resid, &probe_hw_lock) || !probe_hw_lock.coordinated)
		ereport(ERROR, (errmsg("HW probe failed to acquire an exact coordinated grant")));
	PG_RETURN_BOOL(true);
#else
	ereport(ERROR, (errmsg("HW probe requires a cluster build")));
	PG_RETURN_BOOL(false);
#endif
}

Datum
test_pgrac_hw_unlock(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	if (!superuser() || !probe_hw_lock.held)
		ereport(ERROR, (errmsg("HW probe release requires the original held grant")));
	cluster_hw_unlock(&probe_hw_lock);
	PG_RETURN_BOOL(true);
#else
	PG_RETURN_BOOL(false);
#endif
}

Datum
test_pgrac_hw_master(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	Relation relation;
	ClusterResId resid;
	int master;

	if (!superuser())
		ereport(ERROR, (errmsg("HW probe requires superuser")));
	relation = table_open(PG_GETARG_OID(0), AccessShareLock);
	cluster_hw_resid_encode(relation->rd_locator, MAIN_FORKNUM, &resid);
	master = cluster_grd_lookup_master(&resid);
	table_close(relation, AccessShareLock);
	PG_RETURN_INT32(master);
#else
	PG_RETURN_INT32(-1);
#endif
}

Datum
test_pgrac_r4_two_phase_is_prepared(PG_FUNCTION_ARGS)
{
	TransactionId xid = PG_GETARG_TRANSACTIONID(0);

	PG_RETURN_BOOL(TwoPhaseTransactionIdIsPrepared(xid));
}

Datum
test_pgrac_r4_current_xid(PG_FUNCTION_ARGS)
{
	PG_RETURN_TRANSACTIONID(GetCurrentTransactionId());
}

Datum
test_pgrac_r4_subtrans_parent(PG_FUNCTION_ARGS)
{
	TransactionId xid = PG_GETARG_TRANSACTIONID(0);

	PG_RETURN_TRANSACTIONID(SubTransGetParent(xid));
}
