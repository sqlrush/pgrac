/* Copyright (c) 2026, pgrac contributors
 * Test-only, superuser-only native exception witness.  No storage, authority,
 * timeout or transaction-state substitution.  Errors originate at the real
 * transaction callback, inside the production AbortTransaction call stack.
 */
#include "postgres.h"

#include "access/xact.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/ipc.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(test_pgrac_abort_arm);
PG_FUNCTION_INFO_V1(test_pgrac_abort_die);

static int fault_mode;
static int failure_fires;
static bool fail_commit;
static bool registered;

static void
lowlevel_witness(int code pg_attribute_unused(), Datum arg pg_attribute_unused())
{
	elog(LOG, "abort probe reached low-level cleanup; fires=%d", failure_fires);
}

static void
abort_fault(XactEvent event, void *arg pg_attribute_unused())
{
	if (event == XACT_EVENT_PRE_COMMIT && fail_commit) {
		fail_commit = false;
		ereport(ERROR, (errmsg("abort probe initial commit failure")));
	}
	if (event != XACT_EVENT_ABORT)
		return;
	elog(LOG, "abort probe entered callback; mode=%d attempt=%d", fault_mode, failure_fires + 1);
	/* Bound the unfixed ERROR path: expose a second entry, then let native
	 * abort finish.  This is a negative witness, not a soak error allowance. */
	if (fault_mode == 0 || failure_fires >= 2)
		return;
	failure_fires++;
	if (fault_mode == 1)
		ereport(ERROR, (errmsg("abort probe injected ERROR")));
	if (fault_mode == 2)
		ereport(FATAL, (errmsg("abort probe injected FATAL")));
	if (fault_mode == 3)
		AbortOutOfAnyTransaction();
}

Datum
test_pgrac_abort_arm(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("abort probe requires superuser")));
	if (PG_GETARG_INT32(0) < 0 || PG_GETARG_INT32(0) > 3)
		ereport(ERROR, (errmsg("invalid abort probe mode")));
	if (!registered) {
		RegisterXactCallback(abort_fault, NULL);
		on_shmem_exit(lowlevel_witness, 0);
		registered = true;
	}
	fault_mode = PG_GETARG_INT32(0);
	fail_commit = PG_GETARG_BOOL(1);
	failure_fires = 0;
	PG_RETURN_VOID();
}

Datum
test_pgrac_abort_die(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("abort probe requires superuser")));
	ereport(FATAL, (errmsg("abort probe initial FATAL")));
	PG_RETURN_VOID();
}
