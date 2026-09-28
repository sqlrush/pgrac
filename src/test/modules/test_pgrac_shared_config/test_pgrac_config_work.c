/*-------------------------------------------------------------------------
 *
 * test_pgrac_config_work.c
 *    Native command ownership during configuration delivery.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/modules/test_pgrac_shared_config/test_pgrac_config_work.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <unistd.h>
#include "access/xact.h"
#include "commands/vacuum.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_shared_config.h"
#endif

PG_FUNCTION_INFO_V1(test_pgrac_config_work_state);
PG_FUNCTION_INFO_V1(test_pgrac_config_work_launch);
PGDLLEXPORT void test_pgrac_config_work_main(Datum arg);
void test_pgrac_config_work_init(void);

#ifdef USE_PGRAC_CLUSTER
static ProcessUtility_hook_type previous_utility;
static bool probe_utility;
static uint64 work_before, work_after;
static bool work_received;

static uint64
work_generation(void)
{
	ClusterSharedConfigProcess actual;
	return cluster_shared_config_process_observe(&actual) ? actual.ref.identity.generation : 0;
}

/* These fixed files belong only to the disposable TAP cluster. No product
 * hooks or simulated transaction/role state: pause a real owner, let the PM
 * receive the next image, then call the real delivery and utility paths. */
static void
work_file(const char *suffix, const char *body)
{
	char path[MAXPGPATH];
	FILE *file;
	snprintf(path, sizeof(path), "%s/test_config.work_%s", DataDir, suffix);
	file = AllocateFile(path, "w");
	if (file == NULL || fputs(body, file) < 0 || FreeFile(file) != 0)
		ereport(ERROR, (errmsg("could not write configuration work fixture: %m")));
}

static void
work_receive(void)
{
	char path[MAXPGPATH];
	TimestampTz start = GetCurrentTimestamp();
	work_before = work_generation();
	work_after = 0;
	work_file("ready", "ready\n");
	snprintf(path, sizeof(path), "%s/test_config.work_go", DataDir);
	while (access(path, F_OK) != 0) {
		CHECK_FOR_INTERRUPTS();
		if (TimestampDifferenceExceeds(start, GetCurrentTimestamp(), 30000))
			ereport(ERROR, (errmsg("configuration work fixture was not released")));
		pg_usleep(10000L);
	}
	work_received = cluster_shared_config_delivery_reload();
}

static void
work_utility(PlannedStmt *pstmt, const char *query, bool read_only, ProcessUtilityContext context,
			 ParamListInfo params, QueryEnvironment *env, DestReceiver *dest, QueryCompletion *qc)
{
	bool observe = probe_utility && context == PROCESS_UTILITY_TOPLEVEL
				   && (IsA(pstmt->utilityStmt, VacuumStmt) || IsA(pstmt->utilityStmt, IndexStmt)
					   || IsA(pstmt->utilityStmt, DoStmt) || IsA(pstmt->utilityStmt, CallStmt));
	if (observe) {
		probe_utility = false;
		work_receive();
	}
	PG_TRY();
	{
		if (previous_utility)
			previous_utility(pstmt, query, read_only, context, params, env, dest, qc);
		else
			standard_ProcessUtility(pstmt, query, read_only, context, params, env, dest, qc);
	}
	PG_FINALLY();
	{
		if (observe)
			work_after = work_generation();
	}
	PG_END_TRY();
}
#endif

void
test_pgrac_config_work_init(void)
{
#ifdef USE_PGRAC_CLUSTER
	DefineCustomBoolVariable("test_pgrac_shared_config.probe_utility",
							 "Test-only native utility observer.", NULL, &probe_utility, false,
							 PGC_SUSET, 0, NULL, NULL, NULL);
	previous_utility = ProcessUtility_hook;
	ProcessUtility_hook = work_utility;
#endif
}

Datum
test_pgrac_config_work_state(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("test command observation requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	PG_RETURN_TEXT_P(cstring_to_text(psprintf("%llu:%llu:%d", (unsigned long long)work_before,
											  (unsigned long long)work_after, work_received)));
#else
	PG_RETURN_NULL();
#endif
}

Datum
test_pgrac_config_work_launch(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker = { 0 };
	BackgroundWorkerHandle *handle;
	pid_t pid;
	if (!superuser())
		ereport(ERROR, (errmsg("test command worker requires superuser")));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strlcpy(worker.bgw_library_name, "test_pgrac_shared_config", BGW_MAXLEN);
	strlcpy(worker.bgw_function_name, "test_pgrac_config_work_main", BGW_MAXLEN);
	strlcpy(worker.bgw_name, "test configuration direct vacuum", BGW_MAXLEN);
	strlcpy(worker.bgw_type, "test configuration direct vacuum", BGW_MAXLEN);
	worker.bgw_notify_pid = MyProcPid;
	if (!RegisterDynamicBackgroundWorker(&worker, &handle)
		|| WaitForBackgroundWorkerStartup(handle, &pid) != BGWH_STARTED)
		ereport(ERROR, (errmsg("could not start test command worker")));
	PG_RETURN_INT32(pid);
}

void
test_pgrac_config_work_main(Datum arg)
{
	(void)arg;
#ifdef USE_PGRAC_CLUSTER
	{
		VacuumParams params = { 0 };
		MemoryContext context;
		List *relations;
		char result[100];
		pqsignal(SIGTERM, die);
		pqsignal(SIGHUP, SignalHandlerForConfigReload);
		BackgroundWorkerUnblockSignals();
		BackgroundWorkerInitializeConnection("postgres", NULL, 0);
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		context
			= AllocSetContextCreate(TopMemoryContext, "test direct vacuum", ALLOCSET_DEFAULT_SIZES);
		MemoryContextSwitchTo(context);
		relations = list_make1(
			makeVacuumRelation(makeRangeVar(NULL, "utility_target", -1), InvalidOid, NIL));
		params.options = VACOPT_VACUUM | VACOPT_PROCESS_MAIN | VACOPT_PROCESS_TOAST;
		params.freeze_min_age = params.freeze_table_age = -1;
		params.multixact_freeze_min_age = params.multixact_freeze_table_age = -1;
		params.index_cleanup = params.truncate = VACOPTVALUE_UNSPECIFIED;
		params.nworkers = -1;
		work_receive();
		/* Direct native entry, exactly the bypass used by autovacuum. This is
		 * an actual background worker, never a fabricated autovacuum role. */
		vacuum(relations, &params, NULL, context, true);
		work_after = work_generation();
		snprintf(result, sizeof(result), "%llu:%llu:%d\n", (unsigned long long)work_before,
				 (unsigned long long)work_after, work_received);
		CommitTransactionCommand();
		MemoryContextSwitchTo(TopMemoryContext);
		MemoryContextDelete(context);
		work_file("result", result);
	}
#endif
	proc_exit(0);
}
