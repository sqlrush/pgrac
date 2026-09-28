/*-------------------------------------------------------------------------
 *
 * test_pgrac_config_gate.c
 *    Drive a local native producer cut in a disposable real postmaster.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/modules/test_pgrac_shared_config/test_pgrac_config_gate.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <unistd.h>
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/wait_event.h"
#include "utils/timestamp.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_config_use_gate.h"
#include "cluster/cluster_shared_config.h"
#endif

PG_FUNCTION_INFO_V1(test_pgrac_config_gate_state);
PG_FUNCTION_INFO_V1(test_pgrac_config_future_launch);
PG_FUNCTION_INFO_V1(test_pgrac_config_background);
PGDLLEXPORT void test_pgrac_config_future_main(Datum arg);
void test_pgrac_config_gate_init(void);

#ifdef USE_PGRAC_CLUSTER
static int request;
static int bind_request;
static uint32 cut;

static void
future_file(const char *suffix, const char *value)
{
	char path[MAXPGPATH];
	FILE *file;
	snprintf(path, sizeof(path), "%s/test_config.future_%s", DataDir, suffix);
	file = AllocateFile(path, "w");
	if (file == NULL || fputs(value, file) < 0 || FreeFile(file) != 0)
		ereport(ERROR, (errmsg("cannot write native future fixture: %m")));
}

/* Test-only control at a genuinely empty local cut. Actual parent values,
 * not desired file bytes, become this fixture's local comparison target.
 * No all-node/service/CF agreement is supplied or claimed by this test. */
static void
gate_bind(int value, void *extra)
{
	ClusterConfigUseGate *gate;
	ClusterConfigUseTarget *target;
	ClusterSharedConfigProcess actual;
	ClusterSharedConfigActive common;
	bool ok;
	char result[100];
	if (value <= 0 || IsUnderPostmaster)
		return;
	gate = cluster_shared_config_delivery_native_gate();
	target = cluster_shared_config_delivery_native_target();
	ok = gate != NULL && target != NULL && cluster_shared_config_process_observe(&actual)
		 && !actual.failed && cluster_shared_config_common_profile(&common)
		 && cluster_config_use_target_bind(gate, target, cut, actual.node_id, &actual.ref, &common);
	snprintf(result, sizeof(result), "%d:%d:%llu\n", value, ok,
			 (unsigned long long)(ok ? target->ref.identity.generation : 0));
	future_file("bind", result);
}

/* Test input only: deliberately no fabricated CF, membership or service ACK.
 * Ordinary clients cannot control this mapping in a production installation.
 * The real PM remains runnable while independent transaction entry waits. */
static void
gate_request(int value, void *extra)
{
	ClusterConfigUseGate *gate;
	ClusterConfigUseGateState state;
	bool ok = true;
	unsigned waiting = 0;
	char path[MAXPGPATH];
	FILE *file;
	if (value <= 0 || IsUnderPostmaster
		|| (gate = cluster_shared_config_delivery_native_gate()) == NULL)
		return;
	if (value % 3 == 1)
		ok = cluster_config_use_gate_close(gate, &cut);
	else if (value % 3 == 0)
		ok = cluster_config_use_gate_open(gate, cut);
	state = cluster_config_use_gate_read(gate);
	if (ProcGlobal != NULL) {
		for (uint32 i = 0; i < ProcGlobal->allProcCount; ++i) {
			PGPROC *proc = &ProcGlobal->allProcs[i];
			ClusterSharedConfigRegistration actual;
			/* Count live regular clients, not auxiliary waiters or an exited
			 * PGPROC's leftover wait-event word. Native attach/detach owns it. */
			if (proc->wait_event_info == WAIT_EVENT_RECONFIG_SHARED_CONFIG_WAIT
				&& cluster_shared_config_registration_read(&proc->cluster_config, &actual)
				&& actual.pid > 0 && actual.pid == proc->pid && actual.role == B_BACKEND)
				++waiting;
		}
	}
	snprintf(path, sizeof(path), "%s/test_config.gate", DataDir);
	file = AllocateFile(path, "w");
	if (file == NULL)
		ereport(ERROR, (errmsg("cannot open native cut fixture: %m")));
	if (fprintf(file, "%d:%d:%u:%u:%d:%u\n", value, state.closed, state.epoch, state.owners, ok,
				waiting)
			< 0
		|| FreeFile(file) != 0)
		ereport(ERROR, (errmsg("cannot write native cut fixture: %m")));
}
#endif

void
test_pgrac_config_gate_init(void)
{
#ifdef USE_PGRAC_CLUSTER
	DefineCustomIntVariable("test_pgrac_shared_config.gate_request",
							"Test-only local cut control, not global configuration authority.",
							NULL, &request, 0, 0, 10000, PGC_SIGHUP, 0, NULL, gate_request, NULL);
	DefineCustomIntVariable("test_pgrac_shared_config.gate_bind",
							"Test-only native common-use binding.", NULL, &bind_request, 0, 0,
							10000, PGC_SIGHUP, 0, NULL, gate_bind, NULL);
#endif
}

Datum
test_pgrac_config_future_launch(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker = { 0 };
	BackgroundWorkerHandle *handle;
	pid_t pid;
	if (!superuser())
		ereport(ERROR, (errmsg("native future worker requires superuser")));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strlcpy(worker.bgw_library_name, "test_pgrac_shared_config", BGW_MAXLEN);
	strlcpy(worker.bgw_function_name, "test_pgrac_config_future_main", BGW_MAXLEN);
	strlcpy(worker.bgw_name, "test configuration future child", BGW_MAXLEN);
	strlcpy(worker.bgw_type, "test configuration future child", BGW_MAXLEN);
	worker.bgw_notify_pid = MyProcPid;
	if (!RegisterDynamicBackgroundWorker(&worker, &handle)
		|| WaitForBackgroundWorkerStartup(handle, &pid) != BGWH_STARTED)
		ereport(ERROR, (errmsg("could not start native future worker")));
	PG_RETURN_INT32(pid);
}

void
test_pgrac_config_future_main(Datum arg)
{
	(void)arg;
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterSharedConfigProcess actual;
		char path[MAXPGPATH], result[100];
		TimestampTz start = GetCurrentTimestamp();
		pqsignal(SIGTERM, die);
		pqsignal(SIGHUP, SignalHandlerForConfigReload);
		BackgroundWorkerUnblockSignals();
		if (!cluster_shared_config_process_observe(&actual))
			ereport(ERROR, (errmsg("native future child has no inherited configuration")));
		snprintf(result, sizeof(result), "%d:%llu\n", MyProcPid,
				 (unsigned long long)actual.ref.identity.generation);
		future_file("ready", result);
		snprintf(path, sizeof(path), "%s/test_config.future_go", DataDir);
		while (access(path, F_OK) != 0) {
			CHECK_FOR_INTERRUPTS();
			if (TimestampDifferenceExceeds(start, GetCurrentTimestamp(), 30000))
				ereport(ERROR, (errmsg("native future fixture was not released")));
			pg_usleep(10000L);
		}
		/* Real native entry after the real fork, never a role/PGPROC substitute.
		 * Do not manually reload: the production pre-use consumer must do it. */
		BackgroundWorkerInitializeConnection("postgres", NULL, 0);
		if (!cluster_shared_config_process_observe(&actual))
			ereport(ERROR, (errmsg("native future child lost configuration")));
		snprintf(result, sizeof(result), "%llu:%s\n",
				 (unsigned long long)actual.ref.identity.generation,
				 GetConfigOption("cluster.read_scache", false, false));
		future_file("result", result);
	}
#endif
	proc_exit(0);
}

Datum
test_pgrac_config_gate_state(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("native cut inspection requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		ClusterConfigUseGate *gate = cluster_shared_config_delivery_native_gate();
		ClusterConfigUseGateState state;
		if (gate == NULL)
			PG_RETURN_NULL();
		state = cluster_config_use_gate_read(gate);
		PG_RETURN_TEXT_P(
			cstring_to_text(psprintf("%d:%u:%u", state.closed, state.epoch, state.owners)));
	}
#else
	PG_RETURN_NULL();
#endif
}

/* Test-only local controller in a disposable native family. This neither
 * supplies nor simulates distributed membership, service or CF agreement.
 * Targets come from this actual native process, not arbitrary SQL bytes. */
Datum
test_pgrac_config_background(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR, (errmsg("native background cut requires superuser")));
#ifdef USE_PGRAC_CLUSTER
	{
		int kind = PG_GETARG_INT32(0);
		char *action = text_to_cstring(PG_GETARG_TEXT_PP(1));
		uint32 cookie = (uint32)PG_GETARG_INT32(2);
		ClusterConfigUseGate *gate;
		ClusterConfigUseGateState state;
		ClusterConfigUseTarget *target;
		ClusterSharedConfigProcess actual;
		ClusterSharedConfigActive common;
		BackendType roles[] = { B_CHECKPOINTER, B_BG_WRITER, B_WAL_WRITER };
		bool failed, ok = false;
		gate = cluster_shared_config_delivery_background_gate(kind, &failed);
		target = cluster_shared_config_delivery_background_target(kind);
		if (gate == NULL || target == NULL)
			ereport(ERROR, (errmsg("native background cut is unavailable")));
		if (strcmp(action, "state") == 0)
			ok = true;
		else if (strcmp(action, "close") == 0)
			ok = cluster_config_use_gate_close(gate, &cookie);
		else if (strcmp(action, "open") == 0)
			ok = !failed && cluster_config_use_gate_open(gate, cookie);
		else if (strcmp(action, "bind") == 0)
			ok = !failed && cluster_shared_config_process_observe(&actual) && !actual.failed
				 && cluster_shared_config_common_profile(&common)
				 && cluster_config_use_target_bind(gate, target, cookie, actual.node_id,
												   &actual.ref, &common);
		else
			ereport(ERROR, (errmsg("unknown native background cut action")));
		state = cluster_config_use_gate_read(gate);
		for (uint32 i = 0; i < ProcGlobal->allProcCount; i++) {
			PGPROC *proc = &ProcGlobal->allProcs[i];
			ClusterSharedConfigRegistration registration;
			if (cluster_shared_config_registration_read(&proc->cluster_config, &registration)
				&& registration.pid > 0 && registration.pid == proc->pid
				&& registration.role == roles[kind])
				SetLatch(&proc->procLatch);
		}
		PG_RETURN_TEXT_P(cstring_to_text(
			psprintf("%d:%d:%u:%u:%d", ok, state.closed, state.epoch, state.owners, failed)));
	}
#else
	PG_RETURN_NULL();
#endif
}
