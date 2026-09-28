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
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/wait_event.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_config_use_gate.h"
#include "cluster/cluster_shared_config.h"
#endif

PG_FUNCTION_INFO_V1(test_pgrac_config_gate_state);
void test_pgrac_config_gate_init(void);

#ifdef USE_PGRAC_CLUSTER
static int request;
static uint32 cut;

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
#endif
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
