/*-------------------------------------------------------------------------
 *
 * cluster_shared_config_use.c
 *    Bind a held local producer cut to native backend resource lifetimes.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_shared_config_use.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/wait_event.h"
#include "cluster/cluster_config_use_gate.h"
#include "cluster/cluster_semantic_activation.h"

static bool use_held;
static bool use_transaction;

bool
cluster_shared_config_use_session_owned(void)
{
	/* Native temporary-namespace ownership survives COMMIT. Its original
	 * before_shmem_exit callback starts a cleanup transaction with interrupts
	 * disabled, so it must retain its counted continuation until ProcKill.
	 * Never infer this obligation from a filename or desired configuration. */
	return LockHasSessionLocks() || (MyProc != NULL && OidIsValid(MyProc->tempNamespaceId));
}

static bool
use_is_native_producer(void)
{
	/* Cluster services, CF, WALR and native protocol retirement must stay
	 * runnable. Their own original owners need a separate service cut;
	 * a zero native count is NOT a distributed old-work closure proof. */
	return IsUnderPostmaster && MyProc != NULL
		   && (MyBackendType == B_BACKEND || MyBackendType == B_BG_WORKER
			   || MyBackendType == B_AUTOVAC_WORKER || MyBackendType == B_AUTOVAC_LAUNCHER
			   || MyBackendType == B_WAL_SENDER);
}

void
cluster_shared_config_use_exit(void)
{
	ClusterConfigUseGate *gate;
	if (!use_held)
		return;
	gate = cluster_shared_config_delivery_native_gate();
	if (gate == NULL || MyProc == NULL)
		elog(PANIC, "native configuration owner lost its family");
	/* No successor can claim this leader after the count reaches zero.
	 * ProcKill calls here AFTER real resource/lock cleanup, before reuse. */
	pg_atomic_write_u32(&MyProc->cluster_config_use_epoch, 0);
	if (!cluster_config_use_gate_leave(gate))
		elog(PANIC, "native configuration owner count is inconsistent");
	use_held = false;
	use_transaction = false;
}

void
cluster_shared_config_use_idle(void)
{
	if (use_held && !use_transaction && !cluster_shared_config_delivery_work_pending()
		&& !cluster_shared_config_use_session_owned()
		&& !cluster_semantic_activation_backend_has_admission())
		cluster_shared_config_use_exit();
}

void
cluster_shared_config_use_enter(void)
{
	ClusterConfigUseGate *gate;
	if (!use_is_native_producer() || use_held
		|| (gate = cluster_shared_config_delivery_native_gate()) == NULL)
		return;
	for (;;) {
		PGPROC *leader = MyProc->lockGroupLeader;
		bool continuation = leader != NULL && leader != MyProc;
		uint32 epoch;
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
		if (cluster_config_use_gate_enter(gate, continuation, &epoch)) {
			bool valid = true;
			/* Record provisional ownership BEFORE any interruptible operation.
			 * ERROR/exit can then retire it through the same original cleanup. */
			use_held = true;
			pg_atomic_write_u32(&MyProc->cluster_config_use_epoch, epoch);
			if (continuation) {
				LWLock *lock = LockHashPartitionLockByProc(leader);
				LWLockAcquire(lock, LW_SHARED);
				valid = MyProc->lockGroupLeader == leader && leader->lockGroupLeader == leader
						&& leader->pid > 0
						&& pg_atomic_read_u32(&leader->cluster_config_use_epoch) == epoch;
				LWLockRelease(lock);
			}
			if (valid)
				return;
			cluster_shared_config_use_exit();
		}
		/* Service reload only while actually waiting. An open fast path must
		 * not insert a new native file reload after the existing transaction
		 * boundary consumed its selected image (notably AND CHAIN). */
		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		/* The interval schedules another observation, not an error deadline.
		 * Caller cancellation and native postmaster death still terminate. */
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 100L,
						WAIT_EVENT_RECONFIG_SHARED_CONFIG_WAIT);
	}
}

void
cluster_shared_config_use_xact_start(void)
{
	/* A previous command may have released the last session lock. Retire
	 * only a genuinely ended owner before trying independent new work. */
	cluster_shared_config_use_idle();
	cluster_shared_config_use_enter();
	if (use_held)
		use_transaction = true;
}

void
cluster_shared_config_use_xact_end(void)
{
	/* Only native COMMIT/PREPARE/CleanupTransaction call here, AFTER their
	 * real resource teardown. Abort's early callbacks are not retirement. */
	use_transaction = false;
	cluster_shared_config_use_idle();
}
