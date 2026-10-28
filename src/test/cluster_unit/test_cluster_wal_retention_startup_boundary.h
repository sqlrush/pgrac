/* This geometry/guard suite has no Startup process. Any accidental entry
 * into the Startup-only CF wait must fail loudly. The actual CF producer
 * is exercised in test_cluster_startup_cf_entry.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_WAL_RETENTION_STARTUP_BOUNDARY_H
#define TEST_CLUSTER_WAL_RETENTION_STARTUP_BOUNDARY_H
#include "access/xlog.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_startup_phase.h"
#include "postmaster/startup.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"

AuxProcType MyAuxProcType = NotAnAuxProcess;
PGPROC *MyProc;
Latch *MyLatch;
LWLockPadded *MainLWLockArray;
volatile sig_atomic_t InterruptPending, ShutdownRequestPending;
bool InRecovery, cluster_enabled, cluster_controlfile_shared_authority;

/* Match the suite's existing heap-backed palloc0/pfree boundary. */
void *
palloc(Size size)
{
	return malloc(size);
}

uint64
GetSystemIdentifier(void)
{
	abort();
}
uint64
cluster_grd_redeclare_generation(void)
{
	abort();
}
uint64
cluster_qvotec_get_self_incarnation(void)
{
	abort();
}
void
HandleStartupProcInterrupts(void)
{
	abort();
}
void
ProcessInterrupts(void)
{
	abort();
}
bool
LWLockHeldByMe(LWLock *lock pg_attribute_unused())
{
	abort();
}
void
ResetLatch(Latch *latch pg_attribute_unused())
{
	abort();
}
int
WaitLatch(Latch *latch pg_attribute_unused(), int events pg_attribute_unused(),
		  long timeout pg_attribute_unused(), uint32 event pg_attribute_unused())
{
	abort();
}
uint64
cluster_cf_attempt_sequence(void)
{
	abort();
}
bool
cluster_cf_last_attempt_since(uint64 before pg_attribute_unused(),
							  ClusterCfAttemptObservation *out pg_attribute_unused())
{
	abort();
}
bool
cluster_cf_attempt_wait_slice(const ClusterCfAttemptObservation *attempt pg_attribute_unused())
{
	abort();
}
bool
cluster_cf_held(LOCKMODE mode pg_attribute_unused())
{
	abort();
}
bool
cluster_cf_release_completed(LOCKMODE mode pg_attribute_unused(),
							 uint64 cookie pg_attribute_unused())
{
	abort();
}
bool
cluster_cf_retirement_snapshot(LOCKMODE mode pg_attribute_unused(),
							   const void *caller pg_attribute_unused(),
							   ClusterCfRetirementSnapshot *out pg_attribute_unused())
{
	abort();
}
void
cluster_cf_retirement_poll(void)
{
	abort();
}
void
cluster_lock_owners_service_poll(void)
{
	abort();
}
bool
cluster_recovery_transport_components_current(void)
{
	abort();
}
uint64
cluster_epoch_get_current(void)
{
	abort();
}
#endif
