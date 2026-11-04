/* Real H producer bodies composed with the original WAL owner. Only the
 * ProcArray storage, clock, recovery and lock scheduler are test boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "access/twophase.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_mrp.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "test_cluster_pgproc_active_types.inc"

int cluster_node_id = 0;
static ProcArrayStruct *procArray;
static PGPROC history_procs[1];
static PGPROC *allProcs = history_procs;
static LWLockPadded history_locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = history_locks;
static bool history_lock_held;
static bool history_sample_at_grant;
static unsigned history_grant_observations;

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == ProcArrayLock && mode == LW_EXCLUSIVE && !history_lock_held);
	history_lock_held = true;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == ProcArrayLock && history_lock_held);
	history_lock_held = false;
}
bool RecoveryInProgress(void) { return false; }
SCN cluster_scn_current(void) { UT_ASSERT(history_lock_held); return 700; }
bool cluster_mrp_should_start(void) { UT_ASSERT(history_lock_held); return false; }
SCN cluster_mrp_standby_consistent_scn(void) { abort(); }

#include "test_cluster_heap_small_scn.inc"
#define cluster_undo_retention_sample_min k_history_min
#include "test_cluster_heap_freeze_retention_min.inc"
#include "test_cluster_retention_sample.inc"
#undef cluster_undo_retention_sample_min
#include "test_cluster_retention_history.inc"

static void
history_new_boot(void)
{
	if (procArray != NULL)
		UT_ASSERT_EQ(pg_atomic_read_u32(&procArray->cluster_unknown_history_sources), 0);
	free(procArray);
	procArray = calloc(1, sizeof(ProcArrayStruct) + sizeof(int));
	if (procArray == NULL) abort();
	procArray->maxProcs = 1;
	pg_atomic_init_u32(&procArray->cluster_unknown_history_sources, 0);
	memset(history_procs, 0, sizeof(history_procs));
	pg_atomic_init_u64(&history_procs[0].cluster_read_scn_atomic, InvalidScn);
	history_sample_at_grant = history_lock_held = false;
	history_grant_observations = 0;
}

static void
history_observe_first_grant(void)
{
	if (history_sample_at_grant)
	{
		ClusterUndoRetentionSample cut;

		history_grant_observations++;
		UT_ASSERT(!cluster_undo_retention_horizon_sample(&cut));
		UT_ASSERT_EQ(cut.horizon_scn, InvalidScn);
		UT_ASSERT_EQ(procArray->cluster_read_scn_floor, InvalidScn);
	}
}
