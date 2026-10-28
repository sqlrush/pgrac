/* Actual native H sampler composed with the original LMON/cohort bodies.
 * ProcArray storage, clock and lock scheduling remain controlled leaves. */
#include "access/twophase.h"
#include "storage/proc.h"
#include "test_cluster_pgproc_active_types.inc"

static bool native_history_enabled, native_history_locked;
static unsigned native_history_cuts, native_history_unlocks, native_history_clocks;
static unsigned native_history_promise_reads, native_history_promise_unlocks;
static bool native_history_promise_lock;
static SCN native_history_clock;
static PGPROC native_history_procs[4];
static PGPROC *allProcs = native_history_procs;
static ProcArrayStruct *procArray;
bool native_history_sample(ClusterUndoRetentionSample *out);
SCN native_history_scalar(void);
bool native_history_covers(SCN);

SCN
cluster_scn_current(void)
{
	UT_ASSERT(native_history_enabled && native_history_locked && !native_xid_locked);
	UT_ASSERT_EQ(native_history_clocks + 1, native_history_cuts);
	native_history_clocks++;
	return native_history_clock;
}

/* Native recovery is complete in this original LMON composition. */
static bool
native_history_in_recovery(void)
{
	return false;
}

#define RecoveryInProgress native_history_in_recovery

/* Retain the pre-existing controlled sampler for its original tests. Only
 * the explicit composition cases select these unmodified native bodies. */
#define cluster_undo_retention_horizon_sample native_history_sample
#define cluster_undo_retention_horizon native_history_scalar
#include "test_cluster_retention_sample.inc"
#undef cluster_undo_retention_horizon
#undef cluster_undo_retention_horizon_sample
#define cluster_undo_retention_horizon_covers native_history_covers
#include "test_cluster_retention_promise.inc"
#undef cluster_undo_retention_horizon_covers
#undef RecoveryInProgress

static void
native_history_reset(void)
{
	if (procArray == NULL)
		procArray = calloc(1, sizeof(*procArray) + sizeof(int) * lengthof(native_history_procs));
	UT_ASSERT(procArray != NULL);
	memset(procArray, 0, sizeof(*procArray) + sizeof(int) * lengthof(native_history_procs));
	memset(native_history_procs, 0, sizeof(native_history_procs));
	procArray->maxProcs = lengthof(native_history_procs);
	procArray->numProcs = lengthof(native_history_procs);
	for (unsigned i = 0; i < lengthof(native_history_procs); i++) {
		procArray->pgprocnos[i] = i;
		native_history_procs[i].pid = 100 + i;
		pg_atomic_init_u64(&native_history_procs[i].cluster_read_scn_atomic, InvalidScn);
	}
	native_history_clock = scn_encode(0, 700);
	native_history_cuts = native_history_unlocks = native_history_clocks = 0;
	native_history_locked = native_history_promise_lock = false;
	native_history_promise_reads = native_history_promise_unlocks = 0;
	native_history_enabled = true;
}
