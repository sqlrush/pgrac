/* Actual candidate, gate, identity, generation and target reader bodies.
 * Process roster, role scheduling, shared allocation and latch delivery are
 * host boundaries. No input syntax supplies authority or a closure receipt.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "port/pg_crc32c.h"
#include "cluster/cluster_grd.h"
#include "utils/backend_status.h"
#include "test_cluster_survivor_pgsa.inc"
#include "test_cluster_shared_admission_observe.inc"
#include "test_cluster_backend_return_gate.inc"
static ClusterSemanticActivationShmem input_route_gate;
static ClusterGrdShared input_route_grd;
static ClusterGrdShared *cluster_grd_state=&input_route_grd;
#include "test_cluster_survivor_generation.inc"
static PgBackendStatus input_route_status[100+NUM_AUXPROCTYPES];
static PgBackendStatus *BackendStatusArray=input_route_status;
#define NumBackendStatSlots (MaxBackends + NUM_AUXPROCTYPES)
#include "test_cluster_survivor_status_identity.inc"
#undef NumBackendStatSlots
#pragma push_macro("proc")
#undef proc
#include "../../backend/cluster/cluster_semantic_survivor.c"
#include "../../backend/cluster/cluster_semantic_survivor_candidate.c"
#include "../../backend/cluster/cluster_semantic_survivor_targets.c"
#pragma pop_macro("proc")

static unsigned input_route_queries,input_route_replies;
static void
input_route_set_latch(Latch *latch)
{
	/* Only the local mailbox's cross-process delivery is a host boundary.
	 * Original job/ACK SetLatch keeps its own identity check and accounting. */
	if (latch==&job_procs[42].procLatch) {
		UT_ASSERT(MyProc==&job_procs[41] && MyBackendType==B_LMON);
		input_route_queries++;
	} else {
		UT_ASSERT(latch==&job_procs[41].procLatch);
		UT_ASSERT(MyProc==&job_procs[42]);
		input_route_replies++;
	}
	latch->is_set=true;
}
int
SendProcSignal(pid_t pid,ProcSignalReason reason,BackendId id)
{
	(void)pid;(void)reason;(void)id;
	abort(); /* INPUT_JOBS uses its actual PGPROC latch, never a backend slot. */
}
