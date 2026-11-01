/* Each fixture executes its complete actual owner branch. Unrelated owner
 * branches abort if reached; they never supply successful observations.
 * The barrier hook controls host scheduling only, not completion or receipts.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_semantic_survivor_producer.h"
#include "storage/ipc.h"
#include "utils/resowner.h"
/* Original process globals at these host boundaries. Other families abort
 * above before their leaf is reachable; no foreign owner sample is supplied. */
#ifndef SURVIVOR_POLL_REAL_BACKEND
BackendId MyBackendId = InvalidBackendId;
#endif
#ifndef SURVIVOR_POLL_REAL_INPUTS
ResourceOwner AuxProcessResourceOwner;
bool proc_exit_inprogress;
#ifndef SURVIVOR_POLL_REAL_BACKEND
ResourceOwner CurrentResourceOwner;
PROC_HDR *ProcGlobal;
#endif
#endif

#ifndef SURVIVOR_POLL_REAL_BACKEND
ClusterSemanticAdmissionObservationResult
cluster_service_backend_collect_return(uint64 epoch, uint64 gate, uint64 generation,
 ClusterServiceBackendCollection *out)
{ (void)epoch; (void)gate; (void)generation; (void)out; abort(); }
#endif
#ifndef SURVIVOR_POLL_REAL_NATIVE
ClusterSemanticAdmissionObservationResult
cluster_service_producer_collect_return(uint64 epoch, uint64 gate, uint64 generation,
 ClusterServiceProducerCollection *out)
{ (void)epoch; (void)gate; (void)generation; (void)out; abort(); }
#endif
#ifndef SURVIVOR_POLL_REAL_INPUTS
ClusterWalInputsJobsObservationResultV1
cluster_wal_inputs_jobs_service_observe_v1(uint64 epoch, ClusterWalInputsJobsObservationV1 *out)
{ (void)epoch; (void)out; abort(); }
#endif
static void (*survivor_poll_barrier_hook)(void);
static void
survivor_poll_read_barrier(void)
{
 pg_read_barrier();
 if (survivor_poll_barrier_hook != NULL) survivor_poll_barrier_hook();
}
#pragma push_macro("pg_read_barrier")
#undef pg_read_barrier
#define pg_read_barrier() survivor_poll_read_barrier()
#pragma push_macro("proc")
#undef proc
#include "../../backend/cluster/cluster_semantic_survivor_producer.c"
#pragma pop_macro("proc")
#pragma pop_macro("pg_read_barrier")
static void
survivor_poll_unknown(const ClusterSurvivorProducerObservation *out)
{
 ClusterSurvivorProducerObservation value=*out,zero={0};
 value.reason=NULL;UT_ASSERT(memcmp(&value,&zero,sizeof(value))==0);
 UT_ASSERT(out->reason!=NULL);
}
