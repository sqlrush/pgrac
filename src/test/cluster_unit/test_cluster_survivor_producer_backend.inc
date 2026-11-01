#define SURVIVOR_POLL_REAL_BACKEND
AuxProcType MyAuxProcType = NotAnAuxProcess;
#include "test_cluster_survivor_producer_fixture.h"
UT_TEST(survivor_backend_poll_keeps_actual_snapshot_and_original_gate)
{
 ClusterSurvivorProducerObservation out;ClusterSurvivorProducerScope scope={8,7,19};Snapshot held;
 backend_return_begin();snapshot_return_epoch=8;held=registered(80);
 pg_atomic_write_u32(&backend_return_gate.inflight[1][63],3);
 UT_ASSERT_EQ(cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_BACKEND,scope,&out),CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
 UT_ASSERT_EQ(out.owner,CLUSTER_SURVIVOR_PRODUCER_BACKEND);
 UT_ASSERT(out.sample.backend.snapshots.known && out.sample.backend.admission.known);
 UT_ASSERT_EQ(out.sample.backend.snapshots.references,CLUSTER_SERVICE_SNAPSHOT_REGISTERED);
 UT_ASSERT_EQ(out.sample.backend.snapshots.identity.epoch,8);
 UT_ASSERT_EQ(out.sample.backend.admission.formation_epoch,7);
 UT_ASSERT_EQ(out.sample.backend.admission.inflight[1][63],3);
 UT_ASSERT_EQ(held->regd_count,1);UT_ASSERT_EQ(remembered,1);
 UnregisterSnapshot(held);pg_atomic_write_u32(&backend_return_gate.inflight[1][63],0);
 UT_ASSERT_EQ(cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_BACKEND,scope,&out),CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
 UT_ASSERT_EQ(out.sample.backend.snapshots.references,0);
 for(unsigned owner=0;owner<5;++owner) {
  if(owner==CLUSTER_SURVIVOR_PRODUCER_BACKEND)continue;
  UT_ASSERT_EQ(cluster_semantic_survivor_producer_poll((ClusterSurvivorProducerOwner)owner,scope,&out),CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
  survivor_poll_unknown(&out);
 }
}
static unsigned survivor_backend_fault;
static void survivor_backend_drift(void) { survivor_poll_barrier_hook=NULL;if(survivor_backend_fault==0)proc.cluster_grd_generation++;else MyBackendId++; }
UT_TEST(survivor_backend_poll_discards_final_owner_drift_and_leaf_stale)
{
 ClusterSurvivorProducerObservation out;ClusterSurvivorProducerScope scope={7,7,19};uint64 generation;
 backend_return_begin();generation=proc.cluster_grd_generation;
 for(unsigned fault=0;fault<2;++fault) {
  survivor_backend_fault=fault;survivor_poll_barrier_hook=survivor_backend_drift;
  UT_ASSERT_EQ(cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_BACKEND,scope,&out),CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
  survivor_poll_unknown(&out);proc.cluster_grd_generation=generation;MyBackendId=proc.backendId;
 }
 pg_atomic_write_u64(&backend_return_gate.record_generation,20);
 UT_ASSERT_EQ(cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_BACKEND,scope,&out),CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE);
 survivor_poll_unknown(&out);backend_return_begin();
}
