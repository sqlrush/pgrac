#define SURVIVOR_POLL_REAL_INPUTS
#include "test_cluster_survivor_producer_fixture.h"
static ClusterSemanticAdmissionObservationResult
inputs_survivor_poll(ClusterSurvivorProducerObservation *out)
{
 ClusterSurvivorProducerScope scope={owner_epoch,owner_epoch,19};
 return cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS,scope,out);
}
UT_TEST(survivor_input_poll_retains_cf_and_128_walr_at_original_returns)
{
 for(unsigned role=0;role<2;++role) {
  uint16 threads[128];ClusterWalReadPinV1 *pin=NULL;ClusterWalInputsJobV1 *job;
  ClusterSurvivorProducerObservation out;unsigned calls,wakes,refs,waits;
  void (*tick)(void)=role?bgwriter_tick:checkpointer_tick;
  unsigned consumer=role?WAL_INPUT_JOB_PI_REBUILD:WAL_INPUT_JOB_RETAINED_CUT;
  input_observation_begin(role!=0);job=foundation_open((ClusterWalInputsConsumerV1)consumer);
  for(unsigned i=0;i<128;++i)threads[i]=(uint16)(i+1);
  UT_ASSERT_EQ(cluster_wal_read_pin_acquire_job_v1(threads,128,job,&pin),CLUSTER_WAL_PIN_OK);
  g_seven_result=CLUSTER_LOCK_ACQUIRE_OK_GRANTED;UT_ASSERT(cluster_cf_lock(ShareLock));
  UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock,NULL,&job->cf),CF_JOB_PENDING);
  job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,job_resource_argument);
  tick();calls=pin_native_calls;wakes=job_wakes;refs=pin_native_refs;waits=owner_wait_calls;
  UT_ASSERT_EQ(inputs_survivor_poll(&out),CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
  UT_ASSERT_EQ(out.owner,CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS);
  UT_ASSERT_EQ(out.sample.inputs.jobs[consumer].state,WAL_INPUT_JOB_STATE_RETIRING_CF);
  UT_ASSERT_EQ(out.sample.inputs.backend_id,InvalidBackendId);
  UT_ASSERT_EQ(pin_native_calls,calls);UT_ASSERT_EQ(pin_native_refs,refs);UT_ASSERT_EQ(job_wakes,wakes);
  job_ack(&job->cf);tick();
  for(unsigned left=128;left>0;--left) {
   tick(); /* Original next-member request precedes its exact ACK. */
   calls=pin_native_calls;wakes=job_wakes;refs=pin_native_refs;
   UT_ASSERT_EQ(inputs_survivor_poll(&out),CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
   UT_ASSERT_EQ(out.sample.inputs.jobs[consumer].state,WAL_INPUT_JOB_STATE_RETIRING_WALR);
   UT_ASSERT_EQ(out.sample.inputs.proc_generation,MyProc->cluster_grd_generation);
   UT_ASSERT_EQ(out.sample.inputs.jobs[consumer].pin_generation,pin->generation);
   UT_ASSERT_EQ(pin_native_calls,calls);UT_ASSERT_EQ(pin_native_refs,refs);UT_ASSERT_EQ(job_wakes,wakes);
   pin_ack_member(&pin->locks[left-1].request,false);tick();
  }
  UT_ASSERT_EQ(owner_wait_calls,waits);
  UT_ASSERT_EQ(inputs_survivor_poll(&out),CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
  UT_ASSERT_EQ(out.sample.inputs.jobs[consumer].state,WAL_INPUT_JOB_STATE_DONE);
  UT_ASSERT(job->pin==NULL && active_read_pin==NULL && pin_native_refs==0);
  pin=NULL;foundation_finish(&job);
  UT_ASSERT_EQ(inputs_survivor_poll(&out),CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
  UT_ASSERT(out.sample.inputs.known && out.sample.inputs.present==0);
  foundation_end();
 }
}
UT_TEST(survivor_input_poll_refuses_wrong_family_and_preserves_unknown)
{
 ClusterSurvivorProducerObservation out;
 for(unsigned role=0;role<2;++role) {
  ClusterSurvivorProducerScope scope;
  input_observation_begin(role!=0);scope=(ClusterSurvivorProducerScope){owner_epoch,owner_epoch,19};
  for(unsigned family=0;family<5;++family) {
   if(family==CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS)continue;
   memset(&out,0xa5,sizeof(out));
   UT_ASSERT_EQ(cluster_semantic_survivor_producer_poll((ClusterSurvivorProducerOwner)family,scope,&out),CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
   survivor_poll_unknown(&out);
  }
  for(unsigned fault=0;fault<7;++fault) {
   ClusterSurvivorProducerScope bad=scope;
   if(fault==0)bad.native_epoch=0;if(fault==1)bad.native_epoch=UINT64_MAX;
   if(fault==2)bad.gate_epoch=0;if(fault==3)bad.gate_epoch=owner_epoch+1;
   if(fault==4)bad.record_generation=0;if(fault==5)bad.record_generation=UINT64_MAX;
   UT_ASSERT_EQ(cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS,bad,fault==6?NULL:&out),CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
   if(fault!=6)survivor_poll_unknown(&out);
  }
  foundation_end();
 }
}
static unsigned inputs_survivor_fault;
static void
inputs_survivor_drift(void)
{
 survivor_poll_barrier_hook=NULL;
 switch(inputs_survivor_fault) {
 case 0:owner_epoch++;break;case 1:MyProcPid++;break;
 case 2:MyProc->cluster_grd_generation++;break;
 case 3:MyBackendType=B_BG_WRITER;MyAuxProcType=BgWriterProcess;break;
 case 4:job_observation_lock_held=true;break;
 case 5:CurrentResourceOwner=AuxProcessResourceOwner=(ResourceOwner)&job_fixture_foreign_owner;break;
 case 6:proc_exit_inprogress=true;break;
 }
}
UT_TEST(survivor_input_poll_rechecks_actual_original_owner_at_final_return)
{
 ClusterSurvivorProducerObservation out;
 input_observation_begin(false);
 for(unsigned fault=0;fault<7;++fault) {
  PGPROC proc=*MyProc;uint64 epoch=owner_epoch;int pid=MyProcPid;
  ResourceOwner resource=CurrentResourceOwner;
  inputs_survivor_fault=fault;survivor_poll_barrier_hook=inputs_survivor_drift;
  UT_ASSERT_EQ(inputs_survivor_poll(&out),fault==0?CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE:CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
  survivor_poll_unknown(&out);UT_ASSERT(survivor_poll_barrier_hook==NULL);
  *MyProc=proc;owner_epoch=epoch;MyProcPid=pid;job_observation_lock_held=false;
  MyBackendType=B_CHECKPOINTER;MyAuxProcType=CheckpointerProcess;
  CurrentResourceOwner=AuxProcessResourceOwner=resource;proc_exit_inprogress=false;
 }
 foundation_end();
}
UT_TEST(survivor_input_poll_keeps_original_invalid_and_stale_outcomes)
{
 ClusterSurvivorProducerObservation out;ClusterWalInputsJobSlotV1 saved;
 ClusterSurvivorProducerScope scope;
 input_observation_begin(false);scope=(ClusterSurvivorProducerScope){owner_epoch+1,owner_epoch,19};
 UT_ASSERT_EQ(cluster_semantic_survivor_producer_poll(CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS,scope,&out),CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE);
 survivor_poll_unknown(&out);saved=inputs_jobs[3];inputs_jobs[3].reserved=1;
 UT_ASSERT_EQ(inputs_survivor_poll(&out),CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
 survivor_poll_unknown(&out);inputs_jobs[3]=saved;
 foundation_end();
}
