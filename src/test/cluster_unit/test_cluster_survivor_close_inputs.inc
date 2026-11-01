/* Original auxiliary job and CF ACK loop through a real close query.
 * Process tables, latch delivery and event lock ownership are host cuts;
 * no positive native closure or resource-return fact is manufactured. */
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_semantic_survivor_close.h"
static ClusterReconfigState input_close_reconfig;
static ClusterReconfigState *ReconfigShmem=&input_close_reconfig;
void cluster_sinval_reset_all_on_reconfig(void) {}
void cluster_write_fence_authority_cache_invalidate(void) {}
static bool input_close_event_locked;
static bool input_close_acquire(LWLock *l,LWLockMode mode)
{
 UT_ASSERT_EQ(l,&ReconfigShmem->lock);UT_ASSERT(mode==LW_SHARED||mode==LW_EXCLUSIVE);
 UT_ASSERT(!input_close_event_locked);input_close_event_locked=true;return true;
}
static void input_close_release(LWLock *l)
{UT_ASSERT_EQ(l,&ReconfigShmem->lock);UT_ASSERT(input_close_event_locked);input_close_event_locked=false;}
#define LWLockAcquire input_close_acquire
#define LWLockRelease input_close_release
#include "test_cluster_survivor_close_event.inc"
#undef LWLockRelease
#undef LWLockAcquire
#pragma push_macro("proc")
#undef proc
#include "../../backend/cluster/cluster_semantic_survivor_close.c"
#pragma pop_macro("proc")
UT_TEST(close_query_auxiliary_keeps_cf_ack_progress_and_zero_ordinary_coverage)
{
 for(unsigned role=0;role<2;role++) {
  ClusterSurvivorLocalReply r;ClusterSurvivorLocalTarget target;ReconfigEvent e={0};
  void (*tick)(void)=role?bgwriter_tick:checkpointer_tick;
  input_route_bgwriter=role!=0;input_route_aux();input_observation_begin(role!=0);
  memset(&survivor_local_owner_storage,0,sizeof(survivor_local_owner_storage));
  memset(&input_close_reconfig,0,sizeof(input_close_reconfig));
  memset(&input_route_gate,0,sizeof(input_route_gate));SemanticActivationShmem=&input_route_gate;
  pg_atomic_init_u64(&input_route_gate.admission_seq,2);
  pg_atomic_init_u64(&input_route_gate.active_bits,16);
  pg_atomic_init_u64(&input_route_gate.record_generation,19);
  pg_atomic_init_u64(&input_route_gate.formation_epoch,owner_epoch);
  pg_atomic_init_u64(&input_route_grd.next_generation,32);
  memset(input_route_status,0,sizeof(input_route_status));
  input_route_status[MaxBackends+MyAuxProcType].st_changecount=2;
  input_route_status[MaxBackends+MyAuxProcType].st_procpid=MyProcPid;
  input_route_status[MaxBackends+MyAuxProcType].st_backendType=MyBackendType;
  memset(&job_procs[41],0,sizeof(job_procs[41]));
  job_procs[41].pid=4101;job_procs[41].pgprocno=41;job_procs[41].backendId=-1;
  job_procs[41].cluster_grd_generation=17;input_route_queries=input_route_replies=0;
  input_route_lmon();e.event_id=19;e.old_epoch=owner_epoch-1;e.new_epoch=owner_epoch;
  e.reconfig_kind=RECONFIG_KIND_FAIL_STOP;e.observer_role=CLUSTER_RECONFIG_OBSERVER_SURVIVOR;
  e.dead_bitmap[0]=2;cluster_reconfig_publish_event(&e);
  UT_ASSERT_EQ(cluster_semantic_survivor_close_begin(),CLOSE_PENDING);
  input_route_aux();ClusterWalInputsJobV1 *job=foundation_open(role?WAL_INPUT_JOB_PI_REBUILD:WAL_INPUT_JOB_RETAINED_CUT);
  g_seven_result=CLUSTER_LOCK_ACQUIRE_OK_GRANTED;UT_ASSERT(cluster_cf_lock(ShareLock));
  UT_ASSERT_EQ(cluster_cf_unlock_job_v1(ShareLock,NULL,&job->cf),CF_JOB_PENDING);
  job_resource_callback(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,job_resource_argument);
  for(unsigned after_ack=0;after_ack<2;after_ack++) {
   input_route_lmon();UT_ASSERT(targets_read(42,&target));
   UT_ASSERT_EQ(cluster_semantic_survivor_local_close_begin(&target),LOCAL_PENDING);
   input_route_aux();tick();input_route_lmon();
   UT_ASSERT_EQ(cluster_semantic_survivor_local_close_poll(&r),LOCAL_PENDING);
   UT_ASSERT_EQ(r.payload_bytes,1256);UT_ASSERT_EQ(r.family,CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS);
   UT_ASSERT_EQ(survivor_local_scalar(r.body+1136,8),0);
   UT_ASSERT_EQ(survivor_local_scalar(r.body+1208,4),0);
   UT_ASSERT_EQ(survivor_local_owner_storage.header.native_closure,0);
   UT_ASSERT_EQ(survivor_local_owner_storage.header.ordinary_admission,0);
   UT_ASSERT_EQ(pg_atomic_read_u32(&input_route_gate.transition_closed),0);
   input_route_aux();
   UT_ASSERT_EQ(job->state,after_ack?WAL_INPUT_JOB_STATE_DONE:WAL_INPUT_JOB_STATE_RETIRING_CF);
   if(!after_ack)job_ack(&job->cf);
  }
  UT_ASSERT_EQ(input_route_queries,2);UT_ASSERT_EQ(input_route_replies,2);
  foundation_finish(&job);input_route_end();
 }
}
