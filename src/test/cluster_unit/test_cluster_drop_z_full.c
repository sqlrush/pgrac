/* Original KO + WB + LOCAL/AM03 PCM + provider + LMON census.
 * Kernel, transport and WAL inputs are explicit controlled boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
#define Z_REAL_STOP_GATE 1
#define Z_WB_NO_MAIN 1
#include "test_cluster_drop_z_wb.c"
#include "cluster/cluster_service_observe.h"
void z_pcm_bridge_setup(RelFileLocator,RelFileLocator,uint64);
bool z_pcm_bridge_ok(void);
ClusterPcmPiRelationScanV2 z_pcm_bridge_scan(RelFileLocator,uint64,uint32,uint32 *);
uint32 z_pcm_bridge_retire(void);
ClusterNormalStopPollResult z_pcm_bridge_census(const char **);
ClusterNormalStopPollResult cluster_service_normal_stop_observe(ClusterServiceObservation *o)
{
 memset(o,0,sizeof(*o));o->domain="original-pcm";
 return z_pcm_bridge_census(&o->reason);
}
ClusterNormalStopPollResult cluster_pi_data_normal_stop_poll_v1(const char **r) { *r="empty fixture";return CLUSTER_NORMAL_STOP_READY; }
ClusterNormalStopPollResult cluster_wal_cut_normal_stop_poll_v1(const char **r) { *r="empty fixture";return CLUSTER_NORMAL_STOP_READY; }
static ClusterLeaveState full_leave;
static ClusterNormalStopState full_stop;
static ClusterLeaveState *cl_state=&full_leave;
static ClusterNormalStopState *cl_normal_stop=&full_stop;
#include "test_cluster_drop_stop_gate.inc"

static bool full_setup(void)
{
 memset(&full_leave,0,sizeof(full_leave));
 memset(&full_stop,0,sizeof(full_stop));
 pg_atomic_init_u32(&full_stop.requested,0);
 pg_atomic_init_u32(&full_stop.failure_reason,CLUSTER_NORMAL_STOP_FAILURE_NONE);
 pg_atomic_init_u32(&full_stop.identity_published,0);
 pg_atomic_init_u32(&full_stop.phase,CLUSTER_NORMAL_STOP_IDLE);
 pg_atomic_init_u32(&full_stop.service_seal,0);
 local_relation_scan_boundary=NULL;
 if(!z_setup(true)) return false;
 IsUnderPostmaster=true;
 full_leave.barrier_deadline_us=(uint64)GetCurrentTimestamp()+UINT64_C(30000000);
 pg_atomic_write_u32(&full_stop.identity_published,1);
 pg_atomic_write_u32(&full_stop.phase,CLUSTER_NORMAL_STOP_DRAIN);
 pg_atomic_write_u32(&full_stop.requested,1);
 return true;
}
#include "test_cluster_drop_lmon_observe.inc"
UT_TEST(test_two_original_objects_reach_am03_and_lmon_idle_after_stop)
{
 RelFileLocator a,b;
 ResourceXGateSnapshot gate;
 if(!full_setup()) return;
 a=space_identity.key.locator;b=a;a.relNumber=z_objects[0];b.relNumber=z_objects[1];
 UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
 UT_ASSERT(cluster_ko_directory_drop_retired_v3(a));
 UT_ASSERT(cluster_ko_directory_drop_retired_v3(b));
 z_pcm_bridge_setup(a,b,gate.formation);
 UT_ASSERT(z_pcm_bridge_ok());
 local_relation_scan_boundary=z_pcm_bridge_scan;
 UT_ASSERT(cluster_normal_stop_requested());
 UT_ASSERT(cluster_normal_stop_continuation_allowed());
 UT_ASSERT_EQ(lmon_normal_stop_observe(false),CLUSTER_NORMAL_STOP_PENDING);
 for(unsigned i=0;i<64 && main_syncs<2;i++) (void)cluster_ko_shared_native_poll_v2();
 UT_ASSERT_EQ(main_syncs,2);
 UT_ASSERT_EQ(dir_syncs,2);
 UT_ASSERT_EQ(external_fds,0);
 UT_ASSERT_EQ(lmon_normal_stop_observe(false),CLUSTER_NORMAL_STOP_PENDING);
 UT_ASSERT_EQ(z_pcm_bridge_retire(),2);
 UT_ASSERT(z_pcm_bridge_ok());
 z_release_ax();
 UT_ASSERT_EQ(lmon_normal_stop_observe(false),CLUSTER_NORMAL_STOP_PENDING);
 MyBackendType=B_BG_WRITER;
 cluster_pi_writeback_bgwriter_quiesced_v1();
 MyBackendType=B_LMON;
 UT_ASSERT_EQ(lmon_normal_stop_observe(true),CLUSTER_NORMAL_STOP_READY);
 local_relation_scan_boundary=NULL;
}
UT_TEST(test_real_stop_cut_refuses_execution_but_keeps_passive_proof)
{
 for(unsigned mutation=0;mutation<3;mutation++) {
  uint64 revision;
  if(!full_setup()) return;
  if(mutation==0) pg_atomic_write_u32(&full_stop.service_seal,1);
  if(mutation==1) full_leave.barrier_deadline_us=(uint64)GetCurrentTimestamp();
  if(mutation==2) cluster_normal_stop_fail(CLUSTER_NORMAL_STOP_FAILURE_SERVICE);
  UT_ASSERT(!cluster_normal_stop_continuation_allowed());
  UT_ASSERT(cluster_pi_writeback_local_relation_closed_v3(space_identity.key.locator,current_epoch,&revision));
  for(unsigned i=0;i<4;i++) UT_ASSERT(!cluster_ko_shared_native_poll_v2());
  assert_no_drop_io();
 }
}

int main(void)
{
 setvbuf(stdout,NULL,_IOLBF,0);UT_PLAN(2);
 UT_RUN(test_real_stop_cut_refuses_execution_but_keeps_passive_proof);
 UT_RUN(test_two_original_objects_reach_am03_and_lmon_idle_after_stop);
 UT_DONE();return ut_failed_count != 0;
}
