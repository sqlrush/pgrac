/* Isolated runtime boundary for the original PCM implementation.
 * Namespacing keeps its kernel fixtures private; the two KO authority queries
 * link to the actual KO in the controlled DROP chain, never a boolean stub.
 * Author: SqlRush <sqlrush@gmail.com> */
#define Z_PCM_ORIGINAL_KO 1
#define main z_pcm_fixture_main
#include "test_cluster_pcm_lock.c"
#undef main

void z_pcm_bridge_setup(RelFileLocator a, RelFileLocator b, uint64 formation);
bool z_pcm_bridge_ok(void);
ClusterPcmPiRelationScanV2 z_pcm_bridge_scan(RelFileLocator locator,uint64 formation,uint32 budget,uint32 *cursor);
uint32 z_pcm_bridge_retire(void);
ClusterNormalStopPollResult z_pcm_bridge_census(const char **reason);

void z_pcm_bridge_setup(RelFileLocator a, RelFileLocator b, uint64 formation)
{
 ut_current_failed=false;
 reset_fake_pcm_runtime(8);
 cluster_shared_config=true;
 cluster_node_id=0;
 UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(formation),RESOURCE_X_APPLY_APPLIED);
 for(unsigned i=0;i<2;i++) {
  RelFileLocator l=i ? b : a;
  BufferTag tag;
  InitBufferTag(&tag,&l,MAIN_FORKNUM,0);
  UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(tag,PCM_STATE_N,80,90,0,7));
  struct StopPcmEntryLayout *e=hash_search((HTAB *)&fake_pcm_htab_token,&tag,HASH_FIND,NULL);
  UT_ASSERT(e!=NULL);
  if(e!=NULL) pg_atomic_write_u32(&e->pi_holders_bitmap,0);
 }
}
ClusterPcmPiRelationScanV2 z_pcm_bridge_scan(RelFileLocator l,uint64 f,uint32 b,uint32 *c)
{
 return cluster_pcm_lock_ko_local_relation_scan_v3(l,f,b,c);
}
uint32 z_pcm_bridge_retire(void)
{
 PcmReclaimBatch batch;
 UT_ASSERT(cluster_pcm_lock_reclaim_bounded(8,&batch));
 return batch.retired_count;
}
ClusterNormalStopPollResult z_pcm_bridge_census(const char **reason)
{
 BufferTag tag;uint32 slot;
 return stop_pcm_poll(false,&tag,&slot,reason);
}

bool z_pcm_bridge_ok(void) { return !ut_current_failed; }
