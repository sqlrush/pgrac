/* Real immutable registry/ROOT confirmation and the original merge consumer.
 * The inherited lower actor/IR/WALR/source providers remain controlled. */
#include "postgres.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_control_relmap_recovery.h"
bool fixture_barrier_current(ClusterColdTypedV1 *, struct ClusterRecoveryFencePlan *,
 uint64, uint32, const ClusterWalSourceRef *, XLogRecPtr, XLogRecPtr);
bool fixture_barrier_held(ClusterColdTypedV1 *, struct ClusterRecoveryFencePlan *,
 uint64, uint32, const ClusterWalSourceRef *, XLogRecPtr, XLogRecPtr);
#define cluster_cold_typed_relmap_create_barrier_record_current_v2 fixture_barrier_current
#define cluster_cold_typed_relmap_create_barrier_record_held_v2 fixture_barrier_held
#define cluster_cold_typed_relmap_create_barrier_confirmation_current_v2 fixture_barrier_phase
#define TEST_CONTROL_CONFIRM_MERGE 1
#include "test_cluster_control_relmap_creation.c"
#undef cluster_cold_typed_relmap_create_barrier_record_current_v2
#undef cluster_cold_typed_relmap_create_barrier_record_held_v2
#undef cluster_cold_typed_relmap_create_barrier_confirmation_current_v2
#include "test_cluster_startup_confirm_merge.h"
static ClusterRecoveryFencePlan *component_fence;
static uint64 component_episode;
static unsigned component_phases;
static bool component_observe(bool held)
{
 ControlRootImage *image = calloc(1,sizeof(*image));
 uint8 *pair = malloc(133120);
 ClusterControlRootReadToken token;
 bool ok = image && pair && cluster_control_root_registry_capture_v4(
  selection_dir,root.header.storage_uuid,11,pair,image) && actual_root_token(2,&token);
 if (ok) confirm_merge_observe(&image->records[1],&token,held);
 free(pair);free(image);return ok;
}
bool cluster_cold_typed_relmap_create_barrier_record_current_v2(ClusterColdTypedV1 *t,
 struct ClusterRecoveryFencePlan *f,uint64 id,uint32 p,const ClusterWalSourceRef *s,
 XLogRecPtr read,XLogRecPtr end)
{
 return f==component_fence && id==component_episode
  && cluster_cold_typed_relmap_create_record_current_v2(t,recovery_fence,p,s,read,end)
  && component_observe(cf_held) && confirm_merge_current(f,id);
}
bool cluster_cold_typed_relmap_create_barrier_record_held_v2(ClusterColdTypedV1 *t,
 struct ClusterRecoveryFencePlan *f,uint64 id,uint32 p,const ClusterWalSourceRef *s,
 XLogRecPtr read,XLogRecPtr end)
{
 return f==component_fence && id==component_episode
  && cluster_cold_typed_relmap_create_record_held_v2(t,recovery_fence,p,s,read,end)
  && confirm_merge_held(f,id);
}
bool cluster_cold_typed_relmap_create_barrier_confirmation_current_v2(ClusterColdTypedV1 *t,
 struct ClusterRecoveryFencePlan *f,uint64 id,const ClusterControlRelmapRecoveryCreateV2 *c,
 uint32 p,const ClusterWalSourceRef *s,XLogRecPtr read,XLogRecPtr end)
{
 component_phases++;
 return f==component_fence && id==component_episode
  && cluster_cold_typed_relmap_create_record_current_v2(t,recovery_fence,p,s,read,end)
  && component_observe(cf_held) && confirm_merge_phase(f,id,t,c);
}
static bool component_begin(ClusterControlNativeRelmapCreateV2 **original,
 ClusterControlRelmapRecoveryCreateV2 **receipt,ClusterRecoveryCheckpointFileV2 files[2],
 ClusterControlRootReadToken *before)
{
 ControlRootImage *image;
 uint8 *pair;
 bool ok;
 table_reset();owner_begin();
 /* The independent original ROOT is already sealed for cold replay. */
 root.records[1].lifecycle=CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
 root.records[1].root_flags|=CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
  |CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
 root.records[1].tail_tli=1;
 root.records[1].tail_validation_kind=CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
 root.records[1].validated_tail_lsn_exclusive=0x8000;
 root.records[1].tail_last_record_lsn=0x7000;
 root.records[1].tail_last_record_crc32c=7;
 inventory_write();
 if(reserve_target(original)!=0 || !*original)return false;
 recovery_pair_receipts(*original,files);
 image=calloc(1,sizeof(*image));pair=malloc(133120);
 ok=image && pair && actual_root_token(2,before) && cluster_control_root_registry_capture_v4(
  selection_dir,root.header.storage_uuid,11,pair,image);
 component_episode=0;
 if(ok) component_fence=confirm_merge_begin(&image->records[1],before,&component_episode);
 free(pair);free(image);
 recovery_enter();component_phases=0;
 return ok && component_fence && cluster_control_relmap_recovery_create_begin_barrier_locked_v2(
  &recovery_typed,component_fence,component_episode,0,&recovery_source,0x200,0x280,
  &(*original)->expected,(*original)->batch_uuid,receipt)==0;
}
static void component_end(ClusterControlNativeRelmapCreateV2 **original,
 ClusterControlRelmapRecoveryCreateV2 **receipt)
{
 UT_ASSERT(confirm_merge_end(&component_fence));
 recovery_confirmed_cleanup(original,receipt);
}
UT_TEST(actual_confirm_installs_and_resumes_original_token)
{
 ClusterControlNativeRelmapCreateV2 *original=NULL;
 ClusterControlRelmapRecoveryCreateV2 *receipt=NULL;
 ClusterRecoveryCheckpointFileV2 files[2];
 ClusterControlRootReadToken before,after;
 UT_ASSERT(component_begin(&original,&receipt,files,&before));
 if(!receipt){component_end(&original,&receipt);return;}
 UT_ASSERT_EQ(cluster_control_relmap_recovery_create_confirm_locked_v2(receipt,files,2),0);
 UT_ASSERT(component_phases>=5);
 UT_ASSERT(cluster_control_relmap_recovery_create_root_delta_confirmed_v2(
  receipt,&recovery_typed,component_fence,component_episode));
 UT_ASSERT(component_observe(true));
 UT_ASSERT(confirm_merge_install(component_fence,component_episode,&recovery_typed,receipt));
 UT_ASSERT(confirm_merge_baseline(component_fence,&before));
 UT_ASSERT(confirm_merge_install(component_fence,component_episode,&recovery_typed,receipt));
 UT_ASSERT(actual_root_token(2,&after));
 UT_ASSERT_EQ(after.file_txn_seq,before.file_txn_seq+1);
 cf_held=false;
 UT_ASSERT(confirm_merge_resume(component_fence,component_episode,&after));
 UT_ASSERT(confirm_merge_baseline(component_fence,&before));
 cf_held=true;
 component_end(&original,&receipt);
}
UT_TEST(unconfirmed_receipt_cannot_install_or_resume)
{
 ClusterControlNativeRelmapCreateV2 *original=NULL;
 ClusterControlRelmapRecoveryCreateV2 *receipt=NULL;
 ClusterRecoveryCheckpointFileV2 files[2];
 ClusterControlRootReadToken before;
 UT_ASSERT(component_begin(&original,&receipt,files,&before));
 if(!receipt){component_end(&original,&receipt);return;}
 UT_ASSERT(component_observe(true));
 UT_ASSERT(!confirm_merge_install(component_fence,component_episode,&recovery_typed,receipt));
 cf_held=false;
 UT_ASSERT(!confirm_merge_resume(component_fence,component_episode,&before));
 UT_ASSERT(confirm_merge_baseline(component_fence,&before));
 cf_held=true;
 component_end(&original,&receipt);
}
int main(void)
{
 UT_PLAN(2);
 UT_RUN(actual_confirm_installs_and_resumes_original_token);
 UT_RUN(unconfirmed_receipt_cannot_install_or_resume);
 UT_DONE();return ut_failed_count?1:0;
}
