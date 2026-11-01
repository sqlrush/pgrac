/* Actual LMON event consumer and the original transaction startup prefix.
 * PGPROC, locks, event publication storage and OS waiting are host boundaries;
 * no receipt or global producer closure is supplied by this fixture. */
#define SURVIVOR_TARGET_FIXTURE
#include "test_cluster_survivor_local.c"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_semantic_survivor_close.h"
static ClusterReconfigState close_reconfig;
static ClusterReconfigState *ReconfigShmem=&close_reconfig;
static unsigned close_resets;
void cluster_sinval_reset_all_on_reconfig(void) {close_resets++;}
void cluster_write_fence_authority_cache_invalidate(void) {}
#include "test_cluster_survivor_close_event.inc"

typedef struct {int state;FullTransactionId fullTransactionId;} CloseTransactionState;
typedef CloseTransactionState *TransactionState;
static CloseTransactionState TopTransactionStateData;
static TransactionState CurrentTransactionState;
static FullTransactionId XactTopFullTransactionId;
#define TRANS_DEFAULT 0
#define TRANS_START 1
bool cluster_shared_config_delivery_retry_idle(void) {return false;}
#include "test_cluster_survivor_close_xact.inc"
static unsigned close_case,close_wait_calls,close_cleanup_polls;
static bool close_interrupt_after_wake;
static unsigned close_interrupt_calls;
static ClusterSemanticActivationShmem close_old_gate;
static sigjmp_buf close_wait_jump;
static int close_host_wait(Latch *latch,int events,long timeout,uint32 info)
{
 (void)latch;(void)info;
 UT_ASSERT_EQ(events,WL_LATCH_SET|WL_EXIT_ON_PM_DEATH);
 UT_ASSERT_EQ(timeout,-1);
 close_wait_calls++;
 if(close_case==20 && close_wait_calls==1) {
  /* A real wake, not a timer: the next original interrupt check cancels.
   * PG error construction is an explicit host boundary in this fixture. */
  close_interrupt_after_wake=true;InterruptPending=true;
  return WL_LATCH_SET;
 }
 siglongjmp(close_wait_jump,1);
 return 0;
}
/* Original backend cleanup scheduling is a host boundary here. Existing
 * CF/owner suites execute their real retirement bodies separately. */
static void close_cf_cleanup(void) {close_cleanup_polls++;}
static void close_owners_cleanup(void) {close_cleanup_polls++;}
static void close_host_interrupts(void)
{
 close_interrupt_calls++;
 InterruptPending=false;
 if(close_interrupt_after_wake)siglongjmp(close_wait_jump,2);
}
#define ProcessInterrupts close_host_interrupts
#define cluster_cf_retirement_poll close_cf_cleanup
#define cluster_lock_owners_service_poll close_owners_cleanup
#define WaitLatch close_host_wait
#include "../../backend/cluster/cluster_semantic_survivor_close.c"
#undef WaitLatch
#undef cluster_cf_retirement_poll
#undef cluster_lock_owners_service_poll
#undef ProcessInterrupts

static int close_loop_wait(WaitEvent *events)
{
 int result=local_wait(events);
 if(local_waits==1) {
  ReconfigEvent event={0};
  memset(&close_reconfig,0,sizeof(close_reconfig));
  event.event_id=19;event.old_epoch=2;event.new_epoch=3;
  event.reconfig_kind=RECONFIG_KIND_FAIL_STOP;
  event.observer_role=CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
  event.dead_bitmap[0]=2;
  if(close_case==2)event.reconfig_kind=RECONFIG_KIND_CLEAN_LEAVE;
  if(close_case==3)event.reconfig_kind=RECONFIG_KIND_NODE_REMOVED;
  if(close_case==4)event.new_epoch=4;
  if(close_case==5)memset(event.dead_bitmap,0,sizeof(event.dead_bitmap));
  if(close_case==6)event.event_id=0;
  if(close_case!=7 && close_case!=24)cluster_reconfig_publish_event(&event);
  close_old_gate=*SemanticActivationShmem;
 } else {
  UT_ASSERT_EQ(memcmp(&close_old_gate,SemanticActivationShmem,sizeof(close_old_gate)),0);
  if(close_case>=10 && close_case<=19) {
   ClusterSurvivorCloseRecord *cr=close_record();
   ClusterSurvivorCloseSnapshot sample;
   ClusterSurvivorCloseRecord before=*cr;
   UT_ASSERT_EQ(cluster_semantic_survivor_close_observe(&sample),CLOSE_PENDING);
   UT_ASSERT(sample.blocked);
   if(close_case==10) {
	UT_ASSERT_EQ(cluster_semantic_survivor_close_begin(),CLOSE_PENDING);
	UT_ASSERT_EQ(memcmp(cr,&before,sizeof(before)),0);
   }
   if(close_case==11) {
	cluster_semantic_survivor_close_invalidate();
	UT_ASSERT_EQ(cluster_semantic_survivor_close_observe(&sample),CLOSE_STALE);
	UT_ASSERT(sample.blocked);UT_ASSERT_EQ(cr->state,CLUSTER_SURVIVOR_CLOSE_STALE);
	UT_ASSERT_EQ(cr->event_id,before.event_id);
	UT_ASSERT_EQ(cr->generation,before.generation);
   }
   if(close_case==12) {
	uint64 gen=MyProc->cluster_grd_generation;
	MyProc->cluster_grd_generation++;
	UT_ASSERT_EQ(cluster_semantic_survivor_close_observe(&sample),CLOSE_STALE);
	UT_ASSERT(sample.blocked);MyProc->cluster_grd_generation=gen;
	UT_ASSERT_EQ(memcmp(cr,&before,sizeof(before)),0);
   }
   if(close_case==13) {
	pg_atomic_fetch_add_u64(&SemanticActivationShmem->admission_seq,2);
	UT_ASSERT_EQ(cluster_semantic_survivor_close_observe(&sample),CLOSE_STALE);
	UT_ASSERT(sample.blocked);
   }
   if(close_case==14) {
	pg_atomic_write_u64(&cr->seq,3);
	UT_ASSERT_EQ(cluster_semantic_survivor_close_observe(&sample),CLOSE_STALE);
	UT_ASSERT(sample.blocked);
	UT_ASSERT_EQ(cluster_semantic_survivor_close_begin(),CLOSE_STALE);
	UT_ASSERT_EQ(pg_atomic_read_u64(&cr->seq),3);
   }
   if(close_case==15) {
	cr->generation=UINT64_MAX;
	close_reconfig.last_applied.event_seq++;
	UT_ASSERT_EQ(cluster_semantic_survivor_close_begin(),CLOSE_STALE);
	UT_ASSERT_EQ(cr->generation,UINT64_MAX);
	UT_ASSERT_EQ(cluster_semantic_survivor_close_observe(&sample),CLOSE_PENDING);
	UT_ASSERT(sample.blocked);
   }
   if(close_case==16) {
	ReconfigEvent next=close_reconfig.last_applied;
	next.event_id++;cluster_reconfig_publish_event(&next);
	UT_ASSERT_EQ(cluster_semantic_survivor_close_begin(),CLOSE_PENDING);
	UT_ASSERT_EQ(cr->generation,before.generation+1);
	UT_ASSERT_EQ(cr->event_id,next.event_id);
	UT_ASSERT_EQ(cluster_semantic_survivor_close_observe(&sample),CLOSE_PENDING);
	UT_ASSERT(sample.blocked);
   }
   if(close_case==17) {
	BackendType saved=MyBackendType;
	MyBackendType=B_BACKEND;
	UT_ASSERT_EQ(cluster_semantic_survivor_close_begin(),CLOSE_REFUSED);
	cluster_semantic_survivor_close_invalidate();MyBackendType=saved;
	UT_ASSERT_EQ(memcmp(cr,&before,sizeof(before)),0);
   }
   if(close_case==18) {
	pg_atomic_write_u64(&cr->seq,UINT64_MAX-1);
	UT_ASSERT_EQ(cluster_semantic_survivor_close_begin(),CLOSE_STALE);
	cluster_semantic_survivor_close_invalidate();
	UT_ASSERT_EQ(pg_atomic_read_u64(&cr->seq),UINT64_MAX-1);
	UT_ASSERT_EQ(cluster_semantic_survivor_close_observe(&sample),CLOSE_PENDING);
	UT_ASSERT(sample.blocked);
   }
   if(close_case==19) {
	cr->state=42;cr->reserved[0]=1;
	UT_ASSERT_EQ(cluster_semantic_survivor_close_observe(&sample),CLOSE_STALE);
	UT_ASSERT(sample.blocked);
   }
   UT_ASSERT_EQ(memcmp(local_control,&candidate,60704),0);
   return result;
  }
  if(close_case==0 || (close_case>=2 && close_case<=7)) {
   /* Header/census/observer counts cannot masquerade as a live close gate. */
   uint64 gen;uint32 state;
   memcpy(&gen,local_control->reserved+2048+128+8,8);
   memcpy(&state,local_control->reserved+2048+128+88,4);
   UT_ASSERT_EQ(state,close_case==0?1:0);UT_ASSERT_EQ(gen,close_case==0?1:0);
  } else {
   BackendType saved=MyBackendType;
   ProcessingMode saved_mode=Mode;
   ClusterSemanticActivationShmem *saved_shmem=SemanticActivationShmem;
   bool saved_shared=cluster_shared_config,saved_enabled=cluster_enabled;
   MyBackendType=close_case==8?B_CHECKPOINTER:B_BACKEND;
   if(close_case==9)Mode=InitProcessing;
   if(close_case==21)SemanticActivationShmem=NULL;
   if(close_case==22)cluster_shared_config=false;
   if(close_case==23)cluster_enabled=false;
   memset(&TopTransactionStateData,0,sizeof(TopTransactionStateData));
   XactTopFullTransactionId=InvalidFullTransactionId;
   close_wait_calls=close_cleanup_polls=close_interrupt_calls=0;
   close_interrupt_after_wake=false;InterruptPending=close_case==20;
   if(sigsetjmp(close_wait_jump,1)==0)StartTransaction();
   InterruptPending=false;
   MyBackendType=saved;Mode=saved_mode;SemanticActivationShmem=saved_shmem;
   cluster_shared_config=saved_shared;cluster_enabled=saved_enabled;
   UT_ASSERT_EQ(close_wait_calls,(close_case==1 || close_case==20 || close_case==21)?1:0);
   UT_ASSERT_EQ(TopTransactionStateData.state,(close_case==1 || close_case==20 || close_case==21)?TRANS_DEFAULT:TRANS_START);
   UT_ASSERT_EQ(close_cleanup_polls,(close_case==1 || close_case==20 || close_case==21)?2:0);
   if(close_case==20)UT_ASSERT_EQ(close_interrupt_calls,2);
  }
 }
 return result;
}
static void run_close(unsigned which)
{
 PGPROC *saved=MyProc;PROC_HDR *header=ProcGlobal;
 local_region=mmap(NULL,LOCAL_REGION_BYTES,PROT_READ|PROT_WRITE,MAP_SHARED|MAP_ANON,-1,0);
 local_procs=mmap(NULL,8*sizeof(PGPROC),PROT_READ|PROT_WRITE,MAP_SHARED|MAP_ANON,-1,0);
 UT_ASSERT(local_region!=MAP_FAILED);UT_ASSERT(local_procs!=MAP_FAILED);
 test_collection_kind=4;test_collection_extra_wait=close_loop_wait;test_collection_on=true;
 test_producer_module=0;local_waits=0;local_case=0;local_latch_hook=NULL;
 memset(&close_reconfig,0,sizeof(close_reconfig));
 close_case=which;test_native_epoch=3;
 test_run_normal_stop_lmon(true,60);
 test_collection_on=false;test_collection_extra_wait=NULL;
 MyProc=saved;ProcGlobal=header;SemanticActivationShmem=NULL;
 UT_ASSERT_EQ(local_waits,2);
 UT_ASSERT_EQ(munmap(local_region,LOCAL_REGION_BYTES),0);
 UT_ASSERT_EQ(munmap(local_procs,8*sizeof(PGPROC)),0);
}
UT_TEST(real_published_failstop_closes_from_original_lmon) {run_close(0);}
UT_TEST(real_transaction_start_waits_before_first_new_resource) {run_close(1);}
UT_TEST(clean_leave_keeps_independent_gate_unset) {run_close(2);}
UT_TEST(permanent_removal_is_not_a_failstop_episode) {run_close(3);}
UT_TEST(event_epoch_must_be_the_original_current_epoch) {run_close(4);}
UT_TEST(empty_failed_set_cannot_start_an_episode) {run_close(5);}
UT_TEST(unpublished_event_identity_cannot_start_an_episode) {run_close(6);}
UT_TEST(no_real_event_keeps_original_admission) {run_close(7);}
UT_TEST(original_background_transaction_start_keeps_progressing) {run_close(8);}
UT_TEST(original_initialization_transaction_start_keeps_progressing) {run_close(9);}
UT_TEST(same_event_is_idempotent_without_refreshing_old_baseline) {run_close(10);}
UT_TEST(invalidation_withdraws_sample_and_never_reopens) {run_close(11);}
UT_TEST(owner_generation_drift_is_stale_not_retirement) {run_close(12);}
UT_TEST(original_gate_drift_stays_closed_without_rewriting_old_gate) {run_close(13);}
UT_TEST(odd_publication_never_reopens_or_reuses_record) {run_close(14);}
UT_TEST(generation_exhaustion_keeps_negative_gate) {run_close(15);}
UT_TEST(next_real_published_event_advances_only_close_generation) {run_close(16);}
UT_TEST(ordinary_caller_cannot_install_or_invalidate_a_close) {run_close(17);}
UT_TEST(sequence_exhaustion_never_wraps_or_reopens) {run_close(18);}
UT_TEST(unknown_state_or_reserved_bytes_remain_closed) {run_close(19);}
UT_TEST(latch_wake_preserves_original_interrupt_cancellation_and_cleanup) {run_close(20);}
UT_TEST(missing_control_keeps_new_transaction_waiting_without_resources) {run_close(21);}
UT_TEST(nonshared_transaction_preserves_original_startup) {run_close(22);}
UT_TEST(cluster_disabled_preserves_original_startup) {run_close(23);}
UT_TEST(no_event_ordinary_transaction_preserves_original_startup) {run_close(24);}
int main(void)
{
 UT_PLAN(25);UT_RUN(real_published_failstop_closes_from_original_lmon);
 UT_RUN(real_transaction_start_waits_before_first_new_resource);
 UT_RUN(clean_leave_keeps_independent_gate_unset);
 UT_RUN(permanent_removal_is_not_a_failstop_episode);
 UT_RUN(event_epoch_must_be_the_original_current_epoch);
 UT_RUN(empty_failed_set_cannot_start_an_episode);
 UT_RUN(unpublished_event_identity_cannot_start_an_episode);
 UT_RUN(no_real_event_keeps_original_admission);
 UT_RUN(original_background_transaction_start_keeps_progressing);
 UT_RUN(original_initialization_transaction_start_keeps_progressing);
 UT_RUN(same_event_is_idempotent_without_refreshing_old_baseline);
 UT_RUN(invalidation_withdraws_sample_and_never_reopens);
 UT_RUN(owner_generation_drift_is_stale_not_retirement);
 UT_RUN(original_gate_drift_stays_closed_without_rewriting_old_gate);
 UT_RUN(odd_publication_never_reopens_or_reuses_record);
 UT_RUN(generation_exhaustion_keeps_negative_gate);
 UT_RUN(next_real_published_event_advances_only_close_generation);
 UT_RUN(ordinary_caller_cannot_install_or_invalidate_a_close);
 UT_RUN(sequence_exhaustion_never_wraps_or_reopens);
 UT_RUN(unknown_state_or_reserved_bytes_remain_closed);
 UT_RUN(latch_wake_preserves_original_interrupt_cancellation_and_cleanup);
 UT_RUN(missing_control_keeps_new_transaction_waiting_without_resources);
 UT_RUN(nonshared_transaction_preserves_original_startup);
 UT_RUN(cluster_disabled_preserves_original_startup);
 UT_RUN(no_event_ordinary_transaction_preserves_original_startup);
 UT_DONE();return ut_failed_count?1:0;
}
