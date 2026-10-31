/* Original PI callbacks with real input job/CF/WALR/ACK bodies.
 * I/O, transport, allocation, native lock and clock are explicit boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
#define PGRAC_WAL_INPUTS_JOB_PIN_EMBEDDED
#include "test_cluster_wal_inputs_job_pin.c"
#include "test_cluster_wal_inputs_job_begin_fixture.h"
#include "cluster/cluster_pi_writeback.h"
#include "cluster/cluster_pi_rebuild.h"
#include "cluster_wal_inputs_job_payload_private.h"
#include "storage/spin.h"
#include "storage/buffile.h"
#include "test_cluster_wal_inputs_pi_types.inc"
int s_lock(volatile slock_t *lock,const char *file,int line,const char *func)
{ (void)lock;(void)file;(void)line;(void)func; abort(); }
static unsigned pi_file_closes;
void BufFileClose(BufFile *file) { UT_ASSERT(file==(BufFile*)1); ++pi_file_closes; }
/* No physical job/plan/receipt is manufactured by this lifecycle fixture.
 * Such resources must already be cleared by their original error callback. */
#define PI_EMPTY_ONLY(type,name) void name(type **value) { if(value==NULL||*value!=NULL) abort(); }
PI_EMPTY_ONLY(ClusterThreadRecoveryFabricPlanV1,cluster_thread_recovery_fabric_plan_destroy_v1)
PI_EMPTY_ONLY(ClusterPiWritebackJobV1,cluster_pi_writeback_release_v1)
PI_EMPTY_ONLY(ClusterPiDataV1,cluster_pi_data_release_v1)
PI_EMPTY_ONLY(ClusterPiPhysicalAckV1,cluster_page_data_pi_ack_free_v1)
PI_EMPTY_ONLY(ClusterPiStructuralAckV2,cluster_page_structural_pi_ack_free_v2)
PI_EMPTY_ONLY(ClusterPageDataReceiptV1,cluster_page_data_receipt_free_v1)
PI_EMPTY_ONLY(ClusterPageStructuralReceiptV2,cluster_page_structural_receipt_free_v2)
#undef PI_EMPTY_ONLY
#include "test_cluster_wal_inputs_pi_callbacks.inc"

static WritebackShared pi_shared;
static void
pi_setup(ClusterWalInputsConsumerV1 consumer)
{
 uint8 uuid[16]={1}; ClusterWalInputsV1 *inputs=NULL;
 begin_setup(); foundation_finish(&begin_job);
 if(consumer != WAL_INPUT_JOB_PI_BATCH) {
  MyBackendType=B_BG_WRITER; MyAuxProcType=BgWriterProcess;
 }
 begin_job=foundation_open(consumer);
 UT_ASSERT_EQ(cluster_wal_inputs_job_begin_v1(begin_job,uuid,1234,&inputs),CLUSTER_CONTROL_ROOT_OK_PRIMARY);
 memset(&pi_shared,0,sizeof(pi_shared)); SpinLockInit(&pi_shared.lock); wb_shared=&pi_shared;
 if(consumer==WAL_INPUT_JOB_PI_NOTICE) {
  wb_notice=MemoryContextAllocZero(begin_job->context,sizeof(*wb_notice));
  wb_notice->owner=CurrentResourceOwner; wb_notice->pid=getpid(); wb_notice->revision=11;
  wb_notice->version=1; wb_notice->inputs=inputs; wb_notice->inputs_job=begin_job;
  pi_shared.inbound_pid=MyProcPid;pi_shared.inbound_revision=11;pi_shared.inbound_version=1;
  pi_shared.inbound_state=WB_RUNNING;pi_shared.physical_started=true;
 } else if(consumer==WAL_INPUT_JOB_PI_BATCH) {
  wb_batch=MemoryContextAllocZero(begin_job->context,sizeof(*wb_batch));
  wb_batch->owner=CurrentResourceOwner;wb_batch->pid=getpid();wb_batch->inputs=inputs;wb_batch->inputs_job=begin_job;
  pi_shared.checkpointer_pid=MyProcPid;
 } else {
  pi_rebuild_job=MemoryContextAllocZero(begin_job->context,sizeof(*pi_rebuild_job));
  pi_rebuild_job->owner=CurrentResourceOwner;pi_rebuild_job->inputs=inputs;pi_rebuild_job->inputs_job=begin_job;
 }
}
static void pi_end(void)
{
 wb_notice=NULL;wb_batch=NULL;pi_rebuild_job=NULL;wb_shared=NULL;
 if(begin_job!=NULL) begin_cleanup(); else foundation_end();
 cluster_controlfile_shared_authority=false;
 if(inputs_jobs_process.caller_lifetime!=NULL) {
  MemoryContextDelete(inputs_jobs_process.caller_lifetime);
  inputs_jobs_process.caller_lifetime=NULL;
 }
}
UT_TEST(pi_notice_error_keeps_original_header_and_running_request)
{
 ClusterPiWritebackNoticeV1 *saved; unsigned waits;
 pi_setup(WAL_INPUT_JOB_PI_NOTICE);saved=wb_notice;waits=owner_wait_calls;
 inputs_jobs_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);
 wb_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);
 UT_ASSERT(wb_notice==saved);UT_ASSERT_EQ(pi_shared.inbound_state,WB_RUNNING);
 UT_ASSERT_EQ(pi_shared.inbound_pid,MyProcPid);UT_ASSERT(pi_shared.physical_started);
 UT_ASSERT_EQ(pin_native_refs,2);UT_ASSERT_EQ(owner_wait_calls,waits);
 pi_end();
}
UT_TEST(pi_batch_error_keeps_original_header_and_participant)
{
 WritebackBatch *saved;unsigned waits;
 pi_setup(WAL_INPUT_JOB_PI_BATCH);saved=wb_batch;waits=owner_wait_calls;
 inputs_jobs_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);
 wb_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);
 UT_ASSERT(wb_batch==saved);UT_ASSERT_EQ(pi_shared.checkpointer_pid,MyProcPid);
 UT_ASSERT_EQ(pin_native_refs,2);UT_ASSERT_EQ(owner_wait_calls,waits);pi_end();
}
UT_TEST(pi_rebuild_error_keeps_original_header)
{
 PiRebuildJob *saved;unsigned waits;
 pi_setup(WAL_INPUT_JOB_PI_REBUILD);saved=pi_rebuild_job;waits=owner_wait_calls;
 inputs_jobs_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);
 pi_rebuild_owner_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);
 UT_ASSERT(pi_rebuild_job==saved);UT_ASSERT_EQ(pin_native_refs,2);
 UT_ASSERT_EQ(owner_wait_calls,waits);pi_end();
}
UT_TEST(pi_notice_wrong_consumer_backlink_is_hard_refusal)
{
 ClusterWalInputsJobV1 *other;ClusterPiWritebackNoticeV1 *saved;volatile bool caught=false;
 pi_setup(WAL_INPUT_JOB_PI_NOTICE);saved=wb_notice;other=foundation_open(WAL_INPUT_JOB_PI_REBUILD);
 wb_notice->inputs_job=other;job_expect_fatal=true;
 PG_TRY(); { wb_server_finish(NULL); } PG_CATCH(); { caught=true; } PG_END_TRY();
 job_expect_fatal=false;
 UT_ASSERT(caught);UT_ASSERT(wb_notice==saved);UT_ASSERT_EQ(pi_shared.inbound_state,WB_RUNNING);
 UT_ASSERT_EQ(pin_native_refs,2);
 if(caught) foundation_finish(&other);
 wb_notice=saved;wb_notice->inputs_job=begin_job;pi_end();
}

static void
pi_ack_all(void)
{
 while(begin_job->pin!=NULL) {
  ClusterWalReadPinV1 *pin;
  cluster_wal_inputs_jobs_service_poll_v1();pin=begin_job->pin;
  if(pin==NULL) break;
  pin_ack_member(&pin->locks[pin->cursor-1].request,false);
  cluster_wal_inputs_jobs_service_poll_v1();
 }
 UT_ASSERT_EQ(begin_job->state,WAL_INPUT_JOB_STATE_DONE);
}
UT_TEST(pi_notice_pending_never_publishes_stack_reply_or_drops_request)
{
 ClusterPiWritebackMessageV1 reply={0};ClusterPiWritebackNoticeV1 *saved;unsigned waits;
 pi_setup(WAL_INPUT_JOB_PI_NOTICE);saved=wb_notice;owner_ack_on_wait=false;
 reply.verb=CLUSTER_PI_WRITEBACK_ACK;wb_server_finish(&reply);waits=owner_wait_calls;
 UT_ASSERT(wb_notice==saved);UT_ASSERT_EQ(pi_shared.inbound_state,WB_RUNNING);
 UT_ASSERT(!pi_shared.reply_pending&&pi_shared.physical_started);UT_ASSERT_EQ(pin_native_refs,2);
 wb_server_finish(NULL);UT_ASSERT_EQ(owner_wait_calls,waits);UT_ASSERT_EQ(pin_native_refs,2);
 pi_ack_all();wb_server_finish(NULL);begin_job=NULL;
 UT_ASSERT(wb_notice==NULL);UT_ASSERT_EQ(pi_shared.inbound_state,WB_QUEUED);
 UT_ASSERT(pi_shared.physical_started&&!pi_shared.reply_pending);UT_ASSERT_EQ(pi_shared.inbound_pid,0);
 pi_end();
}
UT_TEST(pi_notice_healthy_exact_owner_hands_off_only_after_retirement)
{
 ClusterPiWritebackMessageV1 reply={0};
 pi_setup(WAL_INPUT_JOB_PI_NOTICE);reply.verb=CLUSTER_PI_WRITEBACK_ACK;reply.nonce=27;
 wb_server_finish(&reply);begin_job=NULL;
 UT_ASSERT(wb_notice==NULL);UT_ASSERT_EQ(pin_native_refs,0);
 UT_ASSERT_EQ(pi_shared.inbound_state,WB_REPLIED);UT_ASSERT(pi_shared.reply_pending);
 UT_ASSERT_EQ(pi_shared.reply.nonce,27);pi_end();
}
UT_TEST(pi_batch_pending_keeps_participant_until_exact_ack)
{
 WritebackBatch *saved;unsigned waits;
 pi_setup(WAL_INPUT_JOB_PI_BATCH);saved=wb_batch;owner_ack_on_wait=false;
 wb_batch_release();waits=owner_wait_calls;
 UT_ASSERT(wb_batch==saved);UT_ASSERT_EQ(pi_shared.checkpointer_pid,MyProcPid);
 UT_ASSERT_EQ(pin_native_refs,2);wb_batch_release();UT_ASSERT_EQ(owner_wait_calls,waits);
 pi_ack_all();wb_batch_release();begin_job=NULL;
 UT_ASSERT(wb_batch==NULL);UT_ASSERT_EQ(pi_shared.checkpointer_pid,0);pi_end();
}
UT_TEST(pi_rebuild_pending_closes_original_spool_once_and_keeps_cut)
{
 PiRebuildJob *saved;unsigned waits,closes;
 pi_setup(WAL_INPUT_JOB_PI_REBUILD);saved=pi_rebuild_job;owner_ack_on_wait=false;
 pi_rebuild_job->cut.epoch=57;pi_rebuild_job->spool=(BufFile*)1;closes=pi_file_closes;
 UT_ASSERT(!pi_rebuild_release());waits=owner_wait_calls;
 UT_ASSERT(pi_rebuild_job==saved);UT_ASSERT_EQ(pi_rebuild_job->cut.epoch,57);
 UT_ASSERT_EQ(pi_file_closes,closes+1);UT_ASSERT(!pi_rebuild_release());
 UT_ASSERT_EQ(pi_file_closes,closes+1);UT_ASSERT_EQ(owner_wait_calls,waits);
 pi_ack_all();UT_ASSERT(pi_rebuild_release());begin_job=NULL;UT_ASSERT(pi_rebuild_job==NULL);pi_end();
}
UT_TEST(pi_error_borrows_are_never_revisited_after_work_reset)
{
 for(unsigned kind=WAL_INPUT_JOB_PI_NOTICE;kind<=WAL_INPUT_JOB_PI_REBUILD;++kind) {
  unsigned waits,roots,closes;
  pi_setup((ClusterWalInputsConsumerV1)kind);waits=owner_wait_calls;roots=begin_roots;closes=pi_file_closes;
  if(wb_notice!=NULL) wb_notice->plan=(void*)1;
  if(wb_batch!=NULL) {
   wb_batch->count=1;wb_batch->plan=(void*)1;wb_batch->physical_job=(void*)1;wb_batch->data_job=(void*)1;
   wb_batch->receipts[0]=(void*)1;wb_batch->acks[0][0]=(void*)1;wb_batch->ack_count[0]=1;
  }
  if(pi_rebuild_job!=NULL) pi_rebuild_job->spool=(BufFile*)1;
  inputs_jobs_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);
  if(kind==WAL_INPUT_JOB_PI_REBUILD) pi_rebuild_owner_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);
  else wb_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);
  MemoryContextReset(work_context);UT_ASSERT_EQ(pin_native_refs,2);
  UT_ASSERT_EQ(owner_wait_calls,waits);UT_ASSERT_EQ(begin_roots,roots);
  pi_ack_all();
  if(kind==WAL_INPUT_JOB_PI_NOTICE) wb_server_finish(NULL);
  else if(kind==WAL_INPUT_JOB_PI_BATCH) wb_batch_release();
  else UT_ASSERT(pi_rebuild_release());
  begin_job=NULL;UT_ASSERT_EQ(pi_file_closes,closes);UT_ASSERT_EQ(begin_roots,roots);pi_end();
 }
}
UT_TEST(pi_foreign_owner_cannot_release_any_original_consumer)
{
 for(unsigned kind=WAL_INPUT_JOB_PI_NOTICE;kind<=WAL_INPUT_JOB_PI_REBUILD;++kind) {
  volatile bool caught=false;
  pi_setup((ClusterWalInputsConsumerV1)kind);CurrentResourceOwner=(void*)&job_fixture_foreign_owner;
  job_expect_fatal=true;
  PG_TRY(); {
   if(kind==WAL_INPUT_JOB_PI_NOTICE) wb_server_finish(NULL);
   else if(kind==WAL_INPUT_JOB_PI_BATCH) wb_batch_release();else (void)pi_rebuild_release();
  } PG_CATCH(); {caught=true;} PG_END_TRY();
  job_expect_fatal=false;UT_ASSERT(caught);UT_ASSERT_EQ(pin_native_refs,2);
  CurrentResourceOwner=AuxProcessResourceOwner;pi_end();
 }
}
UT_TEST(pi_batch_header_is_allocated_in_original_aux_job_context)
{
 ClusterWalSourceRef local={0};JobFixtureAllocation *entry;bool found=false;
 begin_setup();foundation_finish(&begin_job);
 UT_ASSERT(wb_batch_allocate(&local));begin_job=wb_batch->inputs_job;
 for(entry=job_fixture_allocations;entry!=NULL;entry=entry->next)
  if(entry->allocation==wb_batch) {found=true;UT_ASSERT(entry->context==begin_job->context);}
 UT_ASSERT(found);UT_ASSERT_EQ(sizeof(ClusterWalInputsJobControlBudgetV1),2040);
 UT_ASSERT(sizeof(*wb_notice)<=CLUSTER_WAL_INPUT_JOB_NOTICE_BYTES);
 UT_ASSERT(sizeof(*wb_batch)<=CLUSTER_WAL_INPUT_JOB_BATCH_BYTES);
 UT_ASSERT(sizeof(*pi_rebuild_job)<=CLUSTER_WAL_INPUT_JOB_REBUILD_BYTES);
 UT_ASSERT(MemoryContextGetParent(begin_job->context)==inputs_jobs_process.caller_lifetime);
 wb_batch_release();begin_job=NULL;pi_end();
}

static bool
pi_exit_callback(unsigned kind)
{
 volatile bool caught=false;
 job_expect_fatal=true;
 PG_TRY(); {
  if(kind==WAL_INPUT_JOB_PI_REBUILD) pi_rebuild_owner_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);
  else wb_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);
 } PG_CATCH(); {caught=true;} PG_END_TRY();
 job_expect_fatal=false;
 return caught;
}
UT_TEST(pi_owner_callbacks_allow_exact_aux_exit_without_wait)
{
 for(unsigned kind=WAL_INPUT_JOB_PI_NOTICE;kind<=WAL_INPUT_JOB_PI_REBUILD;++kind) {
  unsigned waits;
  pi_setup((ClusterWalInputsConsumerV1)kind);waits=owner_wait_calls;proc_exit_inprogress=true;
  UT_ASSERT(!pi_exit_callback(kind));
  UT_ASSERT((begin_job->flags&(WAL_INPUT_JOB_FLAG_REVOKED|WAL_INPUT_JOB_FLAG_ERROR))
   ==(WAL_INPUT_JOB_FLAG_REVOKED|WAL_INPUT_JOB_FLAG_ERROR));
  UT_ASSERT_EQ(owner_wait_calls,waits);UT_ASSERT_EQ(pin_native_refs,2);
  UT_ASSERT(wb_notice!=NULL||wb_batch!=NULL||pi_rebuild_job!=NULL);
  proc_exit_inprogress=false;pi_end();
 }
}
UT_TEST(pi_owner_exit_still_rejects_changed_process_generation)
{
 for(unsigned kind=WAL_INPUT_JOB_PI_NOTICE;kind<=WAL_INPUT_JOB_PI_REBUILD;++kind) {
  pi_setup((ClusterWalInputsConsumerV1)kind);proc_exit_inprogress=true;
  ++MyProc->cluster_grd_generation;
  UT_ASSERT(pi_exit_callback(kind));UT_ASSERT_EQ(pin_native_refs,2);
  --MyProc->cluster_grd_generation;proc_exit_inprogress=false;pi_end();
 }
}

/* Exact installed write-phase call boundary; ROOT/WAL may follow only
 * after this product fragment has relinquished the original batch pin. */
/* These leaves do not supply shutdown authority: unsupported budget/identity
 * reads abort. The ordinary ACK loop and release bodies are actual product. */
#include "access/xlog.h"
#include "utils/wait_event.h"
bool cluster_normal_stop_requested(void) { return false; }
void AbsorbSyncRequests(void) {}
uint64 cluster_checkpoint_wait_budget_now(void) { abort(); }
long cluster_checkpoint_wait_budget_charge(bool wait,uint64 now,const ClusterWalSourceRef *ref,uint64 epoch)
{ (void)wait;(void)now;(void)ref;(void)epoch;abort(); }
bool cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *ref) { (void)ref;abort(); }
#include "test_cluster_pi_checkpoint_wait.inc"
static void pi_checkpoint_boundary(void)
{
 int flags=0;
#include "test_cluster_pi_checkpoint_release.inc"
 UT_ASSERT_EQ(pin_native_refs,0);
 UT_ASSERT(wb_batch==NULL);
}
UT_TEST(pi_checkpoint_never_enters_root_before_original_batch_ack)
{
 pi_setup(WAL_INPUT_JOB_PI_BATCH);owner_ack_on_wait=false;
 UT_ASSERT(!cluster_pi_writeback_checkpointer_release_v1());
 UT_ASSERT_EQ(pin_native_refs,2);owner_ack_on_wait=true;
 pi_checkpoint_boundary();begin_job=NULL;pi_end();
}

static void pi_failed_setup(void)
{
 pi_setup(WAL_INPUT_JOB_PI_REBUILD);begin_guard_output=false;
 pi_rebuild_job->plan_blocked=true;
 begin_job->original_result=CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED;
 UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1(begin_job),WAL_INPUT_JOB_DONE);
}
UT_TEST(pi_failed_root_keeps_done_header_and_never_restores_input)
{
 PiRebuildJob *saved;unsigned reads;
 pi_failed_setup();saved=pi_rebuild_job;reads=begin_roots;
 for(unsigned i=0;i<4;++i) {
  UT_ASSERT(pi_rebuild_failed_wait());
  UT_ASSERT(pi_rebuild_job==saved);UT_ASSERT_EQ(begin_job->state,WAL_INPUT_JOB_STATE_DONE);
  UT_ASSERT(cluster_wal_inputs_job_context_v1(begin_job)==NULL);
  UT_ASSERT_EQ(cluster_wal_inputs_count_v1(begin_job->inputs),0);
  UT_ASSERT_EQ(pin_native_refs,0);UT_ASSERT_EQ(begin_origins,2);UT_ASSERT_EQ(begin_claims,2);
 }
 UT_ASSERT_EQ(begin_roots,reads+4);UT_ASSERT_EQ(begin_job->original_result,CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED);
 pi_end();
}
UT_TEST(pi_failed_root_pending_keeps_exact_detection_until_ack)
{
 unsigned reads;ClusterWalInputsJobV1 *same=NULL;
 pi_failed_setup();begin_changed_phase=begin_pending_phase=begin_roots+1;
 UT_ASSERT(pi_rebuild_failed_wait());
 reads=begin_roots;UT_ASSERT_EQ(begin_job->state,WAL_INPUT_JOB_STATE_RETIRING_CF);
 UT_ASSERT_EQ(pin_native_refs,0);UT_ASSERT_EQ(cluster_wal_inputs_job_finish_v1(&begin_job),WAL_INPUT_JOB_PENDING);
 UT_ASSERT_EQ(cluster_wal_inputs_job_open_v1(WAL_INPUT_JOB_PI_REBUILD,caller_context,&same),CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
 UT_ASSERT(same==begin_job);MemoryContextReset(work_context);
 UT_ASSERT_EQ(cluster_wal_inputs_job_failed_root_v1(begin_job),CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
 UT_ASSERT_EQ(begin_roots,reads);
 UT_ASSERT(acknowledge_retirements());cluster_wal_inputs_jobs_service_poll_v1();
 UT_ASSERT_EQ(begin_job->state,WAL_INPUT_JOB_STATE_DONE);
 UT_ASSERT(!pi_rebuild_failed_wait());
 UT_ASSERT_EQ(begin_roots,reads);UT_ASSERT(cluster_wal_inputs_job_context_v1(begin_job)==NULL);
 UT_ASSERT_EQ(begin_job->original_result,CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED);
 {
  uint64 generation=begin_job->generation;uint8 uuid[16]={1};ClusterWalInputsV1 *inputs=NULL;
  UT_ASSERT(pi_rebuild_release());begin_job=NULL;UT_ASSERT(pi_rebuild_job==NULL);
  begin_job=foundation_open(WAL_INPUT_JOB_PI_REBUILD);begin_guard_output=true;
  UT_ASSERT(begin_job->generation>generation);
  UT_ASSERT_EQ(cluster_wal_inputs_job_begin_v1(begin_job,uuid,1234,&inputs),CLUSTER_CONTROL_ROOT_OK_PRIMARY);
  UT_ASSERT(inputs!=NULL);UT_ASSERT_EQ(begin_roots,reads+3);
  UT_ASSERT_EQ(begin_origins,4);UT_ASSERT_EQ(begin_claims,4);UT_ASSERT_EQ(pin_native_refs,2);
 }
 pi_end();
}
UT_TEST(pi_failed_root_refuses_usable_foreign_and_error_owners)
{
 unsigned reads;
 pi_setup(WAL_INPUT_JOB_PI_REBUILD);reads=begin_roots;
 UT_ASSERT_EQ(cluster_wal_inputs_job_failed_root_v1(begin_job),CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
 UT_ASSERT_EQ(begin_roots,reads);begin_guard_output=false;
 UT_ASSERT_EQ(cluster_wal_inputs_job_wait_failed_v1(begin_job),WAL_INPUT_JOB_DONE);
 CurrentResourceOwner=(ResourceOwner)&job_fixture_foreign_owner;
 UT_ASSERT_EQ(cluster_wal_inputs_job_failed_root_v1(begin_job),CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
 CurrentResourceOwner=AuxProcessResourceOwner;
 pi_rebuild_owner_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);
 UT_ASSERT_EQ(cluster_wal_inputs_job_failed_root_v1(begin_job),CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
 UT_ASSERT_EQ(begin_roots,reads);pi_end();
}


UT_TEST(pi_pending_detection_error_keeps_original_debt_without_cached_business_use)
{
 for(unsigned callback=0;callback<2;++callback) {
  unsigned reads,waits;volatile bool caught=false;bool valid;
  pi_failed_setup();begin_changed_phase=begin_pending_phase=begin_roots+1;
  UT_ASSERT(pi_rebuild_failed_wait());reads=begin_roots;waits=owner_wait_calls;
  UT_ASSERT_EQ(begin_job->state,WAL_INPUT_JOB_STATE_RETIRING_CF);
  job_expect_fatal=true;
  PG_TRY(); {
   if(callback) inputs_jobs_resource_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);
   else UT_ASSERT(cluster_wal_inputs_job_revoke_v1(begin_job,true));
  } PG_CATCH(); {caught=true;} PG_END_TRY();
  job_expect_fatal=false;UT_ASSERT(!caught);
  valid=cluster_wal_inputs_job_consumer_matches_v1(begin_job,WAL_INPUT_JOB_PI_REBUILD);
  UT_ASSERT(valid);UT_ASSERT((begin_job->flags&WAL_INPUT_JOB_FLAG_ERROR)!=0);
  UT_ASSERT((begin_job->flags&WAL_INPUT_JOB_FLAG_DETECT)==0);UT_ASSERT_EQ(begin_job->detect_result,0);
  UT_ASSERT_EQ(owner_wait_calls,waits);UT_ASSERT_EQ(begin_roots,reads);
  /* RED teardown only: repair the diagnostic header, retaining its real
   * pending receipt. The following exact ACK still retires that debt. */
  if(!valid) {
   begin_job->flags=inputs_jobs[WAL_INPUT_JOB_PI_REBUILD].flags=WAL_INPUT_JOB_FLAG_REVOKED|WAL_INPUT_JOB_FLAG_ERROR;
   begin_job->detect_result=0;
  }
  UT_ASSERT_EQ(cluster_wal_inputs_job_failed_root_v1(begin_job),CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
  UT_ASSERT(acknowledge_retirements());cluster_wal_inputs_jobs_service_poll_v1();
  UT_ASSERT_EQ(begin_job->state,WAL_INPUT_JOB_STATE_DONE);
  UT_ASSERT_EQ(pin_native_refs,0);UT_ASSERT_EQ(begin_roots,reads);
  UT_ASSERT_EQ(cluster_wal_inputs_job_failed_root_v1(begin_job),CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
  pi_end();
 }
}

int main(void)
{
 MyProc=&job_procs[42];MyProc->pgprocno=42;MyLatch=&MyProc->procLatch;
 job_proc_global.allProcs=job_procs;job_proc_global.allProcCount=lengthof(job_procs);
 pg_atomic_init_u32(&MyProc->cluster_grd_registered_count,0);
 pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked,0);
 pg_atomic_init_u64(&MyProc->cluster_grd_redeclare_acked_epoch,0);
 MyProc->cluster_grd_generation=1;
 cluster_shared_config = true;
 cluster_control_request_shmem_init();
 control_driver = retire_driver = cluster_control_request_driver_start();
 setvbuf(stdout,NULL,_IONBF,0);
 UT_PLAN(18);
 UT_RUN(pi_notice_error_keeps_original_header_and_running_request);
 UT_RUN(pi_batch_error_keeps_original_header_and_participant);
 UT_RUN(pi_rebuild_error_keeps_original_header);
 UT_RUN(pi_notice_wrong_consumer_backlink_is_hard_refusal);
 UT_RUN(pi_notice_pending_never_publishes_stack_reply_or_drops_request);
 UT_RUN(pi_notice_healthy_exact_owner_hands_off_only_after_retirement);
 UT_RUN(pi_batch_pending_keeps_participant_until_exact_ack);
 UT_RUN(pi_rebuild_pending_closes_original_spool_once_and_keeps_cut);
 UT_RUN(pi_error_borrows_are_never_revisited_after_work_reset);
 UT_RUN(pi_foreign_owner_cannot_release_any_original_consumer);
 UT_RUN(pi_batch_header_is_allocated_in_original_aux_job_context);
 UT_RUN(pi_owner_callbacks_allow_exact_aux_exit_without_wait);
 UT_RUN(pi_owner_exit_still_rejects_changed_process_generation);
 UT_RUN(pi_checkpoint_never_enters_root_before_original_batch_ack);
 UT_RUN(pi_failed_root_keeps_done_header_and_never_restores_input);
 UT_RUN(pi_failed_root_pending_keeps_exact_detection_until_ack);
 UT_RUN(pi_failed_root_refuses_usable_foreign_and_error_owners);
 UT_RUN(pi_pending_detection_error_keeps_original_debt_without_cached_business_use);
 UT_DONE();
 return ut_failed_count != 0;
}
