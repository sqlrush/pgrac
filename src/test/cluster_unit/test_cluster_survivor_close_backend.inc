/* Actual close/event, xact state, snapshot/resource collectors, signal and
 * ProcessInterrupts. Shared allocation, process roster, role scheduling and
 * transaction lifecycle transitions are host boundaries. A waiting original
 * StartTransaction prefix stops at the host latch boundary, before resources.
 * No positive producer closure or native process lifecycle is asserted. */
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_semantic_survivor_close.h"
ProcessingMode Mode = NormalProcessing;
static ClusterReconfigState close_backend_reconfig;
static ClusterReconfigState *ReconfigShmem = &close_backend_reconfig;
void cluster_sinval_reset_all_on_reconfig(void) {}
void cluster_write_fence_authority_cache_invalidate(void) {}
/* Preserve the older XID/retention lock host unchanged. This event's
 * independent lock has its own explicit, checked host boundary. */
static bool close_backend_event_locked;
static bool close_backend_event_acquire(LWLock *l,LWLockMode mode)
{
 UT_ASSERT_EQ(l,&ReconfigShmem->lock);UT_ASSERT(mode==LW_SHARED||mode==LW_EXCLUSIVE);
 UT_ASSERT(!close_backend_event_locked);close_backend_event_locked=true;return true;
}
static void close_backend_event_release(LWLock *l)
{ UT_ASSERT_EQ(l,&ReconfigShmem->lock);UT_ASSERT(close_backend_event_locked);close_backend_event_locked=false; }
#define LWLockAcquire close_backend_event_acquire
#define LWLockRelease close_backend_event_release
#include "test_cluster_survivor_close_event.inc"
#undef LWLockRelease
#undef LWLockAcquire
static sigjmp_buf close_backend_wait_exit;
static unsigned close_backend_cleanup, close_backend_waits, close_backend_resets;
static void close_backend_cleanup_poll(void) { close_backend_cleanup++; }
static void close_backend_reset(Latch *l) { UT_ASSERT_EQ(l,MyLatch);close_backend_resets++; }
static int close_backend_wait(Latch *l,int events,long timeout,uint32 event)
{
 UT_ASSERT_EQ(l,MyLatch);UT_ASSERT_EQ(events,WL_LATCH_SET|WL_EXIT_ON_PM_DEATH);
 UT_ASSERT_EQ(timeout,-1);UT_ASSERT_EQ(event,WAIT_EVENT_RECONFIG_BARRIER_WAIT);
 close_backend_waits++;siglongjmp(close_backend_wait_exit,1);return 0;
}
#pragma push_macro("proc")
#undef proc
#define cluster_cf_retirement_poll close_backend_cleanup_poll
#define cluster_lock_owners_service_poll close_backend_cleanup_poll
#define ResetLatch close_backend_reset
#define WaitLatch close_backend_wait
#include "../../backend/cluster/cluster_semantic_survivor_close.c"
#undef WaitLatch
#undef ResetLatch
#undef cluster_lock_owners_service_poll
#undef cluster_cf_retirement_poll
#pragma pop_macro("proc")
static bool close_backend_delivery_unreached(void) { abort(); }
#define cluster_shared_config_delivery_retry_idle close_backend_delivery_unreached
#define StartTransaction close_backend_start_prefix
#include "test_cluster_survivor_close_xact.inc"
#undef StartTransaction
#undef cluster_shared_config_delivery_retry_idle
static ResourceOwner close_backend_saved_owner;
static void close_backend_prepare(void)
{
 CurrentResourceOwner=backend_resource_one;
 backend_return_begin();snapshot_return_epoch=8;xact_return_reset();
 XactTopFullTransactionId=InvalidFullTransactionId;
 fixture_native_top_owner=fixture_native_transaction_owner=NULL;
 close_backend_saved_owner=CurrentResourceOwner;CurrentResourceOwner=NULL;
 memset(&survivor_local_owner_storage,0,sizeof(survivor_local_owner_storage));
 memset(&close_backend_reconfig,0,sizeof(close_backend_reconfig));
 memset(route_status,0,sizeof(route_status));
 route_status[0].st_changecount=2;route_status[0].st_procpid=1234;route_status[0].st_backendType=B_BACKEND;
 snapshot_procs[1].pid=2345;snapshot_procs[1].pgprocno=1;
 snapshot_procs[1].backendId=-1;snapshot_procs[1].cluster_grd_generation=17;
 ProcGlobal->allProcs=snapshot_procs;ProcGlobal->allProcCount=2;
 memset(route_signal_bytes,0,sizeof(route_signal_bytes));ProcSignal=(ProcSignalHeader *)route_signal_bytes;
 ProcSignal->psh_slot[0].pss_pid=1234;local_notifies=local_wakes=0;local_kill_result=0;local_latch_hook=NULL;
 InterruptPending=ProcDiePending=QueryCancelPending=ClientConnectionLost=false;
 InterruptHoldoffCount=QueryCancelHoldoffCount=CritSectionCount=0;DoingCommandRead=false;
 interrupt_lock_timeout=interrupt_statement_timeout=false;
 pg_atomic_write_u32(&backend_return_gate.transition_closed,0);
 pg_atomic_init_u64(&census_grd.next_generation,32);
 close_backend_cleanup=close_backend_waits=close_backend_resets=0;
 route_lmon();
 ReconfigEvent event={0};event.event_id=19;event.old_epoch=7;event.new_epoch=8;
 event.reconfig_kind=RECONFIG_KIND_FAIL_STOP;event.observer_role=CLUSTER_RECONFIG_OBSERVER_SURVIVOR;
 event.dead_bitmap[0]=2;cluster_reconfig_publish_event(&event);
 UT_ASSERT_EQ(cluster_semantic_survivor_close_begin(),CLOSE_PENDING);
 UT_ASSERT_EQ(pg_atomic_read_u32(&backend_return_gate.transition_closed),0);
 for(unsigned i=0;i<60704;i++)UT_ASSERT_EQ(((uint8 *)&survivor_local_owner_storage)[i],0);
}
static void close_backend_begin(void)
{
 ClusterSurvivorLocalTarget target;
 route_lmon();UT_ASSERT(targets_read(0,&target));
 UT_ASSERT_EQ(cluster_semantic_survivor_local_close_begin(&target),LOCAL_PENDING);
 UT_ASSERT_EQ(route_mailbox()->local_abi,2);UT_ASSERT_EQ(route_mailbox()->flags,1);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_REQUESTED);
}
static void close_backend_deliver(void)
{ route_backend();DoingCommandRead=true;route_deliver(); }
static void close_backend_pending(void)
{
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_REQUESTED);
 UT_ASSERT_EQ(route_mailbox()->reply.payload_bytes,0);
}
static void close_backend_fact(uint32 origin)
{
 ClusterSurvivorLocalReply r;
 route_lmon();UT_ASSERT_EQ(cluster_semantic_survivor_local_close_poll(&r),LOCAL_PENDING);
 UT_ASSERT_EQ(r.payload_bytes,1256);UT_ASSERT_EQ(r.identity.pid,1234);
 UT_ASSERT_EQ(r.identity.pgprocno,0);UT_ASSERT_EQ(r.identity.backend_id,1);
 UT_ASSERT_EQ(r.identity.proc_generation,11);UT_ASSERT_EQ(r.identity.epoch,8);
 UT_ASSERT_EQ(survivor_local_scalar(r.body+1128,4),1);
 UT_ASSERT_EQ(survivor_local_scalar(r.body+1132,4),128);
 UT_ASSERT_EQ(survivor_local_scalar(r.body+1136,8),1);
 UT_ASSERT_EQ(survivor_local_scalar(r.body+1208,4),origin);
 /* NULL owner remains unknown I/O responsibility, even with this fact. */
 UT_ASSERT_NE(survivor_local_scalar(r.body+44,4)&4,0);
 UT_ASSERT_EQ(survivor_local_owner_storage.header.native_closure,0);
 UT_ASSERT_EQ(survivor_local_owner_storage.header.ordinary_admission,0);
 UT_ASSERT_EQ(survivor_local_owner_storage.header.typed_status,0);
 UT_ASSERT_EQ(pg_atomic_read_u32(&backend_return_gate.transition_closed),0);
 for(unsigned i=0;i<60704;i++)UT_ASSERT_EQ(((uint8 *)&survivor_local_owner_storage)[i],0);
}
static void close_backend_clear(void)
{
 route_lmon();cluster_semantic_survivor_local_invalidate();
 route_backend();CurrentResourceOwner=close_backend_saved_owner;
 xact_return_reset();XactTopFullTransactionId=InvalidFullTransactionId;
 fixture_native_top_owner=fixture_native_transaction_owner=NULL;
 route_clear(NULL);backend_return_lock_held=false;
}
UT_TEST(close_query_command_read_reports_only_original_return_fact)
{
 close_backend_prepare();close_backend_begin();close_backend_deliver();
 UT_ASSERT_EQ(local_notifies,1);close_backend_fact(CLUSTER_SURVIVOR_RETURN_COMMAND);
 close_backend_clear();
}
UT_TEST(close_query_exec_copy_and_pre_start_gap_cannot_claim_return)
{
 close_backend_prepare();close_backend_begin();route_deliver();close_backend_pending();
 UT_ASSERT(!DoingCommandRead);UT_ASSERT(!InterruptPending);
 /* Execution/COPY and the interval after a previously open gate read have
  * no original return marker, even if the private point state is empty. */
 cluster_semantic_survivor_local_process();close_backend_pending();
 close_backend_deliver();close_backend_fact(CLUSTER_SURVIVOR_RETURN_COMMAND);close_backend_clear();
}
UT_TEST(close_query_retains_actual_transaction_subtransaction_and_cleanup_phases)
{
 const TransState phases[]={TRANS_START,TRANS_INPROGRESS,TRANS_COMMIT,TRANS_ABORT,TRANS_PREPARE};
 close_backend_prepare();close_backend_begin();
 for(unsigned i=0;i<lengthof(phases);i++) {
  TopTransactionStateData.state=phases[i];close_backend_deliver();close_backend_pending();
  UT_ASSERT_EQ(TopTransactionStateData.state,phases[i]);
 }
 xact_return_reset();TransactionStateData child={0};child.parent=&TopTransactionStateData;
 CurrentTransactionState=&child;close_backend_deliver();close_backend_pending();
 UT_ASSERT_EQ(CurrentTransactionState,&child);
 xact_return_reset();close_backend_deliver();close_backend_fact(1);close_backend_clear();
}
UT_TEST(close_query_idle_transaction_and_resource_or_xid_residue_do_not_prove_return)
{
 close_backend_prepare();close_backend_begin();
 for(unsigned i=0;i<7;i++) {
  xact_return_reset();
  if(i==0)TopTransactionStateData.blockState=TBLOCK_INPROGRESS;
  if(i==1)TopTransactionStateData.fullTransactionId=FullTransactionIdFromU64(88);
  if(i==2)XactTopFullTransactionId=FullTransactionIdFromU64(88);
  if(i==3)TopTransactionStateData.curTransactionOwner=close_backend_saved_owner;
  if(i==4)fixture_native_top_owner=close_backend_saved_owner;
  if(i==5)fixture_native_transaction_owner=close_backend_saved_owner;
  if(i==6)CurrentResourceOwner=close_backend_saved_owner;
  close_backend_deliver();close_backend_pending();
  XactTopFullTransactionId=InvalidFullTransactionId;
  fixture_native_top_owner=fixture_native_transaction_owner=NULL;CurrentResourceOwner=NULL;
 }
 xact_return_reset();close_backend_deliver();close_backend_fact(1);close_backend_clear();
}
UT_TEST(close_query_original_start_wait_covers_chain_and_next_transaction)
{
 close_backend_prepare();close_backend_begin();route_backend();
 TopTransactionStateData.blockState=TBLOCK_END;
 if(sigsetjmp(close_backend_wait_exit,0)==0)close_backend_start_prefix();
 UT_ASSERT_EQ(TopTransactionStateData.state,TRANS_DEFAULT);
 UT_ASSERT_EQ(TopTransactionStateData.blockState,TBLOCK_END);
 UT_ASSERT_EQ(close_backend_waits,1);UT_ASSERT_EQ(close_backend_cleanup,2);
 UT_ASSERT_EQ(close_backend_resets,1);close_backend_fact(2);
 /* Same frontend message's next transaction reaches exactly the same gate. */
 close_backend_begin();route_backend();TopTransactionStateData.blockState=TBLOCK_DEFAULT;
 if(sigsetjmp(close_backend_wait_exit,0)==0)close_backend_start_prefix();
 UT_ASSERT_EQ(TopTransactionStateData.state,TRANS_DEFAULT);UT_ASSERT_EQ(close_backend_waits,2);
 close_backend_fact(2);UT_ASSERT_EQ(local_notifies,2);close_backend_clear();
}
UT_TEST(close_query_cancel_exit_holdoff_and_lock_keep_original_priority)
{
 close_backend_prepare();close_backend_begin();route_backend();DoingCommandRead=true;
 for(unsigned i=0;i<4;i++) {
  QueryCancelPending=i==0;ProcDiePending=i==1;InterruptHoldoffCount=i==2;QueryCancelHoldoffCount=i==3;
  cluster_semantic_survivor_backend_command_return();close_backend_pending();
 }
 QueryCancelPending=ProcDiePending=false;InterruptHoldoffCount=QueryCancelHoldoffCount=0;
 backend_return_lock_held=true;cluster_semantic_survivor_backend_command_return();close_backend_pending();
 backend_return_lock_held=false;QueryCancelPending=true;DoingCommandRead=false;InterruptPending=true;
 volatile bool caught=false;PG_TRY();{ProcessInterrupts();}PG_CATCH();{caught=true;}PG_END_TRY();
 UT_ASSERT(caught);UT_ASSERT_EQ(error_code,ERRCODE_QUERY_CANCELED);close_backend_pending();
 close_backend_deliver();close_backend_fact(1);UT_ASSERT_EQ(local_notifies,1);close_backend_clear();
}
static void close_backend_drift(unsigned kind)
{
 ClusterSurvivorLocalReply r,zero={0};close_backend_prepare();close_backend_begin();close_backend_deliver();
 route_lmon();ClusterSurvivorCloseRecord *c=close_record();
 if(kind==0)pg_atomic_write_u64(&c->seq,pg_atomic_read_u64(&c->seq)+2);
 if(kind==1)pg_atomic_write_u64(&backend_return_gate.record_generation,20);
 if(kind==2)snapshot_procs[1].cluster_grd_generation++;
 if(kind==3)proc.cluster_grd_generation++;
 if(kind==4)snapshot_return_epoch++;
 if(kind==5)proc.backendId++;
 memset(&r,0xa5,sizeof(r));UT_ASSERT_EQ(cluster_semantic_survivor_local_close_poll(&r),LOCAL_STALE);
 UT_ASSERT_EQ(memcmp(&r,&zero,sizeof(r)),0);
 /* Original owner replacement cannot retire an executing callback. */
 if(kind==2){snapshot_procs[1].cluster_grd_generation--;route_lmon();cluster_semantic_survivor_local_invalidate();}
 close_backend_clear();
}
UT_TEST(close_query_withdraws_close_gate_owner_target_and_epoch_drift)
{ for(unsigned i=0;i<6;i++)close_backend_drift(i); }
UT_TEST(close_query_rejects_malformed_binding_and_fact_without_receipt)
{
 for(unsigned i=0;i<8;i++) {
  ClusterSurvivorLocalReply r,zero={0};close_backend_prepare();close_backend_begin();close_backend_deliver();
  ClusterSurvivorLocalMailbox *m=route_mailbox();
  if(i==0)m->reply.body[1128]++;
  if(i==1)m->reply.body[1136]=2;
  if(i==2)m->reply.body[1208]=3;
  if(i==3)m->reply.body[1212]=1;
  if(i==4)m->reply.body[1256]=1;
  if(i==5)m->reserved[127]=1;
  if(i==6)m->reserved[128]=1;
  if(i==7)m->reply.body[1144]++;
  route_lmon();memset(&r,0xa5,sizeof(r));
  UT_ASSERT_EQ(cluster_semantic_survivor_local_close_poll(&r),i==5||i==6?LOCAL_STALE:LOCAL_REFUSED);
  UT_ASSERT_EQ(memcmp(&r,&zero,sizeof(r)),0);close_backend_clear();
 }
}
UT_TEST(close_query_purpose_cannot_cross_consume_callback)
{
 ClusterSurvivorLocalReply r;close_backend_prepare();close_backend_begin();route_lmon();
 uint64 word=pg_atomic_read_u64(&route_mailbox()->word);
 UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),LOCAL_REFUSED);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word),word);
 cluster_semantic_survivor_targets_tick();UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word),word);
 close_backend_deliver();close_backend_fact(1);close_backend_clear();
 Snapshot held=route_prepare();route_begin();route_lmon();word=pg_atomic_read_u64(&route_mailbox()->word);
 UT_ASSERT_EQ(cluster_semantic_survivor_local_close_poll(&r),LOCAL_REFUSED);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word),word);route_clear(held);
}
UT_TEST(close_census_uses_actual_close_and_never_grants_qualification)
{
 close_backend_prepare();route_lmon();cluster_semantic_survivor_targets_close_tick();
 UT_ASSERT_EQ(census_cursor()->purpose,1);UT_ASSERT_EQ(census_cursor()->zero,0);
 UT_ASSERT_EQ(census_cursor()->nonce,19);UT_ASSERT_EQ(census_cursor()->round,1);
 UT_ASSERT_EQ(census_cursor()->attempts,1);close_backend_deliver();
 route_lmon();cluster_semantic_survivor_targets_close_tick();
 UT_ASSERT_EQ(census_cursor()->observed,1);UT_ASSERT_EQ(census_cursor()->unknown,1);
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_DONE);
 UT_ASSERT_EQ(survivor_local_owner_storage.header.native_closure,0);
 UT_ASSERT_EQ(survivor_local_owner_storage.header.ordinary_admission,0);close_backend_clear();
}
UT_TEST(close_census_new_birth_withdraws_reading_without_reusing_callback)
{
 close_backend_prepare();route_lmon();cluster_semantic_survivor_targets_close_tick();
 uint64 word=pg_atomic_read_u64(&route_mailbox()->word);
 /* Host-controlled callback claim, with no fabricated sample. */
 pg_atomic_write_u64(&route_mailbox()->word,(word&~UINT64_C(3))|CLUSTER_SURVIVOR_LOCAL_READING);
 proc.cluster_grd_generation=33;pg_atomic_write_u64(&census_grd.next_generation,34);
 cluster_semantic_survivor_targets_close_tick();
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_STALE);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_READING);
 UT_ASSERT_EQ(local_notifies,1);UT_ASSERT_EQ(route_mailbox()->query.proc_generation,11);
 /* Restore the host claim as an unclaimed request only to retire test
  * storage; this does not stand in for a product callback or reap proof. */
 pg_atomic_write_u64(&route_mailbox()->word,(word&~UINT64_C(3))|CLUSTER_SURVIVOR_LOCAL_REQUESTED);
 close_backend_clear();
}

UT_TEST(close_query_reading_needs_actual_reap_and_preserves_original_snapshot)
{
 close_backend_prepare();CurrentResourceOwner=backend_resource_one;Snapshot held=registered(88);
 CurrentResourceOwner=NULL;fflush(NULL);pid_t child=fork();int status;
 UT_ASSERT(child>=0);if(child==0)_exit(7);
 proc.pid=child;route_status[0].st_procpid=child;ProcSignal->psh_slot[0].pss_pid=child;
 close_backend_begin();uint64 word=pg_atomic_read_u64(&route_mailbox()->word);
 pg_atomic_write_u64(&route_mailbox()->word,(word&~UINT64_C(3))|CLUSTER_SURVIVOR_LOCAL_READING);
 ClusterSurvivorLocalReply r;route_lmon();cluster_semantic_survivor_local_invalidate();
 proc.pid=0;UT_ASSERT_EQ(cluster_semantic_survivor_local_close_poll(&r),LOCAL_STALE);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_READING);
 proc.pid=child;UT_ASSERT_EQ(waitpid(child,&status,0),child);UT_ASSERT(WIFEXITED(status));
 UT_ASSERT_EQ(WEXITSTATUS(status),7);
 IsUnderPostmaster=false;IsPostmasterEnvironment=true;MyProc=NULL;
 cluster_semantic_survivor_local_postmaster_reap(child);
 IsUnderPostmaster=true;IsPostmasterEnvironment=false;route_lmon();
 /* Actual waitpid lets the original poll retire transport as REFUSED. */
 UT_ASSERT_EQ(cluster_semantic_survivor_local_close_poll(&r),LOCAL_REFUSED);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
 UT_ASSERT_EQ(r.payload_bytes,0);
 UT_ASSERT_EQ(cluster_semantic_survivor_local_close_poll(&r),LOCAL_PENDING);
 UT_ASSERT_EQ(held->regd_count,1);UT_ASSERT_EQ(remembered,1);
 proc.pid=1234;CurrentResourceOwner=backend_resource_one;UnregisterSnapshot(held);close_backend_clear();
}
static void close_backend_raise_error(void) { survivor_poll_barrier_hook=NULL; ereport(ERROR,(errmsg("host producer error"))); }
UT_TEST(close_query_original_return_error_publishes_refusal_and_rethrows)
{
 close_backend_prepare();close_backend_begin();route_backend();DoingCommandRead=true;
 /* Original source producer barrier schedules the existing host ERROR,
  * never a successful producer sample or retirement. */
 survivor_poll_barrier_hook=close_backend_raise_error;
 volatile bool caught=false;PG_TRY();{cluster_semantic_survivor_backend_command_return();}
 PG_CATCH();{caught=true;}PG_END_TRY();survivor_poll_barrier_hook=NULL;
 UT_ASSERT(caught);UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_REPLIED);
 route_lmon();ClusterSurvivorLocalReply r;
 UT_ASSERT_EQ(cluster_semantic_survivor_local_close_poll(&r),LOCAL_REFUSED);UT_ASSERT_EQ(r.payload_bytes,0);
 close_backend_clear();
}

UT_TEST(close_census_first_birth_after_unknown_cut_reasks_original_process_table)
{
 close_backend_prepare();memset(&route_status[0],0,sizeof(route_status[0]));
 route_lmon();cluster_semantic_survivor_targets_close_tick();
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_DONE);UT_ASSERT_EQ(local_notifies,0);
 proc.cluster_grd_generation=33;pg_atomic_write_u64(&census_grd.next_generation,34);
 route_status[0].st_changecount=2;route_status[0].st_procpid=1234;route_status[0].st_backendType=B_BACKEND;
 cluster_semantic_survivor_targets_close_tick();
 UT_ASSERT_EQ(census_cursor()->purpose,1);UT_ASSERT_EQ(census_cursor()->birth_cut,34);
 UT_ASSERT_EQ(census_cursor()->observed,0);UT_ASSERT_EQ(census_cursor()->attempts,1);
 UT_ASSERT_EQ(route_mailbox()->query.proc_generation,33);UT_ASSERT_EQ(local_notifies,1);
 close_backend_clear();
}
UT_TEST(close_census_switch_retires_original_candidate_callback_before_rebinding)
{
 Snapshot held=census_prepare();census_begin();uint64 word=pg_atomic_read_u64(&route_mailbox()->word);
 pg_atomic_write_u64(&route_mailbox()->word,(word&~UINT64_C(3))|CLUSTER_SURVIVOR_LOCAL_READING);
 memset(&close_backend_reconfig,0,sizeof(close_backend_reconfig));
 ReconfigEvent e={0};e.event_id=19;e.old_epoch=7;e.new_epoch=8;e.reconfig_kind=RECONFIG_KIND_FAIL_STOP;
 e.observer_role=CLUSTER_RECONFIG_OBSERVER_SURVIVOR;e.dead_bitmap[0]=2;
 cluster_reconfig_publish_event(&e);UT_ASSERT_EQ(cluster_semantic_survivor_close_begin(),CLOSE_PENDING);
 cluster_semantic_survivor_targets_close_tick();
 UT_ASSERT_EQ(census_cursor()->purpose,0);UT_ASSERT_EQ(route_mailbox()->local_abi,1);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_READING);
 UT_ASSERT_EQ(local_notifies,1);
 /* Schedule the original claimed callback's refusing publication; no
  * resource observation or successful retirement is supplied by the host. */
 ClusterSurvivorLocalQuery q=route_mailbox()->query;route_backend();
 local_publish(route_mailbox(),word>>2,&q,LOCAL_REFUSED,NULL,0);
 route_lmon();cluster_semantic_survivor_targets_close_tick();
 UT_ASSERT_EQ(census_cursor()->purpose,1);UT_ASSERT_EQ(route_mailbox()->local_abi,2);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)>>2,(word>>2)+1);
 UT_ASSERT_EQ(local_notifies,2);UT_ASSERT_EQ(held->regd_count,1);
 route_backend();UnregisterSnapshot(held);route_clear(NULL);
}
