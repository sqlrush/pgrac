/* Real LMON loop and local producer; host supplies the existing shared
 * PGPROC/backend-status tables and signal/latch scheduling, not receipts. */
#define SURVIVOR_TARGET_FIXTURE
#include "test_cluster_survivor_local.c"
#include "utils/backend_status.h"
static PgBackendStatus target_status[200 + NUM_AUXPROCTYPES];
static unsigned target_waits, target_case;
static int target_child_write;
static pid_t target_child;
static unsigned status_changes, status_reads;
static PgBackendStatus *BackendStatusArray=target_status;
#define NumBackendStatSlots (MaxBackends + NUM_AUXPROCTYPES)
static void
status_read_end(volatile PgBackendStatus *entry,int *after)
{
 status_reads++;
 if(status_changes) {
  status_changes--;
  PGSTAT_BEGIN_WRITE_ACTIVITY(entry);
  entry->st_backendType=B_LMON;
  PGSTAT_END_WRITE_ACTIVITY(entry);
 }
 pgstat_end_read_activity(entry,*after);
}
#undef pgstat_end_read_activity
#define pgstat_end_read_activity(entry, after) status_read_end(entry, &(after))
#include "test_cluster_survivor_status_identity.inc"
#undef pgstat_end_read_activity

#include "cluster/cluster_grd.h"
static ClusterGrdShared target_grd;
static ClusterGrdShared *cluster_grd_state=&target_grd;
#include "test_cluster_survivor_generation.inc"
#include "../../backend/cluster/cluster_semantic_survivor_targets.c"

static ClusterSurvivorTargetCensus *target_cursor(void)
{return (ClusterSurvivorTargetCensus *)(local_control->reserved+2048);}
static void foreign_owner_reply(bool invalidate)
{
 PGPROC *proc=MyProc;int pid=MyProcPid;BackendType type=MyBackendType;AuxProcType aux=MyAuxProcType;
 MyProc=&local_procs[6];MyProcPid=MyProc->pid;MyBackendType=B_LMS;MyAuxProcType=LmsProcess;
 test_producer_seen=0;
 if(invalidate)pg_atomic_write_u64(&SemanticActivationShmem->formation_epoch,4);
 UT_ASSERT(cluster_normal_stop_service_enter());
 cluster_semantic_survivor_local_process();
 UT_ASSERT(cluster_normal_stop_service_leave(true));
 MyProc=proc;MyProcPid=pid;MyBackendType=type;MyAuxProcType=aux;test_producer_seen=0;
}
static void
check_role_sources(void)
{
 static const struct {BackendType type;AuxProcType aux;unsigned family;} rows[]={
  {B_BACKEND,NotAnAuxProcess,1},{B_CHECKPOINTER,CheckpointerProcess,3},
  {B_BG_WRITER,BgWriterProcess,3},{B_LMON,LmonProcess,2},{B_LMS,LmsProcess,2},
  {B_LMD,LmdProcess,2},{B_SINVAL_BCAST,SinvalBcastProcess,2},
  {B_LMS_WORKER,LmsWorker1Process,2},{B_LMS_WORKER,LmsWorker7Process,2}};
 PGPROC saved=local_procs[0];PgBackendStatus copy[lengthof(target_status)];
 memcpy(copy,target_status,sizeof(copy));
 for(unsigned i=0;i<lengthof(rows);i++) {
  ClusterSurvivorLocalTarget t;
  unsigned n=i==0?0:MaxBackends+rows[i].aux;
  memset(target_status,0,sizeof(target_status));
  local_procs[0].pid=9876;local_procs[0].pgprocno=0;local_procs[0].cluster_grd_generation=19;
  local_procs[0].backendId=i==0?1:InvalidBackendId;
  target_status[n].st_changecount=2;target_status[n].st_procpid=9876;target_status[n].st_backendType=rows[i].type;
  UT_ASSERT(targets_read(0,&t));UT_ASSERT_EQ(t.pid,9876);UT_ASSERT_EQ(t.pgprocno,0);
  UT_ASSERT_EQ(t.proc_generation,19);UT_ASSERT_EQ(t.backend_id,i==0?1:-1);
  UT_ASSERT_EQ(t.backend_type,rows[i].type);UT_ASSERT_EQ(t.aux_type,(uint32)rows[i].aux);UT_ASSERT_EQ(t.family,rows[i].family);
  target_status[n].st_procpid=9877;UT_ASSERT(!targets_read(0,&t));UT_ASSERT_EQ(t.pid,0);
 }
 local_procs[0]=saved;memcpy(target_status,copy,sizeof(copy));
}
static int
target_wait(WaitEvent *events)
{
 ClusterSurvivorLocalMailbox *m;
 ClusterSurvivorTargetCensus *c;
 unsigned attempts,reads;
 if (++target_waits == 1) {
  local_wait(events);
  m=fixture_mailbox();memset(m,0,sizeof(*m));
  pg_atomic_init_u64(&m->word,0);pg_atomic_init_u64(&m->invalidated_generation,0);pg_atomic_init_u64(&m->reaped_generation,0);
  UT_ASSERT_EQ(MaxBackends,200);memset(target_status,0,sizeof(target_status));
  target_status[MaxBackends+LmonProcess].st_changecount=2;
  target_status[MaxBackends+LmonProcess].st_procpid=MyProcPid;
  target_status[MaxBackends+LmonProcess].st_backendType=B_LMON;
  pg_atomic_write_u32(&SemanticActivationShmem->transition_closed,target_case==4?0:1);
  pg_atomic_init_u64(&target_grd.next_generation,32);
  status_changes=status_reads=0;
  if(target_case==1)target_status[MaxBackends+LmonProcess].st_changecount=3;
  if(target_case==2)target_status[MaxBackends+LmonProcess].st_procpid++;
  if(target_case==3)target_status[MaxBackends+LmonProcess].st_backendType=B_STARTUP;
  if((target_case>=5 && target_case<=7) || target_case==14 || target_case==17) {
   local_procs[6].pid=5678;local_procs[6].pgprocno=6;local_procs[6].backendId=-1;
   local_procs[6].cluster_grd_generation=16;
   target_status[MaxBackends+LmsProcess].st_changecount=2;
   target_status[MaxBackends+LmsProcess].st_procpid=5678;
   target_status[MaxBackends+LmsProcess].st_backendType=B_LMS;
   if(target_case==14 || target_case==17) {
	int pipefd[2];char byte;
	UT_ASSERT_EQ(pipe(pipefd),0);fflush(NULL);
	target_child=fork();UT_ASSERT(target_child>=0);
	if(target_child==0) {
	 close(pipefd[1]);if(read(pipefd[0],&byte,1)!=1)_exit(9);close(pipefd[0]);
	 MyProc=&local_procs[6];MyProcPid=getpid();MyBackendType=B_LMS;MyAuxProcType=LmsProcess;
	 if(!cluster_normal_stop_service_enter())_exit(10);
	 survivor_poll_barrier_hook=local_die;local_call();_exit(8);
	}
	close(pipefd[0]);target_child_write=pipefd[1];
	local_procs[6].pid=target_child;target_status[MaxBackends+LmsProcess].st_procpid=target_child;
   }
  }
  return 0;
 }
 m=fixture_mailbox();c=target_cursor();
 if(target_case>=1 && target_case<=3) {
  UT_ASSERT_EQ(c->phase,CLUSTER_SURVIVOR_TARGETS_DONE);UT_ASSERT_EQ(c->unknown,1);
  UT_ASSERT_EQ(c->attempts,0);UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,0);
  ShutdownRequestPending=true;return 0;
 }
 if(target_case==4 && target_waits==2) {
  UT_ASSERT_EQ(c->phase,0);UT_ASSERT_EQ(pg_atomic_read_u64(&m->word),0);
  pg_atomic_write_u32(&SemanticActivationShmem->transition_closed,1);return 0;
 }
 if(target_waits==2 || (target_case==4 && target_waits==3)) {
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_REQUESTED);
  UT_ASSERT_EQ(m->query.pid,target_case==14 || target_case==17?target_child:target_case>=5 && target_case<=7?5678:MyProcPid);
  if(target_case==15 || target_case==16) {
   uint64 old_generation=c->query_generation;
   if(target_case==16) {local_call();test_producer_seen=0;}
   local_control->header.round_generation++;local_control->tables[0].header.round_generation++;
   cluster_semantic_survivor_targets_tick();
   UT_ASSERT_EQ(c->round,14);UT_ASSERT_EQ(c->query_generation,old_generation+1);
   UT_ASSERT_EQ(m->query.round_generation,14);
   UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_REQUESTED);
   return 0;
  }
  if(target_case==14 || target_case==17) {
   int status;char byte=1;PGPROC *sender=MyProc;
   UT_ASSERT_EQ(write(target_child_write,&byte,1),1);close(target_child_write);
   UT_ASSERT_EQ(waitpid(target_child,&status,0),target_child);
   UT_ASSERT(WIFEXITED(status));UT_ASSERT_EQ(WEXITSTATUS(status),7);
   UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_READING);
   local_procs[6].pid=5679;local_procs[6].cluster_grd_generation=33;
   target_status[MaxBackends+LmsProcess].st_procpid=5679;
   pg_atomic_write_u64(&target_grd.next_generation,34);
   if(target_case==17) {
	local_control->header.round_generation++;local_control->tables[0].header.round_generation++;
   }
   cluster_semantic_survivor_targets_tick();
   UT_ASSERT_EQ(c->attempts,1);UT_ASSERT_EQ(c->cursor,6);
   UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_READING);
   IsUnderPostmaster=false;IsPostmasterEnvironment=true;MyProc=NULL;
   cluster_semantic_survivor_local_postmaster_reap(target_child);
   MyProc=sender;IsUnderPostmaster=true;IsPostmasterEnvironment=false;
   UT_ASSERT_EQ(pg_atomic_read_u64(&m->reaped_generation),c->query_generation);
   return 0;
  }
  if(target_case>=5 && target_case<=7) {
   if(target_case==5)local_procs[6].pid=0;
   if(target_case==6) {local_procs[6].cluster_grd_generation=33;pg_atomic_write_u64(&target_grd.next_generation,34);}
   if(target_case==7) {
	foreign_owner_reply(true);
	cluster_semantic_survivor_targets_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
	/* Restore only the host's next original-loop observation cut; the
	 * actual census remains invalid for this nonce/round. */
	pg_atomic_write_u64(&SemanticActivationShmem->formation_epoch,3);
   }
   return 0;
  }
  UT_ASSERT_EQ(m->query.proc_generation,17);UT_ASSERT_EQ(m->query.backend_type,B_LMON);
  UT_ASSERT_EQ(m->query.aux_type,LmonProcess);UT_ASSERT_EQ(m->query.backend_id,-1);
  UT_ASSERT_EQ(m->query.family,CLUSTER_SURVIVOR_PRODUCER_NATIVE);
  if(target_case==9)check_role_sources();
  if(target_case==10) {
   int pid;BackendType type;
   status_changes=1;status_reads=0;
   UT_ASSERT(pgstat_read_backend_identity(MaxBackends+LmonProcess,&pid,&type));
   UT_ASSERT_EQ(status_reads,2);UT_ASSERT_EQ(pid,MyProcPid);
   status_changes=2;status_reads=0;
   UT_ASSERT(!pgstat_read_backend_identity(MaxBackends+LmonProcess,&pid,&type));
   UT_ASSERT_EQ(status_reads,2);UT_ASSERT_EQ(pid,0);UT_ASSERT_EQ(type,B_INVALID);
  }
  if(target_case==11) {
   int pid=1;BackendType type=B_LMON;
   PgBackendStatus *table=BackendStatusArray;
   UT_ASSERT(!pgstat_read_backend_identity(-1,&pid,&type));UT_ASSERT_EQ(pid,0);UT_ASSERT_EQ(type,B_INVALID);
   UT_ASSERT(!pgstat_read_backend_identity(MaxBackends+NUM_AUXPROCTYPES,&pid,&type));
   UT_ASSERT(!pgstat_read_backend_identity(0,NULL,&type));
   UT_ASSERT(!pgstat_read_backend_identity(0,&pid,NULL));
   BackendStatusArray=NULL;
   UT_ASSERT(!pgstat_read_backend_identity(0,&pid,&type));BackendStatusArray=table;
  }
  if(target_case==12) {
   ClusterSurvivorTargetCensus saved=*c;
   BackendType type=MyBackendType;MyBackendType=B_BACKEND;
   cluster_semantic_survivor_targets_tick();MyBackendType=type;
   UT_ASSERT_EQ(memcmp(&saved,c,sizeof(saved)),0);
  }
  if(target_case==13) {
   ClusterSurvivorLocalReply reply;ClusterSurvivorLocalTarget t;
   local_call();
   UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&reply),PENDING);
   t=local_self_target();UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);test_producer_seen=0;
  }
  return 0;
 }
 if(target_case==7) {
  UT_ASSERT_EQ(c->phase,CLUSTER_SURVIVOR_TARGETS_STALE);
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
  UT_ASSERT_EQ(c->observed,0);UT_ASSERT_EQ(c->attempts,1);
  ShutdownRequestPending=true;return 0;
 }
 if(target_case==13) {
  UT_ASSERT_EQ(c->phase,CLUSTER_SURVIVOR_TARGETS_STALE);UT_ASSERT_EQ(c->unknown,1);
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)>>2,2);
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_REPLIED);
  ShutdownRequestPending=true;return 0;
 }
 if((target_case==6 || target_case==14 || target_case==17) && target_waits==3) {
  UT_ASSERT_EQ(c->stale,target_case==17?0:1);UT_ASSERT_EQ(c->attempts,target_case==17?1:2);UT_ASSERT_EQ(m->query.proc_generation,33);
  if(target_case==14) {UT_ASSERT_EQ(c->refused,1);UT_ASSERT_EQ(m->query.pid,5679);}
  if(target_case==17) {UT_ASSERT_EQ(c->round,14);UT_ASSERT_EQ(m->query.round_generation,14);}
  foreign_owner_reply(false);return 0;
 }
 if((target_case==5 && target_waits==3)||((target_case==6 || target_case==14 || target_case==17) && target_waits==4)) {
  UT_ASSERT_EQ(c->stale,target_case==17?0:1);UT_ASSERT_EQ(m->query.pid,MyProcPid);return 0;
 }
 UT_ASSERT_EQ(c->phase,CLUSTER_SURVIVOR_TARGETS_DONE);
 UT_ASSERT_EQ(c->observed,target_case==6 || target_case==14 || target_case==17?2:1);
 UT_ASSERT_EQ(c->attempts,target_case==6 || target_case==14?3:target_case==5 || target_case==17?2:1);
 if(target_case==5)UT_ASSERT_EQ(c->unknown,1);
 attempts=c->attempts;reads=status_reads;
 for(unsigned i=0;i<8;i++)cluster_semantic_survivor_targets_tick();
 UT_ASSERT_EQ(c->attempts,attempts);UT_ASSERT_EQ(status_reads,reads);
 UT_ASSERT_EQ(local_control->header.native_closure,0);UT_ASSERT_EQ(local_control->header.ordinary_admission,0);
 UT_ASSERT_EQ(local_control->tables[0].header.flags,0);
 for(unsigned i=2176;i<sizeof(local_control->reserved);i++)UT_ASSERT_EQ(local_control->reserved[i],0);
 ShutdownRequestPending=true;return 0;
}
static void run_targets(unsigned which)
{
 PGPROC *saved=MyProc;PROC_HDR *saved_header=ProcGlobal;
 local_region=mmap(NULL,LOCAL_REGION_BYTES,PROT_READ|PROT_WRITE,MAP_SHARED|MAP_ANON,-1,0);
 local_procs=mmap(NULL,8*sizeof(PGPROC),PROT_READ|PROT_WRITE,MAP_SHARED|MAP_ANON,-1,0);
 UT_ASSERT(local_region!=MAP_FAILED);UT_ASSERT(local_procs!=MAP_FAILED);
 test_collection_kind=4;test_collection_extra_wait=target_wait;test_collection_on=true;
 test_producer_module=0;target_waits=local_waits=0;local_latch_hook=NULL;target_case=which;
 test_run_normal_stop_lmon(true,60);
 test_collection_on=false;test_collection_extra_wait=NULL;
 MyProc=saved;ProcGlobal=saved_header;SemanticActivationShmem=NULL;
 UT_ASSERT_EQ(munmap(local_region,LOCAL_REGION_BYTES),0);UT_ASSERT_EQ(munmap(local_procs,8*sizeof(PGPROC)),0);
}

UT_TEST(original_lmon_selects_real_process_table_target_before_notification){run_targets(0);}
UT_TEST(odd_status_remains_unknown_without_notification){run_targets(1);}
UT_TEST(status_pid_mismatch_cannot_select_an_auxiliary){run_targets(2);}
UT_TEST(unsupported_type_cannot_be_guessed_into_a_family){run_targets(3);}
UT_TEST(scan_waits_for_original_token_gate_close){run_targets(4);}
UT_TEST(selected_exit_retains_unknown_and_never_counts_as_return){run_targets(5);}
UT_TEST(selected_slot_reuse_is_stale_then_asks_exact_new_identity){run_targets(6);}
UT_TEST(invalid_scope_can_retire_only_a_returned_transport){run_targets(7);}
UT_TEST(all_supported_roles_derive_from_actual_status_slot_and_pid){run_targets(9);}
UT_TEST(changecount_retry_is_bounded_and_discards_changed_values){run_targets(10);}
UT_TEST(unavailable_or_invalid_status_never_returns_an_identity){run_targets(11);}
UT_TEST(non_lmon_cannot_advance_shared_census){run_targets(12);}
UT_TEST(census_does_not_consume_another_query_generation){run_targets(13);}
UT_TEST(reaped_callback_does_not_hide_replacement_identity){run_targets(14);}
UT_TEST(new_round_withdraws_only_old_unclaimed_query){run_targets(15);}
UT_TEST(new_round_collects_returned_old_callback){run_targets(16);}
UT_TEST(new_round_waits_for_real_reap_before_reusing_reading_slot){run_targets(17);}
int main(void)
{
 UT_PLAN(17);
 UT_RUN(original_lmon_selects_real_process_table_target_before_notification);
 UT_RUN(odd_status_remains_unknown_without_notification);
 UT_RUN(status_pid_mismatch_cannot_select_an_auxiliary);
 UT_RUN(unsupported_type_cannot_be_guessed_into_a_family);
 UT_RUN(scan_waits_for_original_token_gate_close);
 UT_RUN(selected_exit_retains_unknown_and_never_counts_as_return);
 UT_RUN(selected_slot_reuse_is_stale_then_asks_exact_new_identity);
 UT_RUN(invalid_scope_can_retire_only_a_returned_transport);
 UT_RUN(all_supported_roles_derive_from_actual_status_slot_and_pid);
 UT_RUN(changecount_retry_is_bounded_and_discards_changed_values);
 UT_RUN(unavailable_or_invalid_status_never_returns_an_identity);
 UT_RUN(non_lmon_cannot_advance_shared_census);
 UT_RUN(census_does_not_consume_another_query_generation);
 UT_RUN(reaped_callback_does_not_hide_replacement_identity);
 UT_RUN(new_round_withdraws_only_old_unclaimed_query);
 UT_RUN(new_round_collects_returned_old_callback);
 UT_RUN(new_round_waits_for_real_reap_before_reusing_reading_slot);
 UT_DONE();return ut_failed_count?1:0;
}
