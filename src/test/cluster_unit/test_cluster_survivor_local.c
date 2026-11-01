/* The real original LmonMain loop and native producer, with host PGPROC,
 * latch and shared-memory scheduling boundaries. Raw queries are untrusted
 * test inputs, never manufactured receipts or six-RED acceptance evidence.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include "storage/procsignal.h"
#include "storage/condition_variable.h"
#define procsignal_sigusr1_handler local_unused_sigusr1
#define SetLatch local_unused_SetLatch
#define SURVIVOR_LOCAL_FIXTURE
#include "test_cluster_survivor_candidate.c"
#undef procsignal_sigusr1_handler
#undef SetLatch
#include "test_cluster_survivor_signal_fixture.h"
#include "cluster/cluster_semantic_survivor_local.h"
#include "test_cluster_survivor_shared.inc"
bool IsPostmasterEnvironment;
#include "../../backend/cluster/cluster_semantic_survivor_local.c"

#define LOCAL_REGION_BYTES (65536 + 131072)
static uint8 *local_region;
static ClusterSurvivorControl *local_control;
static PGPROC *local_procs;
static PROC_HDR local_proc_header;
static unsigned local_waits, local_case;
static void local_exercise(void);

static ClusterSurvivorLocalMailbox *fixture_mailbox(void)
{ return (ClusterSurvivorLocalMailbox *)local_control->reserved; }
static int
local_wait(WaitEvent *events pg_attribute_unused())
{
 ClusterSurvivorLocalMailbox *m;
 if (++local_waits == 1) {
  ClusterServiceProducerCollection direct;
  begin_candidate();
  UT_ASSERT_EQ(probe(&direct),PENDING);
  UT_ASSERT(LOCAL_REGION_BYTES >= MAXALIGN(sizeof(*SemanticActivationShmem)) + sizeof(candidate));
  memset(local_region, 0, LOCAL_REGION_BYTES);
  SemanticActivationShmem = (ClusterSemanticActivationShmem *)local_region;
  memcpy(SemanticActivationShmem, &test_collection_gate, sizeof(test_collection_gate));
  local_control = (ClusterSurvivorControl *)(local_region + MAXALIGN(sizeof(*SemanticActivationShmem)));
  *local_control = candidate;
  local_procs[7] = *MyProc; MyProc = &local_procs[7];
  local_proc_header.allProcs = local_procs; local_proc_header.allProcCount = 8;
  ProcGlobal = &local_proc_header;
  m=fixture_mailbox();
  /* A raw REQUEST, not a producer receipt. The old loop receives its latch
   * but never dispatches the actual owner observation. */
  m->local_abi=CLUSTER_SURVIVOR_LOCAL_ABI;
  m->sender_pid=MyProcPid;m->sender_pgprocno=MyProc->pgprocno;
  m->sender_proc_generation=MyProc->cluster_grd_generation;
  m->query.pid=MyProcPid;m->query.pgprocno=MyProc->pgprocno;
  m->query.backend_id=InvalidBackendId;m->query.family=CLUSTER_SURVIVOR_PRODUCER_NATIVE;
  m->query.proc_generation=MyProc->cluster_grd_generation;
  m->query.backend_type=MyBackendType;m->query.aux_type=MyAuxProcType;
  m->query.native_epoch=3;m->query.gate_epoch=3;m->query.record_generation=11;
  m->query.nonce=5;m->query.round_generation=13;m->query.candidate_seq=2;
  memcpy(m->query.basis,message.basis,32);
  pg_atomic_init_u64(&m->word,(UINT64_C(1)<<2)|CLUSTER_SURVIVOR_LOCAL_REQUESTED);
  SetLatch(MyLatch);
  test_producer_seen=0; /* next actual collection has its own fanout */
  return 0;
 }
 m=fixture_mailbox();
 UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_REPLIED);
 UT_ASSERT_EQ(m->reply.result,PENDING);
 UT_ASSERT_EQ(m->reply.identity.pid,MyProcPid);
 UT_ASSERT_EQ(m->reply.identity.backend_id,InvalidBackendId);
 UT_ASSERT_EQ(m->reply.identity.proc_generation,17);
 UT_ASSERT_EQ(m->reply.payload_bytes,1128);
 UT_ASSERT_EQ(m->reply.family,CLUSTER_SURVIVOR_PRODUCER_NATIVE);
 UT_ASSERT_EQ(local_control->header.native_closure,0);
 UT_ASSERT_EQ(local_control->tables[0].header.flags,0);
 local_exercise();
 ShutdownRequestPending=true;
 return 0;
}

static ClusterSurvivorLocalTarget
local_self_target(void)
{
 ClusterSurvivorLocalTarget t = {MyProcPid,MyProc->pgprocno,MyProc->backendId,
  CLUSTER_SURVIVOR_PRODUCER_NATIVE,MyProc->cluster_grd_generation,MyBackendType,MyAuxProcType};
 return t;
}
static uint64 local_read(const uint8 *p,unsigned n)
{ uint64 x=0;for(unsigned i=0;i<n;i++)x|=(uint64)p[i]<<(8*i);return x; }
static void local_call(void)
{ test_producer_seen=0;cluster_semantic_survivor_local_process(); }
static void local_reenter(void)
{ cluster_semantic_survivor_local_process(); }
static void local_cancel_reading(void)
{
 ClusterSurvivorLocalMailbox *m=fixture_mailbox();
 ClusterSurvivorLocalTarget t=local_self_target();
 UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_READING);
 cluster_semantic_survivor_local_invalidate();
 UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
 UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_READING);
}
static void local_throw(void)
{ survivor_poll_barrier_hook=NULL;PG_RE_THROW(); }
static void local_die(void)
{ _exit(7); }
static ClusterSurvivorLocalMailbox local_after_reuse;
static void local_reuse_after_publish(void)
{
 ClusterSurvivorLocalTarget t=local_self_target();ClusterSurvivorLocalReply r;
 local_latch_hook=NULL;
 UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),PENDING);
 UT_ASSERT_EQ(r.payload_bytes,1128);
 UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
 local_after_reuse=*fixture_mailbox();
}
/* The native reaper notifies immediately after waitpid, before later backend
 * cleanup. Keep the old PGPROC visible to exercise exactly that interval. */
static void
local_exited_observation(bool publish)
{
 ClusterSurvivorLocalMailbox *m=fixture_mailbox();
 ClusterSurvivorLocalTarget t=local_self_target();
 ClusterSurvivorLocalReply r;
 uint8 zero[sizeof(r)]={0};
 int pipefd[2],status;pid_t pid;PGPROC *sender=MyProc;char byte=1;
 uint64 generation;unsigned wakes;
 UT_ASSERT_EQ(pipe(pipefd),0);fflush(NULL);pid=fork();UT_ASSERT(pid>=0);
 if(pid==0) {
  close(pipefd[1]);if(read(pipefd[0],&byte,1)!=1)_exit(9);close(pipefd[0]);
  MyProc=&local_procs[6];MyProcPid=getpid();MyBackendType=B_LMON;MyAuxProcType=LmonProcess;
  if(publish)local_call();
  _exit(7);
 }
 close(pipefd[0]);local_procs[6]=*MyProc;local_procs[6].pid=pid;local_procs[6].pgprocno=6;
 local_procs[6].cluster_grd_generation=21;
 t.pid=pid;t.pgprocno=6;t.proc_generation=21;
 UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
 generation=pg_atomic_read_u64(&m->word)>>2;
 UT_ASSERT_EQ(write(pipefd[1],&byte,1),1);close(pipefd[1]);
 UT_ASSERT_EQ(waitpid(pid,&status,0),pid);UT_ASSERT(WIFEXITED(status));UT_ASSERT_EQ(WEXITSTATUS(status),7);
 UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,publish?CLUSTER_SURVIVOR_LOCAL_REPLIED:CLUSTER_SURVIVOR_LOCAL_REQUESTED);
 if(publish)UT_ASSERT_EQ(m->reply.payload_bytes,1128);
 UT_ASSERT_EQ(local_procs[6].pid,pid);UT_ASSERT_EQ(local_procs[6].cluster_grd_generation,21);
 wakes=local_wakes;
 cluster_semantic_survivor_local_postmaster_reap(pid); /* An ordinary child caller cannot publish reap. */
 UT_ASSERT_EQ(pg_atomic_read_u64(&m->reaped_generation),0);UT_ASSERT_EQ(local_wakes,wakes);
 IsUnderPostmaster=false;IsPostmasterEnvironment=true;MyProc=NULL;
 cluster_semantic_survivor_local_postmaster_reap(pid+1);
 UT_ASSERT_EQ(pg_atomic_read_u64(&m->reaped_generation),0);UT_ASSERT_EQ(local_wakes,wakes);
 cluster_semantic_survivor_local_postmaster_reap(pid);
 MyProc=sender;IsUnderPostmaster=true;IsPostmasterEnvironment=false;
 UT_ASSERT_EQ(pg_atomic_read_u64(&m->reaped_generation),generation);UT_ASSERT_EQ(local_wakes,wakes+1);
 memset(&r,0xa5,sizeof(r));
 UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),REFUSED);
 UT_ASSERT_EQ(memcmp(&r,zero,sizeof(r)),0);
 UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
 /* Reaping retires only observation transport, not the old resource owner. */
 UT_ASSERT_EQ(local_procs[6].pid,pid);UT_ASSERT_EQ(local_procs[6].cluster_grd_generation,21);
 UT_ASSERT_EQ(local_control->header.native_closure,0);UT_ASSERT_EQ(local_control->header.ordinary_admission,0);
}
static void
local_exercise(void)
{
 ClusterSurvivorLocalMailbox *m=fixture_mailbox();
 ClusterSurvivorLocalReply r;
 ClusterSurvivorLocalTarget t=local_self_target();
 uint64 word;
 unsigned wakes;
 /* Actual LMON retrieves the actual first native observation. */
 UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),PENDING);
 UT_ASSERT_EQ(r.payload_bytes,1128);
 UT_ASSERT_EQ(local_read(r.body+612,4),25);
 UT_ASSERT_EQ(r.identity.proc_generation,17);
 UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
 switch(local_case) {
 case 0: break;
 case 1:
  wakes=local_wakes;
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
  word=pg_atomic_read_u64(&m->word);
  UT_ASSERT_EQ(local_wakes-wakes,1);
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word),word);
  UT_ASSERT_EQ(local_wakes-wakes,1);
  local_call();
  UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),PENDING);
  UT_ASSERT_EQ(r.payload_bytes,1128);UT_ASSERT_EQ(r.query_generation,2);
  break;
 case 2:
  for(unsigned side=0;side<2;side++)for(unsigned bit=0;bit<64;bit++)
   pg_atomic_write_u32(&SemanticActivationShmem->inflight[side][bit],1+side*64+bit);
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);local_call();
  UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),PENDING);
  for(unsigned side=0;side<2;side++)for(unsigned bit=0;bit<64;bit++)
   UT_ASSERT_EQ(local_read(r.body+80+(side*64+bit)*4,4),1+side*64+bit);
  UT_ASSERT_EQ(local_read(r.body+64,8),11);UT_ASSERT_EQ(local_read(r.body+72,8),3);
  for(unsigned i=1128;i<sizeof(r.body);i++)UT_ASSERT_EQ(r.body[i],0);
  for(unsigned i=0;i<sizeof(m->reserved);i++)UT_ASSERT_EQ(m->reserved[i],0);
  for(unsigned i=2048;i<sizeof(local_control->reserved);i++)UT_ASSERT_EQ(local_control->reserved[i],0);
  break;
 case 3:
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
  survivor_poll_barrier_hook=local_reenter;local_call();survivor_poll_barrier_hook=NULL;
  UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),PENDING);UT_ASSERT_EQ(r.payload_bytes,1128);
  break;
 case 4:
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
  survivor_poll_barrier_hook=local_cancel_reading;local_call();survivor_poll_barrier_hook=NULL;
  UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),STALE);UT_ASSERT_EQ(r.payload_bytes,0);
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
  break;
 case 5:
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
  cluster_semantic_survivor_local_invalidate();
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);local_call();
  UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),PENDING);UT_ASSERT_EQ(r.query_generation,3);
  break;
 case 6:
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
  pg_atomic_write_u64(&SemanticActivationShmem->record_generation,12);
  UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),STALE);UT_ASSERT_EQ(r.payload_bytes,0);
  pg_atomic_write_u64(&SemanticActivationShmem->record_generation,11);
  break;
 case 7:
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);local_call();
  local_control->header.round_generation++;
  UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),STALE);UT_ASSERT_EQ(r.payload_bytes,0);
  break;
 case 8:
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
  local_latch_hook=local_reuse_after_publish;local_call();
  UT_ASSERT_EQ(memcmp(m,&local_after_reuse,sizeof(*m)),0);
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_REQUESTED);
  cluster_semantic_survivor_local_invalidate();
  break;
 case 9: {
  volatile bool caught=false;
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
  survivor_poll_barrier_hook=local_throw;
  PG_TRY();{local_call();}PG_CATCH();{caught=true;}PG_END_TRY();
  survivor_poll_barrier_hook=NULL;UT_ASSERT(caught);
  UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),REFUSED);UT_ASSERT_EQ(r.payload_bytes,0);
  break;
 }
 case 10: {
  int pipefd[2],status;pid_t pid;PGPROC *sender=MyProc;char byte=1;
  UT_ASSERT_EQ(pipe(pipefd),0);
  fflush(NULL);pid=fork();UT_ASSERT(pid>=0);
  if(pid==0) {
   close(pipefd[1]);if(read(pipefd[0],&byte,1)!=1)_exit(9);close(pipefd[0]);
   MyProc=&local_procs[6];MyProcPid=getpid();MyBackendType=B_LMON;MyAuxProcType=LmonProcess;
   survivor_poll_barrier_hook=local_die;local_call();_exit(8);
  }
  close(pipefd[0]);local_procs[6]=*MyProc;local_procs[6].pid=pid;local_procs[6].pgprocno=6;
  local_procs[6].cluster_grd_generation=21;
  t.pid=pid;t.pgprocno=6;t.proc_generation=21;t.backend_type=B_LMON;t.aux_type=LmonProcess;
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
  UT_ASSERT_EQ(write(pipefd[1],&byte,1),1);close(pipefd[1]);
  UT_ASSERT_EQ(waitpid(pid,&status,0),pid);UT_ASSERT(WIFEXITED(status));UT_ASSERT_EQ(WEXITSTATUS(status),7);
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_READING);
  local_procs[6].pid=0;local_procs[6].cluster_grd_generation=22;
  UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),STALE);
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_READING);
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
  cluster_semantic_survivor_local_postmaster_reap(pid); /* child caller refuses */
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->reaped_generation),0);
  IsUnderPostmaster=false;IsPostmasterEnvironment=true;MyProc=NULL;
  cluster_semantic_survivor_local_postmaster_reap(pid+1);
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->reaped_generation),0);
  cluster_semantic_survivor_local_postmaster_reap(pid);
  MyProc=sender;IsUnderPostmaster=true;IsPostmasterEnvironment=false;
  UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),REFUSED);
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
  UT_ASSERT_EQ(r.payload_bytes,0);
  break;
 }
 case 11:
  for(unsigned field=0;field<8;field++) {
   ClusterSurvivorLocalTarget bad=t;
   switch(field){case 0:bad.pid++;break;case 1:bad.pgprocno=-1;break;
	case 2:bad.backend_id=0;break;case 3:bad.proc_generation++;break;
	case 4:bad.family=0;break;case 5:bad.backend_type=B_BACKEND;break;
	case 6:bad.aux_type=NotAnAuxProcess;break;case 7:bad.pgprocno=8;break;}
   UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&bad),REFUSED);
  }
  break;
 case 12:
  pg_atomic_write_u64(&m->word,CLUSTER_SURVIVOR_LOCAL_MAX_GENERATION<<2);
  wakes=local_wakes;UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),REFUSED);
  UT_ASSERT_EQ(local_wakes,wakes);
  break;
 case 13: {
  uint8 signal_region[offsetof(ProcSignalHeader,psh_slot)+2*sizeof(ProcSignalSlot)]={0};
  ProcSignal=(ProcSignalHeader *)signal_region;MyProcSignalSlot=&ProcSignal->psh_slot[1];
  MyProcSignalSlot->pss_pid=9182;InterruptPending=false;
  wakes=local_wakes;local_notifies=0;local_kill_result=0;
  UT_ASSERT_EQ(SendProcSignal(9182,PROCSIG_CLUSTER_SURVIVOR_OBSERVE,2),0);
  UT_ASSERT_EQ(local_notifies,1);UT_ASSERT_EQ(local_notify_pid,9182);
  UT_ASSERT(!InterruptPending);procsignal_sigusr1_handler(SIGUSR1);
  UT_ASSERT(InterruptPending);UT_ASSERT_EQ(local_wakes-wakes,1);
  UT_ASSERT_EQ(MyProcSignalSlot->pss_signalFlags[PROCSIG_CLUSTER_SURVIVOR_OBSERVE],0);
  UT_ASSERT_EQ(SendProcSignal(9183,PROCSIG_CLUSTER_SURVIVOR_OBSERVE,2),-1);
  UT_ASSERT_EQ(local_notifies,1);ProcSignal=NULL;MyProcSignalSlot=NULL;
  break;
 }
 case 14:
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
  proc_exit_inprogress=true;local_call();proc_exit_inprogress=false;
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_REQUESTED);
  cluster_semantic_survivor_local_invalidate();
  break;
 case 15:
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
  test_stop_lock_depth=1;local_call();test_stop_lock_depth=0;
  UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),REFUSED);UT_ASSERT_EQ(r.payload_bytes,0);
  break;
 case 16: {
  Size bytes=offsetof(ProcSignalHeader,psh_slot)+(MaxBackends+1)*sizeof(ProcSignalSlot);
  ProcSignal=calloc(1,bytes);UT_ASSERT(ProcSignal!=NULL);
  local_procs[6]=*MyProc;local_procs[6].pid=MyProcPid+10;local_procs[6].pgprocno=6;
  local_procs[6].backendId=MaxBackends+1;
  t.pid=local_procs[6].pid;t.pgprocno=6;t.backend_id=MaxBackends+1;
  t.family=CLUSTER_SURVIVOR_PRODUCER_BACKEND;t.backend_type=B_BACKEND;t.aux_type=NotAnAuxProcess;
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),REFUSED);
  free(ProcSignal);ProcSignal=NULL;break;
 }
 case 17:
  test_stop_lock_depth=1;
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),REFUSED);
  test_stop_lock_depth=0;cluster_semantic_survivor_local_invalidate();break;
 case 18:
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
  m->query.reserved[0]=1;local_call();
  UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),REFUSED);
  UT_ASSERT_EQ(r.payload_bytes,0);break;

 case 19:
  for(unsigned bad=0;bad<2;bad++) {
   UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),PENDING);
   if(bad)m->local_abi++;else m->flags=1;
   local_call();
   UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),REFUSED);
   UT_ASSERT_EQ(r.payload_bytes,0);
   UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
   m->local_abi=CLUSTER_SURVIVOR_LOCAL_ABI;m->flags=0;
  }
  break;
 case 20: {
  typedef struct {
   volatile pid_t pid;volatile sig_atomic_t flags[NUM_PROCSIGNALS-1];
   pg_atomic_uint64 generation;pg_atomic_uint32 mask;ConditionVariable cv;
  } PreviousSlot;
  UT_ASSERT_EQ(sizeof(PreviousSlot),sizeof(ProcSignalSlot));
  UT_ASSERT_EQ(offsetof(PreviousSlot,generation),offsetof(ProcSignalSlot,pss_barrierGeneration));
  UT_ASSERT_EQ(sizeof(ClusterSurvivorLocalMailbox),2048);
  UT_ASSERT_EQ(sizeof(ClusterSurvivorControl),65536);
  break;
 }

 case 21:
  for(unsigned field=0;field<5;field++) {
   if(field==0)local_control->header.nonce++;
   if(field==1)local_control->header.round_generation++;
   if(field==2)local_control->header.ordinary_admission=1;
   if(field==3)local_control->header.native_closure=1;
   if(field==4)local_control->header.typed_status=1;
   UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t),REFUSED);
   local_control->header=candidate.header;
   cluster_semantic_survivor_local_invalidate();
  }
  break;

 case 22: local_exited_observation(false); break;
 case 23: local_exited_observation(true); break;
 case 24:
  /* Malformed zero query generation is not a real waitpid witness. */
  pg_atomic_write_u64(&m->word,CLUSTER_SURVIVOR_LOCAL_READING);
  pg_atomic_write_u64(&m->reaped_generation,0);
  memset(&r,0xa5,sizeof(r));
  UT_ASSERT_EQ(cluster_semantic_survivor_local_poll(&r),PENDING);
  UT_ASSERT_EQ(r.payload_bytes,0);
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_READING);
  break;

 }
 if(local_case==7)local_control->header.round_generation--;
 UT_ASSERT_EQ(memcmp(local_control,&candidate,60704),0);
}
static void run_local(unsigned which)
{
 PGPROC *saved=MyProc;PROC_HDR *saved_header=ProcGlobal;
 local_region=mmap(NULL,LOCAL_REGION_BYTES,PROT_READ|PROT_WRITE,MAP_SHARED|MAP_ANON,-1,0);
 local_procs=mmap(NULL,8*sizeof(PGPROC),PROT_READ|PROT_WRITE,MAP_SHARED|MAP_ANON,-1,0);
 UT_ASSERT(local_region!=MAP_FAILED);UT_ASSERT(local_procs!=MAP_FAILED);
 test_collection_kind=4;test_collection_extra_wait=local_wait;test_collection_on=true;
 test_producer_module=0;local_waits=0;local_case=which;local_latch_hook=NULL;
 test_run_normal_stop_lmon(true,60);
 test_collection_on=false;test_collection_extra_wait=NULL;
 MyProc=saved;ProcGlobal=saved_header;SemanticActivationShmem=NULL;
 UT_ASSERT_EQ(local_waits,2);
 UT_ASSERT_EQ(munmap(local_region,LOCAL_REGION_BYTES),0);
 UT_ASSERT_EQ(munmap(local_procs,8*sizeof(PGPROC)),0);
}
UT_TEST(original_lmon_latch_return_delivers_only_unconfirmed_native_observation) {run_local(0);}
UT_TEST(real_query_is_single_target_and_busy_never_resignals) {run_local(1);}
UT_TEST(all_128_actual_gate_cells_project_without_pointers_or_tail_changes) {run_local(2);}
UT_TEST(reentrant_notification_cannot_claim_twice) {run_local(3);}
UT_TEST(invalidation_preserves_claimed_callback_until_publication) {run_local(4);}
UT_TEST(cancelled_unclaimed_query_cannot_publish_into_reused_generation) {run_local(5);}
UT_TEST(gate_drift_withdraws_request_without_clearing_debt) {run_local(6);}
UT_TEST(round_drift_rejects_already_published_old_observation) {run_local(7);}
UT_TEST(published_callback_never_writes_after_lmon_reuses_slot) {run_local(8);}
UT_TEST(original_error_is_rethrown_after_refused_observation) {run_local(9);}
UT_TEST(only_actual_waitpid_retires_dead_reading_callback) {run_local(10);}
UT_TEST(all_target_identity_fields_and_signed_aux_shape_are_checked) {run_local(11);}
UT_TEST(query_generation_never_wraps_or_notifies_after_exhaustion) {run_local(12);}
UT_TEST(actual_sigusr1_reason_is_independent_and_sets_original_interrupt) {run_local(13);}
UT_TEST(exiting_original_owner_does_not_claim_request) {run_local(14);}
UT_TEST(held_native_lock_refuses_sample_without_changing_original_work) {run_local(15);}
UT_TEST(backend_signal_index_must_be_inside_actual_backend_slots) {run_local(16);}
UT_TEST(lmon_cannot_publish_a_query_from_a_held_lock) {run_local(17);}
UT_TEST(unassigned_query_bytes_cannot_supply_a_usable_sample) {run_local(18);}
UT_TEST(unknown_local_header_cannot_supply_a_sample) {run_local(19);}
UT_TEST(original_signal_slot_and_control_sizes_are_exact) {run_local(20);}
UT_TEST(unconfirmed_scope_cannot_mix_rounds_or_claim_authority) {run_local(21);}
UT_TEST(actual_reap_retires_unclaimed_query_before_pgproc_cleanup) {run_local(22);}
UT_TEST(actual_reap_withdraws_published_sample_before_pgproc_cleanup) {run_local(23);}
UT_TEST(zero_reap_sentinel_cannot_retire_a_reading_callback) {run_local(24);}
#ifndef SURVIVOR_TARGET_FIXTURE
int main(void)
{
 UT_PLAN(25);
 UT_RUN(original_lmon_latch_return_delivers_only_unconfirmed_native_observation);
 UT_RUN(real_query_is_single_target_and_busy_never_resignals);
 UT_RUN(all_128_actual_gate_cells_project_without_pointers_or_tail_changes);
 UT_RUN(reentrant_notification_cannot_claim_twice);
 UT_RUN(invalidation_preserves_claimed_callback_until_publication);
 UT_RUN(cancelled_unclaimed_query_cannot_publish_into_reused_generation);
 UT_RUN(gate_drift_withdraws_request_without_clearing_debt);
 UT_RUN(round_drift_rejects_already_published_old_observation);
 UT_RUN(published_callback_never_writes_after_lmon_reuses_slot);
 UT_RUN(original_error_is_rethrown_after_refused_observation);
 UT_RUN(only_actual_waitpid_retires_dead_reading_callback);
 UT_RUN(all_target_identity_fields_and_signed_aux_shape_are_checked);
 UT_RUN(query_generation_never_wraps_or_notifies_after_exhaustion);
 UT_RUN(actual_sigusr1_reason_is_independent_and_sets_original_interrupt);
 UT_RUN(exiting_original_owner_does_not_claim_request);
 UT_RUN(held_native_lock_refuses_sample_without_changing_original_work);
 UT_RUN(backend_signal_index_must_be_inside_actual_backend_slots);
 UT_RUN(lmon_cannot_publish_a_query_from_a_held_lock);
 UT_RUN(unassigned_query_bytes_cannot_supply_a_usable_sample);
 UT_RUN(unknown_local_header_cannot_supply_a_sample);
 UT_RUN(original_signal_slot_and_control_sizes_are_exact);
 UT_RUN(unconfirmed_scope_cannot_mix_rounds_or_claim_authority);
 UT_RUN(actual_reap_retires_unclaimed_query_before_pgproc_cleanup);
 UT_RUN(actual_reap_withdraws_published_sample_before_pgproc_cleanup);
 UT_RUN(zero_reap_sentinel_cannot_retire_a_reading_callback);
 UT_DONE();return ut_failed_count?1:0;
}

#endif /* SURVIVOR_TARGET_FIXTURE */
