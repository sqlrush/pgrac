/* Owner-side projection uses raw untrusted query inputs by default. The
 * optional backend route adds original query construction and dispatch;
 * process allocation and OS delivery remain host boundaries. Actual
 * backend/input producer bodies run in their native-source fixtures. */
#include "storage/procsignal.h"
#include "cluster/cluster_semantic_survivor_candidate.h"
#include "cluster/cluster_semantic_survivor_local.h"
static ClusterSurvivorControl survivor_local_owner_storage;
ClusterSurvivorControl *cluster_semantic_survivor_shared_control(void)
{ return &survivor_local_owner_storage; }
bool IsPostmasterEnvironment;
int MaxBackends=100;
#ifdef SURVIVOR_LOCAL_BACKEND_ROUTE
#include "test_cluster_survivor_backend_route_native.h"
#elif defined(SURVIVOR_LOCAL_INPUT_ROUTE)
#include "test_cluster_survivor_input_route_native.h"
#else
/* Owner-only fixtures never issue LMON queries. Reaching its scope API is
 * a test error, never a replacement positive scope or receipt. */
bool cluster_semantic_survivor_candidate_scope(ClusterSurvivorControl *c,
 ClusterSurvivorProducerScope *scope,uint8 basis[32])
{ (void)c;(void)scope;(void)basis;abort(); }
int SendProcSignal(pid_t pid,ProcSignalReason reason,BackendId id)
{ (void)pid;(void)reason;(void)id;abort(); }
#ifndef SURVIVOR_LOCAL_INPUT_FIXTURE
void SetLatch(Latch *latch) { (void)latch;abort(); }
#endif
#endif
#pragma push_macro("proc")
#undef proc
#pragma push_macro("SetLatch")
#ifdef SURVIVOR_LOCAL_INPUT_ROUTE
#define SetLatch input_route_set_latch
#endif
#include "../../backend/cluster/cluster_semantic_survivor_local.c"
#pragma pop_macro("SetLatch")
#pragma pop_macro("proc")
static uint64 survivor_local_scalar(const uint8 *p,unsigned n)
{ uint64 x=0;for(unsigned i=0;i<n;i++)x|=(uint64)p[i]<<(8*i);return x; }
static ClusterSurvivorLocalReply
survivor_local_owner_sample(unsigned family, uint64 epoch,uint64 gate,uint64 generation)
{
 ClusterSurvivorLocalMailbox *m=(ClusterSurvivorLocalMailbox *)survivor_local_owner_storage.reserved;
 ClusterSurvivorLocalReply r;
 memset(&survivor_local_owner_storage,0,sizeof(survivor_local_owner_storage));
 pg_atomic_init_u64(&m->word,5); /* g=1 REQUESTED; untrusted test input */
 m->local_abi=CLUSTER_SURVIVOR_LOCAL_ABI;
 m->sender_pid=MyProcPid+1;m->sender_pgprocno=-1;m->sender_proc_generation=1;
 m->query.pid=MyProcPid;m->query.pgprocno=MyProc->pgprocno;
 m->query.backend_id=MyProc->backendId;m->query.proc_generation=MyProc->cluster_grd_generation;
 m->query.backend_type=MyBackendType;m->query.aux_type=MyAuxProcType;m->query.family=family;
 m->query.native_epoch=epoch;m->query.gate_epoch=gate;m->query.record_generation=generation;
 m->query.nonce=5;m->query.round_generation=13;m->query.candidate_seq=2;
 cluster_semantic_survivor_local_process();
 UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_REPLIED);
 r=m->reply;
 UT_ASSERT_EQ(r.result,CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
 UT_ASSERT_EQ(r.identity.pid,MyProcPid);UT_ASSERT_EQ(r.identity.backend_id,MyProc->backendId);
 UT_ASSERT_EQ(r.payload_bytes,1128);
 UT_ASSERT_EQ(survivor_local_owner_storage.header.native_closure,0);
 /* Callback has published and returned: test-only raw-input storage can retire. */
 pg_atomic_write_u64(&m->word,4);
 return r;
}
