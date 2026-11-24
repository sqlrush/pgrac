/* Close-to-owner collection and limited original backend return facts.
 * The real LmonMain, event publisher, gate, target selection and local
 * transport run here. PGPROC/status, module completion and OS wakeups are
 * inherited host boundaries; this is neither a receipt nor native evidence. */
#define FreeWaitEventSet scope_legacy_FreeWaitEventSet
#define SURVIVOR_CLOSE_SCOPE_FIXTURE
#include "test_cluster_survivor_targets.c"
#undef FreeWaitEventSet
/* Fresh host wait-set lifetime boundary. The inherited fake set is not heap
 * allocated. Keep its depth/identity checks; do not import another test
 * scenario's requirement that a local producer query ran six module polls.
 * That complete old assertion/function remains in the renamed base. */
void FreeWaitEventSet(WaitEventSet *set);
void FreeWaitEventSet(WaitEventSet *set)
{
 UT_ASSERT_EQ(set, (WaitEventSet *)(uintptr_t)1);
 UT_ASSERT_EQ(cl_normal_stop_service_depth, 0);
 test_stop_frees++;
}
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_semantic_survivor_close.h"
static ClusterReconfigState scope_reconfig;
static ClusterReconfigState *ReconfigShmem = &scope_reconfig;
void cluster_sinval_reset_all_on_reconfig(void) {}
void cluster_write_fence_authority_cache_invalidate(void) {}
#include "test_cluster_survivor_close_event.inc"
static void scope_unexpected_cleanup(void) { UT_ASSERT(false); }
static int scope_unexpected_wait(Latch *l, int e, long t, uint32 i)
{
 (void)l; (void)e; (void)t; (void)i; UT_ASSERT(false); return 0;
}
#define cluster_cf_retirement_poll scope_unexpected_cleanup
#define cluster_lock_owners_service_poll scope_unexpected_cleanup
#define WaitLatch scope_unexpected_wait
#include "../../backend/cluster/cluster_semantic_survivor_close.c"
#undef WaitLatch
#undef cluster_cf_retirement_poll
#undef cluster_lock_owners_service_poll

static unsigned scope_case, scope_waits;
static uint8 scope_signal_bytes[offsetof(ProcSignalHeader,psh_slot)+sizeof(ProcSignalSlot)];
static ClusterSemanticActivationShmem scope_gate_before;
static ClusterSurvivorLocalReply scope_native_reply;
static void scope_native_capture(void)
{
 ClusterSurvivorLocalMailbox *m = fixture_mailbox();
 if ((pg_atomic_read_u64(&m->word) & 3) == CLUSTER_SURVIVOR_LOCAL_REPLIED)
  scope_native_reply = m->reply;
}
static int scope_loop_wait(WaitEvent *events)
{
 ClusterSurvivorLocalMailbox *m;
 ClusterSurvivorCloseSnapshot sample;
 (void)events;
 if (++scope_waits == 1) {
  ReconfigEvent event = {0};
  /* Fresh host shmem, before publication. No candidate is constructed,
   * copied or cleared after close. The original OPEN baseline is untouched. */
  memset(local_region, 0, LOCAL_REGION_BYTES);
  SemanticActivationShmem = (ClusterSemanticActivationShmem *)local_region;
  memcpy(SemanticActivationShmem, &test_collection_gate, sizeof(test_collection_gate));
  local_control = (ClusterSurvivorControl *)(local_region + MAXALIGN(sizeof(*SemanticActivationShmem)));
  local_procs[7] = *MyProc;
  MyProc = &local_procs[7];
  local_proc_header.allProcs = local_procs;
  local_proc_header.allProcCount = 8;
  ProcGlobal = &local_proc_header;
  memset(target_status, 0, sizeof(target_status));
  target_status[MaxBackends + LmonProcess].st_changecount = 2;
  target_status[MaxBackends + LmonProcess].st_procpid = MyProcPid;
  target_status[MaxBackends + LmonProcess].st_backendType = B_LMON;
  local_procs[6].pid = 4242;
  local_procs[6].pgprocno = 6;
  local_procs[6].backendId = 1;
  local_procs[6].cluster_grd_generation = 16;
  target_status[0].st_changecount = 2;
  target_status[0].st_procpid = 4242;
  target_status[0].st_backendType = B_BACKEND;
  memset(scope_signal_bytes, 0, sizeof(scope_signal_bytes));
  ProcSignal = (ProcSignalHeader *)scope_signal_bytes;
  ProcSignal->psh_slot[0].pss_pid = 4242; local_kill_result = 0;
  if (scope_case == 3) {
   local_procs[6].pid = 0; memset(&target_status[0], 0, sizeof(target_status[0]));
   memset(&scope_native_reply, 0, sizeof(scope_native_reply)); local_latch_hook = scope_native_capture;
  }
  pg_atomic_init_u64(&target_grd.next_generation, 32);
  memset(&scope_reconfig, 0, sizeof(scope_reconfig));
  event.event_id = 19; event.old_epoch = 2; event.new_epoch = 3;
  event.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
  event.observer_role = CLUSTER_RECONFIG_OBSERVER_COORDINATOR;
  event.dead_bitmap[0] = 2;
  cluster_reconfig_publish_event(&event);
  scope_gate_before = *SemanticActivationShmem;
  return 0;
 }
 UT_ASSERT_EQ(cluster_semantic_survivor_close_observe(&sample), CLOSE_PENDING);
 UT_ASSERT(sample.blocked);
 UT_ASSERT_EQ(sample.record.state, CLUSTER_SURVIVOR_CLOSE_CLOSING);
 UT_ASSERT_EQ(sample.record.generation, 1);
 UT_ASSERT_EQ(sample.record.event_id, 19);
 UT_ASSERT_EQ(memcmp(&scope_gate_before, SemanticActivationShmem, sizeof(scope_gate_before)), 0);
 UT_ASSERT_EQ(pg_atomic_read_u32(&SemanticActivationShmem->transition_closed), 0);
 for (unsigned i = 0; i < 60704; i++)
  UT_ASSERT_EQ(((uint8 *)local_control)[i], 0);
 if (scope_waits == 2) return 0; /* one more actual LMON turn */
 m = fixture_mailbox();
 if (scope_case == 1) {
  ClusterSurvivorLocalTarget t = {0};
  UT_ASSERT(targets_read(6, &t));
  UT_ASSERT_EQ(t.pid, 4242);
  UT_ASSERT_EQ(t.backend_id, 1);
  /* Retire the still-unclaimed close query by the original exact CAS.
   * The old candidate API must still reject a close-only scope. */
  UT_ASSERT_EQ(m->local_abi, 2);
  cluster_semantic_survivor_local_invalidate();
  UT_ASSERT_EQ(cluster_semantic_survivor_local_begin(&t), REFUSED);
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word) & 3, CLUSTER_SURVIVOR_LOCAL_EMPTY);
 } else if (scope_case == 2) {
  /* Proposal's caller-visible requirement. Baseline has no query, despite
   * a valid close and an original target. No missing-symbol RED. */
  printf("# scope cut: close=%llu event=%llu old_gate_closed=%u query_generation=%llu\n",
   (unsigned long long)sample.record.generation,
   (unsigned long long)sample.record.event_id,
   pg_atomic_read_u32(&SemanticActivationShmem->transition_closed),
   (unsigned long long)(pg_atomic_read_u64(&m->word) >> 2));
  UT_ASSERT_NE(pg_atomic_read_u64(&m->word) >> 2, 0);
  if ((pg_atomic_read_u64(&m->word) >> 2) != 0) {
   UT_ASSERT_EQ(m->query.pid, 4242);
   UT_ASSERT_EQ(m->query.pgprocno, 6);
   UT_ASSERT_EQ(m->query.proc_generation, 16);
   UT_ASSERT_EQ(m->query.backend_id, 1);
  }
 } else if (scope_case == 3) {
  UT_ASSERT_EQ(scope_native_reply.family, CLUSTER_SURVIVOR_PRODUCER_NATIVE);
  UT_ASSERT_EQ(scope_native_reply.result, LOCAL_PENDING);
  UT_ASSERT_EQ(scope_native_reply.payload_bytes, 1256);
  UT_ASSERT_EQ(local_get(scope_native_reply.body + 1136, 8), 0);
  UT_ASSERT_EQ(local_get(scope_native_reply.body + 1208, 4), 0);
  UT_ASSERT_EQ(local_control->header.native_closure, 0);
 }
 ShutdownRequestPending = true;
 return 0;
}
static void run_scope(unsigned which)
{
 PGPROC *saved = MyProc;
 PROC_HDR *header = ProcGlobal;
 local_region = mmap(NULL, LOCAL_REGION_BYTES, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
 local_procs = mmap(NULL, 8*sizeof(PGPROC), PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANON, -1, 0);
 UT_ASSERT(local_region != MAP_FAILED); UT_ASSERT(local_procs != MAP_FAILED);
 scope_case = which; scope_waits = 0;
 test_collection_kind = 4; test_collection_extra_wait = scope_loop_wait;
 test_collection_on = true; test_producer_module = 0; test_native_epoch = 3;
 status_changes = status_reads = 0; local_latch_hook = NULL;
 test_run_normal_stop_lmon(true, 60);
 test_collection_on = false; test_collection_extra_wait = NULL;
 MyProc = saved; ProcGlobal = header; SemanticActivationShmem = NULL; ProcSignal = NULL; local_latch_hook = NULL;
 UT_ASSERT_EQ(scope_waits, 3);
 UT_ASSERT_EQ(munmap(local_region, LOCAL_REGION_BYTES), 0);
 UT_ASSERT_EQ(munmap(local_procs, 8*sizeof(PGPROC)), 0);
}
UT_TEST(real_close_installs_without_candidate_or_old_gate_mutation) { run_scope(0); }
UT_TEST(original_candidate_query_refuses_close_only_scope) { run_scope(1); }
UT_TEST(real_lmon_close_must_request_original_backend_without_candidate) { run_scope(2); }
UT_TEST(real_close_native_observation_has_no_ordinary_return_fact) { run_scope(3); }
int main(void)
{
 UT_PLAN(4);
 UT_RUN(real_close_installs_without_candidate_or_old_gate_mutation);
 UT_RUN(original_candidate_query_refuses_close_only_scope);
 UT_RUN(real_lmon_close_must_request_original_backend_without_candidate);
 UT_RUN(real_close_native_observation_has_no_ordinary_return_fact);
 UT_DONE(); return ut_failed_count ? 1 : 0;
}
