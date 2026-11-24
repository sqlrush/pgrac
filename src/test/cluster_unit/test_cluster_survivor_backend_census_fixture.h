/* Original target cursor through actual backend observations. Process tables,
 * role scheduling and OS delivery stay host boundaries; counters are never
 * closure. LmonMain/PostgresMain and foreign-family producers do not run here.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_grd.h"
#include <sys/wait.h>
static ClusterGrdShared census_grd;
static ClusterGrdShared *cluster_grd_state=&census_grd;
#include "test_cluster_survivor_generation.inc"

static ClusterSurvivorTargetCensus *census_cursor(void)
{
 return (ClusterSurvivorTargetCensus *)(survivor_local_owner_storage.reserved+CLUSTER_SURVIVOR_LOCAL_BYTES);
}
static Snapshot census_prepare(void)
{
 Snapshot held=route_prepare();
 pg_atomic_init_u64(&census_grd.next_generation,32);
 return held;
}
static void census_tick(void)
{
 route_lmon();cluster_semantic_survivor_targets_tick();
}
static void census_begin(void)
{
 census_tick();
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_WAIT);
 UT_ASSERT_EQ(census_cursor()->cursor,0);UT_ASSERT_EQ(census_cursor()->limit,2);
 UT_ASSERT_EQ(census_cursor()->birth_cut,32);UT_ASSERT_EQ(census_cursor()->attempts,1);
 UT_ASSERT_EQ(route_mailbox()->query.pid,1234);UT_ASSERT_EQ(route_mailbox()->query.proc_generation,11);
 UT_ASSERT_EQ(route_mailbox()->query.backend_id,1);UT_ASSERT_EQ(local_notifies,1);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_REQUESTED);
}
static void census_unqualified(void)
{
 UT_ASSERT_EQ(survivor_local_owner_storage.header.native_closure,0);
 UT_ASSERT_EQ(survivor_local_owner_storage.header.ordinary_admission,0);
 UT_ASSERT_EQ(survivor_local_owner_storage.header.typed_status,0);
 for(unsigned i=CLUSTER_SURVIVOR_LOCAL_BYTES+sizeof(ClusterSurvivorTargetCensus);
	i<sizeof(survivor_local_owner_storage.reserved);i++)
  UT_ASSERT_EQ(survivor_local_owner_storage.reserved[i],0);
}
UT_TEST(backend_census_observes_original_callback_without_retiring_snapshot)
{
 Snapshot held=census_prepare();ClusterSurvivorTargetCensus saved;
 census_begin();saved=*census_cursor();
 for(unsigned i=0;i<8;i++)census_tick();
 UT_ASSERT_EQ(memcmp(&saved,census_cursor(),sizeof(saved)),0);UT_ASSERT_EQ(local_notifies,1);
 route_deliver();UT_ASSERT_EQ(route_mailbox()->reply.payload_bytes,1128);
 UT_ASSERT_EQ(survivor_local_scalar(route_mailbox()->reply.body+40,4),CLUSTER_SERVICE_SNAPSHOT_REGISTERED);
 census_tick();UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_DONE);
 UT_ASSERT_EQ(census_cursor()->observed,1);UT_ASSERT_EQ(census_cursor()->unknown,1);
 UT_ASSERT_EQ(census_cursor()->refused,0);UT_ASSERT_EQ(census_cursor()->attempts,1);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
 UT_ASSERT_EQ(held->regd_count,1);UT_ASSERT_EQ(remembered,1);
 saved=*census_cursor();for(unsigned i=0;i<8;i++)census_tick();
 UT_ASSERT_EQ(memcmp(&saved,census_cursor(),sizeof(saved)),0);
 UT_ASSERT_EQ(local_notifies,1);census_unqualified();route_clear(held);
}
UT_TEST(backend_census_requires_original_gate_close_before_query)
{
 Snapshot held=census_prepare();pg_atomic_write_u32(&backend_return_gate.transition_closed,0);
 census_tick();UT_ASSERT_EQ(census_cursor()->phase,0);UT_ASSERT_EQ(local_notifies,0);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word),0);
 pg_atomic_write_u32(&backend_return_gate.transition_closed,1);census_begin();
 census_unqualified();route_clear(held);
}
UT_TEST(backend_census_waits_for_original_cancel_return_before_advancing)
{
 Snapshot held=census_prepare();volatile bool caught=false;uint64 generation;
 census_begin();generation=census_cursor()->query_generation;
 route_backend();QueryCancelPending=true;procsignal_sigusr1_handler(SIGUSR1);
 PG_TRY();{ProcessInterrupts();}PG_CATCH();{caught=true;}PG_END_TRY();
 UT_ASSERT(caught);UT_ASSERT_EQ(error_code,ERRCODE_QUERY_CANCELED);census_tick();
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_WAIT);
 UT_ASSERT_EQ(census_cursor()->query_generation,generation);UT_ASSERT_EQ(census_cursor()->observed,0);
 UT_ASSERT_EQ(local_notifies,1);UT_ASSERT_EQ(held->regd_count,1);
 route_backend();InterruptPending=true;ProcessInterrupts();census_tick();
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_DONE);
 UT_ASSERT_EQ(census_cursor()->observed,1);UT_ASSERT_EQ(census_cursor()->attempts,1);
 UT_ASSERT_EQ(local_notifies,1);census_unqualified();route_clear(held);
}
UT_TEST(backend_census_failed_notification_cannot_count_as_observation)
{
 Snapshot held=census_prepare();local_kill_result=-1;census_tick();
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_DONE);
 UT_ASSERT_EQ(census_cursor()->refused,1);UT_ASSERT_EQ(census_cursor()->unknown,1);
 UT_ASSERT_EQ(census_cursor()->observed,0);UT_ASSERT_EQ(census_cursor()->attempts,0);
 UT_ASSERT_EQ(local_notifies,1);UT_ASSERT_EQ(held->regd_count,1);
 route_deliver();census_tick();UT_ASSERT_EQ(census_cursor()->observed,0);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
 census_unqualified();route_clear(held);
}
UT_TEST(backend_census_unknown_status_is_not_queried_or_rescanned_in_same_round)
{
 Snapshot held=census_prepare();ClusterSurvivorTargetCensus saved;
 route_status[0].st_changecount=3;census_tick();
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_DONE);
 UT_ASSERT_EQ(census_cursor()->unknown,2);UT_ASSERT_EQ(census_cursor()->attempts,0);
 UT_ASSERT_EQ(local_notifies,0);saved=*census_cursor();
 route_status[0].st_changecount=2;for(unsigned i=0;i<8;i++)census_tick();
 UT_ASSERT_EQ(memcmp(&saved,census_cursor(),sizeof(saved)),0);
 UT_ASSERT_EQ(local_notifies,0);census_unqualified();route_clear(held);
}

static unsigned census_drift_kind,census_drift_calls;
static void census_claimed_drift(void)
{
 ClusterSurvivorTargetCensus before=*census_cursor();
 survivor_poll_barrier_hook=NULL;census_drift_calls++;
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_READING);
 if(census_drift_kind==0) {
  proc.cluster_grd_generation=33;pg_atomic_write_u64(&census_grd.next_generation,34);
 } else {
  survivor_local_owner_storage.header.round_generation++;
  survivor_local_owner_storage.tables[0].header.round_generation++;
 }
 census_tick();
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_READING);
 UT_ASSERT_EQ(census_cursor()->phase,census_drift_kind==0?CLUSTER_SURVIVOR_TARGETS_WAIT:CLUSTER_SURVIVOR_TARGETS_STALE);
 UT_ASSERT_EQ(census_cursor()->query_generation,before.query_generation);
 UT_ASSERT_EQ(census_cursor()->attempts,before.attempts);UT_ASSERT_EQ(local_notifies,1);
 UT_ASSERT_EQ(census_cursor()->observed,0);UT_ASSERT_EQ(remembered,1);
 route_backend();
}
static void census_claimed_round(unsigned kind)
{
 Snapshot held=census_prepare();uint64 generation;
 census_begin();generation=census_cursor()->query_generation;
 census_drift_kind=kind;census_drift_calls=0;survivor_poll_barrier_hook=census_claimed_drift;
 route_deliver();UT_ASSERT_EQ(census_drift_calls,1);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_REPLIED);
 census_tick();UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_WAIT);
 UT_ASSERT_EQ(census_cursor()->cursor,0);UT_ASSERT_EQ(census_cursor()->query_generation,generation+1);
 UT_ASSERT_EQ(census_cursor()->observed,0);UT_ASSERT_EQ(local_notifies,2);
 UT_ASSERT_EQ(census_cursor()->attempts,kind==0?2:1);
 UT_ASSERT_EQ(census_cursor()->stale,kind==0?1:0);
 UT_ASSERT_EQ(route_mailbox()->query.proc_generation,kind==0?33:11);
 UT_ASSERT_EQ(route_mailbox()->query.round_generation,kind==0?13:14);
 route_deliver();census_tick();
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_DONE);
 UT_ASSERT_EQ(census_cursor()->observed,1);UT_ASSERT_EQ(held->regd_count,1);
 census_unqualified();route_clear(held);
}
UT_TEST(backend_census_reasks_replaced_target_only_after_original_callback_returns)
{census_claimed_round(0);}
UT_TEST(backend_census_new_round_waits_for_original_claimed_callback_to_return)
{census_claimed_round(1);}
UT_TEST(backend_census_gate_drift_withdraws_original_returned_observation)
{
 Snapshot held=census_prepare();census_begin();route_deliver();
 pg_atomic_write_u64(&backend_return_gate.record_generation,20);census_tick();
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_STALE);
 UT_ASSERT_EQ(census_cursor()->observed,0);UT_ASSERT_EQ(census_cursor()->attempts,1);
 UT_ASSERT_EQ(pg_atomic_read_u64(&route_mailbox()->word)&3,CLUSTER_SURVIVOR_LOCAL_EMPTY);
 pg_atomic_write_u64(&backend_return_gate.record_generation,19);census_tick();
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_STALE);
 UT_ASSERT_EQ(local_notifies,1);UT_ASSERT_EQ(held->regd_count,1);
 census_unqualified();route_clear(held);
}
UT_TEST(backend_census_keeps_next_native_owner_pending_after_backend_observation)
{
 Snapshot held=census_prepare();
 route_status[MaxBackends+LmonProcess].st_changecount=2;
 route_status[MaxBackends+LmonProcess].st_procpid=2345;
 route_status[MaxBackends+LmonProcess].st_backendType=B_LMON;
 census_begin();route_deliver();census_tick();
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_WAIT);
 UT_ASSERT_EQ(census_cursor()->cursor,1);UT_ASSERT_EQ(census_cursor()->attempts,2);
 UT_ASSERT_EQ(census_cursor()->observed,1);UT_ASSERT_EQ(census_cursor()->unknown,0);
 UT_ASSERT_EQ(route_mailbox()->query.family,CLUSTER_SURVIVOR_PRODUCER_NATIVE);
 UT_ASSERT_EQ(route_mailbox()->query.pid,2345);UT_ASSERT_EQ(route_mailbox()->query.backend_id,-1);
 UT_ASSERT_EQ(local_notifies,1);UT_ASSERT_EQ(held->regd_count,1);
 /* Stop before the unrelated native-family leaf, which remains aborting. */
 census_unqualified();route_clear(held);
}
UT_TEST(backend_census_real_reap_retires_transport_without_snapshot_return)
{
 Snapshot held=census_prepare();pid_t child;int status;
 fflush(NULL);child=fork();UT_ASSERT(child>=0);if(child==0)_exit(7);
 /* Only the child PID and waitpid are OS facts. Its selected PGPROC and
  * parent-held snapshot remain explicit host inputs, never child cleanup. */
 proc.pid=child;route_status[0].st_procpid=child;ProcSignal->psh_slot[0].pss_pid=child;
 census_tick();
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_WAIT);
 UT_ASSERT_EQ(route_mailbox()->query.pid,child);
 UT_ASSERT_EQ(waitpid(child,&status,0),child);UT_ASSERT(WIFEXITED(status));UT_ASSERT_EQ(WEXITSTATUS(status),7);
 IsUnderPostmaster=false;IsPostmasterEnvironment=true;MyProc=NULL;
 cluster_semantic_survivor_local_postmaster_reap(child);
 IsUnderPostmaster=true;IsPostmasterEnvironment=false;census_tick();
 UT_ASSERT_EQ(census_cursor()->phase,CLUSTER_SURVIVOR_TARGETS_DONE);
 UT_ASSERT_EQ(census_cursor()->refused,1);UT_ASSERT_EQ(census_cursor()->observed,0);
 UT_ASSERT_EQ(census_cursor()->attempts,1);UT_ASSERT_EQ(held->regd_count,1);
 UT_ASSERT_EQ(remembered,1);census_unqualified();
 proc.pid=1234;route_status[0].st_procpid=1234;route_clear(held);
}
