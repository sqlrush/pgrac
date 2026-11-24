/* The complete original ProcessInterrupts dispatch and actual backend
 * observation. Connection/timeout and unrelated maintenance actions are host
 * boundaries; none supplies a producer sample or authority.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postmaster/bgworker.h"
#include "postmaster/autovacuum.h"
#include "libpq/libpq.h"
#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "tcop/dest.h"
#include "utils/timeout.h"

volatile sig_atomic_t InterruptPending, QueryCancelPending, ProcDiePending;
volatile sig_atomic_t IdleInTransactionSessionTimeoutPending, IdleSessionTimeoutPending;
volatile sig_atomic_t ProcSignalBarrierPending, LogMemoryContextPending;
volatile sig_atomic_t IdleStatsUpdateTimeoutPending, CheckClientConnectionPending;
volatile sig_atomic_t ClientConnectionLost, ParallelMessagePending, ParallelApplyMessagePending;
volatile uint32 QueryCancelHoldoffCount, InterruptHoldoffCount;
bool ClientAuthInProgress, IsBackgroundWorker;
static bool DoingCommandRead, RecoveryConflictPending, RecoveryConflictRetryable = true;
static ProcSignalReason RecoveryConflictReason;
int client_connection_check_interval, IdleInTransactionSessionTimeout, IdleSessionTimeout;
CommandDest whereToSendOutput = DestNone;
BackgroundWorker *MyBgworkerEntry;
static unsigned interrupt_cleanup, interrupt_maintenance;
static bool interrupt_lock_timeout, interrupt_statement_timeout;

void LockErrorCleanup(void) { interrupt_cleanup++; }
bool IsAutoVacuumWorkerProcess(void) { return false; }
bool IsLogicalWorker(void) { return false; }
bool IsLogicalLauncher(void) { return false; }
void pgstat_report_recovery_conflict(int reason) { (void)reason; abort(); }
static int errdetail_recovery_conflict(void) { abort(); }
void proc_exit(int code) { (void)code; abort(); }
bool pq_check_connection(void) { abort(); }
void enable_timeout_after(TimeoutId id, int delay) { (void)id; (void)delay; abort(); }
bool get_timeout_indicator(TimeoutId id, bool reset)
{
 bool value;
 UT_ASSERT(reset);
 if (id == LOCK_TIMEOUT) { value=interrupt_lock_timeout; interrupt_lock_timeout=false; }
 else { UT_ASSERT_EQ(id,STATEMENT_TIMEOUT); value=interrupt_statement_timeout; interrupt_statement_timeout=false; }
 return value;
}
TimestampTz get_timeout_finish_time(TimeoutId id)
{ UT_ASSERT(id==LOCK_TIMEOUT || id==STATEMENT_TIMEOUT); return id==LOCK_TIMEOUT?100:200; }
long pgstat_report_stat(bool force) { (void)force; abort(); }
void ProcessProcSignalBarrier(void) { abort(); }
void HandleParallelMessages(void) { abort(); }
void ProcessLogMemoryContextInterrupt(void) { abort(); }
void HandleParallelApplyMessages(void) { abort(); }
/* These pre-existing independent actions have no pending work in this
 * fixture. Count their execution without replacing the observer or producer. */
#define INTERRUPT_HOST_ACTION(name) void name(void); void name(void) { interrupt_maintenance++; }
INTERRUPT_HOST_ACTION(cluster_fence_check_interrupts)
INTERRUPT_HOST_ACTION(cluster_storage_quorum_check_interrupts)
INTERRUPT_HOST_ACTION(cluster_reconfig_check_pending_in_proc_interrupts)
INTERRUPT_HOST_ACTION(cluster_grd_check_pending_interrupts)
INTERRUPT_HOST_ACTION(cluster_hang_check_pending_interrupt)
INTERRUPT_HOST_ACTION(cluster_clean_leave_check_pending_in_proc_interrupts)
#undef INTERRUPT_HOST_ACTION
#define errmsg_internal errmsg
#include "test_cluster_survivor_backend_interrupts.inc"
#undef errmsg_internal

static Snapshot
interrupt_query(void)
{
 ClusterSurvivorLocalMailbox *m=(ClusterSurvivorLocalMailbox *)survivor_local_owner_storage.reserved;
 Snapshot held;
 backend_return_begin(); snapshot_return_epoch=8; held=registered(88);
 (void)survivor_local_owner_sample(CLUSTER_SURVIVOR_PRODUCER_BACKEND,8,7,19);
 /* A new untrusted raw request at the same owner boundary. It is not a
  * receipt or a production LMON/notification simulation. */
 memset(&m->reply,0,sizeof(m->reply));
 pg_atomic_write_u64(&m->word,(UINT64_C(2)<<2)|CLUSTER_SURVIVOR_LOCAL_REQUESTED);
 InterruptPending=true; QueryCancelPending=ProcDiePending=false;
 ClientConnectionLost=false; QueryCancelHoldoffCount=0;
 DoingCommandRead=false; interrupt_cleanup=interrupt_maintenance=0;
 interrupt_lock_timeout=interrupt_statement_timeout=false;
 return held;
}
static void
interrupt_preserves_query(unsigned scenario)
{
 ClusterSurvivorLocalMailbox *m=(ClusterSurvivorLocalMailbox *)survivor_local_owner_storage.reserved;
 Snapshot held=interrupt_query();
 volatile bool caught=false;
 bool unclaimed;
 uint32 payload;
 int expected=ERRCODE_QUERY_CANCELED;
 if(scenario==0) {ProcDiePending=true;QueryCancelPending=true;expected=ERRCODE_ADMIN_SHUTDOWN;}
 if(scenario==1) QueryCancelPending=true;
 if(scenario==2) {ClientConnectionLost=true;expected=ERRCODE_CONNECTION_FAILURE;}
 if(scenario==3) {QueryCancelPending=true;interrupt_lock_timeout=interrupt_statement_timeout=true;expected=ERRCODE_LOCK_NOT_AVAILABLE;}
 PG_TRY(); { ProcessInterrupts(); } PG_CATCH(); { caught=true; } PG_END_TRY();
 unclaimed=(pg_atomic_read_u64(&m->word)&3)==CLUSTER_SURVIVOR_LOCAL_REQUESTED;
 payload=m->reply.payload_bytes;
 UT_ASSERT(caught); UT_ASSERT_EQ(error_code,expected);
 UT_ASSERT_EQ(interrupt_cleanup,1);
 UT_ASSERT_EQ(held->regd_count,1); UT_ASSERT_EQ(remembered,1);
 UT_ASSERT_EQ(survivor_local_owner_storage.header.native_closure,0);
 /* The original error is already delivered. A later normal return can
  * observe this same request; it still cannot retire the held snapshot. */
 ProcDiePending=QueryCancelPending=ClientConnectionLost=false;
 InterruptPending=true; ProcessInterrupts();
 UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_REPLIED);
 UT_ASSERT_EQ(m->reply.result,CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
 UT_ASSERT_EQ(m->reply.payload_bytes,1128);
 UT_ASSERT_EQ(survivor_local_scalar(m->reply.body+40,4),CLUSTER_SERVICE_SNAPSHOT_REGISTERED);
 UT_ASSERT_EQ(held->regd_count,1); UnregisterSnapshot(held);
 UT_ASSERT(unclaimed); UT_ASSERT_EQ(payload,0);
}
UT_TEST(local_observation_preserves_original_process_termination_priority) {interrupt_preserves_query(0);}
UT_TEST(local_observation_preserves_original_query_cancel_priority) {interrupt_preserves_query(1);}
UT_TEST(local_observation_preserves_original_client_loss_priority) {interrupt_preserves_query(2);}
UT_TEST(local_observation_preserves_original_timeout_priority) {interrupt_preserves_query(3);}
UT_TEST(local_observation_defers_during_cancel_holdoff_and_uses_original_reentry)
{
 ClusterSurvivorLocalMailbox *m=(ClusterSurvivorLocalMailbox *)survivor_local_owner_storage.reserved;
 Snapshot held=interrupt_query();
 bool deferred;
 QueryCancelPending=true; QueryCancelHoldoffCount=1; DoingCommandRead=true;
 ProcessInterrupts();
 UT_ASSERT(InterruptPending && QueryCancelPending);
 deferred=(pg_atomic_read_u64(&m->word)&3)==CLUSTER_SURVIVOR_LOCAL_REQUESTED && m->reply.payload_bytes==0;
 UT_ASSERT_EQ(held->regd_count,1);
 QueryCancelHoldoffCount=0; ProcessInterrupts();
 UT_ASSERT(!InterruptPending && !QueryCancelPending);
 UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_REPLIED);
 UT_ASSERT_EQ(m->reply.payload_bytes,1128);
 UT_ASSERT_EQ(held->regd_count,1); UnregisterSnapshot(held); DoingCommandRead=false;
 UT_ASSERT(deferred);
}
UT_TEST(local_observation_keeps_original_interrupt_and_critical_holdoffs)
{
 ClusterSurvivorLocalMailbox *m=(ClusterSurvivorLocalMailbox *)survivor_local_owner_storage.reserved;
 Snapshot held=interrupt_query();
 for(unsigned i=0;i<2;i++) {
  InterruptHoldoffCount=i==0; CritSectionCount=i==1;
  ProcessInterrupts();
  UT_ASSERT(InterruptPending); UT_ASSERT_EQ(interrupt_maintenance,0);
  UT_ASSERT_EQ(pg_atomic_read_u64(&m->word)&3,CLUSTER_SURVIVOR_LOCAL_REQUESTED);
 }
 InterruptHoldoffCount=CritSectionCount=0; ProcessInterrupts();
 UT_ASSERT(!InterruptPending); UT_ASSERT_EQ(m->reply.payload_bytes,1128);
 UT_ASSERT_EQ(held->regd_count,1); UnregisterSnapshot(held);
}
