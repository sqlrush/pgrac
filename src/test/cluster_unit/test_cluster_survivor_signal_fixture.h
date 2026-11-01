/* Only OS kill/latch delivery is a host boundary. Signal slot layout,
 * SendProcSignal, CheckProcSignal and the full original SIGUSR1 dispatcher
 * are extracted byte-exact below. Unrequested original reasons abort. */
#include "test_cluster_survivor_signal_types.inc"
static ProcSignalHeader *ProcSignal;
static ProcSignalSlot *MyProcSignalSlot;
static unsigned local_notifies, local_wakes;
static pid_t local_notify_pid;
static void (*local_latch_hook)(void);
static int local_kill_result;
static int local_host_kill(pid_t pid,int sig)
{ UT_ASSERT_EQ(sig,SIGUSR1);local_notifies++;local_notify_pid=pid;return local_kill_result; }
void SetLatch(Latch *latch pg_attribute_unused())
{ local_wakes++;if(local_latch_hook!=NULL)local_latch_hook(); }
#define LOCAL_UNCALLED_SIGNAL(name) void name(void); void name(void) { abort(); }
LOCAL_UNCALLED_SIGNAL(HandleCatchupInterrupt)
LOCAL_UNCALLED_SIGNAL(HandleNotifyInterrupt)
LOCAL_UNCALLED_SIGNAL(HandleParallelMessageInterrupt)
LOCAL_UNCALLED_SIGNAL(HandleWalSndInitStopping)
LOCAL_UNCALLED_SIGNAL(HandleProcSignalBarrierInterrupt)
LOCAL_UNCALLED_SIGNAL(HandleLogMemoryContextInterrupt)
LOCAL_UNCALLED_SIGNAL(HandleParallelApplyMessageInterrupt)
LOCAL_UNCALLED_SIGNAL(cluster_handle_reconfig_start_interrupt)
LOCAL_UNCALLED_SIGNAL(cluster_handle_freeze_writes_interrupt)
LOCAL_UNCALLED_SIGNAL(cluster_handle_thaw_writes_interrupt)
LOCAL_UNCALLED_SIGNAL(cluster_handle_ges_bast_interrupt)
LOCAL_UNCALLED_SIGNAL(cluster_handle_ges_cancel_interrupt)
LOCAL_UNCALLED_SIGNAL(cluster_handle_grd_redeclare_interrupt)
LOCAL_UNCALLED_SIGNAL(cluster_handle_hang_dump_interrupt)
LOCAL_UNCALLED_SIGNAL(cluster_clean_leave_handle_quiesce_interrupt)
void RecoveryConflictInterrupt(ProcSignalReason reason);
void RecoveryConflictInterrupt(ProcSignalReason reason pg_attribute_unused()) { abort(); }
#define kill local_host_kill
#include "test_cluster_survivor_signal.inc"
#undef kill
