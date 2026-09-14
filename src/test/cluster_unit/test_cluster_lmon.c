/*-------------------------------------------------------------------------
 *
 * test_cluster_lmon.c
 *	  Compile-time / link-level invariants for spec-1.11 LMON Sprint A.
 *
 *	  Locks:
 *	    - ClusterLmonStatus enum values (NOT_STARTED=0, SPAWNING=1,
 *	      READY=2, SHUTTING_DOWN=3, EXITED=4) are frozen.
 *	    - ClusterLmonSharedState size stays under 4 KiB (catch
 *	      accidental field bloat early).
 *	    - cluster_lmon_status_to_string() returns non-null for every
 *	      enum value and "(unknown)" for out-of-range.
 *	    - Public symbols cluster_lmon_start / wait_for_ready /
 *	      request_shutdown / status / shmem_register/init / LmonMain
 *	      resolve at link time.
 *
 *	  Behavior tests (postmaster spawns LMON, phase 1 sync wait ready,
 *	  clean shutdown, kill -9 crash recovery) live in TAP
 *	  t/061_lmon_skeleton.pl.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_lmon.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-1.11-lmon-skeleton.md (Sprint A scope).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>
#include <signal.h>
#include <time.h>

#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_tt_status_hint.h"
#include "storage/proc.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


/* ----------
 * Stubs needed to link cluster_lmon.o standalone.  Runtime paths
 * (LmonMain / shmem init) are not exercised here; these are address-
 * only / pure-function tests.
 * ----------
 */

bool IsUnderPostmaster = false;
volatile sig_atomic_t ConfigReloadPending = false;
volatile sig_atomic_t ShutdownRequestPending = false;
int MyProcPid = 0;
PGPROC *MyProc = NULL;

static ClusterLmonSharedState test_lmon_state;
static bool test_lmon_shmem_found = false;
static bool test_lmon_exit_armed = false;
static jmp_buf test_lmon_exit_jump;
static int test_lmon_wait_calls = 0;
static bool test_lmon_clock_active = false;
static uint64 test_lmon_clock_ns[2];
static int test_lmon_clock_calls = 0;
static bool test_lmon_seed_saturation = false;
static int test_pcm_reclaim_ticks = 0;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}
bool
errstart_cold(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{}
int
errcode(int s pg_attribute_unused())
{
	return 0;
}
int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
void
elog_start(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		   const char *fn pg_attribute_unused())
{}
void
elog_finish(int e pg_attribute_unused(), const char *f pg_attribute_unused(), ...)
{}
void
pre_format_elog_string(int n pg_attribute_unused(), const char *d pg_attribute_unused())
{}
char *
format_elog_string(const char *f pg_attribute_unused(), ...)
{
	return NULL;
}

#include "storage/lwlock.h"
#include "storage/shmem.h"
static bool test_lwlock_conditional_result = true;
static int test_lwlock_blocking_calls = 0;
static int test_lwlock_conditional_calls = 0;

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}
bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	test_lwlock_blocking_calls++;
	return true;
}
bool
LWLockConditionalAcquire(LWLock *lock pg_attribute_unused(),
						 LWLockMode mode pg_attribute_unused())
{
	test_lwlock_conditional_calls++;
	return test_lwlock_conditional_result;
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr)
{
	if (foundPtr != NULL)
		*foundPtr = test_lmon_shmem_found;
	test_lmon_shmem_found = true;
	return &test_lmon_state;
}

#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

#include "datatype/timestamp.h"
TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
}

/*
 * R1-A O3: instr_time is deliberately sourced from a deterministic
 * monotonic clock while the real LmonMain duty loop runs.  Wall-clock
 * scheduling continues to use the independent GetCurrentTimestamp stub.
 */
int
clock_gettime(clockid_t clock_id pg_attribute_unused(), struct timespec *tp)
{
	uint64 now_ns = 0;

	if (test_lmon_clock_active) {
		int index = Min(test_lmon_clock_calls, 1);

		now_ns = test_lmon_clock_ns[index];
		test_lmon_clock_calls++;
	}

	tp->tv_sec = (time_t)(now_ns / UINT64CONST(1000000000));
	tp->tv_nsec = (long)(now_ns % UINT64CONST(1000000000));
	return 0;
}

/* postmaster-owned wrapper: LMON main never invokes the runtime path
 * here, but cluster_lmon_start() is a thin proxy that forwards to it,
 * so the symbol must resolve at link time. */
pid_t
cluster_postmaster_start_lmon(void)
{
	return 0;
}

/* Spec-1.11 Sprint B: cluster_lmon.c references
 * cluster_lmon_main_loop_interval GUC + WaitLatch / ResetLatch /
 * MyLatch + cluster_inject framework.  Stubs cover them all --
 * runtime LmonMain is not exercised. */
int cluster_lmon_main_loop_interval = 1000;
int cluster_lmon_slow_iteration_warn_ms = 1000;

#include "cluster/cluster_inject.h"
int cluster_injection_armed_count = 0;
char *cluster_injection_points = NULL;

/*
 * spec-2.2 D5 Step 7 stubs -- cluster_lmon.c now references tier1
 * helpers + WaitEventSet API + cluster_enabled / cluster_interconnect_tier
 * GUCs.  Stub here because this unit test only takes function addresses
 * and never invokes LmonMain at runtime; runtime behaviour is verified
 * at TAP layer (075/076 in spec-2.2 Steps 10-11).
 */
bool cluster_enabled = false;
int cluster_interconnect_tier = 0;			  /* CLUSTER_IC_TIER_STUB */
int cluster_interconnect_rdma_completion = 0; /* CLUSTER_IC_RDMA_COMPLETION_EVENT */

/* spec-2.2 D7 GUCs (cluster_lmon.c references heartbeat_interval_ms). */
int cluster_interconnect_heartbeat_interval_ms = 1000;
int cluster_interconnect_connect_timeout_ms = 5000;
int cluster_interconnect_recv_timeout_ms = 30000;

/* spec-7.2 D1 GUC (cluster_lmon_duty_should_run references it). */
bool cluster_ic_duty_lazy = true;

/* spec-7.2 D4 stub: plane-flip registry probe (skeleton = CONTROL). */
bool
cluster_gcs_block_family_on_data_plane(void)
{
	return false;
}

#include "cluster/cluster_ic_tier1.h"

int
cluster_ic_tier1_listener_bind(void)
{
	return -1;
}
bool
cluster_ic_tier1_accept_one(int *out_peer_fd pg_attribute_unused(),
							int32 *out_peer_id pg_attribute_unused())
{
	return false;
}
int
cluster_ic_tier1_get_listener_fd(void)
{
	return -1;
}
int
cluster_ic_tier1_get_peer_fd(int32 peer_id pg_attribute_unused())
{
	return -1;
}
bool
cluster_ic_tier1_connect_one(int32 peer_id pg_attribute_unused(),
							 int *out_peer_fd pg_attribute_unused())
{
	return false;
}
bool
cluster_ic_tier1_finish_connect(int32 peer_id pg_attribute_unused(),
								int peer_fd pg_attribute_unused())
{
	return false;
}
bool
cluster_ic_tier1_recv_and_verify_hello(int32 peer_id pg_attribute_unused(),
									   int peer_fd pg_attribute_unused())
{
	return false;
}
ClusterICSendResult
cluster_ic_tier1_send_heartbeat(int32 peer_id pg_attribute_unused())
{
	return CLUSTER_IC_SEND_HARD_ERROR;
}
bool
cluster_ic_tier1_pending_outbound(int32 peer_id pg_attribute_unused())
{
	return false;
}
/* GCS serve-stall round-5: LMON's WRITEABLE arm drains via the frame-free
 * entry now (never re-enters send_heartbeat while backpressured). */
ClusterICSendResult
cluster_ic_tier1_drain_outbound(int32 peer_id pg_attribute_unused())
{
	return CLUSTER_IC_SEND_DONE;
}

ClusterICPeerTransport
cluster_ic_mux_peer_transport(int32 peer_id pg_attribute_unused())
{
	return CLUSTER_IC_PEER_TRANSPORT_TCP;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type pg_attribute_unused(),
						 int32 dest_node_id pg_attribute_unused(),
						 const void *payload pg_attribute_unused(),
						 uint32 payload_len pg_attribute_unused())
{
	return CLUSTER_IC_SEND_DONE;
}

int
cluster_ic_rdma_lmon_cm_fd(void)
{
	return -1;
}

int
cluster_ic_rdma_lmon_completion_fd(void)
{
	return -1;
}

void
cluster_ic_rdma_lmon_start(void)
{}

void
cluster_ic_rdma_lmon_stop(void)
{}

void
cluster_ic_rdma_lmon_handle_cm_events(void)
{}

void
cluster_ic_rdma_lmon_handle_completion_events(void)
{}

void
cluster_ic_chunk_scan_reassembly_timeouts(void)
{}
bool
cluster_ic_tier1_lmon_drain_close_requests(void)
{
	return false;
}

/* spec-2.5 D2.6 stub: LMON tick drain CSSD outbound queue.  Tests don't
 * exercise CSSD shmem;return NULL → drain noop branch in LmonMain. */
#include "cluster/cluster_cssd.h"
ClusterCssdOutboundSlot *
cluster_cssd_outbound_slots(void)
{
	return NULL;
}

/* spec-2.5 D12 stub: CSSD heartbeat dispatch handler registered in
 * postmaster phase 1.  Tests don't dispatch heartbeats here. */
void
cluster_cssd_dispatch_heartbeat(const ClusterICEnvelope *env pg_attribute_unused(),
								const void *payload pg_attribute_unused())
{}

/* spec-2.9 D1 / L104 stub: BOC broadcast dispatch handler registered in
 * postmaster phase 1.  Tests don't dispatch BOC pulses here;  body in
 * cluster_scn.c is not linked into this standalone binary. */
void
cluster_scn_boc_broadcast_handler(const ClusterICEnvelope *env pg_attribute_unused(),
								  const void *payload pg_attribute_unused())
{}

void
cluster_scn_lmon_drain_boc_broadcast(void)
{}

/* spec-2.5 D2.6 stubs: cluster_ic_envelope_build + cluster_ic_send_bytes
 * referenced by LmonMain CSSD drain branch.  drain noop above means body
 * is unreachable in tests, but link must resolve. */
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic.h"
bool
cluster_ic_envelope_build(ClusterICEnvelope *env pg_attribute_unused(),
						  uint8 msg_type pg_attribute_unused(),
						  uint32 source_node_id pg_attribute_unused(),
						  uint32 dest_node_id pg_attribute_unused(),
						  const void *payload pg_attribute_unused(),
						  uint32 payload_length pg_attribute_unused())
{
	return true;
}
ClusterICSendResult
cluster_ic_send_bytes(int32 target_node_id pg_attribute_unused(),
					  const void *buf pg_attribute_unused(), size_t len pg_attribute_unused())
{
	return CLUSTER_IC_SEND_DONE;
}
bool
cluster_ic_tier1_recv_heartbeat_drain(int32 peer_id pg_attribute_unused(),
									  int peer_fd pg_attribute_unused())
{
	return false;
}
void
cluster_ic_tier1_close_peer(int32 peer_id pg_attribute_unused(),
							const char *reason pg_attribute_unused())
{}

/* Hardening v1.0.1 stubs (F1 + F2). */
bool
cluster_ic_tier1_continue_hello_send(int32 peer_id pg_attribute_unused(),
									 int peer_fd pg_attribute_unused())
{
	return false;
}
int
cluster_ic_tier1_hello_send_remaining(int32 peer_id pg_attribute_unused())
{
	return 0;
}
bool
cluster_ic_tier1_continue_hello_recv(int anon_slot pg_attribute_unused(),
									 int peer_fd pg_attribute_unused(), int32 *out_learned_peer_id)
{
	if (out_learned_peer_id != NULL)
		*out_learned_peer_id = -1;
	return false;
}
void
cluster_ic_tier1_anon_hello_reset(int anon_slot pg_attribute_unused())
{}
const ClusterICPeerStateShmem *
cluster_ic_tier1_peer_get(int32 peer_id pg_attribute_unused())
{
	return NULL;
}

/* spec-2.3 D5: cluster_lmon_shmem_init now registers HEARTBEAT msg_type
 * with cluster_ic_router.  Stub the register API so this address-only
 * link test passes without pulling in the whole router. */
#include "cluster/cluster_ic_router.h"
static bool test_semantic_ack_registered;
static ClusterICMsgTypeInfo test_semantic_ack_registration;

void
cluster_ic_register_msg_type(const ClusterICMsgTypeInfo *info)
{
	if (info != NULL
		&& info->msg_type == PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1) {
		test_semantic_ack_registration = *info;
		test_semantic_ack_registered = true;
	}
}

void
cluster_semantic_activation_ack_handler(
	const ClusterICEnvelope *env pg_attribute_unused(),
	const void *payload pg_attribute_unused())
{}

/* spec-2.32 D4 stub:  cluster_lmon_shmem_init calls cluster_gcs_register_msg_types. */
void
cluster_gcs_register_msg_types(void)
{}

/* spec-2.33 D4 stub:  cluster_lmon_shmem_init also calls
 * cluster_gcs_register_block_msg_types (block-shipping data plane). */
void
cluster_gcs_register_block_msg_types(void)
{}

/* spec-2.38 hardening stubs: LMON registers the SINVAL msg type and
 * drains its outbound queue, but this standalone unit binary does not
 * link cluster_sinval.o. */
void
cluster_sinval_register_msg_type(void)
{}

void
cluster_sinval_drain_outbound_and_broadcast(void)
{}

/* spec-2.39 D5 + D7:  LMON-mediated ack drain + RESET-all broadcast. */
void
cluster_sinval_drain_ack_outbound_and_send(void)
{}
void
cluster_sinval_broadcast_reset_all(void)
{}

/* spec-3.2 D6 + D1 / spec-8.4 D10: gated drain + msg_type register. */
ClusterSemanticAdmissionResult
cluster_tt_status_hint_source_dispatch(
	ClusterTTStatusHintSourceOp op pg_attribute_unused(),
	const ClusterTTStatusHintSourceRequest *request pg_attribute_unused())
{
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}
void
cluster_tt_status_hint_register_msg_type(void)
{}

/* spec-5.7 D1 stub:  cluster_lmon_shmem_init registers the HW_ALLOC IC msg
 * types, but this standalone unit binary does not link cluster_hw_ic.o. */
void
cluster_hw_register_ic_msg_types(void)
{}

/* spec-5.7 D6 stub:  cluster_lmon_shmem_init registers the KO_FLUSH IC msg
 * types, but this standalone unit binary does not link cluster_ko_lock.o. */
void
cluster_ko_register_ic_msg_types(void)
{}

/* spec-5.13 D8 stub:  cluster_lmon_shmem_init registers the CLEAN_LEAVE IC msg
 * types, but this standalone unit binary does not link cluster_clean_leave.o. */
void
cluster_clean_leave_register_ic_msg_types(void)
{}

/* spec-5.13 D6 stub:  the LMON tick calls the clean-leave orchestration; this
 * standalone unit binary does not link cluster_clean_leave.o. */
void
cluster_clean_leave_lmon_tick(void)
{}

/* spec-5.18 D9/D10 stubs: the LMON tick drives node removal + registers its IC
 * msg types; this standalone unit binary does not link cluster_node_remove.o. */
void cluster_node_remove_lmon_tick(void);
void
cluster_node_remove_lmon_tick(void)
{}
void cluster_node_remove_register_ic_msg_types(void);
void
cluster_node_remove_register_ic_msg_types(void)
{}

/* spec-6.5 D1/D4 stubs: cluster_lmon registers and ticks the backup
 * coordinator/peer ACK path, but this standalone unit binary intentionally
 * does not link cluster_backup.o or backend backup symbols. */
void cluster_backup_register_ic_msg_types(void);
void
cluster_backup_register_ic_msg_types(void)
{}
void cluster_backup_lmon_tick(void);
void
cluster_backup_lmon_tick(void)
{}

/* spec-5.22e D5-2 stub: LmonMain also ticks the undo horizon publisher and
 * registers its msg type (cluster_undo_horizon_ic.c not linked here). */
void cluster_undo_horizon_lmon_tick(void);
void
cluster_undo_horizon_lmon_tick(void)
{}
void cluster_undo_horizon_register_ic_msg_types(void);
void
cluster_undo_horizon_register_ic_msg_types(void)
{}

/* spec-6.2 D7 stub: cluster_lmon_shmem_init registers Smart Fusion durable
 * gossip IC msg types; this standalone unit binary does not link
 * cluster_sf_dep.o. */
void
cluster_sf_dep_register_ic_msg_types(void)
{}

/* The standalone LMON fixture does not run its main loop. The production
 * publisher and rate-limited continuation execute in test_cluster_sf_dep. */
void cluster_sf_origin_durable_lmon_tick(void);
void
cluster_sf_origin_durable_lmon_tick(void)
{}

/* spec-2.2 additive amendment (spec-5.22e D5 prereq) stub:
 * cluster_lmon_shmem_init registers the PEER_CAPS_REPLY msg type; this
 * standalone unit binary does not link cluster_ic_tier1.o. */
void
cluster_ic_tier1_register_caps_reply_msg_type(void)
{}

/* GCS-race round-3 P0-1 stubs: LmonMain ticks the xid wrap barrier and
 * cluster_lmon_shmem_init registers its DISABLE/ACK msg types; this
 * standalone unit binary does not link cluster_xid_wrap_barrier.o. */
void cluster_xid_wrap_barrier_lmon_tick(void);
void
cluster_xid_wrap_barrier_lmon_tick(void)
{}
void cluster_xid_wrap_barrier_register_ic_msg_types(void);
void
cluster_xid_wrap_barrier_register_ic_msg_types(void)
{}

/* spec-2.2 D5 LMON drive references cluster_conf_lookup_node + cluster_node_id. */
const struct ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id pg_attribute_unused())
{
	return NULL;
}
int cluster_node_id = -1;

/* WaitEventSet API stubs (storage/latch.h).  Never invoked at unit-test
 * runtime because the test doesn't call LmonMain. */
struct WaitEventSet;
struct WaitEvent;
typedef struct WaitEventSet WaitEventSet;
typedef struct WaitEvent WaitEvent;

/* CurrentMemoryContext type is MemoryContext (struct MemoryContextData *)
 * declared in utils/memutils.h indirectly via cluster_lmon.c includes. */
MemoryContext CurrentMemoryContext = NULL;
WaitEventSet *
CreateWaitEventSet(MemoryContext cxt pg_attribute_unused(), int nevents pg_attribute_unused())
{
	return NULL;
}
int
AddWaitEventToSet(WaitEventSet *set pg_attribute_unused(), uint32 events pg_attribute_unused(),
				  pgsocket fd pg_attribute_unused(), Latch *latch pg_attribute_unused(),
				  void *user_data pg_attribute_unused())
{
	return -1;
}
int
WaitEventSetWait(WaitEventSet *set pg_attribute_unused(), long timeout pg_attribute_unused(),
				 WaitEvent *occurred_events pg_attribute_unused(),
				 int nevents pg_attribute_unused(), uint32 wait_event_info pg_attribute_unused())
{
	return 0;
}
void
FreeWaitEventSet(WaitEventSet *set pg_attribute_unused())
{}
void
cluster_injection_run(const char *name pg_attribute_unused())
{}
/* spec-1.14.1 F20 stub: cluster_*_start() now calls should_skip. */
bool
cluster_injection_should_skip(const char *name pg_attribute_unused())
{
	return false;
}

/* libpq + procsignal stubs (pulled in transitively via cluster_lmon.c
 * includes; LmonMain runtime is not invoked). */
struct sigaction;
typedef void (*pqsigfunc)(int);
pqsigfunc
pqsignal(int signum pg_attribute_unused(), pqsigfunc handler pg_attribute_unused())
{
	return handler;
}
void
SignalHandlerForConfigReload(int sig pg_attribute_unused())
{}
void
SignalHandlerForShutdownRequest(int sig pg_attribute_unused())
{}
void
procsignal_sigusr1_handler(int sig pg_attribute_unused())
{}
sigset_t UnBlockSig;
void
ProcessConfigFile(int context pg_attribute_unused())
{}

void
init_ps_display(const char *fixed_part pg_attribute_unused())
{}

void
proc_exit(int code pg_attribute_unused())
{
	if (test_lmon_exit_armed)
		longjmp(test_lmon_exit_jump, 1);
	abort();
}

#include "utils/timestamp.h"

/* CHECK_FOR_INTERRUPTS stubs */
volatile sig_atomic_t InterruptPending = false;
void
ProcessInterrupts(void)
{}

void
pg_usleep(long microsec pg_attribute_unused())
{}

/* Sprint B: Latch / WaitLatch / ResetLatch stubs (LmonMain runtime
 * is not invoked at unit-test level). */
struct Latch *MyLatch = NULL;
void
SetLatch(struct Latch *latch pg_attribute_unused())
{}
int
WaitLatch(struct Latch *latch pg_attribute_unused(), int wakeEvents pg_attribute_unused(),
		  long timeout pg_attribute_unused(), uint32 wait_event_info pg_attribute_unused())
{
	test_lmon_wait_calls++;
	ShutdownRequestPending = true;
	return 0;
}
void
ResetLatch(struct Latch *latch pg_attribute_unused())
{}

#include "storage/ipc.h"
void
on_shmem_exit(pg_on_exit_callback function pg_attribute_unused(), Datum arg pg_attribute_unused())
{}

/* cluster_lmon.c references MyBackendType (set by LmonMain). */
#include "miscadmin.h"
BackendType MyBackendType = B_INVALID;

/* spec-2.28 Sprint A Step 3 stub:  cluster_lmon.c now calls
 * cluster_fence_lmon_tick() in its main loop (D5).  Empty stub for
 * unit-test link;real broadcast logic is in cluster_fence.c +
 * verified by test_cluster_fence T-fence-2/3/4/5/6 + 098 TAP. */
void cluster_fence_lmon_tick(void);
void
cluster_fence_lmon_tick(void)
{
	if (test_lmon_seed_saturation) {
		test_lmon_state.timed_duty_sample_count = PG_UINT64_MAX;
		test_lmon_state.total_iter_us = PG_UINT64_MAX - 5;
		test_lmon_seed_saturation = false;
	}
}

/* spec-2.29 Sprint A Step 2 stub: cluster_lmon.c now calls
 * cluster_reconfig_lmon_tick() in its main loop.  Empty stub for
 * unit-test link;real coordinator behavior is covered by
 * test_cluster_reconfig and TAP 099. */
void cluster_reconfig_lmon_tick(void);
void
cluster_reconfig_lmon_tick(void)
{}

void
cluster_thread_recovery_lmon_tick(void)
{}

void
cluster_thread_recovery_lmon_shutdown(void)
{}

void cluster_semantic_activation_lmon_tick(void);
void
cluster_semantic_activation_lmon_tick(void)
{}

/* spec-2.16 D8 L104 stub:  cluster_lmon.c calls cluster_grd_lmon_tick_
 * dead_sweep() each tick before reconfig_lmon_tick.  test_cluster_lmon
 * standalone doesn't link cluster_grd.o,  vacuous stub. */
void cluster_grd_lmon_tick_dead_sweep(void);
void
cluster_grd_lmon_tick_dead_sweep(void)
{}

int cluster_grd_reclaim_sweep(void);
int
cluster_grd_reclaim_sweep(void)
{
	return 0;
}

void
cluster_pcm_lock_lmon_reclaim_tick(void)
{
	test_pcm_reclaim_ticks++;
}

/* spec-5.10 fix-forward — cluster_lmon.c calls the runtime-off starvation sweep. */
uint32 cluster_grd_lmon_tick_starvation_sweep(void);
uint32
cluster_grd_lmon_tick_starvation_sweep(void)
{
	return 0;
}

/* spec-4.6 D1 L104 stub:  cluster_lmon.c calls cluster_grd_recovery_
 * lmon_tick() after reconfig_lmon_tick (P0-P7 recovery sequence).
 * Standalone fixture doesn't link cluster_grd.o;  vacuous stub. */
void cluster_grd_recovery_lmon_tick(void);
void
cluster_grd_recovery_lmon_tick(void)
{}

void
cluster_gcs_block_pcm_x_formation_tick(void)
{}

bool
cluster_gcs_block_resource_x_cutover_tick(void)
{
	return false;
}

/* spec-2.34 D6 L104 stub:  cluster_lmon.c LMON tick body calls
 * cluster_gcs_block_dedup_sweep_expired(now).  Standalone fixture has
 * no dedup HTAB linked; vacuous stub. */
void cluster_gcs_block_dedup_sweep_expired(TimestampTz now);
void
cluster_gcs_block_dedup_sweep_expired(TimestampTz now pg_attribute_unused())
{}

/* spec-6.12h D-h2 stub:  LMON tick body drains the PI-discard note ring.
 * Standalone fixture has no gcs_block shmem linked; vacuous stub. */
void cluster_gcs_block_pi_discard_drain(void);
void
cluster_gcs_block_pi_discard_drain(void)
{}

/* spec-2.17 Step 5 L104 stub:  cluster_grd_deadlock_lmon_tick wired
 * into LMON tick body after dead sweep. */
void cluster_grd_deadlock_lmon_tick(void);
void
cluster_grd_deadlock_lmon_tick(void)
{}

/* spec-2.18 Sprint A Step 3 D8 L104 stub:  cluster_lms_owns_grant
 * gates LMON ges drain path so LMS owns grant once READY. */
bool cluster_lms_owns_grant(void);
bool
cluster_lms_owns_grant(void)
{
	return false;
}

int
cluster_ges_lmon_drain_work_queue(void)
{
	return 0;
}

/* spec-6.12b — LmonMain ships finished CR-server results each tick; stub it
 * (no LMS / no IC in the lmon unit harness). */
void
cluster_lms_cr_ship_ready(void)
{}

/* spec-5.16 — LmonMain sweeps abandoned reply-wait tombstones each tick; stub it
 * (real impl in cluster_ges_reply_wait.o, not linked into this standalone test). */
int
cluster_ges_reply_wait_sweep_timeout(TimestampTz now pg_attribute_unused())
{
	return 0;
}

int
cluster_grd_outbound_lmon_drain_send(void)
{
	return 0;
}

int
cluster_gcs_block_lmon_drain_direct_land_aborts(void)
{
	return 0;
}

void
cluster_lms_native_probe_retry_tick(void)
{}

/* spec-2.13 D8 / L104 stubs: cluster_lmon.c registers 2 GES handler
 * function pointers (cluster_ges_{request,reply}_handler) into the
 * ICMsgType registry in postmaster phase 1.  test_cluster_lmon
 * standalone binary doesn't link cluster_ges.o, so symbols need
 * vacuous stubs to satisfy the linker.  Real handler behavior
 * verified by test_cluster_ges T-ges-1 a/b/c/d/e. */
void
cluster_ges_request_handler(const ClusterICEnvelope *env pg_attribute_unused(),
							const void *payload pg_attribute_unused())
{}

void
cluster_ges_reply_handler(const ClusterICEnvelope *env pg_attribute_unused(),
						  const void *payload pg_attribute_unused())
{}

/* spec-2.14 D12 / L104 stub: cluster_lmon.c calls
 * cluster_grd_master_map_init() at postmaster phase 1.  Vacuous stub
 * for standalone link;  real master_map init behavior verified by
 * test_cluster_grd T-grd-1d (sparse declared list). */
void
cluster_grd_master_map_init(void)
{}

void
cluster_grd_recovery_authority_lmon_tick(void)
{}


UT_DEFINE_GLOBALS();


/* ============================================================
 * Compile-time anchors
 * ============================================================ */

UT_TEST(test_lmon_status_enum_values_frozen)
{
	UT_ASSERT_EQ((int)CLUSTER_LMON_NOT_STARTED, 0);
	UT_ASSERT_EQ((int)CLUSTER_LMON_SPAWNING, 1);
	UT_ASSERT_EQ((int)CLUSTER_LMON_READY, 2);
	UT_ASSERT_EQ((int)CLUSTER_LMON_SHUTTING_DOWN, 3);
	UT_ASSERT_EQ((int)CLUSTER_LMON_EXITED, 4);
	UT_ASSERT_EQ((int)CLUSTER_LMON_STATUS_LAST, 4);
}


UT_TEST(test_lmon_shared_state_size_under_4kb)
{
	/* Catch accidental field bloat early (typical size ~80 bytes). */
	UT_ASSERT(sizeof(ClusterLmonSharedState) < 4096);
}


UT_TEST(test_lmon_status_to_string_lookup)
{
	int i;

	for (i = 0; i <= (int)CLUSTER_LMON_STATUS_LAST; i++) {
		const char *s = cluster_lmon_status_to_string((ClusterLmonStatus)i);
		UT_ASSERT_NOT_NULL(s);
		if (s != NULL)
			UT_ASSERT(s[0] != '\0');
	}
}


UT_TEST(test_lmon_status_unknown_returns_unknown)
{
	const char *neg = cluster_lmon_status_to_string((ClusterLmonStatus)-1);
	const char *over
		= cluster_lmon_status_to_string((ClusterLmonStatus)((int)CLUSTER_LMON_STATUS_LAST + 1));

	UT_ASSERT_STR_EQ(neg, "(unknown)");
	UT_ASSERT_STR_EQ(over, "(unknown)");
}


UT_TEST(test_lmon_public_symbols_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_lmon_start);
	UT_ASSERT_NOT_NULL((void *)cluster_lmon_wait_for_ready);
	UT_ASSERT_NOT_NULL((void *)cluster_lmon_request_shutdown);
	UT_ASSERT_NOT_NULL((void *)cluster_lmon_status);
	UT_ASSERT_NOT_NULL((void *)cluster_lmon_status_to_string);
	UT_ASSERT_NOT_NULL((void *)cluster_lmon_shmem_size);
	UT_ASSERT_NOT_NULL((void *)cluster_lmon_shmem_init);
	UT_ASSERT_NOT_NULL((void *)cluster_lmon_shmem_register);
	UT_ASSERT_NOT_NULL((void *)cluster_lmon_last_iter_us);
	UT_ASSERT_NOT_NULL((void *)cluster_lmon_max_iter_us);
	UT_ASSERT_NOT_NULL((void *)cluster_lmon_slow_iter_count);
	UT_ASSERT_NOT_NULL((void *)cluster_lmon_marker_complete_wakeup);
	UT_ASSERT_NOT_NULL((void *)LmonMain);
}

UT_TEST(test_lmon_iteration_counters_null_safe)
{
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_last_iter_us(), 0ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_max_iter_us(), 0ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_slow_iter_count(), 0ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_timed_duty_sample_count(), 0ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_total_iter_us(), 0ULL);
	cluster_lmon_marker_complete_wakeup();
}

UT_TEST(test_lmon_registers_semantic_ack_control_handler_without_broadcast)
{
	test_semantic_ack_registered = false;
	memset(&test_semantic_ack_registration, 0,
		   sizeof(test_semantic_ack_registration));
	cluster_lmon_shmem_init();

	UT_ASSERT(test_semantic_ack_registered);
	UT_ASSERT_EQ(test_semantic_ack_registration.msg_type,
				 PGRAC_IC_MSG_SEMANTIC_ACTIVATION_ACK_V1);
	UT_ASSERT_STR_EQ(test_semantic_ack_registration.name,
				 "semantic_activation_ack_v1");
	UT_ASSERT_EQ(test_semantic_ack_registration.allowed_producer_mask,
				 CLUSTER_IC_PRODUCER_LMON);
	UT_ASSERT(!test_semantic_ack_registration.broadcast_ok);
	UT_ASSERT(test_semantic_ack_registration.handler
			  == cluster_semantic_activation_ack_handler);
	UT_ASSERT_EQ(test_semantic_ack_registration.plane,
				 CLUSTER_IC_PLANE_CONTROL);
}

static void
run_one_real_lmon_duty(uint64 start_us, uint64 finish_us, bool seed_saturation)
{
	if (!test_lmon_shmem_found)
		cluster_lmon_shmem_init();

	IsUnderPostmaster = true;
	cluster_enabled = false;
	ConfigReloadPending = false;
	ShutdownRequestPending = false;
	test_lmon_state.shutdown_requested = false;
	test_lmon_wait_calls = 0;
	test_lmon_clock_ns[0] = start_us * UINT64CONST(1000);
	test_lmon_clock_ns[1] = finish_us * UINT64CONST(1000);
	test_lmon_clock_calls = 0;
	test_lmon_clock_active = true;
	test_lmon_seed_saturation = seed_saturation;
	test_pcm_reclaim_ticks = 0;
	test_lmon_exit_armed = true;

	if (setjmp(test_lmon_exit_jump) == 0)
		LmonMain();

	test_lmon_exit_armed = false;
	test_lmon_clock_active = false;
	IsUnderPostmaster = false;
}

/*
 * R1-A O3: execute the real disabled-interconnect LMON loop once.  The
 * recorder consumes one monotonic interval and publishes every member of
 * the old timing triple plus the new exact count/time pair from that same
 * elapsed value.  WaitLatch requests shutdown only after the completed duty.
 */
UT_TEST(test_lmon_completed_duty_records_one_exact_timed_pair)
{
	uint64 sample_count = 0;
	uint64 total_us = 0;

	cluster_lmon_slow_iteration_warn_ms = 1;
	run_one_real_lmon_duty(500, 2000, false);

	UT_ASSERT_EQ((unsigned long long)cluster_lmon_last_iter_us(), 1500ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_max_iter_us(), 1500ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_slow_iter_count(), 1ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_timed_duty_sample_count(), 1ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_total_iter_us(), 1500ULL);
	UT_ASSERT_EQ((long long)cluster_lmon_main_loop_iters(), 1LL);
	UT_ASSERT_EQ(test_lmon_clock_calls, 2);
	UT_ASSERT_EQ(test_lmon_wait_calls, 1);

	cluster_lmon_timed_duty_pair(&sample_count, &total_us);
	UT_ASSERT_EQ((unsigned long long)sample_count, 1ULL);
	UT_ASSERT_EQ((unsigned long long)total_us, 1500ULL);
}

UT_TEST(test_lmon_zero_elapsed_is_still_one_completed_sample)
{
	cluster_lmon_slow_iteration_warn_ms = 1;
	run_one_real_lmon_duty(700, 700, false);

	UT_ASSERT_EQ((unsigned long long)cluster_lmon_last_iter_us(), 0ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_max_iter_us(), 0ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_slow_iter_count(), 0ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_timed_duty_sample_count(), 1ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_total_iter_us(), 0ULL);
	UT_ASSERT_EQ((long long)cluster_lmon_main_loop_iters(), 1LL);
	UT_ASSERT_EQ(test_lmon_clock_calls, 2);
	UT_ASSERT_EQ(test_lmon_wait_calls, 1);
}

UT_TEST(test_lmon_timed_pair_saturates_without_wrapping)
{
	cluster_lmon_slow_iteration_warn_ms = 1;
	run_one_real_lmon_duty(900, 910, true);

	UT_ASSERT_EQ((unsigned long long)cluster_lmon_last_iter_us(), 10ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_max_iter_us(), 10ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_slow_iter_count(), 0ULL);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_timed_duty_sample_count(), PG_UINT64_MAX);
	UT_ASSERT_EQ((unsigned long long)cluster_lmon_total_iter_us(), PG_UINT64_MAX);
	UT_ASSERT_EQ((long long)cluster_lmon_main_loop_iters(), 1LL);
	UT_ASSERT_EQ(test_lmon_clock_calls, 2);
	UT_ASSERT_EQ(test_lmon_wait_calls, 1);
}

UT_TEST(test_lmon_runs_pcm_reclaim_once_per_duty)
{
	run_one_real_lmon_duty(920, 930, false);
	UT_ASSERT_EQ(test_pcm_reclaim_ticks, 1);
}

/*
 * spec-7.2 D1 -- duty classification truth table (§3.7).
 *
 *	Pins EVERY ClusterLmonDuty row to its never-lazy / lazy-able class.
 *	Re-classifying a duty (or adding one) MUST update this table
 *	deliberately -- that is the anti-drift gate against a future spec
 *	silently sliding a correctness family into the lazy set.
 */
UT_TEST(test_lmon_duty_lazy_truth_table)
{
	/* never-lazy (correctness families) */
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_LIVENESS));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_FENCE));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_GRD_DEAD_SWEEP));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_GRD_RECLAIM));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_GRD_STARVATION_SWEEP));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_DEADLOCK));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_GES_REPLY_WAIT_SWEEP));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_DIRECT_LAND_ABORTS));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_LMS_NATIVE_PROBE_RETRY));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_CLEAN_LEAVE));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_NODE_REMOVE));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_RECONFIG));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_GRD_RECOVERY));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_BOC_BROADCAST));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_SINVAL_RESET_ALL));
	UT_ASSERT(!cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_PI_DISCARD));
	/* lazy-able (queue-consumption families) */
	UT_ASSERT(cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_GES_WORK_QUEUE));
	UT_ASSERT(cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_SHIP_READY));
	UT_ASSERT(cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_GRD_OUTBOUND));
	UT_ASSERT(cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_SINVAL_OUT));
	UT_ASSERT(cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_SINVAL_ACK_OUT));
	UT_ASSERT(cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_TT_HINT));
	UT_ASSERT(cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_DEDUP_TTL));
	UT_ASSERT(cluster_lmon_duty_is_lazy(CLUSTER_LMON_DUTY_BACKUP));
	/* enum shape: lazy block is contiguous + fits the uint32 bitmask */
	UT_ASSERT_EQ(CLUSTER_LMON_DUTY_N - CLUSTER_LMON_DUTY_LAZY_FIRST, 8);
	/* pre-shmem safety: mark is a no-op, should_run fails open (true) */
	cluster_lmon_duty_mark_dirty(CLUSTER_LMON_DUTY_GRD_OUTBOUND);
	UT_ASSERT(cluster_lmon_duty_should_run(CLUSTER_LMON_DUTY_GRD_OUTBOUND, false));
	UT_ASSERT(cluster_lmon_duty_should_run(CLUSTER_LMON_DUTY_FENCE, false));
}


UT_TEST(test_lmon_pid_no_pgproc_never_uses_blocking_lwlock)
{
	PGPROC fake_proc;

	cluster_lmon_shmem_init();
	test_lmon_state.pid = 4321;
	MyProc = NULL;
	test_lwlock_blocking_calls = 0;
	test_lwlock_conditional_calls = 0;
	test_lwlock_conditional_result = false;
	UT_ASSERT_EQ((int)cluster_lmon_pid(), 0);
	UT_ASSERT_EQ(test_lwlock_blocking_calls, 0);
	UT_ASSERT_EQ(test_lwlock_conditional_calls, 1);

	test_lwlock_conditional_result = true;
	UT_ASSERT_EQ((int)cluster_lmon_pid(), 4321);
	UT_ASSERT_EQ(test_lwlock_blocking_calls, 0);
	UT_ASSERT_EQ(test_lwlock_conditional_calls, 2);

	memset(&fake_proc, 0, sizeof(fake_proc));
	MyProc = &fake_proc;
	test_lwlock_blocking_calls = 0;
	test_lwlock_conditional_calls = 0;
	UT_ASSERT_EQ((int)cluster_lmon_pid(), 4321);
	UT_ASSERT_EQ(test_lwlock_blocking_calls, 1);
	UT_ASSERT_EQ(test_lwlock_conditional_calls, 0);
	MyProc = NULL;
}


/* ============================================================
 * Test runner
 * ============================================================ */

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_lmon_status_enum_values_frozen);
	UT_RUN(test_lmon_shared_state_size_under_4kb);
	UT_RUN(test_lmon_status_to_string_lookup);
	UT_RUN(test_lmon_status_unknown_returns_unknown);
	UT_RUN(test_lmon_public_symbols_linkable);
	UT_RUN(test_lmon_iteration_counters_null_safe);
	UT_RUN(test_lmon_registers_semantic_ack_control_handler_without_broadcast);
	UT_RUN(test_lmon_completed_duty_records_one_exact_timed_pair);
	UT_RUN(test_lmon_zero_elapsed_is_still_one_completed_sample);
	UT_RUN(test_lmon_timed_pair_saturates_without_wrapping);
	UT_RUN(test_lmon_runs_pcm_reclaim_once_per_duty);
	UT_RUN(test_lmon_duty_lazy_truth_table);
	UT_RUN(test_lmon_pid_no_pgproc_never_uses_blocking_lwlock);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
