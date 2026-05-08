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

#include <signal.h>

#include "cluster/cluster_lmon.h"

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
void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}
bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}
void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr)
{
	if (foundPtr != NULL)
		*foundPtr = false;
	return NULL;
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
int cluster_interconnect_tier = 0; /* CLUSTER_IC_TIER_STUB */

/* spec-2.2 D7 GUCs (cluster_lmon.c references heartbeat_interval_ms). */
int cluster_interconnect_heartbeat_interval_ms = 1000;
int cluster_interconnect_connect_timeout_ms = 5000;
int cluster_interconnect_recv_timeout_ms = 30000;

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
void
cluster_ic_register_msg_type(const ClusterICMsgTypeInfo *info pg_attribute_unused())
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
				  int fd pg_attribute_unused(), void *latch pg_attribute_unused(),
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
int
WaitLatch(struct Latch *latch pg_attribute_unused(), int wakeEvents pg_attribute_unused(),
		  long timeout pg_attribute_unused(), uint32 wait_event_info pg_attribute_unused())
{
	return 0;
}
void
ResetLatch(struct Latch *latch pg_attribute_unused())
{}

/* cluster_lmon.c references MyBackendType (set by LmonMain). */
#include "miscadmin.h"
BackendType MyBackendType = B_INVALID;


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
	UT_ASSERT_NOT_NULL((void *)LmonMain);
}


/* ============================================================
 * Test runner
 * ============================================================ */

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_lmon_status_enum_values_frozen);
	UT_RUN(test_lmon_shared_state_size_under_4kb);
	UT_RUN(test_lmon_status_to_string_lookup);
	UT_RUN(test_lmon_status_unknown_returns_unknown);
	UT_RUN(test_lmon_public_symbols_linkable);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
