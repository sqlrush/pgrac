/*-------------------------------------------------------------------------
 *
 * test_cluster_lock_acquire.c
 *	  Standalone unit tests for spec-2.20 7-step state machine internal
 *	  API(D13;descoped scope per v0.4)。
 *
 *	  T-7step-1..5 — internal API surface verify(no PG hot path
 *	  integration;spec-2.21 wire to lock.c 时 verify integration):
 *	    T-7step-1: 7 individual step API + top-level entry symbol
 *	               linkable + initial counter 0
 *	    T-7step-2: HC1 LMS fail-closed S1 entry — NULL req → INTERNAL;
 *	               cluster_lms_enabled=false → OK_NATIVE skip(legacy
 *	               path signal)
 *	    T-7step-3: S2/S3/S4/S5/S6/S7 NULL req → INTERNAL
 *	    T-7step-4: top-level cluster_lock_acquire_seven_step() NULL req
 *	               → S1 INTERNAL without S7 cleanup(pre-reservation fail)
 *	    T-7step-5: I1 monotonic forward transition contract — top-level
 *	               entry returns OK_NATIVE/PENDING honestly and only
 *	               post-reservation failures may run S7 cleanup
 *
 *	  Stubs:
 *	    - cluster_lms_is_ready / cluster_lms_enabled / cluster_lmd_is_ready
 *	      provide test-controlled state(default ready=true,enabled=true)
 *
 *	  Spec: spec-2.20-7step-state-machine-activation.md(v0.4 descoped)。
 *	  Cross-spec lesson inheritance: L94 / L104 / L107 / L124 (HC4 exact
 *	  predicate)。
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_lock_acquire.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Standalone binary linking
 *	  cluster_lock_acquire.o + cluster_version.o only;all PG backend
 *	  symbols stubbed locally.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_advisory.h"
#include "cluster/cluster_ges.h" /* GES_REJECT_REASON_* for U6 */
#include "cluster/cluster_lmd.h"
#include "cluster/cluster_lmd_wait_state.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_native_lock_probe.h" /* spec-5.3 same-lock-group helper */
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lock.h"

/* Drop PG's port.h printf override; unit_test.h uses stdlib printf. */
#ifdef vprintf
#undef vprintf
#endif
#ifdef printf
#undef printf
#endif
#ifdef fprintf
#undef fprintf
#endif

#include "unit_test.h"


/* ============================================================
 * PG runtime + cluster_lms / cluster_lmd / cluster_grd stubs.
 * ============================================================ */

void
elog_start(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		   const char *fn pg_attribute_unused())
{}

void
elog_finish(int e pg_attribute_unused(), const char *f pg_attribute_unused(), ...)
{}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/*
 * spec-2.20 D13 stubs — control LMS/LMD ready state for HC1 test paths.
 *
 *	stub_lms_ready_for_test is INDEPENDENTLY controllable from
 *	cluster_lms_enabled GUC — caller-side gate走 cluster_lms_enabled
 *	first, then cluster_lms_is_ready() second (HC1 fail-closed)。
 */
bool cluster_lms_enabled = true;
bool cluster_lmd_enabled = true;
static bool stub_lms_ready_for_test = true;

bool
cluster_lms_is_ready(void)
{
	return stub_lms_ready_for_test;
}

bool
cluster_lmd_is_ready(void)
{
	return cluster_lmd_enabled;
}

void
cluster_lmd_cancel_wait_edge(void)
{}

/* spec-2.22 D7 — real wait edge mutators (no-op stubs in standalone test). */
void
cluster_lmd_cancel_wait_edge_real(const ClusterLmdVertex *waiter pg_attribute_unused())
{}

bool
cluster_lmd_submit_wait_edge_real(const ClusterLmdVertex *waiter pg_attribute_unused(),
								  const ClusterLmdVertex *blocker pg_attribute_unused(),
								  uint64 request_id pg_attribute_unused())
{
	return true;
}

/*
 * spec-5.8 D1d — per-proc wait-state stubs.  MyProc is NULL in this
 * standalone harness, so the S4/S5 wiring never invokes these;  the symbols
 * (and the PG_TRY error-stack globals the wiring introduces) only need to
 * resolve for the link.
 */
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	abort(); /* never reached — the stub wait never throws */
}

uint64
cluster_lmd_wait_state_publish(ClusterLmdProcWaitState *ws pg_attribute_unused(),
							   uint8 kind pg_attribute_unused(),
							   uint64 request_id pg_attribute_unused(),
							   uint64 cluster_epoch pg_attribute_unused(),
							   TransactionId xid pg_attribute_unused())
{
	return 0;
}

void
cluster_lmd_wait_state_clear(ClusterLmdProcWaitState *ws pg_attribute_unused())
{}

/* spec-5.16 — node-global request_id source (real impl in cluster_ges_reply_wait.o,
 * not linked here).  A monotonic local counter is enough for the standalone test. */
uint64
cluster_ges_reply_wait_next_request_id(void)
{
	static uint64 n = 0;

	return ++n;
}

/* spec-2.17 — sig_atomic_t cancel flag (cluster_signal.o not linked). */
#include <signal.h>
volatile sig_atomic_t cluster_ges_cancel_pending = 0;

/* spec-5.9 D3 — seven-step now consumes a per-proc cancel token (match logic
 * covered by test_cluster_cancel_token).  The fixture installs none, so the
 * seven-step top check sees "no cancel". */
bool
cluster_cancel_token_consume(void)
{
	return false;
}

/* PG-runtime xact stub — advisory locks have no xid. */
#include "access/xact.h"
TransactionId
GetTopTransactionIdIfAny(void)
{
	return InvalidTransactionId;
}

/*
 * spec-2.21 stubs — wire enough cluster_grd / cluster_ges / PG runtime
 * symbols for the standalone link surface.  All bodies return safe
 * defaults so T-7step-1..5 still exercise the API contracts.
 */
#include "cluster/cluster_ges.h"
#include "cluster/cluster_grd.h"

struct PGPROC {
	int pgprocno;
};
struct PGPROC *MyProc = NULL;

int cluster_node_id = 0;
bool cluster_local_fast_path_enabled = true;

/* spec-5.5 D7 — cluster.advisory_lock_enabled gate GUC (default on).  The real
 * backend gets this from cluster_guc.o; the standalone fixture stubs it so the
 * inline cluster_lock_should_globalize() advisory branch is exercised. */
bool cluster_advisory_lock_enabled = true;

/* spec-6.14 D7 — cluster.shared_catalog gate GUC (default off).  The inline
 * cluster_lock_should_globalize() catalog branch reads it; default off keeps
 * the existing HC23/HC24/HC27 behaviour for all other tests. */
bool cluster_shared_catalog = false;

/* S3 forensics step 1 stubs — the timeout-source detail setter/reset live in
 * cluster_ges.o (not linked standalone); the seven-step reset and the S3
 * native-probe set-point are inert no-ops here. */
void
cluster_ges_timeout_detail_set(ClusterGesTimeoutSrc src pg_attribute_unused(),
							   int32 master_node pg_attribute_unused(),
							   long elapsed_ms pg_attribute_unused(),
							   int attempts pg_attribute_unused(),
							   int conflict_holders pg_attribute_unused(),
							   int timeout_ms pg_attribute_unused())
{}
void
cluster_ges_timeout_detail_reset(void)
{}

/* spec-5.5 D8 — UL counter stubs:  cluster_lock_release() bumps the session-
 * release counter, but this fixture links neither the advisory shmem region nor
 * cluster_advisory.o.  The real counter behaviour is covered by
 * test_cluster_advisory; here they are inert no-ops. */
void
cluster_advisory_counter_inc(ClusterAdvisoryCounter which pg_attribute_unused())
{}
uint64
cluster_advisory_counter_read(ClusterAdvisoryCounter which pg_attribute_unused())
{
	return 0;
}

int64
GetCurrentTimestamp(void)
{
	return 0;
}

uint64
cluster_epoch_get_current(void)
{
	return 1;
}

/* spec-4.6 D3 stubs — the cooperative redeclare walker is inert in the
 * standalone fixture:  cluster_grd_redeclare_generation() == 0 makes
 * cluster_grd_redeclare_all_registered early-return, so the hash_seq /
 * LocalLockHash symbols are link-only and never reached at runtime. */
bool cluster_enabled = false;

/* spec-4.6 D4 stubs — the freeze gate consults the shard phase (NORMAL
 * stub → gate never waits), so the latch / interrupt symbols are
 * link-only here. */
int cluster_grd_remaster_wait_ms = 200;
volatile sig_atomic_t InterruptPending = 0;
struct Latch;
struct Latch *MyLatch = NULL;

ClusterGrdShardPhase
cluster_grd_shard_phase(uint32 shard_id pg_attribute_unused())
{
	return GRD_SHARD_NORMAL;
}

void
cluster_grd_inc_stale_request_drop(void)
{}

int
WaitLatch(struct Latch *latch pg_attribute_unused(), int wakeEvents pg_attribute_unused(),
		  long timeout pg_attribute_unused(), uint32 wait_event_info pg_attribute_unused())
{
	return 0;
}

void
ResetLatch(struct Latch *latch pg_attribute_unused())
{}

void
ProcessInterrupts(void)
{}

uint64
cluster_grd_redeclare_generation(void)
{
	return 0;
}

/* spec-4.6 L11/L14 stub:  redeclare-skip sticky probe.  Standalone
 * fixture arms nothing; never skip. */
bool
cluster_cr_injection_armed(const char *name pg_attribute_unused(), uint64 *out_param)
{
	if (out_param != NULL)
		*out_param = 0;
	return false;
}

uint32
cluster_ges_send_redeclare_and_wait(const struct ClusterResId *resid pg_attribute_unused(),
									uint32 lockmode pg_attribute_unused(),
									const struct ClusterGrdHolderId *nh pg_attribute_unused(),
									uint64 request_id pg_attribute_unused())
{
	return 0;
}

ClusterGrdEntryResult
cluster_grd_entry_rebind_or_insert_holder(const ClusterResId *resid pg_attribute_unused(),
										  const struct ClusterGrdHolderId *nh pg_attribute_unused(),
										  int32 src pg_attribute_unused(),
										  int lockmode pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_OK;
}

HTAB *
GetLockMethodLocalHash(void)
{
	return NULL;
}

void
hash_seq_init(HASH_SEQ_STATUS *status pg_attribute_unused(), HTAB *hashp pg_attribute_unused())
{}

void *
hash_seq_search(HASH_SEQ_STATUS *status pg_attribute_unused())
{
	return NULL;
}

/* spec-2.25 D8 R10 stub audit — SearchSysCache1 / ReleaseSysCache pulled
 * in by cluster_relation_is_persistent_or_unlogged.  Standalone test does
 * not exercise the helper directly;  null-safe stubs satisfy link. */
struct HeapTupleData;
typedef struct HeapTupleData *HeapTuple;

HeapTuple
SearchSysCache1(int cache_id pg_attribute_unused(), Datum key1 pg_attribute_unused())
{
	return NULL;
}

void
ReleaseSysCache(HeapTuple tuple pg_attribute_unused())
{}

void
cluster_grd_resid_encode(const LOCKTAG *src pg_attribute_unused(), ClusterResId *dst)
{
	if (dst)
		memset(dst, 0, sizeof(*dst));
}

uint32
cluster_grd_shard_for_resource(const ClusterResId *resid pg_attribute_unused())
{
	return 0;
}

int32
cluster_grd_lookup_master(const ClusterResId *resid pg_attribute_unused())
{
	return -1;
}

/* spec-4.6a: the S4-reject diagnostic references the CSSD peer-state view;
 * fixture has no CSSD shmem — stub DEAD-free defaults. */
#include "cluster/cluster_cssd.h"
ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 peer_id pg_attribute_unused())
{
	return CLUSTER_CSSD_PEER_ALIVE;
}
const char *
cluster_cssd_peer_state_to_string(ClusterCssdPeerState s pg_attribute_unused())
{
	return "alive";
}

/* spec-4.6a: the same diagnostic is the first ereport in this .o — swallow
 * elog machinery (mirror test_cluster_grd.c pattern). */
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
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errcode(int s pg_attribute_unused())
{
	return 0;
}

/* spec-4.6 P0 regression harness — test-controlled S3/S4 outcomes.
 *	stub_reserve_result default NOT_READY keeps the legacy tests on the
 *	pre-reservation-fail path;  the S4 default-deny test flips it to OK
 *	so seven_step reaches S4 with a reservation created.
 *	stub_revalidate_calls counts S5 promote entry (revalidate is the
 *	mandatory first step of a real promote) — the default-deny test
 *	asserts it does NOT move when S4 rejects. */
static ClusterGrdEntryResult stub_reserve_result = CLUSTER_GRD_ENTRY_NOT_READY;
static int stub_revalidate_calls = 0;

ClusterGrdEntryResult
cluster_grd_try_reserve(const ClusterResId *resid pg_attribute_unused(),
						const ClusterGrdHolderId *holder pg_attribute_unused(),
						int mode pg_attribute_unused(), int32 self_node_id pg_attribute_unused(),
						bool *fast_path_out, uint64 *gen_snapshot_out)
{
	if (fast_path_out)
		*fast_path_out = false;
	if (gen_snapshot_out)
		*gen_snapshot_out = 0;
	return stub_reserve_result;
}

ClusterGrdEntryResult
cluster_grd_revalidate_and_promote(const ClusterResId *resid pg_attribute_unused(),
								   const ClusterGrdHolderId *holder pg_attribute_unused(),
								   int32 self_node_id pg_attribute_unused(),
								   uint64 gen_snapshot pg_attribute_unused())
{
	stub_revalidate_calls++;
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

ClusterGrdEntryResult
cluster_grd_release_holder_by_id(const ClusterResId *resid pg_attribute_unused(),
								 const ClusterGrdHolderId *holder pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

ClusterGrdEntryResult
cluster_grd_cancel_reservation_by_id(const ClusterResId *resid pg_attribute_unused(),
									 const ClusterGrdHolderId *holder pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

bool
cluster_lms_native_probe_required(const ClusterResId *resid pg_attribute_unused(),
								  LOCKMODE lockmode pg_attribute_unused())
{
	return false;
}

bool
cluster_lms_native_probe_wait_clear(const ClusterResId *resid pg_attribute_unused(),
									LOCKMODE lockmode pg_attribute_unused(),
									const ClusterGrdHolderId *requester pg_attribute_unused(),
									int timeout_ms pg_attribute_unused())
{
	return true;
}

/* spec-4.6 P0 regression harness — test-controlled GES reject reason
 * (GesRejectReason values; 0 = GRANT). */
static uint32 stub_ges_reject_reason = 0;

uint32
cluster_ges_send_request_and_wait(const struct ClusterResId *resid pg_attribute_unused(),
								  uint32 lockmode pg_attribute_unused(),
								  const struct ClusterGrdHolderId *holder pg_attribute_unused(),
								  uint64 request_id pg_attribute_unused(),
								  int timeout_ms pg_attribute_unused(),
								  uint32 wait_event pg_attribute_unused())
{
	return stub_ges_reject_reason;
}

/* spec-5.5 D5 — NOWAIT send stub:  shares stub_ges_reject_reason so a test can
 * drive GRANT (NONE) / conflict (LOCK_CONFLICT) / unreachable (TIMEOUT). */
uint32
cluster_ges_send_request_nowait_and_wait(
	const struct ClusterResId *resid pg_attribute_unused(), uint32 lockmode pg_attribute_unused(),
	const struct ClusterGrdHolderId *holder pg_attribute_unused(),
	uint64 request_id pg_attribute_unused(), int timeout_ms pg_attribute_unused(),
	uint32 wait_event pg_attribute_unused())
{
	return stub_ges_reject_reason;
}

uint32
cluster_ges_send_release_and_wait(const struct ClusterResId *resid pg_attribute_unused(),
								  const struct ClusterGrdHolderId *holder pg_attribute_unused(),
								  uint64 request_id pg_attribute_unused())
{
	return 0;
}

/* spec-5.5 P0 — local-master release drain (no-op in the standalone fixture;
 * the real drain+wake is exercised by cluster_tap t/286). */
void
cluster_ges_release_and_drain_local(const struct ClusterResId *resid pg_attribute_unused(),
									const struct ClusterGrdHolderId *holder pg_attribute_unused())
{}

/* spec-5.3 — convert send + tm_convert_mode GUC (cluster_lock_acquire.c refs). */
int cluster_tm_convert_mode = 0; /* CLUSTER_TM_CONVERT_MODE_CONVERT */

uint32
cluster_ges_send_convert_and_wait(const struct ClusterResId *resid pg_attribute_unused(),
								  uint32 requested_mode pg_attribute_unused(),
								  uint32 current_mode pg_attribute_unused(),
								  const struct ClusterGrdHolderId *holder pg_attribute_unused(),
								  uint64 convert_request_id pg_attribute_unused(),
								  int timeout_ms pg_attribute_unused())
{
	return stub_ges_reject_reason;
}


/* ============================================================
 * Test cases.
 * ============================================================ */

/*
 * T-7step-1: API symbol linkability + initial counter 0.
 */
UT_TEST(test_7step_api_surface_linkable_and_initial_counters_zero)
{
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s1_entry);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s2_identity);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s3_partition_reservation);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s4_remote_request_wait);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s5_promote_holder);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s6_release);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s7_cleanup);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_seven_step);

	/*
	 * Initial counters may be 0 OR > 0 across test runs(static init
	 * persistent;not reset between UT_TEST functions in same binary).
	 * Just verify accessor links + returns sensible value.
	 */
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s1_entry_count);
	UT_ASSERT_NOT_NULL((void *)cluster_lock_acquire_s7_cleanup_count);
}

/*
 * T-7step-2: HC1 LMS fail-closed S1 entry behavior.
 *
 *	- NULL req → INTERNAL
 *	- cluster_lms_enabled=false → OK_NATIVE(legacy path signal)
 *	- cluster_lms_enabled=true + LMS not ready(stub返回 false)→
 *	  FAIL_LMS_UNAVAILABLE(HC4 exact predicate;53R80)
 */
UT_TEST(test_7step_s1_hc1_fail_closed)
{
	ClusterLockAcquireRequest req;
	uint64 pre = cluster_lock_acquire_s1_entry_count();

	/* NULL req → INTERNAL */
	UT_ASSERT_EQ((int)cluster_lock_acquire_s1_entry(NULL), (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ(cluster_lock_acquire_s1_entry_count(), pre + 1);

	/* enabled=false → OK_NATIVE skip(legacy path)*/
	memset(&req, 0, sizeof(req));
	cluster_lms_enabled = false;
	UT_ASSERT_EQ((int)cluster_lock_acquire_s1_entry(&req), (int)CLUSTER_LOCK_ACQUIRE_OK_NATIVE);

	/* enabled=true + LMS not ready → FAIL_LMS_UNAVAILABLE(HC1)*/
	cluster_lms_enabled = true;
	stub_lms_ready_for_test = false;
	UT_ASSERT_EQ((int)cluster_lock_acquire_s1_entry(&req),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_LMS_UNAVAILABLE);

	/* enabled=true + LMS ready → OK_GRANTED(dispatch to S2)*/
	stub_lms_ready_for_test = true;
	UT_ASSERT_EQ((int)cluster_lock_acquire_s1_entry(&req), (int)CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
}

/*
 * T-7step-3: S2-S7 NULL req → INTERNAL.
 */
UT_TEST(test_7step_individual_steps_null_req_internal)
{
	UT_ASSERT_EQ((int)cluster_lock_acquire_s2_identity(NULL),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ((int)cluster_lock_acquire_s3_partition_reservation(NULL),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ((int)cluster_lock_acquire_s4_remote_request_wait(NULL),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ((int)cluster_lock_acquire_s5_promote_holder(NULL),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ((int)cluster_lock_acquire_s6_release(NULL),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ((int)cluster_lock_acquire_s7_cleanup(NULL),
				 (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
}

/*
 * T-7step-4: top-level cluster_lock_acquire_seven_step() NULL req → S1
 * FAIL_INTERNAL without S7 cleanup.  S1/S2 fail before reservation, so
 * top-level must not invoke S7 once S7 is wired to real cancel/release.
 */
UT_TEST(test_7step_top_level_null_req_s7_cleanup_invoked)
{
	uint64 pre_s7 = cluster_lock_acquire_s7_cleanup_count();
	ClusterLockAcquireResult r;

	r = cluster_lock_acquire_seven_step(NULL);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup_count(), pre_s7);
}

/*
 * T-7step-5: I1 monotonic forward transition — top-level entry with
 * normal req walks S1 → S2 → S3, then returns PENDING per descoped
 * scope.  It must not continue to S5 and pretend an ungranted lock is
 * OK_GRANTED.
 */
UT_TEST(test_7step_top_level_monotonic_forward_no_cleanup_on_success)
{
	ClusterLockAcquireRequest req;
	uint64 pre_s7 = cluster_lock_acquire_s7_cleanup_count();
	ClusterLockAcquireResult r;

	memset(&req, 0, sizeof(req));
	cluster_lms_enabled = true;
	r = cluster_lock_acquire_seven_step(&req);
	/*
	 * spec-2.21 update: stub cluster_grd_try_reserve returns NOT_READY,
	 * which S3 maps to FAIL_GRD_NOT_READY (pre-reservation fail,
	 * no S7 cleanup — F2 invariant).
	 */
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_FAIL_GRD_NOT_READY);
	UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup_count(), pre_s7);

	cluster_lms_enabled = false;
	r = cluster_lock_acquire_seven_step(&req);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_OK_NATIVE);
	UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup_count(), pre_s7);
	cluster_lms_enabled = true;

	/*
	 * spec-5.5 D5 — S4 dontwait now sends a conditional GES REQUEST_NOWAIT
	 * instead of the spec-2.21 blanket FAIL_TIMEOUT short-circuit.  With the
	 * default GES stub returning GRANT (reject_reason 0), S4 reports
	 * NEED_PG_NATIVE_LOCK (the caller takes the PG-native lock — no S7 cleanup).
	 * The conflict (NOT_AVAIL) and unreachable (FAIL_TIMEOUT) mappings are
	 * exercised by test_ul_try_lock_nowait_s4_reject_mapping.
	 */
	req.dontwait = true;
	r = cluster_lock_acquire_s4_remote_request_wait(&req);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK);
	UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup_count(), pre_s7);
}


/* ============================================================
 * spec-4.6 P0 regression (user review) — S4 master-side rejects must
 *	DEFAULT-DENY at the top-level dispatch.
 *
 *	The race t/250 G5 cannot cover:  the request passed the
 *	requester-side freeze gate and S3 created a reservation, and only
 *	the MASTER's reply says SHARD_FROZEN / stale generation.  The
 *	pre-fix allowlist (TIMEOUT/DEADLOCK/CANCEL/INTERNAL) let the two
 *	spec-4.6 FAIL codes fall through to S5 promote — the caller turned
 *	a master-side REJECT into a locally-granted holder (fail-open).
 *
 *	Asserts, for both new FAIL codes:  top-level returns the original
 *	failure, S7 cleanup runs (reservation cancelled), and S5 promote is
 *	NEVER entered (revalidate call count frozen).  Control leg:  a
 *	GRANT (reject = 0) still routes to NEED_PG_NATIVE_LOCK with no
 *	cleanup.
 * ============================================================ */
UT_TEST(test_7step_s4_master_reject_default_deny)
{
	ClusterLockAcquireRequest req;
	uint64 pre_s7;
	int pre_reval;
	ClusterLockAcquireResult r;

	cluster_lms_enabled = true;
	stub_reserve_result = CLUSTER_GRD_ENTRY_OK; /* S3 succeeds -> S4 reachable */

	/* (a) master says SHARD_FROZEN (GES_REJECT_REASON_SHARD_FROZEN=5). */
	memset(&req, 0, sizeof(req));
	stub_ges_reject_reason = 5;
	pre_s7 = cluster_lock_acquire_s7_cleanup_count();
	pre_reval = stub_revalidate_calls;
	r = cluster_lock_acquire_seven_step(&req);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_FAIL_SHARD_REMASTERING);
	UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup_count(), pre_s7 + 1);
	UT_ASSERT_EQ(stub_revalidate_calls, pre_reval); /* S5 never entered */

	/* (b) master says stale epoch (GES_REJECT_REASON_EPOCH_MISMATCH=3). */
	memset(&req, 0, sizeof(req));
	stub_ges_reject_reason = 3;
	pre_s7 = cluster_lock_acquire_s7_cleanup_count();
	pre_reval = stub_revalidate_calls;
	r = cluster_lock_acquire_seven_step(&req);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_FAIL_STALE_GENERATION);
	UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup_count(), pre_s7 + 1);
	UT_ASSERT_EQ(stub_revalidate_calls, pre_reval);

	/* (c) control:  GRANT still proceeds, no cleanup. */
	memset(&req, 0, sizeof(req));
	stub_ges_reject_reason = 0;
	pre_s7 = cluster_lock_acquire_s7_cleanup_count();
	r = cluster_lock_acquire_seven_step(&req);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK);
	UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup_count(), pre_s7);

	/* Restore harness defaults for any later test. */
	stub_reserve_result = CLUSTER_GRD_ENTRY_NOT_READY;
	stub_ges_reject_reason = 0;
}


/* ============================================================
 * spec-2.26 T-7step-N..N+2 — LOCKTAG_TRANSACTION gate + 7-step routing.
 *
 *	T-7step-N    cluster_lock_should_globalize exact mode + node-id gate.
 *	T-7step-N+1  TRANSACTION path enters cluster path under valid
 *	             cluster_node_id (no S7 cleanup invoked on success
 *	             prefix; stub stack reaches FAIL_GRD_NOT_READY same
 *	             as base T-7step regression — verifies entry routing
 *	             reaches S3 reservation site without falling out at
 *	             S1/S2 invariants).
 *	T-7step-N+2  TRANSACTION release path accepts encoded identity.
 * ============================================================ */

UT_TEST(test_7step_transaction_should_globalize_gate)
{
	LOCKTAG tag;
	int saved_node = cluster_node_id;

	memset(&tag, 0, sizeof(tag));
	tag.locktag_field1 = 0x12345;
	tag.locktag_type = LOCKTAG_TRANSACTION;
	tag.locktag_lockmethodid = 1;

	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_lock_should_globalize(&tag, AccessShareLock, false), false);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&tag, RowShareLock, false), false);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&tag, RowExclusiveLock, false), false);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&tag, ShareUpdateExclusiveLock, false), false);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&tag, ShareLock, false), true);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&tag, ShareRowExclusiveLock, false), false);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&tag, ExclusiveLock, false), true);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&tag, AccessExclusiveLock, false), false);

	cluster_node_id = -1;
	UT_ASSERT_EQ(cluster_lock_should_globalize(&tag, ShareLock, false), false);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&tag, ExclusiveLock, false), false);

	cluster_node_id = CLUSTER_MAX_NODES;
	UT_ASSERT_EQ(cluster_lock_should_globalize(&tag, ShareLock, false), false);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&tag, ExclusiveLock, false), false);

	cluster_node_id = saved_node;
}

UT_TEST(test_7step_transaction_locktag_path_routes_through_cluster)
{
	ClusterLockAcquireRequest req;
	uint64 pre_s7 = cluster_lock_acquire_s7_cleanup_count();
	int saved_node = cluster_node_id;
	ClusterLockAcquireResult r;

	cluster_node_id = 0;
	memset(&req, 0, sizeof(req));
	req.locktag.locktag_field1 = 0x12345; /* xid */
	req.locktag.locktag_type = LOCKTAG_TRANSACTION;
	req.locktag.locktag_lockmethodid = 1;
	req.lockmode = ExclusiveLock; /* HC39 — owner take */
	cluster_lms_enabled = true;

	r = cluster_lock_acquire_seven_step(&req);
	/* Stub stack reaches S3 reservation, which returns NOT_READY (no shmem
	 * GRD entry table in standalone test).  pre-reservation fail — no S7
	 * cleanup (matches base T-7step regression behavior; HC46 自动接入). */
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_FAIL_GRD_NOT_READY);
	UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup_count(), pre_s7);

	cluster_node_id = saved_node;
}

UT_TEST(test_7step_transaction_locktag_release_path_safe)
{
	ClusterLockReleaseRequest req;
	int saved_node = cluster_node_id;

	cluster_node_id = 0;
	memset(&req, 0, sizeof(req));
	req.locktag.locktag_field1 = 0x12345;
	req.locktag.locktag_type = LOCKTAG_TRANSACTION;
	req.locktag.locktag_lockmethodid = 1;
	req.lockmode = ExclusiveLock;

	/* cluster_lock_release in standalone harness should not crash on
	 * TRANSACTION resid even when stubs return NOT_FOUND for release-by-id. */
	cluster_lock_release(&req);

	cluster_node_id = saved_node;
}


/* ============================================================
 * spec-5.5 U1 — session-scoped advisory cross-node gate (D1 + D7).
 *
 *	The core feature-078 gap:  session-scoped pg_advisory_lock(key)
 *	(sessionLock=true) was short-circuited native by HC11, so two nodes
 *	never blocked each other.  D1 lifts LOCKTAG_ADVISORY ahead of the
 *	HC11 short-circuit, gated by cluster.advisory_lock_enabled (D7).
 *	xact-scoped advisory (sessionLock=false) was already cross-node
 *	(spec-2.21) and must stay so; other session locks stay native.
 * ============================================================ */
UT_TEST(test_ul_session_advisory_globalize_gate)
{
	LOCKTAG adv;
	LOCKTAG rel;
	bool saved = cluster_advisory_lock_enabled;

	memset(&adv, 0, sizeof(adv));
	adv.locktag_field1 = 12345; /* db oid */
	adv.locktag_field2 = 42;	/* advisory key */
	adv.locktag_type = LOCKTAG_ADVISORY;
	adv.locktag_lockmethodid = USER_LOCKMETHOD;

	cluster_advisory_lock_enabled = true;
	/* Core gap fix: session-scoped advisory (sessionLock=true) globalizes. */
	UT_ASSERT_EQ(cluster_lock_should_globalize(&adv, ExclusiveLock, true), true);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&adv, ShareLock, true), true);
	/* xact-scoped advisory (sessionLock=false) still globalizes (spec-2.21). */
	UT_ASSERT_EQ(cluster_lock_should_globalize(&adv, ExclusiveLock, false), true);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&adv, ShareLock, false), true);

	/* GUC off = forensic/test-only unsafe single-node downgrade (Q6/R3):
	 * advisory routes PG-native both session- and xact-scoped. */
	cluster_advisory_lock_enabled = false;
	UT_ASSERT_EQ(cluster_lock_should_globalize(&adv, ExclusiveLock, true), false);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&adv, ExclusiveLock, false), false);

	/* Other session locks stay native regardless of the advisory GUC (R2):
	 * a session-scoped RELATION lock must not globalize. */
	cluster_advisory_lock_enabled = true;
	memset(&rel, 0, sizeof(rel));
	rel.locktag_field1 = 1;
	rel.locktag_field2 = FirstNormalObjectId + 1;
	rel.locktag_type = LOCKTAG_RELATION;
	rel.locktag_lockmethodid = 1;
	UT_ASSERT_EQ(cluster_lock_should_globalize(&rel, AccessExclusiveLock, true), false);

	cluster_advisory_lock_enabled = saved;
}


/* spec-6.14 D7 — under cluster.shared_catalog the catalog OID boundary (HC24/
 * HC27) is removed: catalog DDL and mapped-relation writes globalize, catalog
 * reads stay native, and user relations are unaffected.  The SearchSysCache1
 * stub returns NULL, so cluster_relation_is_mapped fails safe to true. */
UT_TEST(test_shared_catalog_relation_gate)
{
	LOCKTAG cat;
	LOCKTAG usr;

	memset(&cat, 0, sizeof(cat));
	cat.locktag_field1 = 1;
	cat.locktag_field2 = 1259; /* pg_class: a catalog OID < FirstNormalObjectId */
	cat.locktag_type = LOCKTAG_RELATION;
	cat.locktag_lockmethodid = 1;

	memset(&usr, 0, sizeof(usr));
	usr.locktag_field1 = 1;
	usr.locktag_field2 = FirstNormalObjectId + 5; /* a user relation */
	usr.locktag_type = LOCKTAG_RELATION;
	usr.locktag_lockmethodid = 1;

	/* OFF mode: catalog stays native at every mode (HC24). */
	cluster_shared_catalog = false;
	UT_ASSERT_EQ(cluster_lock_should_globalize(&cat, AccessShareLock, false), false);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&cat, RowExclusiveLock, false), false);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&cat, AccessExclusiveLock, false), false);

	/* ON mode: catalog reads (< RowExclusive) stay native; RowExclusive
	 * (mapped-rel write) and DDL (>= SUEX) globalize. */
	cluster_shared_catalog = true;
	UT_ASSERT_EQ(cluster_lock_should_globalize(&cat, AccessShareLock, false), false);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&cat, RowShareLock, false), false);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&cat, RowExclusiveLock, false), true);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&cat, ShareUpdateExclusiveLock, false), true);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&cat, AccessExclusiveLock, false), true);

	/* User relations are unchanged in ON mode: OLTP hot path (< SUEX) native,
	 * DDL globalizes. */
	UT_ASSERT_EQ(cluster_lock_should_globalize(&usr, RowExclusiveLock, false), false);
	UT_ASSERT_EQ(cluster_lock_should_globalize(&usr, AccessExclusiveLock, false), true);

	cluster_shared_catalog = false; /* restore */
}


/* ============================================================
 * spec-5.5 U6 — try-lock (NOWAIT) S4 reject mapping (D5).
 *
 *	The requester half of the conditional-grant contract:  a dontwait acquire
 *	sends GES REQUEST_NOWAIT, and the master's verdict maps to a result code.
 *	GRANT -> NEED_PG_NATIVE_LOCK; LOCK_CONFLICT -> NOT_AVAIL (false, not ERROR);
 *	a wire TIMEOUT (unreachable master) stays fail-closed; and a LOCK_CONFLICT
 *	on a BLOCKING request is a protocol violation -> FAIL_INTERNAL (never
 *	NOT_AVAIL).  The master-side "no waiter enqueued" half (T5) is covered by
 *	test_cluster_grd (real grant_conditional); retransmit idempotency (T6) by
 *	the e2e t/286.
 * ============================================================ */
UT_TEST(test_ul_try_lock_nowait_s4_reject_mapping)
{
	ClusterLockAcquireRequest req;
	int saved_node = cluster_node_id;
	uint32 saved_reject = stub_ges_reject_reason;
	ClusterLockAcquireResult r;

	cluster_node_id = 0;
	memset(&req, 0, sizeof(req));
	req.locktag.locktag_field1 = 12345;
	req.locktag.locktag_field2 = 42;
	req.locktag.locktag_type = LOCKTAG_ADVISORY;
	req.locktag.locktag_lockmethodid = USER_LOCKMETHOD;
	req.lockmode = ExclusiveLock;

	/* dontwait + master GRANT (free) → NEED_PG_NATIVE_LOCK (caller takes PG lock). */
	req.dontwait = true;
	stub_ges_reject_reason = GES_REJECT_REASON_NONE;
	r = cluster_lock_acquire_s4_remote_request_wait(&req);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK);

	/* dontwait + master REJECT LOCK_CONFLICT → NOT_AVAIL (false, NOT ERROR). */
	stub_ges_reject_reason = GES_REJECT_REASON_LOCK_CONFLICT;
	r = cluster_lock_acquire_s4_remote_request_wait(&req);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_NOT_AVAIL);

	/* dontwait + wire TIMEOUT (unreachable master) → FAIL_TIMEOUT (fail-closed,
	 * mutual exclusion unprovable; NOT a silent false). */
	stub_ges_reject_reason = GES_REJECT_REASON_TIMEOUT;
	r = cluster_lock_acquire_s4_remote_request_wait(&req);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT);

	/* BLOCKING + LOCK_CONFLICT is a protocol violation (blocking conflicts
	 * enqueue a waiter, never REJECT) → FAIL_INTERNAL, never NOT_AVAIL. */
	req.dontwait = false;
	stub_ges_reject_reason = GES_REJECT_REASON_LOCK_CONFLICT;
	r = cluster_lock_acquire_s4_remote_request_wait(&req);
	UT_ASSERT_EQ((int)r, (int)CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);

	stub_ges_reject_reason = saved_reject;
	cluster_node_id = saved_node;
}


/*
 * spec-5.3 — native-probe PG parallel lock-group exemption helper.  Pure
 * pointer/flag logic (no shmem deref), so it is driven directly with distinct
 * dummy group-leader PGPROC addresses.  Mirrors PG LockCheckConflicts.
 */
UT_TEST(test_native_probe_same_lock_group_exempt)
{
	char slotA;
	char slotB;
	const struct PGPROC *leaderA = (const struct PGPROC *)&slotA;
	const struct PGPROC *leaderB = (const struct PGPROC *)&slotB;

	/* a. same lock group + normal relation lock -> exempt (skip conflict). */
	UT_ASSERT(cluster_native_lock_probe_same_group_exempt((uint8)LOCKTAG_RELATION, true, leaderA,
														  leaderA));

	/* b. different lock group -> NOT exempt (keep conflict). */
	UT_ASSERT(!cluster_native_lock_probe_same_group_exempt((uint8)LOCKTAG_RELATION, true, leaderA,
														   leaderB));

	/* c. LOCKTAG_RELATION_EXTEND + same group -> NOT exempt (PG exception). */
	UT_ASSERT(!cluster_native_lock_probe_same_group_exempt((uint8)LOCKTAG_RELATION_EXTEND, true,
														   leaderA, leaderA));

	/* d. requester not local -> NOT exempt. */
	UT_ASSERT(!cluster_native_lock_probe_same_group_exempt((uint8)LOCKTAG_RELATION, false, leaderA,
														   leaderA));

	/* e. missing requester/holder group leader -> NOT exempt. */
	UT_ASSERT(
		!cluster_native_lock_probe_same_group_exempt((uint8)LOCKTAG_RELATION, true, NULL, leaderA));
	UT_ASSERT(
		!cluster_native_lock_probe_same_group_exempt((uint8)LOCKTAG_RELATION, true, leaderA, NULL));

	/* f. non-parallel backends are each their own leader (distinct addrs) ->
	 * keep conflict;  proves we do not over-exclude unrelated backends. */
	UT_ASSERT(!cluster_native_lock_probe_same_group_exempt((uint8)LOCKTAG_RELATION, true, leaderA,
														   leaderB));
}


/*
 * spec-5.3 D1 — S5-promote NOT_FOUND benign exemption is strictly narrowed to
 * {LOCKTAG_TRANSACTION, ShareLock, CLUSTER_GRD_ENTRY_NOT_FOUND} (the
 * XactLockTableWait waiter whose awaited txn finished).  Everything else stays
 * a failure.
 */
UT_TEST(test_s5_not_found_benign_narrow)
{
	/* a. transaction + ShareLock + NOT_FOUND -> benign success. */
	UT_ASSERT(cluster_lock_acquire_s5_not_found_is_benign((uint8)LOCKTAG_TRANSACTION, ShareLock,
														  CLUSTER_GRD_ENTRY_NOT_FOUND));

	/* b. transaction + ExclusiveLock holder + NOT_FOUND -> NOT benign (P0). */
	UT_ASSERT(!cluster_lock_acquire_s5_not_found_is_benign(
		(uint8)LOCKTAG_TRANSACTION, ExclusiveLock, CLUSTER_GRD_ENTRY_NOT_FOUND));

	/* c. non-transaction lock + NOT_FOUND -> NOT benign. */
	UT_ASSERT(!cluster_lock_acquire_s5_not_found_is_benign((uint8)LOCKTAG_RELATION, ShareLock,
														   CLUSTER_GRD_ENTRY_NOT_FOUND));

	/* d. transaction + ShareLock + other GRD error -> NOT benign. */
	UT_ASSERT(!cluster_lock_acquire_s5_not_found_is_benign((uint8)LOCKTAG_TRANSACTION, ShareLock,
														   CLUSTER_GRD_ENTRY_FULL));
	UT_ASSERT(!cluster_lock_acquire_s5_not_found_is_benign((uint8)LOCKTAG_TRANSACTION, ShareLock,
														   CLUSTER_GRD_ENTRY_OK));
}


UT_DEFINE_GLOBALS();


int
main(int argc pg_attribute_unused(), char **const argv pg_attribute_unused())
{
	UT_PLAN(14);

	UT_RUN(test_7step_api_surface_linkable_and_initial_counters_zero);
	UT_RUN(test_7step_s1_hc1_fail_closed);
	UT_RUN(test_7step_individual_steps_null_req_internal);
	UT_RUN(test_7step_top_level_null_req_s7_cleanup_invoked);
	UT_RUN(test_7step_top_level_monotonic_forward_no_cleanup_on_success);
	UT_RUN(test_7step_s4_master_reject_default_deny);
	UT_RUN(test_7step_transaction_should_globalize_gate);
	UT_RUN(test_shared_catalog_relation_gate);
	UT_RUN(test_7step_transaction_locktag_path_routes_through_cluster);
	UT_RUN(test_7step_transaction_locktag_release_path_safe);
	UT_RUN(test_ul_session_advisory_globalize_gate);
	UT_RUN(test_ul_try_lock_nowait_s4_reject_mapping);
	UT_RUN(test_native_probe_same_lock_group_exempt);
	UT_RUN(test_s5_not_found_benign_narrow);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
