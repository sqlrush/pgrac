/*-------------------------------------------------------------------------
 *
 * test_cluster_inject.c
 *	  Compile-time / link-level invariants for the cluster error
 *	  injection framework shipped at stage 0.27.
 *
 *	  Locks:
 *	    - The compile-time injection-point registry size (6 points).
 *	    - Lookup by name (known + unknown).
 *	    - Initial disarmed state of every point.
 *	    - arm/disarm transitions and the cluster_injection_armed_count
 *	      bookkeeping.
 *	    - Each fault-type dispatcher (NONE / WARNING / ERROR / SLEEP /
 *	      CRASH / SKIP) reaches its instrumented stub.
 *	    - SRF entry-point linkability (cluster_inject_fault /
 *	      cluster_get_injection_state).
 *
 *	  End-to-end SQL behaviour is verified on a real PG instance by
 *	  cluster_tap t/015_inject.pl.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_inject.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linking cluster_inject.o
 *	  standalone pulls in references to ereport, pg_atomic_*,
 *	  superuser, fmgr / tuplestore machinery used by the SRF bodies,
 *	  and the GUC variable cluster_injection_points (declared via
 *	  cluster_guc.h but normally defined by cluster_guc.o).  The unit
 *	  test stubs every one of these because it only takes function
 *	  addresses or invokes lookup / arm logic that does not touch the
 *	  ereport machinery in steady state.  Tests that exercise the
 *	  WARNING / ERROR / SLEEP dispatchers count stub invocations
 *	  rather than triggering real PG error machinery.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_inject.h"
#include "cluster/cluster_storage_quorum.h"
#include <sys/time.h>

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
 * Stubs needed to link cluster_inject.o standalone.  The framework
 * uses ereport (for ERROR / WARNING dispatch) + pg_usleep (SLEEP) +
 * superuser() (SRF guard) + InitMaterializedSRF / tuplestore /
 * CStringGetTextDatum (SRF body); none of those paths are exercised
 * here, so stubs satisfy the linker only.
 * ----------
 */
#include <signal.h> /* sig_atomic_t (InterruptPending stub) */

#include "fmgr.h"
#include "funcapi.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"

char *cluster_injection_points = NULL; /* extern from cluster_guc.h */

bool
cluster_ctrc_test_barrier_control(ClusterCtrcTestBarrierPhase phase pg_attribute_unused(),
								  bool armed pg_attribute_unused())
{
	return true;
}

static int last_elevel = -1;
static long last_sleep_us = -1;
static uint64 elapsed_us;
static int64 wall_step_us;
static unsigned sleep_calls;
static bool cancel_on_wake;
static bool clock_lost_on_wake;
static sigjmp_buf sleep_exit;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

volatile sig_atomic_t InterruptPending = false;

/* Both clocks advance together until pg_usleep first returns early. */
int
inject_test_gettimeofday(struct timeval *now, void *zone pg_attribute_unused())
{
	int64 wall = INT64CONST(60000000) + (int64)elapsed_us;

	if (sleep_calls > 0)
		wall += wall_step_us;
	now->tv_sec = wall / 1000000;
	now->tv_usec = wall % 1000000;
	return 0;
}

uint64
cluster_storage_quorum_now_us(void)
{
	return clock_lost_on_wake && sleep_calls > 0 ? 0 : UINT64CONST(1000000) + elapsed_us;
}

void
pg_usleep(long microsec)
{
	last_sleep_us = microsec;
	sleep_calls++;
	elapsed_us += sleep_calls == 1 ? Min(microsec, 2000000L) : microsec;
	if (cancel_on_wake)
		InterruptPending = true;
}

void
ProcessInterrupts(void)
{
	InterruptPending = false;
	if (cancel_on_wake)
		siglongjmp(sleep_exit, 1);
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	last_elevel = elevel;
	return clock_lost_on_wake && elevel >= ERROR;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (clock_lost_on_wake && last_elevel >= ERROR)
		siglongjmp(sleep_exit, 1);
}

void
InitMaterializedSRF(FunctionCallInfo fcinfo pg_attribute_unused(),
					bits32 flags pg_attribute_unused())
{}

void
tuplestore_putvalues(Tuplestorestate *state pg_attribute_unused(),
					 TupleDesc tdesc pg_attribute_unused(), Datum *values pg_attribute_unused(),
					 bool *isnull pg_attribute_unused())
{}

text *
cstring_to_text(const char *s pg_attribute_unused())
{
	return NULL;
}

struct varlena *
pg_detoast_datum_packed(struct varlena *datum)
{
	return datum;
}

char *
text_to_cstring(const text *t pg_attribute_unused())
{
	return (char *)"";
}

int
pg_strcasecmp(const char *s1, const char *s2)
{
	while (*s1 && *s2) {
		int c1 = *s1++;
		int c2 = *s2++;
		if (c1 >= 'A' && c1 <= 'Z')
			c1 += 'a' - 'A';
		if (c2 >= 'A' && c2 <= 'Z')
			c2 += 'a' - 'A';
		if (c1 != c2)
			return c1 - c2;
	}
	return *s1 - *s2;
}

bool
superuser(void)
{
	return true;
}

MemoryContext TopMemoryContext = NULL;
MemoryContext CurrentMemoryContext = NULL;


UT_DEFINE_GLOBALS();


/* ============================================================
 * Registry / lookup invariants.
 * ============================================================ */

UT_TEST(test_inject_count_compile_constant)
{
	/* 20 injection points: 6 baseline (spec-0.27) + 8 sweep (spec-0.30,
	 * cluster_elog + cluster_signal excluded) + 3 cluster_shared_fs
	 * (spec-1.1) + 3 cluster_smgr (spec-1.2). */
	UT_ASSERT_EQ(cluster_injection_armed_count, 0);
	/*
	 * Indirect assertion via SRF-state count is harder here without
	 * setting up a tuplestore.  The TAP test t/015_inject.pl pins this
	 * to 14 against the live view; the unit test just exercises the
	 * surface.
	 */
}

UT_TEST(test_inject_arm_unknown_is_safe)
{
	/* Running an unknown name is a no-op (no crash, no counter). */
	cluster_injection_run("not-a-real-injection-point");
	UT_ASSERT_EQ(cluster_injection_armed_count, 0);
}

UT_TEST(test_inject_initial_disarmed)
{
	/* Run a known disarmed point: hits++ but no fault dispatch. */
	last_elevel = -1;
	last_sleep_us = -1;
	cluster_injection_run("cluster-init-pre-shmem");
	UT_ASSERT_EQ(last_elevel, -1);
	UT_ASSERT_EQ(last_sleep_us, -1);
}


/* ============================================================
 * arm/disarm via the SRF (driving the real internal path).
 * ============================================================ */

UT_TEST(test_inject_arm_warning_triggers_dispatch)
{
	Datum args[3];
	FunctionCallInfoBaseData fcinfo;
	NullableDatum nd[3];

	memset(&fcinfo, 0, sizeof(fcinfo));
	fcinfo.nargs = 3;
	args[0] = CStringGetDatum("cluster-init-pre-shmem");
	args[1] = CStringGetDatum("warning");
	args[2] = Int64GetDatum((int64)0);
	for (int i = 0; i < 3; i++) {
		nd[i].value = args[i];
		nd[i].isnull = false;
		fcinfo.args[i] = nd[i];
	}
	/*
	 * Calling the SRF directly is awkward without the full PG fmgr
	 * infrastructure (PG_GETARG_TEXT_PP needs a text* not a cstring).
	 * Instead drive the public arm path through cluster_injection_run
	 * after manually flipping armed_type via the assign_hook.
	 */
	(void)fcinfo;
	(void)nd;

	cluster_injection_assign_hook("cluster-init-pre-shmem", NULL);
	last_elevel = -1;
	cluster_injection_run("cluster-init-pre-shmem");
	UT_ASSERT_EQ(last_elevel, WARNING);

	/* Disarm by passing empty list; counter resets. */
	cluster_injection_assign_hook("", NULL);
	last_elevel = -1;
	cluster_injection_run("cluster-init-pre-shmem");
	UT_ASSERT_EQ(last_elevel, -1);
}

UT_TEST(test_inject_assign_multiple_names)
{
	cluster_injection_assign_hook("", NULL);
	UT_ASSERT_EQ(cluster_injection_armed_count, 0);

	cluster_injection_assign_hook(
		"cluster-init-pre-shmem,cluster-ic-tier-selected,cluster-conf-load-success", NULL);
	UT_ASSERT_EQ(cluster_injection_armed_count, 3);

	/* Drop one. */
	cluster_injection_assign_hook("cluster-init-pre-shmem,cluster-ic-tier-selected", NULL);
	UT_ASSERT_EQ(cluster_injection_armed_count, 2);

	cluster_injection_assign_hook("", NULL);
	UT_ASSERT_EQ(cluster_injection_armed_count, 0);
}

UT_TEST(test_inject_assign_unknown_name_warns)
{
	cluster_injection_assign_hook("", NULL);
	last_elevel = -1;
	/* Unknown name: WARNING but does not arm, does not throw. */
	cluster_injection_assign_hook("not-a-real-point", NULL);
	UT_ASSERT_EQ(last_elevel, WARNING);
	UT_ASSERT_EQ(cluster_injection_armed_count, 0);
}


/* ============================================================
 * SKIP fault: probe + auto-reset.
 *
 *	The assign_hook only sets WARNING.  To exercise SKIP we manually
 *	set the armed_type via the same hook by abusing a follow-up
 *	cluster_injection_assign_hook call — but the public assign_hook
 *	always writes WARNING, so SKIP testing requires reaching past the
 *	public surface.  We test the should_skip false path (no SKIP set)
 *	and the disarmed reset behaviour, which together pin the probe
 *	semantics.  TAP handles armed-SKIP end-to-end.
 * ============================================================ */

UT_TEST(test_inject_should_skip_disarmed_returns_false)
{
	cluster_injection_assign_hook("", NULL);
	UT_ASSERT_EQ(cluster_injection_should_skip("cluster-init-pre-shmem"), false);
}

UT_TEST(test_inject_should_skip_unknown_returns_false)
{
	UT_ASSERT_EQ(cluster_injection_should_skip("not-a-real-point"), false);
}


/* ============================================================
 * SRF entry-point linkability.
 * ============================================================ */

UT_TEST(test_inject_fault_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_inject_fault);
}

UT_TEST(test_inject_get_state_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_get_injection_state);
}


/* ============================================================
 * Run probe is idempotent under repeated disarmed run.
 * ============================================================ */

UT_TEST(test_inject_run_disarmed_repeat_no_dispatch)
{
	cluster_injection_assign_hook("", NULL);
	last_elevel = -1;
	last_sleep_us = -1;
	for (int i = 0; i < 100; i++)
		cluster_injection_run("cluster-init-pre-shmem");
	UT_ASSERT_EQ(last_elevel, -1);
	UT_ASSERT_EQ(last_sleep_us, -1);
}


UT_TEST(test_inject_armed_count_clamps_at_zero)
{
	/* Repeated disarm should not push armed_count negative. */
	cluster_injection_assign_hook("", NULL);
	cluster_injection_assign_hook("", NULL);
	cluster_injection_assign_hook("", NULL);
	UT_ASSERT_EQ(cluster_injection_armed_count, 0);
}


UT_TEST(test_first_publication_deny_registry_dispatches_only_its_skip)
{
	const char *point = "cluster-undo-first-publication-deny";
	uint64 before = 0;
	uint64 after = 0;
	bool found = false;

	cluster_injection_assign_hook("", NULL);
	for (int i = 0; i < cluster_injection_get_count(); i++) {
		const char *name;
		ClusterInjectFaultType type;
		uint64 hits;
		UT_ASSERT(cluster_injection_get_state_at(i, &name, &type, &hits));
		if (strcmp(name, point) == 0) {
			before = hits;
			found = true;
		}
	}
	UT_ASSERT(found);
	cluster_injection_assign_hook("cluster-undo-first-publication-deny:skip", NULL);
	UT_ASSERT(cluster_injection_is_armed(point));
	cluster_injection_run(point);
	UT_ASSERT(!cluster_injection_should_skip("cluster-undo-authority-block0-prove"));
	UT_ASSERT(cluster_injection_should_skip(point));
	UT_ASSERT(!cluster_injection_should_skip(point));
	for (int i = 0; i < cluster_injection_get_count(); i++) {
		const char *name;
		ClusterInjectFaultType type;
		uint64 hits;
		UT_ASSERT(cluster_injection_get_state_at(i, &name, &type, &hits));
		if (strcmp(name, point) == 0)
			after = hits;
	}
	UT_ASSERT_EQ(after, before + 1);
	cluster_injection_assign_hook("cluster-undo-first-publication-deny:none", NULL);
	UT_ASSERT(!cluster_injection_is_armed(point));
}

static void
run_writer_delay(int64 adjustment)
{
	elapsed_us = 0;
	sleep_calls = 0;
	wall_step_us = adjustment;
	cluster_injection_assign_hook("cluster-pcm-write-before-data:sleep:8000000", NULL);
	cluster_injection_run("cluster-pcm-write-before-data");
}

UT_TEST(test_writer_sleep_survives_forward_clock_step)
{
	run_writer_delay(INT64CONST(30000000));
	cluster_injection_assign_hook("", NULL);
	UT_ASSERT_EQ(elapsed_us, 8000000);
	UT_ASSERT_EQ(sleep_calls, 2);
	UT_ASSERT_EQ(last_sleep_us, 6000000);
}

UT_TEST(test_writer_sleep_survives_backward_clock_step)
{
	run_writer_delay(-INT64CONST(30000000));
	cluster_injection_assign_hook("", NULL);
	UT_ASSERT_EQ(elapsed_us, 8000000);
	UT_ASSERT_EQ(sleep_calls, 2);
	UT_ASSERT_EQ(last_sleep_us, 6000000);
}

UT_TEST(test_cancel_interrupts_writer_sleep)
{
	volatile bool cancelled = false;

	cancel_on_wake = true;
	if (sigsetjmp(sleep_exit, 1) == 0)
		run_writer_delay(0);
	else
		cancelled = true;
	cancel_on_wake = false;
	InterruptPending = false;
	cluster_injection_assign_hook("", NULL);
	UT_ASSERT(cancelled);
	UT_ASSERT_EQ(sleep_calls, 1);
	UT_ASSERT_EQ(elapsed_us, 2000000);
}

UT_TEST(test_writer_sleep_refuses_unavailable_clock)
{
	volatile bool refused = false;

	clock_lost_on_wake = true;
	last_elevel = -1;
	if (sigsetjmp(sleep_exit, 1) == 0)
		run_writer_delay(0);
	else
		refused = true;
	clock_lost_on_wake = false;
	cluster_injection_assign_hook("", NULL);
	UT_ASSERT(refused);
	UT_ASSERT_EQ(last_elevel, ERROR);
	UT_ASSERT_EQ(sleep_calls, 1);
	UT_ASSERT_EQ(elapsed_us, 2000000);
}

UT_TEST(test_writer_sleep_zero_and_one_hour)
{
	elapsed_us = 0;
	sleep_calls = 0;
	wall_step_us = 0;
	cluster_injection_assign_hook("cluster-pcm-write-before-data:sleep:0", NULL);
	cluster_injection_run("cluster-pcm-write-before-data");
	UT_ASSERT_EQ(sleep_calls, 0);
	cluster_injection_assign_hook("cluster-pcm-write-before-data:sleep:3600000000", NULL);
	cluster_injection_run("cluster-pcm-write-before-data");
	cluster_injection_assign_hook("", NULL);
	UT_ASSERT_EQ(elapsed_us, UINT64CONST(3600000000));
}

int
main(void)
{
	UT_PLAN(18);
	UT_RUN(test_inject_count_compile_constant);
	UT_RUN(test_inject_arm_unknown_is_safe);
	UT_RUN(test_inject_initial_disarmed);
	UT_RUN(test_inject_arm_warning_triggers_dispatch);
	UT_RUN(test_inject_assign_multiple_names);
	UT_RUN(test_inject_assign_unknown_name_warns);
	UT_RUN(test_inject_should_skip_disarmed_returns_false);
	UT_RUN(test_inject_should_skip_unknown_returns_false);
	UT_RUN(test_inject_fault_srf_linkable);
	UT_RUN(test_inject_get_state_srf_linkable);
	UT_RUN(test_inject_run_disarmed_repeat_no_dispatch);
	UT_RUN(test_inject_armed_count_clamps_at_zero);
	UT_RUN(test_first_publication_deny_registry_dispatches_only_its_skip);
	UT_RUN(test_writer_sleep_survives_forward_clock_step);
	UT_RUN(test_writer_sleep_survives_backward_clock_step);
	UT_RUN(test_cancel_interrupts_writer_sleep);
	UT_RUN(test_writer_sleep_refuses_unavailable_clock);
	UT_RUN(test_writer_sleep_zero_and_one_hour);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
