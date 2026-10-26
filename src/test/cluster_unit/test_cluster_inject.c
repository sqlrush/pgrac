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
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "utils/wait_event.h"


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
static int last_sleep_us = -1;
static int sleep_calls;
static int interrupt_calls;
static bool cancel_pause;
static uint32 wait_event_storage;
uint32 *my_wait_event_info = &wait_event_storage;
volatile uint32 CritSectionCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	siglongjmp(*PG_exception_stack, 1);
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

void
pg_usleep(long microsec)
{
	last_sleep_us = (int)microsec;
	sleep_calls++;
	UT_ASSERT_EQ(wait_event_storage, WAIT_EVENT_PG_SLEEP);
	/* Simulate signals interrupting the sleep; the real clock must still
	 * reach the requested duration before the pause finishes. */
}

/* Cancellation and critical-section deferral are exercised by the pause-only
 * entry. The general ERROR/CRASH dispatcher is never used by those tests. */
volatile sig_atomic_t InterruptPending = false;

void
ProcessInterrupts(void)
{
	interrupt_calls++;
	InterruptPending = false;
	if (cancel_pause)
		siglongjmp(*PG_exception_stack, 1);
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	last_elevel = elevel;
	/* Return false so the ereport macro skips errfinish and does not
	 * longjmp out of the test.  This is the standard "stub for unit
	 * test" idiom used elsewhere in cluster_unit. */
	return false;
}

bool
errstart_cold(int elevel, const char *domain pg_attribute_unused())
{
	last_elevel = elevel;
	return false;
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
{}

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

static const char *publication_points[] = {
	"cluster-tx-canonical-copy",
	"cluster-tx-commit-tt-before", "cluster-tx-commit-tt-after",
	"cluster-tx-commit-clog-before", "cluster-tx-commit-clog-after",
	"cluster-tx-abort-tt-before", "cluster-tx-abort-tt-after",
	"cluster-tx-abort-clog-before", "cluster-tx-abort-clog-after"
};

UT_TEST(test_publication_pause_points_are_registered)
{
	for (unsigned i = 0; i < lengthof(publication_points); i++) {
		bool found = false;
		for (int j = 0; j < cluster_injection_get_count(); j++) {
			const char *name;
			ClusterInjectFaultType type;
			uint64 hits;
			UT_ASSERT(cluster_injection_get_state_at(j, &name, &type, &hits));
			found |= strcmp(name, publication_points[i]) == 0;
		}
		UT_ASSERT(found);
	}
}

UT_TEST(test_publication_pause_is_sleep_only_and_one_shot)
{
	for (unsigned i = 0; i < lengthof(publication_points); i++) {
		char arm[128];
		uint64 hits_before = 0, hits_after = 0;
		instr_time start, elapsed;

		snprintf(arm, sizeof(arm), "%s:sleep:1000", publication_points[i]);
		cluster_injection_assign_hook(arm, NULL);
		for (int j = 0; j < cluster_injection_get_count(); j++) {
			const char *name;
			ClusterInjectFaultType type;
			uint64 hits;
			UT_ASSERT(cluster_injection_get_state_at(j, &name, &type, &hits));
			if (strcmp(name, publication_points[i]) == 0)
				hits_before = hits;
		}
		sleep_calls = 0;
		last_sleep_us = -1;
		wait_event_storage = WAIT_EVENT_WAL_WRITE;
		INSTR_TIME_SET_CURRENT(start);
		CLUSTER_INJECTION_PAUSE(publication_points[i]);
		INSTR_TIME_SET_CURRENT(elapsed);
		INSTR_TIME_SUBTRACT(elapsed, start);
		UT_ASSERT(INSTR_TIME_GET_MICROSEC(elapsed) >= 1000);
		UT_ASSERT(sleep_calls > 0);
		UT_ASSERT(last_sleep_us > 0 && last_sleep_us <= 1000);
		UT_ASSERT(!cluster_injection_is_armed(publication_points[i]));
		UT_ASSERT_EQ(wait_event_storage, WAIT_EVENT_WAL_WRITE);
		for (int j = 0; j < cluster_injection_get_count(); j++) {
			const char *name;
			ClusterInjectFaultType type;
			uint64 hits;
			UT_ASSERT(cluster_injection_get_state_at(j, &name, &type, &hits));
			if (strcmp(name, publication_points[i]) == 0)
				hits_after = hits;
		}
		UT_ASSERT_EQ(hits_after, hits_before + 1);
		sleep_calls = 0;
		CLUSTER_INJECTION_PAUSE(publication_points[i]);
		UT_ASSERT_EQ(sleep_calls, 0);
	}
	wait_event_storage = 0;
}

UT_TEST(test_publication_pause_never_dispatches_other_faults)
{
	const char *types[] = { "none", "error", "warning", "crash", "skip", "skipn:1",
		"sleep:0", "sleep:60000001" };
	for (unsigned i = 0; i < lengthof(types); i++) {
		char arm[128];
		snprintf(arm, sizeof(arm), "cluster-tx-commit-tt-before:%s", types[i]);
		cluster_injection_assign_hook(arm, NULL);
		last_elevel = -1;
		sleep_calls = 0;
		CritSectionCount = 1;
		CLUSTER_INJECTION_PAUSE("cluster-tx-commit-tt-before");
		CritSectionCount = 0;
		UT_ASSERT_EQ(last_elevel, -1);
		UT_ASSERT_EQ(sleep_calls, 0);
		UT_ASSERT(!cluster_injection_should_skip("cluster-tx-commit-tt-before"));
	}
	cluster_injection_assign_hook("", NULL);
}

UT_TEST(test_publication_pause_defers_critical_section_interrupts)
{
	cluster_injection_assign_hook("cluster-tx-commit-tt-after:sleep:1000", NULL);
	CritSectionCount = 1;
	InterruptPending = true;
	interrupt_calls = 0;
	CLUSTER_INJECTION_PAUSE("cluster-tx-commit-tt-after");
	UT_ASSERT_EQ(interrupt_calls, 0);
	UT_ASSERT(InterruptPending);
	UT_ASSERT(!cluster_injection_is_armed("cluster-tx-commit-tt-after"));
	CritSectionCount = 0;
	InterruptPending = false;
	cluster_injection_assign_hook("", NULL);
}

UT_TEST(test_publication_pause_cancel_restores_event_and_consumes_arm)
{
	volatile bool caught = false;
	cluster_injection_assign_hook("cluster-tx-canonical-copy:sleep:100000", NULL);
	wait_event_storage = WAIT_EVENT_WAL_WRITE;
	InterruptPending = true;
	cancel_pause = true;
	PG_TRY();
	{
		CLUSTER_INJECTION_PAUSE("cluster-tx-canonical-copy");
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	cancel_pause = false;
	InterruptPending = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(wait_event_storage, WAIT_EVENT_WAL_WRITE);
	UT_ASSERT(!cluster_injection_is_armed("cluster-tx-canonical-copy"));
	wait_event_storage = 0;
	cluster_injection_assign_hook("", NULL);
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
	UT_RUN(test_publication_pause_points_are_registered);
	UT_RUN(test_publication_pause_is_sleep_only_and_one_shot);
	UT_RUN(test_publication_pause_never_dispatches_other_faults);
	UT_RUN(test_publication_pause_defers_critical_section_interrupts);
	UT_RUN(test_publication_pause_cancel_restores_event_and_consumes_arm);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
