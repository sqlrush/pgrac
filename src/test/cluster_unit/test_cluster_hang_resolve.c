/*-------------------------------------------------------------------------
 *
 * test_cluster_hang_resolve.c
 *	  Unit tests for spec-5.12 Hang Manager disposition policy helpers.
 *
 *	  Exercises the pure decision layer (cluster_hang_resolve_policy.c):
 *	  victim score + deterministic comparator, HARD-skip / lock-holder
 *	  classification, the fail-CLOSED disposition gate, the tier->signal
 *	  map (default-deny, never SIGKILL), the mode predicates, the
 *	  root-first blocker ascent, the confirmation/hysteresis + tier-
 *	  escalation state machine, and the gate->counter mapping.  The
 *	  runtime gathering (pgstat + lock snapshots), DIAG tick hook, signal
 *	  helper, and SQL entry points live in cluster_hang_resolve.c and are
 *	  covered by cluster_tap t/305.
 *
 *	  Test IDs map to spec-5.12 §4.1:
 *	    U1  victim_score_cmp determinism + tie-break
 *	    U2  victim_score monotonicity (age up / blockers up / locks down)
 *	    U3  victim_skip_reason all branches (incl idle-in-tx no-xid holder)
 *	    U4  mode predicates (off/advisory/enforce)
 *	    U5  actionable gate reuse (5.11) + G-actionable reject
 *	    U6  tier->signal default-deny, never SIGKILL
 *	    U7  confirmation / hysteresis + tier escalation state machine
 *	    U8  root-first blocker ascent (chain / cycle / depth / no-blocker)
 *	    U9  gate->counter mapping monotonicity
 *	    U10 over-exclude gate verdict (synthetic COMPLETE + in-WFG)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_hang_resolve.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.12-hang-manager-disposition.md.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <string.h>

#include "cluster/cluster_hang.h"
#include "cluster/cluster_hang_resolve.h"
#include "utils/backend_status.h"
#include "miscadmin.h"

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
 * Stubs to link cluster_hang_resolve_policy.o + cluster_hang_policy.o
 * standalone.  The policy layers are pure arithmetic / struct manipulation;
 * the only externals are the Assert hook and the two DIAG LWLock no-ops used
 * by the spec-5.11 consistent-snapshot store helpers.
 * ----------
 */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}


UT_DEFINE_GLOBALS();


static ClusterHangVictim
make_victim(int pid, double score, int64 wait_age, int n_blocked)
{
	ClusterHangVictim v;

	memset(&v, 0, sizeof(v));
	v.pid = pid;
	v.score = score;
	v.wait_age_us = wait_age;
	v.n_blocked = n_blocked;
	return v;
}


/* ============================================================
 * U1 — victim_score_cmp determinism + tie-break
 * ============================================================ */

UT_TEST(test_victim_score_cmp)
{
	ClusterHangVictim a, b;

	/* higher score sorts first (cmp < 0) */
	a = make_victim(10, 9.0, 100, 1);
	b = make_victim(20, 1.0, 100, 1);
	UT_ASSERT(cluster_hang_victim_score_cmp(&a, &b) < 0);
	UT_ASSERT(cluster_hang_victim_score_cmp(&b, &a) > 0);
	/* antisymmetry */
	UT_ASSERT_EQ(cluster_hang_victim_score_cmp(&a, &b), -cluster_hang_victim_score_cmp(&b, &a));

	/* tie score -> wait_age DESC */
	a = make_victim(10, 5.0, 200, 1);
	b = make_victim(20, 5.0, 100, 9);
	UT_ASSERT(cluster_hang_victim_score_cmp(&a, &b) < 0);

	/* tie score + wait_age -> n_blocked DESC */
	a = make_victim(30, 5.0, 100, 7);
	b = make_victim(10, 5.0, 100, 3);
	UT_ASSERT(cluster_hang_victim_score_cmp(&a, &b) < 0);

	/* tie score + wait_age + n_blocked -> pid ASC (lowest pid first) */
	a = make_victim(10, 5.0, 100, 3);
	b = make_victim(20, 5.0, 100, 3);
	UT_ASSERT(cluster_hang_victim_score_cmp(&a, &b) < 0);

	/* fully identical -> 0 (and stable under reversal) */
	a = make_victim(10, 5.0, 100, 3);
	b = make_victim(10, 5.0, 100, 3);
	UT_ASSERT_EQ(cluster_hang_victim_score_cmp(&a, &b), 0);
}


/* ============================================================
 * U2 — victim_score monotonicity
 * ============================================================ */

UT_TEST(test_victim_score_monotonic)
{
	ClusterHangVictim base, more;
	const double wa = 0.5, wr = 0.3, wb = 0.2;
	double s_base, s_more;

	memset(&base, 0, sizeof(base));
	base.xact_age_us = 10 * 1000000L; /* 10s */
	base.n_locks_held = 3;
	base.n_blocked = 1;
	s_base = cluster_hang_victim_score(&base, wa, wr, wb);

	/* older xact -> higher score */
	more = base;
	more.xact_age_us = 100 * 1000000L;
	s_more = cluster_hang_victim_score(&more, wa, wr, wb);
	UT_ASSERT(s_more > s_base);

	/* more blocked waiters -> higher score */
	more = base;
	more.n_blocked = 9;
	s_more = cluster_hang_victim_score(&more, wa, wr, wb);
	UT_ASSERT(s_more > s_base);

	/* fewer locks held -> higher score (lower rollback cost) */
	more = base;
	more.n_locks_held = 0;
	s_more = cluster_hang_victim_score(&more, wa, wr, wb);
	UT_ASSERT(s_more > s_base);

	/* more locks held -> lower score */
	more = base;
	more.n_locks_held = 50;
	s_more = cluster_hang_victim_score(&more, wa, wr, wb);
	UT_ASSERT(s_more < s_base);
}


/* ============================================================
 * U3 — victim_skip_reason all branches
 * ============================================================ */

UT_TEST(test_victim_skip_reason)
{
	/* recovery dominates */
	UT_ASSERT_EQ((int)cluster_hang_victim_skip_reason(B_BACKEND, true, false, false, true),
				 (int)HANG_VICTIM_SKIP_RECOVERY);
	/* non-regular backend -> system */
	UT_ASSERT_EQ((int)cluster_hang_victim_skip_reason(B_BG_WORKER, false, false, false, true),
				 (int)HANG_VICTIM_SKIP_SYSTEM);
	UT_ASSERT_EQ((int)cluster_hang_victim_skip_reason(B_AUTOVAC_WORKER, false, false, false, true),
				 (int)HANG_VICTIM_SKIP_SYSTEM);
	/* prepared 2PC -> 2pc */
	UT_ASSERT_EQ((int)cluster_hang_victim_skip_reason(B_BACKEND, false, true, false, true),
				 (int)HANG_VICTIM_SKIP_2PC);
	/* operator-critical -> critical */
	UT_ASSERT_EQ((int)cluster_hang_victim_skip_reason(B_BACKEND, false, false, true, true),
				 (int)HANG_VICTIM_SKIP_CRITICAL);
	/* not granted-holding the conflicting lock -> not-lock-holder */
	UT_ASSERT_EQ((int)cluster_hang_victim_skip_reason(B_BACKEND, false, false, false, false),
				 (int)HANG_VICTIM_SKIP_NOT_LOCK_HOLDER);
	/* regular client backend, granted-holding the lock -> OK
	 * (this covers the idle-in-tx LOCK TABLE holder with NO xid: skip_reason
	 * does not look at xid, so a no-xid holder is still a valid victim) */
	UT_ASSERT_EQ((int)cluster_hang_victim_skip_reason(B_BACKEND, false, false, false, true),
				 (int)HANG_VICTIM_OK);
}


/* ============================================================
 * U4 — mode predicates
 * ============================================================ */

UT_TEST(test_mode_predicates)
{
	UT_ASSERT(!cluster_hang_mode_evaluates(HANG_RESOLVE_OFF));
	UT_ASSERT(cluster_hang_mode_evaluates(HANG_RESOLVE_ADVISORY));
	UT_ASSERT(cluster_hang_mode_evaluates(HANG_RESOLVE_ENFORCE));

	UT_ASSERT(!cluster_hang_mode_dispatches(HANG_RESOLVE_OFF));
	UT_ASSERT(!cluster_hang_mode_dispatches(HANG_RESOLVE_ADVISORY));
	UT_ASSERT(cluster_hang_mode_dispatches(HANG_RESOLVE_ENFORCE));
}


/* ============================================================
 * U5 — actionable gate reuse (5.11) + G-actionable reject
 * ============================================================ */

UT_TEST(test_actionable_gate)
{
	ClusterHangSampleSlot slot;

	memset(&slot, 0, sizeof(slot));

	/* reuse spec-5.11 cluster_hang_sample_actionable */
	slot.quality = HANG_SAMPLE_COMPLETE;
	slot.in_confirmed_deadlock = false;
	UT_ASSERT(cluster_hang_sample_actionable(&slot));

	slot.quality = HANG_SAMPLE_APPROXIMATE;
	UT_ASSERT(!cluster_hang_sample_actionable(&slot));
	slot.quality = HANG_SAMPLE_INCOMPLETE;
	UT_ASSERT(!cluster_hang_sample_actionable(&slot));
	slot.quality = HANG_SAMPLE_REMOTE_BOUNDARY;
	UT_ASSERT(!cluster_hang_sample_actionable(&slot));
	slot.quality = HANG_SAMPLE_COMPLETE;
	slot.in_confirmed_deadlock = true;
	UT_ASSERT(!cluster_hang_sample_actionable(&slot));

	/* G-actionable: a non-actionable sample never passes the disposition gate */
	UT_ASSERT_EQ((int)cluster_hang_gate_decide(false, false, HANG_VICTIM_OK, true),
				 (int)HANG_GATE_NOT_ACTIONABLE);
}


/* ============================================================
 * U6 — tier->signal default-deny, never SIGKILL
 * ============================================================ */

UT_TEST(test_tier_signal_default_deny)
{
	UT_ASSERT_EQ(cluster_hang_tier_signal(HANG_ACTION_CANCEL), SIGINT);
	UT_ASSERT_EQ(cluster_hang_tier_signal(HANG_ACTION_TERMINATE), SIGTERM);
	UT_ASSERT_EQ(cluster_hang_tier_signal(HANG_ACTION_NONE), 0);
	UT_ASSERT_EQ(cluster_hang_tier_signal(HANG_ACTION_DEGRADE), 0);
	/* unknown tier never falls through to a destructive signal */
	UT_ASSERT_EQ(cluster_hang_tier_signal((ClusterHangActionTier)99), 0);

	/* the disposition ladder never produces SIGKILL */
	UT_ASSERT_NE(cluster_hang_tier_signal(HANG_ACTION_CANCEL), SIGKILL);
	UT_ASSERT_NE(cluster_hang_tier_signal(HANG_ACTION_TERMINATE), SIGKILL);
}


/* ============================================================
 * U7 — confirmation / hysteresis + tier escalation
 * ============================================================ */

UT_TEST(test_confirm_state_machine)
{
	ClusterHangConfirmMap map;
	ClusterHangConfirmEntry *e;
	const int soft_ms = 5000;
	const TimestampTz t0 = 1000000000LL;

	memset(&map, 0, sizeof(map));

	/* round 1: first sighting -> 1 consecutive round */
	cluster_hang_confirm_begin_round(&map);
	e = cluster_hang_confirm_touch(&map, 111, 7);
	UT_ASSERT(e != NULL);
	if (e == NULL)
		return; /* guard the subsequent derefs (map cannot be full here) */
	UT_ASSERT_EQ(e->consecutive_rounds, 1);
	UT_ASSERT(!cluster_hang_confirm_ready(e, 2));
	UT_ASSERT(cluster_hang_confirm_ready(e, 1));
	cluster_hang_confirm_end_round(&map);

	/* round 2: same identity -> 2 consecutive rounds, now confirmed at 2 */
	cluster_hang_confirm_begin_round(&map);
	e = cluster_hang_confirm_touch(&map, 111, 7);
	UT_ASSERT_EQ(e->consecutive_rounds, 2);
	UT_ASSERT(cluster_hang_confirm_ready(e, 2));
	cluster_hang_confirm_end_round(&map);

	/* round 3: identity 111 not touched -> reset on end_round; new identity 222 = 1 */
	cluster_hang_confirm_begin_round(&map);
	e = cluster_hang_confirm_touch(&map, 222, 9);
	UT_ASSERT_EQ(e->consecutive_rounds, 1);
	cluster_hang_confirm_end_round(&map);

	/* round 4: 111 returns -> counts as a fresh start (1), not resumed at 3 */
	cluster_hang_confirm_begin_round(&map);
	e = cluster_hang_confirm_touch(&map, 111, 7);
	UT_ASSERT_EQ(e->consecutive_rounds, 1);

	/* tier escalation: fresh entry -> CANCEL */
	UT_ASSERT_EQ((int)cluster_hang_confirm_decide_tier(e, t0, soft_ms), (int)HANG_ACTION_CANCEL);
	cluster_hang_confirm_record_action(e, HANG_ACTION_CANCEL, t0);

	/* still within grace -> NONE (anti-thrash, do not re-signal) */
	UT_ASSERT_EQ((int)cluster_hang_confirm_decide_tier(e, t0 + 1000000LL, soft_ms),
				 (int)HANG_ACTION_NONE);
	/* grace elapsed -> escalate to TERMINATE */
	UT_ASSERT_EQ((int)cluster_hang_confirm_decide_tier(e, t0 + 5000000LL, soft_ms),
				 (int)HANG_ACTION_TERMINATE);
	cluster_hang_confirm_record_action(e, HANG_ACTION_TERMINATE, t0 + 5000000LL);

	/* within grace after terminate -> NONE */
	UT_ASSERT_EQ((int)cluster_hang_confirm_decide_tier(e, t0 + 6000000LL, soft_ms),
				 (int)HANG_ACTION_NONE);
	/* grace elapsed after terminate -> DEGRADE */
	UT_ASSERT_EQ((int)cluster_hang_confirm_decide_tier(e, t0 + 10000000LL, soft_ms),
				 (int)HANG_ACTION_DEGRADE);
	cluster_hang_confirm_record_action(e, HANG_ACTION_DEGRADE, t0 + 10000000LL);

	/* degrade is terminal -> stays DEGRADE */
	UT_ASSERT_EQ((int)cluster_hang_confirm_decide_tier(e, t0 + 99000000LL, soft_ms),
				 (int)HANG_ACTION_DEGRADE);
}


/* ============================================================
 * U8 — root-first blocker ascent
 * ============================================================ */

static void
set_slot(ClusterHangSampleStore *store, int idx, int pid, int blocker_pid)
{
	memset(&store->slots[idx], 0, sizeof(ClusterHangSampleSlot));
	store->slots[idx].pid = pid;
	store->slots[idx].blocker_pid = blocker_pid;
	store->slots[idx].quality = HANG_SAMPLE_COMPLETE;
}

UT_TEST(test_root_blocker_ascent)
{
	ClusterHangSampleStore store;
	bool trunc;
	int root;

	memset(&store, 0, sizeof(store));

	/* chain: waiter 100 -> 200 (also a waiter) -> 300 (NOT a sampled waiter = root) */
	set_slot(&store, 0, 100, 200);
	set_slot(&store, 1, 200, 300);
	store.n_samples = 2;

	trunc = true;
	root = cluster_hang_root_blocker_pid(&store, 0, 100, &trunc);
	UT_ASSERT_EQ(root, 300);
	UT_ASSERT(!trunc);

	/* direct: blocker not in store -> that blocker is the root */
	trunc = true;
	root = cluster_hang_root_blocker_pid(&store, 1, 100, &trunc);
	UT_ASSERT_EQ(root, 300);
	UT_ASSERT(!trunc);

	/* no blocker -> -1 */
	set_slot(&store, 2, 400, -1);
	store.n_samples = 3;
	root = cluster_hang_root_blocker_pid(&store, 2, 100, &trunc);
	UT_ASSERT_EQ(root, -1);

	/* cycle: 100 -> 200 -> 100 (defensive; actionable already excludes deadlock) */
	memset(&store, 0, sizeof(store));
	set_slot(&store, 0, 100, 200);
	set_slot(&store, 1, 200, 100);
	store.n_samples = 2;
	trunc = false;
	root = cluster_hang_root_blocker_pid(&store, 0, 100, &trunc);
	UT_ASSERT(trunc);
	UT_ASSERT(root > 0); /* still returns the last blocker pid seen */

	/* depth limit cuts a long chain short */
	memset(&store, 0, sizeof(store));
	set_slot(&store, 0, 100, 200);
	set_slot(&store, 1, 200, 300);
	set_slot(&store, 2, 300, 400);
	store.n_samples = 3;
	trunc = false;
	root = cluster_hang_root_blocker_pid(&store, 0, 1, &trunc);
	UT_ASSERT(trunc);
	UT_ASSERT(root > 0); /* truncated walk still returns a real blocker pid */
}


/* ============================================================
 * U9 — gate->counter mapping monotonicity
 * ============================================================ */

UT_TEST(test_counter_note_gate_reject)
{
	ClusterHangResolveCounters c;

	memset(&c, 0, sizeof(c));

	cluster_hang_counter_note_gate_reject(&c, HANG_GATE_NOT_ACTIONABLE);
	cluster_hang_counter_note_gate_reject(&c, HANG_GATE_NOT_ACTIONABLE);
	UT_ASSERT_EQ((int)c.non_actionable_skipped, 2);

	cluster_hang_counter_note_gate_reject(&c, HANG_GATE_OVER_EXCLUDED);
	UT_ASSERT_EQ((int)c.over_excluded, 1);

	cluster_hang_counter_note_gate_reject(&c, HANG_GATE_HARD_SKIP);
	UT_ASSERT_EQ((int)c.hard_skipped, 1);

	cluster_hang_counter_note_gate_reject(&c, HANG_GATE_NOT_CONFIRMED);
	UT_ASSERT_EQ((int)c.not_confirmed_yet, 1);

	/* PASS and NOT_LOCK_HOLDER have no dedicated counter -> nothing moves */
	cluster_hang_counter_note_gate_reject(&c, HANG_GATE_PASS);
	cluster_hang_counter_note_gate_reject(&c, HANG_GATE_NOT_LOCK_HOLDER);
	UT_ASSERT_EQ((int)c.non_actionable_skipped, 2);
	UT_ASSERT_EQ((int)c.over_excluded, 1);
	UT_ASSERT_EQ((int)c.hard_skipped, 1);
	UT_ASSERT_EQ((int)c.not_confirmed_yet, 1);
}


/* ============================================================
 * U10 — over-exclude gate verdict (synthetic COMPLETE + in-WFG)
 * ============================================================ */

UT_TEST(test_over_exclude_gate)
{
	/* COMPLETE + in 5.8 WFG -> over-excluded (v1 unreachable, forward GES-class) */
	UT_ASSERT_EQ((int)cluster_hang_gate_decide(true, true, HANG_VICTIM_OK, true),
				 (int)HANG_GATE_OVER_EXCLUDED);
	/* not in WFG, all clear -> PASS */
	UT_ASSERT_EQ((int)cluster_hang_gate_decide(true, false, HANG_VICTIM_OK, true),
				 (int)HANG_GATE_PASS);
	/* HARD-skip beats lock-holder / confirm */
	UT_ASSERT_EQ((int)cluster_hang_gate_decide(true, false, HANG_VICTIM_SKIP_SYSTEM, true),
				 (int)HANG_GATE_HARD_SKIP);
	/* not lock holder */
	UT_ASSERT_EQ((int)cluster_hang_gate_decide(true, false, HANG_VICTIM_SKIP_NOT_LOCK_HOLDER, true),
				 (int)HANG_GATE_NOT_LOCK_HOLDER);
	/* not yet confirmed */
	UT_ASSERT_EQ((int)cluster_hang_gate_decide(true, false, HANG_VICTIM_OK, false),
				 (int)HANG_GATE_NOT_CONFIRMED);
	/* gate ordering: actionable is checked before WFG */
	UT_ASSERT_EQ((int)cluster_hang_gate_decide(false, true, HANG_VICTIM_OK, true),
				 (int)HANG_GATE_NOT_ACTIONABLE);
}


/* ============================================================
 * U11 — resolved-confirmed accounting (Hardening v1.1 F4)
 *
 * cluster_hang_confirm_count_resolved() counts identities that were acted on
 * in a prior round (last_action_tier != NONE) but are not seen this round, so
 * the same number is added to resolved_confirmed on BOTH the normal and the
 * empty-sample round — a successful terminate that clears every sample (the
 * common success path) is no longer under-counted.
 * ============================================================ */

UT_TEST(test_confirm_count_resolved)
{
	ClusterHangConfirmMap map;
	ClusterHangConfirmEntry *e;
	const TimestampTz t0 = 1000000000LL;

	memset(&map, 0, sizeof(map));

	/* round 1: an identity is seen and terminated */
	cluster_hang_confirm_begin_round(&map);
	e = cluster_hang_confirm_touch(&map, 111, 7);
	UT_ASSERT(e != NULL);
	if (e == NULL)
		return;
	cluster_hang_confirm_record_action(e, HANG_ACTION_TERMINATE, t0);
	/* still seen this round -> nothing resolved yet */
	UT_ASSERT_EQ(cluster_hang_confirm_count_resolved(&map), 0);
	cluster_hang_confirm_end_round(&map);

	/* round 2 (empty-sample path simulation): identity NOT touched this round.
	 * It was acted on (TERMINATE) and is now gone -> exactly one resolved. */
	cluster_hang_confirm_begin_round(&map);
	UT_ASSERT_EQ(cluster_hang_confirm_count_resolved(&map), 1);
	cluster_hang_confirm_end_round(&map);

	/* round 3: the entry was evicted by end_round -> no longer counted */
	cluster_hang_confirm_begin_round(&map);
	UT_ASSERT_EQ(cluster_hang_confirm_count_resolved(&map), 0);

	/* an identity seen but NEVER acted on (last_action_tier == NONE) is not a
	 * resolution even when it later disappears: touch a fresh one, end, re-begin */
	e = cluster_hang_confirm_touch(&map, 222, 9);
	UT_ASSERT(e != NULL);
	cluster_hang_confirm_end_round(&map);
	cluster_hang_confirm_begin_round(&map);
	UT_ASSERT_EQ(cluster_hang_confirm_count_resolved(&map), 0);
	cluster_hang_confirm_end_round(&map);
}


/* ============================================================
 * Test runner
 * ============================================================ */

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_victim_score_cmp);
	UT_RUN(test_victim_score_monotonic);
	UT_RUN(test_victim_skip_reason);
	UT_RUN(test_mode_predicates);
	UT_RUN(test_actionable_gate);
	UT_RUN(test_tier_signal_default_deny);
	UT_RUN(test_confirm_state_machine);
	UT_RUN(test_root_blocker_ascent);
	UT_RUN(test_counter_note_gate_reject);
	UT_RUN(test_over_exclude_gate);
	UT_RUN(test_confirm_count_resolved);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
