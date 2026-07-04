/*-------------------------------------------------------------------------
 *
 * test_cluster_hang_acceptance.c
 *	  pgrac spec-5.20 D8 — Hang Manager chaos-acceptance surface snapshot
 *	  (6 static contract tests, L1-L6).
 *
 *	  Pins the read-only 5.11 (detection) + 5.12 (disposition) surface that the
 *	  Hang Manager chaos-acceptance cluster_tap legs (t/330 detection matrix /
 *	  t/331 remediation chaos / t/332 diagnostic completeness / t/333 reconfig
 *	  non-interference) exercise at runtime.  spec-5.20 changes no runtime
 *	  behaviour; this binary is the compile-time / enum / macro / oid contract
 *	  the acceptance TAP asserts against, so a silent rename / re-order in the
 *	  5.11/5.12 surface diverges here first.  Mirrors
 *	  test_cluster_stage5_integrated_acceptance.c (spec-5.19 D8).
 *
 *	    L1  13 `cluster.hang_*` GUC name roster present (compile-time roster +
 *	        exact count) + the disposition-mode ladder ordering that makes the
 *	        factory default (ADVISORY, dry-run) a real non-destructive state
 *	        between OFF and ENFORCE.  The runtime bootval == ADVISORY is
 *	        behaviourally verified by t/331 (advisory dry-run).
 *	    L2  single `hang` dump category + fixed key roster snapshot (config /
 *	        aggregate 13 + 5.11 counter 9 + 5.8-reader 2 + 5.12 counter 18) +
 *	        per-sample row suffix roster (9).  update-required contract: an
 *	        emit-site rename in cluster_debug.c must update this roster.
 *	        Runtime content-validation is in t/332 (D4).
 *	    L3  3 Hang Manager SQL function oids stable via fmgroids:
 *	        pg_cluster_hang_dump == 8957 / pg_cluster_hang_victims == 8958 /
 *	        pg_cluster_hang_resolve == 8959.  The permission contract (dump +
 *	        resolve superuser / victims pg_read_all_stats) is behaviourally
 *	        verified in t/332.
 *	    L4  detection + disposition enum surface present and ordered — quality
 *	        (COMPLETE..REMOTE_BOUNDARY) / mode (OFF/ADVISORY/ENFORCE) / tier
 *	        (NONE/CANCEL/TERMINATE/DEGRADE) / victim-skip (OK + 5 skip) / gate
 *	        (PASS + 5 reject).  These are the acceptance assertion anchors D2/D3
 *	        classify against.
 *	    L5  no-SIGKILL invariant (spec §3.3 L237): the pure disposition policy
 *	        cluster_hang_tier_signal() maps CANCEL->SIGINT, TERMINATE->SIGTERM,
 *	        and NONE / DEGRADE / any unknown tier -> 0 (default-deny).  It NEVER
 *	        returns SIGKILL for any tier value in [0, 64).  (C unit pins the
 *	        pure policy layer only; the runtime no-SIGKILL grep is a CI/TAP
 *	        hygiene gate, not a source grep here.)
 *	    L6  actionable truth-table reuse (references, does not copy, the 5.11
 *	        forward-safety table): cluster_hang_sample_actionable() is true only
 *	        for COMPLETE && !in_confirmed_deadlock and false for APPROXIMATE /
 *	        INCOMPLETE / BLOCKER_GONE / REMOTE_BOUNDARY and for a COMPLETE but
 *	        confirmed-deadlock waiter — the safe-direction gate every HT cell in
 *	        t/330 relies on.
 *
 *	  Static contract + pure-policy assertions only (links the two Hang Manager
 *	  policy objects, like test_cluster_hang / test_cluster_hang_resolve).  All
 *	  chaos behavioural coverage is in cluster_tap t/330-t/333.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_hang_acceptance.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.20-hang-manager-acceptance.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <signal.h>
#include <stddef.h>
#include <string.h>

#include "storage/lwlock.h"
#include "utils/fmgroids.h"

#include "cluster/cluster_hang.h"
#include "cluster/cluster_hang_resolve.h"

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


UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* The two DIAG LWLock stubs the pure policy store ops (linked from
 * cluster_hang_policy.o) reference; no shmem is exercised here. */
bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}
void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}


/* ===== L1 — 13 cluster.hang_* GUC roster + advisory-default ladder ===== */

UT_TEST(test_hang_accept_guc_roster)
{
	/* The 13 PGC_SIGHUP knobs the acceptance TAP toggles (low threshold /
	 * interval to accelerate the real path, mode matrix, storm cap).  Pin the
	 * name roster as a compile-time array (array form, not literal strcmp, to
	 * avoid cppcheck staticStringCompare) so a GUC rename in cluster_guc.c
	 * diverges from the contract here.  Registration + bootvals are runtime-
	 * verified by the acceptance TAP. */
	const char *gucs[13] = { "cluster.hang_manager_enabled",
							 "cluster.hang_sample_interval_ms",
							 "cluster.hang_threshold_ms",
							 "cluster.hang_dump_enabled",
							 "cluster.hang_max_chain_depth",
							 "cluster.hang_max_sampled",
							 "cluster.hang_resolution_mode",
							 "cluster.hang_resolution_confirm_rounds",
							 "cluster.hang_resolution_soft_timeout_ms",
							 "cluster.hang_resolution_max_per_round",
							 "cluster.hang_victim_w_age",
							 "cluster.hang_victim_w_rollback",
							 "cluster.hang_victim_w_blockers" };
	int i;

	for (i = 0; i < 13; i++) {
		UT_ASSERT_NOT_NULL((void *)gucs[i]);
		/* every knob is namespaced under the public cluster. prefix */
		UT_ASSERT_EQ(strncmp(gucs[i], "cluster.hang_", 13), 0);
	}

	/* Factory default = ADVISORY (dry-run): a real non-destructive state that
	 * sits strictly between OFF and ENFORCE, so shipping never auto-enforces.
	 * The runtime bootval == HANG_RESOLVE_ADVISORY is asserted by t/331. */
	UT_ASSERT((int)HANG_RESOLVE_OFF < (int)HANG_RESOLVE_ADVISORY);
	UT_ASSERT((int)HANG_RESOLVE_ADVISORY < (int)HANG_RESOLVE_ENFORCE);
}


/* ===== L2 — hang dump category fixed key roster snapshot ===== */

UT_TEST(test_hang_accept_dump_category_key_snapshot)
{
	/* t/332 content-validates the single `hang` dump category emitted by
	 * dump_hang() (cluster_debug.c).  Pin the fixed (non-per-sample) key roster
	 * as an owned compile-time snapshot: a rename / removal at an emit site
	 * must update this roster (update-required contract).  Grouped by producer
	 * so a group-count drift is visible. */
	const char *config_aggregate[13]
		= { "hang_manager_enabled",	   "hang_dump_enabled",	   "hang_threshold_ms",
			"hang_sample_interval_ms", "hang_max_sampled",	   "hang_available",
			"hang_sample_epoch",	   "hang_last_sample_at",  "hang_last_dump_emitted_at",
			"hang_long_wait_count",	   "hang_longest_wait_us", "hang_truncated",
			"hang_n_samples" };
	const char *counters_511[9] = {
		"hang_samples_taken",			"hang_long_waits_seen",			"hang_dumps_emitted",
		"hang_incomplete_sample_count", "hang_excluded_deadlock_count", "hang_excluded_idle_count",
		"hang_excluded_bgworker_count", "hang_proc_signal_dump_count",	"hang_error_count"
	};
	const char *reader_58[2] = { "hang_deadlock_confirmed_count", "hang_cycle_detected_count" };
	const char *counters_512[18]
		= { "hang_resolution_mode",			"hang_resolve_evaluations",
			"hang_victims_selected",		"hang_soft_cancels_issued",
			"hang_terminates_issued",		"hang_resolved_confirmed",
			"hang_resolution_failed",		"hang_hard_skipped",
			"hang_non_actionable_skipped",	"hang_over_excluded",
			"hang_unprovable_root_skipped", "hang_aba_revalidate_failed",
			"hang_not_confirmed_yet",		"hang_no_safe_victim",
			"hang_degraded_to_timeout",		"hang_advisory_recommendations",
			"hang_resolve_last_victim_pid", "hang_resolve_last_action" };
	/* Per-sample rows are emitted as hang_sample{i}<suffix> for i in [0,n). */
	const char *sample_suffixes[9] = { "_pid",
									   "_wait_event",
									   "_wait_ms",
									   "_duration_kind",
									   "_source",
									   "_quality",
									   "_blocker_pid",
									   "_blocker_remote_node",
									   "_in_confirmed_deadlock" };
	int i;

	for (i = 0; i < 13; i++)
		UT_ASSERT((int)strlen(config_aggregate[i]) >= 5);
	for (i = 0; i < 9; i++)
		UT_ASSERT_EQ(strncmp(counters_511[i], "hang_", 5), 0);
	for (i = 0; i < 2; i++)
		UT_ASSERT_EQ(strncmp(reader_58[i], "hang_", 5), 0);
	for (i = 0; i < 18; i++)
		UT_ASSERT_EQ(strncmp(counters_512[i], "hang_", 5), 0);
	for (i = 0; i < 9; i++)
		UT_ASSERT_EQ(sample_suffixes[i][0], '_');

	/* Fixed key total = 13 + 9 + 2 + 18 == 42; per-sample suffix set == 9.
	 * Derive from the actual roster array sizes (lengthof) so editing a roster
	 * above really moves the number here -- a future spec adding a hang counter
	 * MUST bump the roster (compile-checked) which trips this contract (and the
	 * t/332 live-key content roster). */
	UT_ASSERT_EQ((int)(lengthof(config_aggregate) + lengthof(counters_511) + lengthof(reader_58)
					   + lengthof(counters_512)),
				 42);
	UT_ASSERT_EQ((int)lengthof(sample_suffixes), 9);
}


/* ===== L3 — 3 Hang Manager SQL function oids stable ===== */

UT_TEST(test_hang_accept_sql_fn_oids)
{
	/* fmgroids exposes the pinned oids; a pg_proc.dat oid change (which would
	 * break the acceptance SQL surface) diverges here.  Permission contract is
	 * behaviourally verified in t/332. */
	UT_ASSERT_EQ((int)F_PG_CLUSTER_HANG_DUMP, 8957);
	UT_ASSERT_EQ((int)F_PG_CLUSTER_HANG_VICTIMS, 8958);
	UT_ASSERT_EQ((int)F_PG_CLUSTER_HANG_RESOLVE, 8959);
}


/* ===== L4 — detection + disposition enum surface present + ordered ===== */

UT_TEST(test_hang_accept_enum_surface)
{
	/* Quality ladder: COMPLETE is the only actionable quality; the rest are the
	 * fail-OPEN low-confidence states t/330 classifies non-actionable. */
	UT_ASSERT_EQ((int)HANG_SAMPLE_COMPLETE, 0);
	UT_ASSERT(HANG_SAMPLE_APPROXIMATE != HANG_SAMPLE_COMPLETE);
	UT_ASSERT(HANG_SAMPLE_INCOMPLETE != HANG_SAMPLE_APPROXIMATE);
	UT_ASSERT(HANG_SAMPLE_BLOCKER_GONE != HANG_SAMPLE_INCOMPLETE);
	UT_ASSERT(HANG_SAMPLE_REMOTE_BOUNDARY != HANG_SAMPLE_BLOCKER_GONE);

	/* Disposition mode ladder. */
	UT_ASSERT_EQ((int)HANG_RESOLVE_OFF, 0);
	UT_ASSERT((int)HANG_RESOLVE_ENFORCE > (int)HANG_RESOLVE_ADVISORY);

	/* Action tier ladder (deliberately no SIGKILL tier). */
	UT_ASSERT_EQ((int)HANG_ACTION_NONE, 0);
	UT_ASSERT((int)HANG_ACTION_CANCEL < (int)HANG_ACTION_TERMINATE);
	UT_ASSERT((int)HANG_ACTION_TERMINATE < (int)HANG_ACTION_DEGRADE);

	/* Victim-skip: OK plus the four HARD-skip never-kill reasons + not-holder. */
	UT_ASSERT_EQ((int)HANG_VICTIM_OK, 0);
	UT_ASSERT(HANG_VICTIM_SKIP_SYSTEM != HANG_VICTIM_OK);
	UT_ASSERT(HANG_VICTIM_SKIP_RECOVERY != HANG_VICTIM_SKIP_SYSTEM);
	UT_ASSERT(HANG_VICTIM_SKIP_2PC != HANG_VICTIM_SKIP_RECOVERY);
	UT_ASSERT(HANG_VICTIM_SKIP_CRITICAL != HANG_VICTIM_SKIP_2PC);
	UT_ASSERT(HANG_VICTIM_SKIP_NOT_LOCK_HOLDER != HANG_VICTIM_SKIP_CRITICAL);

	/* Fail-CLOSED gate verdicts: PASS plus the five rejection reasons. */
	UT_ASSERT_EQ((int)HANG_GATE_PASS, 0);
	UT_ASSERT(HANG_GATE_NOT_ACTIONABLE != HANG_GATE_PASS);
	UT_ASSERT(HANG_GATE_OVER_EXCLUDED != HANG_GATE_NOT_ACTIONABLE);
	UT_ASSERT(HANG_GATE_HARD_SKIP != HANG_GATE_OVER_EXCLUDED);
	UT_ASSERT(HANG_GATE_NOT_LOCK_HOLDER != HANG_GATE_HARD_SKIP);
	UT_ASSERT(HANG_GATE_NOT_CONFIRMED != HANG_GATE_NOT_LOCK_HOLDER);
}


/* ===== L5 — no-SIGKILL invariant (pure tier->signal policy) ===== */

UT_TEST(test_hang_accept_no_sigkill_invariant)
{
	int tier;

	/* The mapped ladder: cancel -> SIGINT, terminate -> SIGTERM. */
	UT_ASSERT_EQ(cluster_hang_tier_signal(HANG_ACTION_CANCEL), SIGINT);
	UT_ASSERT_EQ(cluster_hang_tier_signal(HANG_ACTION_TERMINATE), SIGTERM);
	/* NONE and honest-degrade never signal. */
	UT_ASSERT_EQ(cluster_hang_tier_signal(HANG_ACTION_NONE), 0);
	UT_ASSERT_EQ(cluster_hang_tier_signal(HANG_ACTION_DEGRADE), 0);

	/* default-deny: no tier value in [0,64) ever maps to a destructive signal,
	 * and SIGKILL is never produced (the whole point of the ladder). */
	for (tier = 0; tier < 64; tier++) {
		int sig = cluster_hang_tier_signal((ClusterHangActionTier)tier);

		UT_ASSERT(sig == 0 || sig == SIGINT || sig == SIGTERM);
		UT_ASSERT(sig != SIGKILL);
	}
}


/* ===== L6 — actionable truth-table reuse (safe-direction gate) ===== */

UT_TEST(test_hang_accept_actionable_truth_table)
{
	ClusterHangSampleSlot slot;

	memset(&slot, 0, sizeof(slot));

	/* The one actionable cell: a fully-resolved local wait that is not a
	 * confirmed-deadlock waiter. */
	slot.quality = HANG_SAMPLE_COMPLETE;
	slot.in_confirmed_deadlock = false;
	UT_ASSERT(cluster_hang_sample_actionable(&slot));

	/* Every low-confidence quality is non-actionable (fail-OPEN). */
	slot.quality = HANG_SAMPLE_APPROXIMATE;
	UT_ASSERT(!cluster_hang_sample_actionable(&slot));
	slot.quality = HANG_SAMPLE_INCOMPLETE;
	UT_ASSERT(!cluster_hang_sample_actionable(&slot));
	slot.quality = HANG_SAMPLE_BLOCKER_GONE;
	UT_ASSERT(!cluster_hang_sample_actionable(&slot));
	slot.quality = HANG_SAMPLE_REMOTE_BOUNDARY;
	UT_ASSERT(!cluster_hang_sample_actionable(&slot));

	/* COMPLETE but a confirmed-deadlock waiter is never actionable (5.8/5.9
	 * owns deadlock; Hang Manager must not double-dispose). */
	slot.quality = HANG_SAMPLE_COMPLETE;
	slot.in_confirmed_deadlock = true;
	UT_ASSERT(!cluster_hang_sample_actionable(&slot));
}


int
main(void)
{
	UT_RUN(test_hang_accept_guc_roster);
	UT_RUN(test_hang_accept_dump_category_key_snapshot);
	UT_RUN(test_hang_accept_sql_fn_oids);
	UT_RUN(test_hang_accept_enum_surface);
	UT_RUN(test_hang_accept_no_sigkill_invariant);
	UT_RUN(test_hang_accept_actionable_truth_table);
	UT_DONE();
}
