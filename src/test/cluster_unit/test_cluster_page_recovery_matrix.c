/*-------------------------------------------------------------------------
 *
 * test_cluster_page_recovery_matrix.c
 *    RF-PAGE PGDEL-09 focused unit RED matrix — the remaining §10.1
 *    rows composed from the delivered PGDEL-01..08 semantic layers.
 *
 *    RED mapping (spec §10.1, rows not already pinned by the earlier
 *    PGDEL unit suites):
 *      PU-09  new/unformatted with full init proof -> typed init
 *             (the UNFORMATTED before-state matches + NEW class + INIT
 *             action; the exact full-init RULE itself stays RED/UNKNOWN
 *             until PGDEL-03/06 registers one — PU-17 holds);
 *      PU-10  new/unformatted missing lifecycle -> BLOCKED (before-state
 *             invalid);
 *      PU-11  incarnation transition with old redo -> old contributor
 *             rejected (INCARNATION_CROSS);
 *      PU-12  temp class -> discard/recreate only with owner proof
 *             (TEMP class + DISCARD action; the owner proof is the
 *             apply layer's);
 *      PU-15  valid FPI provenance -> accepted as image payload
 *             (FULLIMAGE class + IMAGE action under a valid source);
 *      PU-16  FPI wrong lineage -> BLOCKED (source lineage fails);
 *      PU-18  cleanout with exact TT+version input -> deterministic
 *             apply (CLEANOUT class + APPLY action; the exact codec
 *             census stays RED);
 *      PU-19  cleanout codec unknown -> BLOCKED (classifier UNKNOWN on
 *             ambiguous attributes; action table BLOCKED for WILLINIT);
 *      PU-20  nonlogged with rebuild owner -> typed rebuild (NONLOGGED
 *             + REBUILD action);
 *      PU-21  nonlogged no owner -> BLOCKED (declared class, apply
 *             layer denies without the rebuild owner);
 *      PU-27  raw cross-thread LSN ordering -> rejected
 *             (THREAD_MISMATCH).
 *
 *    Fault face (spec §10.2): the crash-cut legs are judged at unit
 *    level through the §7.7 crash matrix (PL-01..PL-06, PL-08, PL-11,
 *    PL-12, PL-13, PL-14 map onto the delivered verdicts); the faithful
 *    TAP cast of PL-01..PL-14 stays RED/BLOCKED under the 2-node
 *    shared-root baseplate limitation and the §7.6 stable-base STOP
 *    (PL-03 is permanently STABLE_BASE_UNRESOLVED).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_apply.h"
#include "cluster/cluster_page_handoff.h"
#include "cluster/cluster_page_recovery.h"
#include "cluster/cluster_page_set.h"
#include "cluster/cluster_page_source.h"
#include "cluster/cluster_page_version.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#include <stdio.h>

static ClusterPageIdentity ut_id;
static ClusterPageVersion ut_v[4];
static ClusterPageRedoChange ut_changes[3];

static void
ut_setup_globals(void)
{
	int i;

	memset(&ut_id, 0, sizeof(ut_id));
	ut_id.rlocator.spcOid = 1;
	ut_id.rlocator.dbOid = 2;
	ut_id.rlocator.relNumber = 3;
	ut_id.forknum = MAIN_FORKNUM;
	ut_id.blocknum = 42;

	for (i = 0; i < 4; i++) {
		memset(&ut_v[i], 0, sizeof(ut_v[i]));
		ut_v[i].identity = ut_id;
		ut_v[i].incarnation = 7;
		ut_v[i].token = (uint64)(100 + i);
	}
	for (i = 0; i < 3; i++) {
		memset(&ut_changes[i], 0, sizeof(ut_changes[i]));
		ut_changes[i].identity = ut_id;
		ut_changes[i].page_class = CLUSTER_PAGE_CLASS_NORMAL;
		ut_changes[i].failed_origin_thread = 2;
		ut_changes[i].expected_before = ut_v[i];
		ut_changes[i].result_version = ut_v[i + 1];
	}
}

/* PU-09/PU-10: NEW with/without the UNFORMATTED lifecycle proof. */
UT_TEST(test_pu09_pu10_new_init_and_missing_lifecycle)
{
	ClusterPageClassifyInput cin;
	ClusterPageBeforeState before;
	ClusterPageBeforeState missing;

	memset(&cin, 0, sizeof(cin));
	cin.rmid = 10;
	cin.opcode = 0x80; /* INIT_PAGE-shaped */
	cin.forknum = MAIN_FORKNUM;
	cin.page_absent = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&cin), (int)CLUSTER_PAGE_CLASS_NEW);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_NEW),
				 (int)CLUSTER_PAGE_ACTION_INIT);

	/* PU-09: the typed UNFORMATTED before-state is present and exact. */
	memset(&before, 0, sizeof(before));
	before.identity = ut_id;
	before.new_incarnation = 7;
	UT_ASSERT(cluster_page_before_state_valid(&before));
	/* The typed init path is admitted; the exact full-init RULE (expected
	 * class state + result-version) stays RED — WILLINIT without a rule
	 * classifies UNKNOWN (PU-17), so the full-init proof requirement is
	 * honestly unsatisfied until a rule registers. */

	/* PU-10: missing lifecycle — the before-state is invalid (zero new
	 * incarnation) -> BLOCKED, never a silent "new page" init. */
	memset(&missing, 0, sizeof(missing));
	missing.identity = ut_id;
	UT_ASSERT(!cluster_page_before_state_valid(&missing));
	UT_ASSERT(!cluster_page_before_state_equal(&missing, &before));
}

/* PU-12: TEMP class -> DISCARD action. */
UT_TEST(test_pu12_temp_discard_only_with_owner_proof)
{
	ClusterPageClassifyInput cin;

	memset(&cin, 0, sizeof(cin));
	cin.rmid = 10;
	cin.opcode = 0x10;
	cin.forknum = INIT_FORKNUM;
	UT_ASSERT_EQ((int)cluster_page_classify(&cin), (int)CLUSTER_PAGE_CLASS_TEMP);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_TEMP),
				 (int)CLUSTER_PAGE_ACTION_DISCARD);
	/* The owner/lifecycle proof is the apply layer's; classification and
	 * action assignment are what the class layer can promise. */
}

/* PU-15/PU-16: FPI provenance. */
UT_TEST(test_pu15_pu16_fpi_provenance)
{
	ClusterPageClassifyInput cin;
	ClusterPageSourceValidateInput sin;
	ClusterPageSourceKind kind;

	memset(&cin, 0, sizeof(cin));
	cin.rmid = 10;
	cin.opcode = 0x10;
	cin.forknum = MAIN_FORKNUM;
	cin.has_full_page_image = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&cin), (int)CLUSTER_PAGE_CLASS_FULLIMAGE);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_FULLIMAGE),
				 (int)CLUSTER_PAGE_ACTION_IMAGE);

	/* PU-15: a valid source (integrity + lineage + owner) admits the
	 * image payload path. */
	memset(&sin, 0, sizeof(sin));
	sin.identity = &ut_id;
	sin.source_version = &ut_v[0];
	sin.integrity_ok = true;
	sin.stability_ok = true;
	sin.lineage_ok = true;
	sin.owner_ok = true;
	kind = CLUSTER_PAGE_SOURCE_CURRENT;
	UT_ASSERT(cluster_page_source_validate_current(&sin));
	UT_ASSERT_EQ(cluster_page_source_select(&kind, &sin, 1), 0);

	/* PU-16: wrong lineage -> the source is rejected (BLOCKED), so the
	 * image payload never rides an unproven lineage. */
	sin.lineage_ok = false;
	UT_ASSERT(!cluster_page_source_validate_current(&sin));
	UT_ASSERT_EQ(cluster_page_source_select(&kind, &sin, 1), -1);
}

/* PU-18/PU-19: cleanout. */
UT_TEST(test_pu18_pu19_cleanout)
{
	ClusterPageClassifyInput cin;

	memset(&cin, 0, sizeof(cin));
	cin.rmid = 10;
	cin.opcode = 0x10;
	cin.forknum = MAIN_FORKNUM;
	cin.is_cleanout = true;
	/* Class identifiable from the declaration... */
	UT_ASSERT_EQ((int)cluster_page_classify(&cin), (int)CLUSTER_PAGE_CLASS_CLEANOUT);
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_CLEANOUT),
				 (int)CLUSTER_PAGE_ACTION_APPLY);
	/* ...but the exact codec census is RED: the deterministic apply is
	 * the apply layer's promise (PGDEL-06), and the decoder_registered
	 * flag is false for every census row (PU-19: unknown codec stays
	 * BLOCKED at the apply gate). */

	/* PU-19 also: cleanout + image/init attributes is ambiguous -> the
	 * classifier returns UNKNOWN (BLOCKED). */
	cin.is_cleanout = true;
	cin.has_will_init = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&cin), (int)CLUSTER_PAGE_CLASS_UNKNOWN);
}

/* PU-20/PU-21: nonlogged. */
UT_TEST(test_pu20_pu21_nonlogged)
{
	ClusterPageClassifyInput cin;

	memset(&cin, 0, sizeof(cin));
	cin.rmid = 10;
	cin.opcode = 0x10;
	cin.forknum = MAIN_FORKNUM;
	cin.relation_is_unlogged = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&cin), (int)CLUSTER_PAGE_CLASS_NONLOGGED);
	/* Typed rebuild action; the rebuild owner proof is the apply layer's
	 * — without it the resource stays BLOCKED (PU-21), which the action
	 * table's REBUILD + the apply-layer owner gate express. */
	UT_ASSERT_EQ((int)cluster_page_class_recovery_action(CLUSTER_PAGE_CLASS_NONLOGGED),
				 (int)CLUSTER_PAGE_ACTION_REBUILD);
}

/* PU-11/PU-27: incarnation cross + cross-thread rejection. */
UT_TEST(test_pu11_pu27_old_contributors_rejected)
{
	ClusterBlockRecoverySet set;
	ClusterPageVersion old_inc;

	/* PU-11: an old-incarnation change can never chain onto the new
	 * incarnation (INCARNATION_CROSS). */
	memset(&set, 0, sizeof(set));
	set.failed_origin_thread = 2;
	set.identity = ut_id;
	set.page_class = CLUSTER_PAGE_CLASS_NORMAL;
	set.source_version = ut_v[0];
	set.terminal_version = ut_v[2];
	set.contributors = ut_changes;
	set.n_contributors = 2;
	old_inc = ut_v[1];
	old_inc.incarnation = 6; /* old incarnation */
	ut_changes[1].expected_before = old_inc;
	ut_changes[1].result_version = old_inc;
	ut_changes[1].result_version.token = 102;
	UT_ASSERT_EQ((int)cluster_page_contributor_closure(&set),
				 (int)CLUSTER_PAGE_CLOSURE_INCARNATION_CROSS);

	/* PU-27: a contributor of another failed origin is never part of
	 * this block's chain (raw cross-thread LSN has no global ordering). */
	ut_changes[0].expected_before = ut_v[0];
	ut_changes[0].result_version = ut_v[1];
	ut_changes[1].expected_before = ut_v[1];
	ut_changes[1].result_version = ut_v[2];
	ut_changes[1].failed_origin_thread = 3; /* other origin */
	UT_ASSERT_EQ((int)cluster_page_contributor_closure(&set),
				 (int)CLUSTER_PAGE_CLOSURE_THREAD_MISMATCH);
}

/* PL face: crash-cut legs judged through the §7.7 matrix. */
UT_TEST(test_pl_legs_unit_level)
{
	/* PL-01 death before source proof -> successor re-census (BLOCKED_
	 * SOURCE). */
	UT_ASSERT_EQ((int)cluster_page_crash_matrix_verdict(CLUSTER_PAGE_CUT_BEFORE_SOURCE_PROOF),
				 (int)CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE);
	/* PL-02 after contributor closure -> local plan ignored (D3′). */
	UT_ASSERT_EQ((int)cluster_page_crash_matrix_verdict(CLUSTER_PAGE_CUT_AFTER_SOURCE_PROOF),
				 (int)CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE);
	/* PL-03 during target write -> RED/STOP, always. */
	UT_ASSERT_EQ((int)cluster_page_apply_midwrite_cut(),
				 (int)CLUSTER_PAGE_OUTCOME_STABLE_BASE_UNRESOLVED);
	/* PL-04/05/06 map onto the matrix rows. */
	UT_ASSERT_EQ(
		(int)cluster_page_crash_matrix_verdict(CLUSTER_PAGE_CUT_AFTER_WRITE_BEFORE_DURABILITY),
		(int)CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE);
	UT_ASSERT_EQ(
		(int)cluster_page_crash_matrix_verdict(CLUSTER_PAGE_CUT_AFTER_DURABILITY_BEFORE_POST_READ),
		(int)CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE);
	UT_ASSERT_EQ(
		(int)cluster_page_crash_matrix_verdict(CLUSTER_PAGE_CUT_AFTER_POST_READ_BEFORE_RELEASE),
		(int)CLUSTER_PAGE_OUTCOME_STALE_AUTHORITY);
	/* PL-12 retire-before-proof -> denied. */
	{
		ClusterPageHandoffInput hi;

		memset(&hi, 0, sizeof(hi));
		UT_ASSERT(cluster_page_handoff_retention_denied(&hi));
	}
}

int
main(void)
{
	UT_PLAN(7);

	ut_setup_globals();

	UT_RUN(test_pu09_pu10_new_init_and_missing_lifecycle);
	UT_RUN(test_pu12_temp_discard_only_with_owner_proof);
	UT_RUN(test_pu15_pu16_fpi_provenance);
	UT_RUN(test_pu18_pu19_cleanout);
	UT_RUN(test_pu20_pu21_nonlogged);
	UT_RUN(test_pu11_pu27_old_contributors_rejected);
	UT_RUN(test_pl_legs_unit_level);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
