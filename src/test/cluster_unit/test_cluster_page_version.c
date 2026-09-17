/*-------------------------------------------------------------------------
 *
 * test_cluster_page_version.c
 *    RF-PAGE PGDEL-01 focused unit tests: the PageVersion semantic type
 *    (valid/exact-equality), the §3.2 expected-before -> result-version
 *    admission decision with the §3.3 shape-1 trusted skip, and the §4.1
 *    closed page/record classifier (unknown/ambiguous default BLOCKED).
 *
 *    RED mapping (spec §10.1, PGDEL-01 scope):
 *      PU-01  InvalidScn normal page -> BLOCKED (containment, never a
 *             version success)
 *      PU-02  expected-before exact match -> one APPLY; result exact
 *      PU-03  expected-before mismatch -> BLOCKED; zero target mutation
 *      PU-04  trusted exact result -> SKIP; zero apply
 *      PU-05  numeric-higher but chain gap -> no skip; BLOCKED
 *      PU-06  same SCN/version different incarnation -> mismatch
 *      PU-07  unknown class/rmid/opcode -> default BLOCKED (UNKNOWN)
 *      PU-08  normal persistent page -> PageVersion chain required
 *      PU-13  rebuildable/FSM -> invalidate+rebuild class (no generic
 *             redo apply)
 *      PU-14  header class -> route typed owner
 *      PU-17  WILL_INIT without rmgr full-init rule -> BLOCKED
 *
 *    Deliberate boundary (spec §2.2/G1/G3): this suite pins the semantic
 *    layer only.  It does NOT claim the existing thread-recovery replay
 *    path is PageVersion-compliant — that path is untouched and stays RED
 *    until the later PGDEL items wire the producer/apply/durability/
 *    post-read chain.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

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

/* ---- identity/version builders -------------------------------------- */

static RelFileLocator
ut_locator(void)
{
	RelFileLocator rl;

	memset(&rl, 0, sizeof(rl));
	rl.spcOid = 1;
	rl.dbOid = 2;
	rl.relNumber = 3;
	return rl;
}

static void
ut_fill_identity(ClusterPageIdentity *id)
{
	memset(id, 0, sizeof(*id));
	id->rlocator = ut_locator();
	id->forknum = MAIN_FORKNUM;
	id->blocknum = 42;
}

static ClusterPageVersion
ut_version(ClusterPageIncarnation inc, ClusterVersionToken token)
{
	ClusterPageVersion v;

	memset(&v, 0, sizeof(v));
	ut_fill_identity(&v.identity);
	v.incarnation = inc;
	v.token = token;
	return v;
}

/* ---- PU-01/02/03/04/05/06: version validity + §3.2 decision ---------- */

UT_TEST(test_pu01_invalid_scn_normal_page_blocked)
{
	ClusterPageVersion current;
	ClusterPageVersion expected;
	ClusterPageVersion result_invalid; /* InvalidScn / zero token */
	ClusterPageVersion trusted;

	current = ut_version(7, 100);
	expected = ut_version(7, 100);
	memset(&result_invalid, 0, sizeof(result_invalid)); /* invalid result */
	ut_fill_identity(&result_invalid.identity);
	result_invalid.incarnation = 7; /* token stays 0: invalid */
	trusted = ut_version(7, 100);

	/* InvalidScn containment: BLOCKED, never a version success. */
	UT_ASSERT(!cluster_page_version_valid(&result_invalid));
	UT_ASSERT_EQ((int)cluster_page_version_decide(&current, &expected, &result_invalid, &trusted),
				 (int)CLUSTER_PAGE_APPLY_BLOCKED);
	/* An invalid expected-before or current is equally BLOCKED. */
	UT_ASSERT_EQ((int)cluster_page_version_decide(NULL, &expected, &current, NULL),
				 (int)CLUSTER_PAGE_APPLY_BLOCKED);
	UT_ASSERT_EQ((int)cluster_page_version_decide(&current, NULL, &current, NULL),
				 (int)CLUSTER_PAGE_APPLY_BLOCKED);
}

UT_TEST(test_pu02_expected_before_exact_match_applies)
{
	ClusterPageVersion current;
	ClusterPageVersion expected;
	ClusterPageVersion result;
	ClusterPageVersion trusted;

	current = ut_version(7, 100);
	expected = ut_version(7, 100);
	result = ut_version(7, 101);
	trusted = ut_version(7, 50);

	UT_ASSERT_EQ((int)cluster_page_version_decide(&current, &expected, &result, &trusted),
				 (int)CLUSTER_PAGE_APPLY_APPLY);
	/* The trusted source is irrelevant when it does not equal the result:
	 * exact expected-before match still admits the apply. */
	UT_ASSERT_EQ((int)cluster_page_version_decide(&current, &expected, &result, NULL),
				 (int)CLUSTER_PAGE_APPLY_APPLY);
}

UT_TEST(test_pu03_expected_before_mismatch_blocked)
{
	ClusterPageVersion current;
	ClusterPageVersion expected;
	ClusterPageVersion result;

	current = ut_version(7, 100);
	expected = ut_version(7, 99); /* mismatch on the token */
	result = ut_version(7, 101);

	UT_ASSERT_EQ((int)cluster_page_version_decide(&current, &expected, &result, NULL),
				 (int)CLUSTER_PAGE_APPLY_BLOCKED);
	/* Different incarnation with the "same" SCN-shaped token is a mismatch
	 * too (PU-06): incarnation participates in exact equality. */
	current = ut_version(8, 100);
	expected = ut_version(7, 100);
	UT_ASSERT_EQ((int)cluster_page_version_decide(&current, &expected, &result, NULL),
				 (int)CLUSTER_PAGE_APPLY_BLOCKED);
}

UT_TEST(test_pu04_trusted_exact_result_skips)
{
	ClusterPageVersion current;
	ClusterPageVersion expected;
	ClusterPageVersion result;
	ClusterPageVersion trusted;

	current = ut_version(7, 100);
	expected = ut_version(7, 99);
	result = ut_version(7, 101);
	trusted = ut_version(7, 101); /* source exactly equals the result */

	/* Trusted result-version skip (shape 1): zero apply, even though the
	 * working version does not match expected_before. */
	UT_ASSERT_EQ((int)cluster_page_version_decide(&current, &expected, &result, &trusted),
				 (int)CLUSTER_PAGE_APPLY_SKIP);
}

UT_TEST(test_pu05_numeric_higher_with_gap_no_skip)
{
	ClusterPageVersion current;
	ClusterPageVersion expected;
	ClusterPageVersion result;
	ClusterPageVersion trusted;

	current = ut_version(7, 5);
	expected = ut_version(7, 6); /* no exact match: only SKIP could pass */
	/* "Numeric-higher" result token is NOT coverage: the trusted source
	 * token 99 != result token 101, so shape 1 fails, and the working
	 * version does not equal expected_before either -> BLOCKED.  There is
	 * no numeric ordering anywhere in this layer (spec §3.1/§3.3). */
	result = ut_version(7, 101);
	trusted = ut_version(7, 99);

	UT_ASSERT_EQ((int)cluster_page_version_decide(&current, &expected, &result, &trusted),
				 (int)CLUSTER_PAGE_APPLY_BLOCKED);

	/* A numerically-higher CURRENT also proves nothing: exact equality is
	 * the only predicate. */
	current = ut_version(7, 200);
	expected = ut_version(7, 5);
	UT_ASSERT_EQ((int)cluster_page_version_decide(&current, &expected, &result, NULL),
				 (int)CLUSTER_PAGE_APPLY_BLOCKED);
}

UT_TEST(test_pu06_same_scn_different_incarnation_mismatch)
{
	ClusterPageVersion current;
	ClusterPageVersion expected;
	ClusterPageVersion result;

	/* Identical identity + token ("SCN"), different incarnation: exact
	 * equality must fail (spec §4.3: old-incarnation redo never applies to
	 * a new-incarnation page, even when SCN/checksum look right). */
	current = ut_version(11, 555);
	expected = ut_version(10, 555);
	result = ut_version(11, 556);

	UT_ASSERT(!cluster_page_version_equal(&current, &expected));
	UT_ASSERT_EQ((int)cluster_page_version_decide(&current, &expected, &result, NULL),
				 (int)CLUSTER_PAGE_APPLY_BLOCKED);

	/* A different physical identity is likewise never equal. */
	{
		ClusterPageVersion other;

		other = ut_version(11, 555);
		other.identity.blocknum = 43;
		UT_ASSERT(!cluster_page_version_equal(&current, &other));
	}
}

UT_TEST(test_version_identity_helpers)
{
	ClusterPageIdentity id;
	ClusterPageIdentity other;
	ClusterPageBeforeState st;
	ClusterPageBeforeState st2;
	ClusterPageVersion v;

	memset(&id, 0, sizeof(id));
	UT_ASSERT(!cluster_page_identity_valid(&id)); /* zeroed: invalid */
	ut_fill_identity(&id);
	UT_ASSERT(cluster_page_identity_valid(&id));
	other = id;
	UT_ASSERT(cluster_page_identity_equal(&id, &other));
	other.blocknum = 43;
	UT_ASSERT(!cluster_page_identity_equal(&id, &other));

	v = ut_version(7, 100);
	UT_ASSERT(cluster_page_version_valid(&v));
	UT_ASSERT(cluster_page_version_equal(&v, &v));
	UT_ASSERT(!cluster_page_version_valid(&(ClusterPageVersion){ 0 }));
	{
		ClusterPageVersion inv = v;

		inv.token = 0;
		UT_ASSERT(!cluster_page_version_valid(&inv));
	}

	/* UNFORMATTED before-state: typed, not a numeric sentinel. */
	memset(&st, 0, sizeof(st));
	UT_ASSERT(!cluster_page_before_state_valid(&st));
	ut_fill_identity(&st.identity);
	st.new_incarnation = 9;
	UT_ASSERT(cluster_page_before_state_valid(&st));
	st2 = st;
	UT_ASSERT(cluster_page_before_state_equal(&st, &st2));
	st2.new_incarnation = 10;
	UT_ASSERT(!cluster_page_before_state_equal(&st, &st2));
}

/* ---- PU-07/08/13/14/17 + closed classifier --------------------------- */

static ClusterPageClassifyInput
ut_classify_input(void)
{
	ClusterPageClassifyInput in;

	memset(&in, 0, sizeof(in));
	in.rmid = 250; /* deliberately unregistered by default */
	in.opcode = 0x10;
	in.forknum = MAIN_FORKNUM;
	in.header_owner = CLUSTER_PAGE_HEADER_OWNER_NONE;
	return in;
}

UT_TEST(test_pu07_unknown_rmid_opcode_default_blocked)
{
	ClusterPageClassifyInput in = ut_classify_input();

	/* Unknown rmid/opcode on a main-fork persistent page: UNKNOWN -> the
	 * caller must fail closed (mutation=0). */
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_UNKNOWN);
	/* NULL input is UNKNOWN too. */
	UT_ASSERT_EQ((int)cluster_page_classify(NULL), (int)CLUSTER_PAGE_CLASS_UNKNOWN);
}

UT_TEST(test_pu08_normal_persistent_page_chain_required)
{
	ClusterPageClassifyInput in = ut_classify_input();
	ClusterPageVersion current;
	ClusterPageVersion expected;
	ClusterPageVersion result;

	/* Until the (rmid, opcode) pair is registered (PGDEL-02 owns the
	 * census), the same record is UNKNOWN.  Registration flips it to
	 * NORMAL — the only general delta-replay class. */
	UT_ASSERT(!cluster_page_class_is_known_opcode(9, 0x20));
	UT_ASSERT(cluster_page_class_register_known_opcode(9, 0x20));
	UT_ASSERT(cluster_page_class_is_known_opcode(9, 0x20));
	in.rmid = 9;
	in.opcode = 0x20;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_NORMAL);

	/* NORMAL requires the full PageVersion before/result chain: with any
	 * invalid member the admission decision is BLOCKED (no chain -> no
	 * apply/skip). */
	current = ut_version(7, 100);
	expected = ut_version(7, 100);
	result = ut_version(7, 101);
	UT_ASSERT_EQ((int)cluster_page_version_decide(&current, &expected, &result, NULL),
				 (int)CLUSTER_PAGE_APPLY_APPLY);
	/* Missing result (chain not produced) -> BLOCKED. */
	UT_ASSERT_EQ((int)cluster_page_version_decide(&current, &expected, NULL, NULL),
				 (int)CLUSTER_PAGE_APPLY_BLOCKED);
}

UT_TEST(test_pu13_fsm_rebuildable_class)
{
	ClusterPageClassifyInput in = ut_classify_input();

	in.forknum = FSM_FORKNUM;
	/* FSM is the approved deviation: rebuildable hint cache, never generic
	 * redo apply — classified without any rmgr registration. */
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_REBUILDABLE);
	/* FSM + image/init/cleanout attributes are ambiguous -> UNKNOWN. */
	in.has_full_page_image = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_UNKNOWN);
	in.has_full_page_image = false;
	in.has_will_init = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_UNKNOWN);
}

UT_TEST(test_pu14_header_routes_to_typed_owner)
{
	ClusterPageClassifyInput in = ut_classify_input();

	in.header_owner = CLUSTER_PAGE_HEADER_OWNER_ROOT;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_HEADER);
	in.header_owner = CLUSTER_PAGE_HEADER_OWNER_SIDE;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_HEADER);
	/* A header declaration combined with temp/FSM/new/image is ambiguous
	 * -> UNKNOWN (multi-match). */
	in.header_owner = CLUSTER_PAGE_HEADER_OWNER_PG_CORE;
	in.forknum = INIT_FORKNUM;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_UNKNOWN);
}

UT_TEST(test_pu17_will_init_without_rule_blocked)
{
	ClusterPageClassifyInput in = ut_classify_input();

	/* WILL_INIT alone never authorizes initialization; no exact rmgr
	 * full-init rule is registered, so the class is UNKNOWN (BLOCKED). */
	in.has_will_init = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_UNKNOWN);
	/* FPI + WILL_INIT is a multi-match -> UNKNOWN as well. */
	in.has_full_page_image = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_UNKNOWN);
}

UT_TEST(test_classifier_remaining_rows_closed)
{
	ClusterPageClassifyInput in = ut_classify_input();

	/* PC-TEMP: temp fork or temp-scoped relation. */
	in.forknum = INIT_FORKNUM;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_TEMP);
	in.forknum = MAIN_FORKNUM;
	in.relation_is_temp = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_TEMP);
	in.relation_is_temp = false;

	/* PC-FULLIMAGE: FPI is an image payload row (lineage admission is the
	 * apply layer's). */
	in.has_full_page_image = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_FULLIMAGE);
	in.has_full_page_image = false;

	/* PC-NEW: absent or uninitialized page. */
	in.page_absent = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_NEW);
	in.page_absent = false;
	in.page_is_new = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_NEW);
	in.page_is_new = false;

	/* PC-CLEANOUT / PC-NONLOGGED: declared classes (recovery action stays
	 * BLOCKED until producer + exact codec census land, PGDEL-02/06). */
	in.is_cleanout = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_CLEANOUT);
	in.is_cleanout = false;
	in.relation_is_unlogged = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_NONLOGGED);
	in.relation_is_unlogged = false;

	/* Cleanout/nonlogged combined with image/init is ambiguous. */
	in.is_cleanout = true;
	in.has_will_init = true;
	UT_ASSERT_EQ((int)cluster_page_classify(&in), (int)CLUSTER_PAGE_CLASS_UNKNOWN);
}

UT_TEST(test_known_opcode_registry_fail_closed)
{
	uint8 rmid = 200;
	int i;

	/* Fixed capacity: registration is refused once the table is full
	 * (fail-closed) — an overflow must never silently admit.  Run LAST:
	 * it fills the registry.  (One earlier test registered (9, 0x20), so
	 * 63 more entries fill the 64-slot table.) */
	for (i = 0; i < 63; i++)
		UT_ASSERT(cluster_page_class_register_known_opcode(rmid, (uint16)(0x100 + i)));
	UT_ASSERT(!cluster_page_class_register_known_opcode(rmid, 0x7fff));
	/* Idempotent re-registration still succeeds. */
	UT_ASSERT(cluster_page_class_register_known_opcode(rmid, 0x100));
}

int
main(void)
{
	UT_PLAN(14);

	UT_RUN(test_pu01_invalid_scn_normal_page_blocked);
	UT_RUN(test_pu02_expected_before_exact_match_applies);
	UT_RUN(test_pu03_expected_before_mismatch_blocked);
	UT_RUN(test_pu04_trusted_exact_result_skips);
	UT_RUN(test_pu05_numeric_higher_with_gap_no_skip);
	UT_RUN(test_pu06_same_scn_different_incarnation_mismatch);
	UT_RUN(test_version_identity_helpers);
	UT_RUN(test_pu07_unknown_rmid_opcode_default_blocked);
	UT_RUN(test_pu08_normal_persistent_page_chain_required);
	UT_RUN(test_pu13_fsm_rebuildable_class);
	UT_RUN(test_pu14_header_routes_to_typed_owner);
	UT_RUN(test_pu17_will_init_without_rule_blocked);
	UT_RUN(test_classifier_remaining_rows_closed);
	UT_RUN(test_known_opcode_registry_fail_closed);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
