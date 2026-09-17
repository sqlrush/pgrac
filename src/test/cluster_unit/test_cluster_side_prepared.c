/*-------------------------------------------------------------------------
 *
 * test_cluster_side_prepared.c
 *    RF-SIDE D-SIDE-03 focused unit tests: the PREPARED/in-doubt binding
 *    and the RECO-style resolution judgements.
 *
 *    RED mapping (spec §5.1 U-SIDE-06/07 + §2.3 + §4 + §5.2 L6):
 *      - IN_DOUBT requires prepare redo + database-scoped durable
 *        pending + matching TT/undo + exact GID/identity — each fact
 *        one-at-a-time missing -> BLOCKED (never a guessed terminal);
 *      - a pending entry that is only origin-local/cache (the caller
 *        declares pending_durable_ok=false) can never carry weight;
 *      - resolution: terminal + exact pending match + side completion
 *        (COMMIT: TT; ROLLBACK: verified undo) all required; premature
 *        abort and terminal-before-prepare are BLOCKED; locks/resources
 *        release only after matching resolution is durable verified
 *        (L6: COMMIT/ROLLBACK PREPARED terminal/undo/cleanup cuts —
 *        each cut keeps resolution BLOCKED, RECO-style resolution is a
 *        pure idempotent judgement);
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_side_prepared.h"

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

static ClusterSidePreparedInput
ut_full_prepare(void)
{
	ClusterSidePreparedInput in;

	memset(&in, 0, sizeof(in));
	in.prepare_redo_ok = true;
	in.pending_durable_ok = true;
	in.tt_undo_match = true;
	in.gid_identity_match = true;
	return in;
}

UT_TEST(test_in_doubt_conjunction)
{
	ClusterSidePreparedInput in = ut_full_prepare();

	UT_ASSERT_EQ((int)cluster_side_prepared_verdict(&in), (int)CLUSTER_SIDE_PREPARED_IN_DOUBT);

	/* U-SIDE-06/07: each fact one-at-a-time missing -> BLOCKED. */
	in.prepare_redo_ok = false;
	UT_ASSERT_EQ((int)cluster_side_prepared_verdict(&in), (int)CLUSTER_SIDE_PREPARED_BLOCKED);
	in = ut_full_prepare();
	in.pending_durable_ok = false; /* origin-local/cache only: no weight */
	UT_ASSERT_EQ((int)cluster_side_prepared_verdict(&in), (int)CLUSTER_SIDE_PREPARED_BLOCKED);
	in = ut_full_prepare();
	in.tt_undo_match = false;
	UT_ASSERT_EQ((int)cluster_side_prepared_verdict(&in), (int)CLUSTER_SIDE_PREPARED_BLOCKED);
	in = ut_full_prepare();
	in.gid_identity_match = false; /* same tx, different GID: conflict */
	UT_ASSERT_EQ((int)cluster_side_prepared_verdict(&in), (int)CLUSTER_SIDE_PREPARED_BLOCKED);
	UT_ASSERT_EQ((int)cluster_side_prepared_verdict(NULL), (int)CLUSTER_SIDE_PREPARED_BLOCKED);
}

UT_TEST(test_resolution_ready_conjunction)
{
	ClusterSidePreparedResolveInput in;

	memset(&in, 0, sizeof(in));
	in.terminal_redo_ok = true;
	in.pending_match = true;
	in.tt_undo_complete = true;
	UT_ASSERT(cluster_side_prepared_resolve_ready(&in));

	/* Terminal without the matching pending (terminal-before-prepare):
	 * BLOCKED (§4). */
	in.pending_match = false;
	UT_ASSERT(!cluster_side_prepared_resolve_ready(&in));
	in.pending_match = true;

	/* Premature abort: undo not complete -> BLOCKED (§2.3 last bullet). */
	in.tt_undo_complete = false;
	UT_ASSERT(!cluster_side_prepared_resolve_ready(&in));
	in.tt_undo_complete = true;

	/* Missing terminal redo -> BLOCKED. */
	in.terminal_redo_ok = false;
	UT_ASSERT(!cluster_side_prepared_resolve_ready(&in));
	UT_ASSERT(!cluster_side_prepared_resolve_ready(NULL));

	/* L6: RECO-style resolution is idempotent — the judgement is a pure
	 * function of its facts, so repeated resolution (restart / retry)
	 * with the same durable facts yields the same verdict, never a
	 * flip or a partial release. */
	in.terminal_redo_ok = true;
	UT_ASSERT(cluster_side_prepared_resolve_ready(&in));
	UT_ASSERT(cluster_side_prepared_resolve_ready(&in));
	in.pending_match = false;
	UT_ASSERT(!cluster_side_prepared_resolve_ready(&in));
	UT_ASSERT(!cluster_side_prepared_resolve_ready(&in));
}

/* implementation: the GID identity leg is extracted for direct
 * coverage — non-empty, bounded, fully inside the available payload. */
UT_TEST(test_gid_identity_leg_exact)
{
	const char gid_ok[] = "pgrac-2pc-42";

	UT_ASSERT(cluster_side_prepared_gid_identity_ok(gid_ok, (uint16)strlen(gid_ok),
													(uint32)strlen(gid_ok)));
	/* extra available bytes are fine (GID need not end the payload) */
	UT_ASSERT(cluster_side_prepared_gid_identity_ok(gid_ok, (uint16)strlen(gid_ok),
													(uint32)strlen(gid_ok) + 64));
	/* empty GID */
	UT_ASSERT(!cluster_side_prepared_gid_identity_ok(gid_ok, 0, 128));
	/* NULL GID */
	UT_ASSERT(!cluster_side_prepared_gid_identity_ok(NULL, 8, 128));
	/* truncated payload: GID does not fit */
	UT_ASSERT(!cluster_side_prepared_gid_identity_ok(gid_ok, (uint16)strlen(gid_ok),
													 (uint32)strlen(gid_ok) - 1));
	/* oversize GID (the 2PC path caps at MAXPGPATH) */
	UT_ASSERT(!cluster_side_prepared_gid_identity_ok(gid_ok, (uint16)(MAXPGPATH + 1),
													 (uint32)(MAXPGPATH + 2)));
	/* NUL-first payload: no usable identity */
	{
		const char empty[] = "";

		UT_ASSERT(!cluster_side_prepared_gid_identity_ok(empty, 1, 1));
	}
}

int
main(void)
{
	UT_PLAN(3);

	UT_RUN(test_in_doubt_conjunction);
	UT_RUN(test_resolution_ready_conjunction);
	UT_RUN(test_gid_identity_leg_exact);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
