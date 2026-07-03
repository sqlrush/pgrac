/*-------------------------------------------------------------------------
 *
 * test_cluster_terminal_authority.c
 *	  spec-6.2 Smart Fusion terminal-authority unit tests.
 *
 *	  Pins the pure fail-closed decision layer that later Smart Fusion data
 *	  paths must call before trusting cross-instance undo / TT terminal state.
 *	  Missing epoch, ownership, durable commit, or retention evidence must never
 *	  degrade to native visibility or UNKNOWN-visible.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_terminal_authority.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <string.h>

#include "cluster/cluster_terminal_authority.h"

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

static ClusterTerminalAuthorityEvidence
base_committed_evidence(void)
{
	ClusterTerminalAuthorityEvidence ev;

	memset(&ev, 0, sizeof(ev));
	ev.enabled = true;
	ev.origin_node = 1;
	ev.owner_node = 1;
	ev.owner_known = true;
	ev.xid = (TransactionId)100;
	ev.epoch_known = true;
	ev.observed_epoch = 7;
	ev.current_epoch = 7;
	ev.terminal_state = CLUSTER_TERMINAL_AUTH_COMMITTED;
	ev.terminal_scn = (SCN)42;
	ev.durable_commit_required = true;
	ev.durable_commit_resolved = true;
	ev.durable_commit_scn = (SCN)42;
	ev.retention_required = false;
	ev.retention_proven = false;
	return ev;
}

UT_TEST(test_committed_requires_matching_durable_scn)
{
	ClusterTerminalAuthorityEvidence ev = base_committed_evidence();

	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev), (int)CLUSTER_TERMINAL_AUTH_OK);

	ev.durable_commit_scn = (SCN)43;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev),
				 (int)CLUSTER_TERMINAL_AUTH_DURABLE_MISMATCH);

	ev.durable_commit_scn = (SCN)42;
	ev.durable_commit_resolved = false;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev),
				 (int)CLUSTER_TERMINAL_AUTH_DURABLE_MISSING);
}

UT_TEST(test_unknown_and_nonterminal_fail_closed)
{
	ClusterTerminalAuthorityEvidence ev = base_committed_evidence();

	ev.terminal_state = CLUSTER_TERMINAL_AUTH_UNKNOWN;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev),
				 (int)CLUSTER_TERMINAL_AUTH_TERMINAL_UNKNOWN);

	ev.terminal_state = CLUSTER_TERMINAL_AUTH_IN_PROGRESS;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev),
				 (int)CLUSTER_TERMINAL_AUTH_NONTERMINAL);
}

UT_TEST(test_epoch_and_ownership_are_mandatory)
{
	ClusterTerminalAuthorityEvidence ev = base_committed_evidence();

	ev.epoch_known = false;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev),
				 (int)CLUSTER_TERMINAL_AUTH_EPOCH_UNKNOWN);

	ev = base_committed_evidence();
	ev.current_epoch = 8;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev),
				 (int)CLUSTER_TERMINAL_AUTH_EPOCH_CHANGED);

	ev = base_committed_evidence();
	ev.owner_known = false;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev),
				 (int)CLUSTER_TERMINAL_AUTH_OWNER_UNKNOWN);

	ev = base_committed_evidence();
	ev.owner_node = 2;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev),
				 (int)CLUSTER_TERMINAL_AUTH_OWNER_MISMATCH);
}

UT_TEST(test_disabled_and_invalid_inputs_fail_closed)
{
	ClusterTerminalAuthorityEvidence ev = base_committed_evidence();

	ev.enabled = false;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev), (int)CLUSTER_TERMINAL_AUTH_DISABLED);

	ev = base_committed_evidence();
	ev.origin_node = -1;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev),
				 (int)CLUSTER_TERMINAL_AUTH_INVALID_ORIGIN);

	ev = base_committed_evidence();
	ev.xid = InvalidTransactionId;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev),
				 (int)CLUSTER_TERMINAL_AUTH_INVALID_XID);
}

UT_TEST(test_aborted_terminal_does_not_need_commit_scn)
{
	ClusterTerminalAuthorityEvidence ev = base_committed_evidence();

	ev.terminal_state = CLUSTER_TERMINAL_AUTH_ABORTED;
	ev.terminal_scn = InvalidScn;
	ev.durable_commit_required = false;
	ev.durable_commit_resolved = false;
	ev.durable_commit_scn = InvalidScn;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev), (int)CLUSTER_TERMINAL_AUTH_OK);
}

UT_TEST(test_retention_proof_is_explicit)
{
	ClusterTerminalAuthorityEvidence ev = base_committed_evidence();

	ev.retention_required = true;
	ev.retention_proven = false;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev),
				 (int)CLUSTER_TERMINAL_AUTH_RETENTION_UNPROVEN);

	ev.retention_proven = true;
	UT_ASSERT_EQ((int)cluster_terminal_authority_decide(&ev), (int)CLUSTER_TERMINAL_AUTH_OK);
}

UT_TEST(test_reason_names_are_stable)
{
	UT_ASSERT_STR_EQ(cluster_terminal_authority_reason_name(CLUSTER_TERMINAL_AUTH_OK), "ok");
	UT_ASSERT_STR_EQ(cluster_terminal_authority_reason_name(CLUSTER_TERMINAL_AUTH_EPOCH_CHANGED),
					 "epoch_changed");
	UT_ASSERT_STR_EQ(cluster_terminal_authority_reason_name(CLUSTER_TERMINAL_AUTH_DURABLE_MISMATCH),
					 "durable_mismatch");
}

int
main(void)
{
	UT_RUN(test_committed_requires_matching_durable_scn);
	UT_RUN(test_unknown_and_nonterminal_fail_closed);
	UT_RUN(test_epoch_and_ownership_are_mandatory);
	UT_RUN(test_disabled_and_invalid_inputs_fail_closed);
	UT_RUN(test_aborted_terminal_does_not_need_commit_scn);
	UT_RUN(test_retention_proof_is_explicit);
	UT_RUN(test_reason_names_are_stable);
	UT_DONE();
}
