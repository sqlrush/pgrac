/*-------------------------------------------------------------------------
 *
 * test_cluster_side_recovery.c
 *    RF-SIDE D-SIDE-09 focused unit corpus (part 1): the D-SIDE-06
 *    RF-PAGE integration verdict, the D-SIDE-07 per-resource readiness
 *    gate and the D-SIDE-08 retention proof exporter.
 *
 *    RED mapping (spec §4 crash-matrix rows + §2.2 + §5 + §5.1 + §5.2):
 *      - a canonical TT/undo/space page consumes the RF-PAGE proof only
 *        when class + expected-before + the complete proof hold;
 *      - readiness is strictly per-resource: resource A verified opens
 *        while resource B stays fenced (no side-wide barrier)
 *        (U-SIDE-13: each leg one-at-a-time false; U-SIDE-14: a stale
 *        fresh-authority proof permits neither mutation nor release;
 *        L15: while a pending 2PC census is incomplete the prepared
 *        resources stay fenced but unrelated healthy resources keep
 *        serving — the per-resource gate is the isolation);
 *      - retirement is denied precisely: not-durable, missing post-read
 *        and remaining-consumer each have their own denial reason; a
 *        logical DONE never substitutes for post-read (U-SIDE-15:
 *        FND-10 side proof — affected-set, durability, post-read and
 *        consumer each missing deny retire).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_side_recovery.h"

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
static ClusterPageVersion ut_expected;

static void
ut_setup_globals(void)
{
	memset(&ut_id, 0, sizeof(ut_id));
	ut_id.rlocator.spcOid = 1;
	ut_id.rlocator.dbOid = 2;
	ut_id.rlocator.relNumber = 3;
	ut_id.forknum = MAIN_FORKNUM;
	ut_id.blocknum = 7; /* a TT/undo/space canonical page */

	memset(&ut_expected, 0, sizeof(ut_expected));
	ut_expected.identity = ut_id;
	ut_expected.incarnation = 7;
	ut_expected.token = 100;
}

static ClusterSidePageConsumeInput
ut_full_consume(void)
{
	ClusterSidePageConsumeInput in;

	memset(&in, 0, sizeof(in));
	in.identity = &ut_id;
	in.page_class = CLUSTER_PAGE_CLASS_NORMAL;
	in.expected_before = &ut_expected;
	in.contributor_coverage = true;
	in.durability_barrier_ok = true;
	in.post_read_ok = true;
	in.authority_revalidated = true;
	return in;
}

UT_TEST(test_side_page_consumer_verdict)
{
	ClusterSidePageConsumeInput in = ut_full_consume();

	UT_ASSERT(cluster_side_page_consumer_ready(&in));

	/* Every proof element is required (spec §2.2: 未成立 -> BLOCKED). */
	in.contributor_coverage = false;
	UT_ASSERT(!cluster_side_page_consumer_ready(&in));
	in = ut_full_consume();
	in.durability_barrier_ok = false;
	UT_ASSERT(!cluster_side_page_consumer_ready(&in));
	in = ut_full_consume();
	in.post_read_ok = false;
	UT_ASSERT(!cluster_side_page_consumer_ready(&in));
	in = ut_full_consume();
	in.authority_revalidated = false;
	UT_ASSERT(!cluster_side_page_consumer_ready(&in));

	/* Unknown class or invalid expected-before fails closed. */
	in = ut_full_consume();
	in.page_class = CLUSTER_PAGE_CLASS_UNKNOWN;
	UT_ASSERT(!cluster_side_page_consumer_ready(&in));
	in = ut_full_consume();
	{
		ClusterPageVersion inv;

		memset(&inv, 0, sizeof(inv));
		in.expected_before = &inv;
		UT_ASSERT(!cluster_side_page_consumer_ready(&in));
	}
	UT_ASSERT(!cluster_side_page_consumer_ready(NULL));
}

/* U-SIDE-13: per-resource readiness — each leg one-at-a-time false;
 * resource A ready stays ready while resource B is blocked (no
 * side-wide barrier).  Also carries the U-SIDE-14 authority leg:
 * authority_fresh false -> no open (mutation/release stay zero). */
UT_TEST(test_side_readiness_per_resource)
{
	ClusterSideReadinessInput a;
	ClusterSideReadinessInput b;

	memset(&a, 0, sizeof(a));
	a.resource_id = 1;
	a.page_proof_ok = true;
	a.side_proof_ok = true;
	a.authority_fresh = true;
	UT_ASSERT(cluster_side_resource_readiness(&a));

	/* §4: resource A verified opens while resource B stays fenced — the
	 * readiness judgement has no side-wide state. */
	memset(&b, 0, sizeof(b));
	b.resource_id = 2;
	b.page_proof_ok = false; /* B blocked */
	UT_ASSERT(!cluster_side_resource_readiness(&b));
	UT_ASSERT(cluster_side_resource_readiness(&a)); /* A unaffected */

	/* Every fact is required for the open resource. */
	a.page_proof_ok = false;
	UT_ASSERT(!cluster_side_resource_readiness(&a));
	a.page_proof_ok = true;
	a.side_proof_ok = false;
	UT_ASSERT(!cluster_side_resource_readiness(&a));
	a.side_proof_ok = true;
	a.authority_fresh = false;
	UT_ASSERT(!cluster_side_resource_readiness(&a));
	UT_ASSERT(!cluster_side_resource_readiness(NULL));
}

/* U-SIDE-15: FND-10 side proof — affected-set (affected_count),
 * durability, post-read and remaining-consumer each missing deny
 * retire with their precise reason; a logical DONE never substitutes
 * for post-read. */
UT_TEST(test_side_retention_proof_verdicts)
{
	ClusterSideRetentionProof p;

	memset(&p, 0, sizeof(p));
	p.failed_origin_thread = 2;
	p.affected_count = 3;
	p.all_bytes_durable = true;
	p.all_post_read_ok = true;
	p.consumers_zero = true;
	UT_ASSERT_EQ((int)cluster_side_retention_proof_ready(&p), (int)CLUSTER_SIDE_RETENTION_READY);

	/* Not durable -> precise denial. */
	p.all_bytes_durable = false;
	UT_ASSERT_EQ((int)cluster_side_retention_proof_ready(&p),
				 (int)CLUSTER_SIDE_RETENTION_DENY_NOT_DURABLE);
	p.all_bytes_durable = true;

	/* Missing post-read -> precise denial (logical DONE never
	 * substitutes). */
	p.all_post_read_ok = false;
	UT_ASSERT_EQ((int)cluster_side_retention_proof_ready(&p),
				 (int)CLUSTER_SIDE_RETENTION_DENY_NO_POST_READ);
	p.all_post_read_ok = true;

	/* Remaining consumer -> precise denial. */
	p.consumers_zero = false;
	UT_ASSERT_EQ((int)cluster_side_retention_proof_ready(&p),
				 (int)CLUSTER_SIDE_RETENTION_DENY_CONSUMER);
	p.consumers_zero = true;

	/* Invalid inputs. */
	UT_ASSERT_EQ((int)cluster_side_retention_proof_ready(NULL),
				 (int)CLUSTER_SIDE_RETENTION_DENY_INVALID);
	/* D-SIDE-08 production caller (implementation): origin_thread_id unset
	 * (0) is the judge's fail-closed shape until the orchestrator wires
	 * the real dead_tid — never READY with an anonymous origin. */
	p.failed_origin_thread = 0;
	UT_ASSERT_EQ((int)cluster_side_retention_proof_ready(&p),
				 (int)CLUSTER_SIDE_RETENTION_DENY_INVALID);
	p.failed_origin_thread = 2;
	p.affected_count = 0;
	UT_ASSERT_EQ((int)cluster_side_retention_proof_ready(&p),
				 (int)CLUSTER_SIDE_RETENTION_DENY_INVALID);
}

int
main(void)
{
	UT_PLAN(3);

	ut_setup_globals();

	UT_RUN(test_side_page_consumer_verdict);
	UT_RUN(test_side_readiness_per_resource);
	UT_RUN(test_side_retention_proof_verdicts);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
