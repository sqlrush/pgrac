/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_validate.c
 *	  Typed cold-crash replay plan input validation: every malformed page
 *	  component, record and SPACE effect is refused when it is fed, before
 *	  anything is planned or observed.
 *
 *	  Pure inputs only.  No WAL is read and no page is written.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_validate.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_cold_recovery.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#define INC_OLD 7
#define INC_NEW 8
#define INC_C 9
#define REL_R 100
#define REL_S 200
#define BUDGET (1024 * 1024)

static RfPageVersionV1
ver(uint8 inc, uint64 token)
{
	RfPageVersionV1 version;

	memset(&version, 0, sizeof(version));
	memset(version.segment_incarnation, inc, sizeof(version.segment_incarnation));
	version.mutation_token = token;
	return version;
}

static ClusterColdComponentV1
delta(uint8 inc, Oid rel, uint64 before, uint64 result)
{
	ClusterColdComponentV1 c;

	memset(&c, 0, sizeof(c));
	c.page.system_identifier = 99;
	memset(c.page.storage_uuid, 3, sizeof(c.page.storage_uuid));
	c.page.locator.spcOid = 1663;
	c.page.locator.dbOid = 5;
	c.page.locator.relNumber = rel;
	c.page.forknum = MAIN_FORKNUM;
	c.page_class = RF_PAGE_CLASS_ORDINARY;
	c.before_kind = RF_PAGE_STATE_PRESENT;
	c.result_kind = RF_PAGE_STATE_PRESENT;
	c.before = ver(inc, before);
	c.result = ver(inc, result);
	return c;
}

static ClusterColdComponentV1
full_image(ClusterColdComponentV1 c)
{
	c.edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
	return c;
}

static ClusterColdComponentV1
init_new(uint8 inc, Oid rel, uint64 result)
{
	ClusterColdComponentV1 c = delta(inc, rel, 0, result);

	c.before_kind = RF_PAGE_STATE_ABSENT;
	memset(&c.before, 0, sizeof(c.before));
	c.edge_flags = RF_PAGE_EDGE_WILL_INIT | RF_PAGE_EDGE_FULL_COVERAGE;
	return c;
}

static const char payload_bytes[4] = { 'S', 'P', 'C', '1' };

static ClusterColdSpaceOpV1
space_op(uint8 kind, Oid rel, uint8 before_inc, uint8 result_inc, BlockNumber nblocks)
{
	ClusterColdSpaceOpV1 op;

	memset(&op, 0, sizeof(op));
	op.kind = kind;
	op.nblocks = nblocks;
	op.locator = delta(0, rel, 0, 0).page.locator;
	memset(op.before_incarnation, before_inc, sizeof(op.before_incarnation));
	memset(op.result_incarnation, result_inc, sizeof(op.result_incarnation));
	op.payload = payload_bytes;
	op.payload_length = sizeof(payload_bytes);
	return op;
}

static XLogRecPtr last_read[2];

static ClusterColdPlanV1 *
fresh_plan(void)
{
	ClusterColdParticipantV1 parts[2];
	ClusterColdPlanV1 *plan = NULL;
	int i;

	for (i = 0; i < 2; i++) {
		memset(&parts[i], 0, sizeof(parts[i]));
		parts[i].thread_id = (uint16)(i + 1);
		parts[i].timeline = 1;
		parts[i].owner_incarnation = 11 + i;
		parts[i].physical_lower = 0x1000;
		parts[i].native_redo = 0x1000;
		parts[i].tail_end = 0x2000;
		last_read[i] = 0;
	}
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(parts, 2, BUDGET, &plan), CLUSTER_COLD_OK);
	return plan;
}

static ClusterColdDetailV1
feed_any(ClusterColdPlanV1 *plan, uint32 participant, XLogRecPtr read, XLogRecPtr end, uint16 count,
		 const ClusterColdComponentV1 *components, uint32 space_count,
		 const ClusterColdSpaceOpV1 *space_ops)
{
	ClusterColdRecordV1 record;

	memset(&record, 0, sizeof(record));
	record.read_rec_ptr = read;
	record.end_rec_ptr = end;
	record.prev_rec_ptr = last_read[participant] == 0 ? read - 0x40 : last_read[participant];
	record.scn = 1;
	record.record_crc = (uint32)(read ^ end);
	record.rmid = space_count > 0 ? RM_SMGR_ID : RM_HEAP_ID;
	record.component_count = count;
	record.components = components;
	record.space_count = space_count;
	record.space_ops = space_ops;
	last_read[participant] = read;
	return cluster_cold_plan_feed_v1(plan, participant, &record);
}

/* Feeding one record of these components to a fresh plan. */
static ClusterColdDetailV1
feed_alone(uint16 count, const ClusterColdComponentV1 *components)
{
	ClusterColdPlanV1 *plan = fresh_plan();
	ClusterColdDetailV1 detail = feed_any(plan, 0, 0x1000, 0x2000, count, components, 0, NULL);

	cluster_cold_plan_destroy_v1(&plan);
	return detail;
}

/*
 * A component the WAL decoder never produces: a fork without WAL, an image
 * that does not cover the page, a change to its own version, a change that
 * crosses incarnations (only a TRUNCATE does), or a new page that is not an
 * anchor.
 */
UT_TEST(test_validate_component_shapes)
{
	ClusterColdComponentV1 good = delta(INC_OLD, REL_R, 1, 2);
	ClusterColdComponentV1 good_image = full_image(good);
	ClusterColdComponentV1 good_new = init_new(INC_OLD, REL_R, 5);
	ClusterColdComponentV1 fsm = good;
	ClusterColdComponentV1 partial = good_image;
	ClusterColdComponentV1 noop = delta(INC_OLD, REL_R, 2, 2);
	ClusterColdComponentV1 crossing = good;
	ClusterColdComponentV1 bare_new = good_new;

	fsm.page.forknum = FSM_FORKNUM;
	partial.edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY;
	memset(crossing.result.segment_incarnation, INC_NEW, 16);
	bare_new.edge_flags = 0;
	UT_ASSERT_EQ(feed_alone(1, &fsm), CLUSTER_COLD_COMPONENT_INVALID);
	UT_ASSERT_EQ(feed_alone(1, &partial), CLUSTER_COLD_COMPONENT_INVALID);
	UT_ASSERT_EQ(feed_alone(1, &noop), CLUSTER_COLD_COMPONENT_INVALID);
	UT_ASSERT_EQ(feed_alone(1, &crossing), CLUSTER_COLD_COMPONENT_INVALID);
	UT_ASSERT_EQ(feed_alone(1, &bare_new), CLUSTER_COLD_COMPONENT_INVALID);
	/* the same components, well formed, are accepted */
	UT_ASSERT_EQ(feed_alone(1, &good), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_alone(1, &good_image), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_alone(1, &good_new), CLUSTER_COLD_OK);
}

/*
 * Within a record each block reference is one page: a repeated block id
 * would make one block's verdict overwrite another's.  Every page belongs
 * to one cluster namespace, within a record and across records.
 */
UT_TEST(test_validate_record_components)
{
	ClusterColdComponentV1 pair[2] = { delta(INC_OLD, REL_R, 1, 2), delta(INC_OLD, REL_S, 5, 6) };
	ClusterColdComponentV1 foreign = delta(INC_OLD, REL_S, 5, 6);
	ClusterColdPlanV1 *plan;

	UT_ASSERT_EQ(feed_alone(2, pair), CLUSTER_COLD_COMPONENT_INVALID);
	pair[1].block_id = 1;
	pair[1].component_ordinal = 1;
	UT_ASSERT_EQ(feed_alone(2, pair), CLUSTER_COLD_OK);

	pair[1].page.system_identifier = 98;
	UT_ASSERT_EQ(feed_alone(2, pair), CLUSTER_COLD_COMPONENT_INVALID);
	pair[1] = delta(INC_OLD, REL_S, 5, 6);
	pair[1].block_id = 1;
	pair[1].component_ordinal = 1;
	pair[1].page.storage_uuid[0] = 4;
	UT_ASSERT_EQ(feed_alone(2, pair), CLUSTER_COLD_COMPONENT_INVALID);

	foreign.page.system_identifier = 98;
	plan = fresh_plan();
	UT_ASSERT_EQ(feed_any(plan, 0, 0x1000, 0x1100, 1, &pair[0], 0, NULL), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_any(plan, 1, 0x1000, 0x1100, 1, &foreign, 0, NULL),
				 CLUSTER_COLD_COMPONENT_INVALID);
	cluster_cold_plan_destroy_v1(&plan);
}

/*
 * A TRUNCATE makes a new incarnation; only a commit or abort carries
 * several effects (relation drops).
 */
UT_TEST(test_validate_space_effects)
{
	ClusterColdSpaceOpV1 same = space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_OLD, 1);
	ClusterColdSpaceOpV1 mixed[2]
		= { space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_NEW, 1),
			space_op(CLUSTER_COLD_SPACE_CREATE, REL_S, 0, INC_C, 0) };
	ClusterColdSpaceOpV1 drops[2] = { space_op(CLUSTER_COLD_SPACE_DROP, REL_R, INC_OLD, 0, 0),
									  space_op(CLUSTER_COLD_SPACE_DROP, REL_S, INC_C, 0, 0) };
	ClusterColdPlanV1 *plan;

	plan = fresh_plan();
	UT_ASSERT_EQ(feed_any(plan, 0, 0x1000, 0x2000, 0, NULL, 1, &same), CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_plan_destroy_v1(&plan);
	plan = fresh_plan();
	UT_ASSERT_EQ(feed_any(plan, 0, 0x1000, 0x2000, 0, NULL, 2, mixed), CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_plan_destroy_v1(&plan);
	plan = fresh_plan();
	UT_ASSERT_EQ(feed_any(plan, 0, 0x1000, 0x2000, 0, NULL, 2, drops), CLUSTER_COLD_OK);
	cluster_cold_plan_destroy_v1(&plan);
}

int
main(void)
{
	UT_RUN(test_validate_component_shapes);
	UT_RUN(test_validate_record_components);
	UT_RUN(test_validate_space_effects);
	UT_DONE();
	return ut_failed_count != 0;
}
