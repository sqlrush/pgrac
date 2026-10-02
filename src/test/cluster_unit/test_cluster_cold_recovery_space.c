/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_space.c
 *	  Typed cold-crash replay plan with SPACE effects: relation extension,
 *	  CREATE, TRUNCATE (VACUUM tail truncation) and DROP across retained
 *	  writer generations.
 *
 *	  Pure inputs only: decoded-record identities, PageVersion components,
 *	  SPACE effects and DATA observations are fixtures.  No WAL is read and
 *	  no page or SPACE fork is written.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_space.c
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
#define INC_MID 10
#define INC_D 11
#define REL_R 100
#define REL_S 200
#define BUDGET (4 * 1024 * 1024)
#define MAX_OBS 16

typedef struct ObserveTable {
	int count;
	int calls;
	RfPageIdentityV1 page[MAX_OBS];
	ClusterColdDataV1 data[MAX_OBS];
} ObserveTable;

static RfPageIdentityV1
page_id(Oid rel, BlockNumber block)
{
	RfPageIdentityV1 page;

	memset(&page, 0, sizeof(page));
	page.system_identifier = 99;
	memset(page.storage_uuid, 3, sizeof(page.storage_uuid));
	page.locator.spcOid = 1663;
	page.locator.dbOid = 5;
	page.locator.relNumber = rel;
	page.forknum = MAIN_FORKNUM;
	page.blockno = block;
	return page;
}

static RfPageVersionV1
ver(uint8 incarnation, uint64 token)
{
	RfPageVersionV1 version;

	memset(version.segment_incarnation, incarnation, sizeof(version.segment_incarnation));
	version.mutation_token = token;
	return version;
}

static ClusterColdComponentV1
in_fork(ClusterColdComponentV1 c, ForkNumber fork)
{
	c.page.forknum = fork;
	return c;
}

static ClusterColdComponentV1
delta(uint8 inc, Oid rel, BlockNumber block, uint64 before, uint64 result)
{
	ClusterColdComponentV1 c;

	memset(&c, 0, sizeof(c));
	c.page = page_id(rel, block);
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

/* A new page of an incarnation (relation creation or re-extension). */
static ClusterColdComponentV1
init_new(uint8 inc, Oid rel, BlockNumber block, uint64 result)
{
	ClusterColdComponentV1 c = delta(inc, rel, block, 0, result);

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
	op.locator = page_id(rel, 0).locator;
	memset(op.before_incarnation, before_inc, sizeof(op.before_incarnation));
	memset(op.result_incarnation, result_inc, sizeof(op.result_incarnation));
	op.payload = payload_bytes;
	op.payload_length = sizeof(payload_bytes);
	return op;
}

static ClusterColdParticipantV1
part(uint16 thread, uint64 incarnation, XLogRecPtr lower, XLogRecPtr redo, XLogRecPtr tail)
{
	ClusterColdParticipantV1 p;

	memset(&p, 0, sizeof(p));
	p.thread_id = thread;
	p.timeline = 1;
	p.owner_incarnation = incarnation;
	p.physical_lower = lower;
	p.native_redo = redo;
	p.tail_end = tail;
	return p;
}

/* Previous read pointer per participant (xl_prev). */
static XLogRecPtr last_read[2];

static void
chains_reset(void)
{
	last_read[0] = last_read[1] = 0;
}

static ClusterColdDetailV1
feed_any(ClusterColdPlanV1 *plan, uint32 participant, XLogRecPtr read, XLogRecPtr end, uint64 scn,
		 uint16 count, const ClusterColdComponentV1 *components, uint32 space_count,
		 const ClusterColdSpaceOpV1 *space_ops)
{
	ClusterColdRecordV1 record;

	memset(&record, 0, sizeof(record));
	record.read_rec_ptr = read;
	record.end_rec_ptr = end;
	record.prev_rec_ptr = last_read[participant] == 0 ? read - 0x40 : last_read[participant];
	record.scn = scn;
	record.record_crc = (uint32)(read ^ end);
	record.rmid = space_count > 0 ? RM_SMGR_ID : RM_HEAP_ID;
	record.component_count = count;
	record.components = components;
	record.space_count = space_count;
	record.space_ops = space_ops;
	last_read[participant] = read;
	return cluster_cold_plan_feed_v1(plan, participant, &record);
}

static ClusterColdDetailV1
feed_page(ClusterColdPlanV1 *plan, uint32 participant, XLogRecPtr read, XLogRecPtr end, uint64 scn,
		  ClusterColdComponentV1 c)
{
	return feed_any(plan, participant, read, end, scn, 1, &c, 0, NULL);
}

static ClusterColdDetailV1
feed_space(ClusterColdPlanV1 *plan, uint32 participant, XLogRecPtr read, XLogRecPtr end, uint64 scn,
		   ClusterColdSpaceOpV1 op)
{
	return feed_any(plan, participant, read, end, scn, 0, NULL, 1, &op);
}

static ClusterColdDetailV1
feed_plain(ClusterColdPlanV1 *plan, uint32 participant, XLogRecPtr read, XLogRecPtr end)
{
	return feed_any(plan, participant, read, end, 1, 0, NULL, 0, NULL);
}

static void
observe_fork(ObserveTable *table, Oid rel, ForkNumber fork, BlockNumber block, uint8 kind,
			 RfPageVersionV1 version, uint8 flags)
{
	int i = table->count++;

	table->page[i] = page_id(rel, block);
	table->page[i].forknum = fork;
	memset(&table->data[i], 0, sizeof(table->data[i]));
	table->data[i].kind = kind;
	table->data[i].version = version;
	table->data[i].flags = flags;
}

static void
observe_set(ObserveTable *table, Oid rel, BlockNumber block, uint8 kind, RfPageVersionV1 version,
			uint8 flags)
{
	observe_fork(table, rel, MAIN_FORKNUM, block, kind, version, flags);
}

#define VERIFIED CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED

static bool
observe(void *arg, const RfPageIdentityV1 *page, ClusterColdDataV1 *out)
{
	ObserveTable *table = (ObserveTable *)arg;
	int i;

	table->calls++;
	for (i = 0; i < table->count; i++)
		if (memcmp(&table->page[i], page, sizeof(*page)) == 0) {
			*out = table->data[i];
			return true;
		}
	memset(out, 0, sizeof(*out));
	out->kind = CLUSTER_COLD_DATA_INVALID;
	return true;
}

static ClusterColdPlanV1 *
make_plan(const ClusterColdParticipantV1 *participants, uint32 count)
{
	ClusterColdPlanV1 *plan = NULL;

	chains_reset();
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(participants, count, BUDGET, &plan), CLUSTER_COLD_OK);
	return plan;
}

static bool
step_at(const ClusterColdPlanV1 *plan, uint32 index, uint32 participant, XLogRecPtr read,
		ClusterColdStepV1 *out)
{
	return cluster_cold_plan_step_v1(plan, index, out) && out->participant == participant
		   && out->read_rec_ptr == read;
}

static bool
version_is(const RfPageVersionV1 *version, uint8 inc, uint64 token)
{
	RfPageVersionV1 expected = ver(inc, token);

	return memcmp(version, &expected, sizeof(expected)) == 0;
}

/*
 * Relation extension moves only the SPACE reservation: the page records are
 * planned as before, and the reservation updates after the native redo start
 * are collected for the SPACE owner, never scheduled.
 */
UT_TEST(test_space_advance_collected_not_scheduled)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1100, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdSpaceInputV1 input;
	RelFileLocator locator;
	uint32 count = 0;
	char mutable_payload[4] = { 'A', 'D', 'V', '1' };
	ClusterColdSpaceOpV1 adv = space_op(CLUSTER_COLD_SPACE_ADVANCE, REL_R, 0, INC_OLD, 0);

	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 1, adv), CLUSTER_COLD_OK); /* history */
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1100, 0x1200, 2, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	adv.payload = mutable_payload;
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1200, 0x2000, 3, adv), CLUSTER_COLD_OK);
	mutable_payload[0] = 'X'; /* the plan keeps its own copy */
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1000, 0x1100, 4, adv), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1100, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_OLD, 1), VERIFIED);

	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 1);
	UT_ASSERT_EQ(cluster_cold_plan_space_relation_count_v1(plan), 1);
	UT_ASSERT(cluster_cold_plan_space_relation_v1(plan, 0, &locator, &count));
	UT_ASSERT_EQ(locator.relNumber, REL_R);
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT(cluster_cold_plan_space_input_v1(plan, 0, 0, &input));
	UT_ASSERT_EQ(input.kind, CLUSTER_COLD_SPACE_ADVANCE);
	UT_ASSERT_EQ(input.participant, 0);
	UT_ASSERT_EQ(input.read_rec_ptr, 0x1200);
	UT_ASSERT_EQ(input.payload_length, 4);
	UT_ASSERT(memcmp(input.payload, "ADV1", 4) == 0);
	UT_ASSERT(cluster_cold_plan_space_input_v1(plan, 0, 1, &input));
	UT_ASSERT_EQ(input.participant, 1);
	UT_ASSERT(!cluster_cold_plan_space_input_v1(plan, 0, 2, &input));
	UT_ASSERT_EQ(cluster_cold_plan_replay_record_count_v1(plan, 0), 2);
	cluster_cold_plan_destroy_v1(&plan);
}

/* A CREATE after a native redo start is replayed by the SPACE owner before
 * any page of the new relation, whatever the SCN order. */
UT_TEST(test_space_create_orders_new_relation_pages)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;

	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 10,
							space_op(CLUSTER_COLD_SPACE_CREATE, REL_S, 0, INC_C, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x1100, 1, init_new(INC_C, REL_S, 0, 5)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1100, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, REL_S, 0, CLUSTER_COLD_DATA_ABSENT, ver(0, 0), VERIFIED);

	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 2);
	UT_ASSERT(step_at(plan, 0, 0, 0x1000, &step));
	UT_ASSERT_EQ(step.step_kind, CLUSTER_COLD_STEP_SPACE);
	UT_ASSERT_EQ(step.space_kind, CLUSTER_COLD_SPACE_CREATE);
	UT_ASSERT_EQ(step.space_relation, 0);
	UT_ASSERT_EQ(step.space_input, 0);
	UT_ASSERT(!step.all_skip);
	UT_ASSERT(step_at(plan, 1, 1, 0x1000, &step));
	UT_ASSERT_EQ(step.step_kind, CLUSTER_COLD_STEP_PAGE);
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_ABSENT);
	cluster_cold_plan_destroy_v1(&plan);
}

/*
 * VACUUM tail truncation: every change before it is durable (object
 * checkpoint); blocks below nblocks continue in the new incarnation from the
 * same token, blocks at or past nblocks are retired and re-extended from
 * ABSENT.  The truncation is a SPACE step before the new incarnation's pages.
 */
static ClusterColdPlanV1 *
truncate_fixture(ObserveTable *table, RfPageVersionV1 block0, uint8 block1_kind,
				 RfPageVersionV1 block1)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);

	memset(table, 0, sizeof(*table));
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1000, 0x1100, 1, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1100, 0x1200, 2, delta(INC_OLD, REL_R, 1, 5, 6)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1200, 0x1300, 3,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_NEW, 1)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1300, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x1100, 4, delta(INC_NEW, REL_R, 0, 2, 3)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1100, 0x1200, 5, init_new(INC_NEW, REL_R, 1, 7)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1200, 0x2000), CLUSTER_COLD_OK);
	observe_set(table, REL_R, 0, CLUSTER_COLD_DATA_PRESENT, block0, VERIFIED);
	observe_set(table, REL_R, 1, block1_kind, block1, VERIFIED);
	return plan;
}

UT_TEST(test_space_truncate_boundary_rebase_and_retire)
{
	ObserveTable table;
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;
	ClusterColdPlanV1 *plan;
	int pass;

	/* Identity durable (new) or still the old one: the same page state. */
	for (pass = 0; pass < 2; pass++) {
		plan = truncate_fixture(&table, ver(pass == 0 ? INC_NEW : INC_OLD, 2),
								CLUSTER_COLD_DATA_PRESENT, ver(INC_OLD, 6));
		UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 5);
		UT_ASSERT(step_at(plan, 0, 0, 0x1000, &step));
		UT_ASSERT(step.all_skip); /* durable before the truncation */
		UT_ASSERT(step_at(plan, 1, 0, 0x1100, &step));
		UT_ASSERT(step.all_skip); /* retired by the truncation */
		UT_ASSERT(step_at(plan, 2, 0, 0x1200, &step));
		UT_ASSERT_EQ(step.step_kind, CLUSTER_COLD_STEP_SPACE);
		UT_ASSERT_EQ(step.space_kind, CLUSTER_COLD_SPACE_TRUNCATE);
		UT_ASSERT(step_at(plan, 3, 1, 0x1000, &step));
		UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
		UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_PRESENT);
		UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_NEW, 2));
		UT_ASSERT(step_at(plan, 4, 1, 0x1100, &step));
		UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
		/* the stale retired content is replaced, not trusted */
		UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
		cluster_cold_plan_destroy_v1(&plan);
	}

	/* A shrink that reached disk leaves the retired block absent. */
	plan = truncate_fixture(&table, ver(INC_NEW, 2), CLUSTER_COLD_DATA_ABSENT, ver(0, 0));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(step_at(plan, 4, 1, 0x1100, &step));
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_ABSENT);
	cluster_cold_plan_destroy_v1(&plan);

	/* Old content under the new identity, or a zeroed block under the old
	 * one, at a retired block: replaced by the new page's init. */
	plan = truncate_fixture(&table, ver(INC_NEW, 2), CLUSTER_COLD_DATA_PRESENT, ver(INC_NEW, 6));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(step_at(plan, 4, 1, 0x1100, &step));
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
	cluster_cold_plan_destroy_v1(&plan);
	plan
		= truncate_fixture(&table, ver(INC_NEW, 2), CLUSTER_COLD_DATA_UNFORMATTED, ver(INC_OLD, 0));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(step_at(plan, 4, 1, 0x1100, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
	cluster_cold_plan_destroy_v1(&plan);

	/* DATA behind the object checkpoint cannot be: refused. */
	plan = truncate_fixture(&table, ver(INC_OLD, 1), CLUSTER_COLD_DATA_ABSENT, ver(0, 0));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_HISTORY_GAP);
	cluster_cold_plan_destroy_v1(&plan);
}

/* A page written after the truncation while the SPACE identity on disk is
 * still the old one: its token alone places it. */
UT_TEST(test_space_stale_identity_newer_page)
{
	ObserveTable table;
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;
	ClusterColdPlanV1 *plan
		= truncate_fixture(&table, ver(INC_OLD, 3), CLUSTER_COLD_DATA_ABSENT, ver(0, 0));

	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(step_at(plan, 3, 1, 0x1000, &step));
	UT_ASSERT(step.all_skip);
	cluster_cold_plan_destroy_v1(&plan);

	/* An incarnation outside the relation's lineage is not this page. */
	plan = truncate_fixture(&table, ver(INC_C, 3), CLUSTER_COLD_DATA_ABSENT, ver(0, 0));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_INCARNATION_MISMATCH);
	cluster_cold_plan_destroy_v1(&plan);
}

/* A truncation carries no block at or past its size: a version there that
 * claims to continue an older incarnation is not this page. */
UT_TEST(test_space_truncate_does_not_carry_past_size)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;

	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 1,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_NEW, 1)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x2000, 2, delta(INC_NEW, REL_R, 1, 6, 7)),
				 CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 1, CLUSTER_COLD_DATA_PRESENT, ver(INC_OLD, 6), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_INCARNATION_MISMATCH);
	cluster_cold_plan_destroy_v1(&plan);

	/* ...and a block it carried does not start new over unrelated content. */
	plan = make_plan(parts, 2);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 1,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_NEW, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x2000, 2, init_new(INC_NEW, REL_R, 1, 7)),
				 CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 1, CLUSTER_COLD_DATA_PRESENT, ver(INC_OLD, 6), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_ANCESTOR_MISSING);
	cluster_cold_plan_destroy_v1(&plan);
}

/*
 * Two truncations, the first two in history.  The block is carried through
 * an incarnation without changes to it, a header may name that one, and
 * nothing waits for a durable truncation.
 */
UT_TEST(test_space_carried_through_two_truncations)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1300, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;

	UT_ASSERT_EQ(feed_page(plan, 0, 0x1000, 0x1100, 1, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1100, 0x1200, 2,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_MID, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1200, 0x1300, 3,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_MID, INC_NEW, 1)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1300, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x2000, 4, delta(INC_NEW, REL_R, 0, 2, 3)),
				 CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_MID, 2), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 1);
	UT_ASSERT(step_at(plan, 0, 1, 0x1000, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_NEW, 2));
	UT_ASSERT_EQ(cluster_cold_plan_space_relation_count_v1(plan), 0);
	cluster_cold_plan_destroy_v1(&plan);
}

/*
 * Outside the main fork the truncated size is not in the effect: a block
 * that continues keeps its version, and a block that starts new in the
 * new incarnation was removed.
 */
UT_TEST(test_space_truncate_visibility_map_fork)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;

	/* the map page cleared past the new size, logged as a full image */
	UT_ASSERT_EQ(
		feed_page(plan, 0, 0x1000, 0x1100, 1,
				  in_fork(full_image(delta(INC_OLD, REL_R, 0, 1, 2)), VISIBILITYMAP_FORKNUM)),
		CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1100, 0x1200, 2,
						   in_fork(delta(INC_OLD, REL_R, 1, 5, 6), VISIBILITYMAP_FORKNUM)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1200, 0x1300, 3,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_NEW, 1)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1300, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x1100, 4,
						   in_fork(delta(INC_NEW, REL_R, 0, 2, 3), VISIBILITYMAP_FORKNUM)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1100, 0x2000, 5,
						   in_fork(init_new(INC_NEW, REL_R, 1, 7), VISIBILITYMAP_FORKNUM)),
				 CLUSTER_COLD_OK);
	observe_fork(&table, REL_R, VISIBILITYMAP_FORKNUM, 0, CLUSTER_COLD_DATA_PRESENT,
				 ver(INC_OLD, 2), VERIFIED);
	observe_fork(&table, REL_R, VISIBILITYMAP_FORKNUM, 1, CLUSTER_COLD_DATA_PRESENT,
				 ver(INC_OLD, 6), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 5);
	/* that image is durable: never restored, though it is an anchor */
	UT_ASSERT(step_at(plan, 0, 0, 0x1000, &step));
	UT_ASSERT(step.all_skip);
	UT_ASSERT(step_at(plan, 3, 1, 0x1000, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_NEW, 2));
	UT_ASSERT(step_at(plan, 4, 1, 0x1100, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
	cluster_cold_plan_destroy_v1(&plan);
}

/* A token reached in two incarnations of the lineage places nothing. */
UT_TEST(test_space_token_repeated_across_incarnations)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;

	UT_ASSERT_EQ(feed_page(plan, 0, 0x1000, 0x1100, 1, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1100, 0x1200, 2,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_NEW, 1)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1200, 0x1300, 3, delta(INC_NEW, REL_R, 0, 2, 3)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1300, 0x1400, 4,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_NEW, INC_D, 1)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1400, 0x2000, 5, delta(INC_D, REL_R, 0, 3, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1000, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_D, 2), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_CHAIN_AMBIGUOUS);
	cluster_cold_plan_destroy_v1(&plan);
}

/* Changes to a dropped relation are irrelevant: skipped, its pages not
 * observed, its drop handed to the SPACE owner. */
UT_TEST(test_space_drop_makes_changes_irrelevant)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;
	ClusterColdSpaceInputV1 input;
	ClusterColdSpaceOpV1 drop = space_op(CLUSTER_COLD_SPACE_DROP, REL_R, INC_OLD, 0, 0);

	UT_ASSERT_EQ(feed_page(plan, 0, 0x1000, 0x1100, 1, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_any(plan, 1, 0x1000, 0x1100, 5, 0, NULL, 1, &drop), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1100, 0x2000), CLUSTER_COLD_OK);

	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(table.calls, 0);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 1);
	UT_ASSERT(step_at(plan, 0, 0, 0x1000, &step));
	UT_ASSERT(step.all_skip);
	UT_ASSERT_EQ(cluster_cold_plan_space_relation_count_v1(plan), 1);
	UT_ASSERT(cluster_cold_plan_space_input_v1(plan, 0, 0, &input));
	UT_ASSERT_EQ(input.kind, CLUSTER_COLD_SPACE_DROP);
	cluster_cold_plan_destroy_v1(&plan);

	/* The relation file number is reused by a new incarnation: the dropped
	 * one's changes were never written and do not join its pages. */
	plan = make_plan(parts, 2);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1000, 0x1100, 1, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_any(plan, 1, 0x1000, 0x1100, 5, 0, NULL, 1, &drop), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1100, 0x1200, 6,
							space_op(CLUSTER_COLD_SPACE_CREATE, REL_R, 0, INC_C, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1200, 0x2000, 7, init_new(INC_C, REL_R, 0, 3)),
				 CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_ABSENT, ver(0, 0), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 3);
	UT_ASSERT(step_at(plan, 2, 1, 0x1200, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_ABSENT);
	cluster_cold_plan_destroy_v1(&plan);
}

/* A relation created after a native redo start may have pages on disk before
 * its SPACE identity: placed by token.  Any other page without an identity is
 * refused. */
UT_TEST(test_space_created_relation_without_identity)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdStepV1 step;

	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 1,
							space_op(CLUSTER_COLD_SPACE_CREATE, REL_S, 0, INC_C, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x1100, 2, init_new(INC_C, REL_S, 0, 5)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1100, 0x1200, 3, delta(INC_C, REL_S, 0, 5, 6)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1200, 0x2000, 4, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	observe_set(&table, REL_S, 0, CLUSTER_COLD_DATA_PRESENT, ver(0, 5),
				VERIFIED | CLUSTER_COLD_DATA_FLAG_NO_IDENTITY);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_OLD, 1), VERIFIED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	/* placed on the init's result: that anchor is restored, as always */
	UT_ASSERT(step_at(plan, 1, 1, 0x1000, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_PRESENT);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_C, 5));
	UT_ASSERT(step_at(plan, 2, 1, 0x1100, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_C, 5));
	cluster_cold_plan_destroy_v1(&plan);

	/* Created and then truncated here: still no identity on disk. */
	plan = make_plan(parts, 2);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 1,
							space_op(CLUSTER_COLD_SPACE_CREATE, REL_S, 0, INC_C, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1100, 0x2000, 2,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_S, INC_C, INC_D, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1000, 0x1100, 3, init_new(INC_D, REL_S, 0, 5)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_page(plan, 1, 0x1100, 0x2000, 4, delta(INC_D, REL_S, 0, 5, 6)),
				 CLUSTER_COLD_OK);
	observe_set(&table, REL_S, 0, CLUSTER_COLD_DATA_PRESENT, ver(0, 6),
				VERIFIED | CLUSTER_COLD_DATA_FLAG_NO_IDENTITY);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 4);
	UT_ASSERT(step_at(plan, 3, 1, 0x1100, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_D, 5));
	cluster_cold_plan_destroy_v1(&plan);

	/* R was not created here: a page without an identity is refused. */
	plan = make_plan(parts, 2);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1000, 0x2000, 1, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1000, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_PRESENT, ver(0, 1),
				VERIFIED | CLUSTER_COLD_DATA_FLAG_NO_IDENTITY);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_IDENTITY_MISSING);
	cluster_cold_plan_destroy_v1(&plan);
}

UT_TEST(test_space_effect_validation)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan;
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdComponentV1 c = delta(INC_OLD, REL_R, 0, 1, 2);
	ClusterColdSpaceOpV1 op = space_op(CLUSTER_COLD_SPACE_ADVANCE, REL_R, 0, INC_OLD, 0);

	/* A record carries page components or SPACE effects, not both. */
	plan = make_plan(parts, 2);
	UT_ASSERT_EQ(feed_any(plan, 0, 0x1000, 0x2000, 1, 1, &c, 1, &op),
				 CLUSTER_COLD_COMPONENT_INVALID);
	cluster_cold_plan_destroy_v1(&plan);

	plan = make_plan(parts, 2);
	op.kind = 9;
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1, op), CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_plan_destroy_v1(&plan);

	plan = make_plan(parts, 2);
	op = space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, 0, INC_NEW, 1);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1, op), CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_plan_destroy_v1(&plan);

	plan = make_plan(parts, 2);
	op = space_op(CLUSTER_COLD_SPACE_ADVANCE, REL_R, 0, INC_OLD, 0);
	op.payload = NULL;
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1, op), CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_plan_destroy_v1(&plan);

	/* Two creations of one incarnation are a branch. */
	plan = make_plan(parts, 2);
	op = space_op(CLUSTER_COLD_SPACE_CREATE, REL_S, 0, INC_C, 0);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1, op), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1000, 0x2000, 2, op), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_SPACE_INVALID);
	UT_ASSERT(diag.has_record);
	UT_ASSERT_EQ(diag.participant, 1);
	cluster_cold_plan_destroy_v1(&plan);

	/* Also when one of them is in history. */
	parts[0].native_redo = 0x1100;
	plan = make_plan(parts, 2);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x1100, 1, op), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1000, 0x2000, 2, op), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_plan_destroy_v1(&plan);
	parts[0].native_redo = 0x1000;

	/* So are two ends of one: a truncation and a drop of the same one. */
	plan = make_plan(parts, 2);
	UT_ASSERT_EQ(feed_space(plan, 0, 0x1000, 0x2000, 1,
							space_op(CLUSTER_COLD_SPACE_TRUNCATE, REL_R, INC_OLD, INC_NEW, 1)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_space(plan, 1, 0x1000, 0x2000, 2,
							space_op(CLUSTER_COLD_SPACE_DROP, REL_R, INC_OLD, 0, 0)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_plan_destroy_v1(&plan);

	/* A page without an identity names no incarnation. */
	plan = make_plan(parts, 2);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed_page(plan, 0, 0x1000, 0x2000, 1, delta(INC_OLD, REL_R, 0, 1, 2)),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1000, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, REL_R, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_OLD, 1),
				VERIFIED | CLUSTER_COLD_DATA_FLAG_NO_IDENTITY);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_OBSERVATION_FAILED);
	cluster_cold_plan_destroy_v1(&plan);
}

int
main(void)
{
	UT_RUN(test_space_advance_collected_not_scheduled);
	UT_RUN(test_space_create_orders_new_relation_pages);
	UT_RUN(test_space_truncate_boundary_rebase_and_retire);
	UT_RUN(test_space_stale_identity_newer_page);
	UT_RUN(test_space_truncate_does_not_carry_past_size);
	UT_RUN(test_space_carried_through_two_truncations);
	UT_RUN(test_space_truncate_visibility_map_fork);
	UT_RUN(test_space_token_repeated_across_incarnations);
	UT_RUN(test_space_drop_makes_changes_irrelevant);
	UT_RUN(test_space_created_relation_without_identity);
	UT_RUN(test_space_effect_validation);
	UT_DONE();
	return ut_failed_count != 0;
}
