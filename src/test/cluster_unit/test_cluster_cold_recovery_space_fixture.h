/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_space_fixture.h
 *	  Fixtures for test_cluster_cold_recovery_space.c: PageVersion
 *	  components, SPACE effects, participants, record feeding, a DATA
 *	  observation table and a stand-in SPACE owner check.
 *
 *	  Included once by test_cluster_cold_recovery_space.c.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_space_fixture.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_COLD_RECOVERY_SPACE_FIXTURE_H
#define TEST_CLUSTER_COLD_RECOVERY_SPACE_FIXTURE_H

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

/*
 * Stand-in for the SPACE owner's check: feed order, CREATEs first (as a
 * chain would order them), a refusal, or a malformed order.
 */
typedef enum CheckMode {
	CHECK_FEED_ORDER,
	CHECK_CREATE_FIRST,
	CHECK_REFUSE,
	CHECK_BAD_ORDER,   /* a repeated position */
	CHECK_OUT_OF_RANGE /* a position past the inputs */
} CheckMode;

typedef struct CheckLog {
	CheckMode mode;
	int calls;
	uint32 last_count;
	RelFileLocator last_locator;
	ClusterColdSpaceInputV1 last_inputs[8];
} CheckLog;

static CheckLog check_log;

static bool
space_check(void *arg, const RelFileLocator *locator, const ClusterColdSpaceInputV1 *inputs,
			uint32 count, uint32 *order)
{
	CheckLog *log = (CheckLog *)arg;
	uint32 next = 0;
	uint32 i;

	log->calls++;
	log->last_count = count;
	log->last_locator = *locator;
	for (i = 0; i < count && i < lengthof(log->last_inputs); i++)
		log->last_inputs[i] = inputs[i];
	switch (log->mode) {
	case CHECK_REFUSE:
		/* a well-formed order does not make a refusal an answer */
		for (i = 0; i < count; i++)
			order[i] = i;
		return false;
	case CHECK_BAD_ORDER:
		for (i = 0; i < count; i++)
			order[i] = 0;
		return true;
	case CHECK_OUT_OF_RANGE:
		for (i = 0; i < count; i++)
			order[i] = i + 1;
		return true;
	case CHECK_CREATE_FIRST:
		for (i = 0; i < count; i++)
			if (inputs[i].kind == CLUSTER_COLD_SPACE_CREATE)
				order[next++] = i;
		for (i = 0; i < count; i++)
			if (inputs[i].kind != CLUSTER_COLD_SPACE_CREATE)
				order[next++] = i;
		return true;
	case CHECK_FEED_ORDER:
		for (i = 0; i < count; i++)
			order[i] = i;
		return true;
	}
	return false;
}

static ClusterColdPlanV1 *
make_plan_checked(const ClusterColdParticipantV1 *participants, uint32 count, CheckMode mode)
{
	ClusterColdPlanV1 *plan = NULL;

	chains_reset();
	memset(&check_log, 0, sizeof(check_log));
	check_log.mode = mode;
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(participants, count, BUDGET, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_set_space_check_v1(plan, space_check, &check_log),
				 CLUSTER_COLD_OK);
	return plan;
}

static ClusterColdPlanV1 *
make_plan(const ClusterColdParticipantV1 *participants, uint32 count)
{
	return make_plan_checked(participants, count, CHECK_FEED_ORDER);
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

#endif /* TEST_CLUSTER_COLD_RECOVERY_SPACE_FIXTURE_H */
