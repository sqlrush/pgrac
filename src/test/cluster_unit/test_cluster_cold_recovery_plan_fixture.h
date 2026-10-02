/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_plan_fixture.h
 *	  Fixtures for test_cluster_cold_recovery_plan.c: PageVersion
 *	  components, participants, chained record feeding, a DATA observation
 *	  table and schedule helpers.
 *
 *	  Included once by test_cluster_cold_recovery_plan.c.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_plan_fixture.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_COLD_RECOVERY_PLAN_FIXTURE_H
#define TEST_CLUSTER_COLD_RECOVERY_PLAN_FIXTURE_H

#define INC_I 7
#define INC_J 9
#define BUDGET (4 * 1024 * 1024)
#define MAX_OBS 16

typedef struct ObserveTable {
	int count;
	int calls;
	bool fail;
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
delta(uint8 block_id, Oid rel, BlockNumber block, uint64 before, uint64 result)
{
	ClusterColdComponentV1 component;

	memset(&component, 0, sizeof(component));
	component.page = page_id(rel, block);
	component.block_id = block_id;
	component.component_ordinal = block_id;
	component.page_class = RF_PAGE_CLASS_ORDINARY;
	component.before_kind = RF_PAGE_STATE_PRESENT;
	component.result_kind = RF_PAGE_STATE_PRESENT;
	component.before = ver(INC_I, before);
	component.result = ver(INC_I, result);
	return component;
}

static ClusterColdComponentV1
fpi(uint8 block_id, Oid rel, BlockNumber block, uint64 before, uint64 result)
{
	ClusterColdComponentV1 component = delta(block_id, rel, block, before, result);

	component.edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
	return component;
}

static ClusterColdComponentV1
init_new(uint8 block_id, Oid rel, BlockNumber block, uint8 before_kind, uint64 result)
{
	ClusterColdComponentV1 component = delta(block_id, rel, block, 0, result);

	component.before_kind = before_kind;
	memset(component.before.segment_incarnation, before_kind == RF_PAGE_STATE_ABSENT ? 0 : INC_I,
		   sizeof(component.before.segment_incarnation));
	component.before.mutation_token = 0;
	component.edge_flags = RF_PAGE_EDGE_WILL_INIT | RF_PAGE_EDGE_FULL_COVERAGE;
	return component;
}

static ClusterColdParticipantV1
part(uint16 thread, uint64 incarnation, XLogRecPtr lower, XLogRecPtr redo, XLogRecPtr tail)
{
	ClusterColdParticipantV1 participant;

	memset(&participant, 0, sizeof(participant));
	participant.thread_id = thread;
	participant.timeline = 1;
	participant.owner_incarnation = incarnation;
	participant.physical_lower = lower;
	participant.native_redo = redo;
	participant.tail_end = tail;
	return participant;
}

/* Fixtures chain each fed record to the previous record of its participant,
 * like xl_prev; records of different plans/participants are tracked apart. */
#define MAX_CHAINS 32
static struct {
	const ClusterColdPlanV1 *plan;
	uint32 participant;
	XLogRecPtr last_read;
} chains[MAX_CHAINS];

static XLogRecPtr
chain_prev(const ClusterColdPlanV1 *plan, uint32 participant, XLogRecPtr read)
{
	int i;

	for (i = 0; i < MAX_CHAINS; i++)
		if (chains[i].plan == plan && chains[i].participant == participant) {
			XLogRecPtr prev = chains[i].last_read;

			chains[i].last_read = read;
			return prev;
		}
	for (i = 0; i < MAX_CHAINS; i++)
		if (chains[i].plan == NULL) {
			chains[i].plan = plan;
			chains[i].participant = participant;
			chains[i].last_read = read;
			return read - 0x40; /* any record before the physical lower */
		}
	abort();
}

static void
chain_forget(const ClusterColdPlanV1 *plan)
{
	int i;

	for (i = 0; i < MAX_CHAINS; i++)
		if (chains[i].plan == plan)
			chains[i].plan = NULL;
}

static ClusterColdDetailV1
feed(ClusterColdPlanV1 *plan, uint32 participant, XLogRecPtr read, XLogRecPtr end, uint64 scn,
	 uint16 count, const ClusterColdComponentV1 *components)
{
	ClusterColdRecordV1 record;

	memset(&record, 0, sizeof(record));
	record.read_rec_ptr = read;
	record.end_rec_ptr = end;
	record.scn = scn;
	record.record_crc = (uint32)(read ^ end);
	record.prev_rec_ptr = chain_prev(plan, participant, read);
	record.rmid = RM_HEAP_ID;
	record.component_count = count;
	record.components = components;
	return cluster_cold_plan_feed_v1(plan, participant, &record);
}

static ClusterColdDetailV1
feed_plain(ClusterColdPlanV1 *plan, uint32 participant, XLogRecPtr read, XLogRecPtr end)
{
	return feed(plan, participant, read, end, 1, 0, NULL);
}

static ClusterColdDetailV1
feed_flagged(ClusterColdPlanV1 *plan, uint32 participant, XLogRecPtr read, XLogRecPtr end,
			 uint8 flags)
{
	ClusterColdRecordV1 record;

	memset(&record, 0, sizeof(record));
	record.read_rec_ptr = read;
	record.end_rec_ptr = end;
	record.scn = 1;
	record.record_crc = (uint32)(read ^ end);
	record.prev_rec_ptr = chain_prev(plan, participant, read);
	record.rmid = RM_SMGR_ID;
	record.record_flags = flags;
	return cluster_cold_plan_feed_v1(plan, participant, &record);
}

static void
observe_set(ObserveTable *table, Oid rel, BlockNumber block, uint8 kind, RfPageVersionV1 version)
{
	int i = table->count++;

	table->page[i] = page_id(rel, block);
	memset(&table->data[i], 0, sizeof(table->data[i]));
	table->data[i].kind = kind;
	table->data[i].version = version;
	/* Fixtures model checksum-verified content unless a test says otherwise. */
	if (kind != CLUSTER_COLD_DATA_INVALID)
		table->data[i].flags = CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED;
}

static bool
observe(void *arg, const RfPageIdentityV1 *page, ClusterColdDataV1 *out)
{
	ObserveTable *table = (ObserveTable *)arg;
	int i;

	table->calls++;
	if (table->fail)
		return false;
	for (i = 0; i < table->count; i++)
		if (memcmp(&table->page[i], page, sizeof(*page)) == 0) {
			*out = table->data[i];
			return true;
		}
	memset(out, 0, sizeof(*out));
	out->kind = CLUSTER_COLD_DATA_INVALID;
	return true;
}

static void
destroy_plan(ClusterColdPlanV1 **plan)
{
	chain_forget(*plan);
	cluster_cold_plan_destroy_v1(plan);
}

static ClusterColdPlanV1 *
make_plan(const ClusterColdParticipantV1 *participants, uint32 count)
{
	ClusterColdPlanV1 *plan = NULL;

	UT_ASSERT_EQ(cluster_cold_plan_create_v1(participants, count, BUDGET, &plan), CLUSTER_COLD_OK);
	return plan;
}

static bool
find_step(const ClusterColdPlanV1 *plan, uint32 participant, XLogRecPtr read,
		  ClusterColdStepV1 *out, uint32 *position)
{
	uint32 n = cluster_cold_plan_step_count_v1(plan);
	uint32 i;

	for (i = 0; i < n; i++) {
		ClusterColdStepV1 step;

		if (!cluster_cold_plan_step_v1(plan, i, &step))
			return false;
		if (step.participant == participant && step.read_rec_ptr == read) {
			if (out != NULL)
				*out = step;
			if (position != NULL)
				*position = i;
			return true;
		}
	}
	return false;
}

static bool
version_is(const RfPageVersionV1 *version, uint8 incarnation, uint64 token)
{
	RfPageVersionV1 expected = ver(incarnation, token);

	return memcmp(version, &expected, sizeof(expected)) == 0;
}

/* Two pages written alternately by two generations. */
static void
build_interleaved(ClusterColdPlanV1 *plan, uint32 a, uint32 b, uint64 scn_a, uint64 scn_b)
{
	ClusterColdComponentV1 a1 = delta(0, 100, 0, 1, 2);
	ClusterColdComponentV1 b1 = delta(0, 100, 0, 2, 3);
	ClusterColdComponentV1 a2 = delta(0, 100, 0, 3, 4);
	ClusterColdComponentV1 b2 = delta(0, 200, 0, 11, 12);
	ClusterColdComponentV1 a3 = delta(0, 200, 0, 12, 13);

	UT_ASSERT_EQ(feed(plan, a, 0x1000, 0x1100, scn_a, 1, &a1), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, a, 0x1100, 0x1200, scn_a, 1, &a2), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, a, 0x1200, 0x2000, scn_a, 1, &a3), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, b, 0x1000, 0x1100, scn_b, 1, &b1), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, b, 0x1100, 0x2000, scn_b, 1, &b2), CLUSTER_COLD_OK);
}

static void
interleaved_observations(ObserveTable *table)
{
	observe_set(table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 1));
	observe_set(table, 200, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 11));
}

static void
schedule_signature(const ClusterColdPlanV1 *plan, const ClusterColdParticipantV1 *parts, char *buf,
				   size_t size)
{
	uint32 n = cluster_cold_plan_step_count_v1(plan);
	size_t used = 0;
	uint32 i;

	buf[0] = '\0';
	for (i = 0; i < n; i++) {
		ClusterColdStepV1 step;
		int w;

		if (!cluster_cold_plan_step_v1(plan, i, &step))
			return;
		w = snprintf(buf + used, size - used, "t%u@%X;",
					 (unsigned)parts[step.participant].thread_id, (unsigned)step.read_rec_ptr);
		if (w < 0 || (size_t)w >= size - used)
			return;
		used += (size_t)w;
	}
}

#endif /* TEST_CLUSTER_COLD_RECOVERY_PLAN_FIXTURE_H */
