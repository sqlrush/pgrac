/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_plan.c
 *	  Typed cold-crash replay plan: PageVersion chains across retained
 *	  writer generations, DATA position, verdicts and deterministic schedule.
 *
 *	  Pure inputs only: decoded-record identities, PageVersion components
 *	  and DATA observations are fixtures.  No WAL is read and no page is
 *	  written; a real all-crash replay is outside this binary.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_plan.c
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

/*
 * Thread 1's FPI after its native redo is older than thread 2's change that
 * sits in thread 2's retained history and is already durable in DATA.  The
 * history edge proves the ancestry, so the FPI must be skipped, never
 * restored over the newer page.
 */
UT_TEST(test_lower_lag_old_fpi_cannot_overwrite_newer_durable)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x100, 0x1000, 0x2000), part(2, 12, 0x100, 0x1000, 0x2000) };
	ClusterColdComponentV1 a_fpi = fpi(0, 100, 0, 5, 10);
	ClusterColdComponentV1 b_delta = delta(0, 100, 0, 10, 20);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ClusterColdStepV1 step;

	UT_ASSERT_EQ(feed_plain(plan, 0, 0x100, 0x180), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1000, 0x1080), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 0, 0x1100, 0x1180, 50, 1, &a_fpi), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1180, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x100, 0x180), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x200, 0x280, 60, 1, &b_delta), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1000, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 20));

	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 1);
	UT_ASSERT(find_step(plan, 0, 0x1100, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_SKIP);
	UT_ASSERT(step.all_skip);
	UT_ASSERT(!step.mixed);
	UT_ASSERT(!find_step(plan, 1, 0x200, NULL, NULL));
	UT_ASSERT_EQ(cluster_cold_plan_replay_record_count_v1(plan, 0), 3);
	UT_ASSERT_EQ(cluster_cold_plan_replay_record_count_v1(plan, 1), 1);
	UT_ASSERT_EQ(cluster_cold_plan_replay_record_count_v1(plan, 2), 0);
	destroy_plan(&plan);
}

/* DATA behind a retained-history record contradicts the original thread's
 * checkpoint; history is never replayed to repair it. */
UT_TEST(test_data_behind_history_is_refused)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x100, 0x1000, 0x2000), part(2, 12, 0x100, 0x1000, 0x2000) };
	ClusterColdComponentV1 a_fpi = fpi(0, 100, 0, 5, 10);
	ClusterColdComponentV1 b_delta = delta(0, 100, 0, 10, 20);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);

	UT_ASSERT_EQ(feed_plain(plan, 0, 0x100, 0x1000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x1080, 50, 1, &a_fpi), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1080, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x100, 0x180, 60, 1, &b_delta), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x180, 0x1000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1000, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 5));

	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_HISTORY_GAP);
	UT_ASSERT_EQ(diag.detail, CLUSTER_COLD_HISTORY_GAP);
	UT_ASSERT(diag.has_record && diag.participant == 1 && diag.read_rec_ptr == 0x100);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 0);
	destroy_plan(&plan);
}

/* A->B->C on one page across three generations is replayed exactly in
 * version order although xl_scn and foreign LSNs point the other way. */
UT_TEST(test_a_b_c_exact_order_ignores_scn_and_lsn)
{
	ClusterColdParticipantV1 parts[3]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x400, 0x400, 0x800),
			part(3, 13, 0x9000, 0x9000, 0xa000) };
	ClusterColdComponentV1 a1 = delta(0, 100, 0, 1, 2);
	ClusterColdComponentV1 b1 = delta(0, 100, 0, 2, 3);
	ClusterColdComponentV1 c1 = delta(0, 100, 0, 3, 4);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 3);
	ClusterColdStepV1 step;
	uint32 pa = 0, pb = 0, pc = 0;

	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 300, 1, &a1), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x400, 0x800, 200, 1, &b1), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 2, 0x9000, 0xa000, 100, 1, &c1), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 1));

	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 3);
	UT_ASSERT(find_step(plan, 0, 0x1000, &step, &pa));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_I, 1));
	UT_ASSERT(version_is(&step.blocks[0].result, INC_I, 2));
	UT_ASSERT(find_step(plan, 1, 0x400, &step, &pb));
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_I, 2));
	UT_ASSERT(find_step(plan, 2, 0x9000, &step, &pc));
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_I, 3));
	UT_ASSERT(pa < pb && pb < pc);
	destroy_plan(&plan);
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

UT_TEST(test_interleaved_writers_follow_both_page_chains)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	char signature[256];

	build_interleaved(plan, 0, 1, 5, 5);
	interleaved_observations(&table);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	schedule_signature(plan, parts, signature, sizeof(signature));
	UT_ASSERT_STR_EQ(signature, "t1@1000;t2@1000;t1@1100;t2@1100;t1@1200;");
	destroy_plan(&plan);
}

/* Changing participant enumeration order and xl_scn ties leaves the
 * schedule and every verdict unchanged. */
UT_TEST(test_enumeration_and_scn_ties_do_not_change_schedule)
{
	ClusterColdParticipantV1 forward[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdParticipantV1 reverse[2] = { forward[1], forward[0] };
	ObserveTable first = { 0 };
	ObserveTable second = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan_forward = make_plan(forward, 2);
	ClusterColdPlanV1 *plan_reverse = make_plan(reverse, 2);
	char sig_forward[256];
	char sig_reverse[256];

	build_interleaved(plan_forward, 0, 1, 7, 7);
	build_interleaved(plan_reverse, 1, 0, 7, 7);
	interleaved_observations(&first);
	interleaved_observations(&second);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan_forward, observe, &first, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan_reverse, observe, &second, &diag), CLUSTER_COLD_OK);
	schedule_signature(plan_forward, forward, sig_forward, sizeof(sig_forward));
	schedule_signature(plan_reverse, reverse, sig_reverse, sizeof(sig_reverse));
	UT_ASSERT_STR_EQ(sig_forward, sig_reverse);
	destroy_plan(&plan_forward);
	destroy_plan(&plan_reverse);
}

/* Opaque tokens: a numerically decreasing chain is still exact. */
UT_TEST(test_opaque_tokens_have_no_numeric_order)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 first = delta(0, 100, 0, 900, 30);
	ClusterColdComponentV1 second = delta(0, 100, 0, 30, 7);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ClusterColdStepV1 step;

	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 9, 1, &first), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x2000, 1, 1, &second), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 30));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(find_step(plan, 0, 0x1000, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_SKIP);
	UT_ASSERT(find_step(plan, 1, 0x1000, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_I, 30));
	destroy_plan(&plan);
}

UT_TEST(test_missing_ancestor_refused_before_mutation)
{
	ClusterColdParticipantV1 parts[1] = { part(1, 11, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 change = delta(0, 100, 0, 5, 6);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 1);

	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 9, 1, &change), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 3));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_ANCESTOR_MISSING);
	UT_ASSERT(diag.has_page && diag.page.locator.relNumber == 100);
	UT_ASSERT(version_is(&diag.version, INC_I, 3));
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 0);
	destroy_plan(&plan);
}

UT_TEST(test_chain_gap_between_generations_refused)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 a1 = delta(0, 100, 0, 1, 2);
	ClusterColdComponentV1 b1 = delta(0, 100, 0, 4, 5);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);

	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 1, &a1), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x2000, 2, 1, &b1), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 1));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_CHAIN_AMBIGUOUS);
	UT_ASSERT(diag.has_page);
	destroy_plan(&plan);
}

UT_TEST(test_cycle_and_repeated_version_refused)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 forward = delta(0, 100, 0, 1, 2);
	ClusterColdComponentV1 back = delta(0, 100, 0, 2, 1);
	ClusterColdComponentV1 other = delta(0, 200, 0, 3, 2);
	ClusterColdComponentV1 repeat = delta(0, 200, 0, 1, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);

	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 1, &forward), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x2000, 2, 1, &back), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 1));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_EDGE_CYCLE);
	destroy_plan(&plan);

	plan = make_plan(parts, 2);
	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 1, &other), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x2000, 2, 1, &repeat), CLUSTER_COLD_OK);
	observe_set(&table, 200, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 3));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_EDGE_CYCLE);
	destroy_plan(&plan);
}

UT_TEST(test_branch_refused)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 left = delta(0, 100, 0, 1, 2);
	ClusterColdComponentV1 right = delta(0, 100, 0, 1, 3);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);

	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 1, &left), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x2000, 2, 1, &right), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 1));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_EDGE_BRANCH);
	destroy_plan(&plan);
}

UT_TEST(test_wrong_incarnation_refused)
{
	ClusterColdParticipantV1 parts[1] = { part(1, 11, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 change = delta(0, 100, 0, 1, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 1);

	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 1, &change), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_J, 2));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_INCARNATION_MISMATCH);
	destroy_plan(&plan);
}

UT_TEST(test_redo_straddle_and_order_refused)
{
	ClusterColdParticipantV1 parts[1] = { part(1, 11, 0x100, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 1);

	UT_ASSERT_EQ(feed_plain(plan, 0, 0x100, 0xf80), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0xf80, 0x1080), CLUSTER_COLD_SOURCE_GAP);
	destroy_plan(&plan);

	plan = make_plan(parts, 1);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x180, 0x200), CLUSTER_COLD_SOURCE_GAP);
	destroy_plan(&plan);

	plan = make_plan(parts, 1);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x100, 0x400), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x300, 0x500), CLUSTER_COLD_SOURCE_GAP);
	destroy_plan(&plan);
}

UT_TEST(test_incomplete_cut_refused)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x100, 0x100, 0x2000), part(2, 12, 0x100, 0x100, 0x2000) };
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);

	UT_ASSERT_EQ(feed_plain(plan, 0, 0x100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x100, 0x1800), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT(diag.participant == 1);
	destroy_plan(&plan);
}

/* Unreadable DATA is replaced from the last anchor whose suffix is all
 * replayable; history before it is never touched. */
UT_TEST(test_torn_data_repaired_from_last_redo_anchor)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x100, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 h1 = delta(0, 100, 0, 1, 2);
	ClusterColdComponentV1 f2 = fpi(0, 100, 0, 2, 3);
	ClusterColdComponentV1 d3 = delta(0, 100, 0, 3, 4);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ClusterColdStepV1 step;

	UT_ASSERT_EQ(feed(plan, 0, 0x100, 0x200, 1, 1, &h1), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x200, 0x1000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 2, 1, &f2), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x2000, 3, 1, &d3), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_INVALID, ver(0, 0));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(find_step(plan, 0, 0x1000, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_IMAGE);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
	UT_ASSERT(find_step(plan, 1, 0x1000, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_I, 3));
	UT_ASSERT(!find_step(plan, 0, 0x100, NULL, NULL));
	destroy_plan(&plan);
}

UT_TEST(test_torn_data_without_redo_anchor_refused)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x100, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 h1 = fpi(0, 100, 0, 1, 2);
	ClusterColdComponentV1 d2 = delta(0, 100, 0, 2, 3);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);

	UT_ASSERT_EQ(feed(plan, 0, 0x100, 0x1000, 1, 1, &h1), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1000, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x2000, 2, 1, &d2), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_INVALID, ver(0, 0));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_ANCHOR_MISSING);
	destroy_plan(&plan);
}

UT_TEST(test_multi_page_record_mixed_verdicts)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 r[2] = { delta(0, 100, 0, 1, 2), delta(1, 200, 0, 11, 12) };
	ClusterColdComponentV1 later = delta(0, 100, 0, 2, 3);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ClusterColdStepV1 step;

	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 2, r), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x2000, 2, 1, &later), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 3));
	observe_set(&table, 200, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 11));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(find_step(plan, 0, 0x1000, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_SKIP);
	UT_ASSERT_EQ(step.blocks[1].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(step.mixed);
	UT_ASSERT(!step.all_skip);
	UT_ASSERT(find_step(plan, 1, 0x1000, &step, NULL));
	UT_ASSERT(step.all_skip);
	destroy_plan(&plan);
}

UT_TEST(test_cross_generation_deadlock_refused)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 a1 = delta(0, 100, 0, 2, 3);
	ClusterColdComponentV1 a2 = delta(0, 200, 0, 11, 12);
	ClusterColdComponentV1 b1 = delta(0, 200, 0, 12, 13);
	ClusterColdComponentV1 b2 = delta(0, 100, 0, 1, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);

	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x1100, 1, 1, &a1), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 0, 0x1100, 0x2000, 1, 1, &a2), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x1100, 1, 1, &b1), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1100, 0x2000, 1, 1, &b2), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 1));
	observe_set(&table, 200, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 11));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_DEADLOCK);
	UT_ASSERT(diag.has_record && diag.has_dependency);
	UT_ASSERT(diag.participant == 0 && diag.read_rec_ptr == 0x1000);
	UT_ASSERT(diag.dependency_participant == 1 && diag.dependency_read_rec_ptr == 0x1100);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 0);
	destroy_plan(&plan);
}

UT_TEST(test_pure_history_page_not_observed)
{
	ClusterColdParticipantV1 parts[1] = { part(1, 11, 0x100, 0x1000, 0x2000) };
	ClusterColdComponentV1 old = delta(0, 300, 0, 1, 2);
	ClusterColdComponentV1 live = delta(0, 100, 0, 5, 6);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 1);

	UT_ASSERT_EQ(feed(plan, 0, 0x100, 0x1000, 1, 1, &old), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 2, 1, &live), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 5));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(table.calls, 1);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 1);
	destroy_plan(&plan);
}

UT_TEST(test_participant_cut_validation)
{
	ClusterColdParticipantV1 bad_order[1] = { part(1, 11, 0x1000, 0x800, 0x2000) };
	ClusterColdParticipantV1 bad_tail[1] = { part(1, 11, 0x100, 0x3000, 0x2000) };
	ClusterColdParticipantV1 no_incarnation[1] = { part(1, 0, 0x100, 0x100, 0x2000) };
	ClusterColdParticipantV1 no_thread[1] = { part(0, 11, 0x100, 0x100, 0x2000) };
	ClusterColdParticipantV1 repeated[2]
		= { part(2, 11, 0x100, 0x100, 0x2000), part(2, 11, 0x100, 0x100, 0x2000) };
	ClusterColdParticipantV1 generations[2]
		= { part(2, 12, 0x100, 0x100, 0x2000), part(2, 11, 0x100, 0x900, 0x900) };
	ClusterColdPlanV1 *plan = NULL;

	UT_ASSERT_EQ(cluster_cold_plan_create_v1(bad_order, 1, BUDGET, &plan),
				 CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT_NULL(plan);
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(bad_tail, 1, BUDGET, &plan),
				 CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(no_incarnation, 1, BUDGET, &plan),
				 CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(no_thread, 1, BUDGET, &plan),
				 CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(repeated, 2, BUDGET, &plan),
				 CLUSTER_COLD_PARTICIPANT_INVALID);
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(generations, 2, BUDGET, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_NOT_NULL(plan);
	destroy_plan(&plan);
	UT_ASSERT_NULL(plan);
}

/* An old, fully checkpointed generation of the same thread contributes
 * ancestry only; the new generation replays from its own native redo. */
UT_TEST(test_two_generations_of_one_thread_link_by_version)
{
	ClusterColdParticipantV1 parts[2]
		= { part(2, 6, 0x100, 0x100, 0x2000), part(2, 5, 0x100, 0x900, 0x900) };
	ClusterColdComponentV1 old = delta(0, 100, 0, 1, 2);
	ClusterColdComponentV1 live = delta(0, 100, 0, 2, 3);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ClusterColdStepV1 step;

	UT_ASSERT_EQ(feed(plan, 1, 0x100, 0x900, 1, 1, &old), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 0, 0x100, 0x2000, 2, 1, &live), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 2));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 1);
	UT_ASSERT(find_step(plan, 0, 0x100, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	destroy_plan(&plan);
}

UT_TEST(test_observation_failure_refused)
{
	ClusterColdParticipantV1 parts[1] = { part(1, 11, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 change = delta(0, 100, 0, 1, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 1);

	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 1, &change), CLUSTER_COLD_OK);
	table.fail = true;
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_OBSERVATION_FAILED);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_STATE);
	destroy_plan(&plan);
}

UT_TEST(test_component_shape_validation)
{
	ClusterColdParticipantV1 parts[1] = { part(1, 11, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 wrong_new = init_new(0, 100, 0, RF_PAGE_STATE_UNFORMATTED, 5);
	ClusterColdComponentV1 routed = delta(0, 100, 0, 1, 2);
	ClusterColdComponentV1 zero_token = delta(0, 100, 0, 1, 0);
	ClusterColdComponentV1 same_page[2] = { delta(0, 100, 0, 1, 2), delta(1, 100, 0, 3, 4) };
	ClusterColdComponentV1 unanchored_new = init_new(0, 100, 0, RF_PAGE_STATE_UNFORMATTED, 5);
	ClusterColdPlanV1 *plan;

	memset(wrong_new.before.segment_incarnation, INC_J, 16);
	routed.page_class = RF_PAGE_CLASS_ROUTED_SIDE;
	unanchored_new.edge_flags = 0;
	plan = make_plan(parts, 1);
	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 1, &wrong_new), CLUSTER_COLD_COMPONENT_INVALID);
	destroy_plan(&plan);
	plan = make_plan(parts, 1);
	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 1, &routed), CLUSTER_COLD_COMPONENT_INVALID);
	destroy_plan(&plan);
	plan = make_plan(parts, 1);
	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 1, &zero_token), CLUSTER_COLD_COMPONENT_INVALID);
	destroy_plan(&plan);
	plan = make_plan(parts, 1);
	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 2, same_page), CLUSTER_COLD_COMPONENT_INVALID);
	destroy_plan(&plan);
	plan = make_plan(parts, 1);
	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 1, &unanchored_new),
				 CLUSTER_COLD_COMPONENT_INVALID);
	destroy_plan(&plan);
}

UT_TEST(test_new_page_starts_from_unformatted_or_absent)
{
	ClusterColdParticipantV1 parts[1] = { part(1, 11, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 made = init_new(0, 100, 0, RF_PAGE_STATE_UNFORMATTED, 5);
	ClusterColdComponentV1 extended = init_new(1, 200, 0, RF_PAGE_STATE_ABSENT, 5);
	ClusterColdComponentV1 both[2];
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 1);
	ClusterColdStepV1 step;

	both[0] = made;
	both[1] = extended;
	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 2, both), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_UNFORMATTED, ver(INC_I, 0));
	observe_set(&table, 200, 0, CLUSTER_COLD_DATA_ABSENT, ver(0, 0));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(find_step(plan, 0, 0x1000, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_UNFORMATTED);
	UT_ASSERT_EQ(step.blocks[1].verdict, CLUSTER_COLD_BLOCK_APPLY_INIT);
	UT_ASSERT_EQ(step.blocks[1].expected_kind, CLUSTER_COLD_DATA_ABSENT);
	destroy_plan(&plan);

	plan = make_plan(parts, 1);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 1, &made), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_UNFORMATTED, ver(INC_J, 0));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_INCARNATION_MISMATCH);
	destroy_plan(&plan);
}

UT_TEST(test_zero_page_with_redo_anchor_is_repaired)
{
	ClusterColdParticipantV1 parts[1] = { part(1, 11, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 image = fpi(0, 100, 0, 1, 2);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 1);
	ClusterColdStepV1 step;

	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 1, 1, &image), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_UNFORMATTED, ver(INC_I, 0));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(find_step(plan, 0, 0x1000, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_IMAGE);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_INVALID);
	destroy_plan(&plan);
}

UT_TEST(test_memory_budget_enforced)
{
	ClusterColdParticipantV1 parts[1] = { part(1, 11, 0x1000, 0x1000, 0x100000) };
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdDetailV1 detail = CLUSTER_COLD_OK;
	XLogRecPtr at = 0x1000;
	int i;

	UT_ASSERT_EQ(cluster_cold_plan_create_v1(parts, 1, 64, &plan), CLUSTER_COLD_CAPACITY);
	UT_ASSERT_NULL(plan);
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(parts, 1, 4096, &plan), CLUSTER_COLD_OK);
	for (i = 0; i < 1000 && detail == CLUSTER_COLD_OK; i++) {
		ClusterColdComponentV1 change = delta(0, 100, (BlockNumber)i, 1, 2);

		detail = feed(plan, 0, at, at + 0x10, 1, 1, &change);
		at += 0x10;
	}
	UT_ASSERT_EQ(detail, CLUSTER_COLD_CAPACITY);
	UT_ASSERT_EQ(feed_plain(plan, 0, at, at + 0x10), CLUSTER_COLD_STATE);
	destroy_plan(&plan);
}

/* A destructive lifecycle record after the native redo start cannot be
 * ordered against other generations' page records without its typed owner;
 * the same record in retained history is already durable. */
UT_TEST(test_structural_records_refused_after_native_redo)
{
	ClusterColdParticipantV1 parts[1] = { part(1, 11, 0x100, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 1);
	ClusterColdRecordV1 bad;

	UT_ASSERT_EQ(feed_flagged(plan, 0, 0x100, 0x200, CLUSTER_COLD_RECORD_STRUCTURAL),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x200, 0x1000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_flagged(plan, 0, 0x1000, 0x1100, CLUSTER_COLD_RECORD_STRUCTURAL),
				 CLUSTER_COLD_STRUCTURAL_UNSUPPORTED);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_STATE);
	destroy_plan(&plan);

	plan = make_plan(parts, 1);
	UT_ASSERT_EQ(feed_flagged(plan, 0, 0x100, 0x200, CLUSTER_COLD_RECORD_UNSUPPORTED),
				 CLUSTER_COLD_STRUCTURAL_UNSUPPORTED);
	destroy_plan(&plan);

	plan = make_plan(parts, 1);
	memset(&bad, 0, sizeof(bad));
	bad.read_rec_ptr = 0x100;
	bad.end_rec_ptr = 0x200;
	bad.record_flags = 0x80;
	UT_ASSERT_EQ(cluster_cold_plan_feed_v1(plan, 0, &bad), CLUSTER_COLD_INVALID_ARGUMENT);
	destroy_plan(&plan);
}

/* No checksum is required in the shared profile: a torn write can pair a
 * new header with an old body.  Like full_page_writes, the last anchor whose
 * suffix is replayable is always restored once DATA is at or past it. */
UT_TEST(test_torn_header_restores_last_replayable_anchor)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x100, 0x1000, 0x2000), part(2, 12, 0x100, 0x1000, 0x2000) };
	ClusterColdComponentV1 e0 = delta(0, 100, 0, 1, 2);
	ClusterColdComponentV1 e1 = fpi(0, 100, 0, 2, 3);
	ClusterColdComponentV1 e2 = delta(0, 100, 0, 3, 4);
	uint64 positions[3] = { 4, 3, 2 };
	int i;

	for (i = 0; i < 3; i++) {
		ObserveTable table = { 0 };
		ClusterColdDiagV1 diag;
		ClusterColdPlanV1 *plan = make_plan(parts, 2);
		ClusterColdStepV1 step;

		UT_ASSERT_EQ(feed(plan, 1, 0x100, 0x200, 1, 1, &e0), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(feed_plain(plan, 1, 0x200, 0x1000), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(feed(plan, 0, 0x100, 0x1000, 2, 0, NULL), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x2000, 3, 1, &e1), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x2000, 4, 1, &e2), CLUSTER_COLD_OK);
		observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, positions[i]));
		UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
		UT_ASSERT(find_step(plan, 0, 0x1000, &step, NULL));
		UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_IMAGE);
		UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_PRESENT);
		UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_I, positions[i]));
		UT_ASSERT(find_step(plan, 1, 0x1000, &step, NULL));
		UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
		UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_I, 3));
		destroy_plan(&plan);
	}
}

/* A record missing between two fed records breaks the xl_prev chain. */
UT_TEST(test_record_gap_refused)
{
	ClusterColdParticipantV1 parts[1] = { part(1, 11, 0x100, 0x100, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 1);
	ClusterColdRecordV1 record;

	UT_ASSERT_EQ(feed_plain(plan, 0, 0x100, 0x180), CLUSTER_COLD_OK);
	memset(&record, 0, sizeof(record));
	record.read_rec_ptr = 0x200;
	record.end_rec_ptr = 0x280;
	record.prev_rec_ptr = 0x180; /* the record at 0x180 was never fed */
	UT_ASSERT_EQ(cluster_cold_plan_feed_v1(plan, 0, &record), CLUSTER_COLD_SOURCE_GAP);
	destroy_plan(&plan);

	plan = make_plan(parts, 1);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x100, 0x180), CLUSTER_COLD_OK);
	record.prev_rec_ptr = 0x100;
	record.read_rec_ptr = 0x180;
	record.end_rec_ptr = 0x2000;
	UT_ASSERT_EQ(cluster_cold_plan_feed_v1(plan, 0, &record), CLUSTER_COLD_OK);
	destroy_plan(&plan);
}

/* Two replayable anchors: a page whose header names an early version may
 * still be torn, so restoration starts at the earliest replayable anchor at
 * or before DATA's successor, never by applying deltas onto that body. */
UT_TEST(test_earliest_replayable_anchor_precedes_torn_deltas)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1000, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 fpi0 = fpi(0, 100, 0, 1, 2);
	ClusterColdComponentV1 d1 = delta(0, 100, 0, 2, 3);
	ClusterColdComponentV1 d2 = delta(0, 100, 0, 3, 4);
	ClusterColdComponentV1 fpi3 = fpi(0, 100, 0, 4, 5);
	ClusterColdComponentV1 d4 = delta(0, 100, 0, 5, 6);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan = make_plan(parts, 2);
	ClusterColdStepV1 step;

	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x1100, 1, 1, &fpi0), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x1100, 2, 1, &d1), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 0, 0x1100, 0x1200, 3, 1, &d2), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1100, 0x1200, 4, 1, &fpi3), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 0, 0x1200, 0x2000, 5, 1, &d4), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1200, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 3)); /* d1's result */
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(find_step(plan, 0, 0x1000, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_IMAGE);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_PRESENT);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_I, 3));
	UT_ASSERT(find_step(plan, 1, 0x1000, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_I, 2));
	UT_ASSERT(find_step(plan, 0, 0x1100, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(find_step(plan, 1, 0x1100, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_IMAGE);
	destroy_plan(&plan);

	/* DATA past the later anchor's predecessor: the earliest anchor still
	 * qualifies and is restored first. */
	plan = make_plan(parts, 2);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x1100, 1, 1, &fpi0), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x1100, 2, 1, &d1), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 0, 0x1100, 0x1200, 3, 1, &d2), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1100, 0x1200, 4, 1, &fpi3), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 0, 0x1200, 0x2000, 5, 1, &d4), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 1, 0x1200, 0x2000), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 6));
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
	UT_ASSERT(find_step(plan, 0, 0x1000, &step, NULL));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_IMAGE);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_I, 6));
	destroy_plan(&plan);
}

/* A side effect without a cold owner is refused after the native redo
 * start; in retained history it is already durable. */
UT_TEST(test_unowned_side_records_refused_after_native_redo)
{
	ClusterColdParticipantV1 parts[1] = { part(2, 12, 0x100, 0x1000, 0x2000) };
	ClusterColdPlanV1 *plan = make_plan(parts, 1);

	UT_ASSERT_EQ(feed_flagged(plan, 0, 0x100, 0x1000, CLUSTER_COLD_RECORD_SIDE_UNOWNED),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_flagged(plan, 0, 0x1000, 0x1100, CLUSTER_COLD_RECORD_SIDE_UNOWNED),
				 CLUSTER_COLD_SIDE_OWNER_MISSING);
	destroy_plan(&plan);
}

/*
 * Without verified content only the page header placed DATA, and a torn
 * write may have left that header over another version's body.  With no
 * anchor after the last history change nothing can rebuild the page, so
 * the plan refuses.  With a later anchor the unverified content is never a
 * delta base (the changes before the anchor are skipped); verified content
 * is used as the base as before.
 */
UT_TEST(test_unverified_content_needs_an_anchor)
{
	ClusterColdParticipantV1 parts[2]
		= { part(1, 11, 0x1000, 0x1100, 0x2000), part(2, 12, 0x1000, 0x1000, 0x2000) };
	ClusterColdComponentV1 h = fpi(0, 100, 0, 1, 2);
	ClusterColdComponentV1 d1 = delta(0, 100, 0, 2, 3);
	ClusterColdComponentV1 d2 = delta(0, 100, 0, 3, 4);
	ClusterColdComponentV1 f2 = fpi(0, 100, 0, 3, 4);
	ObserveTable table = { 0 };
	ClusterColdDiagV1 diag;
	ClusterColdPlanV1 *plan;
	ClusterColdStepV1 step;
	int pass;

	for (pass = 0; pass < 3; pass++) {
		plan = make_plan(parts, 2);
		memset(&table, 0, sizeof(table));
		UT_ASSERT_EQ(feed(plan, 0, 0x1000, 0x1100, 1, 1, &h), CLUSTER_COLD_OK); /* history */
		UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x1100, 2, 1, &d1), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(feed(plan, 1, 0x1100, 0x2000, 3, 1, pass == 2 ? &f2 : &d2), CLUSTER_COLD_OK);
		observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 2));
		if (pass != 1)
			table.data[0].flags = 0;
		if (pass == 0) {
			UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
						 CLUSTER_COLD_CONTENT_UNPROVEN);
			UT_ASSERT_EQ(diag.detail, CLUSTER_COLD_CONTENT_UNPROVEN);
			UT_ASSERT(diag.has_page);
			UT_ASSERT(version_is(&diag.version, INC_I, 2));
			UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 0);
		} else {
			UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag), CLUSTER_COLD_OK);
			/* Unverified content is never a delta base: skip to the anchor. */
			UT_ASSERT(find_step(plan, 1, 0x1000, &step, NULL));
			UT_ASSERT_EQ(step.blocks[0].verdict,
						 pass == 2 ? CLUSTER_COLD_BLOCK_SKIP : CLUSTER_COLD_BLOCK_APPLY_DELTA);
			UT_ASSERT(find_step(plan, 1, 0x1100, &step, NULL));
			UT_ASSERT_EQ(step.blocks[0].verdict, pass == 2 ? CLUSTER_COLD_BLOCK_APPLY_IMAGE
														   : CLUSTER_COLD_BLOCK_APPLY_DELTA);
			if (pass == 2) {
				UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_PRESENT);
				UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_I, 2));
			}
		}
		destroy_plan(&plan);
	}

	/* The flag is part of the observation contract. */
	plan = make_plan(parts, 2);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1000, 0x1100), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x2000, 2, 1, &d1), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_INVALID, ver(0, 0));
	table.data[0].flags = CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED;
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_OBSERVATION_FAILED);
	destroy_plan(&plan);
	plan = make_plan(parts, 2);
	memset(&table, 0, sizeof(table));
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1000, 0x1100), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed_plain(plan, 0, 0x1100, 0x2000), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(feed(plan, 1, 0x1000, 0x2000, 2, 1, &d1), CLUSTER_COLD_OK);
	observe_set(&table, 100, 0, CLUSTER_COLD_DATA_PRESENT, ver(INC_I, 2));
	table.data[0].flags |= 0x02;
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, &table, &diag),
				 CLUSTER_COLD_OBSERVATION_FAILED);
	destroy_plan(&plan);
}

/* Every page's chain starts at token block * 100 + 1 (see capacity test). */
static bool
observe_chain_start(void *arg, const RfPageIdentityV1 *page, ClusterColdDataV1 *out)
{
	(void)arg;
	memset(out, 0, sizeof(*out));
	out->kind = CLUSTER_COLD_DATA_PRESENT;
	out->flags = CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED;
	out->version = ver(INC_I, (uint64)page->blockno * 100 + 1);
	return true;
}

/*
 * Storage is compact enough for production windows: 25,000 single-block
 * records over 2,500 pages fit a 4 MiB budget through seal (about 100
 * bytes a record including seal scratch), where per-record page identity,
 * versions and expected state took about 256.
 */
UT_TEST(test_compact_plan_capacity)
{
	ClusterColdParticipantV1 parts[1] = { part(1, 11, 0x1000, 0x1000, 0x1000 + 25000 * 0x10) };
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdDiagV1 diag;
	ClusterColdDetailV1 detail = CLUSTER_COLD_OK;
	ClusterColdStepV1 step;
	XLogRecPtr at = 0x1000;
	uint32 i;

	UT_ASSERT_EQ(cluster_cold_plan_create_v1(parts, 1, BUDGET, &plan), CLUSTER_COLD_OK);
	for (i = 0; i < 25000 && detail == CLUSTER_COLD_OK; i++) {
		BlockNumber block = i % 2500;
		uint64 base = (uint64)block * 100 + 1 + i / 2500;
		ClusterColdComponentV1 change = delta(0, 100, block, base, base + 1);

		detail = feed(plan, 0, at, at + 0x10, i + 1, 1, &change);
		at += 0x10;
	}
	UT_ASSERT_EQ(detail, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe_chain_start, NULL, &diag),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 25000);
	UT_ASSERT(cluster_cold_plan_step_v1(plan, 24999, &step));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT(version_is(&step.blocks[0].expected_before, INC_I, 2499 * 100 + 10));
	UT_ASSERT(version_is(&step.blocks[0].result, INC_I, 2499 * 100 + 11));
	destroy_plan(&plan);
}

int
main(void)
{
	UT_PLAN(30);
	UT_RUN(test_lower_lag_old_fpi_cannot_overwrite_newer_durable);
	UT_RUN(test_data_behind_history_is_refused);
	UT_RUN(test_a_b_c_exact_order_ignores_scn_and_lsn);
	UT_RUN(test_interleaved_writers_follow_both_page_chains);
	UT_RUN(test_enumeration_and_scn_ties_do_not_change_schedule);
	UT_RUN(test_opaque_tokens_have_no_numeric_order);
	UT_RUN(test_missing_ancestor_refused_before_mutation);
	UT_RUN(test_chain_gap_between_generations_refused);
	UT_RUN(test_cycle_and_repeated_version_refused);
	UT_RUN(test_branch_refused);
	UT_RUN(test_wrong_incarnation_refused);
	UT_RUN(test_redo_straddle_and_order_refused);
	UT_RUN(test_incomplete_cut_refused);
	UT_RUN(test_torn_data_repaired_from_last_redo_anchor);
	UT_RUN(test_torn_data_without_redo_anchor_refused);
	UT_RUN(test_multi_page_record_mixed_verdicts);
	UT_RUN(test_cross_generation_deadlock_refused);
	UT_RUN(test_pure_history_page_not_observed);
	UT_RUN(test_participant_cut_validation);
	UT_RUN(test_two_generations_of_one_thread_link_by_version);
	UT_RUN(test_observation_failure_refused);
	UT_RUN(test_component_shape_validation);
	UT_RUN(test_new_page_starts_from_unformatted_or_absent);
	UT_RUN(test_zero_page_with_redo_anchor_is_repaired);
	UT_RUN(test_memory_budget_enforced);
	UT_RUN(test_structural_records_refused_after_native_redo);
	UT_RUN(test_torn_header_restores_last_replayable_anchor);
	UT_RUN(test_record_gap_refused);
	UT_RUN(test_earliest_replayable_anchor_precedes_torn_deltas);
	UT_RUN(test_unowned_side_records_refused_after_native_redo);
	UT_RUN(test_unverified_content_needs_an_anchor);
	UT_RUN(test_compact_plan_capacity);
	UT_DONE();
	return ut_failed_count != 0;
}
