/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_replay.c
 *	  Typed cold replay pass 2: the sequencer that drives every retained
 *	  writer generation through a sealed schedule.
 *
 *	  Pure inputs only: a sealed plan built from fixtures and in-memory
 *	  record streams standing in for the pass-2 readers.  The callbacks only
 *	  record what the sequencer asked for; nothing is replayed.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_replay.c
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

#define INC 7
#define BUDGET (1024 * 1024)
#define MAX_STREAM 8
#define MAX_TRACE 32

/* One fixture record: a stream entry and, when it has one, its component. */
typedef struct FixRecord {
	XLogRecPtr read;
	XLogRecPtr end;
	uint64 scn;
	bool page;
	ClusterColdComponentV1 component;
	bool space;
	ClusterColdSpaceOpV1 op;
} FixRecord;

typedef struct Stream {
	int count;
	int next;
	ClusterColdReplayRecordV1 record[MAX_STREAM];
} Stream;

typedef enum TraceKind { T_UNSCHEDULED, T_SCHEDULED, T_SPACE_FINAL } TraceKind;

typedef struct TraceEvent {
	TraceKind kind;
	uint32 participant;
	XLogRecPtr read;
	ClusterColdPageActionV1 action;
} TraceEvent;

typedef struct Harness {
	Stream stream[2];
	XLogRecPtr current[2];
	int events;
	TraceEvent trace[MAX_TRACE];
	bool refuse_unscheduled;
	bool refuse_space_final;
} Harness;

static RfPageVersionV1
ver(uint64 token)
{
	RfPageVersionV1 version;

	memset(&version, 0, sizeof(version));
	memset(version.segment_incarnation, INC, sizeof(version.segment_incarnation));
	version.mutation_token = token;
	return version;
}

static ClusterColdComponentV1
component(BlockNumber block, uint64 before, uint64 result, bool image)
{
	ClusterColdComponentV1 c;

	memset(&c, 0, sizeof(c));
	c.page.system_identifier = 99;
	memset(c.page.storage_uuid, 3, sizeof(c.page.storage_uuid));
	c.page.locator.spcOid = 1663;
	c.page.locator.dbOid = 5;
	c.page.locator.relNumber = 100;
	c.page.forknum = MAIN_FORKNUM;
	c.page.blockno = block;
	c.page_class = RF_PAGE_CLASS_ORDINARY;
	c.before_kind = RF_PAGE_STATE_PRESENT;
	c.result_kind = RF_PAGE_STATE_PRESENT;
	c.before = ver(before);
	c.result = ver(result);
	if (image)
		c.edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
	return c;
}

static FixRecord
plain(XLogRecPtr read, XLogRecPtr end)
{
	FixRecord r;

	memset(&r, 0, sizeof(r));
	r.read = read;
	r.end = end;
	r.scn = 1;
	return r;
}

static FixRecord
paged(XLogRecPtr read, XLogRecPtr end, uint64 scn, ClusterColdComponentV1 c)
{
	FixRecord r = plain(read, end);

	r.scn = scn;
	r.page = true;
	r.component = c;
	return r;
}

static const char space_payload[4] = { 'S', 'P', 'C', '1' };

static FixRecord
spaced(XLogRecPtr read, XLogRecPtr end, uint8 kind, Oid rel)
{
	FixRecord r = plain(read, end);

	r.space = true;
	r.op.kind = kind;
	r.op.locator.spcOid = 1663;
	r.op.locator.dbOid = 5;
	r.op.locator.relNumber = rel;
	memset(r.op.result_incarnation, INC + 1, sizeof(r.op.result_incarnation));
	r.op.payload = space_payload;
	r.op.payload_length = sizeof(space_payload);
	return r;
}

static bool
check_feed_order(void *arg, const RelFileLocator *locator, const ClusterColdSpaceInputV1 *inputs,
				 uint32 count, uint32 *order)
{
	uint32 i;

	(void)arg;
	(void)locator;
	(void)inputs;
	for (i = 0; i < count; i++)
		order[i] = i;
	return true;
}

static ClusterColdReplayRecordV1
identity(const FixRecord *r)
{
	ClusterColdReplayRecordV1 id;

	memset(&id, 0, sizeof(id));
	id.read_rec_ptr = r->read;
	id.end_rec_ptr = r->end;
	id.record_crc = (uint32)(r->read ^ r->end);
	id.rmid = RM_HEAP_ID;
	return id;
}

/* Page 0: data token; page 1: data token.  Both verified content. */
static uint64 data_token[2];

static bool
observe(void *arg, const RfPageIdentityV1 *page, ClusterColdDataV1 *out)
{
	(void)arg;
	memset(out, 0, sizeof(*out));
	if (page->blockno > 1)
		return false;
	out->kind = CLUSTER_COLD_DATA_PRESENT;
	out->flags = CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED;
	out->version = ver(data_token[page->blockno]);
	return true;
}

/*
 * Two generations.  Thread 1 (own): plain, page 0 image (1->2), plain,
 * page 1 delta (5->6).  Thread 2: page 0 delta (2->3), plain.  The page 0
 * chain makes thread 2's record depend on thread 1's image.
 */
static ClusterColdParticipantV1 cuts[2];
static FixRecord fix[2][4];
static int fix_count[2];

static void
fixture(bool deltas_only)
{
	memset(cuts, 0, sizeof(cuts));
	cuts[0].thread_id = 1;
	cuts[0].timeline = 1;
	cuts[0].owner_incarnation = 11;
	cuts[0].physical_lower = 0x1000;
	cuts[0].native_redo = 0x1000;
	cuts[0].tail_end = 0x1500;
	cuts[1] = cuts[0];
	cuts[1].thread_id = 2;
	cuts[1].owner_incarnation = 12;
	cuts[1].tail_end = 0x1300;
	fix[0][0] = plain(0x1000, 0x1100);
	fix[0][1] = paged(0x1100, 0x1200, 2, component(0, 1, 2, !deltas_only));
	fix[0][2] = plain(0x1200, 0x1300);
	fix[0][3] = paged(0x1300, 0x1500, 4, component(1, 5, 6, false));
	fix[1][0] = paged(0x1000, 0x1100, 3, component(0, 2, 3, false));
	fix[1][1] = plain(0x1100, 0x1300);
	fix_count[0] = 4;
	fix_count[1] = 2;
	data_token[0] = 1;
	data_token[1] = 5;
}

static ClusterColdPlanV1 *
seal(void)
{
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdDiagV1 diag;
	uint32 p;

	UT_ASSERT_EQ(cluster_cold_plan_create_v1(cuts, 2, BUDGET, &plan), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_set_space_check_v1(plan, check_feed_order, NULL),
				 CLUSTER_COLD_OK);
	for (p = 0; p < 2; p++) {
		XLogRecPtr prev = cuts[p].physical_lower - 0x40;
		int i;

		for (i = 0; i < fix_count[p]; i++) {
			ClusterColdRecordV1 record;
			ClusterColdReplayRecordV1 id = identity(&fix[p][i]);

			memset(&record, 0, sizeof(record));
			record.read_rec_ptr = id.read_rec_ptr;
			record.end_rec_ptr = id.end_rec_ptr;
			record.prev_rec_ptr = prev;
			record.scn = fix[p][i].scn;
			record.record_crc = id.record_crc;
			record.rmid = id.rmid;
			record.component_count = fix[p][i].page ? 1 : 0;
			record.components = fix[p][i].page ? &fix[p][i].component : NULL;
			record.space_count = fix[p][i].space ? 1 : 0;
			record.space_ops = fix[p][i].space ? &fix[p][i].op : NULL;
			UT_ASSERT_EQ(cluster_cold_plan_feed_v1(plan, p, &record), CLUSTER_COLD_OK);
			prev = id.read_rec_ptr;
		}
	}
	UT_ASSERT_EQ(cluster_cold_plan_seal_v1(plan, observe, NULL, &diag), CLUSTER_COLD_OK);
	return plan;
}

static void
harness_init(Harness *h)
{
	uint32 p;

	memset(h, 0, sizeof(*h));
	for (p = 0; p < 2; p++) {
		int i;

		for (i = 0; i < fix_count[p]; i++)
			h->stream[p].record[h->stream[p].count++] = identity(&fix[p][i]);
	}
}

static bool
op_next(void *arg, uint32 participant, ClusterColdReplayRecordV1 *out)
{
	Harness *h = (Harness *)arg;
	Stream *s = &h->stream[participant];

	if (s->next >= s->count)
		return false;
	*out = s->record[s->next++];
	h->current[participant] = out->read_rec_ptr;
	return true;
}

static bool
op_unscheduled(void *arg, uint32 participant)
{
	Harness *h = (Harness *)arg;
	TraceEvent *e = &h->trace[h->events++];

	e->kind = T_UNSCHEDULED;
	e->participant = participant;
	e->read = h->current[participant];
	return !h->refuse_unscheduled;
}

static bool
op_scheduled(void *arg, uint32 participant, const ClusterColdStepV1 *step,
			 ClusterColdPageActionV1 action)
{
	Harness *h = (Harness *)arg;
	TraceEvent *e = &h->trace[h->events++];

	e->kind = T_SCHEDULED;
	e->participant = participant;
	e->read = step->read_rec_ptr;
	e->action = action;
	return h->current[participant] == step->read_rec_ptr;
}

static bool
op_space_final(void *arg, uint32 relation)
{
	Harness *h = (Harness *)arg;
	TraceEvent *e = &h->trace[h->events++];

	e->kind = T_SPACE_FINAL;
	e->participant = relation;
	return !h->refuse_space_final;
}

static const ClusterColdReplayOpsV1 ops = { op_next, op_unscheduled, op_scheduled, op_space_final };

static bool
event_is(const Harness *h, int i, TraceKind kind, uint32 participant, XLogRecPtr read)
{
	return i < h->events && h->trace[i].kind == kind && h->trace[i].participant == participant
		   && h->trace[i].read == read;
}

static ClusterColdReplayDetailV1
run(ClusterColdPlanV1 *plan, Harness *h, ClusterColdReplayResultV1 *result)
{
	return cluster_cold_replay_run_v1(plan, cuts, 2, 0, &ops, h, result);
}

/*
 * Records before each scheduled record replay in stream order, the schedule
 * order crosses generations by dependency, and every stream is drained to
 * its validated tail with exactly its pass-1 record count.
 */
UT_TEST(test_replay_follows_schedule_and_drains)
{
	ClusterColdPlanV1 *plan;
	ClusterColdReplayResultV1 result;
	Harness h;

	fixture(false);
	plan = seal();
	harness_init(&h);
	UT_ASSERT_EQ(run(plan, &h, &result), CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT_EQ(result.detail, CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT_EQ(h.events, 6);
	UT_ASSERT(event_is(&h, 0, T_UNSCHEDULED, 0, 0x1000));
	UT_ASSERT(event_is(&h, 1, T_SCHEDULED, 0, 0x1100));
	UT_ASSERT(event_is(&h, 2, T_SCHEDULED, 1, 0x1000));
	UT_ASSERT(event_is(&h, 3, T_UNSCHEDULED, 0, 0x1200));
	UT_ASSERT(event_is(&h, 4, T_SCHEDULED, 0, 0x1300));
	UT_ASSERT(event_is(&h, 5, T_UNSCHEDULED, 1, 0x1100));
	UT_ASSERT_EQ(h.trace[1].action, CLUSTER_COLD_PAGE_APPLY);
	UT_ASSERT_EQ(result.steps_done, 3);
	UT_ASSERT_EQ(result.pages_applied, 3);
	UT_ASSERT_EQ(result.pages_skipped, 0);
	UT_ASSERT_EQ(result.own_read, 0x1300);
	UT_ASSERT_EQ(result.own_end, 0x1500);
	cluster_cold_plan_destroy_v1(&plan);
}

/* Fully skipped records: own ones still advance nextXid, foreign ones not. */
UT_TEST(test_replay_page_actions)
{
	ClusterColdPlanV1 *plan;
	ClusterColdReplayResultV1 result;
	Harness h;

	fixture(true);
	data_token[0] = 3; /* page 0 already holds thread 2's result */
	data_token[1] = 6; /* page 1 already holds thread 1's result */
	plan = seal();
	harness_init(&h);
	UT_ASSERT_EQ(run(plan, &h, &result), CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT(event_is(&h, 1, T_SCHEDULED, 0, 0x1100));
	UT_ASSERT_EQ(h.trace[1].action, CLUSTER_COLD_PAGE_SKIP_ADVANCE_XID);
	UT_ASSERT(event_is(&h, 2, T_SCHEDULED, 1, 0x1000));
	UT_ASSERT_EQ(h.trace[2].action, CLUSTER_COLD_PAGE_SKIP);
	UT_ASSERT_EQ(result.pages_skipped, 3);
	UT_ASSERT_EQ(result.pages_applied, 0);
	cluster_cold_plan_destroy_v1(&plan);
}

/* A scheduled record that is missing or changed stops replay at it. */
UT_TEST(test_replay_scheduled_record_must_match)
{
	ClusterColdPlanV1 *plan;
	ClusterColdReplayResultV1 result;
	Harness h;

	fixture(false);
	plan = seal();

	/* The stream skips the scheduled record: its start is passed. */
	harness_init(&h);
	h.stream[0].record[1] = h.stream[0].record[2];
	h.stream[0].record[2] = h.stream[0].record[3];
	h.stream[0].count = 3;
	UT_ASSERT_EQ(run(plan, &h, &result), CLUSTER_COLD_REPLAY_TARGET_PASSED);
	UT_ASSERT_EQ(result.participant, 0);
	UT_ASSERT_EQ(result.rec_ptr, 0x1100);
	UT_ASSERT_EQ(h.events, 1); /* only the plain record before it */

	/* The stream ends before it. */
	harness_init(&h);
	h.stream[0].count = 1;
	UT_ASSERT_EQ(run(plan, &h, &result), CLUSTER_COLD_REPLAY_SOURCE_ENDED);
	UT_ASSERT_EQ(result.rec_ptr, 0x1100);

	/* Same start, another record. */
	harness_init(&h);
	h.stream[0].record[1].record_crc ^= 1;
	UT_ASSERT_EQ(run(plan, &h, &result), CLUSTER_COLD_REPLAY_IDENTITY);
	UT_ASSERT_EQ(result.rec_ptr, 0x1100);
	UT_ASSERT_EQ(h.events, 1);
	harness_init(&h);
	h.stream[1].record[0].info = 0x10;
	UT_ASSERT_EQ(run(plan, &h, &result), CLUSTER_COLD_REPLAY_IDENTITY);
	UT_ASSERT_EQ(result.participant, 1);
	harness_init(&h);
	h.stream[0].record[3].end_rec_ptr = 0x1400;
	UT_ASSERT_EQ(run(plan, &h, &result), CLUSTER_COLD_REPLAY_IDENTITY);
	cluster_cold_plan_destroy_v1(&plan);
}

/* Draining proves the cut: nothing past the tail, same end, same count. */
UT_TEST(test_replay_drain_proves_the_cut)
{
	ClusterColdPlanV1 *plan;
	ClusterColdReplayResultV1 result;
	Harness h;

	fixture(false);
	plan = seal();

	harness_init(&h);
	h.stream[0].record[4].read_rec_ptr = 0x1500;
	h.stream[0].record[4].end_rec_ptr = 0x1600;
	h.stream[0].count = 5;
	UT_ASSERT_EQ(run(plan, &h, &result), CLUSTER_COLD_REPLAY_PAST_TAIL);
	UT_ASSERT_EQ(result.participant, 0);
	UT_ASSERT_EQ(result.rec_ptr, 0x1500);

	/* Thread 2 ends early. */
	harness_init(&h);
	h.stream[1].count = 1;
	UT_ASSERT_EQ(run(plan, &h, &result), CLUSTER_COLD_REPLAY_CUT_DIFFERS);
	UT_ASSERT_EQ(result.participant, 1);
	UT_ASSERT_EQ(result.rec_ptr, 0x1100);

	/* Same tail, one more record than pass 1 saw. */
	harness_init(&h);
	h.stream[1].record[1].end_rec_ptr = 0x1200;
	h.stream[1].record[2].read_rec_ptr = 0x1200;
	h.stream[1].record[2].end_rec_ptr = 0x1300;
	h.stream[1].count = 3;
	UT_ASSERT_EQ(run(plan, &h, &result), CLUSTER_COLD_REPLAY_CUT_DIFFERS);
	UT_ASSERT_EQ(result.participant, 1);
	cluster_cold_plan_destroy_v1(&plan);
}

/* A callback refusal stops replay at that record. */
UT_TEST(test_replay_callback_refusal_stops)
{
	ClusterColdPlanV1 *plan;
	ClusterColdReplayResultV1 result;
	Harness h;

	fixture(false);
	plan = seal();
	harness_init(&h);
	h.refuse_unscheduled = true;
	UT_ASSERT_EQ(run(plan, &h, &result), CLUSTER_COLD_REPLAY_CALLBACK);
	UT_ASSERT_EQ(result.rec_ptr, 0x1000);
	UT_ASSERT_EQ(h.events, 1);
	cluster_cold_plan_destroy_v1(&plan);
}

/*
 * A CREATE is a SPACE step in schedule order; a reservation advance replays
 * as an unscheduled record; after every stream is drained each SPACE
 * relation is installed in full, in relation order.
 */
static void
space_fixture(void)
{
	fixture(false);
	fix[0][0] = plain(0x1000, 0x1100);
	fix[0][1] = spaced(0x1100, 0x1200, CLUSTER_COLD_SPACE_ADVANCE, 100);
	fix[0][2] = plain(0x1200, 0x1500);
	fix[1][0] = spaced(0x1000, 0x1100, CLUSTER_COLD_SPACE_CREATE, 200);
	fix[1][1] = plain(0x1100, 0x1300);
	fix_count[0] = 3;
	fix_count[1] = 2;
}

UT_TEST(test_replay_space_steps_and_final_installs)
{
	ClusterColdPlanV1 *plan;
	ClusterColdReplayResultV1 result;
	ClusterColdReplayOpsV1 partial = ops;
	Harness h;

	space_fixture();
	plan = seal();
	harness_init(&h);
	UT_ASSERT_EQ(run(plan, &h, &result), CLUSTER_COLD_REPLAY_OK);
	UT_ASSERT_EQ(h.events, 7);
	UT_ASSERT(event_is(&h, 0, T_SCHEDULED, 1, 0x1000));
	UT_ASSERT_EQ(h.trace[0].action, CLUSTER_COLD_SPACE_STEP);
	UT_ASSERT(event_is(&h, 1, T_UNSCHEDULED, 0, 0x1000));
	UT_ASSERT(event_is(&h, 2, T_UNSCHEDULED, 0, 0x1100));
	UT_ASSERT(event_is(&h, 3, T_UNSCHEDULED, 0, 0x1200));
	UT_ASSERT(event_is(&h, 4, T_UNSCHEDULED, 1, 0x1100));
	UT_ASSERT_EQ(h.trace[5].kind, T_SPACE_FINAL);
	UT_ASSERT_EQ(h.trace[5].participant, 0);
	UT_ASSERT_EQ(h.trace[6].kind, T_SPACE_FINAL);
	UT_ASSERT_EQ(h.trace[6].participant, 1);
	UT_ASSERT_EQ(result.steps_done, 1);
	UT_ASSERT_EQ(result.space_steps, 1);
	UT_ASSERT_EQ(result.pages_applied, 0);
	UT_ASSERT_EQ(result.space_relations_finished, 2);

	/* A refused final install stops replay; no owner for it is refused. */
	harness_init(&h);
	h.refuse_space_final = true;
	UT_ASSERT_EQ(run(plan, &h, &result), CLUSTER_COLD_REPLAY_CALLBACK);
	UT_ASSERT_EQ(result.space_relations_finished, 0);
	UT_ASSERT_EQ(h.events, 6);
	harness_init(&h);
	partial.space_final = NULL;
	UT_ASSERT_EQ(cluster_cold_replay_run_v1(plan, cuts, 2, 0, &partial, &h, &result),
				 CLUSTER_COLD_REPLAY_INVALID_ARGUMENT);
	UT_ASSERT_EQ(h.events, 0);
	cluster_cold_plan_destroy_v1(&plan);

	/* Without SPACE relations no final install is needed. */
	fixture(false);
	plan = seal();
	harness_init(&h);
	UT_ASSERT_EQ(cluster_cold_replay_run_v1(plan, cuts, 2, 0, &partial, &h, &result),
				 CLUSTER_COLD_REPLAY_OK);
	cluster_cold_plan_destroy_v1(&plan);
}

UT_TEST(test_replay_argument_checks)
{
	ClusterColdPlanV1 *plan;
	ClusterColdPlanV1 *unsealed = NULL;
	ClusterColdReplayResultV1 result;
	ClusterColdReplayOpsV1 partial = ops;
	Harness h;

	fixture(false);
	plan = seal();
	harness_init(&h);
	UT_ASSERT_EQ(cluster_cold_replay_run_v1(plan, cuts, 1, 0, &ops, &h, &result),
				 CLUSTER_COLD_REPLAY_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_cold_replay_run_v1(plan, cuts, 2, 2, &ops, &h, &result),
				 CLUSTER_COLD_REPLAY_INVALID_ARGUMENT);
	partial.scheduled = NULL;
	UT_ASSERT_EQ(cluster_cold_replay_run_v1(plan, cuts, 2, 0, &partial, &h, &result),
				 CLUSTER_COLD_REPLAY_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_cold_plan_create_v1(cuts, 2, BUDGET, &unsealed), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_replay_run_v1(unsealed, cuts, 2, 0, &ops, &h, &result),
				 CLUSTER_COLD_REPLAY_INVALID_ARGUMENT);
	UT_ASSERT_EQ(h.events, 0);
	cluster_cold_plan_destroy_v1(&unsealed);
	cluster_cold_plan_destroy_v1(&plan);
}

/* A skipped record of the founder's own thread still moves nextXid past its
 * xid, as replaying it would have; another generation's xid is not ours. */
UT_TEST(test_skipped_own_record_advances_next_xid)
{
	ClusterColdStepV1 step;

	memset(&step, 0, sizeof(step));
	step.all_skip = true;
	UT_ASSERT_EQ(cluster_cold_page_action_v1(&step, true), CLUSTER_COLD_PAGE_SKIP_ADVANCE_XID);
	UT_ASSERT_EQ(cluster_cold_page_action_v1(&step, false), CLUSTER_COLD_PAGE_SKIP);
	step.all_skip = false;
	UT_ASSERT_EQ(cluster_cold_page_action_v1(&step, true), CLUSTER_COLD_PAGE_APPLY);
	UT_ASSERT_EQ(cluster_cold_page_action_v1(&step, false), CLUSTER_COLD_PAGE_APPLY);
	step.step_kind = CLUSTER_COLD_STEP_SPACE;
	UT_ASSERT_EQ(cluster_cold_page_action_v1(&step, true), CLUSTER_COLD_SPACE_STEP);
}

/*
 * A record without a scheduled step: native redo, unless it carries SPACE
 * effects (installed by the SPACE owner, or covered by the SPACE pages on
 * disk, so never replayed natively); page versions, refused flags and a
 * commit's drops (always a step) mean pass 1 saw other input.
 */
UT_TEST(test_unscheduled_record_handling)
{
	ClusterColdRecordV1 record;
	ClusterColdComponentV1 c = component(0, 1, 2, false);
	ClusterColdSpaceOpV1 ops[2];
	uint8 kinds[3]
		= { CLUSTER_COLD_SPACE_CREATE, CLUSTER_COLD_SPACE_TRUNCATE, CLUSTER_COLD_SPACE_ADVANCE };
	int i;

	memset(&record, 0, sizeof(record));
	memset(ops, 0, sizeof(ops));
	UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&record), CLUSTER_COLD_UNSCHEDULED_NATIVE);
	record.space_count = 1;
	record.space_ops = ops;
	for (i = 0; i < 3; i++) {
		ops[0].kind = kinds[i];
		UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&record), CLUSTER_COLD_UNSCHEDULED_SPACE_SKIP);
	}
	ops[0].kind = CLUSTER_COLD_SPACE_ADVANCE;
	ops[1].kind = CLUSTER_COLD_SPACE_DROP;
	record.space_count = 2;
	UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&record), CLUSTER_COLD_UNSCHEDULED_REFUSE);
	record.space_count = 0;
	record.space_ops = NULL;
	record.component_count = 1;
	record.components = &c;
	UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&record), CLUSTER_COLD_UNSCHEDULED_REFUSE);
	record.component_count = 0;
	record.components = NULL;
	record.record_flags = CLUSTER_COLD_RECORD_SIDE_UNOWNED;
	UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&record), CLUSTER_COLD_UNSCHEDULED_REFUSE);
	record.record_flags = CLUSTER_COLD_RECORD_STRUCTURAL;
	UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&record), CLUSTER_COLD_UNSCHEDULED_REFUSE);
	record.record_flags = CLUSTER_COLD_RECORD_UNSUPPORTED;
	UT_ASSERT_EQ(cluster_cold_unscheduled_v1(&record), CLUSTER_COLD_UNSCHEDULED_REFUSE);
	UT_ASSERT_EQ(cluster_cold_unscheduled_v1(NULL), CLUSTER_COLD_UNSCHEDULED_REFUSE);
}

int
main(void)
{
	UT_RUN(test_replay_follows_schedule_and_drains);
	UT_RUN(test_replay_page_actions);
	UT_RUN(test_replay_scheduled_record_must_match);
	UT_RUN(test_replay_drain_proves_the_cut);
	UT_RUN(test_replay_callback_refusal_stops);
	UT_RUN(test_replay_space_steps_and_final_installs);
	UT_RUN(test_unscheduled_record_handling);
	UT_RUN(test_replay_argument_checks);
	UT_RUN(test_skipped_own_record_advances_next_xid);
	UT_DONE();
	return ut_failed_count != 0;
}
