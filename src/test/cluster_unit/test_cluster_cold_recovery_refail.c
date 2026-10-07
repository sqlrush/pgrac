/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_refail.c
 *	  Typed cold-crash replay that fails again: a sealed schedule is replayed
 *	  up to an arbitrary step, an arbitrary subset of pages reaches DATA
 *	  (some torn), and planning again from that DATA must converge to the
 *	  same final pages without applying any block to a page state other
 *	  than the one the schedule expects.
 *
 *	  Pure inputs only: multi-generation PageVersion histories are generated
 *	  from a seeded timeline and a page simulator enforces every block
 *	  expectation.  No WAL is read and no page is written.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_refail.c
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
#define BUDGET (4 * 1024 * 1024)
#define LSN_BASE UINT64CONST(0x10000)
#define LSN_STEP UINT64CONST(0x100)
#define MAX_THREADS 3
#define MAX_PAGES 6
#define MAX_EVENTS 48
#define MAX_RECORDS (MAX_EVENTS * 2)
#define TRIALS 3000
#define MAX_REFAILS 3

/* One generated WAL record of one thread. */
typedef struct GenRecord {
	uint32 thread;
	uint32 ordinal; /* position in its thread */
	uint64 scn;
	uint16 count;
	int page[2];
	ClusterColdComponentV1 components[2];
} GenRecord;

/*
 * Page state, in DATA or in the simulated buffer.  body is the version whose
 * content the page holds; TORN_BODY marks a torn write whose header names
 * one version while the rest mixes two.  Without checksums such a page reads
 * as PRESENT with its header version.
 */
#define TORN_BODY UINT64CONST(0)

typedef struct SimPage {
	uint8 kind; /* ClusterColdDataKindV1 */
	RfPageVersionV1 version;
	uint64 body;
} SimPage;

typedef struct Scenario {
	bool checksums; /* torn pages fail verification and read INVALID */
	uint32 threads;
	uint32 pages;
	uint32 records;
	uint32 thread_records[MAX_THREADS];
	uint32 redo_ordinal[MAX_THREADS]; /* first non-history record */
	GenRecord record[MAX_RECORDS];
	SimPage terminal[MAX_PAGES];
	bool replayable[MAX_PAGES]; /* has a component after native redo */
	bool anchor_after_history[MAX_PAGES];
	SimPage disk[MAX_PAGES];
	SimPage memory[MAX_PAGES];
} Scenario;

static uint64 rng_state;

static uint32
rng(uint32 bound)
{
	rng_state ^= rng_state << 13;
	rng_state ^= rng_state >> 7;
	rng_state ^= rng_state << 17;
	return (uint32)(rng_state % bound);
}

static RfPageIdentityV1
page_id(int page)
{
	RfPageIdentityV1 id;

	memset(&id, 0, sizeof(id));
	id.system_identifier = 99;
	memset(id.storage_uuid, 3, sizeof(id.storage_uuid));
	id.locator.spcOid = 1663;
	id.locator.dbOid = 5;
	id.locator.relNumber = 100;
	id.forknum = MAIN_FORKNUM;
	id.blockno = (BlockNumber)page;
	return id;
}

static RfPageVersionV1
ver(uint64 token)
{
	RfPageVersionV1 version;

	memset(&version, 0, sizeof(version));
	memset(version.segment_incarnation, INC, sizeof(version.segment_incarnation));
	version.mutation_token = token;
	return version;
}

static SimPage
present(uint64 token)
{
	SimPage page;

	memset(&page, 0, sizeof(page));
	page.kind = CLUSTER_COLD_DATA_PRESENT;
	page.version = ver(token);
	page.body = token;
	return page;
}

/* A torn write: verification catches it, or the header alone survives. */
static SimPage
torn(const Scenario *s, const SimPage *header)
{
	SimPage page;

	memset(&page, 0, sizeof(page));
	if (s->checksums || header->kind != CLUSTER_COLD_DATA_PRESENT) {
		page.kind = CLUSTER_COLD_DATA_INVALID;
		return page;
	}
	page = *header;
	page.body = TORN_BODY;
	return page;
}

static bool
sim_equal(const SimPage *left, const SimPage *right)
{
	return left->kind == right->kind
		   && (left->kind != CLUSTER_COLD_DATA_PRESENT
			   || (memcmp(&left->version, &right->version, sizeof(left->version)) == 0
				   && left->body == right->body));
}

/* The header placement of a page, whatever its body holds. */
static bool
sim_header_equal(const SimPage *left, const SimPage *right)
{
	return left->kind == right->kind
		   && (left->kind != CLUSTER_COLD_DATA_PRESENT
			   || memcmp(&left->version, &right->version, sizeof(left->version)) == 0);
}

static XLogRecPtr
rec_read(uint32 ordinal)
{
	return LSN_BASE + (XLogRecPtr)ordinal * LSN_STEP;
}

static ClusterColdParticipantV1
participant_cut(const Scenario *s, uint32 thread)
{
	ClusterColdParticipantV1 cut;

	memset(&cut, 0, sizeof(cut));
	cut.thread_id = (uint16)(thread + 1);
	cut.timeline = 1;
	cut.owner_incarnation = 40 + thread;
	cut.physical_lower = LSN_BASE;
	cut.native_redo = rec_read(s->redo_ordinal[thread]);
	cut.tail_end = rec_read(s->thread_records[thread]);
	return cut;
}

static bool
record_history(const Scenario *s, const GenRecord *record)
{
	return record->ordinal < s->redo_ordinal[record->thread];
}

static void
set_component(ClusterColdComponentV1 *component, int page, uint8 block_id, const SimPage *before,
			  uint64 result, uint16 edge_flags)
{
	memset(component, 0, sizeof(*component));
	component->page = page_id(page);
	component->block_id = block_id;
	component->component_ordinal = block_id;
	component->page_class = RF_PAGE_CLASS_ORDINARY;
	component->result_kind = RF_PAGE_STATE_PRESENT;
	component->result = ver(result);
	component->edge_flags = edge_flags;
	if (before->kind == CLUSTER_COLD_DATA_ABSENT) {
		component->before_kind = RF_PAGE_STATE_ABSENT;
		component->edge_flags = RF_PAGE_EDGE_WILL_INIT | RF_PAGE_EDGE_FULL_COVERAGE;
	} else {
		component->before_kind = RF_PAGE_STATE_PRESENT;
		component->before = before->version;
	}
}

/*
 * Generate one timeline: each event is one record of one thread touching
 * one or two pages.  Under full_page_writes a thread's first change to a
 * page after its own redo start carries a full image; history records may
 * carry either.  DATA holds, for every page, any version at or after its
 * last history change (that thread's checkpoint wrote it), sometimes torn.
 */
static void
generate(Scenario *s)
{
	SimPage current[MAX_PAGES];
	SimPage chain_state[MAX_PAGES][MAX_EVENTS + 1];
	bool chain_anchor[MAX_PAGES][MAX_EVENTS + 1];
	bool touched[MAX_THREADS][MAX_PAGES];
	int last_history[MAX_PAGES];
	int chain_length[MAX_PAGES];
	uint64 token = 1000;
	uint32 events;
	uint32 e;
	int p;

	memset(s, 0, sizeof(*s));
	memset(touched, 0, sizeof(touched));
	memset(chain_anchor, 0, sizeof(chain_anchor));
	s->checksums = rng(2) == 0;
	s->threads = 2 + rng(MAX_THREADS - 1);
	s->pages = 2 + rng(MAX_PAGES - 1);
	events = s->threads + rng(MAX_EVENTS - s->threads);
	for (p = 0; p < (int)s->pages; p++) {
		if (rng(4) == 0) {
			memset(&current[p], 0, sizeof(current[p]));
			current[p].kind = CLUSTER_COLD_DATA_ABSENT;
		} else
			current[p] = present(token++);
		chain_state[p][0] = current[p];
		chain_length[p] = 0;
		last_history[p] = 0;
	}
	/* Threads, ordinals and redo starts first; edge flags need the redo. */
	for (e = 0; e < events; e++) {
		GenRecord *record = &s->record[s->records++];

		record->thread = e < s->threads ? e : rng(s->threads);
		record->ordinal = s->thread_records[record->thread]++;
		record->scn = rng(3) == 0 && e > 0 ? s->record[s->records - 2].scn : e + 1;
		record->count = rng(5) == 0 ? 0 : 1 + (rng(4) == 0);
		record->page[0] = (int)rng(s->pages);
		record->page[1] = (record->page[0] + 1 + (int)rng(s->pages - 1)) % (int)s->pages;
	}
	for (e = 0; e < s->threads; e++)
		s->redo_ordinal[e] = rng(s->thread_records[e] + 1);
	for (e = 0; e < s->records; e++) {
		GenRecord *record = &s->record[e];
		bool history = record_history(s, record);
		uint16 i;

		for (i = 0; i < record->count; i++) {
			int page = record->page[i];
			bool first_touch = !history && !touched[record->thread][page];
			uint16 flags = 0;
			int k;

			if (first_touch || (history && rng(3) == 0))
				flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
			if (!history)
				touched[record->thread][page] = true;
			set_component(&record->components[i], page, (uint8)i, &current[page], token, flags);
			current[page] = present(token++);
			k = ++chain_length[page];
			chain_state[page][k] = current[page];
			chain_anchor[page][k] = record->components[i].edge_flags != 0;
			if (history)
				last_history[page] = k;
			else
				s->replayable[page] = true;
		}
	}
	for (p = 0; p < (int)s->pages; p++) {
		int k;

		/* Only an anchor after the last history change can rebuild a page. */
		for (k = last_history[p] + 1; k <= chain_length[p]; k++)
			s->anchor_after_history[p] |= chain_anchor[p][k];
		s->disk[p] = chain_state[p][last_history[p]
									+ (int)rng((uint32)(chain_length[p] - last_history[p] + 1))];
		s->terminal[p] = current[p];
		if (rng(8) == 0)
			s->disk[p] = torn(s, &s->disk[p]);
	}
}

static bool
observe_disk(void *arg, const RfPageIdentityV1 *page, ClusterColdDataV1 *out)
{
	Scenario *s = (Scenario *)arg;
	int p = (int)page->blockno;

	memset(out, 0, sizeof(*out));
	if (p < 0 || p >= (int)s->pages)
		return false;
	out->kind = s->disk[p].kind;
	if (s->disk[p].kind == CLUSTER_COLD_DATA_PRESENT)
		out->version = s->disk[p].version;
	/* Checksums verify a whole page; without them only absence is proven. */
	if (s->disk[p].kind != CLUSTER_COLD_DATA_INVALID
		&& (s->checksums || s->disk[p].kind != CLUSTER_COLD_DATA_PRESENT))
		out->flags = CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED;
	return true;
}

/* order[i] receives the scenario thread fed as caller participant i. */
static ClusterColdDetailV1
plan_scenario(Scenario *s, ClusterColdPlanV1 **plan, uint32 *order)
{
	ClusterColdParticipantV1 cuts[MAX_THREADS];
	XLogRecPtr last_read[MAX_THREADS];
	ClusterColdDiagV1 diag;
	ClusterColdDetailV1 detail;
	uint32 position[MAX_THREADS];
	uint32 r;
	uint32 t;

	/* Enumeration order of participants must not matter. */
	for (t = 0; t < s->threads; t++)
		order[t] = t;
	for (t = s->threads; t > 1; t--) {
		uint32 j = rng(t);
		uint32 swap = order[t - 1];

		order[t - 1] = order[j];
		order[j] = swap;
	}
	for (t = 0; t < s->threads; t++) {
		cuts[t] = participant_cut(s, order[t]);
		position[order[t]] = t;
		last_read[t] = LSN_BASE - LSN_STEP;
	}
	detail = cluster_cold_plan_create_v1(cuts, s->threads, BUDGET, plan);
	if (detail != CLUSTER_COLD_OK)
		return detail;
	for (r = 0; r < s->records; r++) {
		const GenRecord *gen = &s->record[r];
		ClusterColdRecordV1 record;
		uint32 at = position[gen->thread];

		memset(&record, 0, sizeof(record));
		record.read_rec_ptr = rec_read(gen->ordinal);
		record.end_rec_ptr = record.read_rec_ptr + LSN_STEP;
		record.prev_rec_ptr = last_read[at];
		record.scn = gen->scn;
		record.record_crc = (uint32)(r * 2654435761U);
		record.rmid = RM_HEAP_ID;
		record.component_count = gen->count;
		record.components = gen->components;
		last_read[at] = record.read_rec_ptr;
		detail = cluster_cold_plan_feed_v1(*plan, at, &record);
		if (detail != CLUSTER_COLD_OK)
			return detail;
	}
	return cluster_cold_plan_seal_v1(*plan, observe_disk, s, &diag);
}

static const GenRecord *
find_record(const Scenario *s, uint32 thread, XLogRecPtr read)
{
	uint32 r;

	for (r = 0; r < s->records; r++)
		if (s->record[r].thread == thread && rec_read(s->record[r].ordinal) == read)
			return &s->record[r];
	return NULL;
}

/*
 * Apply one scheduled record to the simulated buffers.  Every applied block
 * must find exactly its expected state; a skipped block is never touched.
 */
static bool
sim_step(Scenario *s, const ClusterColdStepV1 *step, uint32 thread)
{
	const GenRecord *record = find_record(s, thread, step->read_rec_ptr);
	uint16 i;

	if (record == NULL || record->count == 0 || record_history(s, record))
		return false;
	for (i = 0; i < record->count; i++) {
		const ClusterColdBlockStepV1 *block = &step->blocks[record->components[i].block_id];
		SimPage *page = &s->memory[record->page[i]];

		switch (block->verdict) {
		case CLUSTER_COLD_BLOCK_SKIP:
			continue;
		case CLUSTER_COLD_BLOCK_APPLY_DELTA:
			if (block->expected_kind != CLUSTER_COLD_DATA_PRESENT)
				return false;
			/* FALLTHROUGH */
		case CLUSTER_COLD_BLOCK_APPLY_IMAGE:
		case CLUSTER_COLD_BLOCK_APPLY_INIT:
			if (block->expected_kind != CLUSTER_COLD_DATA_INVALID) {
				SimPage expected;

				memset(&expected, 0, sizeof(expected));
				expected.kind = block->expected_kind;
				expected.version = block->expected_before;
				if (!sim_header_equal(page, &expected))
					return false;
			}
			/* A delta needs the content its header names, never a torn body. */
			if (block->verdict == CLUSTER_COLD_BLOCK_APPLY_DELTA
				&& page->body != page->version.mutation_token)
				return false;
			if (memcmp(&block->result, &record->components[i].result, sizeof(block->result)) != 0)
				return false;
			*page = present(block->result.mutation_token);
			break;
		default:
			return false;
		}
	}
	return true;
}

typedef enum RunOutcome {
	RUN_OK,
	RUN_REFUSED, /* a page nothing can rebuild: fail-closed */
	RUN_BROKEN
} RunOutcome;

/*
 * The only accepted refusals: a page with no anchor after its last history
 * change that is unreadable, or (without checksums) only header-placed.
 */
static bool
refusal_explained(const Scenario *s, ClusterColdDetailV1 detail)
{
	uint32 p;

	for (p = 0; p < s->pages; p++) {
		if (!s->replayable[p] || s->anchor_after_history[p])
			continue;
		if (detail == CLUSTER_COLD_ANCHOR_MISSING && s->disk[p].kind == CLUSTER_COLD_DATA_INVALID)
			return true;
		if (detail == CLUSTER_COLD_CONTENT_UNPROVEN && !s->checksums
			&& s->disk[p].kind == CLUSTER_COLD_DATA_PRESENT)
			return true;
	}
	return false;
}

/*
 * Plan from DATA and replay the schedule, writing random pages to DATA as it
 * goes.  With `fail`, the attempt stops before a random step and a random
 * dirty page may be left torn.
 */
static RunOutcome
run_once(Scenario *s, bool fail, bool *finished)
{
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdDetailV1 detail;
	uint32 order[MAX_THREADS];
	uint32 steps;
	uint32 limit;
	uint32 i;

	*finished = false;
	detail = plan_scenario(s, &plan, order);
	if (detail != CLUSTER_COLD_OK) {
		cluster_cold_plan_destroy_v1(&plan);
		if (refusal_explained(s, detail))
			return RUN_REFUSED;
		printf("# seal refused: detail %d\n", (int)detail);
		return RUN_BROKEN;
	}
	memcpy(s->memory, s->disk, sizeof(s->memory));
	steps = cluster_cold_plan_step_count_v1(plan);
	limit = fail && steps > 0 ? rng(steps) : steps;
	for (i = 0; i < steps && i < limit; i++) {
		ClusterColdStepV1 step;

		if (!cluster_cold_plan_step_v1(plan, i, &step)
			|| !sim_step(s, &step, order[step.participant])) {
			printf("# step %u of %u violated its expectation\n", i, steps);
			cluster_cold_plan_destroy_v1(&plan);
			return RUN_BROKEN;
		}
		if (rng(3) == 0) {
			uint32 p = rng(s->pages);

			s->disk[p] = s->memory[p];
		}
	}
	cluster_cold_plan_destroy_v1(&plan);
	if (i == steps) {
		*finished = true;
		return RUN_OK;
	}
	if (rng(4) == 0) {
		uint32 p = rng(s->pages);

		/* The in-flight write tears; either header may survive. */
		if (!sim_equal(&s->disk[p], &s->memory[p]))
			s->disk[p] = torn(s, rng(2) == 0 && s->disk[p].kind == CLUSTER_COLD_DATA_PRESENT
									 ? &s->disk[p]
									 : &s->memory[p]);
	}
	return RUN_OK;
}

static bool
converged(const Scenario *s)
{
	uint32 p;

	for (p = 0; p < s->pages; p++)
		if (s->replayable[p] && !sim_equal(&s->memory[p], &s->terminal[p]))
			return false;
	return true;
}

/*
 * Property: from any DATA a crash can leave, replaying a prefix of the
 * schedule, failing again (any subset of pages written, possibly torn) and
 * planning again always converges to the terminal version of every page,
 * and no delta is ever applied to a state other than the expected one or to
 * a torn body.  The only accepted refusals are pages that no anchor after
 * their last history change can rebuild (refusal_explained).
 */
UT_TEST(test_refail_converges_from_any_prefix)
{
	uint32 trial;
	uint32 converged_trials = 0;
	uint32 refails = 0;
	uint32 refused_trials = 0;
	uint32 converged_unverified = 0;

	for (trial = 0; trial < TRIALS; trial++) {
		Scenario s;
		uint32 attempt;
		bool finished = false;
		RunOutcome outcome = RUN_OK;

		rng_state = UINT64CONST(0x9E3779B97F4A7C15) ^ ((uint64)trial * 7919 + 1);
		generate(&s);
		for (attempt = 0; attempt <= MAX_REFAILS && !finished; attempt++) {
			outcome = run_once(&s, attempt < MAX_REFAILS && rng(4) != 0, &finished);
			if (outcome != RUN_OK)
				break;
			refails += !finished;
		}
		if (outcome == RUN_REFUSED) {
			refused_trials++;
			continue;
		}
		if (outcome != RUN_OK || !finished || !converged(&s))
			printf("# trial %u did not converge (outcome %d, finished %d)\n", trial, (int)outcome,
				   (int)finished);
		UT_ASSERT_EQ(outcome, RUN_OK);
		UT_ASSERT(finished);
		UT_ASSERT(converged(&s));
		converged_trials++;
		converged_unverified += !s.checksums;
	}
	printf("# converged %u (%u without checksums), refused (no anchor) %u of %u; "
		   "%u re-failures\n",
		   converged_trials, converged_unverified, refused_trials, TRIALS, refails);
	/* The generator must exercise the interesting paths, not refuse them. */
	UT_ASSERT(converged_trials > TRIALS / 2);
	UT_ASSERT(converged_unverified > TRIALS / 8);
	UT_ASSERT(refails > TRIALS);
}

/* Build a scenario by hand: records are (thread, page, edge) in time order. */
typedef struct HandEdge {
	uint32 thread;
	int page;
	bool anchor;
} HandEdge;

static void
hand_scenario(Scenario *s, const HandEdge *edges, uint32 count, uint32 threads,
			  const uint32 *redo_ordinal)
{
	SimPage current[MAX_PAGES];
	uint64 token = 500;
	uint32 e;
	int p;

	memset(s, 0, sizeof(*s));
	s->threads = threads;
	s->pages = 1;
	for (e = 0; e < count; e++)
		if (edges[e].page + 1 > (int)s->pages)
			s->pages = (uint32)edges[e].page + 1;
	for (p = 0; p < (int)s->pages; p++)
		current[p] = present(token++);
	for (e = 0; e < threads; e++)
		s->redo_ordinal[e] = redo_ordinal[e];
	for (e = 0; e < count; e++) {
		GenRecord *record = &s->record[s->records++];
		int page = edges[e].page;

		record->thread = edges[e].thread;
		record->ordinal = s->thread_records[record->thread]++;
		record->scn = e + 1;
		record->count = 1;
		record->page[0] = page;
		set_component(&record->components[0], page, 0, &current[page], token,
					  edges[e].anchor ? RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE
									  : 0);
		current[page] = present(token++);
		if (!record_history(s, record))
			s->replayable[page] = true;
	}
	for (p = 0; p < (int)s->pages; p++)
		s->terminal[p] = current[p];
}

static ClusterColdStepV1
nth_step(ClusterColdPlanV1 *plan, uint32 index)
{
	ClusterColdStepV1 step;

	memset(&step, 0, sizeof(step));
	UT_ASSERT(cluster_cold_plan_step_v1(plan, index, &step));
	return step;
}

/*
 * No replayable anchor: after a re-failure that wrote the first applied
 * delta, planning again skips exactly the durable prefix and applies only
 * the suffix, each on its exact predecessor.
 */
UT_TEST(test_refail_without_anchor_applies_only_the_suffix)
{
	/* page 0: thread 2 history image h, then thread 1 deltas d1, d2. */
	HandEdge edges[] = { { 1, 0, true }, { 0, 0, false }, { 0, 0, false } };
	uint32 redo[2] = { 0, 1 };
	uint32 order[MAX_THREADS];
	Scenario s;
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdStepV1 step;

	rng_state = 1;
	hand_scenario(&s, edges, 3, 2, redo);
	s.checksums = true;
	s.disk[0] = present(501); /* h's result: thread 2's checkpoint wrote it */
	UT_ASSERT_EQ(plan_scenario(&s, &plan, order), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 2);
	step = nth_step(plan, 0);
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	cluster_cold_plan_destroy_v1(&plan);

	/* First run applied d1 and wrote it, then failed again. */
	s.disk[0] = present(502);
	UT_ASSERT_EQ(plan_scenario(&s, &plan, order), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 2);
	step = nth_step(plan, 0);
	UT_ASSERT_EQ(step.read_rec_ptr, rec_read(0));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_SKIP);
	UT_ASSERT(step.all_skip);
	step = nth_step(plan, 1);
	UT_ASSERT_EQ(step.read_rec_ptr, rec_read(1));
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_PRESENT);
	UT_ASSERT_EQ(step.blocks[0].expected_before.mutation_token, 502);
	cluster_cold_plan_destroy_v1(&plan);

	/* A torn write of d1 leaves no anchor to rebuild from: refused. */
	memset(&s.disk[0], 0, sizeof(s.disk[0]));
	s.disk[0].kind = CLUSTER_COLD_DATA_INVALID;
	UT_ASSERT_EQ(plan_scenario(&s, &plan, order), CLUSTER_COLD_ANCHOR_MISSING);
	cluster_cold_plan_destroy_v1(&plan);

	/* Without checksums the same intact-looking page cannot be proven. */
	s.checksums = false;
	s.disk[0] = present(502);
	UT_ASSERT_EQ(plan_scenario(&s, &plan, order), CLUSTER_COLD_CONTENT_UNPROVEN);
	cluster_cold_plan_destroy_v1(&plan);
}

/*
 * With a replayable anchor DATA has reached, planning again restores that
 * anchor over the re-failed page and replays forward from it, so a page
 * written by the failed attempt is never trusted by its header alone.
 */
UT_TEST(test_refail_with_anchor_restores_and_replays_forward)
{
	/* page 0: thread 1 image a, thread 2 delta b, thread 1 delta c. */
	HandEdge edges[] = { { 0, 0, true }, { 1, 0, false }, { 0, 0, false } };
	uint32 redo[2] = { 0, 0 };
	Scenario s;
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdStepV1 step;
	uint32 order[MAX_THREADS];
	uint32 i;

	rng_state = 1;
	hand_scenario(&s, edges, 3, 2, redo);
	/* DATA was 500 (before a); the first attempt applied a and b, wrote b
	 * and failed again.  Without checksums the anchor still rebuilds it. */
	s.disk[0] = present(502);
	UT_ASSERT_EQ(plan_scenario(&s, &plan, order), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_plan_step_count_v1(plan), 3);
	step = nth_step(plan, 0);
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_IMAGE);
	UT_ASSERT_EQ(step.blocks[0].expected_kind, CLUSTER_COLD_DATA_PRESENT);
	UT_ASSERT_EQ(step.blocks[0].expected_before.mutation_token, 502);
	step = nth_step(plan, 1);
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT_EQ(step.blocks[0].expected_before.mutation_token, 501);
	step = nth_step(plan, 2);
	UT_ASSERT_EQ(step.blocks[0].verdict, CLUSTER_COLD_BLOCK_APPLY_DELTA);
	UT_ASSERT_EQ(step.blocks[0].expected_before.mutation_token, 502);

	/* Replaying the new schedule from the written page ends at c. */
	memcpy(s.memory, s.disk, sizeof(s.memory));
	for (i = 0; i < 3; i++) {
		ClusterColdStepV1 next = nth_step(plan, i);

		UT_ASSERT(sim_step(&s, &next, order[next.participant]));
	}
	UT_ASSERT(sim_equal(&s.memory[0], &s.terminal[0]));
	cluster_cold_plan_destroy_v1(&plan);
}

int
main(void)
{
	UT_RUN(test_refail_without_anchor_applies_only_the_suffix);
	UT_RUN(test_refail_with_anchor_restores_and_replays_forward);
	UT_RUN(test_refail_converges_from_any_prefix);
	UT_DONE();
	return ut_failed_count != 0;
}
