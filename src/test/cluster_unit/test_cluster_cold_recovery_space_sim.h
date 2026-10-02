/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_space_sim.h
 *	  Planning and pass-2 simulation for the typed cold-crash SPACE
 *	  re-failure tests: DATA observation, a stand-in SPACE owner (check and
 *	  installs), buffers that enforce every block expectation, the accepted
 *	  refusals and the convergence check.
 *
 *	  Included once by test_cluster_cold_recovery_space_refail.c, after
 *	  test_cluster_cold_recovery_space_gen.h.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_space_sim.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_COLD_RECOVERY_SPACE_SIM_H
#define TEST_CLUSTER_COLD_RECOVERY_SPACE_SIM_H

/* Truncation steps that shrank again, and that kept proven files. */
static uint32 shrinks_repeated;
static uint32 shrinks_kept;

static int
find_event(const World *w, uint32 thread, XLogRecPtr read)
{
	uint32 e;

	for (e = 0; e < w->events; e++)
		if (w->event[e].thread == thread && rec_read(w->event[e].ordinal) == read)
			return (int)e;
	return -1;
}

static bool
observe_disk(void *arg, const RfPageIdentityV1 *page, ClusterColdDataV1 *out)
{
	World *w = (World *)arg;
	int r = (int)page->locator.relNumber - REL_BASE;
	int b = (int)page->blockno;
	const Rel *rel;
	uint8 ident;

	memset(out, 0, sizeof(*out));
	if (r < 0 || r >= (int)w->rels || b < 0 || b >= MAX_BLOCKS || page->forknum != MAIN_FORKNUM)
		return false;
	rel = &w->rel[r];
	ident = rel->ident[rel->space_pos];
	out->kind = rel->disk[b].kind;
	if (out->kind == CLUSTER_COLD_DATA_INVALID)
		return true;
	if (out->kind == CLUSTER_COLD_DATA_PRESENT || out->kind == CLUSTER_COLD_DATA_UNFORMATTED) {
		if (has_identity(ident))
			memset(out->version.segment_incarnation, ident, 16);
		else
			out->flags |= CLUSTER_COLD_DATA_FLAG_NO_IDENTITY;
		out->version.mutation_token = rel->disk[b].token;
	}
	/* Checksums verify a whole page; without them only absence is proven. */
	if (w->checksums || out->kind != CLUSTER_COLD_DATA_PRESENT)
		out->flags |= CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED;
	return true;
}

/*
 * Stand-in SPACE owner: the inputs must be exactly the relation's effects
 * after a native redo start that no history change already covered, and
 * the owner orders them by the timeline.
 */
static bool
space_check(void *arg, const RelFileLocator *locator, const ClusterColdSpaceInputV1 *inputs,
			uint32 count, uint32 *order)
{
	World *w = (World *)arg;
	int r = (int)locator->relNumber - REL_BASE;
	int event[MAX_EVENTS];
	uint32 expected = 0;
	uint32 e;
	uint32 i;

	for (e = 0; e < w->events; e++)
		expected += w->event[e].kind == EV_SPACE && w->event[e].expect_input
					&& (int)w->event[e].op.locator.relNumber - REL_BASE == r;
	if (count != expected || count > MAX_EVENTS) {
		w->check_mismatch = true;
		return false;
	}
	for (i = 0; i < count; i++) {
		int found = find_event(w, w->order[inputs[i].participant], inputs[i].read_rec_ptr);

		if (found < 0 || w->event[found].kind != EV_SPACE || !w->event[found].expect_input
			|| w->event[found].op.kind != inputs[i].kind) {
			w->check_mismatch = true;
			return false;
		}
		event[i] = found;
		order[i] = i;
	}
	for (i = 1; i < count; i++) {
		uint32 j = i;

		while (j > 0 && event[order[j - 1]] > event[order[j]]) {
			uint32 swap = order[j - 1];

			order[j - 1] = order[j];
			order[j] = swap;
			j--;
		}
	}
	return true;
}

static ClusterColdDetailV1
plan_world(World *w, ClusterColdPlanV1 **plan)
{
	ClusterColdParticipantV1 cuts[MAX_THREADS];
	XLogRecPtr last_read[MAX_THREADS];
	uint32 position[MAX_THREADS];
	ClusterColdDetailV1 detail;
	uint32 t;
	uint32 e;

	/* Enumeration order of participants must not matter. */
	for (t = 0; t < w->threads; t++)
		w->order[t] = t;
	for (t = w->threads; t > 1; t--) {
		uint32 j = rng(t);
		uint32 swap = w->order[t - 1];

		w->order[t - 1] = w->order[j];
		w->order[j] = swap;
	}
	for (t = 0; t < w->threads; t++) {
		uint32 thread = w->order[t];

		memset(&cuts[t], 0, sizeof(cuts[t]));
		cuts[t].thread_id = (uint16)(thread + 1);
		cuts[t].timeline = 1;
		cuts[t].owner_incarnation = 40 + thread;
		cuts[t].physical_lower = LSN_BASE;
		cuts[t].native_redo = rec_read(w->redo_ordinal[thread]);
		cuts[t].tail_end = rec_read(w->thread_records[thread]);
		position[thread] = t;
		last_read[t] = LSN_BASE - LSN_STEP;
	}
	detail = cluster_cold_plan_create_v1(cuts, w->threads, BUDGET, plan);
	if (detail == CLUSTER_COLD_OK)
		detail = cluster_cold_plan_set_space_check_v1(*plan, space_check, w);
	for (e = 0; detail == CLUSTER_COLD_OK && e < w->events; e++) {
		const Event *ev = &w->event[e];
		ClusterColdRecordV1 record;
		uint32 at = position[ev->thread];

		memset(&record, 0, sizeof(record));
		record.read_rec_ptr = rec_read(ev->ordinal);
		record.end_rec_ptr = record.read_rec_ptr + LSN_STEP;
		record.prev_rec_ptr = last_read[at];
		record.scn = ev->scn;
		record.record_crc = (uint32)(e * 2654435761U);
		record.rmid = ev->kind == EV_SPACE ? RM_SMGR_ID : RM_HEAP_ID;
		if (ev->kind == EV_PAGE) {
			record.component_count = ev->count;
			record.components = ev->components;
		} else if (ev->kind == EV_SPACE) {
			record.space_count = 1;
			record.space_ops = &ev->op;
		}
		last_read[at] = record.read_rec_ptr;
		detail = cluster_cold_plan_feed_v1(*plan, at, &record);
	}
	if (detail != CLUSTER_COLD_OK)
		return detail;
	return cluster_cold_plan_seal_v1(*plan, observe_disk, w, &w->diag);
}

/* Whether a buffer shows exactly the state a block expects. */
static bool
header_matches(const Rel *rel, const Phys *page, const ClusterColdBlockStepV1 *block)
{
	RfPageVersionV1 shown;

	if (page->kind != block->expected_kind)
		return false;
	if (page->kind != CLUSTER_COLD_DATA_PRESENT)
		return true;
	if (!has_identity(rel->ident[rel->space_pos]))
		return false;
	shown = ver(rel->ident[rel->space_pos], page->token);
	return memcmp(&shown, &block->expected_before, sizeof(shown)) == 0;
}

/*
 * Apply one scheduled page record to the buffers.  Every applied block must
 * find exactly its expected state, under the incarnation it belongs to.
 */
static bool
sim_page_step(World *w, const ClusterColdStepV1 *step, const Event *ev)
{
	uint16 i;

	if (ev->kind != EV_PAGE || ev->history)
		return false;
	for (i = 0; i < ev->count; i++) {
		const ClusterColdComponentV1 *c = &ev->components[i];
		const ClusterColdBlockStepV1 *block = &step->blocks[c->block_id];
		Rel *rel = &w->rel[ev->rel[i]];
		Phys *page = &rel->mem[ev->block[i]];

		switch (block->verdict) {
		case CLUSTER_COLD_BLOCK_SKIP:
			continue;
		case CLUSTER_COLD_BLOCK_APPLY_DELTA:
			if (block->expected_kind != CLUSTER_COLD_DATA_PRESENT || page->torn)
				return false;
			/* FALLTHROUGH */
		case CLUSTER_COLD_BLOCK_APPLY_IMAGE:
		case CLUSTER_COLD_BLOCK_APPLY_INIT:
			if (rel->dropped || rel->ident[rel->space_pos] != c->result.segment_incarnation[0]
				|| (block->expected_kind != CLUSTER_COLD_DATA_INVALID
					&& !header_matches(rel, page, block))
				|| memcmp(&block->result, &c->result, sizeof(block->result)) != 0)
				return false;
			memset(page, 0, sizeof(*page));
			page->kind = CLUSTER_COLD_DATA_PRESENT;
			page->token = c->result.mutation_token;
			normalize(rel->mem);
			break;
		default:
			return false;
		}
	}
	return true;
}

/* A truncation's shrink the SPACE owner repeats at its step. */
static bool
shrink_at_step(const ClusterColdSpaceInputV1 *input)
{
	return (input->shrink_forks & (1 << MAIN_FORKNUM)) != 0;
}

/*
 * The SPACE owner brings a relation's SPACE pages through one input (and
 * writes and syncs them); at the input's own step it also performs the
 * input's physical action.
 */
static bool
sim_install(World *w, const ClusterColdPlanV1 *plan, uint32 relation, uint32 input, bool step)
{
	ClusterColdSpaceInputV1 in;
	RelFileLocator locator;
	uint32 count;
	const Event *ev;
	Rel *rel;
	int found;
	uint32 target;
	int b;

	if (!cluster_cold_plan_space_relation_v1(plan, relation, &locator, &count)
		|| !cluster_cold_plan_space_input_v1(plan, relation, input, &in))
		return false;
	found = find_event(w, w->order[in.participant], in.read_rec_ptr);
	if (found < 0)
		return false;
	ev = &w->event[found];
	rel = &w->rel[(int)locator.relNumber - REL_BASE];
	target = (uint32)ev->op_index + 1;
	/* Pages already holding this input or a later one stay as they are. */
	if (rel->space_pos < target)
		rel->space_pos = target;
	if (!step)
		return true;
	if (ev->op.kind == CLUSTER_COLD_SPACE_TRUNCATE) {
		shrinks_repeated += shrink_at_step(&in);
		shrinks_kept += !shrink_at_step(&in);
	}
	if (ev->op.kind == CLUSTER_COLD_SPACE_TRUNCATE && shrink_at_step(&in)) {
		for (b = (int)ev->op.nblocks; b < MAX_BLOCKS; b++) {
			memset(&rel->mem[b], 0, sizeof(Phys));
			rel->mem[b].kind = CLUSTER_COLD_DATA_ABSENT;
			/* The shrink is durable only once the file is synced. */
			if (rng(2) == 0)
				rel->disk[b] = rel->mem[b];
		}
	} else if (ev->op.kind == CLUSTER_COLD_SPACE_DROP) {
		/* The commit's native replay unlinks the files. */
		memset(rel->mem, 0, sizeof(rel->mem));
		for (b = 0; b < MAX_BLOCKS; b++)
			rel->mem[b].kind = CLUSTER_COLD_DATA_ABSENT;
		memcpy(rel->disk, rel->mem, sizeof(rel->disk));
	}
	normalize(rel->mem);
	normalize(rel->disk);
	return true;
}

static bool
sim_step(World *w, const ClusterColdPlanV1 *plan, uint32 index, const ClusterColdStepV1 *step)
{
	int found = find_event(w, w->order[step->participant], step->read_rec_ptr);
	uint32 effect;

	if (found < 0)
		return false;
	if (step->step_kind == CLUSTER_COLD_STEP_PAGE)
		return sim_page_step(w, step, &w->event[found]);
	for (effect = 0; effect < step->space_count; effect++) {
		uint32 relation;
		uint32 input;

		if (!cluster_cold_plan_step_space_v1(plan, index, effect, &relation, &input)
			|| !sim_install(w, plan, relation, input, true))
			return false;
	}
	return true;
}

typedef enum RunOutcome {
	RUN_OK,
	RUN_REFUSED,	   /* a page nothing can rebuild: fail-closed */
	RUN_REFUSED_STALE, /* F-D-21: see stale_identity_refusal */
	RUN_BROKEN
} RunOutcome;

/* The only accepted refusals: a page no anchor after its last settled
 * change can rebuild that is unreadable, or (without checksums) unproven. */
static bool
refusal_explained(const World *w, ClusterColdDetailV1 detail)
{
	int r;
	int b;

	for (r = 0; r < (int)w->rels; r++)
		for (b = 0; b < MAX_BLOCKS; b++) {
			const Rel *rel = &w->rel[r];

			if (!rel->replayable[b] || rel->anchored[b])
				continue;
			if (detail == CLUSTER_COLD_ANCHOR_MISSING
				&& rel->disk[b].kind == CLUSTER_COLD_DATA_INVALID)
				return true;
			if (detail == CLUSTER_COLD_CONTENT_UNPROVEN && !w->checksums
				&& rel->disk[b].kind == CLUSTER_COLD_DATA_PRESENT)
				return true;
		}
	return false;
}

/*
 * Known liveness gap (worklog F-D-21, requests.md R-A17): a block that a
 * replayed TRUNCATE retired and history re-extended holds its new content
 * while the SPACE identity on disk still predates that TRUNCATE.  The
 * header then cannot be told from the retired content, nothing replayable
 * rebuilds the page, and the plan refuses (fail-closed, before any change).
 */
static bool
stale_identity_refusal(const World *w, ClusterColdDetailV1 detail)
{
	int r;
	int b;

	for (r = 0; detail == CLUSTER_COLD_ANCHOR_MISSING && r < (int)w->rels; r++)
		for (b = 0; b < MAX_BLOCKS; b++) {
			const Rel *rel = &w->rel[r];

			if (rel->replayable[b] && !rel->anchored[b] && rel->retire_op[b] >= 0
				&& rel->disk[b].kind == CLUSTER_COLD_DATA_PRESENT
				&& (int)rel->space_pos <= rel->retire_op[b])
				return true;
		}
	return false;
}

static void
crash_tears(World *w)
{
	int r = (int)rng(w->rels);
	int b = (int)rng(MAX_BLOCKS);
	Rel *rel = &w->rel[r];

	/* The in-flight write tears; either header may survive. */
	if (rel->mem[b].kind == CLUSTER_COLD_DATA_PRESENT
		&& memcmp(&rel->disk[b], &rel->mem[b], sizeof(Phys)) != 0)
		rel->disk[b]
			= torn(w, rng(2) == 0 && rel->disk[b].kind == CLUSTER_COLD_DATA_PRESENT ? &rel->disk[b]
																					: &rel->mem[b]);
}

/*
 * Plan from DATA and replay the schedule, writing random buffers to DATA as
 * it goes.  With `fail`, the attempt stops before a random step.
 */
static RunOutcome
run_once(World *w, bool fail, bool *finished)
{
	ClusterColdPlanV1 *plan = NULL;
	ClusterColdDetailV1 detail;
	uint32 steps;
	uint32 limit;
	uint32 i;
	int r;

	*finished = false;
	detail = plan_world(w, &plan);
	if (detail != CLUSTER_COLD_OK) {
		cluster_cold_plan_destroy_v1(&plan);
		if (!w->check_mismatch && refusal_explained(w, detail))
			return RUN_REFUSED;
		if (!w->check_mismatch && stale_identity_refusal(w, detail))
			return RUN_REFUSED_STALE;
		printf("# seal refused: detail %d%s\n", (int)detail,
			   w->check_mismatch ? " (SPACE inputs differ from the timeline)" : "");
		return RUN_BROKEN;
	}
	for (r = 0; r < (int)w->rels; r++)
		memcpy(w->rel[r].mem, w->rel[r].disk, sizeof(w->rel[r].mem));
	steps = cluster_cold_plan_step_count_v1(plan);
	limit = fail && steps > 0 ? rng(steps) : steps;
	for (i = 0; i < steps && i < limit; i++) {
		ClusterColdStepV1 step;

		if (!cluster_cold_plan_step_v1(plan, i, &step) || !sim_step(w, plan, i, &step)) {
			printf("# step %u of %u violated its expectation\n", i, steps);
			cluster_cold_plan_destroy_v1(&plan);
			return RUN_BROKEN;
		}
		if (rng(3) == 0) {
			Rel *rel = &w->rel[rng(w->rels)];
			int b = (int)rng(MAX_BLOCKS);

			rel->disk[b] = rel->mem[b];
			normalize(rel->disk);
		}
	}
	if (i == steps) {
		uint32 relation;

		*finished = true;
		for (relation = 0; relation < cluster_cold_plan_space_relation_count_v1(plan); relation++) {
			RelFileLocator locator;
			uint32 count;

			if (!cluster_cold_plan_space_relation_v1(plan, relation, &locator, &count)
				|| !sim_install(w, plan, relation, count - 1, false)) {
				cluster_cold_plan_destroy_v1(&plan);
				return RUN_BROKEN;
			}
		}
	} else if (rng(4) == 0)
		crash_tears(w);
	cluster_cold_plan_destroy_v1(&plan);
	return RUN_OK;
}

/* Every relation ends with its final identity, pages and size. */
static bool
converged(const World *w)
{
	int r;
	int b;

	for (r = 0; r < (int)w->rels; r++) {
		const Rel *rel = &w->rel[r];

		if (rel->ident[rel->space_pos] != rel->ident[rel->nops])
			return false;
		for (b = 0; !rel->dropped && b < MAX_BLOCKS; b++) {
			const Phys *want = current(rel, b);

			if (rel->mem[b].kind != want->kind || rel->mem[b].torn
				|| (want->kind == CLUSTER_COLD_DATA_PRESENT && rel->mem[b].token != want->token))
				return false;
		}
	}
	return true;
}

/* Print one timeline and its DATA (SPACE_REFAIL_TRIAL=<n> runs only it). */
static void
dump_world(const World *w)
{
	uint32 e;
	int r;
	int b;

	printf("# checksums %d threads %u redo", (int)w->checksums, w->threads);
	for (e = 0; e < w->threads; e++)
		printf(" %u", w->redo_ordinal[e]);
	printf("\n");
	for (e = 0; e < w->events; e++) {
		const Event *ev = &w->event[e];
		uint16 i;

		printf("# e%u t%u o%u scn %llu %s", e, ev->thread, ev->ordinal, (unsigned long long)ev->scn,
			   ev->history ? "hist" : "redo");
		if (ev->kind == EV_SPACE)
			printf(" SPACE kind %u rel %d before %u result %u n %u input %d", ev->op.kind,
				   (int)ev->op.locator.relNumber - REL_BASE, ev->op.before_incarnation[0],
				   ev->op.result_incarnation[0], ev->op.nblocks, (int)ev->expect_input);
		for (i = 0; ev->kind == EV_PAGE && i < ev->count; i++)
			printf(" [r%d b%d %s (%u,%llu)->(%u,%llu) f%x]", ev->rel[i], ev->block[i],
				   ev->components[i].before_kind == RF_PAGE_STATE_ABSENT ? "init" : "chg",
				   ev->components[i].before.segment_incarnation[0],
				   (unsigned long long)ev->components[i].before.mutation_token,
				   ev->components[i].result.segment_incarnation[0],
				   (unsigned long long)ev->components[i].result.mutation_token,
				   ev->components[i].edge_flags);
		printf("\n");
	}
	for (r = 0; r < (int)w->rels; r++) {
		const Rel *rel = &w->rel[r];

		printf("# rel %d ident %u (pos %u of %u, floor %u)", r, rel->ident[rel->space_pos],
			   rel->space_pos, rel->nops, rel->ident_floor);
		for (b = 0; b < MAX_BLOCKS; b++)
			printf(" | b%d disk %u/%llu%s mem %u/%llu want %u/%llu floor %d", b, rel->disk[b].kind,
				   (unsigned long long)rel->disk[b].token, rel->disk[b].torn ? "T" : "",
				   rel->mem[b].kind, (unsigned long long)rel->mem[b].token, current(rel, b)->kind,
				   (unsigned long long)current(rel, b)->token, rel->floor[b]);
		printf("\n");
	}
	printf("# diag detail %u participant %u read %llX page r%d b%u version (%u,%llu)\n",
		   w->diag.detail, w->diag.participant, (unsigned long long)w->diag.read_rec_ptr,
		   (int)w->diag.page.locator.relNumber - REL_BASE, w->diag.page.blockno,
		   w->diag.version.segment_incarnation[0],
		   (unsigned long long)w->diag.version.mutation_token);
}

#endif /* TEST_CLUSTER_COLD_RECOVERY_SPACE_SIM_H */
