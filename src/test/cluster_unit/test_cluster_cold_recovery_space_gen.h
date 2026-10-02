/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_space_gen.h
 *	  World model and timeline generator for the typed cold-crash SPACE
 *	  re-failure tests: relations with SPACE identities and incarnations,
 *	  per-block physical histories, the floors a crash cannot go below, and
 *	  the WAL records (page components and SPACE effects) of each thread.
 *
 *	  Included once by test_cluster_cold_recovery_space_refail.c.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_space_gen.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_COLD_RECOVERY_SPACE_GEN_H
#define TEST_CLUSTER_COLD_RECOVERY_SPACE_GEN_H

#define REL_BASE 100
#define INC_NONE 0
#define INC_TOMB 0xEE
#define BUDGET (4 * 1024 * 1024)
#define LSN_BASE UINT64CONST(0x10000)
#define LSN_STEP UINT64CONST(0x100)
#define MAX_THREADS 3
#define MAX_RELS 2
#define MAX_BLOCKS 3
#define MAX_EVENTS 32
#define MAX_STATES (2 * MAX_EVENTS + 2)

typedef enum EventKind { EV_PLAIN = 0, EV_PAGE, EV_SPACE } EventKind;

/*
 * A block's physical state.  The incarnation a header shows comes from the
 * relation's SPACE identity, so only the token is kept.  torn: the header
 * names token while the body mixes versions (read only without checksums).
 */
typedef struct Phys {
	uint8 kind; /* ClusterColdDataKindV1 */
	bool torn;
	uint64 token;
} Phys;

/* One generated WAL record. */
typedef struct Event {
	uint32 thread;
	uint32 ordinal; /* position in its thread */
	uint64 scn;
	uint8 kind; /* EventKind */
	bool history;
	uint16 count;
	int rel[2];
	int block[2];
	ClusterColdComponentV1 components[2];
	ClusterColdSpaceOpV1 op;
	int op_index;			/* EV_SPACE: position among the relation's ops */
	int shrunk[MAX_BLOCKS]; /* TRUNCATE: each block's state index after it */
	bool expect_input;		/* EV_SPACE: handed to the SPACE owner */
} Event;

typedef struct Rel {
	bool alive;
	bool dropped;
	uint8 inc;
	uint32 size;
	uint32 incarnations;
	uint32 nops;
	int op_event[MAX_EVENTS];
	uint8 ident[MAX_EVENTS + 1]; /* SPACE identity after k ops */
	uint32 ident_floor;			 /* SPACE pages hold at least this many ops */
	Phys hist[MAX_BLOCKS][MAX_STATES];
	int nhist[MAX_BLOCKS];
	int floor[MAX_BLOCKS]; /* durable at least through this state */
	int last_history[MAX_BLOCKS];
	int last_truncate;
	int retire_op[MAX_BLOCKS];	 /* op index of the last TRUNCATE retiring it, -1 */
	bool replayable[MAX_BLOCKS]; /* a change after the last settled one */
	bool anchored[MAX_BLOCKS];	 /* ... and one of them is an anchor */
	uint32 space_pos;			 /* SPACE pages, in DATA and in buffers */
	Phys disk[MAX_BLOCKS];
	Phys mem[MAX_BLOCKS];
} Rel;

typedef struct World {
	bool checksums;
	uint32 threads;
	uint32 rels;
	uint32 events;
	uint64 token;
	uint32 thread_records[MAX_THREADS];
	uint32 redo_ordinal[MAX_THREADS];
	uint32 order[MAX_THREADS]; /* caller participant index -> thread */
	bool touched[MAX_THREADS][MAX_RELS][MAX_BLOCKS];
	Event event[MAX_EVENTS];
	Rel rel[MAX_RELS];
	bool check_mismatch;
	ClusterColdDiagV1 diag; /* of the last seal */
} World;

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
page_id(int rel, int block)
{
	RfPageIdentityV1 id;

	memset(&id, 0, sizeof(id));
	id.system_identifier = 99;
	memset(id.storage_uuid, 3, sizeof(id.storage_uuid));
	id.locator.spcOid = 1663;
	id.locator.dbOid = 5;
	id.locator.relNumber = REL_BASE + rel;
	id.forknum = MAIN_FORKNUM;
	id.blockno = (BlockNumber)block;
	return id;
}

static RfPageVersionV1
ver(uint8 inc, uint64 token)
{
	RfPageVersionV1 version;

	memset(&version, 0, sizeof(version));
	memset(version.segment_incarnation, inc, sizeof(version.segment_incarnation));
	version.mutation_token = token;
	return version;
}

static XLogRecPtr
rec_read(uint32 ordinal)
{
	return LSN_BASE + (XLogRecPtr)ordinal * LSN_STEP;
}

static bool
has_identity(uint8 ident)
{
	return ident != INC_NONE && ident != INC_TOMB;
}

static void
push_state(Rel *rel, int block, uint8 kind, uint64 token)
{
	Phys *state = &rel->hist[block][rel->nhist[block]++];

	memset(state, 0, sizeof(*state));
	state->kind = kind;
	state->token = token;
}

static const Phys *
current(const Rel *rel, int block)
{
	return &rel->hist[block][rel->nhist[block] - 1];
}

/* A file has no holes: a block below a later one reads as zeros. */
static void
normalize(Phys *slots)
{
	int top = -1;
	int b;

	for (b = 0; b < MAX_BLOCKS; b++)
		if (slots[b].kind != CLUSTER_COLD_DATA_ABSENT)
			top = b;
	for (b = 0; b < top; b++)
		if (slots[b].kind == CLUSTER_COLD_DATA_ABSENT) {
			memset(&slots[b], 0, sizeof(slots[b]));
			slots[b].kind = CLUSTER_COLD_DATA_UNFORMATTED;
		}
}

/* A torn write: verification catches it, or the header alone survives. */
static Phys
torn(const World *w, const Phys *header)
{
	Phys page = *header;

	if (w->checksums || header->kind != CLUSTER_COLD_DATA_PRESENT) {
		memset(&page, 0, sizeof(page));
		page.kind = CLUSTER_COLD_DATA_INVALID;
		return page;
	}
	page.torn = true;
	return page;
}

/* One page component: a change of an existing block, or an extension. */
static void
gen_change(World *w, uint32 e, int i, int r, int b)
{
	Event *ev = &w->event[e];
	Rel *rel = &w->rel[r];
	ClusterColdComponentV1 *c = &ev->components[i];
	bool extend = b >= (int)rel->size;

	memset(c, 0, sizeof(*c));
	c->page = page_id(r, b);
	c->block_id = (uint8)i;
	c->component_ordinal = (uint16)i;
	c->page_class = RF_PAGE_CLASS_ORDINARY;
	c->result_kind = RF_PAGE_STATE_PRESENT;
	c->result = ver(rel->inc, w->token);
	if (extend) {
		c->before_kind = RF_PAGE_STATE_ABSENT;
		c->edge_flags = RF_PAGE_EDGE_WILL_INIT | RF_PAGE_EDGE_FULL_COVERAGE;
		push_state(rel, b, CLUSTER_COLD_DATA_UNFORMATTED, 0);
		rel->size++;
	} else {
		c->before_kind = RF_PAGE_STATE_PRESENT;
		c->before = ver(rel->inc, current(rel, b)->token);
		/* full_page_writes: a thread's first change after its redo start */
		if ((!ev->history && !w->touched[ev->thread][r][b]) || (ev->history && rng(3) == 0))
			c->edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
	}
	if (!ev->history)
		w->touched[ev->thread][r][b] = true;
	push_state(rel, b, CLUSTER_COLD_DATA_PRESENT, w->token++);
	ev->rel[i] = r;
	ev->block[i] = b;
	if (ev->history) {
		rel->floor[b] = rel->nhist[b] - 1;
		rel->last_history[b] = (int)e;
	}
}

static const char payload_bytes[4] = { 'S', 'P', 'C', '1' };

static void
gen_space(World *w, uint32 e, int r, uint8 kind, uint32 nblocks)
{
	Event *ev = &w->event[e];
	Rel *rel = &w->rel[r];
	ClusterColdSpaceOpV1 *op = &ev->op;
	uint8 before = rel->inc;
	int b;

	ev->kind = EV_SPACE;
	memset(op, 0, sizeof(*op));
	op->kind = kind;
	op->locator = page_id(r, 0).locator;
	op->payload = payload_bytes;
	op->payload_length = sizeof(payload_bytes);
	switch (kind) {
	case CLUSTER_COLD_SPACE_CREATE:
	case CLUSTER_COLD_SPACE_TRUNCATE:
		rel->inc = (uint8)(16 + 32 * r + rel->incarnations++);
		if (kind == CLUSTER_COLD_SPACE_CREATE) {
			rel->alive = true;
			rel->size = 0;
			break;
		}
		memset(op->before_incarnation, before, 16);
		op->nblocks = nblocks;
		/* The truncation first flushes and syncs every fork (object
		 * checkpoint), SPACE pages included; then it shrinks. */
		rel->ident_floor = Max(rel->ident_floor, rel->nops);
		for (b = 0; b < MAX_BLOCKS; b++) {
			rel->floor[b] = Max(rel->floor[b], rel->nhist[b] - 1);
			if (b >= (int)nblocks && current(rel, b)->kind != CLUSTER_COLD_DATA_ABSENT)
				push_state(rel, b, CLUSTER_COLD_DATA_ABSENT, 0);
			ev->shrunk[b] = rel->nhist[b] - 1;
			if (b >= (int)nblocks)
				rel->retire_op[b] = (int)rel->nops;
			if (ev->history)
				rel->floor[b] = ev->shrunk[b];
		}
		rel->size = Min(rel->size, nblocks);
		rel->last_truncate = (int)e;
		break;
	case CLUSTER_COLD_SPACE_ADVANCE:
		break;
	case CLUSTER_COLD_SPACE_DROP:
		memset(op->before_incarnation, before, 16);
		rel->alive = false;
		rel->dropped = true;
		break;
	}
	if (kind != CLUSTER_COLD_SPACE_DROP)
		memset(op->result_incarnation, rel->inc, 16);
	ev->op_index = (int)rel->nops;
	rel->op_event[rel->nops] = (int)e;
	rel->ident[rel->nops + 1] = kind == CLUSTER_COLD_SPACE_DROP ? INC_TOMB : rel->inc;
	rel->nops++;
	if (ev->history)
		rel->ident_floor = rel->nops;
}

/* A second block for a two-component record, or -1 for none. */
static int
other_slot(const World *w, int r, int b, int *other_rel)
{
	int tries;

	for (tries = 0; tries < 6; tries++) {
		int r2 = (int)rng(w->rels);
		const Rel *rel = &w->rel[r2];
		int b2;

		if (!rel->alive || rel->size == 0)
			continue;
		b2 = (int)rng(rel->size);
		if (r2 != r || b2 != b) {
			*other_rel = r2;
			return b2;
		}
	}
	return -1;
}

static void
gen_event(World *w, uint32 e)
{
	Event *ev = &w->event[e];
	uint32 tries;

	for (tries = 0; tries < 64; tries++) {
		uint32 pick = rng(24);
		int r = (int)rng(w->rels);
		Rel *rel = &w->rel[r];

		if (!rel->alive) {
			if (!rel->dropped && rel->incarnations == 0 && pick < 12) {
				gen_space(w, e, r, CLUSTER_COLD_SPACE_CREATE, 0);
				return;
			}
			continue;
		}
		if (pick == 0 && rng(3) == 0)
			gen_space(w, e, r, CLUSTER_COLD_SPACE_DROP, 0);
		else if (pick < 4)
			gen_space(w, e, r, CLUSTER_COLD_SPACE_TRUNCATE, rng(rel->size + 1));
		else if (pick < 6)
			gen_space(w, e, r, CLUSTER_COLD_SPACE_ADVANCE, 0);
		else if (pick < 11 && rel->size < MAX_BLOCKS) {
			ev->kind = EV_PAGE;
			ev->count = 1;
			gen_change(w, e, 0, r, (int)rel->size);
		} else if (pick == 11)
			ev->kind = EV_PLAIN;
		else if (rel->size > 0) {
			int b = (int)rng(rel->size);
			int r2 = -1;
			int b2 = rng(2) == 0 ? other_slot(w, r, b, &r2) : -1;

			ev->kind = EV_PAGE;
			ev->count = b2 >= 0 ? 2 : 1;
			gen_change(w, e, 0, r, b);
			if (b2 >= 0)
				gen_change(w, e, 1, r2, b2);
		} else
			continue;
		return;
	}
	ev->kind = EV_PLAIN;
}

/*
 * After the timeline: a later durable write to a relation's file (history
 * of any generation) makes an earlier shrink of that file durable; inputs
 * whose incarnation a history change already ended are covered by DATA.
 */
static void
gen_finish_floors(World *w)
{
	uint32 e;
	uint32 later;
	int b;

	for (e = 0; e < w->events; e++) {
		Event *ev = &w->event[e];
		int r = ev->kind == EV_SPACE ? (int)ev->op.locator.relNumber - REL_BASE : -1;
		bool durable = false;

		if (r < 0 || ev->op.kind != CLUSTER_COLD_SPACE_TRUNCATE)
			continue;
		for (later = e + 1; later < w->events; later++) {
			const Event *next = &w->event[later];
			uint16 i;

			for (i = 0; next->kind == EV_PAGE && next->history && i < next->count; i++)
				durable |= next->rel[i] == r;
		}
		for (b = 0; durable && b < MAX_BLOCKS; b++)
			w->rel[r].floor[b] = Max(w->rel[r].floor[b], ev->shrunk[b]);
	}
	for (e = 0; e < w->events; e++) {
		Event *ev = &w->event[e];
		uint8 inc = ev->op.result_incarnation[0];

		if (ev->kind != EV_SPACE || ev->history)
			continue;
		ev->expect_input = true;
		for (later = 0; ev->op.kind != CLUSTER_COLD_SPACE_DROP && later < w->events; later++) {
			const Event *end = &w->event[later];

			if (end->kind == EV_SPACE && end->history
				&& end->op.locator.relNumber == ev->op.locator.relNumber
				&& (end->op.kind == CLUSTER_COLD_SPACE_TRUNCATE
					|| end->op.kind == CLUSTER_COLD_SPACE_DROP)
				&& end->op.before_incarnation[0] == inc)
				ev->expect_input = false;
		}
	}
}

/*
 * Pages with a replayable change that still matters (no later truncation
 * retired the block, the relation is not dropped), and whether an anchor
 * follows their last settled change (history, or durable before a later
 * truncation).
 */
static void
gen_finish_anchors(World *w)
{
	int retired[MAX_RELS][MAX_BLOCKS];
	uint32 e;
	int r;
	int b;

	for (r = 0; r < MAX_RELS; r++)
		for (b = 0; b < MAX_BLOCKS; b++)
			retired[r][b] = -1;
	for (e = 0; e < w->events; e++) {
		const Event *ev = &w->event[e];

		for (b = 0;
			 ev->kind == EV_SPACE && ev->op.kind == CLUSTER_COLD_SPACE_TRUNCATE && b < MAX_BLOCKS;
			 b++)
			if (b >= (int)ev->op.nblocks)
				retired[ev->op.locator.relNumber - REL_BASE][b] = (int)e;
	}
	for (e = 0; e < w->events; e++) {
		const Event *ev = &w->event[e];
		uint16 i;

		for (i = 0; ev->kind == EV_PAGE && !ev->history && i < ev->count; i++) {
			Rel *rel = &w->rel[ev->rel[i]];

			b = ev->block[i];
			if (rel->dropped || (int)e < retired[ev->rel[i]][b])
				continue;
			rel->replayable[b] = true;
			if ((int)e > Max(rel->last_history[b], rel->last_truncate))
				rel->anchored[b] |= ev->components[i].edge_flags != 0;
		}
	}
}

/* DATA a crash leaves: any state at or after each floor, maybe torn. */
static void
gen_disk(World *w)
{
	int r;
	int b;

	for (r = 0; r < (int)w->rels; r++) {
		Rel *rel = &w->rel[r];

		for (b = 0; b < MAX_BLOCKS; b++) {
			int pick = rel->floor[b] + (int)rng((uint32)(rel->nhist[b] - rel->floor[b]));
			bool written = false;
			int k;

			rel->disk[b] = rel->hist[b][pick];
			for (k = rel->floor[b] + 1; k < rel->nhist[b]; k++)
				written |= rel->hist[b][k].kind == CLUSTER_COLD_DATA_PRESENT;
			if (written && rng(8) == 0)
				rel->disk[b] = torn(w, &rel->disk[b]);
		}
		normalize(rel->disk);
		rel->space_pos = rel->ident_floor + rng(rel->nops - rel->ident_floor + 1);
	}
}

/* An empty timeline; sizes[r] > 0 makes relation r exist with that size. */
static void
world_begin(World *w, bool checksums, uint32 threads, uint32 rels, const uint32 *sizes)
{
	int r;

	memset(w, 0, sizeof(*w));
	w->token = 1000;
	w->checksums = checksums;
	w->threads = threads;
	w->rels = rels;
	for (r = 0; r < (int)rels; r++) {
		Rel *rel = &w->rel[r];
		int b;

		rel->last_truncate = -1;
		if (sizes[r] > 0) {
			rel->alive = true;
			rel->inc = (uint8)(16 + 32 * r + rel->incarnations++);
			rel->size = sizes[r];
			rel->ident[0] = rel->inc;
		}
		for (b = 0; b < MAX_BLOCKS; b++) {
			rel->last_history[b] = -1;
			rel->retire_op[b] = -1;
			push_state(rel, b,
					   b < (int)rel->size ? CLUSTER_COLD_DATA_PRESENT : CLUSTER_COLD_DATA_ABSENT,
					   b < (int)rel->size ? w->token++ : 0);
		}
	}
}

/* Close a timeline: floors, inputs and refusal bookkeeping. */
static void
world_finish(World *w)
{
	gen_finish_floors(w);
	gen_finish_anchors(w);
}

static void
generate(World *w)
{
	uint32 sizes[MAX_RELS];
	bool checksums = rng(2) == 0;
	uint32 threads = 2 + rng(MAX_THREADS - 1);
	uint32 rels = rng(4) == 0 ? 1 : MAX_RELS;
	uint32 e;
	int r;

	for (r = 0; r < MAX_RELS; r++)
		sizes[r] = r == 0 || rng(2) == 0 ? 1 + rng(MAX_BLOCKS) : 0;
	world_begin(w, checksums, threads, rels, sizes);
	w->events = w->threads + rng(MAX_EVENTS - w->threads);
	for (e = 0; e < w->events; e++) {
		Event *ev = &w->event[e];

		ev->thread = e < w->threads ? e : rng(w->threads);
		ev->ordinal = w->thread_records[ev->thread]++;
		ev->scn = rng(3) == 0 && e > 0 ? w->event[e - 1].scn : e + 1;
	}
	for (e = 0; e < w->threads; e++)
		w->redo_ordinal[e] = rng(w->thread_records[e] + 1);
	for (e = 0; e < w->events; e++) {
		w->event[e].history = w->event[e].ordinal < w->redo_ordinal[w->event[e].thread];
		gen_event(w, e);
	}
	world_finish(w);
	gen_disk(w);
}

#endif /* TEST_CLUSTER_COLD_RECOVERY_SPACE_GEN_H */
