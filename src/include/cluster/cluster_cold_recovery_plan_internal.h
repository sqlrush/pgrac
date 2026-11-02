/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_plan_internal.h
 *	  Private storage of the typed cold-crash replay plan.
 *
 *	  Shared only by the planner: cluster_cold_recovery_plan.c (input,
 *	  steps, lifetime), cluster_cold_recovery_plan_store.c (compact storage
 *	  and accounting), cluster_cold_recovery_plan_space.c (SPACE effects),
 *	  cluster_cold_recovery_plan_seal.c (chains, DATA placement, verdicts)
 *	  and cluster_cold_recovery_plan_schedule.c (replay order).  Callers
 *	  use cluster_cold_recovery.h.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cold_recovery_plan_internal.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_COLD_RECOVERY_PLAN_INTERNAL_H
#define CLUSTER_COLD_RECOVERY_PLAN_INTERNAL_H

#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_cold_relmap.h"
#include "cluster/cluster_relmap_retention.h"
#include "cluster/cluster_relmap_recovery_internal.h"
#include "cluster/cluster_control_native_files.h"

typedef struct ClusterColdNativeRegistryV2 {
	ClusterControlRelmapFormatV2 format;
	ClusterControlNativeInventoryV2 *inventory;
	ClusterColdRelmapCreateExpectedV1 *entries;
	uint64 count;
	Size bytes;
} ClusterColdNativeRegistryV2;

#ifdef USE_CLUSTER_UNIT
#define cold_alloc0(size_) calloc(1, (size_))
#define cold_realloc(pointer_, size_) realloc((pointer_), (size_))
#define cold_free(pointer_) free((pointer_))
#else
#define cold_alloc0(size_)                                                                         \
	palloc_extended((size_), MCXT_ALLOC_HUGE | MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO)
#define cold_realloc(pointer_, size_)                                                              \
	((pointer_) == NULL                                                                            \
		 ? palloc_extended((size_), MCXT_ALLOC_HUGE | MCXT_ALLOC_NO_OOM)                           \
		 : repalloc_extended((pointer_), (size_), MCXT_ALLOC_HUGE | MCXT_ALLOC_NO_OOM))
#define cold_free(pointer_) pfree(pointer_)
#endif

#define CLUSTER_COLD_PLAN_MAGIC UINT32_C(0x434f4c44)

typedef enum ColdPhase {
	COLD_PHASE_FEEDING = 1,
	COLD_PHASE_SEALED = 2,
	COLD_PHASE_FAILED = 3
} ColdPhase;

typedef struct ColdParticipant {
	ClusterColdParticipantV1 cut;
	uint32 input_index;
	bool seen;
	XLogRecPtr last_read;
	XLogRecPtr last_end;
	uint64 replay_records; /* fed records ending after native redo */
} ColdParticipant;

/*
 * Storage is compact: the page namespace (system identifier, storage UUID)
 * is plan-wide; a component names its page by an interned relation fork
 * and its block; both of its versions share one interned segment
 * incarnation (an ABSENT before-state has none); and expected state and
 * dependency are derived from chain links when a step is read.  Records
 * and components live in fixed-size chunks, so growth never doubles the
 * footprint.
 */
#define COLD_CHUNK_SHIFT 10
#define COLD_CHUNK_ENTRIES (UINT32_C(1) << COLD_CHUNK_SHIFT)
#define COLD_CHUNK_MASK (COLD_CHUNK_ENTRIES - 1)

typedef struct ColdRecord {
	XLogRecPtr read_rec_ptr;
	XLogRecPtr end_rec_ptr;
	uint64 scn;
	uint32 record_crc;
	TransactionId source_xid;
	TransactionId source_top_xid;
	uint32 first_component;
	uint8 participant; /* canonical index */
	uint8 component_count;
	uint8 rmid;
	uint8 info;
	uint8 flags; /* COLD_RECORD_* */
} ColdRecord;

/* Input order reuses planning records and separately retains omitted frames. */
typedef struct ColdInputFrameRef {
	uint32 index;
	bool planning_record;
} ColdInputFrameRef;

#define COLD_RECORD_HISTORY 0x01
#define COLD_RECORD_SCHEDULED 0x02
#define COLD_RECORD_RELMAP 0x10

typedef struct ColdRelmapInput {
	ClusterColdRelmapInputV1 input;
	bool captured;
	bool attempted;
	bool applied;
	/* Producer pin was armed before baseline I/O; explicit input has none. */
	bool has_retention_snapshot;
	ClusterRelmapRetentionSnapshot retention_snapshot;
} ColdRelmapInput;

/* Creation has no pending/terminal pin. Fixed segments use the SAME plan
 * budget, without imposing the legacy pending carrier's 64-entry array. */
#define COLD_RELMAP_CREATE_SEGMENT 16
#define COLD_RECORD_RELMAP_CREATE 0x20

typedef struct ColdRelmapCreateInput {
	ClusterColdRelmapCreateInputV1 input;
	bool captured;
	bool attempted;
	bool applied;
	void *receipt; /* Original executor's local confirmed receipt only. */
	Size receipt_bytes;
	void (*receipt_release)(void *);
	bool (*receipt_output_distinct)(const void *, const void *, Size);
} ColdRelmapCreateInput;
typedef struct ColdRelmapCreateChunk {
	struct ColdRelmapCreateChunk *next;
	ColdRelmapCreateInput entries[COLD_RELMAP_CREATE_SEGMENT];
} ColdRelmapCreateChunk;

/* Shared shape validation; caller has validated plan/count/storage. */
extern ClusterColdDetailV1 cold_relmap_inputs_valid(const ClusterColdPlanV1 *plan,
	const ClusterColdRelmapInputV1 *inputs, uint32 count);
#define COLD_RECORD_SPACE 0x04		/* first_component is its first SPACE effect */
#define COLD_RECORD_SPACE_STEP 0x08 /* CREATE, TRUNCATE or drop after a native redo start */

StaticAssertDecl(CLUSTER_COLD_MAX_PARTICIPANTS <= UINT8_MAX + 1,
				 "canonical participant index must fit ColdRecord");
StaticAssertDecl(CLUSTER_COLD_MAX_COMPONENTS <= UINT8_MAX, "component count must fit ColdRecord");

/* Interned relation fork; the plan-wide namespace completes the page. */
typedef struct ColdRelation {
	RelFileLocator locator;
	uint32 forknum;
} ColdRelation;

/* Interned segment incarnation of one relation fork. */
typedef struct ColdSegment {
	uint32 relation;
	uint8 incarnation[16];
} ColdSegment;

typedef struct ColdComponent {
	uint64 before_token; /* 0 unless the before-state is PRESENT */
	uint64 result_token;
	uint32 segment; /* (relation, result incarnation) */
	BlockNumber blockno;
	uint32 record;
	uint32 link; /* seal: chain predecessor; sealed APPLY: expected source */
	uint16 edge_flags;
	uint8 block_id;
	uint8 before_kind;
	uint8 verdict;
	uint8 state; /* COLD_STATE_* */
} ColdComponent;

/*
 * A sealed APPLY component expects the result of its link (the chain
 * predecessor, whose record it depends on) unless it is the first applied
 * component of its page.  That one expects DATA: with EXACT, the observed
 * state of kind DATA_KIND, whose version is link's result, or link's
 * before-state with DATA_BEFORE; without EXACT, any content (an anchor).
 */
#define COLD_STATE_FIRST_APPLY 0x01
#define COLD_STATE_EXACT 0x02
#define COLD_STATE_DATA_BEFORE 0x04
#define COLD_STATE_FINAL 0x08 /* sealed live PAGE frontier; readback duty only */
#define COLD_STATE_DATA_KIND_SHIFT 4
#define COLD_STATE_DATA_KIND_MASK 0x03
/* Set at seal from SPACE effects; never cleared by verdict assignment. */
#define COLD_STATE_DURABLE 0x40	   /* before a TRUNCATE of its relation (object checkpoint) */
#define COLD_STATE_IRRELEVANT 0x80 /* dropped, or retired by a TRUNCATE */

/*
 * One stored SPACE effect.  Effects in history are kept only when structural
 * (CREATE, TRUNCATE, DROP) and carry no payload; effects after a native redo
 * start are the SPACE owner's inputs and keep a copy of their payload.
 */
typedef struct ColdSpaceOp {
	void *payload;
	uint32 payload_length;
	uint32 record;
	RelFileLocator locator;
	BlockNumber nblocks;
	uint8 kind;			/* ClusterColdSpaceKindV1 */
	bool covered;		/* sealed: its incarnation's end is durable; not an input */
	uint8 shrink_forks; /* sealed TRUNCATE input: forks pass 2 shrinks again */
	uint8 before[16];
	uint8 result[16];
	uint32 relation;	/* sealed: index of its relation among SPACE inputs, or NO_INDEX */
	uint32 input;		/* sealed: position in the SPACE owner's order of those inputs */
	uint32 wait_record; /* sealed: the relation's previous SPACE step, or NO_INDEX */
} ColdSpaceOp;

/*
 * SPACE facts of one interned segment (relation fork, incarnation), valid
 * during seal.  A TRUNCATE or DROP ends a segment: its blocks at or past
 * retired_from (this or any later end) are gone, the rest were written
 * before the change was logged.
 */
typedef struct ColdSegmentSpace {
	uint32 creator_record;	  /* replay-range CREATE/TRUNCATE that made it, or NO_INDEX */
	BlockNumber retired_from; /* InvalidBlockNumber: none known to be retired */
	bool ended;
} ColdSegmentSpace;

/* Open-addressing intern table; slots hold entry index + 1. */
typedef struct ColdIntern {
	char *entries;
	uint32 count;
	uint32 capacity;
	uint32 *slots;
	uint32 slot_count; /* power of two */
} ColdIntern;

struct ClusterColdPlanV1 {
	uint32 magic;
	uint8 phase;
	bool namespace_known;
	uint64 max_scn; /* highest xl_scn of every accepted record */
	/* All retained JOINs must describe the same activation before replay. */
	uint64 stripe_floor_full;
	uint64 stripe_epoch;
	uint64 system_identifier;
	uint8 storage_uuid[16];
	uint32 participant_count;
	Size memory_budget;
	Size memory_used;
	ColdParticipant *participants; /* canonical order */
	uint32 *canonical;			   /* caller index -> canonical index */
	ColdRecord **record_chunks;
	uint32 record_count;
	uint32 record_chunk_count;
	uint32 record_chunk_capacity;
	ColdInputFrameRef **input_frame_chunks;
	uint32 input_frame_count;
	uint32 input_frame_chunk_count;
	uint32 input_frame_chunk_capacity;
	ClusterColdRecordFrameV1 **omitted_frame_chunks;
	uint32 omitted_frame_count;
	uint32 omitted_frame_chunk_count;
	uint32 omitted_frame_chunk_capacity;
	ColdComponent **component_chunks;
	uint32 component_count;
	uint32 component_chunk_count;
	uint32 component_chunk_capacity;
	ClusterColdSpaceCheckV1 space_check;
	void *space_check_arg;
	ColdIntern relations; /* ColdRelation */
	ColdIntern segments;  /* ColdSegment */
	ColdSpaceOp **space_chunks;
	uint32 space_count;
	uint32 space_chunk_count;
	uint32 space_chunk_capacity;
	/* SPACE views kept after seal: the SPACE owner's inputs per relation. */
	uint32 *space_inputs; /* replay-range effects, by relation then feed order */
	uint32 space_input_count;
	uint32 *space_relation_start; /* space_relation_count + 1 boundaries */
	uint32 space_relation_count;
	/* SPACE views used only during seal (cold_plan_space_release). */
	ColdSegmentSpace *segment_space; /* per interned segment */
	uint32 *space_creators;			 /* CREATE/TRUNCATE by (locator, result) */
	uint32 space_creator_count;
	uint32 *space_enders; /* TRUNCATE/DROP by (locator, before) */
	uint32 space_ender_count;
	Size space_scratch_bytes;
	uint32 *schedule;
	uint32 schedule_count;
	/* Only local FD/memory custody; never a debt/pin retirement callback. */
	void *relmap_terminal;
	Size relmap_terminal_bytes;
	void (*relmap_terminal_release)(void *);
	bool relmap_terminal_attempted;
	ClusterRelmapRecoveryPins *relmap_recovery;
	ColdRelmapInput *relmap_inputs;
	uint32 relmap_count;
	ColdRelmapCreateChunk *relmap_creates;
	uint32 relmap_create_count;
	bool relmap_failed;
	/* Original allocation extent; checked before pending input traversal. */
	uint32 relmap_input_capacity;
};

static inline ColdRelmapCreateInput *
cold_relmap_create_at(const ClusterColdPlanV1 *plan, uint32 index)
{
	ColdRelmapCreateChunk *chunk = plan->relmap_creates;
	if (index >= plan->relmap_create_count) return NULL;
	while (chunk != NULL && index >= COLD_RELMAP_CREATE_SEGMENT) {
		chunk = chunk->next;
		index -= COLD_RELMAP_CREATE_SEGMENT;
	}
	return chunk == NULL ? NULL : &chunk->entries[index];
}

static inline bool
cold_relmap_cut_equal(const ClusterColdParticipantV1 *a, const ClusterColdParticipantV1 *b)
{
	return a->thread_id == b->thread_id && a->reserved_zero == b->reserved_zero
		&& a->timeline == b->timeline && a->owner_incarnation == b->owner_incarnation
		&& a->physical_lower == b->physical_lower && a->native_redo == b->native_redo
		&& a->tail_end == b->tail_end;
}

static inline bool
cold_relmap_frame_equal(const ClusterColdRecordFrameV1 *a, const ClusterColdRecordV1 *b)
{
	return a->read_rec_ptr == b->read_rec_ptr && a->end_rec_ptr == b->end_rec_ptr
		&& a->record_crc == b->record_crc && a->rmid == b->rmid && a->info == b->info
		&& a->source_xid == b->source_xid && a->source_top_xid == b->source_top_xid
		&& a->page_components == b->component_count && a->space_effects == b->space_count;
}

static inline ColdRecord *
cold_record(const ClusterColdPlanV1 *plan, uint32 index)
{
	return &plan->record_chunks[index >> COLD_CHUNK_SHIFT][index & COLD_CHUNK_MASK];
}

static inline ColdComponent *
cold_component(const ClusterColdPlanV1 *plan, uint32 index)
{
	return &plan->component_chunks[index >> COLD_CHUNK_SHIFT][index & COLD_CHUNK_MASK];
}

static inline const ColdSegment *
cold_segment(const ClusterColdPlanV1 *plan, uint32 index)
{
	return &((const ColdSegment *)plan->segments.entries)[index];
}

static inline const ColdRelation *
cold_relation(const ClusterColdPlanV1 *plan, uint32 index)
{
	return &((const ColdRelation *)plan->relations.entries)[index];
}

/* Storage-only check at the original Start owner, before current providers.
 * It includes failed and never-captured plans; no receipt registry is needed. */
static inline bool
cold_relmap_storage_overlap(const void *out, Size bytes, const void *value, Size size)
{
	uintptr_t a = (uintptr_t)out, b = (uintptr_t)value;
	return value != NULL && size != 0 && (a <= b ? b - a < bytes : a - b < size);
}

static inline bool
cold_relmap_inputs_bounded(const ClusterColdPlanV1 *plan)
{
	return plan->relmap_count != UINT32_MAX
		&& plan->relmap_count == plan->relmap_input_capacity
		&& (plan->relmap_count == 0 ? plan->relmap_inputs == NULL : plan->relmap_inputs != NULL);
}

static inline bool
cold_relmap_output_distinct(const ClusterColdTypedV1 *typed, const void *out, Size bytes)
{
	const ClusterColdPlanV1 *plan;
	if (typed == NULL) return true;
	if (cold_relmap_storage_overlap(out, bytes, typed, sizeof(*typed))) return false;
	if (typed->native_registry != NULL) {
		const ClusterColdNativeRegistryV2 *s = typed->native_registry;
		if (cold_relmap_storage_overlap(out, bytes, s, sizeof(*s))
			|| cold_relmap_storage_overlap(out, bytes, s->entries,
				s->count * sizeof(*s->entries))
			|| (s->inventory && cold_relmap_storage_overlap(out, bytes, s->inventory,
				s->inventory->private_bytes))) return false;
	}
	plan = typed->plan;
	if (plan == NULL) return true;
	if (cold_relmap_storage_overlap(out, bytes, plan, sizeof(*plan))
		|| plan->magic != CLUSTER_COLD_PLAN_MAGIC
		|| !cold_relmap_inputs_bounded(plan)
		|| (plan->relmap_terminal != NULL && plan->relmap_terminal_bytes == 0))
		return false;
	for (const ColdRelmapCreateChunk *chunk = plan->relmap_creates;
		chunk != NULL; chunk = chunk->next) {
		if (cold_relmap_storage_overlap(out, bytes, chunk, sizeof(*chunk))) return false;
		for (uint32 i = 0; i < COLD_RELMAP_CREATE_SEGMENT; ++i) {
			const ColdRelmapCreateInput *entry = &chunk->entries[i];
			if (entry->receipt != NULL && (entry->receipt_output_distinct == NULL
				|| !entry->receipt_output_distinct(entry->receipt, out, bytes))) return false;
		}
	}
	return !cold_relmap_storage_overlap(out, bytes, plan->relmap_inputs,
			(Size)plan->relmap_count * sizeof(ColdRelmapInput))
		&& cluster_relmap_recovery_output_distinct(plan->relmap_recovery, out, bytes)
		&& !cold_relmap_storage_overlap(out, bytes, plan->relmap_terminal,
			plan->relmap_terminal_bytes);
}

/* Only local saved metadata is retired here. Original files, registrations
 * and recovery protections remain the original owner's responsibility. */
static inline void
cold_relmap_create_cleanup(ClusterColdPlanV1 *plan)
{
	for (uint32 i = 0; i < plan->relmap_create_count; ++i) {
		ColdRelmapCreateInput *entry = cold_relmap_create_at(plan, i);
		if (entry != NULL && entry->receipt != NULL && entry->receipt_release != NULL) {
			void *receipt = entry->receipt;
			void (*release)(void *) = entry->receipt_release;
			entry->receipt = NULL;
			entry->receipt_release = NULL;
			entry->receipt_output_distinct = NULL;
			entry->receipt_bytes = 0;
			plan->relmap_failed = true;
			release(receipt);
		}
	}
}

static inline void
cold_relmap_terminal_cleanup(ClusterColdPlanV1 *plan)
{
	if (plan != NULL && plan->magic == CLUSTER_COLD_PLAN_MAGIC
		&& plan->relmap_terminal_release != NULL) {
		void *receipt = plan->relmap_terminal;
		void (*release)(void *) = plan->relmap_terminal_release;
		plan->relmap_terminal = NULL;
		plan->relmap_terminal_bytes = 0;
		plan->relmap_terminal_release = NULL;
		plan->relmap_failed = true;
		release(receipt);
	}
}

static inline ColdSpaceOp *
cold_space_op(const ClusterColdPlanV1 *plan, uint32 index)
{
	return &plan->space_chunks[index >> COLD_CHUNK_SHIFT][index & COLD_CHUNK_MASK];
}

/* A SPACE record's effects are stored contiguously from first_component. */
static inline uint32
cold_record_space_count(const ClusterColdPlanV1 *plan, uint32 record)
{
	uint32 first = cold_record(plan, record)->first_component;
	uint32 count = 0;

	while (first + count < plan->space_count
		   && cold_space_op(plan, first + count)->record == record)
		count++;
	return count;
}

static inline bool
record_history(const ClusterColdPlanV1 *plan, uint32 record)
{
	return (cold_record(plan, record)->flags & COLD_RECORD_HISTORY) != 0;
}

static inline bool
bytes_nonzero(const uint8 *bytes, Size size)
{
	uint8 value = 0;
	Size i;

	for (i = 0; i < size; i++)
		value |= bytes[i];
	return value != 0;
}

static inline bool
version_present(const RfPageVersionV1 *version)
{
	return version->mutation_token != 0
		   && bytes_nonzero(version->segment_incarnation, sizeof(version->segment_incarnation));
}

static inline bool
version_equal(const RfPageVersionV1 *left, const RfPageVersionV1 *right)
{
	return left->mutation_token == right->mutation_token
		   && memcmp(left->segment_incarnation, right->segment_incarnation,
					 sizeof(left->segment_incarnation))
				  == 0;
}

static inline bool
incarnation_equal(const RfPageVersionV1 *left, const RfPageVersionV1 *right)
{
	return memcmp(left->segment_incarnation, right->segment_incarnation,
				  sizeof(left->segment_incarnation))
		   == 0;
}

static inline bool
plan_valid(const ClusterColdPlanV1 *plan)
{
	return plan != NULL && plan->magic == CLUSTER_COLD_PLAN_MAGIC;
}

/* cluster_cold_recovery_plan_store.c */
extern bool cold_plan_reserve(ClusterColdPlanV1 *plan, Size bytes);
extern void cold_plan_release(ClusterColdPlanV1 *plan, Size bytes);
extern void *cold_plan_scratch(ClusterColdPlanV1 *plan, Size bytes, Size *accounted);
extern ClusterColdDetailV1 cold_plan_chunk_reserve(ClusterColdPlanV1 *plan, void ***chunks,
												   uint32 *chunk_count, uint32 *chunk_capacity,
												   uint64 required, Size element);
extern ClusterColdDetailV1 cold_plan_intern(ClusterColdPlanV1 *plan, ColdIntern *intern,
											const void *key, Size key_size, uint32 *out);
extern bool cold_plan_intern_find(const ColdIntern *intern, const void *key, Size key_size,
								  uint32 *out);
extern void cold_plan_intern_free(ColdIntern *intern);
extern RfPageVersionV1 cold_plan_component_result(const ClusterColdPlanV1 *plan,
												  const ColdComponent *component);
extern RfPageVersionV1 cold_plan_component_before(const ClusterColdPlanV1 *plan,
												  const ColdComponent *component);
extern bool cold_plan_incarnation_is(const ClusterColdPlanV1 *plan, const ColdComponent *component,
									 const RfPageVersionV1 *version);
extern RfPageIdentityV1 cold_plan_component_page(const ClusterColdPlanV1 *plan,
												 const ColdComponent *component);

/* cluster_cold_recovery_plan_schedule.c */
extern ClusterColdDetailV1 cold_plan_schedule(ClusterColdPlanV1 *plan, ClusterColdDiagV1 *diag);

static inline void
cold_diag_record(const ClusterColdPlanV1 *plan, ClusterColdDiagV1 *diag, uint32 record)
{
	const ColdRecord *stored = cold_record(plan, record);

	diag->has_record = true;
	diag->participant = plan->participants[stored->participant].input_index;
	diag->read_rec_ptr = stored->read_rec_ptr;
}

static inline void
cold_diag_component(const ClusterColdPlanV1 *plan, ClusterColdDiagV1 *diag, uint32 component)
{
	const ColdComponent *stored = cold_component(plan, component);

	cold_diag_record(plan, diag, stored->record);
	diag->has_page = true;
	diag->page = cold_plan_component_page(plan, stored);
}

/* cluster_cold_recovery_plan_space.c */
extern ClusterColdDetailV1 cold_plan_space_seal(ClusterColdPlanV1 *plan, ClusterColdDiagV1 *diag);
extern void cold_plan_space_release(ClusterColdPlanV1 *plan);
extern uint32 cold_plan_space_carried_from(const ClusterColdPlanV1 *plan, uint32 segment,
										   BlockNumber blockno);
extern bool cold_plan_space_lineage(const ClusterColdPlanV1 *plan, const ColdComponent *component,
									const uint8 *incarnation);
extern bool cold_plan_space_retired_start(const ClusterColdPlanV1 *plan,
										  const ColdComponent *component);
extern bool cold_plan_space_shrink_pending(const ClusterColdPlanV1 *plan,
										   const ColdComponent *component);
extern bool cold_plan_space_created_here(const ClusterColdPlanV1 *plan,
										 const ColdComponent *component);
extern void cold_plan_space_retire_inferred(const ClusterColdPlanV1 *plan, const uint32 *group,
											uint32 count);

/* History, or written before a TRUNCATE of its relation: never replayed. */
static inline bool
component_settled(const ClusterColdPlanV1 *plan, const ColdComponent *component)
{
	return record_history(plan, component->record) || (component->state & COLD_STATE_DURABLE) != 0;
}

#endif /* CLUSTER_COLD_RECOVERY_PLAN_INTERNAL_H */
