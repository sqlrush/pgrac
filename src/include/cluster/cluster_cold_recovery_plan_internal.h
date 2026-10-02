/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_plan_internal.h
 *	  Private storage of the typed cold-crash replay plan.
 *
 *	  Shared only by the planner: cluster_cold_recovery_plan.c (input,
 *	  steps, lifetime), cluster_cold_recovery_plan_store.c (compact storage
 *	  and accounting) and cluster_cold_recovery_plan_seal.c (chains, DATA
 *	  placement, verdicts, schedule).  Callers use cluster_cold_recovery.h.
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
	uint32 first_component;
	uint8 participant; /* canonical index */
	uint8 component_count;
	uint8 rmid;
	uint8 info;
	uint8 flags; /* COLD_RECORD_* */
} ColdRecord;

#define COLD_RECORD_HISTORY 0x01
#define COLD_RECORD_SCHEDULED 0x02

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
#define COLD_STATE_DATA_KIND_SHIFT 4
#define COLD_STATE_DATA_KIND_MASK 0x03

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
	ColdComponent **component_chunks;
	uint32 component_count;
	uint32 component_chunk_count;
	uint32 component_chunk_capacity;
	ColdIntern relations; /* ColdRelation */
	ColdIntern segments;  /* ColdSegment */
	uint32 *schedule;
	uint32 schedule_count;
};

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
extern ClusterColdDetailV1 cold_plan_chunk_reserve(ClusterColdPlanV1 *plan, void ***chunks,
												   uint32 *chunk_count, uint32 *chunk_capacity,
												   uint64 required, Size element);
extern ClusterColdDetailV1 cold_plan_intern(ClusterColdPlanV1 *plan, ColdIntern *intern,
											const void *key, Size key_size, uint32 *out);
extern void cold_plan_intern_free(ColdIntern *intern);
extern RfPageVersionV1 cold_plan_component_result(const ClusterColdPlanV1 *plan,
												  const ColdComponent *component);
extern RfPageVersionV1 cold_plan_component_before(const ClusterColdPlanV1 *plan,
												  const ColdComponent *component);
extern bool cold_plan_incarnation_is(const ClusterColdPlanV1 *plan, const ColdComponent *component,
									 const RfPageVersionV1 *version);
extern RfPageIdentityV1 cold_plan_component_page(const ClusterColdPlanV1 *plan,
												 const ColdComponent *component);

#endif /* CLUSTER_COLD_RECOVERY_PLAN_INTERNAL_H */
