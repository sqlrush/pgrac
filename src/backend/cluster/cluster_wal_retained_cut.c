/*-------------------------------------------------------------------------
 *
 * cluster_wal_retained_cut.c
 *	  Per-thread physical retention lower from one census of the complete
 *	  retained WAL input (S07).
 *
 *	  Each selected source has a completion position.  The current writer
 *	  of an OPEN or RECOVERY_REQUIRED root completes at its same-token
 *	  native redo; a RECOVERY_COMPLETE or CLOSED generation completes at its
 *	  validated tail; a checkpoint-less initializer terminal completes at
 *	  its first record, so all of it stays an obligation.  A record ending
 *	  at or before its source's completion is history, the rest are
 *	  obligations (a record crossing the completion is refused).
 *
 *	  A history record is still needed when an obligation touches the same
 *	  page or SPACE block (the PAGE dependency rule of the online plan, see
 *	  rf_page_online_plan_dependency_prefix_v1), or when it carries a SIDE
 *	  owner class that has an obligation and no per-key ancestry yet.  The
 *	  keys of obligations go into a fixed-size minimum sketch; collisions
 *	  only make the answer more conservative.  History edges are spooled to
 *	  a temporary file and compared after the census.  A source's bound is
 *	  the earliest needed history record, or its completion.
 *
 *	  Nothing here publishes or retires WAL.  The ROOT owner publishes the
 *	  bound only if the ROOT is unchanged since the census
 *	  (cluster_control_root_v3_retained_lower_publish); WAL cleanup reads the
 *	  published ROOT afterwards.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_wal_retained_cut.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlogreader.h"
#include "cluster_control_root_private.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_wal_claim.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_wal_retained_cut.h"
#include "cluster/cluster_wal_thread.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/buf_internals.h"
#include "storage/buffile.h"

/* 2^19 buckets of 8 bytes: 4 MiB, independent of the database size. */
#define RETAINED_SKETCH_BITS 19
#define RETAINED_SKETCH_BUCKETS (UINT32_C(1) << RETAINED_SKETCH_BITS)
#define RETAINED_SKETCH_EMPTY UINT64_MAX
#define RETAINED_BATCH 256
#define RETAINED_SIDE_CLASSES 9

/* SIDE owners whose ancestry is not keyed yet.  SPACE contributes PCM
 * BufferTags and is keyed like a page; NATIVE_CONTROL records are consumed
 * as typed control observations and never need an older record. */
#define RETAINED_SIDE_KEYLESS                                                                      \
	(RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_UNDO_BLOCK                            \
	 | RF_SIDE_CONTRIBUTION_TERMINAL | RF_SIDE_CONTRIBUTION_PREPARED | RF_SIDE_CONTRIBUTION_CLOG   \
	 | RF_SIDE_CONTRIBUTION_MULTIXACT | RF_SIDE_CONTRIBUTION_COMMIT_TS)

typedef struct RetainedSource {
	ClusterWalSourceRef ref;
	XLogRecPtr completion;
	XLogRecPtr bound;
	/* Earliest history record start per SIDE owner class. */
	XLogRecPtr side_first[RETAINED_SIDE_CLASSES];
	bool page_pinned;
	bool side_pinned;
} RetainedSource;

/* One history edge: a page or SPACE block a durable record changed. */
typedef struct RetainedEdge {
	uint64 key;
	uint64 result_token;
	XLogRecPtr read_ptr;
	uint32 source;
	uint32 reserved_zero;
} RetainedEdge;

typedef struct RetainedCutWork {
	uint32 nsources;
	int32 self;
	RetainedSource sources[CLUSTER_WAL_INPUTS_MAX];
	uint64 *sketch;
	uint32 obligation_side;
	RetainedEdge batch[RETAINED_BATCH];
	uint32 batch_count;
	BufFile *spool;
	uint64 spooled;
	uint64 records;
	uint64 history_edges;
	uint64 retained_edges;
	uint32 last_source;
	/* Classification of the record being decoded. */
	uint32 current_source;
	bool current_history;
	XLogRecPtr current_read;
	XLogRecPtr current_end;
	RfPageProofDetailV1 detail;
} RetainedCutWork;

static bool retained_logged;
static ClusterControlRootResult retained_logged_result;
static RfPageProofDetailV1 retained_logged_detail;
static ClusterWalRetainedPinV1 retained_logged_pin;

/*
 * Hash of one page or SPACE block.  BufferTag has no padding and the
 * incarnation is deliberately not part of the key: edges of different
 * incarnations of the same block share it, which only retains more.
 */
static uint64
retained_key(const RelFileLocator *locator, ForkNumber forknum, BlockNumber blockno)
{
	BufferTag tag;

	InitBufferTag(&tag, locator, forknum, blockno);
	return hash_bytes_extended((const unsigned char *)&tag, sizeof(tag), UINT64CONST(0x5307));
}

static inline uint32
retained_bucket(uint64 key, int which)
{
	uint32 h = which == 0 ? (uint32)key : (uint32)(key >> 32);

	return h & (RETAINED_SKETCH_BUCKETS - 1);
}

/* Remember the earliest before-version token of an obligation on key. */
static void
retained_sketch_add(uint64 *sketch, uint64 key, uint64 before_token)
{
	for (int i = 0; i < 2; i++) {
		uint64 *bucket = &sketch[retained_bucket(key, i)];

		if (before_token < *bucket)
			*bucket = before_token;
	}
}

/*
 * A lower bound of the earliest obligation before-token on key, or EMPTY
 * when no obligation can be on it.  Each bucket holds the minimum of every
 * key hashed to it, hence the larger of the two is still <= the true value.
 */
static uint64
retained_sketch_query(const uint64 *sketch, uint64 key)
{
	return Max(sketch[retained_bucket(key, 0)], sketch[retained_bucket(key, 1)]);
}

/*
 * PAGE dependency rule (approved CR16 semantics, as in
 * rf_page_online_plan_dependency_prefix_v1): while any edge of a page is an
 * obligation, every edge of that page is retained.
 */
static inline bool
retained_edge_needed(uint64 obligation_before, uint64 result_token pg_attribute_unused())
{
	return obligation_before != RETAINED_SKETCH_EMPTY;
}

static void
retained_spill(RetainedCutWork *work)
{
	if (work->batch_count == 0)
		return;
	if (work->spool == NULL)
		work->spool = BufFileCreateTemp(false);
	BufFileWrite(work->spool, work->batch, sizeof(work->batch[0]) * work->batch_count);
	work->spooled += work->batch_count;
	work->batch_count = 0;
}

static void
retained_history_edge(RetainedCutWork *work, uint64 key, uint64 result_token)
{
	RetainedEdge *edge;

	if (work->batch_count == RETAINED_BATCH)
		retained_spill(work);
	edge = &work->batch[work->batch_count++];
	memset(edge, 0, sizeof(*edge));
	edge->key = key;
	edge->result_token = result_token;
	edge->read_ptr = work->current_read;
	edge->source = work->current_source;
	work->history_edges++;
}

static void
retained_key_seen(RetainedCutWork *work, uint64 key, uint64 before_token, uint64 result_token)
{
	if (work->current_history)
		retained_history_edge(work, key, result_token);
	else
		retained_sketch_add(work->sketch, key, before_token);
}

static bool
retained_space(void *arg, const RfSideSpaceContributionV1 *space)
{
	RetainedCutWork *work = arg;

	for (uint8 block = 0; block < 2; block++)
		if ((space->page_mask & (1u << block)) != 0) {
			/* A SPACE contribution has no before token: an obligation keeps
			 * every history edge of the block, whatever the rule. */
			retained_key_seen(work, retained_key(&space->result.key.locator, SPACE_FORKNUM, block),
							  0, space->result_token[block]);
		}
	return true;
}

static void
retained_side(RetainedCutWork *work, uint32 owners)
{
	RetainedSource *source = &work->sources[work->current_source];

	owners &= RETAINED_SIDE_KEYLESS;
	if (!work->current_history) {
		work->obligation_side |= owners;
		return;
	}
	for (int c = 0; c < RETAINED_SIDE_CLASSES; c++)
		if ((owners & (1u << c)) != 0
			&& (source->side_first[c] == InvalidXLogRecPtr
				|| work->current_read < source->side_first[c]))
			source->side_first[c] = work->current_read;
}

/* The same routing policy as the online contribution census
 * (cluster_thread_recovery_record_census_v1): typed SIDE records and logical
 * no-ops are their own owners, SIDE components inside PAGE records are
 * refused, only FSM pages are rebuildable. */
static RfPageProofDetailV1
retained_preflight_side_record(void *arg pg_attribute_unused(), const RfOpcodeRouteV1 *route,
							   const RfPageVersionEdgeEntryV1 *edge, const DecodedBkpBlock *block)
{
	return route != NULL
				   && (route->record_owner == RF_ROUTE_OWNER_SIDE_TYPED
					   || route->record_owner == RF_ROUTE_OWNER_LOGICAL_NOOP)
				   && edge == NULL && block == NULL
			   ? RF_PAGE_PROOF_DETAIL_OK
			   : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

static RfPageProofDetailV1
retained_preflight_side_component(void *arg pg_attribute_unused(),
								  const RfOpcodeRouteV1 *route pg_attribute_unused(),
								  const RfPageVersionEdgeEntryV1 *edge pg_attribute_unused(),
								  const DecodedBkpBlock *block pg_attribute_unused())
{
	return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

static RfPageProofDetailV1
retained_preflight_rebuildable(void *arg pg_attribute_unused(), const RfOpcodeRouteV1 *route,
							   const RfPageVersionEdgeEntryV1 *edge, const DecodedBkpBlock *block)
{
	return route != NULL && route->record_owner == RF_ROUTE_OWNER_PAGE_CODEC && edge != NULL
				   && block != NULL && edge->page_class == RF_PAGE_CLASS_REBUILDABLE_FSM
			   ? RF_PAGE_PROOF_DETAIL_OK
			   : RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN;
}

/* Field comparison: the reference has trailing padding. */
static bool
retained_source_equal(const ClusterWalSourceRef *a, const ClusterWalSourceRef *b)
{
	return memcmp(&a->claim, &b->claim, sizeof(a->claim)) == 0 && a->timeline == b->timeline;
}

/* Index of the selected source a census record came from. */
static bool
retained_source_index(RetainedCutWork *work, const ClusterWalSourceRef *source, uint32 *out)
{
	if (work->last_source < work->nsources
		&& retained_source_equal(&work->sources[work->last_source].ref, source)) {
		*out = work->last_source;
		return true;
	}
	for (uint32 i = 0; i < work->nsources; i++)
		if (retained_source_equal(&work->sources[i].ref, source)) {
			work->last_source = *out = i;
			return true;
		}
	return false;
}

static RfPageProofDetailV1
retained_classify(RetainedCutWork *work, XLogReaderState *record, const ClusterWalSourceRef *source,
				  const RfContributorStreamCutV1 *cut)
{
	const RetainedSource *selected;

	if (record->EndRecPtr <= record->ReadRecPtr
		|| source->claim.identity.origin_owner_incarnation != cut->origin_owner_incarnation
		|| source->claim.identity.origin_thread_id != cut->failed_thread
		|| source->timeline != cut->timeline_id
		|| !retained_source_index(work, source, &work->current_source))
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	selected = &work->sources[work->current_source];
	work->current_read = record->ReadRecPtr;
	work->current_end = record->EndRecPtr;
	if (record->EndRecPtr <= selected->completion)
		work->current_history = true;
	else if (record->ReadRecPtr >= selected->completion)
		work->current_history = false;
	else
		return RF_PAGE_PROOF_DETAIL_SOURCE_GAP; /* completion inside a record */
	return RF_PAGE_PROOF_DETAIL_OK;
}

static RfPageProofDetailV1
retained_record(XLogReaderState *record, const ClusterWalSourceRef *source,
				const RfContributorStreamCutV1 *cut, void *arg)
{
	RetainedCutWork *work = arg;
	RfDetachedOwnerOpsV1 ops = { 0 };
	RfDetachedRecordPlanV1 plan;
	RfPageOnlineRecordIdentityV1 identity = { 0 };
	RfSideContributionOwnersV1 owners;
	const DecodedXLogRecord *decoded;
	RfPageProofDetailV1 detail;

	if ((work->records++ % 1024) == 0)
		CHECK_FOR_INTERRUPTS();
	detail = retained_classify(work, record, source, cut);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return work->detail = detail;
	ops.preflight_side_record = retained_preflight_side_record;
	ops.preflight_side_component = retained_preflight_side_component;
	ops.preflight_rebuildable_component = retained_preflight_rebuildable;
	detail = rf_page_detached_preflight_v1(record, true, &ops, &plan);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return work->detail = detail;
	decoded = record->record;
	identity.record.system_identifier = source->claim.identity.system_identifier;
	memcpy(identity.record.storage_uuid, source->claim.identity.storage_uuid, 16);
	identity.record.origin_thread = cut->failed_thread;
	identity.record.timeline_id = cut->timeline_id;
	identity.record.read_rec_ptr = record->ReadRecPtr;
	identity.record.end_rec_ptr = record->EndRecPtr;
	identity.record.record_crc = (uint32)decoded->header.xl_crc;
	identity.record.rmid = decoded->header.xl_rmid;
	identity.record.info = decoded->header.xl_info;
	detail = rf_side_record_census_v1(&plan, &identity, cut, source->claim.database_incarnation,
									  retained_space, work, &owners);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return work->detail = detail;
	retained_side(work, owners.owners);
	for (uint32 i = 0; i < plan.component_count; i++) {
		const RfDetachedComponentPlanV1 *component = &plan.components[i];
		const DecodedBkpBlock *block = &decoded->blocks[component->block_id];

		if (component->owner == RF_DETACHED_COMPONENT_REBUILDABLE)
			continue;
		if (component->owner != RF_DETACHED_COMPONENT_PAGE_CODEC
			|| component->page_class != RF_PAGE_CLASS_ORDINARY || plan.result_token == 0)
			return work->detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		/* An unformatted or absent before-state is the start of the chain. */
		retained_key_seen(
			work, retained_key(&block->rlocator, block->forknum, block->blkno),
			component->before_kind == RF_PAGE_STATE_PRESENT ? component->before.mutation_token : 0,
			plan.result_token);
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

/* Completion of each selected input; see the file header. */
static bool
retained_sources(RetainedCutWork *work, ClusterWalInputsV1 *inputs, const ClusterWalSourceRef *self)
{
	uint32 count = cluster_wal_inputs_count_v1(inputs);

	if (count == 0 || count > CLUSTER_WAL_INPUTS_MAX)
		return false;
	work->self = -1;
	for (uint32 i = 0; i < count; i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(inputs, i);
		RetainedSource *source = &work->sources[i];

		if (item == NULL)
			return false;
		source->ref = item->source;
		if (item->kind == CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL)
			source->completion = item->first_segment + SizeOfXLogLongPHD;
		else if (item->kind != CLUSTER_WAL_INPUT_CHECKPOINT)
			return false;
		else if (item->checkpoint.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
				 || item->checkpoint.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED)
			source->completion = item->native_redo;
		else if (item->checkpoint.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
				 || item->checkpoint.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED)
			source->completion = item->checkpoint.validated_tail_lsn_exclusive;
		else
			return false;
		if (source->completion == InvalidXLogRecPtr)
			return false;
		source->bound = source->completion;
		/* The writer's own reference may name an older configuration
		 * generation than the ROOT; its claim and timeline are the identity. */
		if (item->kind == CLUSTER_WAL_INPUT_CHECKPOINT && item->current
			&& item->checkpoint.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
			&& memcmp(&item->source.claim.identity, &self->claim.identity,
					  sizeof(self->claim.identity))
				   == 0
			&& memcmp(item->source.claim.claim_sha256, self->claim.claim_sha256, 32) == 0
			&& item->source.timeline == self->timeline) {
			if (work->self >= 0 || item->native_redo < item->checkpoint.checkpoint_lower_lsn)
				return false;
			work->self = (int32)i;
		}
	}
	work->nsources = count;
	return work->self >= 0;
}

/* Fold the spooled history edges and SIDE classes into each source bound. */
static void
retained_fold(RetainedCutWork *work)
{
	RetainedEdge edge;
	uint64 remaining;

	if (work->spool != NULL) {
		retained_spill(work);
		if (BufFileSeek(work->spool, 0, 0, SEEK_SET) != 0)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not rewind the retained WAL census spool")));
	}
	remaining = work->spool != NULL ? work->spooled : work->batch_count;
	for (uint64 i = 0; i < remaining; i++) {
		RetainedSource *source;

		if ((i % 4096) == 0)
			CHECK_FOR_INTERRUPTS();
		if (work->spool != NULL)
			BufFileReadExact(work->spool, &edge, sizeof(edge));
		else
			edge = work->batch[i];
		if (!retained_edge_needed(retained_sketch_query(work->sketch, edge.key), edge.result_token))
			continue;
		source = &work->sources[edge.source];
		work->retained_edges++;
		if (edge.read_ptr < source->bound) {
			source->bound = edge.read_ptr;
			source->page_pinned = true;
		}
	}
	for (uint32 s = 0; s < work->nsources; s++) {
		RetainedSource *source = &work->sources[s];

		for (int c = 0; c < RETAINED_SIDE_CLASSES; c++)
			if ((work->obligation_side & (1u << c)) != 0
				&& source->side_first[c] != InvalidXLogRecPtr
				&& source->side_first[c] < source->bound) {
				source->bound = source->side_first[c];
				source->side_pinned = true;
				source->page_pinned = false;
			}
	}
}

static ClusterControlRootResult
retained_census(RetainedCutWork *work, ClusterWalInputsV1 *inputs, const ClusterWalSourceRef *self,
				ClusterWalRetainedCutV1 *out, RfPageProofDetailV1 *detail)
{
	const ClusterWalInputV1 *item;
	RetainedSource *source;
	uint64 records = 0;
	ClusterControlRootResult result;

	if (!retained_sources(work, inputs, self))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	work->sketch = palloc(sizeof(uint64) * RETAINED_SKETCH_BUCKETS);
	memset(work->sketch, 0xff, sizeof(uint64) * RETAINED_SKETCH_BUCKETS);
	result = cluster_wal_inputs_census_v1(inputs, retained_record, work, &records, detail);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (*detail == RF_PAGE_PROOF_DETAIL_OK)
			*detail = work->detail;
		return result;
	}
	retained_fold(work);
	result = cluster_wal_inputs_revalidate_v1(inputs);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_wal_inputs_root_token_v1(inputs, &out->root_token);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	item = cluster_wal_inputs_at_v1(inputs, (uint32)work->self);
	source = &work->sources[work->self];
	out->old_lower = item->checkpoint.checkpoint_lower_lsn;
	out->native_redo = item->native_redo;
	/* A bound below the published lower is history this thread no longer
	 * retains: keep the published lower (never move it back). */
	out->lower = Max(source->bound, out->old_lower);
	out->records = work->records;
	out->history_edges = work->history_edges;
	out->retained_edges = work->retained_edges;
	out->side_classes = work->obligation_side;
	out->pin = source->side_pinned	 ? CLUSTER_WAL_RETAINED_PIN_SIDE
			   : source->page_pinned ? CLUSTER_WAL_RETAINED_PIN_PAGE
									 : CLUSTER_WAL_RETAINED_PIN_NONE;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static void
retained_work_release(RetainedCutWork *work, ClusterWalInputsV1 **inputs)
{
	if (work->spool != NULL) {
		BufFileClose(work->spool);
		work->spool = NULL;
	}
	if (work->sketch != NULL) {
		pfree(work->sketch);
		work->sketch = NULL;
	}
	cluster_wal_inputs_release_v1(inputs);
}

ClusterControlRootResult
cluster_wal_retained_cut_compute_v1(const ClusterWalSourceRef *self, ClusterWalRetainedCutV1 *out,
									RfPageProofDetailV1 *detail)
{
	ClusterWalInputsV1 *inputs = NULL;
	RetainedCutWork *work;
	ClusterControlRootResult result;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (detail != NULL)
		*detail = RF_PAGE_PROOF_DETAIL_OK;
	if (self == NULL || out == NULL || detail == NULL
		|| !cluster_wal_claim_v2_ref_valid(&self->claim))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_wal_inputs_begin_v1(self->claim.identity.storage_uuid,
										 self->claim.identity.system_identifier, &inputs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work = palloc0(sizeof(*work));
	PG_TRY();
	{
		result = retained_census(work, inputs, self, out, detail);
	}
	PG_CATCH();
	{
		retained_work_release(work, &inputs);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	retained_work_release(work, &inputs);
	pfree(work);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(out, 0, sizeof(*out));
	return result;
}

/* Log a refusal or a pinned bound once per distinct reason. */
static void
retained_report(ClusterControlRootResult result, RfPageProofDetailV1 detail,
				ClusterWalRetainedPinV1 pin, const ClusterWalRetainedCutV1 *cut)
{
	if (retained_logged && retained_logged_result == result && retained_logged_detail == detail
		&& retained_logged_pin == pin)
		return;
	retained_logged = true;
	retained_logged_result = result;
	retained_logged_detail = detail;
	retained_logged_pin = pin;
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		ereport(LOG,
				(errmsg("cluster WAL retention lower not advanced"),
				 errdetail("Retained-input census or ROOT publication result %d, proof detail %d. "
						   "The published lower is kept.",
						   (int)result, (int)detail)));
	else if (pin != CLUSTER_WAL_RETAINED_PIN_NONE)
		ereport(LOG, (errmsg("cluster WAL retention lower held by %s ancestry",
							 pin == CLUSTER_WAL_RETAINED_PIN_SIDE ? "SIDE" : "page"),
					  errdetail("Lower %X/%X, native redo %X/%X, SIDE owners 0x%x, %llu of %llu "
								"history edges needed.",
								LSN_FORMAT_ARGS(cut->lower), LSN_FORMAT_ARGS(cut->native_redo),
								cut->side_classes, (unsigned long long)cut->retained_edges,
								(unsigned long long)cut->history_edges)));
}

void
cluster_wal_retained_cut_after_checkpoint_v1(void)
{
	ClusterWalSourceRef self;
	ClusterWalRetainedCutV1 cut;
	ClusterControlRootSnapshot published;
	RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_OK;
	ClusterControlRootResult result;

	if (!AmCheckpointerProcess() || !cluster_enabled || !cluster_shared_config
		|| ShutdownRequestPending || CritSectionCount != 0
		|| !cluster_wal_thread_current_v2_ref(&self))
		return;
	result = cluster_wal_retained_cut_compute_v1(&self, &cut, &detail);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && cut.lower > cut.old_lower)
		result = cluster_control_root_v3_retained_lower_publish(
			&self.claim.identity, &cut.root_token, cut.native_redo, cut.lower, &published);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && cut.lower > cut.old_lower)
		ereport(DEBUG1, (errmsg("cluster WAL retention lower advanced from %X/%X to %X/%X",
								LSN_FORMAT_ARGS(cut.old_lower), LSN_FORMAT_ARGS(cut.lower))));
	retained_report(result, detail, result == CLUSTER_CONTROL_ROOT_OK_PRIMARY ? cut.pin : 0, &cut);
}

#endif /* USE_PGRAC_CLUSTER */
