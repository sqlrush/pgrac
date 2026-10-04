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
 *	  On a page or SPACE block, the obligations' earliest before-version is
 *	  durable on disk (every earlier edge is history of a completed source),
 *	  so recovery either applies that obligation to it or proves that the
 *	  disk version descends from it through the later edges.  A history
 *	  edge is therefore needed only if its result is newer than that
 *	  before-version, or if it belongs to another segment incarnation than
 *	  every obligation on the block (PU-D-7; this narrows
 *	  rf_page_online_plan_dependency_prefix_v1, which keeps every edge of
 *	  such a page).  Versions are compared in SCN total order and every edge
 *	  must strictly advance its page (result after before), otherwise the
 *	  census refuses.  SIDE records are keyed and classified in
 *	  cluster_wal_retained_side.c.  A history record that ends a relation
 *	  incarnation (TRUNCATE, a DROP tombstone, the dropped relations of a
 *	  COMMIT or an ABORT) is always needed: its structural PI responsibility has no durable retirement
 *	  receipt yet (CR20), so its WAL is never released.  A CREATE starts an
 *	  incarnation and has no such responsibility.
 *
 *	  Native records the typed SIDE decoder does not own yet are classified
 *	  from their own format there too.  The obligations go into a fixed-size sketch keeping, per bucket, the
 *	  earliest before-version and the range of incarnations; collisions
 *	  only make the answer more conservative.  History edges are spooled to
 *	  a temporary file and compared after the census.  A source's bound is
 *	  the earliest needed history record, or its completion.
 *
 *	  This thread also completes no later than the first record of any of its
 *	  unretired local PI responsibilities: a past image handed away before
 *	  its version was written keeps its redo until the PI is retired, like a
 *	  PI holding back an Oracle instance checkpoint.  The directory is read
 *	  after the checkpoint completed; a responsibility recorded later with an
 *	  earlier first record names a version that checkpoint already wrote.
 *	  One whose first record is another source's but whose latest is this
 *	  thread's cannot be bounded, and keeps the published lower.
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

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "catalog/storage_xlog.h"
#include "cluster_control_root_private.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_pi_write.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_wal_claim.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_wal_retained_cut.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster_wal_retained_cut_internal.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/buf_internals.h"
#include "storage/buffile.h"
#include "utils/timestamp.h"

static bool retained_logged;
static uint64 retained_structure_pins;
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

static void
retained_sketch_init(RetainedBucket *sketch)
{
	for (uint32 i = 0; i < RETAINED_SKETCH_BUCKETS; i++) {
		sketch[i].before = InvalidScn;
		memset(sketch[i].inc_min, 0xff, 16);
		memset(sketch[i].inc_max, 0, 16);
	}
}

static inline bool
retained_bucket_empty(const RetainedBucket *bucket)
{
	return memcmp(bucket->inc_min, bucket->inc_max, 16) > 0;
}

static void
retained_bucket_incarnation(RetainedBucket *bucket, const uint8 incarnation[16])
{
	if (memcmp(incarnation, bucket->inc_min, 16) < 0)
		memcpy(bucket->inc_min, incarnation, 16);
	if (memcmp(incarnation, bucket->inc_max, 16) > 0)
		memcpy(bucket->inc_max, incarnation, 16);
}

/*
 * Remember an obligation on key: the version it starts from and its
 * incarnations.  A chain start (no present before-version) records the
 * smallest SCN, so it keeps every history edge of its incarnation.
 */
static void
retained_sketch_add(RetainedBucket *sketch, uint64 key, SCN before,
					const uint8 before_incarnation[16], const uint8 result_incarnation[16])
{
	for (int i = 0; i < 2; i++) {
		RetainedBucket *bucket = &sketch[retained_bucket(key, i)];

		if (retained_bucket_empty(bucket) || scn_total_cmp(before, bucket->before) < 0)
			bucket->before = before;
		retained_bucket_incarnation(bucket, before_incarnation);
		retained_bucket_incarnation(bucket, result_incarnation);
	}
}

/*
 * PAGE dependency rule (PU-D-7).  No obligation can be on key if either of
 * its buckets is empty.  Unless every obligation in both buckets has this
 * edge's incarnation, keep it.  Otherwise the later of the two bucket minima
 * is still at or before the true earliest before-version of key, so an edge
 * whose result is not after it is a predecessor and is not needed.
 */
static bool
retained_edge_needed(const RetainedBucket *sketch, uint64 key, SCN result,
					 const uint8 incarnation[16])
{
	SCN floor = InvalidScn;

	for (int i = 0; i < 2; i++) {
		const RetainedBucket *bucket = &sketch[retained_bucket(key, i)];

		if (retained_bucket_empty(bucket))
			return false;
		if (memcmp(bucket->inc_min, incarnation, 16) != 0
			|| memcmp(bucket->inc_max, incarnation, 16) != 0)
			return true;
		if (i == 0 || scn_total_cmp(bucket->before, floor) > 0)
			floor = bucket->before;
	}
	return scn_total_cmp(result, floor) > 0;
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
retained_history_edge(RetainedCutWork *work, uint64 key, uint64 result_token,
					  const uint8 incarnation[16], uint32 side)
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
	edge->side = side;
	memcpy(edge->incarnation, incarnation, 16);
	work->history_edges++;
}

void
retained_key_seen(RetainedCutWork *work, uint64 key, SCN before, const uint8 before_incarnation[16],
				  SCN result, const uint8 result_incarnation[16], uint32 side)
{
	if (work->current_history)
		retained_history_edge(work, key, result, result_incarnation, side);
	else
		retained_sketch_add(work->sketch, key, before, before_incarnation, result_incarnation);
}

/* A history record that ends a relation incarnation holds its source. */
void
retained_structure(RetainedCutWork *work)
{
	RetainedSource *source = &work->sources[work->current_source];

	if (work->current_history
		&& (source->structure_first == InvalidXLogRecPtr
			|| work->current_read < source->structure_first))
		source->structure_first = work->current_read;
}

static bool
retained_space(void *arg, const RfSideSpaceContributionV1 *space)
{
	RetainedCutWork *work = arg;

	/* SPACE block 0 changes only with the relation's structure.  A CREATE
	 * (the first live identity of its key) starts an incarnation; TRUNCATE
	 * and a DROP tombstone end one. */
	if ((space->page_mask & 1) != 0
		&& !(space->result.state == CLUSTER_SPACE_IDENTITY_LIVE && space->result.sequence == 1))
		retained_structure(work);
	for (uint8 block = 0; block < 2; block++)
		if ((space->page_mask & (1u << block)) != 0) {
			/* A SPACE contribution carries no before-version: as an
			 * obligation it keeps every history edge of its incarnation. */
			retained_key_seen(work, retained_key(&space->result.key.locator, SPACE_FORKNUM, block),
							  InvalidScn, space->result.incarnation, space->result_token[block],
							  space->result.incarnation, 0);
		}
	return true;
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
		return work->detail = retained_native_record(work, record, detail);
	retained_side_record(work, record, source, cut, owners.owners);
	for (uint32 i = 0; i < plan.component_count; i++) {
		const RfDetachedComponentPlanV1 *component = &plan.components[i];
		const DecodedBkpBlock *block = &decoded->blocks[component->block_id];
		bool present;

		if (component->owner == RF_DETACHED_COMPONENT_REBUILDABLE)
			continue;
		if (component->owner != RF_DETACHED_COMPONENT_PAGE_CODEC
			|| component->page_class != RF_PAGE_CLASS_ORDINARY || plan.result_token == 0)
			return work->detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		/* Every edge strictly advances its page; an unformatted or absent
		 * before-state is the start of the chain. */
		present = component->before_kind == RF_PAGE_STATE_PRESENT;
		if (present && scn_total_cmp(plan.result_token, component->before.mutation_token) <= 0)
			return work->detail = RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION;
		retained_key_seen(work, retained_key(&block->rlocator, block->forknum, block->blkno),
						  present ? component->before.mutation_token : InvalidScn,
						  present ? component->before.segment_incarnation
								  : component->result.segment_incarnation,
						  plan.result_token, component->result.segment_incarnation, 0);
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

/* Completion of each selected input; see the file header. */
/* Complete this thread no later than its unretired local PI responsibilities
 * (R-A19).  One that cannot be bounded completes it at the published lower:
 * every retained record of this thread stays an obligation. */
static bool
retained_local_pi(RetainedCutWork *work, const ClusterWalInputV1 *item)
{
	RetainedSource *source = &work->sources[work->self];
	XLogRecPtr floor = work->local_pi.floor;

	if (work->local_pi.unbounded != 0)
		floor = item->checkpoint.checkpoint_lower_lsn;
	else if (work->local_pi.bounded == 0)
		return true;
	if (floor == InvalidXLogRecPtr)
		return false;
	if (floor < source->completion) {
		source->completion = source->bound = floor;
		source->pin = CLUSTER_WAL_RETAINED_PIN_LOCAL_PI;
	}
	return true;
}

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
	return work->self >= 0 && retained_local_pi(work, cluster_wal_inputs_at_v1(inputs, work->self));
}

/* Lower the bound of source to at, recording why. */
void
retained_pin(RetainedSource *source, XLogRecPtr at, ClusterWalRetainedPinV1 pin)
{
	if (at != InvalidXLogRecPtr && at < source->bound) {
		source->bound = at;
		source->pin = pin;
	}
}

/* Fold the spooled history edges, SIDE keys and classes and structure
 * changes into each source bound.  The structure pin is applied last: on a
 * tie it is the reported reason. */
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
		if ((i % 4096) == 0)
			CHECK_FOR_INTERRUPTS();
		if (work->spool != NULL)
			BufFileReadExact(work->spool, &edge, sizeof(edge));
		else
			edge = work->batch[i];
		if (!retained_edge_needed(work->sketch, edge.key, edge.result_token, edge.incarnation))
			continue;
		work->retained_edges++;
		work->pinned_side |= edge.side;
		retained_pin(&work->sources[edge.source], edge.read_ptr,
					 edge.side != 0 ? CLUSTER_WAL_RETAINED_PIN_SIDE
									: CLUSTER_WAL_RETAINED_PIN_PAGE);
	}
	retained_side_fold(work);
	for (uint32 s = 0; s < work->nsources; s++) {
		RetainedSource *source = &work->sources[s];

		if (source->structure_first != InvalidXLogRecPtr
			&& source->structure_first <= source->bound) {
			source->bound = source->structure_first;
			source->pin = CLUSTER_WAL_RETAINED_PIN_STRUCTURE;
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
	work->sketch = palloc(sizeof(RetainedBucket) * RETAINED_SKETCH_BUCKETS);
	retained_sketch_init(work->sketch);
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
	out->spool_bytes = work->spooled * sizeof(RetainedEdge);
	out->side_classes = work->pinned_side;
	out->pin = source->pin;
	out->local_pi_floor = work->local_pi.floor;
	out->local_pi_bounded = work->local_pi.bounded;
	out->local_pi_unbounded = work->local_pi.unbounded;
	if (out->pin == CLUSTER_WAL_RETAINED_PIN_STRUCTURE)
		retained_structure_pins++;
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
	work = palloc0(sizeof(*work));
	/* Before selecting the input: the directory scan waits on entry locks. */
	if (!cluster_pcm_local_pi_floor_v1(self, &work->local_pi)) {
		pfree(work);
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	}
	result = cluster_wal_inputs_begin_v1(self->claim.identity.storage_uuid,
										 self->claim.identity.system_identifier, &inputs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		pfree(work);
		return result;
	}
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
	else if (pin == CLUSTER_WAL_RETAINED_PIN_STRUCTURE)
		ereport(LOG,
				(errmsg("cluster WAL retention lower held by a retained relation structure change"),
				 errdetail("Lower %X/%X, native redo %X/%X: a CREATE, TRUNCATE or DROP in retained "
						   "history keeps its WAL until structural PI retirement is durable; "
						   "held " UINT64_FORMAT " times since start.",
						   LSN_FORMAT_ARGS(cut->lower), LSN_FORMAT_ARGS(cut->native_redo),
						   retained_structure_pins)));
	else if (pin == CLUSTER_WAL_RETAINED_PIN_LOCAL_PI)
		ereport(LOG, (errmsg("cluster WAL retention lower held by a local PI responsibility"),
					  errdetail("Lower %X/%X, native redo %X/%X, earliest responsibility %X/%X: "
								"%llu bounded and %llu unbounded responsibilities keep this "
								"thread's WAL until their PIs are retired.",
								LSN_FORMAT_ARGS(cut->lower), LSN_FORMAT_ARGS(cut->native_redo),
								LSN_FORMAT_ARGS(cut->local_pi_floor),
								(unsigned long long)cut->local_pi_bounded,
								(unsigned long long)cut->local_pi_unbounded)));
	else if (pin != CLUSTER_WAL_RETAINED_PIN_NONE)
		ereport(LOG, (errmsg("cluster WAL retention lower held by %s ancestry",
							 pin == CLUSTER_WAL_RETAINED_PIN_SIDE ? "SIDE" : "page"),
					  errdetail("Lower %X/%X, native redo %X/%X, SIDE owners 0x%x, %llu of %llu "
								"history edges needed.",
								LSN_FORMAT_ARGS(cut->lower), LSN_FORMAT_ARGS(cut->native_redo),
								cut->side_classes, (unsigned long long)cut->retained_edges,
								(unsigned long long)cut->history_edges)));
}

/* One line per checkpoint with the readings of S07: like the checkpoint
 * report itself, at LOG only when log_checkpoints is on. */
static void
retained_report_readings(const ClusterWalRetainedCutV1 *cut, ClusterControlRootResult result,
						 int64 census_us, int64 publication_us)
{
	ereport(
		log_checkpoints ? LOG : DEBUG1,
		(errmsg("cluster WAL retention census: lower %X/%X -> %X/%X (native redo %X/%X)",
				LSN_FORMAT_ARGS(cut->old_lower),
				LSN_FORMAT_ARGS(result == CLUSTER_CONTROL_ROOT_OK_PRIMARY ? cut->lower
																		  : cut->old_lower),
				LSN_FORMAT_ARGS(cut->native_redo)),
		 errdetail("result %d, %llu records, %llu history edges, %llu needed, spool %llu "
				   "bytes, census %lld ms, publication %lld ms, local PI floor %X/%X "
				   "(%llu bounded, %llu unbounded).",
				   (int)result, (unsigned long long)cut->records,
				   (unsigned long long)cut->history_edges, (unsigned long long)cut->retained_edges,
				   (unsigned long long)cut->spool_bytes, (long long)(census_us / 1000),
				   (long long)(publication_us / 1000), LSN_FORMAT_ARGS(cut->local_pi_floor),
				   (unsigned long long)cut->local_pi_bounded,
				   (unsigned long long)cut->local_pi_unbounded)));
}

void
cluster_wal_retained_cut_after_checkpoint_v1(void)
{
	ClusterWalSourceRef self;
	ClusterWalRetainedCutV1 cut;
	ClusterControlRootSnapshot published;
	RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_OK;
	ClusterControlRootResult result;
	TimestampTz started, censused;

	if (!AmCheckpointerProcess() || !cluster_enabled || !cluster_shared_config
		|| ShutdownRequestPending || CritSectionCount != 0
		|| !cluster_wal_thread_current_v2_ref(&self))
		return;
	started = GetCurrentTimestamp();
	result = cluster_wal_retained_cut_compute_v1(&self, &cut, &detail);
	censused = GetCurrentTimestamp();
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && cut.lower > cut.old_lower)
		result = cluster_control_root_v3_retained_lower_publish(
			&self.claim.identity, &cut.root_token, cut.native_redo, cut.lower, &published);
	retained_report_readings(&cut, result, censused - started, GetCurrentTimestamp() - censused);
	retained_report(result, detail, result == CLUSTER_CONTROL_ROOT_OK_PRIMARY ? cut.pin : 0, &cut);
}

#endif /* USE_PGRAC_CLUSTER */
