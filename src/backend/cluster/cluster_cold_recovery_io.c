/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_io.c
 *	  I/O for typed cold-crash replay: participant census, pass-1 scans,
 *	  DATA observation, the pass-2 source reader and the completion proof
 *	  and durability barrier.
 *
 *	  The census selects, from one cold read scope over every ROOT slot,
 *	  each writer generation with retained WAL: crashed ones are replayed,
 *	  older or closed ones are history only.  Pass 1 visits every retained
 *	  record of a crashed (RECOVERY_REQUIRED) root through the sealed recovery
 *	  visitor, and of a history-only generation through that read scope, and
 *	  feeds the plan; nothing visited is trusted until the visit and the
 *	  observed cut both match the ROOT.  DATA observation and the SPACE
 *	  owner's pass-1 check read storage directly, before replay touches
 *	  shared buffers.  Pass 2 re-reads the same generation through the
 *	  selected restart-input segment opener; the caller compares each record
 *	  with its pass-1 identity.  Before a replayed generation is published
 *	  recovered, its pass-2 cut is proven against its ROOT and the files
 *	  pass 2 changed are made durable (their dirty buffers written, each
 *	  fork fsynced).  Nothing else here writes; nothing locks pages or
 *	  grants replay authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cold_recovery_io.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_cold_recovery_census.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_wal_restart_read.h"
#include "cluster/cluster_wal_tail.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"

struct ClusterColdReaderV1 {
	ClusterWalSourceRef source;
	XLogReaderState *reader;
	int segment_fd;
	XLogSegNo segment_no;
};

static int
cold_reader_page_read(XLogReaderState *state, XLogRecPtr target_page, int required,
					  XLogRecPtr target_record, char *read_buffer)
{
	ClusterColdReaderV1 *cold = (ClusterColdReaderV1 *)state->private_data;
	XLogSegNo segment_no;
	int read_bytes;

	(void)target_record;
	XLByteToSeg(target_page, segment_no, state->segcxt.ws_segsize);
	if (cold->segment_fd < 0 || segment_no != cold->segment_no) {
		ClusterControlRootResult result;

		if (cold->segment_fd >= 0) {
			close(cold->segment_fd);
			cold->segment_fd = -1;
		}
		result = cluster_wal_restart_segment_open(cluster_wal_threads_dir, &cold->source,
												  cold->source.timeline, segment_no,
												  state->segcxt.ws_segsize, &cold->segment_fd);
		if (result == CLUSTER_CONTROL_ROOT_ABSENT)
			return -1; /* end of the selected stream */
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not open selected WAL segment of thread %u for cold replay",
							cold->source.claim.identity.origin_thread_id),
					 errdetail("Segment " UINT64_FORMAT ", root result %d.", (uint64)segment_no,
							   (int)result),
					 errhint("Preserve every original WAL generation; no fallback source is "
							 "permitted.")));
		cold->segment_no = segment_no;
	}
	pgstat_report_wait_start(WAIT_EVENT_WAL_READ);
	read_bytes = pg_pread(cold->segment_fd, read_buffer, XLOG_BLCKSZ,
						  (off_t)XLogSegmentOffset(target_page, state->segcxt.ws_segsize));
	pgstat_report_wait_end();
	if (read_bytes < required)
		return -1;
	return read_bytes;
}

ClusterColdReaderV1 *
cluster_cold_reader_open_v1(const ClusterWalSourceRef *source, uint64 system_identifier,
							XLogRecPtr start)
{
	ClusterColdReaderV1 *cold;

	if (source == NULL || source->timeline == 0 || system_identifier == 0
		|| start == InvalidXLogRecPtr || cluster_wal_threads_dir == NULL
		|| cluster_wal_threads_dir[0] == '\0')
		return NULL;
	cold = (ClusterColdReaderV1 *)palloc0(sizeof(*cold));
	cold->source = *source;
	cold->segment_fd = -1;
	cold->reader = XLogReaderAllocate(wal_segment_size, NULL,
									  XL_ROUTINE(.page_read = cold_reader_page_read), cold);
	if (cold->reader == NULL) {
		pfree(cold);
		return NULL;
	}
	cold->reader->system_identifier = system_identifier;
	cold->reader->seg.ws_tli = source->timeline;
	cold->reader->cluster_expected_thread_id = source->claim.identity.origin_thread_id;
	XLogBeginRead(cold->reader, start);
	return cold;
}

XLogReaderState *
cluster_cold_reader_next_v1(ClusterColdReaderV1 *cold, char **errormsg)
{
	if (errormsg != NULL)
		*errormsg = NULL;
	if (cold == NULL || cold->reader == NULL)
		return NULL;
	return XLogReadRecord(cold->reader, errormsg) != NULL ? cold->reader : NULL;
}

void
cluster_cold_reader_close_v1(ClusterColdReaderV1 **cold_address)
{
	ClusterColdReaderV1 *cold;

	if (cold_address == NULL || *cold_address == NULL)
		return;
	cold = *cold_address;
	if (cold->segment_fd >= 0)
		close(cold->segment_fd);
	if (cold->reader != NULL)
		XLogReaderFree(cold->reader);
	pfree(cold);
	*cold_address = NULL;
}

/* Both SPACE pages as storage holds them; zero where a block is absent. */
static void
cold_read_space_pages(RelFileLocator locator, PGAlignedBlock pages[2])
{
	SMgrRelation relation = smgropen(locator, InvalidBackendId);
	BlockNumber nblocks = 0;
	BlockNumber block;

	memset(pages, 0, 2 * sizeof(PGAlignedBlock));
	if (smgrexists(relation, SPACE_FORKNUM))
		nblocks = smgrnblocks(relation, SPACE_FORKNUM);
	for (block = 0; block < 2 && block < nblocks; block++)
		smgrread(relation, SPACE_FORKNUM, block, pages[block].data);
}

static void
cold_space_key(const ClusterColdObserverV1 *observer, RelFileLocator locator,
			   ClusterSpaceIdentityKey *key)
{
	memset(key, 0, sizeof(*key));
	key->system_identifier = observer->system_identifier;
	key->database_incarnation = observer->database_incarnation;
	memcpy(key->storage_uuid, observer->storage_uuid, 16);
	key->locator = locator;
}

/*
 * The relation's live SPACE identity names the incarnation a page header
 * belongs to.  Without one (not yet written for a relation created in the
 * replayed WAL, dropped, or unreadable) the page is reported NO_IDENTITY
 * and the planner decides.
 */
static void
cold_observe_incarnation(ClusterColdObserverV1 *observer, RelFileLocator locator,
						 ClusterColdDataV1 *out)
{
	if (!observer->cached_valid || !RelFileLocatorEquals(observer->cached_locator, locator)) {
		ClusterSpaceIdentityKey key;
		ClusterSpaceIdentity identity;
		PGAlignedBlock pages[2];
		uint64 token;

		cold_space_key(observer, locator, &key);
		cold_read_space_pages(locator, pages);
		observer->cached_no_identity
			= !cluster_space_identity_page_decode(pages[0].data, BLCKSZ, SPACE_FORKNUM, 0, &key,
												  &identity, &token)
			  || identity.state != CLUSTER_SPACE_IDENTITY_LIVE;
		memset(observer->cached_incarnation, 0, 16);
		if (!observer->cached_no_identity)
			memcpy(observer->cached_incarnation, identity.incarnation, 16);
		observer->cached_locator = locator;
		observer->cached_valid = true;
	}
	if (observer->cached_no_identity)
		out->flags |= CLUSTER_COLD_DATA_FLAG_NO_IDENTITY;
	memcpy(out->version.segment_incarnation, observer->cached_incarnation, 16);
}

bool
cluster_cold_space_check_v1(void *arg, const RelFileLocator *locator,
							const ClusterColdSpaceInputV1 *inputs, uint32 count, uint32 *order)
{
	ClusterColdObserverV1 *observer = (ClusterColdObserverV1 *)arg;
	ClusterSpaceIdentityKey key;
	ClusterSpaceRecoveryInput *owned;
	ClusterSpaceRecoveryImage *image;
	PGAlignedBlock pages[2];
	bool proven;
	uint32 i;

	if (observer == NULL || locator == NULL || inputs == NULL || order == NULL || count == 0)
		return false;
	owned = (ClusterSpaceRecoveryInput *)palloc_extended((Size)count * sizeof(*owned),
														 MCXT_ALLOC_HUGE | MCXT_ALLOC_NO_OOM);
	image = (ClusterSpaceRecoveryImage *)palloc_extended(sizeof(*image), MCXT_ALLOC_NO_OOM);
	if (owned == NULL || image == NULL) {
		if (owned != NULL)
			pfree(owned);
		if (image != NULL)
			pfree(image);
		return false;
	}
	for (i = 0; i < count; i++) {
		owned[i].data = inputs[i].payload;
		owned[i].length = inputs[i].payload_length;
	}
	cold_space_key(observer, *locator, &key);
	cold_read_space_pages(*locator, pages);
	proven = cluster_space_recovery_prepare(owned, count, &key, pages[0].data, pages[1].data, order,
											image);
	observer->space_relations_checked++;
	pfree(image);
	pfree(owned);
	return proven;
}

bool
cluster_cold_observe_data_v1(void *arg, const RfPageIdentityV1 *page, ClusterColdDataV1 *out)
{
	ClusterColdObserverV1 *observer = (ClusterColdObserverV1 *)arg;
	PGAlignedBlock block;
	SMgrRelation relation;
	ForkNumber fork;
	bool header_valid;

	if (observer == NULL || page == NULL || out == NULL || page->forknum > MAX_FORKNUM)
		return false;
	memset(out, 0, sizeof(*out));
	fork = (ForkNumber)page->forknum;
	observer->pages_observed++;
	relation = smgropen(page->locator, InvalidBackendId);
	if (!smgrexists(relation, fork) || page->blockno >= smgrnblocks(relation, fork)) {
		out->kind = CLUSTER_COLD_DATA_ABSENT;
		out->flags = CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED;
		return true;
	}
	smgrread(relation, fork, page->blockno, block.data);
	/* The classifier compares the data checksum itself, so a page the native
	 * check passes under ignore_checksum_failure is still judged torn. */
	header_valid = PageIsVerifiedExtended((Page)block.data, page->blockno, 0);
	if (!cluster_cold_classify_page_v1(block.data, page->blockno, header_valid,
									   DataChecksumsEnabled(), out))
		return false;
	if (out->kind == CLUSTER_COLD_DATA_INVALID) {
		observer->pages_invalid++;
		return true;
	}
	cold_observe_incarnation(observer, page->locator, out);
	return true;
}

typedef struct ColdScanWork {
	ClusterColdPlanV1 *plan;
	uint32 participant;
	uint64 system_identifier;
	uint8 storage_uuid[16];
	bool space_active;
	bool foreign;
	ClusterColdDetailV1 detail;
	ClusterColdScanResultV1 *result;
	ClusterColdDecodedV1 decoded;
} ColdScanWork;

static bool
cold_scan_visit(XLogReaderState *reader, void *arg)
{
	ColdScanWork *work = (ColdScanWork *)arg;

	CHECK_FOR_INTERRUPTS();
	work->detail
		= cluster_cold_recovery_decode_v1(reader, work->system_identifier, work->storage_uuid,
										  work->space_active, work->foreign, &work->decoded);
	if (work->detail == CLUSTER_COLD_OK)
		work->detail
			= cluster_cold_plan_feed_v1(work->plan, work->participant, &work->decoded.record);
	if (work->detail != CLUSTER_COLD_OK) {
		work->result->failed_read_rec_ptr = reader->ReadRecPtr;
		work->result->route_detail = work->decoded.route_detail;
		return false;
	}
	work->result->records++;
	return true;
}

static ColdScanWork *
cold_scan_work(ClusterColdPlanV1 *plan, uint32 participant, const ClusterControlRootIdentity *id,
			   bool space_active, bool foreign, ClusterColdScanResultV1 *result)
{
	ColdScanWork *work = (ColdScanWork *)palloc0(sizeof(*work));

	work->plan = plan;
	work->participant = participant;
	work->system_identifier = id->system_identifier;
	memcpy(work->storage_uuid, id->storage_uuid, 16);
	work->space_active = space_active;
	work->foreign = foreign;
	work->detail = CLUSTER_COLD_OK;
	work->result = result;
	return work;
}

/* Visited records are provisional until the visit and its observed cut
 * (record count, complete end) match the ROOT record's validated tail. */
static ClusterColdDetailV1
cold_scan_finish(ColdScanWork *work, ClusterControlRootResult visit,
				 const ClusterWalTailObservation *observed, XLogRecPtr tail)
{
	ClusterColdScanResultV1 *result = work->result;
	ClusterColdDetailV1 detail = work->detail;

	cluster_cold_decoded_release_v1(&work->decoded);
	result->root_result = (int)visit;
	if (detail == CLUSTER_COLD_OK
		&& (visit != CLUSTER_CONTROL_ROOT_OK_PRIMARY || observed->records != result->records
			|| observed->complete_end != tail))
		detail = CLUSTER_COLD_SOURCE_GAP;
	pfree(work);
	return detail;
}

ClusterColdDetailV1
cluster_cold_scan_root_v1(ClusterColdPlanV1 *plan, uint32 participant,
						  const ClusterControlRootSnapshot *root,
						  const ClusterControlRootReadToken *token, bool space_active, bool foreign,
						  ClusterColdScanResultV1 *result)
{
	ColdScanWork *work;
	ClusterWalTailObservation observed;
	ClusterControlRootResult visit;

	if (result == NULL)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	memset(result, 0, sizeof(*result));
	if (plan == NULL || root == NULL || token == NULL)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	work = cold_scan_work(plan, participant, &root->identity, space_active, foreign, result);
	memset(&observed, 0, sizeof(observed));
	visit = cluster_control_root_recovery_visit(root, token, cold_scan_visit, work, &observed);
	return cold_scan_finish(work, visit, &observed, root->validated_tail_lsn_exclusive);
}

ClusterColdDetailV1
cluster_cold_scan_input_v1(ClusterColdPlanV1 *plan, uint32 participant, ClusterWalInputsV1 *inputs,
						   uint32 index, bool space_active, bool foreign,
						   ClusterColdScanResultV1 *result)
{
	const ClusterWalInputV1 *input;
	ColdScanWork *work;
	ClusterWalTailObservation observed;
	ClusterControlRootResult visit;

	if (result == NULL)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	memset(result, 0, sizeof(*result));
	input = inputs != NULL ? cluster_wal_inputs_at_v1(inputs, index) : NULL;
	if (plan == NULL || input == NULL || input->kind != CLUSTER_WAL_INPUT_CHECKPOINT)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	work = cold_scan_work(plan, participant, &input->checkpoint.identity, space_active, foreign,
						  result);
	memset(&observed, 0, sizeof(observed));
	visit = cluster_wal_inputs_visit_retained_v1(inputs, index, cold_scan_visit, work, &observed);
	return cold_scan_finish(work, visit, &observed, input->checkpoint.validated_tail_lsn_exclusive);
}

/* ---- participant census (cluster_cold_recovery_census.h) ---- */

/*
 * A checkpoint input's role, or a refusal.  Its retained range is
 * [checkpoint lower, validated tail); the native redo start splits a
 * crashed generation into history and replay.
 */
static ClusterColdCensusDetailV1
census_checkpoint(const ClusterWalInputV1 *input, ClusterColdCensusEntryV1 *entry, bool *keep)
{
	const ClusterControlRootSnapshot *root = &input->checkpoint;
	bool crashed;

	*keep = false;
	if (input->current && root->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		return CLUSTER_COLD_CENSUS_LIVE_WRITER;
	crashed = input->current && root->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	if (!crashed && root->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
		&& root->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE)
		return CLUSTER_COLD_CENSUS_LIFECYCLE;
	if ((root->root_flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID) == 0
		|| root->checkpoint_lower_lsn == InvalidXLogRecPtr
		|| root->checkpoint_lower_lsn > root->validated_tail_lsn_exclusive
		|| (crashed
			&& (input->native_redo < root->checkpoint_lower_lsn
				|| input->native_redo > root->validated_tail_lsn_exclusive)))
		return CLUSTER_COLD_CENSUS_RANGE;
	/* Nothing retained: no ancestry to prove and nothing to replay. */
	if (!crashed && root->checkpoint_lower_lsn == root->validated_tail_lsn_exclusive)
		return CLUSTER_COLD_CENSUS_OK;
	memset(entry, 0, sizeof(*entry));
	entry->role = crashed ? CLUSTER_COLD_CENSUS_REPLAY : CLUSTER_COLD_CENSUS_HISTORY;
	entry->identity = root->identity;
	entry->source = input->source;
	entry->cut.thread_id = root->identity.origin_thread_id;
	entry->cut.timeline = root->checkpoint_tli;
	entry->cut.owner_incarnation = root->identity.origin_owner_incarnation;
	entry->cut.physical_lower = root->checkpoint_lower_lsn;
	entry->cut.tail_end = root->validated_tail_lsn_exclusive;
	/* Every retained record of a closed or recovered generation is durable. */
	entry->cut.native_redo = crashed ? input->native_redo : root->validated_tail_lsn_exclusive;
	*keep = true;
	return CLUSTER_COLD_CENSUS_OK;
}

ClusterColdCensusDetailV1
cluster_cold_census_select_v1(const ClusterWalInputV1 *const *inputs, uint32 count,
							  ClusterColdCensusEntryV1 *entries, uint32 capacity, uint32 *out_count,
							  uint32 *bad_index)
{
	uint32 kept = 0;
	uint32 i;

	if (out_count == NULL || bad_index == NULL)
		return CLUSTER_COLD_CENSUS_INVALID_ARGUMENT;
	*out_count = 0;
	*bad_index = 0;
	if ((inputs == NULL && count > 0) || (entries == NULL && capacity > 0))
		return CLUSTER_COLD_CENSUS_INVALID_ARGUMENT;
	for (i = 0; i < count; i++) {
		ClusterColdCensusEntryV1 entry;
		ClusterColdCensusDetailV1 detail = CLUSTER_COLD_CENSUS_OK;
		bool keep = false;

		*bad_index = i;
		if (inputs[i] == NULL)
			return CLUSTER_COLD_CENSUS_INVALID_ARGUMENT;
		if (inputs[i]->kind == CLUSTER_WAL_INPUT_INITIALIZER_TERMINAL) {
			/* An initialization that ended before its first checkpoint. */
			if (inputs[i]->terminal.tail.records != 0)
				return CLUSTER_COLD_CENSUS_TERMINAL;
			continue;
		}
		if (inputs[i]->kind != CLUSTER_WAL_INPUT_CHECKPOINT)
			return CLUSTER_COLD_CENSUS_LIFECYCLE;
		detail = census_checkpoint(inputs[i], &entry, &keep);
		if (detail != CLUSTER_COLD_CENSUS_OK)
			return detail;
		if (!keep)
			continue;
		if (kept >= capacity)
			return CLUSTER_COLD_CENSUS_CAPACITY;
		entry.input_index = i;
		entries[kept++] = entry;
	}
	*bad_index = 0;
	*out_count = kept;
	return CLUSTER_COLD_CENSUS_OK;
}

static bool
census_crashed_named(const ClusterColdCensusEntryV1 *entry,
					 const ClusterControlRootSnapshot *crashed, uint32 crashed_count)
{
	uint32 i;

	for (i = 0; i < crashed_count; i++)
		if (cluster_control_root_identity_equal(&crashed[i].identity, &entry->identity))
			return true;
	return false;
}

ClusterColdCensusDetailV1
cluster_cold_census_cover_v1(const ClusterColdCensusEntryV1 *entries, uint32 count,
							 const ClusterControlRootSnapshot *crashed, uint32 crashed_count,
							 uint16 *bad_thread)
{
	uint32 i;
	uint32 j;

	if (bad_thread == NULL || (entries == NULL && count > 0)
		|| (crashed == NULL && crashed_count > 0))
		return CLUSTER_COLD_CENSUS_INVALID_ARGUMENT;
	*bad_thread = 0;
	/* Each named root is a crashed generation of the census, exactly. */
	for (i = 0; i < crashed_count; i++) {
		bool found = false;

		for (j = 0; j < count && !found; j++)
			found = entries[j].role == CLUSTER_COLD_CENSUS_REPLAY
					&& cluster_control_root_identity_equal(&entries[j].identity,
														   &crashed[i].identity);
		if (!found || crashed[i].lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED) {
			*bad_thread = crashed[i].identity.origin_thread_id;
			return CLUSTER_COLD_CENSUS_ORIGIN_MISSING;
		}
	}
	/* Each crashed generation is named: none is skipped. */
	for (j = 0; j < count; j++)
		if (entries[j].role == CLUSTER_COLD_CENSUS_REPLAY
			&& !census_crashed_named(&entries[j], crashed, crashed_count)) {
			*bad_thread = entries[j].cut.thread_id;
			return CLUSTER_COLD_CENSUS_UNCOVERED;
		}
	return CLUSTER_COLD_CENSUS_OK;
}

bool
cluster_cold_completion_proven_v1(const ClusterColdPlanV1 *plan,
								  const ClusterControlRootSnapshot *root,
								  const ClusterColdReplayResultV1 *result, uint32 participant)
{
	return plan != NULL && root != NULL && result != NULL
		   && participant < cluster_cold_plan_participant_count_v1(plan)
		   && result->detail == CLUSTER_COLD_REPLAY_OK
		   && result->steps_done == cluster_cold_plan_step_count_v1(plan)
		   && root->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		   && (root->root_flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID) != 0
		   && root->tail_last_record_lsn != InvalidXLogRecPtr
		   && result->last_read[participant] == root->tail_last_record_lsn
		   && result->last_crc[participant] == root->tail_last_record_crc32c
		   && result->last_end[participant] == root->validated_tail_lsn_exclusive;
}

bool
cluster_cold_completion_ready_v1(const ClusterColdTypedV1 *typed,
								 const struct ClusterRecoveryFencePlan *fence,
								 const ClusterColdReplayResultV1 *result,
								 ClusterColdTouchedV1 *touched, uint16 *thread)
{
	uint16 origins;
	uint32 relations;
	uint32 i;

	*thread = 0;
	if (typed == NULL || fence == NULL || result == NULL || touched == NULL)
		return false;
	origins = cluster_recovery_merge_fence_plan_origin_count(fence);
	for (i = 0; i < origins; i++) {
		ClusterControlRootSnapshot root;
		ClusterControlRootReadToken token;
		uint32 p;

		if (!cluster_recovery_merge_fence_plan_origin(fence, (uint16)i, thread, &root, &token))
			return false;
		for (p = 0; p < typed->replay_count; p++)
			if (p != typed->own_participant && typed->participants[p].thread_id == *thread)
				break;
		if (p == typed->replay_count
			|| typed->participants[p].owner_incarnation != root.identity.origin_owner_incarnation
			|| !cluster_cold_completion_proven_v1(typed->plan, &root, result, p))
			return false;
	}
	*thread = 0;
	relations = cluster_cold_plan_space_relation_count_v1(typed->plan);
	for (i = 0; i < relations; i++) {
		RelFileLocator locator;
		uint32 count = 0;
		ForkNumber fork;

		if (!cluster_cold_plan_space_relation_v1(typed->plan, i, &locator, &count))
			return false;
		for (fork = 0; fork <= MAX_FORKNUM; fork++)
			cluster_cold_touched_add_v1(touched, &locator, fork);
	}
	return true;
}

static int
touched_compare(const RelFileLocator *a, const RelFileLocator *b)
{
	if (a->spcOid != b->spcOid)
		return a->spcOid < b->spcOid ? -1 : 1;
	if (a->dbOid != b->dbOid)
		return a->dbOid < b->dbOid ? -1 : 1;
	if (a->relNumber != b->relNumber)
		return a->relNumber < b->relNumber ? -1 : 1;
	return 0;
}

void
cluster_cold_touched_add_v1(ClusterColdTouchedV1 *touched, const RelFileLocator *locator,
							ForkNumber fork)
{
	uint32 low = 0;
	uint32 high;

	Assert(touched != NULL && locator != NULL && fork >= 0 && fork <= MAX_FORKNUM);
	high = touched->count;
	while (low < high) {
		uint32 middle = low + (high - low) / 2;
		int cmp = touched_compare(&touched->rels[middle].locator, locator);

		if (cmp == 0) {
			touched->rels[middle].forks |= UINT32_C(1) << fork;
			return;
		}
		if (cmp < 0)
			low = middle + 1;
		else
			high = middle;
	}
	if (touched->count == touched->capacity) {
		touched->capacity = touched->capacity == 0 ? 64 : touched->capacity * 2;
		touched->rels
			= touched->rels == NULL
				  ? palloc(sizeof(ClusterColdTouchedRelV1) * touched->capacity)
				  : repalloc(touched->rels, sizeof(ClusterColdTouchedRelV1) * touched->capacity);
	}
	memmove(&touched->rels[low + 1], &touched->rels[low],
			sizeof(ClusterColdTouchedRelV1) * (touched->count - low));
	touched->rels[low].locator = *locator;
	touched->rels[low].forks = UINT32_C(1) << fork;
	touched->count++;
}

/* Every block reference, and the files a creation or truncation changes. */
void
cluster_cold_touched_add_record_v1(ClusterColdTouchedV1 *touched, XLogReaderState *record)
{
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	int block_id;

	for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++) {
		RelFileLocator locator;
		ForkNumber fork;
		BlockNumber block;

		if (XLogRecGetBlockTagExtended(record, (uint8)block_id, &locator, &fork, &block, NULL))
			cluster_cold_touched_add_v1(touched, &locator, fork);
	}
	if (XLogRecGetRmid(record) != RM_SMGR_ID)
		return;
	if (info == XLOG_SMGR_CREATE) {
		const xl_smgr_create *create = (const xl_smgr_create *)XLogRecGetData(record);

		cluster_cold_touched_add_v1(touched, &create->rlocator, create->forkNum);
	} else if (info == XLOG_SMGR_TRUNCATE) {
		const xl_smgr_truncate *truncate = (const xl_smgr_truncate *)XLogRecGetData(record);

		cluster_cold_touched_add_v1(touched, &truncate->rlocator, MAIN_FORKNUM);
		cluster_cold_touched_add_v1(touched, &truncate->rlocator, FSM_FORKNUM);
		cluster_cold_touched_add_v1(touched, &truncate->rlocator, VISIBILITYMAP_FORKNUM);
	}
}

/* Write every dirty buffer of the touched relations in one pass, then fsync
 * each touched fork that exists (smgrDoPendingSyncs' pattern). */
void
cluster_cold_durable_barrier_v1(const ClusterColdTouchedV1 *touched)
{
	SMgrRelation *smgrs;
	uint32 i;

	if (touched == NULL || touched->count == 0)
		return;
	smgrs = palloc(sizeof(SMgrRelation) * touched->count);
	for (i = 0; i < touched->count; i++)
		smgrs[i] = smgropen(touched->rels[i].locator, InvalidBackendId);
	FlushRelationsAllBuffers(smgrs, (int)touched->count);
	for (i = 0; i < touched->count; i++) {
		ForkNumber fork;

		for (fork = 0; fork <= MAX_FORKNUM; fork++)
			if ((touched->rels[i].forks & (UINT32_C(1) << fork)) != 0 && smgrexists(smgrs[i], fork))
				smgrimmedsync(smgrs[i], fork);
	}
	pfree(smgrs);
}

#endif /* USE_PGRAC_CLUSTER */
