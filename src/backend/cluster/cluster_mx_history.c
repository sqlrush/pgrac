/*-------------------------------------------------------------------------
 * cluster_mx_history.c
 *    Capture and retention lifetime for backend-private history collection.
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_mx_history.h"
#ifdef USE_PGRAC_CLUSTER
#include "access/xact.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_tt_local.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

struct ClusterMxHistoryCollector {
	ResourceOwner owner;
	Snapshot snapshot;
	ClusterMxHistoryCapture *capture;
};

static ClusterMxHistoryCollector *active_collector;
static uint64 collector_serial;
static bool cleanup_registered;

/* The dispatching backend's original publication, never a raw-xid alias.
 * Partial publication cannot turn into an explicit NO_OWNER challenge. */
static bool
history_owner(ClusterR4CrOwnerChallenge *owner)
{
	ClusterCanonicalTxnBinding binding;
	ClusterCanonicalTxnPublishState state;
	FullTransactionId top = GetTopFullTransactionIdIfAny();

	memset(owner, 0, sizeof(*owner));
	if (!FullTransactionIdIsValid(top))
		return true;
	state = cluster_tt_local_capture_canonical_binding(XidFromFullTransactionId(top), &binding);
	if (state == CLUSTER_CANONICAL_TXN_NONE)
		return true;
	if (state != CLUSTER_CANONICAL_TXN_PUBLISHED || binding.reserved16 != 0)
		return false;
	owner->top_fxid = top;
	owner->active_lsn = binding.active_lsn;
	owner->segment_id = binding.segment_id;
	owner->segment_generation = binding.segment_generation;
	owner->top_xid = binding.xid;
	owner->slot_offset = binding.slot_offset;
	owner->slot_wrap = binding.slot_wrap;
	owner->origin_instance = binding.origin_instance;
	owner->publish_state = binding.publish_state;
	owner->flags = CLUSTER_R4_CR_OWNER_PRESENT;
	return ClusterR4CrOwnerForRequester(owner, cluster_node_id);
}

/* Short stack scopes preserve the caller's evaluator stack even across ERROR.
 * The collector's separate native registration, not this scope, retains undo. */
static bool
history_snapshot(Snapshot snapshot, SCN *read_scn, SCN *floor, uint64 *epoch, uint64 *identity)
{
	ClusterSnapshotReadScopeV1 scope;
	Snapshot actual;
	const char *reason;
	volatile bool valid = false;

	cluster_snapshot_read_enter_v1(&scope, snapshot);
	PG_TRY();
	{
		valid = cluster_snapshot_read_evidence_v1(scope.read_scn, &actual, floor, &reason)
				&& actual == snapshot && cluster_snapshot_cr_identity_v1(actual, identity);
		if (valid) {
			*read_scn = scope.read_scn;
			*epoch = scope.read_epoch;
		}
	}
	PG_FINALLY();
	{
		cluster_snapshot_read_exit_v1(&scope);
	}
	PG_END_TRY();
	return valid;
}

static bool
history_read_authority(const ClusterPcmOwnSnapshot *pcm)
{
	bool valid = (pcm->semantic_buf_state & BM_VALID) != 0;

	if (pcm->flags != 0
		|| !cluster_pcm_x_activation_fence_open(pcm->writer_activation_token,
												pcm->resource_x_activation_generation))
		return false;
	if (cluster_pcm_x_current_image_shape(pcm->pcm_state, pcm->buffer_type, valid))
		return true;
	return valid && pcm->pcm_state == PCM_STATE_READ_IMAGE && pcm->buffer_type == BUF_TYPE_CURRENT
		   && pcm->generation != 0 && pcm->reservation_token != 0
		   && pcm->reservation_token != UINT64_MAX;
}

/* All pointer/offset checks precede page/tuple dereferences in release builds.
 * Buffer pin and relation lock stay with the heap caller across the unlock. */
static bool
history_heap(Relation rel, Buffer buffer, HeapTuple tuple, ClusterMxHistoryCapture *out)
{
	BufferDesc *desc;
	Page page;
	PageHeader ph;
	OffsetNumber offset;
	ItemId lp;
	bool exclusive;

	if (CritSectionCount != 0 || rel == NULL || tuple == NULL || tuple->t_data == NULL
		|| buffer <= 0 || buffer > NBuffers || !BufferIsPinnedByMe(buffer))
		return false;
	desc = GetBufferDescriptor(buffer - 1);
	exclusive = LWLockHeldByMeInMode(BufferDescriptorGetContentLock(desc), LW_EXCLUSIVE);
	if (!exclusive && !LWLockHeldByMeInMode(BufferDescriptorGetContentLock(desc), LW_SHARED))
		return false;
	if (desc->tag.forkNum != MAIN_FORKNUM
		|| !RelFileLocatorEquals(BufTagGetRelFileLocator(&desc->tag), rel->rd_locator)
		|| ItemPointerGetBlockNumber(&tuple->t_self) != desc->tag.blockNum
		|| tuple->t_tableOid != RelationGetRelid(rel))
		return false;
	page = BufferGetPage(buffer);
	ph = (PageHeader)page;
	if (!PageHasItl(page) || PageGetPageSize(page) != BLCKSZ
		|| PageGetPageLayoutVersion(page) != PG_PAGE_LAYOUT_VERSION
		|| ph->pd_lower < SizeOfPageHeaderData || ph->pd_lower > ph->pd_upper
		|| ph->pd_upper > ph->pd_special || ph->pd_special > BLCKSZ
		|| PageGetSpecialSize(page) < CLUSTER_ITL_SPECIAL_SIZE)
		return false;
	offset = ItemPointerGetOffsetNumber(&tuple->t_self);
	if (offset < FirstOffsetNumber || offset > PageGetMaxOffsetNumber(page))
		return false;
	lp = PageGetItemId(page, offset);
	if (!ItemIdIsNormal(lp) || ItemIdGetLength(lp) < SizeofHeapTupleHeader
		|| ItemIdGetOffset(lp) < ph->pd_upper
		|| (Size)ItemIdGetOffset(lp) + ItemIdGetLength(lp) > ph->pd_special
		|| (HeapTupleHeader)PageGetItem(page, lp) != tuple->t_data
		|| tuple->t_len != ItemIdGetLength(lp) || tuple->t_data->t_hoff < SizeofHeapTupleHeader
		|| tuple->t_data->t_hoff > tuple->t_len)
		return false;
	memset(out, 0, sizeof(*out));
	if (cluster_bufmgr_pcm_own_snapshot(desc, &out->pcm) != CLUSTER_PCM_OWN_OK
		|| !BufferTagsEqual(&out->pcm.tag, &desc->tag) || !history_read_authority(&out->pcm)
		|| (exclusive && out->pcm.pcm_state != PCM_STATE_X))
		return false;
	out->read_only = !exclusive;
	out->buffer = buffer;
	out->relation_id = RelationGetRelid(rel);
	out->page_lsn = PageGetLSN(page);
	out->tid = tuple->t_self;
	out->line_pointer = *lp;
	out->tuple_length = ItemIdGetLength(lp);
	memcpy(out->tuple_header, tuple->t_data, SizeofHeapTupleHeader);
	memcpy(out->roots, ClusterPageGetItlSlots(page), sizeof(out->roots));
	return true;
}

static bool
history_pcm_equal(const ClusterMxHistoryCapture *before, const ClusterMxHistoryCapture *after)
{
	const ClusterPcmOwnSnapshot *a = &before->pcm, *b = &after->pcm;

	if (before->read_only != after->read_only || !BufferTagsEqual(&a->tag, &b->tag)
		|| a->flags != b->flags || a->pcm_state != b->pcm_state
		|| a->resource_x_activation_generation != b->resource_x_activation_generation)
		return false;
	/* Original one-shot read-image relock needs a new generation and token. */
	if (before->read_only && a->pcm_state == PCM_STATE_READ_IMAGE)
		return b->generation > a->generation && b->reservation_token > a->reservation_token;
	return a->generation == b->generation && a->reservation_token == b->reservation_token;
}

void
cluster_mx_history_cancel(ClusterMxHistoryCollector *collector)
{
	Snapshot snapshot;

	if (collector == NULL || collector->capture == NULL)
		return;
	snapshot = collector->snapshot;
	collector->snapshot = NULL;
	pfree(collector->capture);
	collector->capture = NULL;
	if (active_collector == collector)
		active_collector = NULL;
	if (snapshot != NULL)
		UnregisterSnapshotFromOwner(snapshot, collector->owner);
	/* Tiny handle remains in TopTransactionContext for idempotent cancellation;
	 * the full capture and its real native registration have both retired. */
}

static void
history_resource_release(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg)
{
	if (phase == RESOURCE_RELEASE_BEFORE_LOCKS && active_collector != NULL
		&& (isTopLevel || active_collector->owner == CurrentResourceOwner))
		cluster_mx_history_cancel(active_collector);
}

static void
history_exit(int code, Datum arg)
{
	cluster_mx_history_cancel(active_collector);
}

bool
cluster_mx_history_capture_begin(Relation rel, Buffer buffer, HeapTuple tuple, Snapshot actual,
								 const ClusterCurrentMxKey *mxkey,
								 const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
								 const ClusterSemanticAdmissionToken *admission,
								 ClusterMxHistoryCollector **out)
{
	ClusterMxHistoryCapture captured;
	ClusterMxHistoryCollector *collector;

	if (out == NULL)
		return false;
	*out = NULL;
	if (active_collector != NULL || CurrentResourceOwner == NULL || TopTransactionContext == NULL
		|| MyStartTimestamp <= 0 || collector_serial == UINT64_MAX || mxkey == NULL
		|| admission == NULL || admission->feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		|| admission->side != CLUSTER_SEMANTIC_TARGET_SIDE
		|| !cluster_semantic_activation_recheck(admission)
		|| admission->formation_epoch != cluster_epoch_get_current()
		|| cluster_multixact_current_validate_descriptor(mxkey, mxkey->origin_node_id,
														 admission->formation_epoch, members,
														 nmembers, nmembers)
			   != CMX_DESC_OK
		|| !history_heap(rel, buffer, tuple, &captured)
		|| !(tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI)
		|| HeapTupleHeaderGetRawXmax(tuple->t_data) != mxkey->multixact_id
		|| !history_snapshot(actual, &captured.read_scn, &captured.retained_floor,
							 &captured.read_epoch, &captured.snapshot_identity)
		|| !history_owner(&captured.requester_owner))
		return false;
	captured.mxkey = *mxkey;
	captured.nmembers = nmembers;
	memcpy(captured.members, members, sizeof(*members) * nmembers);
	captured.descriptor_hash = cluster_multixact_current_descriptor_hash(mxkey, members, nmembers);
	captured.admission = *admission;
	captured.backend_incarnation = (uint64)MyStartTimestamp;
	captured.collector_id = ++collector_serial;
	/* A fresh capture after requalification advances both identity axes. */
	captured.capture_generation = collector_serial;
	if (!cleanup_registered) {
		RegisterResourceReleaseCallback(history_resource_release, NULL);
		before_shmem_exit(history_exit, (Datum)0);
		cleanup_registered = true;
	}
	collector
		= MemoryContextAllocExtended(TopTransactionContext, sizeof(*collector), MCXT_ALLOC_NO_OOM);
	if (collector == NULL)
		return false;
	memset(collector, 0, sizeof(*collector));
	collector->capture
		= MemoryContextAllocExtended(TopTransactionContext, sizeof(captured), MCXT_ALLOC_NO_OOM);
	if (collector->capture == NULL) {
		pfree(collector);
		return false;
	}
	*collector->capture = captured;
	collector->owner = CurrentResourceOwner;
	active_collector = collector;
	/* The actual live snapshot is already copied. Do not synthesize a new
	 * read SCN or publish a lower floor to rescue an unretained input. */
	collector->snapshot = RegisterSnapshotOnOwner(actual, collector->owner);
	if (collector->snapshot != actual) {
		cluster_mx_history_cancel(collector);
		return false;
	}
	*out = collector;
	return true;
}

bool
cluster_mx_history_capture_read(ClusterMxHistoryCollector *collector, ClusterMxHistoryCapture *out)
{
	ClusterR4CrOwnerChallenge owner;
	SCN read_scn, floor;
	uint64 epoch, identity;
	ClusterMxHistoryCapture *saved;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (collector == NULL || collector != active_collector || collector->capture == NULL)
		return false;
	saved = collector->capture;
	if (CurrentResourceOwner != collector->owner
		|| (uint64)MyStartTimestamp != saved->backend_incarnation
		|| !cluster_semantic_activation_recheck(&saved->admission)
		|| !history_snapshot(collector->snapshot, &read_scn, &floor, &epoch, &identity)
		|| read_scn != saved->read_scn || epoch != saved->read_epoch
		|| identity != saved->snapshot_identity || !history_owner(&owner)
		|| !ClusterR4CrOwnerEqual(&owner, &saved->requester_owner)) {
		cluster_mx_history_cancel(collector);
		return false;
	}
	/* A sibling's older floor may legitimately retire; the original capture
	 * floor remains a comparison input, while the saved snapshot stays pinned. */
	*out = *saved;
	return true;
}

bool
cluster_mx_history_capture_recheck(ClusterMxHistoryCollector *collector, Relation rel,
								   Buffer buffer, HeapTuple tuple, const ClusterCurrentMxKey *mxkey,
								   const ClusterCurrentMxMemberDesc *members, uint16 nmembers)
{
	ClusterMxHistoryCapture saved, fresh;

	if (!cluster_mx_history_capture_read(collector, &saved))
		return false;
	if (mxkey == NULL || members == NULL || nmembers != saved.nmembers
		|| memcmp(mxkey, &saved.mxkey, sizeof(*mxkey)) != 0
		|| memcmp(members, saved.members, nmembers * sizeof(*members)) != 0
		|| !history_heap(rel, buffer, tuple, &fresh) || !history_pcm_equal(&saved, &fresh)
		|| saved.buffer != fresh.buffer || saved.relation_id != fresh.relation_id
		|| saved.page_lsn != fresh.page_lsn || saved.tuple_length != fresh.tuple_length
		|| memcmp(&saved.tid, &fresh.tid, sizeof(saved.tid)) != 0
		|| memcmp(&saved.line_pointer, &fresh.line_pointer, sizeof(saved.line_pointer)) != 0
		|| memcmp(saved.tuple_header, fresh.tuple_header, sizeof(saved.tuple_header)) != 0
		|| memcmp(saved.roots, fresh.roots, sizeof(saved.roots)) != 0) {
		cluster_mx_history_cancel(collector);
		return false;
	}
	/* If collection continues across another unlock, the next one-shot image
	 * must advance beyond this bracket too, not merely the first capture. */
	if (saved.read_only && saved.pcm.pcm_state == PCM_STATE_READ_IMAGE)
		collector->capture->pcm = fresh.pcm;
	return true;
}
#endif
