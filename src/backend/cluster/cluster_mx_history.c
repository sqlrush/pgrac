/*-------------------------------------------------------------------------
 * cluster_mx_history.c
 *    Capture and retention lifetime for backend-private history collection.
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_mx_history_member.h"
#ifdef USE_PGRAC_CLUSTER
#include "access/xact.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_multixact_operation_wire.h"
#include "storage/backendid.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_tt_local.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

typedef struct HistoryCollectorDriver {
	ClusterMxHistoryGraph *graph;
	ClusterMxHistoryRequestWire request;
	ClusterSavepointOperationKey *keys;
	ClusterCurrentMemberOperationProof *proofs;
	uint32 *capabilities;
	ClusterCurrentUpdaterProof updater;
	bool prepared, submitted, complete, members_ready, updater_ready, members_delivered;
} HistoryCollectorDriver;

struct ClusterMxHistoryCollector {
	ResourceOwner owner;
	Snapshot snapshot;
	ClusterMxHistoryCapture *capture;
	HistoryCollectorDriver *driver;
	ClusterCurrentUpdaterChallenge challenge;
	bool has_updater;
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
	if (collector->driver != NULL) {
		HistoryCollectorDriver *driver = collector->driver;
		collector->driver = NULL;
		if (driver->prepared)
			cluster_gcs_block_mx_history_cancel(&driver->request);
		cluster_mx_history_graph_destroy(driver->graph);
		if (driver->keys != NULL)
			pfree(driver->keys);
		pfree(driver);
	}
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
ClusterMxHistoryStep
cluster_mx_history_collect_poll(ClusterMxHistoryCollector *collector)
{
	ClusterMxHistoryCapture capture;
	HistoryCollectorDriver *driver;
	ClusterMxHistoryStep step;
	ClusterMxHistoryRequest request;
	bool needs_read;
	BufferDesc *desc;

	/* Delivery has consumed the graph. Keep its native retention lease until
	 * the caller explicitly cancels after consuming the copied results. */
	if (collector != NULL && collector->driver != NULL && collector->driver->members_delivered)
		return CMXH_UNKNOWN;
	if (CritSectionCount != 0)
		goto failed;
	if (!cluster_mx_history_capture_read(collector, &capture))
		return CMXH_UNKNOWN;
	if (capture.buffer <= 0 || capture.buffer > NBuffers || !BufferIsPinnedByMe(capture.buffer))
		goto failed;
	desc = GetBufferDescriptor(capture.buffer - 1);
	if (LWLockHeldByMeInMode(BufferDescriptorGetContentLock(desc), LW_EXCLUSIVE)
		|| LWLockHeldByMeInMode(BufferDescriptorGetContentLock(desc), LW_SHARED))
		goto failed;
	driver = collector->driver;
	if (driver == NULL) {
		driver
			= MemoryContextAllocExtended(TopTransactionContext, sizeof(*driver), MCXT_ALLOC_NO_OOM);
		if (driver == NULL)
			goto failed;
		memset(driver, 0, sizeof(*driver));
		collector->driver = driver;
		driver->graph = cluster_mx_history_graph_create(&capture, TopTransactionContext);
		if (driver->graph == NULL)
			goto failed;
	}
	if (driver->complete)
		return CMXH_COMPLETE;
	if (driver->prepared) {
		ClusterMxHistoryPeerResult result;
		if (!driver->submitted)
			driver->submitted = cluster_gcs_block_mx_history_submit(&driver->request);
		/* Poll also distinguishes closed admission from enqueue backpressure.
		 * A false enqueue cannot suppress scope/stream requalification. */
		step = cluster_gcs_block_mx_history_poll(&driver->request, &result);
		if (step == CMXH_PENDING)
			return step;
		if (step != CMXH_COMPLETE || !cluster_mx_history_graph_accept(driver->graph, &result))
			goto failed;
		driver->prepared = driver->submitted = false;
		memset(&driver->request, 0, sizeof(driver->request));
		return CMXH_PENDING;
	}
	step = cluster_mx_history_graph_next(driver->graph, &request, &needs_read);
	if (step == CMXH_COMPLETE) {
		driver->complete = true;
		return step;
	}
	if (step != CMXH_PENDING)
		goto failed;
	if (!needs_read)
		return CMXH_PENDING;
	step = cluster_gcs_block_mx_history_prepare_routed(&request, &driver->request);
	if (step == CMXH_PENDING)
		return step;
	if (step != CMXH_COMPLETE)
		goto failed;
	driver->prepared = true;
	return CMXH_PENDING;
failed:
	cluster_mx_history_cancel(collector);
	return CMXH_UNKNOWN;
}

bool
cluster_mx_history_collect_recheck_graph(ClusterMxHistoryCollector *collector, Relation rel,
										 Buffer buffer, HeapTuple tuple,
										 const ClusterCurrentMxKey *mxkey,
										 const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
										 const ClusterMxHistoryGraph **out)
{
	if (out == NULL)
		return false;
	*out = NULL;
	if (collector == NULL || collector->driver == NULL || !collector->driver->complete
		|| collector->driver->graph == NULL || collector->driver->members_delivered)
		return false;
	if (!cluster_mx_history_capture_recheck(collector, rel, buffer, tuple, mxkey, members,
											nmembers))
		return false;
	*out = collector->driver->graph;
	return true;
}

/* A is already staged. An absent B uses only the original updater provenance
 * FSM, via a top-only V6 challenge. In particular, do not attach A's exact key
 * here and accidentally run its three phases a second time. The original
 * outstanding slot owns cancellation and the event-driven backend wait. */
static bool
history_updater_fetch(ClusterMxHistoryCollector *collector)
{
	ClusterMxHistoryCapture capture;
	ClusterCurrentMxProofForwardV2 semantic = { 0 };
	ClusterCurrentMxOperationForwardV6 request;
	ClusterCurrentMemberOperationProof redundant;
	ClusterCurrentUpdaterProof b;
	ClusterCurrentMxUpdaterChallengeWire *wire = &semantic.trailer.body.updater.challenge;
	const ClusterCurrentUpdaterChallenge *challenge = &collector->challenge;
	uint16 count = 0;
	uint32 capability = 0;
	uint16 origin = challenge->candidate_next_xmin_alias.origin_node_id;
	volatile bool valid = false;

	if (!cluster_mx_history_capture_read(collector, &capture) || origin == cluster_node_id)
		return false;
	semantic.prefix.request_id = capture.collector_id;
	semantic.prefix.epoch = capture.read_epoch;
	semantic.prefix.mxkey = capture.mxkey;
	semantic.prefix.original_requester_node = cluster_node_id;
	semantic.prefix.requester_backend_id = MyBackendId;
	semantic.prefix.total_count = capture.nmembers;
	semantic.prefix.entry_count = 1;
	semantic.prefix.body_kind = CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE;
	semantic.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	ClusterCurrentMxProofPrefixSetDescriptorHash(&semantic.prefix, capture.descriptor_hash);
	semantic.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	semantic.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	wire->candidate_next_xmin_alias = challenge->candidate_next_xmin_alias;
	wire->candidate_next_xmin_locator = challenge->candidate_next_xmin_locator;
	wire->updater_xid = challenge->updater_xid;
	wire->member_ordinal = challenge->member_ordinal;
	wire->member_status = capture.members[challenge->member_ordinal].member_status;
	if (!cluster_multixact_operation_wire_upgrade_top_request(&semantic, cluster_node_id, origin,
															  capture.read_epoch, &request))
		return false;
	memset(&b, 0, sizeof(b));
	b.verdict = CUCP_UNKNOWN;
	PG_TRY();
	{
		valid = cluster_gcs_current_mx_operation_proof_fetch_and_wait(origin, &request, &redundant,
																	  1, &count, &b, &capability, 0)
				== CMX_RESOLVE_OK;
	}
	PG_CATCH();
	{
		/* Release the whole attempt after the original transport has released
		 * its outstanding slot; native Snapshot retention is dropped last. */
		cluster_mx_history_cancel(collector);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (!valid || count != 1 || capability == 0 || b.verdict != CUCP_MATCH || b.reserved8 != 0
		|| memcmp(&b.mxkey, &capture.mxkey, sizeof(b.mxkey)) != 0
		|| b.updater_xid != challenge->updater_xid || b.member_ordinal != challenge->member_ordinal
		|| memcmp(&b.candidate_next_xmin_alias, &challenge->candidate_next_xmin_alias,
				  sizeof(b.candidate_next_xmin_alias))
			   != 0
		|| !cluster_tx_locator_reply_matches(&challenge->candidate_next_xmin_locator,
											 &b.candidate_next_xmin_locator)
		|| !cluster_mx_history_capture_read(collector, &capture))
		return false;
	collector->driver->updater = b;
	return true;
}

ClusterMxHistoryStep
cluster_mx_history_poll(ClusterMxHistoryCollector *collector)
{
	ClusterMxHistoryStep step = cluster_mx_history_collect_poll(collector);
	HistoryCollectorDriver *driver;
	uint16 n;
	Size key_bytes, proof_bytes;

	if (step != CMXH_COMPLETE)
		return step;
	driver = collector->driver;
	if (driver->members_ready)
		return CMXH_COMPLETE;
	n = collector->capture->nmembers;
	key_bytes = MAXALIGN(n * sizeof(*driver->keys));
	proof_bytes = MAXALIGN(n * sizeof(*driver->proofs));
	driver->keys = MemoryContextAllocExtended(TopTransactionContext,
		key_bytes + proof_bytes + n * sizeof(*driver->capabilities), MCXT_ALLOC_NO_OOM);
	if (driver->keys == NULL)
		goto failed;
	driver->proofs = (ClusterCurrentMemberOperationProof *)((char *)driver->keys + key_bytes);
	driver->capabilities = (uint32 *)((char *)driver->proofs + proof_bytes);
	if (!cluster_mx_history_assemble_members_with_capabilities(collector->capture, driver->graph,
			driver->keys, driver->proofs, driver->capabilities, n))
		goto failed;
	if (collector->has_updater) {
		step
			= cluster_mx_history_assemble_updater(collector->capture, driver->graph, driver->proofs,
												  &collector->challenge, &driver->updater);
		if (step == CMXH_PENDING) {
			if (!history_updater_fetch(collector))
				goto failed;
		} else if (step != CMXH_COMPLETE)
			goto failed;
		driver->updater_ready = true;
	}
	driver->members_ready = true;
	return CMXH_COMPLETE;
failed:
	cluster_mx_history_cancel(collector);
	return CMXH_UNKNOWN;
}

static void
history_finish_unknown(ClusterSavepointOperationKey *keys,
					   ClusterCurrentMemberOperationProof *proofs, uint16 capacity)
{
	if (capacity > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return;
	if (keys != NULL)
		memset(keys, 0, capacity * sizeof(*keys));
	if (proofs != NULL) {
		memset(proofs, 0, capacity * sizeof(*proofs));
		for (uint16 i = 0; i < capacity; ++i) {
			proofs[i].base.state = CCM_UNKNOWN;
			proofs[i].format_version = CLUSTER_CURRENT_OPERATION_PROOF_VERSION;
		}
	}
}

static ClusterMxHistoryStep
history_finish(ClusterMxHistoryCollector *collector, Relation rel, Buffer buffer, HeapTuple tuple,
			   const ClusterCurrentMxKey *mxkey, const ClusterCurrentMxMemberDesc *members,
			   uint16 nmembers, ClusterSavepointOperationKey *keys,
			   ClusterCurrentMemberOperationProof *proofs, uint32 *capabilities,
			   bool require_capabilities, uint16 capacity,
			   const ClusterCurrentUpdaterChallenge *fresh_challenge,
			   ClusterCurrentUpdaterProof *updater)
{
	HistoryCollectorDriver *driver;
	uint16 n;

	if (updater != NULL) {
		memset(updater, 0, sizeof(*updater));
		updater->verdict = CUCP_UNKNOWN;
	}
	history_finish_unknown(keys, proofs, capacity);
	if (capabilities != NULL && capacity <= CLUSTER_CURRENT_MX_MAX_MEMBERS)
		memset(capabilities, 0, capacity * sizeof(*capabilities));
	if (collector == NULL || collector->capture == NULL)
		return CMXH_UNKNOWN;
	driver = collector->driver;
	if (driver != NULL && driver->members_delivered)
		return CMXH_UNKNOWN;
	n = collector->capture->nmembers;
	if (CritSectionCount != 0 || capacity > CLUSTER_CURRENT_MX_MAX_MEMBERS || capacity < n
		|| keys == NULL || proofs == NULL || (require_capabilities && capabilities == NULL)
		|| (collector->has_updater && (updater == NULL || fresh_challenge == NULL))
		|| (!collector->has_updater && fresh_challenge != NULL)) {
		cluster_mx_history_cancel(collector);
		return CMXH_UNKNOWN;
	}
	if (collector->has_updater
		&& memcmp(fresh_challenge, &collector->challenge, sizeof(*fresh_challenge)) != 0) {
		cluster_mx_history_cancel(collector);
		return CMXH_REQUALIFY;
	}
	if (driver == NULL || !driver->members_ready
		|| (collector->has_updater && !driver->updater_ready))
		return CMXH_PENDING;
	if (!cluster_mx_history_capture_recheck(collector, rel, buffer, tuple, mxkey, members,
											nmembers))
		return CMXH_REQUALIFY;
	memcpy(keys, driver->keys, n * sizeof(*keys));
	memcpy(proofs, driver->proofs, n * sizeof(*proofs));
	if (capabilities != NULL)
		memcpy(capabilities, driver->capabilities, n * sizeof(*capabilities));
	if (collector->has_updater)
		*updater = driver->updater;
	memset(&driver->updater, 0, sizeof(driver->updater));
	driver->members_delivered = true;
	driver->members_ready = false;
	cluster_mx_history_graph_destroy(driver->graph);
	driver->graph = NULL;
	pfree(driver->keys);
	driver->keys = NULL;
	driver->proofs = NULL;
	driver->capabilities = NULL;
	/* Keep capture/Snapshot until the last consumer calls cancel. Even an
	 * otherwise unregistered Snapshot may have this sole native reference. */
	return CMXH_COMPLETE;
}

bool
cluster_mx_history_capture_begin_with_updater(Relation rel, Buffer buffer, HeapTuple tuple,
											  Snapshot actual, const ClusterCurrentMxKey *mxkey,
											  const ClusterCurrentMxMemberDesc *members,
											  uint16 nmembers,
											  const ClusterSemanticAdmissionToken *admission,
											  const ClusterCurrentUpdaterChallenge *challenge,
											  ClusterMxHistoryCollector **out)
{
	if (out == NULL)
		return false;
	*out = NULL;
	if (challenge == NULL || challenge->reserved16 != 0 || members == NULL || admission == NULL
		|| challenge->member_ordinal >= nmembers || nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS
		|| !ISUPDATE_from_mxstatus(members[challenge->member_ordinal].member_status)
		|| members[challenge->member_ordinal].xid != challenge->updater_xid
		|| !cluster_multixact_current_successor_provenance_well_formed(
			&challenge->candidate_next_xmin_alias, &challenge->candidate_next_xmin_locator,
			challenge->updater_xid, challenge->candidate_next_xmin_alias.origin_node_id,
			admission->formation_epoch)
		|| !cluster_mx_history_capture_begin(rel, buffer, tuple, actual, mxkey, members, nmembers,
											 admission, out))
		return false;
	(*out)->has_updater = true;
	(*out)->challenge = *challenge;
	return true;
}

ClusterMxHistoryStep
cluster_mx_history_recheck_and_finish(ClusterMxHistoryCollector *collector, Relation rel,
									  Buffer buffer, HeapTuple tuple,
									  const ClusterCurrentMxKey *mxkey,
									  const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
									  ClusterSavepointOperationKey *keys,
									  ClusterCurrentMemberOperationProof *proofs, uint16 capacity)
{
	return history_finish(collector, rel, buffer, tuple, mxkey, members, nmembers, keys, proofs,
						  NULL, false, capacity, NULL, NULL);
}

ClusterMxHistoryStep
cluster_mx_history_recheck_and_finish_with_updater(
	ClusterMxHistoryCollector *collector, Relation rel, Buffer buffer, HeapTuple tuple,
	const ClusterCurrentMxKey *mxkey, const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	ClusterSavepointOperationKey *keys, ClusterCurrentMemberOperationProof *proofs, uint16 capacity,
	const ClusterCurrentUpdaterChallenge *fresh_challenge, ClusterCurrentUpdaterProof *updater)
{
	return history_finish(collector, rel, buffer, tuple, mxkey, members, nmembers, keys, proofs,
						  NULL, false, capacity, fresh_challenge, updater);
}

/* A receipt consumer cannot silently omit the original capability batch. */
ClusterMxHistoryStep
cluster_mx_history_recheck_and_finish_for_receipts(
	ClusterMxHistoryCollector *collector, Relation rel, Buffer buffer, HeapTuple tuple,
	const ClusterCurrentMxKey *mxkey, const ClusterCurrentMxMemberDesc *members, uint16 nmembers,
	ClusterSavepointOperationKey *keys, ClusterCurrentMemberOperationProof *proofs,
	uint32 *capabilities, uint16 capacity,
	const ClusterCurrentUpdaterChallenge *fresh_challenge, ClusterCurrentUpdaterProof *updater)
{
	return history_finish(collector, rel, buffer, tuple, mxkey, members, nmembers, keys, proofs,
		capabilities, true, capacity, fresh_challenge, updater);
}
#endif
