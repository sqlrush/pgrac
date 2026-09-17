/*-------------------------------------------------------------------------
 *
 * cluster_page_online_plan.c
 *    STOP-06 immutable online PAGE plan assembled before IR.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/cryptohash.h"
#include "cluster/cluster_page_online_plan.h"
#include "storage/bufpage.h"

#ifdef USE_CLUSTER_UNIT
#define online_alloc0(size_) calloc(1, (size_))
#define online_realloc(pointer_, size_) realloc((pointer_), (size_))
#define online_free(pointer_) free((pointer_))
#else
#define online_alloc0(size_) palloc0(size_)
#define online_realloc(pointer_, size_)                                                            \
	((pointer_) == NULL ? palloc0(size_) : repalloc((pointer_), (size_)))
#define online_free(pointer_) pfree(pointer_)
#endif

#define RF_PAGE_ONLINE_PLAN_MAGIC UINT32_C(0x52504f50)

typedef struct RfPageOnlineTargetV1 {
	RfPageOnlineTargetViewV1 view;
	RfPagePinnedSourceV1 source;
	RfContributorVectorV1 contributors;
	RfPageStableGraphRequestV1 graph;
	RfContributorStreamCutV1 *cuts;
	RfPageStableEdgeInputV1 *edges;
	uint32 edge_count;
	uint32 edge_capacity;
	PGAlignedBlock canonical;
} RfPageOnlineTargetV1;

struct RfPageOnlinePlanV1 {
	uint32 magic;
	bool sealed;
	uint8 reserved_zero[3];
	uint64 system_identifier;
	uint8 storage_uuid[16];
	uint32 participant_count;
	uint32 target_count;
	uint32 target_capacity;
	Size memory_budget;
	Size memory_used;
	uint64 retention_binding_cookie;
	RfContributorStreamCutV1 *physical_cuts;
	XLogRecPtr *last_record_end;
	bool *participant_seen;
	RfPageOnlineTargetV1 **targets;
};

typedef struct RfPageOnlinePendingV1 {
	RfPageOnlineTargetV1 *target;
	bool new_target;
	RfPageStableEdgeInputV1 edge;
	PGAlignedBlock canonical;
} RfPageOnlinePendingV1;

static bool
bytes_nonzero(const uint8 *bytes, Size size)
{
	uint8 value = 0;
	Size i;

	for (i = 0; i < size; i++)
		value |= bytes[i];
	return value != 0;
}

static bool
page_layout_valid(const char page[BLCKSZ])
{
	const PageHeader header = (const PageHeader)page;

	return !PageIsNew((Page)page) && PageGetPageSize((Page)page) == BLCKSZ
		   && PageGetPageLayoutVersion((Page)page) == PG_PAGE_LAYOUT_VERSION
		   && header->pd_lower >= SizeOfPageHeaderData && header->pd_lower <= header->pd_upper
		   && header->pd_upper <= header->pd_special && header->pd_special <= BLCKSZ;
}

static int
identity_compare(const RfPageIdentityV1 *left, const RfPageIdentityV1 *right)
{
	int cmp;

#define CMP_FIELD(field_)                                                                          \
	do {                                                                                           \
		if (left->field_ < right->field_)                                                          \
			return -1;                                                                             \
		if (left->field_ > right->field_)                                                          \
			return 1;                                                                              \
	} while (0)
	CMP_FIELD(system_identifier);
	cmp = memcmp(left->storage_uuid, right->storage_uuid, 16);
	if (cmp != 0)
		return cmp;
	CMP_FIELD(locator.spcOid);
	CMP_FIELD(locator.dbOid);
	CMP_FIELD(locator.relNumber);
	CMP_FIELD(forknum);
	CMP_FIELD(blockno);
	return 0;
#undef CMP_FIELD
}

static int
target_pointer_compare(const void *left, const void *right)
{
	const RfPageOnlineTargetV1 *const *left_target = (const RfPageOnlineTargetV1 *const *)left;
	const RfPageOnlineTargetV1 *const *right_target = (const RfPageOnlineTargetV1 *const *)right;

	return identity_compare(&(*left_target)->view.page_identity,
							&(*right_target)->view.page_identity);
}

static bool
plan_reserve(RfPageOnlinePlanV1 *plan, Size additional)
{
	if (additional > plan->memory_budget || plan->memory_used > plan->memory_budget - additional)
		return false;
	plan->memory_used += additional;
	return true;
}

static RfPageOnlineTargetV1 *
target_create(RfPageOnlinePlanV1 *plan, const RfPageIdentityV1 *identity)
{
	RfPageOnlineTargetV1 *target;
	Size bytes = sizeof(*target) + (Size)plan->participant_count * sizeof(*target->cuts);
	uint32 i;

	if (!plan_reserve(plan, bytes))
		return NULL;
	target = (RfPageOnlineTargetV1 *)online_alloc0(sizeof(*target));
	if (target == NULL)
		return NULL;
	target->cuts = (RfContributorStreamCutV1 *)online_alloc0((Size)plan->participant_count
															 * sizeof(*target->cuts));
	if (target->cuts == NULL) {
		online_free(target);
		return NULL;
	}
	target->view.page_identity = *identity;
	for (i = 0; i < plan->participant_count; i++) {
		target->cuts[i] = plan->physical_cuts[i];
		target->cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE | RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
		target->cuts[i].scan_end_exclusive = target->cuts[i].scan_begin_inclusive;
		target->cuts[i].contributor_count = 0;
		target->cuts[i].component_count = 0;
	}
	return target;
}

static void
target_destroy(RfPageOnlineTargetV1 *target)
{
	if (target == NULL)
		return;
	if (target->edges != NULL)
		online_free(target->edges);
	if (target->cuts != NULL)
		online_free(target->cuts);
	online_free(target);
}

static RfPageOnlineTargetV1 *
find_target(const RfPageOnlinePlanV1 *plan, const RfPageIdentityV1 *identity)
{
	uint32 i;

	for (i = 0; i < plan->target_count; i++)
		if (rf_page_identity_equal_v1(&plan->targets[i]->view.page_identity, identity))
			return plan->targets[i];
	return NULL;
}

static bool
ensure_target_capacity(RfPageOnlinePlanV1 *plan, uint32 additional)
{
	uint32 required = plan->target_count + additional;
	uint32 capacity;
	Size delta;
	void *new_targets;

	if (required <= plan->target_capacity)
		return true;
	capacity = plan->target_capacity == 0 ? 8 : plan->target_capacity;
	while (capacity < required) {
		if (capacity > RF_PAGE_STABLE_MAX_EDGES / 2) {
			capacity = RF_PAGE_STABLE_MAX_EDGES;
			break;
		}
		capacity *= 2;
	}
	if (capacity < required)
		return false;
	delta = (Size)(capacity - plan->target_capacity) * sizeof(*plan->targets);
	if (!plan_reserve(plan, delta))
		return false;
	new_targets = online_realloc(plan->targets, (Size)capacity * sizeof(*plan->targets));
	if (new_targets == NULL)
		return false;
	plan->targets = (RfPageOnlineTargetV1 **)new_targets;
	memset(plan->targets + plan->target_capacity, 0, delta);
	plan->target_capacity = capacity;
	return true;
}

static bool
ensure_edge_capacity(RfPageOnlinePlanV1 *plan, RfPageOnlineTargetV1 *target, uint32 additional)
{
	uint32 required = target->edge_count + additional;
	uint32 capacity;
	Size delta;
	void *new_edges;

	if (required <= target->edge_capacity)
		return true;
	capacity = target->edge_capacity == 0 ? 4 : target->edge_capacity;
	while (capacity < required) {
		if (capacity > RF_PAGE_STABLE_MAX_EDGES / 2) {
			capacity = RF_PAGE_STABLE_MAX_EDGES;
			break;
		}
		capacity *= 2;
	}
	if (capacity < required)
		return false;
	delta = (Size)(capacity - target->edge_capacity) * sizeof(*target->edges);
	if (!plan_reserve(plan, delta))
		return false;
	new_edges = online_realloc(target->edges, (Size)capacity * sizeof(*target->edges));
	if (new_edges == NULL)
		return false;
	target->edges = (RfPageStableEdgeInputV1 *)new_edges;
	memset(target->edges + target->edge_capacity, 0, delta);
	target->edge_capacity = capacity;
	return true;
}

static bool
page_sha256(const char page[BLCKSZ], uint8 digest[32])
{
	pg_cryptohash_ctx *context;
	bool ok;

	context = pg_cryptohash_create(PG_SHA256);
	if (context == NULL)
		return false;
	ok = pg_cryptohash_init(context) >= 0
		 && pg_cryptohash_update(context, (const uint8 *)page, BLCKSZ) >= 0
		 && pg_cryptohash_final(context, digest, 32) >= 0;
	pg_cryptohash_free(context);
	return ok;
}

static bool
anchor_flags(uint16 flags)
{
	return (flags & RF_PAGE_EDGE_FULL_COVERAGE) != 0
		   && (flags & (RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_WILL_INIT)) != 0;
}

static RfPageProofDetailV1
record_identity_validate(RfPageOnlinePlanV1 *plan, const RfDetachedRecordPlanV1 *record_plan,
						 const RfPageOnlineRecordIdentityV1 *identity)
{
	const RfContributorStreamCutV1 *cut;
	const RfPageReplayRecordIdentityV1 *record;
	const XLogReaderState *reader;
	const DecodedXLogRecord *decoded;

	if (record_plan == NULL || !record_plan->preflight_complete
		|| record_plan->source_record == NULL || record_plan->source_record->record == NULL
		|| identity == NULL)
		return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	reader = record_plan->source_record;
	decoded = reader->record;
	record = &identity->record;
	if (identity->reserved_zero != 0 || record->reserved_zero != 0 || record->reserved_zero2 != 0
		|| identity->participant_index >= plan->participant_count
		|| record->system_identifier != plan->system_identifier
		|| memcmp(record->storage_uuid, plan->storage_uuid, 16) != 0 || record->origin_thread == 0
		|| record->timeline_id == 0 || record->read_rec_ptr == InvalidXLogRecPtr
		|| record->end_rec_ptr == InvalidXLogRecPtr || record->read_rec_ptr >= record->end_rec_ptr
		|| reader->system_identifier != record->system_identifier
		|| reader->ReadRecPtr != record->read_rec_ptr || reader->EndRecPtr != record->end_rec_ptr
		|| decoded->lsn != record->read_rec_ptr || decoded->next_lsn != record->end_rec_ptr
		|| (uint32)decoded->header.xl_crc != record->record_crc
		|| decoded->header.xl_rmid != record->rmid || decoded->header.xl_info != record->info)
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	cut = &plan->physical_cuts[identity->participant_index];
	if (record->origin_thread != cut->failed_thread || record->timeline_id != cut->timeline_id)
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	if ((cut->flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0
		|| record->read_rec_ptr < cut->scan_begin_inclusive
		|| record->end_rec_ptr > cut->scan_end_exclusive
		|| (plan->participant_seen[identity->participant_index]
			&& record->read_rec_ptr < plan->last_record_end[identity->participant_index]))
		return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_page_online_plan_create_v1(const RfPageOnlinePlanRequestV1 *request,
							  RfPageOnlinePlanV1 **out_plan)
{
	RfPageOnlinePlanV1 *plan;
	Size budget;
	Size arrays_size;
	uint32 i;

	if (out_plan == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	*out_plan = NULL;
	if (request == NULL || request->system_identifier == 0
		|| !bytes_nonzero(request->storage_uuid, 16) || request->physical_cuts == NULL
		|| request->participant_count == 0
		|| request->participant_count > RF_PAGE_STABLE_MAX_PARTICIPANTS
		|| request->retention_binding_cookie == 0)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	budget = request->memory_budget == 0 ? RF_PAGE_ONLINE_PLAN_MAX_BYTES : request->memory_budget;
	if (budget > RF_PAGE_ONLINE_PLAN_MAX_BYTES || budget < sizeof(*plan))
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	for (i = 0; i < request->participant_count; i++) {
		const RfContributorStreamCutV1 *cut = &request->physical_cuts[i];
		bool empty = (cut->flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0;

		if (cut->failed_thread == 0 || cut->timeline_id == 0
			|| (cut->flags & RF_CONTRIBUTOR_CUT_COMPLETE) == 0
			|| (cut->flags & ~RF_CONTRIBUTOR_CUT_KNOWN_MASK) != 0
			|| (empty && cut->scan_begin_inclusive != cut->scan_end_exclusive)
			|| (!empty && cut->scan_begin_inclusive >= cut->scan_end_exclusive)
			|| (i > 0 && request->physical_cuts[i - 1].failed_thread >= cut->failed_thread))
			return RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
	}
	arrays_size = (Size)request->participant_count
				  * (sizeof(*plan->physical_cuts) + sizeof(*plan->last_record_end)
					 + sizeof(*plan->participant_seen));
	if (sizeof(*plan) > budget - Min(budget, arrays_size))
		return RF_PAGE_PROOF_DETAIL_CAPACITY;
	plan = (RfPageOnlinePlanV1 *)online_alloc0(sizeof(*plan));
	if (plan == NULL)
		return RF_PAGE_PROOF_DETAIL_OOM;
	plan->memory_budget = budget;
	plan->memory_used = sizeof(*plan);
	if (!plan_reserve(plan, arrays_size)) {
		online_free(plan);
		return RF_PAGE_PROOF_DETAIL_CAPACITY;
	}
	plan->physical_cuts = (RfContributorStreamCutV1 *)online_alloc0((Size)request->participant_count
																	* sizeof(*plan->physical_cuts));
	plan->last_record_end = (XLogRecPtr *)online_alloc0((Size)request->participant_count
														* sizeof(*plan->last_record_end));
	plan->participant_seen
		= (bool *)online_alloc0((Size)request->participant_count * sizeof(*plan->participant_seen));
	if (plan->physical_cuts == NULL || plan->last_record_end == NULL
		|| plan->participant_seen == NULL) {
		rf_page_online_plan_destroy_v1(&plan);
		return RF_PAGE_PROOF_DETAIL_OOM;
	}
	memcpy(plan->physical_cuts, request->physical_cuts,
		   (Size)request->participant_count * sizeof(*plan->physical_cuts));
	plan->magic = RF_PAGE_ONLINE_PLAN_MAGIC;
	plan->system_identifier = request->system_identifier;
	memcpy(plan->storage_uuid, request->storage_uuid, 16);
	plan->participant_count = request->participant_count;
	plan->retention_binding_cookie = request->retention_binding_cookie;
	*out_plan = plan;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_page_online_plan_feed_record_v1(RfPageOnlinePlanV1 *plan,
								   const RfDetachedRecordPlanV1 *record_plan,
								   const RfPageOnlineRecordIdentityV1 *identity)
{
	RfPageOnlinePendingV1 *pending;
	uint32 page_count = 0;
	uint32 new_target_count = 0;
	uint32 i;
	RfPageProofDetailV1 detail;

	if (plan == NULL || plan->magic != RF_PAGE_ONLINE_PLAN_MAGIC || plan->sealed)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	detail = record_identity_validate(plan, record_plan, identity);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	if (record_plan->component_count > RF_PAGE_STABLE_MAX_COMPONENTS)
		return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
	for (i = 0; i < record_plan->component_count; i++)
		if (record_plan->components[i].owner == RF_DETACHED_COMPONENT_PAGE_CODEC)
			page_count++;
	if (page_count == 0) {
		plan->participant_seen[identity->participant_index] = true;
		plan->last_record_end[identity->participant_index] = identity->record.end_rec_ptr;
		return RF_PAGE_PROOF_DETAIL_OK;
	}
	if (plan->memory_used
		> plan->memory_budget - Min(plan->memory_budget, (Size)page_count * sizeof(*pending)))
		return RF_PAGE_PROOF_DETAIL_CAPACITY;
	pending = (RfPageOnlinePendingV1 *)online_alloc0((Size)page_count * sizeof(*pending));
	if (pending == NULL)
		return RF_PAGE_PROOF_DETAIL_OOM;
	page_count = 0;
	for (i = 0; i < record_plan->component_count; i++) {
		const RfDetachedComponentPlanV1 *component = &record_plan->components[i];
		const DecodedBkpBlock *block;
		RfPageOnlinePendingV1 *item;
		RfPageIdentityV1 page_identity;
		const char *old_page;
		uint32 j;

		if (component->owner != RF_DETACHED_COMPONENT_PAGE_CODEC)
			continue;
		if (component->block_id > record_plan->source_record->record->max_block_id) {
			detail = RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
			goto fail;
		}
		block = &record_plan->source_record->record->blocks[component->block_id];
		memset(&page_identity, 0, sizeof(page_identity));
		page_identity.system_identifier = plan->system_identifier;
		memcpy(page_identity.storage_uuid, plan->storage_uuid, 16);
		page_identity.locator = block->rlocator;
		page_identity.forknum = (uint32)block->forknum;
		page_identity.blockno = block->blkno;
		if (!rf_page_identity_valid_v1(&page_identity)) {
			detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
			goto fail;
		}
		for (j = 0; j < page_count; j++)
			if (rf_page_identity_equal_v1(&pending[j].edge.page_identity, &page_identity)) {
				detail = RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
				goto fail;
			}
		item = &pending[page_count++];
		item->target = find_target(plan, &page_identity);
		if (item->target == NULL) {
			if (!anchor_flags(component->edge_flags)) {
				detail = RF_PAGE_PROOF_DETAIL_ANCHOR_MISSING;
				goto fail;
			}
			item->target = target_create(plan, &page_identity);
			if (item->target == NULL) {
				detail = RF_PAGE_PROOF_DETAIL_CAPACITY;
				goto fail;
			}
			item->new_target = true;
			new_target_count++;
			old_page = item->target->canonical.data;
		} else {
			if (component->before_kind != RF_PAGE_STATE_PRESENT
				|| !rf_page_version_equal_v1(&item->target->view.expected_result,
											 &component->before)) {
				detail = RF_PAGE_PROOF_DETAIL_EDGE_GAP;
				goto fail;
			}
			old_page = item->target->canonical.data;
		}
		detail = rf_page_detached_apply_v1(record_plan, i, old_page, item->canonical.data);
		if (detail != RF_PAGE_PROOF_DETAIL_OK)
			goto fail;
		if (!page_layout_valid(item->canonical.data)
			|| ((PageHeader)item->canonical.data)->pd_block_scn
				   != component->result.mutation_token) {
			detail = RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED;
			goto fail;
		}
		item->edge.page_identity = page_identity;
		item->edge.edge.block_id = component->block_id;
		item->edge.edge.page_class = component->page_class;
		item->edge.edge.before_kind = component->before_kind;
		item->edge.edge.result_kind = component->result_kind;
		item->edge.edge.edge_flags = component->edge_flags;
		item->edge.edge.component_ordinal = component->component_ordinal;
		item->edge.edge.before = component->before;
		memcpy(item->edge.edge.result_incarnation, component->result.segment_incarnation, 16);
		item->edge.result_token = component->result.mutation_token;
		item->edge.record_identity = identity->record;
		item->edge.participant_index = identity->participant_index;
		item->edge.component_count = (uint16)record_plan->component_count;
		item->edge.record_complete = true;
		item->edge.opcode_supported = true;
		item->edge.side_complete = true;
		item->edge.image_integrity_ok = true;
		if (anchor_flags(component->edge_flags)
			&& !page_sha256(item->canonical.data, item->edge.anchor_digest)) {
			detail = RF_PAGE_PROOF_DETAIL_INTERNAL;
			goto fail;
		}
	}
	if (!ensure_target_capacity(plan, new_target_count)) {
		detail = RF_PAGE_PROOF_DETAIL_CAPACITY;
		goto fail;
	}
	for (i = 0; i < page_count; i++)
		if (!ensure_edge_capacity(plan, pending[i].target, 1)) {
			detail = RF_PAGE_PROOF_DETAIL_CAPACITY;
			goto fail;
		}
	for (i = 0; i < page_count; i++) {
		RfPageOnlinePendingV1 *item = &pending[i];
		RfPageOnlineTargetV1 *target = item->target;
		RfContributorStreamCutV1 *cut = &target->cuts[identity->participant_index];

		if (item->new_target) {
			target->view.before_kind = item->edge.edge.before_kind;
			target->view.expected_before = item->edge.edge.before;
			plan->targets[plan->target_count++] = target;
			item->new_target = false;
		}
		target->edges[target->edge_count++] = item->edge;
		target->view.expected_result.segment_incarnation[0] = 0;
		memcpy(target->view.expected_result.segment_incarnation, item->edge.edge.result_incarnation,
			   16);
		target->view.expected_result.mutation_token = item->edge.result_token;
		memcpy(target->canonical.data, item->canonical.data, BLCKSZ);
		if ((cut->flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0) {
			cut->flags = RF_CONTRIBUTOR_CUT_COMPLETE;
			cut->scan_begin_inclusive = identity->record.read_rec_ptr;
		}
		cut->scan_end_exclusive = identity->record.end_rec_ptr;
		cut->contributor_count++;
		cut->component_count += record_plan->component_count;
		if (anchor_flags(item->edge.edge.edge_flags))
			target->source.source_version = target->view.expected_result;
	}
	plan->participant_seen[identity->participant_index] = true;
	plan->last_record_end[identity->participant_index] = identity->record.end_rec_ptr;
	online_free(pending);
	return RF_PAGE_PROOF_DETAIL_OK;

fail:
	for (i = 0; i < page_count; i++)
		if (pending[i].new_target)
			target_destroy(pending[i].target);
	online_free(pending);
	return detail;
}

RfPageProofDetailV1
rf_page_online_plan_seal_v1(RfPageOnlinePlanV1 *plan)
{
	uint32 i;

	if (plan == NULL || plan->magic != RF_PAGE_ONLINE_PLAN_MAGIC || plan->sealed)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	for (i = 0; i < plan->participant_count; i++) {
		bool empty = (plan->physical_cuts[i].flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0;

		if (empty != !plan->participant_seen[i]
			|| (!empty && plan->last_record_end[i] != plan->physical_cuts[i].scan_end_exclusive))
			return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	}
	qsort(plan->targets, plan->target_count, sizeof(*plan->targets), target_pointer_compare);
	for (i = 0; i < plan->target_count; i++) {
		RfPageOnlineTargetV1 *target = plan->targets[i];

		if (target->edge_count == 0 || !rf_page_version_present_v1(&target->source.source_version))
			return RF_PAGE_PROOF_DETAIL_ANCHOR_MISSING;
		target->source.page_identity = target->view.page_identity;
		target->source.binding_cookie = plan->retention_binding_cookie;
		target->source.current_binding_cookie = plan->retention_binding_cookie;
		target->source.identity_verified = true;
		target->source.integrity_verified = true;
		target->contributors.system_identifier = plan->system_identifier;
		memcpy(target->contributors.storage_uuid, plan->storage_uuid, 16);
		target->contributors.participant_count = plan->participant_count;
		target->contributors.edge_count = target->edge_count;
		target->contributors.cuts = target->cuts;
		target->contributors.edges = target->edges;
		memset(&target->graph, 0, sizeof(target->graph));
		target->graph.page_identity = target->view.page_identity;
		target->graph.expected_result = target->view.expected_result;
		target->graph.contributors = &target->contributors;
		target->graph.source = &target->source;
		target->graph.participant_count = plan->participant_count;
		target->view.canonical_page = target->canonical.data;
		target->view.source = &target->source;
		target->view.contributors = &target->contributors;
		target->view.graph = &target->graph;
	}
	plan->sealed = true;
	return RF_PAGE_PROOF_DETAIL_OK;
}

uint32
rf_page_online_plan_target_count_v1(const RfPageOnlinePlanV1 *plan)
{
	return plan != NULL && plan->magic == RF_PAGE_ONLINE_PLAN_MAGIC && plan->sealed
			   ? plan->target_count
			   : 0;
}

bool
rf_page_online_plan_target_v1(const RfPageOnlinePlanV1 *plan, uint32 index,
							  RfPageOnlineTargetViewV1 *out_target)
{
	if (plan == NULL || plan->magic != RF_PAGE_ONLINE_PLAN_MAGIC || !plan->sealed
		|| index >= plan->target_count || out_target == NULL)
		return false;
	*out_target = plan->targets[index]->view;
	return true;
}

void
rf_page_online_plan_destroy_v1(RfPageOnlinePlanV1 **plan_pointer)
{
	RfPageOnlinePlanV1 *plan;

	if (plan_pointer == NULL || *plan_pointer == NULL)
		return;
	plan = *plan_pointer;
	if (plan->magic == RF_PAGE_ONLINE_PLAN_MAGIC) {
		uint32 i;

		for (i = 0; i < plan->target_count; i++)
			target_destroy(plan->targets[i]);
	}
	if (plan->targets != NULL)
		online_free(plan->targets);
	if (plan->participant_seen != NULL)
		online_free(plan->participant_seen);
	if (plan->last_record_end != NULL)
		online_free(plan->last_record_end);
	if (plan->physical_cuts != NULL)
		online_free(plan->physical_cuts);
	plan->magic = 0;
	online_free(plan);
	*plan_pointer = NULL;
}
