/*-------------------------------------------------------------------------
 * test_cluster_resource_x_pi_replace.c
 *    Actual holder capacity, PI references, PAGE coverage and LMS handoff.
 *
 * Authenticated input, native completion, DATA proof and detached redo are
 * explicit fixture seams. The real immutable plan queue/seal/coverage and
 * PCM owners run here; ordinary native callers are not qualified here.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#define cluster_page_wal_ref_retain_v1 replace_ref_retain
#define cluster_page_wal_ref_release_v1 replace_ref_release
#include "test_cluster_resource_x_pi_fixture.inc"
#undef cluster_page_wal_ref_retain_v1
#undef cluster_page_wal_ref_release_v1
#include "cluster/cluster_page_online_plan.h"
#include "access/xlogreader.h"

/* On the parent, exercise its real refusal, not a missing-symbol stub. */
#if !defined(CLUSTER_PCM_COVERED_PI_SOURCE_V1) || defined(CLUSTER_TEST_PI_REPLACE_BASELINE)
#define cluster_pcm_lock_resource_x_acquire_s_source_covered_prepare_exact( \
 block_, master_, id_, native_, page_, wal_, rc_, mc_, r4_, auth_, plan_) \
 cluster_pcm_lock_resource_x_acquire_s_source_service_prepare_exact( \
 block_, master_, id_, native_, page_, wal_, rc_, mc_, r4_, auth_)
#endif

typedef struct ReplaceFixture {
	PiFixture old;
	ResourceXDecodedFrame request, block;
	ClusterPcmOwnSnapshot prepared, finished;
	ClusterPageWalBindingV1 last;
	PGAlignedBlock page;
	RfPageOnlinePlanV1 *plan;
} ReplaceFixture;

/* Standalone allocation and version comparison leaves for the actual plan. */
static int replace_allocations, replace_references;
static bool replace_cancel_after_retain;
extern bool cluster_page_wal_ref_retain_v1(const ClusterPageWalBindingV1 *, ClusterPageWalRefV1 *);
extern bool cluster_page_wal_ref_release_v1(ClusterPageWalRefV1 *);
void *palloc(Size size) { void *p = malloc(size); if (!p) abort(); replace_allocations++; return p; }
void pfree(void *p) { if (p) replace_allocations--; free(p); }
bool
replace_ref_retain(const ClusterPageWalBindingV1 *binding, ClusterPageWalRefV1 *ref)
{
	bool retained = cluster_page_wal_ref_retain_v1(binding, ref);
	if (retained) replace_references++;
	/* Explicit cancellation seam after actual reference reservation, with
	 * the original WAL lock already released. */
	if (retained && replace_cancel_after_retain && binding->version.mutation_token == 90) {
		replace_cancel_after_retain = false; pg_re_throw();
	}
	return retained;
}
bool
replace_ref_release(ClusterPageWalRefV1 *ref)
{
	bool released = cluster_page_wal_ref_release_v1(ref);
	if (released) replace_references--;
	return released;
}
bool
rf_page_version_equal_v1(const RfPageVersionV1 *a, const RfPageVersionV1 *b)
{
	return a != NULL && b != NULL && a->mutation_token == b->mutation_token
		&& memcmp(a->segment_incarnation, b->segment_incarnation, 16) == 0;
}

/* The detached codec is a named input boundary. An invalid dependency or
 * changed image must still be rejected by the actual sealed plan/consumer. */
RfPageProofDetailV1
rf_page_detached_apply_v1(const RfDetachedRecordPlanV1 *plan, uint32 index,
	const char old_page[BLCKSZ], char new_page[BLCKSZ])
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	memset(new_page, 0, BLCKSZ);
	((PageHeader)new_page)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)new_page)->pd_upper = BLCKSZ;
	((PageHeader)new_page)->pd_special = BLCKSZ;
	((PageHeader)new_page)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	((PageHeader)new_page)->pd_block_scn = plan->components[index].result.mutation_token;
	new_page[BLCKSZ - 1] = (char)plan->components[index].result.mutation_token;
	return RF_PAGE_PROOF_DETAIL_OK;
}

static bool
replace_plan(ReplaceFixture *f)
{
	const ClusterPageWalBindingV1 *bindings[] = {&f->old.first, &f->old.last, &f->last};
	RfContributorStreamCutV1 cut = {0};
	RfPageOnlinePlanRequestV1 request = {0};
	RfPageOnlineTargetViewV1 target = {0};
	cut.failed_thread = f->last.source.claim.identity.origin_thread_id;
	cut.timeline_id = f->last.source.timeline;
	cut.origin_owner_incarnation = f->last.source.claim.identity.origin_owner_incarnation;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = 80; cut.scan_end_exclusive = 200;
	request.system_identifier = f->last.identity.system_identifier;
	memcpy(request.storage_uuid, f->last.identity.storage_uuid, 16);
	request.physical_cuts = &cut; request.participant_count = 1;
	request.retention_binding_cookie = 41;
	UT_ASSERT_EQ(rf_page_online_plan_create_v1(&request, &f->plan), RF_PAGE_PROOF_DETAIL_OK);
	if (!f->plan) return false;
	UT_ASSERT(rf_page_online_plan_bind_sources_v1(f->plan, &f->last.source, 1));
	for (unsigned n = 0; n < 3; n++) {
		const ClusterPageWalBindingV1 *b = bindings[n];
		XLogReaderState reader = {0};
		union { DecodedXLogRecord record; char bytes[sizeof(DecodedXLogRecord) + sizeof(DecodedBkpBlock)]; } storage = {0};
		RfDetachedRecordPlanV1 plan = {0};
		RfDetachedComponentPlanV1 *c = &plan.components[0];
		RfPageOnlineRecordIdentityV1 identity = {0};
		reader.record = &storage.record;
		reader.system_identifier = b->identity.system_identifier;
		reader.ReadRecPtr = storage.record.lsn = b->record_start;
		reader.EndRecPtr = storage.record.next_lsn = b->record_end;
		storage.record.header.xl_crc = b->record_crc;
		storage.record.header.xl_rmid = b->rmid; storage.record.header.xl_info = b->info;
		storage.record.blocks[0].in_use = true;
		storage.record.blocks[0].rlocator = b->identity.locator;
		storage.record.blocks[0].forknum = b->identity.forknum;
		storage.record.blocks[0].blkno = b->identity.blockno;
		plan.source_record = &reader; plan.preflight_complete = true; plan.component_count = 1;
		c->page_class = RF_PAGE_CLASS_ORDINARY; c->owner = RF_DETACHED_COMPONENT_PAGE_CODEC;
		c->before_kind = c->result_kind = RF_PAGE_STATE_PRESENT;
		c->before = n ? bindings[n - 1]->version : b->version;
		if (n == 0) c->before.mutation_token = 60;
		c->result = b->version;
		if (n == 0) c->edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
		identity.record.system_identifier = b->identity.system_identifier;
		memcpy(identity.record.storage_uuid, b->identity.storage_uuid, 16);
		identity.record.origin_thread = cut.failed_thread; identity.record.timeline_id = cut.timeline_id;
		identity.record.read_rec_ptr = b->record_start; identity.record.end_rec_ptr = b->record_end;
		identity.record.record_crc = b->record_crc; identity.record.rmid = b->rmid; identity.record.info = b->info;
		UT_ASSERT_EQ(rf_page_online_plan_queue_record_v1(f->plan, &plan, &identity), RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(f->plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_page_online_plan_target_v1(f->plan, 0, &target));
	if (!target.canonical_page) return false;
	memcpy(f->page.data, target.canonical_page, BLCKSZ);
	UT_ASSERT(rf_page_online_plan_image_covers_v1(f->plan, &f->last, f->page.data, &f->old.first, &f->old.last));
	return true;
}

static bool
replace_start_phase(ReplaceFixture *f, unsigned phase)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint16 length; ResourceXWireReject reject;
	memset(f, 0, sizeof(*f));
	if (!pi_start_master(&f->old, true, 2)) return false;
	f->request = pi_request(&f->old);
	if (phase > 0) UT_ASSERT_EQ(clean_receive(&f->request), RESOURCE_X_APPLY_APPLIED);
	if (phase > 1) clean_send(&f->old.source.m.state->holder_status_intent.slot);
	f->block = f->old.source.block;
	f->block.common.logical_assertion.requester_node = 3;
	f->block.common.assertion_sequence = 1;
	f->block.common.base_authority_generation = f->block.common.authority_generation++;
	f->block.body.acquire_decision.enqueue_order++;
	f->block.body.acquire_decision.source_service_generation++;
	f->block.body.acquire_decision.episode.requester_incarnation = f->block.body.acquire_decision.holders[3].boot_incarnation;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &f->block, bytes, sizeof(bytes), &length, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, length, &f->block, &reject));
	f->prepared = f->old.source.prepared; f->finished = f->old.source.finished;
	f->last = f->old.last; f->last.record_start = 160; f->last.record_end = 200;
	f->last.version.mutation_token = 90;
	return replace_plan(f);
}

static bool replace_start(ReplaceFixture *f, bool quiet) { return replace_start_phase(f, quiet ? 2 : 1); }

static bool
replace_block_encode(ReplaceFixture *f)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint16 length; ResourceXWireReject reject;
	return cluster_resource_x_wire_encode(RESOURCE_X_MSG_BLOCK_TO_N, &f->block, bytes, sizeof(bytes), &length, &reject)
		&& cluster_resource_x_wire_decode(RESOURCE_X_MSG_BLOCK_TO_N, bytes, length, &f->block, &reject);
}

static ResourceXApplyResult
replace_prepare(ReplaceFixture *f)
{
	return cluster_pcm_lock_resource_x_acquire_s_source_covered_prepare_exact(&f->block,
		2, 7, &f->prepared, f->page.data, &f->last, 83, 61, 87, 61, f->plan);
}

static bool
replace_claim(ResourceXSourceDrainWork *work)
{
	BackendType saved = MyBackendType;
	ResourceXIntentSlot slot; ResourceXAcquisitionRef delivery;
	uint8 payload[RESOURCE_X_ACQUIRE_IMAGE_V4_BYTES]; uint32 examined; bool found = false;
	MyBackendType = B_LMS;
	for (int n = 0; n < 256; n++) {
		ResourceXIntentProbeResult r = cluster_pcm_lock_resource_x_outbound_work_probe_exact(
			4, &slot, payload, sizeof(payload), &examined, &delivery);
		UT_ASSERT(r != RESOURCE_X_INTENT_PROBE_CORRUPT);
		if (r == RESOURCE_X_INTENT_PROBE_SOURCE_DRAIN) { memcpy(work, payload, sizeof(*work)); found = true; break; }
		if (r == RESOURCE_X_INTENT_PROBE_IDLE || r == RESOURCE_X_INTENT_PROBE_COMPLETE) break;
	}
	MyBackendType = saved; UT_ASSERT_EQ(fake_lwlock_depth, 0); return found;
}

static void
replace_end(ReplaceFixture *f)
{
	rf_page_online_plan_destroy_v1(&f->plan); pi_end();
}

static ResourceXDecodedFrame
replace_receipt(ReplaceFixture *f)
{
	ResourceXDecodedFrame grant = f->block, request = f->old.source.done, status;
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES]; uint16 length; ResourceXWireReject reject;
	grant.kind = RESOURCE_X_WIRE_AUTHORITY_GRANT;
	grant.common.action_node = grant.common.logical_assertion.requester_node;
	UT_ASSERT(cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant,
		bytes, sizeof(bytes), &length, &reject));
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT, bytes, length, &grant, &reject));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_holder_status_exact(&f->block.common.logical_assertion,
		&status), RESOURCE_X_APPLY_APPLIED);
	request.common = f->block.common;
	request.common.action_node = 0; request.common.outcome = RESOURCE_X_OUTCOME_OK;
	request.body.acquire_completion.decision = f->block.body.acquire_decision;
	request.body.acquire_completion.native_generation = f->block.body.acquire_decision.episode.base_generation + 1;
	request.body.acquire_completion.decision_crc32c = grant.common.semantic_crc32c;
	request.body.acquire_completion.page_lsn = 200;
	request.body.acquire_completion.page_scn = status.body.acquire_witness.original_proof.page_scn_lsn;
	request.body.acquire_completion.page_crc32c = status.body.acquire_witness.original_proof.page_checksum;
	request.kind = RESOURCE_X_WIRE_SOURCE_SETTLEMENT_V2;
	UT_ASSERT(clean_encode(&request));
	return request;
}

UT_TEST(test_covered_successor_keeps_earliest_ref_and_original_lms_work)
{
	ReplaceFixture f; ResourceXSourceDrainWork work;
	ClusterPageWalBindingV1 first, last; ClusterPcmLocalPiSnapshotV1 local;
	if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
	if (replace_prepare(&f) != RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); replace_end(&f); return; }
	UT_ASSERT_EQ(f.old.source.m.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 1);
	UT_ASSERT(cluster_page_wal_ref_read_v1(&f.old.source.m.state->pi_carrier.first,
		f.last.identity.locator, MAIN_FORKNUM, f.last.identity.blockno, &first));
	UT_ASSERT_EQ(first.version.mutation_token, 70);
	UT_ASSERT(cluster_page_wal_ref_read_v1(&f.old.source.m.state->pi_carrier.last,
		f.last.identity.locator, MAIN_FORKNUM, f.last.identity.blockno, &last));
	UT_ASSERT_EQ(last.version.mutation_token, 90);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f.old.local.resource, &local));
	UT_ASSERT(memcmp(&local, &f.old.local, sizeof(local)) == 0);
	UT_ASSERT(replace_claim(&work)); UT_ASSERT_EQ(work.r4_record_generation, 87);
	UT_ASSERT_EQ(work.request.authority_generation, f.block.common.authority_generation);
	replace_end(&f);
}

UT_TEST(test_covered_pending_publication_keeps_transport_and_pi_separate)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ClusterPcmLocalPiFloorV1 floor;
	if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
	if (replace_prepare(&f) != RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); replace_end(&f); return; }
	UT_ASSERT(replace_claim(&work));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.block,
		2, 7, &f.prepared, &f.finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.old.source.m.state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_ARMED);
	clean_send(&f.old.source.m.state->holder_status_intent.slot); clean_send(&f.old.source.m.state->holder_image_intent);
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&f.last.source, &floor));
	UT_ASSERT_EQ(floor.floor, 80); UT_ASSERT_EQ(floor.bounded, 2);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 1);
	replace_end(&f);
}

UT_TEST(test_covered_replacement_waits_for_original_ack_callback)
{
	ReplaceFixture f; ClusterPcmResourceXMasterState before;
	if (!replace_start(&f, false)) { UT_ASSERT(false); return; }
	before = *f.old.source.m.state;
	UT_ASSERT_EQ(replace_prepare(&f), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
	clean_send(&f.old.source.m.state->holder_status_intent.slot);
	UT_ASSERT_EQ(replace_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	replace_end(&f);
}

UT_TEST(test_bad_full_page_or_source_never_drops_old_body)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ReplaceFixture f; ClusterPcmResourceXMasterState before; RfPageOnlinePlanV1 *plan;
		if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
		before = *f.old.source.m.state; plan = f.plan;
		switch (fault) {
		case 0: f.page.data[BLCKSZ - 1]++; break;
		case 1: f.last.source.claim.claim_sha256[0]++; break;
		case 2: f.last.version.segment_incarnation[0]++; break;
		case 3: f.last.record_crc++; break;
		case 4: f.plan = NULL; break;
		case 5: f.last.source.claim.identity.origin_owner_incarnation++; break;
		}
		UT_ASSERT(replace_prepare(&f) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
		f.plan = plan; replace_end(&f);
	}
}

UT_TEST(test_original_unproved_entry_still_waits_for_pi_capacity)
{
	ReplaceFixture f; ClusterPcmResourceXMasterState before;
	if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
	before = *f.old.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_service_prepare_exact(&f.block,
		2, 7, &f.prepared, f.page.data, &f.last, 83, 61, 87, 61), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
	replace_end(&f);
}

UT_TEST(test_late_old_request_cannot_touch_successor_descriptor_or_refs)
{
	ReplaceFixture f; ClusterPcmResourceXMasterState before;
	if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
	if (replace_prepare(&f) != RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); replace_end(&f); return; }
	before = *f.old.source.m.state;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_prepare_exact(&f.old.source.block,
		2, 7, &f.old.source.prepared, f.old.source.page.data, &f.old.last, 83, 61), RESOURCE_X_APPLY_STALE);
	UT_ASSERT(clean_receive(&f.request) != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
	replace_end(&f);
}

UT_TEST(test_pending_error_keeps_original_lms_owner)
{
	ReplaceFixture f; ResourceXSourceDrainWork work, next;
	if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
	if (replace_prepare(&f) != RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); replace_end(&f); return; }
	UT_ASSERT(replace_claim(&work));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_BAD_STATE), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.old.source.m.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT(!replace_claim(&next));
	cluster_pcm_lock_resource_x_source_drain_content_released();
	UT_ASSERT(replace_claim(&next)); UT_ASSERT(next.attempt_generation > work.attempt_generation);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 1);
	replace_end(&f);
}

UT_TEST(test_data_during_pending_keeps_native_completion_responsibility)
{
	ReplaceFixture f; ResourceXSourceDrainWork work;
	if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
	if (replace_prepare(&f) != RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); replace_end(&f); return; }
	local_pi_covered = true; UT_ASSERT(pi_retire(&f.old, false));
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 0);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.transport_refs, 1);
	UT_ASSERT_EQ(f.old.source.m.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	UT_ASSERT(replace_claim(&work));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.block,
		2, 7, &f.prepared, &f.finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 0);
	replace_end(&f);
}

UT_TEST(test_successor_settlement_then_data_retires_each_owner_once)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ResourceXDecodedFrame request;
	ClusterPcmLocalPiFloorV1 floor; int refs;
	if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
	refs = replace_references;
	if (replace_prepare(&f) != RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); replace_end(&f); return; }
	UT_ASSERT_EQ(replace_references, refs);
	UT_ASSERT(replace_claim(&work));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.block,
		2, 7, &f.prepared, &f.finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	clean_send(&f.old.source.m.state->holder_status_intent.slot); clean_send(&f.old.source.m.state->holder_image_intent);
	request = replace_receipt(&f);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 1);
	clean_send(&f.old.source.m.state->holder_status_intent.slot);
	local_pi_covered = true; UT_ASSERT(pi_retire(&f.old, false));
	UT_ASSERT_EQ(replace_references, refs - 4);
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&f.last.source, &floor));
	UT_ASSERT_EQ(floor.floor, 0); UT_ASSERT_EQ(floor.bounded, 0);
	UT_ASSERT_EQ(f.old.source.m.state->holder_status.valid, 0);
	UT_ASSERT(!pi_retire(&f.old, false));
	UT_ASSERT_EQ(replace_references, refs - 4);
	replace_end(&f);
}

UT_TEST(test_pending_duplicate_and_failed_rearm_preserve_live_refs)
{
	ReplaceFixture f; ClusterPcmResourceXPiCarrier carrier; int refs; uint64 floor;
	if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
	if (replace_prepare(&f) != RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); replace_end(&f); return; }
	refs = replace_references; carrier = f.old.source.m.state->pi_carrier;
	UT_ASSERT_EQ(replace_prepare(&f), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(replace_references, refs);
	floor = f.old.source.m.state->holder_image_intent.first_armed_us;
	f.old.source.m.state->holder_image_intent.first_armed_us = UINT64_MAX - 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.block,
		2, 7, &f.prepared, &f.finished), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT(memcmp(&carrier, &f.old.source.m.state->pi_carrier, sizeof(carrier)) == 0);
	UT_ASSERT_EQ(replace_references, refs);
	UT_ASSERT_EQ(f.old.source.m.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	f.old.source.m.state->holder_image_intent.first_armed_us = floor;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.block,
		2, 7, &f.prepared, &f.finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(replace_references, refs);
	replace_end(&f);
}

static ReplaceFixture *replace_race;
static unsigned replace_lock_visits;
static void
replace_local_progress(void)
{
	ClusterPageWalBindingV1 next;
	if (++replace_lock_visits < 3) { object_entry_lock_hook = replace_local_progress; return; }
	next = replace_race->old.last;
	next.version.mutation_token = 95; next.record_start = 240; next.record_end = 250;
	UT_ASSERT(cluster_pcm_local_pi_record_v1(replace_race->old.local.resource, &next));
}

UT_TEST(test_local_pi_progress_after_proof_refuses_exact_handoff)
{
	ReplaceFixture f; ClusterPcmResourceXMasterState before; int refs;
	if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
	before = *f.old.source.m.state; refs = replace_references;
	replace_race = &f; replace_lock_visits = 0; object_entry_lock_hook = replace_local_progress;
	UT_ASSERT_EQ(replace_prepare(&f), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(replace_lock_visits, 3);
	UT_ASSERT_EQ(replace_references, refs);
	UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
	object_entry_lock_hook = NULL; replace_race = NULL; replace_end(&f);
}

UT_TEST(test_cancel_after_new_ref_reservation_releases_only_temporary_owner)
{
	ReplaceFixture f; ClusterPcmResourceXMasterState before; int refs, allocations; volatile bool caught = false;
	if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
	before = *f.old.source.m.state; refs = replace_references; allocations = replace_allocations;
	replace_cancel_after_retain = true;
	PG_TRY(); { (void)replace_prepare(&f); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT(!replace_cancel_after_retain);
	UT_ASSERT_EQ(replace_references, refs); UT_ASSERT_EQ(replace_allocations, allocations);
	UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	replace_end(&f);
}

UT_TEST(test_unsettled_old_transport_keeps_original_capacity)
{
	ReplaceFixture f; ClusterPcmResourceXMasterState before; int refs;
	if (!replace_start_phase(&f, 0)) { UT_ASSERT(false); return; }
	before = *f.old.source.m.state; refs = replace_references;
	UT_ASSERT_EQ(replace_prepare(&f), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(replace_references, refs);
	UT_ASSERT_EQ(clean_receive(&f.request), RESOURCE_X_APPLY_APPLIED);
	clean_send(&f.old.source.m.state->holder_status_intent.slot);
	UT_ASSERT_EQ(replace_prepare(&f), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(replace_references, refs);
	replace_end(&f);
}

UT_TEST(test_domain_floor_and_native_backstep_preserve_old_owner)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ReplaceFixture f; ClusterPcmResourceXMasterState before; int refs;
		uint64 session = ut_master_session;
		if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
		switch (fault) {
		case 0: f.prepared.generation--; break;
		case 1: f.block.common.master_session_incarnation++; break;
		case 2: f.block.body.acquire_decision.resource_binding++; break;
		case 3: f.block.common.authority_generation--; f.block.common.base_authority_generation--; break;
		case 4: f.old.source.m.state->settlement_retry.retired_authority++; break;
		case 5: f.old.source.m.state->pi_carrier.pi_revision = UINT64_MAX - 1; break;
		case 6: f.block.body.acquire_decision.holders[0].boot_incarnation++; break;
		case 7: ut_master_session++; f.block.body.acquire_decision.holders[0].boot_incarnation = ut_master_session; break;
		}
		UT_ASSERT(replace_block_encode(&f));
		before = *f.old.source.m.state; refs = replace_references;
		UT_ASSERT(replace_prepare(&f) != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
		UT_ASSERT_EQ(replace_references, refs);
		ut_master_session = session;
		replace_end(&f);
	}
}

UT_TEST(test_foreign_local_wal_scope_cannot_disappear_in_replacement)
{
	ReplaceFixture f; ClusterPageWalBindingV1 foreign; ClusterPcmResourceXMasterState before; int refs;
	if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
	foreign = f.old.last; foreign.source.claim.identity.origin_node_id = 1;
	foreign.source.claim.identity.origin_thread_id = 2; foreign.source.claim.identity.origin_owner_incarnation = 71;
	foreign.record_start = 5; foreign.record_end = 9; foreign.version.mutation_token = 19;
	UT_ASSERT(cluster_pcm_local_pi_record_v1(f.old.local.resource, &foreign));
	before = *f.old.source.m.state; refs = replace_references;
	UT_ASSERT_EQ(replace_prepare(&f), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(replace_references, refs);
	replace_end(&f);
}

static bool replace_only_old_data(const ClusterPcmLocalPiSnapshotV1 *scope)
{
	return scope->last.version.mutation_token == 80;
}

UT_TEST(test_data_covering_old_local_scope_cannot_retire_new_carrier)
{
	ReplaceFixture f; ClusterPcmResourceXPiCarrier before; int refs;
	if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
	if (replace_prepare(&f) != RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); replace_end(&f); return; }
	before = f.old.source.m.state->pi_carrier; refs = replace_references;
	local_pi_covered = true; local_pi_coverage_filter = replace_only_old_data;
	UT_ASSERT(!pi_retire(&f.old, false));
	UT_ASSERT(memcmp(&before, &f.old.source.m.state->pi_carrier, sizeof(before)) == 0);
	UT_ASSERT_EQ(replace_references, refs);
	local_pi_coverage_filter = NULL;
	UT_ASSERT(pi_retire(&f.old, false));
	UT_ASSERT_EQ(replace_references, refs - 4);
	UT_ASSERT_EQ(f.old.source.m.state->holder_status.valid, RESOURCE_X_HOLDER_PAIR_PENDING);
	replace_end(&f);
}

UT_TEST(test_covered_writer_target_requires_its_own_native_quiet_receipt)
{
	ReplaceFixture f; ResourceXSourceDrainWork work; ClusterPcmOwnSnapshot quiet;
	if (!replace_start(&f, true)) { UT_ASSERT(false); return; }
	f.block.common.target_mode = PCM_STATE_X;
	f.block.body.acquire_decision.required_holders_bitmap = f.block.body.acquire_decision.holders_bitmap & ~(UINT32_C(1) << 3);
	UT_ASSERT(replace_block_encode(&f));
	f.finished = f.prepared; f.finished.generation++;
	f.finished.pcm_state = PCM_STATE_N; f.finished.buffer_type = BUF_TYPE_PI;
	if (replace_prepare(&f) != RESOURCE_X_APPLY_APPLIED) { UT_ASSERT(false); replace_end(&f); return; }
	UT_ASSERT(replace_claim(&work));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f.block,
		2, 7, &f.prepared, &f.finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work, RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	quiet = f.finished; quiet.flags = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_handoff_exact(&f.block,
		2, 7, &f.finished, &quiet), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.completed.flags, 0);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.pi_refs, 1);
	UT_ASSERT_EQ(f.old.source.m.state->pi_carrier.transport_refs, 1);
	replace_end(&f);
}

int
main(void)
{
	UT_PLAN(17);
	UT_RUN(test_covered_successor_keeps_earliest_ref_and_original_lms_work);
	UT_RUN(test_covered_pending_publication_keeps_transport_and_pi_separate);
	UT_RUN(test_covered_replacement_waits_for_original_ack_callback);
	UT_RUN(test_bad_full_page_or_source_never_drops_old_body);
	UT_RUN(test_original_unproved_entry_still_waits_for_pi_capacity);
	UT_RUN(test_late_old_request_cannot_touch_successor_descriptor_or_refs);
	UT_RUN(test_pending_error_keeps_original_lms_owner);
	UT_RUN(test_data_during_pending_keeps_native_completion_responsibility);
	UT_RUN(test_successor_settlement_then_data_retires_each_owner_once);
	UT_RUN(test_pending_duplicate_and_failed_rearm_preserve_live_refs);
	UT_RUN(test_local_pi_progress_after_proof_refuses_exact_handoff);
	UT_RUN(test_cancel_after_new_ref_reservation_releases_only_temporary_owner);
	UT_RUN(test_unsettled_old_transport_keeps_original_capacity);
	UT_RUN(test_domain_floor_and_native_backstep_preserve_old_owner);
	UT_RUN(test_foreign_local_wal_scope_cannot_disappear_in_replacement);
	UT_RUN(test_data_covering_old_local_scope_cannot_retire_new_carrier);
	UT_RUN(test_covered_writer_target_requires_its_own_native_quiet_receipt);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
