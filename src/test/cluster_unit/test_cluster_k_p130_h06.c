/* Immutable PAGE coverage and original PI replacement/ref consumers. */
#include "test_cluster_k_p130_h06_replace.inc"

#if defined(K_P130_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "H06 assertion-OFF requires an assertion-disabled configuration"
#endif

/* Controller inputs for the original immutable PAGE plan. Detached redo is
 * the named provider from the original fixture, not a coverage predicate. */
static bool
h06_plan(ReplaceFixture *f, bool first_covered, const ClusterPageWalBindingV1 *foreign,
	bool foreign_covered)
{
	const ClusterPageWalBindingV1 *bindings[4];
	ClusterWalSourceRef sources[2];
	RfContributorStreamCutV1 cuts[2] = {{0}};
	RfPageOnlinePlanRequestV1 request = {0};
	RfPageOnlineTargetViewV1 target = {0};
	unsigned count = 0, participants = foreign && foreign_covered ? 2 : 1;
	if (first_covered) bindings[count++] = &f->old.first;
	bindings[count++] = &f->old.last;
	if (participants == 2) bindings[count++] = foreign;
	bindings[count++] = &f->last;
	sources[0] = f->last.source;
	if (participants == 2) sources[1] = foreign->source;
	for (unsigned i = 0; i < participants; i++) {
		cuts[i].failed_thread = sources[i].claim.identity.origin_thread_id;
		cuts[i].timeline_id = sources[i].timeline;
		cuts[i].origin_owner_incarnation = sources[i].claim.identity.origin_owner_incarnation;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].scan_begin_inclusive = i ? 5 : 80;
		cuts[i].scan_end_exclusive = i ? 9 : 200;
	}
	request.system_identifier = f->last.identity.system_identifier;
	memcpy(request.storage_uuid, f->last.identity.storage_uuid, 16);
	request.physical_cuts = cuts; request.participant_count = participants;
	request.retention_binding_cookie = 41;
	rf_page_online_plan_destroy_v1(&f->plan);
	UT_ASSERT_EQ(rf_page_online_plan_create_v1(&request, &f->plan), RF_PAGE_PROOF_DETAIL_OK);
	if (!f->plan) return false;
	UT_ASSERT(rf_page_online_plan_bind_sources_v1(f->plan, sources, participants));
	for (unsigned n = 0; n < count; n++) {
		const ClusterPageWalBindingV1 *b = bindings[n];
		XLogReaderState reader = {0};
		union { DecodedXLogRecord record; char bytes[sizeof(DecodedXLogRecord) + sizeof(DecodedBkpBlock)]; } storage = {0};
		RfDetachedRecordPlanV1 plan = {0};
		RfDetachedComponentPlanV1 *c = &plan.components[0];
		RfPageOnlineRecordIdentityV1 identity = {0};
		reader.record = &storage.record; reader.system_identifier = b->identity.system_identifier;
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
		if (n == 0) { c->before.mutation_token = 60; c->edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE; }
		c->result = b->version;
		identity.record.system_identifier = b->identity.system_identifier;
		memcpy(identity.record.storage_uuid, b->identity.storage_uuid, 16);
		identity.record.origin_thread = b->source.claim.identity.origin_thread_id;
		identity.participant_index = participants == 2 && b == foreign ? 1 : 0;
		identity.record.timeline_id = b->source.timeline;
		identity.record.read_rec_ptr = b->record_start; identity.record.end_rec_ptr = b->record_end;
		identity.record.record_crc = b->record_crc; identity.record.rmid = b->rmid; identity.record.info = b->info;
		UT_ASSERT_EQ(rf_page_online_plan_queue_record_v1(f->plan, &plan, &identity), RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(f->plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_page_online_plan_target_v1(f->plan, 0, &target));
	if (!target.canonical_page) return false;
	memcpy(f->page.data, target.canonical_page, BLCKSZ);
	return true;
}

static void
h06_floor(ReplaceFixture *f, unsigned bounded, uint64 floor)
{
	ClusterPcmLocalPiFloorV1 actual;
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&f->last.source, &actual));
	UT_ASSERT_EQ(actual.bounded, bounded); UT_ASSERT_EQ(actual.floor, floor);
}

static bool
h06_only_old_DATA(const ClusterPcmLocalPiSnapshotV1 *scope)
{
	return scope->last.version.mutation_token == 80;
}

static void
h06_finish(ReplaceFixture *f)
{
	ResourceXSourceDrainWork work;
	ResourceXDecodedFrame request;
	ClusterPageWalBindingV1 first, last;
	ClusterPcmLocalPiSnapshotV1 local;
	ClusterPcmResourceXPiCarrier carrier;
	ClusterPcmResourceXMasterState *s = f->old.source.m.state;
	UT_ASSERT(cluster_page_wal_ref_read_v1(&s->pi_carrier.first, f->last.identity.locator,
		MAIN_FORKNUM, f->last.identity.blockno, &first));
	UT_ASSERT(cluster_page_wal_ref_read_v1(&s->pi_carrier.last, f->last.identity.locator,
		MAIN_FORKNUM, f->last.identity.blockno, &last));
	UT_ASSERT_EQ(first.version.mutation_token, 70); UT_ASSERT_EQ(first.record_start, 80);
	UT_ASSERT_EQ(last.version.mutation_token, 90); UT_ASSERT_EQ(last.record_end, 200);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f->old.local.resource, &local));
	UT_ASSERT(memcmp(&local, &f->old.local, sizeof(local)) == 0);
	h06_floor(f, 2, 80);
	UT_ASSERT(replace_claim(&work));
	UT_ASSERT_EQ(work.r4_record_generation, 87);
	UT_ASSERT_EQ(work.request.authority_generation, f->block.common.authority_generation);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_acquire_s_source_publish_exact(&f->block,
		2, 7, &f->prepared, &f->finished), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_source_drain_complete_exact(&work,
		RESOURCE_X_APPLY_APPLIED), RESOURCE_X_APPLY_APPLIED);
	clean_send(&s->holder_status_intent.slot); clean_send(&s->holder_image_intent);
	request = replace_receipt(f);
	UT_ASSERT_EQ(clean_receive(&request), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(s->pi_carrier.transport_refs, 0); UT_ASSERT_EQ(s->pi_carrier.pi_refs, 1);
	clean_send(&s->holder_status_intent.slot);
	carrier = s->pi_carrier;
	local_pi_covered = true; local_pi_coverage_filter = h06_only_old_DATA;
	UT_ASSERT(!pi_retire(&f->old, false));
	UT_ASSERT(memcmp(&carrier, &s->pi_carrier, sizeof(carrier)) == 0);
	h06_floor(f, 2, 80);
	local_pi_coverage_filter = NULL;
	UT_ASSERT(pi_retire(&f->old, false));
	UT_ASSERT_EQ(s->holder_status.valid, 0); UT_ASSERT_EQ(s->holder_image.valid, 0);
	UT_ASSERT_EQ(s->pi_carrier.pi_refs, 0); UT_ASSERT_EQ(s->pi_carrier.transport_refs, 0);
	UT_ASSERT_EQ(s->holder_status_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	UT_ASSERT_EQ(s->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	h06_floor(f, 0, 0);
	printf("# H06 done E=" UINT64_FORMAT " A=" UINT64_FORMAT " first=70/80 last=90/200 pair=0 PI=0 transport=0 lower=0 floor=0\n",
		f->block.common.assertion_sequence, f->block.common.authority_generation);
}

static void
h06_matrix(bool first_covered, int foreign_kind, bool external)
{
	ReplaceFixture f;
	ClusterPageWalBindingV1 foreign = {0};
	ClusterPcmLocalPiSnapshotV1 local, after;
	ClusterPcmResourceXMasterState before;
	ResourceXApplyResult result;
	bool coverage, eligible = first_covered && foreign_kind == 0 && !external;
	int refs, allocations;
	if (!replace_start_phase(&f, external ? 0 : 2)) { UT_ASSERT(false); replace_end(&f); return; }
	if (foreign_kind) {
		foreign = f.old.last;
		foreign.source.claim.identity.origin_node_id = 1;
		foreign.source.claim.identity.origin_thread_id = 2;
		foreign.source.claim.identity.origin_owner_incarnation = 71;
		foreign.record_start = 5; foreign.record_end = 9; foreign.version.mutation_token = 85;
		UT_ASSERT(cluster_pcm_local_pi_record_v1(f.old.local.resource, &foreign));
	}
	if (!h06_plan(&f, first_covered, foreign_kind ? &foreign : NULL, foreign_kind == 2)) {
		UT_ASSERT(false); replace_end(&f); return;
	}
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f.old.local.resource, &local));
	coverage = rf_page_online_plan_image_covers_v1(f.plan, &f.last, f.page.data, &local.first, &local.last);
	UT_ASSERT_EQ(coverage, first_covered && foreign_kind != 1);
	before = *f.old.source.m.state;
	refs = replace_references; allocations = replace_allocations;
	result = replace_prepare(&f);
	UT_ASSERT_EQ(replace_references, refs); UT_ASSERT_EQ(replace_allocations, allocations);
	UT_ASSERT_EQ(result, eligible ? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_LOCAL_WAIT);
	if (!eligible) UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(f.old.local.resource, &after));
	UT_ASSERT(memcmp(&local, &after, sizeof(local)) == 0);
	printf("# H06 first=%d foreign=%d external=%d coverage=%d result=%d oldA=3 newA=" UINT64_FORMAT
		" pair=%u PI=%u transport=%u crc=%u\n", first_covered, foreign_kind, external, coverage, result,
		f.block.common.authority_generation, f.old.source.m.state->holder_image.valid,
		f.old.source.m.state->pi_carrier.pi_refs, f.old.source.m.state->pi_carrier.transport_refs,
		cluster_gcs_block_compute_checksum(f.page.data));
	if (external && first_covered && foreign_kind == 0) {
		UT_ASSERT_EQ(clean_receive(&f.request), RESOURCE_X_APPLY_APPLIED);
		clean_send(&f.old.source.m.state->holder_status_intent.slot);
		result = replace_prepare(&f); UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(replace_references, refs); UT_ASSERT_EQ(replace_allocations, allocations);
	}
	if (result == RESOURCE_X_APPLY_APPLIED) { h06_finish(&f); UT_ASSERT_EQ(replace_references, refs - 4); }
	replace_end(&f);
}

UT_TEST(test_k_h06_first_covered_no_external) { h06_matrix(true, 0, false); }
UT_TEST(test_k_h06_first_covered_transport_then_release) { h06_matrix(true, 0, true); }
UT_TEST(test_k_h06_first_uncovered_no_external) { h06_matrix(false, 0, false); }
UT_TEST(test_k_h06_first_uncovered_with_transport) { h06_matrix(false, 0, true); }
UT_TEST(test_k_h06_foreign_uncovered_no_external) { h06_matrix(true, 1, false); }
UT_TEST(test_k_h06_foreign_uncovered_with_transport) { h06_matrix(true, 1, true); }
UT_TEST(test_k_h06_foreign_plan_keeps_unrepresentable_scope) { h06_matrix(true, 2, false); }
UT_TEST(test_k_h06_foreign_plan_with_transport_keeps_old_body) { h06_matrix(true, 2, true); }
UT_TEST(test_k_h06_staged_ACK_must_complete_before_replacement)
{
	ReplaceFixture f; ResourceXIntentSlot ack; ClusterPcmResourceXMasterState before;
	ResourceXApplyResult result;
	int refs;
	if (!replace_start(&f, false)) { UT_ASSERT(false); replace_end(&f); return; }
	ack = clean_stage(&f.old.source.m.state->holder_status_intent.slot);
	before = *f.old.source.m.state;
	refs = replace_references;
	UT_ASSERT_EQ(replace_prepare(&f), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT(memcmp(&before, f.old.source.m.state, sizeof(before)) == 0);
	UT_ASSERT_EQ(replace_references, refs);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&ack, true, ++fake_pcm_clock_us));
	result = replace_prepare(&f);
	UT_ASSERT_EQ(result, RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(replace_references, refs);
	if (result == RESOURCE_X_APPLY_APPLIED) { h06_finish(&f); UT_ASSERT_EQ(replace_references, refs - 4); }
	replace_end(&f);
}
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_ASSERT_CHECKING
	printf("# H06 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H06 USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(9);
	UT_RUN(test_k_h06_first_covered_no_external);
	UT_RUN(test_k_h06_first_covered_transport_then_release);
	UT_RUN(test_k_h06_first_uncovered_no_external);
	UT_RUN(test_k_h06_first_uncovered_with_transport);
	UT_RUN(test_k_h06_foreign_uncovered_no_external);
	UT_RUN(test_k_h06_foreign_uncovered_with_transport);
	UT_RUN(test_k_h06_foreign_plan_keeps_unrepresentable_scope);
	UT_RUN(test_k_h06_foreign_plan_with_transport_keeps_old_body);
	UT_RUN(test_k_h06_staged_ACK_must_complete_before_replacement);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
