/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_ctrc_page_prefix.inc
 *     Original Startup ownership of the first pending graph PAGE operation.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_ctrc_page_prefix.inc
 *
 * NOTES
 *     Actual Start and Startup caller execute. Graph, stable proof, native
 *     PAGE and single-page storage/WAL leaves are controlled independently.
 *-------------------------------------------------------------------------
 */
static uint32 prefix_count;
static void pending_storage_fixture_reset(void);
static RfPageOnlineTargetViewV1 prefix_targets[2];
static RfPageStableGraphRequestV1 prefix_graph[2];
static RfContributorVectorV1 prefix_contributors[2];
static RfPageStableEdgeInputV1 prefix_edges[2];
static PGAlignedBlock prefix_images[2];
static unsigned prefix_steps, prefix_proofs, prefix_pages, prefix_destroyed;
static unsigned pending_readback_calls;
static unsigned prefix_proof_token, prefix_page_token;
static uint32 prefix_source_ordinal;
static bool prefix_error, prefix_wait, prefix_unchanged, prefix_bad_physical, prefix_insert_refuse;
static bool prefix_legacy_synced;
static unsigned prefix_durable_steps;
static void (*prefix_callback)(void);
static void (*prefix_destroy_callback)(void);
static ClusterRecoveryStampHandleV1 prefix_window;

static void
page_prefix_fixture_reset(void)
{
 pending_storage_fixture_reset();
 prefix_count = prefix_steps = prefix_proofs = prefix_pages = prefix_destroyed = 0;
 prefix_source_ordinal = 0;
 prefix_error = prefix_wait = prefix_unchanged = prefix_bad_physical = prefix_insert_refuse = false;
 prefix_legacy_synced = false; prefix_durable_steps = 0;
 prefix_callback = NULL;
 prefix_destroy_callback = NULL;
 memset(&prefix_window, 0, sizeof(prefix_window));
 memset(prefix_targets, 0, sizeof(prefix_targets));
 memset(prefix_graph, 0, sizeof(prefix_graph));
 memset(prefix_contributors, 0, sizeof(prefix_contributors));
 memset(prefix_edges, 0, sizeof(prefix_edges));
 memset(prefix_images, 0, sizeof(prefix_images));
}

uint32
rf_page_online_plan_target_count_v1(const RfPageOnlinePlanV1 *plan)
{
 UT_ASSERT(plan == (const RfPageOnlinePlanV1 *)&page_graph_token && page_graph_live);
 return prefix_count;
}

bool
rf_page_online_plan_target_v1(const RfPageOnlinePlanV1 *plan, uint32 index,
 RfPageOnlineTargetViewV1 *out)
{
 UT_ASSERT(plan == (const RfPageOnlinePlanV1 *)&page_graph_token && page_graph_live);
 if (index >= prefix_count || index >= lengthof(prefix_targets)) return false;
 *out = prefix_targets[index];
 return true;
}

RfPageStableCtrcBuildV1
rf_page_stable_base_proof_build_ctrc_replays_v1(const RfPageStableGraphRequestV1 *graph,
 const ClusterRecoveryCtrcReplaySetSourceV1 *sources, uint32 count, Size budget)
{
 UT_ASSERT(observation_scope_live && observation_cursor_live && page_graph_live);
 UT_ASSERT_EQ(ctrc_driver_lwlocks, 0); UT_ASSERT_EQ(count, page_source_input_count); UT_ASSERT(budget > 0);
 UT_ASSERT(graph == &prefix_graph[0] || graph == &prefix_graph[1]);
 UT_ASSERT(sources == cluster_recovery_cold_start->ctrc_page_sources);
 prefix_proofs++;
 if (prefix_error) pg_re_throw();
 if (prefix_callback) prefix_callback();
 return (RfPageStableCtrcBuildV1){(RfPageStableBaseProofV1 *)&prefix_proof_token, RF_PAGE_PROOF_DETAIL_OK};
}

void
rf_page_stable_base_proof_destroy_v1(RfPageStableBaseProofV1 **proof)
{
 if (proof && *proof) { UT_ASSERT(*proof == (RfPageStableBaseProofV1 *)&prefix_proof_token); *proof = NULL; prefix_destroyed++; }
}

RfPageCtrcAuthorityBuildV1
rf_page_authority_ctrc_replays_preflight_v1(const RfPageAuthorityTargetV1 *targets,
 uint32 count, const ClusterRecoveryCtrcReplaySetSourceV1 *sources,
 uint32 source_count, Size budget)
{
 UT_ASSERT_EQ(count, 1); UT_ASSERT_EQ(source_count, page_source_input_count); UT_ASSERT(budget > 0);
 UT_ASSERT(targets->stable_base == (RfPageStableBaseProofV1 *)&prefix_proof_token);
 UT_ASSERT(sources == cluster_recovery_cold_start->ctrc_page_sources);
 UT_ASSERT_EQ(ctrc_driver_lwlocks, 0); prefix_pages++;
 return (RfPageCtrcAuthorityBuildV1){(RfPageCtrcAuthorityV1 *)&prefix_page_token, RF_PAGE_AUTHORITY_OK};
}

void
rf_page_authority_ctrc_replays_destroy_v1(RfPageCtrcAuthorityV1 **authority)
{
 if (authority && *authority) {
  UT_ASSERT(*authority == (RfPageCtrcAuthorityV1 *)&prefix_page_token);
  *authority = NULL; prefix_destroyed++;
  if (prefix_destroy_callback) prefix_destroy_callback();
 }
}

/* The native PAGE/WAL leaf is controlled here. Publish its descriptor through
 * the original F4 codec/state machine so Start-current and handoff exercise
 * the real shared window, not a replacement currentness predicate. This
 * private publication does not exercise the native PAGE admission factory. */
static void
prefix_publish_window(const struct ClusterRecoveryStartV1 *start,
 const ClusterRecoveryCtrcReplaySetSourceV1 *source,
 const ClusterSpaceIdentity *space, BlockNumber block,
 const RfPageVersionV1 *before, RfPageCtrcFpiInsertV1 *out)
{
 ClusterRecoveryStampOwnerV1 owner = {0};
 ClusterRecoveryStampFpiV1 fpi = {0};
 bool inserted;

 owner.writer = start->writer; owner.target = source->generation.source.claim;
 owner.executor_boot = start->owner_boot; owner.executor_pid = start->pid;
 owner.recovery_serial = start->serial;
 fpi.locator = space->key.locator;
 fpi.image.owner = GENERIC_XLOG_CTRC_ITL;
 fpi.image.fork_number = MAIN_FORKNUM; fpi.image.block_number = block;
 fpi.image.result_token = before->mutation_token + 1;
 fpi.image.version.page_class = RF_PAGE_CLASS_ORDINARY;
 fpi.image.version.before_kind = fpi.image.version.result_kind = RF_PAGE_STATE_PRESENT;
 fpi.image.version.before = *before;
 memcpy(fpi.image.version.result_incarnation, before->segment_incarnation, 16);
 memset(fpi.image.after_sha256, 9, sizeof(fpi.image.after_sha256));
 UT_ASSERT(cluster_space_identity_encode(space, fpi.image.space_identity,
  sizeof(fpi.image.space_identity)));
 fpi.protection.source_binding_cookie = 1; fpi.protection.protection_fingerprint = 1;
 UT_ASSERT(stamp_owner_valid(&owner));
 UT_ASSERT(stamp_fpi_image_matches(&owner, &fpi));
 UT_ASSERT(stamp_prepare(&owner, CLUSTER_RECOVERY_STAMP_FPI,
  (const uint8 *)&fpi, sizeof(fpi), &out->window));
 out->record_start = owner.writer.startup_first_lsn + 40000;
 out->record_end = out->record_start + 128;
 CritSectionCount++;
 inserted = cluster_recovery_stamp_inserting_v1(&out->window)
  && cluster_recovery_stamp_inserted_v1(&out->window, out->record_start, out->record_end);
 CritSectionCount--;
 UT_ASSERT(inserted);
 out->wal_flushed = true;
 prefix_window = out->window;
}

/* The actual native graph step has separate protected file/Generic/F4 tests. */
RfPageCtrcStepResultV1
rf_page_recovery_ctrc_replays_itl_graph_step_smgr_v1(const struct ClusterRecoveryStartV1 *start,
 const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
 RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space, BlockNumber block,
 const RfPageVersionV1 *before, const char canonical[BLCKSZ],
 const ClusterRecoveryUndoEntry *physical, Size count, Size budget)
{
 RfPageCtrcStepResultV1 result = {0};
 UT_ASSERT(start == info.cluster_tx_start && work == &native_ctrc_work);
 UT_ASSERT_EQ(ctrc_driver_lwlocks, 0); UT_ASSERT(authority == (RfPageCtrcAuthorityV1 *)&prefix_page_token);
 UT_ASSERT(source == &cluster_recovery_cold_start->ctrc_page_sources[prefix_source_ordinal]);
 UT_ASSERT_EQ(space->key.locator.relNumber, 900); UT_ASSERT(block == 23 || block == 24);
 UT_ASSERT_EQ(before->mutation_token, 600 + block); UT_ASSERT(canonical == prefix_images[block - 23].data);
 UT_ASSERT(physical == cluster_recovery_cold_start->ctrc_physical->entries);
 UT_ASSERT_EQ(count, native_ctrc_work.counts.subject_count); UT_ASSERT(budget > 0);
 prefix_steps++;
 if (prefix_wait && prefix_steps == 1) { ctrc_driver_waiting = true; result.phase = RF_PAGE_CTRC_STEP_WAIT; return result; }
 if (prefix_unchanged) { result.phase = RF_PAGE_CTRC_STEP_UNCHANGED; return result; }
 result.phase = prefix_insert_refuse ? RF_PAGE_CTRC_STEP_REFUSED : RF_PAGE_CTRC_STEP_TARGET_SYNCED;
 prefix_publish_window(start, source, space, block, before, &result.inserted);
 if (prefix_insert_refuse) UT_ASSERT(cluster_recovery_stamp_fail_v1(&result.inserted.window));
 return result;
}

/* The production caller opts into same-executor physical completion. The
 * old step leaf remains intact for its independent TARGET_SYNCED contract. */
RfPageCtrcStepResultV1
rf_page_recovery_ctrc_replays_itl_graph_durable_smgr_v1(const struct ClusterRecoveryStartV1 *start,
 const ClusterColdCtrcWorkV1 *work, const ClusterRecoveryCtrcReplaySetSourceV1 *source,
 RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space, BlockNumber block,
 const RfPageVersionV1 *before, const char canonical[BLCKSZ],
 const ClusterRecoveryUndoEntry *physical, Size count, Size budget)
{
 RfPageCtrcStepResultV1 result = rf_page_recovery_ctrc_replays_itl_graph_step_smgr_v1(
  start, work, source, authority, space, block, before, canonical, physical, count, budget);
 prefix_durable_steps++;
 if (result.phase == RF_PAGE_CTRC_STEP_TARGET_SYNCED && !prefix_legacy_synced) {
  UT_ASSERT(cluster_recovery_stamp_durable_v1(&result.inserted.window));
  result.phase = RF_PAGE_CTRC_STEP_FPI_DURABLE;
 }
 return result;
}

static void
prefix_setup_targets(void)
{
 prefix_count = 2;
 observation_pages.count = 2;
 for (unsigned i = 0; i < 2; i++) {
  ClusterRecoveryCensusPageV1 *page = &observation_pages.entries[i];
  RfPageOnlineTargetViewV1 *target = &prefix_targets[i];
  ClusterSpaceIdentity *space = &page->reservation.identity;
  memset(page, 0, sizeof(*page));
  page->heap = true; page->forknum = MAIN_FORKNUM; page->blockno = 23 + i;
  page->reservation.next_block = 25;
  space->key.system_identifier = 91; memcpy(space->key.storage_uuid, observed_writer.ref.claim.identity.storage_uuid, 16);
  space->key.database_incarnation = 2;
  space->key.locator = (RelFileLocator){1663, 5, 900};
  memset(space->incarnation, 7, 16); space->state = CLUSTER_SPACE_IDENTITY_LIVE;
  space->sequence = 1; space->operation = 1;
  target->page_identity.system_identifier = 91; memcpy(target->page_identity.storage_uuid, space->key.storage_uuid, 16);
  target->page_identity.locator = space->key.locator; target->page_identity.forknum = MAIN_FORKNUM;
  target->page_identity.blockno = page->blockno; target->expected_result.mutation_token = 623 + i;
  memcpy(target->expected_result.segment_incarnation, space->incarnation, 16);
  target->graph = &prefix_graph[i]; target->contributors = &prefix_contributors[i];
  target->canonical_page = prefix_images[i].data;
  prefix_contributors[i].edge_count = 1; prefix_contributors[i].edges = &prefix_edges[i];
  prefix_edges[i].page_identity = target->page_identity;
  prefix_edges[i].result_token = target->expected_result.mutation_token;
  memcpy(prefix_edges[i].edge.result_incarnation, space->incarnation, 16);
  prefix_edges[i].edge.result_kind = RF_PAGE_STATE_PRESENT;
  prefix_edges[i].record_identity.origin_thread = 2;
  prefix_edges[i].record_identity.timeline_id = 1;
  prefix_edges[i].record_identity.read_rec_ptr = 16256;
  prefix_edges[i].record_identity.end_rec_ptr = 16384;
 }
 if (prefix_bad_physical) observation_pages.entries[0].reservation.identity.incarnation[15]++;
}

static void
prefix_service_wait(void)
{
 UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(prefix_proofs, 2); UT_ASSERT_EQ(prefix_pages, 2);
 UT_ASSERT_EQ(prefix_destroyed, 4); /* Original execution and protected readback. */
 UT_ASSERT(observation_scope_live && observation_cursor_live);
 /* Leave the real CTRC independent result pending, then cancel this fixture. */
 pg_re_throw();
}

UT_TEST(test_actual_startup_retains_first_inserted_page_before_service)
{
 volatile bool caught = false;
 ctrc_handoff_fixture(false, false); prefix_setup_targets(); ctrc_wait_step = prefix_service_wait;
 PG_TRY(); { actual_startup_finish_tx(&info); } PG_CATCH(); { caught = true; } PG_END_TRY();
 UT_ASSERT(caught); UT_ASSERT_EQ(prefix_steps, 1);
 UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
 UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
 UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs != NULL);
}

UT_TEST(test_actual_startup_refuses_changed_page_target_before_wal)
{
 prefix_bad_physical = false;
 ctrc_handoff_fixture(false, false); prefix_bad_physical = true; prefix_setup_targets();
 fatal_armed = true;
 if (setjmp(fatal_jump) == 0) { actual_startup_finish_tx(&info); UT_ASSERT(false); }
 fatal_armed = false;
 UT_ASSERT_EQ(prefix_steps, 0); UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
 UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
}

static void
prefix_ready(void)
{
 ctrc_replay_request_fixture(); prefix_setup_targets();
 UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
 UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
}

UT_TEST(test_startup_page_prefix_repeat_retains_exact_pending_result)
{
 RfPageCtrcStepResultV1 saved;
 prefix_ready();
 UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 UT_ASSERT(cluster_recovery_cold_start->ctrc_page_progress->pending);
 saved = cluster_recovery_cold_start->ctrc_page_progress->result;
 UT_ASSERT_EQ(saved.inserted.window.key.window_seq, prefix_window.key.window_seq);
 UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
 UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(prefix_destroyed, 6);
 UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_progress->next_target, 0);
 UT_ASSERT(memcmp(&saved, &cluster_recovery_cold_start->ctrc_page_progress->result, sizeof(saved)) == 0);
 UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
 UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
}

UT_TEST(test_startup_page_prefix_durable_retains_window_and_service_debt)
{
 RfPageCtrcStepResultV1 saved;
 prefix_ready();
 UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 saved = cluster_recovery_cold_start->ctrc_page_progress->result;
 UT_ASSERT_EQ(saved.phase, RF_PAGE_CTRC_STEP_FPI_DURABLE); UT_ASSERT_EQ(prefix_durable_steps, 1);
 UT_ASSERT(cluster_recovery_cold_start->ctrc_page_progress->pending);
 UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_progress->next_target, 0);
 UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
 UT_ASSERT_EQ(prefix_durable_steps, 1); UT_ASSERT_EQ(prefix_steps, 1);
 UT_ASSERT(memcmp(&saved, &cluster_recovery_cold_start->ctrc_page_progress->result, sizeof(saved)) == 0);
 UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
 UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
}

UT_TEST(test_startup_page_prefix_legacy_synced_cannot_supply_durable)
{
 prefix_ready(); prefix_legacy_synced = true;
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 UT_ASSERT_EQ(prefix_durable_steps, 1); UT_ASSERT_EQ(prefix_steps, 1);
 UT_ASSERT(cluster_recovery_cold_start->ctrc_page_progress->pending);
 UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_progress->result.phase, RF_PAGE_CTRC_STEP_TARGET_SYNCED);
 UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
 UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
 UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
}

UT_TEST(test_startup_page_prefix_unchanged_is_not_completion)
{
 prefix_ready(); prefix_unchanged = true;
 UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(prefix_destroyed, 4);
 UT_ASSERT(!cluster_recovery_cold_start->ctrc_page_progress->pending);
 UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_progress->next_target, 2);
 UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
 UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
}

UT_TEST(test_startup_page_prefix_wait_retains_original_target_and_owners)
{
 prefix_ready(); prefix_wait = true;
 UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 UT_ASSERT_EQ(prefix_steps, 2); UT_ASSERT_EQ(ctrc_driver_waits, 1);
 UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_progress->next_target, 0);
 UT_ASSERT_EQ(page_graph_builds, 1); UT_ASSERT_EQ(observation_scope_begins, 1);
 UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
}

UT_TEST(test_startup_page_prefix_refuses_ambiguous_or_unbound_terminal_edge)
{
 for (unsigned fault = 0; fault < 6; fault++) {
  prefix_ready();
  if (fault == 0) prefix_edges[0].participant_index = 1;
  if (fault == 1) prefix_edges[0].record_identity.origin_thread++;
  if (fault == 2) prefix_edges[0].record_identity.timeline_id++;
  if (fault == 3) prefix_edges[0].record_identity.end_rec_ptr++;
  if (fault == 4) prefix_edges[0].record_identity.read_rec_ptr = prefix_edges[0].record_identity.end_rec_ptr;
  if (fault == 5) prefix_edges[0].result_token++;
  UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
  UT_ASSERT_EQ(prefix_steps, 0); UT_ASSERT_EQ(prefix_pages + prefix_proofs, 0);
  UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
  UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
 }
}

UT_TEST(test_startup_page_prefix_error_or_retirement_cleans_only_private_proof)
{
 for (unsigned fault = 0; fault < 3; fault++) {
  volatile bool caught = false;
  prefix_ready();
  if (fault == 0) prefix_error = true;
  if (fault == 1) prefix_callback = page_graph_change_owner;
  if (fault == 2) prefix_callback = page_graph_close_observation;
  PG_TRY(); { UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024)); }
  PG_CATCH(); { caught = true; } PG_END_TRY();
  UT_ASSERT_EQ(caught, fault == 0); UT_ASSERT_EQ(prefix_steps, 0);
  UT_ASSERT_EQ(prefix_destroyed, fault != 0);
  UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
  if (fault < 2) UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
 }
}

UT_TEST(test_startup_page_prefix_inserted_refusal_retains_original_debt)
{
 prefix_ready(); prefix_insert_refuse = true;
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(prefix_destroyed, 2);
 UT_ASSERT(cluster_recovery_cold_start->ctrc_page_progress->pending);
 UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_progress->result.inserted.window.key.window_seq,
  prefix_window.key.window_seq);
 UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 0));
 UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
 UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
}

UT_TEST(test_startup_page_prefix_uses_graph_ordinal_not_input_or_replay_index)
{
 page_sources_three_ready(); prefix_setup_targets();
 prefix_source_ordinal = 2; prefix_edges[0].participant_index = 2;
 UT_ASSERT(ClusterColdRecoveryCtrcPagePlanPrepare(info.cluster_tx_start, 1024 * 1024));
 UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_sources[2].generation_index, 1);
 UT_ASSERT_EQ(cluster_recovery_cold_start->ctrc_page_sources[2].source_index, 0);
 UT_ASSERT(ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(page_graph_builds, 1);
 UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
}

UT_TEST(test_startup_page_prefix_refuses_duplicate_physical_and_terminal_targets)
{
 for (unsigned fault = 0; fault < 2; fault++) {
  prefix_ready();
  if (fault == 0) observation_pages.entries[1] = observation_pages.entries[0];
  else { prefix_edges[1] = prefix_edges[0]; prefix_contributors[0].edge_count = 2; }
  UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
  UT_ASSERT_EQ(prefix_steps + prefix_proofs + prefix_pages, 0);
  UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
 }
}

UT_TEST(test_startup_page_prefix_wait_rechecks_owner_before_second_attempt)
{
 prefix_ready(); prefix_wait = true; ctrc_driver_wait_step = page_graph_change_owner;
 UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start, 1024 * 1024));
 UT_ASSERT_EQ(prefix_steps, 1); UT_ASSERT_EQ(ctrc_driver_waits, 1);
 UT_ASSERT_EQ(prefix_destroyed, 2);
 UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
 UT_ASSERT(!ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
}

UT_TEST(test_startup_page_prefix_capacity_and_caller_locks_precede_execution)
{
 for (unsigned fault = 0; fault < 4; fault++) {
  unsigned snapshots;
  prefix_ready(); snapshots = observation_manifests;
  if (fault == 0) ctrc_driver_lwlocks = 200;
  if (fault == 1) native_recovery = true;
  if (fault == 2) observation_fail_allocation = allocations + 1;
  UT_ASSERT(!ClusterColdRecoveryCtrcPageAdvance(info.cluster_tx_start,
   fault == 3 ? sizeof(ClusterColdCtrcPageProgressV1) : 1024 * 1024));
  UT_ASSERT_EQ(prefix_steps + prefix_proofs + prefix_pages, 0);
  if (fault < 2) UT_ASSERT_EQ(observation_manifests, snapshots);
  UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
  ctrc_driver_lwlocks = 0;
 }
}
