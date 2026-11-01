/* Original ROOT/anchor/claim/WAL readers and original replay holder set.
 * Startup declaration and GES remain controlled leaves in this fixture.
 * No native CLOG lifetime or COMPLETE qualification is claimed. */
static bool relmap_declared;
static unsigned relmap_failed;
static ClusterRecoverySerialGuard *relmap_sibling;
static ClusterRecoverySerialAcquireResult
relmap_native_acquire(const ClusterRecoverySerialRequest *request, ClusterRecoverySerialGuard *guard)
{
	ClusterRecoverySerialAcquireResult result = ctrc_set_native_acquire(request, guard);
	if (request->duty.origin_thread_id == 128 && result == CLUSTER_RECOVERY_SERIAL_GRANTED)
		relmap_sibling = guard;
	return result;
}

bool
ClusterColdRecoveryRelmapCurrent(const struct ClusterRecoveryStartV1 *start)
{
	return relmap_declared && ClusterColdRecoveryTxCurrent(start);
}

void
ClusterColdRecoveryRelmapFail(const struct ClusterRecoveryStartV1 *start)
{
	if (start == tx_inputs_start && relmap_declared) {
		relmap_failed++;
		tx_inputs_owner_current = false;
	}
}

static void
relmap_participant_fixture(ClusterWalInputsV1 **inputs, bool bad_end)
{
	uint8 bytes[66048]; ClusterRecoveryAnchorV2 anchors[2]; ControlRootImage root;
	ClusterWalInputsV1 *original = NULL; char path[MAXPGPATH];
	ClusterRelmapCheckpointWindow window = {0};
	inputs_closed_wal_fixture(bytes, anchors);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	window.system_identifier = TEST_SYSID;
	window.database_incarnation = anchors[0].database_incarnation;
	window.epoch = 17;
	window.admitted_incarnation = anchors[0].identity.origin_owner_incarnation;
	window.boot_incarnation = 31;
	window.native_floor = 3; window.last_issued = 90; window.allocation_next = 100;
	UT_ASSERT(cluster_relmap_checkpoint_window_encode_v2(&window, &anchors[0].checkpoint_copy,
		anchors[0].checkpoint, root.records[0].validated_tail_lsn_exclusive + (bad_end ? 8 : 0),
		95, anchors[0].native_window));
	v2_anchor_object(bytes, &anchors[0], &anchors[0].identity, path);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	tx_ir_complete(&root.records[0]); tx_ir_complete(&root.records[127]);
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0); v2_write_roots(root.bytes);
	UT_ASSERT_EQ(cluster_wal_inputs_begin_v1(v2_storage, TEST_SYSID, &original), 0);
	tx_inputs_capture(original, NULL); cluster_wal_inputs_release_v1(&original);
	tx_inputs_activate(); ctrc_inputs_activate_work();
	ctrc_inputs_work.counts.subject_count = ctrc_inputs_work.counts.unresolved_count = 0;
	UT_ASSERT_EQ(ctrc_inputs_begin(inputs), 0);
	ctrc_ir_original_scope = *inputs; ctrc_ir_start_current = true;
	ctrc_set_held = ctrc_set_calls = ctrc_set_release_calls = 0;
	ctrc_set_block_thread = ctrc_set_fail_release = 0; ctrc_set_after_grant = NULL;
	ctrc_set_preflight_retire = 0;
	ctrc_replay_current_override = ctrc_set_original_current;
	test_ctrc_set_acquire_hook = relmap_native_acquire;
	relmap_sibling = NULL;
	test_ctrc_set_release_hook = ctrc_set_native_release;
	relmap_declared = false; relmap_failed = 0;
}

UT_TEST(test_relmap_inputs_require_declaration_and_all_original_holders)
{
	ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapSourceV1 source;
	relmap_participant_fixture(&inputs, false);
	relmap_declared = true; ctrc_set_block_thread = 128; ctrc_set_block_result = CLUSTER_RECOVERY_SERIAL_BUSY;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), CLUSTER_CONTROL_ROOT_RECONFIG_WAIT);
	UT_ASSERT_EQ(ctrc_set_held, 1);
	UT_ASSERT_EQ(cluster_wal_inputs_relmap_count_v1(inputs), 0);
	source = cluster_wal_inputs_relmap_source_v1(inputs, 0); UT_ASSERT(v2_zero(&source, sizeof(source)));
	ctrc_set_block_thread = 0;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	relmap_declared = false;
	UT_ASSERT_EQ(cluster_wal_inputs_relmap_count_v1(inputs), 0);
	source = cluster_wal_inputs_relmap_source_v1(inputs, 0); UT_ASSERT(v2_zero(&source, sizeof(source)));
	relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_relmap_count_v1(inputs), 2);
	for (uint32 i = 0; i < 2; i++) {
		source = cluster_wal_inputs_relmap_source_v1(inputs, i);
		UT_ASSERT(cluster_wal_inputs_relmap_source_current_v1(&source));
		UT_ASSERT_EQ(source.input_index, i); UT_ASSERT_EQ(source.input_count, 2);
		UT_ASSERT_EQ(source.replay_count, 2);
		UT_ASSERT(memcmp(&source.selected, cluster_wal_inputs_at_v1(inputs, i), sizeof(source.selected)) == 0);
	}
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT_EQ(relmap_failed, 0);
	ctrc_set_finish(&inputs); relmap_declared = false;
}

UT_TEST(test_relmap_native_input_reads_original_checkpoint_end_and_window)
{
	ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapNativeInputV1 native;
	unsigned calls;
	relmap_participant_fixture(&inputs, false); relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0); calls = ctrc_set_calls;
	native = cluster_wal_inputs_relmap_native_v1(inputs, 0);
	UT_ASSERT(cluster_wal_inputs_relmap_source_current_v1(&native.source));
	UT_ASSERT_EQ(native.checkpoint_end, native.tail.complete_end);
	UT_ASSERT_EQ(native.tail.records, 1); UT_ASSERT_EQ(native.window.native_floor, 3);
	UT_ASSERT_EQ(native.window.last_issued, 90); UT_ASSERT_EQ(native.post_flush_issued, 95);
	UT_ASSERT_EQ(native.window.allocation_next, 100);
	UT_ASSERT_EQ(native.window.epoch, 17); UT_ASSERT_EQ(native.window.boot_incarnation, 31);
	UT_ASSERT_EQ(ctrc_set_calls, calls); UT_ASSERT_EQ(ctrc_set_held, 2);
	UT_ASSERT_EQ(relmap_failed, 0); UT_ASSERT(inputs_recovery_held);
	ctrc_set_finish(&inputs); relmap_declared = false;
}

UT_TEST(test_relmap_source_rejects_changed_values_without_revoking_original)
{
	ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapSourceV1 source, changed;
	relmap_participant_fixture(&inputs, false); relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	source = cluster_wal_inputs_relmap_source_v1(inputs, 0);
	UT_ASSERT(cluster_wal_inputs_relmap_source_current_v1(&source));
	for (size_t i = 0; i < sizeof(source); i++) {
		changed = source; ((uint8 *)&changed)[i] ^= 1;
		UT_ASSERT(!cluster_wal_inputs_relmap_source_current_v1(&changed));
	}
	UT_ASSERT(cluster_wal_inputs_relmap_source_current_v1(&source));
	UT_ASSERT_EQ(relmap_failed, 0); ctrc_set_finish(&inputs); relmap_declared = false;
}

UT_TEST(test_relmap_source_loss_of_sibling_holder_never_renews_old_input)
{
	ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapSourceV1 source;
	relmap_participant_fixture(&inputs, false); relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	source = cluster_wal_inputs_relmap_source_v1(inputs, 0);
	UT_ASSERT(cluster_wal_inputs_relmap_source_current_v1(&source));
	UT_ASSERT_NOT_NULL(relmap_sibling);
	relmap_sibling->release_uncertain = true;
	UT_ASSERT(!cluster_wal_inputs_relmap_source_current_v1(&source));
	relmap_sibling->release_uncertain = false;
	UT_ASSERT(!cluster_wal_inputs_relmap_source_current_v1(&source));
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(inputs_recovery_held);
	ctrc_set_finish(&inputs); relmap_declared = false;
}

UT_TEST(test_relmap_native_input_refuses_self_consistent_wrong_checkpoint_end)
{
	ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapNativeInputV1 native;
	relmap_participant_fixture(&inputs, true); relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	native = cluster_wal_inputs_relmap_native_v1(inputs, 0);
	UT_ASSERT(v2_zero(&native, sizeof(native))); UT_ASSERT_EQ(relmap_failed, 1);
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(inputs_recovery_held);
	ctrc_set_finish(&inputs); relmap_declared = false;
}

UT_TEST(test_relmap_legacy_native_input_revokes_whole_declared_episode)
{
	ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapNativeInputV1 native;
	ClusterRecoveryRelmapSourceV1 first;
	relmap_participant_fixture(&inputs, false); relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	first = cluster_wal_inputs_relmap_source_v1(inputs, 0);
	native = cluster_wal_inputs_relmap_native_v1(inputs, 1);
	UT_ASSERT(v2_zero(&native, sizeof(native))); UT_ASSERT_EQ(relmap_failed, 1);
	UT_ASSERT(!cluster_wal_inputs_relmap_source_current_v1(&first));
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(inputs_recovery_held);
	ctrc_set_finish(&inputs); relmap_declared = false;
}

UT_TEST(test_relmap_native_physical_refusal_retains_all_original_holders)
{
	ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapNativeInputV1 native;
	ClusterRecoveryRelmapSourceV1 source; char path[MAXPGPATH]; uint8 damage = 0xff; int fd;
	relmap_participant_fixture(&inputs, false); relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	source = cluster_wal_inputs_relmap_source_v1(inputs, 0);
	UT_ASSERT(cluster_wal_inputs_relmap_source_current_v1(&source));
	if (source.scope == NULL) { ctrc_set_finish(&inputs); relmap_declared = false; return; }
	v2_checkpoint_wal_path(&source.selected.source.claim.identity,
		source.selected.checkpoint_start, source.selected.source.timeline, path);
	fd = open(path, O_WRONLY); UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, &damage, 1, source.selected.checkpoint_start % wal_segment_size + SizeOfXLogRecord + 2), 1);
	UT_ASSERT_EQ(close(fd), 0);
	native = cluster_wal_inputs_relmap_native_v1(inputs, 0);
	UT_ASSERT(v2_zero(&native, sizeof(native))); UT_ASSERT_EQ(relmap_failed, 1);
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(inputs_recovery_held);
	ctrc_set_finish(&inputs); relmap_declared = false;
}

UT_TEST(test_relmap_native_error_revokes_without_releasing_original_owners)
{
	ClusterWalInputsV1 *inputs = NULL; volatile bool caught = false;
	relmap_participant_fixture(&inputs, false); relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	test_throw_root_read = true;
	PG_TRY(); { (void)cluster_wal_inputs_relmap_native_v1(inputs, 0); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	test_throw_root_read = false;
	UT_ASSERT(caught); UT_ASSERT_EQ(relmap_failed, 1);
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(inputs_recovery_held);
	ctrc_set_finish(&inputs); relmap_declared = false;
}

UT_TEST(test_relmap_input_vector_keeps_every_original_history_generation)
{
	uint8 bytes[66048], uuid[16] = {0x87};
	ClusterPhase1FullStopPlan stop; ClusterWalHistoryImage history;
	ClusterWalHistoryStage stage; ControlRootImage root;
	ClusterWalInputsV1 *inputs = NULL; uint32 count; unsigned historical = 0;
	v2_close_history_fixture_sequenced(bytes, &stop, &history, &stage, 2, true);
	root_fixture_version3(bytes);
	UT_ASSERT_EQ(cluster_control_root_v3_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root), 0);
	tx_ir_complete(&history.records[0].snapshot);
	test_actual_cf = ExclusiveLock;
	UT_ASSERT_EQ(cluster_wal_history_prepare(&root, 0, &history, 890003, uuid, &stage), 0);
	UT_ASSERT_EQ(cluster_wal_history_install(&stage), 0);
	root.refs[0].history_generation = stage.generation;
	memcpy(root.refs[0].history_sha256, stage.sha256, 32);
	UT_ASSERT_EQ(cluster_wal_history_discard(&stage), 0);
	test_actual_cf = test_cf_mode = NoLock;
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0); v2_write_roots(root.bytes);
	cluster_shared_config = true; MyBackendType = B_BG_WORKER;
	inputs_pin_held = inputs_pin_refuse = inputs_pin_race = inputs_pin_throw = false;
	UT_ASSERT_EQ(cluster_wal_inputs_begin_v1(v2_storage, TEST_SYSID, &inputs), 0);
	count = cluster_wal_inputs_count_v1(inputs); UT_ASSERT_EQ(count, 3);
	memset(&tx_inputs_typed, 0, sizeof(tx_inputs_typed));
	tx_inputs_typed.system_identifier = TEST_SYSID;
	tx_inputs_typed.census_root_captured = true;
	UT_ASSERT_EQ(cluster_wal_inputs_root_token_v1(inputs, &tx_inputs_typed.census_root), 0);
	for (uint32 i = 0; i < count; i++) {
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(inputs, i);
		if (!item->current && item->checkpoint.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE) {
			ClusterColdParticipantV1 *cut = &tx_inputs_typed.participants[0];
			tx_inputs_typed.participant_count = tx_inputs_typed.replay_count = 1;
			tx_inputs_typed.sources[0] = item->source;
			cut->thread_id = item->source.claim.identity.origin_thread_id;
			cut->owner_incarnation = item->source.claim.identity.origin_owner_incarnation;
			cut->timeline = item->source.timeline;
			cut->physical_lower = item->checkpoint.checkpoint_lower_lsn;
			cut->native_redo = item->native_redo;
			cut->tail_end = item->checkpoint.validated_tail_lsn_exclusive;
		}
	}
	cluster_wal_inputs_release_v1(&inputs); tx_inputs_activate(); ctrc_inputs_activate_work();
	ctrc_inputs_work.counts.subject_count = ctrc_inputs_work.counts.unresolved_count = 0;
	UT_ASSERT_EQ(ctrc_inputs_begin(&inputs), 0);
	ctrc_ir_original_scope = inputs; ctrc_ir_start_current = true;
	ctrc_set_held = ctrc_set_calls = ctrc_set_release_calls = 0;
	ctrc_set_block_thread = ctrc_set_fail_release = 0; ctrc_set_after_grant = NULL;
	ctrc_set_preflight_retire = 0; ctrc_replay_current_override = ctrc_set_original_current;
	test_ctrc_set_acquire_hook = ctrc_set_native_acquire;
	test_ctrc_set_release_hook = ctrc_set_native_release;
	relmap_declared = true; relmap_failed = 0;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	UT_ASSERT_EQ(cluster_wal_inputs_relmap_count_v1(inputs), count);
	for (uint32 i = 0; i < count; i++) {
		ClusterRecoveryRelmapSourceV1 source = cluster_wal_inputs_relmap_source_v1(inputs, i);
		const ClusterWalInputV1 *item = cluster_wal_inputs_at_v1(inputs, i);
		UT_ASSERT(cluster_wal_inputs_relmap_source_current_v1(&source));
		UT_ASSERT_EQ(source.input_count, count); UT_ASSERT_EQ(source.replay_count, 1);
		UT_ASSERT(memcmp(&source.selected, item, sizeof(*item)) == 0);
		if (!item->current) {
			historical++;
			UT_ASSERT(memcmp(&source.selected.checkpoint, &history.records[historical - 1].snapshot,
				sizeof(source.selected.checkpoint)) == 0);
		}
	}
	UT_ASSERT_EQ(historical, 2); UT_ASSERT_EQ(ctrc_set_held, 1); UT_ASSERT_EQ(relmap_failed, 0);
	ctrc_set_finish(&inputs); relmap_declared = false;
}

UT_TEST(test_relmap_native_input_refuses_caller_cf_or_critical_without_revocation)
{
	ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapNativeInputV1 native;
	unsigned calls;
	relmap_participant_fixture(&inputs, false); relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	calls = test_cf_lock_calls;
	CritSectionCount = 1;
	native = cluster_wal_inputs_relmap_native_v1(inputs, 0);
	CritSectionCount = 0;
	UT_ASSERT(v2_zero(&native, sizeof(native))); UT_ASSERT_EQ(relmap_failed, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_relmap_count_v1(inputs), 2);
	test_actual_cf = ShareLock;
	native = cluster_wal_inputs_relmap_native_v1(inputs, 0);
	test_actual_cf = NoLock;
	UT_ASSERT(v2_zero(&native, sizeof(native))); UT_ASSERT_EQ(relmap_failed, 0);
	UT_ASSERT_EQ(test_cf_lock_calls, calls);
	UT_ASSERT_EQ(cluster_wal_inputs_relmap_count_v1(inputs), 2);
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(inputs_recovery_held);
	ctrc_set_finish(&inputs); relmap_declared = false;
}

#ifdef USE_RELMAP_SOURCE_REFUSAL_SHIM
bool
cluster_wal_inputs_relmap_source_revalidate_v1(const ClusterRecoveryRelmapSourceV1 *source pg_attribute_unused())
{
	return false;
}
void
cluster_wal_inputs_relmap_source_fail_v1(const ClusterRecoveryRelmapSourceV1 *source pg_attribute_unused())
{}
#endif

UT_TEST(test_relmap_source_whole_root_recheck_retains_original_holder_debt)
{
	ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapSourceV1 source;
	volatile bool caught = false;
	relmap_participant_fixture(&inputs, false); relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	source = cluster_wal_inputs_relmap_source_v1(inputs, 0);
	UT_ASSERT(cluster_wal_inputs_relmap_source_revalidate_v1(&source));
	UT_ASSERT_EQ(relmap_failed, 0);
	test_throw_root_read = true;
	PG_TRY(); { (void)cluster_wal_inputs_relmap_source_revalidate_v1(&source); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	test_throw_root_read = false;
	UT_ASSERT(caught); UT_ASSERT_EQ(relmap_failed, 1); UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(inputs_recovery_held);
	ctrc_set_finish(&inputs); relmap_declared = false;
}

UT_TEST(test_relmap_source_failure_is_original_incarnation_only_and_owner_sticky)
{
	ClusterWalInputsV1 *inputs = NULL; ClusterRecoveryRelmapSourceV1 source, wrong;
	ResourceOwner owner;
	relmap_participant_fixture(&inputs, false); relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	source = cluster_wal_inputs_relmap_source_v1(inputs, 0); wrong = source;
	wrong.input_owner.generation++;
	cluster_wal_inputs_relmap_source_fail_v1(&wrong); UT_ASSERT_EQ(relmap_failed, 0);
	owner = CurrentResourceOwner; CurrentResourceOwner = NULL;
	cluster_wal_inputs_relmap_source_fail_v1(&source);
	CurrentResourceOwner = owner;
	UT_ASSERT_EQ(relmap_failed, 1);
	UT_ASSERT(!cluster_wal_inputs_relmap_source_current_v1(&source));
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT(inputs_recovery_held);
	ctrc_set_finish(&inputs); relmap_declared = false;
	/* The released value cannot revoke a later native incarnation. */
	relmap_participant_fixture(&inputs, false); relmap_declared = true;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(inputs), 0);
	cluster_wal_inputs_relmap_source_fail_v1(&source);
	UT_ASSERT_EQ(relmap_failed, 0);
	UT_ASSERT_EQ(cluster_wal_inputs_relmap_count_v1(inputs), 2);
	ctrc_set_finish(&inputs); relmap_declared = false;
}
