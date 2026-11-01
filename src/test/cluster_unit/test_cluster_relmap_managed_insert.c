/*-------------------------------------------------------------------------
 * test_cluster_relmap_managed_insert.c
 *    Native WAL ordering through the original retained, registered pair I/O.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include <setjmp.h>

int original_storage_main(void);
#define main original_storage_main
#include "test_cluster_relmap_v2_storage.c"
#undef main

Oid MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
static unsigned wal_begins, wal_registers, wal_inserts, wal_flushes, wal_step;
static unsigned wal_fault;
static unsigned char wal_payload[CLUSTER_RELMAP_FILE_SIZE_V2];
static const XLogRecPtr actual_end = UINT64CONST(0x987654);
static sigjmp_buf native_error;

FullTransactionId
GetCurrentFullTransactionIdIfAny(void)
{
	UT_ASSERT(!retention_held && CritSectionCount == 0);
	return FullTransactionIdFromU64(runtime_storage_full_xid);
}

static void
wal_context(void)
{
	UT_ASSERT(mapping_held && relmap_x_held && !retention_held);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(writes, 0);
}

void
XLogBeginInsert(void)
{
	wal_context();
	UT_ASSERT_EQ(wal_step, 0);
	wal_begins++;
	wal_step++;
}

void
XLogRegisterData(char *data, uint32 length)
{
	wal_context();
	UT_ASSERT_EQ(wal_step, 1);
	UT_ASSERT_EQ(length, sizeof(wal_payload));
	if (length == sizeof(wal_payload))
		memcpy(wal_payload, data, length);
	wal_registers++;
	wal_step++;
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	wal_context();
	UT_ASSERT_EQ(wal_step, 2);
	UT_ASSERT_EQ(rmid, RM_RELMAP_ID);
	UT_ASSERT_EQ(info, XLOG_RELMAP_UPDATE_V2);
	wal_inserts++;
	wal_step++;
	if (wal_fault == 7)
		siglongjmp(native_error, 1);
	return wal_fault == 1 ? InvalidXLogRecPtr : actual_end;
}

void
XLogFlush(XLogRecPtr end)
{
	wal_context();
	UT_ASSERT_EQ(wal_step, 3);
	UT_ASSERT_EQ(end, actual_end);
	wal_flushes++;
	wal_step++;
	if (wal_fault == 2) runtime_storage_writer.epoch++;
	if (wal_fault == 3) mapping_held = false;
	if (wal_fault == 4) relmap_x_held = false;
	if (wal_fault == 5) stage_directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
	if (wal_fault == 6) stage_directory->pins[0].key.generation++;
	if (wal_fault == 8) siglongjmp(native_error, 1);
}

static void
managed_insert_setup(ClusterRelmapOwnerSampleV2 *sample,
	ClusterRecoveryFileIdentityV2 *id, ClusterRelmapV2Image *intent, bool shared, unsigned origin)
{
	ClusterRelmapV2Namespace ns;
	ClusterRelmapV2Image clean = {0};
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	runtime_storage_setup(sample, &ns, origin, shared);
	memset(id, 0, sizeof(*id));
	id->kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	id->format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	id->format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	id->system_identifier = ns.system_identifier;
	id->database_incarnation = ns.database_incarnation;
	id->storage_uuid[0] = 1;
	id->redo_branch_uuid[0] = 2;
	id->redo_branch_generation = 3;
	id->redo_timeline = 4;
	id->database_oid = ns.dbid;
	id->tablespace_oid = shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
	/* The shared file's creation origin is independent of its current writer. */
	id->origin = 0;
	id->creation_generation = 103;
	id->registration_generation = 19;
	memcpy(runtime_storage_writer.ref.claim.identity.storage_uuid, id->storage_uuid, 16);
	sample->writer = runtime_storage_writer;
	clean.identity = ns;
	clean.committed_generation = 7;
	clean.committed = map_image(40000);
	UT_ASSERT(cluster_relmap_file_encode_v2(id, &clean, bytes, sizeof(bytes)));
	write_copy(0, bytes, sizeof(bytes));
	write_copy(1, bytes, sizeof(bytes));
	*intent = clean;
	intent->pending_generation = 8;
	intent->pending_owner = sample->owner;
	intent->pending = map_image(40001);
	wal_begins = wal_registers = wal_inserts = wal_flushes = wal_step = wal_fault = 0;
	memset(wal_payload, 0xa5, sizeof(wal_payload));
	reset_io();
	stage_expected_wal_end = actual_end;
	mapping_held = relmap_x_held = observing_stage = true;
}

static void
insert_assert_debt(void)
{
	UT_ASSERT(stage_directory->pins[0].occupied);
	UT_ASSERT_EQ(io_under_retention_lock, 0);
	UT_ASSERT_EQ(writes_without_pin, 0);
	UT_ASSERT(!retention_held);
	UT_ASSERT_EQ(retention_acquires, retention_releases);
}

UT_TEST(managed_insert_wal_intent_precedes_exact_actual_end_pair)
{
	for (unsigned shared = 0; shared < 2; ++shared)
		for (unsigned origin = 0; origin < 4; origin += 3) {
			ClusterRelmapOwnerSampleV2 sample;
			ClusterRecoveryFileIdentityV2 id, observed;
			ClusterRelmapV2Image intent, saved, decoded;
			ClusterRelmapFilePairV2 pair;
			unsigned char expected[CLUSTER_RELMAP_FILE_SIZE_V2];
			managed_insert_setup(&sample, &id, &intent, shared, origin);
			saved = intent;
			UT_ASSERT_EQ(cluster_relmap_retention_runtime_insert_managed_v2(&sample, &id, &intent), actual_end);
			UT_ASSERT_EQ(wal_begins, 1); UT_ASSERT_EQ(wal_registers, 1);
			UT_ASSERT_EQ(wal_inserts, 1); UT_ASSERT_EQ(wal_flushes, 1);
			UT_ASSERT_EQ(wal_payload[181], 1);
			for (unsigned i = 208; i < 216; ++i) UT_ASSERT_EQ(wal_payload[i], 0);
			UT_ASSERT(cluster_relmap_wal_observe_intent_v2(wal_payload, sizeof(wal_payload), &observed, &decoded));
			UT_ASSERT_EQ(decoded.pending_owner.wal_end, 0);
			UT_ASSERT_EQ(decoded.pending_owner.node, origin);
			UT_ASSERT_EQ(decoded.committed.mappings[0].mapfilenumber, 40000);
			UT_ASSERT_EQ(decoded.pending.mappings[0].mapfilenumber, 40001);
			UT_ASSERT_EQ(memcmp(&intent, &saved, sizeof(saved)), 0);
			intent.pending_owner.wal_end = actual_end;
			UT_ASSERT(cluster_relmap_file_encode_v2(&id, &intent, expected, sizeof(expected)));
			UT_ASSERT(cluster_relmap_authority_managed_read_v2(&id, &pair));
			UT_ASSERT_EQ(memcmp(pair.bytes[0], expected, sizeof(expected)), 0);
			UT_ASSERT_EQ(memcmp(pair.bytes[1], expected, sizeof(expected)), 0);
			UT_ASSERT_EQ(pair.view.committed_generation, 7);
			UT_ASSERT_EQ(pair.view.committed.mappings[0].mapfilenumber, 40000);
			UT_ASSERT(pair.view.has_pending && pair.view.pending_complete);
			UT_ASSERT_EQ(stage_directory->pins[0].wal_end, actual_end);
			insert_assert_debt(); cleanup();
		}
}

UT_TEST(managed_insert_refuses_invalid_owner_binding_base_and_preparation_before_wal)
{
	for (unsigned fault = 0; fault < 29; ++fault) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRecoveryFileIdentityV2 id;
		ClusterRelmapV2Image intent;
		ClusterRelmapRetentionDirectory before;
		managed_insert_setup(&sample, &id, &intent, false, 3);
		if (fault == 0) sample.owner.top_full_xid++;
		if (fault == 1) intent.pending_owner.node++;
		if (fault == 2) intent.pending_owner.top_full_xid++;
		if (fault == 3) intent.pending_owner.membership_epoch++;
		if (fault == 4) intent.pending_owner.admitted_incarnation++;
		if (fault == 5) intent.pending_owner.boot_incarnation++;
		if (fault == 6) intent.pending_owner.wal_end = 1;
		if (fault == 7) intent.committed = map_image(39999);
		if (fault == 8) { intent.committed_generation = 8; intent.pending_generation = 9; }
		if (fault == 9) intent.pending.crc++;
		if (fault == 10) id.creation_generation++;
		if (fault == 11) id.registration_generation++;
		if (fault == 12) id.storage_uuid[0]++;
		if (fault == 13) id.redo_branch_uuid[0]++;
		if (fault == 14) id.database_incarnation++;
		if (fault == 15) id.tablespace_oid = GLOBALTABLESPACE_OID;
		if (fault == 16) mapping_held = false;
		if (fault == 17) relmap_x_held = false;
		if (fault == 18) stage_directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (fault == 19) stage_directory->pins[0].occupied = false;
		if (fault == 20) stage_directory->pins[0].may_be_staged = true;
		if (fault == 21) stage_directory->pins[0].key.generation++;
		if (fault == 22) enableFsync = false;
		if (fault == 23) runtime_storage_full_xid += UINT64CONST(0x100000000);
		if (fault == 24) CritSectionCount = 1;
		if (fault == 25) id.system_identifier++;
		before = *stage_directory;
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_insert_managed_v2(
			fault == 26 ? NULL : &sample, fault == 27 ? NULL : &id,
			fault == 28 ? NULL : &intent), InvalidXLogRecPtr);
		UT_ASSERT_EQ(wal_begins + wal_inserts + wal_flushes, 0);
		UT_ASSERT_EQ(writes + renames, 0);
		UT_ASSERT_EQ(memcmp(stage_directory, &before, sizeof(before)), 0);
		CritSectionCount = 0; enableFsync = true; cleanup();
	}
}

UT_TEST(managed_insert_late_native_refusal_never_stages_or_cancels)
{
	for (unsigned fault = 1; fault <= 6; ++fault) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRecoveryFileIdentityV2 id;
		ClusterRelmapV2Image intent;
		managed_insert_setup(&sample, &id, &intent, true, 3);
		wal_fault = fault;
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_insert_managed_v2(&sample, &id, &intent), InvalidXLogRecPtr);
		UT_ASSERT_EQ(wal_inserts, 1);
		UT_ASSERT_EQ(wal_flushes, fault == 1 ? 0 : 1);
		UT_ASSERT_EQ(writes + renames, 0);
		UT_ASSERT(!stage_directory->pins[0].may_be_staged);
		UT_ASSERT_EQ(CritSectionCount, 0);
		insert_assert_debt(); cleanup();
	}
}

static void
lose_owner_after_pair_sync(bool is_directory)
{
	if (is_directory && dir_syncs == 2)
		runtime_storage_writer.epoch++;
}

UT_TEST(managed_insert_partial_files_or_final_owner_failure_keep_staged_debt)
{
	for (unsigned fault = 0; fault < 6; ++fault) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRecoveryFileIdentityV2 id;
		ClusterRelmapV2Image intent;
		managed_insert_setup(&sample, &id, &intent, true, 3);
		if (fault == 0) fail_write = 1;
		if (fault == 1) fail_file_sync = 2;
		if (fault == 2) fail_dir_sync = 2;
		if (fault == 3) fail_rename = 2;
		if (fault == 4) post_sync_hook = lose_owner_after_pair_sync;
		if (fault == 5) { corrupt_after_rename = true; fault_rename = 2; fault_copy = 0; }
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_insert_managed_v2(&sample, &id, &intent), InvalidXLogRecPtr);
		UT_ASSERT_EQ(wal_inserts, 1); UT_ASSERT_EQ(wal_flushes, 1);
		UT_ASSERT(stage_directory->pins[0].may_be_staged);
		UT_ASSERT_EQ(stage_directory->pins[0].wal_end, actual_end);
		insert_assert_debt(); cleanup();
	}
}

UT_TEST(managed_insert_critical_errors_propagate_without_file_or_pin_cleanup)
{
	for (unsigned fault = 7; fault <= 8; ++fault) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRecoveryFileIdentityV2 id;
		ClusterRelmapV2Image intent;
		volatile bool caught = false;
		managed_insert_setup(&sample, &id, &intent, true, 3);
		wal_fault = fault;
		if (sigsetjmp(native_error, 1) == 0)
			(void) cluster_relmap_retention_runtime_insert_managed_v2(&sample, &id, &intent);
		else caught = true;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(CritSectionCount, 1);
		UT_ASSERT_EQ(writes + renames, 0);
		UT_ASSERT(!stage_directory->pins[0].may_be_staged);
		insert_assert_debt();
		/* Only the fake native leaf longjmps; actual critical ERROR fail-stops. */
		CritSectionCount = 0; cleanup();
	}
}

UT_TEST(managed_insert_original_staged_pin_rejects_second_native_attempt)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapV2Image intent;
	managed_insert_setup(&sample, &id, &intent, true, 3);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_insert_managed_v2(&sample, &id, &intent), actual_end);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_insert_managed_v2(&sample, &id, &intent), InvalidXLogRecPtr);
	UT_ASSERT_EQ(wal_inserts, 1); UT_ASSERT_EQ(wal_flushes, 1);
	insert_assert_debt(); cleanup();
}

static unsigned terminal_fault;

static bool
managed_terminal_native(const ClusterRelmapOwnerSampleV2 *sample,
	ClusterRelmapRecoveryNativeTerminal *out)
{
	UT_ASSERT(!mapping_held && !retention_held);
	UT_ASSERT_EQ(writes + renames, 0);
	UT_ASSERT(test_relmap_native_commit_default(sample, out));
	if (terminal_fault == 1) out->status = TRANSACTION_STATUS_ABORTED;
	if (terminal_fault == 2) out->top_full_xid++;
	if (terminal_fault == 3) out->bounds.system_identifier++;
	if (terminal_fault == 4) out->bounds.database_incarnation++;
	if (terminal_fault == 5) out->bounds.origin++;
	if (terminal_fault == 6) out->bounds.membership_epoch++;
	if (terminal_fault == 7) out->bounds.admitted_incarnation++;
	if (terminal_fault == 8) out->bounds.boot_incarnation++;
	if (terminal_fault == 9) out->bounds.native_floor = sample->owner.top_full_xid + 1;
	if (terminal_fault == 10) out->bounds.last_issued = sample->owner.top_full_xid - 1;
	if (terminal_fault == 11) runtime_storage_writer.epoch++;
	if (terminal_fault == 12) stage_directory->pins[0].wal_end++;
	if (terminal_fault == 14) siglongjmp(native_error, 1);
	return terminal_fault != 13;
}

static void
managed_finish_setup(ClusterRelmapOwnerSampleV2 *sample,
	ClusterRecoveryFileIdentityV2 *id, ClusterRelmapV2Image *staged,
	bool shared, unsigned origin, bool publish)
{
	managed_insert_setup(sample, id, staged, shared, origin);
	UT_ASSERT_EQ(cluster_relmap_retention_runtime_insert_managed_v2(sample, id, staged), actual_end);
	staged->pending_owner.wal_end = actual_end;
	runtime_storage_reset_terminal_io(publish);
	test_relmap_native_commit_calls = terminal_fault = 0;
	test_relmap_native_commit_hook = managed_terminal_native;
}

static ClusterRelmapV2ClearResult
managed_finish(const ClusterRelmapOwnerSampleV2 *sample,
	const ClusterRecoveryFileIdentityV2 *id, const ClusterRelmapV2Image *staged,
	bool publish)
{
	return publish ? cluster_relmap_retention_runtime_publish_managed_v2(sample, id, staged)
		: cluster_relmap_retention_runtime_discard_pending_managed_v2(sample, id, staged);
}

UT_TEST(managed_terminal_original_stage_to_both_clean_copies_keeps_pin)
{
	for (unsigned shared = 0; shared < 2; ++shared)
		for (unsigned origin = 0; origin < 4; origin += 3)
			for (unsigned publish = 0; publish < 2; ++publish) {
		ClusterRelmapOwnerSampleV2 sample, saved_sample;
		ClusterRecoveryFileIdentityV2 id, saved_id;
		ClusterRelmapV2Image staged, saved_staged, clean;
		ClusterRelmapRetentionPin pin;
		ClusterRelmapFilePairV2 pair;
		unsigned char expected[CLUSTER_RELMAP_FILE_SIZE_V2];
		managed_finish_setup(&sample, &id, &staged, shared, origin, publish);
		saved_sample = sample; saved_id = id; saved_staged = staged;
		pin = stage_directory->pins[0];
		UT_ASSERT_EQ(managed_finish(&sample, &id, &staged, publish), CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
		UT_ASSERT_EQ(test_relmap_native_commit_calls, publish);
		UT_ASSERT_EQ(writes, 2); UT_ASSERT_EQ(renames, 2);
		UT_ASSERT_EQ(file_syncs, 2); UT_ASSERT_EQ(dir_syncs, 2);
		UT_ASSERT(!mapping_held);
		clean = staged;
		clean.pending_generation = 0;
		memset(&clean.pending_owner, 0, sizeof(clean.pending_owner));
		memset(&clean.pending, 0, sizeof(clean.pending));
		if (publish) { clean.committed_generation = staged.pending_generation; clean.committed = staged.pending; }
		UT_ASSERT(cluster_relmap_file_encode_v2(&id, &clean, expected, sizeof(expected)));
		mapping_held = true;
		UT_ASSERT(cluster_relmap_authority_managed_read_v2(&id, &pair));
		mapping_held = false;
		UT_ASSERT_EQ(memcmp(pair.bytes[0], expected, sizeof(expected)), 0);
		UT_ASSERT_EQ(memcmp(pair.bytes[1], expected, sizeof(expected)), 0);
		UT_ASSERT_EQ(memcmp(&pin, &stage_directory->pins[0], sizeof(pin)), 0);
		UT_ASSERT_EQ(memcmp(&sample, &saved_sample, sizeof(sample)), 0);
		UT_ASSERT_EQ(memcmp(&id, &saved_id, sizeof(id)), 0);
		UT_ASSERT_EQ(memcmp(&staged, &saved_staged, sizeof(staged)), 0);
		insert_assert_debt(); cleanup();
	}
	test_relmap_native_commit_hook = NULL;
}

UT_TEST(managed_terminal_refuses_wrong_registration_image_owner_and_pin)
{
	for (unsigned publish = 0; publish < 2; ++publish)
		for (unsigned fault = 0; fault < 21; ++fault) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRecoveryFileIdentityV2 id;
		ClusterRelmapV2Image staged;
		ClusterRelmapRetentionDirectory before;
		managed_finish_setup(&sample, &id, &staged, true, 3, publish);
		if (fault == 0) id.creation_generation++;
		if (fault == 1) id.registration_generation++;
		if (fault == 2) id.storage_uuid[0]++;
		if (fault == 3) id.redo_branch_uuid[0]++;
		if (fault == 4) id.tablespace_oid = DEFAULTTABLESPACE_OID;
		if (fault == 5) id.system_identifier++;
		if (fault == 6) staged.pending.crc++;
		if (fault == 7) staged.committed = map_image(39999);
		if (fault == 8) staged.pending_owner.wal_end++;
		if (fault == 9) staged.pending_owner.node++;
		if (fault == 10) sample.producer_pid++;
		if (fault == 11) stage_directory->pins[0].may_be_staged = false;
		if (fault == 12) stage_directory->pins[0].wal_end++;
		if (fault == 13) stage_directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (fault == 14) relmap_x_held = false;
		if (fault == 15) mapping_held = true;
		if (fault == 16) enableFsync = false;
		if (fault == 17) staged.pending_owner.wal_end = 0;
		before = *stage_directory;
		UT_ASSERT_EQ(managed_finish(fault == 18 ? NULL : &sample,
			fault == 19 ? NULL : &id, fault == 20 ? NULL : &staged, publish), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(writes + renames, 0);
		UT_ASSERT_EQ(memcmp(&before, stage_directory, sizeof(before)), 0);
		UT_ASSERT_EQ(mapping_held, fault == 15);
		mapping_held = false; enableFsync = true;
		insert_assert_debt(); cleanup();
	}
	test_relmap_native_commit_hook = NULL;
}

UT_TEST(managed_terminal_commit_requires_exact_original_native_committed_fact)
{
	for (unsigned fault = 1; fault <= 13; ++fault) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRecoveryFileIdentityV2 id;
		ClusterRelmapV2Image staged;
		managed_finish_setup(&sample, &id, &staged, true, 3, true);
		terminal_fault = fault;
		UT_ASSERT_EQ(managed_finish(&sample, &id, &staged, true), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(test_relmap_native_commit_calls, 1);
		UT_ASSERT_EQ(writes + renames, 0);
		UT_ASSERT(!mapping_held);
		insert_assert_debt(); cleanup();
	}
	test_relmap_native_commit_hook = NULL;
}

UT_TEST(managed_terminal_late_pair_failures_and_owner_drift_retain_pin)
{
	for (unsigned publish = 0; publish < 2; ++publish)
		for (unsigned fault = 0; fault < 7; ++fault) {
		ClusterRelmapOwnerSampleV2 sample;
		ClusterRecoveryFileIdentityV2 id;
		ClusterRelmapV2Image staged;
		ClusterRelmapRetentionPin pin;
		managed_finish_setup(&sample, &id, &staged, true, 3, publish);
		pin = stage_directory->pins[0];
		if (fault == 0) fail_write = 1;
		if (fault == 1) fail_file_sync = 2;
		if (fault == 2) fail_dir_sync = 2;
		if (fault == 3) fail_rename = 2;
		if (fault == 4) post_sync_hook = lose_owner_after_pair_sync;
		if (fault == 5) post_sync_hook = runtime_storage_invalidate_after_last_sync;
		if (fault == 6) fail_directory_close = true;
		UT_ASSERT_EQ(managed_finish(&sample, &id, &staged, publish), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT(!mapping_held);
		UT_ASSERT_EQ(memcmp(&pin, &stage_directory->pins[0], sizeof(pin)), 0);
		insert_assert_debt(); cleanup();
	}
	test_relmap_native_commit_hook = NULL;
}

UT_TEST(managed_terminal_native_error_precedes_map_lock_and_preserves_pin)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRecoveryFileIdentityV2 id;
	ClusterRelmapV2Image staged;
	ClusterRelmapRetentionPin pin;
	volatile bool caught = false;
	managed_finish_setup(&sample, &id, &staged, true, 3, true);
	pin = stage_directory->pins[0];
	terminal_fault = 14;
	if (sigsetjmp(native_error, 1) == 0)
		(void) managed_finish(&sample, &id, &staged, true);
	else caught = true;
	UT_ASSERT(caught);
	UT_ASSERT(!mapping_held);
	UT_ASSERT_EQ(writes + renames, 0);
	UT_ASSERT_EQ(memcmp(&pin, &stage_directory->pins[0], sizeof(pin)), 0);
	insert_assert_debt(); cleanup();
	test_relmap_native_commit_hook = NULL;
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(managed_insert_wal_intent_precedes_exact_actual_end_pair);
	UT_RUN(managed_insert_refuses_invalid_owner_binding_base_and_preparation_before_wal);
	UT_RUN(managed_insert_late_native_refusal_never_stages_or_cancels);
	UT_RUN(managed_insert_partial_files_or_final_owner_failure_keep_staged_debt);
	UT_RUN(managed_insert_critical_errors_propagate_without_file_or_pin_cleanup);
	UT_RUN(managed_insert_original_staged_pin_rejects_second_native_attempt);
	UT_RUN(managed_terminal_original_stage_to_both_clean_copies_keeps_pin);
	UT_RUN(managed_terminal_refuses_wrong_registration_image_owner_and_pin);
	UT_RUN(managed_terminal_commit_requires_exact_original_native_committed_fact);
	UT_RUN(managed_terminal_late_pair_failures_and_owner_drift_retain_pin);
	UT_RUN(managed_terminal_native_error_precedes_map_lock_and_preserves_pin);
	free(runtime_storage_allocation);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
