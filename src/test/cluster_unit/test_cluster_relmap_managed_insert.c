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

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(managed_insert_wal_intent_precedes_exact_actual_end_pair);
	UT_RUN(managed_insert_refuses_invalid_owner_binding_base_and_preparation_before_wal);
	UT_RUN(managed_insert_late_native_refusal_never_stages_or_cancels);
	UT_RUN(managed_insert_partial_files_or_final_owner_failure_keep_staged_debt);
	UT_RUN(managed_insert_critical_errors_propagate_without_file_or_pin_cleanup);
	UT_RUN(managed_insert_original_staged_pin_rejects_second_native_attempt);
	free(runtime_storage_allocation);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
