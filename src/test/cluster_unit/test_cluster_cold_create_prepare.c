#define rf_side_online_plan_bind_database_v1 fixture_rf_side_online_plan_bind_database_v1
#define rf_side_online_plan_bind_sources_v1 fixture_rf_side_online_plan_bind_sources_v1
#define rf_side_online_plan_create_v1 fixture_rf_side_online_plan_create_v1
#define rf_side_online_plan_destroy_v1 fixture_rf_side_online_plan_destroy_v1
#define rf_side_online_plan_native_creation_v1 fixture_rf_side_online_plan_native_creation_v1
#define rf_side_online_plan_operation_count_v1 fixture_rf_side_online_plan_operation_count_v1
#define rf_side_online_plan_operation_v1 fixture_rf_side_online_plan_operation_v1
#define rf_side_online_plan_recovery_abort_sources_v1 fixture_rf_side_online_plan_recovery_abort_sources_v1
#define rf_side_online_plan_replay_start_matches_v1 fixture_rf_side_online_plan_replay_start_matches_v1
#define rf_side_online_plan_seal_v1 fixture_rf_side_online_plan_seal_v1
#define rf_side_online_plan_source_matches_v1 fixture_rf_side_online_plan_source_matches_v1
#define rf_side_online_production_apply_record_v1 fixture_rf_side_online_production_apply_record_v1
#define rf_side_online_production_bind_undo_v1 fixture_rf_side_online_production_bind_undo_v1
#define rf_side_online_production_owner_init_v1 fixture_rf_side_online_production_owner_init_v1
#define cluster_cold_scan_root_v1 fixture_cluster_cold_scan_root_v1
#define cluster_cold_scan_root_deadline_v1 fixture_cluster_cold_scan_root_deadline_v1
int startup_fixture_main(void);
#define main startup_fixture_main
#include "test_cluster_cold_recovery_startup.c"
#undef main
#undef rf_side_online_plan_bind_database_v1
#undef rf_side_online_plan_bind_sources_v1
#undef rf_side_online_plan_create_v1
#undef rf_side_online_plan_destroy_v1
#undef rf_side_online_plan_native_creation_v1
#undef rf_side_online_plan_operation_count_v1
#undef rf_side_online_plan_operation_v1
#undef rf_side_online_plan_recovery_abort_sources_v1
#undef rf_side_online_plan_replay_start_matches_v1
#undef rf_side_online_plan_seal_v1
#undef rf_side_online_plan_source_matches_v1
#undef rf_side_online_production_apply_record_v1
#undef rf_side_online_production_bind_undo_v1
#undef rf_side_online_production_owner_init_v1
#undef cluster_cold_scan_root_v1
#undef cluster_cold_scan_root_deadline_v1
#include "common/cluster_relmap_create.h"
#include "cluster/cluster_block_apply.h"

typedef struct StartupScanRecord {
	XLogReaderState reader;
	union { uint64 align; char bytes[BLCKSZ]; } data;
	DecodedXLogRecord decoded;
} StartupScanRecord;
/* These PAGE execution leaves must remain unreachable in this no-block test. */
ClusterBlkApplyResult cluster_block_apply_one(XLogReaderState *r, uint8 id, char *page)
{ (void)r; (void)id; (void)page; abort(); }
bool rf_page_version_present_v1(const RfPageVersionV1 *version)
{ (void)version; abort(); }
static bool emit_creation;
static unsigned registry_fault;
static unsigned real_visits, real_records;

static void scan_record(StartupScanRecord *r, uint16 thread, uint8 rmid,
					   uint8 info, XLogRecPtr begin, XLogRecPtr end, XLogRecPtr prev)
{
	memset(r, 0, sizeof(*r));
	r->reader.record = &r->decoded;
	r->reader.system_identifier = 9;
	r->reader.seg.ws_tli = 1;
	r->decoded.lsn = begin;
	r->decoded.next_lsn = end;
	r->decoded.header.xl_tot_len = SizeOfXLogRecord;
	r->reader.ReadRecPtr = begin;
	r->reader.EndRecPtr = end;
	r->reader.cluster_expected_thread_id = thread;
	r->decoded.header.xl_rmid = rmid;
	r->decoded.header.xl_info = info;
	r->decoded.header.xl_prev = prev;
	r->decoded.header.xl_scn = 23;
	r->decoded.max_block_id = -1;
	r->decoded.main_data = r->data.bytes;
}

static void scan_creation(StartupScanRecord *r)
{
	ClusterRecoveryFileIdentityV2 id = {0};
	RelMapFile map = {0};
	uint8 batch[16] = {7};
	id.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	id.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	id.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	id.system_identifier = 9;
	id.database_incarnation = 5;
	memset(id.storage_uuid, 1, 16);
	id.redo_branch_uuid[0] = 9;
	id.redo_branch_generation = 4;
	id.redo_timeline = 1;
	id.database_oid = 2000;
	id.tablespace_oid = DEFAULTTABLESPACE_OID;
	id.origin = 0;
	id.creation_generation = 11;
	id.registration_generation = 41;
	map.magic = RELMAPPER_FILEMAGIC;
	map.num_mappings = 1;
	map.mappings[0] = (RelMapping){1259, 9001};
	INIT_CRC32C(map.crc);
	COMP_CRC32C(map.crc, &map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map.crc);
	if (!cluster_relmap_create_encode_v1(&id, batch, &map, r->data.bytes,
									   CLUSTER_RELMAP_CREATE_WAL_SIZE_V1)) abort();
	r->decoded.main_data_len = CLUSTER_RELMAP_CREATE_WAL_SIZE_V1;
	r->decoded.header.xl_tot_len += 5 + CLUSTER_RELMAP_CREATE_WAL_SIZE_V1;
}

ClusterControlRootResult
cluster_control_root_recovery_visit(const ClusterControlRootSnapshot *root,
	const ClusterControlRootReadToken *token, ClusterWalRecordVisitor visit,
	void *arg, ClusterWalTailObservation *out)
{
	StartupScanRecord r;
	uint16 t = root->identity.origin_thread_id;
	if (memcmp(root, &roots[t], sizeof(*root)) ||
		memcmp(token, &tokens[t], sizeof(*token))) return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	UT_ASSERT(!registry_cf_held);
	++real_visits;
	if (registry_fault == 1 && real_visits == 1) registry_drift = true;
	if (registry_fault == 2 && real_visits == 1) registry_entries[0].batch_uuid[0] ^= 1;
	if (registry_fault == 3 && real_visits == 1) source_result[2] = CLUSTER_CONTROL_ROOT_IO_ERROR;
	scan_record(&r, t, RM_XLOG_ID, XLOG_NOOP, 0x100, 0x800, 0x10);
	++real_records;
	if (!visit(&r.reader, arg)) return CLUSTER_CONTROL_ROOT_IO_ERROR;
	scan_record(&r, t, emit_creation && t == 1 ? RM_RELMAP_ID : RM_XLOG_ID,
		emit_creation && t == 1 ? XLOG_RELMAP_CREATE_V1 : XLOG_NOOP,
		0x800, 0x1800, 0x100);
	if (emit_creation && t == 1) scan_creation(&r);
	++real_records;
	if (!visit(&r.reader, arg)) return CLUSTER_CONTROL_ROOT_IO_ERROR;
	scan_record(&r, t, RM_XLOG_ID, XLOG_CHECKPOINT_ONLINE, 0x1800, 0x2000, 0x800);
	native_checkpoint[t].checkpoint.redo = native_redo[t];
	memcpy(r.data.bytes, &native_checkpoint[t].checkpoint, sizeof(CheckPoint));
	r.decoded.main_data_len = sizeof(CheckPoint);
	r.decoded.header.xl_tot_len += 2 + sizeof(CheckPoint);
	++real_records;
	if (!visit(&r.reader, arg)) return CLUSTER_CONTROL_ROOT_IO_ERROR;
	memset(out, 0, sizeof(*out));
	out->complete_end = 0x2000;
	out->last_record_start = 0x1800;
	out->records = 3;
	out->database_incarnation = 5;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
ClusterControlRootResult
cluster_control_root_recovery_visit_deadline_v1(const ClusterControlRootSnapshot *root,
	const ClusterControlRootReadToken *token, ClusterWalRecordVisitor visit,
	void *arg, ClusterWalTailObservation *out, uint64 deadline)
{
	(void) deadline;
	return cluster_control_root_recovery_visit(root, token, visit, arg, out);
}

static ClusterColdTypedV1 *real_prepare(bool create)
{
	fixture();
	registry_probe_active = prepare_probe_active = true;
	prepare_probe_now = prepare_probe_deadline = 0;
	prepare_probe_expire = prepare_probe_checkpoints = prepare_probe_seen = 0;
	prepare_probe_census_read = false;
	prepare_probe_typed = NULL;
	cluster_shared_config = true;
	MyBackendType = B_STARTUP;
	MyAuxProcType = StartupProcess;
	registry_managed = registry_ok = registry_unlock_ok = true;
	registry_error = registry_drift = registry_cf_held = false;
	registry_publishes = registry_locks = registry_releases = registry_checks = 0;
	registry_count = create ? 1 : 0;
	memset(registry_entries, 0, sizeof(registry_entries));
	/* Independent preexisting persistent registration, never decoded from WAL. */
	registry_entries[0].expected = (ClusterRecoveryFileIdentityV2){
		.kind=CLUSTER_RECOVERY_FILE_RELMAP_V2,
		.format_id=CLUSTER_RECOVERY_FORMAT_ID_V2, .format_version=CLUSTER_RECOVERY_FORMAT_VERSION_V2,
		.system_identifier=9, .database_incarnation=5, .redo_branch_generation=4,
		.redo_timeline=1, .database_oid=2000, .tablespace_oid=DEFAULTTABLESPACE_OID,
		.creation_generation=11, .registration_generation=41};
	memset(registry_entries[0].expected.storage_uuid, 1, 16);
	registry_entries[0].expected.redo_branch_uuid[0]=9;
	registry_entries[0].batch_uuid[0]=7;
	memset(&registry_reference, 0, sizeof(registry_reference));
	if (registry_fault == 4) registry_count = 0;
	if (registry_fault == 5) registry_entries[0].batch_uuid[0] ^= 1;
	if (registry_fault == 6) registry_unlock_ok = false;
	if (registry_fault == 7) registry_error = true;
	emit_creation = create;
	native_side_complete_claims = true;
	real_visits = real_records = 0;
	for (uint16 t = 1; t <= NROOTS; ++t) {
		native_checkpoint[t].checkpoint_start = 0x1800;
		native_checkpoint[t].checkpoint.PrevTimeLineID = 1;
		native_checkpoint[t].checkpoint.redo = native_redo[t];
		roots[t].tail_last_record_lsn = 0x1800;
	}
	return prepare(0x800);
}
UT_TEST(original_prepare_real_scan_control)
{
	ClusterColdTypedV1 *typed = real_prepare(false);
	printf("# control refusal=%u scans=%u records=%u detail=%s\n",
		typed->refusal, real_visits, real_records, typed->refusal_detail);
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT(real_visits > 0);
	cluster_cold_typed_destroy_v1(&typed);
}
UT_TEST(original_prepare_complete_creation)
{
	ClusterColdTypedV1 *typed = real_prepare(true);
	ClusterControlNativeTableInfoV2 table = {0};
	printf("# creation refusal=%u scans=%u records=%u detail=%s\n",
		typed->refusal, real_visits, real_records, typed->refusal_detail);
	/* Frozen intended behavior. Independent registry declaration, execution
	 * barrier and final owner conjunction are still absent from this parent. */
	UT_ASSERT_EQ(typed->refusal, CLUSTER_COLD_OK);
	UT_ASSERT_EQ(real_visits, 3);
	UT_ASSERT_EQ(real_records, 9);
	UT_ASSERT_EQ(registry_publishes, 1);
	UT_ASSERT_EQ(registry_locks, registry_releases);
	UT_ASSERT_NOT_NULL(typed->plan);
	if (typed->plan) UT_ASSERT_EQ(typed->plan->relmap_create_count, 1);
	UT_ASSERT(cluster_cold_typed_native_table_v2(typed, &table));
	UT_ASSERT_EQ(table.table_generation, 17);
	registry_cf_held = true;
	UT_ASSERT(cluster_cold_typed_native_registry_current_locked_v2(typed));
	registry_entries[0].batch_uuid[0] ^= 1;
	UT_ASSERT(!cluster_cold_typed_native_registry_current_locked_v2(typed));
	registry_cf_held = false;
	cluster_cold_typed_destroy_v1(&typed);
}
UT_TEST(original_prepare_registry_drift_refuses_without_rebase)
{
	for (registry_fault = 1; registry_fault <= 3; ++registry_fault) {
		ClusterColdTypedV1 *typed = real_prepare(true);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		UT_ASSERT_EQ(registry_publishes, 1);
		UT_ASSERT_EQ(registry_locks, registry_releases);
		UT_ASSERT(!registry_cf_held);
		cluster_cold_typed_destroy_v1(&typed);
	}
	registry_fault = 0;
}
UT_TEST(original_prepare_missing_registration_and_uncertain_release_keep_debt)
{
	for (registry_fault = 4; registry_fault <= 6; ++registry_fault) {
		ClusterColdTypedV1 *typed = real_prepare(true);
		UT_ASSERT(typed->refusal != CLUSTER_COLD_OK);
		UT_ASSERT_EQ(registry_locks, registry_releases);
		UT_ASSERT_EQ(registry_cf_held, registry_fault == 6);
		if (registry_fault == 6) UT_ASSERT_EQ(real_visits, 0);
		cluster_cold_typed_destroy_v1(&typed);
		registry_cf_held = false; /* Only fixture teardown, never product retry. */
	}
	registry_fault = 0;
}
UT_TEST(original_prepare_error_revokes_without_scanning_or_retry)
{
	volatile bool threw = false;
	registry_fault = 7;
	PG_TRY(); { (void)real_prepare(true); }
	PG_CATCH(); { threw = true; } PG_END_TRY();
	UT_ASSERT(threw);
	UT_ASSERT(prepare_probe_typed != NULL);
	UT_ASSERT_EQ(prepare_probe_typed->refusal, CLUSTER_COLD_SOURCE_GAP);
	UT_ASSERT_EQ(real_visits, 0);
	UT_ASSERT_EQ(registry_locks, registry_releases);
	UT_ASSERT(!registry_cf_held);
	cluster_cold_typed_destroy_v1(&prepare_probe_typed);
	registry_fault = 0;
}
int main(void)
{
	UT_PLAN(5);
	UT_RUN(original_prepare_real_scan_control);
	UT_RUN(original_prepare_complete_creation);
	UT_RUN(original_prepare_registry_drift_refuses_without_rebase);
	UT_RUN(original_prepare_missing_registration_and_uncertain_release_keep_debt);
	UT_RUN(original_prepare_error_revokes_without_scanning_or_retry);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
