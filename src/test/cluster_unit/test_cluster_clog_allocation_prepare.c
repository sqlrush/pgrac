/* Actual allocator + CLOG preparation, controlled control/table providers. */
static void observe_extension(void);
static void observe_wait(void);
int clog_allocator_original_main(void);
#define XID_ALLOCATION_EXTENDED() observe_extension()
#define XID_ALLOCATION_WAITED() observe_wait()
#define XID_ALLOCATION_CLOG_PROVIDED
#define XID_ALLOCATION_ALLOW_ERROR_WAIT true
#define main clog_allocator_original_main
#include "test_cluster_xid_allocation.c"
#undef main
#include "access/slru.h"
#include "access/clog_file.h"
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_wal_thread.h"
#define CLOG_CREATION_HAVE_POSTMASTER_ENV
#include "test_cluster_clog_creation_support.h"
#undef CLOG_CREATION_HAVE_POSTMASTER_ENV

volatile uint32 CritSectionCount;
static bool cf, cf_available, cf_release_ok, release_ok, post_error, bad_identity, change_on_wait;
static bool known[4096], available[4096];
static uint64 table_generation, borrowed, startup_generation, counter_advance;
static unsigned control_calls, cf_locks, cf_unlocks, acquired_files, returned_files;
static unsigned reserved_files, destroyed_ranges, current_checks, stale_at;
static unsigned fail_borrow_at;
static ClusterControlNativeResultV2 control_result;
static ClusterControlNativeModeV2 mode;
static uint64 asked_first[2], asked_last[2];

static ClusterRecoveryFileIdentityV2
file_identity(uint64 segment)
{
	ClusterRecoveryFileIdentityV2 value = {0};
	value.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	value.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	value.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	value.system_identifier = 77; value.database_incarnation = 1;
	value.storage_uuid[0] = 1; value.redo_branch_uuid[0] = 2;
	value.redo_branch_generation = 3; value.redo_timeline = 4;
	value.origin = mode == CLUSTER_NATIVE_FILES_CREATION_V2
		? creation_context.boot_files[0].origin : cluster_node_id;
	value.object_id = segment;
	value.creation_generation = 7; value.registration_generation = 9;
	return value;
}
ClusterControlNativeModeV2 cluster_control_native_files_mode_v2(void) { return mode; }
bool cluster_cf_held(LOCKMODE lockmode) { return cf; }
bool cluster_cf_lock(LOCKMODE lockmode)
{
	UT_ASSERT(!held && !cf); UT_ASSERT_EQ(lockmode, ExclusiveLock);
	cf_locks++; cf = cf_available; return cf;
}
ClusterCfReleaseResult cluster_cf_unlock_confirmed(LOCKMODE lockmode)
{
	UT_ASSERT(cf && !held); cf_unlocks++;
	if (!cf_release_ok) return CLUSTER_CF_RELEASE_UNCONFIRMED;
	cf = false; return CLUSTER_CF_RELEASE_CONFIRMED;
}
ClusterControlNativeResultV2
cluster_control_native_files_clog_range_v2(uint16 origin, uint64 first, uint64 last)
{
	UT_ASSERT_EQ(origin, cluster_node_id); UT_ASSERT(first <= last && last < 4096);
	if (mode != CLUSTER_NATIVE_FILES_MANAGED_V2) return CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2;
	for (uint64 i = first; i <= last; i++)
		if (!known[i]) return CLUSTER_NATIVE_FILES_NEEDS_REGISTRATION_V2;
	return CLUSTER_NATIVE_FILES_OK_V2;
}
ClusterControlNativeResultV2
cluster_control_native_files_reserve_clog_range_locked_v2(uint16 origin, uint64 first,
	uint64 last, ClusterControlNativeClogRangeV2 **out)
{
	ClusterControlNativeClogRangeV2 *range;
	UT_ASSERT(cf && !held && *out == NULL); UT_ASSERT_EQ(borrowed, 0);
	UT_ASSERT_EQ(origin, cluster_node_id); UT_ASSERT(first <= last && last < 4096);
	UT_ASSERT(control_calls < 2);
	asked_first[control_calls] = first; asked_last[control_calls++] = last;
	if (control_result != CLUSTER_NATIVE_FILES_OK_V2) return control_result;
	for (uint64 i = first; i <= last; i++) known[i] = true;
	table_generation++;
	if (post_error) ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("post reservation")));
	range = palloc0(offsetof(ClusterControlNativeClogRangeV2, entries)
		+ (last - first + 1) * sizeof(range->entries[0]));
	range->first_segment = first; range->last_segment = last; range->count = last - first + 1;
	range->table_generation = table_generation; range->startup_generation = startup_generation;
	for (uint64 i = 0; i < range->count; i++) {
		range->entries[i].expected = file_identity(first + i);
		range->entries[i].batch_uuid[0] = 42;
		range->entries[i].reserved = !available[first + i];
	}
	*out = range;
	if (counter_advance) { variables.nextXid = FullTransactionIdFromU64(counter_advance); counter_advance = 0; }
	return CLUSTER_NATIVE_FILES_OK_V2;
}
void cluster_control_native_clog_range_destroy_v2(ClusterControlNativeClogRangeV2 **range)
{
	if (*range) { destroyed_ranges++; pfree(*range); *range = NULL; }
}
static bool
borrow_file(uint64 segment, ClusterControlNativeFileBorrowV2 *out)
{
	if (fail_borrow_at && acquired_files + 1 == fail_borrow_at) return false;
	UT_ASSERT_EQ(out->private_self, 0);
	out->expected = file_identity(segment);
	out->private_self = (uintptr_t)out; out->private_pid = 123;
	out->table_generation = table_generation; out->startup_generation = startup_generation;
	borrowed++; acquired_files++; return true;
}
bool cluster_control_native_file_acquire_v2(const ClusterControlFileKeyV2 *key,
	ClusterControlNativeFileBorrowV2 *out)
{
	UT_ASSERT(held || cf); UT_ASSERT_EQ(key->kind, CLUSTER_RECOVERY_FILE_CLOG_V2);
	UT_ASSERT_EQ(key->origin, mode == CLUSTER_NATIVE_FILES_CREATION_V2
		? creation_context.boot_files[0].origin : cluster_node_id);
	UT_ASSERT(key->object_id < 4096);
	if (!known[key->object_id] || !available[key->object_id]) return false;
	return borrow_file(key->object_id, out);
}
ClusterControlNativeResultV2
cluster_control_native_file_reserved_acquire_locked_v2(const ClusterSharedConfigRef *config,
	Size budget, uint64 startup, const uint8 batch[16], const ClusterRecoveryFileIdentityV2 *expected,
	ClusterControlNativeFileBorrowV2 *out)
{
	UT_ASSERT(cf && !held); UT_ASSERT_EQ(budget, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2);
	UT_ASSERT_EQ(startup, startup_generation); UT_ASSERT_EQ(batch[0], 42);
	if (!borrow_file(expected->object_id, out)) return CLUSTER_NATIVE_FILES_REFUSED_V2;
	reserved_files++;
	if (bad_identity) out->expected.creation_generation++;
	return CLUSTER_NATIVE_FILES_OK_V2;
}
bool cluster_control_native_file_current_v2(const ClusterControlNativeFileBorrowV2 *file)
{
	UT_ASSERT_EQ(file->private_self, (uintptr_t)file);
	current_checks++;
	return file->table_generation == table_generation && (!stale_at || current_checks != stale_at);
}
bool cluster_control_native_file_release_v2(ClusterControlNativeFileBorrowV2 *file)
{
	UT_ASSERT_EQ(file->private_self, (uintptr_t)file); UT_ASSERT(borrowed > 0);
	if (!release_ok) return false;
	borrowed--; returned_files++; memset(file, 0, sizeof(*file)); return true;
}
static void observe_extension(void)
{
	if (CLOGAllocationRequired()) { UT_ASSERT(borrowed > 0); UT_ASSERT(!cf); }
}
static void observe_wait(void)
{
	if (change_on_wait) floor_full = UINT64CONST(5) * 1048576 + 4;
}

#include "test_cluster_clog_allocation_prepare.inc"

static void reset_native(void)
{
	reset_allocator(); reservation = true; limit_full = 8196;
	creation_support_reset();
	memset(known, 0, sizeof(known)); memset(available, 0, sizeof(available));
	memset(asked_first, 0, sizeof(asked_first)); memset(asked_last, 0, sizeof(asked_last));
	cf = post_error = bad_identity = change_on_wait = false;
	cf_available = cf_release_ok = release_ok = true;
	table_generation = 5; startup_generation = 7; borrowed = counter_advance = 0;
	control_calls = cf_locks = cf_unlocks = acquired_files = returned_files = 0;
	reserved_files = destroyed_ranges = current_checks = stale_at = fail_borrow_at = 0;
	control_result = CLUSTER_NATIVE_FILES_OK_V2; mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
	PG_exception_stack = NULL;
}
static void expect_native_error(void)
{
	int caught;
	allocation_error_expected = true;
	caught = sigsetjmp(allocation_error_env, 1);
	if (!caught) (void)GetNewTransactionId(false);
	allocation_error_expected = false;
	UT_ASSERT_EQ(caught, 1); UT_ASSERT_EQ(allocation_errors, 1);
	UT_ASSERT_EQ(extensions, 0); UT_ASSERT_EQ(entries[0].xid, InvalidTransactionId);
	UT_ASSERT_EQ(visible_xids[0], InvalidTransactionId);
}
UT_TEST(actual_cold_preparation_retains_gap_borrow_through_issue)
{
	reset_native();
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2052);
	UT_ASSERT_EQ(control_calls, 1); UT_ASSERT_EQ(asked_first[0], 0); UT_ASSERT_EQ(asked_last[0], 2);
	UT_ASSERT_EQ(cf_locks, 1); UT_ASSERT_EQ(cf_unlocks, 1);
	UT_ASSERT_EQ(reserved_files, 1); UT_ASSERT_EQ(returned_files, 1); UT_ASSERT_EQ(borrowed, 0);
	UT_ASSERT_EQ(destroyed_ranges, 1); UT_ASSERT_EQ(lock_errors, 0);
}
UT_TEST(actual_warm_available_gap_never_takes_cf)
{
	reset_native(); known[0] = known[1] = known[2] = available[0] = true;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2052);
	UT_ASSERT_EQ(cf_locks + control_calls + reserved_files, 0);
	UT_ASSERT_EQ(acquired_files, 1); UT_ASSERT_EQ(returned_files, 1); UT_ASSERT_EQ(borrowed, 0);
}
UT_TEST(actual_full_gap_is_not_a_64_file_prefix)
{
	ClusterClogAllocationV2 *state = NULL;
	FullTransactionId first = FullTransactionIdFromU64(2052);
	FullTransactionId last = FullTransactionIdFromU64(UINT64CONST(73) * 1048576 + 4);
	reset_native(); CLOGPrepareForAllocation(first, last, &state);
	UT_ASSERT_EQ(asked_first[0], 0); UT_ASSERT_EQ(asked_last[0], 75);
	UT_ASSERT_EQ(reserved_files, 74); UT_ASSERT_EQ(borrowed, 74);
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	UT_ASSERT(CLOGAllocationRangeReady(first, last, &state));
	LWLockRelease(XidGenLock); CLOGFinishAllocation(&state);
	UT_ASSERT_EQ(returned_files, 74); UT_ASSERT_EQ(borrowed, 0); UT_ASSERT(state == NULL);
}
UT_TEST(actual_wrap_reserves_both_halves_before_any_borrow)
{
	reset_native(); variables.nextXid = FullTransactionIdFromEpochAndXid(7, UINT32_MAX);
	floor_full = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(8, 4));
	limit_full = floor_full + 1024;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), floor_full);
	UT_ASSERT_EQ(control_calls, 2); UT_ASSERT_EQ(asked_first[0], 4095); UT_ASSERT_EQ(asked_last[0], 4095);
	UT_ASSERT_EQ(asked_first[1], 0); UT_ASSERT_EQ(asked_last[1], 2);
	UT_ASSERT_EQ(acquired_files, 2); UT_ASSERT_EQ(returned_files, 2); UT_ASSERT_EQ(borrowed, 0);
	UT_ASSERT_EQ(lock_errors, 0);
}
UT_TEST(actual_refusal_and_post_reserve_error_never_wait_or_issue)
{
	for (unsigned variant = 0; variant < 3; variant++) {
		reset_native();
		if (!variant) control_result = CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2;
		if (variant == 1) control_result = CLUSTER_NATIVE_FILES_REGISTRATION_DEBT_V2;
		post_error = variant == 2;
		expect_native_error(); UT_ASSERT_EQ(control_calls, 1); UT_ASSERT_EQ(waits, 0);
		UT_ASSERT(!cf); UT_ASSERT_EQ(borrowed, 0);
		UT_ASSERT_EQ(known[0], variant == 2);
	}
}
UT_TEST(actual_counter_is_rederived_after_control_preparation)
{
	reset_native(); counter_advance = 2084;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2084);
	UT_ASSERT_EQ(control_calls, 1); UT_ASSERT_EQ(returned_files, 1); UT_ASSERT_EQ(lock_errors, 0);
}
UT_TEST(actual_lease_gap_expansion_does_not_retry_control)
{
	reset_native(); reservation = false; change_on_wait = true;
	expect_native_error(); UT_ASSERT_EQ(waits, 1); UT_ASSERT_EQ(control_calls, 1);
	UT_ASSERT_EQ(borrowed, 0); UT_ASSERT(!cf);
}
UT_TEST(actual_final_drift_and_exact_identity_refuse_storage)
{
	for (unsigned variant = 0; variant < 2; variant++) {
		reset_native();
		if (!variant) { known[0] = known[1] = known[2] = available[0] = true; stale_at = 2; }
		else bad_identity = true;
		expect_native_error(); UT_ASSERT_EQ(borrowed, 0); UT_ASSERT_EQ(returned_files, 1);
		UT_ASSERT_EQ(waits, 0); UT_ASSERT(!cf);
	}
}
UT_TEST(actual_partial_borrow_error_returns_only_its_prefix)
{
	reset_native(); floor_full = UINT64CONST(2) * 1048576 + 4; limit_full = floor_full + 1024;
	fail_borrow_at = 2; expect_native_error();
	UT_ASSERT_EQ(acquired_files, 1); UT_ASSERT_EQ(returned_files, 1); UT_ASSERT_EQ(borrowed, 0);
	UT_ASSERT(known[0] && known[4]); UT_ASSERT_EQ(control_calls, 1); UT_ASSERT(!cf);
}
UT_TEST(actual_cleanup_refusal_keeps_unreturned_duty)
{
	reset_native(); release_ok = false;
	allocation_error_expected = true;
	if (!sigsetjmp(allocation_error_env, 1)) (void)GetNewTransactionId(false);
	allocation_error_expected = false;
	UT_ASSERT_EQ(allocation_errors, 1); UT_ASSERT_EQ(returned_files, 0); UT_ASSERT_EQ(borrowed, 1);
	reset_native(); cf_release_ok = false; expect_native_error();
	UT_ASSERT(cf); UT_ASSERT_EQ(borrowed, 0); UT_ASSERT_EQ(control_calls, 1);
}
UT_TEST(actual_managed_guc_off_still_prepares_and_legacy_stays_raw)
{
	reset_native(); cluster_enabled = false;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2052);
	UT_ASSERT_EQ(control_calls, 1); UT_ASSERT_EQ(returned_files, 1);
	reset_native(); cluster_enabled = cluster_shared_catalog = cluster_xid_striping = false;
	mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
	UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2052);
	UT_ASSERT_EQ(control_calls + acquired_files + cf_locks, 0);
}

static void
select_creation(void)
{
	ClusterRecoveryFileIdentityV2 file = file_identity(0);
	creation_support_select(&file, table_generation, startup_generation);
	cluster_enabled = true;
	mode = CLUSTER_NATIVE_FILES_CREATION_V2;
	known[0] = available[0] = true;
}

UT_TEST(actual_creation_original_allocator_borrows_registered_boot_page)
{
	ClusterControlNativeFileBorrowV2 sample = {0};
	int caught;
	reset_native(); select_creation();
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	UT_ASSERT(CLOGNativeFileAcquire(0, &sample));
	if (sample.private_self) UT_ASSERT(cluster_control_native_file_release_v2(&sample));
	LWLockRelease(XidGenLock);
	allocation_error_expected = true;
	caught = sigsetjmp(allocation_error_env, 1);
	if (!caught) UT_ASSERT_EQ(U64FromFullTransactionId(GetNewTransactionId(false)), 2052);
	allocation_error_expected = false;
	UT_ASSERT_EQ(caught, 0);
	UT_ASSERT_EQ(control_calls + cf_locks + reserved_files, 0);
	UT_ASSERT_EQ(acquired_files, 2); UT_ASSERT_EQ(returned_files, 2);
	UT_ASSERT_EQ(borrowed, 0); UT_ASSERT_EQ(cluster_node_id, -1);
	UT_ASSERT(cluster_enabled);
}

UT_TEST(actual_creation_allocator_refuses_outside_born_page_before_effects)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		reset_native(); select_creation();
		if (fault == 0) variables.nextXid = FullTransactionIdFromU64(32768);
		if (fault == 1) variables.nextXid = FullTransactionIdFromU64(UINT64CONST(4294969348));
		if (fault == 2) creation_has_request = false;
		if (fault == 3) creation_info.table_generation++;
		if (fault == 4) creation_info.creation.creator_pid++;
		if (fault == 5) available[0] = false;
		expect_native_error();
		UT_ASSERT_EQ(control_calls + cf_locks + reserved_files, 0);
		UT_ASSERT_EQ(borrowed, 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid),
			fault == 0 ? 32768 : fault == 1 ? UINT64CONST(4294969348) : 2052);
	}
}
int main(void)
{
	UT_PLAN(13);
	UT_RUN(actual_cold_preparation_retains_gap_borrow_through_issue);
	UT_RUN(actual_warm_available_gap_never_takes_cf);
	UT_RUN(actual_full_gap_is_not_a_64_file_prefix);
	UT_RUN(actual_wrap_reserves_both_halves_before_any_borrow);
	UT_RUN(actual_refusal_and_post_reserve_error_never_wait_or_issue);
	UT_RUN(actual_counter_is_rederived_after_control_preparation);
	UT_RUN(actual_lease_gap_expansion_does_not_retry_control);
	UT_RUN(actual_final_drift_and_exact_identity_refuse_storage);
	UT_RUN(actual_partial_borrow_error_returns_only_its_prefix);
	UT_RUN(actual_cleanup_refusal_keeps_unreturned_duty);
	UT_RUN(actual_managed_guc_off_still_prepares_and_legacy_stays_raw);
	UT_RUN(actual_creation_original_allocator_borrows_registered_boot_page);
	UT_RUN(actual_creation_allocator_refuses_outside_born_page_before_effects);
	UT_DONE(); return ut_failed_count != 0;
}
