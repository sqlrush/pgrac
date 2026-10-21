/* Real freeze-boundary consumer with original stripe widening arithmetic.
 * Author: SqlRush <sqlrush@gmail.com> */
int heap_horizon_stripe_fixture_main(void);
#define main heap_horizon_stripe_fixture_main
#include "test_cluster_xid_stripe.c"
#undef main
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_undo_horizon.h"
#include "access/multixact.h"
#include "utils/rel.h"

TransactionId RecentXmin;
MultiXactId
GetOldestMultiXactId(void)
{
	return 42;
}
/* Execute the original metadata prefix, stopping at physical file creation. */
#include "test_cluster_heap_horizon_create.inc"

bool cluster_shared_config;
static bool activation_available;
static uint64 activation_floor, activation_epoch, activation_generation;
static unsigned activation_reads;

bool
cluster_xid_stripe_get_activation(uint64 *floor, uint64 *epoch, uint64 *generation)
{
	activation_reads++;
	if (!activation_available)
		return false;
	*floor = activation_floor;
	*epoch = activation_epoch;
	*generation = activation_generation;
	return true;
}

static void
setup_horizon(void)
{
	cluster_enabled = cluster_shared_config = cluster_xid_striping = true;
	activation_available = true;
	activation_floor = 4195104;
	activation_epoch = activation_generation = 1;
	activation_reads = 0;
	stub_next_full = FullTransactionIdFromU64(4195121);
}

#include "test_cluster_heap_horizon_vacuum_fixture.inc"

UT_TEST(legal_age_requests_scan_when_local_next_lags)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	UT_ASSERT(vacuum_age_scan(40, 500, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(age_sample_reads, 1);
	UT_ASSERT_EQ(age_fence_reads, 1);
	UT_ASSERT_EQ(age_lock_acquires, 1);
	UT_ASSERT_EQ(age_lock_releases, 1);
	UT_ASSERT(!age_xid_lock);
	/* Scanning must not itself raise tuple freeze or MX cutoffs. */
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 80);
	UT_ASSERT_EQ(cutoffs.MultiXactCutoff, 42);
}

UT_TEST(legal_age_threshold_is_inclusive_without_raw_underflow)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	UT_ASSERT(vacuum_age_scan(40, 960, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT(!vacuum_age_scan(40, 961, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT(!vacuum_age_scan(40, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(age_sample_reads, 3);
}

UT_TEST(legal_age_crosses_epoch_with_full_xid_difference)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	activation_floor = UINT64_C(0x100000000);
	stub_next_full = fxid(1, 100);
	age_cache.nextXid = stub_next_full;
	age_future = fxid(1, 1000);
	UT_ASSERT(vacuum_age_scan(UINT32_C(0xFFFFFFF0), 1016, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT(!vacuum_age_scan(UINT32_C(0xFFFFFFF0), 1017, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
}

UT_TEST(missing_age_promise_requests_scan_without_freeze_permission)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	age_promise_ready = false;
	UT_ASSERT(vacuum_age_scan(40, 500, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(age_sample_reads, 1);
	UT_ASSERT_EQ(age_lock_acquires, age_lock_releases);
	UT_ASSERT_EQ(cutoffs.FreezeLimit, 80);
}

UT_TEST(age_promise_revoked_after_unlock_requests_scan)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	age_revoke_on_release = true;
	UT_ASSERT(vacuum_age_scan(40, 1000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT_EQ(age_fence_reads, 1);
	UT_ASSERT_EQ(age_lock_acquires, age_lock_releases);
	UT_ASSERT(!age_xid_lock);
}

UT_TEST(legal_age_respects_native_default_and_safety_clamp)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	UT_ASSERT(vacuum_age_scan(40, -1, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	autovacuum_freeze_max_age = 1000;
	UT_ASSERT(vacuum_age_scan(40, 100000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
}

UT_TEST(nonshared_and_temporary_age_stay_native)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	cluster_shared_config = false;
	UT_ASSERT(!vacuum_age_scan(40, 500, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	UT_ASSERT(vacuum_age_scan(40, 100, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	cluster_shared_config = true;
	UT_ASSERT(!vacuum_age_scan(40, 500, RELPERSISTENCE_TEMP, 90, &cutoffs));
	UT_ASSERT(vacuum_age_scan(40, 100, RELPERSISTENCE_TEMP, 90, &cutoffs));
	UT_ASSERT_EQ(age_sample_reads, 0);
	UT_ASSERT_EQ(age_lock_acquires, 0);
}

UT_TEST(young_legal_xid_keeps_independent_multixact_scan_trigger)
{
	struct VacuumCutoffs cutoffs;
	setup_age();
	UT_ASSERT(!vacuum_age_scan(40, 1000, RELPERSISTENCE_PERMANENT, 90, &cutoffs));
	vacuum_multixact_freeze_table_age = 50;
	UT_ASSERT(vacuum_age_scan(40, 1000, RELPERSISTENCE_PERMANENT, 50, &cutoffs));
	vacuum_multixact_freeze_table_age = 10000;
}

UT_TEST(slower_writer_can_issue_below_local_freeze_proposal)
{
	TransactionId cutoff = InvalidTransactionId;
	setup_horizon();
	UT_ASSERT(cluster_heap_freeze_cutoff_v1(4195120, &cutoff));
	UT_ASSERT_EQ(cutoff, 4195104);
	UT_ASSERT(cutoff <= 4195105);
	UT_ASSERT_EQ(activation_reads, 1);
}

UT_TEST(older_local_cutoff_is_never_advanced)
{
	TransactionId cutoff;
	setup_horizon();
	UT_ASSERT(cluster_heap_freeze_cutoff_v1(793, &cutoff));
	UT_ASSERT_EQ(cutoff, 793);
	/* Before this idle member allocates its first striped XID. */
	stub_next_full = FullTransactionIdFromU64(794);
	UT_ASSERT(cluster_heap_freeze_cutoff_v1(793, &cutoff));
	UT_ASSERT_EQ(cutoff, 793);
}

UT_TEST(missing_or_invalid_authority_preserves_output)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		TransactionId cutoff = 1234;
		setup_horizon();
		if (fault == 0)
			activation_available = false;
		if (fault == 1)
			activation_floor = 0;
		if (fault == 2)
			activation_epoch = 0;
		if (fault == 3)
			activation_generation = 0;
		if (fault == 4)
			cluster_xid_striping = false;
		if (fault == 5)
			stub_next_full = InvalidFullTransactionId;
		UT_ASSERT(
			!cluster_heap_freeze_cutoff_v1(fault == 6 ? FrozenTransactionId : 4195120, &cutoff));
		UT_ASSERT_EQ(cutoff, 1234);
	}
	UT_ASSERT(!cluster_heap_freeze_cutoff_v1(4195120, NULL));
}

UT_TEST(full_xid_window_cannot_alias_old_activation)
{
	TransactionId cutoff = 1234;
	setup_horizon();
	stub_next_full = FullTransactionIdFromU64(activation_floor + UINT64_C(0x80000000));
	UT_ASSERT(
		!cluster_heap_freeze_cutoff_v1((uint32)U64FromFullTransactionId(stub_next_full), &cutoff));
	UT_ASSERT_EQ(cutoff, 1234);
	activation_floor = U64FromFullTransactionId(stub_next_full) + UINT64_C(0x80000000);
	UT_ASSERT(
		!cluster_heap_freeze_cutoff_v1((uint32)U64FromFullTransactionId(stub_next_full), &cutoff));
	UT_ASSERT_EQ(cutoff, 1234);
}

UT_TEST(wrap_crossing_uses_full_identity_and_skips_special_floor)
{
	TransactionId cutoff;
	setup_horizon();
	activation_floor = UINT64_C(0x100000000);
	stub_next_full = FullTransactionIdFromU64(activation_floor + 100);
	UT_ASSERT(cluster_heap_freeze_cutoff_v1(80, &cutoff));
	UT_ASSERT_EQ(cutoff, FirstNormalTransactionId);
	UT_ASSERT(cluster_heap_freeze_cutoff_v1(UINT32_C(0xFFFFFFF0), &cutoff));
	UT_ASSERT_EQ(cutoff, UINT32_C(0xFFFFFFF0));
}

UT_TEST(nonshared_mode_preserves_native_without_activation_read)
{
	TransactionId cutoff;
	setup_horizon();
	cluster_shared_config = false;
	activation_available = false;
	UT_ASSERT(cluster_heap_freeze_cutoff_v1(4195120, &cutoff));
	UT_ASSERT_EQ(cutoff, 4195120);
	UT_ASSERT_EQ(activation_reads, 0);
}

UT_TEST(new_permanent_relation_is_not_exempt_as_transaction_local)
{
	RelationData rel = { 0 };
	FormData_pg_class form = { 0 };
	TransactionId frozen;
	MultiXactId multi;
	setup_horizon();
	RecentXmin = 4195120;
	rel.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	rel.rd_createSubid = 1;
	UT_ASSERT(RELATION_IS_LOCAL(&rel));
	heapam_relation_set_new_filelocator(&rel, NULL, RELPERSISTENCE_PERMANENT, &frozen, &multi);
	UT_ASSERT_EQ(frozen, 4195104);
	UT_ASSERT_EQ(multi, 42);
	activation_reads = 0;
	heapam_relation_set_new_filelocator(&rel, NULL, RELPERSISTENCE_TEMP, &frozen, &multi);
	UT_ASSERT_EQ(frozen, RecentXmin);
	UT_ASSERT_EQ(activation_reads, 0);
}

static ClusterHeapFreezeReportV2
make_freeze_report_v2(void)
{
	ClusterHeapFreezeReportV2 report = {0};

	report.version = CLUSTER_HEAP_FREEZE_REPORT_V2_VERSION;
	report.length = CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES;
	report.sysid = UINT64CONST(0x0102030405060708);
	report.record_generation = UINT64CONST(0x1112131415161718);
	report.formation_epoch = UINT64CONST(0x2122232425262728);
	report.incarnation = UINT64CONST(0x3132333435363738);
	report.report_sequence = UINT64CONST(0x4142434445464748);
	report.future_full_xid = UINT64CONST(0x6162636465666768);
	report.active_full_xid_min = UINT64CONST(0x5152535455565758);
	report.horizon_scn = UINT64CONST(0x7172737475767778);
	report.requested_full_xid = UINT64CONST(0x8182838485868788);
	report.origin = 0xA1B2C3D4;
	report.sender_interval_ms = 1234;
	report.f_sample_id = UINT64CONST(0x9192939495969798);
	report.h_sample_clock_scn = UINT64CONST(0xA1A2A3A4A5A6A7A8);
	report.h_contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_PREPARED;
	report.h_contributor_token = UINT64CONST(0xB1B2B3B4B5B6B7B8);
	return report;
}

static void
assert_freeze_report_v2_equal(const ClusterHeapFreezeReportV2 *left,
							  const ClusterHeapFreezeReportV2 *right)
{
	UT_ASSERT_EQ(left->version, right->version);
	UT_ASSERT_EQ(left->length, right->length);
	UT_ASSERT_EQ(left->flags, right->flags);
	UT_ASSERT_EQ(left->sysid, right->sysid);
	UT_ASSERT_EQ(left->record_generation, right->record_generation);
	UT_ASSERT_EQ(left->formation_epoch, right->formation_epoch);
	UT_ASSERT_EQ(left->incarnation, right->incarnation);
	UT_ASSERT_EQ(left->report_sequence, right->report_sequence);
	UT_ASSERT_EQ(left->future_full_xid, right->future_full_xid);
	UT_ASSERT_EQ(left->active_full_xid_min, right->active_full_xid_min);
	UT_ASSERT_EQ(left->horizon_scn, right->horizon_scn);
	UT_ASSERT_EQ(left->requested_full_xid, right->requested_full_xid);
	UT_ASSERT_EQ(left->origin, right->origin);
	UT_ASSERT_EQ(left->sender_interval_ms, right->sender_interval_ms);
	UT_ASSERT_EQ(left->f_sample_id, right->f_sample_id);
	UT_ASSERT_EQ(left->h_sample_clock_scn, right->h_sample_clock_scn);
	UT_ASSERT_EQ(left->h_contributor_kind, right->h_contributor_kind);
	UT_ASSERT_EQ(left->reserved0, right->reserved0);
	UT_ASSERT_EQ(left->h_contributor_token, right->h_contributor_token);
}

UT_TEST(freeze_report_v2_roundtrips_exact_little_endian_layout)
{
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	ClusterHeapFreezeReportV2 output = {0};
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	memset(bytes, 0xA5, sizeof(bytes));
	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_VERSION], 2);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_VERSION + 1], 0);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_LENGTH], 120);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_LENGTH + 1], 0);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID], 0x08);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 7], 0x01);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_ORIGIN], 0xD4);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_ORIGIN + 3], 0xA1);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_KIND],
					CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_PREPARED);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_TOKEN], 0xB8);
	UT_ASSERT_EQ(bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_TOKEN + 7], 0xB1);
	UT_ASSERT(cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));
	assert_freeze_report_v2_equal(&input, &output);
}

UT_TEST(freeze_report_v2_rejects_bad_header_reserved_and_interval)
{
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	ClusterHeapFreezeReportV2 output;
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	output = input;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_VERSION]++;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));
	assert_freeze_report_v2_equal(&input, &output);

	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_LENGTH]--;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));

	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FLAGS] = 1;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));

	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_RESERVED0] = 1;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));

	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SENDER_INTERVAL_MS] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SENDER_INTERVAL_MS + 1] = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes) - 1, &output));
}

UT_TEST(freeze_report_v2_rejects_unknown_contributor_kind)
{
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	ClusterHeapFreezeReportV2 output;
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	input.h_contributor_kind = 5;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_KIND] = 5;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));
}

UT_TEST(freeze_report_v2_rejects_missing_identity_and_sample)
{
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	input.sysid = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.record_generation = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.formation_epoch = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.incarnation = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.report_sequence = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.f_sample_id = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.horizon_scn = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.h_sample_clock_scn = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.horizon_scn = CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED;
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
}

UT_TEST(freeze_report_v2_rejects_invalid_full_xid_window)
{
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	input.future_full_xid = UINT64CONST(0x6162636400000000);
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.active_full_xid_min = UINT64CONST(0x5152535400000000);
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.active_full_xid_min = UINT64CONST(0x6162636465666769);
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.requested_full_xid = UINT64CONST(0x8182838400000000);
	UT_ASSERT(!cluster_heap_freeze_report_v2_encode(&input, bytes));
	input = make_freeze_report_v2();
	input.requested_full_xid = 0;
	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
}

UT_TEST(freeze_report_v2_decode_rejects_invalid_payload)
{
	ClusterHeapFreezeReportV2 input = make_freeze_report_v2();
	ClusterHeapFreezeReportV2 output = input;
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	UT_ASSERT(cluster_heap_freeze_report_v2_encode(&input, bytes));
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 1] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 2] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 3] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 4] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 5] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 6] = 0;
	bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID + 7] = 0;
	UT_ASSERT(!cluster_heap_freeze_report_v2_decode(bytes, sizeof(bytes), &output));
	assert_freeze_report_v2_equal(&input, &output);
}

int
main(void)
{
	UT_PLAN(21);
	UT_RUN(legal_age_requests_scan_when_local_next_lags);
	UT_RUN(legal_age_threshold_is_inclusive_without_raw_underflow);
	UT_RUN(legal_age_crosses_epoch_with_full_xid_difference);
	UT_RUN(missing_age_promise_requests_scan_without_freeze_permission);
	UT_RUN(age_promise_revoked_after_unlock_requests_scan);
	UT_RUN(legal_age_respects_native_default_and_safety_clamp);
	UT_RUN(nonshared_and_temporary_age_stay_native);
	UT_RUN(young_legal_xid_keeps_independent_multixact_scan_trigger);
	UT_RUN(slower_writer_can_issue_below_local_freeze_proposal);
	UT_RUN(older_local_cutoff_is_never_advanced);
	UT_RUN(missing_or_invalid_authority_preserves_output);
	UT_RUN(full_xid_window_cannot_alias_old_activation);
	UT_RUN(wrap_crossing_uses_full_identity_and_skips_special_floor);
	UT_RUN(nonshared_mode_preserves_native_without_activation_read);
	UT_RUN(new_permanent_relation_is_not_exempt_as_transaction_local);
	UT_RUN(freeze_report_v2_roundtrips_exact_little_endian_layout);
	UT_RUN(freeze_report_v2_rejects_bad_header_reserved_and_interval);
	UT_RUN(freeze_report_v2_rejects_unknown_contributor_kind);
	UT_RUN(freeze_report_v2_rejects_missing_identity_and_sample);
	UT_RUN(freeze_report_v2_rejects_invalid_full_xid_window);
	UT_RUN(freeze_report_v2_decode_rejects_invalid_payload);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
