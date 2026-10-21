/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_horizon.c
 *	  Unit tests for the undo retention horizon fold and actual report
 *	  receiver, bounded slot sampler and LMON sender.
 *
 *	  Truth table U1-U16: the fold returns OK with the scn_time_cmp-min
 *	  floor only when EVERY required MEMBER peer has a stable, capable,
 *	  current-epoch, well-formed, monotone, fresh report; every unproven
 *	  edge STALLS with first-fail attribution (reason + blame node) and
 *	  never falls back to the local horizon (rule 8.A / Q3'').
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_horizon.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22e-undo-cluster-retention-horizon.md (D5-1, §4.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_undo_horizon.h"
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_undo_cleaner.h"
#include "utils/timestamp.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* Compile the actual receive/store/sender bodies. Connection/semantic
 * publishers, clock and logging are explicit process-boundary fixtures. */
#include "../../backend/cluster/cluster_undo_horizon_ic.c"

static ClusterUndoHorizonShmem test_horizon_store;
bool cluster_enabled = true;
int cluster_node_id = 0;
int MyProcPid = 11;
/* The actual spinlock slow path uses backend wait-event storage. */
static uint32 test_wait_event;
uint32 *my_wait_event_info = &test_wait_event;
int cluster_injection_armed_count;
int cluster_lmon_main_loop_interval = 1000;
static TimestampTz test_horizon_now = INT64CONST(10000000);
static unsigned test_horizon_sends;
static bool test_horizon_cut;
static bool test_horizon_seal;
static bool test_horizon_capable = true;
static bool test_v1_capable = true;
static ClusterUndoHorizonWire test_horizon_last;
static ClusterUndoRetentionSample test_retention_sample
	= { .horizon_scn = 500,
		.clock_scn = 500,
		.contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK };
static unsigned test_retention_scalar_calls;
static unsigned test_retention_sample_calls;
static bool test_retention_sample_valid = true;
static bool test_retention_mrp;
static SCN test_retention_mrp_bound;
static ClusterSemanticActiveSampleFence test_v2_fence;
static bool test_v2_identity = true;
static bool test_v2_ingress = true;
static uint32 test_v2_cap_generation = 19;
static ClusterICControlIngress test_v2_source;
static bool test_v2_change_on_recheck;
static uint64 test_v2_stream_serial;
static unsigned test_v2_stream_checks;
static bool test_v2_stream_change_on_recheck;
static bool test_v2_cap_change_on_recheck;
static int32 test_v2_peer = 1;
static ClusterUndoCleanerActiveSample test_sender_sample;
static bool test_sender_sample_valid, test_sender_sample_current = true;
static bool test_sender_change_on_recheck;
static unsigned test_v2_sends;
static ClusterHeapFreezeReportV2 test_v2_sent;
static bool test_cohort_all_caps;
static uint32 test_cohort_generations[CLUSTER_MAX_NODES];
static int32 test_cohort_trigger_peer = -1, test_cohort_changed_peer = -1;
static int test_cohort_change;
static int64 test_cohort_clock_shift;
static unsigned test_maintenance_calls;
static uint64 test_maintenance_requested;
static ClusterSemanticActiveSampleFence test_maintenance_identity;
static ClusterXidStripeSampleFence test_maintenance_stripe;
static ClusterXidStripeMaintenanceResult test_maintenance_result;
static unsigned test_change_admission_on_capture;

/* QVOTEC's original durable entry is a process-boundary double here. Its
 * actual lease/majority-sync/arm behavior has the separate stripe fixture. */
ClusterXidStripeMaintenanceResult
cluster_xid_stripe_request_maintenance(const ClusterSemanticActiveSampleFence *identity,
									   const ClusterXidStripeSampleFence *stripe,
									   FullTransactionId requested)
{
	test_maintenance_calls++;
	test_maintenance_identity = *identity;
	test_maintenance_stripe = *stripe;
	test_maintenance_requested = U64FromFullTransactionId(requested);
	return test_maintenance_result;
}

bool
cluster_undo_cleaner_active_sample(ClusterUndoCleanerActiveSample *out)
{
	if (test_change_admission_on_capture != 0 && --test_change_admission_on_capture == 0)
		test_v2_fence.admission_publication_seq += 2;
	MemSet(out, 0, sizeof(*out));
	if (!test_sender_sample_valid)
		return false;
	*out = test_sender_sample;
	return true;
}

bool
cluster_undo_cleaner_active_sample_current(const ClusterUndoCleanerActiveSample *expected)
{
	if (test_sender_change_on_recheck)
		test_sender_sample.sample_id++;
	return test_sender_sample_current && expected->sample_id == test_sender_sample.sample_id
		   && cluster_semantic_activation_active_sample_current(&expected->admission_fence);
}

bool
cluster_semantic_activation_active_sample_capture(ClusterSemanticActiveSampleFence *out)
{
	*out = test_v2_fence;
	return test_v2_identity;
}

bool
cluster_semantic_activation_active_sample_current(const ClusterSemanticActiveSampleFence *expected)
{
	if (test_v2_change_on_recheck) {
		test_v2_fence.admission_publication_seq += 2;
		test_v2_change_on_recheck = false;
	}
	return test_v2_identity && memcmp(expected, &test_v2_fence, sizeof(*expected)) == 0;
}

bool
cluster_sf_peer_capability_word_sample(int32 peer, uint32 required, uint32 *word,
									   uint32 *generation)
{
	*word = test_horizon_capable ? PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2 : 0;
	*generation = test_cohort_all_caps ? test_cohort_generations[peer] : test_v2_cap_generation;
	return (test_cohort_all_caps || peer == test_v2_peer) && (*word & required) == required;
}

bool
cluster_sf_peer_capability_generation_matches(int32 peer, uint32 required, uint32 generation)
{
	if (test_cohort_all_caps) {
		if (peer == test_cohort_trigger_peer) {
			int32 changed = test_cohort_changed_peer;

			test_cohort_trigger_peer = -1;
			if (test_cohort_change == 1)
				test_cohort_generations[changed]++;
			else if (test_cohort_change == 2)
				pg_atomic_fetch_add_u32(&test_horizon_store.freeze_slots[changed].seq, 2);
			else if (test_cohort_change == 3)
				test_v2_fence.admission_publication_seq += 2;
		}
		return required == PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2 && test_horizon_capable
			   && generation == test_cohort_generations[peer];
	}
	if (test_v2_cap_change_on_recheck) {
		test_v2_cap_generation++;
		test_v2_cap_change_on_recheck = false;
	}
	return peer == test_v2_peer && required == PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2
		   && test_horizon_capable && generation == test_v2_cap_generation;
}

bool
cluster_ic_tier1_stream_current(const ClusterICTier1Stream *stream)
{
	if (++test_v2_stream_checks == 2 && test_v2_stream_change_on_recheck)
		test_v2_stream_serial++;
	return stream != NULL && stream->serial != 0 && stream->serial != UINT64_MAX
		   && stream->serial == test_v2_stream_serial && stream->epoch == 7
		   && stream->owner_pid == MyProcPid && stream->peer == test_v2_peer
		   && stream->plane == CLUSTER_IC_PLANE_CONTROL && stream->channel == -1;
}

bool
cluster_ic_dispatch_control_ingress(const ClusterICEnvelope *env, ClusterICControlIngress *out)
{
	(void)env;
	*out = test_v2_source;
	return test_v2_ingress;
}

bool
cluster_injection_should_skip(const char *name)
{
	(void)name;
	return false;
}

void
cluster_injection_run(const char *name)
{
	(void)name;
}

bool
errstart(int level, const char *domain)
{
	(void)level;
	(void)domain;
	return false;
}

bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}

int
errmsg_internal(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}

void
errfinish(const char *file, int line, const char *func)
{
	(void)file;
	(void)line;
	(void)func;
	abort();
}

TimestampTz
GetCurrentTimestamp(void)
{
	return test_horizon_now;
}
uint64
cluster_epoch_get_current(void)
{
	return 7;
}
SCN
cluster_undo_retention_horizon(void)
{
	test_retention_scalar_calls++;
	return (SCN)500;
}
bool
cluster_undo_retention_horizon_sample(ClusterUndoRetentionSample *out)
{
	test_retention_sample_calls++;
	*out = test_retention_sample;
	test_horizon_now += test_cohort_clock_shift;
	test_cohort_clock_shift = 0;
	return test_retention_sample_valid;
}
bool
cluster_mrp_should_start(void)
{
	return test_retention_mrp;
}
SCN
cluster_mrp_standby_consistent_scn(void)
{
	return test_retention_mrp_bound;
}
bool
cluster_normal_stop_pi_retirement_allowed(void)
{
	return test_horizon_cut;
}
bool
cluster_normal_stop_service_control_sealed(void)
{
	return test_horizon_seal;
}
bool
cluster_sf_peer_supports_undo_horizon(int32 peer_id)
{
	return test_v1_capable && test_horizon_capable && peer_id > 0 && peer_id < 4;
}
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id)
{
	static const ClusterNodeInfo node = { 0 };
	return node_id >= 0 && node_id < 4 ? &node : NULL;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 destination, const void *payload, uint32 length)
{
	Assert(type == PGRAC_IC_MSG_UNDO_HORIZON && destination > 0 && destination < 4);
	if (length == CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES) {
		if (!cluster_heap_freeze_report_v2_decode(payload, length, &test_v2_sent))
			abort();
		test_v2_sends++;
	} else {
		Assert(length == sizeof(test_horizon_last));
		memcpy(&test_horizon_last, payload, length);
		test_horizon_sends++;
	}
	return CLUSTER_IC_SEND_DONE;
}

static ClusterHeapFreezeReportV2
reset_v2_receiver(void)
{
	ClusterHeapFreezeReportV2 r = { 0 };
	int i;

	memset(&test_horizon_store, 0, sizeof(test_horizon_store));
	UndoHorizonShmem = &test_horizon_store;
	SpinLockInit(&test_horizon_store.maintenance_lock);
	pg_atomic_init_u32(&test_horizon_store.freeze_watermark_seq, 0);
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		pg_atomic_init_u32(&test_horizon_store.slots[i].seq, 0);
		pg_atomic_init_u32(&test_horizon_store.freeze_slots[i].seq, 0);
	}
	for (i = 0; i < UNDO_HORIZON_V2_STATUS_COUNT; i++)
		pg_atomic_init_u64(&test_horizon_store.freeze_observation_count[i], 0);
	pg_atomic_init_u64(&test_horizon_store.wire_reject_count, 0);
	memset(&test_v2_fence, 0, sizeof(test_v2_fence));
	test_v2_fence.system_identifier = 71;
	test_v2_fence.record_generation = 8;
	test_v2_fence.formation_epoch = 7;
	test_v2_fence.admission_publication_seq = 2;
	test_v2_fence.membership_cut_generation = 6;
	test_v2_fence.local_boot_incarnation = 40;
	test_v2_fence.local_node_id = 0;
	test_v2_fence.admitted_members_lo = 3;
	test_v2_fence.admitted_incarnation[0] = 40;
	test_v2_fence.admitted_incarnation[1] = 41;
	test_v2_identity = test_v2_ingress = test_horizon_capable = true;
	test_v2_change_on_recheck = false;
	test_v2_stream_serial = 19;
	test_v2_stream_checks = 0;
	test_v2_stream_change_on_recheck = false;
	test_v2_cap_change_on_recheck = false;
	test_v2_peer = 1;
	test_v2_cap_generation = 19;
	test_cohort_all_caps = false;
	test_cohort_trigger_peer = test_cohort_changed_peer = -1;
	test_cohort_change = 0;
	test_cohort_clock_shift = 0;
	test_v2_source = (ClusterICControlIngress){ .serial = 19,
												.epoch = 7,
												.owner_pid = MyProcPid,
												.source_node = 1,
												.carrier = CLUSTER_IC_CONTROL_TCP };
	r.version = CLUSTER_HEAP_FREEZE_REPORT_V2_VERSION;
	r.length = CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES;
	r.sysid = 71;
	r.record_generation = 8;
	r.formation_epoch = 7;
	r.incarnation = 41;
	r.report_sequence = 1;
	r.future_full_xid = UINT64CONST(0x100001000);
	r.active_full_xid_min = UINT64CONST(0x100000100);
	r.horizon_scn = 500;
	r.origin = 1;
	r.sender_interval_ms = 1000;
	r.f_sample_id = 1;
	r.h_sample_clock_scn = 600;
	r.h_contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK;
	return r;
}

static void
receive_horizon_bytes(const void *bytes, uint32 length)
{
	ClusterICEnvelope env = { 0 };

	env.msg_type = PGRAC_IC_MSG_UNDO_HORIZON;
	env.source_node_id = test_v2_peer;
	env.dest_node_id = 0;
	env.epoch = 7;
	env.payload_length = length;
	cluster_undo_horizon_report_handler(&env, bytes);
}

static void
receive_v2_report(const ClusterHeapFreezeReportV2 *report)
{
	uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	if (!cluster_heap_freeze_report_v2_encode(report, bytes))
		abort();
	receive_horizon_bytes(bytes, sizeof(bytes));
}

UT_TEST(test_v2_actual_ingress_accepts_current_report)
{
	ClusterHeapFreezeReportV2 report = reset_v2_receiver();
	ClusterHeapFreezeReportV2View view;

	receive_v2_report(&report);
	UT_ASSERT_EQ(pg_atomic_read_u64(&test_horizon_store.wire_reject_count), 0);
	UT_ASSERT(!test_horizon_store.slots[1].valid);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT_EQ(view.report.active_full_xid_min, report.active_full_xid_min);
	UT_ASSERT_EQ(view.report.h_sample_clock_scn, report.h_sample_clock_scn);
	UT_ASSERT_EQ(view.recv_at_us, test_horizon_now);
	UT_ASSERT(!view.horizon_regression_flagged);
}

UT_TEST(test_v2_replay_does_not_refresh_h_or_rewind_f)
{
	ClusterHeapFreezeReportV2 report = reset_v2_receiver();
	ClusterHeapFreezeReportV2View first, view;

	report.report_sequence = 2;
	receive_v2_report(&report);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &first));
	test_horizon_now += 1000000;
	receive_v2_report(&report);
	report.report_sequence--;
	receive_v2_report(&report);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT_EQ(view.recv_at_us, first.recv_at_us);
	UT_ASSERT_EQ(view.report.report_sequence, 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(&test_horizon_store.wire_reject_count), 2);
}

UT_TEST(test_v2_new_h_can_reuse_old_f_sample)
{
	ClusterHeapFreezeReportV2 report = reset_v2_receiver();
	ClusterHeapFreezeReportV2View view;

	receive_v2_report(&report);
	test_horizon_now += 1000000;
	report.report_sequence++;
	report.horizon_scn++;
	report.h_sample_clock_scn++;
	receive_v2_report(&report);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT_EQ(view.report.f_sample_id, 1);
	UT_ASSERT_EQ(view.report.horizon_scn, report.horizon_scn);
	UT_ASSERT_EQ(view.recv_at_us, test_horizon_now);
}

UT_TEST(test_v2_f_sample_is_immutable_and_monotone)
{
	ClusterHeapFreezeReportV2 initial = reset_v2_receiver();
	ClusterHeapFreezeReportV2View view;
	int i;

	initial.f_sample_id = 2;
	receive_v2_report(&initial);
	for (i = 0; i < 5; i++) {
		ClusterHeapFreezeReportV2 bad = initial;

		bad.report_sequence++;
		if (i == 0)
			bad.future_full_xid++;
		if (i == 1)
			bad.active_full_xid_min++;
		if (i == 2)
			bad.f_sample_id--;
		if (i == 3) {
			bad.f_sample_id++;
			bad.future_full_xid--;
		}
		if (i == 4) {
			bad.f_sample_id++;
			bad.active_full_xid_min--;
		}
		receive_v2_report(&bad);
		UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
		UT_ASSERT_EQ(view.report.report_sequence, initial.report_sequence);
	}
	initial.f_sample_id++;
	initial.report_sequence++;
	initial.future_full_xid++;
	initial.active_full_xid_min++;
	receive_v2_report(&initial);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT_EQ(view.report.f_sample_id, initial.f_sample_id);
	UT_ASSERT_EQ(pg_atomic_read_u64(&test_horizon_store.wire_reject_count), 5);
}

UT_TEST(test_v2_h_regression_blocks_h_without_destroying_f)
{
	ClusterHeapFreezeReportV2 report = reset_v2_receiver();
	ClusterHeapFreezeReportV2View first, view;
	int i;

	receive_v2_report(&report);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &first));
	for (i = 0; i < 2; i++) {
		ClusterHeapFreezeReportV2 bad = report;

		bad.report_sequence += i + 1;
		if (i == 0)
			bad.horizon_scn--;
		else
			bad.h_sample_clock_scn--;
		receive_v2_report(&bad);
		UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
		UT_ASSERT(view.horizon_regression_flagged);
		UT_ASSERT_EQ(view.recv_at_us, first.recv_at_us);
		UT_ASSERT_EQ(view.report.active_full_xid_min, first.report.active_full_xid_min);
	}
	report.report_sequence += 3;
	receive_v2_report(&report);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT(!view.horizon_regression_flagged);
}

UT_TEST(test_v2_reusing_regressed_h_sequence_cannot_clear_latch)
{
	ClusterHeapFreezeReportV2 report = reset_v2_receiver();
	ClusterHeapFreezeReportV2View first, view;

	receive_v2_report(&report);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &first));
	report.report_sequence++;
	report.horizon_scn--;
	receive_v2_report(&report);
	report.horizon_scn++;
	test_horizon_now += 1000000;
	receive_v2_report(&report);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT(view.horizon_regression_flagged);
	UT_ASSERT_EQ(view.recv_at_us, first.recv_at_us);
	report.report_sequence++;
	receive_v2_report(&report);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT(!view.horizon_regression_flagged);
}

UT_TEST(test_v2_invalid_frames_preserve_valid_observation)
{
	ClusterHeapFreezeReportV2 report = reset_v2_receiver();
	ClusterHeapFreezeReportV2View view;
	int i;

	receive_v2_report(&report);
	for (i = 0; i < 6; i++) {
		uint8 bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES + 1] = { 0 };
		uint32 length = CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES;

		if (!cluster_heap_freeze_report_v2_encode(&report, bytes))
			abort();
		if (i == 0)
			length = 88;
		if (i == 1)
			length--;
		if (i == 2)
			length++;
		if (i == 3)
			bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FLAGS] = 1;
		if (i == 4)
			bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_RESERVED0] = 1;
		if (i == 5)
			bytes[CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_VERSION] = 1;
		receive_horizon_bytes(bytes, length);
		UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
		UT_ASSERT_EQ(view.report.report_sequence, report.report_sequence);
	}
	UT_ASSERT_EQ(pg_atomic_read_u64(&test_horizon_store.wire_reject_count), 6);
}

UT_TEST(test_v2_payload_cannot_assert_identity_or_special_xids)
{
	ClusterHeapFreezeReportV2 initial = reset_v2_receiver();
	ClusterHeapFreezeReportV2View view;
	int i;

	receive_v2_report(&initial);
	for (i = 0; i < 9; i++) {
		ClusterHeapFreezeReportV2 bad = initial;

		bad.report_sequence++;
		if (i == 0)
			bad.sysid++;
		if (i == 1)
			bad.record_generation++;
		if (i == 2)
			bad.formation_epoch++;
		if (i == 3)
			bad.incarnation++;
		if (i == 4)
			bad.origin++;
		if (i == 5)
			bad.future_full_xid = UINT64CONST(0x200000001);
		if (i == 6)
			bad.active_full_xid_min = UINT64CONST(0x100000002);
		if (i == 7)
			bad.requested_full_xid = UINT64CONST(0x100000001);
		if (i == 8)
			bad.horizon_scn = bad.h_sample_clock_scn + 1;
		receive_v2_report(&bad);
		UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
		UT_ASSERT_EQ(view.report.report_sequence, initial.report_sequence);
	}
	UT_ASSERT_EQ(pg_atomic_read_u64(&test_horizon_store.wire_reject_count), 9);
}

UT_TEST(test_v2_requires_real_current_control_ingress)
{
	int i;

	for (i = 0; i < 7; i++) {
		ClusterHeapFreezeReportV2 report = reset_v2_receiver();
		ClusterHeapFreezeReportV2View view;

		if (i == 0)
			test_v2_ingress = false;
		if (i == 1)
			test_v2_source.serial++;
		if (i == 2)
			test_v2_source.epoch++;
		if (i == 3)
			test_v2_source.source_node++;
		if (i == 4)
			test_v2_source.owner_pid++;
		if (i == 5)
			test_v2_source.carrier = CLUSTER_IC_CONTROL_UNKNOWN;
		if (i == 6)
			test_horizon_capable = false;
		receive_v2_report(&report);
		UT_ASSERT(!cluster_heap_freeze_report_v2_sample(1, &view));
		UT_ASSERT_EQ(pg_atomic_read_u64(&test_horizon_store.wire_reject_count), 1);
	}
}

UT_TEST(test_v2_control_serial_is_independent_of_hello_generation)
{
	int peer;

	/* The native owner assigns serials across peers; HELLO generations
	 * are per peer. All first connections have generation 1. */
	for (peer = 1; peer <= 3; peer++) {
		ClusterHeapFreezeReportV2 report = reset_v2_receiver();
		ClusterHeapFreezeReportV2View view;

		test_v2_peer = test_v2_source.source_node = peer;
		test_v2_stream_serial = test_v2_source.serial = peer;
		test_v2_cap_generation = 1;
		test_v2_fence.admitted_members_lo = 1 | (UINT64CONST(1) << peer);
		test_v2_fence.admitted_incarnation[peer] = report.incarnation;
		report.origin = peer;
		receive_v2_report(&report);
		UT_ASSERT(cluster_heap_freeze_report_v2_sample(peer, &view));
		UT_ASSERT_EQ(view.report.report_sequence, report.report_sequence);
		UT_ASSERT_EQ(pg_atomic_read_u64(&test_horizon_store.wire_reject_count), 0);
	}
}

UT_TEST(test_v2_identity_and_connection_loss_retire_observation)
{
	ClusterHeapFreezeReportV2 report = reset_v2_receiver();
	ClusterHeapFreezeReportV2View view;

	receive_v2_report(&report);
	test_v2_identity = false;
	UT_ASSERT(!cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT_EQ(view.report.report_sequence, 0);
	test_v2_identity = true;
	test_v2_fence.admission_publication_seq += 2;
	UT_ASSERT(!cluster_heap_freeze_report_v2_sample(1, &view));
	test_v2_fence.admission_publication_seq -= 2;
	test_v2_cap_generation++;
	UT_ASSERT(!cluster_heap_freeze_report_v2_sample(1, &view));
	test_v2_source.serial++;
	test_v2_stream_serial++;
	report.report_sequence++;
	receive_v2_report(&report);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT_EQ(view.report.report_sequence, report.report_sequence);
}

UT_TEST(test_v2_reconnect_does_not_reset_sequence)
{
	ClusterHeapFreezeReportV2 report = reset_v2_receiver();
	ClusterHeapFreezeReportV2View view;

	receive_v2_report(&report);
	test_v2_cap_generation++;
	test_v2_source.serial++;
	test_v2_stream_serial++;
	receive_v2_report(&report);
	UT_ASSERT(!cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT_EQ(pg_atomic_read_u64(&test_horizon_store.wire_reject_count), 1);
	report.report_sequence++;
	receive_v2_report(&report);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
}

UT_TEST(test_v2_identity_change_allows_new_sequence)
{
	ClusterHeapFreezeReportV2 report = reset_v2_receiver();
	ClusterHeapFreezeReportV2View view;

	report.report_sequence = UINT64_MAX;
	report.f_sample_id = UINT64_MAX;
	receive_v2_report(&report);
	report.report_sequence = report.f_sample_id = 1;
	receive_v2_report(&report);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT_EQ(view.report.report_sequence, UINT64_MAX);
	test_v2_fence.admitted_incarnation[1]++;
	report.incarnation++;
	receive_v2_report(&report);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT_EQ(view.report.report_sequence, 1);
}

UT_TEST(test_v2_publication_rechecks_identity)
{
	int i;

	for (i = 0; i < 3; i++) {
		ClusterHeapFreezeReportV2 report = reset_v2_receiver();
		ClusterHeapFreezeReportV2View view;

		test_v2_change_on_recheck = i == 0;
		test_v2_stream_change_on_recheck = i == 1;
		test_v2_cap_change_on_recheck = i == 2;
		receive_v2_report(&report);
		UT_ASSERT(!cluster_heap_freeze_report_v2_sample(1, &view));
		UT_ASSERT_EQ(pg_atomic_read_u64(&test_horizon_store.wire_reject_count), 1);
	}
}

UT_TEST(test_v2_requires_admitted_peer_in_both_bitmap_halves)
{
	int i;

	for (i = 0; i < 2; i++) {
		ClusterHeapFreezeReportV2 report = reset_v2_receiver();
		ClusterHeapFreezeReportV2View view;

		test_v2_peer = test_v2_source.source_node = 63 + i;
		report.origin = test_v2_peer;
		test_v2_fence.admitted_members_lo = 1;
		test_v2_fence.admitted_incarnation[test_v2_peer] = report.incarnation;
		receive_v2_report(&report);
		UT_ASSERT(!cluster_heap_freeze_report_v2_sample(test_v2_peer, &view));
		if (i == 0)
			test_v2_fence.admitted_members_lo |= UINT64CONST(1) << 63;
		else
			test_v2_fence.admitted_members_hi = 1;
		receive_v2_report(&report);
		UT_ASSERT(cluster_heap_freeze_report_v2_sample(test_v2_peer, &view));
	}
}

UT_TEST(test_v2_sampler_is_bounded_and_f_has_no_age_expiry)
{
	ClusterHeapFreezeReportV2 report = reset_v2_receiver();
	ClusterHeapFreezeReportV2View view;

	receive_v2_report(&report);
	test_horizon_now += INT64CONST(9000000000);
	UT_ASSERT(cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT(view.recv_at_us < (uint64)test_horizon_now - 3000000);
	pg_atomic_write_u32(&test_horizon_store.freeze_slots[1].seq, 3);
	memset(&view, 0xff, sizeof(view));
	UT_ASSERT(!cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT_EQ(view.recv_at_us, 0);
	UT_ASSERT_EQ(view.report.report_sequence, 0);
	UT_ASSERT(!cluster_heap_freeze_report_v2_sample(-1, &view));
	UT_ASSERT(!cluster_heap_freeze_report_v2_sample(CLUSTER_MAX_NODES, &view));
	UT_ASSERT(!cluster_heap_freeze_report_v2_sample(0, &view));
	UT_ASSERT(!cluster_heap_freeze_report_v2_sample(1, NULL));
}

UT_TEST(test_original_v1_receiver_remains_separate)
{
	ClusterUndoHorizonWire wire = { .epoch = 7, .horizon_scn = 500, .sender_interval_ms = 1000 };
	ClusterHeapFreezeReportV2View view;

	(void)reset_v2_receiver();
	receive_horizon_bytes(&wire, sizeof(wire));
	UT_ASSERT(test_horizon_store.slots[1].valid);
	UT_ASSERT_EQ(test_horizon_store.slots[1].horizon_scn, 500);
	UT_ASSERT(!cluster_heap_freeze_report_v2_sample(1, &view));
	UT_ASSERT_EQ(pg_atomic_read_u64(&test_horizon_store.wire_reject_count), 0);
}

UT_TEST(test_original_horizon_sender_stops_only_after_all_checkpoint_cut)
{
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_sends, 3);
	UT_ASSERT_EQ(test_horizon_last.horizon_scn, 500);
	UT_ASSERT_EQ(test_horizon_last.epoch, 7);
	test_horizon_cut = true;
	test_horizon_now += 1000001;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_sends, 3);
	UT_ASSERT_EQ(test_horizon_last.horizon_scn, 500);
	test_horizon_cut = false;
	test_horizon_now += 1000001;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_sends, 6);
	test_horizon_seal = true;
	test_horizon_now += 1000001;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_sends, 6);
	test_horizon_seal = false;
	test_horizon_capable = false;
	test_horizon_now += 1000001;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_sends, 6);
}

UT_TEST(test_lmon_uses_one_typed_retention_cut)
{
	unsigned sends = test_horizon_sends;
	unsigned scalar_calls = test_retention_scalar_calls;
	unsigned sample_calls = test_retention_sample_calls;

	test_horizon_capable = true;
	test_retention_sample.horizon_scn = 400;
	test_retention_sample.clock_scn = 700;
	test_retention_sample.contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT;
	test_retention_sample.contributor_token = 9;
	test_horizon_now += 1000001;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_sends, sends + 3);
	UT_ASSERT_EQ(test_horizon_last.horizon_scn, 400);
	UT_ASSERT_EQ(test_retention_scalar_calls, scalar_calls);
	UT_ASSERT_EQ(test_retention_sample_calls, sample_calls + 1);
}

UT_TEST(test_lmon_refuses_missing_typed_retention_sample)
{
	unsigned sends = test_horizon_sends;

	test_retention_sample_valid = false;
	test_horizon_now += 1000001;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_sends, sends);
	test_retention_sample_valid = true;
}

UT_TEST(test_local_report_preserves_same_cut_metadata)
{
	ClusterUndoRetentionSample sample;
	unsigned calls = test_retention_sample_calls;

	UT_ASSERT(undo_horizon_sample_local_report(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 400);
	UT_ASSERT_EQ(sample.clock_scn, 700);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, 9);
	UT_ASSERT_EQ(test_retention_sample_calls, calls + 1);
}

UT_TEST(test_local_report_attributes_mrp_clamp_and_clock_ties)
{
	ClusterUndoRetentionSample sample;

	test_retention_mrp = true;
	test_retention_mrp_bound = 300;
	UT_ASSERT(undo_horizon_sample_local_report(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 300);
	UT_ASSERT_EQ(sample.clock_scn, 700);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION);
	UT_ASSERT_EQ(sample.contributor_token, 0);

	/* A protected snapshot wins an equal-time tie, including different
	 * origin bits; CLOCK/UNKNOWN must not hide a real MRP bound. */
	test_retention_mrp_bound = (UINT64CONST(3) << SCN_NODE_ID_SHIFT) | 400;
	UT_ASSERT(undo_horizon_sample_local_report(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 400);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, 9);
	for (int i = 0; i < 2; i++) {
		test_retention_sample.contributor_kind
			= i == 0 ? CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK
					 : CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_UNKNOWN;
		UT_ASSERT(undo_horizon_sample_local_report(&sample));
		UT_ASSERT_EQ(sample.contributor_kind,
					 CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION);
		UT_ASSERT_EQ(sample.contributor_token, 0);
	}
	test_retention_sample.contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT;
	test_retention_mrp = false;
}

UT_TEST(test_local_report_refusal_clears_metadata_and_stalls_sender)
{
	ClusterUndoRetentionSample sample;
	unsigned sends = test_horizon_sends;

	test_retention_mrp = true;
	test_retention_mrp_bound = InvalidScn;
	MemSet(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!undo_horizon_sample_local_report(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, InvalidScn);
	UT_ASSERT_EQ(sample.clock_scn, InvalidScn);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_UNKNOWN);
	UT_ASSERT_EQ(sample.contributor_token, 0);
	test_horizon_now += 1000001;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_sends, sends);
	test_retention_mrp = false;
}

static void
reset_v2_sender(void)
{
	(void)reset_v2_receiver();
	MemSet(&test_sender_sample, 0, sizeof(test_sender_sample));
	test_sender_sample.scan.status = CTRC_ACTIVE_SCAN_COMPLETE;
	test_sender_sample.scan.reference_full = FullTransactionIdFromEpochAndXid(1, 4096);
	test_sender_sample.scan.future_full = FullTransactionIdFromEpochAndXid(1, 4096);
	test_sender_sample.scan.min_full = FullTransactionIdFromEpochAndXid(1, 256);
	test_sender_sample.admission_fence = test_v2_fence;
	test_sender_sample.sample_id = 17;
	test_sender_sample.worker_pid = 100;
	test_sender_sample_valid = test_sender_sample_current = test_v1_capable = true;
	test_sender_change_on_recheck = false;
	test_horizon_cut = test_horizon_seal = test_retention_mrp = false;
	test_retention_sample_valid = true;
	test_retention_sample
		= (ClusterUndoRetentionSample){ .horizon_scn = 400,
										.clock_scn = 700,
										.contributor_kind
										= CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT,
										.contributor_token = 9 };
	test_v2_sends = 0;
	test_maintenance_calls = 0;
	test_maintenance_requested = 0;
	test_maintenance_result = CLUSTER_XID_STRIPE_MAINTENANCE_PENDING;
	test_change_admission_on_capture = 0;
	test_horizon_now += 1000001;
}

UT_TEST(test_v2_sender_uses_complete_worker_sample_and_same_typed_h_cut)
{
	unsigned v1_sends = test_horizon_sends;
	unsigned cuts = test_retention_sample_calls;

	reset_v2_sender();
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_sends, v1_sends + 3);
	UT_ASSERT_EQ(test_retention_sample_calls, cuts + 1);
	UT_ASSERT_EQ(test_v2_sends, 1);
	UT_ASSERT_EQ(test_v2_sent.sysid, 71);
	UT_ASSERT_EQ(test_v2_sent.record_generation, 8);
	UT_ASSERT_EQ(test_v2_sent.formation_epoch, 7);
	UT_ASSERT_EQ(test_v2_sent.incarnation, 40);
	UT_ASSERT_EQ(test_v2_sent.origin, 0);
	UT_ASSERT_EQ(test_v2_sent.report_sequence, 1);
	UT_ASSERT_EQ(test_v2_sent.f_sample_id, 17);
	UT_ASSERT_EQ(test_v2_sent.future_full_xid,
				 U64FromFullTransactionId(test_sender_sample.scan.future_full));
	UT_ASSERT_EQ(test_v2_sent.active_full_xid_min,
				 U64FromFullTransactionId(test_sender_sample.scan.min_full));
	UT_ASSERT_EQ(test_v2_sent.horizon_scn, 400);
	UT_ASSERT_EQ(test_v2_sent.h_sample_clock_scn, 700);
	UT_ASSERT_EQ(test_v2_sent.h_contributor_kind,
				 CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(test_v2_sent.h_contributor_token, 9);
	UT_ASSERT_EQ(test_v2_sent.requested_full_xid, 0);

	/* New H reuses exactly the older completed F, without a new census. */
	test_retention_sample.horizon_scn = 450;
	test_retention_sample.clock_scn = 750;
	test_horizon_now += 1000001;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_v2_sends, 2);
	UT_ASSERT_EQ(test_v2_sent.report_sequence, 2);
	UT_ASSERT_EQ(test_v2_sent.f_sample_id, 17);
	UT_ASSERT_EQ(test_v2_sent.horizon_scn, 450);
	UT_ASSERT_EQ(test_v2_sent.h_sample_clock_scn, 750);
	UT_ASSERT_EQ(test_v2_sent.active_full_xid_min,
				 U64FromFullTransactionId(test_sender_sample.scan.min_full));
}

UT_TEST(test_v2_sender_rechecks_capability_and_original_sample_without_stalling_v1)
{
	for (int fault = 0; fault < 7; fault++) {
		unsigned v1_sends = test_horizon_sends;

		reset_v2_sender();
		switch (fault) {
		case 0:
			test_sender_sample_valid = false;
			break;
		case 1:
			test_sender_sample_current = false;
			break;
		case 2:
			test_sender_change_on_recheck = true;
			break;
		case 3:
			test_v2_change_on_recheck = true;
			break;
		case 4:
			test_v2_cap_change_on_recheck = true;
			break;
		case 5:
			test_sender_sample.scan.status = CTRC_ACTIVE_SCAN_INCOMPLETE;
			break;
		case 6:
			test_sender_sample.sample_id = 0;
			break;
		}
		cluster_undo_horizon_lmon_tick();
		UT_ASSERT_EQ(test_horizon_sends, v1_sends + 3);
		UT_ASSERT_EQ(test_v2_sends, 0);
	}
}

UT_TEST(test_v2_sender_capability_is_independent_of_v1_and_sequence_is_shared)
{
	unsigned v1_sends = test_horizon_sends;

	reset_v2_sender();
	test_v1_capable = false;
	test_horizon_store.report_sequence = 900;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_sends, v1_sends);
	UT_ASSERT_EQ(test_v2_sends, 1);
	UT_ASSERT_EQ(test_v2_sent.report_sequence, 901);
	UT_ASSERT_EQ(test_horizon_store.report_sequence, 901);
	test_horizon_now += 1000001;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_v2_sent.report_sequence, 902);

	test_horizon_store.report_sequence = UINT64_MAX;
	test_v1_capable = true;
	test_horizon_now += 1000001;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_sends, v1_sends + 3);
	UT_ASSERT_EQ(test_v2_sends, 2);
	UT_ASSERT_EQ(test_horizon_store.report_sequence, UINT64_MAX);
	test_sender_sample_valid = false;
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/*
 * scn_time_cmp -- byte-faithful local stub (mirrors test_cluster_cr_tuple.c):
 * the fold compares peer bounds vs the local horizon through it.  Linking
 * cluster_scn.o would drag in shmem/atomics; the header-only scn_local()
 * inline matches the real impl.
 */
int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = scn_local(a);
	uint64 lb = scn_local(b);

	if (la < lb)
		return -1;
	if (la > lb)
		return 1;
	return 0;
}

/* ---- helpers ------------------------------------------------------- */

#define TEST_EPOCH ((uint64)7)
#define TEST_NOW_US ((uint64)90 * 1000 * 1000) /* 90s, roomy */
#define TEST_LOCAL_IV ((uint32)1000)		   /* 1s tick */
#define TEST_SELF 0

/* fresh under window = 3 * max(1000,1000)ms = 3s */
#define FRESH_RECV_US (TEST_NOW_US - (uint64)1 * 1000 * 1000)

static void
bm_set(uint8 *bm, int node)
{
	bm[node >> 3] |= (uint8)(1u << (node & 7));
}

/* a fully healthy report view */
static ClusterUndoHorizonReportView
mk_view(uint64 scn)
{
	ClusterUndoHorizonReportView v;

	memset(&v, 0, sizeof(v));
	v.valid = true;
	v.stable = true;
	v.has_capability = true;
	v.regression_flagged = false;
	v.epoch = TEST_EPOCH;
	v.horizon_scn = (SCN)scn;
	v.recv_at_us = FRESH_RECV_US;
	v.sender_interval_ms = 1000;
	return v;
}

typedef struct FoldResult {
	ClusterUndoHorizonFoldStatus st;
	ClusterUndoHorizonFloor floor;
	ClusterUndoHorizonStallReason reason;
	int32 blame;
} FoldResult;

static FoldResult
run_fold(SCN local, const ClusterUndoHorizonReportView *views, int nviews, const uint8 *required)
{
	FoldResult r;

	memset(&r, 0, sizeof(r));
	r.floor.scn = (SCN)0xdeadbeef;
	r.floor.epoch = 0xdead;
	r.reason = CLUSTER_UNDO_HORIZON_STALL_NONE;
	r.blame = -2;
	r.st = cluster_undo_horizon_cluster_floor(local, views, nviews, required, TEST_SELF, TEST_EPOCH,
											  TEST_NOW_US, TEST_LOCAL_IV, &r.floor, &r.reason,
											  &r.blame);
	return r;
}

/* ---- U1: empty required set => OK, floor = local ------------------- */
UT_TEST(test_u1_no_required_peer)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	FoldResult r = run_fold((SCN)500, NULL, 0, req);

	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)500);
	UT_ASSERT_EQ(r.floor.epoch, TEST_EPOCH);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_NONE);
}

/* ---- U2: all required fresh + epoch match => min fold -------------- */
UT_TEST(test_u2_all_fresh_min)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[3];
	FoldResult r;

	views[0] = mk_view(0); /* self slot: ignored */
	views[1] = mk_view(700);
	views[2] = mk_view(900);
	bm_set(req, 1);
	bm_set(req, 2);

	r = run_fold((SCN)800, views, 3, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)700);
	UT_ASSERT_EQ(r.floor.epoch, TEST_EPOCH);
}

/* ---- U3: required peer never reported => MISSING + blame ----------- */
UT_TEST(test_u3_missing)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].valid = false;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_MISSING);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U4: report older than 3 x max(sender, local) => STALE --------- */
UT_TEST(test_u4_stale)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	/* window = 3s; age it 3.5s */
	views[1].recv_at_us = TEST_NOW_US - (uint64)3500 * 1000;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_STALE);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U5: report epoch != current => EPOCH --------------------------- */
UT_TEST(test_u5_epoch_mismatch)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].epoch = TEST_EPOCH - 1;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_EPOCH);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U6: peer horizon below local => floor follows the peer -------- */
UT_TEST(test_u6_peer_below_local)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(100);
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)100);
}

/* ---- U7: InvalidScn report => MALFORMED ----------------------------- */
UT_TEST(test_u7_invalid_scn)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(0);
	views[1].horizon_scn = InvalidScn;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_MALFORMED);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U7b: invalid LOCAL horizon => MALFORMED, blame self ------------ */
UT_TEST(test_u7b_invalid_local)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	bm_set(req, 1);

	r = run_fold(InvalidScn, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_MALFORMED);
	UT_ASSERT_EQ(r.blame, TEST_SELF);
}

/* ---- U8: ghost view for a non-required node is ignored -------------- */
UT_TEST(test_u8_ghost_ignored)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[3];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[2] = mk_view(50);			 /* ghost: NOT required; lower than all */
	views[2].epoch = TEST_EPOCH - 3; /* and stale-epoch: must not matter */
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 3, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)700);
}

/* ---- U9: DEAD/REMOVED peer dropped from required => not consulted --- */
UT_TEST(test_u9_dead_removed_dropped)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[3];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[2] = mk_view(0);
	views[2].valid = false; /* node2 died; caller removed it from
								 * required -- its missing report must
								 * not stall the fold */
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 3, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)700);
}

/* ---- U10: recv_at in the future => conservative STALE --------------- */
UT_TEST(test_u10_future_recv)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].recv_at_us = TEST_NOW_US + 1;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_STALE);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U11: multi-node declared but required empty (cold formation) --- */
UT_TEST(test_u11_cold_formation)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[4];
	FoldResult r;
	int i;

	/* declared 4 nodes, none MEMBER yet: caller passes an empty required
	 * set and whatever sits in the slots must be ignored */
	for (i = 0; i < 4; i++) {
		views[i] = mk_view(1);
		views[i].valid = false;
	}
	r = run_fold((SCN)800, views, 4, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)800);
}

/* ---- U12: seqlock torn read => TORN --------------------------------- */
UT_TEST(test_u12_torn)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].stable = false;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_TORN);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U13: slow-tick sender is not misjudged stale ------------------- */
UT_TEST(test_u13_slow_sender_window)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].sender_interval_ms = 10000; /* 10s tick */
	/* age 20s: stale under 3 x local(1s), fresh under 3 x sender(10s) */
	views[1].recv_at_us = TEST_NOW_US - (uint64)20 * 1000 * 1000;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)700);
}

/* ---- U13b: out-of-range sender interval => MALFORMED ---------------- */
UT_TEST(test_u13b_bad_interval)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].sender_interval_ms = 60001; /* > MAX: handler rejects on
											 * the wire; the fold re-checks
											 * defensively */
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_MALFORMED);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U14: same-epoch regression latch => REGRESSION ----------------- */
UT_TEST(test_u14_regression)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700); /* previously accepted value still in the
								 * slot -- it must NOT be used once the
								 * peer contradicted it */
	views[1].regression_flagged = true;
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_REGRESSION);
	UT_ASSERT_EQ(r.blame, 1);
}

/* U15 (wire validation matrix) lives with the D5-2 handler tests. */

/* ---- U16: required peer without current capability => NOCAP --------- */
UT_TEST(test_u16_nocap)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);		 /* even with a fresh old report... */
	views[1].has_capability = false; /* ...no CURRENT-connection cap */
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_NOCAP);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- U16b: first-fail attribution order is deterministic ------------ */
UT_TEST(test_u16b_blame_order)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[3];
	FoldResult r;

	views[0] = mk_view(0);
	views[1] = mk_view(700);
	views[1].valid = false; /* node1: MISSING */
	views[2] = mk_view(700);
	views[2].has_capability = false; /* node2: NOCAP */
	bm_set(req, 1);
	bm_set(req, 2);

	/* lowest failing node id wins: node1 / MISSING */
	r = run_fold((SCN)800, views, 3, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_MISSING);
	UT_ASSERT_EQ(r.blame, 1);
}

/* Temporary idleness cannot waive a future snapshot's finite bound. */
UT_TEST(test_u17_idle_sentinel_skipped)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[3];
	FoldResult r;

	views[0] = mk_view(0); /* self slot: ignored */
	views[1] = mk_view((uint64)CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED);
	views[2] = mk_view(900);
	bm_set(req, 1);
	bm_set(req, 2);

	/* A peer can start a snapshot before its next report; infinity is not
	 * a proof that that snapshot is at/after the owner's local800. */
	r = run_fold((SCN)800, views, 3, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)InvalidScn);
	UT_ASSERT_EQ(r.floor.epoch, TEST_EPOCH);

	/* All peers idle at an earlier instant still proves no future bound. */
	views[2] = mk_view((uint64)CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED);
	r = run_fold((SCN)800, views, 3, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)InvalidScn);

	/* The original finite sample100 remains safe for a new snapshot110. */
	views[1] = mk_view(100);
	views[2] = mk_view(900);
	r = run_fold((SCN)800, views, 3, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_OK);
	UT_ASSERT_EQ((uint64)r.floor.scn, (uint64)100);
}

/* ---- U17b: sentinel reports still owe every proof obligation --------- */
UT_TEST(test_u17b_idle_sentinel_still_proven)
{
	uint8 req[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES] = { 0 };
	ClusterUndoHorizonReportView views[2];
	FoldResult r;

	/* a STALE sentinel stalls exactly like a stale real report: an idle
	 * peer that stops reporting is unproven coverage, not "unconstrained
	 * forever" */
	views[0] = mk_view(0);
	views[1] = mk_view((uint64)CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED);
	views[1].recv_at_us = TEST_NOW_US - (uint64)10 * 1000 * 1000; /* 10s old */
	bm_set(req, 1);

	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_MALFORMED);
	UT_ASSERT_EQ(r.blame, 1);

	/* an epoch-mismatched sentinel stalls too */
	views[1] = mk_view((uint64)CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED);
	views[1].epoch = TEST_EPOCH - 1;
	r = run_fold((SCN)800, views, 2, req);
	UT_ASSERT_EQ(r.st, CLUSTER_UNDO_HORIZON_FOLD_STALLED);
	UT_ASSERT_EQ(r.reason, CLUSTER_UNDO_HORIZON_STALL_EPOCH);
	UT_ASSERT_EQ(r.blame, 1);
}

/* ---- reason names cover every enum arm ------------------------------ */
UT_TEST(test_reason_names)
{
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_NONE),
					 "none");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_NOCAP),
					 "nocap");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_MISSING),
					 "missing");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_TORN),
					 "torn");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_EPOCH),
					 "epoch");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_MALFORMED),
					 "malformed");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_REGRESSION),
					 "regression");
	UT_ASSERT_STR_EQ(cluster_undo_horizon_stall_reason_name(CLUSTER_UNDO_HORIZON_STALL_STALE),
					 "stale");
}

UT_TEST(test_v2_summary_observes_complete_fixed_cohort)
{
	ClusterHeapFreezeReportV2 report = reset_v2_receiver();
	const char *summary;

	reset_v2_sender();
	report.active_full_xid_min = UINT64CONST(0x100000080);
	report.horizon_scn = 300;
	receive_v2_report(&report);
	summary = cluster_undo_horizon_peer_reports_summary();
	UT_ASSERT(strstr(summary, "v2_observation=READY,f=4294967424,h=300") != NULL);
	/* Preserve the original V1 diagnostic consumer too. */
	UT_ASSERT(strstr(summary, "n1:v=") != NULL);
}

static void
cohort_receive(int32 peer, ClusterHeapFreezeReportV2 report)
{
	test_v2_peer = peer;
	test_v2_source.source_node = peer;
	report.origin = peer;
	report.incarnation = test_v2_fence.admitted_incarnation[peer];
	receive_v2_report(&report);
}

static ClusterHeapFreezeReportV2
reset_v2_cohort(bool high_member)
{
	ClusterHeapFreezeReportV2 report = reset_v2_receiver();
	int peer;

	reset_v2_sender();
	test_cohort_all_caps = true;
	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++)
		test_cohort_generations[peer] = 19;
	if (high_member) {
		test_v2_fence.admitted_members_hi = UINT64CONST(1) << 1;
		test_v2_fence.admitted_incarnation[65] = 105;
		test_sender_sample.admission_fence = test_v2_fence;
	}
	report.active_full_xid_min = UINT64CONST(0x100000080);
	report.horizon_scn = 300;
	cohort_receive(1, report);
	return report;
}

static void
assert_cohort_refusal(ClusterHeapFreezeCohortStatus expected)
{
	ClusterHeapFreezeCohortObservation out, unchanged;
	ClusterHeapFreezeCohortStatus actual;

	memset(&out, 0xa5, sizeof(out));
	unchanged = out;
	actual = cluster_heap_freeze_cohort_observe(&out);
	UT_ASSERT_EQ(actual, expected);
	UT_ASSERT(memcmp(&out, &unchanged, sizeof(out)) == 0);
}

static ClusterHeapFreezeReportV2
maintenance_cohort(bool high_member)
{
	ClusterHeapFreezeReportV2 report = reset_v2_cohort(high_member);

	report.report_sequence++;
	report.f_sample_id++;
	report.future_full_xid = UINT64CONST(0x10000300f);
	report.requested_full_xid = UINT64CONST(0x10000205b);
	test_sender_sample.lease_fence.slot = 0;
	test_sender_sample.lease_fence.incarnation = test_v2_fence.local_boot_incarnation;
	test_sender_sample.lease_fence.lease_limit_full = UINT64CONST(0x100100000);
	cohort_receive(1, report);
	return report;
}

static ClusterHeapFreezeReportV2
maintenance_source_cohort(void)
{
	ClusterHeapFreezeReportV2 report = reset_v2_cohort(false);

	report.report_sequence++;
	report.f_sample_id++;
	report.future_full_xid = UINT64CONST(0x100002001);
	test_sender_sample.lease_fence.slot = 0;
	test_sender_sample.lease_fence.incarnation = test_v2_fence.local_boot_incarnation;
	test_sender_sample.lease_fence.lease_limit_full = UINT64CONST(0x100100000);
	cohort_receive(1, report);
	return report;
}

UT_TEST(test_vacuum_intent_uses_complete_future_and_original_sender)
{
	ClusterHeapFreezeReportV2 report = maintenance_source_cohort();
	ClusterHeapFreezeCohortObservation observation;

	UT_ASSERT(cluster_heap_freeze_request_maintenance());
	UT_ASSERT_EQ(test_maintenance_calls, 0);
	UT_ASSERT(cluster_heap_freeze_cohort_observe(&observation) == CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(observation.requested_full_xid_max, report.future_full_xid);
	UT_ASSERT_EQ(observation.active_full_xid_min, UINT64CONST(0x100000080));
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_maintenance_calls, 1);
	UT_ASSERT_EQ(test_maintenance_requested, UINT64CONST(0x100002000));
	UT_ASSERT_EQ(test_v2_sends, 1);
	UT_ASSERT_EQ(test_v2_sent.requested_full_xid, report.future_full_xid);
	UT_ASSERT_EQ(test_v2_sent.future_full_xid, UINT64CONST(0x100001000));
	UT_ASSERT_EQ(test_v2_sent.active_full_xid_min, UINT64CONST(0x100000100));
}

UT_TEST(test_vacuum_intent_refuses_incomplete_or_stale_cuts)
{
	for (int fault = 0; fault < 5; fault++) {
		(void)maintenance_source_cohort();
		if (fault == 0)
			test_horizon_store.freeze_slots[1].valid = false;
		else if (fault == 1)
			test_horizon_now += 10000001;
		else if (fault == 2)
			test_sender_sample_valid = false;
		else if (fault == 3)
			test_v2_identity = false;
		else
			test_sender_change_on_recheck = true;
		UT_ASSERT(!cluster_heap_freeze_request_maintenance());
		UT_ASSERT_EQ(test_horizon_store.maintenance.requested, 0);
		UT_ASSERT_EQ(test_maintenance_calls, 0);
	}
}

UT_TEST(test_vacuum_intents_coalesce_and_old_retirement_cannot_clear_new_intent)
{
	ClusterHeapFreezeReportV2 report = maintenance_source_cohort();
	UndoHorizonMaintenanceIntent old;

	UT_ASSERT(cluster_heap_freeze_request_maintenance());
	old = test_horizon_store.maintenance;
	UT_ASSERT(cluster_heap_freeze_request_maintenance());
	UT_ASSERT_EQ(test_horizon_store.maintenance.sequence, old.sequence);
	report.report_sequence++;
	report.f_sample_id++;
	report.future_full_xid += CLUSTER_XID_STRIDE;
	cohort_receive(1, report);
	UT_ASSERT(cluster_heap_freeze_request_maintenance());
	UT_ASSERT_EQ(test_horizon_store.maintenance.requested, report.future_full_xid);
	UT_ASSERT(test_horizon_store.maintenance.sequence > old.sequence);
	undo_horizon_maintenance_retire(&old);
	UT_ASSERT_EQ(test_horizon_store.maintenance.requested, report.future_full_xid);
}

UT_TEST(test_lmon_retires_intent_only_after_complete_stripe_future_coverage)
{
	ClusterHeapFreezeReportV2 report = maintenance_source_cohort();

	UT_ASSERT(cluster_heap_freeze_request_maintenance());
	/* The requester is covered, but the other origin still needs a jump. */
	test_sender_sample.scan.future_full = FullTransactionIdFromU64(report.future_full_xid + 15);
	test_sender_sample.sample_id++;
	report.report_sequence++;
	report.f_sample_id++;
	report.future_full_xid -= CLUSTER_XID_STRIDE;
	/* Inject an older, still exact physical observation: no authority is
	 * derived from the requested value or the old ACTIVE minimum. */
	test_horizon_store.freeze_slots[1].view.report = report;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT(test_horizon_store.maintenance.requested != 0);
	UT_ASSERT(test_v2_sent.requested_full_xid != 0);
	report.future_full_xid += CLUSTER_XID_STRIDE;
	report.report_sequence++;
	report.f_sample_id++;
	cohort_receive(1, report);
	test_horizon_now += 1000001;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_store.maintenance.requested, 0);
	UT_ASSERT_EQ(test_v2_sent.requested_full_xid, 0);
	UT_ASSERT_EQ(test_v2_sent.active_full_xid_min, UINT64CONST(0x100000100));
}

UT_TEST(test_lmon_keeps_intent_when_coverage_is_incomplete)
{
	(void)maintenance_source_cohort();
	UT_ASSERT(cluster_heap_freeze_request_maintenance());
	test_sender_sample.scan.future_full = FullTransactionIdFromU64(UINT64CONST(0x100003000));
	test_sender_sample.sample_id++;
	test_horizon_store.freeze_slots[1].valid = false;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_store.maintenance.requested, UINT64CONST(0x100002001));
	UT_ASSERT_EQ(test_maintenance_calls, 0);
}

UT_TEST(test_lmon_retires_stale_identity_even_without_local_history)
{
	(void)maintenance_source_cohort();
	UT_ASSERT(cluster_heap_freeze_request_maintenance());
	test_v2_fence.admission_publication_seq += 2;
	test_retention_sample_valid = false;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_store.maintenance.requested, 0);
	UT_ASSERT_EQ(test_maintenance_calls, 0);
	UT_ASSERT_EQ(test_v2_sends, 0);
}

UT_TEST(test_vacuum_intent_sequence_never_wraps)
{
	(void)maintenance_source_cohort();
	test_horizon_store.maintenance.sequence = UINT64_MAX;
	UT_ASSERT(!cluster_heap_freeze_request_maintenance());
	UT_ASSERT_EQ(test_horizon_store.maintenance.sequence, UINT64_MAX);
	UT_ASSERT_EQ(test_horizon_store.maintenance.requested, 0);
}

UT_TEST(test_v2_lmon_relays_bounded_intent_with_original_local_fences)
{
	(void)maintenance_cohort(false);
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_maintenance_calls, 1);
	UT_ASSERT_EQ(test_maintenance_requested, UINT64CONST(0x100002050));
	UT_ASSERT(memcmp(&test_maintenance_identity, &test_sender_sample.admission_fence,
					 sizeof(test_maintenance_identity))
			  == 0);
	UT_ASSERT(memcmp(&test_maintenance_stripe, &test_sender_sample.lease_fence,
					 sizeof(test_maintenance_stripe))
			  == 0);
	/* The maintenance intent never replaces either proven F or the ceiling. */
	UT_ASSERT(strstr(cluster_undo_horizon_peer_reports_summary(),
					 "v2_observation=READY,f=4294967424,h=300")
			  != NULL);
}

UT_TEST(test_v2_lmon_ignores_absent_over_ceiling_or_covered_intents)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterHeapFreezeReportV2 report = maintenance_cohort(false);

		report.report_sequence++;
		if (fault == 0)
			report.requested_full_xid = 0;
		else if (fault == 1)
			report.requested_full_xid = report.future_full_xid + 1;
		else
			report.requested_full_xid
				= U64FromFullTransactionId(test_sender_sample.scan.future_full)
				  + (fault == 3 ? 15 : 0);
		cohort_receive(1, report);
		cluster_undo_horizon_lmon_tick();
		UT_ASSERT_EQ(test_maintenance_calls, 0);
	}
}

UT_TEST(test_v2_lmon_relays_only_complete_cohort_and_maximum_request)
{
	ClusterHeapFreezeReportV2 report = maintenance_cohort(true);

	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_maintenance_calls, 0);
	report.requested_full_xid = UINT64CONST(0x10000209f);
	cohort_receive(65, report);
	test_horizon_now += 1000000;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_maintenance_calls, 1);
	UT_ASSERT_EQ(test_maintenance_requested, UINT64CONST(0x100002090));
}

UT_TEST(test_v2_lmon_refused_cohorts_cannot_relay_requests)
{
	for (int fault = 0; fault < 5; fault++) {
		(void)maintenance_cohort(false);
		if (fault == 0)
			test_horizon_store.freeze_slots[1].valid = false;
		else if (fault == 1)
			test_horizon_store.freeze_slots[1].view.recv_at_us = test_horizon_now - 3000001;
		else if (fault == 2)
			pg_atomic_write_u32(&test_horizon_store.freeze_slots[1].seq, 1);
		else if (fault == 3)
			test_v2_identity = false;
		else
			test_sender_sample.scan.min_full = FullTransactionIdFromEpochAndXid(1, 8192);
		cluster_undo_horizon_lmon_tick();
		UT_ASSERT_EQ(test_maintenance_calls, 0);
	}
}

UT_TEST(test_v2_lmon_relay_rechecks_admission_before_handoff)
{
	(void)maintenance_cohort(false);
	/* The complete cohort read succeeds, then the relay's capture loses
	 * the admission cut. A READY result is not a cached handoff permit. */
	test_change_admission_on_capture = 2;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_maintenance_calls, 0);
}

UT_TEST(test_v2_lmon_relay_uses_original_cadence_and_no_completion_cache)
{
	for (int result = CLUSTER_XID_STRIPE_MAINTENANCE_PENDING;
		 result <= CLUSTER_XID_STRIPE_MAINTENANCE_UNAVAILABLE; result++) {
		(void)maintenance_cohort(false);
		test_maintenance_result = result;
		cluster_undo_horizon_lmon_tick();
		UT_ASSERT_EQ(test_maintenance_calls, 1);
		cluster_undo_horizon_lmon_tick();
		test_horizon_now += 999999;
		cluster_undo_horizon_lmon_tick();
		UT_ASSERT_EQ(test_maintenance_calls, 1);
		test_horizon_now++;
		cluster_undo_horizon_lmon_tick();
		UT_ASSERT_EQ(test_maintenance_calls, 2);
	}
}

UT_TEST(test_v2_cohort_requires_both_mask_halves_and_keeps_domains_separate)
{
	ClusterHeapFreezeReportV2 report = reset_v2_cohort(true);
	ClusterHeapFreezeCohortObservation out;
	unsigned cuts = test_retention_sample_calls;

	assert_cohort_refusal(CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE);
	UT_ASSERT_EQ(test_retention_sample_calls, cuts + 1);
	report.active_full_xid_min = UINT64CONST(0x100000040);
	report.horizon_scn = (UINT64CONST(65) << SCN_NODE_ID_SHIFT) | 250;
	cohort_receive(65, report);
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_observe(&out), CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(out.active_full_xid_min, UINT64CONST(0x100000040));
	UT_ASSERT_EQ(out.horizon_scn, report.horizon_scn);
	UT_ASSERT_EQ(out.record_generation, 8);
	UT_ASSERT_EQ(out.formation_epoch, 7);
	UT_ASSERT_EQ(out.admitted_members_lo, 3);
	UT_ASSERT_EQ(out.admitted_members_hi, 2);
	/* Membership, not the configured-peer enumeration, determines the AND. */
	UT_ASSERT(cluster_conf_lookup_node(65) == NULL);
}

UT_TEST(test_v2_cohort_reports_only_complete_observed_future_ceiling)
{
	ClusterHeapFreezeReportV2 report = reset_v2_cohort(true);
	ClusterHeapFreezeCohortObservation out;

	/* No subset, including an origin's request, may supply the ceiling. */
	assert_cohort_refusal(CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE);
	report.future_full_xid = UINT64CONST(0x100003000);
	report.requested_full_xid = UINT64CONST(0x100004000);
	cohort_receive(65, report);
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_observe(&out), CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(out.future_full_xid_max, report.future_full_xid);
	UT_ASSERT_EQ(out.active_full_xid_min, UINT64CONST(0x100000080));
	/* The local completed sample participates even when every peer is lower. */
	test_sender_sample.scan.future_full = FullTransactionIdFromU64(UINT64CONST(0x100005000));
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_observe(&out), CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(out.future_full_xid_max, UINT64CONST(0x100005000));
}

UT_TEST(test_v2_cohort_missing_members_never_shrink_required_set)
{
	for (int fault = 0; fault < 3; fault++) {
		(void)reset_v2_cohort(false);
		if (fault == 0)
			test_horizon_store.freeze_slots[1].valid = false;
		else if (fault == 1) {
			test_v2_fence.admitted_members_lo |= UINT64CONST(1) << 63;
			test_v2_fence.admitted_incarnation[63] = 103;
			test_sender_sample.admission_fence = test_v2_fence;
			/* Existing reports belong to the former full cohort. */
			test_horizon_store.freeze_slots[1].fence = test_v2_fence;
		} else {
			test_v2_fence.admitted_members_hi = UINT64CONST(1) << 63;
			test_v2_fence.admitted_incarnation[127] = 167;
			test_sender_sample.admission_fence = test_v2_fence;
			test_horizon_store.freeze_slots[1].fence = test_v2_fence;
		}
		assert_cohort_refusal(CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE);
	}
}

UT_TEST(test_v2_cohort_h_freshness_does_not_expire_f_sample)
{
	ClusterHeapFreezeReportV2 report = reset_v2_cohort(false);
	ClusterHeapFreezeCohortObservation out;
	uint64 old_sample = report.f_sample_id;

	test_horizon_now += 3000000;
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_observe(&out), CLUSTER_HEAP_FREEZE_COHORT_READY);
	test_horizon_now++;
	assert_cohort_refusal(CLUSTER_HEAP_FREEZE_COHORT_H_STALE);
	/* Arbitrarily old completed F becomes observable with a new, fresh H. */
	test_horizon_now += INT64CONST(86400000000);
	report.report_sequence++;
	report.horizon_scn = 350;
	report.h_sample_clock_scn = 800;
	cohort_receive(1, report);
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_observe(&out), CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(out.active_full_xid_min, report.active_full_xid_min);
	UT_ASSERT_EQ(out.horizon_scn, 350);
	UT_ASSERT_EQ(test_horizon_store.freeze_slots[1].view.report.f_sample_id, old_sample);
	/* The original max(sender, local) freshness window is retained. */
	report.report_sequence++;
	report.sender_interval_ms = 60000;
	cohort_receive(1, report);
	test_horizon_now += 180000000;
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_observe(&out), CLUSTER_HEAP_FREEZE_COHORT_READY);
	test_horizon_now++;
	assert_cohort_refusal(CLUSTER_HEAP_FREEZE_COHORT_H_STALE);
}

UT_TEST(test_v2_cohort_uses_end_clock_and_rejects_rollback)
{
	(void)reset_v2_cohort(false);
	test_cohort_clock_shift = 3000001;
	assert_cohort_refusal(CLUSTER_HEAP_FREEZE_COHORT_H_STALE);
	(void)reset_v2_cohort(false);
	test_horizon_now += 100;
	test_cohort_clock_shift = -1;
	/* Even a rollback that leaves recv_at in the past is rejected. */
	assert_cohort_refusal(CLUSTER_HEAP_FREEZE_COHORT_H_STALE);
	(void)reset_v2_cohort(false);
	test_horizon_now--;
	assert_cohort_refusal(CLUSTER_HEAP_FREEZE_COHORT_H_STALE);
}

UT_TEST(test_v2_cohort_rechecks_earlier_peers_after_later_reads)
{
	for (int fault = 1; fault <= 3; fault++) {
		ClusterHeapFreezeReportV2 report = reset_v2_cohort(true);

		cohort_receive(65, report);
		test_cohort_trigger_peer = 65;
		test_cohort_changed_peer = 1;
		test_cohort_change = fault;
		assert_cohort_refusal(fault == 2 ? CLUSTER_HEAP_FREEZE_COHORT_BUSY_PUBLISH
										 : CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED);
	}
	(void)reset_v2_cohort(false);
	pg_atomic_write_u32(&test_horizon_store.freeze_slots[1].seq, 3);
	assert_cohort_refusal(CLUSTER_HEAP_FREEZE_COHORT_BUSY_PUBLISH);
}

UT_TEST(test_v2_cohort_rejects_local_sample_and_admission_changes)
{
	for (int fault = 0; fault < 9; fault++) {
		ClusterHeapFreezeCohortStatus expected = CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;

		(void)reset_v2_cohort(false);
		if (fault == 0)
			test_sender_change_on_recheck = true;
		else if (fault == 1)
			test_sender_sample_current = false;
		else if (fault == 2)
			test_v2_change_on_recheck = true;
		else if (fault == 3)
			test_v2_identity = false;
		else if (fault == 4) {
			test_sender_sample_valid = false;
			expected = CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE;
		} else if (fault == 5) {
			test_retention_sample_valid = false;
			expected = CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE;
		} else if (fault == 6)
			test_v2_fence.admitted_members_lo &= ~UINT64CONST(1);
		else if (fault == 7)
			test_v2_fence.admitted_incarnation[0]++;
		else
			test_cohort_generations[1]++;
		assert_cohort_refusal(expected);
	}
}

UT_TEST(test_v2_cohort_defends_against_malformed_slots)
{
	for (int fault = 0; fault < 13; fault++) {
		ClusterHeapFreezeReportV2 *r;
		ClusterHeapFreezeCohortStatus expected = CLUSTER_HEAP_FREEZE_COHORT_INVALID;

		(void)reset_v2_cohort(false);
		r = &test_horizon_store.freeze_slots[1].view.report;
		switch (fault) {
		case 0:
			r->active_full_xid_min = UINT64CONST(0x100000001);
			break;
		case 1:
			r->future_full_xid = 2;
			break;
		case 2:
			r->active_full_xid_min = r->future_full_xid + 1;
			break;
		case 3:
			r->horizon_scn = r->h_sample_clock_scn + 1;
			break;
		case 4:
			r->horizon_scn = CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED;
			break;
		case 5:
			r->sender_interval_ms = 0;
			break;
		case 6:
			r->f_sample_id = 0;
			break;
		case 7:
			r->flags = 1;
			break;
		case 8:
			r->requested_full_xid = UINT64CONST(0x100000002);
			break;
		case 9:
			test_horizon_store.freeze_slots[1].view.horizon_regression_flagged = true;
			break;
		case 10:
			r->h_contributor_kind = 5;
			break;
		case 11:
			r->incarnation++;
			expected = CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
			break;
		case 12:
			r->origin = 2;
			expected = CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
			break;
		}
		assert_cohort_refusal(expected);
	}
}

UT_TEST(test_v2_cohort_single_member_still_requires_local_evidence)
{
	ClusterHeapFreezeCohortObservation out;

	(void)reset_v2_cohort(false);
	test_v2_fence.admitted_members_lo = 1;
	test_sender_sample.admission_fence = test_v2_fence;
	test_retention_sample.contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_UNKNOWN;
	test_retention_sample.contributor_token = 0;
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_observe(&out), CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(out.active_full_xid_min,
				 U64FromFullTransactionId(test_sender_sample.scan.min_full));
	UT_ASSERT_EQ(out.horizon_scn, 400);
	UT_ASSERT_EQ(out.admitted_members_lo, 1);
	test_sender_sample.scan.min_full = FullTransactionIdFromEpochAndXid(1, 8192);
	assert_cohort_refusal(CLUSTER_HEAP_FREEZE_COHORT_INVALID);
	test_sender_sample.scan.min_full = FullTransactionIdFromEpochAndXid(1, 1);
	assert_cohort_refusal(CLUSTER_HEAP_FREEZE_COHORT_INVALID);
	test_sender_sample.scan.min_full = FullTransactionIdFromEpochAndXid(1, 256);
	test_retention_sample.horizon_scn = test_retention_sample.clock_scn + 1;
	assert_cohort_refusal(CLUSTER_HEAP_FREEZE_COHORT_INVALID);
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_observe(NULL), CLUSTER_HEAP_FREEZE_COHORT_INVALID);
}

UT_TEST(test_v2_summary_refusal_does_not_reuse_observed_values)
{
	const char *summary;

	(void)reset_v2_cohort(false);
	UT_ASSERT(strstr(cluster_undo_horizon_peer_reports_summary(), "v2_observation=READY,f=")
			  != NULL);
	test_horizon_store.freeze_slots[1].valid = false;
	summary = cluster_undo_horizon_peer_reports_summary();
	UT_ASSERT(strstr(summary, "v2_observation=INCOMPLETE ") != NULL);
	UT_ASSERT(strstr(summary, ",f=") == NULL);
	UT_ASSERT(strstr(summary, ",h=") == NULL);
	UT_ASSERT(strstr(summary, "n1:v=") != NULL);
}

UT_TEST(test_v2_lmon_observes_once_per_interval_without_an_extra_retention_cut)
{
	unsigned cuts;

	(void)reset_v2_cohort(false);
	cuts = test_retention_sample_calls;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_retention_sample_calls, cuts + 1);
	UT_ASSERT(strstr(cluster_undo_horizon_peer_reports_summary(), "v2_lmon=READY:1,INCOMPLETE:0")
			  != NULL);
	cluster_undo_horizon_lmon_tick();
	test_horizon_now += 999999;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT(strstr(cluster_undo_horizon_peer_reports_summary(), "v2_lmon=READY:1,INCOMPLETE:0")
			  != NULL);
	test_horizon_now++;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT(strstr(cluster_undo_horizon_peer_reports_summary(), "v2_lmon=READY:2,INCOMPLETE:0")
			  != NULL);
}

UT_TEST(test_v2_lmon_publishes_identity_bound_global_watermark)
{
	ClusterHeapFreezeWatermark watermark;

	(void)reset_v2_cohort(false);
	UT_ASSERT(!cluster_heap_freeze_watermark_sample(&watermark));
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT(cluster_heap_freeze_watermark_sample(&watermark));
	UT_ASSERT_EQ(watermark.future_full_xid_max, UINT64CONST(0x100001000));
	UT_ASSERT_EQ(watermark.active_full_xid_min, UINT64CONST(0x100000080));
	UT_ASSERT_EQ(watermark.horizon_scn, 300);
	UT_ASSERT_EQ(watermark.publication_sequence, 1);
	UT_ASSERT_EQ(watermark.f_sample_id, 17);
	UT_ASSERT(watermark.published_at_us != 0);
	UT_ASSERT_EQ(watermark.record_generation, 8);
	UT_ASSERT_EQ(watermark.formation_epoch, 7);
	UT_ASSERT_EQ(watermark.admitted_members_lo, 3);
	UT_ASSERT_EQ(watermark.admitted_members_hi, 0);
}

UT_TEST(test_v2_refused_fold_preserves_watermark_and_identity_failure_blocks_reader)
{
	ClusterHeapFreezeWatermark first, after;

	(void)reset_v2_cohort(false);
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT(cluster_heap_freeze_watermark_sample(&first));
	test_horizon_store.freeze_slots[1].valid = false;
	test_horizon_now += 1000001;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT(cluster_heap_freeze_watermark_sample(&after));
	UT_ASSERT(memcmp(&after, &first, sizeof(after)) == 0);
	test_v2_identity = false;
	UT_ASSERT(!cluster_heap_freeze_watermark_sample(&after));
	test_v2_identity = true;
	test_sender_sample_current = false;
	UT_ASSERT(!cluster_heap_freeze_watermark_sample(&after));
}

UT_TEST(test_v2_lmon_counts_missing_local_history_without_sending)
{
	unsigned sends;

	(void)reset_v2_cohort(false);
	sends = test_horizon_sends;
	test_retention_sample_valid = false;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT_EQ(test_horizon_sends, sends);
	UT_ASSERT(strstr(cluster_undo_horizon_peer_reports_summary(), "v2_lmon=READY:0,INCOMPLETE:1")
			  != NULL);
	test_horizon_now += 10;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT(strstr(cluster_undo_horizon_peer_reports_summary(), "v2_lmon=READY:0,INCOMPLETE:1")
			  != NULL);
	test_retention_sample_valid = true;
	test_horizon_now += 1000000;
	cluster_undo_horizon_lmon_tick();
	UT_ASSERT(strstr(cluster_undo_horizon_peer_reports_summary(), "v2_lmon=READY:1,INCOMPLETE:1")
			  != NULL);
}

UT_TEST(test_v2_lmon_records_real_cohort_refusals)
{
	static const char *const outcomes[]
		= { "v2_lmon=READY:0,INCOMPLETE:1,H_STALE:0,BUSY_PUBLISH:0,IDENTITY_CHANGED:0,INVALID:0",
			"v2_lmon=READY:0,INCOMPLETE:0,H_STALE:1,BUSY_PUBLISH:0,IDENTITY_CHANGED:0,INVALID:0",
			"v2_lmon=READY:0,INCOMPLETE:0,H_STALE:0,BUSY_PUBLISH:1,IDENTITY_CHANGED:0,INVALID:0",
			"v2_lmon=READY:0,INCOMPLETE:0,H_STALE:0,BUSY_PUBLISH:0,IDENTITY_CHANGED:1,INVALID:0",
			"v2_lmon=READY:0,INCOMPLETE:0,H_STALE:0,BUSY_PUBLISH:0,IDENTITY_CHANGED:0,INVALID:1" };
	int i;

	for (i = 0; i < lengthof(outcomes); i++) {
		const char *summary;

		(void)reset_v2_cohort(false);
		switch (i) {
		case 0:
			test_horizon_store.freeze_slots[1].valid = false;
			break;
		case 1:
			test_horizon_store.freeze_slots[1].view.recv_at_us = test_horizon_now - 3000001;
			break;
		case 2:
			pg_atomic_write_u32(&test_horizon_store.freeze_slots[1].seq, 1);
			break;
		case 3:
			test_v2_identity = false;
			break;
		case 4:
			test_sender_sample.scan.min_full = FullTransactionIdFromEpochAndXid(1, 8192);
			break;
		}
		cluster_undo_horizon_lmon_tick();
		summary = cluster_undo_horizon_peer_reports_summary();
		UT_ASSERT(strstr(summary, outcomes[i]) != NULL);
		UT_ASSERT(strstr(summary, ",f=") == NULL);
		UT_ASSERT(strstr(summary, ",h=") == NULL);
	}
}

UT_TEST(test_v2_lmon_observation_respects_original_shutdown_and_enable_gates)
{
	unsigned sends;

	(void)reset_v2_cohort(false);
	sends = test_horizon_sends;
	test_horizon_seal = true;
	cluster_undo_horizon_lmon_tick();
	test_horizon_seal = false;
	test_horizon_cut = true;
	cluster_undo_horizon_lmon_tick();
	test_horizon_cut = false;
	cluster_enabled = false;
	cluster_undo_horizon_lmon_tick();
	cluster_enabled = true;
	UT_ASSERT_EQ(test_horizon_sends, sends);
	UT_ASSERT(
		strstr(cluster_undo_horizon_peer_reports_summary(),
			   "v2_lmon=READY:0,INCOMPLETE:0,H_STALE:0,BUSY_PUBLISH:0,IDENTITY_CHANGED:0,INVALID:0")
		!= NULL);
}

UT_TEST(test_v2_lmon_observes_clock_rollback_as_stale)
{
	const char *summary;

	(void)reset_v2_cohort(false);
	cluster_undo_horizon_lmon_tick();
	test_horizon_now--;
	cluster_undo_horizon_lmon_tick();
	summary = cluster_undo_horizon_peer_reports_summary();
	UT_ASSERT(strstr(summary, "v2_lmon=READY:1,INCOMPLETE:0,H_STALE:1") != NULL);
	UT_ASSERT(strstr(summary, ",f=") == NULL);
	UT_ASSERT(strstr(summary, ",h=") == NULL);
}

int
main(void)
{
	UT_PLAN(78);
	UndoHorizonShmem = &test_horizon_store;

	UT_RUN(test_u1_no_required_peer);
	UT_RUN(test_u2_all_fresh_min);
	UT_RUN(test_u3_missing);
	UT_RUN(test_u4_stale);
	UT_RUN(test_u5_epoch_mismatch);
	UT_RUN(test_u6_peer_below_local);
	UT_RUN(test_u7_invalid_scn);
	UT_RUN(test_u7b_invalid_local);
	UT_RUN(test_u8_ghost_ignored);
	UT_RUN(test_u9_dead_removed_dropped);
	UT_RUN(test_u10_future_recv);
	UT_RUN(test_u11_cold_formation);
	UT_RUN(test_u12_torn);
	UT_RUN(test_u13_slow_sender_window);
	UT_RUN(test_u13b_bad_interval);
	UT_RUN(test_u14_regression);
	UT_RUN(test_u16_nocap);
	UT_RUN(test_u16b_blame_order);
	UT_RUN(test_u17_idle_sentinel_skipped);
	UT_RUN(test_u17b_idle_sentinel_still_proven);
	UT_RUN(test_reason_names);
	UT_RUN(test_original_horizon_sender_stops_only_after_all_checkpoint_cut);
	UT_RUN(test_lmon_uses_one_typed_retention_cut);
	UT_RUN(test_lmon_refuses_missing_typed_retention_sample);
	UT_RUN(test_local_report_preserves_same_cut_metadata);
	UT_RUN(test_local_report_attributes_mrp_clamp_and_clock_ties);
	UT_RUN(test_local_report_refusal_clears_metadata_and_stalls_sender);
	UT_RUN(test_v2_actual_ingress_accepts_current_report);
	UT_RUN(test_v2_replay_does_not_refresh_h_or_rewind_f);
	UT_RUN(test_v2_new_h_can_reuse_old_f_sample);
	UT_RUN(test_v2_f_sample_is_immutable_and_monotone);
	UT_RUN(test_v2_h_regression_blocks_h_without_destroying_f);
	UT_RUN(test_v2_reusing_regressed_h_sequence_cannot_clear_latch);
	UT_RUN(test_v2_invalid_frames_preserve_valid_observation);
	UT_RUN(test_v2_payload_cannot_assert_identity_or_special_xids);
	UT_RUN(test_v2_requires_real_current_control_ingress);
	UT_RUN(test_v2_control_serial_is_independent_of_hello_generation);
	UT_RUN(test_v2_identity_and_connection_loss_retire_observation);
	UT_RUN(test_v2_reconnect_does_not_reset_sequence);
	UT_RUN(test_v2_identity_change_allows_new_sequence);
	UT_RUN(test_v2_publication_rechecks_identity);
	UT_RUN(test_v2_requires_admitted_peer_in_both_bitmap_halves);
	UT_RUN(test_v2_sampler_is_bounded_and_f_has_no_age_expiry);
	UT_RUN(test_original_v1_receiver_remains_separate);
	UT_RUN(test_v2_sender_uses_complete_worker_sample_and_same_typed_h_cut);
	UT_RUN(test_v2_sender_rechecks_capability_and_original_sample_without_stalling_v1);
	UT_RUN(test_v2_sender_capability_is_independent_of_v1_and_sequence_is_shared);
	UT_RUN(test_v2_summary_observes_complete_fixed_cohort);
	UT_RUN(test_v2_cohort_requires_both_mask_halves_and_keeps_domains_separate);
	UT_RUN(test_v2_cohort_reports_only_complete_observed_future_ceiling);
	UT_RUN(test_vacuum_intent_uses_complete_future_and_original_sender);
	UT_RUN(test_vacuum_intent_refuses_incomplete_or_stale_cuts);
	UT_RUN(test_vacuum_intents_coalesce_and_old_retirement_cannot_clear_new_intent);
	UT_RUN(test_lmon_retires_intent_only_after_complete_stripe_future_coverage);
	UT_RUN(test_lmon_keeps_intent_when_coverage_is_incomplete);
	UT_RUN(test_lmon_retires_stale_identity_even_without_local_history);
	UT_RUN(test_vacuum_intent_sequence_never_wraps);
	UT_RUN(test_v2_lmon_relays_bounded_intent_with_original_local_fences);
	UT_RUN(test_v2_lmon_ignores_absent_over_ceiling_or_covered_intents);
	UT_RUN(test_v2_lmon_relays_only_complete_cohort_and_maximum_request);
	UT_RUN(test_v2_lmon_refused_cohorts_cannot_relay_requests);
	UT_RUN(test_v2_lmon_relay_rechecks_admission_before_handoff);
	UT_RUN(test_v2_lmon_relay_uses_original_cadence_and_no_completion_cache);
	UT_RUN(test_v2_cohort_missing_members_never_shrink_required_set);
	UT_RUN(test_v2_cohort_h_freshness_does_not_expire_f_sample);
	UT_RUN(test_v2_cohort_uses_end_clock_and_rejects_rollback);
	UT_RUN(test_v2_cohort_rechecks_earlier_peers_after_later_reads);
	UT_RUN(test_v2_cohort_rejects_local_sample_and_admission_changes);
	UT_RUN(test_v2_cohort_defends_against_malformed_slots);
	UT_RUN(test_v2_cohort_single_member_still_requires_local_evidence);
	UT_RUN(test_v2_summary_refusal_does_not_reuse_observed_values);
	UT_RUN(test_v2_lmon_observes_once_per_interval_without_an_extra_retention_cut);
	UT_RUN(test_v2_lmon_publishes_identity_bound_global_watermark);
	UT_RUN(test_v2_refused_fold_preserves_watermark_and_identity_failure_blocks_reader);
	UT_RUN(test_v2_lmon_counts_missing_local_history_without_sending);
	UT_RUN(test_v2_lmon_records_real_cohort_refusals);
	UT_RUN(test_v2_lmon_observation_respects_original_shutdown_and_enable_gates);
	UT_RUN(test_v2_lmon_observes_clock_rollback_as_stale);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
