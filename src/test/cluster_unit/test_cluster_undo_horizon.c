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
int cluster_injection_armed_count;
int cluster_lmon_main_loop_interval = 1000;
static TimestampTz test_horizon_now = INT64CONST(10000000);
static unsigned test_horizon_sends;
static bool test_horizon_cut;
static bool test_horizon_seal;
static bool test_horizon_capable = true;
static ClusterUndoHorizonWire test_horizon_last;
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
	*generation = test_v2_cap_generation;
	return peer == test_v2_peer && (*word & required) == required;
}

bool
cluster_sf_peer_capability_generation_matches(int32 peer, uint32 required, uint32 generation)
{
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
	return (SCN)500;
}
bool
cluster_mrp_should_start(void)
{
	return false;
}
SCN
cluster_mrp_standby_consistent_scn(void)
{
	return InvalidScn;
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
	return test_horizon_capable && peer_id > 0 && peer_id < 4;
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
	Assert(length == sizeof(test_horizon_last));
	memcpy(&test_horizon_last, payload, length);
	test_horizon_sends++;
	return CLUSTER_IC_SEND_DONE;
}

static ClusterHeapFreezeReportV2
reset_v2_receiver(void)
{
	ClusterHeapFreezeReportV2 r = { 0 };
	int i;

	memset(&test_horizon_store, 0, sizeof(test_horizon_store));
	UndoHorizonShmem = &test_horizon_store;
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		pg_atomic_init_u32(&test_horizon_store.slots[i].seq, 0);
		pg_atomic_init_u32(&test_horizon_store.freeze_slots[i].seq, 0);
	}
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

int
main(void)
{
	UT_PLAN(39);
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

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
