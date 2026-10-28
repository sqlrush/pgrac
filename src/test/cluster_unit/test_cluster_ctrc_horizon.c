/*-------------------------------------------------------------------------
 * test_cluster_ctrc_horizon.c
 *    Original native/shared protection handoff through the V2 sender.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * The original shared engine, native transaction exit, worker pass and
 * publication are reused unchanged. Peer identity, transport, H and lease
 * admission are controlled boundaries, not positive freeze certificates.
 *-------------------------------------------------------------------------
 */
int ctrc_component_main(void);
#define main ctrc_component_main
#include "test_cluster_ctrc_cleaner.c"
#undef main

#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_undo_horizon.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_sf_dep.h"

int cluster_lmon_main_loop_interval = 1000;
bool cluster_enabled = true;
static unsigned sender_calls, sender_capability_reads;
static unsigned sender_unknown_on_peer;
static ClusterCtrcOriginEntry sender_foreign_request;
static ClusterHeapFreezeReportV2 sender_report;
static uint8 sender_wire[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];
static bool receiver_mode;
static void (*sender_peer_current_observer)(void);
static bool sender_high_peer_enabled;
static int32 sender_peer_observer_target;

/* Observe only the sender's original maintenance spinlock here. The shared
 * engine above keeps its original strict census-lock observer unchanged. */
static volatile slock_t *sender_maintenance_lock;
static bool sender_maintenance_held;
static unsigned sender_maintenance_acquires, sender_maintenance_releases;

static void
sender_spin_acquire(volatile slock_t *lock)
{
	UT_ASSERT(lock == sender_maintenance_lock);
	UT_ASSERT(!sender_maintenance_held && held_count == 0);
	UT_ASSERT(!native_lock_held && !native_cleaner_lock_held);
	S_LOCK(lock);
	sender_maintenance_held = true;
	sender_maintenance_acquires++;
}

static void
sender_spin_release(volatile slock_t *lock)
{
	UT_ASSERT(lock == sender_maintenance_lock && sender_maintenance_held);
	sender_maintenance_held = false;
	sender_maintenance_releases++;
	S_UNLOCK(lock);
}

#undef SpinLockAcquire
#undef SpinLockRelease
#define SpinLockAcquire(lock) sender_spin_acquire(lock)
#define SpinLockRelease(lock) sender_spin_release(lock)
#define cluster_undo_cleaner_active_sample_current native_cleaner_sample_current
#include "test_cluster_ctrc_horizon_sender.inc"
#undef cluster_undo_cleaner_active_sample_current

static ClusterUndoHorizonShmem sender_state;

bool
cluster_sf_peer_capability_word_sample(int32 peer, uint32 required,
									 uint32 *word, uint32 *generation)
{
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(!native_lock_held && !native_cleaner_lock_held && !sender_maintenance_held);
	UT_ASSERT(peer == 1 || (sender_high_peer_enabled && peer == 65)
			  || (receiver_mode && peer == 0));
	UT_ASSERT_EQ(required, PGRAC_IC_HELLO_HEAP_FREEZE_RETAINED_MASK);
	sender_capability_reads++;
	*word = required;
	*generation = test_capability_generation;
	return true;
}

bool
cluster_sf_peer_capability_generation_matches(int32 peer, uint32 required, uint32 generation)
{
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(!native_lock_held && !native_cleaner_lock_held && !sender_maintenance_held);
	UT_ASSERT(peer == 1 || (sender_high_peer_enabled && peer == 65)
			  || (receiver_mode && peer == 0));
	UT_ASSERT_EQ(required, PGRAC_IC_HELLO_HEAP_FREEZE_RETAINED_MASK);
	if (sender_peer_current_observer != NULL
		&& (sender_peer_observer_target == 0 || sender_peer_observer_target == peer)) {
		void (*observe)(void) = sender_peer_current_observer;

		sender_peer_current_observer = NULL;
		observe();
	}
	if (sender_unknown_on_peer != 0) {
		unsigned kind = sender_unknown_on_peer;

		sender_unknown_on_peer = 0;
		if (kind == 1) {
			ClusterCtrcReceiptHandle handle;

			UT_ASSERT_EQ(prepare_fixture_receipt(&sender_foreign_request, 901, &handle),
						 CLUSTER_CTRC_PREPARE_READY);
			UT_ASSERT(!FullTransactionIdIsValid(handle.original_full_xid));
		} else {
			ClusterCtrcLocalReleaseAckV1 ack;
			uint16 reason;

			UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
				&sender_foreign_request.key, &sender_foreign_request.touched[0],
				sender_foreign_request.grant_generation, 1, CTRC_SEAL_CLOSE_AND_CLEAN,
				&reason, &ack), CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK);
		}
	}
	return generation == test_capability_generation;
}

/* This source-handoff fixture keeps H as its declared controlled boundary;
 * native per-boot coverage is executed by retention_sample/undo_horizon. */
bool
cluster_undo_retention_horizon_covers(SCN horizon)
{
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(!native_lock_held && !native_cleaner_lock_held && !sender_maintenance_held);
	return SCN_VALID(horizon) && horizon != (SCN)UINT64_MAX;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 destination, const void *payload, uint32 length)
{
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(!native_lock_held && !native_cleaner_lock_held && !sender_maintenance_held);
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_UNDO_HORIZON);
	UT_ASSERT_EQ(destination, 1);
	UT_ASSERT_EQ(length, CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES);
	UT_ASSERT(cluster_heap_freeze_report_v2_decode(payload, length, &sender_report));
	memcpy(sender_wire, payload, length);
	sender_calls++;
	return CLUSTER_IC_SEND_DONE;
}

static void
finish_original_worker_pass(void)
{
	unsigned slices = 0;

	do {
		undo_cleaner_sample_active_pass();
		UT_ASSERT(++slices <= 1000);
	} while (undo_cleaner_active_scan_pending && slices <= 1000);
	UT_ASSERT_EQ(undo_cleaner_active_scan.status, CTRC_ACTIVE_SCAN_COMPLETE);
	UT_ASSERT(!undo_cleaner_active_scan_pending);
}

static ClusterUndoCleanerActiveSample
original_sender_source(void)
{
	ClusterUndoCleanerActiveSample active;
	const ClusterCtrcOriginEntry *origin;
	uint64 local_index, foreign_index;

	sender_high_peer_enabled = false;
	sender_peer_observer_target = 0;

	/* Original native exit follows exact local origin/receipt installation;
	 * two actual worker passes have retained that older FullXid. */
	exercise_worker_active_pass(0);
	native_source_exit = native_publish_source = native_pass_mode = true;
	UT_ASSERT_EQ(native_xids[0], InvalidTransactionId);
	native_pass_source.admitted_members_lo |= UINT64CONST(1) << 1;
	native_pass_source.admitted_incarnation[1] = 1102;
	/* Rebuild under the changed original cohort before any report. Never
	 * retrofit a peer onto an already completed sample's admission image. */
	finish_original_worker_pass();
	UT_ASSERT(native_cleaner_sample(&active));
	UT_ASSERT(native_cleaner_sample_current(&active));
	UT_ASSERT_EQ(active.scan.unqualified_entries, 0);
	UT_ASSERT(FullTransactionIdEquals(active.scan.min_full, test_top_full));
	UT_ASSERT(FullTransactionIdPrecedes(active.scan.min_full, active.scan.reference_full));
	origin = &ctrc_origin_entries()[0];
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_OPEN);
	UT_ASSERT(FullTransactionIdEquals(origin->original_full_xid, active.scan.min_full));
	/* A remote request carries no authenticated original epoch even if a
	 * caller-side object happened to retain this local FullXid field. */
	sender_foreign_request = *origin;
	sender_foreign_request.key.owner_instance = 2;
	sender_foreign_request.key.origin_node_id = 1;
	sender_foreign_request.key.segment_id = CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	sender_foreign_request.key.origin_boot_incarnation = 1102;
	/* The existing participant table indexes physical local slot ordinals.
	 * Keep the original protected participant and choose a distinct free slot
	 * for the foreign producer, rather than requesting a conflicting reuse. */
	sender_foreign_request.key.slot_offset = 1;
	UT_ASSERT(ctrc_participant_index(&origin->key, 0, &local_index));
	UT_ASSERT(ctrc_participant_index(&sender_foreign_request.key, 0, &foreign_index));
	UT_ASSERT_NE(local_index, foreign_index);
	memset(&sender_state, 0, sizeof(sender_state));
	SpinLockInit(&sender_state.maintenance_lock);
	sender_maintenance_lock = &sender_state.maintenance_lock;
	sender_maintenance_held = false;
	sender_maintenance_acquires = sender_maintenance_releases = 0;
	UndoHorizonShmem = &sender_state;
	sender_calls = sender_capability_reads = sender_unknown_on_peer = 0;
	return active;
}

static void
send_original_sample(const ClusterUndoCleanerActiveSample *active)
{
	const ClusterUndoRetentionSample history = {
		.horizon_scn = 400, .clock_scn = 700,
		.contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT,
		.contributor_token = 9
	};
	unsigned reads = native_future_reads, captures = native_pass_captures;
	unsigned census_locks = conditional_calls;
	ClusterUndoCleanerActiveSample before = *active;

	undo_horizon_send_v2(1, active, &history);
	UT_ASSERT_EQ(native_future_reads, reads);
	UT_ASSERT_EQ(native_pass_captures, captures);
	UT_ASSERT_EQ(conditional_calls, census_locks);
	UT_ASSERT(memcmp(active, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(native_cleaner_lock_acquires, native_cleaner_lock_releases);
	UT_ASSERT_EQ(native_lock_acquires, native_lock_releases);
	UT_ASSERT(!sender_maintenance_held);
	UT_ASSERT_EQ(sender_maintenance_acquires, sender_maintenance_releases);
}

UT_TEST(test_original_protection_exit_and_complete_worker_reach_v2_wire)
{
	ClusterUndoCleanerActiveSample active = original_sender_source(), incomplete, complete;

	send_original_sample(&active);
	UT_ASSERT_EQ(sender_calls, 1);
	UT_ASSERT_EQ(sender_state.report_sequence, 1);
	UT_ASSERT_EQ(sender_report.f_sample_id, active.sample_id);
	UT_ASSERT_EQ(sender_report.active_full_xid_min, U64FromFullTransactionId(test_top_full));
	UT_ASSERT_EQ(sender_report.future_full_xid, U64FromFullTransactionId(active.scan.future_full));
	UT_ASSERT_EQ(sender_report.origin, active.admission_fence.local_node_id);
	UT_ASSERT_EQ(sender_report.incarnation, active.admission_fence.local_boot_incarnation);
	UT_ASSERT_EQ(sender_report.horizon_scn, 400);
	UT_ASSERT_EQ(sender_report.h_sample_clock_scn, 700);
	UT_ASSERT_EQ(sender_report.h_contributor_token, 9);

	native_future = FullTransactionIdFromU64(U64FromFullTransactionId(native_future) + 16);
	undo_cleaner_sample_active_pass();
	UT_ASSERT(undo_cleaner_active_scan_pending);
	UT_ASSERT(native_cleaner_sample(&incomplete));
	UT_ASSERT(memcmp(&active, &incomplete, sizeof(active)) == 0);
	send_original_sample(&incomplete);
	UT_ASSERT_EQ(sender_calls, 2);
	UT_ASSERT_EQ(sender_report.f_sample_id, active.sample_id);
	finish_original_worker_pass();
	UT_ASSERT(native_cleaner_sample(&complete));
	UT_ASSERT_EQ(complete.sample_id, active.sample_id + 1);
	send_original_sample(&complete);
	UT_ASSERT_EQ(sender_calls, 3);
	UT_ASSERT_EQ(sender_state.report_sequence, 3);
	UT_ASSERT_EQ(sender_report.f_sample_id, complete.sample_id);
	UT_ASSERT_EQ(sender_report.active_full_xid_min, U64FromFullTransactionId(test_top_full));
	UT_ASSERT_EQ(sender_report.future_full_xid, U64FromFullTransactionId(native_future));
}

static void
exercise_unknown_producer_handoff(unsigned kind)
{
	ClusterUndoCleanerActiveSample active = original_sender_source(), refreshed;
	unsigned capability_reads;

	send_original_sample(&active);
	UT_ASSERT_EQ(sender_calls, 1);
	sender_unknown_on_peer = kind;
	send_original_sample(&active);
	UT_ASSERT_EQ(sender_unknown_on_peer, 0);
	UT_ASSERT(!cluster_ctrc_active_scan_registration_current(&active.scan));
	UT_ASSERT(!native_cleaner_sample_current(&active));
	UT_ASSERT_EQ(sender_calls, 1);
	UT_ASSERT_EQ(sender_state.report_sequence, 1);
	finish_original_worker_pass();
	UT_ASSERT(native_cleaner_sample(&refreshed));
	UT_ASSERT(native_cleaner_sample_current(&refreshed));
	UT_ASSERT_EQ(refreshed.sample_id, active.sample_id + 1);
	UT_ASSERT_EQ(refreshed.scan.unqualified_entries, 1);
	UT_ASSERT(FullTransactionIdEquals(refreshed.scan.min_full, test_top_full));
	capability_reads = sender_capability_reads;
	send_original_sample(&refreshed);
	UT_ASSERT_EQ(sender_calls, 1);
	UT_ASSERT_EQ(sender_state.report_sequence, 1);
	UT_ASSERT_EQ(sender_capability_reads, capability_reads);
}

UT_TEST(test_actual_foreign_receipt_revokes_old_report_and_blocks_new_unqualified_report)
{
	exercise_unknown_producer_handoff(1);
}

UT_TEST(test_actual_foreign_close_revokes_old_report_and_blocks_new_unqualified_report)
{
	exercise_unknown_producer_handoff(2);
}

/* The original lifecycle helper publishes at three existing completed cuts:
 * after native exit, after receiver certificate retirement but before its
 * exact final reply, and after the original owner finally reclaims its row. */
static unsigned sender_retirement_reports;
static uint64 sender_retirement_last_sample_id;
static FullTransactionId sender_retained_original;
static void (*sender_retirement_observer)(const ClusterUndoCleanerActiveSample *);

static void
observe_original_retirement_publication(const ClusterUndoCleanerActiveSample *published)
{
	ClusterUndoCleanerActiveSample active;
	const ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[0];
	bool previous_pass_mode = native_pass_mode;

	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(!native_cleaner_lock_held && !native_lock_held);
	UT_ASSERT_EQ(native_xids[0], InvalidTransactionId);
	UT_ASSERT(sender_retirement_reports < 3);
	if (sender_retirement_reports == 0) {
		sender_retained_original = published->scan.min_full;
		UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_OPEN);
	} else if (sender_retirement_reports == 1) {
		UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_RELEASE_PROVEN);
		UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_entries()[0],
								 sizeof(ClusterCtrcParticipantEntry)));
	} else
		UT_ASSERT(ctrc_bytes_zero(origin, sizeof(*origin)));

	/* The same required cohort was fixed before the original lifecycle's
	 * first sample. Start a fresh worker pass under that exact source. */
	native_pass_source = published->admission_fence;
	UT_ASSERT((native_pass_source.admitted_members_lo & (UINT64CONST(1) << 1)) != 0);
	UT_ASSERT_EQ(native_pass_source.admitted_incarnation[1], 1102);
	native_pass_mode = true;
	native_pass_capture_allowed = native_pass_lease_valid = native_pass_admission_valid = true;
	finish_original_worker_pass();
	UT_ASSERT(native_cleaner_sample(&active));
	UT_ASSERT(native_cleaner_sample_current(&active));
	UT_ASSERT(active.sample_id > sender_retirement_last_sample_id);
	sender_retirement_last_sample_id = active.sample_id;
	UT_ASSERT_EQ(active.scan.unqualified_entries, 0);
	UT_ASSERT(FullTransactionIdEquals(active.scan.min_full,
		sender_retirement_reports < 2 ? sender_retained_original : native_future));
	send_original_sample(&active);
	UT_ASSERT_EQ(sender_calls, sender_retirement_reports + 1);
	UT_ASSERT_EQ(sender_state.report_sequence, sender_calls);
	UT_ASSERT_EQ(sender_report.f_sample_id, active.sample_id);
	UT_ASSERT_EQ(sender_report.active_full_xid_min, U64FromFullTransactionId(active.scan.min_full));
	UT_ASSERT_EQ(sender_report.future_full_xid, U64FromFullTransactionId(native_future));
	sender_retirement_reports++;
	native_pass_mode = previous_pass_mode;
}

static void
exercise_original_retirement_reports(bool applied)
{
	memset(&sender_state, 0, sizeof(sender_state));
	SpinLockInit(&sender_state.maintenance_lock);
	sender_maintenance_lock = &sender_state.maintenance_lock;
	sender_maintenance_held = false;
	sender_maintenance_acquires = sender_maintenance_releases = 0;
	UndoHorizonShmem = &sender_state;
	sender_calls = sender_capability_reads = sender_unknown_on_peer = 0;
	sender_retirement_reports = 0;
	sender_retirement_last_sample_id = 0;
	sender_retained_original = InvalidFullTransactionId;
	native_source_exit = native_publish_source = true;
	native_published_samples = 0;
	original_retention_report_peer = true;
	original_worker_publication_observer = sender_retirement_observer != NULL
		? sender_retirement_observer : observe_original_retirement_publication;
	exercise_original_producer_retention_lifecycle(applied);
	original_worker_publication_observer = NULL;
	original_retention_report_peer = false;
	UT_ASSERT_EQ(native_published_samples, 3);
	UT_ASSERT_EQ(sender_retirement_reports, 3);
	UT_ASSERT_EQ(sender_calls, 3);
	UT_ASSERT_EQ(sender_state.report_sequence, 3);
	UT_ASSERT(FullTransactionIdPrecedes(sender_retained_original, native_future));
	UT_ASSERT_EQ(sender_report.active_full_xid_min, U64FromFullTransactionId(native_future));
	UT_ASSERT_EQ(sender_maintenance_acquires, sender_maintenance_releases);
	UT_ASSERT_EQ(held_count, 0);
	native_source_exit = native_publish_source = false;
}

UT_TEST(test_cancelled_receipt_report_retains_source_until_exact_final_certificate_reply)
{
	exercise_original_retirement_reports(false);
}

UT_TEST(test_applied_receipt_report_retains_source_until_exact_final_certificate_reply)
{
	exercise_original_retirement_reports(true);
}

/* Compose the original H source, using the same PGPROC whose original
 * native protection is transferred above. Clock and lock scheduling remain
 * fixture leaves; the ProcArray scan, fold and output clearing are product. */
static SCN sender_sample_clock;
static unsigned sender_history_reads;
static ClusterUndoRetentionSample sender_pre_exit_history;

static SCN
sender_history_clock(void)
{
	UT_ASSERT(native_lock_held && native_held_lock == native_procarray_lock);
	UT_ASSERT_EQ(native_lock_mode, LW_EXCLUSIVE);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(!native_cleaner_lock_held && !sender_maintenance_held);
	sender_history_reads++;
	return sender_sample_clock;
}

static bool cohort_no_mrp(void);
static SCN cohort_unavailable_mrp_bound(void);
#define cluster_mrp_should_start cohort_no_mrp
#define cluster_mrp_standby_consistent_scn cohort_unavailable_mrp_bound
#define MainLWLockArray native_locks
#define allProcs (native_procs.allProcs)
#define cluster_scn_current sender_history_clock
#include "test_cluster_retention_sample.inc"
#undef cluster_mrp_standby_consistent_scn
#undef cluster_mrp_should_start
#undef cluster_scn_current
#undef allProcs
#undef MainLWLockArray

static void
sample_original_history_before_exit(void)
{
	/* The original native fixture supplies the transaction fields. Supply
	 * this live backend's PID before sampling its snapshot attribution. A
	 * zero-PID dummy correctly remains UNKNOWN in the original sampler. */
	UT_ASSERT(MyProcPid > 0);
	test_my_proc.pid = MyProcPid;
	UT_ASSERT(TransactionIdIsValid(test_my_proc.xmin));
	UT_ASSERT_EQ(pg_atomic_read_u64(&test_my_proc.cluster_read_scn_atomic), 17);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&sender_pre_exit_history));
	UT_ASSERT_EQ(sender_pre_exit_history.horizon_scn, 17);
	UT_ASSERT_EQ(sender_pre_exit_history.clock_scn, sender_sample_clock);
	UT_ASSERT_EQ(sender_pre_exit_history.contributor_kind,
		CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sender_pre_exit_history.contributor_token, 1);
}

static void
assert_actual_history_report(const ClusterUndoCleanerActiveSample *active,
							const ClusterUndoRetentionSample *history, unsigned expected_calls)
{
	unsigned reads = sender_history_reads;

	undo_horizon_send_v2(1, active, history);
	UT_ASSERT_EQ(sender_history_reads, reads);
	UT_ASSERT_EQ(sender_calls, expected_calls);
	UT_ASSERT_EQ(sender_state.report_sequence, expected_calls);
	UT_ASSERT_EQ(sender_report.f_sample_id, active->sample_id);
	UT_ASSERT_EQ(sender_report.active_full_xid_min, U64FromFullTransactionId(active->scan.min_full));
	UT_ASSERT_EQ(sender_report.horizon_scn, history->horizon_scn);
	UT_ASSERT_EQ(sender_report.h_sample_clock_scn, history->clock_scn);
	UT_ASSERT_EQ(sender_report.h_contributor_kind, history->contributor_kind);
	UT_ASSERT_EQ(sender_report.h_contributor_token, history->contributor_token);
	UT_ASSERT_EQ(native_lock_acquires, native_lock_releases);
	UT_ASSERT_EQ(native_cleaner_lock_acquires, native_cleaner_lock_releases);
	UT_ASSERT_EQ(sender_maintenance_acquires, sender_maintenance_releases);
	UT_ASSERT(!native_lock_held && !native_cleaner_lock_held && !sender_maintenance_held);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_original_snapshot_then_native_exit_clock_h_reach_retained_f_report)
{
	ClusterUndoCleanerActiveSample active;
	ClusterUndoRetentionSample history;

	sender_sample_clock = 700;
	sender_history_reads = 0;
	original_worker_pre_exit_observer = sample_original_history_before_exit;
	active = original_sender_source();
	original_worker_pre_exit_observer = NULL;
	UT_ASSERT_EQ(sender_history_reads, 1);
	UT_ASSERT_EQ(test_my_proc.xmin, InvalidTransactionId);
	UT_ASSERT_EQ(pg_atomic_read_u64(&test_my_proc.cluster_read_scn_atomic), 0);
	assert_actual_history_report(&active, &sender_pre_exit_history, 1);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&history));
	UT_ASSERT_EQ(sender_history_reads, 2);
	UT_ASSERT_EQ(history.horizon_scn, sender_sample_clock);
	UT_ASSERT_EQ(history.clock_scn, sender_sample_clock);
	UT_ASSERT_EQ(history.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	UT_ASSERT_EQ(history.contributor_token, 0);
	assert_actual_history_report(&active, &history, 2);
}

UT_TEST(test_actual_replication_h_refusal_cannot_become_v2_report_or_sequence_debit)
{
	ClusterUndoCleanerActiveSample active = original_sender_source();
	ClusterUndoRetentionSample history, zero = {0};

	sender_sample_clock = 700;
	sender_history_reads = 0;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&history));
	assert_actual_history_report(&active, &history, 1);
	for (unsigned kind = 0; kind < 2; kind++) {
		TransactionId *pin = kind == 0 ? &procArray->replication_slot_xmin
			: &procArray->replication_slot_catalog_xmin;
		unsigned calls = sender_calls;

		*pin = XidFromFullTransactionId(active.scan.min_full);
		memset(&history, 0xa5, sizeof(history));
		UT_ASSERT(!cluster_undo_retention_horizon_sample(&history));
		UT_ASSERT(memcmp(&history, &zero, sizeof(history)) == 0);
		/* The original production caller withholds a failed H sample. Even
		 * direct entry cannot encode its cleared output into a V2 report. */
		undo_horizon_send_v2(1, &active, &history);
		UT_ASSERT_EQ(sender_calls, calls);
		UT_ASSERT_EQ(sender_state.report_sequence, calls);
		UT_ASSERT(native_cleaner_sample_current(&active));
		*pin = InvalidTransactionId;
		UT_ASSERT(cluster_undo_retention_horizon_sample(&history));
		UT_ASSERT_EQ(history.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
		assert_actual_history_report(&active, &history, calls + 1);
	}
	UT_ASSERT_EQ(sender_history_reads, 5);
}

UT_TEST(test_actual_worker_restart_withholds_report_until_new_complete_scan)
{
	ClusterUndoCleanerActiveSample active = original_sender_source(), restarted;
	ClusterUndoRetentionSample history;
	ClusterHeapFreezeReportV2 accepted;

	sender_sample_clock = 700;
	sender_history_reads = 0;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&history));
	assert_actual_history_report(&active, &history, 1);
	memcpy(&accepted, &sender_report, sizeof(accepted));
	native_future = FullTransactionIdFromU64(U64FromFullTransactionId(native_future) + 16);
	undo_cleaner_publish_status(UNDO_CLEANER_SPAWNING);
	for (unsigned phase = 0; phase < 2; phase++) {
		if (phase == 1)
			undo_cleaner_publish_status(UNDO_CLEANER_READY);
		UT_ASSERT_EQ(native_cleaner_state.workers[0].pid, active.worker_pid);
		/* The original registration cut remains current; a matching PID and
		 * that cut cannot replace the retired worker publication. */
		UT_ASSERT(cluster_ctrc_active_scan_registration_current(&active.scan));
		UT_ASSERT(!native_cleaner_sample_current(&active));
		undo_horizon_send_v2(1, &active, &history);
		UT_ASSERT_EQ(sender_calls, 1);
		UT_ASSERT_EQ(sender_state.report_sequence, 1);
		UT_ASSERT(memcmp(&sender_report, &accepted, sizeof(accepted)) == 0);
	}
	finish_original_worker_pass();
	UT_ASSERT(native_cleaner_sample(&restarted));
	UT_ASSERT(native_cleaner_sample_current(&restarted));
	UT_ASSERT_EQ(restarted.worker_pid, active.worker_pid);
	UT_ASSERT_EQ(restarted.sample_id, active.sample_id + 1);
	UT_ASSERT(FullTransactionIdEquals(restarted.scan.future_full, native_future));
	UT_ASSERT(FullTransactionIdEquals(restarted.scan.min_full, active.scan.min_full));
	UT_ASSERT(!native_cleaner_sample_current(&active));
	UT_ASSERT(cluster_undo_retention_horizon_sample(&history));
	assert_actual_history_report(&restarted, &history, 2);
}

static ClusterUndoCleanerActiveSample sender_respawn_source;

static void
restart_original_worker_at_peer_check(void)
{
	UT_ASSERT(!native_cleaner_lock_held && !native_lock_held && held_count == 0);
	UT_ASSERT(!sender_maintenance_held);
	exercise_same_pid_worker_respawn(&sender_respawn_source,
		&sender_respawn_source.admission_fence);
}

UT_TEST(test_actual_same_pid_respawn_during_peer_check_rejects_old_sample)
{
	ClusterUndoCleanerActiveSample active = original_sender_source(), restarted;
	ClusterUndoRetentionSample history;
	ClusterHeapFreezeReportV2 accepted;

	sender_sample_clock = 700;
	sender_history_reads = 0;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&history));
	assert_actual_history_report(&active, &history, 1);
	memcpy(&accepted, &sender_report, sizeof(accepted));
	sender_respawn_source = active;
	native_future = FullTransactionIdFromU64(U64FromFullTransactionId(native_future) + 16);
	native_worker_respawn = true;
	native_worker_respawns = 0;
	sender_peer_current_observer = restart_original_worker_at_peer_check;
	undo_horizon_send_v2(1, &active, &history);
	native_worker_respawn = false;
	UT_ASSERT(sender_peer_current_observer == NULL);
	UT_ASSERT_EQ(native_worker_respawns, 1);
	UT_ASSERT_EQ(sender_calls, 1);
	UT_ASSERT_EQ(sender_state.report_sequence, 1);
	UT_ASSERT(memcmp(&sender_report, &accepted, sizeof(accepted)) == 0);
	UT_ASSERT(cluster_ctrc_active_scan_registration_current(&active.scan));
	UT_ASSERT(!native_cleaner_sample_current(&active));
	UT_ASSERT(native_cleaner_sample(&restarted));
	UT_ASSERT(native_cleaner_sample_current(&restarted));
	UT_ASSERT_EQ(restarted.worker_pid, active.worker_pid);
	UT_ASSERT_EQ(restarted.sample_id, active.sample_id + 1);
	UT_ASSERT(FullTransactionIdEquals(restarted.scan.future_full, native_future));
	UT_ASSERT(FullTransactionIdEquals(restarted.scan.min_full, active.scan.min_full));
	UT_ASSERT(cluster_undo_retention_horizon_sample(&history));
	assert_actual_history_report(&restarted, &history, 2);
}

/* The original native/census/H producers above now feed the actual fixed
 * cohort and candidate consumers. Peer report contents and the clock remain
 * explicit controlled leaves, not authenticated remote retention evidence. */
static TimestampTz cohort_now_us;

static TimestampTz
cohort_timestamp(void)
{
	UT_ASSERT(!native_cleaner_lock_held && !native_lock_held && held_count == 0);
	UT_ASSERT(!sender_maintenance_held);
	return cohort_now_us;
}

static bool
cohort_no_mrp(void)
{
	return false;
}

static SCN
cohort_unavailable_mrp_bound(void)
{
	UT_ASSERT(false);
	return InvalidScn;
}

#define GetCurrentTimestamp cohort_timestamp
#define cluster_mrp_should_start cohort_no_mrp
#define cluster_mrp_standby_consistent_scn cohort_unavailable_mrp_bound
#define cluster_semantic_activation_active_sample_capture native_cleaner_admission_capture
#define cluster_semantic_activation_active_sample_current native_cleaner_admission_current
#define cluster_undo_cleaner_active_sample native_cleaner_sample
#define cluster_undo_cleaner_active_sample_current native_cleaner_sample_current
#include "test_cluster_ctrc_horizon_cohort.inc"
#undef cluster_undo_cleaner_active_sample_current
#undef cluster_undo_cleaner_active_sample
#undef cluster_semantic_activation_active_sample_current
#undef cluster_semantic_activation_active_sample_capture
#undef cluster_mrp_standby_consistent_scn
#undef cluster_mrp_should_start
#undef GetCurrentTimestamp

static void
install_controlled_peer_report(const ClusterUndoCleanerActiveSample *active)
{
	ClusterHeapFreezeReportV2 report = sender_report;

	cohort_now_us = 10000000;
	pg_atomic_init_u32(&sender_state.freeze_watermark_seq, 0);
	for (unsigned i = 0; i < CLUSTER_MAX_NODES; i++)
		pg_atomic_init_u32(&sender_state.freeze_slots[i].seq, 0);
	/* This peer input is synthetic. Keep that boundary distinct from the
	 * local report, whose F/H came from the original producer functions. */
	report.origin = 1;
	report.incarnation = active->admission_fence.admitted_incarnation[1];
	report.future_full_xid = U64FromFullTransactionId(active->scan.future_full) + 33;
	report.active_full_xid_min = U64FromFullTransactionId(active->scan.future_full);
	report.horizon_scn = 300;
	report.h_sample_clock_scn = 700;
	report.h_contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT;
	report.h_contributor_token = 99;
	undo_horizon_v2_publish(&sender_state.freeze_slots[1], &report,
		&active->admission_fence, test_capability_generation, cohort_now_us, false);
}

static ClusterHeapFreezeCohortObservation cohort_last_observation;
static UndoHorizonV2MaintenanceObservation cohort_last_maintenance;

static ClusterHeapFreezeWatermark
observe_original_source_and_publish(const ClusterUndoCleanerActiveSample *active,
									const ClusterUndoRetentionSample *history)
{
	ClusterHeapFreezeCohortObservation cohort;
	UndoHorizonV2MaintenanceObservation observed;
	ClusterHeapFreezeWatermark watermark;
	unsigned reads = sender_history_reads, futures = native_future_reads;

	UT_ASSERT_EQ(undo_horizon_v2_cohort_observe(&cohort, history, &observed),
		CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(cohort.source_local_sample_id, active->sample_id);
	UT_ASSERT(memcmp(&cohort.local_registration_cut, &active->scan, sizeof(active->scan)) == 0);
	UT_ASSERT_EQ(cohort.active_full_xid_min, U64FromFullTransactionId(active->scan.min_full));
	UT_ASSERT_EQ(cohort.future_full_xid_max,
		sender_state.freeze_slots[1].view.report.future_full_xid);
	UT_ASSERT_EQ(cohort.source_local_horizon_scn, history->horizon_scn);
	UT_ASSERT_EQ(cohort.source_local_history.sample_clock_scn, history->clock_scn);
	UT_ASSERT_EQ(cohort.source_local_history.contributor_kind, history->contributor_kind);
	UT_ASSERT_EQ(cohort.source_local_history.contributor_token, history->contributor_token);
	UT_ASSERT_EQ(cohort.source_peer_horizon_scn[1], 300);
	UT_ASSERT_EQ(cohort.horizon_scn, 300);
	UT_ASSERT(undo_horizon_publish_freeze_watermark(&cohort, &observed, cohort_now_us));
	memcpy(&cohort_last_observation, &cohort, sizeof(cohort));
	memcpy(&cohort_last_maintenance, &observed, sizeof(observed));
	UT_ASSERT(cluster_heap_freeze_watermark_sample(&watermark));
	UT_ASSERT_EQ(watermark.f_sample_id, active->sample_id);
	UT_ASSERT_EQ(watermark.active_full_xid_min, U64FromFullTransactionId(active->scan.min_full));
	UT_ASSERT_EQ(watermark.horizon_scn, 300);
	UT_ASSERT_EQ(sender_history_reads, reads);
	UT_ASSERT_EQ(native_future_reads, futures);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(native_lock_acquires, native_lock_releases);
	UT_ASSERT_EQ(native_cleaner_lock_acquires, native_cleaner_lock_releases);
	UT_ASSERT_EQ(sender_maintenance_acquires, sender_maintenance_releases);
	return watermark;
}

UT_TEST(test_original_producers_reach_cohort_and_candidate_without_resampling)
{
	ClusterUndoCleanerActiveSample active = original_sender_source();
	ClusterUndoRetentionSample history;
	ClusterHeapFreezeWatermark watermark;

	sender_sample_clock = 700;
	sender_history_reads = 0;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&history));
	assert_actual_history_report(&active, &history, 1);
	install_controlled_peer_report(&active);
	watermark = observe_original_source_and_publish(&active, &history);
	UT_ASSERT_EQ(watermark.publication_sequence, 1);
	UT_ASSERT_EQ(sender_history_reads, 1);
}

UT_TEST(test_actual_late_receipt_revokes_cohort_and_clears_candidate_read)
{
	ClusterUndoCleanerActiveSample active = original_sender_source();
	ClusterUndoRetentionSample history;
	ClusterHeapFreezeCohortObservation cohort, unchanged;
	ClusterHeapFreezeWatermark old, out, zero = {0};
	uint32 seq;

	sender_sample_clock = 700;
	sender_history_reads = 0;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&history));
	assert_actual_history_report(&active, &history, 1);
	install_controlled_peer_report(&active);
	old = observe_original_source_and_publish(&active, &history);
	seq = pg_atomic_read_u32(&sender_state.freeze_watermark_seq);
	memset(&cohort, 0xa5, sizeof(cohort));
	memcpy(&unchanged, &cohort, sizeof(unchanged));
	sender_unknown_on_peer = 1;
	UT_ASSERT_EQ(undo_horizon_v2_cohort_observe(&cohort, &history, NULL),
		CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED);
	UT_ASSERT_EQ(sender_unknown_on_peer, 0);
	UT_ASSERT(memcmp(&cohort, &unchanged, sizeof(cohort)) == 0);
	UT_ASSERT(!native_cleaner_sample_current(&active));
	UT_ASSERT(!undo_horizon_publish_freeze_watermark(&cohort_last_observation,
		&cohort_last_maintenance, cohort_now_us));
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_heap_freeze_watermark_sample(&out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT(memcmp(&old, &sender_state.freeze_watermark, sizeof(old)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&sender_state.freeze_watermark_seq), seq);
}

UT_TEST(test_actual_peer_callback_respawn_requires_fresh_cohort_publication)
{
	ClusterUndoCleanerActiveSample active = original_sender_source(), restarted;
	ClusterUndoRetentionSample history;
	ClusterHeapFreezeCohortObservation cohort, unchanged;
	ClusterHeapFreezeWatermark old, out, zero = {0};
	uint32 seq;

	sender_sample_clock = 700;
	sender_history_reads = 0;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&history));
	assert_actual_history_report(&active, &history, 1);
	install_controlled_peer_report(&active);
	old = observe_original_source_and_publish(&active, &history);
	seq = pg_atomic_read_u32(&sender_state.freeze_watermark_seq);
	sender_respawn_source = active;
	native_future = FullTransactionIdFromU64(U64FromFullTransactionId(native_future) + 16);
	native_worker_respawn = true;
	native_worker_respawns = 0;
	sender_peer_current_observer = restart_original_worker_at_peer_check;
	memset(&cohort, 0xa5, sizeof(cohort));
	memcpy(&unchanged, &cohort, sizeof(unchanged));
	UT_ASSERT_EQ(undo_horizon_v2_cohort_observe(&cohort, &history, NULL),
		CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED);
	native_worker_respawn = false;
	UT_ASSERT(sender_peer_current_observer == NULL);
	UT_ASSERT_EQ(native_worker_respawns, 1);
	UT_ASSERT(memcmp(&cohort, &unchanged, sizeof(cohort)) == 0);
	UT_ASSERT(!undo_horizon_publish_freeze_watermark(&cohort_last_observation,
		&cohort_last_maintenance, cohort_now_us));
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_heap_freeze_watermark_sample(&out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT(memcmp(&old, &sender_state.freeze_watermark, sizeof(old)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&sender_state.freeze_watermark_seq), seq);
	UT_ASSERT(native_cleaner_sample(&restarted));
	UT_ASSERT(native_cleaner_sample_current(&restarted));
	UT_ASSERT_EQ(restarted.worker_pid, active.worker_pid);
	UT_ASSERT_EQ(restarted.sample_id, active.sample_id + 1);
	out = observe_original_source_and_publish(&restarted, &history);
	UT_ASSERT_EQ(out.publication_sequence, old.publication_sequence + 1);
}


/* Peer 65 is a controlled remote input, not an authenticated producer. Rebuild
 * the actual local scan under this fixed admission before making any report. */
static ClusterUndoCleanerActiveSample
original_high_peer_source(void)
{
	ClusterUndoCleanerActiveSample previous = original_sender_source(), active;

	sender_high_peer_enabled = true;
	native_pass_source.admitted_members_hi |= UINT64CONST(1) << (65 - 64);
	native_pass_source.admitted_incarnation[65] = 1165;
	finish_original_worker_pass();
	UT_ASSERT(native_cleaner_sample(&active));
	UT_ASSERT(native_cleaner_sample_current(&active));
	UT_ASSERT(active.sample_id > previous.sample_id);
	UT_ASSERT_EQ(active.admission_fence.admitted_members_lo, 3);
	UT_ASSERT_EQ(active.admission_fence.admitted_members_hi, 2);
	UT_ASSERT(FullTransactionIdEquals(active.scan.min_full, previous.scan.min_full));
	return active;
}

static void
install_controlled_high_peer_report(const ClusterUndoCleanerActiveSample *active)
{
	ClusterHeapFreezeReportV2 report = sender_state.freeze_slots[1].view.report;

	report.origin = 65;
	report.incarnation = active->admission_fence.admitted_incarnation[65];
	report.future_full_xid = U64FromFullTransactionId(active->scan.future_full) + 65;
	report.active_full_xid_min = U64FromFullTransactionId(active->scan.min_full);
	report.horizon_scn = 250;
	report.h_contributor_token = 165;
	undo_horizon_v2_publish(&sender_state.freeze_slots[65], &report,
		&active->admission_fence, test_capability_generation, cohort_now_us, false);
}

static ClusterHeapFreezeWatermark
observe_high_cohort_and_publish(const ClusterUndoCleanerActiveSample *active,
							   const ClusterUndoRetentionSample *history, SCN expected_h)
{
	ClusterHeapFreezeCohortObservation cohort;
	UndoHorizonV2MaintenanceObservation observed;
	ClusterHeapFreezeWatermark watermark;
	unsigned reads = sender_history_reads, futures = native_future_reads;

	UT_ASSERT_EQ(undo_horizon_v2_cohort_observe(&cohort, history, &observed),
		CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(cohort.admitted_members_lo, 3);
	UT_ASSERT_EQ(cohort.admitted_members_hi, 2);
	UT_ASSERT_EQ(cohort.source_local_sample_id, active->sample_id);
	UT_ASSERT(memcmp(&cohort.local_registration_cut, &active->scan, sizeof(active->scan)) == 0);
	UT_ASSERT_EQ(cohort.active_full_xid_min, U64FromFullTransactionId(active->scan.min_full));
	UT_ASSERT_EQ(cohort.future_full_xid_max,
		sender_state.freeze_slots[65].view.report.future_full_xid);
	UT_ASSERT_EQ(cohort.source_local_horizon_scn, history->horizon_scn);
	UT_ASSERT_EQ(cohort.source_local_history.sample_clock_scn, history->clock_scn);
	UT_ASSERT_EQ(cohort.source_local_history.contributor_kind, history->contributor_kind);
	UT_ASSERT_EQ(cohort.source_local_history.contributor_token, history->contributor_token);
	UT_ASSERT_EQ(cohort.source_peers[65].seq,
		pg_atomic_read_u32(&sender_state.freeze_slots[65].seq));
	UT_ASSERT_NE(cohort.source_peers[65].seq, 0);
	UT_ASSERT_EQ(cohort.source_peers[65].seq % 2, 0);
	UT_ASSERT_EQ(cohort.source_peers[65].generation, test_capability_generation);
	UT_ASSERT_EQ(cohort.source_peer_history[65].sample_clock_scn, 700);
	UT_ASSERT_EQ(cohort.source_peer_history[65].contributor_kind,
		CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(cohort.source_peer_history[65].contributor_token, 165);
	UT_ASSERT_EQ(cohort.source_peer_horizon_scn[65], 250);
	UT_ASSERT_EQ(cohort.horizon_scn, expected_h);
	UT_ASSERT(undo_horizon_publish_freeze_watermark(&cohort, &observed, cohort_now_us));
	memcpy(&cohort_last_observation, &cohort, sizeof(cohort));
	memcpy(&cohort_last_maintenance, &observed, sizeof(observed));
	UT_ASSERT(cluster_heap_freeze_watermark_sample(&watermark));
	UT_ASSERT_EQ(watermark.admitted_members_lo, 3);
	UT_ASSERT_EQ(watermark.admitted_members_hi, 2);
	UT_ASSERT_EQ(watermark.f_sample_id, active->sample_id);
	UT_ASSERT_EQ(watermark.active_full_xid_min, U64FromFullTransactionId(active->scan.min_full));
	UT_ASSERT_EQ(watermark.horizon_scn, expected_h);
	UT_ASSERT_EQ(sender_history_reads, reads);
	UT_ASSERT_EQ(native_future_reads, futures);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(native_lock_acquires, native_lock_releases);
	UT_ASSERT_EQ(native_cleaner_lock_acquires, native_cleaner_lock_releases);
	UT_ASSERT_EQ(sender_maintenance_acquires, sender_maintenance_releases);
	return watermark;
}

UT_TEST(test_required_high_peer_missing_cannot_be_dropped_from_actual_cohort)
{
	ClusterUndoCleanerActiveSample active = original_high_peer_source();
	ClusterUndoRetentionSample history;
	ClusterHeapFreezeCohortObservation cohort, unchanged;
	ClusterHeapFreezeWatermark out, zero = {0};

	sender_sample_clock = 700;
	sender_history_reads = 0;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&history));
	assert_actual_history_report(&active, &history, 1);
	install_controlled_peer_report(&active);
	memset(&cohort, 0xa5, sizeof(cohort));
	memcpy(&unchanged, &cohort, sizeof(unchanged));
	UT_ASSERT_EQ(undo_horizon_v2_cohort_observe(&cohort, &history, NULL),
		CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE);
	UT_ASSERT(memcmp(&cohort, &unchanged, sizeof(cohort)) == 0);
	UT_ASSERT(native_cleaner_sample_current(&active));
	UT_ASSERT_EQ(sender_state.freeze_watermark.publication_sequence, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&sender_state.freeze_watermark_seq), 0);
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_heap_freeze_watermark_sample(&out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	install_controlled_high_peer_report(&active);
	out = observe_high_cohort_and_publish(&active, &history, 250);
	UT_ASSERT_EQ(out.publication_sequence, 1);
	sender_high_peer_enabled = false;
}

static void
republish_earlier_peer_report(void)
{
	ClusterHeapFreezeReportV2 report = sender_state.freeze_slots[1].view.report;

	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(!native_lock_held && !native_cleaner_lock_held && !sender_maintenance_held);
	report.horizon_scn = 200;
	report.report_sequence++;
	undo_horizon_v2_publish(&sender_state.freeze_slots[1], &report,
		&cohort_last_observation.admission_fence, test_capability_generation,
		cohort_now_us, false);
}

UT_TEST(test_late_high_peer_callback_revokes_earlier_peer_copy_before_candidate)
{
	ClusterUndoCleanerActiveSample active = original_high_peer_source();
	ClusterUndoRetentionSample history;
	ClusterHeapFreezeCohortObservation cohort, unchanged;
	ClusterHeapFreezeWatermark old, out, zero = {0};
	uint32 seq;

	sender_sample_clock = 700;
	sender_history_reads = 0;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&history));
	assert_actual_history_report(&active, &history, 1);
	install_controlled_peer_report(&active);
	install_controlled_high_peer_report(&active);
	old = observe_high_cohort_and_publish(&active, &history, 250);
	seq = pg_atomic_read_u32(&sender_state.freeze_watermark_seq);
	sender_peer_observer_target = 65;
	sender_peer_current_observer = republish_earlier_peer_report;
	memset(&cohort, 0xa5, sizeof(cohort));
	memcpy(&unchanged, &cohort, sizeof(unchanged));
	UT_ASSERT_EQ(undo_horizon_v2_cohort_observe(&cohort, &history, NULL),
		CLUSTER_HEAP_FREEZE_COHORT_BUSY_PUBLISH);
	UT_ASSERT(sender_peer_current_observer == NULL);
	UT_ASSERT(memcmp(&cohort, &unchanged, sizeof(cohort)) == 0);
	UT_ASSERT(native_cleaner_sample_current(&active));
	UT_ASSERT(!undo_horizon_publish_freeze_watermark(&cohort_last_observation,
		&cohort_last_maintenance, cohort_now_us));
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_heap_freeze_watermark_sample(&out));
	UT_ASSERT(memcmp(&out, &zero, sizeof(out)) == 0);
	UT_ASSERT(memcmp(&old, &sender_state.freeze_watermark, sizeof(old)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&sender_state.freeze_watermark_seq), seq);
	out = observe_high_cohort_and_publish(&active, &history, 200);
	UT_ASSERT_EQ(out.publication_sequence, old.publication_sequence + 1);
	sender_peer_observer_target = 0;
	sender_high_peer_enabled = false;
}

/* This second process view consumes the actual bytes emitted above. Its
 * ingress, private stream and semantic owner are explicit fixture leaves;
 * the handler, decoder, monotone checks and slot publication are original. */
static ClusterUndoHorizonShmem receiver_state;
static ClusterSemanticActiveSampleFence receiver_fence;
static ClusterICControlIngress receiver_ingress;
static bool receiver_ingress_available, receiver_source_current;
static unsigned receiver_stream_reads, receiver_stream_refuse_at, receiver_late_fault;

bool
cluster_ic_dispatch_control_ingress(const ClusterICEnvelope *env, ClusterICControlIngress *out)
{
	UT_ASSERT(receiver_mode && cluster_node_id == 1);
	UT_ASSERT_EQ(env->source_node_id, 0);
	UT_ASSERT_EQ(env->dest_node_id, 1);
	UT_ASSERT_EQ(held_count, 0);
	if (!receiver_ingress_available)
		return false;
	*out = receiver_ingress;
	return true;
}

bool
cluster_ic_tier1_stream_current(const ClusterICTier1Stream *stream)
{
	UT_ASSERT(receiver_mode && cluster_node_id == 1);
	UT_ASSERT_EQ(stream->serial, receiver_ingress.serial);
	UT_ASSERT_EQ(stream->epoch, receiver_ingress.epoch);
	UT_ASSERT_EQ(stream->owner_pid, MyProcPid);
	UT_ASSERT_EQ(stream->peer, 0);
	UT_ASSERT_EQ(stream->plane, CLUSTER_IC_PLANE_CONTROL);
	UT_ASSERT_EQ(stream->channel, -1);
	UT_ASSERT(!native_lock_held && !native_cleaner_lock_held && held_count == 0);
	receiver_stream_reads++;
	return receiver_stream_reads != receiver_stream_refuse_at;
}

static bool
receiver_source_capture(ClusterSemanticActiveSampleFence *out)
{
	UT_ASSERT(receiver_mode && cluster_node_id == 1);
	*out = receiver_fence;
	return true;
}

static bool
receiver_source_matches(const ClusterSemanticActiveSampleFence *fence)
{
	UT_ASSERT(receiver_mode && cluster_node_id == 1);
	return receiver_source_current && memcmp(fence, &receiver_fence, sizeof(*fence)) == 0;
}

#define GetCurrentTimestamp cohort_timestamp
#define cluster_semantic_activation_active_sample_capture receiver_source_capture
#define cluster_semantic_activation_active_sample_current receiver_source_matches
#include "test_cluster_ctrc_horizon_receive.inc"
#undef cluster_semantic_activation_active_sample_current
#undef cluster_semantic_activation_active_sample_capture
#undef GetCurrentTimestamp

static void
receiver_view(bool receiving)
{
	receiver_mode = receiving;
	cluster_node_id = receiving ? 1 : 0;
	UndoHorizonShmem = receiving ? &receiver_state : &sender_state;
}

static ClusterUndoCleanerActiveSample
start_original_receive(ClusterICEnvelope *env, ClusterUndoRetentionSample *history)
{
	ClusterUndoCleanerActiveSample active;

	receiver_view(false);
	active = original_sender_source();
	sender_sample_clock = 700;
	sender_history_reads = 0;
	UT_ASSERT(cluster_undo_retention_horizon_sample(history));
	assert_actual_history_report(&active, history, 1);
	memset(&receiver_state, 0, sizeof(receiver_state));
	pg_atomic_init_u64(&receiver_state.wire_reject_count, 0);
	for (unsigned i = 0; i < CLUSTER_MAX_NODES; i++)
		pg_atomic_init_u32(&receiver_state.freeze_slots[i].seq, 0);
	/* The receiver observes the same controlled formation from node1.
	 * Do not relabel the sender's origin, incarnation or encoded payload. */
	receiver_fence = active.admission_fence;
	receiver_fence.local_node_id = 1;
	receiver_fence.local_boot_incarnation = receiver_fence.admitted_incarnation[1];
	receiver_ingress = (ClusterICControlIngress){ .serial = 77,
		.epoch = sender_report.formation_epoch, .owner_pid = MyProcPid,
		.source_node = 0, .carrier = CLUSTER_IC_CONTROL_TCP };
	receiver_ingress_available = receiver_source_current = true;
	receiver_stream_reads = receiver_stream_refuse_at = receiver_late_fault = 0;
	cohort_now_us = 10000000;
	*env = (ClusterICEnvelope){ .msg_type = PGRAC_IC_MSG_UNDO_HORIZON,
		.source_node_id = 0, .dest_node_id = 1, .epoch = sender_report.formation_epoch,
		.payload_length = sizeof(sender_wire) };
	receiver_view(true);
	return active;
}

static void
next_original_receive_report(const ClusterUndoCleanerActiveSample *active,
							 const ClusterUndoRetentionSample *history)
{
	receiver_view(false);
	assert_actual_history_report(active, history, 2);
	receiver_view(true);
}

UT_TEST(test_actual_sender_bytes_reach_original_receive_without_origin_relabel)
{
	ClusterICEnvelope env;
	ClusterUndoRetentionSample history;
	ClusterUndoCleanerActiveSample active = start_original_receive(&env, &history);
	unsigned reads = sender_history_reads, future_reads = native_future_reads;
	ClusterHeapFreezeReportV2SlotShmem *slot = &receiver_state.freeze_slots[0];

	undo_horizon_v2_receive(&env, sender_wire);
	UT_ASSERT(slot->valid && !slot->view.horizon_regression_flagged);
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot->seq), 2);
	UT_ASSERT_EQ(slot->last_report_sequence, sender_report.report_sequence);
	UT_ASSERT(memcmp(&slot->view.report, &sender_report, sizeof(sender_report)) == 0);
	UT_ASSERT(memcmp(&slot->fence, &receiver_fence, sizeof(receiver_fence)) == 0);
	UT_ASSERT_EQ(slot->capability_generation, test_capability_generation);
	UT_ASSERT_EQ(slot->view.recv_at_us, cohort_now_us);
	UT_ASSERT_EQ(slot->view.report.origin, 0);
	UT_ASSERT_EQ(slot->view.report.active_full_xid_min, U64FromFullTransactionId(active.scan.min_full));
	UT_ASSERT_EQ(slot->view.report.horizon_scn, history.horizon_scn);
	UT_ASSERT_EQ(receiver_stream_reads, 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(&receiver_state.wire_reject_count), 0);
	UT_ASSERT_EQ(sender_history_reads, reads);
	UT_ASSERT_EQ(native_future_reads, future_reads);
	UT_ASSERT(!receiver_state.freeze_slots[1].valid);
	UT_ASSERT_EQ(pg_atomic_read_u32(&receiver_state.freeze_watermark_seq), 0);
	receiver_view(false);
}

UT_TEST(test_actual_receive_unproven_ingress_preserves_last_accepted_slot)
{
	ClusterICEnvelope env;
	ClusterUndoRetentionSample history;
	ClusterUndoCleanerActiveSample active = start_original_receive(&env, &history);
	ClusterICControlIngress good = receiver_ingress;
	ClusterHeapFreezeReportV2SlotShmem old;

	undo_horizon_v2_receive(&env, sender_wire);
	old = receiver_state.freeze_slots[0];
	UT_ASSERT(old.valid);
	next_original_receive_report(&active, &history);
	for (unsigned fault = 1; fault <= 4; fault++) {
		receiver_ingress = good;
		receiver_ingress_available = fault != 1;
		if (fault == 2) receiver_ingress.owner_pid++;
		if (fault == 3) receiver_ingress.carrier = CLUSTER_IC_CONTROL_UNKNOWN;
		if (fault == 4) receiver_ingress.epoch++;
		receiver_stream_reads = 0;
		undo_horizon_v2_receive(&env, sender_wire);
		UT_ASSERT(memcmp(&old, &receiver_state.freeze_slots[0], sizeof(old)) == 0);
		UT_ASSERT_EQ(receiver_stream_reads, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&receiver_state.wire_reject_count), fault);
	}
	receiver_view(false);
}

static void
change_receiver_identity_after_capture(void)
{
	if (receiver_late_fault == 2)
		test_capability_generation++;
	else {
		UT_ASSERT_EQ(receiver_late_fault, 3);
		receiver_source_current = false;
	}
}

UT_TEST(test_actual_receive_rechecks_private_stream_hello_and_admission_before_publish)
{
	ClusterICEnvelope env;
	ClusterUndoRetentionSample history;
	ClusterUndoCleanerActiveSample active = start_original_receive(&env, &history);
	uint32 generation = test_capability_generation;
	ClusterHeapFreezeReportV2SlotShmem old;

	undo_horizon_v2_receive(&env, sender_wire);
	old = receiver_state.freeze_slots[0];
	UT_ASSERT(old.valid);
	next_original_receive_report(&active, &history);
	for (unsigned fault = 1; fault <= 3; fault++) {
		receiver_stream_reads = 0;
		receiver_stream_refuse_at = fault == 1 ? 2 : 0;
		receiver_source_current = true;
		receiver_late_fault = fault;
		sender_peer_current_observer = fault == 1 ? NULL : change_receiver_identity_after_capture;
		undo_horizon_v2_receive(&env, sender_wire);
		UT_ASSERT(memcmp(&old, &receiver_state.freeze_slots[0], sizeof(old)) == 0);
		UT_ASSERT_EQ(receiver_stream_reads, 2);
		UT_ASSERT_EQ(pg_atomic_read_u64(&receiver_state.wire_reject_count), fault);
		UT_ASSERT(sender_peer_current_observer == NULL);
		test_capability_generation = generation;
	}
	receiver_stream_refuse_at = 0;
	receiver_source_current = true;
	undo_horizon_v2_receive(&env, sender_wire);
	UT_ASSERT_EQ(receiver_state.freeze_slots[0].last_report_sequence, 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(&receiver_state.freeze_slots[0].seq), 4);
	UT_ASSERT(memcmp(&receiver_state.freeze_slots[0].view.report, &sender_report,
				 sizeof(sender_report)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&receiver_state.wire_reject_count), 3);
	receiver_view(false);
}

UT_TEST(test_original_cohort_preserves_fullxid_sources_before_native_window_refusal)
{
	ClusterUndoCleanerActiveSample active = original_sender_source();
	ClusterUndoRetentionSample history;
	ClusterHeapFreezeCohortObservation cohort;
	ClusterHeapFreezePolicySampleV1 policy = { 0 };
	ClusterSemanticAdmissionToken admission = { 0 };
	ClusterHeapFreezeObservedBoundsV1 bounds, before;
	const ClusterHeapFreezeReportV2 *peer;
	unsigned reads, future_reads;

	sender_sample_clock = 700;
	sender_history_reads = 0;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&history));
	assert_actual_history_report(&active, &history, 1);
	install_controlled_peer_report(&active);
	UT_ASSERT_EQ(undo_horizon_v2_cohort_observe(&cohort, NULL, NULL), CLUSTER_HEAP_FREEZE_COHORT_READY);
	peer = &sender_state.freeze_slots[1].view.report;
	UT_ASSERT_EQ(cohort.source_peer_active_full_xid_min[1], peer->active_full_xid_min);
	UT_ASSERT_EQ(cohort.source_peer_future_full_xid[1], peer->future_full_xid);
	UT_ASSERT_EQ(cohort.source_peer_active_full_xid_min[65], 0);
	UT_ASSERT_EQ(cohort.source_peer_future_full_xid[65], 0);
	UT_ASSERT(FullTransactionIdEquals(cohort.local_registration_cut.min_full, active.scan.min_full));
	policy.legal_next = policy.freeze_limit = active.scan.future_full;
	admission.entered = true;
	admission.record_generation = cohort.record_generation;
	admission.formation_epoch = cohort.formation_epoch;
	reads = sender_history_reads; future_reads = native_future_reads;
	/* This original fixture deliberately retains epoch7 below epoch9 N.
	 * Preserve that complete identity; the existing native past-halfspace
	 * gate must still refuse. Do not relabel an epoch to manufacture a plan. */
	UT_ASSERT(U64FromFullTransactionId(policy.legal_next)
		- U64FromFullTransactionId(active.scan.min_full) >= UINT64CONST(0x80000000));
	memset(&bounds, 0xa5, sizeof(bounds)); before = bounds;
	UT_ASSERT(!cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &bounds));
	UT_ASSERT(memcmp(&bounds, &before, sizeof(bounds)) == 0);
	UT_ASSERT_EQ(sender_history_reads, reads);
	UT_ASSERT_EQ(native_future_reads, future_reads);
	/* Missing source data also stays closed here. Specific source-check
	 * sensitivity is isolated by the pure-bounds fixture's legal window. */
	cohort.source_peer_active_full_xid_min[1] = 0;
	memset(&bounds, 0xa5, sizeof(bounds)); before = bounds;
	UT_ASSERT(!cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &bounds));
	UT_ASSERT(memcmp(&bounds, &before, sizeof(bounds)) == 0);
	UT_ASSERT_EQ(sender_history_reads, reads);
	UT_ASSERT_EQ(native_future_reads, future_reads);
}

/* Keep the original three real owner cuts and their original report path.
 * Only the peer report, clock and semantic/lock boundaries remain controlled. */
static ClusterHeapFreezeCohortObservation retirement_previous_cohort;
static bool retirement_previous_valid;
static unsigned retirement_cohort_checks, retirement_native_eligible;

static void
observe_original_retirement_cohort(const ClusterUndoCleanerActiveSample *published)
{
	ClusterHeapFreezeCohortObservation cohort;
	ClusterUndoCleanerActiveSample active;
	ClusterHeapFreezePolicySampleV1 policy = { 0 };
	ClusterSemanticAdmissionToken admission = { 0 };
	ClusterHeapFreezeObservedBoundsV1 bounds, before;
	bool previous_mode = native_pass_mode;
	unsigned native_reads, history_reads;

	observe_original_retirement_publication(published);
	native_pass_mode = true;
	UT_ASSERT(native_cleaner_sample(&active));
	UT_ASSERT(native_cleaner_sample_current(&active));
	/* The same fixed required peer was present before the original owner's
	 * first cut. This peer report is a controlled leaf, not remote retention. */
	install_controlled_peer_report(&active);
	if (retirement_previous_valid) {
		UT_ASSERT(retirement_previous_cohort.source_local_sample_id < active.sample_id);
		UT_ASSERT_EQ(cluster_heap_freeze_cohort_current(&retirement_previous_cohort),
			CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED);
	}
	sender_sample_clock = 700;
	UT_ASSERT_EQ(undo_horizon_v2_cohort_observe(&cohort, NULL, NULL),
		CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_current(&cohort), CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(cohort.source_local_sample_id, active.sample_id);
	UT_ASSERT_EQ(cohort.active_full_xid_min, U64FromFullTransactionId(active.scan.min_full));
	UT_ASSERT_EQ(cohort.source_peer_active_full_xid_min[1],
		U64FromFullTransactionId(active.scan.future_full));
	UT_ASSERT_EQ(cohort.admitted_members_lo, 3);
	UT_ASSERT_EQ(cohort.local_unqualified_entries, 0);
	policy.legal_next = policy.freeze_limit = active.scan.future_full;
	admission.entered = true;
	admission.record_generation = cohort.record_generation;
	admission.formation_epoch = cohort.formation_epoch;
	memset(&bounds, 0xa5, sizeof(bounds)); before = bounds;
	native_reads = native_future_reads; history_reads = sender_history_reads;
	if (sender_retirement_reports < 3) {
		/* Receiver-side certificate retirement alone did not return the
		 * original owner's final reply. Its epoch7 key still protects F. */
		UT_ASSERT_EQ(cohort.active_full_xid_min, U64FromFullTransactionId(sender_retained_original));
		UT_ASSERT(!cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &bounds));
		UT_ASSERT(memcmp(&bounds, &before, sizeof(bounds)) == 0);
	} else {
		UT_ASSERT_EQ(sender_retirement_reports, 3);
		UT_ASSERT(cluster_heap_freeze_observed_bounds_v1(&policy, &cohort, &admission, 0, &bounds));
		UT_ASSERT(FullTransactionIdEquals(bounds.freeze_limit, active.scan.future_full));
		UT_ASSERT_EQ(bounds.horizon_scn, 300);
		retirement_native_eligible++;
	}
	UT_ASSERT_EQ(native_future_reads, native_reads);
	UT_ASSERT_EQ(sender_history_reads, history_reads);
	retirement_previous_cohort = cohort;
	retirement_previous_valid = true;
	retirement_cohort_checks++;
	native_pass_mode = previous_mode;
}

static void
exercise_original_final_reply_cohort(bool applied)
{
	retirement_previous_valid = false;
	retirement_cohort_checks = retirement_native_eligible = 0;
	sender_retirement_observer = observe_original_retirement_cohort;
	exercise_original_retirement_reports(applied);
	sender_retirement_observer = NULL;
	UT_ASSERT(original_worker_publication_observer == NULL);
	UT_ASSERT_EQ(retirement_cohort_checks, 3);
	UT_ASSERT_EQ(retirement_native_eligible, 1);
	UT_ASSERT_EQ(sender_retirement_reports, 3);
	UT_ASSERT_EQ(sender_calls, 3);
	UT_ASSERT_EQ(sender_maintenance_acquires, sender_maintenance_releases);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_original_cancel_final_reply_precedes_new_cohort_native_eligibility)
{
	exercise_original_final_reply_cohort(false);
}

UT_TEST(test_original_applied_final_reply_precedes_new_cohort_native_eligibility)
{
	exercise_original_final_reply_cohort(true);
}

int
main(void)
{
	UT_PLAN(20);
	UT_RUN(test_original_protection_exit_and_complete_worker_reach_v2_wire);
	UT_RUN(test_actual_foreign_receipt_revokes_old_report_and_blocks_new_unqualified_report);
	UT_RUN(test_actual_foreign_close_revokes_old_report_and_blocks_new_unqualified_report);
	UT_RUN(test_cancelled_receipt_report_retains_source_until_exact_final_certificate_reply);
	UT_RUN(test_applied_receipt_report_retains_source_until_exact_final_certificate_reply);
	UT_RUN(test_original_snapshot_then_native_exit_clock_h_reach_retained_f_report);
	UT_RUN(test_actual_replication_h_refusal_cannot_become_v2_report_or_sequence_debit);
	UT_RUN(test_actual_worker_restart_withholds_report_until_new_complete_scan);
	UT_RUN(test_actual_same_pid_respawn_during_peer_check_rejects_old_sample);
	UT_RUN(test_original_producers_reach_cohort_and_candidate_without_resampling);
	UT_RUN(test_actual_late_receipt_revokes_cohort_and_clears_candidate_read);
	UT_RUN(test_actual_peer_callback_respawn_requires_fresh_cohort_publication);
	UT_RUN(test_required_high_peer_missing_cannot_be_dropped_from_actual_cohort);
	UT_RUN(test_late_high_peer_callback_revokes_earlier_peer_copy_before_candidate);
	UT_RUN(test_actual_sender_bytes_reach_original_receive_without_origin_relabel);
	UT_RUN(test_actual_receive_unproven_ingress_preserves_last_accepted_slot);
	UT_RUN(test_actual_receive_rechecks_private_stream_hello_and_admission_before_publish);
	UT_RUN(test_original_cohort_preserves_fullxid_sources_before_native_window_refusal);
	UT_RUN(test_original_cancel_final_reply_precedes_new_cohort_native_eligibility);
	UT_RUN(test_original_applied_final_reply_precedes_new_cohort_native_eligibility);
	return ut_failed_count == 0 ? 0 : 1;
}
