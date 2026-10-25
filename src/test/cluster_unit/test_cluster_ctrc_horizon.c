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
#include "cluster/cluster_sf_dep.h"

int cluster_lmon_main_loop_interval = 1000;
static unsigned sender_calls, sender_capability_reads;
static unsigned sender_unknown_on_peer;
static ClusterCtrcOriginEntry sender_foreign_request;
static ClusterHeapFreezeReportV2 sender_report;

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
	UT_ASSERT_EQ(peer, 1);
	UT_ASSERT_EQ(required, PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2);
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
	UT_ASSERT_EQ(peer, 1);
	UT_ASSERT_EQ(required, PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2);
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

ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 destination, const void *payload, uint32 length)
{
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(!native_lock_held && !native_cleaner_lock_held && !sender_maintenance_held);
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_UNDO_HORIZON);
	UT_ASSERT_EQ(destination, 1);
	UT_ASSERT_EQ(length, CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES);
	UT_ASSERT(cluster_heap_freeze_report_v2_decode(payload, length, &sender_report));
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

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(test_original_protection_exit_and_complete_worker_reach_v2_wire);
	UT_RUN(test_actual_foreign_receipt_revokes_old_report_and_blocks_new_unqualified_report);
	UT_RUN(test_actual_foreign_close_revokes_old_report_and_blocks_new_unqualified_report);
	return ut_failed_count == 0 ? 0 : 1;
}
