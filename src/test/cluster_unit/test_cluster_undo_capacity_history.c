/* Actual LMON local history publication through the physical pool observer
 * and original condition-variable capacity controller. Clock/H, admission,
 * native status and SCUR bytes are explicit input boundaries. No boundary
 * supplies a capacity verdict, report sequence or F sample identity. */
#include "postgres.h"
#include "utils/wait_event.h"
static void history_wait_scheduler(void);
#define G320_CAPACITY_REAL_HISTORY
#include "test_cluster_ctrc_capacity.c"
#undef main

#include "cluster/cluster_undo_horizon.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_sf_dep.h"

static bool
history_no_mrp(void)
{
	return false;
}
static SCN
history_no_mrp_bound(void)
{
	abort();
	return InvalidScn;
}

/* Remote capability and report contents are controlled input leaves. The
 * original self-only cases still reject every remote-capability query. */
static int history_peer = -1;
static uint32 history_peer_generation;

bool
cluster_sf_peer_capability_word_sample(int32 peer, uint32 required, uint32 *word,
									   uint32 *generation)
{
	if (peer != history_peer || required != PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2
		|| word == NULL || generation == NULL)
		abort();
	*word = required;
	*generation = history_peer_generation;
	return true;
}
bool
cluster_sf_peer_capability_generation_matches(int32 peer, uint32 required, uint32 generation)
{
	if (peer != history_peer || required != PGRAC_IC_HELLO_CAP_HEAP_FREEZE_REPORT_V2)
		abort();
	return generation != 0 && generation == history_peer_generation;
}

#undef SpinLockAcquire
#undef SpinLockRelease
#define SpinLockAcquire(lock) S_LOCK(lock)
#define SpinLockRelease(lock) S_UNLOCK(lock)
#define cluster_semantic_activation_active_sample_capture native_cleaner_admission_capture
#define cluster_semantic_activation_active_sample_current native_cleaner_admission_current
#define cluster_undo_cleaner_active_sample native_cleaner_sample
#define cluster_undo_cleaner_active_sample_current native_cleaner_sample_current
#define cluster_mrp_should_start history_no_mrp
#define cluster_mrp_standby_consistent_scn history_no_mrp_bound
#include "test_cluster_ctrc_horizon_sender.inc"
#include "test_cluster_ctrc_horizon_cohort.inc"
#include "test_cluster_undo_capacity_history_lmon.inc"
#undef cluster_mrp_standby_consistent_scn
#undef cluster_mrp_should_start
#undef cluster_undo_cleaner_active_sample_current
#undef cluster_undo_cleaner_active_sample
#undef cluster_semantic_activation_active_sample_current
#undef cluster_semantic_activation_active_sample_capture

static ClusterUndoHorizonShmem history_lmon;
static ClusterUndoCapacityChangeFence history_fence;
static unsigned history_schedule;
static uint64 history_initial_sequence;
static ClusterCtrcTxnKeyV1 history_writer_key;
static ClusterHeapFreezeCohortStatus freshness_status;
static bool freshness_producer;
static bool maximum_pool_seen_at_wait;
static UndoSegmentHeaderData maximum_pool_before[CLUSTER_UNDO_SEGS_PER_INSTANCE];

static void
history_publish(SCN h, uint32 kind)
{
	ClusterUndoCleanerActiveSample active;
	ClusterUndoRetentionSample raw
		= { .horizon_scn = h,
			.clock_scn = 3000,
			.contributor_kind = kind,
			.contributor_token = kind == CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK ? 0 : 91 };
	if (!native_cleaner_sample(&active))
		abort();
	undo_horizon_publish_local_history(&active, &raw);
}

/* This is an accepted remote-input boundary, not an authenticated sender.
 * Its original slot publisher and every subsequent consumer remain real. */
static void
history_publish_peer(uint64 sequence, SCN horizon, uint32 kind)
{
	ClusterUndoCleanerActiveSample active;
	ClusterHeapFreezeReportV2 report = {0};
	uint8 image[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES];

	if ((history_peer != 1 && history_peer != 65) || !native_cleaner_sample(&active))
		abort();
	report.version = CLUSTER_HEAP_FREEZE_REPORT_V2_VERSION;
	report.length = CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES;
	report.sysid = active.admission_fence.system_identifier;
	report.record_generation = active.admission_fence.record_generation;
	report.formation_epoch = active.admission_fence.formation_epoch;
	report.incarnation = active.admission_fence.admitted_incarnation[history_peer];
	report.origin = history_peer;
	report.report_sequence = sequence;
	report.future_full_xid = U64FromFullTransactionId(active.scan.future_full);
	report.active_full_xid_min = U64FromFullTransactionId(active.scan.min_full);
	report.f_sample_id = active.sample_id;
	report.horizon_scn = horizon;
	report.h_sample_clock_scn = 3000;
	report.h_contributor_kind = kind;
	report.h_contributor_token = kind == CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK ? 0 : 91;
	report.sender_interval_ms = cluster_lmon_main_loop_interval;
	if (!cluster_heap_freeze_report_v2_encode(&report, image))
		abort();
	undo_horizon_v2_publish(&history_lmon.freeze_slots[history_peer], &report,
							&active.admission_fence, history_peer_generation,
							GetCurrentTimestamp(), false);
}

/* Exercise the actual shared reserve/cancel producers between complete passes.
 * PGPROC identity is the explicit native-source leaf; the original mutators,
 * registration sequence and new completed worker publication are real. */
static void
history_real_writer_roundtrip(void)
{
	ClusterCtrcOriginReservation reservation;
	uint64 before = pg_atomic_read_u64(&history_fence.change_sequence);
	test_my_proc.xid = history_writer_key.xid;
	test_top_full = FullTransactionIdFromEpochAndXid(7, history_writer_key.xid);
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic,
						U64FromFullTransactionId(test_top_full));
	if (cluster_ctrc_origin_reserve_active(&history_writer_key, &reservation)
		!= CLUSTER_CTRC_ORIGIN_RESERVED_PENDING)
		abort();
	if (!cluster_ctrc_origin_cancel_pre_bind(&reservation))
		abort();
	if (pg_atomic_read_u64(&history_fence.change_sequence) <= before)
		abort();
	test_my_proc.xid = InvalidTransactionId;
	test_top_full = InvalidFullTransactionId;
	pg_atomic_write_u64(&test_my_proc.cluster_top_full_xid_atomic, 0);
	do {
		undo_cleaner_sample_active_pass();
	} while (undo_cleaner_active_scan_pending);
}

static void
history_wait_scheduler(void)
{
	if (capacity_subscribed && capacity_sleep_event == WAIT_EVENT_CLUSTER_UNDO_CAPACITY_WAIT) {
		if (capacity_sleeps > 4)
			abort();
		switch (history_schedule) {
		case 1: /* No reader: next real clock report crosses the retained commits. */
		case 3: /* Reader exits at this actual report boundary. */
			history_publish(3000, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
			break;
		case 2: /* Same protector, distinct real publication. */
			history_publish(1000, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
			break;
		case 5: /* A real writer retires the first complete protective pass. */
			if (capacity_sleeps == 1)
				history_real_writer_roundtrip();
			history_publish(1000, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
			break;
		case 4: /* A duplicate cannot finish the protective pair. */
			if (capacity_sleeps == 1) {
				if (history_lmon.report_sequence != history_initial_sequence)
					abort();
			} else
				history_publish(1000, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
			break;
		case 6:
		case 7:
			/* Observe the real physical path before advancing its report. */
			maximum_pool_seen_at_wait = true;
			for (unsigned segment = 0; segment < 256; segment++)
				if (segment_reads[segment] == 0)
					maximum_pool_seen_at_wait = false;
			history_publish(history_schedule == 6 ? 3000 : 1000,
							history_schedule == 6
								? CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK
								: CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
			break;
		case 8: /* Only local publication advances on the first wake. */
		case 9: /* Only the required peer advances on the first wake. */
			if (capacity_sleeps == 1) {
				if (history_schedule == 8)
					history_publish(3000, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
				else
					history_publish_peer(2, 1000,
						CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
			} else if (capacity_sleeps == 2) {
				history_publish(3000, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
				history_publish_peer(3, 1000,
					CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
			} else {
				/* A bad policy must fail assertions instead of waiting forever. */
				history_publish(3000, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
				history_publish_peer(4, 3000,
					CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
			}
			break;
		case 10: /* A reconnect does not replace the old admitted report. */
			if (capacity_sleeps == 1)
				history_peer_generation++;
			else {
				history_publish(3000, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
				history_publish_peer(2, 3000,
					CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
			}
			break;
		case 11: /* A stale gap must retire the first protective pass. */
		case 12: /* A stale local publisher cannot sustain a report wait. */
			if (capacity_sleeps == 1) {
				ClusterHeapFreezeCohortObservation observation;

				/* Controlled clock input, beyond the six-second freshness window.
				 * Advance time rather than rewriting an accepted publication. */
				test_now_us += UINT64CONST(6000001);
				if (history_schedule == 11)
					history_publish(3000, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
				freshness_status
					= cluster_undo_capacity_history_observe(&observation, &freshness_producer);
			} else {
				history_publish(3000, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
				history_publish_peer(capacity_sleeps,
					history_schedule == 12 || capacity_sleeps > 3 ? 3000 : 1000,
					history_schedule == 12 || capacity_sleeps > 3
						? CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK
						: CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
			}
			break;
		default:
			abort();
		}
	}
}

static void
history_start(uint32 kind)
{
	test_cluster_epoch = 23;
	history_inputs = false;
	exercise_worker_active_pass(0);
	history_fixture();
	history_writer_key = fresh_origin_key();
	native_source_exit = native_publish_source = native_pass_mode = true;
	/* Controlled admission leaves carry the same nonzero cuts as the original
  * semantic publisher; the completed worker sample below retains them. */
	native_pass_source.admission_publication_seq = 2;
	native_pass_source.membership_cut_generation = 6;
	external_lock_hook = observe_native_source_lock;
	/* A new original worker round covers the newly reset real census; retain
  * the original admission/lease identity, never invent a positive F id. */
	do {
		undo_cleaner_sample_active_pass();
	} while (undo_cleaner_active_scan_pending);
	for (unsigned i = 0; i < CLUSTER_UNDO_CLEANER_WORKER_TYPES; i++) {
		native_cleaner_state.workers[i].status = UNDO_CLEANER_READY;
		native_cleaner_state.workers[i].latch = (Latch *)&native_cleaner_state;
		native_cleaner_state.workers[i].incarnation_generation = i == 0 ? 83 : 71;
	}
	capacity_sleeps = capacity_cancels = 0;
	capacity_subscribed = capacity_cancel = false;
	capacity_wait_event = 41;
	capacity_sleep_event = 0;
	MemSet(&history_lmon, 0, sizeof(history_lmon));
	SpinLockInit(&history_lmon.maintenance_lock);
	UndoHorizonShmem = &history_lmon;
	cluster_enabled = true;
	cluster_undo_capacity_fence_init(&history_fence);
	cluster_undo_capacity_fence_attach(&history_fence);
	history_publish(1000, kind);
	history_initial_sequence = history_lmon.report_sequence;
}

static void
history_finish_observation(unsigned sleeps, unsigned cancels)
{
	UT_ASSERT_EQ(capacity_sleeps, sleeps);
	UT_ASSERT_EQ(capacity_cancels, cancels);
	UT_ASSERT(!capacity_subscribed);
	UT_ASSERT_EQ(capacity_wait_event, 41);
	UT_ASSERT_EQ(pg_atomic_read_u64(&history_fence.inflight_writers), 0);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT_EQ(sample_admission_debt, 0);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(!native_cleaner_lock_held);
	UT_ASSERT(CtrcCapacityHistory == NULL);
	cluster_undo_capacity_fence_attach(NULL);
	history_inputs = false;
	native_source_exit = native_publish_source = native_pass_mode = false;
	external_lock_hook = NULL;
	test_cluster_epoch = 13;
}

static void
history_finish(unsigned sleeps)
{
	history_finish_observation(sleeps, 1);
}

UT_TEST(test_actual_clock_report_reaches_original_wait_and_physical_supply)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 };
	ClusterHeapFreezeCohortObservation observed;
	bool producer = false;
	ClusterCapacityHistoryPass pass = { 0 };
	ClusterCapacityObservationV1 physical;
	ClusterUndoCapacityStats stats;
	unsigned passes = 0;
	history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	CtrcCapacitySegmentCursor = 0;
	UT_ASSERT_EQ(history_initial_sequence, 1);
	UT_ASSERT_EQ(cluster_undo_capacity_history_observe(&observed, &producer),
				 CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT(producer);
	UT_ASSERT_EQ(observed.source_local_report_sequence, 1);
	UT_ASSERT_EQ(observed.horizon_scn, 1000);
	do {
		unsigned before = sample_holds, current_before = segment_reads[0];
		UT_ASSERT_EQ(cluster_ctrc_capacity_probe_history_v1(
						 1, observed.horizon_scn, test_cluster_epoch, NULL, &pass, &physical),
					 CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
		UT_ASSERT(segment_reads[0] - current_before <= TT_SLOTS_PER_SEGMENT);
		UT_ASSERT(sample_holds - before - (segment_reads[0] - current_before) <= 16);
		if (++passes > 256)
			abort();
		/* Positions 0..15 contain CURRENT, the record cursor and fourteen
		 * retained headers. The remaining positions are absent. This is a
		 * per-probe union, not fourteen segments or full-pool authority. */
		UT_ASSERT_EQ(physical.retain_reasons,
					 passes == 1 ? CLUSTER_UNDO_RETAIN_HISTORY_PIN : 0);
	} while (!pass.complete && pass.progress);
	UT_ASSERT(pass.complete && pass.history_only && pass.current_proven);
	UT_ASSERT_EQ(pass.covered_positions, 256);
	UT_ASSERT_EQ(pass.allocated_count, 16);
	UT_ASSERT_EQ(pass.covered_positions - pass.allocated_count, 240);
	UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
	UT_ASSERT_EQ(stats.retain_by_reason[4], 0); /* Observing alone does not count. */
	history_schedule = 1;
	UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
	UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
	UT_ASSERT_EQ(history_lmon.report_sequence, 2);
	UT_ASSERT_EQ(history_lmon.local_history.horizon_scn, 3000);
	UT_ASSERT(segment_reads[0] >= 2);
	for (unsigned i = 1; i < 16; i++)
		UT_ASSERT(segment_reads[i] > 0);
	UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
	for (unsigned bit = 0; bit < lengthof(stats.retain_by_reason); bit++)
		UT_ASSERT_EQ(stats.retain_by_reason[bit], bit == 4 ? 1 : 0);
	UT_ASSERT_EQ(stats.pool_wait_count, 1);
	UT_ASSERT_EQ(stats.wakeup_recheck_count, 1);
	UT_ASSERT_EQ(stats.refused_proof_count, 0);
	/* The composed fixture stops at the original allocator re-entry boolean;
  * no allocator/bind implementation is linked here. */
	history_finish(1);
}

UT_TEST(test_actual_protective_pair_requires_distinct_reports)
{
	for (unsigned duplicate = 0; duplicate < 2; duplicate++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		ClusterUndoCapacityStats stats;
		history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
		CtrcCapacitySegmentCursor = 0;
		history_schedule = duplicate ? 4 : 2;
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		UT_ASSERT_EQ(history_lmon.report_sequence, 2);
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		for (unsigned bit = 0; bit < lengthof(stats.retain_by_reason); bit++)
			UT_ASSERT_EQ(stats.retain_by_reason[bit], bit == 4 ? (duplicate ? 3 : 2) : 0);
		UT_ASSERT_EQ(stats.refused_proof_count, 1);
		history_finish(duplicate ? 2 : 1);
	}
}

UT_TEST(test_history_non_h_rejections_count_the_actual_reason_union_once)
{
	static const unsigned expected[][9] = { { 0, 0, 0, 0, 1, 0, 1, 0, 0 },
										  { 0, 0, 0, 0, 1, 1, 0, 0, 0 },
										  { 0, 0, 1, 0, 1, 0, 0, 0, 0 },
										  { 0, 0, 1, 0, 1, 1, 1, 0, 0 } };

	for (unsigned fault = 0; fault < lengthof(expected); fault++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		ClusterUndoCapacityStats stats;
		UndoSegmentHeaderData before;

		history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
		CtrcCapacitySegmentCursor = 2;
		if (fault == 0 || fault == 3)
			history_headers[2].tt_slots[47].flags = 0;
		if (fault == 1 || fault == 3)
			history_headers[2].tt_slots[47].first_undo_block.raw[0] = 1;
		if (fault >= 2)
			history_headers[2].tail_block = 1;
		before = history_headers[2];
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		for (unsigned bit = 0; bit < lengthof(expected[fault]); bit++)
			UT_ASSERT_EQ(stats.retain_by_reason[bit], expected[fault][bit]);
		UT_ASSERT_EQ(stats.refused_proof_count, 1);
		UT_ASSERT_EQ(stats.pool_wait_count, 0);
		UT_ASSERT_EQ(memcmp(&before, &history_headers[2], sizeof(before)), 0);
		UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
		history_finish(0);
	}
}

UT_TEST(test_history_invalid_samples_do_not_fabricate_retention_counts)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		ClusterUndoCapacityStats stats;

		history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
		CtrcCapacitySegmentCursor = 2;
		/* Reject every physical header. A one-shot root drift would allow
		 * later headers to qualify under the newly sampled root. */
		for (unsigned i = 0; i < 16; i++) {
			if (fault == 0)
				history_headers[i].owner_instance = 2;
			if (fault == 1)
				history_headers[i].wrap_count = UINT32_MAX;
		}
		sample_release_failure = fault == 2;
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		for (unsigned bit = 0; bit < lengthof(stats.retain_by_reason); bit++)
			UT_ASSERT_EQ(stats.retain_by_reason[bit], 0);
		UT_ASSERT_EQ(stats.refused_proof_count, 1);
		history_finish(0);
	}
}

UT_TEST(test_history_positive_owner_and_retry_do_not_count_prior_retention)
{
	for (unsigned retry = 0; retry < 2; retry++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		ClusterUndoCapacityStats stats;

		history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
		CtrcCapacitySegmentCursor = 2;
		for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
			history_headers[retry ? 0 : 3].tt_slots[i].commit_scn = 1000;
			if (retry)
				history_current.slots[i].commit_scn = 1000;
		}
		history_schedule = 1;
		UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		for (unsigned bit = 0; bit < lengthof(stats.retain_by_reason); bit++)
			UT_ASSERT_EQ(stats.retain_by_reason[bit], 0);
		UT_ASSERT_EQ(stats.refused_proof_count, 0);
		UT_ASSERT_EQ(stats.pool_wait_count, retry ? 0 : 1);
		UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
		history_finish(retry ? 0 : 1);
	}
}

UT_TEST(test_actual_reader_exit_next_local_report_rechecks_supply)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 };
	history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	history_schedule = 3;
	UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
	UT_ASSERT_EQ(history_lmon.local_history.contributor_kind,
				 CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	history_finish(1);
}

UT_TEST(test_actual_history_unknown_inputs_refuse_before_wait)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterCtrcTxnKeyV1 continuation = { 0 };
		history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
		switch (fault) {
		case 0:
			history_presence_io = true;
			break;
		case 1:
			history_headers[15].tt_slots[47].xid = 6000;
			break; /* native unknown */
		case 2:
			history_headers[15].tt_slots[47].flags = TT_FLAGS_RESERVED;
			break;
		case 3:
			history_headers[15].tt_slots[47].first_undo_block.raw[0] = 1;
			break;
		}
		history_schedule = 0;
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		history_finish(0);
	}
}

UT_TEST(test_actual_history_cancel_retires_original_cv_and_wait_event)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 };
	volatile bool caught = false;
	history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	capacity_cancel = true;
	PG_TRY();
	{
		(void)cluster_undo_cleaner_wait_for_capacity(1, &continuation);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(pg_atomic_read_u64(&native_cleaner_state.capacity_wait_cancelled), 1);
	UT_ASSERT_EQ(native_cleaner_state.capacity_wait_repolled, 0);
	history_finish(1);
}

UT_TEST(test_actual_history_writer_change_cannot_bridge_complete_pass)
{
	ClusterCtrcTxnKeyV1 continuation = { 0 };
	history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	sample_release_hook = history_change_after_release;
	UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &continuation));
	UT_ASSERT(pg_atomic_read_u64(&history_fence.change_sequence) > 0);
	history_finish(0);
	/* A completed first pass cannot survive a real write between reports. */
	MemSet(&continuation, 0, sizeof(continuation));
	history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	history_schedule = 5;
	UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &continuation));
	UT_ASSERT_EQ(history_lmon.report_sequence, 3);
	UT_ASSERT(pg_atomic_read_u64(&history_fence.change_sequence) > 0);
	history_finish(2);
}

/* Physical bytes and native status remain inputs; all observation, retention,
 * LMON sequencing and wait decisions below execute their original consumers. */
static void
history_fill_maximum_pool(int configured_cap)
{
	cluster_undo_segments_max_per_instance = configured_cap;
	for (unsigned segment = 16; segment < 256; segment++)
	{
		UndoSegmentHeaderData *header = &history_headers[segment];

		*header = history_headers[15];
		header->segment_id = segment + 1;
		for (unsigned slot = 0; slot < TT_SLOTS_PER_SEGMENT; slot++)
			header->tt_slots[slot].xid = 7001 + segment * TT_SLOTS_PER_SEGMENT + slot;
		history_present[segment] = true;
	}
	memcpy(maximum_pool_before, history_headers, sizeof(history_headers));
}

UT_TEST(test_maximum_pool_requires_complete_bounded_physical_passes)
{
	static const int configured_caps[] = {16, 256};
	static const unsigned cursors[] = {0, 251};

	for (unsigned cap = 0; cap < lengthof(configured_caps); cap++)
		for (unsigned cursor = 0; cursor < lengthof(cursors); cursor++)
		{
			ClusterCapacityHistoryPass pass = {0};
			ClusterCapacityObservationV1 out;
			unsigned calls = 0;

			history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
			history_fill_maximum_pool(configured_caps[cap]);
			CtrcCapacitySegmentCursor = cursors[cursor];
			do
			{
				unsigned holds = sample_holds, current_reads = segment_reads[0];
				uint32 covered = pass.covered_positions;

				UT_ASSERT_EQ(cluster_ctrc_capacity_probe_history_v1(
					1, 1000, test_cluster_epoch, NULL, &pass, &out),
					CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
				UT_ASSERT(segment_reads[0] - current_reads <= 48);
				UT_ASSERT(sample_holds - holds - (segment_reads[0] - current_reads) <= 16);
				UT_ASSERT(pass.covered_positions > covered);
				UT_ASSERT(pass.covered_positions - covered <= 16);
				if (++calls > 256)
					break;
			} while (!pass.complete && pass.progress);
			UT_ASSERT(calls >= 16 && calls <= 256);
			UT_ASSERT(pass.complete && pass.history_only && pass.current_proven);
			UT_ASSERT_EQ(pass.covered_positions, 256);
			UT_ASSERT_EQ(pass.allocated_count, 256);
			for (unsigned segment = 0; segment < 256; segment++)
			{
				UT_ASSERT(history_logical_positions[segment]);
				UT_ASSERT(segment_reads[segment] > 0);
			}
			UT_ASSERT_EQ(history_lmon.report_sequence, 1);
			/* This direct observer never prepared a CV subscription. */
			UT_ASSERT_EQ(memcmp(maximum_pool_before, history_headers, sizeof(history_headers)), 0);
			history_finish_observation(0, 0);
			cluster_undo_segments_max_per_instance = 16;
		}
}

UT_TEST(test_maximum_pool_clock_and_protector_reach_original_controller)
{
	for (unsigned protector = 0; protector < 2; protector++)
	{
		ClusterCtrcTxnKeyV1 continuation = {0};
		ClusterUndoCapacityStats stats;

		history_start(protector ? CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT
								: CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
		history_fill_maximum_pool(256);
		CtrcCapacitySegmentCursor = 251;
		maximum_pool_seen_at_wait = false;
		history_schedule = protector ? 7 : 6;
		UT_ASSERT_EQ(cluster_undo_cleaner_wait_for_capacity(1, &continuation), !protector);
		UT_ASSERT(maximum_pool_seen_at_wait);
		UT_ASSERT_EQ(history_lmon.report_sequence, 2);
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		UT_ASSERT_EQ(stats.pool_wait_count, 1);
		UT_ASSERT_EQ(stats.wakeup_recheck_count, 1);
		UT_ASSERT_EQ(stats.refused_proof_count, protector ? 1 : 0);
		UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
		UT_ASSERT_EQ(cluster_undo_segments_max_per_instance, 256);
		UT_ASSERT_EQ(memcmp(maximum_pool_before, history_headers, sizeof(history_headers)), 0);
		history_finish(1);
		cluster_undo_segments_max_per_instance = 16;
	}
}

UT_TEST(test_maximum_pool_last_image_cannot_be_hidden_by_a_protected_prefix)
{
	for (unsigned missing = 0; missing < 2; missing++)
	{
		ClusterCtrcTxnKeyV1 continuation = {0};
		UndoSegmentHeaderData last;
		ClusterUndoCapacityStats stats;

		history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
		history_fill_maximum_pool(256);
		CtrcCapacitySegmentCursor = 0;
		if (missing)
			history_present[255] = false;
		else
			history_headers[255].tt_slots[47].first_undo_block.raw[0] = 1;
		last = history_headers[255];
		memcpy(maximum_pool_before, history_headers, sizeof(history_headers));
		/* A missing configured position requests recheck, not an allocation;
		 * non-H record debt is an unproven refusal, not a protective pair. */
		UT_ASSERT_EQ(cluster_undo_cleaner_wait_for_capacity(1, &continuation), missing != 0);
		UT_ASSERT(history_logical_positions[255]);
		if (!missing)
			UT_ASSERT(segment_reads[255] > 0);
		UT_ASSERT_EQ(history_lmon.report_sequence, 1);
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		UT_ASSERT_EQ(stats.pool_wait_count, 0);
		UT_ASSERT_EQ(stats.refused_proof_count, missing ? 0 : 1);
		UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
		UT_ASSERT_EQ(memcmp(&last, &history_headers[255], sizeof(last)), 0);
		UT_ASSERT_EQ(memcmp(maximum_pool_before, history_headers, sizeof(history_headers)), 0);
		UT_ASSERT_EQ(cluster_undo_segments_max_per_instance, 256);
		history_finish(0);
		cluster_undo_segments_max_per_instance = 16;
	}
}

static UndoSegmentHeaderData peer_headers_before[CLUSTER_UNDO_SEGS_PER_INSTANCE];

static void
history_admit_peer(int peer)
{
	history_peer = peer;
	history_peer_generation = 17;
	if (peer < 64)
		native_pass_source.admitted_members_lo |= UINT64CONST(1) << peer;
	else
		native_pass_source.admitted_members_hi |= UINT64CONST(1) << (peer - 64);
	native_pass_source.admitted_incarnation[peer] = 1100 + peer;
	do {
		undo_cleaner_sample_active_pass();
	} while (undo_cleaner_active_scan_pending);
	pg_atomic_init_u32(&history_lmon.freeze_slots[peer].seq, 0);
	history_publish(3000, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	history_publish_peer(1, 1000, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	memcpy(peer_headers_before, history_headers, sizeof(history_headers));
}

UT_TEST(test_required_peer_and_self_reports_both_advance_before_history_refusal)
{
	static const int peers[] = {1, 65};

	for (unsigned index = 0; index < lengthof(peers); index++)
		for (unsigned peer_only = 0; peer_only < 2; peer_only++) {
			ClusterCtrcTxnKeyV1 continuation = {0};
			ClusterHeapFreezeCohortObservation observation;
			ClusterUndoCapacityStats stats;
			bool producer = false;
			int peer = peers[index];

			history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
			history_admit_peer(peer);
			UT_ASSERT_EQ(cluster_undo_capacity_history_observe(&observation, &producer),
				CLUSTER_HEAP_FREEZE_COHORT_READY);
			UT_ASSERT(producer);
			UT_ASSERT_EQ(observation.source_local_report_sequence, 2);
			UT_ASSERT_EQ(observation.source_peer_report_sequence[peer], 1);
			UT_ASSERT_EQ(observation.source_local_horizon_scn, 3000);
			UT_ASSERT_EQ(observation.source_local_history.contributor_kind,
				CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
			UT_ASSERT_EQ(observation.source_peer_history[peer].contributor_kind,
				CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
			UT_ASSERT_EQ(observation.horizon_scn, 1000);
			history_schedule = peer_only ? 9 : 8;
			UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &continuation));
			UT_ASSERT_EQ(history_lmon.report_sequence, peer_only ? 3 : 4);
			UT_ASSERT_EQ(history_lmon.freeze_slots[peer].view.report.report_sequence, 3);
			UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
			UT_ASSERT_EQ(stats.pool_wait_count, 2);
			UT_ASSERT_EQ(stats.wakeup_recheck_count, 2);
			UT_ASSERT_EQ(stats.refused_proof_count, 1);
			UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
			UT_ASSERT_EQ(memcmp(peer_headers_before, history_headers, sizeof(history_headers)), 0);
			history_finish(2);
			history_peer = -1;
		}
}

UT_TEST(test_required_peer_reconnect_cannot_reuse_the_old_history_report)
{
	static const int peers[] = {1, 65};

	for (unsigned index = 0; index < lengthof(peers); index++) {
		ClusterCtrcTxnKeyV1 continuation = {0};
		ClusterHeapFreezeCohortObservation original;
		ClusterHeapFreezeReportV2SlotShmem before;
		ClusterUndoCapacityStats stats;
		bool producer = false;
		int peer = peers[index];

		history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
		history_admit_peer(peer);
		before = history_lmon.freeze_slots[peer];
		UT_ASSERT_EQ(cluster_undo_capacity_history_observe(&original, &producer),
			CLUSTER_HEAP_FREEZE_COHORT_READY);
		UT_ASSERT(producer);
		history_schedule = 10;
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		UT_ASSERT_EQ(cluster_undo_capacity_history_current(&original),
			CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED);
		UT_ASSERT_EQ(history_peer_generation, 18);
		UT_ASSERT_EQ(history_lmon.freeze_slots[peer].capability_generation, 17);
		UT_ASSERT_EQ(history_lmon.freeze_slots[peer].view.report.report_sequence, 1);
		UT_ASSERT_EQ(history_lmon.report_sequence, 2);
		UT_ASSERT_EQ(memcmp(&before, &history_lmon.freeze_slots[peer], sizeof(before)), 0);
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		UT_ASSERT_EQ(stats.pool_wait_count, 1);
		UT_ASSERT_EQ(stats.wakeup_recheck_count, 1);
		UT_ASSERT_EQ(stats.refused_proof_count, 1);
		UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
		UT_ASSERT_EQ(memcmp(peer_headers_before, history_headers, sizeof(history_headers)), 0);
		history_finish(1);
		history_peer = -1;
	}
}

UT_TEST(test_expired_peer_report_breaks_the_original_protective_pair)
{
	static const int peers[] = {1, 65};

	for (unsigned index = 0; index < lengthof(peers); index++) {
		ClusterCtrcTxnKeyV1 continuation = {0};
		ClusterUndoCapacityStats stats;
		uint64 original_time;

		history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
		history_admit_peer(peers[index]);
		original_time = test_now_us;
		freshness_status = CLUSTER_HEAP_FREEZE_COHORT_INVALID;
		freshness_producer = false;
		history_schedule = 11;
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		UT_ASSERT_EQ(test_now_us, original_time + UINT64CONST(6000001));
		UT_ASSERT_EQ(freshness_status, CLUSTER_HEAP_FREEZE_COHORT_H_STALE);
		UT_ASSERT(freshness_producer);
		UT_ASSERT_EQ(history_lmon.report_sequence, 5);
		UT_ASSERT_EQ(history_lmon.freeze_slots[peers[index]].view.report.report_sequence, 3);
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		UT_ASSERT_EQ(stats.pool_wait_count, 3);
		UT_ASSERT_EQ(stats.wakeup_recheck_count, 3);
		UT_ASSERT_EQ(stats.refused_proof_count, 1);
		UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
		UT_ASSERT_EQ(memcmp(peer_headers_before, history_headers, sizeof(history_headers)), 0);
		history_finish(3);
		history_peer = -1;
	}
}

UT_TEST(test_expired_local_publication_cannot_sustain_the_original_report_wait)
{
	static const int peers[] = {1, 65};

	for (unsigned index = 0; index < lengthof(peers); index++) {
		ClusterCtrcTxnKeyV1 continuation = {0};
		ClusterUndoCapacityStats stats;
		UndoHorizonLocalHistoryPublication local_before;
		ClusterHeapFreezeReportV2SlotShmem peer_before;

		history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
		history_admit_peer(peers[index]);
		local_before = history_lmon.local_history;
		peer_before = history_lmon.freeze_slots[peers[index]];
		freshness_status = CLUSTER_HEAP_FREEZE_COHORT_INVALID;
		freshness_producer = true;
		history_schedule = 12;
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		UT_ASSERT_EQ(freshness_status, CLUSTER_HEAP_FREEZE_COHORT_H_STALE);
		UT_ASSERT(!freshness_producer);
		UT_ASSERT_EQ(history_lmon.report_sequence, 2);
		UT_ASSERT_EQ(memcmp(&local_before, &history_lmon.local_history, sizeof(local_before)), 0);
		UT_ASSERT_EQ(memcmp(&peer_before, &history_lmon.freeze_slots[peers[index]],
						   sizeof(peer_before)), 0);
		UT_ASSERT(cluster_undo_cleaner_capacity_stats_snapshot(&stats));
		UT_ASSERT_EQ(stats.pool_wait_count, 1);
		UT_ASSERT_EQ(stats.wakeup_recheck_count, 1);
		UT_ASSERT_EQ(stats.refused_proof_count, 1);
		UT_ASSERT(ctrc_bytes_zero(&continuation, sizeof(continuation)));
		UT_ASSERT_EQ(memcmp(peer_headers_before, history_headers, sizeof(history_headers)), 0);
		history_finish(1);
		history_peer = -1;
	}
}

int
main(void)
{
	/* Other fixtures execute these exact shared extracts' remote/F branches. */
	(void)undo_horizon_send_v2;
	(void)undo_horizon_v2_publish;
	(void)undo_horizon_publish_freeze_watermark;
	UT_PLAN(16);
	UT_RUN(test_actual_clock_report_reaches_original_wait_and_physical_supply);
	UT_RUN(test_actual_protective_pair_requires_distinct_reports);
	UT_RUN(test_history_non_h_rejections_count_the_actual_reason_union_once);
	UT_RUN(test_history_invalid_samples_do_not_fabricate_retention_counts);
	UT_RUN(test_history_positive_owner_and_retry_do_not_count_prior_retention);
	UT_RUN(test_actual_reader_exit_next_local_report_rechecks_supply);
	UT_RUN(test_actual_history_unknown_inputs_refuse_before_wait);
	UT_RUN(test_actual_history_cancel_retires_original_cv_and_wait_event);
	UT_RUN(test_actual_history_writer_change_cannot_bridge_complete_pass);
	UT_RUN(test_maximum_pool_requires_complete_bounded_physical_passes);
	UT_RUN(test_maximum_pool_clock_and_protector_reach_original_controller);
	UT_RUN(test_maximum_pool_last_image_cannot_be_hidden_by_a_protected_prefix);
	UT_RUN(test_required_peer_and_self_reports_both_advance_before_history_refusal);
	UT_RUN(test_required_peer_reconnect_cannot_reuse_the_old_history_report);
	UT_RUN(test_expired_peer_report_breaks_the_original_protective_pair);
	UT_RUN(test_expired_local_publication_cannot_sustain_the_original_report_wait);
	free(CtrcShared);
	UT_DONE();
	return ut_failed_count != 0;
}
