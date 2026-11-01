/* Actual CTRC certificate/handoff -> terminal sampler -> CURRENT GC and
 * allocator. Native status, admission, SCUR and WAL/flush are inherited
 * explicit boundaries. This does not execute BIND, SQL or disk/network I/O.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 */
#define CLUSTER_CTRC_REAL_ALLOCATOR_GC
#include "test_cluster_ctrc_capacity.c"
/* The inherited spin tracker only knows CTRC locks. The allocator's original
 * protected-map spinlock runs directly, uncontended in this process. */
#undef SpinLockAcquire
#undef SpinLockRelease
#define SpinLockAcquire(lock) S_LOCK(lock)
#define SpinLockRelease(lock) S_UNLOCK(lock)
#include "../../backend/cluster/cluster_tt_slot.c"

static ClusterTTSlotShmem gc_allocator;
static ClusterConf gc_conf;
ClusterConf *ClusterConfShmem = &gc_conf;
static unsigned gc_late_change;
static unsigned gc_allocator_lock_depth;

static bool
gc_allocator_lock(LWLock *lock, LWLockMode mode, bool acquire)
{
	if (lock != &gc_allocator.per_node[0].lock) {
		UT_ASSERT_EQ(gc_allocator_lock_depth, 0);
		return false;
	}
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(gc_allocator_lock_depth, acquire ? 0 : 1);
	UT_ASSERT(mode == LW_SHARED || mode == LW_EXCLUSIVE);
	gc_allocator_lock_depth = acquire ? 1 : 0;
	return true;
}

SCN
cluster_undo_retention_horizon(void)
{
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(gc_allocator_lock_depth, 0);
	return 1000;
}

bool
cluster_undo_horizon_epoch_fence_tripped(uint64 epoch)
{
	return epoch != cluster_epoch_get_current();
}

static ClusterCtrcTxnKeyV1
gc_reset(bool committed)
{
	ClusterCtrcTxnKeyV1 key;

	test_cluster_epoch = 23;
	key = reset_capacity_sample();
	MemSet(&gc_allocator, 0, sizeof(gc_allocator));
	MemSet(&gc_conf, 0, sizeof(gc_conf));
	gc_conf.node_count = 4;
	ClusterTTSlotShm = &gc_allocator;
	UT_ASSERT_EQ(gc_allocator_lock_depth, 0);
	external_lock_hook = gc_allocator_lock;
	cluster_tt_slot_rollover(0, key.segment_id, NULL);
	/* The initial incarnation is an explicit input matching the inherited
	 * publisher key. Allocation and every later reuse remain real; no test
	 * completion directly frees a slot. */
	LWLockAcquire(&gc_allocator.per_node[0].lock, LW_EXCLUSIVE);
	gc_allocator.per_node[0].slots[0].wrap = key.slot_wrap;
	LWLockRelease(&gc_allocator.per_node[0].lock);
	UT_ASSERT_EQ(cluster_tt_slot_alloc(1, key.xid), 0);
	UT_ASSERT_EQ(cluster_tt_slot_get_wrap(1, 0), key.slot_wrap);
	for (unsigned i = 1; i < TT_SLOTS_PER_SEGMENT; i++)
		UT_ASSERT_EQ(cluster_tt_slot_alloc(1, key.xid + i), i);
	if (committed)
		cluster_tt_slot_mark_committed(1, 0, key.xid, key.slot_wrap, 1000);
	else {
		cluster_tt_slot_mark_aborted(1, 0, key.xid, key.slot_wrap);
		sample_header.tt_slots[0].status = TT_SLOT_ABORTED;
		sample_header.tt_slots[0].commit_scn = InvalidScn;
		sample_commit = false;
		sample_abort = true;
	}
	gc_late_change = 0;
	return key;
}

static bool
gc_publish(ClusterCtrcOriginCertificateSnapshot *snapshot)
{
	UT_ASSERT(ctrc_origin_certificate_snapshot_index_locked(0, snapshot));
	return ctrc_cleaner_publish_certificate(snapshot);
}

static void
gc_run(bool expected_fence, unsigned recycled)
{
	ClusterUndoCleanerPassStats stats = { 0 };
	ClusterTTSlotAllocEntry others[TT_SLOTS_PER_SEGMENT - 1];

	memcpy(others, &gc_allocator.per_node[0].slots[1], sizeof(others));
	UT_ASSERT_EQ(cluster_tt_slot_gc_current_pass(1000, 23, &stats), expected_fence);
	UT_ASSERT_EQ(stats.segments_scanned, 1);
	UT_ASSERT_EQ(stats.shmem_tt_slots_gcd, recycled);
	UT_ASSERT_EQ(memcmp(others, &gc_allocator.per_node[0].slots[1], sizeof(others)), 0);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT_EQ(sample_admission_debt, 0);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(gc_allocator_lock_depth, 0);
	UT_ASSERT(!certificate_current && !certificate_pin);
	UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), 1);
	if (recycled == 0) {
		UT_ASSERT(gc_allocator.per_node[0].slots[0].status == CTS_COMMITTED
				  || gc_allocator.per_node[0].slots[0].status == CTS_ABORTED);
		UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 9001), INVALID_TT_SLOT_OFFSET);
	} else {
		UT_ASSERT_EQ(gc_allocator.per_node[0].slots[0].status, CTS_FREE);
		UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 9001), 0);
		UT_ASSERT_EQ(cluster_tt_slot_get_wrap(1, 0), 2);
		UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 9001), 0);
		UT_ASSERT_EQ(cluster_tt_slot_get_wrap(1, 0), 2);
	}
}

UT_TEST(test_actual_gc_requires_shared_handoff_after_certificate_flush)
{
	for (unsigned committed = 0; committed < 2; committed++) {
		ClusterCtrcOriginCertificateSnapshot snapshot;
		ClusterCtrcOriginEntry before;
		volatile bool caught = false;

		(void)gc_reset(committed != 0);
		gc_run(true, 0);
		before = ctrc_origin_entries()[0];
		certificate_error_after_flush = true;
		PG_TRY();
		{
			(void)gc_publish(&snapshot);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(certificate_wal_calls, 1);
		UT_ASSERT_EQ(certificate_flush_calls, 1);
		UT_ASSERT_EQ(sample_header.tt_slots[0].flags, TT_SLOT_FLAG_CTRC_RELEASE_PROVEN);
		UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[0], sizeof(before)), 0);
		gc_run(true, 0);
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_L11_RELEASE_SAMPLE), 0);
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_L12_RECYCLE), 0);
		certificate_error_after_flush = false;
		UT_ASSERT(ctrc_cleaner_publish_certificate(&snapshot));
		UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[0], sizeof(before)));
		UT_ASSERT_EQ(certificate_wal_calls, 1);
		UT_ASSERT_EQ(certificate_flush_calls, 1);
		gc_run(true, 1);
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_L11_RELEASE_SAMPLE), 1);
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_L12_RECYCLE), 1);
		UT_ASSERT_EQ(pg_atomic_read_u64(&gc_allocator.retention_recycle_count), committed);
		UT_ASSERT_EQ(pg_atomic_read_u64(&gc_allocator.retention_off_recycle_count), 0);
	}
}

UT_TEST(test_actual_gc_keeps_native_canonical_and_admission_release_guards)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterCtrcOriginCertificateSnapshot snapshot;
		ClusterCtrcTxnKeyV1 key = gc_reset(true);

		UT_ASSERT(gc_publish(&snapshot));
		switch (fault) {
		case 0: sample_active = true; break;
		case 1: sample_abort = true; break;
		case 2: sample_native_drift = true; sample_native_calls = 0; break;
		case 3: sample_header.tt_slots[0].xid++; break;
		case 4: sample_header.tt_slots[0].wrap++; break;
		case 5: sample_header.tt_slots[0].status = TT_SLOT_ABORTED; break;
		case 6: sample_header.tt_slots[0].commit_scn++; break;
		case 7: sample_header.tt_slots[0].flags = 0; break;
		case 8: sample_header.tt_slots[0].first_undo_block.raw[0] = 1; break;
		case 9: sample_admitted = false; break;
		case 10: sample_recheck = false; break;
		case 11: sample_release_failure = true; break;
		case 12:
		case 13:
			segment_inputs = true;
			segment_physical_id = 1;
			segment_native_xids[0] = key.xid;
			segment_native_states[0] = TT_SLOT_COMMITTED;
			segment_root_drift = fault == 12;
			segment_generation_drift = fault == 13;
			break;
		}
		gc_run(true, 0);
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_L11_RELEASE_SAMPLE), 0);
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_L12_RECYCLE), 0);
		UT_ASSERT_EQ(certificate_wal_calls, 1);
	}
}

static void
gc_allocator_change_after_sample(void)
{
	ClusterTTSlotAllocPerSegment *seg = &gc_allocator.per_node[0];

	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(gc_allocator_lock_depth, 0);
	if (gc_late_change == 0)
		test_cluster_epoch++;
	else {
		LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
		if (gc_late_change == 1)
			seg->binding_generation++;
		else
			seg->slots[0].xid++;
		LWLockRelease(&seg->lock);
	}
}

UT_TEST(test_actual_gc_rechecks_epoch_and_allocator_after_scur_release)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterCtrcOriginCertificateSnapshot snapshot;

		(void)gc_reset(true);
		UT_ASSERT(gc_publish(&snapshot));
		gc_late_change = fault;
		sample_release_hook = gc_allocator_change_after_sample;
		gc_run(fault != 0, 0);
		UT_ASSERT(sample_release_hook == NULL);
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_L11_RELEASE_SAMPLE), 1);
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_L12_RECYCLE), 0);
	}
}

UT_TEST(test_actual_sampler_error_unwinds_gc_and_both_original_scopes)
{
	ClusterCtrcOriginCertificateSnapshot snapshot;
	ClusterUndoCleanerPassStats stats = { 0 };
	volatile bool caught = false;

	(void)gc_reset(true);
	UT_ASSERT(gc_publish(&snapshot));
	sample_copy_error = true;
	PG_TRY();
	{
		(void)cluster_tt_slot_gc_current_pass(1000, 23, &stats);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(stats.shmem_tt_slots_gcd, 0);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT_EQ(sample_admission_debt, 0);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT(!certificate_current && !certificate_pin);
	UT_ASSERT_EQ(gc_allocator.per_node[0].slots[0].status, CTS_COMMITTED);
	sample_copy_error = false;
	gc_run(true, 1);
}

static bool gc_wait_publish;
static ClusterCtrcOriginCertificateSnapshot gc_wait_snapshot;
static ClusterTTSlotAllocEntry gc_wait_others[TT_SLOTS_PER_SEGMENT - 1];

static bool
gc_wait_lock(LWLock *lock, LWLockMode mode, bool acquire)
{
	if (lock == &gc_allocator.per_node[0].lock)
		UT_ASSERT(!native_cleaner_lock_held);
	if (gc_allocator_lock(lock, mode, acquire))
		return true;
	return capacity_cleaner_lock(lock, mode, acquire);
}

static ClusterCtrcTxnKeyV1
gc_wait_reset(bool committed)
{
	ClusterCtrcTxnKeyV1 key = gc_reset(committed);

	cluster_enabled = true;
	external_lock_hook = gc_wait_lock;
	MemSet(&native_cleaner_state, 0, sizeof(native_cleaner_state));
	pg_atomic_init_u64(&native_cleaner_state.capacity_wait_refused_context, 0);
	pg_atomic_init_u64(&native_cleaner_state.capacity_wait_refused_proof, 0);
	pg_atomic_init_u64(&native_cleaner_state.capacity_wait_cancelled, 0);
	for (unsigned i = 0; i < CLUSTER_UNDO_CLEANER_WORKER_TYPES; i++) {
		UndoCleanerWorkerState *worker = &native_cleaner_state.workers[i];
		worker->status = UNDO_CLEANER_READY;
		worker->latch = (Latch *)&native_cleaner_state;
		worker->incarnation_generation = i == 0 ? 83 : 71;
	}
	capacity_sleeps = capacity_cancels = 0;
	capacity_subscribed = capacity_cancel = false;
	capacity_wait_event = 41;
	capacity_sleep_event = 0;
	gc_wait_publish = false;
	MemSet(&gc_wait_snapshot, 0, sizeof(gc_wait_snapshot));
	memcpy(gc_wait_others, &gc_allocator.per_node[0].slots[1], sizeof(gc_wait_others));
	allocator_gc_wait_scheduler = NULL;
	return key;
}

/* Run the real coordinator pass without granting an allocator reservation.
 * A CV return remains only a request to re-enter the original allocator. */
static void
gc_wait_pass(unsigned recycled)
{
	ClusterUndoCleanerPassStats stats = { 0 };
	ClusterTTSlotAllocEntry others[TT_SLOTS_PER_SEGMENT - 1];

	memcpy(others, &gc_allocator.per_node[0].slots[1], sizeof(others));
	UT_ASSERT(cluster_tt_slot_gc_current_pass(1000, 23, &stats));
	UT_ASSERT_EQ(stats.segments_scanned, 1);
	UT_ASSERT_EQ(stats.shmem_tt_slots_gcd, recycled);
	UT_ASSERT_EQ(memcmp(others, &gc_allocator.per_node[0].slots[1], sizeof(others)), 0);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT_EQ(sample_admission_debt, 0);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(gc_allocator_lock_depth, 0);
	UT_ASSERT(!native_cleaner_lock_held && !certificate_current && !certificate_pin);
}

static void
gc_wait_schedule(void)
{
	UT_ASSERT(capacity_subscribed && undo_cleaner_capacity_context_safe());
	UT_ASSERT_EQ(capacity_wait_event, WAIT_EVENT_CLUSTER_UNDO_CAPACITY_WAIT);
	gc_wait_pass(0);
	UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 9001), INVALID_TT_SLOT_OFFSET);
	if (!gc_wait_publish)
		return;
	UT_ASSERT(gc_publish(&gc_wait_snapshot));
	UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[0], sizeof(ClusterCtrcOriginEntry)));
	UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 9001), INVALID_TT_SLOT_OFFSET);
	gc_wait_pass(1);
	UT_ASSERT_EQ(gc_allocator.per_node[0].slots[0].status, CTS_FREE);
}

static void
gc_wait_check(unsigned sleeps, unsigned repolls, unsigned cancelled)
{
	UT_ASSERT_EQ(capacity_sleeps, sleeps);
	UT_ASSERT_EQ(capacity_cancels, sleeps);
	UT_ASSERT(!capacity_subscribed && !native_cleaner_lock_held);
	UT_ASSERT_EQ(capacity_wait_event, 41);
	UT_ASSERT_EQ(capacity_sleep_event, WAIT_EVENT_CLUSTER_UNDO_CAPACITY_WAIT);
	UT_ASSERT_EQ(native_cleaner_state.capacity_wait_entered, sleeps);
	UT_ASSERT_EQ(native_cleaner_state.capacity_wait_repolled, repolls);
	UT_ASSERT_EQ(pg_atomic_read_u64(&native_cleaner_state.capacity_wait_cancelled), cancelled);
	UT_ASSERT_EQ(pg_atomic_read_u64(&native_cleaner_state.capacity_wait_refused_context), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&native_cleaner_state.capacity_wait_refused_proof), 0);
	UT_ASSERT_EQ(sample_holds, sample_releases);
	UT_ASSERT_EQ(sample_enters, sample_leaves);
	UT_ASSERT_EQ(sample_admission_debt, 0);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(gc_allocator_lock_depth, 0);
	UT_ASSERT_EQ(memcmp(gc_wait_others, &gc_allocator.per_node[0].slots[1],
					   sizeof(gc_wait_others)), 0);
}

static void
gc_wait_allocate(const ClusterCtrcTxnKeyV1 *key, bool committed)
{
	UT_ASSERT_EQ(cluster_tt_slot_current_segment(0), key->segment_id);
	UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 9001), key->slot_offset);
	UT_ASSERT_EQ(cluster_tt_slot_get_wrap(1, key->slot_offset), key->slot_wrap + 1);
	UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 9001), key->slot_offset);
	UT_ASSERT_EQ(cluster_tt_slot_get_wrap(1, key->slot_offset), key->slot_wrap + 1);
	UT_ASSERT_EQ(certificate_wal_calls, 1);
	UT_ASSERT_EQ(certificate_flush_calls, 1);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_L12_RECYCLE), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&gc_allocator.retention_recycle_count), committed);
	UT_ASSERT_EQ(pg_atomic_read_u64(&gc_allocator.retention_off_recycle_count), 0);
	UT_ASSERT_EQ(memcmp(gc_wait_others, &gc_allocator.per_node[0].slots[1],
					   sizeof(gc_wait_others)), 0);
	allocator_gc_wait_scheduler = NULL;
}

UT_TEST(test_original_capacity_cv_reenters_allocator_only_after_real_handoff_and_gc)
{
	for (unsigned committed = 0; committed < 2; committed++) {
		ClusterCtrcTxnKeyV1 key = gc_wait_reset(committed != 0);
		ClusterCtrcTxnKeyV1 continuation = { 0 };

		allocator_gc_wait_scheduler = gc_wait_schedule;
		UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		UT_ASSERT_EQ(memcmp(&continuation, &key, sizeof(key)), 0);
		gc_wait_check(1, 1, 0);
		UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 9001), INVALID_TT_SLOT_OFFSET);
		UT_ASSERT_EQ(certificate_wal_calls, 0);
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_L12_RECYCLE), 0);
		gc_wait_publish = true;
		UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		gc_wait_check(2, 2, 0);
		gc_wait_allocate(&key, committed != 0);
	}
}

UT_TEST(test_certificate_flush_error_unwinds_original_cv_and_replays_before_gc)
{
	for (unsigned committed = 0; committed < 2; committed++) {
		ClusterCtrcTxnKeyV1 key = gc_wait_reset(committed != 0);
		/* The original wait writes this across PG_TRY/longjmp. */
		static ClusterCtrcTxnKeyV1 continuation;
		ClusterCtrcOriginEntry before = ctrc_origin_entries()[0];
		volatile bool caught = false;

		MemSet(&continuation, 0, sizeof(continuation));
		allocator_gc_wait_scheduler = gc_wait_schedule;
		gc_wait_publish = true;
		certificate_error_after_flush = true;
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
		gc_wait_check(1, 0, 0);
		UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[0], sizeof(before)), 0);
		UT_ASSERT_EQ(sample_header.tt_slots[0].flags, TT_SLOT_FLAG_CTRC_RELEASE_PROVEN);
		UT_ASSERT_EQ(certificate_flush_calls, 1);
		gc_wait_pass(0);
		UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 9001), INVALID_TT_SLOT_OFFSET);
		certificate_error_after_flush = false;
		UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		gc_wait_check(2, 1, 0);
		gc_wait_allocate(&key, committed != 0);
	}
}

UT_TEST(test_cancelled_original_cv_preserves_publisher_until_actual_handoff_and_gc)
{
	for (unsigned committed = 0; committed < 2; committed++) {
		ClusterCtrcTxnKeyV1 key = gc_wait_reset(committed != 0);
		/* The original wait writes this across PG_TRY/longjmp. */
		static ClusterCtrcTxnKeyV1 continuation;
		ClusterCtrcOriginEntry before = ctrc_origin_entries()[0];
		volatile bool caught = false;

		MemSet(&continuation, 0, sizeof(continuation));
		allocator_gc_wait_scheduler = gc_wait_schedule;
		gc_wait_publish = true;
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
		gc_wait_check(1, 0, 1);
		UT_ASSERT_EQ(memcmp(&before, &ctrc_origin_entries()[0], sizeof(before)), 0);
		UT_ASSERT_EQ(certificate_wal_calls, 0);
		UT_ASSERT_EQ(sample_header.tt_slots[0].flags, 0);
		gc_wait_pass(0);
		UT_ASSERT_EQ(cluster_tt_slot_alloc(1, 9001), INVALID_TT_SLOT_OFFSET);
		capacity_cancel = false;
		UT_ASSERT(cluster_undo_cleaner_wait_for_capacity(1, &continuation));
		gc_wait_check(2, 1, 1);
		gc_wait_allocate(&key, committed != 0);
	}
}

int
main(void)
{
	/* Keep inherited assert-only helpers referenced in release builds. */
	(void)native_lock_held_in_mode;
	(void)native_lock_is_held;
	UT_PLAN(7);
	UT_RUN(test_actual_gc_requires_shared_handoff_after_certificate_flush);
	UT_RUN(test_actual_gc_keeps_native_canonical_and_admission_release_guards);
	UT_RUN(test_actual_gc_rechecks_epoch_and_allocator_after_scur_release);
	UT_RUN(test_actual_sampler_error_unwinds_gc_and_both_original_scopes);
	UT_RUN(test_original_capacity_cv_reenters_allocator_only_after_real_handoff_and_gc);
	UT_RUN(test_certificate_flush_error_unwinds_original_cv_and_replays_before_gc);
	UT_RUN(test_cancelled_original_cv_preserves_publisher_until_actual_handoff_and_gc);
	free(CtrcShared);
	UT_DONE();
	return ut_failed_count != 0;
}
