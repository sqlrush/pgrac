/*-------------------------------------------------------------------------
 *
 * test_cluster_ctrc_cleaner.c
 *    Real shared CTRC selection and reply-progress regression tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_ctrc_cleaner.c
 *
 * NOTES
 *    This is a pgrac-original file. No CLUSTER_CTRC_UNIT_TEST: the actual
 *    shared selectors and reply consumers are compiled. Only allocation,
 *    locks, error reporting and wakeup are process-boundary fixtures. This
 *    is not a disk or network end-to-end test.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>
#include <time.h>
#include <sys/mman.h>
#include <unistd.h>
#include "storage/lwlock.h"
#include "storage/spin.h"

static unsigned spin_acquisitions;
static unsigned sleepable_acquisitions;
static unsigned held_count;
static const void *held_locks[2];
static void test_lock_enter(const void *lock, bool sleepable);
static void test_lock_leave(const void *lock);

static void
pg_attribute_unused() test_spin_acquire(volatile slock_t *lock)
{
	S_LOCK(lock);
	test_lock_enter((const void *)lock, false);
}

static void
pg_attribute_unused() test_spin_release(volatile slock_t *lock)
{
	test_lock_leave((const void *)lock);
	S_UNLOCK(lock);
}

/* Observe the real shared bodies, including the old spinlock baseline.
 * LWLock scheduling itself is PostgreSQL, not implemented by this fixture. */
#undef SpinLockAcquire
#undef SpinLockRelease
#define SpinLockAcquire(lock) test_spin_acquire(lock)
#define SpinLockRelease(lock) test_spin_release(lock)

static int test_clock_gettime(clockid_t clock_id, struct timespec *ts);
static Size table_visits[4];
#define CLUSTER_CTRC_TEST_TABLE_VISIT(kind) (table_visits[(kind)]++)
#define clock_gettime test_clock_gettime
#include "../../backend/cluster/cluster_terminal_ref_census.c"
#undef clock_gettime

#undef printf
#undef fprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

int NBuffers = 16;
int MaxBackends = 8;
int cluster_node_id = 0;
static unsigned wake_count;
static uint64 test_now_us = UINT64_C(1000000);
static uint32 test_capability_generation = 19;
static uint64 test_cluster_epoch = 13;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
volatile sig_atomic_t InterruptPending;
volatile uint32 InterruptHoldoffCount;
static unsigned allocated;
static bool allocation_failure;
static unsigned flush_calls, durability_calls, barrier_calls;
static XLogRecPtr test_flush_lsn;
static void (*durability_hook)(unsigned call);
static void (*barrier_hook)(void);

void *
palloc_extended(Size size, int flags)
{
	void *memory;
	if (held_count != 0)
		abort();
	if (allocation_failure)
		return NULL;
	memory = (flags & MCXT_ALLOC_ZERO) != 0 ? calloc(1, size) : malloc(size);
	if (memory != NULL)
		allocated++;
	return memory;
}

void
pfree(void *memory)
{
	if (held_count != 0 || allocated == 0 || memory == NULL)
		abort();
	allocated--;
	free(memory);
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
ProcessInterrupts(void)
{
	if (held_count != 0)
		abort();
	pg_re_throw();
}

void
XLogFlush(XLogRecPtr lsn)
{
	if (held_count != 0)
		abort();
	flush_calls++;
	if (lsn > test_flush_lsn)
		test_flush_lsn = lsn;
}

XLogRecPtr
GetFlushRecPtr(TimeLineID *tli)
{
	if (held_count != 0 || tli != NULL)
		abort();
	durability_calls++;
	if (durability_hook != NULL)
		durability_hook(durability_calls);
	return test_flush_lsn;
}

XLogRecPtr
cluster_sf_observed_origin_durable_lsn(int origin pg_attribute_unused())
{
	if (held_count != 0)
		abort();
	return test_flush_lsn;
}

void
pg_usleep(long microsec)
{
	if (held_count != 0 || microsec != 1000L)
		abort();
	barrier_calls++;
	if (barrier_hook != NULL)
		barrier_hook();
	(void)cluster_ctrc_test_barrier_control(CTRC_TEST_BARRIER_ACK_DURABLE, false);
}

static void
test_lock_enter(const void *lock, bool sleepable)
{
	if (CtrcShared == NULL || held_count >= lengthof(held_locks)
		|| (lock != &CtrcShared->origin_lock && lock != &CtrcShared->participant_lock
			&& lock != &CtrcShared->receipt_lock)
		|| (held_count != 0
			&& (lock != &CtrcShared->receipt_lock
				|| (held_locks[0] != &CtrcShared->origin_lock
					&& held_locks[0] != &CtrcShared->participant_lock))))
		abort();
	held_locks[held_count++] = lock;
	if (sleepable)
		sleepable_acquisitions++;
	else
		spin_acquisitions++;
}

static void
test_lock_leave(const void *lock)
{
	if (held_count == 0 || held_locks[held_count - 1] != lock)
		abort();
	held_locks[--held_count] = NULL;
}

void
LWLockInitialize(LWLock *lock, int tranche_id)
{
	MemSet(lock, 0, sizeof(*lock));
	lock->tranche = tranche_id;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (mode != LW_EXCLUSIVE)
		abort();
	test_lock_enter(lock, true);
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	test_lock_leave(lock);
}

static int
test_clock_gettime(clockid_t clock_id pg_attribute_unused(), struct timespec *ts)
{
	ts->tv_sec = test_now_us / UINT64_C(1000000);
	ts->tv_nsec = (test_now_us % UINT64_C(1000000)) * 1000;
	return 0;
}

int
s_lock(volatile slock_t *lock pg_attribute_unused(), const char *file pg_attribute_unused(),
	   int line pg_attribute_unused(), const char *func pg_attribute_unused())
{
	abort();
}

int
cluster_conf_declared_node_count_early(void)
{
	return 4;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found)
{
	*found = false;
	return calloc(1, size);
}

void
cluster_undo_cleaner_wakeup(void)
{
	wake_count++;
}

bool
errstart(int level pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}

int
errcode(int code)
{
	return code;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	abort();
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

static void
reset_fixture(void)
{
	if (held_count != 0 || allocated != 0)
		abort();
	free(CtrcShared);
	CtrcShared = NULL;
	cluster_ctrc_shmem_init();
	wake_count = 0;
	test_now_us = UINT64_C(1000000);
	spin_acquisitions = 0;
	sleepable_acquisitions = 0;
	allocation_failure = false;
	flush_calls = durability_calls = barrier_calls = 0;
	test_flush_lsn = 0;
	durability_hook = NULL;
	barrier_hook = NULL;
	InterruptPending = 0;
}

static ClusterCtrcOriginEntry *
seed_origin(unsigned index, unsigned participants)
{
	ClusterCtrcTxnKeyV1 key = { 0 };
	ClusterCtrcParticipantIdentity identity = { 0 };
	ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];
	uint32 grant = 0;
	unsigned node;

	key.format_version = CLUSTER_CTRC_FORMAT_VERSION;
	key.owner_instance = 1;
	key.segment_id = index / TT_SLOTS_PER_SEGMENT + 1;
	key.segment_generation = 1;
	key.slot_offset = index % TT_SLOTS_PER_SEGMENT;
	key.slot_wrap = 1;
	key.xid = 7001 + index;
	key.cluster_epoch = test_cluster_epoch;
	key.system_identifier = 42;
	key.origin_boot_incarnation = 1101;
	key.formation_epoch = 23;
	key.admission_record_generation = 29;
	key.root_descriptor_incarnation = 43;
	key.root_id = 37;
	key.root_generation = 43;
	if (cluster_ctrc_origin_open_active(origin, &key, 7) != CLUSTER_CTRC_ORIGIN_OPENED)
		abort();
	for (node = 0; node < participants; node++) {
		identity.node_id = node;
		identity.boot_incarnation = 1101 + node;
		identity.capability_record_generation = test_capability_generation;
		identity.formation_epoch = 23;
		identity.admission_record_generation = 29;
		if (cluster_ctrc_origin_record_touched(origin, &identity, CTRC_PROOF_ACTIVE, &grant)
			!= CLUSTER_CTRC_TOUCH_RECORDED)
			abort();
	}
	if (!cluster_ctrc_origin_begin_seal_entry(origin, 41 + index))
		abort();
	return origin;
}

UT_TEST(test_new_seals_do_not_starve_old_pending)
{
	ClusterCtrcCloseDispatch dispatch;
	unsigned i;
	unsigned revisited = 0;

	reset_fixture();
	for (i = 0; i < 256; i++) {
		ClusterCtrcOriginEntry *origin;

		seed_origin(i, 1);
		UT_ASSERT(cluster_ctrc_origin_next_close_dispatch_shared(&dispatch));
		if (i != 0 && dispatch.key.xid == 7001)
			revisited++;
		origin = &ctrc_origin_entries()[dispatch.key.xid - 7001];
		UT_ASSERT(cluster_ctrc_origin_note_close_reply_entry(origin, 0, dispatch.request_id,
															 CTRC_SEAL_REPLY_PENDING_DRAIN));
	}
	UT_ASSERT(revisited > 0);
}

UT_TEST(test_pending_participant_does_not_starve_peer)
{
	ClusterCtrcCloseDispatch dispatch;
	unsigned seen = 0;
	unsigned i;

	reset_fixture();
	seed_origin(0, 2);
	for (i = 0; i < 4; i++) {
		UT_ASSERT(cluster_ctrc_origin_next_close_dispatch_shared(&dispatch));
		seen |= 1U << dispatch.participant.node_id;
		UT_ASSERT(cluster_ctrc_origin_note_close_reply_shared(
			dispatch.request_id, dispatch.participant.node_id, CTRC_SEAL_REPLY_PENDING_DRAIN));
	}
	UT_ASSERT_EQ(seen, 3);
}

UT_TEST(test_duplicate_pending_is_accepted_without_wakeup)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcOriginEntry before;
	unsigned wakes;
	uint64 progress;

	reset_fixture();
	origin = seed_origin(0, 1);
	UT_ASSERT(cluster_ctrc_origin_arm_close_entry(origin, 0, 99));
	UT_ASSERT(cluster_ctrc_origin_note_close_reply_shared(99, 0, CTRC_SEAL_REPLY_PENDING_DRAIN));
	before = *origin;
	wakes = wake_count;
	progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	UT_ASSERT(wakes > 0);
	UT_ASSERT(cluster_ctrc_origin_note_close_reply_shared(99, 0, CTRC_SEAL_REPLY_PENDING_DRAIN));
	UT_ASSERT_EQ(memcmp(origin, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(wake_count, wakes);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_PENDING_DUPLICATE), 1);
}

UT_TEST(test_uncorrelated_reply_cannot_progress)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcOriginEntry before;

	reset_fixture();
	origin = seed_origin(0, 1);
	UT_ASSERT(cluster_ctrc_origin_arm_close_entry(origin, 0, 99));
	before = *origin;
	UT_ASSERT(!cluster_ctrc_origin_note_close_reply_shared(100, 0, CTRC_SEAL_REPLY_PENDING_DRAIN));
	UT_ASSERT_EQ(memcmp(origin, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(wake_count, 0);
}

UT_TEST(test_batch_is_bounded_oldest_first_and_fair)
{
	bool seen[160] = { false };
	ClusterCtrcCloseDispatch dispatch;
	unsigned pass;
	unsigned i;

	reset_fixture();
	for (i = 0; i < lengthof(seen); i++)
		seed_origin(i, 1);
	CtrcBatch.active = true;
	for (pass = 0; pass < 5; pass++) {
		bool in_batch[160] = { false };
		unsigned count = 0;

		ctrc_dispatch_batch_collect();
		UT_ASSERT_EQ(CtrcBatch.dispatch_count, 64);
		for (i = 0; i < 32; i++)
			UT_ASSERT_EQ(CtrcBatch.dispatch_index[i], i);
		while (cluster_ctrc_origin_next_close_dispatch_shared(&dispatch)) {
			unsigned index = dispatch.key.xid - 7001;

			UT_ASSERT(index < lengthof(seen));
			UT_ASSERT(!in_batch[index]);
			in_batch[index] = true;
			seen[index] = true;
			count++;
			/* Deliberately never ACK the oldest or any peer. */
			UT_ASSERT(cluster_ctrc_origin_note_close_reply_shared(dispatch.request_id, 0,
																  CTRC_SEAL_REPLY_PENDING_DRAIN));
		}
		UT_ASSERT_EQ(count, 64);
		UT_ASSERT(!cluster_ctrc_origin_next_close_dispatch_shared(&dispatch));
	}
	for (i = 0; i < lengthof(seen); i++)
		UT_ASSERT(seen[i]);
	CtrcBatch.active = false;
}

UT_TEST(test_batch_hint_rechecks_current_identity)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcCloseDispatch dispatch;
	ClusterCtrcTxnKeyV1 previous;

	reset_fixture();
	origin = seed_origin(0, 1);
	previous = origin->key;
	CtrcBatch.active = true;
	ctrc_dispatch_batch_collect();
	/* Controlled slot replacement between selection and arm. No proof is
	 * carried by the batch; arm must build only the new current identity. */
	MemSet(origin, 0, sizeof(*origin));
	origin = seed_origin(0, 1);
	origin->key.slot_wrap++;
	origin->key.xid++;
	origin->seal_generation++;
	UT_ASSERT(cluster_ctrc_origin_next_close_dispatch_shared(&dispatch));
	UT_ASSERT_EQ(dispatch.key.xid, origin->key.xid);
	UT_ASSERT_EQ(dispatch.key.slot_wrap, origin->key.slot_wrap);
	UT_ASSERT(memcmp(&dispatch.key, &previous, sizeof(previous)) != 0);
	UT_ASSERT(!cluster_ctrc_origin_next_close_dispatch_shared(&dispatch));
	CtrcBatch.active = false;
}

UT_TEST(test_scan_budget_prevents_same_pending_item_revisit)
{
	ClusterCtrcTxnKeyV1 key;
	ClusterCtrcOriginEntry *origin;
	unsigned count = 0;

	reset_fixture();
	origin = seed_origin(0, 0);
	/* Real OPEN entry: the terminal sample may retain it. The selector
	 * must not rescan the same entry during this bounded pass. */
	origin->state = CTRC_ORIGIN_OPEN;
	CtrcBatch.active = true;
	CtrcBatch.remaining[CTRC_SCAN_OPEN] = CtrcShared->origin_key_entries;
	while (cluster_ctrc_origin_next_open_shared(&key)) {
		count++;
		UT_ASSERT(count <= 1);
		if (count > 1)
			break;
	}
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(CtrcBatch.remaining[CTRC_SCAN_OPEN], 0);
	UT_ASSERT(!cluster_ctrc_origin_next_open_shared(&key));
	CtrcBatch.active = false;
}

UT_TEST(test_observed_age_resets_without_releasing_authority)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcOriginEntry before;

	reset_fixture();
	origin = seed_origin(0, 1);
	before = *origin;
	ctrc_dispatch_batch_collect();
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_DISPATCH_BACKLOG), 1);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_PENDING_OBSERVED_AGE_MS), 0);
	test_now_us += UINT64_C(9000000);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_OBSERVATION_AGE_MS), 9000);
	ctrc_dispatch_batch_collect();
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_PENDING_OBSERVED_AGE_MS), 9000);
	UT_ASSERT_EQ(memcmp(origin, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), 0);
	origin->seal_generation++;
	ctrc_dispatch_batch_collect();
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_PENDING_OBSERVED_AGE_MS), 0);
	MemSet(origin, 0, sizeof(*origin));
	ctrc_dispatch_batch_collect();
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_DISPATCH_BACKLOG), 0);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_PENDING_OBSERVED_AGE_MS), 0);
}

UT_TEST(test_participant_duplicate_pending_does_not_wake)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcParticipantEntry *participant;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint64 index;
	uint64 progress;
	unsigned wakes;
	uint16 reason;

	reset_fixture();
	origin = seed_origin(0, 1);
	UT_ASSERT(ctrc_participant_index(&origin->key, 0, &index));
	participant = &ctrc_participant_entries()[index];
	UT_ASSERT_EQ(cluster_ctrc_participant_open(participant, &origin->key, origin->grant_generation,
											   &origin->touched[0]),
				 CLUSTER_CTRC_PARTICIPANT_OPENED);
	/* A single prepared, not-yet-applied publication holds the close. This
	 * fixture exercises the reply contract, not receipt/disk durability. */
	participant->receipt_count = 1;
	participant->prepared_count = 1;
	participant->last_key_sequence = 1;
	participant->next_key_sequence = 2;
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
					 &origin->key, &origin->touched[0], origin->grant_generation,
					 origin->seal_generation, CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
				 CTRC_SEAL_REPLY_PENDING_DRAIN);
	wakes = wake_count;
	progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	UT_ASSERT(wakes > 0);
	UT_ASSERT_EQ(cluster_ctrc_participant_request_shared(
					 &origin->key, &origin->touched[0], origin->grant_generation,
					 origin->seal_generation, CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
				 CTRC_SEAL_REPLY_PENDING_DRAIN);
	UT_ASSERT_EQ(wake_count, wakes);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
	UT_ASSERT_EQ(participant->prepared_count, 1);
	UT_ASSERT_EQ(participant->state, CTRC_PARTICIPANT_CLOSED_DRAINING);
}

UT_TEST(test_real_ack_and_certificate_consumers_are_idempotent)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcOriginCertificateSnapshot snapshot;
	uint64 progress;
	unsigned wakes;
	unsigned node;

	reset_fixture();
	origin = seed_origin(0, 2);
	for (node = 0; node < 2; node++) {
		ClusterCtrcParticipantEntry participant = { 0 };
		ClusterCtrcLocalReleaseAckV1 summary = { 0 };
		ClusterCtrcLocalReleaseAckV1 ack;
		uint16 reason;

		UT_ASSERT_EQ(cluster_ctrc_participant_open(&participant, &origin->key,
												   origin->grant_generation,
												   &origin->touched[node]),
					 CLUSTER_CTRC_PARTICIPANT_OPENED);
		UT_ASSERT(cluster_ctrc_origin_arm_close_entry(origin, node, 100 + node));
		/* Actual zero-range ACK encoder, not an ACK-shaped literal. */
		UT_ASSERT_EQ(cluster_ctrc_participant_request_apply(
						 &participant, &summary, &origin->key, &origin->touched[node],
						 origin->grant_generation, origin->seal_generation,
						 CTRC_SEAL_CLOSE_AND_CLEAN, &reason, &ack),
					 CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK);
		UT_ASSERT(cluster_ctrc_origin_ack_land_shared(100 + node, &ack));
		progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
		wakes = wake_count;
		UT_ASSERT(cluster_ctrc_origin_ack_land_shared(100 + node, &ack));
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
		UT_ASSERT_EQ(wake_count, wakes);
		if (node == 0)
			UT_ASSERT(!ctrc_origin_certificate_snapshot_index_locked(0, &snapshot));
	}
	UT_ASSERT(!ctrc_origin_dispatchable_locked(origin));
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_CERTIFYING);
	UT_ASSERT(ctrc_origin_certificate_snapshot_index_locked(0, &snapshot));
	/* This call tests the in-memory edge AFTER the separately tested durable
	 * publication point. It does not simulate a successful disk write. */
	UT_ASSERT(cluster_ctrc_origin_certificate_commit_entry(origin, &snapshot));
	UT_ASSERT(cluster_ctrc_origin_arm_certificate_entry(origin, 0, 200));
	UT_ASSERT(cluster_ctrc_origin_note_certificate_reply_shared(
		200, 0, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED));
	progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	wakes = wake_count;
	UT_ASSERT(cluster_ctrc_origin_note_certificate_reply_shared(
		200, 0, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED));
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
	UT_ASSERT_EQ(wake_count, wakes);
	UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_RELEASE_PROVEN);
	UT_ASSERT(cluster_ctrc_origin_arm_certificate_entry(origin, 1, 201));
	UT_ASSERT(cluster_ctrc_origin_note_certificate_reply_shared(
		201, 1, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED));
	UT_ASSERT(ctrc_bytes_zero(origin, sizeof(*origin)));
	UT_ASSERT(!cluster_ctrc_origin_note_certificate_reply_shared(
		200, 0, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED));
	UT_ASSERT(ctrc_bytes_zero(origin, sizeof(*origin)));
}

UT_TEST(test_receipt_and_ack_scans_do_not_retry_retained_entries)
{
	ClusterCtrcParticipantEntry participant_copy;
	ClusterCtrcReceipt receipt_copy;
	uint64 participant_index;
	uint64 receipt_index;
	bool seen[2] = { false };
	unsigned count = 0;
	unsigned i;

	reset_fixture();
	for (i = 0; i < 2; i++) {
		ClusterCtrcOriginEntry *origin = seed_origin(i, 1);
		ClusterCtrcParticipantEntry *participant;
		ClusterCtrcReceipt *receipt = &ctrc_receipt_entries()[i];

		UT_ASSERT(ctrc_participant_index(&origin->key, 0, &participant_index));
		participant = &ctrc_participant_entries()[participant_index];
		UT_ASSERT_EQ(cluster_ctrc_participant_open(participant, &origin->key,
												   origin->grant_generation, &origin->touched[0]),
					 CLUSTER_CTRC_PARTICIPANT_OPENED);
		participant->receipt_count = 1;
		participant->applied_count = 1;
		participant->last_key_sequence = 1;
		participant->next_key_sequence = 2;
		UT_ASSERT_EQ(cluster_ctrc_participant_close(participant, &origin->touched[0],
													origin->grant_generation,
													origin->seal_generation),
					 CLUSTER_CTRC_CLOSE_PENDING_DRAIN);
		receipt->key = origin->key;
		receipt->publication.grant_generation = origin->grant_generation;
		pg_atomic_write_u32((pg_atomic_uint32 *)&receipt->state, CTRC_RECEIPT_APPLIED);
	}
	CtrcBatch.active = true;
	CtrcBatch.remaining[CTRC_SCAN_RECEIPT] = CtrcShared->receipt_entries;
	while (ctrc_cleaner_next_applied_receipt(&participant_copy, &receipt_copy, &participant_index,
											 &receipt_index)) {
		UT_ASSERT(receipt_index < 2);
		UT_ASSERT(!seen[receipt_index]);
		seen[receipt_index] = true;
		if (++count > 2)
			break;
		/* Leave APPLIED unchanged to model a failed physical clean. */
	}
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT_EQ(CtrcBatch.remaining[CTRC_SCAN_RECEIPT], 0);
	for (i = 0; i < 2; i++) {
		ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[i];
		ClusterCtrcParticipantEntry *participant;

		UT_ASSERT(ctrc_participant_index(&origin->key, 0, &participant_index));
		participant = &ctrc_participant_entries()[participant_index];
		participant->applied_count = 0;
		participant->cleaned_count = 1;
	}
	count = 0;
	MemSet(seen, 0, sizeof(seen));
	CtrcBatch.remaining[CTRC_SCAN_ACK] = CtrcShared->participant_key_entries;
	while (ctrc_participant_find_ack_ready(&participant_copy, &participant_index)) {
		unsigned index = participant_copy.key.xid - 7001;

		UT_ASSERT(index < 2);
		UT_ASSERT(!seen[index]);
		seen[index] = true;
		if (++count > 2)
			break;
		/* The ACK provider may retain this snapshot on durability failure. */
	}
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT_EQ(CtrcBatch.remaining[CTRC_SCAN_ACK], 0);
	CtrcBatch.active = false;
}

UT_TEST(test_certificate_scan_bounds_unpublished_candidates)
{
	ClusterCtrcOriginCertificateSnapshot snapshot;
	bool seen[2] = { false };
	unsigned count = 0;
	unsigned i;

	reset_fixture();
	for (i = 0; i < 2; i++) {
		ClusterCtrcOriginEntry *origin = seed_origin(i, 0);

		UT_ASSERT(!ctrc_origin_dispatchable_locked(origin));
		UT_ASSERT_EQ(origin->state, CTRC_ORIGIN_CERTIFYING);
	}
	CtrcBatch.active = true;
	CtrcBatch.remaining[CTRC_SCAN_CERTIFICATE] = CtrcShared->origin_key_entries;
	while (ctrc_origin_next_certificate_snapshot_shared(&snapshot)) {
		UT_ASSERT(snapshot.origin_index < 2);
		UT_ASSERT(!seen[snapshot.origin_index]);
		seen[snapshot.origin_index] = true;
		if (++count > 2)
			break;
		/* No certificate is published; repeated selection is still bounded. */
	}
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT_EQ(CtrcBatch.remaining[CTRC_SCAN_CERTIFICATE], 0);
	UT_ASSERT_EQ(ctrc_origin_entries()[0].state, CTRC_ORIGIN_CERTIFYING);
	UT_ASSERT_EQ(ctrc_origin_entries()[1].state, CTRC_ORIGIN_CERTIFYING);
	CtrcBatch.active = false;
}

/* Build one real pending receipt, cancel it without publishing a page
 * reference, then close and encode its actual nonempty ACK. The fixture
 * owns no disk or page I/O and must not claim durability of an applied row. */
static bool
seed_cancelled_ack_at(unsigned origin_index, ClusterCtrcParticipantEntry *snapshot,
					  ClusterCtrcReceipt *receipt, ClusterCtrcLocalReleaseAckV1 *ack,
					  uint64 *participant_index, uint64 *receipt_index)
{
	ClusterCtrcOriginEntry *origin = seed_origin(origin_index, 1);
	ClusterCtrcPublicationIdV1 publication = { 0 };
	ClusterCtrcTargetV1 target = { 0 };
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcParticipantEntry *participant;
	ClusterCtrcDurability durability = { 0 };

	publication.requester_node_id = 0;
	publication.requester_boot_incarnation = origin->touched[0].boot_incarnation;
	publication.capability_record_generation = origin->touched[0].capability_record_generation;
	publication.requester_backend_id = 11;
	publication.wire_request_id = 101;
	publication.operation_id = 81;
	publication.attempt_generation = 1;
	publication.member_ordinal = UINT16_MAX;
	publication.reference_kind = CTRC_REF_HEAP_ITL_UBA;
	publication.target_kind = CTRC_TARGET_PAGE_PENDING_ITL_SLOT;
	publication.grant_generation = origin->grant_generation;
	target.kind = CTRC_TARGET_PAGE_PENDING_ITL_SLOT;
	target.spc_oid = 1663;
	target.db_oid = 5;
	target.rel_number = 9001;
	target.block_number = 44;
	target.predecessor_page_lsn_origin_node_id = CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID;
	target.publication_own_generation = 17;
	target.publication_acquisition_epoch = 19;
	target.relation_persistence = 'p';
	target.needs_wal = true;
	target.page_operation_kind = 1;
	if (cluster_ctrc_receipt_prepare_shared(&origin->key, &origin->touched[0],
											origin->grant_generation, &publication, &target,
											&handle)
			!= CLUSTER_CTRC_PREPARE_READY
		|| !handle.valid || !cluster_ctrc_receipt_cancel_shared(&handle)
		|| !ctrc_participant_index(&origin->key, 0, participant_index))
		return false;
	*receipt_index = handle.receipt_index;
	participant = &ctrc_participant_entries()[*participant_index];
	if (cluster_ctrc_participant_close(participant, &origin->touched[0], origin->grant_generation,
									   origin->seal_generation)
		!= CLUSTER_CTRC_CLOSE_ACK_READY)
		return false;
	*snapshot = *participant;
	*receipt = ctrc_receipt_entries()[*receipt_index];
	return cluster_ctrc_participant_ack_from_snapshot(snapshot, receipt, 1, &durability, ack)
		   == CLUSTER_CTRC_ACK_RELEASED;
}

static bool
seed_cancelled_ack(ClusterCtrcParticipantEntry *snapshot, ClusterCtrcReceipt *receipt,
				   ClusterCtrcLocalReleaseAckV1 *ack, uint64 *participant_index,
				   uint64 *receipt_index)
{
	return seed_cancelled_ack_at(0, snapshot, receipt, ack, participant_index, receipt_index);
}

UT_TEST(test_shared_prepare_journal_uses_sleepable_exclusion)
{
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcReceipt receipt;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint64 participant_index;
	uint64 receipt_index;

	reset_fixture();
	UT_ASSERT(seed_cancelled_ack(&snapshot, &receipt, &ack, &participant_index, &receipt_index));
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(receipt.state, CTRC_RECEIPT_CANCELLED);
	UT_ASSERT_EQ(snapshot.receipt_count, 1);
	UT_ASSERT_EQ(spin_acquisitions, 0);
	UT_ASSERT_EQ(sleepable_acquisitions, 3);
}

UT_TEST(test_nonempty_ack_copy_and_freeze_use_sleepable_exclusion)
{
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcReceipt receipt;
	ClusterCtrcReceipt copied;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint64 participant_index;
	uint64 receipt_index;

	reset_fixture();
	UT_ASSERT(seed_cancelled_ack(&snapshot, &receipt, &ack, &participant_index, &receipt_index));
	spin_acquisitions = sleepable_acquisitions = 0;
	UT_ASSERT(ctrc_participant_copy_receipts(participant_index, &snapshot, &copied, 1));
	UT_ASSERT_EQ(memcmp(&copied, &receipt, sizeof(receipt)), 0);
	UT_ASSERT(ctrc_participant_freeze_ack_exact(participant_index, &snapshot, &copied, 1, &ack));
	UT_ASSERT_EQ(ctrc_receipt_entries()[receipt_index].state, CTRC_RECEIPT_ACK_FROZEN);
	UT_ASSERT_EQ(ctrc_participant_entries()[participant_index].state, CTRC_PARTICIPANT_ACK_FROZEN);
	UT_ASSERT_EQ(memcmp(&ctrc_participant_ack_entries()[participant_index], &ack, sizeof(ack)), 0);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(spin_acquisitions, 0);
	UT_ASSERT_EQ(sleepable_acquisitions, 4);
}

UT_TEST(test_stale_ack_snapshot_releases_without_publication)
{
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcParticipantEntry before;
	ClusterCtrcReceipt receipt;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint64 participant_index;
	uint64 receipt_index;

	reset_fixture();
	UT_ASSERT(seed_cancelled_ack(&snapshot, &receipt, &ack, &participant_index, &receipt_index));
	before = snapshot;
	snapshot.grant_generation++;
	UT_ASSERT(!ctrc_participant_copy_receipts(participant_index, &snapshot, &receipt, 1));
	UT_ASSERT(!ctrc_participant_freeze_ack_exact(participant_index, &snapshot, &receipt, 1, &ack));
	UT_ASSERT_EQ(memcmp(&ctrc_participant_entries()[participant_index], &before, sizeof(before)),
				 0);
	UT_ASSERT_EQ(ctrc_receipt_entries()[receipt_index].state, CTRC_RECEIPT_CANCELLED);
	UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_ack_entries()[participant_index], sizeof(ack)));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_receipt_drift_retains_bytes_and_blocks_ack)
{
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcReceipt receipt;
	ClusterCtrcReceipt changed;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint64 participant_index;
	uint64 receipt_index;

	reset_fixture();
	UT_ASSERT(seed_cancelled_ack(&snapshot, &receipt, &ack, &participant_index, &receipt_index));
	ctrc_receipt_entries()[receipt_index].target.predecessor_page_scn++;
	changed = ctrc_receipt_entries()[receipt_index];
	UT_ASSERT(!ctrc_participant_freeze_ack_exact(participant_index, &snapshot, &receipt, 1, &ack));
	UT_ASSERT_EQ(ctrc_participant_entries()[participant_index].state, CTRC_PARTICIPANT_BLOCKED);
	UT_ASSERT_EQ(memcmp(&ctrc_receipt_entries()[receipt_index], &changed, sizeof(changed)), 0);
	UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_ack_entries()[participant_index], sizeof(ack)));
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_sleepable_header_requires_exact_layout_and_tranches)
{
	ClusterCtrcCapacity capacity;

	reset_fixture();
	UT_ASSERT(cluster_ctrc_capacity_compute(NBuffers, MaxBackends, 4, &capacity));
	UT_ASSERT(cluster_ctrc_shmem_ready());
	UT_ASSERT_EQ(CtrcShared->total_bytes, capacity.total_bytes);
	UT_ASSERT_EQ(CtrcShared->origin_offset, MAXALIGN(sizeof(*CtrcShared)));
	UT_ASSERT_EQ(CtrcShared->origin_lock.tranche, LWTRANCHE_CLUSTER_CTRC_ORIGIN);
	UT_ASSERT_EQ(CtrcShared->participant_lock.tranche, LWTRANCHE_CLUSTER_CTRC_PARTICIPANT);
	UT_ASSERT_EQ(CtrcShared->receipt_lock.tranche, LWTRANCHE_CLUSTER_CTRC_RECEIPT);
	CtrcShared->version--;
	UT_ASSERT(!cluster_ctrc_shmem_ready());
	CtrcShared->version++;
	CtrcShared->total_bytes--;
	UT_ASSERT(!cluster_ctrc_shmem_ready());
	CtrcShared->total_bytes++;
	UT_ASSERT(cluster_ctrc_shmem_ready());
}

static void
seed_reclaim_batch(ClusterCtrcReclaimWork *work, ClusterCtrcReceipt *rows, uint8 *states,
				   Size capacity, Size count)
{
	MemSet(work, 0, count * sizeof(*work));
	MemSet(rows, 0, capacity * sizeof(*rows));
	MemSet(states, 0, capacity);
	for (Size i = 0; i < count; i++) {
		work[i].key = seed_origin(i, 0)->key;
		work[i].expected_count = 2;
		for (Size j = 0; j < 2; j++) {
			Size slot = 2 * i + j;
			rows[slot].key = work[i].key;
			rows[slot].state = CTRC_RECEIPT_ACK_FROZEN;
			states[slot] = CTRC_RECEIPT_PROBE_OCCUPIED;
		}
	}
}

UT_TEST(test_reclaim_batch_validates_table_once_for_eight_keys)
{
	ClusterCtrcReclaimWork work[8];
	ClusterCtrcReceipt rows[32];
	uint8 states[32];

	reset_fixture();
	seed_reclaim_batch(work, rows, states, lengthof(rows), lengthof(work));
	MemSet(table_visits, 0, sizeof(table_visits));
	UT_ASSERT(cluster_ctrc_receipts_reclaim_batch_locked(work, lengthof(work), rows, states,
														 lengthof(rows)));
	for (Size i = 0; i < lengthof(work); i++) {
		UT_ASSERT(work[i].reclaimed);
		UT_ASSERT_EQ(work[i].found, 2);
	}
	UT_ASSERT(ctrc_bytes_zero(rows, sizeof(rows)));
	UT_ASSERT(ctrc_bytes_zero(states, sizeof(states)));
	UT_ASSERT_EQ(table_visits[0], lengthof(rows));
}

UT_TEST(test_reclaim_batch_bad_key_retains_its_whole_set_only)
{
	ClusterCtrcReclaimWork work[3];
	ClusterCtrcReceipt rows[16];
	ClusterCtrcReceipt retained[2];
	uint8 states[16];

	reset_fixture();
	seed_reclaim_batch(work, rows, states, lengthof(rows), lengthof(work));
	rows[3].state = CTRC_RECEIPT_APPLIED;
	memcpy(retained, &rows[2], sizeof(retained));
	UT_ASSERT(cluster_ctrc_receipts_reclaim_batch_locked(work, lengthof(work), rows, states,
														 lengthof(rows)));
	UT_ASSERT(work[0].reclaimed && work[2].reclaimed);
	UT_ASSERT(!work[1].reclaimed);
	UT_ASSERT_EQ(memcmp(retained, &rows[2], sizeof(retained)), 0);
	UT_ASSERT_EQ(states[2], CTRC_RECEIPT_PROBE_OCCUPIED);
	UT_ASSERT_EQ(states[3], CTRC_RECEIPT_PROBE_OCCUPIED);
}

UT_TEST(test_reclaim_batch_all_empty_bytes_guard_before_any_delete)
{
	ClusterCtrcReclaimWork work[2];
	ClusterCtrcReceipt rows[8];
	ClusterCtrcReceipt before[8];
	uint8 states[8];
	uint8 states_before[8];

	reset_fixture();
	seed_reclaim_batch(work, rows, states, lengthof(rows), lengthof(work));
	for (Size byte = 0; byte < sizeof(rows[7]); byte++) {
		((uint8 *)&rows[7])[byte] = 0x80;
		memcpy(before, rows, sizeof(rows));
		memcpy(states_before, states, sizeof(states));
		(void)cluster_ctrc_receipts_reclaim_batch_locked(work, lengthof(work), rows, states,
														 lengthof(rows));
		UT_ASSERT(!work[0].reclaimed && !work[1].reclaimed);
		UT_ASSERT_EQ(memcmp(before, rows, sizeof(rows)), 0);
		UT_ASSERT_EQ(memcmp(states_before, states, sizeof(states)), 0);
		((uint8 *)&rows[7])[byte] = 0;
	}
}

UT_TEST(test_reclaim_batch_duplicate_and_missing_member_retain_exactly)
{
	ClusterCtrcReclaimWork work[2];
	ClusterCtrcReceipt rows[8];
	ClusterCtrcReceipt before[8];
	uint8 states[8];

	reset_fixture();
	seed_reclaim_batch(work, rows, states, lengthof(rows), lengthof(work));
	memcpy(before, rows, sizeof(rows));
	work[1].key = work[0].key;
	UT_ASSERT(!cluster_ctrc_receipts_reclaim_batch_locked(work, lengthof(work), rows, states,
														  lengthof(rows)));
	UT_ASSERT_EQ(memcmp(before, rows, sizeof(rows)), 0);
	work[1].key = rows[2].key;
	work[0].expected_count++;
	UT_ASSERT(cluster_ctrc_receipts_reclaim_batch_locked(work, lengthof(work), rows, states,
														 lengthof(rows)));
	UT_ASSERT(!work[0].reclaimed && work[1].reclaimed);
	UT_ASSERT_EQ(memcmp(before, rows, 2 * sizeof(rows[0])), 0);
}

UT_TEST(test_zero_bytes_exact_at_every_offset_and_protected_end)
{
	uint8 bytes[608];
	long page_size = sysconf(_SC_PAGESIZE);
	uint8 *guard;

	UT_ASSERT(!ctrc_bytes_zero(NULL, 0));
	UT_ASSERT(!ctrc_bytes_zero(NULL, 1));
	for (Size offset = 0; offset < 8; offset++)
		for (Size length = 0; length <= 592; length++) {
			MemSet(bytes, 0, sizeof(bytes));
			bytes[offset + length] = 0x80;
			UT_ASSERT(ctrc_bytes_zero(bytes + offset, length));
			for (Size changed = 0; changed < length; changed++) {
				bytes[offset + changed] = 1;
				UT_ASSERT(!ctrc_bytes_zero(bytes + offset, length));
				bytes[offset + changed] = 0;
			}
		}
	UT_ASSERT(page_size > 592);
	guard = mmap(NULL, 2 * page_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
	UT_ASSERT(guard != MAP_FAILED);
	UT_ASSERT_EQ(mprotect(guard + page_size, page_size, PROT_NONE), 0);
	for (Size length = 0; length <= 592; length++)
		UT_ASSERT(ctrc_bytes_zero(guard + page_size - length, length));
	UT_ASSERT_EQ(munmap(guard, 2 * page_size), 0);
}

UT_TEST(test_reclaim_empty_range_preserves_failed_and_collapses_success)
{
	ClusterCtrcReclaimWork work = { 0 };
	ClusterCtrcReceipt rows[8] = { { 0 } };
	uint8 states[8] = { 0 };
	uint8 before[8];

	reset_fixture();
	work.key = seed_origin(0, 0)->key;
	work.expected_count = 1;
	states[6] = CTRC_RECEIPT_PROBE_TOMBSTONE;
	memcpy(before, states, sizeof(states));
	UT_ASSERT(cluster_ctrc_receipts_reclaim_batch_locked(&work, 1, rows, states, lengthof(rows)));
	UT_ASSERT(!work.reclaimed);
	UT_ASSERT_EQ(memcmp(before, states, sizeof(states)), 0);
	work.expected_count = 0;
	UT_ASSERT(cluster_ctrc_receipts_reclaim_batch_locked(&work, 1, rows, states, lengthof(rows)));
	UT_ASSERT(work.reclaimed);
	UT_ASSERT(ctrc_bytes_zero(states, sizeof(states)));
}

static bool
seed_frozen_certificate(unsigned index, ClusterCtrcCloseDispatch *dispatch,
						uint64 *participant_index, uint64 *receipt_index)
{
	ClusterCtrcParticipantEntry snapshot;
	ClusterCtrcReceipt receipt;
	ClusterCtrcLocalReleaseAckV1 ack;

	if (!seed_cancelled_ack_at(index, &snapshot, &receipt, &ack, participant_index, receipt_index)
		|| !ctrc_participant_freeze_ack_exact(*participant_index, &snapshot, &receipt, 1, &ack))
		return false;
	MemSet(dispatch, 0, sizeof(*dispatch));
	dispatch->key = snapshot.key;
	dispatch->participant = snapshot.identity;
	dispatch->grant_generation = snapshot.grant_generation;
	dispatch->seal_generation = snapshot.seal_generation;
	dispatch->suboperation = CTRC_SEAL_CERTIFICATE_COMMITTED;
	dispatch->request_id = 1234 + index;
	return true;
}

UT_TEST(test_shared_certificate_batch_uses_one_real_validation)
{
	ClusterCtrcCloseDispatch dispatches[8];
	ClusterCtrcSealReplyResult results[8];
	uint64 participant_indices[8];
	uint64 receipt_indices[8];
	uint64 progress;

	reset_fixture();
	for (Size i = 0; i < lengthof(dispatches); i++)
		UT_ASSERT(seed_frozen_certificate(i, &dispatches[i], &participant_indices[i],
										  &receipt_indices[i]));
	MemSet(table_visits, 0, sizeof(table_visits));
	sleepable_acquisitions = 0;
	progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(dispatches, lengthof(dispatches),
																results));
	for (Size i = 0; i < lengthof(dispatches); i++) {
		UT_ASSERT_EQ(results[i], CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
		UT_ASSERT(ctrc_bytes_zero(&ctrc_participant_entries()[participant_indices[i]],
								  sizeof(ClusterCtrcParticipantEntry)));
		UT_ASSERT(ctrc_bytes_zero(&ctrc_receipt_entries()[receipt_indices[i]],
								  sizeof(ClusterCtrcReceipt)));
	}
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS) - progress, 8);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(table_visits[0], CtrcShared->receipt_entries);
	UT_ASSERT_EQ(sleepable_acquisitions, 2);
}

UT_TEST(test_shared_certificate_batch_identity_and_receipt_faults_are_isolated)
{
	for (int fault = 0; fault < 6; fault++) {
		ClusterCtrcCloseDispatch dispatches[2];
		ClusterCtrcSealReplyResult results[2];
		uint64 participant_indices[2];
		uint64 receipt_indices[2];
		ClusterCtrcReceipt retained;
		ClusterCtrcLocalReleaseAckV1 ack_retained;

		reset_fixture();
		for (Size i = 0; i < 2; i++)
			UT_ASSERT(seed_frozen_certificate(i, &dispatches[i], &participant_indices[i],
											  &receipt_indices[i]));
		if (fault == 0)
			dispatches[0].participant.boot_incarnation++;
		else if (fault == 1)
			dispatches[0].grant_generation++;
		else if (fault == 2)
			dispatches[0].seal_generation++;
		else if (fault == 3)
			dispatches[0].key.xid++;
		else if (fault == 4)
			ctrc_receipt_entries()[receipt_indices[0]].state = CTRC_RECEIPT_APPLIED;
		else
			ctrc_participant_entries()[participant_indices[0]].receipt_count++;
		retained = ctrc_receipt_entries()[receipt_indices[0]];
		ack_retained = ctrc_participant_ack_entries()[participant_indices[0]];
		UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(dispatches, 2, results));
		UT_ASSERT_EQ(results[0], CTRC_SEAL_REPLY_BLOCKED_RETAIN);
		UT_ASSERT_EQ(results[1], CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED);
		UT_ASSERT_EQ(
			memcmp(&retained, &ctrc_receipt_entries()[receipt_indices[0]], sizeof(retained)), 0);
		UT_ASSERT_EQ(memcmp(&ack_retained, &ctrc_participant_ack_entries()[participant_indices[0]],
							sizeof(ack_retained)),
					 0);
		UT_ASSERT_EQ(ctrc_participant_entries()[participant_indices[0]].state,
					 fault < 4 ? CTRC_PARTICIPANT_ACK_FROZEN : CTRC_PARTICIPANT_BLOCKED);
		UT_ASSERT(ctrc_bytes_zero(&ctrc_receipt_entries()[receipt_indices[1]], sizeof(retained)));
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_shared_certificate_global_corruption_and_alias_never_partially_clear)
{
	ClusterCtrcCloseDispatch dispatches[2];
	ClusterCtrcSealReplyResult results[2];
	uint64 participant_indices[2];
	uint64 receipt_indices[2];
	ClusterCtrcReceipt retained[2];
	Size empty = 0;
	uint64 progress;

	reset_fixture();
	for (Size i = 0; i < 2; i++) {
		UT_ASSERT(seed_frozen_certificate(i, &dispatches[i], &participant_indices[i],
										  &receipt_indices[i]));
		retained[i] = ctrc_receipt_entries()[receipt_indices[i]];
	}
	progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	dispatches[1] = dispatches[0];
	UT_ASSERT(!cluster_ctrc_participant_certificate_batch_shared(dispatches, 2, results));
	UT_ASSERT_EQ(ctrc_participant_entries()[participant_indices[0]].state,
				 CTRC_PARTICIPANT_ACK_FROZEN);
	dispatches[1].key = ctrc_participant_entries()[participant_indices[1]].key;
	dispatches[1].seal_generation
		= ctrc_participant_entries()[participant_indices[1]].seal_generation;
	while (ctrc_receipt_probe_states()[empty] != CTRC_RECEIPT_PROBE_EMPTY)
		empty++;
	ctrc_receipt_entries()[empty].reserved8[2] = 1;
	UT_ASSERT(cluster_ctrc_participant_certificate_batch_shared(dispatches, 2, results));
	for (Size i = 0; i < 2; i++) {
		UT_ASSERT_EQ(results[i], CTRC_SEAL_REPLY_BLOCKED_RETAIN);
		UT_ASSERT_EQ(
			memcmp(&retained[i], &ctrc_receipt_entries()[receipt_indices[i]], sizeof(retained[i])),
			0);
		UT_ASSERT_EQ(ctrc_participant_entries()[participant_indices[i]].state,
					 CTRC_PARTICIPANT_BLOCKED);
	}
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), progress);
	UT_ASSERT_EQ(held_count, 0);
}

UT_TEST(test_ack_batch_keeps_four_independent_scans_not_four_per_key)
{
	CtrcAckWork work[8] = { { 0 } };
	ClusterCtrcReceipt copies[8];
	ClusterCtrcReceipt originals[8];
	ClusterCtrcLocalReleaseAckV1 initial_ack;
	ClusterCtrcDurability durability = { 0 };
	uint64 receipt_indices[8];

	reset_fixture();
	for (Size i = 0; i < 8; i++) {
		UT_ASSERT(seed_cancelled_ack_at(i, &work[i].participant, &originals[i], &initial_ack,
										&work[i].participant_index, &receipt_indices[i]));
		work[i].eligible = true;
		work[i].receipts = &copies[i];
		work[i].sorted_receipts = &copies[i];
		work[i].receipt_count = 1;
	}
	MemSet(table_visits, 0, sizeof(table_visits));
	ctrc_participant_copy_ack_batch(work, 8);
	for (Size i = 0; i < 8; i++) {
		UT_ASSERT(work[i].eligible);
		UT_ASSERT_EQ(memcmp(work[i].receipts, &originals[i], sizeof(originals[i])), 0);
		UT_ASSERT_EQ(cluster_ctrc_participant_ack_from_snapshot(&work[i].participant,
																work[i].receipts, 1, &durability,
																&work[i].first_ack),
					 CLUSTER_CTRC_ACK_RELEASED);
	}
	ctrc_participant_copy_ack_batch(work, 8);
	for (Size i = 0; i < 8; i++) {
		UT_ASSERT(work[i].eligible);
		UT_ASSERT_EQ(cluster_ctrc_participant_ack_from_snapshot(&work[i].participant,
																work[i].receipts, 1, &durability,
																&work[i].second_ack),
					 CLUSTER_CTRC_ACK_RELEASED);
		UT_ASSERT_EQ(memcmp(&work[i].first_ack, &work[i].second_ack, sizeof(initial_ack)), 0);
	}
	ctrc_participant_freeze_ack_batch_exact(work, 8);
	for (Size i = 0; i < 8; i++) {
		UT_ASSERT(work[i].frozen);
		UT_ASSERT_EQ(ctrc_receipt_entries()[receipt_indices[i]].state, CTRC_RECEIPT_ACK_FROZEN);
		UT_ASSERT_EQ(memcmp(&ctrc_participant_ack_entries()[work[i].participant_index],
							&work[i].second_ack, sizeof(initial_ack)),
					 0);
	}
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(table_visits[1], 2 * CtrcShared->receipt_entries);
	UT_ASSERT_EQ(table_visits[2], CtrcShared->receipt_entries);
	UT_ASSERT_EQ(table_visits[3], CtrcShared->receipt_entries);
}

UT_TEST(test_real_ack_driver_shares_scans_and_keeps_durability_and_barrier)
{
	ClusterCtrcParticipantEntry participants[8];
	ClusterCtrcReceipt receipts[8];
	ClusterCtrcLocalReleaseAckV1 acks[8];
	uint64 participant_indices[8], receipt_indices[8];

	reset_fixture();
	for (Size i = 0; i < 8; i++)
		UT_ASSERT(seed_cancelled_ack_at(i, &participants[i], &receipts[i], &acks[i],
										&participant_indices[i], &receipt_indices[i]));
	CtrcBatch.active = true;
	CtrcBatch.remaining[CTRC_SCAN_ACK] = CtrcShared->participant_key_entries;
	MemSet(table_visits, 0, sizeof(table_visits));
	UT_ASSERT(cluster_ctrc_test_barrier_control(CTRC_TEST_BARRIER_ACK_DURABLE, true));
	wake_count = 0;
	UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(8), 8);
	CtrcBatch.active = false;
	for (Size i = 0; i < 8; i++) {
		UT_ASSERT_EQ(ctrc_participant_entries()[participant_indices[i]].state,
					 CTRC_PARTICIPANT_ACK_FROZEN);
		UT_ASSERT_EQ(memcmp(&ctrc_participant_ack_entries()[participant_indices[i]], &acks[i],
							sizeof(acks[i])),
					 0);
	}
	UT_ASSERT_EQ(durability_calls, 16);
	UT_ASSERT_EQ(wake_count, 8);
	UT_ASSERT_EQ(barrier_calls, 1);
	UT_ASSERT_EQ(allocated, 0);
	UT_ASSERT_EQ(table_visits[1], 2 * CtrcShared->receipt_entries);
	UT_ASSERT_EQ(table_visits[2], CtrcShared->receipt_entries);
	UT_ASSERT_EQ(table_visits[3], CtrcShared->receipt_entries);
}

static uint64 drift_receipt_index;
static unsigned drift_at_capture;

static void
drift_during_capture(unsigned call)
{
	if (call == drift_at_capture)
		ctrc_receipt_entries()[drift_receipt_index].target.block_number++;
}

static void
drift_during_barrier(void)
{
	ctrc_receipt_entries()[drift_receipt_index].target.block_number++;
}

static void
throw_during_capture(unsigned call)
{
	if (call == 2)
		pg_re_throw();
}

static void
throw_during_barrier(void)
{
	pg_re_throw();
}

UT_TEST(test_real_ack_driver_independent_capture_and_final_drift_retain_one_key)
{
	for (int phase = 0; phase < 3; phase++) {
		ClusterCtrcParticipantEntry snapshots[2];
		ClusterCtrcReceipt receipts[2];
		ClusterCtrcLocalReleaseAckV1 acks[2];
		uint64 participants[2], indices[2];

		reset_fixture();
		for (Size i = 0; i < 2; i++)
			UT_ASSERT(seed_cancelled_ack_at(i, &snapshots[i], &receipts[i], &acks[i],
											&participants[i], &indices[i]));
		drift_receipt_index = indices[0];
		drift_at_capture = phase == 0 ? 2 : 4;
		if (phase < 2)
			durability_hook = drift_during_capture;
		else {
			barrier_hook = drift_during_barrier;
			UT_ASSERT(cluster_ctrc_test_barrier_control(CTRC_TEST_BARRIER_ACK_DURABLE, true));
		}
		CtrcBatch.active = true;
		CtrcBatch.remaining[CTRC_SCAN_ACK] = CtrcShared->participant_key_entries;
		UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(2), 1);
		CtrcBatch.active = false;
		UT_ASSERT_EQ(ctrc_participant_entries()[participants[0]].state,
					 phase == 0 ? CTRC_PARTICIPANT_ACK_READY : CTRC_PARTICIPANT_BLOCKED);
		UT_ASSERT_EQ(ctrc_receipt_entries()[indices[0]].state, CTRC_RECEIPT_CANCELLED);
		UT_ASSERT(
			ctrc_bytes_zero(&ctrc_participant_ack_entries()[participants[0]], sizeof(acks[0])));
		UT_ASSERT_EQ(ctrc_participant_entries()[participants[1]].state,
					 CTRC_PARTICIPANT_ACK_FROZEN);
		UT_ASSERT_EQ(ctrc_receipt_entries()[indices[1]].state, CTRC_RECEIPT_ACK_FROZEN);
		UT_ASSERT_EQ(allocated, 0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

UT_TEST(test_real_ack_driver_exception_and_allocation_failure_never_publish)
{
	for (int phase = 0; phase < 3; phase++) {
		ClusterCtrcParticipantEntry snapshots[2];
		ClusterCtrcReceipt receipts[2];
		ClusterCtrcLocalReleaseAckV1 acks[2];
		uint64 participants[2], indices[2];
		volatile bool caught = false;

		reset_fixture();
		for (Size i = 0; i < 2; i++)
			UT_ASSERT(seed_cancelled_ack_at(i, &snapshots[i], &receipts[i], &acks[i],
											&participants[i], &indices[i]));
		if (phase == 0)
			durability_hook = throw_during_capture;
		else if (phase == 1) {
			barrier_hook = throw_during_barrier;
			UT_ASSERT(cluster_ctrc_test_barrier_control(CTRC_TEST_BARRIER_ACK_DURABLE, true));
		} else
			allocation_failure = true;
		CtrcBatch.active = true;
		CtrcBatch.remaining[CTRC_SCAN_ACK] = CtrcShared->participant_key_entries;
		PG_TRY();
		{
			UT_ASSERT_EQ(ctrc_participant_freeze_ack_batch_shared(2), 0);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		CtrcBatch.active = false;
		UT_ASSERT_EQ(caught, phase != 2);
		UT_ASSERT_EQ(allocated, 0);
		UT_ASSERT_EQ(held_count, 0);
		for (Size i = 0; i < 2; i++) {
			UT_ASSERT_EQ(memcmp(&ctrc_participant_entries()[participants[i]], &snapshots[i],
								sizeof(snapshots[i])),
						 0);
			UT_ASSERT_EQ(
				memcmp(&ctrc_receipt_entries()[indices[i]], &receipts[i], sizeof(receipts[i])), 0);
			UT_ASSERT(
				ctrc_bytes_zero(&ctrc_participant_ack_entries()[participants[i]], sizeof(acks[i])));
		}
	}
}

UT_TEST(test_ack_batch_final_count_summary_and_identity_faults_are_isolated)
{
	for (int fault = 0; fault < 7; fault++) {
		CtrcAckWork work[2] = { { 0 } };
		ClusterCtrcReceipt copies[2], originals[2];
		ClusterCtrcLocalReleaseAckV1 ack;
		uint64 indices[2];
		Size unused = 0;

		reset_fixture();
		for (Size i = 0; i < 2; i++) {
			UT_ASSERT(seed_cancelled_ack_at(i, &work[i].participant, &originals[i], &ack,
											&work[i].participant_index, &indices[i]));
			work[i].receipts = &copies[i];
			work[i].sorted_receipts = &copies[i];
			work[i].receipt_count = 1;
			work[i].eligible = true;
			work[i].second_ack = ack;
		}
		ctrc_participant_copy_ack_batch(work, 2);
		if (fault == 0)
			ctrc_receipt_entries()[indices[0]].state = CTRC_RECEIPT_APPLIED;
		if (fault == 1)
			ctrc_participant_entries()[work[0].participant_index].seal_generation++;
		if (fault == 2)
			ctrc_participant_ack_entries()[work[0].participant_index].crc32c = 1;
		if (fault == 3)
			ctrc_participant_entries()[work[0].participant_index].receipt_count++;
		if (fault == 4)
			MemSet(&ctrc_receipt_entries()[indices[0]], 0, sizeof(ClusterCtrcReceipt));
		if (fault == 5) {
			while (unused == indices[0] || unused == indices[1])
				unused++;
			ctrc_receipt_entries()[unused] = originals[0];
		}
		if (fault == 6)
			ctrc_receipt_entries()[indices[0]].publication.key_sequence = 2;
		originals[0] = ctrc_receipt_entries()[indices[0]];
		ack = ctrc_participant_ack_entries()[work[0].participant_index];
		ctrc_participant_freeze_ack_batch_exact(work, 2);
		UT_ASSERT(!work[0].frozen);
		UT_ASSERT(work[1].frozen);
		UT_ASSERT_EQ(
			memcmp(&ctrc_receipt_entries()[indices[0]], &originals[0], sizeof(originals[0])), 0);
		UT_ASSERT_EQ(
			memcmp(&ctrc_participant_ack_entries()[work[0].participant_index], &ack, sizeof(ack)),
			0);
		UT_ASSERT_EQ(held_count, 0);
	}
}

int
main(void)
{
	ClusterCtrcCapacity capacity;

	if (!cluster_ctrc_capacity_compute(NBuffers, MaxBackends, 4, &capacity))
		abort();
	UT_PLAN(31);
	printf("# CTRC header_bytes=%zu total_bytes=%zu\n", sizeof(ClusterCtrcSharedHeader),
		   capacity.total_bytes);
	UT_RUN(test_new_seals_do_not_starve_old_pending);
	UT_RUN(test_pending_participant_does_not_starve_peer);
	UT_RUN(test_duplicate_pending_is_accepted_without_wakeup);
	UT_RUN(test_uncorrelated_reply_cannot_progress);
	UT_RUN(test_batch_is_bounded_oldest_first_and_fair);
	UT_RUN(test_batch_hint_rechecks_current_identity);
	UT_RUN(test_scan_budget_prevents_same_pending_item_revisit);
	UT_RUN(test_observed_age_resets_without_releasing_authority);
	UT_RUN(test_participant_duplicate_pending_does_not_wake);
	UT_RUN(test_real_ack_and_certificate_consumers_are_idempotent);
	UT_RUN(test_receipt_and_ack_scans_do_not_retry_retained_entries);
	UT_RUN(test_certificate_scan_bounds_unpublished_candidates);
	UT_RUN(test_shared_prepare_journal_uses_sleepable_exclusion);
	UT_RUN(test_nonempty_ack_copy_and_freeze_use_sleepable_exclusion);
	UT_RUN(test_stale_ack_snapshot_releases_without_publication);
	UT_RUN(test_receipt_drift_retains_bytes_and_blocks_ack);
	UT_RUN(test_sleepable_header_requires_exact_layout_and_tranches);
	UT_RUN(test_reclaim_batch_validates_table_once_for_eight_keys);
	UT_RUN(test_reclaim_batch_bad_key_retains_its_whole_set_only);
	UT_RUN(test_reclaim_batch_all_empty_bytes_guard_before_any_delete);
	UT_RUN(test_reclaim_batch_duplicate_and_missing_member_retain_exactly);
	UT_RUN(test_zero_bytes_exact_at_every_offset_and_protected_end);
	UT_RUN(test_reclaim_empty_range_preserves_failed_and_collapses_success);
	UT_RUN(test_shared_certificate_batch_uses_one_real_validation);
	UT_RUN(test_shared_certificate_batch_identity_and_receipt_faults_are_isolated);
	UT_RUN(test_shared_certificate_global_corruption_and_alias_never_partially_clear);
	UT_RUN(test_ack_batch_keeps_four_independent_scans_not_four_per_key);
	UT_RUN(test_real_ack_driver_shares_scans_and_keeps_durability_and_barrier);
	UT_RUN(test_real_ack_driver_independent_capture_and_final_drift_retain_one_key);
	UT_RUN(test_real_ack_driver_exception_and_allocation_failure_never_publish);
	UT_RUN(test_ack_batch_final_count_summary_and_identity_faults_are_isolated);
	free(CtrcShared);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
