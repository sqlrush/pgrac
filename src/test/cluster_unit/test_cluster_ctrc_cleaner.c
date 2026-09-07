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

static int test_clock_gettime(clockid_t clock_id, struct timespec *ts);
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
	free(CtrcShared);
	CtrcShared = NULL;
	cluster_ctrc_shmem_init();
	wake_count = 0;
	test_now_us = UINT64_C(1000000);
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
	key.cluster_epoch = 13;
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
		identity.capability_record_generation = 19;
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

int
main(void)
{
	UT_PLAN(12);
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
	free(CtrcShared);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
