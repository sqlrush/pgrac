/*-------------------------------------------------------------------------
 *
 * test_cluster_ctrc_dispatch.c
 *    Actual GCS dispatcher and shared CTRC certificate regression tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_ctrc_dispatch.c
 *
 * NOTES
 *    Only R4/membership/root/transport/process boundaries are fixture doubles.
 *    No database, durable publication, or interconnect performance claim.
 *-------------------------------------------------------------------------
 */
int ctrc_cleaner_original_main(void);
#define main ctrc_cleaner_original_main
#include "test_cluster_ctrc_cleaner.c"
#undef main

#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_write_fence.h"

int cluster_lms_workers = 1;
static unsigned enters, leaves, enqueues, rechecks;
static bool data_plane = true, root_exact = true, fence_ok = true;
static unsigned fail_recheck;
static unsigned throw_resolve;
static unsigned resolves;
static bool observe_enqueue_order, enqueue_order_exact;

bool
cluster_gcs_block_family_on_data_plane(void)
{
	return data_plane;
}
bool
cluster_write_fence_enforcing(void)
{
	return true;
}
bool
cluster_write_fence_allowed(void)
{
	return fence_ok;
}
bool
cluster_membership_is_member(int node)
{
	return node >= 0 && node < 4;
}
uint64
cluster_epoch_get_current(void)
{
	return 23;
}
uint64
GetSystemIdentifier(void)
{
	return 42;
}
uint64
cluster_qvotec_get_self_incarnation(void)
{
	return 1101;
}
uint64
cluster_membership_get_last_admitted_incarnation(int node)
{
	return 1101 + node;
}
uint32
cluster_ic_local_capability_word(void)
{
	return PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1 | PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1;
}
uint64
cluster_reconfig_get_observed_epoch(int node pg_attribute_unused())
{
	return 23;
}
bool
cluster_reconfig_get_observed_slot(int node, uint64 *incarnation, uint64 *generation)
{
	*incarnation = 1101 + node;
	*generation = 1;
	return true;
}
bool
cluster_sf_peer_capability_generation_matches(int node pg_attribute_unused(),
											  uint32 required pg_attribute_unused(),
											  uint32 generation)
{
	return generation == 29;
}
ClusterSemanticAdmissionResult
cluster_semantic_activation_enter_r4_terminal_census(ClusterSemanticAdmissionToken *token)
{
	MemSet(token, 0, sizeof(*token));
	token->record_generation = 29;
	token->formation_epoch = 23;
	token->entered = true;
	enters++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}
bool
cluster_semantic_activation_recheck_r4_terminal_census(const ClusterSemanticAdmissionToken *token)
{
	if (!token->entered || held_count != 0)
		abort();
	rechecks++;
	return fail_recheck == 0 || rechecks != fail_recheck;
}
void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	if (!token->entered || held_count != 0)
		abort();
	token->entered = false;
	leaves++;
}
bool
cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token, ClusterUndoPathIntent intent,
	uint32 owner pg_attribute_unused(), uint32 segment pg_attribute_unused(),
	ClusterUndoBlock0ResolvedRoot *root)
{
	if (!token->entered || held_count != 0)
		abort();
	resolves++;
	if (throw_resolve != 0 && resolves == throw_resolve)
		pg_re_throw();
	MemSet(root, 0, sizeof(*root));
	root->intent = intent;
	root->root_id = root_exact ? 37 : 38;
	root->root_generation = 43;
	return true;
}
int
cluster_lms_shard_for_tag(const BufferTag *tag pg_attribute_unused(), int workers)
{
	return workers == 1 ? 0 : -1;
}
bool
cluster_lms_outbound_enqueue_cap_bound(int worker pg_attribute_unused(),
									   uint8 type pg_attribute_unused(),
									   uint32 destination pg_attribute_unused(),
									   const void *payload pg_attribute_unused(),
									   uint16 bytes pg_attribute_unused(),
									   uint32 required pg_attribute_unused(),
									   uint32 generation pg_attribute_unused())
{
	if (held_count != 0)
		abort();
	enqueues++;
	if (observe_enqueue_order)
		enqueue_order_exact
			= ctrc_bytes_zero(&ctrc_origin_entries()[0], sizeof(ClusterCtrcOriginEntry))
			  && !ctrc_bytes_zero(&ctrc_origin_entries()[3], sizeof(ClusterCtrcOriginEntry));
	return true;
}

#include "test_cluster_ctrc_dispatch.inc"

static void
reset_dispatch_fixture(void)
{
	reset_fixture();
	test_capability_generation = 29;
	test_cluster_epoch = 23;
	enters = leaves = enqueues = rechecks = fail_recheck = 0;
	resolves = throw_resolve = 0;
	allocation_failure = false;
	data_plane = root_exact = fence_ok = true;
	observe_enqueue_order = enqueue_order_exact = false;
}

static bool
seed_certificate_notification(unsigned index, ClusterCtrcCloseDispatch *dispatch,
							  uint64 *participant, uint64 *receipt)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcOriginCertificateSnapshot snapshot;

	if (!seed_frozen_certificate(index, dispatch, participant, receipt))
		return false;
	origin = &ctrc_origin_entries()[index];
	if (!cluster_ctrc_origin_arm_close_entry(origin, 0, 10000 + index)
		|| !cluster_ctrc_origin_ack_land_shared(10000 + index,
												&ctrc_participant_ack_entries()[*participant]))
		return false;
	(void)ctrc_origin_dispatchable_locked(origin);
	/* Only the in-memory edge after durable publication is provided here;
	 * this fixture does not execute or claim a successful disk write. */
	return ctrc_origin_certificate_snapshot_index_locked(index, &snapshot)
		   && cluster_ctrc_origin_certificate_commit_entry(origin, &snapshot)
		   && cluster_ctrc_origin_arm_certificate_entry(origin, 0, dispatch->request_id);
}

static bool
seed_close_notification(unsigned index, ClusterCtrcCloseDispatch *dispatch,
						ClusterCtrcLocalReleaseAckV1 *ack, bool frozen)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcReceipt receipt;
	uint64 participant_index, receipt_index;

	if (!seed_cancelled_ack_at(index, &participant, &receipt, ack, &participant_index,
							   &receipt_index)
		|| (frozen
			&& !ctrc_participant_freeze_ack_exact(participant_index, &participant, &receipt, 1,
												  ack)))
		return false;
	MemSet(dispatch, 0, sizeof(*dispatch));
	dispatch->key = participant.key;
	dispatch->participant = participant.identity;
	dispatch->grant_generation = participant.grant_generation;
	dispatch->seal_generation = participant.seal_generation;
	dispatch->suboperation = CTRC_SEAL_CLOSE_AND_CLEAN;
	dispatch->request_id = 1234 + index;
	return cluster_ctrc_origin_arm_close_entry(&ctrc_origin_entries()[index], 0,
											   dispatch->request_id);
}

UT_TEST(test_actual_gcs_close_replies_share_full_census)
{
	ClusterCtrcCloseDispatch dispatches[8];
	ClusterCtrcLocalReleaseAckV1 acks[8];

	reset_dispatch_fixture();
	for (Size i = 0; i < 8; i++)
		UT_ASSERT(seed_close_notification(i, &dispatches[i], &acks[i], false));
	for (unsigned replay = 0; replay < 2; replay++) {
		MemSet(table_visits, 0, sizeof(table_visits));
		cluster_gcs_ctrc_dispatch_batch(dispatches, 8);
		for (Size i = 0; i < 8; i++) {
			UT_ASSERT_EQ(ctrc_origin_entries()[i].close_confirmed_bitmap, 1);
			UT_ASSERT_EQ(ctrc_origin_entries()[i].ack_bitmap, 0);
		}
		UT_ASSERT_EQ(table_visits[5], CtrcShared->origin_key_entries);
		/* The already ACK_READY participant accepts a duplicate on both
		 * calls; only the second origin confirmation is also duplicate. */
		UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_PENDING_DUPLICATE), (2 * replay + 1) * 8);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(allocated, 0);
	}
}

UT_TEST(test_local_close_batch_matches_all_scalar_results)
{
	for (unsigned scenario = 0; scenario < 19; scenario++) {
		unsigned char *expected = NULL;
		uint64 expected_progress = 0, expected_duplicates = 0;
		unsigned expected_wakes = 0;

		for (unsigned batch = 0; batch < 2; batch++) {
			ClusterCtrcCloseDispatch dispatches[64];
			ClusterCtrcSealReplyResult results[64];
			ClusterCtrcLocalReleaseAckV1 acks[64];
			Size count = scenario == 0 ? 1 : (scenario == 2 || scenario == 13 ? 64 : 8);
			Size origin_bytes, ack_bytes;
			uint64 collision = 1;

			reset_dispatch_fixture();
			for (Size i = 0; i < count; i++) {
				bool ack = scenario == 3 || scenario == 9 || scenario == 10 || scenario == 11
						   || scenario == 12 || scenario == 16 || scenario == 18
						   || ((scenario == 4 || scenario == 17) && i % 2 != 0);

				UT_ASSERT(seed_close_notification(i, &dispatches[i], &acks[i], ack));
				results[i]
					= ack ? CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK : CTRC_SEAL_REPLY_PENDING_DRAIN;
				if (scenario == 13) {
					while (collision < UINT64_C(65536)
						   && ctrc_receipt_hash_bytes(UINT64_C(1469598103934665603), &collision,
													  sizeof(collision))
									  % 128
								  != 0)
						collision++;
					UT_ASSERT(collision < UINT64_C(65536));
					dispatches[i].request_id = collision++;
					ctrc_origin_entries()[i].close_request_id[0] = dispatches[i].request_id;
				}
			}
			if (scenario == 5)
				results[count - 1] = CTRC_SEAL_REPLY_DENIED;
			if (scenario == 6)
				dispatches[0].request_id += UINT64_C(1) << 50;
			if (scenario == 7 || scenario == 8 || scenario == 16) {
				ctrc_origin_entries()[count] = ctrc_origin_entries()[0];
				if (scenario == 8)
					ctrc_origin_entries()[count + 1] = ctrc_origin_entries()[0];
			}
			if (scenario == 9)
				acks[count - 1].crc32c++;
			if (scenario == 10)
				acks[0].transaction_key.xid++;
			if (scenario == 11 || scenario == 12) {
				uint64 ack_index;
				UT_ASSERT(ctrc_origin_ack_index(&dispatches[0].key, 0, &ack_index));
				if (scenario == 12) {
					UT_ASSERT(
						cluster_ctrc_origin_ack_land_shared(dispatches[0].request_id, &acks[0]));
					ctrc_origin_ack_entries()[ack_index].grant_generation++;
				} else
					ctrc_origin_ack_entries()[ack_index].reserved[0] = 1;
			}
			if (scenario == 14)
				ctrc_origin_entries()[count - 1].state = CTRC_ORIGIN_BLOCKED;
			if (scenario == 15)
				ctrc_origin_entries()[count - 1].close_dispatched_bitmap = 0;
			if (scenario == 17)
				ctrc_origin_entries()[1].close_request_id[0] = dispatches[0].request_id;
			if (scenario == 18)
				acks[0].transaction_key.owner_instance = 0;
			origin_bytes = CtrcShared->origin_key_entries * sizeof(ClusterCtrcOriginEntry);
			ack_bytes = CtrcShared->origin_ack_inbox_entries * sizeof(ClusterCtrcLocalReleaseAckV1);
			MemSet(table_visits, 0, sizeof(table_visits));
			for (unsigned replay = 0; replay < 2; replay++)
				if (batch)
					UT_ASSERT(cluster_ctrc_origin_note_local_close_batch_shared(dispatches, results,
																				acks, count));
				else
					for (Size i = 0; i < count; i++)
						if (results[i] == CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK)
							(void)cluster_ctrc_origin_ack_land_shared(dispatches[i].request_id,
																	  &acks[i]);
						else
							(void)cluster_ctrc_origin_note_close_reply_shared(
								dispatches[i].request_id, 0, results[i]);
			if (!batch) {
				expected = malloc(origin_bytes + ack_bytes);
				if (expected == NULL)
					abort();
				memcpy(expected, ctrc_origin_entries(), origin_bytes);
				memcpy(expected + origin_bytes, ctrc_origin_ack_entries(), ack_bytes);
				expected_progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
				expected_duplicates = cluster_ctrc_stat_get(CTRC_STAT_PENDING_DUPLICATE);
				expected_wakes = wake_count;
			} else {
				UT_ASSERT_EQ(memcmp(expected, ctrc_origin_entries(), origin_bytes), 0);
				UT_ASSERT_EQ(memcmp(expected + origin_bytes, ctrc_origin_ack_entries(), ack_bytes),
							 0);
				UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), expected_progress);
				UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_PENDING_DUPLICATE),
							 expected_duplicates);
				UT_ASSERT_EQ(wake_count, expected_wakes);
				UT_ASSERT_EQ(table_visits[5], results[0] == CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK
												  ? 0
												  : 2 * CtrcShared->origin_key_entries);
			}
			UT_ASSERT_EQ(held_count, 0);
		}
		free(expected);
	}
}

UT_TEST(test_local_close_invalid_shape_has_no_mutation)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		ClusterCtrcCloseDispatch dispatches[2];
		ClusterCtrcSealReplyResult results[2]
			= { CTRC_SEAL_REPLY_PENDING_DRAIN, CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK };
		ClusterCtrcLocalReleaseAckV1 acks[2];
		unsigned char *before;
		unsigned locks;
		Size bytes;
		Size count = fault == 0 ? 0 : (fault == 1 ? 65 : 2);

		reset_dispatch_fixture();
		for (Size i = 0; i < 2; i++)
			UT_ASSERT(seed_close_notification(i, &dispatches[i], &acks[i], i != 0));
		if (fault == 5)
			dispatches[1].participant.node_id = 1;
		if (fault == 6)
			dispatches[1].suboperation = CTRC_SEAL_CERTIFICATE_COMMITTED;
		if (fault == 7)
			dispatches[1].request_id = 0;
		if (fault == 8)
			dispatches[1].request_id = dispatches[0].request_id;
		bytes = CtrcShared->total_bytes;
		before = malloc(bytes);
		if (before == NULL)
			abort();
		memcpy(before, CtrcShared, bytes);
		locks = sleepable_acquisitions;
		UT_ASSERT(!cluster_ctrc_origin_note_local_close_batch_shared(
			fault == 2 ? NULL : dispatches, fault == 3 ? NULL : results, fault == 4 ? NULL : acks,
			count));
		UT_ASSERT_EQ(sleepable_acquisitions, locks);
		UT_ASSERT_EQ(memcmp(before, CtrcShared, bytes), 0);
		free(before);
	}
}

UT_TEST(test_actual_gcs_close_each_token_stage_refuses_only_its_reply)
{
	for (unsigned stage = 0; stage < 4; stage++)
		for (unsigned failed = 0; failed < 3; failed++) {
			ClusterCtrcCloseDispatch dispatches[3];
			ClusterCtrcLocalReleaseAckV1 acks[3];

			reset_dispatch_fixture();
			for (Size i = 0; i < 3; i++)
				UT_ASSERT(seed_close_notification(i, &dispatches[i], &acks[i], false));
			fail_recheck = stage == 3 ? 10 + failed : failed * 3 + stage + 1;
			cluster_gcs_ctrc_dispatch_batch(dispatches, 3);
			for (Size i = 0; i < 3; i++)
				UT_ASSERT_EQ(ctrc_origin_entries()[i].close_confirmed_bitmap, i == failed ? 0 : 1);
			UT_ASSERT_EQ(enters, leaves);
			UT_ASSERT_EQ(allocated, 0);
		}
}

static void
throw_from_participant_wakeup(void)
{
	if (held_count != 0)
		abort();
	wakeup_hook = NULL;
	pg_re_throw();
}

UT_TEST(test_actual_gcs_close_error_and_allocation_fallbacks)
{
	for (unsigned failure = 0; failure < 6; failure++) {
		ClusterCtrcCloseDispatch dispatches[2];
		ClusterCtrcLocalReleaseAckV1 acks[2];
		volatile bool caught = false;

		reset_dispatch_fixture();
		for (Size i = 0; i < 2; i++)
			UT_ASSERT(seed_close_notification(i, &dispatches[i], &acks[i], false));
		if (failure < 2)
			fail_allocation_attempt = allocation_attempts + 1 + failure;
		if (failure == 2)
			dispatches[1] = dispatches[0];
		if (failure == 3)
			dispatches[1].request_id = dispatches[0].request_id;
		if (failure == 4)
			throw_resolve = 2;
		if (failure == 5) {
			uint64 participant;
			UT_ASSERT(ctrc_participant_index(&dispatches[0].key, 0, &participant));
			ctrc_participant_entries()[participant].state = CTRC_PARTICIPANT_OPEN;
			ctrc_participant_entries()[participant].seal_generation = 0;
			wakeup_hook = throw_from_participant_wakeup;
		}
		PG_TRY();
		{
			cluster_gcs_ctrc_dispatch_batch(dispatches, 2);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, failure >= 4);
		if (failure >= 4)
			UT_ASSERT_EQ(ctrc_origin_entries()[0].close_confirmed_bitmap, 0);
		else
			UT_ASSERT_EQ(ctrc_origin_entries()[0].close_confirmed_bitmap, 1);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(allocated, 0);
	}
}

UT_TEST(test_actual_gcs_close_ack_and_mixed_results)
{
	for (unsigned mixed = 0; mixed < 2; mixed++) {
		ClusterCtrcCloseDispatch dispatches[8];
		ClusterCtrcLocalReleaseAckV1 acks[8];

		reset_dispatch_fixture();
		for (Size i = 0; i < 8; i++)
			UT_ASSERT(seed_close_notification(i, &dispatches[i], &acks[i], !mixed || i % 2 != 0));
		MemSet(table_visits, 0, sizeof(table_visits));
		cluster_gcs_ctrc_dispatch_batch(dispatches, 8);
		for (Size i = 0; i < 8; i++) {
			uint64 ack_index;
			UT_ASSERT_EQ(ctrc_origin_entries()[i].close_confirmed_bitmap, 1);
			UT_ASSERT_EQ(ctrc_origin_entries()[i].ack_bitmap, !mixed || i % 2 != 0 ? 1 : 0);
			if (!mixed || i % 2 != 0) {
				UT_ASSERT(ctrc_origin_ack_index(&dispatches[i].key, 0, &ack_index));
				UT_ASSERT_EQ(
					memcmp(&ctrc_origin_ack_entries()[ack_index], &acks[i], sizeof(acks[i])), 0);
			}
		}
		UT_ASSERT_EQ(table_visits[5], mixed ? CtrcShared->origin_key_entries : 0);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(allocated, 0);
	}
}

UT_TEST(test_actual_gcs_batch_reclaims_eight_independent_sets_once)
{
	ClusterCtrcCloseDispatch dispatches[8];
	uint64 participants[8], receipts[8];

	reset_dispatch_fixture();
	for (Size i = 0; i < 8; i++)
		UT_ASSERT(seed_certificate_notification(i, &dispatches[i], &participants[i], &receipts[i]));
	MemSet(table_visits, 0, sizeof(table_visits));
	cluster_gcs_ctrc_dispatch_batch(dispatches, 8);
	for (Size i = 0; i < 8; i++) {
		UT_ASSERT(
			ctrc_bytes_zero(&ctrc_receipt_entries()[receipts[i]], sizeof(ClusterCtrcReceipt)));
		UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[i], sizeof(ClusterCtrcOriginEntry)));
	}
	UT_ASSERT_EQ(enters, 8);
	UT_ASSERT_EQ(leaves, 8);
	UT_ASSERT_EQ(enqueues, 0);
	UT_ASSERT_EQ(table_visits[0], CtrcShared->receipt_entries);
	UT_ASSERT_EQ(table_visits[4], CtrcShared->origin_key_entries);
	UT_ASSERT_EQ(allocated, 0);
}

UT_TEST(test_actual_gcs_authentication_faults_never_reclaim)
{
	for (int fault = 0; fault < 8; fault++) {
		ClusterCtrcCloseDispatch dispatch;
		ClusterCtrcReceipt before;
		uint64 participant, receipt;

		reset_dispatch_fixture();
		UT_ASSERT(seed_certificate_notification(0, &dispatch, &participant, &receipt));
		before = ctrc_receipt_entries()[receipt];
		if (fault == 0)
			dispatch.reserved32 = 1;
		if (fault == 1)
			dispatch.participant.boot_incarnation++;
		if (fault == 2)
			dispatch.participant.capability_record_generation++;
		if (fault == 3)
			dispatch.key.cluster_epoch++;
		if (fault == 4)
			root_exact = false;
		if (fault == 5)
			data_plane = false;
		if (fault == 6)
			fence_ok = false;
		if (fault == 7)
			fail_recheck = 1;
		cluster_gcs_ctrc_dispatch_batch(&dispatch, 1);
		UT_ASSERT_EQ(memcmp(&before, &ctrc_receipt_entries()[receipt], sizeof(before)), 0);
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(enqueues, 0);
	}
}

UT_TEST(test_actual_gcs_exception_releases_all_entered_tokens)
{
	ClusterCtrcCloseDispatch dispatches[2];
	uint64 participants[2], receipts[2];
	volatile bool caught = false;

	reset_dispatch_fixture();
	for (Size i = 0; i < 2; i++)
		UT_ASSERT(seed_certificate_notification(i, &dispatches[i], &participants[i], &receipts[i]));
	throw_resolve = 2;
	PG_TRY();
	{
		cluster_gcs_ctrc_dispatch_batch(dispatches, 2);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(enters, 2);
	UT_ASSERT_EQ(leaves, 2);
	UT_ASSERT_EQ(allocated, 0);
	for (Size i = 0; i < 2; i++)
		UT_ASSERT_EQ(ctrc_receipt_entries()[receipts[i]].state, CTRC_RECEIPT_ACK_FROZEN);
}

UT_TEST(test_actual_gcs_final_recheck_cannot_forge_origin_confirmation)
{
	ClusterCtrcCloseDispatch dispatch;
	uint64 participant, receipt;

	reset_dispatch_fixture();
	UT_ASSERT(seed_certificate_notification(0, &dispatch, &participant, &receipt));
	fail_recheck = 3;
	cluster_gcs_ctrc_dispatch_batch(&dispatch, 1);
	UT_ASSERT(ctrc_bytes_zero(&ctrc_receipt_entries()[receipt], sizeof(ClusterCtrcReceipt)));
	UT_ASSERT_EQ(ctrc_origin_entries()[0].state, CTRC_ORIGIN_RELEASE_PROVEN);
	UT_ASSERT_EQ(ctrc_origin_entries()[0].close_confirmed_bitmap, 0);
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(allocated, 0);
}

UT_TEST(test_actual_gcs_each_final_token_guards_its_origin)
{
	for (unsigned failed = 0; failed < 3; failed++) {
		ClusterCtrcCloseDispatch dispatches[3];
		uint64 participants[3], receipts[3];

		reset_dispatch_fixture();
		for (Size i = 0; i < 3; i++)
			UT_ASSERT(
				seed_certificate_notification(i, &dispatches[i], &participants[i], &receipts[i]));
		fail_recheck = 7 + failed; /* Three prepare, three use, then individual final checks. */
		cluster_gcs_ctrc_dispatch_batch(dispatches, 3);
		for (Size i = 0; i < 3; i++) {
			UT_ASSERT(
				ctrc_bytes_zero(&ctrc_receipt_entries()[receipts[i]], sizeof(ClusterCtrcReceipt)));
			if (i == failed) {
				UT_ASSERT_EQ(ctrc_origin_entries()[i].state, CTRC_ORIGIN_RELEASE_PROVEN);
				UT_ASSERT_EQ(ctrc_origin_entries()[i].close_confirmed_bitmap, 0);
			} else
				UT_ASSERT(
					ctrc_bytes_zero(&ctrc_origin_entries()[i], sizeof(ClusterCtrcOriginEntry)));
		}
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(allocated, 0);
	}
}

UT_TEST(test_origin_batch_preserves_scalar_bytes_and_refusals)
{
	for (unsigned scenario = 0; scenario < 11; scenario++) {
		unsigned char *expected = NULL;
		uint64 expected_progress = 0;
		unsigned expected_wakes = 0;

		for (unsigned batch = 0; batch < 2; batch++) {
			ClusterCtrcCloseDispatch dispatches[64];
			ClusterCtrcSealReplyResult results[64];
			Size count = scenario == 0 ? 1 : (scenario == 2 || scenario == 8 ? 64 : 8);
			Size origin_bytes, ack_bytes;
			uint64 next_collision = 1;

			reset_dispatch_fixture();
			for (Size i = 0; i < count; i++) {
				uint64 participant, receipt;

				UT_ASSERT(seed_certificate_notification(i, &dispatches[i], &participant, &receipt));
				results[i] = CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED;
				if (scenario == 8) {
					/* Distinct full IDs collide in the bounded scratch, never alias. */
					while (next_collision < UINT64_C(65536)
						   && ctrc_receipt_hash_bytes(UINT64_C(1469598103934665603),
													  &next_collision, sizeof(next_collision))
									  % 128
								  != 0)
						next_collision++;
					UT_ASSERT(next_collision < UINT64_C(65536));
					dispatches[i].request_id = next_collision++;
					ctrc_origin_entries()[i].close_request_id[0] = dispatches[i].request_id;
				}
			}
			if (scenario == 3)
				dispatches[0].request_id += UINT64_C(1) << 50;
			if (scenario == 4 || scenario == 10) {
				ctrc_origin_entries()[count] = ctrc_origin_entries()[0];
				if (scenario == 10)
					ctrc_origin_entries()[count + 1] = ctrc_origin_entries()[0];
			}
			if (scenario == 5) {
				uint64 ack_index;
				UT_ASSERT(ctrc_origin_ack_index(&dispatches[0].key, 0, &ack_index));
				ctrc_origin_ack_entries()[ack_index].reserved[0] = 1;
			}
			if (scenario == 6)
				results[0] = CTRC_SEAL_REPLY_BLOCKED_RETAIN;
			if (scenario == 7)
				ctrc_origin_entries()[0].state = CTRC_ORIGIN_CLEANING;
			if (scenario == 9)
				ctrc_origin_entries()[0].close_dispatched_bitmap = 0;
			origin_bytes = CtrcShared->origin_key_entries * sizeof(ClusterCtrcOriginEntry);
			ack_bytes = CtrcShared->origin_ack_inbox_entries * sizeof(ClusterCtrcLocalReleaseAckV1);
			/* Replay twice too: clearing or an ambiguous refusal must not revive. */
			for (unsigned replay = 0; replay < 2; replay++)
				if (batch)
					UT_ASSERT(cluster_ctrc_origin_note_local_certificate_batch_shared(
						dispatches, results, count));
				else
					for (Size i = 0; i < count; i++)
						(void)cluster_ctrc_origin_note_certificate_reply_shared(
							dispatches[i].request_id, 0, results[i]);
			if (!batch) {
				expected = malloc(origin_bytes + ack_bytes);
				if (expected == NULL)
					abort();
				memcpy(expected, ctrc_origin_entries(), origin_bytes);
				memcpy(expected + origin_bytes, ctrc_origin_ack_entries(), ack_bytes);
				expected_progress = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
				expected_wakes = wake_count;
			} else {
				UT_ASSERT_EQ(memcmp(expected, ctrc_origin_entries(), origin_bytes), 0);
				UT_ASSERT_EQ(memcmp(expected + origin_bytes, ctrc_origin_ack_entries(), ack_bytes),
							 0);
				UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS), expected_progress);
				UT_ASSERT_EQ(wake_count, expected_wakes);
			}
			UT_ASSERT_EQ(held_count, 0);
		}
		free(expected);
	}
}

UT_TEST(test_origin_batch_invalid_input_refuses_before_lock_or_mutation)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		ClusterCtrcCloseDispatch dispatches[2];
		ClusterCtrcSealReplyResult results[2]
			= { CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED, CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED };
		ClusterCtrcOriginEntry before[2];
		unsigned locks;
		Size count = fault == 0 ? 0 : (fault == 1 ? 65 : 2);

		reset_dispatch_fixture();
		for (Size i = 0; i < 2; i++) {
			uint64 participant, receipt;
			UT_ASSERT(seed_certificate_notification(i, &dispatches[i], &participant, &receipt));
			before[i] = ctrc_origin_entries()[i];
		}
		if (fault == 4)
			dispatches[1].participant.node_id = 1;
		if (fault == 5)
			dispatches[1].suboperation = CTRC_SEAL_CLOSE_AND_CLEAN;
		if (fault == 6)
			dispatches[1].request_id = 0;
		if (fault == 7)
			dispatches[1].request_id = dispatches[0].request_id;
		locks = sleepable_acquisitions;
		UT_ASSERT(!cluster_ctrc_origin_note_local_certificate_batch_shared(
			fault == 2 ? NULL : dispatches, fault == 3 ? NULL : results, count));
		UT_ASSERT_EQ(sleepable_acquisitions, locks);
		UT_ASSERT_EQ(memcmp(before, ctrc_origin_entries(), sizeof(before)), 0);
	}
}

UT_TEST(test_actual_gcs_allocation_and_alias_preserve_single_dispatch)
{
	for (int fallback = 0; fallback < 2; fallback++) {
		ClusterCtrcCloseDispatch dispatches[2];
		uint64 participant, receipt;

		reset_dispatch_fixture();
		UT_ASSERT(seed_certificate_notification(0, &dispatches[0], &participant, &receipt));
		dispatches[1] = dispatches[0];
		allocation_failure = fallback == 0;
		cluster_gcs_ctrc_dispatch_batch(dispatches, fallback == 0 ? 1 : 2);
		UT_ASSERT(ctrc_bytes_zero(&ctrc_receipt_entries()[receipt], sizeof(ClusterCtrcReceipt)));
		UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[0], sizeof(ClusterCtrcOriginEntry)));
		UT_ASSERT_EQ(enters, leaves);
		UT_ASSERT_EQ(allocated, 0);
	}
}

UT_TEST(test_actual_gcs_mixed_operations_preserve_order_and_remote_enqueue)
{
	ClusterCtrcCloseDispatch dispatches[4];
	uint64 participants[4], receipts[4];
	ClusterCtrcReceipt remote_before;

	reset_dispatch_fixture();
	for (Size i = 0; i < 4; i++)
		UT_ASSERT(seed_certificate_notification(i, &dispatches[i], &participants[i], &receipts[i]));
	dispatches[1].participant.node_id = 1;
	dispatches[1].participant.boot_incarnation = 1102;
	dispatches[2].suboperation = CTRC_SEAL_CLOSE_AND_CLEAN;
	remote_before = ctrc_receipt_entries()[receipts[1]];
	observe_enqueue_order = true;
	cluster_gcs_ctrc_dispatch_batch(dispatches, 4);
	UT_ASSERT_EQ(enqueues, 1);
	UT_ASSERT(enqueue_order_exact);
	UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[0], sizeof(ClusterCtrcOriginEntry)));
	UT_ASSERT(ctrc_bytes_zero(&ctrc_origin_entries()[3], sizeof(ClusterCtrcOriginEntry)));
	UT_ASSERT_EQ(
		memcmp(&ctrc_receipt_entries()[receipts[1]], &remote_before, sizeof(remote_before)), 0);
	UT_ASSERT_EQ(ctrc_receipt_entries()[receipts[2]].state, CTRC_RECEIPT_ACK_FROZEN);
	UT_ASSERT_EQ(enters, 4);
	UT_ASSERT_EQ(enters, leaves);
	UT_ASSERT_EQ(allocated, 0);
}

int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_actual_gcs_close_replies_share_full_census);
	UT_RUN(test_local_close_batch_matches_all_scalar_results);
	UT_RUN(test_local_close_invalid_shape_has_no_mutation);
	UT_RUN(test_actual_gcs_close_each_token_stage_refuses_only_its_reply);
	UT_RUN(test_actual_gcs_close_error_and_allocation_fallbacks);
	UT_RUN(test_actual_gcs_close_ack_and_mixed_results);
	UT_RUN(test_actual_gcs_batch_reclaims_eight_independent_sets_once);
	UT_RUN(test_actual_gcs_authentication_faults_never_reclaim);
	UT_RUN(test_actual_gcs_exception_releases_all_entered_tokens);
	UT_RUN(test_actual_gcs_final_recheck_cannot_forge_origin_confirmation);
	UT_RUN(test_actual_gcs_each_final_token_guards_its_origin);
	UT_RUN(test_origin_batch_preserves_scalar_bytes_and_refusals);
	UT_RUN(test_origin_batch_invalid_input_refuses_before_lock_or_mutation);
	UT_RUN(test_actual_gcs_allocation_and_alias_preserve_single_dispatch);
	UT_RUN(test_actual_gcs_mixed_operations_preserve_order_and_remote_enqueue);
	free(CtrcShared);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
