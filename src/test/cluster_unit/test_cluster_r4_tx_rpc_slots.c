/* Actual TX RPC at the last requester slot, with every peer still active. */
/* Reuse the service leaves; run the default two-slot suite separately. */
#define GCS_R4_TX_RPC_FULL_SLOTS 1
#define main gcs_read_image_hops_original_main
int gcs_read_image_hops_original_main(void);
#include "test_cluster_gcs_read_image_hops.c"
#undef main

static ClusterGcsBlockBackendBlock rpc_full_before[lengthof(blocks)];

static ClusterGcsBlockOutstandingSlot *
rpc_last_slot(void)
{
	return &blocks[1].slots[MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND - 1];
}

static void
rpc_full_fixture(void)
{
	BufferTag tag;
	unsigned backend, index;

	rpc_fixture();
	tag = blocks[1].slots[1].tag;
	memset(blocks, 0, sizeof(blocks));
	for (backend = 0; backend < lengthof(blocks); backend++) {
		unsigned count = MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND - (backend == 1);

		blocks[backend].next_request_id = 51;
		MyBackendId = (BackendId)(backend + 1);
		for (index = 0; index < count; index++) {
			ClusterGcsBlockOutstandingSlot *slot;
			uint64 request_id = 0;

			tag.blockNum = 300 + backend * MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND + index;
			slot = gcs_block_try_reserve_r4_slot(tag, 70, 1, &request_id);
			UT_ASSERT(slot == &blocks[backend].slots[index]);
			UT_ASSERT(request_id != 0 && slot != NULL && slot->request_id == request_id);
		}
	}
	MyBackendId = 2;
	memcpy(rpc_full_before, blocks, sizeof(blocks));
	UT_ASSERT_EQ(MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND, 8);
	UT_ASSERT_EQ(MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND,
				 CLUSTER_GCS_BLOCK_MAX_OUTSTANDING_PER_BACKEND);
	UT_ASSERT(!rpc_last_slot()->in_use);
}

static void
rpc_full_assert_peers(uint64 next_sequence)
{
	unsigned backend, index;

	UT_ASSERT_EQ(locks, 0);
	UT_ASSERT(!rpc_sleeping);
	UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
	for (backend = 0; backend < lengthof(blocks); backend++) {
		const ClusterGcsBlockBackendBlock *before = &rpc_full_before[backend];

		UT_ASSERT_EQ(blocks[backend].next_request_id,
					 backend == 1 ? next_sequence : before->next_request_id);
		UT_ASSERT_EQ(memcmp(&blocks[backend].lock, &before->lock, sizeof(before->lock)), 0);
		UT_ASSERT_EQ(
			memcmp(&blocks[backend].capacity_cv, &before->capacity_cv, sizeof(before->capacity_cv)),
			0);
		for (index = 0; index < MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND; index++) {
			if (backend == 1 && index == MAX_OUTSTANDING_BLOCK_REQUESTS_PER_BACKEND - 1)
				continue;
			UT_ASSERT(blocks[backend].slots[index].in_use);
			UT_ASSERT_EQ(memcmp(&blocks[backend].slots[index], &before->slots[index],
								sizeof(before->slots[index])),
						 0);
		}
	}
}

static void
rpc_full_assert_clean(uint64 next_sequence)
{
	ClusterGcsBlockOutstandingSlot *slot = rpc_last_slot();

	rpc_full_assert_peers(next_sequence);
	UT_ASSERT(!slot->in_use && !slot->reply_received && !slot->stale);
	UT_ASSERT_EQ(slot->request_id | slot->request_epoch, 0);
	UT_ASSERT_EQ(slot->reply_domain, CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE);
	UT_ASSERT(!slot->expected_current_mx_key_valid && !slot->expected_current_mx_proof_valid);
	UT_ASSERT(!slot->direct_target_prepared && !slot->first_fill.armed);
}

UT_TEST(r4_tx_rpc_last_slot_terminal_results_preserve_all_peers)
{
	for (unsigned aborted = 0; aborted < 2; aborted++) {
		rpc_full_fixture();
		if (aborted) {
			rpc_resolution.outcome = CLUSTER_TX_ABORTED;
			rpc_resolution.commit_scn = 0;
		}
		UT_ASSERT_EQ(rpc_call(), rpc_resolution.outcome);
		UT_ASSERT_EQ(rpc_reason, CLUSTER_TX_RESOLVE_NONE);
		UT_ASSERT_EQ(rpc_forward.base.request_id, UINT64CONST(0x020001000000003a));
		UT_ASSERT_EQ(rpc_result.outcome, rpc_resolution.outcome);
		UT_ASSERT_EQ(memcmp(&rpc_result.locator_echo, &rpc_locator, sizeof(rpc_locator)), 0);
		UT_ASSERT_EQ(rpc_result.authority.live_hwm_lsn, 123456);
		UT_ASSERT_EQ(rpc_observations, 3);
		UT_ASSERT_EQ(rpc_observed[0], rpc_resolution.commit_scn);
		UT_ASSERT_EQ(rpc_observed[1], 0);
		UT_ASSERT_EQ(rpc_observed[2], 9002);
		UT_ASSERT_EQ(rpc_prepares, 1);
		UT_ASSERT_EQ(rpc_cancels, 1);
		UT_ASSERT_EQ(signals, 1);
		UT_ASSERT_EQ(broadcasts, 1);
		UT_ASSERT_EQ(gcs_undo_verdict_path_counters.r4_tx_resolve.verified_replies, 1);
		rpc_full_assert_clean(59);
	}
}

UT_TEST(r4_tx_rpc_last_slot_failure_and_interrupts_preserve_all_peers)
{
	static const RpcTestAction actions[]
		= { RPC_SEND_REFUSE,  RPC_CANCEL,		 RPC_DEADLOCK,		RPC_STALE_SLOT,
			RPC_EPOCH_CHANGE, RPC_WRONG_LOCATOR, RPC_WRONG_EPOCH,	RPC_INVALID_SCN,
			RPC_NO_LSN,		  RPC_WRONG_STATUS,	 RPC_ORIGIN_DENIED, RPC_BAD_CHECKSUM };
	ClusterTxResolution zero = { 0 };

	for (unsigned i = 0; i < lengthof(actions); i++) {
		volatile bool caught = false;
		volatile ClusterTxOutcome outcome = CLUSTER_TX_UNKNOWN;
		bool throws = actions[i] == RPC_CANCEL || actions[i] == RPC_DEADLOCK;
		bool matched = actions[i] == RPC_EPOCH_CHANGE || actions[i] == RPC_WRONG_LOCATOR
					   || actions[i] == RPC_WRONG_EPOCH || actions[i] == RPC_INVALID_SCN
					   || actions[i] == RPC_ORIGIN_DENIED;

		rpc_full_fixture();
		rpc_action = actions[i];
		PG_TRY();
		{
			outcome = rpc_call();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, throws);
		UT_ASSERT_EQ(outcome, CLUSTER_TX_UNKNOWN);
		UT_ASSERT_EQ(memcmp(&rpc_result, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(rpc_observations, 0);
		UT_ASSERT_EQ(signals, matched);
		UT_ASSERT_EQ(gcs_undo_verdict_path_counters.r4_tx_resolve.matched_replies, matched);
		UT_ASSERT_EQ(gcs_undo_verdict_path_counters.r4_tx_resolve.verified_replies, 0);
		UT_ASSERT_EQ(broadcasts, 1);
		UT_ASSERT_EQ(rpc_cancels, actions[i] != RPC_SEND_REFUSE);
		if (throws) {
			UT_ASSERT_EQ(rpc_sqlstate, actions[i] == RPC_CANCEL ? ERRCODE_QUERY_CANCELED
																: ERRCODE_T_R_DEADLOCK_DETECTED);
			UT_ASSERT_EQ(report_level, ERROR);
		} else if (actions[i] == RPC_ORIGIN_DENIED)
			UT_ASSERT_EQ(rpc_reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
		else if (matched)
			UT_ASSERT_EQ(rpc_reason, CLUSTER_TX_RESOLVE_AUTHORITY_STALE);
		else
			UT_ASSERT_EQ(rpc_reason, CLUSTER_TX_RESOLVE_IO_ERROR);
		rpc_full_assert_clean(59);
		cluster_gcs_handle_block_reply_envelope(&envelope, &payload);
		rpc_full_assert_clean(59);
	}
}

UT_TEST(r4_tx_rpc_last_slot_late_reply_and_reuse_keep_original_budget)
{
	ClusterTxResolution zero = { 0 };

	rpc_full_fixture();
	rpc_action = RPC_QUIET;
	UT_ASSERT_EQ(rpc_call(), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(rpc_reason, CLUSTER_TX_RESOLVE_IO_ERROR);
	UT_ASSERT_EQ(memcmp(&rpc_result, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(rpc_forward.base.request_id, UINT64CONST(0x020001000000003a));
	UT_ASSERT_EQ(rpc_sleeps, 2);
	UT_ASSERT_EQ(rpc_timeouts[0], 10);
	UT_ASSERT_EQ(rpc_timeouts[1], 6);
	UT_ASSERT_EQ(rpc_now_ms, 10);
	UT_ASSERT_EQ(rpc_observations | signals, 0);
	rpc_full_assert_clean(59);
	rpc_late_reply = payload;
	receive();
	UT_ASSERT_EQ(signals, 0);
	rpc_full_assert_clean(59);

	rpc_action = RPC_LATE_THEN_EXACT;
	rpc_sleeps = 0;
	rpc_now_ms = 0;
	UT_ASSERT_EQ(rpc_call(), CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(rpc_reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(rpc_forward.base.request_id, UINT64CONST(0x020001000000003b));
	UT_ASSERT_EQ(rpc_sleeps, 2);
	UT_ASSERT_EQ(rpc_timeouts[0], 10);
	UT_ASSERT_EQ(rpc_timeouts[1], 6);
	UT_ASSERT_EQ(signals, 1);
	UT_ASSERT_EQ(broadcasts, 2);
	UT_ASSERT_EQ(rpc_prepares, 2);
	UT_ASSERT_EQ(rpc_cancels, 2);
	UT_ASSERT_EQ(gcs_undo_verdict_path_counters.r4_tx_resolve.matched_replies, 1);
	UT_ASSERT_EQ(gcs_undo_verdict_path_counters.r4_tx_resolve.verified_replies, 1);
	rpc_full_assert_clean(60);
}

UT_TEST(r4_tx_rpc_full_row_refusal_does_not_consume_identity)
{
	ClusterTxResolution zero = { 0 };
	ClusterGcsBlockOutstandingSlot occupied;
	BufferTag tag;
	uint64 request_id = 0;

	rpc_full_fixture();
	tag = blocks[1].slots[0].tag;
	UT_ASSERT(gcs_block_try_reserve_r4_slot(tag, 70, 1, &request_id) == rpc_last_slot());
	UT_ASSERT_EQ(request_id, UINT64CONST(0x020001000000003a));
	occupied = *rpc_last_slot();
	UT_ASSERT_EQ(rpc_call(), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(rpc_reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&rpc_result, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(memcmp(&occupied, rpc_last_slot(), sizeof(occupied)), 0);
	UT_ASSERT_EQ(rpc_sends | rpc_prepares | rpc_cancels | rpc_sleeps | rpc_observations, 0);
	UT_ASSERT_EQ(signals | broadcasts, 0);
	rpc_full_assert_peers(59);
	gcs_block_release_slot(rpc_last_slot());
	rpc_full_assert_clean(59);
	UT_ASSERT_EQ(rpc_call(), CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(rpc_forward.base.request_id, UINT64CONST(0x020001000000003b));
	UT_ASSERT_EQ(rpc_sends, 1);
	UT_ASSERT_EQ(broadcasts, 2);
	rpc_full_assert_clean(60);
}

UT_TEST(r4_tx_rpc_exhausted_last_slot_never_reuses_a_request_id)
{
	ClusterTxResolution zero = { 0 };

	rpc_full_fixture();
	blocks[1].next_request_id = GCS_REQID_REQUESTER_SEQ_MASK;
	UT_ASSERT_EQ(rpc_call(), CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(rpc_forward.base.request_id, UINT64CONST(0x020001ffffffffff));
	rpc_full_assert_clean(GCS_REQID_REQUESTER_SEQ_MASK + 1);
	UT_ASSERT_EQ(rpc_call(), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(rpc_reason, CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(memcmp(&rpc_result, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(rpc_sends, 1);
	UT_ASSERT_EQ(rpc_prepares, 1);
	UT_ASSERT_EQ(rpc_cancels, 1);
	UT_ASSERT_EQ(broadcasts, 1);
	UT_ASSERT_EQ(rpc_observations, 3);
	receive();
	UT_ASSERT_EQ(signals, 1);
	rpc_full_assert_clean(GCS_REQID_REQUESTER_SEQ_MASK + 1);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(r4_tx_rpc_last_slot_terminal_results_preserve_all_peers);
	UT_RUN(r4_tx_rpc_last_slot_failure_and_interrupts_preserve_all_peers);
	UT_RUN(r4_tx_rpc_last_slot_late_reply_and_reuse_keep_original_budget);
	UT_RUN(r4_tx_rpc_full_row_refusal_does_not_consume_identity);
	UT_RUN(r4_tx_rpc_exhausted_last_slot_never_reuses_a_request_id);
	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
