/* Observe original stop gates inside a real two-transfer ownership cycle.
 * Settled terminal acceptance requires the original completion proof.
 * Author: SqlRush <sqlrush@gmail.com> */
static void ns_cut(unsigned kind);
#include "test_cluster_native_stop_cycle.inc"
#include "test_cluster_native_stop_census.h"
#if defined(NATIVE_STOP_EXPECT_ASSERT_OFF) && defined(USE_ASSERT_CHECKING)
#error "stop-census assertion-OFF requires an assertion-disabled configuration"
#endif

enum { NS_LIVE, NS_PARK, NS_ACK, NS_ACK_SENT, NS_CUT_COUNT };
static unsigned ns_seen[NS_CUT_COUNT];
static bool ns_successor_x;
static const char *ns_names[] = { "master-live", "holder-parked", "holder-ACK-unoffered", "holder-ACK-callback" };

/* Find the actual retained join by its decoded original assertion. No
 * replacement authority, state import or terminal cleanup is manufactured. */
static bool ns_settled_state(ClusterPcmResourceXMasterState **out)
{
	unsigned char *memory = (void *)fake_pcm_header.data;
	const ClusterPcmResourceXBootstrapRound *round = &request_entry()->resource_x_bootstrap_round;
	unsigned matches = 0;
	for (Size n = 0; n + sizeof(**out) <= fake_pcm_header_requested_size; n += sizeof(uint64)) {
		ClusterPcmResourceXMasterState *state = (void *)(memory + n);
		ResourceXDecodedFrame grant; ResourceXWireReject reject;
		if (state->binding_generation != request_entry()->binding_generation
			|| state->requester_join.acquire_format != 1 || state->requester_join.grant_valid != 1
			|| state->requester_join.grant_payload_bytes != RESOURCE_X_DECISION_V4_BYTES)
			continue;
		if (cluster_resource_x_wire_decode(RESOURCE_X_MSG_IMAGE_OR_GRANT,
			state->requester_join.grant_payload, RESOURCE_X_DECISION_V4_BYTES, &grant, &reject)
			&& resource_x_assertion_equal(&grant.common.logical_assertion, &round->request.logical_assertion)
			&& grant.common.assertion_sequence == round->request.assertion_sequence) {
			*out = state; matches++;
		}
	}
	NM_REQUIRE(matches == 1); return true;
}

static bool ns_settled_boundaries(void)
{
	ClusterPcmResourceXMasterState *state = NULL;
	ClusterPcmResourceXBootstrapRound *round = &request_entry()->resource_x_bootstrap_round;
	ClusterPcmResourceXMasterState saved;
	ClusterPcmResourceXBootstrapRound original;
	bool source;

	NM_REQUIRE(ns_settled_state(&state));
	saved = *state;
	original = *round;
	source = round->request.target_mode == PCM_STATE_X;
	printf("# settled-proof node=%d directory=%u source-history=" UINT64_FORMAT " checks=%u\n",
		cluster_node_id, pg_atomic_read_u32(&request_entry()->master_state),
		state->settlement_retry.generation, source ? 25 : 21);
	for (unsigned n = 0; n < (source ? 25 : 21); n++) {
		ClusterNormalStopPollResult result;
		ClusterPcmResourceXMasterState mutated;
		ClusterPcmResourceXBootstrapRound changed;
		*round = original; *state = saved;
		switch (n) {
			case 0: round->acquire_read_handoff = 0; break;
			case 1: round->delivery_executor_purpose = RESOURCE_X_DELIVERY_ACQUIRE_READ_CONSUMED; break;
			case 2: round->send.pending = 1; break;
			case 3: round->send.sent = 0; break;
			case 4: round->acquire_read_receipt.decision_crc32c ^= 1; break;
			case 5: round->acquire_read_receipt.page_crc32c ^= 1; break;
			case 6: round->delivery_target.generation++; break;
			case 7: round->delivery_executor_sequence = 0; break;
			case 8: state->requester_settlement_intent.slot.first_armed_us = 1; break;
			case 9: state->grant_intent.slot.first_armed_us = 1; break;
			case 10: state->holder_status_intent.slot.last_attempt_us = 1; break;
			case 11: state->holder_image_intent.last_attempt_us = 1; break;
			case 12: state->block_intents[0].slot.first_armed_us = 1; break;
			case 13: state->source_settlement.intent.slot.first_armed_us = 1; break;
			case 14: state->requester_join.grant_payload[0] ^= 1; break;
			case 15: state->requester_join.image_payload[0] ^= 1; break;
			case 16: state->requester_settlement_intent.payload[0] ^= 1; break;
			case 17: round->send.reserved[0] = 1; break;
			case 18: round->send.authority_generation++; break;
			case 19: state->holder_status_intent.slot.first_armed_us = UINT64_MAX - 1; break;
			case 20: state->holder_image_intent.first_armed_us = UINT64_MAX - 1; break;
			case 21: state->settlement_retry.retired_authority++; break;
			case 22: state->settlement_retry.retired_resource_binding++; break;
			case 23: state->settlement_retry.retired_master_session++; break;
			case 24: round->terminal_ref.acquisition_generation++; break;
		}
		mutated = *state;
		changed = *round;
		result = stop_pcm_poll(false, NULL, NULL, NULL);
		if (result == CLUSTER_NORMAL_STOP_READY)
			printf("# settled-proof unexpectedly READY node=%d mutation=%u\n", cluster_node_id, n);
		UT_ASSERT_NE(result, CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT(memcmp(state, &mutated, sizeof(mutated)) == 0);
		UT_ASSERT(memcmp(round, &changed, sizeof(changed)) == 0);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
	*round = original; *state = saved;
	UT_ASSERT_EQ(ns_pcm("settled-proof-restored"), CLUSTER_NORMAL_STOP_READY);
	return true;
}

static void ns_cut(unsigned kind)
{
	UT_ASSERT(kind < NS_CUT_COUNT); if (kind >= NS_CUT_COUNT) return;
	ns_seen[kind]++;
	UT_ASSERT_EQ(ns_pcm(ns_names[kind]), CLUSTER_NORMAL_STOP_PENDING);
	if (kind == NS_PARK || kind == NS_ACK || kind == NS_ACK_SENT)
		ns_buffer_pending(ns_names[kind]);
}

static void ns_roles(bool second)
{
	ss_roles(second);
	if (second && ns_successor_x) nt_target_mode = PCM_STATE_X;
}

/* Keep the first source's real S->N retirement in the same process, then
 * acquire X through its original native writer rather than resetting PCM. */
static bool ns_reader_to_x(int master, int writer, int mode)
{
	ClusterPcmXWriterLedgerEntry *entry;
	ClusterPcmResourceXMasterState *state = NULL;
	ns_roles(false); nm_settled_observer = ss_capture;
	NM_REQUIRE(nt_holder(master, writer, mode)); nm_settled_observer = NULL;
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N);
	ns_roles(true);
	memset(&request_shared, 0, sizeof(request_shared));
	pg_atomic_init_u64(&request_shared.resource_x_transport_mutation_sequence, 1);
	ordinary_sends = ordinary_pauses = ordinary_leaves = 0;
	ordinary_current_calls = ordinary_peer_samples = 0;
	disk_reads = complete_commits = complete_io_calls = complete_broadcasts = 0;
	ordinary_active = NULL; nc_fd = master; nc_image_fd = writer; nc_mode = mode;
	nc_source_node = 1; nc_source_boot = 101;
	auth_observation_hook = nc_auth_peer; ordinary_delivery_transport = xt_deliver;
	MyBackendType = B_BACKEND;
	cluster_enabled = cluster_shared_config = cluster_gcs_block_local_cache = true;
	wc_r4 = 77; wc_path = RESOURCE_X_WRITER_TARGET; wc_session_id = 31; wc_master = 0;
	wc_legacy = wc_failures = wc_waits = 0; rx_barrier = rx_cancel_wait = false;
	rx_native_boundary = NULL;
	entry = rx_prepare(NULL); NM_REQUIRE(entry != NULL);
	NM_REQUIRE(xt_use_writer(entry, false));
	NM_REQUIRE(ns_settled_state(&state)); NM_REQUIRE(xt_notice(master, state, true));
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_X);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT(state->settlement_retry.generation != 0);
	UT_ASSERT(state->settlement_retry.retired_authority
		< request_entry()->resource_x_bootstrap_round.terminal_authority_generation);
	UT_ASSERT_EQ(ns_pcm("S-N-X-prior-source-settled"), CLUSTER_NORMAL_STOP_READY);
	NM_REQUIRE(ns_settled_boundaries());
	complete_idle(); return true;
}

static bool ns_master(int reader, int writer, int mode)
{
	if (ns_successor_x) {
		ns_roles(false); NM_REQUIRE(nt_master(writer, reader, mode));
		ns_roles(true); NM_REQUIRE(nt_master(reader, writer, mode));
	} else NM_REQUIRE(hc_master(reader, writer, mode));
	UT_ASSERT_EQ(ns_seen[NS_LIVE], 2);
	UT_ASSERT_EQ(ns_pcm("master-settled"), CLUSTER_NORMAL_STOP_READY);
	return true;
}
static bool ns_reader(int master, int writer, int mode, int source, uint64 boot)
{
	if (ns_successor_x) return ns_reader_to_x(master, writer, mode);
	NM_REQUIRE(hc_reader(master, writer, mode, source, boot));
	UT_ASSERT_EQ(ns_seen[NS_PARK], 1); UT_ASSERT_EQ(ns_seen[NS_ACK], 1);
	UT_ASSERT_EQ(ns_seen[NS_ACK_SENT], 1);
	UT_ASSERT_EQ(ns_pcm("reader-settled"), CLUSTER_NORMAL_STOP_READY);
	NM_REQUIRE(ns_settled_boundaries());
	return true;
}
static bool ns_writer(int master, int reader, int mode)
{
	if (ns_successor_x) {
		ClusterPcmXWriterLedgerEntry *entry;
		ns_roles(false); NM_REQUIRE(xt_requester(master, reader, mode, 2, 101));
		entry = rx_prepare(NULL); NM_REQUIRE(entry != NULL);
		NM_REQUIRE(xt_use_writer(entry, true));
		holder_writes = holder_write_notes = holder_output_wakes = holder_wal_flushes = 0;
		holder_written_wal_flushes = 0; holder_redirty = false;
		local_pi_writer_ready = local_pi_covered = false;
		BufferDescriptorGetContentLock(&request_buf)->tranche = LWTRANCHE_BUFFER_CONTENT;
		ns_roles(true); NM_REQUIRE(nt_holder(master, reader, mode));
		return true;
	}
	NM_REQUIRE(hc_writer(master, reader, mode));
	UT_ASSERT_EQ(ns_seen[NS_PARK], 1); UT_ASSERT_EQ(ns_seen[NS_ACK], 1);
	UT_ASSERT_EQ(ns_seen[NS_ACK_SENT], 1);
	UT_ASSERT_EQ(ns_pcm("writer-settled"), CLUSTER_NORMAL_STOP_READY);
	NM_REQUIRE(ns_settled_boundaries());
	return true;
}
static void ns_run(int order, bool error)
{
	hc_order = order; hc_inject_error = error;
	hc_releases = hc_errors = hc_drains = hc_seed_calls = 0;
	memset(hc_locks, 0, sizeof(hc_locks)); memset(hc_owners, 0, sizeof(hc_owners));
	memset(ns_seen, 0, sizeof(ns_seen));
	nt_master_role = ns_master; nt_holder_role = ns_writer; nt_requester = ns_reader;
	nt_run(5);
}
UT_TEST(test_stop_observes_forward_reader_drain_and_unoffered_ACK) { ns_run(0, false); }
UT_TEST(test_stop_observes_reverse_reader_drain_and_unoffered_ACK) { ns_run(1, false); }
UT_TEST(test_stop_observes_forward_reader_ERROR_without_clearing_debt) { ns_run(0, true); }
UT_TEST(test_stop_observes_reverse_reader_ERROR_without_clearing_debt) { ns_run(1, true); }
UT_TEST(test_stop_accepts_new_X_after_original_S_source_retirement)
{
	ns_successor_x = true; ns_run(0, false); ns_successor_x = false;
}
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(5);
	UT_RUN(test_stop_observes_forward_reader_drain_and_unoffered_ACK);
	UT_RUN(test_stop_observes_reverse_reader_drain_and_unoffered_ACK);
	UT_RUN(test_stop_observes_forward_reader_ERROR_without_clearing_debt);
	UT_RUN(test_stop_observes_reverse_reader_ERROR_without_clearing_debt);
	UT_RUN(test_stop_accepts_new_X_after_original_S_source_retirement);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
